//! Cursor-backed materialization for the runtime execution receipt ledger.
//!
//! `execution-receipts.jsonl` remains the authoritative record. The SQLite
//! sidecar only records a cursor plus the small projections needed by the
//! runtime worker and virtual-session evidence paths, so normal reads tail new
//! bytes instead of replaying the whole ledger.

use crate::logging::with_jsonl_append_lock;
use crate::runtime_worker::{RuntimeExecutionReceipt, RuntimeExecutionReceiptStatus};
use rusqlite::{
    Connection, OptionalExtension, Transaction, TransactionBehavior, params, params_from_iter,
};
use serde_json::Value;
use std::collections::{BTreeSet, HashMap};
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

const EXECUTION_RECEIPTS_FILE_NAME: &str = "execution-receipts.jsonl";
const EXECUTION_RECEIPT_INDEX_FILE_NAME: &str = "execution-receipt-index.sqlite";
const EXECUTION_RECEIPT_INDEX_SCHEMA: &str = "agent-harness.runtime-execution-receipt-index.v2";
const EXECUTION_RECEIPT_INDEX_REVISION: i64 = 3;
const RECEIPT_TAIL_FINGERPRINT_BYTES: u64 = 4 * 1024;
const MAX_EVIDENCE_PER_QUEUE_ID: i64 = 3;
const MAX_EVIDENCE_STATUS_CHARS: usize = 80;
const MAX_EVIDENCE_REASON_CHARS: usize = 180;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeExecutionReceiptEvidence {
    pub(crate) queue_id: String,
    pub(crate) status: String,
    pub(crate) file: PathBuf,
    pub(crate) at_ms: Option<i64>,
    pub(crate) reason: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ExecutionReceiptLedgerCursor {
    offset_bytes: u64,
    line_number: u64,
    source_modified_at_unix_nanos: Option<u128>,
    prefix_tail_fingerprint: Option<u64>,
}

impl ExecutionReceiptLedgerCursor {
    fn is_pristine(&self) -> bool {
        self.offset_bytes == 0
            && self.line_number == 0
            && self.source_modified_at_unix_nanos.is_none()
            && self.prefix_tail_fingerprint.is_none()
    }
}

/// Returns the durable SQLite sidecar colocated with the source JSONL ledger.
pub(crate) fn runtime_execution_receipt_index_file(queue_dir: &Path) -> PathBuf {
    queue_dir.join(EXECUTION_RECEIPT_INDEX_FILE_NAME)
}

/// Materializes the latest `Prepared` receipt for every queue ID.
///
/// The source JSONL ledger remains authoritative. This function holds its
/// append lock only while it validates/tails the cursor and then reads the
/// durable projection, avoiding a historical replay during normal calls.
pub(crate) fn prepared_execution_receipts_from_index(
    queue_dir: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<HashMap<String, RuntimeExecutionReceipt>> {
    refresh_execution_receipt_index(queue_dir, warnings)?;
    let connection = open_or_create_execution_receipt_index(queue_dir)?;
    read_prepared_execution_receipts(&connection, warnings)
}

/// Answers the legacy prepared-receipt guard without requiring a historical
/// receipt to deserialize into the current full receipt schema.
///
/// Older durable ledgers may contain only a queue id and `prepared` status.
/// The pre-index implementation treated those as sufficient to protect a
/// queued item from being rewritten, so retain that safety property in a
/// separate compact projection.
pub(crate) fn has_prepared_execution_receipt_from_index(
    queue_dir: &Path,
    queue_id: &str,
    warnings: &mut Vec<String>,
) -> io::Result<bool> {
    refresh_execution_receipt_index(queue_dir, warnings)?;
    let connection = open_or_create_execution_receipt_index(queue_dir)?;
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM execution_prepared_queue_ids WHERE queue_id = ?1)",
            [queue_id],
            |row| row.get::<_, i64>(0),
        )
        .map(|value| value != 0)
        .map_err(io::Error::other)
}

/// Returns the last source-line `Prepared` receipt that declares an execution
/// directory, without replaying the source ledger during normal calls.
///
/// This is deliberately a separate bounded projection: the worker's
/// per-queue latest receipt projection cannot answer this question when a
/// newer `Prepared` receipt for the same queue omits `executionDir`.
pub(crate) fn latest_prepared_execution_dir_from_index(
    queue_dir: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<Option<PathBuf>> {
    refresh_execution_receipt_index(queue_dir, warnings)?;
    let connection = open_or_create_execution_receipt_index(queue_dir)?;
    connection
        .query_row(
            "SELECT execution_dir FROM execution_latest_prepared_execution_dir WHERE singleton = 1",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map(|execution_dir| execution_dir.map(PathBuf::from))
        .map_err(io::Error::other)
}

/// Returns at most three recent receipt summaries for each exact queue ID.
///
/// Evidence is intentionally bounded in the sidecar. Callers must still apply
/// their own presentation limits when querying more than one queue ID.
pub(crate) fn execution_receipt_evidence_for_queue_ids(
    queue_dir: &Path,
    queue_ids: &BTreeSet<String>,
    warnings: &mut Vec<String>,
) -> io::Result<Vec<RuntimeExecutionReceiptEvidence>> {
    if queue_ids.is_empty() {
        return Ok(Vec::new());
    }
    refresh_execution_receipt_index(queue_dir, warnings)?;
    let connection = open_or_create_execution_receipt_index(queue_dir)?;
    let placeholders = std::iter::repeat_n("?", queue_ids.len())
        .collect::<Vec<_>>()
        .join(", ");
    let statement = format!(
        "SELECT queue_id, status, reason, at_ms \
         FROM execution_receipt_evidence \
         WHERE queue_id IN ({placeholders}) \
         ORDER BY source_line ASC"
    );
    let mut statement = connection.prepare(&statement).map_err(io::Error::other)?;
    let evidence_file = execution_receipts_file(queue_dir);
    let rows = statement
        .query_map(params_from_iter(queue_ids.iter()), |row| {
            Ok(RuntimeExecutionReceiptEvidence {
                queue_id: row.get(0)?,
                status: row.get(1)?,
                file: evidence_file.clone(),
                at_ms: row.get(3)?,
                reason: row.get(2)?,
            })
        })
        .map_err(io::Error::other)?;
    let mut evidence = Vec::new();
    for row in rows {
        evidence.push(row.map_err(io::Error::other)?);
    }
    Ok(evidence)
}

fn execution_receipts_file(queue_dir: &Path) -> PathBuf {
    queue_dir.join(EXECUTION_RECEIPTS_FILE_NAME)
}

fn refresh_execution_receipt_index(queue_dir: &Path, warnings: &mut Vec<String>) -> io::Result<()> {
    let receipts_file = execution_receipts_file(queue_dir);
    with_jsonl_append_lock(&receipts_file, || {
        refresh_execution_receipt_index_locked(queue_dir, &receipts_file, warnings)
    })
}

/// The caller owns the source-ledger append lock, so all index mutations see a
/// complete source prefix and cannot race a normal JSONL append.
fn refresh_execution_receipt_index_locked(
    queue_dir: &Path,
    receipts_file: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<()> {
    let mut connection = open_or_create_execution_receipt_index(queue_dir)?;
    let cursor = match read_execution_receipt_cursor(&connection) {
        Ok(Some(cursor)) => cursor,
        Ok(None) => ExecutionReceiptLedgerCursor::default(),
        Err(error) => {
            warnings.push(format!(
                "runtime execution receipt index cursor is invalid; rebuilding: {error}"
            ));
            rebuild_execution_receipt_index(&mut connection, receipts_file, warnings)?;
            return Ok(());
        }
    };

    let metadata = match fs::metadata(receipts_file) {
        Ok(metadata) if metadata.is_file() => metadata,
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "runtime execution receipt ledger path is not a file: {}",
                    receipts_file.display()
                ),
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if !cursor.is_pristine() || execution_receipt_index_has_records(&connection)? {
                warnings.push(
                    "runtime execution receipt ledger disappeared; rebuilding receipt index"
                        .to_string(),
                );
                reset_execution_receipt_index(&mut connection)?;
            }
            return Ok(());
        }
        Err(error) => return Err(error),
    };
    let source_len = metadata.len();
    let source_modified_at_unix_nanos = file_modified_at_unix_nanos(&metadata);

    if source_len < cursor.offset_bytes {
        warnings.push(
            "runtime execution receipt ledger was truncated; rebuilding receipt index".to_string(),
        );
        rebuild_execution_receipt_index(&mut connection, receipts_file, warnings)?;
        return Ok(());
    }
    if source_len == cursor.offset_bytes {
        let prefix_tail_matches = execution_receipt_prefix_tail_matches(receipts_file, &cursor)?;
        if source_modified_at_unix_nanos == cursor.source_modified_at_unix_nanos
            && prefix_tail_matches
        {
            return Ok(());
        }
        warnings.push(
            "runtime execution receipt ledger changed without an append; rebuilding receipt index"
                .to_string(),
        );
        rebuild_execution_receipt_index(&mut connection, receipts_file, warnings)?;
        return Ok(());
    }

    if !execution_receipt_prefix_tail_matches(receipts_file, &cursor)? {
        warnings.push(
            "runtime execution receipt ledger prefix no longer matches the index cursor; rebuilding receipt index"
                .to_string(),
        );
        rebuild_execution_receipt_index(&mut connection, receipts_file, warnings)?;
        return Ok(());
    }

    update_execution_receipt_index_from_cursor(
        &mut connection,
        receipts_file,
        cursor,
        false,
        warnings,
        "refreshing",
    )?;
    Ok(())
}

fn rebuild_execution_receipt_index(
    connection: &mut Connection,
    receipts_file: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<()> {
    match fs::metadata(receipts_file) {
        Ok(metadata) if metadata.is_file() => {
            update_execution_receipt_index_from_cursor(
                connection,
                receipts_file,
                ExecutionReceiptLedgerCursor::default(),
                true,
                warnings,
                "rebuilding",
            )?;
        }
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "runtime execution receipt ledger path is not a file: {}",
                    receipts_file.display()
                ),
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            reset_execution_receipt_index(connection)?;
        }
        Err(error) => return Err(error),
    }
    Ok(())
}

fn update_execution_receipt_index_from_cursor(
    connection: &mut Connection,
    receipts_file: &Path,
    cursor: ExecutionReceiptLedgerCursor,
    replace_existing: bool,
    warnings: &mut Vec<String>,
    phase: &str,
) -> io::Result<ExecutionReceiptLedgerCursor> {
    let file = File::open(receipts_file)?;
    let file_len = file.metadata()?.len();
    if cursor.offset_bytes > file_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "runtime execution receipt index cursor {} exceeds source length {}",
                cursor.offset_bytes, file_len
            ),
        ));
    }
    let mut reader = BufReader::new(file);
    reader.seek(SeekFrom::Start(cursor.offset_bytes))?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(io::Error::other)?;
    if replace_existing {
        transaction
            .execute_batch(
                "DELETE FROM execution_prepared_receipts;
                 DELETE FROM execution_prepared_queue_ids;
                 DELETE FROM execution_receipt_evidence;
                 DELETE FROM execution_latest_prepared_execution_dir;",
            )
            .map_err(io::Error::other)?;
    }

    let mut refreshed = cursor;
    loop {
        let mut line = String::new();
        let bytes_read = reader.read_line(&mut line)?;
        if bytes_read == 0 {
            break;
        }
        let complete_line = line.ends_with('\n');
        let trimmed = line.trim();
        if !complete_line && !trimmed.is_empty() && serde_json::from_str::<Value>(trimmed).is_err()
        {
            // Leave an interrupted append untouched. Advancing through this
            // partial record would permanently split a future completed JSONL
            // object into two invalid records.
            break;
        }

        refreshed.line_number = refreshed.line_number.checked_add(1).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "runtime execution receipt line number overflow",
            )
        })?;
        refreshed.offset_bytes = refreshed
            .offset_bytes
            .checked_add(u64::try_from(bytes_read).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "runtime execution receipt line length exceeds u64",
                )
            })?)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "runtime execution receipt byte offset overflow",
                )
            })?;
        if trimmed.is_empty() {
            continue;
        }
        let value = match serde_json::from_str::<Value>(trimmed) {
            Ok(value) => value,
            Err(error) => {
                warnings.push(format!(
                    "runtime execution receipt line {} is not valid JSON while {phase} receipt index: {error}",
                    refreshed.line_number
                ));
                continue;
            }
        };
        apply_execution_receipt_to_index(&transaction, &value, refreshed.line_number, warnings)?;
    }
    let source_metadata = reader.get_ref().metadata()?;
    refreshed.source_modified_at_unix_nanos = file_modified_at_unix_nanos(&source_metadata);
    refreshed.prefix_tail_fingerprint =
        execution_receipt_prefix_tail_fingerprint(receipts_file, refreshed.offset_bytes)?;
    write_execution_receipt_cursor(&transaction, &refreshed)?;
    transaction.commit().map_err(io::Error::other)?;
    Ok(refreshed)
}

fn apply_execution_receipt_to_index(
    transaction: &Transaction<'_>,
    value: &Value,
    source_line: u64,
    warnings: &mut Vec<String>,
) -> io::Result<()> {
    if string_field(value, &["status"]) == Some("prepared")
        && let Some(queue_id) = string_field(value, &["queueId", "queue_id"])
    {
        transaction
            .execute(
                "INSERT INTO execution_prepared_queue_ids (queue_id) VALUES (?1) \
                 ON CONFLICT(queue_id) DO NOTHING",
                [queue_id],
            )
            .map_err(io::Error::other)?;
    }

    if let Some(queue_id) = string_field(value, &["queueId", "queue_id"]) {
        let status = bounded_text(
            string_field(value, &["status"]).unwrap_or("recorded"),
            MAX_EVIDENCE_STATUS_CHARS,
        );
        let reason = string_field(value, &["reason", "error"])
            .map(|value| bounded_text(value, MAX_EVIDENCE_REASON_CHARS));
        let at_ms = i64_field(value, &["atMs", "createdAtMs", "updatedAtMs"]);
        let source_line = sqlite_i64(source_line, "runtime execution receipt source line")?;
        transaction
            .execute(
                "INSERT INTO execution_receipt_evidence \
                 (queue_id, source_line, status, reason, at_ms) \
                 VALUES (?1, ?2, ?3, ?4, ?5) \
                 ON CONFLICT(queue_id, source_line) DO UPDATE SET \
                   status = excluded.status, reason = excluded.reason, at_ms = excluded.at_ms",
                params![queue_id, source_line, status, reason, at_ms],
            )
            .map_err(io::Error::other)?;
        transaction
            .execute(
                "DELETE FROM execution_receipt_evidence \
                 WHERE queue_id = ?1 AND source_line NOT IN ( \
                   SELECT source_line FROM execution_receipt_evidence \
                   WHERE queue_id = ?1 ORDER BY source_line DESC LIMIT ?2 \
                 )",
                params![queue_id, MAX_EVIDENCE_PER_QUEUE_ID],
            )
            .map_err(io::Error::other)?;
    }

    if string_field(value, &["status"]) == Some("prepared")
        && let Some(execution_dir) = string_field(value, &["executionDir", "execution_dir"])
    {
        let source_line = sqlite_i64(source_line, "runtime execution receipt source line")?;
        transaction
            .execute(
                "INSERT INTO execution_latest_prepared_execution_dir \
                 (singleton, execution_dir, source_line) VALUES (1, ?1, ?2) \
                 ON CONFLICT(singleton) DO UPDATE SET \
                   execution_dir = excluded.execution_dir, source_line = excluded.source_line \
                 WHERE excluded.source_line >= execution_latest_prepared_execution_dir.source_line",
                params![execution_dir, source_line],
            )
            .map_err(io::Error::other)?;
    }

    let receipt = match serde_json::from_value::<RuntimeExecutionReceipt>(value.clone()) {
        Ok(receipt) => receipt,
        Err(error) => {
            warnings.push(format!(
                "runtime execution receipt line {source_line} cannot be materialized: {error}"
            ));
            return Ok(());
        }
    };
    if receipt.status != RuntimeExecutionReceiptStatus::Prepared {
        return Ok(());
    }
    let Some(queue_id) = receipt.queue_id.clone() else {
        return Ok(());
    };
    let receipt_json = serde_json::to_string(&receipt).map_err(io::Error::other)?;
    let source_line = sqlite_i64(source_line, "runtime execution receipt source line")?;
    transaction
        .execute(
            "INSERT INTO execution_prepared_receipts (queue_id, receipt_json, source_line) \
             VALUES (?1, ?2, ?3) \
             ON CONFLICT(queue_id) DO UPDATE SET \
               receipt_json = excluded.receipt_json, source_line = excluded.source_line",
            params![queue_id, receipt_json, source_line],
        )
        .map_err(io::Error::other)?;
    Ok(())
}

fn read_prepared_execution_receipts(
    connection: &Connection,
    warnings: &mut Vec<String>,
) -> io::Result<HashMap<String, RuntimeExecutionReceipt>> {
    let mut statement = connection
        .prepare(
            "SELECT queue_id, receipt_json FROM execution_prepared_receipts ORDER BY queue_id ASC",
        )
        .map_err(io::Error::other)?;
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(io::Error::other)?;
    let mut receipts = HashMap::new();
    for row in rows {
        let (queue_id, receipt_json) = row.map_err(io::Error::other)?;
        match serde_json::from_str::<RuntimeExecutionReceipt>(&receipt_json) {
            Ok(receipt) if receipt.queue_id.as_deref() == Some(queue_id.as_str()) => {
                receipts.insert(queue_id, receipt);
            }
            Ok(_) => warnings.push(format!(
                "runtime execution receipt index has a queue-id mismatch for `{queue_id}`; ignoring stale row"
            )),
            Err(error) => warnings.push(format!(
                "runtime execution receipt index has invalid prepared receipt for `{queue_id}`; ignoring stale row: {error}"
            )),
        }
    }
    Ok(receipts)
}

fn execution_receipt_index_has_records(connection: &Connection) -> io::Result<bool> {
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM execution_prepared_receipts LIMIT 1) \
             OR EXISTS(SELECT 1 FROM execution_prepared_queue_ids LIMIT 1) \
             OR EXISTS(SELECT 1 FROM execution_receipt_evidence LIMIT 1) \
             OR EXISTS(SELECT 1 FROM execution_latest_prepared_execution_dir LIMIT 1)",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map(|value| value != 0)
        .map_err(io::Error::other)
}

fn reset_execution_receipt_index(connection: &mut Connection) -> io::Result<()> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(io::Error::other)?;
    transaction
        .execute_batch(
            "DELETE FROM execution_prepared_receipts;
             DELETE FROM execution_prepared_queue_ids;
             DELETE FROM execution_receipt_evidence;
             DELETE FROM execution_latest_prepared_execution_dir;",
        )
        .map_err(io::Error::other)?;
    write_execution_receipt_cursor(&transaction, &ExecutionReceiptLedgerCursor::default())?;
    transaction.commit().map_err(io::Error::other)
}

fn open_or_create_execution_receipt_index(queue_dir: &Path) -> io::Result<Connection> {
    fs::create_dir_all(queue_dir)?;
    let index_file = runtime_execution_receipt_index_file(queue_dir);
    let mut connection = Connection::open(index_file).map_err(io::Error::other)?;
    initialize_execution_receipt_index(&mut connection)?;
    Ok(connection)
}

fn initialize_execution_receipt_index(connection: &mut Connection) -> io::Result<()> {
    connection
        .execute_batch(
            "PRAGMA foreign_keys = ON;
             PRAGMA busy_timeout = 5000;
             CREATE TABLE IF NOT EXISTS execution_receipt_index_meta (
               singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
               schema TEXT NOT NULL,
               revision INTEGER NOT NULL,
               offset_bytes INTEGER NOT NULL,
               line_number INTEGER NOT NULL,
               source_modified_at_unix_nanos TEXT,
               prefix_tail_fingerprint TEXT
             );",
        )
        .map_err(io::Error::other)?;
    let metadata = connection
        .query_row(
            "SELECT schema, revision FROM execution_receipt_index_meta WHERE singleton = 1",
            [],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional();
    match metadata {
        Ok(Some((schema, revision)))
            if schema == EXECUTION_RECEIPT_INDEX_SCHEMA
                && revision == EXECUTION_RECEIPT_INDEX_REVISION =>
        {
            create_execution_receipt_index_tables(connection)
        }
        Ok(None) => {
            create_execution_receipt_index_tables(connection)?;
            write_initial_execution_receipt_cursor(connection)
        }
        Ok(Some(_)) | Err(_) => reset_execution_receipt_index_schema(connection),
    }
}

fn reset_execution_receipt_index_schema(connection: &mut Connection) -> io::Result<()> {
    connection
        .execute_batch(
            "DROP INDEX IF EXISTS execution_receipt_evidence_queue_line_idx;
             DROP TABLE IF EXISTS execution_receipt_evidence;
             DROP TABLE IF EXISTS execution_prepared_receipts;
             DROP TABLE IF EXISTS execution_prepared_queue_ids;
             DROP TABLE IF EXISTS execution_latest_prepared_execution_dir;
             DROP TABLE IF EXISTS execution_receipt_index_meta;",
        )
        .map_err(io::Error::other)?;
    connection
        .execute_batch(
            "CREATE TABLE execution_receipt_index_meta (
               singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
               schema TEXT NOT NULL,
               revision INTEGER NOT NULL,
               offset_bytes INTEGER NOT NULL,
               line_number INTEGER NOT NULL,
               source_modified_at_unix_nanos TEXT,
               prefix_tail_fingerprint TEXT
             );",
        )
        .map_err(io::Error::other)?;
    create_execution_receipt_index_tables(connection)?;
    write_initial_execution_receipt_cursor(connection)
}

fn create_execution_receipt_index_tables(connection: &Connection) -> io::Result<()> {
    connection
        .execute_batch(
            "CREATE TABLE IF NOT EXISTS execution_prepared_receipts (
               queue_id TEXT PRIMARY KEY,
               receipt_json TEXT NOT NULL,
               source_line INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS execution_prepared_queue_ids (
               queue_id TEXT PRIMARY KEY
             );
             CREATE TABLE IF NOT EXISTS execution_latest_prepared_execution_dir (
               singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
               execution_dir TEXT NOT NULL,
               source_line INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS execution_receipt_evidence (
               queue_id TEXT NOT NULL,
               source_line INTEGER NOT NULL,
               status TEXT NOT NULL,
               reason TEXT,
               at_ms INTEGER,
               PRIMARY KEY (queue_id, source_line)
             );
             CREATE INDEX IF NOT EXISTS execution_receipt_evidence_queue_line_idx
               ON execution_receipt_evidence(queue_id, source_line DESC);",
        )
        .map_err(io::Error::other)
}

fn write_initial_execution_receipt_cursor(connection: &Connection) -> io::Result<()> {
    connection
        .execute(
            "INSERT INTO execution_receipt_index_meta \
             (singleton, schema, revision, offset_bytes, line_number, \
              source_modified_at_unix_nanos, prefix_tail_fingerprint) \
             VALUES (1, ?1, ?2, 0, 0, NULL, NULL)",
            params![
                EXECUTION_RECEIPT_INDEX_SCHEMA,
                EXECUTION_RECEIPT_INDEX_REVISION
            ],
        )
        .map_err(io::Error::other)?;
    Ok(())
}

fn read_execution_receipt_cursor(
    connection: &Connection,
) -> io::Result<Option<ExecutionReceiptLedgerCursor>> {
    let row = connection
        .query_row(
            "SELECT offset_bytes, line_number, source_modified_at_unix_nanos, \
                    prefix_tail_fingerprint \
             FROM execution_receipt_index_meta WHERE singleton = 1",
            [],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            },
        )
        .optional()
        .map_err(io::Error::other)?;
    let Some((offset_bytes, line_number, source_modified_at_unix_nanos, prefix_tail_fingerprint)) =
        row
    else {
        return Ok(None);
    };
    if offset_bytes < 0 || line_number < 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "runtime execution receipt index cursor cannot be negative",
        ));
    }
    Ok(Some(ExecutionReceiptLedgerCursor {
        offset_bytes: u64::try_from(offset_bytes).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "runtime execution receipt index offset cannot be represented",
            )
        })?,
        line_number: u64::try_from(line_number).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "runtime execution receipt index line number cannot be represented",
            )
        })?,
        source_modified_at_unix_nanos: parse_optional_u128(
            source_modified_at_unix_nanos,
            "source_modified_at_unix_nanos",
        )?,
        prefix_tail_fingerprint: parse_optional_u64(
            prefix_tail_fingerprint,
            "prefix_tail_fingerprint",
        )?,
    }))
}

fn write_execution_receipt_cursor(
    transaction: &Transaction<'_>,
    cursor: &ExecutionReceiptLedgerCursor,
) -> io::Result<()> {
    transaction
        .execute(
            "INSERT INTO execution_receipt_index_meta \
             (singleton, schema, revision, offset_bytes, line_number, \
              source_modified_at_unix_nanos, prefix_tail_fingerprint) \
             VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6) \
             ON CONFLICT(singleton) DO UPDATE SET \
               schema = excluded.schema, revision = excluded.revision, \
               offset_bytes = excluded.offset_bytes, line_number = excluded.line_number, \
               source_modified_at_unix_nanos = excluded.source_modified_at_unix_nanos, \
               prefix_tail_fingerprint = excluded.prefix_tail_fingerprint",
            params![
                EXECUTION_RECEIPT_INDEX_SCHEMA,
                EXECUTION_RECEIPT_INDEX_REVISION,
                sqlite_i64(cursor.offset_bytes, "runtime execution receipt byte offset")?,
                sqlite_i64(cursor.line_number, "runtime execution receipt line number")?,
                cursor
                    .source_modified_at_unix_nanos
                    .map(|value| value.to_string()),
                cursor
                    .prefix_tail_fingerprint
                    .map(|value| value.to_string()),
            ],
        )
        .map_err(io::Error::other)?;
    Ok(())
}

fn parse_optional_u128(value: Option<String>, field: &str) -> io::Result<Option<u128>> {
    value
        .map(|value| {
            value.parse::<u128>().map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("runtime execution receipt index {field} is invalid: {error}"),
                )
            })
        })
        .transpose()
}

fn parse_optional_u64(value: Option<String>, field: &str) -> io::Result<Option<u64>> {
    value
        .map(|value| {
            value.parse::<u64>().map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("runtime execution receipt index {field} is invalid: {error}"),
                )
            })
        })
        .transpose()
}

fn sqlite_i64(value: u64, field: &str) -> io::Result<i64> {
    i64::try_from(value).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{field} exceeds the SQLite integer range"),
        )
    })
}

fn execution_receipt_prefix_tail_matches(
    receipts_file: &Path,
    cursor: &ExecutionReceiptLedgerCursor,
) -> io::Result<bool> {
    Ok(
        execution_receipt_prefix_tail_fingerprint(receipts_file, cursor.offset_bytes)?
            == cursor.prefix_tail_fingerprint,
    )
}

fn execution_receipt_prefix_tail_fingerprint(
    receipts_file: &Path,
    offset_bytes: u64,
) -> io::Result<Option<u64>> {
    if offset_bytes == 0 {
        return Ok(None);
    }
    let mut file = File::open(receipts_file)?;
    let start = offset_bytes.saturating_sub(RECEIPT_TAIL_FINGERPRINT_BYTES);
    file.seek(SeekFrom::Start(start))?;
    let mut remaining = offset_bytes.saturating_sub(start);
    let mut buffer = [0_u8; RECEIPT_TAIL_FINGERPRINT_BYTES as usize];
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    while remaining > 0 {
        let chunk_len = remaining.min(buffer.len() as u64) as usize;
        file.read_exact(&mut buffer[..chunk_len])?;
        for byte in &buffer[..chunk_len] {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        remaining = remaining.saturating_sub(chunk_len as u64);
    }
    Ok(Some(hash))
}

fn file_modified_at_unix_nanos(metadata: &fs::Metadata) -> Option<u128> {
    metadata
        .modified()
        .ok()
        .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos())
}

fn string_field<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a str> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(Value::as_str))
}

fn i64_field(value: &Value, keys: &[&str]) -> Option<i64> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(Value::as_i64))
}

fn bounded_text(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::fs::{self, OpenOptions};
    use std::io::Write;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_queue_dir(name: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "agent-harness-execution-receipt-index-{name}-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let queue_dir = root.join("state").join("runtime-queue");
        fs::create_dir_all(&queue_dir).unwrap();
        queue_dir
    }

    fn execution_receipt_line(queue_id: &str, status: &str, reason: &str, at_ms: i64) -> String {
        json!({
            "queueId": queue_id,
            "status": status,
            "executionDir": null,
            "promptBundleJson": null,
            "promptMarkdown": null,
            "inboundMediaArtifacts": [],
            "reason": reason,
            "atMs": at_ms,
        })
        .to_string()
    }

    #[test]
    fn returns_the_latest_prepared_receipt_with_an_execution_dir_across_queues() {
        let queue_dir = temp_queue_dir("latest-prepared-execution-dir");
        let receipts_file = queue_dir.join("execution-receipts.jsonl");
        fs::write(
            &receipts_file,
            [
                json!({
                    "queueId": "turn:alpha",
                    "status": "prepared",
                    "executionDir": "C:/runtime/first",
                    "promptBundleJson": null,
                    "promptMarkdown": null,
                    "inboundMediaArtifacts": [],
                    "reason": "first",
                }),
                json!({
                    "queueId": "turn:alpha",
                    "status": "prepared",
                    "executionDir": null,
                    "promptBundleJson": null,
                    "promptMarkdown": null,
                    "inboundMediaArtifacts": [],
                    "reason": "without directory",
                }),
                json!({
                    "queueId": "turn:beta",
                    "status": "prepared",
                    "executionDir": "C:/runtime/latest",
                    "promptBundleJson": null,
                    "promptMarkdown": null,
                    "inboundMediaArtifacts": [],
                    "reason": "latest usable",
                }),
            ]
            .into_iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join("\n")
                + "\n",
        )
        .unwrap();

        let mut warnings = Vec::new();
        assert_eq!(
            latest_prepared_execution_dir_from_index(&queue_dir, &mut warnings).unwrap(),
            Some(PathBuf::from("C:/runtime/latest"))
        );
        assert!(warnings.is_empty());

        let _ = fs::remove_dir_all(queue_dir.ancestors().nth(2).unwrap());
    }

    #[test]
    fn materializes_prepared_receipts_and_keeps_bounded_exact_evidence() {
        let queue_dir = temp_queue_dir("materializes-prepared-and-exact-evidence");
        let receipts_file = queue_dir.join("execution-receipts.jsonl");
        fs::write(
            &receipts_file,
            [
                execution_receipt_line("turn:alpha", "prepared", "first prepared", 10),
                execution_receipt_line("turn:other", "prepared", "unrelated", 11),
                execution_receipt_line("turn:alpha", "no-pending-item", "not pending", 12),
            ]
            .join("\n")
                + "\n",
        )
        .unwrap();

        let mut warnings = Vec::new();
        let prepared = prepared_execution_receipts_from_index(&queue_dir, &mut warnings).unwrap();
        assert!(warnings.is_empty());
        assert_eq!(prepared.len(), 2);
        assert_eq!(prepared["turn:alpha"].reason, "first prepared");
        assert!(runtime_execution_receipt_index_file(&queue_dir).is_file());

        let mut file = OpenOptions::new()
            .append(true)
            .open(&receipts_file)
            .unwrap();
        writeln!(
            file,
            "{}",
            execution_receipt_line("turn:alpha", "lease-busy", "lease busy", 13)
        )
        .unwrap();
        writeln!(
            file,
            "{}",
            execution_receipt_line("turn:alpha", "already-prepared", "already prepared", 14)
        )
        .unwrap();

        let queue_ids = BTreeSet::from(["turn:alpha".to_string(), "turn:missing".to_string()]);
        let evidence =
            execution_receipt_evidence_for_queue_ids(&queue_dir, &queue_ids, &mut warnings)
                .unwrap();
        assert_eq!(evidence.len(), 3);
        assert!(
            evidence
                .iter()
                .all(|record| record.queue_id == "turn:alpha")
        );
        assert_eq!(
            evidence
                .iter()
                .map(|record| record.status.as_str())
                .collect::<Vec<_>>(),
            vec!["no-pending-item", "lease-busy", "already-prepared"]
        );
        assert_eq!(evidence.last().and_then(|record| record.at_ms), Some(14));
        assert_eq!(
            evidence.last().and_then(|record| record.reason.as_deref()),
            Some("already prepared")
        );

        let _ = fs::remove_dir_all(queue_dir.ancestors().nth(2).unwrap());
    }

    #[test]
    fn keeps_sparse_legacy_prepared_receipts_for_existence_guards() {
        let queue_dir = temp_queue_dir("sparse-prepared-guard");
        let receipts_file = queue_dir.join("execution-receipts.jsonl");
        fs::write(
            &receipts_file,
            "{\"queueId\":\"turn:legacy\",\"status\":\"prepared\"}\n",
        )
        .unwrap();

        let mut warnings = Vec::new();
        assert!(
            has_prepared_execution_receipt_from_index(&queue_dir, "turn:legacy", &mut warnings)
                .unwrap()
        );
        assert!(
            !has_prepared_execution_receipt_from_index(&queue_dir, "turn:missing", &mut warnings)
                .unwrap()
        );
        assert!(
            warnings
                .iter()
                .any(|warning| warning.contains("cannot be materialized"))
        );

        let _ = fs::remove_dir_all(queue_dir.ancestors().nth(2).unwrap());
    }

    #[test]
    fn rebuilds_after_truncation_and_skips_malformed_complete_records() {
        let queue_dir = temp_queue_dir("rebuilds-after-truncation");
        let receipts_file = queue_dir.join("execution-receipts.jsonl");
        let old_reason = "old prepared ".repeat(128);
        fs::write(
            &receipts_file,
            execution_receipt_line("turn:old", "prepared", &old_reason, 1) + "\n",
        )
        .unwrap();

        let mut warnings = Vec::new();
        assert!(
            prepared_execution_receipts_from_index(&queue_dir, &mut warnings)
                .unwrap()
                .contains_key("turn:old")
        );

        fs::write(
            &receipts_file,
            format!(
                "not-json\n{}\n",
                execution_receipt_line("turn:new", "prepared", "new prepared", 2)
            ),
        )
        .unwrap();

        warnings.clear();
        let rebuilt = prepared_execution_receipts_from_index(&queue_dir, &mut warnings).unwrap();
        assert!(!rebuilt.contains_key("turn:old"));
        assert_eq!(rebuilt["turn:new"].reason, "new prepared");
        assert!(
            warnings
                .iter()
                .any(|warning| warning.contains("line 1") && warning.contains("not valid JSON"))
        );

        let _ = fs::remove_dir_all(queue_dir.ancestors().nth(2).unwrap());
    }

    #[test]
    fn waits_for_an_unterminated_jsonl_tail_before_materializing_it() {
        let queue_dir = temp_queue_dir("unterminated-tail");
        let receipts_file = queue_dir.join("execution-receipts.jsonl");
        let completed = execution_receipt_line("turn:tail", "prepared", "tail prepared", 7);
        let split_at = completed.len() / 2;
        fs::write(&receipts_file, &completed[..split_at]).unwrap();

        let mut warnings = Vec::new();
        let queue_ids = BTreeSet::from(["turn:tail".to_string()]);
        assert!(
            execution_receipt_evidence_for_queue_ids(&queue_dir, &queue_ids, &mut warnings)
                .unwrap()
                .is_empty()
        );
        assert!(warnings.is_empty());

        let mut file = OpenOptions::new()
            .append(true)
            .open(&receipts_file)
            .unwrap();
        writeln!(file, "{}", &completed[split_at..]).unwrap();

        let evidence =
            execution_receipt_evidence_for_queue_ids(&queue_dir, &queue_ids, &mut warnings)
                .unwrap();
        assert_eq!(evidence.len(), 1);
        assert_eq!(evidence[0].queue_id, "turn:tail");
        assert_eq!(evidence[0].reason.as_deref(), Some("tail prepared"));

        let _ = fs::remove_dir_all(queue_dir.ancestors().nth(2).unwrap());
    }
}
