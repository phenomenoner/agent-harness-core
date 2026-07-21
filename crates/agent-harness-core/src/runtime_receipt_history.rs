//! Durable cold storage for compacted runtime queue receipt summaries.
//!
//! The JSONL receipt ledger remains the hot, mutable source used by queue
//! coordination.  Compaction stages a small SQLite transaction here before it
//! swaps the hot ledger, then marks that transaction committed only after the
//! swap is durable.  Readers intentionally join through the committed state,
//! so a crash can never expose a summary for a receipt ledger that was not
//! actually replaced.

use rusqlite::{
    Connection, ErrorCode, OpenFlags, OptionalExtension, TransactionBehavior, params,
    params_from_iter,
};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

pub(crate) const RUNTIME_QUEUE_RECEIPT_HISTORY_SCHEMA: &str =
    "agent-harness.runtime-queue-receipt-history.v1";
pub(crate) const RUNTIME_RUN_ONCE_RECEIPT_SCHEMA: &str = "agent-harness.runtime-run-once.v1";

const HISTORY_FILE_NAME: &str = "run-once-receipt-history.sqlite";
const HISTORY_STATE_STAGED: &str = "staged";
const HISTORY_STATE_COMMITTED: &str = "committed";
const MAX_REASON_CHARS: usize = 512;
const MAX_DISPOSITION_JSON_CHARS: usize = 4096;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeQueueReceiptHistoryStaging {
    pub(crate) store_file: PathBuf,
    pub(crate) transaction_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeQueueTerminalHistoryRecord {
    pub(crate) row_id: i64,
    pub(crate) queue_id: String,
    pub(crate) trace_id: Option<String>,
    pub(crate) session_key: Option<String>,
    pub(crate) status: String,
    pub(crate) reason: Option<String>,
    pub(crate) terminal_control_source: Option<String>,
    pub(crate) runtime_class: Option<String>,
    pub(crate) origin: Option<String>,
    pub(crate) transcript_file: Option<PathBuf>,
    pub(crate) occurred_at_ms: Option<i64>,
    pub(crate) compacted_at_ms: i64,
    pub(crate) terminal_disposition: Option<String>,
    pub(crate) child_queue_id: Option<String>,
    pub(crate) continuation_index: Option<u64>,
    pub(crate) task_disposition: Option<String>,
    pub(crate) effect_disposition: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct RuntimeQueueTerminalHistorySummary {
    /// Number of terminal receipt events removed from the hot ledger.
    pub(crate) terminal_records: usize,
    /// Number of distinct queue IDs with a committed terminal summary.
    pub(crate) terminal_queue_ids: usize,
}

#[derive(Debug, Clone)]
struct TerminalHistoryCandidate {
    queue_id: String,
    trace_id: Option<String>,
    session_key: Option<String>,
    status: String,
    reason: Option<String>,
    terminal_control_source: Option<String>,
    runtime_class: Option<String>,
    origin: Option<String>,
    transcript_file: Option<PathBuf>,
    occurred_at_ms: Option<i64>,
    terminal_disposition: Option<String>,
    child_queue_id: Option<String>,
    continuation_index: Option<u64>,
    task_disposition: Option<String>,
    effect_disposition: Option<String>,
}

/// Returns the SQLite history store colocated with the runtime queue ledgers.
pub(crate) fn runtime_queue_receipt_history_file(queue_dir: &Path) -> PathBuf {
    queue_dir.join(HISTORY_FILE_NAME)
}

/// Stages a history transaction from a receipt ledger replacement.
///
/// The caller must commit this staging record only after the corresponding
/// receipt-ledger replacement is verified.  Before that point the rows are
/// deliberately invisible to every read API in this module.
pub(crate) fn stage_runtime_queue_receipt_history(
    queue_dir: &Path,
    transaction_id: &str,
    original: &[u8],
    snapshot: &[u8],
    retained_queue_ids: &HashSet<String>,
    now_ms: i64,
) -> io::Result<RuntimeQueueReceiptHistoryStaging> {
    if transaction_id.trim().is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "runtime receipt history transaction id must not be empty",
        ));
    }

    let original_records = parse_jsonl_records(original);
    let snapshot_records = parse_jsonl_records(snapshot);
    let original_status_counts = status_counts(&original_records);
    let snapshot_status_counts = status_counts(&snapshot_records);
    let status_deltas = status_deltas(&original_status_counts, &snapshot_status_counts)?;
    let terminal_records = latest_terminal_records(&original_records, retained_queue_ids);
    let store_file = runtime_queue_receipt_history_file(queue_dir);
    let mut connection = open_or_create_history_store(&store_file)?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(io::Error::other)?;

    transaction
        .execute(
            "INSERT INTO runtime_receipt_history_transactions \
             (transaction_id, state, created_at_ms, committed_at_ms) \
             VALUES (?1, ?2, ?3, NULL)",
            params![transaction_id, HISTORY_STATE_STAGED, now_ms],
        )
        .map_err(io::Error::other)?;

    for (status, count_delta) in status_deltas {
        if count_delta == 0 {
            continue;
        }
        let count_delta = i64::try_from(count_delta).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("runtime receipt history delta for `{status}` exceeds SQLite range"),
            )
        })?;
        transaction
            .execute(
                "INSERT INTO runtime_receipt_history_status_deltas \
                 (transaction_id, status, count_delta) VALUES (?1, ?2, ?3)",
                params![transaction_id, status, count_delta],
            )
            .map_err(io::Error::other)?;
    }

    for record in terminal_records.into_values() {
        transaction
            .execute(
                "INSERT INTO runtime_receipt_terminal_history \
                 (transaction_id, queue_id, trace_id, session_key, status, reason, \
                  terminal_control_source, runtime_class, origin, transcript_file, \
                  occurred_at_ms, compacted_at_ms, terminal_disposition, child_queue_id, \
                  continuation_index, task_disposition, effect_disposition) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
                params![
                    transaction_id,
                    record.queue_id,
                    record.trace_id,
                    record.session_key,
                    record.status,
                    record.reason,
                    record.terminal_control_source,
                    record.runtime_class,
                    record.origin,
                    record
                        .transcript_file
                        .as_ref()
                        .map(|path| path.to_string_lossy().into_owned()),
                    record.occurred_at_ms,
                    now_ms,
                    record.terminal_disposition,
                    record.child_queue_id,
                    record.continuation_index.and_then(|value| i64::try_from(value).ok()),
                    record.task_disposition,
                    record.effect_disposition,
                ],
            )
            .map_err(io::Error::other)?;
    }

    transaction.commit().map_err(io::Error::other)?;
    Ok(RuntimeQueueReceiptHistoryStaging {
        store_file,
        transaction_id: transaction_id.to_string(),
    })
}

/// Atomically exposes a previously staged transaction to history readers.
/// Repeating a successful commit is intentionally idempotent for crash
/// recovery after the hot ledger has already been swapped.
pub(crate) fn commit_runtime_queue_receipt_history(
    staging: &RuntimeQueueReceiptHistoryStaging,
    committed_at_ms: i64,
) -> io::Result<()> {
    let mut connection = open_existing_history_store(&staging.store_file)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "runtime receipt history store is missing while committing transaction `{}`",
                staging.transaction_id
            ),
        )
    })?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(io::Error::other)?;
    let changed = transaction
        .execute(
            "UPDATE runtime_receipt_history_transactions \
             SET state = ?1, committed_at_ms = ?2 \
             WHERE transaction_id = ?3 AND state = ?4",
            params![
                HISTORY_STATE_COMMITTED,
                committed_at_ms,
                staging.transaction_id,
                HISTORY_STATE_STAGED
            ],
        )
        .map_err(io::Error::other)?;

    if changed == 0 {
        let state: Option<String> = transaction
            .query_row(
                "SELECT state FROM runtime_receipt_history_transactions \
                 WHERE transaction_id = ?1",
                params![staging.transaction_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(io::Error::other)?;
        match state.as_deref() {
            Some(HISTORY_STATE_COMMITTED) => {}
            Some(other) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "runtime receipt history transaction `{}` has unexpected state `{other}`",
                        staging.transaction_id
                    ),
                ));
            }
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!(
                        "runtime receipt history transaction `{}` was not staged",
                        staging.transaction_id
                    ),
                ));
            }
        }
    }
    transaction.commit().map_err(io::Error::other)
}

/// Removes a staged transaction after its paired hot-ledger swap is abandoned.
/// A committed transaction is never removed by this helper.
pub(crate) fn discard_runtime_queue_receipt_history(
    staging: &RuntimeQueueReceiptHistoryStaging,
) -> io::Result<()> {
    let Some(mut connection) = open_existing_history_store(&staging.store_file)? else {
        return Ok(());
    };
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(io::Error::other)?;
    transaction
        .execute(
            "DELETE FROM runtime_receipt_history_transactions \
             WHERE transaction_id = ?1 AND state = ?2",
            params![staging.transaction_id, HISTORY_STATE_STAGED],
        )
        .map_err(io::Error::other)?;
    transaction.commit().map_err(io::Error::other)
}

/// Deletes orphaned staged rows after compaction-marker recovery has completed.
///
/// Callers must first resolve every receipt-ledger marker that could still
/// reference a staged transaction.  This function intentionally makes no
/// guess about marker ownership, which keeps the storage layer independent of
/// the JSONL recovery protocol.
pub(crate) fn cleanup_staged_runtime_queue_receipt_history(queue_dir: &Path) -> io::Result<usize> {
    let store_file = runtime_queue_receipt_history_file(queue_dir);
    let Some(mut connection) = open_existing_history_store(&store_file)? else {
        return Ok(0);
    };
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(io::Error::other)?;
    let removed = transaction
        .execute(
            "DELETE FROM runtime_receipt_history_transactions WHERE state = ?1",
            params![HISTORY_STATE_STAGED],
        )
        .map_err(io::Error::other)?;
    transaction.commit().map_err(io::Error::other)?;
    Ok(removed)
}

/// Returns cumulative status deltas for only committed receipt replacements.
pub(crate) fn read_runtime_queue_receipt_history_status_counts(
    queue_dir: &Path,
) -> io::Result<BTreeMap<String, usize>> {
    let store_file = runtime_queue_receipt_history_file(queue_dir);
    let Some(connection) = open_existing_history_store(&store_file)? else {
        return Ok(BTreeMap::new());
    };
    let mut statement = connection
        .prepare(
            "SELECT delta.status, SUM(delta.count_delta) \
             FROM runtime_receipt_history_status_deltas AS delta \
             INNER JOIN runtime_receipt_history_transactions AS tx \
                 ON tx.transaction_id = delta.transaction_id \
             WHERE tx.state = ?1 \
             GROUP BY delta.status",
        )
        .map_err(io::Error::other)?;
    let rows = statement
        .query_map(params![HISTORY_STATE_COMMITTED], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })
        .map_err(io::Error::other)?;
    let mut counts = BTreeMap::new();
    for row in rows {
        let (status, count) = row.map_err(io::Error::other)?;
        let count = usize::try_from(count).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("runtime receipt history count for `{status}` is invalid"),
            )
        })?;
        counts.insert(status, count);
    }
    Ok(counts)
}

/// Returns committed terminal aggregates without materializing the cold history.
/// Diagnostics use this to preserve all-time terminal totals while keeping live
/// pending/open state entirely on the hot index.
pub(crate) fn read_runtime_queue_terminal_history_summary(
    queue_dir: &Path,
) -> io::Result<RuntimeQueueTerminalHistorySummary> {
    let store_file = runtime_queue_receipt_history_file(queue_dir);
    let Some(connection) = open_existing_history_store(&store_file)? else {
        return Ok(RuntimeQueueTerminalHistorySummary::default());
    };
    const TERMINAL_STATUSES: [&str; 7] = [
        "completed",
        "timeout",
        "failed-terminal",
        "canceled",
        "skipped",
        "dead-letter",
        "suppressed",
    ];
    let placeholders = std::iter::repeat_n("?", TERMINAL_STATUSES.len())
        .collect::<Vec<_>>()
        .join(", ");
    let terminal_records_sql = format!(
        "SELECT COALESCE(SUM(delta.count_delta), 0) \
         FROM runtime_receipt_history_status_deltas AS delta \
         INNER JOIN runtime_receipt_history_transactions AS tx \
             ON tx.transaction_id = delta.transaction_id \
         WHERE tx.state = ? AND delta.status IN ({placeholders})"
    );
    let mut terminal_record_args = Vec::with_capacity(1 + TERMINAL_STATUSES.len());
    terminal_record_args.push(HISTORY_STATE_COMMITTED.to_string());
    terminal_record_args.extend(TERMINAL_STATUSES.into_iter().map(ToString::to_string));
    let terminal_records: i64 = connection
        .query_row(
            &terminal_records_sql,
            params_from_iter(terminal_record_args.iter()),
            |row| row.get(0),
        )
        .map_err(io::Error::other)?;
    let terminal_queue_ids: i64 = connection
        .query_row(
            "SELECT COUNT(DISTINCT history.queue_id) \
             FROM runtime_receipt_terminal_history AS history \
             INNER JOIN runtime_receipt_history_transactions AS tx \
                 ON tx.transaction_id = history.transaction_id \
             WHERE tx.state = ?1",
            params![HISTORY_STATE_COMMITTED],
            |row| row.get(0),
        )
        .map_err(io::Error::other)?;
    Ok(RuntimeQueueTerminalHistorySummary {
        terminal_records: usize::try_from(terminal_records).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "runtime receipt history terminal-record aggregate is invalid",
            )
        })?,
        terminal_queue_ids: usize::try_from(terminal_queue_ids).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "runtime receipt history terminal-queue aggregate is invalid",
            )
        })?,
    })
}

/// Finds committed terminal summaries by any known queue, trace, or session
/// identifier.  An empty identifier set deliberately returns no rows rather
/// than accidentally loading the complete cold history on a hot path.
pub(crate) fn find_runtime_queue_terminal_history(
    queue_dir: &Path,
    identifiers: &BTreeSet<String>,
) -> io::Result<Vec<RuntimeQueueTerminalHistoryRecord>> {
    if identifiers.is_empty() {
        return Ok(Vec::new());
    }
    let store_file = runtime_queue_receipt_history_file(queue_dir);
    let Some(connection) = open_existing_history_store(&store_file)? else {
        return Ok(Vec::new());
    };
    let placeholders = std::iter::repeat_n("?", identifiers.len())
        .collect::<Vec<_>>()
        .join(", ");
    let optional_columns = terminal_history_optional_select_columns(&connection)?;
    let statement = format!(
        "SELECT history.row_id, history.queue_id, history.trace_id, history.session_key, \
                history.status, history.reason, history.terminal_control_source, \
                history.runtime_class, history.origin, history.transcript_file, \
                history.occurred_at_ms, history.compacted_at_ms, \
                {optional_columns} \
         FROM runtime_receipt_terminal_history AS history \
         INNER JOIN runtime_receipt_history_transactions AS tx \
             ON tx.transaction_id = history.transaction_id \
         WHERE tx.state = ? \
           AND (history.queue_id IN ({placeholders}) \
                OR history.trace_id IN ({placeholders}) \
                OR history.session_key IN ({placeholders})) \
         ORDER BY history.compacted_at_ms DESC, history.row_id DESC"
    );
    let mut values = Vec::with_capacity(1 + identifiers.len() * 3);
    values.push(HISTORY_STATE_COMMITTED.to_string());
    for _ in 0..3 {
        values.extend(identifiers.iter().cloned());
    }
    read_terminal_history_rows(&connection, &statement, values)
}

/// Finds exact committed cold-history rows without waiting behind a compaction
/// or another SQLite writer.
///
/// This is for user-visible typing/progress paths only. The store is opened
/// read-only and is never initialized, migrated, or created here. A busy or
/// locked snapshot returns `io::ErrorKind::WouldBlock`; callers should retain
/// their hot-state result and treat that as a conservative empty cold result.
pub(crate) fn find_runtime_queue_terminal_history_nonblocking(
    queue_dir: &Path,
    identifiers: &BTreeSet<String>,
) -> io::Result<Vec<RuntimeQueueTerminalHistoryRecord>> {
    if identifiers.is_empty()
        || identifiers
            .iter()
            .all(|identifier| identifier.trim().is_empty())
    {
        return Ok(Vec::new());
    }
    let store_file = runtime_queue_receipt_history_file(queue_dir);
    let Some(connection) = open_existing_history_store_read_only(&store_file)? else {
        return Ok(Vec::new());
    };
    let placeholders = std::iter::repeat_n("?", identifiers.len())
        .collect::<Vec<_>>()
        .join(", ");
    let optional_columns = terminal_history_optional_select_columns(&connection)?;
    let statement = format!(
        "SELECT history.row_id, history.queue_id, history.trace_id, history.session_key, \
                history.status, history.reason, history.terminal_control_source, \
                history.runtime_class, history.origin, history.transcript_file, \
                history.occurred_at_ms, history.compacted_at_ms, \
                {optional_columns} \
         FROM runtime_receipt_terminal_history AS history \
         INNER JOIN runtime_receipt_history_transactions AS tx \
             ON tx.transaction_id = history.transaction_id \
         WHERE tx.state = ? \
           AND (history.queue_id IN ({placeholders}) \
                OR history.trace_id IN ({placeholders}) \
                OR history.session_key IN ({placeholders})) \
         ORDER BY history.compacted_at_ms DESC, history.row_id DESC"
    );
    let mut values = Vec::with_capacity(1 + identifiers.len() * 3);
    values.push(HISTORY_STATE_COMMITTED.to_string());
    for _ in 0..3 {
        values.extend(identifiers.iter().cloned());
    }
    read_terminal_history_rows_nonblocking(&connection, &statement, values)
}

fn open_or_create_history_store(store_file: &Path) -> io::Result<Connection> {
    if let Some(parent) = store_file.parent() {
        fs::create_dir_all(parent)?;
    }
    let connection = Connection::open(store_file).map_err(io::Error::other)?;
    initialize_history_store(&connection)?;
    Ok(connection)
}

fn open_existing_history_store(store_file: &Path) -> io::Result<Option<Connection>> {
    match fs::metadata(store_file) {
        Ok(metadata) if metadata.is_file() => {}
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "runtime receipt history path is not a file: {}",
                    store_file.display()
                ),
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    }
    let connection = Connection::open(store_file).map_err(io::Error::other)?;
    initialize_history_store(&connection)?;
    Ok(Some(connection))
}

/// Opens only an already-committed history snapshot. Unlike the normal
/// existing-store helper, this never invokes schema initialization because a
/// user-visible read must not create, migrate, or wait for a database.
fn open_existing_history_store_read_only(store_file: &Path) -> io::Result<Option<Connection>> {
    match fs::metadata(store_file) {
        Ok(metadata) if metadata.is_file() => {}
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "runtime receipt history path is not a file: {}",
                    store_file.display()
                ),
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    }
    let connection = Connection::open_with_flags(store_file, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(nonblocking_history_io_error)?;
    connection
        .busy_timeout(Duration::from_millis(0))
        .map_err(nonblocking_history_io_error)?;
    let schema: Option<String> = connection
        .query_row(
            "SELECT schema FROM runtime_receipt_history_meta WHERE singleton = 1",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(nonblocking_history_io_error)?;
    match schema.as_deref() {
        Some(RUNTIME_QUEUE_RECEIPT_HISTORY_SCHEMA) => Ok(Some(connection)),
        None => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "runtime receipt history read-only snapshot has no schema metadata",
        )),
        Some(actual) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "unsupported runtime receipt history schema `{actual}`; expected `{}`",
                RUNTIME_QUEUE_RECEIPT_HISTORY_SCHEMA,
            ),
        )),
    }
}

fn nonblocking_history_io_error(error: rusqlite::Error) -> io::Error {
    if matches!(
        error.sqlite_error_code(),
        Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
    ) {
        io::Error::new(
            io::ErrorKind::WouldBlock,
            format!("runtime receipt history read-only snapshot is busy: {error}"),
        )
    } else {
        io::Error::other(error)
    }
}

fn initialize_history_store(connection: &Connection) -> io::Result<()> {
    connection
        .execute_batch(
            "PRAGMA foreign_keys = ON;
             PRAGMA busy_timeout = 5000;
             CREATE TABLE IF NOT EXISTS runtime_receipt_history_meta (
                 singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                 schema TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS runtime_receipt_history_transactions (
                 transaction_id TEXT PRIMARY KEY,
                 state TEXT NOT NULL CHECK (state IN ('staged', 'committed')),
                 created_at_ms INTEGER NOT NULL,
                 committed_at_ms INTEGER
             );
             CREATE TABLE IF NOT EXISTS runtime_receipt_history_status_deltas (
                 transaction_id TEXT NOT NULL,
                 status TEXT NOT NULL,
                 count_delta INTEGER NOT NULL CHECK (count_delta >= 0),
                 PRIMARY KEY (transaction_id, status),
                 FOREIGN KEY (transaction_id) REFERENCES runtime_receipt_history_transactions(transaction_id) ON DELETE CASCADE
             );
             CREATE TABLE IF NOT EXISTS runtime_receipt_terminal_history (
                 row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                 transaction_id TEXT NOT NULL,
                 queue_id TEXT NOT NULL,
                 trace_id TEXT,
                 session_key TEXT,
                 status TEXT NOT NULL,
                 reason TEXT,
                 terminal_control_source TEXT,
                 runtime_class TEXT,
                 origin TEXT,
                 transcript_file TEXT,
                 occurred_at_ms INTEGER,
                 compacted_at_ms INTEGER NOT NULL,
                 terminal_disposition TEXT,
                 child_queue_id TEXT,
                 continuation_index INTEGER,
                 task_disposition TEXT,
                 effect_disposition TEXT,
                 UNIQUE (transaction_id, queue_id),
                 FOREIGN KEY (transaction_id) REFERENCES runtime_receipt_history_transactions(transaction_id) ON DELETE CASCADE
             );
             CREATE INDEX IF NOT EXISTS runtime_receipt_terminal_history_queue_idx
                 ON runtime_receipt_terminal_history(queue_id);
             CREATE INDEX IF NOT EXISTS runtime_receipt_terminal_history_trace_idx
                 ON runtime_receipt_terminal_history(trace_id);
             CREATE INDEX IF NOT EXISTS runtime_receipt_terminal_history_session_idx
                 ON runtime_receipt_terminal_history(session_key);",
        )
        .map_err(io::Error::other)?;
    ensure_terminal_history_optional_columns(connection)?;
    let existing_schema: Option<String> = connection
        .query_row(
            "SELECT schema FROM runtime_receipt_history_meta WHERE singleton = 1",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(io::Error::other)?;
    match existing_schema.as_deref() {
        None => {
            connection
                .execute(
                    "INSERT INTO runtime_receipt_history_meta (singleton, schema) VALUES (1, ?1)",
                    params![RUNTIME_QUEUE_RECEIPT_HISTORY_SCHEMA],
                )
                .map_err(io::Error::other)?;
        }
        Some(RUNTIME_QUEUE_RECEIPT_HISTORY_SCHEMA) => {}
        Some(actual) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "unsupported runtime receipt history schema `{actual}`; expected `{}`",
                    RUNTIME_QUEUE_RECEIPT_HISTORY_SCHEMA,
                ),
            ));
        }
    }
    Ok(())
}

fn terminal_history_column_names(connection: &Connection) -> io::Result<HashSet<String>> {
    let mut statement = connection
        .prepare("PRAGMA table_info(runtime_receipt_terminal_history)")
        .map_err(io::Error::other)?;
    let rows = statement
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(io::Error::other)?;
    rows.collect::<Result<HashSet<_>, _>>()
        .map_err(io::Error::other)
}

fn ensure_terminal_history_optional_columns(connection: &Connection) -> io::Result<()> {
    let mut columns = terminal_history_column_names(connection)?;
    for (name, sql) in [
        (
            "terminal_disposition",
            "ALTER TABLE runtime_receipt_terminal_history ADD COLUMN terminal_disposition TEXT",
        ),
        (
            "child_queue_id",
            "ALTER TABLE runtime_receipt_terminal_history ADD COLUMN child_queue_id TEXT",
        ),
        (
            "continuation_index",
            "ALTER TABLE runtime_receipt_terminal_history ADD COLUMN continuation_index INTEGER",
        ),
        (
            "task_disposition",
            "ALTER TABLE runtime_receipt_terminal_history ADD COLUMN task_disposition TEXT",
        ),
        (
            "effect_disposition",
            "ALTER TABLE runtime_receipt_terminal_history ADD COLUMN effect_disposition TEXT",
        ),
    ] {
        if columns.insert(name.to_string()) {
            connection.execute(sql, []).map_err(io::Error::other)?;
        }
    }
    Ok(())
}

fn terminal_history_optional_select_columns(connection: &Connection) -> io::Result<String> {
    let columns = terminal_history_column_names(connection)?;
    Ok([
        if columns.contains("terminal_disposition") {
            "history.terminal_disposition"
        } else {
            "NULL AS terminal_disposition"
        },
        if columns.contains("child_queue_id") {
            "history.child_queue_id"
        } else {
            "NULL AS child_queue_id"
        },
        if columns.contains("continuation_index") {
            "history.continuation_index"
        } else {
            "NULL AS continuation_index"
        },
        if columns.contains("task_disposition") {
            "history.task_disposition"
        } else {
            "NULL AS task_disposition"
        },
        if columns.contains("effect_disposition") {
            "history.effect_disposition"
        } else {
            "NULL AS effect_disposition"
        },
    ]
    .join(", "))
}

fn parse_jsonl_records(bytes: &[u8]) -> Vec<Value> {
    bytes
        .split(|byte| *byte == b'\n')
        .filter_map(|line| {
            let line = trim_ascii_whitespace(line);
            (!line.is_empty())
                .then(|| serde_json::from_slice::<Value>(line).ok())
                .flatten()
        })
        .collect()
}

fn trim_ascii_whitespace(mut bytes: &[u8]) -> &[u8] {
    while bytes.first().is_some_and(u8::is_ascii_whitespace) {
        bytes = &bytes[1..];
    }
    while bytes.last().is_some_and(u8::is_ascii_whitespace) {
        bytes = &bytes[..bytes.len() - 1];
    }
    bytes
}

fn status_counts(records: &[Value]) -> BTreeMap<String, u64> {
    let mut counts = BTreeMap::new();
    for record in records {
        if !is_trusted_runtime_run_once_receipt(record) {
            continue;
        }
        let Some(status) = string_field(record, &["status"]) else {
            continue;
        };
        if status.trim().is_empty() {
            continue;
        }
        *counts.entry(status.to_string()).or_default() += 1;
    }
    counts
}

fn status_deltas(
    original: &BTreeMap<String, u64>,
    snapshot: &BTreeMap<String, u64>,
) -> io::Result<BTreeMap<String, u64>> {
    let mut deltas = BTreeMap::new();
    for (status, snapshot_count) in snapshot {
        let original_count = original.get(status).copied().unwrap_or_default();
        if *snapshot_count > original_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "runtime receipt compaction snapshot contains more `{status}` records than its original ledger"
                ),
            ));
        }
    }
    for (status, original_count) in original {
        let snapshot_count = snapshot.get(status).copied().unwrap_or_default();
        deltas.insert(
            status.clone(),
            original_count.saturating_sub(snapshot_count),
        );
    }
    Ok(deltas)
}

fn latest_terminal_records(
    records: &[Value],
    retained_queue_ids: &HashSet<String>,
) -> BTreeMap<String, TerminalHistoryCandidate> {
    let mut latest = BTreeMap::new();
    for record in records {
        if !is_trusted_runtime_run_once_receipt(record) {
            continue;
        }
        let Some(queue_id) = string_field(record, &["queueId", "queue_id"]) else {
            continue;
        };
        if queue_id.trim().is_empty() || retained_queue_ids.contains(queue_id) {
            continue;
        }
        let Some(status) = string_field(record, &["status"]) else {
            continue;
        };
        if !is_terminal_run_once_status(status) {
            continue;
        }
        latest.insert(
            queue_id.to_string(),
            TerminalHistoryCandidate {
                queue_id: queue_id.to_string(),
                trace_id: optional_text(record, &["traceId", "trace_id"]),
                session_key: optional_text(record, &["sessionKey", "session_key"]),
                status: status.to_string(),
                reason: optional_text(record, &["reason"])
                    .map(|value| truncate_chars(&value, MAX_REASON_CHARS)),
                terminal_control_source: optional_text(
                    record,
                    &["terminalControlSource", "terminal_control_source"],
                ),
                runtime_class: optional_text(record, &["runtimeClass", "runtime_class"]),
                origin: optional_text(record, &["origin"]),
                transcript_file: optional_text(
                    record,
                    &[
                        "transcriptFile",
                        "transcript_file",
                        "plannedTranscriptFile",
                        "planned_transcript_file",
                    ],
                )
                .map(PathBuf::from),
                occurred_at_ms: i64_field(
                    record,
                    &[
                        "completedAtMs",
                        "completed_at_ms",
                        "occurredAtMs",
                        "occurred_at_ms",
                        "finishedAtMs",
                        "finished_at_ms",
                        "atMs",
                        "at_ms",
                    ],
                ),
                terminal_disposition: optional_text(
                    record,
                    &["terminalDisposition", "terminal_disposition"],
                ),
                child_queue_id: record
                    .get("continuationLink")
                    .and_then(|link| optional_text(link, &["childQueueId", "child_queue_id"]))
                    .or_else(|| optional_text(record, &["childQueueId", "child_queue_id"])),
                continuation_index: record
                    .get("continuationLink")
                    .and_then(|link| {
                        link.get("continuationIndex")
                            .or_else(|| link.get("continuation_index"))
                    })
                    .and_then(Value::as_u64),
                task_disposition: bounded_json_field(
                    record,
                    &["taskDrainEvaluation", "task_drain_evaluation"],
                ),
                effect_disposition: bounded_json_field(
                    record,
                    &["externalEffect", "external_effect"],
                ),
            },
        );
    }
    latest
}

fn bounded_json_field(record: &Value, names: &[&str]) -> Option<String> {
    names
        .iter()
        .find_map(|name| record.get(*name))
        .filter(|value| !value.is_null())
        .and_then(|value| serde_json::to_string(value).ok())
        .map(|value| truncate_chars(&value, MAX_DISPOSITION_JSON_CHARS))
}

/// A schema-less receipt is a supported legacy artifact. An explicit unknown
/// (or malformed) schema is not runtime lifecycle evidence and must not enter
/// a terminal hot/cold projection.
pub(crate) fn is_trusted_runtime_run_once_receipt(value: &Value) -> bool {
    match value.get("schema") {
        None => true,
        Some(Value::String(schema)) => schema == RUNTIME_RUN_ONCE_RECEIPT_SCHEMA,
        Some(_) => false,
    }
}

fn is_terminal_run_once_status(status: &str) -> bool {
    matches!(
        status,
        "completed"
            | "timeout"
            | "failed-terminal"
            | "canceled"
            | "skipped"
            | "dead-letter"
            | "suppressed"
            | "external-effect-denied"
    )
}

fn string_field<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a str> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(Value::as_str))
}

fn optional_text(value: &Value, keys: &[&str]) -> Option<String> {
    string_field(value, keys)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(ToString::to_string)
}

fn i64_field(value: &Value, keys: &[&str]) -> Option<i64> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(Value::as_i64))
}

fn truncate_chars(value: &str, limit: usize) -> String {
    if limit == 0 {
        return String::new();
    }
    let mut chars = value.chars();
    let mut truncated = chars.by_ref().take(limit).collect::<String>();
    if chars.next().is_some() {
        let _ = truncated.pop();
        truncated.push('…');
        truncated
    } else {
        truncated
    }
}

fn read_terminal_history_rows(
    connection: &Connection,
    query: &str,
    values: Vec<String>,
) -> io::Result<Vec<RuntimeQueueTerminalHistoryRecord>> {
    let mut statement = connection.prepare(query).map_err(io::Error::other)?;
    let rows = statement
        .query_map(params_from_iter(values.iter()), |row| {
            Ok(RuntimeQueueTerminalHistoryRecord {
                row_id: row.get(0)?,
                queue_id: row.get(1)?,
                trace_id: row.get(2)?,
                session_key: row.get(3)?,
                status: row.get(4)?,
                reason: row.get(5)?,
                terminal_control_source: row.get(6)?,
                runtime_class: row.get(7)?,
                origin: row.get(8)?,
                transcript_file: row.get::<_, Option<String>>(9)?.map(PathBuf::from),
                occurred_at_ms: row.get(10)?,
                compacted_at_ms: row.get(11)?,
                terminal_disposition: row.get(12)?,
                child_queue_id: row.get(13)?,
                continuation_index: row
                    .get::<_, Option<i64>>(14)?
                    .and_then(|value| u64::try_from(value).ok()),
                task_disposition: row.get(15)?,
                effect_disposition: row.get(16)?,
            })
        })
        .map_err(io::Error::other)?;
    let mut records = Vec::new();
    let mut seen_queue_ids = HashSet::new();
    for row in rows {
        let record = row.map_err(io::Error::other)?;
        if seen_queue_ids.insert(record.queue_id.clone()) {
            records.push(record);
        }
    }
    Ok(records)
}

fn read_terminal_history_rows_nonblocking(
    connection: &Connection,
    query: &str,
    values: Vec<String>,
) -> io::Result<Vec<RuntimeQueueTerminalHistoryRecord>> {
    let mut statement = connection
        .prepare(query)
        .map_err(nonblocking_history_io_error)?;
    let rows = statement
        .query_map(params_from_iter(values.iter()), |row| {
            Ok(RuntimeQueueTerminalHistoryRecord {
                row_id: row.get(0)?,
                queue_id: row.get(1)?,
                trace_id: row.get(2)?,
                session_key: row.get(3)?,
                status: row.get(4)?,
                reason: row.get(5)?,
                terminal_control_source: row.get(6)?,
                runtime_class: row.get(7)?,
                origin: row.get(8)?,
                transcript_file: row.get::<_, Option<String>>(9)?.map(PathBuf::from),
                occurred_at_ms: row.get(10)?,
                compacted_at_ms: row.get(11)?,
                terminal_disposition: row.get(12)?,
                child_queue_id: row.get(13)?,
                continuation_index: row
                    .get::<_, Option<i64>>(14)?
                    .and_then(|value| u64::try_from(value).ok()),
                task_disposition: row.get(15)?,
                effect_disposition: row.get(16)?,
            })
        })
        .map_err(nonblocking_history_io_error)?;
    let mut records = Vec::new();
    let mut seen_queue_ids = HashSet::new();
    for row in rows {
        let record = row.map_err(nonblocking_history_io_error)?;
        if seen_queue_ids.insert(record.queue_id.clone()) {
            records.push(record);
        }
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn external_effect_denied_survives_terminal_history_compaction() {
        let root = std::env::temp_dir().join(format!(
            "agent-harness-runtime-effect-denied-history-test-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let queue_dir = root.join("state").join("runtime-queue");
        fs::create_dir_all(&queue_dir).unwrap();
        let original = br#"{"schema":"agent-harness.runtime-run-once.v1","queueId":"turn:denied","traceId":"trace:denied","sessionKey":"session:denied","status":"external-effect-denied","reason":"approval denied","completedAtMs":42}
"#;
        let staged = stage_runtime_queue_receipt_history(
            &queue_dir,
            "effect-denied-transaction",
            original,
            b"",
            &HashSet::new(),
            100,
        )
        .unwrap();
        commit_runtime_queue_receipt_history(&staged, 101).unwrap();

        let records = find_runtime_queue_terminal_history(
            &queue_dir,
            &BTreeSet::from(["turn:denied".to_string()]),
        )
        .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].status, "external-effect-denied");
        assert_eq!(records[0].reason.as_deref(), Some("approval denied"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn continuation_disposition_and_link_survive_compaction() {
        let root = std::env::temp_dir().join(format!(
            "agent-harness-runtime-continuation-history-test-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let queue_dir = root.join("state").join("runtime-queue");
        fs::create_dir_all(&queue_dir).unwrap();
        let original = br#"{"queueId":"parent","status":"skipped","reason":"continued","terminalDisposition":"continuation-handoff","continuationLink":{"parentQueueId":"parent","childQueueId":"child","continuationIndex":2},"taskDrainEvaluation":{"scheduleContinuation":true,"reason":"open plan"},"externalEffect":{"effectId":"effect-1","state":"confirmed","paramsDigest":"sha256:redacted"}}
"#;
        let staged = stage_runtime_queue_receipt_history(
            &queue_dir,
            "typed-continuation-transaction",
            original,
            b"",
            &HashSet::new(),
            100,
        )
        .unwrap();
        commit_runtime_queue_receipt_history(&staged, 101).unwrap();
        let records = find_runtime_queue_terminal_history(
            &queue_dir,
            &BTreeSet::from(["parent".to_string()]),
        )
        .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].terminal_disposition.as_deref(),
            Some("continuation-handoff")
        );
        assert_eq!(records[0].child_queue_id.as_deref(), Some("child"));
        assert_eq!(records[0].continuation_index, Some(2));
        assert!(
            records[0]
                .task_disposition
                .as_deref()
                .is_some_and(|value| value.contains("scheduleContinuation"))
        );
        assert!(
            records[0]
                .effect_disposition
                .as_deref()
                .is_some_and(|value| value.contains("effect-1"))
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn retry_lineage_and_schedule_survive_required_retention_path() {
        let root = std::env::temp_dir().join(format!(
            "agent-harness-runtime-retry-retention-test-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let harness_home = root.join(".agent-harness");
        let queue_dir = harness_home.join("state").join("runtime-queue");
        fs::create_dir_all(&queue_dir).unwrap();
        fs::write(
            queue_dir.join("pending.jsonl"),
            serde_json::json!({"queueId":"turn:retry","status":"queued"}).to_string(),
        )
        .unwrap();
        let lineage_id = "runtime-retry:turn:retry";
        fs::write(
            queue_dir.join("run-once-receipts.jsonl"),
            [
                serde_json::json!({
                    "schema":"agent-harness.runtime-run-once.v1",
                    "queueId":"turn:historical",
                    "traceId":"trace:historical",
                    "status":"completed",
                    "reason":"terminal history is eligible for cold storage"
                }),
                serde_json::json!({
                    "schema":"agent-harness.runtime-run-once.v1",
                    "queueId":"turn:retry",
                    "status":"failed-retryable",
                    "reason":"first retry failure",
                    "retrySchedule": {
                        "lineageId": lineage_id,
                        "attempt": 1,
                        "maxAttempts": 3,
                        "delayMs": 1000,
                        "scheduledAtMs": 100,
                        "nextEligibleAtMs": 1100,
                        "replayMode": "same-request-no-observable-mutation"
                    }
                }),
                serde_json::json!({
                    "schema":"agent-harness.runtime-run-once.v1",
                    "queueId":"turn:retry",
                    "status":"retry-pending",
                    "reason":"retry remains runnable",
                    "retrySchedule": {
                        "lineageId": lineage_id,
                        "attempt": 2,
                        "maxAttempts": 3,
                        "delayMs": 2000,
                        "scheduledAtMs": 200,
                        "nextEligibleAtMs": 2200,
                        "replayMode": "same-request-no-observable-mutation"
                    }
                }),
            ]
            .into_iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join("\n"),
        )
        .unwrap();

        let report = crate::compact_runtime_queue_receipts_if_needed(
            crate::RuntimeQueueReceiptCompactionOptions {
                harness_home: harness_home.clone(),
                max_bytes: 1,
                max_archives: 1,
                now_ms: 300,
            },
        )
        .unwrap();
        assert_eq!(
            report.status,
            crate::RuntimeQueueReceiptCompactionStatus::Compacted
        );
        let hot = fs::read_to_string(queue_dir.join("run-once-receipts.jsonl")).unwrap();
        let retained = hot
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .filter(|value| value.get("queueId").and_then(Value::as_str) == Some("turn:retry"))
            .collect::<Vec<_>>();
        assert_eq!(
            retained.len(),
            2,
            "the retry attempt sequence must stay hot"
        );
        assert_eq!(
            retained[0]
                .pointer("/retrySchedule/lineageId")
                .and_then(Value::as_str),
            Some(lineage_id)
        );
        assert_eq!(
            retained[1]
                .pointer("/retrySchedule/nextEligibleAtMs")
                .and_then(Value::as_i64),
            Some(2200)
        );
        let index: Value =
            serde_json::from_slice(&fs::read(queue_dir.join("queue-state-index.json")).unwrap())
                .unwrap();
        assert_eq!(
            index
                .pointer("/queues/turn:retry/retrySchedule/lineageId")
                .and_then(Value::as_str),
            Some(lineage_id)
        );
        assert_eq!(
            index
                .pointer("/queues/turn:retry/retrySchedule/nextEligibleAtMs")
                .and_then(Value::as_i64),
            Some(2200)
        );
        let cold = find_runtime_queue_terminal_history(
            &queue_dir,
            &BTreeSet::from(["trace:historical".to_string()]),
        )
        .unwrap();
        assert_eq!(cold.len(), 1);
        assert_eq!(cold[0].queue_id, "turn:historical");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn staged_history_is_hidden_until_committed_and_preserves_terminal_delta() {
        let root = std::env::temp_dir().join(format!(
            "agent-harness-runtime-receipt-history-test-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let queue_dir = root.join("state").join("runtime-queue");
        fs::create_dir_all(&queue_dir).unwrap();
        let original = br#"{"queueId":"turn:finished","traceId":"trace:finished","sessionKey":"session:finished","status":"completed","reason":"completed safely","runtimeClass":"interactive","origin":"channel","transcriptFile":"transcript.jsonl","completedAtMs":42}
{"queueId":"turn:live","status":"queued"}
"#;
        let snapshot = br#"{"queueId":"turn:live","status":"queued"}
"#;
        let retained = HashSet::from(["turn:live".to_string()]);

        let staged = stage_runtime_queue_receipt_history(
            &queue_dir,
            "test-transaction",
            original,
            snapshot,
            &retained,
            100,
        )
        .unwrap();
        assert!(
            read_runtime_queue_receipt_history_status_counts(&queue_dir)
                .unwrap()
                .is_empty(),
            "a staged transaction must never affect a reader"
        );
        assert!(
            find_runtime_queue_terminal_history(
                &queue_dir,
                &BTreeSet::from(["trace:finished".to_string()])
            )
            .unwrap()
            .is_empty(),
            "terminal history must be invisible until the ledger swap commits"
        );

        commit_runtime_queue_receipt_history(&staged, 101).unwrap();
        let counts = read_runtime_queue_receipt_history_status_counts(&queue_dir).unwrap();
        assert_eq!(counts.get("completed"), Some(&1));
        assert_eq!(counts.get("queued"), None);
        let rows = find_runtime_queue_terminal_history(
            &queue_dir,
            &BTreeSet::from(["trace:finished".to_string()]),
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].queue_id, "turn:finished");
        assert_eq!(rows[0].status, "completed");
        assert_eq!(rows[0].reason.as_deref(), Some("completed safely"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn explicit_unknown_schema_is_not_terminal_history_evidence() {
        let root = std::env::temp_dir().join(format!(
            "agent-harness-runtime-receipt-history-schema-gate-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let queue_dir = root.join("state").join("runtime-queue");
        fs::create_dir_all(&queue_dir).unwrap();
        let original = br#"{"schema":"unrelated.runtime-receipt.v1","queueId":"turn:foreign","status":"completed","reason":"must not terminalize"}
{"schema":"agent-harness.runtime-run-once.v1","queueId":"turn:trusted","status":"completed","reason":"trusted completion"}
"#;
        let staged = stage_runtime_queue_receipt_history(
            &queue_dir,
            "schema-gate",
            original,
            b"",
            &HashSet::new(),
            100,
        )
        .unwrap();
        commit_runtime_queue_receipt_history(&staged, 101).unwrap();

        let rows = find_runtime_queue_terminal_history(
            &queue_dir,
            &BTreeSet::from(["turn:foreign".to_string(), "turn:trusted".to_string()]),
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].queue_id, "turn:trusted");
        assert_eq!(
            read_runtime_queue_receipt_history_status_counts(&queue_dir)
                .unwrap()
                .get("completed"),
            Some(&1)
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn nonblocking_reader_returns_only_exact_committed_history() {
        let root = std::env::temp_dir().join(format!(
            "agent-harness-runtime-receipt-history-nonblocking-exact-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let queue_dir = root.join("state").join("runtime-queue");
        fs::create_dir_all(&queue_dir).unwrap();
        let original = br#"{"queueId":"turn:finished","traceId":"trace:finished","sessionKey":"session:finished","status":"completed","reason":"completed safely"}
{"queueId":"turn:other","traceId":"trace:other","sessionKey":"session:other","status":"completed","reason":"other completion"}
"#;
        let snapshot = b"";
        let staged = stage_runtime_queue_receipt_history(
            &queue_dir,
            "nonblocking-exact",
            original,
            snapshot,
            &HashSet::new(),
            100,
        )
        .unwrap();
        commit_runtime_queue_receipt_history(&staged, 101).unwrap();

        let rows = find_runtime_queue_terminal_history_nonblocking(
            &queue_dir,
            &BTreeSet::from(["trace:finished".to_string()]),
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].queue_id, "turn:finished");
        assert_eq!(rows[0].trace_id.as_deref(), Some("trace:finished"));
        assert!(
            find_runtime_queue_terminal_history_nonblocking(&queue_dir, &BTreeSet::new())
                .unwrap()
                .is_empty()
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn nonblocking_reader_returns_would_block_for_an_exclusive_history_lock() {
        let root = std::env::temp_dir().join(format!(
            "agent-harness-runtime-receipt-history-nonblocking-busy-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let queue_dir = root.join("state").join("runtime-queue");
        fs::create_dir_all(&queue_dir).unwrap();
        let original =
            br#"{"queueId":"turn:finished","traceId":"trace:finished","status":"completed"}
"#;
        let staged = stage_runtime_queue_receipt_history(
            &queue_dir,
            "nonblocking-busy",
            original,
            b"",
            &HashSet::new(),
            100,
        )
        .unwrap();
        commit_runtime_queue_receipt_history(&staged, 101).unwrap();

        let store_file = runtime_queue_receipt_history_file(&queue_dir);
        let writer = Connection::open(&store_file).unwrap();
        writer.busy_timeout(Duration::from_millis(0)).unwrap();
        writer.execute_batch("BEGIN EXCLUSIVE").unwrap();

        let error = find_runtime_queue_terminal_history_nonblocking(
            &queue_dir,
            &BTreeSet::from(["trace:finished".to_string()]),
        )
        .expect_err("a user-visible nonblocking lookup must not wait behind an exclusive lock");
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        writer.execute_batch("ROLLBACK").unwrap();

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn nonblocking_reader_does_not_create_a_missing_history_store() {
        let root = std::env::temp_dir().join(format!(
            "agent-harness-runtime-receipt-history-nonblocking-missing-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let queue_dir = root.join("state").join("runtime-queue");
        fs::create_dir_all(&queue_dir).unwrap();
        let store_file = runtime_queue_receipt_history_file(&queue_dir);

        let rows = find_runtime_queue_terminal_history_nonblocking(
            &queue_dir,
            &BTreeSet::from(["turn:missing".to_string()]),
        )
        .unwrap();
        assert!(rows.is_empty());
        assert!(!store_file.exists());

        let _ = fs::remove_dir_all(root);
    }
}
