//! Source-authoritative sidecar for the active runtime pending queue.

use crate::logging::{try_with_jsonl_append_lock, with_jsonl_append_lock};
use rusqlite::{
    Connection, OpenFlags, OptionalExtension, Transaction, TransactionBehavior, params,
};
use serde_json::Value;
use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::{Duration, UNIX_EPOCH};

const PENDING_QUEUE_FILE_NAME: &str = "pending.jsonl";
const RUNTIME_PENDING_INDEX_FILE_NAME: &str = "runtime-pending-index.sqlite";
const RUNTIME_PENDING_INDEX_SCHEMA: &str = "agent-harness.runtime-pending-index.v1";
const RUNTIME_PENDING_INDEX_REVISION: i64 = 1;
const PENDING_INDEX_FINGERPRINT_BYTES: u64 = 4 * 1024;

/// Upper bound for active, runnable queue rows retained by the derived
/// sidecar.  Reaching it is a deliberate backpressure error: the index must
/// never evict runnable work merely to keep itself small.
pub(crate) const RUNTIME_PENDING_INDEX_MAX_ACTIVE_ROWS: usize = 8_192;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct PendingLedgerCursor {
    offset_bytes: u64,
    line_number: u64,
    source_len_bytes: u64,
    source_modified_at_unix_nanos: Option<u128>,
    processed_head_fingerprint: Option<u64>,
    processed_tail_fingerprint: Option<u64>,
    malformed_line_count: u64,
    ignored_record_count: u64,
    incomplete_final_line: bool,
}

impl PendingLedgerCursor {
    fn is_pristine(&self) -> bool {
        self.offset_bytes == 0
            && self.line_number == 0
            && self.source_len_bytes == 0
            && self.source_modified_at_unix_nanos.is_none()
            && self.processed_head_fingerprint.is_none()
            && self.processed_tail_fingerprint.is_none()
            && self.malformed_line_count == 0
            && self.ignored_record_count == 0
            && !self.incomplete_final_line
    }
}

/// Returns the durable SQLite sidecar colocated with `pending.jsonl`.
pub(crate) fn runtime_pending_index_file(queue_dir: &Path) -> PathBuf {
    queue_dir.join(RUNTIME_PENDING_INDEX_FILE_NAME)
}

/// Materializes all active queued JSON objects in stable source order.
///
/// The normal path checks a durable cursor plus two bounded fingerprints, then
/// tails only newly appended bytes while the JSONL append lock is held.
pub(crate) fn read_queued_pending_values_from_index(
    queue_dir: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<Vec<Value>> {
    with_current_pending_index(queue_dir, warnings, |connection, warnings| {
        read_pending_values(connection, None, warnings)
    })
}

/// User-visible working/typing callers use this variant. It never waits for a
/// live JSONL append: if the source append lock is busy, it reads only the
/// already-committed SQLite snapshot through a read-only, zero-timeout handle.
pub(crate) fn read_queued_pending_values_from_index_nonblocking(
    queue_dir: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<Vec<Value>> {
    let pending_file = pending_queue_file(queue_dir);
    let refreshed = try_with_jsonl_append_lock(&pending_file, || {
        let mut connection = open_pending_index_read_write(queue_dir)?;
        refresh_pending_index_locked(
            &mut connection,
            &pending_file,
            warnings,
            RUNTIME_PENDING_INDEX_MAX_ACTIVE_ROWS,
        )?;
        read_pending_values(&connection, None, warnings)
    })?;
    if let Some(values) = refreshed {
        return Ok(values);
    }

    warnings.push(format!(
        "runtime pending index refresh is waiting for a live append at {}; using last committed pending snapshot",
        pending_file.display()
    ));
    match open_existing_pending_index_read_only(queue_dir) {
        Ok(Some(connection)) => match read_pending_values(&connection, None, warnings) {
            Ok(values) => Ok(values),
            Err(error) => {
                warnings.push(format!(
                    "runtime pending index read-only snapshot is unavailable without waiting: {error}"
                ));
                Ok(Vec::new())
            }
        },
        Ok(None) => {
            warnings.push(
                "runtime pending index has no compatible committed snapshot while append is busy"
                    .to_string(),
            );
            Ok(Vec::new())
        }
        Err(error) => {
            warnings.push(format!(
                "runtime pending index read-only snapshot is unavailable without waiting: {error}"
            ));
            Ok(Vec::new())
        }
    }
}

/// Fast exact queue-id existence lookup for ingress/control callers.
pub(crate) fn pending_queue_id_exists_from_index(
    queue_dir: &Path,
    queue_id: &str,
    warnings: &mut Vec<String>,
) -> io::Result<bool> {
    if queue_id.trim().is_empty() {
        return Ok(false);
    }
    with_current_pending_index(queue_dir, warnings, |connection, _warnings| {
        connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM runtime_pending_index_rows WHERE queue_id = ?1)",
                params![queue_id],
                |row| row.get::<_, i64>(0),
            )
            .map(|exists| exists != 0)
            .map_err(io::Error::other)
    })
}

/// Removes only caller-proven terminal IDs from the derived sidecar.
///
/// `pending.jsonl` remains authoritative. A later source rewrite or detected
/// rebuild re-materializes rows from that source, so this helper must never
/// infer terminality on its own.
pub(crate) fn prune_terminal_queue_ids_from_pending_index(
    queue_dir: &Path,
    terminal_queue_ids: &BTreeSet<String>,
    warnings: &mut Vec<String>,
) -> io::Result<usize> {
    let terminal_queue_ids = terminal_queue_ids
        .iter()
        .filter(|queue_id| !queue_id.trim().is_empty())
        .collect::<Vec<_>>();
    if terminal_queue_ids.is_empty() {
        return Ok(0);
    }
    let pending_file = pending_queue_file(queue_dir);
    with_jsonl_append_lock(&pending_file, || {
        let mut connection = open_pending_index_read_write(queue_dir)?;
        refresh_pending_index_locked(
            &mut connection,
            &pending_file,
            warnings,
            RUNTIME_PENDING_INDEX_MAX_ACTIVE_ROWS,
        )?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(io::Error::other)?;
        let mut removed = 0_usize;
        for queue_id in terminal_queue_ids {
            removed = removed.saturating_add(
                transaction
                    .execute(
                        "DELETE FROM runtime_pending_index_rows WHERE queue_id = ?1",
                        params![queue_id.as_str()],
                    )
                    .map_err(io::Error::other)?,
            );
        }
        transaction.commit().map_err(io::Error::other)?;
        Ok(removed)
    })
}

fn with_current_pending_index<T>(
    queue_dir: &Path,
    warnings: &mut Vec<String>,
    operation: impl FnOnce(&Connection, &mut Vec<String>) -> io::Result<T>,
) -> io::Result<T> {
    let pending_file = pending_queue_file(queue_dir);
    with_jsonl_append_lock(&pending_file, || {
        let mut connection = open_pending_index_read_write(queue_dir)?;
        refresh_pending_index_locked(
            &mut connection,
            &pending_file,
            warnings,
            RUNTIME_PENDING_INDEX_MAX_ACTIVE_ROWS,
        )?;
        operation(&connection, warnings)
    })
}

fn pending_queue_file(queue_dir: &Path) -> PathBuf {
    queue_dir.join(PENDING_QUEUE_FILE_NAME)
}

#[cfg(test)]
fn refresh_runtime_pending_index_with_limit(
    queue_dir: &Path,
    warnings: &mut Vec<String>,
    max_active_rows: usize,
) -> io::Result<()> {
    let pending_file = pending_queue_file(queue_dir);
    with_jsonl_append_lock(&pending_file, || {
        let mut connection = open_pending_index_read_write(queue_dir)?;
        refresh_pending_index_locked(&mut connection, &pending_file, warnings, max_active_rows)
    })
}

fn refresh_pending_index_locked(
    connection: &mut Connection,
    pending_file: &Path,
    warnings: &mut Vec<String>,
    max_active_rows: usize,
) -> io::Result<()> {
    let cursor = match read_pending_cursor(connection) {
        Ok(Some(cursor)) => cursor,
        Ok(None) => PendingLedgerCursor::default(),
        Err(error) => {
            warnings.push(format!(
                "runtime pending index cursor is invalid; rebuilding: {error}"
            ));
            return rebuild_pending_index(connection, pending_file, warnings, max_active_rows);
        }
    };

    let metadata = match fs::metadata(pending_file) {
        Ok(metadata) if metadata.is_file() => metadata,
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "runtime pending queue ledger path is not a file: {}",
                    pending_file.display()
                ),
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if !cursor.is_pristine() || pending_index_has_rows(connection)? {
                warnings.push(
                    "runtime pending queue ledger disappeared; rebuilding pending index"
                        .to_string(),
                );
                reset_pending_index(connection)?;
                warnings.push(format!(
                    "runtime pending queue file not found at {}",
                    pending_file.display()
                ));
            }
            // A fresh harness has no pending ledger until the first turn is
            // queued. Treat that as an authoritative empty snapshot rather
            // than a degraded index so `/stop` and other control commands can
            // safely report that there is no active sibling work.
            return Ok(());
        }
        Err(error) => return Err(error),
    };
    let source_len = metadata.len();
    let source_modified_at_unix_nanos = file_modified_at_unix_nanos(&metadata);

    if source_len < cursor.source_len_bytes || source_len < cursor.offset_bytes {
        warnings.push(
            "runtime pending queue ledger was truncated; rebuilding pending index".to_string(),
        );
        return rebuild_pending_index(connection, pending_file, warnings, max_active_rows);
    }
    let head_matches = pending_processed_head_fingerprint(pending_file, cursor.offset_bytes)?
        == cursor.processed_head_fingerprint;
    let tail_matches = pending_processed_tail_fingerprint(pending_file, cursor.offset_bytes)?
        == cursor.processed_tail_fingerprint;
    if !head_matches || !tail_matches {
        warnings.push(
            "runtime pending queue ledger prefix no longer matches the index cursor; rebuilding pending index"
                .to_string(),
        );
        return rebuild_pending_index(connection, pending_file, warnings, max_active_rows);
    }
    if source_len == cursor.source_len_bytes {
        if source_modified_at_unix_nanos == cursor.source_modified_at_unix_nanos {
            return Ok(());
        }
        warnings.push(
            "runtime pending queue ledger changed without an append; rebuilding pending index"
                .to_string(),
        );
        return rebuild_pending_index(connection, pending_file, warnings, max_active_rows);
    }

    update_pending_index_from_cursor(
        connection,
        pending_file,
        cursor,
        false,
        warnings,
        max_active_rows,
        "refreshing",
    )?;
    Ok(())
}

fn rebuild_pending_index(
    connection: &mut Connection,
    pending_file: &Path,
    warnings: &mut Vec<String>,
    max_active_rows: usize,
) -> io::Result<()> {
    match fs::metadata(pending_file) {
        Ok(metadata) if metadata.is_file() => {
            update_pending_index_from_cursor(
                connection,
                pending_file,
                PendingLedgerCursor::default(),
                true,
                warnings,
                max_active_rows,
                "rebuilding",
            )?;
        }
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "runtime pending queue ledger path is not a file: {}",
                    pending_file.display()
                ),
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => reset_pending_index(connection)?,
        Err(error) => return Err(error),
    }
    Ok(())
}

fn update_pending_index_from_cursor(
    connection: &mut Connection,
    pending_file: &Path,
    cursor: PendingLedgerCursor,
    replace_existing: bool,
    warnings: &mut Vec<String>,
    max_active_rows: usize,
    phase: &str,
) -> io::Result<PendingLedgerCursor> {
    let file = File::open(pending_file)?;
    let source_len = file.metadata()?.len();
    if cursor.offset_bytes > source_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "runtime pending index cursor {} exceeds source length {}",
                cursor.offset_bytes, source_len
            ),
        ));
    }
    let mut reader = BufReader::new(file);
    reader.seek(SeekFrom::Start(cursor.offset_bytes))?;
    let mut active_row_count = if replace_existing {
        0
    } else {
        pending_index_row_count(connection)?
    };
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(io::Error::other)?;
    if replace_existing {
        transaction
            .execute("DELETE FROM runtime_pending_index_rows", [])
            .map_err(io::Error::other)?;
    }

    let mut refreshed = cursor;
    refreshed.incomplete_final_line = false;
    loop {
        let mut line = String::new();
        let bytes_read = reader.read_line(&mut line)?;
        if bytes_read == 0 {
            break;
        }
        if !line.ends_with('\n') {
            refreshed.incomplete_final_line = true;
            break;
        }
        refreshed.line_number = refreshed.line_number.checked_add(1).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "runtime pending queue line number overflow",
            )
        })?;
        refreshed.offset_bytes = refreshed
            .offset_bytes
            .checked_add(u64::try_from(bytes_read).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "runtime pending queue line length exceeds u64",
                )
            })?)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "runtime pending queue byte offset overflow",
                )
            })?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let value = match serde_json::from_str::<Value>(trimmed) {
            Ok(value) => value,
            Err(error) => {
                refreshed.malformed_line_count = refreshed.malformed_line_count.saturating_add(1);
                warnings.push(format!(
                    "runtime pending queue line {} is not valid JSON while {phase} pending index: {error}",
                    refreshed.line_number
                ));
                continue;
            }
        };
        let Some(object) = value.as_object() else {
            refreshed.ignored_record_count = refreshed.ignored_record_count.saturating_add(1);
            warnings.push(format!(
                "runtime pending queue line {} is not a JSON object; skipping from active index",
                refreshed.line_number
            ));
            continue;
        };
        let queue_id = object
            .get("queueId")
            .or_else(|| object.get("queue_id"))
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty());
        let Some(queue_id) = queue_id else {
            refreshed.ignored_record_count = refreshed.ignored_record_count.saturating_add(1);
            warnings.push(format!(
                "runtime pending queue line {} has no queue id; skipping from active index",
                refreshed.line_number
            ));
            continue;
        };
        let status = object.get("status").and_then(Value::as_str);
        if status != Some("queued") {
            refreshed.ignored_record_count = refreshed.ignored_record_count.saturating_add(1);
            warnings.push(format!(
                "runtime pending queue item `{queue_id}` is not queued; skipping from active index"
            ));
            continue;
        }
        if active_row_count >= max_active_rows {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                format!(
                    "runtime pending index capacity/backpressure: active queued rows would exceed {max_active_rows}; source ledger remains authoritative and no runnable rows were evicted"
                ),
            ));
        }
        transaction
            .execute(
                "INSERT INTO runtime_pending_index_rows (source_line, queue_id, status, raw_json) \
                 VALUES (?1, ?2, ?3, ?4)",
                params![
                    sqlite_i64(refreshed.line_number, "runtime pending queue source line")?,
                    queue_id,
                    "queued",
                    trimmed,
                ],
            )
            .map_err(io::Error::other)?;
        active_row_count = active_row_count.checked_add(1).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "runtime pending index active row count overflow",
            )
        })?;
    }
    let metadata = reader.get_ref().metadata()?;
    refreshed.source_len_bytes = source_len;
    refreshed.source_modified_at_unix_nanos = file_modified_at_unix_nanos(&metadata);
    refreshed.processed_head_fingerprint =
        pending_processed_head_fingerprint(pending_file, refreshed.offset_bytes)?;
    refreshed.processed_tail_fingerprint =
        pending_processed_tail_fingerprint(pending_file, refreshed.offset_bytes)?;
    write_pending_cursor(&transaction, &refreshed)?;
    transaction.commit().map_err(io::Error::other)?;
    Ok(refreshed)
}

fn read_pending_values(
    connection: &Connection,
    queue_id: Option<&str>,
    warnings: &mut Vec<String>,
) -> io::Result<Vec<Value>> {
    let (sql, parameters) = match queue_id {
        Some(queue_id) => (
            "SELECT source_line, raw_json FROM runtime_pending_index_rows \
             WHERE queue_id = ?1 ORDER BY source_line ASC",
            Some(queue_id),
        ),
        None => (
            "SELECT source_line, raw_json FROM runtime_pending_index_rows ORDER BY source_line ASC",
            None,
        ),
    };
    let mut statement = connection.prepare(sql).map_err(io::Error::other)?;
    let mut raw_rows = Vec::new();
    if let Some(queue_id) = parameters {
        let rows = statement
            .query_map(params![queue_id], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(io::Error::other)?;
        for row in rows {
            raw_rows.push(row.map_err(io::Error::other)?);
        }
    } else {
        let rows = statement
            .query_map([], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(io::Error::other)?;
        for row in rows {
            raw_rows.push(row.map_err(io::Error::other)?);
        }
    }
    let mut values = Vec::with_capacity(raw_rows.len());
    for (source_line, raw_json) in raw_rows {
        match serde_json::from_str::<Value>(&raw_json) {
            Ok(value) if value.is_object() => values.push(value),
            Ok(_) => {
                warnings.push(format!(
                    "runtime pending index source line {source_line} is not a JSON object; source refresh is required"
                ));
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "runtime pending index contains a non-object queued row",
                ));
            }
            Err(error) => {
                warnings.push(format!(
                    "runtime pending index source line {source_line} is invalid; source refresh is required: {error}"
                ));
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "runtime pending index contains invalid queued JSON",
                ));
            }
        }
    }
    Ok(values)
}

fn open_pending_index_read_write(queue_dir: &Path) -> io::Result<Connection> {
    fs::create_dir_all(queue_dir)?;
    let mut connection =
        Connection::open(runtime_pending_index_file(queue_dir)).map_err(io::Error::other)?;
    connection
        .busy_timeout(Duration::from_secs(5))
        .map_err(io::Error::other)?;
    initialize_pending_index(&mut connection)?;
    Ok(connection)
}

fn open_existing_pending_index_read_only(queue_dir: &Path) -> io::Result<Option<Connection>> {
    let index_file = runtime_pending_index_file(queue_dir);
    if !index_file.is_file() {
        return Ok(None);
    }
    let connection = Connection::open_with_flags(&index_file, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(io::Error::other)?;
    connection
        .busy_timeout(Duration::from_millis(0))
        .map_err(io::Error::other)?;
    if !pending_index_schema_is_current(&connection)? {
        return Ok(None);
    }
    Ok(Some(connection))
}

fn initialize_pending_index(connection: &mut Connection) -> io::Result<()> {
    connection
        .execute_batch(
            "PRAGMA foreign_keys = ON;
             CREATE TABLE IF NOT EXISTS runtime_pending_index_meta (
               singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
               schema TEXT NOT NULL,
               revision INTEGER NOT NULL,
               offset_bytes INTEGER NOT NULL,
               line_number INTEGER NOT NULL,
               source_len_bytes INTEGER NOT NULL,
               source_modified_at_unix_nanos TEXT,
               processed_head_fingerprint TEXT,
               processed_tail_fingerprint TEXT,
               malformed_line_count INTEGER NOT NULL,
               ignored_record_count INTEGER NOT NULL,
               incomplete_final_line INTEGER NOT NULL
             );",
        )
        .map_err(io::Error::other)?;
    let metadata = connection
        .query_row(
            "SELECT schema, revision FROM runtime_pending_index_meta WHERE singleton = 1",
            [],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional();
    match metadata {
        Ok(Some((schema, revision)))
            if schema == RUNTIME_PENDING_INDEX_SCHEMA
                && revision == RUNTIME_PENDING_INDEX_REVISION =>
        {
            create_pending_index_tables(connection)
        }
        Ok(None) => {
            create_pending_index_tables(connection)?;
            write_initial_pending_cursor(connection)
        }
        Ok(Some(_)) | Err(_) => reset_pending_index_schema(connection),
    }
}

fn pending_index_schema_is_current(connection: &Connection) -> io::Result<bool> {
    connection
        .query_row(
            "SELECT schema, revision FROM runtime_pending_index_meta WHERE singleton = 1",
            [],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()
        .map(|row| {
            row.is_some_and(|(schema, revision)| {
                schema == RUNTIME_PENDING_INDEX_SCHEMA && revision == RUNTIME_PENDING_INDEX_REVISION
            })
        })
        .map_err(io::Error::other)
}

fn reset_pending_index_schema(connection: &mut Connection) -> io::Result<()> {
    connection
        .execute_batch(
            "DROP INDEX IF EXISTS runtime_pending_index_rows_queue_source_idx;
             DROP TABLE IF EXISTS runtime_pending_index_rows;
             DROP TABLE IF EXISTS runtime_pending_index_meta;",
        )
        .map_err(io::Error::other)?;
    connection
        .execute_batch(
            "CREATE TABLE runtime_pending_index_meta (
               singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
               schema TEXT NOT NULL,
               revision INTEGER NOT NULL,
               offset_bytes INTEGER NOT NULL,
               line_number INTEGER NOT NULL,
               source_len_bytes INTEGER NOT NULL,
               source_modified_at_unix_nanos TEXT,
               processed_head_fingerprint TEXT,
               processed_tail_fingerprint TEXT,
               malformed_line_count INTEGER NOT NULL,
               ignored_record_count INTEGER NOT NULL,
               incomplete_final_line INTEGER NOT NULL
             );",
        )
        .map_err(io::Error::other)?;
    create_pending_index_tables(connection)?;
    write_initial_pending_cursor(connection)
}

fn create_pending_index_tables(connection: &Connection) -> io::Result<()> {
    connection
        .execute_batch(
            "CREATE TABLE IF NOT EXISTS runtime_pending_index_rows (
               source_line INTEGER PRIMARY KEY,
               queue_id TEXT NOT NULL,
               status TEXT NOT NULL,
               raw_json TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS runtime_pending_index_rows_queue_source_idx
               ON runtime_pending_index_rows(queue_id, source_line ASC);",
        )
        .map_err(io::Error::other)
}

fn write_initial_pending_cursor(connection: &Connection) -> io::Result<()> {
    connection
        .execute(
            "INSERT INTO runtime_pending_index_meta \
             (singleton, schema, revision, offset_bytes, line_number, source_len_bytes, \
              source_modified_at_unix_nanos, processed_head_fingerprint, \
              processed_tail_fingerprint, malformed_line_count, ignored_record_count, \
              incomplete_final_line) \
             VALUES (1, ?1, ?2, 0, 0, 0, NULL, NULL, NULL, 0, 0, 0)",
            params![RUNTIME_PENDING_INDEX_SCHEMA, RUNTIME_PENDING_INDEX_REVISION],
        )
        .map_err(io::Error::other)?;
    Ok(())
}

fn read_pending_cursor(connection: &Connection) -> io::Result<Option<PendingLedgerCursor>> {
    let row = connection
        .query_row(
            "SELECT offset_bytes, line_number, source_len_bytes, \
                    source_modified_at_unix_nanos, processed_head_fingerprint, \
                    processed_tail_fingerprint, malformed_line_count, ignored_record_count, \
                    incomplete_final_line \
             FROM runtime_pending_index_meta WHERE singleton = 1",
            [],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, i64>(7)?,
                    row.get::<_, i64>(8)?,
                ))
            },
        )
        .optional()
        .map_err(io::Error::other)?;
    let Some((
        offset_bytes,
        line_number,
        source_len_bytes,
        source_modified_at_unix_nanos,
        processed_head_fingerprint,
        processed_tail_fingerprint,
        malformed_line_count,
        ignored_record_count,
        incomplete_final_line,
    )) = row
    else {
        return Ok(None);
    };
    Ok(Some(PendingLedgerCursor {
        offset_bytes: sqlite_u64(offset_bytes, "runtime pending index offset")?,
        line_number: sqlite_u64(line_number, "runtime pending index line number")?,
        source_len_bytes: sqlite_u64(source_len_bytes, "runtime pending index source length")?,
        source_modified_at_unix_nanos: parse_optional_u128(
            source_modified_at_unix_nanos,
            "source_modified_at_unix_nanos",
        )?,
        processed_head_fingerprint: parse_optional_u64(
            processed_head_fingerprint,
            "processed_head_fingerprint",
        )?,
        processed_tail_fingerprint: parse_optional_u64(
            processed_tail_fingerprint,
            "processed_tail_fingerprint",
        )?,
        malformed_line_count: sqlite_u64(
            malformed_line_count,
            "runtime pending index malformed line count",
        )?,
        ignored_record_count: sqlite_u64(
            ignored_record_count,
            "runtime pending index ignored record count",
        )?,
        incomplete_final_line: incomplete_final_line != 0,
    }))
}

fn write_pending_cursor(
    transaction: &Transaction<'_>,
    cursor: &PendingLedgerCursor,
) -> io::Result<()> {
    transaction
        .execute(
            "INSERT INTO runtime_pending_index_meta \
             (singleton, schema, revision, offset_bytes, line_number, source_len_bytes, \
              source_modified_at_unix_nanos, processed_head_fingerprint, \
              processed_tail_fingerprint, malformed_line_count, ignored_record_count, \
              incomplete_final_line) \
             VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11) \
             ON CONFLICT(singleton) DO UPDATE SET \
               schema = excluded.schema, revision = excluded.revision, \
               offset_bytes = excluded.offset_bytes, line_number = excluded.line_number, \
               source_len_bytes = excluded.source_len_bytes, \
               source_modified_at_unix_nanos = excluded.source_modified_at_unix_nanos, \
               processed_head_fingerprint = excluded.processed_head_fingerprint, \
               processed_tail_fingerprint = excluded.processed_tail_fingerprint, \
               malformed_line_count = excluded.malformed_line_count, \
               ignored_record_count = excluded.ignored_record_count, \
               incomplete_final_line = excluded.incomplete_final_line",
            params![
                RUNTIME_PENDING_INDEX_SCHEMA,
                RUNTIME_PENDING_INDEX_REVISION,
                sqlite_i64(cursor.offset_bytes, "runtime pending index byte offset")?,
                sqlite_i64(cursor.line_number, "runtime pending index line number")?,
                sqlite_i64(
                    cursor.source_len_bytes,
                    "runtime pending index source length"
                )?,
                cursor
                    .source_modified_at_unix_nanos
                    .map(|value| value.to_string()),
                cursor
                    .processed_head_fingerprint
                    .map(|value| value.to_string()),
                cursor
                    .processed_tail_fingerprint
                    .map(|value| value.to_string()),
                sqlite_i64(
                    cursor.malformed_line_count,
                    "runtime pending index malformed line count",
                )?,
                sqlite_i64(
                    cursor.ignored_record_count,
                    "runtime pending index ignored record count",
                )?,
                i64::from(cursor.incomplete_final_line),
            ],
        )
        .map_err(io::Error::other)?;
    Ok(())
}

fn pending_index_row_count(connection: &Connection) -> io::Result<usize> {
    let count = connection
        .query_row(
            "SELECT COUNT(*) FROM runtime_pending_index_rows",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(io::Error::other)?;
    usize::try_from(count).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "runtime pending index row count cannot be represented",
        )
    })
}

fn pending_index_has_rows(connection: &Connection) -> io::Result<bool> {
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM runtime_pending_index_rows LIMIT 1)",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map(|exists| exists != 0)
        .map_err(io::Error::other)
}

fn reset_pending_index(connection: &mut Connection) -> io::Result<()> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(io::Error::other)?;
    transaction
        .execute("DELETE FROM runtime_pending_index_rows", [])
        .map_err(io::Error::other)?;
    write_pending_cursor(&transaction, &PendingLedgerCursor::default())?;
    transaction.commit().map_err(io::Error::other)
}

fn sqlite_i64(value: u64, field: &str) -> io::Result<i64> {
    i64::try_from(value).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{field} exceeds the SQLite integer range"),
        )
    })
}

fn sqlite_u64(value: i64, field: &str) -> io::Result<u64> {
    u64::try_from(value).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{field} cannot be negative"),
        )
    })
}

fn parse_optional_u128(value: Option<String>, field: &str) -> io::Result<Option<u128>> {
    value
        .map(|value| {
            value.parse::<u128>().map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("runtime pending index {field} is invalid: {error}"),
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
                    format!("runtime pending index {field} is invalid: {error}"),
                )
            })
        })
        .transpose()
}

fn file_modified_at_unix_nanos(metadata: &fs::Metadata) -> Option<u128> {
    metadata
        .modified()
        .ok()
        .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos())
}

fn pending_processed_head_fingerprint(
    pending_file: &Path,
    processed_offset: u64,
) -> io::Result<Option<u64>> {
    if processed_offset == 0 {
        return Ok(None);
    }
    fingerprint_pending_range(
        pending_file,
        0,
        processed_offset.min(PENDING_INDEX_FINGERPRINT_BYTES),
    )
}

fn pending_processed_tail_fingerprint(
    pending_file: &Path,
    processed_offset: u64,
) -> io::Result<Option<u64>> {
    if processed_offset == 0 {
        return Ok(None);
    }
    let start = processed_offset.saturating_sub(PENDING_INDEX_FINGERPRINT_BYTES);
    fingerprint_pending_range(pending_file, start, processed_offset.saturating_sub(start))
}

fn fingerprint_pending_range(
    pending_file: &Path,
    start: u64,
    bytes: u64,
) -> io::Result<Option<u64>> {
    if bytes == 0 {
        return Ok(None);
    }
    let mut file = File::open(pending_file)?;
    file.seek(SeekFrom::Start(start))?;
    let mut remaining = bytes;
    let mut buffer = [0_u8; PENDING_INDEX_FINGERPRINT_BYTES as usize];
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};
    use std::collections::BTreeSet;
    use std::fs::{self, OpenOptions};
    use std::io::Write;
    use std::path::PathBuf;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    fn temp_queue_dir(name: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "agent-harness-runtime-pending-index-{name}-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let queue_dir = root.join("state").join("runtime-queue");
        fs::create_dir_all(&queue_dir).unwrap();
        queue_dir
    }

    fn queued_value(queue_id: &str) -> Value {
        json!({
            "queueId": queue_id,
            "status": "queued",
            "messageText": format!("message for {queue_id}"),
        })
    }

    fn queue_ids(values: &[Value]) -> Vec<&str> {
        values
            .iter()
            .map(|value| value["queueId"].as_str().unwrap())
            .collect()
    }

    fn append_lock_file(jsonl_file: &std::path::Path) -> PathBuf {
        let file_name = jsonl_file.file_name().unwrap().to_string_lossy();
        jsonl_file.with_file_name(format!(".{file_name}.append.lock"))
    }

    #[test]
    fn builds_then_tails_queued_values_in_stable_source_order() {
        let queue_dir = temp_queue_dir("build-tail-order");
        let pending_file = queue_dir.join("pending.jsonl");
        fs::write(
            &pending_file,
            [
                queued_value("turn:alpha").to_string(),
                "not-json".to_string(),
                json!({"queueId":"turn:held","status":"held"}).to_string(),
                queued_value("turn:beta").to_string(),
            ]
            .join("\n")
                + "\n",
        )
        .unwrap();

        let mut warnings = Vec::new();
        let initial = read_queued_pending_values_from_index(&queue_dir, &mut warnings).unwrap();
        assert_eq!(queue_ids(&initial), vec!["turn:alpha", "turn:beta"]);
        assert!(runtime_pending_index_file(&queue_dir).is_file());
        assert!(warnings.iter().any(|warning| warning.contains("line 2")));
        assert!(warnings.iter().any(|warning| warning.contains("turn:held")));

        writeln!(
            OpenOptions::new().append(true).open(&pending_file).unwrap(),
            "{}",
            queued_value("turn:gamma")
        )
        .unwrap();
        warnings.clear();
        let tailed = read_queued_pending_values_from_index(&queue_dir, &mut warnings).unwrap();
        assert_eq!(
            queue_ids(&tailed),
            vec!["turn:alpha", "turn:beta", "turn:gamma"]
        );
        assert!(warnings.is_empty());

        let _ = fs::remove_dir_all(queue_dir.ancestors().nth(2).unwrap());
    }

    #[test]
    fn missing_pending_ledger_is_a_pristine_empty_snapshot() {
        let queue_dir = temp_queue_dir("missing-pristine");
        let mut warnings = Vec::new();

        let values = read_queued_pending_values_from_index(&queue_dir, &mut warnings).unwrap();

        assert!(values.is_empty());
        assert!(warnings.is_empty());

        let _ = fs::remove_dir_all(queue_dir.ancestors().nth(2).unwrap());
    }

    #[test]
    fn rebuilds_the_active_snapshot_after_source_rewrite_or_truncation() {
        let queue_dir = temp_queue_dir("rewrite-truncation");
        let pending_file = queue_dir.join("pending.jsonl");
        fs::write(
            &pending_file,
            format!("{}\n", queued_value("turn:original")),
        )
        .unwrap();

        let mut warnings = Vec::new();
        assert_eq!(
            queue_ids(&read_queued_pending_values_from_index(&queue_dir, &mut warnings).unwrap()),
            vec!["turn:original"]
        );

        fs::write(
            &pending_file,
            format!("{}\n", queued_value("turn:replacement")),
        )
        .unwrap();
        warnings.clear();
        let rebuilt = read_queued_pending_values_from_index(&queue_dir, &mut warnings).unwrap();
        assert_eq!(queue_ids(&rebuilt), vec!["turn:replacement"]);
        assert!(warnings.iter().any(|warning| warning.contains("rebuild")));

        let _ = fs::remove_dir_all(queue_dir.ancestors().nth(2).unwrap());
    }

    #[test]
    fn nonblocking_read_uses_last_committed_snapshot_when_append_lock_is_busy() {
        let queue_dir = temp_queue_dir("busy-snapshot");
        let pending_file = queue_dir.join("pending.jsonl");
        fs::write(&pending_file, format!("{}\n", queued_value("turn:old"))).unwrap();

        let mut warnings = Vec::new();
        read_queued_pending_values_from_index(&queue_dir, &mut warnings).unwrap();
        writeln!(
            OpenOptions::new().append(true).open(&pending_file).unwrap(),
            "{}",
            queued_value("turn:new")
        )
        .unwrap();
        let lock_file = append_lock_file(&pending_file);
        fs::write(&lock_file, format!("pid={}\n", std::process::id())).unwrap();

        warnings.clear();
        let started = Instant::now();
        let snapshot =
            read_queued_pending_values_from_index_nonblocking(&queue_dir, &mut warnings).unwrap();
        assert!(started.elapsed() < Duration::from_secs(2));
        assert_eq!(queue_ids(&snapshot), vec!["turn:old"]);
        assert!(
            warnings
                .iter()
                .any(|warning| warning.contains("last committed"))
        );

        fs::remove_file(&lock_file).unwrap();
        let refreshed =
            read_queued_pending_values_from_index_nonblocking(&queue_dir, &mut warnings).unwrap();
        assert_eq!(queue_ids(&refreshed), vec!["turn:old", "turn:new"]);

        let _ = fs::remove_dir_all(queue_dir.ancestors().nth(2).unwrap());
    }

    #[test]
    fn terminal_prune_removes_only_explicit_ids_until_the_source_is_rebuilt() {
        let queue_dir = temp_queue_dir("terminal-prune");
        let pending_file = queue_dir.join("pending.jsonl");
        fs::write(
            &pending_file,
            [queued_value("turn:terminal"), queued_value("turn:runnable")]
                .into_iter()
                .map(|value| value.to_string())
                .collect::<Vec<_>>()
                .join("\n")
                + "\n",
        )
        .unwrap();

        let mut warnings = Vec::new();
        read_queued_pending_values_from_index(&queue_dir, &mut warnings).unwrap();
        assert_eq!(
            prune_terminal_queue_ids_from_pending_index(
                &queue_dir,
                &BTreeSet::from(["turn:terminal".to_string()]),
                &mut warnings,
            )
            .unwrap(),
            1
        );
        assert!(
            !pending_queue_id_exists_from_index(&queue_dir, "turn:terminal", &mut warnings,)
                .unwrap()
        );
        assert_eq!(
            queue_ids(&read_queued_pending_values_from_index(&queue_dir, &mut warnings).unwrap()),
            vec!["turn:runnable"]
        );
        assert!(
            fs::read_to_string(&pending_file)
                .unwrap()
                .contains("turn:terminal")
        );

        fs::write(
            &pending_file,
            [
                json!({
                    "queueId": "turn:terminal",
                    "status": "queued",
                    "messageText": "rebuilt terminal source row",
                }),
                queued_value("turn:runnable"),
                queued_value("turn:recovered"),
            ]
            .into_iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join("\n")
                + "\n",
        )
        .unwrap();
        let rebuilt = read_queued_pending_values_from_index(&queue_dir, &mut warnings).unwrap();
        assert_eq!(
            queue_ids(&rebuilt),
            vec!["turn:terminal", "turn:runnable", "turn:recovered"]
        );

        let _ = fs::remove_dir_all(queue_dir.ancestors().nth(2).unwrap());
    }

    #[test]
    fn capacity_is_a_fail_closed_backpressure_error_without_eviction() {
        let queue_dir = temp_queue_dir("capacity-backpressure");
        let pending_file = queue_dir.join("pending.jsonl");
        fs::write(
            &pending_file,
            [
                queued_value("turn:one"),
                queued_value("turn:two"),
                queued_value("turn:three"),
            ]
            .into_iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join("\n")
                + "\n",
        )
        .unwrap();

        let error = refresh_runtime_pending_index_with_limit(&queue_dir, &mut Vec::new(), 2)
            .expect_err("the sidecar must not evict a runnable message to fit capacity");
        assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
        assert!(error.to_string().contains("capacity/backpressure"));

        let _ = fs::remove_dir_all(queue_dir.ancestors().nth(2).unwrap());
    }

    #[test]
    fn waits_for_a_partial_final_json_line_before_indexing_it() {
        let queue_dir = temp_queue_dir("partial-final-line");
        let pending_file = queue_dir.join("pending.jsonl");
        let completed = queued_value("turn:later").to_string();
        let split_at = completed.len() / 2;
        fs::write(
            &pending_file,
            format!("{}\n{}", queued_value("turn:ready"), &completed[..split_at]),
        )
        .unwrap();

        let mut warnings = Vec::new();
        let initial = read_queued_pending_values_from_index(&queue_dir, &mut warnings).unwrap();
        assert_eq!(queue_ids(&initial), vec!["turn:ready"]);
        assert!(warnings.is_empty());

        writeln!(
            OpenOptions::new().append(true).open(&pending_file).unwrap(),
            "{}",
            &completed[split_at..]
        )
        .unwrap();
        let completed_values =
            read_queued_pending_values_from_index(&queue_dir, &mut warnings).unwrap();
        assert_eq!(
            queue_ids(&completed_values),
            vec!["turn:ready", "turn:later"]
        );

        let _ = fs::remove_dir_all(queue_dir.ancestors().nth(2).unwrap());
    }
}
