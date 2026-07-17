//! Bounded, source-authoritative runtime lookups for virtual sessions.
//!
//! The pending queue and Codex runtime receipt JSONL files are authoritative.
//! This sidecar only retains the latest metadata required by the virtual
//! session hot path, while cursors and bounded fingerprints avoid replaying
//! either source on normal requests.

use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::{Duration, UNIX_EPOCH};

use rusqlite::{
    Connection, OpenFlags, OptionalExtension, Transaction, TransactionBehavior, params,
    params_from_iter,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::codex_runtime::CodexRuntimeUsage;
use crate::context_rollover::root_working_session_key;
use crate::logging::try_with_jsonl_append_lock;
#[cfg(test)]
use crate::logging::with_jsonl_append_lock;

pub(crate) const VIRTUAL_SESSION_RUNTIME_INDEX_SCHEMA: &str =
    "agent-harness.virtual-session-runtime-index.v1";

const INDEX_FILE_NAME: &str = "virtual-session-runtime-index.sqlite";
const INDEX_REVISION: i64 = 3;
const META_SCHEMA: &str = "schema";
const META_REVISION: &str = "revision";
const META_PENDING_CURSOR: &str = "pending-cursor";
const META_PENDING_TOTAL: &str = "pending-total-lines";
const META_PENDING_INVALID: &str = "pending-invalid-lines";
const META_RUN_CURSOR: &str = "runtime-run-cursor";
const META_RUN_TOTAL: &str = "runtime-run-total-lines";
const META_RUN_INVALID: &str = "runtime-run-invalid-lines";
const FINGERPRINT_BYTES: u64 = 4 * 1024;
const DEFAULT_ACCOUNT_KEY: &str = "default";
const ACCOUNT_KEY_PREFIX: &str = "account:";
const MAX_PENDING_ROWS: i64 = 8_192;
const MAX_INTERRUPTION_ROWS: i64 = 8_192;
const MAX_CODEX_USAGE_BINDINGS: i64 = 8_192;
const MAX_INTERRUPTION_REASON_CHARS: usize = 160;
const MAX_INTERRUPTION_TEXT_CHARS: usize = 240;
const MAX_TOOL_PREVIEW_CHARS: usize = 160;
const MAX_CODEX_USAGE_SOURCE_CHARS: usize = 256;
const MAX_CODEX_USAGE_RAW_CHARS: usize = 512;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VirtualSessionRuntimeLaneQuery {
    pub(crate) platform: String,
    /// `None`, empty, and `default` all mean the exact default/missing account
    /// class. They never wildcard-match records from another account.
    pub(crate) account_id: Option<String>,
    pub(crate) channel_id: String,
    pub(crate) user_id: String,
    pub(crate) agent_id: String,
    pub(crate) root_session_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VirtualSessionInterruptionEvidence {
    pub(crate) queue_id: String,
    pub(crate) line_number: usize,
    pub(crate) interruption_reason: String,
    pub(crate) method: Option<String>,
    pub(crate) item_type: Option<String>,
    pub(crate) preview: Option<String>,
    pub(crate) safe_to_rerun: bool,
    pub(crate) interrupted_at_ms: Option<i64>,
    pub(crate) reason: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LedgerCursor {
    #[serde(default)]
    offset_bytes: u64,
    #[serde(default)]
    line_number: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source_modified_at_unix_nanos: Option<u128>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    prefix_tail_fingerprint: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    head_fingerprint: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
enum SourceKind {
    Pending,
    RuntimeRunReceipts,
}

impl SourceKind {
    fn file(self, harness_home: &Path) -> PathBuf {
        let queue_dir = harness_home.join("state").join("runtime-queue");
        match self {
            Self::Pending => queue_dir.join("pending.jsonl"),
            Self::RuntimeRunReceipts => queue_dir.join("codex-runtime-run-receipts.jsonl"),
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Pending => "runtime pending queue",
            Self::RuntimeRunReceipts => "Codex runtime run receipt",
        }
    }

    fn cursor_key(self) -> &'static str {
        match self {
            Self::Pending => META_PENDING_CURSOR,
            Self::RuntimeRunReceipts => META_RUN_CURSOR,
        }
    }

    fn total_key(self) -> &'static str {
        match self {
            Self::Pending => META_PENDING_TOTAL,
            Self::RuntimeRunReceipts => META_RUN_TOTAL,
        }
    }

    fn invalid_key(self) -> &'static str {
        match self {
            Self::Pending => META_PENDING_INVALID,
            Self::RuntimeRunReceipts => META_RUN_INVALID,
        }
    }
}

enum RefreshMode {
    Current,
    Append(LedgerCursor),
    Rebuild(String),
}

#[derive(Debug, Clone, Copy, Default)]
struct CounterDelta {
    total: i64,
    invalid: i64,
}

/// Returns the durable SQLite sidecar stored next to the runtime queue ledgers.
pub(crate) fn virtual_session_runtime_index_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("runtime-queue")
        .join(INDEX_FILE_NAME)
}

/// Refreshes both source indexes. Normal refreshes tail only stable appends.
#[cfg(test)]
pub(crate) fn refresh_virtual_session_runtime_index(
    harness_home: impl AsRef<Path>,
    warnings: &mut Vec<String>,
) -> io::Result<()> {
    refresh_source_index(harness_home.as_ref(), SourceKind::Pending, warnings)?;
    refresh_source_index(
        harness_home.as_ref(),
        SourceKind::RuntimeRunReceipts,
        warnings,
    )
}

/// Returns bounded recent queue ids for one exact virtual-session lane.
pub(crate) fn recent_runtime_queue_ids_for_virtual_session_lane(
    harness_home: impl AsRef<Path>,
    lane: &VirtualSessionRuntimeLaneQuery,
    limit: usize,
    warnings: &mut Vec<String>,
) -> io::Result<Vec<String>> {
    if limit == 0
        || lane.platform.trim().is_empty()
        || lane.channel_id.trim().is_empty()
        || lane.user_id.trim().is_empty()
        || lane.agent_id.trim().is_empty()
        || lane.root_session_key.trim().is_empty()
    {
        return Ok(Vec::new());
    }
    let limit = i64::try_from(limit).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "virtual session queue limit exceeds SQLite range",
        )
    })?;
    let account_key = account_key(lane.account_id.as_deref());
    let root_session_key = root_working_session_key(&lane.root_session_key);
    with_source_index(
        harness_home.as_ref(),
        SourceKind::Pending,
        warnings,
        |connection| {
            let mut statement = connection
                .prepare(
                    "SELECT queue_id FROM virtual_session_runtime_pending_rows
                     WHERE platform = ?1 AND account_key = ?2 AND channel_id = ?3
                       AND user_id = ?4 AND agent_id = ?5 AND root_session_key = ?6
                     ORDER BY line_number DESC LIMIT ?7",
                )
                .map_err(io::Error::other)?;
            let rows = statement
                .query_map(
                    params![
                        lane.platform,
                        account_key,
                        lane.channel_id,
                        lane.user_id,
                        lane.agent_id,
                        root_session_key,
                        limit,
                    ],
                    |row| row.get::<_, String>(0),
                )
                .map_err(io::Error::other)?;
            rows.map(|row| row.map_err(io::Error::other)).collect()
        },
        Vec::new,
    )
}

/// Returns the latest structured interruption evidence among an exact queue-id
/// set. No lane axis is inferred here; callers must supply the already scoped
/// queue set from the exact virtual-session lane.
pub(crate) fn latest_runtime_interruption_evidence_for_queue_ids(
    harness_home: impl AsRef<Path>,
    queue_ids: &BTreeSet<String>,
    warnings: &mut Vec<String>,
) -> io::Result<Option<VirtualSessionInterruptionEvidence>> {
    if queue_ids.is_empty() {
        return Ok(None);
    }
    let placeholders = std::iter::repeat_n("?", queue_ids.len())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT queue_id, line_number, interruption_reason, method, item_type,
                preview, safe_to_rerun, interrupted_at_ms, reason
         FROM virtual_session_runtime_interruptions
         WHERE queue_id IN ({placeholders})
         ORDER BY line_number DESC LIMIT 1"
    );
    with_source_index(
        harness_home.as_ref(),
        SourceKind::RuntimeRunReceipts,
        warnings,
        |connection| {
            let mut statement = connection.prepare(&sql).map_err(io::Error::other)?;
            let values = queue_ids.iter().collect::<Vec<_>>();
            statement
                .query_row(params_from_iter(values), |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, i64>(6)?,
                        row.get::<_, Option<i64>>(7)?,
                        row.get::<_, Option<String>>(8)?,
                    ))
                })
                .optional()
                .map_err(io::Error::other)?
                .map(
                    |(
                        queue_id,
                        line_number,
                        interruption_reason,
                        method,
                        item_type,
                        preview,
                        safe_to_rerun,
                        interrupted_at_ms,
                        reason,
                    )| {
                        Ok(VirtualSessionInterruptionEvidence {
                            queue_id,
                            line_number: usize::try_from(line_number).map_err(|_| {
                                io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "virtual session interruption index has an invalid line number",
                                )
                            })?,
                            interruption_reason,
                            method,
                            item_type,
                            preview,
                            safe_to_rerun: safe_to_rerun != 0,
                            interrupted_at_ms,
                            reason,
                        })
                    },
                )
                .transpose()
        },
        || None,
    )
}

/// Returns the most recent recorded Codex token usage for a binding, falling
/// back to the globally most recent usage when that binding has no receipt.
/// This intentionally mirrors the legacy reverse-ledger scan semantics while
/// reading only the bounded SQLite projection.
pub(crate) fn latest_codex_runtime_usage(
    harness_home: impl AsRef<Path>,
    codex_binding_file: &Path,
    warnings: &mut Vec<String>,
) -> Option<CodexRuntimeUsage> {
    let binding_key = normalize_path_text_for_match(&codex_binding_file.to_string_lossy());
    match with_source_index(
        harness_home.as_ref(),
        SourceKind::RuntimeRunReceipts,
        warnings,
        |connection| {
            let exact = read_codex_usage_for_binding(connection, &binding_key)?;
            if exact.is_some() {
                return Ok(exact);
            }
            read_global_codex_usage(connection).map(|usage| {
                usage.map(|mut usage| {
                    // Legacy token fallback remains available for the absolute
                    // unknown-capacity guard, but a capacity observation is
                    // never allowed to cross a binding/lane boundary.
                    usage.model_context_window = None;
                    usage.model_context_window_source = None;
                    usage.provider = None;
                    usage.model = None;
                    usage.backend_context_generation = None;
                    usage.observed_at_ms = None;
                    usage
                })
            })
        },
        || None,
    ) {
        Ok(usage) => usage,
        Err(error) => {
            warnings.push(format!(
                "failed to refresh/read Codex runtime usage index for context usage preflight: {error}"
            ));
            None
        }
    }
}

#[cfg(test)]
fn refresh_source_index(
    harness_home: &Path,
    source: SourceKind,
    warnings: &mut Vec<String>,
) -> io::Result<()> {
    let file = source.file(harness_home);
    if let Some(parent) = file.parent() {
        fs::create_dir_all(parent)?;
    }
    with_jsonl_append_lock(&file, || {
        let mut connection = open_index(harness_home)?;
        refresh_source_index_locked(&mut connection, source, &file, warnings)
    })
}

/// Runs a user-visible virtual-session lookup from a freshly tailed source
/// projection when its append lock is immediately available. When a source
/// writer owns that lock, do not open or initialize a writable SQLite
/// connection: use an existing read-only committed snapshot, or return the
/// conservative caller-supplied result if no snapshot is immediately readable.
fn with_source_index<T>(
    harness_home: &Path,
    source: SourceKind,
    warnings: &mut Vec<String>,
    operation: impl Fn(&Connection) -> io::Result<T>,
    conservative_fallback: impl FnOnce() -> T,
) -> io::Result<T> {
    let file = source.file(harness_home);
    match try_with_jsonl_append_lock(&file, || {
        let mut connection = open_index(harness_home)?;
        refresh_source_index_locked(&mut connection, source, &file, warnings)?;
        operation(&connection)
    })? {
        Some(value) => Ok(value),
        None => {
            warnings.push(format!(
                "{} index refresh is waiting for a live append at {}; using last committed virtual-session runtime state",
                source.label(),
                file.display()
            ));
            read_runtime_index_snapshot(
                harness_home,
                source,
                warnings,
                &operation,
                conservative_fallback,
            )
        }
    }
}

fn read_runtime_index_snapshot<T>(
    harness_home: &Path,
    source: SourceKind,
    warnings: &mut Vec<String>,
    operation: &impl Fn(&Connection) -> io::Result<T>,
    conservative_fallback: impl FnOnce() -> T,
) -> io::Result<T> {
    let index_file = virtual_session_runtime_index_file(harness_home);
    let connection = match open_existing_runtime_index_snapshot(&index_file) {
        Ok(Some(connection)) => connection,
        Ok(None) => {
            warnings.push(format!(
                "{} index snapshot is unavailable at {}; returning conservative empty virtual-session runtime state",
                source.label(),
                index_file.display()
            ));
            return Ok(conservative_fallback());
        }
        Err(error) => {
            warnings.push(format!(
                "failed to open {} index snapshot at {}; returning conservative empty virtual-session runtime state: {error}",
                source.label(),
                index_file.display()
            ));
            return Ok(conservative_fallback());
        }
    };
    match operation(&connection) {
        Ok(value) => Ok(value),
        Err(error) => {
            warnings.push(format!(
                "failed to read {} index snapshot at {}; returning conservative empty virtual-session runtime state: {error}",
                source.label(),
                index_file.display()
            ));
            Ok(conservative_fallback())
        }
    }
}

fn open_existing_runtime_index_snapshot(index_file: &Path) -> io::Result<Option<Connection>> {
    match fs::metadata(index_file) {
        Ok(metadata) if metadata.is_file() => {}
        Ok(_) => return Ok(None),
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    }
    let connection = Connection::open_with_flags(index_file, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(io::Error::other)?;
    connection
        .busy_timeout(Duration::from_millis(0))
        .map_err(io::Error::other)?;
    Ok(Some(connection))
}

fn open_index(harness_home: &Path) -> io::Result<Connection> {
    let index_file = virtual_session_runtime_index_file(harness_home);
    if let Some(parent) = index_file.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut connection = Connection::open(index_file).map_err(io::Error::other)?;
    connection
        .busy_timeout(Duration::from_secs(10))
        .map_err(io::Error::other)?;
    initialize_index(&mut connection)?;
    Ok(connection)
}

fn initialize_index(connection: &mut Connection) -> io::Result<()> {
    connection.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS virtual_session_runtime_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS virtual_session_runtime_pending_rows (
            platform TEXT NOT NULL,
            account_key TEXT NOT NULL,
            channel_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            agent_id TEXT NOT NULL,
            root_session_key TEXT NOT NULL,
            queue_id TEXT NOT NULL,
            line_number INTEGER NOT NULL,
            created_at_ms INTEGER,
            UNIQUE(platform, account_key, channel_id, user_id, agent_id, root_session_key, queue_id)
        );
        CREATE INDEX IF NOT EXISTS virtual_session_runtime_pending_lane_idx
            ON virtual_session_runtime_pending_rows(
                platform, account_key, channel_id, user_id, agent_id, root_session_key, line_number DESC
            );
        CREATE TABLE IF NOT EXISTS virtual_session_runtime_interruptions (
            queue_id TEXT PRIMARY KEY,
            line_number INTEGER NOT NULL,
            interruption_reason TEXT NOT NULL,
            method TEXT,
            item_type TEXT,
            preview TEXT,
            safe_to_rerun INTEGER NOT NULL,
            interrupted_at_ms INTEGER,
            reason TEXT
        );
        CREATE INDEX IF NOT EXISTS virtual_session_runtime_interruptions_line_idx
            ON virtual_session_runtime_interruptions(line_number DESC);
        CREATE TABLE IF NOT EXISTS virtual_session_runtime_codex_usage_by_binding (
            binding_key TEXT PRIMARY KEY,
            line_number INTEGER NOT NULL,
            input_tokens TEXT,
            output_tokens TEXT,
            total_tokens TEXT,
            model_context_window TEXT,
            model_context_window_source TEXT,
            provider TEXT,
            model TEXT,
            backend_context_generation TEXT,
            observed_at_ms INTEGER,
            source TEXT NOT NULL,
            raw TEXT
        );
        CREATE INDEX IF NOT EXISTS virtual_session_runtime_codex_usage_line_idx
            ON virtual_session_runtime_codex_usage_by_binding(line_number DESC);
        CREATE TABLE IF NOT EXISTS virtual_session_runtime_codex_usage_fallback (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            line_number INTEGER NOT NULL,
            input_tokens TEXT,
            output_tokens TEXT,
            total_tokens TEXT,
            model_context_window TEXT,
            model_context_window_source TEXT,
            provider TEXT,
            model TEXT,
            backend_context_generation TEXT,
            observed_at_ms INTEGER,
            source TEXT NOT NULL,
            raw TEXT
        );
        ",
    ).map_err(io::Error::other)?;
    for table in [
        "virtual_session_runtime_codex_usage_by_binding",
        "virtual_session_runtime_codex_usage_fallback",
    ] {
        ensure_index_column(connection, table, "model_context_window", "TEXT")?;
        ensure_index_column(connection, table, "model_context_window_source", "TEXT")?;
        ensure_index_column(connection, table, "provider", "TEXT")?;
        ensure_index_column(connection, table, "model", "TEXT")?;
        ensure_index_column(connection, table, "backend_context_generation", "TEXT")?;
        ensure_index_column(connection, table, "observed_at_ms", "INTEGER")?;
    }
    let schema = read_meta(connection, META_SCHEMA)?;
    let revision = read_meta(connection, META_REVISION)?;
    if schema.as_deref() == Some(VIRTUAL_SESSION_RUNTIME_INDEX_SCHEMA)
        && revision.as_deref() == Some(&INDEX_REVISION.to_string())
    {
        return Ok(());
    }
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(io::Error::other)?;
    transaction
        .execute("DELETE FROM virtual_session_runtime_pending_rows", [])
        .map_err(io::Error::other)?;
    transaction
        .execute("DELETE FROM virtual_session_runtime_interruptions", [])
        .map_err(io::Error::other)?;
    transaction
        .execute(
            "DELETE FROM virtual_session_runtime_codex_usage_by_binding",
            [],
        )
        .map_err(io::Error::other)?;
    transaction
        .execute(
            "DELETE FROM virtual_session_runtime_codex_usage_fallback",
            [],
        )
        .map_err(io::Error::other)?;
    transaction
        .execute("DELETE FROM virtual_session_runtime_meta", [])
        .map_err(io::Error::other)?;
    write_meta(
        &transaction,
        META_SCHEMA,
        VIRTUAL_SESSION_RUNTIME_INDEX_SCHEMA,
    )?;
    write_meta(&transaction, META_REVISION, &INDEX_REVISION.to_string())?;
    for key in [
        META_PENDING_TOTAL,
        META_PENDING_INVALID,
        META_RUN_TOTAL,
        META_RUN_INVALID,
    ] {
        write_meta(&transaction, key, "0")?;
    }
    transaction.commit().map_err(io::Error::other)
}

fn ensure_index_column(
    connection: &Connection,
    table: &str,
    column: &str,
    sql_type: &str,
) -> io::Result<()> {
    let query = format!("SELECT COUNT(*) FROM pragma_table_info('{table}') WHERE name = ?1");
    let count: i64 = connection
        .query_row(&query, params![column], |row| row.get(0))
        .map_err(io::Error::other)?;
    if count == 0 {
        connection
            .execute(
                &format!("ALTER TABLE {table} ADD COLUMN {column} {sql_type}"),
                [],
            )
            .map_err(io::Error::other)?;
    }
    Ok(())
}

fn refresh_source_index_locked(
    connection: &mut Connection,
    source: SourceKind,
    file: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<()> {
    let cursor = read_cursor(connection, source)?;
    let metadata = match fs::metadata(file) {
        Ok(metadata) if metadata.is_file() => metadata,
        Ok(_) => {
            warnings.push(format!(
                "{} path is not a file; rebuilding index: {}",
                source.label(),
                file.display()
            ));
            reset_source_index(connection, source)?;
            return Ok(());
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if cursor.is_some() || source_has_rows(connection, source)? {
                warnings.push(format!(
                    "{} ledger disappeared; rebuilding index",
                    source.label()
                ));
                reset_source_index(connection, source)?;
            }
            return Ok(());
        }
        Err(error) => return Err(error),
    };
    match refresh_mode(file, &metadata, cursor.as_ref(), source.label())? {
        RefreshMode::Current => Ok(()),
        RefreshMode::Append(cursor) => {
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(io::Error::other)?;
            let (next_cursor, delta) = read_source_from_cursor(
                &transaction,
                source,
                file,
                cursor,
                warnings,
                "refreshing",
            )?;
            apply_counter_delta(&transaction, source, delta)?;
            retain_bounded_source_rows(&transaction, source)?;
            write_cursor(&transaction, source, &next_cursor)?;
            transaction.commit().map_err(io::Error::other)
        }
        RefreshMode::Rebuild(reason) => {
            warnings.push(format!("{reason}; rebuilding {} index", source.label()));
            rebuild_source_index(connection, source, file, warnings)
        }
    }
}

fn rebuild_source_index(
    connection: &mut Connection,
    source: SourceKind,
    file: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<()> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(io::Error::other)?;
    clear_source_rows(&transaction, source)?;
    delete_meta(&transaction, source.cursor_key())?;
    write_meta(&transaction, source.total_key(), "0")?;
    write_meta(&transaction, source.invalid_key(), "0")?;
    let (cursor, delta) = read_source_from_cursor(
        &transaction,
        source,
        file,
        LedgerCursor::default(),
        warnings,
        "rebuilding",
    )?;
    apply_counter_delta(&transaction, source, delta)?;
    retain_bounded_source_rows(&transaction, source)?;
    write_cursor(&transaction, source, &cursor)?;
    transaction.commit().map_err(io::Error::other)
}

fn reset_source_index(connection: &mut Connection, source: SourceKind) -> io::Result<()> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(io::Error::other)?;
    clear_source_rows(&transaction, source)?;
    delete_meta(&transaction, source.cursor_key())?;
    write_meta(&transaction, source.total_key(), "0")?;
    write_meta(&transaction, source.invalid_key(), "0")?;
    transaction.commit().map_err(io::Error::other)
}

fn read_source_from_cursor(
    transaction: &Transaction<'_>,
    source: SourceKind,
    file: &Path,
    cursor: LedgerCursor,
    warnings: &mut Vec<String>,
    phase: &str,
) -> io::Result<(LedgerCursor, CounterDelta)> {
    let handle = File::open(file)?;
    let file_len = handle.metadata()?.len();
    let start_offset = cursor.offset_bytes.min(file_len);
    let mut reader = BufReader::new(handle);
    reader.seek(SeekFrom::Start(start_offset))?;
    let mut offset_bytes = start_offset;
    let mut line_number = if cursor.offset_bytes > file_len {
        0
    } else {
        cursor.line_number
    };
    let mut delta = CounterDelta::default();
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
            break;
        }
        line_number = line_number.saturating_add(1);
        offset_bytes = offset_bytes.saturating_add(bytes_read as u64);
        delta.total = delta.total.saturating_add(1);
        if trimmed.is_empty() {
            delta.invalid = delta.invalid.saturating_add(1);
            continue;
        }
        match serde_json::from_str::<Value>(trimmed) {
            Ok(value) => match source {
                SourceKind::Pending => index_pending_value(transaction, line_number, &value)?,
                SourceKind::RuntimeRunReceipts => {
                    index_runtime_receipt_value(transaction, line_number, &value)?
                }
            },
            Err(error) => {
                delta.invalid = delta.invalid.saturating_add(1);
                warnings.push(format!(
                    "{} line {line_number} is not valid JSON while {phase}: {error}",
                    source.label()
                ));
            }
        }
    }
    let metadata = reader.get_ref().metadata()?;
    Ok((
        cursor_from_processed_ledger(file, offset_bytes, line_number, &metadata)?,
        delta,
    ))
}

fn index_pending_value(
    transaction: &Transaction<'_>,
    line_number: usize,
    value: &Value,
) -> io::Result<()> {
    let Some(queue_id) = nonempty_field(value, &["queueId", "queue_id"]) else {
        return Ok(());
    };
    let Some(platform) = nonempty_field(value, &["platform"]) else {
        return Ok(());
    };
    let Some(channel_id) = nonempty_field(value, &["channelId", "channel_id"]) else {
        return Ok(());
    };
    let Some(user_id) = nonempty_field(value, &["userId", "user_id"]) else {
        return Ok(());
    };
    let Some(agent_id) = nonempty_field(value, &["agentId", "agent_id"]) else {
        return Ok(());
    };
    let Some(session_key) = nonempty_field(value, &["sessionKey", "session_key"]) else {
        return Ok(());
    };
    let root_session_key = root_working_session_key(session_key);
    transaction.execute(
        "INSERT INTO virtual_session_runtime_pending_rows
         (platform, account_key, channel_id, user_id, agent_id, root_session_key, queue_id, line_number, created_at_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
         ON CONFLICT(platform, account_key, channel_id, user_id, agent_id, root_session_key, queue_id)
         DO UPDATE SET line_number = excluded.line_number, created_at_ms = excluded.created_at_ms",
        params![
            platform,
            account_key(string_field(value, &["accountId", "account_id"])),
            channel_id,
            user_id,
            agent_id,
            root_session_key,
            queue_id,
            i64_from_usize(line_number, "virtual session pending line number")?,
            i64_field(value, &["createdAtMs", "created_at_ms"]),
        ],
    ).map_err(io::Error::other)?;
    Ok(())
}

fn index_runtime_receipt_value(
    transaction: &Transaction<'_>,
    line_number: usize,
    value: &Value,
) -> io::Result<()> {
    if let Some(usage) = value
        .get("usage")
        .and_then(|usage| serde_json::from_value::<CodexRuntimeUsage>(usage.clone()).ok())
    {
        index_codex_runtime_usage(transaction, line_number, value, &usage)?;
    }
    let Some(queue_id) = nonempty_field(value, &["queueId", "queue_id"]) else {
        return Ok(());
    };
    let Some(interruption_reason) =
        nonempty_field(value, &["interruptionReason", "interruption_reason"])
    else {
        return Ok(());
    };
    let Some(tool) = value
        .get("interruptedToolUses")
        .or_else(|| value.get("interrupted_tool_uses"))
        .and_then(Value::as_array)
        .and_then(|tools| tools.first())
    else {
        return Ok(());
    };
    transaction.execute(
        "INSERT INTO virtual_session_runtime_interruptions
         (queue_id, line_number, interruption_reason, method, item_type, preview, safe_to_rerun, interrupted_at_ms, reason)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
         ON CONFLICT(queue_id) DO UPDATE SET
             line_number = excluded.line_number,
             interruption_reason = excluded.interruption_reason,
             method = excluded.method,
             item_type = excluded.item_type,
             preview = excluded.preview,
             safe_to_rerun = excluded.safe_to_rerun,
             interrupted_at_ms = excluded.interrupted_at_ms,
             reason = excluded.reason
         WHERE excluded.line_number >= virtual_session_runtime_interruptions.line_number",
        params![
            queue_id,
            i64_from_usize(line_number, "virtual session runtime receipt line number")?,
            truncate_chars(interruption_reason, MAX_INTERRUPTION_REASON_CHARS),
            nonempty_field(tool, &["method"]).map(|text| truncate_chars(text, MAX_INTERRUPTION_TEXT_CHARS)),
            nonempty_field(tool, &["itemType", "item_type"]).map(|text| truncate_chars(text, MAX_INTERRUPTION_TEXT_CHARS)),
            nonempty_field(tool, &["preview"]).map(|text| truncate_chars(text, MAX_TOOL_PREVIEW_CHARS)),
            if tool
                .get("safeToRerun")
                .or_else(|| tool.get("safe_to_rerun"))
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                1_i64
            } else {
                0_i64
            },
            i64_field(tool, &["interruptedAtMs", "interrupted_at_ms"]),
            nonempty_field(value, &["reason"]).map(|text| truncate_chars(text, MAX_INTERRUPTION_TEXT_CHARS)),
        ],
    ).map_err(io::Error::other)?;
    Ok(())
}

fn index_codex_runtime_usage(
    transaction: &Transaction<'_>,
    line_number: usize,
    receipt: &Value,
    usage: &CodexRuntimeUsage,
) -> io::Result<()> {
    let line_number = i64_from_usize(line_number, "Codex usage receipt line number")?;
    let input_tokens = usage.input_tokens.map(|value| value.to_string());
    let output_tokens = usage.output_tokens.map(|value| value.to_string());
    let total_tokens = usage.total_tokens.map(|value| value.to_string());
    let model_context_window = usage.model_context_window.map(|value| value.to_string());
    let model_context_window_source = usage
        .model_context_window_source
        .as_deref()
        .map(|value| truncate_chars(value, MAX_CODEX_USAGE_SOURCE_CHARS));
    let provider = usage
        .provider
        .as_deref()
        .map(|value| truncate_chars(value, 128));
    let model = usage
        .model
        .as_deref()
        .map(|value| truncate_chars(value, 128));
    let backend_context_generation = usage
        .backend_context_generation
        .as_deref()
        .map(|value| truncate_chars(value, MAX_CODEX_USAGE_SOURCE_CHARS));
    let observed_at_ms = usage.observed_at_ms;
    let source = truncate_chars(&usage.source, MAX_CODEX_USAGE_SOURCE_CHARS);
    let raw = usage
        .raw
        .as_deref()
        .map(|value| truncate_chars(value, MAX_CODEX_USAGE_RAW_CHARS));

    transaction
        .execute(
            "INSERT INTO virtual_session_runtime_codex_usage_fallback
             (singleton, line_number, input_tokens, output_tokens, total_tokens, model_context_window,
              model_context_window_source, provider, model, backend_context_generation, observed_at_ms, source, raw)
             VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
             ON CONFLICT(singleton) DO UPDATE SET
                 line_number = excluded.line_number,
                 input_tokens = excluded.input_tokens,
                 output_tokens = excluded.output_tokens,
                 total_tokens = excluded.total_tokens,
                 model_context_window = excluded.model_context_window,
                 model_context_window_source = excluded.model_context_window_source,
                 provider = excluded.provider,
                 model = excluded.model,
                 backend_context_generation = excluded.backend_context_generation,
                 observed_at_ms = excluded.observed_at_ms,
                 source = excluded.source,
                 raw = excluded.raw
             WHERE excluded.line_number >= virtual_session_runtime_codex_usage_fallback.line_number",
            params![
                line_number,
                input_tokens,
                output_tokens,
                total_tokens,
                model_context_window,
                model_context_window_source,
                provider,
                model,
                backend_context_generation,
                observed_at_ms,
                source,
                raw
            ],
        )
        .map_err(io::Error::other)?;

    let Some(binding_file) = string_field(receipt, &["codexBindingFile", "codex_binding_file"])
    else {
        return Ok(());
    };
    let binding_key = normalize_path_text_for_match(binding_file);
    if binding_key.trim().is_empty() {
        return Ok(());
    }
    transaction
        .execute(
            "INSERT INTO virtual_session_runtime_codex_usage_by_binding
             (binding_key, line_number, input_tokens, output_tokens, total_tokens, model_context_window,
              model_context_window_source, provider, model, backend_context_generation, observed_at_ms, source, raw)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
             ON CONFLICT(binding_key) DO UPDATE SET
                 line_number = excluded.line_number,
                 input_tokens = excluded.input_tokens,
                 output_tokens = excluded.output_tokens,
                 total_tokens = excluded.total_tokens,
                 model_context_window = excluded.model_context_window,
                 model_context_window_source = excluded.model_context_window_source,
                 provider = excluded.provider,
                 model = excluded.model,
                 backend_context_generation = excluded.backend_context_generation,
                 observed_at_ms = excluded.observed_at_ms,
                 source = excluded.source,
                 raw = excluded.raw
             WHERE excluded.line_number >= virtual_session_runtime_codex_usage_by_binding.line_number",
            params![
                binding_key,
                line_number,
                input_tokens,
                output_tokens,
                total_tokens,
                model_context_window,
                model_context_window_source,
                provider,
                model,
                backend_context_generation,
                observed_at_ms,
                source,
                raw,
            ],
        )
        .map_err(io::Error::other)?;
    Ok(())
}

fn retain_bounded_source_rows(transaction: &Transaction<'_>, source: SourceKind) -> io::Result<()> {
    let (table, limit) = match source {
        SourceKind::Pending => ("virtual_session_runtime_pending_rows", MAX_PENDING_ROWS),
        SourceKind::RuntimeRunReceipts => (
            "virtual_session_runtime_interruptions",
            MAX_INTERRUPTION_ROWS,
        ),
    };
    transaction.execute(
        &format!("DELETE FROM {table} WHERE rowid NOT IN (SELECT rowid FROM {table} ORDER BY line_number DESC LIMIT ?1)"),
        params![limit],
    ).map_err(io::Error::other)?;
    if matches!(source, SourceKind::RuntimeRunReceipts) {
        transaction
            .execute(
                "DELETE FROM virtual_session_runtime_codex_usage_by_binding
                 WHERE rowid NOT IN (
                     SELECT rowid FROM virtual_session_runtime_codex_usage_by_binding
                     ORDER BY line_number DESC LIMIT ?1
                 )",
                params![MAX_CODEX_USAGE_BINDINGS],
            )
            .map_err(io::Error::other)?;
    }
    Ok(())
}

fn clear_source_rows(transaction: &Transaction<'_>, source: SourceKind) -> io::Result<()> {
    let table = match source {
        SourceKind::Pending => "virtual_session_runtime_pending_rows",
        SourceKind::RuntimeRunReceipts => "virtual_session_runtime_interruptions",
    };
    transaction
        .execute(&format!("DELETE FROM {table}"), [])
        .map_err(io::Error::other)?;
    if matches!(source, SourceKind::RuntimeRunReceipts) {
        transaction
            .execute(
                "DELETE FROM virtual_session_runtime_codex_usage_by_binding",
                [],
            )
            .map_err(io::Error::other)?;
        transaction
            .execute(
                "DELETE FROM virtual_session_runtime_codex_usage_fallback",
                [],
            )
            .map_err(io::Error::other)?;
    }
    Ok(())
}

fn source_has_rows(connection: &Connection, source: SourceKind) -> io::Result<bool> {
    let table = match source {
        SourceKind::Pending => "virtual_session_runtime_pending_rows",
        SourceKind::RuntimeRunReceipts => "virtual_session_runtime_interruptions",
    };
    let count: i64 = connection
        .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get(0)
        })
        .map_err(io::Error::other)?;
    if count > 0 || !matches!(source, SourceKind::RuntimeRunReceipts) {
        return Ok(count > 0);
    }
    let usage_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM virtual_session_runtime_codex_usage_fallback",
            [],
            |row| row.get(0),
        )
        .map_err(io::Error::other)?;
    Ok(usage_count > 0)
}

fn refresh_mode(
    file: &Path,
    metadata: &fs::Metadata,
    cursor: Option<&LedgerCursor>,
    label: &str,
) -> io::Result<RefreshMode> {
    let Some(cursor) = cursor else {
        return Ok(RefreshMode::Rebuild(format!("{label} index has no cursor")));
    };
    if cursor.source_modified_at_unix_nanos.is_none()
        || (cursor.offset_bytes > 0
            && (cursor.prefix_tail_fingerprint.is_none() || cursor.head_fingerprint.is_none()))
    {
        return Ok(RefreshMode::Rebuild(format!(
            "{label} index has no stable cursor fingerprint"
        )));
    }
    if metadata.len() < cursor.offset_bytes {
        return Ok(RefreshMode::Rebuild(format!(
            "{label} ledger was truncated"
        )));
    }
    let prefix_matches = prefix_tail_matches(file, cursor)?;
    let head_matches = head_matches(file, cursor)?;
    if metadata.len() == cursor.offset_bytes {
        if file_modified_at_unix_nanos(metadata) == cursor.source_modified_at_unix_nanos
            && prefix_matches
            && head_matches
        {
            return Ok(RefreshMode::Current);
        }
        return Ok(RefreshMode::Rebuild(format!(
            "{label} ledger changed without a stable append"
        )));
    }
    if prefix_matches && head_matches {
        Ok(RefreshMode::Append(cursor.clone()))
    } else {
        Ok(RefreshMode::Rebuild(format!(
            "{label} ledger prefix no longer matches its cursor"
        )))
    }
}

fn cursor_from_processed_ledger(
    file: &Path,
    offset_bytes: u64,
    line_number: usize,
    metadata: &fs::Metadata,
) -> io::Result<LedgerCursor> {
    Ok(LedgerCursor {
        offset_bytes,
        line_number,
        source_modified_at_unix_nanos: file_modified_at_unix_nanos(metadata),
        prefix_tail_fingerprint: prefix_tail_fingerprint(file, offset_bytes)?,
        head_fingerprint: head_fingerprint(file, offset_bytes)?,
    })
}

fn prefix_tail_matches(file: &Path, cursor: &LedgerCursor) -> io::Result<bool> {
    Ok(prefix_tail_fingerprint(file, cursor.offset_bytes)? == cursor.prefix_tail_fingerprint)
}
fn head_matches(file: &Path, cursor: &LedgerCursor) -> io::Result<bool> {
    Ok(head_fingerprint(file, cursor.offset_bytes)? == cursor.head_fingerprint)
}
fn prefix_tail_fingerprint(file: &Path, offset_bytes: u64) -> io::Result<Option<u64>> {
    if offset_bytes == 0 {
        return Ok(None);
    }
    let mut handle = File::open(file)?;
    let start = offset_bytes.saturating_sub(FINGERPRINT_BYTES);
    handle.seek(SeekFrom::Start(start))?;
    fingerprint_file_range(&mut handle, offset_bytes.saturating_sub(start)).map(Some)
}
fn head_fingerprint(file: &Path, offset_bytes: u64) -> io::Result<Option<u64>> {
    if offset_bytes == 0 {
        return Ok(None);
    }
    let mut handle = File::open(file)?;
    fingerprint_file_range(&mut handle, offset_bytes.min(FINGERPRINT_BYTES)).map(Some)
}
fn fingerprint_file_range(file: &mut File, mut remaining: u64) -> io::Result<u64> {
    let mut buffer = [0_u8; FINGERPRINT_BYTES as usize];
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    while remaining > 0 {
        let length = remaining.min(buffer.len() as u64) as usize;
        file.read_exact(&mut buffer[..length])?;
        for byte in &buffer[..length] {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        remaining = remaining.saturating_sub(length as u64);
    }
    Ok(hash)
}
fn file_modified_at_unix_nanos(metadata: &fs::Metadata) -> Option<u128> {
    metadata
        .modified()
        .ok()
        .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos())
}

fn read_cursor(connection: &Connection, source: SourceKind) -> io::Result<Option<LedgerCursor>> {
    let Some(value) = read_meta(connection, source.cursor_key())? else {
        return Ok(None);
    };
    serde_json::from_str(&value)
        .map(Some)
        .map_err(io::Error::other)
}
fn write_cursor(
    transaction: &Transaction<'_>,
    source: SourceKind,
    cursor: &LedgerCursor,
) -> io::Result<()> {
    write_meta(
        transaction,
        source.cursor_key(),
        &serde_json::to_string(cursor).map_err(io::Error::other)?,
    )
}
fn apply_counter_delta(
    transaction: &Transaction<'_>,
    source: SourceKind,
    delta: CounterDelta,
) -> io::Result<()> {
    for (key, delta) in [
        (source.total_key(), delta.total),
        (source.invalid_key(), delta.invalid),
    ] {
        let current = read_meta_i64_from_transaction(transaction, key)?;
        write_meta(transaction, key, &current.saturating_add(delta).to_string())?;
    }
    Ok(())
}
fn read_meta(connection: &Connection, key: &str) -> io::Result<Option<String>> {
    connection
        .query_row(
            "SELECT value FROM virtual_session_runtime_meta WHERE key = ?1",
            params![key],
            |row| row.get(0),
        )
        .optional()
        .map_err(io::Error::other)
}
fn write_meta(transaction: &Transaction<'_>, key: &str, value: &str) -> io::Result<()> {
    transaction.execute("INSERT INTO virtual_session_runtime_meta (key, value) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET value = excluded.value", params![key, value]).map_err(io::Error::other)?;
    Ok(())
}
fn delete_meta(transaction: &Transaction<'_>, key: &str) -> io::Result<()> {
    transaction
        .execute(
            "DELETE FROM virtual_session_runtime_meta WHERE key = ?1",
            params![key],
        )
        .map_err(io::Error::other)?;
    Ok(())
}
fn read_meta_i64_from_transaction(transaction: &Transaction<'_>, key: &str) -> io::Result<i64> {
    let value: Option<String> = transaction
        .query_row(
            "SELECT value FROM virtual_session_runtime_meta WHERE key = ?1",
            params![key],
            |row| row.get(0),
        )
        .optional()
        .map_err(io::Error::other)?;
    value
        .unwrap_or_else(|| "0".to_string())
        .parse::<i64>()
        .map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("virtual session runtime index metadata `{key}` is invalid: {error}"),
            )
        })
}

fn read_codex_usage_for_binding(
    connection: &Connection,
    binding_key: &str,
) -> io::Result<Option<CodexRuntimeUsage>> {
    let row = connection
        .query_row(
            "SELECT input_tokens, output_tokens, total_tokens, model_context_window,
                    model_context_window_source, provider, model, backend_context_generation,
                    observed_at_ms, source, raw
             FROM virtual_session_runtime_codex_usage_by_binding
             WHERE binding_key = ?1",
            params![binding_key],
            read_codex_usage_row,
        )
        .optional()
        .map_err(io::Error::other)?;
    row.map(codex_usage_from_index_row).transpose()
}

fn read_global_codex_usage(connection: &Connection) -> io::Result<Option<CodexRuntimeUsage>> {
    let row = connection
        .query_row(
            "SELECT input_tokens, output_tokens, total_tokens, model_context_window,
                    model_context_window_source, provider, model, backend_context_generation,
                    observed_at_ms, source, raw
             FROM virtual_session_runtime_codex_usage_fallback
             WHERE singleton = 1",
            [],
            read_codex_usage_row,
        )
        .optional()
        .map_err(io::Error::other)?;
    row.map(codex_usage_from_index_row).transpose()
}

fn read_codex_usage_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<(
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i64>,
    String,
    Option<String>,
)> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
        row.get(5)?,
        row.get(6)?,
        row.get(7)?,
        row.get(8)?,
        row.get(9)?,
        row.get(10)?,
    ))
}

fn codex_usage_from_index_row(
    (
        input_tokens,
        output_tokens,
        total_tokens,
        model_context_window,
        model_context_window_source,
        provider,
        model,
        backend_context_generation,
        observed_at_ms,
        source,
        raw,
    ): (
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<i64>,
        String,
        Option<String>,
    ),
) -> io::Result<CodexRuntimeUsage> {
    Ok(CodexRuntimeUsage {
        input_tokens: parse_optional_u64(input_tokens, "input token")?,
        output_tokens: parse_optional_u64(output_tokens, "output token")?,
        total_tokens: parse_optional_u64(total_tokens, "total token")?,
        model_context_window: parse_optional_u64(model_context_window, "model context window")?,
        model_context_window_source,
        provider,
        model,
        backend_context_generation,
        observed_at_ms,
        source,
        raw,
    })
}

fn parse_optional_u64(value: Option<String>, label: &str) -> io::Result<Option<u64>> {
    value
        .map(|value| {
            value.parse::<u64>().map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("virtual session runtime index has an invalid {label} value: {error}"),
                )
            })
        })
        .transpose()
}

fn string_field<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a str> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(Value::as_str))
}
fn nonempty_field<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a str> {
    string_field(value, keys)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}
fn i64_field(value: &Value, keys: &[&str]) -> Option<i64> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(Value::as_i64))
}
fn account_key(account_id: Option<&str>) -> String {
    match account_id
        .map(str::trim)
        .filter(|value| !value.is_empty() && !value.eq_ignore_ascii_case("default"))
    {
        Some(account_id) => format!("{ACCOUNT_KEY_PREFIX}{account_id}"),
        None => DEFAULT_ACCOUNT_KEY.to_string(),
    }
}

fn normalize_path_text_for_match(path: &str) -> String {
    path.replace('\\', "/").to_ascii_lowercase()
}

fn truncate_chars(value: &str, limit: usize) -> String {
    if limit == 0 {
        return String::new();
    }
    let mut chars = value.chars();
    let mut result = chars.by_ref().take(limit).collect::<String>();
    if chars.next().is_some() {
        let _ = result.pop();
        result.push('…');
    }
    result
}
fn i64_from_usize(value: usize, label: &str) -> io::Result<i64> {
    i64::try_from(value).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{label} exceeds SQLite range"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    #[test]
    fn lane_lookup_keeps_default_account_exact_and_tails_then_rebuilds_sources() {
        let root = std::env::temp_dir().join(format!(
            "agent-harness-virtual-session-runtime-index-test-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let harness_home = root.join(".agent-harness");
        let queue_dir = harness_home.join("state").join("runtime-queue");
        fs::create_dir_all(&queue_dir).unwrap();
        let pending_file = queue_dir.join("pending.jsonl");
        let runtime_file = queue_dir.join("codex-runtime-run-receipts.jsonl");
        fs::write(&pending_file, concat!(
            r#"{"queueId":"queue:default","platform":"discord","channelId":"dm-1","userId":"user-1","agentId":"main","sessionKey":"discord:dm-1:user-1:main"}"#, "\n",
            r#"{"queueId":"queue:other","platform":"discord","accountId":"other","channelId":"dm-1","userId":"user-1","agentId":"main","sessionKey":"discord:dm-1:user-1:main"}"#, "\n",
            r#"{"queueId":"queue:explicit-default","platform":"discord","accountId":"default","channelId":"dm-1","userId":"user-1","agentId":"main","sessionKey":"discord:dm-1:user-1:main:cont-1"}"#, "\n"
        )).unwrap();
        fs::write(&runtime_file, concat!(
            r#"{"queueId":"queue:default","interruptionReason":"interrupted_by_new_turn","reason":"new turn arrived","interruptedToolUses":[{"method":"shell","itemType":"command","preview":"cargo test","interruptedAtMs":12,"safeToRerun":true}]}"#, "\n",
            r#"{"queueId":"queue:other","interruptionReason":"interrupted_by_new_turn","reason":"other account","interruptedToolUses":[{"method":"shell","itemType":"command","preview":"do not leak","interruptedAtMs":20,"safeToRerun":false}]}"#, "\n"
        )).unwrap();
        let default_lane = lane(None);
        let other_lane = lane(Some("other"));
        let mut warnings = Vec::new();
        assert_eq!(
            recent_runtime_queue_ids_for_virtual_session_lane(
                &harness_home,
                &default_lane,
                5,
                &mut warnings
            )
            .unwrap(),
            vec!["queue:explicit-default", "queue:default"]
        );
        assert_eq!(
            recent_runtime_queue_ids_for_virtual_session_lane(
                &harness_home,
                &other_lane,
                5,
                &mut warnings
            )
            .unwrap(),
            vec!["queue:other"]
        );
        let interruption = latest_runtime_interruption_evidence_for_queue_ids(
            &harness_home,
            &BTreeSet::from(["queue:default".to_string()]),
            &mut warnings,
        )
        .unwrap()
        .unwrap();
        assert_eq!(interruption.queue_id, "queue:default");
        assert_eq!(interruption.method.as_deref(), Some("shell"));
        assert!(interruption.safe_to_rerun);
        fs::OpenOptions::new().append(true).open(&pending_file).unwrap().write_all(b"{\"queueId\":\"queue:new\",\"platform\":\"discord\",\"channelId\":\"dm-1\",\"userId\":\"user-1\",\"agentId\":\"main\",\"sessionKey\":\"discord:dm-1:user-1:main\"}\n").unwrap();
        assert_eq!(
            recent_runtime_queue_ids_for_virtual_session_lane(
                &harness_home,
                &default_lane,
                2,
                &mut warnings
            )
            .unwrap(),
            vec!["queue:new", "queue:explicit-default"]
        );
        fs::write(&pending_file, b"{\"queueId\":\"queue:replacement\",\"platform\":\"discord\",\"channelId\":\"dm-1\",\"userId\":\"user-1\",\"agentId\":\"main\",\"sessionKey\":\"discord:dm-1:user-1:main\"}\n").unwrap();
        fs::write(&runtime_file, b"{\"queueId\":\"queue:replacement\",\"interruptionReason\":\"interrupted_by_new_turn\",\"reason\":\"replacement\",\"interruptedToolUses\":[{\"method\":\"tool\",\"interruptedAtMs\":30,\"safeToRerun\":false}]}\n").unwrap();
        assert_eq!(
            recent_runtime_queue_ids_for_virtual_session_lane(
                &harness_home,
                &default_lane,
                5,
                &mut warnings
            )
            .unwrap(),
            vec!["queue:replacement"]
        );
        assert!(
            latest_runtime_interruption_evidence_for_queue_ids(
                &harness_home,
                &BTreeSet::from(["queue:default".to_string()]),
                &mut warnings
            )
            .unwrap()
            .is_none()
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn index_retains_only_latest_metadata_for_repeated_queue_records() {
        let root = std::env::temp_dir().join(format!(
            "agent-harness-virtual-session-runtime-index-bounded-test-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let harness_home = root.join(".agent-harness");
        let queue_dir = harness_home.join("state").join("runtime-queue");
        fs::create_dir_all(&queue_dir).unwrap();
        fs::write(queue_dir.join("pending.jsonl"), concat!(
            r#"{"queueId":"queue:one","platform":"discord","channelId":"dm-1","userId":"user-1","agentId":"main","sessionKey":"discord:dm-1:user-1:main"}"#, "\n",
            r#"{"queueId":"queue:one","platform":"discord","channelId":"dm-1","userId":"user-1","agentId":"main","sessionKey":"discord:dm-1:user-1:main"}"#, "\n"
        )).unwrap();
        fs::write(queue_dir.join("codex-runtime-run-receipts.jsonl"), concat!(
            r#"{"queueId":"queue:one","interruptionReason":"first","interruptedToolUses":[{"method":"tool","interruptedAtMs":1,"safeToRerun":false}]}"#, "\n",
            r#"{"queueId":"queue:one","interruptionReason":"second","interruptedToolUses":[{"method":"tool","interruptedAtMs":2,"safeToRerun":true}]}"#, "\n"
        )).unwrap();
        let mut warnings = Vec::new();
        refresh_virtual_session_runtime_index(&harness_home, &mut warnings).unwrap();
        let connection = open_index(&harness_home).unwrap();
        let pending: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM virtual_session_runtime_pending_rows",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let interruptions: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM virtual_session_runtime_interruptions",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(pending, 1);
        assert_eq!(interruptions, 1);
        assert_eq!(
            latest_runtime_interruption_evidence_for_queue_ids(
                &harness_home,
                &BTreeSet::from(["queue:one".to_string()]),
                &mut warnings
            )
            .unwrap()
            .unwrap()
            .interruption_reason,
            "second"
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn codex_usage_prefers_normalized_binding_then_global_fallback_and_tails() {
        let root = std::env::temp_dir().join(format!(
            "agent-harness-virtual-session-runtime-index-usage-test-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let harness_home = root.join(".agent-harness");
        let queue_dir = harness_home.join("state").join("runtime-queue");
        fs::create_dir_all(&queue_dir).unwrap();
        let receipts_file = queue_dir.join("codex-runtime-run-receipts.jsonl");
        fs::write(
            &receipts_file,
            concat!(
                r#"{"codexBindingFile":"c:/bindings/main.json","usage":{"inputTokens":10,"outputTokens":1,"totalTokens":11,"modelContextWindow":258400,"modelContextWindowSource":"params.tokenUsage.modelContextWindow","provider":"openai","model":"gpt-5.6-sol","backendContextGeneration":"codex-thread:main","observedAtMs":1700000000000,"source":"bound-old","raw":"bound-old-raw"}}"#,
                "\n",
                r#"{"codexBindingFile":"c:/bindings/other.json","usage":{"inputTokens":20,"outputTokens":2,"totalTokens":22,"source":"other"}}"#,
                "\n",
                r#"{"usage":{"inputTokens":30,"outputTokens":3,"totalTokens":33,"modelContextWindow":999999,"modelContextWindowSource":"wrong-lane","provider":"openai","model":"gpt-other","backendContextGeneration":"codex-thread:other","observedAtMs":1700000000000,"source":"global"}}"#,
                "\n"
            ),
        )
        .unwrap();
        let mut warnings = Vec::new();
        let bound = latest_codex_runtime_usage(
            &harness_home,
            Path::new(r"C:\BINDINGS\MAIN.JSON"),
            &mut warnings,
        )
        .unwrap();
        assert_eq!(bound.total_tokens, Some(11));
        assert_eq!(bound.source, "bound-old");
        assert_eq!(bound.raw.as_deref(), Some("bound-old-raw"));
        assert_eq!(bound.model_context_window, Some(258_400));
        assert_eq!(bound.provider.as_deref(), Some("openai"));
        assert_eq!(bound.model.as_deref(), Some("gpt-5.6-sol"));
        assert_eq!(
            bound.backend_context_generation.as_deref(),
            Some("codex-thread:main")
        );
        assert_eq!(bound.observed_at_ms, Some(1_700_000_000_000));
        let fallback = latest_codex_runtime_usage(
            &harness_home,
            Path::new(r"C:\BINDINGS\missing.json"),
            &mut warnings,
        )
        .unwrap();
        assert_eq!(fallback.total_tokens, Some(33));
        assert_eq!(fallback.source, "global");
        assert_eq!(fallback.model_context_window, None);
        assert_eq!(fallback.backend_context_generation, None);
        assert_eq!(fallback.observed_at_ms, None);

        fs::OpenOptions::new()
            .append(true)
            .open(&receipts_file)
            .unwrap()
            .write_all(
                br#"{"codexBindingFile":"C:\\bindings\\main.json","usage":{"inputTokens":40,"outputTokens":4,"totalTokens":44,"source":"bound-new"}}"#,
            )
            .unwrap();
        fs::OpenOptions::new()
            .append(true)
            .open(&receipts_file)
            .unwrap()
            .write_all(b"\n")
            .unwrap();
        let tailed = latest_codex_runtime_usage(
            &harness_home,
            Path::new(r"C:\BINDINGS\MAIN.JSON"),
            &mut warnings,
        )
        .unwrap();
        assert_eq!(tailed.total_tokens, Some(44));
        assert_eq!(tailed.source, "bound-new");

        let connection = open_index(&harness_home).unwrap();
        let bindings: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM virtual_session_runtime_codex_usage_by_binding",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let fallbacks: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM virtual_session_runtime_codex_usage_fallback",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(bindings, 2);
        assert_eq!(fallbacks, 1);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn busy_source_locks_return_empty_without_initializing_the_runtime_index() {
        let root = std::env::temp_dir().join(format!(
            "agent-harness-virtual-session-runtime-index-busy-empty-test-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let harness_home = root.join(".agent-harness");
        let queue_dir = harness_home.join("state").join("runtime-queue");
        fs::create_dir_all(&queue_dir).unwrap();
        let pending_file = queue_dir.join("pending.jsonl");
        let receipt_file = queue_dir.join("codex-runtime-run-receipts.jsonl");
        let pending_lock = append_lock_file(&pending_file);
        fs::write(&pending_lock, format!("pid={}\n", std::process::id())).unwrap();

        let mut warnings = Vec::new();
        let started = Instant::now();
        let queues = recent_runtime_queue_ids_for_virtual_session_lane(
            &harness_home,
            &lane(None),
            4,
            &mut warnings,
        )
        .unwrap();
        assert!(started.elapsed() < Duration::from_secs(2));
        assert!(queues.is_empty());
        assert!(!virtual_session_runtime_index_file(&harness_home).exists());
        fs::remove_file(pending_lock).unwrap();

        let receipt_lock = append_lock_file(&receipt_file);
        fs::write(&receipt_lock, format!("pid={}\n", std::process::id())).unwrap();
        let started = Instant::now();
        let interruption = latest_runtime_interruption_evidence_for_queue_ids(
            &harness_home,
            &BTreeSet::from(["queue:busy".to_string()]),
            &mut warnings,
        )
        .unwrap();
        assert!(started.elapsed() < Duration::from_secs(2));
        assert!(interruption.is_none());
        assert!(!virtual_session_runtime_index_file(&harness_home).exists());
        assert!(
            warnings
                .iter()
                .any(|warning| warning
                    .contains("using last committed virtual-session runtime state"))
        );

        fs::remove_file(receipt_lock).unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn busy_pending_lock_reads_the_last_committed_virtual_session_snapshot() {
        let root = std::env::temp_dir().join(format!(
            "agent-harness-virtual-session-runtime-index-busy-snapshot-test-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let harness_home = root.join(".agent-harness");
        let queue_dir = harness_home.join("state").join("runtime-queue");
        fs::create_dir_all(&queue_dir).unwrap();
        let pending_file = queue_dir.join("pending.jsonl");
        fs::write(
            &pending_file,
            b"{\"queueId\":\"queue:old\",\"platform\":\"discord\",\"channelId\":\"dm-1\",\"userId\":\"user-1\",\"agentId\":\"main\",\"sessionKey\":\"discord:dm-1:user-1:main\"}\n",
        )
        .unwrap();
        let mut warnings = Vec::new();
        refresh_virtual_session_runtime_index(&harness_home, &mut warnings).unwrap();
        fs::OpenOptions::new()
            .append(true)
            .open(&pending_file)
            .unwrap()
            .write_all(
                b"{\"queueId\":\"queue:new\",\"platform\":\"discord\",\"channelId\":\"dm-1\",\"userId\":\"user-1\",\"agentId\":\"main\",\"sessionKey\":\"discord:dm-1:user-1:main\"}\n",
            )
            .unwrap();
        let lock_file = append_lock_file(&pending_file);
        fs::write(&lock_file, format!("pid={}\n", std::process::id())).unwrap();

        let started = Instant::now();
        let snapshot = recent_runtime_queue_ids_for_virtual_session_lane(
            &harness_home,
            &lane(None),
            2,
            &mut warnings,
        )
        .unwrap();
        assert!(started.elapsed() < Duration::from_secs(2));
        assert_eq!(snapshot, vec!["queue:old"]);
        fs::remove_file(lock_file).unwrap();
        assert_eq!(
            recent_runtime_queue_ids_for_virtual_session_lane(
                &harness_home,
                &lane(None),
                2,
                &mut warnings,
            )
            .unwrap(),
            vec!["queue:new", "queue:old"]
        );

        let _ = fs::remove_dir_all(root);
    }

    fn append_lock_file(jsonl_file: &Path) -> PathBuf {
        let file_name = jsonl_file
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("ledger.jsonl");
        jsonl_file.with_file_name(format!(".{file_name}.append.lock"))
    }

    fn lane(account_id: Option<&str>) -> VirtualSessionRuntimeLaneQuery {
        VirtualSessionRuntimeLaneQuery {
            platform: "discord".to_string(),
            account_id: account_id.map(ToString::to_string),
            channel_id: "dm-1".to_string(),
            user_id: "user-1".to_string(),
            agent_id: "main".to_string(),
            root_session_key: "discord:dm-1:user-1:main".to_string(),
        }
    }
}
