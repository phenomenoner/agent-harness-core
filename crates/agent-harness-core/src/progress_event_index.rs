//! Durable cursor-backed lookup state for runtime progress events.
//!
//! `progress-events.jsonl` remains authoritative.  This SQLite sidecar only
//! tails stable source ranges while the source append lock is held.  Normal
//! lookups consequently use a small metadata/fingerprint check and indexed
//! SQL rather than replaying a potentially unbounded event journal.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::{Duration, UNIX_EPOCH};

use rusqlite::{
    Connection, ErrorCode, OpenFlags, OptionalExtension, Transaction, TransactionBehavior, params,
};
use serde::{Deserialize, Serialize};

use crate::logging::try_with_jsonl_append_lock;
#[cfg(test)]
use crate::logging::with_jsonl_append_lock;
use crate::progress::{AgentProgressEvent, agent_progress_events_file};

pub(crate) const PROGRESS_EVENT_INDEX_SCHEMA: &str = "agent-harness.progress-event-index.v2";

const PROGRESS_EVENT_INDEX_FILE_NAME: &str = "progress-event-index.sqlite";
const PROGRESS_EVENT_INDEX_REVISION: i64 = 5;
const META_SCHEMA: &str = "schema";
const META_REVISION: &str = "revision";
const META_CURSOR: &str = "progress-events-cursor";
const META_TOTAL_LINES: &str = "progress-events-total-lines";
const META_VALID_LINES: &str = "progress-events-valid-lines";
const META_INVALID_LINES: &str = "progress-events-invalid-lines";
const FINGERPRINT_BYTES: u64 = 4 * 1024;

/// The index is an operational sidecar, not a second unbounded receipt
/// journal. Callers may request fewer events, but can rely on this retained
/// window for latest/preemption and virtual-session evidence.
pub(crate) const PROGRESS_EVENT_INDEX_RETAINED_EVENTS_PER_QUEUE: usize = 32;

/// Open queues are never silently evicted.  Crossing this hard capacity is a
/// deliberate backpressure condition: the sidecar stops advancing rather
/// than losing a live progress surface.  This is intentionally independent
/// from the much smaller terminal-history window below.
pub(crate) const PROGRESS_EVENT_INDEX_MAX_OPEN_QUEUES: usize = 4_096;

/// Closed queues are historical evidence and may be bounded aggressively.
/// Keep this alias while callers migrate from the old "tracked queues" name.
pub(crate) const PROGRESS_EVENT_INDEX_MAX_TERMINAL_QUEUES: usize = 1_024;
/// Absolute upper bound on retained rows: every open queue, plus the bounded
/// terminal tier, keeps its first event, recent window, and (when different)
/// newest terminal event.  The canonical JSONL journal remains authoritative.
#[cfg(test)]
pub(crate) const PROGRESS_EVENT_INDEX_MAX_PAYLOAD_ROWS: usize =
    (PROGRESS_EVENT_INDEX_RETAINED_EVENTS_PER_QUEUE + 2)
        * (PROGRESS_EVENT_INDEX_MAX_OPEN_QUEUES + PROGRESS_EVENT_INDEX_MAX_TERMINAL_QUEUES);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IndexedProgressEvent {
    pub(crate) line_number: usize,
    pub(crate) event: AgentProgressEvent,
}

/// A bounded, source-authoritative delivery read.  `events` only contains
/// requested queues plus queues newly observed after the caller cursor (or
/// all retained queues during a migration/reset); it never replays JSONL.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ProgressEventDeliverySnapshot {
    pub(crate) events: BTreeMap<String, Vec<IndexedProgressEvent>>,
    pub(crate) cursor: ProgressEventIndexCursor,
    pub(crate) reset: bool,
    pub(crate) fresh: bool,
    pub(crate) used_last_committed_snapshot: bool,
    pub(crate) available: bool,
    pub(crate) observed_delta_events: usize,
    pub(crate) source_valid_delta: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ProgressEventIndexCursor {
    pub(crate) offset_bytes: u64,
    pub(crate) line_number: usize,
    pub(crate) projection_generation: u64,
    pub(crate) total_lines: usize,
    pub(crate) valid_lines: usize,
    pub(crate) invalid_lines: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProgressEventLedgerCursor {
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
    #[serde(default)]
    projection_generation: u64,
}

enum LedgerRefreshMode {
    Current,
    Append(ProgressEventLedgerCursor),
    Rebuild(String),
}

#[derive(Debug, Clone, Copy, Default)]
struct ProgressEventCounterDelta {
    total: i64,
    valid: i64,
    invalid: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProgressEventIndexReadMode {
    Current,
    Snapshot,
    Fallback,
}

#[derive(Debug)]
struct ProgressEventIndexRead<T> {
    value: T,
    mode: ProgressEventIndexReadMode,
}

/// Returns the durable SQLite sidecar colocated with `progress-events.jsonl`.
pub(crate) fn progress_event_index_file(harness_home: impl AsRef<Path>) -> PathBuf {
    agent_progress_events_file(harness_home).with_file_name(PROGRESS_EVENT_INDEX_FILE_NAME)
}

/// Refreshes the sidecar from the canonical source.  An unchanged ledger only
/// checks its persisted cursor and two bounded fingerprints; it is never
/// replayed in full on the normal path.
#[cfg(test)]
pub(crate) fn refresh_progress_event_index(
    harness_home: impl AsRef<Path>,
    warnings: &mut Vec<String>,
) -> io::Result<()> {
    let harness_home = harness_home.as_ref();
    let events_file = agent_progress_events_file(harness_home);
    if let Some(parent) = events_file.parent() {
        fs::create_dir_all(parent)?;
    }
    with_jsonl_append_lock(&events_file, || {
        let mut connection = open_progress_event_index(harness_home)?;
        refresh_progress_event_index_locked(&mut connection, &events_file, warnings)
    })
}

/// Looks up the latest canonical progress event for one queue without a
/// full-ledger scan.
pub(crate) fn latest_progress_event_for_queue(
    harness_home: impl AsRef<Path>,
    queue_id: &str,
    warnings: &mut Vec<String>,
) -> io::Result<Option<IndexedProgressEvent>> {
    if queue_id.trim().is_empty() {
        return Ok(None);
    }
    with_current_or_last_committed_progress_event_index(
        harness_home.as_ref(),
        warnings,
        |connection| {
            let row: Option<(i64, String)> = connection
                .query_row(
                    "SELECT line_number, event_json \
                 FROM progress_event_index_lines \
                 WHERE queue_id = ?1 AND parse_error IS NULL \
                 ORDER BY line_number DESC LIMIT 1",
                    params![queue_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(io::Error::other)?;
            row.map(|(line_number, event_json)| indexed_event(line_number, event_json))
                .transpose()
        },
        || None,
    )
    .map(|read| read.value)
}

/// Returns at most `max_events_per_queue` recent events for each requested
/// queue.  Each queue uses an indexed, bounded query and the returned events
/// are restored to source order for callers that render a panel or timeline.
pub(crate) fn progress_events_for_queue_ids(
    harness_home: impl AsRef<Path>,
    queue_ids: &std::collections::BTreeSet<String>,
    max_events_per_queue: usize,
    warnings: &mut Vec<String>,
) -> io::Result<BTreeMap<String, Vec<IndexedProgressEvent>>> {
    if queue_ids.is_empty() || max_events_per_queue == 0 {
        return Ok(BTreeMap::new());
    }
    let limit = i64::try_from(max_events_per_queue).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "progress event query limit exceeds SQLite range",
        )
    })?;
    with_current_or_last_committed_progress_event_index(
        harness_home.as_ref(),
        warnings,
        |connection| {
            let mut selected = BTreeMap::new();
            let mut statement = connection
                .prepare(
                    "SELECT line_number, event_json \
                 FROM progress_event_index_lines \
                 WHERE queue_id = ?1 AND parse_error IS NULL \
                 ORDER BY line_number DESC LIMIT ?2",
                )
                .map_err(io::Error::other)?;
            for queue_id in queue_ids {
                if queue_id.trim().is_empty() {
                    continue;
                }
                let rows = statement
                    .query_map(params![queue_id, limit], |row| {
                        Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
                    })
                    .map_err(io::Error::other)?;
                let mut events = Vec::new();
                for row in rows {
                    let (line_number, event_json) = row.map_err(io::Error::other)?;
                    events.push(indexed_event(line_number, event_json)?);
                }
                events.reverse();
                if !events.is_empty() {
                    selected.insert(queue_id.clone(), events);
                }
            }
            Ok(selected)
        },
        BTreeMap::new,
    )
    .map(|read| read.value)
}

/// Reads only the bounded projection needed for a normal interactive delivery
/// pass.  The caller supplies its prior indexed cursor and the queues it
/// already owns.  A fresh read tails the authoritative source under the JSONL
/// append lock; a busy writer falls back to the last committed read-only
/// SQLite snapshot without opening a writable database.
pub(crate) fn progress_event_delivery_snapshot(
    harness_home: impl AsRef<Path>,
    known_queue_ids: &BTreeSet<String>,
    after_line: usize,
    after_generation: u64,
    after_valid_lines: usize,
    include_all_retained_queues: bool,
    warnings: &mut Vec<String>,
) -> io::Result<ProgressEventDeliverySnapshot> {
    let read = with_current_or_last_committed_progress_event_index(
        harness_home.as_ref(),
        warnings,
        |connection| {
            read_progress_event_delivery_snapshot(
                connection,
                known_queue_ids,
                after_line,
                after_generation,
                after_valid_lines,
                include_all_retained_queues,
            )
        },
        ProgressEventDeliverySnapshot::default,
    )?;
    let mut snapshot = read.value;
    snapshot.fresh = read.mode == ProgressEventIndexReadMode::Current;
    snapshot.used_last_committed_snapshot = read.mode == ProgressEventIndexReadMode::Snapshot;
    snapshot.available = read.mode != ProgressEventIndexReadMode::Fallback;
    Ok(snapshot)
}

/// Runs a user-visible lookup from a freshly tailed index when the source
/// append lock is immediately available. If a writer owns that source lock,
/// this deliberately avoids opening or initializing a writable SQLite
/// connection: it reads an existing read-only snapshot instead, or returns a
/// conservative caller-supplied result when no snapshot is immediately
/// readable.
fn with_current_or_last_committed_progress_event_index<T>(
    harness_home: &Path,
    warnings: &mut Vec<String>,
    operation: impl Fn(&Connection) -> io::Result<T>,
    conservative_fallback: impl FnOnce() -> T,
) -> io::Result<ProgressEventIndexRead<T>> {
    let events_file = agent_progress_events_file(harness_home);
    match try_with_jsonl_append_lock(&events_file, || {
        let mut connection = open_progress_event_index_hot(harness_home)?;
        refresh_progress_event_index_locked(&mut connection, &events_file, warnings)?;
        operation(&connection)
    }) {
        Ok(Some(value)) => Ok(ProgressEventIndexRead {
            value,
            mode: ProgressEventIndexReadMode::Current,
        }),
        Ok(None) => {
            warnings.push(format!(
                "progress event index refresh is waiting for a live append at {}; using last committed progress state",
                events_file.display()
            ));
            read_progress_event_index_snapshot(
                harness_home,
                warnings,
                &operation,
                conservative_fallback,
            )
        }
        Err(error) if error.kind() == io::ErrorKind::WouldBlock || sqlite_error_is_busy(&error) => {
            warnings.push(format!(
                "progress event index is applying backpressure at {}; using last committed progress state: {error}",
                events_file.display()
            ));
            read_progress_event_index_snapshot(
                harness_home,
                warnings,
                &operation,
                conservative_fallback,
            )
        }
        Err(error) => Err(error),
    }
}

fn read_progress_event_index_snapshot<T>(
    harness_home: &Path,
    warnings: &mut Vec<String>,
    operation: &impl Fn(&Connection) -> io::Result<T>,
    conservative_fallback: impl FnOnce() -> T,
) -> io::Result<ProgressEventIndexRead<T>> {
    let index_file = progress_event_index_file(harness_home);
    let connection = match open_existing_progress_event_index_snapshot(&index_file) {
        Ok(Some(connection)) => connection,
        Ok(None) => {
            warnings.push(format!(
                "progress event index snapshot is unavailable at {}; returning conservative empty progress state",
                index_file.display()
            ));
            return Ok(ProgressEventIndexRead {
                value: conservative_fallback(),
                mode: ProgressEventIndexReadMode::Fallback,
            });
        }
        Err(error) => {
            warnings.push(format!(
                "failed to open progress event index snapshot at {}; returning conservative empty progress state: {error}",
                index_file.display()
            ));
            return Ok(ProgressEventIndexRead {
                value: conservative_fallback(),
                mode: ProgressEventIndexReadMode::Fallback,
            });
        }
    };
    match operation(&connection) {
        Ok(value) => Ok(ProgressEventIndexRead {
            value,
            mode: ProgressEventIndexReadMode::Snapshot,
        }),
        Err(error) => {
            warnings.push(format!(
                "failed to read progress event index snapshot at {}; returning conservative empty progress state: {error}",
                index_file.display()
            ));
            Ok(ProgressEventIndexRead {
                value: conservative_fallback(),
                mode: ProgressEventIndexReadMode::Fallback,
            })
        }
    }
}

fn open_existing_progress_event_index_snapshot(
    index_file: &Path,
) -> io::Result<Option<Connection>> {
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

fn read_progress_event_delivery_snapshot(
    connection: &Connection,
    known_queue_ids: &BTreeSet<String>,
    after_line: usize,
    after_generation: u64,
    after_valid_lines: usize,
    include_all_retained_queues: bool,
) -> io::Result<ProgressEventDeliverySnapshot> {
    let ledger = read_cursor(connection)?.unwrap_or_default();
    let cursor = progress_event_index_cursor(connection, &ledger)?;
    let reset = (after_generation > 0 && after_generation != cursor.projection_generation)
        || after_line > cursor.line_number;
    let effective_after_line = if reset { 0 } else { after_line };
    let delta_events = indexed_events_after_line(connection, effective_after_line)?;
    let mut queue_ids = known_queue_ids
        .iter()
        .filter(|queue_id| !queue_id.trim().is_empty())
        .cloned()
        .collect::<BTreeSet<_>>();
    queue_ids.extend(
        delta_events
            .iter()
            .map(|stored| stored.event.queue_id.clone())
            .filter(|queue_id| !queue_id.trim().is_empty()),
    );
    if include_all_retained_queues || reset {
        queue_ids.extend(indexed_queue_ids(connection)?);
    }
    let events = indexed_events_for_queue_ids(connection, &queue_ids)?;
    let source_valid_delta = if reset {
        cursor.valid_lines
    } else {
        cursor.valid_lines.saturating_sub(after_valid_lines)
    };
    Ok(ProgressEventDeliverySnapshot {
        events,
        cursor,
        reset,
        fresh: false,
        used_last_committed_snapshot: false,
        available: false,
        observed_delta_events: delta_events.len(),
        source_valid_delta,
    })
}

fn progress_event_index_cursor(
    connection: &Connection,
    ledger: &ProgressEventLedgerCursor,
) -> io::Result<ProgressEventIndexCursor> {
    Ok(ProgressEventIndexCursor {
        offset_bytes: ledger.offset_bytes,
        line_number: ledger.line_number,
        projection_generation: ledger.projection_generation,
        total_lines: usize_from_meta(
            read_meta_i64(connection, META_TOTAL_LINES)?,
            META_TOTAL_LINES,
        )?,
        valid_lines: usize_from_meta(
            read_meta_i64(connection, META_VALID_LINES)?,
            META_VALID_LINES,
        )?,
        invalid_lines: usize_from_meta(
            read_meta_i64(connection, META_INVALID_LINES)?,
            META_INVALID_LINES,
        )?,
    })
}

fn indexed_events_after_line(
    connection: &Connection,
    after_line: usize,
) -> io::Result<Vec<IndexedProgressEvent>> {
    let after_line = i64_from_usize(after_line, "progress event index cursor line")?;
    let rows = connection
        .prepare(
            "SELECT line_number, event_json \
             FROM progress_event_index_lines \
             WHERE parse_error IS NULL AND line_number > ?1 \
             ORDER BY line_number ASC",
        )
        .and_then(|mut statement| {
            let rows = statement.query_map(params![after_line], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })?;
            rows.collect::<Result<Vec<_>, _>>()
        })
        .map_err(io::Error::other)?;
    rows.into_iter()
        .map(|(line_number, event_json)| indexed_event(line_number, event_json))
        .collect()
}

fn indexed_queue_ids(connection: &Connection) -> io::Result<BTreeSet<String>> {
    let rows = connection
        .prepare(
            "SELECT DISTINCT queue_id FROM progress_event_index_lines \
             WHERE parse_error IS NULL AND queue_id IS NOT NULL AND queue_id <> ''",
        )
        .and_then(|mut statement| {
            let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
            rows.collect::<Result<Vec<_>, _>>()
        })
        .map_err(io::Error::other)?;
    Ok(rows.into_iter().collect())
}

fn indexed_events_for_queue_ids(
    connection: &Connection,
    queue_ids: &BTreeSet<String>,
) -> io::Result<BTreeMap<String, Vec<IndexedProgressEvent>>> {
    let mut selected = BTreeMap::new();
    let mut statement = connection
        .prepare(
            "SELECT line_number, event_json \
             FROM progress_event_index_lines \
             WHERE queue_id = ?1 AND parse_error IS NULL \
             ORDER BY line_number ASC",
        )
        .map_err(io::Error::other)?;
    for queue_id in queue_ids {
        if queue_id.trim().is_empty() {
            continue;
        }
        let rows = statement
            .query_map(params![queue_id], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(io::Error::other)?;
        let mut events = Vec::new();
        for row in rows {
            let (line_number, event_json) = row.map_err(io::Error::other)?;
            events.push(indexed_event(line_number, event_json)?);
        }
        if !events.is_empty() {
            selected.insert(queue_id.clone(), events);
        }
    }
    Ok(selected)
}

fn usize_from_meta(value: i64, label: &str) -> io::Result<usize> {
    usize::try_from(value).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("progress event index metadata `{label}` is invalid"),
        )
    })
}

#[cfg(test)]
fn open_progress_event_index(harness_home: &Path) -> io::Result<Connection> {
    open_progress_event_index_with_busy_timeout(harness_home, Duration::from_secs(10))
}

fn open_progress_event_index_hot(harness_home: &Path) -> io::Result<Connection> {
    open_progress_event_index_with_busy_timeout(harness_home, Duration::from_millis(0))
}

fn open_progress_event_index_with_busy_timeout(
    harness_home: &Path,
    busy_timeout: Duration,
) -> io::Result<Connection> {
    let index_file = progress_event_index_file(harness_home);
    if let Some(parent) = index_file.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut connection = Connection::open(index_file).map_err(io::Error::other)?;
    connection
        .busy_timeout(busy_timeout)
        .map_err(io::Error::other)?;
    initialize_progress_event_index(&mut connection)?;
    Ok(connection)
}

fn sqlite_error_is_busy(error: &io::Error) -> bool {
    error
        .get_ref()
        .and_then(|source| source.downcast_ref::<rusqlite::Error>())
        .is_some_and(|source| {
            matches!(
                source,
                rusqlite::Error::SqliteFailure(code, _)
                    if matches!(code.code, ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
            )
        })
}

fn initialize_progress_event_index(connection: &mut Connection) -> io::Result<()> {
    connection
        .execute_batch(
            "
            CREATE TABLE IF NOT EXISTS progress_event_index_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            ",
        )
        .map_err(io::Error::other)?;
    let schema = read_meta(connection, META_SCHEMA)?;
    let revision = read_meta(connection, META_REVISION)?;
    if schema.as_deref() == Some(PROGRESS_EVENT_INDEX_SCHEMA)
        && revision.as_deref() == Some(&PROGRESS_EVENT_INDEX_REVISION.to_string())
    {
        connection
            .execute_batch(
                "
            CREATE TABLE IF NOT EXISTS progress_event_index_lines (
                line_number INTEGER PRIMARY KEY,
                queue_id TEXT,
                event_id TEXT,
                is_terminal INTEGER NOT NULL DEFAULT 0,
                event_json TEXT,
                parse_error TEXT
            );
            CREATE INDEX IF NOT EXISTS progress_event_index_queue_line_idx
                ON progress_event_index_lines(queue_id, line_number)
                WHERE parse_error IS NULL;
            CREATE INDEX IF NOT EXISTS progress_event_index_event_id_idx
                ON progress_event_index_lines(event_id)
                WHERE event_id IS NOT NULL AND event_id <> '';
            ",
            )
            .map_err(io::Error::other)?;
        return Ok(());
    }

    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(io::Error::other)?;
    transaction
        .execute_batch(
            "
            DROP TABLE IF EXISTS progress_event_index_lines;
            CREATE TABLE progress_event_index_lines (
                line_number INTEGER PRIMARY KEY,
                queue_id TEXT,
                event_id TEXT,
                is_terminal INTEGER NOT NULL DEFAULT 0,
                event_json TEXT,
                parse_error TEXT
            );
            CREATE INDEX progress_event_index_queue_line_idx
                ON progress_event_index_lines(queue_id, line_number)
                WHERE parse_error IS NULL;
            CREATE INDEX progress_event_index_event_id_idx
                ON progress_event_index_lines(event_id)
                WHERE event_id IS NOT NULL AND event_id <> '';
            ",
        )
        .map_err(io::Error::other)?;
    transaction
        .execute("DELETE FROM progress_event_index_meta", [])
        .map_err(io::Error::other)?;
    write_meta(&transaction, META_SCHEMA, PROGRESS_EVENT_INDEX_SCHEMA)?;
    write_meta(
        &transaction,
        META_REVISION,
        &PROGRESS_EVENT_INDEX_REVISION.to_string(),
    )?;
    write_meta(&transaction, META_TOTAL_LINES, "0")?;
    write_meta(&transaction, META_VALID_LINES, "0")?;
    write_meta(&transaction, META_INVALID_LINES, "0")?;
    transaction.commit().map_err(io::Error::other)
}

fn refresh_progress_event_index_locked(
    connection: &mut Connection,
    events_file: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<()> {
    let cursor = read_cursor(connection)?;
    let metadata = match fs::metadata(events_file) {
        Ok(metadata) if metadata.is_file() => metadata,
        Ok(_) => {
            warnings.push(format!(
                "progress event ledger is not a file; rebuilding index: {}",
                events_file.display()
            ));
            reset_progress_event_index(connection)?;
            return Ok(());
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if cursor.is_some() || progress_event_index_has_rows(connection)? {
                warnings.push("progress event ledger disappeared; rebuilding index".to_string());
                reset_progress_event_index(connection)?;
            }
            return Ok(());
        }
        Err(error) => return Err(error),
    };

    match progress_event_refresh_mode(events_file, &metadata, cursor.as_ref())? {
        LedgerRefreshMode::Current => Ok(()),
        LedgerRefreshMode::Append(cursor) => {
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(io::Error::other)?;
            let (next_cursor, delta) = read_progress_events_from_cursor(
                &transaction,
                events_file,
                cursor,
                warnings,
                "refreshing",
            )?;
            apply_counter_delta(&transaction, delta)?;
            retain_bounded_progress_event_payloads(&transaction)?;
            write_cursor(&transaction, &next_cursor)?;
            transaction.commit().map_err(io::Error::other)
        }
        LedgerRefreshMode::Rebuild(reason) => {
            warnings.push(format!("{reason}; rebuilding progress event index"));
            rebuild_progress_event_index_locked(connection, events_file, warnings)
        }
    }
}

fn rebuild_progress_event_index_locked(
    connection: &mut Connection,
    events_file: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<()> {
    let next_generation = next_projection_generation(connection)?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(io::Error::other)?;
    transaction
        .execute("DELETE FROM progress_event_index_lines", [])
        .map_err(io::Error::other)?;
    delete_meta(&transaction, META_CURSOR)?;
    write_meta(&transaction, META_TOTAL_LINES, "0")?;
    write_meta(&transaction, META_VALID_LINES, "0")?;
    write_meta(&transaction, META_INVALID_LINES, "0")?;
    let (cursor, delta) = read_progress_events_from_cursor(
        &transaction,
        events_file,
        ProgressEventLedgerCursor {
            projection_generation: next_generation,
            ..ProgressEventLedgerCursor::default()
        },
        warnings,
        "rebuilding",
    )?;
    apply_counter_delta(&transaction, delta)?;
    retain_bounded_progress_event_payloads(&transaction)?;
    write_cursor(&transaction, &cursor)?;
    transaction.commit().map_err(io::Error::other)
}

fn reset_progress_event_index(connection: &mut Connection) -> io::Result<()> {
    let next_generation = next_projection_generation(connection)?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(io::Error::other)?;
    transaction
        .execute("DELETE FROM progress_event_index_lines", [])
        .map_err(io::Error::other)?;
    write_cursor(
        &transaction,
        &ProgressEventLedgerCursor {
            projection_generation: next_generation,
            ..ProgressEventLedgerCursor::default()
        },
    )?;
    write_meta(&transaction, META_TOTAL_LINES, "0")?;
    write_meta(&transaction, META_VALID_LINES, "0")?;
    write_meta(&transaction, META_INVALID_LINES, "0")?;
    transaction.commit().map_err(io::Error::other)
}

fn read_progress_events_from_cursor(
    transaction: &Transaction<'_>,
    events_file: &Path,
    cursor: ProgressEventLedgerCursor,
    warnings: &mut Vec<String>,
    phase: &str,
) -> io::Result<(ProgressEventLedgerCursor, ProgressEventCounterDelta)> {
    let projection_generation = cursor.projection_generation;
    let file = File::open(events_file)?;
    let file_len = file.metadata()?.len();
    let start_offset = cursor.offset_bytes.min(file_len);
    let mut reader = BufReader::new(file);
    reader.seek(SeekFrom::Start(start_offset))?;
    let mut offset_bytes = start_offset;
    let mut line_number = if cursor.offset_bytes > file_len {
        0
    } else {
        cursor.line_number
    };
    let mut delta = ProgressEventCounterDelta::default();

    loop {
        let mut line = String::new();
        let bytes_read = reader.read_line(&mut line)?;
        if bytes_read == 0 {
            break;
        }
        let complete_line = line.ends_with('\n');
        let trimmed = line.trim();
        if !complete_line
            && !trimmed.is_empty()
            && serde_json::from_str::<AgentProgressEvent>(trimmed).is_err()
        {
            // Do not permanently skip a writer's partial append. The next
            // refresh will retry from this exact source offset.
            break;
        }
        line_number = line_number.saturating_add(1);
        offset_bytes = offset_bytes.saturating_add(bytes_read as u64);
        delta.total = delta.total.saturating_add(1);
        if trimmed.is_empty() {
            delta.invalid = delta.invalid.saturating_add(1);
            continue;
        }
        match serde_json::from_str::<AgentProgressEvent>(trimmed) {
            Ok(event) if event.queue_id.trim().is_empty() => {
                let message = "progress event has an empty queue id".to_string();
                warnings.push(format!(
                    "progress event line {line_number} is invalid while {phase}: {message}"
                ));
                delta.invalid = delta.invalid.saturating_add(1);
            }
            Ok(event) => {
                insert_progress_event(transaction, line_number, trimmed, &event)?;
                delta.valid = delta.valid.saturating_add(1);
            }
            Err(error) => {
                let message = error.to_string();
                warnings.push(format!(
                    "progress event line {line_number} is not valid JSON while {phase}: {message}"
                ));
                delta.invalid = delta.invalid.saturating_add(1);
            }
        }
    }
    let metadata = reader.get_ref().metadata()?;
    Ok((
        cursor_from_processed_ledger(
            events_file,
            offset_bytes,
            line_number,
            &metadata,
            projection_generation,
        )?,
        delta,
    ))
}

fn insert_progress_event(
    transaction: &Transaction<'_>,
    line_number: usize,
    event_json: &str,
    event: &AgentProgressEvent,
) -> io::Result<()> {
    transaction
        .execute(
            "INSERT INTO progress_event_index_lines \
             (line_number, queue_id, event_id, is_terminal, event_json, parse_error) \
             VALUES (?1, ?2, ?3, ?4, ?5, NULL)",
            params![
                i64_from_usize(line_number, "progress event line number")?,
                event.queue_id.as_str(),
                event.event_id.as_deref(),
                if progress_event_is_terminal(event) {
                    1_i64
                } else {
                    0_i64
                },
                event_json,
            ],
        )
        .map_err(io::Error::other)?;
    Ok(())
}

fn progress_event_refresh_mode(
    events_file: &Path,
    metadata: &fs::Metadata,
    cursor: Option<&ProgressEventLedgerCursor>,
) -> io::Result<LedgerRefreshMode> {
    let Some(cursor) = cursor else {
        return Ok(LedgerRefreshMode::Rebuild(
            "progress event index has no cursor".to_string(),
        ));
    };
    if cursor.source_modified_at_unix_nanos.is_none()
        || (cursor.offset_bytes > 0
            && (cursor.prefix_tail_fingerprint.is_none() || cursor.head_fingerprint.is_none()))
    {
        return Ok(LedgerRefreshMode::Rebuild(
            "progress event index has no stable cursor fingerprint".to_string(),
        ));
    }
    if metadata.len() < cursor.offset_bytes {
        return Ok(LedgerRefreshMode::Rebuild(
            "progress event ledger was truncated".to_string(),
        ));
    }
    let source_modified_at_unix_nanos = file_modified_at_unix_nanos(metadata);
    let prefix_matches = ledger_prefix_tail_matches(events_file, cursor)?;
    let head_matches = ledger_head_matches(events_file, cursor)?;
    if metadata.len() == cursor.offset_bytes {
        if source_modified_at_unix_nanos == cursor.source_modified_at_unix_nanos
            && prefix_matches
            && head_matches
        {
            return Ok(LedgerRefreshMode::Current);
        }
        return Ok(LedgerRefreshMode::Rebuild(
            "progress event ledger changed without a stable append".to_string(),
        ));
    }
    if prefix_matches && head_matches {
        return Ok(LedgerRefreshMode::Append(cursor.clone()));
    }
    Ok(LedgerRefreshMode::Rebuild(
        "progress event ledger prefix no longer matches its cursor".to_string(),
    ))
}

fn cursor_from_processed_ledger(
    events_file: &Path,
    offset_bytes: u64,
    line_number: usize,
    metadata: &fs::Metadata,
    projection_generation: u64,
) -> io::Result<ProgressEventLedgerCursor> {
    Ok(ProgressEventLedgerCursor {
        offset_bytes,
        line_number,
        source_modified_at_unix_nanos: file_modified_at_unix_nanos(metadata),
        prefix_tail_fingerprint: ledger_prefix_tail_fingerprint(events_file, offset_bytes)?,
        head_fingerprint: ledger_head_fingerprint(events_file, offset_bytes)?,
        projection_generation,
    })
}

fn next_projection_generation(connection: &Connection) -> io::Result<u64> {
    Ok(read_cursor(connection)?
        .map(|cursor| cursor.projection_generation.saturating_add(1).max(1))
        .unwrap_or(1))
}

fn progress_event_is_terminal(event: &AgentProgressEvent) -> bool {
    matches!(
        event.status,
        crate::progress::AgentProgressStatus::Completed
            | crate::progress::AgentProgressStatus::Failed
    )
}

fn ledger_prefix_tail_matches(
    events_file: &Path,
    cursor: &ProgressEventLedgerCursor,
) -> io::Result<bool> {
    Ok(
        ledger_prefix_tail_fingerprint(events_file, cursor.offset_bytes)?
            == cursor.prefix_tail_fingerprint,
    )
}

fn ledger_head_matches(events_file: &Path, cursor: &ProgressEventLedgerCursor) -> io::Result<bool> {
    Ok(ledger_head_fingerprint(events_file, cursor.offset_bytes)? == cursor.head_fingerprint)
}

fn ledger_prefix_tail_fingerprint(
    events_file: &Path,
    offset_bytes: u64,
) -> io::Result<Option<u64>> {
    if offset_bytes == 0 {
        return Ok(None);
    }
    let mut handle = File::open(events_file)?;
    let start = offset_bytes.saturating_sub(FINGERPRINT_BYTES);
    handle.seek(SeekFrom::Start(start))?;
    fingerprint_file_range(&mut handle, offset_bytes.saturating_sub(start)).map(Some)
}

fn ledger_head_fingerprint(events_file: &Path, offset_bytes: u64) -> io::Result<Option<u64>> {
    if offset_bytes == 0 {
        return Ok(None);
    }
    let mut handle = File::open(events_file)?;
    fingerprint_file_range(&mut handle, offset_bytes.min(FINGERPRINT_BYTES)).map(Some)
}

fn fingerprint_file_range(file: &mut File, mut remaining: u64) -> io::Result<u64> {
    let mut buffer = [0_u8; FINGERPRINT_BYTES as usize];
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
    Ok(hash)
}

fn file_modified_at_unix_nanos(metadata: &fs::Metadata) -> Option<u128> {
    metadata
        .modified()
        .ok()
        .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos())
}

fn read_cursor(connection: &Connection) -> io::Result<Option<ProgressEventLedgerCursor>> {
    let Some(value) = read_meta(connection, META_CURSOR)? else {
        return Ok(None);
    };
    serde_json::from_str(&value)
        .map(Some)
        .map_err(io::Error::other)
}

fn write_cursor(
    transaction: &Transaction<'_>,
    cursor: &ProgressEventLedgerCursor,
) -> io::Result<()> {
    let encoded = serde_json::to_string(cursor).map_err(io::Error::other)?;
    write_meta(transaction, META_CURSOR, &encoded)
}

fn apply_counter_delta(
    transaction: &Transaction<'_>,
    delta: ProgressEventCounterDelta,
) -> io::Result<()> {
    for (key, increment) in [
        (META_TOTAL_LINES, delta.total),
        (META_VALID_LINES, delta.valid),
        (META_INVALID_LINES, delta.invalid),
    ] {
        let current = read_meta_i64_from_transaction(transaction, key)?;
        write_meta(
            transaction,
            key,
            &current.saturating_add(increment).to_string(),
        )?;
    }
    Ok(())
}

fn retain_bounded_progress_event_payloads(transaction: &Transaction<'_>) -> io::Result<()> {
    retain_bounded_progress_event_payloads_with_limits(
        transaction,
        PROGRESS_EVENT_INDEX_RETAINED_EVENTS_PER_QUEUE,
        PROGRESS_EVENT_INDEX_MAX_OPEN_QUEUES,
        PROGRESS_EVENT_INDEX_MAX_TERMINAL_QUEUES,
    )
}

/// Retains every live queue window and only bounds terminal history.  If live
/// concurrency exceeds `max_open_queues`, leave the committed sidecar intact
/// and report explicit backpressure rather than silently deleting an active
/// queue.  The transaction caller will roll back its source-tail update.
fn retain_bounded_progress_event_payloads_with_limits(
    transaction: &Transaction<'_>,
    events_per_queue: usize,
    max_open_queues: usize,
    max_terminal_queues: usize,
) -> io::Result<()> {
    let retained = i64::try_from(events_per_queue).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "progress event retained-window size exceeds SQLite range",
        )
    })?;
    let max_open_queues = i64::try_from(max_open_queues).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "progress event open-queue limit exceeds SQLite range",
        )
    })?;
    let max_terminal_queues = i64::try_from(max_terminal_queues).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "progress event terminal-queue limit exceeds SQLite range",
        )
    })?;
    let open_queues: i64 = transaction
        .query_row(
            "SELECT COUNT(*) FROM (\
             SELECT queue_id FROM progress_event_index_lines \
             WHERE parse_error IS NULL \
             GROUP BY queue_id HAVING MAX(is_terminal) = 0\
             )",
            [],
            |row| row.get(0),
        )
        .map_err(io::Error::other)?;
    if open_queues > max_open_queues {
        return Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            format!(
                "progress event index live-queue capacity reached: {open_queues} open queues exceeds configured limit {max_open_queues}; refusing to evict live progress state"
            ),
        ));
    }
    transaction
        .execute(
            "WITH queue_state AS (
                 SELECT queue_id,
                        MAX(line_number) AS latest_line,
                        MAX(is_terminal) AS closed
                 FROM progress_event_index_lines
                 WHERE parse_error IS NULL
                 GROUP BY queue_id
             ),
             ranked_terminal_queues AS (
                 SELECT queue_id,
                        ROW_NUMBER() OVER (ORDER BY latest_line DESC, queue_id ASC) AS queue_rank
                 FROM queue_state
                 WHERE closed = 1
             ),
             ranked_events AS (
                 SELECT line_number,
                        queue_id,
                        is_terminal,
                        ROW_NUMBER() OVER (
                            PARTITION BY queue_id ORDER BY line_number DESC
                        ) AS recent_rank,
                        ROW_NUMBER() OVER (
                            PARTITION BY queue_id ORDER BY line_number ASC
                        ) AS first_rank,
                        ROW_NUMBER() OVER (
                            PARTITION BY queue_id
                            ORDER BY CASE WHEN is_terminal = 1 THEN 0 ELSE 1 END,
                                     line_number DESC
                        ) AS terminal_rank
                 FROM progress_event_index_lines
                 WHERE parse_error IS NULL
             )
             DELETE FROM progress_event_index_lines
             WHERE parse_error IS NOT NULL
                OR queue_id IN (
                    SELECT queue_id FROM ranked_terminal_queues WHERE queue_rank > ?2
                )
                OR line_number IN (
                    SELECT line_number FROM ranked_events
                    WHERE recent_rank > ?1
                      AND first_rank > 1
                      AND NOT (is_terminal = 1 AND terminal_rank = 1)
                )",
            params![retained, max_terminal_queues],
        )
        .map_err(io::Error::other)?;
    Ok(())
}

fn read_meta(connection: &Connection, key: &str) -> io::Result<Option<String>> {
    connection
        .query_row(
            "SELECT value FROM progress_event_index_meta WHERE key = ?1",
            params![key],
            |row| row.get(0),
        )
        .optional()
        .map_err(io::Error::other)
}

fn read_meta_i64(connection: &Connection, key: &str) -> io::Result<i64> {
    let value = read_meta(connection, key)?.unwrap_or_else(|| "0".to_string());
    value.parse::<i64>().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("progress event index metadata `{key}` is invalid: {error}"),
        )
    })
}

fn read_meta_i64_from_transaction(transaction: &Transaction<'_>, key: &str) -> io::Result<i64> {
    let value: Option<String> = transaction
        .query_row(
            "SELECT value FROM progress_event_index_meta WHERE key = ?1",
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
                format!("progress event index metadata `{key}` is invalid: {error}"),
            )
        })
}

fn write_meta(transaction: &Transaction<'_>, key: &str, value: &str) -> io::Result<()> {
    transaction
        .execute(
            "INSERT INTO progress_event_index_meta (key, value) VALUES (?1, ?2) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![key, value],
        )
        .map_err(io::Error::other)?;
    Ok(())
}

fn delete_meta(transaction: &Transaction<'_>, key: &str) -> io::Result<()> {
    transaction
        .execute(
            "DELETE FROM progress_event_index_meta WHERE key = ?1",
            params![key],
        )
        .map_err(io::Error::other)?;
    Ok(())
}

fn progress_event_index_has_rows(connection: &Connection) -> io::Result<bool> {
    let count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM progress_event_index_lines",
            [],
            |row| row.get(0),
        )
        .map_err(io::Error::other)?;
    Ok(count > 0)
}

fn indexed_event(line_number: i64, event_json: String) -> io::Result<IndexedProgressEvent> {
    let line_number = usize::try_from(line_number).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "progress event index contains an invalid line number",
        )
    })?;
    let event = serde_json::from_str(&event_json).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("progress event index contains invalid event JSON: {error}"),
        )
    })?;
    Ok(IndexedProgressEvent { line_number, event })
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
    use crate::progress::{
        AgentProgressContext, AgentProgressEvent, AgentProgressKind, AgentProgressStatus,
    };
    use std::collections::BTreeSet;
    use std::fs;
    use std::io::Write;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    #[test]
    fn refresh_tails_appends_and_rebuilds_after_source_truncation() {
        let root = std::env::temp_dir().join(format!(
            "agent-harness-progress-event-index-test-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let harness_home = root.join(".agent-harness");
        let events_file = agent_progress_events_file(&harness_home);
        fs::create_dir_all(events_file.parent().unwrap()).unwrap();
        let first = event("turn:one", "first", 1);
        let second = event("turn:two", "second", 2);
        fs::write(
            &events_file,
            format!(
                "{}\n{}\n",
                serde_json::to_string(&first).unwrap(),
                serde_json::to_string(&second).unwrap()
            ),
        )
        .unwrap();

        let mut warnings = Vec::new();
        refresh_progress_event_index(&harness_home, &mut warnings).unwrap();
        assert_eq!(
            latest_progress_event_for_queue(&harness_home, "turn:one", &mut warnings)
                .unwrap()
                .unwrap()
                .event
                .label,
            "first"
        );

        let third = event("turn:one", "third", 3);
        fs::OpenOptions::new()
            .append(true)
            .open(&events_file)
            .unwrap()
            .write_all(format!("{}\n", serde_json::to_string(&third).unwrap()).as_bytes())
            .unwrap();
        let selected = progress_events_for_queue_ids(
            &harness_home,
            &BTreeSet::from(["turn:one".to_string(), "turn:two".to_string()]),
            2,
            &mut warnings,
        )
        .unwrap();
        assert_eq!(selected["turn:one"].len(), 2);
        assert_eq!(selected["turn:one"][1].event.label, "third");

        let replacement = event("turn:new", "replacement", 4);
        fs::write(
            &events_file,
            format!("{}\n", serde_json::to_string(&replacement).unwrap()),
        )
        .unwrap();
        refresh_progress_event_index(&harness_home, &mut warnings).unwrap();
        assert!(
            latest_progress_event_for_queue(&harness_home, "turn:one", &mut warnings)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            latest_progress_event_for_queue(&harness_home, "turn:new", &mut warnings)
                .unwrap()
                .unwrap()
                .event
                .label,
            "replacement"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn legacy_v1_sidecar_schema_rebuilds_without_querying_missing_columns() {
        let root = std::env::temp_dir().join(format!(
            "agent-harness-progress-event-index-v1-migration-test-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let harness_home = root.join(".agent-harness");
        let events_file = agent_progress_events_file(&harness_home);
        fs::create_dir_all(events_file.parent().unwrap()).unwrap();
        let current = event("turn:migrate", "current", 1);
        fs::write(
            &events_file,
            format!("{}\n", serde_json::to_string(&current).unwrap()),
        )
        .unwrap();

        let index_file = progress_event_index_file(&harness_home);
        let connection = Connection::open(&index_file).unwrap();
        connection
            .execute_batch(
                "
                CREATE TABLE progress_event_index_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE progress_event_index_lines (
                    line_number INTEGER PRIMARY KEY,
                    queue_id TEXT,
                    event_json TEXT,
                    parse_error TEXT
                );
                INSERT INTO progress_event_index_meta (key, value) VALUES
                    ('schema', 'agent-harness.progress-event-index.v1'),
                    ('revision', '3');
                ",
            )
            .unwrap();
        drop(connection);

        let mut warnings = Vec::new();
        refresh_progress_event_index(&harness_home, &mut warnings).unwrap();
        assert_eq!(
            latest_progress_event_for_queue(&harness_home, "turn:migrate", &mut warnings)
                .unwrap()
                .unwrap()
                .event
                .label,
            "current"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn index_keeps_a_bounded_recent_payload_window_per_queue() {
        let root = std::env::temp_dir().join(format!(
            "agent-harness-progress-event-index-bounded-test-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let harness_home = root.join(".agent-harness");
        let events_file = agent_progress_events_file(&harness_home);
        fs::create_dir_all(events_file.parent().unwrap()).unwrap();
        let source_count = PROGRESS_EVENT_INDEX_RETAINED_EVENTS_PER_QUEUE + 5;
        let mut source = String::new();
        for index in 0..source_count {
            source.push_str(
                &serde_json::to_string(&event(
                    "turn:bounded",
                    &format!("event-{index}"),
                    index as i64,
                ))
                .unwrap(),
            );
            source.push('\n');
        }
        fs::write(&events_file, source).unwrap();

        let mut warnings = Vec::new();
        refresh_progress_event_index(&harness_home, &mut warnings).unwrap();
        let connection = open_progress_event_index(&harness_home).unwrap();
        let retained: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM progress_event_index_lines WHERE queue_id = ?1",
                ["turn:bounded"],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            retained as usize,
            PROGRESS_EVENT_INDEX_RETAINED_EVENTS_PER_QUEUE + 1,
            "the projection retains the original start event for correct elapsed time plus the recent window"
        );
        assert_eq!(
            read_meta_i64(&connection, META_TOTAL_LINES).unwrap() as usize,
            source_count
        );
        let selected = progress_events_for_queue_ids(
            &harness_home,
            &BTreeSet::from(["turn:bounded".to_string()]),
            PROGRESS_EVENT_INDEX_RETAINED_EVENTS_PER_QUEUE + 10,
            &mut warnings,
        )
        .unwrap();
        assert_eq!(
            selected["turn:bounded"].len(),
            PROGRESS_EVENT_INDEX_RETAINED_EVENTS_PER_QUEUE + 1
        );
        assert_eq!(
            selected["turn:bounded"].first().unwrap().event.label,
            "event-0"
        );
        assert_eq!(
            selected["turn:bounded"].last().unwrap().event.label,
            format!("event-{}", source_count - 1)
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn index_evicts_only_oldest_terminal_queues_at_the_history_window_limit() {
        let root = std::env::temp_dir().join(format!(
            "agent-harness-progress-event-index-global-bounded-test-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let harness_home = root.join(".agent-harness");
        let events_file = agent_progress_events_file(&harness_home);
        fs::create_dir_all(events_file.parent().unwrap()).unwrap();
        let mut source = String::new();
        for index in 0..=PROGRESS_EVENT_INDEX_MAX_TERMINAL_QUEUES {
            let mut terminal = event(
                &format!("turn:global-{index}"),
                &format!("event-{index}"),
                index as i64,
            );
            terminal.status = AgentProgressStatus::Completed;
            source.push_str(&serde_json::to_string(&terminal).unwrap());
            source.push('\n');
        }
        fs::write(&events_file, source).unwrap();

        let mut warnings = Vec::new();
        refresh_progress_event_index(&harness_home, &mut warnings).unwrap();
        let connection = open_progress_event_index(&harness_home).unwrap();
        let retained_rows: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM progress_event_index_lines",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            retained_rows as usize,
            PROGRESS_EVENT_INDEX_MAX_TERMINAL_QUEUES
        );
        assert!(
            retained_rows as usize <= PROGRESS_EVENT_INDEX_MAX_PAYLOAD_ROWS,
            "the global queue policy must imply an absolute payload row bound"
        );
        assert!(
            latest_progress_event_for_queue(&harness_home, "turn:global-0", &mut warnings)
                .unwrap()
                .is_none(),
            "the least-recent queue is evicted as a complete queue, not partially retained"
        );
        assert_eq!(
            latest_progress_event_for_queue(
                &harness_home,
                &format!("turn:global-{PROGRESS_EVENT_INDEX_MAX_TERMINAL_QUEUES}"),
                &mut warnings,
            )
            .unwrap()
            .unwrap()
            .event
            .label,
            format!("event-{PROGRESS_EVENT_INDEX_MAX_TERMINAL_QUEUES}")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn busy_append_lock_returns_empty_without_creating_a_progress_index() {
        let root = std::env::temp_dir().join(format!(
            "agent-harness-progress-event-index-busy-empty-test-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let harness_home = root.join(".agent-harness");
        let events_file = agent_progress_events_file(&harness_home);
        fs::create_dir_all(events_file.parent().unwrap()).unwrap();
        let lock_file = append_lock_file(&events_file);
        fs::write(&lock_file, format!("pid={}\n", std::process::id())).unwrap();

        let mut warnings = Vec::new();
        let started = Instant::now();
        let event =
            latest_progress_event_for_queue(&harness_home, "turn:busy", &mut warnings).unwrap();
        assert!(started.elapsed() < Duration::from_secs(2));
        assert!(event.is_none());
        assert!(
            !progress_event_index_file(&harness_home).exists(),
            "a busy source lock must not initialize a writable sidecar"
        );
        assert!(
            warnings
                .iter()
                .any(|warning| warning.contains("using last committed progress state"))
        );

        fs::remove_file(lock_file).unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn busy_append_lock_reads_the_last_committed_progress_snapshot() {
        let root = std::env::temp_dir().join(format!(
            "agent-harness-progress-event-index-busy-snapshot-test-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let harness_home = root.join(".agent-harness");
        let events_file = agent_progress_events_file(&harness_home);
        fs::create_dir_all(events_file.parent().unwrap()).unwrap();
        fs::write(
            &events_file,
            format!(
                "{}\n",
                serde_json::to_string(&event("turn:busy", "old", 1)).unwrap()
            ),
        )
        .unwrap();
        let mut warnings = Vec::new();
        refresh_progress_event_index(&harness_home, &mut warnings).unwrap();
        fs::OpenOptions::new()
            .append(true)
            .open(&events_file)
            .unwrap()
            .write_all(
                format!(
                    "{}\n",
                    serde_json::to_string(&event("turn:busy", "new", 2)).unwrap()
                )
                .as_bytes(),
            )
            .unwrap();
        let lock_file = append_lock_file(&events_file);
        fs::write(&lock_file, format!("pid={}\n", std::process::id())).unwrap();

        let started = Instant::now();
        let snapshot = latest_progress_event_for_queue(&harness_home, "turn:busy", &mut warnings)
            .unwrap()
            .unwrap();
        assert!(started.elapsed() < Duration::from_secs(2));
        assert_eq!(snapshot.event.label, "old");
        fs::remove_file(lock_file).unwrap();
        assert_eq!(
            latest_progress_event_for_queue(&harness_home, "turn:busy", &mut warnings)
                .unwrap()
                .unwrap()
                .event
                .label,
            "new"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn normal_progress_planner_uses_the_bounded_projection_after_a_100k_history() {
        let root = std::env::temp_dir().join(format!(
            "agent-harness-progress-event-index-100k-planner-test-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let harness_home = root.join(".agent-harness");
        let events_file = agent_progress_events_file(&harness_home);
        fs::create_dir_all(events_file.parent().unwrap()).unwrap();
        let mut terminal = event("turn:historic", "completed", 1);
        terminal.status = AgentProgressStatus::Completed;
        let encoded = serde_json::to_string(&terminal).unwrap();
        let source = format!("{encoded}\n").repeat(100_000);
        fs::write(&events_file, &source).unwrap();

        // Construct the already-committed sidecar as a normal long-lived
        // harness would have it.  The planner below must read this bounded
        // projection, not replay the 100k-line authoritative journal again.
        let mut connection = open_progress_event_index(&harness_home).unwrap();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        insert_progress_event(&transaction, 100_000, &encoded, &terminal).unwrap();
        write_meta(&transaction, META_TOTAL_LINES, "100000").unwrap();
        write_meta(&transaction, META_VALID_LINES, "100000").unwrap();
        write_meta(&transaction, META_INVALID_LINES, "0").unwrap();
        let metadata = fs::metadata(&events_file).unwrap();
        let cursor =
            cursor_from_processed_ledger(&events_file, metadata.len(), 100_000, &metadata, 1)
                .unwrap();
        write_cursor(&transaction, &cursor).unwrap();
        transaction.commit().unwrap();

        let started = Instant::now();
        let plan = crate::progress::plan_agent_progress_delivery(
            crate::progress::AgentProgressDeliveryPlanOptions {
                harness_home: harness_home.clone(),
                platform: Some("discord".to_string()),
                now_ms: 2_000,
                min_update_interval_ms: 0,
                ..crate::progress::AgentProgressDeliveryPlanOptions::default()
            },
        )
        .unwrap();
        assert_eq!(plan.summary.total_events, 1);
        assert_eq!(plan.summary.new_events, 100_000);
        assert_eq!(plan.summary.index_projected_delta_events, 1);
        assert_eq!(plan.summary.index_source_line, 100_000);
        assert_eq!(plan.summary.index_valid_lines, 100_000);
        assert!(plan.summary.index_available);
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "a current 100k-row progress source must be served from the committed projection"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn retention_keeps_an_old_open_queue_when_terminal_history_exceeds_its_window() {
        let root = std::env::temp_dir().join(format!(
            "agent-harness-progress-event-index-open-retention-test-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let harness_home = root.join(".agent-harness");
        let events_file = agent_progress_events_file(&harness_home);
        fs::create_dir_all(events_file.parent().unwrap()).unwrap();

        let mut source = String::new();
        source
            .push_str(&serde_json::to_string(&event("turn:open-old", "still-running", 1)).unwrap());
        source.push('\n');
        for index in 0..(PROGRESS_EVENT_INDEX_MAX_TERMINAL_QUEUES + 8) {
            let mut terminal = event(
                &format!("turn:terminal-{index}"),
                "completed",
                index as i64 + 2,
            );
            terminal.status = crate::progress::AgentProgressStatus::Completed;
            source.push_str(&serde_json::to_string(&terminal).unwrap());
            source.push('\n');
        }
        fs::write(&events_file, source).unwrap();

        let mut warnings = Vec::new();
        refresh_progress_event_index(&harness_home, &mut warnings).unwrap();
        assert_eq!(
            latest_progress_event_for_queue(&harness_home, "turn:open-old", &mut warnings)
                .unwrap()
                .unwrap()
                .event
                .label,
            "still-running",
            "an old live queue must never be silently evicted to make room for terminal history"
        );
        assert!(
            latest_progress_event_for_queue(&harness_home, "turn:terminal-0", &mut warnings)
                .unwrap()
                .is_none(),
            "terminal history is the bounded tier"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn live_queue_capacity_fails_closed_instead_of_evicting_open_progress() {
        let root = std::env::temp_dir().join(format!(
            "agent-harness-progress-event-index-open-capacity-test-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let harness_home = root.join(".agent-harness");
        let mut connection = open_progress_event_index(&harness_home).unwrap();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        for index in 0..3 {
            let event = event(
                &format!("turn:open-capacity-{index}"),
                "running",
                index as i64,
            );
            let encoded = serde_json::to_string(&event).unwrap();
            insert_progress_event(&transaction, index + 1, &encoded, &event).unwrap();
        }
        let error = retain_bounded_progress_event_payloads_with_limits(&transaction, 4, 2, 1)
            .expect_err("a live queue over-capacity condition must not evict state");
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        drop(transaction);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn stable_event_id_survives_a_source_relocation() {
        let root = std::env::temp_dir().join(format!(
            "agent-harness-progress-event-index-event-id-relocation-test-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let harness_home = root.join(".agent-harness");
        let events_file = agent_progress_events_file(&harness_home);
        fs::create_dir_all(events_file.parent().unwrap()).unwrap();
        let mut preserved = event("turn:stable", "preserved", 10);
        preserved.event_id = Some("progress-event-stable-0001".to_string());
        fs::write(
            &events_file,
            format!("{}\n", serde_json::to_string(&preserved).unwrap()),
        )
        .unwrap();

        let mut warnings = Vec::new();
        refresh_progress_event_index(&harness_home, &mut warnings).unwrap();
        let original = latest_progress_event_for_queue(&harness_home, "turn:stable", &mut warnings)
            .unwrap()
            .unwrap();
        assert_eq!(original.line_number, 1);
        assert_eq!(
            original.event.event_id.as_deref(),
            Some("progress-event-stable-0001")
        );

        let leading = event("turn:other", "leading", 9);
        fs::write(
            &events_file,
            format!(
                "{}\n{}\n",
                serde_json::to_string(&leading).unwrap(),
                serde_json::to_string(&preserved).unwrap()
            ),
        )
        .unwrap();
        refresh_progress_event_index(&harness_home, &mut warnings).unwrap();
        let relocated =
            latest_progress_event_for_queue(&harness_home, "turn:stable", &mut warnings)
                .unwrap()
                .unwrap();
        assert_eq!(relocated.line_number, 2);
        assert_eq!(
            relocated.event.event_id.as_deref(),
            Some("progress-event-stable-0001"),
            "the canonical opaque identity, not the physical JSONL line, survives relocation"
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

    fn event(queue_id: &str, label: &str, at_ms: i64) -> AgentProgressEvent {
        AgentProgressEvent::new(
            &AgentProgressContext {
                queue_id: queue_id.to_string(),
                agent_id: Some("main".to_string()),
                account_id: None,
                thread_id: None,
                session_key: format!("session:{queue_id}"),
                platform: "discord".to_string(),
                channel_id: "channel".to_string(),
                user_id: "user".to_string(),
            },
            AgentProgressKind::Runtime,
            label,
            label,
            AgentProgressStatus::Progress,
            at_ms,
        )
    }
}
