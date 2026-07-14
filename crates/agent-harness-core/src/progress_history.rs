//! Crash-recoverable cold history for completed progress queues.
//!
//! `progress-events.jsonl` is deliberately still the only hot source used by
//! interactive delivery.  This module moves an *entire* queue only after its
//! last canonical v2 terminal event has a durable delivery/inactivity proof.
//! The paired SQLite transaction stays hidden until the hot source replacement
//! is durable, so a crash cannot make a queue disappear from both stores.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params, params_from_iter};
use serde::{Deserialize, Serialize};

use crate::logging::{try_with_jsonl_append_lock, with_jsonl_append_lock};
use crate::progress_event_index::progress_event_index_file;
use crate::write_json_atomic;

use super::{
    AgentProgressEvent, agent_progress_events_file, is_terminal_event, valid_progress_event_id,
};

pub(crate) const PROGRESS_HISTORY_SCHEMA: &str = "agent-harness.progress-history.v1";

const HISTORY_FILE_NAME: &str = "progress-event-history.sqlite";
const HISTORY_STATE_STAGED: &str = "staged";
const HISTORY_STATE_COMMITTED: &str = "committed";
const COMPACTION_PENDING_SCHEMA: &str = "agent-harness.progress-history-compaction-pending.v1";
const MAX_COMPACTION_WARNING_LINES: usize = 16;

static PROGRESS_HISTORY_TRANSACTION_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProgressHistoryCompactionPolicy {
    pub(crate) max_hot_bytes: u64,
    pub(crate) max_terminal_queues: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProgressHistoryCompactionResult {
    pub(crate) events_file: PathBuf,
    pub(crate) history_file: PathBuf,
    pub(crate) marker_file: PathBuf,
    pub(crate) scanned_lines: usize,
    pub(crate) hot_bytes_before: u64,
    pub(crate) hot_bytes_after: u64,
    pub(crate) compacted_queues: usize,
    pub(crate) compacted_events: usize,
    pub(crate) retained_open_queues: usize,
    pub(crate) retained_undelivered_queues: usize,
    pub(crate) retained_legacy_queues: usize,
    pub(crate) retained_terminal_queues: usize,
    pub(crate) retained_unclassified_lines: usize,
    pub(crate) backpressure: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProgressHistoryRecord {
    pub(crate) original_line: usize,
    pub(crate) event: AgentProgressEvent,
    pub(crate) cold: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProgressHistoryCompactionRecovery {
    NoMarker,
    Busy,
    Recovered,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProgressHistoryStaging {
    store_file: PathBuf,
    transaction_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProgressHistoryCompactionPending {
    schema: String,
    events_file: PathBuf,
    archive_file: PathBuf,
    temp_file: PathBuf,
    expected_hot_bytes: u64,
    expected_hot_digest: u64,
    original_hot_bytes: u64,
    original_hot_digest: u64,
    history_store_file: PathBuf,
    history_transaction_id: String,
    index_file: PathBuf,
}

#[derive(Debug, Clone)]
struct HotProgressLine {
    raw: Vec<u8>,
    queue_id: Option<String>,
}

#[derive(Debug, Clone)]
struct QueueEventLine {
    line_number: usize,
    event: AgentProgressEvent,
    canonical_v2: bool,
    raw_bytes: usize,
}

#[derive(Debug, Default)]
struct QueueScan {
    events: Vec<QueueEventLine>,
}

#[derive(Debug)]
struct HotProgressScan {
    original: Vec<u8>,
    lines: Vec<HotProgressLine>,
    queues: BTreeMap<String, QueueScan>,
    last_unstable_line: usize,
    unclassified_lines: usize,
}

#[derive(Debug, Clone)]
struct CompactableQueue {
    queue_id: String,
    last_line: usize,
    bytes: usize,
    events: Vec<QueueEventLine>,
}

/// Returns the cold SQLite history store colocated with the hot progress
/// ledger.  It is an exact-history sidecar, never an interactive planner
/// input.
pub(crate) fn progress_history_file(harness_home: &Path) -> PathBuf {
    agent_progress_events_file(harness_home).with_file_name(HISTORY_FILE_NAME)
}

pub(crate) fn progress_history_marker_file(harness_home: &Path) -> PathBuf {
    progress_history_marker_file_for_events(&agent_progress_events_file(harness_home))
}

/// Compacts only queues whose final canonical event has a matching durable
/// terminal-delivery/inactivity proof.  A queue is moved as a whole so the
/// hot projection can never retain an old nonterminal prefix that would reopen
/// a closed queue after the terminal row is cold.
pub(crate) fn compact_progress_history(
    harness_home: &Path,
    policy: ProgressHistoryCompactionPolicy,
    delivered_terminal_events: &BTreeSet<(String, String)>,
    now_ms: i64,
    warnings: &mut Vec<String>,
) -> io::Result<ProgressHistoryCompactionResult> {
    let events_file = agent_progress_events_file(harness_home);
    if let Some(parent) = events_file.parent() {
        fs::create_dir_all(parent)?;
    }
    with_jsonl_append_lock(&events_file, || {
        let _ = recover_progress_history_compaction_if_needed_locked(
            harness_home,
            &events_file,
            warnings,
        )?;
        compact_progress_history_locked(
            harness_home,
            &events_file,
            policy,
            delivered_terminal_events,
            now_ms,
            warnings,
        )
    })
}

/// Completes a prior source/history transaction before an appender or exact
/// diagnostic reader touches the hot ledger.  This path is intentionally
/// blocking only when a marker exists.
pub(crate) fn recover_progress_history_compaction_if_needed(
    harness_home: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<ProgressHistoryCompactionRecovery> {
    let events_file = agent_progress_events_file(harness_home);
    if !progress_history_marker_file_for_events(&events_file).is_file() {
        return Ok(ProgressHistoryCompactionRecovery::NoMarker);
    }
    with_jsonl_append_lock(&events_file, || {
        recover_progress_history_compaction_if_needed_locked(harness_home, &events_file, warnings)
    })
}

/// A user-visible planner must never wait behind a source replacement.  If a
/// marker owner still has the append lock, the caller must avoid the stale
/// projection and use its already-delivered cache for that one pass.
pub(crate) fn try_recover_progress_history_compaction_if_needed(
    harness_home: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<ProgressHistoryCompactionRecovery> {
    let events_file = agent_progress_events_file(harness_home);
    if !progress_history_marker_file_for_events(&events_file).is_file() {
        return Ok(ProgressHistoryCompactionRecovery::NoMarker);
    }
    match try_with_jsonl_append_lock(&events_file, || {
        recover_progress_history_compaction_if_needed_locked(harness_home, &events_file, warnings)
    })? {
        Some(outcome) => Ok(outcome),
        None => Ok(ProgressHistoryCompactionRecovery::Busy),
    }
}

/// Reads exact history for explicitly requested queues.  This is deliberately
/// separate from the delivery planner: it joins committed cold rows with the
/// hot ledger under its append lock and can therefore be more expensive.
pub(crate) fn read_progress_history_for_queue_ids(
    harness_home: &Path,
    queue_ids: &BTreeSet<String>,
    warnings: &mut Vec<String>,
) -> io::Result<Vec<ProgressHistoryRecord>> {
    if queue_ids.is_empty() {
        return Ok(Vec::new());
    }
    let _ = recover_progress_history_compaction_if_needed(harness_home, warnings)?;
    let events_file = agent_progress_events_file(harness_home);
    let records_by_identity = with_jsonl_append_lock(&events_file, || {
        // Keep the cold commit and hot source in one append-lock snapshot.  A
        // compactor also owns this lock, so an exact lookup cannot observe the
        // pre-commit cold store and the post-swap hot ledger separately.
        let mut records_by_identity = BTreeMap::<String, ProgressHistoryRecord>::new();
        for record in read_committed_progress_history(harness_home, queue_ids)? {
            let key = history_identity_key(&record.event, record.original_line, true);
            records_by_identity.insert(key, record);
        }
        for record in read_hot_progress_history_for_queue_ids(&events_file, queue_ids, warnings)? {
            // A hot copy wins if an operator restored a source file manually;
            // it remains authoritative until a later safe compaction.
            let key = history_identity_key(&record.event, record.original_line, false);
            records_by_identity.insert(key, record);
        }
        Ok(records_by_identity)
    })?;

    let mut records = records_by_identity.into_values().collect::<Vec<_>>();
    records.sort_by(|left, right| {
        left.event
            .at_ms
            .cmp(&right.event.at_ms)
            .then_with(|| left.original_line.cmp(&right.original_line))
            .then_with(|| {
                left.event
                    .event_id
                    .as_deref()
                    .unwrap_or_default()
                    .cmp(right.event.event_id.as_deref().unwrap_or_default())
            })
    });
    Ok(records)
}

fn compact_progress_history_locked(
    harness_home: &Path,
    events_file: &Path,
    policy: ProgressHistoryCompactionPolicy,
    delivered_terminal_events: &BTreeSet<(String, String)>,
    now_ms: i64,
    warnings: &mut Vec<String>,
) -> io::Result<ProgressHistoryCompactionResult> {
    let scan = scan_hot_progress_events(events_file, warnings)?;
    let history_file = progress_history_file(harness_home);
    let marker_file = progress_history_marker_file_for_events(events_file);
    let mut candidates = Vec::<CompactableQueue>::new();
    let mut retained_open_queues = 0usize;
    let mut retained_undelivered_queues = 0usize;
    let mut retained_legacy_queues = 0usize;

    for (queue_id, queue) in &scan.queues {
        let Some(last) = queue.events.last() else {
            continue;
        };
        let all_canonical = queue.events.iter().all(|entry| entry.canonical_v2);
        if !all_canonical {
            retained_legacy_queues = retained_legacy_queues.saturating_add(1);
            continue;
        }
        if queue
            .events
            .iter()
            .any(|entry| entry.line_number <= scan.last_unstable_line)
        {
            // Physical line identity remains the only compatibility key for
            // data before an old/no-id row.  Do not shift it by rewriting a
            // preceding v2 range; explicit backpressure is safer.
            retained_legacy_queues = retained_legacy_queues.saturating_add(1);
            continue;
        }
        if !is_terminal_event(&last.event) {
            retained_open_queues = retained_open_queues.saturating_add(1);
            continue;
        }
        let Some(event_id) = valid_progress_event_id(last.event.event_id.as_deref()) else {
            retained_legacy_queues = retained_legacy_queues.saturating_add(1);
            continue;
        };
        if !delivered_terminal_events.contains(&(queue_id.clone(), event_id.to_string())) {
            retained_undelivered_queues = retained_undelivered_queues.saturating_add(1);
            continue;
        }
        candidates.push(CompactableQueue {
            queue_id: queue_id.clone(),
            last_line: last.line_number,
            bytes: queue.events.iter().map(|entry| entry.raw_bytes).sum(),
            events: queue.events.clone(),
        });
    }

    candidates.sort_by_key(|candidate| candidate.last_line);
    let mut selected_queue_ids = BTreeSet::<String>::new();
    let retain_recent = policy.max_terminal_queues.min(candidates.len());
    let immediately_compactable = candidates.len().saturating_sub(retain_recent);
    for candidate in candidates.iter().take(immediately_compactable) {
        selected_queue_ids.insert(candidate.queue_id.clone());
    }

    let mut projected_hot_bytes = scan.original.len();
    for candidate in &candidates {
        if selected_queue_ids.contains(&candidate.queue_id) {
            projected_hot_bytes = projected_hot_bytes.saturating_sub(candidate.bytes);
        }
    }
    for candidate in candidates.iter().skip(immediately_compactable) {
        if projected_hot_bytes as u64 <= policy.max_hot_bytes {
            break;
        }
        if selected_queue_ids.insert(candidate.queue_id.clone()) {
            projected_hot_bytes = projected_hot_bytes.saturating_sub(candidate.bytes);
        }
    }

    let retained_terminal_queues = candidates
        .iter()
        .filter(|candidate| !selected_queue_ids.contains(&candidate.queue_id))
        .count();
    let selected_events = candidates
        .iter()
        .filter(|candidate| selected_queue_ids.contains(&candidate.queue_id))
        .flat_map(|candidate| candidate.events.iter().cloned())
        .collect::<Vec<_>>();

    if selected_events.is_empty() {
        let backpressure = scan.original.len() as u64 > policy.max_hot_bytes
            || retained_terminal_queues > policy.max_terminal_queues;
        if backpressure {
            warnings.push(format!(
                "progress history compaction is applying backpressure at {}: {} hot bytes remain, but only open, undelivered, legacy, or retained-terminal queues are safe to keep",
                events_file.display(),
                scan.original.len()
            ));
        }
        return Ok(ProgressHistoryCompactionResult {
            events_file: events_file.to_path_buf(),
            history_file,
            marker_file,
            scanned_lines: scan.lines.len(),
            hot_bytes_before: scan.original.len() as u64,
            hot_bytes_after: scan.original.len() as u64,
            compacted_queues: 0,
            compacted_events: 0,
            retained_open_queues,
            retained_undelivered_queues,
            retained_legacy_queues,
            retained_terminal_queues,
            retained_unclassified_lines: scan.unclassified_lines,
            backpressure,
        });
    }

    let mut snapshot = Vec::with_capacity(projected_hot_bytes);
    for line in &scan.lines {
        if line
            .queue_id
            .as_ref()
            .is_some_and(|queue_id| selected_queue_ids.contains(queue_id))
        {
            continue;
        }
        snapshot.extend_from_slice(&line.raw);
    }
    let transaction_id = next_progress_history_transaction_id(now_ms, &scan.original);
    let candidates_for_history = selected_events
        .iter()
        .map(history_candidate_from_event_line)
        .collect::<io::Result<Vec<_>>>()?;
    let staging = stage_progress_history(
        &history_file,
        &transaction_id,
        &candidates_for_history,
        now_ms,
    )?;

    let archive_file = progress_history_temp_file(events_file, now_ms, "archive");
    let temp_file = progress_history_temp_file(events_file, now_ms, "compact");
    let index_file = progress_event_index_file(harness_home);
    let pending = ProgressHistoryCompactionPending {
        schema: COMPACTION_PENDING_SCHEMA.to_string(),
        events_file: events_file.to_path_buf(),
        archive_file: archive_file.clone(),
        temp_file: temp_file.clone(),
        expected_hot_bytes: snapshot.len() as u64,
        expected_hot_digest: progress_history_digest(&snapshot),
        original_hot_bytes: scan.original.len() as u64,
        original_hot_digest: progress_history_digest(&scan.original),
        history_store_file: staging.store_file.clone(),
        history_transaction_id: staging.transaction_id.clone(),
        index_file,
    };

    let operation = (|| {
        write_progress_history_file(&archive_file, &scan.original)?;
        write_progress_history_file(&temp_file, &snapshot)?;
        write_json_atomic(&marker_file, &pending)?;

        // Delete the projection before the source swap.  A marker remains
        // until this succeeds, so no reader can use a stale terminal/open
        // projection against a relocated hot source.
        invalidate_progress_event_index(&pending.index_file)?;
        replace_progress_events_file(&temp_file, events_file)?;
        if !progress_history_file_matches(
            events_file,
            pending.expected_hot_bytes,
            pending.expected_hot_digest,
        )? {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "progress hot ledger replacement did not match its compact snapshot: {}",
                    events_file.display()
                ),
            ));
        }
        commit_progress_history(&staging, now_ms)?;
        remove_file_if_present(&archive_file)?;
        remove_file_if_present(&temp_file)?;
        remove_file_if_present(&marker_file)?;
        Ok(())
    })();

    if operation.is_err() && !marker_file.exists() {
        let _ = discard_progress_history(&staging);
        let _ = remove_file_if_present(&archive_file);
        let _ = remove_file_if_present(&temp_file);
    }
    operation?;

    let hot_bytes_after = fs::metadata(events_file)?.len();
    let backpressure = hot_bytes_after > policy.max_hot_bytes
        || retained_terminal_queues > policy.max_terminal_queues;
    if backpressure {
        warnings.push(format!(
            "progress history compaction retained {} hot bytes at {} because open, undelivered, legacy, or policy-retained data cannot be evicted safely",
            hot_bytes_after,
            events_file.display()
        ));
    }
    Ok(ProgressHistoryCompactionResult {
        events_file: events_file.to_path_buf(),
        history_file,
        marker_file,
        scanned_lines: scan.lines.len(),
        hot_bytes_before: scan.original.len() as u64,
        hot_bytes_after,
        compacted_queues: selected_queue_ids.len(),
        compacted_events: selected_events.len(),
        retained_open_queues,
        retained_undelivered_queues,
        retained_legacy_queues,
        retained_terminal_queues,
        retained_unclassified_lines: scan.unclassified_lines,
        backpressure,
    })
}

fn recover_progress_history_compaction_if_needed_locked(
    harness_home: &Path,
    events_file: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<ProgressHistoryCompactionRecovery> {
    let marker_file = progress_history_marker_file_for_events(events_file);
    if !marker_file.is_file() {
        return Ok(ProgressHistoryCompactionRecovery::NoMarker);
    }
    let pending: ProgressHistoryCompactionPending =
        serde_json::from_slice(&fs::read(&marker_file)?).map_err(io::Error::other)?;
    let expected_history = progress_history_file(harness_home);
    let expected_index = progress_event_index_file(harness_home);
    if pending.schema != COMPACTION_PENDING_SCHEMA
        || pending.events_file.as_path() != events_file
        || pending.history_store_file != expected_history
        || pending.index_file != expected_index
        || pending.history_transaction_id.trim().is_empty()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "progress history compaction marker is invalid for {}",
                events_file.display()
            ),
        ));
    }
    let staging = ProgressHistoryStaging {
        store_file: pending.history_store_file.clone(),
        transaction_id: pending.history_transaction_id.clone(),
    };

    if progress_history_file_matches(
        events_file,
        pending.expected_hot_bytes,
        pending.expected_hot_digest,
    )? {
        invalidate_progress_event_index(&pending.index_file)?;
        commit_progress_history(&staging, current_time_ms()?)?;
        remove_file_if_present(&pending.archive_file)?;
        remove_file_if_present(&pending.temp_file)?;
        remove_file_if_present(&marker_file)?;
        warnings.push(format!(
            "recovered completed progress history compaction for {}",
            events_file.display()
        ));
        return Ok(ProgressHistoryCompactionRecovery::Recovered);
    }

    if progress_history_file_matches(
        &pending.temp_file,
        pending.expected_hot_bytes,
        pending.expected_hot_digest,
    )? {
        invalidate_progress_event_index(&pending.index_file)?;
        replace_progress_events_file(&pending.temp_file, events_file)?;
        commit_progress_history(&staging, current_time_ms()?)?;
        remove_file_if_present(&pending.archive_file)?;
        remove_file_if_present(&marker_file)?;
        warnings.push(format!(
            "recovered interrupted progress history compaction from its staged snapshot for {}",
            events_file.display()
        ));
        return Ok(ProgressHistoryCompactionRecovery::Recovered);
    }

    if progress_history_file_matches(
        &pending.archive_file,
        pending.original_hot_bytes,
        pending.original_hot_digest,
    )? {
        replace_progress_events_file(&pending.archive_file, events_file)?;
        discard_progress_history(&staging)?;
        remove_file_if_present(&pending.temp_file)?;
        remove_file_if_present(&marker_file)?;
        warnings.push(format!(
            "restored the original progress hot ledger after an interrupted compaction: {}",
            events_file.display()
        ));
        return Ok(ProgressHistoryCompactionRecovery::Recovered);
    }

    Err(io::Error::new(
        io::ErrorKind::NotFound,
        format!(
            "progress history compaction cannot recover {}; both staged and archived sources are missing or invalid",
            events_file.display()
        ),
    ))
}

fn scan_hot_progress_events(
    events_file: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<HotProgressScan> {
    let original = match fs::read(events_file) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => Vec::new(),
        Err(error) => return Err(error),
    };
    let mut lines = Vec::new();
    let mut queues = BTreeMap::<String, QueueScan>::new();
    let mut last_unstable_line = 0usize;
    let mut unclassified_lines = 0usize;
    let mut warning_budget = MAX_COMPACTION_WARNING_LINES;

    for (line_index, raw) in original.split_inclusive(|byte| *byte == b'\n').enumerate() {
        let line_number = line_index.saturating_add(1);
        let raw = raw.to_vec();
        let trimmed = trim_ascii_whitespace(&raw);
        if trimmed.is_empty() {
            lines.push(HotProgressLine {
                raw,
                queue_id: None,
            });
            continue;
        }
        match serde_json::from_slice::<AgentProgressEvent>(trimmed) {
            Ok(event) if !event.queue_id.trim().is_empty() => {
                let queue_id = event.queue_id.clone();
                let canonical_v2 = event.schema == super::AGENT_PROGRESS_EVENT_SCHEMA
                    && valid_progress_event_id(event.event_id.as_deref()).is_some();
                if !canonical_v2 {
                    last_unstable_line = last_unstable_line.max(line_number);
                }
                queues
                    .entry(queue_id.clone())
                    .or_default()
                    .events
                    .push(QueueEventLine {
                        line_number,
                        event,
                        canonical_v2,
                        raw_bytes: raw.len(),
                    });
                lines.push(HotProgressLine {
                    raw,
                    queue_id: Some(queue_id),
                });
            }
            Ok(_) => {
                last_unstable_line = last_unstable_line.max(line_number);
                unclassified_lines = unclassified_lines.saturating_add(1);
                if warning_budget > 0 {
                    warning_budget = warning_budget.saturating_sub(1);
                    warnings.push(format!(
                        "progress history compaction retained unscoped source line {} at {}",
                        line_number,
                        events_file.display()
                    ));
                }
                lines.push(HotProgressLine {
                    raw,
                    queue_id: None,
                });
            }
            Err(error) => {
                last_unstable_line = last_unstable_line.max(line_number);
                unclassified_lines = unclassified_lines.saturating_add(1);
                if warning_budget > 0 {
                    warning_budget = warning_budget.saturating_sub(1);
                    warnings.push(format!(
                        "progress history compaction retained malformed source line {} at {}: {}",
                        line_number,
                        events_file.display(),
                        error
                    ));
                }
                lines.push(HotProgressLine {
                    raw,
                    queue_id: None,
                });
            }
        }
    }

    Ok(HotProgressScan {
        original,
        lines,
        queues,
        last_unstable_line,
        unclassified_lines,
    })
}

fn history_candidate_from_event_line(entry: &QueueEventLine) -> io::Result<HistoryCandidate> {
    let event_id = valid_progress_event_id(entry.event.event_id.as_deref())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "attempted to compact a progress event without a canonical event id",
            )
        })?
        .to_string();
    Ok(HistoryCandidate {
        event_id,
        queue_id: entry.event.queue_id.clone(),
        source_line: entry.line_number,
        at_ms: entry.event.at_ms,
        event_json: serde_json::to_string(&entry.event).map_err(io::Error::other)?,
    })
}

#[derive(Debug, Clone)]
struct HistoryCandidate {
    event_id: String,
    queue_id: String,
    source_line: usize,
    at_ms: i64,
    event_json: String,
}

fn stage_progress_history(
    store_file: &Path,
    transaction_id: &str,
    candidates: &[HistoryCandidate],
    now_ms: i64,
) -> io::Result<ProgressHistoryStaging> {
    if transaction_id.trim().is_empty() || candidates.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "progress history staging requires a transaction id and at least one event",
        ));
    }
    let mut connection = open_or_create_progress_history(store_file)?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(io::Error::other)?;
    transaction
        .execute(
            "INSERT INTO progress_history_transactions \
             (transaction_id, state, created_at_ms, committed_at_ms) \
             VALUES (?1, ?2, ?3, NULL)",
            params![transaction_id, HISTORY_STATE_STAGED, now_ms],
        )
        .map_err(io::Error::other)?;
    for candidate in candidates {
        let source_line = i64::try_from(candidate.source_line).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "progress history source line exceeds SQLite range",
            )
        })?;
        transaction
            .execute(
                "INSERT INTO progress_history_events \
                 (transaction_id, event_id, queue_id, source_line, at_ms, event_json) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    transaction_id,
                    candidate.event_id,
                    candidate.queue_id,
                    source_line,
                    candidate.at_ms,
                    candidate.event_json,
                ],
            )
            .map_err(io::Error::other)?;
    }
    transaction.commit().map_err(io::Error::other)?;
    Ok(ProgressHistoryStaging {
        store_file: store_file.to_path_buf(),
        transaction_id: transaction_id.to_string(),
    })
}

fn commit_progress_history(
    staging: &ProgressHistoryStaging,
    committed_at_ms: i64,
) -> io::Result<()> {
    let mut connection = open_existing_progress_history(&staging.store_file)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "progress history store is missing while committing transaction `{}`",
                staging.transaction_id
            ),
        )
    })?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(io::Error::other)?;
    let changed = transaction
        .execute(
            "UPDATE progress_history_transactions \
             SET state = ?1, committed_at_ms = ?2 \
             WHERE transaction_id = ?3 AND state = ?4",
            params![
                HISTORY_STATE_COMMITTED,
                committed_at_ms,
                staging.transaction_id,
                HISTORY_STATE_STAGED,
            ],
        )
        .map_err(io::Error::other)?;
    if changed == 0 {
        let state: Option<String> = transaction
            .query_row(
                "SELECT state FROM progress_history_transactions WHERE transaction_id = ?1",
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
                        "progress history transaction `{}` has unexpected state `{other}`",
                        staging.transaction_id
                    ),
                ));
            }
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!(
                        "progress history transaction `{}` was not staged",
                        staging.transaction_id
                    ),
                ));
            }
        }
    }
    transaction.commit().map_err(io::Error::other)
}

fn discard_progress_history(staging: &ProgressHistoryStaging) -> io::Result<()> {
    let Some(mut connection) = open_existing_progress_history(&staging.store_file)? else {
        return Ok(());
    };
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(io::Error::other)?;
    transaction
        .execute(
            "DELETE FROM progress_history_transactions \
             WHERE transaction_id = ?1 AND state = ?2",
            params![staging.transaction_id, HISTORY_STATE_STAGED],
        )
        .map_err(io::Error::other)?;
    transaction.commit().map_err(io::Error::other)
}

fn read_committed_progress_history(
    harness_home: &Path,
    queue_ids: &BTreeSet<String>,
) -> io::Result<Vec<ProgressHistoryRecord>> {
    let store_file = progress_history_file(harness_home);
    let Some(connection) = open_existing_progress_history(&store_file)? else {
        return Ok(Vec::new());
    };
    let queue_ids = queue_ids
        .iter()
        .filter(|queue_id| !queue_id.trim().is_empty())
        .cloned()
        .collect::<Vec<_>>();
    if queue_ids.is_empty() {
        return Ok(Vec::new());
    }
    let placeholders = std::iter::repeat_n("?", queue_ids.len())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT history.source_line, history.event_json \
         FROM progress_history_events AS history \
         INNER JOIN progress_history_transactions AS tx \
           ON tx.transaction_id = history.transaction_id \
         WHERE tx.state = ? AND history.queue_id IN ({placeholders}) \
         ORDER BY history.at_ms ASC, history.source_line ASC, history.event_id ASC"
    );
    let mut values = Vec::with_capacity(queue_ids.len().saturating_add(1));
    values.push(HISTORY_STATE_COMMITTED.to_string());
    values.extend(queue_ids);
    let mut statement = connection.prepare(&sql).map_err(io::Error::other)?;
    let rows = statement
        .query_map(params_from_iter(values.iter()), |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(io::Error::other)?;
    let mut records = Vec::new();
    for row in rows {
        let (source_line, event_json) = row.map_err(io::Error::other)?;
        let original_line = usize::try_from(source_line).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "progress history source line is outside the supported range",
            )
        })?;
        let event = serde_json::from_str::<AgentProgressEvent>(&event_json).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("committed progress history event is invalid JSON: {error}"),
            )
        })?;
        if valid_progress_event_id(event.event_id.as_deref()).is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "committed progress history event has no canonical event id",
            ));
        }
        records.push(ProgressHistoryRecord {
            original_line,
            event,
            cold: true,
        });
    }
    Ok(records)
}

fn read_hot_progress_history_for_queue_ids(
    events_file: &Path,
    queue_ids: &BTreeSet<String>,
    warnings: &mut Vec<String>,
) -> io::Result<Vec<ProgressHistoryRecord>> {
    let bytes = match fs::read(events_file) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut records = Vec::new();
    let mut warning_budget = MAX_COMPACTION_WARNING_LINES;
    for (line_index, raw) in bytes.split_inclusive(|byte| *byte == b'\n').enumerate() {
        let trimmed = trim_ascii_whitespace(raw);
        if trimmed.is_empty() {
            continue;
        }
        match serde_json::from_slice::<AgentProgressEvent>(trimmed) {
            Ok(event) if queue_ids.contains(&event.queue_id) => {
                records.push(ProgressHistoryRecord {
                    original_line: line_index.saturating_add(1),
                    event,
                    cold: false,
                });
            }
            Ok(_) => {}
            Err(error) if warning_budget > 0 => {
                warning_budget = warning_budget.saturating_sub(1);
                warnings.push(format!(
                    "exact progress history lookup skipped malformed hot line {} at {}: {}",
                    line_index.saturating_add(1),
                    events_file.display(),
                    error
                ));
            }
            Err(_) => {}
        }
    }
    Ok(records)
}

fn open_or_create_progress_history(store_file: &Path) -> io::Result<Connection> {
    if let Some(parent) = store_file.parent() {
        fs::create_dir_all(parent)?;
    }
    let connection = Connection::open(store_file).map_err(io::Error::other)?;
    initialize_progress_history(&connection)?;
    Ok(connection)
}

fn open_existing_progress_history(store_file: &Path) -> io::Result<Option<Connection>> {
    match fs::metadata(store_file) {
        Ok(metadata) if metadata.is_file() => {}
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "progress history path is not a file: {}",
                    store_file.display()
                ),
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    }
    let connection = Connection::open(store_file).map_err(io::Error::other)?;
    initialize_progress_history(&connection)?;
    Ok(Some(connection))
}

fn initialize_progress_history(connection: &Connection) -> io::Result<()> {
    connection
        .execute_batch(
            "PRAGMA foreign_keys = ON;
             PRAGMA busy_timeout = 5000;
             CREATE TABLE IF NOT EXISTS progress_history_meta (
                 singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                 schema TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS progress_history_transactions (
                 transaction_id TEXT PRIMARY KEY,
                 state TEXT NOT NULL CHECK (state IN ('staged', 'committed')),
                 created_at_ms INTEGER NOT NULL,
                 committed_at_ms INTEGER
             );
             CREATE TABLE IF NOT EXISTS progress_history_events (
                 transaction_id TEXT NOT NULL,
                 event_id TEXT NOT NULL UNIQUE,
                 queue_id TEXT NOT NULL,
                 source_line INTEGER NOT NULL,
                 at_ms INTEGER NOT NULL,
                 event_json TEXT NOT NULL,
                 PRIMARY KEY (transaction_id, event_id),
                 FOREIGN KEY (transaction_id) REFERENCES progress_history_transactions(transaction_id) ON DELETE CASCADE
             );
             CREATE INDEX IF NOT EXISTS progress_history_events_queue_idx
                 ON progress_history_events(queue_id, at_ms, source_line);
             CREATE INDEX IF NOT EXISTS progress_history_events_event_idx
                 ON progress_history_events(event_id);",
        )
        .map_err(io::Error::other)?;
    let existing_schema: Option<String> = connection
        .query_row(
            "SELECT schema FROM progress_history_meta WHERE singleton = 1",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(io::Error::other)?;
    match existing_schema.as_deref() {
        None => {
            connection
                .execute(
                    "INSERT INTO progress_history_meta (singleton, schema) VALUES (1, ?1)",
                    params![PROGRESS_HISTORY_SCHEMA],
                )
                .map_err(io::Error::other)?;
        }
        Some(PROGRESS_HISTORY_SCHEMA) => {}
        Some(actual) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "unsupported progress history schema `{actual}`; expected `{PROGRESS_HISTORY_SCHEMA}`"
                ),
            ));
        }
    }
    Ok(())
}

fn progress_history_marker_file_for_events(events_file: &Path) -> PathBuf {
    let file_name = events_file
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("progress-events.jsonl");
    events_file.with_file_name(format!(".{file_name}.history-compaction-pending.json"))
}

fn progress_history_temp_file(events_file: &Path, now_ms: i64, kind: &str) -> PathBuf {
    let file_name = events_file
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("progress-events.jsonl");
    let mut attempt = 0_u32;
    loop {
        let candidate = events_file.with_file_name(format!(
            ".{file_name}.{}.{}.history-{}.{}.tmp",
            std::process::id(),
            now_ms,
            kind,
            attempt
        ));
        if !candidate.exists() {
            return candidate;
        }
        attempt = attempt.saturating_add(1);
    }
}

fn write_progress_history_file(path: &Path, bytes: &[u8]) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = File::create(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

fn replace_progress_events_file(temp_file: &Path, events_file: &Path) -> io::Result<()> {
    let started = Instant::now();
    loop {
        match fs::remove_file(events_file) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::PermissionDenied | io::ErrorKind::AlreadyExists
                ) && started.elapsed() < Duration::from_secs(10) =>
            {
                thread::sleep(Duration::from_millis(10));
                continue;
            }
            Err(error) => return Err(error),
        }
        match fs::rename(temp_file, events_file) {
            Ok(()) => return Ok(()),
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::PermissionDenied | io::ErrorKind::AlreadyExists
                ) && started.elapsed() < Duration::from_secs(10) =>
            {
                thread::sleep(Duration::from_millis(10));
            }
            Err(error) => return Err(error),
        }
    }
}

fn invalidate_progress_event_index(index_file: &Path) -> io::Result<()> {
    remove_file_if_present(index_file)?;
    let index_name = index_file
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("progress-event-index.sqlite");
    for suffix in ["-wal", "-shm"] {
        remove_file_if_present(&index_file.with_file_name(format!("{index_name}{suffix}")))?;
    }
    Ok(())
}

fn remove_file_if_present(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn progress_history_file_matches(
    path: &Path,
    expected_bytes: u64,
    expected_digest: u64,
) -> io::Result<bool> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    Ok(bytes.len() as u64 == expected_bytes && progress_history_digest(&bytes) == expected_digest)
}

fn progress_history_digest(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn next_progress_history_transaction_id(now_ms: i64, source: &[u8]) -> String {
    let sequence = PROGRESS_HISTORY_TRANSACTION_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!(
        "progress-history-{}-{}-{:016x}-{}",
        std::process::id(),
        now_ms,
        progress_history_digest(source),
        sequence
    )
}

fn current_time_ms() -> io::Result<i64> {
    let duration = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(io::Error::other)?;
    i64::try_from(duration.as_millis()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "current timestamp exceeds the supported millisecond range",
        )
    })
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

fn history_identity_key(event: &AgentProgressEvent, line: usize, cold: bool) -> String {
    if let Some(event_id) = valid_progress_event_id(event.event_id.as_deref()) {
        format!("event:{event_id}")
    } else {
        format!(
            "{}:legacy:{}:{}",
            if cold { "cold" } else { "hot" },
            event.queue_id,
            line
        )
    }
}

#[cfg(test)]
mod tests {
    use super::super::{
        AgentProgressContext, AgentProgressDeliveryCursor, AgentProgressDeliveryMessageKind,
        AgentProgressDeliveryState, AgentProgressEvent, AgentProgressHistoryCompactionOptions,
        AgentProgressKind, AgentProgressStatus, agent_progress_delivery_state_file,
        agent_progress_events_file, compact_agent_progress_history, write_delivery_state,
    };
    use super::*;
    use crate::write_json_atomic;
    use rusqlite::Connection;
    use std::collections::BTreeSet;
    use std::fs::{self, File, OpenOptions};
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_harness(test_name: &str) -> (PathBuf, PathBuf) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "agent-harness-progress-history-{test_name}-{}-{nanos}",
            std::process::id()
        ));
        (root.clone(), root.join(".agent-harness"))
    }

    fn event(queue_id: &str, event_id: &str, terminal: bool, at_ms: i64) -> AgentProgressEvent {
        let context = AgentProgressContext {
            queue_id: queue_id.to_string(),
            agent_id: Some("main".to_string()),
            account_id: Some("default".to_string()),
            thread_id: None,
            session_key: format!("session:{queue_id}"),
            platform: "telegram".to_string(),
            channel_id: "dm".to_string(),
            user_id: "user".to_string(),
        };
        let mut event = AgentProgressEvent::new(
            &context,
            if terminal {
                AgentProgressKind::Runtime
            } else {
                AgentProgressKind::ToolCall
            },
            if terminal { "finished" } else { "working" },
            "test progress",
            if terminal {
                AgentProgressStatus::Completed
            } else {
                AgentProgressStatus::Progress
            },
            at_ms,
        );
        event.event_id = Some(event_id.to_string());
        event
    }

    fn write_events(harness_home: &Path, events: &[AgentProgressEvent]) {
        let events_file = agent_progress_events_file(harness_home);
        fs::create_dir_all(events_file.parent().unwrap()).unwrap();
        let mut file = File::create(events_file).unwrap();
        for event in events {
            serde_json::to_writer(&mut file, event).unwrap();
            file.write_all(b"\n").unwrap();
        }
        file.sync_all().unwrap();
    }

    fn compact(
        harness_home: &Path,
        evidence: BTreeSet<(String, String)>,
        max_hot_bytes: u64,
        max_terminal_queues: usize,
    ) -> ProgressHistoryCompactionResult {
        compact_progress_history(
            harness_home,
            ProgressHistoryCompactionPolicy {
                max_hot_bytes,
                max_terminal_queues,
            },
            &evidence,
            1_000,
            &mut Vec::new(),
        )
        .unwrap()
    }

    #[test]
    fn public_compactor_requires_terminal_delivery_proof_before_moving_v2_source() {
        let (root, harness_home) = temp_harness("public-delivery-proof");
        let closed = event("closed", "pe2-public-terminal-00000000000001", true, 100);
        write_events(&harness_home, std::slice::from_ref(&closed));

        let before_delivery =
            compact_agent_progress_history(AgentProgressHistoryCompactionOptions {
                harness_home: harness_home.clone(),
                now_ms: 1_000,
                max_hot_bytes: 0,
                max_terminal_queues: 0,
            })
            .unwrap();
        assert_eq!(before_delivery.compacted_events, 0);
        assert_eq!(before_delivery.retained_undelivered_queues, 1);

        let mut cursor = AgentProgressDeliveryCursor::new(
            closed.platform.clone(),
            closed.channel_id.clone(),
            closed.user_id.clone(),
            closed.session_key.clone(),
        );
        let event_id = closed.event_id.clone().unwrap();
        cursor.record_lane_with_identity(
            AgentProgressDeliveryMessageKind::Body,
            Some("body-message".to_string()),
            1,
            Some(event_id.clone()),
            "body-terminal".to_string(),
            1_001,
            true,
            true,
        );
        cursor.record_lane_with_identity(
            AgentProgressDeliveryMessageKind::Status,
            Some("status-message".to_string()),
            1,
            Some(event_id),
            "status-terminal".to_string(),
            1_001,
            true,
            true,
        );
        let mut state = AgentProgressDeliveryState::default();
        state.queues.insert(closed.queue_id.clone(), cursor);
        write_delivery_state(&agent_progress_delivery_state_file(&harness_home), &state).unwrap();

        let after_delivery =
            compact_agent_progress_history(AgentProgressHistoryCompactionOptions {
                harness_home: harness_home.clone(),
                now_ms: 1_002,
                max_hot_bytes: 0,
                max_terminal_queues: 0,
            })
            .unwrap();
        assert_eq!(after_delivery.compacted_events, 1);
        assert!(
            fs::read(agent_progress_events_file(&harness_home))
                .unwrap()
                .is_empty()
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn open_and_undelivered_queues_are_never_compacted() {
        let (root, harness_home) = temp_harness("open-and-undelivered");
        let delivered = event(
            "delivered",
            "pe2-delivered-terminal-000000000001",
            true,
            100,
        );
        let undelivered = event(
            "undelivered",
            "pe2-undelivered-terminal-00000001",
            true,
            200,
        );
        let open = event("open", "pe2-open-progress-000000000000001", false, 300);
        write_events(
            &harness_home,
            &[delivered.clone(), undelivered.clone(), open.clone()],
        );

        let report = compact(
            &harness_home,
            BTreeSet::from([(
                delivered.queue_id.clone(),
                delivered.event_id.clone().unwrap(),
            )]),
            0,
            0,
        );
        assert_eq!(report.compacted_queues, 1);
        assert_eq!(report.compacted_events, 1);
        assert_eq!(report.retained_undelivered_queues, 1);
        assert_eq!(report.retained_open_queues, 1);
        assert!(report.backpressure);

        let hot = fs::read_to_string(agent_progress_events_file(&harness_home)).unwrap();
        assert!(!hot.contains(delivered.event_id.as_deref().unwrap()));
        assert!(hot.contains(undelivered.event_id.as_deref().unwrap()));
        assert!(hot.contains(open.event_id.as_deref().unwrap()));
        let cold = read_committed_progress_history(
            &harness_home,
            &BTreeSet::from(["delivered".to_string()]),
        )
        .unwrap();
        assert_eq!(cold.len(), 1);
        assert_eq!(cold[0].event.event_id, delivered.event_id);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn legacy_rows_form_a_retention_barrier_instead_of_shifting_line_identity() {
        let (root, harness_home) = temp_harness("legacy-barrier");
        let delivered = event(
            "delivered",
            "pe2-delivered-terminal-000000000001",
            true,
            100,
        );
        let mut legacy = event("legacy", "pe2-legacy-will-be-removed-000001", true, 200);
        legacy.schema = "agent-harness.progress-event.v1".to_string();
        legacy.event_id = None;
        write_events(&harness_home, &[delivered.clone(), legacy.clone()]);

        let report = compact(
            &harness_home,
            BTreeSet::from([(
                delivered.queue_id.clone(),
                delivered.event_id.clone().unwrap(),
            )]),
            0,
            0,
        );
        assert_eq!(report.compacted_events, 0);
        assert!(report.retained_legacy_queues >= 1);
        assert!(report.backpressure);
        let hot = fs::read_to_string(agent_progress_events_file(&harness_home)).unwrap();
        assert!(hot.contains(delivered.event_id.as_deref().unwrap()));
        assert!(hot.contains("agent-harness.progress-event.v1"));
        assert!(
            read_committed_progress_history(
                &harness_home,
                &BTreeSet::from(["delivered".to_string()])
            )
            .unwrap()
            .is_empty()
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn exact_lookup_joins_committed_cold_history_with_hot_events_and_invalidates_index() {
        let (root, harness_home) = temp_harness("exact-hot-cold");
        let delivered = event("closed", "pe2-closed-terminal-00000000000001", true, 100);
        write_events(&harness_home, std::slice::from_ref(&delivered));
        let mut index_warnings = Vec::new();
        crate::progress_event_index::refresh_progress_event_index(
            &harness_home,
            &mut index_warnings,
        )
        .unwrap();
        let index_file = progress_event_index_file(&harness_home);
        assert!(index_file.is_file());

        let report = compact(
            &harness_home,
            BTreeSet::from([(
                delivered.queue_id.clone(),
                delivered.event_id.clone().unwrap(),
            )]),
            0,
            0,
        );
        assert_eq!(report.compacted_events, 1);
        assert!(
            !index_file.exists(),
            "the hot projection must be invalidated before a relocated source becomes visible"
        );

        let hot = event("open", "pe2-open-hot-0000000000000000001", false, 200);
        let mut file = OpenOptions::new()
            .append(true)
            .open(agent_progress_events_file(&harness_home))
            .unwrap();
        serde_json::to_writer(&mut file, &hot).unwrap();
        file.write_all(b"\n").unwrap();
        file.sync_all().unwrap();

        let records = read_progress_history_for_queue_ids(
            &harness_home,
            &BTreeSet::from(["closed".to_string(), "open".to_string()]),
            &mut Vec::new(),
        )
        .unwrap();
        assert_eq!(records.len(), 2);
        assert!(records.iter().any(|record| {
            record.cold && record.event.event_id.as_deref() == delivered.event_id.as_deref()
        }));
        assert!(records.iter().any(|record| {
            !record.cold && record.event.event_id.as_deref() == hot.event_id.as_deref()
        }));

        let snapshot = crate::progress_event_index::progress_event_delivery_snapshot(
            &harness_home,
            &BTreeSet::new(),
            0,
            0,
            0,
            true,
            &mut Vec::new(),
        )
        .unwrap();
        assert!(
            !snapshot.events.contains_key("closed"),
            "a rebuilt hot projection must not resurrect a compacted terminal queue"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn recovery_commits_staged_history_after_a_hot_source_swap_crash() {
        let (root, harness_home) = temp_harness("crash-recovery");
        let closed = event("closed", "pe2-crash-terminal-000000000000001", true, 100);
        write_events(&harness_home, std::slice::from_ref(&closed));
        let events_file = agent_progress_events_file(&harness_home);
        let original = fs::read(&events_file).unwrap();
        let event_line = QueueEventLine {
            line_number: 1,
            event: closed.clone(),
            canonical_v2: true,
            raw_bytes: original.len(),
        };
        let staging = stage_progress_history(
            &progress_history_file(&harness_home),
            "crash-recovery-transaction",
            &[history_candidate_from_event_line(&event_line).unwrap()],
            1_000,
        )
        .unwrap();
        let archive_file = progress_history_temp_file(&events_file, 1_000, "archive");
        let temp_file = progress_history_temp_file(&events_file, 1_000, "compact");
        write_progress_history_file(&archive_file, &original).unwrap();
        write_progress_history_file(&temp_file, b"").unwrap();
        let marker_file = progress_history_marker_file(&harness_home);
        let pending = ProgressHistoryCompactionPending {
            schema: COMPACTION_PENDING_SCHEMA.to_string(),
            events_file: events_file.clone(),
            archive_file: archive_file.clone(),
            temp_file: temp_file.clone(),
            expected_hot_bytes: 0,
            expected_hot_digest: progress_history_digest(b""),
            original_hot_bytes: original.len() as u64,
            original_hot_digest: progress_history_digest(&original),
            history_store_file: staging.store_file.clone(),
            history_transaction_id: staging.transaction_id.clone(),
            index_file: progress_event_index_file(&harness_home),
        };
        write_json_atomic(&marker_file, &pending).unwrap();
        invalidate_progress_event_index(&pending.index_file).unwrap();
        replace_progress_events_file(&temp_file, &events_file).unwrap();
        assert!(
            read_committed_progress_history(&harness_home, &BTreeSet::from(["closed".to_string()]))
                .unwrap()
                .is_empty(),
            "staged cold rows must remain invisible until recovery observes the hot swap"
        );

        let outcome =
            recover_progress_history_compaction_if_needed(&harness_home, &mut Vec::new()).unwrap();
        assert_eq!(outcome, ProgressHistoryCompactionRecovery::Recovered);
        assert!(!marker_file.exists());
        assert!(fs::read(&events_file).unwrap().is_empty());
        let recovered =
            read_committed_progress_history(&harness_home, &BTreeSet::from(["closed".to_string()]))
                .unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].event.event_id, closed.event_id);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn recovery_restores_the_archive_and_discards_staged_history_when_snapshot_is_missing() {
        let (root, harness_home) = temp_harness("crash-restore");
        let closed = event("closed", "pe2-restore-terminal-0000000000001", true, 100);
        write_events(&harness_home, std::slice::from_ref(&closed));
        let events_file = agent_progress_events_file(&harness_home);
        let original = fs::read(&events_file).unwrap();
        let event_line = QueueEventLine {
            line_number: 1,
            event: closed.clone(),
            canonical_v2: true,
            raw_bytes: original.len(),
        };
        let staging = stage_progress_history(
            &progress_history_file(&harness_home),
            "crash-restore-transaction",
            &[history_candidate_from_event_line(&event_line).unwrap()],
            1_000,
        )
        .unwrap();
        let archive_file = progress_history_temp_file(&events_file, 1_000, "archive");
        let missing_temp = progress_history_temp_file(&events_file, 1_000, "compact");
        write_progress_history_file(&archive_file, &original).unwrap();
        let marker_file = progress_history_marker_file(&harness_home);
        let pending = ProgressHistoryCompactionPending {
            schema: COMPACTION_PENDING_SCHEMA.to_string(),
            events_file: events_file.clone(),
            archive_file: archive_file.clone(),
            temp_file: missing_temp,
            expected_hot_bytes: 0,
            expected_hot_digest: progress_history_digest(b""),
            original_hot_bytes: original.len() as u64,
            original_hot_digest: progress_history_digest(&original),
            history_store_file: staging.store_file.clone(),
            history_transaction_id: staging.transaction_id.clone(),
            index_file: progress_event_index_file(&harness_home),
        };
        write_json_atomic(&marker_file, &pending).unwrap();
        write_progress_history_file(&events_file, b"interrupted-source").unwrap();

        let outcome =
            recover_progress_history_compaction_if_needed(&harness_home, &mut Vec::new()).unwrap();
        assert_eq!(outcome, ProgressHistoryCompactionRecovery::Recovered);
        assert!(!marker_file.exists());
        assert_eq!(fs::read(&events_file).unwrap(), original);
        assert!(
            read_committed_progress_history(&harness_home, &BTreeSet::from(["closed".to_string()]))
                .unwrap()
                .is_empty(),
            "restoring pre-compaction hot data must keep a staged transaction invisible"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn compacts_one_hundred_thousand_terminal_events_without_retaining_an_unbounded_hot_payload() {
        let (root, harness_home) = temp_harness("one-hundred-thousand");
        let events_file = agent_progress_events_file(&harness_home);
        fs::create_dir_all(events_file.parent().unwrap()).unwrap();
        let mut file = File::create(&events_file).unwrap();
        let mut last_event_id = String::new();
        for index in 0..100_000usize {
            let event_id = format!("pe2-bulk-terminal-{index:024}");
            let event = event("bulk", &event_id, true, index as i64);
            serde_json::to_writer(&mut file, &event).unwrap();
            file.write_all(b"\n").unwrap();
            last_event_id = event_id;
        }
        file.sync_all().unwrap();

        let report = compact(
            &harness_home,
            BTreeSet::from([("bulk".to_string(), last_event_id)]),
            0,
            0,
        );
        assert_eq!(report.compacted_queues, 1);
        assert_eq!(report.compacted_events, 100_000);
        assert_eq!(report.hot_bytes_after, 0);
        assert!(!report.backpressure);
        assert!(fs::read(&events_file).unwrap().is_empty());

        let connection = Connection::open(progress_history_file(&harness_home)).unwrap();
        let count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM progress_history_events AS history \
                 INNER JOIN progress_history_transactions AS tx \
                   ON tx.transaction_id = history.transaction_id \
                 WHERE tx.state = 'committed'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 100_000);

        let _ = fs::remove_dir_all(root);
    }
}
