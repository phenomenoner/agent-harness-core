//! Durable cold storage for compacted channel-delivery receipt ledgers.
//!
//! The hot `delivery-receipts.jsonl` ledger remains the only file written by
//! normal delivery work.  Terminal, canonical-v2 delivery histories can be
//! moved into immutable cold segments by a journaled maintenance transaction.
//! The manifest exposes only fully committed segments, so a crash cannot make
//! the same receipt visible from both hot and cold storage.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::channel_delivery::ChannelDeliveryReceipt;
use crate::logging::write_json_atomic;

pub(crate) const CHANNEL_DELIVERY_RECEIPT_HISTORY_SCHEMA: &str =
    "agent-harness.channel-delivery-receipt-history.v1";

const CHANNEL_DELIVERY_RECEIPT_HISTORY_MANIFEST_FILE: &str =
    "delivery-receipt-history.manifest.json";
const CHANNEL_DELIVERY_RECEIPT_HISTORY_DIR: &str = "delivery-receipt-history";
const CHANNEL_DELIVERY_RECEIPT_COMPACTION_JOURNAL_FILE: &str =
    ".delivery-receipts.compaction-pending.json";
const CHANNEL_DELIVERY_RECEIPT_COMPACTION_LOCK_FILE: &str =
    "delivery-receipts.compaction.transaction";
const CHANNEL_DELIVERY_RECEIPT_COMPACTION_STATE_FILE: &str =
    "delivery-receipts.compaction-state.json";
const CHANNEL_DELIVERY_RECEIPT_COMPACTION_JOURNAL_SCHEMA: &str =
    "agent-harness.channel-delivery-receipt-compaction-journal.v1";
const CHANNEL_DELIVERY_RECEIPT_COMPACTION_STATE_SCHEMA: &str =
    "agent-harness.channel-delivery-receipt-compaction-state.v1";

pub(crate) const CHANNEL_DELIVERY_RECEIPT_COMPACTION_RETRY_INTERVAL_MS: i64 = 60_000;
pub(crate) const CHANNEL_DELIVERY_RECEIPT_COMPACTION_RETRY_GROWTH_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ChannelDeliveryReceiptHistory {
    pub(crate) generation: u64,
    pub(crate) segments: Vec<ChannelDeliveryReceiptHistorySegment>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ChannelDeliveryReceiptHistorySegment {
    pub(crate) transaction_id: String,
    pub(crate) file_name: String,
    pub(crate) records: usize,
    pub(crate) bytes: u64,
    pub(crate) digest: u64,
    pub(crate) compacted_at_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ChannelDeliveryReceiptCompactionLimits {
    pub(crate) max_hot_bytes: u64,
    pub(crate) target_hot_bytes: u64,
    pub(crate) max_hot_records: usize,
    pub(crate) target_hot_records: usize,
    pub(crate) max_compaction_records: usize,
    pub(crate) force: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ChannelDeliveryReceiptCompactionPlan {
    pub(crate) original_bytes: u64,
    pub(crate) original_records: usize,
    pub(crate) original_digest: u64,
    pub(crate) hot_snapshot: Vec<u8>,
    pub(crate) hot_records: usize,
    pub(crate) cold_snapshot: Vec<u8>,
    pub(crate) compacted_records: usize,
    pub(crate) compacted_delivery_ids: usize,
}

impl ChannelDeliveryReceiptCompactionPlan {
    pub(crate) fn changed(&self) -> bool {
        !self.cold_snapshot.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ChannelDeliveryReceiptHistoryCommit {
    pub(crate) segment: ChannelDeliveryReceiptHistorySegment,
    pub(crate) hot_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ChannelDeliveryReceiptHistoryManifest {
    schema: String,
    #[serde(default)]
    generation: u64,
    #[serde(default)]
    segments: Vec<ChannelDeliveryReceiptHistorySegment>,
}

impl Default for ChannelDeliveryReceiptHistoryManifest {
    fn default() -> Self {
        Self {
            schema: CHANNEL_DELIVERY_RECEIPT_HISTORY_SCHEMA.to_string(),
            generation: 0,
            segments: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum ChannelDeliveryReceiptCompactionJournalPhase {
    Prepared,
    ColdPublished,
    HotSwapped,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ChannelDeliveryReceiptCompactionJournal {
    schema: String,
    transaction_id: String,
    phase: ChannelDeliveryReceiptCompactionJournalPhase,
    original_hot_bytes: u64,
    original_hot_digest: u64,
    hot_snapshot_bytes: u64,
    hot_snapshot_digest: u64,
    cold_bytes: u64,
    cold_digest: u64,
    segment: ChannelDeliveryReceiptHistorySegment,
    hot_snapshot_temp_name: String,
    cold_temp_name: String,
    hot_backup_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ChannelDeliveryReceiptCompactionAttemptState {
    schema: String,
    last_attempt_at_ms: i64,
    last_attempt_hot_bytes: u64,
}

#[derive(Debug, Clone)]
struct LedgerLine {
    start: usize,
    end: usize,
    record_count: usize,
    compactable_delivery_id: Option<String>,
}

#[derive(Debug, Clone, Copy, Default)]
struct DeliveryCandidate {
    records: usize,
    bytes: usize,
}

/// Returns the directory containing immutable cold receipt segments.
pub(crate) fn channel_delivery_receipt_history_dir(channel_dir: &Path) -> PathBuf {
    channel_dir.join(CHANNEL_DELIVERY_RECEIPT_HISTORY_DIR)
}

/// Returns the small manifest read by the indexed normal path.  The manifest
/// is deliberately separate from the cold JSONL segments so normal delivery
/// planning never needs to reopen historical receipt data.
pub(crate) fn channel_delivery_receipt_history_manifest_file(channel_dir: &Path) -> PathBuf {
    channel_dir.join(CHANNEL_DELIVERY_RECEIPT_HISTORY_MANIFEST_FILE)
}

/// Returns the single-writer compaction lock.  It is independent from the hot
/// ledger lock so two maintenance callers cannot construct competing snapshots.
pub(crate) fn channel_delivery_receipt_compaction_lock_file(channel_dir: &Path) -> PathBuf {
    channel_dir.join(CHANNEL_DELIVERY_RECEIPT_COMPACTION_LOCK_FILE)
}

pub(crate) fn channel_delivery_receipt_compaction_state_file(channel_dir: &Path) -> PathBuf {
    channel_dir.join(CHANNEL_DELIVERY_RECEIPT_COMPACTION_STATE_FILE)
}

pub(crate) fn read_channel_delivery_receipt_compaction_attempt_state(
    channel_dir: &Path,
) -> io::Result<Option<ChannelDeliveryReceiptCompactionAttemptState>> {
    let state_file = channel_delivery_receipt_compaction_state_file(channel_dir);
    let bytes = match fs::read(state_file) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let state: ChannelDeliveryReceiptCompactionAttemptState =
        serde_json::from_slice(&bytes).map_err(io::Error::other)?;
    if state.schema != CHANNEL_DELIVERY_RECEIPT_COMPACTION_STATE_SCHEMA {
        return Ok(None);
    }
    Ok(Some(state))
}

pub(crate) fn write_channel_delivery_receipt_compaction_attempt_state(
    channel_dir: &Path,
    now_ms: i64,
    hot_bytes: u64,
) -> io::Result<()> {
    write_json_atomic(
        &channel_delivery_receipt_compaction_state_file(channel_dir),
        &ChannelDeliveryReceiptCompactionAttemptState {
            schema: CHANNEL_DELIVERY_RECEIPT_COMPACTION_STATE_SCHEMA.to_string(),
            last_attempt_at_ms: now_ms,
            last_attempt_hot_bytes: hot_bytes,
        },
    )
}

pub(crate) fn channel_delivery_receipt_compaction_retry_is_deferred(
    now_ms: i64,
    observed_hot_bytes: u64,
    previous: Option<&ChannelDeliveryReceiptCompactionAttemptState>,
) -> bool {
    let Some(previous) = previous else {
        return false;
    };
    if now_ms <= 0 {
        return false;
    }
    let retry_at_ms = previous
        .last_attempt_at_ms
        .saturating_add(CHANNEL_DELIVERY_RECEIPT_COMPACTION_RETRY_INTERVAL_MS);
    let grew_enough = observed_hot_bytes.saturating_sub(previous.last_attempt_hot_bytes)
        >= CHANNEL_DELIVERY_RECEIPT_COMPACTION_RETRY_GROWTH_BYTES;
    now_ms < retry_at_ms && !grew_enough
}

fn channel_delivery_receipt_compaction_journal_file(channel_dir: &Path) -> PathBuf {
    channel_dir.join(CHANNEL_DELIVERY_RECEIPT_COMPACTION_JOURNAL_FILE)
}

pub(crate) fn channel_delivery_receipt_history_segment_file(
    channel_dir: &Path,
    segment: &ChannelDeliveryReceiptHistorySegment,
) -> io::Result<PathBuf> {
    validated_history_file(
        &channel_delivery_receipt_history_dir(channel_dir),
        &segment.file_name,
    )
}

/// Reads the committed manifest only.  This does not open or replay any cold
/// receipt segment and is therefore safe to call from normal interactive paths.
pub(crate) fn read_channel_delivery_receipt_history(
    channel_dir: &Path,
) -> io::Result<ChannelDeliveryReceiptHistory> {
    let manifest = read_manifest(channel_dir)?;
    Ok(ChannelDeliveryReceiptHistory {
        generation: manifest.generation,
        segments: manifest.segments,
    })
}

/// Creates a compaction snapshot while the caller owns the hot ledger append
/// lock.  Each selected delivery moves as one complete receipt history; this
/// preserves retries followed by a terminal receipt without splitting their
/// ordering across hot and cold storage.
pub(crate) fn plan_channel_delivery_receipt_compaction(
    original: &[u8],
    canonical_delivery_ids: &BTreeMap<String, String>,
    terminal_v2_delivery_ids: &BTreeSet<String>,
    limits: ChannelDeliveryReceiptCompactionLimits,
) -> io::Result<ChannelDeliveryReceiptCompactionPlan> {
    let mut lines = Vec::new();
    let mut candidates = BTreeMap::<String, DeliveryCandidate>::new();
    let mut candidate_order = Vec::new();
    let mut original_records = 0_usize;

    for (start, end) in jsonl_line_ranges(original) {
        let bytes = &original[start..end];
        let trimmed = trim_ascii_whitespace(bytes);
        if trimmed.is_empty() {
            lines.push(LedgerLine {
                start,
                end,
                record_count: 0,
                compactable_delivery_id: None,
            });
            continue;
        }
        original_records = original_records.saturating_add(1);
        let compactable_delivery_id = match std::str::from_utf8(trimmed)
            .ok()
            .and_then(|line| serde_json::from_str::<ChannelDeliveryReceipt>(line).ok())
        {
            Some(receipt) => canonical_delivery_ids
                .get(&receipt.delivery_id)
                .cloned()
                .filter(|delivery_id| terminal_v2_delivery_ids.contains(delivery_id)),
            None => None,
        };
        if let Some(delivery_id) = compactable_delivery_id.as_deref() {
            let was_present = candidates.contains_key(delivery_id);
            let candidate = candidates.entry(delivery_id.to_string()).or_default();
            candidate.records = candidate.records.saturating_add(1);
            candidate.bytes = candidate.bytes.saturating_add(end.saturating_sub(start));
            if !was_present {
                candidate_order.push(delivery_id.to_string());
            }
        }
        lines.push(LedgerLine {
            start,
            end,
            record_count: 1,
            compactable_delivery_id,
        });
    }

    let max_hot_bytes = limits.max_hot_bytes.max(1);
    let max_hot_records = limits.max_hot_records.max(1);
    let target_hot_bytes = if limits.force {
        0
    } else {
        limits.target_hot_bytes.min(max_hot_bytes)
    };
    let target_hot_records = if limits.force {
        0
    } else {
        limits.target_hot_records.min(max_hot_records)
    };
    let needs_compaction = limits.force
        || (original.len() as u64) > max_hot_bytes
        || original_records > max_hot_records;
    let mut selected_delivery_ids = HashSet::new();
    let mut selected_records = 0_usize;
    let mut retained_bytes = original.len();
    let mut retained_records = original_records;
    if needs_compaction {
        for delivery_id in candidate_order {
            if (retained_bytes as u64) <= target_hot_bytes && retained_records <= target_hot_records
            {
                break;
            }
            let candidate = candidates
                .get(&delivery_id)
                .copied()
                .expect("candidate order must reference a candidate");
            if selected_records.saturating_add(candidate.records) > limits.max_compaction_records {
                // Keep chronological grouping deterministic.  A delivery's
                // complete audit trail is more important than reducing the
                // bound by splitting it, so report backpressure instead.
                break;
            }
            selected_records = selected_records.saturating_add(candidate.records);
            retained_bytes = retained_bytes.saturating_sub(candidate.bytes);
            retained_records = retained_records.saturating_sub(candidate.records);
            selected_delivery_ids.insert(delivery_id);
        }
    }

    let mut hot_snapshot = Vec::with_capacity(retained_bytes);
    let mut cold_snapshot = Vec::with_capacity(original.len().saturating_sub(retained_bytes));
    let mut compacted_records = 0_usize;
    for line in &lines {
        let raw = &original[line.start..line.end];
        if line
            .compactable_delivery_id
            .as_ref()
            .is_some_and(|delivery_id| selected_delivery_ids.contains(delivery_id))
        {
            cold_snapshot.extend_from_slice(raw);
            compacted_records = compacted_records.saturating_add(line.record_count);
        } else {
            hot_snapshot.extend_from_slice(raw);
        }
    }

    Ok(ChannelDeliveryReceiptCompactionPlan {
        original_bytes: original.len() as u64,
        original_records,
        original_digest: bytes_digest(original),
        hot_records: retained_records,
        hot_snapshot,
        cold_snapshot,
        compacted_records,
        compacted_delivery_ids: selected_delivery_ids.len(),
    })
}

/// Commits a prepared hot/cold receipt split.  The caller must hold both the
/// compaction lock and the hot receipt append lock.  A journal plus durable hot
/// backup makes every interruption recoverable without exposing duplicate
/// records through the committed manifest.
pub(crate) fn commit_channel_delivery_receipt_compaction_locked(
    channel_dir: &Path,
    receipts_file: &Path,
    plan: &ChannelDeliveryReceiptCompactionPlan,
    now_ms: i64,
) -> io::Result<Option<ChannelDeliveryReceiptHistoryCommit>> {
    if !plan.changed() {
        return Ok(None);
    }
    // A journal always owns recovery before a subsequent compaction begins.
    // This call is normally a no-op, but it also prevents an accidental second
    // segment after a process restart between a swap and manifest publication.
    let mut warnings = Vec::new();
    let _ = recover_channel_delivery_receipt_compaction_if_needed_locked(
        channel_dir,
        receipts_file,
        &mut warnings,
    )?;
    let original = fs::read(receipts_file)?;
    if original.len() as u64 != plan.original_bytes
        || bytes_digest(&original) != plan.original_digest
    {
        return Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            format!(
                "channel delivery receipt ledger changed before compaction snapshot could commit: {}",
                receipts_file.display()
            ),
        ));
    }

    let history_dir = channel_delivery_receipt_history_dir(channel_dir);
    fs::create_dir_all(&history_dir)?;
    let manifest = read_manifest(channel_dir)?;
    let transaction_id = next_transaction_id(&history_dir, &manifest, plan.original_digest, now_ms);
    let segment = ChannelDeliveryReceiptHistorySegment {
        transaction_id: transaction_id.clone(),
        file_name: format!("segment-{transaction_id}.jsonl"),
        records: plan.compacted_records,
        bytes: plan.cold_snapshot.len() as u64,
        digest: bytes_digest(&plan.cold_snapshot),
        compacted_at_ms: now_ms,
    };
    let cold_final = channel_delivery_receipt_history_segment_file(channel_dir, &segment)?;
    let cold_temp_name = format!(".{transaction_id}.cold.tmp");
    let hot_backup_name = format!(".{transaction_id}.hot-backup.tmp");
    let hot_snapshot_temp_name = format!(".delivery-receipts.{transaction_id}.compact.tmp");
    let cold_temp = validated_history_file(&history_dir, &cold_temp_name)?;
    let hot_backup = validated_history_file(&history_dir, &hot_backup_name)?;
    let hot_snapshot_temp = validated_sibling_file(receipts_file, &hot_snapshot_temp_name)?;

    write_ledger_file(&cold_temp, &plan.cold_snapshot)?;
    write_ledger_file(&hot_backup, &original)?;
    write_ledger_file(&hot_snapshot_temp, &plan.hot_snapshot)?;
    let mut journal = ChannelDeliveryReceiptCompactionJournal {
        schema: CHANNEL_DELIVERY_RECEIPT_COMPACTION_JOURNAL_SCHEMA.to_string(),
        transaction_id,
        phase: ChannelDeliveryReceiptCompactionJournalPhase::Prepared,
        original_hot_bytes: plan.original_bytes,
        original_hot_digest: plan.original_digest,
        hot_snapshot_bytes: plan.hot_snapshot.len() as u64,
        hot_snapshot_digest: bytes_digest(&plan.hot_snapshot),
        cold_bytes: segment.bytes,
        cold_digest: segment.digest,
        segment: segment.clone(),
        hot_snapshot_temp_name,
        cold_temp_name,
        hot_backup_name,
    };
    write_journal(channel_dir, &journal)?;

    publish_cold_segment(&cold_temp, &cold_final, segment.bytes, segment.digest)?;
    journal.phase = ChannelDeliveryReceiptCompactionJournalPhase::ColdPublished;
    write_journal(channel_dir, &journal)?;

    replace_ledger_file(&hot_snapshot_temp, receipts_file)?;
    if !file_matches(
        receipts_file,
        journal.hot_snapshot_bytes,
        journal.hot_snapshot_digest,
    )? {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "channel delivery receipt hot replacement did not match its snapshot: {}",
                receipts_file.display()
            ),
        ));
    }
    journal.phase = ChannelDeliveryReceiptCompactionJournalPhase::HotSwapped;
    write_journal(channel_dir, &journal)?;
    append_manifest_segment(channel_dir, &segment)?;
    cleanup_committed_transaction(channel_dir, receipts_file, &journal)?;

    Ok(Some(ChannelDeliveryReceiptHistoryCommit {
        segment,
        hot_bytes: plan.hot_snapshot.len() as u64,
    }))
}

/// Resolves a journal left by an interrupted compaction.  A completed hot
/// snapshot is promoted to the manifest; a not-yet-swapped hot ledger is
/// rolled back to its pre-compaction form.  Both branches are idempotent.
pub(crate) fn recover_channel_delivery_receipt_compaction_if_needed_locked(
    channel_dir: &Path,
    receipts_file: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<bool> {
    let journal_file = channel_delivery_receipt_compaction_journal_file(channel_dir);
    if !journal_file.is_file() {
        return Ok(false);
    }
    let journal: ChannelDeliveryReceiptCompactionJournal =
        serde_json::from_slice(&fs::read(&journal_file)?).map_err(io::Error::other)?;
    validate_journal(channel_dir, receipts_file, &journal)?;
    let history_dir = channel_delivery_receipt_history_dir(channel_dir);
    let cold_final = channel_delivery_receipt_history_segment_file(channel_dir, &journal.segment)?;
    let cold_temp = validated_history_file(&history_dir, &journal.cold_temp_name)?;
    let hot_backup = validated_history_file(&history_dir, &journal.hot_backup_name)?;
    let hot_snapshot_temp = validated_sibling_file(receipts_file, &journal.hot_snapshot_temp_name)?;
    let manifest = read_manifest(channel_dir)?;
    let manifest_has_segment = match manifest
        .segments
        .iter()
        .find(|segment| segment.transaction_id == journal.segment.transaction_id)
    {
        None => false,
        Some(segment) if segment == &journal.segment => true,
        Some(_) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "channel delivery receipt history manifest has conflicting transaction `{}`",
                    journal.segment.transaction_id
                ),
            ));
        }
    };
    let hot_matches_snapshot = file_matches(
        receipts_file,
        journal.hot_snapshot_bytes,
        journal.hot_snapshot_digest,
    )?;
    let hot_matches_original = file_matches(
        receipts_file,
        journal.original_hot_bytes,
        journal.original_hot_digest,
    )?;
    let backup_matches_original = file_matches(
        &hot_backup,
        journal.original_hot_bytes,
        journal.original_hot_digest,
    )?;
    let cold_ready = cold_segment_available(
        &cold_temp,
        &cold_final,
        journal.cold_bytes,
        journal.cold_digest,
    )?;
    let snapshot_ready = file_matches(
        &hot_snapshot_temp,
        journal.hot_snapshot_bytes,
        journal.hot_snapshot_digest,
    )?;

    if manifest_has_segment {
        if !cold_ready {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "committed channel delivery history transaction `{}` is missing its cold segment",
                    journal.transaction_id
                ),
            ));
        }
        publish_cold_segment(
            &cold_temp,
            &cold_final,
            journal.cold_bytes,
            journal.cold_digest,
        )?;
        if !hot_matches_snapshot {
            if !snapshot_ready {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "committed channel delivery history transaction `{}` cannot restore its hot snapshot",
                        journal.transaction_id
                    ),
                ));
            }
            replace_ledger_file(&hot_snapshot_temp, receipts_file)?;
        }
        cleanup_committed_transaction(channel_dir, receipts_file, &journal)?;
        warnings.push(format!(
            "recovered committed channel delivery receipt compaction transaction `{}`",
            journal.transaction_id
        ));
        return Ok(true);
    }

    if hot_matches_snapshot {
        if !cold_ready {
            if backup_matches_original {
                restore_original_hot_ledger(receipts_file, &hot_backup)?;
                cleanup_rolled_back_transaction(channel_dir, receipts_file, &journal)?;
                warnings.push(format!(
                    "restored channel delivery receipt ledger after incomplete cold history publication for transaction `{}`",
                    journal.transaction_id
                ));
                return Ok(true);
            }
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "channel delivery receipt compaction transaction `{}` has no recoverable cold segment or backup",
                    journal.transaction_id
                ),
            ));
        }
        publish_cold_segment(
            &cold_temp,
            &cold_final,
            journal.cold_bytes,
            journal.cold_digest,
        )?;
        append_manifest_segment(channel_dir, &journal.segment)?;
        cleanup_committed_transaction(channel_dir, receipts_file, &journal)?;
        warnings.push(format!(
            "recovered completed channel delivery receipt compaction transaction `{}`",
            journal.transaction_id
        ));
        return Ok(true);
    }

    if hot_matches_original {
        // The hot ledger remained authoritative, so an unpublished cold
        // segment is an orphan. Remove only the transaction's own verified
        // files; unrelated history is never pruned here.
        cleanup_rolled_back_transaction(channel_dir, receipts_file, &journal)?;
        warnings.push(format!(
            "rolled back pre-swap channel delivery receipt compaction transaction `{}`",
            journal.transaction_id
        ));
        return Ok(true);
    }

    if snapshot_ready && cold_ready {
        publish_cold_segment(
            &cold_temp,
            &cold_final,
            journal.cold_bytes,
            journal.cold_digest,
        )?;
        replace_ledger_file(&hot_snapshot_temp, receipts_file)?;
        append_manifest_segment(channel_dir, &journal.segment)?;
        cleanup_committed_transaction(channel_dir, receipts_file, &journal)?;
        warnings.push(format!(
            "recovered interrupted channel delivery receipt compaction transaction `{}` from staged snapshots",
            journal.transaction_id
        ));
        return Ok(true);
    }

    if backup_matches_original {
        restore_original_hot_ledger(receipts_file, &hot_backup)?;
        cleanup_rolled_back_transaction(channel_dir, receipts_file, &journal)?;
        warnings.push(format!(
            "restored channel delivery receipt ledger from durable backup after interrupted compaction transaction `{}`",
            journal.transaction_id
        ));
        return Ok(true);
    }

    Err(io::Error::new(
        io::ErrorKind::NotFound,
        format!(
            "channel delivery receipt compaction transaction `{}` cannot recover its hot ledger",
            journal.transaction_id
        ),
    ))
}

fn read_manifest(channel_dir: &Path) -> io::Result<ChannelDeliveryReceiptHistoryManifest> {
    let manifest_file = channel_delivery_receipt_history_manifest_file(channel_dir);
    let bytes = match fs::read(&manifest_file) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(ChannelDeliveryReceiptHistoryManifest::default());
        }
        Err(error) => return Err(error),
    };
    let manifest: ChannelDeliveryReceiptHistoryManifest =
        serde_json::from_slice(&bytes).map_err(io::Error::other)?;
    if manifest.schema != CHANNEL_DELIVERY_RECEIPT_HISTORY_SCHEMA {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "unsupported channel delivery receipt history schema `{}`",
                manifest.schema
            ),
        ));
    }
    validate_manifest(&manifest)?;
    Ok(manifest)
}

fn validate_manifest(manifest: &ChannelDeliveryReceiptHistoryManifest) -> io::Result<()> {
    let mut transaction_ids = HashSet::new();
    let mut file_names = HashSet::new();
    for segment in &manifest.segments {
        if segment.transaction_id.trim().is_empty()
            || !transaction_ids.insert(segment.transaction_id.clone())
            || !file_names.insert(segment.file_name.clone())
            || !is_safe_file_name(&segment.file_name)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "channel delivery receipt history manifest contains an invalid or duplicate segment",
            ));
        }
    }
    Ok(())
}

fn append_manifest_segment(
    channel_dir: &Path,
    segment: &ChannelDeliveryReceiptHistorySegment,
) -> io::Result<()> {
    let mut manifest = read_manifest(channel_dir)?;
    if let Some(existing) = manifest
        .segments
        .iter()
        .find(|existing| existing.transaction_id == segment.transaction_id)
    {
        if existing == segment {
            return Ok(());
        }
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "channel delivery receipt history transaction `{}` conflicts with an existing manifest segment",
                segment.transaction_id
            ),
        ));
    }
    manifest.generation = manifest.generation.saturating_add(1);
    manifest.segments.push(segment.clone());
    validate_manifest(&manifest)?;
    write_json_atomic(
        &channel_delivery_receipt_history_manifest_file(channel_dir),
        &manifest,
    )
}

/// Verifies an immutable segment before an index replays it.  Segment content
/// is normally read only once, when its manifest entry first appears; this
/// inexpensive full validation is therefore a corruption guard rather than a
/// normal interactive-path scan.
pub(crate) fn channel_delivery_receipt_history_segment_is_valid(
    channel_dir: &Path,
    segment: &ChannelDeliveryReceiptHistorySegment,
) -> io::Result<bool> {
    file_matches(
        &channel_delivery_receipt_history_segment_file(channel_dir, segment)?,
        segment.bytes,
        segment.digest,
    )
}

fn write_journal(
    channel_dir: &Path,
    journal: &ChannelDeliveryReceiptCompactionJournal,
) -> io::Result<()> {
    write_json_atomic(
        &channel_delivery_receipt_compaction_journal_file(channel_dir),
        journal,
    )
}

fn validate_journal(
    channel_dir: &Path,
    receipts_file: &Path,
    journal: &ChannelDeliveryReceiptCompactionJournal,
) -> io::Result<()> {
    if journal.schema != CHANNEL_DELIVERY_RECEIPT_COMPACTION_JOURNAL_SCHEMA
        || journal.transaction_id.trim().is_empty()
        || journal.segment.transaction_id != journal.transaction_id
        || journal.segment.bytes != journal.cold_bytes
        || journal.segment.digest != journal.cold_digest
        || !is_safe_file_name(&journal.segment.file_name)
        || !is_safe_file_name(&journal.cold_temp_name)
        || !is_safe_file_name(&journal.hot_backup_name)
        || !is_safe_file_name(&journal.hot_snapshot_temp_name)
        || receipts_file.parent() != Some(channel_dir)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "channel delivery receipt compaction journal is invalid for {}",
                receipts_file.display()
            ),
        ));
    }
    Ok(())
}

fn next_transaction_id(
    history_dir: &Path,
    manifest: &ChannelDeliveryReceiptHistoryManifest,
    original_digest: u64,
    now_ms: i64,
) -> String {
    let existing = manifest
        .segments
        .iter()
        .map(|segment| segment.transaction_id.as_str())
        .collect::<HashSet<_>>();
    let mut attempt = 0_u32;
    loop {
        let suffix = if attempt == 0 {
            String::new()
        } else {
            format!("-{attempt}")
        };
        let candidate = format!(
            "receipt-{}-{now_ms}-{original_digest:016x}{suffix}",
            std::process::id()
        );
        let segment_path = history_dir.join(format!("segment-{candidate}.jsonl"));
        if !existing.contains(candidate.as_str()) && !segment_path.exists() {
            return candidate;
        }
        attempt = attempt.saturating_add(1);
    }
}

fn publish_cold_segment(
    cold_temp: &Path,
    cold_final: &Path,
    expected_bytes: u64,
    expected_digest: u64,
) -> io::Result<()> {
    if file_matches(cold_final, expected_bytes, expected_digest)? {
        let _ = fs::remove_file(cold_temp);
        return Ok(());
    }
    if cold_final.exists() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "channel delivery cold receipt segment conflicts with a different file: {}",
                cold_final.display()
            ),
        ));
    }
    if !file_matches(cold_temp, expected_bytes, expected_digest)? {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "channel delivery cold receipt staging segment is missing or invalid: {}",
                cold_temp.display()
            ),
        ));
    }
    fs::rename(cold_temp, cold_final)?;
    if !file_matches(cold_final, expected_bytes, expected_digest)? {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "channel delivery cold receipt segment did not match its staged contents: {}",
                cold_final.display()
            ),
        ));
    }
    Ok(())
}

fn cold_segment_available(
    cold_temp: &Path,
    cold_final: &Path,
    expected_bytes: u64,
    expected_digest: u64,
) -> io::Result<bool> {
    Ok(file_matches(cold_final, expected_bytes, expected_digest)?
        || file_matches(cold_temp, expected_bytes, expected_digest)?)
}

fn restore_original_hot_ledger(receipts_file: &Path, hot_backup: &Path) -> io::Result<()> {
    let restore_temp = receipts_file.with_file_name(format!(
        ".delivery-receipts.{}.restore.tmp",
        std::process::id()
    ));
    let original = fs::read(hot_backup)?;
    write_ledger_file(&restore_temp, &original)?;
    replace_ledger_file(&restore_temp, receipts_file)
}

fn cleanup_committed_transaction(
    channel_dir: &Path,
    receipts_file: &Path,
    journal: &ChannelDeliveryReceiptCompactionJournal,
) -> io::Result<()> {
    let history_dir = channel_delivery_receipt_history_dir(channel_dir);
    let cold_temp = validated_history_file(&history_dir, &journal.cold_temp_name)?;
    let hot_backup = validated_history_file(&history_dir, &journal.hot_backup_name)?;
    let hot_snapshot_temp = validated_sibling_file(receipts_file, &journal.hot_snapshot_temp_name)?;
    remove_if_exists(&cold_temp)?;
    remove_if_exists(&hot_backup)?;
    remove_if_exists(&hot_snapshot_temp)?;
    remove_if_exists(&channel_delivery_receipt_compaction_journal_file(
        channel_dir,
    ))
}

fn cleanup_rolled_back_transaction(
    channel_dir: &Path,
    receipts_file: &Path,
    journal: &ChannelDeliveryReceiptCompactionJournal,
) -> io::Result<()> {
    let history_dir = channel_delivery_receipt_history_dir(channel_dir);
    let cold_temp = validated_history_file(&history_dir, &journal.cold_temp_name)?;
    let cold_final = channel_delivery_receipt_history_segment_file(channel_dir, &journal.segment)?;
    let hot_backup = validated_history_file(&history_dir, &journal.hot_backup_name)?;
    let hot_snapshot_temp = validated_sibling_file(receipts_file, &journal.hot_snapshot_temp_name)?;
    remove_if_matches(&cold_temp, journal.cold_bytes, journal.cold_digest)?;
    remove_if_matches(&cold_final, journal.cold_bytes, journal.cold_digest)?;
    remove_if_matches(
        &hot_backup,
        journal.original_hot_bytes,
        journal.original_hot_digest,
    )?;
    remove_if_matches(
        &hot_snapshot_temp,
        journal.hot_snapshot_bytes,
        journal.hot_snapshot_digest,
    )?;
    remove_if_exists(&channel_delivery_receipt_compaction_journal_file(
        channel_dir,
    ))
}

fn remove_if_matches(path: &Path, expected_bytes: u64, expected_digest: u64) -> io::Result<()> {
    if file_matches(path, expected_bytes, expected_digest)? {
        remove_if_exists(path)?;
    }
    Ok(())
}

fn remove_if_exists(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn write_ledger_file(path: &Path, bytes: &[u8]) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = File::create(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

fn replace_ledger_file(temp_file: &Path, ledger_file: &Path) -> io::Result<()> {
    let started = Instant::now();
    loop {
        match fs::remove_file(ledger_file) {
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
        match fs::rename(temp_file, ledger_file) {
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

fn file_matches(path: &Path, expected_bytes: u64, expected_digest: u64) -> io::Result<bool> {
    let metadata = match fs::metadata(path) {
        Ok(metadata) if metadata.is_file() => metadata,
        Ok(_) => return Ok(false),
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    if metadata.len() != expected_bytes {
        return Ok(false);
    }
    Ok(file_digest(path)? == expected_digest)
}

fn file_digest(path: &Path) -> io::Result<u64> {
    let mut file = File::open(path)?;
    let mut buffer = [0_u8; 16 * 1024];
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    loop {
        let bytes_read = file.read(&mut buffer)?;
        if bytes_read == 0 {
            return Ok(hash);
        }
        for byte in &buffer[..bytes_read] {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
}

fn bytes_digest(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn jsonl_line_ranges(bytes: &[u8]) -> Vec<(usize, usize)> {
    if bytes.is_empty() {
        return Vec::new();
    }
    let mut ranges = Vec::new();
    let mut start = 0_usize;
    for (index, byte) in bytes.iter().enumerate() {
        if *byte == b'\n' {
            ranges.push((start, index.saturating_add(1)));
            start = index.saturating_add(1);
        }
    }
    if start < bytes.len() {
        ranges.push((start, bytes.len()));
    }
    ranges
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

fn validated_history_file(history_dir: &Path, name: &str) -> io::Result<PathBuf> {
    if !is_safe_file_name(name) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsafe channel delivery receipt history file name `{name}`"),
        ));
    }
    Ok(history_dir.join(name))
}

fn validated_sibling_file(receipts_file: &Path, name: &str) -> io::Result<PathBuf> {
    let parent = receipts_file.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "channel delivery receipt ledger has no parent directory: {}",
                receipts_file.display()
            ),
        )
    })?;
    if !is_safe_file_name(name) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsafe channel delivery receipt temp file name `{name}`"),
        ));
    }
    Ok(parent.join(name))
}

fn is_safe_file_name(name: &str) -> bool {
    !name.trim().is_empty()
        && Path::new(name)
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
        && Path::new(name)
            .file_name()
            .and_then(|file_name| file_name.to_str())
            == Some(name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel_delivery::{ChannelDeliveryReceipt, ChannelDeliveryStatus};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn compaction_plan_preserves_active_and_legacy_receipt_histories() {
        let delivered = "delivery:v2:0123456789abcdef0123456789abcdef";
        let active = "delivery:v2:fedcba9876543210fedcba9876543210";
        let legacy = "delivery:9:legacy";
        let original = [
            receipt_line(delivered, ChannelDeliveryStatus::Failed, 1),
            receipt_line(delivered, ChannelDeliveryStatus::Delivered, 2),
            receipt_line(active, ChannelDeliveryStatus::Failed, 3),
            receipt_line(legacy, ChannelDeliveryStatus::Delivered, 4),
        ]
        .concat();
        let canonical = BTreeMap::from([
            (delivered.to_string(), delivered.to_string()),
            (active.to_string(), active.to_string()),
        ]);
        let terminal = BTreeSet::from([delivered.to_string()]);

        let plan = plan_channel_delivery_receipt_compaction(
            original.as_bytes(),
            &canonical,
            &terminal,
            forced_limits(100),
        )
        .unwrap();

        assert_eq!(plan.compacted_records, 2);
        assert_eq!(plan.compacted_delivery_ids, 1);
        let cold = String::from_utf8(plan.cold_snapshot).unwrap();
        let hot = String::from_utf8(plan.hot_snapshot).unwrap();
        assert!(cold.contains(delivered));
        assert!(!cold.contains(active));
        assert!(!cold.contains(legacy));
        assert!(hot.contains(active));
        assert!(hot.contains(legacy));
        assert!(!hot.contains(delivered));
    }

    #[test]
    fn recovery_commits_hot_swapped_segment_once_without_duplicate_manifest_entry() {
        let root =
            temp_root("recovery_commits_hot_swapped_segment_once_without_duplicate_manifest_entry");
        let channel_dir = root.join("state").join("channels");
        let receipts_file = channel_dir.join("delivery-receipts.jsonl");
        fs::create_dir_all(&channel_dir).unwrap();
        let delivered = "delivery:v2:0123456789abcdef0123456789abcdef";
        let original = receipt_line(delivered, ChannelDeliveryStatus::Delivered, 1).into_bytes();
        let plan = plan_channel_delivery_receipt_compaction(
            &original,
            &BTreeMap::from([(delivered.to_string(), delivered.to_string())]),
            &BTreeSet::from([delivered.to_string()]),
            forced_limits(10),
        )
        .unwrap();
        let transaction_id = "crash-recovery".to_string();
        let segment = ChannelDeliveryReceiptHistorySegment {
            transaction_id: transaction_id.clone(),
            file_name: format!("segment-{transaction_id}.jsonl"),
            records: plan.compacted_records,
            bytes: plan.cold_snapshot.len() as u64,
            digest: bytes_digest(&plan.cold_snapshot),
            compacted_at_ms: 2,
        };
        let history_dir = channel_delivery_receipt_history_dir(&channel_dir);
        fs::create_dir_all(&history_dir).unwrap();
        let cold_final =
            channel_delivery_receipt_history_segment_file(&channel_dir, &segment).unwrap();
        write_ledger_file(&cold_final, &plan.cold_snapshot).unwrap();
        write_ledger_file(&receipts_file, &plan.hot_snapshot).unwrap();
        let journal = ChannelDeliveryReceiptCompactionJournal {
            schema: CHANNEL_DELIVERY_RECEIPT_COMPACTION_JOURNAL_SCHEMA.to_string(),
            transaction_id,
            phase: ChannelDeliveryReceiptCompactionJournalPhase::HotSwapped,
            original_hot_bytes: plan.original_bytes,
            original_hot_digest: plan.original_digest,
            hot_snapshot_bytes: plan.hot_snapshot.len() as u64,
            hot_snapshot_digest: bytes_digest(&plan.hot_snapshot),
            cold_bytes: segment.bytes,
            cold_digest: segment.digest,
            segment: segment.clone(),
            hot_snapshot_temp_name: ".delivery-receipts.crash-recovery.compact.tmp".to_string(),
            cold_temp_name: ".crash-recovery.cold.tmp".to_string(),
            hot_backup_name: ".crash-recovery.hot-backup.tmp".to_string(),
        };
        write_journal(&channel_dir, &journal).unwrap();

        assert!(
            recover_channel_delivery_receipt_compaction_if_needed_locked(
                &channel_dir,
                &receipts_file,
                &mut Vec::new(),
            )
            .unwrap()
        );
        assert!(
            !recover_channel_delivery_receipt_compaction_if_needed_locked(
                &channel_dir,
                &receipts_file,
                &mut Vec::new(),
            )
            .unwrap()
        );
        let history = read_channel_delivery_receipt_history(&channel_dir).unwrap();
        assert_eq!(history.segments, vec![segment]);
        assert_eq!(fs::read(&receipts_file).unwrap(), plan.hot_snapshot);
        assert!(!channel_delivery_receipt_compaction_journal_file(&channel_dir).exists());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn compaction_plan_handles_one_hundred_thousand_terminal_v2_records() {
        const COUNT: usize = 100_000;
        let mut original = Vec::with_capacity(COUNT.saturating_mul(180));
        let mut canonical = BTreeMap::new();
        let mut terminal = BTreeSet::new();
        for index in 0..COUNT {
            let delivery_id = format!("delivery:v2:{index:032x}");
            original.extend_from_slice(
                receipt_line(&delivery_id, ChannelDeliveryStatus::Delivered, index as i64)
                    .as_bytes(),
            );
            canonical.insert(delivery_id.clone(), delivery_id.clone());
            terminal.insert(delivery_id);
        }

        let plan = plan_channel_delivery_receipt_compaction(
            &original,
            &canonical,
            &terminal,
            forced_limits(COUNT),
        )
        .unwrap();

        assert_eq!(plan.compacted_records, COUNT);
        assert_eq!(plan.compacted_delivery_ids, COUNT);
        assert_eq!(plan.hot_records, 0);
        assert!(plan.hot_snapshot.is_empty());
        assert_eq!(plan.cold_snapshot.len(), original.len());
    }

    fn forced_limits(max_compaction_records: usize) -> ChannelDeliveryReceiptCompactionLimits {
        ChannelDeliveryReceiptCompactionLimits {
            max_hot_bytes: u64::MAX,
            target_hot_bytes: 0,
            max_hot_records: usize::MAX,
            target_hot_records: 0,
            max_compaction_records,
            force: true,
        }
    }

    fn receipt_line(delivery_id: &str, status: ChannelDeliveryStatus, at_ms: i64) -> String {
        let receipt = ChannelDeliveryReceipt {
            schema: "agent-harness.channel-delivery-receipt.v1".to_string(),
            delivery_id: delivery_id.to_string(),
            status,
            platform: "discord".to_string(),
            account_id: None,
            channel_id: "dm-1".to_string(),
            user_id: "user-1".to_string(),
            session_key: "session-1".to_string(),
            provider_message_id: None,
            error: None,
            rendered_units: Vec::new(),
            presentation: None,
            at_ms,
        };
        format!("{}\n", serde_json::to_string(&receipt).unwrap())
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-channel-delivery-history-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
