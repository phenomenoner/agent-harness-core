//! Cursor-backed state for channel outbox delivery planning.
//!
//! The append-only outbox and delivery-receipt JSONL files remain the source of
//! truth. This index only tails stable append ranges while holding the ledger's
//! append lock; if a source is missing, truncated, or rewritten, the affected
//! table is rebuilt from that source under the same lock. SQLite transactions
//! make every index transition atomic and keep normal delivery planning to
//! bounded indexed queries instead of replaying either whole ledger.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, UNIX_EPOCH};

#[cfg(test)]
use std::cell::Cell;

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde::{Deserialize, Serialize};

use crate::channel_delivery::{
    CHANNEL_DELIVERY_RECEIPT_COMPACTION_SCHEMA, ChannelDeliveryPending, ChannelDeliveryReceipt,
    ChannelDeliveryReceiptCompactionOptions, ChannelDeliveryReceiptCompactionReport,
    ChannelDeliveryReceiptCompactionStatus, ChannelDeliveryReceiptHistoryEntry,
    ChannelDeliveryReceiptHistoryStorage, ChannelDeliveryStatus, ChannelOutboxPlanSummary,
    DEFAULT_CHANNEL_DELIVERY_RECEIPT_MAX_HOT_BYTES,
    DEFAULT_CHANNEL_DELIVERY_RECEIPT_MAX_HOT_RECORDS, delivery_id, delivery_id_for_outbox_message,
};
use crate::channel_delivery_history::{
    ChannelDeliveryReceiptCompactionLimits, ChannelDeliveryReceiptHistory,
    ChannelDeliveryReceiptHistorySegment, channel_delivery_receipt_compaction_lock_file,
    channel_delivery_receipt_compaction_retry_is_deferred,
    channel_delivery_receipt_history_manifest_file, channel_delivery_receipt_history_segment_file,
    channel_delivery_receipt_history_segment_is_valid,
    commit_channel_delivery_receipt_compaction_locked, plan_channel_delivery_receipt_compaction,
    read_channel_delivery_receipt_compaction_attempt_state, read_channel_delivery_receipt_history,
    recover_channel_delivery_receipt_compaction_if_needed_locked,
    write_channel_delivery_receipt_compaction_attempt_state,
};
use crate::channel_runtime::{
    CHANNEL_OUTBOUND_DELIVERY_ID_V2_PREFIX, ChannelOutboundMessage,
    assign_channel_outbound_delivery_id, valid_channel_outbound_delivery_id,
};
use crate::logging::{try_with_jsonl_append_lock, with_jsonl_append_lock};

pub(crate) const CHANNEL_DELIVERY_STATE_INDEX_SCHEMA: &str =
    "agent-harness.channel-delivery-state-index.v3";

const CHANNEL_DELIVERY_STATE_INDEX_FILE_NAME: &str = "delivery-state.sqlite";
const CHANNEL_DELIVERY_STATE_INDEX_REVISION: i64 = 3;
const META_SCHEMA: &str = "schema";
const META_REVISION: &str = "revision";
const META_OUTBOX_CURSOR: &str = "outbox-cursor";
const META_RECEIPT_CURSOR: &str = "receipt-cursor";
const META_OUTBOX_TOTAL_LINES: &str = "outbox-total-lines";
const META_OUTBOX_INVALID_LINES: &str = "outbox-invalid-lines";
const FINGERPRINT_BYTES: u64 = 4 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ChannelDeliveryIndexedState {
    pub(crate) delivery_id: String,
    pub(crate) line_number: usize,
    pub(crate) attempts: usize,
    pub(crate) last_status: Option<ChannelDeliveryStatus>,
    pub(crate) terminal_status: Option<ChannelDeliveryStatus>,
    pub(crate) message: ChannelOutboundMessage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ChannelDeliveryIndexedPlan {
    pub(crate) outbox_exists: bool,
    pub(crate) pending: Vec<ChannelDeliveryPending>,
    pub(crate) summary: ChannelOutboxPlanSummary,
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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct DeliveryStateRow {
    attempts: usize,
    last_status: Option<ChannelDeliveryStatus>,
    terminal_status: Option<ChannelDeliveryStatus>,
    last_partial_failure: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeliveryDisposition {
    Pending,
    Delivered,
    Failed { partial: bool },
    SkippedPermanent,
}

#[derive(Debug, Clone, Copy, Default)]
struct OutboxCounterDelta {
    total: i64,
    invalid: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ChannelOutboxIndexedAppend {
    pub(crate) delivery_id: String,
    pub(crate) outcome: ChannelOutboxIndexedAppendOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ChannelOutboxIndexedAppendOutcome {
    Appended,
    AlreadyPresent,
    AppendedIndexDeferred { warning: String },
}

#[cfg(test)]
thread_local! {
    static DEFER_NEXT_OUTBOX_INDEX_TAIL_FOR_TEST: Cell<bool> = Cell::new(false);
}

#[cfg(test)]
pub(crate) fn defer_next_outbox_index_tail_for_test() {
    DEFER_NEXT_OUTBOX_INDEX_TAIL_FOR_TEST.with(|defer| defer.set(true));
}

#[cfg(test)]
fn take_outbox_index_tail_deferral_for_test() -> bool {
    DEFER_NEXT_OUTBOX_INDEX_TAIL_FOR_TEST.with(|defer| defer.replace(false))
}

/// Returns the durable state index colocated with the channel ledgers.
pub(crate) fn channel_delivery_state_index_file(channel_dir: &Path) -> PathBuf {
    channel_dir.join(CHANNEL_DELIVERY_STATE_INDEX_FILE_NAME)
}

/// Appends one canonical outbox row while its JSONL append lock is already
/// held. The durable index is first brought current and queried by the opaque
/// v2 identity, so a retry can return `AlreadyPresent` without scanning the
/// JSONL ledger or appending a duplicate physical row.
pub(crate) fn append_channel_outbox_message_indexed_locked(
    channel_dir: &Path,
    outbox_file: &Path,
    message: &mut ChannelOutboundMessage,
) -> io::Result<ChannelOutboxIndexedAppend> {
    let mut connection = open_channel_delivery_state_index(channel_dir)?;
    let mut warnings = Vec::new();
    refresh_outbox_index_locked(&mut connection, outbox_file, &mut warnings)?;

    let delivery_id = match valid_channel_outbound_delivery_id(message) {
        Some(delivery_id) => delivery_id.to_string(),
        None => assign_channel_outbound_delivery_id(message)?.to_string(),
    };
    let message_json = serde_json::to_string(message).map_err(io::Error::other)?;
    if let Some(existing_message_json) =
        indexed_outbox_message_json_by_delivery_id(&connection, &delivery_id)?
    {
        if existing_message_json == message_json {
            return Ok(ChannelOutboxIndexedAppend {
                delivery_id,
                outcome: ChannelOutboxIndexedAppendOutcome::AlreadyPresent,
            });
        }
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "refusing to append a different channel outbox message with existing delivery ID {delivery_id}"
            ),
        ));
    }

    crate::channel_delivery::append_channel_outbox_json_line_locked(outbox_file, message)?;

    #[cfg(test)]
    if take_outbox_index_tail_deferral_for_test() {
        return Ok(ChannelOutboxIndexedAppend {
            delivery_id,
            outcome: ChannelOutboxIndexedAppendOutcome::AppendedIndexDeferred {
                warning: deferred_outbox_index_warning(
                    outbox_file,
                    "test-only deferred index tail after source append",
                ),
            },
        });
    }

    let outcome = match refresh_outbox_index_locked(&mut connection, outbox_file, &mut warnings) {
        Ok(()) => match indexed_outbox_message_json_by_delivery_id(&connection, &delivery_id) {
            Ok(Some(_)) => ChannelOutboxIndexedAppendOutcome::Appended,
            Ok(None) => ChannelOutboxIndexedAppendOutcome::AppendedIndexDeferred {
                warning: deferred_outbox_index_warning(
                    outbox_file,
                    "the append completed but the indexed row is not yet visible",
                ),
            },
            Err(error) => ChannelOutboxIndexedAppendOutcome::AppendedIndexDeferred {
                warning: deferred_outbox_index_warning(outbox_file, &error.to_string()),
            },
        },
        Err(error) => ChannelOutboxIndexedAppendOutcome::AppendedIndexDeferred {
            warning: deferred_outbox_index_warning(outbox_file, &error.to_string()),
        },
    };
    Ok(ChannelOutboxIndexedAppend {
        delivery_id,
        outcome,
    })
}

fn indexed_outbox_message_json_by_delivery_id(
    connection: &Connection,
    delivery_id: &str,
) -> io::Result<Option<String>> {
    connection
        .query_row(
            "SELECT message_json FROM channel_delivery_outbox_lines \
             WHERE delivery_id = ?1 AND parse_error IS NULL",
            params![delivery_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(io::Error::other)
}

fn deferred_outbox_index_warning(outbox_file: &Path, detail: &str) -> String {
    const DETAIL_LIMIT: usize = 240;
    let mut bounded = detail.chars().take(DETAIL_LIMIT).collect::<String>();
    if detail.chars().nth(DETAIL_LIMIT).is_some() {
        bounded.push('…');
    }
    format!(
        "channel outbox source append succeeded at {}; delivery state index tail is deferred: {bounded}",
        outbox_file.display()
    )
}

/// Tails both channel ledgers into the persistent index. Source JSONL is read
/// in full only when the persisted cursor is absent or cannot safely be
/// reconciled with the source ledger.
pub(crate) fn refresh_channel_delivery_index(
    channel_dir: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<()> {
    fs::create_dir_all(channel_dir)?;
    let outbox_file = channel_dir.join("outbox.jsonl");
    let receipts_file = channel_dir.join("delivery-receipts.jsonl");
    let mut connection = open_channel_delivery_state_index(channel_dir)?;

    with_jsonl_append_lock(&outbox_file, || {
        refresh_outbox_index_locked(&mut connection, &outbox_file, warnings)
    })?;
    with_jsonl_append_lock(&receipts_file, || {
        refresh_receipt_index_locked(&mut connection, channel_dir, &receipts_file, warnings)
    })?;
    Ok(())
}

/// Non-blocking refresh for user-visible delivery planning. If a source writer
/// owns an append lock, the last SQLite transaction remains a coherent,
/// conservative snapshot; the next wake tails the append after that writer
/// releases its lock instead of delaying progress or final delivery for up to
/// the generic append-lock timeout.
pub(crate) fn refresh_channel_delivery_index_nonblocking(
    channel_dir: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<()> {
    fs::create_dir_all(channel_dir)?;
    let outbox_file = channel_dir.join("outbox.jsonl");
    let receipts_file = channel_dir.join("delivery-receipts.jsonl");
    let mut connection = open_channel_delivery_state_index(channel_dir)?;

    if try_with_jsonl_append_lock(&outbox_file, || {
        refresh_outbox_index_locked(&mut connection, &outbox_file, warnings)
    })?
    .is_none()
    {
        warnings.push(format!(
            "channel outbox state refresh is waiting for a live append at {}; using last committed delivery state",
            outbox_file.display()
        ));
    }
    if try_with_jsonl_append_lock(&receipts_file, || {
        refresh_receipt_index_locked(&mut connection, channel_dir, &receipts_file, warnings)
    })?
    .is_none()
    {
        warnings.push(format!(
            "channel delivery receipt state refresh is waiting for a live append at {}; using last committed delivery state",
            receipts_file.display()
        ));
    }
    Ok(())
}

/// Builds an outbox plan from the persistent state index. The report's
/// aggregate counts come from per-platform materialized counters, while the
/// pending detail query is ordered and capped by `limit`.
pub(crate) fn plan_channel_outbox_index(
    channel_dir: &Path,
    platform: Option<&str>,
    limit: usize,
    warnings: &mut Vec<String>,
) -> io::Result<ChannelDeliveryIndexedPlan> {
    refresh_channel_delivery_index_nonblocking(channel_dir, warnings)?;
    let connection = open_channel_delivery_state_index(channel_dir)?;
    append_index_parse_warnings(&connection, warnings)?;

    let total_outbox_lines = read_meta_i64(&connection, META_OUTBOX_TOTAL_LINES)?.max(0);
    let invalid_lines = read_meta_i64(&connection, META_OUTBOX_INVALID_LINES)?.max(0);
    let selected = read_platform_summary(&connection, platform)?;
    let valid_lines = total_outbox_lines.saturating_sub(invalid_lines);
    let skipped_platform = platform
        .map(|_| valid_lines.saturating_sub(selected.valid_outbox_count))
        .unwrap_or(0);
    let summary = ChannelOutboxPlanSummary {
        total_outbox_lines: usize_from_i64(total_outbox_lines, "outbox total line count")?,
        sampled: false,
        sampled_bytes: 0,
        pending: usize_from_i64(selected.pending, "pending delivery count")?,
        delivered: usize_from_i64(selected.delivered, "delivered count")?,
        failed_retryable: usize_from_i64(selected.failed_retryable, "failed delivery count")?,
        skipped_permanent: usize_from_i64(
            selected.skipped_permanent,
            "permanently skipped delivery count",
        )?,
        partial_failed: usize_from_i64(selected.partial_failed, "partial failed delivery count")?,
        skipped_platform: usize_from_i64(skipped_platform, "skipped platform count")?,
        invalid_lines: usize_from_i64(invalid_lines, "invalid outbox line count")?,
    };
    let pending = read_pending_deliveries(&connection, platform, limit)?;

    Ok(ChannelDeliveryIndexedPlan {
        outbox_exists: channel_dir.join("outbox.jsonl").is_file(),
        pending,
        summary,
    })
}

/// Appends a source-of-truth delivery receipt and advances the matching state
/// cursor under one append lock. If the process stops after the source append,
/// the next normal refresh safely tails the missing record before using it.
pub(crate) fn record_channel_delivery_receipt(
    channel_dir: &Path,
    receipt: &ChannelDeliveryReceipt,
    warnings: &mut Vec<String>,
) -> io::Result<bool> {
    fs::create_dir_all(channel_dir)?;
    let receipts_file = channel_dir.join("delivery-receipts.jsonl");
    with_jsonl_append_lock(&receipts_file, || {
        let mut connection = open_channel_delivery_state_index(channel_dir)?;
        refresh_receipt_index_locked(&mut connection, channel_dir, &receipts_file, warnings)?;
        let canonical_delivery_id =
            canonical_delivery_id_for_connection(&connection, &receipt.delivery_id)?;
        let superseded_by_terminal = receipt.status == ChannelDeliveryStatus::Failed
            && lookup_delivery_state(&connection, &canonical_delivery_id)?
                .is_some_and(|state| state.terminal_status.is_some());
        append_json_line_while_locked(&receipts_file, receipt)?;
        if let Err(error) =
            refresh_receipt_index_locked(&mut connection, channel_dir, &receipts_file, warnings)
        {
            // The source receipt has already been appended while its lock was
            // held. Returning that append as a failure would invite a caller
            // retry to duplicate a provider-visible audit record. Leave the
            // cursor behind instead: the next refresh detects and tails this
            // exact append before it plans any delivery work.
            warnings.push(format!(
                "delivery receipt was appended but its state index update is deferred: {error}"
            ));
        }
        Ok(superseded_by_terminal)
    })
}

/// Performs one bounded, journaled hot-to-cold compaction batch.  The normal
/// outbox planner never calls this path; it remains entirely index-backed and
/// tails only ordinary append ranges.  Maintenance owns both ledger locks so
/// a v2 receipt history is never split across a concurrent delivery append.
pub(crate) fn compact_channel_delivery_receipts_indexed(
    options: ChannelDeliveryReceiptCompactionOptions,
) -> io::Result<ChannelDeliveryReceiptCompactionReport> {
    if options.max_hot_receipt_bytes == 0
        || options.max_hot_receipt_records == 0
        || options.max_compaction_records == 0
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "channel delivery receipt compaction bounds must all be greater than zero",
        ));
    }
    let channel_dir = options.harness_home.join("state").join("channels");
    let outbox_file = channel_dir.join("outbox.jsonl");
    let receipts_file = channel_dir.join("delivery-receipts.jsonl");
    let mut report = ChannelDeliveryReceiptCompactionReport {
        schema: CHANNEL_DELIVERY_RECEIPT_COMPACTION_SCHEMA,
        harness_home: options.harness_home.clone(),
        receipts_file: receipts_file.clone(),
        history_manifest_file: channel_delivery_receipt_history_manifest_file(&channel_dir),
        status: ChannelDeliveryReceiptCompactionStatus::Missing,
        hot_before_bytes: 0,
        hot_after_bytes: 0,
        hot_before_records: 0,
        hot_after_records: 0,
        compacted_records: 0,
        compacted_delivery_ids: 0,
        cold_segment_file: None,
        warnings: Vec::new(),
    };
    fs::create_dir_all(&channel_dir)?;
    let transaction_lock = channel_delivery_receipt_compaction_lock_file(&channel_dir);
    let acquired = try_with_jsonl_append_lock(&transaction_lock, || {
        with_jsonl_append_lock(&outbox_file, || {
            with_jsonl_append_lock(&receipts_file, || {
                let mut connection = open_channel_delivery_state_index(&channel_dir)?;
                let _ = recover_channel_delivery_receipt_compaction_if_needed_locked(
                    &channel_dir,
                    &receipts_file,
                    &mut report.warnings,
                )?;
                refresh_outbox_index_locked(&mut connection, &outbox_file, &mut report.warnings)?;
                refresh_receipt_index_locked(
                    &mut connection,
                    &channel_dir,
                    &receipts_file,
                    &mut report.warnings,
                )?;

                let original = match fs::read(&receipts_file) {
                    Ok(bytes) => bytes,
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {
                        report.status = ChannelDeliveryReceiptCompactionStatus::Missing;
                        return Ok(());
                    }
                    Err(error) => return Err(error),
                };
                let (canonical_delivery_ids, terminal_v2_delivery_ids) =
                    compactable_terminal_v2_delivery_ids(&connection)?;
                let plan = plan_channel_delivery_receipt_compaction(
                    &original,
                    &canonical_delivery_ids,
                    &terminal_v2_delivery_ids,
                    ChannelDeliveryReceiptCompactionLimits {
                        max_hot_bytes: options.max_hot_receipt_bytes,
                        target_hot_bytes: options.target_hot_receipt_bytes,
                        max_hot_records: options.max_hot_receipt_records,
                        target_hot_records: options.target_hot_receipt_records,
                        max_compaction_records: options.max_compaction_records,
                        force: options.force,
                    },
                )?;
                report.hot_before_bytes = plan.original_bytes;
                report.hot_before_records = plan.original_records;
                report.hot_after_bytes = plan.hot_snapshot.len() as u64;
                report.hot_after_records = plan.hot_records;
                report.compacted_records = plan.compacted_records;
                report.compacted_delivery_ids = plan.compacted_delivery_ids;
                if !plan.changed() {
                    report.status = compaction_status_for_hot_bounds(
                        report.hot_after_bytes,
                        report.hot_after_records,
                        &options,
                    );
                    if report.status == ChannelDeliveryReceiptCompactionStatus::Backpressure {
                        report.warnings.push(format!(
                            "channel delivery receipt hot ledger remains above its configured bound ({} bytes, {} records) because no complete delivered canonical-v2 history can be moved safely",
                            report.hot_after_bytes, report.hot_after_records
                        ));
                    }
                    return Ok(());
                }

                let committed = commit_channel_delivery_receipt_compaction_locked(
                    &channel_dir,
                    &receipts_file,
                    &plan,
                    options.now_ms,
                )?
                .expect("a changed channel delivery receipt compaction plan must commit a segment");
                report.cold_segment_file = Some(channel_delivery_receipt_history_segment_file(
                    &channel_dir,
                    &committed.segment,
                )?);
                report.hot_after_bytes = committed.hot_bytes;
                // The hot source was replaced, so rebuild once from immutable
                // cold segments plus the compact hot ledger. This is a rare
                // maintenance operation, not a normal planner scan.
                rebuild_receipt_index_locked(
                    &mut connection,
                    &channel_dir,
                    &receipts_file,
                    &mut report.warnings,
                )?;
                report.status = compaction_status_for_hot_bounds(
                    report.hot_after_bytes,
                    report.hot_after_records,
                    &options,
                );
                if report.status == ChannelDeliveryReceiptCompactionStatus::Unchanged {
                    report.status = ChannelDeliveryReceiptCompactionStatus::Compacted;
                }
                if report.status == ChannelDeliveryReceiptCompactionStatus::Backpressure {
                    report.warnings.push(format!(
                        "channel delivery receipt compaction moved {} record(s), but the remaining hot ledger still exceeds its configured bound without a safe additional whole-delivery move",
                        report.compacted_records
                    ));
                }
                Ok(())
            })
        })
    })?;
    if acquired.is_none() {
        report.status = ChannelDeliveryReceiptCompactionStatus::Busy;
    }
    Ok(report)
}

/// Cheap post-terminal maintenance gate.  It consults only hot-file metadata
/// and the already-maintained cursor before deciding whether a bounded
/// compaction attempt is warranted.  A backpressure result is throttled until
/// either the retry interval elapses or the hot ledger grows materially, so an
/// all-legacy/all-active ledger cannot cause a full 100k-row reread on every
/// terminal delivery.
pub(crate) fn maybe_compact_channel_delivery_receipts_after_terminal(
    harness_home: PathBuf,
    now_ms: i64,
) -> io::Result<Option<ChannelDeliveryReceiptCompactionReport>> {
    let channel_dir = harness_home.join("state").join("channels");
    let receipts_file = channel_dir.join("delivery-receipts.jsonl");
    let metadata = match fs::metadata(&receipts_file) {
        Ok(metadata) if metadata.is_file() => metadata,
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "channel delivery receipt path is not a file: {}",
                    receipts_file.display()
                ),
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let connection = open_channel_delivery_state_index(&channel_dir)?;
    let hot_records = read_cursor(&connection, META_RECEIPT_CURSOR)?
        .map(|cursor| cursor.line_number)
        .unwrap_or_default();
    if metadata.len() <= DEFAULT_CHANNEL_DELIVERY_RECEIPT_MAX_HOT_BYTES
        && hot_records <= DEFAULT_CHANNEL_DELIVERY_RECEIPT_MAX_HOT_RECORDS
    {
        return Ok(None);
    }
    let previous = read_channel_delivery_receipt_compaction_attempt_state(&channel_dir)?;
    if channel_delivery_receipt_compaction_retry_is_deferred(
        now_ms,
        metadata.len(),
        previous.as_ref(),
    ) {
        return Ok(None);
    }
    let report = compact_channel_delivery_receipts_indexed(
        ChannelDeliveryReceiptCompactionOptions::with_defaults(harness_home, now_ms),
    )?;
    if report.status != ChannelDeliveryReceiptCompactionStatus::Busy {
        let observed_bytes = fs::metadata(&receipts_file)
            .map(|metadata| metadata.len())
            .unwrap_or(report.hot_after_bytes);
        write_channel_delivery_receipt_compaction_attempt_state(
            &channel_dir,
            now_ms,
            observed_bytes,
        )?;
    }
    Ok(Some(report))
}

fn compaction_status_for_hot_bounds(
    hot_bytes: u64,
    hot_records: usize,
    options: &ChannelDeliveryReceiptCompactionOptions,
) -> ChannelDeliveryReceiptCompactionStatus {
    if hot_bytes > options.max_hot_receipt_bytes || hot_records > options.max_hot_receipt_records {
        ChannelDeliveryReceiptCompactionStatus::Backpressure
    } else {
        ChannelDeliveryReceiptCompactionStatus::Unchanged
    }
}

/// Builds the alias map used by receipt compaction and returns only delivered,
/// validated v2 IDs as move candidates.  `SkippedPermanent`, retryable
/// failures, unknown receipts, and legacy line/bytes identities remain hot by
/// design; none can be discarded or reclassified by this maintenance path.
fn compactable_terminal_v2_delivery_ids(
    connection: &Connection,
) -> io::Result<(BTreeMap<String, String>, BTreeSet<String>)> {
    let mut statement = connection
        .prepare(
            "SELECT outbox.delivery_id, outbox.legacy_delivery_id, outbox.message_json, \
                    state.terminal_status \
             FROM channel_delivery_outbox_lines AS outbox \
             LEFT JOIN channel_delivery_receipt_state AS state \
                 ON state.delivery_id = outbox.delivery_id \
             WHERE outbox.parse_error IS NULL",
        )
        .map_err(io::Error::other)?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })
        .map_err(io::Error::other)?;
    let mut canonical_delivery_ids = BTreeMap::new();
    let mut terminal_v2_delivery_ids = BTreeSet::new();
    for row in rows {
        let (delivery_id, legacy_delivery_id, message_json, terminal_status) =
            row.map_err(io::Error::other)?;
        let message: ChannelOutboundMessage =
            serde_json::from_str(&message_json).map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("indexed canonical channel outbox message is invalid: {error}"),
                )
            })?;
        let Some(valid_v2_id) = valid_channel_outbound_delivery_id(&message) else {
            continue;
        };
        if valid_v2_id != delivery_id
            || !delivery_id.starts_with(CHANNEL_OUTBOUND_DELIVERY_ID_V2_PREFIX)
        {
            continue;
        }
        canonical_delivery_ids.insert(delivery_id.clone(), delivery_id.clone());
        canonical_delivery_ids.insert(legacy_delivery_id, delivery_id.clone());
        if terminal_status.as_deref() == Some("delivered") {
            terminal_v2_delivery_ids.insert(delivery_id);
        }
    }
    Ok((canonical_delivery_ids, terminal_v2_delivery_ids))
}

/// Reads exact hot-and-cold receipt evidence from the sidecar.  The query is
/// deliberately bounded and does not reopen historical JSONL segments after
/// their one-time indexed replay.
pub(crate) fn query_channel_delivery_receipt_history_indexed(
    channel_dir: &Path,
    delivery_id: Option<&str>,
    source_queue_id: Option<&str>,
    limit: usize,
    warnings: &mut Vec<String>,
) -> io::Result<Vec<ChannelDeliveryReceiptHistoryEntry>> {
    refresh_channel_delivery_index_nonblocking(channel_dir, warnings)?;
    let connection = open_channel_delivery_state_index(channel_dir)?;
    let limit = i64_from_usize(limit, "channel delivery receipt history query limit")?;
    let mut entries = Vec::new();
    let (sql, values) = match (delivery_id, source_queue_id) {
        (Some(delivery_id), None) => {
            let canonical = canonical_delivery_id_for_connection(&connection, delivery_id)?;
            (
                "SELECT records.delivery_id, records.source_kind, records.line_number, \
                        records.receipt_json \
                 FROM channel_delivery_receipt_records AS records \
                 WHERE records.delivery_id = ?1 \
                 ORDER BY records.at_ms, records.source_kind, records.source_id, records.line_number \
                 LIMIT ?2",
                vec![canonical, limit.to_string()],
            )
        }
        (None, Some(source_queue_id)) => (
            "SELECT records.delivery_id, records.source_kind, records.line_number, \
                    records.receipt_json \
             FROM channel_delivery_receipt_records AS records \
             INNER JOIN channel_delivery_outbox_lines AS outbox \
                 ON outbox.delivery_id = records.delivery_id \
             WHERE outbox.parse_error IS NULL AND outbox.source_queue_id = ?1 \
             ORDER BY outbox.line_number, records.at_ms, records.source_kind, records.source_id, records.line_number \
             LIMIT ?2",
            vec![source_queue_id.to_string(), limit.to_string()],
        ),
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "channel delivery receipt history index requires exactly one exact query key",
            ));
        }
    };
    let mut statement = connection.prepare(sql).map_err(io::Error::other)?;
    let rows = statement
        .query_map(params![&values[0], &values[1]], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
            ))
        })
        .map_err(io::Error::other)?;
    for row in rows {
        let (delivery_id, source_kind, line_number, receipt_json) =
            row.map_err(io::Error::other)?;
        let storage = match source_kind.as_str() {
            "hot" => ChannelDeliveryReceiptHistoryStorage::Hot,
            "cold" => ChannelDeliveryReceiptHistoryStorage::Cold,
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("channel delivery receipt index has unsupported storage `{other}`"),
                ));
            }
        };
        entries.push(ChannelDeliveryReceiptHistoryEntry {
            delivery_id,
            storage,
            line_number: usize_from_i64(line_number, "delivery receipt history line number")?,
            receipt: serde_json::from_str(&receipt_json).map_err(io::Error::other)?,
        });
    }
    Ok(entries)
}

/// Source-authoritative exact lookup for one source queue.
///
/// Final-outbox idempotency must use this variant before it decides whether a
/// new provider-visible message needs an identity. It waits for the ordinary
/// JSONL append locks, brings the cursor-backed index current, and then uses
/// the indexed `source_queue_id` key only. This deliberately does not fall
/// back to replaying the JSONL source or returning a nonblocking snapshot.
pub(crate) fn channel_delivery_states_for_source_queue_blocking(
    channel_dir: &Path,
    source_queue_id: &str,
    warnings: &mut Vec<String>,
) -> io::Result<Vec<ChannelDeliveryIndexedState>> {
    refresh_channel_delivery_index(channel_dir, warnings)?;
    let connection = open_channel_delivery_state_index(channel_dir)?;
    read_indexed_states(
        &connection,
        "WHERE outbox.source_queue_id = ?1 ORDER BY outbox.line_number",
        params![source_queue_id],
    )
}

/// Bounded indexed lookup for a source queue in one physical provider lane.
/// Unlike the exact-session helper below, this deliberately includes the root
/// virtual session and all of its continuation child sessions.
pub(crate) fn channel_delivery_states_for_source_queue_in_channel(
    channel_dir: &Path,
    source_queue_id: &str,
    platform: &str,
    account_id: Option<&str>,
    channel_id: &str,
    user_id: &str,
    warnings: &mut Vec<String>,
) -> io::Result<Vec<ChannelDeliveryIndexedState>> {
    refresh_channel_delivery_index_nonblocking(channel_dir, warnings)?;
    let connection = open_channel_delivery_state_index(channel_dir)?;
    read_indexed_states(
        &connection,
        "WHERE outbox.source_queue_id = ?1 \
         AND outbox.platform = ?2 \
         AND outbox.account_id IS ?3 \
         AND outbox.channel_id = ?4 \
         AND outbox.user_id = ?5 \
         ORDER BY outbox.line_number",
        params![source_queue_id, platform, account_id, channel_id, user_id],
    )
}

/// Bounded indexed lookup for deliveries from one source queue inside one
/// exact virtual-session lane. This is the preferred correlation API for
/// final-surface ordering and virtual evidence readers.
pub(crate) fn channel_delivery_states_for_source_queue_in_lane(
    channel_dir: &Path,
    source_queue_id: &str,
    platform: &str,
    account_id: Option<&str>,
    channel_id: &str,
    user_id: &str,
    session_key: &str,
    warnings: &mut Vec<String>,
) -> io::Result<Vec<ChannelDeliveryIndexedState>> {
    refresh_channel_delivery_index_nonblocking(channel_dir, warnings)?;
    let connection = open_channel_delivery_state_index(channel_dir)?;
    read_indexed_states(
        &connection,
        "WHERE outbox.source_queue_id = ?1 \
         AND outbox.platform = ?2 \
         AND outbox.account_id IS ?3 \
         AND outbox.channel_id = ?4 \
         AND outbox.user_id = ?5 \
         AND outbox.session_key = ?6 \
         ORDER BY outbox.line_number",
        params![
            source_queue_id,
            platform,
            account_id,
            channel_id,
            user_id,
            session_key
        ],
    )
}

fn open_channel_delivery_state_index(channel_dir: &Path) -> io::Result<Connection> {
    fs::create_dir_all(channel_dir)?;
    let mut connection = Connection::open(channel_delivery_state_index_file(channel_dir))
        .map_err(io::Error::other)?;
    connection
        .busy_timeout(Duration::from_secs(10))
        .map_err(io::Error::other)?;
    initialize_channel_delivery_state_index(&mut connection)?;
    Ok(connection)
}

fn initialize_channel_delivery_state_index(connection: &mut Connection) -> io::Result<()> {
    connection
        .execute_batch(
            "
            CREATE TABLE IF NOT EXISTS channel_delivery_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            ",
        )
        .map_err(io::Error::other)?;

    let schema = read_meta(connection, META_SCHEMA)?;
    let revision = read_meta(connection, META_REVISION)?;
    if schema.as_deref() == Some(CHANNEL_DELIVERY_STATE_INDEX_SCHEMA)
        && revision.as_deref() == Some(&CHANNEL_DELIVERY_STATE_INDEX_REVISION.to_string())
    {
        return Ok(());
    }

    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(io::Error::other)?;
    transaction
        .execute_batch(
            "
            DROP TABLE IF EXISTS channel_delivery_outbox_lines;
            DROP TABLE IF EXISTS channel_delivery_receipt_state;
            DROP TABLE IF EXISTS channel_delivery_receipt_errors;
            DROP TABLE IF EXISTS channel_delivery_receipt_records;
            DROP TABLE IF EXISTS channel_delivery_receipt_history_segments;
            DROP TABLE IF EXISTS channel_delivery_platform_summary;

            CREATE TABLE channel_delivery_outbox_lines (
                line_number INTEGER PRIMARY KEY,
                delivery_id TEXT UNIQUE,
                legacy_delivery_id TEXT UNIQUE,
                platform TEXT,
                account_id TEXT,
                channel_id TEXT,
                user_id TEXT,
                session_key TEXT,
                source_queue_id TEXT,
                message_json TEXT,
                parse_error TEXT
            );
            CREATE INDEX channel_delivery_outbox_platform_line_idx
                ON channel_delivery_outbox_lines(platform, line_number)
                WHERE parse_error IS NULL;
            CREATE INDEX channel_delivery_outbox_source_queue_idx
                ON channel_delivery_outbox_lines(source_queue_id, line_number)
                WHERE parse_error IS NULL;
            CREATE INDEX channel_delivery_outbox_channel_identity_idx
                ON channel_delivery_outbox_lines(platform, account_id, channel_id, user_id, line_number)
                WHERE parse_error IS NULL;
            CREATE TABLE channel_delivery_receipt_state (
                delivery_id TEXT PRIMARY KEY,
                attempts INTEGER NOT NULL,
                last_status TEXT,
                terminal_status TEXT,
                last_partial_failure INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX channel_delivery_receipt_terminal_idx
                ON channel_delivery_receipt_state(terminal_status);
            CREATE TABLE channel_delivery_receipt_errors (
                source_kind TEXT NOT NULL,
                source_id TEXT NOT NULL,
                line_number INTEGER NOT NULL,
                parse_error TEXT NOT NULL,
                PRIMARY KEY (source_kind, source_id, line_number)
            );
            CREATE TABLE channel_delivery_receipt_records (
                source_kind TEXT NOT NULL,
                source_id TEXT NOT NULL,
                line_number INTEGER NOT NULL,
                delivery_id TEXT NOT NULL,
                receipt_json TEXT NOT NULL,
                at_ms INTEGER NOT NULL,
                PRIMARY KEY (source_kind, source_id, line_number)
            );
            CREATE INDEX channel_delivery_receipt_records_delivery_idx
                ON channel_delivery_receipt_records(delivery_id, at_ms, source_kind, source_id, line_number);
            CREATE TABLE channel_delivery_receipt_history_segments (
                transaction_id TEXT PRIMARY KEY,
                file_name TEXT NOT NULL,
                records INTEGER NOT NULL,
                bytes INTEGER NOT NULL,
                digest TEXT NOT NULL,
                compacted_at_ms INTEGER NOT NULL
            );
            CREATE TABLE channel_delivery_platform_summary (
                platform TEXT PRIMARY KEY,
                valid_outbox_count INTEGER NOT NULL DEFAULT 0,
                pending INTEGER NOT NULL DEFAULT 0,
                delivered INTEGER NOT NULL DEFAULT 0,
                failed_retryable INTEGER NOT NULL DEFAULT 0,
                skipped_permanent INTEGER NOT NULL DEFAULT 0,
                partial_failed INTEGER NOT NULL DEFAULT 0
            );
            ",
        )
        .map_err(io::Error::other)?;
    transaction
        .execute("DELETE FROM channel_delivery_meta", [])
        .map_err(io::Error::other)?;
    write_meta(
        &transaction,
        META_SCHEMA,
        CHANNEL_DELIVERY_STATE_INDEX_SCHEMA,
    )?;
    write_meta(
        &transaction,
        META_REVISION,
        &CHANNEL_DELIVERY_STATE_INDEX_REVISION.to_string(),
    )?;
    write_meta(&transaction, META_OUTBOX_TOTAL_LINES, "0")?;
    write_meta(&transaction, META_OUTBOX_INVALID_LINES, "0")?;
    transaction.commit().map_err(io::Error::other)
}

fn refresh_outbox_index_locked(
    connection: &mut Connection,
    outbox_file: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<()> {
    let cursor = read_cursor(connection, META_OUTBOX_CURSOR)?;
    let metadata = match fs::metadata(outbox_file) {
        Ok(metadata) if metadata.is_file() => metadata,
        Ok(_) => {
            warnings.push(format!(
                "channel outbox path is not a file; rebuilding delivery state index: {}",
                outbox_file.display()
            ));
            reset_outbox_index(connection)?;
            return Ok(());
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if cursor.is_some() || outbox_index_has_rows(connection)? {
                warnings.push(
                    "channel outbox disappeared; rebuilding delivery state index".to_string(),
                );
                reset_outbox_index(connection)?;
            }
            return Ok(());
        }
        Err(error) => return Err(error),
    };

    let refresh_mode = outbox_refresh_mode(outbox_file, &metadata, cursor.as_ref())?;
    match refresh_mode {
        LedgerRefreshMode::Current => Ok(()),
        LedgerRefreshMode::Append(cursor) => {
            let total_before = read_meta_i64(connection, META_OUTBOX_TOTAL_LINES)?;
            let invalid_before = read_meta_i64(connection, META_OUTBOX_INVALID_LINES)?;
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(io::Error::other)?;
            let (next_cursor, delta) =
                read_outbox_from_cursor(&transaction, outbox_file, cursor, warnings, "refreshing")?;
            write_meta(
                &transaction,
                META_OUTBOX_TOTAL_LINES,
                &total_before.saturating_add(delta.total).to_string(),
            )?;
            write_meta(
                &transaction,
                META_OUTBOX_INVALID_LINES,
                &invalid_before.saturating_add(delta.invalid).to_string(),
            )?;
            write_cursor(&transaction, META_OUTBOX_CURSOR, &next_cursor)?;
            transaction.commit().map_err(io::Error::other)
        }
        LedgerRefreshMode::Rebuild(reason) => {
            warnings.push(format!(
                "{reason}; rebuilding channel delivery outbox index"
            ));
            rebuild_outbox_index_locked(connection, outbox_file, warnings)
        }
    }
}

fn refresh_receipt_index_locked(
    connection: &mut Connection,
    channel_dir: &Path,
    receipts_file: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<()> {
    let recovered = recover_channel_delivery_receipt_compaction_if_needed_locked(
        channel_dir,
        receipts_file,
        warnings,
    )?;
    let history = read_channel_delivery_receipt_history(channel_dir)?;
    let cold_refresh = cold_history_refresh_mode(connection, &history)?;
    let cursor = read_cursor(connection, META_RECEIPT_CURSOR)?;
    let metadata = match fs::metadata(receipts_file) {
        Ok(metadata) if metadata.is_file() => metadata,
        Ok(_) => {
            warnings.push(format!(
                "channel delivery receipt path is not a file; rebuilding delivery state index: {}",
                receipts_file.display()
            ));
            return rebuild_receipt_index_locked(connection, channel_dir, receipts_file, warnings);
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if cursor.is_some()
                || receipt_index_has_rows(connection)?
                || !history.segments.is_empty()
            {
                warnings.push(
                    "channel delivery receipt ledger disappeared; rebuilding cold and hot delivery state index"
                        .to_string(),
                );
                return rebuild_receipt_index_locked(
                    connection,
                    channel_dir,
                    receipts_file,
                    warnings,
                );
            }
            return Ok(());
        }
        Err(error) => return Err(error),
    };

    let refresh_mode = receipt_refresh_mode(receipts_file, &metadata, cursor.as_ref())?;
    if recovered || matches!(&refresh_mode, LedgerRefreshMode::Rebuild(_)) {
        let reason = match &refresh_mode {
            LedgerRefreshMode::Rebuild(reason) => reason.clone(),
            _ => "channel delivery receipt compaction changed the hot ledger".to_string(),
        };
        warnings.push(format!(
            "{reason}; rebuilding channel delivery receipt index from cold and hot ledgers"
        ));
        return rebuild_receipt_index_locked(connection, channel_dir, receipts_file, warnings);
    }
    if let ColdHistoryRefreshMode::Rebuild(reason) = &cold_refresh {
        warnings.push(format!(
            "{reason}; rebuilding channel delivery receipt index from cold and hot ledgers"
        ));
        return rebuild_receipt_index_locked(connection, channel_dir, receipts_file, warnings);
    }

    match refresh_mode {
        LedgerRefreshMode::Current => {
            if let ColdHistoryRefreshMode::Append(segments) = &cold_refresh {
                append_cold_history_segments(connection, channel_dir, segments, warnings)?;
            }
            Ok(())
        }
        LedgerRefreshMode::Append(cursor) => {
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(io::Error::other)?;
            let next_cursor = read_receipts_from_cursor(
                &transaction,
                receipts_file,
                cursor,
                "hot",
                "delivery-receipts",
                warnings,
                "refreshing",
            )?;
            if let ColdHistoryRefreshMode::Append(segments) = &cold_refresh {
                append_cold_history_segments_transaction(
                    &transaction,
                    channel_dir,
                    segments,
                    warnings,
                )?;
            }
            write_cursor(&transaction, META_RECEIPT_CURSOR, &next_cursor)?;
            transaction.commit().map_err(io::Error::other)
        }
        LedgerRefreshMode::Rebuild(_) => unreachable!("rebuild handled before transaction"),
    }
}

fn rebuild_outbox_index_locked(
    connection: &mut Connection,
    outbox_file: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<()> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(io::Error::other)?;
    transaction
        .execute("DELETE FROM channel_delivery_outbox_lines", [])
        .map_err(io::Error::other)?;
    transaction
        .execute("DELETE FROM channel_delivery_platform_summary", [])
        .map_err(io::Error::other)?;
    transaction
        .execute("DELETE FROM channel_delivery_receipt_state", [])
        .map_err(io::Error::other)?;
    transaction
        .execute("DELETE FROM channel_delivery_receipt_errors", [])
        .map_err(io::Error::other)?;
    transaction
        .execute("DELETE FROM channel_delivery_receipt_records", [])
        .map_err(io::Error::other)?;
    transaction
        .execute("DELETE FROM channel_delivery_receipt_history_segments", [])
        .map_err(io::Error::other)?;
    delete_meta(&transaction, META_RECEIPT_CURSOR)?;
    write_meta(&transaction, META_OUTBOX_TOTAL_LINES, "0")?;
    write_meta(&transaction, META_OUTBOX_INVALID_LINES, "0")?;

    let (cursor, delta) = read_outbox_from_cursor(
        &transaction,
        outbox_file,
        LedgerCursor::default(),
        warnings,
        "rebuilding",
    )?;
    write_meta(
        &transaction,
        META_OUTBOX_TOTAL_LINES,
        &delta.total.to_string(),
    )?;
    write_meta(
        &transaction,
        META_OUTBOX_INVALID_LINES,
        &delta.invalid.to_string(),
    )?;
    write_cursor(&transaction, META_OUTBOX_CURSOR, &cursor)?;
    transaction.commit().map_err(io::Error::other)
}

fn rebuild_receipt_index_locked(
    connection: &mut Connection,
    channel_dir: &Path,
    receipts_file: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<()> {
    let history = read_channel_delivery_receipt_history(channel_dir)?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(io::Error::other)?;
    transaction
        .execute("DELETE FROM channel_delivery_receipt_state", [])
        .map_err(io::Error::other)?;
    transaction
        .execute("DELETE FROM channel_delivery_receipt_errors", [])
        .map_err(io::Error::other)?;
    transaction
        .execute("DELETE FROM channel_delivery_receipt_records", [])
        .map_err(io::Error::other)?;
    transaction
        .execute("DELETE FROM channel_delivery_receipt_history_segments", [])
        .map_err(io::Error::other)?;
    reset_platform_delivery_counts(&transaction)?;
    append_cold_history_segments_transaction(
        &transaction,
        channel_dir,
        &history.segments,
        warnings,
    )?;
    match fs::metadata(receipts_file) {
        Ok(metadata) if metadata.is_file() => {
            let cursor = read_receipts_from_cursor(
                &transaction,
                receipts_file,
                LedgerCursor::default(),
                "hot",
                "delivery-receipts",
                warnings,
                "rebuilding",
            )?;
            write_cursor(&transaction, META_RECEIPT_CURSOR, &cursor)?;
        }
        Ok(_) => warnings.push(format!(
            "channel delivery receipt path is not a file while rebuilding: {}",
            receipts_file.display()
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            delete_meta(&transaction, META_RECEIPT_CURSOR)?;
        }
        Err(error) => return Err(error),
    }
    transaction.commit().map_err(io::Error::other)
}

enum ColdHistoryRefreshMode {
    Current,
    Append(Vec<ChannelDeliveryReceiptHistorySegment>),
    Rebuild(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct IndexedColdHistorySegment {
    file_name: String,
    records: usize,
    bytes: u64,
    digest: u64,
    compacted_at_ms: i64,
}

fn cold_history_refresh_mode(
    connection: &Connection,
    history: &ChannelDeliveryReceiptHistory,
) -> io::Result<ColdHistoryRefreshMode> {
    let known = indexed_cold_history_segments(connection)?;
    let declared = history
        .segments
        .iter()
        .map(|segment| (segment.transaction_id.as_str(), segment))
        .collect::<HashMap<_, _>>();
    for transaction_id in known.keys() {
        if !declared.contains_key(transaction_id.as_str()) {
            return Ok(ColdHistoryRefreshMode::Rebuild(format!(
                "channel delivery cold history manifest no longer contains indexed transaction `{transaction_id}`"
            )));
        }
    }
    let mut append = Vec::new();
    for segment in &history.segments {
        match known.get(&segment.transaction_id) {
            None => append.push(segment.clone()),
            Some(indexed) if cold_history_segment_matches(indexed, segment) => {}
            Some(_) => {
                return Ok(ColdHistoryRefreshMode::Rebuild(format!(
                    "channel delivery cold history transaction `{}` changed after indexing",
                    segment.transaction_id
                )));
            }
        }
    }
    if append.is_empty() {
        Ok(ColdHistoryRefreshMode::Current)
    } else {
        Ok(ColdHistoryRefreshMode::Append(append))
    }
}

fn cold_history_segment_matches(
    indexed: &IndexedColdHistorySegment,
    segment: &ChannelDeliveryReceiptHistorySegment,
) -> bool {
    indexed.file_name == segment.file_name
        && indexed.records == segment.records
        && indexed.bytes == segment.bytes
        && indexed.digest == segment.digest
        && indexed.compacted_at_ms == segment.compacted_at_ms
}

fn indexed_cold_history_segments(
    connection: &Connection,
) -> io::Result<HashMap<String, IndexedColdHistorySegment>> {
    let mut statement = connection
        .prepare(
            "SELECT transaction_id, file_name, records, bytes, digest, compacted_at_ms \
             FROM channel_delivery_receipt_history_segments",
        )
        .map_err(io::Error::other)?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, i64>(5)?,
            ))
        })
        .map_err(io::Error::other)?;
    let mut segments = HashMap::new();
    for row in rows {
        let (transaction_id, file_name, records, bytes, digest, compacted_at_ms) =
            row.map_err(io::Error::other)?;
        let digest = u64::from_str_radix(&digest, 16).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "channel delivery cold history digest for transaction `{transaction_id}` is invalid: {error}"
                ),
            )
        })?;
        segments.insert(
            transaction_id,
            IndexedColdHistorySegment {
                file_name,
                records: usize_from_i64(records, "cold history record count")?,
                bytes: u64_from_i64(bytes, "cold history byte count")?,
                digest,
                compacted_at_ms,
            },
        );
    }
    Ok(segments)
}

fn append_cold_history_segments(
    connection: &mut Connection,
    channel_dir: &Path,
    segments: &[ChannelDeliveryReceiptHistorySegment],
    warnings: &mut Vec<String>,
) -> io::Result<()> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(io::Error::other)?;
    append_cold_history_segments_transaction(&transaction, channel_dir, segments, warnings)?;
    transaction.commit().map_err(io::Error::other)
}

fn append_cold_history_segments_transaction(
    transaction: &Transaction<'_>,
    channel_dir: &Path,
    segments: &[ChannelDeliveryReceiptHistorySegment],
    warnings: &mut Vec<String>,
) -> io::Result<()> {
    for segment in segments {
        if !channel_delivery_receipt_history_segment_is_valid(channel_dir, segment)? {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "channel delivery cold history segment failed integrity validation: {}",
                    segment.file_name
                ),
            ));
        }
        let segment_file = channel_delivery_receipt_history_segment_file(channel_dir, segment)?;
        let _ = read_receipts_from_cursor(
            transaction,
            &segment_file,
            LedgerCursor::default(),
            "cold",
            &segment.transaction_id,
            warnings,
            "replaying cold history",
        )?;
        transaction
            .execute(
                "INSERT INTO channel_delivery_receipt_history_segments \
                 (transaction_id, file_name, records, bytes, digest, compacted_at_ms) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    segment.transaction_id,
                    segment.file_name,
                    i64_from_usize(segment.records, "cold history record count")?,
                    i64_from_u64(segment.bytes, "cold history byte count")?,
                    format!("{:016x}", segment.digest),
                    segment.compacted_at_ms,
                ],
            )
            .map_err(io::Error::other)?;
    }
    Ok(())
}

fn reset_outbox_index(connection: &mut Connection) -> io::Result<()> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(io::Error::other)?;
    transaction
        .execute("DELETE FROM channel_delivery_outbox_lines", [])
        .map_err(io::Error::other)?;
    transaction
        .execute("DELETE FROM channel_delivery_platform_summary", [])
        .map_err(io::Error::other)?;
    transaction
        .execute("DELETE FROM channel_delivery_receipt_state", [])
        .map_err(io::Error::other)?;
    transaction
        .execute("DELETE FROM channel_delivery_receipt_errors", [])
        .map_err(io::Error::other)?;
    transaction
        .execute("DELETE FROM channel_delivery_receipt_records", [])
        .map_err(io::Error::other)?;
    transaction
        .execute("DELETE FROM channel_delivery_receipt_history_segments", [])
        .map_err(io::Error::other)?;
    delete_meta(&transaction, META_OUTBOX_CURSOR)?;
    delete_meta(&transaction, META_RECEIPT_CURSOR)?;
    write_meta(&transaction, META_OUTBOX_TOTAL_LINES, "0")?;
    write_meta(&transaction, META_OUTBOX_INVALID_LINES, "0")?;
    transaction.commit().map_err(io::Error::other)
}

fn read_outbox_from_cursor(
    transaction: &Transaction<'_>,
    outbox_file: &Path,
    cursor: LedgerCursor,
    _warnings: &mut Vec<String>,
    _phase: &str,
) -> io::Result<(LedgerCursor, OutboxCounterDelta)> {
    let file = File::open(outbox_file)?;
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
    let mut delta = OutboxCounterDelta::default();
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
            && serde_json::from_str::<serde_json::Value>(trimmed).is_err()
        {
            break;
        }
        line_number = line_number.saturating_add(1);
        offset_bytes = offset_bytes.saturating_add(bytes_read as u64);
        if trimmed.is_empty() {
            continue;
        }
        delta.total = delta.total.saturating_add(1);
        match serde_json::from_str::<ChannelOutboundMessage>(trimmed) {
            Ok(message) => insert_outbox_message(transaction, line_number, trimmed, &message)?,
            Err(error) => {
                delta.invalid = delta.invalid.saturating_add(1);
                transaction
                    .execute(
                        "INSERT OR REPLACE INTO channel_delivery_outbox_lines \
                         (line_number, delivery_id, platform, account_id, channel_id, user_id, \
                          session_key, source_queue_id, message_json, parse_error) \
                         VALUES (?1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, ?2)",
                        params![
                            i64_from_usize(line_number, "outbox line number")?,
                            error.to_string()
                        ],
                    )
                    .map_err(io::Error::other)?;
            }
        }
    }
    let metadata = reader.get_ref().metadata()?;
    Ok((
        cursor_from_processed_ledger(outbox_file, offset_bytes, line_number, &metadata)?,
        delta,
    ))
}

fn read_receipts_from_cursor(
    transaction: &Transaction<'_>,
    receipts_file: &Path,
    cursor: LedgerCursor,
    source_kind: &str,
    source_id: &str,
    _warnings: &mut Vec<String>,
    _phase: &str,
) -> io::Result<LedgerCursor> {
    let file = File::open(receipts_file)?;
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
            && serde_json::from_str::<serde_json::Value>(trimmed).is_err()
        {
            break;
        }
        line_number = line_number.saturating_add(1);
        offset_bytes = offset_bytes.saturating_add(bytes_read as u64);
        if trimmed.is_empty() {
            continue;
        }
        match serde_json::from_str::<ChannelDeliveryReceipt>(trimmed) {
            Ok(receipt) => {
                let canonical_delivery_id = apply_delivery_receipt(transaction, &receipt)?;
                insert_receipt_history_record(
                    transaction,
                    source_kind,
                    source_id,
                    line_number,
                    &canonical_delivery_id,
                    trimmed,
                    &receipt,
                )?;
            }
            Err(error) => {
                transaction
                    .execute(
                        "INSERT OR REPLACE INTO channel_delivery_receipt_errors \
                         (source_kind, source_id, line_number, parse_error) \
                         VALUES (?1, ?2, ?3, ?4)",
                        params![
                            source_kind,
                            source_id,
                            i64_from_usize(line_number, "delivery receipt line number")?,
                            error.to_string()
                        ],
                    )
                    .map_err(io::Error::other)?;
            }
        }
    }
    let metadata = reader.get_ref().metadata()?;
    cursor_from_processed_ledger(receipts_file, offset_bytes, line_number, &metadata)
}

fn insert_outbox_message(
    transaction: &Transaction<'_>,
    line_number: usize,
    raw_line: &str,
    message: &ChannelOutboundMessage,
) -> io::Result<()> {
    let legacy_delivery_id = delivery_id(line_number, raw_line);
    let delivery_id = delivery_id_for_outbox_message(line_number, raw_line, message);
    let message_json = serde_json::to_string(message).map_err(io::Error::other)?;
    transaction
        .execute(
            "INSERT INTO channel_delivery_outbox_lines \
              (line_number, delivery_id, legacy_delivery_id, platform, account_id, channel_id, \
               user_id, session_key, source_queue_id, message_json, parse_error) \
              VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, NULL) \
              ON CONFLICT(line_number) DO UPDATE SET \
                delivery_id = excluded.delivery_id, \
                legacy_delivery_id = excluded.legacy_delivery_id, \
                platform = excluded.platform, \
                account_id = excluded.account_id, \
                channel_id = excluded.channel_id, \
                user_id = excluded.user_id, \
                session_key = excluded.session_key, \
                source_queue_id = excluded.source_queue_id, \
                message_json = excluded.message_json, \
                parse_error = NULL",
            params![
                i64_from_usize(line_number, "outbox line number")?,
                delivery_id,
                legacy_delivery_id,
                message.platform,
                message.account_id,
                message.channel_id,
                message.user_id,
                message.session_key,
                message.source_queue_id,
                message_json,
            ],
        )
        .map_err(io::Error::other)?;
    ensure_platform_summary(transaction, &message.platform)?;
    transaction
        .execute(
            "UPDATE channel_delivery_platform_summary \
             SET valid_outbox_count = valid_outbox_count + 1 \
             WHERE platform = ?1",
            params![message.platform],
        )
        .map_err(io::Error::other)?;
    let state = lookup_delivery_state_transaction(transaction, &delivery_id)?;
    adjust_platform_summary(
        transaction,
        &message.platform,
        delivery_disposition(state.as_ref()),
        1,
    )
}

fn apply_delivery_receipt(
    transaction: &Transaction<'_>,
    receipt: &ChannelDeliveryReceipt,
) -> io::Result<String> {
    let delivery_id = canonical_delivery_id_for_transaction(transaction, &receipt.delivery_id)?;
    let previous = lookup_delivery_state_transaction(transaction, &delivery_id)?;
    let platform: Option<String> = transaction
        .query_row(
            "SELECT platform FROM channel_delivery_outbox_lines \
              WHERE (delivery_id = ?1 OR legacy_delivery_id = ?1) AND parse_error IS NULL",
            params![delivery_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(io::Error::other)?;
    if let Some(platform) = platform.as_deref() {
        adjust_platform_summary(
            transaction,
            platform,
            delivery_disposition(previous.as_ref()),
            -1,
        )?;
    }

    let next = next_delivery_state(previous, receipt);
    write_delivery_state(transaction, &delivery_id, &next)?;
    if let Some(platform) = platform.as_deref() {
        adjust_platform_summary(transaction, platform, delivery_disposition(Some(&next)), 1)?;
    }
    Ok(delivery_id)
}

fn insert_receipt_history_record(
    transaction: &Transaction<'_>,
    source_kind: &str,
    source_id: &str,
    line_number: usize,
    delivery_id: &str,
    raw_receipt_json: &str,
    receipt: &ChannelDeliveryReceipt,
) -> io::Result<()> {
    transaction
        .execute(
            "INSERT OR REPLACE INTO channel_delivery_receipt_records \
             (source_kind, source_id, line_number, delivery_id, receipt_json, at_ms) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                source_kind,
                source_id,
                i64_from_usize(line_number, "delivery receipt line number")?,
                delivery_id,
                raw_receipt_json,
                receipt.at_ms,
            ],
        )
        .map_err(io::Error::other)?;
    Ok(())
}

fn canonical_delivery_id_for_connection(
    connection: &Connection,
    supplied_delivery_id: &str,
) -> io::Result<String> {
    connection
        .query_row(
            "SELECT delivery_id FROM channel_delivery_outbox_lines \
             WHERE (delivery_id = ?1 OR legacy_delivery_id = ?1) AND parse_error IS NULL",
            params![supplied_delivery_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(io::Error::other)
        .map(|delivery_id| delivery_id.unwrap_or_else(|| supplied_delivery_id.to_string()))
}

fn canonical_delivery_id_for_transaction(
    transaction: &Transaction<'_>,
    supplied_delivery_id: &str,
) -> io::Result<String> {
    transaction
        .query_row(
            "SELECT delivery_id FROM channel_delivery_outbox_lines \
             WHERE (delivery_id = ?1 OR legacy_delivery_id = ?1) AND parse_error IS NULL",
            params![supplied_delivery_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(io::Error::other)
        .map(|delivery_id| delivery_id.unwrap_or_else(|| supplied_delivery_id.to_string()))
}

fn next_delivery_state(
    previous: Option<DeliveryStateRow>,
    receipt: &ChannelDeliveryReceipt,
) -> DeliveryStateRow {
    let mut next = previous.unwrap_or_default();
    next.attempts = next.attempts.saturating_add(1);
    next.last_status = Some(receipt.status);
    next.last_partial_failure = receipt.status == ChannelDeliveryStatus::Failed
        && receipt
            .rendered_units
            .iter()
            .any(|unit| unit.status != crate::ChannelDeliveryUnitStatus::Delivered);
    if is_terminal_status(receipt.status) {
        next.terminal_status = Some(receipt.status);
    }
    next
}

fn write_delivery_state(
    transaction: &Transaction<'_>,
    delivery_id: &str,
    state: &DeliveryStateRow,
) -> io::Result<()> {
    transaction
        .execute(
            "INSERT INTO channel_delivery_receipt_state \
             (delivery_id, attempts, last_status, terminal_status, last_partial_failure) \
             VALUES (?1, ?2, ?3, ?4, ?5) \
             ON CONFLICT(delivery_id) DO UPDATE SET \
                attempts = excluded.attempts, \
                last_status = excluded.last_status, \
                terminal_status = excluded.terminal_status, \
                last_partial_failure = excluded.last_partial_failure",
            params![
                delivery_id,
                i64_from_usize(state.attempts, "delivery attempt count")?,
                state.last_status.map(status_name),
                state.terminal_status.map(status_name),
                i64::from(state.last_partial_failure),
            ],
        )
        .map_err(io::Error::other)?;
    Ok(())
}

fn lookup_delivery_state(
    connection: &Connection,
    delivery_id: &str,
) -> io::Result<Option<DeliveryStateRow>> {
    read_delivery_state(connection, delivery_id)
}

fn lookup_delivery_state_transaction(
    transaction: &Transaction<'_>,
    delivery_id: &str,
) -> io::Result<Option<DeliveryStateRow>> {
    let row = transaction
        .query_row(
            "SELECT attempts, last_status, terminal_status, last_partial_failure \
             FROM channel_delivery_receipt_state WHERE delivery_id = ?1",
            params![delivery_id],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            },
        )
        .optional()
        .map_err(io::Error::other)?;
    delivery_state_from_row(row)
}

fn read_delivery_state(
    connection: &Connection,
    delivery_id: &str,
) -> io::Result<Option<DeliveryStateRow>> {
    let row = connection
        .query_row(
            "SELECT attempts, last_status, terminal_status, last_partial_failure \
             FROM channel_delivery_receipt_state WHERE delivery_id = ?1",
            params![delivery_id],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            },
        )
        .optional()
        .map_err(io::Error::other)?;
    delivery_state_from_row(row)
}

fn delivery_state_from_row(
    row: Option<(i64, Option<String>, Option<String>, i64)>,
) -> io::Result<Option<DeliveryStateRow>> {
    let Some((attempts, last_status, terminal_status, last_partial_failure)) = row else {
        return Ok(None);
    };
    Ok(Some(DeliveryStateRow {
        attempts: usize_from_i64(attempts, "delivery attempt count")?,
        last_status: last_status.as_deref().map(parse_status).transpose()?,
        terminal_status: terminal_status.as_deref().map(parse_status).transpose()?,
        last_partial_failure: last_partial_failure != 0,
    }))
}

fn delivery_disposition(state: Option<&DeliveryStateRow>) -> DeliveryDisposition {
    let status = state.and_then(|state| state.terminal_status.or(state.last_status));
    match status {
        Some(ChannelDeliveryStatus::Delivered) => DeliveryDisposition::Delivered,
        Some(ChannelDeliveryStatus::SkippedPermanent) => DeliveryDisposition::SkippedPermanent,
        Some(ChannelDeliveryStatus::Failed) => DeliveryDisposition::Failed {
            partial: state.is_some_and(|state| state.last_partial_failure),
        },
        None => DeliveryDisposition::Pending,
    }
}

fn ensure_platform_summary(transaction: &Transaction<'_>, platform: &str) -> io::Result<()> {
    transaction
        .execute(
            "INSERT OR IGNORE INTO channel_delivery_platform_summary (platform) VALUES (?1)",
            params![platform],
        )
        .map_err(io::Error::other)?;
    Ok(())
}

fn adjust_platform_summary(
    transaction: &Transaction<'_>,
    platform: &str,
    disposition: DeliveryDisposition,
    direction: i64,
) -> io::Result<()> {
    ensure_platform_summary(transaction, platform)?;
    let (pending, delivered, failed_retryable, skipped_permanent, partial_failed) =
        match disposition {
            DeliveryDisposition::Pending => (direction, 0, 0, 0, 0),
            DeliveryDisposition::Delivered => (0, direction, 0, 0, 0),
            DeliveryDisposition::Failed { partial } => {
                (0, 0, direction, 0, if partial { direction } else { 0 })
            }
            DeliveryDisposition::SkippedPermanent => (0, 0, 0, direction, 0),
        };
    transaction
        .execute(
            "UPDATE channel_delivery_platform_summary SET \
                pending = pending + ?2, \
                delivered = delivered + ?3, \
                failed_retryable = failed_retryable + ?4, \
                skipped_permanent = skipped_permanent + ?5, \
                partial_failed = partial_failed + ?6 \
             WHERE platform = ?1",
            params![
                platform,
                pending,
                delivered,
                failed_retryable,
                skipped_permanent,
                partial_failed,
            ],
        )
        .map_err(io::Error::other)?;
    Ok(())
}

fn reset_platform_delivery_counts(transaction: &Transaction<'_>) -> io::Result<()> {
    transaction
        .execute(
            "UPDATE channel_delivery_platform_summary SET \
                pending = valid_outbox_count, \
                delivered = 0, \
                failed_retryable = 0, \
                skipped_permanent = 0, \
                partial_failed = 0",
            [],
        )
        .map_err(io::Error::other)?;
    Ok(())
}

#[derive(Debug, Clone, Copy, Default)]
struct PlatformSummary {
    valid_outbox_count: i64,
    pending: i64,
    delivered: i64,
    failed_retryable: i64,
    skipped_permanent: i64,
    partial_failed: i64,
}

fn read_platform_summary(
    connection: &Connection,
    platform: Option<&str>,
) -> io::Result<PlatformSummary> {
    let values: (i64, i64, i64, i64, i64, i64) = match platform {
        Some(platform) => connection
            .query_row(
                "SELECT valid_outbox_count, pending, delivered, failed_retryable, \
                        skipped_permanent, partial_failed \
                 FROM channel_delivery_platform_summary WHERE platform = ?1",
                params![platform],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
            .optional()
            .map_err(io::Error::other)?
            .unwrap_or_default(),
        None => connection
            .query_row(
                "SELECT COALESCE(SUM(valid_outbox_count), 0), \
                        COALESCE(SUM(pending), 0), \
                        COALESCE(SUM(delivered), 0), \
                        COALESCE(SUM(failed_retryable), 0), \
                        COALESCE(SUM(skipped_permanent), 0), \
                        COALESCE(SUM(partial_failed), 0) \
                 FROM channel_delivery_platform_summary",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
            .map_err(io::Error::other)?,
    };
    Ok(PlatformSummary {
        valid_outbox_count: values.0,
        pending: values.1,
        delivered: values.2,
        failed_retryable: values.3,
        skipped_permanent: values.4,
        partial_failed: values.5,
    })
}

fn read_pending_deliveries(
    connection: &Connection,
    platform: Option<&str>,
    limit: usize,
) -> io::Result<Vec<ChannelDeliveryPending>> {
    let limit = i64_from_usize(limit, "channel delivery plan limit")?;
    let sql = match platform {
        Some(_) => {
            "SELECT outbox.delivery_id, outbox.line_number, outbox.message_json, \
                    COALESCE(state.attempts, 0), state.last_status, state.terminal_status \
             FROM channel_delivery_outbox_lines AS outbox \
             LEFT JOIN channel_delivery_receipt_state AS state \
                 ON state.delivery_id = outbox.delivery_id \
             WHERE outbox.parse_error IS NULL AND outbox.platform = ?1 \
               AND state.terminal_status IS NULL \
               AND (state.last_status IS NULL OR state.last_status = 'failed') \
             ORDER BY outbox.line_number LIMIT ?2"
        }
        None => {
            "SELECT outbox.delivery_id, outbox.line_number, outbox.message_json, \
                    COALESCE(state.attempts, 0), state.last_status, state.terminal_status \
             FROM channel_delivery_outbox_lines AS outbox \
             LEFT JOIN channel_delivery_receipt_state AS state \
                 ON state.delivery_id = outbox.delivery_id \
             WHERE outbox.parse_error IS NULL \
               AND state.terminal_status IS NULL \
               AND (state.last_status IS NULL OR state.last_status = 'failed') \
             ORDER BY outbox.line_number LIMIT ?1"
        }
    };
    let mut statement = connection.prepare(sql).map_err(io::Error::other)?;
    let mut rows = match platform {
        Some(platform) => statement.query(params![platform, limit]),
        None => statement.query(params![limit]),
    }
    .map_err(io::Error::other)?;
    let mut pending = Vec::new();
    while let Some(row) = rows.next().map_err(io::Error::other)? {
        let message_json: String = row.get(2).map_err(io::Error::other)?;
        let message = serde_json::from_str(&message_json).map_err(io::Error::other)?;
        let terminal_status: Option<String> = row.get(5).map_err(io::Error::other)?;
        let last_status: Option<String> = row.get(4).map_err(io::Error::other)?;
        pending.push(ChannelDeliveryPending {
            delivery_id: row.get(0).map_err(io::Error::other)?,
            line_number: usize_from_i64(
                row.get(1).map_err(io::Error::other)?,
                "outbox line number",
            )?,
            attempts: usize_from_i64(
                row.get(3).map_err(io::Error::other)?,
                "delivery attempt count",
            )?,
            last_status: terminal_status
                .as_deref()
                .or(last_status.as_deref())
                .map(parse_status)
                .transpose()?,
            message,
        });
    }
    Ok(pending)
}

fn read_indexed_states<P>(
    connection: &Connection,
    where_clause: &str,
    params: P,
) -> io::Result<Vec<ChannelDeliveryIndexedState>>
where
    P: rusqlite::Params,
{
    let sql = format!(
        "SELECT outbox.delivery_id, outbox.line_number, outbox.message_json, \
                COALESCE(state.attempts, 0), state.last_status, state.terminal_status \
         FROM channel_delivery_outbox_lines AS outbox \
         LEFT JOIN channel_delivery_receipt_state AS state \
             ON state.delivery_id = outbox.delivery_id \
         {where_clause}"
    );
    let mut statement = connection.prepare(&sql).map_err(io::Error::other)?;
    let mut rows = statement.query(params).map_err(io::Error::other)?;
    let mut states = Vec::new();
    while let Some(row) = rows.next().map_err(io::Error::other)? {
        let message_json: String = row.get(2).map_err(io::Error::other)?;
        states.push(ChannelDeliveryIndexedState {
            delivery_id: row.get(0).map_err(io::Error::other)?,
            line_number: usize_from_i64(
                row.get(1).map_err(io::Error::other)?,
                "outbox line number",
            )?,
            attempts: usize_from_i64(
                row.get(3).map_err(io::Error::other)?,
                "delivery attempt count",
            )?,
            last_status: row
                .get::<_, Option<String>>(4)
                .map_err(io::Error::other)?
                .as_deref()
                .map(parse_status)
                .transpose()?,
            terminal_status: row
                .get::<_, Option<String>>(5)
                .map_err(io::Error::other)?
                .as_deref()
                .map(parse_status)
                .transpose()?,
            message: serde_json::from_str(&message_json).map_err(io::Error::other)?,
        });
    }
    Ok(states)
}

fn append_index_parse_warnings(
    connection: &Connection,
    warnings: &mut Vec<String>,
) -> io::Result<()> {
    let mut outbox = connection
        .prepare(
            "SELECT line_number, parse_error FROM channel_delivery_outbox_lines \
             WHERE parse_error IS NOT NULL ORDER BY line_number",
        )
        .map_err(io::Error::other)?;
    let rows = outbox
        .query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(io::Error::other)?;
    for row in rows {
        let (line_number, error) = row.map_err(io::Error::other)?;
        warnings.push(format!(
            "channel outbox line {line_number} is not valid JSON: {error}"
        ));
    }
    let mut receipts = connection
        .prepare(
            "SELECT source_kind, source_id, line_number, parse_error \
             FROM channel_delivery_receipt_errors \
             ORDER BY source_kind, source_id, line_number",
        )
        .map_err(io::Error::other)?;
    let rows = receipts
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
            ))
        })
        .map_err(io::Error::other)?;
    for row in rows {
        let (source_kind, source_id, line_number, error) = row.map_err(io::Error::other)?;
        warnings.push(format!(
            "{source_kind} delivery receipt `{source_id}` line {line_number} is not valid JSON: {error}"
        ));
    }
    Ok(())
}

enum LedgerRefreshMode {
    Current,
    Append(LedgerCursor),
    Rebuild(String),
}

fn outbox_refresh_mode(
    outbox_file: &Path,
    metadata: &fs::Metadata,
    cursor: Option<&LedgerCursor>,
) -> io::Result<LedgerRefreshMode> {
    ledger_refresh_mode("channel outbox", outbox_file, metadata, cursor)
}

fn receipt_refresh_mode(
    receipts_file: &Path,
    metadata: &fs::Metadata,
    cursor: Option<&LedgerCursor>,
) -> io::Result<LedgerRefreshMode> {
    ledger_refresh_mode(
        "channel delivery receipt ledger",
        receipts_file,
        metadata,
        cursor,
    )
}

fn ledger_refresh_mode(
    label: &str,
    file: &Path,
    metadata: &fs::Metadata,
    cursor: Option<&LedgerCursor>,
) -> io::Result<LedgerRefreshMode> {
    let Some(cursor) = cursor else {
        return Ok(LedgerRefreshMode::Rebuild(format!(
            "{label} index has no cursor"
        )));
    };
    if cursor.source_modified_at_unix_nanos.is_none()
        || (cursor.offset_bytes > 0
            && (cursor.prefix_tail_fingerprint.is_none() || cursor.head_fingerprint.is_none()))
    {
        return Ok(LedgerRefreshMode::Rebuild(format!(
            "{label} index has no stable cursor fingerprint"
        )));
    }
    if metadata.len() < cursor.offset_bytes {
        return Ok(LedgerRefreshMode::Rebuild(format!("{label} was truncated")));
    }
    let source_modified_at_unix_nanos = file_modified_at_unix_nanos(metadata);
    let prefix_matches = ledger_prefix_tail_matches(file, cursor)?;
    let head_matches = ledger_head_matches(file, cursor)?;
    if metadata.len() == cursor.offset_bytes {
        if source_modified_at_unix_nanos == cursor.source_modified_at_unix_nanos
            && prefix_matches
            && head_matches
        {
            return Ok(LedgerRefreshMode::Current);
        }
        return Ok(LedgerRefreshMode::Rebuild(format!(
            "{label} changed without a stable append"
        )));
    }
    if prefix_matches && head_matches {
        return Ok(LedgerRefreshMode::Append(cursor.clone()));
    }
    Ok(LedgerRefreshMode::Rebuild(format!(
        "{label} prefix no longer matches its cursor"
    )))
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
        prefix_tail_fingerprint: ledger_prefix_tail_fingerprint(file, offset_bytes)?,
        head_fingerprint: ledger_head_fingerprint(file, offset_bytes)?,
    })
}

fn ledger_prefix_tail_matches(file: &Path, cursor: &LedgerCursor) -> io::Result<bool> {
    Ok(
        ledger_prefix_tail_fingerprint(file, cursor.offset_bytes)?
            == cursor.prefix_tail_fingerprint,
    )
}

fn ledger_head_matches(file: &Path, cursor: &LedgerCursor) -> io::Result<bool> {
    Ok(ledger_head_fingerprint(file, cursor.offset_bytes)? == cursor.head_fingerprint)
}

fn ledger_prefix_tail_fingerprint(file: &Path, offset_bytes: u64) -> io::Result<Option<u64>> {
    if offset_bytes == 0 {
        return Ok(None);
    }
    let mut handle = File::open(file)?;
    let start = offset_bytes.saturating_sub(FINGERPRINT_BYTES);
    handle.seek(SeekFrom::Start(start))?;
    fingerprint_file_range(&mut handle, offset_bytes.saturating_sub(start)).map(Some)
}

fn ledger_head_fingerprint(file: &Path, offset_bytes: u64) -> io::Result<Option<u64>> {
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

fn read_cursor(connection: &Connection, key: &str) -> io::Result<Option<LedgerCursor>> {
    let Some(value) = read_meta(connection, key)? else {
        return Ok(None);
    };
    serde_json::from_str(&value)
        .map(Some)
        .map_err(io::Error::other)
}

fn write_cursor(transaction: &Transaction<'_>, key: &str, cursor: &LedgerCursor) -> io::Result<()> {
    let encoded = serde_json::to_string(cursor).map_err(io::Error::other)?;
    write_meta(transaction, key, &encoded)
}

fn read_meta(connection: &Connection, key: &str) -> io::Result<Option<String>> {
    connection
        .query_row(
            "SELECT value FROM channel_delivery_meta WHERE key = ?1",
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
            format!("channel delivery state metadata `{key}` is invalid: {error}"),
        )
    })
}

fn write_meta(transaction: &Transaction<'_>, key: &str, value: &str) -> io::Result<()> {
    transaction
        .execute(
            "INSERT INTO channel_delivery_meta (key, value) VALUES (?1, ?2) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![key, value],
        )
        .map_err(io::Error::other)?;
    Ok(())
}

fn delete_meta(transaction: &Transaction<'_>, key: &str) -> io::Result<()> {
    transaction
        .execute(
            "DELETE FROM channel_delivery_meta WHERE key = ?1",
            params![key],
        )
        .map_err(io::Error::other)?;
    Ok(())
}

fn outbox_index_has_rows(connection: &Connection) -> io::Result<bool> {
    let count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM channel_delivery_outbox_lines",
            [],
            |row| row.get(0),
        )
        .map_err(io::Error::other)?;
    Ok(count > 0)
}

fn receipt_index_has_rows(connection: &Connection) -> io::Result<bool> {
    let count: i64 = connection
        .query_row(
            "SELECT (SELECT COUNT(*) FROM channel_delivery_receipt_state) \
             + (SELECT COUNT(*) FROM channel_delivery_receipt_errors) \
             + (SELECT COUNT(*) FROM channel_delivery_receipt_records) \
             + (SELECT COUNT(*) FROM channel_delivery_receipt_history_segments)",
            [],
            |row| row.get(0),
        )
        .map_err(io::Error::other)?;
    Ok(count > 0)
}

fn append_json_line_while_locked(path: &Path, value: &impl Serialize) -> io::Result<()> {
    let needs_leading_newline = match fs::metadata(path) {
        Ok(metadata) if metadata.len() > 0 => {
            let mut existing = File::open(path)?;
            existing.seek(SeekFrom::End(-1))?;
            let mut last = [0_u8; 1];
            existing.read_exact(&mut last)?;
            last[0] != b'\n'
        }
        Ok(_) => false,
        Err(error) if error.kind() == io::ErrorKind::NotFound => false,
        Err(error) => return Err(error),
    };
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    if needs_leading_newline {
        file.write_all(b"\n")?;
    }
    serde_json::to_writer(&mut file, value).map_err(io::Error::other)?;
    file.write_all(b"\n")?;
    file.flush()
}

fn is_terminal_status(status: ChannelDeliveryStatus) -> bool {
    matches!(
        status,
        ChannelDeliveryStatus::Delivered | ChannelDeliveryStatus::SkippedPermanent
    )
}

fn status_name(status: ChannelDeliveryStatus) -> &'static str {
    match status {
        ChannelDeliveryStatus::Delivered => "delivered",
        ChannelDeliveryStatus::Failed => "failed",
        ChannelDeliveryStatus::SkippedPermanent => "skipped-permanent",
    }
}

fn parse_status(value: &str) -> io::Result<ChannelDeliveryStatus> {
    match value {
        "delivered" => Ok(ChannelDeliveryStatus::Delivered),
        "failed" => Ok(ChannelDeliveryStatus::Failed),
        "skipped-permanent" => Ok(ChannelDeliveryStatus::SkippedPermanent),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("channel delivery state has unsupported status `{other}`"),
        )),
    }
}

fn i64_from_usize(value: usize, label: &str) -> io::Result<i64> {
    i64::try_from(value).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{label} exceeds SQLite range"),
        )
    })
}

fn i64_from_u64(value: u64, label: &str) -> io::Result<i64> {
    i64::try_from(value).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{label} exceeds SQLite range"),
        )
    })
}

fn usize_from_i64(value: i64, label: &str) -> io::Result<usize> {
    usize::try_from(value).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{label} is outside the valid range"),
        )
    })
}

fn u64_from_i64(value: i64, label: &str) -> io::Result<u64> {
    u64::try_from(value).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{label} is outside the valid range"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ChannelOutboundMessageKind, append_jsonl_value};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn blocking_source_queue_lookup_tails_a_new_source_row_before_querying_the_index() {
        let root = temp_root(
            "blocking_source_queue_lookup_tails_a_new_source_row_before_querying_the_index",
        );
        let channel_dir = root.join("state").join("channels");
        let outbox_file = channel_dir.join("outbox.jsonl");

        append_jsonl_value(&outbox_file, &message("other-source", "already indexed")).unwrap();
        let mut warnings = Vec::new();
        let existing = channel_delivery_states_for_source_queue_blocking(
            &channel_dir,
            "other-source",
            &mut warnings,
        )
        .unwrap();
        assert_eq!(existing.len(), 1);

        // Append through the JSONL source after the index cursor is current.
        // The blocking lookup must take normal locks and tail this row before
        // querying SQLite by the source queue key.
        append_jsonl_value(&outbox_file, &message("source-queue-42", "new final")).unwrap();
        let mut warnings = Vec::new();
        let states = channel_delivery_states_for_source_queue_blocking(
            &channel_dir,
            "source-queue-42",
            &mut warnings,
        )
        .unwrap();

        assert_eq!(states.len(), 1);
        assert_eq!(
            states[0].message.source_queue_id.as_deref(),
            Some("source-queue-42")
        );
        assert_eq!(states[0].message.text, "new final");

        std::fs::remove_dir_all(root).unwrap();
    }

    fn message(source_queue_id: &str, text: &str) -> ChannelOutboundMessage {
        ChannelOutboundMessage {
            platform: "discord".to_string(),
            account_id: None,
            channel_id: "dm-1".to_string(),
            user_id: "user-1".to_string(),
            session_key: "session-1".to_string(),
            delivery_id: None,
            kind: ChannelOutboundMessageKind::AgentReply,
            source_queue_id: Some(source_queue_id.to_string()),
            source_completion_file: None,
            text: text.to_string(),
            presentation: None,
            delivery_intent: None,
            attachments: Vec::new(),
        }
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-channel-delivery-index-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
