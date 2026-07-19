use std::fs;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::{
    ChannelOutboundAttachmentKind, ChannelOutboundMessage, HarnessLogEvent, HarnessLogLevel,
    append_harness_log, current_log_time_ms,
};

const CHANNEL_OUTBOX_PLAN_SCHEMA: &str = "agent-harness.channel-outbox-plan.v1";
const CHANNEL_DELIVERY_RECEIPT_SCHEMA: &str = "agent-harness.channel-delivery-receipt.v1";
pub(crate) const CHANNEL_DELIVERY_RECEIPT_COMPACTION_SCHEMA: &str =
    "agent-harness.channel-delivery-receipt-compaction.v1";
const CHANNEL_DELIVERY_RECEIPT_HISTORY_QUERY_SCHEMA: &str =
    "agent-harness.channel-delivery-receipt-history-query.v1";

/// These defaults keep the normal delivery path small without treating the
/// receipt ledger as disposable.  If an oversized ledger has only active or
/// legacy rows, compaction reports explicit backpressure instead of evicting
/// evidence or changing retry semantics.
pub const DEFAULT_CHANNEL_DELIVERY_RECEIPT_MAX_HOT_BYTES: u64 = 64 * 1024 * 1024;
pub const DEFAULT_CHANNEL_DELIVERY_RECEIPT_TARGET_HOT_BYTES: u64 = 48 * 1024 * 1024;
pub const DEFAULT_CHANNEL_DELIVERY_RECEIPT_MAX_HOT_RECORDS: usize = 100_000;
pub const DEFAULT_CHANNEL_DELIVERY_RECEIPT_TARGET_HOT_RECORDS: usize = 75_000;
pub const DEFAULT_CHANNEL_DELIVERY_RECEIPT_MAX_COMPACTION_RECORDS: usize = 200_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelOutboxPlanOptions {
    pub harness_home: PathBuf,
    pub platform: Option<String>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelOutboxPlanReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub outbox_file: PathBuf,
    pub receipts_file: PathBuf,
    pub pending: Vec<ChannelDeliveryPending>,
    pub summary: ChannelOutboxPlanSummary,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelOutboxPlanSummary {
    pub total_outbox_lines: usize,
    pub sampled: bool,
    pub sampled_bytes: u64,
    pub pending: usize,
    pub delivered: usize,
    pub failed_retryable: usize,
    pub skipped_permanent: usize,
    pub partial_failed: usize,
    pub skipped_platform: usize,
    pub invalid_lines: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelDeliveryPending {
    pub delivery_id: String,
    pub line_number: usize,
    pub attempts: usize,
    pub last_status: Option<ChannelDeliveryStatus>,
    pub message: ChannelOutboundMessage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelDeliveryRecordOptions {
    pub harness_home: PathBuf,
    pub delivery_id: String,
    pub status: ChannelDeliveryStatus,
    pub platform: String,
    pub account_id: Option<String>,
    pub channel_id: String,
    pub user_id: String,
    pub session_key: String,
    pub provider_message_id: Option<String>,
    pub error: Option<String>,
    pub now_ms: i64,
    pub rendered_units: Vec<ChannelDeliveryRenderedUnitReceipt>,
    pub presentation: Option<ChannelDeliveryPresentationReceipt>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelDeliveryReceiptCompactionOptions {
    pub harness_home: PathBuf,
    /// Hard upper bound for the mutable receipt ledger before maintenance is
    /// required. A blocked compaction returns `Backpressure`; it never drops
    /// active, malformed, or legacy evidence to satisfy this bound.
    pub max_hot_receipt_bytes: u64,
    /// Target below the hard byte bound after a successful compaction batch.
    pub target_hot_receipt_bytes: u64,
    pub max_hot_receipt_records: usize,
    pub target_hot_receipt_records: usize,
    /// Maximum complete delivery histories moved by one maintenance batch.
    /// The implementation may stop before this boundary to preserve an
    /// indivisible delivery's ordered audit trail.
    pub max_compaction_records: usize,
    /// Compacts eligible terminal v2 histories even when the hot ledger is
    /// below its normal threshold. Useful for an operator-controlled drain or
    /// verification run; still preserves active and legacy rows.
    pub force: bool,
    pub now_ms: i64,
}

impl ChannelDeliveryReceiptCompactionOptions {
    pub fn with_defaults(harness_home: PathBuf, now_ms: i64) -> Self {
        Self {
            harness_home,
            max_hot_receipt_bytes: DEFAULT_CHANNEL_DELIVERY_RECEIPT_MAX_HOT_BYTES,
            target_hot_receipt_bytes: DEFAULT_CHANNEL_DELIVERY_RECEIPT_TARGET_HOT_BYTES,
            max_hot_receipt_records: DEFAULT_CHANNEL_DELIVERY_RECEIPT_MAX_HOT_RECORDS,
            target_hot_receipt_records: DEFAULT_CHANNEL_DELIVERY_RECEIPT_TARGET_HOT_RECORDS,
            max_compaction_records: DEFAULT_CHANNEL_DELIVERY_RECEIPT_MAX_COMPACTION_RECORDS,
            force: false,
            now_ms,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ChannelDeliveryReceiptCompactionStatus {
    Missing,
    Unchanged,
    Compacted,
    /// The hot ledger remains over its configured hard bound, but every
    /// remaining row is active, legacy, malformed, or otherwise ineligible.
    /// This is an explicit operator signal, never a silent eviction.
    Backpressure,
    Busy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelDeliveryReceiptCompactionReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub receipts_file: PathBuf,
    pub history_manifest_file: PathBuf,
    pub status: ChannelDeliveryReceiptCompactionStatus,
    pub hot_before_bytes: u64,
    pub hot_after_bytes: u64,
    pub hot_before_records: usize,
    pub hot_after_records: usize,
    pub compacted_records: usize,
    pub compacted_delivery_ids: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cold_segment_file: Option<PathBuf>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ChannelDeliveryReceiptHistoryStorage {
    Hot,
    Cold,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelDeliveryReceiptHistoryEntry {
    pub delivery_id: String,
    pub storage: ChannelDeliveryReceiptHistoryStorage,
    pub line_number: usize,
    pub receipt: ChannelDeliveryReceipt,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelDeliveryReceiptHistoryQueryOptions {
    pub harness_home: PathBuf,
    /// Exact opaque v2 or legacy delivery identity. Set exactly one of this
    /// and `source_queue_id`.
    pub delivery_id: Option<String>,
    /// Exact originating runtime queue identity. The query remains indexed and
    /// bounded even when the corresponding receipts have moved cold.
    pub source_queue_id: Option<String>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelDeliveryReceiptHistoryQueryReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub entries: Vec<ChannelDeliveryReceiptHistoryEntry>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ChannelOutboxAppendOutcome {
    Appended,
    AlreadyPresent,
    AppendedIndexDeferred,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelOutboxAppendReport {
    pub outbox_file: PathBuf,
    pub delivery_id: String,
    pub outcome: ChannelOutboxAppendOutcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_warning: Option<String>,
}

/// Appends one outbound channel message through the canonical durable path.
///
/// The same JSONL append lock used by every ledger writer covers v2 delivery
/// ID assignment, serialization, and the physical append. A valid ID is
/// preserved; an absent or malformed ID is replaced before any bytes reach the
/// outbox, while legacy rows already present in the ledger remain untouched.
pub fn append_channel_outbox_message(
    harness_home: &Path,
    message: &mut ChannelOutboundMessage,
) -> io::Result<ChannelOutboxAppendReport> {
    let channel_dir = harness_home.join("state").join("channels");
    let outbox_file = channel_dir.join("outbox.jsonl");
    fs::create_dir_all(&channel_dir)?;
    let indexed = crate::logging::with_jsonl_append_lock(&outbox_file, || {
        crate::channel_delivery_index::append_channel_outbox_message_indexed_locked(
            &channel_dir,
            &outbox_file,
            message,
        )
    })?;
    let (outcome, index_warning) = match indexed.outcome {
        crate::channel_delivery_index::ChannelOutboxIndexedAppendOutcome::Appended => {
            (ChannelOutboxAppendOutcome::Appended, None)
        }
        crate::channel_delivery_index::ChannelOutboxIndexedAppendOutcome::AlreadyPresent => {
            (ChannelOutboxAppendOutcome::AlreadyPresent, None)
        }
        crate::channel_delivery_index::ChannelOutboxIndexedAppendOutcome::AppendedIndexDeferred {
            warning,
        } => (
            ChannelOutboxAppendOutcome::AppendedIndexDeferred,
            Some(warning),
        ),
    };
    if matches!(
        outcome,
        ChannelOutboxAppendOutcome::Appended | ChannelOutboxAppendOutcome::AppendedIndexDeferred
    ) && let Some(source_queue_id) = message
        .source_queue_id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        // The durable final outbox append is the relevant boundary here. A
        // busy diagnostic projection must never delay provider-visible final
        // delivery or cause the outbox write to be retried.
        let _ = crate::latency::record_latency_stage(
            crate::latency::latency_receipts_file(harness_home),
            source_queue_id,
            "final-outbox",
            crate::latency::LatencyStage::OutboxWrite,
            None,
        );
    }
    Ok(ChannelOutboxAppendReport {
        outbox_file,
        delivery_id: indexed.delivery_id,
        outcome,
        index_warning,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelDeliveryReceipt {
    pub schema: String,
    pub delivery_id: String,
    pub status: ChannelDeliveryStatus,
    pub platform: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_id: Option<String>,
    pub channel_id: String,
    pub user_id: String,
    pub session_key: String,
    pub provider_message_id: Option<String>,
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rendered_units: Vec<ChannelDeliveryRenderedUnitReceipt>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub presentation: Option<ChannelDeliveryPresentationReceipt>,
    pub at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelDeliveryPresentationReceipt {
    pub present: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_render_mode: Option<String>,
    pub fallback_reason: ChannelDeliveryPresentationFallbackReason,
    pub full_text_preserved: bool,
}

impl ChannelDeliveryPresentationReceipt {
    pub fn rendered(provider_render_mode: impl Into<String>, full_text_preserved: bool) -> Self {
        Self {
            present: true,
            provider_render_mode: Some(provider_render_mode.into()),
            fallback_reason: ChannelDeliveryPresentationFallbackReason::None,
            full_text_preserved,
        }
    }

    pub fn fallback(
        reason: ChannelDeliveryPresentationFallbackReason,
        provider_render_mode: impl Into<String>,
        full_text_preserved: bool,
    ) -> Self {
        Self {
            present: false,
            provider_render_mode: Some(provider_render_mode.into()),
            fallback_reason: reason,
            full_text_preserved,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ChannelDeliveryPresentationFallbackReason {
    None,
    Disabled,
    ValidationFailure,
    UnsupportedPlainBridge,
    ProviderFallback,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ChannelDeliveryStatus {
    Delivered,
    Failed,
    SkippedPermanent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelDeliveryRenderedUnitReceipt {
    pub unit_id: String,
    pub kind: ChannelDeliveryRenderedUnitKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attachment_kind: Option<ChannelOutboundAttachmentKind>,
    pub status: ChannelDeliveryUnitStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ChannelDeliveryRenderedUnitKind {
    Text,
    Media,
    ComponentAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ChannelDeliveryUnitStatus {
    Delivered,
    Failed,
    Skipped,
}

pub fn plan_channel_outbox(
    options: ChannelOutboxPlanOptions,
) -> io::Result<ChannelOutboxPlanReport> {
    let channel_dir = options.harness_home.join("state").join("channels");
    let outbox_file = channel_dir.join("outbox.jsonl");
    let receipts_file = channel_dir.join("delivery-receipts.jsonl");
    fs::create_dir_all(&channel_dir)?;

    let mut warnings = Vec::new();
    let indexed = crate::channel_delivery_index::plan_channel_outbox_index(
        &channel_dir,
        options.platform.as_deref(),
        options.limit,
        &mut warnings,
    )?;
    if !indexed.outbox_exists {
        warnings.push(format!(
            "channel outbox not found at {}",
            outbox_file.display()
        ));
    }

    Ok(ChannelOutboxPlanReport {
        schema: CHANNEL_OUTBOX_PLAN_SCHEMA,
        harness_home: options.harness_home,
        outbox_file,
        receipts_file,
        pending: indexed.pending,
        summary: indexed.summary,
        warnings,
    })
}

/// Compacts only complete, terminal histories for canonical v2 deliveries.
///
/// Normal planning and final delivery continue to use the SQLite sidecar; this
/// maintenance API never changes outbox rows and never makes a delivery
/// pending again.  A `Backpressure` result means the configured hot bound
/// cannot be met safely with the current active/legacy evidence.
pub fn compact_channel_delivery_receipts_if_needed(
    options: ChannelDeliveryReceiptCompactionOptions,
) -> io::Result<ChannelDeliveryReceiptCompactionReport> {
    crate::channel_delivery_index::compact_channel_delivery_receipts_indexed(options)
}

/// Returns receipt audit history for one exact delivery or source queue using
/// the sidecar index.  The query is bounded and reads cold JSONL only when a
/// new immutable segment must first be indexed or the sidecar is rebuilt.
pub fn query_channel_delivery_receipt_history(
    options: ChannelDeliveryReceiptHistoryQueryOptions,
) -> io::Result<ChannelDeliveryReceiptHistoryQueryReport> {
    let has_delivery_id = options
        .delivery_id
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty());
    let has_source_queue_id = options
        .source_queue_id
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty());
    if has_delivery_id == has_source_queue_id {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "channel delivery receipt history requires exactly one of delivery_id or source_queue_id",
        ));
    }
    let channel_dir = options.harness_home.join("state").join("channels");
    let mut warnings = Vec::new();
    let entries = crate::channel_delivery_index::query_channel_delivery_receipt_history_indexed(
        &channel_dir,
        options.delivery_id.as_deref(),
        options.source_queue_id.as_deref(),
        options.limit,
        &mut warnings,
    )?;
    Ok(ChannelDeliveryReceiptHistoryQueryReport {
        schema: CHANNEL_DELIVERY_RECEIPT_HISTORY_QUERY_SCHEMA,
        harness_home: options.harness_home,
        entries,
        warnings,
    })
}

pub fn record_channel_delivery(
    options: ChannelDeliveryRecordOptions,
) -> io::Result<ChannelDeliveryReceipt> {
    record_channel_delivery_for_source_queue(options, None)
}

/// Records a provider delivery receipt and, when the caller still has the
/// exact originating queue identity, records the matching final-delivery
/// latency stage without rediscovering it from a historical outbox ledger.
pub fn record_channel_delivery_for_source_queue(
    options: ChannelDeliveryRecordOptions,
    source_queue_id: Option<&str>,
) -> io::Result<ChannelDeliveryReceipt> {
    if options.status == ChannelDeliveryStatus::Delivered
        && options
            .rendered_units
            .iter()
            .any(|unit| unit.status != ChannelDeliveryUnitStatus::Delivered)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "cannot mark delivery delivered when a rendered unit is not delivered",
        ));
    }
    let channel_dir = options.harness_home.join("state").join("channels");
    fs::create_dir_all(&channel_dir)?;
    let receipt = ChannelDeliveryReceipt {
        schema: CHANNEL_DELIVERY_RECEIPT_SCHEMA.to_string(),
        delivery_id: options.delivery_id,
        status: options.status,
        platform: options.platform,
        account_id: options.account_id,
        channel_id: options.channel_id,
        user_id: options.user_id,
        session_key: options.session_key,
        provider_message_id: options.provider_message_id,
        error: options.error,
        rendered_units: options.rendered_units,
        presentation: options.presentation,
        at_ms: options.now_ms,
    };
    let mut warnings = Vec::new();
    let superseded_by_terminal = crate::channel_delivery_index::record_channel_delivery_receipt(
        &channel_dir,
        &receipt,
        &mut warnings,
    )?;
    if receipt.status == ChannelDeliveryStatus::Delivered
        && let Some(source_queue_id) = source_queue_id.filter(|value| !value.trim().is_empty())
    {
        // This point is reached only after the provider call has completed
        // and its durable audit receipt has been appended. The bounded,
        // best-effort projection preserves that observation without making a
        // final acknowledgement wait on historical outbox state.
        let _ = crate::latency::record_latency_stage(
            crate::latency::latency_receipts_file(&options.harness_home),
            source_queue_id,
            "channel-delivery",
            crate::latency::LatencyStage::DeliveryDone,
            Some(receipt.at_ms),
        );
    }
    if matches!(
        receipt.status,
        ChannelDeliveryStatus::Delivered | ChannelDeliveryStatus::SkippedPermanent
    ) {
        // A terminal delivery used to try compaction synchronously here.
        // That can take the delivery ledger lock and replay a large history
        // precisely while an interactive turn is trying to finish.  The
        // isolated owner performs the same source-authoritative maintenance
        // after this durable append instead.
        let _ = crate::request_ledger_maintenance(
            &options.harness_home,
            "terminal channel delivery receipt recorded",
        );
    }
    append_harness_log(
        &options.harness_home,
        &HarnessLogEvent::new(
            current_log_time_ms()?,
            match receipt.status {
                ChannelDeliveryStatus::Delivered => HarnessLogLevel::Info,
                ChannelDeliveryStatus::Failed => HarnessLogLevel::Warn,
                ChannelDeliveryStatus::SkippedPermanent => HarnessLogLevel::Info,
            },
            "channel",
            match receipt.status {
                ChannelDeliveryStatus::Delivered => "channel.delivery.delivered",
                ChannelDeliveryStatus::Failed => "channel.delivery.failed",
                ChannelDeliveryStatus::SkippedPermanent => "channel.delivery.skipped-permanent",
            },
            format!(
                "delivery {} recorded as {:?}",
                receipt.delivery_id, receipt.status
            ),
        )
        .session_key(Some(receipt.session_key.clone()))
        .channel(
            receipt.platform.clone(),
            receipt.channel_id.clone(),
            receipt.user_id.clone(),
        ),
    )?;
    if superseded_by_terminal {
        append_harness_log(
            &options.harness_home,
            &HarnessLogEvent::new(
                current_log_time_ms()?,
                HarnessLogLevel::Warn,
                "channel",
                "channel.delivery.failed-superseded-by-terminal",
                format!(
                    "retryable failed receipt for delivery {} was recorded for audit but superseded by an existing terminal receipt",
                    receipt.delivery_id
                ),
            )
            .session_key(Some(receipt.session_key.clone()))
            .channel(
                receipt.platform.clone(),
                receipt.channel_id.clone(),
                receipt.user_id.clone(),
            ),
        )?;
    }
    for warning in warnings {
        let _ = append_harness_log(
            &options.harness_home,
            &HarnessLogEvent::new(
                current_log_time_ms().unwrap_or(receipt.at_ms),
                HarnessLogLevel::Warn,
                "channel",
                "channel.delivery.receipt-index-warning",
                warning,
            )
            .session_key(Some(receipt.session_key.clone()))
            .channel(
                receipt.platform.clone(),
                receipt.channel_id.clone(),
                receipt.user_id.clone(),
            ),
        );
    }
    Ok(receipt)
}

pub(crate) fn delivery_id(line_number: usize, line: &str) -> String {
    format!("delivery:{line_number}:{}", fnv1a_64_hex(line))
}

/// Resolves the durable delivery identity for one parsed outbox row. New
/// canonical rows prefer their validated opaque v2 identity; legacy rows keep
/// the exact historical line-and-raw-bytes fallback unchanged.
pub(crate) fn delivery_id_for_outbox_message(
    line_number: usize,
    raw_line: &str,
    message: &ChannelOutboundMessage,
) -> String {
    crate::channel_runtime::valid_channel_outbound_delivery_id(message)
        .map(str::to_owned)
        .unwrap_or_else(|| delivery_id(line_number, raw_line))
}

/// Writes an already-canonical outbound row. The caller must hold the normal
/// outbox JSONL append lock; use [`append_channel_outbox_message`] externally.
pub(crate) fn append_channel_outbox_json_line_locked(
    outbox_file: &Path,
    message: &ChannelOutboundMessage,
) -> io::Result<()> {
    let mut line = Vec::new();
    if jsonl_needs_leading_newline(outbox_file)? {
        line.push(b'\n');
    }
    line.extend(serde_json::to_vec(message).map_err(io::Error::other)?);
    line.push(b'\n');
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(outbox_file)?;
    file.write_all(&line)?;
    file.flush()
}

fn jsonl_needs_leading_newline(path: &Path) -> io::Result<bool> {
    let mut file = match fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    if file.metadata()?.len() == 0 {
        return Ok(false);
    }
    file.seek(SeekFrom::End(-1))?;
    let mut last_byte = [0_u8; 1];
    file.read_exact(&mut last_byte)?;
    Ok(last_byte[0] != b'\n')
}

#[cfg(test)]
fn append_json_line(path: &Path, value: &impl Serialize) -> io::Result<()> {
    crate::append_jsonl_value(path, value)
}

fn fnv1a_64_hex(value: &str) -> String {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ChannelOutboundMessageKind, RichMessagePresentation, RichPresentationAtomicity,
        RichPresentationDeliveryPolicy, RichPresentationLinkPreview,
        latency::{LatencyStage, latency_receipts_file, read_latest_queue_receipt},
    };
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    #[test]
    fn outbox_plan_filters_delivered_and_retries_failed() {
        let root = temp_root("outbox_plan_filters_delivered_and_retries_failed");
        let harness_home = root.join(".agent-harness");
        let outbox_file = harness_home
            .join("state")
            .join("channels")
            .join("outbox.jsonl");
        let first = message("telegram", "dm-1", "user-1", "session-1", "one");
        let second = message("telegram", "dm-2", "user-2", "session-2", "two");
        let third = message("discord", "dm-3", "user-3", "session-3", "three");
        append_json_line(&outbox_file, &first).unwrap();
        append_json_line(&outbox_file, &second).unwrap();
        append_json_line(&outbox_file, &third).unwrap();

        let initial = plan_channel_outbox(ChannelOutboxPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            limit: 10,
        })
        .unwrap();
        assert_eq!(initial.pending.len(), 2);
        record_channel_delivery(ChannelDeliveryRecordOptions {
            harness_home: harness_home.clone(),
            delivery_id: initial.pending[0].delivery_id.clone(),
            status: ChannelDeliveryStatus::Delivered,
            platform: "telegram".to_string(),
            account_id: None,
            channel_id: "dm-1".to_string(),
            user_id: "user-1".to_string(),
            session_key: "session-1".to_string(),
            provider_message_id: Some("tg-1".to_string()),
            error: None,
            now_ms: 1234,
            rendered_units: Vec::new(),
            presentation: None,
        })
        .unwrap();
        record_channel_delivery(ChannelDeliveryRecordOptions {
            harness_home: harness_home.clone(),
            delivery_id: initial.pending[1].delivery_id.clone(),
            status: ChannelDeliveryStatus::Failed,
            platform: "telegram".to_string(),
            account_id: None,
            channel_id: "dm-2".to_string(),
            user_id: "user-2".to_string(),
            session_key: "session-2".to_string(),
            provider_message_id: None,
            error: Some("rate limited".to_string()),
            now_ms: 1235,
            rendered_units: Vec::new(),
            presentation: None,
        })
        .unwrap();

        let retry = plan_channel_outbox(ChannelOutboxPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            limit: 10,
        })
        .unwrap();
        assert_eq!(retry.pending.len(), 1);
        assert_eq!(retry.pending[0].message.text, "two");
        assert_eq!(retry.pending[0].attempts, 1);
        assert_eq!(
            retry.pending[0].last_status,
            Some(ChannelDeliveryStatus::Failed)
        );
        assert_eq!(retry.summary.delivered, 1);
        assert_eq!(retry.summary.failed_retryable, 1);
        let log = fs::read_to_string(
            harness_home
                .join("state")
                .join("logs")
                .join("harness.jsonl"),
        )
        .unwrap();
        assert!(log.contains("channel.delivery.delivered"));
        assert!(log.contains("channel.delivery.failed"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn canonical_outbox_append_assigns_and_persists_a_v2_delivery_id() {
        let root = temp_root("canonical_outbox_append_assigns_and_persists_a_v2_delivery_id");
        let harness_home = root.join(".agent-harness");
        let mut outbound = message("discord", "dm-1", "user-1", "session-1", "hello");
        outbound.source_queue_id = Some("queue-outbox-latency".to_string());

        let append = append_channel_outbox_message(&harness_home, &mut outbound).unwrap();
        let outbox_file = append.outbox_file.clone();

        let assigned = outbound
            .delivery_id
            .as_deref()
            .expect("canonical append assigns a delivery ID");
        assert!(crate::channel_runtime::valid_channel_outbound_delivery_id(&outbound).is_some());
        assert_eq!(append.delivery_id.as_str(), assigned);
        assert_eq!(append.outcome, ChannelOutboxAppendOutcome::Appended);
        assert!(append.index_warning.is_none());
        assert_eq!(
            outbox_file,
            harness_home
                .join("state")
                .join("channels")
                .join("outbox.jsonl")
        );
        let persisted: ChannelOutboundMessage =
            serde_json::from_str(fs::read_to_string(&outbox_file).unwrap().trim()).unwrap();
        assert_eq!(persisted.delivery_id.as_deref(), Some(assigned));
        let latency =
            read_latest_queue_receipt(latency_receipts_file(&harness_home), "queue-outbox-latency")
                .unwrap()
                .expect("a source-bound final outbox append records latency");
        assert!(latency.stages.contains_key(&LatencyStage::OutboxWrite));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn source_bound_final_delivery_records_delivery_done_latency() {
        let root = temp_root("source_bound_final_delivery_records_delivery_done_latency");
        let harness_home = root.join(".agent-harness");

        let receipt = record_channel_delivery_for_source_queue(
            ChannelDeliveryRecordOptions {
                harness_home: harness_home.clone(),
                delivery_id: "delivery:latency-final:1".to_string(),
                status: ChannelDeliveryStatus::Delivered,
                platform: "discord".to_string(),
                account_id: None,
                channel_id: "dm-1".to_string(),
                user_id: "user-1".to_string(),
                session_key: "session-1".to_string(),
                provider_message_id: Some("provider-final-1".to_string()),
                error: None,
                now_ms: 3_456,
                rendered_units: Vec::new(),
                presentation: None,
            },
            Some("queue-final-latency"),
        )
        .unwrap();
        assert_eq!(receipt.status, ChannelDeliveryStatus::Delivered);

        let latency =
            read_latest_queue_receipt(latency_receipts_file(&harness_home), "queue-final-latency")
                .unwrap()
                .expect("a delivered source-bound final records latency");
        assert_eq!(
            latency.stages.get(&LatencyStage::DeliveryDone).copied(),
            Some(3_456)
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn canonical_outbox_append_preserves_a_message_objects_v2_delivery_id_on_reappend() {
        let root = temp_root(
            "canonical_outbox_append_preserves_a_message_objects_v2_delivery_id_on_reappend",
        );
        let harness_home = root.join(".agent-harness");
        let mut outbound = message("discord", "dm-1", "user-1", "session-1", "hello");

        let first = append_channel_outbox_message(&harness_home, &mut outbound).unwrap();
        let outbox_file = first.outbox_file.clone();
        let assigned = outbound.delivery_id.clone().unwrap();
        let retry = append_channel_outbox_message(&harness_home, &mut outbound).unwrap();

        assert_eq!(outbound.delivery_id.as_deref(), Some(assigned.as_str()));
        assert_eq!(first.outcome, ChannelOutboxAppendOutcome::Appended);
        assert_eq!(retry.outcome, ChannelOutboxAppendOutcome::AlreadyPresent);
        assert_eq!(retry.delivery_id, assigned);
        assert!(retry.index_warning.is_none());
        let persisted: Vec<ChannelOutboundMessage> = fs::read_to_string(outbox_file)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(persisted.len(), 1);
        assert_eq!(persisted[0].delivery_id.as_deref(), Some(assigned.as_str()));
        let plan = plan_channel_outbox(ChannelOutboxPlanOptions {
            harness_home,
            platform: Some("discord".to_string()),
            limit: 10,
        })
        .unwrap();
        assert_eq!(plan.summary.pending, 1);
        assert_eq!(plan.pending.len(), 1);
        assert_eq!(plan.pending[0].delivery_id, assigned);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn canonical_outbox_append_retry_recovers_after_source_before_index_tail() {
        let root =
            temp_root("canonical_outbox_append_retry_recovers_after_source_before_index_tail");
        let harness_home = root.join(".agent-harness");
        let mut outbound = message("telegram", "dm-1", "user-1", "session-1", "hello");

        crate::channel_delivery_index::defer_next_outbox_index_tail_for_test();
        let first = append_channel_outbox_message(&harness_home, &mut outbound).unwrap();
        let outbox_file = first.outbox_file.clone();
        let delivery_id = outbound.delivery_id.clone().unwrap();
        assert_eq!(
            first.outcome,
            ChannelOutboxAppendOutcome::AppendedIndexDeferred
        );
        assert!(first.index_warning.is_some());
        assert_eq!(fs::read_to_string(&outbox_file).unwrap().lines().count(), 1);

        let retry = append_channel_outbox_message(&harness_home, &mut outbound).unwrap();
        assert_eq!(retry.outcome, ChannelOutboxAppendOutcome::AlreadyPresent);
        assert!(retry.index_warning.is_none());
        assert_eq!(fs::read_to_string(&outbox_file).unwrap().lines().count(), 1);
        let plan = plan_channel_outbox(ChannelOutboxPlanOptions {
            harness_home,
            platform: Some("telegram".to_string()),
            limit: 10,
        })
        .unwrap();
        assert_eq!(plan.summary.pending, 1);
        assert_eq!(plan.pending.len(), 1);
        assert_eq!(plan.pending[0].delivery_id, delivery_id);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn canonical_outbox_append_replaces_a_malformed_supplied_delivery_id() {
        let root = temp_root("canonical_outbox_append_replaces_a_malformed_supplied_delivery_id");
        let harness_home = root.join(".agent-harness");
        let mut outbound = message("telegram", "dm-1", "user-1", "session-1", "hello");
        outbound.delivery_id = Some("not-a-v2-delivery-id".to_string());

        let append = append_channel_outbox_message(&harness_home, &mut outbound).unwrap();
        let outbox_file = append.outbox_file.clone();

        let assigned = outbound.delivery_id.as_deref().unwrap();
        assert_ne!(assigned, "not-a-v2-delivery-id");
        assert!(crate::channel_runtime::valid_channel_outbound_delivery_id(&outbound).is_some());
        assert_eq!(append.outcome, ChannelOutboxAppendOutcome::Appended);
        let persisted: ChannelOutboundMessage =
            serde_json::from_str(fs::read_to_string(outbox_file).unwrap().trim()).unwrap();
        assert_eq!(persisted.delivery_id.as_deref(), Some(assigned));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn canonical_outbox_append_keeps_legacy_rows_readable_with_their_original_fallback() {
        let root = temp_root(
            "canonical_outbox_append_keeps_legacy_rows_readable_with_their_original_fallback",
        );
        let harness_home = root.join(".agent-harness");
        let outbox_file = harness_home
            .join("state")
            .join("channels")
            .join("outbox.jsonl");
        let legacy_raw = r#"{"platform":"telegram","channelId":"dm-legacy","userId":"user-legacy","sessionKey":"session-legacy","kind":"agent-reply","text":"legacy payload"}"#;
        fs::create_dir_all(outbox_file.parent().unwrap()).unwrap();
        fs::write(&outbox_file, format!("{legacy_raw}\n")).unwrap();
        let mut outbound = message("telegram", "dm-v2", "user-v2", "session-v2", "new payload");

        let append = append_channel_outbox_message(&harness_home, &mut outbound).unwrap();
        assert_eq!(append.outcome, ChannelOutboxAppendOutcome::Appended);

        let plan = plan_channel_outbox(ChannelOutboxPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            limit: 10,
        })
        .unwrap();
        let legacy = plan
            .pending
            .iter()
            .find(|pending| pending.message.text == "legacy payload")
            .expect("legacy row remains readable");
        assert_eq!(legacy.delivery_id, delivery_id(1, legacy_raw));
        assert_eq!(
            fs::read_to_string(outbox_file).unwrap().lines().next(),
            Some(legacy_raw)
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn v2_delivery_id_survives_outbox_content_and_line_relocation() {
        let root = temp_root("v2_delivery_id_survives_outbox_content_and_line_relocation");
        let harness_home = root.join(".agent-harness");
        let outbox_file = harness_home
            .join("state")
            .join("channels")
            .join("outbox.jsonl");
        let stable_id = "delivery:v2:0123456789abcdef0123456789abcdef";
        let first = message(
            "telegram",
            "dm-1",
            "user-1",
            "session-1",
            "original payload",
        );
        fs::create_dir_all(outbox_file.parent().unwrap()).unwrap();
        fs::write(
            &outbox_file,
            format!(
                "{}\n",
                serialized_message_with_delivery_id(&first, stable_id)
            ),
        )
        .unwrap();

        let initial = plan_channel_outbox(ChannelOutboxPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            limit: 10,
        })
        .unwrap();
        assert_eq!(initial.pending[0].delivery_id, stable_id);

        let sibling = message("telegram", "dm-1", "user-1", "session-1", "new first row");
        let relocated = message(
            "telegram",
            "dm-1",
            "user-1",
            "session-1",
            "relocated and edited payload",
        );
        fs::write(
            &outbox_file,
            format!(
                "{}\n{}\n",
                serde_json::to_string(&sibling).unwrap(),
                serialized_message_with_delivery_id(&relocated, stable_id),
            ),
        )
        .unwrap();

        let relocated_plan = plan_channel_outbox(ChannelOutboxPlanOptions {
            harness_home,
            platform: Some("telegram".to_string()),
            limit: 10,
        })
        .unwrap();
        let relocated_pending = relocated_plan
            .pending
            .iter()
            .find(|pending| pending.message.text == "relocated and edited payload")
            .expect("relocated v2 outbox row remains pending");
        assert_eq!(relocated_pending.delivery_id, stable_id);
        assert_eq!(relocated_pending.line_number, 2);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn legacy_delivery_id_remains_the_exact_line_and_bytes_fallback() {
        let root = temp_root("legacy_delivery_id_remains_the_exact_line_and_bytes_fallback");
        let harness_home = root.join(".agent-harness");
        let outbox_file = harness_home
            .join("state")
            .join("channels")
            .join("outbox.jsonl");
        let raw = r#"{"platform":"telegram","channelId":"dm-legacy","userId":"user-legacy","sessionKey":"session-legacy","kind":"agent-reply","text":"legacy payload"}"#;
        fs::create_dir_all(outbox_file.parent().unwrap()).unwrap();
        fs::write(&outbox_file, format!("{raw}\n")).unwrap();

        let plan = plan_channel_outbox(ChannelOutboxPlanOptions {
            harness_home,
            platform: Some("telegram".to_string()),
            limit: 10,
        })
        .unwrap();
        assert_eq!(delivery_id(1, raw), "delivery:1:d89125706c04e584");
        assert_eq!(plan.pending[0].delivery_id, "delivery:1:d89125706c04e584");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn delivery_index_migrates_legacy_receipts_for_mixed_legacy_and_v2_outbox_rows() {
        let root = temp_root(
            "delivery_index_migrates_legacy_receipts_for_mixed_legacy_and_v2_outbox_rows",
        );
        let harness_home = root.join(".agent-harness");
        let channel_dir = harness_home.join("state").join("channels");
        let outbox_file = channel_dir.join("outbox.jsonl");
        let legacy = message("discord", "dm-1", "user-1", "session-1", "legacy");
        let v2 = message("discord", "dm-1", "user-1", "session-1", "v2");
        let v2_id = "delivery:v2:fedcba98765432100123456789abcdef";
        append_json_line(&outbox_file, &legacy).unwrap();
        let v2_raw = serialized_message_with_delivery_id(&v2, v2_id);
        {
            let mut file = fs::OpenOptions::new()
                .append(true)
                .open(&outbox_file)
                .unwrap();
            writeln!(file, "{v2_raw}").unwrap();
        }

        let initial = plan_channel_outbox(ChannelOutboxPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("discord".to_string()),
            limit: 10,
        })
        .unwrap();
        assert_eq!(initial.pending.len(), 2);
        let legacy_pending = initial
            .pending
            .iter()
            .find(|pending| pending.message.text == "legacy")
            .expect("legacy row is planned");
        assert_eq!(
            legacy_pending.delivery_id,
            delivery_id(1, &serde_json::to_string(&legacy).unwrap())
        );
        let v2_pending = initial
            .pending
            .iter()
            .find(|pending| pending.message.text == "v2")
            .expect("v2 row is planned");
        assert_eq!(v2_pending.delivery_id, v2_id);

        record_channel_delivery(ChannelDeliveryRecordOptions {
            harness_home: harness_home.clone(),
            delivery_id: delivery_id(2, &v2_raw),
            status: ChannelDeliveryStatus::Delivered,
            platform: "discord".to_string(),
            account_id: None,
            channel_id: "dm-1".to_string(),
            user_id: "user-1".to_string(),
            session_key: "session-1".to_string(),
            provider_message_id: Some("dc-v2".to_string()),
            error: None,
            now_ms: 1,
            rendered_units: Vec::new(),
            presentation: None,
        })
        .unwrap();
        fs::remove_file(channel_dir.join("delivery-state.sqlite")).unwrap();

        let rebuilt = plan_channel_outbox(ChannelOutboxPlanOptions {
            harness_home,
            platform: Some("discord".to_string()),
            limit: 10,
        })
        .unwrap();
        assert_eq!(rebuilt.summary.delivered, 1);
        assert_eq!(rebuilt.pending.len(), 1);
        assert_eq!(rebuilt.pending[0].message.text, "legacy");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn outbox_plan_materializes_a_restart_safe_delivery_state_index() {
        let root = temp_root("outbox_plan_materializes_a_restart_safe_delivery_state_index");
        let harness_home = root.join(".agent-harness");
        let channel_dir = harness_home.join("state").join("channels");
        append_json_line(
            &channel_dir.join("outbox.jsonl"),
            &message("telegram", "dm-1", "user-1", "session-1", "one"),
        )
        .unwrap();

        let initial = plan_channel_outbox(ChannelOutboxPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            limit: 10,
        })
        .unwrap();
        assert_eq!(initial.pending.len(), 1);
        assert!(channel_dir.join("delivery-state.sqlite").is_file());

        record_channel_delivery(ChannelDeliveryRecordOptions {
            harness_home: harness_home.clone(),
            delivery_id: initial.pending[0].delivery_id.clone(),
            status: ChannelDeliveryStatus::Delivered,
            platform: "telegram".to_string(),
            account_id: None,
            channel_id: "dm-1".to_string(),
            user_id: "user-1".to_string(),
            session_key: "session-1".to_string(),
            provider_message_id: Some("tg-1".to_string()),
            error: None,
            now_ms: 1234,
            rendered_units: Vec::new(),
            presentation: None,
        })
        .unwrap();

        let after_delivery = plan_channel_outbox(ChannelOutboxPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            limit: 10,
        })
        .unwrap();
        assert!(after_delivery.pending.is_empty());
        assert_eq!(after_delivery.summary.delivered, 1);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn outbox_delivery_index_rebuilds_after_outbox_rewrite() {
        let root = temp_root("outbox_delivery_index_rebuilds_after_outbox_rewrite");
        let harness_home = root.join(".agent-harness");
        let outbox_file = harness_home
            .join("state")
            .join("channels")
            .join("outbox.jsonl");
        append_json_line(
            &outbox_file,
            &message("telegram", "dm-1", "user-1", "session-1", "old payload"),
        )
        .unwrap();
        let initial = plan_channel_outbox(ChannelOutboxPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            limit: 10,
        })
        .unwrap();
        record_channel_delivery(ChannelDeliveryRecordOptions {
            harness_home: harness_home.clone(),
            delivery_id: initial.pending[0].delivery_id.clone(),
            status: ChannelDeliveryStatus::Delivered,
            platform: "telegram".to_string(),
            account_id: None,
            channel_id: "dm-1".to_string(),
            user_id: "user-1".to_string(),
            session_key: "session-1".to_string(),
            provider_message_id: Some("tg-old".to_string()),
            error: None,
            now_ms: 1234,
            rendered_units: Vec::new(),
            presentation: None,
        })
        .unwrap();

        let replacement = message(
            "telegram",
            "dm-1",
            "user-1",
            "session-1",
            "replacement payload with a distinct delivery id",
        );
        fs::write(
            &outbox_file,
            format!("{}\n", serde_json::to_string(&replacement).unwrap()),
        )
        .unwrap();

        let rebuilt = plan_channel_outbox(ChannelOutboxPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            limit: 10,
        })
        .unwrap();
        assert_eq!(rebuilt.summary.total_outbox_lines, 1);
        assert_eq!(rebuilt.summary.delivered, 0);
        assert_eq!(rebuilt.summary.pending, 1);
        assert_eq!(rebuilt.pending[0].message.text, replacement.text);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn delivery_state_lookup_scopes_source_queue_to_the_full_lane() {
        let root = temp_root("delivery_state_lookup_scopes_source_queue_to_the_full_lane");
        let harness_home = root.join(".agent-harness");
        let channel_dir = harness_home.join("state").join("channels");
        let outbox_file = channel_dir.join("outbox.jsonl");
        let mut matching = message("discord", "dm-1", "user-1", "session-1", "matching");
        matching.source_queue_id = Some("turn:shared".to_string());
        let mut other_session = message("discord", "dm-1", "user-1", "session-2", "other");
        other_session.source_queue_id = Some("turn:shared".to_string());
        let mut other_channel = message("discord", "dm-2", "user-1", "session-1", "other channel");
        other_channel.source_queue_id = Some("turn:shared".to_string());
        append_json_line(&outbox_file, &matching).unwrap();
        append_json_line(&outbox_file, &other_session).unwrap();
        append_json_line(&outbox_file, &other_channel).unwrap();

        plan_channel_outbox(ChannelOutboxPlanOptions {
            harness_home: harness_home.clone(),
            platform: None,
            limit: 10,
        })
        .unwrap();

        let states =
            crate::channel_delivery_index::channel_delivery_states_for_source_queue_in_lane(
                &channel_dir,
                "turn:shared",
                "discord",
                None,
                "dm-1",
                "user-1",
                "session-1",
                &mut Vec::new(),
            )
            .unwrap();
        assert_eq!(states.len(), 1);
        assert_eq!(states[0].message.text, "matching");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn delivery_state_lookup_keeps_continuation_sessions_in_the_same_physical_lane() {
        let root = temp_root(
            "delivery_state_lookup_keeps_continuation_sessions_in_the_same_physical_lane",
        );
        let harness_home = root.join(".agent-harness");
        let channel_dir = harness_home.join("state").join("channels");
        let outbox_file = channel_dir.join("outbox.jsonl");
        let mut root_session = message("discord", "dm-1", "user-1", "session-root", "root");
        root_session.source_queue_id = Some("turn:shared".to_string());
        let mut continuation = message(
            "discord",
            "dm-1",
            "user-1",
            "session-continuation:1",
            "continuation",
        );
        continuation.source_queue_id = Some("turn:shared".to_string());
        let mut other_lane = message("discord", "dm-2", "user-1", "session-root", "other");
        other_lane.source_queue_id = Some("turn:shared".to_string());
        append_json_line(&outbox_file, &root_session).unwrap();
        append_json_line(&outbox_file, &continuation).unwrap();
        append_json_line(&outbox_file, &other_lane).unwrap();

        plan_channel_outbox(ChannelOutboxPlanOptions {
            harness_home,
            platform: None,
            limit: 10,
        })
        .unwrap();

        let states =
            crate::channel_delivery_index::channel_delivery_states_for_source_queue_in_channel(
                &channel_dir,
                "turn:shared",
                "discord",
                None,
                "dm-1",
                "user-1",
                &mut Vec::new(),
            )
            .unwrap();
        assert_eq!(states.len(), 2);
        assert_eq!(states[0].message.text, "root");
        assert_eq!(states[1].message.text, "continuation");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn outbox_plan_uses_last_committed_index_while_an_outbox_append_is_locked() {
        let root =
            temp_root("outbox_plan_uses_last_committed_index_while_an_outbox_append_is_locked");
        let harness_home = root.join(".agent-harness");
        let outbox_file = harness_home
            .join("state")
            .join("channels")
            .join("outbox.jsonl");
        append_json_line(
            &outbox_file,
            &message("discord", "dm-1", "user-1", "session-1", "first"),
        )
        .unwrap();
        let initial = plan_channel_outbox(ChannelOutboxPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("discord".to_string()),
            limit: 10,
        })
        .unwrap();
        assert_eq!(initial.summary.pending, 1);

        let second = message("discord", "dm-1", "user-1", "session-1", "second");
        crate::logging::with_jsonl_append_lock(&outbox_file, || {
            let mut file = fs::OpenOptions::new().append(true).open(&outbox_file)?;
            writeln!(file, "{}", serde_json::to_string(&second).unwrap())?;
            file.flush()?;

            let blocked = plan_channel_outbox(ChannelOutboxPlanOptions {
                harness_home: harness_home.clone(),
                platform: Some("discord".to_string()),
                limit: 10,
            })?;
            assert_eq!(blocked.summary.pending, 1);
            assert!(
                blocked
                    .warnings
                    .iter()
                    .any(|warning| warning.contains("using last committed delivery state"))
            );
            Ok(())
        })
        .unwrap();

        let refreshed = plan_channel_outbox(ChannelOutboxPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("discord".to_string()),
            limit: 10,
        })
        .unwrap();
        assert_eq!(refreshed.summary.pending, 2);
        assert_eq!(refreshed.pending[1].message.text, "second");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn outbox_plan_uses_committed_index_after_one_hundred_thousand_delivery_receipts() {
        let root = temp_root(
            "outbox_plan_uses_committed_index_after_one_hundred_thousand_delivery_receipts",
        );
        let harness_home = root.join(".agent-harness");
        let mut outbound = message("discord", "dm-1", "user-1", "session-1", "final");
        let append = append_channel_outbox_message(&harness_home, &mut outbound).unwrap();
        let receipt = ChannelDeliveryReceipt {
            schema: CHANNEL_DELIVERY_RECEIPT_SCHEMA.to_string(),
            delivery_id: append.delivery_id,
            status: ChannelDeliveryStatus::Delivered,
            platform: outbound.platform.clone(),
            account_id: outbound.account_id.clone(),
            channel_id: outbound.channel_id.clone(),
            user_id: outbound.user_id.clone(),
            session_key: outbound.session_key.clone(),
            provider_message_id: Some("provider-1".to_string()),
            error: None,
            rendered_units: Vec::new(),
            presentation: None,
            at_ms: 1_000,
        };
        let receipts_file = harness_home
            .join("state")
            .join("channels")
            .join("delivery-receipts.jsonl");
        let receipt_line = format!("{}\n", serde_json::to_string(&receipt).unwrap());
        fs::write(&receipts_file, receipt_line.repeat(100_000)).unwrap();

        let initial = plan_channel_outbox(ChannelOutboxPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("discord".to_string()),
            limit: 10,
        })
        .unwrap();
        assert_eq!(initial.summary.delivered, 1);
        assert!(initial.pending.is_empty());

        let started = Instant::now();
        let current = plan_channel_outbox(ChannelOutboxPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("discord".to_string()),
            limit: 10,
        })
        .unwrap();
        assert_eq!(current.summary.delivered, 1);
        assert!(current.pending.is_empty());
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "a current delivery index must plan without replaying 100k receipt rows"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn outbox_plan_treats_permanent_skip_as_terminal_not_delivered() {
        let root = temp_root("outbox_plan_treats_permanent_skip_as_terminal_not_delivered");
        let harness_home = root.join(".agent-harness");
        let outbox_file = harness_home
            .join("state")
            .join("channels")
            .join("outbox.jsonl");
        append_json_line(
            &outbox_file,
            &message(
                "telegram",
                "dm-1",
                "user-1",
                "session-1",
                "suppressed evidence payload",
            ),
        )
        .unwrap();

        let initial = plan_channel_outbox(ChannelOutboxPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            limit: 10,
        })
        .unwrap();
        assert_eq!(initial.pending.len(), 1);
        record_channel_delivery(ChannelDeliveryRecordOptions {
            harness_home: harness_home.clone(),
            delivery_id: initial.pending[0].delivery_id.clone(),
            status: ChannelDeliveryStatus::SkippedPermanent,
            platform: "telegram".to_string(),
            account_id: None,
            channel_id: "dm-1".to_string(),
            user_id: "user-1".to_string(),
            session_key: "session-1".to_string(),
            provider_message_id: None,
            error: Some("suppressed invalid final-surface row".to_string()),
            now_ms: 1234,
            rendered_units: Vec::new(),
            presentation: None,
        })
        .unwrap();

        let terminal = plan_channel_outbox(ChannelOutboxPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            limit: 10,
        })
        .unwrap();
        assert!(terminal.pending.is_empty());
        assert_eq!(terminal.summary.pending, 0);
        assert_eq!(terminal.summary.delivered, 0);
        assert_eq!(terminal.summary.failed_retryable, 0);
        assert_eq!(terminal.summary.skipped_permanent, 1);

        let receipt_text = fs::read_to_string(
            harness_home
                .join("state")
                .join("channels")
                .join("delivery-receipts.jsonl"),
        )
        .unwrap();
        assert!(receipt_text.contains("\"status\":\"skipped-permanent\""));
        let log = fs::read_to_string(
            harness_home
                .join("state")
                .join("logs")
                .join("harness.jsonl"),
        )
        .unwrap();
        assert!(log.contains("channel.delivery.skipped-permanent"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn terminal_delivery_receipt_outranks_later_retryable_failed_receipt() {
        let root = temp_root("terminal_delivery_receipt_outranks_later_retryable_failed_receipt");
        let harness_home = root.join(".agent-harness");
        let outbox_file = harness_home
            .join("state")
            .join("channels")
            .join("outbox.jsonl");
        append_json_line(
            &outbox_file,
            &message("telegram", "dm-1", "user-1", "session-1", "overlong final"),
        )
        .unwrap();

        let initial = plan_channel_outbox(ChannelOutboxPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            limit: 10,
        })
        .unwrap();
        assert_eq!(initial.pending.len(), 1);
        let delivery_id = initial.pending[0].delivery_id.clone();

        record_channel_delivery(ChannelDeliveryRecordOptions {
            harness_home: harness_home.clone(),
            delivery_id: delivery_id.clone(),
            status: ChannelDeliveryStatus::SkippedPermanent,
            platform: "telegram".to_string(),
            account_id: None,
            channel_id: "dm-1".to_string(),
            user_id: "user-1".to_string(),
            session_key: "session-1".to_string(),
            provider_message_id: None,
            error: Some("manual terminal skip for provider permanent failure".to_string()),
            now_ms: 1234,
            rendered_units: Vec::new(),
            presentation: None,
        })
        .unwrap();
        record_channel_delivery(ChannelDeliveryRecordOptions {
            harness_home: harness_home.clone(),
            delivery_id,
            status: ChannelDeliveryStatus::Failed,
            platform: "telegram".to_string(),
            account_id: None,
            channel_id: "dm-1".to_string(),
            user_id: "user-1".to_string(),
            session_key: "session-1".to_string(),
            provider_message_id: None,
            error: Some("late in-flight retryable sender failure".to_string()),
            now_ms: 1235,
            rendered_units: Vec::new(),
            presentation: None,
        })
        .unwrap();

        let terminal = plan_channel_outbox(ChannelOutboxPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            limit: 10,
        })
        .unwrap();

        assert!(terminal.pending.is_empty());
        assert_eq!(terminal.summary.pending, 0);
        assert_eq!(terminal.summary.skipped_permanent, 1);
        assert_eq!(terminal.summary.failed_retryable, 0);

        let log = fs::read_to_string(
            harness_home
                .join("state")
                .join("logs")
                .join("harness.jsonl"),
        )
        .unwrap();
        assert!(log.contains("terminal receipt"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn outbox_plan_limit_only_caps_pending_details() {
        let root = temp_root("outbox_plan_limit_only_caps_pending_details");
        let harness_home = root.join(".agent-harness");
        let outbox_file = harness_home
            .join("state")
            .join("channels")
            .join("outbox.jsonl");
        for index in 1..=5 {
            append_json_line(
                &outbox_file,
                &message(
                    "discord",
                    &format!("dm-{index}"),
                    &format!("user-{index}"),
                    &format!("session-{index}"),
                    &format!("message {index}"),
                ),
            )
            .unwrap();
        }

        let initial = plan_channel_outbox(ChannelOutboxPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("discord".to_string()),
            limit: 10,
        })
        .unwrap();
        record_channel_delivery(ChannelDeliveryRecordOptions {
            harness_home: harness_home.clone(),
            delivery_id: initial.pending[0].delivery_id.clone(),
            status: ChannelDeliveryStatus::Delivered,
            platform: "discord".to_string(),
            account_id: None,
            channel_id: "dm-1".to_string(),
            user_id: "user-1".to_string(),
            session_key: "session-1".to_string(),
            provider_message_id: Some("dc-1".to_string()),
            error: None,
            now_ms: 1234,
            rendered_units: Vec::new(),
            presentation: None,
        })
        .unwrap();

        let limited = plan_channel_outbox(ChannelOutboxPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("discord".to_string()),
            limit: 2,
        })
        .unwrap();
        assert_eq!(limited.pending.len(), 2);
        assert_eq!(limited.summary.total_outbox_lines, 5);
        assert_eq!(limited.summary.delivered, 1);
        assert_eq!(limited.summary.pending, 4);
        assert_eq!(limited.pending[0].message.text, "message 2");
        assert_eq!(limited.pending[1].message.text, "message 3");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn rich_delivery_receipt_records_units_and_retries_partial_failure() {
        let root = temp_root("rich_delivery_receipt_records_units_and_retries_partial_failure");
        let harness_home = root.join(".agent-harness");
        let outbox_file = harness_home
            .join("state")
            .join("channels")
            .join("outbox.jsonl");
        let mut outbound = message("telegram", "dm-1", "user-1", "session-1", "fallback");
        outbound.presentation = Some(rich_presentation());
        append_json_line(&outbox_file, &outbound).unwrap();

        let initial = plan_channel_outbox(ChannelOutboxPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            limit: 10,
        })
        .unwrap();
        assert_eq!(initial.pending.len(), 1);

        record_channel_delivery(ChannelDeliveryRecordOptions {
            harness_home: harness_home.clone(),
            delivery_id: initial.pending[0].delivery_id.clone(),
            status: ChannelDeliveryStatus::Failed,
            platform: "telegram".to_string(),
            account_id: None,
            channel_id: "dm-1".to_string(),
            user_id: "user-1".to_string(),
            session_key: "session-1".to_string(),
            provider_message_id: Some("100,101".to_string()),
            error: Some("media unit failed".to_string()),
            now_ms: 1234,
            rendered_units: vec![
                ChannelDeliveryRenderedUnitReceipt {
                    unit_id: "text:0".to_string(),
                    kind: ChannelDeliveryRenderedUnitKind::Text,
                    attachment_kind: None,
                    status: ChannelDeliveryUnitStatus::Delivered,
                    provider_message_id: Some("100".to_string()),
                    error: None,
                },
                ChannelDeliveryRenderedUnitReceipt {
                    unit_id: "media:0".to_string(),
                    kind: ChannelDeliveryRenderedUnitKind::Media,
                    attachment_kind: Some(ChannelOutboundAttachmentKind::Image),
                    status: ChannelDeliveryUnitStatus::Failed,
                    provider_message_id: None,
                    error: Some("upload failed".to_string()),
                },
            ],
            presentation: Some(ChannelDeliveryPresentationReceipt::rendered(
                "telegram:parse_mode=HTML",
                true,
            )),
        })
        .unwrap();

        let retry = plan_channel_outbox(ChannelOutboxPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            limit: 10,
        })
        .unwrap();
        assert_eq!(retry.pending.len(), 1);
        assert_eq!(retry.summary.delivered, 0);
        assert_eq!(retry.summary.failed_retryable, 1);
        assert_eq!(retry.summary.partial_failed, 1);

        let receipt_text = fs::read_to_string(
            harness_home
                .join("state")
                .join("channels")
                .join("delivery-receipts.jsonl"),
        )
        .unwrap();
        assert!(receipt_text.contains("\"renderedUnits\""));
        assert!(receipt_text.contains("\"unitId\":\"media:0\""));
        assert!(receipt_text.contains("\"status\":\"failed\""));
        assert!(receipt_text.contains("\"presentation\""));
        assert!(receipt_text.contains("\"present\":true"));
        assert!(receipt_text.contains("\"providerRenderMode\":\"telegram:parse_mode=HTML\""));
        assert!(receipt_text.contains("\"fallbackReason\":\"none\""));
        assert!(receipt_text.contains("\"fullTextPreserved\":true"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn delivery_receipt_without_presentation_field_stays_readable() {
        let json = r#"{
          "schema": "agent-harness.channel-delivery-receipt.v1",
          "deliveryId": "delivery:1:legacy",
          "status": "delivered",
          "platform": "telegram",
          "channelId": "dm-1",
          "userId": "user-1",
          "sessionKey": "telegram:dm-1:user-1:main",
          "providerMessageId": "100",
          "error": null,
          "atMs": 1234
        }"#;

        let receipt: ChannelDeliveryReceipt = serde_json::from_str(json).unwrap();

        assert_eq!(receipt.status, ChannelDeliveryStatus::Delivered);
        assert!(receipt.rendered_units.is_empty());
        assert!(receipt.presentation.is_none());
    }

    #[test]
    fn rich_delivery_rejects_delivered_receipt_when_any_unit_failed() {
        let root = temp_root("rich_delivery_rejects_delivered_receipt_when_any_unit_failed");
        let harness_home = root.join(".agent-harness");
        let error = record_channel_delivery(ChannelDeliveryRecordOptions {
            harness_home: harness_home.clone(),
            delivery_id: "delivery:1:test".to_string(),
            status: ChannelDeliveryStatus::Delivered,
            platform: "discord".to_string(),
            account_id: None,
            channel_id: "dm-1".to_string(),
            user_id: "user-1".to_string(),
            session_key: "session-1".to_string(),
            provider_message_id: Some("200".to_string()),
            error: None,
            now_ms: 1234,
            rendered_units: vec![ChannelDeliveryRenderedUnitReceipt {
                unit_id: "component-action:approve".to_string(),
                kind: ChannelDeliveryRenderedUnitKind::ComponentAction,
                attachment_kind: None,
                status: ChannelDeliveryUnitStatus::Failed,
                provider_message_id: None,
                error: Some("components disabled".to_string()),
            }],
            presentation: None,
        })
        .unwrap_err();
        assert!(error.to_string().contains("cannot mark delivery delivered"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn terminal_v2_receipts_compact_to_cold_without_losing_exact_delivery_or_source_history() {
        let root = temp_root(
            "terminal_v2_receipts_compact_to_cold_without_losing_exact_delivery_or_source_history",
        );
        let harness_home = root.join(".agent-harness");
        let channel_dir = harness_home.join("state").join("channels");

        let mut delivered = message("discord", "dm-1", "user-1", "session-1", "terminal");
        delivered.source_queue_id = Some("queue:terminal".to_string());
        let delivered_append =
            append_channel_outbox_message(&harness_home, &mut delivered).unwrap();
        let delivered_id = delivered_append.delivery_id;

        let mut active = message("discord", "dm-1", "user-1", "session-1", "active");
        active.source_queue_id = Some("queue:active".to_string());
        append_channel_outbox_message(&harness_home, &mut active).unwrap();

        let mut legacy = message("discord", "dm-1", "user-1", "session-1", "legacy");
        legacy.source_queue_id = Some("queue:legacy".to_string());
        append_json_line(&channel_dir.join("outbox.jsonl"), &legacy).unwrap();

        let initial = plan_channel_outbox(ChannelOutboxPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("discord".to_string()),
            limit: 10,
        })
        .unwrap();
        let legacy_id = initial
            .pending
            .iter()
            .find(|pending| pending.message.text == "legacy")
            .unwrap()
            .delivery_id
            .clone();

        record_test_delivery(
            &harness_home,
            &delivered_id,
            ChannelDeliveryStatus::Failed,
            1,
        );
        record_test_delivery(
            &harness_home,
            &delivered_id,
            ChannelDeliveryStatus::Delivered,
            2,
        );
        record_test_delivery(
            &harness_home,
            &legacy_id,
            ChannelDeliveryStatus::Delivered,
            3,
        );

        let compacted =
            compact_channel_delivery_receipts_if_needed(ChannelDeliveryReceiptCompactionOptions {
                harness_home: harness_home.clone(),
                max_hot_receipt_bytes: u64::MAX,
                target_hot_receipt_bytes: 0,
                max_hot_receipt_records: usize::MAX,
                target_hot_receipt_records: 0,
                max_compaction_records: 100,
                force: true,
                now_ms: 4,
            })
            .unwrap();
        assert_eq!(
            compacted.status,
            ChannelDeliveryReceiptCompactionStatus::Compacted
        );
        assert_eq!(compacted.compacted_records, 2);
        assert_eq!(compacted.compacted_delivery_ids, 1);
        let hot = fs::read_to_string(&compacted.receipts_file).unwrap();
        assert!(!hot.contains(&delivered_id));
        assert!(hot.contains(&legacy_id));
        let cold = fs::read_to_string(compacted.cold_segment_file.as_ref().unwrap()).unwrap();
        assert!(cold.contains(&delivered_id));

        let exact =
            query_channel_delivery_receipt_history(ChannelDeliveryReceiptHistoryQueryOptions {
                harness_home: harness_home.clone(),
                delivery_id: Some(delivered_id.clone()),
                source_queue_id: None,
                limit: 10,
            })
            .unwrap();
        assert_eq!(exact.entries.len(), 2);
        assert!(exact.entries.iter().all(|entry| {
            entry.storage == ChannelDeliveryReceiptHistoryStorage::Cold
                && entry.delivery_id == delivered_id
        }));
        let by_source =
            query_channel_delivery_receipt_history(ChannelDeliveryReceiptHistoryQueryOptions {
                harness_home: harness_home.clone(),
                delivery_id: None,
                source_queue_id: Some("queue:terminal".to_string()),
                limit: 10,
            })
            .unwrap();
        assert_eq!(by_source.entries.len(), 2);
        assert!(
            by_source
                .entries
                .iter()
                .all(|entry| entry.storage == ChannelDeliveryReceiptHistoryStorage::Cold)
        );

        let legacy_history =
            query_channel_delivery_receipt_history(ChannelDeliveryReceiptHistoryQueryOptions {
                harness_home: harness_home.clone(),
                delivery_id: Some(legacy_id.clone()),
                source_queue_id: None,
                limit: 10,
            })
            .unwrap();
        assert_eq!(legacy_history.entries.len(), 1);
        assert_eq!(
            legacy_history.entries[0].storage,
            ChannelDeliveryReceiptHistoryStorage::Hot
        );

        let terminal_states =
            crate::channel_delivery_index::channel_delivery_states_for_source_queue_blocking(
                &channel_dir,
                "queue:terminal",
                &mut Vec::new(),
            )
            .unwrap();
        assert_eq!(terminal_states.len(), 1);
        assert_eq!(
            terminal_states[0].terminal_status,
            Some(ChannelDeliveryStatus::Delivered)
        );
        let after = plan_channel_outbox(ChannelOutboxPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("discord".to_string()),
            limit: 10,
        })
        .unwrap();
        assert_eq!(after.pending.len(), 1);
        assert_eq!(after.pending[0].message.text, "active");
        assert_eq!(after.summary.delivered, 2);

        let repeated =
            compact_channel_delivery_receipts_if_needed(ChannelDeliveryReceiptCompactionOptions {
                harness_home: harness_home.clone(),
                max_hot_receipt_bytes: u64::MAX,
                target_hot_receipt_bytes: 0,
                max_hot_receipt_records: usize::MAX,
                target_hot_receipt_records: 0,
                max_compaction_records: 100,
                force: true,
                now_ms: 5,
            })
            .unwrap();
        assert_eq!(
            repeated.status,
            ChannelDeliveryReceiptCompactionStatus::Unchanged
        );
        assert_eq!(
            crate::channel_delivery_history::read_channel_delivery_receipt_history(&channel_dir)
                .unwrap()
                .segments
                .len(),
            1
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn receipt_retention_reports_backpressure_without_evicting_legacy_terminal_evidence() {
        let root = temp_root(
            "receipt_retention_reports_backpressure_without_evicting_legacy_terminal_evidence",
        );
        let harness_home = root.join(".agent-harness");
        let outbox_file = harness_home
            .join("state")
            .join("channels")
            .join("outbox.jsonl");
        append_json_line(
            &outbox_file,
            &message("discord", "dm-1", "user-1", "session-1", "legacy-only"),
        )
        .unwrap();
        let plan = plan_channel_outbox(ChannelOutboxPlanOptions {
            harness_home: harness_home.clone(),
            platform: None,
            limit: 10,
        })
        .unwrap();
        record_test_delivery(
            &harness_home,
            &plan.pending[0].delivery_id,
            ChannelDeliveryStatus::Delivered,
            1,
        );
        let before = fs::read_to_string(
            harness_home
                .join("state")
                .join("channels")
                .join("delivery-receipts.jsonl"),
        )
        .unwrap();
        let report =
            compact_channel_delivery_receipts_if_needed(ChannelDeliveryReceiptCompactionOptions {
                harness_home: harness_home.clone(),
                max_hot_receipt_bytes: 1,
                target_hot_receipt_bytes: 0,
                max_hot_receipt_records: 1,
                target_hot_receipt_records: 0,
                max_compaction_records: 100,
                force: false,
                now_ms: 2,
            })
            .unwrap();
        assert_eq!(
            report.status,
            ChannelDeliveryReceiptCompactionStatus::Backpressure
        );
        assert_eq!(report.compacted_records, 0);
        assert_eq!(fs::read_to_string(report.receipts_file).unwrap(), before);

        let _ = fs::remove_dir_all(root);
    }

    fn record_test_delivery(
        harness_home: &Path,
        delivery_id: &str,
        status: ChannelDeliveryStatus,
        now_ms: i64,
    ) {
        record_channel_delivery(ChannelDeliveryRecordOptions {
            harness_home: harness_home.to_path_buf(),
            delivery_id: delivery_id.to_string(),
            status,
            platform: "discord".to_string(),
            account_id: None,
            channel_id: "dm-1".to_string(),
            user_id: "user-1".to_string(),
            session_key: "session-1".to_string(),
            provider_message_id: None,
            error: None,
            now_ms,
            rendered_units: Vec::new(),
            presentation: None,
        })
        .unwrap();
    }

    fn message(
        platform: &str,
        channel_id: &str,
        user_id: &str,
        session_key: &str,
        text: &str,
    ) -> ChannelOutboundMessage {
        ChannelOutboundMessage {
            platform: platform.to_string(),
            account_id: None,
            channel_id: channel_id.to_string(),
            user_id: user_id.to_string(),
            session_key: session_key.to_string(),
            delivery_id: None,
            kind: ChannelOutboundMessageKind::AgentReply,
            source_queue_id: None,
            source_completion_file: None,
            text: text.to_string(),
            presentation: None,
            delivery_intent: None,
            attachments: Vec::new(),
        }
    }

    fn serialized_message_with_delivery_id(
        message: &ChannelOutboundMessage,
        delivery_id: &str,
    ) -> String {
        let mut value = serde_json::to_value(message).unwrap();
        value["deliveryId"] = serde_json::Value::String(delivery_id.to_string());
        serde_json::to_string(&value).unwrap()
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-channel-delivery-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }

    fn rich_presentation() -> RichMessagePresentation {
        RichMessagePresentation {
            schema: crate::RICH_MESSAGE_PRESENTATION_SCHEMA.to_string(),
            fallback_text: "fallback".to_string(),
            blocks: Vec::new(),
            actions: Vec::new(),
            media: vec![crate::RichPresentationMediaRef {
                attachment_index: Some(0),
                artifact_ref: None,
                caption: Some("caption".to_string()),
                role: Some("primary".to_string()),
            }],
            link_preview: RichPresentationLinkPreview::default(),
            delivery_policy: RichPresentationDeliveryPolicy {
                atomicity: RichPresentationAtomicity::AllOrTerminal,
                allow_fallback_text: true,
            },
        }
    }

    #[test]
    fn rendered_presentation_receipt_keeps_explicit_preservation_proof() {
        let proven = ChannelDeliveryPresentationReceipt::rendered(
            "discord:safe-markdown;allowed_mentions.parse=[]",
            true,
        );
        let unproven = ChannelDeliveryPresentationReceipt::rendered(
            "discord:safe-markdown;allowed_mentions.parse=[]",
            false,
        );

        assert!(proven.present);
        assert!(proven.full_text_preserved);
        assert!(unproven.present);
        assert!(!unproven.full_text_preserved);
        assert_eq!(
            serde_json::to_value(unproven).unwrap()["fullTextPreserved"],
            false
        );
    }
}
