use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::{
    ChannelOutboundAttachmentKind, ChannelOutboundMessage, HarnessLogEvent, HarnessLogLevel,
    append_harness_log, current_log_time_ms,
};

const CHANNEL_OUTBOX_PLAN_SCHEMA: &str = "agent-harness.channel-outbox-plan.v1";
const CHANNEL_DELIVERY_RECEIPT_SCHEMA: &str = "agent-harness.channel-delivery-receipt.v1";

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
    pub fn rendered(provider_render_mode: impl Into<String>) -> Self {
        Self {
            present: true,
            provider_render_mode: Some(provider_render_mode.into()),
            fallback_reason: ChannelDeliveryPresentationFallbackReason::None,
            full_text_preserved: true,
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
    let receipts = read_delivery_receipts(&receipts_file, &mut warnings)?;
    let mut pending = Vec::new();
    let mut summary = ChannelOutboxPlanSummary::default();

    if !outbox_file.is_file() {
        warnings.push(format!(
            "channel outbox not found at {}",
            outbox_file.display()
        ));
        return Ok(ChannelOutboxPlanReport {
            schema: CHANNEL_OUTBOX_PLAN_SCHEMA,
            harness_home: options.harness_home,
            outbox_file,
            receipts_file,
            pending,
            summary,
            warnings,
        });
    }

    let text = fs::read_to_string(&outbox_file)?;
    for (index, line) in text.lines().enumerate() {
        let line_number = index + 1;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        summary.total_outbox_lines += 1;
        let message: ChannelOutboundMessage = match serde_json::from_str(trimmed) {
            Ok(message) => message,
            Err(error) => {
                summary.invalid_lines += 1;
                warnings.push(format!(
                    "channel outbox line {line_number} is not valid JSON: {error}"
                ));
                continue;
            }
        };
        if options
            .platform
            .as_ref()
            .is_some_and(|platform| platform != &message.platform)
        {
            summary.skipped_platform += 1;
            continue;
        }
        let delivery_id = delivery_id(line_number, trimmed);
        let attempts = receipts.get(&delivery_id).map_or(0, Vec::len);
        let records = receipts.get(&delivery_id).map(Vec::as_slice).unwrap_or(&[]);
        let last_status = delivery_terminal_status(records)
            .or_else(|| records.last().map(|receipt| receipt.status));
        let pending_status = match last_status {
            Some(ChannelDeliveryStatus::Delivered) => {
                summary.delivered += 1;
                false
            }
            Some(ChannelDeliveryStatus::SkippedPermanent) => {
                summary.skipped_permanent += 1;
                false
            }
            Some(ChannelDeliveryStatus::Failed) => {
                summary.failed_retryable += 1;
                if records
                    .last()
                    .is_some_and(|receipt| receipt.has_partial_failure())
                {
                    summary.partial_failed += 1;
                }
                true
            }
            None => true,
        };
        if pending_status {
            summary.pending += 1;
            if pending.len() < options.limit {
                pending.push(ChannelDeliveryPending {
                    delivery_id,
                    line_number,
                    attempts,
                    last_status,
                    message,
                });
            }
        }
    }

    Ok(ChannelOutboxPlanReport {
        schema: CHANNEL_OUTBOX_PLAN_SCHEMA,
        harness_home: options.harness_home,
        outbox_file,
        receipts_file,
        pending,
        summary,
        warnings,
    })
}

pub fn record_channel_delivery(
    options: ChannelDeliveryRecordOptions,
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
    let receipts_file = channel_dir.join("delivery-receipts.jsonl");
    fs::create_dir_all(&channel_dir)?;
    let mut warnings = Vec::new();
    let superseded_by_terminal = options.status == ChannelDeliveryStatus::Failed
        && read_delivery_receipts(&receipts_file, &mut warnings)?
            .get(&options.delivery_id)
            .is_some_and(|records| delivery_terminal_status(records).is_some());
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
    append_json_line(&receipts_file, &receipt)?;
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
    Ok(receipt)
}

fn delivery_terminal_status(records: &[ChannelDeliveryReceipt]) -> Option<ChannelDeliveryStatus> {
    records
        .iter()
        .rev()
        .find_map(|receipt| match receipt.status {
            ChannelDeliveryStatus::Delivered | ChannelDeliveryStatus::SkippedPermanent => {
                Some(receipt.status)
            }
            ChannelDeliveryStatus::Failed => None,
        })
}

fn read_delivery_receipts(
    receipts_file: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<HashMap<String, Vec<ChannelDeliveryReceipt>>> {
    let mut receipts = HashMap::<String, Vec<ChannelDeliveryReceipt>>::new();
    if !receipts_file.is_file() {
        return Ok(receipts);
    }
    let text = fs::read_to_string(receipts_file)?;
    for (index, line) in text.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let receipt: ChannelDeliveryReceipt = match serde_json::from_str(trimmed) {
            Ok(receipt) => receipt,
            Err(error) => {
                warnings.push(format!(
                    "delivery receipt line {} is not valid JSON: {}",
                    index + 1,
                    error
                ));
                continue;
            }
        };
        receipts
            .entry(receipt.delivery_id.clone())
            .or_default()
            .push(receipt);
    }
    Ok(receipts)
}

fn delivery_id(line_number: usize, line: &str) -> String {
    format!("delivery:{line_number}:{}", fnv1a_64_hex(line))
}

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
    };
    use std::time::{SystemTime, UNIX_EPOCH};

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
            kind: ChannelOutboundMessageKind::AgentReply,
            source_queue_id: None,
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
}

impl ChannelDeliveryReceipt {
    fn has_partial_failure(&self) -> bool {
        !self.rendered_units.is_empty()
            && self
                .rendered_units
                .iter()
                .any(|unit| unit.status != ChannelDeliveryUnitStatus::Delivered)
    }
}
