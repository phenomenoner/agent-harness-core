use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json;

use crate::logging::{append_jsonl_value, current_log_time_ms};

const LATENCY_RECEIPT_SCHEMA: &str = "agent-harness.runtime-queue-latency.v1";
const DEFAULT_LATENCY_RECEIPTS_FILE: &str = "latency-receipts.jsonl";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum LatencyStage {
    InboundReceived,
    TurnPlanned,
    RuntimeEnqueued,
    CapacityFirstSeen,
    LeaseAcquired,
    PromptBundleStart,
    PromptBundleDone,
    CodexPlanStart,
    CodexPlanDone,
    CodexSpawnStart,
    CodexSpawnDone,
    ThreadStartRequest,
    ThreadStartDone,
    FirstCodexEvent,
    FirstProgressEvent,
    FinalCodexEvent,
    OutboxWrite,
    DeliveryAttempt,
    DeliveryDone,
}

pub const DEFAULT_LATENCY_STAGE_ORDER: [LatencyStage; 19] = [
    LatencyStage::InboundReceived,
    LatencyStage::TurnPlanned,
    LatencyStage::RuntimeEnqueued,
    LatencyStage::CapacityFirstSeen,
    LatencyStage::LeaseAcquired,
    LatencyStage::PromptBundleStart,
    LatencyStage::PromptBundleDone,
    LatencyStage::CodexPlanStart,
    LatencyStage::CodexPlanDone,
    LatencyStage::CodexSpawnStart,
    LatencyStage::CodexSpawnDone,
    LatencyStage::ThreadStartRequest,
    LatencyStage::ThreadStartDone,
    LatencyStage::FirstCodexEvent,
    LatencyStage::FirstProgressEvent,
    LatencyStage::FinalCodexEvent,
    LatencyStage::OutboxWrite,
    LatencyStage::DeliveryAttempt,
    LatencyStage::DeliveryDone,
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LatencyReceipt {
    pub schema: String,
    pub queue_id: String,
    pub lane: String,
    #[serde(rename = "updatedAtMs")]
    pub updated_at_ms: i64,
    pub stages: BTreeMap<LatencyStage, i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LatencyDelta {
    pub from_stage: LatencyStage,
    pub to_stage: LatencyStage,
    pub delta_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LatencySummary {
    pub queue_id: String,
    pub lane: String,
    pub deltas: Vec<LatencyDelta>,
    pub missing_stages: Vec<LatencyStage>,
    pub total_ms: Option<i64>,
}

pub fn latency_receipts_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("runtime-queue")
        .join(DEFAULT_LATENCY_RECEIPTS_FILE)
}

pub fn record_latency_stage(
    receipts_file: impl AsRef<Path>,
    queue_id: &str,
    lane: &str,
    stage: LatencyStage,
    at_ms: Option<i64>,
) -> io::Result<LatencyReceipt> {
    let receipts_file = receipts_file.as_ref();
    if let Some(parent) = receipts_file.parent() {
        fs::create_dir_all(parent)?;
    }

    let at_ms = at_ms.unwrap_or_else(|| current_log_time_ms().unwrap_or(0));
    let mut receipt =
        read_latest_queue_receipt(receipts_file, queue_id)?.unwrap_or_else(|| LatencyReceipt {
            schema: LATENCY_RECEIPT_SCHEMA.to_string(),
            queue_id: queue_id.to_string(),
            lane: lane.to_string(),
            updated_at_ms: at_ms,
            stages: BTreeMap::new(),
        });
    receipt.lane = lane.to_string();
    receipt.updated_at_ms = at_ms;

    match receipt.stages.get(&stage) {
        Some(existing) if *existing <= at_ms => {}
        _ => {
            receipt.stages.insert(stage, at_ms);
        }
    }

    append_jsonl_value(receipts_file, &receipt)?;
    Ok(receipt)
}

pub fn read_latest_queue_receipt(
    receipts_file: impl AsRef<Path>,
    queue_id: &str,
) -> io::Result<Option<LatencyReceipt>> {
    let receipts_file = receipts_file.as_ref();
    if !receipts_file.is_file() {
        return Ok(None);
    }

    let text = fs::read_to_string(receipts_file)?;
    let mut latest: Option<LatencyReceipt> = None;
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        if let Ok(item) = serde_json::from_str::<LatencyReceipt>(line) {
            if item.queue_id == queue_id {
                latest = Some(item);
            }
        }
    }
    Ok(latest)
}

pub fn latency_summary(
    receipt: &LatencyReceipt,
    expected_stages: &[LatencyStage],
) -> LatencySummary {
    let mut deltas = Vec::new();
    let mut present = Vec::new();

    for stage in expected_stages {
        if let Some(at_ms) = receipt.stages.get(stage) {
            present.push((*stage, *at_ms));
        }
    }

    for window in present.windows(2) {
        let (from_stage, from_ms) = window[0];
        let (to_stage, to_ms) = window[1];
        deltas.push(LatencyDelta {
            from_stage,
            to_stage,
            delta_ms: to_ms.saturating_sub(from_ms),
        });
    }

    let missing_stages = expected_stages
        .iter()
        .copied()
        .filter(|stage| !receipt.stages.contains_key(stage))
        .collect::<Vec<_>>();

    let total_ms = present.first().and_then(|first| {
        present
            .last()
            .map(|(_, last_ms)| last_ms.saturating_sub(first.1))
    });

    LatencySummary {
        queue_id: receipt.queue_id.clone(),
        lane: receipt.lane.clone(),
        deltas,
        missing_stages,
        total_ms,
    }
}

pub fn default_latency_stages() -> &'static [LatencyStage] {
    &DEFAULT_LATENCY_STAGE_ORDER
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;

    #[test]
    fn latency_summary_reports_ordered_deltas() {
        let root = temp_root("latency_summary_reports_ordered_deltas");
        let file = latency_receipts_file(root.join("harness"));

        let _ = record_latency_stage(
            &file,
            "queue-1",
            "runtime",
            LatencyStage::InboundReceived,
            Some(1_000),
        )
        .unwrap();
        let _ = record_latency_stage(
            &file,
            "queue-1",
            "runtime",
            LatencyStage::TurnPlanned,
            Some(1_150),
        )
        .unwrap();
        let _ = record_latency_stage(
            &file,
            "queue-1",
            "runtime",
            LatencyStage::RuntimeEnqueued,
            Some(1_175),
        )
        .unwrap();

        let receipt = read_latest_queue_receipt(&file, "queue-1")
            .unwrap()
            .unwrap();
        let summary = latency_summary(&receipt, default_latency_stages());

        assert_eq!(summary.queue_id, "queue-1");
        assert_eq!(summary.lane, "runtime");
        assert_eq!(summary.deltas.len(), 2);
        assert_eq!(summary.deltas[0].from_stage, LatencyStage::InboundReceived);
        assert_eq!(summary.deltas[0].to_stage, LatencyStage::TurnPlanned);
        assert_eq!(summary.deltas[0].delta_ms, 150);
        assert_eq!(summary.deltas[1].from_stage, LatencyStage::TurnPlanned);
        assert_eq!(summary.deltas[1].to_stage, LatencyStage::RuntimeEnqueued);
        assert_eq!(summary.deltas[1].delta_ms, 25);
        assert_eq!(summary.total_ms, Some(175));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn latency_summary_reports_missing_stages() {
        let root = temp_root("latency_summary_reports_missing_stages");
        let file = latency_receipts_file(root.join("harness"));

        let _ = record_latency_stage(
            &file,
            "queue-2",
            "runtime",
            LatencyStage::InboundReceived,
            Some(500),
        )
        .unwrap();
        let _ = record_latency_stage(
            &file,
            "queue-2",
            "runtime",
            LatencyStage::OutboxWrite,
            Some(700),
        )
        .unwrap();

        let receipt = read_latest_queue_receipt(&file, "queue-2")
            .unwrap()
            .unwrap();
        let summary = latency_summary(&receipt, default_latency_stages());

        assert!(summary.missing_stages.contains(&LatencyStage::TurnPlanned));
        assert!(
            summary
                .missing_stages
                .contains(&LatencyStage::LeaseAcquired)
        );
        assert_eq!(
            summary.missing_stages.len(),
            default_latency_stages().len() - 2
        );
        assert_eq!(summary.deltas.len(), 1);
        assert_eq!(summary.total_ms, Some(200));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn latency_record_updates_stage_timestamps() {
        let root = temp_root("latency_record_updates_stage_timestamps");
        let file = latency_receipts_file(root.join("harness"));

        let first = record_latency_stage(
            &file,
            "queue-3",
            "runtime",
            LatencyStage::InboundReceived,
            Some(1),
        )
        .unwrap();
        let second = record_latency_stage(
            &file,
            "queue-3",
            "runtime",
            LatencyStage::InboundReceived,
            Some(5),
        )
        .unwrap();

        assert_eq!(first.stages[&LatencyStage::InboundReceived], 1);
        assert_eq!(second.stages[&LatencyStage::InboundReceived], 1);
        let _ = fs::remove_dir_all(root);
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-latency-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
