use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json;

use crate::logging::current_log_time_ms;

const LATENCY_RECEIPT_SCHEMA: &str = "agent-harness.runtime-queue-latency.v1";
const DEFAULT_LATENCY_RECEIPTS_FILE: &str = "latency-receipts.jsonl";
const LATENCY_INDEX_FILE: &str = "latency-receipts-index.sqlite";
const LATENCY_INDEX_BUSY_TIMEOUT: Duration = Duration::from_millis(50);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum LatencyStage {
    InboundReceived,
    TurnPlanned,
    RuntimeEnqueued,
    QueueAccepted,
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
    FirstProviderProgressSurface,
    WorkingIndicator,
    FinalCodexEvent,
    TerminalEvent,
    OutboxWrite,
    DeliveryAttempt,
    DeliveryDone,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MonotonicLatencySpan {
    PromptBundlePreparation,
    CodexPlanning,
    CodexExecution,
    Terminalization,
}

pub const DEFAULT_LATENCY_STAGE_ORDER: [LatencyStage; 23] = [
    LatencyStage::InboundReceived,
    LatencyStage::TurnPlanned,
    LatencyStage::RuntimeEnqueued,
    LatencyStage::QueueAccepted,
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
    LatencyStage::FirstProviderProgressSurface,
    LatencyStage::WorkingIndicator,
    LatencyStage::FinalCodexEvent,
    LatencyStage::TerminalEvent,
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
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub derived_stage_sources: BTreeMap<LatencyStage, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub monotonic_spans: BTreeMap<MonotonicLatencySpan, MonotonicLatencySpanReceipt>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MonotonicLatencySpanReceipt {
    pub duration_ms: u64,
    pub recorded_at_ms: i64,
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
    pub out_of_order_deltas: Vec<LatencyDelta>,
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
    let at_ms = at_ms.unwrap_or_else(|| current_log_time_ms().unwrap_or(0));
    let mut connection = open_latency_index_for_write(receipts_file)?;
    {
        let transaction = connection.transaction().map_err(io::Error::other)?;
        transaction
            .execute(
                "INSERT INTO latency_queue_receipts (queue_id, lane, updated_at_ms) \
                 VALUES (?1, ?2, ?3) \
                 ON CONFLICT(queue_id) DO UPDATE SET \
                   lane = CASE \
                     WHEN latency_queue_receipts.lane = '' THEN excluded.lane \
                     ELSE latency_queue_receipts.lane \
                   END, \
                   updated_at_ms = MAX(latency_queue_receipts.updated_at_ms, excluded.updated_at_ms)",
                params![queue_id, lane, at_ms],
            )
            .map_err(io::Error::other)?;
        transaction
            .execute(
                "INSERT INTO latency_stage_receipts (queue_id, stage, at_ms) \
                 VALUES (?1, ?2, ?3) \
                 ON CONFLICT(queue_id, stage) DO UPDATE SET \
                   at_ms = MIN(latency_stage_receipts.at_ms, excluded.at_ms)",
                params![queue_id, latency_stage_key(stage), at_ms],
            )
            .map_err(io::Error::other)?;
        transaction.commit().map_err(io::Error::other)?;
    }

    read_latest_latency_receipt_from_connection(&connection, queue_id)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("latency receipt disappeared for queue `{queue_id}` after write"),
        )
    })
}

pub fn record_monotonic_latency_span(
    receipts_file: impl AsRef<Path>,
    queue_id: &str,
    lane: &str,
    span: MonotonicLatencySpan,
    duration: Duration,
) -> io::Result<LatencyReceipt> {
    let receipts_file = receipts_file.as_ref();
    let recorded_at_ms = current_log_time_ms().unwrap_or(0);
    let duration_ms = duration.as_millis().min(i64::MAX as u128) as i64;
    let mut connection = open_latency_index_for_write(receipts_file)?;
    {
        let transaction = connection.transaction().map_err(io::Error::other)?;
        transaction
            .execute(
                "INSERT INTO latency_queue_receipts (queue_id, lane, updated_at_ms) \
                 VALUES (?1, ?2, ?3) \
                 ON CONFLICT(queue_id) DO UPDATE SET \
                   lane = CASE \
                     WHEN latency_queue_receipts.lane = '' THEN excluded.lane \
                     ELSE latency_queue_receipts.lane \
                   END, \
                   updated_at_ms = MAX(latency_queue_receipts.updated_at_ms, excluded.updated_at_ms)",
                params![queue_id, lane, recorded_at_ms],
            )
            .map_err(io::Error::other)?;
        transaction
            .execute(
                "INSERT INTO latency_span_receipts \
                   (queue_id, span, duration_ms, recorded_at_ms) \
                 VALUES (?1, ?2, ?3, ?4) \
                 ON CONFLICT(queue_id, span) DO NOTHING",
                params![
                    queue_id,
                    monotonic_latency_span_key(span),
                    duration_ms,
                    recorded_at_ms
                ],
            )
            .map_err(io::Error::other)?;
        transaction.commit().map_err(io::Error::other)?;
    }

    read_latest_latency_receipt_from_connection(&connection, queue_id)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("latency receipt disappeared for queue `{queue_id}` after span write"),
        )
    })
}

pub fn read_latest_queue_receipt(
    receipts_file: impl AsRef<Path>,
    queue_id: &str,
) -> io::Result<Option<LatencyReceipt>> {
    let receipts_file = receipts_file.as_ref();
    if let Some(mut receipt) = read_latest_latency_receipt_from_index(receipts_file, queue_id)? {
        enrich_delivery_done_from_durable_receipts(receipts_file, &mut receipt);
        return Ok(Some(receipt));
    }

    // Compatibility-only fallback for old diagnostic journals. Writers never
    // take this branch: interactive timestamping is backed by the bounded
    // SQLite projection above, so it cannot replay this historical JSONL.
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
    if let Some(receipt) = latest.as_mut() {
        enrich_delivery_done_from_durable_receipts(receipts_file, receipt);
    }
    Ok(latest)
}

fn enrich_delivery_done_from_durable_receipts(receipts_file: &Path, receipt: &mut LatencyReceipt) {
    if receipt.stages.contains_key(&LatencyStage::DeliveryDone) {
        return;
    }
    let Some(harness_home) = latency_harness_home(receipts_file) else {
        return;
    };
    let channel_dir = harness_home.join("state").join("channels");
    let mut warnings = Vec::new();
    let Ok(Some(delivered_at_ms)) =
        crate::channel_delivery_index::latest_delivered_at_ms_for_source_queue(
            &channel_dir,
            &receipt.queue_id,
            &mut warnings,
        )
    else {
        return;
    };
    receipt
        .stages
        .insert(LatencyStage::DeliveryDone, delivered_at_ms);
    receipt.derived_stage_sources.insert(
        LatencyStage::DeliveryDone,
        "channel-delivery-receipt-index".to_string(),
    );
    receipt.updated_at_ms = receipt.updated_at_ms.max(delivered_at_ms);
}

fn latency_harness_home(receipts_file: &Path) -> Option<&Path> {
    (receipts_file.file_name()?.to_str()? == DEFAULT_LATENCY_RECEIPTS_FILE).then_some(())?;
    let runtime_queue = receipts_file.parent()?;
    (runtime_queue.file_name()?.to_str()? == "runtime-queue").then_some(())?;
    let state = runtime_queue.parent()?;
    (state.file_name()?.to_str()? == "state").then_some(())?;
    state.parent()
}

fn latency_index_file(receipts_file: &Path) -> PathBuf {
    receipts_file.with_file_name(LATENCY_INDEX_FILE)
}

fn open_latency_index_for_write(receipts_file: &Path) -> io::Result<Connection> {
    let index_file = latency_index_file(receipts_file);
    if let Some(parent) = index_file.parent() {
        fs::create_dir_all(parent)?;
    }
    let connection = Connection::open(&index_file).map_err(io::Error::other)?;
    connection
        .busy_timeout(LATENCY_INDEX_BUSY_TIMEOUT)
        .map_err(io::Error::other)?;
    connection
        .execute_batch(
            "CREATE TABLE IF NOT EXISTS latency_queue_receipts (\
                queue_id TEXT PRIMARY KEY NOT NULL,\
                lane TEXT NOT NULL,\
                updated_at_ms INTEGER NOT NULL\
            );\
            CREATE TABLE IF NOT EXISTS latency_stage_receipts (\
                queue_id TEXT NOT NULL,\
                stage TEXT NOT NULL,\
                at_ms INTEGER NOT NULL,\
                PRIMARY KEY(queue_id, stage)\
            );\
            CREATE INDEX IF NOT EXISTS latency_stage_receipts_by_queue \
                ON latency_stage_receipts(queue_id, stage);\
            CREATE TABLE IF NOT EXISTS latency_span_receipts (\
                queue_id TEXT NOT NULL,\
                span TEXT NOT NULL,\
                duration_ms INTEGER NOT NULL,\
                recorded_at_ms INTEGER NOT NULL,\
                PRIMARY KEY(queue_id, span)\
            );\
            CREATE INDEX IF NOT EXISTS latency_span_receipts_by_queue \
                ON latency_span_receipts(queue_id, span);",
        )
        .map_err(io::Error::other)?;
    Ok(connection)
}

fn read_latest_latency_receipt_from_index(
    receipts_file: &Path,
    queue_id: &str,
) -> io::Result<Option<LatencyReceipt>> {
    let index_file = latency_index_file(receipts_file);
    if !index_file.is_file() {
        return Ok(None);
    }
    let connection = Connection::open_with_flags(&index_file, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(io::Error::other)?;
    connection
        .busy_timeout(Duration::ZERO)
        .map_err(io::Error::other)?;
    read_latest_latency_receipt_from_connection(&connection, queue_id)
}

fn read_latest_latency_receipt_from_connection(
    connection: &Connection,
    queue_id: &str,
) -> io::Result<Option<LatencyReceipt>> {
    let queue = connection
        .query_row(
            "SELECT lane, updated_at_ms FROM latency_queue_receipts WHERE queue_id = ?1",
            params![queue_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()
        .map_err(io::Error::other)?;
    let Some((lane, updated_at_ms)) = queue else {
        return Ok(None);
    };

    let mut stages = BTreeMap::new();
    let mut statement = connection
        .prepare(
            "SELECT stage, at_ms FROM latency_stage_receipts \
             WHERE queue_id = ?1 ORDER BY stage ASC",
        )
        .map_err(io::Error::other)?;
    let rows = statement
        .query_map(params![queue_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })
        .map_err(io::Error::other)?;
    for row in rows {
        let (stage, at_ms) = row.map_err(io::Error::other)?;
        if let Ok(stage) = serde_json::from_value::<LatencyStage>(serde_json::Value::String(stage))
        {
            stages.insert(stage, at_ms);
        }
    }

    let mut monotonic_spans = BTreeMap::new();
    let mut statement = connection
        .prepare(
            "SELECT span, duration_ms, recorded_at_ms FROM latency_span_receipts \
             WHERE queue_id = ?1 ORDER BY span ASC",
        )
        .map_err(io::Error::other)?;
    let rows = statement
        .query_map(params![queue_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })
        .map_err(io::Error::other)?;
    for row in rows {
        let (span, duration_ms, recorded_at_ms) = row.map_err(io::Error::other)?;
        if let Ok(span) =
            serde_json::from_value::<MonotonicLatencySpan>(serde_json::Value::String(span))
        {
            monotonic_spans.insert(
                span,
                MonotonicLatencySpanReceipt {
                    duration_ms: duration_ms.max(0) as u64,
                    recorded_at_ms,
                },
            );
        }
    }

    Ok(Some(LatencyReceipt {
        schema: LATENCY_RECEIPT_SCHEMA.to_string(),
        queue_id: queue_id.to_string(),
        lane,
        updated_at_ms,
        stages,
        derived_stage_sources: BTreeMap::new(),
        monotonic_spans,
    }))
}

fn latency_stage_key(stage: LatencyStage) -> &'static str {
    match stage {
        LatencyStage::InboundReceived => "inbound-received",
        LatencyStage::TurnPlanned => "turn-planned",
        LatencyStage::RuntimeEnqueued => "runtime-enqueued",
        LatencyStage::QueueAccepted => "queue-accepted",
        LatencyStage::CapacityFirstSeen => "capacity-first-seen",
        LatencyStage::LeaseAcquired => "lease-acquired",
        LatencyStage::PromptBundleStart => "prompt-bundle-start",
        LatencyStage::PromptBundleDone => "prompt-bundle-done",
        LatencyStage::CodexPlanStart => "codex-plan-start",
        LatencyStage::CodexPlanDone => "codex-plan-done",
        LatencyStage::CodexSpawnStart => "codex-spawn-start",
        LatencyStage::CodexSpawnDone => "codex-spawn-done",
        LatencyStage::ThreadStartRequest => "thread-start-request",
        LatencyStage::ThreadStartDone => "thread-start-done",
        LatencyStage::FirstCodexEvent => "first-codex-event",
        LatencyStage::FirstProgressEvent => "first-progress-event",
        LatencyStage::FirstProviderProgressSurface => "first-provider-progress-surface",
        LatencyStage::WorkingIndicator => "working-indicator",
        LatencyStage::FinalCodexEvent => "final-codex-event",
        LatencyStage::TerminalEvent => "terminal-event",
        LatencyStage::OutboxWrite => "outbox-write",
        LatencyStage::DeliveryAttempt => "delivery-attempt",
        LatencyStage::DeliveryDone => "delivery-done",
    }
}

fn monotonic_latency_span_key(span: MonotonicLatencySpan) -> &'static str {
    match span {
        MonotonicLatencySpan::PromptBundlePreparation => "prompt-bundle-preparation",
        MonotonicLatencySpan::CodexPlanning => "codex-planning",
        MonotonicLatencySpan::CodexExecution => "codex-execution",
        MonotonicLatencySpan::Terminalization => "terminalization",
    }
}

pub fn latency_summary(
    receipt: &LatencyReceipt,
    expected_stages: &[LatencyStage],
) -> LatencySummary {
    let mut deltas = Vec::new();
    let mut out_of_order_deltas = Vec::new();
    let mut present = Vec::new();

    for stage in expected_stages {
        if let Some(at_ms) = receipt.stages.get(stage) {
            present.push((*stage, *at_ms));
        }
    }

    for window in present.windows(2) {
        let (from_stage, from_ms) = window[0];
        let (to_stage, to_ms) = window[1];
        let delta = LatencyDelta {
            from_stage,
            to_stage,
            delta_ms: to_ms.saturating_sub(from_ms),
        };
        if to_ms >= from_ms {
            deltas.push(delta);
        } else {
            out_of_order_deltas.push(delta);
        }
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
        out_of_order_deltas,
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

    #[test]
    fn latency_record_uses_bounded_projection_when_legacy_ledger_is_unreadable() {
        let root =
            temp_root("latency_record_uses_bounded_projection_when_legacy_ledger_is_unreadable");
        let file = latency_receipts_file(root.join("harness"));
        fs::create_dir_all(&file).unwrap();

        let receipt = record_latency_stage(
            &file,
            "queue-projection",
            "interactive",
            LatencyStage::InboundReceived,
            Some(42),
        )
        .unwrap();

        assert_eq!(receipt.stages[&LatencyStage::InboundReceived], 42);
        assert_eq!(
            read_latest_queue_receipt(&file, "queue-projection")
                .unwrap()
                .unwrap()
                .stages[&LatencyStage::InboundReceived],
            42
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn latency_record_persists_monotonic_span_without_raw_context() {
        let root = temp_root("latency_record_persists_monotonic_span_without_raw_context");
        let file = latency_receipts_file(root.join("harness"));

        let receipt = record_monotonic_latency_span(
            &file,
            "queue-span",
            "interactive",
            MonotonicLatencySpan::PromptBundlePreparation,
            Duration::from_millis(487),
        )
        .unwrap();

        assert_eq!(
            receipt.monotonic_spans[&MonotonicLatencySpan::PromptBundlePreparation].duration_ms,
            487
        );
        let index_bytes = fs::read(latency_index_file(&file)).unwrap();
        let index_text = String::from_utf8_lossy(&index_bytes);
        assert!(!index_text.contains("prompt text"));
        assert!(!index_text.contains("discord:"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn latency_summary_never_reports_negative_elapsed_time() {
        let receipt = LatencyReceipt {
            schema: LATENCY_RECEIPT_SCHEMA.to_string(),
            queue_id: "queue-non-causal".to_string(),
            lane: "discord".to_string(),
            updated_at_ms: 2_000,
            stages: BTreeMap::from([
                (LatencyStage::InboundReceived, 1_000),
                (LatencyStage::FirstCodexEvent, 1_800),
                (LatencyStage::FirstProgressEvent, 1_050),
                (LatencyStage::DeliveryDone, 2_000),
            ]),
            derived_stage_sources: BTreeMap::new(),
            monotonic_spans: BTreeMap::new(),
        };

        let summary = latency_summary(&receipt, default_latency_stages());

        assert!(summary.deltas.iter().all(|delta| delta.delta_ms >= 0));
        assert_eq!(summary.out_of_order_deltas.len(), 1);
        assert_eq!(
            summary.out_of_order_deltas[0].from_stage,
            LatencyStage::FirstCodexEvent
        );
        assert_eq!(
            summary.out_of_order_deltas[0].to_stage,
            LatencyStage::FirstProgressEvent
        );
    }

    #[test]
    fn latency_stage_write_waits_for_a_short_sqlite_writer() {
        let root = temp_root("latency_stage_write_waits_for_a_short_sqlite_writer");
        let file = latency_receipts_file(root.join("harness"));
        let _ = record_latency_stage(
            &file,
            "queue-busy",
            "discord",
            LatencyStage::InboundReceived,
            Some(1_000),
        )
        .unwrap();
        let lock = Connection::open(latency_index_file(&file)).unwrap();
        lock.execute_batch("BEGIN IMMEDIATE").unwrap();

        let worker_file = file.clone();
        let worker = std::thread::spawn(move || {
            record_latency_stage(
                worker_file,
                "queue-busy",
                "discord",
                LatencyStage::DeliveryDone,
                Some(2_000),
            )
        });
        std::thread::sleep(Duration::from_millis(10));
        lock.execute_batch("COMMIT").unwrap();

        let receipt = worker.join().unwrap().unwrap();
        assert_eq!(receipt.stages[&LatencyStage::DeliveryDone], 2_000);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn latency_read_joins_durable_delivery_when_projection_stage_is_missing() {
        let root =
            temp_root("latency_read_joins_durable_delivery_when_projection_stage_is_missing");
        let harness_home = root.join("harness");
        let file = latency_receipts_file(&harness_home);
        let _ = record_latency_stage(
            &file,
            "queue-delivery-fallback",
            "discord",
            LatencyStage::InboundReceived,
            Some(1_000),
        )
        .unwrap();
        let mut outbound = crate::ChannelOutboundMessage {
            platform: "discord".to_string(),
            account_id: Some("default".to_string()),
            channel_id: "channel-1".to_string(),
            user_id: "user-1".to_string(),
            session_key: "session-1".to_string(),
            delivery_id: None,
            kind: crate::ChannelOutboundMessageKind::AgentReply,
            source_queue_id: Some("queue-delivery-fallback".to_string()),
            source_completion_file: None,
            text: "final".to_string(),
            presentation: None,
            delivery_intent: None,
            attachments: Vec::new(),
        };
        let append = crate::append_channel_outbox_message(&harness_home, &mut outbound).unwrap();
        crate::record_channel_delivery(crate::ChannelDeliveryRecordOptions {
            harness_home: harness_home.clone(),
            delivery_id: append.delivery_id,
            status: crate::ChannelDeliveryStatus::Delivered,
            platform: outbound.platform,
            account_id: outbound.account_id,
            channel_id: outbound.channel_id,
            user_id: outbound.user_id,
            session_key: outbound.session_key,
            provider_message_id: Some("provider-message-1".to_string()),
            error: None,
            now_ms: 2_000,
            rendered_units: Vec::new(),
            presentation: None,
        })
        .unwrap();

        let receipt = read_latest_queue_receipt(&file, "queue-delivery-fallback")
            .unwrap()
            .unwrap();
        assert_eq!(receipt.stages[&LatencyStage::DeliveryDone], 2_000);
        assert_eq!(
            receipt.derived_stage_sources[&LatencyStage::DeliveryDone],
            "channel-delivery-receipt-index"
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn latency_delivery_join_is_not_limited_by_receipt_history_window() {
        let root = temp_root("latency_delivery_join_is_not_limited_by_receipt_history_window");
        let harness_home = root.join("harness");
        let file = latency_receipts_file(&harness_home);
        let queue_id = "queue-delivery-beyond-history-window";
        let _ = record_latency_stage(
            &file,
            queue_id,
            "discord",
            LatencyStage::InboundReceived,
            Some(1_000),
        )
        .unwrap();

        for index in 0..=64 {
            let mut outbound = crate::ChannelOutboundMessage {
                platform: "discord".to_string(),
                account_id: Some("default".to_string()),
                channel_id: "channel-1".to_string(),
                user_id: "user-1".to_string(),
                session_key: "session-1".to_string(),
                delivery_id: None,
                kind: crate::ChannelOutboundMessageKind::AgentReply,
                source_queue_id: Some(queue_id.to_string()),
                source_completion_file: None,
                text: format!("attempt {index}"),
                presentation: None,
                delivery_intent: None,
                attachments: Vec::new(),
            };
            let append =
                crate::append_channel_outbox_message(&harness_home, &mut outbound).unwrap();
            let delivered = index == 64;
            crate::record_channel_delivery(crate::ChannelDeliveryRecordOptions {
                harness_home: harness_home.clone(),
                delivery_id: append.delivery_id,
                status: if delivered {
                    crate::ChannelDeliveryStatus::Delivered
                } else {
                    crate::ChannelDeliveryStatus::Failed
                },
                platform: outbound.platform,
                account_id: outbound.account_id,
                channel_id: outbound.channel_id,
                user_id: outbound.user_id,
                session_key: outbound.session_key,
                provider_message_id: delivered.then(|| "provider-message-final".to_string()),
                error: (!delivered).then(|| "retryable fixture".to_string()),
                now_ms: 2_000 + index,
                rendered_units: Vec::new(),
                presentation: None,
            })
            .unwrap();
        }

        let receipt = read_latest_queue_receipt(&file, queue_id).unwrap().unwrap();
        assert_eq!(receipt.stages[&LatencyStage::DeliveryDone], 2_064);
        assert_eq!(
            receipt.derived_stage_sources[&LatencyStage::DeliveryDone],
            "channel-delivery-receipt-index"
        );
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
