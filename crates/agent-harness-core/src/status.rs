use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use serde_json::Value;

use crate::memory::{
    MemoryMemEngineCanaryReport, MemoryMemEngineOwnershipReport, MemorySemanticCoverageReport,
    collect_memory_embedding_coverage, collect_memory_semantic_coverage,
    memory_adapter_readiness_report, memory_capability_mode_from_readiness,
    memory_mem_engine_ownership_report_for_owner_state, memory_qdrant_native_recall_status,
};
use crate::memory_owner::{MemoryOwnerState, read_memory_owner_state_or_default};
use crate::skill_apply::skill_apply_receipts_file;
use crate::skill_learning::skill_proposals_file;
use crate::skill_usage::{skill_usage_events_file, skill_usage_snapshot_file};
use crate::{
    ActivationReadinessOptions, ActivationReadinessReport, ActivationReadinessStatus,
    ChannelOutboxPlanSummary, CronRunSummary, WorkerStatusOptions, WorkerStatusReport,
    check_activation_readiness, collect_cron_run_summary, collect_worker_status, cron_runs_db_file,
    harness_log_file,
};

const HARNESS_STATUS_SCHEMA: &str = "agent-harness.status.v1";
const STATUS_JSONL_SAMPLE_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarnessStatusOptions {
    pub harness_home: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessStatusReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub ready: bool,
    pub readiness: ActivationReadinessReport,
    pub runtime: HarnessRuntimeStatus,
    pub channels: HarnessChannelStatus,
    pub loops: HarnessLoopStatus,
    pub workers: WorkerStatusReport,
    pub cron_scheduler: HarnessCronSchedulerStatus,
    pub cron_runs: HarnessCronRunStatus,
    pub memory: HarnessMemoryStatus,
    pub learning: HarnessLearningStatus,
    pub plugins: HarnessPluginStatus,
    pub logs: HarnessOperationalLogStatus,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessRuntimeStatus {
    pub pending_file: PathBuf,
    pub queued_items: usize,
    pub prepared_items: usize,
    pub completed_items: usize,
    pub open_items: usize,
    pub cron_queued_items: usize,
    pub cron_open_items: usize,
    pub queued_by_runtime_class: BTreeMap<String, usize>,
    pub queued_by_origin: BTreeMap<String, usize>,
    pub open_by_runtime_class: BTreeMap<String, usize>,
    pub open_by_origin: BTreeMap<String, usize>,
    pub class_leases: Vec<HarnessRuntimeClassLeaseStatus>,
    pub pending_invalid_lines: usize,
    pub latest_non_idle_run_once: Option<HarnessRuntimeReceiptStatus>,
    pub execution_receipts: HarnessJsonlStatus,
    pub run_once_receipts: HarnessJsonlStatus,
    pub codex_plan_receipts: HarnessJsonlStatus,
    pub codex_run_receipts: HarnessJsonlStatus,
    pub codex_completion_receipts: HarnessJsonlStatus,
    pub codex_launch_receipts: HarnessJsonlStatus,
    pub control_receipts: HarnessJsonlStatus,
    pub dead_letter_receipts: HarnessJsonlStatus,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessRuntimeClassLeaseStatus {
    pub runtime_class: String,
    pub leases_file: PathBuf,
    pub exists: bool,
    pub leased_items: usize,
    pub active_leases: usize,
    pub expired_leases: usize,
    pub cron_run_leases: usize,
    pub by_agent: BTreeMap<String, usize>,
    pub by_origin: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessChannelStatus {
    pub outbox: HarnessOutboxStatus,
    pub telegram_offset_file: PathBuf,
    pub telegram_offset_present: bool,
    pub telegram_probe: HarnessJsonlStatus,
    pub telegram_poll_log_present: bool,
    pub discord_send_log_present: bool,
    pub discord_event_log_present: bool,
    pub discord_gateway_probe: HarnessJsonlStatus,
    pub discord_reply_context_receipts: HarnessJsonlStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessOutboxStatus {
    pub all: ChannelOutboxPlanSummary,
    pub telegram: ChannelOutboxPlanSummary,
    pub discord: ChannelOutboxPlanSummary,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessLoopStatus {
    pub heartbeat_dir: PathBuf,
    pub heartbeats: Vec<HarnessLoopHeartbeatStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessLoopHeartbeatStatus {
    pub name: String,
    pub heartbeat_file: PathBuf,
    pub present: bool,
    pub status: Option<String>,
    pub iteration: Option<i64>,
    pub process_id: Option<i64>,
    pub at_ms: Option<i64>,
    pub age_ms: Option<i64>,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessCronSchedulerStatus {
    pub state_dir: PathBuf,
    pub database: PathBuf,
    pub database_present: bool,
    pub loop_last_file: PathBuf,
    pub loop_last_present: bool,
    pub latest_status: Option<String>,
    pub latest_native_entries: Option<i64>,
    pub latest_deterministic_entries: Option<i64>,
    pub latest_due_candidates: Option<i64>,
    pub latest_enqueued: Option<i64>,
    pub latest_skipped_held: Option<i64>,
    pub latest_skipped_duplicate: Option<i64>,
    pub latest_skipped_policy: Option<i64>,
    pub latest_errors: Option<i64>,
    pub receipts: HarnessJsonlStatus,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessCronRunStatus {
    pub database: PathBuf,
    pub database_present: bool,
    pub summary: CronRunSummary,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessMemoryStatus {
    pub memory_dir: PathBuf,
    pub exists: bool,
    pub qdrant_edge: bool,
    pub lancedb: bool,
    pub legacy_mem_sqlite: bool,
    pub memory_credentials_env_present: bool,
    pub regular_files: usize,
    pub search_receipts: HarnessJsonlStatus,
    pub vector_recall_receipts: HarnessJsonlStatus,
    pub prompt_context_receipts: HarnessJsonlStatus,
    pub lifecycle_receipts: HarnessJsonlStatus,
    pub canvas_receipts: HarnessJsonlStatus,
    pub hook_receipts: HarnessJsonlStatus,
    pub store_proposals: HarnessJsonlStatus,
    pub slot_receipts: HarnessJsonlStatus,
    pub recall_plan_receipts: HarnessJsonlStatus,
    pub graph_freshness_receipts: HarnessJsonlStatus,
    pub provenance_chain_receipts: HarnessJsonlStatus,
    pub capture_candidates: HarnessJsonlStatus,
    pub summary: HarnessMemoryHealthSummary,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessMemoryHealthSummary {
    pub active_recall_backend: String,
    pub qdrant_parity: String,
    pub adapter_readiness: String,
    pub capability_mode: String,
    pub mem_engine_ownership: MemoryMemEngineOwnershipReport,
    pub qdrant_native_recall: String,
    pub semantic_coverage: MemorySemanticCoverageReport,
    pub prompt_context_status: Option<String>,
    pub lifecycle_status: Option<String>,
    pub canvas_status: Option<String>,
    pub hook_status: Option<String>,
    pub recall_plan_status: Option<String>,
    pub graph_freshness_status: Option<String>,
    pub provenance_chain_status: Option<String>,
    pub capture_candidate_count: usize,
    pub store_proposal_count: usize,
    pub slot_receipt_count: usize,
    pub provenance_chain_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessLearningStatus {
    pub skill_usage_events: HarnessJsonlStatus,
    pub skill_usage_snapshot_file: PathBuf,
    pub skill_usage_snapshot_present: bool,
    pub skill_proposals: HarnessJsonlStatus,
    pub skill_apply_receipts: HarnessJsonlStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessPluginStatus {
    pub catalog_file: PathBuf,
    pub catalog_present: bool,
    pub catalog_tools: usize,
    pub sidecar_execution_receipts: HarnessJsonlStatus,
    pub sidecar_probe_receipts: HarnessJsonlStatus,
    pub sidecar_bridge_receipts: HarnessJsonlStatus,
    pub hook_receipts: HarnessJsonlStatus,
    pub memory_slot_receipts: HarnessJsonlStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessOperationalLogStatus {
    pub log_file: PathBuf,
    pub exists: bool,
    pub bytes: u64,
    pub sampled: bool,
    pub sampled_bytes: u64,
    pub lines: usize,
    pub invalid_lines: usize,
    pub latest_event: Option<String>,
    pub event_present: BTreeMap<String, bool>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessJsonlStatus {
    pub path: PathBuf,
    pub exists: bool,
    pub bytes: u64,
    pub sampled: bool,
    pub sampled_bytes: u64,
    pub lines: usize,
    pub invalid_lines: usize,
    pub latest_status: Option<String>,
    pub latest_method: Option<String>,
    pub latest_backend: Option<String>,
    pub latest_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessRuntimeReceiptStatus {
    pub path: PathBuf,
    pub line_number: usize,
    pub queue_id: Option<String>,
    pub status: Option<String>,
    pub reason: Option<String>,
    pub runtime_class: Option<String>,
    pub origin: Option<String>,
    pub cron_run_id: Option<String>,
    pub scheduled_for_ms: Option<i64>,
}

pub fn collect_harness_status(options: HarnessStatusOptions) -> io::Result<HarnessStatusReport> {
    let readiness = check_activation_readiness(ActivationReadinessOptions {
        harness_home: options.harness_home.clone(),
    })?;
    let mut warnings = Vec::new();
    let channels = channel_status(&options.harness_home, &readiness, &mut warnings)?;
    let logs = log_status(&options.harness_home)?;
    let runtime = runtime_status(&options.harness_home, &mut warnings)?;
    let loops = loop_status(&options.harness_home)?;
    let workers = collect_worker_status(WorkerStatusOptions {
        harness_home: options.harness_home.clone(),
    })?;
    let cron_scheduler = cron_scheduler_status(&options.harness_home)?;
    let cron_runs = cron_runs_status(&options.harness_home)?;
    let memory = memory_status(&options.harness_home)?;
    let learning = learning_status(&options.harness_home)?;
    let plugins = plugin_status(&options.harness_home)?;

    Ok(HarnessStatusReport {
        schema: HARNESS_STATUS_SCHEMA,
        harness_home: options.harness_home,
        ready: readiness.ready,
        readiness,
        runtime,
        channels,
        loops,
        workers,
        cron_scheduler,
        cron_runs,
        memory,
        learning,
        plugins,
        logs,
        warnings,
    })
}

fn learning_status(harness_home: &Path) -> io::Result<HarnessLearningStatus> {
    let snapshot_file = skill_usage_snapshot_file(harness_home);
    Ok(HarnessLearningStatus {
        skill_usage_events: jsonl_status(skill_usage_events_file(harness_home))?,
        skill_usage_snapshot_present: snapshot_file.is_file(),
        skill_usage_snapshot_file: snapshot_file,
        skill_proposals: jsonl_status(skill_proposals_file(harness_home))?,
        skill_apply_receipts: jsonl_status(skill_apply_receipts_file(harness_home))?,
    })
}

fn loop_status(harness_home: &Path) -> io::Result<HarnessLoopStatus> {
    const LOOP_NAMES: &[&str] = &[
        "runtime-loop",
        "progress-delivery-loop",
        "telegram-loop",
        "discord-outbox-loop",
        "discord-gateway-loop",
        "worker-loop",
        "cron-scheduler-loop",
    ];
    let heartbeat_dir = harness_home
        .join("state")
        .join("supervisor")
        .join("loop-heartbeats");
    let now_ms = epoch_ms().unwrap_or(0);
    let mut heartbeats = Vec::new();
    for name in LOOP_NAMES {
        let heartbeat_file = heartbeat_dir.join(format!("{name}.json"));
        let heartbeat = read_loop_heartbeat(name, &heartbeat_file, now_ms)?;
        heartbeats.push(heartbeat);
    }
    Ok(HarnessLoopStatus {
        heartbeat_dir,
        heartbeats,
    })
}

fn cron_scheduler_status(harness_home: &Path) -> io::Result<HarnessCronSchedulerStatus> {
    let state_dir = harness_home.join("state").join("cron-scheduler");
    let database = state_dir.join("watermarks.sqlite");
    let loop_last_file = state_dir.join("loop-last.json");
    let latest = fs::read_to_string(&loop_last_file)
        .ok()
        .and_then(|text| serde_json::from_str::<Value>(&text).ok());
    Ok(HarnessCronSchedulerStatus {
        state_dir: state_dir.clone(),
        database_present: database.is_file(),
        database,
        loop_last_present: loop_last_file.is_file(),
        latest_status: latest
            .as_ref()
            .and_then(|value| string_path(value, &["status"])),
        latest_native_entries: latest
            .as_ref()
            .and_then(|value| i64_path(value, &["summary", "nativeEntries"])),
        latest_deterministic_entries: latest
            .as_ref()
            .and_then(|value| i64_path(value, &["summary", "deterministicEntries"])),
        latest_due_candidates: latest
            .as_ref()
            .and_then(|value| i64_path(value, &["summary", "dueCandidates"])),
        latest_enqueued: latest
            .as_ref()
            .and_then(|value| i64_path(value, &["summary", "enqueued"])),
        latest_skipped_held: latest
            .as_ref()
            .and_then(|value| i64_path(value, &["summary", "skippedHeld"])),
        latest_skipped_duplicate: latest
            .as_ref()
            .and_then(|value| i64_path(value, &["summary", "skippedDuplicate"])),
        latest_skipped_policy: latest
            .as_ref()
            .and_then(|value| i64_path(value, &["summary", "skippedPolicy"])),
        latest_errors: latest
            .as_ref()
            .and_then(|value| i64_path(value, &["summary", "errors"])),
        loop_last_file,
        receipts: jsonl_status(state_dir.join("receipts.jsonl"))?,
    })
}

fn cron_runs_status(harness_home: &Path) -> io::Result<HarnessCronRunStatus> {
    let database = cron_runs_db_file(harness_home);
    if !database.is_file() {
        return Ok(HarnessCronRunStatus {
            database,
            database_present: false,
            summary: CronRunSummary::default(),
            warnings: Vec::new(),
        });
    }
    let report = collect_cron_run_summary(harness_home)?;
    Ok(HarnessCronRunStatus {
        database: report.database,
        database_present: true,
        summary: report.summary,
        warnings: report.warnings,
    })
}

fn read_loop_heartbeat(
    name: &str,
    heartbeat_file: &Path,
    now_ms: i64,
) -> io::Result<HarnessLoopHeartbeatStatus> {
    let text = match fs::read_to_string(heartbeat_file) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(HarnessLoopHeartbeatStatus {
                name: name.to_string(),
                heartbeat_file: heartbeat_file.to_path_buf(),
                present: false,
                status: None,
                iteration: None,
                process_id: None,
                at_ms: None,
                age_ms: None,
                detail: None,
            });
        }
        Err(error) => return Err(error),
    };
    let value = serde_json::from_str::<Value>(&text).unwrap_or(Value::Null);
    let at_ms = i64_path(&value, &["atMs"]);
    Ok(HarnessLoopHeartbeatStatus {
        name: name.to_string(),
        heartbeat_file: heartbeat_file.to_path_buf(),
        present: true,
        status: string_path(&value, &["status"]),
        iteration: i64_path(&value, &["iteration"]),
        process_id: i64_path(&value, &["processId"]),
        at_ms,
        age_ms: at_ms.map(|at_ms| now_ms.saturating_sub(at_ms)),
        detail: string_path(&value, &["detail"]),
    })
}

fn runtime_status(
    harness_home: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<HarnessRuntimeStatus> {
    let queue_dir = harness_home.join("state").join("runtime-queue");
    let pending_file = queue_dir.join("pending.jsonl");
    let pending = read_runtime_pending_queue(&pending_file, warnings)?;
    let prepared_ids = read_receipt_queue_ids_with_status(
        &queue_dir.join("execution-receipts.jsonl"),
        &["prepared", "already-prepared"],
        warnings,
        "runtime execution receipts",
    )?;
    let completed_ids = read_receipt_queue_ids_with_status(
        &queue_dir.join("codex-runtime-completion-receipts.jsonl"),
        &["recorded", "already-recorded"],
        warnings,
        "runtime completion receipts",
    )?;
    let terminal_run_ids = read_receipt_queue_ids_with_status(
        &queue_dir.join("run-once-receipts.jsonl"),
        &[
            "completed",
            "timeout",
            "failed-terminal",
            "canceled",
            "skipped",
            "dead-letter",
        ],
        warnings,
        "runtime run-once receipts",
    )?;
    let mut open_by_runtime_class = BTreeMap::new();
    let mut open_by_origin = BTreeMap::new();
    let mut cron_open_items = 0;
    for item in &pending.items {
        if terminal_run_ids.contains(&item.queue_id) {
            continue;
        }
        *open_by_runtime_class
            .entry(item.runtime_class.clone())
            .or_insert(0) += 1;
        *open_by_origin.entry(item.origin.clone()).or_insert(0) += 1;
        if item.runtime_class == "cron" || item.cron_run_id.is_some() {
            cron_open_items += 1;
        }
    }
    let open_items: usize = open_by_runtime_class.values().copied().sum();
    let class_leases =
        runtime_class_lease_statuses(&queue_dir, pending.queued_by_runtime_class.keys(), warnings)?;
    Ok(HarnessRuntimeStatus {
        pending_file,
        queued_items: pending.queue_ids.len(),
        prepared_items: pending.queue_ids.intersection(&prepared_ids).count(),
        completed_items: pending.queue_ids.intersection(&completed_ids).count(),
        open_items,
        cron_queued_items: pending.cron_queued_items,
        cron_open_items,
        queued_by_runtime_class: pending.queued_by_runtime_class,
        queued_by_origin: pending.queued_by_origin,
        open_by_runtime_class,
        open_by_origin,
        class_leases,
        pending_invalid_lines: pending.invalid_lines,
        latest_non_idle_run_once: latest_non_idle_runtime_receipt(
            &queue_dir.join("run-once-receipts.jsonl"),
        )?,
        execution_receipts: jsonl_status(queue_dir.join("execution-receipts.jsonl"))?,
        run_once_receipts: jsonl_status(queue_dir.join("run-once-receipts.jsonl"))?,
        codex_plan_receipts: jsonl_status(queue_dir.join("codex-runtime-receipts.jsonl"))?,
        codex_run_receipts: jsonl_status(queue_dir.join("codex-runtime-run-receipts.jsonl"))?,
        codex_completion_receipts: jsonl_status(
            queue_dir.join("codex-runtime-completion-receipts.jsonl"),
        )?,
        codex_launch_receipts: jsonl_status(queue_dir.join("codex-runtime-launch-receipts.jsonl"))?,
        control_receipts: jsonl_status(queue_dir.join("control-receipts.jsonl"))?,
        dead_letter_receipts: jsonl_status(queue_dir.join("dead-letter-receipts.jsonl"))?,
    })
}

fn channel_status(
    harness_home: &Path,
    readiness: &ActivationReadinessReport,
    warnings: &mut Vec<String>,
) -> io::Result<HarnessChannelStatus> {
    let telegram_offset_file = harness_home
        .join("state")
        .join("channels")
        .join("telegram-offset.json");
    let channel_dir = harness_home.join("state").join("channels");
    let outbox_file = channel_dir.join("outbox.jsonl");
    let delivery_receipts =
        read_delivery_status_tail(&channel_dir.join("delivery-receipts.jsonl"), warnings)?;
    let all = bounded_outbox_summary(&outbox_file, None, &delivery_receipts, warnings)?;
    let telegram =
        bounded_outbox_summary(&outbox_file, Some("telegram"), &delivery_receipts, warnings)?;
    let discord =
        bounded_outbox_summary(&outbox_file, Some("discord"), &delivery_receipts, warnings)?;

    Ok(HarnessChannelStatus {
        outbox: HarnessOutboxStatus {
            all,
            telegram,
            discord,
        },
        telegram_offset_present: telegram_offset_file.is_file(),
        telegram_offset_file,
        telegram_probe: jsonl_status(channel_dir.join("telegram-probe-receipts.jsonl"))?,
        telegram_poll_log_present: readiness_check_passed(readiness, "telegram-poll-log"),
        discord_send_log_present: readiness_check_passed(readiness, "discord-send-log"),
        discord_event_log_present: readiness_check_passed(readiness, "discord-event-log"),
        discord_gateway_probe: jsonl_status(
            channel_dir.join("discord-gateway-probe-receipts.jsonl"),
        )?,
        discord_reply_context_receipts: jsonl_status(
            channel_dir.join("discord-reply-context-receipts.jsonl"),
        )?,
    })
}

fn memory_status(harness_home: &Path) -> io::Result<HarnessMemoryStatus> {
    let memory_dir = harness_home.join("memory");
    let memory_state_dir = harness_home.join("state").join("memory");
    let exists = memory_dir.is_dir();
    let qdrant_edge = memory_dir.join("qdrant-edge").is_dir();
    let lancedb = memory_dir.join("lancedb").is_dir();
    let legacy_mem_sqlite = memory_dir.join("openclaw-mem.sqlite").is_file();
    let memory_credentials_env_present = harness_home
        .join("secrets")
        .join("memory-credentials.env")
        .is_file();
    let regular_files = count_regular_files(&memory_dir)?;
    let search_receipts = jsonl_status(memory_state_dir.join("search-receipts.jsonl"))?;
    let vector_recall_receipts =
        jsonl_status(memory_state_dir.join("vector-recall-receipts.jsonl"))?;
    let prompt_context_receipts =
        jsonl_status(memory_state_dir.join("prompt-context-receipts.jsonl"))?;
    let lifecycle_receipts = jsonl_status(memory_state_dir.join("lifecycle-receipts.jsonl"))?;
    let canvas_receipts = jsonl_status(memory_state_dir.join("canvas-receipts.jsonl"))?;
    let hook_receipts = jsonl_status(memory_state_dir.join("hook-receipts.jsonl"))?;
    let store_proposals = jsonl_status(memory_state_dir.join("store-proposals.jsonl"))?;
    let slot_receipts = jsonl_status(memory_state_dir.join("slot-receipts.jsonl"))?;
    let recall_plan_receipts = jsonl_status(memory_state_dir.join("recall-plan-receipts.jsonl"))?;
    let graph_freshness_receipts = jsonl_status(
        memory_state_dir
            .join("graph")
            .join("freshness-receipts.jsonl"),
    )?;
    let provenance_chain_receipts =
        jsonl_status(memory_state_dir.join("provenance-chain-receipts.jsonl"))?;
    let capture_candidates = jsonl_status(memory_state_dir.join("auto-capture-candidates.jsonl"))?;
    let embedding_coverage = collect_memory_embedding_coverage(harness_home);
    let semantic_coverage =
        collect_memory_semantic_coverage(harness_home, None, &embedding_coverage)?;
    let memory_owner_state =
        read_memory_owner_state_or_default(harness_home, epoch_ms().unwrap_or(0))?;
    let summary = memory_health_summary(
        &memory_dir,
        qdrant_edge,
        legacy_mem_sqlite,
        &semantic_coverage,
        &memory_owner_state,
        &vector_recall_receipts,
        &prompt_context_receipts,
        &lifecycle_receipts,
        &canvas_receipts,
        &hook_receipts,
        &store_proposals,
        &slot_receipts,
        &recall_plan_receipts,
        &graph_freshness_receipts,
        &provenance_chain_receipts,
        &capture_candidates,
    );
    Ok(HarnessMemoryStatus {
        exists,
        qdrant_edge,
        lancedb,
        legacy_mem_sqlite,
        memory_credentials_env_present,
        regular_files,
        search_receipts,
        vector_recall_receipts,
        prompt_context_receipts,
        lifecycle_receipts,
        canvas_receipts,
        hook_receipts,
        store_proposals,
        slot_receipts,
        recall_plan_receipts,
        graph_freshness_receipts,
        provenance_chain_receipts,
        capture_candidates,
        summary,
        memory_dir,
    })
}

fn memory_health_summary(
    memory_dir: &Path,
    qdrant_edge: bool,
    legacy_mem_sqlite: bool,
    semantic_coverage: &MemorySemanticCoverageReport,
    memory_owner_state: &MemoryOwnerState,
    vector_recall_receipts: &HarnessJsonlStatus,
    prompt_context_receipts: &HarnessJsonlStatus,
    lifecycle_receipts: &HarnessJsonlStatus,
    canvas_receipts: &HarnessJsonlStatus,
    hook_receipts: &HarnessJsonlStatus,
    store_proposals: &HarnessJsonlStatus,
    slot_receipts: &HarnessJsonlStatus,
    recall_plan_receipts: &HarnessJsonlStatus,
    graph_freshness_receipts: &HarnessJsonlStatus,
    provenance_chain_receipts: &HarnessJsonlStatus,
    capture_candidates: &HarnessJsonlStatus,
) -> HarnessMemoryHealthSummary {
    let vector_status = vector_recall_receipts.latest_status.as_deref();
    let active_recall_backend = if matches!(vector_status, Some("ready" | "no-hits")) {
        vector_recall_receipts
            .latest_backend
            .clone()
            .unwrap_or_else(|| "unknown".to_string())
    } else if legacy_mem_sqlite {
        "sqlite-vector-available".to_string()
    } else {
        "none".to_string()
    };
    let has_local_backend = legacy_mem_sqlite
        || semantic_coverage.observations.items.unwrap_or(0) > 0
        || semantic_coverage.episodic_events.items.unwrap_or(0) > 0
        || semantic_coverage.service_writeback.items.unwrap_or(0) > 0;
    let adapter_readiness =
        memory_adapter_readiness_report(has_local_backend, qdrant_edge, false, true);
    let capability_mode = memory_capability_mode_from_readiness(&adapter_readiness);
    let mem_engine_state = memory_dir
        .join("openclaw-mem-engine")
        .join("sunrise_state.json");
    let qdrant_edge_mode = if qdrant_edge {
        "preserved-snapshot"
    } else {
        "missing"
    };
    let mem_engine_canary = MemoryMemEngineCanaryReport {
        status: if mem_engine_state.is_file() {
            "available-not-promoted".to_string()
        } else {
            "not-available".to_string()
        },
        active_slot_owner: "snapshot-adapter".to_string(),
        engine_state_file: mem_engine_state.is_file().then_some(mem_engine_state),
        rollback_slot_owner: "snapshot-adapter".to_string(),
        qdrant_edge_mode: qdrant_edge_mode.to_string(),
        warnings: Vec::new(),
    };
    let mem_engine_ownership =
        memory_mem_engine_ownership_report_for_owner_state(&mem_engine_canary, memory_owner_state);
    let qdrant_parity = if active_recall_backend
        .to_ascii_lowercase()
        .contains("qdrant")
    {
        "native-recall-active".to_string()
    } else if qdrant_edge {
        "snapshot-preserved; native-recall-not-active".to_string()
    } else {
        "not-present".to_string()
    };
    HarnessMemoryHealthSummary {
        active_recall_backend,
        qdrant_parity,
        adapter_readiness: adapter_readiness.status,
        capability_mode,
        mem_engine_ownership,
        qdrant_native_recall: memory_qdrant_native_recall_status(qdrant_edge),
        semantic_coverage: semantic_coverage.clone(),
        prompt_context_status: prompt_context_receipts.latest_status.clone(),
        lifecycle_status: lifecycle_receipts.latest_status.clone(),
        canvas_status: canvas_receipts.latest_status.clone(),
        hook_status: hook_receipts.latest_status.clone(),
        recall_plan_status: recall_plan_receipts.latest_status.clone(),
        graph_freshness_status: graph_freshness_receipts.latest_status.clone(),
        provenance_chain_status: provenance_chain_receipts.latest_status.clone(),
        capture_candidate_count: capture_candidates.lines,
        store_proposal_count: store_proposals.lines,
        slot_receipt_count: slot_receipts.lines,
        provenance_chain_count: provenance_chain_receipts.lines,
    }
}

fn plugin_status(harness_home: &Path) -> io::Result<HarnessPluginStatus> {
    let sidecar_dir = harness_home.join("state").join("plugin-sidecar");
    let catalog_file = sidecar_dir.join("catalog.json");
    let catalog_tools = match fs::read_to_string(&catalog_file) {
        Ok(text) => serde_json::from_str::<Value>(&text)
            .ok()
            .and_then(|value| value.get("tools").and_then(Value::as_array).map(Vec::len))
            .unwrap_or(0),
        Err(error) if error.kind() == io::ErrorKind::NotFound => 0,
        Err(error) => return Err(error),
    };
    Ok(HarnessPluginStatus {
        catalog_present: catalog_file.is_file(),
        catalog_tools,
        catalog_file,
        sidecar_execution_receipts: jsonl_status(sidecar_dir.join("execution-receipts.jsonl"))?,
        sidecar_probe_receipts: jsonl_status(sidecar_dir.join("probe-receipts.jsonl"))?,
        sidecar_bridge_receipts: jsonl_status(sidecar_dir.join("bridge-receipts.jsonl"))?,
        hook_receipts: jsonl_status(sidecar_dir.join("hook-receipts.jsonl"))?,
        memory_slot_receipts: jsonl_status(sidecar_dir.join("memory-slot-receipts.jsonl"))?,
    })
}

fn log_status(harness_home: &Path) -> io::Result<HarnessOperationalLogStatus> {
    const EVENTS: &[&str] = &[
        "activation.enable-check",
        "telegram.probe",
        "telegram.poll-once",
        "discord.outbox-send-once",
        "discord.event-run-once",
        "channel.receive",
        "runtime.run-once.completed",
        "runtime.loop-stopped",
        "codex.run.completed",
        "codex.complete.recorded",
        "channel.delivery.delivered",
    ];

    let log_file = harness_log_file(harness_home);
    let mut lines = 0;
    let mut invalid_lines = 0;
    let mut latest_event = None;
    let mut exists = false;
    let mut bytes = 0;
    let mut sampled = false;
    let mut sampled_bytes = 0;
    let mut event_present = EVENTS
        .iter()
        .map(|event| ((*event).to_string(), false))
        .collect::<BTreeMap<_, _>>();

    match read_tail_sample(&log_file, STATUS_JSONL_SAMPLE_BYTES) {
        Ok(Some(sample)) => {
            exists = true;
            bytes = sample.bytes;
            sampled = sample.sampled;
            sampled_bytes = sample.sampled_bytes;
            for line in sample.text.lines() {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                match serde_json::from_str::<Value>(trimmed) {
                    Ok(value) => {
                        lines += 1;
                        if let Some(event) = value.get("event").and_then(Value::as_str) {
                            latest_event = Some(event.to_string());
                            if let Some(present) = event_present.get_mut(event) {
                                *present = true;
                            }
                        }
                    }
                    Err(_) => invalid_lines += 1,
                }
            }
        }
        Ok(None) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }

    Ok(HarnessOperationalLogStatus {
        exists,
        log_file,
        bytes,
        sampled,
        sampled_bytes,
        lines,
        invalid_lines,
        latest_event,
        event_present,
    })
}

fn latest_non_idle_runtime_receipt(path: &Path) -> io::Result<Option<HarnessRuntimeReceiptStatus>> {
    let Some(sample) = read_tail_sample(path, STATUS_JSONL_SAMPLE_BYTES)? else {
        return Ok(None);
    };
    let mut latest = None;
    for (index, line) in sample.text.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
            continue;
        };
        let status = string_path(&value, &["status"]);
        if status
            .as_deref()
            .is_some_and(|status| matches!(status, "no-work" | "no-prepared-execution"))
        {
            continue;
        }
        latest = Some(HarnessRuntimeReceiptStatus {
            path: path.to_path_buf(),
            line_number: index + 1,
            queue_id: queue_id_from_value(&value),
            status,
            reason: string_path(&value, &["reason"]),
            runtime_class: string_path_any(&value, &["runtimeClass", "runtime_class"]),
            origin: string_path_any(&value, &["origin"]),
            cron_run_id: string_path_any(&value, &["cronRunId", "cron_run_id"]),
            scheduled_for_ms: i64_path(&value, &["scheduledForMs"])
                .or_else(|| i64_path(&value, &["scheduled_for_ms"])),
        });
    }
    Ok(latest)
}

fn jsonl_status(path: PathBuf) -> io::Result<HarnessJsonlStatus> {
    let mut status = HarnessJsonlStatus {
        path,
        ..HarnessJsonlStatus::default()
    };
    let Some(sample) = read_tail_sample(&status.path, STATUS_JSONL_SAMPLE_BYTES)? else {
        return Ok(status);
    };
    status.exists = true;
    status.bytes = sample.bytes;
    status.sampled = sample.sampled;
    status.sampled_bytes = sample.sampled_bytes;
    for line in sample.text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        match serde_json::from_str::<Value>(trimmed) {
            Ok(value) => {
                status.lines += 1;
                status.latest_status = string_path(&value, &["status"]);
                status.latest_method = string_path(&value, &["method"]);
                status.latest_backend = string_path(&value, &["backend"]);
                status.latest_reason = string_path(&value, &["reason"]);
            }
            Err(_) => status.invalid_lines += 1,
        }
    }
    Ok(status)
}

struct TailSample {
    text: String,
    bytes: u64,
    sampled: bool,
    sampled_bytes: u64,
}

fn read_tail_sample(path: &Path, max_bytes: u64) -> io::Result<Option<TailSample>> {
    let mut file = match fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let bytes = file.metadata()?.len();
    if bytes <= max_bytes {
        let mut text = String::new();
        file.read_to_string(&mut text)?;
        return Ok(Some(TailSample {
            text,
            bytes,
            sampled: false,
            sampled_bytes: bytes,
        }));
    }

    let start = bytes.saturating_sub(max_bytes);
    file.seek(SeekFrom::Start(start))?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;
    let sampled_bytes = buffer.len() as u64;
    let mut text = String::from_utf8_lossy(&buffer).into_owned();
    if let Some(newline) = text.find('\n') {
        text = text[newline + 1..].to_string();
    }
    Ok(Some(TailSample {
        text,
        bytes,
        sampled: true,
        sampled_bytes,
    }))
}

#[derive(Default)]
struct RuntimePendingQueueRead {
    queue_ids: BTreeSet<String>,
    items: Vec<RuntimePendingQueueItem>,
    queued_by_runtime_class: BTreeMap<String, usize>,
    queued_by_origin: BTreeMap<String, usize>,
    cron_queued_items: usize,
    invalid_lines: usize,
}

struct RuntimePendingQueueItem {
    queue_id: String,
    runtime_class: String,
    origin: String,
    cron_run_id: Option<String>,
}

fn read_runtime_pending_queue(
    path: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<RuntimePendingQueueRead> {
    let Some(sample) = read_tail_sample(path, STATUS_JSONL_SAMPLE_BYTES)? else {
        return Ok(RuntimePendingQueueRead::default());
    };
    if sample.sampled {
        warnings.push(format!(
            "runtime pending queue is sampled from the last {} bytes of {}",
            sample.sampled_bytes,
            path.display()
        ));
    }
    let mut read = RuntimePendingQueueRead::default();
    for line in sample.text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        match serde_json::from_str::<Value>(trimmed) {
            Ok(value) => {
                let Some(queue_id) = queue_id_from_value(&value) else {
                    read.invalid_lines += 1;
                    continue;
                };
                let platform = string_path_any(&value, &["platform"]).unwrap_or_default();
                let runtime_class = string_path_any(&value, &["runtimeClass", "runtime_class"])
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or_else(|| default_runtime_class_for_platform(&platform));
                let origin = string_path_any(&value, &["origin", "adapter"])
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or_else(|| default_origin_for_platform(&platform));
                let cron_run_id = string_path_any(&value, &["cronRunId", "cron_run_id"]);
                read.queue_ids.insert(queue_id.clone());
                *read
                    .queued_by_runtime_class
                    .entry(runtime_class.clone())
                    .or_insert(0) += 1;
                *read.queued_by_origin.entry(origin.clone()).or_insert(0) += 1;
                if runtime_class == "cron" || cron_run_id.is_some() {
                    read.cron_queued_items += 1;
                }
                read.items.push(RuntimePendingQueueItem {
                    queue_id,
                    runtime_class,
                    origin,
                    cron_run_id,
                });
            }
            Err(_) => read.invalid_lines += 1,
        }
    }
    Ok(read)
}

fn runtime_class_lease_statuses<'a>(
    queue_dir: &Path,
    queued_classes: impl Iterator<Item = &'a String>,
    warnings: &mut Vec<String>,
) -> io::Result<Vec<HarnessRuntimeClassLeaseStatus>> {
    let mut classes = ["interactive", "cron", "worker", "maintenance"]
        .into_iter()
        .map(ToString::to_string)
        .collect::<BTreeSet<_>>();
    classes.extend(queued_classes.cloned());
    let classes_dir = queue_dir.join("classes");
    if let Ok(entries) = fs::read_dir(&classes_dir) {
        for entry in entries {
            let entry = entry?;
            if entry.file_type()?.is_dir()
                && let Some(name) = entry.file_name().to_str()
            {
                classes.insert(name.to_string());
            }
        }
    }
    if queue_dir.join("runtime-leases.json").is_file() {
        classes.insert("legacy".to_string());
    }

    let now_ms = epoch_ms().unwrap_or(0);
    let mut statuses = Vec::new();
    for runtime_class in classes {
        let leases_file = if runtime_class == "legacy" {
            queue_dir.join("runtime-leases.json")
        } else {
            classes_dir.join(&runtime_class).join("runtime-leases.json")
        };
        statuses.push(read_runtime_class_lease_status(
            &runtime_class,
            leases_file,
            now_ms,
            warnings,
        )?);
    }
    Ok(statuses)
}

fn read_runtime_class_lease_status(
    runtime_class: &str,
    leases_file: PathBuf,
    now_ms: i64,
    warnings: &mut Vec<String>,
) -> io::Result<HarnessRuntimeClassLeaseStatus> {
    let mut status = HarnessRuntimeClassLeaseStatus {
        runtime_class: runtime_class.to_string(),
        leases_file: leases_file.clone(),
        ..HarnessRuntimeClassLeaseStatus::default()
    };
    let text = match fs::read_to_string(&leases_file) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(status),
        Err(error) => return Err(error),
    };
    status.exists = true;
    let value = match serde_json::from_str::<Value>(&text) {
        Ok(value) => value,
        Err(error) => {
            warnings.push(format!(
                "runtime class lease file {} is invalid JSON: {error}",
                leases_file.display()
            ));
            return Ok(status);
        }
    };
    let Some(leases) = value.get("leases").and_then(Value::as_object) else {
        return Ok(status);
    };
    for lease in leases.values() {
        status.leased_items += 1;
        let lease_runtime_class = string_path_any(lease, &["runtimeClass", "runtime_class"])
            .unwrap_or_else(|| runtime_class.to_string());
        let origin = string_path_any(lease, &["origin"])
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "unknown".to_string());
        let agent_id = string_path_any(lease, &["agentId", "agent_id"])
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "unknown".to_string());
        if string_path_any(lease, &["cronRunId", "cron_run_id"]).is_some()
            || lease_runtime_class == "cron"
        {
            status.cron_run_leases += 1;
        }
        let expired = i64_path(lease, &["leaseExpiresAtMs"])
            .or_else(|| i64_path(lease, &["lease_expires_at_ms"]))
            .is_some_and(|expires_at| expires_at <= now_ms);
        if expired {
            status.expired_leases += 1;
        } else {
            status.active_leases += 1;
        }
        *status.by_agent.entry(agent_id).or_insert(0) += 1;
        *status.by_origin.entry(origin).or_insert(0) += 1;
    }
    Ok(status)
}

fn default_runtime_class_for_platform(platform: &str) -> String {
    match platform {
        "native-cron" => "cron".to_string(),
        "worker" | "worker-watchdog" => "worker".to_string(),
        _ => "interactive".to_string(),
    }
}

fn default_origin_for_platform(platform: &str) -> String {
    if platform == "native-cron" {
        "cron-scheduler".to_string()
    } else {
        "channel".to_string()
    }
}

fn read_delivery_status_tail(
    path: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<BTreeMap<String, String>> {
    let Some(sample) = read_tail_sample(path, STATUS_JSONL_SAMPLE_BYTES)? else {
        return Ok(BTreeMap::new());
    };
    if sample.sampled {
        warnings.push(format!(
            "delivery receipt status is sampled from the last {} bytes of {}",
            sample.sampled_bytes,
            path.display()
        ));
    }
    let mut statuses = BTreeMap::new();
    for line in sample.text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
            continue;
        };
        let Some(delivery_id) = string_path_any(&value, &["deliveryId", "delivery_id"]) else {
            continue;
        };
        let Some(status) = string_path_any(&value, &["status"]) else {
            continue;
        };
        statuses.insert(delivery_id, status);
    }
    Ok(statuses)
}

fn bounded_outbox_summary(
    path: &Path,
    platform: Option<&str>,
    delivery_statuses: &BTreeMap<String, String>,
    warnings: &mut Vec<String>,
) -> io::Result<ChannelOutboxPlanSummary> {
    let Some(sample) = read_tail_sample(path, STATUS_JSONL_SAMPLE_BYTES)? else {
        return Ok(ChannelOutboxPlanSummary::default());
    };
    if sample.sampled {
        warnings.push(format!(
            "channel outbox status is sampled from the last {} bytes of {}",
            sample.sampled_bytes,
            path.display()
        ));
    }
    let mut summary = ChannelOutboxPlanSummary::default();
    for (index, line) in sample.text.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        summary.total_outbox_lines += 1;
        let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
            summary.invalid_lines += 1;
            continue;
        };
        let message_platform = value.get("platform").and_then(Value::as_str);
        if platform.is_some_and(|platform| message_platform != Some(platform)) {
            summary.skipped_platform += 1;
            continue;
        }
        let delivery_id = delivery_id_for_status(index + 1, trimmed);
        match delivery_statuses.get(&delivery_id).map(String::as_str) {
            Some("delivered") => summary.delivered += 1,
            Some("failed") => {
                summary.failed_retryable += 1;
                summary.pending += 1;
            }
            _ => summary.pending += 1,
        }
    }
    Ok(summary)
}

fn delivery_id_for_status(line_number: usize, line: &str) -> String {
    format!("delivery:{line_number}:{}", fnv1a_64_hex(line))
}

fn fnv1a_64_hex(value: &str) -> String {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

fn read_receipt_queue_ids_with_status(
    path: &Path,
    statuses: &[&str],
    warnings: &mut Vec<String>,
    label: &str,
) -> io::Result<BTreeSet<String>> {
    let Some(sample) = read_tail_sample(path, STATUS_JSONL_SAMPLE_BYTES)? else {
        return Ok(BTreeSet::new());
    };
    if sample.sampled {
        warnings.push(format!(
            "{label} is sampled from the last {} bytes of {}",
            sample.sampled_bytes,
            path.display()
        ));
    }
    let mut queue_ids = BTreeSet::new();
    for line in sample.text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
            continue;
        };
        let status = value.get("status").and_then(Value::as_str);
        if status.is_some_and(|status| statuses.contains(&status))
            && let Some(queue_id) = queue_id_from_value(&value)
        {
            queue_ids.insert(queue_id);
        }
    }
    Ok(queue_ids)
}

fn queue_id_from_value(value: &Value) -> Option<String> {
    value
        .get("queueId")
        .or_else(|| value.get("queue_id"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

fn count_regular_files(root: &Path) -> io::Result<usize> {
    if !root.exists() {
        return Ok(0);
    }
    let mut count = 0;
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            count += count_regular_files(&entry.path())?;
        } else if file_type.is_file() {
            count += 1;
        }
    }
    Ok(count)
}

fn readiness_check_passed(readiness: &ActivationReadinessReport, name: &str) -> bool {
    readiness
        .checks
        .iter()
        .any(|check| check.name == name && check.status == ActivationReadinessStatus::Pass)
}

fn string_path(value: &Value, path: &[&str]) -> Option<String> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    current.as_str().map(ToString::to_string)
}

fn string_path_any(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(Value::as_str))
        .map(ToString::to_string)
}

fn i64_path(value: &Value, path: &[&str]) -> Option<i64> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    current.as_i64()
}

fn epoch_ms() -> io::Result<i64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(io::Error::other)?;
    i64::try_from(duration.as_millis()).map_err(io::Error::other)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn collect_status_summarizes_runtime_channel_and_logs() {
        let root = temp_root("collect_status_summarizes_runtime_channel_and_logs");
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(harness_home.join("state").join("runtime-queue")).unwrap();
        fs::create_dir_all(harness_home.join("state").join("channels")).unwrap();
        fs::create_dir_all(harness_home.join("state").join("logs")).unwrap();
        fs::create_dir_all(harness_home.join("state").join("plugin-sidecar")).unwrap();
        fs::create_dir_all(harness_home.join("secrets")).unwrap();
        fs::create_dir_all(
            harness_home
                .join("state")
                .join("supervisor")
                .join("loop-heartbeats"),
        )
        .unwrap();
        fs::create_dir_all(harness_home.join("state").join("memory")).unwrap();
        fs::create_dir_all(harness_home.join("memory").join("qdrant-edge")).unwrap();
        fs::write(
            harness_home.join("state").join("harness-registry.json"),
            r#"{
              "agents": [{"id":"main","enabled":true}],
              "providers": [],
              "channels": {"telegram": false, "discord": false},
              "plugins": []
            }"#,
        )
        .unwrap();
        fs::write(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("pending.jsonl"),
            r#"{"queueId":"q1"}"#,
        )
        .unwrap();
        fs::write(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("run-once-receipts.jsonl"),
            r#"{"status":"completed","reason":"ok"}"#,
        )
        .unwrap();
        fs::write(
            harness_home.join("state").join("channels").join("outbox.jsonl"),
            r#"{"kind":"command-reply","platform":"telegram","channelId":"dm","userId":"user","sessionKey":"telegram:dm:user:main","text":"ok"}"#,
        )
        .unwrap();
        fs::write(
            harness_home
                .join("state")
                .join("channels")
                .join("discord-reply-context-receipts.jsonl"),
            r#"{"status":"captured","referencedMessageId":"ref-1","reason":"ok"}"#,
        )
        .unwrap();
        fs::write(
            harness_home
                .join("state")
                .join("logs")
                .join("harness.jsonl"),
            r#"{"event":"channel.receive"}
{"event":"runtime.loop-stopped"}"#,
        )
        .unwrap();
        fs::write(
            harness_home
                .join("state")
                .join("supervisor")
                .join("loop-heartbeats")
                .join("runtime-loop.json"),
            r#"{"status":"no-work","iteration":7,"processId":42,"atMs":1000,"detail":"idle"}"#,
        )
        .unwrap();
        fs::write(
            harness_home
                .join("state")
                .join("memory")
                .join("vector-recall-receipts.jsonl"),
            r#"{"status":"ready","hitCount":2,"backend":"sqlite-vector","reason":"ok"}"#,
        )
        .unwrap();
        fs::write(
            harness_home
                .join("state")
                .join("memory")
                .join("prompt-context-receipts.jsonl"),
            r#"{"status":"ready","hitCount":1,"reason":"ok"}"#,
        )
        .unwrap();
        fs::write(
            harness_home
                .join("state")
                .join("memory")
                .join("lifecycle-receipts.jsonl"),
            r#"{"status":"recorded","episodesAppended":2,"reason":"ok"}"#,
        )
        .unwrap();
        fs::write(
            harness_home
                .join("state")
                .join("memory")
                .join("canvas-receipts.jsonl"),
            r#"{"status":"written","candidatesRead":1,"episodesRead":2,"reason":"ok"}"#,
        )
        .unwrap();
        fs::write(
            harness_home
                .join("state")
                .join("memory")
                .join("auto-capture-candidates.jsonl"),
            r#"{"kind":"candidate","text":"remember this"}
{"kind":"candidate","text":"remember that"}"#,
        )
        .unwrap();
        fs::write(
            harness_home.join("secrets").join("memory-credentials.env"),
            "AGENT_HARNESS_MEMORY_EMBEDDING_API_KEY=sk-test\n",
        )
        .unwrap();

        let report = collect_harness_status(HarnessStatusOptions {
            harness_home: harness_home.clone(),
        })
        .unwrap();

        assert_eq!(report.runtime.queued_items, 1);
        assert_eq!(report.runtime.open_items, 1);
        assert_eq!(
            report.runtime.run_once_receipts.latest_status.as_deref(),
            Some("completed")
        );
        assert_eq!(report.channels.outbox.all.pending, 1);
        assert_eq!(
            report
                .channels
                .discord_reply_context_receipts
                .latest_status
                .as_deref(),
            Some("captured")
        );
        assert!(report.memory.qdrant_edge);
        assert!(report.memory.memory_credentials_env_present);
        assert_eq!(
            report
                .memory
                .vector_recall_receipts
                .latest_status
                .as_deref(),
            Some("ready")
        );
        assert_eq!(
            report
                .memory
                .vector_recall_receipts
                .latest_backend
                .as_deref(),
            Some("sqlite-vector")
        );
        assert_eq!(report.memory.summary.active_recall_backend, "sqlite-vector");
        assert_eq!(
            report.memory.summary.qdrant_parity,
            "snapshot-preserved; native-recall-not-active"
        );
        assert_eq!(report.memory.summary.capture_candidate_count, 2);
        assert_eq!(
            report
                .memory
                .prompt_context_receipts
                .latest_status
                .as_deref(),
            Some("ready")
        );
        assert_eq!(
            report.memory.lifecycle_receipts.latest_status.as_deref(),
            Some("recorded")
        );
        assert_eq!(
            report.memory.canvas_receipts.latest_status.as_deref(),
            Some("written")
        );
        let runtime_loop = report
            .loops
            .heartbeats
            .iter()
            .find(|heartbeat| heartbeat.name == "runtime-loop")
            .unwrap();
        assert!(runtime_loop.present);
        assert_eq!(runtime_loop.status.as_deref(), Some("no-work"));
        assert_eq!(runtime_loop.iteration, Some(7));
        assert_eq!(runtime_loop.process_id, Some(42));
        assert_eq!(
            report.logs.event_present.get("channel.receive").copied(),
            Some(true)
        );
        assert_eq!(
            report
                .logs
                .event_present
                .get("runtime.loop-stopped")
                .copied(),
            Some(true)
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn harness_status_reports_memory_capability_mode() {
        let root = temp_root("harness_status_reports_memory_capability_mode");
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(harness_home.join("state").join("memory")).unwrap();
        fs::create_dir_all(harness_home.join("memory").join("qdrant-edge")).unwrap();

        crate::memory::store_openclaw_mem_service_memory(
            crate::memory::OpenClawMemServiceStoreOptions {
                harness_home: harness_home.clone(),
                agent_id: None,
                session_key: Some("telegram:dm:user:main".to_string()),
                text: "Global OpenClawMem service writeback".to_string(),
                payload: serde_json::json!({"source": "status-test"}),
                approved: true,
                now_ms: 1_800_000_010_000,
            },
        )
        .unwrap();
        crate::memory::propose_openclaw_mem_service_memory(
            crate::memory::OpenClawMemServiceProposeOptions {
                harness_home: harness_home.clone(),
                agent_id: None,
                session_key: Some("telegram:dm:user:main".to_string()),
                text: "Global OpenClawMem active store proposal".to_string(),
                payload: serde_json::json!({"source": "status-test"}),
                now_ms: 1_800_000_010_001,
            },
        )
        .unwrap();
        fs::write(
            harness_home
                .join("state")
                .join("memory")
                .join("store-proposals.jsonl"),
            r#"{"status":"pending-review","kind":"generic-store-proposal"}"#,
        )
        .unwrap();
        fs::write(
            harness_home
                .join("state")
                .join("memory")
                .join("slot-receipts.jsonl"),
            r#"{"status":"recorded","owner":"snapshot-adapter"}"#,
        )
        .unwrap();
        fs::write(
            harness_home
                .join("state")
                .join("memory")
                .join("auto-capture-candidates.jsonl"),
            r#"{"kind":"candidate","text":"remember capability mode"}"#,
        )
        .unwrap();

        let report = collect_harness_status(HarnessStatusOptions {
            harness_home: harness_home.clone(),
        })
        .unwrap();

        assert_eq!(report.memory.summary.active_recall_backend, "none");
        assert_eq!(
            report.memory.summary.qdrant_parity,
            "snapshot-preserved; native-recall-not-active"
        );
        assert_eq!(report.memory.summary.adapter_readiness, "ready");
        assert_eq!(
            report.memory.summary.capability_mode,
            "snapshot-adapter-ready"
        );
        assert_eq!(
            report.memory.summary.mem_engine_ownership.active_owner,
            "snapshot-adapter"
        );
        assert!(!report.memory.summary.mem_engine_ownership.promotion_ready);
        assert_eq!(
            report.memory.summary.qdrant_native_recall,
            "snapshot-preserved-native-recall-inactive"
        );
        assert_eq!(
            report
                .memory
                .summary
                .semantic_coverage
                .service_writeback
                .items,
            Some(1)
        );
        assert_eq!(
            report
                .memory
                .summary
                .semantic_coverage
                .active_store_proposals
                .items,
            Some(1)
        );
        assert_eq!(report.memory.summary.capture_candidate_count, 1);
        assert_eq!(report.memory.summary.store_proposal_count, 1);
        assert_eq!(report.memory.summary.slot_receipt_count, 1);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn collect_status_treats_timeout_run_once_receipt_as_closed() {
        let root = temp_root("collect_status_treats_timeout_run_once_receipt_as_closed");
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(harness_home.join("state").join("runtime-queue")).unwrap();
        fs::create_dir_all(harness_home.join("state").join("channels")).unwrap();
        fs::create_dir_all(harness_home.join("state").join("logs")).unwrap();
        fs::create_dir_all(harness_home.join("state").join("plugin-sidecar")).unwrap();
        fs::create_dir_all(harness_home.join("state").join("memory")).unwrap();
        fs::create_dir_all(
            harness_home
                .join("state")
                .join("supervisor")
                .join("loop-heartbeats"),
        )
        .unwrap();
        fs::write(
            harness_home.join("state").join("harness-registry.json"),
            r#"{
              "agents": [{"id":"main","enabled":true}],
              "providers": [],
              "channels": {"telegram": false, "discord": false},
              "plugins": []
            }"#,
        )
        .unwrap();
        fs::write(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("pending.jsonl"),
            r#"{"queueId":"q-timeout","status":"queued"}"#,
        )
        .unwrap();
        fs::write(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("run-once-receipts.jsonl"),
            r#"{"queueId":"q-timeout","status":"timeout","reason":"idle timeout"}"#,
        )
        .unwrap();

        let report = collect_harness_status(HarnessStatusOptions {
            harness_home: harness_home.clone(),
        })
        .unwrap();

        assert_eq!(report.runtime.queued_items, 1);
        assert_eq!(report.runtime.open_items, 0);
        assert_eq!(
            report
                .runtime
                .latest_non_idle_run_once
                .as_ref()
                .and_then(|receipt| receipt.status.as_deref()),
            Some("timeout")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn collect_status_summarizes_cron_runtime_class_and_leases() {
        let root = temp_root("collect_status_summarizes_cron_runtime_class_and_leases");
        let harness_home = root.join(".agent-harness");
        let runtime_queue_dir = harness_home.join("state").join("runtime-queue");
        fs::create_dir_all(&runtime_queue_dir).unwrap();
        fs::create_dir_all(harness_home.join("state").join("channels")).unwrap();
        fs::create_dir_all(harness_home.join("state").join("logs")).unwrap();
        fs::create_dir_all(harness_home.join("state").join("plugin-sidecar")).unwrap();
        fs::create_dir_all(harness_home.join("state").join("memory")).unwrap();
        fs::create_dir_all(
            harness_home
                .join("state")
                .join("supervisor")
                .join("loop-heartbeats"),
        )
        .unwrap();
        fs::write(
            harness_home.join("state").join("harness-registry.json"),
            r#"{
              "agents": [{"id":"main","enabled":true}],
              "providers": [],
              "channels": {"telegram": false, "discord": false},
              "plugins": []
            }"#,
        )
        .unwrap();

        let cron_run = crate::admit_cron_run(crate::CronRunAdmitOptions {
            harness_home: harness_home.clone(),
            source_kind: "native-cron".to_string(),
            source_id: "main".to_string(),
            entry_id: "hourly".to_string(),
            agent_id: "main@cron".to_string(),
            scheduled_for_ms: 1_000,
            runtime_class: "cron".to_string(),
            session_key: "cron:main:hourly:1000".to_string(),
            session_policy: "one-shot".to_string(),
            max_attempts: 3,
            now_ms: 1_100,
        })
        .unwrap();

        fs::write(
            runtime_queue_dir.join("pending.jsonl"),
            format!(
                "{}\n{}",
                serde_json::json!({
                    "queueId": "q-cron",
                    "agentId": "main@cron",
                    "sessionKey": "cron:main:hourly:1000",
                    "platform": "native-cron",
                    "runtimeClass": "cron",
                    "origin": "cron-scheduler",
                    "cronRunId": cron_run.run_id.clone(),
                    "scheduledForMs": 1000
                }),
                serde_json::json!({
                    "queueId": "q-main",
                    "agentId": "main",
                    "sessionKey": "telegram:dm:user:main",
                    "platform": "telegram",
                    "runtimeClass": "interactive",
                    "origin": "channel"
                })
            ),
        )
        .unwrap();
        fs::write(
            runtime_queue_dir.join("run-once-receipts.jsonl"),
            serde_json::json!({
                "queueId": "q-cron",
                "status": "retry-pending",
                "reason": "lease busy",
                "runtimeClass": "cron",
                "origin": "cron-scheduler",
                "cronRunId": cron_run.run_id.clone(),
                "scheduledForMs": 1000
            })
            .to_string(),
        )
        .unwrap();
        let cron_class_dir = runtime_queue_dir.join("classes").join("cron");
        fs::create_dir_all(&cron_class_dir).unwrap();
        fs::write(
            cron_class_dir.join("runtime-leases.json"),
            serde_json::json!({
                "schema": "agent-harness.runtime-queue-leases.v1",
                "leases": {
                    "q-cron": {
                        "queueId": "q-cron",
                        "agentId": "main@cron",
                        "runtimeClass": "cron",
                        "origin": "cron-scheduler",
                        "cronRunId": cron_run.run_id.clone(),
                        "platform": "native-cron",
                        "channelId": "cron",
                        "sessionKey": "cron:main:hourly:1000",
                        "owner": "test",
                        "startedAtMs": 1000,
                        "leaseExpiresAtMs": i64::MAX
                    }
                }
            })
            .to_string(),
        )
        .unwrap();

        let report = collect_harness_status(HarnessStatusOptions {
            harness_home: harness_home.clone(),
        })
        .unwrap();

        assert_eq!(report.runtime.queued_items, 2);
        assert_eq!(report.runtime.open_items, 2);
        assert_eq!(report.runtime.cron_queued_items, 1);
        assert_eq!(report.runtime.cron_open_items, 1);
        assert_eq!(
            report.runtime.queued_by_runtime_class.get("cron").copied(),
            Some(1)
        );
        assert_eq!(
            report.runtime.open_by_origin.get("cron-scheduler").copied(),
            Some(1)
        );
        let cron_leases = report
            .runtime
            .class_leases
            .iter()
            .find(|status| status.runtime_class == "cron")
            .unwrap();
        assert_eq!(cron_leases.active_leases, 1);
        assert_eq!(cron_leases.cron_run_leases, 1);
        assert_eq!(report.cron_runs.summary.active, 1);
        assert_eq!(
            report
                .cron_runs
                .summary
                .by_agent_active
                .get("main@cron")
                .copied(),
            Some(1)
        );
        let latest = report.runtime.latest_non_idle_run_once.unwrap();
        assert_eq!(latest.runtime_class.as_deref(), Some("cron"));
        assert_eq!(latest.origin.as_deref(), Some("cron-scheduler"));
        assert_eq!(
            latest.cron_run_id.as_deref(),
            Some(cron_run.run_id.as_str())
        );
        assert_eq!(latest.scheduled_for_ms, Some(1000));

        let _ = fs::remove_dir_all(root);
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-status-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
