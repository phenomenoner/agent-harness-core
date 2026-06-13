use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use serde_json::Value;

use crate::{
    ActivationReadinessOptions, ActivationReadinessReport, ActivationReadinessStatus,
    ChannelOutboxPlanOptions, ChannelOutboxPlanSummary, WorkerStatusOptions, WorkerStatusReport,
    check_activation_readiness, collect_worker_status, harness_log_file, plan_channel_outbox,
};

const HARNESS_STATUS_SCHEMA: &str = "agent-harness.status.v1";

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
    pub memory: HarnessMemoryStatus,
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
    pub latest_enqueued: Option<i64>,
    pub latest_errors: Option<i64>,
    pub receipts: HarnessJsonlStatus,
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
    pub capture_candidates: HarnessJsonlStatus,
    pub summary: HarnessMemoryHealthSummary,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessMemoryHealthSummary {
    pub active_recall_backend: String,
    pub qdrant_parity: String,
    pub prompt_context_status: Option<String>,
    pub lifecycle_status: Option<String>,
    pub canvas_status: Option<String>,
    pub hook_status: Option<String>,
    pub capture_candidate_count: usize,
    pub store_proposal_count: usize,
    pub slot_receipt_count: usize,
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
}

pub fn collect_harness_status(options: HarnessStatusOptions) -> io::Result<HarnessStatusReport> {
    let readiness = check_activation_readiness(ActivationReadinessOptions {
        harness_home: options.harness_home.clone(),
    })?;
    let mut warnings = Vec::new();
    let channels = channel_status(&options.harness_home, &readiness, &mut warnings)?;
    let logs = log_status(&options.harness_home)?;
    let runtime = runtime_status(&options.harness_home)?;
    let loops = loop_status(&options.harness_home)?;
    let workers = collect_worker_status(WorkerStatusOptions {
        harness_home: options.harness_home.clone(),
    })?;
    let cron_scheduler = cron_scheduler_status(&options.harness_home)?;
    let memory = memory_status(&options.harness_home)?;
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
        memory,
        plugins,
        logs,
        warnings,
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
        latest_enqueued: latest
            .as_ref()
            .and_then(|value| i64_path(value, &["summary", "enqueued"])),
        latest_errors: latest
            .as_ref()
            .and_then(|value| i64_path(value, &["summary", "errors"])),
        loop_last_file,
        receipts: jsonl_status(state_dir.join("receipts.jsonl"))?,
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

fn runtime_status(harness_home: &Path) -> io::Result<HarnessRuntimeStatus> {
    let queue_dir = harness_home.join("state").join("runtime-queue");
    let pending_file = queue_dir.join("pending.jsonl");
    let pending = read_queue_ids(&pending_file)?;
    let prepared_ids = read_receipt_queue_ids_with_status(
        &queue_dir.join("execution-receipts.jsonl"),
        &["prepared", "already-prepared"],
    )?;
    let completed_ids = read_receipt_queue_ids_with_status(
        &queue_dir.join("codex-runtime-completion-receipts.jsonl"),
        &["recorded", "already-recorded"],
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
    )?;
    let open_items = pending.queue_ids.difference(&terminal_run_ids).count();
    Ok(HarnessRuntimeStatus {
        pending_file,
        queued_items: pending.queue_ids.len(),
        prepared_items: pending.queue_ids.intersection(&prepared_ids).count(),
        completed_items: pending.queue_ids.intersection(&completed_ids).count(),
        open_items,
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
    let all = plan_channel_outbox(ChannelOutboxPlanOptions {
        harness_home: harness_home.to_path_buf(),
        platform: None,
        limit: usize::MAX,
    })?;
    warnings.extend(all.warnings.clone());
    let telegram = plan_channel_outbox(ChannelOutboxPlanOptions {
        harness_home: harness_home.to_path_buf(),
        platform: Some("telegram".to_string()),
        limit: usize::MAX,
    })?;
    warnings.extend(telegram.warnings.clone());
    let discord = plan_channel_outbox(ChannelOutboxPlanOptions {
        harness_home: harness_home.to_path_buf(),
        platform: Some("discord".to_string()),
        limit: usize::MAX,
    })?;
    warnings.extend(discord.warnings.clone());
    let telegram_offset_file = harness_home
        .join("state")
        .join("channels")
        .join("telegram-offset.json");
    let channel_dir = harness_home.join("state").join("channels");

    Ok(HarnessChannelStatus {
        outbox: HarnessOutboxStatus {
            all: all.summary,
            telegram: telegram.summary,
            discord: discord.summary,
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
    let capture_candidates = jsonl_status(memory_state_dir.join("auto-capture-candidates.jsonl"))?;
    let summary = memory_health_summary(
        qdrant_edge,
        legacy_mem_sqlite,
        &vector_recall_receipts,
        &prompt_context_receipts,
        &lifecycle_receipts,
        &canvas_receipts,
        &hook_receipts,
        &store_proposals,
        &slot_receipts,
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
        capture_candidates,
        summary,
        memory_dir,
    })
}

fn memory_health_summary(
    qdrant_edge: bool,
    legacy_mem_sqlite: bool,
    vector_recall_receipts: &HarnessJsonlStatus,
    prompt_context_receipts: &HarnessJsonlStatus,
    lifecycle_receipts: &HarnessJsonlStatus,
    canvas_receipts: &HarnessJsonlStatus,
    hook_receipts: &HarnessJsonlStatus,
    store_proposals: &HarnessJsonlStatus,
    slot_receipts: &HarnessJsonlStatus,
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
        prompt_context_status: prompt_context_receipts.latest_status.clone(),
        lifecycle_status: lifecycle_receipts.latest_status.clone(),
        canvas_status: canvas_receipts.latest_status.clone(),
        hook_status: hook_receipts.latest_status.clone(),
        capture_candidate_count: capture_candidates.lines,
        store_proposal_count: store_proposals.lines,
        slot_receipt_count: slot_receipts.lines,
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
    let mut event_present = EVENTS
        .iter()
        .map(|event| ((*event).to_string(), false))
        .collect::<BTreeMap<_, _>>();

    match fs::read_to_string(&log_file) {
        Ok(text) => {
            for line in text.lines() {
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
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }

    Ok(HarnessOperationalLogStatus {
        exists: log_file.is_file(),
        log_file,
        lines,
        invalid_lines,
        latest_event,
        event_present,
    })
}

fn latest_non_idle_runtime_receipt(path: &Path) -> io::Result<Option<HarnessRuntimeReceiptStatus>> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let mut latest = None;
    for (index, line) in text.lines().enumerate() {
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
        });
    }
    Ok(latest)
}

fn jsonl_status(path: PathBuf) -> io::Result<HarnessJsonlStatus> {
    let mut status = HarnessJsonlStatus {
        path,
        ..HarnessJsonlStatus::default()
    };
    let text = match fs::read_to_string(&status.path) {
        Ok(text) => {
            status.exists = true;
            text
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(status),
        Err(error) => return Err(error),
    };
    for line in text.lines() {
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

#[derive(Default)]
struct QueueIdRead {
    queue_ids: BTreeSet<String>,
    invalid_lines: usize,
}

fn read_queue_ids(path: &Path) -> io::Result<QueueIdRead> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(QueueIdRead::default()),
        Err(error) => return Err(error),
    };
    let mut read = QueueIdRead::default();
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        match serde_json::from_str::<Value>(trimmed) {
            Ok(value) => {
                if let Some(queue_id) = queue_id_from_value(&value) {
                    read.queue_ids.insert(queue_id);
                } else {
                    read.invalid_lines += 1;
                }
            }
            Err(_) => read.invalid_lines += 1,
        }
    }
    Ok(read)
}

fn read_receipt_queue_ids_with_status(
    path: &Path,
    statuses: &[&str],
) -> io::Result<BTreeSet<String>> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(BTreeSet::new()),
        Err(error) => return Err(error),
    };
    let mut queue_ids = BTreeSet::new();
    for line in text.lines() {
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
