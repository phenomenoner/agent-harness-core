use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use serde_json::Value;

use crate::loop_health::{process_alive_for_pid, read_supervisor_stop_file};
use crate::memory::{
    MemoryMemEngineOwnershipReport, MemorySemanticCoverageReport,
    collect_memory_embedding_coverage, collect_memory_semantic_coverage,
    memory_adapter_readiness_report, memory_capability_mode_from_readiness,
    memory_mem_engine_canary_report, memory_mem_engine_ownership_report_for_owner_state,
    memory_qdrant_native_recall_status, openclaw_mem_service_store_file_for_agent,
};
use crate::memory_owner::{MemoryOwnerState, read_memory_owner_state_or_default};
use crate::runtime_receipt_history::find_runtime_queue_terminal_history;
use crate::runtime_worker::{refresh_runtime_queue_state_index, terminal_run_once_ids_from_index};
use crate::skill_apply::skill_apply_receipts_file;
use crate::skill_doctor::skill_doctor_receipts_file;
use crate::skill_learning::skill_proposals_file;
use crate::skill_usage::{skill_usage_events_file, skill_usage_snapshot_file};
use crate::{
    ActivationReadinessOptions, ActivationReadinessReport, ActivationReadinessStatus,
    ChannelOutboxPlanSummary, CronRunSummary, SkillDoctorOptions, SkillDoctorStatus,
    SkillDoctorSummary, WorkerStatusOptions, WorkerStatusReport, check_activation_readiness,
    collect_cron_run_summary, collect_worker_status, cron_runs_db_file, harness_log_file,
    run_skill_doctor,
};

const HARNESS_STATUS_SCHEMA: &str = "agent-harness.status.v1";
const STATUS_JSONL_SAMPLE_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarnessStatusOptions {
    pub harness_home: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
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
    pub skills: HarnessSkillStatus,
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
    pub services_dir: PathBuf,
    pub services: Vec<HarnessSupervisorServiceStatus>,
    pub gateway_restart_requests: HarnessJsonlStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessLoopHeartbeatStatus {
    pub name: String,
    pub heartbeat_file: PathBuf,
    pub present: bool,
    pub corrupt: bool,
    pub parse_error: Option<String>,
    pub status: Option<String>,
    pub iteration: Option<i64>,
    pub process_id: Option<i64>,
    pub process_alive: Option<bool>,
    pub generation_id: Option<String>,
    pub parent_pid: Option<i64>,
    pub process_start_time_ms: Option<i64>,
    pub watched_stop_file: Option<PathBuf>,
    pub launch_owner: Option<String>,
    pub observed_only: Option<bool>,
    pub at_ms: Option<i64>,
    pub age_ms: Option<i64>,
    pub detail: Option<String>,
    pub stop_file: PathBuf,
    pub stop_file_present: bool,
    pub stop_file_reason: Option<String>,
    pub stop_file_service_id: Option<String>,
    pub stop_file_created_by: Option<String>,
    pub stop_file_created_at_ms: Option<i64>,
    pub stop_file_expires_at_ms: Option<i64>,
    pub stop_file_persistent: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessSupervisorServiceStatus {
    pub service_id: String,
    pub service_file: PathBuf,
    pub present: bool,
    pub corrupt: bool,
    pub parse_error: Option<String>,
    pub service_kind: Option<String>,
    pub generation_id: Option<String>,
    pub process_id: Option<i64>,
    pub process_alive: Option<bool>,
    pub supervisor_process_id: Option<i64>,
    pub parent_pid: Option<i64>,
    pub started_at_ms: Option<i64>,
    pub process_start_time_ms: Option<i64>,
    pub last_heartbeat_at_ms: Option<i64>,
    pub last_successful_iteration_at_ms: Option<i64>,
    pub last_exit_at_ms: Option<i64>,
    pub last_exit_code: Option<i64>,
    pub last_error_class: Option<String>,
    pub restart_count: Option<i64>,
    pub backoff_until_ms: Option<i64>,
    pub service_priority: Option<String>,
    pub delivery_lane: Option<String>,
    pub restart_delay_ms: Option<i64>,
    pub memory_gate_action: Option<String>,
    pub memory_gate_reason: Option<String>,
    pub age_ms: Option<i64>,
    pub iteration: Option<i64>,
    pub status: Option<String>,
    pub desired_state: Option<String>,
    pub actual_state: Option<String>,
    pub detail: Option<String>,
    pub launch_owner: Option<String>,
    pub observed_only: Option<bool>,
    pub watched_stop_file: Option<PathBuf>,
    pub ownership_conflict: bool,
    pub ownership_conflict_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
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
    pub canon: HarnessCronCanonStatus,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessCronCanonStatus {
    pub canon_path: PathBuf,
    pub canon_present: bool,
    pub keeper_receipt_path: PathBuf,
    pub keeper_receipt_present: bool,
    pub keeper_status: Option<String>,
    pub keeper_ok: Option<bool>,
    pub keeper_age_ms: Option<i64>,
    pub keeper_age_hours: Option<f64>,
    pub monitor_count: usize,
    pub finding_count: usize,
    pub stale_count: usize,
    pub findings: Vec<HarnessCronCanonFinding>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessCronCanonFinding {
    pub severity: String,
    pub code: String,
    pub cron_id: String,
    pub message: String,
    pub path: Option<PathBuf>,
    pub age_ms: Option<i64>,
    pub max_age_ms: Option<i64>,
    pub age_hours: Option<f64>,
    pub max_age_hours: Option<f64>,
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
    pub service_recall_receipts: HarnessJsonlStatus,
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
    pub support_plane: HarnessOpenClawMemSupportPlaneStatus,
    pub summary: HarnessMemoryHealthSummary,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessOpenClawMemSupportPlaneStatus {
    pub db_path: PathBuf,
    pub db_exists: bool,
    pub topology_candidates: Vec<HarnessMemoryTopologyCandidateStatus>,
    pub selected_topology_source: Option<PathBuf>,
    pub service_store_path: PathBuf,
    pub service_store_exists: bool,
    pub writeback_store_path: PathBuf,
    pub writeback_store_exists: bool,
    pub graph_autonomous_matching_ready: bool,
    pub missing_reasons: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessMemoryTopologyCandidateStatus {
    pub path: PathBuf,
    pub exists: bool,
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
pub struct HarnessSkillStatus {
    pub status: SkillDoctorStatus,
    pub summary: SkillDoctorSummary,
    pub findings: usize,
    pub error_findings: usize,
    pub warning_findings: usize,
    pub doctor_receipts_file: PathBuf,
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
pub struct GatewayRestartStatusReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub requests: HarnessJsonlStatus,
    pub completions: HarnessJsonlStatus,
    pub latest_request: Option<GatewayRestartReceiptStatus>,
    pub latest_consumption: Option<GatewayRestartReceiptStatus>,
    pub latest_completion: Option<GatewayRestartCompletionStatus>,
    pub service: GatewayRestartServiceStatus,
    pub heartbeat: GatewayRestartHeartbeatStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GatewayRestartReceiptStatus {
    pub status: Option<String>,
    pub request_file: Option<PathBuf>,
    pub consumed_request_file: Option<PathBuf>,
    pub receipt_file: Option<PathBuf>,
    pub reason: Option<String>,
    pub requesting_platform: Option<String>,
    pub channel_id: Option<String>,
    pub user_id: Option<String>,
    pub session_key: Option<String>,
    pub at_ms: Option<i64>,
    pub consumed_at_ms: Option<i64>,
    pub consumed_by: Option<String>,
    pub consumer_pid: Option<i64>,
    pub parent_pid: Option<i64>,
    pub generation_id: Option<String>,
    pub process_start_time_ms: Option<i64>,
    pub stop_file: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GatewayRestartCompletionStatus {
    pub status: Option<String>,
    pub request_file: Option<PathBuf>,
    pub consumed_request_file: Option<PathBuf>,
    pub consumed_at_ms: Option<i64>,
    pub consumed_by: Option<String>,
    pub consumer_pid: Option<i64>,
    pub generation_id: Option<String>,
    pub process_start_time_ms: Option<i64>,
    pub heartbeat_status: Option<String>,
    pub heartbeat_generation_id: Option<String>,
    pub heartbeat_process_id: Option<i64>,
    pub heartbeat_at_ms: Option<i64>,
    pub notified: Option<bool>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GatewayRestartServiceStatus {
    pub service_file: PathBuf,
    pub present: bool,
    pub corrupt: bool,
    pub parse_error: Option<String>,
    pub status: Option<String>,
    pub actual_state: Option<String>,
    pub generation_id: Option<String>,
    pub process_id: Option<i64>,
    pub process_alive: Option<bool>,
    pub supervisor_process_id: Option<i64>,
    pub process_start_time_ms: Option<i64>,
    pub last_heartbeat_at_ms: Option<i64>,
    pub launch_owner: Option<String>,
    pub observed_only: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GatewayRestartHeartbeatStatus {
    pub heartbeat_file: PathBuf,
    pub present: bool,
    pub corrupt: bool,
    pub parse_error: Option<String>,
    pub status: Option<String>,
    pub generation_id: Option<String>,
    pub process_id: Option<i64>,
    pub process_alive: Option<bool>,
    pub at_ms: Option<i64>,
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
    append_loop_health_warnings(&loops, &mut warnings);
    let workers = collect_worker_status(WorkerStatusOptions {
        harness_home: options.harness_home.clone(),
    })?;
    let cron_scheduler = cron_scheduler_status(&options.harness_home)?;
    warnings.extend(cron_scheduler.warnings.clone());
    let cron_runs = cron_runs_status(&options.harness_home)?;
    let memory = memory_status(&options.harness_home)?;
    let learning = learning_status(&options.harness_home)?;
    let skills = skill_status(&options.harness_home, &mut warnings)?;
    let plugins = plugin_status(&options.harness_home)?;
    let ready = readiness.ready && skills.status != SkillDoctorStatus::Error;

    Ok(HarnessStatusReport {
        schema: HARNESS_STATUS_SCHEMA,
        harness_home: options.harness_home,
        ready,
        readiness,
        runtime,
        channels,
        loops,
        workers,
        cron_scheduler,
        cron_runs,
        memory,
        learning,
        skills,
        plugins,
        logs,
        warnings,
    })
}

pub fn collect_gateway_restart_status(
    harness_home: impl AsRef<Path>,
) -> io::Result<GatewayRestartStatusReport> {
    let harness_home = harness_home.as_ref();
    let supervisor_dir = harness_home.join("state").join("supervisor");
    let requests_file = supervisor_dir.join("gateway-restart-requests.jsonl");
    let completions_file = supervisor_dir.join("gateway-restart-completions.jsonl");
    let latest_request = latest_gateway_restart_receipt(&requests_file, Some("requested"))?;
    let latest_consumption = latest_gateway_restart_receipt(&requests_file, Some("consumed"))?;
    let latest_completion = latest_gateway_restart_completion(&completions_file)?;
    Ok(GatewayRestartStatusReport {
        schema: "agent-harness.gateway-restart-status.v1",
        harness_home: harness_home.to_path_buf(),
        requests: jsonl_status(requests_file)?,
        completions: jsonl_status(completions_file)?,
        latest_request,
        latest_consumption,
        latest_completion,
        service: gateway_restart_service_status(harness_home)?,
        heartbeat: gateway_restart_heartbeat_status(harness_home)?,
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

fn skill_status(harness_home: &Path, warnings: &mut Vec<String>) -> io::Result<HarnessSkillStatus> {
    let report = run_skill_doctor(SkillDoctorOptions {
        harness_home: harness_home.to_path_buf(),
        write_receipt: false,
        now_ms: epoch_ms().unwrap_or(0),
    })?;
    if report.status == SkillDoctorStatus::Error {
        warnings
            .push("skill doctor reports errors; autonomous skill apply is not ready".to_string());
    } else if report.status == SkillDoctorStatus::Warn {
        warnings.push(
            "skill doctor reports warnings; inspect status.skills before cutover".to_string(),
        );
    }
    let error_findings = report
        .findings
        .iter()
        .filter(|finding| finding.status == SkillDoctorStatus::Error)
        .count();
    let warning_findings = report
        .findings
        .iter()
        .filter(|finding| finding.status == SkillDoctorStatus::Warn)
        .count();
    Ok(HarnessSkillStatus {
        status: report.status,
        summary: report.summary,
        findings: report.findings.len(),
        error_findings,
        warning_findings,
        doctor_receipts_file: skill_doctor_receipts_file(harness_home),
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
    let mut loop_names: Vec<String> = LOOP_NAMES.iter().map(|name| (*name).to_string()).collect();
    let mut seen: BTreeSet<String> = loop_names.iter().cloned().collect();
    for name in extra_loop_names(harness_home, &heartbeat_dir)? {
        if seen.insert(name.clone()) {
            loop_names.push(name);
        }
    }
    let mut heartbeats = Vec::new();
    for name in loop_names {
        let heartbeat_file = heartbeat_dir.join(format!("{name}.json"));
        let heartbeat = read_loop_heartbeat(harness_home, &name, &heartbeat_file, now_ms)?;
        heartbeats.push(heartbeat);
    }
    let services_dir = harness_home
        .join("state")
        .join("supervisor")
        .join("services");
    let mut services = read_supervisor_services(&services_dir, now_ms)?;
    apply_fresh_heartbeat_precedence_to_services(&mut services, &heartbeats, now_ms);
    Ok(HarnessLoopStatus {
        heartbeat_dir,
        heartbeats,
        services_dir,
        services,
        gateway_restart_requests: jsonl_status(
            harness_home
                .join("state")
                .join("supervisor")
                .join("gateway-restart-requests.jsonl"),
        )?,
    })
}

fn read_supervisor_services(
    services_dir: &Path,
    now_ms: i64,
) -> io::Result<Vec<HarnessSupervisorServiceStatus>> {
    let mut service_files = Vec::new();
    match fs::read_dir(services_dir) {
        Ok(entries) => {
            for entry in entries {
                let entry = entry?;
                let path = entry.path();
                if path.extension().and_then(|value| value.to_str()) == Some("json") {
                    service_files.push(path);
                }
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    service_files.sort();

    let mut services = Vec::new();
    for service_file in service_files {
        let fallback_service_id = service_file
            .file_stem()
            .and_then(|value| value.to_str())
            .unwrap_or("unknown-service")
            .to_string();
        let text = fs::read_to_string(&service_file)?;
        let value = match serde_json::from_str::<Value>(&text) {
            Ok(value) => value,
            Err(error) => {
                services.push(HarnessSupervisorServiceStatus {
                    service_id: fallback_service_id,
                    service_file,
                    present: true,
                    corrupt: true,
                    parse_error: Some(error.to_string()),
                    service_kind: None,
                    generation_id: None,
                    process_id: None,
                    process_alive: None,
                    supervisor_process_id: None,
                    parent_pid: None,
                    started_at_ms: None,
                    process_start_time_ms: None,
                    last_heartbeat_at_ms: None,
                    last_successful_iteration_at_ms: None,
                    last_exit_at_ms: None,
                    last_exit_code: None,
                    last_error_class: None,
                    restart_count: None,
                    backoff_until_ms: None,
                    service_priority: None,
                    delivery_lane: None,
                    restart_delay_ms: None,
                    memory_gate_action: None,
                    memory_gate_reason: None,
                    age_ms: None,
                    iteration: None,
                    status: None,
                    desired_state: None,
                    actual_state: None,
                    detail: None,
                    launch_owner: None,
                    observed_only: None,
                    watched_stop_file: None,
                    ownership_conflict: false,
                    ownership_conflict_reason: None,
                });
                continue;
            }
        };
        let process_id = i64_path(&value, &["pid"]).or_else(|| i64_path(&value, &["processId"]));
        let last_heartbeat_at_ms = i64_path(&value, &["lastHeartbeatAtMs"]);
        services.push(HarnessSupervisorServiceStatus {
            service_id: string_path(&value, &["serviceId"]).unwrap_or(fallback_service_id),
            service_file,
            present: true,
            corrupt: false,
            parse_error: None,
            service_kind: string_path(&value, &["serviceKind"]),
            generation_id: string_path(&value, &["generationId"]),
            process_id,
            process_alive: process_id.and_then(process_alive_for_pid),
            supervisor_process_id: i64_path(&value, &["supervisorPid"]),
            parent_pid: i64_path(&value, &["parentPid"])
                .or_else(|| i64_path(&value, &["supervisorPid"])),
            started_at_ms: i64_path(&value, &["startedAtMs"]),
            process_start_time_ms: i64_path(&value, &["processStartTimeMs"]),
            last_heartbeat_at_ms,
            last_successful_iteration_at_ms: i64_path(&value, &["lastSuccessfulIterationAtMs"]),
            last_exit_at_ms: i64_path(&value, &["lastExitAtMs"]),
            last_exit_code: i64_path(&value, &["lastExitCode"]),
            last_error_class: string_path(&value, &["lastErrorClass"]),
            restart_count: i64_path(&value, &["restartCount"]),
            backoff_until_ms: i64_path(&value, &["backoffUntilMs"]),
            service_priority: string_path(&value, &["servicePriority"]),
            delivery_lane: string_path(&value, &["deliveryLane"]),
            restart_delay_ms: i64_path(&value, &["restartDelayMs"]),
            memory_gate_action: string_path(&value, &["memoryGateDecision", "action"]),
            memory_gate_reason: string_path(&value, &["memoryGateDecision", "reason"]),
            age_ms: last_heartbeat_at_ms.map(|at_ms| now_ms.saturating_sub(at_ms)),
            iteration: i64_path(&value, &["iteration"]),
            status: string_path(&value, &["status"]),
            desired_state: string_path(&value, &["desiredState"]),
            actual_state: string_path(&value, &["actualState"]),
            detail: string_path(&value, &["detail"]),
            launch_owner: string_path(&value, &["launchOwner"]),
            observed_only: bool_path(&value, &["observedOnly"]),
            watched_stop_file: pathbuf_path(&value, &["watchedStopFile"]),
            ownership_conflict: bool_path(&value, &["ownershipConflict"])
                .unwrap_or_else(|| bool_path(&value, &["observedOnly"]).unwrap_or(false)),
            ownership_conflict_reason: string_path(&value, &["ownershipConflictReason"]).or_else(
                || {
                    if bool_path(&value, &["observedOnly"]) == Some(true) {
                        Some("observed-only-owner".to_string())
                    } else {
                        None
                    }
                },
            ),
        });
    }

    Ok(services)
}

fn apply_fresh_heartbeat_precedence_to_services(
    services: &mut [HarnessSupervisorServiceStatus],
    heartbeats: &[HarnessLoopHeartbeatStatus],
    now_ms: i64,
) {
    let heartbeats_by_name: BTreeMap<&str, &HarnessLoopHeartbeatStatus> = heartbeats
        .iter()
        .map(|heartbeat| (heartbeat.name.as_str(), heartbeat))
        .collect();

    for service in services {
        let Some(heartbeat) = heartbeats_by_name.get(service.service_id.as_str()) else {
            continue;
        };
        if !heartbeat.present || heartbeat.corrupt || heartbeat.process_alive != Some(true) {
            continue;
        }
        let heartbeat_is_newer = match (heartbeat.at_ms, service.last_heartbeat_at_ms) {
            (Some(heartbeat_at_ms), Some(service_at_ms)) => heartbeat_at_ms > service_at_ms,
            (Some(_), None) => true,
            _ => false,
        };
        if !heartbeat_is_newer {
            continue;
        }

        service.process_id = heartbeat.process_id.or(service.process_id);
        service.process_alive = heartbeat.process_alive.or(service.process_alive);
        if heartbeat.watched_stop_file.is_some() {
            service.watched_stop_file = heartbeat.watched_stop_file.clone();
        }
        if heartbeat.generation_id.is_some()
            && service.generation_id.is_some()
            && heartbeat.generation_id != service.generation_id
        {
            service.ownership_conflict = true;
            service.ownership_conflict_reason = Some("generation-mismatch".to_string());
        }
        if heartbeat.observed_only == Some(true) || service.observed_only == Some(true) {
            service.ownership_conflict = true;
            service
                .ownership_conflict_reason
                .get_or_insert_with(|| "observed-only-owner".to_string());
        }
        if heartbeat.launch_owner.is_some() {
            service.launch_owner = heartbeat.launch_owner.clone();
        }
        if heartbeat.observed_only.is_some() {
            service.observed_only = heartbeat.observed_only;
        }
        service.last_heartbeat_at_ms = heartbeat.at_ms.or(service.last_heartbeat_at_ms);
        service.age_ms = service
            .last_heartbeat_at_ms
            .map(|at_ms| now_ms.saturating_sub(at_ms));
        if heartbeat.status.is_some() {
            service.status = heartbeat.status.clone();
        }
        if service.actual_state.as_deref() == Some("spawning") {
            service.actual_state = Some("running".to_string());
        }
    }
}

fn extra_loop_names(harness_home: &Path, heartbeat_dir: &Path) -> io::Result<BTreeSet<String>> {
    let mut names = BTreeSet::new();
    match fs::read_dir(heartbeat_dir) {
        Ok(entries) => {
            for entry in entries {
                let entry = entry?;
                let path = entry.path();
                if path.extension().and_then(|value| value.to_str()) == Some("json") {
                    if let Some(stem) = path.file_stem().and_then(|value| value.to_str()) {
                        names.insert(stem.to_string());
                    }
                }
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }

    let supervisor_plan = harness_home
        .join("state")
        .join("supervisor")
        .join("windows-scheduled-tasks")
        .join("supervisor-plan.json");
    if let Ok(text) = fs::read_to_string(&supervisor_plan) {
        if let Ok(value) = serde_json::from_str::<Value>(&text) {
            if let Some(tasks) = value.get("tasks").and_then(Value::as_array) {
                for task in tasks {
                    if let Some(component) = task.get("component").and_then(Value::as_str) {
                        if !component.trim().is_empty() {
                            names.insert(component.to_string());
                        }
                    }
                }
            }
        }
    }

    Ok(names)
}

fn cron_scheduler_status(harness_home: &Path) -> io::Result<HarnessCronSchedulerStatus> {
    let state_dir = harness_home.join("state").join("cron-scheduler");
    let database = state_dir.join("watermarks.sqlite");
    let loop_last_file = state_dir.join("loop-last.json");
    let latest = fs::read_to_string(&loop_last_file)
        .ok()
        .and_then(|text| serde_json::from_str::<Value>(&text).ok());
    let canon = cron_canon_status(harness_home, epoch_ms().unwrap_or(0))?;
    let warnings = cron_canon_warnings(&canon);
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
        canon,
        warnings,
    })
}

fn cron_canon_status(harness_home: &Path, now_ms: i64) -> io::Result<HarnessCronCanonStatus> {
    let canon_path = harness_home
        .join("workspace")
        .join("docs")
        .join("ops")
        .join("cron-canon.json");
    let mut status = HarnessCronCanonStatus {
        canon_path: canon_path.clone(),
        canon_present: canon_path.is_file(),
        keeper_receipt_path: harness_home
            .join("state")
            .join("ops")
            .join("cron-canon")
            .join("latest-cron-canon-keeper.json"),
        ..HarnessCronCanonStatus::default()
    };
    let Some(canon) = read_json_file(&canon_path) else {
        return Ok(status);
    };
    if let Some(keeper_receipt) = string_path(&canon, &["paths", "keeperReceipt"]) {
        status.keeper_receipt_path = resolve_harness_path(harness_home, &keeper_receipt);
    }
    status.keeper_receipt_present = status.keeper_receipt_path.is_file();
    if let Some(keeper) = read_json_file(&status.keeper_receipt_path) {
        status.keeper_status = string_path(&keeper, &["status"]);
        status.keeper_ok = bool_path(&keeper, &["ok"]);
        status.keeper_age_ms = latest_receipt_age_ms(&status.keeper_receipt_path, &keeper, now_ms);
        status.keeper_age_hours = status.keeper_age_ms.map(ms_to_hours);
        append_keeper_findings(&mut status.findings, &keeper);
    }
    if let Some(active_crons) = canon.get("activeCrons").and_then(Value::as_array) {
        for cron in active_crons {
            if bool_path(cron, &["enabled"]) == Some(false) {
                continue;
            }
            let cron_id = string_path(cron, &["id"]).unwrap_or_else(|| "unknown".to_string());
            let Some(monitor) = cron.get("monitor") else {
                continue;
            };
            if string_path(monitor, &["type"]).as_deref() != Some("latest-json") {
                continue;
            }
            status.monitor_count += 1;
            evaluate_latest_json_monitor(harness_home, now_ms, &cron_id, monitor, &mut status);
        }
    }
    status.finding_count = status.findings.len();
    status.stale_count = status
        .findings
        .iter()
        .filter(|finding| finding.code.contains("stale"))
        .count();
    Ok(status)
}

fn cron_canon_warnings(status: &HarnessCronCanonStatus) -> Vec<String> {
    let mut warnings = Vec::new();
    if status.canon_present && !status.keeper_receipt_present {
        warnings.push(format!(
            "cron canon keeper receipt is missing: {}",
            status.keeper_receipt_path.display()
        ));
    }
    if let Some(keeper_status) = status.keeper_status.as_deref()
        && keeper_status != "ok"
    {
        warnings.push(format!("cron canon keeper status={keeper_status}"));
    }
    if status.keeper_ok == Some(false) {
        warnings.push("cron canon keeper reported ok=false".to_string());
    }
    for finding in &status.findings {
        warnings.push(format!(
            "cron canon {} {} {}: {}{}{}",
            finding.severity,
            finding.cron_id,
            finding.code,
            finding.message,
            finding
                .age_hours
                .map(|age| format!(" ageHours={age:.3}"))
                .unwrap_or_default(),
            finding
                .max_age_hours
                .map(|max| format!(" maxAgeHours={max:.3}"))
                .unwrap_or_default()
        ));
    }
    warnings
}

fn evaluate_latest_json_monitor(
    harness_home: &Path,
    now_ms: i64,
    cron_id: &str,
    monitor: &Value,
    status: &mut HarnessCronCanonStatus,
) {
    let latest_path = if let Some(path) = string_path(monitor, &["path"]) {
        let path = resolve_harness_path(harness_home, &path);
        path.is_file().then_some(path)
    } else if let Some(path_glob) = string_path(monitor, &["pathGlob"]) {
        latest_glob_match(harness_home, &path_glob)
    } else {
        None
    };
    let Some(path) = latest_path else {
        status.findings.push(HarnessCronCanonFinding {
            severity: "warn".to_string(),
            code: "receipt-missing".to_string(),
            cron_id: cron_id.to_string(),
            message: "Expected receipt file is missing.".to_string(),
            path: None,
            age_ms: None,
            max_age_ms: monitor_max_age_ms(monitor),
            age_hours: None,
            max_age_hours: monitor_max_age_ms(monitor).map(ms_to_hours),
        });
        return;
    };
    let Some(json) = read_json_file(&path) else {
        status.findings.push(HarnessCronCanonFinding {
            severity: "warn".to_string(),
            code: "receipt-invalid-json".to_string(),
            cron_id: cron_id.to_string(),
            message: "Latest receipt is not valid JSON.".to_string(),
            path: Some(path),
            age_ms: None,
            max_age_ms: monitor_max_age_ms(monitor),
            age_hours: None,
            max_age_hours: monitor_max_age_ms(monitor).map(ms_to_hours),
        });
        return;
    };
    if let (Some(age_ms), Some(max_age_ms)) = (
        latest_receipt_age_ms(&path, &json, now_ms),
        monitor_max_age_ms(monitor),
    ) && age_ms > max_age_ms
    {
        status.findings.push(HarnessCronCanonFinding {
            severity: "warn".to_string(),
            code: "receipt-stale".to_string(),
            cron_id: cron_id.to_string(),
            message: "Latest receipt is older than canon allows.".to_string(),
            path: Some(path.clone()),
            age_ms: Some(age_ms),
            max_age_ms: Some(max_age_ms),
            age_hours: Some(ms_to_hours(age_ms)),
            max_age_hours: Some(ms_to_hours(max_age_ms)),
        });
    }
    if let Some(ok_field) = string_path(monitor, &["okField"]) {
        let actual = json.get(&ok_field);
        let expected = monitor.get("okValue");
        if actual != expected {
            status.findings.push(HarnessCronCanonFinding {
                severity: "warn".to_string(),
                code: "receipt-not-ok".to_string(),
                cron_id: cron_id.to_string(),
                message: "Latest receipt ok field does not match canon.".to_string(),
                path: Some(path),
                age_ms: None,
                max_age_ms: monitor_max_age_ms(monitor),
                age_hours: None,
                max_age_hours: monitor_max_age_ms(monitor).map(ms_to_hours),
            });
        }
    }
}

fn append_keeper_findings(findings: &mut Vec<HarnessCronCanonFinding>, keeper: &Value) {
    let Some(items) = keeper.get("findings").and_then(Value::as_array) else {
        return;
    };
    for item in items {
        let details = item.get("details").unwrap_or(&Value::Null);
        findings.push(HarnessCronCanonFinding {
            severity: string_path(item, &["severity"]).unwrap_or_else(|| "warn".to_string()),
            code: string_path(item, &["code"]).unwrap_or_else(|| "keeper-finding".to_string()),
            cron_id: string_path(item, &["cronId"]).unwrap_or_else(|| "unknown".to_string()),
            message: string_path(item, &["message"]).unwrap_or_default(),
            path: string_path(details, &["path"]).map(PathBuf::from),
            age_ms: details
                .get("ageHours")
                .and_then(Value::as_f64)
                .map(|hours| (hours * 3_600_000.0).round() as i64),
            max_age_ms: details
                .get("maxAgeHours")
                .and_then(Value::as_f64)
                .map(|hours| (hours * 3_600_000.0).round() as i64),
            age_hours: details.get("ageHours").and_then(Value::as_f64),
            max_age_hours: details.get("maxAgeHours").and_then(Value::as_f64),
        });
    }
}

fn monitor_max_age_ms(monitor: &Value) -> Option<i64> {
    monitor
        .get("maxAgeHours")
        .and_then(Value::as_f64)
        .map(|hours| (hours * 3_600_000.0).round() as i64)
}

fn latest_receipt_age_ms(path: &Path, json: &Value, now_ms: i64) -> Option<i64> {
    let receipt_ms = i64_path(json, &["generatedAtMs"])
        .or_else(|| i64_path(json, &["atMs"]))
        .or_else(|| string_path(json, &["generatedAt"]).and_then(|value| parse_rfc3339_ms(&value)))
        .or_else(|| file_modified_ms(path));
    receipt_ms.map(|receipt_ms| now_ms.saturating_sub(receipt_ms))
}

fn ms_to_hours(ms: i64) -> f64 {
    ms as f64 / 3_600_000.0
}

fn parse_rfc3339_ms(value: &str) -> Option<i64> {
    let value = value.trim();
    let bytes = value.as_bytes();
    if bytes.len() < 20 {
        return None;
    }
    let year = parse_digits_i32(bytes, 0, 4)?;
    expect_byte(bytes, 4, b'-')?;
    let month = parse_digits_u32(bytes, 5, 7)?;
    expect_byte(bytes, 7, b'-')?;
    let day = parse_digits_u32(bytes, 8, 10)?;
    match *bytes.get(10)? {
        b'T' | b't' | b' ' => {}
        _ => return None,
    }
    let hour = parse_digits_u32(bytes, 11, 13)?;
    expect_byte(bytes, 13, b':')?;
    let minute = parse_digits_u32(bytes, 14, 16)?;
    expect_byte(bytes, 16, b':')?;
    let second = parse_digits_u32(bytes, 17, 19)?;
    if month == 0
        || month > 12
        || day == 0
        || day > days_in_month(year, month)?
        || hour > 23
        || minute > 59
        || second > 60
    {
        return None;
    }
    let mut index = 19;
    let mut millis = 0_i64;
    if bytes.get(index) == Some(&b'.') {
        index += 1;
        let fraction_start = index;
        let mut fraction_millis = 0_i64;
        let mut millis_digits = 0_usize;
        while let Some(byte) = bytes.get(index)
            && byte.is_ascii_digit()
        {
            if millis_digits < 3 {
                fraction_millis = fraction_millis * 10 + i64::from(byte - b'0');
                millis_digits += 1;
            }
            index += 1;
        }
        if index == fraction_start {
            return None;
        }
        while millis_digits < 3 {
            fraction_millis *= 10;
            millis_digits += 1;
        }
        millis = fraction_millis;
    }
    let offset_ms = match bytes.get(index).copied()? {
        b'Z' | b'z' => {
            if index + 1 != bytes.len() {
                return None;
            }
            0_i64
        }
        b'+' | b'-' => {
            let sign = if bytes[index] == b'+' { 1_i64 } else { -1_i64 };
            let offset_hour = parse_digits_u32(bytes, index + 1, index + 3)?;
            expect_byte(bytes, index + 3, b':')?;
            let offset_minute = parse_digits_u32(bytes, index + 4, index + 6)?;
            if index + 6 != bytes.len() || offset_hour > 23 || offset_minute > 59 {
                return None;
            }
            sign * i64::from(offset_hour * 60 + offset_minute) * 60_000
        }
        _ => return None,
    };
    let days = days_from_civil(year, month, day)?;
    let local_ms = days
        .checked_mul(86_400_000)?
        .checked_add(i64::from(hour) * 3_600_000)?
        .checked_add(i64::from(minute) * 60_000)?
        .checked_add(i64::from(second) * 1_000)?
        .checked_add(millis)?;
    local_ms.checked_sub(offset_ms)
}

fn parse_digits_i32(bytes: &[u8], start: usize, end: usize) -> Option<i32> {
    let value = parse_digits_u32(bytes, start, end)?;
    i32::try_from(value).ok()
}

fn parse_digits_u32(bytes: &[u8], start: usize, end: usize) -> Option<u32> {
    if start >= end || end > bytes.len() {
        return None;
    }
    let mut value = 0_u32;
    for byte in &bytes[start..end] {
        if !byte.is_ascii_digit() {
            return None;
        }
        value = value.checked_mul(10)?.checked_add(u32::from(byte - b'0'))?;
    }
    Some(value)
}

fn expect_byte(bytes: &[u8], index: usize, expected: u8) -> Option<()> {
    (*bytes.get(index)? == expected).then_some(())
}

fn days_in_month(year: i32, month: u32) -> Option<u32> {
    Some(match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => return None,
    })
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn days_from_civil(year: i32, month: u32, day: u32) -> Option<i64> {
    let year = year - i32::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month = i32::try_from(month).ok()?;
    let day = i32::try_from(day).ok()?;
    let month_prime = month + if month > 2 { -3 } else { 9 };
    let doy = (153 * month_prime + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    Some(i64::from(era) * 146_097 + i64::from(doe) - 719_468)
}

fn file_modified_ms(path: &Path) -> Option<i64> {
    let modified = fs::metadata(path).ok()?.modified().ok()?;
    system_time_ms(modified)
}

fn system_time_ms(value: SystemTime) -> Option<i64> {
    let duration = value.duration_since(UNIX_EPOCH).ok()?;
    i64::try_from(duration.as_millis()).ok()
}

fn read_json_file(path: &Path) -> Option<Value> {
    let text = fs::read_to_string(path).ok()?;
    let text = text.strip_prefix('\u{feff}').unwrap_or(&text);
    serde_json::from_str::<Value>(text).ok()
}

fn resolve_harness_path(harness_home: &Path, value: &str) -> PathBuf {
    let normalized = value.replace('/', "\\");
    let path = PathBuf::from(normalized);
    if path.is_absolute() {
        path
    } else {
        harness_home.join(path)
    }
}

fn latest_glob_match(harness_home: &Path, pattern: &str) -> Option<PathBuf> {
    let mut matches = Vec::new();
    let normalized = pattern.replace('\\', "/");
    let parts: Vec<_> = normalized
        .split('/')
        .filter(|part| !part.is_empty())
        .collect();
    collect_glob_matches(harness_home, &parts, &mut matches);
    matches
        .into_iter()
        .max_by_key(|path| file_modified_ms(path).unwrap_or(0))
}

fn collect_glob_matches(base: &Path, parts: &[&str], matches: &mut Vec<PathBuf>) {
    if parts.is_empty() {
        if base.is_file() {
            matches.push(base.to_path_buf());
        }
        return;
    }
    let (part, rest) = parts.split_first().expect("non-empty parts");
    if part.contains('*') || part.contains('?') {
        let Ok(entries) = fs::read_dir(base) else {
            return;
        };
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if wildcard_match(part, &name) {
                collect_glob_matches(&entry.path(), rest, matches);
            }
        }
    } else {
        collect_glob_matches(&base.join(part), rest, matches);
    }
}

fn wildcard_match(pattern: &str, value: &str) -> bool {
    let pattern = pattern.as_bytes();
    let value = value.as_bytes();
    let (mut p, mut v, mut star, mut match_after_star) = (0usize, 0usize, None, 0usize);
    while v < value.len() {
        if p < pattern.len() && (pattern[p] == b'?' || pattern[p].eq_ignore_ascii_case(&value[v])) {
            p += 1;
            v += 1;
        } else if p < pattern.len() && pattern[p] == b'*' {
            star = Some(p);
            match_after_star = v;
            p += 1;
        } else if let Some(star_pos) = star {
            p = star_pos + 1;
            match_after_star += 1;
            v = match_after_star;
        } else {
            return false;
        }
    }
    while p < pattern.len() && pattern[p] == b'*' {
        p += 1;
    }
    p == pattern.len()
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
    harness_home: &Path,
    name: &str,
    heartbeat_file: &Path,
    now_ms: i64,
) -> io::Result<HarnessLoopHeartbeatStatus> {
    let stop_file = read_supervisor_stop_file(harness_home, name)?;
    let text = match fs::read_to_string(heartbeat_file) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(HarnessLoopHeartbeatStatus {
                name: name.to_string(),
                heartbeat_file: heartbeat_file.to_path_buf(),
                present: false,
                corrupt: false,
                parse_error: None,
                status: None,
                iteration: None,
                process_id: None,
                process_alive: None,
                generation_id: None,
                parent_pid: None,
                process_start_time_ms: None,
                watched_stop_file: None,
                launch_owner: None,
                observed_only: None,
                at_ms: None,
                age_ms: None,
                detail: None,
                stop_file: stop_file.path,
                stop_file_present: stop_file.present,
                stop_file_reason: stop_file.reason,
                stop_file_service_id: stop_file.service_id,
                stop_file_created_by: stop_file.created_by,
                stop_file_created_at_ms: stop_file.created_at_ms,
                stop_file_expires_at_ms: stop_file.expires_at_ms,
                stop_file_persistent: stop_file.persistent,
            });
        }
        Err(error) => return Err(error),
    };
    let value = match serde_json::from_str::<Value>(&text) {
        Ok(value) => value,
        Err(error) => {
            return Ok(HarnessLoopHeartbeatStatus {
                name: name.to_string(),
                heartbeat_file: heartbeat_file.to_path_buf(),
                present: true,
                corrupt: true,
                parse_error: Some(error.to_string()),
                status: None,
                iteration: None,
                process_id: None,
                process_alive: None,
                generation_id: None,
                parent_pid: None,
                process_start_time_ms: None,
                watched_stop_file: None,
                launch_owner: None,
                observed_only: None,
                at_ms: None,
                age_ms: None,
                detail: None,
                stop_file: stop_file.path,
                stop_file_present: stop_file.present,
                stop_file_reason: stop_file.reason,
                stop_file_service_id: stop_file.service_id,
                stop_file_created_by: stop_file.created_by,
                stop_file_created_at_ms: stop_file.created_at_ms,
                stop_file_expires_at_ms: stop_file.expires_at_ms,
                stop_file_persistent: stop_file.persistent,
            });
        }
    };
    let at_ms = i64_path(&value, &["atMs"]);
    let process_id = i64_path(&value, &["processId"]);
    Ok(HarnessLoopHeartbeatStatus {
        name: name.to_string(),
        heartbeat_file: heartbeat_file.to_path_buf(),
        present: true,
        corrupt: false,
        parse_error: None,
        status: string_path(&value, &["status"]),
        iteration: i64_path(&value, &["iteration"]),
        process_id,
        process_alive: process_id.and_then(process_alive_for_pid),
        generation_id: string_path(&value, &["generationId"]),
        parent_pid: i64_path(&value, &["parentPid"]),
        process_start_time_ms: i64_path(&value, &["processStartTimeMs"]),
        watched_stop_file: pathbuf_path(&value, &["watchedStopFile"]),
        launch_owner: string_path(&value, &["launchOwner"]),
        observed_only: bool_path(&value, &["observedOnly"]),
        at_ms,
        age_ms: at_ms.map(|at_ms| now_ms.saturating_sub(at_ms)),
        detail: string_path(&value, &["detail"]),
        stop_file: stop_file.path,
        stop_file_present: stop_file.present,
        stop_file_reason: stop_file.reason,
        stop_file_service_id: stop_file.service_id,
        stop_file_created_by: stop_file.created_by,
        stop_file_created_at_ms: stop_file.created_at_ms,
        stop_file_expires_at_ms: stop_file.expires_at_ms,
        stop_file_persistent: stop_file.persistent,
    })
}

fn append_loop_health_warnings(loops: &HarnessLoopStatus, warnings: &mut Vec<String>) {
    for loop_status in &loops.heartbeats {
        if loop_status.stop_file_present {
            let reason = loop_status
                .stop_file_reason
                .as_deref()
                .filter(|reason| !reason.is_empty())
                .unwrap_or("no reason recorded");
            warnings.push(format!(
                "{} is disabled by stop file at {}: {}",
                loop_status.name,
                loop_status.stop_file.display(),
                reason
            ));
        }
        if loop_status.corrupt {
            let error = loop_status
                .parse_error
                .as_deref()
                .unwrap_or("invalid heartbeat JSON");
            warnings.push(format!(
                "{} heartbeat at {} is corrupt: {}",
                loop_status.name,
                loop_status.heartbeat_file.display(),
                error
            ));
        }
        if loop_status.process_alive == Some(false) {
            let process_id = loop_status
                .process_id
                .map(|process_id| process_id.to_string())
                .unwrap_or_else(|| "-".to_string());
            warnings.push(format!(
                "{} heartbeat references processId={} but that process is not running",
                loop_status.name, process_id
            ));
        }
    }
    for service in &loops.services {
        if service.corrupt {
            let error = service
                .parse_error
                .as_deref()
                .unwrap_or("invalid supervisor service JSON");
            warnings.push(format!(
                "{} supervisor service registry at {} is corrupt: {}",
                service.service_id,
                service.service_file.display(),
                error
            ));
        }
        if service.process_alive == Some(false) {
            let process_id = service
                .process_id
                .map(|process_id| process_id.to_string())
                .unwrap_or_else(|| "-".to_string());
            warnings.push(format!(
                "{} supervisor service registry references pid={} but that process is not running",
                service.service_id, process_id
            ));
        }
    }
}

fn runtime_status(
    harness_home: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<HarnessRuntimeStatus> {
    let queue_dir = harness_home.join("state").join("runtime-queue");
    let pending_file = queue_dir.join("pending.jsonl");
    let pending = read_runtime_pending_queue(&pending_file, warnings)?;
    // Current open-work accounting is driven by the hot materialized index.
    // Cold history is consulted only for the exact currently-pending IDs as a
    // defensive recovery guard; it must never turn an arbitrary old terminal
    // turn into apparent current work.
    let hot_index = refresh_runtime_queue_state_index(&queue_dir, warnings)?;
    let mut terminal_run_ids = terminal_run_once_ids_from_index(&hot_index);
    for terminal in find_runtime_queue_terminal_history(&queue_dir, &pending.queue_ids)? {
        terminal_run_ids.insert(terminal.queue_id);
    }
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
    let service_recall_receipts =
        jsonl_status(memory_state_dir.join("openclaw-mem-service-recall-receipts.jsonl"))?;
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
    let support_plane = openclaw_mem_support_plane_status(harness_home);
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
        &service_recall_receipts,
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
        service_recall_receipts,
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
        support_plane,
        summary,
        memory_dir,
    })
}

fn openclaw_mem_support_plane_status(harness_home: &Path) -> HarnessOpenClawMemSupportPlaneStatus {
    let memory_state_dir = harness_home.join("state").join("memory");
    let db_path = harness_home.join("memory").join("openclaw-mem.sqlite");
    let topology_paths = vec![
        memory_state_dir
            .join("graph")
            .join("topology-extract-full.json"),
        memory_state_dir
            .join("graph")
            .join("topology-extract-full.yaml"),
        memory_state_dir.join("graph").join("topology-seed.yaml"),
    ];
    let topology_candidates = topology_paths
        .iter()
        .map(|path| HarnessMemoryTopologyCandidateStatus {
            path: path.clone(),
            exists: path.is_file(),
        })
        .collect::<Vec<_>>();
    let selected_topology_source = topology_candidates
        .iter()
        .find(|candidate| candidate.exists)
        .map(|candidate| candidate.path.clone());
    let service_store_path = openclaw_mem_service_store_file_for_agent(harness_home, None);
    let writeback_store_path = memory_state_dir.join("openclaw-mem-writeback.jsonl");
    let db_exists = db_path.is_file();
    let service_store_exists = service_store_path.is_file();
    let writeback_store_exists = writeback_store_path.is_file();
    let mut missing_reasons = Vec::new();
    if !db_exists {
        missing_reasons.push(format!("db_missing:{}", db_path.display()));
    }
    if selected_topology_source.is_none() {
        missing_reasons.push(format!(
            "topology_source_missing:{}",
            topology_paths
                .iter()
                .map(|path| path.display().to_string())
                .collect::<Vec<_>>()
                .join("|")
        ));
    }
    if !service_store_exists {
        missing_reasons.push(format!(
            "service_store_missing:{}",
            service_store_path.display()
        ));
    }
    if !writeback_store_exists {
        missing_reasons.push(format!(
            "writeback_store_missing:{}",
            writeback_store_path.display()
        ));
    }
    HarnessOpenClawMemSupportPlaneStatus {
        db_path,
        db_exists,
        topology_candidates,
        selected_topology_source,
        service_store_path,
        service_store_exists,
        writeback_store_path,
        writeback_store_exists,
        graph_autonomous_matching_ready: missing_reasons.is_empty(),
        missing_reasons,
    }
}

fn memory_health_summary(
    memory_dir: &Path,
    qdrant_edge: bool,
    legacy_mem_sqlite: bool,
    semantic_coverage: &MemorySemanticCoverageReport,
    memory_owner_state: &MemoryOwnerState,
    service_recall_receipts: &HarnessJsonlStatus,
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
    let service_status = service_recall_receipts.latest_status.as_deref();
    let vector_status = vector_recall_receipts.latest_status.as_deref();
    let active_recall_backend = if matches!(service_status, Some("ready" | "no-hits")) {
        service_recall_receipts
            .latest_backend
            .clone()
            .unwrap_or_else(|| "unknown".to_string())
    } else if matches!(vector_status, Some("ready" | "no-hits")) {
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
    let qdrant_edge_mode = if qdrant_edge {
        "preserved-snapshot"
    } else {
        "missing"
    };
    let mem_engine_canary = memory_mem_engine_canary_report(
        memory_dir.parent().unwrap_or(memory_dir),
        qdrant_edge_mode,
        memory_owner_state,
    );
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

fn latest_gateway_restart_receipt(
    path: &Path,
    status_filter: Option<&str>,
) -> io::Result<Option<GatewayRestartReceiptStatus>> {
    let Some(sample) = read_tail_sample(path, STATUS_JSONL_SAMPLE_BYTES)? else {
        return Ok(None);
    };
    let mut latest = None;
    for line in sample.text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
            continue;
        };
        let status = string_path(&value, &["status"]);
        if let Some(filter) = status_filter
            && status.as_deref() != Some(filter)
        {
            continue;
        }
        latest = Some(GatewayRestartReceiptStatus {
            status,
            request_file: pathbuf_path(&value, &["requestFile"]),
            consumed_request_file: pathbuf_path(&value, &["consumedRequestFile"]),
            receipt_file: pathbuf_path(&value, &["receiptFile"]),
            reason: string_path(&value, &["reason"]),
            requesting_platform: string_path(&value, &["requestingPlatform"])
                .or_else(|| string_path(&value, &["platform"])),
            channel_id: string_path(&value, &["channelId"]),
            user_id: string_path(&value, &["userId"]),
            session_key: string_path(&value, &["sessionKey"]),
            at_ms: i64_path(&value, &["atMs"]),
            consumed_at_ms: i64_path(&value, &["consumedAtMs"]),
            consumed_by: string_path(&value, &["consumedBy"]),
            consumer_pid: i64_path(&value, &["consumerPid"]),
            parent_pid: i64_path(&value, &["parentPid"]),
            generation_id: string_path(&value, &["generationId"]),
            process_start_time_ms: i64_path(&value, &["processStartTimeMs"]),
            stop_file: pathbuf_path(&value, &["stopFile"]),
        });
    }
    Ok(latest)
}

fn latest_gateway_restart_completion(
    path: &Path,
) -> io::Result<Option<GatewayRestartCompletionStatus>> {
    let Some(sample) = read_tail_sample(path, STATUS_JSONL_SAMPLE_BYTES)? else {
        return Ok(None);
    };
    let mut latest = None;
    for line in sample.text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
            continue;
        };
        latest = Some(GatewayRestartCompletionStatus {
            status: string_path(&value, &["status"]),
            request_file: pathbuf_path(&value, &["requestFile"]),
            consumed_request_file: pathbuf_path(&value, &["consumedRequestFile"]),
            consumed_at_ms: i64_path(&value, &["consumedAtMs"]),
            consumed_by: string_path(&value, &["consumedBy"]),
            consumer_pid: i64_path(&value, &["consumerPid"]),
            generation_id: string_path(&value, &["generationId"]),
            process_start_time_ms: i64_path(&value, &["processStartTimeMs"]),
            heartbeat_status: string_path(&value, &["heartbeatStatus"]),
            heartbeat_generation_id: string_path(&value, &["heartbeatGenerationId"]),
            heartbeat_process_id: i64_path(&value, &["heartbeatProcessId"]),
            heartbeat_at_ms: i64_path(&value, &["heartbeatAtMs"]),
            notified: bool_path(&value, &["notified"]),
            message: string_path(&value, &["message"]),
        });
    }
    Ok(latest)
}

fn gateway_restart_service_status(harness_home: &Path) -> io::Result<GatewayRestartServiceStatus> {
    let service_file = harness_home
        .join("state")
        .join("supervisor")
        .join("services")
        .join("discord-gateway-loop.json");
    let text = match fs::read_to_string(&service_file) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(GatewayRestartServiceStatus {
                service_file,
                present: false,
                corrupt: false,
                parse_error: None,
                status: None,
                actual_state: None,
                generation_id: None,
                process_id: None,
                process_alive: None,
                supervisor_process_id: None,
                process_start_time_ms: None,
                last_heartbeat_at_ms: None,
                launch_owner: None,
                observed_only: None,
            });
        }
        Err(error) => return Err(error),
    };
    let value: Value = match serde_json::from_str(&text) {
        Ok(value) => value,
        Err(error) => {
            return Ok(GatewayRestartServiceStatus {
                service_file,
                present: true,
                corrupt: true,
                parse_error: Some(error.to_string()),
                status: None,
                actual_state: None,
                generation_id: None,
                process_id: None,
                process_alive: None,
                supervisor_process_id: None,
                process_start_time_ms: None,
                last_heartbeat_at_ms: None,
                launch_owner: None,
                observed_only: None,
            });
        }
    };
    let process_id = i64_path(&value, &["pid"]).or_else(|| i64_path(&value, &["processId"]));
    Ok(GatewayRestartServiceStatus {
        service_file,
        present: true,
        corrupt: false,
        parse_error: None,
        status: string_path(&value, &["status"]),
        actual_state: string_path(&value, &["actualState"]),
        generation_id: string_path(&value, &["generationId"]),
        process_id,
        process_alive: process_id.and_then(process_alive_for_pid),
        supervisor_process_id: i64_path(&value, &["supervisorPid"]),
        process_start_time_ms: i64_path(&value, &["processStartTimeMs"]),
        last_heartbeat_at_ms: i64_path(&value, &["lastHeartbeatAtMs"]),
        launch_owner: string_path(&value, &["launchOwner"]),
        observed_only: bool_path(&value, &["observedOnly"]),
    })
}

fn gateway_restart_heartbeat_status(
    harness_home: &Path,
) -> io::Result<GatewayRestartHeartbeatStatus> {
    let heartbeat_file = harness_home
        .join("state")
        .join("supervisor")
        .join("loop-heartbeats")
        .join("discord-gateway-loop.json");
    let text = match fs::read_to_string(&heartbeat_file) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(GatewayRestartHeartbeatStatus {
                heartbeat_file,
                present: false,
                corrupt: false,
                parse_error: None,
                status: None,
                generation_id: None,
                process_id: None,
                process_alive: None,
                at_ms: None,
            });
        }
        Err(error) => return Err(error),
    };
    let value: Value = match serde_json::from_str(&text) {
        Ok(value) => value,
        Err(error) => {
            return Ok(GatewayRestartHeartbeatStatus {
                heartbeat_file,
                present: true,
                corrupt: true,
                parse_error: Some(error.to_string()),
                status: None,
                generation_id: None,
                process_id: None,
                process_alive: None,
                at_ms: None,
            });
        }
    };
    let process_id = i64_path(&value, &["processId"]);
    Ok(GatewayRestartHeartbeatStatus {
        heartbeat_file,
        present: true,
        corrupt: false,
        parse_error: None,
        status: string_path(&value, &["status"]),
        generation_id: string_path(&value, &["generationId"]),
        process_id,
        process_alive: process_id.and_then(process_alive_for_pid),
        at_ms: i64_path(&value, &["atMs"]),
    })
}

fn pathbuf_path(value: &Value, path: &[&str]) -> Option<PathBuf> {
    string_path(value, path).map(PathBuf::from)
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
    summary.sampled = sample.sampled;
    summary.sampled_bytes = sample.sampled_bytes;
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

fn bool_path(value: &Value, path: &[&str]) -> Option<bool> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    current.as_bool()
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
                .join("openclaw-mem-service-recall-receipts.jsonl"),
            r#"{"status":"ready","hitCount":2,"backend":"qdrant-edge","recallProvider":"openclaw-mem-engine","reason":"ok"}"#,
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
        assert_eq!(report.memory.summary.active_recall_backend, "qdrant-edge");
        assert_eq!(report.memory.summary.qdrant_parity, "native-recall-active");
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
        assert_eq!(report.skills.status, SkillDoctorStatus::Warn);
        assert_eq!(report.skills.summary.total_skills, 0);
        assert_eq!(report.skills.warning_findings, 1);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn collect_status_treats_suppressed_run_once_as_terminal() {
        let root = temp_root("collect_status_treats_suppressed_run_once_as_terminal");
        let harness_home = root.join(".agent-harness");
        let queue_dir = harness_home.join("state").join("runtime-queue");
        fs::create_dir_all(&queue_dir).unwrap();
        fs::write(
            queue_dir.join("pending.jsonl"),
            r#"{"queueId":"q-suppressed","platform":"telegram","runtimeClass":"interactive","origin":"channel"}"#,
        )
        .unwrap();
        fs::write(
            queue_dir.join("run-once-receipts.jsonl"),
            r#"{"queueId":"q-suppressed","status":"suppressed","reason":"terminal-control-present"}"#,
        )
        .unwrap();

        let report = collect_harness_status(HarnessStatusOptions { harness_home }).unwrap();

        assert_eq!(report.runtime.queued_items, 1);
        assert_eq!(report.runtime.open_items, 0);
        assert_eq!(
            report.runtime.run_once_receipts.latest_status.as_deref(),
            Some("suppressed")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn loop_status_reports_stop_file_and_missing_process() {
        let root = temp_root("loop_status_reports_stop_file_and_missing_process");
        let harness_home = root.join(".agent-harness");
        let heartbeat_dir = harness_home
            .join("state")
            .join("supervisor")
            .join("loop-heartbeats");
        let stop_dir = harness_home.join("state").join("supervisor").join("stop");
        fs::create_dir_all(&heartbeat_dir).unwrap();
        fs::create_dir_all(&stop_dir).unwrap();
        fs::write(
            heartbeat_dir.join("progress-delivery-loop.json"),
            r#"{"status":"running","iteration":9,"processId":0,"atMs":1000,"detail":"delivering progress"}"#,
        )
        .unwrap();
        fs::write(
            stop_dir.join("progress-delivery-loop.stop"),
            serde_json::to_string(&serde_json::json!({
                "schema": "agent-harness.supervisor-stop-file.v1",
                "serviceId": "progress-delivery-loop",
                "reason": "stop for current-step cutover",
                "createdBy": "ops-control",
                "createdAtMs": 1781343427711_i64,
                "persistent": true
            }))
            .unwrap(),
        )
        .unwrap();

        let loops = loop_status(&harness_home).unwrap();
        let progress_loop = loops
            .heartbeats
            .iter()
            .find(|heartbeat| heartbeat.name == "progress-delivery-loop")
            .unwrap();
        assert!(progress_loop.present);
        assert_eq!(progress_loop.process_id, Some(0));
        assert_eq!(progress_loop.process_alive, Some(false));
        assert!(progress_loop.stop_file_present);
        assert!(
            progress_loop
                .stop_file_reason
                .as_deref()
                .is_some_and(|reason| reason == "stop for current-step cutover")
        );
        assert_eq!(
            progress_loop.stop_file_service_id.as_deref(),
            Some("progress-delivery-loop")
        );
        assert_eq!(
            progress_loop.stop_file_created_by.as_deref(),
            Some("ops-control")
        );
        assert_eq!(progress_loop.stop_file_created_at_ms, Some(1781343427711));
        assert_eq!(progress_loop.stop_file_persistent, Some(true));

        let mut warnings = Vec::new();
        append_loop_health_warnings(&loops, &mut warnings);
        assert!(
            warnings
                .iter()
                .any(|warning| warning.contains("disabled by stop file"))
        );
        assert!(
            warnings
                .iter()
                .any(|warning| warning.contains("processId=0"))
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn loop_status_reads_observe_only_supervisor_service_registry() {
        let root = temp_root("loop_status_reads_observe_only_supervisor_service_registry");
        let harness_home = root.join(".agent-harness");
        let services_dir = harness_home
            .join("state")
            .join("supervisor")
            .join("services");
        fs::create_dir_all(&services_dir).unwrap();
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let watched_stop_file = harness_home
            .join("state")
            .join("supervisor")
            .join("stop")
            .join("runtime-loop.live.stop");
        fs::write(
            services_dir.join("runtime-loop.json"),
            serde_json::to_string(&serde_json::json!({
                "schema": "agent-harness.supervisor-service-state.v1",
                "serviceId": "runtime-loop",
                "serviceKind": "runtime",
                "generationId": "runtime-loop-test-generation",
                "pid": std::process::id(),
                "supervisorPid": 4242,
                "parentPid": 4343,
                "processStartTimeMs": now_ms - 500,
                "startedAtMs": now_ms - 500,
                "watchedStopFile": watched_stop_file,
                "lastHeartbeatAtMs": now_ms - 10,
                "lastSuccessfulIterationAtMs": now_ms - 10,
                "lastExitAtMs": now_ms - 5,
                "lastExitCode": 1,
                "lastErrorClass": "process-exit",
                "restartCount": 2,
                "backoffUntilMs": now_ms + 60_000,
                "servicePriority": "final-delivery",
                "deliveryLane": "final-outbox",
                "restartDelayMs": 15_000,
                "memoryGateDecision": {
                    "action": "pause-low-priority-service",
                    "reason": "resource-exhausted"
                },
                "iteration": 17,
                "status": "no-work",
                "desiredState": "running",
                "actualState": "no-work",
                "detail": "idle",
                "launchOwner": "external-runner-observe-only",
                "observedOnly": true
            }))
            .unwrap(),
        )
        .unwrap();

        let loops = loop_status(&harness_home).unwrap();
        assert_eq!(loops.services_dir, services_dir);
        let runtime_service = loops
            .services
            .iter()
            .find(|service| service.service_id == "runtime-loop")
            .unwrap();
        assert!(runtime_service.present);
        assert!(!runtime_service.corrupt);
        assert_eq!(runtime_service.service_kind.as_deref(), Some("runtime"));
        assert_eq!(
            runtime_service.generation_id.as_deref(),
            Some("runtime-loop-test-generation")
        );
        assert_eq!(
            runtime_service.process_id,
            Some(i64::from(std::process::id()))
        );
        assert_eq!(runtime_service.process_alive, Some(true));
        assert_eq!(runtime_service.supervisor_process_id, Some(4242));
        assert_eq!(runtime_service.parent_pid, Some(4343));
        assert_eq!(runtime_service.started_at_ms, Some(now_ms - 500));
        assert_eq!(runtime_service.process_start_time_ms, Some(now_ms - 500));
        assert_eq!(
            runtime_service.watched_stop_file.as_ref(),
            Some(&watched_stop_file)
        );
        assert_eq!(runtime_service.last_heartbeat_at_ms, Some(now_ms - 10));
        assert_eq!(
            runtime_service.last_successful_iteration_at_ms,
            Some(now_ms - 10)
        );
        assert_eq!(runtime_service.last_exit_at_ms, Some(now_ms - 5));
        assert_eq!(runtime_service.last_exit_code, Some(1));
        assert_eq!(
            runtime_service.last_error_class.as_deref(),
            Some("process-exit")
        );
        assert_eq!(runtime_service.restart_count, Some(2));
        assert_eq!(runtime_service.backoff_until_ms, Some(now_ms + 60_000));
        assert_eq!(
            runtime_service.service_priority.as_deref(),
            Some("final-delivery")
        );
        assert_eq!(
            runtime_service.delivery_lane.as_deref(),
            Some("final-outbox")
        );
        assert_eq!(runtime_service.restart_delay_ms, Some(15_000));
        assert_eq!(
            runtime_service.memory_gate_action.as_deref(),
            Some("pause-low-priority-service")
        );
        assert_eq!(
            runtime_service.memory_gate_reason.as_deref(),
            Some("resource-exhausted")
        );
        assert!(runtime_service.age_ms.is_some_and(|age_ms| age_ms >= 0));
        assert_eq!(runtime_service.iteration, Some(17));
        assert_eq!(runtime_service.status.as_deref(), Some("no-work"));
        assert_eq!(runtime_service.desired_state.as_deref(), Some("running"));
        assert_eq!(runtime_service.actual_state.as_deref(), Some("no-work"));
        assert_eq!(runtime_service.detail.as_deref(), Some("idle"));
        assert_eq!(
            runtime_service.launch_owner.as_deref(),
            Some("external-runner-observe-only")
        );
        assert_eq!(runtime_service.observed_only, Some(true));
        assert!(runtime_service.ownership_conflict);
        assert_eq!(
            runtime_service.ownership_conflict_reason.as_deref(),
            Some("observed-only-owner")
        );

        let mut warnings = Vec::new();
        append_loop_health_warnings(&loops, &mut warnings);
        assert!(warnings.is_empty(), "{warnings:?}");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn loop_status_reads_gateway_restart_request_receipts() {
        let root = temp_root("loop_status_reads_gateway_restart_request_receipts");
        let harness_home = root.join(".agent-harness");
        let supervisor_dir = harness_home.join("state").join("supervisor");
        fs::create_dir_all(&supervisor_dir).unwrap();
        fs::write(
            supervisor_dir.join("gateway-restart-requests.jsonl"),
            "{\"status\":\"requested\",\"reason\":\"operator command\"}\n\
             {\"status\":\"consumed\",\"reason\":\"restart request consumed\"}\n",
        )
        .unwrap();

        let loops = loop_status(&harness_home).unwrap();

        assert!(loops.gateway_restart_requests.exists);
        assert_eq!(loops.gateway_restart_requests.lines, 2);
        assert_eq!(
            loops.gateway_restart_requests.latest_status.as_deref(),
            Some("consumed")
        );
        assert_eq!(
            loops.gateway_restart_requests.latest_reason.as_deref(),
            Some("restart request consumed")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn gateway_restart_status_reads_request_consumption_completion_and_generation() {
        let root =
            temp_root("gateway_restart_status_reads_request_consumption_completion_and_generation");
        let harness_home = root.join(".agent-harness");
        let supervisor_dir = harness_home.join("state").join("supervisor");
        let services_dir = supervisor_dir.join("services");
        let heartbeat_dir = supervisor_dir.join("loop-heartbeats");
        fs::create_dir_all(&services_dir).unwrap();
        fs::create_dir_all(&heartbeat_dir).unwrap();
        fs::write(
            supervisor_dir.join("gateway-restart-requests.jsonl"),
            "{\"status\":\"requested\",\"requestFile\":\"request.json\",\"reason\":\"operator\",\"requestingPlatform\":\"discord\",\"channelId\":\"dm-1\",\"userId\":\"u-1\",\"sessionKey\":\"discord:dm-1:u-1\",\"atMs\":1000}\n\
             {\"status\":\"consumed\",\"requestFile\":\"request.json\",\"consumedRequestFile\":\"consumed.json\",\"consumedAtMs\":1100,\"consumedBy\":\"discord-gateway-loop\",\"consumerPid\":42,\"generationId\":\"gateway-generation-1\",\"processStartTimeMs\":900,\"stopFile\":\"gateway.stop\"}\n",
        )
        .unwrap();
        fs::write(
            supervisor_dir.join("gateway-restart-completions.jsonl"),
            "{\"status\":\"completed\",\"requestFile\":\"request.json\",\"consumedRequestFile\":\"consumed.json\",\"consumedAtMs\":1100,\"consumedBy\":\"discord-gateway-loop\",\"consumerPid\":42,\"generationId\":\"gateway-generation-1\",\"processStartTimeMs\":900,\"heartbeatStatus\":\"spawning\",\"heartbeatGenerationId\":\"gateway-generation-1\",\"heartbeatProcessId\":42,\"heartbeatAtMs\":1200,\"notified\":true,\"message\":\"done\"}\n",
        )
        .unwrap();
        fs::write(
            services_dir.join("discord-gateway-loop.json"),
            serde_json::to_string(&serde_json::json!({
                "schema": "agent-harness.supervisor-service-state.v1",
                "serviceId": "discord-gateway-loop",
                "serviceKind": "discord-gateway",
                "status": "spawning",
                "actualState": "spawning",
                "generationId": "gateway-generation-1",
                "pid": std::process::id(),
                "processId": std::process::id(),
                "supervisorPid": 1234,
                "processStartTimeMs": 900,
                "lastHeartbeatAtMs": 1200,
                "launchOwner": "rust-supervisor-run",
                "observedOnly": false
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            heartbeat_dir.join("discord-gateway-loop.json"),
            serde_json::to_string(&serde_json::json!({
                "schema": "agent-harness.loop-heartbeat.v1",
                "serviceId": "discord-gateway-loop",
                "status": "spawning",
                "generationId": "gateway-generation-1",
                "processId": std::process::id(),
                "atMs": 1200
            }))
            .unwrap(),
        )
        .unwrap();

        let report = collect_gateway_restart_status(&harness_home).unwrap();

        assert_eq!(
            report.latest_request.as_ref().and_then(|value| value.at_ms),
            Some(1000)
        );
        assert_eq!(
            report
                .latest_consumption
                .as_ref()
                .and_then(|value| value.consumed_at_ms),
            Some(1100)
        );
        assert_eq!(
            report
                .latest_completion
                .as_ref()
                .and_then(|value| value.heartbeat_at_ms),
            Some(1200)
        );
        assert_eq!(
            report.service.generation_id.as_deref(),
            Some("gateway-generation-1")
        );
        assert_eq!(report.service.process_alive, Some(true));
        assert_eq!(report.heartbeat.status.as_deref(), Some("spawning"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn restart_status_reads_request_consumption_generation_chain() {
        gateway_restart_status_reads_request_consumption_completion_and_generation();
    }

    #[test]
    fn loop_status_prefers_fresh_loop_heartbeat_over_spawning_service_state() {
        let root =
            temp_root("loop_status_prefers_fresh_loop_heartbeat_over_spawning_service_state");
        let harness_home = root.join(".agent-harness");
        let state = harness_home.join("state");
        let services_dir = state.join("supervisor").join("services");
        let heartbeat_dir = state.join("supervisor").join("loop-heartbeats");
        fs::create_dir_all(&services_dir).unwrap();
        fs::create_dir_all(&heartbeat_dir).unwrap();
        let pid = i64::from(std::process::id());
        let now_ms = current_ms_for_test();
        fs::write(
            services_dir.join("discord-gateway-loop.json"),
            serde_json::to_string(&serde_json::json!({
                "schema": "agent-harness.supervisor-service-state.v1",
                "serviceId": "discord-gateway-loop",
                "serviceKind": "discord-gateway",
                "generationId": "discord-gateway-loop-supervised-test",
                "pid": 0,
                "processId": 0,
                "supervisorPid": pid,
                "startedAtMs": now_ms - 10_000,
                "processStartTimeMs": now_ms - 10_000,
                "lastHeartbeatAtMs": now_ms - 10_000,
                "status": "spawning",
                "desiredState": "running",
                "actualState": "spawning",
                "detail": "starting Discord gateway subprocess",
                "launchOwner": "rust-supervisor-run",
                "observedOnly": false
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            heartbeat_dir.join("discord-gateway-loop.json"),
            serde_json::to_string(&serde_json::json!({
                "schema": "agent-harness.loop-heartbeat.v1",
                "name": "discord-gateway-loop",
                "status": "heartbeat",
                "processId": pid,
                "atMs": now_ms - 100,
                "detail": "Discord heartbeat ack"
            }))
            .unwrap(),
        )
        .unwrap();

        let loops = loop_status(&harness_home).unwrap();
        let service = loops
            .services
            .iter()
            .find(|service| service.service_id == "discord-gateway-loop")
            .unwrap();

        assert_eq!(service.process_id, Some(pid));
        assert_eq!(service.process_alive, Some(true));
        assert_eq!(service.status.as_deref(), Some("heartbeat"));
        assert_eq!(service.actual_state.as_deref(), Some("running"));
        assert_eq!(service.last_heartbeat_at_ms, Some(now_ms - 100));
        assert!(
            service
                .age_ms
                .is_some_and(|age_ms| (100..120_000).contains(&age_ms))
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn loop_status_reports_nul_heartbeat_as_corrupt() {
        let root = temp_root("loop_status_reports_nul_heartbeat_as_corrupt");
        let harness_home = root.join(".agent-harness");
        let heartbeat_dir = harness_home
            .join("state")
            .join("supervisor")
            .join("loop-heartbeats");
        fs::create_dir_all(&heartbeat_dir).unwrap();
        fs::write(heartbeat_dir.join("runtime-loop.json"), b"\0\0\0").unwrap();

        let loops = loop_status(&harness_home).unwrap();
        let runtime_loop = loops
            .heartbeats
            .iter()
            .find(|heartbeat| heartbeat.name == "runtime-loop")
            .unwrap();
        assert!(runtime_loop.present);
        assert!(runtime_loop.corrupt);
        assert!(runtime_loop.parse_error.is_some());
        assert_eq!(runtime_loop.status, None);
        assert_eq!(runtime_loop.process_alive, None);

        let mut warnings = Vec::new();
        append_loop_health_warnings(&loops, &mut warnings);
        assert!(warnings.iter().any(
            |warning| warning.contains("runtime-loop heartbeat") && warning.contains("corrupt")
        ));

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
    fn harness_status_reports_openclaw_mem_support_plane_paths() {
        let root = temp_root("harness_status_reports_openclaw_mem_support_plane_paths");
        let harness_home = root.join(".agent-harness");
        let memory_dir = harness_home.join("memory");
        let memory_state = harness_home.join("state").join("memory");
        fs::create_dir_all(memory_state.join("graph")).unwrap();
        fs::create_dir_all(&memory_dir).unwrap();
        fs::write(
            memory_dir.join("openclaw-mem.sqlite"),
            b"sqlite-placeholder",
        )
        .unwrap();
        fs::write(
            memory_state
                .join("graph")
                .join("topology-extract-full.json"),
            r#"{"nodes":[],"edges":[]}"#,
        )
        .unwrap();
        fs::write(memory_dir.join("openclaw-mem-service-store.jsonl"), "").unwrap();
        fs::write(memory_state.join("openclaw-mem-writeback.jsonl"), "").unwrap();

        let report = collect_harness_status(HarnessStatusOptions {
            harness_home: harness_home.clone(),
        })
        .unwrap();

        assert!(report.memory.support_plane.db_exists);
        assert!(report.memory.support_plane.service_store_exists);
        assert!(report.memory.support_plane.writeback_store_exists);
        assert_eq!(
            report
                .memory
                .support_plane
                .selected_topology_source
                .as_deref(),
            Some(
                memory_state
                    .join("graph")
                    .join("topology-extract-full.json")
                    .as_path()
            )
        );
        assert!(report.memory.support_plane.graph_autonomous_matching_ready);
        assert!(report.memory.support_plane.missing_reasons.is_empty());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn harness_status_reports_openclaw_mem_support_plane_missing_paths() {
        let root = temp_root("harness_status_reports_openclaw_mem_support_plane_missing_paths");
        let harness_home = root.join(".agent-harness");
        let report = collect_harness_status(HarnessStatusOptions {
            harness_home: harness_home.clone(),
        })
        .unwrap();

        assert!(!report.memory.support_plane.db_exists);
        assert!(!report.memory.support_plane.graph_autonomous_matching_ready);
        assert!(
            report
                .memory
                .support_plane
                .missing_reasons
                .iter()
                .any(|reason| reason.starts_with("db_missing:"))
        );
        assert!(
            report
                .memory
                .support_plane
                .missing_reasons
                .iter()
                .any(|reason| reason.starts_with("topology_source_missing:"))
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

    #[test]
    fn collect_status_reports_cron_scheduler_tick_age_and_canon_findings() {
        let root = temp_root("collect_status_reports_cron_scheduler_tick_age_and_canon_findings");
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
        let canon_dir = harness_home.join("workspace").join("docs").join("ops");
        let dream_dir = harness_home
            .join("state")
            .join("memory")
            .join("dream-lite-daily");
        let keeper_dir = harness_home.join("state").join("ops").join("cron-canon");
        fs::create_dir_all(&canon_dir).unwrap();
        fs::create_dir_all(&dream_dir).unwrap();
        fs::create_dir_all(&keeper_dir).unwrap();
        fs::write(
            dream_dir.join("latest.json"),
            serde_json::json!({
                "schema": "openclaw.mem.dream-lite.receipt.v1",
                "generatedAt": "1970-01-01T08:00:01+08:00",
                "ok": true
            })
            .to_string(),
        )
        .unwrap();
        fs::write(
            keeper_dir.join("latest-cron-canon-keeper.json"),
            serde_json::json!({
                "schema": "openclaw.agent-harness.cron-canon-keeper.receipt.v1",
                "generatedAt": "1970-01-01T00:00:01Z",
                "ok": false,
                "status": "warn",
                "findings": [{
                    "severity": "warn",
                    "code": "receipt-stale",
                    "cronId": "keeper-forwarded",
                    "message": "Latest receipt is older than canon allows.",
                    "details": {
                        "path": dream_dir.join("latest.json"),
                        "ageHours": 100,
                        "maxAgeHours": 36
                    }
                }]
            })
            .to_string(),
        )
        .unwrap();
        fs::write(
            canon_dir.join("cron-canon.json"),
            serde_json::json!({
                "schema": "openclaw.agent-harness.cron-canon.v1",
                "paths": {
                    "keeperReceipt": "state/ops/cron-canon/latest-cron-canon-keeper.json"
                },
                "activeCrons": [
                    {
                        "id": "openclaw-mem-dream-lite-daily-0320",
                        "enabled": true,
                        "kind": "deterministic-crontab",
                        "monitor": {
                            "type": "latest-json",
                            "path": "state/memory/dream-lite-daily/latest.json",
                            "maxAgeHours": 36,
                            "okField": "ok",
                            "okValue": true
                        }
                    },
                    {
                        "id": "cron-canon-keeper-daily-0920",
                        "enabled": true,
                        "kind": "deterministic-crontab",
                        "monitor": {
                            "type": "latest-json",
                            "path": "state/ops/cron-canon/latest-cron-canon-keeper.json",
                            "maxAgeHours": 36,
                            "okField": "ok",
                            "okValue": true
                        }
                    }
                ]
            })
            .to_string(),
        )
        .unwrap();

        let report = collect_harness_status(HarnessStatusOptions {
            harness_home: harness_home.clone(),
        })
        .unwrap();

        assert!(report.cron_scheduler.canon.canon_present);
        assert_eq!(report.cron_scheduler.canon.monitor_count, 2);
        assert!(
            report
                .cron_scheduler
                .canon
                .keeper_age_hours
                .is_some_and(|age| age > 36.0)
        );
        assert_eq!(
            report.cron_scheduler.canon.keeper_status.as_deref(),
            Some("warn")
        );
        assert!(report.cron_scheduler.canon.stale_count >= 2);
        assert!(
            report
                .warnings
                .iter()
                .any(|warning| { warning.contains("cron canon keeper status=warn") })
        );
        assert!(report.warnings.iter().any(|warning| {
            warning.contains("openclaw-mem-dream-lite-daily-0320")
                && warning.contains("receipt-stale")
                && warning.contains("ageHours=")
                && warning.contains("maxAgeHours=36.000")
        }));
        assert!(report.warnings.iter().any(|warning| {
            warning.contains("cron-canon-keeper-daily-0920") && warning.contains("receipt-not-ok")
        }));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn collect_status_accepts_utf8_bom_cron_canon_and_monitor_receipts() {
        let root = temp_root("collect_status_accepts_utf8_bom_cron_canon_and_monitor_receipts");
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
        let canon_dir = harness_home.join("workspace").join("docs").join("ops");
        let keeper_dir = harness_home.join("state").join("ops").join("cron-canon");
        let health_dir = harness_home.join("state").join("ops").join("health");
        fs::create_dir_all(&canon_dir).unwrap();
        fs::create_dir_all(&keeper_dir).unwrap();
        fs::create_dir_all(&health_dir).unwrap();
        fs::write(
            keeper_dir.join("latest-cron-canon-keeper.json"),
            format!(
                "\u{feff}{}",
                serde_json::json!({
                    "schema": "openclaw.agent-harness.cron-canon-keeper.receipt.v1",
                    "ok": true,
                    "status": "ok",
                    "findings": []
                })
            ),
        )
        .unwrap();
        fs::write(
            health_dir.join("latest-health.json"),
            format!(
                "\u{feff}{}",
                serde_json::json!({
                    "schema": "openclaw.agent-harness.health-check.receipt.v1",
                    "ok": true
                })
            ),
        )
        .unwrap();
        fs::write(
            canon_dir.join("cron-canon.json"),
            format!(
                "\u{feff}{}",
                serde_json::json!({
                    "schema": "openclaw.agent-harness.cron-canon.v1",
                    "paths": {
                        "keeperReceipt": "state/ops/cron-canon/latest-cron-canon-keeper.json"
                    },
                    "activeCrons": [{
                        "id": "agent-harness-health-q4h",
                        "enabled": true,
                        "kind": "deterministic-crontab",
                        "monitor": {
                            "type": "latest-json",
                            "path": "state/ops/health/latest-health.json",
                            "maxAgeHours": 8,
                            "okField": "ok",
                            "okValue": true
                        }
                    }]
                })
            ),
        )
        .unwrap();

        let report = collect_harness_status(HarnessStatusOptions {
            harness_home: harness_home.clone(),
        })
        .unwrap();

        assert!(report.cron_scheduler.canon.canon_present);
        assert_eq!(report.cron_scheduler.canon.monitor_count, 1);
        assert_eq!(report.cron_scheduler.canon.finding_count, 0);
        assert!(
            report
                .warnings
                .iter()
                .all(|warning| !warning.contains("receipt-invalid-json")),
            "{:?}",
            report.warnings
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn collect_status_includes_custom_supervisor_loop_heartbeats() {
        let root = temp_root("collect_status_includes_custom_supervisor_loop_heartbeats");
        let harness_home = root.join(".agent-harness");
        let heartbeat_dir = harness_home
            .join("state")
            .join("supervisor")
            .join("loop-heartbeats");
        let plan_dir = harness_home
            .join("state")
            .join("supervisor")
            .join("windows-scheduled-tasks");
        fs::create_dir_all(&heartbeat_dir).unwrap();
        fs::create_dir_all(&plan_dir).unwrap();
        crate::write_json_atomic(
            &heartbeat_dir.join("telegram-loop-xiaoxiaoli.json"),
            &serde_json::json!({
                "status": "running",
                "iteration": 11,
                "processId": 456,
                "atMs": 1_000
            }),
        )
        .unwrap();
        crate::write_json_atomic(
            &plan_dir.join("supervisor-plan.json"),
            &serde_json::json!({
                "tasks": [
                    {
                        "name": "AgentHarness-telegram-loop-xiaoxiaoli",
                        "component": "telegram-loop-xiaoxiaoli",
                        "runnerScript": "telegram-loop-xiaoxiaoli.ps1"
                    }
                ]
            }),
        )
        .unwrap();

        let report = collect_harness_status(HarnessStatusOptions { harness_home }).unwrap();

        let custom = report
            .loops
            .heartbeats
            .iter()
            .find(|item| item.name == "telegram-loop-xiaoxiaoli")
            .expect("custom supervisor loop should be reported");
        assert!(custom.present);
        assert_eq!(custom.status.as_deref(), Some("running"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn runtime_status_treats_history_as_an_exact_pending_recovery_guard_not_current_telemetry() {
        let root = temp_root("runtime_status_history_exact_pending_recovery_guard");
        let harness_home = root.join(".agent-harness");
        let queue_dir = harness_home.join("state").join("runtime-queue");
        fs::create_dir_all(&queue_dir).unwrap();
        let staged = crate::runtime_receipt_history::stage_runtime_queue_receipt_history(
            &queue_dir,
            "status-history",
            br#"{"queueId":"queue-terminal","status":"completed","reason":"old terminal"}
"#,
            b"",
            &std::collections::HashSet::new(),
            100,
        )
        .unwrap();
        crate::runtime_receipt_history::commit_runtime_queue_receipt_history(&staged, 101).unwrap();
        fs::write(
            queue_dir.join("pending.jsonl"),
            [
                serde_json::json!({
                    "queueId":"queue-terminal",
                    "status":"queued",
                    "runtimeClass":"interactive",
                    "origin":"channel"
                })
                .to_string(),
                serde_json::json!({
                    "queueId":"queue-open",
                    "status":"queued",
                    "runtimeClass":"interactive",
                    "origin":"channel"
                })
                .to_string(),
            ]
            .join("\n"),
        )
        .unwrap();
        fs::write(queue_dir.join("run-once-receipts.jsonl"), "").unwrap();

        let report = runtime_status(&harness_home, &mut Vec::new()).unwrap();

        assert_eq!(report.queued_items, 2);
        assert_eq!(report.open_items, 1);
        assert!(
            report.latest_non_idle_run_once.is_none(),
            "cold history must not replace current-ledger telemetry"
        );
        assert_eq!(report.run_once_receipts.lines, 0);
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

    fn current_ms_for_test() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }
}
