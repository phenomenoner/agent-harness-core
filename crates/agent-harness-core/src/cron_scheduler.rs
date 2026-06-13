use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use rusqlite::{Connection, Error as SqlError, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    AgentSource, DeterministicCronPlanAction, DeterministicCronPlanInput,
    DeterministicCronSchedule, NativeCronPlanAction, NativeCronPlanInput, NativeCronSchedule,
    WorkerEnqueueOptions, WorkerEnqueueReport, WorkerJobKind, append_harness_log,
    append_jsonl_value, config::harness_config_candidates, current_log_time_ms, enqueue_worker_job,
    load_agent_registry, load_deterministic_cron_store, load_native_cron_store,
    plan_deterministic_cron, plan_native_cron, write_json_atomic,
};

const CRON_SCHEDULER_RUN_ONCE_SCHEMA: &str = "agent-harness.cron-scheduler.run-once.v1";
const CRON_SCHEDULER_TICK_SCHEMA: &str = "agent-harness.cron-scheduler.tick.v1";
const CRON_SCHEDULER_JOB_DECISION_SCHEMA: &str = "agent-harness.cron-scheduler.job-decision.v1";
const DEFAULT_INTERVAL_MS: i64 = 60_000;
const DEFAULT_MAX_CATCHUP_PER_TICK: usize = 10;
const DEFAULT_MAX_ENQUEUE_PER_TICK: usize = 20;
const DEFAULT_WORKER_TIMEOUT_MS: u64 = 300_000;
const DEFAULT_WATCHDOG_TIMEOUT_MS: u64 = 900_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronSchedulerRunOnceOptions {
    pub harness_home: PathBuf,
    pub source: AgentSource,
    pub runtime_workspace: Option<PathBuf>,
    pub now_ms: i64,
    pub dry_run: bool,
    pub enabled_override: Option<bool>,
    pub native_enabled_override: Option<bool>,
    pub deterministic_enabled_override: Option<bool>,
    pub resume_cron_override: Option<bool>,
    pub include_registered_cron_override: Option<bool>,
    pub allow_deterministic_run_override: Option<bool>,
    pub execute_shell_override: Option<bool>,
    pub max_catchup_per_tick_override: Option<usize>,
    pub max_enqueue_per_tick_override: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronSchedulerLoopOptions {
    pub run_once: CronSchedulerRunOnceOptions,
    pub interval_ms: i64,
    pub max_consecutive_errors: usize,
    pub stop_file: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CronSchedulerRunOnceReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub source_home: PathBuf,
    pub source_workspace: PathBuf,
    pub status: CronSchedulerTickStatus,
    pub config_file: Option<PathBuf>,
    pub config: CronSchedulerConfig,
    pub database: PathBuf,
    pub receipts_file: PathBuf,
    pub loop_last_file: PathBuf,
    pub decisions: Vec<CronSchedulerJobDecision>,
    pub summary: CronSchedulerTickSummary,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CronSchedulerTickSummary {
    pub native_entries: usize,
    pub deterministic_entries: usize,
    pub due_candidates: usize,
    pub enqueued: usize,
    pub skipped_held: usize,
    pub skipped_duplicate: usize,
    pub skipped_policy: usize,
    pub errors: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CronSchedulerTickStatus {
    Disabled,
    DryRun,
    Completed,
    LockBusy,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CronSchedulerTickReceipt {
    pub schema: &'static str,
    pub status: CronSchedulerTickStatus,
    pub source_home: PathBuf,
    pub source_workspace: PathBuf,
    pub now_ms: i64,
    pub config_file: Option<PathBuf>,
    pub dry_run: bool,
    pub summary: CronSchedulerTickSummary,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CronSchedulerJobDecision {
    pub schema: &'static str,
    pub source_kind: String,
    pub source_id: String,
    pub entry_id: String,
    pub scheduled_for_ms: i64,
    pub enqueued_at_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_job_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    pub decision: CronSchedulerJobDecisionStatus,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CronSchedulerJobDecisionStatus {
    Enqueued,
    SkippedHeld,
    SkippedDuplicate,
    SkippedPolicy,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CronSchedulerConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_interval_ms")]
    pub interval_ms: i64,
    #[serde(default)]
    pub native_cron: CronSchedulerNativeConfig,
    #[serde(default)]
    pub deterministic_cron: CronSchedulerDeterministicConfig,
    #[serde(default = "default_max_catchup_per_tick")]
    pub max_catchup_per_tick: usize,
    #[serde(default = "default_max_enqueue_per_tick")]
    pub max_enqueue_per_tick: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CronSchedulerNativeConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub resume_cron: bool,
    #[serde(default)]
    pub include_registered_cron: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CronSchedulerDeterministicConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub allow_deterministic_run: bool,
    #[serde(default)]
    pub execute_shell: bool,
}

impl Default for CronSchedulerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            interval_ms: DEFAULT_INTERVAL_MS,
            native_cron: CronSchedulerNativeConfig::default(),
            deterministic_cron: CronSchedulerDeterministicConfig::default(),
            max_catchup_per_tick: DEFAULT_MAX_CATCHUP_PER_TICK,
            max_enqueue_per_tick: DEFAULT_MAX_ENQUEUE_PER_TICK,
        }
    }
}

impl Default for CronSchedulerNativeConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            resume_cron: false,
            include_registered_cron: false,
        }
    }
}

impl Default for CronSchedulerDeterministicConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            allow_deterministic_run: false,
            execute_shell: false,
        }
    }
}

struct SchedulerLock {
    path: PathBuf,
    _file: fs::File,
}

impl Drop for SchedulerLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

pub fn run_cron_scheduler_once(
    options: CronSchedulerRunOnceOptions,
) -> io::Result<CronSchedulerRunOnceReport> {
    let scheduler_dir = options.harness_home.join("state").join("cron-scheduler");
    fs::create_dir_all(&scheduler_dir)?;
    let database = scheduler_dir.join("watermarks.sqlite");
    let receipts_file = scheduler_dir.join("receipts.jsonl");
    let loop_last_file = scheduler_dir.join("loop-last.json");
    let mut warnings = Vec::new();
    let (mut config, config_file) =
        load_cron_scheduler_config(&options.harness_home, &mut warnings)?;
    apply_overrides(&mut config, &options);

    let Some(_lock) = acquire_scheduler_lock(&scheduler_dir, options.now_ms)? else {
        let report = CronSchedulerRunOnceReport {
            schema: CRON_SCHEDULER_RUN_ONCE_SCHEMA,
            harness_home: options.harness_home,
            source_home: options.source.home,
            source_workspace: options.source.workspace,
            status: CronSchedulerTickStatus::LockBusy,
            config_file,
            config,
            database,
            receipts_file,
            loop_last_file,
            decisions: Vec::new(),
            summary: CronSchedulerTickSummary::default(),
            warnings,
        };
        append_tick_receipt(&report, options.now_ms, options.dry_run)?;
        return write_loop_last_report(report);
    };

    init_watermarks(&database)?;
    let mut report = CronSchedulerRunOnceReport {
        schema: CRON_SCHEDULER_RUN_ONCE_SCHEMA,
        harness_home: options.harness_home.clone(),
        source_home: options.source.home.clone(),
        source_workspace: options.source.workspace.clone(),
        status: CronSchedulerTickStatus::Completed,
        config_file,
        config: config.clone(),
        database: database.clone(),
        receipts_file: receipts_file.clone(),
        loop_last_file: loop_last_file.clone(),
        decisions: Vec::new(),
        summary: CronSchedulerTickSummary::default(),
        warnings,
    };

    if !config.enabled {
        report.status = CronSchedulerTickStatus::Disabled;
        report.warnings.push(
            "cronScheduler.enabled is false; scheduler tick did not enqueue work".to_string(),
        );
        append_tick_receipt(&report, options.now_ms, options.dry_run)?;
        return write_loop_last_report(report);
    }

    if options.dry_run {
        report.status = CronSchedulerTickStatus::DryRun;
    }

    if config.native_cron.enabled {
        if let Err(error) = collect_native_cron_decisions(&options, &config, &database, &mut report)
        {
            report.status = CronSchedulerTickStatus::Error;
            report.summary.errors += 1;
            report
                .warnings
                .push(format!("native cron scheduler tick failed: {error}"));
        }
    }
    if config.deterministic_cron.enabled {
        if let Err(error) =
            collect_deterministic_cron_decisions(&options, &config, &database, &mut report)
        {
            report.status = CronSchedulerTickStatus::Error;
            report.summary.errors += 1;
            report
                .warnings
                .push(format!("deterministic cron scheduler tick failed: {error}"));
        }
    }
    if !config.native_cron.enabled && !config.deterministic_cron.enabled {
        report
            .warnings
            .push("cronScheduler has no enabled source lanes".to_string());
    }

    for decision in &report.decisions {
        append_jsonl_value(&receipts_file, decision)?;
    }
    append_tick_receipt(&report, options.now_ms, options.dry_run)?;
    append_harness_log(
        &options.harness_home,
        &crate::HarnessLogEvent::new(
            current_log_time_ms()?,
            if report.status == CronSchedulerTickStatus::Error {
                crate::HarnessLogLevel::Warn
            } else {
                crate::HarnessLogLevel::Info
            },
            "cron-scheduler",
            "cron-scheduler.tick",
            format!(
                "cron scheduler tick {:?}: enqueued={}, duplicate={}, policy={}, errors={}",
                report.status,
                report.summary.enqueued,
                report.summary.skipped_duplicate,
                report.summary.skipped_policy,
                report.summary.errors
            ),
        ),
    )?;
    write_loop_last_report(report)
}

fn collect_native_cron_decisions(
    options: &CronSchedulerRunOnceOptions,
    config: &CronSchedulerConfig,
    database: &Path,
    report: &mut CronSchedulerRunOnceReport,
) -> io::Result<()> {
    let store = load_native_cron_store(&options.source)?;
    let registry = load_agent_registry(&options.source)?;
    let plan = plan_native_cron(
        &store,
        &registry,
        NativeCronPlanInput {
            now_ms: options.now_ms,
            resume_enabled: config.native_cron.resume_cron,
        },
    );
    report.warnings.extend(plan.warnings.clone());
    report.summary.native_entries += plan.entries.len();
    let source_id = stable_source_id("native-cron", &options.source.home);
    let master_agent = "main".to_string();
    let master_session = format!("worker-group:cron-scheduler:{source_id}");

    for entry in &plan.entries {
        if report.summary.enqueued >= config.max_enqueue_per_tick {
            push_decision(
                report,
                "native-cron",
                &source_id,
                &entry.job_id,
                options.now_ms,
                CronSchedulerJobDecisionStatus::SkippedPolicy,
                "maxEnqueuePerTick reached",
                None,
            );
            continue;
        }
        match entry.action {
            NativeCronPlanAction::Disabled
            | NativeCronPlanAction::MissingAgent
            | NativeCronPlanAction::UnsupportedSchedule
            | NativeCronPlanAction::WaitingSchedule => {
                push_decision(
                    report,
                    "native-cron",
                    &source_id,
                    &entry.job_id,
                    options.now_ms,
                    CronSchedulerJobDecisionStatus::SkippedPolicy,
                    &entry.reason,
                    None,
                );
            }
            NativeCronPlanAction::CutoverHold => {
                push_decision(
                    report,
                    "native-cron",
                    &source_id,
                    &entry.job_id,
                    options.now_ms,
                    CronSchedulerJobDecisionStatus::SkippedHeld,
                    &entry.reason,
                    None,
                );
            }
            NativeCronPlanAction::EnqueueAgentTurn | NativeCronPlanAction::CronRegistered => {
                if entry.action == NativeCronPlanAction::CronRegistered
                    && !config.native_cron.include_registered_cron
                {
                    push_decision(
                        report,
                        "native-cron",
                        &source_id,
                        &entry.job_id,
                        options.now_ms,
                        CronSchedulerJobDecisionStatus::SkippedPolicy,
                        "includeRegisteredCron is false",
                        None,
                    );
                    continue;
                }
                let scheduled_for_ms = match native_due_slot(&entry.schedule, options.now_ms) {
                    Ok(Some(value)) => value,
                    Ok(None) => {
                        push_decision(
                            report,
                            "native-cron",
                            &source_id,
                            &entry.job_id,
                            options.now_ms,
                            CronSchedulerJobDecisionStatus::SkippedPolicy,
                            "schedule is not due in the current scheduler slot",
                            None,
                        );
                        continue;
                    }
                    Err(error) => {
                        push_decision(
                            report,
                            "native-cron",
                            &source_id,
                            &entry.job_id,
                            options.now_ms,
                            CronSchedulerJobDecisionStatus::Error,
                            &error,
                            None,
                        );
                        continue;
                    }
                };
                report.summary.due_candidates += 1;
                if options.dry_run {
                    push_decision(
                        report,
                        "native-cron",
                        &source_id,
                        &entry.job_id,
                        scheduled_for_ms,
                        CronSchedulerJobDecisionStatus::SkippedPolicy,
                        "dry-run: due native cron job would enqueue",
                        None,
                    );
                    continue;
                }
                if watermark_exists(
                    database,
                    "native-cron",
                    &source_id,
                    &entry.job_id,
                    scheduled_for_ms,
                )? {
                    push_decision(
                        report,
                        "native-cron",
                        &source_id,
                        &entry.job_id,
                        scheduled_for_ms,
                        CronSchedulerJobDecisionStatus::SkippedDuplicate,
                        "watermark already exists for this scheduled slot",
                        None,
                    );
                    continue;
                }
                let Some(agent_id) = entry.agent_id.clone() else {
                    push_decision(
                        report,
                        "native-cron",
                        &source_id,
                        &entry.job_id,
                        scheduled_for_ms,
                        CronSchedulerJobDecisionStatus::Error,
                        "native cron entry had no agent id",
                        None,
                    );
                    continue;
                };
                let message_text = entry
                    .message_text
                    .clone()
                    .unwrap_or_else(|| format!("Run native cron job {}", entry.job_id));
                let session_key = entry.session_key.clone().unwrap_or_else(|| {
                    format!(
                        "cron:{}:{}",
                        normalize_key_part(&entry.job_id),
                        normalize_key_part(&agent_id)
                    )
                });
                let idempotency_key = scheduler_idempotency_key(
                    "native-cron",
                    &source_id,
                    &entry.job_id,
                    scheduled_for_ms,
                );
                let payload = json!({
                    "adapter": "cron-scheduler",
                    "sourceKind": "native-cron",
                    "sourceId": source_id,
                    "entryId": entry.job_id,
                    "scheduledForMs": scheduled_for_ms,
                    "sourceHome": &options.source.home,
                    "sourceWorkspace": &options.source.workspace,
                    "runtimeWorkspace": &options.runtime_workspace,
                    "agentId": agent_id,
                    "sessionKey": session_key,
                    "platform": "native-cron",
                    "channelId": entry.job_id,
                    "userId": "cron-scheduler",
                    "messageText": message_text,
                    "inboundContext": serde_json::to_string(entry).unwrap_or_default()
                });
                let enqueue = enqueue_worker_job(WorkerEnqueueOptions {
                    harness_home: options.harness_home.clone(),
                    kind: WorkerJobKind::LlmSubagent,
                    lane: Some("llm".to_string()),
                    payload,
                    idempotency_key: Some(idempotency_key.clone()),
                    parent_job_id: None,
                    job_group_id: Some(format!("cron-scheduler:{source_id}")),
                    master_agent_id: Some(master_agent.clone()),
                    master_session_key: Some(master_session.clone()),
                    wake_policy: None,
                    source: Some("cron-scheduler".to_string()),
                    priority: 0,
                    available_at_ms: Some(options.now_ms),
                    max_attempts: 3,
                    timeout_ms: Some(DEFAULT_WORKER_TIMEOUT_MS),
                    cascade_timeout_ms: Some(DEFAULT_WATCHDOG_TIMEOUT_MS),
                    rate_key: Some(format!("llm:{agent_id}")),
                    concurrency_group_key: Some(format!(
                        "{}:{}",
                        normalize_key_part(&master_agent),
                        normalize_key_part(&master_session)
                    )),
                    now_ms: options.now_ms,
                })?;
                insert_watermark(
                    database,
                    "native-cron",
                    &source_id,
                    &entry.job_id,
                    scheduled_for_ms,
                    options.now_ms,
                    &enqueue.job.job_id,
                    CronSchedulerJobDecisionStatus::Enqueued,
                    if enqueue.inserted {
                        "worker job inserted"
                    } else {
                        "worker idempotency key already existed"
                    },
                )?;
                push_decision(
                    report,
                    "native-cron",
                    &source_id,
                    &entry.job_id,
                    scheduled_for_ms,
                    if enqueue.inserted {
                        CronSchedulerJobDecisionStatus::Enqueued
                    } else {
                        CronSchedulerJobDecisionStatus::SkippedDuplicate
                    },
                    if enqueue.inserted {
                        "worker job inserted"
                    } else {
                        "worker idempotency key already existed"
                    },
                    Some((enqueue, idempotency_key)),
                );
            }
        }
    }
    Ok(())
}

fn collect_deterministic_cron_decisions(
    options: &CronSchedulerRunOnceOptions,
    config: &CronSchedulerConfig,
    database: &Path,
    report: &mut CronSchedulerRunOnceReport,
) -> io::Result<()> {
    let store = load_deterministic_cron_store(&options.source)?;
    let plan = plan_deterministic_cron(
        &store,
        DeterministicCronPlanInput {
            allow_deterministic_run: config.deterministic_cron.allow_deterministic_run,
        },
    );
    report.warnings.extend(plan.warnings.clone());
    report.summary.deterministic_entries += plan.entries.len();
    let source_id = stable_source_id("deterministic-cron", &options.source.workspace);
    let master_agent = "main".to_string();
    let master_session = format!("worker-group:cron-scheduler:{source_id}");

    for entry in &plan.entries {
        if report.summary.enqueued >= config.max_enqueue_per_tick {
            push_decision(
                report,
                "deterministic-cron",
                &source_id,
                &entry.entry_id,
                options.now_ms,
                CronSchedulerJobDecisionStatus::SkippedPolicy,
                "maxEnqueuePerTick reached",
                None,
            );
            continue;
        }
        if entry.action == DeterministicCronPlanAction::CutoverHold {
            push_decision(
                report,
                "deterministic-cron",
                &source_id,
                &entry.entry_id,
                options.now_ms,
                CronSchedulerJobDecisionStatus::SkippedHeld,
                &entry.reason,
                None,
            );
            continue;
        }
        if entry.action != DeterministicCronPlanAction::ReadyCommand {
            push_decision(
                report,
                "deterministic-cron",
                &source_id,
                &entry.entry_id,
                options.now_ms,
                CronSchedulerJobDecisionStatus::SkippedPolicy,
                &entry.reason,
                None,
            );
            continue;
        }
        let scheduled_for_ms = match deterministic_due_slot(&entry.schedule, options.now_ms) {
            Ok(Some(value)) => value,
            Ok(None) => {
                push_decision(
                    report,
                    "deterministic-cron",
                    &source_id,
                    &entry.entry_id,
                    options.now_ms,
                    CronSchedulerJobDecisionStatus::SkippedPolicy,
                    "schedule is not due in the current scheduler slot",
                    None,
                );
                continue;
            }
            Err(error) => {
                push_decision(
                    report,
                    "deterministic-cron",
                    &source_id,
                    &entry.entry_id,
                    options.now_ms,
                    CronSchedulerJobDecisionStatus::Error,
                    &error,
                    None,
                );
                continue;
            }
        };
        report.summary.due_candidates += 1;
        if options.dry_run {
            push_decision(
                report,
                "deterministic-cron",
                &source_id,
                &entry.entry_id,
                scheduled_for_ms,
                CronSchedulerJobDecisionStatus::SkippedPolicy,
                "dry-run: due deterministic cron command would enqueue",
                None,
            );
            continue;
        }
        if watermark_exists(
            database,
            "deterministic-cron",
            &source_id,
            &entry.entry_id,
            scheduled_for_ms,
        )? {
            push_decision(
                report,
                "deterministic-cron",
                &source_id,
                &entry.entry_id,
                scheduled_for_ms,
                CronSchedulerJobDecisionStatus::SkippedDuplicate,
                "watermark already exists for this scheduled slot",
                None,
            );
            continue;
        }
        let Some(script_path) = entry.script_path.clone() else {
            push_decision(
                report,
                "deterministic-cron",
                &source_id,
                &entry.entry_id,
                scheduled_for_ms,
                CronSchedulerJobDecisionStatus::Error,
                "deterministic cron entry had no script path",
                None,
            );
            continue;
        };
        let idempotency_key = scheduler_idempotency_key(
            "deterministic-cron",
            &source_id,
            &entry.entry_id,
            scheduled_for_ms,
        );
        let cwd = script_path.parent().unwrap_or(Path::new(".")).to_path_buf();
        let payload = json!({
            "adapter": "cron-scheduler",
            "sourceKind": "deterministic-cron",
            "sourceId": source_id,
            "entryId": entry.entry_id,
            "scheduledForMs": scheduled_for_ms,
            "runnerKind": entry.runner_kind,
            "command": entry.command,
            "scriptPath": script_path,
            "cwd": cwd,
            "argv": [],
            "dryRun": !config.deterministic_cron.execute_shell,
            "sourceHome": &options.source.home,
            "sourceWorkspace": &options.source.workspace,
            "runtimeWorkspace": &options.runtime_workspace,
        });
        let enqueue = enqueue_worker_job(WorkerEnqueueOptions {
            harness_home: options.harness_home.clone(),
            kind: WorkerJobKind::DeterministicShell,
            lane: Some("shell".to_string()),
            payload,
            idempotency_key: Some(idempotency_key.clone()),
            parent_job_id: None,
            job_group_id: Some(format!("cron-scheduler:{source_id}")),
            master_agent_id: Some(master_agent.clone()),
            master_session_key: Some(master_session.clone()),
            wake_policy: None,
            source: Some("cron-scheduler".to_string()),
            priority: 0,
            available_at_ms: Some(options.now_ms),
            max_attempts: 3,
            timeout_ms: Some(DEFAULT_WORKER_TIMEOUT_MS),
            cascade_timeout_ms: Some(DEFAULT_WATCHDOG_TIMEOUT_MS),
            rate_key: None,
            concurrency_group_key: Some(format!(
                "{}:{}",
                normalize_key_part(&master_agent),
                normalize_key_part(&master_session)
            )),
            now_ms: options.now_ms,
        })?;
        insert_watermark(
            database,
            "deterministic-cron",
            &source_id,
            &entry.entry_id,
            scheduled_for_ms,
            options.now_ms,
            &enqueue.job.job_id,
            CronSchedulerJobDecisionStatus::Enqueued,
            if enqueue.inserted {
                "worker job inserted"
            } else {
                "worker idempotency key already existed"
            },
        )?;
        push_decision(
            report,
            "deterministic-cron",
            &source_id,
            &entry.entry_id,
            scheduled_for_ms,
            if enqueue.inserted {
                CronSchedulerJobDecisionStatus::Enqueued
            } else {
                CronSchedulerJobDecisionStatus::SkippedDuplicate
            },
            if enqueue.inserted {
                "worker job inserted"
            } else {
                "worker idempotency key already existed"
            },
            Some((enqueue, idempotency_key)),
        );
    }
    Ok(())
}

fn push_decision(
    report: &mut CronSchedulerRunOnceReport,
    source_kind: &str,
    source_id: &str,
    entry_id: &str,
    scheduled_for_ms: i64,
    decision: CronSchedulerJobDecisionStatus,
    reason: &str,
    enqueue: Option<(WorkerEnqueueReport, String)>,
) {
    match decision {
        CronSchedulerJobDecisionStatus::Enqueued => report.summary.enqueued += 1,
        CronSchedulerJobDecisionStatus::SkippedHeld => report.summary.skipped_held += 1,
        CronSchedulerJobDecisionStatus::SkippedDuplicate => report.summary.skipped_duplicate += 1,
        CronSchedulerJobDecisionStatus::SkippedPolicy => report.summary.skipped_policy += 1,
        CronSchedulerJobDecisionStatus::Error => report.summary.errors += 1,
    }
    report.decisions.push(CronSchedulerJobDecision {
        schema: CRON_SCHEDULER_JOB_DECISION_SCHEMA,
        source_kind: source_kind.to_string(),
        source_id: source_id.to_string(),
        entry_id: entry_id.to_string(),
        scheduled_for_ms,
        enqueued_at_ms: enqueue
            .as_ref()
            .map(|_| report.config.interval_ms)
            .map(|_| {
                // The caller passes a scheduler-slot decision after the actual enqueue,
                // so the receipt timestamp is the tick's current wall clock.
                current_log_time_ms().unwrap_or(scheduled_for_ms)
            }),
        worker_job_id: enqueue
            .as_ref()
            .map(|(report, _)| report.job.job_id.clone()),
        idempotency_key: enqueue.map(|(_, key)| key),
        decision,
        reason: reason.to_string(),
    });
}

fn load_cron_scheduler_config(
    harness_home: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<(CronSchedulerConfig, Option<PathBuf>)> {
    let Some(config_file) = harness_config_candidates(harness_home)
        .into_iter()
        .find(|path| path.is_file())
    else {
        return Ok((CronSchedulerConfig::default(), None));
    };
    let text = fs::read_to_string(&config_file)?;
    let value: Value = match serde_json::from_str(&text) {
        Ok(value) => value,
        Err(error) => {
            warnings.push(format!(
                "harness-config {} is not valid JSON while loading cronScheduler: {error}",
                config_file.display()
            ));
            return Ok((CronSchedulerConfig::default(), Some(config_file)));
        }
    };
    let Some(section) = value.get("cronScheduler") else {
        return Ok((CronSchedulerConfig::default(), Some(config_file)));
    };
    match serde_json::from_value::<CronSchedulerConfig>(section.clone()) {
        Ok(mut config) => {
            if config.interval_ms <= 0 {
                warnings.push(
                    "cronScheduler.intervalMs must be positive; using default interval".to_string(),
                );
                config.interval_ms = DEFAULT_INTERVAL_MS;
            }
            if config.max_catchup_per_tick == 0 {
                warnings
                    .push("cronScheduler.maxCatchupPerTick was zero; using default".to_string());
                config.max_catchup_per_tick = DEFAULT_MAX_CATCHUP_PER_TICK;
            }
            if config.max_enqueue_per_tick == 0 {
                warnings
                    .push("cronScheduler.maxEnqueuePerTick was zero; using default".to_string());
                config.max_enqueue_per_tick = DEFAULT_MAX_ENQUEUE_PER_TICK;
            }
            Ok((config, Some(config_file)))
        }
        Err(error) => {
            warnings.push(format!(
                "cronScheduler section in {} is invalid: {error}",
                config_file.display()
            ));
            Ok((CronSchedulerConfig::default(), Some(config_file)))
        }
    }
}

fn apply_overrides(config: &mut CronSchedulerConfig, options: &CronSchedulerRunOnceOptions) {
    if let Some(value) = options.enabled_override {
        config.enabled = value;
    }
    if let Some(value) = options.native_enabled_override {
        config.native_cron.enabled = value;
    }
    if let Some(value) = options.deterministic_enabled_override {
        config.deterministic_cron.enabled = value;
    }
    if let Some(value) = options.resume_cron_override {
        config.native_cron.resume_cron = value;
    }
    if let Some(value) = options.include_registered_cron_override {
        config.native_cron.include_registered_cron = value;
    }
    if let Some(value) = options.allow_deterministic_run_override {
        config.deterministic_cron.allow_deterministic_run = value;
    }
    if let Some(value) = options.execute_shell_override {
        config.deterministic_cron.execute_shell = value;
    }
    if let Some(value) = options.max_catchup_per_tick_override {
        config.max_catchup_per_tick = value.max(1);
    }
    if let Some(value) = options.max_enqueue_per_tick_override {
        config.max_enqueue_per_tick = value.max(1);
    }
}

fn init_watermarks(database: &Path) -> io::Result<()> {
    let conn = Connection::open(database).map_err(io::Error::other)?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS cron_scheduler_watermarks (
            source_kind TEXT NOT NULL,
            source_id TEXT NOT NULL,
            entry_id TEXT NOT NULL,
            scheduled_for_ms INTEGER NOT NULL,
            enqueued_at_ms INTEGER NOT NULL,
            worker_job_id TEXT NOT NULL,
            decision TEXT NOT NULL,
            reason TEXT NOT NULL,
            PRIMARY KEY (source_kind, source_id, entry_id, scheduled_for_ms)
        );",
    )
    .map_err(io::Error::other)
}

fn watermark_exists(
    database: &Path,
    source_kind: &str,
    source_id: &str,
    entry_id: &str,
    scheduled_for_ms: i64,
) -> io::Result<bool> {
    let conn = Connection::open(database).map_err(io::Error::other)?;
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM cron_scheduler_watermarks
             WHERE source_kind = ?1 AND source_id = ?2 AND entry_id = ?3 AND scheduled_for_ms = ?4",
            params![source_kind, source_id, entry_id, scheduled_for_ms],
            |row| row.get(0),
        )
        .map_err(io::Error::other)?;
    Ok(count > 0)
}

fn insert_watermark(
    database: &Path,
    source_kind: &str,
    source_id: &str,
    entry_id: &str,
    scheduled_for_ms: i64,
    enqueued_at_ms: i64,
    worker_job_id: &str,
    decision: CronSchedulerJobDecisionStatus,
    reason: &str,
) -> io::Result<bool> {
    let conn = Connection::open(database).map_err(io::Error::other)?;
    match conn.execute(
        "INSERT INTO cron_scheduler_watermarks
         (source_kind, source_id, entry_id, scheduled_for_ms, enqueued_at_ms, worker_job_id, decision, reason)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            source_kind,
            source_id,
            entry_id,
            scheduled_for_ms,
            enqueued_at_ms,
            worker_job_id,
            format!("{decision:?}"),
            reason
        ],
    ) {
        Ok(_) => Ok(true),
        Err(SqlError::SqliteFailure(error, _))
            if error.code == rusqlite::ErrorCode::ConstraintViolation =>
        {
            Ok(false)
        }
        Err(error) => Err(io::Error::other(error)),
    }
}

fn append_tick_receipt(
    report: &CronSchedulerRunOnceReport,
    now_ms: i64,
    dry_run: bool,
) -> io::Result<()> {
    let receipt = CronSchedulerTickReceipt {
        schema: CRON_SCHEDULER_TICK_SCHEMA,
        status: report.status,
        source_home: report.source_home.clone(),
        source_workspace: report.source_workspace.clone(),
        now_ms,
        config_file: report.config_file.clone(),
        dry_run,
        summary: report.summary.clone(),
        warnings: report.warnings.clone(),
    };
    append_jsonl_value(&report.receipts_file, &receipt)
}

fn write_loop_last_report(
    report: CronSchedulerRunOnceReport,
) -> io::Result<CronSchedulerRunOnceReport> {
    write_json_atomic(&report.loop_last_file, &report)?;
    Ok(report)
}

fn acquire_scheduler_lock(scheduler_dir: &Path, now_ms: i64) -> io::Result<Option<SchedulerLock>> {
    let lock_file = scheduler_dir.join("scheduler.lock");
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    match options.open(&lock_file) {
        Ok(mut file) => {
            writeln!(file, "{now_ms}")?;
            Ok(Some(SchedulerLock {
                path: lock_file,
                _file: file,
            }))
        }
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::AlreadyExists
                    | io::ErrorKind::PermissionDenied
                    | io::ErrorKind::WouldBlock
            ) =>
        {
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

fn native_due_slot(schedule: &NativeCronSchedule, now_ms: i64) -> Result<Option<i64>, String> {
    match schedule {
        NativeCronSchedule::At {
            epoch_ms: Some(at_ms),
            ..
        } if *at_ms <= now_ms => Ok(Some(*at_ms)),
        NativeCronSchedule::At {
            epoch_ms: Some(_), ..
        } => Ok(None),
        NativeCronSchedule::At { epoch_ms: None, .. } => {
            Err("at schedule has no epoch milliseconds".to_string())
        }
        NativeCronSchedule::Cron { expression } => cron_expression_due_slot(expression, now_ms),
        NativeCronSchedule::Unknown { summary } => {
            Err(format!("unsupported native cron schedule: {summary}"))
        }
    }
}

fn deterministic_due_slot(
    schedule: &DeterministicCronSchedule,
    now_ms: i64,
) -> Result<Option<i64>, String> {
    match schedule {
        DeterministicCronSchedule::Cron { expression } => {
            cron_expression_due_slot(expression, now_ms)
        }
        DeterministicCronSchedule::Macro { name } => macro_due_slot(name, now_ms),
        DeterministicCronSchedule::Unsupported { summary } => Err(format!(
            "unsupported deterministic cron schedule: {summary}"
        )),
    }
}

fn cron_expression_due_slot(expression: &str, now_ms: i64) -> Result<Option<i64>, String> {
    let fields = expression.split_whitespace().collect::<Vec<_>>();
    if fields.len() != 5 {
        return Err(format!(
            "cron expression `{expression}` must have five fields"
        ));
    }
    let minute_slot = floor_to_minute(now_ms);
    let minute = ((minute_slot / 60_000) % 60) as i64;
    let hour = ((minute_slot / 3_600_000) % 24) as i64;
    let days = minute_slot / 86_400_000;
    let dow = (days + 4).rem_euclid(7);
    if !matches_cron_field(fields[0], minute, 0, 59)? {
        return Ok(None);
    }
    if !matches_cron_field(fields[1], hour, 0, 23)? {
        return Ok(None);
    }
    if !matches_restricted_day_field(fields[2], "day-of-month")? {
        return Ok(None);
    }
    if !matches_restricted_day_field(fields[3], "month")? {
        return Ok(None);
    }
    if !matches_cron_field(fields[4], dow, 0, 6)? {
        return Ok(None);
    }
    Ok(Some(minute_slot))
}

fn macro_due_slot(name: &str, now_ms: i64) -> Result<Option<i64>, String> {
    let minute_slot = floor_to_minute(now_ms);
    let minute = ((minute_slot / 60_000) % 60) as i64;
    let hour = ((minute_slot / 3_600_000) % 24) as i64;
    let days = minute_slot / 86_400_000;
    let dow = (days + 4).rem_euclid(7);
    match name {
        "@hourly" => Ok((minute == 0).then_some(minute_slot)),
        "@daily" | "@midnight" => Ok((minute == 0 && hour == 0).then_some(minute_slot)),
        "@weekly" => Ok((minute == 0 && hour == 0 && dow == 0).then_some(minute_slot)),
        other => Err(format!("unsupported cron macro `{other}`")),
    }
}

fn matches_restricted_day_field(field: &str, label: &str) -> Result<bool, String> {
    match field {
        "*" | "?" => Ok(true),
        other => Err(format!(
            "cron {label} field `{other}` is not yet supported by the scheduler"
        )),
    }
}

fn matches_cron_field(field: &str, value: i64, min: i64, max: i64) -> Result<bool, String> {
    if field == "*" || field == "?" {
        return Ok(true);
    }
    for part in field.split(',') {
        if matches_cron_part(part, value, min, max)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn matches_cron_part(part: &str, value: i64, min: i64, max: i64) -> Result<bool, String> {
    if let Some(step) = part.strip_prefix("*/") {
        let step = parse_range_value(step, min, max)?;
        return Ok(step > 0 && (value - min) % step == 0);
    }
    let (range_part, step) = match part.split_once('/') {
        Some((range, step)) => (range, Some(parse_range_value(step, 1, max)?)),
        None => (part, None),
    };
    if let Some((start, end)) = range_part.split_once('-') {
        let start = parse_range_value(start, min, max)?;
        let end = parse_range_value(end, min, max)?;
        if value < start || value > end {
            return Ok(false);
        }
        return Ok(step.is_none_or(|step| step > 0 && (value - start) % step == 0));
    }
    let exact = parse_range_value(range_part, min, max)?;
    Ok(value == exact)
}

fn parse_range_value(value: &str, min: i64, max: i64) -> Result<i64, String> {
    let parsed = value
        .parse::<i64>()
        .map_err(|_| format!("cron field value `{value}` is not an integer"))?;
    if parsed < min || parsed > max {
        return Err(format!(
            "cron field value `{value}` outside supported range {min}-{max}"
        ));
    }
    Ok(parsed)
}

fn floor_to_minute(now_ms: i64) -> i64 {
    now_ms.div_euclid(60_000) * 60_000
}

fn scheduler_idempotency_key(
    source_kind: &str,
    source_id: &str,
    entry_id: &str,
    scheduled_for_ms: i64,
) -> String {
    format!("cron-scheduler:{source_kind}:{source_id}:{entry_id}:{scheduled_for_ms}")
}

fn stable_source_id(kind: &str, path: &Path) -> String {
    format!(
        "{}:{}",
        normalize_key_part(kind),
        fnv1a_64_hex(&path.display().to_string())
    )
}

fn normalize_key_part(value: &str) -> String {
    let mut normalized = String::new();
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
            normalized.push(ch.to_ascii_lowercase());
        } else {
            normalized.push('_');
        }
    }
    if normalized.is_empty() {
        "unknown".to_string()
    } else {
        normalized
    }
}

fn fnv1a_64_hex(value: &str) -> String {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

fn default_interval_ms() -> i64 {
    DEFAULT_INTERVAL_MS
}

fn default_max_catchup_per_tick() -> usize {
    DEFAULT_MAX_CATCHUP_PER_TICK
}

fn default_max_enqueue_per_tick() -> usize {
    DEFAULT_MAX_ENQUEUE_PER_TICK
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn cron_expression_matches_minute_slots() {
        let five_minutes = 5 * 60_000;
        assert_eq!(
            cron_expression_due_slot("*/5 * * * *", five_minutes + 999)
                .unwrap()
                .unwrap(),
            five_minutes
        );
        assert!(
            cron_expression_due_slot("*/5 * * * *", 6 * 60_000)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn run_once_enqueues_native_due_at_once_and_dedupes() {
        let root = temp_root("run_once_enqueues_native_due_at_once_and_dedupes");
        let source = write_source(&root);
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();

        let options = CronSchedulerRunOnceOptions {
            harness_home: harness_home.clone(),
            source,
            runtime_workspace: None,
            now_ms: 10_000,
            dry_run: false,
            enabled_override: Some(true),
            native_enabled_override: Some(true),
            deterministic_enabled_override: Some(false),
            resume_cron_override: Some(true),
            include_registered_cron_override: Some(false),
            allow_deterministic_run_override: None,
            execute_shell_override: None,
            max_catchup_per_tick_override: None,
            max_enqueue_per_tick_override: None,
        };

        let first = run_cron_scheduler_once(options.clone()).unwrap();
        assert_eq!(first.status, CronSchedulerTickStatus::Completed);
        assert_eq!(first.summary.enqueued, 1);
        assert_eq!(
            first.decisions[0].decision,
            CronSchedulerJobDecisionStatus::Enqueued
        );

        let second = run_cron_scheduler_once(options).unwrap();
        assert_eq!(second.summary.skipped_duplicate, 1);

        let _ = fs::remove_dir_all(root);
    }

    fn write_source(root: &Path) -> AgentSource {
        let home = root.join(".openclaw");
        let workspace = home.join("workspace");
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(home.join("agents").join("main").join("sessions")).unwrap();
        fs::create_dir_all(home.join("cron")).unwrap();
        fs::write(workspace.join("AGENTS.md"), "# Agent").unwrap();
        fs::write(
            home.join("openclaw.json"),
            r#"{
              "agents": { "list": [{ "id": "main", "enabled": true }] },
              "models": { "providers": { "openai": {} } }
            }"#,
        )
        .unwrap();
        fs::write(
            home.join("cron").join("jobs.json"),
            r#"{
              "jobs": [
                {
                  "id": "due",
                  "enabled": true,
                  "agentId": "main",
                  "schedule": { "kind": "at", "epochMs": 1000 },
                  "messageText": "run due job"
                }
              ]
            }"#,
        )
        .unwrap();
        AgentSource::with_workspace(home, workspace)
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-cron-scheduler-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
