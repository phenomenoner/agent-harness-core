use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use rusqlite::{Connection, Error as SqlError, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    AgentSource, CronRunAdmitOptions, CronRunStatus, DeterministicCronPlanAction,
    DeterministicCronPlanEntry, DeterministicCronPlanInput, DeterministicCronSchedule,
    NativeCronPlanAction, NativeCronPlanEntry, NativeCronPlanInput, NativeCronSchedule,
    WorkerEnqueueOptions, WorkerEnqueueReport, WorkerJobKind, admit_cron_run, append_harness_log,
    append_jsonl_value, config::harness_config_candidates, cron_run_active_count_for_agent,
    cron_run_active_count_for_job, cron_run_id, cron_run_is_quarantined, current_log_time_ms,
    enqueue_worker_job, get_cron_run_by_slot, load_agent_registry, load_deterministic_cron_store,
    load_native_cron_store, mark_cron_run_worker_enqueued, plan_deterministic_cron,
    plan_native_cron, write_json_atomic,
};

const CRON_SCHEDULER_RUN_ONCE_SCHEMA: &str = "agent-harness.cron-scheduler.run-once.v1";
const CRON_SCHEDULER_LINT_SCHEMA: &str = "agent-harness.cron-scheduler.lint.v1";
const CRON_SCHEDULER_TICK_SCHEMA: &str = "agent-harness.cron-scheduler.tick.v1";
const CRON_SCHEDULER_JOB_DECISION_SCHEMA: &str = "agent-harness.cron-scheduler.job-decision.v1";
const DEFAULT_INTERVAL_MS: i64 = 60_000;
const DEFAULT_MAX_CATCHUP_PER_TICK: usize = 10;
const DEFAULT_MAX_ENQUEUE_PER_TICK: usize = 20;
const DEFAULT_WORKER_TIMEOUT_MS: u64 = 300_000;
const DEFAULT_WATCHDOG_TIMEOUT_MS: u64 = 900_000;
const MIN_DETERMINISTIC_TIMEOUT_MS: u64 = 1_000;
const MAX_DETERMINISTIC_TIMEOUT_MS: u64 = 86_400_000;
const MAX_DETERMINISTIC_ATTEMPTS: i64 = 10;
const SCHEDULER_LOCK_STALE_MS: i64 = 30_000;
const CRON_RECEIPTS_MAX_BYTES: u64 = 16 * 1024 * 1024;
const CRON_RECEIPTS_MAX_ARCHIVES: usize = 5;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CronSchedulerLintStatus {
    Pass,
    Warn,
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CronSchedulerLintSeverity {
    Info,
    Warn,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CronSchedulerLintFinding {
    pub severity: CronSchedulerLintSeverity,
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entry_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proposed_action: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CronSchedulerLintSummary {
    pub findings: usize,
    pub errors: usize,
    pub warnings: usize,
    pub native_entries: usize,
    pub deterministic_entries: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CronSchedulerLintReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub source_home: PathBuf,
    pub source_workspace: PathBuf,
    pub status: CronSchedulerLintStatus,
    pub config_file: Option<PathBuf>,
    pub config: CronSchedulerConfig,
    pub findings: Vec<CronSchedulerLintFinding>,
    pub summary: CronSchedulerLintSummary,
    pub warnings: Vec<String>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catch_up_decision: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub missed_slots: Vec<i64>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct CronSchedulerCatchUpDecision {
    decision: String,
    missed_slots: Vec<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DeterministicCatchUpPlan {
    Enqueue {
        scheduled_for_ms: i64,
        decision: String,
        missed_slots: Vec<i64>,
    },
    Suppress {
        decision: String,
        missed_slots: Vec<i64>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DeterministicExecutionPolicy {
    timeout_ms: u64,
    max_attempts: i64,
}

impl Default for DeterministicExecutionPolicy {
    fn default() -> Self {
        Self {
            timeout_ms: DEFAULT_WORKER_TIMEOUT_MS,
            max_attempts: 3,
        }
    }
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
    #[serde(default = "default_cron_max_active_runs_per_job")]
    pub max_active_runs_per_job: usize,
    #[serde(default = "default_cron_max_active_runs_per_agent")]
    pub max_active_runs_per_agent: usize,
    #[serde(default = "default_cron_max_queued_per_agent")]
    pub max_queued_per_agent: usize,
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
            max_active_runs_per_job: default_cron_max_active_runs_per_job(),
            max_active_runs_per_agent: default_cron_max_active_runs_per_agent(),
            max_queued_per_agent: default_cron_max_queued_per_agent(),
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
    file: Option<fs::File>,
}

impl Drop for SchedulerLock {
    fn drop(&mut self) {
        let _ = self.file.take();
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
        append_cron_scheduler_receipts(&report, options.now_ms, options.dry_run)?;
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
        append_cron_scheduler_receipts(&report, options.now_ms, options.dry_run)?;
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

    append_cron_scheduler_receipts(&report, options.now_ms, options.dry_run)?;
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

pub fn lint_cron_scheduler(
    options: CronSchedulerRunOnceOptions,
) -> io::Result<CronSchedulerLintReport> {
    let mut warnings = Vec::new();
    let (mut config, config_file) =
        load_cron_scheduler_config(&options.harness_home, &mut warnings)?;
    apply_overrides(&mut config, &options);

    let mut findings = Vec::new();
    if config.enabled {
        if config.interval_ms < 10_000 {
            push_lint_finding(
                &mut findings,
                CronSchedulerLintSeverity::Error,
                "interval-too-short",
                format!(
                    "cronScheduler.intervalMs={} is below the 10000 ms runtime guard",
                    config.interval_ms
                ),
                None,
                None,
            );
        } else if config.interval_ms < DEFAULT_INTERVAL_MS {
            push_lint_finding(
                &mut findings,
                CronSchedulerLintSeverity::Warn,
                "interval-below-default",
                format!(
                    "cronScheduler.intervalMs={} is below the default {} ms",
                    config.interval_ms, DEFAULT_INTERVAL_MS
                ),
                None,
                None,
            );
        }
    }
    if config.max_catchup_per_tick > 50 {
        push_lint_finding(
            &mut findings,
            CronSchedulerLintSeverity::Warn,
            "max-catchup-high",
            format!(
                "cronScheduler.maxCatchupPerTick={} can create bursty catch-up work",
                config.max_catchup_per_tick
            ),
            None,
            None,
        );
    }
    if config.max_enqueue_per_tick > 100 {
        push_lint_finding(
            &mut findings,
            CronSchedulerLintSeverity::Warn,
            "max-enqueue-high",
            format!(
                "cronScheduler.maxEnqueuePerTick={} can flood the worker/runtime queues",
                config.max_enqueue_per_tick
            ),
            None,
            None,
        );
    }

    let mut native_entries = 0usize;
    let mut deterministic_entries = 0usize;
    if config.native_cron.enabled {
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
        native_entries = plan.entries.len();
        for warning in plan.warnings {
            push_lint_finding(
                &mut findings,
                CronSchedulerLintSeverity::Warn,
                "native-plan-warning",
                warning,
                Some("native-cron"),
                None,
            );
        }
        for entry in plan.entries {
            match entry.action {
                NativeCronPlanAction::MissingAgent => push_lint_finding_with_agent(
                    &mut findings,
                    CronSchedulerLintSeverity::Error,
                    "native-missing-agent",
                    entry.reason.clone(),
                    Some("native-cron"),
                    Some(entry.job_id.clone()),
                    entry.agent_id.clone(),
                ),
                NativeCronPlanAction::UnsupportedSchedule => push_lint_finding_with_agent(
                    &mut findings,
                    CronSchedulerLintSeverity::Error,
                    "native-unsupported-schedule",
                    entry.reason.clone(),
                    Some("native-cron"),
                    Some(entry.job_id.clone()),
                    entry.agent_id.clone(),
                ),
                NativeCronPlanAction::EnqueueAgentTurn | NativeCronPlanAction::CronRegistered => {
                    if entry.agent_id.as_deref() == Some("main") {
                        push_lint_finding_with_agent(
                            &mut findings,
                            CronSchedulerLintSeverity::Warn,
                            "native-main-agent-cron",
                            "native cron targets interactive agent `main`; prefer a cron-specific runtime identity or rely on runtimeClass=cron isolation",
                            Some("native-cron"),
                            Some(entry.job_id.clone()),
                            entry.agent_id.clone(),
                        );
                    }
                    if let Some(policy) = entry.session_policy.as_deref()
                        && !matches!(policy, "one-shot" | "sticky")
                    {
                        push_lint_finding_with_agent(
                            &mut findings,
                            CronSchedulerLintSeverity::Warn,
                            "native-unknown-session-policy",
                            format!(
                                "native cron sessionPolicy `{policy}` is not recognized; scheduler will use one-shot"
                            ),
                            Some("native-cron"),
                            Some(entry.job_id.clone()),
                            entry.agent_id.clone(),
                        );
                    }
                    if entry
                        .session_policy
                        .as_deref()
                        .map(|policy| policy.eq_ignore_ascii_case("sticky"))
                        .unwrap_or(false)
                        && entry
                            .session_key
                            .as_deref()
                            .map(|key| !key.starts_with("cron:"))
                            .unwrap_or(false)
                    {
                        push_lint_finding_with_agent(
                            &mut findings,
                            CronSchedulerLintSeverity::Warn,
                            "native-sticky-session-outside-cron-namespace",
                            "native cron sticky sessionKey is not in the `cron:` namespace; scheduler will rewrite it into an isolated cron session key",
                            Some("native-cron"),
                            Some(entry.job_id.clone()),
                            entry.agent_id.clone(),
                        );
                    }
                    if let Some(reason) = native_cron_entry_runtime_path_blocker(&entry) {
                        push_lint_finding_with_agent(
                            &mut findings,
                            CronSchedulerLintSeverity::Error,
                            "native-runtime-path-mismatch",
                            reason,
                            Some("native-cron"),
                            Some(entry.job_id.clone()),
                            entry.agent_id.clone(),
                        );
                    }
                }
                _ => {}
            }
        }
    }

    if config.deterministic_cron.enabled {
        let store = load_deterministic_cron_store(&options.source)?;
        let plan = plan_deterministic_cron(
            &store,
            DeterministicCronPlanInput {
                allow_deterministic_run: config.deterministic_cron.allow_deterministic_run,
            },
        );
        deterministic_entries = plan.entries.len();
        for warning in plan.warnings {
            push_lint_finding(
                &mut findings,
                CronSchedulerLintSeverity::Warn,
                "deterministic-plan-warning",
                warning,
                Some("deterministic-cron"),
                None,
            );
        }
        for entry in plan.entries {
            let evidence_entry_id =
                deterministic_cron_evidence_entry_id(&options.harness_home, &entry);
            if let Err(error) = deterministic_cron_execution_policy(&options.harness_home, &entry) {
                push_lint_finding(
                    &mut findings,
                    CronSchedulerLintSeverity::Error,
                    "deterministic-invalid-execution-policy",
                    error,
                    Some("deterministic-cron"),
                    Some(evidence_entry_id.clone()),
                );
            }
            if let Err(error) = deterministic_crontab_timezone(&entry) {
                push_lint_finding(
                    &mut findings,
                    CronSchedulerLintSeverity::Error,
                    "deterministic-invalid-timezone",
                    error,
                    Some("deterministic-cron"),
                    Some(evidence_entry_id),
                );
            }
            let (severity, code) = match entry.action {
                DeterministicCronPlanAction::MissingScript => (
                    CronSchedulerLintSeverity::Error,
                    "deterministic-missing-script",
                ),
                DeterministicCronPlanAction::UnsupportedEntry => (
                    CronSchedulerLintSeverity::Error,
                    "deterministic-unsupported-entry",
                ),
                DeterministicCronPlanAction::ExternalCommandReview => (
                    CronSchedulerLintSeverity::Warn,
                    "deterministic-external-command-review",
                ),
                DeterministicCronPlanAction::ShellCompatibilityRequired => (
                    CronSchedulerLintSeverity::Warn,
                    "deterministic-shell-compatibility-required",
                ),
                _ => continue,
            };
            push_lint_finding(
                &mut findings,
                severity,
                code,
                entry.reason.clone(),
                Some("deterministic-cron"),
                Some(entry.entry_id.clone()),
            );
        }
        if config.deterministic_cron.allow_deterministic_run
            && config.deterministic_cron.execute_shell
        {
            push_lint_finding(
                &mut findings,
                CronSchedulerLintSeverity::Warn,
                "deterministic-shell-execution-enabled",
                "deterministic cron shell execution is enabled; use dry-run-shell unless this was explicitly reviewed",
                Some("deterministic-cron"),
                None,
            );
        }
    }

    if config.enabled && !config.native_cron.enabled && !config.deterministic_cron.enabled {
        push_lint_finding(
            &mut findings,
            CronSchedulerLintSeverity::Warn,
            "no-enabled-source-lanes",
            "cronScheduler.enabled is true but no cron source lane is enabled",
            None,
            None,
        );
    }

    let errors = findings
        .iter()
        .filter(|finding| finding.severity == CronSchedulerLintSeverity::Error)
        .count();
    let warnings_count = findings
        .iter()
        .filter(|finding| finding.severity == CronSchedulerLintSeverity::Warn)
        .count();
    let status = if errors > 0 {
        CronSchedulerLintStatus::Error
    } else if warnings_count > 0 {
        CronSchedulerLintStatus::Warn
    } else {
        CronSchedulerLintStatus::Pass
    };
    Ok(CronSchedulerLintReport {
        schema: CRON_SCHEDULER_LINT_SCHEMA,
        harness_home: options.harness_home,
        source_home: options.source.home,
        source_workspace: options.source.workspace,
        status,
        config_file,
        config,
        summary: CronSchedulerLintSummary {
            findings: findings.len(),
            errors,
            warnings: warnings_count,
            native_entries,
            deterministic_entries,
        },
        findings,
        warnings,
    })
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
                if let Some(reason) = native_cron_entry_runtime_path_blocker(entry) {
                    push_decision(
                        report,
                        "native-cron",
                        &source_id,
                        &entry.job_id,
                        options.now_ms,
                        CronSchedulerJobDecisionStatus::SkippedPolicy,
                        &reason,
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
                let retry_pending_run = get_cron_run_by_slot(
                    &options.harness_home,
                    "native-cron",
                    &source_id,
                    &entry.job_id,
                    scheduled_for_ms,
                )?
                .filter(|run| run.status == CronRunStatus::RetryPending && !run.quarantined);
                if retry_pending_run.is_some() {
                    let cleared = delete_watermark(
                        database,
                        "native-cron",
                        &source_id,
                        &entry.job_id,
                        scheduled_for_ms,
                    )?;
                    if cleared > 0 {
                        report.warnings.push(format!(
                            "cleared {cleared} cron scheduler watermark(s) for retry-pending native cron slot `{}`",
                            entry.job_id
                        ));
                    }
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
                if let Some(reason) =
                    cron_run_is_quarantined(&options.harness_home, &agent_id, &entry.job_id)?
                {
                    push_decision(
                        report,
                        "native-cron",
                        &source_id,
                        &entry.job_id,
                        scheduled_for_ms,
                        CronSchedulerJobDecisionStatus::SkippedPolicy,
                        &format!("cron job quarantined: {reason}"),
                        None,
                    );
                    continue;
                }
                let retrying_same_run = retry_pending_run
                    .as_ref()
                    .is_some_and(|run| run.agent_id == agent_id && run.entry_id == entry.job_id);
                let retry_self_discount = if retrying_same_run { 1 } else { 0 };
                let active_job =
                    cron_run_active_count_for_job(&options.harness_home, &agent_id, &entry.job_id)?;
                if active_job.saturating_sub(retry_self_discount) >= config.max_active_runs_per_job
                {
                    push_decision(
                        report,
                        "native-cron",
                        &source_id,
                        &entry.job_id,
                        scheduled_for_ms,
                        CronSchedulerJobDecisionStatus::SkippedPolicy,
                        &format!("cron active run cap reached for job `{}`", entry.job_id),
                        None,
                    );
                    continue;
                }
                let active_agent =
                    cron_run_active_count_for_agent(&options.harness_home, &agent_id)?;
                if active_agent.saturating_sub(retry_self_discount) >= config.max_queued_per_agent {
                    push_decision(
                        report,
                        "native-cron",
                        &source_id,
                        &entry.job_id,
                        scheduled_for_ms,
                        CronSchedulerJobDecisionStatus::SkippedPolicy,
                        &format!("cron queued run cap reached for agent `{agent_id}`"),
                        None,
                    );
                    continue;
                }
                if active_agent.saturating_sub(retry_self_discount)
                    >= config.max_active_runs_per_agent
                {
                    push_decision(
                        report,
                        "native-cron",
                        &source_id,
                        &entry.job_id,
                        scheduled_for_ms,
                        CronSchedulerJobDecisionStatus::SkippedPolicy,
                        &format!("cron active run cap reached for agent `{agent_id}`"),
                        None,
                    );
                    continue;
                }
                let session_policy =
                    normalized_cron_session_policy(entry.session_policy.as_deref());
                let session_key =
                    native_cron_session_key(entry, &agent_id, scheduled_for_ms, session_policy);
                let run_id =
                    cron_run_id("native-cron", &source_id, &entry.job_id, scheduled_for_ms);
                let cron_run = admit_cron_run(CronRunAdmitOptions {
                    harness_home: options.harness_home.clone(),
                    source_kind: "native-cron".to_string(),
                    source_id: source_id.clone(),
                    entry_id: entry.job_id.clone(),
                    agent_id: agent_id.clone(),
                    scheduled_for_ms,
                    runtime_class: "cron".to_string(),
                    session_key: session_key.clone(),
                    session_policy: session_policy.to_string(),
                    max_attempts: 3,
                    now_ms: options.now_ms,
                })?;
                let idempotency_key = scheduler_idempotency_key(
                    "native-cron",
                    &source_id,
                    &entry.job_id,
                    scheduled_for_ms,
                    cron_run.attempt,
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
                    "agentId": &agent_id,
                    "sessionKey": &session_key,
                    "sessionPolicy": session_policy,
                    "runtimeClass": "cron",
                    "origin": "cron-scheduler",
                    "cronRunId": &run_id,
                    "platform": "native-cron",
                    "channelId": &entry.job_id,
                    "userId": "cron-scheduler",
                    "messageText": &message_text,
                    "inboundContext": serde_json::to_string(entry).unwrap_or_default()
                });
                let enqueue = enqueue_worker_job(WorkerEnqueueOptions {
                    harness_home: options.harness_home.clone(),
                    kind: WorkerJobKind::LlmSubagent,
                    lane: Some("cron".to_string()),
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
                    rate_key: Some(format!("cron:{agent_id}")),
                    concurrency_group_key: Some(format!(
                        "{}:{}",
                        normalize_key_part(&master_agent),
                        normalize_key_part(&master_session)
                    )),
                    now_ms: options.now_ms,
                })?;
                mark_cron_run_worker_enqueued(
                    &options.harness_home,
                    &cron_run.run_id,
                    &enqueue.job.job_id,
                    options.now_ms,
                )?;
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
    let previous_tick_ms = database
        .parent()
        .and_then(|scheduler_dir| last_scheduler_tick_ms(scheduler_dir).ok().flatten());

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
        let evidence_entry_id = deterministic_cron_evidence_entry_id(&options.harness_home, entry);
        let execution_policy =
            match deterministic_cron_execution_policy(&options.harness_home, entry) {
                Ok(policy) => policy,
                Err(error) => {
                    push_decision(
                        report,
                        "deterministic-cron",
                        &source_id,
                        &evidence_entry_id,
                        options.now_ms,
                        CronSchedulerJobDecisionStatus::Error,
                        &error,
                        None,
                    );
                    continue;
                }
            };
        let timezone = match deterministic_crontab_timezone(entry) {
            Ok(timezone) => timezone,
            Err(error) => {
                push_decision(
                    report,
                    "deterministic-cron",
                    &source_id,
                    &evidence_entry_id,
                    options.now_ms,
                    CronSchedulerJobDecisionStatus::Error,
                    &error,
                    None,
                );
                continue;
            }
        };
        let catch_up_policy =
            deterministic_cron_canon_catch_up_policy(&options.harness_home, entry);
        let (scheduled_for_ms, catch_up) =
            match deterministic_due_slot(&entry.schedule, timezone.as_deref(), options.now_ms) {
                Ok(Some(value)) => (value, None),
                Ok(None) => {
                    match deterministic_catch_up_decision(
                        &entry.schedule,
                        timezone.as_deref(),
                        previous_tick_ms,
                        options.now_ms,
                        config.max_catchup_per_tick,
                        catch_up_policy.as_deref(),
                    )
                    .map_err(io::Error::other)?
                    {
                        Some(DeterministicCatchUpPlan::Enqueue {
                            scheduled_for_ms,
                            decision,
                            missed_slots,
                        }) => (
                            scheduled_for_ms,
                            Some(CronSchedulerCatchUpDecision {
                                decision,
                                missed_slots,
                            }),
                        ),
                        Some(DeterministicCatchUpPlan::Suppress {
                            decision,
                            missed_slots,
                        }) => {
                            push_decision_with_catch_up(
                                report,
                                "deterministic-cron",
                                &source_id,
                                &evidence_entry_id,
                                options.now_ms,
                                CronSchedulerJobDecisionStatus::SkippedPolicy,
                                "catch-up policy suppressed missed deterministic cron slots",
                                None,
                                Some(CronSchedulerCatchUpDecision {
                                    decision,
                                    missed_slots,
                                }),
                            );
                            continue;
                        }
                        None => {
                            push_decision(
                                report,
                                "deterministic-cron",
                                &source_id,
                                &evidence_entry_id,
                                options.now_ms,
                                CronSchedulerJobDecisionStatus::SkippedPolicy,
                                "schedule is not due in the current scheduler slot",
                                None,
                            );
                            continue;
                        }
                    }
                }
                Err(error) => {
                    push_decision(
                        report,
                        "deterministic-cron",
                        &source_id,
                        &evidence_entry_id,
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
            push_decision_with_catch_up(
                report,
                "deterministic-cron",
                &source_id,
                &evidence_entry_id,
                scheduled_for_ms,
                CronSchedulerJobDecisionStatus::SkippedPolicy,
                "dry-run: due deterministic cron command would enqueue",
                None,
                catch_up.clone(),
            );
            continue;
        }
        let retry_pending_run = get_cron_run_by_slot(
            &options.harness_home,
            "deterministic-cron",
            &source_id,
            &evidence_entry_id,
            scheduled_for_ms,
        )?
        .filter(|run| run.status == CronRunStatus::RetryPending && !run.quarantined);
        if retry_pending_run.is_some() {
            let cleared = delete_watermark(
                database,
                "deterministic-cron",
                &source_id,
                &evidence_entry_id,
                scheduled_for_ms,
            )?;
            if cleared > 0 {
                report.warnings.push(format!(
                    "cleared {cleared} cron scheduler watermark(s) for retry-pending deterministic cron slot `{}`",
                    evidence_entry_id
                ));
            }
        }
        if watermark_exists(
            database,
            "deterministic-cron",
            &source_id,
            &evidence_entry_id,
            scheduled_for_ms,
        )? {
            push_decision(
                report,
                "deterministic-cron",
                &source_id,
                &evidence_entry_id,
                scheduled_for_ms,
                CronSchedulerJobDecisionStatus::SkippedDuplicate,
                "watermark already exists for this scheduled slot",
                None,
            );
            continue;
        }
        if evidence_entry_id != entry.entry_id
            && watermark_exists(
                database,
                "deterministic-cron",
                &source_id,
                &entry.entry_id,
                scheduled_for_ms,
            )?
        {
            push_decision(
                report,
                "deterministic-cron",
                &source_id,
                &evidence_entry_id,
                scheduled_for_ms,
                CronSchedulerJobDecisionStatus::SkippedDuplicate,
                "legacy deterministic entry watermark already exists for this scheduled slot",
                None,
            );
            continue;
        }
        let Some(script_path) = entry.script_path.clone() else {
            push_decision(
                report,
                "deterministic-cron",
                &source_id,
                &evidence_entry_id,
                scheduled_for_ms,
                CronSchedulerJobDecisionStatus::Error,
                "deterministic cron entry had no script path",
                None,
            );
            continue;
        };
        let agent_id = "cron-scheduler".to_string();
        let session_policy = "one-shot";
        let session_key = deterministic_cron_session_key(&evidence_entry_id, scheduled_for_ms);
        let run_id = cron_run_id(
            "deterministic-cron",
            &source_id,
            &evidence_entry_id,
            scheduled_for_ms,
        );
        let cron_run = admit_cron_run(CronRunAdmitOptions {
            harness_home: options.harness_home.clone(),
            source_kind: "deterministic-cron".to_string(),
            source_id: source_id.clone(),
            entry_id: evidence_entry_id.clone(),
            agent_id: agent_id.clone(),
            scheduled_for_ms,
            runtime_class: "deterministic-shell".to_string(),
            session_key: session_key.clone(),
            session_policy: session_policy.to_string(),
            max_attempts: execution_policy.max_attempts,
            now_ms: options.now_ms,
        })?;
        let idempotency_key = scheduler_idempotency_key(
            "deterministic-cron",
            &source_id,
            &evidence_entry_id,
            scheduled_for_ms,
            cron_run.attempt,
        );
        let cwd = script_path.parent().unwrap_or(Path::new(".")).to_path_buf();
        let mut occurrence_env = serde_json::Map::from_iter([
            (
                "AGENT_HARNESS_CRON_ENTRY_ID".to_string(),
                Value::String(evidence_entry_id.clone()),
            ),
            (
                "AGENT_HARNESS_CRON_SCHEDULED_FOR_MS".to_string(),
                Value::String(scheduled_for_ms.to_string()),
            ),
        ]);
        if let Some(timezone) = timezone.as_deref() {
            occurrence_env.insert(
                "AGENT_HARNESS_CRON_TIMEZONE".to_string(),
                Value::String(timezone.to_string()),
            );
        }
        let payload = json!({
            "adapter": "cron-scheduler",
            "sourceKind": "deterministic-cron",
            "sourceId": &source_id,
            "entryId": &evidence_entry_id,
            "deterministicEntryId": &entry.entry_id,
            "cronCanonId": if evidence_entry_id != entry.entry_id {
                Some(evidence_entry_id.as_str())
            } else {
                None::<&str>
            },
            "scheduledForMs": scheduled_for_ms,
            "runnerKind": entry.runner_kind,
            "command": entry.command,
            "scriptPath": script_path,
            "cwd": cwd,
            "argv": [],
            "env": occurrence_env,
            "dryRun": !config.deterministic_cron.execute_shell,
            "agentId": &agent_id,
            "sessionKey": &session_key,
            "sessionPolicy": session_policy,
            "runtimeClass": "deterministic-shell",
            "origin": "cron-scheduler",
            "cronRunId": &run_id,
            "catchUpDecision": catch_up.as_ref().map(|value| value.decision.as_str()),
            "missedSlots": catch_up
                .as_ref()
                .map(|value| value.missed_slots.clone())
                .unwrap_or_default(),
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
            max_attempts: execution_policy.max_attempts,
            timeout_ms: Some(execution_policy.timeout_ms),
            cascade_timeout_ms: Some(
                DEFAULT_WATCHDOG_TIMEOUT_MS.max(execution_policy.timeout_ms.saturating_mul(3)),
            ),
            rate_key: None,
            concurrency_group_key: Some(format!(
                "{}:{}",
                normalize_key_part(&master_agent),
                normalize_key_part(&master_session)
            )),
            now_ms: options.now_ms,
        })?;
        mark_cron_run_worker_enqueued(
            &options.harness_home,
            &cron_run.run_id,
            &enqueue.job.job_id,
            options.now_ms,
        )?;
        insert_watermark(
            database,
            "deterministic-cron",
            &source_id,
            &evidence_entry_id,
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
        push_decision_with_catch_up(
            report,
            "deterministic-cron",
            &source_id,
            &evidence_entry_id,
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
            catch_up,
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
    push_decision_with_catch_up(
        report,
        source_kind,
        source_id,
        entry_id,
        scheduled_for_ms,
        decision,
        reason,
        enqueue,
        None,
    );
}

fn push_decision_with_catch_up(
    report: &mut CronSchedulerRunOnceReport,
    source_kind: &str,
    source_id: &str,
    entry_id: &str,
    scheduled_for_ms: i64,
    decision: CronSchedulerJobDecisionStatus,
    reason: &str,
    enqueue: Option<(WorkerEnqueueReport, String)>,
    catch_up: Option<CronSchedulerCatchUpDecision>,
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
        catch_up_decision: catch_up.as_ref().map(|value| value.decision.clone()),
        missed_slots: catch_up.map(|value| value.missed_slots).unwrap_or_default(),
        decision,
        reason: reason.to_string(),
    });
}

fn cron_scheduler_decision_should_persist(decision: &CronSchedulerJobDecision) -> bool {
    !matches!(
        decision.decision,
        CronSchedulerJobDecisionStatus::SkippedPolicy
    ) || decision.reason != "schedule is not due in the current scheduler slot"
}

fn append_cron_scheduler_receipts(
    report: &CronSchedulerRunOnceReport,
    now_ms: i64,
    dry_run: bool,
) -> io::Result<()> {
    rotate_cron_scheduler_receipts_if_needed(&report.receipts_file, now_ms)?;
    for decision in report
        .decisions
        .iter()
        .filter(|decision| cron_scheduler_decision_should_persist(decision))
    {
        append_jsonl_value(&report.receipts_file, decision)?;
    }
    if cron_scheduler_tick_should_persist(report, dry_run) {
        append_tick_receipt(report, now_ms, dry_run)?;
    }
    Ok(())
}

fn cron_scheduler_tick_should_persist(report: &CronSchedulerRunOnceReport, dry_run: bool) -> bool {
    dry_run
        || matches!(report.status, CronSchedulerTickStatus::Error)
        || report.summary.enqueued > 0
        || report.summary.errors > 0
        || report.summary.skipped_held > 0
        || report.summary.due_candidates > 0
}

fn rotate_cron_scheduler_receipts_if_needed(receipts_file: &Path, now_ms: i64) -> io::Result<()> {
    let Ok(metadata) = fs::metadata(receipts_file) else {
        return Ok(());
    };
    if metadata.len() < CRON_RECEIPTS_MAX_BYTES {
        return Ok(());
    }
    let archive = next_cron_receipt_archive_path(receipts_file, now_ms)?;
    fs::rename(receipts_file, &archive)?;
    prune_cron_receipt_archives(
        receipts_file.parent().unwrap_or(Path::new(".")),
        Some(&archive),
    )
}

fn next_cron_receipt_archive_path(receipts_file: &Path, now_ms: i64) -> io::Result<PathBuf> {
    for sequence in 0..10_000_u32 {
        let suffix = (sequence > 0)
            .then(|| format!("-{sequence}"))
            .unwrap_or_default();
        let candidate = receipts_file.with_file_name(format!("receipts-{now_ms}{suffix}.jsonl"));
        if !candidate.exists() {
            return Ok(candidate);
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        format!(
            "could not allocate a unique cron scheduler receipt archive for timestamp {now_ms}"
        ),
    ))
}

fn prune_cron_receipt_archives(
    state_dir: &Path,
    protected_archive: Option<&Path>,
) -> io::Result<()> {
    let mut archives = Vec::new();
    for entry in fs::read_dir(state_dir)? {
        let entry = entry?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if name.starts_with("receipts-") && name.ends_with(".jsonl") {
            archives.push((path, entry.metadata()?.modified().ok()));
        }
    }
    archives.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| right.0.cmp(&left.0)));
    let protected_count = usize::from(
        protected_archive
            .is_some_and(|protected| archives.iter().any(|(path, _)| path == protected)),
    );
    let keep_unprotected = CRON_RECEIPTS_MAX_ARCHIVES.saturating_sub(protected_count);
    let mut retained_unprotected = 0;
    for (path, _) in archives {
        if protected_archive.is_some_and(|protected| path == protected) {
            continue;
        }
        if retained_unprotected < keep_unprotected {
            retained_unprotected += 1;
            continue;
        }
        fs::remove_file(path)?;
    }
    Ok(())
}

fn push_lint_finding(
    findings: &mut Vec<CronSchedulerLintFinding>,
    severity: CronSchedulerLintSeverity,
    code: impl Into<String>,
    message: impl Into<String>,
    source_kind: Option<&str>,
    entry_id: Option<String>,
) {
    push_lint_finding_with_agent(
        findings,
        severity,
        code,
        message,
        source_kind,
        entry_id,
        None,
    );
}

fn push_lint_finding_with_agent(
    findings: &mut Vec<CronSchedulerLintFinding>,
    severity: CronSchedulerLintSeverity,
    code: impl Into<String>,
    message: impl Into<String>,
    source_kind: Option<&str>,
    entry_id: Option<String>,
    agent_id: Option<String>,
) {
    let code = code.into();
    let proposed_action = cron_lint_proposed_action(&code).map(ToString::to_string);
    findings.push(CronSchedulerLintFinding {
        severity,
        code,
        message: message.into(),
        source_kind: source_kind.map(ToString::to_string),
        entry_id,
        agent_id,
        proposed_action,
    });
}

fn cron_lint_proposed_action(code: &str) -> Option<&'static str> {
    match code {
        "interval-too-short"
        | "interval-below-default"
        | "max-catchup-high"
        | "max-enqueue-high"
        | "no-enabled-source-lanes" => Some("review-config"),
        "native-main-agent-cron" => Some("review-owner"),
        "native-runtime-path-mismatch" => Some("rewrite-path-or-native-port"),
        "native-missing-agent" => Some("assign-agent-or-disable"),
        "native-unsupported-schedule" => Some("rewrite-schedule-or-disable"),
        "native-unknown-session-policy" => Some("rewrite-session-policy"),
        "native-sticky-session-outside-cron-namespace" => Some("accept-cron-namespace-rewrite"),
        "native-plan-warning" => Some("review-native-plan-warning"),
        "deterministic-shell-compatibility-required" => Some("native-port-or-wrapper-policy"),
        "deterministic-external-command-review" => Some("operator-review"),
        "deterministic-missing-script" => Some("restore-script-or-disable"),
        "deterministic-unsupported-entry" => Some("rewrite-entry-or-disable"),
        "deterministic-shell-execution-enabled" => Some("review-shell-execution-policy"),
        "deterministic-plan-warning" => Some("review-deterministic-plan-warning"),
        _ => None,
    }
}

fn native_cron_entry_runtime_path_blocker(entry: &NativeCronPlanEntry) -> Option<String> {
    if !cfg!(windows) {
        return None;
    }
    let text = entry.message_text.as_deref()?;
    let lowered = text.to_ascii_lowercase();
    let has_linux_absolute_path = lowered.contains("/root/")
        || lowered.contains("/home/")
        || lowered.contains("/mnt/")
        || lowered.contains("/var/")
        || lowered.contains("/etc/");
    if has_linux_absolute_path {
        return Some(
            "native cron message references Linux-style absolute paths while the live runtime is Windows; review or rewrite the job before enqueue"
                .to_string(),
        );
    }
    None
}

fn normalized_cron_session_policy(value: Option<&str>) -> &'static str {
    match value.unwrap_or("one-shot").to_ascii_lowercase().as_str() {
        "sticky" => "sticky",
        _ => "one-shot",
    }
}

fn native_cron_session_key(
    entry: &NativeCronPlanEntry,
    agent_id: &str,
    scheduled_for_ms: i64,
    session_policy: &str,
) -> String {
    if session_policy == "sticky" {
        let suffix = entry
            .session_key
            .as_deref()
            .map(normalize_key_part)
            .unwrap_or_else(|| "sticky".to_string());
        return format!(
            "cron:{}:{}:sticky:{}",
            normalize_key_part(agent_id),
            normalize_key_part(&entry.job_id),
            suffix
        );
    }
    format!(
        "cron:{}:{}:{}",
        normalize_key_part(agent_id),
        normalize_key_part(&entry.job_id),
        scheduled_for_ms
    )
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
            if config.max_active_runs_per_job == 0 {
                warnings
                    .push("cronScheduler.maxActiveRunsPerJob was zero; using default".to_string());
                config.max_active_runs_per_job = default_cron_max_active_runs_per_job();
            }
            if config.max_active_runs_per_agent == 0 {
                warnings.push(
                    "cronScheduler.maxActiveRunsPerAgent was zero; using default".to_string(),
                );
                config.max_active_runs_per_agent = default_cron_max_active_runs_per_agent();
            }
            if config.max_queued_per_agent == 0 {
                warnings
                    .push("cronScheduler.maxQueuedPerAgent was zero; using default".to_string());
                config.max_queued_per_agent = default_cron_max_queued_per_agent();
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

fn delete_watermark(
    database: &Path,
    source_kind: &str,
    source_id: &str,
    entry_id: &str,
    scheduled_for_ms: i64,
) -> io::Result<usize> {
    let conn = Connection::open(database).map_err(io::Error::other)?;
    conn.execute(
        "DELETE FROM cron_scheduler_watermarks
         WHERE source_kind = ?1 AND source_id = ?2 AND entry_id = ?3 AND scheduled_for_ms = ?4",
        params![source_kind, source_id, entry_id, scheduled_for_ms],
    )
    .map_err(io::Error::other)
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
                file: Some(file),
            }))
        }
        Err(error) if scheduler_lock_is_busy(&error) => {
            if scheduler_lock_is_stale(&lock_file, now_ms) {
                match fs::remove_file(&lock_file) {
                    Ok(()) => return acquire_scheduler_lock(scheduler_dir, now_ms),
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {
                        return acquire_scheduler_lock(scheduler_dir, now_ms);
                    }
                    Err(error) if scheduler_lock_is_busy(&error) => return Ok(None),
                    Err(error) => return Err(error),
                }
            }
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

fn scheduler_lock_is_busy(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::AlreadyExists | io::ErrorKind::PermissionDenied | io::ErrorKind::WouldBlock
    ) || {
        #[cfg(windows)]
        {
            error.raw_os_error() == Some(32)
        }
        #[cfg(not(windows))]
        {
            let _ = error;
            false
        }
    }
}

fn scheduler_lock_is_stale(lock_file: &Path, now_ms: i64) -> bool {
    if let Ok(text) = fs::read_to_string(lock_file)
        && let Ok(created_at_ms) = text.trim().parse::<i64>()
    {
        return now_ms.saturating_sub(created_at_ms) > SCHEDULER_LOCK_STALE_MS;
    }
    lock_file
        .metadata()
        .and_then(|metadata| metadata.modified())
        .ok()
        .and_then(|modified| std::time::SystemTime::now().duration_since(modified).ok())
        .is_some_and(|age| age.as_millis() > u128::from(SCHEDULER_LOCK_STALE_MS as u64))
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
        NativeCronSchedule::Cron {
            expression,
            timezone,
        } => cron_expression_due_slot_with_timezone(expression, timezone.as_deref(), now_ms),
        NativeCronSchedule::Unknown { summary } => {
            Err(format!("unsupported native cron schedule: {summary}"))
        }
    }
}

fn deterministic_due_slot(
    schedule: &DeterministicCronSchedule,
    timezone: Option<&str>,
    now_ms: i64,
) -> Result<Option<i64>, String> {
    match schedule {
        DeterministicCronSchedule::Cron { expression } => {
            cron_expression_due_slot_with_timezone(expression, timezone, now_ms)
        }
        DeterministicCronSchedule::Macro { name } => {
            macro_due_slot_with_timezone(name, timezone, now_ms)
        }
        DeterministicCronSchedule::Unsupported { summary } => Err(format!(
            "unsupported deterministic cron schedule: {summary}"
        )),
    }
}

fn deterministic_cron_evidence_entry_id(
    harness_home: &Path,
    entry: &DeterministicCronPlanEntry,
) -> String {
    deterministic_cron_canon_id(harness_home, entry).unwrap_or_else(|| entry.entry_id.clone())
}

fn deterministic_cron_canon_id(
    harness_home: &Path,
    entry: &DeterministicCronPlanEntry,
) -> Option<String> {
    deterministic_cron_canon_field(harness_home, entry, "id")
}

fn deterministic_cron_canon_catch_up_policy(
    harness_home: &Path,
    entry: &DeterministicCronPlanEntry,
) -> Option<String> {
    deterministic_cron_canon_field(harness_home, entry, "catchUpPolicy")
}

fn deterministic_cron_canon_field(
    harness_home: &Path,
    entry: &DeterministicCronPlanEntry,
    field: &str,
) -> Option<String> {
    deterministic_cron_canon_entry(harness_home, entry)?
        .get(field)
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

fn deterministic_cron_canon_entry(
    harness_home: &Path,
    entry: &DeterministicCronPlanEntry,
) -> Option<Value> {
    let canon_path = harness_home
        .join("workspace")
        .join("docs")
        .join("ops")
        .join("cron-canon.json");
    let canon: Value = serde_json::from_str(&fs::read_to_string(canon_path).ok()?).ok()?;
    let active_crons = canon.get("activeCrons")?.as_array()?;
    active_crons.iter().find_map(|cron| {
        if cron
            .get("kind")
            .and_then(Value::as_str)
            .is_some_and(|kind| kind != "deterministic-crontab")
        {
            return None;
        }
        if cron.get("enabled").and_then(Value::as_bool) == Some(false) {
            return None;
        }
        if !deterministic_cron_canon_matches_entry(harness_home, cron, entry) {
            return None;
        }
        Some(cron.clone())
    })
}

fn deterministic_cron_execution_policy(
    harness_home: &Path,
    entry: &DeterministicCronPlanEntry,
) -> Result<DeterministicExecutionPolicy, String> {
    let Some(canon) = deterministic_cron_canon_entry(harness_home, entry) else {
        return Ok(DeterministicExecutionPolicy::default());
    };
    let mut policy = DeterministicExecutionPolicy::default();
    if let Some(value) = canon.get("timeoutMs") {
        let timeout_ms = value.as_u64().ok_or_else(|| {
            format!(
                "deterministic cron `{}` timeoutMs must be an integer",
                entry.entry_id
            )
        })?;
        if !(MIN_DETERMINISTIC_TIMEOUT_MS..=MAX_DETERMINISTIC_TIMEOUT_MS).contains(&timeout_ms) {
            return Err(format!(
                "deterministic cron `{}` timeoutMs must be between {} and {}",
                entry.entry_id, MIN_DETERMINISTIC_TIMEOUT_MS, MAX_DETERMINISTIC_TIMEOUT_MS
            ));
        }
        policy.timeout_ms = timeout_ms;
    }
    if let Some(value) = canon.get("maxAttempts") {
        let max_attempts = value.as_i64().ok_or_else(|| {
            format!(
                "deterministic cron `{}` maxAttempts must be an integer",
                entry.entry_id
            )
        })?;
        if !(1..=MAX_DETERMINISTIC_ATTEMPTS).contains(&max_attempts) {
            return Err(format!(
                "deterministic cron `{}` maxAttempts must be between 1 and {}",
                entry.entry_id, MAX_DETERMINISTIC_ATTEMPTS
            ));
        }
        policy.max_attempts = max_attempts;
    }
    Ok(policy)
}

fn deterministic_crontab_timezone(
    entry: &DeterministicCronPlanEntry,
) -> Result<Option<String>, String> {
    let text = fs::read_to_string(&entry.crontab_file).map_err(|error| {
        format!(
            "could not read deterministic crontab {}: {error}",
            entry.crontab_file.display()
        )
    })?;
    let mut timezone = None;
    for line in text.lines().take(entry.line_number.saturating_sub(1)) {
        let trimmed = line.trim();
        let Some((key, value)) = trimmed.split_once('=') else {
            continue;
        };
        if matches!(key.trim(), "TZ" | "CRON_TZ") {
            let value = value
                .trim()
                .trim_matches('"')
                .trim_matches('\'')
                .to_string();
            timezone_offset_minutes(Some(&value)).map_err(|error| {
                format!(
                    "deterministic crontab {}:{} has {error}",
                    entry.crontab_file.display(),
                    entry.line_number
                )
            })?;
            timezone = Some(value);
        }
    }
    Ok(timezone)
}

fn deterministic_cron_canon_matches_entry(
    harness_home: &Path,
    cron: &Value,
    entry: &DeterministicCronPlanEntry,
) -> bool {
    let Some(script_path) = entry.script_path.as_deref() else {
        return false;
    };
    let Some(script) = cron.get("script").and_then(Value::as_str) else {
        return false;
    };
    if !path_matches_canon_text(harness_home, script_path, script) {
        return false;
    }
    if let Some(source_path) = cron.get("sourcePath").and_then(Value::as_str)
        && !path_matches_canon_text(harness_home, &entry.crontab_file, source_path)
    {
        return false;
    }
    if let Some(schedule) = cron.get("schedule").and_then(Value::as_str)
        && deterministic_schedule_text(&entry.schedule).as_deref() != Some(schedule)
    {
        return false;
    }
    true
}

fn deterministic_schedule_text(schedule: &DeterministicCronSchedule) -> Option<String> {
    match schedule {
        DeterministicCronSchedule::Cron { expression } => Some(expression.clone()),
        DeterministicCronSchedule::Macro { name } => Some(name.clone()),
        DeterministicCronSchedule::Unsupported { .. } => None,
    }
}

fn path_matches_canon_text(harness_home: &Path, actual: &Path, canon_text: &str) -> bool {
    let actual = normalize_path_text(&actual.display().to_string());
    let canon_text = normalize_path_text(canon_text);
    let resolved = if Path::new(canon_text.as_str()).is_absolute() {
        PathBuf::from(canon_text.as_str())
    } else {
        harness_home.join(canon_text.replace('/', std::path::MAIN_SEPARATOR_STR))
    };
    let resolved = normalize_path_text(&resolved.display().to_string());
    actual == resolved || actual.ends_with(&format!("/{canon_text}"))
}

fn normalize_path_text(value: &str) -> String {
    value.replace('\\', "/").to_ascii_lowercase()
}

fn deterministic_cron_session_key(entry_id: &str, scheduled_for_ms: i64) -> String {
    format!(
        "cron:deterministic:{}:{}",
        normalize_key_part(entry_id),
        scheduled_for_ms
    )
}

fn deterministic_catch_up_decision(
    schedule: &DeterministicCronSchedule,
    timezone: Option<&str>,
    previous_tick_ms: Option<i64>,
    now_ms: i64,
    max_catchup_per_tick: usize,
    policy: Option<&str>,
) -> Result<Option<DeterministicCatchUpPlan>, String> {
    let Some(previous_tick_ms) = previous_tick_ms else {
        return Ok(None);
    };
    let Some(policy) = policy else {
        return Ok(None);
    };
    let missed_slots = missed_deterministic_due_slots(
        schedule,
        timezone,
        previous_tick_ms,
        now_ms,
        max_catchup_per_tick.max(1),
    )?;
    if missed_slots.is_empty() {
        return Ok(None);
    }
    match policy {
        "run-late-once" => Ok(Some(DeterministicCatchUpPlan::Enqueue {
            scheduled_for_ms: missed_slots[missed_slots.len() - 1],
            decision: "run-late-once".to_string(),
            missed_slots,
        })),
        "run-immediately-on-restart" => Ok(Some(DeterministicCatchUpPlan::Enqueue {
            scheduled_for_ms: floor_to_minute(now_ms),
            decision: "run-immediately-on-restart".to_string(),
            missed_slots,
        })),
        "guard-source-freshness" => Ok(Some(DeterministicCatchUpPlan::Suppress {
            decision: "guard-source-freshness".to_string(),
            missed_slots,
        })),
        other => Ok(Some(DeterministicCatchUpPlan::Suppress {
            decision: format!("unsupported-policy:{other}"),
            missed_slots,
        })),
    }
}

fn missed_deterministic_due_slots(
    schedule: &DeterministicCronSchedule,
    timezone: Option<&str>,
    previous_tick_ms: i64,
    now_ms: i64,
    max_slots: usize,
) -> Result<Vec<i64>, String> {
    let mut missed = Vec::new();
    let mut slot = floor_to_minute(previous_tick_ms) + 60_000;
    let end = floor_to_minute(now_ms).saturating_sub(60_000);
    while slot <= end && missed.len() < max_slots {
        if deterministic_due_slot(schedule, timezone, slot)?.is_some() {
            missed.push(slot);
        }
        slot = slot.saturating_add(60_000);
    }
    Ok(missed)
}

fn last_scheduler_tick_ms(scheduler_dir: &Path) -> io::Result<Option<i64>> {
    let receipts_file = scheduler_dir.join("receipts.jsonl");
    let Ok(text) = fs::read_to_string(receipts_file) else {
        return Ok(None);
    };
    for line in text.lines().rev() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if value.get("schema").and_then(Value::as_str) == Some(CRON_SCHEDULER_TICK_SCHEMA)
            && let Some(now_ms) = value.get("nowMs").and_then(Value::as_i64)
        {
            return Ok(Some(now_ms));
        }
    }
    Ok(None)
}

#[cfg(test)]
fn cron_expression_due_slot(expression: &str, now_ms: i64) -> Result<Option<i64>, String> {
    cron_expression_due_slot_with_timezone(expression, None, now_ms)
}

fn cron_expression_due_slot_with_timezone(
    expression: &str,
    timezone: Option<&str>,
    now_ms: i64,
) -> Result<Option<i64>, String> {
    let fields = expression.split_whitespace().collect::<Vec<_>>();
    if fields.len() != 5 {
        return Err(format!(
            "cron expression `{expression}` must have five fields"
        ));
    }
    let minute_slot = floor_to_minute(now_ms);
    let offset_minutes = timezone_offset_minutes(timezone)?;
    let local_slot = minute_slot + offset_minutes * 60_000;
    let minute = local_slot.div_euclid(60_000).rem_euclid(60);
    let hour = local_slot.div_euclid(3_600_000).rem_euclid(24);
    let days = local_slot.div_euclid(86_400_000);
    let (_, month, day_of_month) = civil_date_from_unix_days(days);
    let dow = (days + 4).rem_euclid(7);
    if !matches_cron_field(fields[0], minute, 0, 59)? {
        return Ok(None);
    }
    if !matches_cron_field(fields[1], hour, 0, 23)? {
        return Ok(None);
    }
    if !matches_cron_field(fields[3], month, 1, 12)? {
        return Ok(None);
    }
    let day_of_month_matches = matches_cron_field(fields[2], day_of_month, 1, 31)?;
    let day_of_week_matches = matches_cron_field(fields[4], dow, 0, 6)?;
    let day_matches = match (
        matches!(fields[2], "*" | "?"),
        matches!(fields[4], "*" | "?"),
    ) {
        (true, true) => true,
        (true, false) => day_of_week_matches,
        (false, true) => day_of_month_matches,
        (false, false) => day_of_month_matches || day_of_week_matches,
    };
    if !day_matches {
        return Ok(None);
    }
    Ok(Some(minute_slot))
}

fn timezone_offset_minutes(timezone: Option<&str>) -> Result<i64, String> {
    let Some(timezone) = timezone else {
        return Ok(0);
    };
    let normalized = timezone.trim();
    if normalized.is_empty()
        || normalized.eq_ignore_ascii_case("utc")
        || normalized.eq_ignore_ascii_case("z")
    {
        return Ok(0);
    }
    if normalized.eq_ignore_ascii_case("Asia/Taipei") {
        return Ok(8 * 60);
    }
    parse_numeric_timezone_offset(normalized)
        .ok_or_else(|| format!("unsupported cron timezone `{normalized}`"))
}

fn parse_numeric_timezone_offset(value: &str) -> Option<i64> {
    let (sign, rest) = match value.as_bytes().first().copied()? {
        b'+' => (1, &value[1..]),
        b'-' => (-1, &value[1..]),
        _ => return None,
    };
    let (hours, minutes) = match rest.split_once(':') {
        Some((hours, minutes)) => (hours.parse::<i64>().ok()?, minutes.parse::<i64>().ok()?),
        None if rest.len() == 2 => (rest.parse::<i64>().ok()?, 0),
        None if rest.len() == 4 => (
            rest[0..2].parse::<i64>().ok()?,
            rest[2..4].parse::<i64>().ok()?,
        ),
        None => return None,
    };
    if hours > 23 || minutes > 59 {
        return None;
    }
    Some(sign * (hours * 60 + minutes))
}

fn civil_date_from_unix_days(days_since_unix_epoch: i64) -> (i64, i64, i64) {
    // Proleptic Gregorian conversion adapted from Howard Hinnant's
    // civil_from_days algorithm. The offset maps 1970-01-01 to its civil date.
    let shifted = days_since_unix_epoch.saturating_add(719_468);
    let era = if shifted >= 0 {
        shifted
    } else {
        shifted - 146_096
    } / 146_097;
    let day_of_era = shifted - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    if month <= 2 {
        year += 1;
    }
    (year, month, day)
}

fn macro_due_slot_with_timezone(
    name: &str,
    timezone: Option<&str>,
    now_ms: i64,
) -> Result<Option<i64>, String> {
    let minute_slot = floor_to_minute(now_ms);
    let offset_minutes = timezone_offset_minutes(timezone)?;
    let local_slot = minute_slot + offset_minutes * 60_000;
    let minute = ((local_slot / 60_000) % 60) as i64;
    let hour = ((local_slot / 3_600_000) % 24) as i64;
    let days = local_slot / 86_400_000;
    let dow = (days + 4).rem_euclid(7);
    match name {
        "@hourly" => Ok((minute == 0).then_some(minute_slot)),
        "@daily" | "@midnight" => Ok((minute == 0 && hour == 0).then_some(minute_slot)),
        "@weekly" => Ok((minute == 0 && hour == 0 && dow == 0).then_some(minute_slot)),
        other => Err(format!("unsupported cron macro `{other}`")),
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
    attempt: i64,
) -> String {
    let base = format!("cron-scheduler:{source_kind}:{source_id}:{entry_id}:{scheduled_for_ms}");
    if attempt > 0 {
        format!("{base}:attempt:{attempt}")
    } else {
        base
    }
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

fn default_cron_max_active_runs_per_job() -> usize {
    1
}

fn default_cron_max_active_runs_per_agent() -> usize {
    4
}

fn default_cron_max_queued_per_agent() -> usize {
    20
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
    fn cron_expression_supports_calendar_day_and_month_fields() {
        let february_first_1970_08_15 = 31 * 86_400_000 + 8 * 3_600_000 + 15 * 60_000;
        assert_eq!(
            cron_expression_due_slot("15 8 1 2 *", february_first_1970_08_15).unwrap(),
            Some(february_first_1970_08_15)
        );
        assert_eq!(
            cron_expression_due_slot("15 8 1 1 *", february_first_1970_08_15).unwrap(),
            None
        );
        assert_eq!(
            cron_expression_due_slot("15 8 2 2 *", february_first_1970_08_15).unwrap(),
            None
        );
    }

    #[test]
    fn cron_expression_uses_native_timezone_offset() {
        let taipei_09_10_utc_slot = (1 * 3_600_000) + (10 * 60_000);
        assert_eq!(
            cron_expression_due_slot_with_timezone(
                "10 9 * * *",
                Some("Asia/Taipei"),
                taipei_09_10_utc_slot + 1234,
            )
            .unwrap(),
            Some(taipei_09_10_utc_slot)
        );
        assert!(
            cron_expression_due_slot_with_timezone(
                "10 9 * * *",
                Some("Asia/Taipei"),
                9 * 3_600_000 + 10 * 60_000,
            )
            .unwrap()
            .is_none()
        );
    }

    #[test]
    fn native_cron_sticky_session_key_is_forced_into_cron_namespace() {
        let entry = NativeCronPlanEntry {
            job_id: "daily-report".to_string(),
            name: None,
            enabled: true,
            agent_id: Some("main".to_string()),
            action: NativeCronPlanAction::EnqueueAgentTurn,
            reason: "due".to_string(),
            schedule: NativeCronSchedule::At {
                text: None,
                epoch_ms: Some(1000),
            },
            wake_mode: None,
            session_key: Some("main-session".to_string()),
            session_policy: Some("sticky".to_string()),
            message_text: Some("run daily report".to_string()),
            last_run_at_ms: None,
            next_run_at_ms: Some(1000),
        };

        assert_eq!(
            native_cron_session_key(&entry, "main", 1000, "sticky"),
            "cron:main:daily-report:sticky:main-session"
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
        let cron_runs = crate::collect_cron_run_summary(&harness_home).unwrap();
        assert_eq!(cron_runs.summary.active, 1);
        let cron_run = cron_runs.runs.first().unwrap();
        assert_eq!(cron_run.status, CronRunStatus::WorkerEnqueued);
        assert_eq!(cron_run.agent_id, "main");
        assert_eq!(cron_run.entry_id, "due");
        assert_eq!(cron_run.scheduled_for_ms, 1000);
        assert_eq!(cron_run.runtime_class, "cron");
        assert_eq!(cron_run.session_policy, "one-shot");
        assert_eq!(cron_run.session_key, "cron:main:due:1000");
        assert_eq!(cron_run.attempt, 0);

        let worker_conn = Connection::open(crate::worker_db_file(&harness_home)).unwrap();
        let (first_payload_json, first_idempotency_key, first_lane): (String, String, String) = worker_conn
            .query_row(
                "SELECT payload_json, idempotency_key, lane FROM jobs ORDER BY created_at_ms ASC LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        let first_payload: Value = serde_json::from_str(&first_payload_json).unwrap();
        assert_eq!(first_lane, "cron");
        assert_eq!(first_payload["runtimeClass"], "cron");
        assert_eq!(first_payload["origin"], "cron-scheduler");
        assert_eq!(first_payload["sessionPolicy"], "one-shot");
        assert_eq!(first_payload["cronRunId"], cron_run.run_id.as_str());
        assert!(!first_idempotency_key.contains(":attempt:"));

        let second = run_cron_scheduler_once(options.clone()).unwrap();
        assert_eq!(second.summary.skipped_duplicate, 1);

        worker_conn
            .execute(
                "UPDATE jobs SET status='failed-terminal', finished_at_ms=?1 WHERE idempotency_key=?2",
                params![15_000, first_idempotency_key],
            )
            .unwrap();
        crate::mark_cron_run_worker_status(
            &harness_home,
            &cron_run.run_id,
            "failed-terminal",
            "test retry path",
            15_000,
        )
        .unwrap();
        let control = crate::control_cron_run(crate::CronRunControlOptions {
            harness_home: harness_home.clone(),
            action: crate::CronRunControlAction::Retry,
            run_id: Some(cron_run.run_id.clone()),
            agent_id: None,
            entry_id: None,
            reason: "operator retry test".to_string(),
            now_ms: 16_000,
        })
        .unwrap();
        assert_eq!(control.affected, 1);

        let retry = run_cron_scheduler_once(CronSchedulerRunOnceOptions {
            now_ms: 17_000,
            ..options.clone()
        })
        .unwrap();
        assert_eq!(retry.summary.enqueued, 1);
        assert!(
            retry
                .warnings
                .iter()
                .any(|warning| warning.contains("cleared 1 cron scheduler watermark"))
        );
        let retried_runs = crate::collect_cron_run_summary(&harness_home).unwrap();
        let retried_run = retried_runs.runs.first().unwrap();
        assert_eq!(retried_run.status, CronRunStatus::WorkerEnqueued);
        assert_eq!(retried_run.attempt, 1);
        let (job_count, retry_idempotency_key): (i64, String) = worker_conn
            .query_row(
                "SELECT COUNT(*), (SELECT idempotency_key FROM jobs ORDER BY created_at_ms DESC LIMIT 1) FROM jobs",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(job_count, 2);
        assert!(retry_idempotency_key.ends_with(":attempt:1"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn run_once_enqueues_deterministic_cron_with_canon_cron_run_evidence() {
        let root = temp_root("run_once_enqueues_deterministic_cron_with_canon_cron_run_evidence");
        let harness_home = root.join(".agent-harness");
        let source = write_deterministic_source_with_canon(&harness_home);
        fs::create_dir_all(&harness_home).unwrap();

        let options = CronSchedulerRunOnceOptions {
            harness_home: harness_home.clone(),
            source,
            runtime_workspace: None,
            now_ms: 1_234,
            dry_run: false,
            enabled_override: Some(true),
            native_enabled_override: Some(false),
            deterministic_enabled_override: Some(true),
            resume_cron_override: None,
            include_registered_cron_override: None,
            allow_deterministic_run_override: Some(true),
            execute_shell_override: Some(false),
            max_catchup_per_tick_override: None,
            max_enqueue_per_tick_override: None,
        };

        let first = run_cron_scheduler_once(options.clone()).unwrap();
        assert_eq!(first.status, CronSchedulerTickStatus::Completed);
        assert_eq!(first.summary.enqueued, 1);
        assert_eq!(first.decisions.len(), 1);
        assert_eq!(first.decisions[0].entry_id, "canon-deterministic-daily");
        assert_eq!(first.decisions[0].scheduled_for_ms, 0);
        assert_eq!(
            first.decisions[0].decision,
            CronSchedulerJobDecisionStatus::Enqueued
        );

        let cron_runs = crate::collect_cron_run_summary(&harness_home).unwrap();
        assert_eq!(cron_runs.summary.active, 1);
        let cron_run = cron_runs.runs.first().unwrap();
        assert_eq!(cron_run.status, CronRunStatus::WorkerEnqueued);
        assert_eq!(cron_run.source_kind, "deterministic-cron");
        assert_eq!(cron_run.agent_id, "cron-scheduler");
        assert_eq!(cron_run.entry_id, "canon-deterministic-daily");
        assert_eq!(cron_run.scheduled_for_ms, 0);
        assert_eq!(cron_run.runtime_class, "deterministic-shell");
        assert_eq!(cron_run.session_policy, "one-shot");
        assert_eq!(
            cron_run.session_key,
            "cron:deterministic:canon-deterministic-daily:0"
        );
        assert_eq!(cron_run.attempt, 0);
        assert!(
            crate::get_cron_run_by_slot(
                &harness_home,
                "deterministic-cron",
                &cron_run.source_id,
                "canon-deterministic-daily",
                0,
            )
            .unwrap()
            .is_some()
        );

        let worker_conn = Connection::open(crate::worker_db_file(&harness_home)).unwrap();
        let (payload_json, idempotency_key, lane): (String, String, String) = worker_conn
            .query_row(
                "SELECT payload_json, idempotency_key, lane FROM jobs ORDER BY created_at_ms ASC LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        let payload: Value = serde_json::from_str(&payload_json).unwrap();
        assert_eq!(lane, "shell");
        assert_eq!(payload["entryId"], "canon-deterministic-daily");
        assert_eq!(payload["cronCanonId"], "canon-deterministic-daily");
        assert_eq!(payload["cronRunId"], cron_run.run_id.as_str());
        assert_eq!(payload["agentId"], "cron-scheduler");
        assert_eq!(payload["sessionPolicy"], "one-shot");
        assert_eq!(payload["runtimeClass"], "deterministic-shell");
        assert_ne!(payload["deterministicEntryId"], payload["entryId"]);
        assert!(!idempotency_key.contains(":attempt:"));

        let worker_run = crate::run_worker_once(crate::WorkerRunOnceOptions {
            harness_home: harness_home.clone(),
            lane: Some("shell".to_string()),
            worker_id: "cron-terminal-test".to_string(),
            lease_ms: 60_000,
            now_ms: 2_000,
        })
        .unwrap();
        assert_eq!(worker_run.status, crate::WorkerRunOnceStatus::Completed);
        assert_eq!(
            worker_run.result.as_ref().unwrap().status,
            crate::WorkerJobStatus::Succeeded
        );
        assert!(
            worker_run
                .result
                .as_ref()
                .unwrap()
                .reason
                .contains("deterministic shell")
        );
        let terminal_runs = crate::collect_cron_run_summary(&harness_home).unwrap();
        let updated_cron_run = terminal_runs
            .runs
            .iter()
            .find(|run| run.run_id == cron_run.run_id)
            .unwrap();
        assert_eq!(updated_cron_run.status, CronRunStatus::Succeeded);

        let second = run_cron_scheduler_once(options).unwrap();
        assert_eq!(second.summary.skipped_duplicate, 1);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn deterministic_cron_execution_reportable_by_canon_id() {
        run_once_enqueues_deterministic_cron_with_canon_cron_run_evidence();
    }

    #[test]
    fn deterministic_cron_canon_execution_policy_flows_to_cron_run_and_worker() {
        let root =
            temp_root("deterministic_cron_canon_execution_policy_flows_to_cron_run_and_worker");
        let harness_home = root.join(".agent-harness");
        let source = write_deterministic_source_with_canon(&harness_home);
        let canon_path = harness_home
            .join("workspace")
            .join("docs")
            .join("ops")
            .join("cron-canon.json");
        let mut canon: Value = serde_json::from_slice(&fs::read(&canon_path).unwrap()).unwrap();
        canon["activeCrons"][0]["timeoutMs"] = json!(3_600_000);
        canon["activeCrons"][0]["maxAttempts"] = json!(1);
        write_json_atomic(&canon_path, &canon).unwrap();

        let report = run_cron_scheduler_once(CronSchedulerRunOnceOptions {
            harness_home: harness_home.clone(),
            source,
            runtime_workspace: None,
            now_ms: 1_234,
            dry_run: false,
            enabled_override: Some(true),
            native_enabled_override: Some(false),
            deterministic_enabled_override: Some(true),
            resume_cron_override: None,
            include_registered_cron_override: None,
            allow_deterministic_run_override: Some(true),
            execute_shell_override: Some(false),
            max_catchup_per_tick_override: None,
            max_enqueue_per_tick_override: None,
        })
        .unwrap();

        assert_eq!(report.summary.enqueued, 1);
        let cron_runs = crate::collect_cron_run_summary(&harness_home).unwrap();
        assert_eq!(cron_runs.runs[0].max_attempts, 1);
        let worker_conn = Connection::open(crate::worker_db_file(&harness_home)).unwrap();
        let (max_attempts, timeout_ms, payload_json): (i64, i64, String) = worker_conn
            .query_row(
                "SELECT max_attempts, timeout_ms, payload_json FROM jobs LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(max_attempts, 1);
        assert_eq!(timeout_ms, 3_600_000);
        let payload: Value = serde_json::from_str(&payload_json).unwrap();
        assert_eq!(
            payload["env"]["AGENT_HARNESS_CRON_ENTRY_ID"],
            "canon-deterministic-daily"
        );
        assert_eq!(payload["env"]["AGENT_HARNESS_CRON_SCHEDULED_FOR_MS"], "0");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn lint_rejects_invalid_deterministic_cron_execution_policy() {
        let root = temp_root("lint_rejects_invalid_deterministic_cron_execution_policy");
        let harness_home = root.join(".agent-harness");
        let source = write_deterministic_source_with_canon(&harness_home);
        let canon_path = harness_home
            .join("workspace")
            .join("docs")
            .join("ops")
            .join("cron-canon.json");
        let mut canon: Value = serde_json::from_slice(&fs::read(&canon_path).unwrap()).unwrap();
        canon["activeCrons"][0]["timeoutMs"] = json!(0);
        canon["activeCrons"][0]["maxAttempts"] = json!(0);
        write_json_atomic(&canon_path, &canon).unwrap();

        let report = lint_cron_scheduler(CronSchedulerRunOnceOptions {
            harness_home: harness_home.clone(),
            source: source.clone(),
            runtime_workspace: None,
            now_ms: 1_234,
            dry_run: true,
            enabled_override: Some(true),
            native_enabled_override: Some(false),
            deterministic_enabled_override: Some(true),
            resume_cron_override: None,
            include_registered_cron_override: None,
            allow_deterministic_run_override: Some(true),
            execute_shell_override: Some(false),
            max_catchup_per_tick_override: None,
            max_enqueue_per_tick_override: None,
        })
        .unwrap();

        assert_eq!(report.status, CronSchedulerLintStatus::Error);
        assert!(report.findings.iter().any(|finding| {
            finding.code == "deterministic-invalid-execution-policy"
                && finding.entry_id.as_deref() == Some("canon-deterministic-daily")
        }));

        canon["activeCrons"][0]["timeoutMs"] = json!(300_000);
        write_json_atomic(&canon_path, &canon).unwrap();
        let max_attempts_report = lint_cron_scheduler(CronSchedulerRunOnceOptions {
            harness_home: harness_home.clone(),
            source,
            runtime_workspace: None,
            now_ms: 1_234,
            dry_run: true,
            enabled_override: Some(true),
            native_enabled_override: Some(false),
            deterministic_enabled_override: Some(true),
            resume_cron_override: None,
            include_registered_cron_override: None,
            allow_deterministic_run_override: Some(true),
            execute_shell_override: Some(false),
            max_catchup_per_tick_override: None,
            max_enqueue_per_tick_override: None,
        })
        .unwrap();
        assert!(max_attempts_report.findings.iter().any(|finding| {
            finding.code == "deterministic-invalid-execution-policy"
                && finding.message.contains("maxAttempts")
        }));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn scheduler_receipt_rotation_preserves_timestamp_collision_without_overwrite() {
        let root = temp_root("scheduler_receipt_rotation_preserves_timestamp_collision");
        let scheduler_dir = root.join("state").join("cron-scheduler");
        fs::create_dir_all(&scheduler_dir).unwrap();
        let receipts = scheduler_dir.join("receipts.jsonl");
        fs::File::create(&receipts)
            .unwrap()
            .set_len(CRON_RECEIPTS_MAX_BYTES)
            .unwrap();
        let collision = scheduler_dir.join("receipts-1000.jsonl");
        fs::write(&collision, "existing historical receipt").unwrap();
        for index in 1..=3 {
            fs::write(
                scheduler_dir.join(format!("receipts-history-{index}.jsonl")),
                format!("history-{index}"),
            )
            .unwrap();
        }

        rotate_cron_scheduler_receipts_if_needed(&receipts, 1000).unwrap();

        assert_eq!(
            fs::read_to_string(&collision).unwrap(),
            "existing historical receipt"
        );
        assert!(scheduler_dir.join("receipts-1000-1.jsonl").is_file());
        assert!(!receipts.exists());
        let archives = fs::read_dir(&scheduler_dir)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.starts_with("receipts-") && name.ends_with(".jsonl"))
            })
            .count();
        assert_eq!(archives, CRON_RECEIPTS_MAX_ARCHIVES);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn scheduler_receipt_rotation_bounds_archives_and_keeps_current_archive() {
        let root = temp_root("scheduler_receipt_rotation_bounds_archives");
        let scheduler_dir = root.join("state").join("cron-scheduler");
        fs::create_dir_all(&scheduler_dir).unwrap();
        for index in 0..6 {
            fs::write(
                scheduler_dir.join(format!("receipts-history-{index}.jsonl")),
                format!("history-{index}"),
            )
            .unwrap();
        }
        let unrelated = scheduler_dir.join("do-not-delete.txt");
        fs::write(&unrelated, "unrelated state").unwrap();
        let receipts = scheduler_dir.join("receipts.jsonl");
        fs::File::create(&receipts)
            .unwrap()
            .set_len(CRON_RECEIPTS_MAX_BYTES)
            .unwrap();

        rotate_cron_scheduler_receipts_if_needed(&receipts, 2000).unwrap();

        let archives = fs::read_dir(&scheduler_dir)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.starts_with("receipts-") && name.ends_with(".jsonl"))
            })
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert_eq!(archives.len(), CRON_RECEIPTS_MAX_ARCHIVES);
        assert!(archives.iter().any(|name| name == "receipts-2000.jsonl"));
        assert_eq!(fs::read_to_string(unrelated).unwrap(), "unrelated state");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn backup_cron_restart_catch_up_can_be_suppressed_without_enqueue() {
        let root = temp_root("backup_cron_restart_catch_up_can_be_suppressed_without_enqueue");
        let harness_home = root.join(".agent-harness");
        let workspace = harness_home.join("workspace");
        let runner = workspace.join("tools").join("backup-cron-runner");
        let crontab = runner.join("crontab").join("backup.crontab");
        let script = runner.join("jobs").join("backup.ps1");
        fs::create_dir_all(crontab.parent().unwrap()).unwrap();
        fs::create_dir_all(script.parent().unwrap()).unwrap();
        fs::create_dir_all(workspace.join("docs").join("ops")).unwrap();
        fs::write(&crontab, "0 0 * * * jobs/backup.ps1\n").unwrap();
        fs::write(&script, "Write-Output 'backup'\n").unwrap();
        fs::write(
            workspace.join("docs").join("ops").join("cron-canon.json"),
            r#"{
              "schema": "openclaw.agent-harness.cron-canon.v1",
              "activeCrons": [{
                "id": "daily-snapshot-backup",
                "kind": "deterministic-crontab",
                "enabled": true,
                "schedule": "0 0 * * *",
                "sourcePath": "workspace/tools/backup-cron-runner/crontab/backup.crontab",
                "script": "workspace/tools/backup-cron-runner/jobs/backup.ps1",
                "catchUpPolicy": "guard-source-freshness",
                "timeoutMs": 3600000,
                "maxAttempts": 1
              }]
            }"#,
        )
        .unwrap();
        let scheduler_dir = harness_home.join("state").join("cron-scheduler");
        fs::create_dir_all(&scheduler_dir).unwrap();
        fs::write(
            scheduler_dir.join("receipts.jsonl"),
            format!(
                r#"{{"schema":"{CRON_SCHEDULER_TICK_SCHEMA}","status":"completed","nowMs":1000,"summary":{{}}}}"#
            ),
        )
        .unwrap();

        let missed_slot = 86_400_000;
        let report = run_cron_scheduler_once(CronSchedulerRunOnceOptions {
            harness_home: harness_home.clone(),
            source: AgentSource::with_workspace(&harness_home, &workspace),
            runtime_workspace: None,
            now_ms: missed_slot + 61_234,
            dry_run: false,
            enabled_override: Some(true),
            native_enabled_override: Some(false),
            deterministic_enabled_override: Some(true),
            resume_cron_override: None,
            include_registered_cron_override: None,
            allow_deterministic_run_override: Some(true),
            execute_shell_override: Some(false),
            max_catchup_per_tick_override: Some(3),
            max_enqueue_per_tick_override: None,
        })
        .unwrap();

        assert_eq!(report.summary.enqueued, 0);
        let decision = report
            .decisions
            .iter()
            .find(|decision| decision.entry_id == "daily-snapshot-backup")
            .unwrap();
        assert_eq!(
            decision.decision,
            CronSchedulerJobDecisionStatus::SkippedPolicy
        );
        assert_eq!(
            decision.catch_up_decision.as_deref(),
            Some("guard-source-freshness")
        );
        assert_eq!(decision.missed_slots, vec![missed_slot]);
        assert!(!crate::worker_db_file(&harness_home).exists());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn deterministic_crontab_cron_tz_controls_current_and_catch_up_slots() {
        let root = temp_root("deterministic_crontab_cron_tz_controls_current_and_catch_up_slots");
        let harness_home = root.join(".agent-harness");
        let source = write_deterministic_source_with_canon(&harness_home);
        let crontab = harness_home
            .join("workspace")
            .join("tools")
            .join("cron-runner")
            .join("crontab")
            .join("openclaw-mem.crontab");
        fs::write(&crontab, "CRON_TZ=Asia/Taipei\n30 1 * * * jobs/canon.ps1\n").unwrap();
        let canon_path = harness_home
            .join("workspace")
            .join("docs")
            .join("ops")
            .join("cron-canon.json");
        let mut canon: Value = serde_json::from_slice(&fs::read(&canon_path).unwrap()).unwrap();
        canon["activeCrons"][0]["schedule"] = json!("30 1 * * *");
        canon["activeCrons"][0]["catchUpPolicy"] = json!("run-late-once");
        write_json_atomic(&canon_path, &canon).unwrap();

        // 1970-01-02 01:30 Asia/Taipei == 1970-01-01 17:30 UTC. Treating the
        // expression as UTC would instead run it at 09:30 Asia/Taipei.
        let taipei_01_30_slot_utc = 17 * 3_600_000 + 30 * 60_000;
        let current = run_cron_scheduler_once(CronSchedulerRunOnceOptions {
            harness_home: harness_home.clone(),
            source: source.clone(),
            runtime_workspace: None,
            now_ms: taipei_01_30_slot_utc + 1_234,
            dry_run: false,
            enabled_override: Some(true),
            native_enabled_override: Some(false),
            deterministic_enabled_override: Some(true),
            resume_cron_override: None,
            include_registered_cron_override: None,
            allow_deterministic_run_override: Some(true),
            execute_shell_override: Some(false),
            max_catchup_per_tick_override: None,
            max_enqueue_per_tick_override: None,
        })
        .unwrap();
        assert_eq!(current.summary.enqueued, 1);
        assert_eq!(current.decisions[0].scheduled_for_ms, taipei_01_30_slot_utc);
        let worker_conn = Connection::open(crate::worker_db_file(&harness_home)).unwrap();
        let payload_json: String = worker_conn
            .query_row("SELECT payload_json FROM jobs LIMIT 1", [], |row| {
                row.get(0)
            })
            .unwrap();
        let payload: Value = serde_json::from_str(&payload_json).unwrap();
        assert_eq!(payload["env"]["AGENT_HARNESS_CRON_TIMEZONE"], "Asia/Taipei");

        let catch_up_home = root.join("catch-up").join(".agent-harness");
        let catch_up_source = write_deterministic_source_with_canon(&catch_up_home);
        let catch_up_crontab = catch_up_home
            .join("workspace")
            .join("tools")
            .join("cron-runner")
            .join("crontab")
            .join("openclaw-mem.crontab");
        fs::write(
            &catch_up_crontab,
            "TZ=Asia/Taipei\n30 1 * * * jobs/canon.ps1\n",
        )
        .unwrap();
        let catch_up_canon_path = catch_up_home
            .join("workspace")
            .join("docs")
            .join("ops")
            .join("cron-canon.json");
        let mut catch_up_canon: Value =
            serde_json::from_slice(&fs::read(&catch_up_canon_path).unwrap()).unwrap();
        catch_up_canon["activeCrons"][0]["schedule"] = json!("30 1 * * *");
        catch_up_canon["activeCrons"][0]["catchUpPolicy"] = json!("run-late-once");
        write_json_atomic(&catch_up_canon_path, &catch_up_canon).unwrap();
        let scheduler_dir = catch_up_home.join("state").join("cron-scheduler");
        fs::create_dir_all(&scheduler_dir).unwrap();
        fs::write(
            scheduler_dir.join("receipts.jsonl"),
            format!(
                r#"{{"schema":"{CRON_SCHEDULER_TICK_SCHEMA}","status":"completed","nowMs":{},"summary":{{}}}}"#,
                taipei_01_30_slot_utc - 60_000
            ),
        )
        .unwrap();
        let catch_up = run_cron_scheduler_once(CronSchedulerRunOnceOptions {
            harness_home: catch_up_home.clone(),
            source: catch_up_source,
            runtime_workspace: None,
            now_ms: taipei_01_30_slot_utc + 61_234,
            dry_run: false,
            enabled_override: Some(true),
            native_enabled_override: Some(false),
            deterministic_enabled_override: Some(true),
            resume_cron_override: None,
            include_registered_cron_override: None,
            allow_deterministic_run_override: Some(true),
            execute_shell_override: Some(false),
            max_catchup_per_tick_override: Some(3),
            max_enqueue_per_tick_override: None,
        })
        .unwrap();
        assert_eq!(catch_up.summary.enqueued, 1);
        assert_eq!(
            catch_up.decisions[0].scheduled_for_ms,
            taipei_01_30_slot_utc
        );
        assert_eq!(
            catch_up.decisions[0].missed_slots,
            vec![taipei_01_30_slot_utc]
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn run_once_applies_deterministic_catch_up_policies_from_cron_canon() {
        let root = temp_root("run_once_applies_deterministic_catch_up_policies_from_cron_canon");
        let harness_home = root.join(".agent-harness");
        let source = write_deterministic_source_with_catchup_canon(&harness_home);
        let scheduler_dir = harness_home.join("state").join("cron-scheduler");
        fs::create_dir_all(&scheduler_dir).unwrap();
        fs::write(
            scheduler_dir.join("receipts.jsonl"),
            format!(
                r#"{{"schema":"{CRON_SCHEDULER_TICK_SCHEMA}","status":"completed","nowMs":1000,"summary":{{}}}}"#
            ),
        )
        .unwrap();
        let missed_daily_slot = 86_400_000;
        let restart_slot = missed_daily_slot + 60_000;

        let report = run_cron_scheduler_once(CronSchedulerRunOnceOptions {
            harness_home: harness_home.clone(),
            source,
            runtime_workspace: None,
            now_ms: restart_slot + 1_234,
            dry_run: false,
            enabled_override: Some(true),
            native_enabled_override: Some(false),
            deterministic_enabled_override: Some(true),
            resume_cron_override: None,
            include_registered_cron_override: None,
            allow_deterministic_run_override: Some(true),
            execute_shell_override: Some(false),
            max_catchup_per_tick_override: Some(3),
            max_enqueue_per_tick_override: None,
        })
        .unwrap();

        assert_eq!(report.status, CronSchedulerTickStatus::Completed);
        assert_eq!(report.summary.enqueued, 2);
        assert_eq!(report.summary.skipped_policy, 1);
        let analysis = report
            .decisions
            .iter()
            .find(|decision| decision.entry_id == "canon-analysis-daily")
            .unwrap();
        assert_eq!(analysis.decision, CronSchedulerJobDecisionStatus::Enqueued);
        assert_eq!(analysis.scheduled_for_ms, missed_daily_slot);
        assert_eq!(analysis.catch_up_decision.as_deref(), Some("run-late-once"));
        assert_eq!(analysis.missed_slots, vec![missed_daily_slot]);
        let notification = report
            .decisions
            .iter()
            .find(|decision| decision.entry_id == "canon-notification-daily")
            .unwrap();
        assert_eq!(
            notification.decision,
            CronSchedulerJobDecisionStatus::SkippedPolicy
        );
        assert_eq!(
            notification.catch_up_decision.as_deref(),
            Some("guard-source-freshness")
        );
        assert_eq!(notification.missed_slots, vec![missed_daily_slot]);
        let keeper = report
            .decisions
            .iter()
            .find(|decision| decision.entry_id == "canon-keeper-daily")
            .unwrap();
        assert_eq!(keeper.decision, CronSchedulerJobDecisionStatus::Enqueued);
        assert_eq!(keeper.scheduled_for_ms, restart_slot);
        assert_eq!(
            keeper.catch_up_decision.as_deref(),
            Some("run-immediately-on-restart")
        );
        assert_eq!(keeper.missed_slots, vec![missed_daily_slot]);

        let cron_runs = crate::collect_cron_run_summary(&harness_home).unwrap();
        assert_eq!(cron_runs.runs.len(), 2);
        assert!(
            cron_runs
                .runs
                .iter()
                .any(|run| run.entry_id == "canon-analysis-daily"
                    && run.scheduled_for_ms == missed_daily_slot)
        );
        assert!(cron_runs.runs.iter().any(
            |run| run.entry_id == "canon-keeper-daily" && run.scheduled_for_ms == restart_slot
        ));

        let worker_conn = Connection::open(crate::worker_db_file(&harness_home)).unwrap();
        let payloads: Vec<String> = {
            let mut stmt = worker_conn
                .prepare("SELECT payload_json FROM jobs ORDER BY created_at_ms ASC")
                .unwrap();
            stmt.query_map([], |row| row.get::<_, String>(0))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
        };
        assert_eq!(payloads.len(), 2);
        let payload_values = payloads
            .iter()
            .map(|payload| serde_json::from_str::<Value>(payload).unwrap())
            .collect::<Vec<_>>();
        assert!(payload_values.iter().any(|payload| {
            payload["entryId"] == "canon-analysis-daily"
                && payload["catchUpDecision"] == "run-late-once"
                && payload["missedSlots"][0] == missed_daily_slot
        }));
        assert!(payload_values.iter().any(|payload| {
            payload["entryId"] == "canon-keeper-daily"
                && payload["catchUpDecision"] == "run-immediately-on-restart"
                && payload["missedSlots"][0] == missed_daily_slot
        }));
        let receipt_values = fs::read_to_string(scheduler_dir.join("receipts.jsonl"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        let persisted_suppression = receipt_values
            .iter()
            .find(|receipt| {
                receipt["schema"] == CRON_SCHEDULER_JOB_DECISION_SCHEMA
                    && receipt["entryId"] == "canon-notification-daily"
            })
            .unwrap();
        assert_eq!(persisted_suppression["decision"], "skipped-policy");
        assert_eq!(
            persisted_suppression["catchUpDecision"],
            "guard-source-freshness"
        );
        assert_eq!(persisted_suppression["missedSlots"][0], missed_daily_slot);
        assert_eq!(
            persisted_suppression["reason"],
            "catch-up policy suppressed missed deterministic cron slots"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Round16CronOutageFixture {
        now_ms: i64,
        source_max_age_hours: f64,
        missed_daily_slot_ms: i64,
        restart_slot_ms: i64,
        stale_source: Round16StaleSourceFixture,
        cron_canon: Value,
        expected: Round16CronOutageExpected,
    }

    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Round16StaleSourceFixture {
        run_id: String,
        generated_at: String,
        age_hours: i64,
        opinion_text: String,
    }

    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Round16CronOutageExpected {
        health_warning_cron_ids: Vec<String>,
        suppressed_sender_status: String,
        suppressed_sender_reason: String,
        catch_up: std::collections::BTreeMap<String, String>,
    }

    #[test]
    fn e2e_5_cron_outage_replay_from_sanitized_fixture() {
        let fixture: Round16CronOutageFixture = serde_json::from_str(include_str!(
            "../tests/fixtures/round16/e2e5-cron-outage-replay.json"
        ))
        .unwrap();
        let root = temp_root("e2e_5_cron_outage_replay_from_sanitized_fixture");
        let harness_home = root.join(".agent-harness");
        let source = write_round16_cron_outage_source(&harness_home, &fixture.cron_canon);
        seed_round16_cron_outage_receipts(&harness_home, &fixture);

        let health = crate::collect_healthz(crate::HealthzOptions {
            harness_home: harness_home.clone(),
            now_ms: fixture.now_ms,
            loop_stale_ms: 120_000,
            require_writable_state: false,
        })
        .unwrap();
        for cron_id in &fixture.expected.health_warning_cron_ids {
            assert!(
                health
                    .warnings
                    .iter()
                    .any(|warning| warning.contains(cron_id) && warning.contains("receipt")),
                "missing health warning for {cron_id}: {:?}",
                health.warnings
            );
        }
        let status = crate::collect_harness_status(crate::HarnessStatusOptions {
            harness_home: harness_home.clone(),
        })
        .unwrap();
        assert!(status.cron_scheduler.canon.stale_count >= 2);
        assert!(
            status
                .cron_scheduler
                .canon
                .findings
                .iter()
                .any(|finding| finding.cron_id == "cron-canon-keeper"
                    && finding.code == "receipt-not-ok")
        );

        let sender = crate::run_dream_director_send(crate::DreamDirectorSendOptions {
            harness_home: harness_home.clone(),
            target: "fixture-operator".to_string(),
            max_chars: crate::DEFAULT_DREAM_DIRECTOR_MAX_CHARS,
            source_max_age_hours: fixture.source_max_age_hours,
            dry_run: true,
            force: false,
            now_ms: fixture.now_ms,
        })
        .unwrap();
        assert!(!sender.receipt.ok);
        assert_eq!(
            sender.receipt.status,
            fixture.expected.suppressed_sender_status
        );
        assert_eq!(
            sender.receipt.stale_reason.as_deref(),
            Some(fixture.expected.suppressed_sender_reason.as_str())
        );
        assert_eq!(
            sender.receipt.source_run_id.as_deref(),
            Some(fixture.stale_source.run_id.as_str())
        );

        let scheduler_dir = harness_home.join("state").join("cron-scheduler");
        fs::create_dir_all(&scheduler_dir).unwrap();
        fs::write(
            scheduler_dir.join("receipts.jsonl"),
            format!(
                r#"{{"schema":"{CRON_SCHEDULER_TICK_SCHEMA}","status":"completed","nowMs":1000,"summary":{{}}}}"#
            ),
        )
        .unwrap();
        let report = run_cron_scheduler_once(CronSchedulerRunOnceOptions {
            harness_home: harness_home.clone(),
            source,
            runtime_workspace: None,
            now_ms: fixture.restart_slot_ms + 1_234,
            dry_run: false,
            enabled_override: Some(true),
            native_enabled_override: Some(false),
            deterministic_enabled_override: Some(true),
            resume_cron_override: None,
            include_registered_cron_override: None,
            allow_deterministic_run_override: Some(true),
            execute_shell_override: Some(false),
            max_catchup_per_tick_override: Some(3),
            max_enqueue_per_tick_override: None,
        })
        .unwrap();
        assert_eq!(report.status, CronSchedulerTickStatus::Completed);
        assert_eq!(report.summary.enqueued, 2);
        assert_eq!(report.summary.skipped_policy, 1);
        for (entry_id, expected_policy) in &fixture.expected.catch_up {
            let decision = report
                .decisions
                .iter()
                .find(|decision| decision.entry_id == *entry_id)
                .unwrap_or_else(|| panic!("missing decision for {entry_id}"));
            assert_eq!(
                decision.catch_up_decision.as_deref(),
                Some(expected_policy.as_str())
            );
            assert_eq!(decision.missed_slots, vec![fixture.missed_daily_slot_ms]);
        }
        let suppressed = report
            .decisions
            .iter()
            .find(|decision| decision.entry_id == "dream-director-notification")
            .unwrap();
        assert_eq!(
            suppressed.decision,
            CronSchedulerJobDecisionStatus::SkippedPolicy
        );
        let cron_runs = crate::collect_cron_run_summary(&harness_home).unwrap();
        assert!(cron_runs.runs.iter().any(|run| {
            run.entry_id == "canon-analysis-daily"
                && run.scheduled_for_ms == fixture.missed_daily_slot_ms
        }));
        assert!(cron_runs.runs.iter().any(|run| {
            run.entry_id == "cron-canon-keeper" && run.scheduled_for_ms == fixture.restart_slot_ms
        }));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn dream_lite_cron_outage_replay_suppresses_stale_sender_and_catches_up() {
        e2e_5_cron_outage_replay_from_sanitized_fixture();
    }

    #[test]
    fn restart_catch_up_respects_per_job_policy() {
        run_once_applies_deterministic_catch_up_policies_from_cron_canon();
    }

    #[test]
    fn run_once_enqueues_imported_expr_cron_with_native_timezone() {
        let root = temp_root("run_once_enqueues_imported_expr_cron_with_native_timezone");
        let source = write_expr_cron_source(&root);
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        let taipei_09_10_utc_slot = (1 * 3_600_000) + (10 * 60_000);

        let report = run_cron_scheduler_once(CronSchedulerRunOnceOptions {
            harness_home: harness_home.clone(),
            source,
            runtime_workspace: None,
            now_ms: taipei_09_10_utc_slot + 1234,
            dry_run: false,
            enabled_override: Some(true),
            native_enabled_override: Some(true),
            deterministic_enabled_override: Some(false),
            resume_cron_override: Some(true),
            include_registered_cron_override: Some(true),
            allow_deterministic_run_override: None,
            execute_shell_override: None,
            max_catchup_per_tick_override: None,
            max_enqueue_per_tick_override: None,
        })
        .unwrap();

        assert_eq!(report.status, CronSchedulerTickStatus::Completed);
        assert_eq!(report.summary.enqueued, 1);
        assert_eq!(report.decisions.len(), 1);
        assert_eq!(report.decisions[0].entry_id, "expr-cron-job");
        assert_eq!(report.decisions[0].scheduled_for_ms, taipei_09_10_utc_slot);
        assert_eq!(
            report.decisions[0].decision,
            CronSchedulerJobDecisionStatus::Enqueued
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn lint_reports_too_short_interval() {
        let root = temp_root("lint_reports_too_short_interval");
        let source = write_source(&root);
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            r#"{"cronScheduler":{"enabled":true,"intervalMs":1000,"nativeCron":{"enabled":true,"resumeCron":true}}}"#,
        )
        .unwrap();

        let report = lint_cron_scheduler(CronSchedulerRunOnceOptions {
            harness_home: harness_home.clone(),
            source,
            runtime_workspace: None,
            now_ms: 10_000,
            dry_run: true,
            enabled_override: None,
            native_enabled_override: None,
            deterministic_enabled_override: None,
            resume_cron_override: None,
            include_registered_cron_override: None,
            allow_deterministic_run_override: None,
            execute_shell_override: None,
            max_catchup_per_tick_override: None,
            max_enqueue_per_tick_override: None,
        })
        .unwrap();

        assert_eq!(report.status, CronSchedulerLintStatus::Error);
        assert!(
            report
                .findings
                .iter()
                .any(|finding| finding.code == "interval-too-short")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn lint_proposed_actions_cover_operator_migration_ledgers() {
        assert_eq!(
            cron_lint_proposed_action("native-main-agent-cron"),
            Some("review-owner")
        );
        assert_eq!(
            cron_lint_proposed_action("native-runtime-path-mismatch"),
            Some("rewrite-path-or-native-port")
        );
        assert_eq!(
            cron_lint_proposed_action("deterministic-shell-compatibility-required"),
            Some("native-port-or-wrapper-policy")
        );
    }

    #[cfg(windows)]
    #[test]
    fn run_once_and_lint_block_linux_absolute_paths_on_windows() {
        let root = temp_root("run_once_and_lint_block_linux_absolute_paths_on_windows");
        let source = write_source(&root);
        fs::write(
            source.home.join("cron").join("jobs.json"),
            r#"{
              "jobs": [
                {
                  "id": "linux-path",
                  "enabled": true,
                  "agentId": "main",
                  "schedule": { "kind": "at", "epochMs": 1000 },
                  "messageText": "read /root/.openclaw/cron/jobs.json"
                }
              ]
            }"#,
        )
        .unwrap();
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

        let lint = lint_cron_scheduler(options.clone()).unwrap();
        assert_eq!(lint.status, CronSchedulerLintStatus::Error);
        assert!(
            lint.findings
                .iter()
                .any(|finding| finding.code == "native-runtime-path-mismatch")
        );
        let path_finding = lint
            .findings
            .iter()
            .find(|finding| finding.code == "native-runtime-path-mismatch")
            .unwrap();
        assert_eq!(path_finding.agent_id.as_deref(), Some("main"));
        assert_eq!(
            path_finding.proposed_action.as_deref(),
            Some("rewrite-path-or-native-port")
        );
        let tick = run_cron_scheduler_once(options).unwrap();
        assert_eq!(tick.summary.enqueued, 0);
        assert_eq!(tick.summary.skipped_policy, 1);

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

    fn write_expr_cron_source(root: &Path) -> AgentSource {
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
                  "id": "expr-cron-job",
                  "enabled": true,
                  "agentId": "main",
                  "schedule": { "kind": "cron", "expr": "10 9 * * *", "tz": "Asia/Taipei" },
                  "payload": { "message": "Run expr cron" }
                }
              ]
            }"#,
        )
        .unwrap();
        AgentSource::with_workspace(home, workspace)
    }

    fn write_deterministic_source_with_canon(harness_home: &Path) -> AgentSource {
        let home = harness_home.to_path_buf();
        let workspace = home.join("workspace");
        let runner = workspace.join("tools").join("cron-runner");
        fs::create_dir_all(runner.join("crontab")).unwrap();
        fs::create_dir_all(runner.join("jobs")).unwrap();
        fs::create_dir_all(workspace.join("docs").join("ops")).unwrap();
        fs::write(
            runner.join("crontab").join("openclaw-mem.crontab"),
            "0 0 * * * jobs/canon.ps1\n",
        )
        .unwrap();
        fs::write(runner.join("jobs").join("canon.ps1"), "Write-Output 'ok'\n").unwrap();
        fs::write(
            workspace.join("docs").join("ops").join("cron-canon.json"),
            r#"{
              "schema": "openclaw.agent-harness.cron-canon.v1",
              "activeCrons": [
                {
                  "id": "canon-deterministic-daily",
                  "kind": "deterministic-crontab",
                  "enabled": true,
                  "schedule": "0 0 * * *",
                  "sourcePath": "workspace/tools/cron-runner/crontab/openclaw-mem.crontab",
                  "script": "workspace/tools/cron-runner/jobs/canon.ps1",
                  "monitor": {
                    "type": "latest-json",
                    "path": "state/canon/latest.json",
                    "maxAgeHours": 24,
                    "okField": "ok",
                    "okValue": true
                  }
                }
              ]
            }"#,
        )
        .unwrap();
        AgentSource::with_workspace(home, workspace)
    }

    fn write_deterministic_source_with_catchup_canon(harness_home: &Path) -> AgentSource {
        let home = harness_home.to_path_buf();
        let workspace = home.join("workspace");
        let runner = workspace.join("tools").join("cron-runner");
        fs::create_dir_all(runner.join("crontab")).unwrap();
        fs::create_dir_all(runner.join("jobs")).unwrap();
        fs::create_dir_all(workspace.join("docs").join("ops")).unwrap();
        fs::write(
            runner.join("crontab").join("openclaw-mem.crontab"),
            "0 0 * * * jobs/analysis.ps1\n0 0 * * * jobs/notify.ps1\n0 0 * * * jobs/keeper.ps1\n",
        )
        .unwrap();
        for script in ["analysis.ps1", "notify.ps1", "keeper.ps1"] {
            fs::write(
                runner.join("jobs").join(script),
                format!("Write-Output '{script}'\n"),
            )
            .unwrap();
        }
        fs::write(
            workspace.join("docs").join("ops").join("cron-canon.json"),
            r#"{
              "schema": "openclaw.agent-harness.cron-canon.v1",
              "activeCrons": [
                {
                  "id": "canon-analysis-daily",
                  "kind": "deterministic-crontab",
                  "enabled": true,
                  "schedule": "0 0 * * *",
                  "sourcePath": "workspace/tools/cron-runner/crontab/openclaw-mem.crontab",
                  "script": "workspace/tools/cron-runner/jobs/analysis.ps1",
                  "catchUpPolicy": "run-late-once",
                  "monitor": {
                    "type": "latest-json",
                    "path": "state/canon/analysis.json",
                    "maxAgeHours": 36,
                    "okField": "ok",
                    "okValue": true
                  }
                },
                {
                  "id": "canon-notification-daily",
                  "kind": "deterministic-crontab",
                  "enabled": true,
                  "schedule": "0 0 * * *",
                  "sourcePath": "workspace/tools/cron-runner/crontab/openclaw-mem.crontab",
                  "script": "workspace/tools/cron-runner/jobs/notify.ps1",
                  "catchUpPolicy": "guard-source-freshness",
                  "monitor": {
                    "type": "latest-json",
                    "path": "state/canon/notify.json",
                    "maxAgeHours": 36,
                    "okField": "ok",
                    "okValue": true
                  }
                },
                {
                  "id": "canon-keeper-daily",
                  "kind": "deterministic-crontab",
                  "enabled": true,
                  "schedule": "0 0 * * *",
                  "sourcePath": "workspace/tools/cron-runner/crontab/openclaw-mem.crontab",
                  "script": "workspace/tools/cron-runner/jobs/keeper.ps1",
                  "catchUpPolicy": "run-immediately-on-restart",
                  "monitor": {
                    "type": "latest-json",
                    "path": "state/canon/keeper.json",
                    "maxAgeHours": 36,
                    "okField": "ok",
                    "okValue": true
                  }
                }
              ]
            }"#,
        )
        .unwrap();
        AgentSource::with_workspace(home, workspace)
    }

    fn write_round16_cron_outage_source(harness_home: &Path, cron_canon: &Value) -> AgentSource {
        let home = harness_home.to_path_buf();
        let workspace = home.join("workspace");
        let runner = workspace.join("tools").join("cron-runner");
        fs::create_dir_all(runner.join("crontab")).unwrap();
        fs::create_dir_all(runner.join("jobs")).unwrap();
        fs::create_dir_all(workspace.join("docs").join("ops")).unwrap();
        fs::write(
            runner.join("crontab").join("openclaw-mem.crontab"),
            "0 0 * * * jobs/analysis.ps1\n0 0 * * * jobs/notify.ps1\n0 0 * * * jobs/keeper.ps1\n",
        )
        .unwrap();
        for script in ["analysis.ps1", "notify.ps1", "keeper.ps1"] {
            fs::write(
                runner.join("jobs").join(script),
                format!("Write-Output '{script}'\n"),
            )
            .unwrap();
        }
        write_json_atomic(
            &workspace.join("docs").join("ops").join("cron-canon.json"),
            cron_canon,
        )
        .unwrap();
        AgentSource::with_workspace(home, workspace)
    }

    fn seed_round16_cron_outage_receipts(harness_home: &Path, fixture: &Round16CronOutageFixture) {
        let stale_generated_at_ms = fixture.now_ms - fixture.stale_source.age_hours * 3_600_000;
        let daily_dir = crate::dream_director_daily_state_dir(harness_home);
        let opinion_path = daily_dir
            .join("runs")
            .join(&fixture.stale_source.run_id)
            .join("director-opinion.md");
        fs::create_dir_all(opinion_path.parent().unwrap()).unwrap();
        fs::write(&opinion_path, &fixture.stale_source.opinion_text).unwrap();
        write_json_atomic(
            &daily_dir.join("latest.json"),
            &json!({
                "ok": true,
                "runId": fixture.stale_source.run_id,
                "generatedAtMs": stale_generated_at_ms,
                "generatedAt": fixture.stale_source.generated_at,
                "directorOpinion": opinion_path
            }),
        )
        .unwrap();
        write_json_atomic(
            &harness_home
                .join("state")
                .join("memory")
                .join("dream-lite-director")
                .join("latest-send.json"),
            &json!({
                "ok": false,
                "status": "stale-source-suppressed",
                "generatedAtMs": stale_generated_at_ms,
                "stale": true
            }),
        )
        .unwrap();
        write_json_atomic(
            &harness_home
                .join("state")
                .join("memory")
                .join("cron-canon-keeper")
                .join("latest-cron-canon-keeper.json"),
            &json!({
                "schema": "openclaw.agent-harness.cron-canon-keeper.receipt.v1",
                "ok": false,
                "status": "warn",
                "generatedAt": fixture.stale_source.generated_at,
                "findings": [
                    {
                        "severity": "warn",
                        "code": "receipt-not-ok",
                        "cronId": "cron-canon-keeper",
                        "message": "synthetic keeper replay warning",
                        "details": {
                            "path": "state/memory/cron-canon-keeper/latest-cron-canon-keeper.json"
                        }
                    }
                ]
            }),
        )
        .unwrap();
        for path in [
            harness_home.join("state").join("runtime-queue"),
            harness_home.join("state").join("channels"),
            harness_home.join("state").join("logs"),
        ] {
            fs::create_dir_all(path).unwrap();
        }
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
