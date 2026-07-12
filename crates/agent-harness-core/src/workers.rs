use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    CronRunSummary, HarnessLogEvent, HarnessLogLevel, LearningReviewOptions, RuntimeQueueItem,
    RuntimeQueueItemStatus, RuntimeQueueSource, RuntimeQueueSourceKind,
    SelfImprovementNotificationTarget, SelfImprovementReviewMode, SkillApplyOptions,
    SkillLearningProposalOperation, SkillLearningProposalStatus, SkillLearningSignal,
    SkillProposeOptions, SkillSynthesisOptions, append_harness_log,
    append_self_improvement_notification, apply_skill_proposal,
    build_self_improvement_replacement_body,
    child_execution_policy::ChildExecutionPolicyV1,
    collect_cron_run_summary,
    config::{
        HarnessConfigValidationReport, HarnessConfigValidationStatus, validate_harness_config,
    },
    create_skill_learning_proposal, cron_run_worker_dispatch_blocker, current_log_time_ms,
    mark_cron_run_runtime_enqueued, mark_cron_run_worker_status,
    memory_backfill::{
        DEFAULT_MEMORY_BACKFILL_BATCH_SIZE, DEFAULT_MEMORY_BACKFILL_COVERAGE_THRESHOLD_BPS,
        DEFAULT_MEMORY_BACKFILL_MAX_ITEMS, DEFAULT_MEMORY_BACKFILL_RATE_LIMIT_PER_MINUTE,
        DEFAULT_MEMORY_BACKFILL_RETRY_CAP, DEFAULT_MEMORY_BACKFILL_VECTOR_DIMENSION,
        MemoryEmbeddingBackfillLane, MemoryEmbeddingBackfillOptions, run_memory_embedding_backfill,
    },
    run_learning_review,
    subagent_lifecycle::{
        SubagentLifecycleRecordOptions, SubagentLifecycleShowOptions, SubagentLifecycleState,
        record_subagent_lifecycle, show_subagent_lifecycle,
    },
    synthesize_skill,
};

const WORKER_STORE_SCHEMA: &str = "agent-harness.worker-store.v1";
const WORKER_ENQUEUE_SCHEMA: &str = "agent-harness.worker-enqueue.v1";
const WORKER_RUN_ONCE_SCHEMA: &str = "agent-harness.worker-run-once.v1";
const WORKER_STATUS_SCHEMA: &str = "agent-harness.worker-status.v1";
const WORKER_REAP_SCHEMA: &str = "agent-harness.worker-reap-stale.v1";
const WORKER_CANCEL_SCHEMA: &str = "agent-harness.worker-cancel.v1";
const DEFAULT_TIMEOUT_MS: u64 = 300_000;
const SHELL_OUTPUT_CAP_BYTES: usize = 16 * 1024;
const INVALID_STORED_CHILD_POLICY_CODE: &str = "worker.invalid-stored-child-policy";
const INVALID_STORED_CHILD_POLICY_REASON: &str = "stored child execution policy failed validation";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerEnqueueOptions {
    pub harness_home: PathBuf,
    pub kind: WorkerJobKind,
    pub lane: Option<String>,
    pub payload: Value,
    pub idempotency_key: Option<String>,
    pub parent_job_id: Option<String>,
    pub job_group_id: Option<String>,
    pub master_agent_id: Option<String>,
    pub master_session_key: Option<String>,
    pub wake_policy: Option<Value>,
    pub source: Option<String>,
    pub priority: i64,
    pub available_at_ms: Option<i64>,
    pub max_attempts: i64,
    pub timeout_ms: Option<u64>,
    pub cascade_timeout_ms: Option<u64>,
    pub rate_key: Option<String>,
    pub concurrency_group_key: Option<String>,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerEnqueueOptionsV2 {
    pub options: WorkerEnqueueOptions,
    pub child_policy: Option<ChildExecutionPolicyV1>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerEnqueueReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub database: PathBuf,
    pub job: WorkerJob,
    pub inserted: bool,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerRunOnceOptions {
    pub harness_home: PathBuf,
    pub lane: Option<String>,
    pub worker_id: String,
    pub lease_ms: i64,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerRunOnceReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub database: PathBuf,
    pub status: WorkerRunOnceStatus,
    pub job: Option<WorkerJob>,
    pub result: Option<WorkerJobExecutionResult>,
    pub blocked: WorkerCapacityBlockedSummary,
    pub reason: String,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkerRunOnceStatus {
    Completed,
    Dispatched,
    Rescheduled,
    NoWork,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerStatusOptions {
    pub harness_home: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerStatusReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub database: PathBuf,
    pub config: WorkerDispatchConfig,
    pub totals: WorkerStatusTotals,
    pub by_lane: Vec<WorkerLaneStatus>,
    pub blocked: WorkerCapacityBlockedSummary,
    pub downstream_runtime: WorkerDownstreamRuntimeStatus,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerStatusTotals {
    pub total: usize,
    pub pending: usize,
    pub leased: usize,
    pub running: usize,
    pub runtime_queued: usize,
    pub succeeded: usize,
    pub failed_retryable: usize,
    pub failed_terminal: usize,
    pub canceled: usize,
    pub expired: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerLaneStatus {
    pub lane: String,
    pub totals: WorkerStatusTotals,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerCapacityBlockedSummary {
    pub blocked_by_global_limit: usize,
    pub blocked_by_group_limit: usize,
    pub blocked_by_channel_limit: usize,
    pub blocked_by_lane_limit: usize,
    pub blocked_by_rate_lease: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerDownstreamRuntimeStatus {
    pub runtime_queue_file: PathBuf,
    pub open_runtime_items: usize,
    pub open_cron_runtime_items: usize,
    pub open_by_runtime_class: BTreeMap<String, usize>,
    pub open_by_origin: BTreeMap<String, usize>,
    pub cron_runs: CronRunSummary,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerReapStaleOptions {
    pub harness_home: PathBuf,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerReapStaleReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub database: PathBuf,
    pub expired_jobs: usize,
    pub retryable_jobs: usize,
    pub terminal_jobs: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerCancelOptions {
    pub harness_home: PathBuf,
    pub job_id: String,
    pub now_ms: i64,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerCancelReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub database: PathBuf,
    pub job_id: String,
    pub canceled: bool,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerJobKind {
    DeterministicShell,
    LlmSubagent,
    Watchdog,
    MasterWakeup,
    MemoryMaintenance,
    MemoryEmbeddingBackfill,
    LearningReview,
    SkillSynthesis,
    PluginCall,
}

impl WorkerJobKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::DeterministicShell => "deterministic_shell",
            Self::LlmSubagent => "llm_subagent",
            Self::Watchdog => "watchdog",
            Self::MasterWakeup => "master_wakeup",
            Self::MemoryMaintenance => "memory_maintenance",
            Self::MemoryEmbeddingBackfill => "memory_embedding_backfill",
            Self::LearningReview => "learning_review",
            Self::SkillSynthesis => "skill_synthesis",
            Self::PluginCall => "plugin_call",
        }
    }

    pub fn default_lane(&self) -> &'static str {
        match self {
            Self::DeterministicShell => "shell",
            Self::LlmSubagent | Self::MasterWakeup => "llm",
            Self::Watchdog => "watchdog",
            Self::MemoryMaintenance => "maintenance",
            Self::MemoryEmbeddingBackfill => "memory_embedding_backfill",
            Self::LearningReview => "learning_review",
            Self::SkillSynthesis => "skill_synthesis",
            Self::PluginCall => "plugin",
        }
    }
}

impl std::str::FromStr for WorkerJobKind {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "deterministic_shell" | "deterministic-shell" | "shell" => Ok(Self::DeterministicShell),
            "llm_subagent" | "llm-subagent" | "llm" => Ok(Self::LlmSubagent),
            "watchdog" => Ok(Self::Watchdog),
            "master_wakeup" | "master-wakeup" => Ok(Self::MasterWakeup),
            "memory_maintenance" | "memory-maintenance" => Ok(Self::MemoryMaintenance),
            "memory_embedding_backfill" | "memory-embedding-backfill" | "memory-backfill" => {
                Ok(Self::MemoryEmbeddingBackfill)
            }
            "learning_review" | "learning-review" | "skill-learning-review" => {
                Ok(Self::LearningReview)
            }
            "skill_synthesis" | "skill-synthesis" | "skill-synthesis-review" => {
                Ok(Self::SkillSynthesis)
            }
            "plugin_call" | "plugin-call" => Ok(Self::PluginCall),
            other => Err(format!("unsupported worker job kind: {other}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkerJobStatus {
    Pending,
    Leased,
    Running,
    RuntimeQueued,
    Succeeded,
    FailedRetryable,
    FailedTerminal,
    Canceled,
    Expired,
}

impl WorkerJobStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Leased => "leased",
            Self::Running => "running",
            Self::RuntimeQueued => "runtime-queued",
            Self::Succeeded => "succeeded",
            Self::FailedRetryable => "failed-retryable",
            Self::FailedTerminal => "failed-terminal",
            Self::Canceled => "canceled",
            Self::Expired => "expired",
        }
    }

    fn from_db(value: &str) -> Self {
        match value {
            "pending" => Self::Pending,
            "leased" => Self::Leased,
            "running" => Self::Running,
            "runtime-queued" => Self::RuntimeQueued,
            "succeeded" => Self::Succeeded,
            "failed-retryable" => Self::FailedRetryable,
            "failed-terminal" => Self::FailedTerminal,
            "canceled" => Self::Canceled,
            "expired" => Self::Expired,
            _ => Self::FailedTerminal,
        }
    }

    fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::FailedTerminal | Self::Canceled | Self::Expired
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerJob {
    pub schema: &'static str,
    pub job_id: String,
    pub kind: WorkerJobKind,
    pub lane: String,
    pub status: WorkerJobStatus,
    pub parent_job_id: Option<String>,
    pub job_group_id: Option<String>,
    pub master_agent_id: Option<String>,
    pub master_session_key: Option<String>,
    pub wake_policy: Option<Value>,
    pub source: Option<String>,
    pub payload: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub child_policy: Option<ChildExecutionPolicyV1>,
    pub idempotency_key: Option<String>,
    pub priority: i64,
    pub available_at_ms: i64,
    pub lease_owner: Option<String>,
    pub lease_expires_at_ms: Option<i64>,
    pub attempt: i64,
    pub max_attempts: i64,
    pub timeout_ms: Option<u64>,
    pub cascade_timeout_ms: Option<u64>,
    pub rate_key: Option<String>,
    pub concurrency_group_key: Option<String>,
    pub audit_path: Option<PathBuf>,
    pub result: Option<Value>,
    pub artifact_refs: Option<Value>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub finished_at_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerJobExecutionResult {
    pub status: WorkerJobStatus,
    pub reason: String,
    pub audit_path: Option<PathBuf>,
    pub artifact_refs: Option<Value>,
    pub result: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerDispatchConfig {
    pub global_concurrency_limit: usize,
    pub group_concurrency_limit: usize,
    pub channel_concurrency_limit: usize,
    pub lane_concurrency_limits: BTreeMap<String, usize>,
    pub rate_lease_limit: usize,
    pub rate_lease_window_ms: i64,
    pub allowed_script_roots: Vec<PathBuf>,
    pub warnings: Vec<String>,
}

pub fn worker_db_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("workers")
        .join("worker-jobs.sqlite")
}

pub fn init_worker_store(harness_home: impl AsRef<Path>) -> io::Result<PathBuf> {
    let db_file = worker_db_file(harness_home);
    if let Some(parent) = db_file.parent() {
        fs::create_dir_all(parent)?;
    }
    let conn = Connection::open(&db_file).map_err(io::Error::other)?;
    create_schema(&conn).map_err(io::Error::other)?;
    Ok(db_file)
}

pub fn enqueue_worker_job(options: WorkerEnqueueOptions) -> io::Result<WorkerEnqueueReport> {
    enqueue_worker_job_v2(WorkerEnqueueOptionsV2 {
        options,
        child_policy: None,
    })
}

pub fn enqueue_worker_job_v2(options: WorkerEnqueueOptionsV2) -> io::Result<WorkerEnqueueReport> {
    let WorkerEnqueueOptionsV2 {
        options,
        child_policy,
    } = options;
    let child_policy_json = child_policy
        .as_ref()
        .map(|policy| {
            policy.validate().map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("invalid child execution policy: {error}"),
                )
            })?;
            serde_json::to_string(policy).map_err(io::Error::other)
        })
        .transpose()?;
    let db_file = init_worker_store(&options.harness_home)?;
    let conn = Connection::open(&db_file).map_err(io::Error::other)?;
    quarantine_invalid_stored_child_policies(&conn, options.now_ms)?;
    let lane = options
        .lane
        .clone()
        .unwrap_or_else(|| options.kind.default_lane().to_string());
    let idempotency_key = options.idempotency_key.clone().unwrap_or_else(|| {
        let legacy_stable = format!(
            "{}\n{}\n{}\n{}\n{}",
            options.kind.as_str(),
            lane,
            options.parent_job_id.as_deref().unwrap_or(""),
            options.source.as_deref().unwrap_or(""),
            options.payload,
        );
        let stable = child_policy_json
            .as_deref()
            .map(|policy| format!("{legacy_stable}\n{policy}"))
            .unwrap_or(legacy_stable);
        format!("auto:{}", fnv1a_64_hex(&stable))
    });

    if let Some(existing) = find_job_by_idempotency(&conn, &idempotency_key)? {
        ensure_llm_subagent_lifecycle_on_idempotency_hit(
            &options.harness_home,
            &existing,
            options.now_ms,
        )?;
        signal_worker_queue_wake(&options.harness_home, &lane, "worker job idempotency hit");
        return Ok(WorkerEnqueueReport {
            schema: WORKER_ENQUEUE_SCHEMA,
            harness_home: options.harness_home,
            database: db_file,
            job: existing,
            inserted: false,
            reason: "existing job returned for idempotency key".to_string(),
        });
    }

    let job_id = format!(
        "job:{}:{}:{}",
        options.now_ms,
        options.kind.as_str(),
        fnv1a_64_hex(&format!("{}\n{}", idempotency_key, options.payload))
    );
    let available_at_ms = options.available_at_ms.unwrap_or(options.now_ms);
    let wake_policy = options.wake_policy.unwrap_or(Value::Null);
    let timeout_ms = options.timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS);
    let payload_json = serde_json::to_string(&options.payload).map_err(io::Error::other)?;
    let wake_policy_json = value_to_nullable_string(&wake_policy)?;

    conn.execute(
        "INSERT INTO jobs (
            job_id, kind, lane, status, parent_job_id, job_group_id,
            master_agent_id, master_session_key, wake_policy_json, source,
            payload_json, idempotency_key, priority, available_at_ms,
            lease_owner, lease_expires_at_ms, attempt, max_attempts,
            timeout_ms, cascade_timeout_ms, rate_key, concurrency_group_key,
            audit_path, result_json, artifact_refs_json, created_at_ms,
            updated_at_ms, finished_at_ms, child_policy_json
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, NULL, NULL, 0, ?15, ?16, ?17, ?18, ?19, NULL, NULL, NULL, ?20, ?20, NULL, ?21)",
        params![
            job_id,
            options.kind.as_str(),
            lane,
            WorkerJobStatus::Pending.as_str(),
            options.parent_job_id,
            options.job_group_id,
            options.master_agent_id,
            options.master_session_key,
            wake_policy_json,
            options.source,
            payload_json,
            idempotency_key,
            options.priority,
            available_at_ms,
            options.max_attempts,
            i64::try_from(timeout_ms).unwrap_or(i64::MAX),
            options
                .cascade_timeout_ms
                .and_then(|value| i64::try_from(value).ok()),
            options.rate_key,
            options.concurrency_group_key,
            options.now_ms,
            child_policy_json,
        ],
    )
    .map_err(io::Error::other)?;
    let job = find_job_by_id(&conn, &job_id)?
        .ok_or_else(|| io::Error::other(format!("inserted worker job not found: {job_id}")))?;
    record_llm_subagent_lifecycle_queued(
        &options.harness_home,
        &options.kind,
        &options.payload,
        &job,
        options.now_ms,
    )?;
    signal_worker_queue_wake(&options.harness_home, &lane, "worker job enqueue");

    Ok(WorkerEnqueueReport {
        schema: WORKER_ENQUEUE_SCHEMA,
        harness_home: options.harness_home,
        database: db_file,
        job,
        inserted: true,
        reason: "worker job inserted before side effects".to_string(),
    })
}

pub fn run_worker_once(options: WorkerRunOnceOptions) -> io::Result<WorkerRunOnceReport> {
    let db_file = init_worker_store(&options.harness_home)?;
    let conn = Connection::open(&db_file).map_err(io::Error::other)?;
    quarantine_invalid_stored_child_policies(&conn, options.now_ms)?;
    reconcile_runtime_queued_jobs(&options.harness_home, &conn, options.now_ms)?;
    let config = load_worker_dispatch_config(&options.harness_home)?;
    let (job, blocked) = lease_next_job(
        &conn,
        &config,
        options.lane.as_deref(),
        &options.worker_id,
        options.now_ms,
        options.lease_ms,
    )?;
    let Some(job) = job else {
        return Ok(WorkerRunOnceReport {
            schema: WORKER_RUN_ONCE_SCHEMA,
            harness_home: options.harness_home,
            database: db_file,
            status: WorkerRunOnceStatus::NoWork,
            job: None,
            result: None,
            blocked,
            reason: "no pending worker job could be leased".to_string(),
            warnings: config.warnings,
        });
    };

    if let Some(blocker) = cron_worker_dispatch_blocker_for_job(&options.harness_home, &job)? {
        let result = WorkerJobExecutionResult {
            status: WorkerJobStatus::Canceled,
            reason: format!("worker job skipped because {blocker}"),
            audit_path: None,
            artifact_refs: Some(json!({
                "cronRunBlocked": true,
                "reason": blocker,
            })),
            result: Some(json!({"skipped": true})),
        };
        persist_execution_result(&conn, &job, &result, options.now_ms)?;
        sync_cron_run_after_worker_result(&options.harness_home, &job, &result, options.now_ms)?;
        append_harness_log(
            &options.harness_home,
            &HarnessLogEvent::new(
                current_log_time_ms().unwrap_or(options.now_ms),
                HarnessLogLevel::Warn,
                "workers",
                "worker.run-once.cron-run-blocked",
                result.reason.clone(),
            ),
        )?;
        let persisted_job = find_job_by_id(&conn, &job.job_id)?;
        return Ok(WorkerRunOnceReport {
            schema: WORKER_RUN_ONCE_SCHEMA,
            harness_home: options.harness_home,
            database: db_file,
            status: WorkerRunOnceStatus::Failed,
            job: persisted_job,
            result: Some(result),
            blocked,
            reason: "worker job blocked by cron run control".to_string(),
            warnings: config.warnings,
        });
    }

    set_job_status(
        &conn,
        &job.job_id,
        WorkerJobStatus::Running,
        options.now_ms,
        None,
        None,
        None,
    )?;
    let result = normalize_exhausted_retryable_result(
        &job,
        match execute_worker_job(&options.harness_home, &conn, &job, &config, options.now_ms) {
            Ok(result) => result,
            Err(error) => WorkerJobExecutionResult {
                status: WorkerJobStatus::FailedRetryable,
                reason: format!("worker job execution failed before result: {error}"),
                audit_path: None,
                artifact_refs: Some(json!({"error": error.to_string()})),
                result: None,
            },
        },
    );
    let terminal = result.status.is_terminal();
    let rescheduled = result.status == WorkerJobStatus::Pending;
    persist_execution_result(&conn, &job, &result, options.now_ms)?;
    sync_cron_run_after_worker_result(&options.harness_home, &job, &result, options.now_ms)?;
    append_harness_log(
        &options.harness_home,
        &HarnessLogEvent::new(
            current_log_time_ms().unwrap_or(options.now_ms),
            if matches!(
                result.status,
                WorkerJobStatus::Succeeded | WorkerJobStatus::RuntimeQueued
            ) || rescheduled
            {
                HarnessLogLevel::Info
            } else {
                HarnessLogLevel::Warn
            },
            "workers",
            "worker.run-once",
            result.reason.clone(),
        )
        .path(result.audit_path.clone()),
    )?;

    let persisted_job = find_job_by_id(&conn, &job.job_id)?;
    Ok(WorkerRunOnceReport {
        schema: WORKER_RUN_ONCE_SCHEMA,
        harness_home: options.harness_home,
        database: db_file,
        status: if result.status == WorkerJobStatus::RuntimeQueued {
            WorkerRunOnceStatus::Dispatched
        } else if terminal {
            if result.status == WorkerJobStatus::Succeeded {
                WorkerRunOnceStatus::Completed
            } else {
                WorkerRunOnceStatus::Failed
            }
        } else {
            WorkerRunOnceStatus::Rescheduled
        },
        job: persisted_job,
        result: Some(result),
        blocked,
        reason: "worker job execution recorded".to_string(),
        warnings: config.warnings,
    })
}

fn cron_worker_dispatch_blocker_for_job(
    harness_home: &Path,
    job: &WorkerJob,
) -> io::Result<Option<String>> {
    let Some(cron_run_id) = string_path_any(&job.payload, &["cronRunId", "cron_run_id"]) else {
        return Ok(None);
    };
    cron_run_worker_dispatch_blocker(harness_home, cron_run_id, &job.job_id)
}

fn sync_cron_run_after_worker_result(
    harness_home: &Path,
    job: &WorkerJob,
    result: &WorkerJobExecutionResult,
    now_ms: i64,
) -> io::Result<()> {
    let Some(cron_run_id) = string_path_any(&job.payload, &["cronRunId", "cron_run_id"]) else {
        return Ok(());
    };
    if matches!(
        result.status,
        WorkerJobStatus::Succeeded | WorkerJobStatus::RuntimeQueued
    ) && job.kind != WorkerJobKind::DeterministicShell
    {
        return Ok(());
    }
    mark_cron_run_worker_status(
        harness_home,
        cron_run_id,
        result.status.as_str(),
        &result.reason,
        now_ms,
    )
}

pub fn collect_worker_status(options: WorkerStatusOptions) -> io::Result<WorkerStatusReport> {
    let db_file = init_worker_store(&options.harness_home)?;
    let conn = Connection::open(&db_file).map_err(io::Error::other)?;
    let config = load_worker_dispatch_config(&options.harness_home)?;
    let totals = worker_totals(&conn, None)?;
    let lanes = worker_lanes(&conn)?;
    let mut by_lane = Vec::new();
    for lane in lanes {
        by_lane.push(WorkerLaneStatus {
            totals: worker_totals(&conn, Some(&lane))?,
            lane,
        });
    }
    let blocked = blocked_summary(&conn, &config, None, epoch_ms()?)?;
    let downstream_runtime = collect_downstream_runtime_status(&options.harness_home)?;
    let warnings = config.warnings.clone();
    Ok(WorkerStatusReport {
        schema: WORKER_STATUS_SCHEMA,
        harness_home: options.harness_home,
        database: db_file,
        config,
        totals,
        by_lane,
        blocked,
        downstream_runtime,
        warnings,
    })
}

fn collect_downstream_runtime_status(
    harness_home: &Path,
) -> io::Result<WorkerDownstreamRuntimeStatus> {
    let queue_dir = harness_home.join("state").join("runtime-queue");
    let runtime_queue_file = queue_dir.join("pending.jsonl");
    let terminal_ids = read_runtime_terminal_ids(&queue_dir.join("run-once-receipts.jsonl"))?;
    let mut open_runtime_items = 0usize;
    let mut open_cron_runtime_items = 0usize;
    let mut open_by_runtime_class = BTreeMap::new();
    let mut open_by_origin = BTreeMap::new();
    if runtime_queue_file.is_file() {
        let text = fs::read_to_string(&runtime_queue_file)?;
        for line in text.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
                continue;
            };
            let Some(queue_id) = string_path_any(&value, &["queueId", "queue_id"]) else {
                continue;
            };
            if terminal_ids.contains(queue_id) {
                continue;
            }
            let platform = string_path_any(&value, &["platform"]).unwrap_or("worker");
            let runtime_class = string_path_any(&value, &["runtimeClass", "runtime_class"])
                .map(ToString::to_string)
                .unwrap_or_else(|| {
                    if platform == "native-cron" {
                        "cron".to_string()
                    } else {
                        "interactive".to_string()
                    }
                });
            let origin = string_path_any(&value, &["origin"])
                .unwrap_or(if platform == "native-cron" {
                    "cron-scheduler"
                } else {
                    "unknown"
                })
                .to_string();
            open_runtime_items += 1;
            if runtime_class == "cron" {
                open_cron_runtime_items += 1;
            }
            *open_by_runtime_class.entry(runtime_class).or_insert(0) += 1;
            *open_by_origin.entry(origin).or_insert(0) += 1;
        }
    }
    let cron_runs = collect_cron_run_summary(harness_home)
        .map(|report| report.summary)
        .unwrap_or_default();
    Ok(WorkerDownstreamRuntimeStatus {
        runtime_queue_file,
        open_runtime_items,
        open_cron_runtime_items,
        open_by_runtime_class,
        open_by_origin,
        cron_runs,
    })
}

fn read_runtime_terminal_ids(path: &Path) -> io::Result<std::collections::BTreeSet<String>> {
    let mut terminal = std::collections::BTreeSet::new();
    if !path.is_file() {
        return Ok(terminal);
    }
    let text = fs::read_to_string(path)?;
    let mut latest = BTreeMap::new();
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
            continue;
        };
        if let Some(queue_id) = string_path_any(&value, &["queueId", "queue_id"])
            && let Some(status) = string_path_any(&value, &["status"])
        {
            latest.insert(queue_id.to_string(), status.to_string());
        }
    }
    for (queue_id, status) in latest {
        if matches!(
            status.as_str(),
            "completed"
                | "timeout"
                | "failed-terminal"
                | "canceled"
                | "skipped"
                | "dead-letter"
                | "suppressed"
        ) {
            terminal.insert(queue_id);
        }
    }
    Ok(terminal)
}

pub fn reap_stale_worker_jobs(
    options: WorkerReapStaleOptions,
) -> io::Result<WorkerReapStaleReport> {
    let db_file = init_worker_store(&options.harness_home)?;
    let conn = Connection::open(&db_file).map_err(io::Error::other)?;
    quarantine_invalid_stored_child_policies(&conn, options.now_ms)?;
    let mut expired_jobs = 0;
    let mut retryable_jobs = 0;
    let mut terminal_jobs = 0;
    let stale = jobs_with_expired_leases(&conn, options.now_ms)?;
    for job in stale {
        expired_jobs += 1;
        if job.attempt < job.max_attempts {
            retryable_jobs += 1;
            let next_available = options.now_ms.saturating_add(backoff_ms(job.attempt));
            conn.execute(
                "UPDATE jobs SET status=?1, lease_owner=NULL, lease_expires_at_ms=NULL, available_at_ms=?2, updated_at_ms=?3, result_json=?4 WHERE job_id=?5",
                params![
                    WorkerJobStatus::Pending.as_str(),
                    next_available,
                    options.now_ms,
                    json!({"reason":"stale lease reaped for retry","previousStatus":job.status.as_str()}).to_string(),
                    job.job_id,
                ],
            )
            .map_err(io::Error::other)?;
        } else {
            terminal_jobs += 1;
            set_job_status(
                &conn,
                &job.job_id,
                WorkerJobStatus::Expired,
                options.now_ms,
                Some(json!({"reason":"stale lease expired after max attempts"})),
                None,
                None,
            )?;
        }
    }
    Ok(WorkerReapStaleReport {
        schema: WORKER_REAP_SCHEMA,
        harness_home: options.harness_home,
        database: db_file,
        expired_jobs,
        retryable_jobs,
        terminal_jobs,
    })
}

pub fn cancel_worker_job(options: WorkerCancelOptions) -> io::Result<WorkerCancelReport> {
    let db_file = init_worker_store(&options.harness_home)?;
    let conn = Connection::open(&db_file).map_err(io::Error::other)?;
    let rows = conn
        .execute(
            "UPDATE jobs SET status=?1, lease_owner=NULL, lease_expires_at_ms=NULL, result_json=?2, updated_at_ms=?3, finished_at_ms=?3 WHERE job_id=?4 AND status NOT IN ('succeeded','failed-terminal','canceled','expired')",
            params![
                WorkerJobStatus::Canceled.as_str(),
                json!({"reason": options.reason}).to_string(),
                options.now_ms,
                options.job_id,
            ],
        )
        .map_err(io::Error::other)?;
    Ok(WorkerCancelReport {
        schema: WORKER_CANCEL_SCHEMA,
        harness_home: options.harness_home,
        database: db_file,
        job_id: options.job_id,
        canceled: rows > 0,
        reason: if rows > 0 {
            "worker job canceled".to_string()
        } else {
            "worker job not found or already terminal".to_string()
        },
    })
}

pub fn load_worker_dispatch_config(
    harness_home: impl AsRef<Path>,
) -> io::Result<WorkerDispatchConfig> {
    let harness_home = harness_home.as_ref();
    let validation = validate_harness_config(harness_home)?;
    if validation.status == HarnessConfigValidationStatus::Invalid {
        return Err(invalid_harness_config_error(&validation));
    }
    let mut config = WorkerDispatchConfig {
        global_concurrency_limit: 12,
        group_concurrency_limit: 6,
        channel_concurrency_limit: 3,
        lane_concurrency_limits: BTreeMap::from([
            ("cron".to_string(), 3),
            ("llm".to_string(), 6),
            ("shell".to_string(), 6),
            ("watchdog".to_string(), 2),
            ("maintenance".to_string(), 2),
            ("plugin".to_string(), 2),
        ]),
        rate_lease_limit: 0,
        rate_lease_window_ms: 60_000,
        allowed_script_roots: vec![
            harness_home.join("scripts"),
            harness_home.join("state").join("workers").join("scripts"),
        ],
        warnings: Vec::new(),
    };

    for path in [
        harness_home.join("harness-config.json"),
        harness_home.join("config").join("harness-config.json"),
    ] {
        if !path.is_file() {
            continue;
        }
        let text = fs::read_to_string(&path)?;
        let value = serde_json::from_str::<Value>(&text).map_err(io::Error::other)?;
        let dispatch = value.get("workerDispatch").unwrap_or(&value);
        if let Some(limit) = dispatch
            .get("globalConcurrencyLimit")
            .and_then(Value::as_u64)
            .and_then(|value| usize::try_from(value).ok())
        {
            config.global_concurrency_limit = limit;
        }
        if let Some(limit) = dispatch
            .get("groupConcurrencyLimit")
            .and_then(Value::as_u64)
            .and_then(|value| usize::try_from(value).ok())
        {
            config.group_concurrency_limit = limit;
        }
        if let Some(limit) = dispatch
            .get("channelConcurrencyLimit")
            .and_then(Value::as_u64)
            .and_then(|value| usize::try_from(value).ok())
        {
            config.channel_concurrency_limit = limit;
        }
        if let Some(lanes) = dispatch
            .get("laneConcurrencyLimits")
            .and_then(Value::as_object)
        {
            for (lane, limit) in lanes {
                if let Some(limit) = limit.as_u64().and_then(|value| usize::try_from(value).ok()) {
                    config.lane_concurrency_limits.insert(lane.clone(), limit);
                }
            }
        }
        if let Some(limit) = dispatch
            .get("rateLeaseLimit")
            .and_then(Value::as_u64)
            .and_then(|value| usize::try_from(value).ok())
        {
            config.rate_lease_limit = limit;
        }
        if let Some(window) = dispatch
            .get("rateLeaseWindowMs")
            .and_then(Value::as_i64)
            .filter(|value| *value > 0)
        {
            config.rate_lease_window_ms = window;
        }
        if let Some(roots) = dispatch.get("allowedScriptRoots").and_then(Value::as_array) {
            config
                .allowed_script_roots
                .extend(roots.iter().filter_map(Value::as_str).map(PathBuf::from));
        }
        break;
    }

    if config.global_concurrency_limit < config.group_concurrency_limit {
        config.warnings.push(format!(
            "workerDispatch.globalConcurrencyLimit ({}) is lower than groupConcurrencyLimit ({}); group limit is capped to global",
            config.global_concurrency_limit, config.group_concurrency_limit
        ));
        config.group_concurrency_limit = config.global_concurrency_limit;
    }
    if config.group_concurrency_limit < config.channel_concurrency_limit {
        config.warnings.push(format!(
            "workerDispatch.groupConcurrencyLimit ({}) is lower than channelConcurrencyLimit ({}); channel limit is capped to group",
            config.group_concurrency_limit, config.channel_concurrency_limit
        ));
        config.channel_concurrency_limit = config.group_concurrency_limit;
    }
    Ok(config)
}

fn invalid_harness_config_error(report: &HarnessConfigValidationReport) -> io::Error {
    let path = report
        .config_file
        .as_ref()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "harness-config.json".to_string());
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("invalid {path}: {}", report.errors.join("; ")),
    )
}

fn create_schema(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        "
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT PRIMARY KEY,
            kind TEXT NOT NULL,
            lane TEXT NOT NULL,
            status TEXT NOT NULL,
            parent_job_id TEXT,
            job_group_id TEXT,
            master_agent_id TEXT,
            master_session_key TEXT,
            wake_policy_json TEXT,
            source TEXT,
            payload_json TEXT NOT NULL,
            child_policy_json TEXT,
            idempotency_key TEXT NOT NULL UNIQUE,
            priority INTEGER NOT NULL DEFAULT 0,
            available_at_ms INTEGER NOT NULL,
            lease_owner TEXT,
            lease_expires_at_ms INTEGER,
            attempt INTEGER NOT NULL DEFAULT 0,
            max_attempts INTEGER NOT NULL DEFAULT 3,
            timeout_ms INTEGER NOT NULL DEFAULT 300000,
            cascade_timeout_ms INTEGER,
            rate_key TEXT,
            concurrency_group_key TEXT,
            audit_path TEXT,
            result_json TEXT,
            artifact_refs_json TEXT,
            created_at_ms INTEGER NOT NULL,
            updated_at_ms INTEGER NOT NULL,
            finished_at_ms INTEGER
        );
        CREATE INDEX IF NOT EXISTS jobs_pending_idx ON jobs(status, available_at_ms, priority);
        CREATE INDEX IF NOT EXISTS jobs_lane_idx ON jobs(lane, status);
        CREATE INDEX IF NOT EXISTS jobs_group_idx ON jobs(concurrency_group_key, status);
        CREATE INDEX IF NOT EXISTS jobs_job_group_idx ON jobs(job_group_id, status);
        CREATE TABLE IF NOT EXISTS rate_leases (
            lease_id INTEGER PRIMARY KEY AUTOINCREMENT,
            rate_key TEXT NOT NULL,
            job_id TEXT NOT NULL,
            leased_at_ms INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS rate_leases_key_idx ON rate_leases(rate_key, leased_at_ms);
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        INSERT OR REPLACE INTO meta(key, value) VALUES ('schema', 'agent-harness.worker-store.v1');
        ",
    )?;
    ensure_child_policy_column(conn)
}

fn ensure_child_policy_column(conn: &Connection) -> rusqlite::Result<()> {
    let mut stmt = conn.prepare("PRAGMA table_info(jobs)")?;
    let columns = stmt.query_map([], |row| row.get::<_, String>(1))?;
    for column in columns {
        if column? == "child_policy_json" {
            return Ok(());
        }
    }
    conn.execute("ALTER TABLE jobs ADD COLUMN child_policy_json TEXT", [])?;
    Ok(())
}

fn find_job_by_idempotency(
    conn: &Connection,
    idempotency_key: &str,
) -> io::Result<Option<WorkerJob>> {
    conn.query_row(
        "SELECT * FROM jobs WHERE idempotency_key=?1",
        params![idempotency_key],
        row_to_job,
    )
    .optional()
    .map_err(io::Error::other)
}

fn find_job_by_id(conn: &Connection, job_id: &str) -> io::Result<Option<WorkerJob>> {
    conn.query_row(
        "SELECT * FROM jobs WHERE job_id=?1",
        params![job_id],
        row_to_job,
    )
    .optional()
    .map_err(io::Error::other)
}

fn lease_next_job(
    conn: &Connection,
    config: &WorkerDispatchConfig,
    lane_filter: Option<&str>,
    worker_id: &str,
    now_ms: i64,
    lease_ms: i64,
) -> io::Result<(Option<WorkerJob>, WorkerCapacityBlockedSummary)> {
    let candidates = pending_candidates(conn, lane_filter, now_ms)?;
    let mut blocked = WorkerCapacityBlockedSummary::default();
    for job in candidates {
        match capacity_blocker(conn, config, &job, now_ms)? {
            Some(CapacityBlocker::Global) => blocked.blocked_by_global_limit += 1,
            Some(CapacityBlocker::Lane) => blocked.blocked_by_lane_limit += 1,
            Some(CapacityBlocker::Group) => blocked.blocked_by_group_limit += 1,
            Some(CapacityBlocker::Channel) => blocked.blocked_by_channel_limit += 1,
            Some(CapacityBlocker::RateLease) => blocked.blocked_by_rate_lease += 1,
            None => {
                let lease_expires = now_ms.saturating_add(lease_ms.max(1));
                let rows = conn
                    .execute(
                        "UPDATE jobs SET status=?1, lease_owner=?2, lease_expires_at_ms=?3, attempt=attempt+1, updated_at_ms=?4 WHERE job_id=?5 AND status IN ('pending','failed-retryable')",
                        params![
                            WorkerJobStatus::Leased.as_str(),
                            worker_id,
                            lease_expires,
                            now_ms,
                            job.job_id,
                        ],
                    )
                    .map_err(io::Error::other)?;
                if rows > 0 {
                    record_rate_lease(conn, config, &job, now_ms)?;
                    return Ok((find_job_by_id(conn, &job.job_id)?, blocked));
                }
            }
        }
    }
    Ok((None, blocked))
}

fn pending_candidates(
    conn: &Connection,
    lane_filter: Option<&str>,
    now_ms: i64,
) -> io::Result<Vec<WorkerJob>> {
    let sql = if lane_filter.is_some() {
        "SELECT * FROM jobs WHERE status IN ('pending','failed-retryable') AND available_at_ms <= ?1 AND lane=?2 ORDER BY priority DESC, available_at_ms ASC, created_at_ms ASC LIMIT 100"
    } else {
        "SELECT * FROM jobs WHERE status IN ('pending','failed-retryable') AND available_at_ms <= ?1 ORDER BY priority DESC, available_at_ms ASC, created_at_ms ASC LIMIT 100"
    };
    let mut stmt = conn.prepare(sql).map_err(io::Error::other)?;
    let mut rows = if let Some(lane) = lane_filter {
        stmt.query(params![now_ms, lane])
    } else {
        stmt.query(params![now_ms])
    }
    .map_err(io::Error::other)?;
    let mut candidates = Vec::new();
    let mut invalid_child_policy_job_ids = Vec::new();
    while let Some(row) = rows.next().map_err(io::Error::other)? {
        let job_id: String = row.get("job_id").map_err(io::Error::other)?;
        let child_policy_text: Option<String> =
            row.get("child_policy_json").map_err(io::Error::other)?;
        if stored_child_policy_is_invalid(child_policy_text.as_deref()) {
            invalid_child_policy_job_ids.push(job_id);
            continue;
        }
        candidates.push(row_to_job(row).map_err(io::Error::other)?);
    }
    drop(rows);
    drop(stmt);

    for job_id in invalid_child_policy_job_ids {
        quarantine_invalid_stored_child_policy(conn, &job_id, now_ms)?;
    }
    Ok(candidates)
}

fn stored_child_policy_is_invalid(child_policy_text: Option<&str>) -> bool {
    child_policy_text
        .is_some_and(|text| serde_json::from_str::<ChildExecutionPolicyV1>(text).is_err())
}

fn quarantine_invalid_stored_child_policies(conn: &Connection, now_ms: i64) -> io::Result<usize> {
    let invalid_job_ids = {
        let mut stmt = conn
            .prepare(
                "SELECT job_id, child_policy_json FROM jobs WHERE child_policy_json IS NOT NULL",
            )
            .map_err(io::Error::other)?;
        let rows = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(io::Error::other)?;
        rows.filter_map(|row| match row {
            Ok((job_id, policy)) if stored_child_policy_is_invalid(Some(&policy)) => {
                Some(Ok(job_id))
            }
            Ok(_) => None,
            Err(error) => Some(Err(io::Error::other(error))),
        })
        .collect::<io::Result<Vec<_>>>()?
    };
    for job_id in &invalid_job_ids {
        quarantine_invalid_stored_child_policy(conn, job_id, now_ms)?;
    }
    Ok(invalid_job_ids.len())
}

fn quarantine_invalid_stored_child_policy(
    conn: &Connection,
    job_id: &str,
    now_ms: i64,
) -> io::Result<()> {
    let result = json!({
        "status": WorkerJobStatus::FailedTerminal.as_str(),
        "failureCode": INVALID_STORED_CHILD_POLICY_CODE,
        "reason": INVALID_STORED_CHILD_POLICY_REASON,
        "quarantined": true,
    });
    conn.execute(
        "UPDATE jobs SET
            child_policy_json=NULL,
            status=CASE WHEN status IN ('succeeded','failed-terminal','canceled','expired') THEN status ELSE ?1 END,
            lease_owner=NULL,
            lease_expires_at_ms=NULL,
            result_json=CASE WHEN status IN ('succeeded','failed-terminal','canceled','expired') THEN result_json ELSE ?2 END,
            updated_at_ms=?3,
            finished_at_ms=CASE WHEN status IN ('succeeded','failed-terminal','canceled','expired') THEN finished_at_ms ELSE ?3 END
         WHERE job_id=?4 AND child_policy_json IS NOT NULL",
        params![
            WorkerJobStatus::FailedTerminal.as_str(),
            result.to_string(),
            now_ms,
            job_id,
        ],
    )
    .map_err(io::Error::other)?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CapacityBlocker {
    Global,
    Lane,
    Group,
    Channel,
    RateLease,
}

fn capacity_blocker(
    conn: &Connection,
    config: &WorkerDispatchConfig,
    job: &WorkerJob,
    now_ms: i64,
) -> io::Result<Option<CapacityBlocker>> {
    let executing_global = executing_count(conn, None, None)?;
    if executing_global >= config.global_concurrency_limit {
        return Ok(Some(CapacityBlocker::Global));
    }
    let lane_limit = config
        .lane_concurrency_limits
        .get(&job.lane)
        .copied()
        .unwrap_or(config.global_concurrency_limit);
    let executing_lane = executing_count(conn, Some(&job.lane), None)?;
    if executing_lane >= lane_limit {
        return Ok(Some(CapacityBlocker::Lane));
    }
    if let Some(group) = job.concurrency_group_key.as_deref() {
        let executing_group = executing_count(conn, None, Some(group))?;
        if executing_group >= config.group_concurrency_limit {
            return Ok(Some(CapacityBlocker::Group));
        }
    }
    if let Some(channel_key) = worker_channel_key(job) {
        let executing_channel = executing_channel_count(conn, &channel_key, now_ms)?;
        if executing_channel >= config.channel_concurrency_limit {
            return Ok(Some(CapacityBlocker::Channel));
        }
    }
    if rate_lease_blocked(conn, config, job, now_ms)? {
        return Ok(Some(CapacityBlocker::RateLease));
    }
    Ok(None)
}

fn blocked_summary(
    conn: &Connection,
    config: &WorkerDispatchConfig,
    lane_filter: Option<&str>,
    now_ms: i64,
) -> io::Result<WorkerCapacityBlockedSummary> {
    let mut summary = WorkerCapacityBlockedSummary::default();
    for job in pending_candidates(conn, lane_filter, now_ms)? {
        match capacity_blocker(conn, config, &job, now_ms)? {
            Some(CapacityBlocker::Global) => summary.blocked_by_global_limit += 1,
            Some(CapacityBlocker::Lane) => summary.blocked_by_lane_limit += 1,
            Some(CapacityBlocker::Group) => summary.blocked_by_group_limit += 1,
            Some(CapacityBlocker::Channel) => summary.blocked_by_channel_limit += 1,
            Some(CapacityBlocker::RateLease) => summary.blocked_by_rate_lease += 1,
            None => {}
        }
    }
    Ok(summary)
}

fn executing_channel_count(conn: &Connection, channel_key: &str, now_ms: i64) -> io::Result<usize> {
    let rows = {
        let mut stmt = conn
            .prepare("SELECT * FROM jobs WHERE status IN ('leased','running')")
            .map_err(io::Error::other)?;
        stmt.query_map([], row_to_job_tolerating_invalid_child_policy)
            .map_err(io::Error::other)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(io::Error::other)?
    };
    let rows = resolve_tolerant_worker_rows(conn, rows, now_ms, false)?;
    Ok(rows
        .iter()
        .filter(|job| worker_channel_key(job).as_deref() == Some(channel_key))
        .count())
}

fn worker_channel_key(job: &WorkerJob) -> Option<String> {
    let agent = job
        .master_agent_id
        .as_deref()
        .or_else(|| string_path(&job.payload, "agentId"))
        .or_else(|| string_path(&job.payload, "masterAgentId"))
        .or(job.concurrency_group_key.as_deref())?;
    let platform = string_path(&job.payload, "platform").unwrap_or(&job.lane);
    let channel_id = string_path(&job.payload, "channelId")
        .or_else(|| string_path(&job.payload, "jobId"))
        .or_else(|| string_path(&job.payload, "runId"))?;
    Some(format!(
        "{}:{}:{}",
        normalize_key_part(agent),
        normalize_key_part(platform),
        normalize_key_part(channel_id)
    ))
}

fn rate_lease_blocked(
    conn: &Connection,
    config: &WorkerDispatchConfig,
    job: &WorkerJob,
    now_ms: i64,
) -> io::Result<bool> {
    let Some(rate_key) = job.rate_key.as_deref() else {
        return Ok(false);
    };
    if config.rate_lease_limit == 0 {
        return Ok(false);
    }
    reap_old_rate_leases(conn, config, now_ms)?;
    let since = now_ms.saturating_sub(config.rate_lease_window_ms);
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM rate_leases WHERE rate_key=?1 AND leased_at_ms >= ?2",
            params![rate_key, since],
            |row| row.get(0),
        )
        .map_err(io::Error::other)?;
    Ok(usize::try_from(count).unwrap_or(usize::MAX) >= config.rate_lease_limit)
}

fn record_rate_lease(
    conn: &Connection,
    config: &WorkerDispatchConfig,
    job: &WorkerJob,
    now_ms: i64,
) -> io::Result<()> {
    let Some(rate_key) = job.rate_key.as_deref() else {
        return Ok(());
    };
    if config.rate_lease_limit == 0 {
        return Ok(());
    }
    reap_old_rate_leases(conn, config, now_ms)?;
    conn.execute(
        "INSERT INTO rate_leases (rate_key, job_id, leased_at_ms) VALUES (?1, ?2, ?3)",
        params![rate_key, job.job_id, now_ms],
    )
    .map_err(io::Error::other)?;
    Ok(())
}

fn reap_old_rate_leases(
    conn: &Connection,
    config: &WorkerDispatchConfig,
    now_ms: i64,
) -> io::Result<()> {
    if config.rate_lease_limit == 0 {
        return Ok(());
    }
    let before = now_ms.saturating_sub(config.rate_lease_window_ms);
    conn.execute(
        "DELETE FROM rate_leases WHERE leased_at_ms < ?1",
        params![before],
    )
    .map_err(io::Error::other)?;
    Ok(())
}

fn executing_count(
    conn: &Connection,
    lane: Option<&str>,
    group: Option<&str>,
) -> io::Result<usize> {
    let (sql, params_value): (&str, Vec<String>) = match (lane, group) {
        (Some(lane), Some(group)) => (
            "SELECT COUNT(*) FROM jobs WHERE status IN ('leased','running') AND lane=?1 AND concurrency_group_key=?2",
            vec![lane.to_string(), group.to_string()],
        ),
        (Some(lane), None) => (
            "SELECT COUNT(*) FROM jobs WHERE status IN ('leased','running') AND lane=?1",
            vec![lane.to_string()],
        ),
        (None, Some(group)) => (
            "SELECT COUNT(*) FROM jobs WHERE status IN ('leased','running') AND concurrency_group_key=?1",
            vec![group.to_string()],
        ),
        (None, None) => (
            "SELECT COUNT(*) FROM jobs WHERE status IN ('leased','running')",
            Vec::new(),
        ),
    };
    let count: i64 = match params_value.as_slice() {
        [] => conn.query_row(sql, [], |row| row.get(0)),
        [one] => conn.query_row(sql, params![one], |row| row.get(0)),
        [one, two] => conn.query_row(sql, params![one, two], |row| row.get(0)),
        _ => unreachable!(),
    }
    .map_err(io::Error::other)?;
    Ok(usize::try_from(count).unwrap_or(usize::MAX))
}

fn execute_worker_job(
    harness_home: &Path,
    conn: &Connection,
    job: &WorkerJob,
    config: &WorkerDispatchConfig,
    now_ms: i64,
) -> io::Result<WorkerJobExecutionResult> {
    match job.kind {
        WorkerJobKind::DeterministicShell => run_deterministic_shell_job(harness_home, job, config),
        WorkerJobKind::LlmSubagent | WorkerJobKind::MasterWakeup => {
            queue_llm_worker_turn(harness_home, job, now_ms)
        }
        WorkerJobKind::Watchdog => run_watchdog_job(harness_home, conn, job, now_ms),
        WorkerJobKind::MemoryEmbeddingBackfill => {
            run_memory_embedding_backfill_job(harness_home, job, now_ms)
        }
        WorkerJobKind::LearningReview => run_learning_review_job(harness_home, job, now_ms),
        WorkerJobKind::SkillSynthesis => run_skill_synthesis_job(harness_home, job, now_ms),
        WorkerJobKind::MemoryMaintenance | WorkerJobKind::PluginCall => {
            Ok(WorkerJobExecutionResult {
                status: WorkerJobStatus::Succeeded,
                reason: "adapter job recorded as completed by worker dispatch MVP".to_string(),
                audit_path: None,
                artifact_refs: None,
                result: Some(json!({"status":"adapter-recorded","kind":job.kind.as_str()})),
            })
        }
    }
}

fn run_learning_review_job(
    harness_home: &Path,
    job: &WorkerJob,
    now_ms: i64,
) -> io::Result<WorkerJobExecutionResult> {
    let target_path =
        string_path_any(&job.payload, &["targetPath", "target_path"]).map(PathBuf::from);
    let target_skill_id = string_path_any(&job.payload, &["targetSkillId", "target_skill_id"])
        .map(ToString::to_string);
    let signal_text = string_path_any(&job.payload, &["signalText", "signal_text", "text"])
        .unwrap_or("")
        .to_string();
    let source_turn =
        string_path_any(&job.payload, &["sourceTurn", "source_turn"]).map(ToString::to_string);
    let mode = self_improvement_mode_from_payload(&job.payload);
    let payload_replacement_body =
        string_path_any(&job.payload, &["replacementBody", "replacement_body"])
            .map(ToString::to_string);
    let replacement_body = match payload_replacement_body {
        Some(body) => Some(body),
        None if mode == SelfImprovementReviewMode::DispatchAndReplace => {
            match (target_skill_id.as_deref(), target_path.as_ref()) {
                (Some(skill_id), Some(path)) => {
                    let validated_path = crate::skill_learning::validate_skill_target_path(
                        harness_home,
                        skill_id,
                        path,
                    )?;
                    build_self_improvement_replacement_body(
                        &validated_path,
                        &signal_text,
                        source_turn.as_deref(),
                        now_ms,
                    )?
                }
                _ => None,
            }
        }
        None => None,
    };
    let replacement_requested = replacement_body.is_some();

    let report = if let (Some(replacement_body), Some(target_skill_id), Some(target_path)) = (
        replacement_body,
        target_skill_id.clone(),
        target_path.clone(),
    ) {
        let proposal = create_skill_learning_proposal(SkillProposeOptions {
            harness_home: harness_home.to_path_buf(),
            target_skill_id,
            target_path,
            operation: SkillLearningProposalOperation::Replace,
            replacement_body: Some(replacement_body),
            support_files: Vec::new(),
            diff: Some(signal_text.clone()),
            signals: vec![SkillLearningSignal {
                kind: "self-improvement-review".to_string(),
                signal_hash: stable_worker_text_hash("self-improvement-review", &signal_text),
                text: signal_text.clone(),
                trust: string_path_any(&job.payload, &["channelTrust", "channel_trust"])
                    .map(ToString::to_string),
            }],
            source_turn: source_turn.clone(),
            risk_class: "low".to_string(),
            status: SkillLearningProposalStatus::Proposed,
            now_ms,
        })?;
        crate::LearningReviewReport {
            schema: "agent-harness.learning-review.v1",
            harness_home: harness_home.to_path_buf(),
            status: "proposed".to_string(),
            proposals_created: 1,
            proposal_ids: vec![proposal.proposal_id],
            reason: "self-improvement replacement proposal recorded".to_string(),
        }
    } else {
        run_learning_review(LearningReviewOptions {
            harness_home: harness_home.to_path_buf(),
            agent_id: string_path_any(&job.payload, &["agentId", "agent_id"])
                .map(ToString::to_string),
            target_skill_id,
            target_path,
            channel_trust: string_path_any(&job.payload, &["channelTrust", "channel_trust"])
                .map(ToString::to_string),
            signal_text: signal_text.clone(),
            source_turn: source_turn.clone(),
            daily_cap: usize_payload(&job.payload, &["dailyCap", "daily_cap"], 5),
            now_ms,
        })?
    };

    let mut apply_reports = Vec::new();
    if mode == SelfImprovementReviewMode::DispatchAndReplace && replacement_requested {
        for proposal_id in &report.proposal_ids {
            let apply = apply_skill_proposal(SkillApplyOptions {
                harness_home: harness_home.to_path_buf(),
                proposal_id: proposal_id.clone(),
                operator: Some("self-improvement-review".to_string()),
                now_ms,
            })?;
            apply_reports.push(serde_json::to_value(&apply).map_err(io::Error::other)?);
            if bool_payload(&job.payload, &["notify"], true)
                && apply.status == crate::SkillApplyStatus::Applied
                && let Some(target) = notification_target_from_payload(&job.payload)
            {
                let skill_id = string_path_any(&job.payload, &["targetSkillId", "target_skill_id"])
                    .unwrap_or("unknown");
                let text = format!(
                    "Self-improvement review: Patched SKILL.md in skill '{}' (1 replacement).",
                    skill_id
                );
                let _ = append_self_improvement_notification(harness_home, &target, text);
            }
        }
    } else if bool_payload(
        &job.payload,
        &["notifyProposals", "notify_proposals"],
        mode == SelfImprovementReviewMode::ProposeOnly,
    ) && report.proposals_created > 0
        && let Some(target) = notification_target_from_payload(&job.payload)
    {
        let skill_id = string_path_any(&job.payload, &["targetSkillId", "target_skill_id"])
            .unwrap_or("unknown");
        let text = format!(
            "Self-improvement review: Recorded proposal for skill '{}' ({} proposal).",
            skill_id, report.proposals_created
        );
        let _ = append_self_improvement_notification(harness_home, &target, text);
    }

    Ok(WorkerJobExecutionResult {
        status: WorkerJobStatus::Succeeded,
        reason: report.reason.clone(),
        audit_path: Some(skill_learning_audit_path(harness_home)),
        artifact_refs: Some(json!([format!(
            "skill-proposals:{}",
            crate::skill_proposals_file(harness_home).display()
        )])),
        result: Some(json!({
            "review": report,
            "mode": mode.as_str(),
            "applyReports": apply_reports,
        })),
    })
}

fn run_skill_synthesis_job(
    harness_home: &Path,
    job: &WorkerJob,
    now_ms: i64,
) -> io::Result<WorkerJobExecutionResult> {
    let skill_id = string_path_any(&job.payload, &["skillId", "skill_id", "targetSkillId"])
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "skill synthesis job requires skillId",
            )
        })?
        .to_string();
    let task_summary = string_path_any(
        &job.payload,
        &["taskSummary", "task_summary", "summary", "text"],
    )
    .ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "skill synthesis job requires taskSummary or summary",
        )
    })?
    .to_string();
    let evidence = string_path_any(&job.payload, &["evidence", "signalText", "signal_text"])
        .unwrap_or("")
        .to_string();
    let propose_only = bool_payload(&job.payload, &["proposeOnly", "propose_only"], false);
    let report = synthesize_skill(SkillSynthesisOptions {
        harness_home: harness_home.to_path_buf(),
        skill_id,
        task_summary,
        evidence,
        propose_only,
        now_ms,
    })?;
    let apply_decision = report.autonomous_apply.as_ref().map(|apply| apply.decision);
    let apply_status = report
        .autonomous_apply
        .as_ref()
        .map(|apply| apply.apply_report.status);
    let applied_path = report.target_path.clone();
    Ok(WorkerJobExecutionResult {
        status: WorkerJobStatus::Succeeded,
        reason: if report.autonomous_apply.is_some() {
            "skill synthesis autonomously reviewed and applied proposal".to_string()
        } else {
            "skill synthesis recorded proposal without apply".to_string()
        },
        audit_path: Some(skill_learning_audit_path(harness_home)),
        artifact_refs: Some(json!({
            "proposalId": report.proposal.proposal_id,
            "targetPath": applied_path,
            "synthesisReceipts": report.receipts_file,
            "proposals": crate::skill_proposals_file(harness_home),
            "autonomousApplyReceipts": crate::skill_autonomous_apply_receipts_file(harness_home),
        })),
        result: Some(json!({
            "synthesis": report,
            "autonomousApplyDecision": apply_decision,
            "applyStatus": apply_status,
        })),
    })
}

fn skill_learning_audit_path(harness_home: &Path) -> PathBuf {
    harness_home.join("state").join("learning")
}

fn run_memory_embedding_backfill_job(
    harness_home: &Path,
    job: &WorkerJob,
    now_ms: i64,
) -> io::Result<WorkerJobExecutionResult> {
    let lane = string_path_any(&job.payload, &["lane", "memoryLane", "backfillLane"])
        .unwrap_or("episodic_events")
        .parse::<MemoryEmbeddingBackfillLane>()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    let model = string_path_any(&job.payload, &["model", "embeddingModel"])
        .unwrap_or("text-embedding-3-small")
        .to_string();
    let report = run_memory_embedding_backfill(MemoryEmbeddingBackfillOptions {
        harness_home: harness_home.to_path_buf(),
        lane,
        model,
        vector_dimension: i64_payload(
            &job.payload,
            &["vectorDimension", "vector_dimension", "dim"],
            DEFAULT_MEMORY_BACKFILL_VECTOR_DIMENSION,
        ),
        batch_size: usize_payload(
            &job.payload,
            &["batchSize", "batch_size"],
            DEFAULT_MEMORY_BACKFILL_BATCH_SIZE,
        ),
        max_items: usize_payload(
            &job.payload,
            &["maxItems", "max_items"],
            DEFAULT_MEMORY_BACKFILL_MAX_ITEMS,
        ),
        rate_limit_per_minute: usize_payload(
            &job.payload,
            &["rateLimitPerMinute", "rate_limit_per_minute"],
            DEFAULT_MEMORY_BACKFILL_RATE_LIMIT_PER_MINUTE,
        ),
        retry_cap: usize_payload(
            &job.payload,
            &["retryCap", "retry_cap"],
            DEFAULT_MEMORY_BACKFILL_RETRY_CAP,
        ),
        coverage_threshold_bps: u64_payload(
            &job.payload,
            &["coverageThresholdBps", "coverage_threshold_bps"],
            DEFAULT_MEMORY_BACKFILL_COVERAGE_THRESHOLD_BPS,
        ),
        now_ms,
    })?;
    Ok(WorkerJobExecutionResult {
        status: WorkerJobStatus::Succeeded,
        reason: format!("memory embedding backfill {}", report.status),
        audit_path: None,
        artifact_refs: Some(json!({
            "cursorFile": report.cursor_file,
            "receiptFile": report.receipt_file,
            "latestFile": report.latest_file,
        })),
        result: Some(serde_json::to_value(&report).map_err(io::Error::other)?),
    })
}

fn run_deterministic_shell_job(
    harness_home: &Path,
    job: &WorkerJob,
    config: &WorkerDispatchConfig,
) -> io::Result<WorkerJobExecutionResult> {
    let script_path = string_path(&job.payload, "scriptPath")
        .or_else(|| string_path(&job.payload, "script"))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "deterministic shell payload requires scriptPath",
            )
        })?;
    let script = PathBuf::from(script_path);
    let cwd = string_path(&job.payload, "cwd")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            script
                .parent()
                .map(Path::to_path_buf)
                .unwrap_or_else(|| harness_home.to_path_buf())
        });
    let dry_run = job
        .payload
        .get("dryRun")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let argv = string_array_path(&job.payload, "argv");
    let timeout_ms = job.timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS);
    let audit_dir = harness_home.join("state").join("workers").join("audit");
    fs::create_dir_all(&audit_dir)?;
    let audit_path = audit_dir.join(format!(
        "{}-attempt-{}.json",
        safe_file_part(&job.job_id),
        job.attempt
    ));

    let allowed = dry_run || script_allowed(&script, config);
    if !allowed {
        let audit = json!({
            "schema": "agent-harness.worker-shell-audit.v1",
            "status": "blocked",
            "scriptPath": script,
            "reason": "script path is outside allowed workerDispatch.allowedScriptRoots"
        });
        fs::write(
            &audit_path,
            serde_json::to_vec_pretty(&audit).map_err(io::Error::other)?,
        )?;
        return Ok(WorkerJobExecutionResult {
            status: WorkerJobStatus::FailedTerminal,
            reason: "deterministic shell script blocked by allow-list policy".to_string(),
            audit_path: Some(audit_path.clone()),
            artifact_refs: Some(json!({"auditPath": audit_path})),
            result: Some(audit),
        });
    }

    if dry_run {
        let audit = json!({
            "schema": "agent-harness.worker-shell-audit.v1",
            "status": "dry-run",
            "scriptPath": script,
            "argv": argv,
            "cwd": cwd,
            "reason": "dryRun payload requested no process execution"
        });
        fs::write(
            &audit_path,
            serde_json::to_vec_pretty(&audit).map_err(io::Error::other)?,
        )?;
        return Ok(WorkerJobExecutionResult {
            status: WorkerJobStatus::Succeeded,
            reason: "deterministic shell dry-run recorded".to_string(),
            audit_path: Some(audit_path.clone()),
            artifact_refs: Some(json!({"auditPath": audit_path})),
            result: Some(audit),
        });
    }

    let started = epoch_ms()?;
    let mut command = shell_command_for(&script, &argv);
    command.current_dir(&cwd);
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    if let Some(env_object) = job.payload.get("env").and_then(Value::as_object) {
        for (key, value) in env_object {
            if is_env_key_allowed(key)
                && let Some(value) = value.as_str()
            {
                command.env(key, value);
            }
        }
    }

    let mut child = command.spawn().map_err(io::Error::other)?;
    let timed_out = loop {
        if child.try_wait().map_err(io::Error::other)?.is_some() {
            break false;
        }
        if epoch_ms()?.saturating_sub(started) > i64::try_from(timeout_ms).unwrap_or(i64::MAX) {
            let _ = child.kill();
            break true;
        }
        thread::sleep(Duration::from_millis(50));
    };
    let output = child.wait_with_output().map_err(io::Error::other)?;
    let ended = epoch_ms()?;
    let stdout = capped_string(&output.stdout);
    let stderr = capped_string(&output.stderr);
    let exit_code = output.status.code();
    let status = if !timed_out && output.status.success() {
        WorkerJobStatus::Succeeded
    } else if job.attempt < job.max_attempts {
        WorkerJobStatus::FailedRetryable
    } else {
        WorkerJobStatus::FailedTerminal
    };
    let audit = json!({
        "schema": "agent-harness.worker-shell-audit.v1",
        "status": status.as_str(),
        "scriptPath": script,
        "argv": argv,
        "cwd": cwd,
        "exitCode": exit_code,
        "timedOut": timed_out,
        "durationMs": ended.saturating_sub(started),
        "stdoutPreview": stdout,
        "stderrPreview": stderr,
        "stdoutSha256Unavailable": true,
        "stderrSha256Unavailable": true
    });
    fs::write(
        &audit_path,
        serde_json::to_vec_pretty(&audit).map_err(io::Error::other)?,
    )?;
    let reason = if status == WorkerJobStatus::Succeeded {
        "deterministic shell job completed".to_string()
    } else if timed_out {
        "deterministic shell job timed out".to_string()
    } else {
        format!("deterministic shell job exited with code {:?}", exit_code)
    };
    let reason = if status == WorkerJobStatus::FailedTerminal && job.attempt >= job.max_attempts {
        format!("{reason}; {}", exhausted_retry_stop_reason(job))
    } else {
        reason
    };
    Ok(WorkerJobExecutionResult {
        status,
        reason,
        audit_path: Some(audit_path.clone()),
        artifact_refs: Some(json!({"auditPath": audit_path})),
        result: Some(audit),
    })
}

fn queue_llm_worker_turn(
    harness_home: &Path,
    job: &WorkerJob,
    now_ms: i64,
) -> io::Result<WorkerJobExecutionResult> {
    let agent_id = required_payload_string(&job.payload, "agentId")?;
    let session_key = string_path(&job.payload, "sessionKey")
        .or(job.master_session_key.as_deref())
        .unwrap_or("worker-session");
    let message_text = required_payload_string(&job.payload, "messageText")?;
    let source_home = required_payload_path(&job.payload, "sourceHome")?;
    let source_workspace = required_payload_path(&job.payload, "sourceWorkspace")?;
    let runtime_workspace = string_path(&job.payload, "runtimeWorkspace").map(PathBuf::from);
    let platform = string_path(&job.payload, "platform").unwrap_or("worker");
    let runtime_class = string_path(&job.payload, "runtimeClass")
        .or_else(|| string_path(&job.payload, "runtime_class"))
        .map(ToString::to_string)
        .unwrap_or_else(|| {
            if platform == "native-cron" {
                "cron".to_string()
            } else {
                "worker".to_string()
            }
        });
    let origin = string_path(&job.payload, "origin")
        .or_else(|| string_path(&job.payload, "adapter"))
        .unwrap_or("worker")
        .to_string();
    let cron_run_id = string_path(&job.payload, "cronRunId")
        .or_else(|| string_path(&job.payload, "cron_run_id"))
        .map(ToString::to_string);
    let scheduled_for_ms = int_path(&job.payload, "scheduledForMs")
        .or_else(|| int_path(&job.payload, "scheduled_for_ms"));
    let channel_id = string_path(&job.payload, "channelId").unwrap_or("worker");
    let user_id = string_path(&job.payload, "userId").unwrap_or("worker-dispatch");
    let managed_child_policy = job
        .child_policy
        .as_ref()
        .filter(|policy| policy.is_managed_route());
    let queue_dir = harness_home.join("state").join("runtime-queue");
    fs::create_dir_all(&queue_dir)?;
    let queue_file = queue_dir.join("pending.jsonl");
    let file_safe_session = normalize_key_part(session_key);
    let sessions_dir = if runtime_class == "cron" {
        harness_home
            .join("agents")
            .join(agent_id)
            .join("cron-sessions")
    } else {
        harness_home.join("agents").join(agent_id).join("sessions")
    };
    fs::create_dir_all(&sessions_dir)?;
    let queue_id = format!(
        "worker:{}:{}:{}",
        job.created_at_ms,
        safe_file_part(&job.job_id),
        fnv1a_64_hex(message_text)
    );
    let item = RuntimeQueueItem {
        schema: "agent-harness.runtime-queue-item.v1",
        queue_id: queue_id.clone(),
        status: RuntimeQueueItemStatus::Queued,
        runtime_class: runtime_class.clone(),
        origin,
        cron_run_id: cron_run_id.clone(),
        scheduled_for_ms,
        source: RuntimeQueueSource {
            kind: RuntimeQueueSourceKind::Channel,
            source_home,
            source_workspace,
            runtime_workspace,
        },
        created_at_ms: now_ms,
        agent_id: agent_id.to_string(),
        session_key: session_key.to_string(),
        platform: platform.to_string(),
        account_id: None,
        channel_id: channel_id.to_string(),
        user_id: user_id.to_string(),
        message_text: message_text.to_string(),
        inbound_context: string_path(&job.payload, "inboundContext").map(ToString::to_string),
        inbound_canonical_id: None,
        inbound_media_artifacts: Vec::new(),
        provider: managed_child_policy
            .and_then(ChildExecutionPolicyV1::provider)
            .map(ToString::to_string)
            .or_else(|| string_path(&job.payload, "provider").map(ToString::to_string)),
        model: managed_child_policy
            .and_then(ChildExecutionPolicyV1::model)
            .map(ToString::to_string)
            .or_else(|| string_path(&job.payload, "model").map(ToString::to_string)),
        reasoning_preference: managed_child_policy
            .and_then(ChildExecutionPolicyV1::reasoning_preference)
            .cloned(),
        backend_reasoning_policy: managed_child_policy
            .and_then(ChildExecutionPolicyV1::backend_reasoning_policy)
            .cloned(),
        prompt_files_present: 0,
        prompt_files_total: 0,
        selected_skill_ids: Vec::new(),
        planned_transcript_file: sessions_dir.join(format!("{file_safe_session}.jsonl")),
        planned_trajectory_file: sessions_dir.join(format!("{file_safe_session}.trajectory.jsonl")),
        continuation: crate::RuntimeContinuationMetadata::legacy(),
    };
    append_runtime_queue_item_if_missing(&queue_file, &item)?;
    if let Some(cron_run_id) = cron_run_id.as_deref() {
        mark_cron_run_runtime_enqueued(harness_home, cron_run_id, &queue_id, now_ms)?;
    }
    record_llm_subagent_lifecycle_running(harness_home, job, &queue_id, now_ms)?;
    Ok(WorkerJobExecutionResult {
        status: WorkerJobStatus::RuntimeQueued,
        reason: "LLM worker job durably queued; awaiting correlated runtime terminal".to_string(),
        audit_path: None,
        artifact_refs: Some(json!({
            "runtimeQueueFile": queue_file,
            "runtimeQueueId": queue_id,
            "runtimeClass": runtime_class,
            "cronRunId": cron_run_id,
            "transcriptFile": item.planned_transcript_file,
            "trajectoryFile": item.planned_trajectory_file
        })),
        result: Some(json!({"runtimeQueueId": queue_id})),
    })
}

fn ensure_llm_subagent_lifecycle_on_idempotency_hit(
    harness_home: &Path,
    existing: &WorkerJob,
    now_ms: i64,
) -> io::Result<()> {
    if existing.kind != WorkerJobKind::LlmSubagent {
        return Ok(());
    }
    let Some(subagent_id) = subagent_id_from_worker_payload(&existing.payload) else {
        return Ok(());
    };
    let current = show_subagent_lifecycle(SubagentLifecycleShowOptions {
        harness_home: harness_home.to_path_buf(),
        subagent_id: subagent_id.clone(),
        now_ms,
    })?;
    if current.receipt.state != SubagentLifecycleState::Unknown {
        return Ok(());
    }
    record_llm_subagent_lifecycle_queued(
        harness_home,
        &existing.kind,
        &existing.payload,
        existing,
        now_ms,
    )
}

fn record_llm_subagent_lifecycle_queued(
    harness_home: &Path,
    kind: &WorkerJobKind,
    payload: &Value,
    job: &WorkerJob,
    now_ms: i64,
) -> io::Result<()> {
    if *kind != WorkerJobKind::LlmSubagent {
        return Ok(());
    }
    let Some(subagent_id) = subagent_id_from_worker_payload(payload) else {
        return Ok(());
    };
    record_subagent_lifecycle(SubagentLifecycleRecordOptions {
        harness_home: harness_home.to_path_buf(),
        subagent_id,
        state: SubagentLifecycleState::Queued,
        source: subagent_lifecycle_source(payload, job.source.as_deref()),
        operation_plan_id: string_path_any(payload, &["operationPlanId", "operation_plan_id"])
            .map(ToString::to_string),
        operation_plan_item_id: string_path_any(
            payload,
            &["operationPlanItemId", "operation_plan_item_id"],
        )
        .map(ToString::to_string),
        worker_job_id: Some(job.job_id.clone()),
        runtime_queue_id: None,
        requested_model: string_path_any(payload, &["requestedModel", "model"])
            .map(ToString::to_string),
        resolved_model: string_path_any(payload, &["resolvedModel", "resolved_model"])
            .or_else(|| string_path(payload, "model"))
            .map(ToString::to_string),
        provider: string_path(payload, "provider").map(ToString::to_string),
        auth_lane: string_path_any(payload, &["authLane", "auth_lane"]).map(ToString::to_string),
        changed_files: Vec::new(),
        terminal_receipt_file: None,
        reason: "LLM subagent worker job queued before runtime dispatch".to_string(),
        now_ms,
    })?;
    Ok(())
}

fn record_llm_subagent_lifecycle_running(
    harness_home: &Path,
    job: &WorkerJob,
    runtime_queue_id: &str,
    now_ms: i64,
) -> io::Result<()> {
    if job.kind != WorkerJobKind::LlmSubagent {
        return Ok(());
    }
    let Some(subagent_id) = subagent_id_from_worker_payload(&job.payload) else {
        return Ok(());
    };
    record_subagent_lifecycle(SubagentLifecycleRecordOptions {
        harness_home: harness_home.to_path_buf(),
        subagent_id,
        state: SubagentLifecycleState::Running,
        source: subagent_lifecycle_source(&job.payload, job.source.as_deref()),
        operation_plan_id: string_path_any(&job.payload, &["operationPlanId", "operation_plan_id"])
            .map(ToString::to_string),
        operation_plan_item_id: string_path_any(
            &job.payload,
            &["operationPlanItemId", "operation_plan_item_id"],
        )
        .map(ToString::to_string),
        worker_job_id: Some(job.job_id.clone()),
        runtime_queue_id: Some(runtime_queue_id.to_string()),
        requested_model: string_path_any(&job.payload, &["requestedModel", "model"])
            .map(ToString::to_string),
        resolved_model: string_path_any(&job.payload, &["resolvedModel", "resolved_model"])
            .or_else(|| string_path(&job.payload, "model"))
            .map(ToString::to_string),
        provider: string_path(&job.payload, "provider").map(ToString::to_string),
        auth_lane: string_path_any(&job.payload, &["authLane", "auth_lane"])
            .map(ToString::to_string),
        changed_files: Vec::new(),
        terminal_receipt_file: None,
        reason: "LLM worker job queued as durable runtime turn".to_string(),
        now_ms,
    })?;
    Ok(())
}

fn subagent_id_from_worker_payload(payload: &Value) -> Option<String> {
    if let Some(subagent_id) = string_path_any(payload, &["subagentId", "subagent_id"]) {
        return Some(subagent_id.to_string());
    }
    if let Some(run_id) = string_path_any(payload, &["runId", "run_id"]) {
        return Some(format!("subagent:{run_id}"));
    }
    let session_key = string_path(payload, "sessionKey")?;
    let mut parts = session_key.split(':');
    match (parts.next(), parts.next()) {
        (Some("subagent"), Some(run_id)) if !run_id.is_empty() => {
            Some(format!("subagent:{run_id}"))
        }
        _ => Some(session_key.to_string()),
    }
}

fn subagent_lifecycle_source(payload: &Value, job_source: Option<&str>) -> Option<String> {
    job_source
        .or_else(|| string_path(payload, "source"))
        .or_else(|| string_path(payload, "platform"))
        .map(ToString::to_string)
        .or_else(|| Some("worker-dispatch".to_string()))
}

fn run_watchdog_job(
    harness_home: &Path,
    conn: &Connection,
    job: &WorkerJob,
    now_ms: i64,
) -> io::Result<WorkerJobExecutionResult> {
    let group_id = job
        .job_group_id
        .as_deref()
        .or_else(|| string_path(&job.payload, "jobGroupId"))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "watchdog job requires jobGroupId or job_group_id",
            )
        })?;
    let children = group_children(conn, group_id, &job.job_id, now_ms)?;
    let policy = job
        .wake_policy
        .clone()
        .unwrap_or_else(|| json!({"mode":"all_completed"}));
    let mode = policy
        .get("mode")
        .and_then(Value::as_str)
        .unwrap_or("all_completed");
    let all_terminal =
        !children.is_empty() && children.iter().all(|child| child.status.is_terminal());
    let any_failed = children.iter().any(|child| {
        matches!(
            child.status,
            WorkerJobStatus::FailedTerminal | WorkerJobStatus::Expired | WorkerJobStatus::Canceled
        )
    });
    let timeout_fired = policy
        .get("deadlineMs")
        .and_then(Value::as_i64)
        .is_some_and(|deadline| now_ms >= deadline);
    let should_wake = match mode {
        "any_failed" => any_failed,
        "timeout" => timeout_fired,
        "all_succeeded" => {
            !children.is_empty()
                && children
                    .iter()
                    .all(|child| child.status == WorkerJobStatus::Succeeded)
        }
        _ => all_terminal || any_failed || timeout_fired,
    };

    let child_summary = children
        .iter()
        .map(|child| {
            json!({
                "jobId": child.job_id,
                "kind": child.kind.as_str(),
                "lane": child.lane,
                "status": child.status.as_str(),
                "attempt": child.attempt,
                "artifactRefs": child.artifact_refs,
                "auditPath": child.audit_path,
                "finishedAtMs": child.finished_at_ms
            })
        })
        .collect::<Vec<_>>();

    if should_wake {
        let wake_reason = if any_failed {
            "any_failed"
        } else if timeout_fired {
            "timeout"
        } else {
            mode
        };
        let payload = json!({
            "sourceHome": string_path(&job.payload, "sourceHome"),
            "sourceWorkspace": string_path(&job.payload, "sourceWorkspace"),
            "runtimeWorkspace": string_path(&job.payload, "runtimeWorkspace"),
            "agentId": job.master_agent_id.as_deref().or_else(|| string_path(&job.payload, "masterAgentId")).unwrap_or("main"),
            "sessionKey": job.master_session_key.as_deref().or_else(|| string_path(&job.payload, "masterSessionKey")).unwrap_or("master"),
            "platform": "worker-watchdog",
            "channelId": group_id,
            "userId": "worker-watchdog",
            "messageText": format_master_wakeup_message(group_id, wake_reason, &child_summary),
            "inboundContext": serde_json::to_string_pretty(&json!({
                "jobGroupId": group_id,
                "wakeReason": wake_reason,
                "children": child_summary
            })).unwrap_or_default()
        });
        let report = enqueue_worker_job(WorkerEnqueueOptions {
            harness_home: harness_home.to_path_buf(),
            kind: WorkerJobKind::MasterWakeup,
            lane: Some("llm".to_string()),
            payload,
            idempotency_key: Some(format!("watchdog-wakeup:{group_id}:{wake_reason}")),
            parent_job_id: Some(job.job_id.clone()),
            job_group_id: Some(group_id.to_string()),
            master_agent_id: job.master_agent_id.clone(),
            master_session_key: job.master_session_key.clone(),
            wake_policy: None,
            source: Some("watchdog".to_string()),
            priority: job.priority,
            available_at_ms: Some(now_ms),
            max_attempts: 3,
            timeout_ms: Some(DEFAULT_TIMEOUT_MS),
            cascade_timeout_ms: None,
            rate_key: None,
            concurrency_group_key: Some(format!("master:{group_id}")),
            now_ms,
        })?;
        return Ok(WorkerJobExecutionResult {
            status: WorkerJobStatus::Succeeded,
            reason: format!("watchdog fired {wake_reason} and enqueued master wakeup"),
            audit_path: None,
            artifact_refs: Some(json!({"masterWakeupJobId": report.job.job_id})),
            result: Some(json!({
                "wakeReason": wake_reason,
                "childCount": children.len(),
                "masterWakeupJobId": report.job.job_id
            })),
        });
    }

    let next_available = now_ms.saturating_add(backoff_ms(job.attempt));
    conn.execute(
        "UPDATE jobs SET status=?1, available_at_ms=?2, lease_owner=NULL, lease_expires_at_ms=NULL, updated_at_ms=?3, result_json=?4 WHERE job_id=?5",
        params![
            WorkerJobStatus::Pending.as_str(),
            next_available,
            now_ms,
            json!({"reason":"watchdog waiting for child boundary","childCount":children.len()}).to_string(),
            job.job_id,
        ],
    )
    .map_err(io::Error::other)?;
    Ok(WorkerJobExecutionResult {
        status: WorkerJobStatus::Pending,
        reason: "watchdog rescheduled; no wake policy boundary reached".to_string(),
        audit_path: None,
        artifact_refs: None,
        result: Some(json!({"childCount": children.len(), "nextAvailableAtMs": next_available})),
    })
}

fn exhausted_retry_stop_reason(job: &WorkerJob) -> String {
    format!(
        "retry attempts exhausted at attempt {} of {}",
        job.attempt, job.max_attempts
    )
}

fn normalize_exhausted_retryable_result(
    job: &WorkerJob,
    mut result: WorkerJobExecutionResult,
) -> WorkerJobExecutionResult {
    if result.status == WorkerJobStatus::FailedRetryable && job.attempt >= job.max_attempts {
        result.status = WorkerJobStatus::FailedTerminal;
        let stop_reason = exhausted_retry_stop_reason(job);
        if !result.reason.contains("retry attempts exhausted") {
            result.reason = format!("{}; {stop_reason}", result.reason);
        }
        if let Some(Value::Object(payload)) = result.result.as_mut() {
            payload.insert(
                "status".to_string(),
                Value::String(WorkerJobStatus::FailedTerminal.as_str().to_string()),
            );
            payload.insert("stopReason".to_string(), Value::String(stop_reason));
        }
    }
    result
}

fn persist_execution_result(
    conn: &Connection,
    job: &WorkerJob,
    result: &WorkerJobExecutionResult,
    now_ms: i64,
) -> io::Result<()> {
    let result = normalize_exhausted_retryable_result(job, result.clone());
    if result.status == WorkerJobStatus::Pending {
        return Ok(());
    }
    let status = if result.status == WorkerJobStatus::FailedRetryable {
        WorkerJobStatus::Pending
    } else {
        result.status
    };
    let finished_at = status.is_terminal().then_some(now_ms);
    let available_at = if status == WorkerJobStatus::Pending {
        now_ms.saturating_add(backoff_ms(job.attempt))
    } else {
        job.available_at_ms
    };
    conn.execute(
        "UPDATE jobs SET status=?1, available_at_ms=?2, lease_owner=NULL, lease_expires_at_ms=NULL, audit_path=?3, result_json=?4, artifact_refs_json=?5, updated_at_ms=?6, finished_at_ms=?7 WHERE job_id=?8",
        params![
            status.as_str(),
            available_at,
            result.audit_path.as_ref().map(|path| path.to_string_lossy().to_string()),
            result.result.as_ref().map(Value::to_string),
            result.artifact_refs.as_ref().map(Value::to_string),
            now_ms,
            finished_at,
            job.job_id,
        ],
    )
    .map_err(io::Error::other)?;
    Ok(())
}

fn set_job_status(
    conn: &Connection,
    job_id: &str,
    status: WorkerJobStatus,
    now_ms: i64,
    result: Option<Value>,
    artifact_refs: Option<Value>,
    audit_path: Option<PathBuf>,
) -> io::Result<()> {
    conn.execute(
        "UPDATE jobs SET status=?1, lease_owner=NULL, lease_expires_at_ms=NULL, result_json=COALESCE(?2,result_json), artifact_refs_json=COALESCE(?3,artifact_refs_json), audit_path=COALESCE(?4,audit_path), updated_at_ms=?5, finished_at_ms=CASE WHEN ?6 THEN ?5 ELSE finished_at_ms END WHERE job_id=?7",
        params![
            status.as_str(),
            result.map(|value| value.to_string()),
            artifact_refs.map(|value| value.to_string()),
            audit_path.map(|path| path.to_string_lossy().to_string()),
            now_ms,
            status.is_terminal(),
            job_id,
        ],
    )
    .map_err(io::Error::other)?;
    Ok(())
}

fn worker_totals(conn: &Connection, lane: Option<&str>) -> io::Result<WorkerStatusTotals> {
    let jobs = if let Some(lane) = lane {
        let mut stmt = conn
            .prepare("SELECT status FROM jobs WHERE lane=?1")
            .map_err(io::Error::other)?;
        stmt.query_map(params![lane], |row| row.get::<_, String>(0))
            .map_err(io::Error::other)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(io::Error::other)?
    } else {
        let mut stmt = conn
            .prepare("SELECT status FROM jobs")
            .map_err(io::Error::other)?;
        stmt.query_map([], |row| row.get::<_, String>(0))
            .map_err(io::Error::other)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(io::Error::other)?
    };
    let mut totals = WorkerStatusTotals {
        total: jobs.len(),
        ..WorkerStatusTotals::default()
    };
    for status in jobs {
        match WorkerJobStatus::from_db(&status) {
            WorkerJobStatus::Pending => totals.pending += 1,
            WorkerJobStatus::Leased => totals.leased += 1,
            WorkerJobStatus::Running => totals.running += 1,
            WorkerJobStatus::RuntimeQueued => totals.runtime_queued += 1,
            WorkerJobStatus::Succeeded => totals.succeeded += 1,
            WorkerJobStatus::FailedRetryable => totals.failed_retryable += 1,
            WorkerJobStatus::FailedTerminal => totals.failed_terminal += 1,
            WorkerJobStatus::Canceled => totals.canceled += 1,
            WorkerJobStatus::Expired => totals.expired += 1,
        }
    }
    Ok(totals)
}

fn worker_lanes(conn: &Connection) -> io::Result<Vec<String>> {
    let mut stmt = conn
        .prepare("SELECT DISTINCT lane FROM jobs ORDER BY lane")
        .map_err(io::Error::other)?;
    stmt.query_map([], |row| row.get(0))
        .map_err(io::Error::other)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(io::Error::other)
}

#[derive(Debug, Clone)]
struct RuntimeWorkerTerminal {
    worker_status: WorkerJobStatus,
    runtime_status: String,
    reason: String,
}

fn reconcile_runtime_queued_jobs(
    harness_home: &Path,
    conn: &Connection,
    now_ms: i64,
) -> io::Result<usize> {
    let runtime_queued: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM jobs WHERE status=?1",
            params![WorkerJobStatus::RuntimeQueued.as_str()],
            |row| row.get(0),
        )
        .map_err(io::Error::other)?;
    if runtime_queued == 0 {
        return Ok(0);
    }

    let receipts_file = harness_home
        .join("state")
        .join("runtime-queue")
        .join("run-once-receipts.jsonl");
    let receipts = match fs::read_to_string(&receipts_file) {
        Ok(receipts) => receipts,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error),
    };
    let mut terminals = BTreeMap::<String, RuntimeWorkerTerminal>::new();
    for line in receipts.lines().filter(|line| !line.trim().is_empty()) {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let Some(queue_id) = string_path_any(&value, &["queueId", "queue_id"]) else {
            continue;
        };
        let Some(runtime_status) = string_path(&value, "status") else {
            continue;
        };
        let Some(worker_status) = worker_status_from_runtime_terminal(runtime_status) else {
            continue;
        };
        terminals.insert(
            queue_id.to_string(),
            RuntimeWorkerTerminal {
                worker_status,
                runtime_status: runtime_status.to_string(),
                reason: string_path(&value, "reason")
                    .unwrap_or("runtime terminal receipt recorded")
                    .to_string(),
            },
        );
    }
    if terminals.is_empty() {
        return Ok(0);
    }

    let jobs = {
        let mut stmt = conn
            .prepare("SELECT * FROM jobs WHERE status=?1 ORDER BY created_at_ms ASC")
            .map_err(io::Error::other)?;
        stmt.query_map(params![WorkerJobStatus::RuntimeQueued.as_str()], row_to_job)
            .map_err(io::Error::other)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(io::Error::other)?
    };

    let mut reconciled = 0;
    for job in jobs {
        let Some(runtime_queue_id) = runtime_queue_id_from_worker_job(&job) else {
            continue;
        };
        let Some(terminal) = terminals.get(runtime_queue_id) else {
            continue;
        };
        set_job_status(
            conn,
            &job.job_id,
            terminal.worker_status,
            now_ms,
            Some(json!({
                "runtimeQueueId": runtime_queue_id,
                "runtimeStatus": terminal.runtime_status,
                "reason": terminal.reason,
                "terminalReceiptFile": receipts_file,
                "correlation": "exact-runtime-queue-id"
            })),
            None,
            Some(receipts_file.clone()),
        )?;
        reconciled += 1;
    }
    Ok(reconciled)
}

fn worker_status_from_runtime_terminal(status: &str) -> Option<WorkerJobStatus> {
    match status {
        "completed" => Some(WorkerJobStatus::Succeeded),
        "canceled" => Some(WorkerJobStatus::Canceled),
        "dead-letter" | "failed-terminal" | "no-runtime-plan" | "preflight-blocked"
        | "spawn-failed" | "protocol-error" | "context-exhausted" | "timeout" => {
            Some(WorkerJobStatus::FailedTerminal)
        }
        _ => None,
    }
}

fn runtime_queue_id_from_worker_job(job: &WorkerJob) -> Option<&str> {
    job.artifact_refs
        .as_ref()
        .and_then(|value| string_path_any(value, &["runtimeQueueId", "runtime_queue_id"]))
        .or_else(|| {
            job.result
                .as_ref()
                .and_then(|value| string_path_any(value, &["runtimeQueueId", "runtime_queue_id"]))
        })
}

fn jobs_with_expired_leases(conn: &Connection, now_ms: i64) -> io::Result<Vec<WorkerJob>> {
    let rows = {
        let mut stmt = conn
            .prepare("SELECT * FROM jobs WHERE status IN ('leased','running') AND lease_expires_at_ms IS NOT NULL AND lease_expires_at_ms < ?1")
            .map_err(io::Error::other)?;
        stmt.query_map(params![now_ms], row_to_job_tolerating_invalid_child_policy)
            .map_err(io::Error::other)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(io::Error::other)?
    };
    resolve_tolerant_worker_rows(conn, rows, now_ms, false)
}

fn group_children(
    conn: &Connection,
    group_id: &str,
    self_id: &str,
    now_ms: i64,
) -> io::Result<Vec<WorkerJob>> {
    let rows = {
        let mut stmt = conn
            .prepare(
                "SELECT * FROM jobs WHERE job_group_id=?1 AND job_id<>?2 ORDER BY created_at_ms ASC",
            )
            .map_err(io::Error::other)?;
        stmt.query_map(
            params![group_id, self_id],
            row_to_job_tolerating_invalid_child_policy,
        )
        .map_err(io::Error::other)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(io::Error::other)?
    };
    resolve_tolerant_worker_rows(conn, rows, now_ms, true)
}

enum TolerantWorkerRow {
    Job(WorkerJob),
    InvalidChildPolicy(String),
}

fn row_to_job_tolerating_invalid_child_policy(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<TolerantWorkerRow> {
    let job_id: String = row.get("job_id")?;
    let child_policy_text: Option<String> = row.get("child_policy_json")?;
    if stored_child_policy_is_invalid(child_policy_text.as_deref()) {
        Ok(TolerantWorkerRow::InvalidChildPolicy(job_id))
    } else {
        row_to_job(row).map(TolerantWorkerRow::Job)
    }
}

fn resolve_tolerant_worker_rows(
    conn: &Connection,
    rows: Vec<TolerantWorkerRow>,
    now_ms: i64,
    include_quarantined: bool,
) -> io::Result<Vec<WorkerJob>> {
    let mut jobs = Vec::with_capacity(rows.len());
    for row in rows {
        match row {
            TolerantWorkerRow::Job(job) => jobs.push(job),
            TolerantWorkerRow::InvalidChildPolicy(job_id) => {
                quarantine_invalid_stored_child_policy(conn, &job_id, now_ms)?;
                if include_quarantined && let Some(job) = find_job_by_id(conn, &job_id)? {
                    jobs.push(job);
                }
            }
        }
    }
    Ok(jobs)
}

fn row_to_job(row: &rusqlite::Row<'_>) -> rusqlite::Result<WorkerJob> {
    let kind_text: String = row.get("kind")?;
    let status_text: String = row.get("status")?;
    let payload_text: String = row.get("payload_json")?;
    let child_policy_text: Option<String> = row.get("child_policy_json")?;
    let wake_policy_text: Option<String> = row.get("wake_policy_json")?;
    let result_text: Option<String> = row.get("result_json")?;
    let artifact_refs_text: Option<String> = row.get("artifact_refs_json")?;
    let timeout_ms: Option<i64> = row.get("timeout_ms")?;
    let cascade_timeout_ms: Option<i64> = row.get("cascade_timeout_ms")?;
    let audit_path: Option<String> = row.get("audit_path")?;
    Ok(WorkerJob {
        schema: WORKER_STORE_SCHEMA,
        job_id: row.get("job_id")?,
        kind: kind_text
            .parse()
            .unwrap_or(WorkerJobKind::DeterministicShell),
        lane: row.get("lane")?,
        status: WorkerJobStatus::from_db(&status_text),
        parent_job_id: row.get("parent_job_id")?,
        job_group_id: row.get("job_group_id")?,
        master_agent_id: row.get("master_agent_id")?,
        master_session_key: row.get("master_session_key")?,
        wake_policy: wake_policy_text.and_then(|text| serde_json::from_str(&text).ok()),
        source: row.get("source")?,
        payload: serde_json::from_str(&payload_text).unwrap_or(Value::Null),
        child_policy: child_policy_text
            .map(|text| {
                serde_json::from_str(&text).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Text,
                        Box::new(error),
                    )
                })
            })
            .transpose()?,
        idempotency_key: row.get("idempotency_key")?,
        priority: row.get("priority")?,
        available_at_ms: row.get("available_at_ms")?,
        lease_owner: row.get("lease_owner")?,
        lease_expires_at_ms: row.get("lease_expires_at_ms")?,
        attempt: row.get("attempt")?,
        max_attempts: row.get("max_attempts")?,
        timeout_ms: timeout_ms.and_then(|value| u64::try_from(value).ok()),
        cascade_timeout_ms: cascade_timeout_ms.and_then(|value| u64::try_from(value).ok()),
        rate_key: row.get("rate_key")?,
        concurrency_group_key: row.get("concurrency_group_key")?,
        audit_path: audit_path.map(PathBuf::from),
        result: result_text.and_then(|text| serde_json::from_str(&text).ok()),
        artifact_refs: artifact_refs_text.and_then(|text| serde_json::from_str(&text).ok()),
        created_at_ms: row.get("created_at_ms")?,
        updated_at_ms: row.get("updated_at_ms")?,
        finished_at_ms: row.get("finished_at_ms")?,
    })
}

fn value_to_nullable_string(value: &Value) -> io::Result<Option<String>> {
    if value.is_null() {
        Ok(None)
    } else {
        serde_json::to_string(value)
            .map(Some)
            .map_err(io::Error::other)
    }
}

fn append_json_line(path: &Path, value: &impl Serialize) -> io::Result<()> {
    crate::append_jsonl_value(path, value)
}

fn append_runtime_queue_item_if_missing(path: &Path, item: &RuntimeQueueItem) -> io::Result<()> {
    if runtime_queue_contains_id(path, &item.queue_id)? {
        return Ok(());
    }
    append_json_line(path, item)
}

fn runtime_queue_contains_id(path: &Path, queue_id: &str) -> io::Result<bool> {
    if !path.is_file() {
        return Ok(false);
    }
    let text = fs::read_to_string(path)?;
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
            continue;
        };
        if string_path_any(&value, &["queueId", "queue_id"]) == Some(queue_id) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn string_path<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value.get(key).and_then(Value::as_str)
}

fn string_path_any<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a str> {
    for key in keys {
        if let Some(text) = value.get(*key).and_then(Value::as_str) {
            return Some(text);
        }
    }
    None
}

fn int_path(value: &Value, key: &str) -> Option<i64> {
    value.get(key).and_then(Value::as_i64)
}

fn usize_payload(value: &Value, keys: &[&str], default: usize) -> usize {
    for key in keys {
        if let Some(parsed) = value
            .get(*key)
            .and_then(Value::as_u64)
            .and_then(|value| usize::try_from(value).ok())
        {
            return parsed;
        }
    }
    default
}

fn bool_payload(value: &Value, keys: &[&str], default: bool) -> bool {
    for key in keys {
        if let Some(parsed) = value.get(*key).and_then(Value::as_bool) {
            return parsed;
        }
    }
    default
}

fn u64_payload(value: &Value, keys: &[&str], default: u64) -> u64 {
    for key in keys {
        if let Some(parsed) = value.get(*key).and_then(Value::as_u64) {
            return parsed;
        }
    }
    default
}

fn i64_payload(value: &Value, keys: &[&str], default: i64) -> i64 {
    for key in keys {
        if let Some(parsed) = value.get(*key).and_then(Value::as_i64) {
            return parsed;
        }
    }
    default
}

fn self_improvement_mode_from_payload(value: &Value) -> SelfImprovementReviewMode {
    match string_path_any(value, &["mode", "applyMode", "apply_mode"])
        .unwrap_or("dispatch-and-replace")
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "propose" | "propose-only" | "propose-record-only" | "record-only" => {
            SelfImprovementReviewMode::ProposeOnly
        }
        _ => SelfImprovementReviewMode::DispatchAndReplace,
    }
}

fn notification_target_from_payload(value: &Value) -> Option<SelfImprovementNotificationTarget> {
    value
        .get("notificationTarget")
        .or_else(|| value.get("notification_target"))
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
}

fn stable_worker_text_hash(kind: &str, text: &str) -> String {
    let mut hash: u64 = 14_695_981_039_346_656_037u64;
    for byte in format!("{kind}\n{text}").as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(1_099_511_628_211);
    }
    format!("{hash:016x}")
}

fn string_array_path(value: &Value, key: &str) -> Vec<String> {
    value
        .get(key)
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(ToString::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn required_payload_string<'a>(value: &'a Value, key: &str) -> io::Result<&'a str> {
    string_path(value, key).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("worker payload requires {key}"),
        )
    })
}

fn required_payload_path(value: &Value, key: &str) -> io::Result<PathBuf> {
    required_payload_string(value, key).map(PathBuf::from)
}

fn shell_command_for(script: &Path, argv: &[String]) -> Command {
    let extension = script
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    if extension == "ps1" {
        let mut command = Command::new("powershell");
        command
            .arg("-NoProfile")
            .arg("-ExecutionPolicy")
            .arg("Bypass")
            .arg("-File")
            .arg(script);
        command.args(argv);
        command
    } else if matches!(extension.as_str(), "cmd" | "bat") {
        let mut command = Command::new("cmd");
        command.arg("/C").arg(script);
        command.args(argv);
        command
    } else {
        let mut command = Command::new(script);
        command.args(argv);
        command
    }
}

fn script_allowed(script: &Path, config: &WorkerDispatchConfig) -> bool {
    let script = absolutize_best_effort(script);
    config
        .allowed_script_roots
        .iter()
        .map(|path| absolutize_best_effort(path))
        .any(|root| script.starts_with(root))
}

fn absolutize_best_effort(path: &Path) -> PathBuf {
    fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

fn is_env_key_allowed(key: &str) -> bool {
    key.chars()
        .all(|ch| ch.is_ascii_uppercase() || ch.is_ascii_digit() || ch == '_')
        && !key.contains("TOKEN")
        && !key.contains("SECRET")
        && !key.contains("KEY")
        && !key.contains("PASSWORD")
}

fn capped_string(bytes: &[u8]) -> String {
    let capped = if bytes.len() > SHELL_OUTPUT_CAP_BYTES {
        &bytes[..SHELL_OUTPUT_CAP_BYTES]
    } else {
        bytes
    };
    String::from_utf8_lossy(capped).to_string()
}

fn format_master_wakeup_message(group_id: &str, reason: &str, child_summary: &[Value]) -> String {
    let counts = child_summary
        .iter()
        .fold(BTreeMap::new(), |mut counts, child| {
            let status = child
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string();
            *counts.entry(status).or_insert(0usize) += 1;
            counts
        });
    format!(
        "Worker group `{group_id}` reached wake policy `{reason}`. Child status counts: {}. Inspect artifact pointers in inbound context.",
        counts
            .iter()
            .map(|(status, count)| format!("{status}={count}"))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn safe_file_part(value: &str) -> String {
    let mut out = String::new();
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        "job".to_string()
    } else {
        out
    }
}

fn normalize_key_part(value: &str) -> String {
    let mut out = String::new();
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        "unknown".to_string()
    } else {
        out
    }
}

fn signal_worker_queue_wake(harness_home: &Path, lane: &str, reason: &str) {
    let wake_dir = harness_home.join("state").join("wake");
    let _ = crate::wake::signal_wake(harness_home, wake_dir.join("worker.json"), "worker", reason);

    let lane_key = normalize_key_part(lane);
    let lane_name = format!("worker-{lane_key}");
    let _ = crate::wake::signal_wake(
        harness_home,
        wake_dir.join(format!("{lane_name}.json")),
        &lane_name,
        reason,
    );
}

fn backoff_ms(attempt: i64) -> i64 {
    let exponent = attempt.clamp(0, 6) as u32;
    1_000_i64.saturating_mul(2_i64.saturating_pow(exponent))
}

fn fnv1a_64_hex(value: &str) -> String {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
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
    use crate::backend_reasoning::{
        BackendReasoningPolicyV1, BackendReasoningSource, ReasoningPreference,
    };
    use crate::child_execution_policy::{ChildExecutionPolicyV1, ChildExecutionPolicyV1Input};
    use crate::model_catalog::{
        REASONING_RESOLUTION_RECEIPT_SCHEMA_VERSION, ReasoningResolutionReceipt,
        ReasoningResolutionStatus,
    };

    const DEFAULT_LEASE_MS: i64 = 300_000;

    #[test]
    fn worker_enqueue_is_idempotent() {
        let root = temp_root("worker_enqueue_is_idempotent");
        let harness_home = root.join(".agent-harness");
        let first = enqueue_worker_job(shell_options(&harness_home, "same-key", 1000)).unwrap();
        let second = enqueue_worker_job(shell_options(&harness_home, "same-key", 1001)).unwrap();

        assert!(first.inserted);
        assert!(!second.inserted);
        assert_eq!(first.job.job_id, second.job.job_id);
        assert_eq!(
            crate::wake::read_wake_sequence(
                harness_home.join("state").join("wake").join("worker.json")
            )
            .unwrap(),
            2
        );
        assert_eq!(
            crate::wake::read_wake_sequence(
                harness_home
                    .join("state")
                    .join("wake")
                    .join("worker-shell.json")
            )
            .unwrap(),
            2
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn child_policy_column_migration_is_additive_and_idempotent() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute("CREATE TABLE jobs (job_id TEXT PRIMARY KEY)", [])
            .unwrap();

        ensure_child_policy_column(&conn).unwrap();
        ensure_child_policy_column(&conn).unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('jobs') WHERE name='child_policy_json'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn worker_status_reports_group_capacity_blocks() {
        let root = temp_root("worker_status_reports_group_capacity_blocks");
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            r#"{"workerDispatch":{"globalConcurrencyLimit":2,"groupConcurrencyLimit":1,"laneConcurrencyLimits":{"shell":2}}}"#,
        )
        .unwrap();
        let mut first = shell_options(&harness_home, "first", 1000);
        first.concurrency_group_key = Some("group-1".to_string());
        let mut second = shell_options(&harness_home, "second", 1001);
        second.concurrency_group_key = Some("group-1".to_string());
        enqueue_worker_job(first).unwrap();
        enqueue_worker_job(second).unwrap();
        let run = run_worker_once(WorkerRunOnceOptions {
            harness_home: harness_home.clone(),
            lane: Some("shell".to_string()),
            worker_id: "test-worker".to_string(),
            lease_ms: DEFAULT_LEASE_MS,
            now_ms: 1002,
        })
        .unwrap();
        assert_eq!(run.status, WorkerRunOnceStatus::Completed);
        let conn = Connection::open(worker_db_file(&harness_home)).unwrap();
        conn.execute(
            "UPDATE jobs SET status='running', finished_at_ms=NULL WHERE job_id=?1",
            params![run.job.unwrap().job_id],
        )
        .unwrap();

        let status = collect_worker_status(WorkerStatusOptions { harness_home }).unwrap();
        assert_eq!(status.blocked.blocked_by_group_limit, 1);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn downstream_runtime_status_treats_suppressed_run_once_as_terminal() {
        let root = temp_root("downstream_runtime_status_treats_suppressed_run_once_as_terminal");
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

        let status = collect_downstream_runtime_status(&harness_home).unwrap();

        assert_eq!(status.open_runtime_items, 0);
        assert_eq!(status.open_cron_runtime_items, 0);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn worker_status_reports_agent_channel_capacity_blocks() {
        let root = temp_root("worker_status_reports_agent_channel_capacity_blocks");
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            r#"{"workerDispatch":{"globalConcurrencyLimit":3,"groupConcurrencyLimit":3,"channelConcurrencyLimit":1,"laneConcurrencyLimits":{"shell":3}}}"#,
        )
        .unwrap();
        let mut first = shell_options(&harness_home, "first-channel", 1000);
        first.payload = json!({
            "scriptPath": "dry-run.cmd",
            "dryRun": true,
            "agentId": "main",
            "platform": "telegram",
            "channelId": "dm-42"
        });
        let mut second = shell_options(&harness_home, "second-channel", 1001);
        second.payload = first.payload.clone();
        enqueue_worker_job(first).unwrap();
        enqueue_worker_job(second).unwrap();
        let run = run_worker_once(WorkerRunOnceOptions {
            harness_home: harness_home.clone(),
            lane: Some("shell".to_string()),
            worker_id: "test-worker".to_string(),
            lease_ms: DEFAULT_LEASE_MS,
            now_ms: 1002,
        })
        .unwrap();
        assert_eq!(run.status, WorkerRunOnceStatus::Completed);
        let conn = Connection::open(worker_db_file(&harness_home)).unwrap();
        conn.execute(
            "UPDATE jobs SET status='running', finished_at_ms=NULL WHERE job_id=?1",
            params![run.job.unwrap().job_id],
        )
        .unwrap();

        let status = collect_worker_status(WorkerStatusOptions { harness_home }).unwrap();
        assert_eq!(status.blocked.blocked_by_channel_limit, 1);
        assert_eq!(status.blocked.blocked_by_group_limit, 0);
        assert_eq!(status.blocked.blocked_by_global_limit, 0);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn worker_rate_lease_blocks_same_rate_key_within_window() {
        let root = temp_root("worker_rate_lease_blocks_same_rate_key_within_window");
        let harness_home = root.join(".agent-harness");
        let source = root.join("source");
        let workspace = source.join("workspace");
        fs::create_dir_all(&harness_home).unwrap();
        fs::create_dir_all(&workspace).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            r#"{"workerDispatch":{"globalConcurrencyLimit":2,"groupConcurrencyLimit":2,"laneConcurrencyLimits":{"llm":2},"rateLeaseLimit":1,"rateLeaseWindowMs":60000}}"#,
        )
        .unwrap();
        enqueue_worker_job(llm_options(
            &harness_home,
            &source,
            &workspace,
            "first",
            1000,
        ))
        .unwrap();
        enqueue_worker_job(llm_options(
            &harness_home,
            &source,
            &workspace,
            "second",
            1001,
        ))
        .unwrap();

        let first = run_worker_once(WorkerRunOnceOptions {
            harness_home: harness_home.clone(),
            lane: Some("llm".to_string()),
            worker_id: "test-worker".to_string(),
            lease_ms: DEFAULT_LEASE_MS,
            now_ms: 1002,
        })
        .unwrap();
        assert_eq!(first.status, WorkerRunOnceStatus::Dispatched);

        let second = run_worker_once(WorkerRunOnceOptions {
            harness_home: harness_home.clone(),
            lane: Some("llm".to_string()),
            worker_id: "test-worker".to_string(),
            lease_ms: DEFAULT_LEASE_MS,
            now_ms: 1003,
        })
        .unwrap();
        assert_eq!(second.status, WorkerRunOnceStatus::NoWork);
        assert_eq!(second.blocked.blocked_by_rate_lease, 1);

        let later = run_worker_once(WorkerRunOnceOptions {
            harness_home,
            lane: Some("llm".to_string()),
            worker_id: "test-worker".to_string(),
            lease_ms: DEFAULT_LEASE_MS,
            now_ms: 61_100,
        })
        .unwrap();
        assert_eq!(later.status, WorkerRunOnceStatus::Dispatched);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn cron_llm_worker_queues_isolated_runtime_turn_and_updates_cron_run() {
        let root = temp_root("cron_llm_worker_queues_isolated_runtime_turn_and_updates_cron_run");
        let harness_home = root.join(".agent-harness");
        let source = root.join("source");
        let workspace = source.join("workspace");
        fs::create_dir_all(&workspace).unwrap();
        let cron_run = crate::admit_cron_run(crate::CronRunAdmitOptions {
            harness_home: harness_home.clone(),
            source_kind: "native-cron".to_string(),
            source_id: "source-1".to_string(),
            entry_id: "daily".to_string(),
            agent_id: "agent-a".to_string(),
            scheduled_for_ms: 42,
            runtime_class: "cron".to_string(),
            session_key: "cron:agent-a:daily:42".to_string(),
            session_policy: "one-shot".to_string(),
            max_attempts: 3,
            now_ms: 1000,
        })
        .unwrap();
        let mut options = llm_options(&harness_home, &source, &workspace, "cron-worker", 1002);
        options.payload = json!({
            "sourceHome": source,
            "sourceWorkspace": workspace,
            "agentId": "agent-a",
            "sessionKey": "cron:agent-a:daily:42",
            "messageText": "run daily cron",
            "runtimeClass": "cron",
            "origin": "cron-scheduler",
            "cronRunId": &cron_run.run_id,
            "scheduledForMs": 42,
            "platform": "native-cron",
            "sessionPolicy": "one-shot",
            "channelId": "daily",
            "userId": "cron-scheduler"
        });
        let enqueue = enqueue_worker_job(options).unwrap();
        crate::mark_cron_run_worker_enqueued(
            &harness_home,
            &cron_run.run_id,
            &enqueue.job.job_id,
            1001,
        )
        .unwrap();

        let run = run_worker_once(WorkerRunOnceOptions {
            harness_home: harness_home.clone(),
            lane: Some("llm".to_string()),
            worker_id: "test-worker".to_string(),
            lease_ms: DEFAULT_LEASE_MS,
            now_ms: 1003,
        })
        .unwrap();

        assert_eq!(run.status, WorkerRunOnceStatus::Dispatched);
        let queue_file = harness_home
            .join("state")
            .join("runtime-queue")
            .join("pending.jsonl");
        let queue_text = fs::read_to_string(&queue_file).unwrap();
        let item: Value = serde_json::from_str(queue_text.lines().next().unwrap()).unwrap();
        assert_eq!(item["runtimeClass"], "cron");
        assert_eq!(item["origin"], "cron-scheduler");
        assert_eq!(item["cronRunId"], cron_run.run_id.as_str());
        let planned_transcript_file: PathBuf =
            serde_json::from_value(item["plannedTranscriptFile"].clone()).unwrap();
        assert_eq!(
            planned_transcript_file,
            harness_home
                .join("agents")
                .join("agent-a")
                .join("cron-sessions")
                .join("cron_agent-a_daily_42.jsonl")
        );
        let runs = crate::collect_cron_run_summary(&harness_home).unwrap();
        let updated = runs.runs.first().unwrap();
        assert_eq!(updated.status, crate::CronRunStatus::RuntimeEnqueued);
        assert!(updated.runtime_queue_id.is_some());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn child_policy_preserves_heterogeneous_sibling_routes_in_runtime_queue() {
        let root = temp_root("child_policy_preserves_heterogeneous_sibling_routes");
        let harness_home = root.join(".agent-harness");
        let source = root.join("source");
        let workspace = source.join("workspace");
        fs::create_dir_all(&workspace).unwrap();

        let sol_policy = managed_child_policy(1, "openai", "gpt-5.6-sol", "max");
        let terra_policy = managed_child_policy(2, "openai", "gpt-5.6-terra", "ultra");
        let mut sol = llm_options(&harness_home, &source, &workspace, "sol-child", 1000);
        sol.rate_key = None;
        sol.payload["provider"] = json!("late-bound-provider");
        sol.payload["model"] = json!("late-bound-model");
        let mut terra = llm_options(&harness_home, &source, &workspace, "terra-child", 1001);
        terra.rate_key = None;
        terra.payload["provider"] = json!("late-bound-provider");
        terra.payload["model"] = json!("late-bound-model");

        let sol_job = enqueue_worker_job_with_policy(sol, sol_policy.clone()).job;
        let terra_job = enqueue_worker_job_with_policy(terra, terra_policy.clone()).job;
        assert_eq!(sol_job.child_policy.as_ref(), Some(&sol_policy));
        assert_eq!(terra_job.child_policy.as_ref(), Some(&terra_policy));

        for now_ms in [1002, 1003] {
            let run = run_worker_once(WorkerRunOnceOptions {
                harness_home: harness_home.clone(),
                lane: Some("llm".to_string()),
                worker_id: format!("test-worker-{now_ms}"),
                lease_ms: DEFAULT_LEASE_MS,
                now_ms,
            })
            .unwrap();
            assert_eq!(run.status, WorkerRunOnceStatus::Dispatched);
        }

        let queued = runtime_queue_values(&harness_home);
        assert_eq!(queued.len(), 2);
        assert_runtime_route(&queued, "session-sol-child", "gpt-5.6-sol", "max");
        assert_runtime_route(&queued, "session-terra-child", "gpt-5.6-terra", "ultra");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn child_policy_snapshot_overrides_later_payload_model_change() {
        let root = temp_root("child_policy_snapshot_overrides_later_payload_model_change");
        let harness_home = root.join(".agent-harness");
        let source = root.join("source");
        let workspace = source.join("workspace");
        fs::create_dir_all(&workspace).unwrap();
        let policy = managed_child_policy(7, "openai", "gpt-5.6-sol", "max");
        let mut options = llm_options(&harness_home, &source, &workspace, "immutable", 1000);
        options.rate_key = None;
        let report = enqueue_worker_job_with_policy(options, policy.clone());

        let mut duplicate = llm_options(&harness_home, &source, &workspace, "immutable", 1001);
        duplicate.payload["provider"] = json!("other-provider");
        duplicate.payload["model"] = json!("other-model");
        let duplicate = enqueue_worker_job_with_policy(
            duplicate,
            managed_child_policy(8, "openai", "gpt-5.6-terra", "ultra"),
        );
        assert!(!duplicate.inserted);
        assert_eq!(duplicate.job.child_policy.as_ref(), Some(&policy));

        let conn = Connection::open(worker_db_file(&harness_home)).unwrap();
        let changed_payload = json!({
            "sourceHome": source,
            "sourceWorkspace": workspace,
            "agentId": "main",
            "sessionKey": "session-immutable",
            "messageText": "run immutable",
            "provider": "other-provider",
            "model": "other-model"
        });
        conn.execute(
            "UPDATE jobs SET payload_json=?1 WHERE job_id=?2",
            params![changed_payload.to_string(), report.job.job_id],
        )
        .unwrap();

        let run = run_worker_once(WorkerRunOnceOptions {
            harness_home: harness_home.clone(),
            lane: Some("llm".to_string()),
            worker_id: "test-worker".to_string(),
            lease_ms: DEFAULT_LEASE_MS,
            now_ms: 1001,
        })
        .unwrap();
        assert_eq!(run.status, WorkerRunOnceStatus::Dispatched);
        assert_eq!(run.job.unwrap().child_policy, Some(policy));
        assert_runtime_route(
            &runtime_queue_values(&harness_home),
            "session-immutable",
            "gpt-5.6-sol",
            "max",
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn child_policy_snapshot_survives_retry_and_payload_repair() {
        let root = temp_root("child_policy_snapshot_survives_retry_and_payload_repair");
        let harness_home = root.join(".agent-harness");
        let source = root.join("source");
        let workspace = source.join("workspace");
        fs::create_dir_all(&workspace).unwrap();
        let policy = managed_child_policy(11, "openai", "gpt-5.6-terra", "ultra");
        let mut options = llm_options(&harness_home, &source, &workspace, "retry", 1000);
        options.payload = json!({
            "agentId": "main",
            "sessionKey": "session-retry",
            "messageText": "run retry"
        });
        options.rate_key = None;
        let report = enqueue_worker_job_with_policy(options, policy.clone());

        let failed = run_worker_once(WorkerRunOnceOptions {
            harness_home: harness_home.clone(),
            lane: Some("llm".to_string()),
            worker_id: "test-worker-first".to_string(),
            lease_ms: DEFAULT_LEASE_MS,
            now_ms: 1001,
        })
        .unwrap();
        assert_eq!(failed.status, WorkerRunOnceStatus::Rescheduled);
        assert_eq!(failed.job.unwrap().child_policy, Some(policy.clone()));

        let repaired_payload = json!({
            "sourceHome": source,
            "sourceWorkspace": workspace,
            "agentId": "main",
            "sessionKey": "session-retry",
            "messageText": "run retry",
            "provider": "other-provider",
            "model": "other-model"
        });
        let conn = Connection::open(worker_db_file(&harness_home)).unwrap();
        conn.execute(
            "UPDATE jobs SET payload_json=?1 WHERE job_id=?2",
            params![repaired_payload.to_string(), report.job.job_id],
        )
        .unwrap();

        let retried = run_worker_once(WorkerRunOnceOptions {
            harness_home: harness_home.clone(),
            lane: Some("llm".to_string()),
            worker_id: "test-worker-second".to_string(),
            lease_ms: DEFAULT_LEASE_MS,
            now_ms: 3001,
        })
        .unwrap();
        assert_eq!(retried.status, WorkerRunOnceStatus::Dispatched);
        assert_eq!(retried.job.unwrap().child_policy, Some(policy));
        assert_runtime_route(
            &runtime_queue_values(&harness_home),
            "session-retry",
            "gpt-5.6-terra",
            "ultra",
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn invalid_persisted_child_policy_is_terminalized_without_blocking_healthy_work() {
        let root = temp_root(
            "invalid_persisted_child_policy_is_terminalized_without_blocking_healthy_work",
        );
        let harness_home = root.join(".agent-harness");
        let source = root.join("source");
        let workspace = source.join("workspace");
        fs::create_dir_all(&workspace).unwrap();
        let mut invalid = llm_options(&harness_home, &source, &workspace, "invalid-policy", 1000);
        invalid.rate_key = None;
        let invalid_report = enqueue_worker_job_with_policy(
            invalid,
            managed_child_policy(1, "openai", "gpt-5.6-sol", "max"),
        );
        let conn = Connection::open(worker_db_file(&harness_home)).unwrap();
        conn.execute(
            "UPDATE jobs SET child_policy_json=?1 WHERE job_id=?2",
            params!["{not-json", invalid_report.job.job_id],
        )
        .unwrap();
        drop(conn);

        let mut healthy = llm_options(&harness_home, &source, &workspace, "healthy-policy", 1001);
        healthy.rate_key = None;
        let healthy_report = enqueue_worker_job_with_policy(
            healthy,
            managed_child_policy(2, "openai", "gpt-5.6-sol", "max"),
        );

        let run = run_worker_once(WorkerRunOnceOptions {
            harness_home: harness_home.clone(),
            lane: Some("llm".to_string()),
            worker_id: "test-worker".to_string(),
            lease_ms: DEFAULT_LEASE_MS,
            now_ms: 1002,
        })
        .unwrap();
        assert_eq!(run.status, WorkerRunOnceStatus::Dispatched);
        assert_eq!(
            run.job.as_ref().map(|job| job.job_id.as_str()),
            Some(healthy_report.job.job_id.as_str())
        );
        assert_runtime_route(
            &runtime_queue_values(&harness_home),
            "session-healthy-policy",
            "gpt-5.6-sol",
            "max",
        );

        let conn = Connection::open(worker_db_file(&harness_home)).unwrap();
        let (status, result_text, child_policy_text): (String, String, Option<String>) = conn
            .query_row(
                "SELECT status, result_json, child_policy_json FROM jobs WHERE job_id=?1",
                params![invalid_report.job.job_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(status, WorkerJobStatus::FailedTerminal.as_str());
        assert!(child_policy_text.is_none());
        let result: Value = serde_json::from_str(&result_text).unwrap();
        assert_eq!(result["failureCode"], "worker.invalid-stored-child-policy");
        assert_eq!(
            result["reason"],
            "stored child execution policy failed validation"
        );
        assert_eq!(result["quarantined"], true);
        assert!(!result_text.contains("not-json"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn subagent_lifecycle_receipt_created_for_llm_subagent_worker() {
        let root = temp_root("subagent_lifecycle_receipt_created_for_llm_subagent_worker");
        let harness_home = root.join(".agent-harness");
        let source = root.join("source");
        let workspace = source.join("workspace");
        fs::create_dir_all(&workspace).unwrap();
        let mut options = llm_options(&harness_home, &source, &workspace, "subagent-worker", 1002);
        options.kind = WorkerJobKind::LlmSubagent;
        options.payload = json!({
            "runId": "queued-1",
            "sourceHome": source,
            "sourceWorkspace": workspace,
            "agentId": "researcher",
            "sessionKey": "subagent:queued-1:researcher",
            "messageText": "continue research",
            "platform": "subagent-ledger",
            "channelId": "queued-1",
            "userId": "main",
            "provider": "openai",
            "model": "gpt-5.3-codex-spark",
            "authLane": "codex-oauth"
        });

        let enqueue = enqueue_worker_job(options).unwrap();
        let queued = crate::show_subagent_lifecycle(crate::SubagentLifecycleShowOptions {
            harness_home: harness_home.clone(),
            subagent_id: "subagent:queued-1".to_string(),
            now_ms: 1002,
        })
        .unwrap();

        assert_eq!(queued.receipt.state, crate::SubagentLifecycleState::Queued);
        assert_eq!(
            queued.receipt.worker_job_id.as_deref(),
            Some(enqueue.job.job_id.as_str())
        );
        assert_eq!(
            queued.receipt.requested_model.as_deref(),
            Some("gpt-5.3-codex-spark")
        );
        assert_eq!(queued.receipt.provider.as_deref(), Some("openai"));
        assert_eq!(queued.receipt.auth_lane.as_deref(), Some("codex-oauth"));
        assert_eq!(queued.receipt.auth_visibility, "verified");
        assert!(queued.snapshot_file.is_file());

        let run = run_worker_once(WorkerRunOnceOptions {
            harness_home: harness_home.clone(),
            lane: Some("llm".to_string()),
            worker_id: "test-worker".to_string(),
            lease_ms: DEFAULT_LEASE_MS,
            now_ms: 1003,
        })
        .unwrap();
        assert_eq!(run.status, WorkerRunOnceStatus::Dispatched);

        let running = crate::show_subagent_lifecycle(crate::SubagentLifecycleShowOptions {
            harness_home: harness_home.clone(),
            subagent_id: "subagent:queued-1".to_string(),
            now_ms: 1004,
        })
        .unwrap();
        assert_eq!(
            running.receipt.state,
            crate::SubagentLifecycleState::Running
        );
        let queue_file = harness_home
            .join("state")
            .join("runtime-queue")
            .join("pending.jsonl");
        let queue_text = fs::read_to_string(&queue_file).unwrap();
        let item: Value = serde_json::from_str(queue_text.lines().next().unwrap()).unwrap();
        assert_eq!(
            running.receipt.runtime_queue_id.as_deref(),
            item["queueId"].as_str()
        );
        assert_eq!(running.receipt.auth_visibility, "verified");
        assert!(running.receipt.terminal_receipt_file.is_none());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn subagent_lifecycle_idempotency_hit_repairs_persisted_job_payload() {
        let root = temp_root("subagent_lifecycle_idempotency_hit_repairs_persisted_job_payload");
        let harness_home = root.join(".agent-harness");
        let source = root.join("source");
        let workspace = source.join("workspace");
        fs::create_dir_all(&workspace).unwrap();
        let mut first = llm_options(&harness_home, &source, &workspace, "same-key", 1002);
        first.payload = json!({
            "runId": "first",
            "sourceHome": source,
            "sourceWorkspace": workspace,
            "agentId": "researcher",
            "sessionKey": "subagent:first:researcher",
            "messageText": "continue first",
        });
        enqueue_worker_job(first).unwrap();
        fs::remove_dir_all(harness_home.join("state").join("subagents")).unwrap();

        let second = WorkerEnqueueOptions {
            harness_home: harness_home.clone(),
            kind: WorkerJobKind::DeterministicShell,
            lane: Some("shell".to_string()),
            payload: json!({
                "subagentId": "subagent:wrong",
                "scriptPath": "noop.cmd",
                "dryRun": true
            }),
            idempotency_key: Some("same-key".to_string()),
            parent_job_id: None,
            job_group_id: None,
            master_agent_id: None,
            master_session_key: None,
            wake_policy: None,
            source: Some("test".to_string()),
            priority: 0,
            available_at_ms: Some(1003),
            max_attempts: 3,
            timeout_ms: Some(DEFAULT_TIMEOUT_MS),
            cascade_timeout_ms: None,
            rate_key: None,
            concurrency_group_key: None,
            now_ms: 1003,
        };
        let duplicate = enqueue_worker_job(second).unwrap();
        assert!(!duplicate.inserted);

        let repaired = crate::show_subagent_lifecycle(crate::SubagentLifecycleShowOptions {
            harness_home: harness_home.clone(),
            subagent_id: "subagent:first".to_string(),
            now_ms: 1004,
        })
        .unwrap();
        let wrong = crate::show_subagent_lifecycle(crate::SubagentLifecycleShowOptions {
            harness_home: harness_home.clone(),
            subagent_id: "subagent:wrong".to_string(),
            now_ms: 1004,
        })
        .unwrap();

        assert_eq!(
            repaired.receipt.state,
            crate::SubagentLifecycleState::Queued
        );
        assert_eq!(repaired.receipt.worker_job_id, Some(duplicate.job.job_id));
        assert_eq!(wrong.receipt.state, crate::SubagentLifecycleState::Unknown);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn subagent_lifecycle_retry_does_not_duplicate_runtime_queue_item() {
        let root = temp_root("subagent_lifecycle_retry_does_not_duplicate_runtime_queue_item");
        let harness_home = root.join(".agent-harness");
        let source = root.join("source");
        let workspace = source.join("workspace");
        fs::create_dir_all(&workspace).unwrap();
        let mut options = llm_options(&harness_home, &source, &workspace, "retry-subagent", 1002);
        options.payload = json!({
            "runId": "retry",
            "sourceHome": source,
            "sourceWorkspace": workspace,
            "agentId": "researcher",
            "sessionKey": "subagent:retry:researcher",
            "messageText": "continue retry",
        });
        enqueue_worker_job(options).unwrap();

        let lifecycle_dir = harness_home
            .join("state")
            .join("subagents")
            .join("lifecycle");
        fs::remove_dir_all(&lifecycle_dir).unwrap();
        fs::write(&lifecycle_dir, "not a directory").unwrap();

        let first = run_worker_once(WorkerRunOnceOptions {
            harness_home: harness_home.clone(),
            lane: Some("llm".to_string()),
            worker_id: "test-worker".to_string(),
            lease_ms: DEFAULT_LEASE_MS,
            now_ms: 1003,
        })
        .unwrap();
        assert_eq!(first.status, WorkerRunOnceStatus::Rescheduled);

        fs::remove_file(&lifecycle_dir).unwrap();
        fs::create_dir_all(&lifecycle_dir).unwrap();
        let second = run_worker_once(WorkerRunOnceOptions {
            harness_home: harness_home.clone(),
            lane: Some("llm".to_string()),
            worker_id: "test-worker".to_string(),
            lease_ms: DEFAULT_LEASE_MS,
            now_ms: 10_000,
        })
        .unwrap();
        assert_eq!(second.status, WorkerRunOnceStatus::Dispatched);

        let queue_file = harness_home
            .join("state")
            .join("runtime-queue")
            .join("pending.jsonl");
        let queue_text = fs::read_to_string(&queue_file).unwrap();
        let lines = queue_text.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 1);
        let item: Value = serde_json::from_str(lines[0]).unwrap();
        let running = crate::show_subagent_lifecycle(crate::SubagentLifecycleShowOptions {
            harness_home: harness_home.clone(),
            subagent_id: "subagent:retry".to_string(),
            now_ms: 10_001,
        })
        .unwrap();
        assert_eq!(
            running.receipt.runtime_queue_id.as_deref(),
            item["queueId"].as_str()
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn cron_worker_skips_operator_controlled_run_without_runtime_enqueue() {
        let root = temp_root("cron_worker_skips_operator_controlled_run_without_runtime_enqueue");
        let harness_home = root.join(".agent-harness");
        let source = root.join("source");
        let workspace = source.join("workspace");
        fs::create_dir_all(&workspace).unwrap();
        let cron_run = crate::admit_cron_run(crate::CronRunAdmitOptions {
            harness_home: harness_home.clone(),
            source_kind: "native-cron".to_string(),
            source_id: "source-1".to_string(),
            entry_id: "daily".to_string(),
            agent_id: "agent-a".to_string(),
            scheduled_for_ms: 42,
            runtime_class: "cron".to_string(),
            session_key: "cron:agent-a:daily:42".to_string(),
            session_policy: "one-shot".to_string(),
            max_attempts: 3,
            now_ms: 1000,
        })
        .unwrap();
        let mut options = llm_options(&harness_home, &source, &workspace, "cron-worker-skip", 1002);
        options.lane = Some("cron".to_string());
        options.payload = json!({
            "sourceHome": source,
            "sourceWorkspace": workspace,
            "agentId": "agent-a",
            "sessionKey": "cron:agent-a:daily:42",
            "messageText": "run daily cron",
            "runtimeClass": "cron",
            "origin": "cron-scheduler",
            "cronRunId": &cron_run.run_id,
            "scheduledForMs": 42,
            "platform": "native-cron",
            "sessionPolicy": "one-shot",
            "channelId": "daily",
            "userId": "cron-scheduler"
        });
        let enqueue = enqueue_worker_job(options).unwrap();
        crate::mark_cron_run_worker_enqueued(
            &harness_home,
            &cron_run.run_id,
            &enqueue.job.job_id,
            1001,
        )
        .unwrap();
        crate::control_cron_run(crate::CronRunControlOptions {
            harness_home: harness_home.clone(),
            action: crate::CronRunControlAction::Skip,
            run_id: Some(cron_run.run_id.clone()),
            agent_id: None,
            entry_id: None,
            reason: "operator skip before worker dispatch".to_string(),
            now_ms: 1002,
        })
        .unwrap();

        let run = run_worker_once(WorkerRunOnceOptions {
            harness_home: harness_home.clone(),
            lane: Some("cron".to_string()),
            worker_id: "test-worker".to_string(),
            lease_ms: DEFAULT_LEASE_MS,
            now_ms: 1003,
        })
        .unwrap();

        assert_eq!(run.status, WorkerRunOnceStatus::Failed);
        assert_eq!(
            run.result.as_ref().unwrap().status,
            WorkerJobStatus::Canceled
        );
        assert_eq!(run.job.as_ref().unwrap().status, WorkerJobStatus::Canceled);
        assert!(
            !harness_home
                .join("state")
                .join("runtime-queue")
                .join("pending.jsonl")
                .is_file()
        );
        let runs = crate::collect_cron_run_summary(&harness_home).unwrap();
        let updated = runs.runs.first().unwrap();
        assert_eq!(updated.status, crate::CronRunStatus::Skipped);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn cron_llm_worker_failure_marks_cron_run_retry_pending() {
        let root = temp_root("cron_llm_worker_failure_marks_cron_run_retry_pending");
        let harness_home = root.join(".agent-harness");
        let cron_run = crate::admit_cron_run(crate::CronRunAdmitOptions {
            harness_home: harness_home.clone(),
            source_kind: "native-cron".to_string(),
            source_id: "source-1".to_string(),
            entry_id: "bad-dispatch".to_string(),
            agent_id: "agent-a".to_string(),
            scheduled_for_ms: 42,
            runtime_class: "cron".to_string(),
            session_key: "cron:agent-a:bad-dispatch:42".to_string(),
            session_policy: "one-shot".to_string(),
            max_attempts: 3,
            now_ms: 1000,
        })
        .unwrap();
        let enqueue = enqueue_worker_job(WorkerEnqueueOptions {
            harness_home: harness_home.clone(),
            kind: WorkerJobKind::LlmSubagent,
            lane: Some("llm".to_string()),
            payload: json!({
                "agentId": "agent-a",
                "sessionKey": "cron:agent-a:bad-dispatch:42",
                "messageText": "missing source paths",
                "runtimeClass": "cron",
                "origin": "cron-scheduler",
                "cronRunId": &cron_run.run_id,
                "platform": "native-cron"
            }),
            idempotency_key: Some("bad-dispatch".to_string()),
            parent_job_id: None,
            job_group_id: None,
            master_agent_id: Some("main".to_string()),
            master_session_key: Some("master".to_string()),
            wake_policy: None,
            source: Some("test".to_string()),
            priority: 0,
            available_at_ms: Some(1002),
            max_attempts: 3,
            timeout_ms: Some(DEFAULT_TIMEOUT_MS),
            cascade_timeout_ms: None,
            rate_key: None,
            concurrency_group_key: None,
            now_ms: 1002,
        })
        .unwrap();
        crate::mark_cron_run_worker_enqueued(
            &harness_home,
            &cron_run.run_id,
            &enqueue.job.job_id,
            1001,
        )
        .unwrap();

        let run = run_worker_once(WorkerRunOnceOptions {
            harness_home: harness_home.clone(),
            lane: Some("llm".to_string()),
            worker_id: "test-worker".to_string(),
            lease_ms: DEFAULT_LEASE_MS,
            now_ms: 1003,
        })
        .unwrap();

        assert_eq!(run.status, WorkerRunOnceStatus::Rescheduled);
        assert_eq!(
            run.result.as_ref().unwrap().status,
            WorkerJobStatus::FailedRetryable
        );
        let runs = crate::collect_cron_run_summary(&harness_home).unwrap();
        let updated = runs.runs.first().unwrap();
        assert_eq!(updated.status, crate::CronRunStatus::RetryPending);
        assert!(
            updated
                .failure_reason
                .as_deref()
                .unwrap_or_default()
                .contains("worker job execution failed before result")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn deterministic_shell_job_writes_audit_and_succeeds() {
        let root = temp_root("deterministic_shell_job_writes_audit_and_succeeds");
        let harness_home = root.join(".agent-harness");
        let script_dir = harness_home.join("state").join("workers").join("scripts");
        fs::create_dir_all(&script_dir).unwrap();
        let script = script_dir.join("hello.cmd");
        fs::write(&script, "@echo hello-worker\r\n").unwrap();
        let mut options = shell_options(&harness_home, "shell-run", 1000);
        options.payload = json!({"scriptPath": script, "cwd": script_dir});
        enqueue_worker_job(options).unwrap();

        let run = run_worker_once(WorkerRunOnceOptions {
            harness_home: harness_home.clone(),
            lane: Some("shell".to_string()),
            worker_id: "test-worker".to_string(),
            lease_ms: DEFAULT_LEASE_MS,
            now_ms: 1001,
        })
        .unwrap();

        assert_eq!(run.status, WorkerRunOnceStatus::Completed);
        let result = run.result.unwrap();
        assert_eq!(result.status, WorkerJobStatus::Succeeded);
        assert!(result.audit_path.unwrap().is_file());

        let _ = fs::remove_dir_all(root);
    }
    #[test]
    fn deterministic_shell_final_timeout_is_terminal_and_next_job_runs() {
        let root = temp_root("deterministic_shell_final_timeout_is_terminal_and_next_job_runs");
        let harness_home = root.join(".agent-harness");
        let script_dir = harness_home.join("state").join("workers").join("scripts");
        fs::create_dir_all(&script_dir).unwrap();
        let timeout_script = write_timeout_script(&script_dir);
        let cron_run = crate::admit_cron_run(crate::CronRunAdmitOptions {
            harness_home: harness_home.clone(),
            source_kind: "native-cron".to_string(),
            source_id: "source-1".to_string(),
            entry_id: "final-timeout".to_string(),
            agent_id: "agent-a".to_string(),
            scheduled_for_ms: 42,
            runtime_class: "cron".to_string(),
            session_key: "cron:agent-a:final-timeout:42".to_string(),
            session_policy: "one-shot".to_string(),
            max_attempts: 1,
            now_ms: 1000,
        })
        .unwrap();

        let mut timeout = shell_options(&harness_home, "final-timeout", 1000);
        timeout.payload = json!({
            "scriptPath": timeout_script,
            "cwd": script_dir,
            "cronRunId": &cron_run.run_id,
        });
        timeout.max_attempts = 1;
        timeout.timeout_ms = Some(10);
        let timeout_job = enqueue_worker_job(timeout).unwrap();
        crate::mark_cron_run_worker_enqueued(
            &harness_home,
            &cron_run.run_id,
            &timeout_job.job.job_id,
            1001,
        )
        .unwrap();

        let next_job = enqueue_worker_job(shell_options(&harness_home, "next-job", 1001)).unwrap();
        let timed_out = run_worker_once(WorkerRunOnceOptions {
            harness_home: harness_home.clone(),
            lane: Some("shell".to_string()),
            worker_id: "test-worker".to_string(),
            lease_ms: DEFAULT_LEASE_MS,
            now_ms: 1002,
        })
        .unwrap();

        assert_eq!(timed_out.status, WorkerRunOnceStatus::Failed);
        let result = timed_out.result.as_ref().unwrap();
        assert_eq!(result.status, WorkerJobStatus::FailedTerminal);
        assert!(result.reason.contains("timed out"));
        assert!(result.reason.contains("retry attempts exhausted"));
        let persisted = timed_out.job.as_ref().unwrap();
        assert_eq!(persisted.status, WorkerJobStatus::FailedTerminal);
        assert_eq!(persisted.attempt, persisted.max_attempts);
        assert!(persisted.lease_owner.is_none());
        assert!(persisted.lease_expires_at_ms.is_none());
        assert_eq!(persisted.finished_at_ms, Some(1002));

        let runs = crate::collect_cron_run_summary(&harness_home).unwrap();
        let updated = runs
            .runs
            .iter()
            .find(|run| run.run_id == cron_run.run_id)
            .unwrap();
        assert_eq!(updated.status, crate::CronRunStatus::FailedTerminal);
        assert!(
            updated
                .failure_reason
                .as_deref()
                .unwrap_or_default()
                .contains("retry attempts exhausted")
        );

        let next = run_worker_once(WorkerRunOnceOptions {
            harness_home: harness_home.clone(),
            lane: Some("shell".to_string()),
            worker_id: "test-worker".to_string(),
            lease_ms: DEFAULT_LEASE_MS,
            now_ms: 1003,
        })
        .unwrap();
        assert_eq!(next.status, WorkerRunOnceStatus::Completed);
        assert_eq!(next.job.as_ref().unwrap().job_id, next_job.job.job_id);
        assert_eq!(
            next.result.as_ref().unwrap().status,
            WorkerJobStatus::Succeeded
        );

        let worker_status = collect_worker_status(WorkerStatusOptions {
            harness_home: harness_home.clone(),
        })
        .unwrap();
        assert_eq!(worker_status.totals.leased, 0);
        assert_eq!(worker_status.totals.running, 0);
        assert_eq!(worker_status.totals.failed_retryable, 0);
        assert_eq!(worker_status.totals.failed_terminal, 1);
        assert_eq!(worker_status.totals.succeeded, 1);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn skill_synthesis_worker_autonomously_creates_agent_skill() {
        let root = temp_root("skill_synthesis_worker_autonomously_creates_agent_skill");
        let harness_home = root.join(".agent-harness");
        enqueue_worker_job(WorkerEnqueueOptions {
            harness_home: harness_home.clone(),
            kind: WorkerJobKind::SkillSynthesis,
            lane: Some("skill_synthesis".to_string()),
            payload: json!({
                "skillId": "agent-created:follow-up-debugging",
                "taskSummary": "Debug repeated follow-up failures with focused receipts",
                "evidence": "Tests: follow_up_debugging_replay_green",
            }),
            idempotency_key: Some("skill-synthesis:queue-synth-1".to_string()),
            parent_job_id: None,
            job_group_id: Some("queue-synth-1".to_string()),
            master_agent_id: Some("main".to_string()),
            master_session_key: Some("session-1".to_string()),
            wake_policy: None,
            source: Some("runtime-completion-skill-synthesis".to_string()),
            priority: 0,
            available_at_ms: Some(1000),
            max_attempts: 1,
            timeout_ms: Some(DEFAULT_TIMEOUT_MS),
            cascade_timeout_ms: None,
            rate_key: Some("skill-synthesis:follow-up-debugging".to_string()),
            concurrency_group_key: Some("skill-synthesis:follow-up-debugging".to_string()),
            now_ms: 1000,
        })
        .unwrap();

        let run = run_worker_once(WorkerRunOnceOptions {
            harness_home: harness_home.clone(),
            lane: Some("skill_synthesis".to_string()),
            worker_id: "test-worker".to_string(),
            lease_ms: DEFAULT_LEASE_MS,
            now_ms: 1001,
        })
        .unwrap();

        assert_eq!(run.status, WorkerRunOnceStatus::Completed);
        let result = run.result.unwrap();
        assert_eq!(result.status, WorkerJobStatus::Succeeded);
        assert!(
            harness_home
                .join("skills")
                .join("agent-created")
                .join("follow-up-debugging")
                .join(crate::SKILL_FILE_NAME)
                .is_file()
        );
        assert!(crate::skill_autonomous_apply_receipts_file(&harness_home).is_file());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn self_improvement_learning_review_applies_replacement_and_notifies() {
        let root = temp_root("self_improvement_learning_review_applies_replacement_and_notifies");
        let harness_home = root.join(".agent-harness");
        let skill_dir = root.join("skills").join("quiet-cron-watchdogs");
        fs::create_dir_all(&skill_dir).unwrap();
        let skill_file = skill_dir.join("SKILL.md");
        fs::write(&skill_file, "# Quiet Cron Watchdogs\n\nOriginal.\n").unwrap();

        let replacement_body = "# Quiet Cron Watchdogs\n\nUpdated from post-turn review.\n";
        enqueue_worker_job(WorkerEnqueueOptions {
            harness_home: harness_home.clone(),
            kind: WorkerJobKind::LearningReview,
            lane: Some("learning_review".to_string()),
            payload: json!({
                "mode": "dispatch-and-replace",
                "notify": true,
                "targetSkillId": "workspace:quiet-cron-watchdogs",
                "targetPath": skill_file,
                "replacementBody": replacement_body,
                "signalText": "post-turn review found a reusable cron watchdog note",
                "sourceTurn": "queue-1",
                "channelTrust": "operator",
                "notificationTarget": {
                    "platform": "telegram",
                    "channelId": "dm-1",
                    "userId": "operator",
                    "sessionKey": "session-1"
                }
            }),
            idempotency_key: Some("self-improvement:queue-1".to_string()),
            parent_job_id: None,
            job_group_id: None,
            master_agent_id: Some("main".to_string()),
            master_session_key: Some("session-1".to_string()),
            wake_policy: None,
            source: Some("self-improvement-review".to_string()),
            priority: 0,
            available_at_ms: Some(1000),
            max_attempts: 1,
            timeout_ms: Some(DEFAULT_TIMEOUT_MS),
            cascade_timeout_ms: None,
            rate_key: None,
            concurrency_group_key: None,
            now_ms: 1000,
        })
        .unwrap();

        let run = run_worker_once(WorkerRunOnceOptions {
            harness_home: harness_home.clone(),
            lane: Some("learning_review".to_string()),
            worker_id: "test-worker".to_string(),
            lease_ms: DEFAULT_LEASE_MS,
            now_ms: 1001,
        })
        .unwrap();

        assert_eq!(run.status, WorkerRunOnceStatus::Completed);
        let result = run.result.unwrap();
        assert_eq!(result.status, WorkerJobStatus::Succeeded);
        assert_eq!(fs::read_to_string(&skill_file).unwrap(), replacement_body);
        let outbox_file = harness_home
            .join("state")
            .join("channels")
            .join("outbox.jsonl");
        let outbox_text = fs::read_to_string(&outbox_file).unwrap();
        assert!(outbox_text.contains(
            "Self-improvement review: Patched SKILL.md in skill 'workspace:quiet-cron-watchdogs' (1 replacement)."
        ));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn self_improvement_dispatch_replace_auto_applies_high_confidence_signal() {
        let root =
            temp_root("self_improvement_dispatch_replace_auto_applies_high_confidence_signal");
        let harness_home = root.join(".agent-harness");
        let skill_dir = root.join("skills").join("quiet-cron-watchdogs");
        fs::create_dir_all(&skill_dir).unwrap();
        let skill_file = skill_dir.join("SKILL.md");
        fs::write(&skill_file, "# Quiet Cron Watchdogs\n\nOriginal.\n").unwrap();

        enqueue_worker_job(WorkerEnqueueOptions {
            harness_home: harness_home.clone(),
            kind: WorkerJobKind::LearningReview,
            lane: Some("learning_review".to_string()),
            payload: json!({
                "mode": "dispatch-and-replace",
                "notify": true,
                "targetSkillId": "workspace:quiet-cron-watchdogs",
                "targetPath": skill_file,
                "signalText": "remember to keep cron watchdog fixes in this skill after repeated scheduler errors",
                "sourceTurn": "queue-auto-1",
                "channelTrust": "operator",
                "notificationTarget": {
                    "platform": "telegram",
                    "channelId": "dm-1",
                    "userId": "operator",
                    "sessionKey": "session-1"
                }
            }),
            idempotency_key: Some("self-improvement:queue-auto-1".to_string()),
            parent_job_id: None,
            job_group_id: None,
            master_agent_id: Some("main".to_string()),
            master_session_key: Some("session-1".to_string()),
            wake_policy: None,
            source: Some("self-improvement-review".to_string()),
            priority: 0,
            available_at_ms: Some(1000),
            max_attempts: 1,
            timeout_ms: Some(DEFAULT_TIMEOUT_MS),
            cascade_timeout_ms: None,
            rate_key: None,
            concurrency_group_key: None,
            now_ms: 1000,
        })
        .unwrap();

        let run = run_worker_once(WorkerRunOnceOptions {
            harness_home: harness_home.clone(),
            lane: Some("learning_review".to_string()),
            worker_id: "test-worker".to_string(),
            lease_ms: DEFAULT_LEASE_MS,
            now_ms: 1001,
        })
        .unwrap();

        assert_eq!(run.status, WorkerRunOnceStatus::Completed);
        let result = run.result.unwrap();
        assert_eq!(result.status, WorkerJobStatus::Succeeded);
        let skill_text = fs::read_to_string(&skill_file).unwrap();
        assert!(skill_text.contains("## Self-Improvement Notes"));
        assert!(skill_text.contains("remember to keep cron watchdog fixes"));
        assert!(skill_text.contains("sourceTurn `queue-auto-1`"));
        let outbox_file = harness_home
            .join("state")
            .join("channels")
            .join("outbox.jsonl");
        let outbox_text = fs::read_to_string(&outbox_file).unwrap();
        assert!(outbox_text.contains(
            "Self-improvement review: Patched SKILL.md in skill 'workspace:quiet-cron-watchdogs' (1 replacement)."
        ));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn self_improvement_dispatch_replace_does_not_notify_when_signal_is_not_actionable() {
        let root = temp_root(
            "self_improvement_dispatch_replace_does_not_notify_when_signal_is_not_actionable",
        );
        let harness_home = root.join(".agent-harness");
        let skill_dir = root.join("skills").join("openclaw-agent-optimize");
        fs::create_dir_all(&skill_dir).unwrap();
        let skill_file = skill_dir.join("SKILL.md");
        fs::write(&skill_file, "# OpenClaw Agent Optimize\n\nOriginal.\n").unwrap();

        enqueue_worker_job(WorkerEnqueueOptions {
            harness_home: harness_home.clone(),
            kind: WorkerJobKind::LearningReview,
            lane: Some("learning_review".to_string()),
            payload: json!({
                "mode": "dispatch-and-replace",
                "notify": true,
                "targetSkillId": "workspace:openclaw-agent-optimize",
                "targetPath": skill_file,
                "signalText": "post-turn self-improvement review signal: selected skill failed once during runtime without a verified reusable fix",
                "sourceTurn": "queue-low-confidence-1",
                "channelTrust": "operator",
                "notificationTarget": {
                    "platform": "telegram",
                    "channelId": "dm-1",
                    "userId": "operator",
                    "sessionKey": "session-1"
                }
            }),
            idempotency_key: Some("self-improvement:queue-low-confidence-1".to_string()),
            parent_job_id: None,
            job_group_id: None,
            master_agent_id: Some("main".to_string()),
            master_session_key: Some("session-1".to_string()),
            wake_policy: None,
            source: Some("self-improvement-review".to_string()),
            priority: 0,
            available_at_ms: Some(1000),
            max_attempts: 1,
            timeout_ms: Some(DEFAULT_TIMEOUT_MS),
            cascade_timeout_ms: None,
            rate_key: None,
            concurrency_group_key: None,
            now_ms: 1000,
        })
        .unwrap();

        let run = run_worker_once(WorkerRunOnceOptions {
            harness_home: harness_home.clone(),
            lane: Some("learning_review".to_string()),
            worker_id: "test-worker".to_string(),
            lease_ms: DEFAULT_LEASE_MS,
            now_ms: 1001,
        })
        .unwrap();

        assert_eq!(run.status, WorkerRunOnceStatus::Completed);
        let result = run.result.unwrap();
        assert_eq!(result.status, WorkerJobStatus::Succeeded);
        assert!(crate::skill_proposals_file(&harness_home).is_file());
        let outbox_file = harness_home
            .join("state")
            .join("channels")
            .join("outbox.jsonl");
        assert!(
            !outbox_file.exists(),
            "low-confidence dispatch-and-replace fallback must not emit channel noise"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn workers_memory_backfill_runs_embedding_job_and_writes_cursor() {
        let root = temp_root("workers_memory_backfill_runs_embedding_job_and_writes_cursor");
        let harness_home = root.join(".agent-harness");
        create_backfill_sqlite(&harness_home);

        let enqueue = enqueue_worker_job(WorkerEnqueueOptions {
            harness_home: harness_home.clone(),
            kind: WorkerJobKind::MemoryEmbeddingBackfill,
            lane: Some("memory_embedding_backfill".to_string()),
            payload: json!({
                "lane": "episodic_events",
                "model": "test-embedding",
                "vectorDimension": 2,
                "batchSize": 2,
                "maxItems": 2,
                "rateLimitPerMinute": 8
            }),
            idempotency_key: Some("memory-backfill:episodic-events".to_string()),
            parent_job_id: None,
            job_group_id: None,
            master_agent_id: None,
            master_session_key: None,
            wake_policy: None,
            source: None,
            priority: 0,
            available_at_ms: Some(1_000),
            max_attempts: 1,
            timeout_ms: None,
            cascade_timeout_ms: None,
            rate_key: Some("memory-backfill:episodic-events".to_string()),
            concurrency_group_key: Some("memory-backfill".to_string()),
            now_ms: 1_000,
        })
        .unwrap();
        assert!(enqueue.inserted);

        let run = run_worker_once(WorkerRunOnceOptions {
            harness_home: harness_home.clone(),
            lane: Some("memory_embedding_backfill".to_string()),
            worker_id: "worker-1".to_string(),
            lease_ms: DEFAULT_LEASE_MS,
            now_ms: 1_100,
        })
        .unwrap();

        assert_eq!(run.status, WorkerRunOnceStatus::Completed);
        let result = run.result.unwrap();
        assert_eq!(result.status, WorkerJobStatus::Succeeded);
        assert!(result.reason.contains("memory embedding backfill planned"));
        assert!(
            crate::memory_backfill::memory_embedding_backfill_cursor_file(
                &harness_home,
                MemoryEmbeddingBackfillLane::EpisodicEvents
            )
            .is_file()
        );
        assert!(
            crate::memory_backfill::memory_embedding_backfill_receipts_file(
                &harness_home,
                MemoryEmbeddingBackfillLane::EpisodicEvents
            )
            .is_file()
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn watchdog_enqueues_master_wakeup_for_completed_group() {
        let root = temp_root("watchdog_enqueues_master_wakeup_for_completed_group");
        let harness_home = root.join(".agent-harness");
        let source = root.join("source");
        let workspace = source.join("workspace");
        fs::create_dir_all(&workspace).unwrap();
        let mut child = shell_options(&harness_home, "child", 1000);
        child.job_group_id = Some("group-1".to_string());
        let child_report = enqueue_worker_job(child).unwrap();
        let conn = Connection::open(worker_db_file(&harness_home)).unwrap();
        set_job_status(
            &conn,
            &child_report.job.job_id,
            WorkerJobStatus::Succeeded,
            1001,
            Some(json!({"ok":true})),
            Some(json!({"auditPath":"audit.json"})),
            None,
        )
        .unwrap();
        enqueue_worker_job(WorkerEnqueueOptions {
            harness_home: harness_home.clone(),
            kind: WorkerJobKind::Watchdog,
            lane: Some("watchdog".to_string()),
            payload: json!({
                "sourceHome": source,
                "sourceWorkspace": workspace,
                "masterAgentId": "main",
                "masterSessionKey": "master-session"
            }),
            idempotency_key: Some("watchdog".to_string()),
            parent_job_id: None,
            job_group_id: Some("group-1".to_string()),
            master_agent_id: Some("main".to_string()),
            master_session_key: Some("master-session".to_string()),
            wake_policy: Some(json!({"mode":"all_completed"})),
            source: Some("test".to_string()),
            priority: 0,
            available_at_ms: Some(1002),
            max_attempts: 3,
            timeout_ms: Some(DEFAULT_TIMEOUT_MS),
            cascade_timeout_ms: None,
            rate_key: None,
            concurrency_group_key: Some("watchdog-group-1".to_string()),
            now_ms: 1002,
        })
        .unwrap();

        let run = run_worker_once(WorkerRunOnceOptions {
            harness_home: harness_home.clone(),
            lane: Some("watchdog".to_string()),
            worker_id: "test-worker".to_string(),
            lease_ms: DEFAULT_LEASE_MS,
            now_ms: 1003,
        })
        .unwrap();

        assert_eq!(run.status, WorkerRunOnceStatus::Completed);
        let status = collect_worker_status(WorkerStatusOptions { harness_home }).unwrap();
        assert_eq!(status.totals.pending, 1);
        assert!(
            status
                .by_lane
                .iter()
                .any(|lane| lane.lane == "llm" && lane.totals.pending == 1)
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn watchdog_quarantines_invalid_group_child_and_wakes_master() {
        let root = temp_root("watchdog_quarantines_invalid_group_child_and_wakes_master");
        let harness_home = root.join(".agent-harness");
        let source = root.join("source");
        let workspace = source.join("workspace");
        fs::create_dir_all(&workspace).unwrap();

        let mut child = llm_options(&harness_home, &source, &workspace, "corrupt-child", 1000);
        child.job_group_id = Some("group-corrupt".to_string());
        child.rate_key = None;
        let child_report = enqueue_worker_job_with_policy(
            child,
            managed_child_policy(21, "openai", "gpt-5.6-sol", "max"),
        );
        let conn = Connection::open(worker_db_file(&harness_home)).unwrap();
        conn.execute(
            "UPDATE jobs SET child_policy_json=?1 WHERE job_id=?2",
            params!["{malformed", child_report.job.job_id],
        )
        .unwrap();
        let grouped = group_children(&conn, "group-corrupt", "not-a-child", 1001).unwrap();
        assert_eq!(grouped.len(), 1);
        assert_eq!(grouped[0].status, WorkerJobStatus::FailedTerminal);
        assert!(grouped[0].child_policy.is_none());
        drop(conn);

        enqueue_worker_job(WorkerEnqueueOptions {
            harness_home: harness_home.clone(),
            kind: WorkerJobKind::Watchdog,
            lane: Some("watchdog".to_string()),
            payload: json!({
                "sourceHome": source,
                "sourceWorkspace": workspace,
                "masterAgentId": "main",
                "masterSessionKey": "master-session"
            }),
            idempotency_key: Some("watchdog-corrupt".to_string()),
            parent_job_id: None,
            job_group_id: Some("group-corrupt".to_string()),
            master_agent_id: Some("main".to_string()),
            master_session_key: Some("master-session".to_string()),
            wake_policy: Some(json!({"mode":"all_completed"})),
            source: Some("test".to_string()),
            priority: 10,
            available_at_ms: Some(1002),
            max_attempts: 3,
            timeout_ms: Some(DEFAULT_TIMEOUT_MS),
            cascade_timeout_ms: None,
            rate_key: None,
            concurrency_group_key: Some("watchdog-group-corrupt".to_string()),
            now_ms: 1002,
        })
        .unwrap();

        let run = run_worker_once(WorkerRunOnceOptions {
            harness_home: harness_home.clone(),
            lane: Some("watchdog".to_string()),
            worker_id: "test-worker".to_string(),
            lease_ms: DEFAULT_LEASE_MS,
            now_ms: 1003,
        })
        .unwrap();
        assert_eq!(run.status, WorkerRunOnceStatus::Completed);

        let conn = Connection::open(worker_db_file(&harness_home)).unwrap();
        let (status, policy): (String, Option<String>) = conn
            .query_row(
                "SELECT status, child_policy_json FROM jobs WHERE job_id=?1",
                params![child_report.job.job_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(status, WorkerJobStatus::FailedTerminal.as_str());
        assert!(policy.is_none());
        let status = collect_worker_status(WorkerStatusOptions { harness_home }).unwrap();
        assert_eq!(status.totals.pending, 1);

        let _ = fs::remove_dir_all(root);
    }

    fn shell_options(harness_home: &Path, key: &str, now_ms: i64) -> WorkerEnqueueOptions {
        WorkerEnqueueOptions {
            harness_home: harness_home.to_path_buf(),
            kind: WorkerJobKind::DeterministicShell,
            lane: Some("shell".to_string()),
            payload: json!({"scriptPath":"dry-run.cmd","dryRun":true}),
            idempotency_key: Some(key.to_string()),
            parent_job_id: None,
            job_group_id: None,
            master_agent_id: None,
            master_session_key: None,
            wake_policy: None,
            source: Some("test".to_string()),
            priority: 0,
            available_at_ms: Some(now_ms),
            max_attempts: 3,
            timeout_ms: Some(DEFAULT_TIMEOUT_MS),
            cascade_timeout_ms: None,
            rate_key: None,
            concurrency_group_key: None,
            now_ms,
        }
    }

    fn llm_options(
        harness_home: &Path,
        source: &Path,
        workspace: &Path,
        key: &str,
        now_ms: i64,
    ) -> WorkerEnqueueOptions {
        WorkerEnqueueOptions {
            harness_home: harness_home.to_path_buf(),
            kind: WorkerJobKind::LlmSubagent,
            lane: Some("llm".to_string()),
            payload: json!({
                "sourceHome": source,
                "sourceWorkspace": workspace,
                "agentId": "main",
                "sessionKey": format!("session-{key}"),
                "messageText": format!("run {key}"),
            }),
            idempotency_key: Some(key.to_string()),
            parent_job_id: None,
            job_group_id: None,
            master_agent_id: Some("main".to_string()),
            master_session_key: Some("master".to_string()),
            wake_policy: None,
            source: Some("test".to_string()),
            priority: 0,
            available_at_ms: Some(now_ms),
            max_attempts: 3,
            timeout_ms: Some(DEFAULT_TIMEOUT_MS),
            cascade_timeout_ms: None,
            rate_key: Some("llm:provider:test".to_string()),
            concurrency_group_key: Some("master:main".to_string()),
            now_ms,
        }
    }

    fn managed_child_policy(
        policy_revision: u64,
        provider: &str,
        model: &str,
        effort: &str,
    ) -> ChildExecutionPolicyV1 {
        ChildExecutionPolicyV1::new(ChildExecutionPolicyV1Input {
            policy_revision,
            provider: Some(provider.to_string()),
            model: Some(model.to_string()),
            reasoning_preference: Some(ReasoningPreference::explicit(effort).unwrap()),
            backend_reasoning_policy: Some(
                BackendReasoningPolicyV1::new(
                    BackendReasoningSource::ChildAdmission,
                    ReasoningResolutionReceipt {
                        schema_version: REASONING_RESOLUTION_RECEIPT_SCHEMA_VERSION,
                        requested_provider: provider.to_string(),
                        requested_model: model.to_string(),
                        effective_provider: Some(provider.to_string()),
                        effective_model: Some(model.to_string()),
                        requested_effort: effort.to_string(),
                        effective_effort: Some(effort.to_string()),
                        catalog_effective_effort: Some(effort.to_string()),
                        catalog_revision: Some("catalog-test".to_string()),
                        status: ReasoningResolutionStatus::Accepted,
                        authoritative: true,
                        reason: "worker child policy test".to_string(),
                    },
                )
                .unwrap(),
            ),
            catalog_revision: Some("catalog-test".to_string()),
            tools_profile: "default".to_string(),
            sandbox_profile: "workspace-write".to_string(),
            timeout_ms: 300_000,
            heartbeat_timeout_ms: 60_000,
            max_attempts: 3,
            token_or_cost_budget: None,
            delegation_limit: None,
            result_contract: "child-result-envelope-v1".to_string(),
        })
        .unwrap()
    }

    fn enqueue_worker_job_with_policy(
        options: WorkerEnqueueOptions,
        child_policy: ChildExecutionPolicyV1,
    ) -> WorkerEnqueueReport {
        enqueue_worker_job_v2(WorkerEnqueueOptionsV2 {
            options,
            child_policy: Some(child_policy),
        })
        .unwrap()
    }

    fn runtime_queue_values(harness_home: &Path) -> Vec<Value> {
        fs::read_to_string(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("pending.jsonl"),
        )
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
    }

    fn assert_runtime_route(values: &[Value], session_key: &str, model: &str, effort: &str) {
        let item = values
            .iter()
            .find(|item| item["sessionKey"] == session_key)
            .unwrap();
        assert_eq!(item["provider"], "openai");
        assert_eq!(item["model"], model);
        assert_eq!(item["reasoningPreference"]["kind"], "explicit");
        assert_eq!(item["reasoningPreference"]["effort"], effort);
        assert_eq!(
            item["backendReasoningPolicy"]["resolution"]["effectiveModel"],
            model
        );
        assert_eq!(
            item["backendReasoningPolicy"]["resolution"]["effectiveEffort"],
            effort
        );
    }
    fn write_timeout_script(script_dir: &Path) -> PathBuf {
        #[cfg(windows)]
        {
            let script = script_dir.join("timeout.ps1");
            fs::write(&script, "Start-Sleep -Milliseconds 500\r\n").unwrap();
            script
        }
        #[cfg(not(windows))]
        {
            use std::os::unix::fs::PermissionsExt;

            let script = script_dir.join("timeout.sh");
            fs::write(&script, "#!/bin/sh\nsleep 1\n").unwrap();
            let mut permissions = fs::metadata(&script).unwrap().permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&script, permissions).unwrap();
            script
        }
    }

    fn create_backfill_sqlite(harness_home: &Path) {
        let memory_dir = harness_home.join("memory");
        fs::create_dir_all(&memory_dir).unwrap();
        let conn = Connection::open(memory_dir.join("openclaw-mem.sqlite")).unwrap();
        conn.execute_batch(
            "
            CREATE TABLE episodic_events (
                ts TEXT,
                summary TEXT
            );
            CREATE TABLE episodic_event_embeddings (
                event_row_id INTEGER,
                model TEXT,
                dim INTEGER,
                vector BLOB,
                norm REAL
            );
            ",
        )
        .unwrap();
        for summary in ["one", "two", "three"] {
            conn.execute(
                "INSERT INTO episodic_events (ts, summary) VALUES ('2026-06-17', ?1)",
                [summary],
            )
            .unwrap();
        }
        conn.execute(
            "INSERT INTO episodic_event_embeddings (event_row_id, model, dim, vector, norm) VALUES (1, 'test-embedding', 2, x'0000', 1.0)",
            [],
        )
        .unwrap();
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-workers-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
