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
    HarnessLogEvent, HarnessLogLevel, RuntimeQueueItem, RuntimeQueueItemStatus, RuntimeQueueSource,
    RuntimeQueueSourceKind, append_harness_log,
    config::{
        HarnessConfigValidationReport, HarnessConfigValidationStatus, validate_harness_config,
    },
    current_log_time_ms,
};

const WORKER_STORE_SCHEMA: &str = "agent-harness.worker-store.v1";
const WORKER_ENQUEUE_SCHEMA: &str = "agent-harness.worker-enqueue.v1";
const WORKER_RUN_ONCE_SCHEMA: &str = "agent-harness.worker-run-once.v1";
const WORKER_STATUS_SCHEMA: &str = "agent-harness.worker-status.v1";
const WORKER_REAP_SCHEMA: &str = "agent-harness.worker-reap-stale.v1";
const WORKER_CANCEL_SCHEMA: &str = "agent-harness.worker-cancel.v1";
const DEFAULT_TIMEOUT_MS: u64 = 300_000;
const SHELL_OUTPUT_CAP_BYTES: usize = 16 * 1024;

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
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerStatusTotals {
    pub total: usize,
    pub pending: usize,
    pub leased: usize,
    pub running: usize,
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
            Self::PluginCall => "plugin_call",
        }
    }

    pub fn default_lane(&self) -> &'static str {
        match self {
            Self::DeterministicShell => "shell",
            Self::LlmSubagent | Self::MasterWakeup => "llm",
            Self::Watchdog => "watchdog",
            Self::MemoryMaintenance => "maintenance",
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
    let db_file = init_worker_store(&options.harness_home)?;
    let conn = Connection::open(&db_file).map_err(io::Error::other)?;
    let lane = options
        .lane
        .clone()
        .unwrap_or_else(|| options.kind.default_lane().to_string());
    let idempotency_key = options.idempotency_key.clone().unwrap_or_else(|| {
        let stable = format!(
            "{}\n{}\n{}\n{}\n{}",
            options.kind.as_str(),
            lane,
            options.parent_job_id.as_deref().unwrap_or(""),
            options.source.as_deref().unwrap_or(""),
            options.payload
        );
        format!("auto:{}", fnv1a_64_hex(&stable))
    });

    if let Some(existing) = find_job_by_idempotency(&conn, &idempotency_key)? {
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
            updated_at_ms, finished_at_ms
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, NULL, NULL, 0, ?15, ?16, ?17, ?18, ?19, NULL, NULL, NULL, ?20, ?20, NULL)",
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
        ],
    )
    .map_err(io::Error::other)?;
    let job = find_job_by_id(&conn, &job_id)?
        .ok_or_else(|| io::Error::other(format!("inserted worker job not found: {job_id}")))?;

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

    set_job_status(
        &conn,
        &job.job_id,
        WorkerJobStatus::Running,
        options.now_ms,
        None,
        None,
        None,
    )?;
    let result = execute_worker_job(&options.harness_home, &conn, &job, &config, options.now_ms)?;
    let terminal = result.status.is_terminal();
    let rescheduled = result.status == WorkerJobStatus::Pending;
    persist_execution_result(&conn, &job, &result, options.now_ms)?;
    append_harness_log(
        &options.harness_home,
        &HarnessLogEvent::new(
            current_log_time_ms().unwrap_or(options.now_ms),
            if result.status == WorkerJobStatus::Succeeded || rescheduled {
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
        status: if terminal {
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
    let warnings = config.warnings.clone();
    Ok(WorkerStatusReport {
        schema: WORKER_STATUS_SCHEMA,
        harness_home: options.harness_home,
        database: db_file,
        config,
        totals,
        by_lane,
        blocked,
        warnings,
    })
}

pub fn reap_stale_worker_jobs(
    options: WorkerReapStaleOptions,
) -> io::Result<WorkerReapStaleReport> {
    let db_file = init_worker_store(&options.harness_home)?;
    let conn = Connection::open(&db_file).map_err(io::Error::other)?;
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
    )
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
    let rows = if let Some(lane) = lane_filter {
        stmt.query_map(params![now_ms, lane], row_to_job)
            .map_err(io::Error::other)?
            .collect::<Result<Vec<_>, _>>()
    } else {
        stmt.query_map(params![now_ms], row_to_job)
            .map_err(io::Error::other)?
            .collect::<Result<Vec<_>, _>>()
    };
    rows.map_err(io::Error::other)
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
        let executing_channel = executing_channel_count(conn, &channel_key)?;
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

fn executing_channel_count(conn: &Connection, channel_key: &str) -> io::Result<usize> {
    let mut stmt = conn
        .prepare("SELECT * FROM jobs WHERE status IN ('leased','running')")
        .map_err(io::Error::other)?;
    let rows = stmt
        .query_map([], row_to_job)
        .map_err(io::Error::other)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(io::Error::other)?;
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
    let status = if timed_out {
        WorkerJobStatus::FailedRetryable
    } else if output.status.success() {
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
    Ok(WorkerJobExecutionResult {
        status,
        reason: if status == WorkerJobStatus::Succeeded {
            "deterministic shell job completed".to_string()
        } else if timed_out {
            "deterministic shell job timed out".to_string()
        } else {
            format!("deterministic shell job exited with code {:?}", exit_code)
        },
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
    let channel_id = string_path(&job.payload, "channelId").unwrap_or("worker");
    let user_id = string_path(&job.payload, "userId").unwrap_or("worker-dispatch");
    let queue_dir = harness_home.join("state").join("runtime-queue");
    fs::create_dir_all(&queue_dir)?;
    let queue_file = queue_dir.join("pending.jsonl");
    let file_safe_session = safe_file_part(session_key);
    let sessions_dir = harness_home.join("agents").join(agent_id).join("sessions");
    fs::create_dir_all(&sessions_dir)?;
    let queue_id = format!(
        "worker:{}:{}:{}",
        now_ms,
        safe_file_part(&job.job_id),
        fnv1a_64_hex(message_text)
    );
    let item = RuntimeQueueItem {
        schema: "agent-harness.runtime-queue-item.v1",
        queue_id: queue_id.clone(),
        status: RuntimeQueueItemStatus::Queued,
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
        channel_id: channel_id.to_string(),
        user_id: user_id.to_string(),
        message_text: message_text.to_string(),
        inbound_context: string_path(&job.payload, "inboundContext").map(ToString::to_string),
        provider: string_path(&job.payload, "provider").map(ToString::to_string),
        model: string_path(&job.payload, "model").map(ToString::to_string),
        prompt_files_present: 0,
        prompt_files_total: 0,
        selected_skill_ids: Vec::new(),
        planned_transcript_file: sessions_dir.join(format!("{file_safe_session}.jsonl")),
        planned_trajectory_file: sessions_dir.join(format!("{file_safe_session}.trajectory.jsonl")),
    };
    append_json_line(&queue_file, &item)?;
    Ok(WorkerJobExecutionResult {
        status: WorkerJobStatus::Succeeded,
        reason: "LLM worker job queued as durable runtime turn".to_string(),
        audit_path: None,
        artifact_refs: Some(json!({
            "runtimeQueueFile": queue_file,
            "runtimeQueueId": queue_id,
            "transcriptFile": item.planned_transcript_file,
            "trajectoryFile": item.planned_trajectory_file
        })),
        result: Some(json!({"runtimeQueueId": queue_id})),
    })
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
    let children = group_children(conn, group_id, &job.job_id)?;
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

fn persist_execution_result(
    conn: &Connection,
    job: &WorkerJob,
    result: &WorkerJobExecutionResult,
    now_ms: i64,
) -> io::Result<()> {
    if result.status == WorkerJobStatus::Pending {
        return Ok(());
    }
    let finished_at = if result.status.is_terminal() {
        Some(now_ms)
    } else {
        None
    };
    let status =
        if result.status == WorkerJobStatus::FailedRetryable && job.attempt < job.max_attempts {
            WorkerJobStatus::Pending
        } else {
            result.status
        };
    let available_at = if result.status == WorkerJobStatus::FailedRetryable
        && status == WorkerJobStatus::Pending
    {
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

fn jobs_with_expired_leases(conn: &Connection, now_ms: i64) -> io::Result<Vec<WorkerJob>> {
    let mut stmt = conn
        .prepare("SELECT * FROM jobs WHERE status IN ('leased','running') AND lease_expires_at_ms IS NOT NULL AND lease_expires_at_ms < ?1")
        .map_err(io::Error::other)?;
    stmt.query_map(params![now_ms], row_to_job)
        .map_err(io::Error::other)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(io::Error::other)
}

fn group_children(conn: &Connection, group_id: &str, self_id: &str) -> io::Result<Vec<WorkerJob>> {
    let mut stmt = conn
        .prepare(
            "SELECT * FROM jobs WHERE job_group_id=?1 AND job_id<>?2 ORDER BY created_at_ms ASC",
        )
        .map_err(io::Error::other)?;
    stmt.query_map(params![group_id, self_id], row_to_job)
        .map_err(io::Error::other)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(io::Error::other)
}

fn row_to_job(row: &rusqlite::Row<'_>) -> rusqlite::Result<WorkerJob> {
    let kind_text: String = row.get("kind")?;
    let status_text: String = row.get("status")?;
    let payload_text: String = row.get("payload_json")?;
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

fn string_path<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value.get(key).and_then(Value::as_str)
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

        let _ = fs::remove_dir_all(root);
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
        assert_eq!(first.status, WorkerRunOnceStatus::Completed);

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
        assert_eq!(later.status, WorkerRunOnceStatus::Completed);

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
