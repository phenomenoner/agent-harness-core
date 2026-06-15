use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};

const CRON_RUN_STORE_SCHEMA: &str = "agent-harness.cron-runs.v1";
const DEFAULT_JOB_FAILURE_QUARANTINE_THRESHOLD: usize = 3;
const DEFAULT_AGENT_FAILURE_QUARANTINE_THRESHOLD: usize = 10;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronRunAdmitOptions {
    pub harness_home: PathBuf,
    pub source_kind: String,
    pub source_id: String,
    pub entry_id: String,
    pub agent_id: String,
    pub scheduled_for_ms: i64,
    pub runtime_class: String,
    pub session_key: String,
    pub session_policy: String,
    pub max_attempts: i64,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronRunListOptions {
    pub harness_home: PathBuf,
    pub agent_id: Option<String>,
    pub entry_id: Option<String>,
    pub status: Option<String>,
    pub limit: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CronRunControlAction {
    Skip,
    Retry,
    Quarantine,
    Unquarantine,
}

impl std::str::FromStr for CronRunControlAction {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "skip" => Ok(Self::Skip),
            "retry" => Ok(Self::Retry),
            "quarantine" => Ok(Self::Quarantine),
            "unquarantine" => Ok(Self::Unquarantine),
            other => Err(format!("unsupported cron run control action: {other}")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronRunControlOptions {
    pub harness_home: PathBuf,
    pub action: CronRunControlAction,
    pub run_id: Option<String>,
    pub agent_id: Option<String>,
    pub entry_id: Option<String>,
    pub reason: String,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CronRunControlReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub database: PathBuf,
    pub action: CronRunControlAction,
    pub affected: usize,
    pub reason: String,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CronRunStatus {
    Admitted,
    WorkerEnqueued,
    RuntimeEnqueued,
    RuntimeRunning,
    Succeeded,
    FailedTerminal,
    RetryPending,
    Canceled,
    Expired,
    Skipped,
    Quarantined,
    AdmissionBlocked,
}

impl CronRunStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Admitted => "admitted",
            Self::WorkerEnqueued => "worker-enqueued",
            Self::RuntimeEnqueued => "runtime-enqueued",
            Self::RuntimeRunning => "runtime-running",
            Self::Succeeded => "succeeded",
            Self::FailedTerminal => "failed-terminal",
            Self::RetryPending => "retry-pending",
            Self::Canceled => "canceled",
            Self::Expired => "expired",
            Self::Skipped => "skipped",
            Self::Quarantined => "quarantined",
            Self::AdmissionBlocked => "admission-blocked",
        }
    }

    pub fn is_active(&self) -> bool {
        matches!(
            self,
            Self::Admitted
                | Self::WorkerEnqueued
                | Self::RuntimeEnqueued
                | Self::RuntimeRunning
                | Self::RetryPending
        )
    }

    fn from_str_lossy(value: &str) -> Self {
        match value {
            "admitted" => Self::Admitted,
            "worker-enqueued" => Self::WorkerEnqueued,
            "runtime-enqueued" => Self::RuntimeEnqueued,
            "runtime-running" => Self::RuntimeRunning,
            "succeeded" => Self::Succeeded,
            "failed-terminal" => Self::FailedTerminal,
            "retry-pending" => Self::RetryPending,
            "canceled" => Self::Canceled,
            "expired" => Self::Expired,
            "skipped" => Self::Skipped,
            "quarantined" => Self::Quarantined,
            "admission-blocked" => Self::AdmissionBlocked,
            _ => Self::AdmissionBlocked,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CronRun {
    pub run_id: String,
    pub source_kind: String,
    pub source_id: String,
    pub entry_id: String,
    pub agent_id: String,
    pub scheduled_for_ms: i64,
    pub runtime_queue_id: Option<String>,
    pub runtime_class: String,
    pub session_key: String,
    pub session_policy: String,
    pub status: CronRunStatus,
    pub attempt: i64,
    pub max_attempts: i64,
    pub worker_job_id: Option<String>,
    pub started_at_ms: Option<i64>,
    pub lease_expires_at_ms: Option<i64>,
    pub finished_at_ms: Option<i64>,
    pub failure_reason: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub quarantined: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CronRunSummary {
    pub total: usize,
    pub active: usize,
    pub terminal: usize,
    pub quarantined: usize,
    pub by_status: BTreeMap<String, usize>,
    pub by_agent_active: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CronRunListReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub database: PathBuf,
    pub runs: Vec<CronRun>,
    pub summary: CronRunSummary,
    pub warnings: Vec<String>,
}

pub fn cron_run_id(
    source_kind: &str,
    source_id: &str,
    entry_id: &str,
    scheduled_for_ms: i64,
) -> String {
    format!(
        "cronrun:{}:{}:{}:{}",
        normalize_key_part(source_kind),
        normalize_key_part(source_id),
        normalize_key_part(entry_id),
        scheduled_for_ms
    )
}

pub fn cron_runs_db_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("cron-runs")
        .join("cron-runs.sqlite")
}

pub fn init_cron_run_store(harness_home: impl AsRef<Path>) -> io::Result<PathBuf> {
    let db_file = cron_runs_db_file(harness_home);
    if let Some(parent) = db_file.parent() {
        fs::create_dir_all(parent)?;
    }
    let conn = Connection::open(&db_file).map_err(io::Error::other)?;
    create_schema(&conn).map_err(io::Error::other)?;
    Ok(db_file)
}

pub fn admit_cron_run(options: CronRunAdmitOptions) -> io::Result<CronRun> {
    let db_file = init_cron_run_store(&options.harness_home)?;
    let conn = Connection::open(&db_file).map_err(io::Error::other)?;
    create_schema(&conn).map_err(io::Error::other)?;
    if let Some(existing) = find_run_by_slot(
        &conn,
        &options.source_kind,
        &options.source_id,
        &options.entry_id,
        options.scheduled_for_ms,
    )? {
        return Ok(existing);
    }
    let run_id = cron_run_id(
        &options.source_kind,
        &options.source_id,
        &options.entry_id,
        options.scheduled_for_ms,
    );
    conn.execute(
        "INSERT INTO cron_runs (
            run_id, source_kind, source_id, entry_id, agent_id, scheduled_for_ms,
            runtime_queue_id, runtime_class, session_key, session_policy, status,
            attempt, max_attempts, worker_job_id, started_at_ms, lease_expires_at_ms,
            finished_at_ms, failure_reason, created_at_ms, updated_at_ms, quarantined
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, ?7, ?8, ?9, ?10, 0, ?11, NULL, NULL, NULL, NULL, NULL, ?12, ?12, 0)",
        params![
            run_id,
            options.source_kind,
            options.source_id,
            options.entry_id,
            options.agent_id,
            options.scheduled_for_ms,
            options.runtime_class,
            options.session_key,
            options.session_policy,
            CronRunStatus::Admitted.as_str(),
            options.max_attempts,
            options.now_ms,
        ],
    )
    .map_err(io::Error::other)?;
    find_run_by_id(&conn, &run_id)?
        .ok_or_else(|| io::Error::other(format!("inserted cron run not found: {run_id}")))
}

pub fn get_cron_run_by_slot(
    harness_home: impl AsRef<Path>,
    source_kind: &str,
    source_id: &str,
    entry_id: &str,
    scheduled_for_ms: i64,
) -> io::Result<Option<CronRun>> {
    let db_file = init_cron_run_store(harness_home)?;
    let conn = Connection::open(db_file).map_err(io::Error::other)?;
    find_run_by_slot(&conn, source_kind, source_id, entry_id, scheduled_for_ms)
}

pub fn mark_cron_run_worker_enqueued(
    harness_home: impl AsRef<Path>,
    run_id: &str,
    worker_job_id: &str,
    now_ms: i64,
) -> io::Result<()> {
    let db_file = init_cron_run_store(harness_home)?;
    let conn = Connection::open(db_file).map_err(io::Error::other)?;
    conn.execute(
        "UPDATE cron_runs SET status=?1, worker_job_id=?2, updated_at_ms=?3 WHERE run_id=?4 AND status NOT IN ('succeeded','failed-terminal','canceled','expired','skipped','quarantined')",
        params![CronRunStatus::WorkerEnqueued.as_str(), worker_job_id, now_ms, run_id],
    )
    .map_err(io::Error::other)?;
    Ok(())
}

pub fn mark_cron_run_runtime_enqueued(
    harness_home: impl AsRef<Path>,
    run_id: &str,
    runtime_queue_id: &str,
    now_ms: i64,
) -> io::Result<()> {
    let db_file = init_cron_run_store(harness_home)?;
    let conn = Connection::open(db_file).map_err(io::Error::other)?;
    conn.execute(
        "UPDATE cron_runs SET status=?1, runtime_queue_id=?2, updated_at_ms=?3 WHERE run_id=?4 AND status NOT IN ('succeeded','failed-terminal','canceled','expired','skipped','quarantined')",
        params![
            CronRunStatus::RuntimeEnqueued.as_str(),
            runtime_queue_id,
            now_ms,
            run_id
        ],
    )
    .map_err(io::Error::other)?;
    Ok(())
}

pub fn mark_cron_run_worker_status(
    harness_home: impl AsRef<Path>,
    run_id: &str,
    worker_status: &str,
    reason: &str,
    now_ms: i64,
) -> io::Result<()> {
    let Some(status) = cron_status_from_worker_status(worker_status) else {
        return Ok(());
    };
    let db_file = init_cron_run_store(harness_home)?;
    let conn = Connection::open(&db_file).map_err(io::Error::other)?;
    let Some(run) = find_run_by_id(&conn, run_id)? else {
        return Ok(());
    };
    let terminal = !status.is_active();
    conn.execute(
        "UPDATE cron_runs SET status=?1, failure_reason=?2, finished_at_ms=?3, updated_at_ms=?4 WHERE run_id=?5 AND status NOT IN ('succeeded','failed-terminal','canceled','expired','skipped','quarantined')",
        params![
            status.as_str(),
            if status == CronRunStatus::WorkerEnqueued {
                None::<String>
            } else {
                Some(reason.to_string())
            },
            terminal.then_some(now_ms),
            now_ms,
            run_id,
        ],
    )
    .map_err(io::Error::other)?;
    if status == CronRunStatus::FailedTerminal {
        maybe_quarantine_after_failure(&conn, &run.agent_id, &run.entry_id, reason, now_ms)?;
    }
    Ok(())
}

pub fn mark_cron_run_runtime_status_by_queue_id(
    harness_home: impl AsRef<Path>,
    runtime_queue_id: &str,
    run_once_status: &str,
    reason: &str,
    now_ms: i64,
) -> io::Result<()> {
    let db_file = init_cron_run_store(harness_home)?;
    let conn = Connection::open(&db_file).map_err(io::Error::other)?;
    let Some(run) = find_run_by_runtime_queue_id(&conn, runtime_queue_id)? else {
        return Ok(());
    };
    let status = cron_status_from_runtime_status(run_once_status);
    let terminal = !status.is_active();
    conn.execute(
        "UPDATE cron_runs SET status=?1, failure_reason=?2, finished_at_ms=?3, updated_at_ms=?4 WHERE run_id=?5 AND runtime_queue_id=?6 AND status NOT IN ('succeeded','failed-terminal','canceled','expired','skipped','quarantined')",
        params![
            status.as_str(),
            if status == CronRunStatus::Succeeded {
                None::<String>
            } else {
                Some(reason.to_string())
            },
            terminal.then_some(now_ms),
            now_ms,
            run.run_id,
            runtime_queue_id,
        ],
    )
    .map_err(io::Error::other)?;
    if status == CronRunStatus::FailedTerminal {
        maybe_quarantine_after_failure(&conn, &run.agent_id, &run.entry_id, reason, now_ms)?;
    }
    Ok(())
}

pub fn cron_run_active_count_for_job(
    harness_home: impl AsRef<Path>,
    agent_id: &str,
    entry_id: &str,
) -> io::Result<usize> {
    let db_file = init_cron_run_store(harness_home)?;
    let conn = Connection::open(db_file).map_err(io::Error::other)?;
    let mut stmt = conn
        .prepare(
            "SELECT status FROM cron_runs WHERE agent_id=?1 AND entry_id=?2 AND status IN ('admitted','worker-enqueued','runtime-enqueued','runtime-running','retry-pending')",
        )
        .map_err(io::Error::other)?;
    let count = stmt
        .query_map(params![agent_id, entry_id], |row| row.get::<_, String>(0))
        .map_err(io::Error::other)?
        .filter_map(Result::ok)
        .filter(|status| CronRunStatus::from_str_lossy(status).is_active())
        .count();
    Ok(count)
}

pub fn cron_run_active_count_for_agent(
    harness_home: impl AsRef<Path>,
    agent_id: &str,
) -> io::Result<usize> {
    let db_file = init_cron_run_store(harness_home)?;
    let conn = Connection::open(db_file).map_err(io::Error::other)?;
    let count = conn
        .query_row(
            "SELECT COUNT(*) FROM cron_runs WHERE agent_id=?1 AND status IN ('admitted','worker-enqueued','runtime-enqueued','runtime-running','retry-pending')",
            params![agent_id],
            |row| row.get::<_, i64>(0),
        )
        .map_err(io::Error::other)?;
    Ok(usize::try_from(count).unwrap_or(usize::MAX))
}

pub fn cron_run_is_quarantined(
    harness_home: impl AsRef<Path>,
    agent_id: &str,
    entry_id: &str,
) -> io::Result<Option<String>> {
    let db_file = init_cron_run_store(harness_home)?;
    let conn = Connection::open(db_file).map_err(io::Error::other)?;
    quarantine_reason(&conn, agent_id, Some(entry_id))
}

pub fn cron_run_worker_dispatch_blocker(
    harness_home: impl AsRef<Path>,
    run_id: &str,
    worker_job_id: &str,
) -> io::Result<Option<String>> {
    let db_file = init_cron_run_store(harness_home)?;
    let conn = Connection::open(db_file).map_err(io::Error::other)?;
    let Some(run) = find_run_by_id(&conn, run_id)? else {
        return Ok(Some(format!(
            "cron run `{run_id}` no longer exists; skipping worker dispatch"
        )));
    };
    if let Some(blocker) = cron_run_control_blocker(&conn, &run)? {
        return Ok(Some(blocker));
    }
    if let Some(linked_job_id) = run.worker_job_id.as_deref()
        && linked_job_id != worker_job_id
    {
        return Ok(Some(format!(
            "cron run `{}` is linked to worker job `{linked_job_id}`, not `{worker_job_id}`",
            run.run_id
        )));
    }
    if run.status == CronRunStatus::RetryPending && run.worker_job_id.is_none() {
        return Ok(Some(format!(
            "cron run `{}` is pending scheduler retry and is not linked to worker job `{worker_job_id}`",
            run.run_id
        )));
    }
    Ok(None)
}

pub fn cron_run_runtime_dispatch_blocker(
    harness_home: impl AsRef<Path>,
    run_id: &str,
    runtime_queue_id: &str,
    now_ms: i64,
) -> io::Result<Option<String>> {
    let db_file = init_cron_run_store(harness_home)?;
    let conn = Connection::open(db_file).map_err(io::Error::other)?;
    let Some(run) = find_run_by_id(&conn, run_id)? else {
        return Ok(Some(format!(
            "cron run `{run_id}` no longer exists; skipping runtime dispatch"
        )));
    };
    if let Some(blocker) = cron_run_control_blocker(&conn, &run)? {
        return Ok(Some(blocker));
    }
    if let Some(linked_queue_id) = run.runtime_queue_id.as_deref() {
        if linked_queue_id != runtime_queue_id {
            return Ok(Some(format!(
                "cron run `{}` is linked to runtime queue `{linked_queue_id}`, not `{runtime_queue_id}`",
                run.run_id
            )));
        }
        return Ok(None);
    }
    if matches!(
        run.status,
        CronRunStatus::Admitted | CronRunStatus::WorkerEnqueued
    ) {
        conn.execute(
            "UPDATE cron_runs SET status=?1, runtime_queue_id=?2, updated_at_ms=?3 WHERE run_id=?4 AND runtime_queue_id IS NULL AND status IN ('admitted','worker-enqueued')",
            params![
                CronRunStatus::RuntimeEnqueued.as_str(),
                runtime_queue_id,
                now_ms,
                run.run_id,
            ],
        )
        .map_err(io::Error::other)?;
        return Ok(None);
    }
    Ok(Some(format!(
        "cron run `{}` is {} and is not linked to runtime queue `{runtime_queue_id}`",
        run.run_id,
        run.status.as_str()
    )))
}

pub fn list_cron_runs(options: CronRunListOptions) -> io::Result<CronRunListReport> {
    let db_file = init_cron_run_store(&options.harness_home)?;
    let conn = Connection::open(&db_file).map_err(io::Error::other)?;
    let limit = options.limit.max(1);
    let mut stmt = conn
        .prepare(
            "SELECT * FROM cron_runs
             WHERE (?1 IS NULL OR agent_id = ?1)
               AND (?2 IS NULL OR entry_id = ?2)
               AND (?3 IS NULL OR status = ?3)
             ORDER BY updated_at_ms DESC
             LIMIT ?4",
        )
        .map_err(io::Error::other)?;
    let runs = stmt
        .query_map(
            params![
                options.agent_id.as_deref(),
                options.entry_id.as_deref(),
                options.status.as_deref(),
                i64::try_from(limit).unwrap_or(i64::MAX),
            ],
            row_to_cron_run,
        )
        .map_err(io::Error::other)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(io::Error::other)?;
    let summary = summarize_cron_runs(&runs);
    Ok(CronRunListReport {
        schema: CRON_RUN_STORE_SCHEMA,
        harness_home: options.harness_home,
        database: db_file,
        runs,
        summary,
        warnings: Vec::new(),
    })
}

pub fn collect_cron_run_summary(harness_home: impl AsRef<Path>) -> io::Result<CronRunListReport> {
    list_cron_runs(CronRunListOptions {
        harness_home: harness_home.as_ref().to_path_buf(),
        agent_id: None,
        entry_id: None,
        status: None,
        limit: 200,
    })
}

pub fn control_cron_run(options: CronRunControlOptions) -> io::Result<CronRunControlReport> {
    let db_file = init_cron_run_store(&options.harness_home)?;
    let conn = Connection::open(&db_file).map_err(io::Error::other)?;
    let mut warnings = Vec::new();
    let affected = match options.action {
        CronRunControlAction::Skip => {
            if let Some(run_id) = options.run_id.as_deref() {
                conn.execute(
                    "UPDATE cron_runs SET status=?1, finished_at_ms=?2, failure_reason=?3, updated_at_ms=?2 WHERE run_id=?4 AND status NOT IN ('succeeded','failed-terminal','canceled','expired','skipped')",
                    params![CronRunStatus::Skipped.as_str(), options.now_ms, options.reason, run_id],
                )
                .map_err(io::Error::other)?
            } else {
                warnings.push("skip requires --run-id".to_string());
                0
            }
        }
        CronRunControlAction::Retry => {
            if let Some(run_id) = options.run_id.as_deref() {
                conn.execute(
                    "UPDATE cron_runs SET status=?1, runtime_queue_id=NULL, worker_job_id=NULL, started_at_ms=NULL, lease_expires_at_ms=NULL, finished_at_ms=NULL, failure_reason=?2, attempt=attempt+1, updated_at_ms=?3 WHERE run_id=?4",
                    params![
                        CronRunStatus::RetryPending.as_str(),
                        options.reason,
                        options.now_ms,
                        run_id
                    ],
                )
                .map_err(io::Error::other)?
            } else {
                warnings.push("retry requires --run-id".to_string());
                0
            }
        }
        CronRunControlAction::Quarantine => {
            let agent_id = options.agent_id.as_deref();
            let entry_id = options.entry_id.as_deref();
            match (agent_id, entry_id, options.run_id.as_deref()) {
                (_, _, Some(run_id)) => {
                    quarantine_by_run_id(&conn, run_id, &options.reason, options.now_ms)?
                }
                (Some(agent_id), entry_id, None) => {
                    upsert_quarantine(&conn, agent_id, entry_id, &options.reason, options.now_ms)?
                }
                _ => {
                    warnings.push(
                        "quarantine requires --run-id or --agent-id with optional --entry-id"
                            .to_string(),
                    );
                    0
                }
            }
        }
        CronRunControlAction::Unquarantine => {
            let agent_id = options.agent_id.as_deref();
            let entry_id = options.entry_id.as_deref();
            match (agent_id, entry_id, options.run_id.as_deref()) {
                (_, _, Some(run_id)) => unquarantine_by_run_id(&conn, run_id, options.now_ms)?,
                (Some(agent_id), entry_id, None) => conn
                    .execute(
                        "UPDATE cron_quarantine SET active=0, updated_at_ms=?1 WHERE agent_id=?2 AND (?3 IS NULL OR entry_id=?3) AND active=1",
                        params![options.now_ms, agent_id, entry_id],
                    )
                    .map_err(io::Error::other)?,
                _ => {
                    warnings.push(
                        "unquarantine requires --run-id or --agent-id with optional --entry-id"
                            .to_string(),
                    );
                    0
                }
            }
        }
    };
    Ok(CronRunControlReport {
        schema: CRON_RUN_STORE_SCHEMA,
        harness_home: options.harness_home,
        database: db_file,
        action: options.action,
        affected,
        reason: options.reason,
        warnings,
    })
}

fn create_schema(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS cron_runs (
            run_id TEXT PRIMARY KEY,
            source_kind TEXT NOT NULL,
            source_id TEXT NOT NULL,
            entry_id TEXT NOT NULL,
            agent_id TEXT NOT NULL,
            scheduled_for_ms INTEGER NOT NULL,
            runtime_queue_id TEXT,
            runtime_class TEXT NOT NULL,
            session_key TEXT NOT NULL,
            session_policy TEXT NOT NULL,
            status TEXT NOT NULL,
            attempt INTEGER NOT NULL DEFAULT 0,
            max_attempts INTEGER NOT NULL DEFAULT 3,
            worker_job_id TEXT,
            started_at_ms INTEGER,
            lease_expires_at_ms INTEGER,
            finished_at_ms INTEGER,
            failure_reason TEXT,
            created_at_ms INTEGER NOT NULL,
            updated_at_ms INTEGER NOT NULL,
            quarantined INTEGER NOT NULL DEFAULT 0,
            UNIQUE(source_kind, source_id, entry_id, scheduled_for_ms)
        );
        CREATE INDEX IF NOT EXISTS idx_cron_runs_agent_status ON cron_runs(agent_id, status);
        CREATE INDEX IF NOT EXISTS idx_cron_runs_entry_status ON cron_runs(agent_id, entry_id, status);
        CREATE INDEX IF NOT EXISTS idx_cron_runs_runtime_queue ON cron_runs(runtime_queue_id);
        CREATE TABLE IF NOT EXISTS cron_quarantine (
            scope_id TEXT PRIMARY KEY,
            agent_id TEXT NOT NULL,
            entry_id TEXT,
            reason TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 1,
            created_at_ms INTEGER NOT NULL,
            updated_at_ms INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_cron_quarantine_active ON cron_quarantine(agent_id, entry_id, active);",
    )
}

fn find_run_by_slot(
    conn: &Connection,
    source_kind: &str,
    source_id: &str,
    entry_id: &str,
    scheduled_for_ms: i64,
) -> io::Result<Option<CronRun>> {
    conn.query_row(
        "SELECT * FROM cron_runs WHERE source_kind=?1 AND source_id=?2 AND entry_id=?3 AND scheduled_for_ms=?4",
        params![source_kind, source_id, entry_id, scheduled_for_ms],
        row_to_cron_run,
    )
    .optional()
    .map_err(io::Error::other)
}

fn find_run_by_id(conn: &Connection, run_id: &str) -> io::Result<Option<CronRun>> {
    conn.query_row(
        "SELECT * FROM cron_runs WHERE run_id=?1",
        params![run_id],
        row_to_cron_run,
    )
    .optional()
    .map_err(io::Error::other)
}

fn find_run_by_runtime_queue_id(
    conn: &Connection,
    runtime_queue_id: &str,
) -> io::Result<Option<CronRun>> {
    conn.query_row(
        "SELECT * FROM cron_runs WHERE runtime_queue_id=?1",
        params![runtime_queue_id],
        row_to_cron_run,
    )
    .optional()
    .map_err(io::Error::other)
}

fn row_to_cron_run(row: &rusqlite::Row<'_>) -> rusqlite::Result<CronRun> {
    let status: String = row.get("status")?;
    let quarantined: i64 = row.get("quarantined")?;
    Ok(CronRun {
        run_id: row.get("run_id")?,
        source_kind: row.get("source_kind")?,
        source_id: row.get("source_id")?,
        entry_id: row.get("entry_id")?,
        agent_id: row.get("agent_id")?,
        scheduled_for_ms: row.get("scheduled_for_ms")?,
        runtime_queue_id: row.get("runtime_queue_id")?,
        runtime_class: row.get("runtime_class")?,
        session_key: row.get("session_key")?,
        session_policy: row.get("session_policy")?,
        status: CronRunStatus::from_str_lossy(&status),
        attempt: row.get("attempt")?,
        max_attempts: row.get("max_attempts")?,
        worker_job_id: row.get("worker_job_id")?,
        started_at_ms: row.get("started_at_ms")?,
        lease_expires_at_ms: row.get("lease_expires_at_ms")?,
        finished_at_ms: row.get("finished_at_ms")?,
        failure_reason: row.get("failure_reason")?,
        created_at_ms: row.get("created_at_ms")?,
        updated_at_ms: row.get("updated_at_ms")?,
        quarantined: quarantined != 0,
    })
}

fn summarize_cron_runs(runs: &[CronRun]) -> CronRunSummary {
    let mut summary = CronRunSummary {
        total: runs.len(),
        ..CronRunSummary::default()
    };
    for run in runs {
        *summary
            .by_status
            .entry(run.status.as_str().to_string())
            .or_insert(0) += 1;
        if run.status.is_active() {
            summary.active += 1;
            *summary
                .by_agent_active
                .entry(run.agent_id.clone())
                .or_insert(0) += 1;
        } else {
            summary.terminal += 1;
        }
        if run.quarantined || run.status == CronRunStatus::Quarantined {
            summary.quarantined += 1;
        }
    }
    summary
}

fn cron_run_control_blocker(conn: &Connection, run: &CronRun) -> io::Result<Option<String>> {
    if !run.status.is_active() {
        return Ok(Some(format!(
            "cron run `{}` is {}; skipping dispatch",
            run.run_id,
            run.status.as_str()
        )));
    }
    if run.quarantined || run.status == CronRunStatus::Quarantined {
        return Ok(Some(format!(
            "cron run `{}` is quarantined; skipping dispatch",
            run.run_id
        )));
    }
    if let Some(reason) = quarantine_reason(conn, &run.agent_id, Some(&run.entry_id))? {
        return Ok(Some(format!(
            "cron run `{}` is blocked by job quarantine: {reason}",
            run.run_id
        )));
    }
    if let Some(reason) = quarantine_reason(conn, &run.agent_id, None)? {
        return Ok(Some(format!(
            "cron run `{}` is blocked by agent quarantine: {reason}",
            run.run_id
        )));
    }
    Ok(None)
}

fn cron_status_from_runtime_status(status: &str) -> CronRunStatus {
    match status {
        "completed" => CronRunStatus::Succeeded,
        "retry-pending" | "lease-busy" => CronRunStatus::RetryPending,
        "canceled" => CronRunStatus::Canceled,
        "skipped" => CronRunStatus::Skipped,
        "timeout"
        | "dead-letter"
        | "failed-terminal"
        | "spawn-failed"
        | "protocol-error"
        | "preflight-blocked"
        | "no-runtime-plan"
        | "no-prepared-execution" => CronRunStatus::FailedTerminal,
        "no-work" => CronRunStatus::RuntimeEnqueued,
        _ => CronRunStatus::RuntimeRunning,
    }
}

fn cron_status_from_worker_status(status: &str) -> Option<CronRunStatus> {
    match status {
        "pending" | "leased" | "running" => Some(CronRunStatus::WorkerEnqueued),
        "failed-retryable" => Some(CronRunStatus::RetryPending),
        "failed-terminal" => Some(CronRunStatus::FailedTerminal),
        "canceled" => Some(CronRunStatus::Canceled),
        "expired" => Some(CronRunStatus::Expired),
        "succeeded" => None,
        _ => Some(CronRunStatus::FailedTerminal),
    }
}

fn maybe_quarantine_after_failure(
    conn: &Connection,
    agent_id: &str,
    entry_id: &str,
    reason: &str,
    now_ms: i64,
) -> io::Result<()> {
    let job_failures = count_recent_failed_runs(conn, Some(agent_id), Some(entry_id))?;
    if job_failures >= DEFAULT_JOB_FAILURE_QUARANTINE_THRESHOLD {
        upsert_quarantine(conn, agent_id, Some(entry_id), reason, now_ms)?;
    }
    let agent_failures = count_recent_failed_runs(conn, Some(agent_id), None)?;
    if agent_failures >= DEFAULT_AGENT_FAILURE_QUARANTINE_THRESHOLD {
        upsert_quarantine(conn, agent_id, None, reason, now_ms)?;
    }
    Ok(())
}

fn count_recent_failed_runs(
    conn: &Connection,
    agent_id: Option<&str>,
    entry_id: Option<&str>,
) -> io::Result<usize> {
    let count = conn
        .query_row(
            "SELECT COUNT(*) FROM cron_runs
             WHERE status='failed-terminal'
               AND (?1 IS NULL OR agent_id = ?1)
               AND (?2 IS NULL OR entry_id = ?2)",
            params![agent_id, entry_id],
            |row| row.get::<_, i64>(0),
        )
        .map_err(io::Error::other)?;
    Ok(usize::try_from(count).unwrap_or(usize::MAX))
}

fn quarantine_reason(
    conn: &Connection,
    agent_id: &str,
    entry_id: Option<&str>,
) -> io::Result<Option<String>> {
    conn.query_row(
        "SELECT reason FROM cron_quarantine WHERE agent_id=?1 AND active=1 AND (entry_id IS NULL OR entry_id=?2) ORDER BY CASE WHEN entry_id IS NULL THEN 1 ELSE 0 END, updated_at_ms DESC LIMIT 1",
        params![agent_id, entry_id],
        |row| row.get::<_, String>(0),
    )
    .optional()
    .map_err(io::Error::other)
}

fn upsert_quarantine(
    conn: &Connection,
    agent_id: &str,
    entry_id: Option<&str>,
    reason: &str,
    now_ms: i64,
) -> io::Result<usize> {
    let scope_id = quarantine_scope_id(agent_id, entry_id);
    conn.execute(
        "INSERT INTO cron_quarantine (scope_id, agent_id, entry_id, reason, active, created_at_ms, updated_at_ms)
         VALUES (?1, ?2, ?3, ?4, 1, ?5, ?5)
         ON CONFLICT(scope_id) DO UPDATE SET reason=excluded.reason, active=1, updated_at_ms=excluded.updated_at_ms",
        params![scope_id, agent_id, entry_id, reason, now_ms],
    )
    .map_err(io::Error::other)?;
    conn.execute(
        "UPDATE cron_runs SET quarantined=1, status=?1, failure_reason=?2, updated_at_ms=?3 WHERE agent_id=?4 AND (?5 IS NULL OR entry_id=?5) AND status IN ('admitted','worker-enqueued','runtime-enqueued','runtime-running','retry-pending')",
        params![CronRunStatus::Quarantined.as_str(), reason, now_ms, agent_id, entry_id],
    )
    .map_err(io::Error::other)
}

fn quarantine_by_run_id(
    conn: &Connection,
    run_id: &str,
    reason: &str,
    now_ms: i64,
) -> io::Result<usize> {
    let Some(run) = find_run_by_id(conn, run_id)? else {
        return Ok(0);
    };
    upsert_quarantine(conn, &run.agent_id, Some(&run.entry_id), reason, now_ms)
}

fn unquarantine_by_run_id(conn: &Connection, run_id: &str, now_ms: i64) -> io::Result<usize> {
    let Some(run) = find_run_by_id(conn, run_id)? else {
        return Ok(0);
    };
    conn.execute(
        "UPDATE cron_quarantine SET active=0, updated_at_ms=?1 WHERE agent_id=?2 AND entry_id=?3 AND active=1",
        params![now_ms, run.agent_id, run.entry_id],
    )
    .map_err(io::Error::other)
}

fn quarantine_scope_id(agent_id: &str, entry_id: Option<&str>) -> String {
    match entry_id {
        Some(entry_id) => format!(
            "job:{}:{}",
            normalize_key_part(agent_id),
            normalize_key_part(entry_id)
        ),
        None => format!("agent:{}", normalize_key_part(agent_id)),
    }
}

fn normalize_key_part(value: &str) -> String {
    let mut normalized = String::new();
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            normalized.push(ch.to_ascii_lowercase());
        } else if matches!(ch, '-' | '_' | '.' | '@') {
            normalized.push(ch);
        } else {
            normalized.push('_');
        }
    }
    let normalized = normalized.trim_matches('_').to_string();
    if normalized.is_empty() {
        "unknown".to_string()
    } else {
        normalized
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_root(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "agent_harness_cron_runs_test_{}_{}",
            name,
            std::process::id()
        ))
    }

    #[test]
    fn cron_run_store_admits_idempotent_slot_and_links_runtime() {
        let root = temp_root("admit_idempotent");
        let harness_home = root.join(".agent-harness");
        let first = admit_cron_run(CronRunAdmitOptions {
            harness_home: harness_home.clone(),
            source_kind: "native-cron".to_string(),
            source_id: "source".to_string(),
            entry_id: "daily".to_string(),
            agent_id: "main".to_string(),
            scheduled_for_ms: 1234,
            runtime_class: "cron".to_string(),
            session_key: "cron:main:daily:1234".to_string(),
            session_policy: "one-shot".to_string(),
            max_attempts: 3,
            now_ms: 2000,
        })
        .unwrap();
        let second = admit_cron_run(CronRunAdmitOptions {
            now_ms: 3000,
            ..CronRunAdmitOptions {
                harness_home: harness_home.clone(),
                source_kind: "native-cron".to_string(),
                source_id: "source".to_string(),
                entry_id: "daily".to_string(),
                agent_id: "main".to_string(),
                scheduled_for_ms: 1234,
                runtime_class: "cron".to_string(),
                session_key: "cron:main:daily:1234".to_string(),
                session_policy: "one-shot".to_string(),
                max_attempts: 3,
                now_ms: 2000,
            }
        })
        .unwrap();
        assert_eq!(first.run_id, second.run_id);

        mark_cron_run_worker_enqueued(&harness_home, &first.run_id, "worker-1", 3001).unwrap();
        mark_cron_run_runtime_enqueued(&harness_home, &first.run_id, "runtime-1", 3002).unwrap();
        mark_cron_run_runtime_status_by_queue_id(
            &harness_home,
            "runtime-1",
            "completed",
            "done",
            3003,
        )
        .unwrap();
        let report = collect_cron_run_summary(&harness_home).unwrap();
        assert_eq!(report.summary.by_status.get("succeeded"), Some(&1));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn cron_run_store_quarantines_after_repeated_job_failures() {
        let root = temp_root("quarantine_after_failures");
        let harness_home = root.join(".agent-harness");
        for slot in 0..3 {
            let run = admit_cron_run(CronRunAdmitOptions {
                harness_home: harness_home.clone(),
                source_kind: "native-cron".to_string(),
                source_id: "source".to_string(),
                entry_id: "daily".to_string(),
                agent_id: "main".to_string(),
                scheduled_for_ms: 1000 + slot,
                runtime_class: "cron".to_string(),
                session_key: format!("cron:main:daily:{}", 1000 + slot),
                session_policy: "one-shot".to_string(),
                max_attempts: 3,
                now_ms: 2000 + slot,
            })
            .unwrap();
            let queue_id = format!("runtime-{slot}");
            mark_cron_run_runtime_enqueued(&harness_home, &run.run_id, &queue_id, 3000 + slot)
                .unwrap();
            mark_cron_run_runtime_status_by_queue_id(
                &harness_home,
                &queue_id,
                "failed-terminal",
                "failed",
                4000 + slot,
            )
            .unwrap();
        }
        assert!(
            cron_run_is_quarantined(&harness_home, "main", "daily")
                .unwrap()
                .is_some()
        );
        let _ = fs::remove_dir_all(root);
    }
}
