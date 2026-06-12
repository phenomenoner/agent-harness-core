use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};

const BACKGROUND_REGISTRY_SCHEMA: &str = "agent-harness.background-registry.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackgroundTaskUpsertOptions {
    pub harness_home: PathBuf,
    pub task: BackgroundTaskRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackgroundTaskListOptions {
    pub harness_home: PathBuf,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackgroundTaskRecord {
    pub id: String,
    pub kind: String,
    pub parent_queue_id: Option<String>,
    pub trace_id: Option<String>,
    pub platform: Option<String>,
    pub channel_id: Option<String>,
    pub user_id: Option<String>,
    pub session_key: Option<String>,
    pub agent_id: Option<String>,
    pub owner: String,
    pub process_id: Option<u32>,
    pub artifact_root: Option<PathBuf>,
    pub cancel_strategy: String,
    pub ttl_ms: i64,
    pub heartbeat_at_ms: Option<i64>,
    pub status: BackgroundTaskStatus,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum BackgroundTaskStatus {
    Running,
    Completed,
    Blocked,
    Canceled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BackgroundTaskRegistryReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub database_file: PathBuf,
    pub tasks: Vec<BackgroundTaskView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BackgroundTaskView {
    pub record: BackgroundTaskRecord,
    pub stale: bool,
    pub heartbeat_age_ms: Option<i64>,
}

pub fn upsert_background_task(
    options: BackgroundTaskUpsertOptions,
) -> io::Result<BackgroundTaskRegistryReport> {
    let database_file = background_db_file(&options.harness_home);
    let connection = open_background_db(&database_file)?;
    let updated_at_ms = options.task.updated_at_ms;
    let status = status_str(options.task.status);
    let task_json = serde_json::to_string(&options.task).map_err(io::Error::other)?;
    connection
        .execute(
            "INSERT INTO background_tasks (id, kind, status, task_json, updated_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(id) DO UPDATE SET
               kind=excluded.kind,
               status=excluded.status,
               task_json=excluded.task_json,
               updated_at_ms=excluded.updated_at_ms",
            params![
                &options.task.id,
                &options.task.kind,
                status,
                task_json,
                updated_at_ms
            ],
        )
        .map_err(io::Error::other)?;
    list_background_tasks(BackgroundTaskListOptions {
        harness_home: options.harness_home,
        now_ms: updated_at_ms,
    })
}

pub fn list_background_tasks(
    options: BackgroundTaskListOptions,
) -> io::Result<BackgroundTaskRegistryReport> {
    let database_file = background_db_file(&options.harness_home);
    let connection = open_background_db(&database_file)?;
    let mut statement = connection
        .prepare("SELECT task_json FROM background_tasks ORDER BY updated_at_ms DESC, id ASC")
        .map_err(io::Error::other)?;
    let rows = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(io::Error::other)?;
    let mut tasks = Vec::new();
    for row in rows {
        let raw = row.map_err(io::Error::other)?;
        let record =
            serde_json::from_str::<BackgroundTaskRecord>(&raw).map_err(io::Error::other)?;
        let heartbeat_age_ms = record
            .heartbeat_at_ms
            .map(|at_ms| options.now_ms.saturating_sub(at_ms));
        let stale = record.status == BackgroundTaskStatus::Running
            && heartbeat_age_ms.is_some_and(|age| age > record.ttl_ms);
        tasks.push(BackgroundTaskView {
            record,
            stale,
            heartbeat_age_ms,
        });
    }
    Ok(BackgroundTaskRegistryReport {
        schema: BACKGROUND_REGISTRY_SCHEMA,
        harness_home: options.harness_home,
        database_file,
        tasks,
    })
}

fn background_db_file(harness_home: &Path) -> PathBuf {
    harness_home
        .join("state")
        .join("workers")
        .join("worker-store.sqlite")
}

fn open_background_db(path: &Path) -> io::Result<Connection> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let connection = Connection::open(path).map_err(io::Error::other)?;
    connection
        .execute_batch(
            "CREATE TABLE IF NOT EXISTS background_tasks (
                id TEXT PRIMARY KEY,
                kind TEXT NOT NULL,
                status TEXT NOT NULL,
                task_json TEXT NOT NULL,
                updated_at_ms INTEGER NOT NULL
             );
             CREATE INDEX IF NOT EXISTS idx_background_tasks_status ON background_tasks(status);
             CREATE INDEX IF NOT EXISTS idx_background_tasks_kind ON background_tasks(kind);",
        )
        .map_err(io::Error::other)?;
    Ok(connection)
}

fn status_str(status: BackgroundTaskStatus) -> &'static str {
    match status {
        BackgroundTaskStatus::Running => "running",
        BackgroundTaskStatus::Completed => "completed",
        BackgroundTaskStatus::Blocked => "blocked",
        BackgroundTaskStatus::Canceled => "canceled",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn background_registry_marks_stale_running_tasks() {
        let root = temp_root("background_registry_marks_stale_running_tasks");
        let harness_home = root.join(".agent-harness");

        upsert_background_task(BackgroundTaskUpsertOptions {
            harness_home: harness_home.clone(),
            task: BackgroundTaskRecord {
                id: "task-1".to_string(),
                kind: "local-server".to_string(),
                parent_queue_id: Some("q-1".to_string()),
                trace_id: Some("trace-1".to_string()),
                platform: Some("telegram".to_string()),
                channel_id: Some("dm".to_string()),
                user_id: Some("user".to_string()),
                session_key: Some("session".to_string()),
                agent_id: Some("main".to_string()),
                owner: "agent".to_string(),
                process_id: Some(123),
                artifact_root: Some(root.join("artifact")),
                cancel_strategy: "stop-file".to_string(),
                ttl_ms: 1_000,
                heartbeat_at_ms: Some(1_000),
                status: BackgroundTaskStatus::Running,
                updated_at_ms: 1_000,
            },
        })
        .unwrap();

        let report = list_background_tasks(BackgroundTaskListOptions {
            harness_home,
            now_ms: 3_500,
        })
        .unwrap();

        assert_eq!(report.tasks.len(), 1);
        assert!(report.tasks[0].stale);
        assert_eq!(
            report.tasks[0].record.parent_queue_id.as_deref(),
            Some("q-1")
        );

        let _ = fs::remove_dir_all(root);
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-background-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
