use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use agent_harness_core::{
    WorkerEnqueueOptions, WorkerJobKind, WorkerRunOnceOptions, WorkerRunOnceStatus,
    WorkerStatusOptions, collect_worker_status, enqueue_worker_job, run_worker_once,
    worker_db_file,
};
use rusqlite::{Connection, params};
use serde_json::{Value, json};

const LEASE_MS: i64 = 30_000;

#[test]
fn llm_runtime_queue_is_not_terminal_until_correlated_runtime_receipt() {
    let root = temp_root("llm_runtime_queue_is_not_terminal_until_correlated_runtime_receipt");
    let harness_home = root.join(".agent-harness");
    let source = root.join("source");
    let workspace = source.join("workspace");
    fs::create_dir_all(&workspace).unwrap();

    let child =
        enqueue_worker_job(llm_child_options(&harness_home, &source, &workspace, 1_000)).unwrap();
    let dispatch = run_worker_once(WorkerRunOnceOptions {
        harness_home: harness_home.clone(),
        lane: Some("llm".to_string()),
        worker_id: "llm-worker".to_string(),
        lease_ms: LEASE_MS,
        now_ms: 1_001,
    })
    .unwrap();

    assert_eq!(dispatch.status, WorkerRunOnceStatus::Dispatched);
    assert_eq!(
        dispatch.job.as_ref().unwrap().status.as_str(),
        "runtime-queued"
    );
    assert!(dispatch.job.as_ref().unwrap().finished_at_ms.is_none());
    let runtime_queue_id = dispatch
        .result
        .as_ref()
        .and_then(|result| result.result.as_ref())
        .and_then(|value| value.get("runtimeQueueId"))
        .and_then(Value::as_str)
        .unwrap()
        .to_string();

    let receipts_file = harness_home
        .join("state")
        .join("runtime-queue")
        .join("run-once-receipts.jsonl");
    fs::create_dir_all(receipts_file.parent().unwrap()).unwrap();
    fs::write(
        &receipts_file,
        format!(
            "{}\n",
            json!({
                "queueId": "different-child-queue",
                "status": "completed",
                "reason": "must not terminalize this worker"
            })
        ),
    )
    .unwrap();

    enqueue_worker_job(watchdog_options(&harness_home, &source, &workspace, 1_002)).unwrap();
    let before_terminal = run_worker_once(WorkerRunOnceOptions {
        harness_home: harness_home.clone(),
        lane: Some("watchdog".to_string()),
        worker_id: "watchdog-worker".to_string(),
        lease_ms: LEASE_MS,
        now_ms: 1_003,
    })
    .unwrap();
    assert_eq!(before_terminal.status, WorkerRunOnceStatus::Rescheduled);
    assert_eq!(
        worker_status(&harness_home, &child.job.job_id),
        "runtime-queued"
    );
    assert_eq!(
        collect_worker_status(WorkerStatusOptions {
            harness_home: harness_home.clone(),
        })
        .unwrap()
        .totals
        .runtime_queued,
        1
    );

    fs::write(
        &receipts_file,
        format!(
            "{}\n",
            json!({
                "queueId": runtime_queue_id,
                "status": "completed",
                "reason": "correlated child runtime completed"
            })
        ),
    )
    .unwrap();

    let after_terminal = run_worker_once(WorkerRunOnceOptions {
        harness_home: harness_home.clone(),
        lane: Some("watchdog".to_string()),
        worker_id: "watchdog-worker".to_string(),
        lease_ms: LEASE_MS,
        now_ms: 20_000,
    })
    .unwrap();
    assert_eq!(after_terminal.status, WorkerRunOnceStatus::Completed);
    assert_eq!(worker_status(&harness_home, &child.job.job_id), "succeeded");
    assert_eq!(
        worker_result(&harness_home, &child.job.job_id)["runtimeQueueId"],
        runtime_queue_id
    );
    let mailbox = mailbox_result(&harness_home, &child.job.job_id);
    assert_eq!(mailbox["rowCount"], 1);
    assert_eq!(mailbox["autoResumable"], false);
    assert_eq!(mailbox["state"], "unread");
    assert_eq!(mailbox["outcome"], "succeeded");
    assert_eq!(
        mailbox["redactedSummary"],
        "worker runtime terminal correlated as succeeded"
    );

    let status = collect_worker_status(WorkerStatusOptions { harness_home }).unwrap();
    assert!(
        status
            .by_lane
            .iter()
            .any(|lane| lane.lane == "llm" && lane.totals.pending == 1)
    );

    let _ = fs::remove_dir_all(root);
}

fn llm_child_options(
    harness_home: &Path,
    source: &Path,
    workspace: &Path,
    now_ms: i64,
) -> WorkerEnqueueOptions {
    WorkerEnqueueOptions {
        harness_home: harness_home.to_path_buf(),
        kind: WorkerJobKind::LlmSubagent,
        lane: Some("llm".to_string()),
        payload: json!({
            "runId": "runtime-child",
            "sourceHome": source,
            "sourceWorkspace": workspace,
            "agentId": "researcher",
            "sessionKey": "subagent:runtime-child:researcher",
            "messageText": "collect bounded evidence",
            "platform": "subagent",
            "channelId": "internal-child",
            "userId": "main"
        }),
        idempotency_key: Some("runtime-child".to_string()),
        parent_job_id: None,
        job_group_id: Some("group-runtime".to_string()),
        master_agent_id: Some("main".to_string()),
        master_session_key: Some("master-session".to_string()),
        wake_policy: None,
        source: Some("test".to_string()),
        priority: 0,
        available_at_ms: Some(now_ms),
        max_attempts: 3,
        timeout_ms: Some(300_000),
        cascade_timeout_ms: None,
        rate_key: None,
        concurrency_group_key: None,
        now_ms,
    }
}

fn watchdog_options(
    harness_home: &Path,
    source: &Path,
    workspace: &Path,
    now_ms: i64,
) -> WorkerEnqueueOptions {
    WorkerEnqueueOptions {
        harness_home: harness_home.to_path_buf(),
        kind: WorkerJobKind::Watchdog,
        lane: Some("watchdog".to_string()),
        payload: json!({
            "sourceHome": source,
            "sourceWorkspace": workspace,
            "masterAgentId": "main",
            "masterSessionKey": "master-session"
        }),
        idempotency_key: Some("watchdog-runtime".to_string()),
        parent_job_id: None,
        job_group_id: Some("group-runtime".to_string()),
        master_agent_id: Some("main".to_string()),
        master_session_key: Some("master-session".to_string()),
        wake_policy: Some(json!({"mode":"all_completed"})),
        source: Some("test".to_string()),
        priority: 0,
        available_at_ms: Some(now_ms),
        max_attempts: 3,
        timeout_ms: Some(300_000),
        cascade_timeout_ms: None,
        rate_key: None,
        concurrency_group_key: Some("watchdog-runtime".to_string()),
        now_ms,
    }
}

fn worker_status(harness_home: &Path, job_id: &str) -> String {
    let conn = Connection::open(worker_db_file(harness_home)).unwrap();
    conn.query_row(
        "SELECT status FROM jobs WHERE job_id=?1",
        params![job_id],
        |row| row.get(0),
    )
    .unwrap()
}

fn worker_result(harness_home: &Path, job_id: &str) -> Value {
    let conn = Connection::open(worker_db_file(harness_home)).unwrap();
    let value: String = conn
        .query_row(
            "SELECT result_json FROM jobs WHERE job_id=?1",
            params![job_id],
            |row| row.get(0),
        )
        .unwrap();
    serde_json::from_str(&value).unwrap()
}

fn mailbox_result(harness_home: &Path, job_id: &str) -> Value {
    let conn = Connection::open(worker_db_file(harness_home)).unwrap();
    let (row_count, auto_resumable, state, envelope_json): (i64, i64, String, String) = conn
        .query_row(
            "SELECT COUNT(*), auto_resumable, state, envelope_json FROM worker_result_mailbox_v1 WHERE source_worker_job_id=?1",
            params![job_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    let envelope: Value = serde_json::from_str(&envelope_json).unwrap();
    json!({
        "rowCount": row_count,
        "autoResumable": auto_resumable == 1,
        "state": state,
        "outcome": envelope["outcome"],
        "redactedSummary": envelope["redactedSummary"],
    })
}

fn temp_root(test_name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("agent-harness-{test_name}-{nanos}"))
}
