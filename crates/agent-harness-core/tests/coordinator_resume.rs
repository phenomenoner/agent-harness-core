use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use agent_harness_core::lane::FullLaneKeyV1;
use agent_harness_core::worker_coordination::{
    WorkerCoordinatorWaitCreateOptionsV1, WorkerCoordinatorWaitStateV1,
    load_worker_coordinator_wait, persist_waiting_for_children_in_transaction,
};
use agent_harness_core::worker_result_mailbox::{
    ExactWorkerResultOwnerV1, WorkerResultEnvelopeV1, WorkerResultMailboxInsertV1,
    WorkerResultOutcomeV1, WorkerResultOwnerV1, insert_terminal_result_in_transaction,
};
use agent_harness_core::workers::{
    WorkerEnqueueOptions, WorkerEnqueueOptionsV2, WorkerEnqueueOptionsV3, WorkerJobKind,
    WorkerRunOnceOptions, enqueue_worker_job, enqueue_worker_job_v3, init_worker_store,
    run_worker_once, worker_db_file,
};
use agent_harness_core::{
    PromptAssemblyOptions, RuntimeExecutionReceiptStatus, RuntimeQueuePrepareOptions,
    prepare_runtime_queue_item,
};
use rusqlite::{Connection, params};
use serde_json::json;

#[test]
fn released_parent_schedules_one_durable_coordinator_resume_and_no_master_wakeup() {
    let root = temp_root("released-parent");
    let harness_home = root.join("harness");
    let source_home = root.join("source");
    let source_workspace = source_home.join("workspace");
    fs::create_dir_all(&source_workspace).unwrap();
    fs::write(source_workspace.join("AGENTS.md"), "# Coordinator\n").unwrap();
    init_worker_store(&harness_home).unwrap();
    fs::write(
        harness_home.join("harness-config.json"),
        r#"{"runtimeDispatch":{"classes":{"interactive":{"perSessionMaxActive":2,"sessionFifo":false,"sameSessionMainAgentSerialization":false}}}}"#,
    )
    .unwrap();

    let owner_a = exact_owner("child-queue-a");
    let owner_b = exact_owner("child-queue-b");
    let owner_c = exact_owner("child-queue-c");
    let child_a = enqueue_child(&harness_home, "child-a", owner_a.clone(), 1_000);
    let child_b = enqueue_child(&harness_home, "child-b", owner_b.clone(), 1_001);
    let child_c = enqueue_child(&harness_home, "child-c", owner_c, 1_002);

    let mut conn = Connection::open(worker_db_file(&harness_home)).unwrap();
    let transaction = conn.transaction().unwrap();
    for (job_id, event_key, owner, status, outcome, summary, at) in [
        (
            &child_a,
            "terminal/child-a",
            &owner_a,
            "succeeded",
            WorkerResultOutcomeV1::Succeeded,
            "Sol/max child finding: capability propagated.",
            1_010_i64,
        ),
        (
            &child_b,
            "terminal/child-b",
            &owner_b,
            "failed-terminal",
            WorkerResultOutcomeV1::Failed,
            "Terra/high child finding: bounded failure evidence.",
            1_011_i64,
        ),
    ] {
        transaction
            .execute(
                "UPDATE jobs SET status=?1, finished_at_ms=?2, updated_at_ms=?2 WHERE job_id=?3",
                params![status, at, job_id],
            )
            .unwrap();
        insert_terminal_result_in_transaction(
            &transaction,
            &WorkerResultMailboxInsertV1 {
                terminal_event_key: event_key.to_string(),
                owner: WorkerResultOwnerV1::Exact(owner.clone()),
                envelope: WorkerResultEnvelopeV1::new(outcome, summary, Vec::new()).unwrap(),
                source_worker_job_id: Some(job_id.to_string()),
                terminal_at_ms: at,
            },
        )
        .unwrap();
    }
    transaction
        .execute(
            "UPDATE jobs SET status='succeeded', finished_at_ms=1012, updated_at_ms=1012,
                    artifact_refs_json=?1, audit_path=?2 WHERE job_id=?3",
            params![
                r#"{"secret":"coordinator-raw-secret-must-not-leak","transcriptFile":"D:\\outside\\child.jsonl"}"#,
                r#"D:\outside\raw-child-audit.json"#,
                child_c
            ],
        )
        .unwrap();
    persist_waiting_for_children_in_transaction(
        &transaction,
        &WorkerCoordinatorWaitCreateOptionsV1 {
            wait_id: "wait-parent-queue".to_string(),
            owner: owner_a.clone(),
            child_group_id: "group-a".to_string(),
            expected_child_job_ids: vec![child_a.clone(), child_b.clone(), child_c.clone()],
            now_ms: 1_005,
        },
    )
    .unwrap();
    transaction.commit().unwrap();

    enqueue_worker_job(WorkerEnqueueOptions {
        harness_home: harness_home.clone(),
        kind: WorkerJobKind::Watchdog,
        lane: Some("watchdog".to_string()),
        payload: json!({
            "sourceHome": source_home,
            "sourceWorkspace": source_workspace,
            "jobGroupId": "group-a",
            "masterAgentId": "main",
            "masterSessionKey": "discord:channel:user:main",
            "coordinationMode": "durable-v1",
            "coordinatorWaitId": "wait-parent-queue"
        }),
        idempotency_key: Some("watchdog:group-a".to_string()),
        parent_job_id: None,
        job_group_id: Some("group-a".to_string()),
        master_agent_id: Some("main".to_string()),
        master_session_key: Some("discord:channel:user:main".to_string()),
        wake_policy: Some(json!({"mode":"all_completed"})),
        source: Some("test".to_string()),
        priority: 10,
        available_at_ms: Some(1_020),
        max_attempts: 3,
        timeout_ms: Some(300_000),
        cascade_timeout_ms: None,
        rate_key: None,
        concurrency_group_key: Some("master:group-a".to_string()),
        now_ms: 1_020,
    })
    .unwrap();

    let queue_dir = harness_home.join("state").join("runtime-queue");
    fs::create_dir_all(&queue_dir).unwrap();
    let active_lease_file = queue_dir.join("runtime-leases.json");
    fs::write(
        &active_lease_file,
        serde_json::to_vec_pretty(&json!({
            "schema": "agent-harness.runtime-queue-leases.v1",
            "leases": {
                "parent-queue": {
                    "queueId": "parent-queue",
                    "agentId": "main",
                    "runtimeClass": "interactive",
                    "origin": "channel",
                    "platform": "discord",
                    "accountId": "account",
                    "channelId": "channel",
                    "userId": "user",
                    "sessionKey": "discord:channel:user:main",
                    "owner": format!("pid:{}", std::process::id()),
                    "startedAtMs": 1_000,
                    "leaseExpiresAtMs": 90_000
                }
            }
        }))
        .unwrap(),
    )
    .unwrap();

    run_worker_once(WorkerRunOnceOptions {
        harness_home: harness_home.clone(),
        lane: Some("watchdog".to_string()),
        worker_id: "test-watchdog".to_string(),
        now_ms: 1_030,
        lease_ms: 60_000,
    })
    .unwrap();

    let conn = Connection::open(worker_db_file(&harness_home)).unwrap();
    let blocked_coordinator_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM jobs WHERE kind='coordinator_resume'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(blocked_coordinator_count, 0);
    assert_eq!(
        load_worker_coordinator_wait(&conn, "wait-parent-queue")
            .unwrap()
            .unwrap()
            .state,
        WorkerCoordinatorWaitStateV1::WaitingForChildren
    );
    drop(conn);
    fs::write(
        &active_lease_file,
        serde_json::to_vec_pretty(&json!({
            "schema": "agent-harness.runtime-queue-leases.v1",
            "leases": {
                "newer-same-lane-queue": {
                    "queueId": "newer-same-lane-queue",
                    "agentId": "main",
                    "runtimeClass": "interactive",
                    "origin": "channel",
                    "platform": "discord",
                    "accountId": "account",
                    "channelId": "channel",
                    "userId": "user",
                    "sessionKey": "discord:channel:user:main",
                    "virtualSessionId": "discord:channel:user:main:vsession-babfeafb4a118dbd",
                    "owner": format!("pid:{}", std::process::id()),
                    "startedAtMs": 2_000,
                    "leaseExpiresAtMs": 90_000
                }
            }
        }))
        .unwrap(),
    )
    .unwrap();

    run_worker_once(WorkerRunOnceOptions {
        harness_home: harness_home.clone(),
        lane: Some("watchdog".to_string()),
        worker_id: "test-watchdog-newer-lane-active".to_string(),
        now_ms: 10_000,
        lease_ms: 60_000,
    })
    .unwrap();

    let conn = Connection::open(worker_db_file(&harness_home)).unwrap();
    let blocked_by_newer_lane_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM jobs WHERE kind='coordinator_resume'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(blocked_by_newer_lane_count, 0);
    let watchdog_status: String = conn
        .query_row(
            "SELECT status FROM jobs WHERE kind='watchdog' AND job_group_id='group-a'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(watchdog_status, "pending");
    let claimed_mailbox_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM worker_result_mailbox_v1 WHERE state != 'unread'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(claimed_mailbox_count, 0);
    let active_lane_quarantine_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM worker_resume_intents_v1 WHERE state='quarantined' AND blocker_reason LIKE '%newer-same-lane-queue%'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(active_lane_quarantine_count, 1);
    drop(conn);
    fs::remove_file(&active_lease_file).unwrap();

    run_worker_once(WorkerRunOnceOptions {
        harness_home: harness_home.clone(),
        lane: Some("watchdog".to_string()),
        worker_id: "test-watchdog-retry".to_string(),
        now_ms: 100_000,
        lease_ms: 60_000,
    })
    .unwrap();

    let conn = Connection::open(worker_db_file(&harness_home)).unwrap();
    let coordinator_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM jobs WHERE kind='coordinator_resume'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let legacy_wakeup_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM jobs WHERE kind='master_wakeup'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(coordinator_count, 1);
    assert_eq!(legacy_wakeup_count, 0);
    let wait = load_worker_coordinator_wait(&conn, "wait-parent-queue")
        .unwrap()
        .unwrap();
    assert_eq!(wait.state, WorkerCoordinatorWaitStateV1::ResumeScheduled);
    let intent_id = wait.resume_intent_id.clone().unwrap();

    drop(conn);
    run_worker_once(WorkerRunOnceOptions {
        harness_home: harness_home.clone(),
        lane: Some("watchdog".to_string()),
        worker_id: "test-watchdog-restart-replay".to_string(),
        now_ms: 100_005,
        lease_ms: 60_000,
    })
    .unwrap();
    let conn = Connection::open(worker_db_file(&harness_home)).unwrap();
    let replay_coordinator_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM jobs WHERE kind='coordinator_resume'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(replay_coordinator_count, 1);

    run_worker_once(WorkerRunOnceOptions {
        harness_home: harness_home.clone(),
        lane: Some("llm".to_string()),
        worker_id: "test-coordinator-materializer".to_string(),
        now_ms: 100_010,
        lease_ms: 60_000,
    })
    .unwrap();
    let continuation_queue_id = format!("coordinator-resume:{intent_id}");
    let pending_file = harness_home
        .join("state")
        .join("runtime-queue")
        .join("pending.jsonl");
    let queued = fs::read_to_string(&pending_file).unwrap();
    let matching = queued
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .filter(|value| value["queueId"] == continuation_queue_id)
        .collect::<Vec<_>>();
    assert_eq!(matching.len(), 1);
    assert_eq!(matching[0]["origin"], "coordinator-resume");
    assert_eq!(matching[0]["accountId"], "account");
    assert_eq!(matching[0]["source"]["kind"], "coordinator-resume");
    assert!(
        matching[0]["inboundContext"]
            .as_str()
            .unwrap()
            .contains("failed")
    );
    let coordinator_context = matching[0]["inboundContext"].as_str().unwrap();
    assert!(coordinator_context.contains("Sol/max child finding: capability propagated."));
    assert!(coordinator_context.contains("Terra/high child finding: bounded failure evidence."));
    assert!(coordinator_context.contains(
        "child result unavailable: exact terminal mailbox evidence was missing or invalid"
    ));
    assert!(!coordinator_context.contains("coordinator-raw-secret-must-not-leak"));
    assert!(!coordinator_context.contains(r#"D:\outside\raw-child-audit.json"#));
    let coordinator_context_json: serde_json::Value =
        serde_json::from_str(coordinator_context).unwrap();
    let omitted = coordinator_context_json["results"]
        .as_array()
        .unwrap()
        .iter()
        .find(|result| result["sourceWorkerJobId"] == child_c)
        .unwrap();
    assert_eq!(omitted["envelope"]["outcome"], "failed");

    let interleaving_lease_file = queue_dir
        .join("classes")
        .join("interactive")
        .join("runtime-leases.json");
    fs::create_dir_all(interleaving_lease_file.parent().unwrap()).unwrap();
    fs::write(
        &interleaving_lease_file,
        serde_json::to_vec_pretty(&json!({
            "schema": "agent-harness.runtime-queue-leases.v1",
            "leases": {
                "interleaving-same-lane-queue": {
                    "queueId": "interleaving-same-lane-queue",
                    "agentId": "main",
                    "runtimeClass": "interactive",
                    "origin": "channel",
                    "platform": "discord",
                    "accountId": "account",
                    "channelId": "channel",
                    "userId": "user",
                    "sessionKey": "discord:channel:user:main",
                    "virtualSessionId": "discord:channel:user:main:vsession-babfeafb4a118dbd",
                    "owner": format!("pid:{}", std::process::id()),
                    "startedAtMs": 100_020,
                    "leaseExpiresAtMs": i64::MAX
                }
            }
        }))
        .unwrap(),
    )
    .unwrap();
    let interleaving_blocked = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
        harness_home: harness_home.clone(),
        queue_id: Some(continuation_queue_id.clone()),
        prompt_options: PromptAssemblyOptions::default(),
    })
    .unwrap();
    assert_eq!(
        interleaving_blocked.receipt.status,
        RuntimeExecutionReceiptStatus::NoPendingItem
    );
    assert!(
        interleaving_blocked
            .warnings
            .iter()
            .any(|warning| warning.contains("exact-lane coordinator mutual exclusion")),
        "receipt={} warnings={:?}",
        interleaving_blocked.receipt.reason,
        interleaving_blocked.warnings
    );
    fs::remove_file(&interleaving_lease_file).unwrap();

    let prepared = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
        harness_home: harness_home.clone(),
        queue_id: Some(continuation_queue_id),
        prompt_options: PromptAssemblyOptions::default(),
    })
    .unwrap();
    assert_eq!(
        prepared.receipt.status,
        RuntimeExecutionReceiptStatus::Prepared
    );
    let conn = Connection::open(worker_db_file(&harness_home)).unwrap();
    let wait = load_worker_coordinator_wait(&conn, "wait-parent-queue")
        .unwrap()
        .unwrap();
    assert_eq!(wait.state, WorkerCoordinatorWaitStateV1::Consumed);
    let acknowledged: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM worker_result_mailbox_v1 WHERE state='acknowledged'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(acknowledged, 3);

    let _ = fs::remove_dir_all(root);
}

fn enqueue_child(
    harness_home: &PathBuf,
    key: &str,
    owner: ExactWorkerResultOwnerV1,
    now_ms: i64,
) -> String {
    enqueue_worker_job_v3(WorkerEnqueueOptionsV3 {
        options: WorkerEnqueueOptionsV2 {
            options: WorkerEnqueueOptions {
                harness_home: harness_home.clone(),
                kind: WorkerJobKind::LlmSubagent,
                lane: Some("llm".to_string()),
                payload: json!({"agentId":"child","messageText":key}),
                idempotency_key: Some(key.to_string()),
                parent_job_id: None,
                job_group_id: Some("group-a".to_string()),
                master_agent_id: Some("main".to_string()),
                master_session_key: Some("discord:channel:user:main".to_string()),
                wake_policy: None,
                source: Some("test".to_string()),
                priority: 0,
                available_at_ms: Some(now_ms),
                max_attempts: 3,
                timeout_ms: Some(300_000),
                cascade_timeout_ms: None,
                rate_key: None,
                concurrency_group_key: Some("children:group-a".to_string()),
                now_ms,
            },
            child_policy: None,
        },
        result_owner: Some(WorkerResultOwnerV1::Exact(owner)),
    })
    .unwrap()
    .job
    .job_id
}

fn exact_owner(source_queue_id: &str) -> ExactWorkerResultOwnerV1 {
    ExactWorkerResultOwnerV1::new(
        FullLaneKeyV1::new(
            "discord",
            "account",
            "channel",
            "user",
            "main",
            "interactive",
            "discord:channel:user:main",
            "discord:channel:user:main",
        )
        .unwrap(),
        // The coordinator metadata validates the virtual-session derivation in
        // addition to every full-lane axis. Keep this fixture on the same
        // canonical V1 identity that a legacy-compatible runtime item carries.
        "discord:channel:user:main:vsession-babfeafb4a118dbd",
        None,
        Some("parent-queue".to_string()),
        source_queue_id,
        None,
        None,
    )
    .unwrap()
}

fn temp_root(label: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "agent-harness-coordinator-resume-{label}-{}-{nonce}",
        std::process::id()
    ))
}
