use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use agent_harness_core::channel_action::{ChannelApprovalDecisionV1, ChannelInboundActionV1};
use agent_harness_core::codex_runtime::{
    CodexRuntimePlanOptions, CodexRuntimeRunOptions, CodexRuntimeRunStatus, plan_codex_runtime,
    run_codex_runtime,
};
use agent_harness_core::context_rollover::root_working_session_key;
use agent_harness_core::external_effect::{
    ConnectorApprovalPolicyV1, ExternalEffectAdmissionV1, ExternalEffectApprovalDecisionV1,
    ExternalEffectRequestContextV1, ExternalEffectStateV1, McpElicitationDescriptorV1,
    begin_external_effect_request, external_effect_approval_authority_digest,
    external_effect_source_session_key_digest, load_external_effect_intent,
    resolve_external_effect_approval, resolve_external_effect_channel_action,
};
use agent_harness_core::goal_closure::{
    GoalClosureAuthorityV1, GoalClosureDispositionV1, GoalClosureIntentV1, GoalClosurePhaseInputV1,
    GoalClosurePhaseV1, GoalClosureResultV1, GoalClosureTriggerV1, goal_closure_intents_file,
    goal_closure_receipts_for_id, record_goal_closure_intent, record_goal_closure_phase,
};
use agent_harness_core::goal_lineage::{
    GoalLineageDoctorOptions, GoalLineageDoctorStatus, GoalProjectionObservationPhaseV1,
    GoalProjectionTurnRelationV1, latest_goal_projection_for_queue, run_goal_lineage_doctor,
};
use agent_harness_core::lane::FullLaneKeyV1;
use agent_harness_core::progress::{
    AgentProgressContext, AgentProgressDeliveryPlanOptions, AgentProgressDeliveryRecordOptions,
    AgentProgressDeliveryStatus, AgentProgressEvent, AgentProgressKind, AgentProgressStatus,
    append_agent_progress_event, plan_agent_progress_delivery, record_agent_progress_delivery,
};
use agent_harness_core::runtime_pipeline::{
    FinalOutboxDispositionV1, RuntimeRunOnceOptions, RuntimeRunOnceStatus,
    RuntimeSourceClosureKindV1, SourceFinalExpectationV1, run_runtime_queue_once,
};
use agent_harness_core::{
    AgentSource, ChannelReceiveOptions, ChannelReceiveStatus, ChannelStateLane,
    PromptAssemblyOptions, RuntimeExecutionReceiptStatus, RuntimeMutationEvidenceClass,
    RuntimeQueueEnqueueOptions, RuntimeQueueLeaseObservationOptions,
    RuntimeQueueLeaseObservationStatus, RuntimeQueuePrepareOptions, ScopedStopOptions,
    ScopedStopTarget, TurnPlanInput, build_channel_step, build_source_skill_index,
    build_turn_plan_for_account, enqueue_channel_step, load_agent_registry,
    observe_runtime_queue_lease, prepare_runtime_queue_item, receive_channel_message,
    record_scoped_stop, release_runtime_queue_lease,
};
use serde_json::{Value, json};

#[test]
fn discord_active_goal_observe_replay_parks_visibly_instead_of_silent_success() {
    // Contract: I2/I7/I9/I10/I17/I31/I36/I37.
    // Source: sanitized active-goal silent-stop incident replay.
    // Fails on: nonterminal Continue + observe mode becoming logical success with no outbox.
    // Asserts: exactly one visible parent park, no child, and no silent terminal success.
    let fixture: Value = serde_json::from_slice(
        &fs::read(fixture_path(
            "discord-active-goal-observe-silent-stop-replay.json",
        ))
        .unwrap(),
    )
    .unwrap();
    assert_eq!(fixture["source"]["platform"], "discord");
    assert_eq!(fixture["policy"]["configuredMode"], "observe");
    assert_eq!(fixture["incidentSequence"][1]["kind"], "subagent-completed");
    assert_eq!(fixture["incidentSequence"][2]["kind"], "subagent-completed");

    let root = temp_root("discord-active-goal-observe-replay");
    let source = write_source(&root);
    let harness_home = root.join("harness");
    let skills = build_source_skill_index(&source).unwrap();
    let receive = receive_channel_message(ChannelReceiveOptions {
        source,
        runtime_workspace: None,
        harness_home: harness_home.clone(),
        skill_index: skills,
        platform: fixture["source"]["platform"].as_str().unwrap().to_string(),
        account_id: fixture["source"]["accountId"]
            .as_str()
            .map(ToString::to_string),
        channel_id: fixture["source"]["channelId"].as_str().unwrap().to_string(),
        user_id: fixture["source"]["userId"].as_str().unwrap().to_string(),
        agent_id: Some(fixture["source"]["agentId"].as_str().unwrap().to_string()),
        session_key: None,
        message: fixture["source"]["message"].as_str().unwrap().to_string(),
        inbound_context: None,
        inbound_media_artifacts: Vec::new(),
        inbound_event_kind: None,
        inbound_event_id: None,
        skill_limit: 3,
        now_ms: 10_000,
    })
    .unwrap();
    assert_eq!(receive.status, ChannelReceiveStatus::AgentTurnQueued);
    let queue_id = receive.queue_id.expect("source queue id");
    let codex_home = root.join("codex-home");
    fs::create_dir_all(&codex_home).unwrap();
    fs::write(codex_home.join("auth.json"), "{}").unwrap();
    let _codex_home = EnvGuard::set("CODEX_HOME", codex_home.into_os_string());
    let report = run_runtime_queue_once(RuntimeRunOnceOptions {
        harness_home: harness_home.clone(),
        queue_id: Some(queue_id.clone()),
        codex_executable: Some(fake_active_goal_blocker_codex(&root)),
        timeout_ms: 30_000,
        idle_timeout_ms: 30_000,
        prompt_options: PromptAssemblyOptions {
            harness_home: Some(harness_home.clone()),
            ..PromptAssemblyOptions::default()
        },
    })
    .unwrap();

    let incident_events = fs::read_to_string(
        report
            .run
            .as_ref()
            .and_then(|run| run.stdout_log.as_ref())
            .expect("incident replay stdout log"),
    )
    .unwrap()
    .lines()
    .filter_map(|line| serde_json::from_str::<Value>(line).ok())
    .collect::<Vec<_>>();
    assert_eq!(
        incident_events
            .iter()
            .filter(|event| {
                event["method"] == "turn/completed" && event["params"]["turn"]["kind"] == "subAgent"
            })
            .count(),
        2,
        "the sanitized replay must execute both completed child turns"
    );
    assert_eq!(
        incident_events
            .iter()
            .filter(|event| event["params"]["item"]["type"] == "fileChange")
            .count(),
        1,
        "the sanitized replay must execute the parent file change"
    );
    let failed_commands = incident_events
        .iter()
        .filter(|event| {
            event["method"] == "item/completed"
                && event["params"]["item"]["type"] == "commandExecution"
                && event["params"]["item"]["status"] == "failed"
        })
        .collect::<Vec<_>>();
    assert_eq!(
        failed_commands.len(),
        4,
        "ordinary, ambiguous, and both process-start failures must be replayed"
    );
    assert_eq!(
        failed_commands
            .iter()
            .filter(|event| event["params"]["item"]["exitCode"] == -1)
            .count(),
        2,
        "both repeated process-start failures must be replayed"
    );
    let started_failures = failed_commands
        .iter()
        .filter(|event| event["params"]["item"]["exitCode"] == 1)
        .collect::<Vec<_>>();
    assert_eq!(started_failures.len(), 2);
    assert_eq!(
        started_failures
            .iter()
            .filter(|event| event["params"]["item"]["aggregatedOutput"] == "")
            .count(),
        1,
        "one process-started failure must preserve ambiguous empty output"
    );
    assert_eq!(
        started_failures
            .iter()
            .filter(|event| event["params"]["item"]["aggregatedOutput"] != "")
            .count(),
        1,
        "one process-started failure must preserve an ordinary CLI error"
    );
    assert_eq!(
        fixture["incidentSequence"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|event| event["kind"] == "command-failed")
            .count(),
        failed_commands.len(),
        "the executable replay must cover every fixture command failure"
    );

    assert_eq!(
        report.receipt.status,
        RuntimeRunOnceStatus::NeedsUser,
        "observe-mode nonterminal source work must park instead of terminalizing as success: {:#?}",
        report.receipt
    );
    assert_eq!(
        report.receipt.terminal_disposition,
        Some(agent_harness_core::RuntimeTerminalDispositionV1::NeedsUser)
    );
    assert_eq!(
        report.receipt.source_final_expectation,
        Some(SourceFinalExpectationV1::Required)
    );
    assert!(report.receipt.continuation_link.is_none());
    assert!(report.receipt.child_queue_id.is_none());
    let source_outbox = load_outbox(&harness_home)
        .into_iter()
        .filter(|row| row["sourceQueueId"] == queue_id)
        .collect::<Vec<_>>();
    assert_eq!(
        source_outbox.len(),
        1,
        "park must have exactly one source outbox row"
    );
    assert!(
        source_outbox[0]["text"].as_str().is_some_and(
            |text| text.contains("observation-only") && text.contains("remains active")
        ),
        "parked notice must state the bounded policy reason: {source_outbox:#?}"
    );
    let replay = run_runtime_queue_once(RuntimeRunOnceOptions {
        harness_home: harness_home.clone(),
        queue_id: Some(queue_id.clone()),
        codex_executable: Some(fake_active_goal_blocker_codex(&root)),
        timeout_ms: 30_000,
        idle_timeout_ms: 30_000,
        prompt_options: PromptAssemblyOptions {
            harness_home: Some(harness_home.clone()),
            ..PromptAssemblyOptions::default()
        },
    })
    .unwrap();
    assert!(matches!(
        replay.receipt.status,
        RuntimeRunOnceStatus::NoWork | RuntimeRunOnceStatus::Suppressed
    ));
    assert_eq!(
        load_outbox(&harness_home)
            .iter()
            .filter(|row| row["sourceQueueId"] == queue_id)
            .count(),
        1,
        "restart must not duplicate the parked source outbox"
    );
    let released = observe_runtime_queue_lease(RuntimeQueueLeaseObservationOptions {
        harness_home: harness_home.clone(),
        queue_id: queue_id.clone(),
        observed_at_ms: 10_001,
    });
    assert_eq!(
        released.status,
        RuntimeQueueLeaseObservationStatus::Released,
        "parked source work must not retain a runtime lease"
    );
    let later_source = write_source(&root);
    let later_registry = load_agent_registry(&later_source).unwrap();
    let later_skills = build_source_skill_index(&later_source).unwrap();
    let source_pending = pending_queue(&harness_home, &queue_id);
    let later_turn = build_turn_plan_for_account(
        &later_source,
        &later_registry,
        &later_skills,
        TurnPlanInput {
            harness_home: None,
            platform: "discord".to_string(),
            channel_id: "channel-sanitized".to_string(),
            user_id: "user-sanitized".to_string(),
            text: "Later same-session FIFO work must remain claimable.".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            requested_agent_id: Some("main".to_string()),
            session_hint: source_pending["sessionKey"]
                .as_str()
                .map(ToString::to_string),
            skill_limit: 3,
        },
        Some("account-sanitized".to_string()),
    )
    .unwrap();
    let later = enqueue_channel_step(
        &build_channel_step(&later_registry, &later_turn),
        RuntimeQueueEnqueueOptions {
            harness_home: harness_home.clone(),
            runtime_workspace: None,
            inbound_canonical_id: None,
            now_ms: 10_001,
        },
    )
    .unwrap();
    let later_queue_id = later.receipt.queue_id.expect("later queue id");
    let later_prepare = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
        harness_home: harness_home.clone(),
        queue_id: None,
        prompt_options: PromptAssemblyOptions {
            harness_home: Some(harness_home.clone()),
            ..PromptAssemblyOptions::default()
        },
    })
    .unwrap();
    assert_eq!(
        later_prepare.receipt.status,
        RuntimeExecutionReceiptStatus::Prepared,
        "parked goal work must release its lease and leave later FIFO work claimable"
    );
    assert_eq!(
        later_prepare.receipt.queue_id.as_deref(),
        Some(later_queue_id.as_str()),
        "FIFO claim must select the later same-session item"
    );
    release_runtime_queue_lease(&harness_home, &later_queue_id).unwrap();
    let _ = fs::remove_dir_all(root);
}

#[test]
fn discord_active_goal_authority_conflict_parks_visibly_instead_of_silent_success() {
    // Contract: I2/I7/I9/I10/I17/I31/I36/I37.
    // Source: sanitized live-shaped replay of two unresolved active projections in one exact lane.
    // Fails on: ProgressOnly + active + authority conflict bypassing source-closure resolution.
    // Asserts: no child, exactly one visible parent park, and restart-safe source finalization.
    let root = temp_root("discord-active-goal-authority-conflict");
    let harness_home = root.join("harness");
    let codex_home = root.join("codex-home");
    fs::create_dir_all(&codex_home).unwrap();
    fs::write(codex_home.join("auth.json"), "{}").unwrap();
    let _codex_home = EnvGuard::set("CODEX_HOME", codex_home.into_os_string());

    let seed_source = write_source(&root);
    let seed_skills = build_source_skill_index(&seed_source).unwrap();
    let seed_receive = receive_channel_message(ChannelReceiveOptions {
        source: seed_source,
        runtime_workspace: None,
        harness_home: harness_home.clone(),
        skill_index: seed_skills,
        platform: "discord".to_string(),
        account_id: Some("account-sanitized".to_string()),
        channel_id: "channel-sanitized".to_string(),
        user_id: "user-sanitized".to_string(),
        agent_id: Some("main".to_string()),
        session_key: None,
        message: "Continue the durable implementation goal.".to_string(),
        inbound_context: None,
        inbound_media_artifacts: Vec::new(),
        inbound_event_kind: None,
        inbound_event_id: None,
        skill_limit: 3,
        now_ms: 15_000,
    })
    .unwrap();
    let seed_queue_id = seed_receive.queue_id.expect("seed queue id");
    let seed_report = run_runtime_queue_once(RuntimeRunOnceOptions {
        harness_home: harness_home.clone(),
        queue_id: Some(seed_queue_id.clone()),
        codex_executable: Some(fake_active_goal_conflict_codex(&root, "seed")),
        timeout_ms: 30_000,
        idle_timeout_ms: 30_000,
        prompt_options: PromptAssemblyOptions {
            harness_home: Some(harness_home.clone()),
            ..PromptAssemblyOptions::default()
        },
    })
    .unwrap();
    assert_eq!(seed_report.receipt.status, RuntimeRunOnceStatus::NeedsUser);
    remove_codex_bindings(&harness_home);

    let current_source = write_source(&root);
    let current_skills = build_source_skill_index(&current_source).unwrap();
    let current_receive = receive_channel_message(ChannelReceiveOptions {
        source: current_source,
        runtime_workspace: None,
        harness_home: harness_home.clone(),
        skill_index: current_skills,
        platform: "discord".to_string(),
        account_id: Some("account-sanitized".to_string()),
        channel_id: "channel-sanitized".to_string(),
        user_id: "user-sanitized".to_string(),
        agent_id: Some("main".to_string()),
        session_key: None,
        message: "Continue the durable implementation goal.".to_string(),
        inbound_context: None,
        inbound_media_artifacts: Vec::new(),
        inbound_event_kind: None,
        inbound_event_id: None,
        skill_limit: 3,
        now_ms: 15_001,
    })
    .unwrap();
    let current_queue_id = current_receive.queue_id.expect("current queue id");
    assert_ne!(seed_queue_id, current_queue_id);
    assert_eq!(
        pending_queue(&harness_home, &seed_queue_id)["sessionKey"],
        pending_queue(&harness_home, &current_queue_id)["sessionKey"],
        "the replay must retain one exact source session"
    );

    let report = run_runtime_queue_once(RuntimeRunOnceOptions {
        harness_home: harness_home.clone(),
        queue_id: Some(current_queue_id.clone()),
        codex_executable: Some(fake_active_goal_conflict_codex(&root, "current")),
        timeout_ms: 30_000,
        idle_timeout_ms: 30_000,
        prompt_options: PromptAssemblyOptions {
            harness_home: Some(harness_home.clone()),
            ..PromptAssemblyOptions::default()
        },
    })
    .unwrap();
    let doctor = run_goal_lineage_doctor(GoalLineageDoctorOptions {
        harness_home: harness_home.clone(),
        lane_digest: None,
        virtual_session_id: None,
    })
    .unwrap();
    assert_eq!(
        doctor.status,
        GoalLineageDoctorStatus::ReconciliationRequired,
        "two unresolved exact-lane active projections must reproduce authority conflict"
    );
    assert_eq!(
        report.receipt.status,
        RuntimeRunOnceStatus::NeedsUser,
        "authority-conflicted active source work must park instead of terminalizing silently: {:#?}",
        report.receipt
    );
    assert_eq!(
        report.receipt.terminal_disposition,
        Some(agent_harness_core::RuntimeTerminalDispositionV1::NeedsUser)
    );
    assert_eq!(
        report.receipt.source_final_expectation,
        Some(SourceFinalExpectationV1::Required)
    );
    assert!(matches!(
        report.receipt.source_closure_kind,
        Some(RuntimeSourceClosureKindV1::ParkedObserve)
            | Some(RuntimeSourceClosureKindV1::ParkedPolicyDenied)
    ));
    assert_eq!(
        report.receipt.source_closure_reason.as_deref(),
        Some("goal-transition-needs-authority")
    );
    assert!(report.receipt.child_queue_id.is_none());
    assert!(report.receipt.continuation_link.is_none());
    let source_outbox = load_outbox(&harness_home)
        .into_iter()
        .filter(|row| row["sourceQueueId"] == current_queue_id)
        .collect::<Vec<_>>();
    assert_eq!(source_outbox.len(), 1);
    assert!(
        source_outbox[0]["text"]
            .as_str()
            .is_some_and(|text| text.contains("authority requires reconciliation"))
    );

    let replay = run_runtime_queue_once(RuntimeRunOnceOptions {
        harness_home: harness_home.clone(),
        queue_id: Some(current_queue_id.clone()),
        codex_executable: Some(fake_active_goal_conflict_codex(&root, "current")),
        timeout_ms: 30_000,
        idle_timeout_ms: 30_000,
        prompt_options: PromptAssemblyOptions {
            harness_home: Some(harness_home.clone()),
            ..PromptAssemblyOptions::default()
        },
    })
    .unwrap();
    assert!(matches!(
        replay.receipt.status,
        RuntimeRunOnceStatus::NoWork | RuntimeRunOnceStatus::Suppressed
    ));
    assert_eq!(
        load_outbox(&harness_home)
            .iter()
            .filter(|row| row["sourceQueueId"] == current_queue_id)
            .count(),
        1,
        "restart must not duplicate the authority-conflict parked source outbox"
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn active_goal_policy_matrix_preserves_exact_closure_on_discord_and_telegram() {
    let cases = [
        ("telegram", "observe", false),
        ("telegram", "disabled", false),
        ("discord", "active-wrong-lane", false),
        ("discord", "active", true),
    ];
    for (platform, policy, admits_child) in cases {
        let root = temp_root(&format!("active-goal-policy-{platform}-{policy}"));
        let source = write_source(&root);
        let harness_home = root.join("harness");
        let skills = build_source_skill_index(&source).unwrap();
        let receive = receive_channel_message(ChannelReceiveOptions {
            source,
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: platform.to_string(),
            account_id: Some("account-sanitized".to_string()),
            channel_id: "channel-sanitized".to_string(),
            user_id: "user-sanitized".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "Continue the durable implementation goal.".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: None,
            inbound_event_id: None,
            skill_limit: 3,
            now_ms: 20_000,
        })
        .unwrap();
        let queue_id = receive.queue_id.expect("source queue id");
        if policy != "observe" {
            let mode = if policy == "disabled" {
                "disabled"
            } else {
                "active"
            };
            let lane_digest = if admits_child {
                exact_full_lane_digest(&harness_home, &queue_id)
            } else {
                "0".repeat(64)
            };
            fs::write(
                harness_home.join("harness-config.json"),
                serde_json::to_vec_pretty(&serde_json::json!({
                    "goalAutonomy": {
                        "mode": mode,
                        "activeLaneDigests": [lane_digest]
                    }
                }))
                .unwrap(),
            )
            .unwrap();
        }
        let codex_home = root.join("codex-home");
        fs::create_dir_all(&codex_home).unwrap();
        fs::write(codex_home.join("auth.json"), "{}").unwrap();
        let _codex_home = EnvGuard::set("CODEX_HOME", codex_home.into_os_string());
        let report = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: Some(queue_id.clone()),
            codex_executable: Some(fake_active_goal_blocker_codex(&root)),
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();
        if admits_child {
            assert_eq!(
                report.receipt.status,
                RuntimeRunOnceStatus::Skipped,
                "{platform}/{policy}"
            );
            assert_eq!(
                report.receipt.source_closure_kind,
                Some(RuntimeSourceClosureKindV1::CommittedHandoff)
            );
            assert!(report.receipt.child_queue_id.is_some());
            assert!(report.outbound_message.is_none());
            assert_eq!(load_outbox(&harness_home).len(), 0);
        } else {
            assert_eq!(
                report.receipt.status,
                RuntimeRunOnceStatus::NeedsUser,
                "{platform}/{policy}"
            );
            assert!(matches!(
                report.receipt.source_closure_kind,
                Some(RuntimeSourceClosureKindV1::ParkedObserve)
                    | Some(RuntimeSourceClosureKindV1::ParkedPolicyDenied)
            ));
            assert!(report.receipt.child_queue_id.is_none());
            assert_eq!(
                load_outbox(&harness_home)
                    .iter()
                    .filter(|row| row["sourceQueueId"] == queue_id)
                    .count(),
                1,
                "{platform}/{policy}"
            );
        }
        let _ = fs::remove_dir_all(root);
    }
}

#[test]
fn active_goal_shell_drift_replay_recovers_once_then_parks_visibly() {
    for (case, prior_depth, mode, shell_drift, fence, expected) in [
        (
            "first-recovery",
            None,
            "active",
            true,
            "none",
            "shell-child",
        ),
        (
            "repeat-drift",
            Some(1),
            "active",
            true,
            "none",
            "budget-park",
        ),
        (
            "missing-artifact-near-miss",
            None,
            "active",
            false,
            "none",
            "ordinary-child",
        ),
        (
            "observe-drift",
            None,
            "observe",
            true,
            "none",
            "policy-park",
        ),
        (
            "mutation-fenced",
            None,
            "active",
            true,
            "mutation",
            "effect-fence-park",
        ),
        (
            "external-effect-fenced",
            None,
            "active",
            true,
            "external-effect",
            "effect-fence-park",
        ),
    ] {
        let root = temp_root(&format!("active-goal-shell-drift-{case}"));
        let source = write_source(&root);
        let harness_home = root.join("harness");
        let skills = build_source_skill_index(&source).unwrap();
        let receive = receive_channel_message(ChannelReceiveOptions {
            source,
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "discord".to_string(),
            account_id: Some("account-sanitized".to_string()),
            channel_id: "channel-sanitized".to_string(),
            user_id: "user-sanitized".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "Continue the durable implementation goal.".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: None,
            inbound_event_id: None,
            skill_limit: 3,
            now_ms: 30_000,
        })
        .unwrap();
        let queue_id = receive.queue_id.expect("source queue id");
        let lane_digest = exact_full_lane_digest(&harness_home, &queue_id);
        fs::write(
            harness_home.join("harness-config.json"),
            serde_json::to_vec_pretty(&json!({
                "goalAutonomy": {
                    "mode": mode,
                    "activeLaneDigests": [lane_digest]
                }
            }))
            .unwrap(),
        )
        .unwrap();
        if let Some(depth) = prior_depth {
            set_pending_shell_recovery_depth(&harness_home, &queue_id, depth);
        }
        let external_effect_id =
            (fence == "external-effect").then(|| seed_external_effect(&harness_home, &queue_id));
        let external_effect_transitions =
            nonempty_lines(&harness_home.join("state/external-effects/transitions.jsonl"));
        let codex_home = root.join("codex-home");
        fs::create_dir_all(&codex_home).unwrap();
        fs::write(codex_home.join("auth.json"), "{}").unwrap();
        let refreshed_shell = root.join("stable-pwsh.exe");
        fs::write(&refreshed_shell, "test shell boundary").unwrap();
        let _codex_home = EnvGuard::set("CODEX_HOME", codex_home.into_os_string());
        let _shell_boundary = EnvGuard::set(
            "AGENT_HARNESS_POWERSHELL_EXECUTABLE",
            refreshed_shell.into_os_string(),
        );
        let protocol_failure = match fence {
            "mutation" => Some("Synthetic failure after a mutation-capable command".to_string()),
            "external-effect" => Some(format!(
                "Synthetic failure after external effect effectId={}",
                external_effect_id.as_deref().unwrap()
            )),
            _ => None,
        };
        let report = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: Some(queue_id.clone()),
            codex_executable: Some(fake_active_goal_shell_case_codex(
                &root,
                shell_drift,
                protocol_failure.as_deref(),
            )),
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();
        let runtime_receipt = codex_runtime_receipt_for_queue(&harness_home, &queue_id);
        let lifecycle = &runtime_receipt["subagentLifecycle"];
        assert_eq!(lifecycle["childStartedCount"], 2, "{case}");
        assert_eq!(lifecycle["childCompletedCount"], 2, "{case}");
        assert_eq!(lifecycle["childErroredCount"], 0, "{case}");
        assert_eq!(lifecycle["parentWaitCompletedCount"], 1, "{case}");
        assert_eq!(
            lifecycle["parentProcessStartFailure"], shell_drift,
            "{case}"
        );
        assert_eq!(lifecycle["childProcessStartFailure"], false, "{case}");
        assert_eq!(lifecycle["childFinalObservedCount"], 2, "{case}");
        assert_eq!(lifecycle["finalOwner"], "parent", "{case}");
        assert_eq!(lifecycle["children"].as_array().unwrap().len(), 2, "{case}");
        assert!(
            lifecycle["children"]
                .as_array()
                .unwrap()
                .iter()
                .all(|child| child["commandCount"] == 1 && child["fileChangeCount"] == 0),
            "{case}"
        );
        let bounded = serde_json::to_string(lifecycle).unwrap();
        for forbidden in [
            "turn-child-a",
            "turn-child-b",
            "routing-reader",
            "lifecycle-reader",
            "Inspect the routing contract",
            "Inspect the lifecycle contract",
            "Get-Content docs/invariants.md",
            "Get-Content docs/agent-harness-topology-contract.md",
            stale_shell_path(),
        ] {
            assert!(!bounded.contains(forbidden), "{case} leaked {forbidden}");
        }
        if fence == "mutation" {
            assert_eq!(
                report.receipt.mutation_evidence,
                Some(RuntimeMutationEvidenceClass::MutationObserved),
                "mutation-capable protocol failure must fence shell recovery"
            );
            assert!(report.receipt.external_effect.is_none());
        } else if fence == "external-effect" {
            assert_eq!(
                report
                    .receipt
                    .external_effect
                    .as_ref()
                    .map(|effect| effect.effect_id.as_str()),
                external_effect_id.as_deref(),
                "the referenced durable effect must be carried into the run-once fence"
            );
        }

        if expected == "shell-child" {
            assert_eq!(report.receipt.status, RuntimeRunOnceStatus::Skipped);
            assert_eq!(
                report.receipt.source_closure_reason.as_deref(),
                Some("shell-runtime-drift-continuation-committed")
            );
            let child = report
                .receipt
                .child_queue_id
                .as_deref()
                .expect("one fresh-runtime child");
            assert_eq!(pending_queue(&harness_home, child)["shellRecoveryDepth"], 1);
            assert!(load_outbox(&harness_home).is_empty());
        } else if expected == "ordinary-child" {
            assert_eq!(report.receipt.status, RuntimeRunOnceStatus::Skipped);
            assert_eq!(
                report.receipt.source_closure_reason.as_deref(),
                Some("goal-continuation-committed")
            );
            let child = report
                .receipt
                .child_queue_id
                .as_deref()
                .expect("ordinary active-goal child");
            assert!(pending_queue(&harness_home, child)["shellRecoveryDepth"].is_null());
            assert!(load_outbox(&harness_home).is_empty());
        } else {
            assert_eq!(report.receipt.status, RuntimeRunOnceStatus::NeedsUser);
            let expected_reason = match expected {
                "budget-park" => "shell-recovery-budget-exhausted",
                "effect-fence-park" => "shell-recovery-external-effect-fenced",
                _ => "goal-autonomy-observe",
            };
            assert_eq!(
                report.receipt.source_closure_reason.as_deref(),
                Some(expected_reason)
            );
            assert!(report.receipt.child_queue_id.is_none());
            let source_outbox = load_outbox(&harness_home)
                .into_iter()
                .filter(|row| row["sourceQueueId"] == queue_id)
                .collect::<Vec<_>>();
            assert_eq!(source_outbox.len(), 1);
            let expected_notice = match expected {
                "budget-park" => "one automatic fresh-runtime shell recovery",
                "effect-fence-park" => "external-effect or mutation evidence",
                _ => "observation-only",
            };
            assert!(
                source_outbox[0]["text"]
                    .as_str()
                    .is_some_and(|text| text.contains(expected_notice))
            );
            let replay = run_runtime_queue_once(RuntimeRunOnceOptions {
                harness_home: harness_home.clone(),
                queue_id: Some(queue_id.clone()),
                codex_executable: Some(fake_active_goal_shell_case_codex(
                    &root,
                    shell_drift,
                    protocol_failure.as_deref(),
                )),
                timeout_ms: 30_000,
                idle_timeout_ms: 30_000,
                prompt_options: PromptAssemblyOptions {
                    harness_home: Some(harness_home.clone()),
                    ..PromptAssemblyOptions::default()
                },
            })
            .unwrap();
            assert!(matches!(
                replay.receipt.status,
                RuntimeRunOnceStatus::NoWork | RuntimeRunOnceStatus::Suppressed
            ));
            assert_eq!(
                load_outbox(&harness_home)
                    .iter()
                    .filter(|row| row["sourceQueueId"] == queue_id)
                    .count(),
                1,
                "restart must not duplicate the recovery-exhausted park"
            );
            assert_eq!(
                nonempty_lines(&harness_home.join("state/external-effects/transitions.jsonl")),
                external_effect_transitions,
                "restart must not duplicate an external-effect transition"
            );
            if let Some(effect_id) = external_effect_id.as_deref() {
                assert_eq!(
                    load_external_effect_intent(&harness_home, effect_id)
                        .unwrap()
                        .expect("seeded external effect remains durable")
                        .state,
                    ExternalEffectStateV1::ApprovalRequired
                );
            }
        }
        let _ = fs::remove_dir_all(root);
    }
}

#[test]
fn child_final_during_stop_is_internal_and_cannot_reopen_closed_goal() {
    let root = temp_root("child-final-during-stop");
    let source = write_source(&root);
    let harness_home = root.join("harness");
    write_owned_event_config(&harness_home);
    let parent = receive(&source, &harness_home, "parent work", 1_000);
    let queue_id = parent.queue_id.clone().expect("parent queue");
    let session_key = parent.session_key.clone();
    let lane_digest = lane_digest();
    let closure = record_completed_closure(&harness_home, &lane_digest, &session_key);
    let closed_receipts = goal_closure_receipts_for_id(&harness_home, &closure.closure_id).unwrap();
    assert_eq!(closed_receipts.len(), 5);
    assert_eq!(
        closed_receipts.last().unwrap().phase,
        GoalClosurePhaseV1::Completed
    );

    let prepared = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
        harness_home: harness_home.clone(),
        queue_id: Some(queue_id.clone()),
        prompt_options: PromptAssemblyOptions {
            harness_home: Some(harness_home.clone()),
            ..PromptAssemblyOptions::default()
        },
    })
    .unwrap();
    assert_eq!(
        prepared.receipt.status,
        RuntimeExecutionReceiptStatus::Prepared
    );
    let plan = plan_codex_runtime(CodexRuntimePlanOptions {
        harness_home: harness_home.clone(),
        execution_dir: prepared.receipt.execution_dir.clone(),
        codex_executable: Some(fake_child_only_codex(&root)),
    })
    .unwrap();

    let stop = receive(&source, &harness_home, "/stop", 1_100);
    assert_eq!(stop.status, ChannelReceiveStatus::CommandApplied);
    let codex_home = root.join("codex-home");
    fs::create_dir_all(&codex_home).unwrap();
    fs::write(codex_home.join("auth.json"), "{}").unwrap();
    let _codex_home = EnvGuard::set("CODEX_HOME", codex_home.into_os_string());

    // This direct runtime call models the already-spawned process crossing the
    // stop/quiescence boundary. The runtime ownership classifier still sees
    // the late child events even though normal queue dispatch is fenced.
    let child_arrival = run_codex_runtime(CodexRuntimeRunOptions {
        harness_home: harness_home.clone(),
        execution_dir: plan.execution_dir.clone(),
        plan_file: plan.plan_file.clone(),
        timeout_ms: 30_000,
        idle_timeout_ms: 30_000,
        progress_context: None,
    })
    .unwrap();
    assert_eq!(
        child_arrival.receipt.status,
        CodexRuntimeRunStatus::ProtocolError
    );
    assert!(child_arrival.completion.is_none());
    assert!(child_arrival.receipt.completion_file.is_none());
    assert!(
        child_arrival
            .warnings
            .iter()
            .any(|warning| warning.contains("quarantined foreign Codex event")),
        "late child events should leave bounded quarantine evidence: {:#?}",
        child_arrival.warnings
    );
    let stdout = fs::read_to_string(
        plan.execution_dir
            .as_ref()
            .unwrap()
            .join("codex-runtime-run.stdout.jsonl"),
    )
    .unwrap();
    assert!(stdout.contains("LATE CHILD FINAL MUST STAY INTERNAL"));

    // Reconstructing the queue owner after the simulated process restart must
    // honor the durable stop marker and create no provider final.
    release_runtime_queue_lease(&harness_home, &queue_id).unwrap();
    let restarted = run_runtime_queue_once(RuntimeRunOnceOptions {
        harness_home: harness_home.clone(),
        queue_id: Some(queue_id.clone()),
        codex_executable: Some(fake_parent_final_codex(&root)),
        timeout_ms: 30_000,
        idle_timeout_ms: 30_000,
        prompt_options: PromptAssemblyOptions {
            harness_home: Some(harness_home.clone()),
            ..PromptAssemblyOptions::default()
        },
    })
    .unwrap();
    assert_eq!(restarted.receipt.status, RuntimeRunOnceStatus::Suppressed);
    assert_eq!(
        restarted.receipt.source_final_expectation,
        Some(SourceFinalExpectationV1::NotApplicable)
    );
    assert_eq!(
        restarted.receipt.final_outbox_disposition,
        Some(FinalOutboxDispositionV1::NotApplicable)
    );
    assert!(restarted.outbound_message.is_none());
    let outbox = load_outbox(&harness_home);
    assert_eq!(
        outbox.len(),
        1,
        "only the governed stop reply may be visible"
    );
    assert_eq!(outbox[0]["kind"], "command-reply");
    assert!(!outbox.iter().any(|row| {
        row["text"] == "LATE CHILD FINAL MUST STAY INTERNAL" || row["sourceQueueId"] == queue_id
    }));

    let after = goal_closure_receipts_for_id(&harness_home, &closure.closure_id).unwrap();
    assert_eq!(
        after, closed_receipts,
        "foreign completion reopened closed goal evidence"
    );
    assert_eq!(
        nonempty_lines(&goal_closure_intents_file(&harness_home)),
        1,
        "late child completion must not manufacture another closure"
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn current_queue_runs_while_old_session_historical_closure_is_pending() {
    let root = temp_root("fresh-queue-with-historical-closure");
    let source = write_source(&root);
    let harness_home = root.join("harness");
    let queue_b = receive(&source, &harness_home, "fresh queue B", 2_000);
    let queue_b_id = queue_b.queue_id.clone().expect("fresh queue id");
    let queue_binding_before = pending_queue(&harness_home, &queue_b_id);

    let historical = GoalClosureIntentV1::new(
        GoalClosureTriggerV1::OperatorHistorical,
        GoalClosureDispositionV1::Completed,
        GoalClosureAuthorityV1 {
            lane_digest: lane_digest(),
            concrete_session_key: "telegram:dm-42:user-7:main:historical".to_string(),
            virtual_session_id: "historical-virtual-session".to_string(),
            backend_context_generation: "historical-generation".to_string(),
            source_thread_id: "historical-thread".to_string(),
        },
        "historical-goal",
        "historical-generation",
        Some("historical-projection".to_string()),
        "operator-history-close",
        "historical completion remains pending",
    )
    .unwrap();
    record_goal_closure_intent(&harness_home, &historical).unwrap();
    record_goal_closure_phase(
        &harness_home,
        &historical,
        GoalClosurePhaseV1::IntentRecorded,
        closure_phase(GoalClosureResultV1::Pending, 2_010),
    )
    .unwrap();

    let codex_home = root.join("codex-home");
    fs::create_dir_all(&codex_home).unwrap();
    fs::write(codex_home.join("auth.json"), "{}").unwrap();
    let _codex_home = EnvGuard::set("CODEX_HOME", codex_home.into_os_string());
    let restarted = run_runtime_queue_once(RuntimeRunOnceOptions {
        harness_home: harness_home.clone(),
        queue_id: Some(queue_b_id.clone()),
        codex_executable: Some(fake_parent_final_codex(&root)),
        timeout_ms: 30_000,
        idle_timeout_ms: 30_000,
        prompt_options: PromptAssemblyOptions {
            harness_home: Some(harness_home.clone()),
            ..PromptAssemblyOptions::default()
        },
    })
    .unwrap();
    assert_eq!(restarted.receipt.status, RuntimeRunOnceStatus::Completed);
    assert_eq!(
        restarted
            .outbound_message
            .as_ref()
            .and_then(|message| message.source_queue_id.as_deref()),
        Some(queue_b_id.as_str())
    );
    assert_eq!(
        restarted
            .outbound_message
            .as_ref()
            .map(|message| message.text.as_str()),
        Some("Fresh queue B completed.")
    );
    let queue_binding_after = pending_queue(&harness_home, &queue_b_id);
    for field in [
        "sessionKey",
        "platform",
        "accountId",
        "channelId",
        "userId",
        "agentId",
    ] {
        assert_eq!(
            queue_binding_after.get(field),
            queue_binding_before.get(field),
            "historical closure changed fresh queue binding field {field}"
        );
    }
    let pending = goal_closure_receipts_for_id(&harness_home, &historical.closure_id).unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].result, GoalClosureResultV1::Pending);
    assert_eq!(
        load_outbox(&harness_home)
            .iter()
            .filter(|row| row["sourceQueueId"] == queue_b_id)
            .count(),
        1
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn committed_new_boundary_resolves_required_final_hold_with_typed_non_delivery() {
    let root = temp_root("new-boundary-required-final");
    let source = write_source(&root);
    let harness_home = root.join("harness");
    let old = receive(&source, &harness_home, "old session queue", 3_000);
    let queue_id = old.queue_id.clone().expect("old queue");
    let context = progress_context(&queue_id, &old.session_key);
    append_agent_progress_event(
        &harness_home,
        &AgentProgressEvent::new(
            &context,
            AgentProgressKind::Runtime,
            "run",
            "completed",
            AgentProgressStatus::Completed,
            3_100,
        ),
    )
    .unwrap();
    write_source_final_receipt(&harness_home, &context, "running", "required", "appended");

    let held = progress_plan(&harness_home, 3_200);
    assert!(
        held.pending.is_empty(),
        "required final must hold terminal progress"
    );
    let codex_home = root.join("codex-home");
    fs::create_dir_all(&codex_home).unwrap();
    fs::write(codex_home.join("auth.json"), "{}").unwrap();
    let _codex_home = EnvGuard::set("CODEX_HOME", codex_home.into_os_string());
    let delayed_codex = fake_delayed_parent_final_codex(&root);
    let run_home = harness_home.clone();
    let run_queue = queue_id.clone();
    let runtime = std::thread::spawn(move || {
        run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: run_home.clone(),
            queue_id: Some(run_queue),
            codex_executable: Some(delayed_codex),
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(run_home),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap()
    });
    wait_for_file(&root.join("delayed-parent-ready"));

    record_scoped_stop(ScopedStopOptions {
        harness_home: harness_home.clone(),
        target: ScopedStopTarget::QueueItem {
            queue_id: queue_id.clone(),
        },
        reason: "boundary fence before final settlement".to_string(),
        now_ms: 3_300,
    })
    .unwrap();
    let fence_only = progress_plan(&harness_home, 3_400);
    assert!(
        fence_only.pending.is_empty(),
        "a fence alone must not fabricate final delivery evidence"
    );

    let new_boundary = receive(&source, &harness_home, "/new", 3_500);
    assert_eq!(new_boundary.status, ChannelReceiveStatus::CommandApplied);
    let committed_state: Value =
        serde_json::from_slice(&fs::read(exact_channel_state_file(&harness_home)).unwrap())
            .unwrap();
    assert_ne!(committed_state["activeSessionKey"], old.session_key);
    let transition_receipts =
        fs::read_to_string(harness_home.join("state/channel-session-transitions/receipts.jsonl"))
            .unwrap();
    assert!(transition_receipts.contains("\"phase\":\"boundary-committed\""));

    fs::write(root.join("delayed-parent-release"), "release").unwrap();
    let restarted = runtime.join().expect("delayed runtime thread");
    assert_eq!(
        restarted.receipt.status,
        RuntimeRunOnceStatus::FailedTerminal
    );
    assert_eq!(
        restarted.receipt.source_final_expectation,
        Some(SourceFinalExpectationV1::ExplicitNonDelivery),
        "stale-session cancellation must leave durable non-delivery evidence"
    );
    assert_eq!(
        restarted.receipt.final_outbox_disposition,
        Some(FinalOutboxDispositionV1::ExplicitNonDelivery)
    );
    assert!(restarted.outbound_message.is_none());

    let after_restart = progress_plan(&harness_home, 3_700);
    assert!(
        after_restart
            .pending
            .iter()
            .filter(|pending| pending.queue_id == queue_id)
            .all(|pending| pending.terminal),
        "old-session progress may only converge through terminal surfaces"
    );
    let old_terminal = after_restart
        .pending
        .into_iter()
        .filter(|pending| pending.queue_id == queue_id)
        .collect::<Vec<_>>();
    assert!(
        !old_terminal.is_empty(),
        "typed non-delivery should release one final progress settlement"
    );
    for (index, pending) in old_terminal.into_iter().enumerate() {
        record_agent_progress_delivery(AgentProgressDeliveryRecordOptions {
            harness_home: harness_home.clone(),
            queue_id: pending.queue_id,
            platform: pending.platform,
            account_id: pending.account_id,
            channel_id: pending.channel_id,
            thread_id: pending.thread_id,
            user_id: pending.user_id,
            session_key: pending.session_key,
            message_kind: pending.message_kind,
            action: pending.action,
            status: AgentProgressDeliveryStatus::Delivered,
            provider_message_id: Some(format!("old-terminal-settlement-{}", index + 1)),
            event_line: pending.event_line,
            text_hash: pending.text_hash,
            terminal: pending.terminal,
            policy_decision: Some("typed-non-delivery-settlement".to_string()),
            error: None,
            now_ms: 3_750,
        })
        .unwrap();
    }
    let converged = progress_plan(&harness_home, 3_800);
    assert!(
        converged
            .pending
            .iter()
            .all(|pending| pending.queue_id != queue_id),
        "delivered terminal settlement must not hang across another restart"
    );
    let supersede =
        fs::read_to_string(harness_home.join("state/progress/session-supersede-receipts.jsonl"))
            .unwrap();
    assert!(supersede.contains(&old.session_key));
    let _ = fs::remove_dir_all(root);
}

#[test]
fn legacy_state_is_readable_without_fabricating_completion_or_native_authority() {
    let root = temp_root("legacy-state-fail-closed");
    let harness_home = root.join("harness");

    let projection_file =
        harness_home.join("state/runtime-queue/codex-goal-projection-receipts.jsonl");
    append_line(
        &projection_file,
        &json!({
            "schema": "agent-harness.codex-goal-projection.v1",
            "queueId": "legacy-goal-queue",
            "sessionKey": "telegram:dm-42:user-7:main",
            "sourceThreadId": "legacy-thread",
            "sourceTurnId": "legacy-turn",
            "goalReference": "legacy-goal",
            "laneDigest": lane_digest(),
            "backendContextGeneration": "legacy-generation",
            "objective": "legacy objective",
            "status": "completed",
            "sourceFinalEligible": true,
            "goalChecksum": "legacy-goal-checksum",
            "projectionChecksum": "legacy-projection-checksum",
            "projectionComplete": true,
            "observationOrder": 1,
            "observedAtMs": 4_000
        }),
    );
    let projection = latest_goal_projection_for_queue(&harness_home, "legacy-goal-queue")
        .unwrap()
        .expect("legacy projection remains readable");
    assert_eq!(
        projection.observation_phase,
        GoalProjectionObservationPhaseV1::LegacyUnknown
    );
    assert_eq!(
        projection.turn_relation,
        GoalProjectionTurnRelationV1::Uncorrelated
    );
    assert!(!projection.source_final_eligible);
    let doctor = run_goal_lineage_doctor(GoalLineageDoctorOptions {
        harness_home: harness_home.clone(),
        lane_digest: None,
        virtual_session_id: None,
    })
    .unwrap();
    assert!(
        doctor
            .lineages
            .iter()
            .all(|lineage| !lineage.source_final_eligible),
        "legacy projection claimed terminal authority: {:#?}",
        doctor.lineages
    );

    let progress = progress_context("legacy-progress-queue", "telegram:dm-42:user-7:main");
    append_agent_progress_event(
        &harness_home,
        &AgentProgressEvent::new(
            &progress,
            AgentProgressKind::Runtime,
            "legacy run",
            "completed",
            AgentProgressStatus::Completed,
            4_100,
        ),
    )
    .unwrap();
    let runtime_receipts = harness_home.join("state/runtime-queue/run-once-receipts.jsonl");
    append_line(
        &runtime_receipts,
        &json!({
            "schema": "agent-harness.runtime-run-once.v1",
            "queueId": progress.queue_id,
            "status": "completed",
            "reason": "pre-source-final receipt"
        }),
    );
    let legacy_progress = progress_plan(&harness_home, 4_200);
    assert!(
        !legacy_progress.pending.is_empty(),
        "legacy receipt wedged progress"
    );
    assert!(
        legacy_progress
            .pending
            .iter()
            .all(|pending| pending.terminal)
    );
    assert!(
        legacy_progress
            .warnings
            .iter()
            .any(|warning| warning.contains("legacy-expectation"))
    );

    let context = external_effect_context();
    let descriptor = McpElicitationDescriptorV1 {
        connector: "calendar".to_string(),
        action: "create-event".to_string(),
        params_digest: format!("sha256:{}", "2".repeat(64)),
        action_summary: "create a bounded calendar event".to_string(),
        mode: "form".to_string(),
    };
    let (intent, token) = match begin_external_effect_request(
        &harness_home,
        &context,
        &descriptor,
        &ConnectorApprovalPolicyV1::default(),
    )
    .unwrap()
    {
        ExternalEffectAdmissionV1::NeedsUser { intent, token } => (intent, token),
        other => panic!("unexpected approval admission: {other:?}"),
    };
    let latest = harness_home
        .join("state/external-effects/latest")
        .join(format!("{}.json", intent.effect_id));
    let mut legacy: Value = serde_json::from_slice(&fs::read(&latest).unwrap()).unwrap();
    legacy
        .as_object_mut()
        .unwrap()
        .remove("sourceSessionKeyDigest");
    legacy
        .as_object_mut()
        .unwrap()
        .remove("approvalAuthorityDigest");
    legacy["approvalToken"]
        .as_object_mut()
        .unwrap()
        .remove("sourceSessionKeyDigest");
    legacy["approvalToken"]
        .as_object_mut()
        .unwrap()
        .remove("approvalAuthorityDigest");
    fs::write(&latest, serde_json::to_vec_pretty(&legacy).unwrap()).unwrap();

    // Reconstructing from the downgraded durable snapshot keeps the legacy
    // text path available but never upgrades it into native callback authority.
    let reloaded = load_external_effect_intent(&harness_home, &intent.effect_id)
        .unwrap()
        .expect("legacy approval snapshot remains readable");
    assert!(reloaded.source_session_key_digest.is_empty());
    let native = ChannelInboundActionV1::new(
        "provider-event-legacy",
        None,
        &intent.effect_id,
        intent.approval_generation,
        ChannelApprovalDecisionV1::Approve,
        &context.source_session_key_digest,
        &context.approval_authority_digest,
        &token,
    )
    .unwrap();
    assert_eq!(
        resolve_external_effect_channel_action(&harness_home, &native, &context.exact_lane_digest)
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::PermissionDenied
    );
    let approved = resolve_external_effect_approval(
        &harness_home,
        &token,
        &context.exact_lane_digest,
        ExternalEffectApprovalDecisionV1::Approve,
    )
    .unwrap();
    assert_eq!(approved.state, ExternalEffectStateV1::Approved);
    assert!(approved.source_session_key_digest.is_empty());
    let _ = fs::remove_dir_all(root);
}

fn receive(
    source: &AgentSource,
    harness_home: &Path,
    message: &str,
    now_ms: i64,
) -> agent_harness_core::ChannelReceiveReport {
    receive_channel_message(ChannelReceiveOptions {
        source: source.clone(),
        runtime_workspace: None,
        harness_home: harness_home.to_path_buf(),
        skill_index: build_source_skill_index(source).unwrap(),
        platform: "telegram".to_string(),
        account_id: None,
        channel_id: "dm-42".to_string(),
        user_id: "user-7".to_string(),
        agent_id: Some("main".to_string()),
        session_key: None,
        message: message.to_string(),
        inbound_context: None,
        inbound_media_artifacts: Vec::new(),
        inbound_event_kind: None,
        inbound_event_id: None,
        skill_limit: 3,
        now_ms,
    })
    .unwrap()
}

fn progress_context(queue_id: &str, session_key: &str) -> AgentProgressContext {
    AgentProgressContext {
        queue_id: queue_id.to_string(),
        agent_id: Some("main".to_string()),
        account_id: None,
        thread_id: None,
        session_key: session_key.to_string(),
        platform: "telegram".to_string(),
        channel_id: "dm-42".to_string(),
        user_id: "user-7".to_string(),
    }
}

fn progress_plan(
    harness_home: &Path,
    now_ms: i64,
) -> agent_harness_core::AgentProgressDeliveryPlanReport {
    plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
        harness_home: harness_home.to_path_buf(),
        platform: Some("telegram".to_string()),
        now_ms,
        min_update_interval_ms: 0,
        ..AgentProgressDeliveryPlanOptions::default()
    })
    .unwrap()
}

fn write_source_final_receipt(
    harness_home: &Path,
    context: &AgentProgressContext,
    status: &str,
    expectation: &str,
    disposition: &str,
) {
    append_line(
        &harness_home.join("state/runtime-queue/run-once-receipts.jsonl"),
        &json!({
            "schema": "agent-harness.runtime-run-once.v1",
            "queueId": context.queue_id,
            "status": status,
            "sourceFinalExpectation": expectation,
            "finalOutboxDisposition": disposition,
            "sourceFinalLaneDigest": lane_digest(),
            "reason": "terminal runtime evidence before provider final"
        }),
    );
}

fn record_completed_closure(
    harness_home: &Path,
    lane_digest: &str,
    session_key: &str,
) -> GoalClosureIntentV1 {
    let intent = GoalClosureIntentV1::new(
        GoalClosureTriggerV1::ChannelStop,
        GoalClosureDispositionV1::Canceled,
        GoalClosureAuthorityV1 {
            lane_digest: lane_digest.to_string(),
            concrete_session_key: session_key.to_string(),
            virtual_session_id: "closed-virtual-session".to_string(),
            backend_context_generation: "closed-generation".to_string(),
            source_thread_id: "closed-parent-thread".to_string(),
        },
        "closed-goal",
        "closed-generation",
        Some("closed-projection".to_string()),
        "stop-command-effect",
        "closed before late child completion",
    )
    .unwrap();
    record_goal_closure_intent(harness_home, &intent).unwrap();
    for (phase, result, at) in [
        (
            GoalClosurePhaseV1::IntentRecorded,
            GoalClosureResultV1::Pending,
            1,
        ),
        (
            GoalClosurePhaseV1::BackendResultRecorded,
            GoalClosureResultV1::Succeeded,
            2,
        ),
        (
            GoalClosurePhaseV1::TerminalProjectionRecorded,
            GoalClosureResultV1::Succeeded,
            3,
        ),
        (
            GoalClosurePhaseV1::LineageReconciled,
            GoalClosureResultV1::Succeeded,
            4,
        ),
        (
            GoalClosurePhaseV1::Completed,
            GoalClosureResultV1::Succeeded,
            5,
        ),
    ] {
        let mut input = closure_phase(result, at);
        if phase == GoalClosurePhaseV1::TerminalProjectionRecorded {
            input.projection_checksum = Some("terminal-projection".to_string());
        }
        if matches!(
            phase,
            GoalClosurePhaseV1::LineageReconciled | GoalClosurePhaseV1::Completed
        ) {
            input.lineage_checksum = Some("terminal-lineage".to_string());
        }
        record_goal_closure_phase(harness_home, &intent, phase, input).unwrap();
    }
    intent
}

fn closure_phase(result: GoalClosureResultV1, recorded_at_ms: i64) -> GoalClosurePhaseInputV1 {
    GoalClosurePhaseInputV1 {
        result,
        projection_checksum: None,
        lineage_checksum: None,
        result_evidence_digest: Some(format!("evidence-{recorded_at_ms}")),
        recorded_at_ms,
    }
}

fn external_effect_context() -> ExternalEffectRequestContextV1 {
    let exact_lane_digest = lane_digest();
    let source_session_key_digest =
        external_effect_source_session_key_digest("telegram:dm-42:user-7:main").unwrap();
    let approval_authority_digest = external_effect_approval_authority_digest(
        &exact_lane_digest,
        &source_session_key_digest,
        "legacy-lineage",
        "legacy-effect-queue",
    )
    .unwrap();
    ExternalEffectRequestContextV1 {
        exact_lane_digest,
        logical_lineage_id: "legacy-lineage".to_string(),
        source_queue_id: "legacy-effect-queue".to_string(),
        source_session_key_digest,
        approval_authority_digest,
    }
}

fn seed_external_effect(harness_home: &Path, queue_id: &str) -> String {
    let pending = pending_queue(harness_home, queue_id);
    let exact_lane_digest = exact_full_lane_digest(harness_home, queue_id);
    let source_session_key_digest = external_effect_source_session_key_digest(
        pending["sessionKey"].as_str().expect("source session key"),
    )
    .unwrap();
    let context = ExternalEffectRequestContextV1 {
        approval_authority_digest: external_effect_approval_authority_digest(
            &exact_lane_digest,
            &source_session_key_digest,
            "shell-replay-lineage",
            queue_id,
        )
        .unwrap(),
        exact_lane_digest,
        logical_lineage_id: "shell-replay-lineage".to_string(),
        source_queue_id: queue_id.to_string(),
        source_session_key_digest,
    };
    let descriptor = McpElicitationDescriptorV1 {
        connector: "sanitized-calendar".to_string(),
        action: "create-bounded-event".to_string(),
        params_digest: format!("sha256:{}", "4".repeat(64)),
        action_summary: "create one sanitized bounded event".to_string(),
        mode: "form".to_string(),
    };
    match begin_external_effect_request(
        harness_home,
        &context,
        &descriptor,
        &ConnectorApprovalPolicyV1::default(),
    )
    .unwrap()
    {
        ExternalEffectAdmissionV1::NeedsUser { intent, .. } => intent.effect_id,
        other => panic!("unexpected external-effect admission: {other:?}"),
    }
}

fn lane_digest() -> String {
    ChannelStateLane::new("telegram", None, "dm-42", "user-7", "main")
        .unwrap()
        .exact_lane_digest()
}

fn append_line(path: &Path, value: &Value) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    use std::io::Write as _;
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    writeln!(file, "{}", serde_json::to_string(value).unwrap()).unwrap();
}

fn nonempty_lines(path: &Path) -> usize {
    fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter(|line| !line.trim().is_empty())
        .count()
}

fn remove_codex_bindings(harness_home: &Path) {
    let sessions = harness_home.join("agents/main/sessions");
    for entry in fs::read_dir(sessions).unwrap().filter_map(Result::ok) {
        let path = entry.path();
        if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(".codex-app-server.json"))
        {
            fs::remove_file(path).unwrap();
        }
    }
}

fn pending_queue(harness_home: &Path, queue_id: &str) -> Value {
    fs::read_to_string(harness_home.join("state/runtime-queue/pending.jsonl"))
        .unwrap()
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find(|row| row["queueId"] == queue_id)
        .unwrap_or_else(|| panic!("pending queue {queue_id} was not durable"))
}

fn set_pending_shell_recovery_depth(harness_home: &Path, queue_id: &str, depth: u64) {
    let file = harness_home.join("state/runtime-queue/pending.jsonl");
    let mut rows = fs::read_to_string(&file)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    let row = rows
        .iter_mut()
        .find(|row| row["queueId"] == queue_id)
        .unwrap_or_else(|| panic!("pending queue {queue_id} was not durable"));
    row["shellRecoveryDepth"] = json!(depth);
    fs::write(
        file,
        rows.iter()
            .map(|row| serde_json::to_string(row).unwrap())
            .collect::<Vec<_>>()
            .join("\n")
            + "\n",
    )
    .unwrap();
}

fn exact_channel_state_file(harness_home: &Path) -> PathBuf {
    let root = harness_home.join("state/channels/v2");
    let mut states = fs::read_dir(&root)
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.path().join("state.json"))
        .filter(|path| path.is_file())
        .collect::<Vec<_>>();
    states.sort();
    assert_eq!(
        states.len(),
        1,
        "expected one exact channel state under {root:?}"
    );
    states.remove(0)
}

fn load_outbox(harness_home: &Path) -> Vec<Value> {
    fs::read_to_string(harness_home.join("state/channels/outbox.jsonl"))
        .unwrap_or_default()
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect()
}

fn write_owned_event_config(harness_home: &Path) {
    fs::create_dir_all(harness_home).unwrap();
    fs::write(
        harness_home.join("harness-config.json"),
        r#"{"orchestration":{"features":{"ownedCodexEventsV2":{"mode":"authoritative"}}}}"#,
    )
    .unwrap();
}

fn wait_for_file(path: &Path) {
    for _ in 0..3_000 {
        if path.is_file() {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    panic!("timed out waiting for runtime synchronization file {path:?}");
}

fn write_source(root: &Path) -> AgentSource {
    let home = root.join("source");
    let workspace = home.join("workspace");
    let skill = workspace.join("skills/continuity");
    fs::create_dir_all(&skill).unwrap();
    fs::create_dir_all(home.join("agents/main/sessions")).unwrap();
    fs::write(workspace.join("AGENTS.md"), "# Test agent\n").unwrap();
    fs::write(
        skill.join(agent_harness_core::SKILL_FILE_NAME),
        "# Continuity\n",
    )
    .unwrap();
    fs::write(
        home.join("openclaw.json"),
        r#"{
          "agents": {
            "defaults": { "provider": "openai", "model": "codex" },
            "list": [{ "id": "main", "model": "gpt-5", "enabled": true }]
          },
          "models": { "providers": { "openai": { "apiKey": "${OPENAI_API_KEY}" } } }
        }"#,
    )
    .unwrap();
    fs::write(home.join("agents/main/sessions/sessions.json"), "{}").unwrap();
    AgentSource::with_workspace(home, workspace)
}

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("continuity-effects")
        .join(name)
}

fn exact_full_lane_digest(harness_home: &Path, queue_id: &str) -> String {
    let pending = pending_queue(harness_home, queue_id);
    let session_key = pending["sessionKey"].as_str().unwrap();
    FullLaneKeyV1::new(
        pending["platform"].as_str().unwrap(),
        pending["accountId"].as_str().unwrap_or("default"),
        pending["channelId"].as_str().unwrap(),
        pending["userId"].as_str().unwrap(),
        pending["agentId"].as_str().unwrap(),
        pending["runtimeClass"].as_str().unwrap(),
        root_working_session_key(session_key),
        session_key,
    )
    .unwrap()
    .identity_hash()
    .unwrap()
}

fn fake_active_goal_blocker_codex(root: &Path) -> PathBuf {
    let stale = stale_shell_path();
    let rows = vec![
        json!({"method":"turn/started","params":{"threadId":"thread-parent","turn":{"id":"turn-parent","kind":"regular"}}}),
        json!({"method":"turn/started","params":{"threadId":"thread-parent","turn":{"id":"turn-child-a","kind":"subAgent","name":"child-review-a","path":["parent","child-review-a"]}}}),
        json!({"method":"turn/completed","params":{"threadId":"thread-parent","turn":{"id":"turn-child-a","kind":"subAgent","status":"completed"}}}),
        json!({"method":"turn/started","params":{"threadId":"thread-parent","turn":{"id":"turn-child-b","kind":"subAgent","name":"child-review-b","path":["parent","child-review-b"]}}}),
        json!({"method":"turn/completed","params":{"threadId":"thread-parent","turn":{"id":"turn-child-b","kind":"subAgent","status":"completed"}}}),
        json!({"method":"item/completed","params":{"threadId":"thread-parent","turnId":"turn-parent","item":{"type":"fileChange","id":"parent-file-change","status":"completed","changes":[{"path":"sanitized-source.rs","kind":"update"}]}}}),
        json!({"method":"item/started","params":{"threadId":"thread-parent","turnId":"turn-parent","item":{"type":"commandExecution","id":"ordinary-failure","command":"Get-Content sanitized-missing-input.json","cwd":root}}}),
        json!({"method":"item/completed","params":{"threadId":"thread-parent","turnId":"turn-parent","item":{"type":"commandExecution","id":"ordinary-failure","status":"failed","exitCode":1,"durationMs":20,"aggregatedOutput":"Get-Content could not find sanitized-missing-input.json"}}}),
        json!({"method":"item/started","params":{"threadId":"thread-parent","turnId":"turn-parent","item":{"type":"commandExecution","id":"ambiguous-failure","command":"sanitized-cli inspect","cwd":root}}}),
        json!({"method":"item/completed","params":{"threadId":"thread-parent","turnId":"turn-parent","item":{"type":"commandExecution","id":"ambiguous-failure","status":"failed","exitCode":1,"durationMs":15,"aggregatedOutput":""}}}),
        json!({"method":"item/started","params":{"threadId":"thread-parent","turnId":"turn-parent","item":{"type":"commandExecution","id":"process-start-failure-a","command":format!(r#""{stale}" -NoProfile -Command Get-ChildItem"#),"cwd":root}}}),
        json!({"method":"item/completed","params":{"threadId":"thread-parent","turnId":"turn-parent","item":{"type":"commandExecution","id":"process-start-failure-a","status":"failed","exitCode":-1,"durationMs":0,"aggregatedOutput":"process start failed: OS error 3; path not found"}}}),
        json!({"method":"item/started","params":{"threadId":"thread-parent","turnId":"turn-parent","item":{"type":"commandExecution","id":"process-start-failure-b","command":format!(r#""{stale}" -NoProfile -Command Get-Location"#),"cwd":root}}}),
        json!({"method":"item/completed","params":{"threadId":"thread-parent","turnId":"turn-parent","item":{"type":"commandExecution","id":"process-start-failure-b","status":"failed","exitCode":-1,"durationMs":0,"aggregatedOutput":"process start failed: OS error 3; path not found"}}}),
        json!({"method":"thread/goal/updated","params":{"threadId":"thread-parent","turnId":"turn-parent","goal":{"id":"goal-active","objective":"finish the durable implementation","status":"active","completionCriteria":["T3 replay passes"]}}}),
        json!({"method":"item/completed","params":{"threadId":"thread-parent","turnId":"turn-parent","item":{"type":"agentMessage","id":"parent-blocker","phase":"final_answer","text":"The goal remains active, but command execution is blocked."}}}),
        json!({"method":"turn/completed","params":{"threadId":"thread-parent","turn":{"id":"turn-parent","kind":"regular","status":"completed"}}}),
    ];
    fake_event_codex(root, "active-goal-blocker", &rows)
}

fn fake_active_goal_conflict_codex(root: &Path, suffix: &str) -> PathBuf {
    let goal_id = format!("goal-active-{suffix}");
    let rows = vec![
        json!({"method":"turn/started","params":{"threadId":"thread-parent","turn":{"id":"turn-parent","kind":"regular"}}}),
        json!({"method":"thread/goal/updated","params":{"threadId":"thread-parent","turnId":"turn-parent","goal":{"id":goal_id,"objective":"finish the durable implementation","status":"active","completionCriteria":["T3 replay passes"]}}}),
        json!({"method":"item/completed","params":{"threadId":"thread-parent","turnId":"turn-parent","item":{"type":"agentMessage","id":format!("parent-blocker-{suffix}"),"phase":"final_answer","text":"The goal remains active while exact authority is evaluated."}}}),
        json!({"method":"turn/completed","params":{"threadId":"thread-parent","turn":{"id":"turn-parent","kind":"regular","status":"completed"}}}),
    ];
    fake_event_codex(root, &format!("active-goal-conflict-{suffix}"), &rows)
}

fn fake_active_goal_shell_case_codex(
    root: &Path,
    shell_drift: bool,
    protocol_failure: Option<&str>,
) -> PathBuf {
    let stale = stale_shell_path();
    let (exit_code, duration_ms, output) = if shell_drift {
        (-1, 0, "process start failed: OS error 3; path not found")
    } else {
        (
            1,
            25,
            "Get-Content could not find archived-task/missing.json",
        )
    };
    let mut rows = vec![
        json!({"method":"turn/started","params":{"threadId":"thread-parent","turn":{"id":"turn-parent","kind":"regular"}}}),
        json!({"method":"thread/goal/updated","params":{"threadId":"thread-parent","turnId":"turn-parent","goal":{"id":"goal-active","objective":"finish the durable implementation","status":"active","completionCriteria":["T3 replay passes"]}}}),
        json!({"method":"turn/started","params":{"threadId":"thread-parent","turn":{"id":"turn-child-a","kind":"subAgent","name":"routing-reader","path":["parent","routing-reader"]}}}),
        json!({"method":"item/started","params":{"threadId":"thread-parent","turnId":"turn-child-a","item":{"type":"commandExecution","id":"child-a-command","command":"Get-Content docs/invariants.md"}}}),
        json!({"method":"item/completed","params":{"threadId":"thread-parent","turnId":"turn-child-a","item":{"type":"commandExecution","id":"child-a-command","status":"completed","exitCode":0,"durationMs":20}}}),
        json!({"method":"item/completed","params":{"threadId":"thread-parent","turnId":"turn-child-a","item":{"type":"agentMessage","id":"child-a-final","phase":"final_answer","text":"Inspect the routing contract"}}}),
        json!({"method":"turn/completed","params":{"threadId":"thread-parent","turn":{"id":"turn-child-a","kind":"subAgent","status":"completed"}}}),
        json!({"method":"turn/started","params":{"threadId":"thread-parent","turn":{"id":"turn-child-b","kind":"subAgent","name":"lifecycle-reader","path":["parent","lifecycle-reader"]}}}),
        json!({"method":"item/started","params":{"threadId":"thread-parent","turnId":"turn-child-b","item":{"type":"commandExecution","id":"child-b-command","command":"Get-Content docs/agent-harness-topology-contract.md"}}}),
        json!({"method":"item/completed","params":{"threadId":"thread-parent","turnId":"turn-child-b","item":{"type":"commandExecution","id":"child-b-command","status":"completed","exitCode":0,"durationMs":25}}}),
        json!({"method":"item/completed","params":{"threadId":"thread-parent","turnId":"turn-child-b","item":{"type":"agentMessage","id":"child-b-final","phase":"final_answer","text":"Inspect the lifecycle contract"}}}),
        json!({"method":"turn/completed","params":{"threadId":"thread-parent","turn":{"id":"turn-child-b","kind":"subAgent","status":"completed"}}}),
        json!({"method":"item/started","params":{"threadId":"thread-parent","turnId":"turn-parent","item":{"type":"collabToolCall","id":"parent-wait","toolName":"wait_agent"}}}),
        json!({"method":"item/completed","params":{"threadId":"thread-parent","turnId":"turn-parent","item":{"type":"collabToolCall","id":"parent-wait","toolName":"wait_agent","status":"completed"}}}),
        json!({"method":"item/started","params":{"threadId":"thread-parent","turnId":"turn-parent","item":{"type":"commandExecution","id":"shell-drift","command":format!(r#""{stale}" -NoProfile -Command Get-ChildItem"#),"cwd":root}}}),
        json!({"method":"item/completed","params":{"threadId":"thread-parent","turnId":"turn-parent","item":{"type":"commandExecution","id":"shell-drift","status":"failed","exitCode":exit_code,"durationMs":duration_ms,"aggregatedOutput":output}}}),
        json!({"method":"item/completed","params":{"threadId":"thread-parent","turnId":"turn-parent","item":{"type":"agentMessage","id":"parent-blocker","phase":"final_answer","text":"The goal remains active after the shell failed to start."}}}),
        json!({"method":"turn/completed","params":{"threadId":"thread-parent","turn":{"id":"turn-parent","kind":"regular","status":"completed"}}}),
    ];
    if let Some(reason) = protocol_failure {
        rows.insert(
            rows.len() - 1,
            json!({"method":"error","params":{"error":{"message":reason,"codexErrorInfo":"other","additionalDetails":null},"willRetry":false,"threadId":"thread-parent","turnId":"turn-parent"}}),
        );
    }
    fake_event_codex(root, "active-goal-shell-drift", &rows)
}

fn fake_event_codex(root: &Path, name: &str, rows: &[Value]) -> PathBuf {
    #[cfg(windows)]
    {
        let body = rows
            .iter()
            .map(|row| {
                format!(
                    "[Console]::Out.WriteLine('{}')",
                    serde_json::to_string(row).unwrap().replace('\'', "''")
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        fake_windows_codex(root, name, &body)
    }
    #[cfg(not(windows))]
    {
        let body = rows
            .iter()
            .map(|row| {
                format!(
                    "printf '%s\\n' '{}'",
                    serde_json::to_string(row).unwrap().replace('\'', "'\\''")
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        fake_unix_codex(root, name, &body)
    }
}

fn stale_shell_path() -> &'static str {
    r"C:\Program Files\WindowsApps\Microsoft.PowerShell_7.6.3.0_x64__8wekyb3d8bbwe\pwsh.exe"
}

fn codex_runtime_receipt_for_queue(harness_home: &Path, queue_id: &str) -> Value {
    fs::read_to_string(harness_home.join("state/runtime-queue/codex-runtime-run-receipts.jsonl"))
        .unwrap()
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find(|receipt| receipt["queueId"] == queue_id)
        .expect("runtime receipt for source queue")
}

#[cfg(windows)]
fn fake_parent_final_codex(root: &Path) -> PathBuf {
    fake_windows_codex(
        root,
        "parent-final",
        r#"
        [Console]::Out.WriteLine('{"method":"turn/started","params":{"threadId":"thread-parent","turn":{"id":"turn-parent","kind":"regular"}}}')
        [Console]::Out.WriteLine('{"method":"item/agentMessage/delta","params":{"threadId":"thread-parent","turnId":"turn-parent","itemId":"parent-message","delta":"Fresh queue B completed."}}')
        [Console]::Out.WriteLine('{"method":"turn/completed","params":{"threadId":"thread-parent","turn":{"id":"turn-parent","status":"completed"}}}')
"#,
    )
}

#[cfg(windows)]
fn fake_delayed_parent_final_codex(root: &Path) -> PathBuf {
    let ready = root.join("delayed-parent-ready");
    let release = root.join("delayed-parent-release");
    fake_windows_codex(
        root,
        "delayed-parent-final",
        &format!(
            r#"
        [IO.File]::WriteAllText('{}', 'ready')
        while (-not [IO.File]::Exists('{}')) {{ Start-Sleep -Milliseconds 10 }}
        [Console]::Out.WriteLine('{{"method":"turn/started","params":{{"threadId":"thread-delayed","turn":{{"id":"turn-delayed","kind":"regular"}}}}}}')
        [Console]::Out.WriteLine('{{"method":"item/agentMessage/delta","params":{{"threadId":"thread-delayed","turnId":"turn-delayed","itemId":"delayed-message","delta":"Old session final after boundary."}}}}')
        [Console]::Out.WriteLine('{{"method":"turn/completed","params":{{"threadId":"thread-delayed","turn":{{"id":"turn-delayed","status":"completed"}}}}}}')
"#,
            ready.display(),
            release.display()
        ),
    )
}

#[cfg(windows)]
fn fake_child_only_codex(root: &Path) -> PathBuf {
    fake_windows_codex(
        root,
        "child-only",
        r#"
        [Console]::Out.WriteLine('{"method":"turn/started","params":{"threadId":"thread-parent","turn":{"id":"turn-parent","kind":"regular"}}}')
        [Console]::Out.WriteLine('{"method":"turn/started","params":{"threadId":"thread-parent","turn":{"id":"turn-child","kind":"subAgent"}}}')
        [Console]::Out.WriteLine('{"method":"item/completed","params":{"threadId":"thread-parent","turnId":"turn-child","item":{"type":"agentMessage","id":"child-final","text":"LATE CHILD FINAL MUST STAY INTERNAL","phase":"final_answer"}}}')
        [Console]::Out.WriteLine('{"method":"turn/completed","params":{"threadId":"thread-parent","turn":{"id":"turn-child","status":"completed"}}}')
        [Console]::Out.WriteLine('{"method":"turn/completed","params":{"threadId":"thread-parent","turn":{"id":"turn-parent","status":"completed"}}}')
"#,
    )
}

#[cfg(windows)]
fn fake_windows_codex(root: &Path, name: &str, events: &str) -> PathBuf {
    let script = root.join(format!("fake-{name}-app-server.ps1"));
    fs::write(
        &script,
        format!(
            r#"
while ($true) {{
    $line = [Console]::In.ReadLine()
    if ($null -eq $line) {{ break }}
    try {{ $msg = $line | ConvertFrom-Json }} catch {{ continue }}
    if ($msg.id -eq 0) {{
        [Console]::Out.WriteLine('{{"id":0,"result":{{"ok":true}}}}')
    }} elseif ($msg.method -eq 'thread/start') {{
        [Console]::Out.WriteLine('{{"id":1,"result":{{"thread":{{"id":"thread-parent"}}}}}}')
    }} elseif ($msg.method -eq 'turn/start') {{
{events}
        [Console]::Out.Flush()
        break
    }}
    [Console]::Out.Flush()
}}
"#
        ),
    )
    .unwrap();
    let command = root.join(format!("fake-{name}-codex.cmd"));
    fs::write(
        &command,
        format!(
            "@echo off\r\npowershell -NoProfile -ExecutionPolicy Bypass -File \"{}\"\r\n",
            script.display()
        ),
    )
    .unwrap();
    command
}

#[cfg(not(windows))]
fn fake_parent_final_codex(root: &Path) -> PathBuf {
    fake_unix_codex(
        root,
        "parent-final",
        r#"
            printf '%s\n' '{"method":"turn/started","params":{"threadId":"thread-parent","turn":{"id":"turn-parent","kind":"regular"}}}'
            printf '%s\n' '{"method":"item/agentMessage/delta","params":{"threadId":"thread-parent","turnId":"turn-parent","itemId":"parent-message","delta":"Fresh queue B completed."}}'
            printf '%s\n' '{"method":"turn/completed","params":{"threadId":"thread-parent","turn":{"id":"turn-parent","status":"completed"}}}'
"#,
    )
}

#[cfg(not(windows))]
fn fake_delayed_parent_final_codex(root: &Path) -> PathBuf {
    let ready = root.join("delayed-parent-ready");
    let release = root.join("delayed-parent-release");
    fake_unix_codex(
        root,
        "delayed-parent-final",
        &format!(
            r#"
            : > '{}'
            while [ ! -f '{}' ]; do sleep 0.01; done
            printf '%s\n' '{{"method":"turn/started","params":{{"threadId":"thread-delayed","turn":{{"id":"turn-delayed","kind":"regular"}}}}}}'
            printf '%s\n' '{{"method":"item/agentMessage/delta","params":{{"threadId":"thread-delayed","turnId":"turn-delayed","itemId":"delayed-message","delta":"Old session final after boundary."}}}}'
            printf '%s\n' '{{"method":"turn/completed","params":{{"threadId":"thread-delayed","turn":{{"id":"turn-delayed","status":"completed"}}}}}}'
"#,
            ready.display(),
            release.display()
        ),
    )
}

#[cfg(not(windows))]
fn fake_child_only_codex(root: &Path) -> PathBuf {
    fake_unix_codex(
        root,
        "child-only",
        r#"
            printf '%s\n' '{"method":"turn/started","params":{"threadId":"thread-parent","turn":{"id":"turn-parent","kind":"regular"}}}'
            printf '%s\n' '{"method":"turn/started","params":{"threadId":"thread-parent","turn":{"id":"turn-child","kind":"subAgent"}}}'
            printf '%s\n' '{"method":"item/completed","params":{"threadId":"thread-parent","turnId":"turn-child","item":{"type":"agentMessage","id":"child-final","text":"LATE CHILD FINAL MUST STAY INTERNAL","phase":"final_answer"}}}'
            printf '%s\n' '{"method":"turn/completed","params":{"threadId":"thread-parent","turn":{"id":"turn-child","status":"completed"}}}'
            printf '%s\n' '{"method":"turn/completed","params":{"threadId":"thread-parent","turn":{"id":"turn-parent","status":"completed"}}}'
"#,
    )
}

#[cfg(not(windows))]
fn fake_unix_codex(root: &Path, name: &str, events: &str) -> PathBuf {
    let script = root.join(format!("fake-{name}-codex"));
    fs::write(
        &script,
        format!(
            r#"#!/bin/sh
while IFS= read -r line; do
    case "$line" in
        *'"id":0'*) printf '%s\n' '{{"id":0,"result":{{"ok":true}}}}' ;;
        *'"method":"thread/start"'*) printf '%s\n' '{{"id":1,"result":{{"thread":{{"id":"thread-parent"}}}}}}' ;;
        *'"method":"turn/start"'*)
{events}
            exit 0
            ;;
    esac
done
"#
        ),
    )
    .unwrap();
    use std::os::unix::fs::PermissionsExt;
    let mut permissions = fs::metadata(&script).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&script, permissions).unwrap();
    script
}

struct EnvGuard {
    key: &'static str,
    original: Option<OsString>,
}

impl EnvGuard {
    fn set(key: &'static str, value: OsString) -> Self {
        let original = std::env::var_os(key);
        // SAFETY: this integration target is required to run serially and the
        // guard restores the process environment before the test returns.
        unsafe { std::env::set_var(key, value) };
        Self { key, original }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        if let Some(value) = self.original.take() {
            // SAFETY: see EnvGuard::set; this target is executed serially.
            unsafe { std::env::set_var(self.key, value) };
        } else {
            // SAFETY: see EnvGuard::set; this target is executed serially.
            unsafe { std::env::remove_var(self.key) };
        }
    }
}

fn temp_root(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "agent-harness-continuity-{name}-{}-{nanos}",
        std::process::id()
    ))
}
