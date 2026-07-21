use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use agent_harness_core::channel_action::{ChannelApprovalDecisionV1, ChannelInboundActionV1};
use agent_harness_core::codex_runtime::{
    CodexRuntimePlanOptions, CodexRuntimeRunOptions, CodexRuntimeRunStatus, plan_codex_runtime,
    run_codex_runtime,
};
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
    GoalLineageDoctorOptions, GoalProjectionObservationPhaseV1, GoalProjectionTurnRelationV1,
    latest_goal_projection_for_queue, run_goal_lineage_doctor,
};
use agent_harness_core::progress::{
    AgentProgressContext, AgentProgressDeliveryPlanOptions, AgentProgressDeliveryRecordOptions,
    AgentProgressDeliveryStatus, AgentProgressEvent, AgentProgressKind, AgentProgressStatus,
    append_agent_progress_event, plan_agent_progress_delivery, record_agent_progress_delivery,
};
use agent_harness_core::runtime_pipeline::{
    FinalOutboxDispositionV1, RuntimeRunOnceOptions, RuntimeRunOnceStatus,
    SourceFinalExpectationV1, run_runtime_queue_once,
};
use agent_harness_core::{
    AgentSource, ChannelReceiveOptions, ChannelReceiveStatus, ChannelStateLane,
    PromptAssemblyOptions, RuntimeExecutionReceiptStatus, RuntimeQueuePrepareOptions,
    ScopedStopOptions, ScopedStopTarget, build_source_skill_index, prepare_runtime_queue_item,
    receive_channel_message, record_scoped_stop, release_runtime_queue_lease,
};
use serde_json::{Value, json};

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

fn pending_queue(harness_home: &Path, queue_id: &str) -> Value {
    fs::read_to_string(harness_home.join("state/runtime-queue/pending.jsonl"))
        .unwrap()
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find(|row| row["queueId"] == queue_id)
        .unwrap_or_else(|| panic!("pending queue {queue_id} was not durable"))
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
