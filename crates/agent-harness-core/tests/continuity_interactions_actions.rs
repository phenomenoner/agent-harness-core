use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use agent_harness_core::{
    AgentProgressContext, AgentProgressDeliveryPlanOptions, AgentProgressEvent, AgentProgressKind,
    AgentProgressLifecycle, AgentProgressStatus, AgentSource, ChannelApprovalDecisionV1,
    ChannelReceiveOptions, ChannelReceiveStatus, ChannelStateLane, ConnectorApprovalPolicyV1,
    ExternalEffectAdmissionV1, ExternalEffectApprovalDecisionV1,
    ExternalEffectExpiryReconcileRequestV1, ExternalEffectRequestContextV1, ExternalEffectStateV1,
    McpElicitationDescriptorV1, PromptAssemblyOptions, RuntimeExecutionReceiptStatus,
    RuntimeQueuePrepareOptions, append_agent_progress_event, begin_external_effect_request,
    build_source_skill_index, ensure_external_effect_continuation,
    external_effect_approval_authority_digest, external_effect_source_session_key_digest,
    external_effect_transition_file, load_external_effect_intent, plan_agent_progress_delivery,
    prepare_runtime_queue_item, public_approval_action_id, read_channel_session_state_v2,
    receive_channel_message, reconcile_expired_external_effect_approvals,
    resolve_external_effect_approval, resolve_external_effect_public_channel_action,
    settle_expired_external_effect_approval,
};
use serde::Serialize;
use serde_json::{Value, json};

const PLATFORM: &str = "telegram";
const ACCOUNT: &str = "integration-account";
const CHANNEL: &str = "integration-channel";
const USER: &str = "integration-user";
const AGENT: &str = "main";

#[test]
fn waiting_approval_parent_stopped_across_restart_is_terminal_exactly_once() {
    let fixture = Fixture::new("waiting-approval-stop");
    let parent = fixture.receive("start protected work", "event-parent", 1_000);
    let queue_id = parent.queue_id.clone().expect("parent queue ID");
    fixture.prepare(&queue_id, RuntimeExecutionReceiptStatus::Prepared);
    let effect = fixture.park_for_approval(&queue_id, &parent.session_key);
    fixture.append_waiting_progress(&queue_id, &parent.session_key, 1_010);
    fixture.mark_needs_user(&queue_id);
    fixture.prepare(&queue_id, RuntimeExecutionReceiptStatus::NoPendingItem);
    assert!(!fixture.interactive_lease_contains(&queue_id));

    let stop = fixture.receive("/stop stop protected work", "event-stop", 2_000);
    assert_eq!(stop.status, ChannelReceiveStatus::CommandApplied);
    assert_eq!(stop.outbound_messages.len(), 1, "one stop outcome notice");
    let transition = stop.command_apply.as_ref().unwrap();
    assert!(transition.receipt.session_transition_id.is_some());
    assert!(transition.receipt.session_transition_phase.is_some());
    assert_eq!(
        load_external_effect_intent(&fixture.harness_home, &effect.effect_id)
            .unwrap()
            .unwrap()
            .state,
        ExternalEffectStateV1::Denied
    );

    fixture.restart_projection();
    let first = fixture.prepare(&queue_id, RuntimeExecutionReceiptStatus::NoPendingItem);
    assert_eq!(
        first.receipt.terminal_control_source.as_deref(),
        Some("scoped-stop")
    );
    let second = fixture.prepare(&queue_id, RuntimeExecutionReceiptStatus::NoPendingItem);
    assert_eq!(
        second.receipt.terminal_control_source.as_deref(),
        Some("scoped-stop")
    );
    assert_eq!(fixture.run_status_count(&queue_id, "suppressed"), 1);
    assert!(!fixture.interactive_lease_contains(&queue_id));

    let progress = fixture.progress_plan(2_100);
    assert!(
        progress
            .pending
            .iter()
            .all(|pending| pending.queue_id != queue_id),
        "terminal control must close the waiting progress surface"
    );
    assert_eq!(stop.receipt.outbound_count, 1);
}

#[test]
fn waiting_approval_parent_new_session_restart_admits_post_commit_message() {
    let fixture = Fixture::new("waiting-approval-new");
    let parent = fixture.receive("start protected work", "event-parent", 1_000);
    let parent_queue_id = parent.queue_id.clone().expect("parent queue ID");
    fixture.prepare(&parent_queue_id, RuntimeExecutionReceiptStatus::Prepared);
    let effect = fixture.park_for_approval(&parent_queue_id, &parent.session_key);
    fixture.append_waiting_progress(&parent_queue_id, &parent.session_key, 1_010);
    fixture.mark_needs_user(&parent_queue_id);
    fixture.prepare(
        &parent_queue_id,
        RuntimeExecutionReceiptStatus::NoPendingItem,
    );

    let boundary = fixture.receive("/new post-boundary work", "event-new", 2_000);
    assert_eq!(boundary.status, ChannelReceiveStatus::CommandApplied);
    let new_session = boundary
        .command_apply
        .as_ref()
        .and_then(|apply| apply.state.as_ref())
        .expect("committed new-session state")
        .active_session_key
        .clone();
    assert_ne!(new_session, parent.session_key);
    assert_eq!(
        boundary.outbound_messages.len(),
        1,
        "one new-session notice"
    );
    assert_eq!(
        load_external_effect_intent(&fixture.harness_home, &effect.effect_id)
            .unwrap()
            .unwrap()
            .state,
        ExternalEffectStateV1::Denied
    );

    fixture.restart_projection();
    fixture.prepare(
        &parent_queue_id,
        RuntimeExecutionReceiptStatus::NoPendingItem,
    );
    fixture.prepare(
        &parent_queue_id,
        RuntimeExecutionReceiptStatus::NoPendingItem,
    );
    assert_eq!(fixture.run_status_count(&parent_queue_id, "suppressed"), 1);
    assert!(!fixture.interactive_lease_contains(&parent_queue_id));

    let post_commit = fixture.receive("ordinary work after commit", "event-after-new", 2_100);
    assert_eq!(post_commit.status, ChannelReceiveStatus::AgentTurnQueued);
    assert_eq!(post_commit.session_key, new_session);
    let post_queue_id = post_commit.queue_id.expect("post-commit queue ID");
    let prepared = fixture.prepare(&post_queue_id, RuntimeExecutionReceiptStatus::Prepared);
    assert_eq!(
        prepared.receipt.queue_id.as_deref(),
        Some(post_queue_id.as_str())
    );

    let lane = fixture.lane();
    let state = read_channel_session_state_v2(&fixture.harness_home, &lane)
        .unwrap()
        .unwrap();
    assert_eq!(state.active_session_key, new_session);
    assert_eq!(fixture.progress_supersede_count(&parent.session_key), 1);
}

#[test]
fn approved_before_new_session_restart_is_voided_once_without_continuation() {
    let fixture = Fixture::new("approved-before-new");
    let parent = fixture.receive("start protected work", "event-parent", 1_000);
    let queue_id = parent.queue_id.clone().expect("parent queue ID");
    let (intent, raw_token) = fixture.park_for_approval_with_token(&queue_id, &parent.session_key);
    let approved = resolve_external_effect_approval(
        &fixture.harness_home,
        &raw_token,
        &fixture.lane().exact_lane_digest(),
        ExternalEffectApprovalDecisionV1::Approve,
    )
    .unwrap();
    assert_eq!(approved.state, ExternalEffectStateV1::Approved);
    assert_eq!(fixture.continuation_count(&intent.effect_id), 0);

    let boundary = fixture.receive("/new replace approved work", "event-new", 2_000);
    assert_eq!(boundary.status, ChannelReceiveStatus::CommandApplied);
    fixture.restart_projection();
    let terminal = fixture.prepare(&queue_id, RuntimeExecutionReceiptStatus::NoPendingItem);
    assert_eq!(
        terminal.receipt.terminal_control_source.as_deref(),
        Some("scoped-stop")
    );
    fixture.prepare(&queue_id, RuntimeExecutionReceiptStatus::NoPendingItem);

    let durable = load_external_effect_intent(&fixture.harness_home, &intent.effect_id)
        .unwrap()
        .unwrap();
    assert_eq!(durable.state, ExternalEffectStateV1::Denied);
    assert!(ensure_external_effect_continuation(&fixture.harness_home, &durable).is_err());
    assert!(
        resolve_external_effect_approval(
            &fixture.harness_home,
            &raw_token,
            &fixture.lane().exact_lane_digest(),
            ExternalEffectApprovalDecisionV1::Approve,
        )
        .is_err(),
        "the fenced capability cannot be consumed after restart"
    );
    assert_eq!(fixture.continuation_count(&intent.effect_id), 0);
    assert_eq!(
        fixture.effect_transition_count(&intent.effect_id, "approved"),
        1
    );
    assert_eq!(
        fixture.effect_transition_count(&intent.effect_id, "denied"),
        1
    );
    assert_eq!(fixture.run_status_count(&queue_id, "suppressed"), 1);
    let submitted = fixture.effect_transition_count(&intent.effect_id, "submitted");
    let confirmed = fixture.effect_transition_count(&intent.effect_id, "confirmed");
    let explicit_void = fixture.effect_transition_count(&intent.effect_id, "denied");
    assert_eq!(
        submitted + confirmed,
        0,
        "no connector mutation crossed the fence"
    );
    assert_eq!(
        explicit_void, 1,
        "the approved decision was explicitly voided once"
    );
}

#[test]
fn expiry_reconciler_and_native_callback_race_consumes_decision_once_after_restart() {
    let fixture = Arc::new(Fixture::new("expiry-native-race"));
    let parent = fixture.receive("start protected work", "event-parent", 1_000);
    let queue_id = parent.queue_id.clone().expect("parent queue ID");
    fixture.prepare(&queue_id, RuntimeExecutionReceiptStatus::Prepared);
    let effect = fixture.park_for_approval(&queue_id, &parent.session_key);
    fixture.mark_needs_user(&queue_id);
    fixture.prepare(&queue_id, RuntimeExecutionReceiptStatus::NoPendingItem);
    let approve_action = public_approval_action_id(
        &effect.effect_id,
        effect.approval_generation,
        ChannelApprovalDecisionV1::Approve,
    )
    .unwrap();
    fixture.restart_projection();

    let barrier = Arc::new(Barrier::new(3));
    let expiry_fixture = Arc::clone(&fixture);
    let expiry_barrier = Arc::clone(&barrier);
    let expiry = thread::spawn(move || {
        expiry_barrier.wait();
        reconcile_expired_external_effect_approvals(
            &expiry_fixture.harness_home,
            &ExternalEffectExpiryReconcileRequestV1 {
                now_ms: i64::MAX - 1,
                max_rows: 8,
                after_effect_id: None,
            },
        )
    });
    let callback_fixture = Arc::clone(&fixture);
    let callback_barrier = Arc::clone(&barrier);
    let callback_session = parent.session_key.clone();
    let callback = thread::spawn(move || {
        callback_barrier.wait();
        resolve_external_effect_public_channel_action(
            &callback_fixture.harness_home,
            "provider-callback-event",
            Some("provider-message".to_string()),
            &approve_action,
            &callback_fixture.lane().exact_lane_digest(),
            &external_effect_source_session_key_digest(&callback_session).unwrap(),
            Some(ChannelApprovalDecisionV1::Approve),
        )
    });
    barrier.wait();
    let expiry = expiry.join().unwrap().unwrap();
    let callback = callback.join().unwrap();

    let durable = load_external_effect_intent(&fixture.harness_home, &effect.effect_id)
        .unwrap()
        .unwrap();
    match durable.state {
        ExternalEffectStateV1::Denied => {
            assert!(
                callback.is_err(),
                "expired capability must reject the callback loser"
            );
            let resolution = expiry
                .resolutions
                .iter()
                .find(|resolution| resolution.effect_id == effect.effect_id)
                .expect("expiry winner resolution");
            let first = settle_expired_external_effect_approval(
                &fixture.harness_home,
                resolution,
                i64::MAX - 1,
            )
            .unwrap();
            let replay = settle_expired_external_effect_approval(
                &fixture.harness_home,
                resolution,
                i64::MAX - 1,
            )
            .unwrap();
            assert!(first.notice_appended);
            assert!(first.queue_terminal_receipt_appended);
            assert!(first.progress_terminal_appended);
            assert!(!replay.notice_appended);
            assert!(!replay.queue_terminal_receipt_appended);
            assert!(!replay.progress_terminal_appended);
            assert_eq!(fixture.outbox_source_count(&queue_id), 1);
        }
        ExternalEffectStateV1::Approved => {
            let approved = callback.expect("native callback winner");
            assert_eq!(approved.state, ExternalEffectStateV1::Approved);
            assert!(
                expiry
                    .resolutions
                    .iter()
                    .all(|resolution| resolution.effect_id != effect.effect_id),
                "expiry loser must not fabricate a terminal resolution"
            );
            let first = ensure_external_effect_continuation(&fixture.harness_home, &approved)
                .expect("approved callback schedules continuation");
            let replay = ensure_external_effect_continuation(&fixture.harness_home, &approved)
                .expect("approved callback continuation replay");
            assert!(first.requeued);
            assert!(!replay.requeued);
            assert_eq!(first.child_queue_id, replay.child_queue_id);
            assert_eq!(fixture.continuation_count(&effect.effect_id), 1);
        }
        state => panic!("race produced non-resolution state {state:?}"),
    }

    let terminal_decisions = fixture.effect_transition_count(&effect.effect_id, "approved")
        + fixture.effect_transition_count(&effect.effect_id, "denied");
    assert_eq!(terminal_decisions, 1, "consume-once decision ledger");
}

struct Fixture {
    root: PathBuf,
    source: AgentSource,
    harness_home: PathBuf,
}

impl Fixture {
    fn new(name: &str) -> Self {
        let root = temp_root(name);
        let source_home = root.join("source");
        let workspace = source_home.join("workspace");
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(source_home.join("agents").join(AGENT).join("sessions")).unwrap();
        fs::write(workspace.join("AGENTS.md"), "# Integration agent\n").unwrap();
        fs::write(
            source_home.join("openclaw.json"),
            json!({
                "agents": {
                    "defaults": { "provider": "openai", "model": "codex" },
                    "list": [{ "id": AGENT, "model": "gpt-5", "enabled": true }]
                },
                "models": { "providers": { "openai": { "apiKey": "test-only" } } }
            })
            .to_string(),
        )
        .unwrap();
        fs::write(
            source_home
                .join("agents")
                .join(AGENT)
                .join("sessions")
                .join("sessions.json"),
            "{}",
        )
        .unwrap();
        let source = AgentSource::with_workspace(source_home, workspace);
        Self {
            harness_home: root.join("isolated-harness-home"),
            root,
            source,
        }
    }

    fn lane(&self) -> ChannelStateLane {
        ChannelStateLane::new(PLATFORM, Some(ACCOUNT), CHANNEL, USER, AGENT).unwrap()
    }

    fn receive(
        &self,
        message: &str,
        event_id: &str,
        now_ms: i64,
    ) -> agent_harness_core::ChannelReceiveReport {
        receive_channel_message(ChannelReceiveOptions {
            source: self.source.clone(),
            runtime_workspace: None,
            harness_home: self.harness_home.clone(),
            skill_index: build_source_skill_index(&self.source).unwrap(),
            platform: PLATFORM.to_string(),
            account_id: Some(ACCOUNT.to_string()),
            channel_id: CHANNEL.to_string(),
            user_id: USER.to_string(),
            agent_id: Some(AGENT.to_string()),
            session_key: None,
            message: message.to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: Some("message".to_string()),
            inbound_event_id: Some(event_id.to_string()),
            skill_limit: 3,
            now_ms,
        })
        .unwrap()
    }

    fn prepare(
        &self,
        queue_id: &str,
        expected: RuntimeExecutionReceiptStatus,
    ) -> agent_harness_core::RuntimeQueuePrepareReport {
        let report = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: self.harness_home.clone(),
            queue_id: Some(queue_id.to_string()),
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();
        assert_eq!(report.receipt.status, expected, "{}", report.receipt.reason);
        report
    }

    fn park_for_approval(
        &self,
        queue_id: &str,
        session_key: &str,
    ) -> agent_harness_core::ExternalEffectIntentV1 {
        self.park_for_approval_with_token(queue_id, session_key).0
    }

    fn park_for_approval_with_token(
        &self,
        queue_id: &str,
        session_key: &str,
    ) -> (agent_harness_core::ExternalEffectIntentV1, String) {
        let exact_lane_digest = self.lane().exact_lane_digest();
        let source_session_key_digest =
            external_effect_source_session_key_digest(session_key).unwrap();
        let logical_lineage_id = format!("lineage:{queue_id}");
        let approval_authority_digest = external_effect_approval_authority_digest(
            &exact_lane_digest,
            &source_session_key_digest,
            &logical_lineage_id,
            queue_id,
        )
        .unwrap();
        let admission = begin_external_effect_request(
            &self.harness_home,
            &ExternalEffectRequestContextV1 {
                exact_lane_digest,
                logical_lineage_id,
                source_queue_id: queue_id.to_string(),
                source_session_key_digest,
                approval_authority_digest,
            },
            &McpElicitationDescriptorV1 {
                connector: "github".to_string(),
                action: "create_issue".to_string(),
                params_digest: format!("sha256:{}", "4".repeat(64)),
                action_summary: "create one protected issue".to_string(),
                mode: "form".to_string(),
            },
            &ConnectorApprovalPolicyV1::default(),
        )
        .unwrap();
        match admission {
            ExternalEffectAdmissionV1::NeedsUser { intent, token } => (intent, token),
            other => panic!("unexpected approval admission {other:?}"),
        }
    }

    fn append_waiting_progress(&self, queue_id: &str, session_key: &str, now_ms: i64) {
        append_agent_progress_event(
            &self.harness_home,
            &AgentProgressEvent::new(
                &AgentProgressContext {
                    queue_id: queue_id.to_string(),
                    agent_id: Some(AGENT.to_string()),
                    account_id: Some(ACCOUNT.to_string()),
                    thread_id: None,
                    session_key: session_key.to_string(),
                    platform: PLATFORM.to_string(),
                    channel_id: CHANNEL.to_string(),
                    user_id: USER.to_string(),
                },
                AgentProgressKind::Runtime,
                "approval",
                "waiting for protected connector approval",
                AgentProgressStatus::Progress,
                now_ms,
            )
            .lifecycle(AgentProgressLifecycle::WaitingForApproval),
        )
        .unwrap();
    }

    fn mark_needs_user(&self, queue_id: &str) {
        self.append_jsonl(
            &self
                .harness_home
                .join("state")
                .join("runtime-queue")
                .join("run-once-receipts.jsonl"),
            &json!({
                "schema": "agent-harness.runtime-run-once.v1",
                "queueId": queue_id,
                "status": "needs-user",
                "runtimeClass": "interactive",
                "origin": "channel",
                "reason": "protected connector approval required"
            }),
        );
    }

    fn append_jsonl(&self, path: &Path, value: &impl Serialize) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap();
        serde_json::to_writer(&mut file, value).unwrap();
        file.write_all(b"\n").unwrap();
        file.sync_all().unwrap();
    }

    fn restart_projection(&self) {
        let index = self
            .harness_home
            .join("state")
            .join("runtime-queue")
            .join("queue-state-index.json");
        match fs::remove_file(index) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => panic!("remove derived queue projection: {error}"),
        }
    }

    fn progress_plan(&self, now_ms: i64) -> agent_harness_core::AgentProgressDeliveryPlanReport {
        plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: self.harness_home.clone(),
            platform: Some(PLATFORM.to_string()),
            now_ms,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap()
    }

    fn run_status_count(&self, queue_id: &str, status: &str) -> usize {
        self.jsonl_values(
            &self
                .harness_home
                .join("state")
                .join("runtime-queue")
                .join("run-once-receipts.jsonl"),
        )
        .iter()
        .filter(|value| value["queueId"] == queue_id && value["status"] == status)
        .count()
    }

    fn effect_transition_count(&self, effect_id: &str, state: &str) -> usize {
        self.jsonl_values(&external_effect_transition_file(&self.harness_home))
            .iter()
            .filter(|value| value["effectId"] == effect_id && value["toState"] == state)
            .count()
    }

    fn continuation_count(&self, effect_id: &str) -> usize {
        self.jsonl_values(
            &self
                .harness_home
                .join("state")
                .join("runtime-queue")
                .join("pending.jsonl"),
        )
        .iter()
        .filter(|value| value["continuationIntentKey"] == effect_id)
        .count()
    }

    fn outbox_source_count(&self, queue_id: &str) -> usize {
        self.jsonl_values(
            &self
                .harness_home
                .join("state")
                .join("channels")
                .join("outbox.jsonl"),
        )
        .iter()
        .filter(|value| value["sourceQueueId"] == queue_id)
        .count()
    }

    fn interactive_lease_contains(&self, queue_id: &str) -> bool {
        let candidates = [
            self.harness_home
                .join("state")
                .join("runtime-queue")
                .join("runtime-leases.json"),
            self.harness_home
                .join("state")
                .join("runtime-queue")
                .join("runtime-leases-interactive.json"),
        ];
        candidates.iter().any(|path| {
            fs::read_to_string(path)
                .ok()
                .is_some_and(|text| text.contains(queue_id))
        })
    }

    fn progress_supersede_count(&self, session_key: &str) -> usize {
        self.jsonl_values(
            &agent_harness_core::agent_progress_session_supersede_receipts_file(&self.harness_home),
        )
        .iter()
        .filter(|value| value["sessionKey"] == session_key)
        .count()
    }

    fn jsonl_values(&self, path: &Path) -> Vec<Value> {
        fs::read_to_string(path)
            .unwrap_or_default()
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn temp_root(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "agent-harness-continuity-interactions-{name}-{}-{nanos}",
        std::process::id()
    ))
}
