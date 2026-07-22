use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::thread;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::channel_state::ChannelStateLane;
use crate::codex_runtime::{
    CodexContextRecoveryReceipt, CodexShellExecutionFailureKindV1, CodexThreadHealthStatus,
    tool_timeout_summary,
};
use crate::context_rollover::{
    CompletedTurnWorkingSetSnapshotV2Options, VirtualSessionTerminalV2Options,
    mark_virtual_session_terminal_for_lane, record_completed_turn_working_set_snapshot_for_lane,
};
use crate::goal_budget::{
    GoalCampaignBudgetInput, current_goal_campaign_timeouts, evaluate_goal_campaign_budget,
    load_goal_campaign_policy,
};
use crate::goal_continuation::{
    GoalAutonomyActivation, GoalAutonomyMode, acknowledge_goal_continuation_after_lease,
    commit_goal_continuation_intent, ensure_goal_continuation_enqueued,
    load_goal_autonomy_activation, reconcile_goal_continuation_intents,
};
use crate::goal_lineage::{
    GoalLineageDisposition, GoalLineageDoctorOptions, GoalLineageDoctorStatus, GoalProjectionHint,
    GoalProjectionTurnRelationV1, latest_goal_projection_for_queue, run_goal_lineage_doctor,
};
use crate::goal_transition::{
    GoalTransitionAuthority, GoalTransitionDecision, GoalTransitionEventKind, GoalTransitionInput,
    GoalTransitionReceiptV1, GoalTransitionRelation, GoalTransitionSurface,
    evaluate_goal_transition, record_goal_transition,
};
use crate::rich_presentation::{
    RichMessagePresentation, RichPresentationAction, RichPresentationActionKind,
    rich_presentation_from_plain_final_with_attachment_count,
};
use crate::runtime_worker::{
    QueueTerminalControl, record_terminal_control_suppression, refresh_runtime_queue_state_index,
    resolve_queue_terminal_control, runtime_queue_prior_failure_count_from_index,
    runtime_queue_status_count_from_index,
};
use crate::{
    AgentProgressContext, AgentProgressEvent, AgentProgressKind, AgentProgressStatus,
    AssistantNarrationConfig, AssistantNarrationMode, ChannelApprovalPromptV1,
    ChannelDeliveryIntent, ChannelDeliveryIntentKind, ChannelOutboundAttachment,
    ChannelOutboundAttachmentKind, ChannelOutboundMessage, ChannelOutboundMessageKind,
    CodexRuntimePlan, CodexRuntimePlanOptions, CodexRuntimePlanReport, CodexRuntimeReceiptStatus,
    CodexRuntimeRunOptions, CodexRuntimeRunReport, CodexRuntimeRunStatus, ContextRolloverMode,
    ContextRolloverPreparedRequeueReport, ContextRolloverRequeuePreparedOptions, HarnessLogEvent,
    HarnessLogLevel, InboundMediaArtifact, MediaDeliveryVerdict, MemoryLifecycleTurnOptions,
    PromptAssemblyOptions, ResponseToneConfig, ResponseToneContext, RuntimeContinuationMetadata,
    RuntimeExecutionReceiptStatus, RuntimeQueuePrepareOptions, RuntimeQueuePrepareReport,
    RuntimeQueuePreparedItem, SelfImprovementNotificationTarget, SelfImprovementReviewHookOptions,
    SkillEpisodeRuntimeCaptureOptions, SkillOutcomeStatusV1, VirtualSessionTerminalOptions,
    advance_learning_nudge_counters, append_agent_progress_event, append_channel_outbox_message,
    append_harness_log, apply_response_tone, attachment_kind_from_path,
    capture_skill_episode_runtime_evidence, continuation_session_key, current_log_time_ms,
    evaluate_outbound_media_path, inspect_runtime_backoff_policy, is_deliverable_media_path,
    load_assistant_narration_config, load_context_rollover_config, load_harness_media_config,
    load_response_tone_config, mark_cron_run_runtime_status_by_queue_id,
    mark_virtual_session_terminal, plan_codex_runtime, prepare_runtime_queue_item,
    read_channel_session_state_v2, record_memory_lifecycle_turn,
    record_skill_usage_from_prompt_bundle, release_runtime_queue_lease,
    requeue_prepared_context_rollover_if_no_parent_siblings,
    resolve_inbound_media_artifact_reference, root_working_session_key, run_codex_runtime,
    run_self_improvement_review_hook, write_json_atomic, write_media_policy_receipt,
    write_runtime_queue_quarantine_marker,
};

const RUNTIME_RUN_ONCE_SCHEMA: &str = "agent-harness.runtime-run-once.v1";
const RUNTIME_DEAD_LETTER_SCHEMA: &str = "agent-harness.runtime-dead-letter.v1";
const FINAL_OUTBOX_RECEIPT_SCHEMA: &str = "agent-harness.runtime-final-outbox.v1";
const VIRTUAL_SESSION_AUTHORITY_SCHEMA: &str = "agent-harness.virtual-session-authority.v1";
const GOAL_TERMINAL_OUTBOX_SCHEMA: &str = "agent-harness.goal-terminal-outbox.v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum GoalTerminalOutboxState {
    Committed,
    Appended,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GoalTerminalOutboxReceiptV1 {
    schema: String,
    terminal_key: String,
    delivery_id: String,
    campaign_family_id: String,
    lane_digest: String,
    virtual_session_id: String,
    decision: GoalTransitionDecision,
    surface: GoalTransitionSurface,
    source_queue_id: Option<String>,
    message: ChannelOutboundMessage,
    state: GoalTerminalOutboxState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    outbox_file: Option<PathBuf>,
    recorded_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct VirtualSessionAuthorityReceiptV1 {
    schema: &'static str,
    queue_id: Option<String>,
    virtual_session_id: String,
    working_session_key: String,
    lane_digest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    backend_context_generation: Option<String>,
    working_set_file: PathBuf,
    status: String,
    reason: String,
    updated_at_ms: i64,
}
#[derive(Debug, Clone)]
struct RuntimeContinuationCandidate {
    queue_id: String,
    session_key: String,
    runtime_class: String,
    origin: String,
    platform: Option<String>,
    account_id: Option<String>,
    channel_id: Option<String>,
    user_id: Option<String>,
    agent_id: Option<String>,
    inbound_media_artifacts: Vec<InboundMediaArtifact>,
    continuation: RuntimeContinuationMetadata,
}

impl RuntimeContinuationCandidate {
    fn from_prepared_item(item: &RuntimeQueuePreparedItem) -> Self {
        Self {
            queue_id: item.queue_id.clone(),
            session_key: item.session_key.clone(),
            runtime_class: item.runtime_class.clone(),
            origin: item.origin.clone(),
            platform: Some(item.platform.clone()),
            account_id: item.account_id.clone(),
            channel_id: Some(item.channel_id.clone()),
            user_id: Some(item.user_id.clone()),
            agent_id: Some(item.agent_id.clone()),
            inbound_media_artifacts: item.inbound_media_artifacts.clone(),
            continuation: item.continuation.clone(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PendingContinuationCandidateRecord {
    queue_id: String,
    session_key: String,
    #[serde(default = "default_interactive_runtime_class_text")]
    runtime_class: String,
    #[serde(default = "default_channel_origin_text")]
    origin: String,
    #[serde(default)]
    platform: Option<String>,
    #[serde(default)]
    account_id: Option<String>,
    #[serde(default)]
    channel_id: Option<String>,
    #[serde(default)]
    user_id: Option<String>,
    #[serde(default)]
    agent_id: Option<String>,
    #[serde(default)]
    inbound_media_artifacts: Vec<InboundMediaArtifact>,
    #[serde(default, flatten)]
    continuation: RuntimeContinuationMetadata,
}

impl From<PendingContinuationCandidateRecord> for RuntimeContinuationCandidate {
    fn from(record: PendingContinuationCandidateRecord) -> Self {
        Self {
            queue_id: record.queue_id,
            session_key: record.session_key,
            runtime_class: record.runtime_class,
            origin: record.origin,
            platform: record.platform,
            account_id: record.account_id,
            channel_id: record.channel_id,
            user_id: record.user_id,
            agent_id: record.agent_id,
            inbound_media_artifacts: record.inbound_media_artifacts,
            continuation: record.continuation,
        }
    }
}

fn default_interactive_runtime_class_text() -> String {
    "interactive".to_string()
}

fn default_channel_origin_text() -> String {
    "channel".to_string()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeRunOnceOptions {
    pub harness_home: PathBuf,
    pub queue_id: Option<String>,
    pub codex_executable: Option<PathBuf>,
    pub timeout_ms: u64,
    pub idle_timeout_ms: u64,
    pub prompt_options: PromptAssemblyOptions,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeRunOnceReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub report_file: PathBuf,
    pub receipts_file: PathBuf,
    pub receipt: RuntimeRunOnceReceipt,
    pub prepare: Option<RuntimeQueuePrepareReport>,
    pub plan: Option<CodexRuntimePlanReport>,
    pub run: Option<CodexRuntimeRunReport>,
    pub outbox_file: Option<PathBuf>,
    pub outbound_message: Option<ChannelOutboundMessage>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeRunOnceReceipt {
    pub queue_id: Option<String>,
    pub status: RuntimeRunOnceStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime_class: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cron_run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scheduled_for_ms: Option<i64>,
    pub execution_dir: Option<PathBuf>,
    pub transcript_file: Option<PathBuf>,
    pub outbox_file: Option<PathBuf>,
    #[serde(default, flatten)]
    pub continuation: RuntimeContinuationMetadata,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub child_queue_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub child_session_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub terminal_disposition: Option<crate::RuntimeTerminalDispositionV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub continuation_link: Option<crate::RuntimeContinuationLinkV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol_failure: Option<crate::CodexProtocolFailureV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mutation_evidence: Option<crate::RuntimeMutationEvidenceClass>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_schedule: Option<crate::RuntimeRetryScheduleV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_drain_evaluation: Option<crate::TaskDrainEvaluationV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external_effect: Option<crate::ExternalEffectIntentV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub terminal_control_matched: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub terminal_control_source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suppressed_run_once_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prepared_execution_terminalization_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_final_expectation: Option<SourceFinalExpectationV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_closure_kind: Option<RuntimeSourceClosureKindV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_closure_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub final_outbox_disposition: Option<FinalOutboxDispositionV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canonical_source_queue_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub final_delivery_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_final_lane_digest: Option<String>,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SourceFinalExpectationV1 {
    Required,
    ExplicitNonDelivery,
    NotApplicable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RuntimeSourceClosureKindV1 {
    CommittedHandoff,
    TerminalGoalSurface,
    ParkedObserve,
    ParkedPolicyDenied,
    OrdinaryFinal,
    SuppressedExactOwnerMismatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RuntimeSourceClosureDecision {
    CommittedHandoff,
    TerminalGoalSurface,
    ParkedObserve,
    ParkedPolicyDenied,
    OrdinaryFinal,
    SuppressedExactOwnerMismatch,
}

impl RuntimeSourceClosureDecision {
    fn kind(self) -> RuntimeSourceClosureKindV1 {
        match self {
            Self::CommittedHandoff => RuntimeSourceClosureKindV1::CommittedHandoff,
            Self::TerminalGoalSurface => RuntimeSourceClosureKindV1::TerminalGoalSurface,
            Self::ParkedObserve => RuntimeSourceClosureKindV1::ParkedObserve,
            Self::ParkedPolicyDenied => RuntimeSourceClosureKindV1::ParkedPolicyDenied,
            Self::OrdinaryFinal => RuntimeSourceClosureKindV1::OrdinaryFinal,
            Self::SuppressedExactOwnerMismatch => {
                RuntimeSourceClosureKindV1::SuppressedExactOwnerMismatch
            }
        }
    }

    fn terminal_disposition(self) -> crate::RuntimeTerminalDispositionV1 {
        match self {
            Self::CommittedHandoff => crate::RuntimeTerminalDispositionV1::ContinuationHandoff,
            Self::ParkedObserve | Self::ParkedPolicyDenied => {
                crate::RuntimeTerminalDispositionV1::NeedsUser
            }
            Self::SuppressedExactOwnerMismatch => {
                crate::RuntimeTerminalDispositionV1::TerminalSuppression
            }
            Self::TerminalGoalSurface | Self::OrdinaryFinal => {
                crate::RuntimeTerminalDispositionV1::LogicalSuccess
            }
        }
    }

    fn source_final_expectation(self) -> SourceFinalExpectationV1 {
        match self {
            Self::CommittedHandoff => SourceFinalExpectationV1::NotApplicable,
            Self::SuppressedExactOwnerMismatch => SourceFinalExpectationV1::ExplicitNonDelivery,
            Self::TerminalGoalSurface
            | Self::ParkedObserve
            | Self::ParkedPolicyDenied
            | Self::OrdinaryFinal => SourceFinalExpectationV1::Required,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum FinalOutboxDispositionV1 {
    Appended,
    AlreadyPresent,
    ReusedCanonicalCampaign,
    ExplicitNonDelivery,
    NotApplicable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FinalOutboxOutcome {
    outbox_file: PathBuf,
    disposition: FinalOutboxDispositionV1,
    canonical_source_queue_id: Option<String>,
    delivery_id: Option<String>,
}

impl FinalOutboxOutcome {
    fn appended(&self) -> bool {
        self.disposition == FinalOutboxDispositionV1::Appended
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum RuntimeRunOnceStatus {
    Completed,
    NeedsUser,
    ExternalEffectDenied,
    Suppressed,
    Skipped,
    LeaseBusy,
    NoWork,
    NoPreparedExecution,
    NoRuntimePlan,
    PreflightBlocked,
    AuthDeferred,
    SpawnFailed,
    ProtocolError,
    ContextExhausted,
    Timeout,
    RetryPending,
    DeadLetter,
    FailedTerminal,
    Canceled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExternalEffectExpirySettlementReportV1 {
    pub effect_id: String,
    pub source_queue_id: String,
    pub resolution_id: String,
    pub notice_id: String,
    pub notice_delivery_id: Option<String>,
    pub notice_appended: bool,
    pub queue_terminal_receipt_appended: bool,
    pub progress_terminal_appended: bool,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct ExternalEffectExpiryQueueTerminalReceiptV1 {
    event_key: String,
    queue_id: Option<String>,
    status: RuntimeRunOnceStatus,
    runtime_class: Option<String>,
    origin: Option<String>,
    reason: String,
    effect_id: String,
    approval_generation: u64,
    resolution_id: String,
    notice_id: String,
    final_outbox_disposition: FinalOutboxDispositionV1,
}

/// Completes the user-visible side of one already-durable approval expiry.
/// The deterministic notice delivery and progress identities make restart
/// replay safe; the run-once terminal receipt is appended once by event key.
pub fn settle_expired_external_effect_approval(
    harness_home: &Path,
    resolution: &crate::ExternalEffectExpiryResolutionV1,
    now_ms: i64,
) -> io::Result<ExternalEffectExpirySettlementReportV1> {
    let intent = crate::load_external_effect_intent(harness_home, &resolution.effect_id)?
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "expired effect was not found"))?;
    if intent.state != crate::ExternalEffectStateV1::Denied
        || intent.approval_generation != resolution.approval_generation
        || intent.expiry_resolution_id.as_deref() != Some(resolution.resolution_id.as_str())
        || intent.expiry_notice_id.as_deref() != Some(resolution.notice_id.as_str())
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "expired effect settlement authority does not match protected terminal state",
        ));
    }
    let mut warnings = Vec::new();
    let context = find_queue_channel_context(harness_home, &intent.source_queue_id, &mut warnings)?;
    let mut notice_delivery_id = None;
    let mut notice_appended = false;
    let mut progress_terminal_appended = false;
    let mut final_outbox_disposition = FinalOutboxDispositionV1::ExplicitNonDelivery;
    if let Some(context) = context.as_ref() {
        let suffix = resolution
            .notice_id
            .strip_prefix("ahen1_")
            .filter(|value| value.len() == 64)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid expiry notice identity")
            })?;
        let delivery_id = format!("delivery:v2:{}", &suffix[..32]);
        let mut message = ChannelOutboundMessage {
            platform: context.platform.clone(),
            account_id: context.account_id.clone(),
            channel_id: context.channel_id.clone(),
            user_id: context.user_id.clone(),
            session_key: context.session_key.clone(),
            delivery_id: Some(delivery_id.clone()),
            kind: ChannelOutboundMessageKind::AgentReply,
            source_queue_id: Some(intent.source_queue_id.clone()),
            source_completion_file: None,
            text: "Connector approval expired. The protected action was denied and will not be retried.".to_string(),
            presentation: None,
            delivery_intent: delivery_intent_from_inbound_context(
                &context.platform,
                &context.channel_id,
                context.inbound_context.as_deref(),
            ),
            attachments: Vec::new(),
        };
        let outbox = append_channel_outbox_message(harness_home, &mut message)?;
        notice_appended = matches!(
            outbox.outcome,
            crate::ChannelOutboxAppendOutcome::Appended
                | crate::ChannelOutboxAppendOutcome::AppendedIndexDeferred
        );
        final_outbox_disposition = if notice_appended {
            FinalOutboxDispositionV1::Appended
        } else {
            FinalOutboxDispositionV1::AlreadyPresent
        };
        if let Some(warning) = outbox.index_warning {
            warnings.push(warning);
        }
        notice_delivery_id = Some(outbox.delivery_id);

        let progress_id = format!("pe2-{}", &suffix[..32]);
        let events_file = crate::agent_progress_events_file(harness_home);
        let progress_already_present = fs::read_to_string(&events_file)
            .ok()
            .is_some_and(|text| text.contains(&format!("\"eventId\":\"{progress_id}\"")));
        if !progress_already_present {
            let progress_context = AgentProgressContext {
                queue_id: intent.source_queue_id.clone(),
                agent_id: Some(context.agent_id.clone()),
                account_id: context.account_id.clone(),
                thread_id: None,
                session_key: context.session_key.clone(),
                platform: context.platform.clone(),
                channel_id: context.channel_id.clone(),
                user_id: context.user_id.clone(),
            };
            let mut event = AgentProgressEvent::new(
                &progress_context,
                AgentProgressKind::Runtime,
                "approval-expired",
                "connector approval expired and the protected action was denied",
                AgentProgressStatus::Failed,
                now_ms,
            )
            .source("external-effect-expiry-reconciler");
            event.event_id = Some(progress_id);
            append_agent_progress_event(harness_home, &event)?;
            progress_terminal_appended = true;
        }
    } else {
        warnings.push(format!(
            "expiry notice for queue {} has no channel context",
            intent.source_queue_id
        ));
    }

    let receipt = ExternalEffectExpiryQueueTerminalReceiptV1 {
        event_key: format!("{}:queue-terminal", resolution.resolution_id),
        queue_id: Some(intent.source_queue_id.clone()),
        status: RuntimeRunOnceStatus::ExternalEffectDenied,
        runtime_class: Some("interactive".to_string()),
        origin: Some("external-effect-expiry-reconciler".to_string()),
        reason: "connector approval expired and was denied".to_string(),
        effect_id: intent.effect_id.clone(),
        approval_generation: intent.approval_generation,
        resolution_id: resolution.resolution_id.clone(),
        notice_id: resolution.notice_id.clone(),
        final_outbox_disposition,
    };
    let receipt_file = harness_home
        .join("state")
        .join("runtime-queue")
        .join("run-once-receipts.jsonl");
    let queue_terminal_receipt_appended =
        crate::append_jsonl_value_once_by_event_key(&receipt_file, &receipt)?;
    if queue_terminal_receipt_appended {
        let queue_dir = harness_home.join("state").join("runtime-queue");
        let _ =
            crate::runtime_worker::refresh_runtime_queue_state_index(&queue_dir, &mut warnings)?;
    }

    Ok(ExternalEffectExpirySettlementReportV1 {
        effect_id: intent.effect_id,
        source_queue_id: intent.source_queue_id,
        resolution_id: resolution.resolution_id.clone(),
        notice_id: resolution.notice_id.clone(),
        notice_delivery_id,
        notice_appended,
        queue_terminal_receipt_appended,
        progress_terminal_appended,
        warnings,
    })
}

impl RuntimeRunOnceStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::NeedsUser => "needs-user",
            Self::ExternalEffectDenied => "external-effect-denied",
            Self::Suppressed => "suppressed",
            Self::Skipped => "skipped",
            Self::LeaseBusy => "lease-busy",
            Self::NoWork => "no-work",
            Self::NoPreparedExecution => "no-prepared-execution",
            Self::NoRuntimePlan => "no-runtime-plan",
            Self::PreflightBlocked => "preflight-blocked",
            Self::AuthDeferred => "auth-deferred",
            Self::SpawnFailed => "spawn-failed",
            Self::ProtocolError => "protocol-error",
            Self::ContextExhausted => "context-exhausted",
            Self::Timeout => "timeout",
            Self::RetryPending => "retry-pending",
            Self::DeadLetter => "dead-letter",
            Self::FailedTerminal => "failed-terminal",
            Self::Canceled => "canceled",
        }
    }
}

fn should_request_ledger_maintenance_after_terminal(
    status: RuntimeRunOnceStatus,
    queue_id: Option<&str>,
) -> bool {
    queue_id.is_some()
        && matches!(
            status,
            RuntimeRunOnceStatus::Completed
                | RuntimeRunOnceStatus::Timeout
                | RuntimeRunOnceStatus::FailedTerminal
                | RuntimeRunOnceStatus::Canceled
                | RuntimeRunOnceStatus::Skipped
                | RuntimeRunOnceStatus::DeadLetter
                | RuntimeRunOnceStatus::Suppressed
        )
}

fn should_record_failed_memory_lifecycle(status: &RuntimeRunOnceStatus) -> bool {
    matches!(
        status,
        RuntimeRunOnceStatus::PreflightBlocked
            | RuntimeRunOnceStatus::SpawnFailed
            | RuntimeRunOnceStatus::ProtocolError
            | RuntimeRunOnceStatus::ContextExhausted
            | RuntimeRunOnceStatus::Timeout
            | RuntimeRunOnceStatus::DeadLetter
            | RuntimeRunOnceStatus::FailedTerminal
            | RuntimeRunOnceStatus::Canceled
    )
}

fn should_run_self_improvement_hook(
    status: RuntimeRunOnceStatus,
    continuation: &RuntimeContinuationMetadata,
    codex_plan: Option<&CodexRuntimePlan>,
) -> bool {
    status == RuntimeRunOnceStatus::Completed
        && !continuation.should_suppress_self_improvement()
        && !codex_plan
            .map(|plan| plan.continuation.should_suppress_self_improvement())
            .unwrap_or(false)
}

fn count_transcript_tool_calls(path: Option<&Path>) -> usize {
    let Some(path) = path else {
        return 0;
    };
    let Ok(text) = fs::read_to_string(path) else {
        return 0;
    };
    text.lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(json_value_mentions_tool_call)
        .count()
}

fn count_prompt_bundle_sections(path: &Path) -> usize {
    let Ok(text) = fs::read_to_string(path) else {
        return 0;
    };
    let Ok(value) = serde_json::from_str::<Value>(&text) else {
        return 0;
    };
    value
        .get("sections")
        .and_then(Value::as_array)
        .map(Vec::len)
        .unwrap_or(0)
}

fn json_value_mentions_tool_call(value: &Value) -> bool {
    if value
        .get("item_type")
        .or_else(|| value.get("itemType"))
        .or_else(|| value.get("type"))
        .and_then(Value::as_str)
        .is_some_and(is_tool_call_type)
    {
        return true;
    }
    if value
        .get("method")
        .and_then(Value::as_str)
        .is_some_and(|method| method.contains("tool") || method == "item/started")
        && value
            .get("item")
            .and_then(|item| item.get("type"))
            .and_then(Value::as_str)
            .is_some_and(is_tool_call_type)
    {
        return true;
    }
    value.get("item").is_some_and(json_value_mentions_tool_call)
}

fn is_tool_call_type(value: &str) -> bool {
    let normalized = value.to_ascii_lowercase();
    normalized.contains("tool")
        || normalized == "commandexecution"
        || normalized == "function_call"
        || normalized == "functioncall"
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct RuntimeDeadLetterReceipt {
    schema: &'static str,
    queue_id: Option<String>,
    status: RuntimeRunOnceStatus,
    execution_dir: Option<PathBuf>,
    transcript_file: Option<PathBuf>,
    outbox_file: Option<PathBuf>,
    reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FinalOutboxReceipt {
    schema: String,
    queue_id: Option<String>,
    completion_file: Option<PathBuf>,
    outbox_file: PathBuf,
    platform: String,
    account_id: Option<String>,
    channel_id: String,
    user_id: String,
    session_key: String,
    kind: ChannelOutboundMessageKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    delivery_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct QueueChannelContext {
    platform: String,
    account_id: Option<String>,
    channel_id: String,
    user_id: String,
    agent_id: String,
    session_key: String,
    inbound_context: Option<String>,
    inbound_media_artifacts: Vec<InboundMediaArtifact>,
}

#[derive(Debug, Clone, Default)]
struct RuntimeRunMetadata {
    runtime_class: Option<String>,
    origin: Option<String>,
    cron_run_id: Option<String>,
    scheduled_for_ms: Option<i64>,
    continuation: RuntimeContinuationMetadata,
}

fn runtime_run_metadata(prepare: &RuntimeQueuePrepareReport) -> RuntimeRunMetadata {
    if let Some(item) = prepare.item.as_ref() {
        return RuntimeRunMetadata {
            runtime_class: Some(item.runtime_class.clone()),
            origin: Some(item.origin.clone()),
            cron_run_id: item.cron_run_id.clone(),
            scheduled_for_ms: item.scheduled_for_ms,
            continuation: item.continuation.clone(),
        };
    }
    RuntimeRunMetadata {
        runtime_class: prepare.receipt.runtime_class.clone(),
        origin: prepare.receipt.origin.clone(),
        cron_run_id: prepare.receipt.cron_run_id.clone(),
        scheduled_for_ms: prepare.receipt.scheduled_for_ms,
        continuation: prepare.receipt.continuation.clone(),
    }
}

pub fn run_runtime_queue_once(options: RuntimeRunOnceOptions) -> io::Result<RuntimeRunOnceReport> {
    let queue_dir = options.harness_home.join("state").join("runtime-queue");
    let report_file = queue_dir.join("run-once-last.json");
    let receipts_file = queue_dir.join("run-once-receipts.jsonl");
    fs::create_dir_all(&queue_dir)?;

    let reconciled_goal_intents =
        reconcile_goal_continuation_intents(&options.harness_home, current_log_time_ms()?)?;
    let prepare = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
        harness_home: options.harness_home.clone(),
        queue_id: options.queue_id.clone(),
        prompt_options: options.prompt_options,
    })?;
    let mut warnings = prepare.warnings.clone();
    if !reconciled_goal_intents.is_empty() {
        warnings.push(format!(
            "reconciled {} committed goal continuation intent(s) before queue selection",
            reconciled_goal_intents.len()
        ));
    }
    let mut channel_context = prepare
        .item
        .as_ref()
        .map(channel_context_from_prepared_item);

    if prepare.receipt.status == RuntimeExecutionReceiptStatus::LeaseBusy {
        let metadata = runtime_run_metadata(&prepare);
        let receipt = RuntimeRunOnceReceipt {
            queue_id: prepare.receipt.queue_id.clone(),
            status: RuntimeRunOnceStatus::LeaseBusy,
            runtime_class: metadata.runtime_class,
            origin: metadata.origin,
            cron_run_id: metadata.cron_run_id,
            scheduled_for_ms: metadata.scheduled_for_ms,
            execution_dir: None,
            transcript_file: None,
            outbox_file: None,
            continuation: metadata.continuation,
            child_queue_id: None,
            child_session_key: None,
            terminal_disposition: None,
            continuation_link: None,
            protocol_failure: None,
            mutation_evidence: None,
            retry_schedule: None,
            task_drain_evaluation: None,
            external_effect: None,
            terminal_control_matched: None,
            terminal_control_source: None,
            suppressed_run_once_reason: None,
            prepared_execution_terminalization_reason: None,
            source_final_expectation: Some(SourceFinalExpectationV1::NotApplicable),
            source_closure_kind: None,
            source_closure_reason: None,
            final_outbox_disposition: Some(FinalOutboxDispositionV1::NotApplicable),
            canonical_source_queue_id: None,
            final_delivery_id: None,
            source_final_lane_digest: None,
            reason: "runtime queue lease lock is busy; retrying later".to_string(),
        };
        append_runtime_run_once_log(
            &options.harness_home,
            HarnessLogLevel::Info,
            "runtime.run-once.lease-busy",
            &receipt,
        )?;
        return write_runtime_run_once_report(
            RuntimeRunOnceReport {
                schema: RUNTIME_RUN_ONCE_SCHEMA,
                harness_home: options.harness_home,
                report_file,
                receipts_file,
                receipt,
                prepare: Some(prepare),
                plan: None,
                run: None,
                outbox_file: None,
                outbound_message: None,
                warnings,
            },
            true,
        );
    }

    if prepare.receipt.status == RuntimeExecutionReceiptStatus::NoPendingItem {
        let metadata = runtime_run_metadata(&prepare);
        let requested_queue = options.queue_id;
        let requested_specific_queue = requested_queue.is_some();
        let suppressed_queue = if prepare.receipt.terminal_control_matched == Some(true) {
            prepare.receipt.queue_id.clone().or(requested_queue)
        } else {
            requested_queue
        };
        let receipt = RuntimeRunOnceReceipt {
            queue_id: suppressed_queue,
            status: if prepare.receipt.terminal_control_matched == Some(true) {
                RuntimeRunOnceStatus::Suppressed
            } else {
                RuntimeRunOnceStatus::NoWork
            },
            runtime_class: metadata.runtime_class,
            origin: metadata.origin,
            cron_run_id: metadata.cron_run_id,
            scheduled_for_ms: metadata.scheduled_for_ms,
            execution_dir: None,
            transcript_file: None,
            outbox_file: None,
            continuation: metadata.continuation,
            child_queue_id: None,
            child_session_key: None,
            terminal_disposition: (prepare.receipt.terminal_control_matched == Some(true))
                .then_some(crate::RuntimeTerminalDispositionV1::TerminalSuppression),
            continuation_link: None,
            protocol_failure: None,
            mutation_evidence: None,
            retry_schedule: None,
            task_drain_evaluation: None,
            external_effect: None,
            terminal_control_matched: prepare.receipt.terminal_control_matched,
            terminal_control_source: prepare.receipt.terminal_control_source.clone(),
            suppressed_run_once_reason: prepare.receipt.suppressed_run_once_reason.clone(),
            prepared_execution_terminalization_reason: None,
            source_final_expectation: Some(SourceFinalExpectationV1::NotApplicable),
            source_closure_kind: None,
            source_closure_reason: None,
            final_outbox_disposition: Some(FinalOutboxDispositionV1::NotApplicable),
            canonical_source_queue_id: None,
            final_delivery_id: None,
            source_final_lane_digest: None,
            reason: if requested_specific_queue {
                if prepare.receipt.terminal_control_matched == Some(true) {
                    prepare.receipt.reason.clone()
                } else {
                    "requested queue item was not pending or prepared".to_string()
                }
            } else if prepare.receipt.terminal_control_matched == Some(true) {
                prepare.receipt.reason.clone()
            } else {
                "no pending or prepared runtime queue item is available".to_string()
            },
        };
        if receipt.status != RuntimeRunOnceStatus::Suppressed {
            append_runtime_run_once_log(
                &options.harness_home,
                HarnessLogLevel::Warn,
                "runtime.run-once.no-work",
                &receipt,
            )?;
        } else if let Some(queue_id) = receipt.queue_id.as_deref() {
            append_suppressed_runtime_progress(
                &options.harness_home,
                &prepare,
                None,
                queue_id,
                &receipt.reason,
                &mut warnings,
            )?;
        }
        let append_receipt = receipt.status != RuntimeRunOnceStatus::Suppressed;
        return write_runtime_run_once_report(
            RuntimeRunOnceReport {
                schema: RUNTIME_RUN_ONCE_SCHEMA,
                harness_home: options.harness_home,
                report_file,
                receipts_file,
                receipt,
                prepare: Some(prepare),
                plan: None,
                run: None,
                outbox_file: None,
                outbound_message: None,
                warnings,
            },
            append_receipt,
        );
    }

    if let Some(item) = prepare.item.as_ref()
        && let Some(acknowledged) = acknowledge_goal_continuation_after_lease(
            &options.harness_home,
            item,
            current_log_time_ms()?,
        )?
    {
        warnings.push(format!(
            "goal continuation intent {} acknowledged after child lane lease acquisition",
            acknowledged.intent_key
        ));
    }

    let plan = plan_codex_runtime(CodexRuntimePlanOptions {
        harness_home: options.harness_home.clone(),
        execution_dir: prepare.receipt.execution_dir.clone(),
        codex_executable: options.codex_executable,
    })?;
    warnings.extend(plan.warnings.clone());

    if plan.receipt.status == CodexRuntimeReceiptStatus::NoPreparedExecution {
        if let Some(queue_id) = prepare.receipt.queue_id.as_deref()
            && let QueueTerminalControl::Terminal(control) = resolve_queue_terminal_control(
                &options.harness_home,
                queue_id,
                prepare.item.as_ref().map(|item| item.session_key.as_str()),
            )?
        {
            let metadata = runtime_run_metadata(&prepare);
            if let Err(error) = release_runtime_queue_lease(&options.harness_home, queue_id) {
                warnings.push(format!("runtime queue lease release failed: {error}"));
            }
            let _ = record_terminal_control_suppression(
                &options.harness_home,
                queue_id,
                metadata.runtime_class.as_deref(),
                metadata.origin.as_deref(),
                metadata.cron_run_id.as_deref(),
                metadata.scheduled_for_ms,
                &metadata.continuation,
                &control,
            )?;
            let receipt = RuntimeRunOnceReceipt {
                queue_id: Some(queue_id.to_string()),
                status: RuntimeRunOnceStatus::Suppressed,
                runtime_class: metadata.runtime_class,
                origin: metadata.origin,
                cron_run_id: metadata.cron_run_id,
                scheduled_for_ms: metadata.scheduled_for_ms,
                execution_dir: plan.execution_dir.clone(),
                transcript_file: None,
                outbox_file: None,
                continuation: metadata.continuation,
                child_queue_id: None,
                child_session_key: None,
                terminal_disposition: Some(
                    crate::RuntimeTerminalDispositionV1::TerminalSuppression,
                ),
                continuation_link: None,
                protocol_failure: None,
                mutation_evidence: None,
                retry_schedule: None,
                task_drain_evaluation: None,
                external_effect: None,
                terminal_control_matched: Some(true),
                terminal_control_source: Some(control.source.as_str().to_string()),
                suppressed_run_once_reason: Some("terminal-control-present".to_string()),
                prepared_execution_terminalization_reason: None,
                source_final_expectation: Some(SourceFinalExpectationV1::NotApplicable),
                source_closure_kind: None,
                source_closure_reason: None,
                final_outbox_disposition: Some(FinalOutboxDispositionV1::NotApplicable),
                canonical_source_queue_id: None,
                final_delivery_id: None,
                source_final_lane_digest: None,
                reason: format!(
                    "runtime queue item suppressed before no-prepared execution fallback because terminal control is present: {}: {}",
                    control.source.as_str(),
                    control.reason
                ),
            };
            append_suppressed_runtime_progress(
                &options.harness_home,
                &prepare,
                plan.plan.as_ref(),
                queue_id,
                &receipt.reason,
                &mut warnings,
            )?;
            return write_runtime_run_once_report(
                RuntimeRunOnceReport {
                    schema: RUNTIME_RUN_ONCE_SCHEMA,
                    harness_home: options.harness_home,
                    report_file,
                    receipts_file,
                    receipt,
                    prepare: Some(prepare),
                    plan: Some(plan),
                    run: None,
                    outbox_file: None,
                    outbound_message: None,
                    warnings,
                },
                false,
            );
        }
        let no_prepared_attempts = prepare
            .receipt
            .queue_id
            .as_deref()
            .map(|queue_id| {
                count_prior_run_once_status(
                    &options.harness_home,
                    queue_id,
                    "no-prepared-execution",
                )
            })
            .transpose()?
            .map(|attempts| attempts.saturating_add(1))
            .unwrap_or(1);
        let no_prepared_threshold =
            no_prepared_execution_terminal_threshold(&options.harness_home).max(1);
        if let Some(queue_id) = prepare.receipt.queue_id.as_deref() {
            if let Err(error) = release_runtime_queue_lease(&options.harness_home, queue_id) {
                warnings.push(format!("runtime queue lease release failed: {error}"));
            }
        }
        let terminalization_reason =
            (no_prepared_attempts >= no_prepared_threshold).then(|| {
                format!(
                    "no prepared execution repeated {no_prepared_attempts}/{no_prepared_threshold} times for queue item"
                )
            });
        if let (Some(queue_id), Some(reason)) = (
            prepare.receipt.queue_id.as_deref(),
            terminalization_reason.as_deref(),
        ) {
            write_runtime_queue_quarantine_marker(
                &options.harness_home,
                queue_id,
                reason,
                current_log_time_ms()?,
            )?;
        }
        let receipt = RuntimeRunOnceReceipt {
            queue_id: prepare.receipt.queue_id.clone(),
            status: if terminalization_reason.is_some() {
                RuntimeRunOnceStatus::FailedTerminal
            } else {
                RuntimeRunOnceStatus::NoPreparedExecution
            },
            runtime_class: prepare.receipt.runtime_class.clone(),
            origin: prepare.receipt.origin.clone(),
            cron_run_id: prepare.receipt.cron_run_id.clone(),
            scheduled_for_ms: prepare.receipt.scheduled_for_ms,
            execution_dir: plan.execution_dir.clone(),
            transcript_file: None,
            outbox_file: None,
            continuation: prepare.receipt.continuation.clone(),
            child_queue_id: None,
            child_session_key: None,
            terminal_disposition: None,
            continuation_link: None,
            protocol_failure: None,
            mutation_evidence: None,
            retry_schedule: None,
            task_drain_evaluation: None,
            external_effect: None,
            terminal_control_matched: None,
            terminal_control_source: None,
            suppressed_run_once_reason: None,
            prepared_execution_terminalization_reason: terminalization_reason.clone(),
            source_final_expectation: Some(SourceFinalExpectationV1::NotApplicable),
            source_closure_kind: None,
            source_closure_reason: None,
            final_outbox_disposition: Some(FinalOutboxDispositionV1::NotApplicable),
            canonical_source_queue_id: None,
            final_delivery_id: None,
            source_final_lane_digest: None,
            reason: terminalization_reason
                .clone()
                .unwrap_or_else(|| "no prepared runtime execution is available to run".to_string()),
        };
        append_runtime_run_once_log(
            &options.harness_home,
            if receipt.status == RuntimeRunOnceStatus::FailedTerminal {
                HarnessLogLevel::Error
            } else {
                HarnessLogLevel::Warn
            },
            if receipt.status == RuntimeRunOnceStatus::FailedTerminal {
                "runtime.run-once.failed-terminal"
            } else {
                "runtime.run-once.no-prepared-execution"
            },
            &receipt,
        )?;
        return write_runtime_run_once_report(
            RuntimeRunOnceReport {
                schema: RUNTIME_RUN_ONCE_SCHEMA,
                harness_home: options.harness_home,
                report_file,
                receipts_file,
                receipt,
                prepare: Some(prepare),
                plan: Some(plan),
                run: None,
                outbox_file: None,
                outbound_message: None,
                warnings,
            },
            true,
        );
    }

    if channel_context.is_none()
        && let Some(queue_id) = plan.receipt.queue_id.as_deref()
    {
        channel_context =
            find_queue_channel_context(&options.harness_home, queue_id, &mut warnings)?;
    }
    let channel_context_for_self_improvement = channel_context.clone();
    let failure_channel_context = channel_context.clone();

    let progress_context =
        progress_context_from(&prepare, plan.plan.as_ref(), channel_context.as_ref());
    if let Some(context) = &progress_context {
        append_runtime_progress_started(
            &options.harness_home,
            context,
            prepare.item.as_ref(),
            plan.plan.as_ref(),
            &mut warnings,
        );
    }
    if let Some(codex_plan) = plan.plan.as_ref()
        && let Err(error) = record_skill_usage_from_prompt_bundle(
            &options.harness_home,
            &codex_plan.prompt_bundle_json,
            codex_plan.queue_id.as_deref(),
            "runtime-plan",
        )
    {
        warnings.push(format!("skill usage ledger recording failed: {error}"));
    }

    let (_, effective_timeout_ms, effective_idle_timeout_ms) = current_goal_campaign_timeouts(
        &options.harness_home,
        options.timeout_ms,
        options.idle_timeout_ms,
    )?;
    if effective_timeout_ms != options.timeout_ms
        || effective_idle_timeout_ms != options.idle_timeout_ms
    {
        warnings.push(format!(
            "runtime slice timeouts bounded by goal campaign policy: hard={}ms idle={}ms",
            effective_timeout_ms, effective_idle_timeout_ms
        ));
    }
    let run = run_codex_runtime(CodexRuntimeRunOptions {
        harness_home: options.harness_home.clone(),
        execution_dir: plan.execution_dir.clone(),
        plan_file: plan.plan_file.clone(),
        timeout_ms: effective_timeout_ms,
        idle_timeout_ms: effective_idle_timeout_ms,
        progress_context: progress_context.clone(),
    })?;
    warnings.extend(run.warnings.clone());
    let auth_deferred = run.receipt.status == CodexRuntimeRunStatus::PreflightBlocked
        && run.receipt.reason.contains("needs-operator-auth");
    let queue_failure_attempts = run
        .receipt
        .queue_id
        .as_deref()
        .filter(|_| run.receipt.status != CodexRuntimeRunStatus::Completed && !auth_deferred)
        .filter(|_| run.receipt.status != CodexRuntimeRunStatus::ApprovalRequired)
        .map(|queue_id| count_prior_runtime_failures(&options.harness_home, queue_id))
        .transpose()?
        .map(|attempts| attempts.saturating_add(1))
        .unwrap_or(0);
    let retry_policy = inspect_runtime_backoff_policy(&options.harness_home)?;
    warnings.extend(retry_policy.warnings.clone());
    let mut receipt_status = final_run_once_status_with_protocol(
        run.receipt.status,
        queue_failure_attempts,
        &run.receipt.reason,
        retry_policy.policy.max_failure_attempts,
        run.receipt.protocol_failure.as_ref(),
        run.receipt.mutation_evidence,
    );
    let mut receipt_reason = final_run_once_reason(
        receipt_status,
        run.receipt.status,
        queue_failure_attempts,
        retry_policy.policy.max_failure_attempts,
        &run.receipt.reason,
    );
    let server_overloaded_after_mutation = matches!(
        run.receipt
            .protocol_failure
            .as_ref()
            .map(|failure| failure.class),
        Some(crate::CodexProtocolFailureClass::ServerOverloaded)
    ) && run.receipt.mutation_evidence
        == Some(crate::RuntimeMutationEvidenceClass::MutationObserved);
    if matches!(
        run.receipt
            .protocol_failure
            .as_ref()
            .map(|failure| failure.class),
        Some(crate::CodexProtocolFailureClass::ServerOverloaded)
    ) && run.receipt.mutation_evidence == Some(crate::RuntimeMutationEvidenceClass::Unknown)
    {
        receipt_reason = format!(
            "Codex server overload was observed, but mutation evidence was incomplete; automatic prompt replay was suppressed to avoid duplicate side effects. Operator retry is required. Prior reason: {}",
            run.receipt.reason
        );
    }
    let retry_scheduled_at_ms = current_log_time_ms()?;
    let mut retry_schedule = runtime_retry_schedule_for(
        receipt_status,
        run.receipt.queue_id.as_deref(),
        queue_failure_attempts,
        retry_policy.policy.max_failure_attempts,
        retry_policy.policy.retry_delay_ms(queue_failure_attempts),
        retry_scheduled_at_ms,
        run.receipt.protocol_failure.as_ref(),
        run.receipt.mutation_evidence,
    );
    if receipt_status == RuntimeRunOnceStatus::RetryPending {
        let delay_ms = retry_policy.policy.retry_delay_ms(queue_failure_attempts);
        warnings.push(format!(
            "runtime retry policy scheduled attempt {}/{} after about {} ms",
            queue_failure_attempts, retry_policy.policy.max_failure_attempts, delay_ms
        ));
    } else if matches!(
        receipt_status,
        RuntimeRunOnceStatus::DeadLetter | RuntimeRunOnceStatus::FailedTerminal
    ) && retry_policy.policy.operator_hints
    {
        let provider = prepare
            .item
            .as_ref()
            .and_then(|item| item.provider.as_deref())
            .or_else(|| plan.plan.as_ref().and_then(|plan| plan.provider.as_deref()));
        let model = prepare
            .item
            .as_ref()
            .and_then(|item| item.model.as_deref())
            .or_else(|| plan.plan.as_ref().and_then(|plan| plan.model.as_deref()));
        if let Some(hint) = retry_policy
            .policy
            .fallback_hint(provider, model, &run.receipt.reason)
        {
            warnings.push(hint);
        }
    }
    if should_record_failed_memory_lifecycle(&receipt_status)
        && let Some(codex_plan) = plan.plan.as_ref()
    {
        match record_memory_lifecycle_turn(MemoryLifecycleTurnOptions {
            harness_home: options.harness_home.clone(),
            prompt_bundle_json: codex_plan.prompt_bundle_json.clone(),
            assistant_text: format!("runtime {}: {}", receipt_status.as_str(), receipt_reason),
            success: false,
            now_ms: current_log_time_ms()?,
        }) {
            Ok(memory) => warnings.extend(memory.warnings),
            Err(error) => warnings.push(format!(
                "failed-turn memory lifecycle recording failed: {error}"
            )),
        }
    }
    let mut outbox_file = None;
    let mut outbound_message = None;
    let mut final_outbox_disposition = FinalOutboxDispositionV1::NotApplicable;
    let mut canonical_source_queue_id = None;
    let mut final_delivery_id = None;
    let mut stale_session_explicit_non_delivery = false;
    let mut child_queue_id = None;
    let mut child_session_key = None;
    let mut source_closure_decision = None;
    let mut source_closure_reason = None;
    let mut parked_goal_activation = None;
    let goal_authority = establish_runtime_goal_authority_before_outbox(
        &options.harness_home,
        prepare.item.as_ref(),
        plan.plan.as_ref(),
        &run,
        receipt_status,
        &receipts_file,
        &mut warnings,
    );
    let goal_transition = evaluate_runtime_goal_transition(
        &options.harness_home,
        &run,
        receipt_status,
        &prepare.receipt.continuation,
        &goal_authority,
        &mut warnings,
    )?;
    warnings.push(format!(
        "goal transition decided {:?} with {:?} surface before final-outbox selection",
        goal_transition.decision, goal_transition.surface
    ));
    let mut task_drain_evaluation = None;
    let disposition_recovery_run = prepare
        .item
        .as_ref()
        .and_then(|item| item.continuation.disposition_recovery_depth)
        .is_some_and(|depth| depth > 0);
    let accepted_ordinary_drain = (goal_transition.event
        == GoalTransitionEventKind::DrainCompletion
        || disposition_recovery_run
        || (goal_transition.event == GoalTransitionEventKind::AbsoluteTimeout
            && run.receipt.drain_disposition_error.is_some()))
        && !goal_transition
            .goal_status
            .as_deref()
            .is_some_and(is_goal_status_active);
    let mut checkpoint_for_evaluation = run.receipt.drain_checkpoint.clone();
    if accepted_ordinary_drain {
        if let Some(marker) = run.receipt.drain_disposition.as_ref() {
            let expected_generation = latest_productive_deadline_generation(
                &options.harness_home,
                run.receipt.queue_id.as_deref(),
            )?;
            if marker.observed_deadline_generation != expected_generation {
                task_drain_evaluation = Some(crate::TaskDrainEvaluationV1 {
                    schema: crate::TASK_TRANSITION_SCHEMA.to_string(),
                    disposition: crate::DrainDispositionV1::Indeterminate,
                    schedule_continuation: false,
                    allow_logical_final: false,
                    reason: "drain disposition deadline generation is stale".to_string(),
                });
            } else {
                match marker.disposition {
                    crate::DrainDispositionKindV1::LogicalComplete => {
                        task_drain_evaluation = Some(crate::logical_complete_drain());
                    }
                    crate::DrainDispositionKindV1::ContinuationRequired => {
                        match crate::drain_marker_checkpoint(marker) {
                            Ok(checkpoint) => checkpoint_for_evaluation = Some(checkpoint),
                            Err(error) => {
                                task_drain_evaluation = Some(crate::TaskDrainEvaluationV1 {
                                    schema: crate::TASK_TRANSITION_SCHEMA.to_string(),
                                    disposition: crate::DrainDispositionV1::Indeterminate,
                                    schedule_continuation: false,
                                    allow_logical_final: false,
                                    reason: error.to_string(),
                                });
                            }
                        }
                    }
                    crate::DrainDispositionKindV1::NeedsUser => {
                        task_drain_evaluation = Some(crate::TaskDrainEvaluationV1 {
                            schema: crate::TASK_TRANSITION_SCHEMA.to_string(),
                            disposition: crate::DrainDispositionV1::NeedsUser,
                            schedule_continuation: false,
                            allow_logical_final: false,
                            reason: marker
                                .question
                                .clone()
                                .unwrap_or_else(|| "the drained task needs user input".to_string()),
                        });
                    }
                    crate::DrainDispositionKindV1::NeedsAuthority => {
                        task_drain_evaluation = Some(crate::TaskDrainEvaluationV1 {
                            schema: crate::TASK_TRANSITION_SCHEMA.to_string(),
                            disposition: crate::DrainDispositionV1::NeedsAuthority,
                            schedule_continuation: false,
                            allow_logical_final: false,
                            reason: marker.reason_code.clone().unwrap_or_else(|| {
                                "the drained task needs additional authority".to_string()
                            }),
                        });
                    }
                    crate::DrainDispositionKindV1::Blocked => {
                        task_drain_evaluation = Some(crate::TaskDrainEvaluationV1 {
                            schema: crate::TASK_TRANSITION_SCHEMA.to_string(),
                            disposition: crate::DrainDispositionV1::Blocked,
                            schedule_continuation: false,
                            allow_logical_final: false,
                            reason: marker
                                .recovery_hint
                                .clone()
                                .unwrap_or_else(|| "the drained task is blocked".to_string()),
                        });
                    }
                }
            }
        } else if let Some(reason) = run.receipt.drain_disposition_error.as_ref() {
            task_drain_evaluation = Some(crate::TaskDrainEvaluationV1 {
                schema: crate::TASK_TRANSITION_SCHEMA.to_string(),
                disposition: crate::DrainDispositionV1::Indeterminate,
                schedule_continuation: false,
                allow_logical_final: false,
                reason: reason.clone(),
            });
        }
    }
    if accepted_ordinary_drain
        && task_drain_evaluation.is_none()
        && let Some(checkpoint) = checkpoint_for_evaluation
    {
        let item = prepare.item.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "typed drain checkpoint has no prepared exact-lane queue item",
            )
        })?;
        let operator_stop = ChannelStateLane::new(
            &item.platform,
            item.account_id.as_deref(),
            &item.channel_id,
            &item.user_id,
            &item.agent_id,
        )
        .ok()
        .and_then(|lane| {
            crate::read_channel_session_state_v2(&options.harness_home, &lane)
                .ok()
                .flatten()
        })
        .is_some_and(|state| state.stop_requested);
        let newer_steer = match channel_context.as_ref() {
            Some(context) => {
                !channel_session_is_current(&options.harness_home, context, &mut warnings)?
            }
            None => true,
        };
        let task_budget_status = if checkpoint.authority_kind
            == crate::ContinuationAuthorityKindV1::ExplicitCheckpoint
        {
            Some(if disposition_recovery_run {
                crate::task_budget_status(&options.harness_home, &checkpoint.authority_id)?
            } else {
                crate::record_task_budget_slice(
                    &options.harness_home,
                    crate::TaskBudgetSliceV1 {
                        schema: crate::TASK_BUDGET_LEDGER_SCHEMA.to_string(),
                        family_id: checkpoint.authority_id.clone(),
                        slice_generation: item.continuation.task_slice_generation.unwrap_or(0),
                        wall_time_ms: u64::try_from(run.receipt.elapsed_ms).unwrap_or(u64::MAX),
                        total_tokens: run
                            .receipt
                            .usage
                            .as_ref()
                            .and_then(|usage| usage.total_tokens)
                            .unwrap_or(0),
                        progress_digest: checkpoint.checkpoint_digest.clone(),
                        recovery_slice: run.receipt.context_recovery.is_some(),
                        disposition_recovery: false,
                        observed_at_ms: current_log_time_ms()?,
                    },
                )?
            })
        } else {
            None
        };
        let breakers = crate::TaskContinuationBreakers {
            operator_stop,
            newer_steer,
            budget_exhausted: task_budget_status
                .as_ref()
                .is_some_and(|status| status.exhausted),
            no_progress_exhausted: task_budget_status
                .as_ref()
                .is_some_and(|status| status.reason_code == "no-progress-budget-exhausted"),
            continuation_depth: item.continuation.task_slice_generation.unwrap_or(0),
            max_continuation_depth: crate::task_transition::DEFAULT_MAX_TASK_CONTINUATION_DEPTH,
        };
        let evaluation = match checkpoint.authority_kind {
            crate::ContinuationAuthorityKindV1::OperationPlan => {
                crate::evaluate_operation_plan_drain(crate::OperationPlanAuthorityOptions {
                    harness_home: options.harness_home.clone(),
                    checkpoint,
                    exact_lane_digest: goal_authority.lane_digest.clone().unwrap_or_default(),
                    virtual_session_id: goal_authority
                        .virtual_session_id
                        .clone()
                        .unwrap_or_default(),
                    working_session_key: item.session_key.clone(),
                    agent_id: item.agent_id.clone(),
                    breakers,
                })
            }
            crate::ContinuationAuthorityKindV1::ExplicitCheckpoint => {
                let expected_family_id = item
                    .continuation
                    .task_family_id
                    .clone()
                    .unwrap_or_else(|| checkpoint.authority_id.clone());
                crate::evaluate_explicit_checkpoint_drain(
                    crate::ExplicitCheckpointAuthorityOptions {
                        harness_home: options.harness_home.clone(),
                        checkpoint,
                        expected_family_id,
                        expected_root_queue_id: item
                            .continuation
                            .task_root_queue_id
                            .clone()
                            .unwrap_or_else(|| item.queue_id.clone()),
                        exact_lane_digest: goal_authority.lane_digest.clone().unwrap_or_default(),
                        virtual_session_id: goal_authority
                            .virtual_session_id
                            .clone()
                            .unwrap_or_default(),
                        working_session_key: item.session_key.clone(),
                        agent_id: item.agent_id.clone(),
                        breakers,
                    },
                )
            }
            crate::ContinuationAuthorityKindV1::Goal => crate::TaskDrainEvaluationV1 {
                schema: crate::TASK_TRANSITION_SCHEMA.to_string(),
                disposition: crate::DrainDispositionV1::NeedsAuthority,
                schedule_continuation: false,
                allow_logical_final: false,
                reason: "ordinary task checkpoint cannot claim Goal authority".to_string(),
            },
        };
        warnings.push(format!(
            "task drain transition scheduled={} disposition={:?}: {}",
            evaluation.schedule_continuation, evaluation.disposition, evaluation.reason
        ));
        task_drain_evaluation = Some(evaluation);
    }
    if disposition_recovery_run {
        let item = prepare.item.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "disposition recovery has no prepared exact-lane queue item",
            )
        })?;
        let family_id = item.continuation.task_family_id.as_deref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "disposition recovery has no harness-owned task family",
            )
        })?;
        let progress_digest = run
            .receipt
            .drain_disposition
            .as_ref()
            .and_then(|marker| marker.checkpoint_digest.clone())
            .unwrap_or_else(|| {
                crate::task_transition::sha256_hex(
                    task_drain_evaluation
                        .as_ref()
                        .map(|evaluation| evaluation.reason.as_bytes())
                        .unwrap_or_default(),
                )
            });
        crate::record_task_budget_slice(
            &options.harness_home,
            crate::TaskBudgetSliceV1 {
                schema: crate::TASK_BUDGET_LEDGER_SCHEMA.to_string(),
                family_id: family_id.to_string(),
                slice_generation: item.continuation.task_slice_generation.unwrap_or(0),
                wall_time_ms: u64::try_from(run.receipt.elapsed_ms).unwrap_or(u64::MAX),
                total_tokens: run
                    .receipt
                    .usage
                    .as_ref()
                    .and_then(|usage| usage.total_tokens)
                    .unwrap_or(0),
                progress_digest,
                recovery_slice: run.receipt.context_recovery.is_some(),
                disposition_recovery: true,
                observed_at_ms: current_log_time_ms()?,
            },
        )?;
    }
    if goal_transition.schedule_continuation
        && goal_transition
            .goal_status
            .as_deref()
            .is_some_and(is_goal_status_active)
    {
        let classified_shell_drift =
            run.receipt
                .shell_execution_failure
                .as_ref()
                .is_some_and(|failure| {
                    failure.recovery_eligible
                        && failure.kind == CodexShellExecutionFailureKindV1::AppxExecutableDrift
                });
        let shell_effect_fence_clear = shell_recovery_effect_fence_clear(&run.receipt);
        let eligible_shell_drift = classified_shell_drift && shell_effect_fence_clear;
        let shell_recovery_effect_fenced = classified_shell_drift && !shell_effect_fence_clear;
        let source_shell_recovery_depth = prepare
            .item
            .as_ref()
            .and_then(|item| item.continuation.shell_recovery_depth)
            .unwrap_or(0);
        let shell_recovery_budget_exhausted =
            eligible_shell_drift && source_shell_recovery_depth >= 1;
        let mut activation = load_goal_autonomy_activation(
            &options.harness_home,
            goal_transition.lane_digest.as_deref(),
        )?;
        if shell_recovery_effect_fenced && activation.mode == GoalAutonomyMode::Active {
            activation.reason =
                "shell-recovery-fenced-by-external-effect-or-mutation-evidence".to_string();
        } else if shell_recovery_budget_exhausted {
            activation.reason =
                "shell-recovery-budget-exhausted-after-fresh-runtime-continuation".to_string();
        }
        warnings.push(format!(
            "goal autonomy mode {:?}: {}",
            activation.mode, activation.reason
        ));
        if activation.mode == GoalAutonomyMode::Active
            && !shell_recovery_budget_exhausted
            && !shell_recovery_effect_fenced
        {
            let scheduling_result = (|| -> io::Result<_> {
                let item = prepare.item.as_ref().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "active goal transition has no prepared queue item",
                    )
                })?;
                let now_ms = current_log_time_ms()?;
                let mut continuation = item.continuation.clone();
                if eligible_shell_drift {
                    continuation.shell_recovery_depth = Some(1);
                }
                let intent = commit_goal_continuation_intent(
                    &options.harness_home,
                    &goal_transition,
                    &item.session_key,
                    &continuation,
                    now_ms,
                )?;
                ensure_goal_continuation_enqueued(&options.harness_home, &intent.intent_key, now_ms)
            })();
            let enqueued = match scheduling_result {
                Ok(enqueued) => enqueued,
                Err(error) => {
                    if let Some(queue_id) = prepare.receipt.queue_id.as_deref() {
                        let _ = release_runtime_queue_lease(&options.harness_home, queue_id);
                    }
                    return Err(io::Error::new(
                        error.kind(),
                        format!(
                            "active goal continuation failed closed before final outbox: {error}"
                        ),
                    ));
                }
            };
            child_queue_id = enqueued.child_queue_id.clone();
            child_session_key = Some(enqueued.working_session_key.clone());
            receipt_status = RuntimeRunOnceStatus::Skipped;
            receipt_reason = format!(
                "goal slice yielded to deterministic continuation {}; childQueueId={}; priorReason={}",
                enqueued.intent_key,
                enqueued.child_queue_id.as_deref().unwrap_or("missing"),
                receipt_reason
            );
            warnings.push(format!(
                "goal continuation intent {} enqueued one logical child before final-outbox selection",
                enqueued.intent_key
            ));
            source_closure_decision = Some(RuntimeSourceClosureDecision::CommittedHandoff);
            source_closure_reason = Some(
                if eligible_shell_drift {
                    "shell-runtime-drift-continuation-committed"
                } else {
                    "goal-continuation-committed"
                }
                .to_string(),
            );
        } else {
            source_closure_decision =
                Some(if activation.configured_mode == GoalAutonomyMode::Observe {
                    RuntimeSourceClosureDecision::ParkedObserve
                } else {
                    RuntimeSourceClosureDecision::ParkedPolicyDenied
                });
            source_closure_reason = Some(if shell_recovery_effect_fenced {
                "shell-recovery-external-effect-fenced".to_string()
            } else if shell_recovery_budget_exhausted {
                "shell-recovery-budget-exhausted".to_string()
            } else {
                match activation.configured_mode {
                    GoalAutonomyMode::Disabled => "goal-autonomy-disabled",
                    GoalAutonomyMode::Active => "goal-autonomy-lane-denied",
                    GoalAutonomyMode::Observe => "goal-autonomy-observe",
                }
                .to_string()
            });
            receipt_status = RuntimeRunOnceStatus::NeedsUser;
            receipt_reason = format!(
                "active goal parked without an automatic child: {}; priorReason={}",
                activation.reason, receipt_reason
            );
            parked_goal_activation = Some(activation);
        }
    }
    if child_queue_id.is_none()
        && task_drain_evaluation.as_ref().is_some_and(|evaluation| {
            evaluation.disposition == crate::DrainDispositionV1::Indeterminate
        })
    {
        schedule_disposition_recovery_child(
            &options.harness_home,
            prepare.item.as_ref(),
            &goal_transition,
            &goal_authority,
            task_drain_evaluation.as_ref(),
            &mut child_queue_id,
            &mut child_session_key,
            &mut receipt_status,
            &mut receipt_reason,
        )?;
    }
    if child_queue_id.is_none()
        && let Some(evaluation) = task_drain_evaluation.as_ref()
        && let crate::DrainDispositionV1::ContinuationRequired(authority) = &evaluation.disposition
    {
        let scheduling_result = (|| -> io::Result<_> {
            let item = prepare.item.as_ref().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "task drain continuation has no prepared queue item",
                )
            })?;
            let now_ms = current_log_time_ms()?;
            let intent = crate::commit_task_continuation_intent(
                &options.harness_home,
                &item.queue_id,
                &item.session_key,
                &item.continuation,
                authority,
                item.continuation.task_slice_generation.unwrap_or(0),
                goal_transition.decision_generation,
                false,
                now_ms,
            )?;
            ensure_goal_continuation_enqueued(&options.harness_home, &intent.intent_key, now_ms)
        })();
        let enqueued = match scheduling_result {
            Ok(enqueued) => enqueued,
            Err(error) => {
                if let Some(queue_id) = prepare.receipt.queue_id.as_deref() {
                    let _ = release_runtime_queue_lease(&options.harness_home, queue_id);
                }
                return Err(io::Error::new(
                    error.kind(),
                    format!("task continuation failed closed before final outbox: {error}"),
                ));
            }
        };
        child_queue_id = enqueued.child_queue_id.clone();
        child_session_key = Some(enqueued.working_session_key.clone());
        receipt_status = RuntimeRunOnceStatus::Skipped;
        receipt_reason = format!(
            "deadline-drain task checkpoint yielded to deterministic continuation {}; childQueueId={}; priorReason={}",
            enqueued.intent_key,
            enqueued.child_queue_id.as_deref().unwrap_or("missing"),
            receipt_reason
        );
        warnings.push(format!(
            "task continuation intent {} admitted one child before parent handoff",
            enqueued.intent_key
        ));
    }
    if let Some(activation) = parked_goal_activation.as_ref() {
        if let Some(context) = channel_context {
            let agent_id = prepare
                .item
                .as_ref()
                .map(|item| item.agent_id.as_str())
                .or_else(|| plan.plan.as_ref().and_then(|plan| plan.agent_id.as_deref()));
            let run_session_key = prepare
                .item
                .as_ref()
                .map(|item| item.session_key.as_str())
                .or_else(|| plan.plan.as_ref().map(|plan| plan.session_key.as_str()));
            if !final_outbox_run_owns_channel_parent(
                prepare.receipt.runtime_class.as_deref(),
                prepare.receipt.origin.as_deref(),
                agent_id,
                run_session_key,
                &context.session_key,
            ) {
                warnings.push(
                    "active-goal parked notice suppressed because this run does not own the exact channel parent"
                        .to_string(),
                );
            } else if !channel_session_is_current(&options.harness_home, &context, &mut warnings)? {
                warnings.push(
                    "active-goal parked notice held because the exact channel session is stale"
                        .to_string(),
                );
            } else {
                let safe_checkpoint = match run.receipt.transcript_file.as_ref() {
                    Some(transcript_file) => latest_assistant_response(
                        transcript_file,
                        &AssistantNarrationConfig::default(),
                    )?
                    .and_then(|response| {
                        let input_kind = final_outbox_input_kind_for_completed_response(
                            prepare.receipt.runtime_class.as_deref(),
                            prepare.receipt.origin.as_deref(),
                            agent_id,
                            run_session_key,
                            &context.session_key,
                            response.prior_user_text.as_deref(),
                            &response.final_text,
                        );
                        final_outbox_decision(input_kind)
                            .may_write_final_outbox()
                            .then_some(response.final_text)
                    }),
                    None => None,
                };
                let mut message = ChannelOutboundMessage {
                    platform: context.platform.clone(),
                    account_id: context.account_id.clone(),
                    channel_id: context.channel_id.clone(),
                    user_id: context.user_id.clone(),
                    session_key: context.session_key.clone(),
                    delivery_id: None,
                    kind: ChannelOutboundMessageKind::AgentReply,
                    source_queue_id: run.receipt.queue_id.clone(),
                    source_completion_file: run.receipt.completion_file.clone(),
                    presentation: None,
                    text: active_goal_parked_notice_text(activation, safe_checkpoint.as_deref()),
                    delivery_intent: delivery_intent_from_inbound_context(
                        &context.platform,
                        &context.channel_id,
                        context.inbound_context.as_deref(),
                    ),
                    attachments: Vec::new(),
                };
                let outcome = append_final_outbound_message_once(
                    &options.harness_home,
                    run.receipt.execution_dir.as_deref(),
                    run.receipt.completion_file.as_deref(),
                    &mut message,
                    &mut warnings,
                )?;
                let appended = outcome.appended();
                outbox_file = Some(outcome.outbox_file.clone());
                final_outbox_disposition = outcome.disposition;
                canonical_source_queue_id = outcome.canonical_source_queue_id;
                final_delivery_id = outcome.delivery_id;
                if appended {
                    outbound_message = Some(message);
                }
            }
        } else {
            warnings.push(
                "queue channel context was unavailable; active-goal parked notice was not written"
                    .to_string(),
            );
        }
    } else if goal_transition.surface == GoalTransitionSurface::TerminalNotice
        && goal_transition.campaign_family_id.is_some()
    {
        if let Some(context) = channel_context {
            let agent_id = prepare
                .item
                .as_ref()
                .map(|item| item.agent_id.as_str())
                .or_else(|| plan.plan.as_ref().and_then(|plan| plan.agent_id.as_deref()));
            let run_session_key = prepare
                .item
                .as_ref()
                .map(|item| item.session_key.as_str())
                .or_else(|| plan.plan.as_ref().map(|plan| plan.session_key.as_str()));
            if !final_outbox_run_owns_channel_parent(
                prepare.receipt.runtime_class.as_deref(),
                prepare.receipt.origin.as_deref(),
                agent_id,
                run_session_key,
                &context.session_key,
            ) {
                warnings.push(
                    "goal terminal notice suppressed because this run does not own the exact channel parent"
                        .to_string(),
                );
            } else if !channel_session_is_current(&options.harness_home, &context, &mut warnings)? {
                warnings.push(
                    "goal terminal notice suppressed because the exact channel session is stale"
                        .to_string(),
                );
            } else if let Some(text) = goal_terminal_notice_text(goal_transition.decision) {
                let mut message = ChannelOutboundMessage {
                    platform: context.platform.clone(),
                    account_id: context.account_id.clone(),
                    channel_id: context.channel_id.clone(),
                    user_id: context.user_id.clone(),
                    session_key: context.session_key.clone(),
                    delivery_id: None,
                    kind: ChannelOutboundMessageKind::AgentReply,
                    source_queue_id: run.receipt.queue_id.clone(),
                    source_completion_file: run.receipt.completion_file.clone(),
                    presentation: None,
                    text: text.to_string(),
                    delivery_intent: delivery_intent_from_inbound_context(
                        &context.platform,
                        &context.channel_id,
                        context.inbound_context.as_deref(),
                    ),
                    attachments: Vec::new(),
                };
                let outcome = append_goal_terminal_outbound_message_once(
                    &options.harness_home,
                    run.receipt.execution_dir.as_deref(),
                    run.receipt.completion_file.as_deref(),
                    &goal_transition,
                    &mut message,
                    &mut warnings,
                )?;
                let appended = outcome.appended();
                outbox_file = Some(outcome.outbox_file.clone());
                final_outbox_disposition = outcome.disposition;
                canonical_source_queue_id = outcome.canonical_source_queue_id;
                final_delivery_id = outcome.delivery_id;
                if appended {
                    outbound_message = Some(message);
                }
            } else {
                warnings.push(format!(
                    "goal terminal notice suppressed because decision {:?} has no sanitized provider surface",
                    goal_transition.decision
                ));
            }
        } else {
            warnings.push(
                "queue channel context was unavailable; goal terminal notice was not written to outbox"
                    .to_string(),
            );
        }
    } else if child_queue_id.is_none()
        && run.receipt.status == CodexRuntimeRunStatus::Completed
        && task_drain_evaluation.as_ref().is_some_and(|evaluation| {
            !evaluation.schedule_continuation && !evaluation.allow_logical_final
        })
    {
        receipt_status = RuntimeRunOnceStatus::FailedTerminal;
        receipt_reason = task_drain_evaluation
            .as_ref()
            .map(|evaluation| evaluation.reason.clone())
            .unwrap_or_else(|| "task continuation authority is unavailable".to_string());
        if let Some(context) = channel_context {
            if channel_session_is_current(&options.harness_home, &context, &mut warnings)? {
                let mut message = ChannelOutboundMessage {
                    platform: context.platform.clone(),
                    account_id: context.account_id.clone(),
                    channel_id: context.channel_id.clone(),
                    user_id: context.user_id.clone(),
                    session_key: context.session_key.clone(),
                    delivery_id: None,
                    kind: ChannelOutboundMessageKind::ErrorReply,
                    source_queue_id: run.receipt.queue_id.clone(),
                    source_completion_file: run.receipt.completion_file.clone(),
                    text: task_drain_notice_text(
                        task_drain_evaluation.as_ref().expect("checked above"),
                        item_disposition_recovery_depth(prepare.item.as_ref()),
                    ),
                    presentation: None,
                    delivery_intent: delivery_intent_from_inbound_context(
                        &context.platform,
                        &context.channel_id,
                        context.inbound_context.as_deref(),
                    ),
                    attachments: Vec::new(),
                };
                let outcome = append_final_outbound_message_once(
                    &options.harness_home,
                    run.receipt.execution_dir.as_deref(),
                    run.receipt.completion_file.as_deref(),
                    &mut message,
                    &mut warnings,
                )?;
                let appended = outcome.appended();
                outbox_file = Some(outcome.outbox_file.clone());
                final_outbox_disposition = outcome.disposition;
                canonical_source_queue_id = outcome.canonical_source_queue_id;
                final_delivery_id = outcome.delivery_id;
                if appended {
                    outbound_message = Some(message);
                }
            }
        }
    } else if run.receipt.status == CodexRuntimeRunStatus::Completed
        && (!matches!(
            goal_transition.surface,
            GoalTransitionSurface::CampaignFinal | GoalTransitionSurface::OrdinaryFinal
        ) || task_drain_evaluation
            .as_ref()
            .is_some_and(|evaluation| !evaluation.allow_logical_final))
    {
        warnings.push(format!(
            "completed slice final outbox suppressed by unified goal transition {:?}: {}",
            goal_transition.decision, goal_transition.reason
        ));
    } else if run.receipt.status == CodexRuntimeRunStatus::Completed {
        if run.receipt.event_count == 0 && run.receipt.reason.contains("already recorded") {
            warnings.push(
                "codex-run reported an already recorded completion; checking final outbox idempotency"
                    .to_string(),
            );
        }
        if let Some(context) = channel_context {
            if let Some(transcript_file) = run.receipt.transcript_file.as_ref() {
                let narration_config = match load_assistant_narration_config(&options.harness_home)
                {
                    Ok(config) => {
                        warnings.extend(config.warnings.clone());
                        config
                    }
                    Err(error) => {
                        warnings.push(format!(
                            "assistant narration config could not be loaded; using defaults: {error}"
                        ));
                        AssistantNarrationConfig::default()
                    }
                };
                match latest_assistant_response(transcript_file, &narration_config)? {
                    Some(response) => {
                        if channel_session_is_current(
                            &options.harness_home,
                            &context,
                            &mut warnings,
                        )? {
                            let agent_id = prepare
                                .item
                                .as_ref()
                                .map(|item| item.agent_id.as_str())
                                .or_else(|| {
                                    plan.plan.as_ref().and_then(|plan| plan.agent_id.as_deref())
                                });
                            let run_session_key = prepare
                                .item
                                .as_ref()
                                .map(|item| item.session_key.as_str())
                                .or_else(|| {
                                    plan.plan.as_ref().map(|plan| plan.session_key.as_str())
                                });
                            let input_kind = final_outbox_input_kind_for_completed_response(
                                prepare.receipt.runtime_class.as_deref(),
                                prepare.receipt.origin.as_deref(),
                                agent_id,
                                run_session_key,
                                &context.session_key,
                                response.prior_user_text.as_deref(),
                                &response.final_text,
                            );
                            let decision = final_outbox_decision(input_kind);
                            if !decision.may_write_final_outbox() {
                                warnings.push(format!(
                                    "final outbox suppressed: completed response classified as {:?} with disposition {:?}",
                                    input_kind, decision.disposition
                                ));
                                let receipt = RuntimeRunOnceReceipt {
                                    queue_id: run.receipt.queue_id.clone(),
                                    status: map_run_once_status(run.receipt.status),
                                    runtime_class: prepare.receipt.runtime_class.clone(),
                                    origin: prepare.receipt.origin.clone(),
                                    cron_run_id: prepare.receipt.cron_run_id.clone(),
                                    scheduled_for_ms: prepare.receipt.scheduled_for_ms,
                                    execution_dir: run.receipt.execution_dir.clone(),
                                    transcript_file: run.receipt.transcript_file.clone(),
                                    outbox_file: None,
                                    continuation: prepare.receipt.continuation.clone(),
                                    child_queue_id: None,
                                    child_session_key: None,
                                    terminal_disposition: None,
                                    continuation_link: None,
                                    protocol_failure: run.receipt.protocol_failure.clone(),
                                    mutation_evidence: run.receipt.mutation_evidence,
                                    retry_schedule: None,
                                    task_drain_evaluation: None,
                                    external_effect: run.receipt.external_effect.clone(),
                                    terminal_control_matched: None,
                                    terminal_control_source: None,
                                    suppressed_run_once_reason: None,
                                    prepared_execution_terminalization_reason: None,
                                    source_final_expectation: Some(
                                        SourceFinalExpectationV1::ExplicitNonDelivery,
                                    ),
                                    source_closure_kind: Some(
                                        RuntimeSourceClosureKindV1::SuppressedExactOwnerMismatch,
                                    ),
                                    source_closure_reason: Some(
                                        "completed-response-not-parent-final".to_string(),
                                    ),
                                    final_outbox_disposition: Some(
                                        FinalOutboxDispositionV1::ExplicitNonDelivery,
                                    ),
                                    canonical_source_queue_id: None,
                                    final_delivery_id: None,
                                    source_final_lane_digest: None,
                                    reason: format!(
                                        "final outbox suppressed: completed response classified as {:?} with disposition {:?}",
                                        input_kind, decision.disposition
                                    ),
                                };
                                append_runtime_run_once_log(
                                    &options.harness_home,
                                    HarnessLogLevel::Warn,
                                    "runtime.run-once.final-outbox-suppressed",
                                    &receipt,
                                )?;
                            } else {
                                let (mut text, mut attachments) = split_outbound_media_directives(
                                    &options.harness_home,
                                    run.receipt.queue_id.as_deref(),
                                    Some(context.platform.as_str()),
                                    &response.outbound_text,
                                    &mut warnings,
                                )?;
                                let lint_result = maybe_record_media_delivery_lint(
                                    &options.harness_home,
                                    run.receipt.queue_id.as_deref(),
                                    Some(context.platform.as_str()),
                                    response.prior_user_text.as_deref(),
                                    &response.outbound_text,
                                    attachments.len(),
                                    &mut warnings,
                                );
                                let mut outbound_kind = decision
                                    .outbound_kind
                                    .unwrap_or(ChannelOutboundMessageKind::AgentReply);
                                if lint_result.fail_closed {
                                    text = media_delivery_lint_terminal_notice();
                                    attachments.clear();
                                    outbound_kind = ChannelOutboundMessageKind::ErrorReply;
                                }
                                let tone_config = match load_response_tone_config(
                                    &options.harness_home,
                                ) {
                                    Ok(config) => {
                                        warnings.extend(config.warnings.clone());
                                        config
                                    }
                                    Err(error) => {
                                        warnings.push(format!(
                                            "response tone config could not be loaded; using defaults: {error}"
                                        ));
                                        ResponseToneConfig::default()
                                    }
                                };
                                text = apply_response_tone(
                                    &text,
                                    ResponseToneContext {
                                        agent_id,
                                        platform: &context.platform,
                                        channel_id: &context.channel_id,
                                        user_id: &context.user_id,
                                    },
                                    &tone_config,
                                );
                                let mut presentation = decision
                                    .attach_plain_final_presentation
                                    .then(|| {
                                        rich_presentation_from_plain_final_with_attachment_count(
                                            &text,
                                            attachments.len(),
                                        )
                                    })
                                    .flatten();
                                if let Some(presentation) = presentation.as_mut() {
                                    resolve_presentation_artifact_refs(
                                        &options.harness_home,
                                        run.receipt.queue_id.as_deref(),
                                        Some(context.platform.as_str()),
                                        presentation,
                                        &mut attachments,
                                        &mut warnings,
                                    )?;
                                }
                                let mut message = ChannelOutboundMessage {
                                    platform: context.platform.clone(),
                                    account_id: context.account_id.clone(),
                                    channel_id: context.channel_id.clone(),
                                    user_id: context.user_id.clone(),
                                    session_key: context.session_key.clone(),
                                    delivery_id: None,
                                    kind: outbound_kind,
                                    source_queue_id: run.receipt.queue_id.clone(),
                                    source_completion_file: run.receipt.completion_file.clone(),
                                    presentation,
                                    text,
                                    delivery_intent: delivery_intent_from_inbound_context(
                                        &context.platform,
                                        &context.channel_id,
                                        context.inbound_context.as_deref(),
                                    ),
                                    attachments,
                                };
                                let outcome = if goal_transition.surface
                                    == GoalTransitionSurface::CampaignFinal
                                {
                                    append_goal_terminal_outbound_message_once(
                                        &options.harness_home,
                                        run.receipt.execution_dir.as_deref(),
                                        run.receipt.completion_file.as_deref(),
                                        &goal_transition,
                                        &mut message,
                                        &mut warnings,
                                    )?
                                } else {
                                    append_final_outbound_message_once(
                                        &options.harness_home,
                                        run.receipt.execution_dir.as_deref(),
                                        run.receipt.completion_file.as_deref(),
                                        &mut message,
                                        &mut warnings,
                                    )?
                                };
                                let appended = outcome.appended();
                                outbox_file = Some(outcome.outbox_file.clone());
                                final_outbox_disposition = outcome.disposition;
                                canonical_source_queue_id = outcome.canonical_source_queue_id;
                                final_delivery_id = outcome.delivery_id;
                                if appended {
                                    if let Some(codex_plan) = plan.plan.as_ref() {
                                        match record_memory_lifecycle_turn(
                                            MemoryLifecycleTurnOptions {
                                                harness_home: options.harness_home.clone(),
                                                prompt_bundle_json: codex_plan
                                                    .prompt_bundle_json
                                                    .clone(),
                                                assistant_text: response.final_text.clone(),
                                                success: true,
                                                now_ms: current_log_time_ms()?,
                                            },
                                        ) {
                                            Ok(memory) => warnings.extend(memory.warnings),
                                            Err(error) => warnings.push(format!(
                                                "memory lifecycle recording failed: {error}"
                                            )),
                                        }
                                    }
                                    outbound_message = Some(message);
                                }
                            }
                        } else {
                            let receipt = RuntimeRunOnceReceipt {
                                queue_id: run.receipt.queue_id.clone(),
                                status: map_run_once_status(run.receipt.status),
                                runtime_class: prepare.receipt.runtime_class.clone(),
                                origin: prepare.receipt.origin.clone(),
                                cron_run_id: prepare.receipt.cron_run_id.clone(),
                                scheduled_for_ms: prepare.receipt.scheduled_for_ms,
                                execution_dir: run.receipt.execution_dir.clone(),
                                transcript_file: run.receipt.transcript_file.clone(),
                                outbox_file: None,
                                continuation: prepare.receipt.continuation.clone(),
                                child_queue_id: None,
                                child_session_key: None,
                                terminal_disposition: None,
                                continuation_link: None,
                                protocol_failure: run.receipt.protocol_failure.clone(),
                                mutation_evidence: run.receipt.mutation_evidence,
                                retry_schedule: None,
                                task_drain_evaluation: None,
                                external_effect: run.receipt.external_effect.clone(),
                                terminal_control_matched: None,
                                terminal_control_source: None,
                                suppressed_run_once_reason: None,
                                prepared_execution_terminalization_reason: None,
                                source_final_expectation: Some(
                                    SourceFinalExpectationV1::ExplicitNonDelivery,
                                ),
                                source_closure_kind: Some(
                                    RuntimeSourceClosureKindV1::SuppressedExactOwnerMismatch,
                                ),
                                source_closure_reason: Some(
                                    "failure-response-not-parent-final".to_string(),
                                ),
                                final_outbox_disposition: Some(
                                    FinalOutboxDispositionV1::ExplicitNonDelivery,
                                ),
                                canonical_source_queue_id: None,
                                final_delivery_id: None,
                                source_final_lane_digest: None,
                                reason: run.receipt.reason.clone(),
                            };
                            append_runtime_run_once_log(
                                &options.harness_home,
                                HarnessLogLevel::Warn,
                                "runtime.run-once.stale-session-suppressed",
                                &receipt,
                            )?;
                        }
                    }
                    None => warnings.push(format!(
                        "no assistant message found in transcript {}",
                        transcript_file.display()
                    )),
                }
            }
        } else {
            warnings.push(
                "queue channel context was unavailable; assistant reply was not written to outbox"
                    .to_string(),
            );
        }
    } else if should_write_failure_outbox(receipt_status)
        && matches!(
            goal_transition.surface,
            GoalTransitionSurface::ProgressOnly | GoalTransitionSurface::SuppressStale
        )
    {
        warnings.push(format!(
            "failure outbox suppressed by unified goal transition {:?}: {}",
            goal_transition.decision, goal_transition.reason
        ));
    } else if should_write_failure_outbox(receipt_status)
        && let Some(context) = channel_context
    {
        let status = receipt_status;
        if let Some(rollover) = maybe_enqueue_polluted_thread_continuation(
            &options.harness_home,
            prepare.item.as_ref(),
            &run,
            status,
            &mut warnings,
        )? {
            receipt_reason = format!(
                "context rollover parent tombstoned after polluted thread failure; childQueueId={}; childSessionKey={}; priorReason={}",
                rollover.requeued_queue_id, rollover.new_working_session_key, receipt_reason
            );
            child_queue_id = Some(rollover.requeued_queue_id.clone());
            child_session_key = Some(rollover.new_working_session_key.clone());
            warnings.push(format!(
                "polluted thread recovery enqueued continuation queue item {} and suppressed parent error outbox",
                rollover.requeued_queue_id
            ));
        } else if channel_session_is_current(&options.harness_home, &context, &mut warnings)? {
            let agent_id = prepare
                .item
                .as_ref()
                .map(|item| item.agent_id.as_str())
                .or_else(|| plan.plan.as_ref().and_then(|plan| plan.agent_id.as_deref()));
            let run_session_key = prepare
                .item
                .as_ref()
                .map(|item| item.session_key.as_str())
                .or_else(|| plan.plan.as_ref().map(|plan| plan.session_key.as_str()));
            let input_kind = final_outbox_input_kind_for_terminal_error(
                prepare.receipt.runtime_class.as_deref(),
                prepare.receipt.origin.as_deref(),
                agent_id,
                run_session_key,
                &context.session_key,
            );
            let decision = final_outbox_decision(input_kind);
            if !decision.may_write_final_outbox() {
                warnings.push(format!(
                    "failure outbox suppressed: terminal error classified as {:?} with disposition {:?}; runtime receipt remains authoritative evidence",
                    input_kind, decision.disposition
                ));
            } else {
                let approval_presentation = if status == RuntimeRunOnceStatus::NeedsUser {
                    run.receipt
                        .external_effect
                        .as_ref()
                        .filter(|intent| {
                            !intent.source_session_key_digest.is_empty()
                                && !intent.approval_authority_digest.is_empty()
                        })
                        .map(approval_request_presentation)
                        .transpose()?
                } else {
                    None
                };
                let (message_kind, message_text, presentation) =
                    if let Some((text, presentation)) = approval_presentation {
                        (
                            ChannelOutboundMessageKind::ApprovalRequest,
                            text,
                            Some(presentation),
                        )
                    } else {
                        (
                            decision
                                .outbound_kind
                                .unwrap_or(ChannelOutboundMessageKind::ErrorReply),
                            runtime_failure_reply_text(
                                status,
                                &runtime_failure_reply_reason(&run.receipt, &receipt_reason),
                                run.receipt.queue_id.as_deref(),
                            ),
                            None,
                        )
                    };
                let mut message = ChannelOutboundMessage {
                    platform: context.platform.clone(),
                    account_id: context.account_id.clone(),
                    channel_id: context.channel_id.clone(),
                    user_id: context.user_id.clone(),
                    session_key: context.session_key.clone(),
                    delivery_id: None,
                    kind: message_kind,
                    source_queue_id: run.receipt.queue_id.clone(),
                    source_completion_file: run.receipt.completion_file.clone(),
                    text: message_text,
                    presentation,
                    delivery_intent: delivery_intent_from_inbound_context(
                        &context.platform,
                        &context.channel_id,
                        context.inbound_context.as_deref(),
                    ),
                    attachments: Vec::new(),
                };
                let outcome = append_final_outbound_message_once(
                    &options.harness_home,
                    run.receipt.execution_dir.as_deref(),
                    run.receipt.completion_file.as_deref(),
                    &mut message,
                    &mut warnings,
                )?;
                let appended = outcome.appended();
                outbox_file = Some(outcome.outbox_file.clone());
                final_outbox_disposition = outcome.disposition;
                canonical_source_queue_id = outcome.canonical_source_queue_id;
                final_delivery_id = outcome.delivery_id;
                if appended {
                    outbound_message = Some(message);
                }
            }
        } else {
            stale_session_explicit_non_delivery = true;
            let receipt = RuntimeRunOnceReceipt {
                queue_id: run.receipt.queue_id.clone(),
                status,
                runtime_class: prepare.receipt.runtime_class.clone(),
                origin: prepare.receipt.origin.clone(),
                cron_run_id: prepare.receipt.cron_run_id.clone(),
                scheduled_for_ms: prepare.receipt.scheduled_for_ms,
                execution_dir: run.receipt.execution_dir.clone(),
                transcript_file: run.receipt.transcript_file.clone(),
                outbox_file: None,
                continuation: prepare.receipt.continuation.clone(),
                child_queue_id: None,
                child_session_key: None,
                terminal_disposition: None,
                continuation_link: None,
                protocol_failure: run.receipt.protocol_failure.clone(),
                mutation_evidence: run.receipt.mutation_evidence,
                retry_schedule: None,
                task_drain_evaluation: None,
                external_effect: run.receipt.external_effect.clone(),
                terminal_control_matched: None,
                terminal_control_source: None,
                suppressed_run_once_reason: None,
                prepared_execution_terminalization_reason: None,
                source_final_expectation: Some(SourceFinalExpectationV1::ExplicitNonDelivery),
                source_closure_kind: Some(RuntimeSourceClosureKindV1::SuppressedExactOwnerMismatch),
                source_closure_reason: Some("stale-exact-session".to_string()),
                final_outbox_disposition: Some(FinalOutboxDispositionV1::ExplicitNonDelivery),
                canonical_source_queue_id: None,
                final_delivery_id: None,
                source_final_lane_digest: None,
                reason: receipt_reason.clone(),
            };
            append_runtime_run_once_log(
                &options.harness_home,
                HarnessLogLevel::Warn,
                "runtime.run-once.failure-stale-session-suppressed",
                &receipt,
            )?;
        }
    } else if should_write_failure_outbox(receipt_status) {
        warnings.push(
            "queue channel context was unavailable; runtime failure was not written to outbox"
                .to_string(),
        );
    } else if receipt_status == RuntimeRunOnceStatus::RetryPending {
        if let Some(rollover) = maybe_enqueue_server_overloaded_retry_continuation(
            &options.harness_home,
            prepare.item.as_ref(),
            &run,
            &mut warnings,
        )? {
            receipt_reason = format!(
                "server-overloaded mutation-aware retry requeued into continuation; childQueueId={}; childSessionKey={}; priorReason={}",
                rollover.requeued_queue_id, rollover.new_working_session_key, receipt_reason
            );
            child_queue_id = Some(rollover.requeued_queue_id.clone());
            child_session_key = Some(rollover.new_working_session_key.clone());
            warnings.push(format!(
                "server-overloaded retry enqueued exact-lane continuation queue item {} because mutation evidence was observed",
                rollover.requeued_queue_id
            ));
            receipt_status = RuntimeRunOnceStatus::Skipped;
            retry_schedule = None;
        } else if server_overloaded_after_mutation {
            receipt_reason = format!(
                "Codex server overload occurred after observable mutation, but an exact-lane continuation could not become runnable. Automatic original-prompt replay was suppressed; operator recovery is required. Prior reason: {receipt_reason}"
            );
            warnings.push(
                "mutation-observed overload could not commit a runnable continuation; failing closed"
                    .to_string(),
            );
            receipt_status = RuntimeRunOnceStatus::FailedTerminal;
            retry_schedule = None;
        } else if let Some(rollover) = maybe_enqueue_tool_timeout_retry_continuation(
            &options.harness_home,
            prepare.item.as_ref(),
            &run,
            &mut warnings,
        )? {
            receipt_reason = format!(
                "interrupted long-task retry requeued into continuation; childQueueId={}; childSessionKey={}; priorReason={}",
                rollover.requeued_queue_id, rollover.new_working_session_key, receipt_reason
            );
            child_queue_id = Some(rollover.requeued_queue_id.clone());
            child_session_key = Some(rollover.new_working_session_key.clone());
            warnings.push(format!(
                "interrupted long-task retry enqueued continuation queue item {} after tool-timeout fallback failure",
                rollover.requeued_queue_id
            ));
            receipt_status = RuntimeRunOnceStatus::Skipped;
            retry_schedule = None;
        } else if let Some(rollover) = maybe_enqueue_stream_unstable_retry_continuation(
            &options.harness_home,
            prepare.item.as_ref(),
            &run,
            queue_failure_attempts,
            &mut warnings,
        )? {
            receipt_reason = format!(
                "stream-unstable retry requeued into continuation; childQueueId={}; childSessionKey={}; priorReason={}",
                rollover.requeued_queue_id, rollover.new_working_session_key, receipt_reason
            );
            child_queue_id = Some(rollover.requeued_queue_id.clone());
            child_session_key = Some(rollover.new_working_session_key.clone());
            warnings.push(format!(
                "stream-unstable retry enqueued continuation queue item {} after repeated high-risk Codex stream disconnect",
                rollover.requeued_queue_id
            ));
            receipt_status = RuntimeRunOnceStatus::Skipped;
            retry_schedule = None;
        } else {
            warnings.push(format!(
                "runtime failure for queue item will be retried; attempt {}/{}",
                queue_failure_attempts, retry_policy.policy.max_failure_attempts
            ));
        }
    } else {
        warnings.push(format!(
            "runtime failure for queue item will be retried; attempt {}/{}",
            queue_failure_attempts, retry_policy.policy.max_failure_attempts
        ));
    }

    if server_overloaded_after_mutation
        && receipt_status == RuntimeRunOnceStatus::FailedTerminal
        && outbound_message.is_none()
        && let Some(context) = failure_channel_context
    {
        if channel_session_is_current(&options.harness_home, &context, &mut warnings)? {
            let mut message = ChannelOutboundMessage {
                platform: context.platform.clone(),
                account_id: context.account_id.clone(),
                channel_id: context.channel_id.clone(),
                user_id: context.user_id.clone(),
                session_key: context.session_key.clone(),
                delivery_id: None,
                kind: ChannelOutboundMessageKind::ErrorReply,
                source_queue_id: run.receipt.queue_id.clone(),
                source_completion_file: run.receipt.completion_file.clone(),
                text: runtime_failure_reply_text(
                    receipt_status,
                    &runtime_failure_reply_reason(&run.receipt, &receipt_reason),
                    run.receipt.queue_id.as_deref(),
                ),
                presentation: None,
                delivery_intent: delivery_intent_from_inbound_context(
                    &context.platform,
                    &context.channel_id,
                    context.inbound_context.as_deref(),
                ),
                attachments: Vec::new(),
            };
            let outcome = append_final_outbound_message_once(
                &options.harness_home,
                run.receipt.execution_dir.as_deref(),
                run.receipt.completion_file.as_deref(),
                &mut message,
                &mut warnings,
            )?;
            let appended = outcome.appended();
            outbox_file = Some(outcome.outbox_file.clone());
            final_outbox_disposition = outcome.disposition;
            canonical_source_queue_id = outcome.canonical_source_queue_id;
            final_delivery_id = outcome.delivery_id;
            if appended {
                outbound_message = Some(message);
            }
        } else {
            warnings.push(
                "mutation-observed overload terminal notification suppressed for stale exact session"
                    .to_string(),
            );
        }
    }

    let continuation_link =
        child_queue_id
            .as_ref()
            .map(|child_queue_id| crate::RuntimeContinuationLinkV1 {
                parent_queue_id: run.receipt.queue_id.clone().unwrap_or_default(),
                child_queue_id: child_queue_id.clone(),
                continuation_index: child_session_key
                    .as_deref()
                    .and_then(crate::context_rollover::continuation_index_from_session_key)
                    .unwrap_or_else(|| {
                        prepare.receipt.continuation.continuation_index.unwrap_or(0)
                    }),
                virtual_lane_digest: None,
            });
    if source_closure_decision.is_none() && continuation_link.is_some() {
        source_closure_decision = Some(RuntimeSourceClosureDecision::CommittedHandoff);
        source_closure_reason = Some("deterministic-continuation-committed".to_string());
    }
    if stale_session_explicit_non_delivery {
        source_closure_decision = Some(RuntimeSourceClosureDecision::SuppressedExactOwnerMismatch);
        source_closure_reason = Some("stale-exact-session".to_string());
    }
    if source_closure_decision.is_none() {
        source_closure_decision = match goal_transition.surface {
            GoalTransitionSurface::TerminalNotice | GoalTransitionSurface::CampaignFinal => {
                Some(RuntimeSourceClosureDecision::TerminalGoalSurface)
            }
            GoalTransitionSurface::OrdinaryFinal => {
                Some(RuntimeSourceClosureDecision::OrdinaryFinal)
            }
            GoalTransitionSurface::FailureNotice => {
                Some(RuntimeSourceClosureDecision::TerminalGoalSurface)
            }
            GoalTransitionSurface::SuppressStale => {
                Some(RuntimeSourceClosureDecision::SuppressedExactOwnerMismatch)
            }
            GoalTransitionSurface::ProgressOnly => None,
        };
        if source_closure_decision.is_some() {
            source_closure_reason = Some(
                match goal_transition.surface {
                    GoalTransitionSurface::TerminalNotice
                    | GoalTransitionSurface::CampaignFinal => "terminal-goal-surface",
                    GoalTransitionSurface::OrdinaryFinal => "ordinary-final",
                    GoalTransitionSurface::FailureNotice => "failure-notice",
                    GoalTransitionSurface::SuppressStale => "suppressed-stale-owner",
                    GoalTransitionSurface::ProgressOnly => unreachable!(),
                }
                .to_string(),
            );
        }
    }
    if goal_transition.schedule_continuation
        && goal_transition
            .goal_status
            .as_deref()
            .is_some_and(is_goal_status_active)
        && source_closure_decision.is_none()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "active nonterminal goal has no committed continuation or visible parked outcome",
        ));
    }
    let terminal_disposition = if let Some(decision) = source_closure_decision {
        Some(decision.terminal_disposition())
    } else {
        match receipt_status {
            RuntimeRunOnceStatus::Completed => {
                Some(crate::RuntimeTerminalDispositionV1::LogicalSuccess)
            }
            RuntimeRunOnceStatus::Canceled => {
                Some(crate::RuntimeTerminalDispositionV1::LogicalCanceled)
            }
            RuntimeRunOnceStatus::NeedsUser => Some(crate::RuntimeTerminalDispositionV1::NeedsUser),
            RuntimeRunOnceStatus::ExternalEffectDenied => {
                Some(crate::RuntimeTerminalDispositionV1::LogicalFailure)
            }
            RuntimeRunOnceStatus::DeadLetter
            | RuntimeRunOnceStatus::FailedTerminal
            | RuntimeRunOnceStatus::ProtocolError
            | RuntimeRunOnceStatus::ContextExhausted
            | RuntimeRunOnceStatus::SpawnFailed
            | RuntimeRunOnceStatus::Timeout => {
                Some(crate::RuntimeTerminalDispositionV1::LogicalFailure)
            }
            RuntimeRunOnceStatus::Suppressed | RuntimeRunOnceStatus::Skipped => {
                Some(crate::RuntimeTerminalDispositionV1::TerminalSuppression)
            }
            _ => None,
        }
    };
    let source_final_expectation = if stale_session_explicit_non_delivery {
        final_outbox_disposition = FinalOutboxDispositionV1::ExplicitNonDelivery;
        SourceFinalExpectationV1::ExplicitNonDelivery
    } else if let Some(decision) = source_closure_decision {
        match decision {
            RuntimeSourceClosureDecision::TerminalGoalSurface
            | RuntimeSourceClosureDecision::OrdinaryFinal => {
                if matches!(
                    final_outbox_disposition,
                    FinalOutboxDispositionV1::Appended
                        | FinalOutboxDispositionV1::AlreadyPresent
                        | FinalOutboxDispositionV1::ReusedCanonicalCampaign
                ) {
                    SourceFinalExpectationV1::Required
                } else {
                    final_outbox_disposition = FinalOutboxDispositionV1::ExplicitNonDelivery;
                    SourceFinalExpectationV1::ExplicitNonDelivery
                }
            }
            _ => decision.source_final_expectation(),
        }
    } else {
        SourceFinalExpectationV1::NotApplicable
    };
    let source_final_lane_digest = progress_context.as_ref().and_then(|context| {
        context.agent_id.as_deref().and_then(|agent_id| {
            ChannelStateLane::new(
                &context.platform,
                context.account_id.as_deref(),
                &context.channel_id,
                &context.user_id,
                agent_id,
            )
            .ok()
            .map(|lane| lane.exact_lane_digest())
        })
    });
    let receipt = RuntimeRunOnceReceipt {
        queue_id: run.receipt.queue_id.clone(),
        status: receipt_status,
        runtime_class: prepare.receipt.runtime_class.clone(),
        origin: prepare.receipt.origin.clone(),
        cron_run_id: prepare.receipt.cron_run_id.clone(),
        scheduled_for_ms: prepare.receipt.scheduled_for_ms,
        execution_dir: run.receipt.execution_dir.clone(),
        transcript_file: run.receipt.transcript_file.clone(),
        outbox_file: outbox_file.clone(),
        continuation: prepare.receipt.continuation.clone(),
        child_queue_id,
        child_session_key,
        terminal_disposition,
        continuation_link,
        protocol_failure: run.receipt.protocol_failure.clone(),
        mutation_evidence: run.receipt.mutation_evidence,
        retry_schedule,
        task_drain_evaluation,
        external_effect: run.receipt.external_effect.clone(),
        terminal_control_matched: None,
        terminal_control_source: None,
        suppressed_run_once_reason: None,
        prepared_execution_terminalization_reason: None,
        source_final_expectation: Some(source_final_expectation),
        source_closure_kind: source_closure_decision.map(RuntimeSourceClosureDecision::kind),
        source_closure_reason,
        final_outbox_disposition: Some(final_outbox_disposition),
        canonical_source_queue_id,
        final_delivery_id,
        source_final_lane_digest,
        reason: receipt_reason,
    };
    if matches!(
        run.receipt.status,
        CodexRuntimeRunStatus::Completed
            | CodexRuntimeRunStatus::Timeout
            | CodexRuntimeRunStatus::ContextExhausted
            | CodexRuntimeRunStatus::ProtocolError
            | CodexRuntimeRunStatus::Canceled
    ) && let Some(item) = prepare.item.as_ref()
        && let Some(manifest_file) = item.virtual_skill_manifest_file.as_ref()
        && !item.skill_delivery_receipt_files.is_empty()
    {
        let terminal_eligible = (receipt.status == RuntimeRunOnceStatus::Completed
            && outbound_message.is_some()
            && receipt.child_queue_id.is_none())
            || matches!(
                receipt.status,
                RuntimeRunOnceStatus::DeadLetter | RuntimeRunOnceStatus::FailedTerminal
            );
        let outcome_status = if matches!(
            receipt.status,
            RuntimeRunOnceStatus::DeadLetter | RuntimeRunOnceStatus::FailedTerminal
        ) {
            SkillOutcomeStatusV1::Abandoned
        } else {
            // A model final is not a verifier result.
            SkillOutcomeStatusV1::Unknown
        };
        match capture_skill_episode_runtime_evidence(SkillEpisodeRuntimeCaptureOptions {
            harness_home: options.harness_home.clone(),
            manifest_file: manifest_file.clone(),
            delivery_receipt_files: item.skill_delivery_receipt_files.clone(),
            queue_id: item.queue_id.clone(),
            execution_class: item.runtime_class.clone(),
            source_origin: item.origin.clone(),
            outcome_status,
            verifier_type: None,
            verifier_ref: None,
            correction_ref: None,
            terminal_eligible,
            now_ms: current_log_time_ms()?,
        }) {
            Ok(report) => {
                if let Some(review) = report.terminal_review {
                    warnings.push(format!(
                        "skill terminal review {} recorded in evidence-only mode ({})",
                        review.review_id, review.disposition
                    ));
                }
            }
            Err(error) => warnings.push(format!(
                "skill episode runtime evidence capture failed: {error}"
            )),
        }
    }
    let log_level = match receipt.status {
        RuntimeRunOnceStatus::Completed => HarnessLogLevel::Info,
        RuntimeRunOnceStatus::Timeout
        | RuntimeRunOnceStatus::ProtocolError
        | RuntimeRunOnceStatus::ContextExhausted
        | RuntimeRunOnceStatus::SpawnFailed
        | RuntimeRunOnceStatus::DeadLetter
        | RuntimeRunOnceStatus::FailedTerminal => HarnessLogLevel::Error,
        RuntimeRunOnceStatus::Canceled
        | RuntimeRunOnceStatus::NeedsUser
        | RuntimeRunOnceStatus::ExternalEffectDenied
        | RuntimeRunOnceStatus::Suppressed
        | RuntimeRunOnceStatus::Skipped
        | RuntimeRunOnceStatus::RetryPending
        | RuntimeRunOnceStatus::AuthDeferred
        | RuntimeRunOnceStatus::LeaseBusy => HarnessLogLevel::Warn,
        RuntimeRunOnceStatus::NoWork
        | RuntimeRunOnceStatus::NoPreparedExecution
        | RuntimeRunOnceStatus::NoRuntimePlan
        | RuntimeRunOnceStatus::PreflightBlocked => HarnessLogLevel::Warn,
    };
    let log_event = match receipt.status {
        RuntimeRunOnceStatus::Completed => "runtime.run-once.completed",
        RuntimeRunOnceStatus::NeedsUser => "runtime.run-once.needs-user",
        RuntimeRunOnceStatus::ExternalEffectDenied => "runtime.run-once.external-effect-denied",
        RuntimeRunOnceStatus::Suppressed => "runtime.run-once.suppressed",
        RuntimeRunOnceStatus::Skipped => "runtime.run-once.skipped",
        RuntimeRunOnceStatus::LeaseBusy => "runtime.run-once.lease-busy",
        RuntimeRunOnceStatus::NoWork => "runtime.run-once.no-work",
        RuntimeRunOnceStatus::NoPreparedExecution => "runtime.run-once.no-prepared-execution",
        RuntimeRunOnceStatus::NoRuntimePlan => "runtime.run-once.no-runtime-plan",
        RuntimeRunOnceStatus::PreflightBlocked => "runtime.run-once.preflight-blocked",
        RuntimeRunOnceStatus::AuthDeferred => "runtime.run-once.auth-deferred",
        RuntimeRunOnceStatus::SpawnFailed => "runtime.run-once.spawn-failed",
        RuntimeRunOnceStatus::ProtocolError => "runtime.run-once.protocol-error",
        RuntimeRunOnceStatus::ContextExhausted => "runtime.run-once.context-exhausted",
        RuntimeRunOnceStatus::Timeout => "runtime.run-once.timeout",
        RuntimeRunOnceStatus::RetryPending => "runtime.run-once.retry-pending",
        RuntimeRunOnceStatus::DeadLetter => "runtime.run-once.dead-letter",
        RuntimeRunOnceStatus::FailedTerminal => "runtime.run-once.failed-terminal",
        RuntimeRunOnceStatus::Canceled => "runtime.run-once.canceled",
    };
    append_runtime_run_once_log(&options.harness_home, log_level, log_event, &receipt)?;
    if receipt.status == RuntimeRunOnceStatus::DeadLetter {
        append_runtime_dead_letter_receipt(
            &options.harness_home,
            &RuntimeDeadLetterReceipt {
                schema: RUNTIME_DEAD_LETTER_SCHEMA,
                queue_id: receipt.queue_id.clone(),
                status: receipt.status,
                execution_dir: receipt.execution_dir.clone(),
                transcript_file: receipt.transcript_file.clone(),
                outbox_file: receipt.outbox_file.clone(),
                reason: receipt.reason.clone(),
            },
        )?;
    }
    if outbound_message.is_some()
        && let Some(codex_plan) = plan.plan.as_ref()
        && should_run_self_improvement_hook(receipt.status, &receipt.continuation, Some(codex_plan))
    {
        if let Some(session_key) = channel_context_for_self_improvement
            .as_ref()
            .map(|context| context.session_key.as_str())
        {
            let prompt_sections = count_prompt_bundle_sections(&codex_plan.prompt_bundle_json);
            if let Err(error) = advance_learning_nudge_counters(
                &options.harness_home,
                session_key,
                prompt_sections,
                current_log_time_ms()?,
            ) {
                warnings.push(format!("learning nudge counter update failed: {error}"));
            }
        } else {
            warnings
                .push("learning nudge counter update skipped: no channel session key".to_string());
        }
        let notification_target = channel_context_for_self_improvement
            .as_ref()
            .map(|context| SelfImprovementNotificationTarget {
                platform: context.platform.clone(),
                account_id: context.account_id.clone(),
                channel_id: context.channel_id.clone(),
                user_id: context.user_id.clone(),
                session_key: context.session_key.clone(),
            });
        let assistant_text = outbound_message
            .as_ref()
            .map(|message| message.text.clone())
            .unwrap_or_else(|| receipt.reason.clone());
        match run_self_improvement_review_hook(SelfImprovementReviewHookOptions {
            harness_home: options.harness_home.clone(),
            prompt_bundle_json: codex_plan.prompt_bundle_json.clone(),
            assistant_text,
            queue_id: receipt.queue_id.clone(),
            session_key: channel_context_for_self_improvement
                .as_ref()
                .map(|context| context.session_key.clone()),
            agent_id: codex_plan.agent_id.clone(),
            notification_target,
            tool_call_count: count_transcript_tool_calls(run.receipt.transcript_file.as_deref()),
            now_ms: current_log_time_ms()?,
        }) {
            Ok(report) => warnings.extend(report.warnings),
            Err(error) => warnings.push(format!(
                "self-improvement review hook failed after completed turn: {error}"
            )),
        }
    }
    if let Some(queue_id) = receipt.queue_id.as_deref() {
        if let Err(error) = release_runtime_queue_lease(&options.harness_home, queue_id) {
            warnings.push(format!("runtime queue lease release failed: {error}"));
        }
    }

    let progress_harness_home = options.harness_home.clone();
    let progress_elapsed_ms = run.receipt.elapsed_ms;
    let child_progress = receipt.continuation_link.as_ref().and_then(|link| {
        let parent = progress_context.as_ref()?;
        Some((
            AgentProgressContext {
                queue_id: link.child_queue_id.clone(),
                agent_id: parent.agent_id.clone(),
                account_id: parent.account_id.clone(),
                thread_id: parent.thread_id.clone(),
                session_key: receipt
                    .child_session_key
                    .clone()
                    .unwrap_or_else(|| parent.session_key.clone()),
                platform: parent.platform.clone(),
                channel_id: parent.channel_id.clone(),
                user_id: parent.user_id.clone(),
            },
            link.clone(),
        ))
    });
    let mut report = write_runtime_run_once_report(
        RuntimeRunOnceReport {
            schema: RUNTIME_RUN_ONCE_SCHEMA,
            harness_home: options.harness_home,
            report_file,
            receipts_file,
            receipt,
            prepare: Some(prepare),
            plan: Some(plan),
            run: Some(run),
            outbox_file,
            outbound_message,
            warnings,
        },
        true,
    )?;
    if let Some(context) = &progress_context {
        append_runtime_progress_finished(
            &progress_harness_home,
            context,
            report.receipt.status,
            report.receipt.terminal_disposition,
            &report.receipt.reason,
            progress_elapsed_ms,
            &mut report.warnings,
        );
    }
    if let Some((context, _link)) = child_progress {
        let event = AgentProgressEvent::new(
            &context,
            AgentProgressKind::Runtime,
            "continuation-handoff",
            "Continuation admitted; waiting for the exact session lane.",
            AgentProgressStatus::Started,
            current_log_time_ms().unwrap_or(0),
        )
        .lifecycle(crate::AgentProgressLifecycle::Continuing)
        .source("runtime-pipeline");
        if let Err(error) = crate::append_agent_progress_event(&progress_harness_home, &event) {
            report.warnings.push(format!(
                "durable continuation child progress append failed after parent handoff receipt: {error}"
            ));
        }
    }
    write_json_atomic(&report.report_file, &report)?;
    Ok(report)
}

fn progress_context_from(
    prepare: &RuntimeQueuePrepareReport,
    plan: Option<&CodexRuntimePlan>,
    channel_context: Option<&QueueChannelContext>,
) -> Option<AgentProgressContext> {
    let channel_context = channel_context?;
    let queue_id = prepare
        .receipt
        .queue_id
        .clone()
        .or_else(|| plan.and_then(|plan| plan.queue_id.clone()))?;
    let agent_id = prepare
        .item
        .as_ref()
        .map(|item| item.agent_id.clone())
        .or_else(|| plan.and_then(|plan| plan.agent_id.clone()));
    let runtime_class = prepare
        .item
        .as_ref()
        .map(|item| item.runtime_class.as_str())
        .or(prepare.receipt.runtime_class.as_deref());
    let origin = prepare
        .item
        .as_ref()
        .map(|item| item.origin.as_str())
        .or(prepare.receipt.origin.as_deref());
    let run_session_key = prepare
        .item
        .as_ref()
        .map(|item| item.session_key.as_str())
        .or_else(|| plan.map(|plan| plan.session_key.as_str()));
    if !final_outbox_run_owns_channel_parent(
        runtime_class,
        origin,
        agent_id.as_deref(),
        run_session_key,
        &channel_context.session_key,
    ) {
        return None;
    }
    Some(AgentProgressContext {
        queue_id,
        agent_id,
        account_id: channel_context.account_id.clone(),
        thread_id: channel_context
            .inbound_context
            .as_deref()
            .and_then(|context| context_value(context, "messageThreadId")),
        session_key: channel_context.session_key.clone(),
        platform: channel_context.platform.clone(),
        channel_id: channel_context.channel_id.clone(),
        user_id: channel_context.user_id.clone(),
    })
}

fn progress_context_from_prepared_item(
    item: &RuntimeQueuePreparedItem,
) -> Option<AgentProgressContext> {
    if !final_outbox_run_owns_channel_parent(
        Some(&item.runtime_class),
        Some(&item.origin),
        Some(&item.agent_id),
        Some(&item.session_key),
        &item.session_key,
    ) {
        return None;
    }
    Some(AgentProgressContext {
        queue_id: item.queue_id.clone(),
        agent_id: Some(item.agent_id.clone()),
        account_id: item.account_id.clone(),
        thread_id: item
            .inbound_context
            .as_deref()
            .and_then(|context| context_value(context, "messageThreadId")),
        session_key: item.session_key.clone(),
        platform: item.platform.clone(),
        channel_id: item.channel_id.clone(),
        user_id: item.user_id.clone(),
    })
}

fn append_suppressed_runtime_progress(
    harness_home: &Path,
    prepare: &RuntimeQueuePrepareReport,
    plan: Option<&CodexRuntimePlan>,
    queue_id: &str,
    reason: &str,
    warnings: &mut Vec<String>,
) -> io::Result<()> {
    let progress_context = if let Some(item) = prepare.item.as_ref() {
        progress_context_from_prepared_item(item)
    } else {
        let channel_context = find_queue_channel_context(harness_home, queue_id, warnings)?;
        progress_context_from(prepare, plan, channel_context.as_ref())
    };
    let Some(context) = progress_context else {
        let warning = format!(
            "suppressed runtime progress event skipped because channel context for queue `{queue_id}` could not be resolved"
        );
        warnings.push(warning.clone());
        append_harness_log(
            harness_home,
            &HarnessLogEvent::new(
                current_log_time_ms()?,
                HarnessLogLevel::Warn,
                "runtime-queue",
                "runtime.run-once.suppressed-progress-context-missing",
                format!("queueId={queue_id} warning={warning}"),
            ),
        )?;
        return Ok(());
    };
    append_runtime_progress_finished(
        harness_home,
        &context,
        RuntimeRunOnceStatus::Suppressed,
        Some(crate::RuntimeTerminalDispositionV1::TerminalSuppression),
        reason,
        0,
        warnings,
    );
    Ok(())
}

fn append_runtime_progress_started(
    harness_home: &Path,
    context: &AgentProgressContext,
    prepared: Option<&RuntimeQueuePreparedItem>,
    plan: Option<&CodexRuntimePlan>,
    warnings: &mut Vec<String>,
) {
    let Ok(at_ms) = current_log_time_ms() else {
        warnings.push("progress event timestamp could not be read".to_string());
        return;
    };
    append_progress_nonfatal(
        harness_home,
        AgentProgressEvent::new(
            context,
            AgentProgressKind::Todo,
            "todo",
            "planning 1 task(s)",
            AgentProgressStatus::Completed,
            at_ms,
        )
        .source("runtime-pipeline"),
        warnings,
    );
    if let Some(prepared) = prepared {
        if prepared.selected_skill_ids.is_empty() {
            append_progress_nonfatal(
                harness_home,
                AgentProgressEvent::new(
                    context,
                    AgentProgressKind::SkillView,
                    "skill_view",
                    "no matched skills",
                    AgentProgressStatus::Completed,
                    at_ms,
                )
                .source("runtime-pipeline"),
                warnings,
            );
        } else {
            for skill_id in &prepared.selected_skill_ids {
                append_progress_nonfatal(
                    harness_home,
                    AgentProgressEvent::new(
                        context,
                        AgentProgressKind::SkillView,
                        "skill_view",
                        skill_id,
                        AgentProgressStatus::Completed,
                        at_ms,
                    )
                    .source("runtime-pipeline"),
                    warnings,
                );
            }
        }
    }
    if let Some(plan) = plan {
        let mut command = plan.invocation.executable.display().to_string();
        if !plan.invocation.arguments.is_empty() {
            command.push(' ');
            command.push_str(&plan.invocation.arguments.join(" "));
        }
        append_progress_nonfatal(
            harness_home,
            AgentProgressEvent::new(
                context,
                AgentProgressKind::Terminal,
                "terminal",
                command,
                AgentProgressStatus::Started,
                at_ms,
            )
            .source("runtime-pipeline"),
            warnings,
        );
    }
}

fn append_runtime_progress_finished(
    harness_home: &Path,
    context: &AgentProgressContext,
    status: RuntimeRunOnceStatus,
    disposition: Option<crate::RuntimeTerminalDispositionV1>,
    reason: &str,
    elapsed_ms: u128,
    warnings: &mut Vec<String>,
) {
    let Ok(at_ms) = current_log_time_ms() else {
        warnings.push("progress event timestamp could not be read".to_string());
        return;
    };
    let progress_status = match status {
        RuntimeRunOnceStatus::Completed | RuntimeRunOnceStatus::Suppressed => {
            AgentProgressStatus::Completed
        }
        RuntimeRunOnceStatus::Skipped => AgentProgressStatus::Completed,
        RuntimeRunOnceStatus::RetryPending
        | RuntimeRunOnceStatus::AuthDeferred
        | RuntimeRunOnceStatus::NeedsUser => AgentProgressStatus::Progress,
        _ => AgentProgressStatus::Failed,
    };
    let preview = runtime_progress_preview(status, reason);
    let label = match disposition {
        Some(crate::RuntimeTerminalDispositionV1::ContinuationHandoff) => "continuation-handoff",
        Some(crate::RuntimeTerminalDispositionV1::LogicalCanceled) => "canceled",
        Some(crate::RuntimeTerminalDispositionV1::NeedsUser) => "waiting-for-approval",
        Some(crate::RuntimeTerminalDispositionV1::TerminalSuppression) => "terminal-suppression",
        _ => "run",
    };
    let mut event = AgentProgressEvent::new(
        context,
        AgentProgressKind::Runtime,
        label,
        preview,
        progress_status,
        at_ms,
    )
    .elapsed_ms(elapsed_ms)
    .source("runtime-pipeline");
    if disposition == Some(crate::RuntimeTerminalDispositionV1::ContinuationHandoff) {
        event = event.lifecycle(crate::AgentProgressLifecycle::Continuing);
    } else if disposition == Some(crate::RuntimeTerminalDispositionV1::NeedsUser) {
        event = event.lifecycle(crate::AgentProgressLifecycle::WaitingForApproval);
    }
    append_progress_nonfatal(harness_home, event, warnings);
}

fn runtime_progress_preview(status: RuntimeRunOnceStatus, reason: &str) -> String {
    match status {
        RuntimeRunOnceStatus::Completed => "done".to_string(),
        RuntimeRunOnceStatus::NeedsUser => {
            "connector action is waiting for exact, explicit approval".to_string()
        }
        RuntimeRunOnceStatus::ExternalEffectDenied => {
            "connector action was denied and will not be retried".to_string()
        }
        RuntimeRunOnceStatus::Suppressed => "suppressed by terminal control".to_string(),
        RuntimeRunOnceStatus::Skipped => "skipped because continuation work was queued".to_string(),
        RuntimeRunOnceStatus::RetryPending => {
            "transient runtime failure; preserving session for retry".to_string()
        }
        RuntimeRunOnceStatus::AuthDeferred => {
            "waiting for operator authentication; preserving queued work".to_string()
        }
        RuntimeRunOnceStatus::DeadLetter if is_retryable_codex_protocol_error(reason) => {
            "transient Codex stream disconnect exhausted retry budget; moved to dead-letter"
                .to_string()
        }
        RuntimeRunOnceStatus::ContextExhausted => {
            "Codex context exhausted; compact recovery failed or required manual recovery"
                .to_string()
        }
        _ => reason.to_string(),
    }
}

fn append_progress_nonfatal(
    harness_home: &Path,
    event: AgentProgressEvent,
    warnings: &mut Vec<String>,
) {
    if let Err(error) = append_agent_progress_event(harness_home, &event) {
        warnings.push(format!("progress event write failed: {error}"));
    }
}

fn map_run_once_status(status: CodexRuntimeRunStatus) -> RuntimeRunOnceStatus {
    match status {
        CodexRuntimeRunStatus::Completed => RuntimeRunOnceStatus::Completed,
        CodexRuntimeRunStatus::ApprovalRequired => RuntimeRunOnceStatus::NeedsUser,
        CodexRuntimeRunStatus::ExternalEffectDenied => RuntimeRunOnceStatus::ExternalEffectDenied,
        CodexRuntimeRunStatus::PreflightBlocked => RuntimeRunOnceStatus::PreflightBlocked,
        CodexRuntimeRunStatus::NoRuntimePlan => RuntimeRunOnceStatus::NoRuntimePlan,
        CodexRuntimeRunStatus::SpawnFailed => RuntimeRunOnceStatus::SpawnFailed,
        CodexRuntimeRunStatus::ProtocolError => RuntimeRunOnceStatus::ProtocolError,
        CodexRuntimeRunStatus::ContextExhausted => RuntimeRunOnceStatus::ContextExhausted,
        CodexRuntimeRunStatus::Timeout => RuntimeRunOnceStatus::Timeout,
        CodexRuntimeRunStatus::Canceled => RuntimeRunOnceStatus::Canceled,
    }
}

fn runtime_failure_reply_text(
    status: RuntimeRunOnceStatus,
    reason: &str,
    queue_id: Option<&str>,
) -> String {
    let queue_line = queue_id
        .map(|queue_id| format!("\nQueue: {queue_id}"))
        .unwrap_or_default();
    if status == RuntimeRunOnceStatus::NeedsUser {
        return format!(
            "Waiting for approval.{queue_line}\n{}\n\nThis exact connector action is parked. Generic timeout recovery will not replay it.",
            truncate_for_channel(reason, 720),
        );
    }
    if status == RuntimeRunOnceStatus::ExternalEffectDenied {
        return format!(
            "Connector action denied.{queue_line}\n{}\n\nThis approval generation is terminal and will not be retried.",
            truncate_for_channel(reason, 720),
        );
    }
    if status == RuntimeRunOnceStatus::Canceled {
        if reason.contains("interrupted_by_new_turn") {
            return format!(
                "This run was interrupted by a newer turn before the in-flight command produced an exit code.{queue_line}\nReason: {}\n\nUse the continuation context to inspect the interrupted command evidence and resume or rerun only verification-safe commands.",
                truncate_for_channel(reason, 360),
            );
        }
        return "Stopped.".to_string();
    }
    if status == RuntimeRunOnceStatus::FailedTerminal {
        return format!(
            "Agent harness could not process this request and marked it failed-terminal.{queue_line}\nReason: {}\n\nGateway restart will not resume a terminal queue item. Use /status runtime to inspect the queue.",
            truncate_for_channel(reason, 360),
        );
    }
    if status == RuntimeRunOnceStatus::ContextExhausted {
        return format!(
            "This session reached the Codex context limit, and automatic compact recovery did not complete.{queue_line}\nReason: {}\n\nUse /status runtime to inspect the queue. Start a fresh session or retry after manual recovery.",
            truncate_for_channel(reason, 360),
        );
    }
    if status == RuntimeRunOnceStatus::DeadLetter {
        return format!(
            "Agent harness retried this request and moved it to dead-letter.{queue_line}\nReason: {}\n\nSession context is preserved; use queue-retry with the queue id to create a fresh retry.",
            truncate_for_channel(reason, 360),
        );
    }
    format!(
        "Agent harness runtime error: {:?}{queue_line}\nReason: {}\n\nUse /status security to check approvals and sandbox policy.",
        status,
        truncate_for_channel(reason, 360)
    )
}

fn approval_request_presentation(
    intent: &crate::ExternalEffectIntentV1,
) -> io::Result<(String, RichMessagePresentation)> {
    let token = intent.approval_token.as_ref().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "approval-required external effect is missing protected token metadata",
        )
    })?;
    let prompt = ChannelApprovalPromptV1::new(
        intent.effect_id.clone(),
        intent.approval_generation,
        intent.source_session_key_digest.clone(),
        intent.approval_authority_digest.clone(),
        intent.action_summary.clone(),
        token.expires_at_ms,
    )
    .map_err(io::Error::other)?;
    let approve = prompt
        .actions
        .iter()
        .find(|action| action.decision == crate::ChannelApprovalDecisionV1::Approve)
        .ok_or_else(|| io::Error::other("approval prompt has no approve action"))?;
    let deny = prompt
        .actions
        .iter()
        .find(|action| action.decision == crate::ChannelApprovalDecisionV1::Deny)
        .ok_or_else(|| io::Error::other("approval prompt has no deny action"))?;
    let text = format!(
        "Approval required: {}\n\nApprove: /approve {}\nDeny: /deny {}\nExpires at: {}",
        prompt.action_summary,
        approve.public_action_id,
        deny.public_action_id,
        prompt.expires_at_ms
    );
    let mut presentation = rich_presentation_from_plain_final_with_attachment_count(&text, 0)
        .ok_or_else(|| io::Error::other("approval prompt could not be rendered"))?;
    presentation.actions = prompt
        .actions
        .into_iter()
        .map(|action| RichPresentationAction {
            id: action.public_action_id,
            label: action.label,
            kind: RichPresentationActionKind::Callback,
            url: None,
        })
        .collect();
    Ok((text, presentation))
}

#[allow(clippy::too_many_arguments)]
fn runtime_retry_schedule_for(
    status: RuntimeRunOnceStatus,
    queue_id: Option<&str>,
    attempt: usize,
    max_attempts: usize,
    delay_ms: i64,
    scheduled_at_ms: i64,
    protocol_failure: Option<&crate::CodexProtocolFailureV1>,
    mutation_evidence: Option<crate::RuntimeMutationEvidenceClass>,
) -> Option<crate::RuntimeRetryScheduleV1> {
    (status == RuntimeRunOnceStatus::RetryPending).then(|| {
        let replay_mode = if matches!(
            protocol_failure.map(|failure| failure.class),
            Some(crate::CodexProtocolFailureClass::ServerOverloaded)
        ) && mutation_evidence
            == Some(crate::RuntimeMutationEvidenceClass::NoObservableMutation)
        {
            crate::RuntimeRetryReplayModeV1::SameRequestNoObservableMutation
        } else {
            crate::RuntimeRetryReplayModeV1::SameQueueLegacyPolicy
        };
        crate::RuntimeRetryScheduleV1 {
            lineage_id: format!("runtime-retry:{}", queue_id.unwrap_or("unattributed")),
            attempt,
            max_attempts,
            delay_ms,
            scheduled_at_ms,
            next_eligible_at_ms: scheduled_at_ms.saturating_add(delay_ms),
            replay_mode,
        }
    })
}

fn final_run_once_status_with_protocol(
    codex_status: CodexRuntimeRunStatus,
    failure_attempts: usize,
    reason: &str,
    max_failure_attempts: usize,
    protocol_failure: Option<&crate::CodexProtocolFailureV1>,
    mutation_evidence: Option<crate::RuntimeMutationEvidenceClass>,
) -> RuntimeRunOnceStatus {
    if codex_status == CodexRuntimeRunStatus::ProtocolError
        && let Some(protocol_failure) = protocol_failure
    {
        use crate::CodexProtocolFailureClass as Class;
        use crate::RuntimeMutationEvidenceClass as Evidence;
        return match protocol_failure.class {
            Class::ServerOverloaded => match mutation_evidence {
                Some(Evidence::NoObservableMutation) | Some(Evidence::MutationObserved)
                    if failure_attempts < max_failure_attempts =>
                {
                    RuntimeRunOnceStatus::RetryPending
                }
                Some(Evidence::NoObservableMutation) | Some(Evidence::MutationObserved) => {
                    RuntimeRunOnceStatus::DeadLetter
                }
                Some(Evidence::Unknown) | None => RuntimeRunOnceStatus::FailedTerminal,
            },
            Class::RateLimited => match mutation_evidence {
                Some(Evidence::NoObservableMutation) if failure_attempts < max_failure_attempts => {
                    RuntimeRunOnceStatus::RetryPending
                }
                Some(Evidence::NoObservableMutation) => RuntimeRunOnceStatus::DeadLetter,
                Some(Evidence::MutationObserved | Evidence::Unknown) | None => {
                    RuntimeRunOnceStatus::FailedTerminal
                }
            },
            Class::StreamDisconnected if failure_attempts < max_failure_attempts => {
                RuntimeRunOnceStatus::RetryPending
            }
            Class::StreamDisconnected => RuntimeRunOnceStatus::DeadLetter,
            Class::ContextExhausted => RuntimeRunOnceStatus::ContextExhausted,
            Class::Authentication
            | Class::Configuration
            | Class::InvalidRequest
            | Class::Unknown => RuntimeRunOnceStatus::FailedTerminal,
        };
    }
    match codex_status {
        CodexRuntimeRunStatus::Completed => RuntimeRunOnceStatus::Completed,
        CodexRuntimeRunStatus::ApprovalRequired => RuntimeRunOnceStatus::NeedsUser,
        CodexRuntimeRunStatus::ExternalEffectDenied => RuntimeRunOnceStatus::ExternalEffectDenied,
        CodexRuntimeRunStatus::Canceled => RuntimeRunOnceStatus::Canceled,
        CodexRuntimeRunStatus::PreflightBlocked if reason.contains("needs-operator-auth") => {
            RuntimeRunOnceStatus::AuthDeferred
        }
        CodexRuntimeRunStatus::Timeout if failure_attempts < max_failure_attempts => {
            RuntimeRunOnceStatus::RetryPending
        }
        CodexRuntimeRunStatus::Timeout => RuntimeRunOnceStatus::DeadLetter,
        CodexRuntimeRunStatus::ContextExhausted => RuntimeRunOnceStatus::ContextExhausted,
        CodexRuntimeRunStatus::ProtocolError
            if is_retryable_codex_protocol_error(reason)
                && failure_attempts < max_failure_attempts =>
        {
            RuntimeRunOnceStatus::RetryPending
        }
        CodexRuntimeRunStatus::ProtocolError if is_retryable_codex_protocol_error(reason) => {
            RuntimeRunOnceStatus::DeadLetter
        }
        CodexRuntimeRunStatus::ProtocolError
            if is_external_review_evidence_protocol_error(reason)
                && failure_attempts < max_failure_attempts =>
        {
            RuntimeRunOnceStatus::RetryPending
        }
        CodexRuntimeRunStatus::ProtocolError
            if is_external_review_evidence_protocol_error(reason) =>
        {
            RuntimeRunOnceStatus::DeadLetter
        }
        CodexRuntimeRunStatus::PreflightBlocked
        | CodexRuntimeRunStatus::NoRuntimePlan
        | CodexRuntimeRunStatus::SpawnFailed
        | CodexRuntimeRunStatus::ProtocolError => RuntimeRunOnceStatus::FailedTerminal,
    }
}

#[cfg(test)]
fn final_run_once_status(
    codex_status: CodexRuntimeRunStatus,
    failure_attempts: usize,
    reason: &str,
    max_failure_attempts: usize,
) -> RuntimeRunOnceStatus {
    final_run_once_status_with_protocol(
        codex_status,
        failure_attempts,
        reason,
        max_failure_attempts,
        None,
        None,
    )
}

fn is_retryable_codex_protocol_error(reason: &str) -> bool {
    let lower = reason.to_ascii_lowercase();
    lower.contains("stream disconnected before completion")
        || lower.contains("websocket closed by server before response.completed")
        || lower.contains("reconnecting...")
}

fn is_stream_unstable_codex_protocol_error(reason: &str) -> bool {
    let lower = reason.to_ascii_lowercase();
    lower.contains("stream disconnected before completion")
        || lower.contains("websocket closed by server before response.completed")
}

fn is_external_review_evidence_protocol_error(reason: &str) -> bool {
    reason
        .to_ascii_lowercase()
        .contains("external review evidence without parent workflow completion")
}

fn final_run_once_reason(
    receipt_status: RuntimeRunOnceStatus,
    codex_status: CodexRuntimeRunStatus,
    failure_attempts: usize,
    max_failure_attempts: usize,
    reason: &str,
) -> String {
    match receipt_status {
        RuntimeRunOnceStatus::RetryPending => format!(
            "runtime queue item transient failure attempt {failure_attempts}/{max_failure_attempts}; last codex status={codex_status:?}; reason: {reason}"
        ),
        RuntimeRunOnceStatus::DeadLetter => format!(
            "runtime queue item dead-lettered after {failure_attempts} attempt(s); last codex status={codex_status:?}; reason: {reason}"
        ),
        RuntimeRunOnceStatus::FailedTerminal => format!(
            "runtime queue item failed terminally after {failure_attempts} attempt(s); last codex status={codex_status:?}; reason: {reason}"
        ),
        RuntimeRunOnceStatus::ContextExhausted => format!(
            "runtime queue item reached Codex context limit after {failure_attempts} attempt(s); compact recovery did not complete; last codex status={codex_status:?}; reason: {reason}"
        ),
        RuntimeRunOnceStatus::Canceled => {
            format!("runtime queue item was canceled by operator request; reason: {reason}")
        }
        RuntimeRunOnceStatus::Skipped => {
            format!(
                "runtime queue item was skipped because continuation work was queued; reason: {reason}"
            )
        }
        _ => reason.to_string(),
    }
}

fn runtime_failure_reply_reason(
    receipt: &crate::CodexRuntimeRunReceipt,
    fallback_reason: &str,
) -> String {
    if receipt.status == CodexRuntimeRunStatus::Canceled
        && receipt.interruption_reason.as_deref() == Some("interrupted_by_new_turn")
    {
        let tool_summary = receipt
            .interrupted_tool_uses
            .iter()
            .take(3)
            .map(|tool| {
                let preview = tool.preview.as_deref().unwrap_or("(no preview)");
                let action = if tool.safe_to_rerun {
                    "verification-rerun-eligible"
                } else {
                    "explicit-review-required"
                };
                format!("{} [{action}]", truncate_for_channel(preview, 120))
            })
            .collect::<Vec<_>>()
            .join("; ");
        let mut reason = format!("interrupted_by_new_turn: {fallback_reason}");
        if !tool_summary.is_empty() {
            reason.push_str("; interruptedToolUses=");
            reason.push_str(&tool_summary);
        }
        return reason;
    }
    fallback_reason.to_string()
}

fn should_write_failure_outbox(status: RuntimeRunOnceStatus) -> bool {
    matches!(
        status,
        RuntimeRunOnceStatus::NeedsUser
            | RuntimeRunOnceStatus::ExternalEffectDenied
            | RuntimeRunOnceStatus::DeadLetter
            | RuntimeRunOnceStatus::FailedTerminal
            | RuntimeRunOnceStatus::Canceled
            | RuntimeRunOnceStatus::NoRuntimePlan
            | RuntimeRunOnceStatus::PreflightBlocked
            | RuntimeRunOnceStatus::SpawnFailed
            | RuntimeRunOnceStatus::ProtocolError
            | RuntimeRunOnceStatus::ContextExhausted
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FinalOutboxInputKind {
    AgentReply,
    ReviewEvidence,
    InternalEvidence,
    #[allow(dead_code)]
    ProgressStatus,
    TerminalError,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FinalOutboxDisposition {
    UserFacingFinal,
    InternalEvidenceOnly,
    ProgressOnly,
    FailureNotice,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FinalOutboxDecision {
    disposition: FinalOutboxDisposition,
    outbound_kind: Option<ChannelOutboundMessageKind>,
    attach_plain_final_presentation: bool,
}

impl FinalOutboxDecision {
    fn may_write_final_outbox(self) -> bool {
        self.outbound_kind.is_some()
    }
}

fn final_outbox_decision(input: FinalOutboxInputKind) -> FinalOutboxDecision {
    match input {
        FinalOutboxInputKind::AgentReply => FinalOutboxDecision {
            disposition: FinalOutboxDisposition::UserFacingFinal,
            outbound_kind: Some(ChannelOutboundMessageKind::AgentReply),
            attach_plain_final_presentation: true,
        },
        FinalOutboxInputKind::ReviewEvidence | FinalOutboxInputKind::InternalEvidence => {
            FinalOutboxDecision {
                disposition: FinalOutboxDisposition::InternalEvidenceOnly,
                outbound_kind: None,
                attach_plain_final_presentation: false,
            }
        }
        FinalOutboxInputKind::ProgressStatus => FinalOutboxDecision {
            disposition: FinalOutboxDisposition::ProgressOnly,
            outbound_kind: None,
            attach_plain_final_presentation: false,
        },
        FinalOutboxInputKind::TerminalError => FinalOutboxDecision {
            disposition: FinalOutboxDisposition::FailureNotice,
            outbound_kind: Some(ChannelOutboundMessageKind::ErrorReply),
            attach_plain_final_presentation: false,
        },
    }
}

fn final_outbox_input_kind_for_completed_response(
    runtime_class: Option<&str>,
    origin: Option<&str>,
    agent_id: Option<&str>,
    run_session_key: Option<&str>,
    channel_session_key: &str,
    user_message: Option<&str>,
    final_text: &str,
) -> FinalOutboxInputKind {
    if !final_outbox_run_owns_channel_parent(
        runtime_class,
        origin,
        agent_id,
        run_session_key,
        channel_session_key,
    ) {
        return FinalOutboxInputKind::InternalEvidence;
    }
    if implementation_request_requires_parent_completion(user_message)
        && looks_like_read_only_review_evidence(final_text)
    {
        return FinalOutboxInputKind::ReviewEvidence;
    }
    FinalOutboxInputKind::AgentReply
}

fn final_outbox_input_kind_for_terminal_error(
    runtime_class: Option<&str>,
    origin: Option<&str>,
    agent_id: Option<&str>,
    run_session_key: Option<&str>,
    channel_session_key: &str,
) -> FinalOutboxInputKind {
    if final_outbox_run_owns_channel_parent(
        runtime_class,
        origin,
        agent_id,
        run_session_key,
        channel_session_key,
    ) {
        FinalOutboxInputKind::TerminalError
    } else {
        FinalOutboxInputKind::InternalEvidence
    }
}

fn final_outbox_run_owns_channel_parent(
    runtime_class: Option<&str>,
    origin: Option<&str>,
    agent_id: Option<&str>,
    run_session_key: Option<&str>,
    channel_session_key: &str,
) -> bool {
    !runtime_class.is_some_and(|value| value != "interactive")
        && !origin.is_some_and(|value| !runtime_origin_is_parent_channel(value))
        && final_outbox_owner_is_channel_parent(agent_id, run_session_key, channel_session_key)
}

fn final_outbox_owner_is_channel_parent(
    agent_id: Option<&str>,
    run_session_key: Option<&str>,
    channel_session_key: &str,
) -> bool {
    if let Some(run_session_key) = run_session_key
        && run_session_key != channel_session_key
    {
        return false;
    }
    let agent_id = agent_id.map(str::trim).filter(|value| !value.is_empty());
    if let Some(lane_agent) = session_key_agent_segment(channel_session_key) {
        if let Some(agent_id) = agent_id {
            return agent_id == lane_agent;
        }
        return true;
    }
    if let Some(agent_id) = agent_id {
        return agent_id == "main";
    }
    true
}

fn runtime_origin_is_parent_channel(origin: &str) -> bool {
    matches!(origin, "channel" | "coordinator-resume")
}

fn implementation_request_requires_parent_completion(user_message: Option<&str>) -> bool {
    let Some(message) = user_message else {
        return false;
    };
    let lower = message.to_ascii_lowercase();
    let has_ascii_action = lower
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .any(|word| {
            matches!(
                word,
                "implement" | "finish" | "complete" | "cutover" | "ship"
            )
        });
    has_ascii_action
        || lower.contains("battle-set")
        || message.contains("開 goal")
        || message.contains("開goal")
        || message.contains("戰定")
        || message.contains("實作")
        || message.contains("開發")
        || message.contains("落實")
        || message.contains("完成")
}

fn looks_like_read_only_review_evidence(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    lower.contains("read-only inspection only")
        || lower.contains("read-only review only")
        || (lower.contains("no files changed") && lower.contains("no tests run"))
        || (lower.contains("recommended seam") && lower.contains("fail-first tests"))
        || (lower.contains("dirty worktree risks") && lower.contains("read-only"))
        || (lower.contains("audit complete")
            && (lower.contains("sent root") || lower.contains("sent to root")))
}

fn continuation_candidate_for_run(
    harness_home: &Path,
    item: Option<&RuntimeQueuePreparedItem>,
    queue_id: Option<&str>,
    label: &str,
    warnings: &mut Vec<String>,
) -> io::Result<Option<RuntimeContinuationCandidate>> {
    let candidate = if let Some(item) = item {
        Some(RuntimeContinuationCandidate::from_prepared_item(item))
    } else {
        let Some(queue_id) = queue_id else {
            warnings.push(format!(
                "{label} skipped: runtime run receipt had no queue id"
            ));
            return Ok(None);
        };
        let recovered = read_pending_continuation_candidate(harness_home, queue_id, warnings)?;
        if recovered.is_none() {
            warnings.push(format!(
                "{label} skipped: queue item metadata was unavailable for {queue_id}"
            ));
        }
        recovered
    };
    let Some(mut candidate) = candidate else {
        return Ok(None);
    };
    if let (Some(platform), Some(channel_id), Some(user_id), Some(agent_id)) = (
        candidate.platform.as_deref(),
        candidate.channel_id.as_deref(),
        candidate.user_id.as_deref(),
        candidate.agent_id.as_deref(),
    ) {
        match crate::channel_session_key::CanonicalChannelSessionKey::bind_for_lane(
            &candidate.session_key,
            platform,
            candidate.account_id.as_deref().unwrap_or("default"),
            channel_id,
            user_id,
            agent_id,
        ) {
            Ok(canonical) => {
                let canonical_session_key = canonical.canonical_string();
                if canonical_session_key != candidate.session_key {
                    warnings.push(format!(
                        "{label} normalized legacy queue session `{}` to exact-lane canonical session `{canonical_session_key}`",
                        candidate.session_key
                    ));
                    candidate.session_key = canonical_session_key;
                }
            }
            Err(error) => {
                warnings.push(format!(
                    "{label} skipped: queue session key failed exact-lane canonicalization: {error}"
                ));
                return Ok(None);
            }
        }
    }
    Ok(Some(candidate))
}

fn read_pending_continuation_candidate(
    harness_home: &Path,
    queue_id: &str,
    warnings: &mut Vec<String>,
) -> io::Result<Option<RuntimeContinuationCandidate>> {
    let queue_file = harness_home
        .join("state")
        .join("runtime-queue")
        .join("pending.jsonl");
    let text = match fs::read_to_string(&queue_file) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    for (index, line) in text.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        match serde_json::from_str::<PendingContinuationCandidateRecord>(trimmed) {
            Ok(record) if record.queue_id == queue_id => return Ok(Some(record.into())),
            Ok(_) => {}
            Err(error) => warnings.push(format!(
                "pending queue item line {} could not be read for continuation metadata: {}",
                index + 1,
                error
            )),
        }
    }
    Ok(None)
}

fn maybe_enqueue_server_overloaded_retry_continuation(
    harness_home: &Path,
    item: Option<&RuntimeQueuePreparedItem>,
    run: &CodexRuntimeRunReport,
    warnings: &mut Vec<String>,
) -> io::Result<Option<ContextRolloverPreparedRequeueReport>> {
    if !matches!(
        run.receipt
            .protocol_failure
            .as_ref()
            .map(|failure| failure.class),
        Some(crate::CodexProtocolFailureClass::ServerOverloaded)
    ) || run.receipt.mutation_evidence
        != Some(crate::RuntimeMutationEvidenceClass::MutationObserved)
    {
        return Ok(None);
    }
    let Some(item) = continuation_candidate_for_run(
        harness_home,
        item,
        run.receipt.queue_id.as_deref(),
        "server-overloaded mutation-aware retry continuation",
        warnings,
    )?
    else {
        return Ok(None);
    };
    if item.runtime_class != "interactive" || !runtime_origin_is_parent_channel(&item.origin) {
        return Ok(None);
    }
    let config = load_context_rollover_config(harness_home)?;
    if config.rollover_mode == ContextRolloverMode::Disabled {
        warnings.push(
            "server-overloaded retry continuation skipped: context rollover disabled".to_string(),
        );
        return Ok(None);
    }
    let current_index = item.continuation.continuation_index.unwrap_or(0);
    if current_index >= config.max_continuation_depth {
        warnings.push(format!(
            "server-overloaded retry continuation skipped: continuation depth {} reached configured limit {}",
            current_index, config.max_continuation_depth
        ));
        mark_virtual_session_terminal_after_depth_limit(
            harness_home,
            &item,
            config.max_continuation_depth,
            warnings,
        )?;
        return Ok(None);
    }
    let next_index = current_index.saturating_add(1);
    let root_session = root_working_session_key(&item.session_key);
    let new_working_session_key = continuation_session_key(&root_session, next_index);
    let now_ms = current_log_time_ms()?;
    match requeue_prepared_context_rollover_if_no_parent_siblings(
        ContextRolloverRequeuePreparedOptions {
            harness_home: harness_home.to_path_buf(),
            queue_id: item.queue_id.clone(),
            new_working_session_key,
            reason: format!(
                "mutation-aware server-overloaded continuation; codexErrorInfo={}; priorReason={}",
                run.receipt
                    .protocol_failure
                    .as_ref()
                    .and_then(|failure| failure.codex_error_info.as_deref())
                    .unwrap_or("serverOverloaded"),
                run.receipt.reason
            ),
            now_ms,
            preserve_continuation_index: false,
            campaign_slice_generation: None,
            task_slice_generation: None,
            task_family_id: None,
            task_family_version: None,
            task_root_queue_id: None,
            disposition_recovery_depth: None,
            shell_recovery_depth: None,
            replacement_message_text: None,
            continuation_intent_key: None,
            completion_kind: None,
            allow_exact_state_bootstrap: false,
        },
    ) {
        Ok(report) => Ok(Some(report)),
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::InvalidData | io::ErrorKind::NotFound
            ) =>
        {
            warnings.push(format!(
                "server-overloaded retry continuation skipped: {error}"
            ));
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

fn maybe_enqueue_tool_timeout_retry_continuation(
    harness_home: &Path,
    item: Option<&RuntimeQueuePreparedItem>,
    run: &CodexRuntimeRunReport,
    warnings: &mut Vec<String>,
) -> io::Result<Option<ContextRolloverPreparedRequeueReport>> {
    let Some(item) = continuation_candidate_for_run(
        harness_home,
        item,
        run.receipt.queue_id.as_deref(),
        "tool-timeout retry continuation",
        warnings,
    )?
    else {
        return Ok(None);
    };
    if item.runtime_class != "interactive" || !runtime_origin_is_parent_channel(&item.origin) {
        return Ok(None);
    }
    if !context_recovery_indicates_interrupted_long_task(run) {
        return Ok(None);
    }
    let config = load_context_rollover_config(harness_home)?;
    if config.rollover_mode == ContextRolloverMode::Disabled {
        warnings
            .push("tool-timeout retry continuation skipped: context rollover disabled".to_string());
        return Ok(None);
    }
    let current_index = item.continuation.continuation_index.unwrap_or(0);
    if current_index >= config.max_continuation_depth {
        warnings.push(format!(
            "tool-timeout retry continuation skipped: continuation depth {} reached configured limit {}",
            current_index, config.max_continuation_depth
        ));
        mark_virtual_session_terminal_after_depth_limit(
            harness_home,
            &item,
            config.max_continuation_depth,
            warnings,
        )?;
        return Ok(None);
    }
    let next_index = current_index.saturating_add(1);
    let root_session = root_working_session_key(&item.session_key);
    let new_working_session_key = continuation_session_key(&root_session, next_index);
    let now_ms = current_log_time_ms()?;
    match requeue_prepared_context_rollover_if_no_parent_siblings(
        ContextRolloverRequeuePreparedOptions {
            harness_home: harness_home.to_path_buf(),
            queue_id: item.queue_id.clone(),
            new_working_session_key,
            reason: interrupted_long_task_rollover_reason("retry-pending", run),
            now_ms,
            preserve_continuation_index: false,
            campaign_slice_generation: None,
            task_slice_generation: None,
            task_family_id: None,
            task_family_version: None,
            task_root_queue_id: None,
            disposition_recovery_depth: None,
            shell_recovery_depth: None,
            replacement_message_text: None,
            continuation_intent_key: None,
            completion_kind: None,
            allow_exact_state_bootstrap: false,
        },
    ) {
        Ok(report) => Ok(Some(report)),
        Err(error) if error.kind() == io::ErrorKind::InvalidData => {
            warnings.push(format!("tool-timeout retry continuation skipped: {error}"));
            Ok(None)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            warnings.push(format!("tool-timeout retry continuation skipped: {error}"));
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

fn maybe_enqueue_polluted_thread_continuation(
    harness_home: &Path,
    item: Option<&RuntimeQueuePreparedItem>,
    run: &CodexRuntimeRunReport,
    receipt_status: RuntimeRunOnceStatus,
    warnings: &mut Vec<String>,
) -> io::Result<Option<ContextRolloverPreparedRequeueReport>> {
    if !matches!(
        receipt_status,
        RuntimeRunOnceStatus::FailedTerminal | RuntimeRunOnceStatus::DeadLetter
    ) {
        return Ok(None);
    }
    let Some(item) = continuation_candidate_for_run(
        harness_home,
        item,
        run.receipt.queue_id.as_deref(),
        "polluted thread recovery",
        warnings,
    )?
    else {
        return Ok(None);
    };
    if item.runtime_class != "interactive" || !runtime_origin_is_parent_channel(&item.origin) {
        return Ok(None);
    }
    let interrupted_long_task = context_recovery_indicates_interrupted_long_task(run);
    if !context_recovery_indicates_polluted_thread(run.receipt.context_recovery.as_ref())
        && !interrupted_long_task
    {
        return Ok(None);
    }
    let config = load_context_rollover_config(harness_home)?;
    if config.rollover_mode == ContextRolloverMode::Disabled {
        warnings.push("polluted thread recovery skipped: context rollover disabled".to_string());
        return Ok(None);
    }
    let current_index = item.continuation.continuation_index.unwrap_or(0);
    if current_index >= config.max_continuation_depth {
        warnings.push(format!(
            "polluted thread recovery skipped: continuation depth {} reached configured limit {}",
            current_index, config.max_continuation_depth
        ));
        mark_virtual_session_terminal_after_depth_limit(
            harness_home,
            &item,
            config.max_continuation_depth,
            warnings,
        )?;
        return Ok(None);
    }
    let next_index = current_index.saturating_add(1);
    let root_session = root_working_session_key(&item.session_key);
    let new_working_session_key = continuation_session_key(&root_session, next_index);
    let now_ms = current_log_time_ms()?;
    match requeue_prepared_context_rollover_if_no_parent_siblings(
        ContextRolloverRequeuePreparedOptions {
            harness_home: harness_home.to_path_buf(),
            queue_id: item.queue_id.clone(),
            new_working_session_key,
            reason: if interrupted_long_task {
                interrupted_long_task_rollover_reason(receipt_status.as_str(), run)
            } else {
                format!(
                    "automatic polluted-thread virtual session recovery after terminal {}; codexStatus={:?}; reason={}",
                    receipt_status.as_str(),
                    run.receipt.status,
                    run.receipt.reason
                )
            },
            now_ms,
            preserve_continuation_index: false,
            campaign_slice_generation: None,
            task_slice_generation: None,
            task_family_id: None,
            task_family_version: None,
            task_root_queue_id: None,
            disposition_recovery_depth: None,
            shell_recovery_depth: None,
            replacement_message_text: None,
            continuation_intent_key: None,
            completion_kind: None,
            allow_exact_state_bootstrap: false,
        },
    ) {
        Ok(report) => Ok(Some(report)),
        Err(error) if error.kind() == io::ErrorKind::InvalidData => {
            warnings.push(format!("polluted thread recovery skipped: {error}"));
            Ok(None)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            warnings.push(format!("polluted thread recovery skipped: {error}"));
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

fn maybe_enqueue_stream_unstable_retry_continuation(
    harness_home: &Path,
    item: Option<&RuntimeQueuePreparedItem>,
    run: &CodexRuntimeRunReport,
    failure_attempts: usize,
    warnings: &mut Vec<String>,
) -> io::Result<Option<ContextRolloverPreparedRequeueReport>> {
    let Some(item) = continuation_candidate_for_run(
        harness_home,
        item,
        run.receipt.queue_id.as_deref(),
        "stream-unstable retry continuation",
        warnings,
    )?
    else {
        return Ok(None);
    };
    let config = load_context_rollover_config(harness_home)?;
    if !should_enqueue_stream_unstable_retry_continuation(
        &item,
        run,
        failure_attempts,
        config.stream_unstable_continuation_min_attempts,
        config.stream_unstable_continuation_token_limit,
    ) {
        return Ok(None);
    }
    if config.rollover_mode == ContextRolloverMode::Disabled {
        warnings.push(
            "stream-unstable retry continuation skipped: context rollover disabled".to_string(),
        );
        return Ok(None);
    }
    let current_index = item.continuation.continuation_index.unwrap_or(0);
    if current_index >= config.max_continuation_depth {
        warnings.push(format!(
            "stream-unstable retry continuation skipped: continuation depth {} reached configured limit {}",
            current_index, config.max_continuation_depth
        ));
        mark_virtual_session_terminal_after_depth_limit(
            harness_home,
            &item,
            config.max_continuation_depth,
            warnings,
        )?;
        return Ok(None);
    }
    let next_index = current_index.saturating_add(1);
    let root_session = root_working_session_key(&item.session_key);
    let new_working_session_key = continuation_session_key(&root_session, next_index);
    let now_ms = current_log_time_ms()?;
    match requeue_prepared_context_rollover_if_no_parent_siblings(
        ContextRolloverRequeuePreparedOptions {
            harness_home: harness_home.to_path_buf(),
            queue_id: item.queue_id.clone(),
            new_working_session_key,
            reason: format!(
                "automatic stream-unstable virtual session recovery after retry-pending attempt {}; codexStatus={:?}; inputTokens={:?}; totalTokens={:?}; mediaArtifacts={}; nativeMediaParts={}; inboundMediaArtifacts={}; reason={}",
                failure_attempts,
                run.receipt.status,
                run.receipt
                    .usage
                    .as_ref()
                    .and_then(|usage| usage.input_tokens),
                run.receipt
                    .usage
                    .as_ref()
                    .and_then(|usage| usage.total_tokens),
                run.receipt.media_plan.artifacts.len(),
                run.receipt.media_plan.native_input_parts.len(),
                item.inbound_media_artifacts.len(),
                run.receipt.reason
            ),
            now_ms,
            preserve_continuation_index: false,
            campaign_slice_generation: None,
            task_slice_generation: None,
            task_family_id: None,
            task_family_version: None,
            task_root_queue_id: None,
            disposition_recovery_depth: None,
            shell_recovery_depth: None,
            replacement_message_text: None,
            continuation_intent_key: None,
            completion_kind: None,
            allow_exact_state_bootstrap: false,
        },
    ) {
        Ok(report) => Ok(Some(report)),
        Err(error) if error.kind() == io::ErrorKind::InvalidData => {
            warnings.push(format!(
                "stream-unstable retry continuation skipped: {error}"
            ));
            Ok(None)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            warnings.push(format!(
                "stream-unstable retry continuation skipped: {error}"
            ));
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

fn mark_virtual_session_terminal_after_depth_limit(
    harness_home: &Path,
    item: &RuntimeContinuationCandidate,
    max_continuation_depth: u64,
    warnings: &mut Vec<String>,
) -> io::Result<()> {
    let ended_by = format!("max-continuation-depth:{max_continuation_depth}");
    let now_ms = current_log_time_ms()?;
    let v2_lane = exact_channel_state_lane_for_continuation(item);
    let result = if let Some(channel_lane) = v2_lane {
        mark_virtual_session_terminal_for_lane(VirtualSessionTerminalV2Options {
            harness_home: harness_home.to_path_buf(),
            channel_lane,
            session_key: item.session_key.clone(),
            ended_by,
            now_ms,
        })
    } else {
        mark_virtual_session_terminal(VirtualSessionTerminalOptions {
            harness_home: harness_home.to_path_buf(),
            session_key: item.session_key.clone(),
            ended_by,
            now_ms,
        })
    };
    match result {
        Ok(Some(file)) => warnings.push(format!(
            "virtual session marked terminal after continuation depth limit: {}",
            file.display()
        )),
        Ok(None) => warnings.push(
            "virtual session terminal mark skipped: no working-set record found for session"
                .to_string(),
        ),
        Err(error) => warnings.push(format!(
            "virtual session terminal mark failed after continuation depth limit: {error}"
        )),
    }
    Ok(())
}

fn exact_channel_state_lane_for_continuation(
    item: &RuntimeContinuationCandidate,
) -> Option<ChannelStateLane> {
    ChannelStateLane::new(
        item.platform.as_deref()?,
        item.account_id.as_deref(),
        item.channel_id.as_deref()?,
        item.user_id.as_deref()?,
        item.agent_id.as_deref()?,
    )
    .ok()
}

#[derive(Debug, Clone)]
struct RuntimeGoalAuthorityContext {
    lane_digest: Option<String>,
    virtual_session_id: Option<String>,
    projection_hint: Option<GoalProjectionHint>,
}

fn establish_runtime_goal_authority_before_outbox(
    harness_home: &Path,
    item: Option<&RuntimeQueuePreparedItem>,
    codex_plan: Option<&CodexRuntimePlan>,
    run: &CodexRuntimeRunReport,
    receipt_status: RuntimeRunOnceStatus,
    run_once_receipts_file: &Path,
    warnings: &mut Vec<String>,
) -> RuntimeGoalAuthorityContext {
    let projection_hint =
        run.receipt.queue_id.as_deref().and_then(
            |queue_id| match latest_goal_projection_for_queue(harness_home, queue_id) {
                Ok(value) => value,
                Err(error) => {
                    warnings.push(format!(
                        "goal transition projection hint failed closed: {error}"
                    ));
                    None
                }
            },
        );
    let Some(item) = item else {
        return RuntimeGoalAuthorityContext {
            lane_digest: None,
            virtual_session_id: None,
            projection_hint,
        };
    };
    let full_lane = match crate::lane::FullLaneKeyV1::new(
        item.platform.clone(),
        item.account_id
            .clone()
            .unwrap_or_else(|| "default".to_string()),
        item.channel_id.clone(),
        item.user_id.clone(),
        item.agent_id.clone(),
        item.runtime_class.clone(),
        root_working_session_key(&item.session_key),
        item.session_key.clone(),
    ) {
        Ok(lane) => lane,
        Err(error) => {
            warnings.push(format!(
                "goal transition exact full-lane construction failed closed: {error}"
            ));
            return RuntimeGoalAuthorityContext {
                lane_digest: None,
                virtual_session_id: None,
                projection_hint,
            };
        }
    };
    let lane_digest = match full_lane.identity_hash() {
        Ok(value) => value,
        Err(error) => {
            warnings.push(format!(
                "goal transition exact full-lane digest failed closed: {error}"
            ));
            return RuntimeGoalAuthorityContext {
                lane_digest: None,
                virtual_session_id: None,
                projection_hint,
            };
        }
    };
    let plan_generation =
        codex_plan.and_then(|plan| plan.prompt_authority.backend_context_generation.as_deref());
    let active_exact_projection = projection_hint.as_ref().is_some_and(|projection| {
        projection.projection_complete
            && is_goal_status_active(&projection.status)
            && projection.session_key == item.session_key
            && projection.lane_digest.as_deref() == Some(lane_digest.as_str())
            && projection.backend_context_generation.as_deref() == plan_generation
    });
    let should_snapshot =
        run.receipt.status == CodexRuntimeRunStatus::Completed || active_exact_projection;
    if !should_snapshot {
        return RuntimeGoalAuthorityContext {
            lane_digest: Some(lane_digest),
            virtual_session_id: None,
            projection_hint,
        };
    }
    let channel_lane = match ChannelStateLane::new(
        &item.platform,
        item.account_id.as_deref(),
        &item.channel_id,
        &item.user_id,
        &item.agent_id,
    ) {
        Ok(lane) => lane,
        Err(error) => {
            warnings.push(format!(
                "goal transition exact channel lane failed closed: {error}"
            ));
            return RuntimeGoalAuthorityContext {
                lane_digest: Some(lane_digest),
                virtual_session_id: None,
                projection_hint,
            };
        }
    };
    match record_completed_turn_working_set_snapshot_for_lane(
        CompletedTurnWorkingSetSnapshotV2Options {
            harness_home: harness_home.to_path_buf(),
            channel_lane,
            working_session_key: item.session_key.clone(),
            queue_id: run.receipt.queue_id.clone(),
            message_text: Some(item.message_text.clone()),
            status: receipt_status.as_str().to_string(),
            run_once_receipt_file: Some(run_once_receipts_file.to_path_buf()),
            outbox_file: None,
            completion_file: run.receipt.completion_file.clone(),
            now_ms: current_log_time_ms().unwrap_or(0),
        },
    ) {
        Ok(snapshot) => {
            if let Err(error) =
                record_virtual_session_authority(harness_home, item, codex_plan, &snapshot)
            {
                warnings.push(format!(
                    "pre-outbox exact-lane virtual-session authority receipt failed: {error}"
                ));
                return RuntimeGoalAuthorityContext {
                    lane_digest: Some(lane_digest),
                    virtual_session_id: None,
                    projection_hint,
                };
            }
            RuntimeGoalAuthorityContext {
                lane_digest: Some(lane_digest),
                virtual_session_id: Some(snapshot.virtual_session_id),
                projection_hint,
            }
        }
        Err(error) => {
            warnings.push(format!(
                "pre-outbox exact-lane working-set authority failed closed: {error}"
            ));
            RuntimeGoalAuthorityContext {
                lane_digest: Some(lane_digest),
                virtual_session_id: None,
                projection_hint,
            }
        }
    }
}

fn evaluate_runtime_goal_transition(
    harness_home: &Path,
    run: &CodexRuntimeRunReport,
    receipt_status: RuntimeRunOnceStatus,
    continuation: &RuntimeContinuationMetadata,
    authority_context: &RuntimeGoalAuthorityContext,
    warnings: &mut Vec<String>,
) -> io::Result<GoalTransitionReceiptV1> {
    let report = run_goal_lineage_doctor(GoalLineageDoctorOptions {
        harness_home: harness_home.to_path_buf(),
        lane_digest: authority_context.lane_digest.clone(),
        virtual_session_id: authority_context.virtual_session_id.clone(),
    });
    let mut authority;
    let mut selected = None;
    let mut older_than_authoritative_lineage = false;
    match report {
        Ok(report) => {
            let current_queue = run.receipt.queue_id.as_deref();
            let current_selected = report
                .lineages
                .iter()
                .filter(|lineage| lineage.queue_id.as_deref() == current_queue)
                .max_by_key(|lineage| (lineage.observed_at_ms, lineage.observation_order))
                .cloned();
            older_than_authoritative_lineage = current_selected.as_ref().is_some_and(|current| {
                report.lineages.iter().any(|candidate| {
                    candidate.campaign_family_id == current.campaign_family_id
                        && candidate.lineage_id != current.lineage_id
                        && (candidate.observed_at_ms, candidate.observation_order)
                            > (current.observed_at_ms, current.observation_order)
                })
            });
            selected = current_selected.or_else(|| {
                report
                    .lineages
                    .iter()
                    .filter(|lineage| {
                        is_goal_status_active(&lineage.goal_status) && lineage.runnable
                    })
                    .max_by_key(|lineage| (lineage.observed_at_ms, lineage.observation_order))
                    .cloned()
            });
            authority = match report.status {
                GoalLineageDoctorStatus::ReconciliationRequired => {
                    GoalTransitionAuthority::Conflict
                }
                GoalLineageDoctorStatus::Blocked => GoalTransitionAuthority::Invalid,
                GoalLineageDoctorStatus::Ready => selected
                    .as_ref()
                    .map(|lineage| match lineage.disposition {
                        GoalLineageDisposition::Runnable | GoalLineageDisposition::Inactive
                            if lineage.source_final_eligible
                                && matches!(
                                    lineage.turn_relation,
                                    GoalProjectionTurnRelationV1::CurrentOwnedTurn
                                        | GoalProjectionTurnRelationV1::ExplicitCampaignContinuation
                                ) =>
                        {
                            GoalTransitionAuthority::Ready
                        }
                        GoalLineageDisposition::Runnable | GoalLineageDisposition::Inactive => {
                            GoalTransitionAuthority::Missing
                        }
                        GoalLineageDisposition::StaleGeneration => GoalTransitionAuthority::Stale,
                        GoalLineageDisposition::MissingAuthority => {
                            GoalTransitionAuthority::Missing
                        }
                        GoalLineageDisposition::InvalidIdentity => GoalTransitionAuthority::Invalid,
                        GoalLineageDisposition::Superseded => GoalTransitionAuthority::Conflict,
                    })
                    .unwrap_or(GoalTransitionAuthority::NotApplicable),
                GoalLineageDoctorStatus::Empty => GoalTransitionAuthority::NotApplicable,
            };
            if older_than_authoritative_lineage {
                authority = GoalTransitionAuthority::Stale;
            }
        }
        Err(error) => {
            warnings.push(format!(
                "goal transition lineage doctor failed closed: {error}"
            ));
            authority = GoalTransitionAuthority::Invalid;
        }
    }
    let hint = authority_context.projection_hint.as_ref();
    if selected.is_none() && hint.is_some() && authority == GoalTransitionAuthority::NotApplicable {
        authority = GoalTransitionAuthority::Missing;
    }
    let event = if older_than_authoritative_lineage {
        GoalTransitionEventKind::OlderCompletion
    } else {
        classify_goal_transition_event(run, receipt_status)
    };
    let relation = selected
        .as_ref()
        .map(|lineage| match lineage.turn_relation {
            GoalProjectionTurnRelationV1::CurrentOwnedTurn => {
                GoalTransitionRelation::CurrentGoalSlice
            }
            GoalProjectionTurnRelationV1::ExplicitCampaignContinuation => {
                GoalTransitionRelation::AuthorizedCampaignContinuation
            }
            GoalProjectionTurnRelationV1::HistoricalThreadState
            | GoalProjectionTurnRelationV1::AuthoritativeGoalClosure => {
                GoalTransitionRelation::HistoricalState
            }
            GoalProjectionTurnRelationV1::Uncorrelated => GoalTransitionRelation::Unproven,
        })
        .or_else(|| {
            hint.map(|projection| match projection.turn_relation {
                GoalProjectionTurnRelationV1::CurrentOwnedTurn => {
                    GoalTransitionRelation::CurrentGoalSlice
                }
                GoalProjectionTurnRelationV1::ExplicitCampaignContinuation => {
                    GoalTransitionRelation::AuthorizedCampaignContinuation
                }
                GoalProjectionTurnRelationV1::HistoricalThreadState
                | GoalProjectionTurnRelationV1::AuthoritativeGoalClosure => {
                    GoalTransitionRelation::HistoricalState
                }
                GoalProjectionTurnRelationV1::Uncorrelated => GoalTransitionRelation::Unproven,
            })
        })
        .unwrap_or(GoalTransitionRelation::FreshUnrelatedTurn);
    let goal_status = selected
        .as_ref()
        .map(|lineage| lineage.goal_status.clone())
        .or_else(|| hint.map(|projection| projection.status.clone()));
    let lineage_id = selected.as_ref().map(|lineage| lineage.lineage_id.clone());
    let campaign_family_id = selected
        .as_ref()
        .map(|lineage| lineage.campaign_family_id.clone());
    let lane_digest = selected
        .as_ref()
        .and_then(|lineage| lineage.lane_digest.clone())
        .or_else(|| authority_context.lane_digest.clone());
    let virtual_session_id = selected
        .as_ref()
        .and_then(|lineage| lineage.virtual_session_id.clone())
        .or_else(|| authority_context.virtual_session_id.clone());
    let backend_context_generation = selected
        .as_ref()
        .and_then(|lineage| lineage.backend_context_generation.clone())
        .or_else(|| hint.and_then(|projection| projection.backend_context_generation.clone()));
    let source_thread_id = selected
        .as_ref()
        .map(|lineage| lineage.source_thread_id.clone())
        .or_else(|| hint.map(|projection| projection.source_thread_id.clone()));
    let source_turn_id = selected
        .as_ref()
        .and_then(|lineage| lineage.source_turn_id.clone())
        .or_else(|| hint.and_then(|projection| projection.source_turn_id.clone()));
    let goal_checksum = selected
        .as_ref()
        .map(|lineage| lineage.goal_checksum.clone())
        .or_else(|| hint.map(|projection| projection.goal_checksum.clone()));
    let source_slice_generation = continuation.campaign_slice_generation.unwrap_or(0);
    if let Some(existing) = find_existing_goal_transition(
        harness_home,
        run.receipt.queue_id.as_deref(),
        event,
        receipt_status,
        source_slice_generation,
        source_turn_id.as_deref(),
        goal_checksum.as_deref(),
    )? {
        return Ok(existing);
    }
    let campaign_budget = if goal_status.as_deref().is_some_and(is_goal_status_active) {
        match (
            campaign_family_id.as_deref(),
            lane_digest.as_deref(),
            virtual_session_id.as_deref(),
        ) {
            (Some(campaign_family_id), Some(lane_digest), Some(virtual_session_id)) => {
                let policy = load_goal_campaign_policy(harness_home)?;
                let receipt = evaluate_goal_campaign_budget(
                    harness_home,
                    &policy,
                    GoalCampaignBudgetInput {
                        campaign_family_id,
                        lane_digest,
                        virtual_session_id,
                        queue_id: run.receipt.queue_id.as_deref(),
                        goal_checksum: goal_checksum.as_deref(),
                        source_slice_generation,
                        event,
                        slice_elapsed_ms: run.receipt.elapsed_ms.min(u128::from(u64::MAX)) as u64,
                        slice_tokens: run
                            .receipt
                            .usage
                            .as_ref()
                            .and_then(|usage| usage.total_tokens)
                            .unwrap_or(0),
                        output_tokens: run
                            .receipt
                            .usage
                            .as_ref()
                            .and_then(|usage| usage.output_tokens)
                            .unwrap_or(0),
                        event_count: run.receipt.event_count,
                        runtime_status: receipt_status.as_str(),
                        recovery_slice: run.receipt.context_recovery.is_some(),
                        now_ms: current_log_time_ms()?,
                    },
                )?;
                warnings.push(format!(
                    "goal campaign budget boundary {:?}: slices={} tokens={} noProgress={} recoveries={} elapsed={}ms",
                    receipt.boundary,
                    receipt.slices_observed,
                    receipt.total_tokens_observed,
                    receipt.consecutive_no_progress_slices,
                    receipt.recovery_slices_observed,
                    receipt.campaign_elapsed_ms
                ));
                Some(receipt)
            }
            _ => {
                warnings.push(
                    "goal campaign budget evaluation skipped because exact campaign authority is incomplete"
                        .to_string(),
                );
                None
            }
        }
    } else {
        None
    };
    let decision_generation =
        next_goal_transition_generation(harness_home, run.receipt.queue_id.as_deref())?;
    let receipt = evaluate_goal_transition(GoalTransitionInput {
        queue_id: run.receipt.queue_id.clone(),
        event,
        runtime_status: receipt_status.as_str().to_string(),
        runtime_reason: run.receipt.reason.clone(),
        goal_status,
        lineage_id,
        campaign_family_id,
        lane_digest,
        virtual_session_id,
        backend_context_generation,
        source_thread_id,
        source_turn_id,
        goal_checksum,
        source_slice_generation,
        decision_generation,
        authority,
        relation,
        retryable_failure: receipt_status == RuntimeRunOnceStatus::RetryPending,
        context_rollover_required: context_recovery_indicates_interrupted_long_task(run)
            || context_recovery_indicates_polluted_thread(run.receipt.context_recovery.as_ref()),
        budget_exhausted: campaign_budget
            .as_ref()
            .is_some_and(|receipt| receipt.budget_exhausted),
        no_progress_exhausted: campaign_budget
            .as_ref()
            .is_some_and(|receipt| receipt.no_progress_exhausted),
        observed_at_ms: current_log_time_ms()?,
    });
    record_goal_transition(harness_home, &receipt)?;
    Ok(receipt)
}

fn classify_goal_transition_event(
    run: &CodexRuntimeRunReport,
    receipt_status: RuntimeRunOnceStatus,
) -> GoalTransitionEventKind {
    if run.receipt.status == CodexRuntimeRunStatus::PreflightBlocked
        && run.receipt.reason.contains("needs-operator-auth")
    {
        return GoalTransitionEventKind::AuthDeferred;
    }
    if run.receipt.status == CodexRuntimeRunStatus::Canceled {
        return if run.receipt.interruption_reason.as_deref() == Some("interrupted_by_new_turn") {
            GoalTransitionEventKind::NewerSteer
        } else {
            GoalTransitionEventKind::OperatorStop
        };
    }
    if run.receipt.event_count == 0 && run.receipt.reason.contains("already recorded") {
        return GoalTransitionEventKind::ProcessRestart;
    }
    if run.receipt.status == CodexRuntimeRunStatus::Completed {
        return if run.receipt.reason.contains("deadline drain") {
            GoalTransitionEventKind::DrainCompletion
        } else {
            GoalTransitionEventKind::NormalCompletion
        };
    }
    if run.receipt.context_recovery.is_some() {
        return GoalTransitionEventKind::CompactRollover;
    }
    if run.receipt.status == CodexRuntimeRunStatus::Timeout {
        if run.receipt.tool_use_timeout.is_some() {
            return GoalTransitionEventKind::ToolTimeout;
        }
        if run.receipt.reason.contains("JSONL event") || run.receipt.reason.contains("inactivity") {
            return GoalTransitionEventKind::IdleTimeout;
        }
        return GoalTransitionEventKind::AbsoluteTimeout;
    }
    let _ = receipt_status;
    GoalTransitionEventKind::RuntimeFailure
}

fn next_goal_transition_generation(harness_home: &Path, queue_id: Option<&str>) -> io::Result<u64> {
    let file = crate::goal_transition::goal_transition_receipts_file(harness_home);
    let text = match fs::read_to_string(file) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(1),
        Err(error) => return Err(error),
    };
    let count = text
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|value| value.get("queueId").and_then(Value::as_str) == queue_id)
        .count() as u64;
    Ok(count.saturating_add(1))
}

fn latest_productive_deadline_generation(
    harness_home: &Path,
    queue_id: Option<&str>,
) -> io::Result<u32> {
    let Some(queue_id) = queue_id else {
        return Ok(0);
    };
    let file = harness_home
        .join("state")
        .join("runtime-queue")
        .join("productive-deadline-decisions.jsonl");
    let text = match fs::read_to_string(file) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error),
    };
    let mut generation = 0_u32;
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let value: Value = serde_json::from_str(line).map_err(io::Error::other)?;
        if string_field(&value, &["queueId", "queue_id"]) == Some(queue_id)
            && value.get("decision").and_then(Value::as_str) == Some("applied")
        {
            generation = generation.max(
                value
                    .get("deadlineGeneration")
                    .or_else(|| value.get("deadline_generation"))
                    .and_then(Value::as_u64)
                    .and_then(|value| u32::try_from(value).ok())
                    .unwrap_or(0),
            );
        }
    }
    Ok(generation)
}

#[allow(clippy::too_many_arguments)]
fn schedule_disposition_recovery_child(
    harness_home: &Path,
    item: Option<&RuntimeQueuePreparedItem>,
    goal_transition: &GoalTransitionReceiptV1,
    goal_authority: &RuntimeGoalAuthorityContext,
    evaluation: Option<&crate::TaskDrainEvaluationV1>,
    child_queue_id: &mut Option<String>,
    child_session_key: &mut Option<String>,
    receipt_status: &mut RuntimeRunOnceStatus,
    receipt_reason: &mut String,
) -> io::Result<()> {
    let item = item.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "indeterminate task drain has no prepared queue item",
        )
    })?;
    let recovery_depth = item.continuation.disposition_recovery_depth.unwrap_or(0);
    if recovery_depth >= crate::DEFAULT_MAX_DISPOSITION_RECOVERY {
        return Ok(());
    }
    let root_queue_id = item
        .continuation
        .task_root_queue_id
        .as_deref()
        .unwrap_or(&item.queue_id);
    let family =
        crate::find_task_family_for_root_queue(harness_home, root_queue_id)?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "indeterminate drain has no harness-owned task family",
            )
        })?;
    let prepared_virtual_session_id = goal_authority
        .virtual_session_id
        .clone()
        .or_else(|| item.continuation.virtual_session_id.clone())
        .or_else(|| {
            ChannelStateLane::new(
                &item.platform,
                item.account_id.as_deref(),
                &item.channel_id,
                &item.user_id,
                &item.agent_id,
            )
            .ok()
            .map(|lane| {
                crate::context_rollover::derive_virtual_session_id_v2(
                    &lane,
                    &root_working_session_key(&item.session_key),
                )
            })
        });
    if item
        .continuation
        .task_family_id
        .as_deref()
        .is_some_and(|value| value != family.family_id)
        || item
            .continuation
            .task_family_version
            .is_some_and(|value| value != family.authority_version)
        || goal_authority.lane_digest.as_deref() != Some(family.exact_lane_digest.as_str())
        || prepared_virtual_session_id.as_deref() != Some(family.virtual_session_id.as_str())
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "indeterminate drain task-family authority does not match the exact run lane",
        ));
    }
    let progress_digest = crate::task_transition::sha256_hex(
        evaluation
            .map(|evaluation| evaluation.reason.as_bytes())
            .unwrap_or_default(),
    );
    let authority = crate::ContinuationAuthoritySnapshotV1 {
        kind: crate::ContinuationAuthorityKindV1::ExplicitCheckpoint,
        authority_id: family.family_id.clone(),
        authority_version: family.authority_version,
        authority_checksum: crate::task_family_checksum(&family)?,
        exact_lane_digest: family.exact_lane_digest.clone(),
        virtual_session_id: family.virtual_session_id.clone(),
        active_item_id: None,
        active_item_version: None,
        checkpoint_digest: progress_digest,
    };
    let now_ms = current_log_time_ms()?;
    let intent = crate::commit_task_continuation_intent(
        harness_home,
        &item.queue_id,
        &item.session_key,
        &item.continuation,
        &authority,
        item.continuation.task_slice_generation.unwrap_or(0),
        goal_transition.decision_generation,
        true,
        now_ms,
    )?;
    let enqueued =
        crate::ensure_goal_continuation_enqueued(harness_home, &intent.intent_key, now_ms)?;
    *child_queue_id = enqueued.child_queue_id.clone();
    *child_session_key = Some(enqueued.working_session_key.clone());
    *receipt_status = RuntimeRunOnceStatus::Skipped;
    *receipt_reason = format!(
        "indeterminate drain yielded to one observation-only disposition recovery child {}; childQueueId={}",
        enqueued.intent_key,
        enqueued.child_queue_id.as_deref().unwrap_or("missing")
    );
    Ok(())
}

fn item_disposition_recovery_depth(item: Option<&RuntimeQueuePreparedItem>) -> u64 {
    item.and_then(|item| item.continuation.disposition_recovery_depth)
        .unwrap_or(0)
}

fn task_drain_notice_text(
    evaluation: &crate::TaskDrainEvaluationV1,
    recovery_depth: u64,
) -> String {
    match evaluation.disposition {
        crate::DrainDispositionV1::NeedsUser => evaluation.reason.clone(),
        crate::DrainDispositionV1::NeedsAuthority => {
            "The task is parked because it needs additional authority or approval. Review the pending request, then send a new message to continue."
                .to_string()
        }
        crate::DrainDispositionV1::Blocked => {
            "The task is parked because a safety or budget breaker was reached. Review the current state, then send a new message to continue."
                .to_string()
        }
        crate::DrainDispositionV1::Indeterminate if recovery_depth > 0 => {
            "The task is parked because the bounded disposition-recovery pass could not establish a valid final outcome. Please send a new message with the desired next step."
                .to_string()
        }
        crate::DrainDispositionV1::Indeterminate => {
            "The task is parked because its deadline-drain outcome could not be verified. Please send a new message with the desired next step."
                .to_string()
        }
        _ => {
            "The checkpointed task could not continue because its exact authority changed. Review the current state, then send a new message to continue."
                .to_string()
        }
    }
}

fn find_existing_goal_transition(
    harness_home: &Path,
    queue_id: Option<&str>,
    event: GoalTransitionEventKind,
    runtime_status: RuntimeRunOnceStatus,
    source_slice_generation: u64,
    source_turn_id: Option<&str>,
    goal_checksum: Option<&str>,
) -> io::Result<Option<GoalTransitionReceiptV1>> {
    let file = crate::goal_transition::goal_transition_receipts_file(harness_home);
    let text = match fs::read_to_string(file) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    Ok(text
        .lines()
        .filter_map(|line| serde_json::from_str::<GoalTransitionReceiptV1>(line).ok())
        .filter(|receipt| {
            receipt.queue_id.as_deref() == queue_id
                && receipt.event == event
                && receipt.runtime_status == runtime_status.as_str()
                && receipt.source_slice_generation == source_slice_generation
                && receipt.source_turn_id.as_deref() == source_turn_id
                && receipt.goal_checksum.as_deref() == goal_checksum
        })
        .last())
}

fn is_goal_status_active(status: &str) -> bool {
    matches!(
        status
            .trim()
            .to_ascii_lowercase()
            .replace(['-', '_', ' '], "")
            .as_str(),
        "active" | "running" | "inprogress"
    )
}

fn record_virtual_session_authority(
    harness_home: &Path,
    item: &RuntimeQueuePreparedItem,
    codex_plan: Option<&CodexRuntimePlan>,
    snapshot: &crate::context_rollover::CompletedTurnWorkingSetSnapshotReport,
) -> io::Result<()> {
    let full_lane = crate::lane::FullLaneKeyV1::new(
        item.platform.clone(),
        item.account_id
            .clone()
            .unwrap_or_else(|| "default".to_string()),
        item.channel_id.clone(),
        item.user_id.clone(),
        item.agent_id.clone(),
        item.runtime_class.clone(),
        root_working_session_key(&item.session_key),
        item.session_key.clone(),
    )
    .map_err(io::Error::other)?;
    let lane_digest = full_lane.identity_hash().map_err(io::Error::other)?;
    let backend_context_generation =
        codex_plan.and_then(|plan| plan.prompt_authority.backend_context_generation.clone());
    let generation_bound = backend_context_generation.is_some();
    let receipt = VirtualSessionAuthorityReceiptV1 {
        schema: VIRTUAL_SESSION_AUTHORITY_SCHEMA,
        queue_id: item.queue_id.clone().into(),
        virtual_session_id: snapshot.virtual_session_id.clone(),
        working_session_key: snapshot.working_session_key.clone(),
        lane_digest,
        backend_context_generation,
        working_set_file: snapshot.working_set_file.clone(),
        status: if generation_bound {
            "authoritative-v2".to_string()
        } else {
            "generation-unbound".to_string()
        },
        reason: if generation_bound {
            "production completion wrote exact-account v2 working-set state bound to the prepared backend generation".to_string()
        } else {
            "production completion wrote exact-account v2 working-set state but no trusted backend generation was available; autonomy must fail closed".to_string()
        },
        updated_at_ms: current_log_time_ms()?,
    };
    append_json_line(
        &harness_home
            .join("state")
            .join("context-rollover")
            .join("virtual-session-authority-receipts.jsonl"),
        &receipt,
    )
}

fn interrupted_long_task_rollover_reason(status: &str, run: &CodexRuntimeRunReport) -> String {
    let tool = run
        .receipt
        .tool_use_timeout
        .as_ref()
        .map(tool_timeout_summary)
        .unwrap_or_else(|| "tool=(none)".to_string());
    format!(
        "automatic interrupted long-task virtual session recovery after {}; codexStatus={:?}; {}; reason={}",
        status, run.receipt.status, tool, run.receipt.reason
    )
}

fn context_recovery_indicates_interrupted_long_task(run: &CodexRuntimeRunReport) -> bool {
    let receipt = &run.receipt;
    let recovery_status = receipt
        .context_recovery
        .as_ref()
        .map(|recovery| recovery.status.as_str());
    if recovery_status == Some("tool-timeout-fallback-failed") {
        return true;
    }
    if receipt.tool_use_timeout.is_some()
        && matches!(
            receipt.status,
            CodexRuntimeRunStatus::Timeout | CodexRuntimeRunStatus::ProtocolError
        )
    {
        return true;
    }
    if receipt.status == CodexRuntimeRunStatus::Timeout
        && receipt
            .reason
            .to_ascii_lowercase()
            .contains("timed out waiting for codex app-server completion after")
        && codex_stdout_has_productive_progress(run.stdout_log.as_deref())
    {
        return true;
    }
    receipt
        .reason
        .to_ascii_lowercase()
        .contains(crate::codex_runtime::NO_FINAL_ANSWER_WITH_NARRATION_MARKER)
}

fn codex_stdout_has_productive_progress(path: Option<&Path>) -> bool {
    let Some(path) = path else {
        return false;
    };
    let Ok(text) = fs::read_to_string(path) else {
        return false;
    };
    crate::codex_runtime::codex_jsonl_has_productive_progress(&text)
}

fn should_enqueue_stream_unstable_retry_continuation(
    item: &RuntimeContinuationCandidate,
    run: &CodexRuntimeRunReport,
    failure_attempts: usize,
    min_attempts: usize,
    token_limit: u64,
) -> bool {
    if item.runtime_class != "interactive" || !runtime_origin_is_parent_channel(&item.origin) {
        return false;
    }
    if run.receipt.status != CodexRuntimeRunStatus::ProtocolError {
        return false;
    }
    if failure_attempts < min_attempts {
        return false;
    }
    if !is_stream_unstable_codex_protocol_error(&run.receipt.reason) {
        return false;
    }
    let usage_exceeds_limit = run.receipt.usage.as_ref().is_some_and(|usage| {
        usage
            .input_tokens
            .or(usage.total_tokens)
            .is_some_and(|tokens| tokens >= token_limit)
    });
    usage_exceeds_limit
}

fn context_recovery_indicates_polluted_thread(
    recovery: Option<&CodexContextRecoveryReceipt>,
) -> bool {
    let Some(recovery) = recovery else {
        return false;
    };
    if matches!(
        recovery.thread_health_status,
        Some(CodexThreadHealthStatus::Polluted | CodexThreadHealthStatus::PollutedAfterCompact)
    ) {
        return true;
    }
    matches!(
        recovery.status.as_str(),
        "compact-before-turn-fallback-failed"
            | "fresh-thread-failed"
            | "preflight-thread-health-rollover-failed"
            | "compact-before-turn-failed"
            | "compact-before-turn-timeout"
    ) || recovery
        .reason
        .to_ascii_lowercase()
        .contains("thread health")
        || recovery.reason.to_ascii_lowercase().contains("polluted")
        || recovery
            .reason
            .to_ascii_lowercase()
            .contains("inline image")
}

fn truncate_for_channel(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_string();
    }
    let mut truncated = value
        .chars()
        .take(max_chars.saturating_sub(3))
        .collect::<String>();
    truncated.push_str("...");
    truncated
}

fn write_runtime_run_once_report(
    report: RuntimeRunOnceReport,
    append_receipt: bool,
) -> io::Result<RuntimeRunOnceReport> {
    write_json_atomic(&report.report_file, &report)?;
    if append_receipt {
        append_json_line(&report.receipts_file, &report.receipt)?;
        if let Some(queue_id) = report.receipt.queue_id.as_deref() {
            mark_cron_run_runtime_status_by_queue_id(
                &report.harness_home,
                queue_id,
                report.receipt.status.as_str(),
                &report.receipt.reason,
                current_log_time_ms().unwrap_or(0),
            )?;
        }
        if should_request_ledger_maintenance_after_terminal(
            report.receipt.status,
            report.receipt.queue_id.as_deref(),
        ) {
            // Completion must not acquire a large ledger lock or replay old
            // receipts.  The separate maintenance owner receives this
            // coalesced wake after the durable terminal append.
            let _ = crate::request_ledger_maintenance(
                &report.harness_home,
                "terminal runtime queue receipt recorded",
            );
        }
        write_json_atomic(&report.report_file, &report)?;
    }
    Ok(report)
}

fn count_prior_runtime_failures(harness_home: &Path, queue_id: &str) -> io::Result<usize> {
    let queue_dir = harness_home.join("state").join("runtime-queue");
    let index = refresh_runtime_queue_state_index(&queue_dir, &mut Vec::new())?;
    Ok(runtime_queue_prior_failure_count_from_index(
        &index, queue_id,
    ))
}

fn count_prior_run_once_status(
    harness_home: &Path,
    queue_id: &str,
    expected_status: &str,
) -> io::Result<usize> {
    let queue_dir = harness_home.join("state").join("runtime-queue");
    let index = refresh_runtime_queue_state_index(&queue_dir, &mut Vec::new())?;
    Ok(runtime_queue_status_count_from_index(
        &index,
        queue_id,
        expected_status,
    ))
}

fn no_prepared_execution_terminal_threshold(harness_home: &Path) -> usize {
    let config_file = harness_home.join("harness-config.json");
    let Ok(text) = fs::read_to_string(config_file) else {
        return 3;
    };
    let Ok(value) = serde_json::from_str::<Value>(&text) else {
        return 3;
    };
    value
        .get("runtime")
        .and_then(|runtime| runtime.get("noPreparedExecutionTerminalThreshold"))
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .filter(|value| *value > 0)
        .unwrap_or(3)
}

fn append_runtime_run_once_log(
    harness_home: &Path,
    level: HarnessLogLevel,
    event: &'static str,
    receipt: &RuntimeRunOnceReceipt,
) -> io::Result<()> {
    append_harness_log(
        harness_home,
        &HarnessLogEvent::new(
            current_log_time_ms()?,
            level,
            "runtime-queue",
            event,
            receipt.reason.clone(),
        )
        .queue_id(receipt.queue_id.clone())
        .path(receipt.execution_dir.clone()),
    )
    .map(|_| ())
}

fn append_runtime_dead_letter_receipt(
    harness_home: &Path,
    receipt: &RuntimeDeadLetterReceipt,
) -> io::Result<()> {
    let path = harness_home
        .join("state")
        .join("runtime-queue")
        .join("dead-letter-receipts.jsonl");
    append_json_line(&path, receipt)
}

fn channel_context_from_prepared_item(item: &RuntimeQueuePreparedItem) -> QueueChannelContext {
    QueueChannelContext {
        platform: item.platform.clone(),
        account_id: item.account_id.clone(),
        channel_id: item.channel_id.clone(),
        user_id: item.user_id.clone(),
        agent_id: item.agent_id.clone(),
        session_key: item.session_key.clone(),
        inbound_context: item.inbound_context.clone(),
        inbound_media_artifacts: item.inbound_media_artifacts.clone(),
    }
}

fn channel_session_is_current(
    harness_home: &Path,
    context: &QueueChannelContext,
    warnings: &mut Vec<String>,
) -> io::Result<bool> {
    // Freshness is an exact provider/account/channel/user/agent boundary.  A
    // legacy state file has no account axis and must never act as wildcard
    // suppression for a queued v2 lane.
    let lane = ChannelStateLane::new(
        &context.platform,
        context.account_id.as_deref(),
        &context.channel_id,
        &context.user_id,
        &context.agent_id,
    )?;
    let Some(state) = read_channel_session_state_v2(harness_home, &lane)? else {
        return Ok(true);
    };
    if state.active_session_key == context.session_key {
        return Ok(true);
    }

    warnings.push(format!(
        "assistant reply for stale session {} suppressed because exact active lane {}/{}/{}/{}/{} has session {}",
        context.session_key,
        lane.platform(),
        lane.account_id(),
        lane.channel_id(),
        lane.user_id(),
        lane.agent_id(),
        state.active_session_key
    ));
    Ok(false)
}

fn session_key_agent_segment(session_key: &str) -> Option<String> {
    session_key
        .split(':')
        .nth(3)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

fn find_queue_channel_context(
    harness_home: &Path,
    queue_id: &str,
    warnings: &mut Vec<String>,
) -> io::Result<Option<QueueChannelContext>> {
    let queue_file = harness_home
        .join("state")
        .join("runtime-queue")
        .join("pending.jsonl");
    if !queue_file.is_file() {
        warnings.push(format!(
            "runtime queue file not found while resolving channel context: {}",
            queue_file.display()
        ));
        return Ok(queue_channel_context_from_queue_id(queue_id));
    }
    let text = fs::read_to_string(&queue_file)?;
    for (index, line) in text.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let value: Value = match serde_json::from_str(trimmed) {
            Ok(value) => value,
            Err(error) => {
                warnings.push(format!(
                    "runtime queue line {} is not valid JSON while resolving channel context: {}",
                    index + 1,
                    error
                ));
                continue;
            }
        };
        if string_field(&value, &["queueId", "queue_id"]) != Some(queue_id) {
            continue;
        }
        return Ok(Some(QueueChannelContext {
            platform: string_field(&value, &["platform"])
                .unwrap_or("unknown")
                .to_string(),
            account_id: string_field(&value, &["accountId", "account_id"]).map(ToString::to_string),
            channel_id: string_field(&value, &["channelId", "channel_id"])
                .unwrap_or("unknown")
                .to_string(),
            user_id: string_field(&value, &["userId", "user_id"])
                .unwrap_or("unknown")
                .to_string(),
            agent_id: string_field(&value, &["agentId", "agent_id"])
                .unwrap_or("unknown")
                .to_string(),
            session_key: string_field(&value, &["sessionKey", "session_key"])
                .unwrap_or("unknown")
                .to_string(),
            inbound_context: string_field(&value, &["inboundContext", "inbound_context"])
                .map(ToString::to_string),
            inbound_media_artifacts: inbound_media_artifacts_field(
                &value,
                &["inboundMediaArtifacts", "inbound_media_artifacts"],
            ),
        }));
    }
    warnings.push(format!(
        "runtime queue item `{queue_id}` was not found while resolving channel context"
    ));
    Ok(queue_channel_context_from_queue_id(queue_id))
}

fn queue_channel_context_from_queue_id(queue_id: &str) -> Option<QueueChannelContext> {
    let mut parts = queue_id.split(':');
    if parts.next()? != "turn" {
        return None;
    }
    let _created_at = parts.next()?;
    let platform = parts.next()?.to_string();
    let channel_id = parts.next()?.to_string();
    let user_id = parts.next()?.to_string();
    let agent_id = parts.next()?.to_string();
    if platform.is_empty() || channel_id.is_empty() || user_id.is_empty() || agent_id.is_empty() {
        return None;
    }
    Some(QueueChannelContext {
        session_key: format!("{platform}:{channel_id}:{user_id}:{agent_id}"),
        platform,
        account_id: None,
        channel_id,
        user_id,
        agent_id,
        inbound_context: None,
        inbound_media_artifacts: Vec::new(),
    })
}

fn delivery_intent_from_inbound_context(
    platform: &str,
    channel_id: &str,
    inbound_context: Option<&str>,
) -> Option<ChannelDeliveryIntent> {
    let platform = platform.to_ascii_lowercase();
    if platform == "telegram" {
        let thread_id =
            inbound_context.and_then(|context| context_value(context, "messageThreadId"));
        let Some(inbound_context) = inbound_context else {
            return None;
        };
        let Some(message_id) = context_value(inbound_context, "messageId") else {
            return thread_id.map(|thread_id| ChannelDeliveryIntent {
                schema: "agent-harness.delivery-intent.v1".to_string(),
                kind: ChannelDeliveryIntentKind::ThreadReply,
                platform_message_id: None,
                platform_channel_id: Some(channel_id.to_string()),
                platform_thread_id: Some(thread_id),
                quote_text: None,
                validated: true,
                downgrade_reason: None,
            });
        };
        return Some(ChannelDeliveryIntent {
            schema: "agent-harness.delivery-intent.v1".to_string(),
            kind: ChannelDeliveryIntentKind::ReplyToMessage,
            platform_message_id: Some(message_id),
            platform_channel_id: Some(channel_id.to_string()),
            platform_thread_id: thread_id,
            quote_text: context_text_block(inbound_context),
            validated: true,
            downgrade_reason: None,
        });
    }
    let inbound_context = inbound_context?;
    if platform == "discord" {
        let message_id = context_value(inbound_context, "referencedMessageId")?;
        let referenced_channel = context_value(inbound_context, "referencedChannelId")
            .unwrap_or_else(|| channel_id.to_string());
        return Some(ChannelDeliveryIntent {
            schema: "agent-harness.delivery-intent.v1".to_string(),
            kind: ChannelDeliveryIntentKind::ReplyToMessage,
            platform_message_id: Some(message_id),
            platform_channel_id: Some(referenced_channel),
            platform_thread_id: None,
            quote_text: context_text_block(inbound_context),
            validated: true,
            downgrade_reason: None,
        });
    }
    None
}

fn context_value(context: &str, key: &str) -> Option<String> {
    let prefix = format!("- {key}:");
    context.lines().find_map(|line| {
        line.trim()
            .strip_prefix(&prefix)
            .map(str::trim)
            .filter(|value| !value.is_empty() && *value != "-")
            .map(ToString::to_string)
    })
}

fn context_text_block(context: &str) -> Option<String> {
    let mut lines = Vec::new();
    let mut in_block = false;
    for line in context.lines() {
        if line.trim() == "text:" || line.trim() == "referencedText:" {
            in_block = true;
            continue;
        }
        if in_block {
            if let Some(text) = line.strip_prefix("  ") {
                lines.push(text.to_string());
            } else if !line.trim().is_empty() {
                break;
            }
        }
    }
    let text = lines.join("\n").trim().to_string();
    (!text.is_empty()).then_some(text)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LatestAssistantResponse {
    final_text: String,
    outbound_text: String,
    prior_user_text: Option<String>,
}

fn latest_assistant_response(
    transcript_file: &Path,
    config: &AssistantNarrationConfig,
) -> io::Result<Option<LatestAssistantResponse>> {
    let entries = transcript_entries(transcript_file)?;
    let Some(assistant_index) = entries
        .iter()
        .rposition(|(role, content)| role == "assistant" && !content.trim().is_empty())
    else {
        return Ok(None);
    };
    let prior_user_entry_index = entries[..assistant_index]
        .iter()
        .rposition(|(role, _)| role == "user");
    let prior_user_text = prior_user_entry_index.map(|index| entries[index].1.clone());
    let final_text = entries[assistant_index].1.clone();
    let outbound_text = match config.mode {
        AssistantNarrationMode::Off | AssistantNarrationMode::ProgressPanel => final_text.clone(),
        AssistantNarrationMode::InlinePreface => {
            let prior_user_index = prior_user_entry_index.map(|index| index + 1).unwrap_or(0);
            let narration = entries[prior_user_index..assistant_index]
                .iter()
                .filter(|(role, content)| {
                    role == "assistant_narration" && !content.trim().is_empty()
                })
                .map(|(_, content)| compact_inline_narration(content))
                .collect::<Vec<_>>();
            if narration.is_empty() {
                final_text.clone()
            } else {
                let narration_block =
                    truncate_for_channel(&narration.join("\n\n"), config.max_chars.max(1));
                format!(
                    "{}\n---\n{}\n\nFinal reply\n---\n{}",
                    config.final_prefix, narration_block, final_text
                )
            }
        }
    };
    Ok(Some(LatestAssistantResponse {
        final_text,
        outbound_text,
        prior_user_text,
    }))
}

pub(crate) fn latest_assistant_final_text(transcript_file: &Path) -> io::Result<Option<String>> {
    Ok(transcript_entries(transcript_file)?
        .into_iter()
        .rev()
        .find(|(role, content)| role == "assistant" && !content.trim().is_empty())
        .map(|(_, content)| content))
}

fn transcript_entries(transcript_file: &Path) -> io::Result<Vec<(String, String)>> {
    let text = fs::read_to_string(transcript_file)?;
    let mut entries = Vec::new();
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let value: Value = match serde_json::from_str(trimmed) {
            Ok(value) => value,
            Err(_) => continue,
        };
        let Some(role) = string_field(&value, &["role"]) else {
            continue;
        };
        let Some(content) = string_field(&value, &["content"]) else {
            continue;
        };
        entries.push((role.to_string(), content.to_string()));
    }
    Ok(entries)
}

fn compact_inline_narration(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn split_outbound_media_directives(
    harness_home: &Path,
    queue_id: Option<&str>,
    platform: Option<&str>,
    text: &str,
    warnings: &mut Vec<String>,
) -> io::Result<(String, Vec<ChannelOutboundAttachment>)> {
    let config = match load_harness_media_config(harness_home) {
        Ok(config) => config,
        Err(error) => {
            warnings.push(format!(
                "media delivery config could not be loaded; using defaults: {error}"
            ));
            Default::default()
        }
    };
    let parsed = parse_outbound_media_directives(text);
    let mut replacements = parsed
        .modifier_spans
        .iter()
        .map(|span| SpanReplacement {
            start: span.start,
            end: span.end,
            replacement: String::new(),
        })
        .collect::<Vec<_>>();
    let mut attachments = Vec::new();
    let as_document = parsed.as_document;
    let audio_as_voice = parsed.audio_as_voice;
    for directive in parsed.directives {
        let forced_kind = forced_attachment_kind(&directive.path, as_document, audio_as_voice);
        let evaluation = evaluate_outbound_media_path(
            harness_home,
            &directive.path,
            &config.policy,
            forced_kind,
        );
        if let Err(error) =
            write_media_policy_receipt(harness_home, queue_id, platform, &evaluation)
        {
            warnings.push(format!(
                "outbound media policy receipt write failed: {error}"
            ));
        }
        match evaluation.verdict {
            MediaDeliveryVerdict::Accepted {
                kind,
                mime,
                byte_len: _,
            } => {
                attachments.push(ChannelOutboundAttachment {
                    kind,
                    mime,
                    filename: directive
                        .path
                        .file_name()
                        .map(|name| name.to_string_lossy().to_string()),
                    caption: None,
                    path: directive.path,
                });
                replacements.push(SpanReplacement {
                    start: directive.start,
                    end: directive.end,
                    replacement: String::new(),
                });
            }
            MediaDeliveryVerdict::Rejected { reason_code } => {
                replacements.push(SpanReplacement {
                    start: directive.start,
                    end: directive.end,
                    replacement: format!("[attachment not delivered: {reason_code}]"),
                });
            }
        }
    }
    Ok((
        apply_span_replacements(text, &mut replacements),
        attachments,
    ))
}

fn resolve_presentation_artifact_refs(
    harness_home: &Path,
    queue_id: Option<&str>,
    platform: Option<&str>,
    presentation: &mut RichMessagePresentation,
    attachments: &mut Vec<ChannelOutboundAttachment>,
    warnings: &mut Vec<String>,
) -> io::Result<()> {
    if presentation.media.is_empty() {
        return Ok(());
    }
    let config = match load_harness_media_config(harness_home) {
        Ok(config) => config,
        Err(error) => {
            warnings.push(format!(
                "media delivery config could not be loaded for artifact refs; using defaults: {error}"
            ));
            Default::default()
        }
    };
    for media in &mut presentation.media {
        if media.attachment_index.is_some() {
            continue;
        }
        let Some(artifact_ref) = media.artifact_ref.clone() else {
            continue;
        };
        let path = match resolve_outbound_media_artifact_reference(harness_home, &artifact_ref) {
            Ok(path) => path,
            Err(error) => {
                warnings.push(format!(
                    "rich media artifactRef {artifact_ref} was not resolved for delivery: {error}; fallback=artifact-ref-unresolvable"
                ));
                continue;
            }
        };
        let evaluation = evaluate_outbound_media_path(harness_home, &path, &config.policy, None);
        if let Err(error) =
            write_media_policy_receipt(harness_home, queue_id, platform, &evaluation)
        {
            warnings.push(format!(
                "outbound media policy receipt write failed for artifactRef {artifact_ref}: {error}"
            ));
        }
        match evaluation.verdict {
            MediaDeliveryVerdict::Accepted { kind, mime, .. } => {
                let index = attachments.len();
                attachments.push(ChannelOutboundAttachment {
                    kind,
                    path,
                    mime,
                    filename: artifact_ref
                        .rsplit('/')
                        .next()
                        .filter(|name| !name.trim().is_empty())
                        .map(ToString::to_string),
                    caption: media.caption.clone(),
                });
                media.attachment_index = Some(index);
            }
            MediaDeliveryVerdict::Rejected { reason_code } => {
                warnings.push(format!(
                    "rich media artifactRef {artifact_ref} resolved but was rejected by media policy: {reason_code}; fallback=artifact-ref-policy-rejected"
                ));
            }
        }
    }
    Ok(())
}

fn resolve_outbound_media_artifact_reference(
    harness_home: &Path,
    artifact_ref: &str,
) -> Result<PathBuf, String> {
    let trimmed = artifact_ref.trim();
    if trimmed.is_empty() {
        return Err("artifact reference is empty".to_string());
    }
    if trimmed.starts_with("agent-harness://inbound-media/") {
        return resolve_inbound_media_artifact_reference(harness_home, trimmed);
    }
    if let Some(relative) = trimmed.strip_prefix("agent-harness://generated-images/") {
        return resolve_outbound_media_relative_artifact(
            &harness_home.join("codex-home").join("generated_images"),
            relative,
        );
    }
    if let Some(relative) = trimmed.strip_prefix("agent-harness://generated-media/") {
        return resolve_outbound_media_relative_artifact(
            &harness_home.join("state").join("generated-media"),
            relative,
        );
    }
    Err("artifact URI namespace is not supported for outbound delivery".to_string())
}

fn resolve_outbound_media_relative_artifact(
    root: &Path,
    relative: &str,
) -> Result<PathBuf, String> {
    let relative_path = safe_outbound_media_relative_path(relative)?;
    let candidate = root.join(relative_path);
    let root = fs::canonicalize(root).map_err(|err| {
        format!(
            "artifact root {} does not exist or is not readable: {err}",
            root.display()
        )
    })?;
    let candidate = fs::canonicalize(&candidate).map_err(|err| {
        format!(
            "artifact path {} does not exist or is not readable: {err}",
            candidate.display()
        )
    })?;
    if !candidate.starts_with(&root) {
        return Err("artifact path is outside the supported artifact root".to_string());
    }
    if !candidate.is_file() {
        return Err("artifact path does not exist or is not a file".to_string());
    }
    Ok(candidate)
}

fn safe_outbound_media_relative_path(value: &str) -> Result<PathBuf, String> {
    let mut path = PathBuf::new();
    for component in Path::new(value).components() {
        match component {
            Component::Normal(part) => path.push(part),
            _ => return Err("artifact URI contains an unsafe path component".to_string()),
        }
    }
    if path.as_os_str().is_empty() {
        return Err("artifact URI does not name a file".to_string());
    }
    Ok(path)
}

fn forced_attachment_kind(
    path: &Path,
    as_document: bool,
    audio_as_voice: bool,
) -> Option<ChannelOutboundAttachmentKind> {
    match attachment_kind_from_path(path) {
        Some(ChannelOutboundAttachmentKind::Image) if as_document => {
            Some(ChannelOutboundAttachmentKind::Document)
        }
        Some(ChannelOutboundAttachmentKind::Audio) if audio_as_voice => {
            Some(ChannelOutboundAttachmentKind::Voice)
        }
        _ => None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedOutboundMediaDirectives {
    directives: Vec<ParsedOutboundMediaDirective>,
    modifier_spans: Vec<ProtectedSpan>,
    as_document: bool,
    audio_as_voice: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedOutboundMediaDirective {
    start: usize,
    end: usize,
    path: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProtectedSpan {
    start: usize,
    end: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SpanReplacement {
    start: usize,
    end: usize,
    replacement: String,
}

fn parse_outbound_media_directives(text: &str) -> ParsedOutboundMediaDirectives {
    let protected = protected_outbound_spans(text);
    let mut directives = Vec::new();
    let mut offset = 0usize;
    while let Some(relative) = text[offset..].find("MEDIA:") {
        let start = offset + relative;
        if byte_is_protected(start, &protected) {
            offset = start + "MEDIA:".len();
            continue;
        }
        let Some((end, path)) = parse_media_path_after_prefix(text, start + "MEDIA:".len()) else {
            offset = start + "MEDIA:".len();
            continue;
        };
        if path.is_absolute() && is_deliverable_media_path(&path) {
            let (start, end) = expand_standalone_media_directive_span(text, start, end);
            directives.push(ParsedOutboundMediaDirective { start, end, path });
        }
        offset = end.max(start + "MEDIA:".len());
    }

    let mut modifier_spans = Vec::new();
    let as_document =
        collect_modifier_spans(text, "[[as_document]]", &protected, &mut modifier_spans);
    let audio_as_voice =
        collect_modifier_spans(text, "[[audio_as_voice]]", &protected, &mut modifier_spans);
    ParsedOutboundMediaDirectives {
        directives,
        modifier_spans,
        as_document,
        audio_as_voice,
    }
}

fn collect_modifier_spans(
    text: &str,
    marker: &str,
    protected: &[ProtectedSpan],
    spans: &mut Vec<ProtectedSpan>,
) -> bool {
    let mut found = false;
    let mut offset = 0usize;
    while let Some(relative) = text[offset..].find(marker) {
        let start = offset + relative;
        let end = start + marker.len();
        if !byte_is_protected(start, protected) {
            spans.push(ProtectedSpan { start, end });
            found = true;
        }
        offset = end;
    }
    found
}

fn parse_media_path_after_prefix(text: &str, mut index: usize) -> Option<(usize, PathBuf)> {
    let bytes = text.as_bytes();
    while index < bytes.len() && matches!(bytes[index], b' ' | b'\t') {
        index += 1;
    }
    if index >= bytes.len() {
        return None;
    }
    let first = bytes[index];
    if matches!(first, b'"' | b'\'' | b'`') {
        let quote = first;
        let path_start = index + 1;
        let mut end = path_start;
        while end < bytes.len() && bytes[end] != quote {
            end += 1;
        }
        if end >= bytes.len() {
            return None;
        }
        let path = text[path_start..end].trim();
        return (!path.is_empty()).then(|| (end + 1, PathBuf::from(path)));
    }
    let path_start = index;
    while index < bytes.len() && !bytes[index].is_ascii_whitespace() {
        index += 1;
    }
    let path = text[path_start..index].trim();
    (!path.is_empty()).then(|| (index, PathBuf::from(path)))
}

fn expand_standalone_media_directive_span(text: &str, start: usize, end: usize) -> (usize, usize) {
    let line_start = text[..start]
        .rfind('\n')
        .map(|index| index + 1)
        .unwrap_or(0);
    let line_end = text[end..]
        .find('\n')
        .map(|relative| end + relative + 1)
        .unwrap_or(end);
    let line_without_directive = format!("{}{}", &text[line_start..start], &text[end..line_end]);
    if line_without_directive.trim().is_empty() {
        (line_start, line_end)
    } else {
        (start, end)
    }
}

fn protected_outbound_spans(text: &str) -> Vec<ProtectedSpan> {
    let mut spans = Vec::new();
    let mut offset = 0usize;
    let mut in_fence = false;
    for line in text.split_inclusive('\n') {
        let line_end = offset + line.len();
        let trimmed = line.trim_start();
        let fence_line = trimmed.starts_with("```");
        if in_fence || fence_line || trimmed.starts_with('>') {
            spans.push(ProtectedSpan {
                start: offset,
                end: line_end,
            });
        }
        if !in_fence && !fence_line && !trimmed.starts_with('>') {
            push_inline_code_spans(line, offset, &mut spans);
        }
        if fence_line {
            in_fence = !in_fence;
        }
        offset = line_end;
    }
    if offset < text.len() {
        let tail = &text[offset..];
        if in_fence || tail.trim_start().starts_with('>') {
            spans.push(ProtectedSpan {
                start: offset,
                end: text.len(),
            });
        } else {
            push_inline_code_spans(tail, offset, &mut spans);
        }
    }
    spans
}

fn push_inline_code_spans(line: &str, line_offset: usize, spans: &mut Vec<ProtectedSpan>) {
    let mut code_start = None;
    for (relative, ch) in line.char_indices() {
        if ch != '`' {
            continue;
        }
        if let Some(start) = code_start.take() {
            spans.push(ProtectedSpan {
                start,
                end: line_offset + relative + ch.len_utf8(),
            });
        } else {
            code_start = Some(line_offset + relative);
        }
    }
    if let Some(start) = code_start {
        spans.push(ProtectedSpan {
            start,
            end: line_offset + line.len(),
        });
    }
}

fn byte_is_protected(index: usize, spans: &[ProtectedSpan]) -> bool {
    spans
        .iter()
        .any(|span| index >= span.start && index < span.end)
}

fn apply_span_replacements(text: &str, replacements: &mut Vec<SpanReplacement>) -> String {
    replacements.sort_by_key(|span| (span.start, span.end));
    let mut out = String::with_capacity(text.len());
    let mut cursor = 0usize;
    for replacement in replacements {
        if replacement.start < cursor
            || replacement.start > text.len()
            || replacement.end > text.len()
        {
            continue;
        }
        out.push_str(&text[cursor..replacement.start]);
        out.push_str(&replacement.replacement);
        cursor = replacement.end;
    }
    out.push_str(&text[cursor..]);
    cleanup_outbound_media_text(&out)
}

fn cleanup_outbound_media_text(text: &str) -> String {
    let mut cleaned = Vec::new();
    let mut blank_count = 0usize;
    for line in text.lines() {
        let trimmed_end = line.trim_end();
        if trimmed_end.trim().is_empty() {
            blank_count += 1;
            if blank_count <= 1 {
                cleaned.push(String::new());
            }
        } else {
            blank_count = 0;
            cleaned.push(trimmed_end.to_string());
        }
    }
    cleaned.join("\n").trim().to_string()
}

fn maybe_record_media_delivery_lint(
    harness_home: &Path,
    queue_id: Option<&str>,
    platform: Option<&str>,
    prior_user_text: Option<&str>,
    final_text: &str,
    attachment_count: usize,
    warnings: &mut Vec<String>,
) -> MediaDeliveryLintResult {
    if attachment_count > 0 {
        return MediaDeliveryLintResult::default();
    }
    let Some(user_text) = prior_user_text else {
        return MediaDeliveryLintResult::default();
    };
    if !user_text_has_media_send_intent(user_text)
        || !final_text_has_media_delivery_clue(final_text)
    {
        return MediaDeliveryLintResult::default();
    }
    let fail_closed = load_harness_media_config(harness_home)
        .map(|config| config.lint.fail_closed)
        .unwrap_or(false);
    let receipt_file = harness_home
        .join("state")
        .join("channels")
        .join("media-delivery-lint-receipts.jsonl");
    let receipt = serde_json::json!({
        "schema": "agent-harness.media-delivery-lint.v1",
        "atMs": current_log_time_ms().unwrap_or(0),
        "queueId": queue_id,
        "platform": platform,
        "status": if fail_closed { "failed-closed" } else { "warning" },
        "reason": "media-send-intent-with-zero-attachments",
        "failClosed": fail_closed,
    });
    if let Err(error) = crate::append_jsonl_value(&receipt_file, &receipt) {
        warnings.push(format!("media delivery lint receipt write failed: {error}"));
    } else {
        let action = if fail_closed {
            "terminal notification will replace the text-only final"
        } else {
            "warning only"
        };
        warnings.push(format!(
            "media-delivery-lint: send intent found but final outbox has zero attachments; {action}"
        ));
    }
    MediaDeliveryLintResult {
        triggered: true,
        fail_closed,
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct MediaDeliveryLintResult {
    triggered: bool,
    fail_closed: bool,
}

fn media_delivery_lint_terminal_notice() -> String {
    "I could not attach the requested media. No file was delivered because the final response did not include a policy-accepted attachment directive.".to_string()
}

fn user_text_has_media_send_intent(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    let has_action = ["send", "attach", "upload", "drop", "傳", "丟", "發"]
        .iter()
        .any(|needle| lower.contains(needle));
    let has_media_object = [
        "image",
        "photo",
        "picture",
        "file",
        "attachment",
        "圖",
        "照片",
        "檔",
    ]
    .iter()
    .any(|needle| lower.contains(needle));
    has_action && has_media_object
}

fn final_text_has_media_delivery_clue(text: &str) -> bool {
    if text.contains("MEDIA:") || text.contains("[attachment not delivered:") {
        return true;
    }
    text.split_whitespace().any(|part| {
        let trimmed = part.trim_matches(|ch: char| {
            matches!(
                ch,
                '"' | '\'' | '`' | '(' | ')' | '[' | ']' | '<' | '>' | ',' | '.'
            )
        });
        let path = Path::new(trimmed);
        path.is_absolute() && is_deliverable_media_path(path)
    })
}

fn append_outbound_message(
    harness_home: &Path,
    message: &mut ChannelOutboundMessage,
    warnings: &mut Vec<String>,
) -> io::Result<(PathBuf, bool)> {
    let report = append_channel_outbox_message(harness_home, message)?;
    if let Some(warning) = report.index_warning {
        warnings.push(warning);
    }
    let wake_file = harness_home
        .join("state")
        .join("wake")
        .join("final-outbox.json");
    let _ = crate::wake::signal_wake(
        harness_home,
        wake_file,
        "final-outbox",
        "channel outbox message appended",
    );
    Ok((
        report.outbox_file,
        !matches!(
            report.outcome,
            crate::ChannelOutboxAppendOutcome::AlreadyPresent
        ),
    ))
}

fn append_final_outbound_message_once(
    harness_home: &Path,
    execution_dir: Option<&Path>,
    completion_file: Option<&Path>,
    message: &mut ChannelOutboundMessage,
    warnings: &mut Vec<String>,
) -> io::Result<FinalOutboxOutcome> {
    let _lock = acquire_final_outbox_lock(execution_dir, warnings)?;
    if let Some(receipt) = read_final_outbox_receipt(execution_dir, warnings)? {
        if final_outbox_receipt_matches(&receipt, message, completion_file) {
            warnings.push(format!(
                "runtime final outbox already enqueued for queue {}; skipping duplicate append",
                receipt.queue_id.as_deref().unwrap_or("-")
            ));
            return Ok(FinalOutboxOutcome {
                outbox_file: receipt.outbox_file,
                disposition: FinalOutboxDispositionV1::AlreadyPresent,
                canonical_source_queue_id: receipt.queue_id,
                delivery_id: message.delivery_id.clone(),
            });
        }
        warnings.push(format!(
            "runtime final outbox receipt did not match queue/completion {}; falling back to outbox scan",
            message.source_queue_id.as_deref().unwrap_or("-")
        ));
    }
    if let Some(outbox_file) = find_existing_source_outbox_message(harness_home, message, warnings)?
    {
        if let Some(execution_dir) = execution_dir {
            record_final_outbox_receipt(
                execution_dir,
                completion_file,
                message,
                &outbox_file,
                warnings,
            )?;
        }
        warnings.push(format!(
            "runtime final outbox already present for queue {}; recorded marker without duplicate append",
            message.source_queue_id.as_deref().unwrap_or("-")
        ));
        return Ok(FinalOutboxOutcome {
            outbox_file,
            disposition: FinalOutboxDispositionV1::AlreadyPresent,
            canonical_source_queue_id: message.source_queue_id.clone(),
            delivery_id: message.delivery_id.clone(),
        });
    }

    let (outbox_file, appended) = append_outbound_message(harness_home, message, warnings)?;
    if let Some(execution_dir) = execution_dir {
        record_final_outbox_receipt(
            execution_dir,
            completion_file,
            message,
            &outbox_file,
            warnings,
        )?;
    }
    Ok(FinalOutboxOutcome {
        outbox_file,
        disposition: if appended {
            FinalOutboxDispositionV1::Appended
        } else {
            FinalOutboxDispositionV1::AlreadyPresent
        },
        canonical_source_queue_id: message.source_queue_id.clone(),
        delivery_id: message.delivery_id.clone(),
    })
}

fn append_goal_terminal_outbound_message_once(
    harness_home: &Path,
    execution_dir: Option<&Path>,
    completion_file: Option<&Path>,
    transition: &GoalTransitionReceiptV1,
    message: &mut ChannelOutboundMessage,
    warnings: &mut Vec<String>,
) -> io::Result<FinalOutboxOutcome> {
    let (terminal_key, delivery_id) = goal_terminal_outbox_identity(transition)?;
    let receipt_file = goal_terminal_outbox_receipts_file(harness_home);
    let _lock = acquire_goal_terminal_outbox_lock(harness_home, &terminal_key, warnings)?;
    let existing = read_latest_goal_terminal_outbox_receipt(&receipt_file, &terminal_key)?;
    let requested_source_queue_id = message.source_queue_id.clone();
    let mut canonical = if let Some(existing) = existing.as_ref() {
        if existing.delivery_id != delivery_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "goal terminal outbox receipt {} has a conflicting delivery identity",
                    existing.terminal_key
                ),
            ));
        }
        if &existing.message != message {
            let different_source = existing.source_queue_id != message.source_queue_id;
            if different_source
                && transition.relation != GoalTransitionRelation::AuthorizedCampaignContinuation
            {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "foreign campaign terminal reuse lacks an authorized continuation relation",
                ));
            }
            warnings.push(format!(
                "goal terminal {} already committed by queue {}; reusing the canonical campaign surface",
                terminal_key,
                existing.source_queue_id.as_deref().unwrap_or("-")
            ));
        }
        existing.message.clone()
    } else {
        message.delivery_id = Some(delivery_id.clone());
        let committed = GoalTerminalOutboxReceiptV1 {
            schema: GOAL_TERMINAL_OUTBOX_SCHEMA.to_string(),
            terminal_key: terminal_key.clone(),
            delivery_id: delivery_id.clone(),
            campaign_family_id: transition
                .campaign_family_id
                .clone()
                .expect("goal terminal identity validated campaign family"),
            lane_digest: transition
                .lane_digest
                .clone()
                .expect("goal terminal identity validated lane digest"),
            virtual_session_id: transition
                .virtual_session_id
                .clone()
                .expect("goal terminal identity validated virtual session"),
            decision: transition.decision,
            surface: transition.surface,
            source_queue_id: message.source_queue_id.clone(),
            message: message.clone(),
            state: GoalTerminalOutboxState::Committed,
            outbox_file: None,
            recorded_at_ms: current_log_time_ms()?,
        };
        append_json_line(&receipt_file, &committed)?;
        committed.message
    };

    if let Some(existing) = existing.as_ref()
        && existing.state == GoalTerminalOutboxState::Appended
    {
        let outbox_file = existing.outbox_file.clone().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "appended goal terminal receipt is missing its outbox file",
            )
        })?;
        *message = canonical;
        warnings.push(format!(
            "goal terminal {} was already appended; suppressing duplicate campaign surface",
            terminal_key
        ));
        return Ok(FinalOutboxOutcome {
            outbox_file,
            disposition: if existing.source_queue_id != requested_source_queue_id {
                FinalOutboxDispositionV1::ReusedCanonicalCampaign
            } else {
                FinalOutboxDispositionV1::AlreadyPresent
            },
            canonical_source_queue_id: existing.source_queue_id.clone(),
            delivery_id: Some(existing.delivery_id.clone()),
        });
    }

    let mut outcome = append_final_outbound_message_once(
        harness_home,
        execution_dir,
        completion_file,
        &mut canonical,
        warnings,
    )?;
    let appended_receipt = GoalTerminalOutboxReceiptV1 {
        schema: GOAL_TERMINAL_OUTBOX_SCHEMA.to_string(),
        terminal_key,
        delivery_id: delivery_id.clone(),
        campaign_family_id: transition
            .campaign_family_id
            .clone()
            .expect("goal terminal identity validated campaign family"),
        lane_digest: transition
            .lane_digest
            .clone()
            .expect("goal terminal identity validated lane digest"),
        virtual_session_id: transition
            .virtual_session_id
            .clone()
            .expect("goal terminal identity validated virtual session"),
        decision: transition.decision,
        surface: transition.surface,
        source_queue_id: canonical.source_queue_id.clone(),
        message: canonical.clone(),
        state: GoalTerminalOutboxState::Appended,
        outbox_file: Some(outcome.outbox_file.clone()),
        recorded_at_ms: current_log_time_ms()?,
    };
    append_json_line(&receipt_file, &appended_receipt)?;
    *message = canonical;
    if existing
        .as_ref()
        .is_some_and(|receipt| receipt.source_queue_id != requested_source_queue_id)
    {
        outcome.disposition = FinalOutboxDispositionV1::ReusedCanonicalCampaign;
        outcome.canonical_source_queue_id = existing
            .as_ref()
            .and_then(|receipt| receipt.source_queue_id.clone());
    }
    outcome.delivery_id = Some(delivery_id);
    Ok(outcome)
}

fn goal_terminal_outbox_identity(
    transition: &GoalTransitionReceiptV1,
) -> io::Result<(String, String)> {
    if !transition.terminal
        || !matches!(
            transition.surface,
            GoalTransitionSurface::CampaignFinal | GoalTransitionSurface::TerminalNotice
        )
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "goal terminal outbox identity requires a terminal campaign surface",
        ));
    }
    let campaign_family_id = required_goal_terminal_identity_field(
        transition.campaign_family_id.as_deref(),
        "campaignFamilyId",
    )?;
    let lane_digest =
        required_goal_terminal_identity_field(transition.lane_digest.as_deref(), "laneDigest")?;
    let virtual_session_id = required_goal_terminal_identity_field(
        transition.virtual_session_id.as_deref(),
        "virtualSessionId",
    )?;
    let mut canonical = Vec::new();
    for value in [
        "goal-terminal-outbox-v1",
        campaign_family_id,
        lane_digest,
        virtual_session_id,
        goal_transition_decision_token(transition.decision),
    ] {
        canonical.extend_from_slice(&(value.len() as u64).to_be_bytes());
        canonical.extend_from_slice(value.as_bytes());
    }
    let digest = ring::digest::digest(&ring::digest::SHA256, &canonical);
    let terminal_key = digest
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let delivery_id = format!("delivery:v2:{}", &terminal_key[..32]);
    Ok((terminal_key, delivery_id))
}

fn required_goal_terminal_identity_field<'a>(
    value: Option<&'a str>,
    name: &str,
) -> io::Result<&'a str> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("goal terminal surface lacks exact {name} authority"),
            )
        })
}

fn goal_transition_decision_token(decision: GoalTransitionDecision) -> &'static str {
    match decision {
        GoalTransitionDecision::Continue => "continue",
        GoalTransitionDecision::Complete => "complete",
        GoalTransitionDecision::Pause => "pause",
        GoalTransitionDecision::Stop => "stop",
        GoalTransitionDecision::NeedsUser => "needs-user",
        GoalTransitionDecision::NeedsAuthority => "needs-authority",
        GoalTransitionDecision::NeedsOperatorAuth => "needs-operator-auth",
        GoalTransitionDecision::Rollover => "rollover",
        GoalTransitionDecision::BudgetExhausted => "budget-exhausted",
        GoalTransitionDecision::NoProgressExhausted => "no-progress-exhausted",
        GoalTransitionDecision::Failed => "failed",
    }
}

fn goal_terminal_notice_text(decision: GoalTransitionDecision) -> Option<&'static str> {
    match decision {
        GoalTransitionDecision::Pause => Some("The goal is paused."),
        GoalTransitionDecision::Stop => Some("The goal was stopped."),
        GoalTransitionDecision::NeedsUser => {
            Some("The goal needs your input before it can continue.")
        }
        GoalTransitionDecision::NeedsAuthority => {
            Some("The goal needs additional authorization before it can continue.")
        }
        GoalTransitionDecision::NeedsOperatorAuth => {
            Some("The goal needs an operator to authenticate before it can continue.")
        }
        GoalTransitionDecision::BudgetExhausted => Some("The goal budget is exhausted."),
        GoalTransitionDecision::NoProgressExhausted => {
            Some("The goal stopped after reaching its no-progress limit.")
        }
        _ => None,
    }
}

fn active_goal_parked_notice_text(
    activation: &GoalAutonomyActivation,
    safe_checkpoint: Option<&str>,
) -> String {
    let policy = if activation
        .reason
        .starts_with("shell-recovery-fenced-by-external-effect")
    {
        "Automatic shell recovery was fenced because external-effect or mutation evidence requires explicit reconciliation, so no continuation child was started."
    } else if activation
        .reason
        .starts_with("shell-recovery-budget-exhausted")
    {
        "The one automatic fresh-runtime shell recovery was already used, so another continuation child was not started."
    } else {
        match activation.configured_mode {
            GoalAutonomyMode::Observe => {
                "This exact lane is observation-only, so no automatic continuation child was started."
            }
            GoalAutonomyMode::Disabled => {
                "Automatic goal continuation is disabled, so no continuation child was started."
            }
            GoalAutonomyMode::Active => {
                "Automatic goal continuation is not admitted for this exact lane, so no continuation child was started."
            }
        }
    };
    let mut notice = format!(
        "The goal remains active and is parked safely. {policy} Resume it manually or authorize a reviewed exact-lane cohort."
    );
    if let Some(checkpoint) = safe_checkpoint
        .map(str::trim)
        .filter(|text| !text.is_empty())
    {
        notice.push_str("\n\nLatest parent checkpoint:\n");
        notice.push_str(&truncate_for_channel(checkpoint, 1_000));
    }
    notice
}

fn shell_recovery_effect_fence_clear(receipt: &crate::CodexRuntimeRunReceipt) -> bool {
    receipt.external_effect.is_none()
        && !matches!(
            receipt.mutation_evidence,
            Some(
                crate::RuntimeMutationEvidenceClass::MutationObserved
                    | crate::RuntimeMutationEvidenceClass::Unknown
            )
        )
}

fn goal_terminal_outbox_receipts_file(harness_home: &Path) -> PathBuf {
    harness_home
        .join("state")
        .join("goal-lineage")
        .join("terminal-outbox-receipts.jsonl")
}

fn read_latest_goal_terminal_outbox_receipt(
    receipt_file: &Path,
    terminal_key: &str,
) -> io::Result<Option<GoalTerminalOutboxReceiptV1>> {
    let text = match fs::read_to_string(receipt_file) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let mut latest = None;
    for (index, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let receipt =
            serde_json::from_str::<GoalTerminalOutboxReceiptV1>(line).map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "invalid goal terminal outbox receipt at {}:{}: {error}",
                        receipt_file.display(),
                        index + 1
                    ),
                )
            })?;
        if receipt.schema != GOAL_TERMINAL_OUTBOX_SCHEMA {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported goal terminal outbox schema {}", receipt.schema),
            ));
        }
        if receipt.terminal_key == terminal_key {
            latest = Some(receipt);
        }
    }
    Ok(latest)
}

struct GoalTerminalOutboxLockGuard {
    path: PathBuf,
}

impl Drop for GoalTerminalOutboxLockGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn acquire_goal_terminal_outbox_lock(
    harness_home: &Path,
    terminal_key: &str,
    warnings: &mut Vec<String>,
) -> io::Result<GoalTerminalOutboxLockGuard> {
    let lock_dir = harness_home
        .join("state")
        .join("goal-lineage")
        .join("locks");
    fs::create_dir_all(&lock_dir)?;
    let lock_file = lock_dir.join(format!("{terminal_key}.lock"));
    for attempt in 0..25 {
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&lock_file)
        {
            Ok(_) => return Ok(GoalTerminalOutboxLockGuard { path: lock_file }),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                if final_outbox_lock_is_stale(&lock_file) {
                    match fs::remove_file(&lock_file) {
                        Ok(()) => {
                            warnings.push(
                                "removed stale goal terminal outbox lock before enqueue"
                                    .to_string(),
                            );
                            continue;
                        }
                        Err(remove_error) if remove_error.kind() == io::ErrorKind::NotFound => {
                            continue;
                        }
                        Err(remove_error) => return Err(remove_error),
                    }
                }
                if attempt == 24 {
                    return Err(io::Error::new(
                        io::ErrorKind::WouldBlock,
                        format!("goal terminal outbox lock is busy: {}", lock_file.display()),
                    ));
                }
                thread::sleep(Duration::from_millis(200));
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("goal terminal outbox lock loop returns on every terminal branch")
}

struct FinalOutboxLockGuard {
    path: PathBuf,
}

impl Drop for FinalOutboxLockGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn acquire_final_outbox_lock(
    execution_dir: Option<&Path>,
    warnings: &mut Vec<String>,
) -> io::Result<Option<FinalOutboxLockGuard>> {
    let Some(execution_dir) = execution_dir else {
        return Ok(None);
    };
    let lock_file = execution_dir.join("channel-final-outbox.lock");
    for attempt in 0..25 {
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&lock_file)
        {
            Ok(_) => return Ok(Some(FinalOutboxLockGuard { path: lock_file })),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                if final_outbox_lock_is_stale(&lock_file) {
                    match fs::remove_file(&lock_file) {
                        Ok(()) => {
                            warnings.push(
                                "removed stale runtime final outbox lock before enqueue"
                                    .to_string(),
                            );
                            continue;
                        }
                        Err(remove_error) if remove_error.kind() == io::ErrorKind::NotFound => {
                            continue;
                        }
                        Err(remove_error) => return Err(remove_error),
                    }
                }
                if attempt == 24 {
                    return Err(io::Error::new(
                        io::ErrorKind::WouldBlock,
                        format!("runtime final outbox lock is busy: {}", lock_file.display()),
                    ));
                }
                thread::sleep(Duration::from_millis(200));
            }
            Err(error) => return Err(error),
        }
    }
    Ok(None)
}

fn final_outbox_lock_is_stale(lock_file: &Path) -> bool {
    let Ok(metadata) = fs::metadata(lock_file) else {
        return false;
    };
    let Ok(modified) = metadata.modified() else {
        return false;
    };
    SystemTime::now()
        .duration_since(modified)
        .map(|age| age > Duration::from_secs(60))
        .unwrap_or(false)
}

fn final_outbox_receipt_matches(
    receipt: &FinalOutboxReceipt,
    message: &ChannelOutboundMessage,
    completion_file: Option<&Path>,
) -> bool {
    receipt.queue_id == message.source_queue_id
        && receipt.completion_file.as_deref() == completion_file
        && receipt.platform == message.platform
        && receipt.account_id == message.account_id
        && receipt.channel_id == message.channel_id
        && receipt.user_id == message.user_id
        && receipt.session_key == message.session_key
        && receipt.kind == message.kind
}

fn final_outbox_receipt_file(execution_dir: &Path) -> PathBuf {
    execution_dir.join("channel-final-outbox-receipt.json")
}

fn read_final_outbox_receipt(
    execution_dir: Option<&Path>,
    warnings: &mut Vec<String>,
) -> io::Result<Option<FinalOutboxReceipt>> {
    let Some(execution_dir) = execution_dir else {
        return Ok(None);
    };
    let receipt_file = final_outbox_receipt_file(execution_dir);
    if !receipt_file.is_file() {
        return Ok(None);
    }
    match fs::read(&receipt_file).and_then(|bytes| {
        serde_json::from_slice::<FinalOutboxReceipt>(&bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    }) {
        Ok(receipt) => Ok(Some(receipt)),
        Err(error) => {
            warnings.push(format!(
                "runtime final outbox receipt could not be read; falling back to outbox scan: {error}"
            ));
            Ok(None)
        }
    }
}

fn record_final_outbox_receipt(
    execution_dir: &Path,
    completion_file: Option<&Path>,
    message: &ChannelOutboundMessage,
    outbox_file: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<()> {
    let receipt = FinalOutboxReceipt {
        schema: FINAL_OUTBOX_RECEIPT_SCHEMA.to_string(),
        queue_id: message.source_queue_id.clone(),
        completion_file: completion_file.map(Path::to_path_buf),
        outbox_file: outbox_file.to_path_buf(),
        platform: message.platform.clone(),
        account_id: message.account_id.clone(),
        channel_id: message.channel_id.clone(),
        user_id: message.user_id.clone(),
        session_key: message.session_key.clone(),
        kind: message.kind,
        delivery_id: message.delivery_id.clone(),
    };
    let receipt_file = final_outbox_receipt_file(execution_dir);
    if let Err(error) = write_json_atomic(&receipt_file, &receipt) {
        warnings.push(format!(
            "runtime final outbox receipt write failed; duplicate suppression will rely on outbox scan: {error}"
        ));
    }
    Ok(())
}

fn find_existing_source_outbox_message(
    harness_home: &Path,
    message: &ChannelOutboundMessage,
    warnings: &mut Vec<String>,
) -> io::Result<Option<PathBuf>> {
    let Some(source_queue_id) = message.source_queue_id.as_deref() else {
        return Ok(None);
    };
    let channel_dir = harness_home.join("state").join("channels");
    let outbox_file = channel_dir.join("outbox.jsonl");
    let indexed = crate::channel_delivery_index::channel_delivery_states_for_source_queue_blocking(
        &channel_dir,
        source_queue_id,
        warnings,
    )?;
    for state in indexed {
        let existing = state.message;
        if existing.source_queue_id.as_deref() == Some(source_queue_id)
            && existing.kind == message.kind
            && (existing.source_completion_file == message.source_completion_file
                || same_outbound_message_without_source(&existing, message))
        {
            return Ok(Some(outbox_file));
        }
    }
    Ok(None)
}

fn same_outbound_message_without_source(
    left: &ChannelOutboundMessage,
    right: &ChannelOutboundMessage,
) -> bool {
    left.platform == right.platform
        && left.account_id == right.account_id
        && left.channel_id == right.channel_id
        && left.user_id == right.user_id
        && left.session_key == right.session_key
        && left.kind == right.kind
        && left.text == right.text
        && left.delivery_intent == right.delivery_intent
        && left.attachments == right.attachments
}

fn append_json_line(path: &Path, value: &impl Serialize) -> io::Result<()> {
    crate::append_jsonl_value(path, value)
}

fn string_field<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a str> {
    for key in keys {
        if let Some(text) = value.get(*key).and_then(Value::as_str) {
            return Some(text);
        }
    }
    None
}

fn inbound_media_artifacts_field(value: &Value, keys: &[&str]) -> Vec<InboundMediaArtifact> {
    for key in keys {
        if let Some(artifacts) = value.get(*key) {
            return serde_json::from_value::<Vec<InboundMediaArtifact>>(artifacts.clone())
                .unwrap_or_default();
        }
    }
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codex_runtime::{CodexRuntimeUsage, CodexToolUseTimeout};
    use crate::{
        AgentProgressDeliveryMessageKind, AgentProgressDeliveryPlanOptions,
        AgentProgressDeliveryRecordOptions, AgentProgressDeliveryStatus, AgentSource,
        ChannelReceiveOptions, ChannelReceiveStatus, ScopedStopOptions, ScopedStopTarget,
        build_source_skill_index, plan_agent_progress_delivery, receive_channel_message,
        record_agent_progress_delivery, record_scoped_stop,
    };
    use std::ffi::OsString;
    use std::fs::OpenOptions;
    use std::io::Write;
    #[cfg(windows)]
    use std::os::windows::fs::OpenOptionsExt;
    use std::sync::Mutex;
    use std::time::{SystemTime, UNIX_EPOCH};

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[test]
    fn authorized_campaign_replay_reuses_one_canonical_terminal() {
        let root = temp_root("goal_terminal_outbox_is_exactly_once_across_source_queues");
        let harness_home = root.join(".agent-harness");
        let transition = test_goal_terminal_transition(GoalTransitionDecision::Complete);
        let mut first = test_goal_terminal_message("queue-goal-1", "Authoritative final.");
        let mut warnings = Vec::new();
        let outcome = append_goal_terminal_outbound_message_once(
            &harness_home,
            None,
            None,
            &transition,
            &mut first,
            &mut warnings,
        )
        .unwrap();
        assert!(outcome.appended());
        assert!(
            first
                .delivery_id
                .as_deref()
                .is_some_and(|value| value.starts_with("delivery:v2:"))
        );

        let mut replay = test_goal_terminal_message("queue-goal-2", "Late duplicate final.");
        let mut replay_transition = transition.clone();
        replay_transition.relation = GoalTransitionRelation::AuthorizedCampaignContinuation;
        let replay_outcome = append_goal_terminal_outbound_message_once(
            &harness_home,
            None,
            None,
            &replay_transition,
            &mut replay,
            &mut warnings,
        )
        .unwrap();
        assert_eq!(
            replay_outcome.disposition,
            FinalOutboxDispositionV1::ReusedCanonicalCampaign
        );
        assert_eq!(replay.source_queue_id.as_deref(), Some("queue-goal-1"));
        assert_eq!(replay.text, "Authoritative final.");
        assert_eq!(
            fs::read_to_string(outcome.outbox_file)
                .unwrap()
                .lines()
                .count(),
            1
        );
        let receipts =
            fs::read_to_string(goal_terminal_outbox_receipts_file(&harness_home)).unwrap();
        assert_eq!(receipts.lines().count(), 2);
        assert!(receipts.contains(r#""state":"committed""#));
        assert!(receipts.contains(r#""state":"appended""#));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn fresh_queue_rejects_foreign_campaign_terminal_reuse() {
        let root = temp_root("fresh_queue_rejects_foreign_campaign_terminal_reuse");
        let harness_home = root.join(".agent-harness");
        let transition = test_goal_terminal_transition(GoalTransitionDecision::Complete);
        let mut first = test_goal_terminal_message("queue-goal-a", "Canonical final.");
        append_goal_terminal_outbound_message_once(
            &harness_home,
            None,
            None,
            &transition,
            &mut first,
            &mut Vec::new(),
        )
        .unwrap();

        let mut foreign = test_goal_terminal_message("queue-fresh-b", "Fresh queue final.");
        let error = append_goal_terminal_outbound_message_once(
            &harness_home,
            None,
            None,
            &transition,
            &mut foreign,
            &mut Vec::new(),
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert_eq!(foreign.source_queue_id.as_deref(), Some("queue-fresh-b"));
        assert_eq!(foreign.text, "Fresh queue final.");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn goal_terminal_notices_are_fixed_and_do_not_expose_runtime_reason() {
        assert_eq!(
            goal_terminal_notice_text(GoalTransitionDecision::NeedsUser),
            Some("The goal needs your input before it can continue.")
        );
        assert_eq!(
            goal_terminal_notice_text(GoalTransitionDecision::NeedsAuthority),
            Some("The goal needs additional authorization before it can continue.")
        );
        assert_eq!(
            goal_terminal_notice_text(GoalTransitionDecision::NeedsOperatorAuth),
            Some("The goal needs an operator to authenticate before it can continue.")
        );
    }

    fn test_goal_terminal_transition(decision: GoalTransitionDecision) -> GoalTransitionReceiptV1 {
        GoalTransitionReceiptV1 {
            schema: crate::goal_transition::GOAL_TRANSITION_SCHEMA.to_string(),
            slice_schema: crate::goal_transition::GOAL_SLICE_SCHEMA.to_string(),
            queue_id: Some("queue-goal-1".to_string()),
            event: GoalTransitionEventKind::NormalCompletion,
            runtime_status: "completed".to_string(),
            runtime_reason: "test".to_string(),
            goal_status: Some("completed".to_string()),
            lineage_id: Some("lineage-1".to_string()),
            campaign_family_id: Some("campaign-1".to_string()),
            lane_digest: Some("a".repeat(64)),
            virtual_session_id: Some("virtual-session-1".to_string()),
            backend_context_generation: Some("generation-1".to_string()),
            source_thread_id: Some("thread-1".to_string()),
            source_turn_id: Some("turn-1".to_string()),
            goal_checksum: Some("sha256:goal".to_string()),
            source_slice_generation: 2,
            decision_generation: 1,
            authority: GoalTransitionAuthority::Ready,
            relation: crate::goal_transition::GoalTransitionRelation::CurrentGoalSlice,
            decision,
            surface: GoalTransitionSurface::CampaignFinal,
            schedule_continuation: false,
            allow_campaign_final: true,
            terminal: true,
            reason: "test".to_string(),
            observed_at_ms: 1,
        }
    }

    fn test_goal_terminal_message(queue_id: &str, text: &str) -> ChannelOutboundMessage {
        ChannelOutboundMessage {
            platform: "telegram".to_string(),
            account_id: Some("acct-goal".to_string()),
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            session_key: "agent:main:telegram:dm:dm-42:user:user-7".to_string(),
            delivery_id: None,
            kind: ChannelOutboundMessageKind::AgentReply,
            source_queue_id: Some(queue_id.to_string()),
            source_completion_file: None,
            presentation: None,
            text: text.to_string(),
            delivery_intent: None,
            attachments: Vec::new(),
        }
    }

    #[test]
    fn receipt_compaction_runs_only_after_terminal_queue_turns() {
        assert!(should_request_ledger_maintenance_after_terminal(
            RuntimeRunOnceStatus::Completed,
            Some("queue-1")
        ));
        assert!(should_request_ledger_maintenance_after_terminal(
            RuntimeRunOnceStatus::Timeout,
            Some("queue-1")
        ));
        assert!(!should_request_ledger_maintenance_after_terminal(
            RuntimeRunOnceStatus::LeaseBusy,
            Some("queue-1")
        ));
        assert!(!should_request_ledger_maintenance_after_terminal(
            RuntimeRunOnceStatus::Completed,
            None
        ));
    }

    #[test]
    fn retry_accounting_reads_per_queue_counts_from_the_hot_index() {
        let root = temp_root("retry_accounting_reads_per_queue_counts_from_the_hot_index");
        let harness_home = root.join(".agent-harness");
        let queue_dir = harness_home.join("state").join("runtime-queue");
        fs::create_dir_all(&queue_dir).unwrap();
        fs::write(
            queue_dir.join("run-once-receipts.jsonl"),
            [
                serde_json::json!({"queueId":"turn:retry","status":"failed-retryable"}).to_string(),
                serde_json::json!({"queueId":"turn:retry","status":"no-prepared-execution"})
                    .to_string(),
                serde_json::json!({"queueId":"turn:retry","status":"retry-pending"}).to_string(),
                serde_json::json!({"queueId":"turn:retry","status":"completed"}).to_string(),
            ]
            .join("\n"),
        )
        .unwrap();
        crate::runtime_worker::rebuild_runtime_queue_state_index(&queue_dir, &mut Vec::new())
            .unwrap();

        assert_eq!(
            count_prior_runtime_failures(&harness_home, "turn:retry").unwrap(),
            3
        );
        assert_eq!(
            count_prior_run_once_status(&harness_home, "turn:retry", "no-prepared-execution")
                .unwrap(),
            1
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn split_outbound_media_directives_extracts_attachments() {
        let root = temp_root("split_outbound_media_directives_extracts_attachments");
        let harness_home = root.join("harness");
        let media = harness_home.join("workspace").join("image.png");
        fs::create_dir_all(media.parent().unwrap()).unwrap();
        fs::write(&media, b"image").unwrap();
        let mut warnings = Vec::new();
        let (text, attachments) = split_outbound_media_directives(
            &harness_home,
            Some("queue-media"),
            Some("telegram"),
            &format!("Here is the file.\nMEDIA:{}\nDone.", media.display()),
            &mut warnings,
        )
        .unwrap();

        assert_eq!(text, "Here is the file.\nDone.");
        assert_eq!(attachments.len(), 1);
        assert_eq!(attachments[0].kind, ChannelOutboundAttachmentKind::Image);
        assert_eq!(attachments[0].mime.as_deref(), Some("image/png"));
        assert_eq!(attachments[0].filename.as_deref(), Some("image.png"));
        assert_eq!(attachments[0].path, media);
        let receipt_text = fs::read_to_string(crate::media_policy_receipts_file(&harness_home))
            .expect("policy receipt should be written");
        assert!(receipt_text.contains("\"verdict\":\"accepted\""));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn outbound_media_parser_masks_protected_spans_and_preserves_unknown_tags() {
        let parsed = parse_outbound_media_directives(
            "```text\nMEDIA:C:\\safe\\example.png\n```\n`MEDIA:C:\\safe\\inline.png`\n> MEDIA:C:\\safe\\quote.png\nMEDIA:C:\\safe\\ship.png\nMEDIA:C:\\safe\\unknown.exe",
        );

        assert_eq!(parsed.directives.len(), 1);
        assert_eq!(
            parsed.directives[0].path,
            PathBuf::from("C:\\safe\\ship.png")
        );
    }

    #[test]
    fn rejected_outbound_media_directive_leaves_visible_note() {
        let root = temp_root("rejected_outbound_media_directive_leaves_visible_note");
        let harness_home = root.join("harness");
        let denied = harness_home
            .join("state")
            .join("channels")
            .join("secret.png");
        fs::create_dir_all(denied.parent().unwrap()).unwrap();
        fs::write(&denied, b"secret").unwrap();
        let mut warnings = Vec::new();

        let (text, attachments) = split_outbound_media_directives(
            &harness_home,
            Some("queue-denied"),
            Some("telegram"),
            &format!("Please send this.\nMEDIA:{}", denied.display()),
            &mut warnings,
        )
        .unwrap();

        assert!(attachments.is_empty());
        assert!(text.contains("[attachment not delivered: denied-prefix]"));
        let receipt_text = fs::read_to_string(crate::media_policy_receipts_file(&harness_home))
            .expect("policy receipt should be written");
        assert!(receipt_text.contains("\"verdict\":\"rejected\""));
        assert!(receipt_text.contains("\"reasonCode\":\"denied-prefix\""));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn rich_media_artifact_ref_resolves_to_attachment_backed_unit() {
        let root = temp_root("rich_media_artifact_ref_resolves_to_attachment_backed_unit");
        let harness_home = root.join("harness");
        let artifact_ref = "agent-harness://inbound-media/telegram/update-1/0.png";
        let media = crate::inbound_media_attachment_root(&harness_home)
            .join("update-1")
            .join("0.png");
        fs::create_dir_all(media.parent().unwrap()).unwrap();
        fs::write(&media, b"image").unwrap();
        let mut presentation = RichMessagePresentation {
            schema: crate::rich_presentation::RICH_MESSAGE_PRESENTATION_SCHEMA.to_string(),
            fallback_text: "image".to_string(),
            blocks: Vec::new(),
            actions: Vec::new(),
            media: vec![crate::rich_presentation::RichPresentationMediaRef {
                attachment_index: None,
                artifact_ref: Some(artifact_ref.to_string()),
                caption: Some("caption".to_string()),
                role: None,
            }],
            link_preview: crate::rich_presentation::RichPresentationLinkPreview::default(),
            delivery_policy: crate::rich_presentation::RichPresentationDeliveryPolicy::default(),
        };
        let mut attachments = Vec::new();
        let mut warnings = Vec::new();

        resolve_presentation_artifact_refs(
            &harness_home,
            Some("queue-artifact-ref"),
            Some("telegram"),
            &mut presentation,
            &mut attachments,
            &mut warnings,
        )
        .unwrap();

        assert_eq!(attachments.len(), 1);
        assert_eq!(attachments[0].path, media);
        assert_eq!(attachments[0].kind, ChannelOutboundAttachmentKind::Image);
        assert_eq!(attachments[0].caption.as_deref(), Some("caption"));
        assert_eq!(presentation.media[0].attachment_index, Some(0));
        let receipt_text = fs::read_to_string(crate::media_policy_receipts_file(&harness_home))
            .expect("policy receipt should be written");
        assert!(receipt_text.contains("\"verdict\":\"accepted\""));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn rich_media_generated_image_artifact_ref_resolves_to_attachment() {
        let root = temp_root("rich_media_generated_image_artifact_ref_resolves_to_attachment");
        let harness_home = root.join("harness");
        let artifact_ref = "agent-harness://generated-images/thread-1/ig_sample.png";
        let media = harness_home
            .join("codex-home")
            .join("generated_images")
            .join("thread-1")
            .join("ig_sample.png");
        fs::create_dir_all(media.parent().unwrap()).unwrap();
        fs::write(&media, b"image").unwrap();
        let mut presentation = RichMessagePresentation {
            schema: crate::rich_presentation::RICH_MESSAGE_PRESENTATION_SCHEMA.to_string(),
            fallback_text: "generated image".to_string(),
            blocks: Vec::new(),
            actions: Vec::new(),
            media: vec![crate::rich_presentation::RichPresentationMediaRef {
                attachment_index: None,
                artifact_ref: Some(artifact_ref.to_string()),
                caption: None,
                role: None,
            }],
            link_preview: crate::rich_presentation::RichPresentationLinkPreview::default(),
            delivery_policy: crate::rich_presentation::RichPresentationDeliveryPolicy::default(),
        };
        let mut attachments = Vec::new();
        let mut warnings = Vec::new();

        resolve_presentation_artifact_refs(
            &harness_home,
            Some("queue-generated-artifact-ref"),
            Some("discord"),
            &mut presentation,
            &mut attachments,
            &mut warnings,
        )
        .unwrap();

        assert_eq!(attachments.len(), 1);
        assert_eq!(attachments[0].path, fs::canonicalize(&media).unwrap());
        assert_eq!(attachments[0].kind, ChannelOutboundAttachmentKind::Image);
        assert_eq!(presentation.media[0].attachment_index, Some(0));
        let receipt_text = fs::read_to_string(crate::media_policy_receipts_file(&harness_home))
            .expect("policy receipt should be written");
        assert!(receipt_text.contains("\"verdict\":\"accepted\""));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn rich_media_artifact_ref_policy_rejects_oversize_resolved_path() {
        let root = temp_root("rich_media_artifact_ref_policy_rejects_oversize_resolved_path");
        let harness_home = root.join("harness");
        let artifact_ref = "agent-harness://inbound-media/telegram/update-1/0.png";
        let media = crate::inbound_media_attachment_root(&harness_home)
            .join("update-1")
            .join("0.png");
        fs::create_dir_all(media.parent().unwrap()).unwrap();
        fs::write(&media, vec![b'x'; 2 * 1024 * 1024]).unwrap();
        let mut presentation = RichMessagePresentation {
            schema: crate::rich_presentation::RICH_MESSAGE_PRESENTATION_SCHEMA.to_string(),
            fallback_text: "image".to_string(),
            blocks: Vec::new(),
            actions: Vec::new(),
            media: vec![crate::rich_presentation::RichPresentationMediaRef {
                attachment_index: None,
                artifact_ref: Some(artifact_ref.to_string()),
                caption: None,
                role: None,
            }],
            link_preview: crate::rich_presentation::RichPresentationLinkPreview::default(),
            delivery_policy: crate::rich_presentation::RichPresentationDeliveryPolicy::default(),
        };
        let config = serde_json::json!({
            "media": {
                "maxMbPerAttachment": 1,
                "allowDirs": [crate::inbound_media_attachment_root(&harness_home)],
                "trustRecentSeconds": null,
                "strict": true
            }
        });
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            serde_json::to_string_pretty(&config).unwrap(),
        )
        .unwrap();
        let mut attachments = Vec::new();
        let mut warnings = Vec::new();

        resolve_presentation_artifact_refs(
            &harness_home,
            Some("queue-artifact-ref"),
            Some("telegram"),
            &mut presentation,
            &mut attachments,
            &mut warnings,
        )
        .unwrap();

        assert!(attachments.is_empty());
        assert_eq!(presentation.media[0].attachment_index, None);
        assert!(
            warnings
                .iter()
                .any(|warning| warning.contains("artifact-ref-policy-rejected"))
        );
        let receipt_text = fs::read_to_string(crate::media_policy_receipts_file(&harness_home))
            .expect("policy receipt should be written");
        assert!(receipt_text.contains("\"verdict\":\"rejected\""));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn media_delivery_lint_warns_or_fails_closed_from_config() {
        let root = temp_root("media_delivery_lint_warns_or_fails_closed_from_config");
        let harness_home = root.join("harness");
        let media = harness_home.join("workspace").join("image.png");
        let mut warnings = Vec::new();

        let warning = maybe_record_media_delivery_lint(
            &harness_home,
            Some("queue-lint"),
            Some("discord"),
            Some("please send the image"),
            &format!("The image is at {}", media.display()),
            0,
            &mut warnings,
        );
        assert!(warning.triggered);
        assert!(!warning.fail_closed);

        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            r#"{"media":{"lintFailClosed":true}}"#,
        )
        .unwrap();
        let failed = maybe_record_media_delivery_lint(
            &harness_home,
            Some("queue-lint-closed"),
            Some("discord"),
            Some("please send the image"),
            &format!("The image is at {}", media.display()),
            0,
            &mut warnings,
        );
        assert!(failed.triggered);
        assert!(failed.fail_closed);
        let receipt = fs::read_to_string(
            harness_home
                .join("state")
                .join("channels")
                .join("media-delivery-lint-receipts.jsonl"),
        )
        .unwrap();
        assert!(receipt.contains("\"status\":\"warning\""));
        assert!(receipt.contains("\"status\":\"failed-closed\""));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn latest_assistant_response_defaults_to_final_only_with_narration_rows() {
        let root =
            temp_root("latest_assistant_response_defaults_to_final_only_with_narration_rows");
        let transcript = root.join("transcript.jsonl");
        append_json_line(
            &transcript,
            &serde_json::json!({"role": "user", "content": "Please check config"}),
        )
        .unwrap();
        append_json_line(
            &transcript,
            &serde_json::json!({"role": "assistant_narration", "content": "Checking config"}),
        )
        .unwrap();
        append_json_line(
            &transcript,
            &serde_json::json!({"role": "assistant", "content": "Done."}),
        )
        .unwrap();

        let response = latest_assistant_response(&transcript, &AssistantNarrationConfig::default())
            .unwrap()
            .unwrap();

        assert_eq!(response.final_text, "Done.");
        assert_eq!(response.outbound_text, "Done.");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn latest_assistant_response_inline_preface_formats_work_log() {
        let root = temp_root("latest_assistant_response_inline_preface_formats_work_log");
        let transcript = root.join("transcript.jsonl");
        append_json_line(
            &transcript,
            &serde_json::json!({"role": "user", "content": "Please check config"}),
        )
        .unwrap();
        append_json_line(
            &transcript,
            &serde_json::json!({"role": "assistant_narration", "content": "Checking   config"}),
        )
        .unwrap();
        append_json_line(
            &transcript,
            &serde_json::json!({"role": "assistant", "content": "Done."}),
        )
        .unwrap();
        let config = AssistantNarrationConfig {
            mode: AssistantNarrationMode::InlinePreface,
            ..AssistantNarrationConfig::default()
        };

        let response = latest_assistant_response(&transcript, &config)
            .unwrap()
            .unwrap();

        assert_eq!(response.final_text, "Done.");
        assert_eq!(
            response.outbound_text,
            "Work log\n---\nChecking config\n\nFinal reply\n---\nDone."
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn run_runtime_queue_once_records_agent_reply_outbox() {
        let _guard = env_lock();
        let root = temp_root("run_runtime_queue_once_records_agent_reply_outbox");
        let source = write_pipeline_source(&root);
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            r#"{
              "skills": {
                "matcher": {"shadowV2Enabled": true},
                "virtualManifest": {"observeEnabled": true}
              }
            }"#,
        )
        .unwrap();
        let skills = build_source_skill_index(&source).unwrap();
        let receive = receive_channel_message(ChannelReceiveOptions {
            source,
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "telegram".to_string(),
            account_id: None,
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "repair memory cron".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: None,
            inbound_event_id: None,
            skill_limit: 3,
            now_ms: 1234,
        })
        .unwrap();
        assert_eq!(receive.status, ChannelReceiveStatus::AgentTurnQueued);
        let exact_account_id = receive.account_id.clone();
        let exact_session_key = receive.session_key.clone();
        let fake_codex = fake_codex_executable(&root);
        let codex_home = root.join("codex-home");
        fs::create_dir_all(&codex_home).unwrap();
        fs::write(codex_home.join("auth.json"), "{}").unwrap();
        let _env = EnvGuard::set("CODEX_HOME", codex_home.into_os_string());

        let report = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            codex_executable: Some(fake_codex),
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();

        assert_eq!(report.receipt.status, RuntimeRunOnceStatus::Completed);
        let prepared_item = report
            .prepare
            .as_ref()
            .and_then(|prepare| prepare.item.as_ref())
            .expect("prepared runtime item");
        let skill_manifest_file = prepared_item
            .virtual_skill_manifest_file
            .clone()
            .expect("observe-enabled virtual skill manifest");
        let skill_delivery_files = prepared_item.skill_delivery_receipt_files.clone();
        assert!(skill_manifest_file.is_file());
        assert!(!skill_delivery_files.is_empty());
        assert!(skill_delivery_files.iter().all(|path| path.is_file()));
        assert_eq!(
            report.prepare.as_ref().unwrap().receipt.status,
            RuntimeExecutionReceiptStatus::Prepared
        );
        assert!(
            report
                .plan
                .as_ref()
                .unwrap()
                .plan_file
                .as_ref()
                .unwrap()
                .is_file()
        );
        assert_eq!(
            report.run.as_ref().unwrap().receipt.status,
            CodexRuntimeRunStatus::Completed
        );
        let completed_queue_id = report.receipt.queue_id.clone().unwrap();
        let outbound = report.outbound_message.unwrap();
        assert_eq!(outbound.kind, ChannelOutboundMessageKind::AgentReply);
        assert_eq!(outbound.platform, "telegram");
        assert_eq!(outbound.channel_id, "dm-42");
        assert_eq!(outbound.user_id, "user-7");
        assert_eq!(outbound.text, "Pipeline fake reply.");
        let presentation = outbound.presentation.as_ref().unwrap();
        assert_eq!(presentation.fallback_text, "Pipeline fake reply.");
        assert_eq!(presentation.blocks.len(), 1);
        let outbox_file = report.outbox_file.unwrap();
        let outbox = fs::read_to_string(outbox_file).unwrap();
        assert!(outbox.contains("\"kind\":\"agent-reply\""));
        assert!(outbox.contains("\"presentation\""));
        assert!(outbox.contains("Pipeline fake reply."));
        let log = fs::read_to_string(
            harness_home
                .join("state")
                .join("logs")
                .join("harness.jsonl"),
        )
        .unwrap();
        assert!(log.contains("runtime.run-once.completed"));
        let lifecycle = fs::read_to_string(
            harness_home
                .join("state")
                .join("memory")
                .join("lifecycle-receipts.jsonl"),
        )
        .unwrap();
        assert!(lifecycle.contains(r#""status":"recorded""#));
        assert!(lifecycle.contains(r#""episodesAppended":2"#));
        let episodes = fs::read_to_string(
            harness_home
                .join("agents")
                .join("main")
                .join("memory")
                .join("episodes.jsonl"),
        )
        .unwrap();
        assert_eq!(episodes.lines().count(), 2);
        let full_lane = crate::lane::FullLaneKeyV1::new(
            "telegram",
            exact_account_id.as_deref().unwrap_or("default"),
            "dm-42",
            "user-7",
            "main",
            "interactive",
            root_working_session_key(&exact_session_key),
            exact_session_key.clone(),
        )
        .unwrap();
        let envelope = crate::resolve_virtual_session_working_context_for_lane(
            crate::VirtualSessionContextQuery {
                harness_home: harness_home.clone(),
                platform: "telegram".to_string(),
                account_id: exact_account_id.clone(),
                channel_id: "dm-42".to_string(),
                user_id: "user-7".to_string(),
                agent_id: "main".to_string(),
                session_key: Some(exact_session_key.clone()),
                now_ms: 1235,
            },
            Some(&full_lane),
        )
        .unwrap();
        assert_eq!(
            envelope.scope_decision.status, "same-virtual-session",
            "completed runtime turn should write a root working-set snapshot"
        );
        assert!(
            envelope.working_set_file.is_some(),
            "exact-lane resolver should return an opaque working-set reference"
        );
        assert!(
            envelope
                .recent_queue_ids
                .iter()
                .any(|queue_id| queue_id == &completed_queue_id)
        );
        let exact_lane = ChannelStateLane::new(
            "telegram",
            exact_account_id.as_deref(),
            "dm-42",
            "user-7",
            "main",
        )
        .unwrap();
        let v2_index = crate::context_rollover::working_set_session_index_v2_file(
            &harness_home,
            &exact_lane,
            &exact_session_key,
        );
        assert!(
            v2_index.is_file(),
            "production completion must write the exact-account v2 session index"
        );
        assert!(
            !crate::context_rollover::working_set_session_index_file(
                &harness_home,
                &exact_session_key
            )
            .is_file(),
            "new production completion must not create an accountless legacy index"
        );
        let v2_index_text = fs::read_to_string(v2_index).unwrap();
        let v2_index_json: Value = serde_json::from_str(&v2_index_text).unwrap();
        assert_eq!(
            v2_index_json.get("accountId").and_then(Value::as_str),
            Some(exact_account_id.as_deref().unwrap_or("default"))
        );
        let authority_receipts = fs::read_to_string(
            harness_home
                .join("state")
                .join("context-rollover")
                .join("virtual-session-authority-receipts.jsonl"),
        )
        .unwrap();
        let authority: Value = serde_json::from_str(
            authority_receipts
                .lines()
                .filter(|line| !line.trim().is_empty())
                .last()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            authority.get("status").and_then(Value::as_str),
            Some("authoritative-v2")
        );
        assert!(
            authority
                .get("laneDigest")
                .and_then(Value::as_str)
                .is_some_and(|value| value.len() == 64)
        );
        assert!(authority.get("backendContextGeneration").is_some());

        let manifest: crate::VirtualSkillManifestV1 =
            serde_json::from_slice(&fs::read(&skill_manifest_file).unwrap()).unwrap();
        let outcome_dir =
            crate::skill_outcome_receipt_dir(&harness_home, &manifest.identity.virtual_session_id);
        let episode_dir =
            crate::skill_episode_receipt_dir(&harness_home, &manifest.identity.virtual_session_id);
        let review_dir = crate::skill_terminal_review_receipt_dir(
            &harness_home,
            &manifest.identity.virtual_session_id,
        );
        assert_eq!(fs::read_dir(outcome_dir).unwrap().count(), 1);
        assert_eq!(
            fs::read_dir(episode_dir).unwrap().count(),
            skill_delivery_files.len()
        );
        assert_eq!(fs::read_dir(review_dir).unwrap().count(), 1);
        assert!(
            !crate::skill_improvement_proposal_dir(
                &harness_home,
                &manifest.identity.virtual_session_id,
            )
            .exists()
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn runtime_source_closure_decisions_have_one_consistent_terminal_contract() {
        let cases = [
            (
                RuntimeSourceClosureDecision::CommittedHandoff,
                RuntimeSourceClosureKindV1::CommittedHandoff,
                crate::RuntimeTerminalDispositionV1::ContinuationHandoff,
                SourceFinalExpectationV1::NotApplicable,
            ),
            (
                RuntimeSourceClosureDecision::TerminalGoalSurface,
                RuntimeSourceClosureKindV1::TerminalGoalSurface,
                crate::RuntimeTerminalDispositionV1::LogicalSuccess,
                SourceFinalExpectationV1::Required,
            ),
            (
                RuntimeSourceClosureDecision::ParkedObserve,
                RuntimeSourceClosureKindV1::ParkedObserve,
                crate::RuntimeTerminalDispositionV1::NeedsUser,
                SourceFinalExpectationV1::Required,
            ),
            (
                RuntimeSourceClosureDecision::ParkedPolicyDenied,
                RuntimeSourceClosureKindV1::ParkedPolicyDenied,
                crate::RuntimeTerminalDispositionV1::NeedsUser,
                SourceFinalExpectationV1::Required,
            ),
            (
                RuntimeSourceClosureDecision::OrdinaryFinal,
                RuntimeSourceClosureKindV1::OrdinaryFinal,
                crate::RuntimeTerminalDispositionV1::LogicalSuccess,
                SourceFinalExpectationV1::Required,
            ),
            (
                RuntimeSourceClosureDecision::SuppressedExactOwnerMismatch,
                RuntimeSourceClosureKindV1::SuppressedExactOwnerMismatch,
                crate::RuntimeTerminalDispositionV1::TerminalSuppression,
                SourceFinalExpectationV1::ExplicitNonDelivery,
            ),
        ];
        for (decision, kind, disposition, expectation) in cases {
            assert_eq!(decision.kind(), kind);
            assert_eq!(decision.terminal_disposition(), disposition);
            assert_eq!(decision.source_final_expectation(), expectation);
        }
    }

    #[test]
    fn active_goal_completed_slice_is_transitioned_before_final_outbox() {
        let _guard = env_lock();
        let root = temp_root("active-goal-pre-outbox-transition");
        let source = write_pipeline_source(&root);
        let harness_home = root.join(".agent-harness");
        let skills = build_source_skill_index(&source).unwrap();
        let receive = receive_channel_message(ChannelReceiveOptions {
            source,
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "telegram".to_string(),
            account_id: None,
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "continue the durable campaign".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: None,
            inbound_event_id: None,
            skill_limit: 3,
            now_ms: 1234,
        })
        .unwrap();
        assert_eq!(receive.status, ChannelReceiveStatus::AgentTurnQueued);
        let codex_home = root.join("codex-home");
        fs::create_dir_all(&codex_home).unwrap();
        fs::write(codex_home.join("auth.json"), "{}").unwrap();
        let _env = EnvGuard::set("CODEX_HOME", codex_home.into_os_string());
        let report = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            codex_executable: Some(fake_active_goal_codex_executable(&root)),
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();
        assert_eq!(report.receipt.status, RuntimeRunOnceStatus::NeedsUser);
        assert_eq!(
            report.run.as_ref().unwrap().receipt.status,
            CodexRuntimeRunStatus::Completed
        );
        assert!(report.outbound_message.as_ref().is_some_and(|message| {
            message.text.contains("remains active") && message.text.contains("observation-only")
        }));
        assert!(report.outbox_file.is_some());
        assert_eq!(
            report.receipt.terminal_disposition,
            Some(crate::RuntimeTerminalDispositionV1::NeedsUser)
        );
        assert_eq!(
            report.receipt.source_final_expectation,
            Some(SourceFinalExpectationV1::Required)
        );
        assert_eq!(
            report.receipt.source_closure_kind,
            Some(RuntimeSourceClosureKindV1::ParkedObserve)
        );
        assert!(report.receipt.continuation_link.is_none());
        let transitions = fs::read_to_string(
            crate::goal_transition::goal_transition_receipts_file(&harness_home),
        )
        .unwrap();
        let transition: GoalTransitionReceiptV1 = serde_json::from_str(
            transitions
                .lines()
                .filter(|line| !line.trim().is_empty())
                .last()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            transition.decision,
            crate::goal_transition::GoalTransitionDecision::Continue
        );
        assert_eq!(transition.surface, GoalTransitionSurface::ProgressOnly);
        assert_eq!(transition.authority, GoalTransitionAuthority::Ready);
        assert!(transition.schedule_continuation);
        assert!(!transition.allow_campaign_final);
        assert!(transition.lineage_id.is_some());
        assert!(
            !report
                .warnings
                .iter()
                .any(|warning| warning.contains("failed closed"))
        );

        let projection_file = harness_home
            .join("state")
            .join("runtime-queue")
            .join("codex-goal-projection-receipts.jsonl");
        let mut newer_projection: Value = serde_json::from_str(
            fs::read_to_string(&projection_file)
                .unwrap()
                .lines()
                .filter(|line| !line.trim().is_empty())
                .last()
                .unwrap(),
        )
        .unwrap();
        newer_projection["queueId"] = Value::String("queue-newer-goal-slice".to_string());
        newer_projection["sourceThreadId"] = Value::String("thread-newer-goal".to_string());
        newer_projection["sourceTurnId"] = Value::String("turn-newer-goal".to_string());
        newer_projection["observationOrder"] = Value::from(
            newer_projection
                .get("observationOrder")
                .and_then(Value::as_u64)
                .unwrap_or(0)
                + 1,
        );
        newer_projection["observedAtMs"] = Value::from(
            newer_projection
                .get("observedAtMs")
                .and_then(Value::as_i64)
                .unwrap_or(0)
                + 1,
        );
        append_json_line(&projection_file, &newer_projection).unwrap();

        let authority_file = harness_home
            .join("state")
            .join("context-rollover")
            .join("virtual-session-authority-receipts.jsonl");
        let mut newer_authority: Value = serde_json::from_str(
            fs::read_to_string(&authority_file)
                .unwrap()
                .lines()
                .filter(|line| !line.trim().is_empty())
                .last()
                .unwrap(),
        )
        .unwrap();
        newer_authority["queueId"] = Value::String("queue-newer-goal-slice".to_string());
        newer_authority["updatedAtMs"] = Value::from(
            newer_authority
                .get("updatedAtMs")
                .and_then(Value::as_i64)
                .unwrap_or(0)
                + 1,
        );
        append_json_line(&authority_file, &newer_authority).unwrap();

        let current_queue = transition.queue_id.as_deref().unwrap();
        let mut replay_warnings = Vec::new();
        let late_transition = evaluate_runtime_goal_transition(
            &harness_home,
            report.run.as_ref().unwrap(),
            RuntimeRunOnceStatus::Completed,
            &report.receipt.continuation,
            &RuntimeGoalAuthorityContext {
                lane_digest: transition.lane_digest.clone(),
                virtual_session_id: transition.virtual_session_id.clone(),
                projection_hint: latest_goal_projection_for_queue(&harness_home, current_queue)
                    .unwrap(),
            },
            &mut replay_warnings,
        )
        .unwrap();
        assert_eq!(
            late_transition.event,
            GoalTransitionEventKind::OlderCompletion
        );
        assert_eq!(late_transition.authority, GoalTransitionAuthority::Stale);
        assert_eq!(
            late_transition.surface,
            GoalTransitionSurface::SuppressStale
        );
        assert!(!late_transition.allow_campaign_final);
        assert!(report.outbox_file.is_some());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn paused_goal_emits_one_sanitized_terminal_notice() {
        let _guard = env_lock();
        let root = temp_root("paused_goal_emits_one_sanitized_terminal_notice");
        let source = write_pipeline_source(&root);
        let harness_home = root.join(".agent-harness");
        let skills = build_source_skill_index(&source).unwrap();
        let receive = receive_channel_message(ChannelReceiveOptions {
            source,
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "telegram".to_string(),
            account_id: Some("acct-goal".to_string()),
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "pause the durable campaign".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: None,
            inbound_event_id: None,
            skill_limit: 3,
            now_ms: 1234,
        })
        .unwrap();
        let codex_home = root.join("codex-home");
        fs::create_dir_all(&codex_home).unwrap();
        fs::write(codex_home.join("auth.json"), "{}").unwrap();
        let _env = EnvGuard::set("CODEX_HOME", codex_home.into_os_string());
        let report = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: receive.queue_id,
            codex_executable: Some(fake_terminal_goal_codex_executable(
                &root,
                "paused",
                "raw credential-like material must not surface",
            )),
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();
        assert_eq!(report.receipt.status, RuntimeRunOnceStatus::Completed);
        let outbound = report.outbound_message.as_ref().unwrap();
        assert_eq!(outbound.text, "The goal is paused.");
        assert!(!outbound.text.contains("credential"));
        let outbox = fs::read_to_string(report.outbox_file.as_ref().unwrap()).unwrap();
        assert_eq!(outbox.lines().count(), 1);
        assert!(outbox.contains("The goal is paused."));
        assert!(!outbox.contains("credential-like"));
        let terminal_receipts =
            fs::read_to_string(goal_terminal_outbox_receipts_file(&harness_home)).unwrap();
        assert!(terminal_receipts.contains(r#""decision":"pause""#));
        assert!(terminal_receipts.contains(r#""state":"appended""#));
        let transition_receipts = fs::read_to_string(
            crate::goal_transition::goal_transition_receipts_file(&harness_home),
        )
        .unwrap();
        let transition: GoalTransitionReceiptV1 = serde_json::from_str(
            transition_receipts
                .lines()
                .filter(|line| !line.trim().is_empty())
                .last()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(transition.decision, GoalTransitionDecision::Pause);
        assert_eq!(transition.surface, GoalTransitionSurface::TerminalNotice);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn active_goal_budget_boundary_blocks_continuation_and_emits_one_notice() {
        let _guard = env_lock();
        let root = temp_root("active_goal_budget_boundary_blocks_continuation");
        let source = write_pipeline_source(&root);
        let harness_home = root.join(".agent-harness");
        let skills = build_source_skill_index(&source).unwrap();
        let receive = receive_channel_message(ChannelReceiveOptions {
            source,
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "telegram".to_string(),
            account_id: Some("acct-goal".to_string()),
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "continue within the campaign budget".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: None,
            inbound_event_id: None,
            skill_limit: 3,
            now_ms: 1234,
        })
        .unwrap();
        let queue_id = receive.queue_id.unwrap();
        let pending_text = fs::read_to_string(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("pending.jsonl"),
        )
        .unwrap();
        let pending: Value = pending_text
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .find(|value| value.get("queueId").and_then(Value::as_str) == Some(&queue_id))
            .unwrap();
        let session_key = pending.get("sessionKey").and_then(Value::as_str).unwrap();
        let lane = crate::lane::FullLaneKeyV1::new(
            pending.get("platform").and_then(Value::as_str).unwrap(),
            pending.get("accountId").and_then(Value::as_str).unwrap(),
            pending.get("channelId").and_then(Value::as_str).unwrap(),
            pending.get("userId").and_then(Value::as_str).unwrap(),
            pending.get("agentId").and_then(Value::as_str).unwrap(),
            pending.get("runtimeClass").and_then(Value::as_str).unwrap(),
            root_working_session_key(session_key),
            session_key,
        )
        .unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "goalAutonomy": {
                    "mode": "active",
                    "activeLaneDigests": [lane.identity_hash().unwrap()],
                    "maxSlices": 1
                }
            }))
            .unwrap(),
        )
        .unwrap();
        let codex_home = root.join("codex-home");
        fs::create_dir_all(&codex_home).unwrap();
        fs::write(codex_home.join("auth.json"), "{}").unwrap();
        let _env = EnvGuard::set("CODEX_HOME", codex_home.into_os_string());
        let report = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: Some(queue_id),
            codex_executable: Some(fake_active_goal_codex_executable(&root)),
            timeout_ms: 60 * 60 * 1_000,
            idle_timeout_ms: 10 * 60 * 1_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();
        assert_eq!(report.receipt.status, RuntimeRunOnceStatus::Completed);
        assert!(report.receipt.child_queue_id.is_none());
        assert_eq!(
            report
                .outbound_message
                .as_ref()
                .map(|message| message.text.as_str()),
            Some("The goal budget is exhausted.")
        );
        assert!(
            report
                .warnings
                .iter()
                .any(|warning| { warning.contains("hard=1800000ms idle=300000ms") })
        );
        let budget_receipts = fs::read_to_string(
            crate::goal_budget::goal_campaign_budget_receipts_file(&harness_home),
        )
        .unwrap();
        assert!(budget_receipts.contains(r#""boundary":"slice-count""#));
        let transitions = fs::read_to_string(
            crate::goal_transition::goal_transition_receipts_file(&harness_home),
        )
        .unwrap();
        let transition: GoalTransitionReceiptV1 = serde_json::from_str(
            transitions
                .lines()
                .filter(|line| !line.trim().is_empty())
                .last()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(transition.decision, GoalTransitionDecision::BudgetExhausted);
        assert!(!transition.schedule_continuation);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn active_goal_continuation_is_exactly_once_and_acknowledged_after_child_lease() {
        let _guard = env_lock();
        let root = temp_root("active-goal-exactly-once-continuation");
        let source = write_pipeline_source(&root);
        let harness_home = root.join(".agent-harness");
        let skills = build_source_skill_index(&source).unwrap();
        let receive = receive_channel_message(ChannelReceiveOptions {
            source,
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "telegram".to_string(),
            account_id: Some("acct-goal".to_string()),
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "continue the durable campaign".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: None,
            inbound_event_id: None,
            skill_limit: 3,
            now_ms: 1234,
        })
        .unwrap();
        let parent_queue_id = receive.queue_id.clone().unwrap();
        let pending_text = fs::read_to_string(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("pending.jsonl"),
        )
        .unwrap();
        let pending: Value = pending_text
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .find(|value| value.get("queueId").and_then(Value::as_str) == Some(&parent_queue_id))
            .unwrap();
        let session_key = pending.get("sessionKey").and_then(Value::as_str).unwrap();
        let lane = crate::lane::FullLaneKeyV1::new(
            pending.get("platform").and_then(Value::as_str).unwrap(),
            pending
                .get("accountId")
                .and_then(Value::as_str)
                .unwrap_or("default"),
            pending.get("channelId").and_then(Value::as_str).unwrap(),
            pending.get("userId").and_then(Value::as_str).unwrap(),
            pending.get("agentId").and_then(Value::as_str).unwrap(),
            pending.get("runtimeClass").and_then(Value::as_str).unwrap(),
            root_working_session_key(session_key),
            session_key,
        )
        .unwrap();
        let lane_digest = lane.identity_hash().unwrap();
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "goalAutonomy": {
                    "mode": "active",
                    "activeLaneDigests": [lane_digest]
                }
            }))
            .unwrap(),
        )
        .unwrap();
        let codex_home = root.join("codex-home");
        fs::create_dir_all(&codex_home).unwrap();
        fs::write(codex_home.join("auth.json"), "{}").unwrap();
        let _env = EnvGuard::set("CODEX_HOME", codex_home.into_os_string());
        let executable = fake_active_goal_codex_executable(&root);
        let first = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: Some(parent_queue_id),
            codex_executable: Some(executable.clone()),
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();
        assert_eq!(first.receipt.status, RuntimeRunOnceStatus::Skipped);
        assert!(first.outbox_file.is_none());
        assert_eq!(
            first.receipt.terminal_disposition,
            Some(crate::RuntimeTerminalDispositionV1::ContinuationHandoff)
        );
        assert_eq!(
            first.receipt.source_final_expectation,
            Some(SourceFinalExpectationV1::NotApplicable)
        );
        assert_eq!(
            first.receipt.source_closure_kind,
            Some(RuntimeSourceClosureKindV1::CommittedHandoff)
        );
        let first_child = first.receipt.child_queue_id.clone().unwrap();
        assert_eq!(
            first.receipt.child_session_key.as_deref(),
            Some(session_key)
        );
        let intents_file = crate::goal_continuation::goal_continuation_intents_file(&harness_home);
        let first_intent: crate::goal_continuation::GoalContinuationIntentV1 =
            fs::read_to_string(&intents_file)
                .unwrap()
                .lines()
                .filter_map(|line| serde_json::from_str(line).ok())
                .find(
                    |intent: &crate::goal_continuation::GoalContinuationIntentV1| {
                        intent.child_queue_id.as_deref() == Some(first_child.as_str())
                    },
                )
                .unwrap();
        assert_eq!(first_intent.campaign_slice_generation, 1);
        assert_eq!(first_intent.recovery_continuation_index, 0);
        crate::goal_continuation::ensure_goal_continuation_enqueued(
            &harness_home,
            &first_intent.intent_key,
            2000,
        )
        .unwrap();
        crate::goal_continuation::reconcile_goal_continuation_intents(&harness_home, 2001).unwrap();
        let pending_after_replay = fs::read_to_string(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("pending.jsonl"),
        )
        .unwrap();
        assert_eq!(
            pending_after_replay
                .lines()
                .filter_map(|line| serde_json::from_str::<Value>(line).ok())
                .filter(|value| {
                    value.get("continuationIntentKey").and_then(Value::as_str)
                        == Some(first_intent.intent_key.as_str())
                })
                .count(),
            1,
            "replay and reconciliation must retain one logical child"
        );
        let second = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: Some(first_child.clone()),
            codex_executable: Some(executable),
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();
        assert_eq!(second.receipt.status, RuntimeRunOnceStatus::Skipped);
        let acknowledged = crate::goal_continuation::latest_goal_continuation_intent(
            &harness_home,
            &first_intent.intent_key,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            acknowledged.status,
            crate::goal_continuation::GoalContinuationIntentStatus::Acknowledged
        );
        assert_eq!(
            acknowledged.child_queue_id.as_deref(),
            Some(first_child.as_str())
        );
        assert_eq!(
            second.receipt.continuation.campaign_slice_generation,
            Some(1)
        );
        assert_eq!(second.receipt.continuation.continuation_index, Some(0));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn deadline_drain_operation_plan_replay_creates_one_child_and_one_eventual_final() {
        let _guard = env_lock();
        let root = temp_root("deadline-drain-operation-plan-replay");
        let source = write_pipeline_source(&root);
        let harness_home = root.join(".agent-harness");
        let skills = build_source_skill_index(&source).unwrap();
        let receive = receive_channel_message(ChannelReceiveOptions {
            source,
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "telegram".to_string(),
            account_id: Some("acct-plan".to_string()),
            channel_id: "dm-plan".to_string(),
            user_id: "user-plan".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "continue the exact-lane operation plan".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: None,
            inbound_event_id: None,
            skill_limit: 3,
            now_ms: 1234,
        })
        .unwrap();
        let parent_queue_id = receive.queue_id.unwrap();
        let pending_text = fs::read_to_string(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("pending.jsonl"),
        )
        .unwrap();
        let pending: Value = pending_text
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .find(|value| value.get("queueId").and_then(Value::as_str) == Some(&parent_queue_id))
            .unwrap();
        let session_key = pending.get("sessionKey").and_then(Value::as_str).unwrap();
        let lane_digest = crate::lane::FullLaneKeyV1::new(
            pending.get("platform").and_then(Value::as_str).unwrap(),
            pending.get("accountId").and_then(Value::as_str).unwrap(),
            pending.get("channelId").and_then(Value::as_str).unwrap(),
            pending.get("userId").and_then(Value::as_str).unwrap(),
            pending.get("agentId").and_then(Value::as_str).unwrap(),
            pending.get("runtimeClass").and_then(Value::as_str).unwrap(),
            root_working_session_key(session_key),
            session_key,
        )
        .unwrap()
        .identity_hash()
        .unwrap();
        crate::operation_plan::create_operation_plan_v2(
            crate::operation_plan::CreateOperationPlanOptionsV2 {
                options: crate::operation_plan::CreateOperationPlanOptions {
                    harness_home: harness_home.clone(),
                    plan_id: "plan-deadline-drain".to_string(),
                    origin_queue_id: Some(parent_queue_id.clone()),
                    session_key: session_key.to_string(),
                    agent_id: "main".to_string(),
                    goal: "finish the exact-lane replay".to_string(),
                    acceptance_criteria: None,
                    constraints: None,
                    max_open_items: None,
                    max_fanout: None,
                    now_ms: 1235,
                },
                lane_digest,
            },
        )
        .unwrap();
        crate::operation_plan::add_operation_plan_item(
            crate::operation_plan::OperationPlanAddItemOptions {
                harness_home: harness_home.clone(),
                plan_id: "plan-deadline-drain".to_string(),
                item_id: "item-running".to_string(),
                title: "Run replay".to_string(),
                body: "Remain active across the deadline drain".to_string(),
                depends_on: Vec::new(),
                acceptance_criteria: None,
                risk: None,
                now_ms: 1236,
            },
        )
        .unwrap();
        let mut item_version = 1;
        for status in [
            crate::operation_plan::OperationPlanItemStatus::Ready,
            crate::operation_plan::OperationPlanItemStatus::Running,
        ] {
            let update = crate::operation_plan::update_operation_plan_item(
                crate::operation_plan::OperationPlanUpdateItemOptions {
                    harness_home: harness_home.clone(),
                    plan_id: "plan-deadline-drain".to_string(),
                    item_id: "item-running".to_string(),
                    expected_item_version: Some(item_version),
                    status: Some(status),
                    title: None,
                    body: None,
                    depends_on: None,
                    assignee: None,
                    worker_job_id: None,
                    queue_id: None,
                    risk: None,
                    evidence: None,
                    replace_evidence: false,
                    add_evidence: Vec::new(),
                    now_ms: 1236 + item_version as i64,
                },
            )
            .unwrap();
            item_version = update.item.version;
        }
        let plan = crate::operation_plan::show_operation_plan(
            crate::operation_plan::OperationPlanShowOptions {
                harness_home: harness_home.clone(),
                plan_id: "plan-deadline-drain".to_string(),
            },
        )
        .unwrap();
        let checkpoint_body = "checkpoint: replay work remains active".to_string();
        let checkpoint = crate::TaskContinuationCheckpointV1 {
            schema: crate::TASK_CONTINUATION_CHECKPOINT_SCHEMA.to_string(),
            authority_kind: crate::ContinuationAuthorityKindV1::OperationPlan,
            authority_id: "plan-deadline-drain".to_string(),
            authority_version: plan.plan.version,
            active_item_id: Some("item-running".to_string()),
            active_item_version: Some(item_version),
            checkpoint_digest: crate::task_transition::sha256_hex(checkpoint_body.as_bytes()),
            checkpoint: checkpoint_body,
        };
        let assistant_text = format!(
            "Work is checkpointed for continuation.\n{}{}{}",
            crate::TASK_CONTINUATION_MARKER_OPEN,
            serde_json::to_string(&checkpoint).unwrap(),
            crate::TASK_CONTINUATION_MARKER_CLOSE,
        );
        let codex_home = root.join("codex-home");
        fs::create_dir_all(&codex_home).unwrap();
        fs::write(codex_home.join("auth.json"), "{}").unwrap();
        let _env = EnvGuard::set("CODEX_HOME", codex_home.into_os_string());
        let first = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: Some(parent_queue_id),
            codex_executable: Some(fake_deadline_drain_checkpoint_codex_executable(
                &root,
                &assistant_text,
            )),
            timeout_ms: 10_000,
            idle_timeout_ms: 10_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();
        assert_eq!(first.receipt.status, RuntimeRunOnceStatus::Skipped);
        assert!(first.outbox_file.is_none());
        assert!(first.outbound_message.is_none());
        let evaluation = first.receipt.task_drain_evaluation.as_ref().unwrap();
        assert!(evaluation.schedule_continuation, "{}", evaluation.reason);
        let child_queue_id = first.receipt.child_queue_id.clone().unwrap();
        let intent = fs::read_to_string(crate::goal_continuation::goal_continuation_intents_file(
            &harness_home,
        ))
        .unwrap()
        .lines()
        .filter_map(|line| {
            serde_json::from_str::<crate::goal_continuation::GoalContinuationIntentV1>(line).ok()
        })
        .find(|intent| intent.child_queue_id.as_deref() == Some(child_queue_id.as_str()))
        .unwrap();
        crate::goal_continuation::ensure_goal_continuation_enqueued(
            &harness_home,
            &intent.intent_key,
            6000,
        )
        .unwrap();
        crate::goal_continuation::reconcile_goal_continuation_intents(&harness_home, 6001).unwrap();
        let pending_after_restart = fs::read_to_string(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("pending.jsonl"),
        )
        .unwrap();
        assert_eq!(
            pending_after_restart
                .lines()
                .filter_map(|line| serde_json::from_str::<Value>(line).ok())
                .filter(|value| {
                    value.get("continuationIntentKey").and_then(Value::as_str)
                        == Some(intent.intent_key.as_str())
                })
                .count(),
            1,
            "restart reconciliation must preserve one logical child"
        );

        let second = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: Some(child_queue_id.clone()),
            codex_executable: Some(fake_codex_executable(&root)),
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();
        assert_eq!(second.receipt.status, RuntimeRunOnceStatus::Completed);
        assert_eq!(
            second
                .outbound_message
                .as_ref()
                .map(|message| message.text.as_str()),
            Some("Pipeline fake reply.")
        );
        let outbox = fs::read_to_string(second.outbox_file.as_ref().unwrap()).unwrap();
        assert_eq!(outbox.lines().count(), 1, "the lineage must emit one final");
        let acknowledged = crate::goal_continuation::latest_goal_continuation_intent(
            &harness_home,
            &intent.intent_key,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            acknowledged.status,
            crate::goal_continuation::GoalContinuationIntentStatus::Acknowledged
        );
        assert_eq!(
            acknowledged.child_queue_id.as_deref(),
            Some(child_queue_id.as_str())
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn deadline_drain_explicit_task_family_replay_creates_one_child_and_one_final() {
        let _guard = env_lock();
        let root = temp_root("explicit-task-replay");
        let source = write_pipeline_source(&root);
        let harness_home = root.join(".agent-harness");
        let skills = build_source_skill_index(&source).unwrap();
        let receive = receive_channel_message(ChannelReceiveOptions {
            source,
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "telegram".to_string(),
            account_id: Some("acct-task".to_string()),
            channel_id: "dm-task".to_string(),
            user_id: "user-task".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "complete a long ordinary task".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: None,
            inbound_event_id: None,
            skill_limit: 3,
            now_ms: 1234,
        })
        .unwrap();
        let parent_queue_id = receive.queue_id.unwrap();
        let codex_home = root.join("codex-home");
        fs::create_dir_all(&codex_home).unwrap();
        fs::write(codex_home.join("auth.json"), "{}").unwrap();
        let _env = EnvGuard::set("CODEX_HOME", codex_home.into_os_string());

        let first = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: Some(parent_queue_id.clone()),
            codex_executable: Some(fake_explicit_checkpoint_deadline_codex_executable(&root)),
            timeout_ms: 10_000,
            idle_timeout_ms: 10_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();
        assert_eq!(first.receipt.status, RuntimeRunOnceStatus::Skipped);
        assert!(
            first.outbound_message.is_none(),
            "drained parent cannot own final"
        );
        let evaluation = first.receipt.task_drain_evaluation.as_ref().unwrap();
        assert!(evaluation.schedule_continuation, "{}", evaluation.reason);
        let child_queue_id = first.receipt.child_queue_id.clone().unwrap();
        let pending = fs::read_to_string(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("pending.jsonl"),
        )
        .unwrap();
        let child = pending
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .find(|value| value.get("queueId").and_then(Value::as_str) == Some(&child_queue_id))
            .unwrap();
        assert_eq!(
            child.get("taskSliceGeneration").and_then(Value::as_u64),
            Some(1)
        );
        assert!(child.get("taskFamilyId").and_then(Value::as_str).is_some());
        assert!(
            child
                .get("taskRootQueueId")
                .and_then(Value::as_str)
                .is_some()
        );
        assert!(child.get("campaignSliceGeneration").is_none());

        let second = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: Some(child_queue_id),
            codex_executable: Some(fake_codex_executable(&root)),
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();
        assert_eq!(second.receipt.status, RuntimeRunOnceStatus::Completed);
        assert_eq!(
            second
                .outbound_message
                .as_ref()
                .map(|message| message.text.as_str()),
            Some("Pipeline fake reply.")
        );
        let outbox = fs::read_to_string(second.outbox_file.as_ref().unwrap()).unwrap();
        assert_eq!(
            outbox.lines().count(),
            1,
            "task lineage must emit one final"
        );
        let budget_dir = harness_home
            .join("state")
            .join("runtime-queue")
            .join("task-budgets");
        assert_eq!(fs::read_dir(budget_dir).unwrap().count(), 1);
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(windows)]
    #[test]
    fn indeterminate_drain_uses_one_bounded_recovery_then_needs_user() {
        let _guard = env_lock();
        let root = temp_root("disposition-recovery");
        let source = write_pipeline_source(&root);
        let harness_home = root.join(".agent-harness");
        let skills = build_source_skill_index(&source).unwrap();
        let receive = receive_channel_message(ChannelReceiveOptions {
            source,
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "telegram".to_string(),
            account_id: Some("acct-recovery".to_string()),
            channel_id: "dm-recovery".to_string(),
            user_id: "user-recovery".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "complete a task whose drain outcome is initially unclear".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: None,
            inbound_event_id: None,
            skill_limit: 3,
            now_ms: 1234,
        })
        .unwrap();
        let parent_queue_id = receive.queue_id.unwrap();
        let codex_home = root.join("codex-home");
        fs::create_dir_all(&codex_home).unwrap();
        fs::write(codex_home.join("auth.json"), "{}").unwrap();
        let _env = EnvGuard::set("CODEX_HOME", codex_home.into_os_string());

        let first = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: Some(parent_queue_id),
            codex_executable: Some(fake_missing_disposition_deadline_codex_executable(&root)),
            timeout_ms: 10_000,
            idle_timeout_ms: 10_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();
        assert_eq!(first.receipt.status, RuntimeRunOnceStatus::Skipped);
        assert!(first.outbound_message.is_none());
        assert!(matches!(
            first
                .receipt
                .task_drain_evaluation
                .as_ref()
                .map(|evaluation| &evaluation.disposition),
            Some(crate::DrainDispositionV1::Indeterminate)
        ));
        let child_queue_id = first.receipt.child_queue_id.clone().unwrap();
        let pending = fs::read_to_string(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("pending.jsonl"),
        )
        .unwrap();
        let child = pending
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .find(|value| value.get("queueId").and_then(Value::as_str) == Some(&child_queue_id))
            .unwrap();
        assert_eq!(
            child
                .get("dispositionRecoveryDepth")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert!(
            child["messageText"]
                .as_str()
                .is_some_and(|text| text.starts_with("Disposition recovery only:"))
        );

        let second = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: Some(child_queue_id),
            codex_executable: Some(fake_missing_disposition_recovery_codex_executable(&root)),
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();
        assert_eq!(second.receipt.status, RuntimeRunOnceStatus::FailedTerminal);
        assert!(second.receipt.child_queue_id.is_none());
        assert_eq!(
            second
                .outbound_message
                .as_ref()
                .map(|message| message.text.as_str()),
            Some(
                "The task is parked because the bounded disposition-recovery pass could not establish a valid final outcome. Please send a new message with the desired next step."
            )
        );
        let intents = fs::read_to_string(crate::goal_continuation::goal_continuation_intents_file(
            &harness_home,
        ))
        .unwrap();
        let mut recovery_intent_keys = intents
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .filter(|value| {
                value.get("schema").and_then(Value::as_str)
                    == Some(crate::goal_continuation::TASK_CONTINUATION_INTENT_SCHEMA)
            })
            .filter(|value| {
                value
                    .get("dispositionRecoveryDepth")
                    .and_then(Value::as_u64)
                    == Some(1)
            })
            .filter_map(|value| {
                value
                    .get("intentKey")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
            .collect::<Vec<_>>();
        recovery_intent_keys.sort();
        recovery_intent_keys.dedup();
        assert_eq!(
            recovery_intent_keys.len(),
            1,
            "one indeterminate task family may enqueue at most one disposition recovery"
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn productive_deadline_authoritative_replay_uses_prepared_authority_and_renews_lease_before_timer()
     {
        let _guard = env_lock();
        let root = temp_root("productive-renewal");
        let source = write_pipeline_source(&root);
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join(crate::HARNESS_CONFIG_FILE_NAME),
            r#"{"orchestration":{"features":{"productiveDeadlineV1":{"mode":"authoritative","enabledAgentIds":["main"],"renewalIncrementMs":60000,"productiveWindowMs":30000,"hardCapMs":180000,"maxRenewals":2,"pendingExactLaneWorkBlocksRenewal":true}}}}"#,
        )
        .unwrap();
        let skills = build_source_skill_index(&source).unwrap();
        let receive = receive_channel_message(ChannelReceiveOptions {
            source,
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "telegram".to_string(),
            account_id: Some("acct-productive".to_string()),
            channel_id: "dm-productive".to_string(),
            user_id: "user-productive".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "finish work across the initial deadline".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: None,
            inbound_event_id: None,
            skill_limit: 3,
            now_ms: 1234,
        })
        .unwrap();
        let codex_home = root.join("codex-home");
        fs::create_dir_all(&codex_home).unwrap();
        fs::write(codex_home.join("auth.json"), "{}").unwrap();
        let _env = EnvGuard::set("CODEX_HOME", codex_home.into_os_string());

        let report = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: receive.queue_id,
            codex_executable: Some(fake_productive_deadline_renewal_codex_executable(&root)),
            timeout_ms: 60_000,
            idle_timeout_ms: 70_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();
        assert_eq!(report.receipt.status, RuntimeRunOnceStatus::Completed);
        assert_eq!(
            report
                .outbound_message
                .as_ref()
                .map(|message| message.text.as_str()),
            Some("Productive task completed after the original deadline.")
        );
        let decisions = fs::read_to_string(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("productive-deadline-decisions.jsonl"),
        )
        .unwrap();
        let decision: Value = serde_json::from_str(decisions.lines().next().unwrap()).unwrap();
        assert_eq!(decision["decision"], "applied");
        assert_eq!(decisions.lines().count(), 1);
        let lease_receipts = fs::read_to_string(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("lease-renewal-receipts.jsonl"),
        )
        .unwrap();
        let lease: Value = serde_json::from_str(lease_receipts.lines().next().unwrap()).unwrap();
        assert!(matches!(
            lease["status"].as_str(),
            Some("applied" | "not-required")
        ));
        assert_eq!(decision["queueLeaseRenewalId"], lease["renewalId"]);
        assert!(
            lease["renewedExpiresAtMs"].as_i64().unwrap()
                >= decision["candidateDeadlineAtMs"].as_i64().unwrap() + 3 * 60 * 1_000
        );
        let steer_file = harness_home
            .join("state")
            .join("runtime-queue")
            .join("codex-turn-steer-receipts.jsonl");
        if steer_file.is_file() {
            assert!(
                !fs::read_to_string(steer_file)
                    .unwrap()
                    .contains("sent-deadline-drain")
            );
        }
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(windows)]
    #[test]
    fn partial_final_at_bounded_yield_preserves_timeout_and_one_recovery_replay() {
        let _guard = env_lock();
        let replay: Value = serde_json::from_str(include_str!(
            "../tests/fixtures/continuity-effects/bounded-yield-partial-final-replay.json"
        ))
        .unwrap();
        assert_eq!(replay["expected"]["primaryOutcome"], "absolute-timeout");
        let root = temp_root("bounded-yield-partial-final-replay");
        let source = write_pipeline_source(&root);
        let harness_home = root.join(".agent-harness");
        let skills = build_source_skill_index(&source).unwrap();
        let receive = receive_channel_message(ChannelReceiveOptions {
            source,
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "telegram".to_string(),
            account_id: Some("acct-yield".to_string()),
            channel_id: "dm-yield".to_string(),
            user_id: "user-yield".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "continue a bounded task whose cooperative final may be partial".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: None,
            inbound_event_id: None,
            skill_limit: 3,
            now_ms: 1234,
        })
        .unwrap();
        let codex_home = root.join("codex-home");
        fs::create_dir_all(&codex_home).unwrap();
        fs::write(codex_home.join("auth.json"), "{}").unwrap();
        let _env = EnvGuard::set("CODEX_HOME", codex_home.into_os_string());

        let report = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: receive.queue_id,
            codex_executable: Some(fake_partial_final_bounded_yield_codex_executable(&root)),
            // Leave enough wall-clock budget for a cold PowerShell app-server
            // process to complete the JSON-RPC handshake before exercising the
            // bounded-yield timeout path itself.
            timeout_ms: 10_000,
            idle_timeout_ms: 10_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();

        assert_eq!(
            report.run.as_ref().unwrap().receipt.status,
            CodexRuntimeRunStatus::Timeout,
            "I9/I10: advisory marker syntax cannot replace the primary absolute-timeout outcome"
        );
        assert_eq!(report.receipt.status, RuntimeRunOnceStatus::Skipped);
        assert!(report.receipt.child_queue_id.is_some());
        assert!(report.outbound_message.is_none());
        assert_ne!(
            report
                .run
                .as_ref()
                .unwrap()
                .receipt
                .drain_disposition_error
                .as_deref(),
            Some("multiple-or-unbalanced-disposition-markers")
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn expired_external_effect_settlement_is_source_correlated_and_exactly_once() {
        let _guard = env_lock();
        let root = temp_root("expired-external-effect-settlement");
        let source = write_pipeline_source(&root);
        let harness_home = root.join(".agent-harness");
        let skills = build_source_skill_index(&source).unwrap();
        let receive = receive_channel_message(ChannelReceiveOptions {
            source,
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "telegram".to_string(),
            account_id: Some("acct-expiry".to_string()),
            channel_id: "dm-expiry".to_string(),
            user_id: "user-expiry".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "request one protected connector action".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: None,
            inbound_event_id: Some("provider-expiry-1".to_string()),
            skill_limit: 3,
            now_ms: 10,
        })
        .unwrap();
        let queue_id = receive.queue_id.unwrap();
        let admission = crate::begin_external_effect_request(
            &harness_home,
            &crate::ExternalEffectRequestContextV1 {
                exact_lane_digest: format!("sha256:{}", "1".repeat(64)),
                logical_lineage_id: "virtual-session-expiry".to_string(),
                source_queue_id: queue_id.clone(),
                source_session_key_digest: format!("sha256:{}", "2".repeat(64)),
                approval_authority_digest: format!("sha256:{}", "3".repeat(64)),
            },
            &crate::McpElicitationDescriptorV1 {
                connector: "github".to_string(),
                action: "create_issue".to_string(),
                params_digest: format!("sha256:{}", "4".repeat(64)),
                action_summary: "github/create_issue: create a tracked issue".to_string(),
                mode: "form".to_string(),
            },
            &crate::ConnectorApprovalPolicyV1::default(),
        )
        .unwrap();
        let intent = match admission {
            crate::ExternalEffectAdmissionV1::NeedsUser { intent, .. } => intent,
            other => panic!("unexpected external-effect admission {other:?}"),
        };
        let snapshot = harness_home
            .join("state")
            .join("external-effects")
            .join("latest")
            .join(format!("{}.json", intent.effect_id));
        let mut value: Value = serde_json::from_slice(&fs::read(&snapshot).unwrap()).unwrap();
        value["approvalToken"]["expiresAtMs"] = Value::from(100);
        fs::write(&snapshot, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
        let reconcile = crate::reconcile_expired_external_effect_approvals(
            &harness_home,
            &crate::ExternalEffectExpiryReconcileRequestV1 {
                now_ms: 100,
                max_rows: 1,
                after_effect_id: None,
            },
        )
        .unwrap();
        let resolution = reconcile.resolutions.first().unwrap();

        let first =
            settle_expired_external_effect_approval(&harness_home, resolution, 100).unwrap();
        let replay =
            settle_expired_external_effect_approval(&harness_home, resolution, 101).unwrap();
        assert_eq!(first.source_queue_id, queue_id);
        assert!(first.notice_appended);
        assert!(first.queue_terminal_receipt_appended);
        assert!(first.progress_terminal_appended);
        assert!(!replay.notice_appended);
        assert!(!replay.queue_terminal_receipt_appended);
        assert!(!replay.progress_terminal_appended);
        let outbox = fs::read_to_string(
            harness_home
                .join("state")
                .join("channels")
                .join("outbox.jsonl"),
        )
        .unwrap();
        assert_eq!(outbox.lines().count(), 1);
        let run_receipts = fs::read_to_string(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("run-once-receipts.jsonl"),
        )
        .unwrap();
        assert_eq!(
            run_receipts
                .lines()
                .filter(|line| line.contains(&resolution.resolution_id))
                .count(),
            1
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn auth_deferred_turn_has_zero_delivery_side_effects_and_wakes_once_when_ready() {
        let _guard = env_lock();
        let root = temp_root("auth_deferred_turn_zero_side_effect_replay");
        let source = write_pipeline_source(&root);
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            r#"{"backendAuth":{"runtimeGateEnabled":true}}"#,
        )
        .unwrap();
        let skills = build_source_skill_index(&source).unwrap();
        let receive = receive_channel_message(ChannelReceiveOptions {
            source,
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "telegram".to_string(),
            account_id: None,
            channel_id: "dm-auth".to_string(),
            user_id: "user-auth".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "wait for operator auth without replying".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: None,
            inbound_event_id: None,
            skill_limit: 3,
            now_ms: 1234,
        })
        .unwrap();
        let queue_id = receive.queue_id.unwrap();

        let report = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: Some(queue_id.clone()),
            codex_executable: Some(fake_failing_codex_executable(&root)),
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();

        assert_eq!(report.receipt.status, RuntimeRunOnceStatus::AuthDeferred);
        assert_eq!(
            report.run.as_ref().unwrap().receipt.status,
            CodexRuntimeRunStatus::PreflightBlocked
        );
        assert!(report.receipt.reason.contains("needs-operator-auth"));
        assert!(report.outbound_message.is_none());
        assert!(report.outbox_file.is_none());
        assert!(
            !harness_home
                .join("state")
                .join("channels")
                .join("outbox.jsonl")
                .exists()
        );
        let receipts = fs::read_to_string(&report.receipts_file).unwrap();
        assert!(receipts.contains(r#""status":"auth-deferred""#));
        assert!(!receipts.contains(r#""status":"failed-terminal""#));
        let deferred_capacity =
            crate::inspect_runtime_queue_capacity(crate::RuntimeQueueCapacityOptions {
                harness_home: harness_home.clone(),
            })
            .unwrap();
        assert_eq!(deferred_capacity.claimable_items, 0);

        let codex_home =
            crate::resolve_or_create_provider_codex_home(&harness_home, "openai").unwrap();
        let ready = crate::BackendAuthStateV1 {
            schema: crate::BACKEND_AUTH_STATE_SCHEMA.to_string(),
            provider: "openai".to_string(),
            lifecycle_state: crate::BackendAuthLifecycleState::Ready,
            readiness_generation: 1,
            observed_at_ms: 1235,
            operation_id: None,
            requires_openai_auth: Some(false),
            failure_code: None,
        };
        crate::persist_backend_auth_state(&codex_home, &ready).unwrap();
        assert_eq!(
            crate::resume_all_backend_auth_defer_intents(&harness_home, "openai", &ready).unwrap(),
            1
        );
        assert_eq!(
            crate::resume_all_backend_auth_defer_intents(&harness_home, "openai", &ready).unwrap(),
            0
        );
        let wake_capacity =
            crate::inspect_runtime_queue_capacity(crate::RuntimeQueueCapacityOptions {
                harness_home: harness_home.clone(),
            })
            .unwrap();
        assert_eq!(wake_capacity.claimable_items, 1);
        assert_eq!(wake_capacity.claimable_queue_ids, vec![queue_id]);
        let wake_receipts = fs::read_to_string(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("run-once-receipts.jsonl"),
        )
        .unwrap();
        assert_eq!(
            wake_receipts.matches(r#""status":"retry-pending""#).count(),
            1
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn run_runtime_queue_once_keeps_media_attachments_in_rich_presentation() {
        let _guard = env_lock();
        let root = temp_root("run_runtime_queue_once_keeps_media_attachments_in_rich_presentation");
        fs::create_dir_all(&root).unwrap();
        let media_file = root.join("final-artifact.txt");
        fs::write(&media_file, "package d media smoke").unwrap();
        let source = write_pipeline_source(&root);
        let harness_home = root.join(".agent-harness");
        let skills = build_source_skill_index(&source).unwrap();
        let receive = receive_channel_message(ChannelReceiveOptions {
            source,
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "discord".to_string(),
            account_id: None,
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "send the generated artifact".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: None,
            inbound_event_id: None,
            skill_limit: 3,
            now_ms: 1234,
        })
        .unwrap();
        assert_eq!(receive.status, ChannelReceiveStatus::AgentTurnQueued);
        let fake_codex = fake_media_final_codex_executable(&root, &media_file);
        let codex_home = root.join("codex-home");
        fs::create_dir_all(&codex_home).unwrap();
        fs::write(codex_home.join("auth.json"), "{}").unwrap();
        let _env = EnvGuard::set("CODEX_HOME", codex_home.into_os_string());

        let report = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            codex_executable: Some(fake_codex),
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();

        assert_eq!(report.receipt.status, RuntimeRunOnceStatus::Completed);
        let outbound = report.outbound_message.unwrap();
        assert_eq!(outbound.kind, ChannelOutboundMessageKind::AgentReply);
        assert_eq!(outbound.platform, "discord");
        assert_eq!(outbound.text, "Here is the file.\nDone.");
        assert_eq!(outbound.attachments.len(), 1);
        assert_eq!(outbound.attachments[0].path, media_file);
        assert_eq!(outbound.attachments[0].mime.as_deref(), Some("text/plain"));
        let presentation = outbound.presentation.as_ref().unwrap();
        assert_eq!(presentation.fallback_text, "Here is the file.\nDone.");
        assert_eq!(presentation.media.len(), 1);
        assert_eq!(presentation.media[0].attachment_index, Some(0));
        let outbox = fs::read_to_string(report.outbox_file.unwrap()).unwrap();
        assert!(outbox.contains("\"presentation\""));
        assert!(outbox.contains("\"attachments\""));
        assert!(outbox.contains("\"attachmentIndex\":0"));
        assert!(!outbox.contains("MEDIA:"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn run_runtime_queue_once_repairs_outbox_for_already_recorded_completion() {
        let _guard = env_lock();
        let root =
            temp_root("run_runtime_queue_once_repairs_outbox_for_already_recorded_completion");
        let (harness_home, queue_id, execution_dir, _env) =
            prepare_already_recorded_completion_without_outbox(&root);

        let report = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: Some(queue_id.clone()),
            codex_executable: Some(fake_codex_executable(&root)),
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();

        assert_eq!(report.receipt.status, RuntimeRunOnceStatus::Completed);
        let run = report.run.as_ref().unwrap();
        assert_eq!(run.receipt.event_count, 0);
        assert!(run.receipt.reason.contains("already recorded"));
        assert!(report.outbox_file.is_some());
        let outbound = report.outbound_message.unwrap();
        assert_eq!(outbound.source_queue_id.as_deref(), Some(queue_id.as_str()));
        assert_eq!(outbound.text, "Pipeline fake reply.");
        let outbox = fs::read_to_string(report.outbox_file.unwrap()).unwrap();
        assert_eq!(outbox.lines().count(), 1);
        assert!(outbox.contains(r#""sourceQueueId""#));
        assert!(outbox.contains("Pipeline fake reply."));
        assert!(final_outbox_receipt_file(&execution_dir).is_file());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn already_recorded_completion_repair_keeps_progress_panel_out_of_final_outbox() {
        let _guard = env_lock();
        let root = temp_root(
            "already_recorded_completion_repair_keeps_progress_panel_out_of_final_outbox",
        );
        let (harness_home, queue_id, execution_dir, _env) =
            prepare_already_recorded_completion_without_outbox_for_platform(
                &root, "telegram", "dm-42", "user-7",
            );
        let completion_receipt: Value = serde_json::from_str(
            &fs::read_to_string(execution_dir.join("codex-runtime-completion-receipt.json"))
                .unwrap(),
        )
        .unwrap();
        let transcript = PathBuf::from(
            completion_receipt
                .get("transcriptFile")
                .and_then(Value::as_str)
                .unwrap(),
        );
        append_json_line(
            &transcript,
            &serde_json::json!({
                "schema": "agent-harness.transcript-message.v1",
                "role": "assistant_narration",
                "content": "progress: step 1\nprogress: step 2\nprogress: step 3"
            }),
        )
        .unwrap();
        append_json_line(
            &transcript,
            &serde_json::json!({
                "schema": "agent-harness.transcript-message.v1",
                "role": "assistant",
                "content": "Final answer only."
            }),
        )
        .unwrap();

        let report = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: Some(queue_id.clone()),
            codex_executable: Some(fake_codex_executable(&root)),
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();

        assert_eq!(report.receipt.status, RuntimeRunOnceStatus::Completed);
        let outbound = report.outbound_message.unwrap();
        assert_eq!(outbound.source_queue_id.as_deref(), Some(queue_id.as_str()));
        assert_eq!(outbound.text, "Final answer only.");
        assert!(!outbound.text.contains("progress:"));
        let presentation = outbound.presentation.as_ref().unwrap();
        assert_eq!(presentation.fallback_text, "Final answer only.");
        assert!(
            presentation
                .blocks
                .iter()
                .all(|block| !serde_json::to_string(block).unwrap().contains("progress:"))
        );
        let outbox = fs::read_to_string(report.outbox_file.unwrap()).unwrap();
        assert!(outbox.contains("Final answer only."));
        assert!(!outbox.contains("progress:"));
        assert!(final_outbox_receipt_file(&execution_dir).is_file());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn already_recorded_completion_repair_keeps_progress_panel_out_of_discord_final_outbox() {
        let _guard = env_lock();
        let root = temp_root(
            "already_recorded_completion_repair_keeps_progress_panel_out_of_discord_final_outbox",
        );
        let (harness_home, queue_id, execution_dir, _env) =
            prepare_already_recorded_completion_without_outbox_for_platform(
                &root,
                "discord",
                "discord-dm-42",
                "discord-user-7",
            );
        let completion_receipt: Value = serde_json::from_str(
            &fs::read_to_string(execution_dir.join("codex-runtime-completion-receipt.json"))
                .unwrap(),
        )
        .unwrap();
        let transcript = PathBuf::from(
            completion_receipt
                .get("transcriptFile")
                .and_then(Value::as_str)
                .unwrap(),
        );
        append_json_line(
            &transcript,
            &serde_json::json!({
                "schema": "agent-harness.transcript-message.v1",
                "role": "assistant_narration",
                "content": "progress: discord step 1\nprogress: discord step 2"
            }),
        )
        .unwrap();
        append_json_line(
            &transcript,
            &serde_json::json!({
                "schema": "agent-harness.transcript-message.v1",
                "role": "assistant",
                "content": "Discord final answer only."
            }),
        )
        .unwrap();

        let report = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: Some(queue_id.clone()),
            codex_executable: Some(fake_codex_executable(&root)),
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();

        assert_eq!(report.receipt.status, RuntimeRunOnceStatus::Completed);
        let outbound = report.outbound_message.unwrap();
        assert_eq!(outbound.platform, "discord");
        assert_eq!(outbound.source_queue_id.as_deref(), Some(queue_id.as_str()));
        assert_eq!(outbound.text, "Discord final answer only.");
        assert!(!outbound.text.contains("progress:"));
        let outbox = fs::read_to_string(report.outbox_file.unwrap()).unwrap();
        assert!(outbox.contains(r#""platform":"discord""#));
        assert!(outbox.contains("Discord final answer only."));
        assert!(!outbox.contains("progress:"));
        assert!(final_outbox_receipt_file(&execution_dir).is_file());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn run_runtime_queue_once_skips_duplicate_for_existing_final_outbox_marker() {
        let _guard = env_lock();
        let root =
            temp_root("run_runtime_queue_once_skips_duplicate_for_existing_final_outbox_marker");
        let (harness_home, queue_id, execution_dir, _env) =
            prepare_already_recorded_completion_without_outbox(&root);
        let outbox_file = harness_home
            .join("state")
            .join("channels")
            .join("outbox.jsonl");
        fs::create_dir_all(outbox_file.parent().unwrap()).unwrap();
        let queue_context = find_queue_channel_context(&harness_home, &queue_id, &mut Vec::new())
            .unwrap()
            .expect("prepared queue retains its exact channel lane");
        let seeded = ChannelOutboundMessage {
            platform: queue_context.platform,
            account_id: queue_context.account_id,
            channel_id: queue_context.channel_id,
            user_id: queue_context.user_id,
            session_key: queue_context.session_key,
            delivery_id: None,
            kind: ChannelOutboundMessageKind::AgentReply,
            source_queue_id: Some(queue_id.clone()),
            source_completion_file: Some(
                execution_dir.join("codex-runtime-completion-receipt.json"),
            ),
            text: "Pipeline fake reply.".to_string(),
            presentation: None,
            delivery_intent: None,
            attachments: Vec::new(),
        };
        append_json_line(&outbox_file, &seeded).unwrap();
        record_final_outbox_receipt(
            &execution_dir,
            seeded.source_completion_file.as_deref(),
            &seeded,
            &outbox_file,
            &mut Vec::new(),
        )
        .unwrap();

        let report = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: Some(queue_id.clone()),
            codex_executable: Some(fake_codex_executable(&root)),
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();

        assert_eq!(report.receipt.status, RuntimeRunOnceStatus::Completed);
        assert!(report.outbound_message.is_none());
        assert_eq!(report.outbox_file.as_ref(), Some(&outbox_file));
        let outbox = fs::read_to_string(outbox_file).unwrap();
        assert_eq!(outbox.lines().count(), 1);
        assert!(
            report
                .warnings
                .iter()
                .any(|warning| warning.contains("already enqueued")
                    || warning.contains("already present"))
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn run_runtime_queue_once_respects_emoji_accent_off_config() {
        let _guard = env_lock();
        let root = temp_root("run_runtime_queue_once_respects_emoji_accent_off_config");
        let source = write_pipeline_source(&root);
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            r#"{"response":{"emojiAccentMode":"off"}}"#,
        )
        .unwrap();
        let skills = build_source_skill_index(&source).unwrap();
        let receive = receive_channel_message(ChannelReceiveOptions {
            source,
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "telegram".to_string(),
            account_id: None,
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "repair memory cron".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: None,
            inbound_event_id: None,
            skill_limit: 3,
            now_ms: 1234,
        })
        .unwrap();
        assert_eq!(receive.status, ChannelReceiveStatus::AgentTurnQueued);
        let fake_codex = fake_codex_executable(&root);
        let codex_home = root.join("codex-home");
        fs::create_dir_all(&codex_home).unwrap();
        fs::write(codex_home.join("auth.json"), "{}").unwrap();
        let _env = EnvGuard::set("CODEX_HOME", codex_home.into_os_string());

        let report = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            codex_executable: Some(fake_codex),
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();

        let outbound = report.outbound_message.unwrap();
        assert_eq!(outbound.kind, ChannelOutboundMessageKind::AgentReply);
        assert_eq!(outbound.text, "Pipeline fake reply.");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn run_runtime_queue_once_treats_busy_lease_lock_as_retryable_no_work() {
        let _guard = env_lock();
        let root = temp_root("run_runtime_queue_once_treats_busy_lease_lock_as_retryable_no_work");
        let source = write_pipeline_source(&root);
        let harness_home = root.join(".agent-harness");
        let skills = build_source_skill_index(&source).unwrap();
        let receive = receive_channel_message(ChannelReceiveOptions {
            source,
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "telegram".to_string(),
            account_id: None,
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "repair memory cron".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: None,
            inbound_event_id: None,
            skill_limit: 3,
            now_ms: 1234,
        })
        .unwrap();
        assert_eq!(receive.status, ChannelReceiveStatus::AgentTurnQueued);
        let queue_id = receive.queue_id.unwrap();
        let _held_lock = hold_runtime_queue_lease_lock(
            &harness_home.join("state").join("runtime-queue"),
            "interactive",
        );

        let report = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: Some(queue_id.clone()),
            codex_executable: None,
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();

        assert_eq!(report.receipt.status, RuntimeRunOnceStatus::LeaseBusy);
        assert_eq!(report.receipt.queue_id.as_deref(), Some(queue_id.as_str()));
        assert!(report.receipt.reason.contains("lease lock is busy"));
        assert_eq!(
            report.prepare.as_ref().unwrap().receipt.status,
            RuntimeExecutionReceiptStatus::LeaseBusy
        );
        assert_eq!(
            report.prepare.as_ref().unwrap().receipt.queue_id.as_deref(),
            Some(queue_id.as_str())
        );
        assert!(report.plan.is_none());
        assert!(report.run.is_none());
        assert!(report.outbound_message.is_none());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prepared_protocol_error_terminalizes_after_threshold() {
        let _guard = env_lock();
        let root = temp_root("prepared_protocol_error_terminalizes_after_threshold");
        let source = write_pipeline_source(&root);
        let harness_home = root.join(".agent-harness");
        let skills = build_source_skill_index(&source).unwrap();
        let receive = receive_channel_message(ChannelReceiveOptions {
            source,
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "telegram".to_string(),
            account_id: None,
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "repair memory cron".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: None,
            inbound_event_id: None,
            skill_limit: 3,
            now_ms: 1234,
        })
        .unwrap();
        assert_eq!(receive.status, ChannelReceiveStatus::AgentTurnQueued);
        let queue_id = receive.queue_id.unwrap();
        let prepare = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: Some(queue_id.clone()),
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();
        let execution_dir = prepare.receipt.execution_dir.clone().unwrap();
        fs::remove_file(execution_dir.join("execution-receipt.json")).unwrap();
        release_runtime_queue_lease(&harness_home, &queue_id).unwrap();

        for attempt in 1..=3 {
            let report = run_runtime_queue_once(RuntimeRunOnceOptions {
                harness_home: harness_home.clone(),
                queue_id: Some(queue_id.clone()),
                codex_executable: None,
                timeout_ms: 30_000,
                idle_timeout_ms: 30_000,
                prompt_options: PromptAssemblyOptions {
                    harness_home: Some(harness_home.clone()),
                    ..PromptAssemblyOptions::default()
                },
            })
            .unwrap();
            if attempt < 3 {
                assert_eq!(
                    report.receipt.status,
                    RuntimeRunOnceStatus::NoPreparedExecution
                );
            } else {
                assert_eq!(report.receipt.status, RuntimeRunOnceStatus::FailedTerminal);
            }
        }

        let run_once_receipts = fs::read_to_string(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("run-once-receipts.jsonl"),
        )
        .unwrap();
        assert!(run_once_receipts.contains(r#""status":"failed-terminal""#));
        assert!(run_once_receipts.contains("preparedExecutionTerminalizationReason"));
        let quarantine_dir = harness_home
            .join("state")
            .join("runtime-queue")
            .join("quarantine");
        assert_eq!(fs::read_dir(quarantine_dir).unwrap().count(), 1);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn scoped_stop_suppresses_missing_prepared_execution_before_no_prepared_churn() {
        let _guard = env_lock();
        let root =
            temp_root("scoped_stop_suppresses_missing_prepared_execution_before_no_prepared_churn");
        let source = write_pipeline_source(&root);
        let harness_home = root.join(".agent-harness");
        let skills = build_source_skill_index(&source).unwrap();
        let receive = receive_channel_message(ChannelReceiveOptions {
            source,
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "discord".to_string(),
            account_id: None,
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "stop ghost queue".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: None,
            inbound_event_id: None,
            skill_limit: 3,
            now_ms: 1234,
        })
        .unwrap();
        assert_eq!(receive.status, ChannelReceiveStatus::AgentTurnQueued);
        let queue_id = receive.queue_id.unwrap();
        let prepare = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: Some(queue_id.clone()),
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();
        let execution_dir = prepare.receipt.execution_dir.clone().unwrap();
        fs::remove_file(execution_dir.join("execution-receipt.json")).unwrap();
        release_runtime_queue_lease(&harness_home, &queue_id).unwrap();
        record_scoped_stop(ScopedStopOptions {
            harness_home: harness_home.clone(),
            target: ScopedStopTarget::QueueItem {
                queue_id: queue_id.clone(),
            },
            reason: "operator scoped stop for missing prepared execution".to_string(),
            now_ms: 5678,
        })
        .unwrap();

        let report = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: Some(queue_id.clone()),
            codex_executable: None,
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();

        assert_eq!(report.receipt.status, RuntimeRunOnceStatus::Suppressed);
        assert_eq!(report.receipt.terminal_control_matched, Some(true));
        assert_eq!(
            report.receipt.terminal_control_source.as_deref(),
            Some("scoped-stop")
        );
        let run_once_receipts = fs::read_to_string(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("run-once-receipts.jsonl"),
        )
        .unwrap();
        assert!(run_once_receipts.contains(r#""status":"suppressed""#));
        assert!(!run_once_receipts.contains(r#""status":"no-prepared-execution""#));
        assert_terminal_progress_once_then_silent(&harness_home, "discord", &queue_id);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn untargeted_terminal_control_suppression_appends_progress_with_queue_id() {
        let _guard = env_lock();
        let root =
            temp_root("untargeted_terminal_control_suppression_appends_progress_with_queue_id");
        let source = write_pipeline_source(&root);
        let harness_home = root.join(".agent-harness");
        let skills = build_source_skill_index(&source).unwrap();
        let receive = receive_channel_message(ChannelReceiveOptions {
            source,
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "discord".to_string(),
            account_id: None,
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "stop ghost queue".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: None,
            inbound_event_id: None,
            skill_limit: 3,
            now_ms: 1234,
        })
        .unwrap();
        assert_eq!(receive.status, ChannelReceiveStatus::AgentTurnQueued);
        let queue_id = receive.queue_id.unwrap();
        record_scoped_stop(ScopedStopOptions {
            harness_home: harness_home.clone(),
            target: ScopedStopTarget::QueueItem {
                queue_id: queue_id.clone(),
            },
            reason: "operator scoped stop before untargeted selection".to_string(),
            now_ms: 5678,
        })
        .unwrap();

        let report = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            codex_executable: None,
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();

        assert_eq!(report.receipt.status, RuntimeRunOnceStatus::Suppressed);
        assert_eq!(report.receipt.queue_id.as_deref(), Some(queue_id.as_str()));
        let progress_events = fs::read_to_string(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("progress-events.jsonl"),
        )
        .unwrap();
        let suppressed_events = progress_events
            .lines()
            .filter(|line| {
                line.contains(&queue_id) && line.contains("suppressed by terminal control")
            })
            .count();
        assert_eq!(suppressed_events, 1, "{progress_events}");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn quarantine_suppression_records_terminal_progress_once_then_silent() {
        let _guard = env_lock();
        let root = temp_root("quarantine_suppression_records_terminal_progress_once_then_silent");
        let source = write_pipeline_source(&root);
        let harness_home = root.join(".agent-harness");
        let skills = build_source_skill_index(&source).unwrap();
        let receive = receive_channel_message(ChannelReceiveOptions {
            source,
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "telegram".to_string(),
            account_id: None,
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "quarantine this queue".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: None,
            inbound_event_id: None,
            skill_limit: 3,
            now_ms: 1234,
        })
        .unwrap();
        assert_eq!(receive.status, ChannelReceiveStatus::AgentTurnQueued);
        let queue_id = receive.queue_id.unwrap();
        write_runtime_queue_quarantine_marker(
            &harness_home,
            &queue_id,
            "operator quarantined queue before runtime",
            5_678,
        )
        .unwrap();

        let report = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: Some(queue_id.clone()),
            codex_executable: None,
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();

        assert_eq!(report.receipt.status, RuntimeRunOnceStatus::Suppressed);
        assert_eq!(report.receipt.terminal_control_matched, Some(true));
        assert_eq!(
            report.receipt.terminal_control_source.as_deref(),
            Some("quarantine")
        );
        assert_terminal_progress_once_then_silent(&harness_home, "telegram", &queue_id);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn terminal_control_queue_gets_one_final_edit_then_silence() {
        quarantine_suppression_records_terminal_progress_once_then_silent();
    }

    #[test]
    fn e2e_1_ghost_queue_replay_from_sanitized_fixture() {
        #[derive(serde::Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Fixture {
            platform: String,
            account_id: Option<String>,
            channel_id: String,
            user_id: String,
            agent_id: String,
            session_key: String,
            message: String,
            inbound_event_kind: String,
            inbound_event_id: String,
            duplicate_inbound_event_id: String,
            stop_message: String,
            stop_inbound_event_kind: String,
            stop_inbound_event_id: String,
            first_now_ms: i64,
            duplicate_now_ms: i64,
            stop_now_ms: i64,
            expected: GhostQueueExpected,
        }

        #[derive(serde::Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct GhostQueueExpected {
            stop_scope: String,
            terminal_control_source: String,
            suppressed_run_once_reason: String,
            suppression_receipt_count: usize,
        }

        let fixture: Fixture = serde_json::from_str(include_str!(
            "../tests/fixtures/round16/e2e1-ghost-queue-replay.json"
        ))
        .unwrap();
        let _guard = env_lock();
        let root = temp_root("e2e_1_ghost_queue_replay_from_sanitized_fixture");
        let source = write_pipeline_source(&root);
        let harness_home = root.join(".agent-harness");
        let skills = build_source_skill_index(&source).unwrap();

        let first = receive_channel_message(ChannelReceiveOptions {
            source: source.clone(),
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills.clone(),
            platform: fixture.platform.clone(),
            account_id: fixture.account_id.clone(),
            channel_id: fixture.channel_id.clone(),
            user_id: fixture.user_id.clone(),
            agent_id: Some(fixture.agent_id.clone()),
            session_key: Some(fixture.session_key.clone()),
            message: fixture.message.clone(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: Some(fixture.inbound_event_kind.clone()),
            inbound_event_id: Some(fixture.inbound_event_id.clone()),
            skill_limit: 3,
            now_ms: fixture.first_now_ms,
        })
        .unwrap();
        assert_eq!(first.status, ChannelReceiveStatus::AgentTurnQueued);
        let queue_id = first.queue_id.clone().unwrap();

        let duplicate = receive_channel_message(ChannelReceiveOptions {
            source: source.clone(),
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills.clone(),
            platform: fixture.platform.clone(),
            account_id: fixture.account_id.clone(),
            channel_id: fixture.channel_id.clone(),
            user_id: fixture.user_id.clone(),
            agent_id: Some(fixture.agent_id.clone()),
            session_key: Some(fixture.session_key.clone()),
            message: fixture.message.clone(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: Some(fixture.inbound_event_kind.clone()),
            inbound_event_id: Some(fixture.duplicate_inbound_event_id.clone()),
            skill_limit: 3,
            now_ms: fixture.duplicate_now_ms,
        })
        .unwrap();
        assert_eq!(duplicate.status, ChannelReceiveStatus::DuplicateSuppressed);
        assert_eq!(
            duplicate.duplicate_sibling_of.as_deref(),
            Some(queue_id.as_str())
        );
        assert_eq!(duplicate.inbound_canonical_id, first.inbound_canonical_id);

        let prepare = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: Some(queue_id.clone()),
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();
        let execution_dir = prepare.receipt.execution_dir.clone().unwrap();
        fs::remove_file(execution_dir.join("execution-receipt.json")).unwrap();
        release_runtime_queue_lease(&harness_home, &queue_id).unwrap();

        let stop = receive_channel_message(ChannelReceiveOptions {
            source,
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: fixture.platform.clone(),
            account_id: fixture.account_id.clone(),
            channel_id: fixture.channel_id.clone(),
            user_id: fixture.user_id.clone(),
            agent_id: Some(fixture.agent_id.clone()),
            session_key: Some(fixture.session_key.clone()),
            message: fixture.stop_message.clone(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: Some(fixture.stop_inbound_event_kind.clone()),
            inbound_event_id: Some(fixture.stop_inbound_event_id.clone()),
            skill_limit: 3,
            now_ms: fixture.stop_now_ms,
        })
        .unwrap();
        assert_eq!(stop.status, ChannelReceiveStatus::CommandApplied);
        let receipt_text =
            fs::read_to_string(stop.command_apply.as_ref().unwrap().receipts_file.clone()).unwrap();
        assert!(
            receipt_text.contains(&format!(r#""stopScope":"{}""#, fixture.expected.stop_scope))
        );
        assert!(receipt_text.contains(&queue_id));

        let report = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: Some(queue_id.clone()),
            codex_executable: None,
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();
        assert_eq!(report.receipt.status, RuntimeRunOnceStatus::Suppressed);
        assert_eq!(report.receipt.terminal_control_matched, Some(true));
        assert_eq!(
            report.receipt.terminal_control_source.as_deref(),
            Some(fixture.expected.terminal_control_source.as_str())
        );
        assert_eq!(
            report.receipt.suppressed_run_once_reason.as_deref(),
            Some(fixture.expected.suppressed_run_once_reason.as_str())
        );
        assert!(report.plan.is_none() || report.run.is_none());

        let run_once_receipts = fs::read_to_string(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("run-once-receipts.jsonl"),
        )
        .unwrap();
        let suppression_count = run_once_receipts
            .lines()
            .filter(|line| {
                line.contains(&queue_id)
                    && line.contains(r#""status":"suppressed""#)
                    && line.contains(&format!(
                        r#""terminalControlSource":"{}""#,
                        fixture.expected.terminal_control_source
                    ))
            })
            .count();
        assert_eq!(
            suppression_count,
            fixture.expected.suppression_receipt_count
        );
        assert!(!run_once_receipts.contains(r#""status":"no-prepared-execution""#));
        assert_terminal_progress_once_then_silent(&harness_home, &fixture.platform, &queue_id);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn e2e_2_duplicate_ingress_stop_progress_replay() {
        let _guard = env_lock();
        let root = temp_root("e2e_2_duplicate_ingress_stop_progress_replay");
        let source = write_pipeline_source(&root);
        let harness_home = root.join(".agent-harness");
        let skills = build_source_skill_index(&source).unwrap();

        let first = receive_channel_message(ChannelReceiveOptions {
            source: source.clone(),
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills.clone(),
            platform: "telegram".to_string(),
            account_id: None,
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: Some("telegram:dm-42:user-7:main".to_string()),
            message: "repair memory cron".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: Some("message".to_string()),
            inbound_event_id: Some("provider-msg-e2e-2-a".to_string()),
            skill_limit: 3,
            now_ms: 1000,
        })
        .unwrap();
        assert_eq!(first.status, ChannelReceiveStatus::AgentTurnQueued);
        let queue_a = first.queue_id.clone().unwrap();

        let duplicate = receive_channel_message(ChannelReceiveOptions {
            source: source.clone(),
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills.clone(),
            platform: "telegram".to_string(),
            account_id: None,
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: Some("telegram:dm-42:user-7:main".to_string()),
            message: "repair memory cron".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: Some("message".to_string()),
            inbound_event_id: Some("provider-msg-e2e-2-a".to_string()),
            skill_limit: 3,
            now_ms: 1001,
        })
        .unwrap();
        assert_eq!(duplicate.status, ChannelReceiveStatus::DuplicateSuppressed);
        assert_eq!(
            duplicate.duplicate_sibling_of.as_deref(),
            Some(queue_a.as_str())
        );

        let sibling = receive_channel_message(ChannelReceiveOptions {
            source: source.clone(),
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills.clone(),
            platform: "telegram".to_string(),
            account_id: None,
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: Some("telegram:dm-42:user-7:main".to_string()),
            message: "follow-up work".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: Some("message".to_string()),
            inbound_event_id: Some("provider-msg-e2e-2-b".to_string()),
            skill_limit: 3,
            now_ms: 1002,
        })
        .unwrap();
        assert_eq!(sibling.status, ChannelReceiveStatus::AgentTurnQueued);
        let queue_b = sibling.queue_id.clone().unwrap();

        let stop = receive_channel_message(ChannelReceiveOptions {
            source,
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "telegram".to_string(),
            account_id: None,
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: Some("telegram:dm-42:user-7:main".to_string()),
            message: "/stop operator replay".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: Some("interaction".to_string()),
            inbound_event_id: Some("provider-interaction-e2e-2-stop".to_string()),
            skill_limit: 3,
            now_ms: 1003,
        })
        .unwrap();
        assert_eq!(stop.status, ChannelReceiveStatus::CommandApplied);
        let receipt_text =
            fs::read_to_string(stop.command_apply.as_ref().unwrap().receipts_file.clone()).unwrap();
        assert!(receipt_text.contains("\"stopScope\":\"active-session-siblings\""));
        assert!(receipt_text.contains(&queue_a));
        assert!(receipt_text.contains(&queue_b));

        let report = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: Some(queue_a.clone()),
            codex_executable: None,
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();
        assert_eq!(report.receipt.status, RuntimeRunOnceStatus::Suppressed);
        assert_eq!(report.receipt.terminal_control_matched, Some(true));
        assert_eq!(
            report.receipt.terminal_control_source.as_deref(),
            Some("scoped-stop")
        );
        assert_terminal_progress_once_then_silent(&harness_home, "telegram", &queue_a);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn run_runtime_queue_once_records_runtime_failure_error_outbox() {
        let _guard = env_lock();
        let root = temp_root("run_runtime_queue_once_records_runtime_failure_error_outbox");
        let source = write_pipeline_source(&root);
        let harness_home = root.join(".agent-harness");
        let skills = build_source_skill_index(&source).unwrap();
        let receive = receive_channel_message(ChannelReceiveOptions {
            source,
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "telegram".to_string(),
            account_id: None,
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "run a blocked command".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: None,
            inbound_event_id: None,
            skill_limit: 3,
            now_ms: 1234,
        })
        .unwrap();
        assert_eq!(receive.status, ChannelReceiveStatus::AgentTurnQueued);
        let fake_codex = fake_failing_codex_executable(&root);
        let codex_home = root.join("codex-home");
        fs::create_dir_all(&codex_home).unwrap();
        fs::write(codex_home.join("auth.json"), "{}").unwrap();
        let _env = EnvGuard::set("CODEX_HOME", codex_home.into_os_string());

        let report = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            codex_executable: Some(fake_codex),
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();

        assert_eq!(report.receipt.status, RuntimeRunOnceStatus::FailedTerminal);
        assert_eq!(
            report.run.as_ref().unwrap().receipt.status,
            CodexRuntimeRunStatus::ProtocolError
        );
        let outbound = report.outbound_message.unwrap();
        assert_eq!(outbound.kind, ChannelOutboundMessageKind::ErrorReply);
        assert_eq!(outbound.platform, "telegram");
        assert!(outbound.text.contains("failed-terminal"));
        assert!(outbound.text.contains("synthetic app-server refusal"));
        let outbox = fs::read_to_string(report.outbox_file.unwrap()).unwrap();
        assert!(outbox.contains("\"kind\":\"error-reply\""));
        assert!(outbox.contains("synthetic app-server refusal"));

        let second = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            codex_executable: Some(fake_failing_codex_executable(&root)),
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();
        assert_eq!(second.receipt.status, RuntimeRunOnceStatus::Suppressed);
        assert_eq!(
            second.receipt.terminal_control_source.as_deref(),
            Some("run-once-terminal")
        );
        assert!(second.run.is_none());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn failed_worker_child_with_channel_context_does_not_write_parent_error_outbox() {
        let _guard = env_lock();
        let root = temp_root("failed_worker_child_does_not_write_parent_error_outbox");
        let source = write_pipeline_source(&root);
        let harness_home = root.join(".agent-harness");
        let skills = build_source_skill_index(&source).unwrap();
        let receive = receive_channel_message(ChannelReceiveOptions {
            source,
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "telegram".to_string(),
            account_id: None,
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "delegate a bounded child task".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: None,
            inbound_event_id: None,
            skill_limit: 3,
            now_ms: 1234,
        })
        .unwrap();
        let queue_id = receive.queue_id.unwrap();
        rewrite_pending_queue_identity(
            &harness_home,
            &queue_id,
            "worker",
            "subagent-ledger",
            "xiaoxiaoli",
            "telegram:dm-42:user-7:xiaoxiaoli:child-1",
        );
        let fake_codex = fake_failing_codex_executable(&root);
        let codex_home = root.join("codex-home");
        fs::create_dir_all(&codex_home).unwrap();
        fs::write(codex_home.join("auth.json"), "{}").unwrap();
        let _env = EnvGuard::set("CODEX_HOME", codex_home.into_os_string());

        let report = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: Some(queue_id.clone()),
            codex_executable: Some(fake_codex),
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();

        assert_eq!(report.receipt.status, RuntimeRunOnceStatus::FailedTerminal);
        assert!(report.outbound_message.is_none());
        assert!(report.outbox_file.is_none());
        assert!(
            report.warnings.iter().any(|warning| {
                warning.contains("runtime receipt remains authoritative evidence")
            })
        );
        let receipts = fs::read_to_string(&report.receipts_file).unwrap();
        assert!(receipts.contains(&queue_id));
        assert!(receipts.contains(r#""status":"failed-terminal""#));
        assert!(
            !harness_home
                .join("state")
                .join("channels")
                .join("outbox.jsonl")
                .exists()
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn failed_external_agent_origin_with_channel_context_does_not_write_error_outbox() {
        let _guard = env_lock();
        let root = temp_root("failed_external_agent_does_not_write_error_outbox");
        let source = write_pipeline_source(&root);
        let harness_home = root.join(".agent-harness");
        let skills = build_source_skill_index(&source).unwrap();
        let receive = receive_channel_message(ChannelReceiveOptions {
            source,
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "telegram".to_string(),
            account_id: None,
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "ask an external agent for evidence".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: None,
            inbound_event_id: None,
            skill_limit: 3,
            now_ms: 1234,
        })
        .unwrap();
        let queue_id = receive.queue_id.unwrap();
        rewrite_pending_queue_identity(
            &harness_home,
            &queue_id,
            "interactive",
            "external-agent",
            "main",
            "telegram:dm-42:user-7:main",
        );
        let fake_codex = fake_failing_codex_executable(&root);
        let codex_home = root.join("codex-home");
        fs::create_dir_all(&codex_home).unwrap();
        fs::write(codex_home.join("auth.json"), "{}").unwrap();
        let _env = EnvGuard::set("CODEX_HOME", codex_home.into_os_string());

        let report = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: Some(queue_id.clone()),
            codex_executable: Some(fake_codex),
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();

        assert_eq!(report.receipt.status, RuntimeRunOnceStatus::FailedTerminal);
        assert!(report.outbound_message.is_none());
        assert!(report.outbox_file.is_none());
        assert!(
            report.warnings.iter().any(|warning| {
                warning.contains("runtime receipt remains authoritative evidence")
            })
        );
        let receipts = fs::read_to_string(&report.receipts_file).unwrap();
        assert!(receipts.contains(&queue_id));
        assert!(receipts.contains(r#""status":"failed-terminal""#));
        assert!(
            !harness_home
                .join("state")
                .join("channels")
                .join("outbox.jsonl")
                .exists()
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn external_agent_with_spoofed_parent_identity_emits_no_deliverable_progress() {
        let _guard = env_lock();
        let root = temp_root("external_agent_with_spoofed_parent_identity_emits_no_progress");
        let source = write_pipeline_source(&root);
        let harness_home = root.join(".agent-harness");
        let skills = build_source_skill_index(&source).unwrap();
        let receive = receive_channel_message(ChannelReceiveOptions {
            source,
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "telegram".to_string(),
            account_id: None,
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "collect bounded external evidence".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: None,
            inbound_event_id: None,
            skill_limit: 3,
            now_ms: 1234,
        })
        .unwrap();
        let queue_id = receive.queue_id.unwrap();
        // Channel ingress intentionally emits one immediate, user-facing
        // "queued" event before this test rewrites the synthetic fixture to
        // an external origin. That ingress event belongs to the parent turn;
        // only progress appended by the subsequently executed external child
        // would be a delivery-boundary violation.
        let progress_file = harness_home
            .join("state")
            .join("runtime-queue")
            .join("progress-events.jsonl");
        let progress_lines_before_external_run = fs::read_to_string(&progress_file)
            .unwrap_or_default()
            .lines()
            .count();
        rewrite_pending_queue_identity(
            &harness_home,
            &queue_id,
            "interactive",
            "external-agent",
            "main",
            "telegram:dm-42:user-7:main",
        );
        let codex_home = root.join("codex-home");
        fs::create_dir_all(&codex_home).unwrap();
        fs::write(codex_home.join("auth.json"), "{}").unwrap();
        let _env = EnvGuard::set("CODEX_HOME", codex_home.into_os_string());

        let report = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: Some(queue_id.clone()),
            codex_executable: Some(fake_codex_executable(&root)),
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();

        assert_eq!(report.receipt.status, RuntimeRunOnceStatus::Completed);
        assert!(report.outbound_message.is_none());
        assert!(report.outbox_file.is_none());
        let progress = fs::read_to_string(&progress_file).unwrap_or_default();
        assert!(
            progress
                .lines()
                .skip(progress_lines_before_external_run)
                .all(|line| !line.contains(&queue_id)),
            "external child runtime progress must remain internal even when agentId and sessionKey spoof the parent lane: {progress}"
        );
        let delivery = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 6_000,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert!(
            delivery
                .pending
                .iter()
                .filter(|pending| pending.queue_id == queue_id)
                .all(|pending| pending.event_line <= progress_lines_before_external_run),
            "external child runtime progress must never become user-deliverable: {:#?}",
            delivery.pending
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn failed_stale_same_agent_channel_run_remains_suppressed() {
        let _guard = env_lock();
        let root = temp_root("failed_stale_same_agent_channel_run_remains_suppressed");
        let source = write_pipeline_source(&root);
        let harness_home = root.join(".agent-harness");
        let skills = build_source_skill_index(&source).unwrap();
        let receive = receive_channel_message(ChannelReceiveOptions {
            source,
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "telegram".to_string(),
            account_id: None,
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "fail an obsolete same-agent turn".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: None,
            inbound_event_id: None,
            skill_limit: 3,
            now_ms: 1234,
        })
        .unwrap();
        let queue_id = receive.queue_id.unwrap();
        rewrite_pending_queue_identity(
            &harness_home,
            &queue_id,
            "interactive",
            "channel",
            "main",
            "telegram:dm-42:user-7:main:stale-child",
        );
        let state_file =
            crate::channel_session_state_file(&harness_home, "telegram", "dm-42", "user-7");
        fs::create_dir_all(state_file.parent().unwrap()).unwrap();
        write_json_atomic(
            &state_file,
            &crate::ChannelSessionState {
                schema: "agent-harness.channel-session-state.v1".to_string(),
                platform: "telegram".to_string(),
                account_id: None,
                channel_id: "dm-42".to_string(),
                user_id: "user-7".to_string(),
                active_session_key: "telegram:dm-42:user-7:main:live-parent".to_string(),
                agent_id: Some("main".to_string()),
                config_revision: None,
                provider: None,
                model: None,
                session_topic: None,
                model_override: None,
                model_override_provider: None,
                model_override_model: None,
                thinking_enabled: false,
                thinking_level: None,
                thinking_instruction: None,
                reasoning_preference: None,
                backend_reasoning_policy: None,
                fast_mode: None,
                stop_requested: false,
                stop_reason: None,
                steering_notes: Vec::new(),
                btw_notes: Vec::new(),
                last_command: None,
                updated_at_ms: 1234,
            },
        )
        .unwrap();
        let fake_codex = fake_failing_codex_executable(&root);
        let codex_home = root.join("codex-home");
        fs::create_dir_all(&codex_home).unwrap();
        fs::write(codex_home.join("auth.json"), "{}").unwrap();
        let _env = EnvGuard::set("CODEX_HOME", codex_home.into_os_string());

        let report = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: Some(queue_id),
            codex_executable: Some(fake_codex),
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();

        assert_eq!(report.receipt.status, RuntimeRunOnceStatus::FailedTerminal);
        assert!(report.outbound_message.is_none());
        assert!(report.outbox_file.is_none());
        assert!(report.warnings.iter().any(|warning| {
            warning.contains("stale session")
                && warning.contains("stale-child")
                && warning.contains("suppressed")
        }));
        assert!(
            !harness_home
                .join("state")
                .join("channels")
                .join("outbox.jsonl")
                .exists()
        );

        let _ = fs::remove_dir_all(root);
    }

    #[cfg(windows)]
    #[test]
    fn run_runtime_queue_once_newer_steer_suppresses_stale_outbox_and_keeps_evidence() {
        let _guard = env_lock();
        let root =
            temp_root("run_runtime_queue_once_interrupted_command_uses_structured_reason_outbox");
        let source = write_pipeline_source(&root);
        let harness_home = root.join(".agent-harness");
        let skills = build_source_skill_index(&source).unwrap();
        let receive = receive_channel_message(ChannelReceiveOptions {
            source: source.clone(),
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "telegram".to_string(),
            account_id: None,
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "run a long verification command".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: None,
            inbound_event_id: None,
            skill_limit: 3,
            now_ms: 1234,
        })
        .unwrap();
        assert_eq!(receive.status, ChannelReceiveStatus::AgentTurnQueued);

        // Cancel requests are keyed by the exact persisted lane. Ingress
        // adds the deterministic account suffix even when the caller used
        // the legacy account-less session hint.
        let session_key = receive.session_key;
        let cancel_file = harness_home
            .join("state")
            .join("runtime-queue")
            .join("cancel-requests")
            .join(format!("{}.json", test_normalize_key_part(&session_key)));
        let fake_codex =
            fake_interrupted_command_codex_executable(&root, &cancel_file, &source.workspace);
        let codex_home = root.join("codex-home");
        fs::create_dir_all(&codex_home).unwrap();
        fs::write(codex_home.join("auth.json"), "{}").unwrap();
        let _env = EnvGuard::set("CODEX_HOME", codex_home.into_os_string());

        let report = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            codex_executable: Some(fake_codex),
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();

        assert_eq!(report.receipt.status, RuntimeRunOnceStatus::Canceled);
        let run = report.run.as_ref().unwrap();
        assert_eq!(run.receipt.status, CodexRuntimeRunStatus::Canceled);
        assert_eq!(
            run.receipt.interruption_reason.as_deref(),
            Some("interrupted_by_new_turn")
        );
        assert_eq!(run.receipt.interrupted_tool_uses.len(), 1);
        assert!(report.outbound_message.is_none());
        assert!(report.outbox_file.is_none());
        let transitions = fs::read_to_string(
            crate::goal_transition::goal_transition_receipts_file(&harness_home),
        )
        .unwrap();
        let transition: GoalTransitionReceiptV1 = serde_json::from_str(
            transitions
                .lines()
                .filter(|line| !line.trim().is_empty())
                .last()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(transition.event, GoalTransitionEventKind::NewerSteer);
        assert_eq!(transition.surface, GoalTransitionSurface::SuppressStale);
        assert!(report.warnings.iter().any(|warning| {
            warning.contains("failure outbox suppressed by unified goal transition")
        }));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn timeout_policy_retries_then_dead_letters() {
        assert_eq!(
            final_run_once_status(CodexRuntimeRunStatus::Timeout, 1, "idle timeout", 3),
            RuntimeRunOnceStatus::RetryPending
        );
        assert_eq!(
            final_run_once_status(CodexRuntimeRunStatus::Timeout, 2, "idle timeout", 3),
            RuntimeRunOnceStatus::RetryPending
        );
        assert_eq!(
            final_run_once_status(CodexRuntimeRunStatus::Timeout, 3, "idle timeout", 3),
            RuntimeRunOnceStatus::DeadLetter
        );
        assert!(!should_write_failure_outbox(
            RuntimeRunOnceStatus::RetryPending
        ));
        assert!(should_write_failure_outbox(
            RuntimeRunOnceStatus::DeadLetter
        ));

        let root = temp_root("timeout_policy_retries_then_dead_letters");
        let harness_home = root.join(".agent-harness");
        append_runtime_dead_letter_receipt(
            &harness_home,
            &RuntimeDeadLetterReceipt {
                schema: RUNTIME_DEAD_LETTER_SCHEMA,
                queue_id: Some("queue-timeout".to_string()),
                status: RuntimeRunOnceStatus::DeadLetter,
                execution_dir: None,
                transcript_file: None,
                outbox_file: None,
                reason: final_run_once_reason(
                    RuntimeRunOnceStatus::DeadLetter,
                    CodexRuntimeRunStatus::Timeout,
                    3,
                    3,
                    "idle timeout",
                ),
            },
        )
        .unwrap();
        let receipt_text = fs::read_to_string(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("dead-letter-receipts.jsonl"),
        )
        .unwrap();
        assert!(receipt_text.contains("\"status\":\"dead-letter\""));
        assert!(receipt_text.contains("queue-timeout"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn external_review_evidence_protocol_error_stays_retryable_until_budget_exhausted() {
        let reason = "external review evidence without parent workflow completion";

        assert!(is_external_review_evidence_protocol_error(reason));
        assert_eq!(
            final_run_once_status(CodexRuntimeRunStatus::ProtocolError, 1, reason, 3),
            RuntimeRunOnceStatus::RetryPending
        );
        assert_eq!(
            final_run_once_status(CodexRuntimeRunStatus::ProtocolError, 2, reason, 3),
            RuntimeRunOnceStatus::RetryPending
        );
        assert_eq!(
            final_run_once_status(CodexRuntimeRunStatus::ProtocolError, 3, reason, 3),
            RuntimeRunOnceStatus::DeadLetter
        );
        assert!(!should_write_failure_outbox(
            RuntimeRunOnceStatus::RetryPending
        ));
    }

    #[test]
    fn final_outbox_decision_classifies_user_final_evidence_progress_and_error() {
        let agent_reply = final_outbox_decision(FinalOutboxInputKind::AgentReply);
        assert_eq!(
            agent_reply.disposition,
            FinalOutboxDisposition::UserFacingFinal
        );
        assert_eq!(
            agent_reply.outbound_kind,
            Some(ChannelOutboundMessageKind::AgentReply)
        );
        assert!(agent_reply.attach_plain_final_presentation);
        assert!(agent_reply.may_write_final_outbox());

        for evidence_kind in [
            FinalOutboxInputKind::ReviewEvidence,
            FinalOutboxInputKind::InternalEvidence,
        ] {
            let decision = final_outbox_decision(evidence_kind);
            assert_eq!(
                decision.disposition,
                FinalOutboxDisposition::InternalEvidenceOnly
            );
            assert_eq!(decision.outbound_kind, None);
            assert!(!decision.attach_plain_final_presentation);
            assert!(!decision.may_write_final_outbox());
        }

        let progress = final_outbox_decision(FinalOutboxInputKind::ProgressStatus);
        assert_eq!(progress.disposition, FinalOutboxDisposition::ProgressOnly);
        assert_eq!(progress.outbound_kind, None);
        assert!(!progress.attach_plain_final_presentation);
        assert!(!progress.may_write_final_outbox());

        let terminal_error = final_outbox_decision(FinalOutboxInputKind::TerminalError);
        assert_eq!(
            terminal_error.disposition,
            FinalOutboxDisposition::FailureNotice
        );
        assert_eq!(
            terminal_error.outbound_kind,
            Some(ChannelOutboundMessageKind::ErrorReply)
        );
        assert!(!terminal_error.attach_plain_final_presentation);
        assert!(terminal_error.may_write_final_outbox());
    }

    #[test]
    fn final_outbox_input_kind_suppresses_read_only_review_only_for_workflow_requests() {
        let review_text = "Read-only inspection only. No files changed, no tests run.\n\n- Final/outbox authority seam.";

        assert_eq!(
            final_outbox_input_kind_for_completed_response(
                Some("interactive"),
                Some("channel"),
                Some("main"),
                Some("telegram:dm-42:user-7:main"),
                "telegram:dm-42:user-7:main",
                Some("開 goal 把所有 package 都完成，落實下來，準備進入實作"),
                review_text,
            ),
            FinalOutboxInputKind::ReviewEvidence
        );
        assert_eq!(
            final_outbox_input_kind_for_completed_response(
                Some("interactive"),
                Some("channel"),
                Some("main"),
                Some("telegram:dm-42:user-7:main"),
                "telegram:dm-42:user-7:main",
                Some("開 goal 把所有 package 都完成，落實下來，準備進入實作"),
                "Audit complete; sent root the detailed findings.",
            ),
            FinalOutboxInputKind::ReviewEvidence
        );
        assert_eq!(
            final_outbox_input_kind_for_completed_response(
                Some("interactive"),
                Some("channel"),
                Some("main"),
                Some("telegram:dm-42:user-7:main"),
                "telegram:dm-42:user-7:main",
                Some("please review this implementation plan"),
                review_text,
            ),
            FinalOutboxInputKind::AgentReply
        );
        assert_eq!(
            final_outbox_input_kind_for_completed_response(
                Some("interactive"),
                Some("coordinator-resume"),
                Some("main"),
                Some("telegram:dm-42:user-7:main"),
                "telegram:dm-42:user-7:main",
                Some("complete the package"),
                "Done.",
            ),
            FinalOutboxInputKind::AgentReply
        );
        assert_eq!(
            final_outbox_input_kind_for_completed_response(
                Some("worker"),
                Some("subagent-ledger"),
                Some("child"),
                Some("subagent:child"),
                "telegram:dm-42:user-7:main",
                Some("child result"),
                "child result",
            ),
            FinalOutboxInputKind::InternalEvidence
        );
        assert_eq!(
            final_outbox_input_kind_for_completed_response(
                Some("worker"),
                Some("channel"),
                Some("main"),
                Some("telegram:dm-42:user-7:main"),
                "telegram:dm-42:user-7:main",
                Some("complete the package"),
                "Done.",
            ),
            FinalOutboxInputKind::InternalEvidence
        );
        assert_eq!(
            final_outbox_input_kind_for_completed_response(
                Some("interactive"),
                Some("channel"),
                Some("xiaoxiaoli"),
                Some("telegram:dm-42:user-7:xiaoxiaoli"),
                "telegram:dm-42:user-7:xiaoxiaoli",
                Some("complete the package"),
                "Done."
            ),
            FinalOutboxInputKind::AgentReply
        );
        assert_eq!(
            final_outbox_input_kind_for_completed_response(
                Some("interactive"),
                Some("channel"),
                Some("main"),
                Some("telegram:dm-42:user-7:main:old"),
                "telegram:dm-42:user-7:main:new",
                Some("complete the package"),
                "Done."
            ),
            FinalOutboxInputKind::InternalEvidence
        );
    }

    #[test]
    fn final_outbox_owner_accepts_lane_agent_and_rejects_owner_mismatch() {
        assert!(final_outbox_owner_is_channel_parent(
            Some("main"),
            Some("telegram:dm-42:user-7:main"),
            "telegram:dm-42:user-7:main"
        ));
        assert!(final_outbox_owner_is_channel_parent(
            Some("xiaoxiaoli"),
            Some("telegram:group-alpha:user-limited:xiaoxiaoli"),
            "telegram:group-alpha:user-limited:xiaoxiaoli"
        ));
        assert!(!final_outbox_owner_is_channel_parent(
            Some("xiaoxiaoli"),
            Some("telegram:dm-42:user-7:main"),
            "telegram:dm-42:user-7:main"
        ));
        assert!(!final_outbox_owner_is_channel_parent(
            Some("main"),
            Some("telegram:group-alpha:user-limited:xiaoxiaoli"),
            "telegram:group-alpha:user-limited:xiaoxiaoli"
        ));
        assert!(!final_outbox_owner_is_channel_parent(
            Some("xiaoxiaoli"),
            Some("telegram:group-alpha:user-limited:xiaoxiaoli:old"),
            "telegram:group-alpha:user-limited:xiaoxiaoli:new"
        ));
        assert!(final_outbox_owner_is_channel_parent(
            Some("xiaoxiaoli"),
            Some("telegram:group-alpha:user-limited:xiaoxiaoli:cont-1"),
            "telegram:group-alpha:user-limited:xiaoxiaoli:cont-1"
        ));
        assert!(final_outbox_owner_is_channel_parent(
            Some("xiaoxiaoli"),
            Some("telegram:group-alpha:user-limited:xiaoxiaoli:session-123"),
            "telegram:group-alpha:user-limited:xiaoxiaoli:session-123"
        ));
        assert!(final_outbox_owner_is_channel_parent(
            None,
            Some("telegram:group-alpha:user-limited:xiaoxiaoli"),
            "telegram:group-alpha:user-limited:xiaoxiaoli"
        ));
        assert!(final_outbox_owner_is_channel_parent(
            Some("main"),
            Some("legacy-session-key"),
            "legacy-session-key"
        ));
    }

    #[test]
    fn retryable_protocol_error_policy_retries_then_dead_letters() {
        let reason = "Reconnecting... 2/5; stream disconnected before completion: websocket closed by server before response.completed";

        assert!(is_retryable_codex_protocol_error(reason));
        assert_eq!(
            final_run_once_status(CodexRuntimeRunStatus::ProtocolError, 1, reason, 3),
            RuntimeRunOnceStatus::RetryPending
        );
        assert_eq!(
            final_run_once_status(CodexRuntimeRunStatus::ProtocolError, 2, reason, 3),
            RuntimeRunOnceStatus::RetryPending
        );
        assert_eq!(
            final_run_once_status(CodexRuntimeRunStatus::ProtocolError, 3, reason, 3),
            RuntimeRunOnceStatus::DeadLetter
        );
        assert_eq!(
            final_run_once_status(
                CodexRuntimeRunStatus::ProtocolError,
                1,
                "synthetic app-server refusal",
                3
            ),
            RuntimeRunOnceStatus::FailedTerminal
        );
        assert_eq!(
            runtime_progress_preview(RuntimeRunOnceStatus::RetryPending, reason),
            "transient runtime failure; preserving session for retry"
        );
        assert_eq!(
            map_run_once_status(CodexRuntimeRunStatus::ContextExhausted),
            RuntimeRunOnceStatus::ContextExhausted
        );
        assert_eq!(
            final_run_once_status(
                CodexRuntimeRunStatus::ContextExhausted,
                1,
                "ContextWindowExceeded",
                3
            ),
            RuntimeRunOnceStatus::ContextExhausted
        );
        let reply = runtime_failure_reply_text(
            RuntimeRunOnceStatus::ContextExhausted,
            "ContextWindowExceeded",
            Some("queue-context"),
        );
        assert!(reply.contains("Codex context limit"));
        assert!(reply.contains("queue-context"));
    }

    #[test]
    fn server_overloaded_retry_policy_is_mutation_aware() {
        let failure = crate::CodexProtocolFailureV1 {
            class: crate::CodexProtocolFailureClass::ServerOverloaded,
            codex_error_info: Some("serverOverloaded".to_string()),
            message: "Synthetic selected model capacity failure".to_string(),
            additional_details: None,
            will_retry: Some(false),
            source_method: Some("error".to_string()),
        };

        for evidence in [
            crate::RuntimeMutationEvidenceClass::NoObservableMutation,
            crate::RuntimeMutationEvidenceClass::MutationObserved,
        ] {
            assert_eq!(
                final_run_once_status_with_protocol(
                    CodexRuntimeRunStatus::ProtocolError,
                    1,
                    &failure.message,
                    3,
                    Some(&failure),
                    Some(evidence),
                ),
                RuntimeRunOnceStatus::RetryPending
            );
            assert_eq!(
                final_run_once_status_with_protocol(
                    CodexRuntimeRunStatus::ProtocolError,
                    3,
                    &failure.message,
                    3,
                    Some(&failure),
                    Some(evidence),
                ),
                RuntimeRunOnceStatus::DeadLetter
            );
        }
        assert_eq!(
            final_run_once_status_with_protocol(
                CodexRuntimeRunStatus::ProtocolError,
                1,
                &failure.message,
                3,
                Some(&failure),
                Some(crate::RuntimeMutationEvidenceClass::Unknown),
            ),
            RuntimeRunOnceStatus::FailedTerminal
        );
    }

    #[test]
    fn server_overloaded_no_mutation_schedules_retry_instead_of_attempt_one_terminal() {
        let failure = crate::CodexProtocolFailureV1 {
            class: crate::CodexProtocolFailureClass::ServerOverloaded,
            codex_error_info: Some("serverOverloaded".to_string()),
            message: "synthetic overload".to_string(),
            additional_details: None,
            will_retry: Some(false),
            source_method: Some("error".to_string()),
        };
        let status = final_run_once_status_with_protocol(
            CodexRuntimeRunStatus::ProtocolError,
            1,
            &failure.message,
            3,
            Some(&failure),
            Some(crate::RuntimeMutationEvidenceClass::NoObservableMutation),
        );
        let schedule = runtime_retry_schedule_for(
            status,
            Some("queue-overloaded"),
            1,
            3,
            1_000,
            10_000,
            Some(&failure),
            Some(crate::RuntimeMutationEvidenceClass::NoObservableMutation),
        )
        .unwrap();

        assert_eq!(status, RuntimeRunOnceStatus::RetryPending);
        assert_eq!(schedule.next_eligible_at_ms, 11_000);
        assert_eq!(
            schedule.replay_mode,
            crate::RuntimeRetryReplayModeV1::SameRequestNoObservableMutation
        );
        assert_eq!(schedule.lineage_id, "runtime-retry:queue-overloaded");
    }

    #[test]
    fn structured_protocol_negative_classification_matrix_fails_closed() {
        for class in [
            crate::CodexProtocolFailureClass::Authentication,
            crate::CodexProtocolFailureClass::Configuration,
            crate::CodexProtocolFailureClass::InvalidRequest,
            crate::CodexProtocolFailureClass::Unknown,
        ] {
            let failure = crate::CodexProtocolFailureV1 {
                class,
                codex_error_info: None,
                message: "reconnecting... synthetic text must not override structured class"
                    .to_string(),
                additional_details: None,
                will_retry: Some(true),
                source_method: Some("error".to_string()),
            };
            assert_eq!(
                final_run_once_status_with_protocol(
                    CodexRuntimeRunStatus::ProtocolError,
                    1,
                    &failure.message,
                    3,
                    Some(&failure),
                    Some(crate::RuntimeMutationEvidenceClass::NoObservableMutation),
                ),
                RuntimeRunOnceStatus::FailedTerminal,
                "structured class {class:?} must override retry-like legacy message text"
            );
        }
        let context_failure = crate::CodexProtocolFailureV1 {
            class: crate::CodexProtocolFailureClass::ContextExhausted,
            codex_error_info: Some("contextWindowExceeded".to_string()),
            message: "synthetic context exhaustion".to_string(),
            additional_details: None,
            will_retry: None,
            source_method: Some("error".to_string()),
        };
        assert_eq!(
            final_run_once_status_with_protocol(
                CodexRuntimeRunStatus::ProtocolError,
                1,
                &context_failure.message,
                3,
                Some(&context_failure),
                Some(crate::RuntimeMutationEvidenceClass::NoObservableMutation),
            ),
            RuntimeRunOnceStatus::ContextExhausted
        );
    }

    #[test]
    fn interrupted_verification_not_counted_as_test_failure() {
        let receipt = crate::CodexRuntimeRunReceipt {
            queue_id: Some("queue-interrupted".to_string()),
            status: CodexRuntimeRunStatus::Canceled,
            execution_dir: Some(PathBuf::from("execution")),
            plan_file: Some(PathBuf::from("codex-runtime-plan.json")),
            run_file: Some(PathBuf::from("codex-runtime-run.json")),
            completion_file: None,
            transcript_file: None,
            trajectory_file: None,
            codex_binding_file: None,
            reason: "operator requested stop after newer same-lane turn arrived".to_string(),
            elapsed_ms: 12_000,
            event_count: 3,
            usage: None,
            media_plan: Default::default(),
            context_recovery: None,
            tool_use_timeout: None,
            shell_execution_failure: None,
            subagent_lifecycle: None,
            interruption_reason: Some("interrupted_by_new_turn".to_string()),
            interrupted_tool_uses: vec![crate::codex_runtime::CodexInterruptedToolUse {
                method: "item/started".to_string(),
                item_id: Some("cmd-1".to_string()),
                item_type: Some("commandExecution".to_string()),
                preview: Some("cargo test -p agent-harness-core".to_string()),
                cwd: Some(PathBuf::from("D:/Warehouse/Rust-OpenClaw-Core")),
                started_at_ms: Some(1_000),
                interrupted_at_ms: 2_000,
                stdout_log: Some(PathBuf::from("stdout.log")),
                stderr_log: Some(PathBuf::from("stderr.log")),
                safe_to_rerun: true,
                reason: "interrupted before exit code".to_string(),
            }],
            backend_reasoning_execution: None,
            protocol_failure: None,
            mutation_evidence: None,
            external_effect: None,
            drain_checkpoint: None,
            drain_disposition: None,
            drain_disposition_error: None,
            primary_outcome: crate::CodexRuntimePrimaryOutcomeV1::Interrupted,
            secondary_diagnostics: Vec::new(),
            work_authority_class: crate::WorkAuthorityClassV1::Unknown,
        };
        let reason = runtime_failure_reply_reason(
            &receipt,
            "runtime queue item was canceled by operator request; reason: operator requested stop after newer same-lane turn arrived",
        );
        let reply = runtime_failure_reply_text(
            RuntimeRunOnceStatus::Canceled,
            &reason,
            Some("queue-interrupted"),
        );

        assert!(reply.contains("interrupted by a newer turn"));
        assert!(reply.contains("queue-interrupted"));
        assert!(reply.contains("cargo test -p agent-harness-core"));
        assert!(reply.contains("verification-rerun-eligible"));
        assert!(reply.contains("resume"));
        assert!(!reply.to_ascii_lowercase().contains("failed test"));
        assert!(!reply.to_ascii_lowercase().contains("test failed"));
        assert_ne!(reply, "Stopped.");
    }

    #[test]
    fn stream_unstable_retry_continuation_requires_repeated_high_usage_stream_failure() {
        let item = prepared_test_item("queue-stream", "discord:dm-42:user-7:main", None);
        let candidate = RuntimeContinuationCandidate::from_prepared_item(&item);
        let run = stream_unstable_failed_run("queue-stream", Some(90_038), true);
        let high_usage_without_current_media =
            stream_unstable_failed_run("queue-stream", Some(192_644), false);
        let mut reconnecting_only = stream_unstable_failed_run("queue-stream", Some(90_038), true);
        reconnecting_only.receipt.reason = "Reconnecting... 2/5".to_string();
        let config = crate::ContextRolloverConfig::default();

        assert!(should_enqueue_stream_unstable_retry_continuation(
            &candidate,
            &run,
            2,
            config.stream_unstable_continuation_min_attempts,
            config.stream_unstable_continuation_token_limit
        ));
        assert!(!should_enqueue_stream_unstable_retry_continuation(
            &candidate,
            &reconnecting_only,
            2,
            config.stream_unstable_continuation_min_attempts,
            config.stream_unstable_continuation_token_limit
        ));
        assert!(!should_enqueue_stream_unstable_retry_continuation(
            &candidate,
            &run,
            1,
            config.stream_unstable_continuation_min_attempts,
            config.stream_unstable_continuation_token_limit
        ));
        assert!(!should_enqueue_stream_unstable_retry_continuation(
            &candidate,
            &stream_unstable_failed_run("queue-stream", Some(79_999), true),
            2,
            config.stream_unstable_continuation_min_attempts,
            config.stream_unstable_continuation_token_limit
        ));
        assert!(should_enqueue_stream_unstable_retry_continuation(
            &candidate,
            &high_usage_without_current_media,
            2,
            config.stream_unstable_continuation_min_attempts,
            config.stream_unstable_continuation_token_limit
        ));
    }

    #[test]
    fn pending_continuation_candidate_record_preserves_inbound_media_artifacts() {
        let record: PendingContinuationCandidateRecord = serde_json::from_str(
            r#"{
                "queueId": "queue-stream",
                "sessionKey": "discord:dm-42:user-7:main",
                "runtimeClass": "interactive",
                "origin": "channel",
                "inboundMediaArtifacts": [
                    {
                        "schema": "agent-harness.inbound-media-artifact.v1",
                        "platform": "discord",
                        "kind": "attachment-image",
                        "source": "discord-gateway",
                        "artifactUri": "agent-harness://inbound-media/discord/msg-1/0.jpg",
                        "mime": "image/jpeg",
                        "sha256": "abc123"
                    }
                ]
            }"#,
        )
        .unwrap();

        let candidate = RuntimeContinuationCandidate::from(record);

        assert_eq!(candidate.queue_id, "queue-stream");
        assert_eq!(candidate.inbound_media_artifacts.len(), 1);
        assert_eq!(
            candidate.inbound_media_artifacts[0].artifact_uri.as_deref(),
            Some("agent-harness://inbound-media/discord/msg-1/0.jpg")
        );
        assert_eq!(candidate.inbound_media_artifacts[0].platform, "discord");
    }

    #[test]
    fn self_improvement_hook_is_suppressed_for_rollover_continuations() {
        let legacy = RuntimeContinuationMetadata::legacy();
        assert!(should_run_self_improvement_hook(
            RuntimeRunOnceStatus::Completed,
            &legacy,
            None
        ));

        let mut rollover = RuntimeContinuationMetadata::legacy();
        rollover.completion_kind = Some("continuation-rollover".to_string());
        assert!(!should_run_self_improvement_hook(
            RuntimeRunOnceStatus::Completed,
            &rollover,
            None
        ));

        let mut explicit = RuntimeContinuationMetadata::legacy();
        explicit.suppress_self_improvement = true;
        assert!(!should_run_self_improvement_hook(
            RuntimeRunOnceStatus::Completed,
            &explicit,
            None
        ));
        assert!(!should_run_self_improvement_hook(
            RuntimeRunOnceStatus::RetryPending,
            &legacy,
            None
        ));
    }

    #[test]
    fn polluted_thread_continuation_runs_at_terminal_failure_and_respects_depth_limit() {
        let root = temp_root("polluted_thread_continuation_terminal_depth");
        let harness_home = root.join(".agent-harness");
        let retry_item = prepared_test_item("queue-retry", "telegram:dm-42:user-7:main", None);
        let run = polluted_thread_failed_run("queue-retry");
        let mut warnings = Vec::new();

        let retry_result = maybe_enqueue_polluted_thread_continuation(
            &harness_home,
            Some(&retry_item),
            &run,
            RuntimeRunOnceStatus::RetryPending,
            &mut warnings,
        )
        .unwrap();

        assert!(retry_result.is_none());
        assert!(warnings.is_empty());

        let queue_id = "queue-terminal";
        let session_key = "discord:dm-42:user-7:main";
        seed_prepared_pending_for_stream_retry(&harness_home, queue_id, session_key);
        let terminal_item = prepared_test_item(queue_id, session_key, None);
        let terminal_result = maybe_enqueue_polluted_thread_continuation(
            &harness_home,
            Some(&terminal_item),
            &polluted_thread_failed_run(queue_id),
            RuntimeRunOnceStatus::FailedTerminal,
            &mut warnings,
        )
        .unwrap()
        .expect("terminal polluted failure should enqueue child continuation");

        assert_eq!(terminal_result.queue_id, queue_id);
        assert_eq!(
            terminal_result.new_working_session_key,
            canonical_test_continuation(session_key, 1)
        );

        fs::write(
            harness_home.join(crate::HARNESS_CONFIG_FILE_NAME),
            r#"{"codexContext":{"maxSuccessfulCompactsBeforeRollover":9,"maxContinuationDepth":2}}"#,
        )
        .unwrap();
        let max_depth_item =
            prepared_test_item("queue-max", "telegram:dm-42:user-7:main:cont-2", Some(2));
        let max_depth_result = maybe_enqueue_polluted_thread_continuation(
            &harness_home,
            Some(&max_depth_item),
            &polluted_thread_failed_run("queue-max"),
            RuntimeRunOnceStatus::DeadLetter,
            &mut warnings,
        )
        .unwrap();

        assert!(max_depth_result.is_none());
        assert!(
            warnings
                .iter()
                .any(|warning| warning.contains("continuation depth 2"))
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn server_overloaded_mutation_retry_requeues_exact_lane_continuation() {
        let root = temp_root("server_overloaded_mutation_retry_requeues_exact_lane_continuation");
        let harness_home = root.join(".agent-harness");
        let queue_id = "queue-overloaded";
        let canonical = crate::channel_session_key::CanonicalChannelSessionKey::bind_for_lane(
            "discord:dm-42:user-7:main",
            "discord",
            "default",
            "dm-42",
            "user-7",
            "main",
        )
        .unwrap();
        let session_key = canonical.canonical_string();
        let expected_child_session_key = canonical.continuation(1).unwrap().canonical_string();
        seed_prepared_pending_for_stream_retry(&harness_home, queue_id, &session_key);
        let item = prepared_test_item(queue_id, &session_key, None);
        let mut run = stream_unstable_failed_run(queue_id, None, false);
        run.receipt.protocol_failure = Some(crate::CodexProtocolFailureV1 {
            class: crate::CodexProtocolFailureClass::ServerOverloaded,
            codex_error_info: Some("serverOverloaded".to_string()),
            message: "Synthetic selected model capacity failure".to_string(),
            additional_details: None,
            will_retry: Some(false),
            source_method: Some("error".to_string()),
        });
        run.receipt.mutation_evidence = Some(crate::RuntimeMutationEvidenceClass::MutationObserved);

        let mut warnings = Vec::new();
        let rollover = maybe_enqueue_server_overloaded_retry_continuation(
            &harness_home,
            Some(&item),
            &run,
            &mut warnings,
        )
        .unwrap()
        .unwrap_or_else(|| {
            panic!("mutation-observed overload should requeue a continuation: {warnings:?}")
        });
        assert_eq!(rollover.queue_id, queue_id);
        assert_eq!(rollover.new_working_session_key, expected_child_session_key);
        let pending = fs::read_to_string(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("pending.jsonl"),
        )
        .unwrap();
        assert!(pending.contains("\"requeuedFromQueueId\":\"queue-overloaded\""));
        assert!(pending.contains("mutation-aware server-overloaded continuation"));

        let mut no_mutation_run = run.clone();
        no_mutation_run.receipt.mutation_evidence =
            Some(crate::RuntimeMutationEvidenceClass::NoObservableMutation);
        assert!(
            maybe_enqueue_server_overloaded_retry_continuation(
                &harness_home,
                Some(&item),
                &no_mutation_run,
                &mut Vec::new(),
            )
            .unwrap()
            .is_none()
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn server_overloaded_after_mutation_uses_continuation_not_prompt_replay() {
        server_overloaded_mutation_retry_requeues_exact_lane_continuation();
    }

    #[test]
    fn tool_timeout_fallback_failure_retry_requeues_continuation() {
        let root = temp_root("tool_timeout_fallback_failure_retry_requeues_continuation");
        let harness_home = root.join(".agent-harness");
        let queue_id = "queue-tool-timeout";
        let session_key = "discord:dm-42:user-7:main";
        seed_prepared_pending_for_stream_retry(&harness_home, queue_id, session_key);
        let item = prepared_test_item(queue_id, session_key, None);
        let run = interrupted_long_task_run(
            queue_id,
            CodexRuntimeRunStatus::Timeout,
            Some("tool-timeout-fallback-failed"),
            true,
        );
        let mut warnings = Vec::new();

        let rollover = maybe_enqueue_tool_timeout_retry_continuation(
            &harness_home,
            Some(&item),
            &run,
            &mut warnings,
        )
        .unwrap()
        .unwrap_or_else(|| {
            panic!("tool-timeout fallback failure should requeue continuation: {warnings:?}")
        });

        assert_eq!(rollover.queue_id, queue_id);
        assert_eq!(
            rollover.new_working_session_key,
            canonical_test_continuation(session_key, 1)
        );
        let pending = fs::read_to_string(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("pending.jsonl"),
        )
        .unwrap();
        assert!(pending.contains("\"requeuedFromQueueId\":\"queue-tool-timeout\""));
        assert!(pending.contains("\"completionKind\":\"continuation-rollover\""));
        assert!(pending.contains("interrupted long-task"));
        assert!(pending.contains("method=pwsh"));
        assert!(pending.contains("preview=cargo clippy"));
        assert!(
            !harness_home
                .join("state")
                .join("channels")
                .join("outbox.jsonl")
                .exists()
        );
        assert!(
            warnings
                .iter()
                .any(|warning| warning.contains("normalized legacy queue session"))
        );
        assert!(!warnings.iter().any(|warning| warning.contains("skipped")));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn no_final_answer_terminal_interruption_requeues_continuation() {
        let root = temp_root("no_final_answer_terminal_interruption_requeues_continuation");
        let harness_home = root.join(".agent-harness");
        let queue_id = "queue-no-final";
        let session_key = "discord:dm-42:user-7:main";
        seed_prepared_pending_for_stream_retry(&harness_home, queue_id, session_key);
        let item = prepared_test_item(queue_id, session_key, None);
        let run =
            interrupted_long_task_run(queue_id, CodexRuntimeRunStatus::ProtocolError, None, false);
        let mut warnings = Vec::new();

        let rollover = maybe_enqueue_polluted_thread_continuation(
            &harness_home,
            Some(&item),
            &run,
            RuntimeRunOnceStatus::FailedTerminal,
            &mut warnings,
        )
        .unwrap()
        .expect("no-final-answer interruption should requeue continuation");

        assert_eq!(rollover.queue_id, queue_id);
        assert_eq!(
            rollover.new_working_session_key,
            canonical_test_continuation(session_key, 1)
        );
        let pending = fs::read_to_string(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("pending.jsonl"),
        )
        .unwrap();
        assert!(pending.contains("interrupted long-task"));
        assert!(!pending.contains("polluted-thread"));
        assert!(
            warnings
                .iter()
                .any(|warning| { warning.contains("normalized legacy queue session") })
        );
        assert!(!warnings.iter().any(|warning| warning.contains("skipped")));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn plain_provider_timeout_does_not_requeue_interrupted_continuation() {
        let root = temp_root("plain_provider_timeout_does_not_requeue_interrupted_continuation");
        let harness_home = root.join(".agent-harness");
        let queue_id = "queue-provider-timeout";
        let session_key = "discord:dm-42:user-7:main";
        seed_prepared_pending_for_stream_retry(&harness_home, queue_id, session_key);
        let item = prepared_test_item(queue_id, session_key, None);
        let mut run =
            interrupted_long_task_run(queue_id, CodexRuntimeRunStatus::Timeout, None, false);
        run.receipt.reason = "provider request timed out before any tool execution".to_string();
        let mut warnings = Vec::new();

        let rollover = maybe_enqueue_tool_timeout_retry_continuation(
            &harness_home,
            Some(&item),
            &run,
            &mut warnings,
        )
        .unwrap();

        assert!(rollover.is_none());
        let pending = fs::read_to_string(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("pending.jsonl"),
        )
        .unwrap();
        assert!(!pending.contains("\"requeuedFromQueueId\":\"queue-provider-timeout\""));
        assert!(
            warnings
                .iter()
                .all(|warning| warning.contains("normalized legacy queue session")),
            "{warnings:#?}"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn productive_absolute_timeout_retry_requeues_continuation_instead_of_replaying_parent() {
        let root = temp_root(
            "productive_absolute_timeout_retry_requeues_continuation_instead_of_replaying_parent",
        );
        let harness_home = root.join(".agent-harness");
        let queue_id = "queue-productive-absolute-timeout";
        let session_key = "discord:dm-42:user-7:main";
        seed_prepared_pending_for_stream_retry(&harness_home, queue_id, session_key);
        let item = prepared_test_item(queue_id, session_key, None);
        let mut run =
            interrupted_long_task_run(queue_id, CodexRuntimeRunStatus::Timeout, None, false);
        run.receipt.reason =
            "timed out waiting for Codex app-server completion after 1800000ms".to_string();
        run.receipt.event_count = 64;
        run.receipt.elapsed_ms = 1_800_010;
        let stdout_log = root.join("productive-timeout.stdout.jsonl");
        fs::write(
            &stdout_log,
            r#"{"method":"turn/started","params":{"threadId":"thread-1","turn":{"id":"turn-1","kind":"regular"}}}
{"method":"item/completed","params":{"threadId":"thread-1","turnId":"turn-1","item":{"type":"commandExecution","id":"cmd-1","status":"completed","exitCode":0,"aggregatedOutput":"sanitized verification completed"}}}
"#,
        )
        .unwrap();
        run.stdout_log = Some(stdout_log);
        let mut warnings = Vec::new();

        let rollover = maybe_enqueue_tool_timeout_retry_continuation(
            &harness_home,
            Some(&item),
            &run,
            &mut warnings,
        )
        .unwrap()
        .expect(
            "I3/I10/I13 T3 replay: a productive local absolute timeout must checkpoint into one continuation instead of replaying the prepared parent",
        );

        assert_eq!(rollover.queue_id, queue_id);
        assert_eq!(
            rollover.new_working_session_key,
            canonical_test_continuation(session_key, 1)
        );
        let pending = fs::read_to_string(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("pending.jsonl"),
        )
        .unwrap();
        assert_eq!(pending.matches("rollover-requeue").count(), 1);
        assert!(pending.contains("interrupted long-task"));
        let parent_receipts = fs::read_to_string(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("run-once-receipts.jsonl"),
        )
        .unwrap();
        let handoff = parent_receipts
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .find(|value| {
                value.get("terminalDisposition").and_then(Value::as_str)
                    == Some("continuation-handoff")
                    && value
                        .get("childQueueId")
                        .and_then(Value::as_str)
                        .is_some_and(|child| child.contains("rollover-requeue"))
            })
            .expect("typed continuation handoff receipt must name the admitted child");
        let child_queue_id = handoff
            .get("childQueueId")
            .and_then(Value::as_str)
            .expect("handoff childQueueId");
        assert_ne!(child_queue_id, queue_id);
        assert_eq!(
            handoff
                .pointer("/continuationLink/parentQueueId")
                .and_then(Value::as_str),
            Some(queue_id)
        );
        assert_eq!(
            handoff
                .pointer("/continuationLink/childQueueId")
                .and_then(Value::as_str),
            Some(child_queue_id)
        );
        assert!(!crate::agent_progress_events_file(&harness_home).exists());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn productive_timeout_commits_typed_continuation_handoff_before_progress_projection() {
        productive_absolute_timeout_retry_requeues_continuation_instead_of_replaying_parent();
    }

    #[test]
    fn absolute_timeout_without_productive_progress_does_not_requeue_continuation() {
        let root =
            temp_root("absolute_timeout_without_productive_progress_does_not_requeue_continuation");
        let harness_home = root.join(".agent-harness");
        let queue_id = "queue-handshake-only-timeout";
        let session_key = "discord:dm-42:user-7:main";
        seed_prepared_pending_for_stream_retry(&harness_home, queue_id, session_key);
        let item = prepared_test_item(queue_id, session_key, None);
        let mut run =
            interrupted_long_task_run(queue_id, CodexRuntimeRunStatus::Timeout, None, false);
        run.receipt.reason =
            "timed out waiting for Codex app-server completion after 1800000ms".to_string();
        run.receipt.event_count = 64;
        let stdout_log = root.join("handshake-only-timeout.stdout.jsonl");
        fs::write(
            &stdout_log,
            r#"{"method":"item/completed","params":{"item":{"type":"contextCompaction","id":"compact-1"}}}
{"method":"turn/started","params":{"turn":{"id":"turn-1","kind":"regular"}}}
"#,
        )
        .unwrap();
        run.stdout_log = Some(stdout_log);
        let mut warnings = Vec::new();

        let rollover = maybe_enqueue_tool_timeout_retry_continuation(
            &harness_home,
            Some(&item),
            &run,
            &mut warnings,
        )
        .unwrap();

        assert!(rollover.is_none());
        let pending = fs::read_to_string(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("pending.jsonl"),
        )
        .unwrap();
        assert!(!pending.contains("rollover-requeue"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn repeated_stream_disconnect_high_usage_retry_requeues_continuation() {
        let root = temp_root("repeated_stream_disconnect_high_usage_retry_requeues_continuation");
        let harness_home = root.join(".agent-harness");
        let queue_id = "queue-stream";
        let session_key = "discord:dm-42:user-7:main";
        seed_prepared_pending_for_stream_retry(&harness_home, queue_id, session_key);
        let mut item = prepared_test_item(queue_id, session_key, None);
        item.platform = "discord".to_string();
        item.channel_id = "dm-42".to_string();
        item.user_id = "user-7".to_string();
        item.inbound_media_artifacts = vec![InboundMediaArtifact {
            platform: "discord".to_string(),
            kind: "attachment-image".to_string(),
            artifact_uri: Some("agent-harness://inbound-media/discord/msg-1/0.jpg".to_string()),
            mime: Some("image/jpeg".to_string()),
            sha256: Some("abc123".to_string()),
            ..InboundMediaArtifact::default()
        }];
        let run = stream_unstable_failed_run(queue_id, Some(90_038), true);
        let mut warnings = Vec::new();

        let rollover = maybe_enqueue_stream_unstable_retry_continuation(
            &harness_home,
            Some(&item),
            &run,
            2,
            &mut warnings,
        )
        .unwrap()
        .expect("expected stream-unstable retry to requeue a continuation");

        assert_eq!(rollover.queue_id, queue_id);
        assert_eq!(
            rollover.previous_working_session_key,
            Some(canonical_test_session(session_key))
        );
        assert_eq!(
            rollover.new_working_session_key,
            canonical_test_continuation(session_key, 1)
        );
        assert_eq!(rollover.continuation_index, 1);
        assert!(
            rollover
                .requeued_queue_id
                .starts_with("queue-stream:rollover-requeue-")
        );
        let pending = fs::read_to_string(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("pending.jsonl"),
        )
        .unwrap();
        assert!(pending.contains("\"requeuedFromQueueId\":\"queue-stream\""));
        assert!(pending.contains("\"completionKind\":\"continuation-rollover\""));
        assert!(pending.contains("stream-unstable"));
        assert!(
            !harness_home
                .join("state")
                .join("channels")
                .join("outbox.jsonl")
                .exists()
        );
        assert!(
            warnings
                .iter()
                .any(|warning| { warning.contains("normalized legacy queue session") })
        );
        assert!(!warnings.iter().any(|warning| warning.contains("skipped")));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn stream_unstable_retry_continuation_preserves_parent_when_child_enqueue_fails() {
        let root = temp_root("stream_unstable_retry_continuation_child_enqueue_fails");
        let harness_home = root.join(".agent-harness");
        let queue_id = "queue-stream-missing";
        let session_key = "discord:dm-42:user-7:main";
        let item = prepared_test_item(queue_id, session_key, None);
        let run = stream_unstable_failed_run(queue_id, Some(90_038), true);
        let mut warnings = Vec::new();

        let rollover = maybe_enqueue_stream_unstable_retry_continuation(
            &harness_home,
            Some(&item),
            &run,
            2,
            &mut warnings,
        )
        .unwrap();

        assert!(rollover.is_none());
        assert!(warnings.iter().any(|warning| {
            warning.contains("pending queue item queue-stream-missing was not found")
        }));
        assert!(
            !harness_home
                .join("state")
                .join("channels")
                .join("outbox.jsonl")
                .exists()
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn stream_unstable_retry_continuation_skips_when_parent_session_sibling_exists() {
        let root = temp_root("stream_unstable_retry_continuation_parent_sibling");
        let harness_home = root.join(".agent-harness");
        let queue_id = "queue-stream";
        let session_key = "discord:dm-42:user-7:main";
        seed_prepared_pending_for_stream_retry(&harness_home, queue_id, session_key);
        append_json_line(
            &harness_home
                .join("state")
                .join("runtime-queue")
                .join("pending.jsonl"),
            &serde_json::json!({
                "queueId": "queue-sibling",
                "status": "queued",
                "runtimeClass": "interactive",
                "origin": "channel",
                "agentId": "main",
                "platform": "discord",
                "accountId": "default",
                "channelId": "dm-42",
                "userId": "user-7",
                "sessionKey": session_key
            }),
        )
        .unwrap();
        let item = prepared_test_item(queue_id, session_key, None);
        let run = stream_unstable_failed_run(queue_id, Some(90_038), true);
        let mut warnings = Vec::new();

        let rollover = maybe_enqueue_stream_unstable_retry_continuation(
            &harness_home,
            Some(&item),
            &run,
            2,
            &mut warnings,
        )
        .unwrap();

        assert!(rollover.is_none());
        assert!(warnings.iter().any(|warning| {
            warning.contains("another pending item targets the parent working session")
        }));
        let pending = fs::read_to_string(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("pending.jsonl"),
        )
        .unwrap();
        assert!(!pending.contains("\"requeuedFromQueueId\":\"queue-stream\""));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn stream_unstable_retry_continuation_ignores_other_account_parent_session_sibling() {
        let root = temp_root("stream_unstable_retry_continuation_other_account_sibling");
        let harness_home = root.join(".agent-harness");
        let queue_id = "queue-stream";
        let session_key = "discord:dm-42:user-7:main";
        seed_prepared_pending_for_stream_retry(&harness_home, queue_id, session_key);
        append_json_line(
            &harness_home
                .join("state")
                .join("runtime-queue")
                .join("pending.jsonl"),
            &serde_json::json!({
                "queueId": "queue-other-account-sibling",
                "status": "queued",
                "runtimeClass": "interactive",
                "origin": "channel",
                "agentId": "main",
                "platform": "discord",
                "accountId": "other-account",
                "channelId": "dm-42",
                "userId": "user-7",
                "sessionKey": session_key
            }),
        )
        .unwrap();
        let item = prepared_test_item(queue_id, session_key, None);
        let run = stream_unstable_failed_run(queue_id, Some(90_038), true);
        let mut warnings = Vec::new();

        let rollover = maybe_enqueue_stream_unstable_retry_continuation(
            &harness_home,
            Some(&item),
            &run,
            2,
            &mut warnings,
        )
        .unwrap()
        .expect("a sibling in another account must not block exact-lane rollover");

        assert!(
            rollover
                .requeued_queue_id
                .starts_with("queue-stream:rollover-requeue-")
        );
        assert!(
            warnings
                .iter()
                .all(|warning| warning.contains("normalized legacy queue session")),
            "{warnings:#?}"
        );
        let pending = fs::read_to_string(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("pending.jsonl"),
        )
        .unwrap();
        assert!(pending.contains("\"accountId\":\"default\""));
        assert!(pending.contains("\"accountId\":\"other-account\""));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn stream_unstable_retry_continuation_tombstones_parent_queue_item() {
        let _guard = env_lock();
        let root = temp_root("stream_unstable_retry_continuation_tombstones_parent_queue_item");
        let source = write_pipeline_source(&root);
        let harness_home = root.join(".agent-harness");
        let skills = build_source_skill_index(&source).unwrap();
        let receive = receive_channel_message(ChannelReceiveOptions {
            source,
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "discord".to_string(),
            account_id: None,
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "handle these media files without losing the workflow".to_string(),
            inbound_context: None,
            inbound_media_artifacts: vec![InboundMediaArtifact {
                platform: "discord".to_string(),
                kind: "attachment-image".to_string(),
                artifact_uri: Some("agent-harness://inbound-media/discord/msg-1/0.jpg".to_string()),
                mime: Some("image/jpeg".to_string()),
                sha256: Some("abc123".to_string()),
                ..InboundMediaArtifact::default()
            }],
            inbound_event_kind: None,
            inbound_event_id: None,
            skill_limit: 3,
            now_ms: 1234,
        })
        .unwrap();
        let queue_id = receive.queue_id.unwrap();
        write_test_channel_session_state(
            &harness_home,
            "discord",
            "dm-42",
            "user-7",
            &receive.session_key,
            "main",
        );
        let codex_home = root.join("codex-home");
        fs::create_dir_all(&codex_home).unwrap();
        fs::write(codex_home.join("auth.json"), "{}").unwrap();
        let _env = EnvGuard::set("CODEX_HOME", codex_home.into_os_string());

        let first = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: Some(queue_id.clone()),
            codex_executable: Some(fake_high_usage_reconnecting_codex_executable(&root)),
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();
        assert_eq!(first.receipt.status, RuntimeRunOnceStatus::RetryPending);
        assert!(first.outbound_message.is_none());
        force_retry_eligible(&harness_home, &queue_id);

        let second = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: Some(queue_id.clone()),
            codex_executable: Some(fake_high_usage_reconnecting_codex_executable(&root)),
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();

        assert_eq!(second.receipt.status, RuntimeRunOnceStatus::Skipped);
        assert!(second.outbound_message.is_none());
        assert!(second.receipt.reason.contains("childQueueId="));

        let run_once_receipts = fs::read_to_string(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("run-once-receipts.jsonl"),
        )
        .unwrap();
        let latest_parent_status = run_once_receipts
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .filter(|value| string_field(value, &["queueId"]) == Some(queue_id.as_str()))
            .filter_map(|value| string_field(&value, &["status"]).map(str::to_string))
            .last()
            .unwrap();
        assert_eq!(latest_parent_status, "skipped");

        let pending = fs::read_to_string(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("pending.jsonl"),
        )
        .unwrap();
        assert!(pending.contains("\"requeuedFromQueueId\""));
        assert!(pending.contains("\"completionKind\":\"continuation-rollover\""));

        let third = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            codex_executable: Some(fake_high_usage_reconnecting_codex_executable(&root)),
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();
        assert_ne!(third.receipt.queue_id.as_deref(), Some(queue_id.as_str()));
        assert!(
            third
                .receipt
                .queue_id
                .as_deref()
                .is_some_and(|id| id.starts_with("turn:") || id.contains(":rollover-requeue-"))
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn stream_unstable_retry_continuation_child_writes_exactly_one_final_outbox() {
        let _guard = env_lock();
        let root = temp_root("stream_unstable_retry_continuation_child_writes_final_outbox");
        let source = write_pipeline_source(&root);
        let harness_home = root.join(".agent-harness");
        let skills = build_source_skill_index(&source).unwrap();
        let receive = receive_channel_message(ChannelReceiveOptions {
            source,
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "discord".to_string(),
            account_id: None,
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "finish this long media-heavy task".to_string(),
            inbound_context: None,
            inbound_media_artifacts: vec![InboundMediaArtifact {
                platform: "discord".to_string(),
                kind: "attachment-image".to_string(),
                artifact_uri: Some("agent-harness://inbound-media/discord/msg-1/0.jpg".to_string()),
                mime: Some("image/jpeg".to_string()),
                sha256: Some("abc123".to_string()),
                ..InboundMediaArtifact::default()
            }],
            inbound_event_kind: None,
            inbound_event_id: None,
            skill_limit: 3,
            now_ms: 1234,
        })
        .unwrap();
        let parent_queue_id = receive.queue_id.unwrap();
        let expected_child_session_key = format!("{}:cont-1", receive.session_key);
        write_test_channel_session_state(
            &harness_home,
            "discord",
            "dm-42",
            "user-7",
            &receive.session_key,
            "main",
        );
        let codex_home = root.join("codex-home");
        fs::create_dir_all(&codex_home).unwrap();
        fs::write(codex_home.join("auth.json"), "{}").unwrap();
        let _env = EnvGuard::set("CODEX_HOME", codex_home.into_os_string());

        let first = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: Some(parent_queue_id.clone()),
            codex_executable: Some(fake_high_usage_reconnecting_codex_executable(&root)),
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();
        assert_eq!(first.receipt.status, RuntimeRunOnceStatus::RetryPending);
        assert!(first.outbound_message.is_none());
        force_retry_eligible(&harness_home, &parent_queue_id);

        let second = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: Some(parent_queue_id.clone()),
            codex_executable: Some(fake_high_usage_reconnecting_codex_executable(&root)),
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();
        assert_eq!(second.receipt.status, RuntimeRunOnceStatus::Skipped);
        let child_queue_id = second
            .receipt
            .child_queue_id
            .clone()
            .expect("skip receipt should include structured childQueueId");
        assert_ne!(child_queue_id, parent_queue_id);
        assert_eq!(
            second.receipt.child_session_key.as_deref(),
            Some(expected_child_session_key.as_str())
        );

        let child = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: Some(child_queue_id.clone()),
            codex_executable: Some(fake_codex_executable(&root)),
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();

        assert_eq!(child.receipt.status, RuntimeRunOnceStatus::Completed);
        assert_eq!(
            child.receipt.queue_id.as_deref(),
            Some(child_queue_id.as_str())
        );
        assert_eq!(
            child.outbound_message.as_ref().unwrap().text,
            "Pipeline fake reply."
        );

        let outbox = fs::read_to_string(
            harness_home
                .join("state")
                .join("channels")
                .join("outbox.jsonl"),
        )
        .unwrap();
        let outbox_values: Vec<Value> = outbox
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .collect();
        assert_eq!(outbox_values.len(), 1);
        assert_eq!(
            string_field(&outbox_values[0], &["sourceQueueId"]),
            Some(child_queue_id.as_str())
        );
        assert_ne!(
            string_field(&outbox_values[0], &["sourceQueueId"]),
            Some(parent_queue_id.as_str())
        );
        assert_eq!(
            string_field(&outbox_values[0], &["text"]),
            Some("Pipeline fake reply.")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn run_runtime_queue_once_retries_reconnecting_protocol_error_then_dead_letters() {
        let _guard = env_lock();
        let root = temp_root(
            "run_runtime_queue_once_retries_reconnecting_protocol_error_then_dead_letters",
        );
        let source = write_pipeline_source(&root);
        let harness_home = root.join(".agent-harness");
        let skills = build_source_skill_index(&source).unwrap();
        let receive = receive_channel_message(ChannelReceiveOptions {
            source,
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "telegram".to_string(),
            account_id: None,
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "keep my session while reconnecting".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: None,
            inbound_event_id: None,
            skill_limit: 3,
            now_ms: 1234,
        })
        .unwrap();
        assert_eq!(receive.status, ChannelReceiveStatus::AgentTurnQueued);
        let queue_id = receive.queue_id.unwrap();
        let codex_home = root.join("codex-home");
        fs::create_dir_all(&codex_home).unwrap();
        fs::write(codex_home.join("auth.json"), "{}").unwrap();
        let _env = EnvGuard::set("CODEX_HOME", codex_home.into_os_string());

        let first = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: Some(queue_id.clone()),
            codex_executable: Some(fake_reconnecting_codex_executable(&root)),
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();
        assert_eq!(first.receipt.status, RuntimeRunOnceStatus::RetryPending);
        assert_eq!(
            first.run.as_ref().unwrap().receipt.status,
            CodexRuntimeRunStatus::ProtocolError
        );
        assert!(first.outbound_message.is_none());
        assert!(
            first
                .receipt
                .reason
                .contains("stream disconnected before completion")
        );
        force_retry_eligible(&harness_home, &queue_id);

        let second = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            codex_executable: Some(fake_reconnecting_codex_executable(&root)),
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();
        assert_eq!(second.receipt.status, RuntimeRunOnceStatus::RetryPending);
        assert_eq!(second.receipt.queue_id.as_deref(), Some(queue_id.as_str()));
        assert!(second.outbound_message.is_none());
        force_retry_eligible(&harness_home, &queue_id);

        let third = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: Some(queue_id.clone()),
            codex_executable: Some(fake_reconnecting_codex_executable(&root)),
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();
        assert_eq!(third.receipt.status, RuntimeRunOnceStatus::DeadLetter);
        let outbound = third.outbound_message.unwrap();
        assert_eq!(outbound.kind, ChannelOutboundMessageKind::ErrorReply);
        assert!(outbound.text.contains("dead-letter"));
        assert!(outbound.text.contains(&queue_id));
        assert!(outbound.text.contains("Session context is preserved"));
        let dead_letter = fs::read_to_string(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("dead-letter-receipts.jsonl"),
        )
        .unwrap();
        assert!(dead_letter.contains("\"status\":\"dead-letter\""));
        assert!(dead_letter.contains(&queue_id));
        let transitions = fs::read_to_string(
            crate::goal_transition::goal_transition_receipts_file(&harness_home),
        )
        .unwrap();
        let transition_rows = transitions.lines().collect::<Vec<_>>();
        assert_eq!(transition_rows.len(), 2);
        assert!(transition_rows[0].contains("\"runtimeStatus\":\"retry-pending\""));
        assert!(transition_rows[1].contains("\"runtimeStatus\":\"dead-letter\""));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn run_runtime_queue_once_keeps_external_review_evidence_resumable_without_final_outbox() {
        let _guard = env_lock();
        let root = temp_root(
            "run_runtime_queue_once_keeps_external_review_evidence_resumable_without_final_outbox",
        );
        let source = write_pipeline_source(&root);
        let harness_home = root.join(".agent-harness");
        let skills = build_source_skill_index(&source).unwrap();
        let receive = receive_channel_message(ChannelReceiveOptions {
            source,
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "discord".to_string(),
            account_id: None,
            channel_id: "discord-dm-42".to_string(),
            user_id: "discord-user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "run claude second brain review then continue implementation".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: None,
            inbound_event_id: None,
            skill_limit: 3,
            now_ms: 1234,
        })
        .unwrap();
        assert_eq!(receive.status, ChannelReceiveStatus::AgentTurnQueued);
        let queue_id = receive.queue_id.unwrap();
        let codex_home = root.join("codex-home");
        fs::create_dir_all(&codex_home).unwrap();
        fs::write(codex_home.join("auth.json"), "{}").unwrap();
        let _env = EnvGuard::set("CODEX_HOME", codex_home.into_os_string());

        let report = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: Some(queue_id.clone()),
            codex_executable: Some(fake_external_review_only_codex_executable(&root)),
            timeout_ms: 30_000,
            idle_timeout_ms: 6_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();

        assert_eq!(report.receipt.status, RuntimeRunOnceStatus::RetryPending);
        assert_eq!(report.receipt.queue_id.as_deref(), Some(queue_id.as_str()));
        assert_eq!(
            report.run.as_ref().unwrap().receipt.status,
            CodexRuntimeRunStatus::ProtocolError
        );
        assert!(report.outbound_message.is_none());
        assert!(report.outbox_file.is_none());
        let execution_dir = report
            .run
            .as_ref()
            .unwrap()
            .receipt
            .execution_dir
            .as_ref()
            .unwrap();
        let evidence = execution_dir.join("external-review-evidence.json");
        assert!(evidence.is_file());
        let evidence_text = fs::read_to_string(evidence).unwrap();
        assert!(evidence_text.contains("agent-harness.external-review-evidence.v1"));
        assert!(evidence_text.contains("Claude second brain review"));
        assert!(
            !execution_dir
                .join("codex-runtime-completion-receipt.json")
                .exists(),
            "review-only evidence must not masquerade as a parent workflow completion"
        );
        assert!(
            !harness_home
                .join("state")
                .join("channels")
                .join("outbox.jsonl")
                .exists()
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn run_runtime_queue_once_suppresses_read_only_review_final_for_implementation_goal() {
        let _guard = env_lock();
        let root = temp_root("run_runtime_queue_once_suppresses_read_only_review_final_for_goal");
        let source = write_pipeline_source(&root);
        let harness_home = root.join(".agent-harness");
        let skills = build_source_skill_index(&source).unwrap();
        let receive = receive_channel_message(ChannelReceiveOptions {
            source,
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "telegram".to_string(),
            account_id: None,
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message:
                "開 goal 把所有 package 都完成，先 double-check review loop 後落實下來，準備進入實作"
                    .to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: None,
            inbound_event_id: None,
            skill_limit: 3,
            now_ms: 1234,
        })
        .unwrap();
        assert_eq!(receive.status, ChannelReceiveStatus::AgentTurnQueued);

        let codex_home = root.join("codex-home");
        fs::create_dir_all(&codex_home).unwrap();
        fs::write(codex_home.join("auth.json"), "{}").unwrap();
        let _env = EnvGuard::set("CODEX_HOME", codex_home.into_os_string());

        let report = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: receive.queue_id.clone(),
            codex_executable: Some(fake_read_only_review_final_codex_executable(&root)),
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();

        assert_eq!(report.receipt.status, RuntimeRunOnceStatus::Completed);
        assert!(report.outbound_message.is_none());
        assert!(report.outbox_file.is_none());
        assert!(
            !harness_home
                .join("state")
                .join("channels")
                .join("outbox.jsonl")
                .exists(),
            "review/evidence shaped output for an implementation goal must not masquerade as user final"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn run_runtime_queue_once_writes_final_outbox_for_non_main_agent_owned_group_lane() {
        let _guard = env_lock();
        let root = temp_root("run_runtime_queue_once_writes_non_main_group_final_outbox");
        let source = write_pipeline_source(&root);
        let harness_home = root.join(".agent-harness");
        let skills = build_source_skill_index(&source).unwrap();
        let receive = receive_channel_message(ChannelReceiveOptions {
            source,
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "telegram".to_string(),
            account_id: Some("xiaoxiaoli".to_string()),
            channel_id: "group-alpha".to_string(),
            user_id: "user-limited".to_string(),
            agent_id: Some("xiaoxiaoli".to_string()),
            session_key: None,
            message: "@xiao2li_bot hello".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: Some("telegram-update".to_string()),
            inbound_event_id: Some("487831905".to_string()),
            skill_limit: 3,
            now_ms: 1234,
        })
        .unwrap();
        assert_eq!(receive.status, ChannelReceiveStatus::AgentTurnQueued);
        let exact_session_key = receive.session_key.clone();

        let codex_home = root.join("codex-home");
        fs::create_dir_all(&codex_home).unwrap();
        fs::write(codex_home.join("auth.json"), "{}").unwrap();
        let _env = EnvGuard::set("CODEX_HOME", codex_home.into_os_string());

        let report = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: receive.queue_id.clone(),
            codex_executable: Some(fake_codex_executable(&root)),
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();

        assert_eq!(report.receipt.status, RuntimeRunOnceStatus::Completed);
        assert!(report.outbound_message.is_some());
        let outbox_file = report
            .outbox_file
            .as_ref()
            .expect("owned non-main group lane should write final outbox");
        let outbox_text = fs::read_to_string(outbox_file).unwrap();
        let row: serde_json::Value =
            serde_json::from_str(outbox_text.lines().next().unwrap()).unwrap();
        assert_eq!(
            row.get("kind").and_then(serde_json::Value::as_str),
            Some("agent-reply")
        );
        assert_eq!(
            row.get("platform").and_then(serde_json::Value::as_str),
            Some("telegram")
        );
        assert_eq!(
            row.get("accountId").and_then(serde_json::Value::as_str),
            Some("xiaoxiaoli")
        );
        assert_eq!(
            row.get("channelId").and_then(serde_json::Value::as_str),
            Some("group-alpha")
        );
        assert_eq!(
            row.get("userId").and_then(serde_json::Value::as_str),
            Some("user-limited")
        );
        assert_eq!(
            row.get("sessionKey").and_then(serde_json::Value::as_str),
            Some(exact_session_key.as_str())
        );
        assert!(
            !report
                .warnings
                .iter()
                .any(|warning| warning.contains("classified as InternalEvidence")),
            "owned non-main channel turn must not be classified as internal evidence"
        );
        let log_text = fs::read_to_string(
            harness_home
                .join("state")
                .join("logs")
                .join("harness.jsonl"),
        )
        .unwrap_or_default();
        assert!(!log_text.contains("runtime.run-once.final-outbox-suppressed"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn run_runtime_queue_once_quarantines_owner_mismatched_agent_before_final_outbox() {
        let _guard = env_lock();
        let root =
            temp_root("run_runtime_queue_once_suppresses_owner_mismatched_agent_final_outbox");
        let source = write_pipeline_source(&root);
        let harness_home = root.join(".agent-harness");
        let skills = build_source_skill_index(&source).unwrap();
        let receive = receive_channel_message(ChannelReceiveOptions {
            source,
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "telegram".to_string(),
            account_id: None,
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "complete this delegated implementation".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: None,
            inbound_event_id: None,
            skill_limit: 3,
            now_ms: 1234,
        })
        .unwrap();
        assert_eq!(receive.status, ChannelReceiveStatus::AgentTurnQueued);
        let pending_file = harness_home
            .join("state")
            .join("runtime-queue")
            .join("pending.jsonl");
        let pending = fs::read_to_string(&pending_file).unwrap();
        fs::write(
            &pending_file,
            pending.replace(r#""agentId":"main""#, r#""agentId":"xiaoxiaoli""#),
        )
        .unwrap();
        write_test_channel_session_state(
            &harness_home,
            "telegram",
            "dm-42",
            "user-7",
            "telegram:dm-42:user-7:main",
            "main",
        );

        let codex_home = root.join("codex-home");
        fs::create_dir_all(&codex_home).unwrap();
        fs::write(codex_home.join("auth.json"), "{}").unwrap();
        let _env = EnvGuard::set("CODEX_HOME", codex_home.into_os_string());

        let report = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: receive.queue_id.clone(),
            codex_executable: Some(fake_codex_executable(&root)),
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();

        assert_eq!(report.receipt.status, RuntimeRunOnceStatus::NoWork);
        assert!(report.plan.is_none());
        assert!(report.run.is_none());
        assert!(report.outbound_message.is_none());
        assert!(report.outbox_file.is_none());
        let quarantine = fs::read_to_string(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("execution-receipts.jsonl"),
        )
        .unwrap();
        assert!(quarantine.contains("invalid-canonical-lane-quarantined"));
        assert!(quarantine.contains("session identity is invalid for its exact lane"));
        assert!(
            !harness_home
                .join("state")
                .join("channels")
                .join("outbox.jsonl")
                .exists()
        );
        let log_text = fs::read_to_string(
            harness_home
                .join("state")
                .join("logs")
                .join("harness.jsonl"),
        )
        .unwrap_or_default();
        assert_eq!(
            log_text
                .matches("runtime.run-once.final-outbox-suppressed")
                .count(),
            0,
            "invalid ownership must be quarantined before runtime or final-outbox planning"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn run_runtime_queue_once_suppresses_stale_session_reply_after_new() {
        let _guard = env_lock();
        let root = temp_root("run_runtime_queue_once_suppresses_stale_session_reply_after_new");
        let source = write_pipeline_source(&root);
        let harness_home = root.join(".agent-harness");
        let skills = build_source_skill_index(&source).unwrap();
        let receive = receive_channel_message(ChannelReceiveOptions {
            source: source.clone(),
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills.clone(),
            platform: "telegram".to_string(),
            account_id: None,
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "old in-flight request".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: None,
            inbound_event_id: None,
            skill_limit: 3,
            now_ms: 1234,
        })
        .unwrap();
        assert_eq!(receive.status, ChannelReceiveStatus::AgentTurnQueued);

        let new_session = receive_channel_message(ChannelReceiveOptions {
            source,
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "telegram".to_string(),
            account_id: None,
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "/new".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: None,
            inbound_event_id: None,
            skill_limit: 3,
            now_ms: 1235,
        })
        .unwrap();
        assert_eq!(new_session.status, ChannelReceiveStatus::CommandApplied);

        let fake_codex = fake_codex_executable(&root);
        let codex_home = root.join("codex-home");
        fs::create_dir_all(&codex_home).unwrap();
        fs::write(codex_home.join("auth.json"), "{}").unwrap();
        let _env = EnvGuard::set("CODEX_HOME", codex_home.into_os_string());

        let report = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            codex_executable: Some(fake_codex),
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();

        assert_eq!(report.receipt.status, RuntimeRunOnceStatus::Suppressed);
        assert_eq!(report.receipt.terminal_control_matched, Some(true));
        assert_eq!(
            report.receipt.terminal_control_source.as_deref(),
            Some("scoped-stop")
        );
        assert_eq!(
            report.receipt.suppressed_run_once_reason.as_deref(),
            Some("terminal-control-present")
        );
        assert!(report.run.is_none());
        assert!(report.outbound_message.is_none());
        assert!(
            report
                .warnings
                .iter()
                .any(|warning| warning.contains("terminal control scoped-stop"))
        );
        let outbox = fs::read_to_string(
            harness_home
                .join("state")
                .join("channels")
                .join("outbox.jsonl"),
        )
        .unwrap();
        assert!(outbox.contains("New session planned"));
        assert!(!outbox.contains("Pipeline fake reply."));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn run_runtime_queue_once_agent_reply_outbox_includes_plain_final_presentation() {
        let _guard = env_lock();
        let root = temp_root("run_runtime_queue_once_agent_reply_has_presentation");
        let source = write_pipeline_source(&root);
        let harness_home = root.join(".agent-harness");
        let skills = build_source_skill_index(&source).unwrap();
        let receive = receive_channel_message(ChannelReceiveOptions {
            source,
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "telegram".to_string(),
            account_id: None,
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "ordinary final presentation guard".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: None,
            inbound_event_id: None,
            skill_limit: 3,
            now_ms: 1234,
        })
        .unwrap();
        assert_eq!(receive.status, ChannelReceiveStatus::AgentTurnQueued);

        let fake_codex = fake_codex_executable(&root);
        let codex_home = root.join("codex-home");
        fs::create_dir_all(&codex_home).unwrap();
        fs::write(codex_home.join("auth.json"), "{}").unwrap();
        let _env = EnvGuard::set("CODEX_HOME", codex_home.into_os_string());

        let report = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            codex_executable: Some(fake_codex),
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();

        assert_eq!(report.receipt.status, RuntimeRunOnceStatus::Completed);
        let outbox = fs::read_to_string(
            harness_home
                .join("state")
                .join("channels")
                .join("outbox.jsonl"),
        )
        .unwrap();
        let message: Value = serde_json::from_str(outbox.lines().next().unwrap()).unwrap();
        assert_eq!(message["kind"], "agent-reply");
        assert_eq!(message["text"], "Pipeline fake reply.");
        assert_eq!(
            message["presentation"]["schema"],
            crate::rich_presentation::RICH_MESSAGE_PRESENTATION_SCHEMA
        );
        assert_eq!(
            message["presentation"]["fallbackText"],
            "Pipeline fake reply."
        );
        assert!(message["presentation"]["blocks"].as_array().unwrap().len() >= 1);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn legacy_channel_session_state_does_not_suppress_a_v2_lane() {
        let root = temp_root("legacy_channel_session_state_does_not_suppress_a_v2_lane");
        let harness_home = root.join(".agent-harness");
        let state_file = crate::channel_session_state_file(
            &harness_home,
            "telegram",
            "dm-agent-boundary",
            "user-agent-boundary",
        );
        fs::create_dir_all(state_file.parent().unwrap()).unwrap();
        write_json_atomic(
            &state_file,
            &crate::ChannelSessionState {
                schema: "agent-harness.channel-session-state.v1".to_string(),
                platform: "telegram".to_string(),
                account_id: None,
                channel_id: "dm-agent-boundary".to_string(),
                user_id: "user-agent-boundary".to_string(),
                active_session_key:
                    "telegram:dm-agent-boundary:user-agent-boundary:main:session-live-main"
                        .to_string(),
                agent_id: Some("main".to_string()),
                config_revision: None,
                provider: None,
                model: None,
                session_topic: None,
                model_override: None,
                model_override_provider: None,
                model_override_model: None,
                thinking_enabled: false,
                thinking_level: None,
                thinking_instruction: None,
                reasoning_preference: None,
                backend_reasoning_policy: None,
                fast_mode: None,
                stop_requested: false,
                stop_reason: None,
                steering_notes: Vec::new(),
                btw_notes: Vec::new(),
                last_command: None,
                updated_at_ms: 1234,
            },
        )
        .unwrap();
        let context = QueueChannelContext {
            platform: "telegram".to_string(),
            account_id: None,
            channel_id: "dm-agent-boundary".to_string(),
            user_id: "user-agent-boundary".to_string(),
            agent_id: "xiaoxiaoli".to_string(),
            session_key:
                "telegram:dm-agent-boundary:user-agent-boundary:xiaoxiaoli:session-completed"
                    .to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
        };
        let mut warnings = Vec::new();

        assert!(channel_session_is_current(&harness_home, &context, &mut warnings).unwrap());
        assert!(warnings.is_empty());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn channel_session_freshness_reads_only_the_exact_v2_lane() {
        let root = temp_root("channel_session_freshness_reads_only_the_exact_v2_lane");
        let harness_home = root.join(".agent-harness");
        let lane = ChannelStateLane::new(
            "telegram",
            Some("account-a"),
            "dm-exact-lane",
            "user-exact-lane",
            "main",
        )
        .unwrap();
        let mut state = crate::ChannelSessionState {
            schema: "agent-harness.channel-session-state.v1".to_string(),
            platform: "telegram".to_string(),
            account_id: None,
            channel_id: "dm-exact-lane".to_string(),
            user_id: "user-exact-lane".to_string(),
            active_session_key: "telegram:dm-exact-lane:user-exact-lane:main:session-live"
                .to_string(),
            agent_id: Some("main".to_string()),
            config_revision: None,
            provider: None,
            model: None,
            session_topic: None,
            model_override: None,
            model_override_provider: None,
            model_override_model: None,
            thinking_enabled: false,
            thinking_level: None,
            thinking_instruction: None,
            reasoning_preference: None,
            backend_reasoning_policy: None,
            fast_mode: None,
            stop_requested: false,
            stop_reason: None,
            steering_notes: Vec::new(),
            btw_notes: Vec::new(),
            last_command: None,
            updated_at_ms: 1234,
        };
        crate::bind_channel_session_state_to_lane_v2(&mut state, &lane);
        crate::write_channel_session_state_v2(&harness_home, &lane, &state).unwrap();

        let stale_same_lane = QueueChannelContext {
            platform: "telegram".to_string(),
            account_id: Some("account-a".to_string()),
            channel_id: "dm-exact-lane".to_string(),
            user_id: "user-exact-lane".to_string(),
            agent_id: "main".to_string(),
            session_key: "telegram:dm-exact-lane:user-exact-lane:main:session-completed"
                .to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
        };
        let different_account = QueueChannelContext {
            account_id: Some("account-b".to_string()),
            ..stale_same_lane.clone()
        };
        let different_agent = QueueChannelContext {
            agent_id: "audit-worker".to_string(),
            session_key: "telegram:dm-exact-lane:user-exact-lane:audit-worker:session-completed"
                .to_string(),
            ..stale_same_lane.clone()
        };
        let mut warnings = Vec::new();

        assert!(
            !channel_session_is_current(&harness_home, &stale_same_lane, &mut warnings).unwrap()
        );
        assert!(
            channel_session_is_current(&harness_home, &different_account, &mut warnings).unwrap()
        );
        assert!(
            channel_session_is_current(&harness_home, &different_agent, &mut warnings).unwrap()
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn run_runtime_queue_once_stops_when_only_terminal_queue_items_remain() {
        let _guard = env_lock();
        let root = temp_root("no_work_after_terminal");
        let source = write_pipeline_source(&root);
        let harness_home = root.join(".agent-harness");
        let skills = build_source_skill_index(&source).unwrap();
        let receive = receive_channel_message(ChannelReceiveOptions {
            source,
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "telegram".to_string(),
            account_id: None,
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "repair memory cron".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: None,
            inbound_event_id: None,
            skill_limit: 3,
            now_ms: 1234,
        })
        .unwrap();
        assert_eq!(receive.status, ChannelReceiveStatus::AgentTurnQueued);
        let fake_codex = fake_codex_executable(&root);
        let codex_home = root.join("codex-home");
        fs::create_dir_all(&codex_home).unwrap();
        fs::write(codex_home.join("auth.json"), "{}").unwrap();
        let _env = EnvGuard::set("CODEX_HOME", codex_home.into_os_string());

        let first = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            codex_executable: Some(fake_codex.clone()),
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();
        assert_eq!(first.receipt.status, RuntimeRunOnceStatus::Completed);
        assert!(first.outbox_file.is_some());

        let second = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            codex_executable: Some(fake_codex),
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();

        assert_eq!(second.receipt.status, RuntimeRunOnceStatus::Suppressed);
        assert_eq!(
            second.receipt.terminal_control_source.as_deref(),
            Some("run-once-terminal")
        );
        assert_eq!(
            second.prepare.as_ref().unwrap().receipt.status,
            RuntimeExecutionReceiptStatus::NoPendingItem
        );
        assert!(second.plan.is_none());
        assert!(second.run.is_none());
        assert!(second.outbound_message.is_none());
        let outbox = fs::read_to_string(
            harness_home
                .join("state")
                .join("channels")
                .join("outbox.jsonl"),
        )
        .unwrap();
        assert_eq!(outbox.lines().count(), 1);

        let _ = fs::remove_dir_all(root);
    }

    struct EnvGuard {
        key: &'static str,
        old: Option<OsString>,
    }

    impl EnvGuard {
        fn set(key: &'static str, value: OsString) -> Self {
            let old = std::env::var_os(key);
            unsafe {
                std::env::set_var(key, value);
            }
            Self { key, old }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            unsafe {
                match &self.old {
                    Some(value) => std::env::set_var(self.key, value),
                    None => std::env::remove_var(self.key),
                }
            }
        }
    }

    fn assert_terminal_progress_once_then_silent(
        harness_home: &Path,
        platform: &str,
        queue_id: &str,
    ) {
        let first = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.to_path_buf(),
            platform: Some(platform.to_string()),
            now_ms: 6_000,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert!(!first.pending.is_empty(), "{:#?}", first.pending);
        assert!(
            first
                .pending
                .iter()
                .all(|pending| pending.queue_id == queue_id && pending.terminal),
            "{:#?}",
            first.pending
        );
        assert!(
            first.pending.iter().any(|pending| {
                pending.message_kind == AgentProgressDeliveryMessageKind::Status
                    && pending.text.contains("Stopped")
            }),
            "{:#?}",
            first.pending
        );
        let seed = first.pending[0].clone();
        let late_context = AgentProgressContext {
            queue_id: seed.queue_id.clone(),
            agent_id: seed.agent_id.clone(),
            account_id: seed.account_id.clone(),
            thread_id: seed.thread_id.clone(),
            session_key: seed.session_key.clone(),
            platform: seed.platform.clone(),
            channel_id: seed.channel_id.clone(),
            user_id: seed.user_id.clone(),
        };
        for pending in first.pending {
            let provider_suffix = match pending.message_kind {
                AgentProgressDeliveryMessageKind::Body => "body",
                AgentProgressDeliveryMessageKind::Status => "status",
            };
            record_agent_progress_delivery(AgentProgressDeliveryRecordOptions {
                harness_home: harness_home.to_path_buf(),
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
                provider_message_id: Some(format!("provider-{provider_suffix}")),
                event_line: pending.event_line,
                text_hash: pending.text_hash,
                terminal: pending.terminal,
                policy_decision: Some("test".to_string()),
                error: None,
                now_ms: 6_500,
            })
            .unwrap();
        }
        append_agent_progress_event(
            harness_home,
            &AgentProgressEvent::new(
                &late_context,
                AgentProgressKind::ToolCall,
                "tool_call",
                "late progress after terminal control",
                AgentProgressStatus::Progress,
                7_000,
            ),
        )
        .unwrap();

        let after_late_event = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.to_path_buf(),
            platform: Some(platform.to_string()),
            now_ms: 8_000,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert!(
            after_late_event.pending.is_empty(),
            "{:#?}",
            after_late_event.pending
        );
    }

    fn canonical_test_session(session_key: &str) -> String {
        let platform = session_key.split(':').next().unwrap_or("telegram");
        crate::channel_session_key::CanonicalChannelSessionKey::bind_for_lane(
            session_key,
            platform,
            "default",
            "dm-42",
            "user-7",
            "main",
        )
        .unwrap()
        .canonical_string()
    }

    fn canonical_test_continuation(session_key: &str, index: u64) -> String {
        let platform = session_key.split(':').next().unwrap_or("telegram");
        crate::channel_session_key::CanonicalChannelSessionKey::bind_for_lane(
            session_key,
            platform,
            "default",
            "dm-42",
            "user-7",
            "main",
        )
        .unwrap()
        .continuation(index)
        .unwrap()
        .canonical_string()
    }

    fn prepared_test_item(
        queue_id: &str,
        session_key: &str,
        continuation_index: Option<u64>,
    ) -> RuntimeQueuePreparedItem {
        RuntimeQueuePreparedItem {
            queue_id: queue_id.to_string(),
            admission_queue_id: None,
            agent_id: "main".to_string(),
            session_key: session_key.to_string(),
            runtime_class: "interactive".to_string(),
            origin: "channel".to_string(),
            cron_run_id: None,
            scheduled_for_ms: None,
            platform: session_key
                .split(':')
                .next()
                .unwrap_or("telegram")
                .to_string(),
            account_id: None,
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            message_text: "continue after recovery".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            provider: Some("openai".to_string()),
            model: Some("gpt-5.5".to_string()),
            reasoning_preference: None,
            backend_reasoning_policy: None,
            authorized_execution_mode: None,
            execution_dir: PathBuf::from("execution"),
            prompt_bundle_json: PathBuf::from("prompt-bundle.json"),
            prompt_markdown: PathBuf::from("prompt.md"),
            receipt_file: PathBuf::from("execution-receipt.json"),
            planned_transcript_file: PathBuf::from("transcript.jsonl"),
            planned_trajectory_file: PathBuf::from("trajectory.jsonl"),
            selected_skill_ids: Vec::new(),
            virtual_skill_manifest_file: None,
            skill_delivery_receipt_files: Vec::new(),
            continuation: RuntimeContinuationMetadata {
                continuation_index,
                ..RuntimeContinuationMetadata::legacy()
            },
        }
    }

    #[test]
    fn shell_recovery_effect_fence_rejects_mutation_or_ambiguous_evidence() {
        let mut receipt = polluted_thread_failed_run("queue-shell-fence").receipt;
        receipt.mutation_evidence = None;
        assert!(shell_recovery_effect_fence_clear(&receipt));
        receipt.mutation_evidence = Some(crate::RuntimeMutationEvidenceClass::NoObservableMutation);
        assert!(shell_recovery_effect_fence_clear(&receipt));
        receipt.mutation_evidence = Some(crate::RuntimeMutationEvidenceClass::MutationObserved);
        assert!(!shell_recovery_effect_fence_clear(&receipt));
        receipt.mutation_evidence = Some(crate::RuntimeMutationEvidenceClass::Unknown);
        assert!(!shell_recovery_effect_fence_clear(&receipt));
    }

    fn polluted_thread_failed_run(queue_id: &str) -> CodexRuntimeRunReport {
        CodexRuntimeRunReport {
            schema: "agent-harness.codex-runtime-run.v1",
            harness_home: PathBuf::from(".agent-harness"),
            execution_dir: Some(PathBuf::from("execution")),
            plan_file: Some(PathBuf::from("codex-runtime-plan.json")),
            run_file: Some(PathBuf::from("codex-runtime-run.json")),
            receipts_file: PathBuf::from("codex-runtime-run-receipts.jsonl"),
            receipt: crate::CodexRuntimeRunReceipt {
                queue_id: Some(queue_id.to_string()),
                status: CodexRuntimeRunStatus::ProtocolError,
                execution_dir: Some(PathBuf::from("execution")),
                plan_file: Some(PathBuf::from("codex-runtime-plan.json")),
                run_file: Some(PathBuf::from("codex-runtime-run.json")),
                completion_file: None,
                transcript_file: None,
                trajectory_file: None,
                codex_binding_file: None,
                reason: "app-server completed without final answer after polluted thread health recovery".to_string(),
                elapsed_ms: 300_000,
                event_count: 0,
                usage: None,
                media_plan: Default::default(),
                context_recovery: Some(CodexContextRecoveryReceipt {
                    status: "compact-before-turn-fallback-failed".to_string(),
                    thread_health_status: Some(CodexThreadHealthStatus::PollutedAfterCompact),
                    queue_id: Some(queue_id.to_string()),
                    session_key: "telegram:dm-42:user-7:main".to_string(),
                    original_thread_id: Some("thread-polluted".to_string()),
                    recovered_thread_id: Some("thread-fresh".to_string()),
                    official_compact_attempts: 1,
                    retry_attempted: true,
                    fallback_policy: "checkpoint-and-new-thread".to_string(),
                    fresh_thread_attempted: true,
                    fresh_thread_succeeded: false,
                    checkpoint_file: None,
                    rollover_file: None,
                    binding_backup_file: None,
                    reason: "bound thread health guard found inline image bloat and fallback failed"
                        .to_string(),
                }),
                tool_use_timeout: None,
                shell_execution_failure: None,
                subagent_lifecycle: None,
                interruption_reason: None,
                interrupted_tool_uses: Vec::new(),
                backend_reasoning_execution: None,
                protocol_failure: None,
                mutation_evidence: None,
                external_effect: None,
                drain_checkpoint: None,
                drain_disposition: None,
                drain_disposition_error: None,
                primary_outcome: crate::CodexRuntimePrimaryOutcomeV1::RuntimeFailure,
                secondary_diagnostics: Vec::new(),
                work_authority_class: crate::WorkAuthorityClassV1::Unknown,
            },
            completion: None,
            stdout_log: None,
            stderr_log: None,
            warnings: Vec::new(),
        }
    }

    fn stream_unstable_failed_run(
        queue_id: &str,
        input_tokens: Option<u64>,
        include_media: bool,
    ) -> CodexRuntimeRunReport {
        let mut media_plan = crate::InboundMediaInputPlan::default();
        if include_media {
            media_plan.artifacts.push(InboundMediaArtifact {
                platform: "discord".to_string(),
                kind: "attachment-image".to_string(),
                artifact_uri: Some("agent-harness://inbound-media/discord/msg-1/0.jpg".to_string()),
                mime: Some("image/jpeg".to_string()),
                sha256: Some("abc123".to_string()),
                ..InboundMediaArtifact::default()
            });
        }
        CodexRuntimeRunReport {
            schema: "agent-harness.codex-runtime-run.v1",
            harness_home: PathBuf::from(".agent-harness"),
            execution_dir: Some(PathBuf::from("execution")),
            plan_file: Some(PathBuf::from("codex-runtime-plan.json")),
            run_file: Some(PathBuf::from("codex-runtime-run.json")),
            receipts_file: PathBuf::from("codex-runtime-run-receipts.jsonl"),
            receipt: crate::CodexRuntimeRunReceipt {
                queue_id: Some(queue_id.to_string()),
                status: CodexRuntimeRunStatus::ProtocolError,
                execution_dir: Some(PathBuf::from("execution")),
                plan_file: Some(PathBuf::from("codex-runtime-plan.json")),
                run_file: Some(PathBuf::from("codex-runtime-run.json")),
                completion_file: None,
                transcript_file: None,
                trajectory_file: None,
                codex_binding_file: None,
                reason: "Reconnecting... 2/5; stream disconnected before completion: websocket closed by server before response.completed".to_string(),
                elapsed_ms: 1_584_219,
                event_count: 2_110,
                usage: input_tokens.map(|tokens| CodexRuntimeUsage {
                    input_tokens: Some(tokens),
                    output_tokens: Some(2_513),
                    total_tokens: Some(tokens.saturating_add(2_513)),
                    model_context_window: None,
                    model_context_window_source: None,
                    provider: None,
                    model: None,
                    backend_context_generation: None,
                    observed_at_ms: None,
                    source: "test".to_string(),
                    raw: None,
                }),
                media_plan,
                context_recovery: None,
                tool_use_timeout: None,
                shell_execution_failure: None,
                subagent_lifecycle: None,
                interruption_reason: None,
                interrupted_tool_uses: Vec::new(),
                backend_reasoning_execution: None,
                protocol_failure: None,
                mutation_evidence: None,
                external_effect: None,
                drain_checkpoint: None,
                drain_disposition: None,
                drain_disposition_error: None,
                primary_outcome: crate::CodexRuntimePrimaryOutcomeV1::ProviderProtocolFailure,
                secondary_diagnostics: Vec::new(),
                work_authority_class: crate::WorkAuthorityClassV1::Unknown,
            },
            completion: None,
            stdout_log: None,
            stderr_log: None,
            warnings: Vec::new(),
        }
    }

    fn interrupted_long_task_run(
        queue_id: &str,
        status: CodexRuntimeRunStatus,
        recovery_status: Option<&str>,
        include_tool_timeout: bool,
    ) -> CodexRuntimeRunReport {
        let context_recovery = recovery_status.map(|status| CodexContextRecoveryReceipt {
            status: status.to_string(),
            thread_health_status: None,
            queue_id: Some(queue_id.to_string()),
            session_key: "discord:dm-42:user-7:main".to_string(),
            original_thread_id: Some("thread-interrupted".to_string()),
            recovered_thread_id: Some("thread-fallback".to_string()),
            official_compact_attempts: 0,
            retry_attempted: true,
            fallback_policy: "checkpoint-and-new-thread".to_string(),
            fresh_thread_attempted: true,
            fresh_thread_succeeded: false,
            checkpoint_file: None,
            rollover_file: None,
            binding_backup_file: None,
            reason:
                "Codex tool-use idle timeout fallback also failed. Prior reason: pwsh: cargo clippy"
                    .to_string(),
        });
        CodexRuntimeRunReport {
            schema: "agent-harness.codex-runtime-run.v1",
            harness_home: PathBuf::from(".agent-harness"),
            execution_dir: Some(PathBuf::from("execution")),
            plan_file: Some(PathBuf::from("codex-runtime-plan.json")),
            run_file: Some(PathBuf::from("codex-runtime-run.json")),
            receipts_file: PathBuf::from("codex-runtime-run-receipts.jsonl"),
            receipt: crate::CodexRuntimeRunReceipt {
                queue_id: Some(queue_id.to_string()),
                status,
                execution_dir: Some(PathBuf::from("execution")),
                plan_file: Some(PathBuf::from("codex-runtime-plan.json")),
                run_file: Some(PathBuf::from("codex-runtime-run.json")),
                completion_file: None,
                transcript_file: None,
                trajectory_file: None,
                codex_binding_file: None,
                reason: crate::codex_runtime::NO_FINAL_ANSWER_WITH_NARRATION_MARKER.to_string(),
                elapsed_ms: 1_800_000,
                event_count: 20_937,
                usage: None,
                media_plan: Default::default(),
                context_recovery,
                tool_use_timeout: include_tool_timeout.then(|| CodexToolUseTimeout {
                    method: "pwsh".to_string(),
                    item_id: Some("cmd-1".to_string()),
                    item_type: Some("commandExecution".to_string()),
                    preview: Some("cargo clippy".to_string()),
                    started_at_ms: Some(1_000),
                    reason: "tool execution exceeded timeout".to_string(),
                }),
                shell_execution_failure: None,
                subagent_lifecycle: None,
                interruption_reason: None,
                interrupted_tool_uses: Vec::new(),
                backend_reasoning_execution: None,
                protocol_failure: None,
                mutation_evidence: None,
                external_effect: None,
                drain_checkpoint: None,
                drain_disposition: None,
                drain_disposition_error: None,
                primary_outcome: if status == CodexRuntimeRunStatus::Timeout {
                    crate::CodexRuntimePrimaryOutcomeV1::AbsoluteTimeout
                } else {
                    crate::CodexRuntimePrimaryOutcomeV1::RuntimeFailure
                },
                secondary_diagnostics: Vec::new(),
                work_authority_class: crate::WorkAuthorityClassV1::Unknown,
            },
            completion: None,
            stdout_log: None,
            stderr_log: None,
            warnings: Vec::new(),
        }
    }

    fn seed_prepared_pending_for_stream_retry(
        harness_home: &Path,
        queue_id: &str,
        session_key: &str,
    ) {
        let queue_dir = harness_home.join("state").join("runtime-queue");
        fs::create_dir_all(&queue_dir).unwrap();
        append_json_line(
            &queue_dir.join("pending.jsonl"),
            &serde_json::json!({
                "queueId": queue_id,
                "status": "queued",
                "runtimeClass": "interactive",
                "origin": "channel",
                "agentId": "main",
                "platform": "discord",
                "accountId": "default",
                "channelId": "dm-42",
                "userId": "user-7",
                "sessionKey": session_key,
                "inboundMediaArtifacts": [{
                    "schema": crate::INBOUND_MEDIA_ARTIFACT_SCHEMA,
                    "platform": "discord",
                    "kind": "attachment-image",
                    "artifactUri": "agent-harness://inbound-media/discord/msg-1/0.jpg",
                    "mime": "image/jpeg",
                    "sha256": "abc123",
                    "downloadStatus": "downloaded",
                    "modelAttachmentStatus": "vision-tool-available"
                }]
            }),
        )
        .unwrap();
        append_json_line(
            &queue_dir.join("execution-receipts.jsonl"),
            &serde_json::json!({
                "queueId": queue_id,
                "status": "prepared",
                "runtimeClass": "interactive",
                "origin": "channel",
                "executionDir": "execution"
            }),
        )
        .unwrap();
        let state_file =
            crate::channel_session_state_file(harness_home, "discord", "dm-42", "user-7");
        write_json_atomic(
            &state_file,
            &crate::ChannelSessionState {
                schema: "agent-harness.channel-session-state.v1".to_string(),
                platform: "discord".to_string(),
                account_id: None,
                channel_id: "dm-42".to_string(),
                user_id: "user-7".to_string(),
                active_session_key: session_key.to_string(),
                agent_id: Some("main".to_string()),
                config_revision: None,
                provider: None,
                model: None,
                session_topic: None,
                model_override: None,
                model_override_provider: None,
                model_override_model: None,
                thinking_enabled: false,
                thinking_level: None,
                thinking_instruction: None,
                reasoning_preference: None,
                backend_reasoning_policy: None,
                fast_mode: None,
                stop_requested: false,
                stop_reason: None,
                steering_notes: Vec::new(),
                btw_notes: Vec::new(),
                last_command: None,
                updated_at_ms: 1_234,
            },
        )
        .unwrap();
    }

    fn write_test_channel_session_state(
        harness_home: &Path,
        platform: &str,
        channel_id: &str,
        user_id: &str,
        session_key: &str,
        agent_id: &str,
    ) {
        let state_file =
            crate::channel_session_state_file(harness_home, platform, channel_id, user_id);
        write_json_atomic(
            &state_file,
            &crate::ChannelSessionState {
                schema: "agent-harness.channel-session-state.v1".to_string(),
                platform: platform.to_string(),
                account_id: None,
                channel_id: channel_id.to_string(),
                user_id: user_id.to_string(),
                active_session_key: session_key.to_string(),
                agent_id: Some(agent_id.to_string()),
                config_revision: None,
                provider: None,
                model: None,
                session_topic: None,
                model_override: None,
                model_override_provider: None,
                model_override_model: None,
                thinking_enabled: false,
                thinking_level: None,
                thinking_instruction: None,
                reasoning_preference: None,
                backend_reasoning_policy: None,
                fast_mode: None,
                stop_requested: false,
                stop_reason: None,
                steering_notes: Vec::new(),
                btw_notes: Vec::new(),
                last_command: None,
                updated_at_ms: 1_234,
            },
        )
        .unwrap();
    }

    fn prepare_already_recorded_completion_without_outbox(
        root: &Path,
    ) -> (PathBuf, String, PathBuf, EnvGuard) {
        prepare_already_recorded_completion_without_outbox_for_platform(
            root, "telegram", "dm-42", "user-7",
        )
    }

    fn prepare_already_recorded_completion_without_outbox_for_platform(
        root: &Path,
        platform: &str,
        channel_id: &str,
        user_id: &str,
    ) -> (PathBuf, String, PathBuf, EnvGuard) {
        let source = write_pipeline_source(root);
        let harness_home = root.join(".agent-harness");
        let skills = build_source_skill_index(&source).unwrap();
        let receive = receive_channel_message(ChannelReceiveOptions {
            source,
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: platform.to_string(),
            account_id: None,
            channel_id: channel_id.to_string(),
            user_id: user_id.to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "repair memory cron".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: None,
            inbound_event_id: None,
            skill_limit: 3,
            now_ms: 1234,
        })
        .unwrap();
        assert_eq!(receive.status, ChannelReceiveStatus::AgentTurnQueued);

        let prepare = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();
        assert_eq!(
            prepare.receipt.status,
            RuntimeExecutionReceiptStatus::Prepared
        );
        let queue_id = prepare.receipt.queue_id.clone().unwrap();
        let plan = plan_codex_runtime(CodexRuntimePlanOptions {
            harness_home: harness_home.clone(),
            execution_dir: prepare.receipt.execution_dir.clone(),
            codex_executable: Some(fake_codex_executable(root)),
        })
        .unwrap();
        let codex_home = root.join("codex-home");
        fs::create_dir_all(&codex_home).unwrap();
        fs::write(codex_home.join("auth.json"), "{}").unwrap();
        let env = EnvGuard::set("CODEX_HOME", codex_home.into_os_string());
        let first = run_codex_runtime(CodexRuntimeRunOptions {
            harness_home: harness_home.clone(),
            execution_dir: plan.execution_dir.clone(),
            plan_file: plan.plan_file.clone(),
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            progress_context: None,
        })
        .unwrap();
        assert_eq!(first.receipt.status, CodexRuntimeRunStatus::Completed);
        assert!(first.receipt.completion_file.as_ref().unwrap().is_file());
        assert!(
            !harness_home
                .join("state")
                .join("channels")
                .join("outbox.jsonl")
                .exists()
        );
        release_runtime_queue_lease(&harness_home, &queue_id).unwrap();
        (
            harness_home,
            queue_id,
            first.receipt.execution_dir.unwrap(),
            env,
        )
    }

    #[cfg(windows)]
    fn fake_codex_executable(root: &Path) -> PathBuf {
        let script = root.join("fake-app-server.ps1");
        fs::write(
            &script,
            r#"
while ($true) {
    $line = [Console]::In.ReadLine()
    if ($null -eq $line) { break }
    try {
        $msg = $line | ConvertFrom-Json
    } catch {
        continue
    }
    if ($msg.id -eq 0) {
        [Console]::Out.WriteLine('{"id":0,"result":{"ok":true}}')
        [Console]::Out.Flush()
    } elseif ($msg.method -eq 'thread/start') {
        [Console]::Out.WriteLine('{"id":1,"result":{"thread":{"id":"thread-pipeline"}}}')
        [Console]::Out.Flush()
    } elseif ($msg.method -eq 'turn/start') {
        [Console]::Out.WriteLine('{"method":"turn/started","params":{"threadId":"thread-pipeline","turn":{"id":"turn-pipeline","kind":"regular"}}}')
        [Console]::Out.WriteLine('{"method":"item/agentMessage/delta","params":{"threadId":"thread-pipeline","turnId":"turn-pipeline","itemId":"msg-pipeline","delta":"Pipeline fake reply."}}')
        [Console]::Out.WriteLine('{"method":"turn/completed","params":{"threadId":"thread-pipeline","turn":{"id":"turn-pipeline","status":"completed"}}}')
        [Console]::Out.Flush()
        break
    }
}
"#,
        )
        .unwrap();
        let cmd = root.join("fake-codex.cmd");
        fs::write(
            &cmd,
            format!(
                "@echo off\r\npowershell -NoProfile -ExecutionPolicy Bypass -File \"{}\"\r\n",
                script.display()
            ),
        )
        .unwrap();
        cmd
    }

    #[cfg(windows)]
    fn fake_active_goal_codex_executable(root: &Path) -> PathBuf {
        let script = root.join("fake-active-goal-app-server.ps1");
        fs::write(
            &script,
            r#"
while ($true) {
    $line = [Console]::In.ReadLine()
    if ($null -eq $line) { break }
    try { $msg = $line | ConvertFrom-Json } catch { continue }
    if ($msg.id -eq 0) {
        [Console]::Out.WriteLine('{"id":0,"result":{"ok":true}}')
    } elseif ($msg.method -eq 'thread/start') {
        [Console]::Out.WriteLine('{"id":1,"result":{"thread":{"id":"thread-active-goal"}}}')
    } elseif ($msg.method -eq 'turn/start') {
        [Console]::Out.WriteLine('{"method":"turn/started","params":{"threadId":"thread-active-goal","turn":{"id":"turn-active-goal","kind":"regular"}}}')
        [Console]::Out.WriteLine('{"method":"thread/goal/updated","params":{"threadId":"thread-active-goal","turnId":"turn-active-goal","goal":{"id":"goal-active","objective":"finish the durable campaign","status":"active","completionCriteria":["T3 replay passes"]}}}')
        [Console]::Out.WriteLine('{"method":"item/agentMessage/delta","params":{"threadId":"thread-active-goal","turnId":"turn-active-goal","itemId":"msg-active-goal","delta":"Slice checkpoint only."}}')
        [Console]::Out.WriteLine('{"method":"turn/completed","params":{"threadId":"thread-active-goal","turn":{"id":"turn-active-goal","status":"completed"}}}')
        [Console]::Out.Flush()
        break
    }
    [Console]::Out.Flush()
}
"#,
        )
        .unwrap();
        let cmd = root.join("fake-active-goal-codex.cmd");
        fs::write(
            &cmd,
            format!(
                "@echo off\r\npowershell -NoProfile -ExecutionPolicy Bypass -File \"{}\"\r\n",
                script.display()
            ),
        )
        .unwrap();
        cmd
    }

    #[cfg(windows)]
    fn fake_deadline_drain_checkpoint_codex_executable(
        root: &Path,
        assistant_text: &str,
    ) -> PathBuf {
        let assistant_text = serde_json::to_string(assistant_text).unwrap();
        let script = root.join("fake-deadline-drain-checkpoint-app-server.ps1");
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
    }} elseif ($msg.method -eq 'modelProvider/capabilities/read') {{
        [Console]::Out.WriteLine('{{"id":8900,"result":{{"namespaceTools":true,"imageGeneration":true,"webSearch":true}}}}')
    }} elseif ($msg.method -eq 'thread/start') {{
        [Console]::Out.WriteLine('{{"id":1,"result":{{"thread":{{"id":"thread-deadline-plan"}}}}}}')
    }} elseif ($msg.method -eq 'turn/start') {{
        [Console]::Out.WriteLine('{{"method":"turn/started","params":{{"threadId":"thread-deadline-plan","turn":{{"id":"turn-deadline-plan","kind":"regular"}}}}}}')
        [Console]::Out.Flush()
    }} elseif ($msg.method -eq 'turn/steer') {{
        [Console]::Out.WriteLine((ConvertTo-Json @{{id=$msg.id;result=@{{turnId='turn-deadline-plan'}}}} -Compress))
        [Console]::Out.WriteLine('{{"method":"item/completed","params":{{"threadId":"thread-deadline-plan","turnId":"turn-deadline-plan","item":{{"id":"yield-prompt","type":"userMessage","text":"Runtime bounded-yield guard: observed"}}}}}}')
        [Console]::Out.WriteLine('{{"method":"item/completed","params":{{"threadId":"thread-deadline-plan","turnId":"turn-deadline-plan","item":{{"id":"yield-final","type":"agentMessage","phase":"final_answer","text":{assistant_text}}}}}}}')
        [Console]::Out.WriteLine('{{"method":"turn/completed","params":{{"threadId":"thread-deadline-plan","turn":{{"id":"turn-deadline-plan","status":"completed"}}}}}}')
        [Console]::Out.Flush()
        break
    }}
    [Console]::Out.Flush()
}}
"#,
            ),
        )
        .unwrap();
        let cmd = root.join("fake-deadline-drain-checkpoint-codex.cmd");
        fs::write(
            &cmd,
            format!(
                "@echo off\r\npowershell -NoProfile -ExecutionPolicy Bypass -File \"{}\"\r\n",
                script.display()
            ),
        )
        .unwrap();
        cmd
    }

    #[cfg(windows)]
    fn fake_explicit_checkpoint_deadline_codex_executable(root: &Path) -> PathBuf {
        let checkpoint = "ordinary task remains incomplete";
        let checkpoint_digest = crate::task_transition::sha256_hex(checkpoint.as_bytes());
        let script = root.join("fake-explicit-checkpoint-deadline-app-server.ps1");
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
    }} elseif ($msg.method -eq 'modelProvider/capabilities/read') {{
        [Console]::Out.WriteLine('{{"id":8900,"result":{{"namespaceTools":true,"imageGeneration":true,"webSearch":true}}}}')
    }} elseif ($msg.method -eq 'thread/start') {{
        [Console]::Out.WriteLine('{{"id":1,"result":{{"thread":{{"id":"thread-explicit-task"}}}}}}')
    }} elseif ($msg.method -eq 'turn/start') {{
        [Console]::Out.WriteLine('{{"method":"turn/started","params":{{"threadId":"thread-explicit-task","turn":{{"id":"turn-explicit-task","kind":"regular"}}}}}}')
    }} elseif ($msg.method -eq 'turn/steer') {{
        $instruction = [string]$msg.params.input[0].text
        $match = [regex]::Match($instruction, 'authorityId ([0-9a-f]{{64}}), authorityVersion ([0-9]+)')
        $generation = [regex]::Match($instruction, 'observedDeadlineGeneration to ([0-9]+)')
        if (-not $match.Success -or -not $generation.Success) {{ exit 23 }}
        $disposition = @{{
            schema='agent-harness.drain-disposition.v1'
            disposition='continuation-required'
            observedDeadlineGeneration=[uint32]$generation.Groups[1].Value
            authorityKind='explicit-checkpoint'
            authorityId=$match.Groups[1].Value
            authorityVersion=[uint64]$match.Groups[2].Value
            taskFamilyId=$match.Groups[1].Value
            taskFamilyVersion=[uint64]$match.Groups[2].Value
            activeItemId=$null
            activeItemVersion=$null
            checkpoint='{checkpoint}'
            checkpointDigest='{checkpoint_digest}'
            completionEvidenceDigests=@()
        }} | ConvertTo-Json -Compress
        $assistant = 'Ordinary work is checkpointed.' + [Environment]::NewLine + '<agent-harness-drain-disposition>' + $disposition + '</agent-harness-drain-disposition>'
        [Console]::Out.WriteLine((@{{id=$msg.id;result=@{{turnId='turn-explicit-task'}}}} | ConvertTo-Json -Compress))
        [Console]::Out.WriteLine('{{"method":"item/completed","params":{{"threadId":"thread-explicit-task","turnId":"turn-explicit-task","item":{{"id":"yield-prompt","type":"userMessage","text":"Runtime bounded-yield guard: observed"}}}}}}')
        [Console]::Out.WriteLine((@{{method='item/completed';params=@{{threadId='thread-explicit-task';turnId='turn-explicit-task';item=@{{id='yield-final';type='agentMessage';phase='final_answer';text=$assistant}}}}}} | ConvertTo-Json -Compress -Depth 8))
        [Console]::Out.WriteLine('{{"method":"turn/completed","params":{{"threadId":"thread-explicit-task","turn":{{"id":"turn-explicit-task","status":"completed"}}}}}}')
        [Console]::Out.Flush()
        break
    }}
    [Console]::Out.Flush()
}}
"#,
            ),
        )
        .unwrap();
        let cmd = root.join("fake-explicit-checkpoint-deadline-codex.cmd");
        fs::write(
            &cmd,
            format!(
                "@echo off\r\npowershell -NoProfile -ExecutionPolicy Bypass -File \"{}\"\r\n",
                script.display()
            ),
        )
        .unwrap();
        cmd
    }

    #[cfg(windows)]
    fn fake_productive_deadline_renewal_codex_executable(root: &Path) -> PathBuf {
        let script = root.join("fake-productive-deadline-renewal-app-server.ps1");
        fs::write(
            &script,
            r#"
while ($true) {
    $line = [Console]::In.ReadLine()
    if ($null -eq $line) { break }
    try { $msg = $line | ConvertFrom-Json } catch { continue }
    if ($msg.id -eq 0) {
        [Console]::Out.WriteLine('{"id":0,"result":{"ok":true}}')
    } elseif ($msg.method -eq 'modelProvider/capabilities/read') {
        [Console]::Out.WriteLine('{"id":8900,"result":{"namespaceTools":true,"imageGeneration":true,"webSearch":true}}')
    } elseif ($msg.method -eq 'thread/start') {
        [Console]::Out.WriteLine('{"id":1,"result":{"thread":{"id":"thread-productive"}}}')
    } elseif ($msg.method -eq 'turn/start') {
        [Console]::Out.WriteLine('{"method":"turn/started","params":{"threadId":"thread-productive","turn":{"id":"turn-productive","kind":"regular"}}}')
        [Console]::Out.WriteLine('{"method":"thread/goal/updated","params":{"threadId":"thread-productive","turnId":"turn-productive","goal":{"id":"goal-telemetry-only","objective":"track productive work","status":"active","completionCriteria":["finish"]}}}')
        [Console]::Out.Flush()
        Start-Sleep -Milliseconds 45000
        [Console]::Out.WriteLine('{"method":"item/completed","params":{"threadId":"thread-productive","turnId":"turn-productive","item":{"id":"cmd-productive","type":"commandExecution","status":"completed"}}}')
        [Console]::Out.Flush()
        Start-Sleep -Milliseconds 17000
        [Console]::Out.WriteLine('{"method":"thread/goal/updated","params":{"threadId":"thread-productive","turnId":"turn-productive","goal":{"id":"goal-telemetry-only","objective":"track productive work","status":"completed","completionCriteria":["finish"]}}}')
        [Console]::Out.WriteLine('{"method":"item/agentMessage/delta","params":{"threadId":"thread-productive","turnId":"turn-productive","delta":"Productive task completed after the original deadline."}}')
        [Console]::Out.WriteLine('{"method":"turn/completed","params":{"threadId":"thread-productive","turn":{"id":"turn-productive","status":"completed"}}}')
        [Console]::Out.Flush()
        break
    }
    [Console]::Out.Flush()
}
"#,
        )
        .unwrap();
        let cmd = root.join("fake-productive-deadline-renewal-codex.cmd");
        fs::write(
            &cmd,
            format!(
                "@echo off\r\npowershell -NoProfile -ExecutionPolicy Bypass -File \"{}\"\r\n",
                script.display()
            ),
        )
        .unwrap();
        cmd
    }

    #[cfg(windows)]
    fn fake_partial_final_bounded_yield_codex_executable(root: &Path) -> PathBuf {
        let script = root.join("fake-partial-final-bounded-yield-app-server.ps1");
        fs::write(
            &script,
            r#"
while ($true) {
    $line = [Console]::In.ReadLine()
    if ($null -eq $line) { break }
    try { $msg = $line | ConvertFrom-Json } catch { continue }
    if ($msg.id -eq 0) {
        [Console]::Out.WriteLine('{"id":0,"result":{"ok":true}}')
    } elseif ($msg.method -eq 'modelProvider/capabilities/read') {
        [Console]::Out.WriteLine('{"id":8900,"result":{"namespaceTools":true,"imageGeneration":true,"webSearch":true}}')
    } elseif ($msg.method -eq 'thread/start') {
        [Console]::Out.WriteLine('{"id":1,"result":{"thread":{"id":"thread-partial-yield"}}}')
    } elseif ($msg.method -eq 'turn/start') {
        [Console]::Out.WriteLine('{"method":"turn/started","params":{"threadId":"thread-partial-yield","turn":{"id":"turn-partial-yield","kind":"regular"}}}')
    } elseif ($msg.method -eq 'turn/steer') {
        [Console]::Out.WriteLine((@{id=$msg.id;result=@{turnId='turn-partial-yield'}} | ConvertTo-Json -Compress))
        [Console]::Out.WriteLine('{"method":"item/started","params":{"threadId":"thread-partial-yield","turnId":"turn-partial-yield","item":{"id":"yield-prompt","type":"userMessage","text":"Runtime bounded-yield guard: observed"}}}')
        [Console]::Out.WriteLine('{"method":"item/completed","params":{"threadId":"thread-partial-yield","turnId":"turn-partial-yield","item":{"id":"yield-prompt","type":"userMessage","text":"Runtime bounded-yield guard: observed"}}}')
        [Console]::Out.WriteLine('{"method":"item/started","params":{"threadId":"thread-partial-yield","turnId":"turn-partial-yield","item":{"id":"partial-final","type":"agentMessage","phase":"final_answer","text":""}}}')
        [Console]::Out.WriteLine('{"method":"item/agentMessage/delta","params":{"threadId":"thread-partial-yield","turnId":"turn-partial-yield","itemId":"partial-final","delta":"Checkpointing before yield. <agent-harness-drain-disposition>{\"schema\":\"agent-harness.drain-disposition.v1\",\"disposition\":\"continuation-required\""}}')
        [Console]::Out.Flush()
        Start-Sleep -Milliseconds 1200
    }
    [Console]::Out.Flush()
}
"#,
        )
        .unwrap();
        let cmd = root.join("fake-partial-final-bounded-yield-codex.cmd");
        fs::write(
            &cmd,
            format!(
                "@echo off\r\npowershell -NoProfile -ExecutionPolicy Bypass -File \"{}\"\r\n",
                script.display()
            ),
        )
        .unwrap();
        cmd
    }

    #[cfg(windows)]
    fn fake_missing_disposition_deadline_codex_executable(root: &Path) -> PathBuf {
        let script = root.join("fake-missing-disposition-deadline-app-server.ps1");
        fs::write(
            &script,
            r#"
while ($true) {
    $line = [Console]::In.ReadLine()
    if ($null -eq $line) { break }
    try { $msg = $line | ConvertFrom-Json } catch { continue }
    if ($msg.id -eq 0) {
        [Console]::Out.WriteLine('{"id":0,"result":{"ok":true}}')
    } elseif ($msg.method -eq 'modelProvider/capabilities/read') {
        [Console]::Out.WriteLine('{"id":8900,"result":{"namespaceTools":true,"imageGeneration":true,"webSearch":true}}')
    } elseif ($msg.method -eq 'thread/start') {
        [Console]::Out.WriteLine('{"id":1,"result":{"thread":{"id":"thread-missing-disposition"}}}')
    } elseif ($msg.method -eq 'turn/start') {
        [Console]::Out.WriteLine('{"method":"turn/started","params":{"threadId":"thread-missing-disposition","turn":{"id":"turn-missing-disposition","kind":"regular"}}}')
    } elseif ($msg.method -eq 'turn/steer') {
        [Console]::Out.WriteLine((@{id=$msg.id;result=@{turnId='turn-missing-disposition'}} | ConvertTo-Json -Compress))
        [Console]::Out.WriteLine('{"method":"item/completed","params":{"threadId":"thread-missing-disposition","turnId":"turn-missing-disposition","item":{"id":"yield-prompt","type":"userMessage","text":"Runtime bounded-yield guard: observed"}}}')
        [Console]::Out.WriteLine('{"method":"item/completed","params":{"threadId":"thread-missing-disposition","turnId":"turn-missing-disposition","item":{"id":"yield-final","type":"agentMessage","phase":"final_answer","text":"The task outcome could not be classified."}}}')
        [Console]::Out.WriteLine('{"method":"turn/completed","params":{"threadId":"thread-missing-disposition","turn":{"id":"turn-missing-disposition","status":"completed"}}}')
        [Console]::Out.Flush()
        break
    }
    [Console]::Out.Flush()
}
"#,
        )
        .unwrap();
        let cmd = root.join("fake-missing-disposition-deadline-codex.cmd");
        fs::write(
            &cmd,
            format!(
                "@echo off\r\npowershell -NoProfile -ExecutionPolicy Bypass -File \"{}\"\r\n",
                script.display()
            ),
        )
        .unwrap();
        cmd
    }

    #[cfg(windows)]
    fn fake_missing_disposition_recovery_codex_executable(root: &Path) -> PathBuf {
        let script = root.join("fake-missing-disposition-recovery-app-server.ps1");
        fs::write(
            &script,
            r#"
while ($true) {
    $line = [Console]::In.ReadLine()
    if ($null -eq $line) { break }
    try { $msg = $line | ConvertFrom-Json } catch { continue }
    if ($msg.id -eq 0) {
        [Console]::Out.WriteLine('{"id":0,"result":{"ok":true}}')
    } elseif ($msg.method -eq 'modelProvider/capabilities/read') {
        [Console]::Out.WriteLine('{"id":8900,"result":{"namespaceTools":true,"imageGeneration":true,"webSearch":true}}')
    } elseif ($msg.method -eq 'thread/start' -or $msg.method -eq 'thread/resume') {
        [Console]::Out.WriteLine((@{id=$msg.id;result=@{thread=@{id='thread-disposition-recovery'}}} | ConvertTo-Json -Compress -Depth 8))
    } elseif ($msg.method -eq 'turn/start') {
        [Console]::Out.WriteLine('{"method":"turn/started","params":{"threadId":"thread-disposition-recovery","turn":{"id":"turn-disposition-recovery","kind":"regular"}}}')
        [Console]::Out.WriteLine('{"method":"item/completed","params":{"threadId":"thread-disposition-recovery","turnId":"turn-disposition-recovery","item":{"id":"recovery-final","type":"agentMessage","phase":"final_answer","text":"The bounded recovery pass still could not classify the outcome."}}}')
        [Console]::Out.WriteLine('{"method":"turn/completed","params":{"threadId":"thread-disposition-recovery","turn":{"id":"turn-disposition-recovery","status":"completed"}}}')
        [Console]::Out.Flush()
        break
    }
    [Console]::Out.Flush()
}
"#,
        )
        .unwrap();
        let cmd = root.join("fake-missing-disposition-recovery-codex.cmd");
        fs::write(
            &cmd,
            format!(
                "@echo off\r\npowershell -NoProfile -ExecutionPolicy Bypass -File \"{}\"\r\n",
                script.display()
            ),
        )
        .unwrap();
        cmd
    }

    #[cfg(windows)]
    fn fake_terminal_goal_codex_executable(
        root: &Path,
        status: &str,
        assistant_text: &str,
    ) -> PathBuf {
        let status = serde_json::to_string(status).unwrap();
        let assistant_text = serde_json::to_string(assistant_text).unwrap();
        let script = root.join("fake-terminal-goal-app-server.ps1");
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
        [Console]::Out.WriteLine('{{"id":1,"result":{{"thread":{{"id":"thread-terminal-goal"}}}}}}')
    }} elseif ($msg.method -eq 'turn/start') {{
        [Console]::Out.WriteLine('{{"method":"turn/started","params":{{"threadId":"thread-terminal-goal","turn":{{"id":"turn-terminal-goal","kind":"regular"}}}}}}')
        [Console]::Out.WriteLine('{{"method":"thread/goal/updated","params":{{"threadId":"thread-terminal-goal","turnId":"turn-terminal-goal","goal":{{"id":"goal-terminal","objective":"finish the durable campaign","status":{status},"completionCriteria":["T3 replay passes"]}}}}}}')
        [Console]::Out.WriteLine('{{"method":"item/agentMessage/delta","params":{{"threadId":"thread-terminal-goal","turnId":"turn-terminal-goal","itemId":"msg-terminal-goal","delta":{assistant_text}}}}}')
        [Console]::Out.WriteLine('{{"method":"turn/completed","params":{{"threadId":"thread-terminal-goal","turn":{{"id":"turn-terminal-goal","status":"completed"}}}}}}')
        [Console]::Out.Flush()
        break
    }}
    [Console]::Out.Flush()
}}
"#,
            ),
        )
        .unwrap();
        let cmd = root.join("fake-terminal-goal-codex.cmd");
        fs::write(
            &cmd,
            format!(
                "@echo off\r\npowershell -NoProfile -ExecutionPolicy Bypass -File \"{}\"\r\n",
                script.display()
            ),
        )
        .unwrap();
        cmd
    }

    #[cfg(windows)]
    fn fake_interrupted_command_codex_executable(
        root: &Path,
        cancel_file: &Path,
        cwd: &Path,
    ) -> PathBuf {
        let script = root.join("fake-interrupted-command-app-server.ps1");
        let cancel_file = cancel_file.display().to_string().replace('\'', "''");
        let cwd = cwd.display().to_string().replace('\'', "''");
        fs::write(
            &script,
            format!(
                r#"
$cancelFile = '{cancel_file}'
$cwd = '{cwd}'
while ($true) {{
    $line = [Console]::In.ReadLine()
    if ($null -eq $line) {{ break }}
    try {{
        $msg = $line | ConvertFrom-Json
    }} catch {{
        continue
    }}
    if ($msg.id -eq 0) {{
        [Console]::Out.WriteLine('{{"id":0,"result":{{"ok":true}}}}')
        [Console]::Out.Flush()
    }} elseif ($msg.method -eq 'modelProvider/capabilities/read') {{
        [Console]::Out.WriteLine('{{"id":8900,"result":{{"namespaceTools":true,"imageGeneration":true,"webSearch":true}}}}')
        [Console]::Out.Flush()
    }} elseif ($msg.method -eq 'thread/start' -or $msg.method -eq 'thread/resume') {{
        [Console]::Out.WriteLine('{{"id":1,"result":{{"thread":{{"id":"thread-interrupted-pipeline"}}}}}}')
        [Console]::Out.Flush()
    }} elseif ($msg.method -eq 'turn/start') {{
        [Console]::Out.WriteLine('{{"method":"turn/started","params":{{"threadId":"thread-interrupted-pipeline","turn":{{"id":"turn-interrupted","kind":"regular"}}}}}}')
        [Console]::Out.WriteLine(('{{"method":"item/started","params":{{"item":{{"type":"commandExecution","id":"cmd-interrupted","command":"cargo test -p agent-harness-core","cwd":"' + ($cwd -replace '\\','\\') + '"}},"threadId":"thread-interrupted-pipeline","turnId":"turn-interrupted"}}}}'))
        [Console]::Out.Flush()
        New-Item -ItemType Directory -Force -Path (Split-Path -Parent $cancelFile) | Out-Null
        $payload = '{{"schema":"agent-harness.runtime-cancel-request.v1","atMs":' + ([DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()) + ',"sessionKey":"telegram:dm-42:user-7:main","reason":"new turn arrived while validation command was running"}}'
        Set-Content -LiteralPath $cancelFile -Value $payload
        Start-Sleep -Seconds 10
    }}
}}
"#
            ),
        )
        .unwrap();
        let cmd = root.join("fake-interrupted-codex.cmd");
        fs::write(
            &cmd,
            format!(
                "@echo off\r\npowershell -NoProfile -ExecutionPolicy Bypass -File \"{}\"\r\n",
                script.display()
            ),
        )
        .unwrap();
        cmd
    }

    fn test_normalize_key_part(value: &str) -> String {
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

    #[cfg(windows)]
    fn fake_media_final_codex_executable(root: &Path, media_path: &Path) -> PathBuf {
        fs::write(
            root.join("media-path.txt"),
            media_path.display().to_string(),
        )
        .unwrap();
        let script = root.join("fake-media-final-app-server.ps1");
        fs::write(
            &script,
            r#"
$media = (Get-Content -LiteralPath (Join-Path $PSScriptRoot 'media-path.txt') -Raw).Trim()
while ($true) {
    $line = [Console]::In.ReadLine()
    if ($null -eq $line) { break }
    try {
        $msg = $line | ConvertFrom-Json
    } catch {
        continue
    }
    if ($msg.id -eq 0) {
        [Console]::Out.WriteLine('{"id":0,"result":{"ok":true}}')
        [Console]::Out.Flush()
    } elseif ($msg.method -eq 'thread/start') {
        [Console]::Out.WriteLine('{"id":1,"result":{"thread":{"id":"thread-media-final"}}}')
        [Console]::Out.Flush()
    } elseif ($msg.method -eq 'turn/start') {
        [Console]::Out.WriteLine('{"method":"turn/started","params":{"threadId":"thread-media-final","turn":{"id":"turn-media-final","kind":"regular"}}}')
        $delta = "Here is the file.`nMEDIA:$media`nDone."
        $event = @{
            method = 'item/agentMessage/delta'
            params = @{ threadId = 'thread-media-final'; turnId = 'turn-media-final'; itemId = 'msg-media-final'; delta = $delta }
        } | ConvertTo-Json -Compress
        [Console]::Out.WriteLine($event)
        [Console]::Out.WriteLine('{"method":"turn/completed","params":{"threadId":"thread-media-final","turn":{"id":"turn-media-final","status":"completed"}}}')
        [Console]::Out.Flush()
        break
    }
}
"#,
        )
        .unwrap();
        let cmd = root.join("fake-media-final-codex.cmd");
        fs::write(
            &cmd,
            format!(
                "@echo off\r\npowershell -NoProfile -ExecutionPolicy Bypass -File \"{}\"\r\n",
                script.display()
            ),
        )
        .unwrap();
        cmd
    }

    #[cfg(windows)]
    fn fake_external_review_only_codex_executable(root: &Path) -> PathBuf {
        let script = root.join("fake-external-review-only-app-server.cmd");
        fs::write(
            &script,
            r#"@echo off
setlocal
set "countFile=%~dp0external-review-only-attempt.txt"
set "attempt=0"
if exist "%countFile%" set /p attempt=<"%countFile%"
set /a next=attempt+1
>"%countFile%" echo %next%
set /p "request="
echo {"id":0,"result":{"ok":true}}
set /p "request="
set /p "request="
echo {"id":8900,"result":{"namespaceTools":true,"imageGeneration":true,"webSearch":true}}
set /p "request="
echo {"id":1,"result":{"thread":{"id":"thread-review-evidence"}}}
set /p "request="
if "%attempt%"=="0" (
    echo {"method":"turn/started","params":{"threadId":"thread-review-evidence","turn":{"id":"turn-review-timeout","kind":"regular"}}}
    echo {"method":"item/started","params":{"item":{"type":"commandExecution","id":"cmd-review-timeout","command":"claude -p review prompt"},"threadId":"thread-review-evidence","turnId":"turn-review-timeout"}}
    %SystemRoot%\System32\ping.exe -n 11 127.0.0.1 >nul
) else (
    echo {"method":"turn/started","params":{"threadId":"thread-review-evidence","turn":{"id":"turn-review-only","kind":"regular"}}}
    echo {"method":"item/completed","params":{"item":{"type":"agentMessage","id":"msg-review-only","text":"Claude second brain review: PASS. Findings only; implementation still needs to continue.","phase":"final_answer"},"threadId":"thread-review-evidence","turnId":"turn-review-only","completedAtMs":1234}}
    echo {"method":"turn/completed","params":{"threadId":"thread-review-evidence","turn":{"id":"turn-review-only","status":"completed","usage":{"inputTokens":30,"outputTokens":12,"totalTokens":42}}}}
    %SystemRoot%\System32\ping.exe -n 4 127.0.0.1 >nul
)
"#,
        )
        .unwrap();
        script
    }

    #[cfg(not(windows))]
    fn fake_productive_deadline_renewal_codex_executable(root: &Path) -> PathBuf {
        let script = root.join("fake-productive-deadline-renewal-codex");
        fs::write(
            &script,
            r#"#!/bin/sh
while IFS= read -r line; do
    case "$line" in
        *'"id":0'*) printf '%s\n' '{"id":0,"result":{"ok":true}}' ;;
        *'"method":"modelProvider/capabilities/read"'*) printf '%s\n' '{"id":8900,"result":{"namespaceTools":true,"imageGeneration":true,"webSearch":true}}' ;;
        *'"method":"thread/start"'*) printf '%s\n' '{"id":1,"result":{"thread":{"id":"thread-productive"}}}' ;;
        *'"method":"turn/start"'*)
            printf '%s\n' '{"method":"turn/started","params":{"threadId":"thread-productive","turn":{"id":"turn-productive","kind":"regular"}}}'
            printf '%s\n' '{"method":"thread/goal/updated","params":{"threadId":"thread-productive","turnId":"turn-productive","goal":{"id":"goal-telemetry-only","objective":"track productive work","status":"active","completionCriteria":["finish"]}}}'
            sleep 13.8
            printf '%s\n' '{"method":"item/completed","params":{"threadId":"thread-productive","turnId":"turn-productive","item":{"id":"cmd-productive","type":"commandExecution","status":"completed"}}}'
            sleep 4.5
            printf '%s\n' '{"method":"thread/goal/updated","params":{"threadId":"thread-productive","turnId":"turn-productive","goal":{"id":"goal-telemetry-only","objective":"track productive work","status":"completed","completionCriteria":["finish"]}}}'
            printf '%s\n' '{"method":"item/agentMessage/delta","params":{"threadId":"thread-productive","turnId":"turn-productive","delta":"Productive task completed after the original deadline."}}'
            printf '%s\n' '{"method":"turn/completed","params":{"threadId":"thread-productive","turn":{"id":"turn-productive","status":"completed"}}}'
            exit 0 ;;
    esac
done
"#,
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = fs::metadata(&script).unwrap().permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&script, permissions).unwrap();
        }
        script
    }

    #[cfg(not(windows))]
    fn fake_explicit_checkpoint_deadline_codex_executable(root: &Path) -> PathBuf {
        let checkpoint = "ordinary task remains incomplete";
        let checkpoint_digest = crate::task_transition::sha256_hex(checkpoint.as_bytes());
        let script = root.join("fake-explicit-checkpoint-deadline-codex");
        fs::write(
            &script,
            format!(
                r#"#!/bin/sh
while IFS= read -r line; do
    case "$line" in
        *'"id":0'*) printf '%s\n' '{{"id":0,"result":{{"ok":true}}}}' ;;
        *'"method":"modelProvider/capabilities/read"'*) printf '%s\n' '{{"id":8900,"result":{{"namespaceTools":true,"imageGeneration":true,"webSearch":true}}}}' ;;
        *'"method":"thread/start"'*) printf '%s\n' '{{"id":1,"result":{{"thread":{{"id":"thread-explicit-task"}}}}}}' ;;
        *'"method":"turn/start"'*) printf '%s\n' '{{"method":"turn/started","params":{{"threadId":"thread-explicit-task","turn":{{"id":"turn-explicit-task","kind":"regular"}}}}}}' ;;
        *'"method":"turn/steer"'*)
            family=$(printf '%s' "$line" | sed -n 's/.*authorityId \([0-9a-f]\{{64\}}\), authorityVersion.*/\1/p')
            generation=$(printf '%s' "$line" | sed -n 's/.*observedDeadlineGeneration to \([0-9][0-9]*\).*/\1/p')
            rpc_id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
            [ -n "$family" ] && [ -n "$generation" ] || exit 23
            printf '%s\n' "{{\"id\":${{rpc_id}},\"result\":{{\"turnId\":\"turn-explicit-task\"}}}}"
            printf '%s\n' '{{"method":"item/completed","params":{{"threadId":"thread-explicit-task","turnId":"turn-explicit-task","item":{{"id":"yield-prompt","type":"userMessage","text":"Runtime bounded-yield guard: observed"}}}}}}'
            printf '%s\n' "{{\"method\":\"item/completed\",\"params\":{{\"threadId\":\"thread-explicit-task\",\"turnId\":\"turn-explicit-task\",\"item\":{{\"id\":\"yield-final\",\"type\":\"agentMessage\",\"phase\":\"final_answer\",\"text\":\"Ordinary work is checkpointed.\\n<agent-harness-drain-disposition>{{\\\"schema\\\":\\\"agent-harness.drain-disposition.v1\\\",\\\"disposition\\\":\\\"continuation-required\\\",\\\"observedDeadlineGeneration\\\":$generation,\\\"authorityKind\\\":\\\"explicit-checkpoint\\\",\\\"authorityId\\\":\\\"$family\\\",\\\"authorityVersion\\\":1,\\\"taskFamilyId\\\":\\\"$family\\\",\\\"taskFamilyVersion\\\":1,\\\"activeItemId\\\":null,\\\"activeItemVersion\\\":null,\\\"checkpoint\\\":\\\"{checkpoint}\\\",\\\"checkpointDigest\\\":\\\"{checkpoint_digest}\\\",\\\"completionEvidenceDigests\\\":[]}}</agent-harness-drain-disposition>\"}}}}}}"
            printf '%s\n' '{{"method":"turn/completed","params":{{"threadId":"thread-explicit-task","turn":{{"id":"turn-explicit-task","status":"completed"}}}}}}'
            exit 0 ;;
    esac
done
"#,
            ),
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = fs::metadata(&script).unwrap().permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&script, permissions).unwrap();
        }
        script
    }

    #[cfg(windows)]
    fn fake_read_only_review_final_codex_executable(root: &Path) -> PathBuf {
        let script = root.join("fake-read-only-review-final-app-server.ps1");
        fs::write(
            &script,
            r#"
while ($true) {
    $line = [Console]::In.ReadLine()
    if ($null -eq $line) { break }
    try {
        $msg = $line | ConvertFrom-Json
    } catch {
        continue
    }
    if ($msg.id -eq 0) {
        [Console]::Out.WriteLine('{"id":0,"result":{"ok":true}}')
        [Console]::Out.Flush()
    } elseif ($msg.method -eq 'thread/start') {
        [Console]::Out.WriteLine('{"id":1,"result":{"thread":{"id":"thread-read-only-review-final"}}}')
        [Console]::Out.Flush()
    } elseif ($msg.method -eq 'turn/start') {
        [Console]::Out.WriteLine('{"method":"turn/started","params":{"threadId":"thread-read-only-review-final","turn":{"id":"turn-read-only-review-final","kind":"regular"}}}')
        [Console]::Out.WriteLine('{"method":"item/agentMessage/delta","params":{"threadId":"thread-read-only-review-final","turnId":"turn-read-only-review-final","itemId":"msg-read-only-review-final","delta":"Read-only inspection only. No files changed, no tests run.\n\n- Final/outbox authority seam: runtime_pipeline owns final decisions.\n- Dirty Worktree Risks: implementation still needs to continue."}}')
        [Console]::Out.WriteLine('{"method":"turn/completed","params":{"threadId":"thread-read-only-review-final","turn":{"id":"turn-read-only-review-final","status":"completed"}}}')
        [Console]::Out.Flush()
        break
    }
}
"#,
        )
        .unwrap();
        let cmd = root.join("fake-read-only-review-final-codex.cmd");
        fs::write(
            &cmd,
            format!(
                "@echo off\r\npowershell -NoProfile -ExecutionPolicy Bypass -File \"{}\"\r\n",
                script.display()
            ),
        )
        .unwrap();
        cmd
    }

    #[cfg(windows)]
    fn fake_failing_codex_executable(root: &Path) -> PathBuf {
        let script = root.join("fake-failing-app-server.ps1");
        fs::write(
            &script,
            r#"
while ($true) {
    $line = [Console]::In.ReadLine()
    if ($null -eq $line) { break }
    try {
        $msg = $line | ConvertFrom-Json
    } catch {
        continue
    }
    if ($msg.id -eq 0) {
        [Console]::Out.WriteLine('{"id":0,"result":{"ok":true}}')
        [Console]::Out.Flush()
    } elseif ($msg.method -eq 'thread/start') {
        [Console]::Out.WriteLine('{"id":1,"error":{"message":"synthetic app-server refusal"}}')
        [Console]::Out.Flush()
        break
    }
}
"#,
        )
        .unwrap();
        let cmd = root.join("fake-failing-codex.cmd");
        fs::write(
            &cmd,
            format!(
                "@echo off\r\npowershell -NoProfile -ExecutionPolicy Bypass -File \"{}\"\r\n",
                script.display()
            ),
        )
        .unwrap();
        cmd
    }

    #[cfg(windows)]
    fn fake_reconnecting_codex_executable(root: &Path) -> PathBuf {
        let script = root.join("fake-reconnecting-app-server.ps1");
        fs::write(
            &script,
            r#"
while ($true) {
    $line = [Console]::In.ReadLine()
    if ($null -eq $line) { break }
    try {
        $msg = $line | ConvertFrom-Json
    } catch {
        continue
    }
    if ($msg.id -eq 0) {
        [Console]::Out.WriteLine('{"id":0,"result":{"ok":true}}')
        [Console]::Out.Flush()
    } elseif ($msg.method -eq 'thread/start') {
        [Console]::Out.WriteLine('{"id":1,"result":{"thread":{"id":"thread-reconnect"}}}')
        [Console]::Out.Flush()
    } elseif ($msg.method -eq 'turn/start') {
        [Console]::Out.WriteLine('{"method":"error","params":{"message":"Reconnecting... 2/5","additionalDetails":"stream disconnected before completion: websocket closed by server before response.completed"}}')
        [Console]::Out.Flush()
        break
    }
}
"#,
        )
        .unwrap();
        let cmd = root.join("fake-reconnecting-codex.cmd");
        fs::write(
            &cmd,
            format!(
                "@echo off\r\npowershell -NoProfile -ExecutionPolicy Bypass -File \"{}\"\r\n",
                script.display()
            ),
        )
        .unwrap();
        cmd
    }

    #[cfg(windows)]
    fn fake_high_usage_reconnecting_codex_executable(root: &Path) -> PathBuf {
        let script = root.join("fake-high-usage-reconnecting-app-server.ps1");
        fs::write(
            &script,
            r#"
while ($true) {
    $line = [Console]::In.ReadLine()
    if ($null -eq $line) { break }
    try {
        $msg = $line | ConvertFrom-Json
    } catch {
        continue
    }
    if ($msg.id -eq 0) {
        [Console]::Out.WriteLine('{"id":0,"result":{"ok":true}}')
        [Console]::Out.Flush()
    } elseif ($msg.method -eq 'thread/start') {
        [Console]::Out.WriteLine('{"id":1,"result":{"thread":{"id":"thread-high-usage-reconnect"}}}')
        [Console]::Out.Flush()
    } elseif ($msg.method -eq 'turn/start') {
        [Console]::Out.WriteLine('{"method":"thread/tokenUsage/updated","params":{"tokenUsage":{"input":90038,"output":2513,"total":92551}}}')
        [Console]::Out.WriteLine('{"method":"error","params":{"message":"Reconnecting... 2/5","additionalDetails":"stream disconnected before completion: websocket closed by server before response.completed"}}')
        [Console]::Out.Flush()
        break
    }
}
"#,
        )
        .unwrap();
        let cmd = root.join("fake-high-usage-reconnecting-codex.cmd");
        fs::write(
            &cmd,
            format!(
                "@echo off\r\npowershell -NoProfile -ExecutionPolicy Bypass -File \"{}\"\r\n",
                script.display()
            ),
        )
        .unwrap();
        cmd
    }

    #[cfg(not(windows))]
    fn fake_codex_executable(root: &Path) -> PathBuf {
        let script = root.join("fake-codex");
        fs::write(
            &script,
            r#"#!/bin/sh
while IFS= read -r line; do
    case "$line" in
        *'"id":0'*)
            printf '%s\n' '{"id":0,"result":{"ok":true}}'
            ;;
        *'"method":"thread/start"'*)
            printf '%s\n' '{"id":1,"result":{"thread":{"id":"thread-pipeline"}}}'
            ;;
        *'"method":"turn/start"'*)
            printf '%s\n' '{"method":"turn/started","params":{"threadId":"thread-pipeline","turn":{"id":"turn-pipeline","kind":"regular"}}}'
            printf '%s\n' '{"method":"item/agentMessage/delta","params":{"threadId":"thread-pipeline","turnId":"turn-pipeline","itemId":"msg-pipeline","delta":"Pipeline fake reply."}}'
            printf '%s\n' '{"method":"turn/completed","params":{"threadId":"thread-pipeline","turn":{"id":"turn-pipeline","status":"completed"}}}'
            exit 0
            ;;
    esac
done
"#,
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = fs::metadata(&script).unwrap().permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&script, permissions).unwrap();
        }
        script
    }

    #[cfg(not(windows))]
    fn fake_active_goal_codex_executable(root: &Path) -> PathBuf {
        let script = root.join("fake-active-goal-codex");
        fs::write(
            &script,
            r#"#!/bin/sh
while IFS= read -r line; do
    case "$line" in
        *'"id":0'*)
            printf '%s\n' '{"id":0,"result":{"ok":true}}'
            ;;
        *'"method":"thread/start"'*)
            printf '%s\n' '{"id":1,"result":{"thread":{"id":"thread-active-goal"}}}'
            ;;
        *'"method":"turn/start"'*)
            printf '%s\n' '{"method":"turn/started","params":{"threadId":"thread-active-goal","turn":{"id":"turn-active-goal","kind":"regular"}}}'
            printf '%s\n' '{"method":"thread/goal/updated","params":{"threadId":"thread-active-goal","turnId":"turn-active-goal","goal":{"id":"goal-active","objective":"finish the durable campaign","status":"active","completionCriteria":["T3 replay passes"]}}}'
            printf '%s\n' '{"method":"item/agentMessage/delta","params":{"threadId":"thread-active-goal","turnId":"turn-active-goal","itemId":"msg-active-goal","delta":"Slice checkpoint only."}}'
            printf '%s\n' '{"method":"turn/completed","params":{"threadId":"thread-active-goal","turn":{"id":"turn-active-goal","status":"completed"}}}'
            exit 0
            ;;
    esac
done
"#,
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = fs::metadata(&script).unwrap().permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&script, permissions).unwrap();
        }
        script
    }

    #[cfg(not(windows))]
    fn fake_deadline_drain_checkpoint_codex_executable(
        root: &Path,
        assistant_text: &str,
    ) -> PathBuf {
        let assistant_text = serde_json::to_string(assistant_text).unwrap();
        let script = root.join("fake-deadline-drain-checkpoint-codex");
        fs::write(
            &script,
            format!(
                r#"#!/bin/sh
while IFS= read -r line; do
    case "$line" in
        *'"id":0'*)
            printf '%s\n' '{{"id":0,"result":{{"ok":true}}}}'
            ;;
        *'"method":"modelProvider/capabilities/read"'*)
            printf '%s\n' '{{"id":8900,"result":{{"namespaceTools":true,"imageGeneration":true,"webSearch":true}}}}'
            ;;
        *'"method":"thread/start"'*)
            printf '%s\n' '{{"id":1,"result":{{"thread":{{"id":"thread-deadline-plan"}}}}}}'
            ;;
        *'"method":"turn/start"'*)
            printf '%s\n' '{{"method":"turn/started","params":{{"threadId":"thread-deadline-plan","turn":{{"id":"turn-deadline-plan","kind":"regular"}}}}}}'
            ;;
        *'"method":"turn/steer"'*)
            rpc_id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
            printf '%s\n' "{{\"id\":${{rpc_id}},\"result\":{{\"turnId\":\"turn-deadline-plan\"}}}}"
            printf '%s\n' '{{"method":"item/completed","params":{{"threadId":"thread-deadline-plan","turnId":"turn-deadline-plan","item":{{"id":"yield-prompt","type":"userMessage","text":"Runtime bounded-yield guard: observed"}}}}}}'
            printf '%s\n' '{{"method":"item/completed","params":{{"threadId":"thread-deadline-plan","turnId":"turn-deadline-plan","item":{{"id":"yield-final","type":"agentMessage","phase":"final_answer","text":{assistant_text}}}}}}}'
            printf '%s\n' '{{"method":"turn/completed","params":{{"threadId":"thread-deadline-plan","turn":{{"id":"turn-deadline-plan","status":"completed"}}}}}}'
            exit 0
            ;;
    esac
done
"#,
            ),
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = fs::metadata(&script).unwrap().permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&script, permissions).unwrap();
        }
        script
    }

    #[cfg(not(windows))]
    fn fake_terminal_goal_codex_executable(
        root: &Path,
        status: &str,
        assistant_text: &str,
    ) -> PathBuf {
        let status = serde_json::to_string(status).unwrap();
        let assistant_text = serde_json::to_string(assistant_text).unwrap();
        let script = root.join("fake-terminal-goal-codex");
        fs::write(
            &script,
            format!(
                r#"#!/bin/sh
while IFS= read -r line; do
    case "$line" in
        *'"id":0'*)
            printf '%s\n' '{{"id":0,"result":{{"ok":true}}}}'
            ;;
        *'"method":"thread/start"'*)
            printf '%s\n' '{{"id":1,"result":{{"thread":{{"id":"thread-terminal-goal"}}}}}}'
            ;;
        *'"method":"turn/start"'*)
            printf '%s\n' '{{"method":"turn/started","params":{{"threadId":"thread-terminal-goal","turn":{{"id":"turn-terminal-goal","kind":"regular"}}}}}}'
            printf '%s\n' '{{"method":"thread/goal/updated","params":{{"threadId":"thread-terminal-goal","turnId":"turn-terminal-goal","goal":{{"id":"goal-terminal","objective":"finish the durable campaign","status":{status},"completionCriteria":["T3 replay passes"]}}}}}}'
            printf '%s\n' '{{"method":"item/agentMessage/delta","params":{{"threadId":"thread-terminal-goal","turnId":"turn-terminal-goal","itemId":"msg-terminal-goal","delta":{assistant_text}}}}}'
            printf '%s\n' '{{"method":"turn/completed","params":{{"threadId":"thread-terminal-goal","turn":{{"id":"turn-terminal-goal","status":"completed"}}}}}}'
            exit 0
            ;;
    esac
done
"#,
            ),
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = fs::metadata(&script).unwrap().permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&script, permissions).unwrap();
        }
        script
    }

    #[cfg(not(windows))]
    fn fake_media_final_codex_executable(root: &Path, media_path: &Path) -> PathBuf {
        fs::write(
            root.join("media-path.txt"),
            media_path.display().to_string(),
        )
        .unwrap();
        let script = root.join("fake-media-final-codex");
        fs::write(
            &script,
            r#"#!/bin/sh
media="$(cat "$(dirname "$0")/media-path.txt")"
while IFS= read -r line; do
    case "$line" in
        *'"id":0'*)
            printf '%s\n' '{"id":0,"result":{"ok":true}}'
            ;;
        *'"method":"thread/start"'*)
            printf '%s\n' '{"id":1,"result":{"thread":{"id":"thread-media-final"}}}'
            ;;
        *'"method":"turn/start"'*)
            printf '%s\n' '{"method":"turn/started","params":{"threadId":"thread-media-final","turn":{"id":"turn-media-final","kind":"regular"}}}'
            printf '%s\n' "{\"method\":\"item/agentMessage/delta\",\"params\":{\"threadId\":\"thread-media-final\",\"turnId\":\"turn-media-final\",\"itemId\":\"msg-media-final\",\"delta\":\"Here is the file.\\nMEDIA:$media\\nDone.\"}}"
            printf '%s\n' '{"method":"turn/completed","params":{"threadId":"thread-media-final","turn":{"id":"turn-media-final","status":"completed"}}}'
            exit 0
            ;;
    esac
done
"#,
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = fs::metadata(&script).unwrap().permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&script, permissions).unwrap();
        }
        script
    }

    #[cfg(not(windows))]
    fn fake_failing_codex_executable(root: &Path) -> PathBuf {
        let script = root.join("fake-failing-codex");
        fs::write(
            &script,
            r#"#!/bin/sh
while IFS= read -r line; do
    case "$line" in
        *'"id":0'*)
            printf '%s\n' '{"id":0,"result":{"ok":true}}'
            ;;
        *'"method":"thread/start"'*)
            printf '%s\n' '{"id":1,"error":{"message":"synthetic app-server refusal"}}'
            exit 0
            ;;
    esac
done
"#,
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = fs::metadata(&script).unwrap().permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&script, permissions).unwrap();
        }
        script
    }

    #[cfg(not(windows))]
    fn fake_reconnecting_codex_executable(root: &Path) -> PathBuf {
        let script = root.join("fake-reconnecting-codex");
        fs::write(
            &script,
            r#"#!/bin/sh
while IFS= read -r line; do
    case "$line" in
        *'"id":0'*)
            printf '%s\n' '{"id":0,"result":{"ok":true}}'
            ;;
        *'"method":"thread/start"'*)
            printf '%s\n' '{"id":1,"result":{"thread":{"id":"thread-reconnect"}}}'
            ;;
        *'"method":"turn/start"'*)
            printf '%s\n' '{"method":"error","params":{"message":"Reconnecting... 2/5","additionalDetails":"stream disconnected before completion: websocket closed by server before response.completed"}}'
            exit 0
            ;;
    esac
done
"#,
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = fs::metadata(&script).unwrap().permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&script, permissions).unwrap();
        }
        script
    }

    #[cfg(not(windows))]
    fn fake_high_usage_reconnecting_codex_executable(root: &Path) -> PathBuf {
        let script = root.join("fake-high-usage-reconnecting-codex");
        fs::write(
            &script,
            r#"#!/bin/sh
while IFS= read -r line; do
    case "$line" in
        *'"id":0'*)
            printf '%s\n' '{"id":0,"result":{"ok":true}}'
            ;;
        *'"method":"thread/start"'*)
            printf '%s\n' '{"id":1,"result":{"thread":{"id":"thread-high-usage-reconnect"}}}'
            ;;
        *'"method":"turn/start"'*)
            printf '%s\n' '{"method":"thread/tokenUsage/updated","params":{"tokenUsage":{"input":90038,"output":2513,"total":92551}}}'
            printf '%s\n' '{"method":"error","params":{"message":"Reconnecting... 2/5","additionalDetails":"stream disconnected before completion: websocket closed by server before response.completed"}}'
            exit 0
            ;;
    esac
done
"#,
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = fs::metadata(&script).unwrap().permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&script, permissions).unwrap();
        }
        script
    }

    #[cfg(not(windows))]
    fn fake_external_review_only_codex_executable(root: &Path) -> PathBuf {
        let script = root.join("fake-external-review-only-codex");
        fs::write(
            &script,
            r#"#!/bin/sh
count_file="$(dirname "$0")/external-review-only-attempt.txt"
attempt=0
if [ -f "$count_file" ]; then
    attempt="$(cat "$count_file")"
fi
next=$((attempt + 1))
printf '%s\n' "$next" > "$count_file"
while IFS= read -r line; do
    case "$line" in
        *'"id":0'*)
            printf '%s\n' '{"id":0,"result":{"ok":true}}'
            ;;
        *'"method":"thread/start"'*|*'"method":"thread/resume"'*)
            printf '%s\n' '{"id":1,"result":{"thread":{"id":"thread-review-evidence"}}}'
            ;;
        *'"method":"turn/start"'*)
            if [ "$attempt" = "0" ]; then
                printf '%s\n' '{"method":"turn/started","params":{"threadId":"thread-review-evidence","turn":{"id":"turn-review-timeout","kind":"regular"}}}'
                printf '%s\n' '{"method":"item/started","params":{"item":{"type":"commandExecution","id":"cmd-review-timeout","command":"claude -p review prompt"},"threadId":"thread-review-evidence","turnId":"turn-review-timeout"}}'
                sleep 10
            else
                printf '%s\n' '{"method":"turn/started","params":{"threadId":"thread-review-evidence","turn":{"id":"turn-review-only","kind":"regular"}}}'
                printf '%s\n' '{"method":"item/completed","params":{"item":{"type":"agentMessage","id":"msg-review-only","text":"Claude second brain review: PASS. Findings only; implementation still needs to continue.","phase":"final_answer"},"threadId":"thread-review-evidence","turnId":"turn-review-only","completedAtMs":1234}}'
                printf '%s\n' '{"method":"turn/completed","params":{"threadId":"thread-review-evidence","turn":{"id":"turn-review-only","status":"completed","usage":{"inputTokens":30,"outputTokens":12,"totalTokens":42}}}}'
                exit 0
            fi
            ;;
    esac
done
"#,
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = fs::metadata(&script).unwrap().permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&script, permissions).unwrap();
        }
        script
    }

    #[cfg(not(windows))]
    fn fake_read_only_review_final_codex_executable(root: &Path) -> PathBuf {
        let script = root.join("fake-read-only-review-final-codex");
        fs::write(
            &script,
            r#"#!/bin/sh
while IFS= read -r line; do
    case "$line" in
        *'"id":0'*)
            printf '%s\n' '{"id":0,"result":{"ok":true}}'
            ;;
        *'"method":"thread/start"'*)
            printf '%s\n' '{"id":1,"result":{"thread":{"id":"thread-read-only-review-final"}}}'
            ;;
        *'"method":"turn/start"'*)
            printf '%s\n' '{"method":"turn/started","params":{"threadId":"thread-read-only-review-final","turn":{"id":"turn-read-only-review-final","kind":"regular"}}}'
            printf '%s\n' '{"method":"item/agentMessage/delta","params":{"threadId":"thread-read-only-review-final","turnId":"turn-read-only-review-final","itemId":"msg-read-only-review-final","delta":"Read-only inspection only. No files changed, no tests run.\n\n- Final/outbox authority seam: runtime_pipeline owns final decisions.\n- Dirty Worktree Risks: implementation still needs to continue."}}'
            printf '%s\n' '{"method":"turn/completed","params":{"threadId":"thread-read-only-review-final","turn":{"id":"turn-read-only-review-final","status":"completed"}}}'
            exit 0
            ;;
    esac
done
"#,
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = fs::metadata(&script).unwrap().permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&script, permissions).unwrap();
        }
        script
    }

    fn write_pipeline_source(root: &Path) -> AgentSource {
        let home = root.join(".openclaw");
        let workspace = home.join("workspace");
        let skill = workspace.join("skills").join("memory-cron");
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(&skill).unwrap();
        fs::create_dir_all(home.join("agents").join("main").join("sessions")).unwrap();
        fs::create_dir_all(home.join("agents").join("xiaoxiaoli").join("sessions")).unwrap();
        fs::create_dir_all(home.join("agents").join("xiaoxiaoli").join("workspace")).unwrap();
        fs::write(workspace.join("AGENTS.md"), "# Agent").unwrap();
        fs::write(
            skill.join(crate::SKILL_FILE_NAME),
            "# Memory Cron\n\nRepair openclaw-mem cron jobs.",
        )
        .unwrap();
        fs::write(
            home.join("openclaw.json"),
            r#"{
              "agents": {
                "defaults": { "provider": "openai", "model": "codex" },
                "list": [
                  { "id": "main", "model": "gpt-5", "enabled": true },
                  { "id": "xiaoxiaoli", "model": "gpt-5", "enabled": true }
                ]
              },
                "models": {
                  "providers": {
                    "openai": { "apiKey": "${OPENAI_API_KEY}" }
                  }
                },
                "plugins": {
                  "entries": {
                    "openclaw-mem": {
                      "config": {
                        "episodes": {
                          "enabled": true,
                          "outputPath": "memory/episodes.jsonl"
                        }
                      }
                    },
                    "openclaw-mem-engine": {
                      "config": {
                        "autoCapture": { "enabled": true },
                        "symbolicCanvas": { "autoBuild": { "enabled": false } }
                      }
                    }
                  }
                }
              }"#,
        )
        .unwrap();
        fs::write(
            home.join("agents")
                .join("main")
                .join("sessions")
                .join("sessions.json"),
            "{}",
        )
        .unwrap();
        fs::write(
            home.join("agents")
                .join("xiaoxiaoli")
                .join("sessions")
                .join("sessions.json"),
            "{}",
        )
        .unwrap();
        AgentSource::with_workspace(home, workspace)
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-runtime-pipeline-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }

    fn force_retry_eligible(harness_home: &Path, queue_id: &str) {
        let queue_dir = harness_home.join("state").join("runtime-queue");
        let receipts_file = queue_dir.join("run-once-receipts.jsonl");
        let receipts = fs::read_to_string(&receipts_file).unwrap();
        let mut rows = receipts
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        let receipt = rows
            .iter_mut()
            .rev()
            .find(|receipt| {
                string_field(receipt, &["queueId"]) == Some(queue_id)
                    && string_field(receipt, &["status"]) == Some("retry-pending")
            })
            .expect("retry-pending receipt must exist before forcing eligibility");
        receipt["retrySchedule"]["nextEligibleAtMs"] = Value::from(0);
        let mut rewritten = rows
            .into_iter()
            .map(|row| serde_json::to_string(&row).unwrap())
            .collect::<Vec<_>>()
            .join("\n");
        rewritten.push('\n');
        fs::write(&receipts_file, rewritten).unwrap();
        let index_file = queue_dir.join("queue-state-index.json");
        if index_file.is_file() {
            fs::remove_file(index_file).unwrap();
        }
    }

    fn rewrite_pending_queue_identity(
        harness_home: &Path,
        queue_id: &str,
        runtime_class: &str,
        origin: &str,
        agent_id: &str,
        session_key: &str,
    ) {
        let queue_file = harness_home
            .join("state")
            .join("runtime-queue")
            .join("pending.jsonl");
        let text = fs::read_to_string(&queue_file).unwrap();
        let mut found = false;
        let rewritten = text
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| {
                let mut value: Value = serde_json::from_str(line).unwrap();
                if string_field(&value, &["queueId", "queue_id"]) == Some(queue_id) {
                    value["runtimeClass"] = Value::String(runtime_class.to_string());
                    value["origin"] = Value::String(origin.to_string());
                    value["agentId"] = Value::String(agent_id.to_string());
                    value["sessionKey"] = Value::String(session_key.to_string());
                    found = true;
                }
                serde_json::to_string(&value).unwrap()
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(found, "queue item {queue_id} should exist before rewrite");
        fs::write(queue_file, format!("{rewritten}\n")).unwrap();
    }

    fn hold_runtime_queue_lease_lock(queue_dir: &Path, runtime_class: &str) -> fs::File {
        let lock_dir = if runtime_class == "legacy" {
            queue_dir.to_path_buf()
        } else {
            queue_dir.join("classes").join(runtime_class)
        };
        fs::create_dir_all(&lock_dir).unwrap();
        let lock_file = lock_dir.join("runtime-leases.lock");
        let mut options = OpenOptions::new();
        options.create(true).write(true).truncate(true);
        #[cfg(not(windows))]
        {
            options.create_new(true);
        }
        #[cfg(windows)]
        {
            options.share_mode(0);
        }
        let mut file = options.open(lock_file).unwrap();
        writeln!(file, "{}", current_log_time_ms().unwrap()).unwrap();
        file
    }
}
