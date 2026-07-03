use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::thread;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::codex_runtime::{CodexContextRecoveryReceipt, CodexThreadHealthStatus};
use crate::rich_presentation::{
    RichMessagePresentation, rich_presentation_from_plain_final_with_attachment_count,
};
use crate::{
    AgentProgressContext, AgentProgressEvent, AgentProgressKind, AgentProgressStatus,
    AssistantNarrationConfig, AssistantNarrationMode, ChannelDeliveryIntent,
    ChannelDeliveryIntentKind, ChannelOutboundAttachment, ChannelOutboundAttachmentKind,
    ChannelOutboundMessage, ChannelOutboundMessageKind, CodexRuntimePlan, CodexRuntimePlanOptions,
    CodexRuntimePlanReport, CodexRuntimeReceiptStatus, CodexRuntimeRunOptions,
    CodexRuntimeRunReport, CodexRuntimeRunStatus, CompletedTurnWorkingSetSnapshotOptions,
    ContextRolloverMode, ContextRolloverPreparedRequeueReport,
    ContextRolloverRequeuePreparedOptions, HarnessLogEvent, HarnessLogLevel, InboundMediaArtifact,
    MediaDeliveryVerdict, MemoryLifecycleTurnOptions, PromptAssemblyOptions, ResponseToneConfig,
    ResponseToneContext, RuntimeContinuationMetadata, RuntimeExecutionReceiptStatus,
    RuntimeQueuePrepareOptions, RuntimeQueuePrepareReport, RuntimeQueuePreparedItem,
    SelfImprovementNotificationTarget, SelfImprovementReviewHookOptions,
    VirtualSessionTerminalOptions, append_agent_progress_event, append_harness_log,
    apply_response_tone, attachment_kind_from_path, continuation_session_key, current_log_time_ms,
    evaluate_outbound_media_path, inspect_runtime_backoff_policy, is_deliverable_media_path,
    load_assistant_narration_config, load_context_rollover_config, load_harness_media_config,
    load_response_tone_config, mark_cron_run_runtime_status_by_queue_id,
    mark_virtual_session_terminal, plan_codex_runtime, prepare_runtime_queue_item,
    read_channel_session_state, record_completed_turn_working_set_snapshot,
    record_memory_lifecycle_turn, record_skill_usage_from_prompt_bundle,
    release_runtime_queue_lease, requeue_prepared_context_rollover_if_no_parent_siblings,
    resolve_inbound_media_artifact_reference, root_working_session_key, run_codex_runtime,
    run_self_improvement_review_hook, write_json_atomic, write_media_policy_receipt,
};

const RUNTIME_RUN_ONCE_SCHEMA: &str = "agent-harness.runtime-run-once.v1";
const RUNTIME_DEAD_LETTER_SCHEMA: &str = "agent-harness.runtime-dead-letter.v1";
const FINAL_OUTBOX_RECEIPT_SCHEMA: &str = "agent-harness.runtime-final-outbox.v1";
#[derive(Debug, Clone)]
struct RuntimeContinuationCandidate {
    queue_id: String,
    session_key: String,
    runtime_class: String,
    origin: String,
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
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum RuntimeRunOnceStatus {
    Completed,
    Skipped,
    LeaseBusy,
    NoWork,
    NoPreparedExecution,
    NoRuntimePlan,
    PreflightBlocked,
    SpawnFailed,
    ProtocolError,
    ContextExhausted,
    Timeout,
    RetryPending,
    DeadLetter,
    FailedTerminal,
    Canceled,
}

impl RuntimeRunOnceStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Skipped => "skipped",
            Self::LeaseBusy => "lease-busy",
            Self::NoWork => "no-work",
            Self::NoPreparedExecution => "no-prepared-execution",
            Self::NoRuntimePlan => "no-runtime-plan",
            Self::PreflightBlocked => "preflight-blocked",
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct QueueChannelContext {
    platform: String,
    account_id: Option<String>,
    channel_id: String,
    user_id: String,
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

    let prepare = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
        harness_home: options.harness_home.clone(),
        queue_id: options.queue_id.clone(),
        prompt_options: options.prompt_options,
    })?;
    let mut warnings = prepare.warnings.clone();
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
        let receipt = RuntimeRunOnceReceipt {
            queue_id: requested_queue,
            status: RuntimeRunOnceStatus::NoWork,
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
            reason: if requested_specific_queue {
                "requested queue item was not pending or prepared".to_string()
            } else {
                "no pending or prepared runtime queue item is available".to_string()
            },
        };
        append_runtime_run_once_log(
            &options.harness_home,
            HarnessLogLevel::Warn,
            "runtime.run-once.no-work",
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

    let plan = plan_codex_runtime(CodexRuntimePlanOptions {
        harness_home: options.harness_home.clone(),
        execution_dir: prepare.receipt.execution_dir.clone(),
        codex_executable: options.codex_executable,
    })?;
    warnings.extend(plan.warnings.clone());

    if plan.receipt.status == CodexRuntimeReceiptStatus::NoPreparedExecution {
        if let Some(queue_id) = prepare.receipt.queue_id.as_deref() {
            if let Err(error) = release_runtime_queue_lease(&options.harness_home, queue_id) {
                warnings.push(format!("runtime queue lease release failed: {error}"));
            }
        }
        let receipt = RuntimeRunOnceReceipt {
            queue_id: prepare.receipt.queue_id.clone(),
            status: RuntimeRunOnceStatus::NoPreparedExecution,
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
            reason: "no prepared runtime execution is available to run".to_string(),
        };
        append_runtime_run_once_log(
            &options.harness_home,
            HarnessLogLevel::Warn,
            "runtime.run-once.no-prepared-execution",
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

    let run = run_codex_runtime(CodexRuntimeRunOptions {
        harness_home: options.harness_home.clone(),
        execution_dir: plan.execution_dir.clone(),
        plan_file: plan.plan_file.clone(),
        timeout_ms: options.timeout_ms,
        idle_timeout_ms: options.idle_timeout_ms,
        progress_context: progress_context.clone(),
    })?;
    warnings.extend(run.warnings.clone());
    let queue_failure_attempts = run
        .receipt
        .queue_id
        .as_deref()
        .filter(|_| run.receipt.status != CodexRuntimeRunStatus::Completed)
        .map(|queue_id| count_prior_runtime_failures(&receipts_file, queue_id))
        .transpose()?
        .map(|attempts| attempts.saturating_add(1))
        .unwrap_or(0);
    let retry_policy = inspect_runtime_backoff_policy(&options.harness_home)?;
    warnings.extend(retry_policy.warnings.clone());
    let mut receipt_status = final_run_once_status(
        run.receipt.status,
        queue_failure_attempts,
        &run.receipt.reason,
        retry_policy.policy.max_failure_attempts,
    );
    let mut receipt_reason = final_run_once_reason(
        receipt_status,
        run.receipt.status,
        queue_failure_attempts,
        retry_policy.policy.max_failure_attempts,
        &run.receipt.reason,
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
    if let Some(context) = &progress_context {
        append_runtime_progress_finished(
            &options.harness_home,
            context,
            receipt_status,
            &receipt_reason,
            run.receipt.elapsed_ms,
            &mut warnings,
        );
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
    let mut child_queue_id = None;
    let mut child_session_key = None;
    if run.receipt.status == CodexRuntimeRunStatus::Completed {
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
                                let message = ChannelOutboundMessage {
                                    platform: context.platform.clone(),
                                    account_id: context.account_id.clone(),
                                    channel_id: context.channel_id.clone(),
                                    user_id: context.user_id.clone(),
                                    session_key: context.session_key.clone(),
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
                                let (file, appended) = append_final_outbound_message_once(
                                    &options.harness_home,
                                    run.receipt.execution_dir.as_deref(),
                                    run.receipt.completion_file.as_deref(),
                                    &message,
                                    &mut warnings,
                                )?;
                                outbox_file = Some(file);
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
            let decision = final_outbox_decision(FinalOutboxInputKind::TerminalError);
            let message = ChannelOutboundMessage {
                platform: context.platform.clone(),
                account_id: context.account_id.clone(),
                channel_id: context.channel_id.clone(),
                user_id: context.user_id.clone(),
                session_key: context.session_key.clone(),
                kind: decision
                    .outbound_kind
                    .unwrap_or(ChannelOutboundMessageKind::ErrorReply),
                source_queue_id: run.receipt.queue_id.clone(),
                source_completion_file: run.receipt.completion_file.clone(),
                text: runtime_failure_reply_text(
                    status,
                    &receipt_reason,
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
            let file = append_outbound_message(&options.harness_home, &message)?;
            outbox_file = Some(file);
            outbound_message = Some(message);
        } else {
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
        if let Some(rollover) = maybe_enqueue_stream_unstable_retry_continuation(
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
        reason: receipt_reason,
    };
    if receipt.status == RuntimeRunOnceStatus::Completed
        && let Some(item) = prepare.item.as_ref()
    {
        match record_completed_turn_working_set_snapshot(CompletedTurnWorkingSetSnapshotOptions {
            harness_home: options.harness_home.clone(),
            platform: item.platform.clone(),
            channel_id: item.channel_id.clone(),
            user_id: item.user_id.clone(),
            agent_id: item.agent_id.clone(),
            working_session_key: item.session_key.clone(),
            queue_id: receipt.queue_id.clone(),
            message_text: Some(item.message_text.clone()),
            status: receipt.status.as_str().to_string(),
            run_once_receipt_file: Some(receipts_file.clone()),
            outbox_file: outbox_file.clone(),
            completion_file: run.receipt.completion_file.clone(),
            now_ms: current_log_time_ms()?,
        }) {
            Ok(_) => {}
            Err(error) => warnings.push(format!("working-set snapshot write failed: {error}")),
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
        | RuntimeRunOnceStatus::Skipped
        | RuntimeRunOnceStatus::RetryPending
        | RuntimeRunOnceStatus::LeaseBusy => HarnessLogLevel::Warn,
        RuntimeRunOnceStatus::NoWork
        | RuntimeRunOnceStatus::NoPreparedExecution
        | RuntimeRunOnceStatus::NoRuntimePlan
        | RuntimeRunOnceStatus::PreflightBlocked => HarnessLogLevel::Warn,
    };
    let log_event = match receipt.status {
        RuntimeRunOnceStatus::Completed => "runtime.run-once.completed",
        RuntimeRunOnceStatus::Skipped => "runtime.run-once.skipped",
        RuntimeRunOnceStatus::LeaseBusy => "runtime.run-once.lease-busy",
        RuntimeRunOnceStatus::NoWork => "runtime.run-once.no-work",
        RuntimeRunOnceStatus::NoPreparedExecution => "runtime.run-once.no-prepared-execution",
        RuntimeRunOnceStatus::NoRuntimePlan => "runtime.run-once.no-runtime-plan",
        RuntimeRunOnceStatus::PreflightBlocked => "runtime.run-once.preflight-blocked",
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

    write_runtime_run_once_report(
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
    )
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
    reason: &str,
    elapsed_ms: u128,
    warnings: &mut Vec<String>,
) {
    let Ok(at_ms) = current_log_time_ms() else {
        warnings.push("progress event timestamp could not be read".to_string());
        return;
    };
    let progress_status = match status {
        RuntimeRunOnceStatus::Completed => AgentProgressStatus::Completed,
        RuntimeRunOnceStatus::Skipped => AgentProgressStatus::Completed,
        RuntimeRunOnceStatus::RetryPending => AgentProgressStatus::Progress,
        _ => AgentProgressStatus::Failed,
    };
    let preview = runtime_progress_preview(status, reason);
    append_progress_nonfatal(
        harness_home,
        AgentProgressEvent::new(
            context,
            AgentProgressKind::Runtime,
            "run",
            preview,
            progress_status,
            at_ms,
        )
        .elapsed_ms(elapsed_ms)
        .source("runtime-pipeline"),
        warnings,
    );
}

fn runtime_progress_preview(status: RuntimeRunOnceStatus, reason: &str) -> String {
    match status {
        RuntimeRunOnceStatus::Completed => "done".to_string(),
        RuntimeRunOnceStatus::Skipped => "skipped because continuation work was queued".to_string(),
        RuntimeRunOnceStatus::RetryPending => {
            "transient runtime failure; preserving session for retry".to_string()
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
    if status == RuntimeRunOnceStatus::Canceled {
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

fn final_run_once_status(
    codex_status: CodexRuntimeRunStatus,
    failure_attempts: usize,
    reason: &str,
    max_failure_attempts: usize,
) -> RuntimeRunOnceStatus {
    match codex_status {
        CodexRuntimeRunStatus::Completed => RuntimeRunOnceStatus::Completed,
        CodexRuntimeRunStatus::Canceled => RuntimeRunOnceStatus::Canceled,
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

fn should_write_failure_outbox(status: RuntimeRunOnceStatus) -> bool {
    matches!(
        status,
        RuntimeRunOnceStatus::DeadLetter
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
    if runtime_class.is_some_and(|value| value != "interactive")
        || origin.is_some_and(|value| value != "channel")
    {
        return FinalOutboxInputKind::InternalEvidence;
    }
    if !final_outbox_owner_is_channel_parent(agent_id, run_session_key, channel_session_key) {
        return FinalOutboxInputKind::InternalEvidence;
    }
    if implementation_request_requires_parent_completion(user_message)
        && looks_like_read_only_review_evidence(final_text)
    {
        return FinalOutboxInputKind::ReviewEvidence;
    }
    FinalOutboxInputKind::AgentReply
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
    if session_key_agent_segment(channel_session_key)
        .as_deref()
        .is_some_and(|agent| agent != "main")
    {
        return false;
    }
    let agent_id = agent_id.map(str::trim).filter(|value| !value.is_empty());
    if let Some(agent_id) = agent_id {
        return agent_id == "main";
    }
    true
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
}

fn continuation_candidate_for_run(
    harness_home: &Path,
    item: Option<&RuntimeQueuePreparedItem>,
    queue_id: Option<&str>,
    label: &str,
    warnings: &mut Vec<String>,
) -> io::Result<Option<RuntimeContinuationCandidate>> {
    if let Some(item) = item {
        return Ok(Some(RuntimeContinuationCandidate::from_prepared_item(item)));
    }
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
    Ok(recovered)
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
    if item.runtime_class != "interactive" || item.origin != "channel" {
        return Ok(None);
    }
    if !context_recovery_indicates_polluted_thread(run.receipt.context_recovery.as_ref()) {
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
            &item.session_key,
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
                "automatic polluted-thread virtual session recovery after terminal {}; codexStatus={:?}; reason={}",
                receipt_status.as_str(),
                run.receipt.status,
                run.receipt.reason
            ),
            now_ms,
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
            &item.session_key,
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
    session_key: &str,
    max_continuation_depth: u64,
    warnings: &mut Vec<String>,
) -> io::Result<()> {
    let ended_by = format!("max-continuation-depth:{max_continuation_depth}");
    match mark_virtual_session_terminal(VirtualSessionTerminalOptions {
        harness_home: harness_home.to_path_buf(),
        session_key: session_key.to_string(),
        ended_by,
        now_ms: current_log_time_ms()?,
    }) {
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

fn should_enqueue_stream_unstable_retry_continuation(
    item: &RuntimeContinuationCandidate,
    run: &CodexRuntimeRunReport,
    failure_attempts: usize,
    min_attempts: usize,
    token_limit: u64,
) -> bool {
    if item.runtime_class != "interactive" || item.origin != "channel" {
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
    }
    Ok(report)
}

fn count_prior_runtime_failures(receipts_file: &Path, queue_id: &str) -> io::Result<usize> {
    if !receipts_file.is_file() {
        return Ok(0);
    }
    let text = fs::read_to_string(receipts_file)?;
    let mut failures = 0usize;
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let value: Value = match serde_json::from_str(trimmed) {
            Ok(value) => value,
            Err(_) => continue,
        };
        if string_field(&value, &["queueId", "queue_id"]) != Some(queue_id) {
            continue;
        }
        let Some(status) = string_field(&value, &["status"]) else {
            continue;
        };
        if is_terminal_run_once_status(status) {
            continue;
        }
        if status != "completed" && status != "no-work" {
            failures = failures.saturating_add(1);
        }
    }
    Ok(failures)
}

fn is_terminal_run_once_status(status: &str) -> bool {
    matches!(
        status,
        "completed" | "timeout" | "failed-terminal" | "canceled" | "skipped" | "dead-letter"
    )
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
    let Some(state) = read_channel_session_state(
        harness_home,
        &context.platform,
        &context.channel_id,
        &context.user_id,
    )?
    else {
        return Ok(true);
    };
    if state.active_session_key == context.session_key {
        return Ok(true);
    }
    let active_agent = session_key_agent_segment(&state.active_session_key);
    let context_agent = session_key_agent_segment(&context.session_key);
    if let (Some(active_agent), Some(context_agent)) =
        (active_agent.as_deref(), context_agent.as_deref())
        && active_agent != context_agent
    {
        warnings.push(format!(
            "assistant reply session {} is not suppressed by active session {} because active state belongs to agent `{}` while the reply belongs to agent `{}`",
            context.session_key, state.active_session_key, active_agent, context_agent
        ));
        return Ok(true);
    }

    warnings.push(format!(
        "assistant reply for stale session {} suppressed because active session is {}",
        context.session_key, state.active_session_key
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
        return Ok(None);
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
    Ok(None)
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
    message: &ChannelOutboundMessage,
) -> io::Result<PathBuf> {
    let outbox_file = harness_home
        .join("state")
        .join("channels")
        .join("outbox.jsonl");
    append_json_line(&outbox_file, message)?;
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
    Ok(outbox_file)
}

fn append_final_outbound_message_once(
    harness_home: &Path,
    execution_dir: Option<&Path>,
    completion_file: Option<&Path>,
    message: &ChannelOutboundMessage,
    warnings: &mut Vec<String>,
) -> io::Result<(PathBuf, bool)> {
    let _lock = acquire_final_outbox_lock(execution_dir, warnings)?;
    if let Some(receipt) = read_final_outbox_receipt(execution_dir, warnings)? {
        if final_outbox_receipt_matches(&receipt, message, completion_file) {
            warnings.push(format!(
                "runtime final outbox already enqueued for queue {}; skipping duplicate append",
                receipt.queue_id.as_deref().unwrap_or("-")
            ));
            return Ok((receipt.outbox_file, false));
        }
        warnings.push(format!(
            "runtime final outbox receipt did not match queue/completion {}; falling back to outbox scan",
            message.source_queue_id.as_deref().unwrap_or("-")
        ));
    }
    if let Some(outbox_file) = find_existing_source_outbox_message(harness_home, message)? {
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
        return Ok((outbox_file, false));
    }

    let outbox_file = append_outbound_message(harness_home, message)?;
    if let Some(execution_dir) = execution_dir {
        record_final_outbox_receipt(
            execution_dir,
            completion_file,
            message,
            &outbox_file,
            warnings,
        )?;
    }
    Ok((outbox_file, true))
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
) -> io::Result<Option<PathBuf>> {
    let Some(source_queue_id) = message.source_queue_id.as_deref() else {
        return Ok(None);
    };
    let outbox_file = harness_home
        .join("state")
        .join("channels")
        .join("outbox.jsonl");
    let text = match fs::read_to_string(&outbox_file) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(existing) = serde_json::from_str::<ChannelOutboundMessage>(trimmed) else {
            continue;
        };
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
    use crate::codex_runtime::CodexRuntimeUsage;
    use crate::{
        AgentSource, ChannelReceiveOptions, ChannelReceiveStatus, build_source_skill_index,
        receive_channel_message,
    };
    use std::ffi::OsString;
    use std::fs::OpenOptions;
    use std::io::Write;
    #[cfg(windows)]
    use std::os::windows::fs::OpenOptionsExt;
    use std::sync::Mutex;
    use std::time::{SystemTime, UNIX_EPOCH};

    static ENV_LOCK: Mutex<()> = Mutex::new(());

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
        let _guard = ENV_LOCK.lock().unwrap();
        let root = temp_root("run_runtime_queue_once_records_agent_reply_outbox");
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
        let envelope =
            crate::resolve_virtual_session_working_context(crate::VirtualSessionContextQuery {
                harness_home: harness_home.clone(),
                platform: "telegram".to_string(),
                channel_id: "dm-42".to_string(),
                user_id: "user-7".to_string(),
                agent_id: "main".to_string(),
                session_key: Some("telegram:dm-42:user-7:main".to_string()),
                now_ms: 1235,
            })
            .unwrap();
        assert_eq!(
            envelope.scope_decision.status, "same-virtual-session",
            "completed runtime turn should write a root working-set snapshot"
        );
        assert!(envelope.working_set_file.as_ref().unwrap().is_file());
        assert!(
            envelope
                .recent_queue_ids
                .iter()
                .any(|queue_id| queue_id == &completed_queue_id)
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn run_runtime_queue_once_keeps_media_attachments_in_rich_presentation() {
        let _guard = ENV_LOCK.lock().unwrap();
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
        let _guard = ENV_LOCK.lock().unwrap();
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
        let _guard = ENV_LOCK.lock().unwrap();
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
        let _guard = ENV_LOCK.lock().unwrap();
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
        let _guard = ENV_LOCK.lock().unwrap();
        let root =
            temp_root("run_runtime_queue_once_skips_duplicate_for_existing_final_outbox_marker");
        let (harness_home, queue_id, execution_dir, _env) =
            prepare_already_recorded_completion_without_outbox(&root);
        let outbox_file = harness_home
            .join("state")
            .join("channels")
            .join("outbox.jsonl");
        fs::create_dir_all(outbox_file.parent().unwrap()).unwrap();
        let seeded = ChannelOutboundMessage {
            platform: "telegram".to_string(),
            account_id: None,
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            session_key: "telegram:dm-42:user-7:main".to_string(),
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
            queue_id: Some(queue_id),
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
        let _guard = ENV_LOCK.lock().unwrap();
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
        let _guard = ENV_LOCK.lock().unwrap();
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
    fn run_runtime_queue_once_records_runtime_failure_error_outbox() {
        let _guard = ENV_LOCK.lock().unwrap();
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
        assert_eq!(second.receipt.status, RuntimeRunOnceStatus::NoWork);
        assert!(second.run.is_none());

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
                Some("please review this implementation plan"),
                review_text,
            ),
            FinalOutboxInputKind::AgentReply
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
            FinalOutboxInputKind::InternalEvidence
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
            "discord:dm-42:user-7:main:cont-1"
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
            rollover.previous_working_session_key.as_deref(),
            Some(session_key)
        );
        assert_eq!(
            rollover.new_working_session_key,
            "discord:dm-42:user-7:main:cont-1"
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
        assert!(warnings.is_empty());

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
    fn stream_unstable_retry_continuation_tombstones_parent_queue_item() {
        let _guard = ENV_LOCK.lock().unwrap();
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
            "discord:dm-42:user-7:main",
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
        let _guard = ENV_LOCK.lock().unwrap();
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
            skill_limit: 3,
            now_ms: 1234,
        })
        .unwrap();
        let parent_queue_id = receive.queue_id.unwrap();
        write_test_channel_session_state(
            &harness_home,
            "discord",
            "dm-42",
            "user-7",
            "discord:dm-42:user-7:main",
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
            Some("discord:dm-42:user-7:main:cont-1")
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
        let _guard = ENV_LOCK.lock().unwrap();
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

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn run_runtime_queue_once_keeps_external_review_evidence_resumable_without_final_outbox() {
        let _guard = ENV_LOCK.lock().unwrap();
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
            idle_timeout_ms: 3_000,
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
        let _guard = ENV_LOCK.lock().unwrap();
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
    fn run_runtime_queue_once_suppresses_non_main_agent_final_outbox() {
        let _guard = ENV_LOCK.lock().unwrap();
        let root = temp_root("run_runtime_queue_once_suppresses_non_main_agent_final_outbox");
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
            agent_id: Some("xiaoxiaoli".to_string()),
            session_key: None,
            message: "complete this delegated implementation".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            skill_limit: 3,
            now_ms: 1234,
        })
        .unwrap();
        assert_eq!(receive.status, ChannelReceiveStatus::AgentTurnQueued);
        let delegated_session_key = "telegram:dm-42:user-7:xiaoxiaoli";
        let pending_file = harness_home
            .join("state")
            .join("runtime-queue")
            .join("pending.jsonl");
        let pending = fs::read_to_string(&pending_file).unwrap();
        fs::write(
            &pending_file,
            pending
                .replace(r#""agentId":"main""#, r#""agentId":"xiaoxiaoli""#)
                .replace(
                    r#""sessionKey":"telegram:dm-42:user-7:main""#,
                    &format!(r#""sessionKey":"{delegated_session_key}""#),
                ),
        )
        .unwrap();
        write_test_channel_session_state(
            &harness_home,
            "telegram",
            "dm-42",
            "user-7",
            delegated_session_key,
            "xiaoxiaoli",
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

        assert_eq!(report.receipt.status, RuntimeRunOnceStatus::Completed);
        assert!(report.outbound_message.is_none());
        assert!(report.outbox_file.is_none());
        assert!(
            report
                .warnings
                .iter()
                .any(|warning| warning.contains("classified as InternalEvidence")),
            "non-main completed output must be classified as internal evidence"
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
    fn run_runtime_queue_once_suppresses_stale_session_reply_after_new() {
        let _guard = ENV_LOCK.lock().unwrap();
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

        assert_eq!(report.receipt.status, RuntimeRunOnceStatus::Completed);
        assert!(report.outbound_message.is_none());
        assert!(
            report
                .warnings
                .iter()
                .any(|warning| warning.contains("stale session"))
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
        let _guard = ENV_LOCK.lock().unwrap();
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
    fn channel_session_freshness_does_not_cross_suppress_other_agent() {
        let root = temp_root("channel_session_freshness_does_not_cross_suppress_other_agent");
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
                channel_id: "dm-agent-boundary".to_string(),
                user_id: "user-agent-boundary".to_string(),
                active_session_key:
                    "telegram:dm-agent-boundary:user-agent-boundary:main:session-live-main"
                        .to_string(),
                agent_id: Some("main".to_string()),
                provider: None,
                model: None,
                session_topic: None,
                model_override: None,
                model_override_provider: None,
                model_override_model: None,
                thinking_enabled: false,
                thinking_level: None,
                thinking_instruction: None,
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
            session_key:
                "telegram:dm-agent-boundary:user-agent-boundary:xiaoxiaoli:session-completed"
                    .to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
        };
        let mut warnings = Vec::new();

        assert!(channel_session_is_current(&harness_home, &context, &mut warnings).unwrap());
        assert!(
            warnings
                .iter()
                .any(|warning| warning.contains("belongs to agent `main`"))
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn run_runtime_queue_once_stops_when_only_terminal_queue_items_remain() {
        let _guard = ENV_LOCK.lock().unwrap();
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

        assert_eq!(second.receipt.status, RuntimeRunOnceStatus::NoWork);
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

    fn prepared_test_item(
        queue_id: &str,
        session_key: &str,
        continuation_index: Option<u64>,
    ) -> RuntimeQueuePreparedItem {
        RuntimeQueuePreparedItem {
            queue_id: queue_id.to_string(),
            agent_id: "main".to_string(),
            session_key: session_key.to_string(),
            runtime_class: "interactive".to_string(),
            origin: "channel".to_string(),
            cron_run_id: None,
            scheduled_for_ms: None,
            platform: "telegram".to_string(),
            account_id: None,
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            message_text: "continue after recovery".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            provider: Some("openai".to_string()),
            model: Some("gpt-5.5".to_string()),
            execution_dir: PathBuf::from("execution"),
            prompt_bundle_json: PathBuf::from("prompt-bundle.json"),
            prompt_markdown: PathBuf::from("prompt.md"),
            receipt_file: PathBuf::from("execution-receipt.json"),
            planned_transcript_file: PathBuf::from("transcript.jsonl"),
            planned_trajectory_file: PathBuf::from("trajectory.jsonl"),
            selected_skill_ids: Vec::new(),
            continuation: RuntimeContinuationMetadata {
                continuation_index,
                ..RuntimeContinuationMetadata::legacy()
            },
        }
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
                    source: "test".to_string(),
                    raw: None,
                }),
                media_plan,
                context_recovery: None,
                tool_use_timeout: None,
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
                channel_id: "dm-42".to_string(),
                user_id: "user-7".to_string(),
                active_session_key: session_key.to_string(),
                agent_id: Some("main".to_string()),
                provider: None,
                model: None,
                session_topic: None,
                model_override: None,
                model_override_provider: None,
                model_override_model: None,
                thinking_enabled: false,
                thinking_level: None,
                thinking_instruction: None,
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
                channel_id: channel_id.to_string(),
                user_id: user_id.to_string(),
                active_session_key: session_key.to_string(),
                agent_id: Some(agent_id.to_string()),
                provider: None,
                model: None,
                session_topic: None,
                model_override: None,
                model_override_provider: None,
                model_override_model: None,
                thinking_enabled: false,
                thinking_level: None,
                thinking_instruction: None,
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
        [Console]::Out.WriteLine('{"method":"item/agentMessage/delta","params":{"delta":"Pipeline fake reply."}}')
        [Console]::Out.WriteLine('{"method":"turn/completed","params":{"turn":{"id":"turn-pipeline","status":"completed"}}}')
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
        $delta = "Here is the file.`nMEDIA:$media`nDone."
        $event = @{
            method = 'item/agentMessage/delta'
            params = @{ delta = $delta }
        } | ConvertTo-Json -Compress
        [Console]::Out.WriteLine($event)
        [Console]::Out.WriteLine('{"method":"turn/completed","params":{"turn":{"id":"turn-media-final","status":"completed"}}}')
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
        let script = root.join("fake-external-review-only-app-server.ps1");
        fs::write(
            &script,
            r#"
$countFile = Join-Path $PSScriptRoot 'external-review-only-attempt.txt'
$attempt = 0
if (Test-Path -LiteralPath $countFile) {
    $raw = Get-Content -LiteralPath $countFile -Raw
    [void][int]::TryParse($raw.Trim(), [ref]$attempt)
}
Set-Content -LiteralPath $countFile -Value ([string]($attempt + 1))
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
    } elseif ($msg.method -eq 'thread/start' -or $msg.method -eq 'thread/resume') {
        [Console]::Out.WriteLine('{"id":1,"result":{"thread":{"id":"thread-review-evidence"}}}')
        [Console]::Out.Flush()
    } elseif ($msg.method -eq 'turn/start') {
        if ($attempt -eq 0) {
            [Console]::Out.WriteLine('{"method":"turn/started","params":{"threadId":"thread-review-evidence","turn":{"id":"turn-review-timeout","kind":"regular"}}}')
            [Console]::Out.WriteLine('{"method":"item/started","params":{"item":{"type":"commandExecution","id":"cmd-review-timeout","command":"claude -p review prompt"},"threadId":"thread-review-evidence","turnId":"turn-review-timeout"}}')
            [Console]::Out.Flush()
            Start-Sleep -Seconds 10
        } else {
            [Console]::Out.WriteLine('{"method":"turn/started","params":{"threadId":"thread-review-evidence","turn":{"id":"turn-review-only","kind":"regular"}}}')
            [Console]::Out.WriteLine('{"method":"item/completed","params":{"item":{"type":"agentMessage","id":"msg-review-only","text":"Claude second brain review: PASS. Findings only; implementation still needs to continue.","phase":"final_answer"},"threadId":"thread-review-evidence","turnId":"turn-review-only","completedAtMs":1234}}')
            [Console]::Out.WriteLine('{"method":"turn/completed","params":{"threadId":"thread-review-evidence","turn":{"id":"turn-review-only","status":"completed","usage":{"inputTokens":30,"outputTokens":12,"totalTokens":42}}}}')
            [Console]::Out.Flush()
            break
        }
    }
}
"#,
        )
        .unwrap();
        let cmd = root.join("fake-external-review-only-codex.cmd");
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
        [Console]::Out.WriteLine('{"method":"item/agentMessage/delta","params":{"delta":"Read-only inspection only. No files changed, no tests run.\n\n- Final/outbox authority seam: runtime_pipeline owns final decisions.\n- Dirty Worktree Risks: implementation still needs to continue."}}')
        [Console]::Out.WriteLine('{"method":"turn/completed","params":{"turn":{"id":"turn-read-only-review-final","status":"completed"}}}')
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
            printf '%s\n' '{"method":"item/agentMessage/delta","params":{"delta":"Pipeline fake reply."}}'
            printf '%s\n' '{"method":"turn/completed","params":{"turn":{"id":"turn-pipeline","status":"completed"}}}'
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
            printf '%s\n' "{\"method\":\"item/agentMessage/delta\",\"params\":{\"delta\":\"Here is the file.\\nMEDIA:$media\\nDone.\"}}"
            printf '%s\n' '{"method":"turn/completed","params":{"turn":{"id":"turn-media-final","status":"completed"}}}'
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
            printf '%s\n' '{"method":"item/agentMessage/delta","params":{"delta":"Read-only inspection only. No files changed, no tests run.\n\n- Final/outbox authority seam: runtime_pipeline owns final decisions.\n- Dirty Worktree Risks: implementation still needs to continue."}}'
            printf '%s\n' '{"method":"turn/completed","params":{"turn":{"id":"turn-read-only-review-final","status":"completed"}}}'
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
                  { "id": "main", "model": "gpt-5", "enabled": true }
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
