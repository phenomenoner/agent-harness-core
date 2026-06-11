use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::Value;

use crate::{
    AgentProgressContext, AgentProgressEvent, AgentProgressKind, AgentProgressStatus,
    AssistantNarrationConfig, AssistantNarrationMode, ChannelOutboundAttachment,
    ChannelOutboundAttachmentKind, ChannelOutboundMessage, ChannelOutboundMessageKind,
    CodexRuntimePlan, CodexRuntimePlanOptions, CodexRuntimePlanReport, CodexRuntimeReceiptStatus,
    CodexRuntimeRunOptions, CodexRuntimeRunReport, CodexRuntimeRunStatus, HarnessLogEvent,
    HarnessLogLevel, MemoryLifecycleTurnOptions, PromptAssemblyOptions,
    RuntimeExecutionReceiptStatus, RuntimeQueuePrepareOptions, RuntimeQueuePrepareReport,
    RuntimeQueuePreparedItem, append_agent_progress_event, append_harness_log, current_log_time_ms,
    load_assistant_narration_config, plan_codex_runtime, prepare_runtime_queue_item,
    read_channel_session_state, record_memory_lifecycle_turn, release_runtime_queue_lease,
    run_codex_runtime, write_json_atomic,
};

const RUNTIME_RUN_ONCE_SCHEMA: &str = "agent-harness.runtime-run-once.v1";
const MAX_RUNTIME_FAILURE_ATTEMPTS: usize = 3;

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
    pub execution_dir: Option<PathBuf>,
    pub transcript_file: Option<PathBuf>,
    pub outbox_file: Option<PathBuf>,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum RuntimeRunOnceStatus {
    Completed,
    NoWork,
    NoPreparedExecution,
    NoRuntimePlan,
    PreflightBlocked,
    SpawnFailed,
    ProtocolError,
    Timeout,
    FailedTerminal,
    Canceled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct QueueChannelContext {
    platform: String,
    channel_id: String,
    user_id: String,
    session_key: String,
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
        let receipt = RuntimeRunOnceReceipt {
            queue_id: None,
            status: RuntimeRunOnceStatus::NoWork,
            execution_dir: None,
            transcript_file: None,
            outbox_file: None,
            reason: "runtime queue lease lock is busy; retrying later".to_string(),
        };
        append_runtime_run_once_log(
            &options.harness_home,
            HarnessLogLevel::Info,
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

    if prepare.receipt.status == RuntimeExecutionReceiptStatus::NoPendingItem {
        let requested_queue = options.queue_id;
        let requested_specific_queue = requested_queue.is_some();
        let receipt = RuntimeRunOnceReceipt {
            queue_id: requested_queue,
            status: RuntimeRunOnceStatus::NoWork,
            execution_dir: None,
            transcript_file: None,
            outbox_file: None,
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
            release_runtime_queue_lease(&options.harness_home, queue_id)?;
        }
        let receipt = RuntimeRunOnceReceipt {
            queue_id: prepare.receipt.queue_id.clone(),
            status: RuntimeRunOnceStatus::NoPreparedExecution,
            execution_dir: plan.execution_dir.clone(),
            transcript_file: None,
            outbox_file: None,
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

    let run = run_codex_runtime(CodexRuntimeRunOptions {
        harness_home: options.harness_home.clone(),
        execution_dir: plan.execution_dir.clone(),
        plan_file: plan.plan_file.clone(),
        timeout_ms: options.timeout_ms,
        idle_timeout_ms: options.idle_timeout_ms,
        progress_context: progress_context.clone(),
    })?;
    warnings.extend(run.warnings.clone());
    if let Some(context) = &progress_context {
        append_runtime_progress_finished(
            &options.harness_home,
            context,
            run.receipt.status,
            &run.receipt.reason,
            run.receipt.elapsed_ms,
            &mut warnings,
        );
    }

    let queue_failure_attempts = run
        .receipt
        .queue_id
        .as_deref()
        .filter(|_| run.receipt.status != CodexRuntimeRunStatus::Completed)
        .map(|queue_id| count_prior_runtime_failures(&receipts_file, queue_id))
        .transpose()?
        .map(|attempts| attempts.saturating_add(1))
        .unwrap_or(0);
    let receipt_status = final_run_once_status(run.receipt.status, queue_failure_attempts);
    let receipt_reason = final_run_once_reason(
        receipt_status,
        run.receipt.status,
        queue_failure_attempts,
        &run.receipt.reason,
    );
    let mut outbox_file = None;
    let mut outbound_message = None;
    if run.receipt.status == CodexRuntimeRunStatus::Completed {
        if run.receipt.event_count == 0 && run.receipt.reason.contains("already recorded") {
            warnings.push(
                "codex-run reported an already recorded completion; outbox write skipped to avoid duplicate delivery"
                    .to_string(),
            );
        } else if let Some(context) = channel_context {
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
                            if let Some(codex_plan) = plan.plan.as_ref() {
                                match record_memory_lifecycle_turn(MemoryLifecycleTurnOptions {
                                    harness_home: options.harness_home.clone(),
                                    prompt_bundle_json: codex_plan.prompt_bundle_json.clone(),
                                    assistant_text: response.final_text.clone(),
                                    success: true,
                                    now_ms: current_log_time_ms()?,
                                }) {
                                    Ok(memory) => warnings.extend(memory.warnings),
                                    Err(error) => warnings.push(format!(
                                        "memory lifecycle recording failed: {error}"
                                    )),
                                }
                            }
                            let (text, attachments) =
                                split_outbound_media_directives(&response.outbound_text);
                            let message = ChannelOutboundMessage {
                                platform: context.platform,
                                channel_id: context.channel_id,
                                user_id: context.user_id,
                                session_key: context.session_key,
                                kind: ChannelOutboundMessageKind::AgentReply,
                                text,
                                attachments,
                            };
                            let file = append_outbound_message(&options.harness_home, &message)?;
                            outbox_file = Some(file);
                            outbound_message = Some(message);
                        } else {
                            let receipt = RuntimeRunOnceReceipt {
                                queue_id: run.receipt.queue_id.clone(),
                                status: map_run_once_status(run.receipt.status),
                                execution_dir: run.receipt.execution_dir.clone(),
                                transcript_file: run.receipt.transcript_file.clone(),
                                outbox_file: None,
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
        if channel_session_is_current(&options.harness_home, &context, &mut warnings)? {
            let message = ChannelOutboundMessage {
                platform: context.platform,
                channel_id: context.channel_id,
                user_id: context.user_id,
                session_key: context.session_key,
                kind: ChannelOutboundMessageKind::ErrorReply,
                text: runtime_failure_reply_text(status, &receipt_reason),
                attachments: Vec::new(),
            };
            let file = append_outbound_message(&options.harness_home, &message)?;
            outbox_file = Some(file);
            outbound_message = Some(message);
        } else {
            let receipt = RuntimeRunOnceReceipt {
                queue_id: run.receipt.queue_id.clone(),
                status,
                execution_dir: run.receipt.execution_dir.clone(),
                transcript_file: run.receipt.transcript_file.clone(),
                outbox_file: None,
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
    } else {
        warnings.push(format!(
            "runtime failure for queue item will be retried; attempt {queue_failure_attempts}/{MAX_RUNTIME_FAILURE_ATTEMPTS}"
        ));
    }

    let receipt = RuntimeRunOnceReceipt {
        queue_id: run.receipt.queue_id.clone(),
        status: receipt_status,
        execution_dir: run.receipt.execution_dir.clone(),
        transcript_file: run.receipt.transcript_file.clone(),
        outbox_file: outbox_file.clone(),
        reason: receipt_reason,
    };
    let log_level = match receipt.status {
        RuntimeRunOnceStatus::Completed => HarnessLogLevel::Info,
        RuntimeRunOnceStatus::Timeout
        | RuntimeRunOnceStatus::ProtocolError
        | RuntimeRunOnceStatus::SpawnFailed
        | RuntimeRunOnceStatus::FailedTerminal => HarnessLogLevel::Error,
        RuntimeRunOnceStatus::Canceled => HarnessLogLevel::Warn,
        RuntimeRunOnceStatus::NoWork
        | RuntimeRunOnceStatus::NoPreparedExecution
        | RuntimeRunOnceStatus::NoRuntimePlan
        | RuntimeRunOnceStatus::PreflightBlocked => HarnessLogLevel::Warn,
    };
    let log_event = match receipt.status {
        RuntimeRunOnceStatus::Completed => "runtime.run-once.completed",
        RuntimeRunOnceStatus::NoWork => "runtime.run-once.no-work",
        RuntimeRunOnceStatus::NoPreparedExecution => "runtime.run-once.no-prepared-execution",
        RuntimeRunOnceStatus::NoRuntimePlan => "runtime.run-once.no-runtime-plan",
        RuntimeRunOnceStatus::PreflightBlocked => "runtime.run-once.preflight-blocked",
        RuntimeRunOnceStatus::SpawnFailed => "runtime.run-once.spawn-failed",
        RuntimeRunOnceStatus::ProtocolError => "runtime.run-once.protocol-error",
        RuntimeRunOnceStatus::Timeout => "runtime.run-once.timeout",
        RuntimeRunOnceStatus::FailedTerminal => "runtime.run-once.failed-terminal",
        RuntimeRunOnceStatus::Canceled => "runtime.run-once.canceled",
    };
    append_runtime_run_once_log(&options.harness_home, log_level, log_event, &receipt)?;
    if let Some(queue_id) = receipt.queue_id.as_deref() {
        release_runtime_queue_lease(&options.harness_home, queue_id)?;
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
    status: CodexRuntimeRunStatus,
    reason: &str,
    elapsed_ms: u128,
    warnings: &mut Vec<String>,
) {
    let Ok(at_ms) = current_log_time_ms() else {
        warnings.push("progress event timestamp could not be read".to_string());
        return;
    };
    let progress_status = if status == CodexRuntimeRunStatus::Completed {
        AgentProgressStatus::Completed
    } else {
        AgentProgressStatus::Failed
    };
    let preview = if status == CodexRuntimeRunStatus::Completed {
        "done".to_string()
    } else {
        reason.to_string()
    };
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
        CodexRuntimeRunStatus::Timeout => RuntimeRunOnceStatus::Timeout,
        CodexRuntimeRunStatus::Canceled => RuntimeRunOnceStatus::Canceled,
    }
}

fn runtime_failure_reply_text(status: RuntimeRunOnceStatus, reason: &str) -> String {
    if status == RuntimeRunOnceStatus::Canceled {
        return "Stopped.".to_string();
    }
    if status == RuntimeRunOnceStatus::FailedTerminal {
        return format!(
            "Agent harness could not process this request and marked it failed-terminal.\nReason: {}\n\nUse /status runtime to inspect the queue.",
            truncate_for_channel(reason, 360)
        );
    }
    format!(
        "Agent harness runtime error: {:?}\nReason: {}\n\nUse /status security to check approvals and sandbox policy.",
        status,
        truncate_for_channel(reason, 360)
    )
}

fn final_run_once_status(
    codex_status: CodexRuntimeRunStatus,
    failure_attempts: usize,
) -> RuntimeRunOnceStatus {
    match codex_status {
        CodexRuntimeRunStatus::Completed => RuntimeRunOnceStatus::Completed,
        CodexRuntimeRunStatus::Canceled => RuntimeRunOnceStatus::Canceled,
        CodexRuntimeRunStatus::Timeout if failure_attempts < MAX_RUNTIME_FAILURE_ATTEMPTS => {
            RuntimeRunOnceStatus::Timeout
        }
        CodexRuntimeRunStatus::Timeout
        | CodexRuntimeRunStatus::PreflightBlocked
        | CodexRuntimeRunStatus::NoRuntimePlan
        | CodexRuntimeRunStatus::SpawnFailed
        | CodexRuntimeRunStatus::ProtocolError => RuntimeRunOnceStatus::FailedTerminal,
    }
}

fn final_run_once_reason(
    receipt_status: RuntimeRunOnceStatus,
    codex_status: CodexRuntimeRunStatus,
    failure_attempts: usize,
    reason: &str,
) -> String {
    match receipt_status {
        RuntimeRunOnceStatus::FailedTerminal => format!(
            "runtime queue item failed terminally after {failure_attempts} attempt(s); last codex status={codex_status:?}; reason: {reason}"
        ),
        RuntimeRunOnceStatus::Canceled => {
            format!("runtime queue item was canceled by operator request; reason: {reason}")
        }
        _ => reason.to_string(),
    }
}

fn should_write_failure_outbox(status: RuntimeRunOnceStatus) -> bool {
    matches!(
        status,
        RuntimeRunOnceStatus::Timeout
            | RuntimeRunOnceStatus::FailedTerminal
            | RuntimeRunOnceStatus::Canceled
            | RuntimeRunOnceStatus::NoRuntimePlan
            | RuntimeRunOnceStatus::PreflightBlocked
            | RuntimeRunOnceStatus::SpawnFailed
            | RuntimeRunOnceStatus::ProtocolError
    )
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

fn channel_context_from_prepared_item(item: &RuntimeQueuePreparedItem) -> QueueChannelContext {
    QueueChannelContext {
        platform: item.platform.clone(),
        channel_id: item.channel_id.clone(),
        user_id: item.user_id.clone(),
        session_key: item.session_key.clone(),
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

    warnings.push(format!(
        "assistant reply for stale session {} suppressed because active session is {}",
        context.session_key, state.active_session_key
    ));
    Ok(false)
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
            channel_id: string_field(&value, &["channelId", "channel_id"])
                .unwrap_or("unknown")
                .to_string(),
            user_id: string_field(&value, &["userId", "user_id"])
                .unwrap_or("unknown")
                .to_string(),
            session_key: string_field(&value, &["sessionKey", "session_key"])
                .unwrap_or("unknown")
                .to_string(),
        }));
    }
    warnings.push(format!(
        "runtime queue item `{queue_id}` was not found while resolving channel context"
    ));
    Ok(None)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LatestAssistantResponse {
    final_text: String,
    outbound_text: String,
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
    let final_text = entries[assistant_index].1.clone();
    let outbound_text = match config.mode {
        AssistantNarrationMode::Off | AssistantNarrationMode::ProgressPanel => final_text.clone(),
        AssistantNarrationMode::InlinePreface => {
            let prior_user_index = entries[..assistant_index]
                .iter()
                .rposition(|(role, _)| role == "user")
                .map(|index| index + 1)
                .unwrap_or(0);
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
    }))
}

fn compact_inline_narration(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn split_outbound_media_directives(text: &str) -> (String, Vec<ChannelOutboundAttachment>) {
    let mut clean_lines = Vec::new();
    let mut attachments = Vec::new();
    for line in text.lines() {
        let trimmed = line.trim();
        if let Some(path) = trimmed.strip_prefix("MEDIA:") {
            let path = path.trim();
            if !path.is_empty() {
                let path = PathBuf::from(path);
                attachments.push(ChannelOutboundAttachment {
                    kind: attachment_kind_from_path(&path),
                    mime: attachment_mime_from_path(&path),
                    filename: path
                        .file_name()
                        .map(|name| name.to_string_lossy().to_string()),
                    caption: None,
                    path,
                });
            }
            continue;
        }
        clean_lines.push(line);
    }
    (clean_lines.join("\n").trim().to_string(), attachments)
}

fn attachment_kind_from_path(path: &Path) -> ChannelOutboundAttachmentKind {
    match path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(|extension| extension.to_ascii_lowercase())
        .as_deref()
    {
        Some("jpg" | "jpeg" | "png" | "gif" | "webp") => ChannelOutboundAttachmentKind::Image,
        _ => ChannelOutboundAttachmentKind::Document,
    }
}

fn attachment_mime_from_path(path: &Path) -> Option<String> {
    match path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(|extension| extension.to_ascii_lowercase())
        .as_deref()
    {
        Some("jpg" | "jpeg") => Some("image/jpeg".to_string()),
        Some("png") => Some("image/png".to_string()),
        Some("gif") => Some("image/gif".to_string()),
        Some("webp") => Some("image/webp".to_string()),
        Some("pdf") => Some("application/pdf".to_string()),
        Some("txt" | "log" | "md") => Some("text/plain".to_string()),
        Some("json") => Some("application/json".to_string()),
        _ => None,
    }
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
    Ok(outbox_file)
}

fn append_json_line(path: &Path, value: &impl Serialize) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    let line = serde_json::to_string(value).map_err(io::Error::other)?;
    writeln!(file, "{line}")?;
    Ok(())
}

fn string_field<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a str> {
    for key in keys {
        if let Some(text) = value.get(*key).and_then(Value::as_str) {
            return Some(text);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AgentSource, ChannelReceiveOptions, ChannelReceiveStatus, build_source_skill_index,
        receive_channel_message,
    };
    use std::ffi::OsString;
    #[cfg(windows)]
    use std::os::windows::fs::OpenOptionsExt;
    use std::sync::Mutex;
    use std::time::{SystemTime, UNIX_EPOCH};

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn split_outbound_media_directives_extracts_attachments() {
        let (text, attachments) = split_outbound_media_directives(
            "Here is the file.\nMEDIA:D:\\Warehouse\\image.png\nDone.",
        );

        assert_eq!(text, "Here is the file.\nDone.");
        assert_eq!(attachments.len(), 1);
        assert_eq!(attachments[0].kind, ChannelOutboundAttachmentKind::Image);
        assert_eq!(attachments[0].mime.as_deref(), Some("image/png"));
        assert_eq!(attachments[0].filename.as_deref(), Some("image.png"));
        assert_eq!(
            attachments[0].path,
            PathBuf::from("D:\\Warehouse\\image.png")
        );
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
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "repair memory cron".to_string(),
            inbound_context: None,
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
            timeout_ms: 5_000,
            idle_timeout_ms: 5_000,
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
        let outbound = report.outbound_message.unwrap();
        assert_eq!(outbound.kind, ChannelOutboundMessageKind::AgentReply);
        assert_eq!(outbound.platform, "telegram");
        assert_eq!(outbound.channel_id, "dm-42");
        assert_eq!(outbound.user_id, "user-7");
        assert_eq!(outbound.text, "Pipeline fake reply.");
        let outbox_file = report.outbox_file.unwrap();
        let outbox = fs::read_to_string(outbox_file).unwrap();
        assert!(outbox.contains("\"kind\":\"agent-reply\""));
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
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "repair memory cron".to_string(),
            inbound_context: None,
            skill_limit: 3,
            now_ms: 1234,
        })
        .unwrap();
        assert_eq!(receive.status, ChannelReceiveStatus::AgentTurnQueued);
        let queue_id = receive.queue_id.unwrap();
        let _held_lock =
            hold_runtime_queue_lease_lock(&harness_home.join("state").join("runtime-queue"));

        let report = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: Some(queue_id.clone()),
            codex_executable: None,
            timeout_ms: 5_000,
            idle_timeout_ms: 5_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();

        assert_eq!(report.receipt.status, RuntimeRunOnceStatus::NoWork);
        assert_eq!(report.receipt.queue_id, None);
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
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "run a blocked command".to_string(),
            inbound_context: None,
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
            timeout_ms: 5_000,
            idle_timeout_ms: 5_000,
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
            timeout_ms: 5_000,
            idle_timeout_ms: 5_000,
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
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "old in-flight request".to_string(),
            inbound_context: None,
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
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "/new".to_string(),
            inbound_context: None,
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
            timeout_ms: 5_000,
            idle_timeout_ms: 5_000,
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
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "repair memory cron".to_string(),
            inbound_context: None,
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
            timeout_ms: 5_000,
            idle_timeout_ms: 5_000,
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
            timeout_ms: 5_000,
            idle_timeout_ms: 5_000,
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

    fn hold_runtime_queue_lease_lock(queue_dir: &Path) -> fs::File {
        fs::create_dir_all(queue_dir).unwrap();
        let lock_file = queue_dir.join("runtime-leases.lock");
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
