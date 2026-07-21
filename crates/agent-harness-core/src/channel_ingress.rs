use std::fs;
use std::io;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::latency::{LatencyStage, latency_receipts_file, record_latency_stage};
use crate::runtime_pending_index::pending_queue_id_exists_from_index;
use crate::runtime_receipt_history::find_runtime_queue_terminal_history;
use crate::runtime_worker::{refresh_runtime_queue_state_index, terminal_run_once_ids_from_index};
use crate::{
    AgentProgressContext, AgentProgressEvent, AgentProgressKind, AgentProgressStatus, AgentSource,
    ChannelCommandApplyOptions, ChannelCommandApplyReport, ChannelOutboundMessage,
    ChannelStepAction, HarnessLogEvent, HarnessLogLevel, InboundMediaArtifact,
    RuntimeQueueEnqueueOptions, RuntimeQueueEnqueueReport, SkillIndex, TurnPlanInput,
    append_agent_progress_event, append_channel_outbox_message, append_harness_log,
    apply_channel_command_step, build_channel_step, build_turn_plan_for_account,
    load_agent_registry,
};

const CHANNEL_RECEIVE_SCHEMA: &str = "agent-harness.channel-receive.v1";
const INGRESS_CLAIM_SCHEMA: &str = "agent-harness.ingress-claim.v1";
const INGRESS_CLAIM_TTL_MS: i64 = 24 * 60 * 60 * 1000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelReceiveOptions {
    pub source: AgentSource,
    pub runtime_workspace: Option<PathBuf>,
    pub harness_home: PathBuf,
    pub skill_index: SkillIndex,
    pub platform: String,
    pub account_id: Option<String>,
    pub channel_id: String,
    pub user_id: String,
    pub agent_id: Option<String>,
    pub session_key: Option<String>,
    pub message: String,
    pub inbound_context: Option<String>,
    pub inbound_media_artifacts: Vec<InboundMediaArtifact>,
    pub inbound_event_kind: Option<String>,
    pub inbound_event_id: Option<String>,
    pub skill_limit: usize,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelReceiveReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub platform: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account_id: Option<String>,
    pub channel_id: String,
    pub user_id: String,
    pub session_key: String,
    pub status: ChannelReceiveStatus,
    pub step_action: ChannelStepAction,
    pub command_name: Option<String>,
    pub queue_id: Option<String>,
    pub inbound_canonical_id: Option<String>,
    pub duplicate_sibling_of: Option<String>,
    pub outbox_file: PathBuf,
    pub receipts_file: PathBuf,
    pub command_apply: Option<ChannelCommandApplyReport>,
    pub queue_enqueue: Option<RuntimeQueueEnqueueReport>,
    pub outbound_messages: Vec<ChannelOutboundMessage>,
    pub receipt: ChannelReceiveReceipt,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ChannelReceiveStatus {
    CommandApplied,
    AgentTurnQueued,
    SessionTransitionPending,
    ErrorReplied,
    DuplicateSuppressed,
    Skipped,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelReceiveReceipt {
    pub status: ChannelReceiveStatus,
    pub platform: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account_id: Option<String>,
    pub channel_id: String,
    pub user_id: String,
    pub session_key: String,
    pub queue_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inbound_canonical_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duplicate_sibling_of: Option<String>,
    pub outbound_count: usize,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct IngressClaimRecord {
    schema: String,
    inbound_canonical_id: String,
    platform: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    account_id: Option<String>,
    channel_id: String,
    user_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    agent_id: Option<String>,
    event_kind: String,
    event_id: String,
    first_seen_at_ms: i64,
    expires_at_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    queue_id: Option<String>,
}

const HELD_CHANNEL_MESSAGE_SCHEMA: &str = "agent-harness.held-channel-message.v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HeldChannelMessageV1 {
    schema: String,
    held_id: String,
    source_home: PathBuf,
    source_workspace: PathBuf,
    runtime_workspace: Option<PathBuf>,
    platform: String,
    account_id: Option<String>,
    channel_id: String,
    user_id: String,
    agent_id: Option<String>,
    old_session_key: String,
    message: String,
    inbound_context: Option<String>,
    inbound_media_artifacts: Vec<InboundMediaArtifact>,
    inbound_event_kind: Option<String>,
    inbound_event_id: Option<String>,
    inbound_canonical_id: Option<String>,
    skill_limit: usize,
    received_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HeldChannelMessageReconcileReport {
    pub scanned: usize,
    pub replayed: usize,
    pub still_held: usize,
    pub failed: usize,
    pub queue_ids: Vec<String>,
    pub warnings: Vec<String>,
}

pub fn receive_channel_message(options: ChannelReceiveOptions) -> io::Result<ChannelReceiveReport> {
    let channel_state_dir = options.harness_home.join("state").join("channels");
    let outbox_file = channel_state_dir.join("outbox.jsonl");
    let receipts_file = channel_state_dir.join("receive-receipts.jsonl");
    fs::create_dir_all(&channel_state_dir)?;

    let registry = load_agent_registry(&options.source)?;
    // Every provider ingress receives an explicit account axis.  Legacy
    // callers without an account are deterministically placed in `default`,
    // never in a wildcard/shared lane.
    let account_id = options
        .account_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("default")
        .to_string();
    let mut held_input = HeldChannelMessageV1 {
        schema: HELD_CHANNEL_MESSAGE_SCHEMA.to_string(),
        held_id: String::new(),
        source_home: options.source.home.clone(),
        source_workspace: options.source.workspace.clone(),
        runtime_workspace: options.runtime_workspace.clone(),
        platform: options.platform.clone(),
        account_id: Some(account_id.clone()),
        channel_id: options.channel_id.clone(),
        user_id: options.user_id.clone(),
        agent_id: options.agent_id.clone(),
        old_session_key: options.session_key.clone().unwrap_or_default(),
        message: options.message.clone(),
        inbound_context: options.inbound_context.clone(),
        inbound_media_artifacts: options.inbound_media_artifacts.clone(),
        inbound_event_kind: options.inbound_event_kind.clone(),
        inbound_event_id: options.inbound_event_id.clone(),
        inbound_canonical_id: None,
        skill_limit: options.skill_limit,
        received_at_ms: options.now_ms,
    };
    let turn = build_turn_plan_for_account(
        &options.source,
        &registry,
        &options.skill_index,
        TurnPlanInput {
            harness_home: Some(options.harness_home.clone()),
            platform: options.platform.clone(),
            channel_id: options.channel_id.clone(),
            user_id: options.user_id.clone(),
            text: options.message,
            inbound_context: options.inbound_context,
            inbound_media_artifacts: options.inbound_media_artifacts,
            requested_agent_id: options.agent_id.clone(),
            session_hint: options.session_key,
            skill_limit: options.skill_limit,
        },
        Some(account_id.clone()),
    )?;
    let mut step = build_channel_step(&registry, &turn);
    step.account_id = turn.account_id.clone().or_else(|| Some(account_id.clone()));
    let command_name = turn
        .command
        .as_ref()
        .map(|command| command.name().to_string());
    let mut warnings = step.warnings.clone();
    let inbound_canonical_id = inbound_canonical_id(
        &options.platform,
        Some(account_id.as_str()),
        &options.channel_id,
        &options.user_id,
        options.agent_id.as_deref(),
        options.inbound_event_kind.as_deref(),
        options.inbound_event_id.as_deref(),
    );
    if step.action == ChannelStepAction::EnqueueAgentTurn
        && let Some(agent) = turn.agent.as_ref()
    {
        let lane = crate::ChannelStateLane::new(
            &options.platform,
            Some(account_id.as_str()),
            &options.channel_id,
            &options.user_id,
            &agent.id,
        )?;
        if crate::channel_session_transition_admission(
            &options.harness_home,
            &lane.exact_lane_digest(),
            &step.session_key,
        )? == crate::ChannelSessionAdmissionV1::HoldPendingNew
        {
            held_input.agent_id = Some(agent.id.clone());
            held_input.old_session_key = step.session_key.clone();
            held_input.inbound_canonical_id = inbound_canonical_id.clone();
            held_input.held_id = held_channel_message_id(&held_input);
            persist_held_channel_message_once(&options.harness_home, &held_input)?;
            let reason =
                "message durably held while the current /new session transition is pending"
                    .to_string();
            let receipt = ChannelReceiveReceipt {
                status: ChannelReceiveStatus::SessionTransitionPending,
                platform: options.platform.clone(),
                account_id: Some(account_id.clone()),
                channel_id: options.channel_id.clone(),
                user_id: options.user_id.clone(),
                session_key: step.session_key.clone(),
                queue_id: None,
                inbound_canonical_id: inbound_canonical_id.clone(),
                duplicate_sibling_of: None,
                outbound_count: 0,
                reason: reason.clone(),
            };
            append_json_line(&receipts_file, &receipt)?;
            append_harness_log(
                &options.harness_home,
                &HarnessLogEvent::new(
                    options.now_ms,
                    HarnessLogLevel::Info,
                    "channel",
                    "channel.receive.session-transition-pending",
                    reason,
                )
                .session_key(Some(step.session_key.clone()))
                .agent_id(Some(agent.id.clone()))
                .channel(
                    options.platform.clone(),
                    options.channel_id.clone(),
                    options.user_id.clone(),
                ),
            )?;
            return Ok(ChannelReceiveReport {
                schema: CHANNEL_RECEIVE_SCHEMA,
                harness_home: options.harness_home,
                platform: options.platform,
                account_id: Some(account_id),
                channel_id: options.channel_id,
                user_id: options.user_id,
                session_key: step.session_key,
                status: ChannelReceiveStatus::SessionTransitionPending,
                step_action: step.action,
                command_name,
                queue_id: None,
                inbound_canonical_id,
                duplicate_sibling_of: None,
                outbox_file,
                receipts_file,
                command_apply: None,
                queue_enqueue: None,
                outbound_messages: Vec::new(),
                receipt,
                warnings,
            });
        }
    }
    let mut claim_file = None;
    if let (Some(canonical_id), Some(event_kind), Some(event_id)) = (
        inbound_canonical_id.as_deref(),
        options.inbound_event_kind.as_deref(),
        options.inbound_event_id.as_deref(),
    ) {
        cleanup_expired_ingress_claims(
            &channel_state_dir,
            &options.harness_home,
            &options.platform,
            options.now_ms,
        )?;
        let path = ingress_claim_file(&channel_state_dir, &options.platform, canonical_id);
        match create_ingress_claim(
            &path,
            canonical_id,
            &options.platform,
            Some(account_id.clone()),
            &options.channel_id,
            &options.user_id,
            options.agent_id.clone(),
            event_kind,
            event_id,
            options.now_ms,
        ) {
            Ok(()) => claim_file = Some(path),
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                let duplicate_sibling_of = read_ingress_claim_queue_id(&path)?;
                let receipt = ChannelReceiveReceipt {
                    status: ChannelReceiveStatus::DuplicateSuppressed,
                    platform: options.platform,
                    account_id: Some(account_id.clone()),
                    channel_id: options.channel_id,
                    user_id: options.user_id,
                    session_key: step.session_key.clone(),
                    queue_id: None,
                    inbound_canonical_id: inbound_canonical_id.clone(),
                    duplicate_sibling_of: duplicate_sibling_of.clone(),
                    outbound_count: 0,
                    reason: "duplicate inbound provider event suppressed before queue effects"
                        .to_string(),
                };
                append_json_line(&receipts_file, &receipt)?;
                append_harness_log(
                    &options.harness_home,
                    &HarnessLogEvent::new(
                        options.now_ms,
                        HarnessLogLevel::Info,
                        "channel",
                        "channel.receive.duplicate",
                        receipt.reason.clone(),
                    )
                    .session_key(Some(step.session_key.clone()))
                    .agent_id(turn.agent.as_ref().map(|agent| agent.id.clone()))
                    .channel(
                        receipt.platform.clone(),
                        receipt.channel_id.clone(),
                        receipt.user_id.clone(),
                    ),
                )?;
                return Ok(ChannelReceiveReport {
                    schema: CHANNEL_RECEIVE_SCHEMA,
                    harness_home: options.harness_home,
                    platform: receipt.platform.clone(),
                    account_id: receipt.account_id.clone(),
                    channel_id: receipt.channel_id.clone(),
                    user_id: receipt.user_id.clone(),
                    session_key: step.session_key,
                    status: ChannelReceiveStatus::DuplicateSuppressed,
                    step_action: step.action,
                    command_name,
                    queue_id: None,
                    inbound_canonical_id,
                    duplicate_sibling_of,
                    outbox_file,
                    receipts_file,
                    command_apply: None,
                    queue_enqueue: None,
                    outbound_messages: Vec::new(),
                    receipt,
                    warnings,
                });
            }
            Err(error) => return Err(error),
        }
    }

    let (status, command_apply, queue_enqueue, outbound_messages, queue_id, reason) =
        match step.action {
            ChannelStepAction::ReplyOnly => {
                let apply = apply_channel_command_step(
                    &step,
                    ChannelCommandApplyOptions {
                        harness_home: options.harness_home.clone(),
                        now_ms: options.now_ms,
                    },
                )?;
                let mut outbound =
                    with_account_id(apply.outbound_messages.clone(), Some(account_id.clone()));
                warnings.extend(append_outbound_messages(
                    &options.harness_home,
                    &mut outbound,
                )?);
                (
                    ChannelReceiveStatus::CommandApplied,
                    Some(apply.clone()),
                    None,
                    outbound,
                    None,
                    "channel command applied and outbound reply recorded".to_string(),
                )
            }
            ChannelStepAction::EnqueueAgentTurn => {
                let queue = crate::enqueue_channel_step_v2(
                    &step,
                    crate::RuntimeQueueEnqueueOptionsV2 {
                        options: RuntimeQueueEnqueueOptions {
                            harness_home: options.harness_home.clone(),
                            runtime_workspace: options.runtime_workspace.clone(),
                            inbound_canonical_id: inbound_canonical_id.clone(),
                            now_ms: options.now_ms,
                        },
                    },
                )?;
                let queue_id = queue.receipt.queue_id.clone();
                if let (Some(path), Some(queue_id)) = (claim_file.as_ref(), queue_id.as_deref()) {
                    update_ingress_claim_queue_id(path, queue_id)?;
                }
                if let (Some(queue_id), Some(item)) = (queue_id.as_deref(), queue.item.as_ref()) {
                    if let Err(error) = record_latency_stage(
                        latency_receipts_file(&options.harness_home),
                        queue_id,
                        &item.runtime_class,
                        LatencyStage::InboundReceived,
                        Some(options.now_ms),
                    ) {
                        warnings.push(format!(
                            "failed to record inbound latency stage for `{queue_id}`: {error}"
                        ));
                    }
                }
                // The queue write is durable before this event is appended.  Emit a
                // user-visible root-lane status here rather than waiting for prompt
                // assembly, virtual-session evidence, or the backend process to
                // start.  A progress failure must not invalidate a successfully
                // accepted provider message, so preserve the turn and surface the
                // observability failure as a warning instead.
                if let Some(queue_id) = queue_id.as_deref() {
                    let progress_context = AgentProgressContext {
                        queue_id: queue_id.to_string(),
                        agent_id: turn.agent.as_ref().map(|agent| agent.id.clone()),
                        account_id: Some(account_id.clone()),
                        thread_id: None,
                        session_key: step.session_key.clone(),
                        platform: options.platform.clone(),
                        channel_id: options.channel_id.clone(),
                        user_id: options.user_id.clone(),
                    };
                    let event = AgentProgressEvent::new(
                        &progress_context,
                        AgentProgressKind::Runtime,
                        "queued",
                        "Queued; preparing your request.",
                        AgentProgressStatus::Started,
                        options.now_ms,
                    )
                    .lifecycle(crate::AgentProgressLifecycle::Queued)
                    .source("channel-ingress");
                    if let Err(error) = append_agent_progress_event(&options.harness_home, &event) {
                        warnings.push(format!(
                        "failed to append immediate queued progress event for `{queue_id}`: {error}"
                    ));
                    }
                    if let Err(error) = record_latency_stage(
                        latency_receipts_file(&options.harness_home),
                        queue_id,
                        "channel-ingress",
                        LatencyStage::QueueAccepted,
                        Some(options.now_ms),
                    ) {
                        warnings.push(format!(
                        "failed to record queue-accepted latency stage for `{queue_id}`: {error}"
                    ));
                    }
                }
                warnings.extend(queue.warnings.clone());
                (
                    ChannelReceiveStatus::AgentTurnQueued,
                    None,
                    Some(queue),
                    Vec::new(),
                    queue_id,
                    "agent turn queued for runtime worker".to_string(),
                )
            }
            ChannelStepAction::NoAgentAvailable => {
                let mut outbound =
                    with_account_id(step.outbound_messages.clone(), Some(account_id.clone()));
                warnings.extend(append_outbound_messages(
                    &options.harness_home,
                    &mut outbound,
                )?);
                (
                    ChannelReceiveStatus::ErrorReplied,
                    None,
                    None,
                    outbound,
                    None,
                    "channel error reply recorded".to_string(),
                )
            }
        };

    let receipt = ChannelReceiveReceipt {
        status,
        platform: options.platform,
        account_id: Some(account_id.clone()),
        channel_id: options.channel_id,
        user_id: options.user_id,
        session_key: step.session_key.clone(),
        queue_id: queue_id.clone(),
        inbound_canonical_id: inbound_canonical_id.clone(),
        duplicate_sibling_of: None,
        outbound_count: outbound_messages.len(),
        reason,
    };
    append_json_line(&receipts_file, &receipt)?;
    append_harness_log(
        &options.harness_home,
        &HarnessLogEvent::new(
            options.now_ms,
            match status {
                ChannelReceiveStatus::CommandApplied
                | ChannelReceiveStatus::AgentTurnQueued
                | ChannelReceiveStatus::SessionTransitionPending => HarnessLogLevel::Info,
                ChannelReceiveStatus::DuplicateSuppressed => HarnessLogLevel::Info,
                ChannelReceiveStatus::ErrorReplied => HarnessLogLevel::Warn,
                ChannelReceiveStatus::Skipped => HarnessLogLevel::Debug,
            },
            "channel",
            "channel.receive",
            receipt.reason.clone(),
        )
        .queue_id(queue_id.clone())
        .session_key(Some(step.session_key.clone()))
        .agent_id(turn.agent.as_ref().map(|agent| agent.id.clone()))
        .channel(
            receipt.platform.clone(),
            receipt.channel_id.clone(),
            receipt.user_id.clone(),
        ),
    )?;

    Ok(ChannelReceiveReport {
        schema: CHANNEL_RECEIVE_SCHEMA,
        harness_home: options.harness_home,
        platform: receipt.platform.clone(),
        account_id: receipt.account_id.clone(),
        channel_id: receipt.channel_id.clone(),
        user_id: receipt.user_id.clone(),
        session_key: step.session_key,
        status,
        step_action: step.action,
        command_name,
        queue_id,
        inbound_canonical_id,
        duplicate_sibling_of: None,
        outbox_file,
        receipts_file,
        command_apply,
        queue_enqueue,
        outbound_messages,
        receipt,
        warnings,
    })
}

fn append_outbound_messages(
    harness_home: &Path,
    messages: &mut [ChannelOutboundMessage],
) -> io::Result<Vec<String>> {
    let mut warnings = Vec::new();
    for message in messages.iter_mut() {
        let report = append_channel_outbox_message(harness_home, message)?;
        if let Some(warning) = report.index_warning {
            warnings.push(warning);
        }
    }
    if !messages.is_empty() {
        let wake_file = harness_home
            .join("state")
            .join("wake")
            .join("final-outbox.json");
        let _ = crate::wake::signal_wake(
            harness_home,
            wake_file,
            "final-outbox",
            "channel ingress outbound messages appended",
        );
    }
    Ok(warnings)
}

fn with_account_id(
    mut messages: Vec<ChannelOutboundMessage>,
    account_id: Option<String>,
) -> Vec<ChannelOutboundMessage> {
    if let Some(account_id) = account_id {
        for message in &mut messages {
            message.account_id = Some(account_id.clone());
        }
    }
    messages
}

fn append_json_line(path: &Path, value: &impl Serialize) -> io::Result<()> {
    crate::append_jsonl_value(path, value)
}

const MAX_HELD_CHANNEL_MESSAGES_PER_RECONCILE: usize = 128;

fn held_channel_messages_dir(harness_home: &Path) -> PathBuf {
    harness_home
        .join("state")
        .join("channels")
        .join("held-session-transition-messages")
}

fn held_channel_message_id(record: &HeldChannelMessageV1) -> String {
    let identity = record.inbound_canonical_id.clone().unwrap_or_else(|| {
        format!(
            "{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}",
            record.platform,
            record.account_id.as_deref().unwrap_or("default"),
            record.channel_id,
            record.user_id,
            record.received_at_ms,
            record.message
        )
    });
    format!("held:{}", fnv1a_64_hex(&identity))
}

fn held_channel_message_file(harness_home: &Path, held_id: &str) -> PathBuf {
    held_channel_messages_dir(harness_home).join(format!("{}.json", fnv1a_64_hex(held_id)))
}

fn persist_held_channel_message_once(
    harness_home: &Path,
    record: &HeldChannelMessageV1,
) -> io::Result<()> {
    if record.schema != HELD_CHANNEL_MESSAGE_SCHEMA
        || record.held_id != held_channel_message_id(record)
        || record.old_session_key.trim().is_empty()
        || record.agent_id.as_deref().is_none_or(str::is_empty)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "held channel message identity is incomplete",
        ));
    }
    fs::create_dir_all(held_channel_messages_dir(harness_home))?;
    let file = held_channel_message_file(harness_home, &record.held_id);
    if file.exists() {
        let existing: HeldChannelMessageV1 =
            serde_json::from_slice(&fs::read(&file)?).map_err(io::Error::other)?;
        // A provider retry can carry a later local receive timestamp. The
        // canonical provider identity owns this held row, so retain the first
        // durable payload instead of treating that timestamp drift as a
        // collision. Every behavior-bearing field must still match.
        if existing.schema != record.schema
            || existing.held_id != record.held_id
            || existing.source_home != record.source_home
            || existing.source_workspace != record.source_workspace
            || existing.runtime_workspace != record.runtime_workspace
            || existing.platform != record.platform
            || existing.account_id != record.account_id
            || existing.channel_id != record.channel_id
            || existing.user_id != record.user_id
            || existing.agent_id != record.agent_id
            || existing.old_session_key != record.old_session_key
            || existing.message != record.message
            || existing.inbound_context != record.inbound_context
            || existing.inbound_media_artifacts != record.inbound_media_artifacts
            || existing.inbound_event_kind != record.inbound_event_kind
            || existing.inbound_event_id != record.inbound_event_id
            || existing.inbound_canonical_id != record.inbound_canonical_id
            || existing.skill_limit != record.skill_limit
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "held channel message identity collision",
            ));
        }
        return Ok(());
    }
    crate::write_json_atomic(&file, record)
}

pub fn reconcile_held_channel_messages(
    harness_home: &Path,
    max_rows: usize,
    now_ms: i64,
) -> io::Result<HeldChannelMessageReconcileReport> {
    if max_rows == 0 || max_rows > MAX_HELD_CHANNEL_MESSAGES_PER_RECONCILE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "held channel message reconcile row cap is outside policy",
        ));
    }
    let dir = held_channel_messages_dir(harness_home);
    let entries = match fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(HeldChannelMessageReconcileReport {
                scanned: 0,
                replayed: 0,
                still_held: 0,
                failed: 0,
                queue_ids: Vec::new(),
                warnings: Vec::new(),
            });
        }
        Err(error) => return Err(error),
    };
    let mut records = Vec::new();
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let bytes = fs::read(entry.path())?;
        let record: HeldChannelMessageV1 =
            serde_json::from_slice(&bytes).map_err(io::Error::other)?;
        if record.schema != HELD_CHANNEL_MESSAGE_SCHEMA
            || record.held_id != held_channel_message_id(&record)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "held channel message failed schema or identity validation",
            ));
        }
        records.push((record, entry.path()));
    }
    records.sort_by(|left, right| {
        left.0
            .received_at_ms
            .cmp(&right.0.received_at_ms)
            .then_with(|| left.0.held_id.cmp(&right.0.held_id))
    });
    records.truncate(max_rows);

    let mut report = HeldChannelMessageReconcileReport {
        scanned: records.len(),
        replayed: 0,
        still_held: 0,
        failed: 0,
        queue_ids: Vec::new(),
        warnings: Vec::new(),
    };
    for (record, file) in records {
        let Some(agent_id) = record.agent_id.as_deref() else {
            report.failed += 1;
            report
                .warnings
                .push(format!("held message {} has no agent", record.held_id));
            continue;
        };
        let lane = crate::ChannelStateLane::new(
            &record.platform,
            record.account_id.as_deref(),
            &record.channel_id,
            &record.user_id,
            agent_id,
        )?;
        if crate::channel_session_transition_admission(
            harness_home,
            &lane.exact_lane_digest(),
            &record.old_session_key,
        )? == crate::ChannelSessionAdmissionV1::HoldPendingNew
        {
            report.still_held += 1;
            continue;
        }
        let source = AgentSource::with_workspace(&record.source_home, &record.source_workspace);
        let skill_index = crate::build_source_skill_index(&source)?;
        match receive_channel_message(ChannelReceiveOptions {
            source,
            runtime_workspace: record.runtime_workspace.clone(),
            harness_home: harness_home.to_path_buf(),
            skill_index,
            platform: record.platform.clone(),
            account_id: record.account_id.clone(),
            channel_id: record.channel_id.clone(),
            user_id: record.user_id.clone(),
            agent_id: record.agent_id.clone(),
            session_key: None,
            message: record.message.clone(),
            inbound_context: record.inbound_context.clone(),
            inbound_media_artifacts: record.inbound_media_artifacts.clone(),
            inbound_event_kind: record.inbound_event_kind.clone(),
            inbound_event_id: record.inbound_event_id.clone(),
            skill_limit: record.skill_limit,
            now_ms: now_ms.max(record.received_at_ms),
        }) {
            Ok(receive) if receive.status == ChannelReceiveStatus::SessionTransitionPending => {
                report.still_held += 1;
            }
            Ok(receive) => {
                if let Some(queue_id) = receive.queue_id {
                    report.queue_ids.push(queue_id);
                }
                fs::remove_file(&file)?;
                report.replayed += 1;
            }
            Err(error) => {
                report.failed += 1;
                report.warnings.push(format!(
                    "held message {} replay failed: {error}",
                    record.held_id
                ));
            }
        }
    }
    Ok(report)
}

fn inbound_canonical_id(
    platform: &str,
    account_id: Option<&str>,
    channel_id: &str,
    user_id: &str,
    agent_id: Option<&str>,
    event_kind: Option<&str>,
    event_id: Option<&str>,
) -> Option<String> {
    let event_kind = event_kind?.trim();
    let event_id = event_id?.trim();
    if event_kind.is_empty() || event_id.is_empty() {
        return None;
    }
    Some(format!(
        "{}:{}:{}:{}:{}:{}:{}",
        platform,
        account_id.unwrap_or("default"),
        channel_id,
        user_id,
        agent_id.unwrap_or("default"),
        event_kind,
        event_id
    ))
}

fn ingress_claim_file(
    channel_state_dir: &Path,
    platform: &str,
    inbound_canonical_id: &str,
) -> PathBuf {
    channel_state_dir
        .join("ingress-claims")
        .join(normalize_key_part(platform))
        .join(format!("{}.json", fnv1a_64_hex(inbound_canonical_id)))
}

#[allow(clippy::too_many_arguments)]
fn create_ingress_claim(
    path: &Path,
    inbound_canonical_id: &str,
    platform: &str,
    account_id: Option<String>,
    channel_id: &str,
    user_id: &str,
    agent_id: Option<String>,
    event_kind: &str,
    event_id: &str,
    now_ms: i64,
) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let record = IngressClaimRecord {
        schema: INGRESS_CLAIM_SCHEMA.to_string(),
        inbound_canonical_id: inbound_canonical_id.to_string(),
        platform: platform.to_string(),
        account_id,
        channel_id: channel_id.to_string(),
        user_id: user_id.to_string(),
        agent_id,
        event_kind: event_kind.to_string(),
        event_id: event_id.to_string(),
        first_seen_at_ms: now_ms,
        expires_at_ms: now_ms.saturating_add(INGRESS_CLAIM_TTL_MS),
        queue_id: None,
    };
    let bytes = serde_json::to_vec_pretty(&record).map_err(io::Error::other)?;
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?;
    std::io::Write::write_all(&mut file, &bytes)
}

fn read_ingress_claim_queue_id(path: &Path) -> io::Result<Option<String>> {
    let bytes = fs::read(path)?;
    let value: serde_json::Value = serde_json::from_slice(&bytes).map_err(io::Error::other)?;
    Ok(value
        .get("queueId")
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string))
}

fn update_ingress_claim_queue_id(path: &Path, queue_id: &str) -> io::Result<()> {
    let bytes = fs::read(path)?;
    let mut value: serde_json::Value = serde_json::from_slice(&bytes).map_err(io::Error::other)?;
    if let Some(object) = value.as_object_mut() {
        object.insert(
            "queueId".to_string(),
            serde_json::Value::String(queue_id.to_string()),
        );
    }
    crate::write_json_atomic(path, &value)
}

fn cleanup_expired_ingress_claims(
    channel_state_dir: &Path,
    harness_home: &Path,
    platform: &str,
    now_ms: i64,
) -> io::Result<()> {
    let claims_dir = channel_state_dir
        .join("ingress-claims")
        .join(normalize_key_part(platform));
    let entries = match fs::read_dir(&claims_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        let Ok(record) = serde_json::from_slice::<IngressClaimRecord>(&bytes) else {
            continue;
        };
        if now_ms.saturating_sub(record.first_seen_at_ms) <= INGRESS_CLAIM_TTL_MS {
            continue;
        }
        if let Some(queue_id) = record.queue_id.as_deref()
            && runtime_queue_id_active(harness_home, queue_id)?
        {
            continue;
        }
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn runtime_queue_id_active(harness_home: &Path, queue_id: &str) -> io::Result<bool> {
    let queue_dir = harness_home.join("state").join("runtime-queue");
    let mut warnings = Vec::new();
    if !pending_queue_id_exists_from_index(&queue_dir, queue_id, &mut warnings)? {
        return Ok(false);
    }
    let hot_index = refresh_runtime_queue_state_index(&queue_dir, &mut warnings)?;
    if terminal_run_once_ids_from_index(&hot_index).contains(queue_id) {
        return Ok(false);
    }
    let identifiers = std::collections::BTreeSet::from([queue_id.to_string()]);
    Ok(find_runtime_queue_terminal_history(&queue_dir, &identifiers)?.is_empty())
}

fn normalize_key_part(value: &str) -> String {
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

fn fnv1a_64_hex(value: &str) -> String {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend_reasoning::ReasoningPreference;
    use crate::{
        ChannelStateLane, build_source_skill_index,
        latency::{LatencyStage, latency_receipts_file, read_latest_queue_receipt},
        read_channel_session_state_v2,
    };
    use serde_json::Value;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn durable_queue_append_precedes_queued_progress_event() {
        let root = temp_root("durable_queue_append_precedes_queued_progress_event");
        let source = write_receive_source(&root);
        let harness_home = root.join(".agent-harness");
        let progress_file = crate::agent_progress_events_file(&harness_home);
        fs::create_dir_all(&progress_file).unwrap();
        let skills = build_source_skill_index(&source).unwrap();

        let report = receive_channel_message(ChannelReceiveOptions {
            source,
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "telegram".to_string(),
            account_id: Some("account-a".to_string()),
            channel_id: "dm".to_string(),
            user_id: "user".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "synthetic queued work".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: None,
            inbound_event_id: None,
            skill_limit: 3,
            now_ms: 1_000,
        })
        .unwrap();

        assert_eq!(report.status, ChannelReceiveStatus::AgentTurnQueued);
        assert!(report.warnings.iter().any(|warning| {
            warning.contains("failed to append immediate queued progress event")
        }));
        let pending = fs::read_to_string(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("pending.jsonl"),
        )
        .unwrap();
        assert!(pending.contains(report.queue_id.as_deref().unwrap()));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn channel_receive_applies_command_and_writes_outbox() {
        let root = temp_root("channel_receive_applies_command_and_writes_outbox");
        let source = write_receive_source(&root);
        let harness_home = root.join(".agent-harness");
        let skills = build_source_skill_index(&source).unwrap();

        let report = receive_channel_message(ChannelReceiveOptions {
            source: source.clone(),
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "telegram".to_string(),
            account_id: None,
            channel_id: "dm".to_string(),
            user_id: "user".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "/model openrouter/anthropic/claude-sonnet-4".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: None,
            inbound_event_id: None,
            skill_limit: 3,
            now_ms: 1000,
        })
        .unwrap();

        assert_eq!(report.status, ChannelReceiveStatus::CommandApplied);
        assert_eq!(report.command_name.as_deref(), Some("model"));
        assert_eq!(report.outbound_messages.len(), 1);
        assert!(report.outbox_file.is_file());
        let lane =
            ChannelStateLane::new("telegram", Some("default"), "dm", "user", "main").unwrap();
        let state = read_channel_session_state_v2(&harness_home, &lane)
            .unwrap()
            .unwrap();
        assert_eq!(state.model_override_provider.as_deref(), Some("openrouter"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn channel_receive_queues_agent_turn_with_state_override() {
        let root = temp_root("channel_receive_queues_agent_turn_with_state_override");
        let source = write_receive_source(&root);
        let harness_home = root.join(".agent-harness");
        let skills = build_source_skill_index(&source).unwrap();
        receive_channel_message(ChannelReceiveOptions {
            source: source.clone(),
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills.clone(),
            platform: "telegram".to_string(),
            account_id: None,
            channel_id: "dm".to_string(),
            user_id: "user".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "/model openrouter/anthropic/claude-sonnet-4".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: None,
            inbound_event_id: None,
            skill_limit: 3,
            now_ms: 1000,
        })
        .unwrap();

        let report = receive_channel_message(ChannelReceiveOptions {
            source,
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "telegram".to_string(),
            account_id: None,
            channel_id: "dm".to_string(),
            user_id: "user".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "continue".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: None,
            inbound_event_id: None,
            skill_limit: 3,
            now_ms: 1001,
        })
        .unwrap();

        assert_eq!(report.status, ChannelReceiveStatus::AgentTurnQueued);
        assert!(report.queue_id.is_some());
        let queue = report.queue_enqueue.unwrap();
        let item = queue.item.unwrap();
        assert_eq!(item.provider.as_deref(), Some("openrouter"));
        assert_eq!(item.model.as_deref(), Some("anthropic/claude-sonnet-4"));
        let events = fs::read_to_string(crate::agent_progress_events_file(&harness_home)).unwrap();
        let queued = events
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .find(|event| event["queueId"] == report.queue_id.as_deref().unwrap())
            .expect("an enqueued human turn emits an immediate queued progress event");
        assert_eq!(queued["status"], "started");
        assert_eq!(queued["kind"], "runtime");
        assert_eq!(queued["label"], "queued");
        assert_eq!(queued["agentId"], "main");
        let latency = read_latest_queue_receipt(
            latency_receipts_file(&harness_home),
            report.queue_id.as_deref().unwrap(),
        )
        .unwrap()
        .expect("an accepted inbound turn records its ingress timestamp");
        assert_eq!(
            latency.stages.get(&LatencyStage::InboundReceived).copied(),
            Some(1001)
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn unified_think_alias_max_is_command_only_agent_scoped_and_propagates_to_next_turn() {
        let root = temp_root(
            "unified_think_alias_max_is_command_only_agent_scoped_and_propagates_to_next_turn",
        );
        let source = write_reasoning_receive_source(&root);
        let harness_home = root.join(".agent-harness");
        write_reasoning_catalog(&harness_home);
        let skills = build_source_skill_index(&source).unwrap();
        let receive = |agent_id: &str, session_key: &str, message: &str, now_ms: i64| {
            receive_channel_message(ChannelReceiveOptions {
                source: source.clone(),
                runtime_workspace: None,
                harness_home: harness_home.clone(),
                skill_index: skills.clone(),
                platform: "discord".to_string(),
                account_id: Some("discord-main".to_string()),
                channel_id: "dm-42".to_string(),
                user_id: "user-7".to_string(),
                agent_id: Some(agent_id.to_string()),
                session_key: Some(session_key.to_string()),
                message: message.to_string(),
                inbound_context: None,
                inbound_media_artifacts: Vec::new(),
                inbound_event_kind: None,
                inbound_event_id: None,
                skill_limit: 3,
                now_ms,
            })
            .unwrap()
        };

        let think_max = receive("main", "discord:dm-42:user-7:main", "/think max", 1000);
        let reasoning_high = receive("main", "discord:dm-42:user-7:main", "/reasoning high", 1001);
        let reasoning_max = receive("main", "discord:dm-42:user-7:main", "/reasoning max", 1002);

        for report in [&think_max, &reasoning_high, &reasoning_max] {
            assert_eq!(report.status, ChannelReceiveStatus::CommandApplied);
            assert_eq!(report.step_action, ChannelStepAction::ReplyOnly);
            assert_eq!(report.command_name.as_deref(), Some("think"));
            assert!(report.queue_id.is_none());
            assert!(report.queue_enqueue.is_none());
        }
        assert!(
            !harness_home
                .join("state")
                .join("runtime-queue")
                .join("pending.jsonl")
                .exists(),
            "thinking commands must not enqueue model turns"
        );
        let lane =
            ChannelStateLane::new("discord", Some("discord-main"), "dm-42", "user-7", "main")
                .unwrap();
        let state = read_channel_session_state_v2(&harness_home, &lane)
            .unwrap()
            .unwrap();
        assert!(matches!(
            state.reasoning_preference,
            Some(ReasoningPreference::Explicit { ref effort }) if effort == "max"
        ));

        let sibling_turn = receive(
            "side",
            "discord:dm-42:user-7:side",
            "inspect independently",
            1003,
        );
        let main_turn = receive(
            "main",
            "discord:dm-42:user-7:main",
            "continue parent work",
            1004,
        );

        assert_eq!(sibling_turn.status, ChannelReceiveStatus::AgentTurnQueued);
        assert_eq!(main_turn.status, ChannelReceiveStatus::AgentTurnQueued);
        let sibling_item = sibling_turn.queue_enqueue.unwrap().item.unwrap();
        let main_item = main_turn.queue_enqueue.unwrap().item.unwrap();
        assert_eq!(sibling_item.agent_id, "side");
        assert!(!matches!(
            sibling_item.reasoning_preference,
            Some(ReasoningPreference::Explicit { ref effort }) if effort == "max"
        ));
        assert_eq!(main_item.agent_id, "main");
        assert!(matches!(
            main_item.reasoning_preference,
            Some(ReasoningPreference::Explicit { ref effort }) if effort == "max"
        ));
        assert_eq!(runtime_queue_line_count(&harness_home), 2);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn channel_receive_duplicate_inbound_canonical_id_has_no_second_queue_effect() {
        let root =
            temp_root("channel_receive_duplicate_inbound_canonical_id_has_no_second_queue_effect");
        let source = write_receive_source(&root);
        let harness_home = root.join(".agent-harness");
        let skills = build_source_skill_index(&source).unwrap();

        let first = receive_channel_message(ChannelReceiveOptions {
            source: source.clone(),
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills.clone(),
            platform: "discord".to_string(),
            account_id: Some("discord-main".to_string()),
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: Some("discord:dm-42:user-7:main".to_string()),
            message: "repair memory cron".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: Some("message".to_string()),
            inbound_event_id: Some("provider-msg-123".to_string()),
            skill_limit: 3,
            now_ms: 1000,
        })
        .unwrap();

        let duplicate = receive_channel_message(ChannelReceiveOptions {
            source,
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "discord".to_string(),
            account_id: Some("discord-main".to_string()),
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: Some("discord:dm-42:user-7:main".to_string()),
            message: "repair memory cron".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: Some("message".to_string()),
            inbound_event_id: Some("provider-msg-123".to_string()),
            skill_limit: 3,
            now_ms: 1001,
        })
        .unwrap();

        assert_eq!(first.status, ChannelReceiveStatus::AgentTurnQueued);
        assert_eq!(duplicate.status, ChannelReceiveStatus::DuplicateSuppressed);
        assert!(first.queue_id.is_some());
        assert!(duplicate.queue_id.is_none());
        assert_eq!(
            duplicate.duplicate_sibling_of.as_deref(),
            first.queue_id.as_deref()
        );
        assert_eq!(
            duplicate.inbound_canonical_id.as_deref(),
            first.inbound_canonical_id.as_deref()
        );

        let queue_lines = fs::read_to_string(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("pending.jsonl"),
        )
        .unwrap()
        .lines()
        .count();
        assert_eq!(queue_lines, 1);

        let receipt_text = fs::read_to_string(
            harness_home
                .join("state")
                .join("channels")
                .join("receive-receipts.jsonl"),
        )
        .unwrap();
        assert!(receipt_text.contains("\"status\":\"duplicate-suppressed\""));
        assert!(receipt_text.contains("\"inboundCanonicalId\""));
        assert!(receipt_text.contains("\"duplicateSiblingOf\""));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn discord_gateway_http_poll_same_message_single_queue_effect() {
        channel_receive_duplicate_inbound_canonical_id_has_no_second_queue_effect();
    }

    #[test]
    fn ingress_claim_create_new_first_writer_wins_under_race() {
        let root = temp_root("ingress_claim_create_new_first_writer_wins_under_race");
        let channel_state_dir = root.join(".agent-harness").join("state").join("channels");
        let canonical_id = inbound_canonical_id(
            "discord",
            Some("discord-main"),
            "dm-42",
            "user-7",
            Some("main"),
            Some("message"),
            Some("provider-msg-race"),
        )
        .unwrap();
        let claim_file = ingress_claim_file(&channel_state_dir, "discord", &canonical_id);

        create_ingress_claim(
            &claim_file,
            &canonical_id,
            "discord",
            Some("discord-main".to_string()),
            "dm-42",
            "user-7",
            Some("main".to_string()),
            "message",
            "provider-msg-race",
            1000,
        )
        .unwrap();
        let second = create_ingress_claim(
            &claim_file,
            &canonical_id,
            "discord",
            Some("discord-main".to_string()),
            "dm-42",
            "user-7",
            Some("main".to_string()),
            "message",
            "provider-msg-race",
            1001,
        )
        .unwrap_err();

        assert_eq!(second.kind(), ErrorKind::AlreadyExists);
        let record: IngressClaimRecord =
            serde_json::from_slice(&fs::read(&claim_file).unwrap()).unwrap();
        assert_eq!(record.inbound_canonical_id, canonical_id);
        assert_eq!(record.first_seen_at_ms, 1000);
        assert_eq!(record.expires_at_ms, 1000 + INGRESS_CLAIM_TTL_MS);
        assert_eq!(record.queue_id, None);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn duplicate_interaction_single_command_effect() {
        let root = temp_root("duplicate_interaction_single_command_effect");
        let source = write_receive_source(&root);
        let harness_home = root.join(".agent-harness");
        let skills = build_source_skill_index(&source).unwrap();

        let first = receive_channel_message(ChannelReceiveOptions {
            source: source.clone(),
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills.clone(),
            platform: "discord".to_string(),
            account_id: Some("discord-main".to_string()),
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: Some("discord:dm-42:user-7:main".to_string()),
            message: "/model openrouter/anthropic/claude-sonnet-4".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: Some("interaction".to_string()),
            inbound_event_id: Some("interaction-123".to_string()),
            skill_limit: 3,
            now_ms: 1000,
        })
        .unwrap();
        let duplicate = receive_channel_message(ChannelReceiveOptions {
            source,
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "discord".to_string(),
            account_id: Some("discord-main".to_string()),
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: Some("discord:dm-42:user-7:main".to_string()),
            message: "/model openai/gpt-5".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: Some("interaction".to_string()),
            inbound_event_id: Some("interaction-123".to_string()),
            skill_limit: 3,
            now_ms: 1001,
        })
        .unwrap();

        assert_eq!(first.status, ChannelReceiveStatus::CommandApplied);
        assert_eq!(duplicate.status, ChannelReceiveStatus::DuplicateSuppressed);
        assert!(first.command_apply.is_some());
        assert!(duplicate.command_apply.is_none());
        assert_eq!(
            duplicate.inbound_canonical_id.as_deref(),
            first.inbound_canonical_id.as_deref()
        );
        let lane =
            ChannelStateLane::new("discord", Some("discord-main"), "dm-42", "user-7", "main")
                .unwrap();
        let state = read_channel_session_state_v2(&harness_home, &lane)
            .unwrap()
            .unwrap();
        assert_eq!(state.model_override_provider.as_deref(), Some("openrouter"));
        assert_eq!(
            fs::read_to_string(first.outbox_file)
                .unwrap()
                .lines()
                .count(),
            1
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn canonical_id_persisted_receive_to_queue_receipts() {
        channel_receive_duplicate_inbound_canonical_id_has_no_second_queue_effect();
    }

    #[test]
    fn claim_ttl_cleanup_does_not_release_active_claims() {
        let root = temp_root("claim_ttl_cleanup_does_not_release_active_claims");
        let source = write_receive_source(&root);
        let harness_home = root.join(".agent-harness");
        let skills = build_source_skill_index(&source).unwrap();
        let first_seen_ms = 1000;

        let first = receive_channel_message(ChannelReceiveOptions {
            source: source.clone(),
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills.clone(),
            platform: "discord".to_string(),
            account_id: Some("discord-main".to_string()),
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: Some("discord:dm-42:user-7:main".to_string()),
            message: "repair memory cron".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: Some("message".to_string()),
            inbound_event_id: Some("provider-msg-ttl".to_string()),
            skill_limit: 3,
            now_ms: first_seen_ms,
        })
        .unwrap();

        let active_duplicate = receive_channel_message(ChannelReceiveOptions {
            source: source.clone(),
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills.clone(),
            platform: "discord".to_string(),
            account_id: Some("discord-main".to_string()),
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: Some("discord:dm-42:user-7:main".to_string()),
            message: "repair memory cron".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: Some("message".to_string()),
            inbound_event_id: Some("provider-msg-ttl".to_string()),
            skill_limit: 3,
            now_ms: first_seen_ms + INGRESS_CLAIM_TTL_MS + 1,
        })
        .unwrap();

        assert_eq!(first.status, ChannelReceiveStatus::AgentTurnQueued);
        assert_eq!(
            active_duplicate.status,
            ChannelReceiveStatus::DuplicateSuppressed
        );
        assert_eq!(
            active_duplicate.duplicate_sibling_of.as_deref(),
            first.queue_id.as_deref()
        );
        assert_eq!(
            runtime_queue_line_count(&harness_home),
            1,
            "active expired claim must still suppress duplicate queue effects"
        );

        let queue_dir = harness_home.join("state").join("runtime-queue");
        fs::write(
            queue_dir.join("run-once-receipts.jsonl"),
            format!(
                "{{\"queueId\":\"{}\",\"status\":\"completed\"}}\n",
                first.queue_id.as_deref().unwrap()
            ),
        )
        .unwrap();

        let after_terminal_ttl = receive_channel_message(ChannelReceiveOptions {
            source,
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "discord".to_string(),
            account_id: Some("discord-main".to_string()),
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: Some("discord:dm-42:user-7:main".to_string()),
            message: "repair memory cron".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: Some("message".to_string()),
            inbound_event_id: Some("provider-msg-ttl".to_string()),
            skill_limit: 3,
            now_ms: first_seen_ms + INGRESS_CLAIM_TTL_MS + 2,
        })
        .unwrap();

        assert_eq!(
            after_terminal_ttl.status,
            ChannelReceiveStatus::AgentTurnQueued
        );
        assert_eq!(runtime_queue_line_count(&harness_home), 2);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn channel_receive_same_provider_event_id_different_agent_is_not_suppressed() {
        let root =
            temp_root("channel_receive_same_provider_event_id_different_agent_is_not_suppressed");
        let source = write_receive_source(&root);
        let harness_home = root.join(".agent-harness");
        let skills = build_source_skill_index(&source).unwrap();

        let first = receive_channel_message(ChannelReceiveOptions {
            source: source.clone(),
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills.clone(),
            platform: "discord".to_string(),
            account_id: Some("discord-main".to_string()),
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: Some("discord:dm-42:user-7:main".to_string()),
            message: "repair memory cron".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: Some("message".to_string()),
            inbound_event_id: Some("provider-msg-123".to_string()),
            skill_limit: 3,
            now_ms: 1000,
        })
        .unwrap();

        let side_agent = receive_channel_message(ChannelReceiveOptions {
            source,
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "discord".to_string(),
            account_id: Some("discord-main".to_string()),
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("side".to_string()),
            session_key: Some("discord:dm-42:user-7:side".to_string()),
            message: "repair memory cron".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            inbound_event_kind: Some("message".to_string()),
            inbound_event_id: Some("provider-msg-123".to_string()),
            skill_limit: 3,
            now_ms: 1001,
        })
        .unwrap();

        assert_eq!(first.status, ChannelReceiveStatus::AgentTurnQueued);
        assert_eq!(side_agent.status, ChannelReceiveStatus::AgentTurnQueued);
        assert!(first.queue_id.is_some());
        assert!(side_agent.queue_id.is_some());
        assert_ne!(
            first.inbound_canonical_id.as_deref(),
            side_agent.inbound_canonical_id.as_deref()
        );

        let queue_lines = fs::read_to_string(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("pending.jsonl"),
        )
        .unwrap()
        .lines()
        .count();
        assert_eq!(queue_lines, 2);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn pending_new_holds_provider_messages_once_and_replays_them_fifo() {
        let root = temp_root("pending_new_holds_provider_messages_once_and_replays_them_fifo");
        let source = write_receive_source(&root);
        let harness_home = root.join(".agent-harness");
        let skills = build_source_skill_index(&source).unwrap();
        let registry = load_agent_registry(&source).unwrap();
        let planned = build_turn_plan_for_account(
            &source,
            &registry,
            &skills,
            TurnPlanInput {
                harness_home: Some(harness_home.clone()),
                platform: "discord".to_string(),
                channel_id: "dm-42".to_string(),
                user_id: "user-7".to_string(),
                text: "held first".to_string(),
                inbound_context: None,
                inbound_media_artifacts: Vec::new(),
                requested_agent_id: Some("main".to_string()),
                session_hint: Some("discord:dm-42:user-7:main".to_string()),
                skill_limit: 3,
            },
            Some("discord-main".to_string()),
        )
        .unwrap();
        let old_session_key = planned.session_key.clone();
        let lane =
            ChannelStateLane::new("discord", Some("discord-main"), "dm-42", "user-7", "main")
                .unwrap();
        let transition = crate::prepare_channel_session_transition(
            crate::PrepareChannelSessionTransitionOptionsV1 {
                harness_home: harness_home.clone(),
                command: crate::ChannelSessionTransitionCommandV1::New,
                lane_digest: lane.exact_lane_digest(),
                old_session_key: old_session_key.clone(),
                proposed_new_session_key: Some(format!("{old_session_key}:new")),
                topic: None,
                reason: Some("test pending boundary".to_string()),
                now_ms: 900,
            },
        )
        .unwrap();

        let receive = |event_id: &str, message: &str, now_ms: i64| {
            receive_channel_message(ChannelReceiveOptions {
                source: source.clone(),
                runtime_workspace: None,
                harness_home: harness_home.clone(),
                skill_index: skills.clone(),
                platform: "discord".to_string(),
                account_id: Some("discord-main".to_string()),
                channel_id: "dm-42".to_string(),
                user_id: "user-7".to_string(),
                agent_id: Some("main".to_string()),
                session_key: Some(old_session_key.clone()),
                message: message.to_string(),
                inbound_context: None,
                inbound_media_artifacts: Vec::new(),
                inbound_event_kind: Some("message".to_string()),
                inbound_event_id: Some(event_id.to_string()),
                skill_limit: 3,
                now_ms,
            })
            .unwrap()
        };
        assert_eq!(
            receive("provider-held-1", "held first", 1_000).status,
            ChannelReceiveStatus::SessionTransitionPending
        );
        assert_eq!(
            receive("provider-held-2", "held second", 1_001).status,
            ChannelReceiveStatus::SessionTransitionPending
        );
        // Provider retry owns the same held row even though its local receive
        // timestamp is different.
        assert_eq!(
            receive("provider-held-1", "held first", 1_100).status,
            ChannelReceiveStatus::SessionTransitionPending
        );
        assert_eq!(
            fs::read_dir(held_channel_messages_dir(&harness_home))
                .unwrap()
                .count(),
            2
        );
        assert!(
            !harness_home
                .join("state")
                .join("runtime-queue")
                .join("pending.jsonl")
                .exists()
        );

        crate::record_channel_session_transition_phase(
            &harness_home,
            &transition.intent,
            crate::ChannelSessionTransitionPhaseV1::BoundaryCommitted,
            "session state boundary committed",
            1_200,
        )
        .unwrap();
        let replay = reconcile_held_channel_messages(&harness_home, 8, 1_300).unwrap();
        assert_eq!(replay.scanned, 2);
        assert_eq!(replay.replayed, 2);
        assert_eq!(replay.still_held, 0);
        assert_eq!(replay.failed, 0);
        assert_eq!(replay.queue_ids.len(), 2);
        assert_eq!(
            fs::read_dir(held_channel_messages_dir(&harness_home))
                .unwrap()
                .count(),
            0
        );
        let queued = fs::read_to_string(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("pending.jsonl"),
        )
        .unwrap();
        assert!(queued.find("held first").unwrap() < queued.find("held second").unwrap());

        let _ = fs::remove_dir_all(root);
    }

    fn runtime_queue_line_count(harness_home: &Path) -> usize {
        fs::read_to_string(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("pending.jsonl"),
        )
        .unwrap()
        .lines()
        .count()
    }

    fn write_receive_source(root: &Path) -> AgentSource {
        let home = root.join(".openclaw");
        let workspace = home.join("workspace");
        let skill = workspace.join("skills").join("handoff");
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(&skill).unwrap();
        fs::create_dir_all(home.join("agents").join("main").join("sessions")).unwrap();
        fs::write(workspace.join("AGENTS.md"), "# Agent").unwrap();
        fs::write(skill.join(crate::SKILL_FILE_NAME), "# Handoff").unwrap();
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
              "plugins": [
                { "id": "telegram", "enabled": true },
                { "id": "discord", "enabled": true }
              ]
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

    fn write_reasoning_receive_source(root: &Path) -> AgentSource {
        let home = root.join(".openclaw");
        let workspace = home.join("workspace");
        fs::create_dir_all(&workspace).unwrap();
        for agent_id in ["main", "side"] {
            fs::create_dir_all(home.join("agents").join(agent_id).join("sessions")).unwrap();
            fs::write(
                home.join("agents")
                    .join(agent_id)
                    .join("sessions")
                    .join("sessions.json"),
                "{}",
            )
            .unwrap();
        }
        fs::write(workspace.join("AGENTS.md"), "# Agent").unwrap();
        fs::write(
            home.join("openclaw.json"),
            r#"{
              "agents": {
                "defaults": { "provider": "openai", "model": "gpt-5.6-sol" },
                "list": [
                  { "id": "main", "model": "gpt-5.6-sol", "enabled": true },
                  { "id": "side", "model": "gpt-5.6-sol", "enabled": true }
                ]
              },
              "models": {
                "providers": {
                  "openai": { "apiKey": "${OPENAI_API_KEY}" }
                }
              },
              "plugins": [
                { "id": "discord", "enabled": true }
              ]
            }"#,
        )
        .unwrap();
        AgentSource::with_workspace(home, workspace)
    }

    fn write_reasoning_catalog(harness_home: &Path) {
        let codex_home = harness_home.join("codex-home");
        fs::create_dir_all(&codex_home).unwrap();
        fs::write(
            codex_home.join("models_cache.json"),
            r#"{
              "models": [
                {
                  "slug": "gpt-5.6-sol",
                  "default_reasoning_level": "medium",
                  "supported_reasoning_levels": [
                    { "effort": "low" },
                    { "effort": "medium" },
                    { "effort": "high" },
                    { "effort": "xhigh" },
                    { "effort": "max" },
                    { "effort": "ultra" }
                  ],
                  "service_tiers": [],
                  "comp_hash": "ingress-test"
                }
              ]
            }"#,
        )
        .unwrap();
        fs::create_dir_all(harness_home).unwrap();
        fs::write(
            harness_home.join(crate::HARNESS_CONFIG_FILE_NAME),
            r#"{
              "orchestration": {
                "features": {
                  "modelCatalogV2": {
                    "mode": "authoritative",
                    "enabledAgentIds": ["main", "side"]
                  }
                }
              }
            }"#,
        )
        .unwrap();
    }

    #[test]
    fn historical_terminal_queue_is_not_kept_active_by_a_stale_pending_claim() {
        let root = temp_root("historical_terminal_queue_is_not_kept_active");
        let harness_home = root.join(".agent-harness");
        let queue_dir = harness_home.join("state").join("runtime-queue");
        fs::create_dir_all(&queue_dir).unwrap();
        let staged = crate::runtime_receipt_history::stage_runtime_queue_receipt_history(
            &queue_dir,
            "ingress-history",
            br#"{"queueId":"queue-terminal","status":"completed","reason":"terminal"}
"#,
            b"",
            &std::collections::HashSet::new(),
            100,
        )
        .unwrap();
        crate::runtime_receipt_history::commit_runtime_queue_receipt_history(&staged, 101).unwrap();
        fs::write(
            queue_dir.join("pending.jsonl"),
            [
                serde_json::json!({"queueId":"queue-terminal","status":"queued"}).to_string(),
                serde_json::json!({"queueId":"other-queue","status":"queued"}).to_string(),
            ]
            .join("\n")
                + "\n",
        )
        .unwrap();
        fs::write(queue_dir.join("run-once-receipts.jsonl"), "").unwrap();

        assert!(!runtime_queue_id_active(&harness_home, "queue-terminal").unwrap());
        assert!(
            runtime_queue_id_active(&harness_home, "other-queue").unwrap(),
            "an unrelated current pending queue must remain active"
        );

        let _ = fs::remove_dir_all(root);
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-channel-ingress-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
