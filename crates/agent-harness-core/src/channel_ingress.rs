use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    AgentSource, ChannelCommandApplyOptions, ChannelCommandApplyReport, ChannelOutboundMessage,
    ChannelStepAction, HarnessLogEvent, HarnessLogLevel, InboundMediaArtifact,
    RuntimeQueueEnqueueOptions, RuntimeQueueEnqueueReport, SkillIndex, TurnPlanInput,
    append_harness_log, apply_channel_command_step, build_channel_step, build_turn_plan,
    enqueue_channel_step, load_agent_registry,
};

const CHANNEL_RECEIVE_SCHEMA: &str = "agent-harness.channel-receive.v1";
const INGRESS_CLAIM_SCHEMA: &str = "agent-harness.ingress-claim.v1";
const INGRESS_CLAIM_TTL_MS: i64 = 24 * 60 * 60 * 1000;
const RUNTIME_TERMINAL_STATUSES: &[&str] = &[
    "completed",
    "timeout",
    "failed-terminal",
    "canceled",
    "skipped",
    "dead-letter",
    "suppressed",
];

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

pub fn receive_channel_message(options: ChannelReceiveOptions) -> io::Result<ChannelReceiveReport> {
    let channel_state_dir = options.harness_home.join("state").join("channels");
    let outbox_file = channel_state_dir.join("outbox.jsonl");
    let receipts_file = channel_state_dir.join("receive-receipts.jsonl");
    fs::create_dir_all(&channel_state_dir)?;

    let registry = load_agent_registry(&options.source)?;
    let turn = build_turn_plan(
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
    )?;
    let mut step = build_channel_step(&registry, &turn);
    step.account_id = options.account_id.clone();
    let command_name = turn
        .command
        .as_ref()
        .map(|command| command.name().to_string());
    let mut warnings = step.warnings.clone();
    let inbound_canonical_id = inbound_canonical_id(
        &options.platform,
        options.account_id.as_deref(),
        &options.channel_id,
        &options.user_id,
        options.agent_id.as_deref(),
        options.inbound_event_kind.as_deref(),
        options.inbound_event_id.as_deref(),
    );
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
            options.account_id.clone(),
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
                    account_id: options.account_id.clone(),
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
                let outbound =
                    with_account_id(apply.outbound_messages.clone(), options.account_id.clone());
                append_outbound_messages(&options.harness_home, &outbox_file, &outbound)?;
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
                let queue = enqueue_channel_step(
                    &step,
                    RuntimeQueueEnqueueOptions {
                        harness_home: options.harness_home.clone(),
                        runtime_workspace: options.runtime_workspace.clone(),
                        inbound_canonical_id: inbound_canonical_id.clone(),
                        now_ms: options.now_ms,
                    },
                )?;
                let queue_id = queue.receipt.queue_id.clone();
                if let (Some(path), Some(queue_id)) = (claim_file.as_ref(), queue_id.as_deref()) {
                    update_ingress_claim_queue_id(path, queue_id)?;
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
                let outbound =
                    with_account_id(step.outbound_messages.clone(), options.account_id.clone());
                append_outbound_messages(&options.harness_home, &outbox_file, &outbound)?;
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
        account_id: options.account_id.clone(),
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
                ChannelReceiveStatus::CommandApplied | ChannelReceiveStatus::AgentTurnQueued => {
                    HarnessLogLevel::Info
                }
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
    path: &Path,
    messages: &[ChannelOutboundMessage],
) -> io::Result<()> {
    for message in messages {
        append_json_line(path, message)?;
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
    Ok(())
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
    if !runtime_queue_pending_contains(&queue_dir.join("pending.jsonl"), queue_id)? {
        return Ok(false);
    }
    let terminal_ids = read_runtime_receipt_queue_ids(
        &queue_dir.join("run-once-receipts.jsonl"),
        RUNTIME_TERMINAL_STATUSES,
    )?;
    Ok(!terminal_ids.contains(queue_id))
}

fn runtime_queue_pending_contains(path: &Path, queue_id: &str) -> io::Result<bool> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
            continue;
        };
        if queue_id_from_value(&value).as_deref() == Some(queue_id) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn read_runtime_receipt_queue_ids(path: &Path, statuses: &[&str]) -> io::Result<BTreeSet<String>> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(BTreeSet::new()),
        Err(error) => return Err(error),
    };
    let mut queue_ids = BTreeSet::new();
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
            continue;
        };
        let status = value.get("status").and_then(Value::as_str);
        if status.is_some_and(|status| statuses.contains(&status))
            && let Some(queue_id) = queue_id_from_value(&value)
        {
            queue_ids.insert(queue_id);
        }
    }
    Ok(queue_ids)
}

fn queue_id_from_value(value: &Value) -> Option<String> {
    value
        .get("queueId")
        .or_else(|| value.get("queue_id"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
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
    use crate::{build_source_skill_index, read_channel_session_state};
    use std::time::{SystemTime, UNIX_EPOCH};

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
        let state = read_channel_session_state(&harness_home, "telegram", "dm", "user")
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
            harness_home,
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
        let state = read_channel_session_state(&harness_home, "discord", "dm-42", "user-7")
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
