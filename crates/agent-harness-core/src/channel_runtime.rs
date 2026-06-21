use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    AgentRegistry, ChannelCommandIntent, DEFAULT_THINKING_LEVEL, InboundMediaArtifact,
    THINKING_LEVELS, TurnDispatch, TurnPlan, XHIGH_THINKING_LEVEL, inspect_codex_approval_policy,
    inspect_codex_sandbox, inspect_codex_sandbox_policy, normalize_thinking_level,
};

const CHANNEL_STEP_SCHEMA: &str = "agent-harness.channel-step.v1";
const CHANNEL_RESTART_REQUEST_SCHEMA: &str = "agent-harness.channel-restart-request.v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelStep {
    pub schema: &'static str,
    pub source_home: PathBuf,
    pub source_workspace: PathBuf,
    pub platform: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_id: Option<String>,
    pub channel_id: String,
    pub user_id: String,
    pub message_text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inbound_context: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub inbound_media_artifacts: Vec<InboundMediaArtifact>,
    pub session_key: String,
    pub action: ChannelStepAction,
    pub command_effect: Option<ChannelCommandEffect>,
    pub agent_turn: Option<ChannelAgentTurnDispatch>,
    pub outbound_messages: Vec<ChannelOutboundMessage>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ChannelStepAction {
    ReplyOnly,
    EnqueueAgentTurn,
    NoAgentAvailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase", tag = "kind")]
pub enum ChannelCommandEffect {
    StartNewSession {
        topic: Option<String>,
        new_session_key: String,
    },
    StopCurrentRun {
        reason: Option<String>,
    },
    RestartChannel {
        target: Option<String>,
        platform: String,
        service_id: Option<String>,
        status: String,
        detail: String,
        reason: Option<String>,
        stop_file: Option<PathBuf>,
        request_file: Option<PathBuf>,
        receipt_file: Option<PathBuf>,
    },
    AddSteering {
        instruction: String,
    },
    AddBtwNote {
        note: String,
    },
    ShowThinking {
        agent_id: Option<String>,
        provider: Option<String>,
        model: Option<String>,
        thinking_enabled: bool,
        current_level: Option<String>,
        available_levels: Vec<String>,
    },
    SwitchThinking {
        agent_id: Option<String>,
        provider: Option<String>,
        model: Option<String>,
        thinking_enabled: bool,
        current_level: Option<String>,
        level: String,
        global: bool,
        valid: bool,
        available_levels: Vec<String>,
    },
    ShowModel {
        agent_id: Option<String>,
        current_provider: Option<String>,
        current_model: Option<String>,
        providers: Vec<String>,
    },
    ListProviderModels {
        agent_id: Option<String>,
        current_provider: Option<String>,
        current_model: Option<String>,
        provider: String,
        provider_known: bool,
        models: Vec<String>,
    },
    SwitchModel {
        agent_id: Option<String>,
        provider: String,
        model: String,
        global: bool,
        current_provider: Option<String>,
        current_model: Option<String>,
        provider_known: bool,
        model_known: bool,
    },
    ShowStatus {
        scope: Option<String>,
        snapshot: ChannelStatusSnapshot,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelStatusSnapshot {
    pub scope: Option<String>,
    pub platform: String,
    pub session_key: String,
    pub agents_total: usize,
    pub agents_enabled: usize,
    pub providers_total: usize,
    pub plugins_total: usize,
    pub telegram_configured: bool,
    pub discord_configured: bool,
    pub current_agent_id: Option<String>,
    pub agent_directory_exists: bool,
    pub agent_sessions_index_exists: bool,
    pub current_provider: Option<String>,
    pub current_model: Option<String>,
    pub model_override: Option<String>,
    pub codex_approval_policy: Option<String>,
    pub codex_sandbox: Option<String>,
    pub codex_sandbox_policy: Option<String>,
    pub prompt_files_present: usize,
    pub prompt_files_total: usize,
    pub prompt_file_names: Vec<String>,
    pub selected_skills: usize,
    pub selected_skill_ids: Vec<String>,
    pub channel_state_loaded: bool,
    pub active_session_key: Option<String>,
    pub thinking_enabled: bool,
    pub thinking_level: Option<String>,
    pub steering_notes: usize,
    pub btw_notes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelAgentTurnDispatch {
    pub agent_id: String,
    pub session_key: String,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub prompt_files_present: usize,
    pub prompt_files_total: usize,
    pub selected_skill_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelOutboundMessage {
    pub platform: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_id: Option<String>,
    pub channel_id: String,
    pub user_id: String,
    pub session_key: String,
    pub kind: ChannelOutboundMessageKind,
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery_intent: Option<ChannelDeliveryIntent>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<ChannelOutboundAttachment>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelDeliveryIntent {
    #[serde(default = "default_delivery_intent_schema")]
    pub schema: String,
    pub kind: ChannelDeliveryIntentKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub platform_message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub platform_channel_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub platform_thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quote_text: Option<String>,
    #[serde(default)]
    pub validated: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub downgrade_reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ChannelDeliveryIntentKind {
    Direct,
    ReplyToMessage,
    QuoteReply,
    ThreadReply,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelOutboundAttachment {
    pub kind: ChannelOutboundAttachmentKind,
    pub path: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub caption: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ChannelOutboundAttachmentKind {
    Image,
    Document,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ChannelOutboundMessageKind {
    CommandReply,
    AgentReply,
    ErrorReply,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelStepFile {
    pub json: PathBuf,
}

pub fn build_channel_step(registry: &AgentRegistry, turn: &TurnPlan) -> ChannelStep {
    let mut warnings = turn.warnings.clone();
    match turn.dispatch {
        TurnDispatch::ChannelCommand => build_command_step(registry, turn, &mut warnings)
            .unwrap_or_else(|| {
                warnings.push("channel command dispatch had no parsed command intent".to_string());
                error_step(turn, warnings, "Command could not be parsed.")
            }),
        TurnDispatch::AgentTurn => build_agent_step(turn, warnings),
        TurnDispatch::NoAgentAvailable => error_step(
            turn,
            warnings,
            "No harness agent is available for this channel message.",
        ),
    }
}

pub fn write_channel_step(
    step: &ChannelStep,
    output_dir: impl AsRef<Path>,
) -> io::Result<ChannelStepFile> {
    let output_dir = output_dir.as_ref();
    fs::create_dir_all(output_dir)?;
    let json = output_dir.join("channel-step.json");
    let text = serde_json::to_string_pretty(step).map_err(io::Error::other)?;
    fs::write(&json, text)?;
    Ok(ChannelStepFile { json })
}

fn build_command_step(
    registry: &AgentRegistry,
    turn: &TurnPlan,
    warnings: &mut Vec<String>,
) -> Option<ChannelStep> {
    let intent = turn.command_intent.clone()?;
    let effect = command_effect(registry, turn, intent, warnings);
    let text = command_reply_text(&effect);
    Some(ChannelStep {
        schema: CHANNEL_STEP_SCHEMA,
        source_home: turn.source_home.clone(),
        source_workspace: turn.source_workspace.clone(),
        platform: turn.platform.clone(),
        account_id: None,
        channel_id: turn.channel_id.clone(),
        user_id: turn.user_id.clone(),
        message_text: turn.message_text.clone(),
        inbound_context: turn.inbound_context.clone(),
        inbound_media_artifacts: turn.inbound_media_artifacts.clone(),
        session_key: turn.session_key.clone(),
        action: ChannelStepAction::ReplyOnly,
        command_effect: Some(effect),
        agent_turn: None,
        outbound_messages: vec![outbound(
            turn,
            ChannelOutboundMessageKind::CommandReply,
            text,
        )],
        warnings: warnings.clone(),
    })
}

fn build_agent_step(turn: &TurnPlan, warnings: Vec<String>) -> ChannelStep {
    let agent_turn = turn.agent.as_ref().map(|agent| ChannelAgentTurnDispatch {
        agent_id: agent.id.clone(),
        session_key: turn.session_key.clone(),
        provider: turn.model_policy.provider.clone(),
        model: turn.model_policy.model.clone(),
        prompt_files_present: prompt_files_present(turn),
        prompt_files_total: turn.prompt_files.len(),
        selected_skill_ids: turn
            .selected_skills
            .iter()
            .map(|skill| skill.skill_id.clone())
            .collect(),
    });
    ChannelStep {
        schema: CHANNEL_STEP_SCHEMA,
        source_home: turn.source_home.clone(),
        source_workspace: turn.source_workspace.clone(),
        platform: turn.platform.clone(),
        account_id: None,
        channel_id: turn.channel_id.clone(),
        user_id: turn.user_id.clone(),
        message_text: turn.message_text.clone(),
        inbound_context: turn.inbound_context.clone(),
        inbound_media_artifacts: turn.inbound_media_artifacts.clone(),
        session_key: turn.session_key.clone(),
        action: ChannelStepAction::EnqueueAgentTurn,
        command_effect: None,
        agent_turn,
        outbound_messages: Vec::new(),
        warnings,
    }
}

fn error_step(turn: &TurnPlan, warnings: Vec<String>, text: &str) -> ChannelStep {
    ChannelStep {
        schema: CHANNEL_STEP_SCHEMA,
        source_home: turn.source_home.clone(),
        source_workspace: turn.source_workspace.clone(),
        platform: turn.platform.clone(),
        account_id: None,
        channel_id: turn.channel_id.clone(),
        user_id: turn.user_id.clone(),
        message_text: turn.message_text.clone(),
        inbound_context: turn.inbound_context.clone(),
        inbound_media_artifacts: turn.inbound_media_artifacts.clone(),
        session_key: turn.session_key.clone(),
        action: ChannelStepAction::NoAgentAvailable,
        command_effect: None,
        agent_turn: None,
        outbound_messages: vec![outbound(
            turn,
            ChannelOutboundMessageKind::ErrorReply,
            text.to_string(),
        )],
        warnings,
    }
}

fn command_effect(
    registry: &AgentRegistry,
    turn: &TurnPlan,
    intent: ChannelCommandIntent,
    warnings: &mut Vec<String>,
) -> ChannelCommandEffect {
    match intent {
        ChannelCommandIntent::StartNewSession { topic } => ChannelCommandEffect::StartNewSession {
            topic,
            new_session_key: new_session_key(turn),
        },
        ChannelCommandIntent::Think { level, global } => {
            thinking_command_effect(turn, level, global)
        }
        ChannelCommandIntent::StopCurrentRun { reason } => {
            ChannelCommandEffect::StopCurrentRun { reason }
        }
        ChannelCommandIntent::RestartChannel { target, reason } => {
            restart_channel_effect(turn, target, reason, warnings)
        }
        ChannelCommandIntent::AddSteering { instruction } => {
            ChannelCommandEffect::AddSteering { instruction }
        }
        ChannelCommandIntent::AddBtwNote { note } => ChannelCommandEffect::AddBtwNote { note },
        ChannelCommandIntent::Model { target, global } => {
            model_command_effect(registry, turn, target, global)
        }
        ChannelCommandIntent::ShowStatus { scope } => ChannelCommandEffect::ShowStatus {
            snapshot: status_snapshot(registry, turn, scope.clone()),
            scope,
        },
    }
}

fn restart_channel_effect(
    turn: &TurnPlan,
    target: Option<String>,
    reason: Option<String>,
    warnings: &mut Vec<String>,
) -> ChannelCommandEffect {
    let Some(platform) = restart_target_platform(turn, target.as_deref()) else {
        return ChannelCommandEffect::RestartChannel {
            target,
            platform: turn.platform.clone(),
            service_id: None,
            status: "unsupported-target".to_string(),
            detail: "restart target must be current, channel, telegram, tg, or discord".to_string(),
            reason,
            stop_file: None,
            request_file: None,
            receipt_file: None,
        };
    };
    let Some(service_id) = restart_service_id(&platform) else {
        return ChannelCommandEffect::RestartChannel {
            target,
            platform,
            service_id: None,
            status: "unsupported-platform".to_string(),
            detail: "restart is only supported for Telegram and Discord channel loops".to_string(),
            reason,
            stop_file: None,
            request_file: None,
            receipt_file: None,
        };
    };
    let Some(harness_home) = turn.harness_home.as_ref() else {
        return ChannelCommandEffect::RestartChannel {
            target,
            platform,
            service_id: Some(service_id.to_string()),
            status: "missing-harness-home".to_string(),
            detail:
                "restart command requires a harness home so the supervisor stop file can be written"
                    .to_string(),
            reason,
            stop_file: None,
            request_file: None,
            receipt_file: None,
        };
    };

    match write_channel_restart_request(turn, harness_home, &platform, service_id, reason.as_deref())
    {
        Ok(record) => ChannelCommandEffect::RestartChannel {
            target,
            platform,
            service_id: Some(service_id.to_string()),
            status: "requested".to_string(),
            detail: "restart stop file written; supervised loop will relaunch when the runner observes it"
                .to_string(),
            reason,
            stop_file: Some(record.stop_file),
            request_file: Some(record.request_file),
            receipt_file: Some(record.receipt_file),
        },
        Err(error) => {
            warnings.push(format!("failed to record channel restart request: {error}"));
            ChannelCommandEffect::RestartChannel {
                target,
                platform,
                service_id: Some(service_id.to_string()),
                status: "failed".to_string(),
                detail: error.to_string(),
                reason,
                stop_file: None,
                request_file: None,
                receipt_file: None,
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ChannelRestartFiles {
    stop_file: PathBuf,
    request_file: PathBuf,
    receipt_file: PathBuf,
}

fn write_channel_restart_request(
    turn: &TurnPlan,
    harness_home: &Path,
    platform: &str,
    service_id: &str,
    reason: Option<&str>,
) -> io::Result<ChannelRestartFiles> {
    let at_ms = current_time_ms();
    let reason = reason
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("channel /restart command requested");
    let stop_file = channel_restart_stop_file(harness_home, service_id);
    if let Some(parent) = stop_file.parent() {
        fs::create_dir_all(parent)?;
    }
    let stop_file_envelope = serde_json::json!({
        "schema": "agent-harness.supervisor-stop-file.v1",
        "serviceId": service_id,
        "action": "restart",
        "restart": true,
        "reason": reason,
        "createdBy": "channel-restart-command",
        "createdAtMs": at_ms,
        "persistent": false,
        "platform": platform,
        "channelId": turn.channel_id,
        "userId": turn.user_id,
        "sessionKey": turn.session_key,
    });
    crate::write_json_atomic(&stop_file, &stop_file_envelope)?;

    let request_dir = harness_home
        .join("state")
        .join("channels")
        .join("restart-requests");
    fs::create_dir_all(&request_dir)?;
    let request_file = request_dir.join(format!(
        "{}-{}.json",
        at_ms,
        safe_file_component(service_id)
    ));
    let receipt_file = harness_home
        .join("state")
        .join("channels")
        .join("channel-restart-requests.jsonl");
    let record = serde_json::json!({
        "schema": CHANNEL_RESTART_REQUEST_SCHEMA,
        "status": "requested",
        "harnessHome": harness_home,
        "platform": platform,
        "serviceId": service_id,
        "stopFile": stop_file,
        "requestFile": request_file,
        "receiptFile": receipt_file,
        "reason": reason,
        "channelId": turn.channel_id,
        "userId": turn.user_id,
        "sessionKey": turn.session_key,
        "atMs": at_ms,
    });
    crate::write_json_atomic(&request_file, &record)?;
    crate::append_jsonl_value(&receipt_file, &record)?;

    Ok(ChannelRestartFiles {
        stop_file,
        request_file,
        receipt_file,
    })
}

fn restart_target_platform(turn: &TurnPlan, target: Option<&str>) -> Option<String> {
    match target.map(|value| value.trim().to_ascii_lowercase()) {
        None => Some(turn.platform.trim().to_ascii_lowercase()),
        Some(target) if target.is_empty() || target == "current" || target == "channel" => {
            Some(turn.platform.trim().to_ascii_lowercase())
        }
        Some(target) if target == "tg" || target == "telegram" => Some("telegram".to_string()),
        Some(target) if target == "discord" => Some("discord".to_string()),
        Some(_) => None,
    }
}

fn restart_service_id(platform: &str) -> Option<&'static str> {
    match platform {
        "telegram" => Some("telegram-loop"),
        "discord" => Some("discord-gateway-loop"),
        _ => None,
    }
}

fn channel_restart_stop_file(harness_home: &Path, service_id: &str) -> PathBuf {
    let plan_file = harness_home
        .join("state")
        .join("supervisor")
        .join("windows-scheduled-tasks")
        .join("supervisor-plan.json");
    if let Ok(text) = fs::read_to_string(&plan_file) {
        if let Ok(value) = serde_json::from_str::<Value>(&text) {
            if let Some(stop_file) = value
                .get("tasks")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .find(|task| task.get("component").and_then(Value::as_str) == Some(service_id))
                .and_then(|task| task.get("stopFile").and_then(Value::as_str))
            {
                return PathBuf::from(stop_file);
            }
        }
    }
    crate::loop_health::supervisor_stop_file_path(harness_home, service_id)
}

fn thinking_command_effect(
    turn: &TurnPlan,
    level: Option<String>,
    global: bool,
) -> ChannelCommandEffect {
    let available_levels = available_thinking_levels(turn);
    let agent_id = turn.agent.as_ref().map(|agent| agent.id.clone());
    let provider = turn.model_policy.provider.clone();
    let model = turn.model_policy.model.clone();
    let thinking_enabled = turn.thinking_policy.enabled;
    let current_level = current_thinking_level(turn);
    match level {
        Some(level) => {
            let normalized = normalize_thinking_level(&level)
                .unwrap_or_else(|| level.trim().to_ascii_lowercase());
            let valid = available_levels
                .iter()
                .any(|candidate| candidate == &normalized);
            ChannelCommandEffect::SwitchThinking {
                agent_id,
                provider,
                model,
                thinking_enabled,
                current_level,
                level: normalized,
                global,
                valid,
                available_levels,
            }
        }
        None => ChannelCommandEffect::ShowThinking {
            agent_id,
            provider,
            model,
            thinking_enabled,
            current_level,
            available_levels,
        },
    }
}

fn model_command_effect(
    registry: &AgentRegistry,
    turn: &TurnPlan,
    target: Option<String>,
    global: bool,
) -> ChannelCommandEffect {
    let agent_id = turn.agent.as_ref().map(|agent| agent.id.clone());
    let current_provider = turn.model_policy.provider.clone();
    let current_model = turn.model_policy.model.clone();
    match target {
        Some(target) => match split_provider_model_target(&target) {
            Some((provider, model)) => {
                let provider_known = provider_profile(registry, &provider).is_some();
                let model_known = provider_profile(registry, &provider)
                    .map(|profile| {
                        profile.models.is_empty()
                            || profile.models.iter().any(|candidate| candidate == &model)
                    })
                    .unwrap_or(false);
                ChannelCommandEffect::SwitchModel {
                    agent_id,
                    provider,
                    model,
                    global,
                    current_provider,
                    current_model,
                    provider_known,
                    model_known,
                }
            }
            None => {
                let provider = target.trim().to_string();
                let profile = provider_profile(registry, &provider);
                ChannelCommandEffect::ListProviderModels {
                    agent_id,
                    current_provider,
                    current_model,
                    provider,
                    provider_known: profile.is_some(),
                    models: profile
                        .map(|profile| profile.models.clone())
                        .unwrap_or_default(),
                }
            }
        },
        None => ChannelCommandEffect::ShowModel {
            agent_id,
            current_provider,
            current_model,
            providers: registry
                .providers
                .iter()
                .map(|provider| provider.id.clone())
                .collect(),
        },
    }
}

fn new_session_key(turn: &TurnPlan) -> String {
    let agent_id = turn
        .agent
        .as_ref()
        .map(|agent| agent.id.as_str())
        .unwrap_or("unassigned");
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default();
    format!(
        "{}:{}:{}:{}:session-{}",
        normalize_key_part(&turn.platform),
        normalize_key_part(&turn.channel_id),
        normalize_key_part(&turn.user_id),
        normalize_key_part(agent_id),
        millis
    )
}

fn current_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or_default()
}

fn safe_file_component(value: &str) -> String {
    let mut output = String::new();
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
            output.push(ch);
        } else {
            output.push('_');
        }
    }
    if output.is_empty() {
        "restart".to_string()
    } else {
        output
    }
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

fn command_reply_text(effect: &ChannelCommandEffect) -> String {
    match effect {
        ChannelCommandEffect::StartNewSession {
            topic,
            new_session_key,
        } => format!(
            "New session planned: {}{}",
            new_session_key,
            topic
                .as_ref()
                .map(|topic| format!(" ({topic})"))
                .unwrap_or_default()
        ),
        ChannelCommandEffect::ShowThinking {
            provider,
            model,
            thinking_enabled,
            current_level,
            available_levels,
            ..
        } => format!(
            "Current session thinking level: {} (enabled={})\nAvailable thinking levels for {}: {}",
            display_thinking_level(current_level),
            yes_no(*thinking_enabled),
            display_model_route(provider, model),
            display_list(available_levels)
        ),
        ChannelCommandEffect::SwitchThinking {
            agent_id,
            provider,
            model,
            thinking_enabled,
            current_level,
            level,
            global,
            valid,
            available_levels,
        } => {
            let mut text = format!(
                "Current session thinking level: {} (enabled={})\n",
                display_thinking_level(current_level),
                yes_no(*thinking_enabled)
            );
            if *valid {
                if *global {
                    text.push_str(&format!(
                        "Thinking level updated for this session and agent `{}` default: {}\n",
                        display_opt(agent_id),
                        level
                    ));
                } else {
                    text.push_str(&format!(
                        "Thinking level updated for this session: {}\n",
                        level
                    ));
                }
                text.push_str(&format!("Model: {}", display_model_route(provider, model)));
            } else {
                text.push_str(&format!(
                    "Unsupported thinking level for {}: {}\nAvailable thinking levels: {}",
                    display_model_route(provider, model),
                    level,
                    display_list(available_levels)
                ));
            }
            text
        }
        ChannelCommandEffect::StopCurrentRun { reason } => reason
            .as_ref()
            .map(|reason| format!("Stop requested for the current run: {reason}"))
            .unwrap_or_else(|| "Stop requested for the current run.".to_string()),
        ChannelCommandEffect::RestartChannel {
            platform,
            service_id,
            status,
            detail,
            reason,
            ..
        } => {
            if status == "requested" {
                format!(
                    "Restart requested for {} channel loop `{}`.{}",
                    platform,
                    display_opt(service_id),
                    reason
                        .as_ref()
                        .map(|reason| format!("\nReason: {reason}"))
                        .unwrap_or_default()
                )
            } else {
                format!("Restart request status: {status}\n{detail}")
            }
        }
        ChannelCommandEffect::AddSteering { instruction } => {
            format!("Steering note recorded for this session: {instruction}")
        }
        ChannelCommandEffect::AddBtwNote { note } => {
            format!("BTW note recorded for this session: {note}")
        }
        ChannelCommandEffect::ShowModel {
            agent_id,
            current_provider,
            current_model,
            providers,
        } => format!(
            "Current session model: {}\nAgent: {}\nAvailable providers: {}",
            display_model_route(current_provider, current_model),
            display_opt(agent_id),
            display_list(providers)
        ),
        ChannelCommandEffect::ListProviderModels {
            agent_id,
            current_provider,
            current_model,
            provider,
            provider_known,
            models,
        } => format!(
            "Current session model: {}\nAgent: {}\n{}",
            display_model_route(current_provider, current_model),
            display_opt(agent_id),
            if *provider_known {
                format!(
                    "Models for provider `{}`: {}",
                    provider,
                    display_list(models)
                )
            } else {
                format!(
                    "Provider `{}` is not registered. Available models: -",
                    provider
                )
            }
        ),
        ChannelCommandEffect::SwitchModel {
            agent_id,
            provider,
            model,
            global,
            current_provider,
            current_model,
            provider_known,
            model_known,
        } => format!(
            "Current session model: {}\nModel updated for {}: {}\nRegistry: provider={}, model={}",
            display_model_route(current_provider, current_model),
            if *global {
                format!("this session and agent `{}` default", display_opt(agent_id))
            } else {
                "this session".to_string()
            },
            display_model_route(&Some(provider.clone()), &Some(model.clone())),
            yes_no(*provider_known),
            yes_no(*model_known)
        ),
        ChannelCommandEffect::ShowStatus { snapshot, .. } => status_reply_text(snapshot),
    }
}

fn status_reply_text(snapshot: &ChannelStatusSnapshot) -> String {
    match snapshot
        .scope
        .as_deref()
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("agents") => format!(
            "Agent Harness Agent Status\nAgents: {}/{} enabled\nCurrent: {}\nDirectory: {}\nSessions index: {}",
            snapshot.agents_enabled,
            snapshot.agents_total,
            display_opt(&snapshot.current_agent_id),
            yes_no(snapshot.agent_directory_exists),
            yes_no(snapshot.agent_sessions_index_exists)
        ),
        Some("channels") => format!(
            "Agent Harness Channel Status\nPlatform: {}\nSession: {}\nTelegram: {}\nDiscord: {}",
            snapshot.platform,
            snapshot.session_key,
            yes_no(snapshot.telegram_configured),
            yes_no(snapshot.discord_configured)
        ),
        Some("model") => format!(
            "Agent Harness Model Status\nAgent: {}\nProvider: {}\nModel: {}\nOverride: {}\nThinking: {}, level={}",
            display_opt(&snapshot.current_agent_id),
            display_opt(&snapshot.current_provider),
            display_opt(&snapshot.current_model),
            display_opt(&snapshot.model_override),
            yes_no(snapshot.thinking_enabled),
            display_thinking_level(&snapshot.thinking_level)
        ),
        Some("security") => format!(
            "Agent Harness Security Status\nApprovals: {}\nWindows sandbox: {}\nFilesystem sandbox: {}",
            display_opt(&snapshot.codex_approval_policy),
            display_opt(&snapshot.codex_sandbox),
            display_opt(&snapshot.codex_sandbox_policy)
        ),
        Some("skills") => format!(
            "Agent Harness Skill Status\nSelected: {}\nMatches: {}",
            snapshot.selected_skills,
            display_list(&snapshot.selected_skill_ids)
        ),
        Some("cron") => {
            "Cron status is available through cron-plan and deterministic-cron-plan.".to_string()
        }
        _ => format!(
            "Agent Harness Status\nAgent: {} ({}/{})\nModel: provider={}, model={}, override={}\nThinking: enabled={}, level={}\nSecurity: approvals={}, windowsSandbox={}, filesystemSandbox={}\nChannels: telegram={}, discord={}, current={}\nSession: active={}, stateLoaded={}\nPrompt: files {}/{} ({})\nSkills: {} selected ({})\nState: steer={}, btw={}\nRegistry: providers={}, plugins={}",
            display_opt(&snapshot.current_agent_id),
            snapshot.agents_enabled,
            snapshot.agents_total,
            display_opt(&snapshot.current_provider),
            display_opt(&snapshot.current_model),
            display_opt(&snapshot.model_override),
            yes_no(snapshot.thinking_enabled),
            display_thinking_level(&snapshot.thinking_level),
            display_opt(&snapshot.codex_approval_policy),
            display_opt(&snapshot.codex_sandbox),
            display_opt(&snapshot.codex_sandbox_policy),
            yes_no(snapshot.telegram_configured),
            yes_no(snapshot.discord_configured),
            snapshot.platform,
            display_opt(&snapshot.active_session_key),
            yes_no(snapshot.channel_state_loaded),
            snapshot.prompt_files_present,
            snapshot.prompt_files_total,
            display_list(&snapshot.prompt_file_names),
            snapshot.selected_skills,
            display_list(&snapshot.selected_skill_ids),
            snapshot.steering_notes,
            snapshot.btw_notes,
            snapshot.providers_total,
            snapshot.plugins_total
        ),
    }
}

fn status_snapshot(
    registry: &AgentRegistry,
    turn: &TurnPlan,
    scope: Option<String>,
) -> ChannelStatusSnapshot {
    let codex_approval_policy = turn.harness_home.as_ref().map(|harness_home| {
        inspect_codex_approval_policy(harness_home)
            .policy
            .as_str()
            .to_string()
    });
    let codex_sandbox = turn
        .harness_home
        .as_ref()
        .map(|harness_home| inspect_codex_sandbox(harness_home).sandbox);
    let codex_sandbox_policy = turn
        .harness_home
        .as_ref()
        .map(|harness_home| inspect_codex_sandbox_policy(harness_home).sandbox);
    ChannelStatusSnapshot {
        scope,
        platform: turn.platform.clone(),
        session_key: turn.session_key.clone(),
        agents_total: registry.agents.len(),
        agents_enabled: registry
            .agents
            .iter()
            .filter(|agent| agent.enabled != Some(false))
            .count(),
        providers_total: registry.providers.len(),
        plugins_total: registry.plugins.len(),
        telegram_configured: registry.channels.telegram,
        discord_configured: registry.channels.discord,
        current_agent_id: turn.agent.as_ref().map(|agent| agent.id.clone()),
        agent_directory_exists: turn
            .agent
            .as_ref()
            .is_some_and(|agent| agent.directory_exists),
        agent_sessions_index_exists: turn
            .agent
            .as_ref()
            .is_some_and(|agent| agent.sessions_index_exists),
        current_provider: turn.model_policy.provider.clone(),
        current_model: turn.model_policy.model.clone(),
        model_override: turn
            .channel_state
            .as_ref()
            .and_then(|state| state.model_override.clone()),
        codex_approval_policy,
        codex_sandbox,
        codex_sandbox_policy,
        prompt_files_present: prompt_files_present(turn),
        prompt_files_total: turn.prompt_files.len(),
        prompt_file_names: turn
            .prompt_files
            .iter()
            .filter(|file| file.exists)
            .map(|file| file.name.clone())
            .collect(),
        selected_skills: turn.selected_skills.len(),
        selected_skill_ids: turn
            .selected_skills
            .iter()
            .map(|skill| skill.skill_id.clone())
            .collect(),
        channel_state_loaded: turn.channel_state.is_some(),
        active_session_key: turn
            .channel_state
            .as_ref()
            .map(|state| state.active_session_key.clone()),
        thinking_enabled: turn.thinking_policy.enabled,
        thinking_level: turn.thinking_policy.level.clone(),
        steering_notes: turn
            .channel_state
            .as_ref()
            .map(|state| state.steering_notes.len())
            .unwrap_or(0),
        btw_notes: turn
            .channel_state
            .as_ref()
            .map(|state| state.btw_notes.len())
            .unwrap_or(0),
    }
}

fn outbound(
    turn: &TurnPlan,
    kind: ChannelOutboundMessageKind,
    text: String,
) -> ChannelOutboundMessage {
    ChannelOutboundMessage {
        platform: turn.platform.clone(),
        account_id: None,
        channel_id: turn.channel_id.clone(),
        user_id: turn.user_id.clone(),
        session_key: turn.session_key.clone(),
        kind,
        text,
        delivery_intent: None,
        attachments: Vec::new(),
    }
}

fn default_delivery_intent_schema() -> String {
    "agent-harness.delivery-intent.v1".to_string()
}

fn prompt_files_present(turn: &TurnPlan) -> usize {
    turn.prompt_files.iter().filter(|file| file.exists).count()
}

fn display_opt(value: &Option<String>) -> &str {
    value.as_deref().unwrap_or("-")
}

fn display_model_route(provider: &Option<String>, model: &Option<String>) -> String {
    match (provider.as_deref(), model.as_deref()) {
        (Some(provider), Some(model)) => format!("{provider}/{model}"),
        (None, Some(model)) => model.to_string(),
        (Some(provider), None) => format!("{provider}/-"),
        (None, None) => "-".to_string(),
    }
}

fn display_thinking_level(level: &Option<String>) -> &str {
    level.as_deref().unwrap_or(DEFAULT_THINKING_LEVEL)
}

fn display_list(values: &[String]) -> String {
    if values.is_empty() {
        "-".to_string()
    } else {
        values.join(", ")
    }
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

fn provider_profile<'a>(
    registry: &'a AgentRegistry,
    provider: &str,
) -> Option<&'a crate::ProviderProfile> {
    registry
        .providers
        .iter()
        .find(|profile| profile.id.eq_ignore_ascii_case(provider))
}

fn split_provider_model_target(target: &str) -> Option<(String, String)> {
    let trimmed = target.trim().trim_matches('"');
    let (provider, model) = trimmed.split_once('/')?;
    let provider = provider.trim();
    let model = model.trim();
    if provider.is_empty() || model.is_empty() {
        return None;
    }
    Some((provider.to_string(), model.to_string()))
}

fn available_thinking_levels(turn: &TurnPlan) -> Vec<String> {
    let mut levels = THINKING_LEVELS
        .iter()
        .map(|level| (*level).to_string())
        .collect::<Vec<_>>();
    if supports_xhigh_thinking(turn) && !levels.iter().any(|level| level == XHIGH_THINKING_LEVEL) {
        levels.push(XHIGH_THINKING_LEVEL.to_string());
    }
    levels
}

fn supports_xhigh_thinking(turn: &TurnPlan) -> bool {
    let model = turn
        .model_policy
        .model
        .as_deref()
        .unwrap_or_default()
        .to_ascii_lowercase();
    if !(model.starts_with("gpt-5") || model.contains("codex")) {
        return false;
    }
    turn.model_policy
        .provider
        .as_deref()
        .map(|provider| {
            provider.eq_ignore_ascii_case("openai")
                || provider.eq_ignore_ascii_case("codex")
                || (provider.eq_ignore_ascii_case("openrouter") && model.contains("openai/"))
        })
        .unwrap_or(true)
}

fn current_thinking_level(turn: &TurnPlan) -> Option<String> {
    turn.thinking_policy
        .level
        .clone()
        .or_else(|| Some(DEFAULT_THINKING_LEVEL.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{TurnPlanInput, build_source_skill_index, build_turn_plan, load_agent_registry};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn channel_step_enqueues_plain_agent_turn() {
        let root = temp_root("channel_step_enqueues_plain_agent_turn");
        let source = write_channel_source(&root);
        let registry = load_agent_registry(&source).unwrap();
        let skills = build_source_skill_index(&source).unwrap();
        let turn = build_turn_plan(
            &source,
            &registry,
            &skills,
            TurnPlanInput {
                harness_home: None,
                platform: "telegram".to_string(),
                channel_id: "dm-42".to_string(),
                user_id: "user-7".to_string(),
                text: "repair memory cron".to_string(),
                inbound_context: None,
                inbound_media_artifacts: Vec::new(),
                requested_agent_id: Some("main".to_string()),
                session_hint: None,
                skill_limit: 3,
            },
        )
        .unwrap();

        let step = build_channel_step(&registry, &turn);

        assert_eq!(step.action, ChannelStepAction::EnqueueAgentTurn);
        assert!(step.command_effect.is_none());
        assert!(step.outbound_messages.is_empty());
        let dispatch = step.agent_turn.unwrap();
        assert_eq!(dispatch.agent_id, "main");
        assert_eq!(dispatch.provider.as_deref(), Some("openai"));
        assert_eq!(dispatch.model.as_deref(), Some("gpt-5"));
        assert_eq!(dispatch.selected_skill_ids, vec!["workspace:memory-cron"]);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn channel_step_replies_to_status_command() {
        let root = temp_root("channel_step_replies_to_status_command");
        let source = write_channel_source(&root);
        let registry = load_agent_registry(&source).unwrap();
        let skills = build_source_skill_index(&source).unwrap();
        let turn = build_turn_plan(
            &source,
            &registry,
            &skills,
            TurnPlanInput {
                harness_home: None,
                platform: "discord".to_string(),
                channel_id: "dm#42".to_string(),
                user_id: "user#7".to_string(),
                text: "/status channels".to_string(),
                inbound_context: None,
                inbound_media_artifacts: Vec::new(),
                requested_agent_id: Some("main".to_string()),
                session_hint: None,
                skill_limit: 3,
            },
        )
        .unwrap();

        let step = build_channel_step(&registry, &turn);

        assert_eq!(step.action, ChannelStepAction::ReplyOnly);
        assert!(step.agent_turn.is_none());
        assert_eq!(step.outbound_messages.len(), 1);
        assert!(step.outbound_messages[0].text.contains("Telegram: yes"));
        assert!(matches!(
            step.command_effect,
            Some(ChannelCommandEffect::ShowStatus { ref snapshot, .. })
                if snapshot.scope.as_deref() == Some("channels")
                    && snapshot.discord_configured
                    && snapshot.telegram_configured
        ));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn channel_step_requests_channel_restart_stop_file() {
        let root = temp_root("channel_step_requests_channel_restart_stop_file");
        let source = write_channel_source(&root);
        let harness_home = root.join(".agent-harness");
        let supervisor_dir = harness_home
            .join("state")
            .join("supervisor")
            .join("windows-scheduled-tasks");
        let stop_file = supervisor_dir.join("stop").join("telegram-loop.stop");
        fs::create_dir_all(stop_file.parent().unwrap()).unwrap();
        fs::write(
            supervisor_dir.join("supervisor-plan.json"),
            serde_json::to_string(&serde_json::json!({
                "tasks": [
                    { "component": "telegram-loop", "stopFile": stop_file }
                ]
            }))
            .unwrap(),
        )
        .unwrap();
        let registry = load_agent_registry(&source).unwrap();
        let skills = build_source_skill_index(&source).unwrap();
        let turn = build_turn_plan(
            &source,
            &registry,
            &skills,
            TurnPlanInput {
                harness_home: Some(harness_home.clone()),
                platform: "telegram".to_string(),
                channel_id: "dm".to_string(),
                user_id: "operator".to_string(),
                text: "/restart reconnect adapter".to_string(),
                inbound_context: None,
                inbound_media_artifacts: Vec::new(),
                requested_agent_id: Some("main".to_string()),
                session_hint: None,
                skill_limit: 3,
            },
        )
        .unwrap();

        let step = build_channel_step(&registry, &turn);

        assert_eq!(step.action, ChannelStepAction::ReplyOnly);
        assert!(step.agent_turn.is_none());
        assert!(
            step.outbound_messages[0]
                .text
                .contains("Restart requested for telegram channel loop `telegram-loop`")
        );
        assert!(matches!(
            step.command_effect,
            Some(ChannelCommandEffect::RestartChannel {
                ref platform,
                ref service_id,
                ref status,
                ref reason,
                ..
            }) if platform == "telegram"
                && service_id.as_deref() == Some("telegram-loop")
                && status == "requested"
                && reason.as_deref() == Some("reconnect adapter")
        ));
        let stop_value: Value = serde_json::from_slice(&fs::read(&stop_file).unwrap()).unwrap();
        assert_eq!(
            stop_value["schema"],
            "agent-harness.supervisor-stop-file.v1"
        );
        assert_eq!(stop_value["serviceId"], "telegram-loop");
        assert_eq!(stop_value["action"], "restart");
        assert_eq!(stop_value["restart"], true);
        assert_eq!(stop_value["createdBy"], "channel-restart-command");
        assert_eq!(stop_value["persistent"], false);
        assert!(
            harness_home
                .join("state")
                .join("channels")
                .join("channel-restart-requests.jsonl")
                .is_file()
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn channel_step_replies_to_security_status_command() {
        let root = temp_root("channel_step_replies_to_security_status_command");
        let source = write_channel_source(&root);
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            r#"{"security":{"codexApprovalPolicy":"accept","codexSandbox":"elevated","codexSandboxPolicy":"dangerFullAccess"}}"#,
        )
        .unwrap();
        let registry = load_agent_registry(&source).unwrap();
        let skills = build_source_skill_index(&source).unwrap();
        let turn = build_turn_plan(
            &source,
            &registry,
            &skills,
            TurnPlanInput {
                harness_home: Some(harness_home),
                platform: "telegram".to_string(),
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                text: "/status security".to_string(),
                inbound_context: None,
                inbound_media_artifacts: Vec::new(),
                requested_agent_id: Some("main".to_string()),
                session_hint: None,
                skill_limit: 3,
            },
        )
        .unwrap();

        let step = build_channel_step(&registry, &turn);

        assert_eq!(step.action, ChannelStepAction::ReplyOnly);
        assert!(step.outbound_messages[0].text.contains("Approvals: accept"));
        assert!(
            step.outbound_messages[0]
                .text
                .contains("Windows sandbox: elevated")
        );
        assert!(
            step.outbound_messages[0]
                .text
                .contains("Filesystem sandbox: dangerFullAccess")
        );
        assert!(matches!(
            step.command_effect,
            Some(ChannelCommandEffect::ShowStatus { ref snapshot, .. })
                if snapshot.scope.as_deref() == Some("security")
                    && snapshot.codex_approval_policy.as_deref() == Some("accept")
                    && snapshot.codex_sandbox.as_deref() == Some("elevated")
                    && snapshot.codex_sandbox_policy.as_deref() == Some("dangerFullAccess")
        ));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn channel_step_plans_model_switch_effect() {
        let root = temp_root("channel_step_plans_model_switch_effect");
        let source = write_channel_source(&root);
        let registry = load_agent_registry(&source).unwrap();
        let skills = build_source_skill_index(&source).unwrap();
        let turn = build_turn_plan(
            &source,
            &registry,
            &skills,
            TurnPlanInput {
                harness_home: None,
                platform: "telegram".to_string(),
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                text: "/model openrouter/anthropic/claude-sonnet-4".to_string(),
                inbound_context: None,
                inbound_media_artifacts: Vec::new(),
                requested_agent_id: Some("main".to_string()),
                session_hint: None,
                skill_limit: 3,
            },
        )
        .unwrap();

        let step = build_channel_step(&registry, &turn);

        assert_eq!(step.action, ChannelStepAction::ReplyOnly);
        assert!(
            step.outbound_messages[0]
                .text
                .contains("Model updated for this session")
        );
        assert!(step.warnings.is_empty());
        assert!(matches!(
            step.command_effect,
            Some(ChannelCommandEffect::SwitchModel {
                ref provider,
                ref model,
                global,
                ..
            }) if provider == "openrouter"
                && model == "anthropic/claude-sonnet-4"
                && !global
        ));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn channel_step_lists_model_providers_and_provider_models() {
        let root = temp_root("channel_step_lists_model_providers_and_provider_models");
        let source = write_channel_source(&root);
        let registry = load_agent_registry(&source).unwrap();
        let skills = build_source_skill_index(&source).unwrap();
        let show_turn = build_turn_plan(
            &source,
            &registry,
            &skills,
            TurnPlanInput {
                harness_home: None,
                platform: "telegram".to_string(),
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                text: "/model".to_string(),
                inbound_context: None,
                inbound_media_artifacts: Vec::new(),
                requested_agent_id: Some("main".to_string()),
                session_hint: None,
                skill_limit: 3,
            },
        )
        .unwrap();
        let show_step = build_channel_step(&registry, &show_turn);
        assert!(
            show_step.outbound_messages[0]
                .text
                .starts_with("Current session model: openai/gpt-5")
        );
        assert!(
            show_step.outbound_messages[0]
                .text
                .contains("Available providers: openai, openrouter")
        );

        let list_turn = build_turn_plan(
            &source,
            &registry,
            &skills,
            TurnPlanInput {
                harness_home: None,
                platform: "telegram".to_string(),
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                text: "/model openrouter".to_string(),
                inbound_context: None,
                inbound_media_artifacts: Vec::new(),
                requested_agent_id: Some("main".to_string()),
                session_hint: None,
                skill_limit: 3,
            },
        )
        .unwrap();
        let list_step = build_channel_step(&registry, &list_turn);
        assert!(
            list_step.outbound_messages[0]
                .text
                .contains("Models for provider `openrouter`: anthropic/claude-sonnet-4")
        );
        assert!(matches!(
            list_step.command_effect,
            Some(ChannelCommandEffect::ListProviderModels {
                ref provider,
                provider_known,
                ..
            }) if provider == "openrouter" && provider_known
        ));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn channel_step_reports_and_switches_thinking_level() {
        let root = temp_root("channel_step_reports_and_switches_thinking_level");
        let source = write_channel_source(&root);
        let registry = load_agent_registry(&source).unwrap();
        let skills = build_source_skill_index(&source).unwrap();
        let show_turn = build_turn_plan(
            &source,
            &registry,
            &skills,
            TurnPlanInput {
                harness_home: None,
                platform: "telegram".to_string(),
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                text: "/think".to_string(),
                inbound_context: None,
                inbound_media_artifacts: Vec::new(),
                requested_agent_id: Some("main".to_string()),
                session_hint: None,
                skill_limit: 3,
            },
        )
        .unwrap();
        let show_step = build_channel_step(&registry, &show_turn);
        assert!(
            show_step.outbound_messages[0]
                .text
                .starts_with("Current session thinking level: medium")
        );
        assert!(
            show_step.outbound_messages[0]
                .text
                .contains("minimal, low, medium, high")
        );

        let switch_turn = build_turn_plan(
            &source,
            &registry,
            &skills,
            TurnPlanInput {
                harness_home: None,
                platform: "telegram".to_string(),
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                text: "/think high --global".to_string(),
                inbound_context: None,
                inbound_media_artifacts: Vec::new(),
                requested_agent_id: Some("main".to_string()),
                session_hint: None,
                skill_limit: 3,
            },
        )
        .unwrap();
        let switch_step = build_channel_step(&registry, &switch_turn);
        assert!(
            switch_step.outbound_messages[0]
                .text
                .contains("agent `main` default: high")
        );
        assert!(matches!(
            switch_step.command_effect,
            Some(ChannelCommandEffect::SwitchThinking {
                ref level,
                global,
                valid,
                ..
            }) if level == "high" && global && valid
        ));

        let xhigh_turn = build_turn_plan(
            &source,
            &registry,
            &skills,
            TurnPlanInput {
                harness_home: None,
                platform: "telegram".to_string(),
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                text: "/ think 超高".to_string(),
                inbound_context: None,
                inbound_media_artifacts: Vec::new(),
                requested_agent_id: Some("main55".to_string()),
                session_hint: None,
                skill_limit: 3,
            },
        )
        .unwrap();
        let xhigh_step = build_channel_step(&registry, &xhigh_turn);
        assert!(
            xhigh_step.outbound_messages[0]
                .text
                .contains("session: xhigh")
        );
        assert!(matches!(
            xhigh_step.command_effect,
            Some(ChannelCommandEffect::SwitchThinking {
                ref level,
                valid,
                ..
            }) if level == "xhigh" && valid
        ));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn channel_step_new_session_uses_unique_base_session_key() {
        let root = temp_root("channel_step_new_session_uses_unique_base_session_key");
        let source = write_channel_source(&root);
        let registry = load_agent_registry(&source).unwrap();
        let skills = build_source_skill_index(&source).unwrap();
        let turn = build_turn_plan(
            &source,
            &registry,
            &skills,
            TurnPlanInput {
                harness_home: None,
                platform: "telegram".to_string(),
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                text: "/new weekly review".to_string(),
                inbound_context: None,
                inbound_media_artifacts: Vec::new(),
                requested_agent_id: Some("main".to_string()),
                session_hint: Some("telegram:dm:user:main:new".to_string()),
                skill_limit: 3,
            },
        )
        .unwrap();

        let step = build_channel_step(&registry, &turn);

        assert!(matches!(
            step.command_effect,
            Some(ChannelCommandEffect::StartNewSession {
                ref topic,
                ref new_session_key,
            }) if topic.as_deref() == Some("weekly review")
                && new_session_key.starts_with("telegram:dm:user:main:session-")
                && !new_session_key.contains(":new")
        ));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn write_channel_step_outputs_json() {
        let root = temp_root("write_channel_step_outputs_json");
        let source = write_channel_source(&root);
        let registry = load_agent_registry(&source).unwrap();
        let skills = build_source_skill_index(&source).unwrap();
        let turn = build_turn_plan(
            &source,
            &registry,
            &skills,
            TurnPlanInput {
                harness_home: None,
                platform: "telegram".to_string(),
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                text: "/model".to_string(),
                inbound_context: None,
                inbound_media_artifacts: Vec::new(),
                requested_agent_id: Some("main".to_string()),
                session_hint: None,
                skill_limit: 3,
            },
        )
        .unwrap();
        let step = build_channel_step(&registry, &turn);

        let file = write_channel_step(&step, root.join("out")).unwrap();

        assert!(file.json.is_file());
        let json: serde_json::Value =
            serde_json::from_slice(&fs::read(file.json).unwrap()).unwrap();
        assert_eq!(json["schema"], CHANNEL_STEP_SCHEMA);
        assert_eq!(json["action"], "reply-only");

        let _ = fs::remove_dir_all(root);
    }

    fn write_channel_source(root: &Path) -> crate::AgentSource {
        let home = root.join(".openclaw");
        let workspace = home.join("workspace");
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(home.join("agents").join("main").join("sessions")).unwrap();
        fs::create_dir_all(workspace.join("skills").join("memory-cron")).unwrap();
        fs::write(workspace.join("AGENTS.md"), "# Agent").unwrap();
        fs::write(
            workspace
                .join("skills")
                .join("memory-cron")
                .join(crate::SKILL_FILE_NAME),
            "# Memory Cron\n\nRepair openclaw-mem cron jobs.",
        )
        .unwrap();
        fs::write(
            home.join("openclaw.json"),
            r#"{
              "agents": {
                "defaults": {
                  "provider": "openai",
                  "model": "codex",
                  "models": {
                    "openai/gpt-5": {},
                    "openai/gpt-5.5": {},
                    "openrouter/anthropic/claude-sonnet-4": {}
                  }
                },
                "list": [
                  { "id": "main", "model": "gpt-5", "enabled": true },
                  { "id": "main55", "model": "gpt-5.5", "enabled": true }
                ]
              },
              "models": {
                "providers": {
                  "openai": {
                    "apiKey": "${OPENAI_API_KEY}",
                    "models": [
                      { "id": "gpt-5" },
                      { "id": "gpt-5.5" }
                    ]
                  },
                  "openrouter": {
                    "baseURL": "https://openrouter.ai/api/v1",
                    "apiKey": "${OPENROUTER_API_KEY}"
                  }
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
        crate::AgentSource::with_workspace(home, workspace)
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-channel-runtime-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
