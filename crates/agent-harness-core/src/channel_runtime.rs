use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::backend_reasoning::{
    BackendReasoningPolicyV1, BackendReasoningSource, ReasoningPreference,
};
use crate::{
    AgentRegistry, ChannelCommandIntent, DEFAULT_THINKING_LEVEL, FastCommandMode,
    GatewayRestartStatusReport, InboundMediaArtifact, RichMessagePresentation, THINKING_LEVELS,
    TurnDispatch, TurnPlan, XHIGH_THINKING_LEVEL, collect_gateway_restart_status,
    inspect_codex_approval_policy, inspect_codex_sandbox, inspect_codex_sandbox_policy,
};

const CHANNEL_STEP_SCHEMA: &str = "agent-harness.channel-step.v1";
const CHANNEL_RESTART_REQUEST_SCHEMA: &str = "agent-harness.channel-restart-request.v1";
const CHANNEL_GATEWAY_RESTART_REQUEST_SCHEMA: &str = "agent-harness.gateway-restart-request.v1";

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
    RestartGateway {
        status: String,
        detail: String,
        reason: Option<String>,
        request_file: Option<PathBuf>,
        receipt_file: Option<PathBuf>,
    },
    RestartStatus {
        status: String,
        detail: String,
        report: Option<GatewayRestartStatusReport>,
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
    ShowReasoning {
        agent_id: Option<String>,
        provider: Option<String>,
        model: Option<String>,
        current_preference: Option<ReasoningPreference>,
        available_efforts: Vec<String>,
        catalog_default: Option<String>,
        catalog_revision: Option<String>,
        authoritative: bool,
        reason: String,
    },
    SwitchReasoning {
        agent_id: Option<String>,
        provider: Option<String>,
        model: Option<String>,
        preference: ReasoningPreference,
        global: bool,
        accepted: bool,
        resolved_policy: Option<BackendReasoningPolicyV1>,
        resolution: Option<crate::model_catalog::ReasoningResolutionReceipt>,
        available_efforts: Vec<String>,
        catalog_default: Option<String>,
        catalog_revision: Option<String>,
        reason: String,
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
    ShowFast {
        agent_id: Option<String>,
        provider: Option<String>,
        model: Option<String>,
        current_mode: String,
        effective_acceleration: String,
        reason: String,
    },
    SwitchFast {
        agent_id: Option<String>,
        provider: Option<String>,
        model: Option<String>,
        global: bool,
        previous_mode: String,
        mode: String,
        effective_acceleration: String,
        reason: String,
    },
    ShowStatus {
        scope: Option<String>,
        snapshot: ChannelStatusSnapshot,
    },
    UnknownCommand {
        name: String,
        rest: Option<String>,
        detail: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FastRequestRoutePolicy {
    pub effective_acceleration: String,
    pub reason: String,
    pub service_tier: Option<String>,
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
    pub fast_mode: Option<String>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_preference: Option<ReasoningPreference>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend_reasoning_policy: Option<BackendReasoningPolicyV1>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_queue_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_completion_file: Option<PathBuf>,
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub presentation: Option<RichMessagePresentation>,
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
    Audio,
    Voice,
    Video,
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
        reasoning_preference: turn.reasoning_preference.clone(),
        backend_reasoning_policy: turn.backend_reasoning_policy.clone(),
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
            unified_thinking_command_effect(turn, level, global)
        }
        ChannelCommandIntent::StopCurrentRun { reason } => {
            ChannelCommandEffect::StopCurrentRun { reason }
        }
        ChannelCommandIntent::RestartChannel { target, reason } => {
            restart_channel_effect(turn, target, reason, warnings)
        }
        ChannelCommandIntent::RestartGateway { reason } => {
            restart_gateway_effect(turn, reason, warnings)
        }
        ChannelCommandIntent::RestartStatus => restart_status_effect(turn, warnings),
        ChannelCommandIntent::AddSteering { instruction } => {
            ChannelCommandEffect::AddSteering { instruction }
        }
        ChannelCommandIntent::AddBtwNote { note } => ChannelCommandEffect::AddBtwNote { note },
        ChannelCommandIntent::Model { target, global } => {
            model_command_effect(registry, turn, target, global)
        }
        ChannelCommandIntent::Fast { mode, global } => fast_command_effect(turn, mode, global),
        ChannelCommandIntent::ShowStatus { scope } => ChannelCommandEffect::ShowStatus {
            snapshot: status_snapshot(registry, turn, scope.clone()),
            scope,
        },
        ChannelCommandIntent::UnknownCommand { name, rest } => {
            ChannelCommandEffect::UnknownCommand {
                name,
                rest,
                detail: "unsupported channel command; no model turn was started".to_string(),
            }
        }
    }
}

fn restart_status_effect(turn: &TurnPlan, warnings: &mut Vec<String>) -> ChannelCommandEffect {
    let Some(harness_home) = turn.harness_home.as_ref() else {
        return ChannelCommandEffect::RestartStatus {
            status: "missing-harness-home".to_string(),
            detail: "restart status requires a harness home".to_string(),
            report: None,
        };
    };
    match collect_gateway_restart_status(harness_home) {
        Ok(report) => ChannelCommandEffect::RestartStatus {
            status: "ok".to_string(),
            detail: restart_status_summary(&report),
            report: Some(report),
        },
        Err(error) => {
            warnings.push(format!("failed to read restart status: {error}"));
            ChannelCommandEffect::RestartStatus {
                status: "failed".to_string(),
                detail: error.to_string(),
                report: None,
            }
        }
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

fn restart_gateway_effect(
    turn: &TurnPlan,
    reason: Option<String>,
    warnings: &mut Vec<String>,
) -> ChannelCommandEffect {
    let Some(harness_home) = turn.harness_home.as_ref() else {
        return ChannelCommandEffect::RestartGateway {
            status: "failed".to_string(),
            detail:
                "restart command requires a harness home so the gateway restart request can be recorded"
                    .to_string(),
            reason,
            request_file: None,
            receipt_file: None,
        };
    };

    match write_gateway_restart_request(turn, harness_home, reason.as_deref()) {
        Ok(record) => ChannelCommandEffect::RestartGateway {
            status: "requested".to_string(),
            detail: "protected gateway restart request recorded; operator token + idle gate control are required"
                .to_string(),
            reason,
            request_file: Some(record.request_file),
            receipt_file: Some(record.receipt_file),
        },
        Err(error) => {
            warnings.push(format!("failed to record gateway restart request: {error}"));
            ChannelCommandEffect::RestartGateway {
                status: "failed".to_string(),
                detail: error.to_string(),
                reason,
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct GatewayRestartFiles {
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
    let reason = normalize_restart_reason(reason, "channel /restart command requested");
    if let Some(conflict) = channel_restart_owner_conflict(harness_home, service_id)? {
        return Err(io::Error::other(conflict));
    }
    let stop_file = channel_restart_stop_file(harness_home, service_id)?;
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

fn write_gateway_restart_request(
    turn: &TurnPlan,
    harness_home: &Path,
    reason: Option<&str>,
) -> io::Result<GatewayRestartFiles> {
    let at_ms = current_time_ms();
    let reason = normalize_restart_reason(reason, "gateway /restart command requested");
    let request_dir = harness_home
        .join("state")
        .join("supervisor")
        .join("gateway-restart-requests");
    fs::create_dir_all(&request_dir)?;
    let request_file = request_dir.join(format!(
        "{}-{}.json",
        at_ms,
        safe_file_component(&turn.user_id)
    ));
    let receipt_file = harness_home
        .join("state")
        .join("supervisor")
        .join("gateway-restart-requests.jsonl");
    let record = serde_json::json!({
        "schema": CHANNEL_GATEWAY_RESTART_REQUEST_SCHEMA,
        "status": "requested",
        "target": "gateway",
        "platform": "gateway",
        "requestingPlatform": turn.platform,
        "channelId": turn.channel_id,
        "userId": turn.user_id,
        "sessionKey": turn.session_key,
        "reason": reason,
        "requestFile": request_file,
        "receiptFile": receipt_file,
        "atMs": at_ms,
    });
    crate::write_json_atomic(&request_file, &record)?;
    crate::append_jsonl_value(&receipt_file, &record)?;

    Ok(GatewayRestartFiles {
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

fn normalize_restart_reason(reason: Option<&str>, fallback: &str) -> String {
    reason
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(fallback)
        .to_string()
}

fn restart_service_id(platform: &str) -> Option<&'static str> {
    match platform {
        "telegram" => Some("telegram-loop"),
        "discord" => Some("discord-gateway-loop"),
        _ => None,
    }
}

fn channel_restart_stop_file(harness_home: &Path, service_id: &str) -> io::Result<PathBuf> {
    let heartbeat_file = channel_restart_owner_file(harness_home, "loop-heartbeats", service_id);
    let heartbeat = read_channel_restart_owner_json(&heartbeat_file)?;
    if let Some(stop_file) = heartbeat
        .as_ref()
        .filter(|value| channel_restart_owner_is_live(value))
        .and_then(channel_restart_watched_stop_file)
    {
        return Ok(stop_file);
    }

    let service_file = channel_restart_owner_file(harness_home, "services", service_id);
    let service = read_channel_restart_owner_json(&service_file)?;
    if let Some(stop_file) = service
        .as_ref()
        .filter(|value| channel_restart_owner_is_live(value))
        .and_then(channel_restart_watched_stop_file)
    {
        return Ok(stop_file);
    }

    Ok(crate::loop_health::supervisor_stop_file_path(
        harness_home,
        service_id,
    ))
}

fn channel_restart_owner_conflict(
    harness_home: &Path,
    service_id: &str,
) -> io::Result<Option<String>> {
    let service_file = channel_restart_owner_file(harness_home, "services", service_id);
    let service = read_channel_restart_owner_json(&service_file)?;
    if let Some(service) = &service {
        if service
            .get("ownershipConflict")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            return Ok(Some(format!(
                "ownership-ambiguous: {service_id} service registry already reports ownership conflict"
            )));
        }
    }

    let heartbeat_file = channel_restart_owner_file(harness_home, "loop-heartbeats", service_id);
    let heartbeat = read_channel_restart_owner_json(&heartbeat_file)?;
    let service_alive = service.as_ref().is_some_and(channel_restart_owner_is_live);
    let heartbeat_alive = heartbeat
        .as_ref()
        .is_some_and(channel_restart_owner_is_live);

    if service_alive
        && service
            .as_ref()
            .and_then(|value| value.get("observedOnly"))
            .and_then(Value::as_bool)
            .unwrap_or(false)
    {
        return Ok(Some(format!(
            "ownership-ambiguous: {service_id} is live but observed-only; stop-file path is not machine-owned"
        )));
    }
    if heartbeat_alive
        && heartbeat
            .as_ref()
            .and_then(|value| value.get("observedOnly"))
            .and_then(Value::as_bool)
            .unwrap_or(false)
    {
        return Ok(Some(format!(
            "ownership-ambiguous: {service_id} heartbeat is live but observed-only; stop-file path is not machine-owned"
        )));
    }

    let service_generation = service
        .as_ref()
        .and_then(|value| value.get("generationId"))
        .and_then(Value::as_str);
    let heartbeat_generation = heartbeat
        .as_ref()
        .and_then(|value| value.get("generationId"))
        .and_then(Value::as_str);
    if service_alive
        && heartbeat_alive
        && service_generation.is_some()
        && heartbeat_generation.is_some()
        && service_generation != heartbeat_generation
    {
        return Ok(Some(format!(
            "ownership-ambiguous: {service_id} service registry and heartbeat generations differ"
        )));
    }
    Ok(None)
}

fn channel_restart_owner_file(harness_home: &Path, folder: &str, service_id: &str) -> PathBuf {
    harness_home
        .join("state")
        .join("supervisor")
        .join(folder)
        .join(format!("{service_id}.json"))
}

fn read_channel_restart_owner_json(path: &Path) -> io::Result<Option<Value>> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    serde_json::from_str(&text)
        .map(Some)
        .map_err(io::Error::other)
}

fn channel_restart_owner_process_id(value: &Value) -> Option<i64> {
    value
        .get("pid")
        .or_else(|| value.get("processId"))
        .and_then(Value::as_i64)
}

fn channel_restart_owner_is_live(value: &Value) -> bool {
    channel_restart_owner_process_id(value).and_then(crate::loop_health::process_alive_for_pid)
        == Some(true)
}

fn channel_restart_watched_stop_file(value: &Value) -> Option<PathBuf> {
    value
        .get("watchedStopFile")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn load_model_catalog_for_turn(
    turn: &TurnPlan,
) -> Option<crate::model_catalog::ModelCapabilityCatalog> {
    let harness_home = turn.harness_home.as_deref()?;
    let cache_file = harness_home.join("codex-home").join("models_cache.json");
    let text = fs::read_to_string(cache_file).ok()?;
    crate::model_catalog::parse_codex_model_catalog(&text).ok()
}

fn model_catalog_rollout_mode_for_turn(
    turn: &TurnPlan,
) -> crate::model_catalog::ModelCatalogRolloutMode {
    crate::model_catalog::model_catalog_rollout_mode_for_agent(
        turn.harness_home.as_deref(),
        turn.agent.as_ref().map(|agent| agent.id.as_str()),
    )
}

fn model_catalog_rollout_assessment_for_turn(
    turn: &TurnPlan,
) -> crate::model_catalog::ModelCatalogRolloutAssessment {
    crate::model_catalog::model_catalog_rollout_assessment_for_agent(
        turn.harness_home.as_deref(),
        turn.agent.as_ref().map(|agent| agent.id.as_str()),
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PersistedReasoningStatus {
    preference: Option<ReasoningPreference>,
    preference_source: &'static str,
    masked_agent_default: Option<ReasoningPreference>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct UnifiedThinkStatusSnapshot {
    agent_id: Option<String>,
    provider: Option<String>,
    model: Option<String>,
    persisted: PersistedReasoningStatus,
    rollout: String,
    backend_policy: &'static str,
    resolved_next_turn_effort: Option<String>,
    legacy_enabled: bool,
    legacy_level: Option<String>,
    catalog_default: Option<String>,
    catalog_efforts: Vec<String>,
    catalog_revision: Option<String>,
    authoritative: bool,
    detail: String,
}

fn read_agent_default_reasoning_preference(turn: &TurnPlan) -> Option<ReasoningPreference> {
    turn.harness_home
        .as_deref()
        .zip(turn.agent.as_ref())
        .and_then(|(harness_home, agent)| {
            crate::read_agent_override(harness_home, &agent.id)
                .ok()
                .flatten()
        })
        .and_then(|entry| entry.reasoning_preference)
}

fn persisted_reasoning_status(
    turn: &TurnPlan,
    agent_default: Option<ReasoningPreference>,
) -> PersistedReasoningStatus {
    if let Some(state) = turn.channel_state.as_ref() {
        if let Some(preference) = state.reasoning_preference.as_ref() {
            return PersistedReasoningStatus {
                preference: Some(preference.clone()),
                preference_source: "session",
                masked_agent_default: None,
            };
        }
        if state.thinking_enabled || state.thinking_level.is_some() {
            return PersistedReasoningStatus {
                preference: None,
                preference_source: "none",
                masked_agent_default: agent_default,
            };
        }
    }

    match agent_default {
        Some(preference) => PersistedReasoningStatus {
            preference: Some(preference),
            preference_source: "agent-default",
            masked_agent_default: None,
        },
        None => PersistedReasoningStatus {
            preference: None,
            preference_source: "none",
            masked_agent_default: None,
        },
    }
}

fn rollout_status_label(assessment: crate::model_catalog::ModelCatalogRolloutAssessment) -> String {
    let mode = |mode| match mode {
        crate::model_catalog::ModelCatalogRolloutMode::Off => "off",
        crate::model_catalog::ModelCatalogRolloutMode::Shadow => "shadow",
        crate::model_catalog::ModelCatalogRolloutMode::Authoritative => "authoritative",
    };
    if assessment.excluded {
        format!("excluded (configured={})", mode(assessment.configured_mode))
    } else {
        mode(assessment.effective_mode).to_string()
    }
}

fn unified_think_status_snapshot(turn: &TurnPlan) -> UnifiedThinkStatusSnapshot {
    let assessment = model_catalog_rollout_assessment_for_turn(turn);
    let catalog = load_model_catalog_for_turn(turn);
    let agent_default = read_agent_default_reasoning_preference(turn);
    let persisted = persisted_reasoning_status(turn, agent_default);
    let agent_id = turn.agent.as_ref().map(|agent| agent.id.clone());
    let provider = turn.model_policy.provider.clone();
    let model = turn.model_policy.model.clone();
    let route = catalog.as_ref().and_then(|catalog| {
        catalog.exact_route(
            provider.as_deref().unwrap_or_default(),
            model.as_deref().unwrap_or_default(),
        )
    });
    let catalog_default = route.and_then(|route| route.default_reasoning_effort.clone());
    let catalog_efforts = route
        .map(|route| route.supported_reasoning_efforts.clone())
        .unwrap_or_default();
    let catalog_revision = catalog.as_ref().map(|catalog| catalog.revision.clone());
    let authoritative =
        assessment.effective_mode == crate::model_catalog::ModelCatalogRolloutMode::Authoritative;
    let preference_matches_turn =
        persisted.preference.as_ref() == turn.reasoning_preference.as_ref();
    let policy = turn.backend_reasoning_policy.as_ref();
    let policy_effort = policy.map(|policy| policy.effective_effort());
    let policy_route_valid = policy.is_some_and(|policy| {
        policy
            .validate_for_execution_route(
                provider.as_deref().unwrap_or_default(),
                model.as_deref().unwrap_or_default(),
            )
            .is_ok()
    });
    let policy_effort_matches_preference = match persisted.preference.as_ref() {
        Some(ReasoningPreference::Default) => policy_effort == catalog_default.as_deref(),
        Some(ReasoningPreference::Explicit { effort }) => policy_effort == Some(effort.as_str()),
        None => policy.is_none(),
    };
    let policy_effort_is_current = policy_effort.is_some_and(|effort| {
        catalog_efforts
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(effort))
    });
    let ready = authoritative
        && persisted.preference.is_some()
        && preference_matches_turn
        && policy_route_valid
        && policy_effort_matches_preference
        && policy_effort_is_current;
    let has_backend_snapshot = persisted.preference.is_some()
        || turn.reasoning_preference.is_some()
        || turn.backend_reasoning_policy.is_some();
    let backend_policy = if authoritative && has_backend_snapshot {
        if ready { "ready" } else { "suspended" }
    } else if persisted.preference.is_some() {
        "dormant"
    } else if turn.thinking_policy.enabled {
        "legacy-only"
    } else {
        "unset"
    };
    let resolved_next_turn_effort = ready.then(|| {
        policy
            .expect("ready backend policy requires an execution policy")
            .effective_effort()
            .to_string()
    });
    let detail = match backend_policy {
        "ready" => "policy snapshot is internally consistent for the next turn".to_string(),
        "suspended" if !preference_matches_turn => {
            "persisted preference and TurnPlan preference differ; policy is stale/suspended"
                .to_string()
        }
        "suspended" if !policy_route_valid || !policy_effort_matches_preference => {
            "policy route or effort is stale/inconsistent with the current snapshot".to_string()
        }
        "suspended" => turn
            .warnings
            .iter()
            .find(|warning| warning.contains("backend reasoning"))
            .cloned()
            .unwrap_or_else(|| {
                "stored backend preference has no current route-valid execution policy".to_string()
            }),
        "dormant" => {
            "stored backend preference is dormant while rollout is not authoritative".to_string()
        }
        "legacy-only" => {
            "legacy prompt control is active; no backend effort is currently planned for the next turn"
                .to_string()
        }
        _ => "no thinking or backend reasoning preference is set".to_string(),
    };

    UnifiedThinkStatusSnapshot {
        agent_id,
        provider,
        model,
        persisted,
        rollout: rollout_status_label(assessment),
        backend_policy,
        resolved_next_turn_effort,
        legacy_enabled: turn.thinking_policy.enabled,
        legacy_level: turn
            .thinking_policy
            .enabled
            .then(|| turn.thinking_policy.level.clone())
            .flatten(),
        catalog_default,
        catalog_efforts,
        catalog_revision,
        authoritative,
        detail,
    }
}

fn unified_thinking_status_effect(turn: &TurnPlan) -> ChannelCommandEffect {
    let snapshot = unified_think_status_snapshot(turn);
    let reason = format!(
        "Rollout: {}\nBackend policy: {}\nPreference source: {}\nMasked agent default: {}\nResolved next-turn effort: {}\nLegacy prompt enabled: {}\nLegacy prompt level: {}\nRuntime revalidation: required before turn/start; status is not wire execution evidence\nDetail: {}",
        snapshot.rollout,
        snapshot.backend_policy,
        snapshot.persisted.preference_source,
        display_reasoning_preference_or_dash(snapshot.persisted.masked_agent_default.as_ref()),
        snapshot.resolved_next_turn_effort.as_deref().unwrap_or("-"),
        yes_no(snapshot.legacy_enabled),
        snapshot.legacy_level.as_deref().unwrap_or("-"),
        snapshot.detail
    );

    ChannelCommandEffect::ShowReasoning {
        agent_id: snapshot.agent_id,
        provider: snapshot.provider,
        model: snapshot.model,
        current_preference: snapshot.persisted.preference,
        available_efforts: snapshot.catalog_efforts,
        catalog_default: snapshot.catalog_default,
        catalog_revision: snapshot.catalog_revision,
        authoritative: snapshot.authoritative,
        reason,
    }
}

fn reasoning_resolution_for_turn(
    turn: &TurnPlan,
    requested_effort: &str,
) -> crate::model_catalog::ReasoningResolutionReceipt {
    let catalog = load_model_catalog_for_turn(turn);
    let mode = model_catalog_rollout_mode_for_turn(turn);
    crate::model_catalog::resolve_reasoning_effort(
        catalog.as_ref(),
        mode,
        turn.model_policy.provider.as_deref().unwrap_or_default(),
        turn.model_policy.model.as_deref().unwrap_or_default(),
        requested_effort,
        crate::model_catalog::UnsupportedReasoningPolicy::Reject,
    )
}

fn unified_thinking_command_effect(
    turn: &TurnPlan,
    effort: Option<String>,
    global: bool,
) -> ChannelCommandEffect {
    let Some(effort) = effort else {
        return unified_thinking_status_effect(turn);
    };
    if model_catalog_rollout_mode_for_turn(turn)
        == crate::model_catalog::ModelCatalogRolloutMode::Authoritative
    {
        reasoning_command_effect(turn, effort, global)
    } else {
        thinking_command_effect(turn, effort, global)
    }
}

fn reasoning_command_effect(turn: &TurnPlan, effort: String, global: bool) -> ChannelCommandEffect {
    let agent_id = turn.agent.as_ref().map(|agent| agent.id.clone());
    let provider = turn.model_policy.provider.clone();
    let model = turn.model_policy.model.clone();
    let catalog = load_model_catalog_for_turn(turn);
    let mode = model_catalog_rollout_mode_for_turn(turn);
    let route = catalog.as_ref().and_then(|catalog| {
        catalog.exact_route(
            provider.as_deref().unwrap_or_default(),
            model.as_deref().unwrap_or_default(),
        )
    });
    let catalog_revision = catalog.as_ref().map(|catalog| catalog.revision.clone());
    let catalog_default = route.and_then(|route| route.default_reasoning_effort.clone());
    let available_efforts = if mode == crate::model_catalog::ModelCatalogRolloutMode::Off {
        Vec::new()
    } else {
        route
            .map(|route| route.supported_reasoning_efforts.clone())
            .unwrap_or_default()
    };
    let authoritative = mode == crate::model_catalog::ModelCatalogRolloutMode::Authoritative;
    let rollout_reason = match mode {
        crate::model_catalog::ModelCatalogRolloutMode::Off => {
            "backend reasoning is disabled or this agent is outside the rollout cohort"
        }
        crate::model_catalog::ModelCatalogRolloutMode::Shadow => {
            "backend reasoning is shadow-only; no preference will be persisted or sent"
        }
        crate::model_catalog::ModelCatalogRolloutMode::Authoritative => {
            "catalog snapshot is authoritative for this agent"
        }
    };

    let normalized_requested = effort.trim().to_ascii_lowercase();
    let (preference, resolution, legacy_alias_from) = if normalized_requested
        .eq_ignore_ascii_case("default")
    {
        (
            ReasoningPreference::Default,
            catalog_default
                .as_deref()
                .map(|default| reasoning_resolution_for_turn(turn, default)),
            None,
        )
    } else {
        let exact_resolution = reasoning_resolution_for_turn(turn, &normalized_requested);
        if exact_resolution.status == crate::model_catalog::ReasoningResolutionStatus::Accepted {
            (
                ReasoningPreference::explicit(normalized_requested.clone())
                    .expect("parsed /think effort must be non-empty"),
                Some(exact_resolution),
                None,
            )
        } else if let Some(alias) = crate::normalize_thinking_level(&normalized_requested)
            && alias != normalized_requested
        {
            let alias_resolution = reasoning_resolution_for_turn(turn, &alias);
            if alias_resolution.status == crate::model_catalog::ReasoningResolutionStatus::Accepted
            {
                (
                    ReasoningPreference::explicit(alias)
                        .expect("legacy /think alias must be non-empty"),
                    Some(alias_resolution),
                    Some(normalized_requested),
                )
            } else {
                (
                    ReasoningPreference::explicit(normalized_requested)
                        .expect("parsed /think effort must be non-empty"),
                    Some(exact_resolution),
                    None,
                )
            }
        } else {
            (
                ReasoningPreference::explicit(normalized_requested)
                    .expect("parsed /think effort must be non-empty"),
                Some(exact_resolution),
                None,
            )
        }
    };
    let ultra_requires_separate_authorization = matches!(
        &preference,
        ReasoningPreference::Explicit { effort } if effort == "ultra"
    );
    let default_requested = matches!(preference, ReasoningPreference::Default);
    let resolved_policy =
        if authoritative && !default_requested && !ultra_requires_separate_authorization {
            resolution.clone().and_then(|resolution| {
                (resolution.status == crate::model_catalog::ReasoningResolutionStatus::Accepted)
                    .then(|| {
                        BackendReasoningPolicyV1::new(
                            BackendReasoningSource::ChannelCommand,
                            resolution,
                        )
                    })
                    .and_then(Result::ok)
            })
        } else {
            None
        };
    let accepted = if default_requested {
        authoritative && route.is_some()
    } else {
        resolved_policy.is_some()
    };
    let reason = if ultra_requires_separate_authorization {
        "ultra requires an explicit agent allow-list and delegation/resource authorization receipt"
            .to_string()
    } else if default_requested && accepted {
        if let Some(default) = catalog_default.as_deref() {
            format!(
                "default reset recorded; runtime will use catalog default `{default}` only when the backend thread requires an explicit sticky reset"
            )
        } else {
            "default reset recorded as pending; model execution is blocked until the exact route advertises a catalog default effort"
                .to_string()
        }
    } else if accepted {
        legacy_alias_from.map_or_else(
            || {
                "preference accepted by the command snapshot; same-connection runtime verification remains pending"
                    .to_string()
            },
            |requested| {
                format!(
                    "legacy /think alias `{requested}` resolved to `{}`; same-connection runtime verification remains pending",
                    preference.explicit_effort().unwrap_or("default")
                )
            },
        )
    } else if default_requested {
        "the exact route is unavailable; default reset was not recorded".to_string()
    } else {
        resolution
            .as_ref()
            .map(|resolution| resolution.reason.clone())
            .unwrap_or_else(|| rollout_reason.to_string())
    };

    ChannelCommandEffect::SwitchReasoning {
        agent_id,
        provider,
        model,
        preference,
        global,
        accepted,
        resolved_policy,
        resolution,
        available_efforts,
        catalog_default,
        catalog_revision,
        reason,
    }
}

fn thinking_command_effect(turn: &TurnPlan, level: String, global: bool) -> ChannelCommandEffect {
    let available_levels = available_thinking_levels(turn);
    let agent_id = turn.agent.as_ref().map(|agent| agent.id.clone());
    let provider = turn.model_policy.provider.clone();
    let model = turn.model_policy.model.clone();
    let thinking_enabled = turn.thinking_policy.enabled;
    let current_level = current_thinking_level(turn);
    let normalized = crate::normalize_thinking_level(&level)
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

fn fast_command_effect(
    turn: &TurnPlan,
    mode: FastCommandMode,
    global: bool,
) -> ChannelCommandEffect {
    let agent_id = turn.agent.as_ref().map(|agent| agent.id.clone());
    let provider = turn.model_policy.provider.clone();
    let model = turn.model_policy.model.clone();
    let current_mode = turn.provider_request_policy.fast_mode.clone();
    let request_policy = fast_request_policy_for_route(
        &provider,
        &model,
        mode_to_state(mode, &current_mode),
        turn.harness_home.as_deref(),
    );
    match mode {
        FastCommandMode::Status => ChannelCommandEffect::ShowFast {
            agent_id,
            provider,
            model,
            current_mode,
            effective_acceleration: request_policy.effective_acceleration,
            reason: request_policy.reason,
        },
        FastCommandMode::Fast | FastCommandMode::Normal => ChannelCommandEffect::SwitchFast {
            agent_id,
            provider,
            model,
            global,
            previous_mode: current_mode,
            mode: mode_to_state(mode, "normal").to_string(),
            effective_acceleration: request_policy.effective_acceleration,
            reason: request_policy.reason,
        },
    }
}

fn mode_to_state(mode: FastCommandMode, current: &str) -> &str {
    match mode {
        FastCommandMode::Status => current,
        FastCommandMode::Fast => "fast",
        FastCommandMode::Normal => "normal",
    }
}

pub fn fast_request_policy_for_route(
    provider: &Option<String>,
    model: &Option<String>,
    mode: &str,
    harness_home: Option<&Path>,
) -> FastRequestRoutePolicy {
    let fast_service_tier = codex_fast_service_tier_for_model(provider, model, harness_home);
    if mode != "fast" {
        if fast_service_tier.is_some() {
            return FastRequestRoutePolicy {
                effective_acceleration: "disabled".to_string(),
                reason:
                    "fast mode is normal; Codex app-server serviceTier=default will be requested"
                        .to_string(),
                service_tier: Some("default".to_string()),
            };
        }
        return FastRequestRoutePolicy {
            effective_acceleration: "disabled".to_string(),
            reason: "fast mode is normal for this session".to_string(),
            service_tier: None,
        };
    }
    if let Some(service_tier) = fast_service_tier {
        return FastRequestRoutePolicy {
            effective_acceleration: "enabled".to_string(),
            reason: format!(
                "Codex app-server serviceTier={service_tier} will be requested for this model"
            ),
            service_tier: Some(service_tier),
        };
    }
    FastRequestRoutePolicy {
        effective_acceleration: "unsupported".to_string(),
        reason:
            "Codex app-server model catalog does not advertise a Fast service tier for this route"
                .to_string(),
        service_tier: None,
    }
}

fn codex_fast_service_tier_for_model(
    provider: &Option<String>,
    model: &Option<String>,
    harness_home: Option<&Path>,
) -> Option<String> {
    if !is_codex_openai_provider(provider) {
        return None;
    }
    let model = model.as_deref()?.trim();
    if model.is_empty() {
        return None;
    }
    let cache_file = harness_home?.join("codex-home").join("models_cache.json");
    let cache = fs::read_to_string(cache_file).ok()?;
    let catalog = serde_json::from_str::<Value>(&cache).ok()?;
    catalog
        .get("models")
        .and_then(Value::as_array)?
        .iter()
        .find(|entry| codex_catalog_entry_matches_model(entry, model))
        .and_then(codex_fast_service_tier_id)
}

fn is_codex_openai_provider(provider: &Option<String>) -> bool {
    match provider.as_deref().map(str::trim) {
        Some(provider) if provider.eq_ignore_ascii_case("openai") => true,
        Some(provider) if provider.eq_ignore_ascii_case("codex") => true,
        Some(_) => false,
        None => true,
    }
}

fn codex_catalog_entry_matches_model(entry: &Value, model: &str) -> bool {
    ["slug", "id", "model"].iter().any(|key| {
        entry
            .get(*key)
            .and_then(Value::as_str)
            .is_some_and(|candidate| candidate.eq_ignore_ascii_case(model))
    })
}

fn codex_fast_service_tier_id(entry: &Value) -> Option<String> {
    for key in ["service_tiers", "serviceTiers"] {
        if let Some(service_tiers) = entry.get(key).and_then(Value::as_array) {
            for tier in service_tiers {
                let tier_id = tier.get("id").and_then(Value::as_str)?.trim();
                if tier_id.is_empty() {
                    continue;
                }
                let tier_name = tier.get("name").and_then(Value::as_str).unwrap_or_default();
                if tier_name.eq_ignore_ascii_case("fast")
                    || tier_id.eq_ignore_ascii_case("priority")
                    || tier_id.eq_ignore_ascii_case("fast")
                {
                    return Some(tier_id.to_string());
                }
            }
        }
    }
    for key in ["additional_speed_tiers", "additionalSpeedTiers"] {
        if entry
            .get(key)
            .and_then(Value::as_array)
            .is_some_and(|tiers| {
                tiers.iter().any(|tier| {
                    tier.as_str()
                        .is_some_and(|tier| tier.eq_ignore_ascii_case("fast"))
                })
            })
        {
            return Some("fast".to_string());
        }
    }
    None
}

#[cfg(test)]
fn write_codex_models_cache_for_test(harness_home: &Path) {
    let codex_home = harness_home.join("codex-home");
    fs::create_dir_all(&codex_home).unwrap();
    fs::write(
        codex_home.join("models_cache.json"),
        r#"{
          "models": [
            {
              "slug": "gpt-5.5",
              "service_tiers": [
                { "id": "priority", "name": "Fast" }
              ]
            },
            {
              "slug": "gpt-5.4-mini",
              "service_tiers": []
            },
            {
              "slug": "gpt-5.6-sol",
              "default_reasoning_level": "low",
              "supported_reasoning_levels": [
                { "effort": "low" },
                { "effort": "medium" },
                { "effort": "high" },
                { "effort": "xhigh" },
                { "effort": "max" },
                { "effort": "ultra" }
              ],
              "service_tiers": [
                { "id": "priority", "name": "Fast" }
              ],
              "comp_hash": 3000
            },
            {
              "slug": "gpt-5.6-luna",
              "default_reasoning_level": "medium",
              "supported_reasoning_levels": [
                { "effort": "low" },
                { "effort": "medium" },
                { "effort": "high" },
                { "effort": "xhigh" },
                { "effort": "max" }
              ],
              "service_tiers": [
                { "id": "priority", "name": "Fast" }
              ],
              "comp_hash": "3000"
            }
          ]
        }"#,
    )
    .unwrap();
}

#[cfg(test)]
fn write_model_catalog_mode_for_test(harness_home: &Path, mode: &str) {
    fs::create_dir_all(harness_home).unwrap();
    fs::write(
        harness_home.join(crate::HARNESS_CONFIG_FILE_NAME),
        format!(r#"{{"orchestration":{{"features":{{"modelCatalogV2":{{"mode":"{mode}"}}}}}}}}"#),
    )
    .unwrap();
}

#[cfg(test)]
fn write_model_catalog_cohort_for_test(
    harness_home: &Path,
    mode: &str,
    enabled_agent_ids: &[&str],
) {
    fs::create_dir_all(harness_home).unwrap();
    let config = serde_json::json!({
        "orchestration": {
            "features": {
                "modelCatalogV2": {
                    "mode": mode,
                    "enabledAgentIds": enabled_agent_ids,
                }
            }
        }
    });
    fs::write(
        harness_home.join(crate::HARNESS_CONFIG_FILE_NAME),
        serde_json::to_vec(&config).unwrap(),
    )
    .unwrap();
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
        ChannelCommandEffect::ShowReasoning {
            agent_id,
            provider,
            model,
            current_preference,
            available_efforts,
            catalog_default,
            catalog_revision,
            reason,
            ..
        } => format!(
            "Thinking control: /think (alias: /reasoning)\nAgent: {}\nModel: {}\nStored backend preference: {}\n{}\nCatalog default: {}\nCatalog efforts (observed): {}\nCatalog revision: {}",
            display_opt(agent_id),
            display_model_route(provider, model),
            display_reasoning_preference(current_preference.as_ref()),
            reason,
            display_opt(catalog_default),
            display_list(available_efforts),
            display_opt(catalog_revision)
        ),
        ChannelCommandEffect::SwitchReasoning {
            agent_id,
            provider,
            model,
            preference,
            global,
            accepted,
            available_efforts,
            reason,
            ..
        } => {
            if *accepted {
                format!(
                    "Backend reasoning preference requested{} for agent `{}`: {}\nModel: {}\nRuntime verification: pending\n{}",
                    if *global {
                        " globally"
                    } else {
                        " for this session"
                    },
                    display_opt(agent_id),
                    display_reasoning_preference(Some(preference)),
                    display_model_route(provider, model),
                    reason
                )
            } else {
                format!(
                    "Backend reasoning preference was not recorded for {}: {}\nAvailable backend efforts: {}\n{}",
                    display_model_route(provider, model),
                    display_reasoning_preference(Some(preference)),
                    display_list(available_efforts),
                    reason
                )
            }
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
        ChannelCommandEffect::RestartGateway {
            status,
            detail,
            reason,
            ..
        } => {
            if status == "requested" {
                format!(
                    "{}{}\n{}",
                    detail,
                    reason
                        .as_ref()
                        .map(|reason| format!("\nReason: {reason}"))
                        .unwrap_or_default(),
                    "Operator token and idle-gate control are required before restart execution."
                )
            } else {
                format!("Restart request status: {status}\n{detail}")
            }
        }
        ChannelCommandEffect::RestartStatus { status, detail, .. } => {
            if status == "ok" {
                detail.clone()
            } else {
                format!("Restart status failed: {detail}")
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
        ChannelCommandEffect::ShowFast {
            provider,
            model,
            current_mode,
            effective_acceleration,
            reason,
            ..
        } => format!(
            "Fast mode: {}\nScope: current session\nRoute: {}\nRequest acceleration: {} ({})",
            current_mode,
            display_model_route(provider, model),
            effective_acceleration,
            reason
        ),
        ChannelCommandEffect::SwitchFast {
            agent_id,
            provider,
            model,
            global,
            mode,
            effective_acceleration,
            reason,
            ..
        } => {
            let scope = if *global {
                format!(
                    "current session and agent `{}` default",
                    display_opt(agent_id)
                )
            } else {
                "current session".to_string()
            };
            format!(
                "Fast mode: {}\nScope: {}\nRoute: {}\nRequest acceleration: {} ({})",
                mode,
                scope,
                display_model_route(provider, model),
                effective_acceleration,
                reason
            )
        }
        ChannelCommandEffect::ShowStatus { snapshot, .. } => status_reply_text(snapshot),
        ChannelCommandEffect::UnknownCommand { name, rest, detail } => {
            let mut text = format!("Unknown or unsupported command: /{name}");
            if let Some(rest) = rest {
                text.push(' ');
                text.push_str(rest);
            }
            text.push('\n');
            text.push_str(detail);
            text
        }
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
            "Agent Harness Model Status\nAgent: {}\nProvider: {}\nModel: {}\nOverride: {}\nThinking: {}, level={}\nFast: {}",
            display_opt(&snapshot.current_agent_id),
            display_opt(&snapshot.current_provider),
            display_opt(&snapshot.current_model),
            display_opt(&snapshot.model_override),
            yes_no(snapshot.thinking_enabled),
            display_thinking_level(&snapshot.thinking_level),
            display_opt(&snapshot.fast_mode)
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
            "Agent Harness Status\nAgent: {} ({}/{})\nModel: provider={}, model={}, override={}\nThinking: enabled={}, level={}\nFast: {}\nSecurity: approvals={}, windowsSandbox={}, filesystemSandbox={}\nChannels: telegram={}, discord={}, current={}\nSession: active={}, stateLoaded={}\nPrompt: files {}/{} ({})\nSkills: {} selected ({})\nState: steer={}, btw={}\nRegistry: providers={}, plugins={}",
            display_opt(&snapshot.current_agent_id),
            snapshot.agents_enabled,
            snapshot.agents_total,
            display_opt(&snapshot.current_provider),
            display_opt(&snapshot.current_model),
            display_opt(&snapshot.model_override),
            yes_no(snapshot.thinking_enabled),
            display_thinking_level(&snapshot.thinking_level),
            display_opt(&snapshot.fast_mode),
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

fn restart_status_summary(report: &GatewayRestartStatusReport) -> String {
    let latest_request = report
        .latest_request
        .as_ref()
        .and_then(|value| value.at_ms)
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string());
    let consumed = report
        .latest_consumption
        .as_ref()
        .and_then(|value| value.consumed_at_ms)
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string());
    let completion = report
        .latest_completion
        .as_ref()
        .and_then(|value| value.heartbeat_at_ms)
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string());
    format!(
        "Restart status:\nLatest request at: {latest_request}\nConsumed at: {consumed}\nCompletion heartbeat at: {completion}\nGateway service: {}\nGateway generation: {}\nGateway heartbeat: {}",
        report.service.status.as_deref().unwrap_or("-"),
        report
            .service
            .generation_id
            .as_deref()
            .or(report.heartbeat.generation_id.as_deref())
            .unwrap_or("-"),
        report.heartbeat.status.as_deref().unwrap_or("-")
    )
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
        fast_mode: turn
            .channel_state
            .as_ref()
            .and_then(|state| state.fast_mode.clone()),
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
        source_queue_id: None,
        source_completion_file: None,
        text,
        presentation: None,
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

fn display_reasoning_preference(preference: Option<&ReasoningPreference>) -> &str {
    match preference {
        None => "unset",
        Some(ReasoningPreference::Default) => "default",
        Some(ReasoningPreference::Explicit { effort }) => effort.as_str(),
    }
}

fn display_reasoning_preference_or_dash(preference: Option<&ReasoningPreference>) -> &str {
    preference
        .map(|preference| display_reasoning_preference(Some(preference)))
        .unwrap_or("-")
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
    fn channel_step_replies_to_unknown_slash_command_without_agent_turn() {
        let root = temp_root("channel_step_replies_to_unknown_slash_command_without_agent_turn");
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
                text: "/unknown value".to_string(),
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
        assert_eq!(
            step.outbound_messages[0].kind,
            ChannelOutboundMessageKind::CommandReply
        );
        assert!(
            step.outbound_messages[0]
                .text
                .contains("no model turn was started")
        );
        assert!(matches!(
            step.command_effect,
            Some(ChannelCommandEffect::UnknownCommand {
                ref name,
                ref rest,
                ..
            }) if name == "unknown" && rest.as_deref() == Some("value")
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
        let stale_plan_stop_file = supervisor_dir.join("stop").join("telegram-loop.stop");
        let stop_file =
            crate::loop_health::supervisor_stop_file_path(&harness_home, "telegram-loop");
        fs::create_dir_all(stale_plan_stop_file.parent().unwrap()).unwrap();
        fs::write(
            supervisor_dir.join("supervisor-plan.json"),
            serde_json::to_string(&serde_json::json!({
                "tasks": [
                    { "component": "telegram-loop", "stopFile": stale_plan_stop_file }
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
                text: "/restart telegram reconnect adapter".to_string(),
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
                stop_file: ref effect_stop_file,
                ..
            }) if platform == "telegram"
                && service_id.as_deref() == Some("telegram-loop")
                && status == "requested"
                && reason.as_deref() == Some("reconnect adapter")
                && effect_stop_file.as_ref() == Some(&stop_file)
        ));
        assert!(!stale_plan_stop_file.is_file());
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
    fn channel_restart_prefers_live_watched_stop_file() {
        let root = temp_root("channel_restart_prefers_live_watched_stop_file");
        let source = write_channel_source(&root);
        let harness_home = root.join(".agent-harness");
        let service_id = "telegram-loop";
        let pid = std::process::id();
        let generation_id = "telegram-loop-live-generation";
        let watched_stop_file = harness_home
            .join("state")
            .join("supervisor")
            .join("live-stop-files")
            .join("telegram-loop.live.stop");
        let canonical_stop_file =
            crate::loop_health::supervisor_stop_file_path(&harness_home, service_id);
        let services_dir = harness_home
            .join("state")
            .join("supervisor")
            .join("services");
        let heartbeat_dir = harness_home
            .join("state")
            .join("supervisor")
            .join("loop-heartbeats");
        fs::create_dir_all(&services_dir).unwrap();
        fs::create_dir_all(&heartbeat_dir).unwrap();
        fs::write(
            services_dir.join(format!("{service_id}.json")),
            serde_json::to_string(&serde_json::json!({
                "schema": "agent-harness.supervisor-service-state.v1",
                "serviceId": service_id,
                "serviceKind": "telegram-ingress",
                "pid": pid,
                "processId": pid,
                "generationId": generation_id,
                "launchOwner": "rust-supervisor-run",
                "observedOnly": false,
                "status": "running",
                "actualState": "running",
                "watchedStopFile": watched_stop_file
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            heartbeat_dir.join(format!("{service_id}.json")),
            serde_json::to_string(&serde_json::json!({
                "schema": "agent-harness.loop-heartbeat.v1",
                "serviceId": service_id,
                "processId": pid,
                "generationId": generation_id,
                "launchOwner": "rust-supervisor-run",
                "observedOnly": false,
                "status": "heartbeat",
                "atMs": 10_000,
                "watchedStopFile": watched_stop_file
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
                text: "/restart telegram reconnect adapter".to_string(),
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
        assert!(matches!(
            step.command_effect,
            Some(ChannelCommandEffect::RestartChannel {
                ref status,
                stop_file: ref effect_stop_file,
                ..
            }) if status == "requested" && effect_stop_file.as_ref() == Some(&watched_stop_file)
        ));
        assert!(watched_stop_file.is_file());
        assert!(!canonical_stop_file.exists());
        let stop_value: Value =
            serde_json::from_slice(&fs::read(&watched_stop_file).unwrap()).unwrap();
        assert_eq!(stop_value["serviceId"], service_id);
        assert_eq!(stop_value["createdBy"], "channel-restart-command");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn channel_restart_fails_when_live_owner_is_observed_only() {
        let root = temp_root("channel_restart_fails_when_live_owner_is_observed_only");
        let source = write_channel_source(&root);
        let harness_home = root.join(".agent-harness");
        let services_dir = harness_home
            .join("state")
            .join("supervisor")
            .join("services");
        fs::create_dir_all(&services_dir).unwrap();
        fs::write(
            services_dir.join("telegram-loop.json"),
            serde_json::to_string(&serde_json::json!({
                "schema": "agent-harness.supervisor-service-state.v1",
                "serviceId": "telegram-loop",
                "serviceKind": "telegram-ingress",
                "pid": std::process::id(),
                "processId": std::process::id(),
                "generationId": "manual-telegram-loop",
                "launchOwner": "external-runner-observe-only",
                "observedOnly": true,
                "status": "running",
                "actualState": "running"
            }))
            .unwrap(),
        )
        .unwrap();
        let stop_file =
            crate::loop_health::supervisor_stop_file_path(&harness_home, "telegram-loop");
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
                text: "/restart telegram reconnect adapter".to_string(),
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
                .contains("ownership-ambiguous")
        );
        assert!(matches!(
            step.command_effect,
            Some(ChannelCommandEffect::RestartChannel {
                ref status,
                ref detail,
                stop_file: None,
                ..
            }) if status == "failed" && detail.contains("ownership-ambiguous")
        ));
        assert!(!stop_file.exists());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn channel_restart_stop_file_targets_live_owner_or_fails_explicit() {
        channel_restart_prefers_live_watched_stop_file();
        channel_restart_fails_when_live_owner_is_observed_only();
    }

    #[test]
    fn channel_step_requests_gateway_restart_for_bare_restart_command() {
        let root = temp_root("channel_step_requests_gateway_restart_for_bare_restart_command");
        let source = write_channel_source(&root);
        let harness_home = root.join(".agent-harness");
        let stop_file =
            crate::loop_health::supervisor_stop_file_path(&harness_home, "telegram-loop");
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
                text: "/restart".to_string(),
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
                .contains("protected gateway restart request recorded")
        );
        assert!(
            step.outbound_messages[0]
                .text
                .contains("Operator token and idle-gate control are required")
        );
        assert!(matches!(
            step.command_effect,
            Some(ChannelCommandEffect::RestartGateway {
                ref status,
                ref reason,
                ref request_file,
                ref receipt_file,
                ..
            }) if status == "requested"
                && reason.is_none()
                && request_file.as_ref().is_some_and(|path| path.is_file())
                && receipt_file.as_ref().is_some_and(|path| path.is_file())
        ));
        assert!(!stop_file.exists());
        assert!(
            harness_home
                .join("state")
                .join("supervisor")
                .join("gateway-restart-requests.jsonl")
                .is_file()
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn channel_step_replies_to_restart_status_without_agent_turn() {
        let root = temp_root("channel_step_replies_to_restart_status_without_agent_turn");
        let source = write_channel_source(&root);
        let harness_home = root.join(".agent-harness");
        let supervisor_dir = harness_home.join("state").join("supervisor");
        fs::create_dir_all(&supervisor_dir).unwrap();
        fs::write(
            supervisor_dir.join("gateway-restart-requests.jsonl"),
            "{\"status\":\"requested\",\"requestFile\":\"request.json\",\"atMs\":1000}\n\
             {\"status\":\"consumed\",\"requestFile\":\"request.json\",\"consumedRequestFile\":\"consumed.json\",\"consumedAtMs\":1100,\"consumedBy\":\"discord-gateway-loop\",\"generationId\":\"gateway-generation-1\"}\n",
        )
        .unwrap();
        fs::write(
            supervisor_dir.join("gateway-restart-completions.jsonl"),
            "{\"status\":\"completed\",\"requestFile\":\"request.json\",\"consumedRequestFile\":\"consumed.json\",\"heartbeatAtMs\":1200,\"heartbeatStatus\":\"spawning\",\"heartbeatGenerationId\":\"gateway-generation-1\"}\n",
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
                platform: "discord".to_string(),
                channel_id: "dm".to_string(),
                user_id: "operator".to_string(),
                text: "/restart status".to_string(),
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
        assert!(step.outbound_messages[0].text.contains("Restart status"));
        assert!(step.outbound_messages[0].text.contains("Consumed at: 1100"));
        assert!(matches!(
            step.command_effect,
            Some(ChannelCommandEffect::RestartStatus {
                ref status,
                report: Some(ref report),
                ..
            }) if status == "ok"
                && report
                    .latest_completion
                    .as_ref()
                    .and_then(|completion| completion.heartbeat_at_ms)
                    == Some(1200)
        ));

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
        let show_text = &show_step.outbound_messages[0].text;
        assert!(show_text.contains("Thinking control: /think (alias: /reasoning)"));
        assert!(show_text.contains("Rollout: off"));
        assert!(show_text.contains("Backend policy: unset"));
        assert!(show_text.contains("Legacy prompt enabled: no"));
        assert!(show_text.contains("Legacy prompt level: -"));

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
    fn unified_think_status_is_alias_identical_and_reports_authoritative_state() {
        let root = temp_root("unified_think_status_authoritative");
        let source = write_channel_source(&root);
        let harness_home = root.join(".agent-harness");
        write_codex_models_cache_for_test(&harness_home);
        write_model_catalog_mode_for_test(&harness_home, "authoritative");
        let registry = load_agent_registry(&source).unwrap();
        let skills = build_source_skill_index(&source).unwrap();
        let build = |text: &str| {
            build_turn_plan(
                &source,
                &registry,
                &skills,
                TurnPlanInput {
                    harness_home: Some(harness_home.clone()),
                    platform: "telegram".to_string(),
                    channel_id: "dm".to_string(),
                    user_id: "user".to_string(),
                    text: text.to_string(),
                    inbound_context: None,
                    inbound_media_artifacts: Vec::new(),
                    requested_agent_id: Some("main56sol".to_string()),
                    session_hint: None,
                    skill_limit: 3,
                },
            )
            .unwrap()
        };
        let apply = |text: &str, now_ms: i64| {
            crate::apply_channel_command_step(
                &build_channel_step(&registry, &build(text)),
                crate::ChannelCommandApplyOptions {
                    harness_home: harness_home.clone(),
                    now_ms,
                },
            )
            .unwrap()
            .state
            .unwrap()
        };

        let baseline = apply("/think max", 100);
        let think_text = build_channel_step(&registry, &build("/think")).outbound_messages[0]
            .text
            .clone();
        let reasoning_text = build_channel_step(&registry, &build("/reasoning")).outbound_messages
            [0]
        .text
        .clone();
        assert_eq!(think_text, reasoning_text);
        for expected in [
            "Thinking control: /think (alias: /reasoning)",
            "Agent: main56sol",
            "Model: openai/gpt-5.6-sol",
            "Rollout: authoritative",
            "Backend policy: ready",
            "Stored backend preference: max",
            "Preference source: session",
            "Masked agent default: -",
            "Resolved next-turn effort: max",
            "Legacy prompt enabled: yes",
            "Legacy prompt level: max",
            "Runtime revalidation: required before turn/start; status is not wire execution evidence",
            "Catalog default: low",
            "Catalog efforts (observed): low, medium, high, xhigh, max, ultra",
            "Catalog revision:",
        ] {
            assert!(
                think_text.contains(expected),
                "missing `{expected}` in {think_text}"
            );
        }

        let after_status = apply("/think", 101);
        assert_eq!(
            after_status.reasoning_preference,
            baseline.reasoning_preference
        );
        assert_eq!(
            after_status.backend_reasoning_policy,
            baseline.backend_reasoning_policy
        );
        assert_eq!(after_status.thinking_enabled, baseline.thinking_enabled);
        assert_eq!(after_status.thinking_level, baseline.thinking_level);

        let drift_turn = build_turn_plan(
            &source,
            &registry,
            &skills,
            TurnPlanInput {
                harness_home: Some(harness_home.clone()),
                platform: "telegram".to_string(),
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                text: "/reasoning".to_string(),
                inbound_context: None,
                inbound_media_artifacts: Vec::new(),
                requested_agent_id: Some("main56luna".to_string()),
                session_hint: None,
                skill_limit: 3,
            },
        )
        .unwrap();
        let drift_step = build_channel_step(&registry, &drift_turn);
        assert!(
            drift_step.outbound_messages[0]
                .text
                .contains("openai/gpt-5.6-luna")
        );
        let drift_state = crate::apply_channel_command_step(
            &drift_step,
            crate::ChannelCommandApplyOptions {
                harness_home: harness_home.clone(),
                now_ms: 102,
            },
        )
        .unwrap()
        .state
        .unwrap();
        let mut expected_control_state = after_status;
        expected_control_state.last_command = drift_state.last_command.clone();
        expected_control_state.updated_at_ms = drift_state.updated_at_ms;
        assert_eq!(drift_state, expected_control_state);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn unified_think_status_reports_default_and_suspended_route_without_inference() {
        let root = temp_root("unified_think_status_default_suspended");
        let source = write_channel_source(&root);
        let harness_home = root.join(".agent-harness");
        write_codex_models_cache_for_test(&harness_home);
        write_model_catalog_mode_for_test(&harness_home, "authoritative");
        let registry = load_agent_registry(&source).unwrap();
        let skills = build_source_skill_index(&source).unwrap();
        let build = |text: &str| {
            build_turn_plan(
                &source,
                &registry,
                &skills,
                TurnPlanInput {
                    harness_home: Some(harness_home.clone()),
                    platform: "telegram".to_string(),
                    channel_id: "dm".to_string(),
                    user_id: "user".to_string(),
                    text: text.to_string(),
                    inbound_context: None,
                    inbound_media_artifacts: Vec::new(),
                    requested_agent_id: Some("main56sol".to_string()),
                    session_hint: None,
                    skill_limit: 3,
                },
            )
            .unwrap()
        };
        let apply = |text: &str, now_ms: i64| {
            crate::apply_channel_command_step(
                &build_channel_step(&registry, &build(text)),
                crate::ChannelCommandApplyOptions {
                    harness_home: harness_home.clone(),
                    now_ms,
                },
            )
            .unwrap();
        };

        apply("/think default", 110);
        let default_text = build_channel_step(&registry, &build("/think")).outbound_messages[0]
            .text
            .clone();
        for expected in [
            "Backend policy: ready",
            "Stored backend preference: default",
            "Resolved next-turn effort: low",
            "Legacy prompt enabled: no",
            "Legacy prompt level: -",
        ] {
            assert!(
                default_text.contains(expected),
                "missing `{expected}` in {default_text}"
            );
        }

        apply("/think max", 111);
        apply("/model openai/gpt-5.5", 112);
        let suspended_text = build_channel_step(&registry, &build("/reasoning")).outbound_messages
            [0]
        .text
        .clone();
        for expected in [
            "Model: openai/gpt-5.5",
            "Backend policy: suspended",
            "Stored backend preference: max",
            "Resolved next-turn effort: -",
        ] {
            assert!(
                suspended_text.contains(expected),
                "missing `{expected}` in {suspended_text}"
            );
        }
        assert!(!suspended_text.contains("Resolved next-turn effort: max"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn unified_think_status_reports_excluded_dormant_and_masked_legacy_state() {
        let root = temp_root("unified_think_status_excluded_masked");
        let source = write_channel_source(&root);
        let harness_home = root.join(".agent-harness");
        write_codex_models_cache_for_test(&harness_home);
        write_model_catalog_mode_for_test(&harness_home, "authoritative");
        let registry = load_agent_registry(&source).unwrap();
        let skills = build_source_skill_index(&source).unwrap();
        let build = |text: &str| {
            build_turn_plan(
                &source,
                &registry,
                &skills,
                TurnPlanInput {
                    harness_home: Some(harness_home.clone()),
                    platform: "telegram".to_string(),
                    channel_id: "dm".to_string(),
                    user_id: "user".to_string(),
                    text: text.to_string(),
                    inbound_context: None,
                    inbound_media_artifacts: Vec::new(),
                    requested_agent_id: Some("main56sol".to_string()),
                    session_hint: None,
                    skill_limit: 3,
                },
            )
            .unwrap()
        };
        let apply = |text: &str, now_ms: i64| {
            crate::apply_channel_command_step(
                &build_channel_step(&registry, &build(text)),
                crate::ChannelCommandApplyOptions {
                    harness_home: harness_home.clone(),
                    now_ms,
                },
            )
            .unwrap();
        };

        apply("/think global max", 120);
        write_model_catalog_cohort_for_test(&harness_home, "authoritative", &["main56luna"]);
        let dormant_text = build_channel_step(&registry, &build("/think")).outbound_messages[0]
            .text
            .clone();
        for expected in [
            "Rollout: excluded (configured=authoritative)",
            "Backend policy: dormant",
            "Stored backend preference: max",
            "Preference source: session",
            "Resolved next-turn effort: -",
            "Catalog efforts (observed): low, medium, high, xhigh, max, ultra",
        ] {
            assert!(
                dormant_text.contains(expected),
                "missing `{expected}` in {dormant_text}"
            );
        }

        apply("/reasoning low", 121);
        let legacy_text = build_channel_step(&registry, &build("/reasoning")).outbound_messages[0]
            .text
            .clone();
        for expected in [
            "Rollout: excluded (configured=authoritative)",
            "Backend policy: legacy-only",
            "Stored backend preference: unset",
            "Preference source: none",
            "Masked agent default: max",
            "Resolved next-turn effort: -",
            "Legacy prompt enabled: yes",
            "Legacy prompt level: low",
        ] {
            assert!(
                legacy_text.contains(expected),
                "missing `{expected}` in {legacy_text}"
            );
        }
        assert!(!legacy_text.contains("Resolved next-turn effort: low"));

        write_model_catalog_mode_for_test(&harness_home, "shadow");
        let shadow_text = build_channel_step(&registry, &build("/think")).outbound_messages[0]
            .text
            .clone();
        assert!(shadow_text.contains("Rollout: shadow"));
        assert!(shadow_text.contains("Backend policy: legacy-only"));
        assert!(shadow_text.contains("Resolved next-turn effort: -"));

        write_model_catalog_mode_for_test(&harness_home, "off");
        let off_text = build_channel_step(&registry, &build("/reasoning")).outbound_messages[0]
            .text
            .clone();
        assert!(off_text.contains("Rollout: off"));
        assert!(off_text.contains("Backend policy: legacy-only"));
        assert!(off_text.contains("Resolved next-turn effort: -"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn model_catalog_authoritative_reasoning_preserves_max_and_guards_ultra() {
        let root =
            temp_root("model_catalog_authoritative_reasoning_preserves_max_and_guards_ultra");
        let source = write_channel_source(&root);
        let harness_home = root.join(".agent-harness");
        write_codex_models_cache_for_test(&harness_home);
        write_model_catalog_mode_for_test(&harness_home, "authoritative");
        let registry = load_agent_registry(&source).unwrap();
        let skills = build_source_skill_index(&source).unwrap();
        let expected = vec!["low", "medium", "high", "xhigh", "max", "ultra"]
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>();

        for effort in ["max", "ultra"] {
            let turn = build_turn_plan(
                &source,
                &registry,
                &skills,
                TurnPlanInput {
                    harness_home: Some(harness_home.clone()),
                    platform: "telegram".to_string(),
                    channel_id: "dm".to_string(),
                    user_id: "user".to_string(),
                    text: format!("/think {effort}"),
                    inbound_context: None,
                    inbound_media_artifacts: Vec::new(),
                    requested_agent_id: Some("main56sol".to_string()),
                    session_hint: None,
                    skill_limit: 3,
                },
            )
            .unwrap();
            let step = build_channel_step(&registry, &turn);
            let Some(ChannelCommandEffect::SwitchReasoning {
                preference:
                    ReasoningPreference::Explicit {
                        effort: ref stored_effort,
                    },
                accepted,
                ref resolved_policy,
                available_efforts,
                ..
            }) = step.command_effect
            else {
                panic!("expected SwitchReasoning for Sol {effort}");
            };
            assert_eq!(stored_effort, effort);
            assert_eq!(accepted, effort == "max");
            assert_eq!(
                resolved_policy
                    .as_ref()
                    .map(|policy| policy.effective_effort()),
                (effort == "max").then_some("max")
            );
            assert_eq!(available_efforts, expected);
        }

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn model_catalog_authoritative_reasoning_preserves_exact_sol_max() {
        let root = temp_root("model_catalog_authoritative_reasoning_preserves_exact_sol_max");
        let source = write_channel_source(&root);
        let harness_home = root.join(".agent-harness");
        write_codex_models_cache_for_test(&harness_home);
        write_model_catalog_mode_for_test(&harness_home, "authoritative");
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
                user_id: "user".to_string(),
                text: "/think max".to_string(),
                inbound_context: None,
                inbound_media_artifacts: Vec::new(),
                requested_agent_id: Some("main56sol".to_string()),
                session_hint: None,
                skill_limit: 3,
            },
        )
        .unwrap();
        let step = build_channel_step(&registry, &turn);
        let Some(ChannelCommandEffect::SwitchReasoning {
            preference: crate::backend_reasoning::ReasoningPreference::Explicit { ref effort },
            accepted,
            ref resolved_policy,
            ref available_efforts,
            catalog_default: Some(ref catalog_default),
            ..
        }) = step.command_effect
        else {
            panic!(
                "expected exact backend SwitchReasoning: {:?}",
                step.command_effect
            );
        };
        assert_eq!(effort, "max");
        assert!(accepted);
        assert_eq!(
            resolved_policy
                .as_ref()
                .map(|policy| policy.effective_effort()),
            Some("max")
        );
        assert_eq!(catalog_default, "low");
        assert!(available_efforts.iter().any(|value| value == "max"));
        assert!(available_efforts.iter().any(|value| value == "ultra"));
        assert!(step.outbound_messages[0].text.contains("Backend reasoning"));

        let applied = crate::apply_channel_command_step(
            &step,
            crate::ChannelCommandApplyOptions {
                harness_home: harness_home.clone(),
                now_ms: 42,
            },
        )
        .unwrap();
        let state = applied
            .state
            .expect("reasoning command records channel state");
        assert_eq!(
            state.reasoning_preference,
            Some(crate::backend_reasoning::ReasoningPreference::Explicit {
                effort: "max".to_string()
            })
        );
        assert_eq!(
            state
                .backend_reasoning_policy
                .as_ref()
                .map(|policy| policy.effective_effort()),
            Some("max")
        );

        let followup = build_turn_plan(
            &source,
            &registry,
            &skills,
            TurnPlanInput {
                harness_home: Some(harness_home.clone()),
                platform: "telegram".to_string(),
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                text: "continue".to_string(),
                inbound_context: None,
                inbound_media_artifacts: Vec::new(),
                requested_agent_id: Some("main56sol".to_string()),
                session_hint: None,
                skill_limit: 3,
            },
        )
        .unwrap();
        assert_eq!(
            followup.reasoning_preference,
            Some(crate::backend_reasoning::ReasoningPreference::Explicit {
                effort: "max".to_string()
            })
        );
        assert_eq!(
            followup
                .backend_reasoning_policy
                .as_ref()
                .map(|policy| policy.effective_effort()),
            Some("max")
        );
        assert_eq!(followup.thinking_policy.level.as_deref(), Some("max"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn reasoning_default_preserves_reset_semantics_and_catalog_default() {
        let root = temp_root("reasoning_default_preserves_reset_semantics");
        let source = write_channel_source(&root);
        let harness_home = root.join(".agent-harness");
        write_codex_models_cache_for_test(&harness_home);
        write_model_catalog_mode_for_test(&harness_home, "authoritative");
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
                user_id: "user".to_string(),
                text: "/think default".to_string(),
                inbound_context: None,
                inbound_media_artifacts: Vec::new(),
                requested_agent_id: Some("main56sol".to_string()),
                session_hint: None,
                skill_limit: 3,
            },
        )
        .unwrap();
        let step = build_channel_step(&registry, &turn);
        assert!(matches!(
            step.command_effect,
            Some(ChannelCommandEffect::SwitchReasoning {
                preference: ReasoningPreference::Default,
                accepted: true,
                resolved_policy: None,
                catalog_default: Some(ref default),
                ..
            }) if default == "low"
        ));

        let applied = crate::apply_channel_command_step(
            &step,
            crate::ChannelCommandApplyOptions {
                harness_home: harness_home.clone(),
                now_ms: 43,
            },
        )
        .unwrap();
        let state = applied.state.unwrap();
        assert_eq!(
            state.reasoning_preference,
            Some(ReasoningPreference::Default)
        );
        assert_eq!(state.backend_reasoning_policy, None);
        assert_eq!(state.thinking_level, None);

        let build_followup = || {
            build_turn_plan(
                &source,
                &registry,
                &skills,
                TurnPlanInput {
                    harness_home: Some(harness_home.clone()),
                    platform: "telegram".to_string(),
                    channel_id: "dm".to_string(),
                    user_id: "user".to_string(),
                    text: "continue".to_string(),
                    inbound_context: None,
                    inbound_media_artifacts: Vec::new(),
                    requested_agent_id: Some("main56sol".to_string()),
                    session_hint: None,
                    skill_limit: 3,
                },
            )
            .unwrap()
        };
        let initial_default = build_followup();
        assert_eq!(
            initial_default.reasoning_preference,
            Some(ReasoningPreference::Default)
        );
        assert_eq!(
            initial_default
                .backend_reasoning_policy
                .as_ref()
                .map(|policy| policy.effective_effort()),
            Some("low"),
            "Default must resolve to an explicit effort because Codex turn/start effort is sticky"
        );

        let cache_file = harness_home.join("codex-home").join("models_cache.json");
        let cache = fs::read_to_string(&cache_file).unwrap();
        fs::write(
            &cache_file,
            cache.replacen(
                "\"slug\": \"gpt-5.6-sol\",\n              \"default_reasoning_level\": \"low\"",
                "\"slug\": \"gpt-5.6-sol\",\n              \"default_reasoning_level\": \"high\"",
                1,
            ),
        )
        .unwrap();
        let revised_default = build_followup();
        assert_eq!(
            revised_default.reasoning_preference,
            Some(ReasoningPreference::Default)
        );
        assert_eq!(
            revised_default
                .backend_reasoning_policy
                .as_ref()
                .map(|policy| policy.effective_effort()),
            Some("high"),
            "Default must be re-resolved for each turn instead of pinning the command-time default"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn reasoning_global_survives_new_and_model_switch_revalidates_portable_preference() {
        let root = temp_root("reasoning_global_survives_new_and_model_switch");
        let source = write_channel_source(&root);
        let harness_home = root.join(".agent-harness");
        write_codex_models_cache_for_test(&harness_home);
        write_model_catalog_mode_for_test(&harness_home, "authoritative");
        let registry = load_agent_registry(&source).unwrap();
        let skills = build_source_skill_index(&source).unwrap();

        let build = |text: &str| {
            build_turn_plan(
                &source,
                &registry,
                &skills,
                TurnPlanInput {
                    harness_home: Some(harness_home.clone()),
                    platform: "telegram".to_string(),
                    channel_id: "dm".to_string(),
                    user_id: "user".to_string(),
                    text: text.to_string(),
                    inbound_context: None,
                    inbound_media_artifacts: Vec::new(),
                    requested_agent_id: Some("main56sol".to_string()),
                    session_hint: None,
                    skill_limit: 3,
                },
            )
            .unwrap()
        };

        let reasoning = build_channel_step(&registry, &build("/think global max"));
        crate::apply_channel_command_step(
            &reasoning,
            crate::ChannelCommandApplyOptions {
                harness_home: harness_home.clone(),
                now_ms: 50,
            },
        )
        .unwrap();
        let global = crate::read_agent_override(&harness_home, "main56sol")
            .unwrap()
            .unwrap();
        assert_eq!(
            global.reasoning_preference,
            Some(ReasoningPreference::Explicit {
                effort: "max".to_string()
            })
        );
        assert_eq!(
            global
                .backend_reasoning_policy
                .as_ref()
                .map(|policy| policy.effective_effort()),
            Some("max")
        );

        let new_step = build_channel_step(&registry, &build("/new continue"));
        let new_report = crate::apply_channel_command_step(
            &new_step,
            crate::ChannelCommandApplyOptions {
                harness_home: harness_home.clone(),
                now_ms: 51,
            },
        )
        .unwrap();
        let new_state = new_report.state.unwrap();
        assert_eq!(new_state.reasoning_preference, None);
        assert_eq!(new_state.backend_reasoning_policy, None);
        let inherited = build("continue");
        assert_eq!(
            inherited.reasoning_preference,
            Some(ReasoningPreference::Explicit {
                effort: "max".to_string()
            })
        );
        assert_eq!(
            inherited
                .backend_reasoning_policy
                .as_ref()
                .map(|policy| policy.effective_effort()),
            Some("max")
        );

        let model_step =
            build_channel_step(&registry, &build("/model openai/gpt-5.6-luna --global"));
        crate::apply_channel_command_step(
            &model_step,
            crate::ChannelCommandApplyOptions {
                harness_home: harness_home.clone(),
                now_ms: 52,
            },
        )
        .unwrap();
        let switched = build("continue after switch");
        assert_eq!(switched.model_policy.model.as_deref(), Some("gpt-5.6-luna"));
        assert_eq!(
            switched.reasoning_preference,
            Some(ReasoningPreference::Explicit {
                effort: "max".to_string()
            })
        );
        assert_eq!(
            switched
                .backend_reasoning_policy
                .as_ref()
                .map(|policy| policy.effective_effort()),
            Some("max"),
            "portable preferences must be re-resolved against the selected route"
        );

        let incompatible_model_step =
            build_channel_step(&registry, &build("/model openai/gpt-5.5 --global"));
        crate::apply_channel_command_step(
            &incompatible_model_step,
            crate::ChannelCommandApplyOptions {
                harness_home: harness_home.clone(),
                now_ms: 53,
            },
        )
        .unwrap();
        let incompatible = build("continue after incompatible switch");
        assert_eq!(incompatible.model_policy.model.as_deref(), Some("gpt-5.5"));
        assert_eq!(
            incompatible.reasoning_preference,
            Some(ReasoningPreference::Explicit {
                effort: "max".to_string()
            })
        );
        assert_eq!(incompatible.backend_reasoning_policy, None);
        assert!(incompatible.warnings.iter().any(|warning| {
            warning.contains("preference is stored but has no route-valid resolved policy")
        }));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn think_and_reasoning_alias_share_one_last_write_wins_state() {
        let root = temp_root("think_and_reasoning_alias_share_one_state");
        let source = write_channel_source(&root);
        let harness_home = root.join(".agent-harness");
        write_codex_models_cache_for_test(&harness_home);
        write_model_catalog_mode_for_test(&harness_home, "authoritative");
        let registry = load_agent_registry(&source).unwrap();
        let skills = build_source_skill_index(&source).unwrap();

        let build = |text: &str| {
            build_turn_plan(
                &source,
                &registry,
                &skills,
                TurnPlanInput {
                    harness_home: Some(harness_home.clone()),
                    platform: "telegram".to_string(),
                    channel_id: "dm".to_string(),
                    user_id: "user".to_string(),
                    text: text.to_string(),
                    inbound_context: None,
                    inbound_media_artifacts: Vec::new(),
                    requested_agent_id: Some("main56sol".to_string()),
                    session_hint: None,
                    skill_limit: 3,
                },
            )
            .unwrap()
        };
        let apply = |text: &str, now_ms: i64| {
            let step = build_channel_step(&registry, &build(text));
            crate::apply_channel_command_step(
                &step,
                crate::ChannelCommandApplyOptions {
                    harness_home: harness_home.clone(),
                    now_ms,
                },
            )
            .unwrap()
            .state
            .unwrap()
        };

        let high = apply("/think high", 60);
        assert_eq!(high.thinking_level.as_deref(), Some("high"));
        assert_eq!(
            high.backend_reasoning_policy
                .as_ref()
                .map(|policy| policy.effective_effort()),
            Some("high")
        );

        let low = apply("/reasoning low", 61);
        assert_eq!(low.thinking_level.as_deref(), Some("low"));
        assert_eq!(
            low.reasoning_preference,
            Some(ReasoningPreference::Explicit {
                effort: "low".to_string()
            })
        );
        assert_eq!(
            low.backend_reasoning_policy
                .as_ref()
                .map(|policy| policy.effective_effort()),
            Some("low")
        );

        let high_again = apply("/think high", 62);
        assert_eq!(high_again.thinking_level.as_deref(), Some("high"));
        assert_eq!(
            high_again
                .backend_reasoning_policy
                .as_ref()
                .map(|policy| policy.effective_effort()),
            Some("high")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn unified_think_rollout_transition_never_carries_or_resurrects_stale_backend_state() {
        let root = temp_root("unified_think_rollout_transition_clears_stale_backend");
        let source = write_channel_source(&root);
        let harness_home = root.join(".agent-harness");
        write_codex_models_cache_for_test(&harness_home);
        write_model_catalog_mode_for_test(&harness_home, "authoritative");
        let registry = load_agent_registry(&source).unwrap();
        let skills = build_source_skill_index(&source).unwrap();
        let build = |text: &str| {
            build_turn_plan(
                &source,
                &registry,
                &skills,
                TurnPlanInput {
                    harness_home: Some(harness_home.clone()),
                    platform: "telegram".to_string(),
                    channel_id: "dm".to_string(),
                    user_id: "user".to_string(),
                    text: text.to_string(),
                    inbound_context: None,
                    inbound_media_artifacts: Vec::new(),
                    requested_agent_id: Some("main56sol".to_string()),
                    session_hint: None,
                    skill_limit: 3,
                },
            )
            .unwrap()
        };
        let apply = |text: &str, now_ms: i64| {
            let step = build_channel_step(&registry, &build(text));
            crate::apply_channel_command_step(
                &step,
                crate::ChannelCommandApplyOptions {
                    harness_home: harness_home.clone(),
                    now_ms,
                },
            )
            .unwrap()
            .state
            .unwrap()
        };

        let high = apply("/think high", 70);
        assert_eq!(
            high.backend_reasoning_policy
                .as_ref()
                .map(|policy| policy.effective_effort()),
            Some("high")
        );

        write_model_catalog_mode_for_test(&harness_home, "off");
        let rolled_back = build("plain turn while rollout is off");
        assert_eq!(rolled_back.reasoning_preference, None);
        assert_eq!(rolled_back.backend_reasoning_policy, None);
        assert_eq!(rolled_back.thinking_policy.level.as_deref(), Some("high"));

        let low = apply("/reasoning low", 71);
        assert_eq!(low.thinking_level.as_deref(), Some("low"));
        assert_eq!(low.reasoning_preference, None);
        assert_eq!(low.backend_reasoning_policy, None);

        write_model_catalog_mode_for_test(&harness_home, "authoritative");
        let reenabled = build("plain turn after rollout returns");
        assert_eq!(reenabled.reasoning_preference, None);
        assert_eq!(reenabled.backend_reasoning_policy, None);
        assert_eq!(reenabled.thinking_policy.level.as_deref(), Some("low"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn think_default_records_pending_reset_when_route_has_no_catalog_default() {
        let root = temp_root("think_default_records_pending_reset_without_catalog_default");
        let source = write_channel_source(&root);
        let harness_home = root.join(".agent-harness");
        let codex_home = harness_home.join("codex-home");
        fs::create_dir_all(&codex_home).unwrap();
        fs::write(
            codex_home.join("models_cache.json"),
            r#"{"models":[{"slug":"gpt-5.6-sol","supported_reasoning_levels":["low","high","xhigh","max"]}]}"#,
        )
        .unwrap();
        write_model_catalog_mode_for_test(&harness_home, "authoritative");
        let registry = load_agent_registry(&source).unwrap();
        let skills = build_source_skill_index(&source).unwrap();
        let build = |text: &str| {
            build_turn_plan(
                &source,
                &registry,
                &skills,
                TurnPlanInput {
                    harness_home: Some(harness_home.clone()),
                    platform: "telegram".to_string(),
                    channel_id: "dm".to_string(),
                    user_id: "user".to_string(),
                    text: text.to_string(),
                    inbound_context: None,
                    inbound_media_artifacts: Vec::new(),
                    requested_agent_id: Some("main56sol".to_string()),
                    session_hint: None,
                    skill_limit: 3,
                },
            )
            .unwrap()
        };

        let high_step = build_channel_step(&registry, &build("/think high"));
        crate::apply_channel_command_step(
            &high_step,
            crate::ChannelCommandApplyOptions {
                harness_home: harness_home.clone(),
                now_ms: 80,
            },
        )
        .unwrap();
        let default_step = build_channel_step(&registry, &build("/think default"));
        assert!(matches!(
            default_step.command_effect,
            Some(ChannelCommandEffect::SwitchReasoning {
                preference: ReasoningPreference::Default,
                accepted: true,
                resolved_policy: None,
                catalog_default: None,
                ..
            })
        ));
        let reset = crate::apply_channel_command_step(
            &default_step,
            crate::ChannelCommandApplyOptions {
                harness_home: harness_home.clone(),
                now_ms: 81,
            },
        )
        .unwrap()
        .state
        .unwrap();
        assert_eq!(
            reset.reasoning_preference,
            Some(ReasoningPreference::Default)
        );
        assert_eq!(reset.backend_reasoning_policy, None);
        assert_eq!(reset.thinking_level, None);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn rejected_or_forged_reasoning_effects_preserve_existing_state() {
        let root = temp_root("rejected_or_forged_reasoning_effects_preserve_state");
        let source = write_channel_source(&root);
        let harness_home = root.join(".agent-harness");
        write_codex_models_cache_for_test(&harness_home);
        write_model_catalog_mode_for_test(&harness_home, "authoritative");
        let registry = load_agent_registry(&source).unwrap();
        let skills = build_source_skill_index(&source).unwrap();
        let build_step = |text: &str| {
            let turn = build_turn_plan(
                &source,
                &registry,
                &skills,
                TurnPlanInput {
                    harness_home: Some(harness_home.clone()),
                    platform: "telegram".to_string(),
                    channel_id: "dm".to_string(),
                    user_id: "user".to_string(),
                    text: text.to_string(),
                    inbound_context: None,
                    inbound_media_artifacts: Vec::new(),
                    requested_agent_id: Some("main56sol".to_string()),
                    session_hint: None,
                    skill_limit: 3,
                },
            )
            .unwrap();
            build_channel_step(&registry, &turn)
        };
        let apply_step = |step: &ChannelStep, now_ms: i64| {
            crate::apply_channel_command_step(
                step,
                crate::ChannelCommandApplyOptions {
                    harness_home: harness_home.clone(),
                    now_ms,
                },
            )
            .unwrap()
        };

        let baseline = apply_step(&build_step("/think high"), 90).state.unwrap();
        let baseline_policy = baseline.backend_reasoning_policy.clone().unwrap();
        let assert_baseline = |state: &crate::ChannelSessionState| {
            assert_eq!(state.provider.as_deref(), Some("openai"));
            assert_eq!(state.model.as_deref(), Some("gpt-5.6-sol"));
            assert_eq!(state.thinking_level.as_deref(), Some("high"));
            assert_eq!(
                state.reasoning_preference,
                Some(ReasoningPreference::Explicit {
                    effort: "high".to_string()
                })
            );
            assert_eq!(
                state
                    .backend_reasoning_policy
                    .as_ref()
                    .map(|policy| policy.effective_effort()),
                Some("high")
            );
        };

        let mut rejected = build_step("/think bogus");
        if let Some(ChannelCommandEffect::SwitchReasoning {
            provider, model, ..
        }) = rejected.command_effect.as_mut()
        {
            *provider = Some("openai".to_string());
            *model = Some("gpt-5.6-luna".to_string());
        }
        let rejected_report = apply_step(&rejected, 91);
        assert_baseline(rejected_report.state.as_ref().unwrap());

        let mut missing_policy = build_step("/think low");
        if let Some(ChannelCommandEffect::SwitchReasoning {
            accepted,
            resolved_policy,
            ..
        }) = missing_policy.command_effect.as_mut()
        {
            *accepted = true;
            *resolved_policy = None;
        }
        let missing_report = apply_step(&missing_policy, 92);
        assert_baseline(missing_report.state.as_ref().unwrap());
        assert!(
            missing_report
                .warnings
                .iter()
                .any(|warning| warning.contains("rejected inconsistent backend reasoning effect"))
        );

        let mut mismatched_effort = build_step("/think low");
        if let Some(ChannelCommandEffect::SwitchReasoning {
            resolved_policy,
            resolution,
            ..
        }) = mismatched_effort.command_effect.as_mut()
        {
            *resolved_policy = Some(baseline_policy.clone());
            *resolution = Some(baseline_policy.resolution().clone());
        }
        let mismatch_report = apply_step(&mismatched_effort, 93);
        assert_baseline(mismatch_report.state.as_ref().unwrap());

        let mut mismatched_route = build_step("/think low");
        if let Some(ChannelCommandEffect::SwitchReasoning { model, .. }) =
            mismatched_route.command_effect.as_mut()
        {
            *model = Some("gpt-5.6-luna".to_string());
        }
        let route_report = apply_step(&mismatched_route, 94);
        assert_baseline(route_report.state.as_ref().unwrap());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn agents_outside_rollout_keep_legacy_think_semantics() {
        let root = temp_root("legacy_think_semantics_do_not_become_backend_reasoning");
        let source = write_channel_source(&root);
        let harness_home = root.join(".agent-harness");
        write_codex_models_cache_for_test(&harness_home);
        write_model_catalog_cohort_for_test(&harness_home, "authoritative", &["other-agent"]);
        let registry = load_agent_registry(&source).unwrap();
        let skills = build_source_skill_index(&source).unwrap();

        for (input, expected_level, expected_valid) in [
            ("/think max", "xhigh", true),
            ("/think ultra-high", "xhigh", true),
            ("/think ultra", "ultra", false),
        ] {
            let turn = build_turn_plan(
                &source,
                &registry,
                &skills,
                TurnPlanInput {
                    harness_home: Some(harness_home.clone()),
                    platform: "telegram".to_string(),
                    channel_id: "dm".to_string(),
                    user_id: "user".to_string(),
                    text: input.to_string(),
                    inbound_context: None,
                    inbound_media_artifacts: Vec::new(),
                    requested_agent_id: Some("main56sol".to_string()),
                    session_hint: None,
                    skill_limit: 3,
                },
            )
            .unwrap();
            let step = build_channel_step(&registry, &turn);
            assert!(
                matches!(
                    step.command_effect,
                    Some(ChannelCommandEffect::SwitchThinking {
                        ref level,
                        valid,
                        ref available_levels,
                        ..
                    }) if level == expected_level
                        && valid == expected_valid
                        && !available_levels.iter().any(|value| value == "max" || value == "ultra")
                ),
                "input={input}, effect={:?}",
                step.command_effect
            );
        }

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn model_catalog_agent_cohort_controls_resolution_and_advertised_levels() {
        let root = temp_root("model_catalog_agent_cohort_controls_resolution");
        let source = write_channel_source(&root);
        let harness_home = root.join(".agent-harness");
        write_codex_models_cache_for_test(&harness_home);
        let registry = load_agent_registry(&source).unwrap();
        let skills = build_source_skill_index(&source).unwrap();

        let build_max_step = || {
            let turn = build_turn_plan(
                &source,
                &registry,
                &skills,
                TurnPlanInput {
                    harness_home: Some(harness_home.clone()),
                    platform: "telegram".to_string(),
                    channel_id: "dm".to_string(),
                    user_id: "user".to_string(),
                    text: "/think max".to_string(),
                    inbound_context: None,
                    inbound_media_artifacts: Vec::new(),
                    requested_agent_id: Some("main56sol".to_string()),
                    session_hint: None,
                    skill_limit: 3,
                },
            )
            .unwrap();
            build_channel_step(&registry, &turn)
        };

        write_model_catalog_cohort_for_test(&harness_home, "authoritative", &["main56sol"]);
        let included = build_max_step();
        let Some(ChannelCommandEffect::SwitchReasoning {
            accepted,
            available_efforts,
            ..
        }) = included.command_effect
        else {
            panic!("expected included Sol cohort to produce SwitchReasoning");
        };
        assert!(accepted, "included Sol cohort should accept max");
        assert!(available_efforts.iter().any(|level| level == "max"));
        assert!(available_efforts.iter().any(|level| level == "ultra"));

        write_model_catalog_cohort_for_test(&harness_home, "authoritative", &["main56luna"]);
        let excluded = build_max_step();
        let Some(ChannelCommandEffect::SwitchThinking {
            level,
            valid,
            available_levels,
            ..
        }) = excluded.command_effect
        else {
            panic!("expected excluded Sol cohort to retain legacy SwitchThinking");
        };
        assert!(valid);
        assert_eq!(level, "xhigh");
        assert!(!available_levels.iter().any(|level| level == "max"));
        assert!(!available_levels.iter().any(|level| level == "ultra"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn model_catalog_authoritative_channel_rejects_luna_ultra_without_state_coercion() {
        let root =
            temp_root("model_catalog_authoritative_channel_rejects_luna_ultra_without_coercion");
        let source = write_channel_source(&root);
        let harness_home = root.join(".agent-harness");
        write_codex_models_cache_for_test(&harness_home);
        write_model_catalog_mode_for_test(&harness_home, "authoritative");
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
                user_id: "user".to_string(),
                text: "/think ultra".to_string(),
                inbound_context: None,
                inbound_media_artifacts: Vec::new(),
                requested_agent_id: Some("main56luna".to_string()),
                session_hint: None,
                skill_limit: 3,
            },
        )
        .unwrap();
        let step = build_channel_step(&registry, &turn);
        let Some(ChannelCommandEffect::SwitchReasoning {
            preference: ReasoningPreference::Explicit { ref effort },
            accepted,
            ref available_efforts,
            ..
        }) = step.command_effect
        else {
            panic!("expected SwitchReasoning for Luna ultra");
        };
        assert_eq!(effort, "ultra");
        assert!(!accepted);
        assert_eq!(
            available_efforts,
            &["low", "medium", "high", "xhigh", "max"]
                .into_iter()
                .map(str::to_string)
                .collect::<Vec<_>>()
        );

        let report = crate::apply_channel_command_step(
            &step,
            crate::ChannelCommandApplyOptions {
                harness_home: harness_home.clone(),
                now_ms: 42,
            },
        )
        .unwrap();
        let state = report
            .state
            .expect("invalid command still records channel state");
        assert_eq!(state.reasoning_preference, None);
        assert_eq!(state.backend_reasoning_policy, None);
        assert_eq!(state.thinking_level, None);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn model_catalog_shadow_channel_keeps_legacy_authority_and_observes_catalog() {
        let root =
            temp_root("model_catalog_shadow_channel_keeps_legacy_authority_and_observes_catalog");
        let source = write_channel_source(&root);
        let harness_home = root.join(".agent-harness");
        write_codex_models_cache_for_test(&harness_home);
        write_model_catalog_mode_for_test(&harness_home, "shadow");
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
                text: "/think max".to_string(),
                inbound_context: None,
                inbound_media_artifacts: Vec::new(),
                requested_agent_id: Some("main56sol".to_string()),
                session_hint: None,
                skill_limit: 3,
            },
        )
        .unwrap();

        let receipt = reasoning_resolution_for_turn(&turn, "max");
        assert_eq!(
            receipt.status,
            crate::model_catalog::ReasoningResolutionStatus::Shadow
        );
        assert_eq!(receipt.effective_effort.as_deref(), Some("xhigh"));
        assert_eq!(receipt.catalog_effective_effort.as_deref(), Some("max"));
        assert!(!receipt.authoritative);

        let step = build_channel_step(&registry, &turn);
        assert!(matches!(
            step.command_effect,
            Some(ChannelCommandEffect::SwitchThinking {
                ref level,
                valid,
                ..
            }) if level == "xhigh" && valid
        ));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn channel_step_reports_and_switches_fast_mode_with_route_capability() {
        let root = temp_root("channel_step_reports_and_switches_fast_mode");
        let source = write_channel_source(&root);
        let harness_home = root.join(".agent-harness");
        write_codex_models_cache_for_test(&harness_home);
        let registry = load_agent_registry(&source).unwrap();
        let skills = build_source_skill_index(&source).unwrap();
        let show_turn = build_turn_plan(
            &source,
            &registry,
            &skills,
            TurnPlanInput {
                harness_home: Some(harness_home.clone()),
                platform: "telegram".to_string(),
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                text: "/fast".to_string(),
                inbound_context: None,
                inbound_media_artifacts: Vec::new(),
                requested_agent_id: Some("main55".to_string()),
                session_hint: None,
                skill_limit: 3,
            },
        )
        .unwrap();
        let show_step = build_channel_step(&registry, &show_turn);
        assert_eq!(show_step.action, ChannelStepAction::ReplyOnly);
        assert!(
            show_step.outbound_messages[0]
                .text
                .contains("Fast mode: normal")
        );
        assert!(
            show_step.outbound_messages[0]
                .text
                .contains("Request acceleration: disabled")
        );
        assert!(matches!(
            show_step.command_effect,
            Some(ChannelCommandEffect::ShowFast {
                ref current_mode,
                ref effective_acceleration,
                ..
            }) if current_mode == "normal" && effective_acceleration == "disabled"
        ));

        let switch_turn = build_turn_plan(
            &source,
            &registry,
            &skills,
            TurnPlanInput {
                harness_home: Some(harness_home.clone()),
                platform: "telegram".to_string(),
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                text: "/fast on".to_string(),
                inbound_context: None,
                inbound_media_artifacts: Vec::new(),
                requested_agent_id: Some("main55".to_string()),
                session_hint: None,
                skill_limit: 3,
            },
        )
        .unwrap();
        let switch_step = build_channel_step(&registry, &switch_turn);
        assert_eq!(switch_step.action, ChannelStepAction::ReplyOnly);
        assert!(
            switch_step.outbound_messages[0]
                .text
                .contains("Fast mode: fast")
        );
        assert!(
            switch_step.outbound_messages[0]
                .text
                .contains("Request acceleration: enabled")
        );
        assert!(matches!(
            switch_step.command_effect,
            Some(ChannelCommandEffect::SwitchFast {
                ref previous_mode,
                ref mode,
                ref effective_acceleration,
                global,
                ..
            }) if previous_mode == "normal"
                && mode == "fast"
                && effective_acceleration == "enabled"
                && !global
        ));

        let global_turn = build_turn_plan(
            &source,
            &registry,
            &skills,
            TurnPlanInput {
                harness_home: Some(harness_home),
                platform: "telegram".to_string(),
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                text: "/fast on --global".to_string(),
                inbound_context: None,
                inbound_media_artifacts: Vec::new(),
                requested_agent_id: Some("main55".to_string()),
                session_hint: None,
                skill_limit: 3,
            },
        )
        .unwrap();
        let global_step = build_channel_step(&registry, &global_turn);
        assert!(
            global_step.outbound_messages[0]
                .text
                .contains("Scope: current session and agent `main55` default")
        );
        assert!(matches!(
            global_step.command_effect,
            Some(ChannelCommandEffect::SwitchFast {
                ref mode,
                global,
                ..
            }) if mode == "fast" && global
        ));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn fast_request_policy_is_codex_model_catalog_gated() {
        let root = temp_root("fast_request_policy_is_codex_model_catalog_gated");
        let harness_home = root.join(".agent-harness");
        write_codex_models_cache_for_test(&harness_home);

        let supported = fast_request_policy_for_route(
            &Some("openai".to_string()),
            &Some("gpt-5.5".to_string()),
            "fast",
            Some(&harness_home),
        );
        assert_eq!(supported.effective_acceleration, "enabled");
        assert_eq!(supported.service_tier.as_deref(), Some("priority"));

        let normal = fast_request_policy_for_route(
            &Some("openai".to_string()),
            &Some("gpt-5.5".to_string()),
            "normal",
            Some(&harness_home),
        );
        assert_eq!(normal.effective_acceleration, "disabled");
        assert_eq!(normal.service_tier.as_deref(), Some("default"));

        let unsupported_model = fast_request_policy_for_route(
            &Some("openai".to_string()),
            &Some("gpt-5.4-mini".to_string()),
            "fast",
            Some(&harness_home),
        );
        assert_eq!(unsupported_model.effective_acceleration, "unsupported");
        assert_eq!(unsupported_model.service_tier, None);

        let unsupported_provider = fast_request_policy_for_route(
            &Some("openrouter".to_string()),
            &Some("gpt-5.5".to_string()),
            "fast",
            Some(&harness_home),
        );
        assert_eq!(unsupported_provider.effective_acceleration, "unsupported");
        assert_eq!(unsupported_provider.service_tier, None);

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
                    "openai/gpt-5.6-sol": {},
                    "openai/gpt-5.6-luna": {},
                    "openai/gpt-5.5": {},
                    "openrouter/anthropic/claude-sonnet-4": {}
                  }
                },
                "list": [
                  { "id": "main", "model": "gpt-5", "enabled": true },
                  { "id": "main55", "model": "gpt-5.5", "enabled": true },
                  { "id": "main56sol", "model": "gpt-5.6-sol", "enabled": true },
                  { "id": "main56luna", "model": "gpt-5.6-luna", "enabled": true }
                ]
              },
              "models": {
                "providers": {
                  "openai": {
                    "apiKey": "${OPENAI_API_KEY}",
                    "models": [
                      { "id": "gpt-5" },
                      { "id": "gpt-5.5" },
                      { "id": "gpt-5.6-sol" },
                      { "id": "gpt-5.6-luna" }
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
