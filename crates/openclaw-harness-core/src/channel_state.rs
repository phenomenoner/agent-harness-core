use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::{ChannelCommandEffect, ChannelOutboundMessage, ChannelStep};

const CHANNEL_COMMAND_APPLY_SCHEMA: &str = "openclaw-harness.channel-command-apply.v1";
const CHANNEL_STATE_SCHEMA: &str = "openclaw-harness.channel-session-state.v1";
const CHANNEL_COMMAND_EVENT_SCHEMA: &str = "openclaw-harness.channel-command-event.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelCommandApplyOptions {
    pub harness_home: PathBuf,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelCommandApplyReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub state_file: PathBuf,
    pub events_file: PathBuf,
    pub receipts_file: PathBuf,
    pub event: Option<ChannelCommandEvent>,
    pub state: Option<ChannelSessionState>,
    pub outbound_messages: Vec<ChannelOutboundMessage>,
    pub receipt: ChannelCommandApplyReceipt,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelCommandApplyReceipt {
    pub status: ChannelCommandApplyReceiptStatus,
    pub session_key: String,
    pub state_file: PathBuf,
    pub events_file: PathBuf,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ChannelCommandApplyReceiptStatus {
    Applied,
    SkippedNotCommand,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelCommandEvent {
    pub schema: &'static str,
    pub at_ms: i64,
    pub platform: String,
    pub channel_id: String,
    pub user_id: String,
    pub session_key: String,
    pub active_session_key: String,
    pub command: &'static str,
    pub effect: ChannelCommandEffect,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelSessionState {
    pub schema: String,
    pub platform: String,
    pub channel_id: String,
    pub user_id: String,
    pub active_session_key: String,
    pub agent_id: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub session_topic: Option<String>,
    pub model_override: Option<String>,
    pub model_override_provider: Option<String>,
    pub model_override_model: Option<String>,
    pub thinking_enabled: bool,
    pub thinking_instruction: Option<String>,
    pub stop_requested: bool,
    pub stop_reason: Option<String>,
    pub steering_notes: Vec<ChannelSessionNote>,
    pub btw_notes: Vec<ChannelSessionNote>,
    pub last_command: Option<String>,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelSessionNote {
    pub at_ms: i64,
    pub text: String,
}

pub fn apply_channel_command_step(
    step: &ChannelStep,
    options: ChannelCommandApplyOptions,
) -> io::Result<ChannelCommandApplyReport> {
    let state_dir = channel_session_state_dir(
        &options.harness_home,
        &step.platform,
        &step.channel_id,
        &step.user_id,
    );
    fs::create_dir_all(&state_dir)?;
    let state_file = state_dir.join("state.json");
    let events_file = state_dir.join("events.jsonl");
    let receipts_file = options
        .harness_home
        .join("state")
        .join("channels")
        .join("command-apply-receipts.jsonl");
    let mut warnings = step.warnings.clone();

    let Some(effect) = step.command_effect.clone() else {
        let receipt = ChannelCommandApplyReceipt {
            status: ChannelCommandApplyReceiptStatus::SkippedNotCommand,
            session_key: step.session_key.clone(),
            state_file: state_file.clone(),
            events_file: events_file.clone(),
            reason: "channel step has no command effect".to_string(),
        };
        append_json_line(&receipts_file, &receipt)?;
        return Ok(ChannelCommandApplyReport {
            schema: CHANNEL_COMMAND_APPLY_SCHEMA,
            harness_home: options.harness_home,
            state_file,
            events_file,
            receipts_file,
            event: None,
            state: None,
            outbound_messages: step.outbound_messages.clone(),
            receipt,
            warnings,
        });
    };

    let mut state = read_channel_state(&state_file)?.unwrap_or_else(|| new_state(step));
    apply_effect(&mut state, &effect, options.now_ms, &mut warnings);
    let event = ChannelCommandEvent {
        schema: CHANNEL_COMMAND_EVENT_SCHEMA,
        at_ms: options.now_ms,
        platform: step.platform.clone(),
        channel_id: step.channel_id.clone(),
        user_id: step.user_id.clone(),
        session_key: step.session_key.clone(),
        active_session_key: state.active_session_key.clone(),
        command: command_name(&effect),
        effect,
    };

    fs::write(
        &state_file,
        serde_json::to_string_pretty(&state).map_err(io::Error::other)?,
    )?;
    append_json_line(&events_file, &event)?;

    let receipt = ChannelCommandApplyReceipt {
        status: ChannelCommandApplyReceiptStatus::Applied,
        session_key: state.active_session_key.clone(),
        state_file: state_file.clone(),
        events_file: events_file.clone(),
        reason: "channel command state updated".to_string(),
    };
    append_json_line(&receipts_file, &receipt)?;

    Ok(ChannelCommandApplyReport {
        schema: CHANNEL_COMMAND_APPLY_SCHEMA,
        harness_home: options.harness_home,
        state_file,
        events_file,
        receipts_file,
        event: Some(event),
        state: Some(state),
        outbound_messages: step.outbound_messages.clone(),
        receipt,
        warnings,
    })
}

pub fn read_channel_session_state(
    harness_home: impl AsRef<Path>,
    platform: &str,
    channel_id: &str,
    user_id: &str,
) -> io::Result<Option<ChannelSessionState>> {
    read_channel_state(&channel_session_state_file(
        harness_home.as_ref(),
        platform,
        channel_id,
        user_id,
    ))
}

pub fn channel_session_state_file(
    harness_home: &Path,
    platform: &str,
    channel_id: &str,
    user_id: &str,
) -> PathBuf {
    channel_session_state_dir(harness_home, platform, channel_id, user_id).join("state.json")
}

fn read_channel_state(path: &Path) -> io::Result<Option<ChannelSessionState>> {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(io::Error::other),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn new_state(step: &ChannelStep) -> ChannelSessionState {
    ChannelSessionState {
        schema: CHANNEL_STATE_SCHEMA.to_string(),
        platform: step.platform.clone(),
        channel_id: step.channel_id.clone(),
        user_id: step.user_id.clone(),
        active_session_key: step.session_key.clone(),
        agent_id: None,
        provider: None,
        model: None,
        session_topic: None,
        model_override: None,
        model_override_provider: None,
        model_override_model: None,
        thinking_enabled: false,
        thinking_instruction: None,
        stop_requested: false,
        stop_reason: None,
        steering_notes: Vec::new(),
        btw_notes: Vec::new(),
        last_command: None,
        updated_at_ms: 0,
    }
}

fn apply_effect(
    state: &mut ChannelSessionState,
    effect: &ChannelCommandEffect,
    now_ms: i64,
    warnings: &mut Vec<String>,
) {
    state.last_command = Some(command_name(effect).to_string());
    state.updated_at_ms = now_ms;

    match effect {
        ChannelCommandEffect::StartNewSession {
            topic,
            new_session_key,
        } => {
            state.active_session_key = new_session_key.clone();
            state.session_topic = topic.clone();
            state.thinking_enabled = false;
            state.thinking_instruction = None;
            state.stop_requested = false;
            state.stop_reason = None;
            state.steering_notes.clear();
            state.btw_notes.clear();
        }
        ChannelCommandEffect::SetThinkingMode { instruction } => {
            state.thinking_enabled = true;
            state.thinking_instruction = instruction.clone();
        }
        ChannelCommandEffect::StopCurrentRun { reason } => {
            state.stop_requested = true;
            state.stop_reason = reason.clone();
        }
        ChannelCommandEffect::AddSteering { instruction } => {
            state.steering_notes.push(ChannelSessionNote {
                at_ms: now_ms,
                text: instruction.clone(),
            });
        }
        ChannelCommandEffect::AddBtwNote { note } => {
            state.btw_notes.push(ChannelSessionNote {
                at_ms: now_ms,
                text: note.clone(),
            });
        }
        ChannelCommandEffect::ShowModel {
            agent_id,
            provider,
            model,
        } => {
            update_model_context(state, agent_id, provider, model);
        }
        ChannelCommandEffect::SwitchModel {
            agent_id,
            target,
            current_provider,
            current_model,
        } => {
            update_model_context(state, agent_id, current_provider, current_model);
            state.model_override = Some(target.clone());
            let (provider, model) = split_model_target(target);
            state.model_override_provider = provider;
            state.model_override_model = model;
        }
        ChannelCommandEffect::ShowStatus { snapshot, .. } => {
            update_model_context(
                state,
                &snapshot.current_agent_id,
                &snapshot.current_provider,
                &snapshot.current_model,
            );
        }
    }

    if state.model_override.is_some() && state.model_override_model.is_none() {
        warnings.push("model override target did not include a usable model name".to_string());
    }
}

fn update_model_context(
    state: &mut ChannelSessionState,
    agent_id: &Option<String>,
    provider: &Option<String>,
    model: &Option<String>,
) {
    if agent_id.is_some() {
        state.agent_id = agent_id.clone();
    }
    if provider.is_some() {
        state.provider = provider.clone();
    }
    if model.is_some() {
        state.model = model.clone();
    }
}

fn command_name(effect: &ChannelCommandEffect) -> &'static str {
    match effect {
        ChannelCommandEffect::StartNewSession { .. } => "new",
        ChannelCommandEffect::SetThinkingMode { .. } => "think",
        ChannelCommandEffect::StopCurrentRun { .. } => "stop",
        ChannelCommandEffect::AddSteering { .. } => "steer",
        ChannelCommandEffect::AddBtwNote { .. } => "btw",
        ChannelCommandEffect::ShowModel { .. } => "model",
        ChannelCommandEffect::SwitchModel { .. } => "model",
        ChannelCommandEffect::ShowStatus { .. } => "status",
    }
}

fn split_model_target(target: &str) -> (Option<String>, Option<String>) {
    let trimmed = target.trim();
    if trimmed.is_empty() {
        return (None, None);
    }
    match trimmed.split_once('/') {
        Some((provider, model)) if !provider.trim().is_empty() && !model.trim().is_empty() => (
            Some(provider.trim().to_string()),
            Some(model.trim().to_string()),
        ),
        _ => (None, Some(trimmed.to_string())),
    }
}

fn channel_session_state_dir(
    harness_home: &Path,
    platform: &str,
    channel_id: &str,
    user_id: &str,
) -> PathBuf {
    harness_home
        .join("state")
        .join("channels")
        .join(normalize_key_part(platform))
        .join(normalize_key_part(channel_id))
        .join(normalize_key_part(user_id))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ChannelOutboundMessageKind, ChannelStatusSnapshot, ChannelStepAction};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn applies_new_session_and_accumulates_notes() {
        let root = temp_root("applies_new_session_and_accumulates_notes");
        let harness_home = root.join(".openclaw-harness");
        let new_step = command_step(ChannelCommandEffect::StartNewSession {
            topic: Some("weekly review".to_string()),
            new_session_key: "telegram:dm:user:main:new".to_string(),
        });

        let report = apply_channel_command_step(
            &new_step,
            ChannelCommandApplyOptions {
                harness_home: harness_home.clone(),
                now_ms: 1000,
            },
        )
        .unwrap();

        assert_eq!(
            report.receipt.status,
            ChannelCommandApplyReceiptStatus::Applied
        );
        let state = report.state.unwrap();
        assert_eq!(state.active_session_key, "telegram:dm:user:main:new");
        assert_eq!(state.session_topic.as_deref(), Some("weekly review"));
        assert!(state.steering_notes.is_empty());

        let steer_step = command_step_with_session(
            "telegram:dm:user:main:new",
            ChannelCommandEffect::AddSteering {
                instruction: "keep replies short".to_string(),
            },
        );
        let report = apply_channel_command_step(
            &steer_step,
            ChannelCommandApplyOptions {
                harness_home,
                now_ms: 1001,
            },
        )
        .unwrap();

        let state = report.state.unwrap();
        assert_eq!(state.active_session_key, "telegram:dm:user:main:new");
        assert_eq!(state.steering_notes.len(), 1);
        assert_eq!(state.steering_notes[0].text, "keep replies short");
        assert_eq!(
            fs::read_to_string(report.events_file)
                .unwrap()
                .lines()
                .count(),
            2
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn applies_model_switch_override() {
        let root = temp_root("applies_model_switch_override");
        let harness_home = root.join(".openclaw-harness");
        let step = command_step(ChannelCommandEffect::SwitchModel {
            agent_id: Some("main".to_string()),
            target: "openrouter/anthropic/claude-sonnet-4".to_string(),
            current_provider: Some("openai".to_string()),
            current_model: Some("gpt-5".to_string()),
        });

        let report = apply_channel_command_step(
            &step,
            ChannelCommandApplyOptions {
                harness_home,
                now_ms: 2000,
            },
        )
        .unwrap();

        let state = report.state.unwrap();
        assert_eq!(state.agent_id.as_deref(), Some("main"));
        assert_eq!(state.provider.as_deref(), Some("openai"));
        assert_eq!(state.model.as_deref(), Some("gpt-5"));
        assert_eq!(
            state.model_override.as_deref(),
            Some("openrouter/anthropic/claude-sonnet-4")
        );
        assert_eq!(state.model_override_provider.as_deref(), Some("openrouter"));
        assert_eq!(
            state.model_override_model.as_deref(),
            Some("anthropic/claude-sonnet-4")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn skips_non_command_steps() {
        let root = temp_root("skips_non_command_steps");
        let harness_home = root.join(".openclaw-harness");
        let mut step = command_step(ChannelCommandEffect::ShowStatus {
            scope: None,
            snapshot: ChannelStatusSnapshot {
                scope: None,
                platform: "telegram".to_string(),
                session_key: "telegram:dm:user:main".to_string(),
                agents_total: 1,
                agents_enabled: 1,
                providers_total: 1,
                plugins_total: 0,
                telegram_configured: true,
                discord_configured: false,
                current_agent_id: Some("main".to_string()),
                agent_directory_exists: true,
                agent_sessions_index_exists: true,
                current_provider: Some("openai".to_string()),
                current_model: Some("gpt-5".to_string()),
                model_override: None,
                codex_approval_policy: None,
                codex_sandbox: None,
                prompt_files_present: 1,
                prompt_files_total: 1,
                prompt_file_names: vec!["AGENTS.md".to_string()],
                selected_skills: 0,
                selected_skill_ids: Vec::new(),
                channel_state_loaded: false,
                active_session_key: None,
                thinking_enabled: false,
                steering_notes: 0,
                btw_notes: 0,
            },
        });
        step.command_effect = None;
        step.action = ChannelStepAction::EnqueueAgentTurn;

        let report = apply_channel_command_step(
            &step,
            ChannelCommandApplyOptions {
                harness_home,
                now_ms: 3000,
            },
        )
        .unwrap();

        assert_eq!(
            report.receipt.status,
            ChannelCommandApplyReceiptStatus::SkippedNotCommand
        );
        assert!(report.state.is_none());
        assert!(!report.state_file.is_file());
        assert!(report.receipts_file.is_file());

        let _ = fs::remove_dir_all(root);
    }

    fn command_step(effect: ChannelCommandEffect) -> ChannelStep {
        command_step_with_session("telegram:dm:user:main", effect)
    }

    fn command_step_with_session(session_key: &str, effect: ChannelCommandEffect) -> ChannelStep {
        ChannelStep {
            schema: "test",
            source_home: PathBuf::from(".openclaw"),
            source_workspace: PathBuf::from(".openclaw/workspace"),
            platform: "telegram".to_string(),
            channel_id: "dm".to_string(),
            user_id: "user".to_string(),
            message_text: "/test".to_string(),
            session_key: session_key.to_string(),
            action: ChannelStepAction::ReplyOnly,
            command_effect: Some(effect),
            agent_turn: None,
            outbound_messages: vec![ChannelOutboundMessage {
                platform: "telegram".to_string(),
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                session_key: session_key.to_string(),
                kind: ChannelOutboundMessageKind::CommandReply,
                text: "ok".to_string(),
            }],
            warnings: Vec::new(),
        }
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "openclaw-harness-channel-state-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
