use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::{
    ChannelCommandEffect, ChannelOutboundMessage, ChannelStep,
    VirtualSessionTaskBoundaryCloseOptions, close_virtual_session_for_task_boundary,
    codex_runtime::{CodexTurnSteerRequestOptions, queue_codex_turn_steer_request},
    write_json_atomic,
};

const CHANNEL_COMMAND_APPLY_SCHEMA: &str = "agent-harness.channel-command-apply.v1";
const CHANNEL_STATE_SCHEMA: &str = "agent-harness.channel-session-state.v1";
const CHANNEL_COMMAND_EVENT_SCHEMA: &str = "agent-harness.channel-command-event.v1";
const AGENT_OVERRIDES_SCHEMA: &str = "agent-harness.agent-overrides.v1";
const RUNTIME_CANCEL_REQUEST_SCHEMA: &str = "agent-harness.runtime-cancel-request.v1";

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
    #[serde(default)]
    pub thinking_enabled: bool,
    #[serde(default)]
    pub thinking_level: Option<String>,
    #[serde(default)]
    pub thinking_instruction: Option<String>,
    #[serde(default)]
    pub fast_mode: Option<String>,
    #[serde(default)]
    pub stop_requested: bool,
    #[serde(default)]
    pub stop_reason: Option<String>,
    #[serde(default)]
    pub steering_notes: Vec<ChannelSessionNote>,
    #[serde(default)]
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

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentOverride {
    pub provider: Option<String>,
    pub model: Option<String>,
    pub thinking_level: Option<String>,
    #[serde(default)]
    pub fast_mode: Option<String>,
    pub updated_at_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentOverridesStore {
    #[serde(default = "agent_overrides_schema")]
    pub schema: String,
    #[serde(default)]
    pub agents: BTreeMap<String, AgentOverride>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct RuntimeCancelRequest {
    schema: &'static str,
    at_ms: i64,
    platform: String,
    channel_id: String,
    user_id: String,
    session_key: String,
    reason: Option<String>,
}

impl Default for AgentOverridesStore {
    fn default() -> Self {
        Self {
            schema: agent_overrides_schema(),
            agents: BTreeMap::new(),
        }
    }
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
    apply_effect(
        &mut state,
        &effect,
        &options.harness_home,
        options.now_ms,
        &mut warnings,
    )?;
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

    write_json_atomic(&state_file, &state)?;
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

pub fn agent_overrides_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("agents")
        .join("overrides.json")
}

pub fn read_agent_override(
    harness_home: impl AsRef<Path>,
    agent_id: &str,
) -> io::Result<Option<AgentOverride>> {
    let store = read_agent_overrides_store(harness_home)?;
    Ok(store.agents.get(agent_id).cloned())
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
        thinking_level: None,
        thinking_instruction: None,
        fast_mode: None,
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
    harness_home: &Path,
    now_ms: i64,
    warnings: &mut Vec<String>,
) -> io::Result<()> {
    state.last_command = Some(command_name(effect).to_string());
    state.updated_at_ms = now_ms;

    match effect {
        ChannelCommandEffect::StartNewSession {
            topic,
            new_session_key,
        } => {
            if let Err(error) =
                close_virtual_session_for_task_boundary(VirtualSessionTaskBoundaryCloseOptions {
                    harness_home: harness_home.to_path_buf(),
                    previous_session_key: state.active_session_key.clone(),
                    ended_by: "channel-command:/new".to_string(),
                    now_ms,
                })
            {
                warnings.push(format!(
                    "virtual session task boundary close failed for previous session `{}`: {error}",
                    state.active_session_key
                ));
            }
            if let Some(agent_id) = active_session_key_agent_segment(new_session_key) {
                if state.agent_id.as_deref() != Some(agent_id.as_str()) {
                    state.provider = None;
                    state.model = None;
                    state.model_override = None;
                    state.model_override_provider = None;
                    state.model_override_model = None;
                }
                state.agent_id = Some(agent_id);
            }
            state.active_session_key = new_session_key.clone();
            state.session_topic = topic.clone();
            state.thinking_enabled = false;
            state.thinking_level = None;
            state.thinking_instruction = None;
            state.fast_mode = None;
            state.stop_requested = false;
            state.stop_reason = None;
            state.steering_notes.clear();
            state.btw_notes.clear();
        }
        ChannelCommandEffect::ShowThinking {
            agent_id,
            provider,
            model,
            ..
        } => {
            update_model_context(state, agent_id, provider, model);
        }
        ChannelCommandEffect::SwitchThinking {
            agent_id,
            level,
            global,
            provider,
            model,
            valid,
            ..
        } => {
            update_model_context(state, agent_id, provider, model);
            if *valid {
                state.thinking_enabled = true;
                state.thinking_level = Some(level.clone());
                state.thinking_instruction = None;
                if *global {
                    write_agent_override_thinking(harness_home, agent_id, level, now_ms, warnings)?;
                }
            }
        }
        ChannelCommandEffect::StopCurrentRun { reason } => {
            state.stop_requested = true;
            state.stop_reason = reason.clone();
            write_runtime_cancel_request(harness_home, state, reason.clone(), now_ms)?;
        }
        ChannelCommandEffect::RestartChannel { .. } => {}
        ChannelCommandEffect::RestartGateway { .. } => {}
        ChannelCommandEffect::AddSteering { instruction } => {
            state.steering_notes.push(ChannelSessionNote {
                at_ms: now_ms,
                text: instruction.clone(),
            });
            if let Err(error) = queue_codex_turn_steer_request(CodexTurnSteerRequestOptions {
                harness_home: harness_home.to_path_buf(),
                platform: state.platform.clone(),
                channel_id: state.channel_id.clone(),
                user_id: state.user_id.clone(),
                session_key: state.active_session_key.clone(),
                agent_id: state.agent_id.clone(),
                text: instruction.clone(),
                client_user_message_id: None,
                now_ms,
            }) {
                warnings.push(format!(
                    "codex turn/steer bridge request could not be recorded: {error}"
                ));
            }
        }
        ChannelCommandEffect::AddBtwNote { note } => {
            state.btw_notes.push(ChannelSessionNote {
                at_ms: now_ms,
                text: note.clone(),
            });
        }
        ChannelCommandEffect::ShowModel {
            agent_id,
            current_provider,
            current_model,
            ..
        } => {
            update_model_context(state, agent_id, current_provider, current_model);
        }
        ChannelCommandEffect::ListProviderModels {
            agent_id,
            current_provider,
            current_model,
            ..
        } => {
            update_model_context(state, agent_id, current_provider, current_model);
        }
        ChannelCommandEffect::SwitchModel {
            agent_id,
            provider,
            model,
            global,
            current_provider,
            current_model,
            ..
        } => {
            update_model_context(state, agent_id, current_provider, current_model);
            state.model_override = Some(format!("{provider}/{model}"));
            state.model_override_provider = Some(provider.clone());
            state.model_override_model = Some(model.clone());
            if *global {
                write_agent_override_model(
                    harness_home,
                    agent_id,
                    provider,
                    model,
                    now_ms,
                    warnings,
                )?;
            }
        }
        ChannelCommandEffect::ShowFast {
            agent_id,
            provider,
            model,
            ..
        } => {
            update_model_context(state, agent_id, provider, model);
        }
        ChannelCommandEffect::SwitchFast {
            agent_id,
            provider,
            model,
            global,
            mode,
            ..
        } => {
            update_model_context(state, agent_id, provider, model);
            state.fast_mode = Some(mode.clone());
            if *global {
                write_agent_override_fast_mode(harness_home, agent_id, mode, now_ms, warnings)?;
            }
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
    Ok(())
}

fn active_session_key_agent_segment(session_key: &str) -> Option<String> {
    session_key
        .split(':')
        .nth(3)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
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
        ChannelCommandEffect::ShowThinking { .. } => "think",
        ChannelCommandEffect::SwitchThinking { .. } => "think",
        ChannelCommandEffect::StopCurrentRun { .. } => "stop",
        ChannelCommandEffect::RestartChannel { .. } => "restart",
        ChannelCommandEffect::RestartGateway { .. } => "restart",
        ChannelCommandEffect::AddSteering { .. } => "steer",
        ChannelCommandEffect::AddBtwNote { .. } => "btw",
        ChannelCommandEffect::ShowModel { .. } => "model",
        ChannelCommandEffect::ListProviderModels { .. } => "model",
        ChannelCommandEffect::SwitchModel { .. } => "model",
        ChannelCommandEffect::ShowFast { .. } => "fast",
        ChannelCommandEffect::SwitchFast { .. } => "fast",
        ChannelCommandEffect::ShowStatus { .. } => "status",
    }
}

fn read_agent_overrides_store(harness_home: impl AsRef<Path>) -> io::Result<AgentOverridesStore> {
    let path = agent_overrides_file(harness_home);
    match fs::read(&path) {
        Ok(bytes) => serde_json::from_slice(&bytes).map_err(io::Error::other),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(AgentOverridesStore::default()),
        Err(error) => Err(error),
    }
}

fn write_agent_overrides_store(harness_home: &Path, store: &AgentOverridesStore) -> io::Result<()> {
    let path = agent_overrides_file(harness_home);
    write_json_atomic(&path, store)
}

fn write_runtime_cancel_request(
    harness_home: &Path,
    state: &ChannelSessionState,
    reason: Option<String>,
    now_ms: i64,
) -> io::Result<()> {
    let path = harness_home
        .join("state")
        .join("runtime-queue")
        .join("cancel-requests")
        .join(format!(
            "{}.json",
            normalize_key_part(&state.active_session_key)
        ));
    write_json_atomic(
        &path,
        &RuntimeCancelRequest {
            schema: RUNTIME_CANCEL_REQUEST_SCHEMA,
            at_ms: now_ms,
            platform: state.platform.clone(),
            channel_id: state.channel_id.clone(),
            user_id: state.user_id.clone(),
            session_key: state.active_session_key.clone(),
            reason,
        },
    )
}

fn write_agent_override_model(
    harness_home: &Path,
    agent_id: &Option<String>,
    provider: &str,
    model: &str,
    now_ms: i64,
    warnings: &mut Vec<String>,
) -> io::Result<()> {
    let Some(agent_id) = agent_id else {
        warnings.push("model --global was requested but no current agent was selected".to_string());
        return Ok(());
    };
    let mut store = read_agent_overrides_store(harness_home)?;
    let override_entry = store.agents.entry(agent_id.clone()).or_default();
    override_entry.provider = Some(provider.to_string());
    override_entry.model = Some(model.to_string());
    override_entry.updated_at_ms = Some(now_ms);
    write_agent_overrides_store(harness_home, &store)
}

fn write_agent_override_thinking(
    harness_home: &Path,
    agent_id: &Option<String>,
    level: &str,
    now_ms: i64,
    warnings: &mut Vec<String>,
) -> io::Result<()> {
    let Some(agent_id) = agent_id else {
        warnings.push("think --global was requested but no current agent was selected".to_string());
        return Ok(());
    };
    let mut store = read_agent_overrides_store(harness_home)?;
    let override_entry = store.agents.entry(agent_id.clone()).or_default();
    override_entry.thinking_level = Some(level.to_string());
    override_entry.updated_at_ms = Some(now_ms);
    write_agent_overrides_store(harness_home, &store)
}

fn write_agent_override_fast_mode(
    harness_home: &Path,
    agent_id: &Option<String>,
    mode: &str,
    now_ms: i64,
    warnings: &mut Vec<String>,
) -> io::Result<()> {
    let Some(agent_id) = agent_id else {
        warnings.push("fast --global was requested but no current agent was selected".to_string());
        return Ok(());
    };
    let mut store = read_agent_overrides_store(harness_home)?;
    let override_entry = store.agents.entry(agent_id.clone()).or_default();
    override_entry.fast_mode = Some(mode.to_string());
    override_entry.updated_at_ms = Some(now_ms);
    write_agent_overrides_store(harness_home, &store)
}

fn agent_overrides_schema() -> String {
    AGENT_OVERRIDES_SCHEMA.to_string()
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
    crate::append_jsonl_value(path, value)
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
    use crate::{
        ChannelOutboundMessageKind, ChannelStatusSnapshot, ChannelStepAction,
        CompletedTurnWorkingSetSnapshotOptions, ContextVirtualSessionRecord,
        record_completed_turn_working_set_snapshot,
    };
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn applies_new_session_and_accumulates_notes() {
        let root = temp_root("applies_new_session_and_accumulates_notes");
        let harness_home = root.join(".agent-harness");
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
        let harness_home = root.join(".agent-harness");
        let step = command_step(ChannelCommandEffect::SwitchModel {
            agent_id: Some("main".to_string()),
            provider: "openrouter".to_string(),
            model: "anthropic/claude-sonnet-4".to_string(),
            global: false,
            current_provider: Some("openai".to_string()),
            current_model: Some("gpt-5".to_string()),
            provider_known: true,
            model_known: true,
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
    fn new_session_command_closes_previous_virtual_session_record() {
        let root = temp_root("new_session_command_closes_previous_virtual_session_record");
        let harness_home = root.join(".agent-harness");
        let previous_session = "telegram:dm:user:main";
        let snapshot =
            record_completed_turn_working_set_snapshot(CompletedTurnWorkingSetSnapshotOptions {
                harness_home: harness_home.clone(),
                platform: "telegram".to_string(),
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                agent_id: "main".to_string(),
                working_session_key: previous_session.to_string(),
                queue_id: Some("queue-before-new".to_string()),
                message_text: Some("previous virtual session task".to_string()),
                status: "completed".to_string(),
                run_once_receipt_file: None,
                outbox_file: None,
                completion_file: None,
                now_ms: 900,
            })
            .unwrap();
        let new_step = command_step_with_session(
            previous_session,
            ChannelCommandEffect::StartNewSession {
                topic: Some("fresh task".to_string()),
                new_session_key: "telegram:dm:user:main:session-new".to_string(),
            },
        );

        let report = apply_channel_command_step(
            &new_step,
            ChannelCommandApplyOptions {
                harness_home: harness_home.clone(),
                now_ms: 1000,
            },
        )
        .unwrap();

        assert!(report.warnings.is_empty());
        let record: ContextVirtualSessionRecord =
            serde_json::from_slice(&fs::read(snapshot.virtual_session_file).unwrap()).unwrap();
        assert_eq!(record.status, "closed");
        let previous = record
            .working_sessions
            .iter()
            .find(|session| session.session_key == previous_session)
            .expect("previous working session");
        assert_eq!(previous.ended_at_ms, Some(1000));
        assert_eq!(previous.ended_by.as_deref(), Some("channel-command:/new"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn applies_fast_mode_and_new_session_clears_it() {
        let root = temp_root("applies_fast_mode_and_new_session_clears_it");
        let harness_home = root.join(".agent-harness");
        let fast_step = command_step(ChannelCommandEffect::SwitchFast {
            agent_id: Some("main".to_string()),
            provider: Some("openai".to_string()),
            model: Some("gpt-5".to_string()),
            global: false,
            previous_mode: "normal".to_string(),
            mode: "fast".to_string(),
            effective_acceleration: "enabled".to_string(),
            reason: "Codex app-server serviceTier=priority will be requested".to_string(),
        });

        let report = apply_channel_command_step(
            &fast_step,
            ChannelCommandApplyOptions {
                harness_home: harness_home.clone(),
                now_ms: 2000,
            },
        )
        .unwrap();

        let state = report.state.unwrap();
        assert_eq!(state.fast_mode.as_deref(), Some("fast"));
        assert_eq!(state.last_command.as_deref(), Some("fast"));

        let new_step = command_step_with_session(
            "telegram:dm:user:main",
            ChannelCommandEffect::StartNewSession {
                topic: None,
                new_session_key: "telegram:dm:user:main:new".to_string(),
            },
        );
        let report = apply_channel_command_step(
            &new_step,
            ChannelCommandApplyOptions {
                harness_home,
                now_ms: 2001,
            },
        )
        .unwrap();

        let state = report.state.unwrap();
        assert_eq!(state.active_session_key, "telegram:dm:user:main:new");
        assert_eq!(state.fast_mode, None);
        assert_eq!(state.last_command.as_deref(), Some("new"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn applies_global_fast_mode_as_agent_override() {
        let root = temp_root("applies_global_fast_mode_as_agent_override");
        let harness_home = root.join(".agent-harness");
        let fast_step = command_step(ChannelCommandEffect::SwitchFast {
            agent_id: Some("main".to_string()),
            provider: Some("openai".to_string()),
            model: Some("gpt-5".to_string()),
            global: true,
            previous_mode: "normal".to_string(),
            mode: "fast".to_string(),
            effective_acceleration: "enabled".to_string(),
            reason: "Codex app-server serviceTier=priority will be requested".to_string(),
        });

        let report = apply_channel_command_step(
            &fast_step,
            ChannelCommandApplyOptions {
                harness_home: harness_home.clone(),
                now_ms: 2100,
            },
        )
        .unwrap();

        let state = report.state.unwrap();
        assert_eq!(state.fast_mode.as_deref(), Some("fast"));
        let override_entry = read_agent_override(&harness_home, "main").unwrap().unwrap();
        assert_eq!(override_entry.fast_mode.as_deref(), Some("fast"));
        assert_eq!(override_entry.updated_at_ms, Some(2100));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn stop_command_writes_runtime_cancel_marker() {
        let root = temp_root("stop_command_writes_runtime_cancel_marker");
        let harness_home = root.join(".agent-harness");
        let step = command_step_with_session(
            "telegram:dm:user:main",
            ChannelCommandEffect::StopCurrentRun {
                reason: Some("operator stop".to_string()),
            },
        );

        let report = apply_channel_command_step(
            &step,
            ChannelCommandApplyOptions {
                harness_home: harness_home.clone(),
                now_ms: 3000,
            },
        )
        .unwrap();

        assert_eq!(
            report.state.as_ref().unwrap().stop_reason.as_deref(),
            Some("operator stop")
        );
        let marker = harness_home
            .join("state")
            .join("runtime-queue")
            .join("cancel-requests")
            .join("telegram_dm_user_main.json");
        let value: serde_json::Value = serde_json::from_slice(&fs::read(marker).unwrap()).unwrap();
        assert_eq!(value["sessionKey"], "telegram:dm:user:main");
        assert_eq!(value["reason"], "operator stop");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn applies_per_agent_global_model_and_thinking_overrides() {
        let root = temp_root("applies_per_agent_global_model_and_thinking_overrides");
        let harness_home = root.join(".agent-harness");
        let model_step = command_step(ChannelCommandEffect::SwitchModel {
            agent_id: Some("main".to_string()),
            provider: "openrouter".to_string(),
            model: "anthropic/claude-sonnet-4".to_string(),
            global: true,
            current_provider: Some("openai".to_string()),
            current_model: Some("gpt-5".to_string()),
            provider_known: true,
            model_known: true,
        });
        apply_channel_command_step(
            &model_step,
            ChannelCommandApplyOptions {
                harness_home: harness_home.clone(),
                now_ms: 2000,
            },
        )
        .unwrap();

        let think_step = command_step(ChannelCommandEffect::SwitchThinking {
            agent_id: Some("main".to_string()),
            provider: Some("openrouter".to_string()),
            model: Some("anthropic/claude-sonnet-4".to_string()),
            thinking_enabled: false,
            current_level: Some("medium".to_string()),
            level: "high".to_string(),
            global: true,
            valid: true,
            available_levels: vec![
                "minimal".to_string(),
                "low".to_string(),
                "medium".to_string(),
                "high".to_string(),
            ],
        });
        let report = apply_channel_command_step(
            &think_step,
            ChannelCommandApplyOptions {
                harness_home: harness_home.clone(),
                now_ms: 2001,
            },
        )
        .unwrap();

        let state = report.state.unwrap();
        assert!(state.thinking_enabled);
        assert_eq!(state.thinking_level.as_deref(), Some("high"));
        let override_entry = read_agent_override(&harness_home, "main").unwrap().unwrap();
        assert_eq!(override_entry.provider.as_deref(), Some("openrouter"));
        assert_eq!(
            override_entry.model.as_deref(),
            Some("anthropic/claude-sonnet-4")
        );
        assert_eq!(override_entry.thinking_level.as_deref(), Some("high"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn new_session_records_agent_from_new_session_key() {
        let root = temp_root("new_session_records_agent_from_new_session_key");
        let harness_home = root.join(".agent-harness");
        let new_step = command_step_with_session(
            "telegram:dm:user:main",
            ChannelCommandEffect::StartNewSession {
                topic: Some("xiaoxiaoli lane".to_string()),
                new_session_key: "telegram:dm:user:xiaoxiaoli:session-1".to_string(),
            },
        );

        let report = apply_channel_command_step(
            &new_step,
            ChannelCommandApplyOptions {
                harness_home,
                now_ms: 1000,
            },
        )
        .unwrap();

        let state = report.state.unwrap();
        assert_eq!(
            state.active_session_key,
            "telegram:dm:user:xiaoxiaoli:session-1"
        );
        assert_eq!(state.agent_id.as_deref(), Some("xiaoxiaoli"));
        assert_eq!(report.receipt.session_key, state.active_session_key);

        let _ = fs::remove_dir_all(root);
    }
    #[test]
    fn skips_non_command_steps() {
        let root = temp_root("skips_non_command_steps");
        let harness_home = root.join(".agent-harness");
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
                codex_sandbox_policy: None,
                prompt_files_present: 1,
                prompt_files_total: 1,
                prompt_file_names: vec!["AGENTS.md".to_string()],
                selected_skills: 0,
                selected_skill_ids: Vec::new(),
                channel_state_loaded: false,
                active_session_key: None,
                thinking_enabled: false,
                thinking_level: Some("medium".to_string()),
                fast_mode: None,
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
            account_id: None,
            channel_id: "dm".to_string(),
            user_id: "user".to_string(),
            message_text: "/test".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            session_key: session_key.to_string(),
            action: ChannelStepAction::ReplyOnly,
            command_effect: Some(effect),
            agent_turn: None,
            outbound_messages: vec![ChannelOutboundMessage {
                platform: "telegram".to_string(),
                account_id: None,
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                session_key: session_key.to_string(),
                kind: ChannelOutboundMessageKind::CommandReply,
                source_queue_id: None,
                source_completion_file: None,
                text: "ok".to_string(),
                presentation: None,
                delivery_intent: None,
                attachments: Vec::new(),
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
            "agent-harness-channel-state-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
