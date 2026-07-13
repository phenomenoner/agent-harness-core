use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::execution_mode::{ExecutionModePolicyV1, ExecutionModePreference};
use crate::{
    ChannelCommandEffect, ChannelOutboundMessage, ChannelStep,
    VirtualSessionTaskBoundaryCloseOptions,
    admission::{ScopedStopOptions, ScopedStopTarget, record_scoped_stop},
    backend_reasoning::{BackendReasoningPolicyV1, BackendReasoningSource, ReasoningPreference},
    close_virtual_session_for_task_boundary,
    codex_runtime::{CodexTurnSteerRequestOptions, queue_codex_turn_steer_request},
    progress::{AgentProgressSessionSupersedeOptions, supersede_agent_progress_session_surfaces},
    write_json_atomic,
};

const CHANNEL_COMMAND_APPLY_SCHEMA: &str = "agent-harness.channel-command-apply.v1";
const CHANNEL_STATE_SCHEMA: &str = "agent-harness.channel-session-state.v1";
const CHANNEL_COMMAND_EVENT_SCHEMA: &str = "agent-harness.channel-command-event.v1";
const AGENT_OVERRIDES_SCHEMA: &str = "agent-harness.agent-overrides.v1";
const CHANNEL_EXECUTION_MODE_SCHEMA: &str = "agent-harness.channel-execution-mode.v1";
const AGENT_EXECUTION_MODE_OVERRIDES_SCHEMA: &str =
    "agent-harness.agent-execution-mode-overrides.v1";
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effect: Option<ChannelCommandEffect>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_scope: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub stop_applied_queue_ids: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duplicate_sibling_count: Option<usize>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_preference: Option<ReasoningPreference>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend_reasoning_policy: Option<BackendReasoningPolicyV1>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_preference: Option<ReasoningPreference>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend_reasoning_policy: Option<BackendReasoningPolicyV1>,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelExecutionModeStateV1 {
    pub schema: String,
    pub preference: ExecutionModePreference,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<ExecutionModePolicyV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub migration_reason: Option<String>,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AgentExecutionModeOverridesStoreV1 {
    schema: String,
    #[serde(default)]
    agents: BTreeMap<String, ChannelExecutionModeStateV1>,
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
            command: None,
            effect: None,
            stop_scope: None,
            stop_applied_queue_ids: Vec::new(),
            duplicate_sibling_count: None,
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
    let stop_applied_queue_ids = apply_effect(
        &mut state,
        &effect,
        &options.harness_home,
        options.now_ms,
        &mut warnings,
    )?;
    let stop_scope = if stop_applied_queue_ids.is_empty() {
        None
    } else {
        Some("active-session-siblings".to_string())
    };
    let duplicate_sibling_count = if stop_applied_queue_ids.is_empty() {
        None
    } else {
        Some(stop_applied_queue_ids.len().saturating_sub(1))
    };
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
        command: Some(event.command),
        effect: Some(event.effect.clone()),
        stop_scope,
        stop_applied_queue_ids,
        duplicate_sibling_count,
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

pub fn channel_execution_mode_state_file(
    harness_home: &Path,
    platform: &str,
    channel_id: &str,
    user_id: &str,
) -> PathBuf {
    channel_session_state_dir(harness_home, platform, channel_id, user_id)
        .join("execution-mode.json")
}

pub fn read_channel_execution_mode_state(
    harness_home: &Path,
    platform: &str,
    channel_id: &str,
    user_id: &str,
) -> io::Result<Option<ChannelExecutionModeStateV1>> {
    let path = channel_execution_mode_state_file(harness_home, platform, channel_id, user_id);
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(io::Error::other),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn agent_execution_mode_overrides_file(harness_home: &Path) -> PathBuf {
    harness_home
        .join("state")
        .join("agents")
        .join("execution-mode-overrides.json")
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
        Ok(bytes) => {
            let mut state =
                serde_json::from_slice::<ChannelSessionState>(&bytes).map_err(io::Error::other)?;
            if let Some(execution_state) = quarantine_legacy_ultra_state(
                &mut state.reasoning_preference,
                &mut state.backend_reasoning_policy,
                &mut state.thinking_level,
                state.updated_at_ms,
            ) {
                let execution_file = path
                    .parent()
                    .unwrap_or_else(|| Path::new("."))
                    .join("execution-mode.json");
                write_json_atomic(&execution_file, &execution_state)?;
                write_json_atomic(path, &state)?;
            }
            Ok(Some(state))
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn quarantine_legacy_ultra_state(
    reasoning_preference: &mut Option<ReasoningPreference>,
    backend_reasoning_policy: &mut Option<BackendReasoningPolicyV1>,
    thinking_level: &mut Option<String>,
    updated_at_ms: i64,
) -> Option<ChannelExecutionModeStateV1> {
    let preference = reasoning_preference
        .as_ref()
        .and_then(ExecutionModePreference::quarantine_legacy_ultra_reasoning)?;
    *reasoning_preference = None;
    *backend_reasoning_policy = None;
    if thinking_level
        .as_deref()
        .is_some_and(|level| level.eq_ignore_ascii_case("ultra"))
    {
        *thinking_level = None;
    }
    Some(ChannelExecutionModeStateV1 {
        schema: CHANNEL_EXECUTION_MODE_SCHEMA.to_string(),
        preference,
        policy: None,
        migration_reason: Some(
            "legacy reasoning effort `ultra` quarantined; explicit reissue required".to_string(),
        ),
        updated_at_ms,
    })
}

#[cfg(test)]
mod execution_mode_state_tests {
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::backend_reasoning::{BackendReasoningSource, ReasoningPreference};
    use crate::model_catalog::{
        REASONING_RESOLUTION_RECEIPT_SCHEMA_VERSION, ReasoningResolutionReceipt,
        ReasoningResolutionStatus,
    };

    #[test]
    fn legacy_persisted_ultra_reasoning_is_quarantined_without_backend_policy() {
        let mut preference = Some(ReasoningPreference::explicit("ultra").unwrap());
        let mut policy = Some(
            BackendReasoningPolicyV1::new(
                BackendReasoningSource::ChannelCommand,
                ReasoningResolutionReceipt {
                    schema_version: REASONING_RESOLUTION_RECEIPT_SCHEMA_VERSION,
                    requested_provider: "openai".to_string(),
                    requested_model: "gpt-5.6-sol".to_string(),
                    effective_provider: Some("openai".to_string()),
                    effective_model: Some("gpt-5.6-sol".to_string()),
                    requested_effort: "ultra".to_string(),
                    effective_effort: Some("ultra".to_string()),
                    catalog_effective_effort: Some("ultra".to_string()),
                    catalog_revision: Some("legacy".to_string()),
                    status: ReasoningResolutionStatus::Accepted,
                    authoritative: true,
                    reason: "legacy fixture".to_string(),
                },
            )
            .unwrap(),
        );
        let mut thinking_level = Some("ultra".to_string());
        let migrated =
            quarantine_legacy_ultra_state(&mut preference, &mut policy, &mut thinking_level, 1000)
                .unwrap();
        assert!(preference.is_none());
        assert!(policy.is_none());
        assert!(thinking_level.is_none());
        assert!(matches!(
            migrated.preference,
            ExecutionModePreference::LegacyQuarantined { ref mode } if mode == "ultra"
        ));
        assert!(migrated.policy.is_none());
        assert!(!migrated.preference.is_authorizable());
    }

    #[test]
    fn separate_execution_preference_persists_without_rewriting_reasoning() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let harness_home =
            std::env::temp_dir().join(format!("agent-harness-execution-state-{nonce}"));
        let state = ChannelSessionState {
            schema: CHANNEL_STATE_SCHEMA.to_string(),
            platform: "discord".to_string(),
            channel_id: "channel-1".to_string(),
            user_id: "user-1".to_string(),
            active_session_key: "session-1".to_string(),
            agent_id: Some("main".to_string()),
            provider: Some("openai".to_string()),
            model: Some("gpt-5.6-sol".to_string()),
            session_topic: None,
            model_override: None,
            model_override_provider: None,
            model_override_model: None,
            thinking_enabled: true,
            thinking_level: Some("max".to_string()),
            thinking_instruction: None,
            reasoning_preference: Some(ReasoningPreference::explicit("max").unwrap()),
            backend_reasoning_policy: None,
            fast_mode: None,
            stop_requested: false,
            stop_reason: None,
            steering_notes: Vec::new(),
            btw_notes: Vec::new(),
            last_command: None,
            updated_at_ms: 1000,
        };
        let execution = ChannelExecutionModeStateV1 {
            schema: CHANNEL_EXECUTION_MODE_SCHEMA.to_string(),
            preference: ExecutionModePreference::explicit("standard").unwrap(),
            policy: None,
            migration_reason: None,
            updated_at_ms: 1001,
        };
        write_channel_execution_mode_state(&harness_home, &state, &execution).unwrap();
        assert_eq!(
            read_channel_execution_mode_state(
                &harness_home,
                &state.platform,
                &state.channel_id,
                &state.user_id,
            )
            .unwrap(),
            Some(execution)
        );
        assert_eq!(
            state
                .reasoning_preference
                .as_ref()
                .and_then(ReasoningPreference::explicit_effort),
            Some("max")
        );
        let _ = fs::remove_dir_all(harness_home);
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
        reasoning_preference: None,
        backend_reasoning_policy: None,
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
) -> io::Result<Vec<String>> {
    state.last_command = Some(command_name(effect).to_string());
    state.updated_at_ms = now_ms;
    let mut stop_applied_queue_ids = Vec::new();

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
            if state.active_session_key != *new_session_key
                && let Err(error) = supersede_agent_progress_session_surfaces(
                    AgentProgressSessionSupersedeOptions {
                        harness_home: harness_home.to_path_buf(),
                        platform: state.platform.clone(),
                        account_id: None,
                        channel_id: state.channel_id.clone(),
                        user_id: state.user_id.clone(),
                        agent_id: state.agent_id.clone(),
                        session_key: state.active_session_key.clone(),
                        now_ms,
                    },
                )
            {
                warnings.push(format!(
                    "progress surfaces could not be superseded for previous session `{}`: {error}",
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
            state.reasoning_preference = None;
            state.backend_reasoning_policy = None;
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
                state.reasoning_preference = None;
                state.backend_reasoning_policy = None;
                if *global {
                    write_agent_override_thinking(harness_home, agent_id, level, now_ms, warnings)?;
                }
            }
        }
        // Unified `/think` status is observational. In particular, querying a
        // different selected agent or route must not rewrite or invalidate any
        // persisted channel control state.
        ChannelCommandEffect::ShowReasoning { .. } => {}
        ChannelCommandEffect::SwitchReasoning {
            agent_id,
            provider,
            model,
            preference,
            global,
            accepted,
            resolved_policy,
            resolution,
            catalog_default,
            ..
        } => {
            if *accepted {
                if let Err(error) = validate_accepted_reasoning_effect(
                    preference,
                    resolved_policy.as_ref(),
                    resolution.as_ref(),
                    provider.as_deref(),
                    model.as_deref(),
                    catalog_default.as_deref(),
                ) {
                    warnings.push(format!(
                        "rejected inconsistent backend reasoning effect: {error}"
                    ));
                    return Ok(stop_applied_queue_ids);
                }
                update_model_context(state, agent_id, provider, model);
                state.reasoning_preference = Some(preference.clone());
                state.backend_reasoning_policy = resolved_policy.clone();
                match preference {
                    ReasoningPreference::Default => {
                        state.thinking_enabled = false;
                        state.thinking_level = None;
                        state.thinking_instruction = None;
                    }
                    ReasoningPreference::Explicit { effort } => {
                        state.thinking_enabled = true;
                        state.thinking_level = Some(effort.clone());
                        state.thinking_instruction = None;
                    }
                }
                if *global {
                    write_agent_override_reasoning(
                        harness_home,
                        agent_id,
                        preference,
                        resolved_policy.as_ref(),
                        now_ms,
                        warnings,
                    )?;
                }
            }
        }
        ChannelCommandEffect::SwitchExecutionMode {
            agent_id,
            preference,
            global,
            accepted,
            resolved_policy,
            ..
        } => {
            if *accepted {
                let execution_state = ChannelExecutionModeStateV1 {
                    schema: CHANNEL_EXECUTION_MODE_SCHEMA.to_string(),
                    preference: preference.clone(),
                    policy: resolved_policy.clone(),
                    migration_reason: None,
                    updated_at_ms: now_ms,
                };
                write_channel_execution_mode_state(harness_home, state, &execution_state)?;
                if *global {
                    write_agent_execution_mode_override(
                        harness_home,
                        agent_id,
                        &execution_state,
                        warnings,
                    )?;
                }
            }
        }
        ChannelCommandEffect::StopCurrentRun { reason } => {
            state.stop_requested = true;
            state.stop_reason = reason.clone();
            write_runtime_cancel_request(harness_home, state, reason.clone(), now_ms)?;
            stop_applied_queue_ids =
                record_active_session_sibling_stops(harness_home, state, reason.clone(), now_ms)?;
        }
        ChannelCommandEffect::RestartChannel { .. } => {}
        ChannelCommandEffect::RestartGateway { .. } => {}
        ChannelCommandEffect::RestartStatus { .. } => {}
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
            state.backend_reasoning_policy = None;
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
        ChannelCommandEffect::UnknownCommand { .. } => {}
    }

    if state.model_override.is_some() && state.model_override_model.is_none() {
        warnings.push("model override target did not include a usable model name".to_string());
    }
    Ok(stop_applied_queue_ids)
}

fn active_session_key_agent_segment(session_key: &str) -> Option<String> {
    session_key
        .split(':')
        .nth(3)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

fn record_active_session_sibling_stops(
    harness_home: &Path,
    state: &ChannelSessionState,
    reason: Option<String>,
    now_ms: i64,
) -> io::Result<Vec<String>> {
    let queue_ids = active_session_sibling_queue_ids(harness_home, state)?;
    let reason = reason.unwrap_or_else(|| "operator requested stop".to_string());
    for queue_id in &queue_ids {
        record_scoped_stop(ScopedStopOptions {
            harness_home: harness_home.to_path_buf(),
            target: ScopedStopTarget::QueueItem {
                queue_id: queue_id.clone(),
            },
            reason: reason.clone(),
            now_ms,
        })?;
    }
    Ok(queue_ids)
}

fn active_session_sibling_queue_ids(
    harness_home: &Path,
    state: &ChannelSessionState,
) -> io::Result<Vec<String>> {
    let pending_file = harness_home
        .join("state")
        .join("runtime-queue")
        .join("pending.jsonl");
    let text = match fs::read_to_string(&pending_file) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let expected_agent = state
        .agent_id
        .clone()
        .or_else(|| active_session_key_agent_segment(&state.active_session_key));
    let mut seen = BTreeSet::new();
    let mut queue_ids = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        if value.get("platform").and_then(serde_json::Value::as_str)
            != Some(state.platform.as_str())
            || value.get("channelId").and_then(serde_json::Value::as_str)
                != Some(state.channel_id.as_str())
            || value.get("userId").and_then(serde_json::Value::as_str)
                != Some(state.user_id.as_str())
            || value.get("sessionKey").and_then(serde_json::Value::as_str)
                != Some(state.active_session_key.as_str())
        {
            continue;
        }
        if let Some(expected_agent) = expected_agent.as_deref() {
            if value.get("agentId").and_then(serde_json::Value::as_str) != Some(expected_agent) {
                continue;
            }
        }
        let Some(queue_id) = value
            .get("queueId")
            .and_then(serde_json::Value::as_str)
            .filter(|queue_id| !queue_id.is_empty())
        else {
            continue;
        };
        if seen.insert(queue_id.to_string()) {
            queue_ids.push(queue_id.to_string());
        }
    }
    Ok(queue_ids)
}

fn validate_accepted_reasoning_effect(
    preference: &ReasoningPreference,
    policy: Option<&BackendReasoningPolicyV1>,
    resolution: Option<&crate::model_catalog::ReasoningResolutionReceipt>,
    provider: Option<&str>,
    model: Option<&str>,
    catalog_default: Option<&str>,
) -> Result<(), String> {
    preference.validate().map_err(|error| error.to_string())?;
    let provider = provider
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "execution provider is missing".to_string())?;
    let model = model
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "execution model is missing".to_string())?;

    match preference {
        ReasoningPreference::Default => {
            if policy.is_some() {
                return Err("default reset must not carry an explicit execution policy".to_string());
            }
            match (catalog_default, resolution) {
                (None, None) => Ok(()),
                (Some(default), Some(resolution))
                    if resolution.status
                        == crate::model_catalog::ReasoningResolutionStatus::Accepted
                        && resolution.effective_effort.as_deref() == Some(default) =>
                {
                    Ok(())
                }
                _ => Err("default reset catalog evidence is inconsistent".to_string()),
            }
        }
        ReasoningPreference::Explicit { effort } => {
            let policy = policy
                .ok_or_else(|| "explicit preference is missing its execution policy".to_string())?;
            if policy.source() != BackendReasoningSource::ChannelCommand {
                return Err("channel command policy has the wrong source".to_string());
            }
            policy
                .validate_for_route(provider, model)
                .map_err(|error| error.to_string())?;
            if policy.effective_effort() != effort {
                return Err(format!(
                    "preference effort `{effort}` does not match policy effort `{}`",
                    policy.effective_effort()
                ));
            }
            if resolution != Some(policy.resolution()) {
                return Err(
                    "effect resolution does not match the execution policy receipt".to_string(),
                );
            }
            Ok(())
        }
    }
}

fn update_model_context(
    state: &mut ChannelSessionState,
    agent_id: &Option<String>,
    provider: &Option<String>,
    model: &Option<String>,
) {
    let agent_changed = agent_id
        .as_ref()
        .is_some_and(|agent_id| state.agent_id.as_ref() != Some(agent_id));
    let route_changed = provider
        .as_ref()
        .is_some_and(|provider| state.provider.as_ref() != Some(provider))
        || model
            .as_ref()
            .is_some_and(|model| state.model.as_ref() != Some(model));
    if agent_changed {
        state.reasoning_preference = None;
        state.backend_reasoning_policy = None;
    } else if route_changed {
        state.backend_reasoning_policy = None;
    }
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
        ChannelCommandEffect::ShowReasoning { .. } => "think",
        ChannelCommandEffect::SwitchReasoning { .. } => "think",
        ChannelCommandEffect::SwitchExecutionMode { .. } => "think",
        ChannelCommandEffect::StopCurrentRun { .. } => "stop",
        ChannelCommandEffect::RestartChannel { .. } => "restart",
        ChannelCommandEffect::RestartGateway { .. } => "restart",
        ChannelCommandEffect::RestartStatus { .. } => "restart-status",
        ChannelCommandEffect::AddSteering { .. } => "steer",
        ChannelCommandEffect::AddBtwNote { .. } => "btw",
        ChannelCommandEffect::ShowModel { .. } => "model",
        ChannelCommandEffect::ListProviderModels { .. } => "model",
        ChannelCommandEffect::SwitchModel { .. } => "model",
        ChannelCommandEffect::ShowFast { .. } => "fast",
        ChannelCommandEffect::SwitchFast { .. } => "fast",
        ChannelCommandEffect::ShowStatus { .. } => "status",
        ChannelCommandEffect::UnknownCommand { .. } => "unknown-command",
    }
}

fn write_channel_execution_mode_state(
    harness_home: &Path,
    state: &ChannelSessionState,
    execution_state: &ChannelExecutionModeStateV1,
) -> io::Result<()> {
    write_json_atomic(
        &channel_execution_mode_state_file(
            harness_home,
            &state.platform,
            &state.channel_id,
            &state.user_id,
        ),
        execution_state,
    )
}

fn read_agent_execution_mode_overrides_store(
    harness_home: &Path,
) -> io::Result<AgentExecutionModeOverridesStoreV1> {
    let path = agent_execution_mode_overrides_file(harness_home);
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes).map_err(io::Error::other),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            Ok(AgentExecutionModeOverridesStoreV1 {
                schema: AGENT_EXECUTION_MODE_OVERRIDES_SCHEMA.to_string(),
                agents: BTreeMap::new(),
            })
        }
        Err(error) => Err(error),
    }
}

pub fn read_agent_execution_mode_override(
    harness_home: &Path,
    agent_id: &str,
) -> io::Result<Option<ChannelExecutionModeStateV1>> {
    Ok(read_agent_execution_mode_overrides_store(harness_home)?
        .agents
        .get(agent_id)
        .cloned())
}

fn write_agent_execution_mode_override(
    harness_home: &Path,
    agent_id: &Option<String>,
    state: &ChannelExecutionModeStateV1,
    warnings: &mut Vec<String>,
) -> io::Result<()> {
    let Some(agent_id) = agent_id else {
        warnings.push("execution mode --global requires a selected agent".to_string());
        return Ok(());
    };
    let mut store = read_agent_execution_mode_overrides_store(harness_home)?;
    store.agents.insert(agent_id.clone(), state.clone());
    write_json_atomic(&agent_execution_mode_overrides_file(harness_home), &store)
}

fn read_agent_overrides_store(harness_home: impl AsRef<Path>) -> io::Result<AgentOverridesStore> {
    let harness_home = harness_home.as_ref();
    let path = agent_overrides_file(harness_home);
    match fs::read(&path) {
        Ok(bytes) => {
            let mut store =
                serde_json::from_slice::<AgentOverridesStore>(&bytes).map_err(io::Error::other)?;
            let mut execution_store = read_agent_execution_mode_overrides_store(harness_home)?;
            let mut migrated = false;
            for (agent_id, override_entry) in &mut store.agents {
                if let Some(execution_state) = quarantine_legacy_ultra_state(
                    &mut override_entry.reasoning_preference,
                    &mut override_entry.backend_reasoning_policy,
                    &mut override_entry.thinking_level,
                    override_entry.updated_at_ms.unwrap_or(0),
                ) {
                    execution_store
                        .agents
                        .insert(agent_id.clone(), execution_state);
                    migrated = true;
                }
            }
            if migrated {
                write_agent_overrides_store(harness_home, &store)?;
                write_json_atomic(
                    &agent_execution_mode_overrides_file(harness_home),
                    &execution_store,
                )?;
            }
            Ok(store)
        }
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
    override_entry.backend_reasoning_policy = None;
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
    override_entry.reasoning_preference = None;
    override_entry.backend_reasoning_policy = None;
    override_entry.updated_at_ms = Some(now_ms);
    write_agent_overrides_store(harness_home, &store)
}

fn write_agent_override_reasoning(
    harness_home: &Path,
    agent_id: &Option<String>,
    preference: &ReasoningPreference,
    policy: Option<&BackendReasoningPolicyV1>,
    now_ms: i64,
    warnings: &mut Vec<String>,
) -> io::Result<()> {
    let Some(agent_id) = agent_id else {
        warnings
            .push("reasoning global was requested but no current agent was selected".to_string());
        return Ok(());
    };
    let mut store = read_agent_overrides_store(harness_home)?;
    let override_entry = store.agents.entry(agent_id.clone()).or_default();
    override_entry.reasoning_preference = Some(preference.clone());
    override_entry.backend_reasoning_policy = policy.cloned();
    override_entry.thinking_level = preference.explicit_effort().map(str::to_string);
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
        AgentProgressContext, AgentProgressDeliveryPlanOptions, AgentProgressKind,
        AgentProgressStatus, ChannelOutboundMessageKind, ChannelStatusSnapshot, ChannelStepAction,
        CompletedTurnWorkingSetSnapshotOptions, ContextVirtualSessionRecord,
        append_agent_progress_event, plan_agent_progress_delivery,
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
    fn new_session_command_supersedes_previous_progress_surfaces() {
        let root = temp_root("new_session_command_supersedes_previous_progress_surfaces");
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        let progress_context = AgentProgressContext {
            queue_id: "turn:old".to_string(),
            agent_id: Some("main".to_string()),
            account_id: None,
            thread_id: None,
            session_key: "telegram:dm:user:main".to_string(),
            platform: "telegram".to_string(),
            channel_id: "dm".to_string(),
            user_id: "user".to_string(),
        };
        append_agent_progress_event(
            &harness_home,
            &crate::AgentProgressEvent::new(
                &progress_context,
                AgentProgressKind::Todo,
                "todo",
                "old session task",
                AgentProgressStatus::Started,
                900,
            ),
        )
        .unwrap();
        let initial = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 950,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert_eq!(initial.pending.len(), 2);

        let step = command_step(ChannelCommandEffect::StartNewSession {
            topic: Some("fresh task".to_string()),
            new_session_key: "telegram:dm:user:main:session-new".to_string(),
        });
        let report = apply_channel_command_step(
            &step,
            ChannelCommandApplyOptions {
                harness_home: harness_home.clone(),
                now_ms: 1000,
            },
        )
        .unwrap();
        assert!(report.warnings.is_empty());
        assert!(crate::agent_progress_session_supersede_receipts_file(&harness_home).is_file());

        append_agent_progress_event(
            &harness_home,
            &crate::AgentProgressEvent::new(
                &progress_context,
                AgentProgressKind::ReadFile,
                "read",
                "old session late event",
                AgentProgressStatus::Started,
                1100,
            ),
        )
        .unwrap();
        let old_plan = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 1200,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert!(old_plan.pending.is_empty());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn applies_unknown_command_as_structured_receipt() {
        let root = temp_root("applies_unknown_command_as_structured_receipt");
        let harness_home = root.join(".agent-harness");
        let step = command_step(ChannelCommandEffect::UnknownCommand {
            name: "unknown".to_string(),
            rest: Some("value".to_string()),
            detail: "unsupported channel command; no model turn was started".to_string(),
        });

        let report = apply_channel_command_step(
            &step,
            ChannelCommandApplyOptions {
                harness_home,
                now_ms: 1000,
            },
        )
        .unwrap();

        assert_eq!(
            report.receipt.status,
            ChannelCommandApplyReceiptStatus::Applied
        );
        assert_eq!(report.receipt.command, Some("unknown-command"));
        assert!(matches!(
            report.receipt.effect,
            Some(ChannelCommandEffect::UnknownCommand {
                ref name,
                ref rest,
                ..
            }) if name == "unknown" && rest.as_deref() == Some("value")
        ));
        let event = report.event.as_ref().expect("command event");
        assert_eq!(event.command, "unknown-command");
        assert!(matches!(
            event.effect,
            ChannelCommandEffect::UnknownCommand {
                ref name,
                ref rest,
                ..
            } if name == "unknown" && rest.as_deref() == Some("value")
        ));
        let state = report.state.expect("channel state");
        assert_eq!(state.last_command.as_deref(), Some("unknown-command"));
        let receipts = fs::read_to_string(report.receipts_file).unwrap();
        assert!(receipts.contains("\"command\":\"unknown-command\""));
        assert!(receipts.contains("\"kind\":\"unknownCommand\""));

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
    fn stop_command_writes_scoped_stop_markers_for_active_session_siblings() {
        let root = temp_root("stop_command_writes_scoped_stop_markers_for_active_session_siblings");
        let harness_home = root.join(".agent-harness");
        let pending_file = harness_home
            .join("state")
            .join("runtime-queue")
            .join("pending.jsonl");
        fs::create_dir_all(pending_file.parent().unwrap()).unwrap();
        let queue_a = "turn:1000:telegram:dm:user:main:aaaa";
        let queue_b = "turn:1001:telegram:dm:user:main:bbbb";
        let other_agent_queue = "turn:1002:telegram:dm:user:side:cccc";
        for (queue_id, agent_id, session_key) in [
            (queue_a, "main", "telegram:dm:user:main"),
            (queue_b, "main", "telegram:dm:user:main"),
            (other_agent_queue, "side", "telegram:dm:user:side"),
        ] {
            crate::append_jsonl_value(
                &pending_file,
                &serde_json::json!({
                    "schema": "agent-harness.runtime-queue-item.v1",
                    "queueId": queue_id,
                    "status": "queued",
                    "runtimeClass": "interactive",
                    "origin": "channel",
                    "createdAtMs": 1000,
                    "agentId": agent_id,
                    "sessionKey": session_key,
                    "platform": "telegram",
                    "channelId": "dm",
                    "userId": "user",
                    "messageText": "work",
                    "source": {
                        "kind": "channel",
                        "sourceHome": ".openclaw",
                        "sourceWorkspace": ".openclaw/workspace"
                    },
                    "promptFilesPresent": 0,
                    "promptFilesTotal": 0,
                    "selectedSkillIds": [],
                    "plannedTranscriptFile": "transcript.jsonl",
                    "plannedTrajectoryFile": "trajectory.jsonl"
                }),
            )
            .unwrap();
        }
        let step = command_step_with_session(
            "telegram:dm:user:main",
            ChannelCommandEffect::StopCurrentRun {
                reason: Some("operator stop siblings".to_string()),
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

        let cancel_dir = harness_home
            .join("state")
            .join("runtime-queue")
            .join("cancel");
        assert!(
            cancel_dir
                .join("queue-turn_1000_telegram_dm_user_main_aaaa.stop")
                .is_file()
        );
        assert!(
            cancel_dir
                .join("queue-turn_1001_telegram_dm_user_main_bbbb.stop")
                .is_file()
        );
        assert!(
            !cancel_dir
                .join("queue-turn_1002_telegram_dm_user_side_cccc.stop")
                .is_file()
        );

        let receipt_text = fs::read_to_string(report.receipts_file).unwrap();
        assert!(receipt_text.contains("\"stopScope\":\"active-session-siblings\""));
        assert!(receipt_text.contains("\"stopAppliedQueueIds\""));
        assert!(receipt_text.contains(queue_a));
        assert!(receipt_text.contains(queue_b));
        assert!(receipt_text.contains("\"duplicateSiblingCount\":1"));
        assert!(!receipt_text.contains(other_agent_queue));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn stop_cancels_all_lane_siblings() {
        stop_command_writes_scoped_stop_markers_for_active_session_siblings();
    }

    #[test]
    fn stop_receipt_lists_applied_queue_ids_and_scope() {
        stop_command_writes_scoped_stop_markers_for_active_session_siblings();
    }

    #[test]
    fn queued_sibling_started_after_60s_still_stopped() {
        let root = temp_root("queued_sibling_started_after_60s_still_stopped");
        let harness_home = root.join(".agent-harness");
        let pending_file = harness_home
            .join("state")
            .join("runtime-queue")
            .join("pending.jsonl");
        fs::create_dir_all(pending_file.parent().unwrap()).unwrap();
        let queue_id = "turn:1000:telegram:dm:user:main:older";
        crate::append_jsonl_value(
            &pending_file,
            &serde_json::json!({
                "schema": "agent-harness.runtime-queue-item.v1",
                "queueId": queue_id,
                "status": "queued",
                "runtimeClass": "interactive",
                "origin": "channel",
                "createdAtMs": 1000,
                "agentId": "main",
                "sessionKey": "telegram:dm:user:main",
                "platform": "telegram",
                "channelId": "dm",
                "userId": "user",
                "messageText": "work",
                "source": {
                    "kind": "channel",
                    "sourceHome": ".openclaw",
                    "sourceWorkspace": ".openclaw/workspace"
                },
                "promptFilesPresent": 0,
                "promptFilesTotal": 0,
                "selectedSkillIds": [],
                "plannedTranscriptFile": "transcript.jsonl",
                "plannedTrajectoryFile": "trajectory.jsonl"
            }),
        )
        .unwrap();
        let step = command_step_with_session(
            "telegram:dm:user:main",
            ChannelCommandEffect::StopCurrentRun {
                reason: Some("operator stop older sibling".to_string()),
            },
        );

        let report = apply_channel_command_step(
            &step,
            ChannelCommandApplyOptions {
                harness_home: harness_home.clone(),
                now_ms: 70_000,
            },
        )
        .unwrap();

        let cancel_file = harness_home
            .join("state")
            .join("runtime-queue")
            .join("cancel")
            .join("queue-turn_1000_telegram_dm_user_main_older.stop");
        assert!(cancel_file.is_file());
        let receipt_text = fs::read_to_string(report.receipts_file).unwrap();
        assert!(receipt_text.contains("\"stopScope\":\"active-session-siblings\""));
        assert!(receipt_text.contains(queue_id));

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
    fn c3_per_agent_same_channel_user_command_defaults_are_not_collapsed() {
        let root = temp_root("c3_per_agent_same_channel_user_command_defaults_are_not_collapsed");
        let harness_home = root.join(".agent-harness");
        let main_step = command_step_with_session(
            "telegram:dm:user:main",
            ChannelCommandEffect::SwitchModel {
                agent_id: Some("main".to_string()),
                provider: "openrouter".to_string(),
                model: "anthropic/claude-sonnet-4".to_string(),
                global: true,
                current_provider: Some("openai".to_string()),
                current_model: Some("gpt-5".to_string()),
                provider_known: true,
                model_known: true,
            },
        );
        let main_report = apply_channel_command_step(
            &main_step,
            ChannelCommandApplyOptions {
                harness_home: harness_home.clone(),
                now_ms: 2100,
            },
        )
        .unwrap();

        let side_step = command_step_with_session(
            "telegram:dm:user:side",
            ChannelCommandEffect::SwitchModel {
                agent_id: Some("side".to_string()),
                provider: "openai".to_string(),
                model: "gpt-5-mini".to_string(),
                global: true,
                current_provider: Some("openai".to_string()),
                current_model: Some("gpt-5".to_string()),
                provider_known: true,
                model_known: true,
            },
        );
        let side_report = apply_channel_command_step(
            &side_step,
            ChannelCommandApplyOptions {
                harness_home: harness_home.clone(),
                now_ms: 2101,
            },
        )
        .unwrap();

        assert_eq!(main_report.receipt.session_key, "telegram:dm:user:main");
        assert_eq!(side_report.receipt.command, Some("model"));
        assert!(matches!(
            side_report.receipt.effect,
            Some(ChannelCommandEffect::SwitchModel {
                agent_id: Some(ref agent_id),
                ..
            }) if agent_id == "side"
        ));
        let main_override = read_agent_override(&harness_home, "main").unwrap().unwrap();
        let side_override = read_agent_override(&harness_home, "side").unwrap().unwrap();
        assert_eq!(main_override.provider.as_deref(), Some("openrouter"));
        assert_eq!(
            main_override.model.as_deref(),
            Some("anthropic/claude-sonnet-4")
        );
        assert_eq!(side_override.provider.as_deref(), Some("openai"));
        assert_eq!(side_override.model.as_deref(), Some("gpt-5-mini"));
        assert_ne!(main_override.model, side_override.model);

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

    #[test]
    fn per_agent_same_channel_user_command_defaults_are_not_collapsed() {
        c3_per_agent_same_channel_user_command_defaults_are_not_collapsed();
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
