use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use ring::digest;
use serde::{Deserialize, Serialize};

use crate::channel_session_transition::{
    ChannelGoalBoundaryV1, ChannelSessionTransitionClosureMaterialV1,
    ChannelSessionTransitionCommandV1, ChannelSessionTransitionIntentV1,
    ChannelSessionTransitionPhaseV1, PrepareChannelSessionTransitionOptionsV1,
    channel_session_transition_boundary_is_ready, channel_session_transition_closure_material,
    pending_channel_session_transition_intents, prepare_channel_session_transition,
    record_channel_session_transition_phase,
};
use crate::context_rollover::{
    VirtualSessionTaskBoundaryCloseV2Options, close_virtual_session_for_task_boundary_for_lane,
};
use crate::execution_mode::{ExecutionModePolicyV1, ExecutionModePreference};
use crate::runtime_pending_index::read_queued_pending_values_from_index;
use crate::{
    ChannelCommandEffect, ChannelOutboundMessage, ChannelStep,
    VirtualSessionTaskBoundaryCloseOptions,
    admission::{ScopedStopOptions, ScopedStopTarget, record_scoped_stop},
    backend_reasoning::{BackendReasoningPolicyV1, BackendReasoningSource, ReasoningPreference},
    close_virtual_session_for_task_boundary,
    codex_runtime::{
        CodexGoalClosureExecutionOptions, CodexTurnSteerRequestOptions, execute_codex_goal_closure,
        queue_codex_turn_steer_request,
    },
    progress::{AgentProgressSessionSupersedeOptions, supersede_agent_progress_session_surfaces},
    write_json_atomic,
};

const CHANNEL_COMMAND_APPLY_SCHEMA: &str = "agent-harness.channel-command-apply.v1";
const CHANNEL_STATE_SCHEMA: &str = "agent-harness.channel-session-state.v1";
pub const CHANNEL_SESSION_STATE_V2_SCHEMA: &str = "agent-harness.channel-session-state.v2";
const CHANNEL_COMMAND_EVENT_SCHEMA: &str = "agent-harness.channel-command-event.v1";
const AGENT_OVERRIDES_SCHEMA: &str = "agent-harness.agent-overrides.v1";
const CHANNEL_EXECUTION_MODE_SCHEMA: &str = "agent-harness.channel-execution-mode.v1";
const AGENT_EXECUTION_MODE_OVERRIDES_SCHEMA: &str =
    "agent-harness.agent-execution-mode-overrides.v1";
const RUNTIME_CANCEL_REQUEST_SCHEMA: &str = "agent-harness.runtime-cancel-request.v1";
const DEFAULT_CHANNEL_STATE_ACCOUNT_ID: &str = "default";
const MAX_CHANNEL_STATE_LANE_AXIS_BYTES: usize = 4 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelCommandApplyOptions {
    pub harness_home: PathBuf,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcileChannelSessionTransitionsOptionsV1 {
    pub harness_home: PathBuf,
    pub executable: PathBuf,
    pub arguments: Vec<String>,
    pub working_directory: PathBuf,
    pub codex_home: Option<PathBuf>,
    pub timeout_ms: u64,
    pub max_transitions: usize,
    pub now_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ChannelSessionTransitionReconcileStatusV1 {
    BoundaryCommitted,
    RetryPending,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelSessionTransitionReconcileItemV1 {
    pub transition_id: String,
    pub command: ChannelSessionTransitionCommandV1,
    pub status: ChannelSessionTransitionReconcileStatusV1,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelSessionTransitionReconcileReportV1 {
    pub scanned: usize,
    pub committed: usize,
    pub retry_pending: usize,
    pub items: Vec<ChannelSessionTransitionReconcileItemV1>,
}

/// Exact durable ownership boundary for v2 channel session state.
///
/// All axes are required after construction. A missing or blank provider
/// account is normalized to the explicit `default` account; the other axes
/// reject blank or control-character values. The fields are intentionally
/// private so callers cannot create an unnormalized or partial lane.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelStateLane {
    platform: String,
    account_id: String,
    channel_id: String,
    user_id: String,
    agent_id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ChannelStateLaneWire {
    platform: String,
    #[serde(alias = "account_id")]
    account_id: String,
    #[serde(alias = "channel_id")]
    channel_id: String,
    #[serde(alias = "user_id")]
    user_id: String,
    #[serde(alias = "agent_id")]
    agent_id: String,
}

impl<'de> Deserialize<'de> for ChannelStateLane {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = ChannelStateLaneWire::deserialize(deserializer)?;
        Self::new(
            &wire.platform,
            Some(&wire.account_id),
            &wire.channel_id,
            &wire.user_id,
            &wire.agent_id,
        )
        .map_err(serde::de::Error::custom)
    }
}

impl ChannelStateLane {
    pub fn new(
        platform: &str,
        account_id: Option<&str>,
        channel_id: &str,
        user_id: &str,
        agent_id: &str,
    ) -> io::Result<Self> {
        Ok(Self {
            platform: normalize_channel_state_lane_platform(platform)?,
            account_id: normalize_channel_state_lane_account_id(account_id)?,
            channel_id: normalize_required_channel_state_lane_axis(channel_id, "channelId", false)?,
            user_id: normalize_required_channel_state_lane_axis(user_id, "userId", false)?,
            agent_id: normalize_required_channel_state_lane_axis(agent_id, "agentId", false)?,
        })
    }

    pub fn platform(&self) -> &str {
        &self.platform
    }

    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    pub fn channel_id(&self) -> &str {
        &self.channel_id
    }

    pub fn user_id(&self) -> &str {
        &self.user_id
    }

    pub fn agent_id(&self) -> &str {
        &self.agent_id
    }

    pub fn exact_lane_digest(&self) -> String {
        format!("sha256:{}", channel_state_lane_v2_key(self))
    }

    fn has_default_account(&self) -> bool {
        self.account_id == DEFAULT_CHANNEL_STATE_ACCOUNT_ID
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ChannelSessionStateV2MigrationStatus {
    ExistingV2,
    MigratedLegacyDefaultAccount,
    LegacyStateMissing,
    RejectedLegacyUnknownAccount,
    RejectedLegacyMissingAgent,
    RejectedLegacyAgentMismatch,
    RejectedLegacyIdentityMismatch,
}

/// Result of an explicit, fail-closed v1-to-v2 state migration attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelSessionStateV2MigrationReport {
    pub status: ChannelSessionStateV2MigrationStatus,
    pub state_file: PathBuf,
    pub legacy_state_file: PathBuf,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<ChannelSessionState>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_id: Option<String>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_transition_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_transition_phase: Option<ChannelSessionTransitionPhaseV1>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_id: Option<String>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_id: Option<String>,
    pub channel_id: String,
    pub user_id: String,
    pub active_session_key: String,
    pub agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_revision: Option<String>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    account_id: Option<String>,
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
    let mut warnings = step.warnings.clone();
    let exact_lane = match step.account_id.as_deref() {
        Some(account_id) => {
            let agent_id = step_agent_id(step).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "account-scoped channel command has no exact selected agent",
                )
            })?;
            Some(ChannelStateLane::new(
                &step.platform,
                Some(account_id),
                &step.channel_id,
                &step.user_id,
                &agent_id,
            )?)
        }
        None => None,
    };
    let state_file = exact_lane.as_ref().map_or_else(
        || {
            channel_session_state_file(
                &options.harness_home,
                &step.platform,
                &step.channel_id,
                &step.user_id,
            )
        },
        |lane| channel_session_state_v2_file(&options.harness_home, lane),
    );
    let state_dir = state_file.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("channel state file has no parent: {}", state_file.display()),
        )
    })?;
    fs::create_dir_all(&state_dir)?;
    let events_file = state_dir.join("events.jsonl");
    let receipts_file = options
        .harness_home
        .join("state")
        .join("channels")
        .join("command-apply-receipts.jsonl");

    let Some(effect) = step.command_effect.clone() else {
        let receipt = ChannelCommandApplyReceipt {
            status: ChannelCommandApplyReceiptStatus::SkippedNotCommand,
            session_key: step.session_key.clone(),
            account_id: exact_lane
                .as_ref()
                .map(|lane| lane.account_id().to_string()),
            state_file: state_file.clone(),
            events_file: events_file.clone(),
            reason: "channel step has no command effect".to_string(),
            command: None,
            effect: None,
            stop_scope: None,
            stop_applied_queue_ids: Vec::new(),
            duplicate_sibling_count: None,
            session_transition_id: None,
            session_transition_phase: None,
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

    let mut state = match exact_lane.as_ref() {
        Some(lane) => read_channel_session_state_v2(&options.harness_home, lane)?
            .unwrap_or_else(|| new_state(step, Some(lane))),
        None => read_channel_state(&state_file)?.unwrap_or_else(|| new_state(step, None)),
    };
    if let Some(lane) = exact_lane.as_ref() {
        bind_channel_session_state_to_lane_v2(&mut state, lane);
    }
    let applied_effect = apply_effect(
        &mut state,
        &effect,
        &options.harness_home,
        options.now_ms,
        &mut warnings,
    )?;
    if let Some(lane) = exact_lane.as_ref() {
        bind_channel_session_state_to_lane_v2(&mut state, lane);
    }
    let stop_applied_queue_ids = applied_effect.stop_applied_queue_ids.clone();
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
        account_id: exact_lane
            .as_ref()
            .map(|lane| lane.account_id().to_string()),
        channel_id: step.channel_id.clone(),
        user_id: step.user_id.clone(),
        session_key: step.session_key.clone(),
        active_session_key: state.active_session_key.clone(),
        command: command_name(&effect),
        effect,
    };

    if let Some(lane) = exact_lane.as_ref() {
        write_channel_session_state_v2(&options.harness_home, lane, &state)?;
    } else {
        write_json_atomic(&state_file, &state)?;
    }
    let session_transition_phase = if let Some(intent) = applied_effect.transition.as_ref() {
        let phase = if applied_effect.boundary_committed {
            ChannelSessionTransitionPhaseV1::BoundaryCommitted
        } else if intent.goal_boundary == ChannelGoalBoundaryV1::Ambiguous {
            ChannelSessionTransitionPhaseV1::RetryPending
        } else {
            ChannelSessionTransitionPhaseV1::GoalClosurePending
        };
        if applied_effect.boundary_committed {
            record_channel_session_transition_phase(
                &options.harness_home,
                intent,
                phase,
                "channel session state write committed",
                options.now_ms,
            )?;
        }
        Some(phase)
    } else {
        None
    };
    append_json_line(&events_file, &event)?;

    let receipt = ChannelCommandApplyReceipt {
        status: ChannelCommandApplyReceiptStatus::Applied,
        session_key: state.active_session_key.clone(),
        account_id: exact_lane
            .as_ref()
            .map(|lane| lane.account_id().to_string()),
        state_file: state_file.clone(),
        events_file: events_file.clone(),
        reason: "channel command state updated".to_string(),
        command: Some(event.command),
        effect: Some(event.effect.clone()),
        stop_scope,
        stop_applied_queue_ids,
        duplicate_sibling_count,
        session_transition_id: applied_effect
            .transition
            .as_ref()
            .map(|intent| intent.transition_id.clone()),
        session_transition_phase,
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

pub fn reconcile_channel_session_transitions(
    options: ReconcileChannelSessionTransitionsOptionsV1,
) -> io::Result<ChannelSessionTransitionReconcileReportV1> {
    let execution = options.clone();
    reconcile_channel_session_transitions_with(options, move |material| {
        execute_codex_goal_closure(CodexGoalClosureExecutionOptions {
            harness_home: execution.harness_home.clone(),
            intent: material.goal_closure_intent.clone(),
            resolution: material.resolution.clone(),
            executable: execution.executable.clone(),
            arguments: execution.arguments.clone(),
            working_directory: execution.working_directory.clone(),
            codex_home: execution.codex_home.clone(),
            timeout_ms: execution.timeout_ms,
        })?;
        Ok(())
    })
}

fn reconcile_channel_session_transitions_with<F>(
    options: ReconcileChannelSessionTransitionsOptionsV1,
    mut execute_closure: F,
) -> io::Result<ChannelSessionTransitionReconcileReportV1>
where
    F: FnMut(&ChannelSessionTransitionClosureMaterialV1) -> io::Result<()>,
{
    if options.timeout_ms == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "session transition closure timeout must be positive",
        ));
    }
    let pending =
        pending_channel_session_transition_intents(&options.harness_home, options.max_transitions)?;
    let mut report = ChannelSessionTransitionReconcileReportV1 {
        scanned: pending.len(),
        committed: 0,
        retry_pending: 0,
        items: Vec::with_capacity(pending.len()),
    };
    for intent in pending {
        let result =
            reconcile_one_channel_session_transition(&options, &intent, &mut execute_closure);
        match result {
            Ok(()) => {
                report.committed += 1;
                report.items.push(ChannelSessionTransitionReconcileItemV1 {
                    transition_id: intent.transition_id,
                    command: intent.command,
                    status: ChannelSessionTransitionReconcileStatusV1::BoundaryCommitted,
                    reason: None,
                });
            }
            Err(error) => {
                let reason = format!(
                    "session transition reconciliation is retry-pending after {} failure",
                    error.kind()
                );
                record_channel_session_transition_phase(
                    &options.harness_home,
                    &intent,
                    ChannelSessionTransitionPhaseV1::RetryPending,
                    &reason,
                    options.now_ms,
                )?;
                report.retry_pending += 1;
                report.items.push(ChannelSessionTransitionReconcileItemV1 {
                    transition_id: intent.transition_id,
                    command: intent.command,
                    status: ChannelSessionTransitionReconcileStatusV1::RetryPending,
                    reason: Some(reason),
                });
            }
        }
    }
    Ok(report)
}

fn reconcile_one_channel_session_transition<F>(
    options: &ReconcileChannelSessionTransitionsOptionsV1,
    intent: &ChannelSessionTransitionIntentV1,
    execute_closure: &mut F,
) -> io::Result<()>
where
    F: FnMut(&ChannelSessionTransitionClosureMaterialV1) -> io::Result<()>,
{
    let (lane, mut state) = exact_state_for_transition(&options.harness_home, intent)?;
    if state.active_session_key == intent.old_session_key {
        fence_channel_external_effects(
            &options.harness_home,
            &state,
            "session transition recovery fenced pending connector approval",
            &mut Vec::new(),
        )?;
        write_runtime_cancel_request(
            &options.harness_home,
            &state,
            intent.reason.clone(),
            options.now_ms,
        )?;
        let _ = record_active_session_sibling_stops(
            &options.harness_home,
            &state,
            intent.reason.clone(),
            options.now_ms,
        )?;
    }

    if !channel_session_transition_boundary_is_ready(&options.harness_home, intent)? {
        let material = channel_session_transition_closure_material(&options.harness_home, intent)?
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "session transition has no exact goal closure authority",
                )
            })?;
        execute_closure(&material)?;
    }
    if !channel_session_transition_boundary_is_ready(&options.harness_home, intent)? {
        return Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "goal closure execution did not produce durable completed evidence",
        ));
    }

    match intent.command {
        ChannelSessionTransitionCommandV1::Stop => {
            if state.active_session_key != intent.old_session_key {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "stop transition old session is no longer authoritative",
                ));
            }
        }
        ChannelSessionTransitionCommandV1::New => {
            let proposed_frozen = intent.frozen_new_session_key.as_deref().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "durable /new transition has no frozen target session",
                )
            })?;
            let frozen = canonical_session_key_for_lane(proposed_frozen, &lane)?;
            if state.active_session_key != frozen {
                if state.active_session_key != intent.old_session_key {
                    return Err(io::Error::new(
                        io::ErrorKind::WouldBlock,
                        "channel state moved to a session other than the frozen transition target",
                    ));
                }
                commit_new_session_transition_state(
                    &options.harness_home,
                    &lane,
                    &mut state,
                    intent,
                    options.now_ms,
                )?;
                write_channel_session_state_v2(&options.harness_home, &lane, &state)?;
            }
        }
    }
    record_channel_session_transition_phase(
        &options.harness_home,
        intent,
        ChannelSessionTransitionPhaseV1::BoundaryCommitted,
        "autonomous session transition reconciliation committed exact-lane state",
        options.now_ms,
    )?;
    Ok(())
}

fn exact_state_for_transition(
    harness_home: &Path,
    intent: &ChannelSessionTransitionIntentV1,
) -> io::Result<(ChannelStateLane, ChannelSessionState)> {
    let key = intent
        .lane_digest
        .strip_prefix("sha256:")
        .filter(|value| {
            value.len() == 64
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "session transition lane digest is not a canonical v2 state key",
            )
        })?;
    let state_file = harness_home
        .join("state")
        .join("channels")
        .join("v2")
        .join(key)
        .join("state.json");
    let state = read_channel_state(&state_file)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::WouldBlock,
            "exact v2 channel state is unavailable for session transition recovery",
        )
    })?;
    let agent_id = state.agent_id.as_deref().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "v2 channel state has no exact agent identity",
        )
    })?;
    let lane = ChannelStateLane::new(
        &state.platform,
        state.account_id.as_deref(),
        &state.channel_id,
        &state.user_id,
        agent_id,
    )?;
    let frozen_new_session_key = intent
        .frozen_new_session_key
        .as_deref()
        .map(|session_key| canonical_session_key_for_lane(session_key, &lane))
        .transpose()?;
    if lane.exact_lane_digest() != intent.lane_digest
        || (state.active_session_key != intent.old_session_key
            && frozen_new_session_key.as_deref() != Some(state.active_session_key.as_str()))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "persisted channel state does not match the frozen transition authority",
        ));
    }
    validate_channel_session_state_v2_lane(&state, &lane)?;
    Ok((lane, state))
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

/// Returns the deterministic v2 state path for one exact provider/account/
/// channel/user/agent lane. The path is a SHA-256 digest of an unambiguous
/// canonical lane encoding, not a lossy sanitization of provider identifiers.
pub fn channel_session_state_v2_file(
    harness_home: impl AsRef<Path>,
    lane: &ChannelStateLane,
) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("channels")
        .join("v2")
        .join(channel_state_lane_v2_key(lane))
        .join("state.json")
}

/// Reads only the exact v2 lane state. This API never falls back to a legacy
/// state file; callers that deliberately want migration must invoke the
/// explicit migration API below.
pub fn read_channel_session_state_v2(
    harness_home: impl AsRef<Path>,
    lane: &ChannelStateLane,
) -> io::Result<Option<ChannelSessionState>> {
    let state_file = channel_session_state_v2_file(harness_home, lane);
    let Some(mut state) = read_channel_state(&state_file)? else {
        return Ok(None);
    };
    validate_channel_session_state_v2_lane(&state, lane)?;
    let parsed = crate::channel_session_key::CanonicalChannelSessionKey::parse_for_lane(
        &state.active_session_key,
        lane.platform(),
        lane.account_id(),
        lane.channel_id(),
        lane.user_id(),
        lane.agent_id(),
    )
    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    if parsed.normalization()
        == crate::channel_session_key::SessionKeyNormalization::LegacyDuplicateAccountCollapsed
    {
        state.active_session_key = parsed.canonical_string();
        write_json_atomic(&state_file, &state)?;
    }
    Ok(Some(state))
}

/// Binds a state object to an exact v2 lane before it is persisted.
///
/// This is deliberately explicit because v1 callers have no account axis and
/// may leave `agent_id` unset. It preserves all non-identity fields, including
/// `config_revision`, so callers can stamp a current configuration revision
/// without losing it during an upgrade.
pub fn bind_channel_session_state_to_lane_v2(
    state: &mut ChannelSessionState,
    lane: &ChannelStateLane,
) {
    state.schema = CHANNEL_SESSION_STATE_V2_SCHEMA.to_string();
    state.platform = lane.platform.clone();
    state.account_id = Some(lane.account_id.clone());
    state.channel_id = lane.channel_id.clone();
    state.user_id = lane.user_id.clone();
    state.agent_id = Some(lane.agent_id.clone());
    if let Ok(session_key) = crate::channel_session_key::CanonicalChannelSessionKey::bind_for_lane(
        &state.active_session_key,
        lane.platform(),
        lane.account_id(),
        lane.channel_id(),
        lane.user_id(),
        lane.agent_id(),
    ) {
        state.active_session_key = session_key.canonical_string();
    }
}

/// Writes one state object after checking that its persisted identity exactly
/// matches the supplied v2 lane. An existing file is read first so a corrupted
/// or hash-colliding lane cannot be overwritten as another lane's state.
pub fn write_channel_session_state_v2(
    harness_home: impl AsRef<Path>,
    lane: &ChannelStateLane,
    state: &ChannelSessionState,
) -> io::Result<PathBuf> {
    validate_channel_session_state_v2_lane(state, lane)?;
    let parsed = crate::channel_session_key::CanonicalChannelSessionKey::parse_for_lane(
        &state.active_session_key,
        lane.platform(),
        lane.account_id(),
        lane.channel_id(),
        lane.user_id(),
        lane.agent_id(),
    )
    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    if parsed.normalization()
        != crate::channel_session_key::SessionKeyNormalization::AlreadyCanonical
        || parsed.canonical_string() != state.active_session_key
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "new channel session state writes require a canonical exact-lane session key",
        ));
    }
    let harness_home = harness_home.as_ref();
    let state_file = channel_session_state_v2_file(harness_home, lane);
    if state_file.is_file() {
        let _ = read_channel_session_state_v2(harness_home, lane)?;
    }
    write_json_atomic(&state_file, state)?;
    Ok(state_file)
}

/// Explicitly migrates a legacy state file only when it is provably the same
/// default-account lane. Unknown/non-default accounts and missing or mismatched
/// agents remain unavailable rather than becoming wildcard state.
pub fn migrate_legacy_channel_session_state_to_v2(
    harness_home: impl AsRef<Path>,
    lane: &ChannelStateLane,
) -> io::Result<ChannelSessionStateV2MigrationReport> {
    let harness_home = harness_home.as_ref();
    let state_file = channel_session_state_v2_file(harness_home, lane);
    let legacy_state_file = channel_session_state_file(
        harness_home,
        lane.platform(),
        lane.channel_id(),
        lane.user_id(),
    );

    if let Some(state) = read_channel_session_state_v2(harness_home, lane)? {
        return Ok(ChannelSessionStateV2MigrationReport {
            status: ChannelSessionStateV2MigrationStatus::ExistingV2,
            state_file,
            legacy_state_file,
            state: Some(state),
        });
    }

    let Some(mut state) = read_channel_state(&legacy_state_file)? else {
        return Ok(ChannelSessionStateV2MigrationReport {
            status: ChannelSessionStateV2MigrationStatus::LegacyStateMissing,
            state_file,
            legacy_state_file,
            state: None,
        });
    };

    if !lane.has_default_account() || !legacy_state_has_default_account(&state) {
        return Ok(ChannelSessionStateV2MigrationReport {
            status: ChannelSessionStateV2MigrationStatus::RejectedLegacyUnknownAccount,
            state_file,
            legacy_state_file,
            state: None,
        });
    }
    if !legacy_state_matches_non_agent_lane_axes(&state, lane) {
        return Ok(ChannelSessionStateV2MigrationReport {
            status: ChannelSessionStateV2MigrationStatus::RejectedLegacyIdentityMismatch,
            state_file,
            legacy_state_file,
            state: None,
        });
    }
    let Some(agent_id) = state.agent_id.as_deref() else {
        return Ok(ChannelSessionStateV2MigrationReport {
            status: ChannelSessionStateV2MigrationStatus::RejectedLegacyMissingAgent,
            state_file,
            legacy_state_file,
            state: None,
        });
    };
    let Ok(agent_id) = normalize_required_channel_state_lane_axis(agent_id, "agentId", false)
    else {
        return Ok(ChannelSessionStateV2MigrationReport {
            status: ChannelSessionStateV2MigrationStatus::RejectedLegacyAgentMismatch,
            state_file,
            legacy_state_file,
            state: None,
        });
    };
    if agent_id != lane.agent_id() {
        return Ok(ChannelSessionStateV2MigrationReport {
            status: ChannelSessionStateV2MigrationStatus::RejectedLegacyAgentMismatch,
            state_file,
            legacy_state_file,
            state: None,
        });
    }

    bind_channel_session_state_to_lane_v2(&mut state, lane);
    write_channel_session_state_v2(harness_home, lane, &state)?;
    Ok(ChannelSessionStateV2MigrationReport {
        status: ChannelSessionStateV2MigrationStatus::MigratedLegacyDefaultAccount,
        state_file,
        legacy_state_file,
        state: Some(state),
    })
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
            account_id: None,
            channel_id: "channel-1".to_string(),
            user_id: "user-1".to_string(),
            active_session_key: "session-1".to_string(),
            agent_id: Some("main".to_string()),
            config_revision: None,
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

fn new_state(step: &ChannelStep, exact_lane: Option<&ChannelStateLane>) -> ChannelSessionState {
    let agent_id = exact_lane
        .map(|lane| lane.agent_id().to_string())
        .or_else(|| step_agent_id(step));
    ChannelSessionState {
        schema: exact_lane.map_or_else(
            || CHANNEL_STATE_SCHEMA.to_string(),
            |_| CHANNEL_SESSION_STATE_V2_SCHEMA.to_string(),
        ),
        platform: step.platform.clone(),
        account_id: exact_lane.map(|lane| lane.account_id().to_string()),
        channel_id: step.channel_id.clone(),
        user_id: step.user_id.clone(),
        active_session_key: step.session_key.clone(),
        agent_id,
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
        updated_at_ms: 0,
    }
}

fn step_agent_id(step: &ChannelStep) -> Option<String> {
    step.agent_turn
        .as_ref()
        .map(|dispatch| dispatch.agent_id.clone())
        .or_else(|| active_session_key_agent_segment(&step.session_key))
}

struct AppliedChannelEffect {
    stop_applied_queue_ids: Vec<String>,
    transition: Option<ChannelSessionTransitionIntentV1>,
    boundary_committed: bool,
}

fn apply_effect(
    state: &mut ChannelSessionState,
    effect: &ChannelCommandEffect,
    harness_home: &Path,
    now_ms: i64,
    warnings: &mut Vec<String>,
) -> io::Result<AppliedChannelEffect> {
    state.last_command = Some(command_name(effect).to_string());
    state.updated_at_ms = now_ms;
    let mut stop_applied_queue_ids = Vec::new();
    let mut transition = None;
    let mut boundary_committed = false;

    match effect {
        ChannelCommandEffect::StartNewSession {
            topic,
            new_session_key,
        } => {
            let lane_digest = channel_state_boundary_lane_digest(state)?;
            let prepared =
                prepare_channel_session_transition(PrepareChannelSessionTransitionOptionsV1 {
                    harness_home: harness_home.to_path_buf(),
                    command: ChannelSessionTransitionCommandV1::New,
                    lane_digest,
                    old_session_key: state.active_session_key.clone(),
                    proposed_new_session_key: Some(new_session_key.clone()),
                    topic: topic.clone(),
                    reason: Some("channel command /new".to_string()),
                    now_ms,
                })?;
            let boundary_ready = prepared.boundary_ready;
            let transition_intent = prepared.intent;
            fence_channel_external_effects(
                harness_home,
                state,
                "new session superseded pending connector approval",
                warnings,
            )?;
            let new_boundary_reason = Some("new session boundary requested".to_string());
            write_runtime_cancel_request(harness_home, state, new_boundary_reason.clone(), now_ms)?;
            stop_applied_queue_ids = record_active_session_sibling_stops(
                harness_home,
                state,
                new_boundary_reason,
                now_ms,
            )?;
            if !boundary_ready {
                if transition_intent.goal_boundary == ChannelGoalBoundaryV1::Ambiguous {
                    record_channel_session_transition_phase(
                        harness_home,
                        &transition_intent,
                        ChannelSessionTransitionPhaseV1::RetryPending,
                        "goal closure authority is ambiguous or unavailable",
                        now_ms,
                    )?;
                }
                return Ok(AppliedChannelEffect {
                    stop_applied_queue_ids,
                    transition: Some(transition_intent),
                    boundary_committed: false,
                });
            }
            let frozen_new_session_key = transition_intent
                .frozen_new_session_key
                .as_deref()
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "durable /new transition has no frozen target session",
                    )
                })?
                .to_string();
            let close_result = match (state.account_id.as_deref(), state.agent_id.as_deref()) {
                (Some(account_id), Some(agent_id)) => match ChannelStateLane::new(
                    &state.platform,
                    Some(account_id),
                    &state.channel_id,
                    &state.user_id,
                    agent_id,
                ) {
                    Ok(channel_lane) => close_virtual_session_for_task_boundary_for_lane(
                        VirtualSessionTaskBoundaryCloseV2Options {
                            harness_home: harness_home.to_path_buf(),
                            channel_lane,
                            previous_session_key: state.active_session_key.clone(),
                            ended_by: "channel-command:/new".to_string(),
                            now_ms,
                        },
                    ),
                    Err(error) => Err(error),
                },
                (Some(_), None) => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "account-scoped channel state has no agent for v2 virtual-session close",
                )),
                (None, _) => close_virtual_session_for_task_boundary(
                    VirtualSessionTaskBoundaryCloseOptions {
                        harness_home: harness_home.to_path_buf(),
                        previous_session_key: state.active_session_key.clone(),
                        ended_by: "channel-command:/new".to_string(),
                        now_ms,
                    },
                ),
            };
            if let Err(error) = close_result {
                record_channel_session_transition_phase(
                    harness_home,
                    &transition_intent,
                    ChannelSessionTransitionPhaseV1::RetryPending,
                    &format!("virtual session task boundary close failed: {error}"),
                    now_ms,
                )?;
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    format!(
                        "session transition remains pending because the old virtual session could not close: {error}"
                    ),
                ));
            }
            if state.active_session_key != frozen_new_session_key
                && let Err(error) = supersede_agent_progress_session_surfaces(
                    AgentProgressSessionSupersedeOptions {
                        harness_home: harness_home.to_path_buf(),
                        platform: state.platform.clone(),
                        account_id: state.account_id.clone(),
                        channel_id: state.channel_id.clone(),
                        user_id: state.user_id.clone(),
                        agent_id: state.agent_id.clone(),
                        session_key: state.active_session_key.clone(),
                        now_ms,
                    },
                )
            {
                record_channel_session_transition_phase(
                    harness_home,
                    &transition_intent,
                    ChannelSessionTransitionPhaseV1::RetryPending,
                    &format!("old-session progress supersession failed: {error}"),
                    now_ms,
                )?;
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    format!(
                        "session transition remains pending because old-session progress could not be superseded: {error}"
                    ),
                ));
            }
            if let Some(agent_id) = active_session_key_agent_segment(&frozen_new_session_key) {
                if state.agent_id.as_deref() != Some(agent_id.as_str()) {
                    state.provider = None;
                    state.model = None;
                    state.model_override = None;
                    state.model_override_provider = None;
                    state.model_override_model = None;
                }
                state.agent_id = Some(agent_id);
            }
            state.active_session_key = frozen_new_session_key;
            state.session_topic = transition_intent.topic.clone();
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
            transition = Some(transition_intent);
            boundary_committed = true;
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
                    return Ok(AppliedChannelEffect {
                        stop_applied_queue_ids,
                        transition: None,
                        boundary_committed: false,
                    });
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
            let prepared =
                prepare_channel_session_transition(PrepareChannelSessionTransitionOptionsV1 {
                    harness_home: harness_home.to_path_buf(),
                    command: ChannelSessionTransitionCommandV1::Stop,
                    lane_digest: channel_state_boundary_lane_digest(state)?,
                    old_session_key: state.active_session_key.clone(),
                    proposed_new_session_key: None,
                    topic: None,
                    reason: reason.clone(),
                    now_ms,
                })?;
            let boundary_ready = prepared.boundary_ready;
            let transition_intent = prepared.intent;
            fence_channel_external_effects(
                harness_home,
                state,
                "operator stop fenced pending connector approval",
                warnings,
            )?;
            state.stop_requested = true;
            state.stop_reason = reason.clone();
            write_runtime_cancel_request(harness_home, state, reason.clone(), now_ms)?;
            stop_applied_queue_ids =
                record_active_session_sibling_stops(harness_home, state, reason.clone(), now_ms)?;
            boundary_committed = boundary_ready;
            transition = Some(transition_intent);
        }
        ChannelCommandEffect::RestartChannel { .. } => {}
        ChannelCommandEffect::RestartGateway { .. } => {}
        ChannelCommandEffect::RestartStatus { .. } => {}
        ChannelCommandEffect::AddSteering { instruction } => {
            fence_channel_external_effects(
                harness_home,
                state,
                "newer steering input fenced pending connector approval",
                warnings,
            )?;
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
        // The durable effect ledger and exact-lane continuation queue are the
        // authorities for this command. Channel session state only records
        // the command name below; it must not mutate conversational controls.
        ChannelCommandEffect::ResolveExternalEffect { .. } => {}
        ChannelCommandEffect::UnknownCommand { .. } => {}
    }

    if state.model_override.is_some() && state.model_override_model.is_none() {
        warnings.push("model override target did not include a usable model name".to_string());
    }
    Ok(AppliedChannelEffect {
        stop_applied_queue_ids,
        transition,
        boundary_committed,
    })
}

fn fence_channel_external_effects(
    harness_home: &Path,
    state: &ChannelSessionState,
    reason: &str,
    warnings: &mut Vec<String>,
) -> io::Result<()> {
    let Some(agent_id) = state.agent_id.as_deref() else {
        return Ok(());
    };
    let lane = ChannelStateLane::new(
        &state.platform,
        state.account_id.as_deref(),
        &state.channel_id,
        &state.user_id,
        agent_id,
    )?;
    let fenced =
        crate::fence_external_effects_for_lane(harness_home, &lane.exact_lane_digest(), reason)?;
    if !fenced.is_empty() {
        warnings.push(format!(
            "fenced {} pending external effect(s) for the exact lane",
            fenced.len()
        ));
    }
    Ok(())
}

fn channel_state_boundary_lane_digest(state: &ChannelSessionState) -> io::Result<String> {
    let agent_id = state
        .agent_id
        .clone()
        .or_else(|| active_session_key_agent_segment(&state.active_session_key))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "channel session boundary has no exact agent identity",
            )
        })?;
    Ok(ChannelStateLane::new(
        &state.platform,
        state.account_id.as_deref(),
        &state.channel_id,
        &state.user_id,
        &agent_id,
    )?
    .exact_lane_digest())
}

fn commit_new_session_transition_state(
    harness_home: &Path,
    lane: &ChannelStateLane,
    state: &mut ChannelSessionState,
    intent: &ChannelSessionTransitionIntentV1,
    now_ms: i64,
) -> io::Result<()> {
    if intent.command != ChannelSessionTransitionCommandV1::New
        || intent.lane_digest != lane.exact_lane_digest()
        || state.active_session_key != intent.old_session_key
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "new-session commit does not match the frozen exact-lane transition authority",
        ));
    }
    let proposed_new_session_key = intent.frozen_new_session_key.as_deref().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "durable /new transition has no frozen target session",
        )
    })?;
    let frozen_new_session_key = canonical_session_key_for_lane(proposed_new_session_key, lane)?;
    close_virtual_session_for_task_boundary_for_lane(VirtualSessionTaskBoundaryCloseV2Options {
        harness_home: harness_home.to_path_buf(),
        channel_lane: lane.clone(),
        previous_session_key: state.active_session_key.clone(),
        ended_by: "new session boundary requested".to_string(),
        now_ms,
    })?;
    if state.active_session_key != frozen_new_session_key {
        supersede_agent_progress_session_surfaces(AgentProgressSessionSupersedeOptions {
            harness_home: harness_home.to_path_buf(),
            platform: state.platform.clone(),
            account_id: state.account_id.clone(),
            channel_id: state.channel_id.clone(),
            user_id: state.user_id.clone(),
            agent_id: state.agent_id.clone(),
            session_key: state.active_session_key.clone(),
            now_ms,
        })?;
    }
    if let Some(agent_id) = active_session_key_agent_segment(&frozen_new_session_key) {
        if state.agent_id.as_deref() != Some(agent_id.as_str()) {
            state.provider = None;
            state.model = None;
            state.model_override = None;
            state.model_override_provider = None;
            state.model_override_model = None;
            state.reasoning_preference = None;
            state.backend_reasoning_policy = None;
        }
        state.agent_id = Some(agent_id);
    }
    state.active_session_key = frozen_new_session_key;
    state.session_topic = intent.topic.clone();
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
    state.last_command = Some("new".to_string());
    state.updated_at_ms = now_ms;
    Ok(())
}

fn canonical_session_key_for_lane(
    session_key: &str,
    lane: &ChannelStateLane,
) -> io::Result<String> {
    crate::channel_session_key::CanonicalChannelSessionKey::bind_for_lane(
        session_key,
        lane.platform(),
        lane.account_id(),
        lane.channel_id(),
        lane.user_id(),
        lane.agent_id(),
    )
    .map(|parsed| parsed.canonical_string())
    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
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
    let queue_dir = harness_home.join("state").join("runtime-queue");
    let mut warnings = Vec::new();
    // This is an operator control path, but it still must not replay an
    // unbounded pending.jsonl just to stop siblings.  The source-authoritative
    // sidecar tails new bytes under the append lock and returns only active
    // rows; it fails closed rather than silently omitting runnable work.
    let values = read_queued_pending_values_from_index(&queue_dir, &mut warnings)?;
    if !warnings.is_empty() {
        return Err(io::Error::other(format!(
            "could not establish an exact active pending snapshot for sibling stop: {}",
            warnings.join("; ")
        )));
    }
    let expected_agent = state
        .agent_id
        .clone()
        .or_else(|| active_session_key_agent_segment(&state.active_session_key));
    let mut seen = BTreeSet::new();
    let mut queue_ids = Vec::new();
    for value in values {
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
        match state.account_id.as_deref() {
            Some(expected_account)
                if value.get("accountId").and_then(serde_json::Value::as_str)
                    != Some(expected_account) =>
            {
                continue;
            }
            None if value
                .get("accountId")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|account_id| {
                    !account_id.trim().is_empty() && account_id != DEFAULT_CHANNEL_STATE_ACCOUNT_ID
                }) =>
            {
                // An old account-less state file has no authority to stop a
                // non-default provider account.
                continue;
            }
            _ => {}
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
        ChannelCommandEffect::ResolveExternalEffect { approved: true, .. } => "approve",
        ChannelCommandEffect::ResolveExternalEffect {
            approved: false, ..
        } => "deny",
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
            account_id: state.account_id.clone(),
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

fn validate_channel_session_state_v2_lane(
    state: &ChannelSessionState,
    lane: &ChannelStateLane,
) -> io::Result<()> {
    if state.schema != CHANNEL_SESSION_STATE_V2_SCHEMA {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "channel state schema `{}` is not the required v2 schema",
                state.schema
            ),
        ));
    }
    if state.platform != lane.platform()
        || state.account_id.as_deref() != Some(lane.account_id())
        || state.channel_id != lane.channel_id()
        || state.user_id != lane.user_id()
        || state.agent_id.as_deref() != Some(lane.agent_id())
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "channel session state does not match its requested v2 lane",
        ));
    }
    Ok(())
}

fn legacy_state_has_default_account(state: &ChannelSessionState) -> bool {
    normalize_channel_state_lane_account_id(state.account_id.as_deref())
        .is_ok_and(|account_id| account_id == DEFAULT_CHANNEL_STATE_ACCOUNT_ID)
}

fn legacy_state_matches_non_agent_lane_axes(
    state: &ChannelSessionState,
    lane: &ChannelStateLane,
) -> bool {
    state.schema == CHANNEL_STATE_SCHEMA
        && normalize_channel_state_lane_platform(&state.platform)
            .is_ok_and(|platform| platform == lane.platform())
        && normalize_required_channel_state_lane_axis(&state.channel_id, "channelId", false)
            .is_ok_and(|channel_id| channel_id == lane.channel_id())
        && normalize_required_channel_state_lane_axis(&state.user_id, "userId", false)
            .is_ok_and(|user_id| user_id == lane.user_id())
}

fn normalize_channel_state_lane_platform(value: &str) -> io::Result<String> {
    normalize_required_channel_state_lane_axis(value, "platform", true)
}

fn normalize_channel_state_lane_account_id(value: Option<&str>) -> io::Result<String> {
    match value {
        Some(value) if !value.trim().is_empty() => {
            normalize_required_channel_state_lane_axis(value, "accountId", true)
        }
        _ => Ok(DEFAULT_CHANNEL_STATE_ACCOUNT_ID.to_string()),
    }
}

fn normalize_required_channel_state_lane_axis(
    value: &str,
    axis: &str,
    lowercase: bool,
) -> io::Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("channel-state v2 lane requires a non-empty {axis}"),
        ));
    }
    if trimmed.chars().any(char::is_control) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("channel-state v2 lane {axis} cannot contain control characters"),
        ));
    }
    let normalized = if lowercase {
        trimmed.to_ascii_lowercase()
    } else {
        trimmed.to_string()
    };
    if normalized.len() > MAX_CHANNEL_STATE_LANE_AXIS_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "channel-state v2 lane {axis} exceeds {MAX_CHANNEL_STATE_LANE_AXIS_BYTES} bytes"
            ),
        ));
    }
    Ok(normalized)
}

fn channel_state_lane_v2_key(lane: &ChannelStateLane) -> String {
    let mut canonical = Vec::new();
    for (axis, value) in [
        ("platform", lane.platform()),
        ("accountId", lane.account_id()),
        ("channelId", lane.channel_id()),
        ("userId", lane.user_id()),
        ("agentId", lane.agent_id()),
    ] {
        canonical.extend_from_slice(axis.as_bytes());
        canonical.push(0);
        canonical.extend_from_slice(&(value.len() as u64).to_be_bytes());
        canonical.extend_from_slice(value.as_bytes());
    }
    let digest = digest::digest(&digest::SHA256, &canonical);
    let mut key = String::with_capacity(digest.as_ref().len() * 2);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in digest.as_ref() {
        key.push(char::from(HEX[usize::from(*byte >> 4)]));
        key.push(char::from(HEX[usize::from(*byte & 0x0f)]));
    }
    key
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
        SkillEcosystemIdentity, VirtualSkillManifestStatus, append_agent_progress_event,
        create_virtual_skill_manifest, load_virtual_skill_manifest, plan_agent_progress_delivery,
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
        create_virtual_skill_manifest(
            &harness_home,
            SkillEcosystemIdentity {
                virtual_session_id: snapshot.virtual_session_id.clone(),
                root_session_key_hash: "abcdef123456".to_string(),
                concrete_session_hash: "123456abcdef".to_string(),
                exact_lane_digest: "feedface1234".to_string(),
                agent_id: "main".to_string(),
            },
            Some("turn-sha256:abcdef123456".to_string()),
            "catalog-1".to_string(),
            "topology-1".to_string(),
        )
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
        let skill_manifest =
            load_virtual_skill_manifest(&harness_home, &snapshot.virtual_session_id)
                .unwrap()
                .expect("previous task skill manifest");
        assert_eq!(skill_manifest.status, VirtualSkillManifestStatus::Completed);
        assert_eq!(
            skill_manifest.close_reason.as_deref(),
            Some("channel-command:/new")
        );

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
        assert!(report.receipt.session_transition_id.is_some());
        assert_eq!(
            report.receipt.session_transition_phase,
            Some(ChannelSessionTransitionPhaseV1::BoundaryCommitted)
        );
        let transition_receipts = fs::read_to_string(
            crate::channel_session_transition_receipts_file(&harness_home),
        )
        .unwrap();
        assert!(transition_receipts.contains("\"goalBoundary\":\"not-applicable\""));
        assert!(transition_receipts.contains("\"phase\":\"boundary-committed\""));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn stop_without_active_goal_uses_not_applicable_fast_path() {
        stop_command_writes_runtime_cancel_marker();
    }

    #[test]
    fn new_keeps_old_session_active_until_goal_cancel_is_proven() {
        let root = temp_root("new_keeps_old_session_active_until_goal_cancel_is_proven");
        let harness_home = root.join(".agent-harness");
        let old_session = "telegram:dm:user:main";
        let lane = ChannelStateLane::new("telegram", None, "dm", "user", "main").unwrap();
        let authority_file = harness_home
            .join("state")
            .join("context-rollover")
            .join("virtual-session-authority-receipts.jsonl");
        crate::append_jsonl_value(
            &authority_file,
            &serde_json::json!({
                "schema": "agent-harness.virtual-session-authority.v1",
                "queueId": "queue-active-goal",
                "virtualSessionId": "virtual-active-goal",
                "workingSessionKey": old_session,
                "laneDigest": lane.exact_lane_digest(),
                "backendContextGeneration": "generation-active-goal",
                "workingSetFile": "opaque-working-set-ref",
                "status": "authoritative-v2",
                "reason": "fixture",
                "updatedAtMs": 900
            }),
        )
        .unwrap();
        let projection_file = harness_home
            .join("state")
            .join("runtime-queue")
            .join("codex-goal-projection-receipts.jsonl");
        crate::append_jsonl_value(
            &projection_file,
            &serde_json::json!({
                "schema": "agent-harness.codex-goal-projection.v1",
                "queueId": "queue-active-goal",
                "sessionKey": old_session,
                "sourceThreadId": "thread-active-goal",
                "sourceTurnId": "turn-active-goal",
                "backendGoalRef": "goal-active",
                "goalReference": "goal-active",
                "laneDigest": lane.exact_lane_digest(),
                "backendContextGeneration": "generation-active-goal",
                "objective": "finish the active goal",
                "status": "active",
                "observationPhase": "owned-turn",
                "turnRelation": "current-owned-turn",
                "sourceFinalEligible": true,
                "goalChecksum": "goal-checksum-active",
                "projectionChecksum": "projection-checksum-active",
                "projectionComplete": true,
                "observationOrder": 1,
                "observedAtMs": 901
            }),
        )
        .unwrap();

        let report = apply_channel_command_step(
            &command_step_with_session(
                old_session,
                ChannelCommandEffect::StartNewSession {
                    topic: Some("next task".to_string()),
                    new_session_key: "telegram:dm:user:main:session-new".to_string(),
                },
            ),
            ChannelCommandApplyOptions {
                harness_home: harness_home.clone(),
                now_ms: 1000,
            },
        )
        .unwrap();

        assert_eq!(
            report.state.as_ref().unwrap().active_session_key,
            old_session
        );
        assert_eq!(
            report.receipt.session_transition_phase,
            Some(ChannelSessionTransitionPhaseV1::GoalClosurePending)
        );
        assert!(crate::goal_closure_intents_file(&harness_home).is_file());
        let cancel_marker = harness_home
            .join("state")
            .join("runtime-queue")
            .join("cancel-requests")
            .join("telegram_dm_user_main.json");
        assert!(cancel_marker.is_file());
        assert_eq!(
            crate::channel_session_transition_admission(
                &harness_home,
                &lane.exact_lane_digest(),
                old_session,
            )
            .unwrap(),
            crate::ChannelSessionAdmissionV1::HoldPendingNew
        );

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

    #[test]
    fn v2_lane_state_isolates_same_channel_user_by_account_and_agent() {
        let root = temp_root("v2_lane_state_isolates_same_channel_user_by_account_and_agent");
        let harness_home = root.join(".agent-harness");
        let account_a_main = ChannelStateLane::new(
            "discord",
            Some("bot-a"),
            "shared-channel",
            "shared-user",
            "main",
        )
        .unwrap();
        let account_b_main = ChannelStateLane::new(
            "discord",
            Some("bot-b"),
            "shared-channel",
            "shared-user",
            "main",
        )
        .unwrap();
        let account_a_side = ChannelStateLane::new(
            "discord",
            Some("bot-a"),
            "shared-channel",
            "shared-user",
            "side",
        )
        .unwrap();

        let mut a_main =
            sample_channel_state("discord", "shared-channel", "shared-user", "main", "a-main");
        bind_channel_session_state_to_lane_v2(&mut a_main, &account_a_main);
        write_channel_session_state_v2(&harness_home, &account_a_main, &a_main).unwrap();

        let mut b_main =
            sample_channel_state("discord", "shared-channel", "shared-user", "main", "b-main");
        bind_channel_session_state_to_lane_v2(&mut b_main, &account_b_main);
        write_channel_session_state_v2(&harness_home, &account_b_main, &b_main).unwrap();

        let mut a_side =
            sample_channel_state("discord", "shared-channel", "shared-user", "side", "a-side");
        bind_channel_session_state_to_lane_v2(&mut a_side, &account_a_side);
        write_channel_session_state_v2(&harness_home, &account_a_side, &a_side).unwrap();

        assert_ne!(
            channel_session_state_v2_file(&harness_home, &account_a_main),
            channel_session_state_v2_file(&harness_home, &account_b_main)
        );
        assert_ne!(
            channel_session_state_v2_file(&harness_home, &account_a_main),
            channel_session_state_v2_file(&harness_home, &account_a_side)
        );
        assert_eq!(
            read_channel_session_state_v2(&harness_home, &account_a_main)
                .unwrap()
                .unwrap()
                .session_topic
                .as_deref(),
            Some("a-main")
        );
        assert_eq!(
            read_channel_session_state_v2(&harness_home, &account_b_main)
                .unwrap()
                .unwrap()
                .session_topic
                .as_deref(),
            Some("b-main")
        );
        assert_eq!(
            read_channel_session_state_v2(&harness_home, &account_a_side)
                .unwrap()
                .unwrap()
                .session_topic
                .as_deref(),
            Some("a-side")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn v2_state_path_is_deterministic_safe_and_not_lossy() {
        let harness_home = PathBuf::from("C:/harness-home");
        let canonical =
            ChannelStateLane::new("discord", Some("ops"), "channel/a", "user?one", "main").unwrap();
        let equivalent =
            ChannelStateLane::new(" Discord ", Some(" OPS "), "channel/a", "user?one", "main")
                .unwrap();
        let distinct =
            ChannelStateLane::new("discord", Some("ops"), "channel?a", "user?one", "main").unwrap();

        let path = channel_session_state_v2_file(&harness_home, &canonical);
        assert_eq!(
            path,
            channel_session_state_v2_file(&harness_home, &equivalent)
        );
        assert_ne!(
            path,
            channel_session_state_v2_file(&harness_home, &distinct)
        );
        let v2_root = harness_home.join("state").join("channels").join("v2");
        let relative = path.strip_prefix(v2_root).unwrap();
        let parts = relative.components().collect::<Vec<_>>();
        assert_eq!(parts.len(), 2);
        let key = parts[0].as_os_str().to_str().unwrap();
        assert_eq!(key.len(), 64);
        assert!(
            key.bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        );
        assert_eq!(parts[1].as_os_str(), "state.json");
        assert!(!path.to_string_lossy().contains("channel/a"));
    }

    #[test]
    fn legacy_unknown_account_or_missing_or_mismatched_agent_cannot_bleed_into_v2() {
        let root =
            temp_root("legacy_unknown_account_or_missing_or_mismatched_agent_cannot_bleed_into_v2");
        let harness_home = root.join(".agent-harness");
        let default_main =
            ChannelStateLane::new("discord", None, "channel-1", "user-1", "main").unwrap();
        let legacy_file =
            channel_session_state_file(&harness_home, "discord", "channel-1", "user-1");

        let missing_agent = sample_channel_state("discord", "channel-1", "user-1", "", "missing");
        write_json_atomic(&legacy_file, &missing_agent).unwrap();
        let missing =
            migrate_legacy_channel_session_state_to_v2(&harness_home, &default_main).unwrap();
        assert_eq!(
            missing.status,
            ChannelSessionStateV2MigrationStatus::RejectedLegacyMissingAgent
        );
        assert!(missing.state.is_none());
        assert!(!channel_session_state_v2_file(&harness_home, &default_main).is_file());

        let mismatched_agent =
            sample_channel_state("discord", "channel-1", "user-1", "side", "mismatch");
        write_json_atomic(&legacy_file, &mismatched_agent).unwrap();
        let mismatch =
            migrate_legacy_channel_session_state_to_v2(&harness_home, &default_main).unwrap();
        assert_eq!(
            mismatch.status,
            ChannelSessionStateV2MigrationStatus::RejectedLegacyAgentMismatch
        );
        assert!(mismatch.state.is_none());
        assert!(!channel_session_state_v2_file(&harness_home, &default_main).is_file());

        let mismatched_channel =
            sample_channel_state("discord", "other-channel", "user-1", "main", "wrong lane");
        write_json_atomic(&legacy_file, &mismatched_channel).unwrap();
        let wrong_lane =
            migrate_legacy_channel_session_state_to_v2(&harness_home, &default_main).unwrap();
        assert_eq!(
            wrong_lane.status,
            ChannelSessionStateV2MigrationStatus::RejectedLegacyIdentityMismatch
        );
        assert!(wrong_lane.state.is_none());
        assert!(!channel_session_state_v2_file(&harness_home, &default_main).is_file());

        let configured_account =
            ChannelStateLane::new("discord", Some("ops"), "channel-1", "user-1", "main").unwrap();
        let account =
            migrate_legacy_channel_session_state_to_v2(&harness_home, &configured_account).unwrap();
        assert_eq!(
            account.status,
            ChannelSessionStateV2MigrationStatus::RejectedLegacyUnknownAccount
        );
        assert!(account.state.is_none());
        assert!(!channel_session_state_v2_file(&harness_home, &configured_account).is_file());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn exact_default_legacy_state_migrates_to_v2_without_losing_fields() {
        let root = temp_root("exact_default_legacy_state_migrates_to_v2_without_losing_fields");
        let harness_home = root.join(".agent-harness");
        let lane = ChannelStateLane::new("discord", None, "channel-1", "user-1", "main").unwrap();
        let legacy_file =
            channel_session_state_file(&harness_home, "discord", "channel-1", "user-1");
        let mut legacy =
            sample_channel_state("discord", "channel-1", "user-1", "main", "migration topic");
        legacy.provider = Some("openai".to_string());
        legacy.model = Some("gpt-5.6-sol".to_string());
        legacy.thinking_level = Some("max".to_string());
        legacy.config_revision = Some("config-revision-7".to_string());
        legacy.steering_notes.push(ChannelSessionNote {
            at_ms: 7,
            text: "retain this note".to_string(),
        });
        write_json_atomic(&legacy_file, &legacy).unwrap();

        let report = migrate_legacy_channel_session_state_to_v2(&harness_home, &lane).unwrap();
        assert_eq!(
            report.status,
            ChannelSessionStateV2MigrationStatus::MigratedLegacyDefaultAccount
        );
        let migrated = report.state.unwrap();
        assert_eq!(migrated.schema, CHANNEL_SESSION_STATE_V2_SCHEMA);
        assert_eq!(migrated.account_id.as_deref(), Some("default"));
        assert_eq!(
            migrated.config_revision.as_deref(),
            Some("config-revision-7")
        );
        assert_eq!(migrated.provider, legacy.provider);
        assert_eq!(migrated.model, legacy.model);
        assert_eq!(migrated.thinking_level, legacy.thinking_level);
        assert_eq!(migrated.session_topic, legacy.session_topic);
        assert_eq!(migrated.steering_notes, legacy.steering_notes);
        assert!(legacy_file.is_file());
        assert_eq!(
            read_channel_session_state_v2(&harness_home, &lane).unwrap(),
            Some(migrated)
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn autonomous_new_reconciliation_commits_frozen_target_and_releases_admission() {
        let root =
            temp_root("autonomous_new_reconciliation_commits_frozen_target_and_releases_admission");
        let harness_home = root.join(".agent-harness");
        let lane =
            ChannelStateLane::new("telegram", Some("default"), "dm", "user", "main").unwrap();
        let mut state = sample_channel_state("telegram", "dm", "user", "main", "old topic");
        bind_channel_session_state_to_lane_v2(&mut state, &lane);
        let old_session = state.active_session_key.clone();
        let proposed_new_session = "telegram:dm:user:main:session-recovered".to_string();
        let frozen_new_session =
            canonical_session_key_for_lane(&proposed_new_session, &lane).unwrap();
        write_channel_session_state_v2(&harness_home, &lane, &state).unwrap();
        let prepared =
            prepare_channel_session_transition(PrepareChannelSessionTransitionOptionsV1 {
                harness_home: harness_home.clone(),
                command: ChannelSessionTransitionCommandV1::New,
                lane_digest: lane.exact_lane_digest(),
                old_session_key: old_session.clone(),
                proposed_new_session_key: Some(proposed_new_session),
                topic: Some("recovered topic".to_string()),
                reason: Some("recover pending new".to_string()),
                now_ms: 1000,
            })
            .unwrap();
        assert_eq!(
            prepared.intent.goal_boundary,
            ChannelGoalBoundaryV1::NotApplicable
        );
        assert_eq!(
            crate::channel_session_transition_admission(
                &harness_home,
                &lane.exact_lane_digest(),
                &old_session,
            )
            .unwrap(),
            crate::ChannelSessionAdmissionV1::HoldPendingNew
        );

        let options = ReconcileChannelSessionTransitionsOptionsV1 {
            harness_home: harness_home.clone(),
            executable: PathBuf::from("unused"),
            arguments: Vec::new(),
            working_directory: root.clone(),
            codex_home: None,
            timeout_ms: 1000,
            max_transitions: 8,
            now_ms: 1100,
        };
        let report = reconcile_channel_session_transitions_with(options.clone(), |_| {
            panic!("not-applicable transition must not execute a backend goal mutation")
        })
        .unwrap();
        assert_eq!(report.scanned, 1);
        assert_eq!(report.committed, 1, "{report:?}");
        assert_eq!(report.retry_pending, 0);
        let recovered = read_channel_session_state_v2(&harness_home, &lane)
            .unwrap()
            .unwrap();
        assert_eq!(recovered.active_session_key, frozen_new_session);
        assert_eq!(recovered.session_topic.as_deref(), Some("recovered topic"));
        assert_eq!(
            crate::channel_session_transition_admission(
                &harness_home,
                &lane.exact_lane_digest(),
                &old_session,
            )
            .unwrap(),
            crate::ChannelSessionAdmissionV1::Open
        );

        let replay = reconcile_channel_session_transitions_with(options, |_| {
            panic!("committed transition must not execute again")
        })
        .unwrap();
        assert_eq!(replay.scanned, 0);
        assert_eq!(replay.committed, 0);
        let receipts = fs::read_to_string(crate::channel_session_transition_receipts_file(
            &harness_home,
        ))
        .unwrap();
        assert_eq!(
            receipts.matches("\"phase\":\"boundary-committed\"").count(),
            1
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn account_scoped_new_commits_canonical_frozen_session() {
        let root = temp_root("account_scoped_new_commits_canonical_frozen_session");
        let harness_home = root.join(".agent-harness");
        let mut step = command_step_with_session(
            "telegram:dm:user:main",
            ChannelCommandEffect::StartNewSession {
                topic: Some("account-bound topic".to_string()),
                new_session_key: "telegram:dm:user:main:session-account-bound".to_string(),
            },
        );
        step.account_id = Some("bot-primary".to_string());
        step.outbound_messages[0].account_id = Some("bot-primary".to_string());

        let report = apply_channel_command_step(
            &step,
            ChannelCommandApplyOptions {
                harness_home: harness_home.clone(),
                now_ms: 1200,
            },
        )
        .unwrap();
        let state = report.state.unwrap();
        let lane =
            ChannelStateLane::new("telegram", Some("bot-primary"), "dm", "user", "main").unwrap();
        let parsed = crate::channel_session_key::CanonicalChannelSessionKey::parse_for_lane(
            &state.active_session_key,
            lane.platform(),
            lane.account_id(),
            lane.channel_id(),
            lane.user_id(),
            lane.agent_id(),
        )
        .unwrap();
        assert_eq!(parsed.canonical_string(), state.active_session_key);
        assert_eq!(
            report.receipt.session_transition_phase,
            Some(ChannelSessionTransitionPhaseV1::BoundaryCommitted)
        );

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
                delivery_id: None,
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

    fn sample_channel_state(
        platform: &str,
        channel_id: &str,
        user_id: &str,
        agent_id: &str,
        topic: &str,
    ) -> ChannelSessionState {
        ChannelSessionState {
            schema: CHANNEL_STATE_SCHEMA.to_string(),
            platform: platform.to_string(),
            account_id: None,
            channel_id: channel_id.to_string(),
            user_id: user_id.to_string(),
            active_session_key: format!("{platform}:{channel_id}:{user_id}:{agent_id}:session"),
            agent_id: (!agent_id.is_empty()).then(|| agent_id.to_string()),
            provider: None,
            model: None,
            session_topic: Some(topic.to_string()),
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
            config_revision: None,
            updated_at_ms: 1,
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
