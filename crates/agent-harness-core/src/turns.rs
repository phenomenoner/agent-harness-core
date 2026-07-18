use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::Value;

use crate::backend_reasoning::{
    BackendReasoningPolicyV1, BackendReasoningSource, ReasoningPreference,
};
use crate::channel_session_key::CanonicalChannelSessionKey;
use crate::channel_state::{
    ChannelSessionStateV2MigrationStatus, ChannelStateLane,
    migrate_legacy_channel_session_state_to_v2, read_channel_session_state_v2,
};
use crate::{
    AgentOverride, AgentProfile, AgentRegistry, AgentSource, ChannelCommand, ChannelCommandIntent,
    ChannelSessionState, DEFAULT_THINKING_LEVEL, InboundMediaArtifact, PROMPT_FILE_NAMES,
    SkillIndex, SkillRoutingQueryV2, SkillSelection, SkillSelectionQuery,
    build_runtime_skill_index, build_source_skill_index, collect_skill_usage_snapshot_for_agent,
    config::harness_config_candidates, parse_channel_command, read_agent_override,
    read_channel_session_state, select_skills, write_skill_selection_receipt,
};

const TURN_PLAN_SCHEMA: &str = "agent-harness.turn-plan.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnPlanInput {
    pub harness_home: Option<PathBuf>,
    pub platform: String,
    pub channel_id: String,
    pub user_id: String,
    pub text: String,
    pub inbound_context: Option<String>,
    pub inbound_media_artifacts: Vec<InboundMediaArtifact>,
    pub requested_agent_id: Option<String>,
    pub session_hint: Option<String>,
    pub skill_limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnPlan {
    pub schema: &'static str,
    pub harness_home: Option<PathBuf>,
    pub source_home: PathBuf,
    pub source_workspace: PathBuf,
    pub platform: String,
    /// `Some` denotes an exact v2 channel/account lane. `None` is retained
    /// only for legacy callers that deliberately use the v1 channel state.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account_id: Option<String>,
    pub channel_id: String,
    pub user_id: String,
    pub message_text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inbound_context: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub inbound_media_artifacts: Vec<InboundMediaArtifact>,
    pub session_key: String,
    pub dispatch: TurnDispatch,
    pub agent: Option<TurnAgent>,
    pub model_policy: TurnModelPolicy,
    pub provider_request_policy: TurnProviderRequestPolicy,
    pub thinking_policy: TurnThinkingPolicy,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_preference: Option<ReasoningPreference>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backend_reasoning_policy: Option<BackendReasoningPolicyV1>,
    pub channel_state: Option<ChannelSessionState>,
    pub command: Option<ChannelCommand>,
    pub command_intent: Option<ChannelCommandIntent>,
    pub prompt_files: Vec<TurnPromptFile>,
    pub selected_skills: Vec<SkillSelection>,
    /// Internal-only input for the runtime shadow router. It is deliberately
    /// excluded from the serialized turn plan and model-facing prompt.
    #[serde(skip)]
    pub skill_shadow_v2_query: Option<SkillRoutingQueryV2>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum TurnDispatch {
    AgentTurn,
    ChannelCommand,
    NoAgentAvailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnAgent {
    pub id: String,
    pub enabled: Option<bool>,
    pub workspace: Option<String>,
    pub directory: PathBuf,
    pub directory_exists: bool,
    pub sessions_index_exists: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnModelPolicy {
    pub provider: Option<String>,
    pub model: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnProviderRequestPolicy {
    pub fast_mode: String,
    pub effective_acceleration: String,
    pub route_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_tier: Option<String>,
    pub reason: String,
}

impl Default for TurnProviderRequestPolicy {
    fn default() -> Self {
        Self {
            fast_mode: "normal".to_string(),
            effective_acceleration: "disabled".to_string(),
            route_kind: "codex-app-server".to_string(),
            service_tier: None,
            reason: "fast mode is normal for this session".to_string(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnThinkingPolicy {
    pub enabled: bool,
    pub level: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnPromptFile {
    pub name: String,
    pub path: PathBuf,
    pub exists: bool,
    pub bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnPlanFile {
    pub json: PathBuf,
}

pub fn build_turn_plan(
    source: &AgentSource,
    registry: &AgentRegistry,
    skill_index: &SkillIndex,
    input: TurnPlanInput,
) -> io::Result<TurnPlan> {
    build_turn_plan_for_account(source, registry, skill_index, input, None)
}

/// Builds an interactive turn for an explicit provider account. This is the
/// only entry point that reads or migrates v2 channel state; the compatibility
/// wrapper above intentionally retains the legacy, account-less behavior for
/// older integrations.
pub fn build_turn_plan_for_account(
    source: &AgentSource,
    registry: &AgentRegistry,
    skill_index: &SkillIndex,
    input: TurnPlanInput,
    account_id: Option<String>,
) -> io::Result<TurnPlan> {
    let mut warnings = Vec::new();
    // An explicit account turns this into a full-lane operation.  If the
    // selected agent/lane cannot be derived, do not fall through to the
    // account-less compatibility state file: that would let one account
    // inherit another account's session overrides.
    let account_scope_requested = account_id.is_some();
    let selected_agent = select_agent(registry, input.requested_agent_id.as_deref(), &mut warnings);
    let skill_source = skill_source_for_agent(source, selected_agent);
    let scoped_skill_index = if let Some(harness_home) = input.harness_home.as_ref() {
        Some(build_runtime_skill_index(&skill_source, harness_home)?)
    } else if skill_source != *source {
        Some(build_source_skill_index(&skill_source)?)
    } else {
        None
    };
    let skill_index = scoped_skill_index.as_ref().unwrap_or(skill_index);
    let agent = selected_agent.map(turn_agent);
    let mut model_policy = selected_agent.map(turn_model_policy).unwrap_or_default();
    let mut thinking_policy = TurnThinkingPolicy::default();
    let mut reasoning_preference = None;
    let mut backend_reasoning_policy = None;
    let command = parse_channel_command(&input.text);
    let command_intent = command.clone().map(ChannelCommand::into_intent);
    let dispatch = if command.is_some() {
        TurnDispatch::ChannelCommand
    } else if agent.is_some() {
        TurnDispatch::AgentTurn
    } else {
        TurnDispatch::NoAgentAvailable
    };
    if dispatch == TurnDispatch::NoAgentAvailable {
        warnings.push("no harness agent is available for this turn".to_string());
    }

    let agent_override = load_agent_override(
        input.harness_home.as_deref(),
        agent.as_ref().map(|agent| agent.id.as_str()),
        &mut warnings,
    );
    if let Some(agent_override) = &agent_override {
        apply_agent_override(
            &mut model_policy,
            &mut thinking_policy,
            &mut reasoning_preference,
            &mut backend_reasoning_policy,
            agent_override,
        );
    }

    let selected_agent_id = agent.as_ref().map(|agent| agent.id.as_str());
    let account_lane = match (account_id.as_deref(), selected_agent_id) {
        (Some(account_id), Some(agent_id)) => {
            match ChannelStateLane::new(
                &input.platform,
                Some(account_id),
                &input.channel_id,
                &input.user_id,
                agent_id,
            ) {
                Ok(lane) => Some(lane),
                Err(error) => {
                    warnings.push(format!(
                        "exact channel account lane is invalid; refusing legacy state fallback: {error}"
                    ));
                    None
                }
            }
        }
        (Some(_), None) => {
            warnings.push(
                "exact channel account lane could not be built because no agent was selected"
                    .to_string(),
            );
            None
        }
        (None, _) => None,
    };
    let account_id = account_lane
        .as_ref()
        .map(|lane| lane.account_id().to_string());
    let channel_state = if account_scope_requested && account_lane.is_none() {
        None
    } else {
        load_channel_state_for_turn(
            input.harness_home.as_deref(),
            &input.platform,
            &input.channel_id,
            &input.user_id,
            account_lane.as_ref(),
            &mut warnings,
        )
    }
    .and_then(|state| channel_state_for_selected_agent(state, selected_agent_id, &mut warnings));
    if let Some(state) = &channel_state {
        apply_model_override(&mut model_policy, state);
        apply_thinking_override(&mut thinking_policy, state);
        apply_reasoning_override(
            &mut reasoning_preference,
            &mut backend_reasoning_policy,
            state,
        );
    }
    if crate::model_catalog::model_catalog_rollout_mode_for_agent(
        input.harness_home.as_deref(),
        selected_agent_id,
    ) != crate::model_catalog::ModelCatalogRolloutMode::Authoritative
    {
        reasoning_preference = None;
        backend_reasoning_policy = None;
    } else if let Some(preference) = reasoning_preference.as_ref() {
        backend_reasoning_policy = resolve_backend_reasoning_policy_for_turn(
            input.harness_home.as_deref(),
            &model_policy,
            preference,
            backend_reasoning_policy
                .as_ref()
                .map(|policy| policy.source()),
            &mut warnings,
        );
    }
    if reasoning_preference.is_some() && backend_reasoning_policy.is_none() {
        warnings.push(
            "backend reasoning preference is stored but has no route-valid resolved policy; execution must fail closed until it can be revalidated"
                .to_string(),
        );
    }
    let provider_request_policy = provider_request_policy_from_state(
        &model_policy,
        channel_state.as_ref(),
        agent_override.as_ref(),
        input.harness_home.as_deref(),
    );
    let session_key = if let Some(account_id) = account_id.as_deref() {
        let existing_session_key = input.session_hint.clone().or_else(|| {
            channel_state
                .as_ref()
                .map(|state| state.active_session_key.clone())
        });
        if let Some(existing_session_key) = existing_session_key {
            bind_session_key_to_account_checked(
                &existing_session_key,
                &input.platform,
                account_id,
                &input.channel_id,
                &input.user_id,
                agent.as_ref().map(|agent| agent.id.as_str()),
            )?
        } else {
            session_key_for_account(
                &input.platform,
                account_id,
                &input.channel_id,
                &input.user_id,
                agent.as_ref().map(|agent| agent.id.as_str()),
            )
        }
    } else {
        input
            .session_hint
            .clone()
            .or_else(|| {
                channel_state
                    .as_ref()
                    .map(|state| state.active_session_key.clone())
            })
            .unwrap_or_else(|| {
                session_key(
                    &input.platform,
                    &input.channel_id,
                    &input.user_id,
                    agent.as_ref().map(|agent| agent.id.as_str()),
                )
            })
    };

    let (prompt_workspace, prompt_files) =
        prompt_files_for_selected_agent(source, selected_agent, &mut warnings)?;
    let skill_query_text = channel_state_query_text(&input.text, channel_state.as_ref());
    let skill_config = input
        .harness_home
        .as_ref()
        .map(|harness_home| load_skill_selection_config(harness_home))
        .transpose()?
        .unwrap_or_default();
    let usage_snapshot = if skill_config.usage_prior_enabled {
        match (input.harness_home.as_ref(), selected_agent_id) {
            (Some(harness_home), Some(agent_id)) => Some(collect_skill_usage_snapshot_for_agent(
                harness_home,
                agent_id,
            )?),
            _ => None,
        }
    } else {
        None
    };
    let skill_query = SkillSelectionQuery {
        text: skill_query_text,
        include_context_tokens: true,
        agent_id: agent.as_ref().map(|agent| agent.id.clone()),
        channel: Some(input.platform.clone()),
        workspace: agent
            .as_ref()
            .and_then(|agent| agent.workspace.clone())
            .or_else(|| Some(prompt_workspace.display().to_string())),
        agent_mode: None,
        available_tools: Vec::new(),
        available_toolsets: Vec::new(),
        fts_enabled: skill_config.fts_enabled,
        vector_tie_break_enabled: false,
        usage_snapshot,
        usage_prior_enabled: skill_config.usage_prior_enabled,
        limit: input.skill_limit,
    };
    let selected_skills = if dispatch == TurnDispatch::AgentTurn {
        let selected = select_skills(skill_index, &skill_query);
        if let Some(harness_home) = input.harness_home.as_ref()
            && let Err(error) = write_skill_selection_receipt(harness_home, &skill_query, &selected)
        {
            warnings.push(format!(
                "skill selection receipt could not be written: {error}"
            ));
        }
        selected
    } else {
        Vec::new()
    };
    let skill_shadow_v2_query = (dispatch == TurnDispatch::AgentTurn
        && skill_config.shadow_v2_enabled)
        .then(|| SkillRoutingQueryV2 {
            task_text: input.text.clone(),
            explicit_invocations: Vec::new(),
            agent_id: agent
                .as_ref()
                .map(|agent| agent.id.clone())
                .unwrap_or_default(),
            channel: input.platform.clone(),
            available_tools: Vec::new(),
            available_toolsets: Vec::new(),
            risk_context: Vec::new(),
            virtual_task_intent: None,
            ambient_notes_excluded_bytes: skill_query.text.len().saturating_sub(input.text.len()),
            usage_snapshot: skill_query.usage_snapshot.clone(),
        });
    Ok(TurnPlan {
        schema: TURN_PLAN_SCHEMA,
        harness_home: input.harness_home,
        source_home: source.home.clone(),
        source_workspace: prompt_workspace,
        platform: input.platform,
        account_id,
        channel_id: input.channel_id,
        user_id: input.user_id,
        message_text: input.text,
        inbound_context: input
            .inbound_context
            .filter(|value| !value.trim().is_empty()),
        inbound_media_artifacts: input.inbound_media_artifacts,
        session_key,
        dispatch,
        agent,
        model_policy,
        provider_request_policy,
        thinking_policy,
        reasoning_preference,
        backend_reasoning_policy,
        channel_state,
        command,
        command_intent,
        prompt_files,
        selected_skills,
        skill_shadow_v2_query,
        warnings,
    })
}

fn resolve_backend_reasoning_policy_for_turn(
    harness_home: Option<&Path>,
    model_policy: &TurnModelPolicy,
    preference: &ReasoningPreference,
    prior_source: Option<BackendReasoningSource>,
    warnings: &mut Vec<String>,
) -> Option<BackendReasoningPolicyV1> {
    if let Err(error) = preference.validate() {
        warnings.push(format!(
            "backend reasoning preference is invalid and was suspended: {error}"
        ));
        return None;
    }
    let provider = model_policy.provider.as_deref().unwrap_or_default();
    let model = model_policy.model.as_deref().unwrap_or_default();
    let catalog = load_cached_model_catalog(harness_home, warnings)?;
    let requested_effort = match preference {
        ReasoningPreference::Default => {
            let Some(route) = catalog.exact_route(provider, model) else {
                warnings.push(format!(
                    "backend reasoning default cannot be resolved because the exact route {provider}/{model} is absent from the model catalog"
                ));
                return None;
            };
            let Some(default) = route.default_reasoning_effort.as_deref() else {
                warnings.push(format!(
                    "backend reasoning default cannot be resolved because {provider}/{model} advertises no default effort"
                ));
                return None;
            };
            if !route
                .supported_reasoning_efforts
                .iter()
                .any(|effort| effort.eq_ignore_ascii_case(default))
            {
                warnings.push(format!(
                    "backend reasoning default {default} is not present in the supported effort list for {provider}/{model}"
                ));
                return None;
            }
            default.to_string()
        }
        ReasoningPreference::Explicit { effort } => effort.clone(),
    };

    // Ultra is a separate resource/delegation mode. A persisted channel/default
    // preference never authorizes it by itself; only a child-admission receipt may.
    let source = prior_source.unwrap_or(BackendReasoningSource::AgentDefault);
    if requested_effort.eq_ignore_ascii_case("ultra")
        && source != BackendReasoningSource::ChildAdmission
    {
        warnings.push(
            "backend reasoning ultra remains suspended until a child-admission authorization receipt is present"
                .to_string(),
        );
        return None;
    }

    let resolution = crate::model_catalog::resolve_reasoning_effort(
        Some(&catalog),
        crate::model_catalog::ModelCatalogRolloutMode::Authoritative,
        provider,
        model,
        &requested_effort,
        crate::model_catalog::UnsupportedReasoningPolicy::Reject,
    );
    if !resolution.authoritative
        || resolution.status != crate::model_catalog::ReasoningResolutionStatus::Accepted
    {
        warnings.push(format!(
            "backend reasoning preference could not be resolved for {provider}/{model}: {}",
            resolution.reason
        ));
        return None;
    }
    BackendReasoningPolicyV1::new(source, resolution)
        .map_err(|error| {
            warnings.push(format!(
                "backend reasoning preference resolved but could not become an execution policy: {error}"
            ));
        })
        .ok()
}

fn load_cached_model_catalog(
    harness_home: Option<&Path>,
    warnings: &mut Vec<String>,
) -> Option<crate::model_catalog::ModelCapabilityCatalog> {
    let harness_home = harness_home?;
    let cache_file = harness_home.join("codex-home").join("models_cache.json");
    let text = match fs::read_to_string(&cache_file) {
        Ok(text) => text,
        Err(error) => {
            warnings.push(format!(
                "Codex model catalog cache {} is unavailable: {error}",
                cache_file.display()
            ));
            return None;
        }
    };
    match crate::model_catalog::parse_codex_model_catalog(&text) {
        Ok(catalog) => Some(catalog),
        Err(error) => {
            warnings.push(format!(
                "Codex model catalog cache {} is invalid: {error}",
                cache_file.display()
            ));
            None
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct SkillSelectionConfig {
    fts_enabled: bool,
    usage_prior_enabled: bool,
    shadow_v2_enabled: bool,
}

impl Default for SkillSelectionConfig {
    fn default() -> Self {
        Self {
            fts_enabled: true,
            usage_prior_enabled: true,
            shadow_v2_enabled: false,
        }
    }
}

fn load_skill_selection_config(harness_home: &Path) -> io::Result<SkillSelectionConfig> {
    let mut config = SkillSelectionConfig::default();
    let Some(config_file) = harness_config_candidates(harness_home)
        .into_iter()
        .find(|path| path.is_file())
    else {
        return Ok(config);
    };
    let text = fs::read_to_string(config_file)?;
    let Ok(value) = serde_json::from_str::<Value>(&text) else {
        return Ok(config);
    };
    let Some(matcher) = value
        .get("skills")
        .and_then(|skills| skills.get("matcher"))
        .and_then(Value::as_object)
    else {
        return Ok(config);
    };
    if let Some(enabled) = matcher.get("ftsEnabled").and_then(Value::as_bool) {
        config.fts_enabled = enabled;
    }
    if let Some(enabled) = matcher.get("usagePriorEnabled").and_then(Value::as_bool) {
        config.usage_prior_enabled = enabled;
    }
    if let Some(enabled) = matcher.get("shadowV2Enabled").and_then(Value::as_bool) {
        config.shadow_v2_enabled = enabled;
    }
    Ok(config)
}

fn provider_request_policy_from_state(
    model_policy: &TurnModelPolicy,
    state: Option<&ChannelSessionState>,
    override_entry: Option<&AgentOverride>,
    harness_home: Option<&Path>,
) -> TurnProviderRequestPolicy {
    let fast_mode = state
        .and_then(|state| state.fast_mode.clone())
        .or_else(|| override_entry.and_then(|entry| entry.fast_mode.clone()))
        .unwrap_or_else(|| "normal".to_string());
    let request_policy = crate::fast_request_policy_for_route(
        &model_policy.provider,
        &model_policy.model,
        &fast_mode,
        harness_home,
    );
    TurnProviderRequestPolicy {
        fast_mode,
        effective_acceleration: request_policy.effective_acceleration,
        route_kind: "codex-app-server".to_string(),
        service_tier: request_policy.service_tier,
        reason: request_policy.reason,
    }
}

pub fn write_turn_plan(plan: &TurnPlan, output_dir: impl AsRef<Path>) -> io::Result<TurnPlanFile> {
    let output_dir = output_dir.as_ref();
    fs::create_dir_all(output_dir)?;
    let json = output_dir.join("turn-plan.json");
    let text = serde_json::to_string_pretty(plan).map_err(io::Error::other)?;
    fs::write(&json, text)?;
    Ok(TurnPlanFile { json })
}

fn select_agent<'a>(
    registry: &'a AgentRegistry,
    requested_agent_id: Option<&str>,
    warnings: &mut Vec<String>,
) -> Option<&'a AgentProfile> {
    if let Some(requested_agent_id) = requested_agent_id {
        if let Some(agent) = registry
            .agents
            .iter()
            .find(|agent| agent.id == requested_agent_id)
        {
            return Some(agent);
        }
        warnings.push(format!(
            "requested agent `{requested_agent_id}` was not found; falling back to default routing"
        ));
    }

    registry
        .agents
        .iter()
        .find(|agent| agent.id == "main" && agent.enabled != Some(false))
        .or_else(|| {
            registry
                .agents
                .iter()
                .find(|agent| agent.enabled != Some(false))
        })
        .or_else(|| registry.agents.first())
}

fn turn_agent(agent: &AgentProfile) -> TurnAgent {
    TurnAgent {
        id: agent.id.clone(),
        enabled: agent.enabled,
        workspace: agent.workspace.clone(),
        directory: agent.directory.clone(),
        directory_exists: agent.directory_exists,
        sessions_index_exists: agent.sessions_index_exists,
    }
}

fn turn_model_policy(agent: &AgentProfile) -> TurnModelPolicy {
    TurnModelPolicy {
        provider: agent.provider.clone(),
        model: agent.model.clone(),
    }
}

fn load_channel_state_for_turn(
    harness_home: Option<&Path>,
    platform: &str,
    channel_id: &str,
    user_id: &str,
    exact_lane: Option<&ChannelStateLane>,
    warnings: &mut Vec<String>,
) -> Option<ChannelSessionState> {
    let harness_home = harness_home?;
    if let Some(exact_lane) = exact_lane {
        match read_channel_session_state_v2(harness_home, exact_lane) {
            Ok(Some(state)) => return Some(state),
            Ok(None) => {
                match migrate_legacy_channel_session_state_to_v2(harness_home, exact_lane) {
                    Ok(report) => {
                        if let Some(state) = report.state {
                            if report.status
                            == ChannelSessionStateV2MigrationStatus::MigratedLegacyDefaultAccount
                        {
                            warnings.push(format!(
                                "migrated exact default-account channel state into v2 lane for agent `{}`",
                                exact_lane.agent_id()
                            ));
                        }
                            return Some(state);
                        }
                        if !matches!(
                            report.status,
                            ChannelSessionStateV2MigrationStatus::LegacyStateMissing
                        ) {
                            warnings.push(format!(
                            "legacy channel state was not used for exact v2 lane `{}` / `{}`: {:?}",
                            exact_lane.account_id(),
                            exact_lane.agent_id(),
                            report.status
                        ));
                        }
                        return None;
                    }
                    Err(error) => {
                        warnings.push(format!(
                            "exact channel session state could not be read or migrated from {}: {}",
                            harness_home.display(),
                            error
                        ));
                        return None;
                    }
                }
            }
            Err(error) => {
                warnings.push(format!(
                    "exact channel session state could not be read from {}: {}",
                    harness_home.display(),
                    error
                ));
                return None;
            }
        }
    }
    match read_channel_session_state(harness_home, platform, channel_id, user_id) {
        Ok(state) => state,
        Err(error) => {
            warnings.push(format!(
                "channel session state could not be read from {}: {}",
                harness_home.display(),
                error
            ));
            None
        }
    }
}

fn channel_state_for_selected_agent(
    state: ChannelSessionState,
    selected_agent_id: Option<&str>,
    warnings: &mut Vec<String>,
) -> Option<ChannelSessionState> {
    let Some(selected_agent_id) = selected_agent_id else {
        return Some(state);
    };
    let selected_agent = normalize_key_part(selected_agent_id);
    let state_agent = state.agent_id.as_deref().map(normalize_key_part);
    let session_agent =
        active_session_key_agent_segment(&state.active_session_key).map(normalize_key_part);
    let state_matches = state_agent
        .as_deref()
        .map(|agent_id| agent_id == selected_agent)
        .unwrap_or(true);
    let session_matches = session_agent
        .as_deref()
        .map(|agent_id| agent_id == selected_agent)
        .unwrap_or(true);
    if state_matches && session_matches {
        return Some(state);
    }
    warnings.push(format!(
        "ignored channel session state for selected agent `{}` because state agent `{}` and active session agent `{}` did not match",
        selected_agent_id,
        state_agent.as_deref().unwrap_or("<unset>"),
        session_agent.as_deref().unwrap_or("<unset>")
    ));
    None
}

fn active_session_key_agent_segment(session_key: &str) -> Option<&str> {
    session_key
        .split(':')
        .nth(3)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}
fn load_agent_override(
    harness_home: Option<&Path>,
    agent_id: Option<&str>,
    warnings: &mut Vec<String>,
) -> Option<AgentOverride> {
    let (Some(harness_home), Some(agent_id)) = (harness_home, agent_id) else {
        return None;
    };
    match read_agent_override(harness_home, agent_id) {
        Ok(override_entry) => override_entry,
        Err(error) => {
            warnings.push(format!(
                "agent override state could not be read from {}: {}",
                harness_home.display(),
                error
            ));
            None
        }
    }
}

fn apply_model_override(model_policy: &mut TurnModelPolicy, state: &ChannelSessionState) {
    if let Some(provider) = &state.model_override_provider {
        model_policy.provider = Some(provider.clone());
    }
    if let Some(model) = &state.model_override_model {
        model_policy.model = Some(model.clone());
    }
}

fn apply_agent_override(
    model_policy: &mut TurnModelPolicy,
    thinking_policy: &mut TurnThinkingPolicy,
    reasoning_preference: &mut Option<ReasoningPreference>,
    backend_reasoning_policy: &mut Option<BackendReasoningPolicyV1>,
    override_entry: &AgentOverride,
) {
    if let Some(provider) = &override_entry.provider {
        model_policy.provider = Some(provider.clone());
    }
    if let Some(model) = &override_entry.model {
        model_policy.model = Some(model.clone());
    }
    if let Some(level) = &override_entry.thinking_level {
        thinking_policy.enabled = true;
        thinking_policy.level = Some(level.clone());
    }
    if let Some(preference) = &override_entry.reasoning_preference {
        *reasoning_preference = Some(preference.clone());
        *backend_reasoning_policy = override_entry.backend_reasoning_policy.clone();
    }
}

fn apply_reasoning_override(
    reasoning_preference: &mut Option<ReasoningPreference>,
    backend_reasoning_policy: &mut Option<BackendReasoningPolicyV1>,
    state: &ChannelSessionState,
) {
    if let Some(preference) = &state.reasoning_preference {
        *reasoning_preference = Some(preference.clone());
        *backend_reasoning_policy = state.backend_reasoning_policy.clone();
    } else if state.thinking_enabled || state.thinking_level.is_some() {
        // A session-scoped legacy /think write is newer and more specific than
        // any agent-global backend preference. It masks the inherited backend
        // state without ever promoting legacy thinkingLevel into wire effort.
        *reasoning_preference = None;
        *backend_reasoning_policy = None;
    }
}

#[cfg(test)]
mod reasoning_override_precedence_tests {
    use super::*;

    #[test]
    fn legacy_session_think_masks_inherited_backend_preference_without_migration() {
        let mut preference = Some(ReasoningPreference::explicit("max").unwrap());
        let mut policy = None;
        let state = ChannelSessionState {
            schema: "agent-harness.channel-session.v1".to_string(),
            platform: "telegram".to_string(),
            account_id: None,
            channel_id: "dm".to_string(),
            user_id: "user".to_string(),
            active_session_key: "telegram:dm:user:main".to_string(),
            agent_id: Some("main".to_string()),
            config_revision: None,
            provider: None,
            model: None,
            session_topic: None,
            model_override: None,
            model_override_provider: None,
            model_override_model: None,
            thinking_enabled: true,
            thinking_level: Some("low".to_string()),
            thinking_instruction: None,
            reasoning_preference: None,
            backend_reasoning_policy: None,
            fast_mode: None,
            stop_requested: false,
            stop_reason: None,
            steering_notes: Vec::new(),
            btw_notes: Vec::new(),
            last_command: None,
            updated_at_ms: 1,
        };

        apply_reasoning_override(&mut preference, &mut policy, &state);

        assert_eq!(preference, None);
        assert_eq!(policy, None);
    }
}

fn apply_thinking_override(thinking_policy: &mut TurnThinkingPolicy, state: &ChannelSessionState) {
    if state.thinking_enabled || state.thinking_level.is_some() {
        thinking_policy.enabled = true;
        thinking_policy.level = Some(
            state
                .thinking_level
                .clone()
                .unwrap_or_else(|| DEFAULT_THINKING_LEVEL.to_string()),
        );
    }
}

fn channel_state_query_text(message: &str, state: Option<&ChannelSessionState>) -> String {
    let Some(state) = state else {
        return message.to_string();
    };
    let mut text = message.to_string();
    if let Some(level) = &state.thinking_level {
        text.push_str("\nthink_level: ");
        text.push_str(level);
    }
    if let Some(instruction) = &state.thinking_instruction {
        text.push_str("\nthink: ");
        text.push_str(instruction);
    }
    for note in state.steering_notes.iter().rev().take(6).rev() {
        text.push_str("\nsteer: ");
        text.push_str(&note.text);
    }
    for note in state.btw_notes.iter().rev().take(6).rev() {
        text.push_str("\nbtw: ");
        text.push_str(&note.text);
    }
    text
}

fn prompt_files(workspace: &Path, warnings: &mut Vec<String>) -> io::Result<Vec<TurnPromptFile>> {
    let mut files = Vec::new();
    for name in PROMPT_FILE_NAMES {
        let canonical_path = workspace.join(name);
        let canonical_metadata = prompt_file_metadata(&canonical_path)?;
        let alias = match *name {
            "AGENTS.md" => Some("AGENT.md"),
            "BOOTSTRAP.md" => Some("BOOT.md"),
            _ => None,
        };
        let alias_path = alias.map(|alias| workspace.join(alias));
        let alias_metadata = alias_path
            .as_deref()
            .map(prompt_file_metadata)
            .transpose()?
            .flatten();
        if canonical_metadata.is_some() && alias_metadata.is_some() {
            warnings.push(format!(
                "prompt alias conflict in {}: canonical `{name}` wins and `{}` is ignored",
                workspace.display(),
                alias.unwrap_or_default()
            ));
        }
        let (path, metadata) = if canonical_metadata.is_some() {
            (canonical_path, canonical_metadata)
        } else if alias_metadata.is_some() {
            (
                alias_path.expect("alias path exists when alias metadata exists"),
                alias_metadata,
            )
        } else {
            (canonical_path, None)
        };
        files.push(TurnPromptFile {
            name: (*name).to_string(),
            path,
            exists: metadata.is_some(),
            bytes: metadata.map(|metadata| metadata.len()),
        });
    }
    Ok(files)
}

fn prompt_file_metadata(path: &Path) -> io::Result<Option<fs::Metadata>> {
    match fs::metadata(path) {
        Ok(metadata) if metadata.is_file() => Ok(Some(metadata)),
        Ok(_) => Ok(None),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn prompt_files_for_source(
    source: &AgentSource,
    warnings: &mut Vec<String>,
) -> io::Result<(PathBuf, Vec<TurnPromptFile>)> {
    let files = prompt_files(&source.workspace, warnings)?;
    if files.iter().any(|file| file.exists) {
        return Ok((source.workspace.clone(), files));
    }

    let imported_workspace = source.home.join("workspace");
    if imported_workspace != source.workspace && imported_workspace.is_dir() {
        let imported_files = prompt_files(&imported_workspace, warnings)?;
        if imported_files.iter().any(|file| file.exists) {
            warnings.push(format!(
                "using imported source workspace {} for prompt files because queued workspace {} has no prompt files",
                imported_workspace.display(),
                source.workspace.display()
            ));
            return Ok((imported_workspace, imported_files));
        }
    }

    Ok((source.workspace.clone(), files))
}

fn prompt_files_for_selected_agent(
    source: &AgentSource,
    agent: Option<&AgentProfile>,
    warnings: &mut Vec<String>,
) -> io::Result<(PathBuf, Vec<TurnPromptFile>)> {
    let Some(agent) = agent.filter(|agent| is_independent_agent(agent)) else {
        return prompt_files_for_source(source, warnings);
    };

    let workspace = independent_agent_workspace(source, agent);
    let files = prompt_files(&workspace, warnings)?;
    if !files.iter().any(|file| file.exists) {
        warnings.push(format!(
            "independent agent `{}` has no prompt files in isolated workspace {}; shared source workspace {} was not used",
            agent.id,
            workspace.display(),
            source.workspace.display()
        ));
    }
    Ok((workspace, files))
}

fn is_independent_agent(agent: &AgentProfile) -> bool {
    agent.id != "main"
}

fn independent_agent_workspace(source: &AgentSource, agent: &AgentProfile) -> PathBuf {
    if let Some(workspace) = agent
        .workspace
        .as_deref()
        .and_then(independent_workspace_value)
    {
        let workspace = PathBuf::from(workspace);
        let candidate = if workspace.is_absolute() {
            workspace
        } else {
            source.home.join(workspace)
        };
        if candidate != source.workspace {
            return candidate;
        }
    }

    agent.directory.join("workspace")
}

fn skill_source_for_agent(source: &AgentSource, agent: Option<&AgentProfile>) -> AgentSource {
    let Some(agent) = agent else {
        return source.clone();
    };
    if !is_independent_agent(agent) {
        return source.clone();
    }
    AgentSource::with_workspace(
        agent.directory.clone(),
        independent_agent_workspace(source, agent),
    )
}

fn independent_workspace_value(value: &str) -> Option<&str> {
    let value = value.trim();
    if value.is_empty() { None } else { Some(value) }
}

fn session_key(platform: &str, channel_id: &str, user_id: &str, agent_id: Option<&str>) -> String {
    format!(
        "{}:{}:{}:{}",
        normalize_key_part(platform),
        normalize_key_part(channel_id),
        normalize_key_part(user_id),
        normalize_key_part(agent_id.unwrap_or("unassigned"))
    )
}

pub(crate) fn session_key_for_account(
    platform: &str,
    account_id: &str,
    channel_id: &str,
    user_id: &str,
    agent_id: Option<&str>,
) -> String {
    let base = session_key(platform, channel_id, user_id, agent_id);
    bind_session_key_to_account(&base, platform, account_id, channel_id, user_id, agent_id)
}

pub(crate) fn bind_session_key_to_account(
    session_key: &str,
    platform: &str,
    account_id: &str,
    channel_id: &str,
    user_id: &str,
    agent_id: Option<&str>,
) -> String {
    bind_session_key_to_account_checked(
        session_key,
        platform,
        account_id,
        channel_id,
        user_id,
        agent_id,
    )
    .unwrap_or_else(|_| session_key.to_string())
}

fn bind_session_key_to_account_checked(
    session_key: &str,
    platform: &str,
    account_id: &str,
    channel_id: &str,
    user_id: &str,
    agent_id: Option<&str>,
) -> io::Result<String> {
    CanonicalChannelSessionKey::bind_for_lane(
        session_key,
        platform,
        account_id,
        channel_id,
        user_id,
        agent_id.unwrap_or("unassigned"),
    )
    .map(|key| key.canonical_string())
    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
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
        SkillDeliveryMode, SkillSourceKind, SkillUsageAction, SkillUsageEventOptions,
        build_runtime_skill_index, build_source_skill_index, load_agent_registry,
        record_skill_usage_event,
    };
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn account_binding_is_idempotent_for_account_bound_root() {
        let bound = bind_session_key_to_account(
            "synthetic-root",
            "telegram",
            "account-a",
            "channel-a",
            "user-a",
            Some("main"),
        );
        assert_eq!(
            bind_session_key_to_account(
                &bound,
                "telegram",
                "account-a",
                "channel-a",
                "user-a",
                Some("main"),
            ),
            bound
        );
    }

    #[test]
    fn account_binding_is_idempotent_for_continuation() {
        let bound = bind_session_key_to_account(
            "synthetic-root",
            "telegram",
            "account-a",
            "channel-a",
            "user-a",
            Some("main"),
        );
        let continuation = crate::context_rollover::continuation_session_key(&bound, 1);
        let rebound = bind_session_key_to_account(
            &continuation,
            "telegram",
            "account-a",
            "channel-a",
            "user-a",
            Some("main"),
        );

        assert_eq!(rebound, continuation);
        assert_eq!(
            crate::context_rollover::root_working_session_key(&rebound),
            bound
        );
        assert_eq!(
            crate::context_rollover::continuation_index_from_session_key(&rebound),
            Some(1)
        );
    }

    #[test]
    fn turn_plan_routes_message_to_agent_and_skills() {
        let root = temp_root("turn_plan_routes_message_to_agent_and_skills");
        let source = write_turn_source(&root);
        let skill = source.workspace.join("skills").join("memory-cron");
        fs::create_dir_all(&skill).unwrap();
        fs::write(
            skill.join(crate::SKILL_FILE_NAME),
            "# Memory Cron\n\nRepair openclaw-mem cron jobs.",
        )
        .unwrap();
        let registry = load_agent_registry(&source).unwrap();
        let skills = build_source_skill_index(&source).unwrap();

        let plan = build_turn_plan(
            &source,
            &registry,
            &skills,
            TurnPlanInput {
                harness_home: None,
                platform: "telegram".to_string(),
                channel_id: "dm-42".to_string(),
                user_id: "user-7".to_string(),
                text: "please repair memory cron jobs".to_string(),
                inbound_context: None,
                inbound_media_artifacts: Vec::new(),
                requested_agent_id: Some("main".to_string()),
                session_hint: None,
                skill_limit: 3,
            },
        )
        .unwrap();

        assert_eq!(plan.dispatch, TurnDispatch::AgentTurn);
        assert_eq!(plan.agent.as_ref().unwrap().id, "main");
        assert_eq!(plan.model_policy.provider.as_deref(), Some("openai"));
        assert_eq!(plan.model_policy.model.as_deref(), Some("gpt-5"));
        assert_eq!(plan.session_key, "telegram:dm-42:user-7:main");
        assert!(
            plan.prompt_files
                .iter()
                .any(|file| file.name == "AGENTS.md" && file.exists)
        );
        assert_eq!(plan.selected_skills[0].skill_id, "workspace:memory-cron");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn turn_plan_parses_commands_before_agent_dispatch() {
        let root = temp_root("turn_plan_parses_commands_before_agent_dispatch");
        let source = write_turn_source(&root);
        let registry = load_agent_registry(&source).unwrap();
        let skills = build_source_skill_index(&source).unwrap();

        let plan = build_turn_plan(
            &source,
            &registry,
            &skills,
            TurnPlanInput {
                harness_home: None,
                platform: "discord".to_string(),
                channel_id: "dm#42".to_string(),
                user_id: "user#7".to_string(),
                text: "/status cron".to_string(),
                inbound_context: None,
                inbound_media_artifacts: Vec::new(),
                requested_agent_id: Some("main".to_string()),
                session_hint: None,
                skill_limit: 3,
            },
        )
        .unwrap();

        assert_eq!(plan.dispatch, TurnDispatch::ChannelCommand);
        assert_eq!(
            plan.command_intent,
            Some(ChannelCommandIntent::ShowStatus {
                scope: Some("cron".to_string())
            })
        );
        assert!(plan.selected_skills.is_empty());
        assert_eq!(plan.session_key, "discord:dm_42:user_7:main");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn turn_plan_parses_unknown_slash_command_before_agent_dispatch() {
        let root = temp_root("turn_plan_parses_unknown_slash_command_before_agent_dispatch");
        let source = write_turn_source(&root);
        let registry = load_agent_registry(&source).unwrap();
        let skills = build_source_skill_index(&source).unwrap();

        let plan = build_turn_plan(
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

        assert_eq!(plan.dispatch, TurnDispatch::ChannelCommand);
        assert_eq!(
            plan.command_intent,
            Some(ChannelCommandIntent::UnknownCommand {
                name: "unknown".to_string(),
                rest: Some("value".to_string())
            })
        );
        assert!(plan.selected_skills.is_empty());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn turn_plan_falls_back_to_imported_workspace_for_prompt_files() {
        let root = temp_root("turn_plan_falls_back_to_imported_workspace_for_prompt_files");
        let source = write_turn_source(&root);
        let runtime_workspace = root.join("runtime-workspace");
        fs::create_dir_all(&runtime_workspace).unwrap();
        let drift_source = AgentSource::with_workspace(source.home.clone(), runtime_workspace);
        let registry = load_agent_registry(&drift_source).unwrap();
        let skills = build_source_skill_index(&drift_source).unwrap();

        let plan = build_turn_plan(
            &drift_source,
            &registry,
            &skills,
            TurnPlanInput {
                harness_home: None,
                platform: "telegram".to_string(),
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                text: "/status".to_string(),
                inbound_context: None,
                inbound_media_artifacts: Vec::new(),
                requested_agent_id: Some("main".to_string()),
                session_hint: None,
                skill_limit: 3,
            },
        )
        .unwrap();

        assert_eq!(plan.source_workspace, source.workspace);
        assert_eq!(prompt_files_present_for_test(&plan), 1);
        assert!(
            plan.warnings
                .iter()
                .any(|warning| warning.contains("using imported source workspace"))
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn turn_plan_uses_independent_agent_workspace_for_prompt_files() {
        let root = temp_root("turn_plan_uses_independent_agent_workspace_for_prompt_files");
        let source = write_turn_source(&root);
        let agent_workspace = add_directory_agent(&source, "xiaoxiaoli");
        fs::create_dir_all(&agent_workspace).unwrap();
        fs::write(
            agent_workspace.join("AGENTS.md"),
            "# Xiaoxiaoli\n\nIndependent prompt.",
        )
        .unwrap();
        let main_skill = source.workspace.join("skills").join("main-private");
        let xiaoxiaoli_skill = agent_workspace.join("skills").join("xiaoxiaoli-private");
        fs::create_dir_all(&main_skill).unwrap();
        fs::create_dir_all(&xiaoxiaoli_skill).unwrap();
        fs::write(
            main_skill.join(crate::SKILL_FILE_NAME),
            "# Main Private\n\nHandle isolated routing skill.",
        )
        .unwrap();
        fs::write(
            xiaoxiaoli_skill.join(crate::SKILL_FILE_NAME),
            "# Xiaoxiaoli Private\n\nHandle isolated routing skill.",
        )
        .unwrap();
        let registry = load_agent_registry(&source).unwrap();
        let skills = build_source_skill_index(&source).unwrap();

        let plan = build_turn_plan(
            &source,
            &registry,
            &skills,
            TurnPlanInput {
                harness_home: None,
                platform: "telegram".to_string(),
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                text: "handle isolated routing skill".to_string(),
                inbound_context: None,
                inbound_media_artifacts: Vec::new(),
                requested_agent_id: Some("xiaoxiaoli".to_string()),
                session_hint: None,
                skill_limit: 3,
            },
        )
        .unwrap();

        assert_eq!(plan.agent.as_ref().unwrap().id, "xiaoxiaoli");
        assert_eq!(plan.source_workspace, agent_workspace);
        assert_eq!(prompt_files_present_for_test(&plan), 1);
        assert!(plan.prompt_files.iter().any(|file| file.name == "AGENTS.md"
            && file.exists
            && file.path == agent_workspace.join("AGENTS.md")));
        assert!(
            !plan
                .prompt_files
                .iter()
                .any(|file| file.path.starts_with(&source.workspace))
        );
        assert!(
            plan.selected_skills
                .iter()
                .any(|skill| skill.skill_id == "workspace:xiaoxiaoli-private")
        );
        assert!(
            plan.selected_skills
                .iter()
                .all(|skill| skill.skill_id != "workspace:main-private")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn turn_plan_skips_shared_prompt_files_for_independent_agent_without_workspace_files() {
        let root = temp_root(
            "turn_plan_skips_shared_prompt_files_for_independent_agent_without_workspace_files",
        );
        let source = write_turn_source(&root);
        let agent_workspace = add_directory_agent(&source, "xiaoxiaoli");
        let registry = load_agent_registry(&source).unwrap();
        let skills = build_source_skill_index(&source).unwrap();

        let plan = build_turn_plan(
            &source,
            &registry,
            &skills,
            TurnPlanInput {
                harness_home: None,
                platform: "telegram".to_string(),
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                text: "hello".to_string(),
                inbound_context: None,
                inbound_media_artifacts: Vec::new(),
                requested_agent_id: Some("xiaoxiaoli".to_string()),
                session_hint: None,
                skill_limit: 3,
            },
        )
        .unwrap();

        assert_eq!(plan.agent.as_ref().unwrap().id, "xiaoxiaoli");
        assert_eq!(plan.source_workspace, agent_workspace);
        assert_eq!(prompt_files_present_for_test(&plan), 0);
        assert!(
            plan.prompt_files
                .iter()
                .all(|file| !file.exists && file.path.starts_with(&agent_workspace))
        );
        assert!(
            plan.warnings
                .iter()
                .any(|warning| warning.contains("independent agent `xiaoxiaoli`"))
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn configured_non_main_agent_without_directory_never_inherits_main_prompt_files() {
        let root = temp_root("configured_non_main_without_directory_prompt_isolation");
        let source = write_turn_source(&root);
        fs::write(
            source.workspace.join("MEMORY.md"),
            "MAIN-ONLY-MEMORY-MARKER",
        )
        .unwrap();
        let registry = load_agent_registry(&source).unwrap();
        let skills = build_source_skill_index(&source).unwrap();

        let plan = build_turn_plan(
            &source,
            &registry,
            &skills,
            TurnPlanInput {
                harness_home: None,
                platform: "telegram".to_string(),
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                text: "hello".to_string(),
                inbound_context: None,
                inbound_media_artifacts: Vec::new(),
                requested_agent_id: Some("other".to_string()),
                session_hint: None,
                skill_limit: 3,
            },
        )
        .unwrap();

        let isolated_workspace = source.home.join("agents").join("other").join("workspace");
        assert_eq!(plan.agent.as_ref().unwrap().id, "other");
        assert_eq!(plan.source_workspace, isolated_workspace);
        assert_eq!(prompt_files_present_for_test(&plan), 0);
        assert!(
            plan.prompt_files
                .iter()
                .all(|file| !file.exists && file.path.starts_with(&isolated_workspace))
        );
        assert!(
            plan.warnings
                .iter()
                .any(|warning| warning.contains("independent agent `other`"))
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn multi_agent_skill_matrix_isolates_workspaces_allowlists_and_usage_priors() {
        let root = temp_root("multi_agent_skill_matrix_isolates_skills");
        let source = write_turn_source(&root);
        let xiaoxiaoli_workspace = add_directory_agent(&source, "xiaoxiaoli");
        let other_workspace = source.home.join("agents").join("other").join("workspace");

        let write_skill = |directory: PathBuf, body: &str| {
            fs::create_dir_all(&directory).unwrap();
            fs::write(directory.join(crate::SKILL_FILE_NAME), body).unwrap();
        };
        write_skill(
            source.workspace.join("skills").join("main-private"),
            "# Main Private\n\nHandle isolation operations.",
        );
        write_skill(
            xiaoxiaoli_workspace
                .join("skills")
                .join("xiaoxiaoli-private"),
            "# Xiaoxiaoli Private\n\nHandle isolation operations.",
        );
        write_skill(
            other_workspace.join("skills").join("other-private"),
            "# Other Private\n\nHandle isolation operations.",
        );
        let imported_root = source
            .home
            .join("skills")
            .join("openclaw-imports")
            .join("workspace");
        write_skill(
            imported_root.join("shared-ops"),
            "# Shared Ops\n\nHandle isolation operations.",
        );
        write_skill(
            imported_root.join("xiaoxiaoli-vibe"),
            "---\nagents: [xiaoxiaoli]\n---\n# Xiaoxiaoli Vibe\n\nHandle isolation operations.",
        );

        record_skill_usage_event(SkillUsageEventOptions {
            harness_home: source.home.clone(),
            action: SkillUsageAction::Injected,
            skill_id: "imported-workspace:shared-ops".to_string(),
            source_kind: Some(SkillSourceKind::ImportedWorkspace),
            source_turn_id: Some("turn-xiaoxiaoli".to_string()),
            runtime_queue_id: Some("queue-xiaoxiaoli".to_string()),
            session_key: Some("discord:dm:user:xiaoxiaoli".to_string()),
            channel: Some("discord".to_string()),
            agent_id: Some("xiaoxiaoli".to_string()),
            delivery_mode: Some(SkillDeliveryMode::InjectedBody),
            body_checksum: None,
            selection_receipt_id: None,
            reason: Some("multi-agent isolation regression".to_string()),
            now_ms: 42,
        })
        .unwrap();

        let registry = load_agent_registry(&source).unwrap();
        let plan_for = |agent_id: &str| {
            let agent = registry.agents.iter().find(|agent| agent.id == agent_id);
            let skill_source = skill_source_for_agent(&source, agent);
            let skill_index = build_runtime_skill_index(&skill_source, &source.home).unwrap();
            build_turn_plan(
                &source,
                &registry,
                &skill_index,
                TurnPlanInput {
                    harness_home: Some(source.home.clone()),
                    platform: "discord".to_string(),
                    channel_id: "dm".to_string(),
                    user_id: "user".to_string(),
                    text: "handle isolation operations".to_string(),
                    inbound_context: None,
                    inbound_media_artifacts: Vec::new(),
                    requested_agent_id: Some(agent_id.to_string()),
                    session_hint: None,
                    skill_limit: 20,
                },
            )
            .unwrap()
        };

        let main = plan_for("main");
        let main_ids = main
            .selected_skills
            .iter()
            .map(|skill| skill.skill_id.as_str())
            .collect::<Vec<_>>();
        assert!(main_ids.contains(&"workspace:main-private"));
        assert!(main_ids.contains(&"imported-workspace:shared-ops"));
        assert!(!main_ids.contains(&"workspace:xiaoxiaoli-private"));
        assert!(!main_ids.contains(&"workspace:other-private"));
        assert!(!main_ids.contains(&"imported-workspace:xiaoxiaoli-vibe"));
        assert!(
            main.selected_skills
                .iter()
                .find(|skill| skill.skill_id == "imported-workspace:shared-ops")
                .unwrap()
                .score_components
                .iter()
                .all(|component| component.name != "usage-prior")
        );

        let xiaoxiaoli = plan_for("xiaoxiaoli");
        let xiaoxiaoli_ids = xiaoxiaoli
            .selected_skills
            .iter()
            .map(|skill| skill.skill_id.as_str())
            .collect::<Vec<_>>();
        assert!(xiaoxiaoli_ids.contains(&"workspace:xiaoxiaoli-private"));
        assert!(xiaoxiaoli_ids.contains(&"imported-workspace:shared-ops"));
        assert!(xiaoxiaoli_ids.contains(&"imported-workspace:xiaoxiaoli-vibe"));
        assert!(!xiaoxiaoli_ids.contains(&"workspace:main-private"));
        assert!(!xiaoxiaoli_ids.contains(&"workspace:other-private"));
        assert!(
            xiaoxiaoli
                .selected_skills
                .iter()
                .find(|skill| skill.skill_id == "imported-workspace:shared-ops")
                .unwrap()
                .score_components
                .iter()
                .any(|component| component.name == "usage-prior")
        );

        let other = plan_for("other");
        let other_ids = other
            .selected_skills
            .iter()
            .map(|skill| skill.skill_id.as_str())
            .collect::<Vec<_>>();
        assert!(other_ids.contains(&"workspace:other-private"));
        assert!(other_ids.contains(&"imported-workspace:shared-ops"));
        assert!(!other_ids.contains(&"workspace:main-private"));
        assert!(!other_ids.contains(&"workspace:xiaoxiaoli-private"));
        assert!(!other_ids.contains(&"imported-workspace:xiaoxiaoli-vibe"));

        assert_eq!(main.source_workspace, source.workspace);
        assert_eq!(xiaoxiaoli.source_workspace, xiaoxiaoli_workspace);
        assert_eq!(other.source_workspace, other_workspace);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn unsafe_config_agent_id_is_rejected_before_turn_can_read_outside_prompt_files() {
        let root = temp_root("unsafe_config_agent_id_prompt_isolation");
        let home = root.join(".openclaw");
        let source_workspace = home.join("workspace");
        let outside_agent = root.join("outside-agent");
        let outside_workspace = outside_agent.join("workspace");
        fs::create_dir_all(&source_workspace).unwrap();
        fs::create_dir_all(&outside_workspace).unwrap();
        fs::write(
            outside_workspace.join("MEMORY.md"),
            "OUTSIDE-AGENT-MEMORY-MUST-NOT-BE-READ",
        )
        .unwrap();
        fs::write(
            home.join("openclaw.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "agents": {
                    "list": [{
                        "id": outside_agent.display().to_string(),
                        "enabled": true
                    }]
                }
            }))
            .unwrap(),
        )
        .unwrap();
        let source = AgentSource::with_workspace(home, source_workspace);

        let error = load_agent_registry(&source).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            error
                .to_string()
                .contains("agentId must be a single safe path component"),
            "unexpected validation error: {error}"
        );
        assert_eq!(
            fs::read_to_string(outside_workspace.join("MEMORY.md")).unwrap(),
            "OUTSIDE-AGENT-MEMORY-MUST-NOT-BE-READ"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn turn_plan_warns_and_falls_back_when_requested_agent_is_missing() {
        let root = temp_root("turn_plan_warns_and_falls_back_when_requested_agent_is_missing");
        let source = write_turn_source(&root);
        let registry = load_agent_registry(&source).unwrap();
        let skills = build_source_skill_index(&source).unwrap();

        let plan = build_turn_plan(
            &source,
            &registry,
            &skills,
            TurnPlanInput {
                harness_home: None,
                platform: "telegram".to_string(),
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                text: "hello".to_string(),
                inbound_context: None,
                inbound_media_artifacts: Vec::new(),
                requested_agent_id: Some("missing-agent".to_string()),
                session_hint: None,
                skill_limit: 3,
            },
        )
        .unwrap();

        assert_eq!(plan.agent.as_ref().unwrap().id, "main");
        assert!(
            plan.warnings
                .iter()
                .any(|warning| warning.contains("missing-agent"))
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn turn_plan_uses_explicit_session_hint() {
        let root = temp_root("turn_plan_uses_explicit_session_hint");
        let source = write_turn_source(&root);
        let registry = load_agent_registry(&source).unwrap();
        let skills = build_source_skill_index(&source).unwrap();

        let plan = build_turn_plan(
            &source,
            &registry,
            &skills,
            TurnPlanInput {
                harness_home: None,
                platform: "telegram".to_string(),
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                text: "hello".to_string(),
                inbound_context: None,
                inbound_media_artifacts: Vec::new(),
                requested_agent_id: None,
                session_hint: Some("imported-session-key".to_string()),
                skill_limit: 3,
            },
        )
        .unwrap();

        assert_eq!(plan.session_key, "imported-session-key");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn turn_plan_applies_channel_state_session_model_and_skill_context() {
        let root = temp_root("turn_plan_applies_channel_state_session_model_and_skill_context");
        let source = write_turn_source(&root);
        let skill = source.workspace.join("skills").join("openrouter-routing");
        fs::create_dir_all(&skill).unwrap();
        fs::write(
            skill.join(crate::SKILL_FILE_NAME),
            "# OpenRouter Routing\n\nUse this when steering mentions OpenRouter.",
        )
        .unwrap();
        let harness_home = root.join(".agent-harness");
        write_channel_state(
            &harness_home,
            r#"{
              "schema": "agent-harness.channel-session-state.v1",
              "platform": "telegram",
              "channelId": "dm",
              "userId": "user",
              "activeSessionKey": "telegram:dm:user:main:new",
              "agentId": "main",
              "provider": "openai",
              "model": "gpt-5",
              "sessionTopic": "routing",
              "modelOverride": "openrouter/anthropic/claude-sonnet-4",
              "modelOverrideProvider": "openrouter",
              "modelOverrideModel": "anthropic/claude-sonnet-4",
              "thinkingEnabled": true,
              "thinkingLevel": "medium",
              "thinkingInstruction": "compare provider constraints",
              "stopRequested": false,
              "stopReason": null,
              "steeringNotes": [
                { "atMs": 1000, "text": "prefer OpenRouter routing skill" }
              ],
              "btwNotes": [],
              "lastCommand": "steer",
              "updatedAtMs": 1000
            }"#,
        );
        let registry = load_agent_registry(&source).unwrap();
        let skills = build_source_skill_index(&source).unwrap();
        write_codex_models_cache(&harness_home);

        let plan = build_turn_plan(
            &source,
            &registry,
            &skills,
            TurnPlanInput {
                harness_home: Some(harness_home),
                platform: "telegram".to_string(),
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                text: "continue".to_string(),
                inbound_context: None,
                inbound_media_artifacts: Vec::new(),
                requested_agent_id: Some("main".to_string()),
                session_hint: None,
                skill_limit: 3,
            },
        )
        .unwrap();

        assert_eq!(plan.session_key, "telegram:dm:user:main:new");
        assert_eq!(plan.model_policy.provider.as_deref(), Some("openrouter"));
        assert_eq!(
            plan.model_policy.model.as_deref(),
            Some("anthropic/claude-sonnet-4")
        );
        assert!(plan.thinking_policy.enabled);
        assert_eq!(plan.thinking_policy.level.as_deref(), Some("medium"));
        assert!(plan.channel_state.is_some());
        assert_eq!(
            plan.selected_skills[0].skill_id,
            "workspace:openrouter-routing"
        );
        assert_eq!(plan.provider_request_policy.fast_mode, "normal");
        assert_eq!(
            plan.provider_request_policy.effective_acceleration,
            "disabled"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn turn_plan_applies_fast_mode_as_provider_request_policy() {
        let root = temp_root("turn_plan_applies_fast_mode_as_provider_request_policy");
        let source = write_turn_source(&root);
        let harness_home = root.join(".agent-harness");
        write_agent_overrides(
            &harness_home,
            r#"{
              "schema": "agent-harness.agent-overrides.v1",
              "agents": {
                "main": {
                  "fastMode": "normal",
                  "updatedAtMs": 999
                }
              }
            }"#,
        );
        write_channel_state(
            &harness_home,
            r#"{
              "schema": "agent-harness.channel-session-state.v1",
              "platform": "telegram",
              "channelId": "dm",
              "userId": "user",
              "activeSessionKey": "telegram:dm:user:main:session-1",
              "agentId": "main",
              "provider": "openai",
              "model": "gpt-5.5",
              "modelOverride": "openai/gpt-5.5",
              "modelOverrideProvider": "openai",
              "modelOverrideModel": "gpt-5.5",
              "fastMode": "fast",
              "thinkingEnabled": true,
              "thinkingLevel": "medium",
              "steeringNotes": [],
              "btwNotes": [],
              "updatedAtMs": 1000
            }"#,
        );
        let registry = load_agent_registry(&source).unwrap();
        let skills = build_source_skill_index(&source).unwrap();
        write_codex_models_cache(&harness_home);

        let plan = build_turn_plan(
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
                requested_agent_id: Some("main".to_string()),
                session_hint: None,
                skill_limit: 3,
            },
        )
        .unwrap();

        assert_eq!(plan.dispatch, TurnDispatch::AgentTurn);
        assert_eq!(plan.provider_request_policy.fast_mode, "fast");
        assert_eq!(
            plan.provider_request_policy.effective_acceleration,
            "enabled"
        );
        assert_eq!(
            plan.provider_request_policy.service_tier.as_deref(),
            Some("priority")
        );
        assert_eq!(plan.provider_request_policy.route_kind, "codex-app-server");
        assert!(
            plan.provider_request_policy
                .reason
                .contains("serviceTier=priority")
        );

        let other_plan = build_turn_plan(
            &source,
            &registry,
            &skills,
            TurnPlanInput {
                harness_home: Some(harness_home),
                platform: "telegram".to_string(),
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                text: "continue".to_string(),
                inbound_context: None,
                inbound_media_artifacts: Vec::new(),
                requested_agent_id: Some("other".to_string()),
                session_hint: None,
                skill_limit: 3,
            },
        )
        .unwrap();
        assert_eq!(other_plan.provider_request_policy.fast_mode, "normal");
        assert_eq!(
            other_plan.provider_request_policy.effective_acceleration,
            "disabled"
        );
        assert_eq!(other_plan.provider_request_policy.service_tier, None);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn turn_plan_uses_global_fast_mode_when_session_has_no_override() {
        let root = temp_root("turn_plan_uses_global_fast_mode_when_session_has_no_override");
        let source = write_turn_source(&root);
        let harness_home = root.join(".agent-harness");
        write_agent_overrides(
            &harness_home,
            r#"{
              "schema": "agent-harness.agent-overrides.v1",
              "agents": {
                "main": {
                  "fastMode": "fast",
                  "updatedAtMs": 1000
                }
              }
            }"#,
        );
        let registry = load_agent_registry(&source).unwrap();
        let skills = build_source_skill_index(&source).unwrap();

        let plan = build_turn_plan(
            &source,
            &registry,
            &skills,
            TurnPlanInput {
                harness_home: Some(harness_home),
                platform: "telegram".to_string(),
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                text: "continue".to_string(),
                inbound_context: None,
                inbound_media_artifacts: Vec::new(),
                requested_agent_id: Some("main".to_string()),
                session_hint: None,
                skill_limit: 3,
            },
        )
        .unwrap();

        assert_eq!(plan.provider_request_policy.fast_mode, "fast");
        assert_eq!(
            plan.provider_request_policy.effective_acceleration,
            "unsupported"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn turn_plan_ignores_channel_state_session_for_different_agent() {
        let root = temp_root("turn_plan_ignores_channel_state_session_for_different_agent");
        let source = write_turn_source(&root);
        let harness_home = root.join(".agent-harness");
        write_channel_state(
            &harness_home,
            r#"{
              "schema": "agent-harness.channel-session-state.v1",
              "platform": "telegram",
              "channelId": "dm",
              "userId": "user",
              "activeSessionKey": "telegram:dm:user:main:session-1",
              "agentId": "main",
              "provider": "openai",
              "model": "gpt-5",
              "sessionTopic": "main work",
              "modelOverride": "openai/gpt-5",
              "modelOverrideProvider": "openai",
              "modelOverrideModel": "gpt-5",
              "thinkingEnabled": true,
              "thinkingLevel": "high",
              "steeringNotes": [
                { "atMs": 1000, "text": "main-only steering" }
              ],
              "updatedAtMs": 1000
            }"#,
        );
        let registry = load_agent_registry(&source).unwrap();
        let skills = build_source_skill_index(&source).unwrap();

        let plan = build_turn_plan(
            &source,
            &registry,
            &skills,
            TurnPlanInput {
                harness_home: Some(harness_home),
                platform: "telegram".to_string(),
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                text: "hello".to_string(),
                inbound_context: None,
                inbound_media_artifacts: Vec::new(),
                requested_agent_id: Some("other".to_string()),
                session_hint: None,
                skill_limit: 3,
            },
        )
        .unwrap();

        assert_eq!(plan.agent.as_ref().unwrap().id, "other");
        assert_eq!(plan.session_key, "telegram:dm:user:other");
        assert_eq!(plan.model_policy.model.as_deref(), Some("gpt-5.4"));
        assert!(!plan.thinking_policy.enabled);
        assert!(plan.channel_state.is_none());
        assert!(
            plan.warnings
                .iter()
                .any(|warning| warning.contains("ignored channel session state"))
        );

        let _ = fs::remove_dir_all(root);
    }
    #[test]
    fn turn_plan_applies_per_agent_global_overrides() {
        let root = temp_root("turn_plan_applies_per_agent_global_overrides");
        let source = write_turn_source(&root);
        let harness_home = root.join(".agent-harness");
        write_agent_overrides(
            &harness_home,
            r#"{
              "schema": "agent-harness.agent-overrides.v1",
              "agents": {
                "main": {
                  "provider": "openrouter",
                  "model": "anthropic/claude-sonnet-4",
                  "thinkingLevel": "high",
                  "updatedAtMs": 1000
                },
                "other": {
                  "provider": "openai",
                  "model": "gpt-5.4-mini",
                  "thinkingLevel": "low",
                  "updatedAtMs": 1000
                }
              }
            }"#,
        );
        let registry = load_agent_registry(&source).unwrap();
        let skills = build_source_skill_index(&source).unwrap();

        let plan = build_turn_plan(
            &source,
            &registry,
            &skills,
            TurnPlanInput {
                harness_home: Some(harness_home),
                platform: "telegram".to_string(),
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                text: "continue".to_string(),
                inbound_context: None,
                inbound_media_artifacts: Vec::new(),
                requested_agent_id: Some("main".to_string()),
                session_hint: None,
                skill_limit: 3,
            },
        )
        .unwrap();

        assert_eq!(plan.model_policy.provider.as_deref(), Some("openrouter"));
        assert_eq!(
            plan.model_policy.model.as_deref(),
            Some("anthropic/claude-sonnet-4")
        );
        assert!(plan.thinking_policy.enabled);
        assert_eq!(plan.thinking_policy.level.as_deref(), Some("high"));

        let other_plan = build_turn_plan(
            &source,
            &registry,
            &skills,
            TurnPlanInput {
                harness_home: Some(root.join(".agent-harness")),
                platform: "telegram".to_string(),
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                text: "continue".to_string(),
                inbound_context: None,
                inbound_media_artifacts: Vec::new(),
                requested_agent_id: Some("other".to_string()),
                session_hint: None,
                skill_limit: 3,
            },
        )
        .unwrap();
        assert_eq!(other_plan.model_policy.provider.as_deref(), Some("openai"));
        assert_eq!(
            other_plan.model_policy.model.as_deref(),
            Some("gpt-5.4-mini")
        );
        assert_eq!(other_plan.thinking_policy.level.as_deref(), Some("low"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prompt_file_aliases_are_fallback_only_and_conflicts_are_deterministic() {
        let root = temp_root("prompt_file_aliases_are_fallback_only");
        let workspace = root.join("workspace");
        fs::create_dir_all(&workspace).unwrap();
        fs::write(workspace.join("AGENT.md"), "legacy singular").unwrap();
        fs::write(workspace.join("BOOT.md"), "legacy bootstrap").unwrap();
        let mut warnings = Vec::new();
        let files = prompt_files(&workspace, &mut warnings).unwrap();
        assert_eq!(
            files
                .iter()
                .find(|file| file.name == "AGENTS.md")
                .unwrap()
                .path,
            workspace.join("AGENT.md")
        );
        assert_eq!(
            files
                .iter()
                .find(|file| file.name == "BOOTSTRAP.md")
                .unwrap()
                .path,
            workspace.join("BOOT.md")
        );
        assert!(warnings.is_empty());

        fs::write(workspace.join("AGENTS.md"), "canonical plural").unwrap();
        fs::write(workspace.join("BOOTSTRAP.md"), "canonical bootstrap").unwrap();
        let mut warnings = Vec::new();
        let files = prompt_files(&workspace, &mut warnings).unwrap();
        assert_eq!(
            files
                .iter()
                .find(|file| file.name == "AGENTS.md")
                .unwrap()
                .path,
            workspace.join("AGENTS.md")
        );
        assert_eq!(warnings.len(), 2);
        assert!(warnings[0].contains("AGENTS.md"));
        assert!(warnings[0].contains("AGENT.md"));
        assert!(warnings[1].contains("BOOTSTRAP.md"));
        assert!(warnings[1].contains("BOOT.md"));

        let _ = fs::remove_dir_all(root);
    }

    fn write_turn_source(root: &Path) -> AgentSource {
        let home = root.join(".openclaw");
        let workspace = home.join("workspace");
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(home.join("agents").join("main").join("sessions")).unwrap();
        fs::write(workspace.join("AGENTS.md"), "# Agent").unwrap();
        fs::write(
            home.join("openclaw.json"),
            r#"{
              "agents": {
                "defaults": { "provider": "openai", "model": "codex" },
                "list": [
                  { "id": "main", "model": "gpt-5", "enabled": true },
                  { "id": "other", "model": "gpt-5.4", "enabled": true }
                ]
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

    fn add_directory_agent(source: &AgentSource, id: &str) -> PathBuf {
        let agent_dir = source.home.join("agents").join(id);
        fs::create_dir_all(agent_dir.join("agent")).unwrap();
        fs::create_dir_all(agent_dir.join("sessions")).unwrap();
        fs::write(agent_dir.join("agent").join("models.json"), "{}").unwrap();
        fs::write(agent_dir.join("sessions").join("sessions.json"), "{}").unwrap();
        agent_dir.join("workspace")
    }

    fn prompt_files_present_for_test(plan: &TurnPlan) -> usize {
        plan.prompt_files.iter().filter(|file| file.exists).count()
    }

    fn write_channel_state(harness_home: &Path, state_json: &str) {
        let state_file = harness_home
            .join("state")
            .join("channels")
            .join("telegram")
            .join("dm")
            .join("user")
            .join("state.json");
        fs::create_dir_all(state_file.parent().unwrap()).unwrap();
        fs::write(state_file, state_json).unwrap();
    }

    fn write_codex_models_cache(harness_home: &Path) {
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
                }
              ]
            }"#,
        )
        .unwrap();
    }

    fn write_agent_overrides(harness_home: &Path, overrides_json: &str) {
        let path = harness_home
            .join("state")
            .join("agents")
            .join("overrides.json");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, overrides_json).unwrap();
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-turns-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
