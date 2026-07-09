use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::Value;

use crate::{
    AgentOverride, AgentProfile, AgentRegistry, AgentSource, ChannelCommand, ChannelCommandIntent,
    ChannelSessionState, DEFAULT_THINKING_LEVEL, InboundMediaArtifact, PROMPT_FILE_NAMES,
    SkillIndex, SkillSelection, SkillSelectionQuery, collect_skill_usage_snapshot,
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
    pub channel_state: Option<ChannelSessionState>,
    pub command: Option<ChannelCommand>,
    pub command_intent: Option<ChannelCommandIntent>,
    pub prompt_files: Vec<TurnPromptFile>,
    pub selected_skills: Vec<SkillSelection>,
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
    let mut warnings = Vec::new();
    let selected_agent = select_agent(registry, input.requested_agent_id.as_deref(), &mut warnings);
    let agent = selected_agent.map(turn_agent);
    let mut model_policy = selected_agent.map(turn_model_policy).unwrap_or_default();
    let mut thinking_policy = TurnThinkingPolicy::default();
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
        apply_agent_override(&mut model_policy, &mut thinking_policy, &agent_override);
    }

    let selected_agent_id = agent.as_ref().map(|agent| agent.id.as_str());
    let channel_state = load_channel_state(
        input.harness_home.as_deref(),
        &input.platform,
        &input.channel_id,
        &input.user_id,
        &mut warnings,
    )
    .and_then(|state| channel_state_for_selected_agent(state, selected_agent_id, &mut warnings));
    if let Some(state) = &channel_state {
        apply_model_override(&mut model_policy, state);
        apply_thinking_override(&mut thinking_policy, state);
    }
    let provider_request_policy = provider_request_policy_from_state(
        &model_policy,
        channel_state.as_ref(),
        agent_override.as_ref(),
        input.harness_home.as_deref(),
    );
    let session_key = input
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
        });

    let (prompt_workspace, prompt_files) =
        prompt_files_for_selected_agent(source, selected_agent, &mut warnings)?;
    let skill_query_text = channel_state_query_text(&input.text, channel_state.as_ref());
    let skill_config = input
        .harness_home
        .as_ref()
        .map(|harness_home| load_skill_selection_config(harness_home))
        .transpose()?
        .unwrap_or_default();
    let usage_snapshot = if input.harness_home.is_some() && skill_config.usage_prior_enabled {
        input
            .harness_home
            .as_ref()
            .map(collect_skill_usage_snapshot)
            .transpose()?
    } else {
        None
    };
    let skill_query = SkillSelectionQuery {
        text: skill_query_text,
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
    Ok(TurnPlan {
        schema: TURN_PLAN_SCHEMA,
        harness_home: input.harness_home,
        source_home: source.home.clone(),
        source_workspace: prompt_workspace,
        platform: input.platform,
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
        channel_state,
        command,
        command_intent,
        prompt_files,
        selected_skills,
        warnings,
    })
}

#[derive(Debug, Clone, Copy)]
struct SkillSelectionConfig {
    fts_enabled: bool,
    usage_prior_enabled: bool,
}

impl Default for SkillSelectionConfig {
    fn default() -> Self {
        Self {
            fts_enabled: true,
            usage_prior_enabled: true,
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

fn load_channel_state(
    harness_home: Option<&Path>,
    platform: &str,
    channel_id: &str,
    user_id: &str,
    warnings: &mut Vec<String>,
) -> Option<ChannelSessionState> {
    let harness_home = harness_home?;
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

fn prompt_files(workspace: &Path) -> io::Result<Vec<TurnPromptFile>> {
    let mut files = Vec::new();
    for name in PROMPT_FILE_NAMES {
        let path = workspace.join(name);
        let metadata = match fs::metadata(&path) {
            Ok(metadata) if metadata.is_file() => Some(metadata),
            Ok(_) => None,
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
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

fn prompt_files_for_source(
    source: &AgentSource,
    warnings: &mut Vec<String>,
) -> io::Result<(PathBuf, Vec<TurnPromptFile>)> {
    let files = prompt_files(&source.workspace)?;
    if files.iter().any(|file| file.exists) {
        return Ok((source.workspace.clone(), files));
    }

    let imported_workspace = source.home.join("workspace");
    if imported_workspace != source.workspace && imported_workspace.is_dir() {
        let imported_files = prompt_files(&imported_workspace)?;
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
    let files = prompt_files(&workspace)?;
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
    agent.id != "main" && agent.directory_exists
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
    use crate::{build_source_skill_index, load_agent_registry};
    use std::time::{SystemTime, UNIX_EPOCH};

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
