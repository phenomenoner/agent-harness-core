use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::{
    AgentProfile, AgentRegistry, ChannelCommand, ChannelCommandIntent, ChannelSessionState,
    OpenClawSource, PROMPT_FILE_NAMES, SkillIndex, SkillSelection, SkillSelectionQuery,
    parse_channel_command, read_channel_session_state, select_skills,
};

const TURN_PLAN_SCHEMA: &str = "openclaw-harness.turn-plan.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnPlanInput {
    pub harness_home: Option<PathBuf>,
    pub platform: String,
    pub channel_id: String,
    pub user_id: String,
    pub text: String,
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
    pub session_key: String,
    pub dispatch: TurnDispatch,
    pub agent: Option<TurnAgent>,
    pub model_policy: TurnModelPolicy,
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
    source: &OpenClawSource,
    registry: &AgentRegistry,
    skill_index: &SkillIndex,
    input: TurnPlanInput,
) -> io::Result<TurnPlan> {
    let mut warnings = Vec::new();
    let selected_agent = select_agent(registry, input.requested_agent_id.as_deref(), &mut warnings);
    let agent = selected_agent.map(turn_agent);
    let mut model_policy = selected_agent.map(turn_model_policy).unwrap_or_default();
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
        warnings.push("no OpenClaw agent is available for this turn".to_string());
    }

    let channel_state = load_channel_state(
        input.harness_home.as_deref(),
        &input.platform,
        &input.channel_id,
        &input.user_id,
        &mut warnings,
    );
    if let Some(state) = &channel_state {
        apply_model_override(&mut model_policy, state);
    }
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

    let prompt_files = prompt_files(&source.workspace)?;
    let skill_query_text = channel_state_query_text(&input.text, channel_state.as_ref());
    let selected_skills = if dispatch == TurnDispatch::AgentTurn {
        select_skills(
            skill_index,
            &SkillSelectionQuery {
                text: skill_query_text,
                agent_id: agent.as_ref().map(|agent| agent.id.clone()),
                channel: Some(input.platform.clone()),
                workspace: agent
                    .as_ref()
                    .and_then(|agent| agent.workspace.clone())
                    .or_else(|| Some(source.workspace.display().to_string())),
                limit: input.skill_limit,
            },
        )
    } else {
        Vec::new()
    };
    Ok(TurnPlan {
        schema: TURN_PLAN_SCHEMA,
        harness_home: input.harness_home,
        source_home: source.home.clone(),
        source_workspace: source.workspace.clone(),
        platform: input.platform,
        channel_id: input.channel_id,
        user_id: input.user_id,
        message_text: input.text,
        session_key,
        dispatch,
        agent,
        model_policy,
        channel_state,
        command,
        command_intent,
        prompt_files,
        selected_skills,
        warnings,
    })
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

fn apply_model_override(model_policy: &mut TurnModelPolicy, state: &ChannelSessionState) {
    if let Some(provider) = &state.model_override_provider {
        model_policy.provider = Some(provider.clone());
    }
    if let Some(model) = &state.model_override_model {
        model_policy.model = Some(model.clone());
    }
}

fn channel_state_query_text(message: &str, state: Option<&ChannelSessionState>) -> String {
    let Some(state) = state else {
        return message.to_string();
    };
    let mut text = message.to_string();
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
        let harness_home = root.join(".openclaw-harness");
        write_channel_state(
            &harness_home,
            r#"{
              "schema": "openclaw-harness.channel-session-state.v1",
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
        assert!(plan.channel_state.is_some());
        assert_eq!(
            plan.selected_skills[0].skill_id,
            "workspace:openrouter-routing"
        );

        let _ = fs::remove_dir_all(root);
    }

    fn write_turn_source(root: &Path) -> OpenClawSource {
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
                  { "id": "main", "model": "gpt-5", "enabled": true }
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
        OpenClawSource::with_workspace(home, workspace)
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

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "openclaw-harness-turns-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
