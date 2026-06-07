use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::{AgentRegistry, ChannelCommandIntent, TurnDispatch, TurnPlan};

const CHANNEL_STEP_SCHEMA: &str = "openclaw-harness.channel-step.v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelStep {
    pub schema: &'static str,
    pub source_home: PathBuf,
    pub source_workspace: PathBuf,
    pub platform: String,
    pub channel_id: String,
    pub user_id: String,
    pub message_text: String,
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
    SetThinkingMode {
        instruction: Option<String>,
    },
    StopCurrentRun {
        reason: Option<String>,
    },
    AddSteering {
        instruction: String,
    },
    AddBtwNote {
        note: String,
    },
    ShowModel {
        agent_id: Option<String>,
        provider: Option<String>,
        model: Option<String>,
    },
    SwitchModel {
        agent_id: Option<String>,
        target: String,
        current_provider: Option<String>,
        current_model: Option<String>,
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
    pub agents_total: usize,
    pub agents_enabled: usize,
    pub providers_total: usize,
    pub plugins_total: usize,
    pub telegram_configured: bool,
    pub discord_configured: bool,
    pub current_agent_id: Option<String>,
    pub current_provider: Option<String>,
    pub current_model: Option<String>,
    pub prompt_files_present: usize,
    pub prompt_files_total: usize,
    pub selected_skills: usize,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelOutboundMessage {
    pub platform: String,
    pub channel_id: String,
    pub user_id: String,
    pub session_key: String,
    pub kind: ChannelOutboundMessageKind,
    pub text: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ChannelOutboundMessageKind {
    CommandReply,
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
            "No OpenClaw agent is available for this channel message.",
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
        channel_id: turn.channel_id.clone(),
        user_id: turn.user_id.clone(),
        message_text: turn.message_text.clone(),
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
        channel_id: turn.channel_id.clone(),
        user_id: turn.user_id.clone(),
        message_text: turn.message_text.clone(),
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
        channel_id: turn.channel_id.clone(),
        user_id: turn.user_id.clone(),
        message_text: turn.message_text.clone(),
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
            new_session_key: format!("{}:new", turn.session_key),
        },
        ChannelCommandIntent::SetThinkingMode { instruction } => {
            ChannelCommandEffect::SetThinkingMode { instruction }
        }
        ChannelCommandIntent::StopCurrentRun { reason } => {
            ChannelCommandEffect::StopCurrentRun { reason }
        }
        ChannelCommandIntent::AddSteering { instruction } => {
            ChannelCommandEffect::AddSteering { instruction }
        }
        ChannelCommandIntent::AddBtwNote { note } => ChannelCommandEffect::AddBtwNote { note },
        ChannelCommandIntent::ShowModel => ChannelCommandEffect::ShowModel {
            agent_id: turn.agent.as_ref().map(|agent| agent.id.clone()),
            provider: turn.model_policy.provider.clone(),
            model: turn.model_policy.model.clone(),
        },
        ChannelCommandIntent::SwitchModel { target } => {
            warnings.push(
                "model switch is planned but not persisted until the runtime state writer is enabled"
                    .to_string(),
            );
            ChannelCommandEffect::SwitchModel {
                agent_id: turn.agent.as_ref().map(|agent| agent.id.clone()),
                target,
                current_provider: turn.model_policy.provider.clone(),
                current_model: turn.model_policy.model.clone(),
            }
        }
        ChannelCommandIntent::ShowStatus { scope } => ChannelCommandEffect::ShowStatus {
            snapshot: status_snapshot(registry, turn, scope.clone()),
            scope,
        },
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
        ChannelCommandEffect::SetThinkingMode { instruction } => instruction
            .as_ref()
            .map(|instruction| format!("Thinking mode updated for this session: {instruction}"))
            .unwrap_or_else(|| "Thinking mode updated for this session.".to_string()),
        ChannelCommandEffect::StopCurrentRun { reason } => reason
            .as_ref()
            .map(|reason| format!("Stop requested for the current run: {reason}"))
            .unwrap_or_else(|| "Stop requested for the current run.".to_string()),
        ChannelCommandEffect::AddSteering { instruction } => {
            format!("Steering note recorded for this session: {instruction}")
        }
        ChannelCommandEffect::AddBtwNote { note } => {
            format!("BTW note recorded for this session: {note}")
        }
        ChannelCommandEffect::ShowModel {
            agent_id,
            provider,
            model,
        } => format!(
            "Current model for agent {}: provider={}, model={}",
            display_opt(agent_id),
            display_opt(provider),
            display_opt(model)
        ),
        ChannelCommandEffect::SwitchModel {
            agent_id,
            target,
            current_provider,
            current_model,
        } => format!(
            "Model switch planned for agent {}: {} (current provider={}, model={})",
            display_opt(agent_id),
            target,
            display_opt(current_provider),
            display_opt(current_model)
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
            "Agents: {}/{} enabled. Current agent: {}.",
            snapshot.agents_enabled,
            snapshot.agents_total,
            display_opt(&snapshot.current_agent_id)
        ),
        Some("channels") => format!(
            "Channels: telegram={}, discord={}.",
            yes_no(snapshot.telegram_configured),
            yes_no(snapshot.discord_configured)
        ),
        Some("model") => format!(
            "Model: agent={}, provider={}, model={}.",
            display_opt(&snapshot.current_agent_id),
            display_opt(&snapshot.current_provider),
            display_opt(&snapshot.current_model)
        ),
        Some("skills") => format!(
            "Selected skills for this turn: {}.",
            snapshot.selected_skills
        ),
        Some("cron") => {
            "Cron status is available through cron-plan and deterministic-cron-plan.".to_string()
        }
        _ => format!(
            "Status: agents {}/{} enabled, providers={}, plugins={}, telegram={}, discord={}, model={}/{}.",
            snapshot.agents_enabled,
            snapshot.agents_total,
            snapshot.providers_total,
            snapshot.plugins_total,
            yes_no(snapshot.telegram_configured),
            yes_no(snapshot.discord_configured),
            display_opt(&snapshot.current_provider),
            display_opt(&snapshot.current_model)
        ),
    }
}

fn status_snapshot(
    registry: &AgentRegistry,
    turn: &TurnPlan,
    scope: Option<String>,
) -> ChannelStatusSnapshot {
    ChannelStatusSnapshot {
        scope,
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
        current_provider: turn.model_policy.provider.clone(),
        current_model: turn.model_policy.model.clone(),
        prompt_files_present: prompt_files_present(turn),
        prompt_files_total: turn.prompt_files.len(),
        selected_skills: turn.selected_skills.len(),
    }
}

fn outbound(
    turn: &TurnPlan,
    kind: ChannelOutboundMessageKind,
    text: String,
) -> ChannelOutboundMessage {
    ChannelOutboundMessage {
        platform: turn.platform.clone(),
        channel_id: turn.channel_id.clone(),
        user_id: turn.user_id.clone(),
        session_key: turn.session_key.clone(),
        kind,
        text,
    }
}

fn prompt_files_present(turn: &TurnPlan) -> usize {
    turn.prompt_files.iter().filter(|file| file.exists).count()
}

fn display_opt(value: &Option<String>) -> &str {
    value.as_deref().unwrap_or("-")
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
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
                platform: "telegram".to_string(),
                channel_id: "dm-42".to_string(),
                user_id: "user-7".to_string(),
                text: "repair memory cron".to_string(),
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
                platform: "discord".to_string(),
                channel_id: "dm#42".to_string(),
                user_id: "user#7".to_string(),
                text: "/status channels".to_string(),
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
        assert!(step.outbound_messages[0].text.contains("telegram=yes"));
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
    fn channel_step_plans_model_switch_without_persisting() {
        let root = temp_root("channel_step_plans_model_switch_without_persisting");
        let source = write_channel_source(&root);
        let registry = load_agent_registry(&source).unwrap();
        let skills = build_source_skill_index(&source).unwrap();
        let turn = build_turn_plan(
            &source,
            &registry,
            &skills,
            TurnPlanInput {
                platform: "telegram".to_string(),
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                text: "/model openrouter/anthropic/claude-sonnet-4".to_string(),
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
                .contains("Model switch planned")
        );
        assert!(
            step.warnings
                .iter()
                .any(|warning| warning.contains("not persisted"))
        );
        assert!(matches!(
            step.command_effect,
            Some(ChannelCommandEffect::SwitchModel { ref target, .. })
                if target == "openrouter/anthropic/claude-sonnet-4"
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
                platform: "telegram".to_string(),
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                text: "/model".to_string(),
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

    fn write_channel_source(root: &Path) -> crate::OpenClawSource {
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
        crate::OpenClawSource::with_workspace(home, workspace)
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "openclaw-harness-channel-runtime-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
