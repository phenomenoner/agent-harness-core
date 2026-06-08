use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::{
    ChannelCommandApplyOptions, ChannelCommandApplyReport, ChannelOutboundMessage,
    ChannelStepAction, OpenClawSource, RuntimeQueueEnqueueOptions, RuntimeQueueEnqueueReport,
    SkillIndex, TurnPlanInput, apply_channel_command_step, build_channel_step, build_turn_plan,
    enqueue_channel_step, load_agent_registry,
};

const CHANNEL_RECEIVE_SCHEMA: &str = "openclaw-harness.channel-receive.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelReceiveOptions {
    pub source: OpenClawSource,
    pub harness_home: PathBuf,
    pub skill_index: SkillIndex,
    pub platform: String,
    pub channel_id: String,
    pub user_id: String,
    pub agent_id: Option<String>,
    pub session_key: Option<String>,
    pub message: String,
    pub skill_limit: usize,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelReceiveReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub platform: String,
    pub channel_id: String,
    pub user_id: String,
    pub session_key: String,
    pub status: ChannelReceiveStatus,
    pub step_action: ChannelStepAction,
    pub command_name: Option<String>,
    pub queue_id: Option<String>,
    pub outbox_file: PathBuf,
    pub receipts_file: PathBuf,
    pub command_apply: Option<ChannelCommandApplyReport>,
    pub queue_enqueue: Option<RuntimeQueueEnqueueReport>,
    pub outbound_messages: Vec<ChannelOutboundMessage>,
    pub receipt: ChannelReceiveReceipt,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ChannelReceiveStatus {
    CommandApplied,
    AgentTurnQueued,
    ErrorReplied,
    Skipped,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelReceiveReceipt {
    pub status: ChannelReceiveStatus,
    pub platform: String,
    pub channel_id: String,
    pub user_id: String,
    pub session_key: String,
    pub queue_id: Option<String>,
    pub outbound_count: usize,
    pub reason: String,
}

pub fn receive_channel_message(options: ChannelReceiveOptions) -> io::Result<ChannelReceiveReport> {
    let channel_state_dir = options.harness_home.join("state").join("channels");
    let outbox_file = channel_state_dir.join("outbox.jsonl");
    let receipts_file = channel_state_dir.join("receive-receipts.jsonl");
    fs::create_dir_all(&channel_state_dir)?;

    let registry = load_agent_registry(&options.source)?;
    let turn = build_turn_plan(
        &options.source,
        &registry,
        &options.skill_index,
        TurnPlanInput {
            harness_home: Some(options.harness_home.clone()),
            platform: options.platform.clone(),
            channel_id: options.channel_id.clone(),
            user_id: options.user_id.clone(),
            text: options.message,
            requested_agent_id: options.agent_id,
            session_hint: options.session_key,
            skill_limit: options.skill_limit,
        },
    )?;
    let step = build_channel_step(&registry, &turn);
    let command_name = turn
        .command
        .as_ref()
        .map(|command| command.name().to_string());
    let mut warnings = step.warnings.clone();

    let (status, command_apply, queue_enqueue, outbound_messages, queue_id, reason) =
        match step.action {
            ChannelStepAction::ReplyOnly => {
                let apply = apply_channel_command_step(
                    &step,
                    ChannelCommandApplyOptions {
                        harness_home: options.harness_home.clone(),
                        now_ms: options.now_ms,
                    },
                )?;
                append_outbound_messages(&outbox_file, &apply.outbound_messages)?;
                (
                    ChannelReceiveStatus::CommandApplied,
                    Some(apply.clone()),
                    None,
                    apply.outbound_messages,
                    None,
                    "channel command applied and outbound reply recorded".to_string(),
                )
            }
            ChannelStepAction::EnqueueAgentTurn => {
                let queue = enqueue_channel_step(
                    &step,
                    RuntimeQueueEnqueueOptions {
                        harness_home: options.harness_home.clone(),
                        now_ms: options.now_ms,
                    },
                )?;
                let queue_id = queue.receipt.queue_id.clone();
                warnings.extend(queue.warnings.clone());
                (
                    ChannelReceiveStatus::AgentTurnQueued,
                    None,
                    Some(queue),
                    Vec::new(),
                    queue_id,
                    "agent turn queued for runtime worker".to_string(),
                )
            }
            ChannelStepAction::NoAgentAvailable => {
                append_outbound_messages(&outbox_file, &step.outbound_messages)?;
                (
                    ChannelReceiveStatus::ErrorReplied,
                    None,
                    None,
                    step.outbound_messages.clone(),
                    None,
                    "channel error reply recorded".to_string(),
                )
            }
        };

    let receipt = ChannelReceiveReceipt {
        status,
        platform: options.platform,
        channel_id: options.channel_id,
        user_id: options.user_id,
        session_key: step.session_key.clone(),
        queue_id: queue_id.clone(),
        outbound_count: outbound_messages.len(),
        reason,
    };
    append_json_line(&receipts_file, &receipt)?;

    Ok(ChannelReceiveReport {
        schema: CHANNEL_RECEIVE_SCHEMA,
        harness_home: options.harness_home,
        platform: receipt.platform.clone(),
        channel_id: receipt.channel_id.clone(),
        user_id: receipt.user_id.clone(),
        session_key: step.session_key,
        status,
        step_action: step.action,
        command_name,
        queue_id,
        outbox_file,
        receipts_file,
        command_apply,
        queue_enqueue,
        outbound_messages,
        receipt,
        warnings,
    })
}

fn append_outbound_messages(path: &Path, messages: &[ChannelOutboundMessage]) -> io::Result<()> {
    for message in messages {
        append_json_line(path, message)?;
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{build_source_skill_index, read_channel_session_state};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn channel_receive_applies_command_and_writes_outbox() {
        let root = temp_root("channel_receive_applies_command_and_writes_outbox");
        let source = write_receive_source(&root);
        let harness_home = root.join(".openclaw-harness");
        let skills = build_source_skill_index(&source).unwrap();

        let report = receive_channel_message(ChannelReceiveOptions {
            source: source.clone(),
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "telegram".to_string(),
            channel_id: "dm".to_string(),
            user_id: "user".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "/model openrouter/anthropic/claude-sonnet-4".to_string(),
            skill_limit: 3,
            now_ms: 1000,
        })
        .unwrap();

        assert_eq!(report.status, ChannelReceiveStatus::CommandApplied);
        assert_eq!(report.command_name.as_deref(), Some("model"));
        assert_eq!(report.outbound_messages.len(), 1);
        assert!(report.outbox_file.is_file());
        let state = read_channel_session_state(&harness_home, "telegram", "dm", "user")
            .unwrap()
            .unwrap();
        assert_eq!(state.model_override_provider.as_deref(), Some("openrouter"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn channel_receive_queues_agent_turn_with_state_override() {
        let root = temp_root("channel_receive_queues_agent_turn_with_state_override");
        let source = write_receive_source(&root);
        let harness_home = root.join(".openclaw-harness");
        let skills = build_source_skill_index(&source).unwrap();
        receive_channel_message(ChannelReceiveOptions {
            source: source.clone(),
            harness_home: harness_home.clone(),
            skill_index: skills.clone(),
            platform: "telegram".to_string(),
            channel_id: "dm".to_string(),
            user_id: "user".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "/model openrouter/anthropic/claude-sonnet-4".to_string(),
            skill_limit: 3,
            now_ms: 1000,
        })
        .unwrap();

        let report = receive_channel_message(ChannelReceiveOptions {
            source,
            harness_home,
            skill_index: skills,
            platform: "telegram".to_string(),
            channel_id: "dm".to_string(),
            user_id: "user".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "continue".to_string(),
            skill_limit: 3,
            now_ms: 1001,
        })
        .unwrap();

        assert_eq!(report.status, ChannelReceiveStatus::AgentTurnQueued);
        assert!(report.queue_id.is_some());
        let queue = report.queue_enqueue.unwrap();
        let item = queue.item.unwrap();
        assert_eq!(item.provider.as_deref(), Some("openrouter"));
        assert_eq!(item.model.as_deref(), Some("anthropic/claude-sonnet-4"));

        let _ = fs::remove_dir_all(root);
    }

    fn write_receive_source(root: &Path) -> OpenClawSource {
        let home = root.join(".openclaw");
        let workspace = home.join("workspace");
        let skill = workspace.join("skills").join("handoff");
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(&skill).unwrap();
        fs::create_dir_all(home.join("agents").join("main").join("sessions")).unwrap();
        fs::write(workspace.join("AGENTS.md"), "# Agent").unwrap();
        fs::write(skill.join(crate::SKILL_FILE_NAME), "# Handoff").unwrap();
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
        OpenClawSource::with_workspace(home, workspace)
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "openclaw-harness-channel-ingress-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
