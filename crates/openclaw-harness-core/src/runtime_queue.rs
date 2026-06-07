use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::{ChannelStep, ChannelStepAction};

const RUNTIME_QUEUE_REPORT_SCHEMA: &str = "openclaw-harness.runtime-queue-enqueue.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeQueueEnqueueOptions {
    pub harness_home: PathBuf,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeQueueEnqueueReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub queue_file: PathBuf,
    pub receipts_file: PathBuf,
    pub item: Option<RuntimeQueueItem>,
    pub receipt: RuntimeQueueReceipt,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeQueueItem {
    pub schema: &'static str,
    pub queue_id: String,
    pub status: RuntimeQueueItemStatus,
    pub source: RuntimeQueueSource,
    pub created_at_ms: i64,
    pub agent_id: String,
    pub session_key: String,
    pub platform: String,
    pub channel_id: String,
    pub user_id: String,
    pub message_text: String,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub prompt_files_present: usize,
    pub prompt_files_total: usize,
    pub selected_skill_ids: Vec<String>,
    pub planned_transcript_file: PathBuf,
    pub planned_trajectory_file: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum RuntimeQueueItemStatus {
    Queued,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeQueueSource {
    pub kind: RuntimeQueueSourceKind,
    pub source_home: PathBuf,
    pub source_workspace: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum RuntimeQueueSourceKind {
    Channel,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeQueueReceipt {
    pub queue_id: Option<String>,
    pub status: RuntimeQueueReceiptStatus,
    pub queue_file: PathBuf,
    pub receipts_file: PathBuf,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum RuntimeQueueReceiptStatus {
    Queued,
    SkippedNotAgentTurn,
}

pub fn enqueue_channel_step(
    step: &ChannelStep,
    options: RuntimeQueueEnqueueOptions,
) -> io::Result<RuntimeQueueEnqueueReport> {
    let queue_dir = options.harness_home.join("state").join("runtime-queue");
    let queue_file = queue_dir.join("pending.jsonl");
    let receipts_file = queue_dir.join("receipts.jsonl");
    fs::create_dir_all(&queue_dir)?;

    let mut warnings = step.warnings.clone();
    let item = if step.action == ChannelStepAction::EnqueueAgentTurn {
        match build_queue_item(step, &options)? {
            Some(item) => Some(item),
            None => {
                warnings.push(
                    "channel step requested enqueue but did not include agent turn dispatch"
                        .to_string(),
                );
                None
            }
        }
    } else {
        None
    };

    let receipt = match &item {
        Some(item) => {
            append_json_line(&queue_file, item)?;
            RuntimeQueueReceipt {
                queue_id: Some(item.queue_id.clone()),
                status: RuntimeQueueReceiptStatus::Queued,
                queue_file: queue_file.clone(),
                receipts_file: receipts_file.clone(),
                reason: "agent turn appended to runtime queue".to_string(),
            }
        }
        None => RuntimeQueueReceipt {
            queue_id: None,
            status: RuntimeQueueReceiptStatus::SkippedNotAgentTurn,
            queue_file: queue_file.clone(),
            receipts_file: receipts_file.clone(),
            reason: format!("channel step action {:?} is not an agent turn", step.action),
        },
    };
    append_json_line(&receipts_file, &receipt)?;

    Ok(RuntimeQueueEnqueueReport {
        schema: RUNTIME_QUEUE_REPORT_SCHEMA,
        harness_home: options.harness_home,
        queue_file,
        receipts_file,
        item,
        receipt,
        warnings,
    })
}

fn build_queue_item(
    step: &ChannelStep,
    options: &RuntimeQueueEnqueueOptions,
) -> io::Result<Option<RuntimeQueueItem>> {
    let Some(agent_turn) = step.agent_turn.as_ref() else {
        return Ok(None);
    };
    let file_safe_session_key = normalize_key_part(&agent_turn.session_key);
    let sessions_dir = options
        .harness_home
        .join("agents")
        .join(&agent_turn.agent_id)
        .join("sessions");
    let planned_transcript_file = sessions_dir.join(format!("{file_safe_session_key}.jsonl"));
    let planned_trajectory_file =
        sessions_dir.join(format!("{file_safe_session_key}.trajectory.jsonl"));
    fs::create_dir_all(&sessions_dir)?;

    Ok(Some(RuntimeQueueItem {
        schema: "openclaw-harness.runtime-queue-item.v1",
        queue_id: queue_id(step, &agent_turn.agent_id, options.now_ms),
        status: RuntimeQueueItemStatus::Queued,
        source: RuntimeQueueSource {
            kind: RuntimeQueueSourceKind::Channel,
            source_home: step.source_home.clone(),
            source_workspace: step.source_workspace.clone(),
        },
        created_at_ms: options.now_ms,
        agent_id: agent_turn.agent_id.clone(),
        session_key: agent_turn.session_key.clone(),
        platform: step.platform.clone(),
        channel_id: step.channel_id.clone(),
        user_id: step.user_id.clone(),
        message_text: step.message_text.clone(),
        provider: agent_turn.provider.clone(),
        model: agent_turn.model.clone(),
        prompt_files_present: agent_turn.prompt_files_present,
        prompt_files_total: agent_turn.prompt_files_total,
        selected_skill_ids: agent_turn.selected_skill_ids.clone(),
        planned_transcript_file,
        planned_trajectory_file,
    }))
}

fn append_json_line(path: &Path, value: &impl Serialize) -> io::Result<()> {
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    let line = serde_json::to_string(value).map_err(io::Error::other)?;
    writeln!(file, "{line}")?;
    Ok(())
}

fn queue_id(step: &ChannelStep, agent_id: &str, now_ms: i64) -> String {
    let hash_input = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        step.platform, step.channel_id, step.user_id, agent_id, step.session_key, step.message_text
    );
    format!(
        "turn:{}:{}:{}:{}:{}:{}",
        now_ms,
        normalize_key_part(&step.platform),
        normalize_key_part(&step.channel_id),
        normalize_key_part(&step.user_id),
        normalize_key_part(agent_id),
        fnv1a_64_hex(&hash_input)
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

fn fnv1a_64_hex(value: &str) -> String {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ChannelStepAction, TurnPlanInput, build_channel_step, build_source_skill_index,
        build_turn_plan, load_agent_registry,
    };
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn enqueue_channel_agent_turn_writes_queue_and_receipt() {
        let root = temp_root("enqueue_channel_agent_turn_writes_queue_and_receipt");
        let source = write_runtime_queue_source(&root);
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
                requested_agent_id: Some("main".to_string()),
                session_hint: None,
                skill_limit: 3,
            },
        )
        .unwrap();
        let step = build_channel_step(&registry, &turn);

        let report = enqueue_channel_step(
            &step,
            RuntimeQueueEnqueueOptions {
                harness_home: root.join(".openclaw-harness"),
                now_ms: 1234,
            },
        )
        .unwrap();

        assert_eq!(report.receipt.status, RuntimeQueueReceiptStatus::Queued);
        assert!(report.queue_file.is_file());
        assert!(report.receipts_file.is_file());
        let item = report.item.unwrap();
        assert!(
            item.queue_id
                .starts_with("turn:1234:telegram:dm-42:user-7:main:")
        );
        assert_eq!(item.agent_id, "main");
        assert_eq!(item.provider.as_deref(), Some("openai"));
        assert_eq!(item.model.as_deref(), Some("gpt-5"));
        assert_eq!(item.selected_skill_ids, vec!["workspace:memory-cron"]);
        assert!(
            item.planned_transcript_file
                .ends_with("agents\\main\\sessions\\telegram_dm-42_user-7_main.jsonl")
                || item
                    .planned_transcript_file
                    .ends_with("agents/main/sessions/telegram_dm-42_user-7_main.jsonl")
        );

        let queue_json: serde_json::Value = serde_json::from_str(
            fs::read_to_string(&report.queue_file)
                .unwrap()
                .lines()
                .next()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(queue_json["status"], "queued");
        assert_eq!(queue_json["source"]["kind"], "channel");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn command_reply_step_writes_skip_receipt_without_queue_item() {
        let root = temp_root("command_reply_step_writes_skip_receipt_without_queue_item");
        let source = write_runtime_queue_source(&root);
        let registry = load_agent_registry(&source).unwrap();
        let skills = build_source_skill_index(&source).unwrap();
        let turn = build_turn_plan(
            &source,
            &registry,
            &skills,
            TurnPlanInput {
                harness_home: None,
                platform: "discord".to_string(),
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                text: "/status".to_string(),
                requested_agent_id: Some("main".to_string()),
                session_hint: None,
                skill_limit: 3,
            },
        )
        .unwrap();
        let step = build_channel_step(&registry, &turn);
        assert_eq!(step.action, ChannelStepAction::ReplyOnly);

        let report = enqueue_channel_step(
            &step,
            RuntimeQueueEnqueueOptions {
                harness_home: root.join(".openclaw-harness"),
                now_ms: 1234,
            },
        )
        .unwrap();

        assert!(report.item.is_none());
        assert_eq!(
            report.receipt.status,
            RuntimeQueueReceiptStatus::SkippedNotAgentTurn
        );
        assert!(!report.queue_file.is_file());
        assert!(report.receipts_file.is_file());
        let receipt_json: serde_json::Value = serde_json::from_str(
            fs::read_to_string(&report.receipts_file)
                .unwrap()
                .lines()
                .next()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(receipt_json["status"], "skipped-not-agent-turn");

        let _ = fs::remove_dir_all(root);
    }

    fn write_runtime_queue_source(root: &Path) -> crate::OpenClawSource {
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
        crate::OpenClawSource::with_workspace(home, workspace)
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "openclaw-harness-runtime-queue-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
