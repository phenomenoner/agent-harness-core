use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::Value;

use crate::{ChannelStep, ChannelStepAction};

const RUNTIME_QUEUE_REPORT_SCHEMA: &str = "agent-harness.runtime-queue-enqueue.v1";
const RUNTIME_QUEUE_CONTROL_SCHEMA: &str = "agent-harness.runtime-queue-control.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeQueueEnqueueOptions {
    pub harness_home: PathBuf,
    pub runtime_workspace: Option<PathBuf>,
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
    pub runtime_class: String,
    pub origin: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cron_run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scheduled_for_ms: Option<i64>,
    pub source: RuntimeQueueSource,
    pub created_at_ms: i64,
    pub agent_id: String,
    pub session_key: String,
    pub platform: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account_id: Option<String>,
    pub channel_id: String,
    pub user_id: String,
    pub message_text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inbound_context: Option<String>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime_workspace: Option<PathBuf>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum RuntimeQueueControlAction {
    Retry,
    Skip,
}

impl std::str::FromStr for RuntimeQueueControlAction {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "retry" => Ok(Self::Retry),
            "skip" => Ok(Self::Skip),
            other => Err(format!("unsupported runtime queue control action: {other}")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeQueueControlOptions {
    pub harness_home: PathBuf,
    pub queue_id: String,
    pub action: RuntimeQueueControlAction,
    pub reason: String,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeQueueControlReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub queue_file: PathBuf,
    pub receipts_file: PathBuf,
    pub run_once_receipts_file: PathBuf,
    pub action: RuntimeQueueControlAction,
    pub status: RuntimeQueueControlStatus,
    pub original_queue_id: String,
    pub new_queue_id: Option<String>,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum RuntimeQueueControlStatus {
    Retried,
    Skipped,
    NotFound,
    InvalidItem,
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

pub fn control_runtime_queue_item(
    options: RuntimeQueueControlOptions,
) -> io::Result<RuntimeQueueControlReport> {
    let queue_dir = options.harness_home.join("state").join("runtime-queue");
    let queue_file = queue_dir.join("pending.jsonl");
    let receipts_file = queue_dir.join("control-receipts.jsonl");
    let run_once_receipts_file = queue_dir.join("run-once-receipts.jsonl");
    fs::create_dir_all(&queue_dir)?;

    let mut report = RuntimeQueueControlReport {
        schema: RUNTIME_QUEUE_CONTROL_SCHEMA,
        harness_home: options.harness_home.clone(),
        queue_file: queue_file.clone(),
        receipts_file: receipts_file.clone(),
        run_once_receipts_file: run_once_receipts_file.clone(),
        action: options.action,
        status: RuntimeQueueControlStatus::NotFound,
        original_queue_id: options.queue_id.clone(),
        new_queue_id: None,
        reason: options.reason.clone(),
    };

    let Some(mut item) = find_queue_item_value(&queue_file, &options.queue_id)? else {
        append_json_line(&receipts_file, &report)?;
        return Ok(report);
    };

    match options.action {
        RuntimeQueueControlAction::Retry => {
            let Some(object) = item.as_object_mut() else {
                report.status = RuntimeQueueControlStatus::InvalidItem;
                append_json_line(&receipts_file, &report)?;
                return Ok(report);
            };
            let new_queue_id = retry_queue_id(&options.queue_id, &options.reason, options.now_ms);
            object.insert("queueId".to_string(), Value::String(new_queue_id.clone()));
            object.insert(
                "createdAtMs".to_string(),
                Value::Number(serde_json::Number::from(options.now_ms)),
            );
            object.insert(
                "retryOfQueueId".to_string(),
                Value::String(options.queue_id.clone()),
            );
            object.insert(
                "retryReason".to_string(),
                Value::String(options.reason.clone()),
            );
            append_json_line(&queue_file, &item)?;
            report.status = RuntimeQueueControlStatus::Retried;
            report.new_queue_id = Some(new_queue_id);
        }
        RuntimeQueueControlAction::Skip => {
            let receipt = RuntimeQueueSkipReceipt {
                queue_id: Some(options.queue_id.clone()),
                status: "skipped",
                execution_dir: None,
                transcript_file: None,
                outbox_file: None,
                reason: options.reason.clone(),
            };
            append_json_line(&run_once_receipts_file, &receipt)?;
            report.status = RuntimeQueueControlStatus::Skipped;
        }
    }

    append_json_line(&receipts_file, &report)?;
    Ok(report)
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
        schema: "agent-harness.runtime-queue-item.v1",
        queue_id: queue_id(step, &agent_turn.agent_id, options.now_ms),
        status: RuntimeQueueItemStatus::Queued,
        runtime_class: "interactive".to_string(),
        origin: "channel".to_string(),
        cron_run_id: None,
        scheduled_for_ms: None,
        source: RuntimeQueueSource {
            kind: RuntimeQueueSourceKind::Channel,
            source_home: step.source_home.clone(),
            source_workspace: step.source_workspace.clone(),
            runtime_workspace: options.runtime_workspace.clone(),
        },
        created_at_ms: options.now_ms,
        agent_id: agent_turn.agent_id.clone(),
        session_key: agent_turn.session_key.clone(),
        platform: step.platform.clone(),
        account_id: step.account_id.clone(),
        channel_id: step.channel_id.clone(),
        user_id: step.user_id.clone(),
        message_text: step.message_text.clone(),
        inbound_context: step.inbound_context.clone(),
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
    crate::append_jsonl_value(path, value)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct RuntimeQueueSkipReceipt {
    pub queue_id: Option<String>,
    pub status: &'static str,
    pub execution_dir: Option<PathBuf>,
    pub transcript_file: Option<PathBuf>,
    pub outbox_file: Option<PathBuf>,
    pub reason: String,
}

fn find_queue_item_value(path: &Path, queue_id: &str) -> io::Result<Option<Value>> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let value = match serde_json::from_str::<Value>(trimmed) {
            Ok(value) => value,
            Err(_) => continue,
        };
        if value.get("queueId").and_then(Value::as_str) == Some(queue_id) {
            return Ok(Some(value));
        }
    }
    Ok(None)
}

fn retry_queue_id(queue_id: &str, reason: &str, now_ms: i64) -> String {
    format!(
        "retry:{}:{}:{}",
        now_ms,
        normalize_key_part(queue_id),
        fnv1a_64_hex(reason)
    )
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
                inbound_context: None,
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
                harness_home: root.join(".agent-harness"),
                runtime_workspace: None,
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
    fn queue_control_retries_with_new_queue_id_and_skips_terminally() {
        let root = temp_root("queue_control_retries_with_new_queue_id_and_skips_terminally");
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
                inbound_context: None,
                requested_agent_id: Some("main".to_string()),
                session_hint: None,
                skill_limit: 3,
            },
        )
        .unwrap();
        let step = build_channel_step(&registry, &turn);
        let harness_home = root.join(".agent-harness");
        let enqueue = enqueue_channel_step(
            &step,
            RuntimeQueueEnqueueOptions {
                harness_home: harness_home.clone(),
                runtime_workspace: None,
                now_ms: 1234,
            },
        )
        .unwrap();
        let original_queue_id = enqueue.item.unwrap().queue_id;

        let retry = control_runtime_queue_item(RuntimeQueueControlOptions {
            harness_home: harness_home.clone(),
            queue_id: original_queue_id.clone(),
            action: RuntimeQueueControlAction::Retry,
            reason: "operator retry after timeout".to_string(),
            now_ms: 2234,
        })
        .unwrap();

        assert_eq!(retry.status, RuntimeQueueControlStatus::Retried);
        let new_queue_id = retry.new_queue_id.unwrap();
        assert_ne!(new_queue_id, original_queue_id);
        let queue_text = fs::read_to_string(&retry.queue_file).unwrap();
        assert!(queue_text.contains(&original_queue_id));
        assert!(queue_text.contains(&new_queue_id));
        assert!(queue_text.contains("retryOfQueueId"));
        let queued_items = queue_text
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        let original_item = queued_items
            .iter()
            .find(|item| {
                item.get("queueId").and_then(Value::as_str) == Some(original_queue_id.as_str())
            })
            .unwrap();
        let retry_item = queued_items
            .iter()
            .find(|item| item.get("queueId").and_then(Value::as_str) == Some(new_queue_id.as_str()))
            .unwrap();
        assert_eq!(
            retry_item.get("retryOfQueueId").and_then(Value::as_str),
            Some(original_queue_id.as_str())
        );
        for field in [
            "agentId",
            "sessionKey",
            "platform",
            "channelId",
            "userId",
            "provider",
            "model",
            "selectedSkillIds",
            "plannedTranscriptFile",
            "plannedTrajectoryFile",
            "source",
        ] {
            assert_eq!(retry_item.get(field), original_item.get(field), "{field}");
        }

        let skip = control_runtime_queue_item(RuntimeQueueControlOptions {
            harness_home,
            queue_id: original_queue_id.clone(),
            action: RuntimeQueueControlAction::Skip,
            reason: "operator confirmed stale request".to_string(),
            now_ms: 3234,
        })
        .unwrap();

        assert_eq!(skip.status, RuntimeQueueControlStatus::Skipped);
        let run_once_receipts = fs::read_to_string(skip.run_once_receipts_file).unwrap();
        assert!(run_once_receipts.contains("\"status\":\"skipped\""));
        assert!(run_once_receipts.contains(&original_queue_id));
        let control_receipts = fs::read_to_string(skip.receipts_file).unwrap();
        assert!(control_receipts.contains("\"status\":\"retried\""));
        assert!(control_receipts.contains("\"status\":\"skipped\""));

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
                inbound_context: None,
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
                harness_home: root.join(".agent-harness"),
                runtime_workspace: None,
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

    fn write_runtime_queue_source(root: &Path) -> crate::AgentSource {
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
        crate::AgentSource::with_workspace(home, workspace)
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-runtime-queue-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
