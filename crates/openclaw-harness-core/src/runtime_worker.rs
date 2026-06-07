use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::Value;

use crate::{
    OpenClawSource, PromptAssemblyOptions, assemble_prompt_bundle, build_source_skill_index,
    build_turn_plan, load_agent_registry, write_prompt_bundle,
};

const RUNTIME_QUEUE_PREPARE_REPORT_SCHEMA: &str = "openclaw-harness.runtime-queue-prepare.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeQueuePrepareOptions {
    pub harness_home: PathBuf,
    pub queue_id: Option<String>,
    pub prompt_options: PromptAssemblyOptions,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeQueuePrepareReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub queue_file: PathBuf,
    pub execution_receipts_file: PathBuf,
    pub item: Option<RuntimeQueuePreparedItem>,
    pub receipt: RuntimeExecutionReceipt,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeQueuePreparedItem {
    pub queue_id: String,
    pub agent_id: String,
    pub session_key: String,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub execution_dir: PathBuf,
    pub prompt_bundle_json: PathBuf,
    pub prompt_markdown: PathBuf,
    pub receipt_file: PathBuf,
    pub planned_transcript_file: PathBuf,
    pub planned_trajectory_file: PathBuf,
    pub selected_skill_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeExecutionReceipt {
    pub queue_id: Option<String>,
    pub status: RuntimeExecutionReceiptStatus,
    pub execution_dir: Option<PathBuf>,
    pub prompt_bundle_json: Option<PathBuf>,
    pub prompt_markdown: Option<PathBuf>,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum RuntimeExecutionReceiptStatus {
    Prepared,
    NoPendingItem,
}

struct PendingQueueItem {
    queue_id: String,
    agent_id: String,
    session_key: String,
    platform: String,
    channel_id: String,
    user_id: String,
    message_text: String,
    source_home: PathBuf,
    source_workspace: PathBuf,
    planned_transcript_file: PathBuf,
    planned_trajectory_file: PathBuf,
    selected_skill_ids: Vec<String>,
}

pub fn prepare_runtime_queue_item(
    options: RuntimeQueuePrepareOptions,
) -> io::Result<RuntimeQueuePrepareReport> {
    let queue_dir = options.harness_home.join("state").join("runtime-queue");
    let queue_file = queue_dir.join("pending.jsonl");
    let execution_receipts_file = queue_dir.join("execution-receipts.jsonl");
    fs::create_dir_all(&queue_dir)?;

    let mut warnings = Vec::new();
    let Some(pending) = read_pending_item(&queue_file, options.queue_id.as_deref(), &mut warnings)?
    else {
        let receipt = RuntimeExecutionReceipt {
            queue_id: options.queue_id,
            status: RuntimeExecutionReceiptStatus::NoPendingItem,
            execution_dir: None,
            prompt_bundle_json: None,
            prompt_markdown: None,
            reason: "no matching queued runtime item found".to_string(),
        };
        append_json_line(&execution_receipts_file, &receipt)?;
        return Ok(RuntimeQueuePrepareReport {
            schema: RUNTIME_QUEUE_PREPARE_REPORT_SCHEMA,
            harness_home: options.harness_home,
            queue_file,
            execution_receipts_file,
            item: None,
            receipt,
            warnings,
        });
    };

    let source = OpenClawSource::with_workspace(&pending.source_home, &pending.source_workspace);
    let registry = load_agent_registry(&source)?;
    let skill_index = build_source_skill_index(&source)?;
    let plan = build_turn_plan(
        &source,
        &registry,
        &skill_index,
        crate::TurnPlanInput {
            harness_home: Some(options.harness_home.clone()),
            platform: pending.platform.clone(),
            channel_id: pending.channel_id.clone(),
            user_id: pending.user_id.clone(),
            text: pending.message_text.clone(),
            requested_agent_id: Some(pending.agent_id.clone()),
            session_hint: Some(pending.session_key.clone()),
            skill_limit: pending.selected_skill_ids.len().max(5),
        },
    )?;
    let actual_skill_ids = plan
        .selected_skills
        .iter()
        .map(|skill| skill.skill_id.clone())
        .collect::<Vec<_>>();
    if !pending.selected_skill_ids.is_empty() && pending.selected_skill_ids != actual_skill_ids {
        warnings.push(format!(
            "prepared skill selection differs from queued selection: queued={:?}, prepared={:?}",
            pending.selected_skill_ids, actual_skill_ids
        ));
    }
    let bundle = assemble_prompt_bundle(&plan, options.prompt_options)?;

    let execution_dir = queue_execution_dir(&options.harness_home, &pending.queue_id);
    fs::create_dir_all(&execution_dir)?;
    let prompt_files = write_prompt_bundle(&bundle, &execution_dir)?;
    let receipt_file = execution_dir.join("execution-receipt.json");
    let item = RuntimeQueuePreparedItem {
        queue_id: pending.queue_id.clone(),
        agent_id: pending.agent_id.clone(),
        session_key: pending.session_key.clone(),
        provider: bundle.provider.clone(),
        model: bundle.model.clone(),
        execution_dir: execution_dir.clone(),
        prompt_bundle_json: prompt_files.json.clone(),
        prompt_markdown: prompt_files.markdown.clone(),
        receipt_file: receipt_file.clone(),
        planned_transcript_file: pending.planned_transcript_file,
        planned_trajectory_file: pending.planned_trajectory_file,
        selected_skill_ids: actual_skill_ids,
    };
    let receipt = RuntimeExecutionReceipt {
        queue_id: Some(pending.queue_id),
        status: RuntimeExecutionReceiptStatus::Prepared,
        execution_dir: Some(execution_dir),
        prompt_bundle_json: Some(prompt_files.json),
        prompt_markdown: Some(prompt_files.markdown),
        reason: "prompt bundle prepared; Codex runtime adapter not invoked yet".to_string(),
    };
    let receipt_json = serde_json::to_string_pretty(&receipt).map_err(io::Error::other)?;
    fs::write(&receipt_file, receipt_json)?;
    append_json_line(&execution_receipts_file, &receipt)?;

    Ok(RuntimeQueuePrepareReport {
        schema: RUNTIME_QUEUE_PREPARE_REPORT_SCHEMA,
        harness_home: options.harness_home,
        queue_file,
        execution_receipts_file,
        item: Some(item),
        receipt,
        warnings,
    })
}

fn read_pending_item(
    queue_file: &Path,
    requested_queue_id: Option<&str>,
    warnings: &mut Vec<String>,
) -> io::Result<Option<PendingQueueItem>> {
    if !queue_file.is_file() {
        warnings.push(format!(
            "runtime queue file not found at {}",
            queue_file.display()
        ));
        return Ok(None);
    }

    let text = fs::read_to_string(queue_file)?;
    for (index, line) in text.lines().enumerate() {
        let line_number = index + 1;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let value: Value = match serde_json::from_str(trimmed) {
            Ok(value) => value,
            Err(error) => {
                warnings.push(format!(
                    "runtime queue line {line_number} is not valid JSON: {error}"
                ));
                continue;
            }
        };
        let Some(queue_id) = string_field(&value, &["queueId", "queue_id"]) else {
            warnings.push(format!("runtime queue line {line_number} has no queue id"));
            continue;
        };
        if requested_queue_id.is_some_and(|requested| requested != queue_id) {
            continue;
        }
        if string_field(&value, &["status"]) != Some("queued") {
            warnings.push(format!(
                "runtime queue item `{queue_id}` is not queued; skipping"
            ));
            continue;
        }
        match parse_pending_item(&value) {
            Some(item) => return Ok(Some(item)),
            None => warnings.push(format!(
                "runtime queue item `{queue_id}` is missing required fields"
            )),
        }
    }

    Ok(None)
}

fn parse_pending_item(value: &Value) -> Option<PendingQueueItem> {
    let source = value.get("source")?;
    Some(PendingQueueItem {
        queue_id: string_field(value, &["queueId", "queue_id"])?.to_string(),
        agent_id: string_field(value, &["agentId", "agent_id"])?.to_string(),
        session_key: string_field(value, &["sessionKey", "session_key"])?.to_string(),
        platform: string_field(value, &["platform"])?.to_string(),
        channel_id: string_field(value, &["channelId", "channel_id"])?.to_string(),
        user_id: string_field(value, &["userId", "user_id"])?.to_string(),
        message_text: string_field(value, &["messageText", "message_text"])?.to_string(),
        source_home: path_field(source, &["sourceHome", "source_home"])?,
        source_workspace: path_field(source, &["sourceWorkspace", "source_workspace"])?,
        planned_transcript_file: path_field(
            value,
            &["plannedTranscriptFile", "planned_transcript_file"],
        )?,
        planned_trajectory_file: path_field(
            value,
            &["plannedTrajectoryFile", "planned_trajectory_file"],
        )?,
        selected_skill_ids: string_array_field(value, &["selectedSkillIds", "selected_skill_ids"]),
    })
}

fn queue_execution_dir(harness_home: &Path, queue_id: &str) -> PathBuf {
    harness_home
        .join("state")
        .join("runtime-queue")
        .join("executions")
        .join(normalize_key_part(queue_id))
}

fn append_json_line(path: &Path, value: &impl Serialize) -> io::Result<()> {
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    let line = serde_json::to_string(value).map_err(io::Error::other)?;
    writeln!(file, "{line}")?;
    Ok(())
}

fn string_field<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a str> {
    for key in keys {
        if let Some(text) = value.get(*key).and_then(Value::as_str) {
            return Some(text);
        }
    }
    None
}

fn path_field(value: &Value, keys: &[&str]) -> Option<PathBuf> {
    string_field(value, keys).map(PathBuf::from)
}

fn string_array_field(value: &Value, keys: &[&str]) -> Vec<String> {
    for key in keys {
        if let Some(array) = value.get(*key).and_then(Value::as_array) {
            return array
                .iter()
                .filter_map(Value::as_str)
                .map(ToString::to_string)
                .collect();
        }
    }
    Vec::new()
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
        RuntimeQueueEnqueueOptions, TurnPlanInput, build_channel_step, build_turn_plan,
        enqueue_channel_step,
    };
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn prepare_runtime_queue_item_writes_prompt_bundle_and_receipts() {
        let root = temp_root("prepare_runtime_queue_item_writes_prompt_bundle_and_receipts");
        let source = write_worker_source(&root);
        let harness_home = root.join(".openclaw-harness");
        enqueue_fixture_turn(&source, &harness_home);

        let report = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();

        assert_eq!(
            report.receipt.status,
            RuntimeExecutionReceiptStatus::Prepared
        );
        assert!(report.execution_receipts_file.is_file());
        let item = report.item.unwrap();
        assert_eq!(item.agent_id, "main");
        assert_eq!(item.provider.as_deref(), Some("openai"));
        assert_eq!(item.model.as_deref(), Some("gpt-5"));
        assert!(item.prompt_bundle_json.is_file());
        assert!(item.prompt_markdown.is_file());
        assert!(item.receipt_file.is_file());
        let bundle_json: Value =
            serde_json::from_slice(&fs::read(item.prompt_bundle_json).unwrap()).unwrap();
        assert_eq!(bundle_json["summary"]["userMessagesIncluded"], 1);
        assert_eq!(bundle_json["agentId"], "main");
        assert!(
            fs::read_to_string(item.prompt_markdown)
                .unwrap()
                .contains("repair memory cron")
        );
        let receipt_json: Value =
            serde_json::from_slice(&fs::read(item.receipt_file).unwrap()).unwrap();
        assert_eq!(receipt_json["status"], "prepared");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prepare_runtime_queue_item_reports_no_pending_item() {
        let root = temp_root("prepare_runtime_queue_item_reports_no_pending_item");
        let harness_home = root.join(".openclaw-harness");

        let report = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home,
            queue_id: Some("missing".to_string()),
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();

        assert!(report.item.is_none());
        assert_eq!(
            report.receipt.status,
            RuntimeExecutionReceiptStatus::NoPendingItem
        );
        assert!(report.execution_receipts_file.is_file());

        let _ = fs::remove_dir_all(root);
    }

    fn enqueue_fixture_turn(source: &OpenClawSource, harness_home: &Path) {
        let registry = load_agent_registry(source).unwrap();
        let skills = build_source_skill_index(source).unwrap();
        let turn = build_turn_plan(
            source,
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
        enqueue_channel_step(
            &step,
            RuntimeQueueEnqueueOptions {
                harness_home: harness_home.to_path_buf(),
                now_ms: 1234,
            },
        )
        .unwrap();
    }

    fn write_worker_source(root: &Path) -> OpenClawSource {
        let home = root.join(".openclaw");
        let workspace = home.join("workspace");
        let skill = workspace.join("skills").join("memory-cron");
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(&skill).unwrap();
        fs::create_dir_all(home.join("agents").join("main").join("sessions")).unwrap();
        fs::write(workspace.join("AGENTS.md"), "# Agent").unwrap();
        fs::write(
            skill.join(crate::SKILL_FILE_NAME),
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
        OpenClawSource::with_workspace(home, workspace)
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "openclaw-harness-runtime-worker-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
