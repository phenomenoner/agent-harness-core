use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::Value;

use crate::OpenClawSource;

const SUBAGENT_PLAN_SCHEMA: &str = "openclaw-harness.subagent-plan.v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubagentLedger {
    pub source_home: PathBuf,
    pub runs_file: PathBuf,
    pub summary: SubagentLedgerSummary,
    pub runs: Vec<SubagentRun>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubagentLedgerSummary {
    pub total_runs: usize,
    pub queued: usize,
    pub running: usize,
    pub completed: usize,
    pub failed: usize,
    pub canceled: usize,
    pub unknown: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubagentRun {
    pub id: String,
    pub agent_id: Option<String>,
    pub parent_agent_id: Option<String>,
    pub status: SubagentRunStatus,
    pub task: Option<String>,
    pub created_at_ms: Option<i64>,
    pub updated_at_ms: Option<i64>,
    pub raw_status: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum SubagentRunStatus {
    Queued,
    Running,
    Completed,
    Failed,
    Canceled,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubagentPlanInput {
    pub resume_subagents: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubagentPlan {
    pub schema: &'static str,
    pub source_home: PathBuf,
    pub resume_subagents: bool,
    pub summary: SubagentPlanSummary,
    pub entries: Vec<SubagentPlanEntry>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubagentPlanSummary {
    pub total_runs: usize,
    pub completed_noop: usize,
    pub failed_noop: usize,
    pub canceled_noop: usize,
    pub cutover_held: usize,
    pub resume_candidates: usize,
    pub unknown_status_review: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubagentPlanEntry {
    pub run_id: String,
    pub agent_id: Option<String>,
    pub parent_agent_id: Option<String>,
    pub status: SubagentRunStatus,
    pub action: SubagentPlanAction,
    pub reason: String,
    pub task: Option<String>,
    pub session_key: Option<String>,
    pub created_at_ms: Option<i64>,
    pub updated_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum SubagentPlanAction {
    CompletedNoop,
    FailedNoop,
    CanceledNoop,
    CutoverHold,
    ResumeCandidate,
    UnknownStatusReview,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubagentPlanFile {
    pub json: PathBuf,
}

pub fn load_subagent_ledger(source: &OpenClawSource) -> io::Result<SubagentLedger> {
    let runs_file = source.home.join("subagents").join("runs.json");
    let mut warnings = Vec::new();
    let runs = if runs_file.is_file() {
        let value = read_json_file(&runs_file)?;
        parse_runs(&value, &mut warnings)
    } else {
        warnings.push(format!(
            "subagent runs ledger not found at {}",
            runs_file.display()
        ));
        Vec::new()
    };
    let summary = summarize_ledger(&runs);

    Ok(SubagentLedger {
        source_home: source.home.clone(),
        runs_file,
        summary,
        runs,
        warnings,
    })
}

pub fn plan_subagents(ledger: &SubagentLedger, input: SubagentPlanInput) -> SubagentPlan {
    let mut warnings = ledger.warnings.clone();
    if !input.resume_subagents {
        warnings.push(
            "resume-subagents is disabled; queued/running subagent runs are held at cutover"
                .to_string(),
        );
    }

    let entries = ledger
        .runs
        .iter()
        .map(|run| plan_run(run, input.resume_subagents))
        .collect::<Vec<_>>();
    let summary = summarize_plan(&entries);

    SubagentPlan {
        schema: SUBAGENT_PLAN_SCHEMA,
        source_home: ledger.source_home.clone(),
        resume_subagents: input.resume_subagents,
        summary,
        entries,
        warnings,
    }
}

pub fn write_subagent_plan(
    plan: &SubagentPlan,
    output_dir: impl AsRef<Path>,
) -> io::Result<SubagentPlanFile> {
    let output_dir = output_dir.as_ref();
    fs::create_dir_all(output_dir)?;
    let json = output_dir.join("subagent-plan.json");
    let text = serde_json::to_string_pretty(plan).map_err(io::Error::other)?;
    fs::write(&json, text)?;
    Ok(SubagentPlanFile { json })
}

fn plan_run(run: &SubagentRun, resume_subagents: bool) -> SubagentPlanEntry {
    let (action, reason) = match run.status {
        SubagentRunStatus::Completed => (
            SubagentPlanAction::CompletedNoop,
            "completed run is preserved as historical state".to_string(),
        ),
        SubagentRunStatus::Failed => (
            SubagentPlanAction::FailedNoop,
            "failed run is preserved as historical state".to_string(),
        ),
        SubagentRunStatus::Canceled => (
            SubagentPlanAction::CanceledNoop,
            "canceled run is preserved as historical state".to_string(),
        ),
        SubagentRunStatus::Queued | SubagentRunStatus::Running if resume_subagents => (
            SubagentPlanAction::ResumeCandidate,
            "resume-subagents enabled; run can be reviewed for worker queue resume".to_string(),
        ),
        SubagentRunStatus::Queued | SubagentRunStatus::Running => (
            SubagentPlanAction::CutoverHold,
            "resume-subagents not enabled; run held to prevent duplicate worker execution"
                .to_string(),
        ),
        SubagentRunStatus::Unknown => (
            SubagentPlanAction::UnknownStatusReview,
            "run status is unknown and requires operator review".to_string(),
        ),
    };

    let session_key = match action {
        SubagentPlanAction::CutoverHold | SubagentPlanAction::ResumeCandidate => Some(format!(
            "subagent:{}:{}",
            normalize_key_part(&run.id),
            normalize_key_part(run.agent_id.as_deref().unwrap_or("unknown"))
        )),
        _ => None,
    };

    SubagentPlanEntry {
        run_id: run.id.clone(),
        agent_id: run.agent_id.clone(),
        parent_agent_id: run.parent_agent_id.clone(),
        status: run.status,
        action,
        reason,
        task: run.task.clone(),
        session_key,
        created_at_ms: run.created_at_ms,
        updated_at_ms: run.updated_at_ms,
    }
}

fn read_json_file(path: &Path) -> io::Result<Value> {
    let text = fs::read_to_string(path)?;
    serde_json::from_str(&text).map_err(io::Error::other)
}

fn parse_runs(value: &Value, warnings: &mut Vec<String>) -> Vec<SubagentRun> {
    let runs_value = value.get("runs").unwrap_or(value);
    let mut runs = Vec::new();

    match runs_value {
        Value::Array(array) => {
            for (index, value) in array.iter().enumerate() {
                if let Some(run) = parse_run(value, None, index, warnings) {
                    runs.push(run);
                }
            }
        }
        Value::Object(_) if looks_like_run_object(runs_value) => {
            if let Some(run) = parse_run(runs_value, None, 0, warnings) {
                runs.push(run);
            }
        }
        Value::Object(object) => {
            for (index, (key, value)) in object.iter().enumerate() {
                if let Some(run) = parse_run(value, Some(key), index, warnings) {
                    runs.push(run);
                }
            }
        }
        _ => warnings.push("subagent runs ledger did not contain an array or object".to_string()),
    }

    runs.sort_by(|left, right| left.id.cmp(&right.id));
    runs
}

fn parse_run(
    value: &Value,
    keyed_id: Option<&str>,
    index: usize,
    warnings: &mut Vec<String>,
) -> Option<SubagentRun> {
    if !value.is_object() {
        warnings.push(format!("subagent run at index {index} is not an object"));
        return None;
    }

    let id = string_field(
        value,
        &["id", "runId", "run_id", "subagentRunId", "subagent_run_id"],
    )
    .or(keyed_id)
    .map(ToString::to_string)
    .unwrap_or_else(|| format!("run-{index}"));
    let raw_status = string_field(value, &["status", "state", "phase"]).map(ToString::to_string);

    Some(SubagentRun {
        id,
        agent_id: string_field(
            value,
            &[
                "agentId",
                "agent_id",
                "agent",
                "subagentId",
                "subagent_id",
                "targetAgentId",
                "target_agent_id",
            ],
        )
        .map(ToString::to_string),
        parent_agent_id: string_field(
            value,
            &[
                "parentAgentId",
                "parent_agent_id",
                "parentAgent",
                "parent_agent",
                "parent",
                "ownerAgentId",
                "owner_agent_id",
            ],
        )
        .map(ToString::to_string),
        status: raw_status
            .as_deref()
            .map(parse_status)
            .unwrap_or(SubagentRunStatus::Unknown),
        task: extract_task_text(value),
        created_at_ms: i64_field(
            value,
            &[
                "createdAtMs",
                "created_at_ms",
                "createdMs",
                "created_ms",
                "startedAtMs",
                "started_at_ms",
            ],
        ),
        updated_at_ms: i64_field(
            value,
            &[
                "updatedAtMs",
                "updated_at_ms",
                "updatedMs",
                "updated_ms",
                "finishedAtMs",
                "finished_at_ms",
                "completedAtMs",
                "completed_at_ms",
            ],
        ),
        raw_status,
    })
}

fn looks_like_run_object(value: &Value) -> bool {
    value.is_object()
        && (has_any_key(
            value,
            &["id", "runId", "run_id", "subagentRunId", "subagent_run_id"],
        ) || has_any_key(value, &["status", "state", "phase"])
            || has_any_key(
                value,
                &["task", "prompt", "message", "instruction", "description"],
            ))
}

fn parse_status(value: &str) -> SubagentRunStatus {
    let normalized = normalize_status(value);
    match normalized.as_str() {
        "queue" | "queued" | "pending" | "ready" | "scheduled" => SubagentRunStatus::Queued,
        "running" | "inprogress" | "active" | "started" | "working" => SubagentRunStatus::Running,
        "complete" | "completed" | "done" | "finished" | "success" | "succeeded" => {
            SubagentRunStatus::Completed
        }
        "failed" | "failure" | "error" | "errored" | "crashed" => SubagentRunStatus::Failed,
        "canceled" | "cancelled" | "stopped" | "aborted" | "skipped" => SubagentRunStatus::Canceled,
        _ => SubagentRunStatus::Unknown,
    }
}

fn normalize_status(value: &str) -> String {
    value
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

fn extract_task_text(value: &Value) -> Option<String> {
    value
        .get("payload")
        .and_then(|payload| {
            first_text_for_keys(
                payload,
                &[
                    "task",
                    "prompt",
                    "message",
                    "instruction",
                    "description",
                    "input",
                    "content",
                    "title",
                ],
            )
        })
        .or_else(|| {
            first_text_for_keys(
                value,
                &[
                    "task",
                    "prompt",
                    "message",
                    "instruction",
                    "description",
                    "input",
                    "content",
                    "title",
                ],
            )
        })
        .map(|text| truncate_chars(&text, 4000))
}

fn first_text_for_keys(value: &Value, keys: &[&str]) -> Option<String> {
    match value {
        Value::Object(object) => {
            for key in keys {
                if let Some(text) = object.get(*key).and_then(Value::as_str)
                    && !text.trim().is_empty()
                {
                    return Some(text.trim().to_string());
                }
            }
            for child in object.values() {
                if let Some(text) = first_text_for_keys(child, keys) {
                    return Some(text);
                }
            }
            None
        }
        Value::Array(array) => array
            .iter()
            .find_map(|child| first_text_for_keys(child, keys)),
        Value::String(text) if !text.trim().is_empty() => Some(text.trim().to_string()),
        _ => None,
    }
}

fn string_field<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a str> {
    for key in keys {
        if let Some(text) = value.get(*key).and_then(Value::as_str) {
            return Some(text);
        }
    }
    None
}

fn i64_field(value: &Value, keys: &[&str]) -> Option<i64> {
    for key in keys {
        if let Some(number) = value.get(*key).and_then(Value::as_i64) {
            return Some(number);
        }
        if let Some(number) = value
            .get(*key)
            .and_then(Value::as_str)
            .and_then(|text| text.parse::<i64>().ok())
        {
            return Some(number);
        }
    }
    None
}

fn has_any_key(value: &Value, keys: &[&str]) -> bool {
    keys.iter().any(|key| value.get(*key).is_some())
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
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

fn summarize_ledger(runs: &[SubagentRun]) -> SubagentLedgerSummary {
    let mut summary = SubagentLedgerSummary {
        total_runs: runs.len(),
        ..SubagentLedgerSummary::default()
    };
    for run in runs {
        match run.status {
            SubagentRunStatus::Queued => summary.queued += 1,
            SubagentRunStatus::Running => summary.running += 1,
            SubagentRunStatus::Completed => summary.completed += 1,
            SubagentRunStatus::Failed => summary.failed += 1,
            SubagentRunStatus::Canceled => summary.canceled += 1,
            SubagentRunStatus::Unknown => summary.unknown += 1,
        }
    }
    summary
}

fn summarize_plan(entries: &[SubagentPlanEntry]) -> SubagentPlanSummary {
    let mut summary = SubagentPlanSummary {
        total_runs: entries.len(),
        ..SubagentPlanSummary::default()
    };
    for entry in entries {
        match entry.action {
            SubagentPlanAction::CompletedNoop => summary.completed_noop += 1,
            SubagentPlanAction::FailedNoop => summary.failed_noop += 1,
            SubagentPlanAction::CanceledNoop => summary.canceled_noop += 1,
            SubagentPlanAction::CutoverHold => summary.cutover_held += 1,
            SubagentPlanAction::ResumeCandidate => summary.resume_candidates += 1,
            SubagentPlanAction::UnknownStatusReview => summary.unknown_status_review += 1,
        }
    }
    summary
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn load_ledger_parses_runs_and_status_summary() {
        let root = temp_root("load_ledger_parses_runs_and_status_summary");
        let source = write_subagent_source(&root);

        let ledger = load_subagent_ledger(&source).unwrap();

        assert_eq!(ledger.summary.total_runs, 6);
        assert_eq!(ledger.summary.queued, 1);
        assert_eq!(ledger.summary.running, 1);
        assert_eq!(ledger.summary.completed, 1);
        assert_eq!(ledger.summary.failed, 1);
        assert_eq!(ledger.summary.canceled, 1);
        assert_eq!(ledger.summary.unknown, 1);
        let running = ledger
            .runs
            .iter()
            .find(|run| run.id == "running-1")
            .unwrap();
        assert_eq!(running.agent_id.as_deref(), Some("researcher"));
        assert_eq!(running.parent_agent_id.as_deref(), Some("main"));
        assert_eq!(running.task.as_deref(), Some("continue literature scan"));
        assert_eq!(running.created_at_ms, Some(1000));
        assert_eq!(running.updated_at_ms, Some(1500));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn load_ledger_parses_keyed_object_runs() {
        let root = temp_root("load_ledger_parses_keyed_object_runs");
        let home = root.join(".openclaw");
        fs::create_dir_all(home.join("subagents")).unwrap();
        fs::write(
            home.join("subagents").join("runs.json"),
            r#"{
              "runs": {
                "queued-key": {
                  "agent": "main",
                  "state": "pending",
                  "payload": { "prompt": "queued keyed task" }
                },
                "done-key": {
                  "agent_id": "main",
                  "status": "succeeded",
                  "task": "historical task"
                }
              }
            }"#,
        )
        .unwrap();

        let ledger = load_subagent_ledger(&OpenClawSource::with_workspace(
            &home,
            home.join("workspace"),
        ))
        .unwrap();

        assert_eq!(ledger.summary.total_runs, 2);
        assert_eq!(ledger.summary.queued, 1);
        assert_eq!(ledger.summary.completed, 1);
        assert!(
            ledger
                .runs
                .iter()
                .any(|run| run.id == "queued-key"
                    && run.task.as_deref() == Some("queued keyed task"))
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn plan_holds_queued_and_running_until_resume() {
        let root = temp_root("plan_holds_queued_and_running_until_resume");
        let source = write_subagent_source(&root);
        let ledger = load_subagent_ledger(&source).unwrap();

        let plan = plan_subagents(
            &ledger,
            SubagentPlanInput {
                resume_subagents: false,
            },
        );

        assert_eq!(plan.summary.total_runs, 6);
        assert_eq!(plan.summary.cutover_held, 2);
        assert_eq!(plan.summary.resume_candidates, 0);
        assert_eq!(plan.summary.completed_noop, 1);
        assert_eq!(plan.summary.failed_noop, 1);
        assert_eq!(plan.summary.canceled_noop, 1);
        assert_eq!(plan.summary.unknown_status_review, 1);
        assert!(
            plan.warnings
                .iter()
                .any(|warning| warning.contains("held at cutover"))
        );
        let held = plan
            .entries
            .iter()
            .find(|entry| entry.run_id == "queued-1")
            .unwrap();
        assert_eq!(held.action, SubagentPlanAction::CutoverHold);
        assert_eq!(held.session_key.as_deref(), Some("subagent:queued-1:main"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn plan_classifies_resume_candidates_when_enabled() {
        let root = temp_root("plan_classifies_resume_candidates_when_enabled");
        let source = write_subagent_source(&root);
        let ledger = load_subagent_ledger(&source).unwrap();

        let plan = plan_subagents(
            &ledger,
            SubagentPlanInput {
                resume_subagents: true,
            },
        );

        assert_eq!(plan.summary.cutover_held, 0);
        assert_eq!(plan.summary.resume_candidates, 2);
        let running = plan
            .entries
            .iter()
            .find(|entry| entry.run_id == "running-1")
            .unwrap();
        assert_eq!(running.action, SubagentPlanAction::ResumeCandidate);
        assert_eq!(
            running.session_key.as_deref(),
            Some("subagent:running-1:researcher")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn write_plan_outputs_json() {
        let root = temp_root("write_plan_outputs_json");
        let source = write_subagent_source(&root);
        let ledger = load_subagent_ledger(&source).unwrap();
        let plan = plan_subagents(
            &ledger,
            SubagentPlanInput {
                resume_subagents: true,
            },
        );

        let file = write_subagent_plan(&plan, root.join("out")).unwrap();

        assert!(file.json.is_file());
        let json: serde_json::Value =
            serde_json::from_slice(&fs::read(file.json).unwrap()).unwrap();
        assert_eq!(json["schema"], SUBAGENT_PLAN_SCHEMA);
        assert_eq!(json["summary"]["resumeCandidates"], 2);

        let _ = fs::remove_dir_all(root);
    }

    fn write_subagent_source(root: &Path) -> OpenClawSource {
        let home = root.join(".openclaw");
        let workspace = home.join("workspace");
        fs::create_dir_all(home.join("subagents")).unwrap();
        fs::create_dir_all(&workspace).unwrap();
        fs::write(
            home.join("subagents").join("runs.json"),
            r#"{
              "runs": [
                {
                  "id": "queued-1",
                  "agentId": "main",
                  "parentAgentId": "main",
                  "status": "queued",
                  "task": "prepare handoff"
                },
                {
                  "run_id": "running-1",
                  "agent_id": "researcher",
                  "parent_agent_id": "main",
                  "phase": "in_progress",
                  "payload": { "message": "continue literature scan" },
                  "createdAtMs": "1000",
                  "updated_at_ms": 1500
                },
                {
                  "runId": "completed-1",
                  "agent": "main",
                  "state": "done",
                  "prompt": "historical complete"
                },
                {
                  "id": "failed-1",
                  "agentId": "main",
                  "status": "error"
                },
                {
                  "id": "canceled-1",
                  "agentId": "main",
                  "status": "cancelled"
                },
                {
                  "id": "unknown-1",
                  "agentId": "main",
                  "status": "paused"
                }
              ]
            }"#,
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
            "openclaw-harness-subagents-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
