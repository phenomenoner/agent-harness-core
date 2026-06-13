use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::Value;

use crate::{AgentRegistry, AgentSource};

const NATIVE_CRON_PLAN_SCHEMA: &str = "agent-harness.native-cron-plan.v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeCronStore {
    pub source_home: PathBuf,
    pub jobs_file: PathBuf,
    pub state_file: PathBuf,
    pub runs_dir: PathBuf,
    pub summary: NativeCronStoreSummary,
    pub jobs: Vec<NativeCronJob>,
    pub states: Vec<NativeCronJobState>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeCronStoreSummary {
    pub total_jobs: usize,
    pub enabled_jobs: usize,
    pub disabled_jobs: usize,
    pub state_entries: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeCronJob {
    pub id: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub enabled: bool,
    pub agent_id: Option<String>,
    pub schedule: NativeCronSchedule,
    pub wake_mode: Option<String>,
    pub session_target: Option<String>,
    pub message_text: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeCronJobState {
    pub job_id: String,
    pub last_run_at_ms: Option<i64>,
    pub next_run_at_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase", tag = "kind")]
pub enum NativeCronSchedule {
    Cron {
        expression: String,
        timezone: Option<String>,
    },
    At {
        text: Option<String>,
        epoch_ms: Option<i64>,
    },
    Unknown {
        summary: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeCronPlanInput {
    pub now_ms: i64,
    pub resume_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeCronPlan {
    pub schema: &'static str,
    pub source_home: PathBuf,
    pub now_ms: i64,
    pub resume_enabled: bool,
    pub summary: NativeCronPlanSummary,
    pub entries: Vec<NativeCronPlanEntry>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeCronPlanSummary {
    pub total_jobs: usize,
    pub disabled: usize,
    pub cutover_held: usize,
    pub enqueue_agent_turns: usize,
    pub waiting_schedule: usize,
    pub cron_registered: usize,
    pub missing_agent: usize,
    pub unsupported_schedule: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeCronPlanEntry {
    pub job_id: String,
    pub name: Option<String>,
    pub enabled: bool,
    pub agent_id: Option<String>,
    pub action: NativeCronPlanAction,
    pub reason: String,
    pub schedule: NativeCronSchedule,
    pub wake_mode: Option<String>,
    pub session_key: Option<String>,
    pub message_text: Option<String>,
    pub last_run_at_ms: Option<i64>,
    pub next_run_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum NativeCronPlanAction {
    Disabled,
    CutoverHold,
    EnqueueAgentTurn,
    WaitingSchedule,
    CronRegistered,
    MissingAgent,
    UnsupportedSchedule,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeCronPlanFile {
    pub json: PathBuf,
}

pub fn load_native_cron_store(source: &AgentSource) -> io::Result<NativeCronStore> {
    let cron_root = source.home.join("cron");
    let jobs_file = cron_root.join("jobs.json");
    let state_file = cron_root.join("jobs-state.json");
    let runs_dir = cron_root.join("runs");
    let mut warnings = Vec::new();
    let jobs = if jobs_file.is_file() {
        let value = read_json_file(&jobs_file)?;
        parse_jobs(&value, &mut warnings)
    } else {
        warnings.push(format!(
            "native cron jobs file not found at {}",
            jobs_file.display()
        ));
        Vec::new()
    };
    let states = if state_file.is_file() {
        let value = read_json_file(&state_file)?;
        parse_states(&value, &mut warnings)
    } else {
        Vec::new()
    };
    let summary = summarize_store(&jobs, &states);

    Ok(NativeCronStore {
        source_home: source.home.clone(),
        jobs_file,
        state_file,
        runs_dir,
        summary,
        jobs,
        states,
        warnings,
    })
}

pub fn plan_native_cron(
    store: &NativeCronStore,
    registry: &AgentRegistry,
    input: NativeCronPlanInput,
) -> NativeCronPlan {
    let state_by_job: BTreeMap<&str, &NativeCronJobState> = store
        .states
        .iter()
        .map(|state| (state.job_id.as_str(), state))
        .collect();
    let agent_ids: BTreeSet<&str> = registry
        .agents
        .iter()
        .map(|agent| agent.id.as_str())
        .collect();
    let mut warnings = store.warnings.clone();
    if !input.resume_enabled {
        warnings.push(
            "resume-cron is disabled; enabled jobs are held to prevent cutover catch-up storms"
                .to_string(),
        );
    }

    let mut entries = Vec::new();
    for job in &store.jobs {
        let state = state_by_job.get(job.id.as_str()).copied();
        entries.push(plan_job(job, state, &agent_ids, &input, &mut warnings));
    }
    let summary = summarize_plan(&entries);

    NativeCronPlan {
        schema: NATIVE_CRON_PLAN_SCHEMA,
        source_home: store.source_home.clone(),
        now_ms: input.now_ms,
        resume_enabled: input.resume_enabled,
        summary,
        entries,
        warnings,
    }
}

pub fn write_native_cron_plan(
    plan: &NativeCronPlan,
    output_dir: impl AsRef<Path>,
) -> io::Result<NativeCronPlanFile> {
    let output_dir = output_dir.as_ref();
    fs::create_dir_all(output_dir)?;
    let json = output_dir.join("native-cron-plan.json");
    let text = serde_json::to_string_pretty(plan).map_err(io::Error::other)?;
    fs::write(&json, text)?;
    Ok(NativeCronPlanFile { json })
}

fn plan_job(
    job: &NativeCronJob,
    state: Option<&NativeCronJobState>,
    agent_ids: &BTreeSet<&str>,
    input: &NativeCronPlanInput,
    warnings: &mut Vec<String>,
) -> NativeCronPlanEntry {
    let agent_id = job.agent_id.clone().or_else(|| Some("main".to_string()));
    let (action, reason) = if !job.enabled {
        (NativeCronPlanAction::Disabled, "job disabled".to_string())
    } else if !input.resume_enabled {
        (
            NativeCronPlanAction::CutoverHold,
            "resume-cron not enabled; job held at cutover".to_string(),
        )
    } else if !agent_id
        .as_deref()
        .is_some_and(|agent_id| agent_ids.contains(agent_id))
    {
        (
            NativeCronPlanAction::MissingAgent,
            format!(
                "agent `{}` is not available in the imported registry",
                agent_id.as_deref().unwrap_or("main")
            ),
        )
    } else {
        plan_schedule_action(job, input.now_ms)
    };

    if job.message_text.is_none()
        && matches!(
            action,
            NativeCronPlanAction::EnqueueAgentTurn | NativeCronPlanAction::CronRegistered
        )
    {
        warnings.push(format!(
            "cron job `{}` has no extracted message text; runtime adapter must inspect payload",
            job.id
        ));
    }

    NativeCronPlanEntry {
        job_id: job.id.clone(),
        name: job.name.clone(),
        enabled: job.enabled,
        agent_id: agent_id.clone(),
        action,
        reason,
        schedule: job.schedule.clone(),
        wake_mode: job.wake_mode.clone(),
        session_key: agent_id.map(|agent_id| cron_session_key(&job.id, &agent_id)),
        message_text: job.message_text.clone(),
        last_run_at_ms: state.and_then(|state| state.last_run_at_ms),
        next_run_at_ms: state.and_then(|state| state.next_run_at_ms),
    }
}

fn plan_schedule_action(job: &NativeCronJob, now_ms: i64) -> (NativeCronPlanAction, String) {
    match &job.schedule {
        NativeCronSchedule::At {
            epoch_ms: Some(at_ms),
            ..
        } if *at_ms <= now_ms => (
            NativeCronPlanAction::EnqueueAgentTurn,
            format!("at schedule is due: {at_ms} <= {now_ms}"),
        ),
        NativeCronSchedule::At {
            epoch_ms: Some(at_ms),
            ..
        } => (
            NativeCronPlanAction::WaitingSchedule,
            format!("at schedule is not due: {at_ms} > {now_ms}"),
        ),
        NativeCronSchedule::At { epoch_ms: None, .. } => (
            NativeCronPlanAction::UnsupportedSchedule,
            "at schedule has no parseable epoch milliseconds".to_string(),
        ),
        NativeCronSchedule::Cron { expression, .. } => (
            NativeCronPlanAction::CronRegistered,
            format!("cron expression registered for scheduler evaluation: {expression}"),
        ),
        NativeCronSchedule::Unknown { summary } => (
            NativeCronPlanAction::UnsupportedSchedule,
            format!("unsupported schedule: {summary}"),
        ),
    }
}

fn read_json_file(path: &Path) -> io::Result<Value> {
    let text = fs::read_to_string(path)?;
    serde_json::from_str(&text).map_err(io::Error::other)
}

fn parse_jobs(value: &Value, warnings: &mut Vec<String>) -> Vec<NativeCronJob> {
    let mut jobs = Vec::new();
    match value.get("jobs").unwrap_or(value) {
        Value::Array(array) => {
            for (index, value) in array.iter().enumerate() {
                if let Some(job) = parse_job(value, None, index, warnings) {
                    jobs.push(job);
                }
            }
        }
        Value::Object(object) => {
            for (index, (key, value)) in object.iter().enumerate() {
                if let Some(job) = parse_job(value, Some(key), index, warnings) {
                    jobs.push(job);
                }
            }
        }
        _ => warnings.push("native cron jobs file did not contain an array or object".to_string()),
    }
    jobs.sort_by(|left, right| left.id.cmp(&right.id));
    jobs
}

fn parse_job(
    value: &Value,
    keyed_id: Option<&str>,
    index: usize,
    warnings: &mut Vec<String>,
) -> Option<NativeCronJob> {
    if !value.is_object() {
        warnings.push(format!("native cron job at index {index} is not an object"));
        return None;
    }
    let id = string_field(value, &["id", "jobId", "job_id"])
        .or(keyed_id)
        .map(ToString::to_string)
        .unwrap_or_else(|| format!("job-{index}"));
    let name = string_field(value, &["name", "title"]).map(ToString::to_string);
    let description = string_field(value, &["description", "summary"]).map(ToString::to_string);
    let enabled = value.get("enabled").and_then(Value::as_bool) != Some(false);
    let agent_id = string_field(value, &["agentId", "agent_id", "agent"]).map(ToString::to_string);
    let schedule = parse_schedule(value);
    let wake_mode = string_field(value, &["wakeMode", "wake_mode"]).map(ToString::to_string);
    let session_target = value
        .get("sessionTarget")
        .or_else(|| value.get("session_target"))
        .and_then(compact_json_string);
    let message_text = extract_message_text(value)
        .or_else(|| description.clone())
        .or_else(|| name.clone());

    Some(NativeCronJob {
        id,
        name,
        description,
        enabled,
        agent_id,
        schedule,
        wake_mode,
        session_target,
        message_text,
    })
}

fn parse_states(value: &Value, warnings: &mut Vec<String>) -> Vec<NativeCronJobState> {
    let mut states = Vec::new();
    match value.get("jobs").unwrap_or(value) {
        Value::Array(array) => {
            for (index, value) in array.iter().enumerate() {
                if let Some(state) = parse_state(value, None, index, warnings) {
                    states.push(state);
                }
            }
        }
        Value::Object(object) => {
            for (index, (key, value)) in object.iter().enumerate() {
                if let Some(state) = parse_state(value, Some(key), index, warnings) {
                    states.push(state);
                }
            }
        }
        _ => warnings
            .push("native cron jobs-state file did not contain an array or object".to_string()),
    }
    states.sort_by(|left, right| left.job_id.cmp(&right.job_id));
    states
}

fn parse_state(
    value: &Value,
    keyed_id: Option<&str>,
    index: usize,
    warnings: &mut Vec<String>,
) -> Option<NativeCronJobState> {
    if !value.is_object() {
        warnings.push(format!(
            "native cron state at index {index} is not an object"
        ));
        return None;
    }
    let job_id = string_field(value, &["id", "jobId", "job_id"])
        .or(keyed_id)
        .map(ToString::to_string)
        .unwrap_or_else(|| format!("job-{index}"));
    Some(NativeCronJobState {
        job_id,
        last_run_at_ms: i64_field(
            value,
            &[
                "lastRunAtMs",
                "last_run_at_ms",
                "lastStartedAtMs",
                "last_started_at_ms",
            ],
        ),
        next_run_at_ms: i64_field(
            value,
            &[
                "nextRunAtMs",
                "next_run_at_ms",
                "nextDueAtMs",
                "next_due_at_ms",
            ],
        ),
    })
}

fn parse_schedule(job: &Value) -> NativeCronSchedule {
    let Some(schedule) = job.get("schedule") else {
        return NativeCronSchedule::Unknown {
            summary: "missing schedule".to_string(),
        };
    };

    if let Some(text) = schedule.as_str() {
        if looks_like_cron(text) {
            return NativeCronSchedule::Cron {
                expression: text.to_string(),
                timezone: None,
            };
        }
        return NativeCronSchedule::Unknown {
            summary: text.to_string(),
        };
    }

    if let Some(kind) = string_field(schedule, &["type", "kind"]) {
        if kind.eq_ignore_ascii_case("cron")
            && let Some(expression) = string_field(schedule, &["expression", "cron", "expr"])
        {
            return NativeCronSchedule::Cron {
                expression: expression.to_string(),
                timezone: schedule_timezone(schedule),
            };
        }
        if kind.eq_ignore_ascii_case("at") {
            return NativeCronSchedule::At {
                text: string_field(schedule, &["at", "time"]).map(ToString::to_string),
                epoch_ms: i64_field(
                    schedule,
                    &["atMs", "at_ms", "timeMs", "time_ms", "epochMs", "epoch_ms"],
                ),
            };
        }
    }

    if let Some(expression) = string_field(schedule, &["expression", "cron", "expr"]) {
        return NativeCronSchedule::Cron {
            expression: expression.to_string(),
            timezone: schedule_timezone(schedule),
        };
    }
    if schedule.get("at").is_some() || schedule.get("time").is_some() {
        return NativeCronSchedule::At {
            text: string_field(schedule, &["at", "time"]).map(ToString::to_string),
            epoch_ms: i64_field(
                schedule,
                &["atMs", "at_ms", "timeMs", "time_ms", "epochMs", "epoch_ms"],
            ),
        };
    }

    NativeCronSchedule::Unknown {
        summary: compact_json_string(schedule).unwrap_or_else(|| "unknown".to_string()),
    }
}

fn schedule_timezone(schedule: &Value) -> Option<String> {
    string_field(schedule, &["tz", "timezone", "timeZone"]).map(ToString::to_string)
}

fn extract_message_text(job: &Value) -> Option<String> {
    job.get("payload")
        .and_then(|payload| {
            first_text_for_keys(
                payload,
                &[
                    "message",
                    "text",
                    "prompt",
                    "input",
                    "body",
                    "content",
                    "task",
                    "instruction",
                ],
            )
        })
        .or_else(|| {
            first_text_for_keys(
                job,
                &[
                    "message",
                    "text",
                    "prompt",
                    "input",
                    "body",
                    "content",
                    "task",
                    "instruction",
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

fn compact_json_string(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(text) => Some(text.clone()),
        _ => serde_json::to_string(value).ok(),
    }
}

fn looks_like_cron(value: &str) -> bool {
    value.split_whitespace().count() >= 5
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

fn cron_session_key(job_id: &str, agent_id: &str) -> String {
    format!(
        "cron:{}:{}",
        normalize_key_part(job_id),
        normalize_key_part(agent_id)
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

fn summarize_store(
    jobs: &[NativeCronJob],
    states: &[NativeCronJobState],
) -> NativeCronStoreSummary {
    let mut summary = NativeCronStoreSummary {
        total_jobs: jobs.len(),
        state_entries: states.len(),
        ..NativeCronStoreSummary::default()
    };
    for job in jobs {
        if job.enabled {
            summary.enabled_jobs += 1;
        } else {
            summary.disabled_jobs += 1;
        }
    }
    summary
}

fn summarize_plan(entries: &[NativeCronPlanEntry]) -> NativeCronPlanSummary {
    let mut summary = NativeCronPlanSummary {
        total_jobs: entries.len(),
        ..NativeCronPlanSummary::default()
    };
    for entry in entries {
        match entry.action {
            NativeCronPlanAction::Disabled => summary.disabled += 1,
            NativeCronPlanAction::CutoverHold => summary.cutover_held += 1,
            NativeCronPlanAction::EnqueueAgentTurn => summary.enqueue_agent_turns += 1,
            NativeCronPlanAction::WaitingSchedule => summary.waiting_schedule += 1,
            NativeCronPlanAction::CronRegistered => summary.cron_registered += 1,
            NativeCronPlanAction::MissingAgent => summary.missing_agent += 1,
            NativeCronPlanAction::UnsupportedSchedule => summary.unsupported_schedule += 1,
        }
    }
    summary
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::load_agent_registry;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn load_native_cron_store_parses_jobs_and_state() {
        let root = temp_root("load_native_cron_store_parses_jobs_and_state");
        let source = write_cron_source(&root);

        let store = load_native_cron_store(&source).unwrap();

        assert_eq!(store.summary.total_jobs, 7);
        assert_eq!(store.summary.enabled_jobs, 6);
        assert_eq!(store.summary.disabled_jobs, 1);
        assert_eq!(store.summary.state_entries, 2);
        let cron_job = store.jobs.iter().find(|job| job.id == "cron-job").unwrap();
        assert_eq!(cron_job.agent_id.as_deref(), Some("main"));
        assert_eq!(cron_job.message_text.as_deref(), Some("Run memory cron"));
        assert!(matches!(
            cron_job.schedule,
            NativeCronSchedule::Cron { ref expression, .. } if expression == "*/5 * * * *"
        ));
        let expr_cron_job = store
            .jobs
            .iter()
            .find(|job| job.id == "expr-cron-job")
            .unwrap();
        assert!(matches!(
            expr_cron_job.schedule,
            NativeCronSchedule::Cron {
                ref expression,
                timezone: Some(ref timezone),
            } if expression == "10 9 * * *" && timezone == "Asia/Taipei"
        ));
        let at_job = store.jobs.iter().find(|job| job.id == "at-due").unwrap();
        assert!(matches!(
            at_job.schedule,
            NativeCronSchedule::At {
                epoch_ms: Some(1000),
                ..
            }
        ));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn plan_native_cron_holds_enabled_jobs_until_resume() {
        let root = temp_root("plan_native_cron_holds_enabled_jobs_until_resume");
        let source = write_cron_source(&root);
        let store = load_native_cron_store(&source).unwrap();
        let registry = load_agent_registry(&source).unwrap();

        let plan = plan_native_cron(
            &store,
            &registry,
            NativeCronPlanInput {
                now_ms: 2000,
                resume_enabled: false,
            },
        );

        assert_eq!(plan.summary.total_jobs, 7);
        assert_eq!(plan.summary.disabled, 1);
        assert_eq!(plan.summary.cutover_held, 6);
        assert_eq!(plan.summary.enqueue_agent_turns, 0);
        assert!(
            plan.warnings
                .iter()
                .any(|warning| warning.contains("cutover catch-up"))
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn plan_native_cron_classifies_resume_actions() {
        let root = temp_root("plan_native_cron_classifies_resume_actions");
        let source = write_cron_source(&root);
        let store = load_native_cron_store(&source).unwrap();
        let registry = load_agent_registry(&source).unwrap();

        let plan = plan_native_cron(
            &store,
            &registry,
            NativeCronPlanInput {
                now_ms: 2000,
                resume_enabled: true,
            },
        );

        assert_eq!(plan.summary.disabled, 1);
        assert_eq!(plan.summary.enqueue_agent_turns, 1);
        assert_eq!(plan.summary.waiting_schedule, 1);
        assert_eq!(plan.summary.cron_registered, 2);
        assert_eq!(plan.summary.missing_agent, 1);
        assert_eq!(plan.summary.unsupported_schedule, 1);

        let due = plan
            .entries
            .iter()
            .find(|entry| entry.job_id == "at-due")
            .unwrap();
        assert_eq!(due.action, NativeCronPlanAction::EnqueueAgentTurn);
        assert_eq!(due.session_key.as_deref(), Some("cron:at-due:main"));
        assert_eq!(due.last_run_at_ms, Some(900));

        let cron = plan
            .entries
            .iter()
            .find(|entry| entry.job_id == "cron-job")
            .unwrap();
        assert_eq!(cron.action, NativeCronPlanAction::CronRegistered);

        let missing = plan
            .entries
            .iter()
            .find(|entry| entry.job_id == "missing-agent")
            .unwrap();
        assert_eq!(missing.action, NativeCronPlanAction::MissingAgent);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn write_native_cron_plan_outputs_json() {
        let root = temp_root("write_native_cron_plan_outputs_json");
        let source = write_cron_source(&root);
        let store = load_native_cron_store(&source).unwrap();
        let registry = load_agent_registry(&source).unwrap();
        let plan = plan_native_cron(
            &store,
            &registry,
            NativeCronPlanInput {
                now_ms: 2000,
                resume_enabled: true,
            },
        );

        let file = write_native_cron_plan(&plan, root.join("out")).unwrap();

        assert!(file.json.is_file());
        let json: serde_json::Value =
            serde_json::from_slice(&fs::read(file.json).unwrap()).unwrap();
        assert_eq!(json["schema"], NATIVE_CRON_PLAN_SCHEMA);
        assert_eq!(json["summary"]["totalJobs"], 7);

        let _ = fs::remove_dir_all(root);
    }

    fn write_cron_source(root: &Path) -> AgentSource {
        let home = root.join(".openclaw");
        let workspace = home.join("workspace");
        let cron = home.join("cron");
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(&cron).unwrap();
        fs::create_dir_all(home.join("agents").join("main").join("sessions")).unwrap();
        fs::write(
            home.join("openclaw.json"),
            r#"{
              "agents": {
                "defaults": { "provider": "openai", "model": "codex" },
                "list": [
                  { "id": "main", "enabled": true }
                ]
              }
            }"#,
        )
        .unwrap();
        fs::write(
            cron.join("jobs.json"),
            r#"{
              "jobs": [
                {
                  "id": "cron-job",
                  "enabled": true,
                  "agentId": "main",
                  "schedule": { "type": "cron", "expression": "*/5 * * * *" },
                  "wakeMode": "now",
                  "payload": { "message": "Run memory cron" }
                },
                {
                  "id": "expr-cron-job",
                  "enabled": true,
                  "agentId": "main",
                  "schedule": { "kind": "cron", "expr": "10 9 * * *", "tz": "Asia/Taipei" },
                  "payload": { "message": "Run expr cron" }
                },
                {
                  "id": "at-due",
                  "enabled": true,
                  "agentId": "main",
                  "schedule": { "type": "at", "atMs": 1000 },
                  "payload": { "text": "Due one-shot" }
                },
                {
                  "id": "at-future",
                  "enabled": true,
                  "agentId": "main",
                  "schedule": { "type": "at", "atMs": 5000 },
                  "payload": { "text": "Future one-shot" }
                },
                {
                  "id": "disabled-job",
                  "enabled": false,
                  "agentId": "main",
                  "schedule": { "type": "cron", "expression": "* * * * *" }
                },
                {
                  "id": "missing-agent",
                  "enabled": true,
                  "agentId": "ghost",
                  "schedule": { "type": "at", "atMs": 1000 }
                },
                {
                  "id": "unsupported",
                  "enabled": true,
                  "agentId": "main",
                  "schedule": { "type": "at", "time": "2026-06-08T00:00:00Z" }
                }
              ]
            }"#,
        )
        .unwrap();
        fs::write(
            cron.join("jobs-state.json"),
            r#"{
              "jobs": {
                "at-due": { "lastRunAtMs": 900 },
                "cron-job": { "nextRunAtMs": 3000 }
              }
            }"#,
        )
        .unwrap();
        AgentSource::with_workspace(home, workspace)
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-cron-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
