use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    ChannelOutboundMessage, ChannelOutboundMessageKind, SKILL_FILE_NAME, WorkerEnqueueOptions,
    WorkerEnqueueReport, WorkerJobKind, append_channel_outbox_message, append_jsonl_value,
    config::harness_config_candidates, enqueue_worker_job,
};

const SELF_IMPROVEMENT_REVIEW_SCHEMA: &str = "agent-harness.self-improvement-review.v1";
const SELF_IMPROVEMENT_DEFAULT_DAILY_CAP: usize = 24;
const SELF_IMPROVEMENT_DEFAULT_MAX_SELECTED_SKILLS: usize = 1;
const SKILL_SYNTHESIS_DEFAULT_DAILY_CAP: usize = 3;
const SKILL_SYNTHESIS_DEFAULT_MIN_TOOL_CALLS: usize = 5;
const SKILL_SYNTHESIS_DEFAULT_MIN_ASSISTANT_CHARS: usize = 600;
const DAY_MS: i64 = 86_400_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SelfImprovementReviewMode {
    ProposeOnly,
    DispatchAndReplace,
}

impl SelfImprovementReviewMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ProposeOnly => "propose-only",
            Self::DispatchAndReplace => "dispatch-and-replace",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SelfImprovementReviewConfig {
    pub enabled: bool,
    pub mode: SelfImprovementReviewMode,
    pub notify: bool,
    pub daily_cap: usize,
    pub max_selected_skills: usize,
    pub skill_synthesis_enabled: bool,
    pub skill_synthesis_autonomous_apply: bool,
    pub skill_synthesis_daily_cap: usize,
    pub skill_synthesis_min_tool_calls: usize,
    pub skill_synthesis_min_assistant_chars: usize,
    pub warnings: Vec<String>,
}

impl Default for SelfImprovementReviewConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            mode: SelfImprovementReviewMode::DispatchAndReplace,
            notify: true,
            daily_cap: SELF_IMPROVEMENT_DEFAULT_DAILY_CAP,
            max_selected_skills: SELF_IMPROVEMENT_DEFAULT_MAX_SELECTED_SKILLS,
            skill_synthesis_enabled: true,
            skill_synthesis_autonomous_apply: false,
            skill_synthesis_daily_cap: SKILL_SYNTHESIS_DEFAULT_DAILY_CAP,
            skill_synthesis_min_tool_calls: SKILL_SYNTHESIS_DEFAULT_MIN_TOOL_CALLS,
            skill_synthesis_min_assistant_chars: SKILL_SYNTHESIS_DEFAULT_MIN_ASSISTANT_CHARS,
            warnings: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelfImprovementReviewHookOptions {
    pub harness_home: PathBuf,
    pub prompt_bundle_json: PathBuf,
    pub assistant_text: String,
    pub queue_id: Option<String>,
    pub session_key: Option<String>,
    pub agent_id: Option<String>,
    pub notification_target: Option<SelfImprovementNotificationTarget>,
    pub tool_call_count: usize,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SelfImprovementNotificationTarget {
    pub platform: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_id: Option<String>,
    pub channel_id: String,
    pub user_id: String,
    pub session_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SelfImprovementReviewHookReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub receipts_file: PathBuf,
    pub status: String,
    pub mode: SelfImprovementReviewMode,
    pub jobs_enqueued: usize,
    pub job_ids: Vec<String>,
    pub target_skill_ids: Vec<String>,
    #[serde(default)]
    pub skill_synthesis_enqueued: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skill_synthesis_job_id: Option<String>,
    pub reason: String,
    pub warnings: Vec<String>,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SelfImprovementSkillTarget {
    skill_id: String,
    target_path: PathBuf,
}

pub fn self_improvement_review_receipts_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("learning")
        .join("self-improvement-review-receipts.jsonl")
}

pub fn load_self_improvement_review_config(
    harness_home: impl AsRef<Path>,
) -> io::Result<SelfImprovementReviewConfig> {
    let harness_home = harness_home.as_ref();
    let mut config = SelfImprovementReviewConfig::default();
    let Some(config_file) = harness_config_candidates(harness_home)
        .into_iter()
        .find(|path| path.is_file())
    else {
        return Ok(config);
    };
    let text = fs::read_to_string(&config_file)?;
    let value: Value = match serde_json::from_str(&text) {
        Ok(value) => value,
        Err(error) => {
            config.warnings.push(format!(
                "self-improvement review config ignored because {} is invalid JSON: {}",
                config_file.display(),
                error
            ));
            return Ok(config);
        }
    };
    let Some(section) = value
        .get("learning")
        .and_then(|learning| learning.get("selfImprovementReview"))
        .and_then(Value::as_object)
    else {
        return Ok(config);
    };
    if let Some(enabled) = section.get("enabled").and_then(Value::as_bool) {
        config.enabled = enabled;
    }
    if let Some(notify) = section.get("notify").and_then(Value::as_bool) {
        config.notify = notify;
    }
    if let Some(mode) = section
        .get("mode")
        .or_else(|| section.get("applyMode"))
        .and_then(Value::as_str)
    {
        if matches!(
            mode.trim().to_ascii_lowercase().as_str(),
            "off" | "disabled"
        ) {
            config.enabled = false;
        } else {
            config.mode = match normalize_mode(mode) {
                Some(mode) => mode,
                None => {
                    config.warnings.push(format!(
                        "unknown selfImprovementReview mode `{mode}`; using dispatch-and-replace"
                    ));
                    SelfImprovementReviewMode::DispatchAndReplace
                }
            };
        }
    }
    if let Some(cap) = section
        .get("dailyCap")
        .or_else(|| section.get("dailyJobCap"))
        .and_then(Value::as_u64)
    {
        config.daily_cap = usize::try_from(cap)
            .unwrap_or(SELF_IMPROVEMENT_DEFAULT_DAILY_CAP)
            .max(1);
    }
    if let Some(max_selected) = section.get("maxSelectedSkills").and_then(Value::as_u64) {
        config.max_selected_skills = usize::try_from(max_selected)
            .unwrap_or(SELF_IMPROVEMENT_DEFAULT_MAX_SELECTED_SKILLS)
            .max(1);
    }
    if let Some(section) = value
        .get("learning")
        .and_then(|learning| learning.get("skillSynthesis"))
        .and_then(Value::as_object)
    {
        if let Some(enabled) = section.get("enabled").and_then(Value::as_bool) {
            config.skill_synthesis_enabled = enabled;
        }
        if let Some(mode) = section.get("mode").and_then(Value::as_str) {
            match mode.trim().to_ascii_lowercase().as_str() {
                "off" | "disabled" => config.skill_synthesis_enabled = false,
                "propose-only" | "propose" | "propose-record-only" | "record-only" | "record" => {
                    config.skill_synthesis_autonomous_apply = false;
                }
                "auto" | "apply" | "dispatch-and-replace" | "dispatch-and-replacement" => {
                    config.skill_synthesis_autonomous_apply = true;
                }
                other => config.warnings.push(format!(
                    "unknown skillSynthesis mode `{other}`; using propose-only"
                )),
            }
        }
        if let Some(cap) = section.get("dailyCap").and_then(Value::as_u64) {
            config.skill_synthesis_daily_cap = usize::try_from(cap)
                .unwrap_or(SKILL_SYNTHESIS_DEFAULT_DAILY_CAP)
                .max(1);
        }
        if let Some(min_tool_calls) = section.get("minToolCalls").and_then(Value::as_u64) {
            config.skill_synthesis_min_tool_calls =
                usize::try_from(min_tool_calls).unwrap_or(SKILL_SYNTHESIS_DEFAULT_MIN_TOOL_CALLS);
        }
        if let Some(min_assistant_chars) = section.get("minAssistantChars").and_then(Value::as_u64)
        {
            config.skill_synthesis_min_assistant_chars = usize::try_from(min_assistant_chars)
                .unwrap_or(SKILL_SYNTHESIS_DEFAULT_MIN_ASSISTANT_CHARS);
        }
    }
    Ok(config)
}

pub fn run_self_improvement_review_hook(
    options: SelfImprovementReviewHookOptions,
) -> io::Result<SelfImprovementReviewHookReport> {
    let config = load_self_improvement_review_config(&options.harness_home)?;
    let receipts_file = self_improvement_review_receipts_file(&options.harness_home);
    let mut warnings = config.warnings.clone();
    if !config.enabled {
        return write_report(SelfImprovementReviewHookReport {
            schema: SELF_IMPROVEMENT_REVIEW_SCHEMA,
            harness_home: options.harness_home,
            receipts_file,
            status: "disabled".to_string(),
            mode: config.mode,
            jobs_enqueued: 0,
            job_ids: Vec::new(),
            target_skill_ids: Vec::new(),
            skill_synthesis_enqueued: false,
            skill_synthesis_job_id: None,
            reason: "self-improvement review is disabled by config".to_string(),
            warnings,
            now_ms: options.now_ms,
        });
    }
    let targets = selected_skill_targets(&options.prompt_bundle_json, config.max_selected_skills)?;
    if targets.is_empty() {
        if let Some((report, skill_id)) =
            maybe_enqueue_skill_synthesis(&options, &config, &receipts_file, &mut warnings)?
        {
            let job_id = report.job.job_id.clone();
            let inserted = report.inserted;
            if !inserted {
                warnings.push(format!(
                    "skill synthesis reused existing worker job {}: {}",
                    report.job.job_id, report.reason
                ));
            }
            return write_report(SelfImprovementReviewHookReport {
                schema: SELF_IMPROVEMENT_REVIEW_SCHEMA,
                harness_home: options.harness_home,
                receipts_file,
                status: if inserted { "enqueued" } else { "skipped" }.to_string(),
                mode: config.mode,
                jobs_enqueued: usize::from(inserted),
                job_ids: vec![job_id.clone()],
                target_skill_ids: vec![skill_id],
                skill_synthesis_enqueued: inserted,
                skill_synthesis_job_id: Some(job_id),
                reason: if inserted {
                    "skill synthesis job enqueued after completed no-skill runtime turn"
                } else {
                    "skill synthesis debounce reused an existing worker job"
                }
                .to_string(),
                warnings,
                now_ms: options.now_ms,
            });
        }
        return write_report(SelfImprovementReviewHookReport {
            schema: SELF_IMPROVEMENT_REVIEW_SCHEMA,
            harness_home: options.harness_home,
            receipts_file,
            status: "skipped".to_string(),
            mode: config.mode,
            jobs_enqueued: 0,
            job_ids: Vec::new(),
            target_skill_ids: Vec::new(),
            skill_synthesis_enqueued: false,
            skill_synthesis_job_id: None,
            reason: "completed turn had no concrete selected skill target".to_string(),
            warnings,
            now_ms: options.now_ms,
        });
    }

    let mut job_ids = Vec::new();
    let mut target_skill_ids = Vec::new();
    for target in targets {
        let payload = json!({
            "source": "runtime-completion-self-improvement",
            "mode": config.mode.as_str(),
            "notify": config.notify,
            "targetSkillId": target.skill_id,
            "targetPath": target.target_path,
            "agentId": options.agent_id.clone(),
            "channelTrust": "operator",
            "signalText": self_improvement_signal_text(&options.assistant_text),
            "sourceTurn": options.queue_id.clone(),
            "dailyCap": config.daily_cap,
            "notificationTarget": options.notification_target.clone(),
        });
        let skill_id = payload
            .get("targetSkillId")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string();
        let report = enqueue_worker_job(WorkerEnqueueOptions {
            harness_home: options.harness_home.clone(),
            kind: WorkerJobKind::LearningReview,
            lane: Some("learning_review".to_string()),
            payload,
            idempotency_key: Some(format!(
                "self-improvement:{}:{}",
                options.queue_id.as_deref().unwrap_or("no-queue"),
                skill_id
            )),
            parent_job_id: None,
            job_group_id: options.queue_id.clone(),
            master_agent_id: options.agent_id.clone(),
            master_session_key: options.session_key.clone(),
            wake_policy: None,
            source: Some("runtime-completion-self-improvement".to_string()),
            priority: 50,
            available_at_ms: Some(options.now_ms),
            max_attempts: 1,
            timeout_ms: Some(300_000),
            cascade_timeout_ms: None,
            rate_key: Some(format!("self-improvement:{skill_id}")),
            concurrency_group_key: Some(format!("self-improvement:{skill_id}")),
            now_ms: options.now_ms,
        })?;
        collect_enqueue_report(&report, &mut job_ids, &mut warnings);
        target_skill_ids.push(skill_id);
    }

    write_report(SelfImprovementReviewHookReport {
        schema: SELF_IMPROVEMENT_REVIEW_SCHEMA,
        harness_home: options.harness_home,
        receipts_file,
        status: "enqueued".to_string(),
        mode: config.mode,
        jobs_enqueued: job_ids.len(),
        job_ids,
        target_skill_ids,
        skill_synthesis_enqueued: false,
        skill_synthesis_job_id: None,
        reason: "self-improvement review job enqueued after completed runtime turn".to_string(),
        warnings,
        now_ms: options.now_ms,
    })
}

pub fn append_self_improvement_notification(
    harness_home: impl AsRef<Path>,
    target: &SelfImprovementNotificationTarget,
    text: String,
) -> io::Result<PathBuf> {
    let harness_home = harness_home.as_ref();
    let mut message = ChannelOutboundMessage {
        platform: target.platform.clone(),
        account_id: target.account_id.clone(),
        channel_id: target.channel_id.clone(),
        user_id: target.user_id.clone(),
        session_key: target.session_key.clone(),
        delivery_id: None,
        kind: ChannelOutboundMessageKind::CommandReply,
        source_queue_id: None,
        source_completion_file: None,
        text,
        presentation: None,
        delivery_intent: None,
        attachments: Vec::new(),
    };
    let append = append_channel_outbox_message(harness_home, &mut message)?;
    let wake_file = harness_home
        .join("state")
        .join("wake")
        .join("final-outbox.json");
    let _ = crate::wake::signal_wake(
        harness_home,
        wake_file,
        "final-outbox",
        "self-improvement notification appended",
    );
    Ok(append.outbox_file)
}

fn collect_enqueue_report(
    report: &WorkerEnqueueReport,
    job_ids: &mut Vec<String>,
    warnings: &mut Vec<String>,
) {
    job_ids.push(report.job.job_id.clone());
    if !report.inserted {
        warnings.push(format!(
            "self-improvement review reused existing worker job {}: {}",
            report.job.job_id, report.reason
        ));
    }
}

fn maybe_enqueue_skill_synthesis(
    options: &SelfImprovementReviewHookOptions,
    config: &SelfImprovementReviewConfig,
    receipts_file: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<Option<(WorkerEnqueueReport, String)>> {
    if !config.skill_synthesis_enabled {
        warnings.push(
            "skill synthesis skipped because learning.skillSynthesis is disabled".to_string(),
        );
        return Ok(None);
    }
    let assistant_chars = options.assistant_text.chars().count();
    let complex_enough = options.tool_call_count >= config.skill_synthesis_min_tool_calls
        || assistant_chars >= config.skill_synthesis_min_assistant_chars;
    if !complex_enough {
        warnings.push(format!(
            "skill synthesis skipped because complexity gate was not met: toolCalls={} minToolCalls={} assistantChars={} minAssistantChars={}",
            options.tool_call_count,
            config.skill_synthesis_min_tool_calls,
            assistant_chars,
            config.skill_synthesis_min_assistant_chars
        ));
        return Ok(None);
    }
    let today_count = count_skill_synthesis_enqueued_today(receipts_file, options.now_ms)?;
    if today_count >= config.skill_synthesis_daily_cap {
        warnings.push(format!(
            "skill synthesis skipped because daily cap {} is already reached",
            config.skill_synthesis_daily_cap
        ));
        return Ok(None);
    }

    let skill_slug = skill_synthesis_slug(&options.assistant_text, options.queue_id.as_deref());
    let skill_id = format!("agent-created:{skill_slug}");
    let payload = json!({
        "source": "runtime-completion-skill-synthesis",
        "skillId": skill_id.clone(),
        "taskSummary": skill_synthesis_summary(&options.assistant_text),
        "evidence": self_improvement_signal_text(&options.assistant_text),
        "sourceTurn": options.queue_id.clone(),
        "agentId": options.agent_id.clone(),
        "proposeOnly": !config.skill_synthesis_autonomous_apply,
        "applyAuthorized": config.skill_synthesis_autonomous_apply,
        "toolCallCount": options.tool_call_count,
    });
    let report = enqueue_worker_job(WorkerEnqueueOptions {
        harness_home: options.harness_home.clone(),
        kind: WorkerJobKind::SkillSynthesis,
        lane: Some("skill_synthesis".to_string()),
        payload,
        idempotency_key: Some(format!(
            "skill-synthesis:{}",
            options.queue_id.as_deref().unwrap_or(skill_slug.as_str())
        )),
        parent_job_id: None,
        job_group_id: options.queue_id.clone(),
        master_agent_id: options.agent_id.clone(),
        master_session_key: options.session_key.clone(),
        wake_policy: None,
        source: Some("runtime-completion-skill-synthesis".to_string()),
        priority: 45,
        available_at_ms: Some(options.now_ms),
        max_attempts: 1,
        timeout_ms: Some(300_000),
        cascade_timeout_ms: None,
        rate_key: Some(format!("skill-synthesis:{skill_slug}")),
        concurrency_group_key: Some(format!("skill-synthesis:{skill_slug}")),
        now_ms: options.now_ms,
    })?;
    Ok(Some((report, skill_id)))
}

fn count_skill_synthesis_enqueued_today(receipts_file: &Path, now_ms: i64) -> io::Result<usize> {
    if !receipts_file.is_file() {
        return Ok(0);
    }
    let day = now_ms.div_euclid(DAY_MS);
    let text = fs::read_to_string(receipts_file)?;
    Ok(text
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|value| {
            value
                .get("skillSynthesisEnqueued")
                .and_then(Value::as_bool)
                .unwrap_or(false)
                && value
                    .get("nowMs")
                    .and_then(Value::as_i64)
                    .is_some_and(|then| then.div_euclid(DAY_MS) == day)
        })
        .count())
}

fn skill_synthesis_slug(text: &str, fallback: Option<&str>) -> String {
    let source = text
        .split_whitespace()
        .take(8)
        .collect::<Vec<_>>()
        .join(" ");
    let mut slug = String::new();
    let mut last_dash = false;
    for ch in source.chars() {
        let lower = ch.to_ascii_lowercase();
        if lower.is_ascii_alphanumeric() {
            slug.push(lower);
            last_dash = false;
        } else if !last_dash && !slug.is_empty() {
            slug.push('-');
            last_dash = true;
        }
        if slug.len() >= 48 {
            break;
        }
    }
    while slug.ends_with('-') {
        slug.pop();
    }
    if slug.is_empty() {
        let fallback_slug = fallback
            .unwrap_or("learned-workflow")
            .chars()
            .filter(|ch| ch.is_ascii_alphanumeric() || *ch == '-')
            .take(48)
            .collect::<String>();
        if fallback_slug.is_empty() {
            "learned-workflow".to_string()
        } else {
            fallback_slug
        }
    } else {
        slug
    }
}

fn skill_synthesis_summary(text: &str) -> String {
    let mut summary = text.trim().replace(['\r', '\n'], " ");
    if summary.chars().count() > 220 {
        summary = summary.chars().take(220).collect();
    }
    if summary.is_empty() {
        "Learn a reusable workflow from a completed no-skill turn".to_string()
    } else {
        summary
    }
}

fn write_report(
    report: SelfImprovementReviewHookReport,
) -> io::Result<SelfImprovementReviewHookReport> {
    append_jsonl_value(&report.receipts_file, &report)?;
    Ok(report)
}

fn normalize_mode(value: &str) -> Option<SelfImprovementReviewMode> {
    match value.trim().to_ascii_lowercase().as_str() {
        "propose"
        | "propose-only"
        | "propose_record_only"
        | "propose-record-only"
        | "record-only"
        | "record" => Some(SelfImprovementReviewMode::ProposeOnly),
        "dispatch-and-replace"
        | "dispatch-and-replacement"
        | "dispatch_and_replace"
        | "dispatch_and_replacement"
        | "auto"
        | "apply" => Some(SelfImprovementReviewMode::DispatchAndReplace),
        "off" | "disabled" => Some(SelfImprovementReviewMode::ProposeOnly),
        _ => None,
    }
}

fn selected_skill_targets(
    path: &Path,
    limit: usize,
) -> io::Result<Vec<SelfImprovementSkillTarget>> {
    let text = fs::read_to_string(path)?;
    let value: Value = serde_json::from_str(&text).map_err(io::Error::other)?;
    let mut targets = Vec::new();
    if let Some(sections) = value.get("sections").and_then(Value::as_array) {
        for section in sections {
            if section.get("kind").and_then(Value::as_str) != Some("skill") {
                continue;
            }
            let Some(skill_id) = section.get("skillId").and_then(Value::as_str) else {
                continue;
            };
            let Some(path) = section.get("path").and_then(Value::as_str) else {
                continue;
            };
            targets.push(SelfImprovementSkillTarget {
                skill_id: skill_id.to_string(),
                target_path: PathBuf::from(path),
            });
            if targets.len() >= limit {
                return Ok(targets);
            }
        }
    }
    if let Some(selections) = value.get("selectedSkills").and_then(Value::as_array) {
        for selection in selections {
            let Some(skill_id) = selection.get("skillId").and_then(Value::as_str) else {
                continue;
            };
            let Some(directory) = selection.get("directory").and_then(Value::as_str) else {
                continue;
            };
            targets.push(SelfImprovementSkillTarget {
                skill_id: skill_id.to_string(),
                target_path: PathBuf::from(directory).join(SKILL_FILE_NAME),
            });
            if targets.len() >= limit {
                break;
            }
        }
    }
    Ok(targets)
}

fn self_improvement_signal_text(text: &str) -> String {
    let normalized = text.trim();
    let capped = normalized.chars().take(4_000).collect::<String>();
    format!("post-turn self-improvement review signal:\n{capped}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn self_improvement_config_defaults_to_enabled_dispatch_replace() {
        let root = temp_root("self_improvement_config_defaults_to_enabled_dispatch_replace");
        let config = load_self_improvement_review_config(root.join(".agent-harness")).unwrap();
        assert!(config.enabled);
        assert_eq!(config.mode, SelfImprovementReviewMode::DispatchAndReplace);
        assert!(config.notify);
    }

    #[test]
    fn self_improvement_hook_enqueues_learning_review_for_selected_skill() {
        let root = temp_root("self_improvement_hook_enqueues_learning_review_for_selected_skill");
        let harness_home = root.join(".agent-harness");
        let skill_dir = root
            .join("workspace")
            .join("skills")
            .join("quiet-cron-watchdogs");
        let skill_file = skill_dir.join(SKILL_FILE_NAME);
        fs::create_dir_all(&skill_dir).unwrap();
        fs::write(&skill_file, "# Quiet Cron Watchdogs\n").unwrap();
        let prompt_bundle = root.join("prompt-bundle.json");
        fs::write(
            &prompt_bundle,
            serde_json::to_string(&json!({
                "schema": "agent-harness.prompt-bundle.v1",
                "selectedSkills": [{
                    "skillId": "workspace:quiet-cron-watchdogs",
                    "directory": skill_dir
                }],
                "sections": []
            }))
            .unwrap(),
        )
        .unwrap();

        let report = run_self_improvement_review_hook(SelfImprovementReviewHookOptions {
            harness_home: harness_home.clone(),
            prompt_bundle_json: prompt_bundle,
            assistant_text: "completed cron watchdog work".to_string(),
            queue_id: Some("queue-1".to_string()),
            session_key: Some("session-1".to_string()),
            agent_id: Some("main".to_string()),
            notification_target: None,
            tool_call_count: 0,
            now_ms: 1000,
        })
        .unwrap();

        assert_eq!(report.status, "enqueued");
        assert_eq!(report.jobs_enqueued, 1);
        assert_eq!(
            report.target_skill_ids,
            vec!["workspace:quiet-cron-watchdogs".to_string()]
        );
        assert!(self_improvement_review_receipts_file(&harness_home).is_file());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn self_improvement_hook_enqueues_skill_synthesis_for_complex_no_skill_turn() {
        let root =
            temp_root("self_improvement_hook_enqueues_skill_synthesis_for_complex_no_skill_turn");
        let harness_home = root.join(".agent-harness");
        let prompt_bundle = root.join("prompt-bundle.json");
        fs::create_dir_all(&root).unwrap();
        fs::write(
            &prompt_bundle,
            serde_json::to_string(&json!({
                "schema": "agent-harness.prompt-bundle.v1",
                "selectedSkills": [],
                "sections": []
            }))
            .unwrap(),
        )
        .unwrap();

        let report = run_self_improvement_review_hook(SelfImprovementReviewHookOptions {
            harness_home: harness_home.clone(),
            prompt_bundle_json: prompt_bundle,
            assistant_text: "Debugged a novel flaky queue replay by collecting receipts, isolating the retry boundary, and adding a focused regression.".to_string(),
            queue_id: Some("queue-synthesis-1".to_string()),
            session_key: Some("session-1".to_string()),
            agent_id: Some("main".to_string()),
            notification_target: None,
            tool_call_count: 5,
            now_ms: 1000,
        })
        .unwrap();

        assert_eq!(report.status, "enqueued");
        assert_eq!(report.jobs_enqueued, 1);
        assert!(report.skill_synthesis_enqueued);
        assert_eq!(report.target_skill_ids.len(), 1);
        assert!(report.target_skill_ids[0].starts_with("agent-created:"));
        assert!(self_improvement_review_receipts_file(&harness_home).is_file());

        let _ = fs::remove_dir_all(root);
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-self-improvement-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
