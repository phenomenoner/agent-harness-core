use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    ChannelOutboundMessage, ChannelOutboundMessageKind, SKILL_FILE_NAME, WorkerEnqueueOptions,
    WorkerEnqueueReport, WorkerJobKind, append_jsonl_value, config::harness_config_candidates,
    enqueue_worker_job,
};

const SELF_IMPROVEMENT_REVIEW_SCHEMA: &str = "agent-harness.self-improvement-review.v1";
const SELF_IMPROVEMENT_DEFAULT_DAILY_CAP: usize = 24;
const SELF_IMPROVEMENT_DEFAULT_MAX_SELECTED_SKILLS: usize = 1;

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
    pub reason: String,
    pub warnings: Vec<String>,
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
            reason: "self-improvement review is disabled by config".to_string(),
            warnings,
        });
    }
    let targets = selected_skill_targets(&options.prompt_bundle_json, config.max_selected_skills)?;
    if targets.is_empty() {
        return write_report(SelfImprovementReviewHookReport {
            schema: SELF_IMPROVEMENT_REVIEW_SCHEMA,
            harness_home: options.harness_home,
            receipts_file,
            status: "skipped".to_string(),
            mode: config.mode,
            jobs_enqueued: 0,
            job_ids: Vec::new(),
            target_skill_ids: Vec::new(),
            reason: "completed turn had no concrete selected skill target".to_string(),
            warnings,
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
        reason: "self-improvement review job enqueued after completed runtime turn".to_string(),
        warnings,
    })
}

pub fn append_self_improvement_notification(
    harness_home: impl AsRef<Path>,
    target: &SelfImprovementNotificationTarget,
    text: String,
) -> io::Result<PathBuf> {
    let harness_home = harness_home.as_ref();
    let outbox_file = harness_home
        .join("state")
        .join("channels")
        .join("outbox.jsonl");
    let message = ChannelOutboundMessage {
        platform: target.platform.clone(),
        account_id: target.account_id.clone(),
        channel_id: target.channel_id.clone(),
        user_id: target.user_id.clone(),
        session_key: target.session_key.clone(),
        kind: ChannelOutboundMessageKind::CommandReply,
        text,
        delivery_intent: None,
        attachments: Vec::new(),
    };
    append_jsonl_value(&outbox_file, &message)?;
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
    Ok(outbox_file)
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
