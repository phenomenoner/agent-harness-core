use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    SkillUsageAction, SkillUsageEventOptions, append_jsonl_value, current_log_time_ms,
    record_skill_usage_event, skill_body_checksum,
};

const SKILL_PROPOSAL_SCHEMA: &str = "agent-harness.skill-proposal.v1";
const LEARNING_REVIEW_SCHEMA: &str = "agent-harness.learning-review.v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SkillLearningProposalOperation {
    Create,
    Patch,
    Replace,
    Archive,
}

impl SkillLearningProposalOperation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Patch => "patch",
            Self::Replace => "replace",
            Self::Archive => "archive",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SkillLearningProposalStatus {
    Proposed,
    Applied,
    Rejected,
    Archived,
    Quarantined,
}

impl SkillLearningProposalStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Proposed => "proposed",
            Self::Applied => "applied",
            Self::Rejected => "rejected",
            Self::Archived => "archived",
            Self::Quarantined => "quarantined",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillLearningSignal {
    pub kind: String,
    pub signal_hash: String,
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trust: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillStructuredPatch {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replacement_body: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub support_files: Vec<SkillSupportFileOperation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillSupportFileOperation {
    pub relative_path: PathBuf,
    pub body: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillLearningProposal {
    pub schema: String,
    pub proposal_id: String,
    pub target_skill_id: String,
    pub target_path: PathBuf,
    pub base_checksum: String,
    pub base_version: String,
    pub operation: SkillLearningProposalOperation,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diff: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub structured_patch: Option<SkillStructuredPatch>,
    #[serde(default)]
    pub signals: Vec<SkillLearningSignal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_turn: Option<String>,
    pub risk_class: String,
    pub status: SkillLearningProposalStatus,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillProposeOptions {
    pub harness_home: PathBuf,
    pub target_skill_id: String,
    pub target_path: PathBuf,
    pub operation: SkillLearningProposalOperation,
    pub replacement_body: Option<String>,
    pub support_files: Vec<SkillSupportFileOperation>,
    pub diff: Option<String>,
    pub signals: Vec<SkillLearningSignal>,
    pub source_turn: Option<String>,
    pub risk_class: String,
    pub status: SkillLearningProposalStatus,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillArchiveOptions {
    pub harness_home: PathBuf,
    pub target_skill_id: String,
    pub target_path: PathBuf,
    pub reason: String,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LearningReviewOptions {
    pub harness_home: PathBuf,
    pub agent_id: Option<String>,
    pub target_skill_id: Option<String>,
    pub target_path: Option<PathBuf>,
    pub channel_trust: Option<String>,
    pub signal_text: String,
    pub source_turn: Option<String>,
    pub daily_cap: usize,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillCuratorOptions {
    pub harness_home: PathBuf,
    pub target_skill_id: String,
    pub target_path: PathBuf,
    pub stale_event_threshold: usize,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LearningReviewReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub status: String,
    pub proposals_created: usize,
    pub proposal_ids: Vec<String>,
    pub reason: String,
}

pub fn skill_proposals_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("learning")
        .join("skill-proposals.jsonl")
}

pub fn create_skill_learning_proposal(
    options: SkillProposeOptions,
) -> io::Result<SkillLearningProposal> {
    let base_checksum = file_checksum_or_missing(&options.target_path)?;
    let base_version = base_checksum.clone();
    let structured_patch = (options.replacement_body.is_some()
        || !options.support_files.is_empty())
    .then(|| SkillStructuredPatch {
        replacement_body: options.replacement_body,
        support_files: options.support_files,
    });
    let proposal_id = proposal_id(
        &options.target_skill_id,
        options.operation,
        &base_checksum,
        &options.diff,
        options.now_ms,
    );
    let proposal = SkillLearningProposal {
        schema: SKILL_PROPOSAL_SCHEMA.to_string(),
        proposal_id: proposal_id.clone(),
        target_skill_id: options.target_skill_id.clone(),
        target_path: options.target_path,
        base_checksum,
        base_version,
        operation: options.operation,
        diff: options.diff,
        structured_patch,
        signals: options.signals,
        source_turn: options.source_turn,
        risk_class: options.risk_class,
        status: options.status,
        created_at_ms: options.now_ms,
    };
    append_jsonl_value(&skill_proposals_file(&options.harness_home), &proposal)?;
    let _ = record_skill_usage_event(SkillUsageEventOptions {
        harness_home: options.harness_home,
        action: SkillUsageAction::Proposed,
        skill_id: options.target_skill_id,
        source_kind: None,
        source_turn_id: proposal.source_turn.clone(),
        runtime_queue_id: None,
        session_key: None,
        channel: None,
        agent_id: None,
        delivery_mode: None,
        body_checksum: Some(proposal.base_checksum.clone()),
        selection_receipt_id: None,
        reason: Some(format!("proposal {}", proposal.operation.as_str())),
        now_ms: proposal.created_at_ms,
    });
    Ok(proposal)
}

pub fn create_skill_archive_proposal(
    options: SkillArchiveOptions,
) -> io::Result<SkillLearningProposal> {
    create_skill_learning_proposal(SkillProposeOptions {
        harness_home: options.harness_home,
        target_skill_id: options.target_skill_id,
        target_path: options.target_path,
        operation: SkillLearningProposalOperation::Archive,
        replacement_body: None,
        support_files: Vec::new(),
        diff: Some(format!("archive requested: {}", options.reason)),
        signals: vec![SkillLearningSignal {
            kind: "operator-archive".to_string(),
            signal_hash: stable_text_hash("skill-archive", &options.reason),
            text: options.reason,
            trust: Some("operator".to_string()),
        }],
        source_turn: None,
        risk_class: "medium".to_string(),
        status: SkillLearningProposalStatus::Proposed,
        now_ms: options.now_ms,
    })
}

pub fn run_learning_review(options: LearningReviewOptions) -> io::Result<LearningReviewReport> {
    let Some(target_skill_id) = options.target_skill_id.clone() else {
        return Ok(LearningReviewReport {
            schema: LEARNING_REVIEW_SCHEMA,
            harness_home: options.harness_home,
            status: "skipped".to_string(),
            proposals_created: 0,
            proposal_ids: Vec::new(),
            reason: "no target skill supplied for deterministic learning review".to_string(),
        });
    };
    let Some(target_path) = options.target_path.clone() else {
        return Ok(LearningReviewReport {
            schema: LEARNING_REVIEW_SCHEMA,
            harness_home: options.harness_home,
            status: "skipped".to_string(),
            proposals_created: 0,
            proposal_ids: Vec::new(),
            reason: "no target path supplied for deterministic learning review".to_string(),
        });
    };
    let signal = classify_learning_signal(&options.signal_text);
    let debounce_key = format!(
        "{}|{}|{}",
        options.agent_id.as_deref().unwrap_or("unknown"),
        target_skill_id,
        signal.signal_hash
    );
    if review_debounce_seen(&options.harness_home, &debounce_key)? {
        return Ok(LearningReviewReport {
            schema: LEARNING_REVIEW_SCHEMA,
            harness_home: options.harness_home,
            status: "debounced".to_string(),
            proposals_created: 0,
            proposal_ids: Vec::new(),
            reason: "matching learning signal was already proposed".to_string(),
        });
    }
    if daily_proposal_count(&options.harness_home, options.now_ms)? >= options.daily_cap.max(1) {
        return Ok(LearningReviewReport {
            schema: LEARNING_REVIEW_SCHEMA,
            harness_home: options.harness_home,
            status: "capped".to_string(),
            proposals_created: 0,
            proposal_ids: Vec::new(),
            reason: "daily deterministic learning proposal cap reached".to_string(),
        });
    }
    let status = if options.channel_trust.as_deref() == Some("operator") {
        SkillLearningProposalStatus::Proposed
    } else {
        SkillLearningProposalStatus::Quarantined
    };
    let proposal = create_skill_learning_proposal(SkillProposeOptions {
        harness_home: options.harness_home.clone(),
        target_skill_id,
        target_path,
        operation: SkillLearningProposalOperation::Patch,
        replacement_body: None,
        support_files: Vec::new(),
        diff: Some(options.signal_text),
        signals: vec![signal],
        source_turn: options.source_turn,
        risk_class: "low".to_string(),
        status,
        now_ms: options.now_ms,
    })?;
    record_review_debounce(&options.harness_home, &debounce_key, &proposal.proposal_id)?;
    Ok(LearningReviewReport {
        schema: LEARNING_REVIEW_SCHEMA,
        harness_home: options.harness_home,
        status: "proposed".to_string(),
        proposals_created: 1,
        proposal_ids: vec![proposal.proposal_id],
        reason: "deterministic learning signal recorded".to_string(),
    })
}

pub fn run_skill_curator(options: SkillCuratorOptions) -> io::Result<LearningReviewReport> {
    let usage_text = fs::read_to_string(crate::skill_usage_events_file(&options.harness_home))
        .unwrap_or_default();
    let mut selected_count = 0usize;
    let mut active_count = 0usize;
    for line in usage_text.lines().filter(|line| !line.trim().is_empty()) {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if value.get("skillId").and_then(Value::as_str) != Some(options.target_skill_id.as_str()) {
            continue;
        }
        match value.get("action").and_then(Value::as_str) {
            Some("selected") => selected_count += 1,
            Some("injected" | "invoked" | "patched") => active_count += 1,
            _ => {}
        }
    }
    if selected_count < options.stale_event_threshold.max(1) || active_count > 0 {
        return Ok(LearningReviewReport {
            schema: LEARNING_REVIEW_SCHEMA,
            harness_home: options.harness_home,
            status: "skipped".to_string(),
            proposals_created: 0,
            proposal_ids: Vec::new(),
            reason: "skill usage is not stale enough for archive proposal".to_string(),
        });
    }
    let proposal = create_skill_archive_proposal(SkillArchiveOptions {
        harness_home: options.harness_home.clone(),
        target_skill_id: options.target_skill_id,
        target_path: options.target_path,
        reason: format!("selected {selected_count} times without injected/invoked usage"),
        now_ms: options.now_ms,
    })?;
    Ok(LearningReviewReport {
        schema: LEARNING_REVIEW_SCHEMA,
        harness_home: options.harness_home,
        status: "proposed".to_string(),
        proposals_created: 1,
        proposal_ids: vec![proposal.proposal_id],
        reason: "deterministic stale usage curator proposal recorded".to_string(),
    })
}

fn classify_learning_signal(text: &str) -> SkillLearningSignal {
    let lower = text.to_ascii_lowercase();
    let kind = if lower.contains("save") || lower.contains("remember") || text.contains("記得") {
        "explicit-save-request"
    } else if (lower.contains("verified") || lower.contains("validated"))
        && (lower.contains("complex task") || lower.contains("multi-step"))
    {
        "verified-complex-task"
    } else if lower.contains("update") || lower.contains("revise") || text.contains("更新") {
        "explicit-update-request"
    } else if lower.contains("failed") || lower.contains("error") {
        if lower.contains("again") || lower.contains("repeat") || lower.contains("recur") {
            "repeated-error-signature"
        } else {
            "selected-skill-runtime-failure"
        }
    } else if lower.contains("actually") || lower.contains("correction") || text.contains("其實")
    {
        "workflow-correction"
    } else {
        "operator-command"
    };
    SkillLearningSignal {
        kind: kind.to_string(),
        signal_hash: stable_text_hash(kind, &normalize_error_signature("learning-review", text)),
        text: text.to_string(),
        trust: None,
    }
}

pub fn normalize_error_signature(stage: &str, error: &str) -> String {
    let lower = error.to_ascii_lowercase();
    let class = if lower.contains("timeout") {
        "timeout"
    } else if lower.contains("permission") || lower.contains("denied") {
        "permission"
    } else if lower.contains("checksum") || lower.contains("stale") {
        "stale-base"
    } else if lower.contains("parse") || lower.contains("json") {
        "parse"
    } else {
        "generic"
    };
    let excerpt = lower
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { ' ' })
        .collect::<String>()
        .split_whitespace()
        .take(16)
        .collect::<Vec<_>>()
        .join(" ");
    stable_text_hash("error-signature", &format!("{stage}|{class}|{excerpt}"))
}

fn file_checksum_or_missing(path: &Path) -> io::Result<String> {
    match fs::read_to_string(path) {
        Ok(text) => Ok(skill_body_checksum(&text)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok("missing".to_string()),
        Err(error) => Err(error),
    }
}

fn proposal_id(
    target_skill_id: &str,
    operation: SkillLearningProposalOperation,
    base_checksum: &str,
    diff: &Option<String>,
    now_ms: i64,
) -> String {
    stable_text_hash(
        "skill-proposal",
        &format!(
            "{target_skill_id}|{}|{base_checksum}|{}|{now_ms}",
            operation.as_str(),
            diff.as_deref().unwrap_or("")
        ),
    )
}

fn review_debounce_file(harness_home: &Path) -> PathBuf {
    harness_home
        .join("state")
        .join("learning")
        .join("learning-review-debounce.jsonl")
}

fn review_debounce_seen(harness_home: &Path, key: &str) -> io::Result<bool> {
    let text = match fs::read_to_string(review_debounce_file(harness_home)) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    Ok(text.lines().any(|line| line.contains(key)))
}

fn record_review_debounce(harness_home: &Path, key: &str, proposal_id: &str) -> io::Result<()> {
    append_jsonl_value(
        &review_debounce_file(harness_home),
        &serde_json::json!({
            "schema": "agent-harness.learning-review-debounce.v1",
            "key": key,
            "proposalId": proposal_id,
            "atMs": current_log_time_ms().unwrap_or(0)
        }),
    )
}

fn daily_proposal_count(harness_home: &Path, now_ms: i64) -> io::Result<usize> {
    let day_start = now_ms - now_ms.rem_euclid(86_400_000);
    let text = match fs::read_to_string(skill_proposals_file(harness_home)) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error),
    };
    let mut proposal_ids = BTreeSet::new();
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if value
            .get("createdAtMs")
            .and_then(Value::as_i64)
            .is_some_and(|at_ms| at_ms >= day_start)
            && let Some(id) = value.get("proposalId").and_then(Value::as_str)
        {
            proposal_ids.insert(id.to_string());
        }
    }
    Ok(proposal_ids.len())
}

fn stable_text_hash(namespace: &str, text: &str) -> String {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in namespace
        .as_bytes()
        .iter()
        .chain([0].iter())
        .chain(text.as_bytes())
    {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("fnv1a64:{hash:016x}")
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::{SkillUsageAction, SkillUsageEventOptions, record_skill_usage_event};

    use super::*;

    #[test]
    fn learning_review_classifies_verified_and_repeated_error_signals() {
        let verified = classify_learning_signal("verified complex task should update this skill");
        assert_eq!(verified.kind, "verified-complex-task");

        let repeated = classify_learning_signal("tool error happened again with the same timeout");
        assert_eq!(repeated.kind, "repeated-error-signature");
    }

    #[test]
    fn learning_review_debounces_signal_and_quarantines_lower_trust() {
        let root = temp_root("learning_review_debounces_signal_and_quarantines_lower_trust");
        let home = root.join(".openclaw");
        let skill = root.join("skill.md");
        fs::create_dir_all(&root).unwrap();
        fs::write(&skill, "# Skill\n").unwrap();
        let options = LearningReviewOptions {
            harness_home: home.clone(),
            agent_id: Some("main".to_string()),
            target_skill_id: Some("workspace:skill".to_string()),
            target_path: Some(skill),
            channel_trust: Some("chat".to_string()),
            signal_text: "please update this skill with the corrected workflow".to_string(),
            source_turn: Some("turn-1".to_string()),
            daily_cap: 3,
            now_ms: 86_400_000,
        };
        let report = run_learning_review(options.clone()).unwrap();
        assert_eq!(report.proposals_created, 1);
        let proposals = fs::read_to_string(skill_proposals_file(&home)).unwrap();
        assert!(proposals.contains("\"status\":\"quarantined\""));
        let debounced = run_learning_review(options).unwrap();
        assert_eq!(debounced.status, "debounced");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn skill_curator_creates_archive_proposal_from_stale_usage() {
        let root = temp_root("skill_curator_creates_archive_proposal_from_stale_usage");
        let home = root.join(".openclaw");
        let skill = root.join("skill.md");
        fs::create_dir_all(&root).unwrap();
        fs::write(&skill, "# Skill\n").unwrap();
        for index in 0..2 {
            record_skill_usage_event(SkillUsageEventOptions {
                harness_home: home.clone(),
                action: SkillUsageAction::Selected,
                skill_id: "workspace:old".to_string(),
                source_kind: None,
                source_turn_id: None,
                runtime_queue_id: None,
                session_key: None,
                channel: None,
                agent_id: None,
                delivery_mode: None,
                body_checksum: None,
                selection_receipt_id: None,
                reason: Some(format!("selected {index}")),
                now_ms: index,
            })
            .unwrap();
        }
        let report = run_skill_curator(SkillCuratorOptions {
            harness_home: home.clone(),
            target_skill_id: "workspace:old".to_string(),
            target_path: skill,
            stale_event_threshold: 2,
            now_ms: 123,
        })
        .unwrap();
        assert_eq!(report.proposals_created, 1);
        assert!(
            fs::read_to_string(skill_proposals_file(&home))
                .unwrap()
                .contains("\"operation\":\"archive\"")
        );

        let _ = fs::remove_dir_all(root);
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-skill-learning-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
