use std::io;
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::{
    SkillAutonomousApplyOptions, SkillAutonomousApplyReport, SkillLearningProposal,
    SkillLearningProposalOperation, SkillLearningProposalStatus, SkillProposeOptions,
    autonomous_apply_skill_proposal, create_skill_learning_proposal, current_log_time_ms,
};

const SKILL_SYNTHESIS_RECEIPT_SCHEMA: &str = "agent-harness.skill-synthesis-receipt.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillSynthesisOptions {
    pub harness_home: PathBuf,
    pub skill_id: String,
    pub task_summary: String,
    pub evidence: String,
    pub propose_only: bool,
    pub now_ms: i64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillSynthesisReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub skill_id: String,
    pub target_path: PathBuf,
    pub proposal: SkillLearningProposal,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub autonomous_apply: Option<SkillAutonomousApplyReport>,
    pub receipts_file: PathBuf,
}

pub fn skill_synthesis_receipts_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("skills")
        .join("synthesis-receipts.jsonl")
}

pub fn synthesize_skill(options: SkillSynthesisOptions) -> io::Result<SkillSynthesisReport> {
    let now_ms = if options.now_ms <= 0 {
        current_log_time_ms().unwrap_or(0)
    } else {
        options.now_ms
    };
    let leaf = skill_leaf(&options.skill_id);
    let skill_id = if options.skill_id.contains(':') {
        options.skill_id.clone()
    } else {
        format!("agent-created:{}", leaf)
    };
    let target_path = options
        .harness_home
        .join("skills")
        .join("agent-created")
        .join(leaf)
        .join(crate::SKILL_FILE_NAME);
    let body = render_synthesized_skill(leaf, &options.task_summary, &options.evidence);
    let proposal = create_skill_learning_proposal(SkillProposeOptions {
        harness_home: options.harness_home.clone(),
        target_skill_id: skill_id.clone(),
        target_path: target_path.clone(),
        operation: SkillLearningProposalOperation::Create,
        replacement_body: Some(body),
        support_files: Vec::new(),
        diff: Some("synthesized agent-created skill from completed task evidence".to_string()),
        signals: Vec::new(),
        source_turn: None,
        risk_class: "low".to_string(),
        status: SkillLearningProposalStatus::Proposed,
        now_ms,
    })?;
    let autonomous_apply = if options.propose_only {
        None
    } else {
        Some(autonomous_apply_skill_proposal(
            SkillAutonomousApplyOptions {
                harness_home: options.harness_home.clone(),
                proposal_id: proposal.proposal_id.clone(),
                reviewer: Some("skill-synthesis-autonomous-review".to_string()),
                now_ms,
            },
        )?)
    };
    let report = SkillSynthesisReport {
        schema: SKILL_SYNTHESIS_RECEIPT_SCHEMA,
        harness_home: options.harness_home.clone(),
        skill_id,
        target_path,
        proposal,
        autonomous_apply,
        receipts_file: skill_synthesis_receipts_file(&options.harness_home),
    };
    crate::append_jsonl_value(&report.receipts_file, &report)?;
    Ok(report)
}

fn render_synthesized_skill(leaf: &str, task_summary: &str, evidence: &str) -> String {
    let description = sentence(
        truncate_ascii_words(task_summary, 120)
            .trim()
            .trim_end_matches('.'),
    );
    format!(
        "---\nname: {leaf}\ndescription: {description}\ncategory: operations\ntags: [agent-created, learned]\nversion: 0.1.0\nauthor: agent-harness\n---\n# {title}\n\n## When To Use\n\nUse this skill when a task matches this learned pattern: {task_summary}\n\n## Procedure\n\n1. Review the current request and confirm it matches the pattern.\n2. Reuse the verified approach from the evidence below.\n3. Keep changes scoped and run the relevant verification before reporting completion.\n\n## Evidence\n\n{evidence}\n",
        title = title_case(leaf)
    )
}

fn skill_leaf(skill_id: &str) -> &str {
    skill_id
        .rsplit(|ch| ch == ':' || ch == '/' || ch == '\\')
        .find(|part| !part.trim().is_empty())
        .unwrap_or(skill_id)
}

fn sentence(value: &str) -> String {
    if value.ends_with('.') || value.ends_with('!') || value.ends_with('?') {
        value.to_string()
    } else {
        format!("{value}.")
    }
}

fn truncate_ascii_words(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_string();
    }
    let mut out = String::new();
    for word in value.split_whitespace() {
        if !out.is_empty() && out.len() + 1 + word.len() > max_bytes {
            break;
        }
        if out.is_empty() {
            if word.len() > max_bytes {
                break;
            }
            out.push_str(word);
        } else {
            out.push(' ');
            out.push_str(word);
        }
    }
    if out.is_empty() {
        "Learned task workflow".to_string()
    } else {
        out
    }
}

fn title_case(value: &str) -> String {
    value
        .split(['-', '_'])
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                Some(first) => format!("{}{}", first.to_ascii_uppercase(), chars.as_str()),
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::{
        SkillSelectionQuery, SkillUsageSnapshot, build_harness_skill_index, select_skills,
        skill_autonomous_apply_receipts_file, skill_proposals_file,
    };

    #[test]
    fn skill_synthesis_autonomously_creates_agent_skill_by_default() {
        let root = temp_root("skill_synthesis_autonomously_creates_agent_skill_by_default");
        let home = root.join(".agent-harness");
        let report = synthesize_skill(SkillSynthesisOptions {
            harness_home: home.clone(),
            skill_id: "agent-created:learned-routing".to_string(),
            task_summary: "Apply routing fixes with focused tests".to_string(),
            evidence: "Tests: routing_matrix_green".to_string(),
            propose_only: false,
            now_ms: 1,
        })
        .unwrap();
        assert!(report.autonomous_apply.is_some());
        assert!(report.target_path.is_file());
        assert!(skill_synthesis_receipts_file(home).is_file());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn skill_synthesis_propose_only_leaves_create_proposal_unapplied() {
        let root = temp_root("skill_synthesis_propose_only_leaves_create_proposal_unapplied");
        let home = root.join(".agent-harness");
        let report = synthesize_skill(SkillSynthesisOptions {
            harness_home: home,
            skill_id: "learned-propose-only".to_string(),
            task_summary: "Document a recurring workflow".to_string(),
            evidence: "Evidence: docs updated".to_string(),
            propose_only: true,
            now_ms: 1,
        })
        .unwrap();
        assert!(report.autonomous_apply.is_none());
        assert!(!report.target_path.exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn closed_loop_scenario_selects_synthesizes_applies_and_reselects() {
        let root = temp_root("closed_loop_scenario_selects_synthesizes_applies_and_reselects");
        let home = root.join(".agent-harness");
        let cjk_dir = home
            .join("skills")
            .join("agent-created")
            .join("cjk-weather-check");
        fs::create_dir_all(&cjk_dir).unwrap();
        fs::write(
            cjk_dir.join(crate::SKILL_FILE_NAME),
            "---\nname: cjk-weather-check\ndescription: Handle Chinese weather check requests.\ntriggers: [天氣檢查, 天氣]\ncategory: operations\ntags: [weather, chinese]\n---\n# CJK Weather Check\n",
        )
        .unwrap();

        let index = build_harness_skill_index(&home).unwrap();
        let cjk_selection = select_skills(
            &index,
            &SkillSelectionQuery {
                text: "請幫我做天氣檢查".to_string(),
                include_context_tokens: true,
                agent_id: None,
                channel: None,
                workspace: None,
                agent_mode: None,
                available_tools: Vec::new(),
                available_toolsets: Vec::new(),
                fts_enabled: false,
                vector_tie_break_enabled: false,
                usage_snapshot: Some(SkillUsageSnapshot::default()),
                usage_prior_enabled: false,
                limit: 3,
            },
        );
        assert_eq!(
            cjk_selection.first().map(|item| item.skill_id.as_str()),
            Some("agent-created:cjk-weather-check")
        );

        let synthesis = synthesize_skill(SkillSynthesisOptions {
            harness_home: home.clone(),
            skill_id: "receipt-triage-workflow".to_string(),
            task_summary:
                "Receipt triage workflow for runtime queue replay failures with focused regression tests"
                    .to_string(),
            evidence:
                "Evidence: inspected queue receipts, isolated replay boundary, added regression test"
                    .to_string(),
            propose_only: false,
            now_ms: 10,
        })
        .unwrap();
        assert!(synthesis.target_path.is_file());
        assert!(synthesis.autonomous_apply.is_some());
        assert!(skill_synthesis_receipts_file(&home).is_file());
        assert!(skill_proposals_file(&home).is_file());
        assert!(skill_autonomous_apply_receipts_file(&home).is_file());

        let refreshed = build_harness_skill_index(&home).unwrap();
        let learned_selection = select_skills(
            &refreshed,
            &SkillSelectionQuery {
                text: "Please use the receipt triage workflow for queue replay failures"
                    .to_string(),
                include_context_tokens: true,
                agent_id: None,
                channel: None,
                workspace: None,
                agent_mode: None,
                available_tools: Vec::new(),
                available_toolsets: Vec::new(),
                fts_enabled: false,
                vector_tie_break_enabled: false,
                usage_snapshot: Some(SkillUsageSnapshot::default()),
                usage_prior_enabled: false,
                limit: 3,
            },
        );
        assert_eq!(
            learned_selection
                .first()
                .map(|selection| selection.skill_id.as_str()),
            Some("agent-created:receipt-triage-workflow")
        );

        let _ = fs::remove_dir_all(root);
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-skill-synthesis-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
