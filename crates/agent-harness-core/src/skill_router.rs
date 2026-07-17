use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;

use crate::{
    SkillDeliveryMode, SkillIndex, SkillSelection, SkillSelectionQuery, SkillUsageSnapshot,
    select_skills,
};

pub const SKILL_ROUTER_V2_METHOD: &str = "bounded-lexical-variable-k";
pub const SKILL_ROUTER_V2_VERSION: &str = "shadow-v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillRoutingQueryV2 {
    pub task_text: String,
    pub explicit_invocations: Vec<String>,
    pub agent_id: String,
    pub channel: String,
    pub available_tools: Vec<String>,
    pub available_toolsets: Vec<String>,
    pub risk_context: Vec<String>,
    pub virtual_task_intent: Option<String>,
    pub ambient_notes_excluded_bytes: usize,
    pub usage_snapshot: Option<SkillUsageSnapshot>,
}

impl SkillRoutingQueryV2 {
    pub fn lexical_text(&self) -> String {
        let mut text = self.task_text.trim().to_string();
        if let Some(intent) = self
            .virtual_task_intent
            .as_deref()
            .map(str::trim)
            .filter(|intent| !intent.is_empty())
        {
            if !text.is_empty() {
                text.push('\n');
            }
            text.push_str(intent);
        }
        text
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SkillRouterV2Policy {
    pub candidate_limit: usize,
    pub automatic_limit: usize,
    pub explicit_limit: usize,
    pub metadata_threshold: f64,
    pub full_body_threshold: f64,
    pub minimum_margin: f64,
    pub automatic_full_body_enabled: bool,
}

impl Default for SkillRouterV2Policy {
    fn default() -> Self {
        Self {
            candidate_limit: 12,
            automatic_limit: 2,
            explicit_limit: 5,
            metadata_threshold: 0.62,
            full_body_threshold: 0.82,
            minimum_margin: 0.12,
            automatic_full_body_enabled: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillRouterV2Candidate {
    pub selection: SkillSelection,
    pub confidence: f64,
    pub rank: usize,
    pub selected: bool,
    pub delivery_mode: SkillDeliveryMode,
    pub reason_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillRouterV2Rejection {
    pub skill_id: String,
    pub reason_code: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillRouterV2Result {
    pub method: String,
    pub version: String,
    pub candidates: Vec<SkillRouterV2Candidate>,
    pub rejected: Vec<SkillRouterV2Rejection>,
    pub selected_skill_ids: Vec<String>,
    pub abstention_reason: Option<String>,
    pub query_text_bytes: usize,
    pub ambient_notes_excluded_bytes: usize,
}

pub fn route_skills_v2(
    index: &SkillIndex,
    query: &SkillRoutingQueryV2,
    policy: SkillRouterV2Policy,
) -> SkillRouterV2Result {
    let lexical_text = query.lexical_text();
    let explicit = !query.explicit_invocations.is_empty() || has_explicit_invocation(&lexical_text);
    let (eligible_index, rejected) = eligible_index(index, query, explicit);
    let legacy_query = SkillSelectionQuery {
        text: lexical_text.clone(),
        include_context_tokens: false,
        agent_id: Some(query.agent_id.clone()),
        channel: Some(query.channel.clone()),
        workspace: None,
        agent_mode: None,
        available_tools: query.available_tools.clone(),
        available_toolsets: query.available_toolsets.clone(),
        fts_enabled: true,
        vector_tie_break_enabled: false,
        usage_snapshot: query.usage_snapshot.clone(),
        usage_prior_enabled: false,
        limit: policy.candidate_limit.max(1),
    };
    let raw = select_skills(&eligible_index, &legacy_query);
    let maximum_score = raw.first().map(|item| item.score).unwrap_or(0).max(1) as f64;
    let selected_limit = if explicit {
        policy.explicit_limit.min(5)
    } else {
        policy.automatic_limit.min(2)
    };
    let mut candidates = Vec::new();
    let mut selected_skill_ids = Vec::new();
    let mut prior_confidence = None;
    for (index, mut selection) in raw.into_iter().enumerate() {
        let structured_score: usize = selection
            .score_components
            .iter()
            .filter(|component| component.name != "fts" && component.name != "usage-prior")
            .map(|component| component.score)
            .sum();
        let relative = selection.score as f64 / maximum_score;
        let structured = (structured_score as f64 / 40.0).clamp(0.0, 1.0);
        let confidence = if explicit {
            1.0
        } else {
            (relative * 0.55 + structured * 0.45).clamp(0.0, 1.0)
        };
        let margin_ok = prior_confidence
            .map(|prior: f64| prior - confidence >= policy.minimum_margin)
            .unwrap_or(true);
        let selected = selected_skill_ids.len() < selected_limit
            && (explicit || (confidence >= policy.metadata_threshold && margin_ok));
        let delivery_mode = if explicit {
            SkillDeliveryMode::InvocationEnvelope
        } else if policy.automatic_full_body_enabled
            && confidence >= policy.full_body_threshold
            && margin_ok
        {
            SkillDeliveryMode::InjectedBody
        } else {
            SkillDeliveryMode::ToolView
        };
        selection.delivery_mode = delivery_mode;
        if selected {
            selected_skill_ids.push(selection.skill_id.clone());
        }
        let mut reason_codes = vec![if explicit {
            "explicit-invocation".to_string()
        } else {
            "bounded-lexical-candidate".to_string()
        }];
        if !selected {
            reason_codes.push(if confidence < policy.metadata_threshold {
                "below-threshold".to_string()
            } else {
                "insufficient-margin-or-k".to_string()
            });
        }
        candidates.push(SkillRouterV2Candidate {
            selection,
            confidence,
            rank: index + 1,
            selected,
            delivery_mode,
            reason_codes,
        });
        prior_confidence = Some(confidence);
    }
    let abstention_reason = selected_skill_ids.is_empty().then(|| {
        if candidates.is_empty() {
            "no-eligible-candidate".to_string()
        } else {
            "below-threshold-or-margin".to_string()
        }
    });
    SkillRouterV2Result {
        method: SKILL_ROUTER_V2_METHOD.to_string(),
        version: SKILL_ROUTER_V2_VERSION.to_string(),
        candidates,
        rejected,
        selected_skill_ids,
        abstention_reason,
        query_text_bytes: lexical_text.len(),
        ambient_notes_excluded_bytes: query.ambient_notes_excluded_bytes,
    }
}

fn eligible_index(
    index: &SkillIndex,
    query: &SkillRoutingQueryV2,
    explicit: bool,
) -> (SkillIndex, Vec<SkillRouterV2Rejection>) {
    let mut eligible = index.clone();
    let mut rejected = Vec::new();
    eligible.skills.retain(|skill| {
        let reason = eligibility_rejection(skill, query, explicit);
        if let Some(reason_code) = reason {
            rejected.push(SkillRouterV2Rejection {
                skill_id: skill.id.clone(),
                reason_code,
            });
            false
        } else {
            true
        }
    });
    eligible.summary.total_skills = eligible.skills.len();
    (eligible, rejected)
}

fn eligibility_rejection(
    skill: &crate::SkillRecord,
    query: &SkillRoutingQueryV2,
    explicit: bool,
) -> Option<String> {
    let frontmatter = &skill.frontmatter;
    let lifecycle = frontmatter.lifecycle.as_deref().unwrap_or("active");
    if matches!(
        normalize(lifecycle).as_str(),
        "retired" | "retired-historical" | "disabled" | "archived"
    ) {
        return Some("lifecycle".to_string());
    }
    if !frontmatter.agents.is_empty() && !matches_value(&frontmatter.agents, &query.agent_id) {
        return Some("wrong-agent".to_string());
    }
    if (!frontmatter.channels.is_empty() && !matches_value(&frontmatter.channels, &query.channel))
        || (!frontmatter.platforms.is_empty()
            && !matches_value(&frontmatter.platforms, &query.channel))
    {
        return Some("wrong-channel".to_string());
    }
    if explicit && frontmatter.user_invocable == Some(false) {
        return Some("user-invocation-disabled".to_string());
    }
    if !explicit && frontmatter.disable_model_invocation == Some(true) {
        return Some("model-invocation-disabled".to_string());
    }
    if !required_available(&frontmatter.requires_tools, &query.available_tools) {
        return Some("missing-tool".to_string());
    }
    if !required_available(&frontmatter.requires_toolsets, &query.available_toolsets) {
        return Some("missing-toolset".to_string());
    }
    if !frontmatter.risks.is_empty()
        && !query.risk_context.is_empty()
        && !frontmatter
            .risks
            .iter()
            .all(|risk| matches_value(&query.risk_context, risk))
    {
        return Some("risk-policy".to_string());
    }
    let text = normalize(&query.lexical_text());
    if frontmatter
        .negative_triggers
        .iter()
        .any(|trigger| text.contains(&normalize(trigger)))
    {
        return Some("negative-trigger".to_string());
    }
    None
}

fn has_explicit_invocation(text: &str) -> bool {
    let trimmed = text.trim_start();
    trimmed.starts_with("$skill")
        || trimmed.starts_with("/skill ")
        || (trimmed.starts_with('$') && trimmed.split_whitespace().next().is_some())
}

fn required_available(required: &[String], available: &[String]) -> bool {
    required.iter().all(|item| matches_value(available, item))
}

fn matches_value(values: &[String], expected: &str) -> bool {
    let expected = normalize(expected);
    values.iter().any(|value| normalize(value) == expected)
}

fn normalize(value: &str) -> String {
    value
        .trim()
        .to_ascii_lowercase()
        .replace('_', "-")
        .replace(' ', "-")
}

pub fn routing_feature_map(candidate: &SkillRouterV2Candidate) -> BTreeMap<String, f64> {
    let mut features = BTreeMap::new();
    features.insert("confidence".to_string(), candidate.confidence);
    for component in &candidate.selection.score_components {
        features.insert(component.name.clone(), component.score as f64);
    }
    features
}

pub fn selected_original_ids(result: &SkillRouterV2Result) -> BTreeSet<String> {
    result
        .candidates
        .iter()
        .filter(|candidate| candidate.selected)
        .map(|candidate| candidate.selection.original_id.to_ascii_lowercase())
        .collect()
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::{AgentSource, build_runtime_skill_index};

    use super::*;

    fn temp_root(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("skill-router-v2-{name}-{nanos}"))
    }

    fn index_with_skills() -> (PathBuf, SkillIndex) {
        let root = temp_root("index");
        let home = root.join("home");
        let workspace = root.join("workspace");
        let harness = root.join("harness");
        let write = |id: &str, frontmatter: &str, description: &str| {
            let dir = workspace.join("skills").join(id);
            fs::create_dir_all(&dir).expect("create skill");
            fs::write(
                dir.join("SKILL.md"),
                format!("---\nname: {id}\ndescription: {description}\n{frontmatter}---\n# {id}\n"),
            )
            .expect("write skill");
        };
        write(
            "discord-review",
            "channels: [discord]\nnegative_triggers: [generate image]\ndelivery_mode: tool-view\n",
            "Review a Discord screenshot",
        );
        write(
            "telegram-review",
            "channels: [telegram]\ndelivery_mode: tool-view\n",
            "Review a Telegram screenshot",
        );
        write(
            "retired-review",
            "lifecycle: retired\n",
            "Review any screenshot",
        );
        let source = AgentSource::with_workspace(home, workspace);
        let index = build_runtime_skill_index(&source, &harness).expect("index");
        (root, index)
    }

    fn query(text: &str) -> SkillRoutingQueryV2 {
        SkillRoutingQueryV2 {
            task_text: text.to_string(),
            explicit_invocations: Vec::new(),
            agent_id: "main".to_string(),
            channel: "discord".to_string(),
            available_tools: Vec::new(),
            available_toolsets: Vec::new(),
            risk_context: Vec::new(),
            virtual_task_intent: None,
            ambient_notes_excluded_bytes: 0,
            usage_snapshot: None,
        }
    }

    #[test]
    fn current_task_query_excludes_ambient_context_and_identity_tokens() {
        let (root, index) = index_with_skills();
        let mut query = query("hello");
        query.ambient_notes_excluded_bytes = 900;
        query.virtual_task_intent = None;
        let result = route_skills_v2(&index, &query, SkillRouterV2Policy::default());
        assert!(result.selected_skill_ids.is_empty());
        assert_eq!(result.query_text_bytes, "hello".len());
        assert_eq!(result.ambient_notes_excluded_bytes, 900);
        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn hard_gates_reject_wrong_channel_retired_and_negative_trigger() {
        let (root, index) = index_with_skills();
        let result = route_skills_v2(
            &index,
            &query("generate image from Discord screenshot"),
            SkillRouterV2Policy::default(),
        );
        let reasons = result
            .rejected
            .iter()
            .map(|item| item.reason_code.as_str())
            .collect::<BTreeSet<_>>();
        assert!(reasons.contains("wrong-channel"));
        assert!(reasons.contains("lifecycle"));
        assert!(reasons.contains("negative-trigger"));
        assert!(result.selected_skill_ids.is_empty());
        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn automatic_router_never_pads_and_uses_tool_view() {
        let (root, index) = index_with_skills();
        let result = route_skills_v2(
            &index,
            &query("Review this Discord screenshot"),
            SkillRouterV2Policy::default(),
        );
        assert!(result.selected_skill_ids.len() <= 2);
        assert!(
            result
                .candidates
                .iter()
                .filter(|candidate| candidate.selected)
                .all(|candidate| candidate.delivery_mode == SkillDeliveryMode::ToolView)
        );
        fs::remove_dir_all(root).ok();
    }
}
