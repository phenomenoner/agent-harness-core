use serde::Serialize;

use crate::skills::{SKILL_MATCHER_NAME, SKILL_MATCHER_TOKENIZER, SKILL_MATCHER_VERSION};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillMatcherInfo {
    pub name: &'static str,
    pub version: &'static str,
    pub tokenizer: &'static str,
    pub vector_enabled_by_default: bool,
    pub stages: [&'static str; 4],
}

pub fn skill_matcher_info() -> SkillMatcherInfo {
    SkillMatcherInfo {
        name: SKILL_MATCHER_NAME,
        version: SKILL_MATCHER_VERSION,
        tokenizer: SKILL_MATCHER_TOKENIZER,
        vector_enabled_by_default: false,
        stages: [
            "explicit-invocation-or-skill-id",
            "agent-lifecycle-and-invocation-policy",
            "frontmatter-triggers-and-gates",
            "weighted-lexical-score",
        ],
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::{
        AgentSource, SKILL_FILE_NAME, SkillDeliveryMode, SkillSelectionQuery,
        build_source_skill_index, select_skills,
    };

    use super::*;

    #[test]
    fn skill_matcher_info_exposes_v4_mixed_tokenizer_defaults() {
        let info = skill_matcher_info();
        assert_eq!(info.version, "v4");
        assert_eq!(info.tokenizer, "mixed-v1");
        assert!(!info.vector_enabled_by_default);
        assert_eq!(info.stages[0], "explicit-invocation-or-skill-id");
        assert_eq!(info.stages[1], "agent-lifecycle-and-invocation-policy");
    }

    #[test]
    fn skill_matcher_explicit_invocation_selects_envelope_mode() {
        let root = temp_root("skill_matcher_explicit_invocation_selects_envelope_mode");
        let home = root.join(".openclaw");
        let workspace = home.join("workspace");
        let skill = workspace.join("skills").join("memory-cron");
        fs::create_dir_all(&skill).unwrap();
        fs::write(
            skill.join(SKILL_FILE_NAME),
            "# Memory Cron\n\nRepair memory cron jobs.",
        )
        .unwrap();
        let index =
            build_source_skill_index(&AgentSource::with_workspace(&home, &workspace)).unwrap();
        let selections = select_skills(
            &index,
            &SkillSelectionQuery {
                text: "/skill memory-cron rerun stale embedding jobs".to_string(),
                agent_id: None,
                channel: Some("telegram".to_string()),
                workspace: None,
                agent_mode: None,
                available_tools: Vec::new(),
                available_toolsets: Vec::new(),
                fts_enabled: false,
                vector_tie_break_enabled: false,
                usage_snapshot: None,
                usage_prior_enabled: false,
                limit: 5,
            },
        );

        assert_eq!(selections.len(), 1);
        assert_eq!(selections[0].skill_id, "workspace:memory-cron");
        assert_eq!(
            selections[0].delivery_mode,
            SkillDeliveryMode::InvocationEnvelope
        );
        assert_eq!(
            selections[0].user_instruction.as_deref(),
            Some("rerun stale embedding jobs")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn skill_matcher_optional_sqlite_fts5_adds_bm25_component() {
        let root = temp_root("skill_matcher_optional_sqlite_fts5_adds_bm25_component");
        let home = root.join(".openclaw");
        let workspace = home.join("workspace");
        let skill = workspace.join("skills").join("rare-workflow");
        fs::create_dir_all(&skill).unwrap();
        fs::write(
            skill.join(SKILL_FILE_NAME),
            "# Rare Workflow\n\nHandle zetaomega build recovery.",
        )
        .unwrap();
        let index =
            build_source_skill_index(&AgentSource::with_workspace(&home, &workspace)).unwrap();
        let selections = select_skills(
            &index,
            &SkillSelectionQuery {
                text: "zetaomega recovery".to_string(),
                agent_id: None,
                channel: None,
                workspace: None,
                agent_mode: None,
                available_tools: Vec::new(),
                available_toolsets: Vec::new(),
                fts_enabled: true,
                vector_tie_break_enabled: false,
                usage_snapshot: None,
                usage_prior_enabled: false,
                limit: 5,
            },
        );

        assert_eq!(selections[0].skill_id, "workspace:rare-workflow");
        assert!(
            selections[0]
                .score_components
                .iter()
                .any(|component| component.name == "sqlite-fts5-bm25")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn skill_matcher_mixed_tokenizer_selects_cjk_trigger_skill() {
        let root = temp_root("skill_matcher_mixed_tokenizer_selects_cjk_trigger_skill");
        let home = root.join(".openclaw");
        let workspace = home.join("workspace");
        let skill = workspace.join("skills").join("neoapi-orders");
        fs::create_dir_all(&skill).unwrap();
        fs::write(
            skill.join(SKILL_FILE_NAME),
            "---\ntriggers: [下單, 富邦, neoapi]\ncategory: trading\ntags: [neoapi, orders]\n---\n# NeoAPI Orders\n\nUse when placing Fubon Neo orders.\n",
        )
        .unwrap();
        let index =
            build_source_skill_index(&AgentSource::with_workspace(&home, &workspace)).unwrap();

        let selections = select_skills(
            &index,
            &SkillSelectionQuery {
                text: "幫我用富邦下單".to_string(),
                agent_id: None,
                channel: None,
                workspace: None,
                agent_mode: None,
                available_tools: Vec::new(),
                available_toolsets: Vec::new(),
                fts_enabled: false,
                vector_tie_break_enabled: false,
                usage_snapshot: None,
                usage_prior_enabled: false,
                limit: 5,
            },
        );

        assert_eq!(selections[0].skill_id, "workspace:neoapi-orders");
        assert!(
            selections[0]
                .score_components
                .iter()
                .any(|component| component.name == "declared-triggers")
        );

        let _ = fs::remove_dir_all(root);
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-skill-matcher-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
