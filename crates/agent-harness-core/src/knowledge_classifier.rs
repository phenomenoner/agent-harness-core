use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::{
    CanonicalKnowledgeDispositionV1, KNOWLEDGE_CLASSIFICATION_SCHEMA,
    KnowledgeClassificationReceiptV1, SkillContractError, SkillEcosystemIdentity,
    write_json_atomic,
};

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeClassificationOptions {
    pub identity: SkillEcosystemIdentity,
    pub receipt_id: String,
    pub candidate_id: String,
    pub evidence_refs: Vec<String>,
    pub source_class: String,
    pub candidate_kind: String,
    pub typed_refs: BTreeMap<String, String>,
    pub contradiction_refs: Vec<String>,
    pub confidence: f64,
    pub ambiguous: bool,
    pub dream_run_id: Option<String>,
}

pub fn classify_knowledge_candidate(
    options: KnowledgeClassificationOptions,
) -> Result<KnowledgeClassificationReceiptV1, SkillContractError> {
    let normalized_source = normalize(&options.source_class);
    let normalized_kind = normalize(&options.candidate_kind);
    let (disposition, deterministic_exclusion, ambiguous) = if matches!(
        normalized_source.as_str(),
        "skill-dream" | "reviewer" | "system-notice" | "unjoined-child" | "cron-control"
    ) {
        (
            CanonicalKnowledgeDispositionV1::Discard,
            Some(format!("excluded-source-class:{normalized_source}")),
            false,
        )
    } else if normalized_kind == "raw-assistant-final" {
        (
            CanonicalKnowledgeDispositionV1::Defer,
            Some("raw-assistant-final-is-not-canonical-knowledge".to_string()),
            true,
        )
    } else if options.ambiguous
        || !options.contradiction_refs.is_empty()
        || options.confidence < 0.5
    {
        (CanonicalKnowledgeDispositionV1::Defer, None, true)
    } else if options.typed_refs.contains_key("duplicateOf")
        || options.typed_refs.contains_key("relatedSkill")
    {
        (CanonicalKnowledgeDispositionV1::SkillReference, None, false)
    } else {
        let disposition = match normalized_kind.as_str() {
            "discard" | "ephemeral" => CanonicalKnowledgeDispositionV1::Discard,
            "task-state" | "checkpoint" => CanonicalKnowledgeDispositionV1::VirtualTaskState,
            "episode" | "outcome" | "correction" => {
                CanonicalKnowledgeDispositionV1::EpisodeEvidence
            }
            "user-memory" | "user-preference" => CanonicalKnowledgeDispositionV1::UserProfileMemory,
            "agent-memory" | "environment-memory" => {
                CanonicalKnowledgeDispositionV1::AgentEnvironmentMemory
            }
            "skill-procedure" if !options.evidence_refs.is_empty() => {
                CanonicalKnowledgeDispositionV1::SkillProcedure
            }
            "skill-reference" => CanonicalKnowledgeDispositionV1::SkillReference,
            "replay-fixture" => CanonicalKnowledgeDispositionV1::ReplayFixture,
            "policy" | "contract" => CanonicalKnowledgeDispositionV1::PolicyOrContract,
            _ => CanonicalKnowledgeDispositionV1::Defer,
        };
        let ambiguous = disposition == CanonicalKnowledgeDispositionV1::Defer;
        (disposition, None, ambiguous)
    };
    let receipt = KnowledgeClassificationReceiptV1 {
        schema: KNOWLEDGE_CLASSIFICATION_SCHEMA.to_string(),
        receipt_id: options.receipt_id,
        identity: options.identity,
        candidate_id: options.candidate_id,
        evidence_refs: options.evidence_refs,
        source_class: options.source_class,
        disposition,
        typed_refs: options.typed_refs,
        deterministic_exclusion,
        confidence: options.confidence,
        ambiguous,
        contradiction_refs: options.contradiction_refs,
        dream_run_id: options.dream_run_id,
    };
    receipt.validate()?;
    Ok(receipt)
}

pub fn knowledge_classification_receipt_dir(
    harness_home: impl AsRef<Path>,
    virtual_session_id: &str,
) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("skills")
        .join("knowledge-classifications")
        .join(safe_component(virtual_session_id))
}

pub fn persist_knowledge_classification_once(
    harness_home: impl AsRef<Path>,
    receipt: &KnowledgeClassificationReceiptV1,
) -> io::Result<PathBuf> {
    receipt.validate().map_err(io::Error::other)?;
    let dir =
        knowledge_classification_receipt_dir(harness_home, &receipt.identity.virtual_session_id);
    fs::create_dir_all(&dir)?;
    let path = dir.join(format!("{}.json", safe_component(&receipt.receipt_id)));
    persist_once(&path, receipt)?;
    Ok(path)
}

fn persist_once<T: Serialize>(path: &Path, value: &T) -> io::Result<()> {
    let next = serde_json::to_value(value).map_err(io::Error::other)?;
    if path.is_file() {
        let existing: serde_json::Value =
            serde_json::from_slice(&fs::read(path)?).map_err(io::Error::other)?;
        if existing != next {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "classification receipt id collides with different content",
            ));
        }
        return Ok(());
    }
    write_json_atomic(path, value)
}

fn normalize(value: &str) -> String {
    value.trim().to_ascii_lowercase().replace(['_', ' '], "-")
}

fn safe_component(value: &str) -> String {
    let value = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_') {
                ch
            } else {
                '_'
            }
        })
        .take(160)
        .collect::<String>();
    if value.is_empty() {
        "unknown".to_string()
    } else {
        value
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity() -> SkillEcosystemIdentity {
        SkillEcosystemIdentity {
            virtual_session_id: "vs-1".to_string(),
            root_session_key_hash: "abcdef123456".to_string(),
            concrete_session_hash: "123456abcdef".to_string(),
            exact_lane_digest: "feedface1234".to_string(),
            agent_id: "main".to_string(),
        }
    }

    fn options(kind: &str) -> KnowledgeClassificationOptions {
        KnowledgeClassificationOptions {
            identity: identity(),
            receipt_id: format!("classification-{kind}"),
            candidate_id: format!("candidate-{kind}"),
            evidence_refs: vec!["episode-1".to_string()],
            source_class: "interactive".to_string(),
            candidate_kind: kind.to_string(),
            typed_refs: BTreeMap::new(),
            contradiction_refs: Vec::new(),
            confidence: 0.9,
            ambiguous: false,
            dream_run_id: None,
        }
    }

    #[test]
    fn deterministic_exclusions_never_become_skill_proposals() {
        let mut excluded = options("skill-procedure");
        excluded.source_class = "skill-dream".to_string();
        let receipt = classify_knowledge_candidate(excluded).unwrap();
        assert_eq!(
            receipt.disposition,
            CanonicalKnowledgeDispositionV1::Discard
        );
        assert!(receipt.deterministic_exclusion.is_some());
    }

    #[test]
    fn ambiguity_and_contradiction_defer_to_one_disposition() {
        let mut ambiguous = options("skill-procedure");
        ambiguous
            .contradiction_refs
            .push("contradiction-1".to_string());
        let receipt = classify_knowledge_candidate(ambiguous).unwrap();
        assert_eq!(receipt.disposition, CanonicalKnowledgeDispositionV1::Defer);
        assert!(receipt.ambiguous);
    }

    #[test]
    fn duplicate_procedure_becomes_reference_not_second_mutable_copy() {
        let mut duplicate = options("skill-procedure");
        duplicate
            .typed_refs
            .insert("duplicateOf".to_string(), "workspace:existing".to_string());
        let receipt = classify_knowledge_candidate(duplicate).unwrap();
        assert_eq!(
            receipt.disposition,
            CanonicalKnowledgeDispositionV1::SkillReference
        );
    }
}
