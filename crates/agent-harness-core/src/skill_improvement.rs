use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use ring::digest;
use serde::{Deserialize, Serialize};

use crate::{
    CanonicalKnowledgeDispositionV1, KnowledgeClassificationReceiptV1, SkillContractError,
    SkillDeliveryReasonV2, SkillEcosystemIdentity, SkillEpisodeUseEvidenceV2, SkillEpisodeV2,
    SkillOutcomeStatusV1, validate_joined_identity, write_json_atomic,
};

pub const SKILL_IMPROVEMENT_TARGET_SCHEMA: &str = "agent-harness.skill-improvement-target.v1";
pub const SKILL_IMPROVEMENT_PROPOSAL_SCHEMA: &str = "agent-harness.skill-improvement-proposal.v1";
const SKILL_IMPROVEMENT_HASH_DOMAIN: &[u8] = b"agent-harness/skill-improvement/v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SkillImprovementTargetReasonV1 {
    ExplicitInvocation,
    ObservedProcedure,
    VerifierOrCorrectionLink,
    UmbrellaFit,
    NoTarget,
    Ambiguous,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillImprovementTargetV1 {
    pub schema: String,
    pub target_id: String,
    pub identity: SkillEcosystemIdentity,
    pub episode_ids: Vec<String>,
    pub target_skill_id: Option<String>,
    pub target_revision: Option<String>,
    pub reason: SkillImprovementTargetReasonV1,
    pub confidence: f64,
    pub abstention_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillImprovementTargetOptions<'a> {
    pub target_id: String,
    pub episodes: &'a [SkillEpisodeV2],
    pub umbrella_skill_id: Option<String>,
    pub umbrella_revision: Option<String>,
}

pub fn attribute_skill_improvement_target(
    options: SkillImprovementTargetOptions<'_>,
) -> Result<SkillImprovementTargetV1, SkillContractError> {
    let Some(first) = options.episodes.first() else {
        return Err(SkillContractError::MissingField("episodes"));
    };
    validate_joined_identity(options.episodes.iter().map(|episode| &episode.identity))?;
    let episode_ids = options
        .episodes
        .iter()
        .map(|episode| episode.episode_id.clone())
        .collect::<Vec<_>>();
    let choose = |candidates: Vec<&SkillEpisodeV2>,
                  reason: SkillImprovementTargetReasonV1,
                  confidence: f64|
     -> Option<SkillImprovementTargetV1> {
        let distinct = candidates
            .iter()
            .map(|episode| episode.skill_id.to_ascii_lowercase())
            .collect::<BTreeSet<_>>();
        if distinct.len() == 1 {
            let episode = candidates[0];
            Some(SkillImprovementTargetV1 {
                schema: SKILL_IMPROVEMENT_TARGET_SCHEMA.to_string(),
                target_id: options.target_id.clone(),
                identity: first.identity.clone(),
                episode_ids: episode_ids.clone(),
                target_skill_id: Some(episode.skill_id.clone()),
                target_revision: Some(episode.skill_revision.clone()),
                reason,
                confidence,
                abstention_reason: None,
            })
        } else if distinct.len() > 1 {
            Some(SkillImprovementTargetV1 {
                schema: SKILL_IMPROVEMENT_TARGET_SCHEMA.to_string(),
                target_id: options.target_id.clone(),
                identity: first.identity.clone(),
                episode_ids: episode_ids.clone(),
                target_skill_id: None,
                target_revision: None,
                reason: SkillImprovementTargetReasonV1::Ambiguous,
                confidence: 0.0,
                abstention_reason: Some(
                    "multiple-skills-share-the-highest-priority-evidence".to_string(),
                ),
            })
        } else {
            None
        }
    };

    let explicit = options
        .episodes
        .iter()
        .filter(|episode| {
            episode.delivery_reason == SkillDeliveryReasonV2::Explicit
                && episode.has_attributable_use()
        })
        .collect::<Vec<_>>();
    if let Some(target) = choose(
        explicit,
        SkillImprovementTargetReasonV1::ExplicitInvocation,
        1.0,
    ) {
        return Ok(target);
    }
    let observed = options
        .episodes
        .iter()
        .filter(|episode| {
            episode.use_evidence.iter().any(|evidence| {
                matches!(
                    evidence,
                    SkillEpisodeUseEvidenceV2::ProcedureAcknowledged
                        | SkillEpisodeUseEvidenceV2::ProcedureStepObserved
                )
            }) && episode.delivery_reason != SkillDeliveryReasonV2::Rehydration
        })
        .collect::<Vec<_>>();
    if let Some(target) = choose(
        observed,
        SkillImprovementTargetReasonV1::ObservedProcedure,
        0.95,
    ) {
        return Ok(target);
    }
    let linked = options
        .episodes
        .iter()
        .filter(|episode| {
            episode.verifier_ref.is_some()
                || episode.correction_ref.is_some()
                || matches!(
                    episode.outcome_status,
                    Some(SkillOutcomeStatusV1::VerifiedFailure | SkillOutcomeStatusV1::Corrected)
                )
        })
        .filter(|episode| episode.has_attributable_use())
        .collect::<Vec<_>>();
    if let Some(target) = choose(
        linked,
        SkillImprovementTargetReasonV1::VerifierOrCorrectionLink,
        0.9,
    ) {
        return Ok(target);
    }
    if let (Some(skill_id), Some(revision)) = (options.umbrella_skill_id, options.umbrella_revision)
    {
        return Ok(SkillImprovementTargetV1 {
            schema: SKILL_IMPROVEMENT_TARGET_SCHEMA.to_string(),
            target_id: options.target_id,
            identity: first.identity.clone(),
            episode_ids,
            target_skill_id: Some(skill_id),
            target_revision: Some(revision),
            reason: SkillImprovementTargetReasonV1::UmbrellaFit,
            confidence: 0.75,
            abstention_reason: None,
        });
    }
    Ok(SkillImprovementTargetV1 {
        schema: SKILL_IMPROVEMENT_TARGET_SCHEMA.to_string(),
        target_id: options.target_id,
        identity: first.identity.clone(),
        episode_ids,
        target_skill_id: None,
        target_revision: None,
        reason: SkillImprovementTargetReasonV1::NoTarget,
        confidence: 0.0,
        abstention_reason: Some("no-attributable-skill-evidence".to_string()),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SkillImprovementProposalKindV1 {
    SemanticPatch,
    Synthesis,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillImprovementProposalV1 {
    pub schema: String,
    pub proposal_id: String,
    pub identity: SkillEcosystemIdentity,
    pub kind: SkillImprovementProposalKindV1,
    pub classification_receipt_id: String,
    pub target_skill_id: String,
    pub target_revision: String,
    pub episode_ids: Vec<String>,
    pub outcome_ids: Vec<String>,
    pub semantic_sections: BTreeMap<String, String>,
    pub before_checksum: String,
    pub proposed_after_checksum: String,
    pub rollback_id: String,
    pub evidence_refs: Vec<String>,
    pub affected_fixture_refs: Vec<String>,
    pub novelty_refs: Vec<String>,
    pub state: String,
    pub report_indexable: bool,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SemanticPatchProposalOptions<'a> {
    pub target: &'a SkillImprovementTargetV1,
    pub classification: &'a KnowledgeClassificationReceiptV1,
    pub episodes: &'a [SkillEpisodeV2],
    pub semantic_sections: BTreeMap<String, String>,
    pub affected_fixture_refs: Vec<String>,
    pub now_ms: i64,
}

pub fn propose_semantic_skill_patch(
    options: SemanticPatchProposalOptions<'_>,
) -> Result<SkillImprovementProposalV1, SkillContractError> {
    validate_proposal_inputs(options.target, options.classification, options.episodes)?;
    let target_skill_id = options.target.target_skill_id.clone().ok_or_else(|| {
        SkillContractError::InvalidField("semantic patch requires an attributed target".to_string())
    })?;
    let target_revision = options.target.target_revision.clone().ok_or_else(|| {
        SkillContractError::InvalidField("semantic patch requires a frozen revision".to_string())
    })?;
    validate_semantic_sections(&options.semantic_sections)?;
    build_proposal(
        SkillImprovementProposalKindV1::SemanticPatch,
        options.target.identity.clone(),
        options.classification.receipt_id.clone(),
        target_skill_id,
        target_revision.clone(),
        options.episodes,
        options.semantic_sections,
        target_revision,
        options.affected_fixture_refs,
        Vec::new(),
        options.now_ms,
    )
}

#[derive(Debug, Clone, PartialEq)]
pub struct SkillSynthesisProposalOptions<'a> {
    pub identity: SkillEcosystemIdentity,
    pub classification: &'a KnowledgeClassificationReceiptV1,
    pub episodes: &'a [SkillEpisodeV2],
    pub proposed_name: String,
    pub semantic_sections: BTreeMap<String, String>,
    pub nearest_skill_refs: Vec<String>,
    pub explicit_learn: bool,
    pub now_ms: i64,
}

pub fn propose_novel_skill_synthesis(
    options: SkillSynthesisProposalOptions<'_>,
) -> Result<SkillImprovementProposalV1, SkillContractError> {
    validate_joined_identity(
        std::iter::once(&options.identity)
            .chain(std::iter::once(&options.classification.identity))
            .chain(options.episodes.iter().map(|episode| &episode.identity)),
    )?;
    if options.classification.disposition != CanonicalKnowledgeDispositionV1::SkillProcedure {
        return Err(SkillContractError::InvalidField(
            "synthesis proposal requires skill-procedure classification".to_string(),
        ));
    }
    let verified = options
        .episodes
        .iter()
        .filter(|episode| episode.positive_learning_eligible())
        .count();
    let required = if options.explicit_learn { 1 } else { 2 };
    if verified < required {
        return Err(SkillContractError::InvalidField(format!(
            "synthesis requires {required} verified attributable episode(s), got {verified}"
        )));
    }
    validate_semantic_sections(&options.semantic_sections)?;
    let skill_name = safe_skill_name(&options.proposed_name);
    let target_skill_id = format!("agent-created:{skill_name}");
    build_proposal(
        SkillImprovementProposalKindV1::Synthesis,
        options.identity,
        options.classification.receipt_id.clone(),
        target_skill_id,
        "missing".to_string(),
        options.episodes,
        options.semantic_sections,
        "missing".to_string(),
        Vec::new(),
        options.nearest_skill_refs,
        options.now_ms,
    )
}

pub fn skill_improvement_proposal_dir(
    harness_home: impl AsRef<Path>,
    virtual_session_id: &str,
) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("skills")
        .join("improvement-proposals")
        .join(safe_component(virtual_session_id))
}

pub fn persist_skill_improvement_proposal_once(
    harness_home: impl AsRef<Path>,
    proposal: &SkillImprovementProposalV1,
) -> io::Result<PathBuf> {
    if proposal.state != "proposal-only" || proposal.report_indexable {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "skill improvement proposal must remain proposal-only and non-indexable",
        ));
    }
    let dir = skill_improvement_proposal_dir(harness_home, &proposal.identity.virtual_session_id);
    fs::create_dir_all(&dir)?;
    let path = dir.join(format!("{}.json", safe_component(&proposal.proposal_id)));
    let next = serde_json::to_value(proposal).map_err(io::Error::other)?;
    if path.is_file() {
        let existing: serde_json::Value =
            serde_json::from_slice(&fs::read(&path)?).map_err(io::Error::other)?;
        if existing != next {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "skill improvement proposal id collision",
            ));
        }
        return Ok(path);
    }
    write_json_atomic(&path, proposal)?;
    Ok(path)
}

fn validate_proposal_inputs(
    target: &SkillImprovementTargetV1,
    classification: &KnowledgeClassificationReceiptV1,
    episodes: &[SkillEpisodeV2],
) -> Result<(), SkillContractError> {
    validate_joined_identity(
        std::iter::once(&target.identity)
            .chain(std::iter::once(&classification.identity))
            .chain(episodes.iter().map(|episode| &episode.identity)),
    )?;
    if classification.disposition != CanonicalKnowledgeDispositionV1::SkillProcedure {
        return Err(SkillContractError::InvalidField(
            "proposal requires skill-procedure classification".to_string(),
        ));
    }
    let target_episode_ids = target.episode_ids.iter().collect::<BTreeSet<_>>();
    if !episodes
        .iter()
        .all(|episode| target_episode_ids.contains(&episode.episode_id))
    {
        return Err(SkillContractError::InvalidField(
            "proposal episode is outside target attribution".to_string(),
        ));
    }
    Ok(())
}

fn validate_semantic_sections(
    sections: &BTreeMap<String, String>,
) -> Result<(), SkillContractError> {
    if sections.is_empty() {
        return Err(SkillContractError::MissingField("semanticSections"));
    }
    let allowed = [
        "triggers",
        "procedure",
        "pitfalls",
        "verification",
        "non-goals",
    ];
    for (key, value) in sections {
        if !allowed.contains(&key.as_str()) {
            return Err(SkillContractError::InvalidField(format!(
                "unsupported semantic patch section {key}"
            )));
        }
        if value.trim().is_empty() || value.len() > 16_384 {
            return Err(SkillContractError::InvalidField(format!(
                "semantic patch section {key} is empty or over budget"
            )));
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn build_proposal(
    kind: SkillImprovementProposalKindV1,
    identity: SkillEcosystemIdentity,
    classification_receipt_id: String,
    target_skill_id: String,
    target_revision: String,
    episodes: &[SkillEpisodeV2],
    semantic_sections: BTreeMap<String, String>,
    before_checksum: String,
    affected_fixture_refs: Vec<String>,
    novelty_refs: Vec<String>,
    now_ms: i64,
) -> Result<SkillImprovementProposalV1, SkillContractError> {
    validate_joined_identity(episodes.iter().map(|episode| &episode.identity))?;
    let canonical_patch = serde_json::to_vec(&semantic_sections).map_err(|error| {
        SkillContractError::InvalidField(format!("semantic patch encoding failed: {error}"))
    })?;
    let proposed_after_checksum = format!("sha256:{}", sha256_hex(&canonical_patch));
    let episode_ids = episodes
        .iter()
        .map(|episode| episode.episode_id.clone())
        .collect::<Vec<_>>();
    let outcome_ids = episodes
        .iter()
        .filter_map(|episode| episode.outcome_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let evidence_refs = episode_ids
        .iter()
        .cloned()
        .chain(outcome_ids.iter().cloned())
        .collect::<Vec<_>>();
    let kind_label = match kind {
        SkillImprovementProposalKindV1::SemanticPatch => "semantic-patch",
        SkillImprovementProposalKindV1::Synthesis => "synthesis",
    };
    let proposal_id = format!(
        "skill-improvement-proposal-{}",
        hash_components(&[
            identity.virtual_session_id.as_bytes(),
            target_skill_id.as_bytes(),
            before_checksum.as_bytes(),
            proposed_after_checksum.as_bytes(),
            kind_label.as_bytes(),
        ])
    );
    let rollback_id = format!(
        "skill-improvement-rollback-{}",
        hash_components(&[
            proposal_id.as_bytes(),
            target_skill_id.as_bytes(),
            before_checksum.as_bytes(),
        ])
    );
    Ok(SkillImprovementProposalV1 {
        schema: SKILL_IMPROVEMENT_PROPOSAL_SCHEMA.to_string(),
        proposal_id,
        identity,
        kind,
        classification_receipt_id,
        target_skill_id,
        target_revision,
        episode_ids,
        outcome_ids,
        semantic_sections,
        before_checksum,
        proposed_after_checksum,
        rollback_id,
        evidence_refs,
        affected_fixture_refs,
        novelty_refs,
        state: "proposal-only".to_string(),
        report_indexable: false,
        created_at_ms: now_ms,
    })
}

fn safe_skill_name(value: &str) -> String {
    let normalized = value
        .trim()
        .to_ascii_lowercase()
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '-' })
        .collect::<String>()
        .split('-')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("-");
    if normalized.is_empty() {
        format!("learned-{}", &sha256_hex(value.as_bytes())[..12])
    } else {
        normalized.chars().take(80).collect()
    }
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

fn hash_components(components: &[&[u8]]) -> String {
    let mut context = digest::Context::new(&digest::SHA256);
    context.update(SKILL_IMPROVEMENT_HASH_DOMAIN);
    for component in components {
        context.update(&(component.len() as u64).to_be_bytes());
        context.update(component);
    }
    lower_hex(context.finish().as_ref())
}

fn sha256_hex(value: &[u8]) -> String {
    lower_hex(digest::digest(&digest::SHA256, value).as_ref())
}

fn lower_hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
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

    fn episode(
        id: &str,
        skill: &str,
        reason: SkillDeliveryReasonV2,
        evidence: Vec<SkillEpisodeUseEvidenceV2>,
    ) -> SkillEpisodeV2 {
        SkillEpisodeV2 {
            schema: crate::SKILL_EPISODE_SCHEMA.to_string(),
            episode_id: id.to_string(),
            source_turn_hash: "abcdef123456".to_string(),
            identity: identity(),
            rollover_index: 0,
            execution_class: "interactive".to_string(),
            catalog_revision: "catalog-1".to_string(),
            skill_id: skill.to_string(),
            skill_revision: "sha256:abcdef123456".to_string(),
            delivery_id: format!("delivery-{id}"),
            delivery_reason: reason,
            use_evidence: evidence,
            outcome_id: Some(format!("outcome-{id}")),
            outcome_status: Some(SkillOutcomeStatusV1::VerifiedSuccess),
            verifier_ref: Some(format!("verifier-{id}")),
            correction_ref: None,
            source_origin: "user".to_string(),
            admission_eligible: true,
        }
    }

    fn classification() -> KnowledgeClassificationReceiptV1 {
        KnowledgeClassificationReceiptV1 {
            schema: crate::KNOWLEDGE_CLASSIFICATION_SCHEMA.to_string(),
            receipt_id: "classification-1".to_string(),
            identity: identity(),
            candidate_id: "candidate-1".to_string(),
            evidence_refs: vec!["episode-1".to_string()],
            source_class: "interactive".to_string(),
            disposition: CanonicalKnowledgeDispositionV1::SkillProcedure,
            typed_refs: BTreeMap::new(),
            deterministic_exclusion: None,
            confidence: 0.9,
            ambiguous: false,
            contradiction_refs: Vec::new(),
            dream_run_id: None,
        }
    }

    #[test]
    fn target_attribution_never_blames_first_selected_only_skill() {
        let episodes = vec![
            episode(
                "selected",
                "workspace:first",
                SkillDeliveryReasonV2::FirstLoad,
                vec![SkillEpisodeUseEvidenceV2::SelectedOnly],
            ),
            episode(
                "used",
                "workspace:used",
                SkillDeliveryReasonV2::FirstLoad,
                vec![SkillEpisodeUseEvidenceV2::ProcedureStepObserved],
            ),
        ];
        let target = attribute_skill_improvement_target(SkillImprovementTargetOptions {
            target_id: "target-1".to_string(),
            episodes: &episodes,
            umbrella_skill_id: None,
            umbrella_revision: None,
        })
        .unwrap();
        assert_eq!(target.target_skill_id.as_deref(), Some("workspace:used"));
        assert_eq!(
            target.reason,
            SkillImprovementTargetReasonV1::ObservedProcedure
        );
    }

    #[test]
    fn multiple_selected_only_skills_abstain() {
        let episodes = vec![
            episode(
                "one",
                "workspace:one",
                SkillDeliveryReasonV2::FirstLoad,
                vec![SkillEpisodeUseEvidenceV2::SelectedOnly],
            ),
            episode(
                "two",
                "workspace:two",
                SkillDeliveryReasonV2::FirstLoad,
                vec![SkillEpisodeUseEvidenceV2::SelectedOnly],
            ),
        ];
        let target = attribute_skill_improvement_target(SkillImprovementTargetOptions {
            target_id: "target-1".to_string(),
            episodes: &episodes,
            umbrella_skill_id: None,
            umbrella_revision: None,
        })
        .unwrap();
        assert_eq!(target.reason, SkillImprovementTargetReasonV1::NoTarget);
        assert!(target.target_skill_id.is_none());
    }

    #[test]
    fn semantic_patch_is_checksummed_reversible_and_proposal_only() {
        let episodes = vec![episode(
            "used",
            "workspace:used",
            SkillDeliveryReasonV2::Explicit,
            vec![SkillEpisodeUseEvidenceV2::ProcedureStepObserved],
        )];
        let target = attribute_skill_improvement_target(SkillImprovementTargetOptions {
            target_id: "target-1".to_string(),
            episodes: &episodes,
            umbrella_skill_id: None,
            umbrella_revision: None,
        })
        .unwrap();
        let proposal = propose_semantic_skill_patch(SemanticPatchProposalOptions {
            target: &target,
            classification: &classification(),
            episodes: &episodes,
            semantic_sections: BTreeMap::from([
                (
                    "procedure".to_string(),
                    "Add a bounded retry step.".to_string(),
                ),
                (
                    "verification".to_string(),
                    "Replay the failing case.".to_string(),
                ),
            ]),
            affected_fixture_refs: vec!["fixture-1".to_string()],
            now_ms: 1,
        })
        .unwrap();
        assert_eq!(proposal.state, "proposal-only");
        assert!(!proposal.report_indexable);
        assert!(proposal.proposed_after_checksum.starts_with("sha256:"));
        assert!(
            proposal
                .rollback_id
                .starts_with("skill-improvement-rollback-")
        );
    }

    #[test]
    fn synthesis_requires_two_verified_episodes_and_uses_cjk_safe_name() {
        let one = episode(
            "one",
            "workspace:source",
            SkillDeliveryReasonV2::FirstLoad,
            vec![SkillEpisodeUseEvidenceV2::ProcedureStepObserved],
        );
        let sections = BTreeMap::from([
            (
                "triggers".to_string(),
                "Use for the novel intent.".to_string(),
            ),
            (
                "procedure".to_string(),
                "Follow the verified sequence.".to_string(),
            ),
            (
                "verification".to_string(),
                "Run both source replays.".to_string(),
            ),
        ]);
        assert!(
            propose_novel_skill_synthesis(SkillSynthesisProposalOptions {
                identity: identity(),
                classification: &classification(),
                episodes: std::slice::from_ref(&one),
                proposed_name: "新流程".to_string(),
                semantic_sections: sections.clone(),
                nearest_skill_refs: vec!["nearest-search:no-duplicate".to_string()],
                explicit_learn: false,
                now_ms: 1,
            })
            .is_err()
        );
        let two = episode(
            "two",
            "workspace:source",
            SkillDeliveryReasonV2::FirstLoad,
            vec![SkillEpisodeUseEvidenceV2::ProcedureStepObserved],
        );
        let proposal = propose_novel_skill_synthesis(SkillSynthesisProposalOptions {
            identity: identity(),
            classification: &classification(),
            episodes: &[one, two],
            proposed_name: "新流程".to_string(),
            semantic_sections: sections,
            nearest_skill_refs: vec!["nearest-search:no-duplicate".to_string()],
            explicit_learn: false,
            now_ms: 1,
        })
        .unwrap();
        assert!(
            proposal
                .target_skill_id
                .starts_with("agent-created:learned-")
        );
        assert_eq!(proposal.kind, SkillImprovementProposalKindV1::Synthesis);
        assert_eq!(proposal.state, "proposal-only");
    }
}
