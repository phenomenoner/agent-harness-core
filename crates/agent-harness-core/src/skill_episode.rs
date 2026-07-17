use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use ring::digest;
use serde::{Deserialize, Serialize};

use crate::{
    SKILL_OUTCOME_SCHEMA, SkillContractError, SkillDeliveryReasonV2, SkillDeliveryReceiptV2,
    SkillEcosystemIdentity, SkillOutcomeReceiptV1, SkillOutcomeStatusV1, VirtualSkillManifestV1,
    validate_joined_identity, write_json_atomic,
};

pub const SKILL_EPISODE_SCHEMA: &str = "agent-harness.skill-episode.v2";
pub const SKILL_TERMINAL_REVIEW_SCHEMA: &str = "agent-harness.skill-terminal-review.v1";
const SKILL_EPISODE_RUNTIME_HASH_DOMAIN: &[u8] = b"agent-harness/skill-episode-runtime/v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SkillEpisodeUseEvidenceV2 {
    SelectedOnly,
    CatalogCard,
    FullBodyViewed,
    ReferenceViewed,
    ProcedureAcknowledged,
    ProcedureStepObserved,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillEpisodeV2 {
    pub schema: String,
    pub episode_id: String,
    pub source_turn_hash: String,
    pub identity: SkillEcosystemIdentity,
    pub rollover_index: u32,
    pub execution_class: String,
    pub catalog_revision: String,
    pub skill_id: String,
    pub skill_revision: String,
    pub delivery_id: String,
    pub delivery_reason: SkillDeliveryReasonV2,
    pub use_evidence: Vec<SkillEpisodeUseEvidenceV2>,
    pub outcome_id: Option<String>,
    pub outcome_status: Option<SkillOutcomeStatusV1>,
    pub verifier_ref: Option<String>,
    pub correction_ref: Option<String>,
    pub source_origin: String,
    pub admission_eligible: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillEpisodeRuntimeCaptureOptions {
    pub harness_home: PathBuf,
    pub manifest_file: PathBuf,
    pub delivery_receipt_files: Vec<PathBuf>,
    pub queue_id: String,
    pub execution_class: String,
    pub source_origin: String,
    pub outcome_status: SkillOutcomeStatusV1,
    pub verifier_type: Option<String>,
    pub verifier_ref: Option<String>,
    pub correction_ref: Option<String>,
    pub terminal_eligible: bool,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillTerminalReviewReceiptV1 {
    pub schema: String,
    pub review_id: String,
    pub identity: SkillEcosystemIdentity,
    pub catalog_revision: String,
    pub checkpoint_hash: String,
    pub outcome_id: String,
    pub episode_ids: Vec<String>,
    pub disposition: String,
    pub proposal_ids: Vec<String>,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillEpisodeRuntimeCaptureReport {
    pub outcome_file: Option<PathBuf>,
    pub outcome: Option<SkillOutcomeReceiptV1>,
    pub episode_files: Vec<PathBuf>,
    pub episodes: Vec<SkillEpisodeV2>,
    pub terminal_review_file: Option<PathBuf>,
    pub terminal_review: Option<SkillTerminalReviewReceiptV1>,
}

impl SkillEpisodeV2 {
    pub fn has_attributable_use(&self) -> bool {
        self.use_evidence.iter().any(|evidence| {
            matches!(
                evidence,
                SkillEpisodeUseEvidenceV2::FullBodyViewed
                    | SkillEpisodeUseEvidenceV2::ReferenceViewed
                    | SkillEpisodeUseEvidenceV2::ProcedureAcknowledged
                    | SkillEpisodeUseEvidenceV2::ProcedureStepObserved
            )
        }) && self.delivery_reason != SkillDeliveryReasonV2::Rehydration
    }

    pub fn positive_learning_eligible(&self) -> bool {
        self.admission_eligible
            && self.has_attributable_use()
            && matches!(
                self.outcome_status,
                Some(SkillOutcomeStatusV1::VerifiedSuccess)
            )
            && self.verifier_ref.is_some()
    }
}

pub fn join_skill_episode_v2(
    episode_id: impl Into<String>,
    source_turn_hash: impl Into<String>,
    catalog_revision: impl Into<String>,
    execution_class: impl Into<String>,
    source_origin: impl Into<String>,
    rollover_index: u32,
    delivery: &SkillDeliveryReceiptV2,
    outcome: Option<&SkillOutcomeReceiptV1>,
    use_evidence: Vec<SkillEpisodeUseEvidenceV2>,
) -> Result<SkillEpisodeV2, SkillContractError> {
    delivery.validate()?;
    if let Some(outcome) = outcome {
        outcome.validate()?;
        validate_joined_identity([&delivery.identity, &outcome.identity])?;
        if !outcome
            .delivery_ids
            .iter()
            .any(|id| id == &delivery.receipt_id)
        {
            return Err(SkillContractError::InvalidField(
                "outcome does not reference delivery".to_string(),
            ));
        }
    }
    let execution_class = execution_class.into();
    let admission_eligible = !matches!(
        execution_class.as_str(),
        "cron" | "reviewer" | "skill-dream" | "system-notice" | "unjoined-child"
    );
    let episode = SkillEpisodeV2 {
        schema: SKILL_EPISODE_SCHEMA.to_string(),
        episode_id: episode_id.into(),
        source_turn_hash: source_turn_hash.into(),
        identity: delivery.identity.clone(),
        rollover_index,
        execution_class,
        catalog_revision: catalog_revision.into(),
        skill_id: delivery.skill_id.clone(),
        skill_revision: delivery.skill_revision.clone(),
        delivery_id: delivery.receipt_id.clone(),
        delivery_reason: delivery.delivery_reason,
        use_evidence,
        outcome_id: outcome.map(|value| value.receipt_id.clone()),
        outcome_status: outcome.map(|value| value.status.clone()),
        verifier_ref: outcome.and_then(|value| value.verifier_ref.clone()),
        correction_ref: outcome.and_then(|value| value.correction_ref.clone()),
        source_origin: source_origin.into(),
        admission_eligible,
    };
    if episode.episode_id.trim().is_empty() || episode.source_turn_hash.trim().is_empty() {
        return Err(SkillContractError::MissingField("episode identity"));
    }
    Ok(episode)
}

pub fn skill_outcome_receipt_dir(
    harness_home: impl AsRef<Path>,
    virtual_session_id: &str,
) -> PathBuf {
    skill_evidence_dir(harness_home, "outcomes", virtual_session_id)
}

pub fn skill_episode_receipt_dir(
    harness_home: impl AsRef<Path>,
    virtual_session_id: &str,
) -> PathBuf {
    skill_evidence_dir(harness_home, "episodes", virtual_session_id)
}

pub fn skill_terminal_review_receipt_dir(
    harness_home: impl AsRef<Path>,
    virtual_session_id: &str,
) -> PathBuf {
    skill_evidence_dir(harness_home, "terminal-reviews", virtual_session_id)
}

/// Joins already-persisted delivery evidence after a runtime result. This path
/// is evidence-only: it never mutates a skill, usage prior, catalog, or serving
/// policy, and it does not infer verified success from a normal model final.
pub fn capture_skill_episode_runtime_evidence(
    options: SkillEpisodeRuntimeCaptureOptions,
) -> io::Result<SkillEpisodeRuntimeCaptureReport> {
    let manifest: VirtualSkillManifestV1 =
        serde_json::from_slice(&fs::read(&options.manifest_file)?).map_err(io::Error::other)?;
    manifest.validate().map_err(io::Error::other)?;
    if options.delivery_receipt_files.is_empty() {
        return Ok(SkillEpisodeRuntimeCaptureReport {
            outcome_file: None,
            outcome: None,
            episode_files: Vec::new(),
            episodes: Vec::new(),
            terminal_review_file: None,
            terminal_review: None,
        });
    }
    let mut deliveries = Vec::new();
    let mut delivery_ids = BTreeSet::new();
    for path in &options.delivery_receipt_files {
        let delivery: SkillDeliveryReceiptV2 =
            serde_json::from_slice(&fs::read(path)?).map_err(io::Error::other)?;
        delivery.validate().map_err(io::Error::other)?;
        validate_joined_identity([&manifest.identity, &delivery.identity])
            .map_err(io::Error::other)?;
        if !delivery_ids.insert(delivery.receipt_id.clone()) {
            continue;
        }
        deliveries.push(delivery);
    }
    if deliveries.is_empty() {
        return Ok(SkillEpisodeRuntimeCaptureReport {
            outcome_file: None,
            outcome: None,
            episode_files: Vec::new(),
            episodes: Vec::new(),
            terminal_review_file: None,
            terminal_review: None,
        });
    }
    let source_turn_hash = sha256_hex(options.queue_id.as_bytes());
    let outcome_id = format!(
        "skill-outcome-{}",
        hash_components(&[
            manifest.identity.virtual_session_id.as_bytes(),
            source_turn_hash.as_bytes(),
            outcome_status_label(&options.outcome_status).as_bytes(),
        ])
    );
    let outcome = SkillOutcomeReceiptV1 {
        schema: SKILL_OUTCOME_SCHEMA.to_string(),
        receipt_id: outcome_id.clone(),
        identity: deliveries[0].identity.clone(),
        delivery_ids: deliveries
            .iter()
            .map(|delivery| delivery.receipt_id.clone())
            .collect(),
        // Delivery or selection is not proof that a procedure was used.
        used_skill_ids: Vec::new(),
        procedure_trace_refs: Vec::new(),
        verifier_type: options.verifier_type,
        verifier_ref: options.verifier_ref,
        status: options.outcome_status,
        correction_ref: options.correction_ref,
    };
    outcome.validate().map_err(io::Error::other)?;
    let outcome_file = write_evidence_once(
        &skill_outcome_receipt_dir(&options.harness_home, &manifest.identity.virtual_session_id),
        &outcome.receipt_id,
        &outcome,
    )?;

    let mut episodes = Vec::new();
    let mut episode_files = Vec::new();
    for delivery in &deliveries {
        let use_evidence = delivery_use_evidence(delivery);
        let episode_id = format!(
            "skill-episode-{}",
            hash_components(&[
                source_turn_hash.as_bytes(),
                delivery.receipt_id.as_bytes(),
                outcome.receipt_id.as_bytes(),
            ])
        );
        let episode = join_skill_episode_v2(
            episode_id,
            source_turn_hash.clone(),
            manifest.catalog_revision.clone(),
            options.execution_class.clone(),
            options.source_origin.clone(),
            manifest.rollover_count,
            delivery,
            Some(&outcome),
            use_evidence,
        )
        .map_err(io::Error::other)?;
        let file = write_evidence_once(
            &skill_episode_receipt_dir(
                &options.harness_home,
                &manifest.identity.virtual_session_id,
            ),
            &episode.episode_id,
            &episode,
        )?;
        episodes.push(episode);
        episode_files.push(file);
    }

    let (terminal_review_file, terminal_review) = if options.terminal_eligible {
        let review_id = format!(
            "skill-terminal-review-{}",
            hash_components(&[
                manifest.identity.virtual_session_id.as_bytes(),
                manifest.catalog_revision.as_bytes(),
                b"terminal",
            ])
        );
        let review_dir = skill_terminal_review_receipt_dir(
            &options.harness_home,
            &manifest.identity.virtual_session_id,
        );
        let existing_review_file = review_dir.join(format!("{}.json", safe_component(&review_id)));
        if existing_review_file.is_file() {
            let existing: SkillTerminalReviewReceiptV1 =
                serde_json::from_slice(&fs::read(&existing_review_file)?)
                    .map_err(io::Error::other)?;
            validate_joined_identity([&manifest.identity, &existing.identity])
                .map_err(io::Error::other)?;
            return Ok(SkillEpisodeRuntimeCaptureReport {
                outcome_file: Some(outcome_file),
                outcome: Some(outcome),
                episode_files,
                episodes,
                terminal_review_file: Some(existing_review_file),
                terminal_review: Some(existing),
            });
        }
        let attributable_count = episodes
            .iter()
            .filter(|episode| episode.has_attributable_use())
            .count();
        let review = SkillTerminalReviewReceiptV1 {
            schema: SKILL_TERMINAL_REVIEW_SCHEMA.to_string(),
            review_id: review_id.clone(),
            identity: outcome.identity.clone(),
            catalog_revision: manifest.catalog_revision.clone(),
            checkpoint_hash: source_turn_hash,
            outcome_id: outcome.receipt_id.clone(),
            episode_ids: episodes
                .iter()
                .map(|episode| episode.episode_id.clone())
                .collect(),
            disposition: if attributable_count == 0 {
                "evidence-only-no-attributable-use".to_string()
            } else if matches!(outcome.status, SkillOutcomeStatusV1::Unknown) {
                "awaiting-verifier-or-correction".to_string()
            } else {
                "proposal-evaluation-eligible".to_string()
            },
            proposal_ids: Vec::new(),
            created_at_ms: options.now_ms,
        };
        let file = write_evidence_once(&review_dir, &review_id, &review)?;
        (Some(file), Some(review))
    } else {
        (None, None)
    };
    Ok(SkillEpisodeRuntimeCaptureReport {
        outcome_file: Some(outcome_file),
        outcome: Some(outcome),
        episode_files,
        episodes,
        terminal_review_file,
        terminal_review,
    })
}

fn delivery_use_evidence(delivery: &SkillDeliveryReceiptV2) -> Vec<SkillEpisodeUseEvidenceV2> {
    let mut evidence = vec![SkillEpisodeUseEvidenceV2::SelectedOnly];
    if delivery.delivery_reason == SkillDeliveryReasonV2::None {
        evidence.push(SkillEpisodeUseEvidenceV2::CatalogCard);
    } else if delivery.delivery_reason == SkillDeliveryReasonV2::Reference {
        evidence.push(SkillEpisodeUseEvidenceV2::ReferenceViewed);
    } else if delivery.included_bytes > 0 {
        evidence.push(SkillEpisodeUseEvidenceV2::FullBodyViewed);
    }
    evidence
}

fn skill_evidence_dir(
    harness_home: impl AsRef<Path>,
    kind: &str,
    virtual_session_id: &str,
) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("skills")
        .join(kind)
        .join(safe_component(virtual_session_id))
}

fn write_evidence_once<T>(dir: &Path, id: &str, value: &T) -> io::Result<PathBuf>
where
    T: Serialize,
{
    fs::create_dir_all(dir)?;
    let path = dir.join(format!("{}.json", safe_component(id)));
    let next = serde_json::to_value(value).map_err(io::Error::other)?;
    if path.is_file() {
        let existing: serde_json::Value =
            serde_json::from_slice(&fs::read(&path)?).map_err(io::Error::other)?;
        if existing != next {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("evidence id {id} collides with different content"),
            ));
        }
        return Ok(path);
    }
    write_json_atomic(&path, value)?;
    Ok(path)
}

fn outcome_status_label(status: &SkillOutcomeStatusV1) -> &'static str {
    match status {
        SkillOutcomeStatusV1::VerifiedSuccess => "verified-success",
        SkillOutcomeStatusV1::VerifiedFailure => "verified-failure",
        SkillOutcomeStatusV1::Corrected => "corrected",
        SkillOutcomeStatusV1::Abandoned => "abandoned",
        SkillOutcomeStatusV1::Unknown => "unknown",
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
    context.update(SKILL_EPISODE_RUNTIME_HASH_DOMAIN);
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
    use crate::{
        SKILL_DELIVERY_SCHEMA, SKILL_OUTCOME_SCHEMA, activate_manifest_skill,
        create_virtual_skill_manifest, persist_manifest, record_manifest_delivery_receipt,
    };
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn identity() -> SkillEcosystemIdentity {
        SkillEcosystemIdentity {
            virtual_session_id: "vs-1".to_string(),
            root_session_key_hash: "abcdef123456".to_string(),
            concrete_session_hash: "123456abcdef".to_string(),
            exact_lane_digest: "feedface1234".to_string(),
            agent_id: "main".to_string(),
        }
    }

    fn delivery(reason: SkillDeliveryReasonV2) -> SkillDeliveryReceiptV2 {
        SkillDeliveryReceiptV2 {
            schema: SKILL_DELIVERY_SCHEMA.to_string(),
            receipt_id: "delivery-1".to_string(),
            routing_receipt_id: (reason != SkillDeliveryReasonV2::Rehydration)
                .then(|| "route-1".to_string()),
            identity: identity(),
            backend_generation: "generation-1".to_string(),
            skill_id: "demo".to_string(),
            skill_revision: "r1".to_string(),
            body_checksum: "abcdef123456".to_string(),
            delivery_kind: "full-body".to_string(),
            delivery_reason: reason,
            included_bytes: 10,
            reused_bytes: 0,
            cache_revision: "cache-1".to_string(),
        }
    }

    fn outcome() -> SkillOutcomeReceiptV1 {
        SkillOutcomeReceiptV1 {
            schema: SKILL_OUTCOME_SCHEMA.to_string(),
            receipt_id: "outcome-1".to_string(),
            identity: identity(),
            delivery_ids: vec!["delivery-1".to_string()],
            used_skill_ids: vec!["demo".to_string()],
            procedure_trace_refs: vec!["trace-1".to_string()],
            verifier_type: Some("scenario".to_string()),
            verifier_ref: Some("scenario-1".to_string()),
            status: SkillOutcomeStatusV1::VerifiedSuccess,
            correction_ref: None,
        }
    }

    #[test]
    fn selected_only_exposure_never_becomes_positive_learning() {
        let episode = join_skill_episode_v2(
            "episode-1",
            "turnhash1",
            "catalog-1",
            "interactive",
            "user",
            0,
            &delivery(SkillDeliveryReasonV2::FirstLoad),
            Some(&outcome()),
            vec![SkillEpisodeUseEvidenceV2::SelectedOnly],
        )
        .expect("join");
        assert!(!episode.has_attributable_use());
        assert!(!episode.positive_learning_eligible());
    }

    #[test]
    fn rehydration_never_becomes_fresh_positive_use() {
        let episode = join_skill_episode_v2(
            "episode-1",
            "turnhash1",
            "catalog-1",
            "interactive",
            "user",
            1,
            &delivery(SkillDeliveryReasonV2::Rehydration),
            Some(&outcome()),
            vec![SkillEpisodeUseEvidenceV2::ProcedureStepObserved],
        )
        .expect("join");
        assert!(!episode.positive_learning_eligible());
    }

    #[test]
    fn wrong_lane_outcome_is_rejected() {
        let mut wrong = outcome();
        wrong.identity.exact_lane_digest = "deadbeef1234".to_string();
        assert!(
            join_skill_episode_v2(
                "episode-1",
                "turnhash1",
                "catalog-1",
                "interactive",
                "user",
                0,
                &delivery(SkillDeliveryReasonV2::FirstLoad),
                Some(&wrong),
                vec![SkillEpisodeUseEvidenceV2::ProcedureStepObserved],
            )
            .is_err()
        );
    }

    #[test]
    fn runtime_capture_persists_joined_evidence_and_terminal_review_once() {
        let root = temp_root("runtime-capture");
        let harness_home = root.join("harness");
        let mut manifest = create_virtual_skill_manifest(
            &harness_home,
            identity(),
            Some("turn-sha256:abcdef123456".to_string()),
            "catalog-1".to_string(),
            "topology-1".to_string(),
        )
        .unwrap();
        let mut receipt = delivery(SkillDeliveryReasonV2::FirstLoad);
        receipt.skill_revision = receipt.body_checksum.clone();
        activate_manifest_skill(
            &mut manifest,
            receipt.skill_id.clone(),
            receipt.skill_revision.clone(),
            "serving-selection",
        )
        .unwrap();
        record_manifest_delivery_receipt(&mut manifest, &receipt).unwrap();
        let manifest_file = persist_manifest(&harness_home, &manifest).unwrap();
        let delivery_file = root.join("delivery.json");
        write_json_atomic(&delivery_file, &receipt).unwrap();

        let capture = |now_ms| {
            capture_skill_episode_runtime_evidence(SkillEpisodeRuntimeCaptureOptions {
                harness_home: harness_home.clone(),
                manifest_file: manifest_file.clone(),
                delivery_receipt_files: vec![delivery_file.clone(), delivery_file.clone()],
                queue_id: "queue-1".to_string(),
                execution_class: "interactive".to_string(),
                source_origin: "channel".to_string(),
                outcome_status: SkillOutcomeStatusV1::Unknown,
                verifier_type: None,
                verifier_ref: None,
                correction_ref: None,
                terminal_eligible: true,
                now_ms,
            })
            .unwrap()
        };
        let first = capture(1);
        assert_eq!(first.episodes.len(), 1, "duplicate delivery paths dedupe");
        assert!(
            first
                .outcome_file
                .as_ref()
                .is_some_and(|path| path.is_file())
        );
        assert!(first.episode_files[0].is_file());
        assert!(
            first
                .terminal_review_file
                .as_ref()
                .is_some_and(|path| path.is_file())
        );
        assert!(
            first.episodes[0]
                .use_evidence
                .contains(&SkillEpisodeUseEvidenceV2::FullBodyViewed)
        );
        assert!(!first.episodes[0].positive_learning_eligible());

        let second = capture(2);
        assert_eq!(
            second
                .terminal_review
                .as_ref()
                .map(|review| review.review_id.as_str()),
            first
                .terminal_review
                .as_ref()
                .map(|review| review.review_id.as_str())
        );
        assert_eq!(
            second
                .terminal_review
                .as_ref()
                .map(|review| review.created_at_ms),
            Some(1),
            "existing terminal review wins over a retry timestamp"
        );
        fs::remove_dir_all(root).ok();
    }

    fn temp_root(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("skill-episode-{name}-{nanos}"))
    }
}
