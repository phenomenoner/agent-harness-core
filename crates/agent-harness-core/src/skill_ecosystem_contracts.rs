use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub const VIRTUAL_SKILL_MANIFEST_SCHEMA: &str = "agent-harness.virtual-skill-manifest.v1";
pub const SKILL_ROUTING_SCHEMA: &str = "agent-harness.skill-routing.v2";
pub const SKILL_DELIVERY_SCHEMA: &str = "agent-harness.skill-delivery.v2";
pub const SKILL_OUTCOME_SCHEMA: &str = "agent-harness.skill-outcome.v1";
pub const SKILL_LEARNING_SCHEMA: &str = "agent-harness.skill-learning.v2";
pub const KNOWLEDGE_CLASSIFICATION_SCHEMA: &str = "agent-harness.knowledge-classification.v1";
pub const SKILL_DREAM_RUN_SCHEMA: &str = "agent-harness.skill-dream-run.v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillEcosystemIdentity {
    pub virtual_session_id: String,
    pub root_session_key_hash: String,
    pub concrete_session_hash: String,
    pub exact_lane_digest: String,
    pub agent_id: String,
}

impl SkillEcosystemIdentity {
    pub fn validate(&self) -> Result<(), SkillContractError> {
        validate_required("virtualSessionId", &self.virtual_session_id)?;
        validate_hash("rootSessionKeyHash", &self.root_session_key_hash)?;
        validate_hash("concreteSessionHash", &self.concrete_session_hash)?;
        validate_hash("exactLaneDigest", &self.exact_lane_digest)?;
        validate_required("agentId", &self.agent_id)
    }

    pub fn same_virtual_lane(&self, other: &Self) -> bool {
        self.virtual_session_id == other.virtual_session_id
            && self.root_session_key_hash == other.root_session_key_hash
            && self.exact_lane_digest == other.exact_lane_digest
            && self.agent_id.eq_ignore_ascii_case(&other.agent_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum VirtualSkillState {
    Candidate,
    Viewed,
    Active,
    Rejected,
    Superseded,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum VirtualSkillManifestStatus {
    Active,
    Completed,
    Abandoned,
    TerminalFailed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VirtualSkillManifestEntryV1 {
    pub skill_id: String,
    pub revision: String,
    pub state: VirtualSkillState,
    pub first_delivery_id: Option<String>,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VirtualSkillDeliveryLedgerEntryV1 {
    pub delivery_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing_receipt_id: Option<String>,
    pub skill_id: String,
    pub revision: String,
    pub backend_generation: String,
    pub concrete_session_hash: String,
    pub reason: SkillDeliveryReasonV2,
    pub included_bytes: usize,
    pub reused_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VirtualSkillManifestV1 {
    pub schema: String,
    pub identity: SkillEcosystemIdentity,
    pub task_intent_ref: Option<String>,
    pub catalog_revision: String,
    pub topology_revision: String,
    pub skills: Vec<VirtualSkillManifestEntryV1>,
    /// Append-only delivery observations. Older manifests omit this field and
    /// continue to parse as an empty ledger.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deliveries: Vec<VirtualSkillDeliveryLedgerEntryV1>,
    pub rollover_count: u32,
    pub status: VirtualSkillManifestStatus,
    pub close_reason: Option<String>,
}

impl VirtualSkillManifestV1 {
    pub fn validate(&self) -> Result<(), SkillContractError> {
        validate_schema(&self.schema, VIRTUAL_SKILL_MANIFEST_SCHEMA)?;
        self.identity.validate()?;
        validate_required("catalogRevision", &self.catalog_revision)?;
        validate_required("topologyRevision", &self.topology_revision)?;
        let mut ids = std::collections::BTreeSet::new();
        for entry in &self.skills {
            validate_required("skills.skillId", &entry.skill_id)?;
            validate_required("skills.revision", &entry.revision)?;
            validate_required("skills.reason", &entry.reason)?;
            if !ids.insert(entry.skill_id.to_ascii_lowercase()) {
                return Err(SkillContractError::DuplicateSkill(entry.skill_id.clone()));
            }
        }
        let skill_revisions = self
            .skills
            .iter()
            .map(|entry| (entry.skill_id.to_ascii_lowercase(), entry.revision.as_str()))
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut delivery_ids = std::collections::BTreeSet::new();
        for delivery in &self.deliveries {
            validate_required("deliveries.deliveryId", &delivery.delivery_id)?;
            validate_required("deliveries.skillId", &delivery.skill_id)?;
            validate_required("deliveries.revision", &delivery.revision)?;
            validate_required("deliveries.backendGeneration", &delivery.backend_generation)?;
            validate_hash(
                "deliveries.concreteSessionHash",
                &delivery.concrete_session_hash,
            )?;
            if !delivery_ids.insert(delivery.delivery_id.as_str()) {
                return Err(SkillContractError::InvalidField(format!(
                    "duplicate delivery id {}",
                    delivery.delivery_id
                )));
            }
            let Some(revision) = skill_revisions.get(&delivery.skill_id.to_ascii_lowercase())
            else {
                return Err(SkillContractError::InvalidField(format!(
                    "delivery references non-manifest skill {}",
                    delivery.skill_id
                )));
            };
            if **revision != delivery.revision {
                return Err(SkillContractError::InvalidField(format!(
                    "delivery revision differs from frozen manifest revision for {}",
                    delivery.skill_id
                )));
            }
            if delivery.reason == SkillDeliveryReasonV2::Rehydration
                && delivery.routing_receipt_id.is_some()
            {
                return Err(SkillContractError::InvalidField(
                    "rehydration ledger entry cannot create a routing link".to_string(),
                ));
            }
            if delivery.reason == SkillDeliveryReasonV2::None && delivery.included_bytes != 0 {
                return Err(SkillContractError::InvalidField(
                    "none delivery must not include prompt bytes".to_string(),
                ));
            }
        }
        if self.status == VirtualSkillManifestStatus::Active && self.close_reason.is_some() {
            return Err(SkillContractError::InvalidField(
                "active manifest cannot have closeReason".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillRoutingCandidateV2 {
    pub skill_id: String,
    pub revision: String,
    pub confidence: f64,
    pub rank: u16,
    pub disposition: String,
    pub reason_codes: Vec<String>,
    #[serde(default)]
    pub features: BTreeMap<String, f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillRoutingReceiptV2 {
    pub schema: String,
    pub receipt_id: String,
    pub turn_hash: String,
    pub identity: SkillEcosystemIdentity,
    pub channel: String,
    pub catalog_revision: String,
    pub topology_revision: String,
    pub method: String,
    pub method_version: String,
    pub task_text_bytes: usize,
    pub virtual_task_intent_bytes: usize,
    pub ambient_notes_excluded_bytes: usize,
    pub candidates: Vec<SkillRoutingCandidateV2>,
    pub selected_count: usize,
    /// The active serving policy remains authoritative in shadow mode. These
    /// IDs make the comparison explicit without changing its output.
    #[serde(default)]
    pub active_serving_skill_ids: Vec<String>,
    /// Candidate-card decisions made by the shadow policy. They are
    /// observability only and never imply prompt delivery.
    #[serde(default)]
    pub shadow_selected_skill_ids: Vec<String>,
    pub abstention_reason: Option<String>,
    pub shadow: bool,
    pub duration_ms: u64,
}

impl SkillRoutingReceiptV2 {
    pub fn validate(&self) -> Result<(), SkillContractError> {
        validate_schema(&self.schema, SKILL_ROUTING_SCHEMA)?;
        self.identity.validate()?;
        validate_hash("turnHash", &self.turn_hash)?;
        validate_required("receiptId", &self.receipt_id)?;
        validate_required("channel", &self.channel)?;
        validate_required("catalogRevision", &self.catalog_revision)?;
        validate_required("topologyRevision", &self.topology_revision)?;
        if self.selected_count > self.candidates.len() {
            return Err(SkillContractError::InvalidField(
                "selectedCount exceeds candidate count".to_string(),
            ));
        }
        if self.selected_count != self.shadow_selected_skill_ids.len() {
            return Err(SkillContractError::InvalidField(
                "selectedCount differs from shadowSelectedSkillIds".to_string(),
            ));
        }
        for candidate in &self.candidates {
            if !candidate.confidence.is_finite() || !(0.0..=1.0).contains(&candidate.confidence) {
                return Err(SkillContractError::InvalidField(format!(
                    "candidate confidence out of range for {}",
                    candidate.skill_id
                )));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SkillDeliveryReasonV2 {
    FirstLoad,
    Rehydration,
    Explicit,
    Reference,
    None,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillDeliveryReceiptV2 {
    pub schema: String,
    pub receipt_id: String,
    pub routing_receipt_id: Option<String>,
    pub identity: SkillEcosystemIdentity,
    pub backend_generation: String,
    pub skill_id: String,
    pub skill_revision: String,
    pub body_checksum: String,
    pub delivery_kind: String,
    pub delivery_reason: SkillDeliveryReasonV2,
    pub included_bytes: usize,
    pub reused_bytes: usize,
    pub cache_revision: String,
}

impl SkillDeliveryReceiptV2 {
    pub fn validate(&self) -> Result<(), SkillContractError> {
        validate_schema(&self.schema, SKILL_DELIVERY_SCHEMA)?;
        self.identity.validate()?;
        validate_required("receiptId", &self.receipt_id)?;
        validate_required("backendGeneration", &self.backend_generation)?;
        validate_required("skillId", &self.skill_id)?;
        validate_required("skillRevision", &self.skill_revision)?;
        validate_hash("bodyChecksum", &self.body_checksum)?;
        if self.delivery_reason == SkillDeliveryReasonV2::Rehydration
            && self.routing_receipt_id.is_some()
        {
            return Err(SkillContractError::InvalidField(
                "rehydration cannot create a fresh routing receipt link".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SkillOutcomeStatusV1 {
    VerifiedSuccess,
    VerifiedFailure,
    Corrected,
    Abandoned,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillOutcomeReceiptV1 {
    pub schema: String,
    pub receipt_id: String,
    pub identity: SkillEcosystemIdentity,
    pub delivery_ids: Vec<String>,
    pub used_skill_ids: Vec<String>,
    pub procedure_trace_refs: Vec<String>,
    pub verifier_type: Option<String>,
    pub verifier_ref: Option<String>,
    pub status: SkillOutcomeStatusV1,
    pub correction_ref: Option<String>,
}

impl SkillOutcomeReceiptV1 {
    pub fn validate(&self) -> Result<(), SkillContractError> {
        validate_schema(&self.schema, SKILL_OUTCOME_SCHEMA)?;
        self.identity.validate()?;
        validate_required("receiptId", &self.receipt_id)?;
        if matches!(
            self.status,
            SkillOutcomeStatusV1::VerifiedSuccess | SkillOutcomeStatusV1::VerifiedFailure
        ) && (self.verifier_type.is_none() || self.verifier_ref.is_none())
        {
            return Err(SkillContractError::InvalidField(
                "verified outcome requires verifierType and verifierRef".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillLearningReceiptV2 {
    pub schema: String,
    pub receipt_id: String,
    pub identity: SkillEcosystemIdentity,
    pub episode_ids: Vec<String>,
    pub target_skill_id: Option<String>,
    pub operation: String,
    pub source_turn_hashes: Vec<String>,
    pub delivery_ids: Vec<String>,
    pub outcome_ids: Vec<String>,
    pub before_checksum: Option<String>,
    pub after_checksum: Option<String>,
    pub token_delta: i64,
    pub replay_decision: String,
    pub rollback_id: Option<String>,
}

impl SkillLearningReceiptV2 {
    pub fn validate(&self) -> Result<(), SkillContractError> {
        validate_schema(&self.schema, SKILL_LEARNING_SCHEMA)?;
        self.identity.validate()?;
        validate_required("receiptId", &self.receipt_id)?;
        validate_required("operation", &self.operation)?;
        if self.episode_ids.is_empty() {
            return Err(SkillContractError::MissingField("episodeIds"));
        }
        if self.delivery_ids.is_empty() || self.outcome_ids.is_empty() {
            return Err(SkillContractError::InvalidField(
                "learning requires delivery and outcome evidence".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CanonicalKnowledgeDispositionV1 {
    Discard,
    VirtualTaskState,
    EpisodeEvidence,
    UserProfileMemory,
    AgentEnvironmentMemory,
    SkillProcedure,
    SkillReference,
    ReplayFixture,
    PolicyOrContract,
    Defer,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KnowledgeClassificationReceiptV1 {
    pub schema: String,
    pub receipt_id: String,
    pub identity: SkillEcosystemIdentity,
    pub candidate_id: String,
    pub evidence_refs: Vec<String>,
    pub source_class: String,
    pub disposition: CanonicalKnowledgeDispositionV1,
    pub typed_refs: BTreeMap<String, String>,
    pub deterministic_exclusion: Option<String>,
    pub confidence: f64,
    pub ambiguous: bool,
    pub contradiction_refs: Vec<String>,
    pub dream_run_id: Option<String>,
}

impl KnowledgeClassificationReceiptV1 {
    pub fn validate(&self) -> Result<(), SkillContractError> {
        validate_schema(&self.schema, KNOWLEDGE_CLASSIFICATION_SCHEMA)?;
        self.identity.validate()?;
        validate_required("receiptId", &self.receipt_id)?;
        validate_required("candidateId", &self.candidate_id)?;
        if !self.confidence.is_finite() || !(0.0..=1.0).contains(&self.confidence) {
            return Err(SkillContractError::InvalidField(
                "confidence must be within [0,1]".to_string(),
            ));
        }
        if self.ambiguous && self.disposition != CanonicalKnowledgeDispositionV1::Defer {
            return Err(SkillContractError::InvalidField(
                "ambiguous classification must defer".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SkillDreamTriggerV1 {
    Scheduled,
    StartupCatchUp,
    Operator,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillDreamRunReceiptV1 {
    pub schema: String,
    pub run_id: String,
    pub agent_id: String,
    pub library_revision: String,
    pub topology_revision: String,
    pub schedule_slot: String,
    pub timezone: String,
    pub trigger: SkillDreamTriggerV1,
    pub idempotency_key: String,
    pub mutation_mode: String,
    pub admitted_virtual_sessions: Vec<String>,
    pub excluded_virtual_sessions: BTreeMap<String, String>,
    pub phase_status: BTreeMap<String, String>,
    pub proposal_ids: Vec<String>,
    pub activated_revisions: Vec<String>,
    pub rollback_manifest: Option<String>,
    pub report_path: String,
    pub report_indexable: bool,
    pub checkpoint: String,
}

impl SkillDreamRunReceiptV1 {
    pub fn validate(&self) -> Result<(), SkillContractError> {
        validate_schema(&self.schema, SKILL_DREAM_RUN_SCHEMA)?;
        validate_required("runId", &self.run_id)?;
        validate_required("agentId", &self.agent_id)?;
        validate_required("libraryRevision", &self.library_revision)?;
        validate_required("topologyRevision", &self.topology_revision)?;
        validate_required("idempotencyKey", &self.idempotency_key)?;
        validate_required("reportPath", &self.report_path)?;
        if self.report_indexable {
            return Err(SkillContractError::InvalidField(
                "dream reports must be non-indexable".to_string(),
            ));
        }
        if !self.activated_revisions.is_empty() && self.mutation_mode == "proposal-only" {
            return Err(SkillContractError::InvalidField(
                "proposal-only dream cannot activate revisions".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkillContractError {
    SchemaMismatch {
        expected: &'static str,
        actual: String,
    },
    MissingField(&'static str),
    InvalidField(String),
    DuplicateSkill(String),
    IdentityMismatch,
}

impl std::fmt::Display for SkillContractError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SchemaMismatch { expected, actual } => {
                write!(
                    formatter,
                    "schema mismatch: expected {expected}, got {actual}"
                )
            }
            Self::MissingField(field) => write!(formatter, "missing required field {field}"),
            Self::InvalidField(reason) => write!(formatter, "invalid field: {reason}"),
            Self::DuplicateSkill(skill) => write!(formatter, "duplicate skill {skill}"),
            Self::IdentityMismatch => write!(formatter, "joined receipts cross a virtual lane"),
        }
    }
}

impl std::error::Error for SkillContractError {}

pub fn validate_joined_identity<'a>(
    identities: impl IntoIterator<Item = &'a SkillEcosystemIdentity>,
) -> Result<(), SkillContractError> {
    let mut identities = identities.into_iter();
    let Some(first) = identities.next() else {
        return Err(SkillContractError::MissingField("identity"));
    };
    first.validate()?;
    for identity in identities {
        identity.validate()?;
        if !first.same_virtual_lane(identity) {
            return Err(SkillContractError::IdentityMismatch);
        }
    }
    Ok(())
}

fn validate_schema(actual: &str, expected: &'static str) -> Result<(), SkillContractError> {
    if actual == expected {
        Ok(())
    } else {
        Err(SkillContractError::SchemaMismatch {
            expected,
            actual: actual.to_string(),
        })
    }
}

fn validate_required(field: &'static str, value: &str) -> Result<(), SkillContractError> {
    if value.trim().is_empty() {
        Err(SkillContractError::MissingField(field))
    } else if value.len() > 512 {
        Err(SkillContractError::InvalidField(format!(
            "{field} exceeds 512 bytes"
        )))
    } else {
        Ok(())
    }
}

fn validate_hash(field: &'static str, value: &str) -> Result<(), SkillContractError> {
    validate_required(field, value)?;
    let encoded = value.strip_prefix("sha256:").unwrap_or(value);
    if encoded.len() < 8
        || !encoded
            .chars()
            .all(|ch| ch.is_ascii_hexdigit() || ch == '-' || ch == '_')
    {
        return Err(SkillContractError::InvalidField(format!(
            "{field} is not a bounded opaque hash"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(session: &str, lane: &str) -> SkillEcosystemIdentity {
        SkillEcosystemIdentity {
            virtual_session_id: session.to_string(),
            root_session_key_hash: "abcdef123456".to_string(),
            concrete_session_hash: "123456abcdef".to_string(),
            exact_lane_digest: lane.to_string(),
            agent_id: "main".to_string(),
        }
    }

    #[test]
    fn joined_receipts_reject_wrong_virtual_lane() {
        let left = identity("vs-1", "feedface1234");
        let right = identity("vs-1", "deadbeef1234");
        assert_eq!(
            validate_joined_identity([&left, &right]),
            Err(SkillContractError::IdentityMismatch)
        );
    }

    #[test]
    fn routing_v2_round_trips_and_rejects_legacy_schema() {
        let receipt = SkillRoutingReceiptV2 {
            schema: SKILL_ROUTING_SCHEMA.to_string(),
            receipt_id: "route-1".to_string(),
            turn_hash: "aa11bb22".to_string(),
            identity: identity("vs-1", "feedface1234"),
            channel: "discord".to_string(),
            catalog_revision: "catalog-1".to_string(),
            topology_revision: "topology-1".to_string(),
            method: "shadow-variable-k".to_string(),
            method_version: "v2".to_string(),
            task_text_bytes: 42,
            virtual_task_intent_bytes: 12,
            ambient_notes_excluded_bytes: 900,
            candidates: vec![SkillRoutingCandidateV2 {
                skill_id: "demo".to_string(),
                revision: "r1".to_string(),
                confidence: 0.91,
                rank: 1,
                disposition: "selected".to_string(),
                reason_codes: vec!["trigger".to_string()],
                features: BTreeMap::new(),
            }],
            selected_count: 1,
            active_serving_skill_ids: vec!["legacy-demo".to_string()],
            shadow_selected_skill_ids: vec!["demo".to_string()],
            abstention_reason: None,
            shadow: true,
            duration_ms: 3,
        };
        receipt.validate().expect("valid routing receipt");
        let json = serde_json::to_string(&receipt).expect("serialize");
        let decoded: SkillRoutingReceiptV2 = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(decoded, receipt);

        let mut legacy = decoded;
        legacy.schema = "agent-harness.skill-routing.v1".to_string();
        assert!(matches!(
            legacy.validate(),
            Err(SkillContractError::SchemaMismatch { .. })
        ));
    }

    #[test]
    fn rehydration_cannot_link_a_fresh_routing_receipt() {
        let delivery = SkillDeliveryReceiptV2 {
            schema: SKILL_DELIVERY_SCHEMA.to_string(),
            receipt_id: "delivery-1".to_string(),
            routing_receipt_id: Some("route-2".to_string()),
            identity: identity("vs-1", "feedface1234"),
            backend_generation: "generation-2".to_string(),
            skill_id: "demo".to_string(),
            skill_revision: "r1".to_string(),
            body_checksum: "abcdef123456".to_string(),
            delivery_kind: "full-body".to_string(),
            delivery_reason: SkillDeliveryReasonV2::Rehydration,
            included_bytes: 100,
            reused_bytes: 0,
            cache_revision: "cache-2".to_string(),
        };
        assert!(delivery.validate().is_err());
    }

    #[test]
    fn dream_report_is_fail_closed_non_indexable_and_proposal_only() {
        let receipt = SkillDreamRunReceiptV1 {
            schema: SKILL_DREAM_RUN_SCHEMA.to_string(),
            run_id: "dream-1".to_string(),
            agent_id: "main".to_string(),
            library_revision: "library-1".to_string(),
            topology_revision: "topology-1".to_string(),
            schedule_slot: "2026-07-17T02:00:00+08:00".to_string(),
            timezone: "Asia/Taipei".to_string(),
            trigger: SkillDreamTriggerV1::Scheduled,
            idempotency_key: "slot-main-1".to_string(),
            mutation_mode: "proposal-only".to_string(),
            admitted_virtual_sessions: vec!["vs-1".to_string()],
            excluded_virtual_sessions: BTreeMap::new(),
            phase_status: BTreeMap::new(),
            proposal_ids: Vec::new(),
            activated_revisions: Vec::new(),
            rollback_manifest: None,
            report_path: "reports/dream-1.md".to_string(),
            report_indexable: false,
            checkpoint: "complete".to_string(),
        };
        receipt.validate().expect("proposal-only receipt is safe");
        let mut unsafe_receipt = receipt.clone();
        unsafe_receipt.report_indexable = true;
        assert!(unsafe_receipt.validate().is_err());
        unsafe_receipt.report_indexable = false;
        unsafe_receipt
            .activated_revisions
            .push("catalog-2".to_string());
        assert!(unsafe_receipt.validate().is_err());
    }
}
