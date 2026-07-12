use std::fmt;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize};

use crate::backend_reasoning::{
    BackendReasoningPolicyV1, BackendReasoningSource, ReasoningPreference,
};
use crate::model_catalog::ReasoningResolutionStatus;

const LEGACY_BACKEND_REASONING_EXECUTION_EVIDENCE_SCHEMA_VERSION: u32 = 1;
pub(crate) const BACKEND_REASONING_EXECUTION_EVIDENCE_SCHEMA_VERSION: u32 = 2;
const BACKEND_REASONING_POLICY_PROJECTION_SCHEMA_VERSION: u32 = 1;
const BACKEND_REASONING_EVENT_KEY_PREFIX: &str = "backend-reasoning-execution";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum BackendReasoningCapabilitySourceV1 {
    NotObserved,
    ModelsCache,
    AppServerModelList,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum BackendReasoningWireActionV1 {
    Pending,
    Sent,
    Omitted,
    Rejected,
    Indeterminate,
}

impl BackendReasoningWireActionV1 {
    pub(crate) const fn transition_sequence(self) -> u32 {
        match self {
            Self::Pending | Self::Omitted | Self::Rejected => 1,
            Self::Sent | Self::Indeterminate => 2,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BackendReasoningPolicyProjectionV1 {
    schema_version: u32,
    source: BackendReasoningSource,
    resolution_status: ReasoningResolutionStatus,
    authoritative: bool,
    effective_provider: String,
    effective_model: String,
    effective_effort: String,
    catalog_revision: String,
}

impl BackendReasoningPolicyProjectionV1 {
    fn from_policy(
        policy: &BackendReasoningPolicyV1,
    ) -> Result<Self, BackendReasoningExecutionEvidenceError> {
        let resolution = policy.resolution();
        Ok(Self {
            schema_version: BACKEND_REASONING_POLICY_PROJECTION_SCHEMA_VERSION,
            source: policy.source(),
            resolution_status: resolution.status,
            authoritative: resolution.authoritative,
            effective_provider: resolution
                .effective_provider
                .clone()
                .ok_or(BackendReasoningExecutionEvidenceError::PolicyProjectionBinding)?,
            effective_model: resolution
                .effective_model
                .clone()
                .ok_or(BackendReasoningExecutionEvidenceError::PolicyProjectionBinding)?,
            effective_effort: policy.effective_effort().to_string(),
            catalog_revision: resolution
                .catalog_revision
                .clone()
                .ok_or(BackendReasoningExecutionEvidenceError::PolicyCatalogRevisionMismatch)?,
        })
    }

    fn validate(&self) -> Result<(), BackendReasoningExecutionEvidenceError> {
        if self.schema_version != BACKEND_REASONING_POLICY_PROJECTION_SCHEMA_VERSION
            || self.resolution_status != ReasoningResolutionStatus::Accepted
            || !self.authoritative
        {
            return Err(BackendReasoningExecutionEvidenceError::PolicyProjectionBinding);
        }
        validate_bounded_value("policy.effectiveProvider", &self.effective_provider, 128)?;
        validate_bounded_value("policy.effectiveModel", &self.effective_model, 256)?;
        validate_bounded_value("policy.effectiveEffort", &self.effective_effort, 64)?;
        validate_bounded_value("policy.catalogRevision", &self.catalog_revision, 128)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BackendReasoningExecutionEvidenceV2 {
    schema_version: u32,
    pub(crate) attempt_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) queue_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) agent_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) provider: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) preference: Option<ReasoningPreference>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) policy: Option<BackendReasoningPolicyProjectionV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) policy_catalog_revision: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) wire_catalog_revision: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) capability_proof_digest: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) capability_observation_digest: Option<String>,
    pub(crate) capability_source: BackendReasoningCapabilitySourceV1,
    pub(crate) wire_action: BackendReasoningWireActionV1,
    pub(crate) transition_sequence: u32,
    pub(crate) event_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) wire_effort: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) turn_start_request_id: Option<u64>,
    pub(crate) decision_code: String,
    pub(crate) recorded_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BackendReasoningExecutionEvidenceFieldsV2 {
    pub(crate) attempt_id: String,
    pub(crate) queue_id: Option<String>,
    pub(crate) agent_id: Option<String>,
    pub(crate) provider: Option<String>,
    pub(crate) model: Option<String>,
    pub(crate) preference: Option<ReasoningPreference>,
    pub(crate) policy: Option<BackendReasoningPolicyV1>,
    pub(crate) policy_catalog_revision: Option<String>,
    pub(crate) wire_catalog_revision: Option<String>,
    pub(crate) capability_proof_digest: Option<String>,
    pub(crate) capability_observation_digest: Option<String>,
    pub(crate) capability_source: BackendReasoningCapabilitySourceV1,
    pub(crate) wire_action: BackendReasoningWireActionV1,
    pub(crate) transition_sequence: u32,
    pub(crate) wire_effort: Option<String>,
    pub(crate) turn_start_request_id: Option<u64>,
    pub(crate) decision_code: String,
    pub(crate) recorded_at_ms: i64,
}

impl BackendReasoningExecutionEvidenceV2 {
    pub(crate) fn new(
        fields: BackendReasoningExecutionEvidenceFieldsV2,
    ) -> Result<Self, BackendReasoningExecutionEvidenceError> {
        let policy = fields
            .policy
            .as_ref()
            .map(BackendReasoningPolicyProjectionV1::from_policy)
            .transpose()?;
        let event_key = backend_reasoning_event_key(&fields.attempt_id, fields.transition_sequence);
        let evidence = Self {
            schema_version: BACKEND_REASONING_EXECUTION_EVIDENCE_SCHEMA_VERSION,
            attempt_id: fields.attempt_id,
            queue_id: fields.queue_id,
            agent_id: fields.agent_id,
            provider: fields.provider,
            model: fields.model,
            preference: fields.preference,
            policy,
            policy_catalog_revision: fields.policy_catalog_revision,
            wire_catalog_revision: fields.wire_catalog_revision,
            capability_proof_digest: fields.capability_proof_digest,
            capability_observation_digest: fields.capability_observation_digest,
            capability_source: fields.capability_source,
            wire_action: fields.wire_action,
            transition_sequence: fields.transition_sequence,
            event_key,
            wire_effort: fields.wire_effort,
            turn_start_request_id: fields.turn_start_request_id,
            decision_code: fields.decision_code,
            recorded_at_ms: fields.recorded_at_ms,
        };
        evidence.validate()?;
        Ok(evidence)
    }

    #[cfg(test)]
    pub(crate) const fn schema_version(&self) -> u32 {
        self.schema_version
    }

    pub(crate) fn validate(&self) -> Result<(), BackendReasoningExecutionEvidenceError> {
        if self.schema_version != BACKEND_REASONING_EXECUTION_EVIDENCE_SCHEMA_VERSION {
            return Err(
                BackendReasoningExecutionEvidenceError::UnsupportedSchemaVersion(
                    self.schema_version,
                ),
            );
        }
        validate_required_value("attemptId", &self.attempt_id)?;
        validate_optional_value("queueId", self.queue_id.as_deref())?;
        validate_optional_value("agentId", self.agent_id.as_deref())?;
        validate_optional_value("provider", self.provider.as_deref())?;
        validate_optional_value("model", self.model.as_deref())?;
        validate_optional_value(
            "policyCatalogRevision",
            self.policy_catalog_revision.as_deref(),
        )?;
        validate_optional_value("wireCatalogRevision", self.wire_catalog_revision.as_deref())?;
        validate_optional_value(
            "capabilityProofDigest",
            self.capability_proof_digest.as_deref(),
        )?;
        validate_optional_value(
            "capabilityObservationDigest",
            self.capability_observation_digest.as_deref(),
        )?;
        validate_optional_value("wireEffort", self.wire_effort.as_deref())?;
        if self.provider.is_some() != self.model.is_some() {
            return Err(BackendReasoningExecutionEvidenceError::RoutePairMismatch);
        }
        if !valid_decision_code(&self.decision_code) {
            return Err(BackendReasoningExecutionEvidenceError::InvalidDecisionCode);
        }
        if self.recorded_at_ms < 0 {
            return Err(BackendReasoningExecutionEvidenceError::InvalidRecordedAt);
        }
        if self.turn_start_request_id == Some(0) {
            return Err(BackendReasoningExecutionEvidenceError::WireActionBinding);
        }
        if self.transition_sequence != self.wire_action.transition_sequence()
            || self.event_key
                != backend_reasoning_event_key(&self.attempt_id, self.transition_sequence)
        {
            return Err(BackendReasoningExecutionEvidenceError::TransitionBinding);
        }

        let managed = match (&self.preference, &self.policy) {
            (None, None) => {
                if self.policy_catalog_revision.is_some() {
                    return Err(
                        BackendReasoningExecutionEvidenceError::PolicyCatalogRevisionMismatch,
                    );
                }
                false
            }
            (Some(preference), Some(policy)) => {
                policy.validate()?;
                let provider = self
                    .provider
                    .as_deref()
                    .ok_or(BackendReasoningExecutionEvidenceError::MissingManagedRoute)?;
                let model = self
                    .model
                    .as_deref()
                    .ok_or(BackendReasoningExecutionEvidenceError::MissingManagedRoute)?;
                if !evidence_providers_match(provider, &policy.effective_provider)
                    || !model.eq_ignore_ascii_case(&policy.effective_model)
                {
                    return Err(BackendReasoningExecutionEvidenceError::PolicyRouteMismatch);
                }
                if self.policy_catalog_revision.as_deref() != Some(policy.catalog_revision.as_str())
                    || self.policy_catalog_revision.is_none()
                {
                    return Err(
                        BackendReasoningExecutionEvidenceError::PolicyCatalogRevisionMismatch,
                    );
                }
                if let ReasoningPreference::Explicit { effort } = preference
                    && effort != &policy.effective_effort
                {
                    return Err(BackendReasoningExecutionEvidenceError::PreferenceEffortMismatch);
                }
                true
            }
            _ => {
                return Err(BackendReasoningExecutionEvidenceError::PreferencePolicyPairMismatch);
            }
        };

        match self.capability_source {
            BackendReasoningCapabilitySourceV1::NotObserved
                if self.wire_catalog_revision.is_some()
                    || self.capability_proof_digest.is_some()
                    || self.capability_observation_digest.is_some() =>
            {
                return Err(BackendReasoningExecutionEvidenceError::CapabilityBinding);
            }
            BackendReasoningCapabilitySourceV1::ModelsCache
                if self.wire_catalog_revision.is_none()
                    || self.capability_proof_digest.is_some()
                    || self.capability_observation_digest.is_some() =>
            {
                return Err(BackendReasoningExecutionEvidenceError::CapabilityBinding);
            }
            BackendReasoningCapabilitySourceV1::AppServerModelList
                if self.wire_catalog_revision.is_some()
                    || self.capability_proof_digest.is_some()
                        == self.capability_observation_digest.is_some() =>
            {
                return Err(BackendReasoningExecutionEvidenceError::CapabilityBinding);
            }
            _ => {}
        }
        if self
            .capability_proof_digest
            .as_deref()
            .is_some_and(|digest| !valid_sha256_digest(digest))
        {
            return Err(BackendReasoningExecutionEvidenceError::InvalidCapabilityProofDigest);
        }
        if self
            .capability_observation_digest
            .as_deref()
            .is_some_and(|digest| !valid_sha256_digest(digest))
        {
            return Err(BackendReasoningExecutionEvidenceError::InvalidCapabilityObservationDigest);
        }

        match self.wire_action {
            BackendReasoningWireActionV1::Sent => {
                if !managed
                    || self.turn_start_request_id.is_none()
                    || !self.has_capability_binding()
                {
                    return Err(BackendReasoningExecutionEvidenceError::WireActionBinding);
                }
                self.validate_managed_wire_effort()?;
            }
            BackendReasoningWireActionV1::Omitted => {
                if managed
                    || self.turn_start_request_id.is_none()
                    || self.wire_effort.is_some()
                    || self.policy_catalog_revision.is_some()
                    || self.wire_catalog_revision.is_some()
                    || self.capability_proof_digest.is_some()
                    || self.capability_observation_digest.is_some()
                    || self.capability_source != BackendReasoningCapabilitySourceV1::NotObserved
                {
                    return Err(BackendReasoningExecutionEvidenceError::WireActionBinding);
                }
            }
            BackendReasoningWireActionV1::Rejected => {
                if self.turn_start_request_id.is_some() || self.wire_effort.is_some() {
                    return Err(BackendReasoningExecutionEvidenceError::WireActionBinding);
                }
            }
            BackendReasoningWireActionV1::Pending | BackendReasoningWireActionV1::Indeterminate => {
                if self.turn_start_request_id.is_none() {
                    return Err(BackendReasoningExecutionEvidenceError::WireActionBinding);
                }
                if managed {
                    if !self.has_capability_binding() {
                        return Err(BackendReasoningExecutionEvidenceError::WireActionBinding);
                    }
                    self.validate_managed_wire_effort()?;
                } else if self.wire_effort.is_some()
                    || self.policy_catalog_revision.is_some()
                    || self.wire_catalog_revision.is_some()
                    || self.capability_proof_digest.is_some()
                    || self.capability_observation_digest.is_some()
                    || self.capability_source != BackendReasoningCapabilitySourceV1::NotObserved
                {
                    return Err(BackendReasoningExecutionEvidenceError::WireActionBinding);
                }
            }
        }
        Ok(())
    }

    fn validate_managed_wire_effort(&self) -> Result<(), BackendReasoningExecutionEvidenceError> {
        let policy = self
            .policy
            .as_ref()
            .ok_or(BackendReasoningExecutionEvidenceError::WireActionBinding)?;
        if self.wire_effort.as_deref() != Some(policy.effective_effort.as_str()) {
            return Err(BackendReasoningExecutionEvidenceError::WireEffortMismatch);
        }
        Ok(())
    }

    fn has_capability_binding(&self) -> bool {
        match self.capability_source {
            BackendReasoningCapabilitySourceV1::NotObserved => false,
            BackendReasoningCapabilitySourceV1::ModelsCache => self.wire_catalog_revision.is_some(),
            BackendReasoningCapabilitySourceV1::AppServerModelList => {
                self.capability_proof_digest.is_some()
            }
        }
    }
}

impl<'de> Deserialize<'de> for BackendReasoningExecutionEvidenceV2 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = BackendReasoningExecutionEvidenceWireV1::deserialize(deserializer)?;
        if !matches!(
            wire.schema_version,
            LEGACY_BACKEND_REASONING_EXECUTION_EVIDENCE_SCHEMA_VERSION
                | BACKEND_REASONING_EXECUTION_EVIDENCE_SCHEMA_VERSION
        ) {
            return Err(D::Error::custom(
                BackendReasoningExecutionEvidenceError::UnsupportedSchemaVersion(
                    wire.schema_version,
                ),
            ));
        }
        wire.into_evidence().map_err(D::Error::custom)
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum BackendReasoningPolicyWireV1 {
    Projection(BackendReasoningPolicyProjectionV1),
    Legacy(BackendReasoningPolicyV1),
}

impl BackendReasoningPolicyWireV1 {
    fn into_projection(
        self,
    ) -> Result<BackendReasoningPolicyProjectionV1, BackendReasoningExecutionEvidenceError> {
        match self {
            Self::Projection(projection) => Ok(projection),
            Self::Legacy(policy) => BackendReasoningPolicyProjectionV1::from_policy(&policy),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BackendReasoningExecutionEvidenceWireV1 {
    #[serde(default = "legacy_backend_reasoning_execution_evidence_schema_version")]
    schema_version: u32,
    attempt_id: String,
    #[serde(default)]
    queue_id: Option<String>,
    #[serde(default)]
    agent_id: Option<String>,
    #[serde(default)]
    provider: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    preference: Option<ReasoningPreference>,
    #[serde(default)]
    policy: Option<BackendReasoningPolicyWireV1>,
    #[serde(default)]
    policy_catalog_revision: Option<String>,
    #[serde(default)]
    wire_catalog_revision: Option<String>,
    #[serde(default)]
    capability_proof_digest: Option<String>,
    #[serde(default)]
    capability_observation_digest: Option<String>,
    capability_source: BackendReasoningCapabilitySourceV1,
    wire_action: BackendReasoningWireActionV1,
    #[serde(default)]
    transition_sequence: Option<u32>,
    #[serde(default)]
    event_key: Option<String>,
    #[serde(default)]
    wire_effort: Option<String>,
    #[serde(default)]
    turn_start_request_id: Option<u64>,
    decision_code: String,
    recorded_at_ms: i64,
}

impl BackendReasoningExecutionEvidenceWireV1 {
    fn into_evidence(
        self,
    ) -> Result<BackendReasoningExecutionEvidenceV2, BackendReasoningExecutionEvidenceError> {
        let legacy_shape = self.schema_version
            == LEGACY_BACKEND_REASONING_EXECUTION_EVIDENCE_SCHEMA_VERSION
            && self.transition_sequence.is_none()
            && self.event_key.is_none();
        let (capability_source, wire_catalog_revision, capability_proof_digest) = match (
            self.capability_source,
            self.wire_catalog_revision,
            self.capability_proof_digest,
        ) {
            (BackendReasoningCapabilitySourceV1::AppServerModelList, Some(legacy_digest), None)
                if legacy_shape && valid_sha256_digest(&legacy_digest) =>
            {
                (
                    BackendReasoningCapabilitySourceV1::AppServerModelList,
                    None,
                    Some(legacy_digest),
                )
            }
            (
                BackendReasoningCapabilitySourceV1::AppServerModelList,
                Some(legacy_revision),
                None,
            ) if legacy_shape => (
                BackendReasoningCapabilitySourceV1::ModelsCache,
                Some(legacy_revision),
                None,
            ),
            (_, wire_catalog_revision, capability_proof_digest) => (
                self.capability_source,
                wire_catalog_revision,
                capability_proof_digest,
            ),
        };
        let transition_sequence = match (self.transition_sequence, self.event_key.as_deref()) {
            (Some(sequence), Some(_)) => sequence,
            (None, None) if legacy_shape => self.wire_action.transition_sequence(),
            _ => return Err(BackendReasoningExecutionEvidenceError::TransitionBinding),
        };
        let event_key = self
            .event_key
            .unwrap_or_else(|| backend_reasoning_event_key(&self.attempt_id, transition_sequence));
        let evidence = BackendReasoningExecutionEvidenceV2 {
            schema_version: BACKEND_REASONING_EXECUTION_EVIDENCE_SCHEMA_VERSION,
            attempt_id: self.attempt_id,
            queue_id: self.queue_id,
            agent_id: self.agent_id,
            provider: self.provider,
            model: self.model,
            preference: self.preference,
            policy: self
                .policy
                .map(BackendReasoningPolicyWireV1::into_projection)
                .transpose()?,
            policy_catalog_revision: self.policy_catalog_revision,
            wire_catalog_revision,
            capability_proof_digest,
            capability_observation_digest: self.capability_observation_digest,
            capability_source,
            wire_action: self.wire_action,
            transition_sequence,
            event_key,
            wire_effort: self.wire_effort,
            turn_start_request_id: self.turn_start_request_id,
            decision_code: self.decision_code,
            recorded_at_ms: self.recorded_at_ms,
        };
        evidence.validate()?;
        Ok(evidence)
    }
}

const fn legacy_backend_reasoning_execution_evidence_schema_version() -> u32 {
    LEGACY_BACKEND_REASONING_EXECUTION_EVIDENCE_SCHEMA_VERSION
}

fn backend_reasoning_event_key(attempt_id: &str, transition_sequence: u32) -> String {
    format!("{BACKEND_REASONING_EVENT_KEY_PREFIX}/{attempt_id}/{transition_sequence}")
}

fn evidence_providers_match(left: &str, right: &str) -> bool {
    let codex_alias = |provider: &str| {
        provider.eq_ignore_ascii_case("openai") || provider.eq_ignore_ascii_case("codex")
    };
    match (codex_alias(left), codex_alias(right)) {
        (true, true) => true,
        (false, false) => left.eq_ignore_ascii_case(right),
        _ => false,
    }
}

fn valid_sha256_digest(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|hex| {
        hex.len() == 64
            && hex
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    })
}

fn validate_required_value(
    field: &'static str,
    value: &str,
) -> Result<(), BackendReasoningExecutionEvidenceError> {
    if value.trim().is_empty() {
        return Err(BackendReasoningExecutionEvidenceError::MissingField(field));
    }
    if value != value.trim()
        || value
            .chars()
            .any(|character| matches!(character, '\r' | '\n' | '\0'))
        || value.len() > 512
    {
        return Err(BackendReasoningExecutionEvidenceError::NonCanonicalField(
            field,
        ));
    }
    Ok(())
}

fn validate_optional_value(
    field: &'static str,
    value: Option<&str>,
) -> Result<(), BackendReasoningExecutionEvidenceError> {
    match value {
        Some(value) => validate_required_value(field, value),
        None => Ok(()),
    }
}

fn validate_bounded_value(
    field: &'static str,
    value: &str,
    max_len: usize,
) -> Result<(), BackendReasoningExecutionEvidenceError> {
    validate_required_value(field, value)?;
    if value.len() > max_len {
        return Err(BackendReasoningExecutionEvidenceError::NonCanonicalField(
            field,
        ));
    }
    Ok(())
}

fn valid_decision_code(value: &str) -> bool {
    value.len() <= 96
        && value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && value
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && !value.contains("--")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BackendReasoningExecutionEvidenceError {
    UnsupportedSchemaVersion(u32),
    MissingField(&'static str),
    NonCanonicalField(&'static str),
    InvalidDecisionCode,
    InvalidRecordedAt,
    RoutePairMismatch,
    MissingManagedRoute,
    PreferencePolicyPairMismatch,
    PolicyProjectionBinding,
    PolicyRouteMismatch,
    PreferenceEffortMismatch,
    PolicyCatalogRevisionMismatch,
    CapabilityBinding,
    InvalidCapabilityProofDigest,
    InvalidCapabilityObservationDigest,
    TransitionBinding,
    WireEffortMismatch,
    WireActionBinding,
}

impl fmt::Display for BackendReasoningExecutionEvidenceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedSchemaVersion(version) => write!(
                formatter,
                "unsupported backend reasoning execution evidence schema version {version}"
            ),
            Self::MissingField(field) => write!(formatter, "missing required field {field}"),
            Self::NonCanonicalField(field) => {
                write!(formatter, "field {field} is not canonical")
            }
            Self::InvalidDecisionCode => formatter
                .write_str("decisionCode must be a bounded lowercase kebab-case machine code"),
            Self::InvalidRecordedAt => formatter.write_str("recordedAtMs must not be negative"),
            Self::RoutePairMismatch => {
                formatter.write_str("provider and model must be present together")
            }
            Self::MissingManagedRoute => {
                formatter.write_str("managed reasoning evidence requires an exact route")
            }
            Self::PreferencePolicyPairMismatch => {
                formatter.write_str("reasoning preference and policy must be present together")
            }
            Self::PolicyProjectionBinding => formatter.write_str(
                "reasoning policy projection must be accepted, authoritative, and bounded",
            ),
            Self::PolicyRouteMismatch => {
                formatter.write_str("reasoning policy does not bind the evidence route")
            }
            Self::PreferenceEffortMismatch => {
                formatter.write_str("explicit preference does not match policy effort")
            }
            Self::PolicyCatalogRevisionMismatch => {
                formatter.write_str("policy catalog revision does not match the policy resolution")
            }
            Self::CapabilityBinding => {
                formatter.write_str("capability source and evidence binding are inconsistent")
            }
            Self::InvalidCapabilityProofDigest => {
                formatter.write_str("capabilityProofDigest must be a lowercase sha256 digest")
            }
            Self::InvalidCapabilityObservationDigest => {
                formatter.write_str("capabilityObservationDigest must be a lowercase sha256 digest")
            }
            Self::TransitionBinding => formatter
                .write_str("transitionSequence and eventKey must bind the wire action and attempt"),
            Self::WireEffortMismatch => {
                formatter.write_str("wire effort does not match effective policy effort")
            }
            Self::WireActionBinding => {
                formatter.write_str("wire action is inconsistent with its bound execution evidence")
            }
        }
    }
}

impl std::error::Error for BackendReasoningExecutionEvidenceError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend_reasoning::{
        BackendReasoningPolicyV1, BackendReasoningSource, ReasoningPreference,
    };
    use crate::model_catalog::{
        REASONING_RESOLUTION_RECEIPT_SCHEMA_VERSION, ReasoningResolutionReceipt,
        ReasoningResolutionStatus,
    };

    const POLICY_REVISION: &str = "sha256:policy";
    const WIRE_REVISION: &str = "sha256:wire";
    const CAPABILITY_PROOF_DIGEST: &str =
        "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    fn policy(effort: &str) -> BackendReasoningPolicyV1 {
        BackendReasoningPolicyV1::new(
            BackendReasoningSource::ChannelCommand,
            ReasoningResolutionReceipt {
                schema_version: REASONING_RESOLUTION_RECEIPT_SCHEMA_VERSION,
                requested_provider: "openai".to_string(),
                requested_model: "gpt-5.6-sol".to_string(),
                effective_provider: Some("openai".to_string()),
                effective_model: Some("gpt-5.6-sol".to_string()),
                requested_effort: effort.to_string(),
                effective_effort: Some(effort.to_string()),
                catalog_effective_effort: Some(effort.to_string()),
                catalog_revision: Some(POLICY_REVISION.to_string()),
                status: ReasoningResolutionStatus::Accepted,
                authoritative: true,
                reason: "accepted exact route effort".to_string(),
            },
        )
        .unwrap()
    }

    fn managed_fields(
        preference: ReasoningPreference,
        policy: BackendReasoningPolicyV1,
        wire_action: BackendReasoningWireActionV1,
    ) -> BackendReasoningExecutionEvidenceFieldsV2 {
        let wire_effort = matches!(
            wire_action,
            BackendReasoningWireActionV1::Pending
                | BackendReasoningWireActionV1::Sent
                | BackendReasoningWireActionV1::Indeterminate
        )
        .then(|| policy.effective_effort().to_string());
        let turn_start_request_id = matches!(
            wire_action,
            BackendReasoningWireActionV1::Pending
                | BackendReasoningWireActionV1::Sent
                | BackendReasoningWireActionV1::Indeterminate
        )
        .then_some(2);
        let (wire_catalog_revision, capability_source) =
            if wire_action == BackendReasoningWireActionV1::Rejected {
                (None, BackendReasoningCapabilitySourceV1::NotObserved)
            } else {
                (
                    Some(WIRE_REVISION.to_string()),
                    BackendReasoningCapabilitySourceV1::ModelsCache,
                )
            };
        BackendReasoningExecutionEvidenceFieldsV2 {
            attempt_id: "attempt-1".to_string(),
            queue_id: Some("queue-1".to_string()),
            agent_id: Some("main56sol".to_string()),
            provider: Some("openai".to_string()),
            model: Some("gpt-5.6-sol".to_string()),
            preference: Some(preference),
            policy: Some(policy),
            policy_catalog_revision: Some(POLICY_REVISION.to_string()),
            wire_catalog_revision,
            capability_proof_digest: None,
            capability_observation_digest: None,
            capability_source,
            wire_action,
            transition_sequence: wire_action.transition_sequence(),
            wire_effort,
            turn_start_request_id,
            decision_code: "reasoning-policy-authorized".to_string(),
            recorded_at_ms: 10,
        }
    }

    fn omitted_fields() -> BackendReasoningExecutionEvidenceFieldsV2 {
        BackendReasoningExecutionEvidenceFieldsV2 {
            attempt_id: "attempt-legacy".to_string(),
            queue_id: Some("queue-legacy".to_string()),
            agent_id: Some("main".to_string()),
            provider: Some("openai".to_string()),
            model: Some("gpt-5".to_string()),
            preference: None,
            policy: None,
            policy_catalog_revision: None,
            wire_catalog_revision: None,
            capability_proof_digest: None,
            capability_observation_digest: None,
            capability_source: BackendReasoningCapabilitySourceV1::NotObserved,
            wire_action: BackendReasoningWireActionV1::Omitted,
            transition_sequence: BackendReasoningWireActionV1::Omitted.transition_sequence(),
            wire_effort: None,
            turn_start_request_id: Some(2),
            decision_code: "unmanaged-effort-omitted".to_string(),
            recorded_at_ms: 11,
        }
    }

    #[test]
    fn accepts_explicit_sent_evidence_and_serializes_only_safe_metadata() {
        let evidence = BackendReasoningExecutionEvidenceV2::new(managed_fields(
            ReasoningPreference::explicit("max").unwrap(),
            policy("max"),
            BackendReasoningWireActionV1::Sent,
        ))
        .unwrap();

        assert_eq!(evidence.wire_effort.as_deref(), Some("max"));
        let json = serde_json::to_value(&evidence).unwrap();
        assert_eq!(json["schemaVersion"], 2);
        assert_eq!(json["wireAction"], "sent");
        assert_eq!(json["capabilitySource"], "models-cache");
        assert_eq!(json["transitionSequence"], 2);
        assert_eq!(json["eventKey"], "backend-reasoning-execution/attempt-1/2");
        assert_eq!(json["policy"]["schemaVersion"], 1);
        assert_eq!(json["policy"]["source"], "channel-command");
        assert_eq!(json["policy"]["resolutionStatus"], "accepted");
        assert_eq!(json["policy"]["authoritative"], true);
        assert_eq!(json["policy"]["effectiveProvider"], "openai");
        assert_eq!(json["policy"]["effectiveModel"], "gpt-5.6-sol");
        assert_eq!(json["policy"]["effectiveEffort"], "max");
        assert_eq!(json["policy"]["catalogRevision"], POLICY_REVISION);
        assert!(json["policy"].get("resolution").is_none());
        assert!(json.get("capabilityProofDigest").is_none());
        assert_eq!(evidence.schema_version(), 2);
        let mut additive_json = json.clone();
        additive_json["futureAdditiveField"] = serde_json::json!(true);
        assert_eq!(
            serde_json::from_value::<BackendReasoningExecutionEvidenceV2>(additive_json).unwrap(),
            evidence
        );
        for forbidden in [
            "messageText",
            "prompt",
            "sessionKey",
            "channelId",
            "userId",
            "accountId",
            "path",
            "reason",
        ] {
            assert!(json.get(forbidden).is_none(), "leaked field {forbidden}");
        }
        let durable_json = serde_json::to_string(&evidence).unwrap();
        assert!(!durable_json.contains("accepted exact route effort"));
        assert!(!durable_json.contains("requestedProvider"));
        assert!(!durable_json.contains("requestedModel"));
        assert!(!durable_json.contains("requestedEffort"));
    }

    #[test]
    fn app_server_evidence_binds_proof_digest_not_catalog_revision() {
        let mut fields = managed_fields(
            ReasoningPreference::explicit("max").unwrap(),
            policy("max"),
            BackendReasoningWireActionV1::Sent,
        );
        fields.capability_source = BackendReasoningCapabilitySourceV1::AppServerModelList;
        fields.wire_catalog_revision = None;
        fields.capability_proof_digest = Some(CAPABILITY_PROOF_DIGEST.to_string());

        let evidence = BackendReasoningExecutionEvidenceV2::new(fields).unwrap();
        let json = serde_json::to_value(evidence).unwrap();
        assert_eq!(json["capabilityProofDigest"], CAPABILITY_PROOF_DIGEST);
        assert!(json.get("wireCatalogRevision").is_none());
    }

    #[test]
    fn rejected_app_server_observation_is_not_mislabelled_as_a_proof() {
        let mut fields = managed_fields(
            ReasoningPreference::explicit("max").unwrap(),
            policy("max"),
            BackendReasoningWireActionV1::Rejected,
        );
        fields.capability_source = BackendReasoningCapabilitySourceV1::AppServerModelList;
        fields.capability_observation_digest = Some(CAPABILITY_PROOF_DIGEST.to_string());

        let evidence = BackendReasoningExecutionEvidenceV2::new(fields).unwrap();
        let json = serde_json::to_value(evidence).unwrap();
        assert_eq!(json["capabilityObservationDigest"], CAPABILITY_PROOF_DIGEST);
        assert!(json.get("capabilityProofDigest").is_none());
        assert!(json.get("wireCatalogRevision").is_none());
    }

    #[test]
    fn observation_digest_cannot_authorize_turn_start() {
        let mut fields = managed_fields(
            ReasoningPreference::explicit("max").unwrap(),
            policy("max"),
            BackendReasoningWireActionV1::Sent,
        );
        fields.capability_source = BackendReasoningCapabilitySourceV1::AppServerModelList;
        fields.wire_catalog_revision = None;
        fields.capability_observation_digest = Some(CAPABILITY_PROOF_DIGEST.to_string());

        assert_eq!(
            BackendReasoningExecutionEvidenceV2::new(fields).unwrap_err(),
            BackendReasoningExecutionEvidenceError::WireActionBinding
        );
    }

    #[test]
    fn rejects_cross_bound_capability_evidence() {
        let mut app_server = managed_fields(
            ReasoningPreference::explicit("max").unwrap(),
            policy("max"),
            BackendReasoningWireActionV1::Sent,
        );
        app_server.capability_source = BackendReasoningCapabilitySourceV1::AppServerModelList;
        assert_eq!(
            BackendReasoningExecutionEvidenceV2::new(app_server).unwrap_err(),
            BackendReasoningExecutionEvidenceError::CapabilityBinding
        );

        let mut models_cache = managed_fields(
            ReasoningPreference::explicit("max").unwrap(),
            policy("max"),
            BackendReasoningWireActionV1::Sent,
        );
        models_cache.wire_catalog_revision = None;
        models_cache.capability_proof_digest = Some(CAPABILITY_PROOF_DIGEST.to_string());
        assert_eq!(
            BackendReasoningExecutionEvidenceV2::new(models_cache).unwrap_err(),
            BackendReasoningExecutionEvidenceError::CapabilityBinding
        );
    }

    #[test]
    fn rejects_noncanonical_transition_sequence() {
        let mut fields = managed_fields(
            ReasoningPreference::explicit("max").unwrap(),
            policy("max"),
            BackendReasoningWireActionV1::Sent,
        );
        fields.transition_sequence = 1;
        assert_eq!(
            BackendReasoningExecutionEvidenceV2::new(fields).unwrap_err(),
            BackendReasoningExecutionEvidenceError::TransitionBinding
        );
    }

    #[test]
    fn rejects_tampered_event_key_on_deserialization() {
        let evidence = BackendReasoningExecutionEvidenceV2::new(managed_fields(
            ReasoningPreference::explicit("max").unwrap(),
            policy("max"),
            BackendReasoningWireActionV1::Sent,
        ))
        .unwrap();
        let mut json = serde_json::to_value(evidence).unwrap();
        json["eventKey"] = serde_json::json!("backend-reasoning-execution/other-attempt/2");

        assert!(serde_json::from_value::<BackendReasoningExecutionEvidenceV2>(json).is_err());
    }

    #[test]
    fn rejects_unbounded_policy_projection_values() {
        let effort = "x".repeat(65);
        let fields = managed_fields(
            ReasoningPreference::explicit(&effort).unwrap(),
            policy(&effort),
            BackendReasoningWireActionV1::Sent,
        );

        assert_eq!(
            BackendReasoningExecutionEvidenceV2::new(fields).unwrap_err(),
            BackendReasoningExecutionEvidenceError::NonCanonicalField("policy.effectiveEffort")
        );
    }

    #[test]
    fn rejects_noncanonical_capability_proof_digest() {
        let mut fields = managed_fields(
            ReasoningPreference::explicit("max").unwrap(),
            policy("max"),
            BackendReasoningWireActionV1::Sent,
        );
        fields.capability_source = BackendReasoningCapabilitySourceV1::AppServerModelList;
        fields.wire_catalog_revision = None;
        fields.capability_proof_digest = Some("sha256:not-a-digest".to_string());

        assert_eq!(
            BackendReasoningExecutionEvidenceV2::new(fields).unwrap_err(),
            BackendReasoningExecutionEvidenceError::InvalidCapabilityProofDigest
        );
    }

    #[test]
    fn rejects_noncanonical_capability_observation_digest() {
        let mut fields = managed_fields(
            ReasoningPreference::explicit("max").unwrap(),
            policy("max"),
            BackendReasoningWireActionV1::Rejected,
        );
        fields.capability_source = BackendReasoningCapabilitySourceV1::AppServerModelList;
        fields.capability_observation_digest = Some("sha256:not-a-digest".to_string());

        assert_eq!(
            BackendReasoningExecutionEvidenceV2::new(fields).unwrap_err(),
            BackendReasoningExecutionEvidenceError::InvalidCapabilityObservationDigest
        );
    }

    #[test]
    fn deserializes_legacy_full_policy_and_mislabelled_app_server_digest_safely() {
        let legacy = serde_json::json!({
            "schemaVersion": 1,
            "attemptId": "attempt-legacy-proof",
            "queueId": "queue-1",
            "agentId": "main56sol",
            "provider": "openai",
            "model": "gpt-5.6-sol",
            "preference": ReasoningPreference::explicit("max").unwrap(),
            "policy": policy("max"),
            "policyCatalogRevision": POLICY_REVISION,
            "wireCatalogRevision": CAPABILITY_PROOF_DIGEST,
            "capabilitySource": "app-server-model-list",
            "wireAction": "sent",
            "wireEffort": "max",
            "turnStartRequestId": 2,
            "decisionCode": "reasoning-policy-authorized",
            "recordedAtMs": 10
        });

        let evidence =
            serde_json::from_value::<BackendReasoningExecutionEvidenceV2>(legacy).unwrap();
        let normalized = serde_json::to_value(evidence).unwrap();
        assert_eq!(normalized["schemaVersion"], 2);
        assert_eq!(normalized["transitionSequence"], 2);
        assert_eq!(
            normalized["eventKey"],
            "backend-reasoning-execution/attempt-legacy-proof/2"
        );
        assert_eq!(normalized["capabilityProofDigest"], CAPABILITY_PROOF_DIGEST);
        assert!(normalized.get("wireCatalogRevision").is_none());
        assert!(normalized["policy"].get("resolution").is_none());
        assert!(
            !serde_json::to_string(&normalized)
                .unwrap()
                .contains("accepted exact route effort")
        );
    }

    #[test]
    fn migrates_legacy_non_digest_app_server_revision_without_forging_a_proof() {
        let legacy = serde_json::json!({
            "schemaVersion": 1,
            "attemptId": "attempt-legacy-revision",
            "provider": "openai",
            "model": "gpt-5.6-sol",
            "preference": ReasoningPreference::explicit("max").unwrap(),
            "policy": policy("max"),
            "policyCatalogRevision": POLICY_REVISION,
            "wireCatalogRevision": WIRE_REVISION,
            "capabilitySource": "app-server-model-list",
            "wireAction": "sent",
            "wireEffort": "max",
            "turnStartRequestId": 2,
            "decisionCode": "reasoning-policy-authorized",
            "recordedAtMs": 10
        });

        let evidence =
            serde_json::from_value::<BackendReasoningExecutionEvidenceV2>(legacy).unwrap();
        let normalized = serde_json::to_value(evidence).unwrap();
        assert_eq!(normalized["schemaVersion"], 2);
        assert_eq!(normalized["capabilitySource"], "models-cache");
        assert_eq!(normalized["wireCatalogRevision"], WIRE_REVISION);
        assert!(normalized.get("capabilityProofDigest").is_none());
    }

    #[test]
    fn accepts_default_sent_evidence_with_exact_resolved_effort() {
        let evidence = BackendReasoningExecutionEvidenceV2::new(managed_fields(
            ReasoningPreference::Default,
            policy("low"),
            BackendReasoningWireActionV1::Sent,
        ))
        .unwrap();

        assert_eq!(evidence.wire_effort.as_deref(), Some("low"));
    }

    #[test]
    fn accepts_unmanaged_omitted_evidence() {
        let evidence = BackendReasoningExecutionEvidenceV2::new(omitted_fields()).unwrap();
        assert_eq!(evidence.wire_action, BackendReasoningWireActionV1::Omitted);
        assert!(evidence.policy.is_none());
    }

    #[test]
    fn accepts_pending_rejected_and_indeterminate_evidence() {
        for action in [
            BackendReasoningWireActionV1::Pending,
            BackendReasoningWireActionV1::Rejected,
            BackendReasoningWireActionV1::Indeterminate,
        ] {
            BackendReasoningExecutionEvidenceV2::new(managed_fields(
                ReasoningPreference::explicit("max").unwrap(),
                policy("max"),
                action,
            ))
            .unwrap();
        }
    }

    #[test]
    fn rejects_preference_policy_pair_mismatch() {
        let mut fields = omitted_fields();
        fields.preference = Some(ReasoningPreference::explicit("max").unwrap());
        assert_eq!(
            BackendReasoningExecutionEvidenceV2::new(fields).unwrap_err(),
            BackendReasoningExecutionEvidenceError::PreferencePolicyPairMismatch
        );
    }

    #[test]
    fn rejects_policy_route_mismatch() {
        let mut fields = managed_fields(
            ReasoningPreference::explicit("max").unwrap(),
            policy("max"),
            BackendReasoningWireActionV1::Sent,
        );
        fields.model = Some("gpt-5.6-luna".to_string());
        assert_eq!(
            BackendReasoningExecutionEvidenceV2::new(fields).unwrap_err(),
            BackendReasoningExecutionEvidenceError::PolicyRouteMismatch
        );
    }

    #[test]
    fn rejects_preference_or_wire_effort_mismatch() {
        let mut preference_fields = managed_fields(
            ReasoningPreference::explicit("high").unwrap(),
            policy("max"),
            BackendReasoningWireActionV1::Sent,
        );
        assert_eq!(
            BackendReasoningExecutionEvidenceV2::new(preference_fields.clone()).unwrap_err(),
            BackendReasoningExecutionEvidenceError::PreferenceEffortMismatch
        );

        preference_fields.preference = Some(ReasoningPreference::explicit("max").unwrap());
        preference_fields.wire_effort = Some("high".to_string());
        assert_eq!(
            BackendReasoningExecutionEvidenceV2::new(preference_fields).unwrap_err(),
            BackendReasoningExecutionEvidenceError::WireEffortMismatch
        );
    }

    #[test]
    fn rejects_invalid_action_bindings() {
        let mut omitted = omitted_fields();
        omitted.wire_effort = Some("low".to_string());
        assert_eq!(
            BackendReasoningExecutionEvidenceV2::new(omitted).unwrap_err(),
            BackendReasoningExecutionEvidenceError::WireActionBinding
        );

        let mut rejected = managed_fields(
            ReasoningPreference::explicit("max").unwrap(),
            policy("max"),
            BackendReasoningWireActionV1::Rejected,
        );
        rejected.turn_start_request_id = Some(2);
        assert_eq!(
            BackendReasoningExecutionEvidenceV2::new(rejected).unwrap_err(),
            BackendReasoningExecutionEvidenceError::WireActionBinding
        );

        let mut unsafe_decision = omitted_fields();
        unsafe_decision.decision_code = "raw error: C:\\private\\prompt.txt".to_string();
        assert_eq!(
            BackendReasoningExecutionEvidenceV2::new(unsafe_decision).unwrap_err(),
            BackendReasoningExecutionEvidenceError::InvalidDecisionCode
        );
    }
}
