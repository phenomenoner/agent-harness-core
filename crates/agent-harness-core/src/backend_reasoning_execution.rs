use std::fmt;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize};

use crate::backend_reasoning::{BackendReasoningPolicyV1, ReasoningPreference};

pub(crate) const BACKEND_REASONING_EXECUTION_EVIDENCE_SCHEMA_VERSION: u32 = 1;

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BackendReasoningExecutionEvidenceV1 {
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
    pub(crate) policy: Option<BackendReasoningPolicyV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) policy_catalog_revision: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) wire_catalog_revision: Option<String>,
    pub(crate) capability_source: BackendReasoningCapabilitySourceV1,
    pub(crate) wire_action: BackendReasoningWireActionV1,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) wire_effort: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) turn_start_request_id: Option<u64>,
    pub(crate) decision_code: String,
    pub(crate) recorded_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BackendReasoningExecutionEvidenceFieldsV1 {
    pub(crate) attempt_id: String,
    pub(crate) queue_id: Option<String>,
    pub(crate) agent_id: Option<String>,
    pub(crate) provider: Option<String>,
    pub(crate) model: Option<String>,
    pub(crate) preference: Option<ReasoningPreference>,
    pub(crate) policy: Option<BackendReasoningPolicyV1>,
    pub(crate) policy_catalog_revision: Option<String>,
    pub(crate) wire_catalog_revision: Option<String>,
    pub(crate) capability_source: BackendReasoningCapabilitySourceV1,
    pub(crate) wire_action: BackendReasoningWireActionV1,
    pub(crate) wire_effort: Option<String>,
    pub(crate) turn_start_request_id: Option<u64>,
    pub(crate) decision_code: String,
    pub(crate) recorded_at_ms: i64,
}

impl BackendReasoningExecutionEvidenceV1 {
    pub(crate) fn new(
        fields: BackendReasoningExecutionEvidenceFieldsV1,
    ) -> Result<Self, BackendReasoningExecutionEvidenceError> {
        let evidence = Self {
            schema_version: BACKEND_REASONING_EXECUTION_EVIDENCE_SCHEMA_VERSION,
            attempt_id: fields.attempt_id,
            queue_id: fields.queue_id,
            agent_id: fields.agent_id,
            provider: fields.provider,
            model: fields.model,
            preference: fields.preference,
            policy: fields.policy,
            policy_catalog_revision: fields.policy_catalog_revision,
            wire_catalog_revision: fields.wire_catalog_revision,
            capability_source: fields.capability_source,
            wire_action: fields.wire_action,
            wire_effort: fields.wire_effort,
            turn_start_request_id: fields.turn_start_request_id,
            decision_code: fields.decision_code,
            recorded_at_ms: fields.recorded_at_ms,
        };
        evidence.validate()?;
        Ok(evidence)
    }

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
                let provider = self
                    .provider
                    .as_deref()
                    .ok_or(BackendReasoningExecutionEvidenceError::MissingManagedRoute)?;
                let model = self
                    .model
                    .as_deref()
                    .ok_or(BackendReasoningExecutionEvidenceError::MissingManagedRoute)?;
                policy
                    .validate_for_execution_route(provider, model)
                    .map_err(|_| BackendReasoningExecutionEvidenceError::PolicyRouteMismatch)?;
                if self.policy_catalog_revision.as_deref()
                    != policy.resolution().catalog_revision.as_deref()
                    || self.policy_catalog_revision.is_none()
                {
                    return Err(
                        BackendReasoningExecutionEvidenceError::PolicyCatalogRevisionMismatch,
                    );
                }
                if let ReasoningPreference::Explicit { effort } = preference
                    && effort != policy.effective_effort()
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
                if self.wire_catalog_revision.is_some() =>
            {
                return Err(BackendReasoningExecutionEvidenceError::CapabilityBinding);
            }
            BackendReasoningCapabilitySourceV1::ModelsCache
            | BackendReasoningCapabilitySourceV1::AppServerModelList
                if self.wire_catalog_revision.is_none() =>
            {
                return Err(BackendReasoningExecutionEvidenceError::CapabilityBinding);
            }
            _ => {}
        }

        match self.wire_action {
            BackendReasoningWireActionV1::Sent => {
                if !managed
                    || self.turn_start_request_id.is_none()
                    || self.wire_catalog_revision.is_none()
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
                    if self.wire_catalog_revision.is_none() {
                        return Err(BackendReasoningExecutionEvidenceError::WireActionBinding);
                    }
                    self.validate_managed_wire_effort()?;
                } else if self.wire_effort.is_some()
                    || self.policy_catalog_revision.is_some()
                    || self.wire_catalog_revision.is_some()
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
        if self.wire_effort.as_deref() != Some(policy.effective_effort()) {
            return Err(BackendReasoningExecutionEvidenceError::WireEffortMismatch);
        }
        Ok(())
    }
}

impl<'de> Deserialize<'de> for BackendReasoningExecutionEvidenceV1 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = BackendReasoningExecutionEvidenceWireV1::deserialize(deserializer)?;
        if wire.schema_version != BACKEND_REASONING_EXECUTION_EVIDENCE_SCHEMA_VERSION {
            return Err(D::Error::custom(
                BackendReasoningExecutionEvidenceError::UnsupportedSchemaVersion(
                    wire.schema_version,
                ),
            ));
        }
        Self::new(wire.into_fields()).map_err(D::Error::custom)
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BackendReasoningExecutionEvidenceWireV1 {
    #[serde(default = "backend_reasoning_execution_evidence_schema_version")]
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
    policy: Option<BackendReasoningPolicyV1>,
    #[serde(default)]
    policy_catalog_revision: Option<String>,
    #[serde(default)]
    wire_catalog_revision: Option<String>,
    capability_source: BackendReasoningCapabilitySourceV1,
    wire_action: BackendReasoningWireActionV1,
    #[serde(default)]
    wire_effort: Option<String>,
    #[serde(default)]
    turn_start_request_id: Option<u64>,
    decision_code: String,
    recorded_at_ms: i64,
}

impl BackendReasoningExecutionEvidenceWireV1 {
    fn into_fields(self) -> BackendReasoningExecutionEvidenceFieldsV1 {
        BackendReasoningExecutionEvidenceFieldsV1 {
            attempt_id: self.attempt_id,
            queue_id: self.queue_id,
            agent_id: self.agent_id,
            provider: self.provider,
            model: self.model,
            preference: self.preference,
            policy: self.policy,
            policy_catalog_revision: self.policy_catalog_revision,
            wire_catalog_revision: self.wire_catalog_revision,
            capability_source: self.capability_source,
            wire_action: self.wire_action,
            wire_effort: self.wire_effort,
            turn_start_request_id: self.turn_start_request_id,
            decision_code: self.decision_code,
            recorded_at_ms: self.recorded_at_ms,
        }
    }
}

const fn backend_reasoning_execution_evidence_schema_version() -> u32 {
    BACKEND_REASONING_EXECUTION_EVIDENCE_SCHEMA_VERSION
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
    PolicyRouteMismatch,
    PreferenceEffortMismatch,
    PolicyCatalogRevisionMismatch,
    CapabilityBinding,
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
                formatter.write_str("capability source and wire catalog revision are inconsistent")
            }
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
    ) -> BackendReasoningExecutionEvidenceFieldsV1 {
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
        BackendReasoningExecutionEvidenceFieldsV1 {
            attempt_id: "attempt-1".to_string(),
            queue_id: Some("queue-1".to_string()),
            agent_id: Some("main56sol".to_string()),
            provider: Some("openai".to_string()),
            model: Some("gpt-5.6-sol".to_string()),
            preference: Some(preference),
            policy: Some(policy),
            policy_catalog_revision: Some(POLICY_REVISION.to_string()),
            wire_catalog_revision,
            capability_source,
            wire_action,
            wire_effort,
            turn_start_request_id,
            decision_code: "reasoning-policy-authorized".to_string(),
            recorded_at_ms: 10,
        }
    }

    fn omitted_fields() -> BackendReasoningExecutionEvidenceFieldsV1 {
        BackendReasoningExecutionEvidenceFieldsV1 {
            attempt_id: "attempt-legacy".to_string(),
            queue_id: Some("queue-legacy".to_string()),
            agent_id: Some("main".to_string()),
            provider: Some("openai".to_string()),
            model: Some("gpt-5".to_string()),
            preference: None,
            policy: None,
            policy_catalog_revision: None,
            wire_catalog_revision: None,
            capability_source: BackendReasoningCapabilitySourceV1::NotObserved,
            wire_action: BackendReasoningWireActionV1::Omitted,
            wire_effort: None,
            turn_start_request_id: Some(2),
            decision_code: "unmanaged-effort-omitted".to_string(),
            recorded_at_ms: 11,
        }
    }

    #[test]
    fn accepts_explicit_sent_evidence_and_serializes_only_safe_metadata() {
        let evidence = BackendReasoningExecutionEvidenceV1::new(managed_fields(
            ReasoningPreference::explicit("max").unwrap(),
            policy("max"),
            BackendReasoningWireActionV1::Sent,
        ))
        .unwrap();

        assert_eq!(evidence.wire_effort.as_deref(), Some("max"));
        let json = serde_json::to_value(&evidence).unwrap();
        assert_eq!(json["schemaVersion"], 1);
        assert_eq!(json["wireAction"], "sent");
        assert_eq!(json["capabilitySource"], "models-cache");
        assert_eq!(evidence.schema_version(), 1);
        let mut additive_json = json.clone();
        additive_json["futureAdditiveField"] = serde_json::json!(true);
        assert_eq!(
            serde_json::from_value::<BackendReasoningExecutionEvidenceV1>(additive_json).unwrap(),
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
    }

    #[test]
    fn accepts_default_sent_evidence_with_exact_resolved_effort() {
        let evidence = BackendReasoningExecutionEvidenceV1::new(managed_fields(
            ReasoningPreference::Default,
            policy("low"),
            BackendReasoningWireActionV1::Sent,
        ))
        .unwrap();

        assert_eq!(evidence.wire_effort.as_deref(), Some("low"));
    }

    #[test]
    fn accepts_unmanaged_omitted_evidence() {
        let evidence = BackendReasoningExecutionEvidenceV1::new(omitted_fields()).unwrap();
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
            BackendReasoningExecutionEvidenceV1::new(managed_fields(
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
            BackendReasoningExecutionEvidenceV1::new(fields).unwrap_err(),
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
            BackendReasoningExecutionEvidenceV1::new(fields).unwrap_err(),
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
            BackendReasoningExecutionEvidenceV1::new(preference_fields.clone()).unwrap_err(),
            BackendReasoningExecutionEvidenceError::PreferenceEffortMismatch
        );

        preference_fields.preference = Some(ReasoningPreference::explicit("max").unwrap());
        preference_fields.wire_effort = Some("high".to_string());
        assert_eq!(
            BackendReasoningExecutionEvidenceV1::new(preference_fields).unwrap_err(),
            BackendReasoningExecutionEvidenceError::WireEffortMismatch
        );
    }

    #[test]
    fn rejects_invalid_action_bindings() {
        let mut omitted = omitted_fields();
        omitted.wire_effort = Some("low".to_string());
        assert_eq!(
            BackendReasoningExecutionEvidenceV1::new(omitted).unwrap_err(),
            BackendReasoningExecutionEvidenceError::WireActionBinding
        );

        let mut rejected = managed_fields(
            ReasoningPreference::explicit("max").unwrap(),
            policy("max"),
            BackendReasoningWireActionV1::Rejected,
        );
        rejected.turn_start_request_id = Some(2);
        assert_eq!(
            BackendReasoningExecutionEvidenceV1::new(rejected).unwrap_err(),
            BackendReasoningExecutionEvidenceError::WireActionBinding
        );

        let mut unsafe_decision = omitted_fields();
        unsafe_decision.decision_code = "raw error: C:\\private\\prompt.txt".to_string();
        assert_eq!(
            BackendReasoningExecutionEvidenceV1::new(unsafe_decision).unwrap_err(),
            BackendReasoningExecutionEvidenceError::InvalidDecisionCode
        );
    }
}
