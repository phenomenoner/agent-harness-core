use std::fmt;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize};

use crate::backend_reasoning::{BackendReasoningPolicyV1, ReasoningPreference};
use crate::execution_mode::{AuthorizedExecutionModeSnapshotV2, is_reserved_execution_mode_effort};

pub const CHILD_EXECUTION_POLICY_SCHEMA: &str = "agent-harness.child-execution-policy.v1";
pub const CHILD_EXECUTION_POLICY_V2_SCHEMA: &str = "agent-harness.child-execution-policy.v2";
pub const CHILD_EXECUTION_POLICY_MAX_STRING_BYTES: usize = 4_096;
pub const CHILD_EXECUTION_POLICY_MAX_TIMEOUT_MS: u64 = 604_800_000;
pub const CHILD_EXECUTION_POLICY_MAX_ATTEMPTS: u32 = 100;
pub const CHILD_EXECUTION_POLICY_MAX_DELEGATION_LIMIT: u32 = 10_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChildExecutionPolicyV1 {
    schema: String,
    policy_revision: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    provider: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reasoning_preference: Option<ReasoningPreference>,
    #[serde(skip_serializing_if = "Option::is_none")]
    backend_reasoning_policy: Option<BackendReasoningPolicyV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    catalog_revision: Option<String>,
    tools_profile: String,
    sandbox_profile: String,
    timeout_ms: u64,
    heartbeat_timeout_ms: u64,
    max_attempts: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    token_or_cost_budget: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    delegation_limit: Option<u32>,
    result_contract: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChildExecutionPolicyV1Input {
    pub policy_revision: u64,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub reasoning_preference: Option<ReasoningPreference>,
    pub backend_reasoning_policy: Option<BackendReasoningPolicyV1>,
    pub catalog_revision: Option<String>,
    pub tools_profile: String,
    pub sandbox_profile: String,
    pub timeout_ms: u64,
    pub heartbeat_timeout_ms: u64,
    pub max_attempts: u32,
    pub token_or_cost_budget: Option<String>,
    pub delegation_limit: Option<u32>,
    pub result_contract: String,
}

/// Additive wrapper that keeps the public V1 policy/input source-compatible
/// while carrying execution-mode authorization on an independent axis.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChildExecutionPolicyV2 {
    schema: String,
    child_policy: ChildExecutionPolicyV1,
    #[serde(skip_serializing_if = "Option::is_none")]
    authorized_execution_mode: Option<AuthorizedExecutionModeSnapshotV2>,
}

impl ChildExecutionPolicyV2 {
    pub fn new(
        child_policy: ChildExecutionPolicyV1,
        authorized_execution_mode: Option<AuthorizedExecutionModeSnapshotV2>,
    ) -> Result<Self, ChildExecutionPolicyError> {
        let policy = Self {
            schema: CHILD_EXECUTION_POLICY_V2_SCHEMA.to_string(),
            child_policy,
            authorized_execution_mode,
        };
        policy.validate()?;
        Ok(policy)
    }

    pub fn validate(&self) -> Result<(), ChildExecutionPolicyError> {
        if self.schema != CHILD_EXECUTION_POLICY_V2_SCHEMA {
            return Err(ChildExecutionPolicyError::UnsupportedSchema(
                self.schema.clone(),
            ));
        }
        self.child_policy.validate()?;
        if let Some(ReasoningPreference::Explicit { effort }) =
            self.child_policy.reasoning_preference()
            && is_reserved_execution_mode_effort(effort)
        {
            return Err(ChildExecutionPolicyError::ReservedExecutionModeEffort(
                effort.clone(),
            ));
        }
        if let Some(effort) = self.child_policy.effective_effort()
            && is_reserved_execution_mode_effort(effort)
        {
            return Err(ChildExecutionPolicyError::ReservedExecutionModeEffort(
                effort.to_string(),
            ));
        }
        if let Some(snapshot) = &self.authorized_execution_mode {
            snapshot.validate().map_err(|error| {
                ChildExecutionPolicyError::ExecutionModeSnapshot(error.to_string())
            })?;
        }
        Ok(())
    }

    pub fn schema(&self) -> &str {
        &self.schema
    }

    pub const fn child_policy(&self) -> &ChildExecutionPolicyV1 {
        &self.child_policy
    }

    pub const fn authorized_execution_mode(&self) -> Option<&AuthorizedExecutionModeSnapshotV2> {
        self.authorized_execution_mode.as_ref()
    }
}

impl<'de> Deserialize<'de> for ChildExecutionPolicyV2 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        struct Wire {
            schema: String,
            child_policy: ChildExecutionPolicyV1,
            #[serde(default)]
            authorized_execution_mode: Option<AuthorizedExecutionModeSnapshotV2>,
        }

        let wire = Wire::deserialize(deserializer)?;
        let policy = Self {
            schema: wire.schema,
            child_policy: wire.child_policy,
            authorized_execution_mode: wire.authorized_execution_mode,
        };
        policy.validate().map_err(D::Error::custom)?;
        Ok(policy)
    }
}

impl ChildExecutionPolicyV1 {
    pub fn new(input: ChildExecutionPolicyV1Input) -> Result<Self, ChildExecutionPolicyError> {
        let policy = Self {
            schema: CHILD_EXECUTION_POLICY_SCHEMA.to_string(),
            policy_revision: input.policy_revision,
            provider: input.provider,
            model: input.model,
            reasoning_preference: input.reasoning_preference,
            backend_reasoning_policy: input.backend_reasoning_policy,
            catalog_revision: input.catalog_revision,
            tools_profile: input.tools_profile,
            sandbox_profile: input.sandbox_profile,
            timeout_ms: input.timeout_ms,
            heartbeat_timeout_ms: input.heartbeat_timeout_ms,
            max_attempts: input.max_attempts,
            token_or_cost_budget: input.token_or_cost_budget,
            delegation_limit: input.delegation_limit,
            result_contract: input.result_contract,
        };
        policy.validate()?;
        Ok(policy)
    }

    pub fn validate(&self) -> Result<(), ChildExecutionPolicyError> {
        if self.schema != CHILD_EXECUTION_POLICY_SCHEMA {
            return Err(ChildExecutionPolicyError::UnsupportedSchema(
                self.schema.clone(),
            ));
        }
        if self.policy_revision == 0 {
            return Err(ChildExecutionPolicyError::ZeroPolicyRevision);
        }
        validate_optional_string("provider", self.provider.as_deref())?;
        validate_optional_string("model", self.model.as_deref())?;
        validate_optional_string("catalogRevision", self.catalog_revision.as_deref())?;
        validate_required_string("toolsProfile", &self.tools_profile)?;
        validate_required_string("sandboxProfile", &self.sandbox_profile)?;
        validate_optional_string("tokenOrCostBudget", self.token_or_cost_budget.as_deref())?;
        validate_required_string("resultContract", &self.result_contract)?;

        if self.provider.is_some() != self.model.is_some() {
            return Err(ChildExecutionPolicyError::ProviderModelPairRequired);
        }
        if self.reasoning_preference.is_some() != self.backend_reasoning_policy.is_some() {
            return Err(ChildExecutionPolicyError::ReasoningPairRequired);
        }
        if let (Some(preference), Some(policy)) = (
            self.reasoning_preference.as_ref(),
            self.backend_reasoning_policy.as_ref(),
        ) {
            let (Some(provider), Some(model)) = (self.provider.as_deref(), self.model.as_deref())
            else {
                return Err(ChildExecutionPolicyError::ReasoningRequiresRoute);
            };
            preference.validate().map_err(|error| {
                ChildExecutionPolicyError::ReasoningPreference(error.to_string())
            })?;
            if let ReasoningPreference::Explicit { effort } = preference {
                validate_string("reasoningPreference.effort", effort)?;
                if is_reserved_execution_mode_effort(effort) {
                    return Err(ChildExecutionPolicyError::ReservedExecutionModeEffort(
                        effort.clone(),
                    ));
                }
            }
            policy
                .validate_for_execution_route(provider, model)
                .map_err(|error| ChildExecutionPolicyError::BackendPolicy(error.to_string()))?;
            validate_string(
                "backendReasoningPolicy.effectiveEffort",
                policy.effective_effort(),
            )?;
            if is_reserved_execution_mode_effort(policy.effective_effort()) {
                return Err(ChildExecutionPolicyError::ReservedExecutionModeEffort(
                    policy.effective_effort().to_string(),
                ));
            }
            if let ReasoningPreference::Explicit { effort } = preference
                && effort != policy.effective_effort()
            {
                return Err(ChildExecutionPolicyError::ReasoningEffortMismatch {
                    preference: effort.clone(),
                    policy: policy.effective_effort().to_string(),
                });
            }
            let receipt_revision = policy.resolution().catalog_revision.as_deref();
            if self.catalog_revision.as_deref() != receipt_revision {
                return Err(ChildExecutionPolicyError::CatalogRevisionMismatch {
                    snapshot: self.catalog_revision.clone(),
                    policy: receipt_revision.map(ToString::to_string),
                });
            }
        }

        validate_timeout("timeoutMs", self.timeout_ms)?;
        validate_timeout("heartbeatTimeoutMs", self.heartbeat_timeout_ms)?;
        if self.heartbeat_timeout_ms > self.timeout_ms {
            return Err(ChildExecutionPolicyError::HeartbeatExceedsTimeout {
                heartbeat_timeout_ms: self.heartbeat_timeout_ms,
                timeout_ms: self.timeout_ms,
            });
        }
        if self.max_attempts == 0 || self.max_attempts > CHILD_EXECUTION_POLICY_MAX_ATTEMPTS {
            return Err(ChildExecutionPolicyError::InvalidMaxAttempts(
                self.max_attempts,
            ));
        }
        if self
            .delegation_limit
            .is_some_and(|limit| limit > CHILD_EXECUTION_POLICY_MAX_DELEGATION_LIMIT)
        {
            return Err(ChildExecutionPolicyError::DelegationLimitTooLarge(
                self.delegation_limit.unwrap_or_default(),
            ));
        }
        Ok(())
    }

    pub fn schema(&self) -> &str {
        &self.schema
    }

    pub const fn policy_revision(&self) -> u64 {
        self.policy_revision
    }

    pub fn provider(&self) -> Option<&str> {
        self.provider.as_deref()
    }

    pub fn model(&self) -> Option<&str> {
        self.model.as_deref()
    }

    pub const fn reasoning_preference(&self) -> Option<&ReasoningPreference> {
        self.reasoning_preference.as_ref()
    }

    pub const fn backend_reasoning_policy(&self) -> Option<&BackendReasoningPolicyV1> {
        self.backend_reasoning_policy.as_ref()
    }

    pub fn catalog_revision(&self) -> Option<&str> {
        self.catalog_revision.as_deref()
    }

    pub fn tools_profile(&self) -> &str {
        &self.tools_profile
    }

    pub fn sandbox_profile(&self) -> &str {
        &self.sandbox_profile
    }

    pub const fn timeout_ms(&self) -> u64 {
        self.timeout_ms
    }

    pub const fn heartbeat_timeout_ms(&self) -> u64 {
        self.heartbeat_timeout_ms
    }

    pub const fn max_attempts(&self) -> u32 {
        self.max_attempts
    }

    pub fn token_or_cost_budget(&self) -> Option<&str> {
        self.token_or_cost_budget.as_deref()
    }

    pub const fn delegation_limit(&self) -> Option<u32> {
        self.delegation_limit
    }

    pub fn result_contract(&self) -> &str {
        &self.result_contract
    }

    pub const fn is_managed_route(&self) -> bool {
        self.provider.is_some()
    }

    pub fn effective_effort(&self) -> Option<&str> {
        self.backend_reasoning_policy
            .as_ref()
            .map(BackendReasoningPolicyV1::effective_effort)
    }
}

impl<'de> Deserialize<'de> for ChildExecutionPolicyV1 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ChildExecutionPolicyV1Wire::deserialize(deserializer)?;
        if wire.schema != CHILD_EXECUTION_POLICY_SCHEMA {
            return Err(D::Error::custom(
                ChildExecutionPolicyError::UnsupportedSchema(wire.schema),
            ));
        }
        Self::new(wire.into_input()).map_err(D::Error::custom)
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ChildExecutionPolicyV1Wire {
    schema: String,
    policy_revision: u64,
    #[serde(default)]
    provider: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    reasoning_preference: Option<ReasoningPreference>,
    #[serde(default)]
    backend_reasoning_policy: Option<BackendReasoningPolicyV1>,
    #[serde(default)]
    catalog_revision: Option<String>,
    tools_profile: String,
    sandbox_profile: String,
    timeout_ms: u64,
    heartbeat_timeout_ms: u64,
    max_attempts: u32,
    #[serde(default)]
    token_or_cost_budget: Option<String>,
    #[serde(default)]
    delegation_limit: Option<u32>,
    result_contract: String,
}

impl ChildExecutionPolicyV1Wire {
    fn into_input(self) -> ChildExecutionPolicyV1Input {
        ChildExecutionPolicyV1Input {
            policy_revision: self.policy_revision,
            provider: self.provider,
            model: self.model,
            reasoning_preference: self.reasoning_preference,
            backend_reasoning_policy: self.backend_reasoning_policy,
            catalog_revision: self.catalog_revision,
            tools_profile: self.tools_profile,
            sandbox_profile: self.sandbox_profile,
            timeout_ms: self.timeout_ms,
            heartbeat_timeout_ms: self.heartbeat_timeout_ms,
            max_attempts: self.max_attempts,
            token_or_cost_budget: self.token_or_cost_budget,
            delegation_limit: self.delegation_limit,
            result_contract: self.result_contract,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChildExecutionPolicyError {
    UnsupportedSchema(String),
    ZeroPolicyRevision,
    MissingString(&'static str),
    NonCanonicalString(&'static str),
    ControlCharacter(&'static str),
    StringTooLong {
        field: &'static str,
        max_bytes: usize,
    },
    ProviderModelPairRequired,
    ReasoningPairRequired,
    ReasoningRequiresRoute,
    ReasoningPreference(String),
    BackendPolicy(String),
    ReasoningEffortMismatch {
        preference: String,
        policy: String,
    },
    CatalogRevisionMismatch {
        snapshot: Option<String>,
        policy: Option<String>,
    },
    ReservedExecutionModeEffort(String),
    ExecutionModeSnapshot(String),
    InvalidTimeout {
        field: &'static str,
        value: u64,
    },
    HeartbeatExceedsTimeout {
        heartbeat_timeout_ms: u64,
        timeout_ms: u64,
    },
    InvalidMaxAttempts(u32),
    DelegationLimitTooLarge(u32),
}

impl fmt::Display for ChildExecutionPolicyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedSchema(schema) => {
                write!(formatter, "unsupported child execution policy schema `{schema}`")
            }
            Self::ZeroPolicyRevision => {
                formatter.write_str("child execution policy revision must be greater than zero")
            }
            Self::MissingString(field) => write!(formatter, "{field} must not be empty"),
            Self::NonCanonicalString(field) => {
                write!(formatter, "{field} must not have outer whitespace")
            }
            Self::ControlCharacter(field) => {
                write!(formatter, "{field} must not contain control characters")
            }
            Self::StringTooLong { field, max_bytes } => {
                write!(formatter, "{field} exceeds {max_bytes} bytes")
            }
            Self::ProviderModelPairRequired => {
                formatter.write_str("provider and model must both be present or both be absent")
            }
            Self::ReasoningPairRequired => formatter.write_str(
                "reasoning preference and backend reasoning policy must both be present or both be absent",
            ),
            Self::ReasoningRequiresRoute => {
                formatter.write_str("managed reasoning requires an exact provider/model route")
            }
            Self::ReasoningPreference(error) => {
                write!(formatter, "invalid child reasoning preference: {error}")
            }
            Self::BackendPolicy(error) => {
                write!(formatter, "invalid child backend reasoning policy: {error}")
            }
            Self::ReasoningEffortMismatch { preference, policy } => write!(
                formatter,
                "reasoning preference effort `{preference}` does not match policy effort `{policy}`"
            ),
            Self::CatalogRevisionMismatch { snapshot, policy } => write!(
                formatter,
                "catalog revision snapshot {:?} does not match backend policy revision {:?}",
                snapshot, policy
            ),
            Self::ReservedExecutionModeEffort(effort) => write!(
                formatter,
                "reasoning effort `{effort}` is reserved for execution-mode admission"
            ),
            Self::ExecutionModeSnapshot(error) => {
                write!(formatter, "invalid authorized execution-mode snapshot: {error}")
            }
            Self::InvalidTimeout { field, value } => write!(
                formatter,
                "{field} must be in 1..={CHILD_EXECUTION_POLICY_MAX_TIMEOUT_MS}, got {value}"
            ),
            Self::HeartbeatExceedsTimeout {
                heartbeat_timeout_ms,
                timeout_ms,
            } => write!(
                formatter,
                "heartbeat timeout {heartbeat_timeout_ms} exceeds child timeout {timeout_ms}"
            ),
            Self::InvalidMaxAttempts(value) => write!(
                formatter,
                "maxAttempts must be in 1..={CHILD_EXECUTION_POLICY_MAX_ATTEMPTS}, got {value}"
            ),
            Self::DelegationLimitTooLarge(value) => write!(
                formatter,
                "delegationLimit exceeds {CHILD_EXECUTION_POLICY_MAX_DELEGATION_LIMIT}, got {value}"
            ),
        }
    }
}

impl std::error::Error for ChildExecutionPolicyError {}

fn validate_required_string(
    field: &'static str,
    value: &str,
) -> Result<(), ChildExecutionPolicyError> {
    validate_string(field, value)
}

fn validate_optional_string(
    field: &'static str,
    value: Option<&str>,
) -> Result<(), ChildExecutionPolicyError> {
    match value {
        Some(value) => validate_string(field, value),
        None => Ok(()),
    }
}

fn validate_string(field: &'static str, value: &str) -> Result<(), ChildExecutionPolicyError> {
    if value.is_empty() {
        return Err(ChildExecutionPolicyError::MissingString(field));
    }
    if value != value.trim() {
        return Err(ChildExecutionPolicyError::NonCanonicalString(field));
    }
    if value.chars().any(char::is_control) {
        return Err(ChildExecutionPolicyError::ControlCharacter(field));
    }
    if value.len() > CHILD_EXECUTION_POLICY_MAX_STRING_BYTES {
        return Err(ChildExecutionPolicyError::StringTooLong {
            field,
            max_bytes: CHILD_EXECUTION_POLICY_MAX_STRING_BYTES,
        });
    }
    Ok(())
}

fn validate_timeout(field: &'static str, value: u64) -> Result<(), ChildExecutionPolicyError> {
    if value == 0 || value > CHILD_EXECUTION_POLICY_MAX_TIMEOUT_MS {
        return Err(ChildExecutionPolicyError::InvalidTimeout { field, value });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend_reasoning::BackendReasoningSource;
    use crate::model_catalog::{
        REASONING_RESOLUTION_RECEIPT_SCHEMA_VERSION, ReasoningResolutionReceipt,
        ReasoningResolutionStatus,
    };

    #[test]
    fn v2_keeps_max_reasoning_separate_from_ultra_execution_mode() {
        let child = ChildExecutionPolicyV2::new(
            managed_policy(11, "openai", "gpt-5.6-sol", "max"),
            Some(authorized_ultra_snapshot("main")),
        )
        .unwrap();

        assert_eq!(child.child_policy().effective_effort(), Some("max"));
        assert_eq!(
            child.authorized_execution_mode().unwrap().effective_mode(),
            crate::execution_mode::ULTRA_EXECUTION_MODE
        );
    }

    #[test]
    fn v2_rejects_ultra_from_either_reasoning_field() {
        let legacy_ultra = ChildExecutionPolicyV1::new(ChildExecutionPolicyV1Input {
            policy_revision: 12,
            provider: Some("openai".to_string()),
            model: Some("gpt-5.6-sol".to_string()),
            reasoning_preference: Some(ReasoningPreference::explicit("ultra").unwrap()),
            backend_reasoning_policy: Some(backend_policy("openai", "gpt-5.6-sol", "ultra")),
            catalog_revision: Some("catalog-test".to_string()),
            ..base_input()
        })
        .unwrap_err();
        assert_eq!(
            legacy_ultra,
            ChildExecutionPolicyError::ReservedExecutionModeEffort("ultra".to_string())
        );
    }

    #[test]
    fn heterogeneous_v2_siblings_preserve_independent_execution_modes() {
        let standard = ChildExecutionPolicyV2::new(
            managed_policy(13, "openai", "gpt-5.6-sol", "max"),
            Some(
                crate::execution_mode::AuthorizedExecutionModeSnapshotV2::new(
                    crate::execution_mode::ExecutionModePreference::Default,
                    None,
                    None,
                    None,
                )
                .unwrap(),
            ),
        )
        .unwrap();
        let ultra = ChildExecutionPolicyV2::new(
            managed_policy(14, "openai", "gpt-5.6-terra", "max"),
            Some(authorized_ultra_snapshot("main")),
        )
        .unwrap();

        assert_eq!(standard.child_policy().effective_effort(), Some("max"));
        assert_eq!(ultra.child_policy().effective_effort(), Some("max"));
        assert_eq!(
            standard
                .authorized_execution_mode()
                .unwrap()
                .effective_mode(),
            crate::execution_mode::STANDARD_EXECUTION_MODE
        );
        assert_eq!(
            ultra.authorized_execution_mode().unwrap().effective_mode(),
            crate::execution_mode::ULTRA_EXECUTION_MODE
        );
        assert_ne!(standard, ultra);
    }

    #[test]
    fn v2_serde_roundtrip_preserves_child_and_execution_snapshot() {
        let original = ChildExecutionPolicyV2::new(
            managed_policy(15, "openai", "gpt-5.6-sol", "max"),
            Some(authorized_ultra_snapshot("main")),
        )
        .unwrap();
        let encoded = serde_json::to_vec(&original).unwrap();
        let decoded: ChildExecutionPolicyV2 = serde_json::from_slice(&encoded).unwrap();

        assert_eq!(decoded, original);
        assert_eq!(
            decoded
                .authorized_execution_mode()
                .unwrap()
                .retry_identity()
                .unwrap(),
            original
                .authorized_execution_mode()
                .unwrap()
                .retry_identity()
                .unwrap()
        );
    }

    #[test]
    fn heterogeneous_siblings_preserve_independent_open_ended_routes_and_efforts() {
        let max = managed_policy(1, "openai", "gpt-5.6-sol", "max");
        let high = managed_policy(2, "openai", "gpt-5.6-terra", "high");

        assert_eq!(max.provider(), Some("openai"));
        assert_eq!(max.model(), Some("gpt-5.6-sol"));
        assert_eq!(
            max.reasoning_preference().unwrap().explicit_effort(),
            Some("max")
        );
        assert_eq!(high.model(), Some("gpt-5.6-terra"));
        assert_eq!(
            high.reasoning_preference().unwrap().explicit_effort(),
            Some("high")
        );
        assert_ne!(max, high);
    }

    #[test]
    fn route_and_effort_mismatch_are_rejected() {
        let policy = backend_policy("openai", "gpt-5.6-sol", "max");
        let route_error = ChildExecutionPolicyV1::new(ChildExecutionPolicyV1Input {
            policy_revision: 1,
            provider: Some("openai".to_string()),
            model: Some("gpt-5.6-luna".to_string()),
            reasoning_preference: Some(ReasoningPreference::explicit("max").unwrap()),
            backend_reasoning_policy: Some(policy.clone()),
            catalog_revision: Some("catalog-test".to_string()),
            ..base_input()
        })
        .unwrap_err();
        assert!(matches!(
            route_error,
            ChildExecutionPolicyError::BackendPolicy(_)
        ));

        let effort_error = ChildExecutionPolicyV1::new(ChildExecutionPolicyV1Input {
            policy_revision: 1,
            provider: Some("openai".to_string()),
            model: Some("gpt-5.6-sol".to_string()),
            reasoning_preference: Some(ReasoningPreference::explicit("ultra").unwrap()),
            backend_reasoning_policy: Some(policy),
            catalog_revision: Some("catalog-test".to_string()),
            ..base_input()
        })
        .unwrap_err();
        assert_eq!(
            effort_error,
            ChildExecutionPolicyError::ReservedExecutionModeEffort("ultra".to_string())
        );
    }

    #[test]
    fn retry_clone_and_serde_roundtrip_preserve_exact_snapshot() {
        let original = managed_policy(7, "openai", "gpt-5.6-sol", "max");
        let retry = original.clone();
        let encoded = serde_json::to_vec(&retry).unwrap();
        let decoded: ChildExecutionPolicyV1 = serde_json::from_slice(&encoded).unwrap();

        assert_eq!(retry, original);
        assert_eq!(decoded, original);
        assert_eq!(decoded.policy_revision(), 7);
        assert_eq!(decoded.effective_effort(), Some("max"));
    }

    #[test]
    fn old_unmanaged_policy_defaults_optional_route_and_reasoning_fields() {
        let policy: ChildExecutionPolicyV1 = serde_json::from_value(serde_json::json!({
            "schema": CHILD_EXECUTION_POLICY_SCHEMA,
            "policyRevision": 1,
            "toolsProfile": "default",
            "sandboxProfile": "workspace-write",
            "timeoutMs": 300000,
            "heartbeatTimeoutMs": 60000,
            "maxAttempts": 3,
            "resultContract": "child-result-envelope-v1"
        }))
        .unwrap();

        assert!(!policy.is_managed_route());
        assert_eq!(policy.provider(), None);
        assert_eq!(policy.reasoning_preference(), None);
        assert_eq!(policy.backend_reasoning_policy(), None);
    }

    fn managed_policy(
        policy_revision: u64,
        provider: &str,
        model: &str,
        effort: &str,
    ) -> ChildExecutionPolicyV1 {
        ChildExecutionPolicyV1::new(ChildExecutionPolicyV1Input {
            policy_revision,
            provider: Some(provider.to_string()),
            model: Some(model.to_string()),
            reasoning_preference: Some(ReasoningPreference::explicit(effort).unwrap()),
            backend_reasoning_policy: Some(backend_policy(provider, model, effort)),
            catalog_revision: Some("catalog-test".to_string()),
            ..base_input()
        })
        .unwrap()
    }

    fn backend_policy(provider: &str, model: &str, effort: &str) -> BackendReasoningPolicyV1 {
        BackendReasoningPolicyV1::new(
            BackendReasoningSource::ChildAdmission,
            ReasoningResolutionReceipt {
                schema_version: REASONING_RESOLUTION_RECEIPT_SCHEMA_VERSION,
                requested_provider: provider.to_string(),
                requested_model: model.to_string(),
                effective_provider: Some(provider.to_string()),
                effective_model: Some(model.to_string()),
                requested_effort: effort.to_string(),
                effective_effort: Some(effort.to_string()),
                catalog_effective_effort: Some(effort.to_string()),
                catalog_revision: Some("catalog-test".to_string()),
                status: ReasoningResolutionStatus::Accepted,
                authoritative: true,
                reason: "test child admission".to_string(),
            },
        )
        .unwrap()
    }

    fn base_input() -> ChildExecutionPolicyV1Input {
        ChildExecutionPolicyV1Input {
            policy_revision: 1,
            provider: None,
            model: None,
            reasoning_preference: None,
            backend_reasoning_policy: None,
            catalog_revision: None,
            tools_profile: "default".to_string(),
            sandbox_profile: "workspace-write".to_string(),
            timeout_ms: 300_000,
            heartbeat_timeout_ms: 60_000,
            max_attempts: 3,
            token_or_cost_budget: None,
            delegation_limit: None,
            result_contract: "child-result-envelope-v1".to_string(),
        }
    }

    fn authorized_ultra_snapshot(
        agent_id: &str,
    ) -> crate::execution_mode::AuthorizedExecutionModeSnapshotV2 {
        use crate::execution_mode::{
            ExecutionModePolicyV1, ExecutionModePreference, ExecutionModeSource,
            SafeResumeReadinessReceiptV1, ULTRA_EXECUTION_MODE,
        };
        use crate::worker_result_mailbox::WorkerResultOwnerV1;

        const DIGEST: &str =
            "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let preference = ExecutionModePreference::explicit(ULTRA_EXECUTION_MODE).unwrap();
        let policy = ExecutionModePolicyV1::new(
            ExecutionModeSource::ChildAdmission,
            &preference,
            ULTRA_EXECUTION_MODE,
            agent_id,
            "authorization-v1",
            DIGEST,
            2,
            6,
            300_000,
        )
        .unwrap();
        let lane = crate::lane::FullLaneKeyV1::new(
            "discord",
            "primary",
            "channel-1",
            "user-1",
            agent_id,
            "subagent",
            "root-session",
            "concrete-session",
        )
        .unwrap();
        let owner = crate::worker_result_mailbox::ExactWorkerResultOwnerV1::new(
            lane,
            "virtual-session-1",
            None,
            Some("parent-queue-1".to_string()),
            "source-queue-1",
            None,
            None,
        )
        .unwrap();
        let readiness =
            SafeResumeReadinessReceiptV1::new(&owner, "durability-r1", true, true, true, true)
                .unwrap();
        crate::execution_mode::AuthorizedExecutionModeSnapshotV2::new(
            preference,
            Some(policy),
            Some(WorkerResultOwnerV1::Exact(owner)),
            Some(readiness),
        )
        .unwrap()
    }
}
