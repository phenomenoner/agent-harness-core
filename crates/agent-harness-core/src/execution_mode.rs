use std::fmt;
use std::path::Path;

use ring::digest;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize};

use crate::backend_reasoning::ReasoningPreference;
use crate::worker_coordination::WORKER_COORDINATOR_WAIT_SCHEMA;
use crate::worker_result_mailbox::{
    ExactWorkerResultOwnerV1, WORKER_RESULT_MAILBOX_SCHEMA, WorkerResultOwnerV1,
};
use crate::worker_resume::WORKER_RESUME_INTENT_SCHEMA;

pub const EXECUTION_MODE_POLICY_SCHEMA_VERSION: u32 = 1;
pub const AUTHORIZED_EXECUTION_MODE_SNAPSHOT_SCHEMA_VERSION: u32 = 2;
pub const SAFE_RESUME_READINESS_RECEIPT_SCHEMA: &str = "agent-harness.safe-resume-readiness.v1";
pub const SAFE_RESUME_LEASE_CONTRACT: &str = "agent-harness.worker-resume-lease.v1";
pub const STANDARD_EXECUTION_MODE: &str = "standard";
pub const ULTRA_EXECUTION_MODE: &str = "ultra";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionModeAuthorizationAssessmentV1 {
    pub authorized: bool,
    pub policy: Option<ExecutionModePolicyV1>,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum ExecutionModePreference {
    Default,
    Explicit { mode: String },
    LegacyQuarantined { mode: String },
}

impl ExecutionModePreference {
    pub fn explicit(mode: impl Into<String>) -> Result<Self, ExecutionModeError> {
        let mode = mode.into();
        validate_mode("mode", &mode)?;
        Ok(Self::Explicit { mode })
    }

    pub fn legacy_quarantined(mode: impl Into<String>) -> Result<Self, ExecutionModeError> {
        let mode = mode.into();
        validate_mode("mode", &mode)?;
        Ok(Self::LegacyQuarantined { mode })
    }

    pub fn requested_mode(&self) -> &str {
        match self {
            Self::Default => STANDARD_EXECUTION_MODE,
            Self::Explicit { mode } | Self::LegacyQuarantined { mode } => mode,
        }
    }

    pub fn is_authorizable(&self) -> bool {
        !matches!(self, Self::LegacyQuarantined { .. })
    }

    pub fn validate(&self) -> Result<(), ExecutionModeError> {
        match self {
            Self::Default => Ok(()),
            Self::Explicit { mode } | Self::LegacyQuarantined { mode } => {
                validate_mode("mode", mode)
            }
        }
    }

    pub fn quarantine_legacy_ultra_reasoning(preference: &ReasoningPreference) -> Option<Self> {
        match preference {
            ReasoningPreference::Explicit { effort }
                if effort.eq_ignore_ascii_case(ULTRA_EXECUTION_MODE) =>
            {
                Some(Self::LegacyQuarantined {
                    mode: ULTRA_EXECUTION_MODE.to_string(),
                })
            }
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ExecutionModeSource {
    ChannelCommand,
    AgentOverride,
    ChildAdmission,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExecutionModePolicyV1 {
    pub schema_version: u32,
    pub source: ExecutionModeSource,
    pub requested_mode: String,
    pub effective_mode: String,
    pub agent_id: String,
    pub authorization_revision: String,
    pub resource_policy_digest: String,
    pub max_parallel_children: u32,
    pub max_total_children: u32,
    pub child_timeout_ms: u64,
}

impl ExecutionModePolicyV1 {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        source: ExecutionModeSource,
        preference: &ExecutionModePreference,
        effective_mode: impl Into<String>,
        agent_id: impl Into<String>,
        authorization_revision: impl Into<String>,
        resource_policy_digest: impl Into<String>,
        max_parallel_children: u32,
        max_total_children: u32,
        child_timeout_ms: u64,
    ) -> Result<Self, ExecutionModeError> {
        preference.validate()?;
        if !preference.is_authorizable() {
            return Err(ExecutionModeError::LegacyPreferenceRequiresReissue);
        }
        let policy = Self {
            schema_version: EXECUTION_MODE_POLICY_SCHEMA_VERSION,
            source,
            requested_mode: preference.requested_mode().to_string(),
            effective_mode: effective_mode.into(),
            agent_id: agent_id.into(),
            authorization_revision: authorization_revision.into(),
            resource_policy_digest: resource_policy_digest.into(),
            max_parallel_children,
            max_total_children,
            child_timeout_ms,
        };
        policy.validate()?;
        Ok(policy)
    }

    pub fn validate(&self) -> Result<(), ExecutionModeError> {
        if self.schema_version != EXECUTION_MODE_POLICY_SCHEMA_VERSION {
            return Err(ExecutionModeError::UnsupportedSchemaVersion(
                self.schema_version,
            ));
        }
        validate_mode("requestedMode", &self.requested_mode)?;
        validate_mode("effectiveMode", &self.effective_mode)?;
        if self.requested_mode != self.effective_mode {
            return Err(ExecutionModeError::ModeMismatch);
        }
        validate_identifier("agentId", &self.agent_id, 256)?;
        validate_identifier("authorizationRevision", &self.authorization_revision, 256)?;
        validate_sha256(&self.resource_policy_digest)?;
        if self.effective_mode == ULTRA_EXECUTION_MODE
            && (self.max_parallel_children == 0
                || self.max_total_children == 0
                || self.max_parallel_children > self.max_total_children
                || self.child_timeout_ms == 0)
        {
            return Err(ExecutionModeError::InvalidResourcePolicy);
        }
        Ok(())
    }
}

/// Durable evidence captured at admission time that an exact result owner can
/// be resumed through the mailbox/coordinator/lease pipeline. The booleans are
/// deliberately explicit so an unready observation can be retained for audit;
/// Ultra admission accepts only a fully ready receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SafeResumeReadinessReceiptV1 {
    schema: String,
    owner_key: String,
    coordinator_key: String,
    durability_revision: String,
    mailbox_contract: String,
    coordinator_contract: String,
    resume_intent_contract: String,
    lease_contract: String,
    durable_store_ready: bool,
    mailbox_ready: bool,
    coordinator_ready: bool,
    lease_ready: bool,
}

impl SafeResumeReadinessReceiptV1 {
    pub fn new(
        owner: &ExactWorkerResultOwnerV1,
        durability_revision: impl Into<String>,
        durable_store_ready: bool,
        mailbox_ready: bool,
        coordinator_ready: bool,
        lease_ready: bool,
    ) -> Result<Self, ExecutionModeError> {
        owner
            .validate()
            .map_err(|error| ExecutionModeError::InvalidResultOwner(error.to_string()))?;
        let receipt = Self {
            schema: SAFE_RESUME_READINESS_RECEIPT_SCHEMA.to_string(),
            owner_key: owner
                .owner_key()
                .map_err(|error| ExecutionModeError::InvalidResultOwner(error.to_string()))?,
            coordinator_key: owner
                .coordinator_key()
                .map_err(|error| ExecutionModeError::InvalidResultOwner(error.to_string()))?,
            durability_revision: durability_revision.into(),
            mailbox_contract: WORKER_RESULT_MAILBOX_SCHEMA.to_string(),
            coordinator_contract: WORKER_COORDINATOR_WAIT_SCHEMA.to_string(),
            resume_intent_contract: WORKER_RESUME_INTENT_SCHEMA.to_string(),
            lease_contract: SAFE_RESUME_LEASE_CONTRACT.to_string(),
            durable_store_ready,
            mailbox_ready,
            coordinator_ready,
            lease_ready,
        };
        receipt.validate_for_owner(owner)?;
        Ok(receipt)
    }

    pub fn validate_for_owner(
        &self,
        owner: &ExactWorkerResultOwnerV1,
    ) -> Result<(), ExecutionModeError> {
        if self.schema != SAFE_RESUME_READINESS_RECEIPT_SCHEMA {
            return Err(ExecutionModeError::UnsupportedSafeResumeReadinessSchema(
                self.schema.clone(),
            ));
        }
        validate_identifier("durabilityRevision", &self.durability_revision, 256)?;
        if self.mailbox_contract != WORKER_RESULT_MAILBOX_SCHEMA {
            return Err(ExecutionModeError::InvalidSafeResumeContract(
                "mailboxContract",
            ));
        }
        if self.coordinator_contract != WORKER_COORDINATOR_WAIT_SCHEMA {
            return Err(ExecutionModeError::InvalidSafeResumeContract(
                "coordinatorContract",
            ));
        }
        if self.resume_intent_contract != WORKER_RESUME_INTENT_SCHEMA {
            return Err(ExecutionModeError::InvalidSafeResumeContract(
                "resumeIntentContract",
            ));
        }
        if self.lease_contract != SAFE_RESUME_LEASE_CONTRACT {
            return Err(ExecutionModeError::InvalidSafeResumeContract(
                "leaseContract",
            ));
        }
        owner
            .validate()
            .map_err(|error| ExecutionModeError::InvalidResultOwner(error.to_string()))?;
        let owner_key = owner
            .owner_key()
            .map_err(|error| ExecutionModeError::InvalidResultOwner(error.to_string()))?;
        let coordinator_key = owner
            .coordinator_key()
            .map_err(|error| ExecutionModeError::InvalidResultOwner(error.to_string()))?;
        if self.owner_key != owner_key || self.coordinator_key != coordinator_key {
            return Err(ExecutionModeError::SafeResumeOwnerMismatch);
        }
        Ok(())
    }

    pub const fn is_ready(&self) -> bool {
        self.durable_store_ready && self.mailbox_ready && self.coordinator_ready && self.lease_ready
    }

    pub fn owner_key(&self) -> &str {
        &self.owner_key
    }

    pub fn coordinator_key(&self) -> &str {
        &self.coordinator_key
    }
}

/// Immutable execution-mode authorization captured before a runtime/worker
/// enqueue. V1 policy remains unchanged; V2 binds it to exact ownership and
/// durable safe-resume readiness without conflating execution mode with model
/// reasoning effort.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthorizedExecutionModeSnapshotV2 {
    schema_version: u32,
    preference: ExecutionModePreference,
    #[serde(skip_serializing_if = "Option::is_none")]
    policy: Option<ExecutionModePolicyV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    result_owner: Option<ExactWorkerResultOwnerV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    safe_resume_readiness: Option<SafeResumeReadinessReceiptV1>,
}

impl AuthorizedExecutionModeSnapshotV2 {
    pub fn new(
        preference: ExecutionModePreference,
        policy: Option<ExecutionModePolicyV1>,
        result_owner: Option<WorkerResultOwnerV1>,
        safe_resume_readiness: Option<SafeResumeReadinessReceiptV1>,
    ) -> Result<Self, ExecutionModeError> {
        let result_owner = match result_owner {
            Some(WorkerResultOwnerV1::Exact(owner)) => Some(owner),
            Some(WorkerResultOwnerV1::LegacyIncomplete(_)) => {
                return Err(ExecutionModeError::LegacyResultOwnerDenied);
            }
            None => None,
        };
        let snapshot = Self {
            schema_version: AUTHORIZED_EXECUTION_MODE_SNAPSHOT_SCHEMA_VERSION,
            preference,
            policy,
            result_owner,
            safe_resume_readiness,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }

    pub fn validate(&self) -> Result<(), ExecutionModeError> {
        if self.schema_version != AUTHORIZED_EXECUTION_MODE_SNAPSHOT_SCHEMA_VERSION {
            return Err(ExecutionModeError::UnsupportedSnapshotSchemaVersion(
                self.schema_version,
            ));
        }
        self.preference.validate()?;
        if !self.preference.is_authorizable() {
            return Err(ExecutionModeError::LegacyPreferenceRequiresReissue);
        }

        let requested_mode = self.preference.requested_mode();
        if let Some(policy) = &self.policy {
            policy.validate()?;
            if policy.requested_mode != requested_mode || policy.effective_mode != requested_mode {
                return Err(ExecutionModeError::SnapshotPolicyModeMismatch);
            }
        } else if requested_mode != STANDARD_EXECUTION_MODE {
            return Err(ExecutionModeError::ExecutionModePolicyRequired);
        }

        if let Some(owner) = &self.result_owner {
            owner
                .validate()
                .map_err(|error| ExecutionModeError::InvalidResultOwner(error.to_string()))?;
        }
        if let Some(readiness) = &self.safe_resume_readiness {
            let owner = self
                .result_owner
                .as_ref()
                .ok_or(ExecutionModeError::ExactResultOwnerRequired)?;
            readiness.validate_for_owner(owner)?;
        }

        if requested_mode == ULTRA_EXECUTION_MODE {
            let _policy = self
                .policy
                .as_ref()
                .ok_or(ExecutionModeError::ExecutionModePolicyRequired)?;
            let _owner = self
                .result_owner
                .as_ref()
                .ok_or(ExecutionModeError::ExactResultOwnerRequired)?;
            let readiness = self
                .safe_resume_readiness
                .as_ref()
                .ok_or(ExecutionModeError::SafeResumeReadinessRequired)?;
            if !readiness.is_ready() {
                return Err(ExecutionModeError::SafeResumeNotReady);
            }
        }
        Ok(())
    }

    pub fn retry_identity(&self) -> Result<String, ExecutionModeError> {
        self.validate()?;
        let encoded = serde_json::to_vec(self)
            .map_err(|error| ExecutionModeError::SnapshotSerialization(error.to_string()))?;
        let mut context = digest::Context::new(&digest::SHA256);
        context.update(b"agent-harness/authorized-execution-mode-snapshot/v2\0");
        context.update(&encoded);
        Ok(format!(
            "sha256:{}",
            context
                .finish()
                .as_ref()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        ))
    }

    pub fn effective_mode(&self) -> &str {
        self.policy.as_ref().map_or_else(
            || self.preference.requested_mode(),
            |policy| policy.effective_mode.as_str(),
        )
    }

    pub const fn preference(&self) -> &ExecutionModePreference {
        &self.preference
    }

    pub const fn policy(&self) -> Option<&ExecutionModePolicyV1> {
        self.policy.as_ref()
    }

    pub const fn result_owner(&self) -> Option<&ExactWorkerResultOwnerV1> {
        self.result_owner.as_ref()
    }

    /// The agent authorized to execute this snapshot. This is deliberately
    /// independent from the result owner's coordinator lane: child work may
    /// execute as `researcher` while its terminal result is owned by `main`.
    pub fn execution_agent_id(&self) -> Option<&str> {
        self.policy.as_ref().map(|policy| policy.agent_id.as_str())
    }

    pub const fn safe_resume_readiness(&self) -> Option<&SafeResumeReadinessReceiptV1> {
        self.safe_resume_readiness.as_ref()
    }
}

impl<'de> Deserialize<'de> for AuthorizedExecutionModeSnapshotV2 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        struct Wire {
            schema_version: u32,
            preference: ExecutionModePreference,
            #[serde(default)]
            policy: Option<ExecutionModePolicyV1>,
            #[serde(default)]
            result_owner: Option<ExactWorkerResultOwnerV1>,
            #[serde(default)]
            safe_resume_readiness: Option<SafeResumeReadinessReceiptV1>,
        }

        let wire = Wire::deserialize(deserializer)?;
        let snapshot = Self {
            schema_version: wire.schema_version,
            preference: wire.preference,
            policy: wire.policy,
            result_owner: wire.result_owner,
            safe_resume_readiness: wire.safe_resume_readiness,
        };
        snapshot.validate().map_err(D::Error::custom)?;
        Ok(snapshot)
    }
}

pub fn is_reserved_execution_mode_effort(value: &str) -> bool {
    value.eq_ignore_ascii_case(ULTRA_EXECUTION_MODE)
}

pub fn authorize_execution_mode_for_agent(
    _harness_home: Option<&Path>,
    _agent_id: Option<&str>,
    _source: ExecutionModeSource,
    preference: &ExecutionModePreference,
) -> ExecutionModeAuthorizationAssessmentV1 {
    if !preference.is_authorizable() {
        return denied("legacy Ultra reasoning is quarantined and must be explicitly reissued");
    }
    if preference.requested_mode() == STANDARD_EXECUTION_MODE {
        return ExecutionModeAuthorizationAssessmentV1 {
            authorized: true,
            policy: None,
            reason: "standard execution mode is active".to_string(),
        };
    }
    denied(format!(
        "execution mode `{}` is not supported in this release; reasoning effort tops out at `max`",
        preference.requested_mode()
    ))
}

fn denied(reason: impl Into<String>) -> ExecutionModeAuthorizationAssessmentV1 {
    ExecutionModeAuthorizationAssessmentV1 {
        authorized: false,
        policy: None,
        reason: reason.into(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionModeError {
    UnsupportedSchemaVersion(u32),
    UnsupportedSnapshotSchemaVersion(u32),
    UnsupportedSafeResumeReadinessSchema(String),
    MissingField(&'static str),
    NonCanonicalField(&'static str),
    FieldTooLong(&'static str),
    LegacyPreferenceRequiresReissue,
    LegacyResultOwnerDenied,
    ModeMismatch,
    SnapshotPolicyModeMismatch,
    ExecutionModePolicyRequired,
    ExactResultOwnerRequired,
    SafeResumeReadinessRequired,
    SafeResumeNotReady,
    SafeResumeOwnerMismatch,
    InvalidSafeResumeContract(&'static str),
    InvalidResultOwner(String),
    ResultOwnerAgentMismatch {
        policy_agent_id: String,
        owner_agent_id: String,
    },
    SnapshotSerialization(String),
    InvalidResourcePolicyDigest,
    InvalidResourcePolicy,
}

impl fmt::Display for ExecutionModeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedSchemaVersion(version) => {
                write!(
                    formatter,
                    "unsupported execution mode policy schema {version}"
                )
            }
            Self::UnsupportedSnapshotSchemaVersion(version) => write!(
                formatter,
                "unsupported authorized execution-mode snapshot schema {version}"
            ),
            Self::UnsupportedSafeResumeReadinessSchema(schema) => write!(
                formatter,
                "unsupported safe-resume readiness schema `{schema}`"
            ),
            Self::MissingField(field) => write!(formatter, "{field} is required"),
            Self::NonCanonicalField(field) => write!(formatter, "{field} is not canonical"),
            Self::FieldTooLong(field) => write!(formatter, "{field} exceeds its size limit"),
            Self::LegacyPreferenceRequiresReissue => formatter.write_str(
                "legacy Ultra reasoning is quarantined and must be reissued as an execution mode",
            ),
            Self::LegacyResultOwnerDenied => formatter.write_str(
                "legacy incomplete result ownership cannot authorize an execution mode",
            ),
            Self::ModeMismatch => {
                formatter.write_str("requested and effective execution modes differ")
            }
            Self::SnapshotPolicyModeMismatch => formatter.write_str(
                "execution-mode preference and authorized policy snapshot differ",
            ),
            Self::ExecutionModePolicyRequired => {
                formatter.write_str("non-standard execution mode requires an authorized policy")
            }
            Self::ExactResultOwnerRequired => {
                formatter.write_str("Ultra execution mode requires an exact result owner")
            }
            Self::SafeResumeReadinessRequired => formatter.write_str(
                "Ultra execution mode requires a durable safe-resume readiness receipt",
            ),
            Self::SafeResumeNotReady => formatter.write_str(
                "Ultra execution mode requires ready durable mailbox, coordinator, and lease contracts",
            ),
            Self::SafeResumeOwnerMismatch => formatter.write_str(
                "safe-resume readiness receipt does not match the exact result owner",
            ),
            Self::InvalidSafeResumeContract(field) => {
                write!(formatter, "safe-resume {field} is not the required contract")
            }
            Self::InvalidResultOwner(error) => {
                write!(formatter, "invalid exact result owner: {error}")
            }
            Self::ResultOwnerAgentMismatch {
                policy_agent_id,
                owner_agent_id,
            } => write!(
                formatter,
                "execution-mode policy agent `{policy_agent_id}` does not match result owner agent `{owner_agent_id}`"
            ),
            Self::SnapshotSerialization(error) => {
                write!(formatter, "execution-mode snapshot serialization failed: {error}")
            }
            Self::InvalidResourcePolicyDigest => {
                formatter.write_str("resourcePolicyDigest must be a lowercase sha256 digest")
            }
            Self::InvalidResourcePolicy => {
                formatter.write_str("Ultra resource policy limits are invalid")
            }
        }
    }
}

impl std::error::Error for ExecutionModeError {}

fn validate_mode(field: &'static str, value: &str) -> Result<(), ExecutionModeError> {
    validate_identifier(field, value, 64)?;
    if value != value.to_ascii_lowercase()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        return Err(ExecutionModeError::NonCanonicalField(field));
    }
    Ok(())
}

fn validate_identifier(
    field: &'static str,
    value: &str,
    max_bytes: usize,
) -> Result<(), ExecutionModeError> {
    if value.is_empty() {
        return Err(ExecutionModeError::MissingField(field));
    }
    if value != value.trim() || value.chars().any(char::is_control) {
        return Err(ExecutionModeError::NonCanonicalField(field));
    }
    if value.len() > max_bytes {
        return Err(ExecutionModeError::FieldTooLong(field));
    }
    Ok(())
}

fn validate_sha256(value: &str) -> Result<(), ExecutionModeError> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(ExecutionModeError::InvalidResourcePolicyDigest);
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ExecutionModeError::InvalidResourcePolicyDigest);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    const DIGEST: &str = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    #[test]
    fn ultra_snapshot_rejects_missing_legacy_and_unready_resume_ownership() {
        let preference = ExecutionModePreference::explicit(ULTRA_EXECUTION_MODE).unwrap();
        let policy = ultra_policy("main");

        assert_eq!(
            AuthorizedExecutionModeSnapshotV2::new(
                preference.clone(),
                Some(policy.clone()),
                None,
                None,
            )
            .unwrap_err(),
            ExecutionModeError::ExactResultOwnerRequired
        );

        let legacy = crate::worker_result_mailbox::LegacyIncompleteWorkerResultOwnerV0 {
            schema: crate::worker_result_mailbox::LEGACY_WORKER_RESULT_OWNER_SCHEMA.to_string(),
            legacy_owner_ref: "legacy-owner".to_string(),
            lane: None,
            virtual_session_id: None,
            parent_worker_job_id: None,
            parent_queue_id: None,
            source_queue_id: None,
            operation_plan_id: None,
            operation_plan_item_id: None,
            missing_identity_axes: vec![
                crate::worker_result_mailbox::LegacyOwnerMissingAxisV1::AgentId,
            ],
        };
        assert_eq!(
            AuthorizedExecutionModeSnapshotV2::new(
                preference.clone(),
                Some(policy.clone()),
                Some(crate::worker_result_mailbox::WorkerResultOwnerV1::LegacyIncomplete(legacy,)),
                None,
            )
            .unwrap_err(),
            ExecutionModeError::LegacyResultOwnerDenied
        );

        let owner = exact_owner("main");
        let unready =
            SafeResumeReadinessReceiptV1::new(&owner, "durability-r1", true, true, true, false)
                .unwrap();
        assert_eq!(
            AuthorizedExecutionModeSnapshotV2::new(
                preference,
                Some(policy),
                Some(crate::worker_result_mailbox::WorkerResultOwnerV1::Exact(
                    owner,
                )),
                Some(unready),
            )
            .unwrap_err(),
            ExecutionModeError::SafeResumeNotReady
        );
    }

    #[test]
    fn ultra_snapshot_separates_execution_agent_from_master_result_owner() {
        let owner = exact_owner("worker-agent");
        let readiness = ready_receipt(&owner);
        let snapshot = AuthorizedExecutionModeSnapshotV2::new(
            ExecutionModePreference::explicit(ULTRA_EXECUTION_MODE).unwrap(),
            Some(ultra_policy("main")),
            Some(crate::worker_result_mailbox::WorkerResultOwnerV1::Exact(
                owner,
            )),
            Some(readiness),
        )
        .unwrap();

        assert_eq!(snapshot.execution_agent_id(), Some("main"));
        assert_eq!(
            snapshot.result_owner().unwrap().lane.agent_id(),
            "worker-agent"
        );
    }

    #[test]
    fn authorized_snapshot_serde_roundtrip_preserves_retry_identity() {
        let owner = exact_owner("main");
        let snapshot = AuthorizedExecutionModeSnapshotV2::new(
            ExecutionModePreference::explicit(ULTRA_EXECUTION_MODE).unwrap(),
            Some(ultra_policy("main")),
            Some(crate::worker_result_mailbox::WorkerResultOwnerV1::Exact(
                owner.clone(),
            )),
            Some(ready_receipt(&owner)),
        )
        .unwrap();
        let identity = snapshot.retry_identity().unwrap();
        let encoded = serde_json::to_vec(&snapshot).unwrap();
        let decoded: AuthorizedExecutionModeSnapshotV2 = serde_json::from_slice(&encoded).unwrap();

        assert_eq!(decoded, snapshot);
        assert_eq!(decoded.retry_identity().unwrap(), identity);
        assert_eq!(snapshot.clone().retry_identity().unwrap(), identity);
    }

    #[test]
    fn standard_snapshot_remains_compatible_without_resume_ownership() {
        let snapshot = AuthorizedExecutionModeSnapshotV2::new(
            ExecutionModePreference::Default,
            None,
            None,
            None,
        )
        .unwrap();

        assert_eq!(snapshot.effective_mode(), STANDARD_EXECUTION_MODE);
        assert!(snapshot.policy().is_none());
        assert!(snapshot.result_owner().is_none());
    }

    #[test]
    fn max_is_not_an_execution_mode_effort_and_ultra_is_reserved() {
        assert!(!is_reserved_execution_mode_effort("max"));
        assert!(is_reserved_execution_mode_effort("ultra"));
        assert!(is_reserved_execution_mode_effort("ULTRA"));
    }

    #[test]
    fn legacy_ultra_reasoning_is_quarantined_and_never_auto_authorized() {
        let legacy = ReasoningPreference::explicit("ultra").unwrap();
        let preference =
            ExecutionModePreference::quarantine_legacy_ultra_reasoning(&legacy).unwrap();
        assert_eq!(preference.requested_mode(), ULTRA_EXECUTION_MODE);
        assert!(!preference.is_authorizable());
        assert_eq!(
            ExecutionModePolicyV1::new(
                ExecutionModeSource::ChannelCommand,
                &preference,
                ULTRA_EXECUTION_MODE,
                "main",
                "auth-v1",
                DIGEST,
                2,
                4,
                60_000,
            )
            .unwrap_err(),
            ExecutionModeError::LegacyPreferenceRequiresReissue
        );
    }

    #[test]
    fn explicit_ultra_requires_a_bounded_resource_receipt() {
        let preference = ExecutionModePreference::explicit(ULTRA_EXECUTION_MODE).unwrap();
        let policy = ExecutionModePolicyV1::new(
            ExecutionModeSource::ChannelCommand,
            &preference,
            ULTRA_EXECUTION_MODE,
            "main56sol",
            "authorization-v1",
            DIGEST,
            2,
            6,
            300_000,
        )
        .unwrap();
        assert_eq!(policy.effective_mode, ULTRA_EXECUTION_MODE);

        assert_eq!(
            ExecutionModePolicyV1::new(
                ExecutionModeSource::ChannelCommand,
                &preference,
                ULTRA_EXECUTION_MODE,
                "main56sol",
                "authorization-v1",
                DIGEST,
                7,
                6,
                300_000,
            )
            .unwrap_err(),
            ExecutionModeError::InvalidResourcePolicy
        );
    }

    #[test]
    fn future_modes_remain_open_ended_but_canonical() {
        let preference = ExecutionModePreference::explicit("future-mode").unwrap();
        assert_eq!(preference.requested_mode(), "future-mode");
        assert!(ExecutionModePreference::explicit("Future_Mode").is_err());
    }

    #[test]
    fn ultra_authorization_stays_disabled_for_max_only_release() {
        let root = temp_root("ultra_authorization");
        fs::create_dir_all(&root).unwrap();
        let preference = ExecutionModePreference::explicit("ultra").unwrap();
        assert!(
            !authorize_execution_mode_for_agent(
                Some(&root),
                Some("main"),
                ExecutionModeSource::ChannelCommand,
                &preference,
            )
            .authorized
        );
        fs::write(
            root.join("harness-config.json"),
            r#"{"orchestration":{"features":{"executionModeV1":{"mode":"authoritative","enabledAgentIds":["main"],"authorizationRevision":"auth-v1","ultra":{"maxParallelChildren":2,"maxTotalChildren":6,"childTimeoutMs":300000}}}}}"#,
        )
        .unwrap();
        assert!(
            !authorize_execution_mode_for_agent(
                Some(&root),
                Some("other"),
                ExecutionModeSource::ChannelCommand,
                &preference,
            )
            .authorized
        );
        let denied = authorize_execution_mode_for_agent(
            Some(&root),
            Some("main"),
            ExecutionModeSource::ChannelCommand,
            &preference,
        );
        assert!(!denied.authorized);
        assert!(denied.policy.is_none());
        assert!(denied.reason.contains("not supported in this release"));
        let _ = fs::remove_dir_all(root);
    }

    fn temp_root(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("agent-harness-{label}-{nonce}"))
    }

    fn ultra_policy(agent_id: &str) -> ExecutionModePolicyV1 {
        let preference = ExecutionModePreference::explicit(ULTRA_EXECUTION_MODE).unwrap();
        ExecutionModePolicyV1::new(
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
        .unwrap()
    }

    fn exact_owner(agent_id: &str) -> crate::worker_result_mailbox::ExactWorkerResultOwnerV1 {
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
        crate::worker_result_mailbox::ExactWorkerResultOwnerV1::new(
            lane,
            "virtual-session-1",
            None,
            Some("parent-queue-1".to_string()),
            "source-queue-1",
            None,
            None,
        )
        .unwrap()
    }

    fn ready_receipt(
        owner: &crate::worker_result_mailbox::ExactWorkerResultOwnerV1,
    ) -> SafeResumeReadinessReceiptV1 {
        SafeResumeReadinessReceiptV1::new(owner, "durability-r1", true, true, true, true).unwrap()
    }
}
