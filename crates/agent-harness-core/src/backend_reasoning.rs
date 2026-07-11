use std::fmt;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize};

use crate::model_catalog::{
    REASONING_RESOLUTION_RECEIPT_SCHEMA_VERSION, ReasoningResolutionReceipt,
    ReasoningResolutionStatus,
};

pub const BACKEND_REASONING_POLICY_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum BackendReasoningSource {
    ChannelCommand,
    ChildAdmission,
    AgentDefault,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BackendReasoningPolicyV1 {
    schema_version: u32,
    source: BackendReasoningSource,
    resolution: ReasoningResolutionReceipt,
}

impl BackendReasoningPolicyV1 {
    pub fn new(
        source: BackendReasoningSource,
        resolution: ReasoningResolutionReceipt,
    ) -> Result<Self, BackendReasoningPolicyError> {
        if resolution.schema_version != REASONING_RESOLUTION_RECEIPT_SCHEMA_VERSION {
            return Err(
                BackendReasoningPolicyError::UnsupportedResolutionSchemaVersion(
                    resolution.schema_version,
                ),
            );
        }

        match resolution.status {
            ReasoningResolutionStatus::Legacy
            | ReasoningResolutionStatus::Shadow
            | ReasoningResolutionStatus::Accepted
            | ReasoningResolutionStatus::Fallback => {}
            ReasoningResolutionStatus::Rejected => {
                return Err(BackendReasoningPolicyError::RejectedResolution);
            }
        }

        let Some(effective_effort) = resolution.effective_effort.as_deref() else {
            return Err(BackendReasoningPolicyError::MissingEffectiveEffort);
        };
        if effective_effort.trim().is_empty() {
            return Err(BackendReasoningPolicyError::MissingEffectiveEffort);
        }
        if effective_effort != effective_effort.trim() {
            return Err(BackendReasoningPolicyError::NonCanonicalEffectiveEffort);
        }
        for (field, value) in [
            (
                "effectiveProvider",
                resolution.effective_provider.as_deref(),
            ),
            ("effectiveModel", resolution.effective_model.as_deref()),
        ] {
            let Some(value) = value else {
                return Err(BackendReasoningPolicyError::MissingEffectiveRoute(field));
            };
            if value.trim().is_empty() {
                return Err(BackendReasoningPolicyError::MissingEffectiveRoute(field));
            }
            if value != value.trim() {
                return Err(BackendReasoningPolicyError::NonCanonicalEffectiveRoute(
                    field,
                ));
            }
        }

        Ok(Self {
            schema_version: BACKEND_REASONING_POLICY_SCHEMA_VERSION,
            source,
            resolution,
        })
    }

    pub const fn schema_version(&self) -> u32 {
        self.schema_version
    }

    pub const fn source(&self) -> BackendReasoningSource {
        self.source
    }

    pub const fn resolution(&self) -> &ReasoningResolutionReceipt {
        &self.resolution
    }

    pub fn effective_effort(&self) -> &str {
        self.resolution
            .effective_effort
            .as_deref()
            .expect("backend reasoning policy construction requires an effective effort")
    }

    pub fn validate_for_route(
        &self,
        execution_provider: &str,
        execution_model: &str,
    ) -> Result<(), BackendReasoningPolicyError> {
        let effective_provider = self
            .resolution
            .effective_provider
            .as_deref()
            .expect("backend reasoning policy construction requires an effective provider");
        let effective_model = self
            .resolution
            .effective_model
            .as_deref()
            .expect("backend reasoning policy construction requires an effective model");
        if providers_match(execution_provider, effective_provider)
            && models_match(execution_model, effective_model)
        {
            return Ok(());
        }

        Err(BackendReasoningPolicyError::RouteMismatch {
            execution_provider: execution_provider.to_string(),
            execution_model: execution_model.to_string(),
            receipt_provider: effective_provider.to_string(),
            receipt_model: effective_model.to_string(),
        })
    }
}

impl<'de> Deserialize<'de> for BackendReasoningPolicyV1 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = BackendReasoningPolicyV1Wire::deserialize(deserializer)?;
        debug_assert_eq!(wire.schema_version, BACKEND_REASONING_POLICY_SCHEMA_VERSION);
        Self::new(wire.source, wire.resolution).map_err(D::Error::custom)
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BackendReasoningPolicyV1Wire {
    #[serde(
        default = "backend_reasoning_policy_schema_version",
        deserialize_with = "deserialize_backend_reasoning_policy_schema_version"
    )]
    schema_version: u32,
    source: BackendReasoningSource,
    resolution: ReasoningResolutionReceipt,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendReasoningPolicyError {
    UnsupportedResolutionSchemaVersion(u32),
    RejectedResolution,
    MissingEffectiveEffort,
    NonCanonicalEffectiveEffort,
    MissingEffectiveRoute(&'static str),
    NonCanonicalEffectiveRoute(&'static str),
    RouteMismatch {
        execution_provider: String,
        execution_model: String,
        receipt_provider: String,
        receipt_model: String,
    },
}

impl fmt::Display for BackendReasoningPolicyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedResolutionSchemaVersion(version) => write!(
                formatter,
                "unsupported reasoning resolution receipt schema version {version}"
            ),
            Self::RejectedResolution => {
                formatter.write_str("rejected reasoning resolution cannot become backend policy")
            }
            Self::MissingEffectiveEffort => formatter
                .write_str("backend reasoning policy requires a non-empty effective effort"),
            Self::NonCanonicalEffectiveEffort => formatter.write_str(
                "backend reasoning policy effective effort must not have outer whitespace",
            ),
            Self::MissingEffectiveRoute(field) => write!(
                formatter,
                "backend reasoning policy requires a non-empty {field}"
            ),
            Self::NonCanonicalEffectiveRoute(field) => write!(
                formatter,
                "backend reasoning policy {field} must not have outer whitespace"
            ),
            Self::RouteMismatch {
                execution_provider,
                execution_model,
                receipt_provider,
                receipt_model,
            } => write!(
                formatter,
                "backend reasoning execution route {execution_provider}/{execution_model} does not match effective receipt route {receipt_provider}/{receipt_model}"
            ),
        }
    }
}

impl std::error::Error for BackendReasoningPolicyError {}

const fn backend_reasoning_policy_schema_version() -> u32 {
    BACKEND_REASONING_POLICY_SCHEMA_VERSION
}

fn deserialize_backend_reasoning_policy_schema_version<'de, D>(
    deserializer: D,
) -> Result<u32, D::Error>
where
    D: Deserializer<'de>,
{
    let version = u32::deserialize(deserializer)?;
    if version != BACKEND_REASONING_POLICY_SCHEMA_VERSION {
        return Err(D::Error::custom(format!(
            "unsupported backend reasoning policy schema version {version}"
        )));
    }
    Ok(version)
}

fn providers_match(left: &str, right: &str) -> bool {
    let left = left.trim();
    let right = right.trim();
    if left.is_empty() || right.is_empty() {
        return false;
    }

    match (
        is_codex_provider_alias(left),
        is_codex_provider_alias(right),
    ) {
        (true, true) => true,
        (false, false) => left.eq_ignore_ascii_case(right),
        _ => false,
    }
}

fn is_codex_provider_alias(provider: &str) -> bool {
    provider.eq_ignore_ascii_case("openai") || provider.eq_ignore_ascii_case("codex")
}

fn models_match(left: &str, right: &str) -> bool {
    let left = left.trim();
    let right = right.trim();
    !left.is_empty() && !right.is_empty() && left.eq_ignore_ascii_case(right)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model_catalog::{
        REASONING_RESOLUTION_RECEIPT_SCHEMA_VERSION, ReasoningResolutionReceipt,
        ReasoningResolutionStatus,
    };

    #[test]
    fn backend_reasoning_policy_v1_preserves_open_ended_future_effort() {
        let policy = BackendReasoningPolicyV1::new(
            BackendReasoningSource::ChannelCommand,
            receipt(ReasoningResolutionStatus::Accepted, Some("beyond-ultra")),
        )
        .unwrap();

        assert_eq!(policy.effective_effort(), "beyond-ultra");
        assert_eq!(policy.source(), BackendReasoningSource::ChannelCommand);
        assert_eq!(
            policy.resolution().effective_effort.as_deref(),
            Some("beyond-ultra")
        );
        assert!(policy.validate_for_route(" OPENAI ", "gpt-5.6-sol").is_ok());

        let value = serde_json::to_value(&policy).unwrap();
        assert_eq!(value["schemaVersion"], 1);
        assert_eq!(value["source"], "channel-command");
        assert_eq!(value["resolution"]["effectiveEffort"], "beyond-ultra");
        assert_eq!(
            serde_json::from_value::<BackendReasoningPolicyV1>(value).unwrap(),
            policy
        );
    }

    #[test]
    fn backend_reasoning_policy_v1_accepts_every_non_rejected_resolution_status() {
        for status in [
            ReasoningResolutionStatus::Legacy,
            ReasoningResolutionStatus::Shadow,
            ReasoningResolutionStatus::Accepted,
            ReasoningResolutionStatus::Fallback,
        ] {
            assert!(
                BackendReasoningPolicyV1::new(
                    BackendReasoningSource::ChildAdmission,
                    receipt(status, Some("ultra")),
                )
                .is_ok(),
                "status {status:?} should be eligible"
            );
        }
    }

    #[test]
    fn backend_reasoning_policy_v1_rejects_rejected_or_blank_resolution() {
        assert_eq!(
            BackendReasoningPolicyV1::new(
                BackendReasoningSource::AgentDefault,
                receipt(ReasoningResolutionStatus::Rejected, Some("ultra")),
            ),
            Err(BackendReasoningPolicyError::RejectedResolution)
        );
        assert_eq!(
            BackendReasoningPolicyV1::new(
                BackendReasoningSource::AgentDefault,
                receipt(ReasoningResolutionStatus::Accepted, None),
            ),
            Err(BackendReasoningPolicyError::MissingEffectiveEffort)
        );
        assert_eq!(
            BackendReasoningPolicyV1::new(
                BackendReasoningSource::AgentDefault,
                receipt(ReasoningResolutionStatus::Fallback, Some("   ")),
            ),
            Err(BackendReasoningPolicyError::MissingEffectiveEffort)
        );
        assert_eq!(
            BackendReasoningPolicyV1::new(
                BackendReasoningSource::AgentDefault,
                receipt(ReasoningResolutionStatus::Accepted, Some(" ultra ")),
            ),
            Err(BackendReasoningPolicyError::NonCanonicalEffectiveEffort)
        );
    }

    #[test]
    fn backend_reasoning_policy_v1_validates_exact_catalog_route_aliases_and_case() {
        let policy = BackendReasoningPolicyV1::new(
            BackendReasoningSource::ChildAdmission,
            receipt(ReasoningResolutionStatus::Accepted, Some("xhigh")),
        )
        .unwrap();

        assert!(policy.validate_for_route("openai", "gpt-5.6-sol").is_ok());
        assert!(policy.validate_for_route("CODEX", "GPT-5.6-SOL").is_ok());
        assert!(matches!(
            policy.validate_for_route("openai", "gpt-5.6-terra"),
            Err(BackendReasoningPolicyError::RouteMismatch { .. })
        ));
        assert!(matches!(
            policy.validate_for_route("openrouter", "gpt-5.6-sol"),
            Err(BackendReasoningPolicyError::RouteMismatch { .. })
        ));
        assert!(matches!(
            policy.validate_for_route("openai", "gpt-5.6-sol-preview"),
            Err(BackendReasoningPolicyError::RouteMismatch { .. })
        ));
    }

    #[test]
    fn backend_reasoning_policy_v1_validates_the_effective_not_requested_route() {
        let mut resolution = receipt(ReasoningResolutionStatus::Accepted, Some("ultra"));
        resolution.requested_provider = "openrouter".to_string();
        resolution.requested_model = "requested-alias".to_string();
        let policy =
            BackendReasoningPolicyV1::new(BackendReasoningSource::ChildAdmission, resolution)
                .unwrap();

        assert!(policy.validate_for_route("codex", "gpt-5.6-sol").is_ok());
        assert!(matches!(
            policy.validate_for_route("openrouter", "requested-alias"),
            Err(BackendReasoningPolicyError::RouteMismatch { .. })
        ));
    }

    #[test]
    fn backend_reasoning_policy_v1_rejects_missing_or_noncanonical_effective_route() {
        let mut missing_provider = receipt(ReasoningResolutionStatus::Accepted, Some("max"));
        missing_provider.effective_provider = None;
        assert_eq!(
            BackendReasoningPolicyV1::new(BackendReasoningSource::ChannelCommand, missing_provider,),
            Err(BackendReasoningPolicyError::MissingEffectiveRoute(
                "effectiveProvider"
            ))
        );

        let mut blank_model = receipt(ReasoningResolutionStatus::Accepted, Some("max"));
        blank_model.effective_model = Some("   ".to_string());
        assert_eq!(
            BackendReasoningPolicyV1::new(BackendReasoningSource::ChannelCommand, blank_model),
            Err(BackendReasoningPolicyError::MissingEffectiveRoute(
                "effectiveModel"
            ))
        );

        let mut padded_model = receipt(ReasoningResolutionStatus::Accepted, Some("max"));
        padded_model.effective_model = Some(" gpt-5.6-sol ".to_string());
        assert_eq!(
            BackendReasoningPolicyV1::new(BackendReasoningSource::ChannelCommand, padded_model),
            Err(BackendReasoningPolicyError::NonCanonicalEffectiveRoute(
                "effectiveModel"
            ))
        );
    }

    #[test]
    fn backend_reasoning_policy_v1_rejects_unsupported_schema_versions() {
        let policy = BackendReasoningPolicyV1::new(
            BackendReasoningSource::ChannelCommand,
            receipt(ReasoningResolutionStatus::Accepted, Some("max")),
        )
        .unwrap();
        let mut value = serde_json::to_value(policy).unwrap();
        value["schemaVersion"] = serde_json::json!(2);

        assert!(serde_json::from_value::<BackendReasoningPolicyV1>(value).is_err());
    }

    #[test]
    fn backend_reasoning_policy_v1_deserialization_preserves_constructor_invariants() {
        let policy = BackendReasoningPolicyV1::new(
            BackendReasoningSource::ChannelCommand,
            receipt(ReasoningResolutionStatus::Accepted, Some("max")),
        )
        .unwrap();
        let mut rejected = serde_json::to_value(&policy).unwrap();
        rejected["resolution"]["status"] = serde_json::json!("rejected");
        assert!(serde_json::from_value::<BackendReasoningPolicyV1>(rejected).is_err());

        let mut blank = serde_json::to_value(policy).unwrap();
        blank["resolution"]["effectiveEffort"] = serde_json::json!("  ");
        assert!(serde_json::from_value::<BackendReasoningPolicyV1>(blank).is_err());
    }

    fn receipt(
        status: ReasoningResolutionStatus,
        effective_effort: Option<&str>,
    ) -> ReasoningResolutionReceipt {
        ReasoningResolutionReceipt {
            schema_version: REASONING_RESOLUTION_RECEIPT_SCHEMA_VERSION,
            requested_provider: "Codex".to_string(),
            requested_model: "GPT-5.6-SOL".to_string(),
            effective_provider: Some("openai".to_string()),
            effective_model: Some("gpt-5.6-sol".to_string()),
            requested_effort: effective_effort.unwrap_or_default().to_string(),
            effective_effort: effective_effort.map(str::to_string),
            catalog_effective_effort: effective_effort.map(str::to_string),
            catalog_revision: Some("test-revision".to_string()),
            status,
            authoritative: matches!(
                status,
                ReasoningResolutionStatus::Accepted
                    | ReasoningResolutionStatus::Fallback
                    | ReasoningResolutionStatus::Rejected
            ),
            reason: "test fixture".to_string(),
        }
    }
}
