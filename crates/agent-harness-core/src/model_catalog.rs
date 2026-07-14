use std::{collections::BTreeSet, fs, path::Path};

use ring::digest::{SHA256, digest};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

pub const MODEL_CAPABILITY_CATALOG_SCHEMA_VERSION: u32 = 1;
pub const REASONING_RESOLUTION_RECEIPT_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ModelCatalogRolloutMode {
    #[default]
    Off,
    Shadow,
    Authoritative,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ModelCatalogRolloutAssessment {
    pub configured_mode: ModelCatalogRolloutMode,
    pub effective_mode: ModelCatalogRolloutMode,
    pub excluded: bool,
}

impl ModelCatalogRolloutAssessment {
    fn off() -> Self {
        Self {
            configured_mode: ModelCatalogRolloutMode::Off,
            effective_mode: ModelCatalogRolloutMode::Off,
            excluded: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum UnsupportedReasoningPolicy {
    Reject,
    FallbackToDefault,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReasoningResolutionStatus {
    Legacy,
    Shadow,
    Accepted,
    Rejected,
    Fallback,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelCapability {
    pub provider: String,
    pub model: String,
    pub display_name: Option<String>,
    pub default_reasoning_effort: Option<String>,
    pub supported_reasoning_efforts: Vec<String>,
    pub fast_service_tier: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelCapabilityCatalog {
    #[serde(
        default = "model_catalog_schema_version",
        deserialize_with = "deserialize_model_catalog_schema_version"
    )]
    pub schema_version: u32,
    pub revision: String,
    pub models: Vec<ModelCapability>,
}

impl ModelCapabilityCatalog {
    pub fn exact_route(&self, provider: &str, model: &str) -> Option<&ModelCapability> {
        let provider = canonical_codex_provider(provider)?;
        let model = model.trim();
        if model.is_empty() {
            return None;
        }
        self.models
            .iter()
            .find(|entry| entry.provider == provider && entry.model.eq_ignore_ascii_case(model))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReasoningResolutionReceipt {
    #[serde(
        default = "reasoning_receipt_schema_version",
        deserialize_with = "deserialize_reasoning_receipt_schema_version"
    )]
    pub schema_version: u32,
    #[serde(default)]
    pub requested_provider: String,
    #[serde(default)]
    pub requested_model: String,
    #[serde(default)]
    pub effective_provider: Option<String>,
    #[serde(default)]
    pub effective_model: Option<String>,
    pub requested_effort: String,
    pub effective_effort: Option<String>,
    pub catalog_effective_effort: Option<String>,
    pub catalog_revision: Option<String>,
    pub status: ReasoningResolutionStatus,
    pub authoritative: bool,
    pub reason: String,
}

pub fn parse_codex_model_catalog(text: &str) -> Result<ModelCapabilityCatalog, String> {
    let value = serde_json::from_str::<Value>(text)
        .map_err(|error| format!("invalid Codex model catalog JSON: {error}"))?;
    let entries = value
        .as_object()
        .and_then(|object| object.get("models"))
        .and_then(Value::as_array)
        .ok_or_else(|| "invalid Codex model catalog schema: models must be an array".to_string())?;
    let models = entries
        .iter()
        .filter_map(|entry| serde_json::from_value::<RawModel>(entry.clone()).ok())
        .filter_map(|entry| {
            let model = entry.slug.trim().to_ascii_lowercase();
            if model.is_empty() {
                return None;
            }
            let supported_reasoning_efforts = entry
                .supported_reasoning_levels
                .into_iter()
                .filter_map(|value| serde_json::from_value::<RawReasoningLevel>(value).ok())
                .filter_map(RawReasoningLevel::effort)
                .filter_map(|effort| normalize_catalog_effort(&effort))
                .fold(Vec::<String>::new(), |mut efforts, effort| {
                    if !efforts.iter().any(|current| current == &effort) {
                        efforts.push(effort);
                    }
                    efforts
                });
            Some(ModelCapability {
                provider: "openai".to_string(),
                model,
                display_name: nonempty(entry.display_name),
                default_reasoning_effort: nonempty(entry.default_reasoning_level)
                    .and_then(|effort| normalize_catalog_effort(&effort)),
                supported_reasoning_efforts,
                fast_service_tier: fast_service_tier(
                    &entry.service_tiers,
                    &entry.additional_speed_tiers,
                ),
            })
        })
        .collect::<Vec<_>>();
    if models.is_empty() {
        return Err("invalid Codex model catalog schema: no usable model entries".to_string());
    }
    let mut exact_routes = BTreeSet::new();
    for model in &models {
        if !exact_routes.insert((model.provider.clone(), model.model.clone())) {
            return Err(format!(
                "invalid Codex model catalog schema: duplicate exact route {}/{}",
                model.provider, model.model
            ));
        }
    }
    let mut revision_models = models
        .iter()
        .map(CatalogRevisionModelProjection::from)
        .collect::<Vec<_>>();
    revision_models.sort_by(|left, right| {
        left.provider
            .cmp(right.provider)
            .then_with(|| left.model.cmp(right.model))
    });
    let projection = CatalogRevisionProjection {
        schema_version: MODEL_CAPABILITY_CATALOG_SCHEMA_VERSION,
        models: &revision_models,
    };
    let canonical = serde_json::to_vec(&projection)
        .map_err(|error| format!("failed to canonicalize Codex model catalog: {error}"))?;
    let revision = format!("sha256:{}", hex_lower(digest(&SHA256, &canonical).as_ref()));
    Ok(ModelCapabilityCatalog {
        schema_version: MODEL_CAPABILITY_CATALOG_SCHEMA_VERSION,
        revision,
        models,
    })
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CatalogRevisionProjection<'a> {
    schema_version: u32,
    models: &'a [CatalogRevisionModelProjection<'a>],
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CatalogRevisionModelProjection<'a> {
    provider: &'a str,
    model: &'a str,
    default_reasoning_effort: Option<&'a str>,
    supported_reasoning_efforts: &'a [String],
    fast_service_tier: Option<&'a str>,
}

impl<'a> From<&'a ModelCapability> for CatalogRevisionModelProjection<'a> {
    fn from(model: &'a ModelCapability) -> Self {
        Self {
            provider: model.provider.as_str(),
            model: model.model.as_str(),
            default_reasoning_effort: model.default_reasoning_effort.as_deref(),
            supported_reasoning_efforts: &model.supported_reasoning_efforts,
            fast_service_tier: model.fast_service_tier.as_deref(),
        }
    }
}

/// Backward-compatible identity-less lookup. When a cohort is configured it
/// remains off unless the cohort contains `*`; agent-scoped callers should use
/// [`model_catalog_rollout_mode_for_agent`] so configured identities stay exact.
pub fn model_catalog_rollout_mode(harness_home: Option<&Path>) -> ModelCatalogRolloutMode {
    model_catalog_rollout_mode_for_agent(harness_home, None)
}

pub fn model_catalog_rollout_mode_for_agent(
    harness_home: Option<&Path>,
    agent_id: Option<&str>,
) -> ModelCatalogRolloutMode {
    model_catalog_rollout_assessment_for_agent(harness_home, agent_id).effective_mode
}

pub(crate) fn model_catalog_rollout_assessment_for_agent(
    harness_home: Option<&Path>,
    agent_id: Option<&str>,
) -> ModelCatalogRolloutAssessment {
    let Some(harness_home) = harness_home else {
        return ModelCatalogRolloutAssessment::off();
    };
    let Some(config_file) = crate::harness_config_candidates(harness_home)
        .into_iter()
        .find(|path| path.is_file())
    else {
        return ModelCatalogRolloutAssessment::off();
    };
    let Ok(text) = fs::read_to_string(config_file) else {
        return ModelCatalogRolloutAssessment::off();
    };
    let Ok(value) = serde_json::from_str::<Value>(&text) else {
        return ModelCatalogRolloutAssessment::off();
    };
    let Some(feature) = value
        .pointer("/orchestration/features/modelCatalogV2")
        .and_then(Value::as_object)
    else {
        return ModelCatalogRolloutAssessment::off();
    };
    let mode = match feature.get("mode").and_then(Value::as_str) {
        Some("shadow") => ModelCatalogRolloutMode::Shadow,
        Some("authoritative") => ModelCatalogRolloutMode::Authoritative,
        _ => ModelCatalogRolloutMode::Off,
    };
    let Some(cohort) = feature.get("enabledAgentIds") else {
        return ModelCatalogRolloutAssessment {
            configured_mode: mode,
            effective_mode: mode,
            excluded: false,
        };
    };
    let Some(cohort) = cohort.as_array() else {
        return ModelCatalogRolloutAssessment::off();
    };
    if cohort.is_empty() {
        return ModelCatalogRolloutAssessment::off();
    }

    let requested_agent_id = agent_id.map(str::trim).filter(|value| !value.is_empty());
    let mut included = false;
    for member in cohort {
        let Some(member) = member.as_str() else {
            return ModelCatalogRolloutAssessment::off();
        };
        let member = member.trim();
        if member.is_empty() {
            return ModelCatalogRolloutAssessment::off();
        }
        if member == "*" || requested_agent_id.is_some_and(|agent_id| agent_id == member) {
            included = true;
        }
    }
    ModelCatalogRolloutAssessment {
        configured_mode: mode,
        effective_mode: included
            .then_some(mode)
            .unwrap_or(ModelCatalogRolloutMode::Off),
        excluded: !included && mode != ModelCatalogRolloutMode::Off,
    }
}

pub fn resolve_reasoning_effort(
    catalog: Option<&ModelCapabilityCatalog>,
    mode: ModelCatalogRolloutMode,
    provider: &str,
    model: &str,
    requested_effort: &str,
    policy: UnsupportedReasoningPolicy,
) -> ReasoningResolutionReceipt {
    let requested = requested_effort.to_string();
    let requested_provider = provider.to_string();
    let requested_model = model.to_string();
    let normalized = normalize_catalog_effort(requested_effort);
    let legacy_effective = crate::normalize_thinking_level(requested_effort).or_else(|| {
        let normalized = requested_effort.trim().to_ascii_lowercase();
        (!normalized.is_empty()).then_some(normalized)
    });
    let legacy_provider = nonempty(Some(provider.to_string()));
    let legacy_model = nonempty(Some(model.to_string()));
    let route = catalog.and_then(|catalog| catalog.exact_route(provider, model));
    let advertised = normalized.as_ref().and_then(|effort| {
        route.and_then(|route| {
            route
                .supported_reasoning_efforts
                .iter()
                .find(|candidate| candidate.eq_ignore_ascii_case(effort))
                .cloned()
        })
    });

    match mode {
        ModelCatalogRolloutMode::Off => ReasoningResolutionReceipt {
            schema_version: REASONING_RESOLUTION_RECEIPT_SCHEMA_VERSION,
            requested_provider,
            requested_model,
            effective_provider: legacy_provider,
            effective_model: legacy_model,
            requested_effort: requested,
            effective_effort: legacy_effective,
            catalog_effective_effort: None,
            catalog_revision: None,
            status: ReasoningResolutionStatus::Legacy,
            authoritative: false,
            reason: "model catalog authority is disabled; legacy normalization applies".into(),
        },
        ModelCatalogRolloutMode::Shadow => ReasoningResolutionReceipt {
            schema_version: REASONING_RESOLUTION_RECEIPT_SCHEMA_VERSION,
            requested_provider,
            requested_model,
            effective_provider: legacy_provider,
            effective_model: legacy_model,
            requested_effort: requested,
            effective_effort: legacy_effective,
            catalog_effective_effort: advertised,
            catalog_revision: catalog.map(|catalog| catalog.revision.clone()),
            status: ReasoningResolutionStatus::Shadow,
            authoritative: false,
            reason:
                "model catalog result is observational; legacy normalization remains authoritative"
                    .into(),
        },
        ModelCatalogRolloutMode::Authoritative => {
            let Some(catalog) = catalog else {
                return ReasoningResolutionReceipt {
                    schema_version: REASONING_RESOLUTION_RECEIPT_SCHEMA_VERSION,
                    requested_provider,
                    requested_model,
                    effective_provider: legacy_provider,
                    effective_model: legacy_model,
                    requested_effort: requested,
                    effective_effort: legacy_effective,
                    catalog_effective_effort: None,
                    catalog_revision: None,
                    status: ReasoningResolutionStatus::Legacy,
                    authoritative: false,
                    reason: "catalog is unavailable; conservative legacy compatibility remains authoritative"
                        .into(),
                };
            };
            let Some(route) = route else {
                return ReasoningResolutionReceipt {
                    schema_version: REASONING_RESOLUTION_RECEIPT_SCHEMA_VERSION,
                    requested_provider,
                    requested_model,
                    effective_provider: legacy_provider,
                    effective_model: legacy_model,
                    requested_effort: requested,
                    effective_effort: legacy_effective,
                    catalog_effective_effort: None,
                    catalog_revision: Some(catalog.revision.clone()),
                    status: ReasoningResolutionStatus::Legacy,
                    authoritative: false,
                    reason: "exact provider/model route is unknown; conservative legacy compatibility remains authoritative"
                        .into(),
                };
            };
            if let Some(effective) = advertised {
                return ReasoningResolutionReceipt {
                    schema_version: REASONING_RESOLUTION_RECEIPT_SCHEMA_VERSION,
                    requested_provider,
                    requested_model,
                    effective_provider: Some(route.provider.clone()),
                    effective_model: Some(route.model.clone()),
                    requested_effort: requested,
                    effective_effort: Some(effective.clone()),
                    catalog_effective_effort: Some(effective),
                    catalog_revision: Some(catalog.revision.clone()),
                    status: ReasoningResolutionStatus::Accepted,
                    authoritative: true,
                    reason: "requested reasoning effort is advertised for the exact route".into(),
                };
            }
            if policy == UnsupportedReasoningPolicy::FallbackToDefault
                && let Some(default) = route.default_reasoning_effort.as_ref()
                && route
                    .supported_reasoning_efforts
                    .iter()
                    .any(|candidate| candidate.eq_ignore_ascii_case(default))
            {
                return ReasoningResolutionReceipt {
                    schema_version: REASONING_RESOLUTION_RECEIPT_SCHEMA_VERSION,
                    requested_provider,
                    requested_model,
                    effective_provider: Some(route.provider.clone()),
                    effective_model: Some(route.model.clone()),
                    requested_effort: requested,
                    effective_effort: Some(default.clone()),
                    catalog_effective_effort: Some(default.clone()),
                    catalog_revision: Some(catalog.revision.clone()),
                    status: ReasoningResolutionStatus::Fallback,
                    authoritative: true,
                    reason: "unsupported effort fell back to the advertised model default".into(),
                };
            }
            ReasoningResolutionReceipt {
                schema_version: REASONING_RESOLUTION_RECEIPT_SCHEMA_VERSION,
                requested_provider,
                requested_model,
                effective_provider: Some(route.provider.clone()),
                effective_model: Some(route.model.clone()),
                requested_effort: requested,
                effective_effort: None,
                catalog_effective_effort: None,
                catalog_revision: Some(catalog.revision.clone()),
                status: ReasoningResolutionStatus::Rejected,
                authoritative: true,
                reason: "requested reasoning effort is not advertised for the exact route".into(),
            }
        }
    }
}

#[derive(Debug, Deserialize)]
struct RawModel {
    #[serde(default)]
    slug: String,
    #[serde(default, alias = "displayName")]
    display_name: Option<String>,
    #[serde(
        default,
        alias = "defaultReasoningLevel",
        alias = "default_reasoning_effort",
        alias = "defaultReasoningEffort"
    )]
    default_reasoning_level: Option<String>,
    #[serde(default, alias = "supportedReasoningLevels")]
    supported_reasoning_levels: Vec<Value>,
    #[serde(default, alias = "serviceTiers")]
    service_tiers: Vec<RawServiceTier>,
    #[serde(default, alias = "additionalSpeedTiers")]
    additional_speed_tiers: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RawReasoningLevel {
    Name(String),
    Object { effort: String },
}

impl RawReasoningLevel {
    fn effort(self) -> Option<String> {
        match self {
            Self::Name(effort) | Self::Object { effort } => nonempty(Some(effort)),
        }
    }
}

#[derive(Debug, Deserialize)]
struct RawServiceTier {
    #[serde(default)]
    id: String,
    #[serde(default)]
    name: String,
}

fn canonical_codex_provider(provider: &str) -> Option<&'static str> {
    match provider.trim().to_ascii_lowercase().as_str() {
        "openai" | "codex" => Some("openai"),
        _ => None,
    }
}

fn normalize_catalog_effort(effort: &str) -> Option<String> {
    let normalized = effort.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return None;
    }
    match normalized.as_str() {
        "ultra" => None,
        _ => Some(
            match normalized.as_str() {
                "x-high" | "x_high" | "extra-high" | "extra_high" | "very-high" | "very_high" => {
                    "xhigh"
                }
                "maximum" => "max",
                "ultra-high" | "ultra_high" => "xhigh",
                _ => normalized.as_str(),
            }
            .to_string(),
        ),
    }
}

fn fast_service_tier(
    service_tiers: &[RawServiceTier],
    additional_speed_tiers: &[String],
) -> Option<String> {
    service_tiers
        .iter()
        .find(|tier| {
            let id = tier.id.trim();
            !id.is_empty()
                && (tier.name.eq_ignore_ascii_case("fast")
                    || id.eq_ignore_ascii_case("priority")
                    || id.eq_ignore_ascii_case("fast"))
        })
        .map(|tier| tier.id.trim().to_string())
        .or_else(|| {
            additional_speed_tiers
                .iter()
                .any(|tier| tier.eq_ignore_ascii_case("fast"))
                .then_some("fast".to_string())
        })
}

fn nonempty(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

const fn model_catalog_schema_version() -> u32 {
    MODEL_CAPABILITY_CATALOG_SCHEMA_VERSION
}

const fn reasoning_receipt_schema_version() -> u32 {
    REASONING_RESOLUTION_RECEIPT_SCHEMA_VERSION
}

fn deserialize_model_catalog_schema_version<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: Deserializer<'de>,
{
    let version = u32::deserialize(deserializer)?;
    if version != MODEL_CAPABILITY_CATALOG_SCHEMA_VERSION {
        return Err(serde::de::Error::custom(format!(
            "unsupported model capability catalog schema version {version}"
        )));
    }
    Ok(version)
}

fn deserialize_reasoning_receipt_schema_version<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: Deserializer<'de>,
{
    let version = u32::deserialize(deserializer)?;
    if version != REASONING_RESOLUTION_RECEIPT_SCHEMA_VERSION {
        return Err(serde::de::Error::custom(format!(
            "unsupported reasoning resolution receipt schema version {version}"
        )));
    }
    Ok(version)
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(fetched_at: &str) -> String {
        format!(
            r#"{{
          "fetched_at":"{fetched_at}","etag":"same","client_version":"0.144.1",
          "models":[
            {{"slug":"gpt-5.6-sol","display_name":"GPT-5.6-Sol",
              "default_reasoning_level":"low",
              "supported_reasoning_levels":[
                {{"effort":"low"}},{{"effort":"medium"}},{{"effort":"high"}},
                {{"effort":"xhigh"}},{{"effort":"max"}},{{"effort":"ultra"}}],
              "service_tiers":[{{"id":"priority","name":"Fast"}}],
              "comp_hash":3000,"future_field":{{"accepted":true}}}},
            {{"slug":"gpt-5.6-luna","default_reasoning_level":"medium",
              "supported_reasoning_levels":["low","medium","high","xhigh","max"],
              "additional_speed_tiers":["fast"],"comp_hash":"3000"}}
          ]}}"#
        )
    }

    #[test]
    fn model_catalog_parses_current_tolerant_cache_schema() {
        let catalog = parse_codex_model_catalog(&fixture("2026-07-10")).unwrap();
        let sol = catalog.exact_route("openai", "gpt-5.6-sol").unwrap();
        assert_eq!(sol.default_reasoning_effort.as_deref(), Some("low"));
        assert_eq!(sol.fast_service_tier.as_deref(), Some("priority"));
        assert_eq!(
            catalog
                .exact_route("codex", "gpt-5.6-luna")
                .unwrap()
                .default_reasoning_effort
                .as_deref(),
            Some("medium")
        );
    }

    #[test]
    fn model_catalog_requires_exact_provider_and_canonical_slug() {
        let catalog = parse_codex_model_catalog(&fixture("one")).unwrap();
        assert!(catalog.exact_route("openai", "GPT-5.6-SOL").is_some());
        assert!(catalog.exact_route("codex", "gpt-5.6-sol").is_some());
        assert!(catalog.exact_route("openrouter", "gpt-5.6-sol").is_none());
        assert!(
            catalog
                .exact_route("openai", "gpt-5.6-sol:latest")
                .is_none()
        );
    }

    #[test]
    fn model_catalog_filters_reserved_ultra_and_keeps_legacy_ultra_high_as_xhigh() {
        let catalog = parse_codex_model_catalog(&fixture("one")).unwrap();
        assert_eq!(
            catalog
                .exact_route("openai", "gpt-5.6-sol")
                .unwrap()
                .supported_reasoning_efforts,
            ["low", "medium", "high", "xhigh", "max"]
        );
        assert_eq!(normalize_catalog_effort("ultra"), None);
        assert_eq!(
            normalize_catalog_effort("ultra-high").as_deref(),
            Some("xhigh")
        );
    }

    #[test]
    fn model_catalog_luna_rejects_ultra_without_coercion() {
        let catalog = parse_codex_model_catalog(&fixture("one")).unwrap();
        let receipt = resolve_reasoning_effort(
            Some(&catalog),
            ModelCatalogRolloutMode::Authoritative,
            "openai",
            "gpt-5.6-luna",
            "ultra",
            UnsupportedReasoningPolicy::Reject,
        );
        assert_eq!(receipt.requested_effort, "ultra");
        assert_eq!(receipt.effective_effort, None);
        assert_eq!(receipt.status, ReasoningResolutionStatus::Rejected);
        assert!(receipt.catalog_revision.is_some());
    }

    #[test]
    fn model_catalog_receipt_records_requested_effective_revision_and_fallback() {
        let catalog = parse_codex_model_catalog(&fixture("one")).unwrap();
        let accepted = resolve_reasoning_effort(
            Some(&catalog),
            ModelCatalogRolloutMode::Authoritative,
            "openai",
            "gpt-5.6-sol",
            "MAX",
            UnsupportedReasoningPolicy::Reject,
        );
        assert_eq!(accepted.requested_effort, "MAX");
        assert_eq!(accepted.effective_effort.as_deref(), Some("max"));
        assert_eq!(accepted.status, ReasoningResolutionStatus::Accepted);
        assert_eq!(
            accepted.catalog_revision.as_deref(),
            Some(catalog.revision.as_str())
        );
        let fallback = resolve_reasoning_effort(
            Some(&catalog),
            ModelCatalogRolloutMode::Authoritative,
            "openai",
            "gpt-5.6-luna",
            "ultra",
            UnsupportedReasoningPolicy::FallbackToDefault,
        );
        assert_eq!(fallback.effective_effort.as_deref(), Some("medium"));
        assert_eq!(fallback.status, ReasoningResolutionStatus::Fallback);
    }

    #[test]
    fn model_catalog_revision_ignores_fetched_at() {
        assert_eq!(
            parse_codex_model_catalog(&fixture("one")).unwrap().revision,
            parse_codex_model_catalog(&fixture("two")).unwrap().revision
        );
    }

    #[test]
    fn model_catalog_contract_v1_serializes_schema_and_route_evidence() {
        let catalog = parse_codex_model_catalog(&fixture("one")).unwrap();
        let catalog_json = serde_json::to_value(&catalog).unwrap();
        assert_eq!(catalog_json["schemaVersion"], 1);
        let catalog_round_trip =
            serde_json::from_value::<ModelCapabilityCatalog>(catalog_json).unwrap();
        assert_eq!(catalog_round_trip, catalog);

        let receipt = resolve_reasoning_effort(
            Some(&catalog),
            ModelCatalogRolloutMode::Authoritative,
            "codex",
            "GPT-5.6-SOL",
            "XHIGH",
            UnsupportedReasoningPolicy::Reject,
        );
        let receipt_json = serde_json::to_value(&receipt).unwrap();
        assert_eq!(receipt_json["schemaVersion"], 1);
        assert_eq!(receipt_json["requestedProvider"], "codex");
        assert_eq!(receipt_json["requestedModel"], "GPT-5.6-SOL");
        assert_eq!(receipt_json["effectiveProvider"], "openai");
        assert_eq!(receipt_json["effectiveModel"], "gpt-5.6-sol");
        assert_eq!(receipt_json["requestedEffort"], "XHIGH");
        assert_eq!(receipt_json["effectiveEffort"], "xhigh");
        assert_eq!(receipt_json["catalogRevision"], catalog.revision);
        let receipt_round_trip =
            serde_json::from_value::<ReasoningResolutionReceipt>(receipt_json).unwrap();
        assert_eq!(receipt_round_trip, receipt);
    }

    #[test]
    fn model_catalog_contract_v1_revision_is_capability_canonical() {
        let first = r#"{
          "fetched_at":"one","etag":"volatile-a","client_version":"0.1",
          "models":[
            {"slug":"gpt-5.6-sol","display_name":"Sol","default_reasoning_level":"low",
             "supported_reasoning_levels":["low","xhigh","ultra"]},
            {"slug":"gpt-5.6-terra","display_name":"Terra","default_reasoning_level":"deep",
             "supported_reasoning_levels":["deep","frontier"]}
          ]}"#;
        let reordered = r#"{
          "fetched_at":"two","etag":"volatile-b","client_version":"99.0",
          "unrelated":{"cache":"metadata"},
          "models":[
            {"slug":"gpt-5.6-terra","display_name":"Terra presentation v2","default_reasoning_level":"deep",
             "supported_reasoning_levels":["deep","frontier"]},
            {"slug":"gpt-5.6-sol","display_name":"Sol presentation v2","default_reasoning_level":"low",
             "supported_reasoning_levels":["low","xhigh","ultra"]}
          ]}"#;
        let first = parse_codex_model_catalog(first).unwrap();
        let reordered = parse_codex_model_catalog(reordered).unwrap();
        assert_eq!(first.revision, reordered.revision);
        assert_eq!(first.models[0].model, "gpt-5.6-sol");
        assert_eq!(reordered.models[0].model, "gpt-5.6-terra");
    }

    #[test]
    fn model_catalog_contract_v1_shadow_keeps_legacy_effective_route() {
        let catalog = parse_codex_model_catalog(&fixture("one")).unwrap();
        let receipt = resolve_reasoning_effort(
            Some(&catalog),
            ModelCatalogRolloutMode::Shadow,
            "codex",
            "GPT-5.6-SOL",
            "max",
            UnsupportedReasoningPolicy::Reject,
        );
        assert_eq!(receipt.status, ReasoningResolutionStatus::Shadow);
        assert!(!receipt.authoritative);
        assert_eq!(receipt.requested_provider, "codex");
        assert_eq!(receipt.requested_model, "GPT-5.6-SOL");
        assert_eq!(receipt.effective_provider.as_deref(), Some("codex"));
        assert_eq!(receipt.effective_model.as_deref(), Some("GPT-5.6-SOL"));
        assert_eq!(receipt.effective_effort.as_deref(), Some("max"));
        assert_eq!(receipt.catalog_effective_effort.as_deref(), Some("max"));
    }

    #[test]
    fn model_catalog_contract_v1_rejects_unsupported_serialized_schema_versions() {
        let catalog = parse_codex_model_catalog(&fixture("one")).unwrap();
        let mut catalog_json = serde_json::to_value(catalog).unwrap();
        catalog_json["schemaVersion"] = serde_json::json!(2);
        assert!(serde_json::from_value::<ModelCapabilityCatalog>(catalog_json).is_err());

        let catalog = parse_codex_model_catalog(&fixture("one")).unwrap();
        let receipt = resolve_reasoning_effort(
            Some(&catalog),
            ModelCatalogRolloutMode::Authoritative,
            "openai",
            "gpt-5.6-sol",
            "xhigh",
            UnsupportedReasoningPolicy::Reject,
        );
        let mut receipt_json = serde_json::to_value(receipt).unwrap();
        receipt_json["schemaVersion"] = serde_json::json!(2);
        assert!(serde_json::from_value::<ReasoningResolutionReceipt>(receipt_json).is_err());
    }

    #[test]
    fn model_catalog_contract_v1_rejects_duplicate_exact_routes() {
        let duplicate = r#"{
          "models":[
            {"slug":"gpt-5.6-sol","supported_reasoning_levels":["low"]},
            {"slug":"GPT-5.6-SOL","supported_reasoning_levels":["ultra"]}
          ]}"#;
        assert!(parse_codex_model_catalog(duplicate).is_err());
    }

    #[test]
    fn model_catalog_contract_v1_skips_bad_entries_and_reasoning_elements() {
        let text = r#"{
          "models":[
            42,
            {"slug":false,"supported_reasoning_levels":["low"]},
            {"slug":"gpt-5.6-terra","default_reasoning_level":"deep",
             "supported_reasoning_levels":["deep",7,{"effort":"frontier"},{},null,"summit"]}
          ]}"#;
        let catalog = parse_codex_model_catalog(text).unwrap();
        assert_eq!(catalog.models.len(), 1);
        assert_eq!(
            catalog
                .exact_route("openai", "gpt-5.6-terra")
                .unwrap()
                .supported_reasoning_efforts,
            ["deep", "frontier", "summit"]
        );
    }

    #[test]
    fn model_catalog_contract_v1_rejects_wholly_unusable_catalog() {
        assert!(parse_codex_model_catalog(r#"{"models":[]}"#).is_err());
        assert!(parse_codex_model_catalog(r#"{"models":[42,null,{"slug":""}]}"#).is_err());
        assert!(parse_codex_model_catalog(r#"[]"#).is_err());
    }

    #[test]
    fn model_catalog_contract_v1_preserves_future_effort_names_and_order() {
        let text = r#"{"models":[{"slug":"gpt-5.6-terra",
          "default_reasoning_level":"deliberate",
          "supported_reasoning_levels":["summit","deliberate","abyss","summit"]}]}"#;
        let catalog = parse_codex_model_catalog(text).unwrap();
        let terra = catalog.exact_route("openai", "gpt-5.6-terra").unwrap();
        assert_eq!(
            terra.supported_reasoning_efforts,
            ["summit", "deliberate", "abyss"]
        );
        let receipt = resolve_reasoning_effort(
            Some(&catalog),
            ModelCatalogRolloutMode::Authoritative,
            "openai",
            "gpt-5.6-terra",
            "abyss",
            UnsupportedReasoningPolicy::Reject,
        );
        assert_eq!(receipt.effective_effort.as_deref(), Some("abyss"));
        assert_eq!(receipt.status, ReasoningResolutionStatus::Accepted);
    }

    #[test]
    fn model_catalog_contract_v1_codex_alias_is_explicit_and_receipted() {
        let catalog = parse_codex_model_catalog(&fixture("one")).unwrap();
        let openai = catalog.exact_route("openai", "gpt-5.6-sol").unwrap();
        let codex = catalog.exact_route("codex", "gpt-5.6-sol").unwrap();
        assert_eq!(openai, codex);

        let receipt = resolve_reasoning_effort(
            Some(&catalog),
            ModelCatalogRolloutMode::Authoritative,
            "codex",
            "gpt-5.6-sol",
            "xhigh",
            UnsupportedReasoningPolicy::Reject,
        );
        let json = serde_json::to_value(receipt).unwrap();
        assert_eq!(json["requestedProvider"], "codex");
        assert_eq!(json["effectiveProvider"], "openai");
    }

    #[test]
    fn model_catalog_contract_v1_missing_catalog_keeps_legacy_compatibility() {
        let receipt = resolve_reasoning_effort(
            None,
            ModelCatalogRolloutMode::Authoritative,
            "legacy-provider",
            "legacy-model",
            "custom-effort",
            UnsupportedReasoningPolicy::Reject,
        );
        assert_eq!(receipt.status, ReasoningResolutionStatus::Legacy);
        assert!(!receipt.authoritative);
        assert_eq!(receipt.effective_effort.as_deref(), Some("custom-effort"));
        let json = serde_json::to_value(receipt).unwrap();
        assert_eq!(json["effectiveProvider"], "legacy-provider");
        assert_eq!(json["effectiveModel"], "legacy-model");
        assert!(json["reason"].as_str().unwrap().contains("compatibility"));
    }

    #[test]
    fn model_catalog_contract_v1_unknown_route_keeps_legacy_compatibility() {
        let catalog = parse_codex_model_catalog(&fixture("one")).unwrap();
        let receipt = resolve_reasoning_effort(
            Some(&catalog),
            ModelCatalogRolloutMode::Authoritative,
            "openrouter",
            "future-model",
            "future-effort",
            UnsupportedReasoningPolicy::Reject,
        );
        assert_eq!(receipt.status, ReasoningResolutionStatus::Legacy);
        assert!(!receipt.authoritative);
        assert_eq!(receipt.effective_effort.as_deref(), Some("future-effort"));
        assert_eq!(
            receipt.catalog_revision.as_deref(),
            Some(catalog.revision.as_str())
        );
        let json = serde_json::to_value(receipt).unwrap();
        assert_eq!(json["requestedProvider"], "openrouter");
        assert_eq!(json["requestedModel"], "future-model");
        assert_eq!(json["effectiveProvider"], "openrouter");
        assert_eq!(json["effectiveModel"], "future-model");
    }

    #[test]
    fn model_catalog_flags_off_is_legacy_equivalent() {
        let catalog = parse_codex_model_catalog(&fixture("one")).unwrap();
        let receipt = resolve_reasoning_effort(
            Some(&catalog),
            ModelCatalogRolloutMode::Off,
            "openai",
            "gpt-5.6-sol",
            "max",
            UnsupportedReasoningPolicy::Reject,
        );
        assert_eq!(
            receipt.effective_effort,
            crate::normalize_thinking_level("max")
        );
        assert_eq!(receipt.status, ReasoningResolutionStatus::Legacy);
        assert!(!receipt.authoritative);
        assert_eq!(receipt.catalog_revision, None);
    }

    #[test]
    fn model_catalog_shadow_observes_but_has_no_authority() {
        let catalog = parse_codex_model_catalog(&fixture("one")).unwrap();
        let receipt = resolve_reasoning_effort(
            Some(&catalog),
            ModelCatalogRolloutMode::Shadow,
            "openai",
            "gpt-5.6-sol",
            "max",
            UnsupportedReasoningPolicy::Reject,
        );
        assert_eq!(receipt.status, ReasoningResolutionStatus::Shadow);
        assert_eq!(receipt.effective_effort.as_deref(), Some("max"));
        assert_eq!(receipt.catalog_effective_effort.as_deref(), Some("max"));
        assert!(!receipt.authoritative);
    }
    #[test]
    fn model_catalog_rollout_mode_reads_validated_v2_flag() {
        use std::fs;
        use std::time::{SystemTime, UNIX_EPOCH};

        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let harness_home = std::env::temp_dir().join(format!(
            "agent-harness-model-catalog-rollout-{}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(&harness_home).unwrap();
        let config_file = harness_home.join(crate::HARNESS_CONFIG_FILE_NAME);

        assert_eq!(
            model_catalog_rollout_mode(Some(&harness_home)),
            ModelCatalogRolloutMode::Off
        );
        fs::write(
            &config_file,
            r#"{"orchestration":{"features":{"modelCatalogV2":{"mode":"shadow"}}}}"#,
        )
        .unwrap();
        assert_eq!(
            model_catalog_rollout_mode(Some(&harness_home)),
            ModelCatalogRolloutMode::Shadow
        );
        fs::write(
            &config_file,
            r#"{"orchestration":{"features":{"modelCatalogV2":{"mode":"authoritative"}}}}"#,
        )
        .unwrap();
        assert_eq!(
            model_catalog_rollout_mode(Some(&harness_home)),
            ModelCatalogRolloutMode::Authoritative
        );

        let _ = fs::remove_dir_all(harness_home);
    }

    #[test]
    fn model_catalog_rollout_mode_applies_exact_and_wildcard_agent_cohorts() {
        use std::fs;
        use std::time::{SystemTime, UNIX_EPOCH};

        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let harness_home = std::env::temp_dir().join(format!(
            "agent-harness-model-catalog-cohort-{}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(&harness_home).unwrap();
        let config_file = harness_home.join(crate::HARNESS_CONFIG_FILE_NAME);

        assert_eq!(
            model_catalog_rollout_mode_for_agent(Some(&harness_home), Some("agent-a")),
            ModelCatalogRolloutMode::Off
        );

        fs::write(
            &config_file,
            r#"{"orchestration":{"features":{"modelCatalogV2":{"mode":" authoritative "}}}}"#,
        )
        .unwrap();
        assert_eq!(
            model_catalog_rollout_mode_for_agent(Some(&harness_home), Some("agent-a")),
            ModelCatalogRolloutMode::Off,
            "runtime parsing must not enable a mode rejected by config validation"
        );

        fs::write(
            &config_file,
            r#"{"orchestration":{"features":{"modelCatalogV2":{"mode":"shadow"}}}}"#,
        )
        .unwrap();
        assert_eq!(
            model_catalog_rollout_mode_for_agent(Some(&harness_home), Some("agent-b")),
            ModelCatalogRolloutMode::Shadow
        );
        assert_eq!(
            model_catalog_rollout_mode_for_agent(Some(&harness_home), None),
            ModelCatalogRolloutMode::Shadow
        );

        fs::write(
            &config_file,
            r#"{"orchestration":{"features":{"modelCatalogV2":{"mode":"authoritative","enabledAgentIds":["agent-a"]}}}}"#,
        )
        .unwrap();
        assert_eq!(
            model_catalog_rollout_mode_for_agent(Some(&harness_home), Some("agent-a")),
            ModelCatalogRolloutMode::Authoritative
        );
        assert_eq!(
            model_catalog_rollout_mode_for_agent(Some(&harness_home), Some("agent-b")),
            ModelCatalogRolloutMode::Off
        );
        assert_eq!(
            model_catalog_rollout_mode_for_agent(Some(&harness_home), None),
            ModelCatalogRolloutMode::Off
        );

        fs::write(
            &config_file,
            r#"{"orchestration":{"features":{"modelCatalogV2":{"mode":"authoritative","enabledAgentIds":[" Agent-A "]}}}}"#,
        )
        .unwrap();
        assert_eq!(
            model_catalog_rollout_mode_for_agent(Some(&harness_home), Some(" Agent-A ")),
            ModelCatalogRolloutMode::Authoritative,
            "outer whitespace is not part of the routing identity"
        );
        assert_eq!(
            model_catalog_rollout_mode_for_agent(Some(&harness_home), Some("agent-a")),
            ModelCatalogRolloutMode::Off,
            "agent routing identities remain case-sensitive"
        );

        fs::write(
            &config_file,
            r#"{"orchestration":{"features":{"modelCatalogV2":{"mode":"authoritative","enabledAgentIds":["*"]}}}}"#,
        )
        .unwrap();
        assert_eq!(
            model_catalog_rollout_mode_for_agent(Some(&harness_home), Some("agent-b")),
            ModelCatalogRolloutMode::Authoritative
        );
        assert_eq!(
            model_catalog_rollout_mode_for_agent(Some(&harness_home), None),
            ModelCatalogRolloutMode::Authoritative
        );

        for cohort in [
            "[]",
            "[\"agent-a\", 7]",
            "\"agent-a\"",
            "[\"agent-a\", \"   \"]",
            "[\"*\", \"\"]",
        ] {
            let cohort = serde_json::from_str::<Value>(cohort).unwrap();
            let config = serde_json::json!({
                "orchestration": {
                    "features": {
                        "modelCatalogV2": {
                            "mode": "authoritative",
                            "enabledAgentIds": cohort,
                        }
                    }
                }
            });
            fs::write(&config_file, serde_json::to_vec(&config).unwrap()).unwrap();
            assert_eq!(
                model_catalog_rollout_mode_for_agent(Some(&harness_home), Some("agent-a")),
                ModelCatalogRolloutMode::Off,
                "malformed or empty cohort must fail closed: {cohort}"
            );
        }

        let _ = fs::remove_dir_all(harness_home);
    }

    #[test]
    fn rollout_assessment_distinguishes_excluded_agent_without_changing_public_mode() {
        use std::time::{SystemTime, UNIX_EPOCH};

        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let harness_home = std::env::temp_dir().join(format!(
            "agent-harness-model-catalog-assessment-{}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join(crate::HARNESS_CONFIG_FILE_NAME),
            r#"{"orchestration":{"features":{"modelCatalogV2":{"mode":"authoritative","enabledAgentIds":["agent-a"]}}}}"#,
        )
        .unwrap();

        let included =
            model_catalog_rollout_assessment_for_agent(Some(&harness_home), Some("agent-a"));
        assert_eq!(
            included.configured_mode,
            ModelCatalogRolloutMode::Authoritative
        );
        assert_eq!(
            included.effective_mode,
            ModelCatalogRolloutMode::Authoritative
        );
        assert!(!included.excluded);

        let excluded =
            model_catalog_rollout_assessment_for_agent(Some(&harness_home), Some("agent-b"));
        assert_eq!(
            excluded.configured_mode,
            ModelCatalogRolloutMode::Authoritative
        );
        assert_eq!(excluded.effective_mode, ModelCatalogRolloutMode::Off);
        assert!(excluded.excluded);
        assert_eq!(
            model_catalog_rollout_mode_for_agent(Some(&harness_home), Some("agent-b")),
            ModelCatalogRolloutMode::Off,
            "the existing public API remains backward compatible"
        );

        let _ = fs::remove_dir_all(harness_home);
    }
}
