use std::{fs, path::Path};

use ring::digest::{SHA256, digest};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ModelCatalogRolloutMode {
    #[default]
    Off,
    Shadow,
    Authoritative,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum UnsupportedReasoningPolicy {
    Reject,
    FallbackToDefault,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReasoningResolutionStatus {
    Legacy,
    Shadow,
    Accepted,
    Rejected,
    Fallback,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelCapability {
    pub provider: String,
    pub model: String,
    pub display_name: Option<String>,
    pub default_reasoning_effort: Option<String>,
    pub supported_reasoning_efforts: Vec<String>,
    pub fast_service_tier: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelCapabilityCatalog {
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReasoningResolutionReceipt {
    pub requested_effort: String,
    pub effective_effort: Option<String>,
    pub catalog_effective_effort: Option<String>,
    pub catalog_revision: Option<String>,
    pub status: ReasoningResolutionStatus,
    pub authoritative: bool,
    pub reason: String,
}

pub fn parse_codex_model_catalog(text: &str) -> Result<ModelCapabilityCatalog, String> {
    let mut value = serde_json::from_str::<Value>(text)
        .map_err(|error| format!("invalid Codex model catalog JSON: {error}"))?;
    let raw = serde_json::from_value::<RawCatalog>(value.clone())
        .map_err(|error| format!("invalid Codex model catalog schema: {error}"))?;
    if let Some(object) = value.as_object_mut() {
        object.remove("fetched_at");
        object.remove("fetchedAt");
    }
    let canonical = serde_json::to_vec(&value)
        .map_err(|error| format!("failed to canonicalize Codex model catalog: {error}"))?;
    let revision = format!("sha256:{}", hex_lower(digest(&SHA256, &canonical).as_ref()));
    let models = raw
        .models
        .into_iter()
        .filter_map(|entry| {
            let model = entry.slug.trim().to_ascii_lowercase();
            if model.is_empty() {
                return None;
            }
            let supported_reasoning_efforts = entry
                .supported_reasoning_levels
                .into_iter()
                .filter_map(RawReasoningLevel::effort)
                .map(|effort| effort.trim().to_ascii_lowercase())
                .filter(|effort| !effort.is_empty())
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
                    .map(|effort| effort.to_ascii_lowercase()),
                supported_reasoning_efforts,
                fast_service_tier: fast_service_tier(
                    &entry.service_tiers,
                    &entry.additional_speed_tiers,
                ),
            })
        })
        .collect();
    Ok(ModelCapabilityCatalog { revision, models })
}

pub fn model_catalog_rollout_mode(harness_home: Option<&Path>) -> ModelCatalogRolloutMode {
    let Some(harness_home) = harness_home else {
        return ModelCatalogRolloutMode::Off;
    };
    let Some(config_file) = crate::harness_config_candidates(harness_home)
        .into_iter()
        .find(|path| path.is_file())
    else {
        return ModelCatalogRolloutMode::Off;
    };
    let Ok(text) = fs::read_to_string(config_file) else {
        return ModelCatalogRolloutMode::Off;
    };
    let Ok(value) = serde_json::from_str::<Value>(&text) else {
        return ModelCatalogRolloutMode::Off;
    };
    match value
        .pointer("/orchestration/features/modelCatalogV2/mode")
        .and_then(Value::as_str)
        .map(str::trim)
    {
        Some("shadow") => ModelCatalogRolloutMode::Shadow,
        Some("authoritative") => ModelCatalogRolloutMode::Authoritative,
        _ => ModelCatalogRolloutMode::Off,
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
    let normalized = normalize_catalog_effort(requested_effort);
    let legacy_effective = crate::normalize_thinking_level(requested_effort).or_else(|| {
        let normalized = requested_effort.trim().to_ascii_lowercase();
        (!normalized.is_empty()).then_some(normalized)
    });
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
            requested_effort: requested,
            effective_effort: legacy_effective,
            catalog_effective_effort: None,
            catalog_revision: None,
            status: ReasoningResolutionStatus::Legacy,
            authoritative: false,
            reason: "model catalog authority is disabled; legacy normalization applies".into(),
        },
        ModelCatalogRolloutMode::Shadow => ReasoningResolutionReceipt {
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
                return rejected_receipt(
                    requested,
                    None,
                    "authoritative model catalog is unavailable",
                );
            };
            let Some(route) = route else {
                return rejected_receipt(
                    requested,
                    Some(&catalog.revision),
                    "provider/model route is absent from the authoritative catalog",
                );
            };
            if let Some(effective) = advertised {
                return ReasoningResolutionReceipt {
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
                    requested_effort: requested,
                    effective_effort: Some(default.clone()),
                    catalog_effective_effort: Some(default.clone()),
                    catalog_revision: Some(catalog.revision.clone()),
                    status: ReasoningResolutionStatus::Fallback,
                    authoritative: true,
                    reason: "unsupported effort fell back to the advertised model default".into(),
                };
            }
            rejected_receipt(
                requested,
                Some(&catalog.revision),
                "requested reasoning effort is not advertised for the exact route",
            )
        }
    }
}

#[derive(Debug, Deserialize)]
struct RawCatalog {
    #[serde(default)]
    models: Vec<RawModel>,
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
    supported_reasoning_levels: Vec<RawReasoningLevel>,
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
    Some(
        match normalized.as_str() {
            "x-high" | "x_high" | "extra-high" | "extra_high" | "very-high" | "very_high" => {
                "xhigh"
            }
            "maximum" => "max",
            "ultra-high" | "ultra_high" => "ultra",
            _ => normalized.as_str(),
        }
        .to_string(),
    )
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

fn rejected_receipt(
    requested_effort: String,
    revision: Option<&str>,
    reason: &str,
) -> ReasoningResolutionReceipt {
    ReasoningResolutionReceipt {
        requested_effort,
        effective_effort: None,
        catalog_effective_effort: None,
        catalog_revision: revision.map(str::to_string),
        status: ReasoningResolutionStatus::Rejected,
        authoritative: true,
        reason: reason.to_string(),
    }
}

fn nonempty(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
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
    fn model_catalog_preserves_sol_effort_order_through_ultra() {
        let catalog = parse_codex_model_catalog(&fixture("one")).unwrap();
        assert_eq!(
            catalog
                .exact_route("openai", "gpt-5.6-sol")
                .unwrap()
                .supported_reasoning_efforts,
            ["low", "medium", "high", "xhigh", "max", "ultra"]
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
        assert_eq!(receipt.effective_effort.as_deref(), Some("xhigh"));
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
}
