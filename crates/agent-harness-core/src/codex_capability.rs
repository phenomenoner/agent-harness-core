use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use ring::digest::{SHA256, digest};
use serde::{Deserialize, Serialize};

pub(crate) const CODEX_CAPABILITY_PROOF_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CodexConfigReadResponse {
    pub(crate) config: CodexEffectiveConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct CodexEffectiveConfig {
    pub(crate) model_provider: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CodexModelListPage {
    pub(crate) data: Vec<CodexAdvertisedModel>,
    #[serde(default)]
    pub(crate) next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CodexAdvertisedModel {
    pub(crate) id: String,
    pub(crate) model: String,
    pub(crate) default_reasoning_effort: String,
    pub(crate) supported_reasoning_efforts: Vec<CodexReasoningEffortOption>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CodexReasoningEffortOption {
    pub(crate) reasoning_effort: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LiveModelCapability {
    pub(crate) model_id: String,
    pub(crate) model: String,
    pub(crate) default_reasoning_effort: String,
    pub(crate) supported_reasoning_efforts: Vec<String>,
    pub(crate) page_count: usize,
}

impl LiveModelCapability {
    pub(crate) fn supports(&self, effort: &str) -> bool {
        self.advertised_effort(effort).is_some()
    }

    pub(crate) fn advertised_effort(&self, effort: &str) -> Option<&str> {
        let effort = effort.trim();
        if effort.is_empty() {
            return None;
        }
        self.supported_reasoning_efforts
            .iter()
            .find(|advertised| advertised.eq_ignore_ascii_case(effort))
            .map(String::as_str)
    }
}

#[derive(Debug)]
pub(crate) struct CodexModelListCollector {
    max_pages: usize,
    page_count: usize,
    expected_cursor: Option<String>,
    seen_cursors: BTreeSet<String>,
    models: BTreeMap<String, CodexAdvertisedModel>,
    complete: bool,
}

impl CodexModelListCollector {
    pub(crate) fn new(max_pages: usize) -> Result<Self, CodexCapabilityError> {
        if max_pages == 0 {
            return Err(CodexCapabilityError::InvalidPageLimit);
        }
        Ok(Self {
            max_pages,
            page_count: 0,
            expected_cursor: None,
            seen_cursors: BTreeSet::new(),
            models: BTreeMap::new(),
            complete: false,
        })
    }

    pub(crate) fn push_page(
        &mut self,
        request_cursor: Option<&str>,
        page: CodexModelListPage,
    ) -> Result<Option<String>, CodexCapabilityError> {
        if self.complete {
            return Err(CodexCapabilityError::UnexpectedPageAfterCompletion);
        }
        if self.page_count >= self.max_pages {
            return Err(CodexCapabilityError::PageLimitExceeded {
                max_pages: self.max_pages,
            });
        }
        if self.expected_cursor.as_deref() != request_cursor {
            return Err(CodexCapabilityError::CursorMismatch {
                expected: self.expected_cursor.clone(),
                actual: request_cursor.map(ToString::to_string),
            });
        }

        let next_cursor = match page.next_cursor {
            Some(cursor) if cursor.is_empty() => {
                return Err(CodexCapabilityError::EmptyCursor);
            }
            Some(cursor) if self.seen_cursors.contains(&cursor) => {
                return Err(CodexCapabilityError::RepeatedCursor { cursor });
            }
            cursor => cursor,
        };

        let mut validated = Vec::with_capacity(page.data.len());
        let mut page_models = BTreeSet::new();
        for model in page.data {
            let (key, model) = validate_advertised_model(model)?;
            if self.models.contains_key(&key) || !page_models.insert(key.clone()) {
                return Err(CodexCapabilityError::DuplicateModel { model: model.model });
            }
            validated.push((key, model));
        }

        for (key, model) in validated {
            self.models.insert(key, model);
        }
        self.page_count += 1;
        self.expected_cursor = next_cursor.clone();
        match next_cursor.as_ref() {
            Some(cursor) => {
                self.seen_cursors.insert(cursor.clone());
            }
            None => self.complete = true,
        }
        Ok(next_cursor)
    }

    pub(crate) fn finish(
        self,
        expected_model: &str,
    ) -> Result<LiveModelCapability, CodexCapabilityError> {
        if !self.complete {
            return Err(CodexCapabilityError::PaginationIncomplete);
        }
        let expected_model = canonical_nonempty("expectedModel", expected_model)?;
        let key = expected_model.to_ascii_lowercase();
        let model =
            self.models
                .get(&key)
                .ok_or_else(|| CodexCapabilityError::TargetModelMissing {
                    model: expected_model.to_string(),
                })?;
        Ok(LiveModelCapability {
            model_id: model.id.clone(),
            model: model.model.clone(),
            default_reasoning_effort: model.default_reasoning_effort.clone(),
            supported_reasoning_efforts: model
                .supported_reasoning_efforts
                .iter()
                .map(|option| option.reasoning_effort.clone())
                .collect(),
            page_count: self.page_count,
        })
    }
}

fn validate_advertised_model(
    mut model: CodexAdvertisedModel,
) -> Result<(String, CodexAdvertisedModel), CodexCapabilityError> {
    canonical_nonempty("model.id", &model.id)?;
    canonical_nonempty("model.model", &model.model)?;
    canonical_nonempty(
        "model.defaultReasoningEffort",
        &model.default_reasoning_effort,
    )?;

    let mut seen = BTreeSet::new();
    for option in &model.supported_reasoning_efforts {
        let effort = canonical_nonempty(
            "model.supportedReasoningEfforts.reasoningEffort",
            &option.reasoning_effort,
        )?;
        if !seen.insert(effort.to_ascii_lowercase()) {
            return Err(CodexCapabilityError::DuplicateEffort {
                model: model.model.clone(),
                effort: effort.to_string(),
            });
        }
    }
    let Some(default) = model
        .supported_reasoning_efforts
        .iter()
        .map(|option| option.reasoning_effort.as_str())
        .find(|effort| effort.eq_ignore_ascii_case(&model.default_reasoning_effort))
    else {
        return Err(CodexCapabilityError::DefaultEffortNotSupported {
            model: model.model,
            effort: model.default_reasoning_effort,
        });
    };
    model.default_reasoning_effort = default.to_string();
    let key = model.model.to_ascii_lowercase();
    Ok((key, model))
}

pub(crate) fn bind_effective_provider(
    requested_provider: &str,
    response: &CodexConfigReadResponse,
) -> Result<String, CodexCapabilityError> {
    let requested = canonical_provider("requestedProvider", requested_provider)?;
    let Some(configured) = response.config.model_provider.as_deref() else {
        return if requested == "openai" {
            Ok(requested)
        } else {
            Err(CodexCapabilityError::MissingConfiguredProvider {
                requested: requested_provider.to_string(),
            })
        };
    };
    let configured = canonical_provider("config.model_provider", configured)?;
    if requested != configured {
        return Err(CodexCapabilityError::ProviderMismatch {
            requested,
            configured,
        });
    }
    Ok(requested)
}

fn canonical_provider(field: &'static str, provider: &str) -> Result<String, CodexCapabilityError> {
    let provider = canonical_nonempty(field, provider)?.to_ascii_lowercase();
    Ok(match provider.as_str() {
        "openai" | "codex" => "openai".to_string(),
        _ => provider,
    })
}

fn canonical_nonempty<'a>(
    field: &'static str,
    value: &'a str,
) -> Result<&'a str, CodexCapabilityError> {
    if value.trim().is_empty() || value != value.trim() {
        return Err(CodexCapabilityError::InvalidCanonicalValue {
            field,
            value: value.to_string(),
        });
    }
    Ok(value)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "kind")]
pub(crate) enum CapabilityPreference {
    Default,
    Explicit { effort: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CacheCapabilitySnapshot {
    pub(crate) provider: String,
    pub(crate) model: String,
    pub(crate) default_reasoning_effort: Option<String>,
    pub(crate) supported_reasoning_efforts: Vec<String>,
    pub(crate) revision: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "decision")]
pub(crate) enum CacheLiveDriftDecision {
    Allow { wire_effort: String },
    Deny { reason: CacheLiveDriftReason },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "kind")]
pub(crate) enum CacheLiveDriftReason {
    RouteMismatch {
        cache_model: String,
        live_model: String,
    },
    InvalidRequestedEffort {
        effort: String,
    },
    AdmissionEffortMismatch {
        admission_effort: String,
        expected_effort: String,
    },
    CacheDefaultMissing,
    CacheEffortMissing {
        effort: String,
    },
    LiveEffortMissing {
        effort: String,
    },
    DefaultMismatch {
        cache_default: String,
        live_default: String,
    },
}

pub(crate) fn decide_cache_live_drift(
    cache: &CacheCapabilitySnapshot,
    live: &LiveModelCapability,
    preference: &CapabilityPreference,
    admission_effort: &str,
) -> CacheLiveDriftDecision {
    if !cache.model.eq_ignore_ascii_case(&live.model) {
        return CacheLiveDriftDecision::Deny {
            reason: CacheLiveDriftReason::RouteMismatch {
                cache_model: cache.model.clone(),
                live_model: live.model.clone(),
            },
        };
    }

    let (expected_effort, require_matching_default) = match preference {
        CapabilityPreference::Default => {
            let Some(default) = cache.default_reasoning_effort.as_deref() else {
                return CacheLiveDriftDecision::Deny {
                    reason: CacheLiveDriftReason::CacheDefaultMissing,
                };
            };
            (default, true)
        }
        CapabilityPreference::Explicit { effort } => {
            if canonical_nonempty("preference.effort", effort).is_err() {
                return CacheLiveDriftDecision::Deny {
                    reason: CacheLiveDriftReason::InvalidRequestedEffort {
                        effort: effort.clone(),
                    },
                };
            }
            (effort.as_str(), false)
        }
    };

    if !admission_effort.eq_ignore_ascii_case(expected_effort) {
        return CacheLiveDriftDecision::Deny {
            reason: CacheLiveDriftReason::AdmissionEffortMismatch {
                admission_effort: admission_effort.to_string(),
                expected_effort: expected_effort.to_string(),
            },
        };
    }
    if !contains_effort(&cache.supported_reasoning_efforts, expected_effort) {
        return CacheLiveDriftDecision::Deny {
            reason: CacheLiveDriftReason::CacheEffortMissing {
                effort: expected_effort.to_string(),
            },
        };
    }
    let Some(live_effort) = live.advertised_effort(expected_effort) else {
        return CacheLiveDriftDecision::Deny {
            reason: CacheLiveDriftReason::LiveEffortMissing {
                effort: expected_effort.to_string(),
            },
        };
    };
    if require_matching_default
        && !expected_effort.eq_ignore_ascii_case(&live.default_reasoning_effort)
    {
        return CacheLiveDriftDecision::Deny {
            reason: CacheLiveDriftReason::DefaultMismatch {
                cache_default: expected_effort.to_string(),
                live_default: live.default_reasoning_effort.clone(),
            },
        };
    }
    CacheLiveDriftDecision::Allow {
        wire_effort: live_effort.to_string(),
    }
}

fn contains_effort(efforts: &[String], expected: &str) -> bool {
    efforts
        .iter()
        .any(|effort| effort.eq_ignore_ascii_case(expected))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SameConnectionProofContext {
    pub(crate) runtime_pid: u32,
    pub(crate) provider: String,
    pub(crate) preference: CapabilityPreference,
    pub(crate) admission_effort: String,
    pub(crate) cache_revision: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SameConnectionCapabilityProofV1 {
    pub(crate) schema_version: u32,
    pub(crate) source: String,
    pub(crate) runtime_pid: u32,
    pub(crate) provider: String,
    pub(crate) model_id: String,
    pub(crate) model: String,
    pub(crate) advertised_default_effort: String,
    pub(crate) advertised_efforts: Vec<String>,
    pub(crate) preference: CapabilityPreference,
    pub(crate) admission_effort: String,
    pub(crate) wire_effort: String,
    pub(crate) cache_revision: Option<String>,
    pub(crate) page_count: usize,
    pub(crate) proof_digest: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ProofDigestProjection<'a> {
    schema_version: u32,
    source: &'a str,
    runtime_pid: u32,
    provider: &'a str,
    model_id: &'a str,
    model: &'a str,
    advertised_default_effort: &'a str,
    advertised_efforts: &'a [String],
    preference: &'a CapabilityPreference,
    admission_effort: &'a str,
    wire_effort: &'a str,
    cache_revision: Option<&'a str>,
    page_count: usize,
}

pub(crate) fn build_same_connection_proof(
    context: &SameConnectionProofContext,
    live: &LiveModelCapability,
    wire_effort: &str,
) -> Result<SameConnectionCapabilityProofV1, CodexCapabilityError> {
    let provider = canonical_provider("proof.provider", &context.provider)?;
    canonical_nonempty("proof.admissionEffort", &context.admission_effort)?;
    let wire_effort = canonical_nonempty("proof.wireEffort", wire_effort)?;
    let Some(wire_effort) = live.advertised_effort(wire_effort) else {
        return Err(CodexCapabilityError::ProofInvariant {
            reason: "wire effort is not advertised by the live model".to_string(),
        });
    };
    match &context.preference {
        CapabilityPreference::Default
            if !wire_effort.eq_ignore_ascii_case(&live.default_reasoning_effort) =>
        {
            return Err(CodexCapabilityError::ProofInvariant {
                reason: "default preference wire effort does not match the live default"
                    .to_string(),
            });
        }
        CapabilityPreference::Explicit { effort }
            if !effort.eq_ignore_ascii_case(wire_effort)
                || !effort.eq_ignore_ascii_case(&context.admission_effort) =>
        {
            return Err(CodexCapabilityError::ProofInvariant {
                reason: "explicit preference, admission effort, and wire effort differ".to_string(),
            });
        }
        CapabilityPreference::Default | CapabilityPreference::Explicit { .. } => {}
    }

    let mut advertised_efforts = live.supported_reasoning_efforts.clone();
    advertised_efforts.sort_by_key(|effort| effort.to_ascii_lowercase());
    let source = "same-connection-app-server".to_string();
    let projection = ProofDigestProjection {
        schema_version: CODEX_CAPABILITY_PROOF_SCHEMA_VERSION,
        source: &source,
        runtime_pid: context.runtime_pid,
        provider: &provider,
        model_id: &live.model_id,
        model: &live.model,
        advertised_default_effort: &live.default_reasoning_effort,
        advertised_efforts: &advertised_efforts,
        preference: &context.preference,
        admission_effort: &context.admission_effort,
        wire_effort,
        cache_revision: context.cache_revision.as_deref(),
        page_count: live.page_count,
    };
    let canonical = serde_json::to_vec(&projection).map_err(|error| {
        CodexCapabilityError::ProofSerialization {
            reason: error.to_string(),
        }
    })?;
    let proof_digest = format!("sha256:{}", hex_lower(digest(&SHA256, &canonical).as_ref()));
    Ok(SameConnectionCapabilityProofV1 {
        schema_version: CODEX_CAPABILITY_PROOF_SCHEMA_VERSION,
        source,
        runtime_pid: context.runtime_pid,
        provider,
        model_id: live.model_id.clone(),
        model: live.model.clone(),
        advertised_default_effort: live.default_reasoning_effort.clone(),
        advertised_efforts,
        preference: context.preference.clone(),
        admission_effort: context.admission_effort.clone(),
        wire_effort: wire_effort.to_string(),
        cache_revision: context.cache_revision.clone(),
        page_count: live.page_count,
        proof_digest,
    })
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CodexCapabilityError {
    InvalidPageLimit,
    InvalidCanonicalValue {
        field: &'static str,
        value: String,
    },
    MissingConfiguredProvider {
        requested: String,
    },
    ProviderMismatch {
        requested: String,
        configured: String,
    },
    CursorMismatch {
        expected: Option<String>,
        actual: Option<String>,
    },
    EmptyCursor,
    RepeatedCursor {
        cursor: String,
    },
    PageLimitExceeded {
        max_pages: usize,
    },
    UnexpectedPageAfterCompletion,
    DuplicateModel {
        model: String,
    },
    DuplicateEffort {
        model: String,
        effort: String,
    },
    DefaultEffortNotSupported {
        model: String,
        effort: String,
    },
    PaginationIncomplete,
    TargetModelMissing {
        model: String,
    },
    ProofInvariant {
        reason: String,
    },
    ProofSerialization {
        reason: String,
    },
}

impl fmt::Display for CodexCapabilityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPageLimit => formatter.write_str("model/list max pages must be positive"),
            Self::InvalidCanonicalValue { field, value } => {
                write!(
                    formatter,
                    "{field} must be non-empty without outer whitespace: {value:?}"
                )
            }
            Self::MissingConfiguredProvider { requested } => write!(
                formatter,
                "config/read omitted model_provider for non-default provider {requested}"
            ),
            Self::ProviderMismatch {
                requested,
                configured,
            } => write!(
                formatter,
                "requested provider {requested} does not match config/read provider {configured}"
            ),
            Self::CursorMismatch { expected, actual } => write!(
                formatter,
                "model/list cursor mismatch: expected {expected:?}, received {actual:?}"
            ),
            Self::EmptyCursor => formatter.write_str("model/list returned an empty next cursor"),
            Self::RepeatedCursor { cursor } => {
                write!(formatter, "model/list repeated cursor {cursor:?}")
            }
            Self::PageLimitExceeded { max_pages } => {
                write!(formatter, "model/list exceeded page limit {max_pages}")
            }
            Self::UnexpectedPageAfterCompletion => {
                formatter.write_str("model/list returned a page after pagination completed")
            }
            Self::DuplicateModel { model } => {
                write!(formatter, "model/list duplicated model {model}")
            }
            Self::DuplicateEffort { model, effort } => {
                write!(
                    formatter,
                    "model/list duplicated effort {effort} for {model}"
                )
            }
            Self::DefaultEffortNotSupported { model, effort } => write!(
                formatter,
                "model/list default effort {effort} is not supported by {model}"
            ),
            Self::PaginationIncomplete => {
                formatter.write_str("model/list pagination did not reach a terminal page")
            }
            Self::TargetModelMissing { model } => {
                write!(
                    formatter,
                    "model/list did not advertise exact model {model}"
                )
            }
            Self::ProofInvariant { reason } => {
                write!(
                    formatter,
                    "same-connection capability proof invariant failed: {reason}"
                )
            }
            Self::ProofSerialization { reason } => {
                write!(formatter, "failed to serialize capability proof: {reason}")
            }
        }
    }
}

impl std::error::Error for CodexCapabilityError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(model_provider: Option<&str>) -> CodexConfigReadResponse {
        CodexConfigReadResponse {
            config: CodexEffectiveConfig {
                model_provider: model_provider.map(ToString::to_string),
            },
        }
    }

    fn model(id: &str, model: &str, default: &str, efforts: &[&str]) -> CodexAdvertisedModel {
        CodexAdvertisedModel {
            id: id.to_string(),
            model: model.to_string(),
            default_reasoning_effort: default.to_string(),
            supported_reasoning_efforts: efforts
                .iter()
                .map(|effort| CodexReasoningEffortOption {
                    reasoning_effort: (*effort).to_string(),
                })
                .collect(),
        }
    }

    fn page(data: Vec<CodexAdvertisedModel>, next_cursor: Option<&str>) -> CodexModelListPage {
        CodexModelListPage {
            data,
            next_cursor: next_cursor.map(ToString::to_string),
        }
    }

    fn cache(default: &str, efforts: &[&str]) -> CacheCapabilitySnapshot {
        CacheCapabilitySnapshot {
            provider: "openai".to_string(),
            model: "gpt-5.6-sol".to_string(),
            default_reasoning_effort: Some(default.to_string()),
            supported_reasoning_efforts: efforts
                .iter()
                .map(|effort| (*effort).to_string())
                .collect(),
            revision: Some("cache-r1".to_string()),
        }
    }

    #[test]
    fn provider_binding_accepts_codex_alias_and_rejects_mismatch() {
        assert_eq!(
            bind_effective_provider("codex", &config(None)).unwrap(),
            "openai"
        );
        assert_eq!(
            bind_effective_provider("openai", &config(Some("codex"))).unwrap(),
            "openai"
        );
        assert!(matches!(
            bind_effective_provider("openai", &config(Some("openrouter"))),
            Err(CodexCapabilityError::ProviderMismatch { .. })
        ));
        assert!(matches!(
            bind_effective_provider("openrouter", &config(None)),
            Err(CodexCapabilityError::MissingConfiguredProvider { .. })
        ));
    }

    #[test]
    fn paginated_catalog_matches_model_field_and_keeps_open_efforts_distinct() {
        let mut collector = CodexModelListCollector::new(4).unwrap();
        let next = collector
            .push_page(
                None,
                page(
                    vec![model("gpt-5.6-sol", "not-the-wire-model", "low", &["low"])],
                    Some("page-2"),
                ),
            )
            .unwrap();
        assert_eq!(next.as_deref(), Some("page-2"));
        assert_eq!(
            collector
                .push_page(
                    Some("page-2"),
                    page(
                        vec![model(
                            "picker-sol",
                            "gpt-5.6-sol",
                            "max",
                            &["xhigh", "max", "ultra", "frontier"],
                        )],
                        None,
                    ),
                )
                .unwrap(),
            None
        );

        let live = collector.finish("gpt-5.6-sol").unwrap();
        assert_eq!(live.model_id, "picker-sol");
        assert_eq!(live.model, "gpt-5.6-sol");
        assert_eq!(live.default_reasoning_effort, "max");
        assert!(live.supports("max"));
        assert!(live.supports("ultra"));
        assert_ne!(
            live.advertised_effort("max"),
            live.advertised_effort("ultra")
        );
    }

    #[test]
    fn collector_rejects_duplicate_model_effort_and_unsupported_default() {
        let mut duplicate_model = CodexModelListCollector::new(2).unwrap();
        duplicate_model
            .push_page(
                None,
                page(
                    vec![model("one", "gpt-5.6-sol", "max", &["max"])],
                    Some("2"),
                ),
            )
            .unwrap();
        assert!(matches!(
            duplicate_model.push_page(
                Some("2"),
                page(vec![model("two", "GPT-5.6-SOL", "max", &["max"])], None,)
            ),
            Err(CodexCapabilityError::DuplicateModel { .. })
        ));

        let mut duplicate_effort = CodexModelListCollector::new(1).unwrap();
        assert!(matches!(
            duplicate_effort.push_page(
                None,
                page(
                    vec![model("sol", "gpt-5.6-sol", "max", &["max", "MAX"])],
                    None,
                )
            ),
            Err(CodexCapabilityError::DuplicateEffort { .. })
        ));

        let mut bad_default = CodexModelListCollector::new(1).unwrap();
        assert!(matches!(
            bad_default.push_page(
                None,
                page(vec![model("sol", "gpt-5.6-sol", "max", &["xhigh"])], None,)
            ),
            Err(CodexCapabilityError::DefaultEffortNotSupported { .. })
        ));
    }

    #[test]
    fn collector_rejects_repeated_cursor_and_page_limit() {
        let mut repeated = CodexModelListCollector::new(3).unwrap();
        repeated
            .push_page(None, page(vec![], Some("next")))
            .unwrap();
        assert!(matches!(
            repeated.push_page(Some("next"), page(vec![], Some("next"))),
            Err(CodexCapabilityError::RepeatedCursor { .. })
        ));

        let mut bounded = CodexModelListCollector::new(1).unwrap();
        bounded.push_page(None, page(vec![], Some("next"))).unwrap();
        assert!(matches!(
            bounded.push_page(Some("next"), page(vec![], None)),
            Err(CodexCapabilityError::PageLimitExceeded { max_pages: 1 })
        ));
    }

    #[test]
    fn drift_requires_cache_and_live_to_agree_on_explicit_and_default_effort() {
        let live = LiveModelCapability {
            model_id: "picker-sol".to_string(),
            model: "gpt-5.6-sol".to_string(),
            default_reasoning_effort: "max".to_string(),
            supported_reasoning_efforts: vec![
                "xhigh".to_string(),
                "max".to_string(),
                "ultra".to_string(),
            ],
            page_count: 1,
        };
        assert_eq!(
            decide_cache_live_drift(
                &cache("max", &["xhigh", "max", "ultra"]),
                &live,
                &CapabilityPreference::Explicit {
                    effort: "max".to_string(),
                },
                "max",
            ),
            CacheLiveDriftDecision::Allow {
                wire_effort: "max".to_string()
            }
        );

        assert!(matches!(
            decide_cache_live_drift(
                &cache("xhigh", &["xhigh", "max"]),
                &live,
                &CapabilityPreference::Default,
                "xhigh",
            ),
            CacheLiveDriftDecision::Deny {
                reason: CacheLiveDriftReason::DefaultMismatch { .. }
            }
        ));

        let cache_allows_live_denies = LiveModelCapability {
            supported_reasoning_efforts: vec!["xhigh".to_string(), "ultra".to_string()],
            default_reasoning_effort: "xhigh".to_string(),
            ..live
        };
        assert!(matches!(
            decide_cache_live_drift(
                &cache("max", &["max"]),
                &cache_allows_live_denies,
                &CapabilityPreference::Explicit {
                    effort: "max".to_string(),
                },
                "max",
            ),
            CacheLiveDriftDecision::Deny {
                reason: CacheLiveDriftReason::LiveEffortMissing { .. }
            }
        ));
    }

    #[test]
    fn proof_digest_is_canonical_across_advertised_effort_order() {
        let context = SameConnectionProofContext {
            runtime_pid: 42,
            provider: "openai".to_string(),
            preference: CapabilityPreference::Explicit {
                effort: "max".to_string(),
            },
            admission_effort: "max".to_string(),
            cache_revision: Some("cache-r1".to_string()),
        };
        let first = LiveModelCapability {
            model_id: "picker-sol".to_string(),
            model: "gpt-5.6-sol".to_string(),
            default_reasoning_effort: "max".to_string(),
            supported_reasoning_efforts: vec!["ultra".to_string(), "max".to_string()],
            page_count: 2,
        };
        let mut reordered = first.clone();
        reordered.supported_reasoning_efforts.reverse();

        let first_proof = build_same_connection_proof(&context, &first, "max").unwrap();
        let reordered_proof = build_same_connection_proof(&context, &reordered, "max").unwrap();
        assert_eq!(first_proof.proof_digest, reordered_proof.proof_digest);
        assert!(first_proof.proof_digest.starts_with("sha256:"));
    }
}
