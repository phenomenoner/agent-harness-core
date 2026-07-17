use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use ring::digest::{SHA256, digest};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    ChannelStateLane, append_jsonl_value_once_by_event_key, current_log_time_ms,
    harness_config_candidates, write_json_atomic,
};

pub const CODEX_WEB_SEARCH_DECISION_SCHEMA: &str = "agent-harness.codex-web-search-decision.v1";
pub const CODEX_WEB_SEARCH_ACTION_SCHEMA: &str = "agent-harness.codex-web-search-action.v1";
pub const CODEX_WEB_SEARCH_THREAD_BINDING_SCHEMA: &str =
    "agent-harness.codex-web-search-thread-binding.v1";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CodexWebSearchMode {
    Disabled,
    #[default]
    Cached,
    Indexed,
    Live,
}

impl CodexWebSearchMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Cached => "cached",
            Self::Indexed => "indexed",
            Self::Live => "live",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CodexWebSearchIntent {
    #[default]
    Ordinary,
    Freshness,
    Offline,
    Replay,
    Sensitive,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexWebSearchPlan {
    pub requested_mode: CodexWebSearchMode,
    pub intent: CodexWebSearchIntent,
    pub reason: String,
    pub require_capability: bool,
    pub lane_digest: Option<String>,
    pub provider: String,
    pub policy_digest: String,
}

impl Default for CodexWebSearchPlan {
    fn default() -> Self {
        Self {
            requested_mode: CodexWebSearchMode::Cached,
            intent: CodexWebSearchIntent::Ordinary,
            reason: "default-cached".to_string(),
            require_capability: true,
            lane_digest: None,
            provider: "openai".to_string(),
            policy_digest: digest_text("codex-web-search-default-v1"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexWebSearchCapabilityV1 {
    pub web_search: bool,
    pub namespace_tools: Option<bool>,
    pub image_generation: Option<bool>,
    pub generation: String,
    pub observed_at_ms: i64,
    pub source: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexWebSearchDecisionReceiptV1 {
    pub schema: String,
    pub requested_mode: CodexWebSearchMode,
    pub effective_mode: CodexWebSearchMode,
    pub intent: CodexWebSearchIntent,
    pub reason: String,
    pub policy_digest: String,
    pub capability: CodexWebSearchCapabilityV1,
    pub provider: String,
    pub lane: Option<ChannelStateLane>,
    pub lane_digest: Option<String>,
    pub queue_id: Option<String>,
    pub sandbox_independent: bool,
    pub limitation_notice: Option<String>,
    pub observed_at_ms: i64,
}

impl CodexWebSearchDecisionReceiptV1 {
    pub fn effective_mode_value(&self) -> Value {
        Value::String(self.effective_mode.as_str().to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexWebSearchThreadBindingV1 {
    pub schema: String,
    pub event_key: String,
    pub thread_id: String,
    pub effective_mode: CodexWebSearchMode,
    pub capability_generation: String,
    pub policy_digest: String,
    pub provider: String,
    pub lane_digest: Option<String>,
    pub bound_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexWebSearchActionReceiptV1 {
    pub schema: String,
    pub event_key: String,
    pub at_ms: i64,
    pub queue_id: Option<String>,
    pub lane: Option<ChannelStateLane>,
    pub lane_digest: Option<String>,
    pub provider: String,
    pub capability_generation: String,
    pub requested_mode: CodexWebSearchMode,
    pub effective_mode: CodexWebSearchMode,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub item_id: Option<String>,
    pub phase: String,
    pub action: String,
    pub query_digest: Option<String>,
    pub query_redacted: bool,
    pub opened_domain: Option<String>,
    pub citation_ref_count: usize,
    pub action_ordinal: u64,
    pub admission: String,
}

#[derive(Debug, Clone)]
pub struct CodexWebSearchObserver {
    actions_file: PathBuf,
    receipt: CodexWebSearchDecisionReceiptV1,
    action_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct CodexWebSearchPolicyDigestProjection<'a> {
    default_mode: CodexWebSearchMode,
    freshness_mode: CodexWebSearchMode,
    sensitive_mode: CodexWebSearchMode,
    require_capability: bool,
    allow_live: bool,
    disabled_lane_digests: &'a [String],
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CodexWebSearchPolicy {
    default_mode: CodexWebSearchMode,
    freshness_mode: CodexWebSearchMode,
    sensitive_mode: CodexWebSearchMode,
    require_capability: bool,
    allow_live: bool,
    disabled_lane_digests: Vec<String>,
}

impl Default for CodexWebSearchPolicy {
    fn default() -> Self {
        Self {
            default_mode: CodexWebSearchMode::Cached,
            freshness_mode: CodexWebSearchMode::Live,
            sensitive_mode: CodexWebSearchMode::Disabled,
            require_capability: true,
            allow_live: true,
            disabled_lane_digests: Vec::new(),
        }
    }
}

pub fn plan_codex_web_search(
    harness_home: &Path,
    user_message: Option<&str>,
    prompt_bundle: &Value,
    prepared_receipt: &Value,
    lane_digest: Option<&str>,
    provider: Option<&str>,
) -> io::Result<CodexWebSearchPlan> {
    let policy = load_policy(harness_home)?;
    let text = user_message.unwrap_or_default();
    let normalized = text.to_ascii_lowercase();
    let replay = metadata_flag(prompt_bundle, prepared_receipt, &["replay", "replayMode"])
        || metadata_text_contains(prompt_bundle, prepared_receipt, "replay");
    let offline = metadata_flag(prompt_bundle, prepared_receipt, &["offline", "offlineMode"])
        || contains_any(
            &normalized,
            &[
                "do not browse",
                "don't browse",
                "without internet",
                "offline only",
                "no web search",
                "不要上網",
                "不要搜尋網路",
            ],
        );
    let sensitive = metadata_flag(
        prompt_bundle,
        prepared_receipt,
        &["sensitive", "containsSensitiveData", "privateIncident"],
    ) || contains_any(
        &normalized,
        &[
            "private incident",
            "credential leak",
            "api key=",
            "api_key=",
            "password=",
            "authorization: bearer",
            "device code",
            "私密事件",
            "憑證外洩",
        ],
    );
    let freshness = contains_freshness_intent(&normalized);
    let lane_disabled = lane_digest.is_some_and(|digest| {
        policy
            .disabled_lane_digests
            .iter()
            .any(|disabled| disabled == digest)
    });

    let (intent, requested_mode, reason) = if replay {
        (
            CodexWebSearchIntent::Replay,
            CodexWebSearchMode::Disabled,
            "replay-forces-disabled",
        )
    } else if offline {
        (
            CodexWebSearchIntent::Offline,
            CodexWebSearchMode::Disabled,
            "offline-intent-forces-disabled",
        )
    } else if sensitive {
        (
            CodexWebSearchIntent::Sensitive,
            policy.sensitive_mode,
            "sensitive-turn-forces-disabled",
        )
    } else if freshness && policy.allow_live && !lane_disabled {
        (
            CodexWebSearchIntent::Freshness,
            policy.freshness_mode,
            "explicit-freshness-intent",
        )
    } else if freshness {
        (
            CodexWebSearchIntent::Freshness,
            policy.default_mode,
            "freshness-live-denied-by-lane-policy",
        )
    } else {
        (
            CodexWebSearchIntent::Ordinary,
            policy.default_mode,
            "default-cached",
        )
    };
    let projection = CodexWebSearchPolicyDigestProjection {
        default_mode: policy.default_mode,
        freshness_mode: policy.freshness_mode,
        sensitive_mode: policy.sensitive_mode,
        require_capability: policy.require_capability,
        allow_live: policy.allow_live,
        disabled_lane_digests: &policy.disabled_lane_digests,
    };
    let canonical = serde_json::to_vec(&projection).map_err(io::Error::other)?;
    Ok(CodexWebSearchPlan {
        requested_mode,
        intent,
        reason: reason.to_string(),
        require_capability: policy.require_capability,
        lane_digest: lane_digest.map(ToString::to_string),
        provider: provider.unwrap_or("openai").to_string(),
        policy_digest: digest_bytes(&canonical),
    })
}

pub fn parse_codex_web_search_capability(result: &Value) -> io::Result<CodexWebSearchCapabilityV1> {
    let web_search = result
        .get("webSearch")
        .and_then(Value::as_bool)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "provider capability response omitted webSearch",
            )
        })?;
    let canonical = serde_json::to_vec(result).map_err(io::Error::other)?;
    Ok(CodexWebSearchCapabilityV1 {
        web_search,
        namespace_tools: result.get("namespaceTools").and_then(Value::as_bool),
        image_generation: result.get("imageGeneration").and_then(Value::as_bool),
        generation: digest_bytes(&canonical),
        observed_at_ms: now_ms(),
        source: "modelProvider/capabilities/read".to_string(),
    })
}

pub fn unavailable_codex_web_search_capability(reason: &str) -> CodexWebSearchCapabilityV1 {
    CodexWebSearchCapabilityV1 {
        web_search: false,
        namespace_tools: None,
        image_generation: None,
        generation: digest_text(reason),
        observed_at_ms: now_ms(),
        source: "capability-unavailable".to_string(),
    }
}

pub fn decide_effective_codex_web_search(
    plan: &CodexWebSearchPlan,
    capability: CodexWebSearchCapabilityV1,
    lane: Option<ChannelStateLane>,
    queue_id: Option<String>,
) -> CodexWebSearchDecisionReceiptV1 {
    let capability_missing = plan.require_capability && !capability.web_search;
    let effective_mode = if capability_missing {
        CodexWebSearchMode::Disabled
    } else {
        plan.requested_mode
    };
    let limitation_notice = if capability_missing
        && plan.requested_mode != CodexWebSearchMode::Disabled
    {
        Some("Web search is unavailable for this turn because the selected provider did not advertise the webSearch capability. Do not claim that online verification or browsing occurred.".to_string())
    } else if plan.intent == CodexWebSearchIntent::Freshness
        && plan.requested_mode != CodexWebSearchMode::Live
    {
        Some("Live web search was denied by lane policy for this turn. Cached results may be used, but do not claim current online verification.".to_string())
    } else {
        None
    };
    CodexWebSearchDecisionReceiptV1 {
        schema: CODEX_WEB_SEARCH_DECISION_SCHEMA.to_string(),
        requested_mode: plan.requested_mode,
        effective_mode,
        intent: plan.intent,
        reason: if capability_missing {
            "provider-web-search-capability-absent".to_string()
        } else {
            plan.reason.clone()
        },
        policy_digest: plan.policy_digest.clone(),
        capability,
        provider: plan.provider.clone(),
        lane,
        lane_digest: plan.lane_digest.clone(),
        queue_id,
        sandbox_independent: true,
        limitation_notice,
        observed_at_ms: now_ms(),
    }
}

pub fn persist_codex_web_search_decision(
    execution_dir: &Path,
    receipt: &CodexWebSearchDecisionReceiptV1,
) -> io::Result<PathBuf> {
    let path = execution_dir.join("codex-web-search-decision.v1.json");
    write_json_atomic(&path, receipt)?;
    Ok(path)
}

pub fn codex_web_search_thread_bindings_file(codex_binding_file: &Path) -> PathBuf {
    codex_binding_file.with_file_name("codex-web-search-thread-bindings.v1.jsonl")
}

pub fn read_codex_web_search_thread_binding(
    path: &Path,
    thread_id: &str,
) -> io::Result<Option<CodexWebSearchThreadBindingV1>> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let mut latest = None;
    for (index, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let record =
            serde_json::from_str::<CodexWebSearchThreadBindingV1>(line).map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "invalid web-search thread binding at line {}: {error}",
                        index + 1
                    ),
                )
            })?;
        if record.thread_id == thread_id {
            latest = Some(record);
        }
    }
    Ok(latest)
}

pub fn persist_codex_web_search_thread_binding(
    path: &Path,
    thread_id: &str,
    receipt: &CodexWebSearchDecisionReceiptV1,
) -> io::Result<bool> {
    let event_key = digest_text(&format!(
        "{}\n{}\n{}\n{}",
        thread_id,
        receipt.effective_mode.as_str(),
        receipt.capability.generation,
        receipt.policy_digest
    ));
    append_jsonl_value_once_by_event_key(
        path,
        &CodexWebSearchThreadBindingV1 {
            schema: CODEX_WEB_SEARCH_THREAD_BINDING_SCHEMA.to_string(),
            event_key,
            thread_id: thread_id.to_string(),
            effective_mode: receipt.effective_mode,
            capability_generation: receipt.capability.generation.clone(),
            policy_digest: receipt.policy_digest.clone(),
            provider: receipt.provider.clone(),
            lane_digest: receipt.lane_digest.clone(),
            bound_at_ms: now_ms(),
        },
    )
}

pub fn codex_web_search_resume_is_stable(
    binding: Option<&CodexWebSearchThreadBindingV1>,
    receipt: &CodexWebSearchDecisionReceiptV1,
) -> bool {
    binding.is_some_and(|binding| {
        binding.effective_mode == receipt.effective_mode
            && binding.provider == receipt.provider
            && binding.lane_digest == receipt.lane_digest
    })
}

impl CodexWebSearchObserver {
    pub fn new(execution_dir: &Path, receipt: CodexWebSearchDecisionReceiptV1) -> Self {
        Self {
            actions_file: execution_dir.join("codex-web-search-actions.v1.jsonl"),
            receipt,
            action_count: 0,
        }
    }

    pub fn observe(&mut self, value: &Value, event_ordinal: u64) -> io::Result<()> {
        let method = value
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if !matches!(method, "item/started" | "item/completed")
            || value.pointer("/params/item/type").and_then(Value::as_str) != Some("webSearch")
        {
            return Ok(());
        }
        self.action_count += 1;
        let item = value.pointer("/params/item").unwrap_or(&Value::Null);
        let action = item
            .pointer("/action/type")
            .and_then(Value::as_str)
            .unwrap_or("other");
        let query = item
            .pointer("/action/query")
            .and_then(Value::as_str)
            .or_else(|| item.get("query").and_then(Value::as_str));
        let item_id = item
            .get("id")
            .and_then(Value::as_str)
            .map(ToString::to_string);
        let thread_id = value
            .pointer("/params/threadId")
            .and_then(Value::as_str)
            .map(ToString::to_string);
        let turn_id = value
            .pointer("/params/turnId")
            .and_then(Value::as_str)
            .map(ToString::to_string);
        let opened_domain = item
            .pointer("/action/url")
            .and_then(Value::as_str)
            .and_then(public_http_domain);
        let citation_ref_count = count_citation_refs(item);
        let event_key = digest_text(&format!(
            "{}\n{}\n{}\n{}\n{}",
            thread_id.as_deref().unwrap_or(""),
            turn_id.as_deref().unwrap_or(""),
            item_id.as_deref().unwrap_or(""),
            method,
            event_ordinal
        ));
        append_jsonl_value_once_by_event_key(
            &self.actions_file,
            &CodexWebSearchActionReceiptV1 {
                schema: CODEX_WEB_SEARCH_ACTION_SCHEMA.to_string(),
                event_key,
                at_ms: now_ms(),
                queue_id: self.receipt.queue_id.clone(),
                lane: self.receipt.lane.clone(),
                lane_digest: self.receipt.lane_digest.clone(),
                provider: self.receipt.provider.clone(),
                capability_generation: self.receipt.capability.generation.clone(),
                requested_mode: self.receipt.requested_mode,
                effective_mode: self.receipt.effective_mode,
                thread_id,
                turn_id,
                item_id,
                phase: method.trim_start_matches("item/").to_string(),
                action: action.to_string(),
                query_digest: query.map(digest_text),
                query_redacted: query.is_some(),
                opened_domain,
                citation_ref_count,
                action_ordinal: self.action_count,
                admission: "runtime-only-untrusted-not-admitted-to-memory-skill-or-dream"
                    .to_string(),
            },
        )?;
        Ok(())
    }
}

fn load_policy(harness_home: &Path) -> io::Result<CodexWebSearchPolicy> {
    let Some(path) = harness_config_candidates(harness_home)
        .into_iter()
        .find(|path| path.is_file())
    else {
        return Ok(CodexWebSearchPolicy::default());
    };
    let value: Value =
        serde_json::from_str(&fs::read_to_string(path)?).map_err(io::Error::other)?;
    let Some(object) = value.get("codexWebSearch").and_then(Value::as_object) else {
        return Ok(CodexWebSearchPolicy::default());
    };
    let mut policy = CodexWebSearchPolicy::default();
    if let Some(mode) = object.get("defaultMode").and_then(Value::as_str) {
        policy.default_mode = parse_mode(mode)?;
    }
    if let Some(mode) = object.get("freshnessMode").and_then(Value::as_str) {
        policy.freshness_mode = parse_mode(mode)?;
    }
    if let Some(mode) = object.get("sensitiveMode").and_then(Value::as_str) {
        policy.sensitive_mode = parse_mode(mode)?;
    }
    policy.require_capability = object
        .get("requireCapability")
        .and_then(Value::as_bool)
        .unwrap_or(policy.require_capability);
    policy.allow_live = object
        .get("allowLive")
        .and_then(Value::as_bool)
        .unwrap_or(policy.allow_live);
    policy.disabled_lane_digests = object
        .get("disabledLaneDigests")
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .map(ToString::to_string)
                .collect()
        })
        .unwrap_or_default();
    Ok(policy)
}

fn parse_mode(value: &str) -> io::Result<CodexWebSearchMode> {
    match value.trim().to_ascii_lowercase().as_str() {
        "disabled" => Ok(CodexWebSearchMode::Disabled),
        "cached" => Ok(CodexWebSearchMode::Cached),
        "indexed" => Ok(CodexWebSearchMode::Indexed),
        "live" => Ok(CodexWebSearchMode::Live),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid codexWebSearch mode `{other}`"),
        )),
    }
}

fn metadata_flag(bundle: &Value, receipt: &Value, keys: &[&str]) -> bool {
    keys.iter().any(|key| {
        bundle.get(*key).and_then(Value::as_bool) == Some(true)
            || receipt.get(*key).and_then(Value::as_bool) == Some(true)
    })
}

fn metadata_text_contains(bundle: &Value, receipt: &Value, needle: &str) -> bool {
    [bundle, receipt].into_iter().any(|value| {
        ["source", "sourceKind", "executionMode", "laneMode"]
            .into_iter()
            .filter_map(|key| value.get(key).and_then(Value::as_str))
            .any(|text| text.to_ascii_lowercase().contains(needle))
    })
}

fn contains_freshness_intent(text: &str) -> bool {
    contains_any(
        text,
        &[
            "search the web",
            "web search",
            "browse the web",
            "browse online",
            "look up online",
            "verify online",
            "latest",
            "today",
            "current price",
            "current rules",
            "current release",
            "up to date",
            "up-to-date",
            "搜尋網路",
            "上網查",
            "最新",
            "今天",
            "目前價格",
            "現行規則",
        ],
    )
}

fn contains_any(text: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| text.contains(needle))
}

fn public_http_domain(url: &str) -> Option<String> {
    let rest = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))?;
    let authority = rest.split('/').next()?.split('@').next_back()?;
    let host = authority.split(':').next()?.trim().to_ascii_lowercase();
    if host.is_empty()
        || host == "localhost"
        || host.starts_with("127.")
        || host.starts_with("10.")
        || host.starts_with("192.168.")
        || host.ends_with(".local")
        || host.ends_with(".internal")
    {
        None
    } else {
        Some(host)
    }
}

fn count_citation_refs(value: &Value) -> usize {
    match value {
        Value::Object(object) => object
            .iter()
            .map(|(key, value)| {
                usize::from(
                    key.eq_ignore_ascii_case("citations")
                        || key.eq_ignore_ascii_case("citationRefs"),
                ) * value.as_array().map(Vec::len).unwrap_or(0)
                    + count_citation_refs(value)
            })
            .sum(),
        Value::Array(values) => values.iter().map(count_citation_refs).sum(),
        _ => 0,
    }
}

fn digest_text(value: &str) -> String {
    digest_bytes(value.as_bytes())
}

fn now_ms() -> i64 {
    current_log_time_ms().unwrap_or(0)
}

fn digest_bytes(value: &[u8]) -> String {
    let bytes = digest(&SHA256, value);
    let mut output = String::with_capacity(7 + bytes.as_ref().len() * 2);
    output.push_str("sha256:");
    for byte in bytes.as_ref() {
        use std::fmt::Write as _;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn classifier_is_explicit_and_sensitive_offline_replay_win() {
        let root = std::env::temp_dir().join("agent-harness-web-search-classifier");
        let ordinary = plan_codex_web_search(
            &root,
            Some("Explain the architecture"),
            &json!({}),
            &json!({}),
            Some("lane-a"),
            Some("openai"),
        )
        .unwrap();
        assert_eq!(ordinary.requested_mode, CodexWebSearchMode::Cached);

        let fresh = plan_codex_web_search(
            &root,
            Some("Verify online the latest release"),
            &json!({}),
            &json!({}),
            Some("lane-a"),
            Some("openai"),
        )
        .unwrap();
        assert_eq!(fresh.requested_mode, CodexWebSearchMode::Live);

        for (message, metadata) in [
            ("search the web but do not browse", json!({})),
            ("latest private incident status", json!({"sensitive": true})),
            ("latest release", json!({"replay": true})),
        ] {
            let plan = plan_codex_web_search(
                &root,
                Some(message),
                &metadata,
                &json!({}),
                Some("lane-a"),
                Some("openai"),
            )
            .unwrap();
            assert_eq!(plan.requested_mode, CodexWebSearchMode::Disabled);
        }
    }

    #[test]
    fn capability_absence_degrades_explicitly_without_sandbox_input() {
        let plan = CodexWebSearchPlan {
            requested_mode: CodexWebSearchMode::Live,
            intent: CodexWebSearchIntent::Freshness,
            ..CodexWebSearchPlan::default()
        };
        let receipt = decide_effective_codex_web_search(
            &plan,
            unavailable_codex_web_search_capability("missing"),
            None,
            None,
        );
        assert_eq!(receipt.effective_mode, CodexWebSearchMode::Disabled);
        assert!(receipt.sandbox_independent);
        assert!(receipt.limitation_notice.unwrap().contains("Do not claim"));
    }

    #[test]
    fn action_receipt_hashes_query_and_omits_private_url() {
        let root = std::env::temp_dir().join(format!("agent-harness-web-action-{}", now_ms()));
        let receipt = decide_effective_codex_web_search(
            &CodexWebSearchPlan::default(),
            parse_codex_web_search_capability(&json!({
                "webSearch": true,
                "namespaceTools": true,
                "imageGeneration": false
            }))
            .unwrap(),
            None,
            None,
        );
        let mut observer = CodexWebSearchObserver::new(&root, receipt);
        observer
            .observe(
                &json!({
                    "method": "item/started",
                    "params": {
                        "threadId": "thread-a",
                        "turnId": "turn-a",
                        "item": {
                            "id": "item-a",
                            "type": "webSearch",
                            "query": "private prompt words",
                            "action": {"type": "openPage", "url": "http://localhost/private"}
                        }
                    }
                }),
                7,
            )
            .unwrap();
        let text = fs::read_to_string(root.join("codex-web-search-actions.v1.jsonl")).unwrap();
        assert!(!text.contains("private prompt words"));
        assert!(!text.contains("localhost"));
        assert!(text.contains("queryDigest"));
        let _ = fs::remove_dir_all(root);
    }
}
