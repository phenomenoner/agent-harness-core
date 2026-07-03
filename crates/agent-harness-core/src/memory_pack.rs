use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use ring::digest;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::logging::{append_jsonl_value, write_json_atomic};

pub const PACK_ARTIFACT_MARKER_PREFIX: &str = "<<ocm:artifact:v1:sha256:";
pub const PACK_ARTIFACT_MARKER_SUFFIX: &str = ">>";
pub const PACK_ARTIFACT_PUT_RECEIPT_SCHEMA: &str = "openclaw-mem.pack-artifact-put-receipt.v1";
pub const PACK_RETRIEVE_RECEIPT_SCHEMA: &str = "openclaw-mem.pack-retrieve-receipt.v1";
pub const PACK_RECEIPT_SCHEMA: &str = "openclaw-mem.pack-receipt.v1";
pub const PACK_OBSERVE_REPORT_SCHEMA: &str = "openclaw-mem.pack-observe-report.v1";
pub const PACK_CANARY_REPORT_SCHEMA: &str = "openclaw-mem.pack-canary-report.v1";

const DEFAULT_MIN_PACK_BYTES: usize = 4096;
const DEFAULT_MIN_PACK_TOKENS_ESTIMATE: usize = 1000;
const DEFAULT_MAX_ARTIFACT_BYTES: usize = 10 * 1024 * 1024;
const DEFAULT_MAX_STORE_BYTES_PER_SESSION: usize = 100 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackArtifactMarker {
    pub marker: String,
    pub artifact_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PackTtlPolicy {
    pub mode: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<i64>,
    pub max_artifact_bytes: usize,
    pub max_store_bytes_per_session: usize,
}

impl Default for PackTtlPolicy {
    fn default() -> Self {
        Self {
            mode: "session".to_string(),
            expires_at_ms: None,
            max_artifact_bytes: DEFAULT_MAX_ARTIFACT_BYTES,
            max_store_bytes_per_session: DEFAULT_MAX_STORE_BYTES_PER_SESSION,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PackArtifactMetadata {
    pub agent_id: String,
    pub session_key: String,
    pub source_kind: String,
    pub source_id: String,
    pub trust_level: String,
    pub scope: String,
    pub content_type: String,
    pub producer: String,
    pub command_or_tool: String,
    pub receipt_id: String,
    pub ttl_policy: PackTtlPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackArtifactPutOptions {
    pub harness_home: PathBuf,
    pub raw_bytes: Vec<u8>,
    pub metadata: PackArtifactMetadata,
    pub config: PackAdmissionConfig,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackArtifactRetrieveOptions {
    pub harness_home: PathBuf,
    pub marker_or_hash: String,
    pub agent_id: String,
    pub session_key: String,
    pub requester: String,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackCandidateOptions {
    pub harness_home: PathBuf,
    pub raw_bytes: Vec<u8>,
    pub metadata: PackArtifactMetadata,
    pub admission: PackAdmissionConfig,
    pub strategy_config: PackStrategyConfig,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PackAdmissionConfig {
    pub min_pack_bytes: usize,
    pub min_pack_tokens_estimate: usize,
    pub max_artifact_bytes: usize,
    pub max_store_bytes_per_session: usize,
}

impl PackAdmissionConfig {
    pub fn testing() -> Self {
        Self {
            min_pack_bytes: 0,
            min_pack_tokens_estimate: 0,
            max_artifact_bytes: 1024 * 1024,
            max_store_bytes_per_session: 10 * 1024 * 1024,
        }
    }
}

impl Default for PackAdmissionConfig {
    fn default() -> Self {
        Self {
            min_pack_bytes: DEFAULT_MIN_PACK_BYTES,
            min_pack_tokens_estimate: DEFAULT_MIN_PACK_TOKENS_ESTIMATE,
            max_artifact_bytes: DEFAULT_MAX_ARTIFACT_BYTES,
            max_store_bytes_per_session: DEFAULT_MAX_STORE_BYTES_PER_SESSION,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PackStrategyConfig {
    #[serde(default)]
    pub disabled_strategies: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PackArtifactPutReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub store_file: PathBuf,
    pub receipt_file: PathBuf,
    pub decision: String,
    pub reason: String,
    pub artifact_hash: String,
    pub marker: String,
    pub duplicate: bool,
    pub agent_id: String,
    pub session_key: String,
    pub source_kind: String,
    pub scope: String,
    pub trust_level: String,
    pub bytes_stored: u64,
    pub content_type: String,
    pub expires_at_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PackArtifactRetrieveReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub store_file: PathBuf,
    pub receipt_file: PathBuf,
    pub marker: String,
    pub artifact_hash: String,
    pub requester: String,
    pub agent_id: String,
    pub session_key: String,
    pub decision: String,
    pub bytes_returned: u64,
    #[serde(skip_serializing)]
    pub raw_bytes: Option<Vec<u8>>,
    pub latency_ms: u64,
    pub scope_decision: PackPolicyDecision,
    pub trust_decision: PackPolicyDecision,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PackPolicyDecision {
    pub allowed: bool,
    pub policy: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PackCandidateReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub decision: String,
    pub strategy: String,
    pub loss_mode: String,
    pub reason: String,
    pub prompt_text: String,
    pub marker: String,
    pub artifact_hash: String,
    pub tokens_before: u64,
    pub tokens_after: u64,
    pub bytes_before: u64,
    pub bytes_after: u64,
    pub latency_ms: u64,
    pub omission_summary: PackOmissionSummary,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PackOmissionSummary {
    pub rows_omitted: u64,
    pub lines_omitted: u64,
    pub fields_omitted: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PackCanary {
    pub schema: String,
    pub id: String,
    pub strategy: String,
    pub input_fixture: String,
    pub question: String,
    pub expected_signals: Vec<String>,
    pub must_retrieve: bool,
    pub allowed_deviation: String,
    pub disable_strategy_on_failure: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PackCanaryValidationReport {
    pub accepted: bool,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PackCanaryReport {
    pub schema: &'static str,
    pub all_green: bool,
    pub canaries_run: u64,
    pub failed: u64,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PackObserveReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub disabled_strategies: Vec<String>,
    pub pack_receipts: u64,
    pub packed: u64,
    pub pass_through: u64,
    pub blocked: u64,
    pub total_tokens_saved: u64,
    pub strategy_tokens_saved: BTreeMap<String, u64>,
    pub retrieval_returned: u64,
    pub retrieval_missing: u64,
    pub retrieval_expired: u64,
    pub retrieval_scope_denied: u64,
    pub retrieval_trust_denied: u64,
    pub latency_histogram: BTreeMap<String, u64>,
    pub canary_report: PackCanaryReport,
}

#[derive(Debug, Clone)]
struct StoredArtifact {
    raw_bytes: Vec<u8>,
    agent_id: String,
    session_key: String,
    trust_level: String,
    scope: String,
    expires_at_ms: Option<i64>,
}

pub fn parse_pack_artifact_marker(marker: &str) -> Result<PackArtifactMarker, String> {
    let Some(hash) = marker
        .strip_prefix(PACK_ARTIFACT_MARKER_PREFIX)
        .and_then(|value| value.strip_suffix(PACK_ARTIFACT_MARKER_SUFFIX))
    else {
        return Err("marker must use openclaw-mem artifact v1 sha256 envelope".to_string());
    };
    if hash.len() != 64 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("artifact marker requires a full 64-hex sha256 hash".to_string());
    }
    let hash = hash.to_ascii_lowercase();
    Ok(PackArtifactMarker {
        marker: pack_artifact_marker_for_hash(&format!("sha256:{hash}")),
        artifact_hash: format!("sha256:{hash}"),
    })
}

pub fn pack_artifact_hash_for_bytes(raw_bytes: &[u8]) -> String {
    let digest = digest::digest(&digest::SHA256, raw_bytes);
    let mut hex = String::with_capacity(64);
    for byte in digest.as_ref() {
        hex.push_str(&format!("{byte:02x}"));
    }
    format!("sha256:{hex}")
}

pub fn pack_artifact_store_file(harness_home: impl AsRef<Path>) -> PathBuf {
    pack_artifact_dir(harness_home).join("openclaw-mem-pack-artifacts.sqlite")
}

pub fn pack_artifact_put_receipts_file(harness_home: impl AsRef<Path>) -> PathBuf {
    pack_artifact_dir(harness_home).join("pack-artifact-put-receipts.jsonl")
}

pub fn pack_artifact_retrieve_receipts_file(harness_home: impl AsRef<Path>) -> PathBuf {
    pack_artifact_dir(harness_home).join("pack-artifact-retrieve-receipts.jsonl")
}

pub fn pack_receipts_file(harness_home: impl AsRef<Path>) -> PathBuf {
    pack_artifact_dir(harness_home).join("pack-receipts.jsonl")
}

pub fn pack_strategy_config_file(harness_home: impl AsRef<Path>) -> PathBuf {
    pack_artifact_dir(harness_home).join("strategy-config.json")
}

pub fn pack_observe_latest_file(harness_home: impl AsRef<Path>) -> PathBuf {
    pack_artifact_dir(harness_home).join("observe-last.json")
}

pub fn put_pack_artifact(options: PackArtifactPutOptions) -> io::Result<PackArtifactPutReport> {
    let started_at = current_time_ms();
    let store_file = pack_artifact_store_file(&options.harness_home);
    let receipt_file = pack_artifact_put_receipts_file(&options.harness_home);
    if let Some(parent) = store_file.parent() {
        fs::create_dir_all(parent)?;
    }
    let conn = Connection::open(&store_file).map_err(io::Error::other)?;
    init_pack_artifact_store(&conn)?;

    let artifact_hash = pack_artifact_hash_for_bytes(&options.raw_bytes);
    let marker = pack_artifact_marker_for_hash(&artifact_hash);
    let existing = existing_artifact_for_session(
        &conn,
        &artifact_hash,
        &options.metadata.agent_id,
        &options.metadata.session_key,
    )?;
    if existing {
        let report = put_report(
            options,
            store_file,
            receipt_file,
            "stored",
            "duplicate-existing-artifact",
            artifact_hash,
            marker,
            true,
            started_at,
        );
        append_jsonl_value(&report.receipt_file, &report)?;
        return Ok(report);
    }

    if options.raw_bytes.len() > options.config.max_artifact_bytes
        || options.raw_bytes.len() > options.metadata.ttl_policy.max_artifact_bytes
    {
        let report = put_report(
            options,
            store_file,
            receipt_file,
            "blocked",
            "admission-denied",
            artifact_hash,
            marker,
            false,
            started_at,
        );
        append_jsonl_value(&report.receipt_file, &report)?;
        return Ok(report);
    }

    let session_bytes = session_store_bytes(
        &conn,
        &options.metadata.agent_id,
        &options.metadata.session_key,
    )?;
    let max_session = options
        .config
        .max_store_bytes_per_session
        .min(options.metadata.ttl_policy.max_store_bytes_per_session);
    if session_bytes.saturating_add(options.raw_bytes.len() as u64) > max_session as u64 {
        let report = put_report(
            options,
            store_file,
            receipt_file,
            "blocked",
            "admission-denied",
            artifact_hash,
            marker,
            false,
            started_at,
        );
        append_jsonl_value(&report.receipt_file, &report)?;
        return Ok(report);
    }

    conn.execute(
        "INSERT INTO pack_artifacts (
            artifact_hash, agent_id, session_key, source_kind, source_id, trust_level, scope,
            created_at_ms, expires_at_ms, ttl_mode, content_type, producer, command_or_tool,
            receipt_id, byte_length, raw_bytes
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
        params![
            artifact_hash,
            options.metadata.agent_id,
            options.metadata.session_key,
            options.metadata.source_kind,
            options.metadata.source_id,
            options.metadata.trust_level,
            options.metadata.scope,
            options.now_ms,
            options.metadata.ttl_policy.expires_at_ms,
            options.metadata.ttl_policy.mode,
            options.metadata.content_type,
            options.metadata.producer,
            options.metadata.command_or_tool,
            options.metadata.receipt_id,
            options.raw_bytes.len() as i64,
            options.raw_bytes,
        ],
    )
    .map_err(io::Error::other)?;

    let report = put_report(
        options,
        store_file,
        receipt_file,
        "stored",
        "stored",
        artifact_hash,
        marker,
        false,
        started_at,
    );
    append_jsonl_value(&report.receipt_file, &report)?;
    Ok(report)
}

pub fn retrieve_pack_artifact(
    options: PackArtifactRetrieveOptions,
) -> io::Result<PackArtifactRetrieveReport> {
    let started_at = current_time_ms();
    let store_file = pack_artifact_store_file(&options.harness_home);
    let receipt_file = pack_artifact_retrieve_receipts_file(&options.harness_home);
    let artifact_hash = normalize_marker_or_hash(&options.marker_or_hash)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    let marker = pack_artifact_marker_for_hash(&artifact_hash);
    let mut report = PackArtifactRetrieveReport {
        schema: PACK_RETRIEVE_RECEIPT_SCHEMA,
        harness_home: options.harness_home.clone(),
        store_file: store_file.clone(),
        receipt_file,
        marker,
        artifact_hash: artifact_hash.clone(),
        requester: options.requester,
        agent_id: options.agent_id.clone(),
        session_key: options.session_key.clone(),
        decision: "missing".to_string(),
        bytes_returned: 0,
        raw_bytes: None,
        latency_ms: 0,
        scope_decision: PackPolicyDecision {
            allowed: false,
            policy: "missing".to_string(),
            reason: "artifact-not-found".to_string(),
        },
        trust_decision: PackPolicyDecision {
            allowed: false,
            policy: "missing".to_string(),
            reason: "artifact-not-found".to_string(),
        },
    };

    if let Some(parent) = store_file.parent() {
        fs::create_dir_all(parent)?;
    }
    let conn = Connection::open(&store_file).map_err(io::Error::other)?;
    init_pack_artifact_store(&conn)?;

    let candidates = read_artifact_candidates(&conn, &artifact_hash)?;
    let selection =
        select_retrieval_candidate_with_scope(&candidates, &options.agent_id, &options.session_key);
    let Some((candidate, scope)) = selection.candidate else {
        if let Some(scope_denial) = selection.scope_denial {
            report.decision = "scope-denied".to_string();
            report.scope_decision = scope_denial;
            report.trust_decision = PackPolicyDecision {
                allowed: false,
                policy: "not-evaluated".to_string(),
                reason: "scope-denied-before-trust".to_string(),
            };
        }
        finish_retrieve_report(&mut report, started_at)?;
        return Ok(report);
    };

    report.scope_decision = scope.clone();
    if !scope.allowed {
        report.decision = "scope-denied".to_string();
        report.trust_decision = PackPolicyDecision {
            allowed: false,
            policy: candidate.trust_level.clone(),
            reason: "scope-denied-before-trust".to_string(),
        };
        finish_retrieve_report(&mut report, started_at)?;
        return Ok(report);
    }

    if let Some(expires_at_ms) = candidate.expires_at_ms
        && expires_at_ms <= options.now_ms
    {
        report.decision = "expired".to_string();
        report.trust_decision = PackPolicyDecision {
            allowed: false,
            policy: candidate.trust_level.clone(),
            reason: "artifact-expired".to_string(),
        };
        finish_retrieve_report(&mut report, started_at)?;
        return Ok(report);
    }

    let trust = evaluate_trust(candidate);
    report.trust_decision = trust.clone();
    if !trust.allowed {
        report.decision = "trust-denied".to_string();
        finish_retrieve_report(&mut report, started_at)?;
        return Ok(report);
    }

    report.decision = "returned".to_string();
    report.bytes_returned = candidate.raw_bytes.len() as u64;
    report.raw_bytes = Some(candidate.raw_bytes.clone());
    finish_retrieve_report(&mut report, started_at)?;
    Ok(report)
}

pub fn pack_candidate(options: PackCandidateOptions) -> io::Result<PackCandidateReport> {
    let started_at = current_time_ms();
    let tokens_before = estimate_tokens(&options.raw_bytes);
    let raw_text = match String::from_utf8(options.raw_bytes.clone()) {
        Ok(text) => text,
        Err(_) => {
            return Ok(pack_candidate_receipt(
                &options,
                "pass-through",
                "pass-through",
                "pass-through",
                "unsafe-type",
                String::from_utf8_lossy(&options.raw_bytes).into_owned(),
                String::new(),
                String::new(),
                tokens_before,
                started_at,
                PackOmissionSummary::default(),
            )?);
        }
    };

    if options.raw_bytes.len() < options.admission.min_pack_bytes
        && tokens_before < options.admission.min_pack_tokens_estimate as u64
    {
        return pack_candidate_receipt(
            &options,
            "pass-through",
            "pass-through",
            "pass-through",
            "under-threshold",
            raw_text,
            String::new(),
            String::new(),
            tokens_before,
            started_at,
            PackOmissionSummary::default(),
        );
    }

    let strategy = select_strategy(&options.metadata, &raw_text);
    if options
        .strategy_config
        .disabled_strategies
        .iter()
        .any(|disabled| disabled == &strategy)
    {
        return pack_candidate_receipt(
            &options,
            "pass-through",
            strategy,
            "pass-through",
            "strategy-disabled",
            raw_text,
            String::new(),
            String::new(),
            tokens_before,
            started_at,
            PackOmissionSummary::default(),
        );
    }

    if strategy == "pass-through" {
        return pack_candidate_receipt(
            &options,
            "pass-through",
            strategy,
            "pass-through",
            "unsafe-type",
            raw_text,
            String::new(),
            String::new(),
            tokens_before,
            started_at,
            PackOmissionSummary::default(),
        );
    }

    let put = put_pack_artifact(PackArtifactPutOptions {
        harness_home: options.harness_home.clone(),
        raw_bytes: options.raw_bytes.clone(),
        metadata: options.metadata.clone(),
        config: options.admission.clone(),
        now_ms: options.now_ms,
    })?;
    if put.decision != "stored" {
        return pack_candidate_receipt(
            &options,
            "blocked",
            strategy,
            "pass-through",
            "admission-denied",
            raw_text,
            put.marker,
            put.artifact_hash,
            tokens_before,
            started_at,
            PackOmissionSummary::default(),
        );
    }

    let packed = match strategy.as_str() {
        "json-shape-v1" => pack_json_shape(&raw_text, &put.marker),
        "log-anomaly-v1" => pack_log_anomaly(&raw_text, &put.marker),
        "search-results-v1" => pack_search_results(&raw_text, &put.marker),
        _ => None,
    };
    let Some((prompt_text, omission_summary)) = packed else {
        return pack_candidate_receipt(
            &options,
            "pass-through",
            "pass-through",
            "pass-through",
            "unsafe-type",
            raw_text,
            put.marker,
            put.artifact_hash,
            tokens_before,
            started_at,
            PackOmissionSummary::default(),
        );
    };

    pack_candidate_receipt(
        &options,
        "packed",
        strategy,
        "structure-preserving",
        "stored-and-packed",
        prompt_text,
        put.marker,
        put.artifact_hash,
        tokens_before,
        started_at,
        omission_summary,
    )
}

pub fn validate_pack_canary_schema(canary: &PackCanary) -> PackCanaryValidationReport {
    let mut warnings = Vec::new();
    if canary.schema != "openclaw-mem.pack-canary.v1" {
        warnings.push("unsupported pack canary schema".to_string());
    }
    if canary.id.trim().is_empty() {
        warnings.push("pack canary missing id".to_string());
    }
    if canary.strategy.trim().is_empty() {
        warnings.push("pack canary missing strategy".to_string());
    }
    if canary.input_fixture.is_empty() {
        warnings.push("pack canary missing input fixture".to_string());
    }
    if canary.question.trim().is_empty() {
        warnings.push("pack canary missing question".to_string());
    }
    if canary.expected_signals.is_empty()
        || canary
            .expected_signals
            .iter()
            .any(|signal| signal.trim().is_empty())
    {
        warnings.push("pack canary missing expected signals".to_string());
    }
    PackCanaryValidationReport {
        accepted: warnings.is_empty(),
        warnings,
    }
}

pub fn write_pack_strategy_config(
    harness_home: impl AsRef<Path>,
    config: &PackStrategyConfig,
) -> io::Result<()> {
    write_json_atomic(&pack_strategy_config_file(harness_home), config)
}

pub fn read_pack_strategy_config(harness_home: impl AsRef<Path>) -> io::Result<PackStrategyConfig> {
    let path = pack_strategy_config_file(harness_home);
    match fs::read_to_string(&path) {
        Ok(text) => serde_json::from_str(&text).map_err(io::Error::other),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(PackStrategyConfig::default()),
        Err(error) => Err(error),
    }
}

pub fn collect_pack_observe_report(
    harness_home: impl AsRef<Path>,
) -> io::Result<PackObserveReport> {
    let harness_home = harness_home.as_ref().to_path_buf();
    let config = read_pack_strategy_config(&harness_home)?;
    let mut report = PackObserveReport {
        schema: PACK_OBSERVE_REPORT_SCHEMA,
        harness_home: harness_home.clone(),
        disabled_strategies: config.disabled_strategies,
        pack_receipts: 0,
        packed: 0,
        pass_through: 0,
        blocked: 0,
        total_tokens_saved: 0,
        strategy_tokens_saved: BTreeMap::new(),
        retrieval_returned: 0,
        retrieval_missing: 0,
        retrieval_expired: 0,
        retrieval_scope_denied: 0,
        retrieval_trust_denied: 0,
        latency_histogram: BTreeMap::new(),
        canary_report: PackCanaryReport {
            schema: PACK_CANARY_REPORT_SCHEMA,
            all_green: true,
            canaries_run: 0,
            failed: 0,
            warnings: Vec::new(),
        },
    };

    for value in read_jsonl_values(&pack_receipts_file(&harness_home))? {
        report.pack_receipts += 1;
        match value.get("decision").and_then(Value::as_str).unwrap_or("") {
            "packed" => report.packed += 1,
            "blocked" => report.blocked += 1,
            _ => report.pass_through += 1,
        }
        let before = value
            .get("tokensBefore")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let after = value
            .get("tokensAfter")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let saved = before.saturating_sub(after);
        report.total_tokens_saved = report.total_tokens_saved.saturating_add(saved);
        let strategy = value
            .get("strategy")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string();
        *report.strategy_tokens_saved.entry(strategy).or_default() += saved;
        add_latency_bucket(
            &mut report.latency_histogram,
            value.get("latencyMs").and_then(Value::as_u64).unwrap_or(0),
        );
    }

    for value in read_jsonl_values(&pack_artifact_retrieve_receipts_file(&harness_home))? {
        match value.get("decision").and_then(Value::as_str).unwrap_or("") {
            "returned" => report.retrieval_returned += 1,
            "missing" => report.retrieval_missing += 1,
            "expired" => report.retrieval_expired += 1,
            "scope-denied" => report.retrieval_scope_denied += 1,
            "trust-denied" => report.retrieval_trust_denied += 1,
            _ => {}
        }
        add_latency_bucket(
            &mut report.latency_histogram,
            value.get("latencyMs").and_then(Value::as_u64).unwrap_or(0),
        );
    }

    write_json_atomic(&pack_observe_latest_file(&harness_home), &report)?;
    Ok(report)
}

fn pack_artifact_dir(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("memory")
        .join("pack-artifacts")
}

fn pack_artifact_marker_for_hash(artifact_hash: &str) -> String {
    let hash = artifact_hash
        .strip_prefix("sha256:")
        .unwrap_or(artifact_hash);
    format!("{PACK_ARTIFACT_MARKER_PREFIX}{hash}{PACK_ARTIFACT_MARKER_SUFFIX}")
}

fn normalize_marker_or_hash(value: &str) -> Result<String, String> {
    if value.starts_with(PACK_ARTIFACT_MARKER_PREFIX) {
        return parse_pack_artifact_marker(value).map(|marker| marker.artifact_hash);
    }
    let Some(hash) = value.strip_prefix("sha256:") else {
        return Err("artifact lookup requires full marker or sha256:<64-hex>".to_string());
    };
    if hash.len() != 64 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("artifact lookup requires full 64-hex sha256".to_string());
    }
    Ok(format!("sha256:{}", hash.to_ascii_lowercase()))
}

fn init_pack_artifact_store(conn: &Connection) -> io::Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS pack_artifacts (
            artifact_hash TEXT NOT NULL,
            agent_id TEXT NOT NULL,
            session_key TEXT NOT NULL,
            source_kind TEXT NOT NULL,
            source_id TEXT NOT NULL,
            trust_level TEXT NOT NULL,
            scope TEXT NOT NULL,
            created_at_ms INTEGER NOT NULL,
            expires_at_ms INTEGER,
            ttl_mode TEXT NOT NULL,
            content_type TEXT NOT NULL,
            producer TEXT NOT NULL,
            command_or_tool TEXT NOT NULL,
            receipt_id TEXT NOT NULL,
            byte_length INTEGER NOT NULL,
            raw_bytes BLOB NOT NULL,
            PRIMARY KEY (artifact_hash, agent_id, session_key)
        );
        CREATE INDEX IF NOT EXISTS idx_pack_artifacts_hash
            ON pack_artifacts(artifact_hash);
        CREATE INDEX IF NOT EXISTS idx_pack_artifacts_session
            ON pack_artifacts(agent_id, session_key);
        ",
    )
    .map_err(io::Error::other)
}

fn existing_artifact_for_session(
    conn: &Connection,
    artifact_hash: &str,
    agent_id: &str,
    session_key: &str,
) -> io::Result<bool> {
    let found = conn
        .query_row(
            "SELECT 1 FROM pack_artifacts
             WHERE artifact_hash = ?1 AND agent_id = ?2 AND session_key = ?3
             LIMIT 1",
            params![artifact_hash, agent_id, session_key],
            |_| Ok(()),
        )
        .optional()
        .map_err(io::Error::other)?
        .is_some();
    Ok(found)
}

fn session_store_bytes(conn: &Connection, agent_id: &str, session_key: &str) -> io::Result<u64> {
    let bytes: Option<i64> = conn
        .query_row(
            "SELECT SUM(byte_length) FROM pack_artifacts WHERE agent_id = ?1 AND session_key = ?2",
            params![agent_id, session_key],
            |row| row.get(0),
        )
        .map_err(io::Error::other)?;
    Ok(bytes.unwrap_or(0).max(0) as u64)
}

fn put_report(
    options: PackArtifactPutOptions,
    store_file: PathBuf,
    receipt_file: PathBuf,
    decision: &str,
    reason: &str,
    artifact_hash: String,
    marker: String,
    duplicate: bool,
    _started_at: i64,
) -> PackArtifactPutReport {
    PackArtifactPutReport {
        schema: PACK_ARTIFACT_PUT_RECEIPT_SCHEMA,
        harness_home: options.harness_home,
        store_file,
        receipt_file,
        decision: decision.to_string(),
        reason: reason.to_string(),
        artifact_hash,
        marker,
        duplicate,
        agent_id: options.metadata.agent_id,
        session_key: options.metadata.session_key,
        source_kind: options.metadata.source_kind,
        scope: options.metadata.scope,
        trust_level: options.metadata.trust_level,
        bytes_stored: if decision == "stored" {
            options.raw_bytes.len() as u64
        } else {
            0
        },
        content_type: options.metadata.content_type,
        expires_at_ms: options.metadata.ttl_policy.expires_at_ms,
    }
}

fn read_artifact_candidates(
    conn: &Connection,
    artifact_hash: &str,
) -> io::Result<Vec<StoredArtifact>> {
    let mut statement = conn
        .prepare(
            "SELECT raw_bytes, agent_id, session_key, trust_level, scope, expires_at_ms
             FROM pack_artifacts
             WHERE artifact_hash = ?1
             ORDER BY created_at_ms ASC",
        )
        .map_err(io::Error::other)?;
    let rows = statement
        .query_map([artifact_hash], |row| {
            Ok(StoredArtifact {
                raw_bytes: row.get(0)?,
                agent_id: row.get(1)?,
                session_key: row.get(2)?,
                trust_level: row.get(3)?,
                scope: row.get(4)?,
                expires_at_ms: row.get(5)?,
            })
        })
        .map_err(io::Error::other)?;
    let mut artifacts = Vec::new();
    for row in rows {
        artifacts.push(row.map_err(io::Error::other)?);
    }
    Ok(artifacts)
}

struct RetrievalCandidateSelection<'a> {
    candidate: Option<(&'a StoredArtifact, PackPolicyDecision)>,
    scope_denial: Option<PackPolicyDecision>,
}

fn select_retrieval_candidate_with_scope<'a>(
    candidates: &'a [StoredArtifact],
    agent_id: &str,
    session_key: &str,
) -> RetrievalCandidateSelection<'a> {
    let mut first_denial = None;
    let mut broad_candidate = None;

    for candidate in candidates {
        let scope = evaluate_scope(candidate, agent_id, session_key);
        if !scope.allowed {
            if first_denial.is_none() {
                first_denial = Some(scope);
            }
            continue;
        }
        if candidate.agent_id == agent_id && candidate.session_key == session_key {
            return RetrievalCandidateSelection {
                candidate: Some((candidate, scope)),
                scope_denial: first_denial,
            };
        }
        if matches!(candidate.scope.as_str(), "global-imported" | "project")
            && broad_candidate.is_none()
        {
            broad_candidate = Some((candidate, scope));
        }
    }

    RetrievalCandidateSelection {
        candidate: broad_candidate,
        scope_denial: first_denial,
    }
}

#[cfg(test)]
fn select_retrieval_candidate<'a>(
    candidates: &'a [StoredArtifact],
    agent_id: &str,
    session_key: &str,
) -> Option<&'a StoredArtifact> {
    select_retrieval_candidate_with_scope(candidates, agent_id, session_key)
        .candidate
        .map(|(candidate, _)| candidate)
}

fn evaluate_scope(
    candidate: &StoredArtifact,
    agent_id: &str,
    session_key: &str,
) -> PackPolicyDecision {
    match candidate.scope.as_str() {
        "agent-private" => {
            if !is_lane_qualified_session_key_for_agent(&candidate.session_key, &candidate.agent_id)
            {
                return PackPolicyDecision {
                    allowed: false,
                    policy: "agent-private".to_string(),
                    reason: "non-lane-qualified-session-key-denied".to_string(),
                };
            }
            let allowed = candidate.agent_id == agent_id && candidate.session_key == session_key;
            PackPolicyDecision {
                allowed,
                policy: "agent-private".to_string(),
                reason: if allowed {
                    "same-agent-session".to_string()
                } else {
                    "cross-agent-private-denied".to_string()
                },
            }
        }
        "session" => {
            if !is_lane_qualified_session_key_for_agent(&candidate.session_key, &candidate.agent_id)
            {
                return PackPolicyDecision {
                    allowed: false,
                    policy: "session".to_string(),
                    reason: "non-lane-qualified-session-key-denied".to_string(),
                };
            }
            let allowed = candidate.agent_id == agent_id && candidate.session_key == session_key;
            PackPolicyDecision {
                allowed,
                policy: "session".to_string(),
                reason: if allowed {
                    "same-agent-session".to_string()
                } else if candidate.agent_id != agent_id {
                    "cross-agent-session-denied".to_string()
                } else {
                    "cross-session-denied".to_string()
                },
            }
        }
        "global-imported" | "project" => PackPolicyDecision {
            allowed: true,
            policy: candidate.scope.clone(),
            reason: "scope-allowed".to_string(),
        },
        other => PackPolicyDecision {
            allowed: false,
            policy: other.to_string(),
            reason: "unknown-scope-denied".to_string(),
        },
    }
}

fn is_lane_qualified_session_key_for_agent(session_key: &str, agent_id: &str) -> bool {
    let root = if let Some((root, suffix)) = session_key.rsplit_once(":cont-")
        && !suffix.is_empty()
        && suffix.chars().all(|ch| ch.is_ascii_digit())
    {
        root
    } else {
        session_key
    };
    let mut parts = root.split(':');
    let (Some(platform), Some(channel), Some(user), Some(agent)) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return false;
    };
    [platform, channel, user, agent]
        .iter()
        .all(|part| !part.trim().is_empty())
        && agent == agent_id
}

fn evaluate_trust(candidate: &StoredArtifact) -> PackPolicyDecision {
    let allowed = matches!(
        candidate.trust_level.as_str(),
        "trusted" | "global-imported" | "user-provided" | "tool-output"
    );
    PackPolicyDecision {
        allowed,
        policy: "conservative-snapshot-default".to_string(),
        reason: if allowed {
            "trusted-source".to_string()
        } else {
            "unknown-trust-denied".to_string()
        },
    }
}

fn finish_retrieve_report(
    report: &mut PackArtifactRetrieveReport,
    started_at: i64,
) -> io::Result<()> {
    report.latency_ms = current_time_ms().saturating_sub(started_at) as u64;
    append_jsonl_value(&report.receipt_file, report)
}

fn select_strategy(metadata: &PackArtifactMetadata, raw_text: &str) -> String {
    if metadata.source_kind == "search-results" {
        return "search-results-v1".to_string();
    }
    if metadata.source_kind == "log" {
        return "log-anomaly-v1".to_string();
    }
    if metadata.content_type.contains("json") && serde_json::from_str::<Value>(raw_text).is_ok() {
        return "json-shape-v1".to_string();
    }
    "pass-through".to_string()
}

fn pack_candidate_receipt(
    options: &PackCandidateOptions,
    decision: &str,
    strategy: impl Into<String>,
    loss_mode: &str,
    reason: &str,
    prompt_text: String,
    marker: String,
    artifact_hash: String,
    tokens_before: u64,
    started_at: i64,
    omission_summary: PackOmissionSummary,
) -> io::Result<PackCandidateReport> {
    let tokens_after = estimate_tokens(prompt_text.as_bytes());
    let report = PackCandidateReport {
        schema: PACK_RECEIPT_SCHEMA,
        harness_home: options.harness_home.clone(),
        decision: decision.to_string(),
        strategy: strategy.into(),
        loss_mode: loss_mode.to_string(),
        reason: reason.to_string(),
        bytes_before: options.raw_bytes.len() as u64,
        bytes_after: prompt_text.len() as u64,
        prompt_text,
        marker,
        artifact_hash,
        tokens_before,
        tokens_after,
        latency_ms: current_time_ms().saturating_sub(started_at) as u64,
        omission_summary,
    };
    append_jsonl_value(&pack_receipts_file(&options.harness_home), &report)?;
    Ok(report)
}

fn pack_json_shape(raw_text: &str, marker: &str) -> Option<(String, PackOmissionSummary)> {
    let value = serde_json::from_str::<Value>(raw_text).ok()?;
    let (rows, row_values) = json_row_values(&value);
    let fields = collect_json_fields(&row_values);
    let anomalies = row_values
        .iter()
        .filter_map(|row| json_anomaly_line(row))
        .collect::<Vec<_>>();
    let sample_first = row_values
        .iter()
        .take(3)
        .map(|row| escape_marker_like_text(&row.to_string()))
        .collect::<Vec<_>>();
    let sample_last = row_values
        .iter()
        .rev()
        .take(3)
        .map(|row| escape_marker_like_text(&row.to_string()))
        .collect::<Vec<_>>();
    let omitted = rows.saturating_sub(sample_first.len() + sample_last.len() + anomalies.len());
    let mut output = String::new();
    output.push_str("OpenClaw-mem packed JSON with strategy `json-shape-v1`.\n");
    output.push_str(&format!("- rows: {rows}\n"));
    if !fields.is_empty() {
        output.push_str(&format!("- fields: {}\n", fields.join(", ")));
    }
    if !anomalies.is_empty() {
        output.push_str("- anomalies:\n");
        for anomaly in anomalies.iter().take(12) {
            output.push_str(&format!("  - {anomaly}\n"));
        }
    }
    if !sample_first.is_empty() {
        output.push_str(&format!("- first rows: {}\n", sample_first.join(" | ")));
    }
    if !sample_last.is_empty() {
        output.push_str(&format!("- last rows: {}\n", sample_last.join(" | ")));
    }
    output.push_str(&format!("- omitted rows: {omitted}\n"));
    output.push_str(&format!("- exact raw artifact: {marker}\n"));
    Some((
        output,
        PackOmissionSummary {
            rows_omitted: omitted as u64,
            lines_omitted: 0,
            fields_omitted: Vec::new(),
        },
    ))
}

fn json_row_values(value: &Value) -> (usize, Vec<&Value>) {
    if let Some(rows) = value.as_array() {
        return (rows.len(), rows.iter().collect());
    }
    if let Some(rows) = value.get("rows").and_then(Value::as_array) {
        return (rows.len(), rows.iter().collect());
    }
    if let Some(items) = value.get("items").and_then(Value::as_array) {
        return (items.len(), items.iter().collect());
    }
    if let Some(matches) = value.get("matches").and_then(Value::as_array) {
        return (matches.len(), matches.iter().collect());
    }
    (1, vec![value])
}

fn collect_json_fields(rows: &[&Value]) -> Vec<String> {
    let mut fields = Vec::new();
    for row in rows {
        let Some(object) = row.as_object() else {
            continue;
        };
        for key in object.keys() {
            if !fields.iter().any(|field| field == key) {
                fields.push(key.clone());
            }
        }
    }
    fields
}

fn json_anomaly_line(row: &Value) -> Option<String> {
    let object = row.as_object()?;
    let text = row.to_string().to_ascii_lowercase();
    if !(text.contains("error")
        || text.contains("failed")
        || text.contains("exception")
        || text.contains("panic"))
    {
        return None;
    }
    let id = object
        .get("id")
        .map(json_scalar)
        .unwrap_or_else(|| "?".to_string());
    let status = object
        .get("status")
        .map(json_scalar)
        .unwrap_or_else(|| "?".to_string());
    let code = object
        .get("code")
        .map(json_scalar)
        .unwrap_or_else(|| "?".to_string());
    let latency = object
        .get("latency_ms")
        .or_else(|| object.get("latencyMs"))
        .map(json_scalar)
        .unwrap_or_else(|| "?".to_string());
    Some(format!(
        "row id={id} status={status} code={code} latency_ms={latency}"
    ))
}

fn json_scalar(value: &Value) -> String {
    match value {
        Value::String(value) => escape_marker_like_text(value),
        Value::Number(value) => value.to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Null => "null".to_string(),
        _ => escape_marker_like_text(&value.to_string()),
    }
}

fn pack_log_anomaly(raw_text: &str, marker: &str) -> Option<(String, PackOmissionSummary)> {
    let lines = raw_text.lines().collect::<Vec<_>>();
    if lines.is_empty() {
        return None;
    }
    let anomaly_lines = lines
        .iter()
        .enumerate()
        .filter(|(_, line)| {
            let lower = line.to_ascii_lowercase();
            lower.contains("error")
                || lower.contains("warn")
                || lower.contains("fail")
                || lower.contains("panic")
                || lower.contains("exception")
                || lower.contains("traceback")
        })
        .map(|(index, line)| format!("line {}: {}", index + 1, escape_marker_like_text(line)))
        .collect::<Vec<_>>();
    let first = lines
        .iter()
        .take(5)
        .map(|line| escape_marker_like_text(line))
        .collect::<Vec<_>>();
    let last = lines
        .iter()
        .rev()
        .take(5)
        .map(|line| escape_marker_like_text(line))
        .collect::<Vec<_>>();
    let kept = first.len() + last.len() + anomaly_lines.len();
    let omitted = lines.len().saturating_sub(kept);
    let mut output = String::new();
    output.push_str("OpenClaw-mem packed log with strategy `log-anomaly-v1`.\n");
    output.push_str(&format!("- total lines: {}\n", lines.len()));
    output.push_str(&format!("- omitted lines: {omitted}\n"));
    output.push_str(&format!("- first lines: {}\n", first.join(" | ")));
    output.push_str(&format!("- last lines: {}\n", last.join(" | ")));
    if !anomaly_lines.is_empty() {
        output.push_str("- anomalies:\n");
        for line in anomaly_lines.iter().take(24) {
            output.push_str(&format!("  - {line}\n"));
        }
    }
    output.push_str(&format!("- exact raw artifact: {marker}\n"));
    Some((
        output,
        PackOmissionSummary {
            rows_omitted: 0,
            lines_omitted: omitted as u64,
            fields_omitted: Vec::new(),
        },
    ))
}

fn pack_search_results(raw_text: &str, marker: &str) -> Option<(String, PackOmissionSummary)> {
    let value = serde_json::from_str::<Value>(raw_text).ok()?;
    let query = value
        .get("query")
        .and_then(Value::as_str)
        .unwrap_or("(unknown)");
    let matches = value
        .get("matches")
        .or_else(|| value.get("results"))
        .and_then(Value::as_array)?;
    let total = value
        .get("totalMatches")
        .or_else(|| value.get("total_matches"))
        .and_then(Value::as_u64)
        .unwrap_or(matches.len() as u64);
    let mut histogram: BTreeMap<String, u64> = BTreeMap::new();
    let mut top_paths = Vec::new();
    let mut first_matches = Vec::new();
    for item in matches {
        let path = item
            .get("path")
            .or_else(|| item.get("file"))
            .and_then(Value::as_str)
            .unwrap_or("(unknown)");
        *histogram.entry(path.to_string()).or_default() += 1;
        if !top_paths.iter().any(|existing| existing == path) {
            top_paths.push(path.to_string());
        }
        if first_matches.len() < 8 {
            let line = item.get("line").and_then(Value::as_u64).unwrap_or(0);
            let text = item
                .get("text")
                .or_else(|| item.get("snippet"))
                .and_then(Value::as_str)
                .unwrap_or("");
            first_matches.push(format!(
                "{}:{} {}",
                path,
                line,
                escape_marker_like_text(text)
            ));
        }
    }
    let omitted = matches.len().saturating_sub(first_matches.len());
    let histogram_text = histogram
        .iter()
        .map(|(path, count)| format!("{path}={count}"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut output = String::new();
    output.push_str("OpenClaw-mem packed search results with strategy `search-results-v1`.\n");
    output.push_str(&format!("- query: {}\n", escape_marker_like_text(query)));
    output.push_str(&format!("- total matches: {total}\n"));
    output.push_str(&format!(
        "- top paths: {}\n",
        top_paths
            .iter()
            .take(8)
            .cloned()
            .collect::<Vec<_>>()
            .join(", ")
    ));
    output.push_str(&format!("- path histogram: {histogram_text}\n"));
    output.push_str(&format!("- first matches: {}\n", first_matches.join(" | ")));
    output.push_str(&format!("- omitted matches: {omitted}\n"));
    output.push_str(&format!("- exact raw artifact: {marker}\n"));
    Some((
        output,
        PackOmissionSummary {
            rows_omitted: omitted as u64,
            lines_omitted: 0,
            fields_omitted: Vec::new(),
        },
    ))
}

fn escape_marker_like_text(text: &str) -> String {
    text.replace("<<ocm:artifact:", "[escaped ocm artifact marker:")
}

fn estimate_tokens(bytes: &[u8]) -> u64 {
    (bytes.len() as u64).div_ceil(4).max(1)
}

fn current_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
        .unwrap_or(0)
}

fn read_jsonl_values(path: &Path) -> io::Result<Vec<Value>> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut values = Vec::new();
    for line in text.lines().map(str::trim).filter(|line| !line.is_empty()) {
        if let Ok(value) = serde_json::from_str::<Value>(line) {
            values.push(value);
        }
    }
    Ok(values)
}

fn add_latency_bucket(histogram: &mut BTreeMap<String, u64>, latency_ms: u64) {
    let bucket = match latency_ms {
        0..=10 => "0-10ms",
        11..=100 => "11-100ms",
        101..=1000 => "101-1000ms",
        _ => "1000ms+",
    };
    *histogram.entry(bucket.to_string()).or_default() += 1;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_harness_home(name: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "agent-harness-memory-pack-{name}-{}-{}",
            std::process::id(),
            current_time_ms()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("create temp harness home");
        root
    }

    fn stored(agent_id: &str, session_key: &str, scope: &str) -> StoredArtifact {
        StoredArtifact {
            raw_bytes: format!("{agent_id}:{session_key}:{scope}").into_bytes(),
            agent_id: agent_id.to_string(),
            session_key: session_key.to_string(),
            trust_level: "trusted".to_string(),
            scope: scope.to_string(),
            expires_at_ms: None,
        }
    }

    fn metadata(agent_id: &str, session_key: &str, scope: &str) -> PackArtifactMetadata {
        PackArtifactMetadata {
            agent_id: agent_id.to_string(),
            session_key: session_key.to_string(),
            source_kind: "test".to_string(),
            source_id: format!("{agent_id}:{session_key}:{scope}"),
            trust_level: "trusted".to_string(),
            scope: scope.to_string(),
            content_type: "text/plain".to_string(),
            producer: "memory-pack-test".to_string(),
            command_or_tool: "unit-test".to_string(),
            receipt_id: format!("receipt:{agent_id}:{session_key}:{scope}"),
            ttl_policy: PackTtlPolicy::default(),
        }
    }

    fn put_text(root: &Path, text: &str, agent_id: &str, session_key: &str, scope: &str) -> String {
        let report = put_pack_artifact(PackArtifactPutOptions {
            harness_home: root.to_path_buf(),
            raw_bytes: text.as_bytes().to_vec(),
            metadata: metadata(agent_id, session_key, scope),
            config: PackAdmissionConfig::testing(),
            now_ms: 1_000,
        })
        .expect("put pack artifact");
        assert_eq!(report.decision, "stored");
        report.artifact_hash
    }

    fn retrieve(
        root: &Path,
        artifact_hash: &str,
        agent_id: &str,
        session_key: &str,
    ) -> PackArtifactRetrieveReport {
        retrieve_pack_artifact(PackArtifactRetrieveOptions {
            harness_home: root.to_path_buf(),
            marker_or_hash: artifact_hash.to_string(),
            agent_id: agent_id.to_string(),
            session_key: session_key.to_string(),
            requester: "unit-test".to_string(),
            now_ms: 2_000,
        })
        .expect("retrieve pack artifact")
    }

    #[test]
    fn retrieval_scope_session_requires_same_agent_and_session_key() {
        let candidate = stored("main", "telegram:dm:user:main:session-a", "session");
        let same = evaluate_scope(&candidate, "main", "telegram:dm:user:main:session-a");
        assert!(same.allowed);
        assert_eq!(same.reason, "same-agent-session");

        let cross_agent =
            evaluate_scope(&candidate, "public-bot", "telegram:dm:user:main:session-a");
        assert!(!cross_agent.allowed);
        assert_eq!(cross_agent.reason, "cross-agent-session-denied");
    }

    #[test]
    fn retrieval_scope_session_rejects_bare_session_key_even_when_equal() {
        let candidate = stored("main", "session-a", "session");

        let same_bare = evaluate_scope(&candidate, "main", "session-a");

        assert!(!same_bare.allowed);
        assert_eq!(same_bare.reason, "non-lane-qualified-session-key-denied");
    }

    #[test]
    fn retrieval_scope_agent_private_rejects_bare_session_key_even_when_equal() {
        let candidate = stored("main", "session-a", "agent-private");

        let same_bare = evaluate_scope(&candidate, "main", "session-a");

        assert!(!same_bare.allowed);
        assert_eq!(same_bare.reason, "non-lane-qualified-session-key-denied");
    }

    #[test]
    fn retrieval_candidate_does_not_fall_back_to_wrong_lane_concrete_history() {
        let candidates = vec![stored("main", "telegram:dm:user:main:session-a", "session")];

        let selected =
            select_retrieval_candidate(&candidates, "main", "discord:dm:user:main:session-b");

        assert!(selected.is_none());
    }

    #[test]
    fn retrieval_candidate_uses_project_after_wrong_lane_concrete_candidate() {
        let candidates = vec![
            stored("main", "telegram:dm:user:main:session-a", "session"),
            stored("main", "project:docs", "project"),
        ];

        let selected =
            select_retrieval_candidate(&candidates, "main", "discord:dm:user:main:session-b")
                .expect("explicit project scope should remain available");

        assert_eq!(selected.scope, "project");
        assert_eq!(selected.session_key, "project:docs");
    }

    #[test]
    fn retrieve_wrong_lane_concrete_history_reports_scope_denied_not_missing() {
        let root = temp_harness_home("wrong-lane-scope-denied");
        let artifact_hash = put_text(
            &root,
            "same hash concrete history",
            "main",
            "telegram:dm:user:main:session-a",
            "session",
        );

        let report = retrieve(
            &root,
            &artifact_hash,
            "main",
            "discord:dm:user:main:session-b",
        );

        assert_eq!(report.decision, "scope-denied");
        assert_eq!(report.bytes_returned, 0);
        assert!(report.raw_bytes.is_none());
        assert_eq!(report.scope_decision.reason, "cross-session-denied");
        assert_ne!(report.scope_decision.reason, "artifact-not-found");
    }

    #[test]
    fn retrieve_wrong_lane_concrete_then_project_returns_explicit_broad_scope() {
        let root = temp_harness_home("wrong-lane-project-fallback");
        let artifact_hash = put_text(
            &root,
            "same hash project-visible history",
            "main",
            "telegram:dm:user:main:session-a",
            "session",
        );
        let project_hash = put_text(
            &root,
            "same hash project-visible history",
            "main",
            "project:docs",
            "project",
        );
        assert_eq!(artifact_hash, project_hash);

        let report = retrieve(
            &root,
            &artifact_hash,
            "main",
            "discord:dm:user:main:session-b",
        );

        assert_eq!(report.decision, "returned");
        assert_eq!(report.scope_decision.policy, "project");
        assert_eq!(
            report.raw_bytes.as_deref(),
            Some(b"same hash project-visible history".as_slice())
        );
    }

    #[test]
    fn retrieve_wrong_lane_concrete_then_global_imported_returns_explicit_broad_scope() {
        let root = temp_harness_home("wrong-lane-global-fallback");
        let artifact_hash = put_text(
            &root,
            "same hash global-visible history",
            "main",
            "telegram:dm:user:main:session-a",
            "agent-private",
        );
        let global_hash = put_text(
            &root,
            "same hash global-visible history",
            "main",
            "global:imported",
            "global-imported",
        );
        assert_eq!(artifact_hash, global_hash);

        let report = retrieve(
            &root,
            &artifact_hash,
            "xiaoxiaoli",
            "telegram:dm:user:xiaoxiaoli:session-b",
        );

        assert_eq!(report.decision, "returned");
        assert_eq!(report.scope_decision.policy, "global-imported");
        assert_eq!(
            report.raw_bytes.as_deref(),
            Some(b"same hash global-visible history".as_slice())
        );
    }
}
