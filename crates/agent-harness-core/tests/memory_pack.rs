use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use agent_harness_core::{
    PackAdmissionConfig, PackArtifactMetadata, PackArtifactPutOptions, PackArtifactRetrieveOptions,
    PackCanary, PackCandidateOptions, PackStrategyConfig, PackTtlPolicy,
    collect_pack_observe_report, pack_artifact_hash_for_bytes, pack_artifact_put_receipts_file,
    pack_artifact_retrieve_receipts_file, pack_artifact_store_file, pack_candidate,
    parse_pack_artifact_marker, put_pack_artifact, retrieve_pack_artifact,
    validate_pack_canary_schema, write_pack_strategy_config,
};

const MAIN_SESSION_KEY: &str = "telegram:dm-42:user-7:main:session-1";
const OTHER_SESSION_KEY: &str = "telegram:dm-42:user-7:other:session-2";

#[test]
fn marker_parser_requires_exact_full_sha256_hashes() {
    let marker = concat!(
        "<<ocm:artifact:v1:sha256:",
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        ">>"
    );

    let parsed = parse_pack_artifact_marker(marker).expect("valid full-hash marker");

    assert_eq!(
        parsed.artifact_hash,
        "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    );
    assert!(parse_pack_artifact_marker("<<ocm:artifact:v1:sha256:0123>>").is_err());
    assert!(
        parse_pack_artifact_marker(
            "<<ocm:artifact:v1:sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdeg>>"
        )
        .is_err()
    );
}

#[test]
fn artifact_store_round_trips_exact_bytes_and_reuses_same_session_hash() {
    let root = temp_root("artifact_store_round_trips_exact_bytes_and_reuses_same_session_hash");
    let harness_home = root.join("harness");
    let raw = br#"{"rows":[{"id":1},{"id":2}],"secret":"do-not-log"}"#.to_vec();
    let metadata = metadata("main", MAIN_SESSION_KEY, "tool-output", "application/json");

    let first = put_pack_artifact(PackArtifactPutOptions {
        harness_home: harness_home.clone(),
        raw_bytes: raw.clone(),
        metadata: metadata.clone(),
        config: PackAdmissionConfig::testing(),
        now_ms: 1_800_000_000_000,
    })
    .expect("put artifact");
    let second = put_pack_artifact(PackArtifactPutOptions {
        harness_home: harness_home.clone(),
        raw_bytes: raw.clone(),
        metadata,
        config: PackAdmissionConfig::testing(),
        now_ms: 1_800_000_000_001,
    })
    .expect("put duplicate artifact");

    assert_eq!(first.decision, "stored");
    assert_eq!(first.artifact_hash, second.artifact_hash);
    assert!(!first.duplicate);
    assert!(second.duplicate);
    assert_eq!(pack_artifact_hash_for_bytes(&raw), first.artifact_hash);
    assert!(pack_artifact_store_file(&harness_home).is_file());

    let retrieved = retrieve_pack_artifact(PackArtifactRetrieveOptions {
        harness_home: harness_home.clone(),
        marker_or_hash: first.marker.clone(),
        agent_id: "main".to_string(),
        session_key: MAIN_SESSION_KEY.to_string(),
        requester: "operator".to_string(),
        now_ms: 1_800_000_000_002,
    })
    .expect("retrieve artifact");

    assert_eq!(retrieved.decision, "returned");
    assert_eq!(retrieved.raw_bytes.as_deref(), Some(raw.as_slice()));
    assert_eq!(retrieved.bytes_returned, raw.len() as u64);
    let put_receipts = fs::read_to_string(pack_artifact_put_receipts_file(&harness_home)).unwrap();
    let get_receipts =
        fs::read_to_string(pack_artifact_retrieve_receipts_file(&harness_home)).unwrap();
    assert!(put_receipts.contains("\"schema\":\"openclaw-mem.pack-artifact-put-receipt.v1\""));
    assert!(get_receipts.contains("\"decision\":\"returned\""));
    assert!(!put_receipts.contains("do-not-log"));
    assert!(!get_receipts.contains("do-not-log"));

    let _ = fs::remove_dir_all(root);
}

#[test]
fn artifact_retrieval_denials_emit_explicit_scope_trust_and_expiry_receipts() {
    let root =
        temp_root("artifact_retrieval_denials_emit_explicit_scope_trust_and_expiry_receipts");
    let harness_home = root.join("harness");

    let private = put_pack_artifact(PackArtifactPutOptions {
        harness_home: harness_home.clone(),
        raw_bytes: b"private raw".to_vec(),
        metadata: metadata("main", MAIN_SESSION_KEY, "tool-output", "text/plain"),
        config: PackAdmissionConfig::testing(),
        now_ms: 1_000,
    })
    .unwrap();
    let mut unknown = metadata("main", MAIN_SESSION_KEY, "tool-output", "text/plain");
    unknown.trust_level = "unknown".to_string();
    let unknown = put_pack_artifact(PackArtifactPutOptions {
        harness_home: harness_home.clone(),
        raw_bytes: b"unknown raw".to_vec(),
        metadata: unknown,
        config: PackAdmissionConfig::testing(),
        now_ms: 1_001,
    })
    .unwrap();
    let mut expiring = metadata("main", MAIN_SESSION_KEY, "log", "text/plain");
    expiring.ttl_policy = PackTtlPolicy {
        mode: "duration".to_string(),
        expires_at_ms: Some(1_050),
        max_artifact_bytes: 1024,
        max_store_bytes_per_session: 10 * 1024,
    };
    let expiring = put_pack_artifact(PackArtifactPutOptions {
        harness_home: harness_home.clone(),
        raw_bytes: b"expired raw".to_vec(),
        metadata: expiring,
        config: PackAdmissionConfig::testing(),
        now_ms: 1_002,
    })
    .unwrap();

    let scope = retrieve_pack_artifact(PackArtifactRetrieveOptions {
        harness_home: harness_home.clone(),
        marker_or_hash: private.marker,
        agent_id: "other".to_string(),
        session_key: OTHER_SESSION_KEY.to_string(),
        requester: "model".to_string(),
        now_ms: 1_010,
    })
    .unwrap();
    let trust = retrieve_pack_artifact(PackArtifactRetrieveOptions {
        harness_home: harness_home.clone(),
        marker_or_hash: unknown.marker,
        agent_id: "main".to_string(),
        session_key: MAIN_SESSION_KEY.to_string(),
        requester: "model".to_string(),
        now_ms: 1_011,
    })
    .unwrap();
    let expired = retrieve_pack_artifact(PackArtifactRetrieveOptions {
        harness_home: harness_home.clone(),
        marker_or_hash: expiring.marker,
        agent_id: "main".to_string(),
        session_key: MAIN_SESSION_KEY.to_string(),
        requester: "model".to_string(),
        now_ms: 1_051,
    })
    .unwrap();
    let missing = retrieve_pack_artifact(PackArtifactRetrieveOptions {
        harness_home: harness_home.clone(),
        marker_or_hash: concat!(
            "<<ocm:artifact:v1:sha256:",
            "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            ">>"
        )
        .to_string(),
        agent_id: "main".to_string(),
        session_key: MAIN_SESSION_KEY.to_string(),
        requester: "model".to_string(),
        now_ms: 1_052,
    })
    .unwrap();

    assert_eq!(scope.decision, "scope-denied");
    assert_eq!(trust.decision, "trust-denied");
    assert_eq!(expired.decision, "expired");
    assert_eq!(missing.decision, "missing");
    assert!(scope.raw_bytes.is_none());
    assert!(trust.raw_bytes.is_none());
    assert!(expired.raw_bytes.is_none());
    assert!(missing.raw_bytes.is_none());

    let receipts = fs::read_to_string(pack_artifact_retrieve_receipts_file(&harness_home)).unwrap();
    assert!(receipts.contains("\"decision\":\"scope-denied\""));
    assert!(receipts.contains("\"decision\":\"trust-denied\""));
    assert!(receipts.contains("\"decision\":\"expired\""));
    assert!(receipts.contains("\"decision\":\"missing\""));

    let _ = fs::remove_dir_all(root);
}

#[test]
fn pack_registry_shortens_json_log_and_search_results_but_retrieves_exact_originals() {
    let root = temp_root(
        "pack_registry_shortens_json_log_and_search_results_but_retrieves_exact_originals",
    );
    let harness_home = root.join("harness");

    let json_raw = json_rows_fixture(120);
    let json = pack_candidate(PackCandidateOptions {
        harness_home: harness_home.clone(),
        raw_bytes: json_raw.clone().into_bytes(),
        metadata: metadata("main", MAIN_SESSION_KEY, "tool-output", "application/json"),
        admission: PackAdmissionConfig::testing(),
        strategy_config: PackStrategyConfig::default(),
        now_ms: 2_000,
    })
    .unwrap();
    assert_eq!(json.decision, "packed");
    assert_eq!(json.strategy, "json-shape-v1");
    assert!(json.prompt_text.contains("rows: 120"));
    assert!(json.prompt_text.contains("AUTH_EXPIRED"));
    assert!(json.prompt_text.len() < json_raw.len());

    let log_raw = long_log_fixture();
    let log = pack_candidate(PackCandidateOptions {
        harness_home: harness_home.clone(),
        raw_bytes: log_raw.clone().into_bytes(),
        metadata: metadata("main", MAIN_SESSION_KEY, "log", "text/plain"),
        admission: PackAdmissionConfig::testing(),
        strategy_config: PackStrategyConfig::default(),
        now_ms: 2_001,
    })
    .unwrap();
    assert_eq!(log.strategy, "log-anomaly-v1");
    assert!(log.prompt_text.contains("panic"));
    assert!(log.prompt_text.contains("omitted lines:"));

    let search_raw = search_results_fixture();
    let search = pack_candidate(PackCandidateOptions {
        harness_home: harness_home.clone(),
        raw_bytes: search_raw.clone().into_bytes(),
        metadata: metadata(
            "main",
            MAIN_SESSION_KEY,
            "search-results",
            "application/json",
        ),
        admission: PackAdmissionConfig::testing(),
        strategy_config: PackStrategyConfig::default(),
        now_ms: 2_002,
    })
    .unwrap();
    assert_eq!(search.strategy, "search-results-v1");
    assert!(search.prompt_text.contains("query: memory pack"));
    assert!(search.prompt_text.contains("path histogram:"));

    for (marker, expected) in [
        (json.marker, json_raw.into_bytes()),
        (log.marker, log_raw.into_bytes()),
        (search.marker, search_raw.into_bytes()),
    ] {
        let retrieved = retrieve_pack_artifact(PackArtifactRetrieveOptions {
            harness_home: harness_home.clone(),
            marker_or_hash: marker,
            agent_id: "main".to_string(),
            session_key: MAIN_SESSION_KEY.to_string(),
            requester: "operator".to_string(),
            now_ms: 2_010,
        })
        .unwrap();
        assert_eq!(retrieved.decision, "returned");
        assert_eq!(retrieved.raw_bytes, Some(expected));
    }

    let _ = fs::remove_dir_all(root);
}

#[test]
fn canary_and_observe_reports_include_strategy_controls_and_retrieval_rates() {
    let root =
        temp_root("canary_and_observe_reports_include_strategy_controls_and_retrieval_rates");
    let harness_home = root.join("harness");
    write_pack_strategy_config(
        &harness_home,
        &PackStrategyConfig {
            disabled_strategies: vec!["log-anomaly-v1".to_string()],
        },
    )
    .unwrap();

    let disabled_log = pack_candidate(PackCandidateOptions {
        harness_home: harness_home.clone(),
        raw_bytes: long_log_fixture().into_bytes(),
        metadata: metadata("main", MAIN_SESSION_KEY, "log", "text/plain"),
        admission: PackAdmissionConfig::testing(),
        strategy_config: PackStrategyConfig {
            disabled_strategies: vec!["log-anomaly-v1".to_string()],
        },
        now_ms: 3_000,
    })
    .unwrap();
    assert_eq!(disabled_log.decision, "pass-through");
    assert_eq!(disabled_log.reason, "strategy-disabled");

    let valid_canary = PackCanary {
        schema: "openclaw-mem.pack-canary.v1".to_string(),
        id: "json-auth-error-001".to_string(),
        strategy: "json-shape-v1".to_string(),
        input_fixture: json_rows_fixture(80),
        question: "Which row contains the auth error and what code is reported?".to_string(),
        expected_signals: vec!["row id=67".to_string(), "AUTH_EXPIRED".to_string()],
        must_retrieve: false,
        allowed_deviation: "all expected signals present".to_string(),
        disable_strategy_on_failure: true,
    };
    assert!(validate_pack_canary_schema(&valid_canary).accepted);

    let json = pack_candidate(PackCandidateOptions {
        harness_home: harness_home.clone(),
        raw_bytes: json_rows_fixture(90).into_bytes(),
        metadata: metadata("main", MAIN_SESSION_KEY, "tool-output", "application/json"),
        admission: PackAdmissionConfig::testing(),
        strategy_config: PackStrategyConfig::default(),
        now_ms: 3_001,
    })
    .unwrap();
    let _ = retrieve_pack_artifact(PackArtifactRetrieveOptions {
        harness_home: harness_home.clone(),
        marker_or_hash: json.marker,
        agent_id: "main".to_string(),
        session_key: MAIN_SESSION_KEY.to_string(),
        requester: "model".to_string(),
        now_ms: 3_002,
    })
    .unwrap();
    let _ = retrieve_pack_artifact(PackArtifactRetrieveOptions {
        harness_home: harness_home.clone(),
        marker_or_hash: concat!(
            "<<ocm:artifact:v1:sha256:",
            "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
            ">>"
        )
        .to_string(),
        agent_id: "main".to_string(),
        session_key: MAIN_SESSION_KEY.to_string(),
        requester: "model".to_string(),
        now_ms: 3_003,
    })
    .unwrap();

    let observe = collect_pack_observe_report(&harness_home).unwrap();
    assert!(
        observe
            .disabled_strategies
            .contains(&"log-anomaly-v1".to_string())
    );
    assert!(observe.total_tokens_saved > 0);
    assert_eq!(observe.retrieval_returned, 1);
    assert_eq!(observe.retrieval_missing, 1);
    assert!(observe.canary_report.all_green);

    let _ = fs::remove_dir_all(root);
}

fn metadata(
    agent_id: &str,
    session_key: &str,
    source_kind: &str,
    content_type: &str,
) -> PackArtifactMetadata {
    PackArtifactMetadata {
        agent_id: agent_id.to_string(),
        session_key: session_key.to_string(),
        source_kind: source_kind.to_string(),
        source_id: format!("{source_kind}-fixture"),
        trust_level: "tool-output".to_string(),
        scope: "agent-private".to_string(),
        content_type: content_type.to_string(),
        producer: "tool".to_string(),
        command_or_tool: source_kind.to_string(),
        receipt_id: format!("receipt-{source_kind}"),
        ttl_policy: PackTtlPolicy {
            mode: "session".to_string(),
            expires_at_ms: None,
            max_artifact_bytes: 1024 * 1024,
            max_store_bytes_per_session: 10 * 1024 * 1024,
        },
    }
}

fn json_rows_fixture(rows: usize) -> String {
    let mut items = Vec::new();
    for id in 0..rows {
        let status = if id == 67 { "error" } else { "ok" };
        let code = if id == 67 { "AUTH_EXPIRED" } else { "OK" };
        items.push(serde_json::json!({
            "id": id,
            "status": status,
            "code": code,
            "latency_ms": if id == 67 { 912 } else { 20 + id }
        }));
    }
    serde_json::json!({ "rows": items }).to_string()
}

fn long_log_fixture() -> String {
    let mut lines = Vec::new();
    for index in 0..160 {
        if index == 81 {
            lines.push("ERROR panic while opening memory pack store".to_string());
        } else {
            lines.push(format!("INFO build line {index} completed"));
        }
    }
    lines.join("\n")
}

fn search_results_fixture() -> String {
    serde_json::json!({
        "query": "memory pack",
        "totalMatches": 3,
        "matches": [
            {"path":"crates/agent-harness-core/src/memory.rs","line":10,"text":"memory pack artifact"},
            {"path":"crates/agent-harness-core/src/memory.rs","line":20,"text":"pack marker"},
            {"path":"docs/agent-harness-operations-handbook.md","line":30,"text":"OpenClaw memory"}
        ]
    })
    .to_string()
}

fn temp_root(test_name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "agent-harness-memory-pack-{test_name}-{}-{nanos}",
        std::process::id()
    ))
}
