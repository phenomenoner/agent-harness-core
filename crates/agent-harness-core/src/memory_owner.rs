use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

pub const MEMORY_OWNER_STATE_SCHEMA: &str = "agent-harness.memory-owner-state.v1";
pub const MEMORY_OWNER_ENDPOINT_PROBE_SCHEMA: &str = "agent-harness.memory-owner-endpoint-probe.v1";
pub const MEMORY_OWNER_SHADOW_RECEIPT_SCHEMA: &str = "agent-harness.memory-owner-shadow-receipt.v1";
pub const MEMORY_OWNER_PROMOTION_RECEIPT_SCHEMA: &str =
    "agent-harness.memory-owner-promotion-receipt.v1";
pub const MEMORY_OWNER_ROLLBACK_RECEIPT_SCHEMA: &str =
    "agent-harness.memory-owner-rollback-receipt.v1";
pub const OPENCLAW_MEM_REMOTE_SERVICE_CONTRACT: &str = "openclaw-mem.remote-memory-service.v1";

pub const SNAPSHOT_MEMORY_OWNER: &str = "snapshot-adapter";
pub const MEM_ENGINE_OWNER: &str = "mem-engine";
pub const DEFAULT_MEMORY_OWNER_HEARTBEAT_MAX_AGE_MS: i64 = 120_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryOwnerState {
    pub schema: String,
    pub owner: String,
    pub lease_id: Option<String>,
    pub heartbeat_at_ms: Option<i64>,
    pub lease_expires_at_ms: Option<i64>,
    pub rollback_owner: String,
    pub promotion_status: String,
    pub last_parity_receipt: Option<MemoryOwnerReceiptRef>,
    pub endpoint_probe: MemoryOwnerEndpointProbeState,
    pub promotion_gates: MemoryOwnerPromotionGates,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryOwnerReceiptRef {
    pub receipt_id: String,
    pub kind: String,
    pub status: String,
    pub path: PathBuf,
    pub at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryOwnerEndpointProbeState {
    pub status: String,
    pub endpoint_configured: bool,
    pub compatible: bool,
    pub required_contract: String,
    pub observed_contract: Option<String>,
    pub last_probe_receipt: Option<MemoryOwnerReceiptRef>,
    pub checked_at_ms: Option<i64>,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryOwnerPromotionGates {
    pub endpoint_probe_passed: bool,
    pub lease_active: bool,
    pub heartbeat_fresh: bool,
    pub rollback_proof: bool,
    pub trust_scope_tests: bool,
    pub recall_parity_sample: bool,
    pub store_propose_parity_sample: bool,
    pub operator_approved_promotion: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryOwnerEnsureOptions {
    pub harness_home: PathBuf,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryOwnerEndpointProbeOptions {
    pub harness_home: PathBuf,
    pub endpoint: Option<String>,
    pub observed_contract: Option<String>,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryOwnerHeartbeatOptions {
    pub harness_home: PathBuf,
    pub lease_id: String,
    pub now_ms: i64,
    pub lease_ttl_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MemoryOwnerShadowKind {
    Recall,
    Store,
    Capture,
    StorePropose,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryOwnerShadowOptions {
    pub harness_home: PathBuf,
    pub kind: MemoryOwnerShadowKind,
    pub input_id: String,
    pub snapshot_status: String,
    pub mem_engine_status: String,
    pub snapshot_digest: String,
    pub mem_engine_digest: String,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryOwnerTrustScopeOptions {
    pub harness_home: PathBuf,
    pub passed: bool,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryOwnerPromotionOptions {
    pub harness_home: PathBuf,
    pub operator_approved: bool,
    pub now_ms: i64,
    pub heartbeat_max_age_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryOwnerRecoveryOptions {
    pub harness_home: PathBuf,
    pub now_ms: i64,
    pub heartbeat_max_age_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryOwnerEndpointProbeReport {
    pub schema: &'static str,
    pub receipt_id: String,
    pub state_file: PathBuf,
    pub receipt_file: PathBuf,
    pub status: String,
    pub compatible: bool,
    pub endpoint_configured: bool,
    pub endpoint: Option<String>,
    pub required_contract: &'static str,
    pub observed_contract: Option<String>,
    pub owner_before: String,
    pub owner_after: String,
    pub reason: String,
    pub checked_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryOwnerHeartbeatReport {
    pub schema: &'static str,
    pub receipt_id: String,
    pub state_file: PathBuf,
    pub receipt_file: PathBuf,
    pub lease_id: String,
    pub heartbeat_at_ms: i64,
    pub lease_expires_at_ms: i64,
    pub owner_before: String,
    pub owner_after: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryOwnerShadowReceipt {
    pub schema: &'static str,
    pub receipt_id: String,
    pub state_file: PathBuf,
    pub receipt_file: PathBuf,
    pub kind: MemoryOwnerShadowKind,
    pub input_id: String,
    pub snapshot_status: String,
    pub mem_engine_status: String,
    pub snapshot_digest: String,
    pub mem_engine_digest: String,
    pub status: String,
    pub matches: bool,
    pub mutates_active_context: bool,
    pub owner_before: String,
    pub owner_after: String,
    pub at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryOwnerTrustScopeReceipt {
    pub schema: &'static str,
    pub receipt_id: String,
    pub state_file: PathBuf,
    pub receipt_file: PathBuf,
    pub status: String,
    pub passed: bool,
    pub owner_before: String,
    pub owner_after: String,
    pub at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryOwnerPromotionReceipt {
    pub schema: &'static str,
    pub receipt_id: String,
    pub state_file: PathBuf,
    pub receipt_file: PathBuf,
    pub status: String,
    pub owner_before: String,
    pub owner_after: String,
    pub criteria: MemoryOwnerPromotionGates,
    pub blockers: Vec<String>,
    pub operator_approved: bool,
    pub at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryOwnerRollbackReceipt {
    pub schema: &'static str,
    pub receipt_id: String,
    pub state_file: PathBuf,
    pub receipt_file: PathBuf,
    pub status: String,
    pub owner_before: String,
    pub owner_after: String,
    pub reason: String,
    pub at_ms: i64,
}

pub fn memory_owner_state_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("memory")
        .join("owner.json")
}

pub fn memory_owner_endpoint_probe_receipts_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("memory")
        .join("owner-endpoint-probe-receipts.jsonl")
}

pub fn memory_owner_heartbeat_receipts_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("memory")
        .join("owner-heartbeat-receipts.jsonl")
}

pub fn memory_owner_shadow_receipts_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("memory")
        .join("owner-shadow-receipts.jsonl")
}

pub fn memory_owner_trust_scope_receipts_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("memory")
        .join("owner-trust-scope-receipts.jsonl")
}

pub fn memory_owner_promotion_receipts_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("memory")
        .join("owner-promotion-receipts.jsonl")
}

pub fn memory_owner_rollback_receipts_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("memory")
        .join("owner-rollback-receipts.jsonl")
}

pub fn read_memory_owner_state(
    harness_home: impl AsRef<Path>,
) -> io::Result<Option<MemoryOwnerState>> {
    let path = memory_owner_state_file(harness_home);
    let text = match fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    serde_json::from_str(&text)
        .map(Some)
        .map_err(io::Error::other)
}

pub fn read_memory_owner_state_or_default(
    harness_home: impl AsRef<Path>,
    now_ms: i64,
) -> io::Result<MemoryOwnerState> {
    Ok(read_memory_owner_state(&harness_home)?.unwrap_or_else(|| default_owner_state(now_ms)))
}

pub fn ensure_memory_owner_state(
    options: MemoryOwnerEnsureOptions,
) -> io::Result<MemoryOwnerState> {
    let state = read_memory_owner_state(&options.harness_home)?
        .unwrap_or_else(|| default_owner_state(options.now_ms));
    write_memory_owner_state(&options.harness_home, &state)?;
    Ok(state)
}

pub fn record_memory_owner_endpoint_probe(
    options: MemoryOwnerEndpointProbeOptions,
) -> io::Result<MemoryOwnerEndpointProbeReport> {
    let state_file = memory_owner_state_file(&options.harness_home);
    let receipt_file = memory_owner_endpoint_probe_receipts_file(&options.harness_home);
    let mut state = read_memory_owner_state_or_default(&options.harness_home, options.now_ms)?;
    let owner_before = state.owner.clone();
    let endpoint_configured = options
        .endpoint
        .as_deref()
        .is_some_and(|endpoint| !endpoint.trim().is_empty());
    let compatible = endpoint_configured
        && options.observed_contract.as_deref() == Some(OPENCLAW_MEM_REMOTE_SERVICE_CONTRACT);
    let status = if compatible {
        "compatible"
    } else if endpoint_configured {
        "incompatible"
    } else {
        "not-configured"
    };
    let reason = match status {
        "compatible" => "remote openclaw-mem endpoint advertised the required contract",
        "incompatible" => "remote openclaw-mem endpoint did not advertise the required contract",
        _ => "remote openclaw-mem endpoint is not configured",
    }
    .to_string();
    let receipt_id = receipt_id("memory-owner.endpoint-probe", options.now_ms);
    let report = MemoryOwnerEndpointProbeReport {
        schema: MEMORY_OWNER_ENDPOINT_PROBE_SCHEMA,
        receipt_id: receipt_id.clone(),
        state_file: state_file.clone(),
        receipt_file: receipt_file.clone(),
        status: status.to_string(),
        compatible,
        endpoint_configured,
        endpoint: endpoint_configured.then_some("[configured-redacted]".to_string()),
        required_contract: OPENCLAW_MEM_REMOTE_SERVICE_CONTRACT,
        observed_contract: options.observed_contract,
        owner_before,
        owner_after: SNAPSHOT_MEMORY_OWNER.to_string(),
        reason: reason.clone(),
        checked_at_ms: options.now_ms,
    };
    let probe_ref = MemoryOwnerReceiptRef {
        receipt_id,
        kind: "endpoint-probe".to_string(),
        status: status.to_string(),
        path: receipt_file.clone(),
        at_ms: options.now_ms,
    };
    state.owner = SNAPSHOT_MEMORY_OWNER.to_string();
    state.endpoint_probe = MemoryOwnerEndpointProbeState {
        status: status.to_string(),
        endpoint_configured,
        compatible,
        required_contract: OPENCLAW_MEM_REMOTE_SERVICE_CONTRACT.to_string(),
        observed_contract: report.observed_contract.clone(),
        last_probe_receipt: Some(probe_ref),
        checked_at_ms: Some(options.now_ms),
        reason,
    };
    state.promotion_gates.endpoint_probe_passed = compatible;
    state.promotion_status = if compatible {
        "probe-passed-awaiting-shadow-gates".to_string()
    } else {
        "snapshot-active".to_string()
    };
    state.updated_at_ms = options.now_ms;
    write_memory_owner_state(&options.harness_home, &state)?;
    crate::append_jsonl_value(&receipt_file, &report)?;
    Ok(report)
}

pub fn record_memory_owner_heartbeat(
    options: MemoryOwnerHeartbeatOptions,
) -> io::Result<MemoryOwnerHeartbeatReport> {
    let state_file = memory_owner_state_file(&options.harness_home);
    let receipt_file = memory_owner_heartbeat_receipts_file(&options.harness_home);
    let mut state = read_memory_owner_state_or_default(&options.harness_home, options.now_ms)?;
    let owner_before = state.owner.clone();
    let lease_ttl_ms = options.lease_ttl_ms.max(1);
    let lease_expires_at_ms = options.now_ms.saturating_add(lease_ttl_ms);
    state.lease_id = Some(options.lease_id.clone());
    state.heartbeat_at_ms = Some(options.now_ms);
    state.lease_expires_at_ms = Some(lease_expires_at_ms);
    state.rollback_owner = SNAPSHOT_MEMORY_OWNER.to_string();
    state.promotion_gates.lease_active = true;
    state.promotion_gates.heartbeat_fresh = true;
    state.updated_at_ms = options.now_ms;
    write_memory_owner_state(&options.harness_home, &state)?;
    let report = MemoryOwnerHeartbeatReport {
        schema: MEMORY_OWNER_STATE_SCHEMA,
        receipt_id: receipt_id("memory-owner.heartbeat", options.now_ms),
        state_file,
        receipt_file: receipt_file.clone(),
        lease_id: options.lease_id,
        heartbeat_at_ms: options.now_ms,
        lease_expires_at_ms,
        owner_before,
        owner_after: state.owner,
    };
    crate::append_jsonl_value(&receipt_file, &report)?;
    Ok(report)
}

pub fn record_memory_owner_shadow_receipt(
    options: MemoryOwnerShadowOptions,
) -> io::Result<MemoryOwnerShadowReceipt> {
    let state_file = memory_owner_state_file(&options.harness_home);
    let receipt_file = memory_owner_shadow_receipts_file(&options.harness_home);
    let mut state = read_memory_owner_state_or_default(&options.harness_home, options.now_ms)?;
    let owner_before = state.owner.clone();
    let matches = options.snapshot_status == options.mem_engine_status
        && options.snapshot_digest == options.mem_engine_digest;
    let status = if matches { "matched" } else { "diverged" };
    let receipt_id = receipt_id(
        &format!("memory-owner.shadow.{}", options.kind.as_str()),
        options.now_ms,
    );
    let report = MemoryOwnerShadowReceipt {
        schema: MEMORY_OWNER_SHADOW_RECEIPT_SCHEMA,
        receipt_id: receipt_id.clone(),
        state_file: state_file.clone(),
        receipt_file: receipt_file.clone(),
        kind: options.kind,
        input_id: options.input_id,
        snapshot_status: options.snapshot_status,
        mem_engine_status: options.mem_engine_status,
        snapshot_digest: options.snapshot_digest,
        mem_engine_digest: options.mem_engine_digest,
        status: status.to_string(),
        matches,
        mutates_active_context: false,
        owner_before,
        owner_after: state.owner.clone(),
        at_ms: options.now_ms,
    };
    state.last_parity_receipt = Some(MemoryOwnerReceiptRef {
        receipt_id,
        kind: options.kind.as_str().to_string(),
        status: status.to_string(),
        path: receipt_file.clone(),
        at_ms: options.now_ms,
    });
    if matches {
        match options.kind {
            MemoryOwnerShadowKind::Recall => state.promotion_gates.recall_parity_sample = true,
            MemoryOwnerShadowKind::Store | MemoryOwnerShadowKind::StorePropose => {
                state.promotion_gates.store_propose_parity_sample = true;
            }
            MemoryOwnerShadowKind::Capture => {}
        }
    }
    state.updated_at_ms = options.now_ms;
    write_memory_owner_state(&options.harness_home, &state)?;
    crate::append_jsonl_value(&receipt_file, &report)?;
    Ok(report)
}

pub fn record_memory_owner_trust_scope_receipt(
    options: MemoryOwnerTrustScopeOptions,
) -> io::Result<MemoryOwnerTrustScopeReceipt> {
    let state_file = memory_owner_state_file(&options.harness_home);
    let receipt_file = memory_owner_trust_scope_receipts_file(&options.harness_home);
    let mut state = read_memory_owner_state_or_default(&options.harness_home, options.now_ms)?;
    let owner_before = state.owner.clone();
    state.promotion_gates.trust_scope_tests = options.passed;
    state.updated_at_ms = options.now_ms;
    write_memory_owner_state(&options.harness_home, &state)?;
    let report = MemoryOwnerTrustScopeReceipt {
        schema: MEMORY_OWNER_STATE_SCHEMA,
        receipt_id: receipt_id("memory-owner.trust-scope", options.now_ms),
        state_file,
        receipt_file: receipt_file.clone(),
        status: if options.passed { "passed" } else { "failed" }.to_string(),
        passed: options.passed,
        owner_before,
        owner_after: state.owner,
        at_ms: options.now_ms,
    };
    crate::append_jsonl_value(&receipt_file, &report)?;
    Ok(report)
}

pub fn request_memory_owner_promotion(
    options: MemoryOwnerPromotionOptions,
) -> io::Result<MemoryOwnerPromotionReceipt> {
    let state_file = memory_owner_state_file(&options.harness_home);
    let receipt_file = memory_owner_promotion_receipts_file(&options.harness_home);
    let mut state = read_memory_owner_state_or_default(&options.harness_home, options.now_ms)?;
    let owner_before = state.owner.clone();
    let criteria = evaluate_promotion_gates(
        &state,
        options.now_ms,
        options.heartbeat_max_age_ms,
        options.operator_approved,
    );
    let blockers = promotion_blockers(&criteria);
    let promoted = blockers.is_empty();
    if promoted {
        state.owner = MEM_ENGINE_OWNER.to_string();
        state.promotion_status = "promoted".to_string();
    } else {
        state.owner = SNAPSHOT_MEMORY_OWNER.to_string();
        state.promotion_status = if !criteria.operator_approved_promotion {
            "blocked-awaiting-operator-approval".to_string()
        } else {
            "blocked-promotion-gates".to_string()
        };
    }
    state.promotion_gates = criteria.clone();
    state.rollback_owner = SNAPSHOT_MEMORY_OWNER.to_string();
    state.updated_at_ms = options.now_ms;
    write_memory_owner_state(&options.harness_home, &state)?;
    let report = MemoryOwnerPromotionReceipt {
        schema: MEMORY_OWNER_PROMOTION_RECEIPT_SCHEMA,
        receipt_id: receipt_id("memory-owner.promotion", options.now_ms),
        state_file,
        receipt_file: receipt_file.clone(),
        status: if promoted { "promoted" } else { "blocked" }.to_string(),
        owner_before,
        owner_after: state.owner,
        criteria,
        blockers,
        operator_approved: options.operator_approved,
        at_ms: options.now_ms,
    };
    crate::append_jsonl_value(&receipt_file, &report)?;
    Ok(report)
}

pub fn recover_memory_owner_state(
    options: MemoryOwnerRecoveryOptions,
) -> io::Result<MemoryOwnerRollbackReceipt> {
    let state_file = memory_owner_state_file(&options.harness_home);
    let receipt_file = memory_owner_rollback_receipts_file(&options.harness_home);
    let mut state = read_memory_owner_state_or_default(&options.harness_home, options.now_ms)?;
    let owner_before = state.owner.clone();
    let lease_expired = state
        .lease_expires_at_ms
        .is_some_and(|expires_at_ms| expires_at_ms <= options.now_ms);
    let heartbeat_stale = state.heartbeat_at_ms.is_none_or(|heartbeat_at_ms| {
        options.now_ms.saturating_sub(heartbeat_at_ms) > options.heartbeat_max_age_ms
    });
    let should_rollback = state.owner == MEM_ENGINE_OWNER && (lease_expired || heartbeat_stale);
    let (status, reason) = if should_rollback {
        state.owner = SNAPSHOT_MEMORY_OWNER.to_string();
        state.rollback_owner = SNAPSHOT_MEMORY_OWNER.to_string();
        state.promotion_status = "rolled-back-expired-lease".to_string();
        state.promotion_gates.lease_active = false;
        state.promotion_gates.heartbeat_fresh = false;
        state.updated_at_ms = options.now_ms;
        write_memory_owner_state(&options.harness_home, &state)?;
        (
            "rolled-back",
            "expired mem-engine lease or stale heartbeat; snapshot-adapter restored",
        )
    } else {
        (
            "unchanged",
            "memory owner did not require crash recovery rollback",
        )
    };
    let report = MemoryOwnerRollbackReceipt {
        schema: MEMORY_OWNER_ROLLBACK_RECEIPT_SCHEMA,
        receipt_id: receipt_id("memory-owner.rollback", options.now_ms),
        state_file,
        receipt_file: receipt_file.clone(),
        status: status.to_string(),
        owner_before,
        owner_after: state.owner,
        reason: reason.to_string(),
        at_ms: options.now_ms,
    };
    if should_rollback {
        crate::append_jsonl_value(&receipt_file, &report)?;
    }
    Ok(report)
}

pub fn default_owner_state(now_ms: i64) -> MemoryOwnerState {
    MemoryOwnerState {
        schema: MEMORY_OWNER_STATE_SCHEMA.to_string(),
        owner: SNAPSHOT_MEMORY_OWNER.to_string(),
        lease_id: None,
        heartbeat_at_ms: None,
        lease_expires_at_ms: None,
        rollback_owner: SNAPSHOT_MEMORY_OWNER.to_string(),
        promotion_status: "snapshot-active".to_string(),
        last_parity_receipt: None,
        endpoint_probe: MemoryOwnerEndpointProbeState {
            status: "not-probed".to_string(),
            endpoint_configured: false,
            compatible: false,
            required_contract: OPENCLAW_MEM_REMOTE_SERVICE_CONTRACT.to_string(),
            observed_contract: None,
            last_probe_receipt: None,
            checked_at_ms: None,
            reason: "remote openclaw-mem endpoint has not been probed".to_string(),
        },
        promotion_gates: MemoryOwnerPromotionGates {
            endpoint_probe_passed: false,
            lease_active: false,
            heartbeat_fresh: false,
            rollback_proof: true,
            trust_scope_tests: false,
            recall_parity_sample: false,
            store_propose_parity_sample: false,
            operator_approved_promotion: false,
        },
        updated_at_ms: now_ms,
    }
}

impl MemoryOwnerShadowKind {
    pub fn as_str(self) -> &'static str {
        match self {
            MemoryOwnerShadowKind::Recall => "recall",
            MemoryOwnerShadowKind::Store => "store",
            MemoryOwnerShadowKind::Capture => "capture",
            MemoryOwnerShadowKind::StorePropose => "store-propose",
        }
    }
}

fn write_memory_owner_state(harness_home: &Path, state: &MemoryOwnerState) -> io::Result<()> {
    crate::write_json_atomic(&memory_owner_state_file(harness_home), state)
}

fn evaluate_promotion_gates(
    state: &MemoryOwnerState,
    now_ms: i64,
    heartbeat_max_age_ms: i64,
    operator_approved: bool,
) -> MemoryOwnerPromotionGates {
    let heartbeat_max_age_ms = heartbeat_max_age_ms.max(1);
    let mut gates = state.promotion_gates.clone();
    gates.lease_active = state
        .lease_expires_at_ms
        .is_some_and(|expires_at_ms| expires_at_ms > now_ms)
        && state.lease_id.is_some();
    gates.heartbeat_fresh = state.heartbeat_at_ms.is_some_and(|heartbeat_at_ms| {
        now_ms.saturating_sub(heartbeat_at_ms) <= heartbeat_max_age_ms
    });
    gates.rollback_proof = state.rollback_owner == SNAPSHOT_MEMORY_OWNER;
    gates.operator_approved_promotion = operator_approved;
    gates
}

fn promotion_blockers(criteria: &MemoryOwnerPromotionGates) -> Vec<String> {
    let mut blockers = Vec::new();
    if !criteria.endpoint_probe_passed {
        blockers.push("endpoint probe has not passed compatible remote contract".to_string());
    }
    if !criteria.lease_active {
        blockers.push("mem-engine lease is not active".to_string());
    }
    if !criteria.heartbeat_fresh {
        blockers.push("mem-engine heartbeat is not fresh".to_string());
    }
    if !criteria.rollback_proof {
        blockers.push("snapshot-adapter rollback proof is missing".to_string());
    }
    if !criteria.trust_scope_tests {
        blockers.push("trust/scope tests have not passed".to_string());
    }
    if !criteria.recall_parity_sample {
        blockers.push("recall parity sample has not passed".to_string());
    }
    if !criteria.store_propose_parity_sample {
        blockers.push("store/propose parity sample has not passed".to_string());
    }
    if !criteria.operator_approved_promotion {
        blockers.push("operator-approved promotion is missing".to_string());
    }
    blockers
}

fn receipt_id(prefix: &str, now_ms: i64) -> String {
    format!("{prefix}-{now_ms}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn memory_owner_endpoint_probe_and_shadow_gates_keep_snapshot_active() {
        let root = temp_root("endpoint_probe_and_shadow_gates_keep_snapshot_active");
        let harness_home = root.join("harness");

        let state = ensure_memory_owner_state(MemoryOwnerEnsureOptions {
            harness_home: harness_home.clone(),
            now_ms: 1_000,
        })
        .unwrap();
        assert_eq!(state.owner, SNAPSHOT_MEMORY_OWNER);
        assert!(memory_owner_state_file(&harness_home).is_file());

        let probe = record_memory_owner_endpoint_probe(MemoryOwnerEndpointProbeOptions {
            harness_home: harness_home.clone(),
            endpoint: Some("http://127.0.0.1:7788".to_string()),
            observed_contract: Some(OPENCLAW_MEM_REMOTE_SERVICE_CONTRACT.to_string()),
            now_ms: 1_100,
        })
        .unwrap();
        assert_eq!(probe.status, "compatible");
        assert_eq!(probe.owner_after, SNAPSHOT_MEMORY_OWNER);
        assert_eq!(probe.endpoint.as_deref(), Some("[configured-redacted]"));

        record_memory_owner_heartbeat(MemoryOwnerHeartbeatOptions {
            harness_home: harness_home.clone(),
            lease_id: "lease-1".to_string(),
            now_ms: 1_200,
            lease_ttl_ms: 5_000,
        })
        .unwrap();
        let recall_shadow = record_memory_owner_shadow_receipt(MemoryOwnerShadowOptions {
            harness_home: harness_home.clone(),
            kind: MemoryOwnerShadowKind::Recall,
            input_id: "recall-1".to_string(),
            snapshot_status: "ready".to_string(),
            mem_engine_status: "ready".to_string(),
            snapshot_digest: "sha256:abc".to_string(),
            mem_engine_digest: "sha256:abc".to_string(),
            now_ms: 1_300,
        })
        .unwrap();
        assert!(recall_shadow.matches);
        assert!(!recall_shadow.mutates_active_context);
        assert_eq!(recall_shadow.owner_after, SNAPSHOT_MEMORY_OWNER);
        record_memory_owner_shadow_receipt(MemoryOwnerShadowOptions {
            harness_home: harness_home.clone(),
            kind: MemoryOwnerShadowKind::StorePropose,
            input_id: "store-1".to_string(),
            snapshot_status: "stored".to_string(),
            mem_engine_status: "stored".to_string(),
            snapshot_digest: "sha256:def".to_string(),
            mem_engine_digest: "sha256:def".to_string(),
            now_ms: 1_400,
        })
        .unwrap();
        record_memory_owner_trust_scope_receipt(MemoryOwnerTrustScopeOptions {
            harness_home: harness_home.clone(),
            passed: true,
            now_ms: 1_500,
        })
        .unwrap();

        let blocked = request_memory_owner_promotion(MemoryOwnerPromotionOptions {
            harness_home: harness_home.clone(),
            operator_approved: false,
            now_ms: 1_600,
            heartbeat_max_age_ms: DEFAULT_MEMORY_OWNER_HEARTBEAT_MAX_AGE_MS,
        })
        .unwrap();
        assert_eq!(blocked.status, "blocked");
        assert_eq!(blocked.owner_after, SNAPSHOT_MEMORY_OWNER);
        assert!(
            blocked
                .blockers
                .iter()
                .any(|blocker| blocker.contains("operator-approved"))
        );

        let state = read_memory_owner_state(&harness_home).unwrap().unwrap();
        assert_eq!(state.owner, SNAPSHOT_MEMORY_OWNER);
        assert_eq!(state.lease_id.as_deref(), Some("lease-1"));
        assert_eq!(state.heartbeat_at_ms, Some(1_200));
        assert_eq!(state.rollback_owner, SNAPSHOT_MEMORY_OWNER);
        assert_eq!(state.promotion_status, "blocked-awaiting-operator-approval");
        assert!(state.last_parity_receipt.is_some());
        assert!(state.promotion_gates.endpoint_probe_passed);
        assert!(state.promotion_gates.lease_active);
        assert!(state.promotion_gates.heartbeat_fresh);
        assert!(state.promotion_gates.recall_parity_sample);
        assert!(state.promotion_gates.store_propose_parity_sample);
        assert!(state.promotion_gates.trust_scope_tests);
        assert!(!state.promotion_gates.operator_approved_promotion);
        assert!(memory_owner_shadow_receipts_file(&harness_home).is_file());
        assert!(memory_owner_promotion_receipts_file(&harness_home).is_file());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn memory_owner_operator_promotion_requires_all_gates_and_recovers_expired_lease() {
        let root = temp_root("operator_promotion_requires_all_gates_and_recovers_expired_lease");
        let harness_home = root.join("harness");

        ensure_memory_owner_state(MemoryOwnerEnsureOptions {
            harness_home: harness_home.clone(),
            now_ms: 2_000,
        })
        .unwrap();
        record_memory_owner_endpoint_probe(MemoryOwnerEndpointProbeOptions {
            harness_home: harness_home.clone(),
            endpoint: Some("http://127.0.0.1:7788".to_string()),
            observed_contract: Some(OPENCLAW_MEM_REMOTE_SERVICE_CONTRACT.to_string()),
            now_ms: 2_100,
        })
        .unwrap();
        record_memory_owner_heartbeat(MemoryOwnerHeartbeatOptions {
            harness_home: harness_home.clone(),
            lease_id: "lease-2".to_string(),
            now_ms: 2_200,
            lease_ttl_ms: 1_000,
        })
        .unwrap();
        record_memory_owner_shadow_receipt(MemoryOwnerShadowOptions {
            harness_home: harness_home.clone(),
            kind: MemoryOwnerShadowKind::Recall,
            input_id: "recall-2".to_string(),
            snapshot_status: "ready".to_string(),
            mem_engine_status: "ready".to_string(),
            snapshot_digest: "sha256:recall".to_string(),
            mem_engine_digest: "sha256:recall".to_string(),
            now_ms: 2_300,
        })
        .unwrap();
        record_memory_owner_shadow_receipt(MemoryOwnerShadowOptions {
            harness_home: harness_home.clone(),
            kind: MemoryOwnerShadowKind::Store,
            input_id: "store-2".to_string(),
            snapshot_status: "stored".to_string(),
            mem_engine_status: "stored".to_string(),
            snapshot_digest: "sha256:store".to_string(),
            mem_engine_digest: "sha256:store".to_string(),
            now_ms: 2_400,
        })
        .unwrap();
        record_memory_owner_trust_scope_receipt(MemoryOwnerTrustScopeOptions {
            harness_home: harness_home.clone(),
            passed: true,
            now_ms: 2_500,
        })
        .unwrap();

        let promoted = request_memory_owner_promotion(MemoryOwnerPromotionOptions {
            harness_home: harness_home.clone(),
            operator_approved: true,
            now_ms: 2_600,
            heartbeat_max_age_ms: DEFAULT_MEMORY_OWNER_HEARTBEAT_MAX_AGE_MS,
        })
        .unwrap();
        assert_eq!(promoted.status, "promoted");
        assert_eq!(promoted.owner_after, MEM_ENGINE_OWNER);
        assert!(promoted.blockers.is_empty());
        let state = read_memory_owner_state(&harness_home).unwrap().unwrap();
        assert_eq!(state.owner, MEM_ENGINE_OWNER);
        assert_eq!(state.rollback_owner, SNAPSHOT_MEMORY_OWNER);
        assert_eq!(state.promotion_status, "promoted");

        let rollback = recover_memory_owner_state(MemoryOwnerRecoveryOptions {
            harness_home: harness_home.clone(),
            now_ms: 3_300,
            heartbeat_max_age_ms: DEFAULT_MEMORY_OWNER_HEARTBEAT_MAX_AGE_MS,
        })
        .unwrap();
        assert_eq!(rollback.status, "rolled-back");
        assert_eq!(rollback.owner_before, MEM_ENGINE_OWNER);
        assert_eq!(rollback.owner_after, SNAPSHOT_MEMORY_OWNER);
        let state = read_memory_owner_state(&harness_home).unwrap().unwrap();
        assert_eq!(state.owner, SNAPSHOT_MEMORY_OWNER);
        assert_eq!(state.promotion_status, "rolled-back-expired-lease");
        assert!(memory_owner_rollback_receipts_file(&harness_home).is_file());

        let _ = fs::remove_dir_all(root);
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-memory-owner-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
