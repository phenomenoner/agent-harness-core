use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::Value;
#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt;

use crate::backend_reasoning::{BackendReasoningPolicyV1, ReasoningPreference};
use crate::channel_state::ChannelStateLane;
use crate::context_rollover::{
    derive_virtual_session_id, derive_virtual_session_id_v2, root_working_session_key,
};
use crate::execution_mode::{
    AuthorizedExecutionModeSnapshotV2, STANDARD_EXECUTION_MODE, is_reserved_execution_mode_effort,
};
use crate::lane::FullLaneKeyV1;
use crate::logging::{try_with_jsonl_append_lock, with_jsonl_append_lock};
use crate::loop_health::process_alive_for_pid;
use crate::runtime_execution_receipt_index::prepared_execution_receipts_from_index;
use crate::runtime_pending_index::{
    prune_terminal_queue_ids_from_pending_index, read_queued_pending_values_from_index,
    read_queued_pending_values_from_index_nonblocking,
};
use crate::runtime_receipt_history::{
    RuntimeQueueReceiptHistoryStaging, cleanup_staged_runtime_queue_receipt_history,
    commit_runtime_queue_receipt_history, discard_runtime_queue_receipt_history,
    find_runtime_queue_terminal_history, find_runtime_queue_terminal_history_nonblocking,
    is_trusted_runtime_run_once_receipt, runtime_queue_receipt_history_file,
    stage_runtime_queue_receipt_history,
};
use crate::{
    AgentSource, HarnessLogEvent, HarnessLogLevel, InboundMediaArtifact, PromptAssemblyOptions,
    RuntimeContinuationMetadata, SkillRouterV2Policy, SkillShadowRuntimeReceiptOptions,
    VirtualSkillRuntimeObserveOptions, append_harness_log, apply_context_rollover_before_turn,
    assemble_prompt_bundle, build_runtime_skill_index, build_turn_plan_for_account,
    cron_run_runtime_dispatch_blocker, current_log_time_ms, load_agent_registry,
    load_worker_dispatch_config, observe_virtual_skill_runtime,
    record_skill_shadow_runtime_receipt, virtual_skill_manifest_observation_enabled,
    write_json_atomic, write_prompt_bundle,
};
use crate::{ContextRolloverBeforeTurnOptions, ContextRolloverStatus};

const RUNTIME_QUEUE_PREPARE_REPORT_SCHEMA: &str = "agent-harness.runtime-queue-prepare.v1";
const RUNTIME_QUEUE_LEASES_SCHEMA: &str = "agent-harness.runtime-queue-leases.v1";
const RUNTIME_QUEUE_LEASE_RECONCILIATION_SCHEMA: &str =
    "agent-harness.runtime-queue-lease-reconciliation.v1";
const RUNTIME_QUEUE_LEASE_OBSERVATION_SCHEMA: &str =
    "agent-harness.runtime-queue-lease-observation.v1";
const RUNTIME_QUEUE_STATE_INDEX_SCHEMA: &str = "agent-harness.runtime-queue-state-index.v1";
const RUNTIME_QUEUE_STATE_INDEX_REVISION: u32 = 4;
const RUNTIME_QUEUE_RECEIPT_COMPACTION_SCHEMA: &str =
    "agent-harness.runtime-queue-receipt-compaction.v1";
const RUNTIME_QUEUE_RECEIPT_COMPACTION_PENDING_SCHEMA: &str =
    "agent-harness.runtime-queue-receipt-compaction-pending.v3";
const RUNTIME_QUEUE_RECEIPT_COMPACTION_PENDING_SCHEMA_V2: &str =
    "agent-harness.runtime-queue-receipt-compaction-pending.v2";
const RUNTIME_QUEUE_RECEIPT_COMPACTION_STATE_SCHEMA: &str =
    "agent-harness.runtime-queue-receipt-compaction-state.v1";
const RUNTIME_QUEUE_RECEIPT_COMPACTION_DEFAULT_MAX_BYTES: u64 = 16 * 1024 * 1024;
const RUNTIME_QUEUE_RECEIPT_COMPACTION_DEFAULT_MAX_ARCHIVES: usize = 3;
const RUNTIME_QUEUE_RECEIPT_COMPACTION_RETRY_GROWTH_BYTES: u64 = 1024 * 1024;
const RUNTIME_QUEUE_RECEIPT_COMPACTION_RETRY_INTERVAL_MS: i64 = 5 * 60 * 1000;
const RUNTIME_QUEUE_QUARANTINE_SCHEMA: &str = "agent-harness.runtime-queue-quarantine.v1";
const TERMINAL_CONTROL_SUPPRESSION_REASON: &str = "terminal-control-present";
const RUNTIME_LOOP_SERVICE_ID: &str = "runtime-loop";
const DEFAULT_RUNTIME_LEASE_MS: i64 = 30 * 60 * 1000;
const RUNTIME_LEASE_ACQUIRE_LOCK_RETRY_MS: u64 = 2_000;
const RUNTIME_LEASE_RELEASE_LOCK_RETRY_MS: u64 = 2_000;
const RUNTIME_LEASE_RELEASE_LOCK_RETRY_SLEEP_MS: u64 = 25;
#[cfg(not(windows))]
const RUNTIME_LEASE_LOCK_STALE_MS: i64 = 30_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeQueuePrepareOptions {
    pub harness_home: PathBuf,
    pub queue_id: Option<String>,
    pub prompt_options: PromptAssemblyOptions,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeQueueCapacityOptions {
    pub harness_home: PathBuf,
}

/// Minimal routing information required to emit a provider typing/working
/// indicator for one still-runnable queue item.  This deliberately excludes
/// user text, prompt content, and filesystem paths so the CLI can obtain it
/// from bounded runtime projections instead of replaying `pending.jsonl`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeQueueTypingContext {
    pub agent_id: String,
    pub platform: String,
    pub channel_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeQueueReceiptCompactionOptions {
    pub harness_home: PathBuf,
    pub max_bytes: u64,
    pub max_archives: usize,
    pub now_ms: i64,
}

impl RuntimeQueueReceiptCompactionOptions {
    pub fn with_defaults(harness_home: PathBuf, now_ms: i64) -> Self {
        Self {
            harness_home,
            max_bytes: RUNTIME_QUEUE_RECEIPT_COMPACTION_DEFAULT_MAX_BYTES,
            max_archives: RUNTIME_QUEUE_RECEIPT_COMPACTION_DEFAULT_MAX_ARCHIVES,
            now_ms,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum RuntimeQueueReceiptCompactionStatus {
    Missing,
    Unchanged,
    Busy,
    Compacted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeQueueReceiptCompactionReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub receipts_file: PathBuf,
    pub pending_file: PathBuf,
    pub status: RuntimeQueueReceiptCompactionStatus,
    pub original_bytes: u64,
    pub compacted_bytes: u64,
    pub archive_file: Option<PathBuf>,
    pub removed_archives: Vec<PathBuf>,
    pub removed_pending_items: usize,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeQueueCapacityReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub queue_file: PathBuf,
    pub leases_file: PathBuf,
    pub classes: Vec<RuntimeQueueClassCapacity>,
    pub claimable_items: usize,
    pub claimable_queue_ids: Vec<String>,
    pub leased_items: usize,
    pub global_limit: usize,
    pub agent_limit: usize,
    pub agent_channel_limit: usize,
    pub session_limit: usize,
    pub lease_lock_busy: bool,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeDispatchConfig {
    pub global_concurrency_limit: usize,
    pub interactive_reserve: usize,
    pub classes: BTreeMap<String, RuntimeDispatchClassConfig>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeDispatchClassConfig {
    pub max_active: usize,
    pub per_agent_max_active: usize,
    pub per_channel_max_active: usize,
    pub per_session_max_active: usize,
    pub session_fifo: bool,
    pub same_session_main_agent_serialization: bool,
    pub per_job_max_active: usize,
    pub max_queued_per_agent: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeQueueClassCapacity {
    pub runtime_class: String,
    pub leases_file: PathBuf,
    pub lock_file: PathBuf,
    pub leased_items: usize,
    pub claimable_items: usize,
    pub lock_busy: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeQueuePrepareReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub queue_file: PathBuf,
    pub execution_receipts_file: PathBuf,
    pub item: Option<RuntimeQueuePreparedItem>,
    pub receipt: RuntimeExecutionReceipt,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeQueuePreparedItem {
    pub queue_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admission_queue_id: Option<String>,
    pub agent_id: String,
    pub session_key: String,
    pub runtime_class: String,
    pub origin: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cron_run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scheduled_for_ms: Option<i64>,
    pub platform: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account_id: Option<String>,
    pub channel_id: String,
    pub user_id: String,
    pub message_text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inbound_context: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub inbound_media_artifacts: Vec<InboundMediaArtifact>,
    pub provider: Option<String>,
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_preference: Option<ReasoningPreference>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backend_reasoning_policy: Option<BackendReasoningPolicyV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authorized_execution_mode: Option<AuthorizedExecutionModeSnapshotV2>,
    pub execution_dir: PathBuf,
    pub prompt_bundle_json: PathBuf,
    pub prompt_markdown: PathBuf,
    pub receipt_file: PathBuf,
    pub planned_transcript_file: PathBuf,
    pub planned_trajectory_file: PathBuf,
    pub selected_skill_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub virtual_skill_manifest_file: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skill_delivery_receipt_files: Vec<PathBuf>,
    #[serde(default, flatten)]
    pub continuation: RuntimeContinuationMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeExecutionReceipt {
    pub queue_id: Option<String>,
    pub status: RuntimeExecutionReceiptStatus,
    /// Exact channel/account/agent lane for prepared interactive work.  This
    /// is emitted from the durable queued item so downstream adapters never
    /// reconstruct a virtual-session lane from a lossy session key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel_lane: Option<crate::ChannelStateLane>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_class: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cron_run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scheduled_for_ms: Option<i64>,
    pub execution_dir: Option<PathBuf>,
    pub prompt_bundle_json: Option<PathBuf>,
    pub prompt_markdown: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_workspace: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inbound_media_artifacts: Vec<InboundMediaArtifact>,
    #[serde(default, flatten)]
    pub continuation: RuntimeContinuationMetadata,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_control_matched: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_control_source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suppressed_run_once_reason: Option<String>,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RuntimeExecutionReceiptStatus {
    Prepared,
    AlreadyPrepared,
    LeaseAcquired,
    StaleOwnerReaped,
    LeaseBusy,
    NoPendingItem,
    CanonicalCollisionReconciled,
    InvalidCanonicalLaneQuarantined,
    SessionIdentityNormalized,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalControlSource {
    QueueSkip,
    ScopedStop,
    RunOnceTerminal,
    Quarantine,
}

impl TerminalControlSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::QueueSkip => "queue-skip",
            Self::ScopedStop => "scoped-stop",
            Self::RunOnceTerminal => "run-once-terminal",
            Self::Quarantine => "quarantine",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueTerminalControlMatch {
    pub source: TerminalControlSource,
    pub reason: String,
    pub suppression_recorded: bool,
    pub terminal_status: Option<String>,
    pub terminal_disposition: Option<crate::RuntimeTerminalDispositionV1>,
    pub continuation_link: Option<crate::RuntimeContinuationLinkV1>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueueTerminalControl {
    Runnable,
    Terminal(QueueTerminalControlMatch),
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RuntimeQueueStateIndex {
    #[serde(default = "runtime_queue_state_index_schema")]
    schema: String,
    #[serde(default)]
    revision: u32,
    #[serde(default)]
    receipt_ledger: RuntimeQueueReceiptLedgerCursor,
    #[serde(default)]
    pub(crate) queues: BTreeMap<String, RuntimeQueueStateIndexEntry>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RuntimeQueueReceiptLedgerCursor {
    #[serde(default)]
    offset_bytes: u64,
    #[serde(default)]
    line_number: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source_modified_at_unix_nanos: Option<u128>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    prefix_tail_fingerprint: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RuntimeQueueStateIndexEntry {
    #[serde(default)]
    terminal_ever: bool,
    #[serde(default)]
    terminal_run_once_ever: bool,
    #[serde(default)]
    suppression_recorded: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    latest_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    latest_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    latest_occurred_at_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    latest_runtime_class: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    latest_origin: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    latest_transcript_file: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    run_once_status_counts: BTreeMap<String, usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    terminal_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    terminal_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    terminal_control_source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    terminal_runtime_class: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    terminal_origin: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    terminal_transcript_file: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    terminal_occurred_at_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    terminal_disposition: Option<crate::RuntimeTerminalDispositionV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    continuation_link: Option<crate::RuntimeContinuationLinkV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    retry_schedule: Option<crate::RuntimeRetryScheduleV1>,
}

/// One exact queue record materialized from the append-only hot receipt index.
///
/// This intentionally contains only the latest receipt metadata required by
/// normal runtime readers. Historical terminal evidence lives in the compact
/// SQLite store once it leaves the hot ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeQueueHotReceiptRecord {
    pub(crate) queue_id: String,
    pub(crate) status: String,
    pub(crate) reason: Option<String>,
    pub(crate) runtime_class: Option<String>,
    pub(crate) origin: Option<String>,
    pub(crate) transcript_file: Option<PathBuf>,
    pub(crate) occurred_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RuntimeQueueLedgerCompactionPending {
    schema: String,
    ledger_file: PathBuf,
    archive_file: PathBuf,
    temp_file: PathBuf,
    expected_bytes: u64,
    expected_digest: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    archive_expected_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    archive_expected_digest: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    history_store_file: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    history_transaction_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RuntimeQueueReceiptCompactionState {
    schema: String,
    last_attempt_at_ms: i64,
    last_attempt_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct RuntimeRunOnceSkipReceipt {
    schema: &'static str,
    queue_id: Option<String>,
    status: &'static str,
    runtime_class: Option<String>,
    origin: Option<String>,
    cron_run_id: Option<String>,
    scheduled_for_ms: Option<i64>,
    execution_dir: Option<PathBuf>,
    transcript_file: Option<PathBuf>,
    outbox_file: Option<PathBuf>,
    reason: String,
}

#[derive(Debug, Clone)]
struct PendingQueueItem {
    queue_id: String,
    admission_queue_id: Option<String>,
    created_at_ms: i64,
    agent_id: String,
    session_key: String,
    runtime_class: String,
    origin: String,
    cron_run_id: Option<String>,
    scheduled_for_ms: Option<i64>,
    platform: String,
    account_id: Option<String>,
    channel_id: String,
    user_id: String,
    message_text: String,
    inbound_context: Option<String>,
    inbound_media_artifacts: Vec<InboundMediaArtifact>,
    source_home: PathBuf,
    source_workspace: PathBuf,
    runtime_workspace: Option<PathBuf>,
    provider: Option<String>,
    model: Option<String>,
    reasoning_preference: Option<ReasoningPreference>,
    backend_reasoning_policy: Option<BackendReasoningPolicyV1>,
    authorized_execution_mode: Option<AuthorizedExecutionModeSnapshotV2>,
    planned_transcript_file: PathBuf,
    planned_trajectory_file: PathBuf,
    selected_skill_ids: Vec<String>,
    continuation: RuntimeContinuationMetadata,
    coordinator_resume: Option<crate::coordinator_resume::CoordinatorResumeMetadataV1>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RuntimeQueueLeaseState {
    #[serde(default = "runtime_queue_leases_schema")]
    schema: String,
    #[serde(default)]
    leases: BTreeMap<String, RuntimeQueueLease>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RuntimeQueueLease {
    queue_id: String,
    agent_id: String,
    #[serde(default = "default_interactive_runtime_class")]
    runtime_class: String,
    #[serde(default = "default_channel_origin")]
    origin: String,
    #[serde(default)]
    cron_run_id: Option<String>,
    platform: String,
    #[serde(default)]
    account_id: Option<String>,
    channel_id: String,
    #[serde(default)]
    user_id: Option<String>,
    session_key: String,
    #[serde(default)]
    virtual_session_id: Option<String>,
    #[serde(default)]
    session_lane_key: Option<String>,
    owner: RuntimeQueueLeaseOwner,
    started_at_ms: i64,
    lease_expires_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
enum RuntimeQueueLeaseOwner {
    Legacy(String),
    Envelope(RuntimeQueueLeaseOwnerEnvelope),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RuntimeQueueLeaseOwnerEnvelope {
    kind: String,
    service_id: String,
    generation_id: String,
    pid: i64,
    #[serde(alias = "processStartTime")]
    process_start_time_ms: i64,
    acquired_at_ms: i64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeQueueLeaseReconciliationReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub service_id: String,
    pub generation_id: String,
    pub reaped_leases: Vec<RuntimeQueueLeaseReconciledItem>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeQueueLeaseReconciledItem {
    pub queue_id: String,
    pub runtime_class: String,
    pub origin: String,
    pub cron_run_id: Option<String>,
    pub owner: Value,
    pub at_ms: i64,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum RuntimeSessionIdentityInventoryStatus {
    Canonical,
    NeedsNormalization,
    MissingAccountIdentity,
    Invalid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum RuntimeSessionIdentityInventorySource {
    Pending,
    Lease,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeSessionIdentityInventoryEntry {
    pub queue_id: String,
    pub source: RuntimeSessionIdentityInventorySource,
    pub status: RuntimeSessionIdentityInventoryStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canonical_lane_digest: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeSessionIdentityCollisionGroup {
    pub canonical_lane_digest: String,
    pub queue_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeSessionIdentityInventoryReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub inspected_at_ms: i64,
    pub entries: Vec<RuntimeSessionIdentityInventoryEntry>,
    pub collision_groups: Vec<RuntimeSessionIdentityCollisionGroup>,
    pub warnings: Vec<String>,
}

struct RuntimeQueueLeaseLock {
    path: PathBuf,
    file: Option<fs::File>,
}

impl Drop for RuntimeQueueLeaseLock {
    fn drop(&mut self) {
        let _ = self.file.take();
        let _ = fs::remove_file(&self.path);
    }
}

pub fn prepare_runtime_queue_item(
    options: RuntimeQueuePrepareOptions,
) -> io::Result<RuntimeQueuePrepareReport> {
    let queue_dir = options.harness_home.join("state").join("runtime-queue");
    let queue_file = queue_dir.join("pending.jsonl");
    let execution_receipts_file = queue_dir.join("execution-receipts.jsonl");
    fs::create_dir_all(&queue_dir)?;

    let mut warnings = Vec::new();
    let now_ms = current_log_time_ms()?;
    let mut preliminary_pending_items = read_pending_items(&queue_file, &mut warnings)?;
    canonicalize_pending_items_for_dispatch(
        &options.harness_home,
        &execution_receipts_file,
        &mut preliminary_pending_items,
        now_ms,
        &mut warnings,
    )?;
    // A terminal-control marker may cause the derived active index to prune a
    // row before this invocation reaches its requested-item branch. Keep the
    // bounded, source-authoritative snapshot from immediately before that
    // cleanup so the terminal path can still emit one fully attributed
    // suppression receipt. This snapshot is never used to select runnable
    // work after the index has been refreshed.
    let preliminary_pending_by_id = preliminary_pending_items
        .iter()
        .cloned()
        .map(|item| (item.queue_id.clone(), item))
        .collect::<HashMap<_, _>>();
    let prepared_receipts = prepared_execution_receipts_from_index(&queue_dir, &mut warnings)?;
    let run_once_index = refresh_runtime_queue_state_index(&queue_dir, &mut warnings)?;
    let mut terminal_run_ids = terminal_run_once_ids_from_index(&run_once_index);
    terminal_run_ids.extend(pending_terminal_control_ids(
        &queue_dir,
        &preliminary_pending_items,
        &run_once_index,
        &mut warnings,
    )?);
    let terminal_queue_ids = terminal_run_ids.iter().cloned().collect::<BTreeSet<_>>();
    if let Err(error) =
        prune_terminal_queue_ids_from_pending_index(&queue_dir, &terminal_queue_ids, &mut warnings)
    {
        // The pending JSONL remains authoritative, and selection below still
        // filters terminal IDs.  A failed derived-index cleanup must not
        // prevent a runnable queue item from being prepared.
        warnings.push(format!(
            "could not prune caller-proven terminal pending rows from the active index: {error}"
        ));
    }
    let retry_pending_run_ids = retry_pending_run_once_ids_from_index(&run_once_index);
    let auth_deferred_run_ids = auth_deferred_run_once_ids_from_index(&run_once_index);
    let lock_runtime_class = select_lock_runtime_class(
        options.queue_id.as_deref(),
        &preliminary_pending_items,
        &prepared_receipts,
        &terminal_run_ids,
    );
    let lease_owner = runtime_queue_lease_owner_from_env(now_ms);
    let _lease_lock = match acquire_runtime_queue_lease_lock_with_retry(
        &queue_dir,
        &lock_runtime_class,
        Duration::from_millis(RUNTIME_LEASE_ACQUIRE_LOCK_RETRY_MS),
    )? {
        Some(lock) => lock,
        None => {
            let receipt = RuntimeExecutionReceipt {
                queue_id: options.queue_id,
                status: RuntimeExecutionReceiptStatus::LeaseBusy,
                channel_lane: None,
                runtime_class: Some(lock_runtime_class.clone()),
                origin: None,
                cron_run_id: None,
                scheduled_for_ms: None,
                execution_dir: None,
                prompt_bundle_json: None,
                prompt_markdown: None,
                runtime_workspace: None,
                inbound_media_artifacts: Vec::new(),
                continuation: RuntimeContinuationMetadata::legacy(),
                terminal_control_matched: None,
                terminal_control_source: None,
                suppressed_run_once_reason: None,
                reason: format!(
                    "runtime queue lease lock is busy for class `{lock_runtime_class}`"
                ),
            };
            append_json_line(&execution_receipts_file, &receipt)?;
            return Ok(RuntimeQueuePrepareReport {
                schema: RUNTIME_QUEUE_PREPARE_REPORT_SCHEMA,
                harness_home: options.harness_home,
                queue_file,
                execution_receipts_file,
                item: None,
                receipt,
                warnings,
            });
        }
    };
    let mut lease_state =
        read_runtime_queue_leases(&queue_dir, &lock_runtime_class, &mut warnings)?;
    purge_runtime_queue_leases(
        &mut lease_state,
        now_ms,
        &terminal_run_ids,
        &retry_pending_run_ids,
        Some(&execution_receipts_file),
        &mut warnings,
    )?;
    let mut pending_items = read_pending_items(&queue_file, &mut warnings)?;
    canonicalize_pending_items_for_dispatch(
        &options.harness_home,
        &execution_receipts_file,
        &mut pending_items,
        now_ms,
        &mut warnings,
    )?;
    let pending_by_id = pending_items
        .iter()
        .cloned()
        .map(|item| (item.queue_id.clone(), item))
        .collect::<HashMap<_, _>>();
    if let Some(requested_queue_id) = options.queue_id.as_deref() {
        let session_key = pending_by_id
            .get(requested_queue_id)
            .map(|item| item.session_key.as_str());
        if let QueueTerminalControl::Terminal(control) =
            resolve_queue_terminal_control(&options.harness_home, requested_queue_id, session_key)?
        {
            if let Some(pending) = pending_by_id
                .get(requested_queue_id)
                .or_else(|| preliminary_pending_by_id.get(requested_queue_id))
            {
                record_terminal_control_suppression(
                    &options.harness_home,
                    requested_queue_id,
                    Some(&pending.runtime_class),
                    Some(&pending.origin),
                    pending.cron_run_id.as_deref(),
                    pending.scheduled_for_ms,
                    &pending.continuation,
                    &control,
                )?;
                write_runtime_queue_leases(&queue_dir, &lock_runtime_class, &lease_state)?;
                let receipt = terminal_control_no_pending_receipt(
                    requested_queue_id,
                    Some(pending.runtime_class.clone()),
                    Some(pending.origin.clone()),
                    pending.cron_run_id.clone(),
                    pending.scheduled_for_ms,
                    pending.continuation.clone(),
                    &control,
                );
                append_json_line(&execution_receipts_file, &receipt)?;
                return Ok(RuntimeQueuePrepareReport {
                    schema: RUNTIME_QUEUE_PREPARE_REPORT_SCHEMA,
                    harness_home: options.harness_home,
                    queue_file,
                    execution_receipts_file,
                    item: None,
                    receipt,
                    warnings,
                });
            }
            if let Some(prepared) = prepared_receipts.get(requested_queue_id) {
                record_terminal_control_suppression(
                    &options.harness_home,
                    requested_queue_id,
                    prepared.runtime_class.as_deref(),
                    prepared.origin.as_deref(),
                    prepared.cron_run_id.as_deref(),
                    prepared.scheduled_for_ms,
                    &prepared.continuation,
                    &control,
                )?;
                write_runtime_queue_leases(&queue_dir, &lock_runtime_class, &lease_state)?;
                let receipt = terminal_control_no_pending_receipt(
                    requested_queue_id,
                    prepared.runtime_class.clone(),
                    prepared.origin.clone(),
                    prepared.cron_run_id.clone(),
                    prepared.scheduled_for_ms,
                    prepared.continuation.clone(),
                    &control,
                );
                append_json_line(&execution_receipts_file, &receipt)?;
                return Ok(RuntimeQueuePrepareReport {
                    schema: RUNTIME_QUEUE_PREPARE_REPORT_SCHEMA,
                    harness_home: options.harness_home,
                    queue_file,
                    execution_receipts_file,
                    item: None,
                    receipt,
                    warnings,
                });
            }
        }
    }
    if let Some(requested_queue_id) = options.queue_id.as_deref()
        && terminal_run_ids.contains(requested_queue_id)
    {
        write_runtime_queue_leases(&queue_dir, &lock_runtime_class, &lease_state)?;
        let pending = pending_by_id
            .get(requested_queue_id)
            .or_else(|| preliminary_pending_by_id.get(requested_queue_id));
        let receipt = RuntimeExecutionReceipt {
            queue_id: Some(requested_queue_id.to_string()),
            status: RuntimeExecutionReceiptStatus::NoPendingItem,
            channel_lane: None,
            runtime_class: pending.map(|item| item.runtime_class.clone()),
            origin: pending.map(|item| item.origin.clone()),
            cron_run_id: pending.and_then(|item| item.cron_run_id.clone()),
            scheduled_for_ms: pending.and_then(|item| item.scheduled_for_ms),
            execution_dir: None,
            prompt_bundle_json: None,
            prompt_markdown: None,
            runtime_workspace: None,
            inbound_media_artifacts: Vec::new(),
            continuation: pending
                .map(|item| item.continuation.clone())
                .unwrap_or_else(RuntimeContinuationMetadata::legacy),
            terminal_control_matched: None,
            terminal_control_source: None,
            suppressed_run_once_reason: None,
            reason: "requested runtime queue item already has a terminal run receipt".to_string(),
        };
        append_json_line(&execution_receipts_file, &receipt)?;
        return Ok(RuntimeQueuePrepareReport {
            schema: RUNTIME_QUEUE_PREPARE_REPORT_SCHEMA,
            harness_home: options.harness_home,
            queue_file,
            execution_receipts_file,
            item: None,
            receipt,
            warnings,
        });
    }
    if let Some(requested_queue_id) = options.queue_id.as_deref()
        && auth_deferred_run_ids.contains(requested_queue_id)
    {
        write_runtime_queue_leases(&queue_dir, &lock_runtime_class, &lease_state)?;
        let pending = pending_by_id
            .get(requested_queue_id)
            .or_else(|| preliminary_pending_by_id.get(requested_queue_id));
        let receipt = RuntimeExecutionReceipt {
            queue_id: Some(requested_queue_id.to_string()),
            status: RuntimeExecutionReceiptStatus::NoPendingItem,
            channel_lane: None,
            runtime_class: pending.map(|item| item.runtime_class.clone()),
            origin: pending.map(|item| item.origin.clone()),
            cron_run_id: pending.and_then(|item| item.cron_run_id.clone()),
            scheduled_for_ms: pending.and_then(|item| item.scheduled_for_ms),
            execution_dir: None,
            prompt_bundle_json: None,
            prompt_markdown: None,
            runtime_workspace: None,
            inbound_media_artifacts: Vec::new(),
            continuation: pending
                .map(|item| item.continuation.clone())
                .unwrap_or_else(RuntimeContinuationMetadata::legacy),
            terminal_control_matched: None,
            terminal_control_source: None,
            suppressed_run_once_reason: None,
            reason: "requested runtime queue item is waiting for operator authentication"
                .to_string(),
        };
        append_json_line(&execution_receipts_file, &receipt)?;
        return Ok(RuntimeQueuePrepareReport {
            schema: RUNTIME_QUEUE_PREPARE_REPORT_SCHEMA,
            harness_home: options.harness_home,
            queue_file,
            execution_receipts_file,
            item: None,
            receipt,
            warnings,
        });
    }
    if let Some(requested_queue_id) = options.queue_id.as_deref()
        && let Some(prepared) = prepared_receipts.get(requested_queue_id)
    {
        let pending = pending_by_id.get(requested_queue_id);
        let session_key = pending.map(|item| item.session_key.as_str());
        if let QueueTerminalControl::Terminal(control) =
            resolve_queue_terminal_control(&options.harness_home, requested_queue_id, session_key)?
        {
            let runtime_class = pending
                .map(|item| item.runtime_class.clone())
                .or_else(|| prepared.runtime_class.clone());
            let origin = pending
                .map(|item| item.origin.clone())
                .or_else(|| prepared.origin.clone());
            let cron_run_id = pending
                .and_then(|item| item.cron_run_id.clone())
                .or_else(|| prepared.cron_run_id.clone());
            let scheduled_for_ms = pending
                .and_then(|item| item.scheduled_for_ms)
                .or(prepared.scheduled_for_ms);
            let continuation = pending
                .map(|item| item.continuation.clone())
                .unwrap_or_else(|| prepared.continuation.clone());
            record_terminal_control_suppression(
                &options.harness_home,
                requested_queue_id,
                runtime_class.as_deref(),
                origin.as_deref(),
                cron_run_id.as_deref(),
                scheduled_for_ms,
                &continuation,
                &control,
            )?;
            write_runtime_queue_leases(&queue_dir, &lock_runtime_class, &lease_state)?;
            let receipt = terminal_control_no_pending_receipt(
                requested_queue_id,
                runtime_class,
                origin,
                cron_run_id,
                scheduled_for_ms,
                continuation,
                &control,
            );
            append_json_line(&execution_receipts_file, &receipt)?;
            return Ok(RuntimeQueuePrepareReport {
                schema: RUNTIME_QUEUE_PREPARE_REPORT_SCHEMA,
                harness_home: options.harness_home,
                queue_file,
                execution_receipts_file,
                item: None,
                receipt,
                warnings,
            });
        }
        if lease_state.leases.contains_key(requested_queue_id) {
            let receipt = RuntimeExecutionReceipt {
                queue_id: Some(requested_queue_id.to_string()),
                status: RuntimeExecutionReceiptStatus::NoPendingItem,
                channel_lane: None,
                runtime_class: prepared.runtime_class.clone(),
                origin: prepared.origin.clone(),
                cron_run_id: prepared.cron_run_id.clone(),
                scheduled_for_ms: prepared.scheduled_for_ms,
                execution_dir: None,
                prompt_bundle_json: None,
                prompt_markdown: None,
                runtime_workspace: None,
                inbound_media_artifacts: Vec::new(),
                continuation: prepared.continuation.clone(),
                terminal_control_matched: None,
                terminal_control_source: None,
                suppressed_run_once_reason: None,
                reason: "requested runtime queue item is already leased".to_string(),
            };
            write_runtime_queue_leases(&queue_dir, &lock_runtime_class, &lease_state)?;
            append_json_line(&execution_receipts_file, &receipt)?;
            return Ok(RuntimeQueuePrepareReport {
                schema: RUNTIME_QUEUE_PREPARE_REPORT_SCHEMA,
                harness_home: options.harness_home,
                queue_file,
                execution_receipts_file,
                item: None,
                receipt,
                warnings,
            });
        }
        let mut acquired_prepared_lease = None;
        if let Some(pending) = pending_by_id.get(requested_queue_id) {
            if let Some(blocker) =
                retry_schedule_dispatch_blocker(&run_once_index, requested_queue_id, now_ms)
            {
                let receipt = RuntimeExecutionReceipt {
                    queue_id: Some(requested_queue_id.to_string()),
                    status: RuntimeExecutionReceiptStatus::NoPendingItem,
                    channel_lane: None,
                    runtime_class: Some(pending.runtime_class.clone()),
                    origin: Some(pending.origin.clone()),
                    cron_run_id: pending.cron_run_id.clone(),
                    scheduled_for_ms: pending.scheduled_for_ms,
                    execution_dir: None,
                    prompt_bundle_json: None,
                    prompt_markdown: None,
                    runtime_workspace: None,
                    inbound_media_artifacts: Vec::new(),
                    continuation: pending.continuation.clone(),
                    terminal_control_matched: None,
                    terminal_control_source: None,
                    suppressed_run_once_reason: None,
                    reason: blocker,
                };
                write_runtime_queue_leases(&queue_dir, &lock_runtime_class, &lease_state)?;
                append_json_line(&execution_receipts_file, &receipt)?;
                return Ok(RuntimeQueuePrepareReport {
                    schema: RUNTIME_QUEUE_PREPARE_REPORT_SCHEMA,
                    harness_home: options.harness_home,
                    queue_file,
                    execution_receipts_file,
                    item: None,
                    receipt,
                    warnings,
                });
            }
            if let Some(blocker) =
                cron_runtime_dispatch_blocker_for_item(&options.harness_home, pending, now_ms)?
            {
                tombstone_runtime_queue_item_skipped(&queue_dir, pending, &blocker)?;
                let receipt = RuntimeExecutionReceipt {
                    queue_id: Some(requested_queue_id.to_string()),
                    status: RuntimeExecutionReceiptStatus::NoPendingItem,
                    channel_lane: None,
                    runtime_class: Some(pending.runtime_class.clone()),
                    origin: Some(pending.origin.clone()),
                    cron_run_id: pending.cron_run_id.clone(),
                    scheduled_for_ms: pending.scheduled_for_ms,
                    execution_dir: None,
                    prompt_bundle_json: None,
                    prompt_markdown: None,
                    runtime_workspace: None,
                    inbound_media_artifacts: Vec::new(),
                    continuation: pending.continuation.clone(),
                    terminal_control_matched: None,
                    terminal_control_source: None,
                    suppressed_run_once_reason: None,
                    reason: format!("runtime queue item blocked by {blocker}"),
                };
                write_runtime_queue_leases(&queue_dir, &lock_runtime_class, &lease_state)?;
                append_json_line(&execution_receipts_file, &receipt)?;
                return Ok(RuntimeQueuePrepareReport {
                    schema: RUNTIME_QUEUE_PREPARE_REPORT_SCHEMA,
                    harness_home: options.harness_home,
                    queue_file,
                    execution_receipts_file,
                    item: None,
                    receipt,
                    warnings,
                });
            }
            if let Some(blocker) =
                runtime_capacity_blocker(&options.harness_home, &lease_state, pending)?
            {
                let receipt = RuntimeExecutionReceipt {
                    queue_id: Some(requested_queue_id.to_string()),
                    status: RuntimeExecutionReceiptStatus::NoPendingItem,
                    channel_lane: None,
                    runtime_class: Some(pending.runtime_class.clone()),
                    origin: Some(pending.origin.clone()),
                    cron_run_id: pending.cron_run_id.clone(),
                    scheduled_for_ms: pending.scheduled_for_ms,
                    execution_dir: None,
                    prompt_bundle_json: None,
                    prompt_markdown: None,
                    runtime_workspace: None,
                    inbound_media_artifacts: Vec::new(),
                    continuation: pending.continuation.clone(),
                    terminal_control_matched: None,
                    terminal_control_source: None,
                    suppressed_run_once_reason: None,
                    reason: format!("runtime queue capacity blocked by {blocker}"),
                };
                write_runtime_queue_leases(&queue_dir, &lock_runtime_class, &lease_state)?;
                append_json_line(&execution_receipts_file, &receipt)?;
                return Ok(RuntimeQueuePrepareReport {
                    schema: RUNTIME_QUEUE_PREPARE_REPORT_SCHEMA,
                    harness_home: options.harness_home,
                    queue_file,
                    execution_receipts_file,
                    item: None,
                    receipt,
                    warnings,
                });
            }
            if let Some(blocker) = same_session_fifo_blocker(
                &pending_items,
                pending,
                &terminal_run_ids,
                &load_runtime_dispatch_config(&options.harness_home)?,
            ) {
                let receipt = RuntimeExecutionReceipt {
                    queue_id: Some(requested_queue_id.to_string()),
                    status: RuntimeExecutionReceiptStatus::NoPendingItem,
                    channel_lane: None,
                    runtime_class: Some(pending.runtime_class.clone()),
                    origin: Some(pending.origin.clone()),
                    cron_run_id: pending.cron_run_id.clone(),
                    scheduled_for_ms: pending.scheduled_for_ms,
                    execution_dir: None,
                    prompt_bundle_json: None,
                    prompt_markdown: None,
                    runtime_workspace: None,
                    inbound_media_artifacts: Vec::new(),
                    continuation: pending.continuation.clone(),
                    terminal_control_matched: None,
                    terminal_control_source: None,
                    suppressed_run_once_reason: None,
                    reason: format!("runtime queue item blocked by {blocker}"),
                };
                write_runtime_queue_leases(&queue_dir, &lock_runtime_class, &lease_state)?;
                append_json_line(&execution_receipts_file, &receipt)?;
                return Ok(RuntimeQueuePrepareReport {
                    schema: RUNTIME_QUEUE_PREPARE_REPORT_SCHEMA,
                    harness_home: options.harness_home,
                    queue_file,
                    execution_receipts_file,
                    item: None,
                    receipt,
                    warnings,
                });
            }
            lease_runtime_queue_item(&mut lease_state, pending, &lease_owner, now_ms);
            acquired_prepared_lease = Some(pending.clone());
        }
        write_runtime_queue_leases(&queue_dir, &lock_runtime_class, &lease_state)?;
        if let Some(pending) = acquired_prepared_lease.as_ref() {
            if let Err(error) =
                consume_coordinator_resume_after_lease(&options.harness_home, pending, now_ms)
            {
                lease_state.leases.remove(&pending.queue_id);
                write_runtime_queue_leases(&queue_dir, &lock_runtime_class, &lease_state)?;
                return Err(error);
            }
            append_json_line(
                &execution_receipts_file,
                &lease_acquired_receipt(
                    pending,
                    "runtime queue lease acquired for requested prepared item resume",
                ),
            )?;
        }
        let receipt = RuntimeExecutionReceipt {
            queue_id: Some(requested_queue_id.to_string()),
            status: RuntimeExecutionReceiptStatus::AlreadyPrepared,
            channel_lane: None,
            runtime_class: prepared.runtime_class.clone(),
            origin: prepared.origin.clone(),
            cron_run_id: prepared.cron_run_id.clone(),
            scheduled_for_ms: prepared.scheduled_for_ms,
            execution_dir: prepared.execution_dir.clone(),
            prompt_bundle_json: prepared.prompt_bundle_json.clone(),
            prompt_markdown: prepared.prompt_markdown.clone(),
            runtime_workspace: prepared.runtime_workspace.clone(),
            inbound_media_artifacts: prepared.inbound_media_artifacts.clone(),
            continuation: prepared.continuation.clone(),
            terminal_control_matched: None,
            terminal_control_source: None,
            suppressed_run_once_reason: None,
            reason: "requested runtime queue item was already prepared".to_string(),
        };
        append_json_line(&execution_receipts_file, &receipt)?;
        append_harness_log(
            &options.harness_home,
            &HarnessLogEvent::new(
                current_log_time_ms()?,
                HarnessLogLevel::Info,
                "runtime-queue",
                "queue.prepare.already-prepared",
                receipt.reason.clone(),
            )
            .queue_id(receipt.queue_id.clone())
            .path(receipt.execution_dir.clone()),
        )?;
        return Ok(RuntimeQueuePrepareReport {
            schema: RUNTIME_QUEUE_PREPARE_REPORT_SCHEMA,
            harness_home: options.harness_home,
            queue_file,
            execution_receipts_file,
            item: None,
            receipt,
            warnings,
        });
    }
    if options.queue_id.is_none() {
        for (queue_id, prepared) in prepared_receipts.iter().filter(|(queue_id, _)| {
            !terminal_run_ids.contains(*queue_id)
                && !auth_deferred_run_ids.contains(*queue_id)
                && !lease_state.leases.contains_key(*queue_id)
        }) {
            if let Some(pending) = pending_by_id.get(queue_id) {
                if pending.runtime_class != lock_runtime_class {
                    continue;
                }
                if let Some(blocker) =
                    retry_schedule_dispatch_blocker(&run_once_index, queue_id, now_ms)
                {
                    warnings.push(format!(
                        "prepared runtime queue item `{queue_id}` blocked by {blocker}; checking queued items"
                    ));
                    continue;
                }
                if let QueueTerminalControl::Terminal(control) = resolve_queue_terminal_control(
                    &options.harness_home,
                    &pending.queue_id,
                    Some(&pending.session_key),
                )? {
                    warnings.push(format!(
                        "prepared runtime queue item `{queue_id}` suppressed by terminal control {}",
                        control.source.as_str()
                    ));
                    record_terminal_control_suppression(
                        &options.harness_home,
                        &pending.queue_id,
                        Some(&pending.runtime_class),
                        Some(&pending.origin),
                        pending.cron_run_id.as_deref(),
                        pending.scheduled_for_ms,
                        &pending.continuation,
                        &control,
                    )?;
                    continue;
                }
                if let Some(blocker) =
                    cron_runtime_dispatch_blocker_for_item(&options.harness_home, pending, now_ms)?
                {
                    warnings.push(format!(
                        "prepared runtime queue item `{queue_id}` blocked by {blocker}; tombstoning"
                    ));
                    tombstone_runtime_queue_item_skipped(&queue_dir, pending, &blocker)?;
                    continue;
                }
                if let Some(blocker) =
                    runtime_capacity_blocker(&options.harness_home, &lease_state, pending)?
                {
                    warnings.push(format!(
                    "prepared runtime queue item `{queue_id}` blocked by {blocker}; checking queued items"
                ));
                } else {
                    if let Some(blocker) = same_session_fifo_blocker(
                        &pending_items,
                        pending,
                        &terminal_run_ids,
                        &load_runtime_dispatch_config(&options.harness_home)?,
                    ) {
                        warnings.push(format!(
                            "prepared runtime queue item `{queue_id}` blocked by {blocker}; checking queued items"
                        ));
                        continue;
                    }
                    lease_runtime_queue_item(&mut lease_state, pending, &lease_owner, now_ms);
                    write_runtime_queue_leases(&queue_dir, &lock_runtime_class, &lease_state)?;
                    if let Err(error) = consume_coordinator_resume_after_lease(
                        &options.harness_home,
                        pending,
                        now_ms,
                    ) {
                        lease_state.leases.remove(&pending.queue_id);
                        write_runtime_queue_leases(&queue_dir, &lock_runtime_class, &lease_state)?;
                        return Err(error);
                    }
                    append_json_line(
                        &execution_receipts_file,
                        &lease_acquired_receipt(
                            pending,
                            "runtime queue lease acquired for prepared item resume",
                        ),
                    )?;
                    let receipt = RuntimeExecutionReceipt {
                    queue_id: Some(queue_id.clone()),
                    status: RuntimeExecutionReceiptStatus::AlreadyPrepared,
                    channel_lane: None,
                    runtime_class: prepared.runtime_class.clone(),
                    origin: prepared.origin.clone(),
                    cron_run_id: prepared.cron_run_id.clone(),
                    scheduled_for_ms: prepared.scheduled_for_ms,
                    execution_dir: prepared.execution_dir.clone(),
                    prompt_bundle_json: prepared.prompt_bundle_json.clone(),
                    prompt_markdown: prepared.prompt_markdown.clone(),
                    runtime_workspace: prepared.runtime_workspace.clone(),
                    inbound_media_artifacts: prepared.inbound_media_artifacts.clone(),
                    continuation: prepared.continuation.clone(),
                    terminal_control_matched: None,
                    terminal_control_source: None,
                    suppressed_run_once_reason: None,
                    reason:
                        "resuming previously prepared runtime queue item without terminal run receipt"
                            .to_string(),
                };
                    append_json_line(&execution_receipts_file, &receipt)?;
                    append_harness_log(
                        &options.harness_home,
                        &HarnessLogEvent::new(
                            current_log_time_ms()?,
                            HarnessLogLevel::Info,
                            "runtime-queue",
                            "queue.prepare.resume-prepared",
                            receipt.reason.clone(),
                        )
                        .queue_id(receipt.queue_id.clone())
                        .path(receipt.execution_dir.clone()),
                    )?;
                    return Ok(RuntimeQueuePrepareReport {
                        schema: RUNTIME_QUEUE_PREPARE_REPORT_SCHEMA,
                        harness_home: options.harness_home,
                        queue_file,
                        execution_receipts_file,
                        item: None,
                        receipt,
                        warnings,
                    });
                }
            } else {
                warnings.push(format!(
                    "prepared runtime queue item `{queue_id}` had no pending queue metadata; skipping automatic resume"
                ));
            }
        }
    }
    let prepared_ids = prepared_receipts.keys().cloned().collect::<HashSet<_>>();
    let Some(pending) = select_pending_item(
        pending_items,
        options.queue_id.as_deref(),
        &prepared_ids,
        &terminal_run_ids,
        &auth_deferred_run_ids,
        &run_once_index,
        &lease_state,
        &lock_runtime_class,
        &options.harness_home,
        &queue_dir,
        now_ms,
        &mut warnings,
    )?
    else {
        write_runtime_queue_leases(&queue_dir, &lock_runtime_class, &lease_state)?;
        if options.queue_id.is_none() {
            let mut terminal_candidates = preliminary_pending_by_id.values().collect::<Vec<_>>();
            terminal_candidates.sort_by(|left, right| {
                runtime_selection_key(left)
                    .cmp(&runtime_selection_key(right))
                    .then_with(|| left.queue_id.cmp(&right.queue_id))
            });
            for pending in terminal_candidates {
                if let QueueTerminalControl::Terminal(control) = resolve_queue_terminal_control(
                    &options.harness_home,
                    &pending.queue_id,
                    Some(&pending.session_key),
                )? {
                    record_terminal_control_suppression(
                        &options.harness_home,
                        &pending.queue_id,
                        Some(&pending.runtime_class),
                        Some(&pending.origin),
                        pending.cron_run_id.as_deref(),
                        pending.scheduled_for_ms,
                        &pending.continuation,
                        &control,
                    )?;
                    let receipt = terminal_control_no_pending_receipt(
                        &pending.queue_id,
                        Some(pending.runtime_class.clone()),
                        Some(pending.origin.clone()),
                        pending.cron_run_id.clone(),
                        pending.scheduled_for_ms,
                        pending.continuation.clone(),
                        &control,
                    );
                    append_json_line(&execution_receipts_file, &receipt)?;
                    return Ok(RuntimeQueuePrepareReport {
                        schema: RUNTIME_QUEUE_PREPARE_REPORT_SCHEMA,
                        harness_home: options.harness_home,
                        queue_file,
                        execution_receipts_file,
                        item: None,
                        receipt,
                        warnings,
                    });
                }
            }
        }
        let receipt = RuntimeExecutionReceipt {
            queue_id: options.queue_id,
            status: RuntimeExecutionReceiptStatus::NoPendingItem,
            channel_lane: None,
            runtime_class: None,
            origin: None,
            cron_run_id: None,
            scheduled_for_ms: None,
            execution_dir: None,
            prompt_bundle_json: None,
            prompt_markdown: None,
            runtime_workspace: None,
            inbound_media_artifacts: Vec::new(),
            continuation: RuntimeContinuationMetadata::legacy(),
            terminal_control_matched: None,
            terminal_control_source: None,
            suppressed_run_once_reason: None,
            reason: "no matching queued runtime item found".to_string(),
        };
        append_json_line(&execution_receipts_file, &receipt)?;
        append_harness_log(
            &options.harness_home,
            &HarnessLogEvent::new(
                current_log_time_ms()?,
                HarnessLogLevel::Info,
                "runtime-queue",
                "queue.prepare.no-pending",
                receipt.reason.clone(),
            )
            .queue_id(receipt.queue_id.clone()),
        )?;
        return Ok(RuntimeQueuePrepareReport {
            schema: RUNTIME_QUEUE_PREPARE_REPORT_SCHEMA,
            harness_home: options.harness_home,
            queue_file,
            execution_receipts_file,
            item: None,
            receipt,
            warnings,
        });
    };
    if let Err(reason) = revalidate_pending_reasoning_snapshot(&options.harness_home, &pending) {
        let reason = format!("runtime reasoning snapshot failed admission: {reason}");
        write_runtime_queue_quarantine_marker(
            &options.harness_home,
            &pending.queue_id,
            &reason,
            now_ms,
        )?;
        warnings.push(format!(
            "runtime queue item `{}` was quarantined before lease acquisition: {reason}",
            pending.queue_id
        ));
        let control = QueueTerminalControlMatch {
            source: TerminalControlSource::Quarantine,
            reason,
            suppression_recorded: false,
            terminal_status: None,
            terminal_disposition: None,
            continuation_link: None,
        };
        record_terminal_control_suppression(
            &options.harness_home,
            &pending.queue_id,
            Some(&pending.runtime_class),
            Some(&pending.origin),
            pending.cron_run_id.as_deref(),
            pending.scheduled_for_ms,
            &pending.continuation,
            &control,
        )?;
        write_runtime_queue_leases(&queue_dir, &lock_runtime_class, &lease_state)?;
        let receipt = terminal_control_no_pending_receipt(
            &pending.queue_id,
            Some(pending.runtime_class.clone()),
            Some(pending.origin.clone()),
            pending.cron_run_id.clone(),
            pending.scheduled_for_ms,
            pending.continuation.clone(),
            &control,
        );
        append_json_line(&execution_receipts_file, &receipt)?;
        return Ok(RuntimeQueuePrepareReport {
            schema: RUNTIME_QUEUE_PREPARE_REPORT_SCHEMA,
            harness_home: options.harness_home,
            queue_file,
            execution_receipts_file,
            item: None,
            receipt,
            warnings,
        });
    }
    let pending = maybe_apply_context_rollover_before_turn(
        &options.harness_home,
        &queue_file,
        pending,
        now_ms,
        &mut warnings,
    )?;
    lease_runtime_queue_item(&mut lease_state, &pending, &lease_owner, now_ms);
    write_runtime_queue_leases(&queue_dir, &lock_runtime_class, &lease_state)?;
    if let Err(error) =
        consume_coordinator_resume_after_lease(&options.harness_home, &pending, now_ms)
    {
        lease_state.leases.remove(&pending.queue_id);
        write_runtime_queue_leases(&queue_dir, &lock_runtime_class, &lease_state)?;
        return Err(error);
    }
    append_json_line(
        &execution_receipts_file,
        &lease_acquired_receipt(
            &pending,
            "runtime queue lease acquired before prompt bundle preparation",
        ),
    )?;
    if pending.origin == "channel" {
        let progress_context = crate::AgentProgressContext {
            queue_id: pending.queue_id.clone(),
            agent_id: Some(pending.agent_id.clone()),
            account_id: pending.account_id.clone(),
            thread_id: None,
            session_key: pending.session_key.clone(),
            platform: pending.platform.clone(),
            channel_id: pending.channel_id.clone(),
            user_id: pending.user_id.clone(),
        };
        let preparing_event = crate::AgentProgressEvent::new(
            &progress_context,
            crate::AgentProgressKind::Runtime,
            "preparing",
            "Lease acquired; preparing runtime context.",
            crate::AgentProgressStatus::Started,
            now_ms,
        )
        .lifecycle(crate::AgentProgressLifecycle::Preparing)
        .source("runtime-worker");
        if let Err(error) =
            crate::append_agent_progress_event(&options.harness_home, &preparing_event)
        {
            warnings.push(format!(
                "failed to append preparing progress event for `{}`: {error}",
                pending.queue_id
            ));
        }
    }
    if let Err(error) = crate::latency::record_latency_stage(
        crate::latency::latency_receipts_file(&options.harness_home),
        &pending.queue_id,
        &pending.runtime_class,
        crate::latency::LatencyStage::LeaseAcquired,
        Some(now_ms),
    ) {
        warnings.push(format!(
            "failed to record lease-acquired latency stage for `{}`: {error}",
            pending.queue_id
        ));
    }

    let prompt_workspace = prompt_source_workspace(&pending.source_home, &pending.source_workspace);
    if !paths_equivalent(&prompt_workspace, &pending.source_workspace) {
        warnings.push(format!(
            "using imported prompt workspace {} instead of queued source workspace {}",
            prompt_workspace.display(),
            pending.source_workspace.display()
        ));
    }
    let source = AgentSource::with_workspace(&pending.source_home, &prompt_workspace);
    let registry = load_agent_registry(&source)?;
    let skill_index = build_runtime_skill_index(&source, &options.harness_home)?;
    let mut plan = build_turn_plan_for_account(
        &source,
        &registry,
        &skill_index,
        crate::TurnPlanInput {
            harness_home: Some(options.harness_home.clone()),
            platform: pending.platform.clone(),
            channel_id: pending.channel_id.clone(),
            user_id: pending.user_id.clone(),
            text: pending.message_text.clone(),
            inbound_context: pending.inbound_context.clone(),
            inbound_media_artifacts: pending.inbound_media_artifacts.clone(),
            requested_agent_id: Some(pending.agent_id.clone()),
            session_hint: Some(pending.session_key.clone()),
            skill_limit: pending.selected_skill_ids.len().max(5),
        },
        pending.account_id.clone(),
    )?;
    if let (Some(provider), Some(model)) = (&pending.provider, &pending.model) {
        plan.model_policy.provider = Some(provider.clone());
        plan.model_policy.model = Some(model.clone());
        plan.reasoning_preference = pending.reasoning_preference.clone();
        plan.backend_reasoning_policy = pending.backend_reasoning_policy.clone();
    }
    let actual_skill_ids = plan
        .selected_skills
        .iter()
        .map(|skill| skill.skill_id.clone())
        .collect::<Vec<_>>();
    if !pending.selected_skill_ids.is_empty() && pending.selected_skill_ids != actual_skill_ids {
        warnings.push(format!(
            "prepared skill selection differs from queued selection: queued={:?}, prepared={:?}",
            pending.selected_skill_ids, actual_skill_ids
        ));
    }
    let (full_lane, backend_context_generation) =
        derive_prompt_runtime_context(&pending, &mut warnings)?;
    let channel_lane = channel_state_lane_for_pending(&pending)?;
    let mut skill_shadow_runtime_receipt = None;
    let mut skill_virtual_session_id = None;
    if let Some(query) = plan.skill_shadow_v2_query.as_ref() {
        match (full_lane.as_ref(), channel_lane.as_ref()) {
            (Some(full_lane), Some(channel_lane)) => {
                let virtual_session_id = derive_virtual_session_id_v2(
                    channel_lane,
                    full_lane.root_virtual_session(),
                );
                match record_skill_shadow_runtime_receipt(
                    SkillShadowRuntimeReceiptOptions {
                        harness_home: &options.harness_home,
                        full_lane,
                        virtual_session_id: &virtual_session_id,
                        query,
                        skill_index: &skill_index,
                        active_serving_skills: &plan.selected_skills,
                        policy: SkillRouterV2Policy::default(),
                    },
                ) {
                    Ok((_, receipt)) => {
                        skill_virtual_session_id = Some(virtual_session_id);
                        skill_shadow_runtime_receipt = Some(receipt);
                    }
                    Err(error) => warnings.push(format!(
                        "skill router v2 shadow receipt could not be written: {error}"
                    )),
                }
            }
            _ => warnings.push(
                "skill router v2 shadow skipped because an exact account-aware lane was unavailable"
                    .to_string(),
            ),
        }
    }
    let mut prompt_options = options.prompt_options;
    prompt_options.harness_home = Some(options.harness_home.clone());
    // The queued item is the authoritative post-routing identity. Never accept
    // a caller-supplied lane or backend generation that can disagree with it.
    prompt_options.full_lane = full_lane.clone();
    prompt_options.backend_context_generation = Some(backend_context_generation.clone());
    let bundle = assemble_prompt_bundle(&plan, prompt_options)?;
    let mut virtual_skill_observation = None;
    let virtual_manifest_observe_enabled =
        match virtual_skill_manifest_observation_enabled(&options.harness_home) {
            Ok(enabled) => enabled,
            Err(error) => {
                warnings.push(format!(
                    "virtual skill manifest observation config could not be read: {error}"
                ));
                false
            }
        };
    if virtual_manifest_observe_enabled && skill_shadow_runtime_receipt.is_none() {
        warnings.push(
            "virtual skill manifest observation skipped because skills.matcher.shadowV2Enabled did not produce an exact-lane routing receipt"
                .to_string(),
        );
    }
    if virtual_manifest_observe_enabled
        && let (Some(routing_receipt), Some(full_lane), Some(virtual_session_id)) = (
            skill_shadow_runtime_receipt.as_ref(),
            full_lane.as_ref(),
            skill_virtual_session_id.as_deref(),
        )
    {
        match observe_virtual_skill_runtime(VirtualSkillRuntimeObserveOptions {
            harness_home: &options.harness_home,
            full_lane,
            virtual_session_id,
            backend_generation: &backend_context_generation,
            queue_id: &pending.queue_id,
            routing_receipt,
            prompt_bundle: &bundle,
        }) {
            Ok(report) => virtual_skill_observation = Some(report),
            Err(error) => warnings.push(format!(
                "virtual skill manifest observation could not be written: {error}"
            )),
        }
    }

    let execution_dir = queue_execution_dir(&options.harness_home, &pending.queue_id);
    fs::create_dir_all(&execution_dir)?;
    let prompt_files = write_prompt_bundle(&bundle, &execution_dir)?;
    let receipt_file = execution_dir.join("execution-receipt.json");
    let receipt_runtime_class = pending.runtime_class.clone();
    let receipt_origin = pending.origin.clone();
    let receipt_cron_run_id = pending.cron_run_id.clone();
    let receipt_scheduled_for_ms = pending.scheduled_for_ms;
    let item = RuntimeQueuePreparedItem {
        queue_id: pending.queue_id.clone(),
        admission_queue_id: pending.admission_queue_id.clone(),
        agent_id: pending.agent_id.clone(),
        session_key: pending.session_key.clone(),
        runtime_class: pending.runtime_class.clone(),
        origin: pending.origin.clone(),
        cron_run_id: pending.cron_run_id.clone(),
        scheduled_for_ms: pending.scheduled_for_ms,
        platform: pending.platform.clone(),
        account_id: pending.account_id.clone(),
        channel_id: pending.channel_id.clone(),
        user_id: pending.user_id.clone(),
        message_text: pending.message_text.clone(),
        inbound_context: pending.inbound_context.clone(),
        inbound_media_artifacts: pending.inbound_media_artifacts.clone(),
        provider: bundle.provider.clone(),
        model: bundle.model.clone(),
        reasoning_preference: bundle.reasoning_preference.clone(),
        backend_reasoning_policy: bundle.backend_reasoning_policy.clone(),
        authorized_execution_mode: pending.authorized_execution_mode.clone(),
        execution_dir: execution_dir.clone(),
        prompt_bundle_json: prompt_files.json.clone(),
        prompt_markdown: prompt_files.markdown.clone(),
        receipt_file: receipt_file.clone(),
        planned_transcript_file: pending.planned_transcript_file,
        planned_trajectory_file: pending.planned_trajectory_file,
        selected_skill_ids: actual_skill_ids,
        virtual_skill_manifest_file: virtual_skill_observation
            .as_ref()
            .map(|report| report.manifest_file.clone()),
        skill_delivery_receipt_files: virtual_skill_observation
            .map(|report| report.delivery_receipt_files)
            .unwrap_or_default(),
        continuation: pending.continuation.clone(),
    };
    let receipt = RuntimeExecutionReceipt {
        queue_id: Some(pending.queue_id),
        status: RuntimeExecutionReceiptStatus::Prepared,
        channel_lane,
        runtime_class: Some(receipt_runtime_class),
        origin: Some(receipt_origin),
        cron_run_id: receipt_cron_run_id,
        scheduled_for_ms: receipt_scheduled_for_ms,
        execution_dir: Some(execution_dir),
        prompt_bundle_json: Some(prompt_files.json),
        prompt_markdown: Some(prompt_files.markdown),
        runtime_workspace: pending.runtime_workspace,
        inbound_media_artifacts: item.inbound_media_artifacts.clone(),
        continuation: item.continuation.clone(),
        terminal_control_matched: None,
        terminal_control_source: None,
        suppressed_run_once_reason: None,
        reason: "prompt bundle prepared; Codex runtime adapter not invoked yet".to_string(),
    };
    write_json_atomic(&receipt_file, &receipt)?;
    append_json_line(&execution_receipts_file, &receipt)?;
    append_harness_log(
        &options.harness_home,
        &HarnessLogEvent::new(
            current_log_time_ms()?,
            HarnessLogLevel::Info,
            "runtime-queue",
            "queue.prepare.prepared",
            receipt.reason.clone(),
        )
        .queue_id(receipt.queue_id.clone())
        .session_key(Some(item.session_key.clone()))
        .agent_id(Some(item.agent_id.clone()))
        .path(Some(item.execution_dir.clone())),
    )?;

    Ok(RuntimeQueuePrepareReport {
        schema: RUNTIME_QUEUE_PREPARE_REPORT_SCHEMA,
        harness_home: options.harness_home,
        queue_file,
        execution_receipts_file,
        item: Some(item),
        receipt,
        warnings,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeQueueLeaseObservationOptions {
    pub harness_home: PathBuf,
    pub queue_id: String,
    pub observed_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeQueueLaneActivityObservationOptions {
    pub harness_home: PathBuf,
    pub owner: crate::worker_result_mailbox::ExactWorkerResultOwnerV1,
    pub observed_at_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum RuntimeQueueLeaseObservationStatus {
    Active,
    Released,
    Expired,
    DeadOwner,
    Unknown,
}

impl RuntimeQueueLeaseObservationStatus {
    /// `Expired` and `DeadOwner` still require reconciliation; only physical
    /// absence across every locked class is a confirmed release.
    pub fn is_confirmed_released(self) -> bool {
        self == Self::Released
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeQueueLeaseObservationReceipt {
    pub schema: &'static str,
    pub queue_id: String,
    pub status: RuntimeQueueLeaseObservationStatus,
    pub observed_at_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime_class: Option<String>,
    pub reason: String,
}

fn derive_prompt_runtime_context(
    pending: &PendingQueueItem,
    warnings: &mut Vec<String>,
) -> io::Result<(Option<FullLaneKeyV1>, String)> {
    let full_lane = pending
        .account_id
        .as_deref()
        .map(|account_id| {
            FullLaneKeyV1::new(
                &pending.platform,
                account_id,
                &pending.channel_id,
                &pending.user_id,
                &pending.agent_id,
                &pending.runtime_class,
                root_working_session_key(&pending.session_key),
                &pending.session_key,
            )
            .map_err(io::Error::other)
        })
        .transpose()?;

    let policy_mode = if pending.backend_reasoning_policy.is_some() {
        "managed"
    } else {
        "unmanaged"
    };
    let binding_file = prompt_codex_binding_file(&pending.planned_transcript_file);
    let thread_id = if binding_file.is_file() {
        match fs::read_to_string(&binding_file)
            .and_then(|text| serde_json::from_str::<Value>(&text).map_err(io::Error::other))
        {
            Ok(value) => value
                .get("threadId")
                .or_else(|| value.get("thread_id"))
                .or_else(|| value.get("codexThreadId"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty() && !value.chars().any(char::is_control))
                .map(ToString::to_string),
            Err(error) => {
                warnings.push(format!(
                    "could not read Codex binding for prompt generation at {}: {error}",
                    binding_file.display()
                ));
                None
            }
        }
    } else {
        None
    };
    let backend_context_generation = match thread_id {
        Some(thread_id) => format!("codex-thread:{thread_id}:policy={policy_mode}"),
        None => format!("codex-unbound:{}:policy={policy_mode}", pending.queue_id),
    };

    Ok((full_lane, backend_context_generation))
}

fn channel_state_lane_for_pending(
    pending: &PendingQueueItem,
) -> io::Result<Option<ChannelStateLane>> {
    let Some(account_id) = pending.account_id.as_deref() else {
        return Ok(None);
    };
    ChannelStateLane::new(
        &pending.platform,
        Some(account_id),
        &pending.channel_id,
        &pending.user_id,
        &pending.agent_id,
    )
    .map(Some)
}

fn prompt_codex_binding_file(transcript_file: &Path) -> PathBuf {
    let file_name = transcript_file
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("session.jsonl");
    transcript_file.with_file_name(format!("{file_name}.codex-app-server.json"))
}

pub fn inspect_runtime_queue_capacity(
    options: RuntimeQueueCapacityOptions,
) -> io::Result<RuntimeQueueCapacityReport> {
    let queue_dir = options.harness_home.join("state").join("runtime-queue");
    let queue_file = queue_dir.join("pending.jsonl");
    let execution_receipts_file = queue_dir.join("execution-receipts.jsonl");
    fs::create_dir_all(&queue_dir)?;

    let config = load_runtime_dispatch_config(&options.harness_home)?;
    let mut warnings = Vec::new();
    let now_ms = current_log_time_ms()?;
    let prepared_receipts = prepared_execution_receipts_from_index(&queue_dir, &mut warnings)?;
    let run_once_index = refresh_runtime_queue_state_index(&queue_dir, &mut warnings)?;
    let terminal_run_ids = terminal_run_once_ids_from_index(&run_once_index);
    let terminal_queue_ids = terminal_run_ids.iter().cloned().collect::<BTreeSet<_>>();
    if let Err(error) =
        prune_terminal_queue_ids_from_pending_index(&queue_dir, &terminal_queue_ids, &mut warnings)
    {
        warnings.push(format!(
            "could not prune caller-proven terminal pending rows from the active index during capacity inspection: {error}"
        ));
    }
    let retry_pending_run_ids = retry_pending_run_once_ids_from_index(&run_once_index);
    let auth_deferred_run_ids = auth_deferred_run_once_ids_from_index(&run_once_index);
    let pending_items = read_pending_items(&queue_file, &mut warnings)?;
    let pending_by_id = pending_items
        .iter()
        .cloned()
        .map(|item| (item.queue_id.clone(), item))
        .collect::<HashMap<_, _>>();
    let mut claimable_items = 0usize;
    let mut claimable_queue_ids = Vec::new();
    let mut leased_items = 0usize;
    let mut classes = Vec::new();
    let mut any_lock_busy = false;
    let runtime_classes = runtime_classes_for_capacity(&pending_items);
    for runtime_class in runtime_classes {
        let Some(_lease_lock) = acquire_runtime_queue_lease_lock_with_retry(
            &queue_dir,
            &runtime_class,
            Duration::from_millis(RUNTIME_LEASE_ACQUIRE_LOCK_RETRY_MS),
        )?
        else {
            any_lock_busy = true;
            warnings.push(format!(
                "runtime queue lease lock is busy for class `{runtime_class}`; class capacity assumed zero"
            ));
            classes.push(RuntimeQueueClassCapacity {
                leases_file: runtime_queue_leases_file(&queue_dir, &runtime_class),
                lock_file: runtime_queue_lease_lock_file(&queue_dir, &runtime_class),
                runtime_class,
                leased_items: 0,
                claimable_items: 0,
                lock_busy: true,
            });
            continue;
        };
        let mut lease_state = read_runtime_queue_leases(&queue_dir, &runtime_class, &mut warnings)?;
        purge_runtime_queue_leases(
            &mut lease_state,
            now_ms,
            &terminal_run_ids,
            &retry_pending_run_ids,
            Some(&execution_receipts_file),
            &mut warnings,
        )?;
        write_runtime_queue_leases(&queue_dir, &runtime_class, &lease_state)?;
        leased_items += lease_state.leases.len();
        let mut simulated = lease_state.clone();
        let capacity_owner = RuntimeQueueLeaseOwner::Legacy("capacity-inspect".to_string());
        let mut class_claimable = 0usize;
        let prepared_candidates = prepared_receipts
            .keys()
            .filter(|queue_id| {
                !terminal_run_ids.contains(*queue_id)
                    && !auth_deferred_run_ids.contains(*queue_id)
                    && !lease_state.leases.contains_key(*queue_id)
            })
            .cloned()
            .collect::<Vec<_>>();
        for queue_id in prepared_candidates {
            let Some(pending) = pending_by_id.get(&queue_id) else {
                continue;
            };
            if pending.runtime_class != runtime_class {
                continue;
            }
            if let Some(blocker) =
                same_session_fifo_blocker(&pending_items, pending, &terminal_run_ids, &config)
            {
                warnings.push(format!(
                    "prepared runtime queue item `{queue_id}` blocked by {blocker}; capacity excludes it"
                ));
                continue;
            }
            if let Some(blocker) =
                cron_runtime_dispatch_blocker_for_item(&options.harness_home, pending, now_ms)?
            {
                warnings.push(format!(
                    "prepared runtime queue item `{queue_id}` blocked by {blocker}; capacity excludes it"
                ));
                continue;
            }
            if let Some(blocker) =
                runtime_capacity_blocker(&options.harness_home, &simulated, pending)?
            {
                warnings.push(format!(
                    "prepared runtime queue item `{queue_id}` blocked by {blocker}; capacity excludes it"
                ));
                continue;
            }
            claimable_items += 1;
            class_claimable += 1;
            claimable_queue_ids.push(queue_id);
            lease_runtime_queue_item(&mut simulated, pending, &capacity_owner, now_ms);
        }

        let prepared_ids = prepared_receipts.keys().cloned().collect::<HashSet<_>>();
        for pending in pending_items
            .iter()
            .filter(|pending| pending.runtime_class == runtime_class)
        {
            if prepared_ids.contains(&pending.queue_id)
                || terminal_run_ids.contains(&pending.queue_id)
                || auth_deferred_run_ids.contains(&pending.queue_id)
                || simulated.leases.contains_key(&pending.queue_id)
            {
                continue;
            }
            if let Some(blocker) =
                cron_runtime_dispatch_blocker_for_item(&options.harness_home, pending, now_ms)?
            {
                warnings.push(format!(
                    "runtime queue item `{}` blocked by {}; capacity excludes it",
                    pending.queue_id, blocker
                ));
                continue;
            }
            if let Some(blocker) =
                same_session_fifo_blocker(&pending_items, pending, &terminal_run_ids, &config)
            {
                warnings.push(format!(
                    "runtime queue item `{}` blocked by {}; capacity excludes it",
                    pending.queue_id, blocker
                ));
                continue;
            }
            if let Some(blocker) =
                runtime_capacity_blocker(&options.harness_home, &simulated, pending)?
            {
                warnings.push(format!(
                    "runtime queue item `{}` blocked by {}; capacity excludes it",
                    pending.queue_id, blocker
                ));
                continue;
            }
            claimable_items += 1;
            class_claimable += 1;
            claimable_queue_ids.push(pending.queue_id.clone());
            lease_runtime_queue_item(&mut simulated, pending, &capacity_owner, now_ms);
        }
        classes.push(RuntimeQueueClassCapacity {
            leases_file: runtime_queue_leases_file(&queue_dir, &runtime_class),
            lock_file: runtime_queue_lease_lock_file(&queue_dir, &runtime_class),
            runtime_class,
            leased_items: lease_state.leases.len(),
            claimable_items: class_claimable,
            lock_busy: false,
        });
    }

    Ok(RuntimeQueueCapacityReport {
        schema: "agent-harness.runtime-queue-capacity.v1",
        harness_home: options.harness_home,
        queue_file,
        leases_file: runtime_queue_leases_file(&queue_dir, "interactive"),
        classes,
        claimable_items,
        claimable_queue_ids,
        leased_items,
        global_limit: config.global_concurrency_limit,
        agent_limit: config
            .classes
            .get("interactive")
            .map(|class| class.per_agent_max_active)
            .unwrap_or(config.global_concurrency_limit),
        agent_channel_limit: config
            .classes
            .get("interactive")
            .map(|class| class.per_channel_max_active)
            .unwrap_or(config.global_concurrency_limit),
        session_limit: config
            .classes
            .get("interactive")
            .map(|class| class.per_session_max_active)
            .unwrap_or(1),
        lease_lock_busy: any_lock_busy,
        warnings,
    })
}

pub fn load_runtime_dispatch_config(
    harness_home: impl AsRef<Path>,
) -> io::Result<RuntimeDispatchConfig> {
    let harness_home = harness_home.as_ref();
    let worker = load_worker_dispatch_config(harness_home)?;
    let mut config = RuntimeDispatchConfig {
        global_concurrency_limit: worker.global_concurrency_limit,
        interactive_reserve: worker.global_concurrency_limit.min(2),
        classes: BTreeMap::from([
            (
                "interactive".to_string(),
                RuntimeDispatchClassConfig {
                    max_active: worker.global_concurrency_limit,
                    per_agent_max_active: worker.group_concurrency_limit,
                    per_channel_max_active: worker.channel_concurrency_limit,
                    per_session_max_active: 1,
                    session_fifo: true,
                    same_session_main_agent_serialization: true,
                    per_job_max_active: usize::MAX,
                    max_queued_per_agent: usize::MAX,
                },
            ),
            (
                "cron".to_string(),
                RuntimeDispatchClassConfig {
                    max_active: worker.global_concurrency_limit.min(4),
                    per_agent_max_active: 1,
                    per_channel_max_active: 1,
                    per_session_max_active: 1,
                    session_fifo: true,
                    same_session_main_agent_serialization: false,
                    per_job_max_active: 1,
                    max_queued_per_agent: 20,
                },
            ),
            (
                "worker".to_string(),
                RuntimeDispatchClassConfig {
                    max_active: worker.global_concurrency_limit.min(2),
                    per_agent_max_active: worker.group_concurrency_limit.min(2),
                    per_channel_max_active: worker.channel_concurrency_limit.min(2),
                    per_session_max_active: usize::MAX,
                    session_fifo: false,
                    same_session_main_agent_serialization: false,
                    per_job_max_active: usize::MAX,
                    max_queued_per_agent: usize::MAX,
                },
            ),
            (
                "maintenance".to_string(),
                RuntimeDispatchClassConfig {
                    max_active: worker.global_concurrency_limit.min(1),
                    per_agent_max_active: 1,
                    per_channel_max_active: 1,
                    per_session_max_active: 1,
                    session_fifo: true,
                    same_session_main_agent_serialization: false,
                    per_job_max_active: usize::MAX,
                    max_queued_per_agent: usize::MAX,
                },
            ),
        ]),
        warnings: worker.warnings.clone(),
    };

    for path in crate::config::harness_config_candidates(harness_home) {
        if !path.is_file() {
            continue;
        }
        let text = fs::read_to_string(&path)?;
        let value = serde_json::from_str::<Value>(&text).map_err(io::Error::other)?;
        let Some(dispatch) = value.get("runtimeDispatch") else {
            continue;
        };
        if let Some(limit) = dispatch
            .get("globalConcurrencyLimit")
            .and_then(Value::as_u64)
            .and_then(|value| usize::try_from(value).ok())
        {
            config.global_concurrency_limit = limit;
        }
        if let Some(reserve) = dispatch
            .get("interactiveReserve")
            .or_else(|| dispatch.get("interactiveReserved"))
            .and_then(Value::as_u64)
            .and_then(|value| usize::try_from(value).ok())
        {
            config.interactive_reserve = reserve;
        }
        if let Some(classes) = dispatch.get("classes").and_then(Value::as_object) {
            for (runtime_class, class_value) in classes {
                let mut class_config = config.classes.get(runtime_class).cloned().unwrap_or(
                    RuntimeDispatchClassConfig {
                        max_active: config.global_concurrency_limit,
                        per_agent_max_active: config.global_concurrency_limit,
                        per_channel_max_active: config.global_concurrency_limit,
                        per_session_max_active: usize::MAX,
                        session_fifo: false,
                        same_session_main_agent_serialization: false,
                        per_job_max_active: usize::MAX,
                        max_queued_per_agent: usize::MAX,
                    },
                );
                if let Some(max_active) = class_value
                    .get("maxActive")
                    .and_then(Value::as_u64)
                    .and_then(|value| usize::try_from(value).ok())
                {
                    class_config.max_active = max_active;
                }
                if let Some(max_active) = class_value
                    .get("perAgentMaxActive")
                    .and_then(Value::as_u64)
                    .and_then(|value| usize::try_from(value).ok())
                {
                    class_config.per_agent_max_active = max_active;
                }
                if let Some(max_active) = class_value
                    .get("perChannelMaxActive")
                    .or_else(|| class_value.get("perAgentChannelMaxActive"))
                    .and_then(Value::as_u64)
                    .and_then(|value| usize::try_from(value).ok())
                {
                    class_config.per_channel_max_active = max_active;
                }
                if let Some(max_active) = class_value
                    .get("perSessionMaxActive")
                    .or_else(|| class_value.get("perSessionLaneMaxActive"))
                    .and_then(Value::as_u64)
                    .and_then(|value| usize::try_from(value).ok())
                {
                    class_config.per_session_max_active = max_active;
                }
                if let Some(session_fifo) = class_value.get("sessionFifo").and_then(Value::as_bool)
                {
                    class_config.session_fifo = session_fifo;
                }
                if let Some(enabled) = class_value
                    .get("sameSessionMainAgentSerialization")
                    .and_then(Value::as_bool)
                {
                    class_config.same_session_main_agent_serialization = enabled;
                }
                if let Some(max_active) = class_value
                    .get("perJobMaxActive")
                    .and_then(Value::as_u64)
                    .and_then(|value| usize::try_from(value).ok())
                {
                    class_config.per_job_max_active = max_active;
                }
                if let Some(max_queued) = class_value
                    .get("maxQueuedPerAgent")
                    .and_then(Value::as_u64)
                    .and_then(|value| usize::try_from(value).ok())
                {
                    class_config.max_queued_per_agent = max_queued;
                }
                config.classes.insert(runtime_class.clone(), class_config);
            }
        }
        break;
    }

    if config.interactive_reserve > config.global_concurrency_limit {
        config.warnings.push(format!(
            "runtimeDispatch.interactiveReserve ({}) exceeds globalConcurrencyLimit ({}); reserve is capped",
            config.interactive_reserve, config.global_concurrency_limit
        ));
        config.interactive_reserve = config.global_concurrency_limit;
    }
    Ok(config)
}

fn prompt_source_workspace(source_home: &Path, queued_source_workspace: &Path) -> PathBuf {
    let imported_workspace = source_home.join("workspace");
    if imported_workspace.is_dir() {
        imported_workspace
    } else {
        queued_source_workspace.to_path_buf()
    }
}

fn paths_equivalent(left: &Path, right: &Path) -> bool {
    if left == right {
        return true;
    }
    match (fs::canonicalize(left), fs::canonicalize(right)) {
        (Ok(left), Ok(right)) => left == right,
        _ => false,
    }
}

fn runtime_queue_leases_schema() -> String {
    RUNTIME_QUEUE_LEASES_SCHEMA.to_string()
}

fn runtime_queue_state_index_schema() -> String {
    RUNTIME_QUEUE_STATE_INDEX_SCHEMA.to_string()
}

fn default_interactive_runtime_class() -> String {
    "interactive".to_string()
}

fn default_channel_origin() -> String {
    "channel".to_string()
}

fn select_lock_runtime_class(
    requested_queue_id: Option<&str>,
    pending_items: &[PendingQueueItem],
    prepared_receipts: &HashMap<String, RuntimeExecutionReceipt>,
    terminal_run_ids: &HashSet<String>,
) -> String {
    if let Some(requested_queue_id) = requested_queue_id
        && let Some(item) = pending_items
            .iter()
            .find(|item| item.queue_id == requested_queue_id)
    {
        return item.runtime_class.clone();
    }
    if let Some(requested_queue_id) = requested_queue_id
        && let Some(prepared) = prepared_receipts.get(requested_queue_id)
        && let Some(runtime_class) = prepared.runtime_class.as_ref()
    {
        return runtime_class.clone();
    }

    let prepared_ids = prepared_receipts.keys().cloned().collect::<HashSet<_>>();
    let mut prepared_candidates = pending_items
        .iter()
        .filter(|item| {
            prepared_ids.contains(&item.queue_id) && !terminal_run_ids.contains(&item.queue_id)
        })
        .collect::<Vec<_>>();
    prepared_candidates.sort_by(|left, right| {
        runtime_selection_key(left)
            .cmp(&runtime_selection_key(right))
            .then_with(|| left.queue_id.cmp(&right.queue_id))
    });
    if let Some(item) = prepared_candidates.first() {
        return item.runtime_class.clone();
    }

    let mut queued_candidates = pending_items
        .iter()
        .filter(|item| {
            !prepared_ids.contains(&item.queue_id) && !terminal_run_ids.contains(&item.queue_id)
        })
        .collect::<Vec<_>>();
    queued_candidates.sort_by(|left, right| {
        runtime_selection_key(left)
            .cmp(&runtime_selection_key(right))
            .then_with(|| left.queue_id.cmp(&right.queue_id))
    });
    if let Some(item) = queued_candidates.first() {
        return item.runtime_class.clone();
    }
    "interactive".to_string()
}

fn runtime_classes_for_capacity(pending_items: &[PendingQueueItem]) -> Vec<String> {
    let mut classes = vec!["interactive".to_string(), "cron".to_string()];
    for item in pending_items {
        if !classes.contains(&item.runtime_class) {
            classes.push(item.runtime_class.clone());
        }
    }
    classes
}

fn runtime_class_state_dir(queue_dir: &Path, runtime_class: &str) -> PathBuf {
    queue_dir
        .join("classes")
        .join(normalize_key_part(runtime_class))
}

fn runtime_queue_leases_file(queue_dir: &Path, runtime_class: &str) -> PathBuf {
    if runtime_class == "legacy" {
        return queue_dir.join("runtime-leases.json");
    }
    runtime_class_state_dir(queue_dir, runtime_class).join("runtime-leases.json")
}

fn runtime_queue_lease_lock_file(queue_dir: &Path, runtime_class: &str) -> PathBuf {
    if runtime_class == "legacy" {
        return queue_dir.join("runtime-leases.lock");
    }
    runtime_class_state_dir(queue_dir, runtime_class).join("runtime-leases.lock")
}

fn acquire_runtime_queue_lease_lock(
    queue_dir: &Path,
    runtime_class: &str,
    now_ms: i64,
) -> io::Result<Option<RuntimeQueueLeaseLock>> {
    let lock_file = runtime_queue_lease_lock_file(queue_dir, runtime_class);
    match create_runtime_queue_lease_lock(&lock_file, now_ms) {
        Ok(lock) => return Ok(Some(lock)),
        Err(error) if runtime_queue_lease_lock_is_busy(&error) => {}
        Err(error) => return Err(error),
    }

    #[cfg(windows)]
    {
        Ok(None)
    }
    #[cfg(not(windows))]
    {
        if runtime_queue_lease_lock_is_stale(&lock_file, now_ms) {
            let _ = fs::remove_file(&lock_file);
            return match create_runtime_queue_lease_lock(&lock_file, now_ms) {
                Ok(lock) => Ok(Some(lock)),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(None),
                Err(error) => Err(error),
            };
        }

        Ok(None)
    }
}

fn acquire_runtime_queue_lease_lock_with_retry(
    queue_dir: &Path,
    runtime_class: &str,
    timeout: Duration,
) -> io::Result<Option<RuntimeQueueLeaseLock>> {
    let started = Instant::now();
    loop {
        let now_ms = current_log_time_ms()?;
        if let Some(lock) = acquire_runtime_queue_lease_lock(queue_dir, runtime_class, now_ms)? {
            return Ok(Some(lock));
        }
        if started.elapsed() >= timeout {
            return Ok(None);
        }
        thread::sleep(Duration::from_millis(
            RUNTIME_LEASE_RELEASE_LOCK_RETRY_SLEEP_MS,
        ));
    }
}

fn runtime_queue_lease_lock_is_busy(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::AlreadyExists | io::ErrorKind::PermissionDenied | io::ErrorKind::WouldBlock
    ) || {
        #[cfg(windows)]
        {
            // ERROR_SHARING_VIOLATION: another loop thread/process has the
            // exclusive Windows lock file open.
            error.raw_os_error() == Some(32)
        }
        #[cfg(not(windows))]
        {
            false
        }
    }
}

fn create_runtime_queue_lease_lock(
    lock_file: &Path,
    now_ms: i64,
) -> io::Result<RuntimeQueueLeaseLock> {
    if let Some(parent) = lock_file.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut options = OpenOptions::new();
    options.create(true).write(true).truncate(true);
    #[cfg(not(windows))]
    {
        options.create_new(true);
    }
    #[cfg(windows)]
    {
        options.share_mode(0);
    }
    let mut file = options.open(lock_file)?;
    writeln!(file, "{now_ms}")?;
    Ok(RuntimeQueueLeaseLock {
        path: lock_file.to_path_buf(),
        file: Some(file),
    })
}

#[cfg(not(windows))]
fn runtime_queue_lease_lock_is_stale(lock_file: &Path, now_ms: i64) -> bool {
    if let Ok(text) = fs::read_to_string(lock_file)
        && let Ok(created_at_ms) = text.trim().parse::<i64>()
    {
        return now_ms.saturating_sub(created_at_ms) > RUNTIME_LEASE_LOCK_STALE_MS;
    }
    lock_file
        .metadata()
        .and_then(|metadata| metadata.modified())
        .ok()
        .and_then(|modified| SystemTime::now().duration_since(modified).ok())
        .is_some_and(|age| age.as_millis() > u128::from(RUNTIME_LEASE_LOCK_STALE_MS as u64))
}

#[allow(clippy::too_many_arguments)]
fn runtime_session_identity_inventory_entry(
    queue_id: &str,
    source: RuntimeSessionIdentityInventorySource,
    runtime_class: &str,
    agent_id: &str,
    platform: &str,
    account_id: Option<&str>,
    channel_id: &str,
    user_id: &str,
    session_key: &str,
) -> RuntimeSessionIdentityInventoryEntry {
    let Some(account_id) = account_id.filter(|value| !value.trim().is_empty()) else {
        return RuntimeSessionIdentityInventoryEntry {
            queue_id: queue_id.to_string(),
            source,
            status: RuntimeSessionIdentityInventoryStatus::MissingAccountIdentity,
            canonical_lane_digest: None,
            reason: Some(
                "accountId is unavailable; exact-lane canonicalization is not safe".to_string(),
            ),
        };
    };
    match crate::channel_session_key::CanonicalChannelSessionKey::bind_for_lane(
        session_key,
        platform,
        account_id,
        channel_id,
        user_id,
        agent_id,
    ) {
        Ok(canonical) => {
            let canonical_session = canonical.canonical_string();
            let lane = runtime_session_lane_key(
                runtime_class,
                agent_id,
                platform,
                Some(account_id),
                channel_id,
                user_id,
                &canonical_session,
            );
            RuntimeSessionIdentityInventoryEntry {
                queue_id: queue_id.to_string(),
                source,
                status: if canonical_session == session_key {
                    RuntimeSessionIdentityInventoryStatus::Canonical
                } else {
                    RuntimeSessionIdentityInventoryStatus::NeedsNormalization
                },
                canonical_lane_digest: Some(format!(
                    "{:016x}",
                    runtime_queue_bytes_digest(lane.as_bytes())
                )),
                reason: (canonical_session != session_key).then(|| {
                    "legacy session key maps unambiguously to one exact canonical lane".to_string()
                }),
            }
        }
        Err(error) => RuntimeSessionIdentityInventoryEntry {
            queue_id: queue_id.to_string(),
            source,
            status: RuntimeSessionIdentityInventoryStatus::Invalid,
            canonical_lane_digest: None,
            reason: Some(error.to_string()),
        },
    }
}

pub fn inspect_runtime_session_identity(
    harness_home: impl AsRef<Path>,
) -> io::Result<RuntimeSessionIdentityInventoryReport> {
    let harness_home = harness_home.as_ref().to_path_buf();
    let queue_dir = harness_home.join("state").join("runtime-queue");
    let mut warnings = Vec::new();
    let pending = read_pending_items(&queue_dir.join("pending.jsonl"), &mut warnings)?;
    let mut entries = pending
        .iter()
        .map(|item| {
            runtime_session_identity_inventory_entry(
                &item.queue_id,
                RuntimeSessionIdentityInventorySource::Pending,
                &item.runtime_class,
                &item.agent_id,
                &item.platform,
                item.account_id.as_deref(),
                &item.channel_id,
                &item.user_id,
                &item.session_key,
            )
        })
        .collect::<Vec<_>>();
    let mut active_by_digest = BTreeMap::<String, Vec<String>>::new();
    for runtime_class in runtime_classes_for_release(&queue_dir) {
        let state = read_runtime_queue_leases(&queue_dir, &runtime_class, &mut warnings)?;
        for lease in state.leases.values() {
            let entry = runtime_session_identity_inventory_entry(
                &lease.queue_id,
                RuntimeSessionIdentityInventorySource::Lease,
                &lease.runtime_class,
                &lease.agent_id,
                &lease.platform,
                lease.account_id.as_deref(),
                &lease.channel_id,
                lease.user_id.as_deref().unwrap_or(""),
                &lease.session_key,
            );
            if let Some(digest) = entry.canonical_lane_digest.as_ref() {
                active_by_digest
                    .entry(digest.clone())
                    .or_default()
                    .push(lease.queue_id.clone());
            }
            entries.push(entry);
        }
    }
    entries.sort_by(|left, right| {
        left.queue_id
            .cmp(&right.queue_id)
            .then_with(|| left.source.cmp(&right.source))
    });
    let collision_groups = active_by_digest
        .into_iter()
        .filter_map(|(canonical_lane_digest, mut queue_ids)| {
            if queue_ids.len() < 2 {
                return None;
            }
            queue_ids.sort();
            Some(RuntimeSessionIdentityCollisionGroup {
                canonical_lane_digest,
                queue_ids,
            })
        })
        .collect();
    Ok(RuntimeSessionIdentityInventoryReport {
        schema: "agent-harness.runtime-session-identity-inventory.v1",
        harness_home,
        inspected_at_ms: current_log_time_ms()?,
        entries,
        collision_groups,
        warnings,
    })
}

fn read_runtime_queue_leases(
    queue_dir: &Path,
    runtime_class: &str,
    warnings: &mut Vec<String>,
) -> io::Result<RuntimeQueueLeaseState> {
    let leases_file = runtime_queue_leases_file(queue_dir, runtime_class);
    if !leases_file.is_file() {
        return Ok(RuntimeQueueLeaseState {
            schema: runtime_queue_leases_schema(),
            leases: BTreeMap::new(),
        });
    }
    let text = fs::read_to_string(&leases_file)?;
    match serde_json::from_str::<RuntimeQueueLeaseState>(&text) {
        Ok(mut state) => {
            if state.schema.trim().is_empty() {
                state.schema = runtime_queue_leases_schema();
            }
            Ok(state)
        }
        Err(error) => {
            warnings.push(format!(
                "runtime queue leases file {} is not valid JSON: {error}; starting with empty lease state",
                leases_file.display()
            ));
            Ok(RuntimeQueueLeaseState {
                schema: runtime_queue_leases_schema(),
                leases: BTreeMap::new(),
            })
        }
    }
}

fn write_runtime_queue_leases(
    queue_dir: &Path,
    runtime_class: &str,
    state: &RuntimeQueueLeaseState,
) -> io::Result<()> {
    write_json_atomic(&runtime_queue_leases_file(queue_dir, runtime_class), state)
}

fn purge_runtime_queue_leases(
    state: &mut RuntimeQueueLeaseState,
    now_ms: i64,
    terminal_run_ids: &HashSet<String>,
    retry_pending_run_ids: &HashSet<String>,
    receipts_file: Option<&Path>,
    warnings: &mut Vec<String>,
) -> io::Result<()> {
    let mut remove_silently = Vec::new();
    let mut reap_dead_owner = Vec::new();
    for (queue_id, lease) in &state.leases {
        if terminal_run_ids.contains(queue_id)
            || retry_pending_run_ids.contains(queue_id)
            || lease.lease_expires_at_ms <= now_ms
        {
            remove_silently.push(queue_id.clone());
            continue;
        }
        if let Some(process_id) = dead_runtime_queue_lease_owner(&lease.owner) {
            reap_dead_owner.push((queue_id.clone(), process_id));
        }
    }
    for queue_id in remove_silently {
        state.leases.remove(&queue_id);
    }
    for (queue_id, process_id) in reap_dead_owner {
        let Some(lease) = state.leases.remove(&queue_id) else {
            continue;
        };
        let owner = lease.owner.clone();
        let reason = format!(
            "runtime queue lease owner {} references a non-running processId={process_id}",
            owner.display_label()
        );
        warnings.push(format!(
            "runtime queue lease `{queue_id}` reaped because {reason}"
        ));
        if let Some(receipts_file) = receipts_file {
            append_json_line(
                receipts_file,
                &serde_json::json!({
                    "queueId": queue_id,
                    "status": RuntimeExecutionReceiptStatus::StaleOwnerReaped,
                    "runtimeClass": lease.runtime_class,
                    "origin": lease.origin,
                    "cronRunId": lease.cron_run_id,
                    "owner": owner,
                    "atMs": now_ms,
                    "reason": reason
                }),
            )?;
        }
    }
    reconcile_canonical_lease_collisions(state, now_ms, receipts_file, warnings)?;
    Ok(())
}

fn reconcile_canonical_lease_collisions(
    state: &mut RuntimeQueueLeaseState,
    now_ms: i64,
    receipts_file: Option<&Path>,
    warnings: &mut Vec<String>,
) -> io::Result<()> {
    let mut groups = BTreeMap::<String, Vec<String>>::new();
    let mut invalid = Vec::<(String, String)>::new();
    for (queue_id, lease) in &state.leases {
        let lane = if let Some(account_id) = lease.account_id.as_deref() {
            match crate::channel_session_key::CanonicalChannelSessionKey::bind_for_lane(
                &lease.session_key,
                &lease.platform,
                account_id,
                &lease.channel_id,
                lease.user_id.as_deref().unwrap_or(""),
                &lease.agent_id,
            ) {
                Ok(canonical) => runtime_session_lane_key(
                    &lease.runtime_class,
                    &lease.agent_id,
                    &lease.platform,
                    Some(account_id),
                    &lease.channel_id,
                    lease.user_id.as_deref().unwrap_or(""),
                    &canonical.canonical_string(),
                ),
                Err(error) => {
                    invalid.push((queue_id.clone(), error.to_string()));
                    continue;
                }
            }
        } else {
            runtime_session_lane_key(
                &lease.runtime_class,
                &lease.agent_id,
                &lease.platform,
                None,
                &lease.channel_id,
                lease.user_id.as_deref().unwrap_or(""),
                &lease.session_key,
            )
        };
        groups.entry(lane).or_default().push(queue_id.clone());
    }

    for (queue_id, error) in invalid {
        let Some(lease) = state.leases.remove(&queue_id) else {
            continue;
        };
        let reason = format!(
            "runtime lease removed from active ownership because its exact-lane session key failed canonicalization: {error}"
        );
        warnings.push(format!(
            "runtime queue lease `{queue_id}` quarantined: {reason}"
        ));
        if let Some(receipts_file) = receipts_file {
            append_json_line(
                receipts_file,
                &serde_json::json!({
                    "queueId": queue_id,
                    "status": RuntimeExecutionReceiptStatus::InvalidCanonicalLaneQuarantined,
                    "runtimeClass": lease.runtime_class,
                    "origin": lease.origin,
                    "atMs": now_ms,
                    "reason": reason,
                }),
            )?;
        }
    }

    for (lane, mut queue_ids) in groups {
        if queue_ids.len() < 2 {
            continue;
        }
        queue_ids.sort_by(|left, right| {
            state.leases[left]
                .started_at_ms
                .cmp(&state.leases[right].started_at_ms)
                .then_with(|| left.cmp(right))
        });
        let retained_queue_id = queue_ids.remove(0);
        let lane_digest = format!("{:016x}", runtime_queue_bytes_digest(lane.as_bytes()));
        for queue_id in queue_ids {
            let Some(lease) = state.leases.remove(&queue_id) else {
                continue;
            };
            let reason = format!(
                "duplicate canonical active lease reconciled into retained queue `{retained_queue_id}`; canonicalLaneDigest={lane_digest}"
            );
            warnings.push(format!(
                "runtime queue lease `{queue_id}` reconciled: {reason}"
            ));
            if let Some(receipts_file) = receipts_file {
                append_json_line(
                    receipts_file,
                    &serde_json::json!({
                        "queueId": queue_id,
                        "status": RuntimeExecutionReceiptStatus::CanonicalCollisionReconciled,
                        "runtimeClass": lease.runtime_class,
                        "origin": lease.origin,
                        "retainedQueueId": retained_queue_id.clone(),
                        "canonicalLaneDigest": lane_digest.clone(),
                        "atMs": now_ms,
                        "reason": reason,
                    }),
                )?;
            }
        }
    }
    Ok(())
}

pub fn release_runtime_queue_lease(
    harness_home: impl AsRef<Path>,
    queue_id: &str,
) -> io::Result<()> {
    let queue_dir = harness_home.as_ref().join("state").join("runtime-queue");
    fs::create_dir_all(&queue_dir)?;
    let mut last_busy = None;
    for runtime_class in runtime_classes_for_release(&queue_dir) {
        let Some(_lease_lock) = acquire_runtime_queue_lease_lock_with_retry(
            &queue_dir,
            &runtime_class,
            Duration::from_millis(RUNTIME_LEASE_RELEASE_LOCK_RETRY_MS),
        )?
        else {
            last_busy = Some(runtime_class);
            continue;
        };
        let mut warnings = Vec::new();
        let mut state = read_runtime_queue_leases(&queue_dir, &runtime_class, &mut warnings)?;
        if state.leases.remove(queue_id).is_some() {
            write_runtime_queue_leases(&queue_dir, &runtime_class, &state)?;
            return Ok(());
        }
    }
    if let Some(runtime_class) = last_busy {
        return Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            format!(
                "runtime queue lease lock stayed busy for class `{runtime_class}` while releasing queue lease `{queue_id}`"
            ),
        ));
    }
    Ok(())
}

/// Observes one exact runtime queue lease under every known runtime-class
/// lease lock. This API never converts malformed, unreadable, or lock-busy
/// state into `Released`; all such cases fail closed as `Unknown`.
pub fn observe_runtime_queue_lease(
    options: RuntimeQueueLeaseObservationOptions,
) -> RuntimeQueueLeaseObservationReceipt {
    let unknown =
        |runtime_class: Option<String>, reason: String| RuntimeQueueLeaseObservationReceipt {
            schema: RUNTIME_QUEUE_LEASE_OBSERVATION_SCHEMA,
            queue_id: options.queue_id.clone(),
            status: RuntimeQueueLeaseObservationStatus::Unknown,
            observed_at_ms: options.observed_at_ms,
            runtime_class,
            reason,
        };
    if options.queue_id.trim().is_empty() || options.queue_id.chars().any(char::is_control) {
        return unknown(
            None,
            "queueId is empty or contains a control character".to_string(),
        );
    }
    if options.observed_at_ms < 0 {
        return unknown(None, "observedAtMs cannot be negative".to_string());
    }

    let queue_dir = options.harness_home.join("state").join("runtime-queue");
    let runtime_classes = match runtime_classes_for_observation(&queue_dir) {
        Ok(classes) => classes,
        Err(error) => {
            return unknown(
                None,
                format!("runtime lease class discovery failed: {error}"),
            );
        }
    };
    let mut matches = Vec::new();
    for runtime_class in runtime_classes {
        let lease_lock = match acquire_runtime_queue_lease_lock(
            &queue_dir,
            &runtime_class,
            options.observed_at_ms,
        ) {
            Ok(Some(lock)) => lock,
            Ok(None) => {
                return unknown(
                    Some(runtime_class.clone()),
                    format!(
                        "runtime queue lease lock is busy for class `{runtime_class}`; lease state was not observed"
                    ),
                );
            }
            Err(error) => {
                return unknown(
                    Some(runtime_class.clone()),
                    format!("runtime queue lease lock failed for class `{runtime_class}`: {error}"),
                );
            }
        };
        let state = match read_runtime_queue_leases_strict(&queue_dir, &runtime_class) {
            Ok(state) => state,
            Err(error) => {
                drop(lease_lock);
                return unknown(
                    Some(runtime_class.clone()),
                    format!(
                        "runtime queue lease state for class `{runtime_class}` is unknown: {error}"
                    ),
                );
            }
        };
        if let Some(lease) = state.leases.get(&options.queue_id) {
            matches.push((runtime_class.clone(), lease.clone()));
        }
        drop(lease_lock);
    }

    if matches.len() > 1 {
        let classes = matches
            .iter()
            .map(|(runtime_class, _)| runtime_class.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        return unknown(
            None,
            format!(
                "queueId `{}` has duplicate runtime leases across classes: {classes}",
                options.queue_id
            ),
        );
    }
    let Some((runtime_class, lease)) = matches.pop() else {
        return RuntimeQueueLeaseObservationReceipt {
            schema: RUNTIME_QUEUE_LEASE_OBSERVATION_SCHEMA,
            queue_id: options.queue_id,
            status: RuntimeQueueLeaseObservationStatus::Released,
            observed_at_ms: options.observed_at_ms,
            runtime_class: None,
            reason: "no matching runtime queue lease exists in any known class".to_string(),
        };
    };
    if lease.lease_expires_at_ms <= options.observed_at_ms {
        return RuntimeQueueLeaseObservationReceipt {
            schema: RUNTIME_QUEUE_LEASE_OBSERVATION_SCHEMA,
            queue_id: options.queue_id,
            status: RuntimeQueueLeaseObservationStatus::Expired,
            observed_at_ms: options.observed_at_ms,
            runtime_class: Some(runtime_class),
            reason: format!(
                "runtime queue lease expired at {}",
                lease.lease_expires_at_ms
            ),
        };
    }
    if let Some(process_id) = dead_runtime_queue_lease_owner(&lease.owner) {
        return RuntimeQueueLeaseObservationReceipt {
            schema: RUNTIME_QUEUE_LEASE_OBSERVATION_SCHEMA,
            queue_id: options.queue_id,
            status: RuntimeQueueLeaseObservationStatus::DeadOwner,
            observed_at_ms: options.observed_at_ms,
            runtime_class: Some(runtime_class),
            reason: format!(
                "runtime queue lease owner {} references non-running processId={process_id}",
                lease.owner.display_label()
            ),
        };
    }
    RuntimeQueueLeaseObservationReceipt {
        schema: RUNTIME_QUEUE_LEASE_OBSERVATION_SCHEMA,
        queue_id: options.queue_id,
        status: RuntimeQueueLeaseObservationStatus::Active,
        observed_at_ms: options.observed_at_ms,
        runtime_class: Some(runtime_class),
        reason: format!(
            "runtime queue lease is active until {} for owner {}",
            lease.lease_expires_at_ms,
            lease.owner.display_label()
        ),
    }
}

/// Observes runtime leases for one exact coordinator owner lane. Any unreadable,
/// malformed, or lock-busy lease state is reported as active so a coordinator
/// resume cannot interrupt a turn whose absence was not actually observed.
pub fn observe_runtime_queue_lane_activity(
    options: RuntimeQueueLaneActivityObservationOptions,
) -> crate::worker_resume::LaneActivityReceiptV1 {
    let active = |reason: String| {
        crate::worker_resume::LaneActivityReceiptV1::active(options.observed_at_ms.max(0), reason)
            .expect("bounded runtime lane activity reason must be valid")
    };
    if options.observed_at_ms < 0 {
        return active("runtime lane activity observation timestamp is invalid".to_string());
    }
    if let Err(error) = options.owner.validate() {
        return active(format!("exact coordinator owner is invalid: {error}"));
    }

    let queue_dir = options.harness_home.join("state").join("runtime-queue");
    let runtime_classes = match runtime_classes_for_observation(&queue_dir) {
        Ok(classes) => classes,
        Err(error) => {
            return active(format!("runtime lease class discovery failed: {error}"));
        }
    };
    for runtime_class in runtime_classes {
        let lease_lock = match acquire_runtime_queue_lease_lock(
            &queue_dir,
            &runtime_class,
            options.observed_at_ms,
        ) {
            Ok(Some(lock)) => lock,
            Ok(None) => {
                return active(format!(
                    "runtime queue lease lock is busy for class `{runtime_class}`; exact lane activity was not observed"
                ));
            }
            Err(error) => {
                return active(format!(
                    "runtime queue lease lock failed for class `{runtime_class}`: {error}"
                ));
            }
        };
        let state = match read_runtime_queue_leases_strict(&queue_dir, &runtime_class) {
            Ok(state) => state,
            Err(error) => {
                drop(lease_lock);
                return active(format!(
                    "runtime queue lease state for class `{runtime_class}` is unknown: {error}"
                ));
            }
        };
        if let Some(lease) = state
            .leases
            .values()
            .find(|lease| runtime_queue_lease_matches_exact_owner(lease, &options.owner))
        {
            let reason = if lease.lease_expires_at_ms <= options.observed_at_ms {
                format!(
                    "exact lane runtime queue lease `{}` expired at {} but is not reconciled",
                    lease.queue_id, lease.lease_expires_at_ms
                )
            } else if let Some(process_id) = dead_runtime_queue_lease_owner(&lease.owner) {
                format!(
                    "exact lane runtime queue lease `{}` references non-running processId={process_id} but is not reconciled",
                    lease.queue_id
                )
            } else {
                format!(
                    "exact lane runtime queue lease `{}` is active until {}",
                    lease.queue_id, lease.lease_expires_at_ms
                )
            };
            drop(lease_lock);
            return active(reason);
        }
        drop(lease_lock);
    }

    crate::worker_resume::LaneActivityReceiptV1::idle(options.observed_at_ms)
        .expect("non-negative runtime lane activity timestamp must be valid")
}

fn runtime_queue_lease_matches_exact_owner(
    lease: &RuntimeQueueLease,
    owner: &crate::worker_result_mailbox::ExactWorkerResultOwnerV1,
) -> bool {
    let lane = &owner.lane;
    lease.platform == lane.platform()
        && lease.account_id.as_deref() == Some(lane.account_id())
        && lease.channel_id == lane.channel_id()
        && lease.user_id.as_deref() == Some(lane.user_id())
        && lease.agent_id == lane.agent_id()
        && lease.runtime_class == lane.runtime_class()
        && root_working_session_key(&lease.session_key) == lane.root_virtual_session()
        && lease.session_key == lane.concrete_session()
        && runtime_queue_lease_virtual_session_id(lease).as_deref()
            == Some(owner.virtual_session_id.as_str())
}

fn runtime_queue_lease_virtual_session_id(lease: &RuntimeQueueLease) -> Option<String> {
    lease.virtual_session_id.clone().or_else(|| {
        let user_id = lease.user_id.as_deref()?;
        let root_session = root_working_session_key(&lease.session_key);
        Some(derive_virtual_session_id(
            &lease.platform,
            &lease.channel_id,
            user_id,
            &lease.agent_id,
            &root_session,
        ))
    })
}

pub fn reconcile_runtime_queue_leases_for_generation(
    harness_home: impl AsRef<Path>,
    service_id: &str,
    generation_id: &str,
    at_ms: i64,
) -> io::Result<RuntimeQueueLeaseReconciliationReport> {
    let harness_home = harness_home.as_ref().to_path_buf();
    let queue_dir = harness_home.join("state").join("runtime-queue");
    fs::create_dir_all(&queue_dir)?;
    let receipts_file = queue_dir.join("execution-receipts.jsonl");
    let mut report = RuntimeQueueLeaseReconciliationReport {
        schema: RUNTIME_QUEUE_LEASE_RECONCILIATION_SCHEMA,
        harness_home: harness_home.clone(),
        service_id: service_id.to_string(),
        generation_id: generation_id.to_string(),
        reaped_leases: Vec::new(),
        warnings: Vec::new(),
    };
    let pending_file = queue_dir.join("pending.jsonl");
    let pending_items = read_pending_items(&pending_file, &mut report.warnings)?;
    let pending_by_id = pending_items
        .iter()
        .map(|item| (item.queue_id.clone(), item.clone()))
        .collect::<HashMap<_, _>>();

    for runtime_class in runtime_classes_for_release(&queue_dir) {
        let Some(_lease_lock) = acquire_runtime_queue_lease_lock_with_retry(
            &queue_dir,
            &runtime_class,
            Duration::from_millis(RUNTIME_LEASE_RELEASE_LOCK_RETRY_MS),
        )?
        else {
            report.warnings.push(format!(
                "runtime queue lease lock stayed busy for class `{runtime_class}` while reconciling serviceId={service_id} generationId={generation_id}"
            ));
            continue;
        };
        let mut warnings = Vec::new();
        let mut state = read_runtime_queue_leases(&queue_dir, &runtime_class, &mut warnings)?;
        report.warnings.extend(warnings);
        let matching_queue_ids = state
            .leases
            .iter()
            .filter(|(_, lease)| lease.owner.matches_generation(service_id, generation_id))
            .map(|(queue_id, _)| queue_id.clone())
            .collect::<Vec<_>>();
        if matching_queue_ids.is_empty() {
            continue;
        }

        for queue_id in matching_queue_ids {
            let Some(lease) = state.leases.remove(&queue_id) else {
                continue;
            };
            let owner = lease.owner.as_json_value();
            let runtime_class = lease.runtime_class.clone();
            let origin = lease.origin.clone();
            let cron_run_id = lease.cron_run_id.clone();
            let reason = format!(
                "runtime queue lease owner {} belonged to exited supervisor serviceId={service_id} generationId={generation_id}",
                lease.owner.display_label()
            );
            if let Some(pending) = pending_by_id.get(&queue_id)
                && let QueueTerminalControl::Terminal(control) = resolve_queue_terminal_control(
                    &harness_home,
                    &queue_id,
                    Some(&pending.session_key),
                )?
            {
                record_terminal_control_suppression(
                    &harness_home,
                    &queue_id,
                    Some(&pending.runtime_class),
                    Some(&pending.origin),
                    pending.cron_run_id.as_deref(),
                    pending.scheduled_for_ms,
                    &pending.continuation,
                    &control,
                )?;
                report.warnings.push(format!(
                    "reaped runtime queue lease `{queue_id}` remains suppressed by terminal control {}",
                    control.source.as_str()
                ));
            }
            append_json_line(
                &receipts_file,
                &serde_json::json!({
                    "queueId": queue_id,
                    "status": RuntimeExecutionReceiptStatus::StaleOwnerReaped,
                    "runtimeClass": runtime_class,
                    "origin": origin,
                    "cronRunId": cron_run_id,
                    "owner": owner,
                    "serviceId": service_id,
                    "generationId": generation_id,
                    "atMs": at_ms,
                    "reason": reason
                }),
            )?;
            report.reaped_leases.push(RuntimeQueueLeaseReconciledItem {
                queue_id,
                runtime_class,
                origin,
                cron_run_id,
                owner,
                at_ms,
                reason,
            });
        }
        write_runtime_queue_leases(&queue_dir, &runtime_class, &state)?;
    }

    Ok(report)
}

fn runtime_classes_for_release(queue_dir: &Path) -> Vec<String> {
    let mut classes = vec![
        "interactive".to_string(),
        "cron".to_string(),
        "worker".to_string(),
        "maintenance".to_string(),
    ];
    if queue_dir.join("runtime-leases.json").is_file() {
        classes.push("legacy".to_string());
    }
    let classes_dir = queue_dir.join("classes");
    if let Ok(entries) = fs::read_dir(classes_dir) {
        for entry in entries.flatten() {
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if !file_type.is_dir() {
                continue;
            }
            if let Some(name) = entry.file_name().to_str()
                && !classes.iter().any(|class| class == name)
            {
                classes.push(name.to_string());
            }
        }
    }
    classes
}

fn runtime_classes_for_observation(queue_dir: &Path) -> io::Result<Vec<String>> {
    let mut classes = vec![
        "interactive".to_string(),
        "cron".to_string(),
        "worker".to_string(),
        "maintenance".to_string(),
    ];
    let legacy_file = runtime_queue_leases_file(queue_dir, "legacy");
    match fs::metadata(&legacy_file) {
        Ok(metadata) if metadata.is_file() => classes.push("legacy".to_string()),
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "legacy runtime lease path is not a file: {}",
                    legacy_file.display()
                ),
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }

    let classes_dir = queue_dir.join("classes");
    let entries = match fs::read_dir(&classes_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(classes),
        Err(error) => return Err(error),
    };
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_str().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "runtime class directory name is not valid UTF-8 under {}",
                    classes_dir.display()
                ),
            )
        })?;
        if !classes.iter().any(|runtime_class| runtime_class == name) {
            classes.push(name.to_string());
        }
    }
    Ok(classes)
}

fn read_runtime_queue_leases_strict(
    queue_dir: &Path,
    runtime_class: &str,
) -> io::Result<RuntimeQueueLeaseState> {
    let leases_file = runtime_queue_leases_file(queue_dir, runtime_class);
    match fs::metadata(&leases_file) {
        Ok(metadata) if metadata.is_file() => {}
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "runtime lease path is not a file: {}",
                    leases_file.display()
                ),
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(RuntimeQueueLeaseState {
                schema: runtime_queue_leases_schema(),
                leases: BTreeMap::new(),
            });
        }
        Err(error) => return Err(error),
    }
    let text = fs::read_to_string(&leases_file)?;
    let state = serde_json::from_str::<RuntimeQueueLeaseState>(&text).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "runtime lease state {} is not valid JSON: {error}",
                leases_file.display()
            ),
        )
    })?;
    if state.schema != runtime_queue_leases_schema() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "runtime lease state {} has unsupported schema `{}`",
                leases_file.display(),
                state.schema
            ),
        ));
    }
    for (queue_id, lease) in &state.leases {
        if queue_id != &lease.queue_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "runtime lease state {} maps queueId `{queue_id}` to payload queueId `{}`",
                    leases_file.display(),
                    lease.queue_id
                ),
            ));
        }
    }
    Ok(state)
}

pub fn resolve_queue_terminal_control(
    harness_home: impl AsRef<Path>,
    queue_id: &str,
    session_key: Option<&str>,
) -> io::Result<QueueTerminalControl> {
    let queue_dir = harness_home.as_ref().join("state").join("runtime-queue");
    let mut warnings = Vec::new();
    let index = refresh_runtime_queue_state_index(&queue_dir, &mut warnings)?;
    let control = resolve_queue_terminal_control_from_index(
        &queue_dir,
        queue_id,
        session_key,
        index.queues.get(queue_id),
    )?;
    if !matches!(&control, QueueTerminalControl::Runnable) {
        return Ok(control);
    }
    let requested_queue_ids = std::iter::once(queue_id.to_string()).collect::<BTreeSet<_>>();
    let cold_records = find_runtime_queue_terminal_history(&queue_dir, &requested_queue_ids)?;
    let Some(record) = cold_records
        .into_iter()
        .find(|record| record.queue_id == queue_id)
    else {
        return Ok(QueueTerminalControl::Runnable);
    };
    let reason = record
        .reason
        .unwrap_or_else(|| format!("historical terminal run-once status `{}`", record.status));
    let terminal_disposition = record.terminal_disposition.as_deref().and_then(|value| {
        serde_json::from_value::<crate::RuntimeTerminalDispositionV1>(Value::String(
            value.to_string(),
        ))
        .ok()
    });
    let continuation_link =
        record
            .child_queue_id
            .map(|child_queue_id| crate::RuntimeContinuationLinkV1 {
                parent_queue_id: record.queue_id.clone(),
                child_queue_id,
                continuation_index: record.continuation_index.unwrap_or(0),
                virtual_lane_digest: None,
            });
    Ok(QueueTerminalControl::Terminal(QueueTerminalControlMatch {
        source: TerminalControlSource::RunOnceTerminal,
        reason,
        suppression_recorded: index
            .queues
            .get(queue_id)
            .is_some_and(|entry| entry.suppression_recorded),
        terminal_status: Some(record.status),
        terminal_disposition,
        continuation_link,
    }))
}

fn cold_terminal_history_reasons(
    queue_dir: &Path,
    queue_ids: &BTreeSet<String>,
) -> io::Result<BTreeMap<String, String>> {
    if queue_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    Ok(find_runtime_queue_terminal_history(queue_dir, queue_ids)?
        .into_iter()
        .filter(|record| queue_ids.contains(&record.queue_id))
        .map(|record| {
            let reason = record.reason.unwrap_or_else(|| {
                format!("historical terminal run-once status `{}`", record.status)
            });
            (record.queue_id, reason)
        })
        .collect())
}

pub(crate) fn resolve_queue_terminal_control_from_index(
    queue_dir: &Path,
    queue_id: &str,
    session_key: Option<&str>,
    indexed: Option<&RuntimeQueueStateIndexEntry>,
) -> io::Result<QueueTerminalControl> {
    let suppression_recorded = indexed
        .map(|entry| entry.suppression_recorded)
        .unwrap_or(false);

    if let Some(reason) = quarantine_marker_reason(&queue_dir, queue_id)? {
        return Ok(QueueTerminalControl::Terminal(QueueTerminalControlMatch {
            source: TerminalControlSource::Quarantine,
            reason,
            suppression_recorded,
            terminal_status: None,
            terminal_disposition: None,
            continuation_link: None,
        }));
    }
    if let Some(reason) = scoped_stop_marker_reason(&queue_dir, queue_id, session_key)? {
        return Ok(QueueTerminalControl::Terminal(QueueTerminalControlMatch {
            source: TerminalControlSource::ScopedStop,
            reason,
            suppression_recorded,
            terminal_status: Some("canceled".to_string()),
            terminal_disposition: Some(crate::RuntimeTerminalDispositionV1::LogicalCanceled),
            continuation_link: None,
        }));
    }
    if let Some(reason) = queue_skip_control_reason(&queue_dir, queue_id)? {
        return Ok(QueueTerminalControl::Terminal(QueueTerminalControlMatch {
            source: TerminalControlSource::QueueSkip,
            reason,
            suppression_recorded,
            terminal_status: Some("skipped".to_string()),
            terminal_disposition: Some(crate::RuntimeTerminalDispositionV1::TerminalSuppression),
            continuation_link: None,
        }));
    }
    if let Some(entry) = indexed
        && entry.terminal_ever
    {
        let status = entry.terminal_status.as_deref().unwrap_or("terminal");
        let reason = entry
            .terminal_reason
            .clone()
            .unwrap_or_else(|| format!("historical terminal run-once status `{status}`"));
        return Ok(QueueTerminalControl::Terminal(QueueTerminalControlMatch {
            source: TerminalControlSource::RunOnceTerminal,
            reason,
            suppression_recorded,
            terminal_status: entry.terminal_status.clone(),
            terminal_disposition: entry.terminal_disposition,
            continuation_link: entry.continuation_link.clone(),
        }));
    }
    Ok(QueueTerminalControl::Runnable)
}

pub fn write_runtime_queue_quarantine_marker(
    harness_home: impl AsRef<Path>,
    queue_id: &str,
    reason: &str,
    now_ms: i64,
) -> io::Result<PathBuf> {
    let quarantine_dir = harness_home
        .as_ref()
        .join("state")
        .join("runtime-queue")
        .join("quarantine");
    fs::create_dir_all(&quarantine_dir)?;
    let marker_file = quarantine_dir.join(format!("{}.json", normalize_key_part(queue_id)));
    let marker = serde_json::json!({
        "schema": RUNTIME_QUEUE_QUARANTINE_SCHEMA,
        "queueId": queue_id,
        "status": "quarantined",
        "reason": reason,
        "atMs": now_ms
    });
    write_json_atomic(&marker_file, &marker)?;
    Ok(marker_file)
}

pub(crate) fn record_terminal_control_suppression(
    harness_home: &Path,
    queue_id: &str,
    runtime_class: Option<&str>,
    origin: Option<&str>,
    cron_run_id: Option<&str>,
    scheduled_for_ms: Option<i64>,
    continuation: &RuntimeContinuationMetadata,
    control: &QueueTerminalControlMatch,
) -> io::Result<bool> {
    let queue_dir = harness_home.join("state").join("runtime-queue");
    fs::create_dir_all(&queue_dir)?;
    let mut warnings = Vec::new();
    let mut index = refresh_runtime_queue_state_index(&queue_dir, &mut warnings)?;
    let entry = index.queues.entry(queue_id.to_string()).or_default();
    if entry.suppression_recorded {
        return Ok(false);
    }
    let receipts_file = queue_dir.join("run-once-receipts.jsonl");
    let receipt = serde_json::json!({
        "schema": "agent-harness.runtime-run-once.v1",
        "queueId": queue_id,
        "status": "suppressed",
        "runtimeClass": runtime_class,
        "origin": origin,
        "cronRunId": cron_run_id,
        "scheduledForMs": scheduled_for_ms,
        "continuation": continuation,
        "terminalControlMatched": true,
        "terminalControlSource": control.source.as_str(),
        "suppressedRunOnceReason": TERMINAL_CONTROL_SUPPRESSION_REASON,
        "reason": format!(
            "runtime queue item suppressed because terminal control is present: {}: {}",
            control.source.as_str(),
            control.reason
        )
    });
    append_json_line(&receipts_file, &receipt)?;
    entry.suppression_recorded = true;
    entry.terminal_run_once_ever = true;
    entry.latest_status = Some("suppressed".to_string());
    entry.terminal_control_source = Some(control.source.as_str().to_string());
    write_runtime_queue_state_index(&queue_dir, &index)?;
    Ok(true)
}

fn terminal_control_no_pending_receipt(
    queue_id: &str,
    runtime_class: Option<String>,
    origin: Option<String>,
    cron_run_id: Option<String>,
    scheduled_for_ms: Option<i64>,
    continuation: RuntimeContinuationMetadata,
    control: &QueueTerminalControlMatch,
) -> RuntimeExecutionReceipt {
    RuntimeExecutionReceipt {
        queue_id: Some(queue_id.to_string()),
        status: RuntimeExecutionReceiptStatus::NoPendingItem,
        channel_lane: None,
        runtime_class,
        origin,
        cron_run_id,
        scheduled_for_ms,
        execution_dir: None,
        prompt_bundle_json: None,
        prompt_markdown: None,
        runtime_workspace: None,
        inbound_media_artifacts: Vec::new(),
        continuation,
        terminal_control_matched: Some(true),
        terminal_control_source: Some(control.source.as_str().to_string()),
        suppressed_run_once_reason: Some(TERMINAL_CONTROL_SUPPRESSION_REASON.to_string()),
        reason: format!(
            "runtime queue item matched terminal control {}: {}",
            control.source.as_str(),
            control.reason
        ),
    }
}

fn pending_terminal_control_ids(
    queue_dir: &Path,
    pending_items: &[PendingQueueItem],
    index: &RuntimeQueueStateIndex,
    warnings: &mut Vec<String>,
) -> io::Result<HashSet<String>> {
    let mut ids = HashSet::new();
    let pending_queue_ids = pending_items
        .iter()
        .map(|item| item.queue_id.clone())
        .collect::<BTreeSet<_>>();
    let cold_reasons = cold_terminal_history_reasons(queue_dir, &pending_queue_ids)?;
    for item in pending_items {
        let control = resolve_queue_terminal_control_from_index(
            queue_dir,
            &item.queue_id,
            Some(&item.session_key),
            index.queues.get(&item.queue_id),
        )?;
        let control = match control {
            QueueTerminalControl::Runnable => cold_reasons.get(&item.queue_id).map(|reason| {
                QueueTerminalControl::Terminal(QueueTerminalControlMatch {
                    source: TerminalControlSource::RunOnceTerminal,
                    reason: reason.clone(),
                    suppression_recorded: index
                        .queues
                        .get(&item.queue_id)
                        .is_some_and(|entry| entry.suppression_recorded),
                    terminal_status: None,
                    terminal_disposition: None,
                    continuation_link: None,
                })
            }),
            QueueTerminalControl::Terminal(control) => {
                Some(QueueTerminalControl::Terminal(control))
            }
        };
        match control {
            None => {}
            Some(QueueTerminalControl::Runnable) => {}
            Some(QueueTerminalControl::Terminal(control)) => {
                warnings.push(format!(
                    "runtime queue item `{}` has terminal control {}; excluding from selection",
                    item.queue_id,
                    control.source.as_str()
                ));
                ids.insert(item.queue_id.clone());
            }
        }
    }
    Ok(ids)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RuntimeQueueStateIndexRefreshMode {
    Blocking,
    NonBlocking,
}

pub(crate) fn rebuild_runtime_queue_state_index(
    queue_dir: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<RuntimeQueueStateIndex> {
    let receipts_file = queue_dir.join("run-once-receipts.jsonl");
    with_jsonl_append_lock(&receipts_file, || {
        // Recovery must be rechecked after ownership is acquired. A failed
        // compactor can publish its marker while this caller is waiting for the
        // append lock; rebuilding a ledger before that recovery would accept an
        // incomplete replacement as authoritative state.
        recover_runtime_queue_ledger_compaction_if_needed_locked(&receipts_file, warnings)?;
        rebuild_runtime_queue_state_index_locked(queue_dir, warnings)
    })
}

fn rebuild_runtime_queue_state_index_for_refresh(
    mode: RuntimeQueueStateIndexRefreshMode,
    queue_dir: &Path,
    cached_index: Option<RuntimeQueueStateIndex>,
    warnings: &mut Vec<String>,
) -> io::Result<RuntimeQueueStateIndex> {
    match mode {
        RuntimeQueueStateIndexRefreshMode::Blocking => {
            rebuild_runtime_queue_state_index(queue_dir, warnings)
        }
        RuntimeQueueStateIndexRefreshMode::NonBlocking => {
            let receipts_file = queue_dir.join("run-once-receipts.jsonl");
            match try_with_jsonl_append_lock(&receipts_file, || {
                recover_runtime_queue_ledger_compaction_if_needed_locked(
                    &receipts_file,
                    warnings,
                )?;
                rebuild_runtime_queue_state_index_locked(queue_dir, warnings)
            })? {
                Some(index) => Ok(index),
                None => cached_index.ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::WouldBlock,
                        format!(
                            "runtime queue-state refresh is waiting for live receipt compaction at {}",
                            receipts_file.display()
                        ),
                    )
                }),
            }
        }
    }
}

/// Rebuilds the receipt index while the caller owns the receipt-ledger append
/// lock. This deliberately skips marker recovery so compaction never tries to
/// reacquire its own non-reentrant lock.
fn rebuild_runtime_queue_state_index_locked(
    queue_dir: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<RuntimeQueueStateIndex> {
    let mut index = RuntimeQueueStateIndex {
        schema: runtime_queue_state_index_schema(),
        revision: RUNTIME_QUEUE_STATE_INDEX_REVISION,
        receipt_ledger: RuntimeQueueReceiptLedgerCursor::default(),
        queues: BTreeMap::new(),
    };
    let receipts_file = queue_dir.join("run-once-receipts.jsonl");
    if receipts_file.is_file() {
        index.receipt_ledger = read_runtime_queue_receipts_from_cursor(
            &receipts_file,
            &mut index,
            RuntimeQueueReceiptLedgerCursor::default(),
            warnings,
            "rebuilding",
        )?;
    }
    write_runtime_queue_state_index(queue_dir, &index)?;
    Ok(index)
}

/// Refreshes the terminal-control index without replaying a stable historical
/// run-once ledger. The index remains compatible with old state files: a
/// missing cursor is treated as stale and rebuilt once from the ledger.
pub(crate) fn refresh_runtime_queue_state_index(
    queue_dir: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<RuntimeQueueStateIndex> {
    refresh_runtime_queue_state_index_with_mode(
        queue_dir,
        warnings,
        RuntimeQueueStateIndexRefreshMode::Blocking,
    )
}

/// Progress delivery is allowed to use the last materialized terminal-control
/// index during a live compaction window. It still tails normal appends, but it
/// never waits behind a ledger replacement just to plan a user-visible update.
pub(crate) fn refresh_runtime_queue_state_index_nonblocking(
    queue_dir: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<RuntimeQueueStateIndex> {
    refresh_runtime_queue_state_index_with_mode(
        queue_dir,
        warnings,
        RuntimeQueueStateIndexRefreshMode::NonBlocking,
    )
}

fn refresh_runtime_queue_state_index_with_mode(
    queue_dir: &Path,
    warnings: &mut Vec<String>,
    mode: RuntimeQueueStateIndexRefreshMode,
) -> io::Result<RuntimeQueueStateIndex> {
    let receipts_file = queue_dir.join("run-once-receipts.jsonl");
    if mode == RuntimeQueueStateIndexRefreshMode::Blocking {
        recover_runtime_queue_ledger_compaction_if_needed(&receipts_file, warnings)?;
    }
    let mut index = match read_runtime_queue_state_index(queue_dir) {
        Ok(Some(index))
            if index.schema == runtime_queue_state_index_schema()
                && index.revision == RUNTIME_QUEUE_STATE_INDEX_REVISION =>
        {
            index
        }
        Ok(Some(index)) => {
            warnings.push(format!(
                "runtime queue-state index has unsupported schema/revision `{}`/{}, rebuilding",
                index.schema, index.revision
            ));
            return rebuild_runtime_queue_state_index_for_refresh(mode, queue_dir, None, warnings);
        }
        Ok(None) => {
            return rebuild_runtime_queue_state_index_for_refresh(mode, queue_dir, None, warnings);
        }
        Err(error) => {
            warnings.push(format!(
                "runtime queue-state index could not be read; rebuilding: {error}"
            ));
            return rebuild_runtime_queue_state_index_for_refresh(mode, queue_dir, None, warnings);
        }
    };

    // A compactor writes this marker before replacing the ledger. Returning the
    // previously materialized index for this short maintenance window keeps the
    // progress path non-blocking and prevents a reader from observing the
    // Windows remove/rename gap.
    if runtime_queue_ledger_compaction_marker_file(&receipts_file).is_file() {
        return refresh_runtime_queue_state_index_after_marker(
            mode,
            queue_dir,
            &receipts_file,
            Some(index),
            warnings,
        );
    }

    let metadata = match fs::metadata(&receipts_file) {
        Ok(metadata) if metadata.is_file() => metadata,
        Ok(_) => {
            warnings.push(format!(
                "runtime run-once receipt path is not a file; rebuilding queue-state index: {}",
                receipts_file.display()
            ));
            return rebuild_runtime_queue_state_index_for_refresh(
                mode,
                queue_dir,
                Some(index),
                warnings,
            );
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if runtime_queue_ledger_compaction_marker_file(&receipts_file).is_file() {
                return refresh_runtime_queue_state_index_after_marker(
                    mode,
                    queue_dir,
                    &receipts_file,
                    Some(index),
                    warnings,
                );
            }
            if index.receipt_ledger.offset_bytes > 0
                || index.receipt_ledger.line_number > 0
                || index.receipt_ledger.source_modified_at_unix_nanos.is_some()
                || !index.queues.is_empty()
            {
                warnings.push(
                    "runtime run-once receipt ledger disappeared; rebuilding queue-state index"
                        .to_string(),
                );
                return rebuild_runtime_queue_state_index_for_refresh(
                    mode,
                    queue_dir,
                    Some(index),
                    warnings,
                );
            }
            return Ok(index);
        }
        Err(error) => return Err(error),
    };
    let source_modified_at_unix_nanos = file_modified_at_unix_nanos(&metadata);
    if index.receipt_ledger.source_modified_at_unix_nanos.is_none()
        || (index.receipt_ledger.offset_bytes > 0
            && index.receipt_ledger.prefix_tail_fingerprint.is_none())
    {
        warnings
            .push("runtime queue-state index has no receipt ledger cursor; rebuilding".to_string());
        return rebuild_runtime_queue_state_index_for_refresh(
            mode,
            queue_dir,
            Some(index),
            warnings,
        );
    }
    if metadata.len() < index.receipt_ledger.offset_bytes {
        warnings.push(
            "runtime run-once receipt ledger was truncated; rebuilding queue-state index"
                .to_string(),
        );
        return rebuild_runtime_queue_state_index_for_refresh(
            mode,
            queue_dir,
            Some(index),
            warnings,
        );
    }
    if metadata.len() == index.receipt_ledger.offset_bytes {
        if source_modified_at_unix_nanos == index.receipt_ledger.source_modified_at_unix_nanos
            && runtime_queue_receipt_prefix_tail_matches(&receipts_file, &index.receipt_ledger)?
        {
            return Ok(index);
        }
        warnings.push(
            "runtime run-once receipt ledger changed without an append; rebuilding queue-state index"
                .to_string(),
        );
        return rebuild_runtime_queue_state_index_for_refresh(
            mode,
            queue_dir,
            Some(index),
            warnings,
        );
    }
    let prefix_tail_matches =
        match runtime_queue_receipt_prefix_tail_matches(&receipts_file, &index.receipt_ledger) {
            Ok(matches) => matches,
            Err(error)
                if error.kind() == io::ErrorKind::NotFound
                    && runtime_queue_ledger_compaction_marker_file(&receipts_file).is_file() =>
            {
                return refresh_runtime_queue_state_index_after_marker(
                    mode,
                    queue_dir,
                    &receipts_file,
                    Some(index),
                    warnings,
                );
            }
            Err(error) => return Err(error),
        };
    if !prefix_tail_matches {
        warnings.push(
            "runtime run-once receipt ledger prefix no longer matches the index cursor; rebuilding queue-state index"
                .to_string(),
        );
        return rebuild_runtime_queue_state_index_for_refresh(
            mode,
            queue_dir,
            Some(index),
            warnings,
        );
    }

    let previous_cursor = index.receipt_ledger.clone();
    let refreshed_cursor = match read_runtime_queue_receipts_from_cursor(
        &receipts_file,
        &mut index,
        previous_cursor.clone(),
        warnings,
        "refreshing",
    ) {
        Ok(cursor) => cursor,
        Err(error)
            if error.kind() == io::ErrorKind::NotFound
                && runtime_queue_ledger_compaction_marker_file(&receipts_file).is_file() =>
        {
            return refresh_runtime_queue_state_index_after_marker(
                mode,
                queue_dir,
                &receipts_file,
                Some(index),
                warnings,
            );
        }
        Err(error) => return Err(error),
    };
    if refreshed_cursor != previous_cursor {
        index.receipt_ledger = refreshed_cursor;
        write_runtime_queue_state_index(queue_dir, &index)?;
    }
    Ok(index)
}

fn refresh_runtime_queue_state_index_after_marker(
    mode: RuntimeQueueStateIndexRefreshMode,
    queue_dir: &Path,
    receipts_file: &Path,
    cached_index: Option<RuntimeQueueStateIndex>,
    warnings: &mut Vec<String>,
) -> io::Result<RuntimeQueueStateIndex> {
    match mode {
        RuntimeQueueStateIndexRefreshMode::Blocking => {
            recover_runtime_queue_ledger_compaction_if_needed(receipts_file, warnings)?;
            refresh_runtime_queue_state_index_with_mode(queue_dir, warnings, mode)
        }
        RuntimeQueueStateIndexRefreshMode::NonBlocking => {
            match try_recover_runtime_queue_ledger_compaction_if_needed(receipts_file, warnings)? {
                RuntimeQueueLedgerCompactionRecovery::Busy => cached_index.ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::WouldBlock,
                        format!(
                            "runtime queue-state refresh has no cached index while compaction owns {}",
                            receipts_file.display()
                        ),
                    )
                }),
                RuntimeQueueLedgerCompactionRecovery::NoMarker
                | RuntimeQueueLedgerCompactionRecovery::Recovered => {
                    refresh_runtime_queue_state_index_with_mode(queue_dir, warnings, mode)
                }
            }
        }
    }
}

fn read_runtime_queue_state_index(queue_dir: &Path) -> io::Result<Option<RuntimeQueueStateIndex>> {
    let path = runtime_queue_state_index_file(queue_dir);
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(io::Error::other)
}

fn read_runtime_queue_receipts_from_cursor(
    receipts_file: &Path,
    index: &mut RuntimeQueueStateIndex,
    cursor: RuntimeQueueReceiptLedgerCursor,
    warnings: &mut Vec<String>,
    phase: &str,
) -> io::Result<RuntimeQueueReceiptLedgerCursor> {
    let file = File::open(receipts_file)?;
    let file_len = file.metadata()?.len();
    let start_offset = cursor.offset_bytes.min(file_len);
    let mut reader = BufReader::new(file);
    reader.seek(SeekFrom::Start(start_offset))?;
    let mut offset_bytes = start_offset;
    let mut line_number = if cursor.offset_bytes > file_len {
        0
    } else {
        cursor.line_number
    };
    loop {
        let mut line = String::new();
        let bytes_read = reader.read_line(&mut line)?;
        if bytes_read == 0 {
            break;
        }
        let complete_line = line.ends_with('\n');
        let trimmed = line.trim();
        if !complete_line && !trimmed.is_empty() && serde_json::from_str::<Value>(trimmed).is_err()
        {
            // A writer may have appended only part of a JSONL record. Keep the
            // cursor before that tail so the next wake can parse the completed
            // record instead of permanently skipping it as corruption.
            break;
        }
        line_number = line_number.saturating_add(1);
        offset_bytes = offset_bytes.saturating_add(bytes_read as u64);
        if trimmed.is_empty() {
            continue;
        }
        let value: Value = match serde_json::from_str(trimmed) {
            Ok(value) => value,
            Err(error) => {
                warnings.push(format!(
                    "runtime run-once receipt line {line_number} is not valid JSON while {phase} queue-state index: {error}"
                ));
                continue;
            }
        };
        apply_runtime_queue_receipt_to_index(index, &value);
    }
    let metadata = reader.get_ref().metadata()?;
    Ok(RuntimeQueueReceiptLedgerCursor {
        offset_bytes,
        line_number,
        source_modified_at_unix_nanos: file_modified_at_unix_nanos(&metadata),
        prefix_tail_fingerprint: runtime_queue_receipt_prefix_tail_fingerprint(
            receipts_file,
            offset_bytes,
        )?,
    })
}

fn apply_runtime_queue_receipt_to_index(index: &mut RuntimeQueueStateIndex, value: &Value) {
    if !is_trusted_runtime_run_once_receipt(value) {
        return;
    }
    let Some(queue_id) = string_field(value, &["queueId", "queue_id"]) else {
        return;
    };
    let Some(status) = string_field(value, &["status"]) else {
        return;
    };
    let entry = index.queues.entry(queue_id.to_string()).or_default();
    entry.latest_status = Some(status.to_string());
    entry.latest_reason = string_field(value, &["reason"]).map(ToString::to_string);
    entry.latest_occurred_at_ms = i64_field(
        value,
        &[
            "completedAtMs",
            "completed_at_ms",
            "occurredAtMs",
            "occurred_at_ms",
            "finishedAtMs",
            "finished_at_ms",
            "atMs",
            "at_ms",
        ],
    );
    entry.latest_runtime_class =
        string_field(value, &["runtimeClass", "runtime_class"]).map(ToString::to_string);
    entry.latest_origin = string_field(value, &["origin"]).map(ToString::to_string);
    entry.latest_transcript_file = path_field(
        value,
        &[
            "transcriptFile",
            "transcript_file",
            "plannedTranscriptFile",
            "planned_transcript_file",
        ],
    );
    entry.retry_schedule = value
        .get("retrySchedule")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok());
    let status_count = entry
        .run_once_status_counts
        .entry(status.to_string())
        .or_default();
    *status_count = status_count.saturating_add(1);
    if is_terminal_run_once_status(status) {
        entry.terminal_run_once_ever = true;
    }
    if status == "suppressed" {
        entry.suppression_recorded = true;
        if let Some(source) = string_field(value, &["terminalControlSource"]) {
            entry.terminal_control_source = Some(source.to_string());
        }
        return;
    }
    if is_terminal_run_once_status(status) {
        entry.terminal_ever = true;
        entry.terminal_status = Some(status.to_string());
        entry.terminal_reason = string_field(value, &["reason"]).map(ToString::to_string);
        entry.terminal_runtime_class =
            string_field(value, &["runtimeClass", "runtime_class"]).map(ToString::to_string);
        entry.terminal_origin = string_field(value, &["origin"]).map(ToString::to_string);
        entry.terminal_transcript_file = path_field(
            value,
            &[
                "transcriptFile",
                "transcript_file",
                "plannedTranscriptFile",
                "planned_transcript_file",
            ],
        );
        entry.terminal_occurred_at_ms = i64_field(
            value,
            &[
                "completedAtMs",
                "completed_at_ms",
                "occurredAtMs",
                "occurred_at_ms",
                "finishedAtMs",
                "finished_at_ms",
                "atMs",
                "at_ms",
            ],
        );
        entry.terminal_disposition = value
            .get("terminalDisposition")
            .cloned()
            .and_then(|value| serde_json::from_value(value).ok());
        entry.continuation_link = value
            .get("continuationLink")
            .cloned()
            .and_then(|value| serde_json::from_value(value).ok());
        entry
            .terminal_control_source
            .get_or_insert_with(|| TerminalControlSource::RunOnceTerminal.as_str().to_string());
    }
}

fn file_modified_at_unix_nanos(metadata: &fs::Metadata) -> Option<u128> {
    metadata
        .modified()
        .ok()
        .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos())
}

fn runtime_queue_receipt_prefix_tail_matches(
    receipts_file: &Path,
    cursor: &RuntimeQueueReceiptLedgerCursor,
) -> io::Result<bool> {
    Ok(
        runtime_queue_receipt_prefix_tail_fingerprint(receipts_file, cursor.offset_bytes)?
            == cursor.prefix_tail_fingerprint,
    )
}

fn runtime_queue_receipt_prefix_tail_fingerprint(
    receipts_file: &Path,
    offset_bytes: u64,
) -> io::Result<Option<u64>> {
    const FINGERPRINT_BYTES: u64 = 4 * 1024;
    if offset_bytes == 0 {
        return Ok(None);
    }
    let mut file = File::open(receipts_file)?;
    let start = offset_bytes.saturating_sub(FINGERPRINT_BYTES);
    file.seek(SeekFrom::Start(start))?;
    let mut remaining = offset_bytes.saturating_sub(start);
    let mut buffer = [0_u8; FINGERPRINT_BYTES as usize];
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    while remaining > 0 {
        let chunk_len = remaining.min(buffer.len() as u64) as usize;
        file.read_exact(&mut buffer[..chunk_len])?;
        for byte in &buffer[..chunk_len] {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        remaining = remaining.saturating_sub(chunk_len as u64);
    }
    Ok(Some(hash))
}

fn write_runtime_queue_state_index(
    queue_dir: &Path,
    index: &RuntimeQueueStateIndex,
) -> io::Result<()> {
    write_json_atomic(&runtime_queue_state_index_file(queue_dir), index)
}

pub(crate) fn runtime_queue_state_index_file(queue_dir: &Path) -> PathBuf {
    queue_dir.join("queue-state-index.json")
}

fn runtime_queue_receipt_compaction_lock_file(queue_dir: &Path) -> PathBuf {
    queue_dir.join("runtime-queue-receipt-compaction.transaction")
}

/// Compacts runtime queue state only after a completed turn, never on the
/// progress-delivery hot path. The active ledgers retain the queue states that
/// can still affect runnable work; bounded archives retain recent diagnostics.
pub fn compact_runtime_queue_receipts_if_needed(
    options: RuntimeQueueReceiptCompactionOptions,
) -> io::Result<RuntimeQueueReceiptCompactionReport> {
    let queue_dir = options.harness_home.join("state").join("runtime-queue");
    fs::create_dir_all(&queue_dir)?;
    let receipts_file = queue_dir.join("run-once-receipts.jsonl");
    let pending_file = queue_dir.join("pending.jsonl");
    let max_bytes = options.max_bytes.max(1);
    let mut report = RuntimeQueueReceiptCompactionReport {
        schema: RUNTIME_QUEUE_RECEIPT_COMPACTION_SCHEMA,
        harness_home: options.harness_home.clone(),
        receipts_file: receipts_file.clone(),
        pending_file: pending_file.clone(),
        status: RuntimeQueueReceiptCompactionStatus::Missing,
        original_bytes: 0,
        compacted_bytes: 0,
        archive_file: None,
        removed_archives: Vec::new(),
        removed_pending_items: 0,
        warnings: Vec::new(),
    };

    let transaction_lock = runtime_queue_receipt_compaction_lock_file(&queue_dir);
    let acquired = try_with_jsonl_append_lock(&transaction_lock, || {
        // The receipt and pending append locks form the cross-ledger snapshot
        // boundary. Recovery is deliberately repeated only after both locks
        // are owned so no writer can race a recovered file into the snapshot.
        with_jsonl_append_lock(&receipts_file, || {
            with_jsonl_append_lock(&pending_file, || {
                recover_runtime_queue_ledger_compaction_if_needed_locked(
                    &receipts_file,
                    &mut report.warnings,
                )?;
                recover_runtime_queue_ledger_compaction_if_needed_locked(
                    &pending_file,
                    &mut report.warnings,
                )?;
                // Both ledger markers are resolved while their append locks
                // are held, so any remaining staged batch is orphaned from a
                // pre-marker crash and can no longer be made visible.
                let removed_staged_history =
                    cleanup_staged_runtime_queue_receipt_history(&queue_dir)?;
                if removed_staged_history > 0 {
                    report.warnings.push(format!(
                        "discarded {removed_staged_history} orphaned runtime receipt history staging transaction(s)"
                    ));
                }

                let metadata = match fs::metadata(&receipts_file) {
                    Ok(metadata) if metadata.is_file() => metadata,
                    Ok(_) => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "runtime run-once receipt path is not a file: {}",
                                receipts_file.display()
                            ),
                        ));
                    }
                    Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
                    Err(error) => return Err(error),
                };
                report.original_bytes = metadata.len();
                if metadata.len() <= max_bytes {
                    report.status = RuntimeQueueReceiptCompactionStatus::Unchanged;
                    report.compacted_bytes = metadata.len();
                    return Ok(());
                }

                let index =
                    rebuild_runtime_queue_state_index_locked(&queue_dir, &mut report.warnings)?;
                let mut removed_archives = Vec::new();
                let (pending_snapshot, retained_queue_ids, removed_pending_items) =
                    runtime_queue_pending_snapshot(&pending_file, &index, &mut report.warnings)?;
                if let Some(outcome) = replace_runtime_queue_ledger_with_snapshot_locked(
                    &pending_file,
                    &queue_dir.join("pending-archive"),
                    &pending_snapshot,
                    options.max_archives.max(1),
                    options.now_ms,
                    None,
                )? {
                    removed_archives.extend(outcome.removed_archives);
                }

                let receipt_original = fs::read(&receipts_file)?;
                let receipt_snapshot = runtime_queue_receipt_snapshot(
                    &receipts_file,
                    &retained_queue_ids,
                    &mut report.warnings,
                )?;
                let receipt_history = if receipt_original == receipt_snapshot {
                    None
                } else {
                    Some(stage_runtime_queue_receipt_history(
                        &queue_dir,
                        &runtime_queue_receipt_history_transaction_id(
                            &receipt_original,
                            options.now_ms,
                        ),
                        &receipt_original,
                        &receipt_snapshot,
                        &retained_queue_ids,
                        options.now_ms,
                    )?)
                };
                let receipt_marker = runtime_queue_ledger_compaction_marker_file(&receipts_file);
                let receipt_outcome = replace_runtime_queue_ledger_with_snapshot_locked(
                    &receipts_file,
                    &queue_dir.join("run-once-receipts-archive"),
                    &receipt_snapshot,
                    options.max_archives.max(1),
                    options.now_ms,
                    receipt_history.as_ref(),
                );
                let outcome = match receipt_outcome {
                    Ok(Some(outcome)) => outcome,
                    Ok(None) => {
                        if let Some(staging) = receipt_history.as_ref() {
                            discard_runtime_queue_receipt_history(staging)?;
                        }
                        report.status = RuntimeQueueReceiptCompactionStatus::Unchanged;
                        report.compacted_bytes = metadata.len();
                        report.removed_archives = removed_archives;
                        report.removed_pending_items = removed_pending_items;
                        return Ok(());
                    }
                    Err(error) => {
                        // A published marker owns recovery. Without one, the
                        // hot-ledger swap never became durable and the staged
                        // cold rows are safe to remove immediately.
                        if !receipt_marker.is_file()
                            && let Some(staging) = receipt_history.as_ref()
                        {
                            discard_runtime_queue_receipt_history(staging)?;
                        }
                        return Err(error);
                    }
                };

                // Rebuild only from the new compact snapshot while both append
                // locks remain held. This makes the materialized index and both
                // active ledgers one recoverable maintenance transaction.
                let _ = rebuild_runtime_queue_state_index_locked(&queue_dir, &mut report.warnings)?;
                report.status = RuntimeQueueReceiptCompactionStatus::Compacted;
                report.compacted_bytes = outcome.compacted_bytes;
                report.archive_file = Some(outcome.archive_file);
                removed_archives.extend(outcome.removed_archives);
                report.removed_archives = removed_archives;
                report.removed_pending_items = removed_pending_items;
                Ok(())
            })
        })
    })?;
    if acquired.is_none() {
        report.status = RuntimeQueueReceiptCompactionStatus::Busy;
    }

    Ok(report)
}

pub(crate) fn default_runtime_queue_receipt_compaction_options(
    harness_home: PathBuf,
    now_ms: i64,
) -> RuntimeQueueReceiptCompactionOptions {
    RuntimeQueueReceiptCompactionOptions::with_defaults(harness_home, now_ms)
}

/// Performs bounded receipt maintenance after a terminal turn.  A manual
/// compact command intentionally bypasses this cadence; the automatic path
/// avoids repeatedly reparsing a large all-active queue that cannot yet shrink.
pub(crate) fn maybe_compact_runtime_queue_receipts_after_terminal(
    harness_home: PathBuf,
    now_ms: i64,
) -> io::Result<Option<RuntimeQueueReceiptCompactionReport>> {
    let queue_dir = harness_home.join("state").join("runtime-queue");
    let receipts_file = queue_dir.join("run-once-receipts.jsonl");
    let metadata = match fs::metadata(&receipts_file) {
        Ok(metadata) if metadata.is_file() => metadata,
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "runtime run-once receipt path is not a file: {}",
                    receipts_file.display()
                ),
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    if metadata.len() <= RUNTIME_QUEUE_RECEIPT_COMPACTION_DEFAULT_MAX_BYTES {
        return Ok(None);
    }

    let state_file = runtime_queue_receipt_compaction_state_file(&queue_dir);
    let previous_state = read_runtime_queue_receipt_compaction_state(&state_file)?;
    if runtime_queue_receipt_compaction_retry_is_deferred(
        now_ms,
        metadata.len(),
        previous_state.as_ref(),
    ) {
        return Ok(None);
    }

    let report = compact_runtime_queue_receipts_if_needed(
        default_runtime_queue_receipt_compaction_options(harness_home, now_ms),
    )?;
    if report.status == RuntimeQueueReceiptCompactionStatus::Busy {
        return Ok(Some(report));
    }
    let observed_bytes = fs::metadata(&receipts_file)
        .map(|metadata| metadata.len())
        .unwrap_or(report.compacted_bytes);
    write_json_atomic(
        &state_file,
        &RuntimeQueueReceiptCompactionState {
            schema: RUNTIME_QUEUE_RECEIPT_COMPACTION_STATE_SCHEMA.to_string(),
            last_attempt_at_ms: now_ms,
            last_attempt_bytes: observed_bytes,
        },
    )?;
    Ok(Some(report))
}

fn runtime_queue_receipt_compaction_state_file(queue_dir: &Path) -> PathBuf {
    queue_dir.join("run-once-receipt-compaction-state.json")
}

fn read_runtime_queue_receipt_compaction_state(
    state_file: &Path,
) -> io::Result<Option<RuntimeQueueReceiptCompactionState>> {
    let bytes = match fs::read(state_file) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let state: RuntimeQueueReceiptCompactionState =
        serde_json::from_slice(&bytes).map_err(io::Error::other)?;
    if state.schema != RUNTIME_QUEUE_RECEIPT_COMPACTION_STATE_SCHEMA {
        return Ok(None);
    }
    Ok(Some(state))
}

fn runtime_queue_receipt_compaction_retry_is_deferred(
    now_ms: i64,
    observed_bytes: u64,
    previous_state: Option<&RuntimeQueueReceiptCompactionState>,
) -> bool {
    let Some(state) = previous_state else {
        return false;
    };
    if now_ms <= 0 {
        return false;
    }
    let retry_at_ms = state
        .last_attempt_at_ms
        .saturating_add(RUNTIME_QUEUE_RECEIPT_COMPACTION_RETRY_INTERVAL_MS);
    let grew_enough = observed_bytes.saturating_sub(state.last_attempt_bytes)
        >= RUNTIME_QUEUE_RECEIPT_COMPACTION_RETRY_GROWTH_BYTES;
    now_ms < retry_at_ms && !grew_enough
}

#[derive(Debug)]
struct RuntimeQueueLedgerCompactionOutcome {
    archive_file: PathBuf,
    compacted_bytes: u64,
    removed_archives: Vec<PathBuf>,
}

fn runtime_queue_pending_snapshot(
    pending_file: &Path,
    index: &RuntimeQueueStateIndex,
    warnings: &mut Vec<String>,
) -> io::Result<(Vec<u8>, HashSet<String>, usize)> {
    let text = match fs::read_to_string(pending_file) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok((Vec::new(), HashSet::new(), 0));
        }
        Err(error) => return Err(error),
    };
    let mut snapshot = Vec::new();
    let mut retained_queue_ids = HashSet::new();
    let mut removed_terminal_items = 0usize;
    for (line_index, line) in text.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let value: Value = match serde_json::from_str(trimmed) {
            Ok(value) => value,
            Err(error) => {
                warnings.push(format!(
                    "runtime pending queue line {} is not valid JSON during compaction; retaining it: {}",
                    line_index + 1,
                    error
                ));
                snapshot.extend_from_slice(trimmed.as_bytes());
                snapshot.push(b'\n');
                continue;
            }
        };
        let Some(queue_id) = string_field(&value, &["queueId", "queue_id"]) else {
            warnings.push(format!(
                "runtime pending queue line {} has no queue id during compaction; retaining it",
                line_index + 1
            ));
            snapshot.extend_from_slice(trimmed.as_bytes());
            snapshot.push(b'\n');
            continue;
        };
        if string_field(&value, &["status"]) != Some("queued") {
            continue;
        }
        if index
            .queues
            .get(queue_id)
            .is_some_and(|entry| entry.terminal_run_once_ever)
        {
            removed_terminal_items = removed_terminal_items.saturating_add(1);
            continue;
        }
        retained_queue_ids.insert(queue_id.to_string());
        snapshot.extend(serde_json::to_vec(&value).map_err(io::Error::other)?);
        snapshot.push(b'\n');
    }
    Ok((snapshot, retained_queue_ids, removed_terminal_items))
}

fn runtime_queue_receipt_snapshot(
    receipts_file: &Path,
    retained_queue_ids: &HashSet<String>,
    warnings: &mut Vec<String>,
) -> io::Result<Vec<u8>> {
    let text = fs::read_to_string(receipts_file)?;
    let mut snapshot = Vec::new();
    for (line_index, line) in text.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let value: Value = match serde_json::from_str(trimmed) {
            Ok(value) => value,
            Err(error) => {
                // A malformed record has no safe queue identity. Preserve it
                // verbatim rather than silently deleting operator evidence.
                warnings.push(format!(
                    "runtime run-once receipt line {} is not valid JSON during compaction; retaining it: {}",
                    line_index + 1,
                    error
                ));
                snapshot.extend_from_slice(trimmed.as_bytes());
                snapshot.push(b'\n');
                continue;
            }
        };
        match string_field(&value, &["queueId", "queue_id"]) {
            Some(queue_id) if retained_queue_ids.contains(queue_id) => {
                // Retain the complete ordered history for runnable/retryable
                // work. Retry accounting consumes prior failure records, so a
                // latest-status-only snapshot would change execution behavior.
                snapshot.extend(serde_json::to_vec(&value).map_err(io::Error::other)?);
                snapshot.push(b'\n');
            }
            Some(_) => {}
            None => {
                // Keep unscoped records until an operator can classify them;
                // they may carry an implementation-specific control fact.
                warnings.push(format!(
                    "runtime run-once receipt line {} has no queue id during compaction; retaining it",
                    line_index + 1
                ));
                snapshot.extend(serde_json::to_vec(&value).map_err(io::Error::other)?);
                snapshot.push(b'\n');
            }
        }
    }
    Ok(snapshot)
}

fn replace_runtime_queue_ledger_with_snapshot_locked(
    ledger_file: &Path,
    archive_dir: &Path,
    snapshot: &[u8],
    max_archives: usize,
    now_ms: i64,
    history_staging: Option<&RuntimeQueueReceiptHistoryStaging>,
) -> io::Result<Option<RuntimeQueueLedgerCompactionOutcome>> {
    let original = match fs::read(ledger_file) {
        Ok(original) => original,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    if original == snapshot {
        return Ok(None);
    }
    fs::create_dir_all(archive_dir)?;
    let archive_file = next_runtime_queue_ledger_archive_file(archive_dir, ledger_file, now_ms);
    let archive_temp = runtime_queue_ledger_temp_file(&archive_file, now_ms, "archive");
    write_runtime_queue_ledger_file(&archive_temp, &original)?;
    fs::rename(&archive_temp, &archive_file)?;

    let compact_temp = runtime_queue_ledger_temp_file(ledger_file, now_ms, "compact");
    write_runtime_queue_ledger_file(&compact_temp, snapshot)?;
    let pending = RuntimeQueueLedgerCompactionPending {
        schema: RUNTIME_QUEUE_RECEIPT_COMPACTION_PENDING_SCHEMA.to_string(),
        ledger_file: ledger_file.to_path_buf(),
        archive_file: archive_file.clone(),
        temp_file: compact_temp.clone(),
        expected_bytes: snapshot.len() as u64,
        expected_digest: runtime_queue_ledger_digest(&compact_temp)?,
        archive_expected_bytes: Some(original.len() as u64),
        archive_expected_digest: Some(runtime_queue_ledger_digest(&archive_file)?),
        history_store_file: history_staging.map(|staging| staging.store_file.clone()),
        history_transaction_id: history_staging.map(|staging| staging.transaction_id.clone()),
    };
    let marker_file = runtime_queue_ledger_compaction_marker_file(ledger_file);
    write_json_atomic(&marker_file, &pending)?;
    replace_runtime_queue_ledger_file(&compact_temp, ledger_file)?;
    if !runtime_queue_ledger_matches(ledger_file, pending.expected_bytes, pending.expected_digest)?
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "runtime queue ledger replacement did not match its compact snapshot: {}",
                ledger_file.display()
            ),
        ));
    }
    if let Some(staging) = history_staging {
        commit_runtime_queue_receipt_history(staging, now_ms)?;
    }
    let removed_archives =
        prune_runtime_queue_ledger_archives(archive_dir, ledger_file, max_archives.max(1))?;
    let _ = fs::remove_file(&marker_file);
    Ok(Some(RuntimeQueueLedgerCompactionOutcome {
        archive_file,
        compacted_bytes: snapshot.len() as u64,
        removed_archives,
    }))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RuntimeQueueLedgerCompactionRecovery {
    NoMarker,
    Busy,
    Recovered,
}

fn recover_runtime_queue_ledger_compaction_if_needed(
    ledger_file: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<()> {
    let marker_file = runtime_queue_ledger_compaction_marker_file(ledger_file);
    if !marker_file.is_file() {
        return Ok(());
    }
    with_jsonl_append_lock(ledger_file, || {
        recover_runtime_queue_ledger_compaction_if_needed_locked(ledger_file, warnings).map(|_| ())
    })
}

fn try_recover_runtime_queue_ledger_compaction_if_needed(
    ledger_file: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<RuntimeQueueLedgerCompactionRecovery> {
    if !runtime_queue_ledger_compaction_marker_file(ledger_file).is_file() {
        return Ok(RuntimeQueueLedgerCompactionRecovery::NoMarker);
    }
    match try_with_jsonl_append_lock(ledger_file, || {
        recover_runtime_queue_ledger_compaction_if_needed_locked(ledger_file, warnings)
    })? {
        Some(outcome) => Ok(outcome),
        None => Ok(RuntimeQueueLedgerCompactionRecovery::Busy),
    }
}

fn recover_runtime_queue_ledger_compaction_if_needed_locked(
    ledger_file: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<RuntimeQueueLedgerCompactionRecovery> {
    let marker_file = runtime_queue_ledger_compaction_marker_file(ledger_file);
    if !marker_file.is_file() {
        return Ok(RuntimeQueueLedgerCompactionRecovery::NoMarker);
    }
    let pending: RuntimeQueueLedgerCompactionPending =
        serde_json::from_slice(&fs::read(&marker_file)?).map_err(io::Error::other)?;
    let is_v3 = pending.schema == RUNTIME_QUEUE_RECEIPT_COMPACTION_PENDING_SCHEMA;
    let is_v2 = pending.schema == RUNTIME_QUEUE_RECEIPT_COMPACTION_PENDING_SCHEMA_V2;
    if (!is_v3 && !is_v2) || pending.ledger_file.as_path() != ledger_file {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "runtime queue ledger compaction marker is invalid for {}",
                ledger_file.display()
            ),
        ));
    }
    let history_staging = runtime_queue_receipt_history_staging_from_marker(ledger_file, &pending)?;
    if runtime_queue_ledger_matches(ledger_file, pending.expected_bytes, pending.expected_digest)? {
        if let Some(staging) = history_staging.as_ref() {
            commit_runtime_queue_receipt_history(staging, current_log_time_ms()?)?;
        }
        let _ = fs::remove_file(&pending.temp_file);
        let _ = fs::remove_file(&marker_file);
        warnings.push(format!(
            "recovered completed runtime queue ledger compaction for {}",
            ledger_file.display()
        ));
        return Ok(RuntimeQueueLedgerCompactionRecovery::Recovered);
    }
    if runtime_queue_ledger_matches(
        &pending.temp_file,
        pending.expected_bytes,
        pending.expected_digest,
    )? {
        replace_runtime_queue_ledger_file(&pending.temp_file, ledger_file)?;
        if let Some(staging) = history_staging.as_ref() {
            commit_runtime_queue_receipt_history(staging, current_log_time_ms()?)?;
        }
        let _ = fs::remove_file(&marker_file);
        warnings.push(format!(
            "recovered interrupted runtime queue ledger compaction from its snapshot for {}",
            ledger_file.display()
        ));
        return Ok(RuntimeQueueLedgerCompactionRecovery::Recovered);
    }
    if pending.archive_file.is_file() {
        let archive_is_valid = if is_v3 {
            match (
                pending.archive_expected_bytes,
                pending.archive_expected_digest,
            ) {
                (Some(expected_bytes), Some(expected_digest)) => runtime_queue_ledger_matches(
                    &pending.archive_file,
                    expected_bytes,
                    expected_digest,
                )?,
                _ => false,
            }
        } else {
            runtime_queue_ledger_is_valid_jsonl(&pending.archive_file)?
        };
        if !archive_is_valid {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "runtime queue ledger compaction archive failed integrity validation for {}",
                    ledger_file.display()
                ),
            ));
        }
        let restore_temp = runtime_queue_ledger_temp_file(ledger_file, 0, "restore");
        let archived = fs::read(&pending.archive_file)?;
        write_runtime_queue_ledger_file(&restore_temp, &archived)?;
        replace_runtime_queue_ledger_file(&restore_temp, ledger_file)?;
        if let Some(staging) = history_staging.as_ref() {
            discard_runtime_queue_receipt_history(staging)?;
        }
        let _ = fs::remove_file(&pending.temp_file);
        let _ = fs::remove_file(&marker_file);
        if is_v2 {
            warnings.push(format!(
                "accepted legacy v2 runtime queue compaction marker with structural archive validation for {}",
                ledger_file.display()
            ));
        }
        warnings.push(format!(
            "restored runtime queue ledger from archive after interrupted compaction: {}",
            ledger_file.display()
        ));
        return Ok(RuntimeQueueLedgerCompactionRecovery::Recovered);
    }
    Err(io::Error::new(
        io::ErrorKind::NotFound,
        format!(
            "runtime queue ledger compaction cannot recover {}; both snapshot and archive are missing",
            ledger_file.display()
        ),
    ))
}

fn runtime_queue_receipt_history_staging_from_marker(
    ledger_file: &Path,
    pending: &RuntimeQueueLedgerCompactionPending,
) -> io::Result<Option<RuntimeQueueReceiptHistoryStaging>> {
    match (&pending.history_store_file, &pending.history_transaction_id) {
        (None, None) => Ok(None),
        (Some(store_file), Some(transaction_id)) if !transaction_id.trim().is_empty() => {
            let queue_dir = ledger_file.parent().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "runtime queue ledger has no parent directory for history recovery: {}",
                        ledger_file.display()
                    ),
                )
            })?;
            let expected_store = runtime_queue_receipt_history_file(queue_dir);
            if store_file != &expected_store {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "runtime queue ledger compaction marker points at an unexpected history store for {}",
                        ledger_file.display()
                    ),
                ));
            }
            Ok(Some(RuntimeQueueReceiptHistoryStaging {
                store_file: store_file.clone(),
                transaction_id: transaction_id.clone(),
            }))
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "runtime queue ledger compaction marker has incomplete history staging metadata for {}",
                ledger_file.display()
            ),
        )),
    }
}

fn runtime_queue_ledger_is_valid_jsonl(path: &Path) -> io::Result<bool> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    let mut records = 0usize;
    for line in BufReader::new(file).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        if serde_json::from_str::<Value>(&line).is_err() {
            return Ok(false);
        }
        records = records.saturating_add(1);
    }
    Ok(records > 0)
}

fn runtime_queue_ledger_compaction_marker_file(ledger_file: &Path) -> PathBuf {
    let file_name = ledger_file
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("ledger.jsonl");
    ledger_file.with_file_name(format!(".{file_name}.compaction-pending.json"))
}

fn runtime_queue_ledger_temp_file(ledger_file: &Path, now_ms: i64, kind: &str) -> PathBuf {
    let file_name = ledger_file
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("ledger.jsonl");
    let mut attempt = 0_u32;
    loop {
        let candidate = ledger_file.with_file_name(format!(
            ".{file_name}.{}.{}.{}.{}.tmp",
            std::process::id(),
            now_ms,
            kind,
            attempt
        ));
        if !candidate.exists() {
            return candidate;
        }
        attempt = attempt.saturating_add(1);
    }
}

fn next_runtime_queue_ledger_archive_file(
    archive_dir: &Path,
    ledger_file: &Path,
    now_ms: i64,
) -> PathBuf {
    let stem = ledger_file
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("ledger");
    let mut attempt = 0_u32;
    loop {
        let suffix = if attempt == 0 {
            String::new()
        } else {
            format!("-{attempt}")
        };
        let candidate = archive_dir.join(format!("{stem}-{now_ms}{suffix}.jsonl"));
        if !candidate.exists() {
            return candidate;
        }
        attempt = attempt.saturating_add(1);
    }
}

fn write_runtime_queue_ledger_file(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut file = File::create(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

fn replace_runtime_queue_ledger_file(temp_file: &Path, ledger_file: &Path) -> io::Result<()> {
    let started = Instant::now();
    loop {
        match fs::remove_file(ledger_file) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::PermissionDenied | io::ErrorKind::AlreadyExists
                ) && started.elapsed() < Duration::from_secs(10) =>
            {
                thread::sleep(Duration::from_millis(10));
                continue;
            }
            Err(error) => return Err(error),
        }
        match fs::rename(temp_file, ledger_file) {
            Ok(()) => return Ok(()),
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::PermissionDenied | io::ErrorKind::AlreadyExists
                ) && started.elapsed() < Duration::from_secs(10) =>
            {
                thread::sleep(Duration::from_millis(10));
            }
            Err(error) => return Err(error),
        }
    }
}

fn runtime_queue_ledger_matches(
    ledger_file: &Path,
    expected_bytes: u64,
    expected_digest: u64,
) -> io::Result<bool> {
    let metadata = match fs::metadata(ledger_file) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    if metadata.len() != expected_bytes {
        return Ok(false);
    }
    Ok(runtime_queue_ledger_digest(ledger_file)? == expected_digest)
}

/// Full-stream FNV-1a checksum for compaction markers. This is not a security
/// primitive; it is a durable accidental-corruption detector used only on the
/// rare recovery path, where reading the complete compact snapshot is safe.
fn runtime_queue_ledger_digest(path: &Path) -> io::Result<u64> {
    let mut file = File::open(path)?;
    let mut buffer = [0_u8; 16 * 1024];
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    loop {
        let bytes_read = file.read(&mut buffer)?;
        if bytes_read == 0 {
            return Ok(hash);
        }
        for byte in &buffer[..bytes_read] {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
}

fn runtime_queue_receipt_history_transaction_id(original: &[u8], now_ms: i64) -> String {
    format!(
        "receipt-{}-{now_ms}-{:016x}",
        std::process::id(),
        runtime_queue_bytes_digest(original)
    )
}

fn runtime_queue_bytes_digest(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn prune_runtime_queue_ledger_archives(
    archive_dir: &Path,
    ledger_file: &Path,
    max_archives: usize,
) -> io::Result<Vec<PathBuf>> {
    let stem = ledger_file
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("ledger");
    let expected_prefix = format!("{stem}-");
    let mut archives = fs::read_dir(archive_dir)?
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let path = entry.path();
            let name = path.file_name()?.to_str()?;
            if !entry.file_type().ok()?.is_file()
                || !name.starts_with(&expected_prefix)
                || !name.ends_with(".jsonl")
            {
                return None;
            }
            let modified = entry
                .metadata()
                .ok()
                .and_then(|metadata| metadata.modified().ok());
            Some((path, modified))
        })
        .collect::<Vec<_>>();
    archives.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| right.0.cmp(&left.0)));
    let mut removed = Vec::new();
    for (path, _) in archives.into_iter().skip(max_archives.max(1)) {
        fs::remove_file(&path)?;
        removed.push(path);
    }
    Ok(removed)
}

fn queue_skip_control_reason(queue_dir: &Path, queue_id: &str) -> io::Result<Option<String>> {
    let receipts_file = queue_dir.join("control-receipts.jsonl");
    if !receipts_file.is_file() {
        return Ok(None);
    }
    let text = fs::read_to_string(receipts_file)?;
    let mut reason = None;
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let value: Value = match serde_json::from_str(trimmed) {
            Ok(value) => value,
            Err(_) => continue,
        };
        let id = string_field(&value, &["originalQueueId", "queueId", "queue_id"]);
        if id != Some(queue_id) {
            continue;
        }
        let action = string_field(&value, &["action"]);
        let status = string_field(&value, &["status"]);
        if action == Some("skip") || status == Some("skipped") {
            reason = Some(
                string_field(&value, &["reason"])
                    .unwrap_or("queue-skip control receipt matched")
                    .to_string(),
            );
        }
    }
    Ok(reason)
}

fn scoped_stop_marker_reason(
    queue_dir: &Path,
    queue_id: &str,
    session_key: Option<&str>,
) -> io::Result<Option<String>> {
    let cancel_dir = queue_dir.join("cancel");
    let queue_marker = cancel_dir.join(format!("queue-{}.stop", normalize_key_part(queue_id)));
    if let Some(reason) = marker_reason(&queue_marker)? {
        return Ok(Some(reason));
    }
    if let Some(session_key) = session_key {
        let turn_marker = cancel_dir.join(format!("turn-{}.stop", normalize_key_part(session_key)));
        if let Some(reason) = marker_reason(&turn_marker)? {
            return Ok(Some(reason));
        }
    }
    Ok(None)
}

fn quarantine_marker_reason(queue_dir: &Path, queue_id: &str) -> io::Result<Option<String>> {
    let marker_file = queue_dir
        .join("quarantine")
        .join(format!("{}.json", normalize_key_part(queue_id)));
    marker_reason(&marker_file)
}

fn marker_reason(path: &Path) -> io::Result<Option<String>> {
    if !path.is_file() {
        return Ok(None);
    }
    let text = fs::read_to_string(path)?;
    let value: Value = match serde_json::from_str(&text) {
        Ok(value) => value,
        Err(_) => {
            return Ok(Some(format!(
                "terminal control marker exists at {}",
                path.display()
            )));
        }
    };
    Ok(Some(
        string_field(&value, &["reason"])
            .unwrap_or("terminal control marker exists")
            .to_string(),
    ))
}

fn runtime_capacity_blocker(
    harness_home: &Path,
    state: &RuntimeQueueLeaseState,
    item: &PendingQueueItem,
) -> io::Result<Option<String>> {
    let config = load_runtime_dispatch_config(harness_home)?;
    let all_leases =
        read_all_runtime_queue_leases_with_override(harness_home, state, &item.runtime_class)?;
    if all_leases
        .iter()
        .any(|lease| exact_lane_coordinator_mutual_exclusion(item, lease))
    {
        return Ok(Some(
            "exact-lane coordinator mutual exclusion with active runtime lease".to_string(),
        ));
    }
    let executing_global = all_leases.len();
    if executing_global >= config.global_concurrency_limit {
        return Ok(Some("global runtime limit".to_string()));
    }

    if item.runtime_class != "interactive" {
        let non_reserved_limit = config
            .global_concurrency_limit
            .saturating_sub(config.interactive_reserve);
        if executing_global >= non_reserved_limit {
            return Ok(Some("interactive reserve".to_string()));
        }
    }

    let class_config =
        config
            .classes
            .get(&item.runtime_class)
            .cloned()
            .unwrap_or(RuntimeDispatchClassConfig {
                max_active: config.global_concurrency_limit,
                per_agent_max_active: config.global_concurrency_limit,
                per_channel_max_active: config.global_concurrency_limit,
                per_session_max_active: usize::MAX,
                session_fifo: false,
                same_session_main_agent_serialization: false,
                per_job_max_active: usize::MAX,
                max_queued_per_agent: usize::MAX,
            });

    let executing_class = all_leases
        .iter()
        .filter(|lease| lease.runtime_class == item.runtime_class)
        .count();
    if executing_class >= class_config.max_active {
        return Ok(Some(format!(
            "runtime class `{}` limit",
            item.runtime_class
        )));
    }

    let executing_agent = all_leases
        .iter()
        .filter(|lease| {
            lease.runtime_class == item.runtime_class && lease.agent_id == item.agent_id
        })
        .count();
    if executing_agent >= class_config.per_agent_max_active {
        return Ok(Some(format!(
            "runtime class `{}` agent limit for `{}`",
            item.runtime_class, item.agent_id
        )));
    }

    if let Some(cron_run_id) = item.cron_run_id.as_deref() {
        let executing_job = all_leases
            .iter()
            .filter(|lease| lease.cron_run_id.as_deref() == Some(cron_run_id))
            .count();
        if executing_job >= class_config.per_job_max_active {
            return Ok(Some(format!("cron job active limit for `{cron_run_id}`")));
        }
    }

    let channel_key = runtime_channel_key(&item.agent_id, &item.platform, &item.channel_id);
    if let Some(session_lane_key) = item_session_lane_key(item, &class_config) {
        let executing_session = all_leases
            .iter()
            .filter(|lease| {
                lease_session_lane_key(lease, &class_config).as_deref()
                    == Some(session_lane_key.as_str())
            })
            .count();
        if executing_session >= class_config.per_session_max_active {
            return Ok(Some(format!(
                "session-active limit for `{session_lane_key}`"
            )));
        }
    }

    let executing_channel = all_leases
        .iter()
        .filter(|lease| {
            runtime_channel_key(&lease.agent_id, &lease.platform, &lease.channel_id) == channel_key
        })
        .count();
    if executing_channel >= class_config.per_channel_max_active {
        return Ok(Some(format!("agent-channel limit for `{}`", channel_key)));
    }

    Ok(None)
}

fn read_all_runtime_queue_leases_with_override(
    harness_home: &Path,
    override_state: &RuntimeQueueLeaseState,
    override_class: &str,
) -> io::Result<Vec<RuntimeQueueLease>> {
    let queue_dir = harness_home.join("state").join("runtime-queue");
    let mut classes = runtime_classes_for_release(&queue_dir);
    if !classes.iter().any(|class| class == override_class) {
        classes.push(override_class.to_string());
    }
    let mut warnings = Vec::new();
    let mut leases = Vec::new();
    let now_ms = current_log_time_ms().unwrap_or(0);
    for runtime_class in classes {
        if runtime_class == override_class {
            leases.extend(
                override_state
                    .leases
                    .values()
                    .filter(|lease| {
                        lease.lease_expires_at_ms > now_ms
                            && dead_runtime_queue_lease_owner(&lease.owner).is_none()
                    })
                    .cloned(),
            );
            continue;
        }
        let state = read_runtime_queue_leases(&queue_dir, &runtime_class, &mut warnings)?;
        leases.extend(state.leases.values().filter_map(|lease| {
            (lease.lease_expires_at_ms > now_ms
                && dead_runtime_queue_lease_owner(&lease.owner).is_none())
            .then_some(lease.clone())
        }));
    }
    Ok(leases)
}

impl RuntimeQueueLeaseOwner {
    fn process_id(&self) -> Option<i64> {
        match self {
            RuntimeQueueLeaseOwner::Legacy(owner) => legacy_pid_owner(owner),
            RuntimeQueueLeaseOwner::Envelope(owner) => Some(owner.pid),
        }
    }

    fn display_label(&self) -> String {
        match self {
            RuntimeQueueLeaseOwner::Legacy(owner) => owner.clone(),
            RuntimeQueueLeaseOwner::Envelope(owner) => format!(
                "{} serviceId={} generationId={} pid={} processStartTimeMs={}",
                owner.kind,
                owner.service_id,
                owner.generation_id,
                owner.pid,
                owner.process_start_time_ms
            ),
        }
    }

    fn matches_generation(&self, service_id: &str, generation_id: &str) -> bool {
        match self {
            RuntimeQueueLeaseOwner::Envelope(owner) => {
                owner.service_id == service_id && owner.generation_id == generation_id
            }
            RuntimeQueueLeaseOwner::Legacy(_) => false,
        }
    }

    fn as_json_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or_else(|_| Value::String(self.display_label()))
    }
}

fn runtime_queue_lease_owner_from_env(now_ms: i64) -> RuntimeQueueLeaseOwner {
    let generation_id = nonempty_env("AGENT_HARNESS_SERVICE_GENERATION_ID");
    let process_start_time_ms = env_i64("AGENT_HARNESS_SERVICE_STARTED_AT_MS");
    let launch_owner = nonempty_env("AGENT_HARNESS_SUPERVISOR_LAUNCH_OWNER");
    runtime_queue_lease_owner_from_metadata(
        now_ms,
        generation_id,
        process_start_time_ms,
        launch_owner,
    )
}

fn runtime_queue_lease_owner_from_metadata(
    now_ms: i64,
    generation_id: Option<String>,
    process_start_time_ms: Option<i64>,
    launch_owner: Option<String>,
) -> RuntimeQueueLeaseOwner {
    let pid = i64::from(std::process::id());
    let process_start_time_ms = process_start_time_ms
        .filter(|value| *value > 0)
        .unwrap_or(now_ms);
    let generation_id = generation_id
        .unwrap_or_else(|| format!("{RUNTIME_LOOP_SERVICE_ID}-{pid}-{process_start_time_ms}"));
    let kind = if launch_owner.is_some() {
        "supervisor-child"
    } else {
        "process"
    };
    RuntimeQueueLeaseOwner::Envelope(RuntimeQueueLeaseOwnerEnvelope {
        kind: kind.to_string(),
        service_id: RUNTIME_LOOP_SERVICE_ID.to_string(),
        generation_id,
        pid,
        process_start_time_ms,
        acquired_at_ms: now_ms,
    })
}

fn nonempty_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn env_i64(name: &str) -> Option<i64> {
    nonempty_env(name)?.parse::<i64>().ok()
}

fn legacy_pid_owner(owner: &str) -> Option<i64> {
    owner.strip_prefix("pid:")?.trim().parse::<i64>().ok()
}

fn dead_runtime_queue_lease_owner(owner: &RuntimeQueueLeaseOwner) -> Option<i64> {
    let process_id = owner.process_id()?;
    (process_alive_for_pid(process_id) == Some(false)).then_some(process_id)
}

fn lease_acquired_receipt(item: &PendingQueueItem, reason: &str) -> RuntimeExecutionReceipt {
    RuntimeExecutionReceipt {
        queue_id: Some(item.queue_id.clone()),
        status: RuntimeExecutionReceiptStatus::LeaseAcquired,
        channel_lane: None,
        runtime_class: Some(item.runtime_class.clone()),
        origin: Some(item.origin.clone()),
        cron_run_id: item.cron_run_id.clone(),
        scheduled_for_ms: item.scheduled_for_ms,
        execution_dir: None,
        prompt_bundle_json: None,
        prompt_markdown: None,
        runtime_workspace: item.runtime_workspace.clone(),
        inbound_media_artifacts: item.inbound_media_artifacts.clone(),
        continuation: item.continuation.clone(),
        terminal_control_matched: None,
        terminal_control_source: None,
        suppressed_run_once_reason: None,
        reason: reason.to_string(),
    }
}

fn lease_runtime_queue_item(
    state: &mut RuntimeQueueLeaseState,
    item: &PendingQueueItem,
    owner: &RuntimeQueueLeaseOwner,
    now_ms: i64,
) {
    let session_lane_key = default_runtime_class_config_for_item(item)
        .and_then(|class_config| item_session_lane_key(item, &class_config));
    state.leases.insert(
        item.queue_id.clone(),
        RuntimeQueueLease {
            queue_id: item.queue_id.clone(),
            agent_id: item.agent_id.clone(),
            runtime_class: item.runtime_class.clone(),
            origin: item.origin.clone(),
            cron_run_id: item.cron_run_id.clone(),
            platform: item.platform.clone(),
            account_id: item.account_id.clone(),
            channel_id: item.channel_id.clone(),
            user_id: Some(item.user_id.clone()),
            session_key: item.session_key.clone(),
            virtual_session_id: item
                .coordinator_resume
                .as_ref()
                .map(|metadata| metadata.owner.virtual_session_id.clone())
                .or_else(|| item.continuation.virtual_session_id.clone())
                .or_else(|| {
                    Some(derive_virtual_session_id(
                        &item.platform,
                        &item.channel_id,
                        &item.user_id,
                        &item.agent_id,
                        &root_working_session_key(&item.session_key),
                    ))
                }),
            session_lane_key,
            owner: owner.clone(),
            started_at_ms: now_ms,
            lease_expires_at_ms: now_ms.saturating_add(DEFAULT_RUNTIME_LEASE_MS),
        },
    );
}

fn exact_lane_coordinator_mutual_exclusion(
    item: &PendingQueueItem,
    lease: &RuntimeQueueLease,
) -> bool {
    if item.origin != "coordinator-resume" && lease.origin != "coordinator-resume" {
        return false;
    }
    let item_virtual_session_id = item
        .coordinator_resume
        .as_ref()
        .map(|metadata| metadata.owner.virtual_session_id.clone())
        .or_else(|| item.continuation.virtual_session_id.clone())
        .unwrap_or_else(|| {
            derive_virtual_session_id(
                &item.platform,
                &item.channel_id,
                &item.user_id,
                &item.agent_id,
                &root_working_session_key(&item.session_key),
            )
        });
    item.runtime_class == lease.runtime_class
        && item.agent_id == lease.agent_id
        && item.platform == lease.platform
        && item.account_id == lease.account_id
        && item.channel_id == lease.channel_id
        && lease.user_id.as_deref() == Some(item.user_id.as_str())
        && root_working_session_key(&item.session_key)
            == root_working_session_key(&lease.session_key)
        && item.session_key == lease.session_key
        && runtime_queue_lease_virtual_session_id(lease).as_deref()
            == Some(item_virtual_session_id.as_str())
}

fn runtime_channel_key(agent_id: &str, platform: &str, channel_id: &str) -> String {
    format!(
        "{}:{}:{}",
        normalize_key_part(agent_id),
        normalize_key_part(platform),
        normalize_key_part(channel_id)
    )
}

fn runtime_session_lane_key(
    runtime_class: &str,
    agent_id: &str,
    platform: &str,
    account_id: Option<&str>,
    channel_id: &str,
    user_id: &str,
    session_key: &str,
) -> String {
    let canonical_session_key = account_id
        .filter(|value| !value.trim().is_empty())
        .and_then(|account_id| {
            crate::channel_session_key::CanonicalChannelSessionKey::bind_for_lane(
                session_key,
                platform,
                account_id,
                channel_id,
                user_id,
                agent_id,
            )
            .ok()
            .map(|key| key.canonical_string())
        })
        .unwrap_or_else(|| session_key.to_string());
    [
        "v2".to_string(),
        normalize_key_part(runtime_class),
        normalize_key_part(agent_id),
        normalize_key_part(platform),
        normalize_key_part(account_id.unwrap_or("legacy-default")),
        normalize_key_part(channel_id),
        normalize_key_part(user_id),
        normalize_key_part(&canonical_session_key),
    ]
    .join(":")
}

fn default_runtime_class_config_for_item(
    item: &PendingQueueItem,
) -> Option<RuntimeDispatchClassConfig> {
    Some(match item.runtime_class.as_str() {
        "interactive" => RuntimeDispatchClassConfig {
            max_active: usize::MAX,
            per_agent_max_active: usize::MAX,
            per_channel_max_active: usize::MAX,
            per_session_max_active: 1,
            session_fifo: true,
            same_session_main_agent_serialization: true,
            per_job_max_active: usize::MAX,
            max_queued_per_agent: usize::MAX,
        },
        _ => RuntimeDispatchClassConfig {
            max_active: usize::MAX,
            per_agent_max_active: usize::MAX,
            per_channel_max_active: usize::MAX,
            per_session_max_active: usize::MAX,
            session_fifo: false,
            same_session_main_agent_serialization: false,
            per_job_max_active: usize::MAX,
            max_queued_per_agent: usize::MAX,
        },
    })
}

fn is_interactive_channel_main_lane(
    runtime_class: &str,
    origin: &str,
    cron_run_id: Option<&str>,
    class_config: &RuntimeDispatchClassConfig,
) -> bool {
    class_config.same_session_main_agent_serialization
        && runtime_class == "interactive"
        && runtime_origin_is_parent_channel(origin)
        && cron_run_id.is_none()
        && class_config.per_session_max_active > 0
}

fn runtime_origin_is_parent_channel(origin: &str) -> bool {
    matches!(origin, "channel" | "coordinator-resume")
}

fn item_session_lane_key(
    item: &PendingQueueItem,
    class_config: &RuntimeDispatchClassConfig,
) -> Option<String> {
    is_interactive_channel_main_lane(
        &item.runtime_class,
        &item.origin,
        item.cron_run_id.as_deref(),
        class_config,
    )
    .then(|| {
        runtime_session_lane_key(
            &item.runtime_class,
            &item.agent_id,
            &item.platform,
            item.account_id.as_deref(),
            &item.channel_id,
            &item.user_id,
            &item.session_key,
        )
    })
}

fn lease_session_lane_key(
    lease: &RuntimeQueueLease,
    class_config: &RuntimeDispatchClassConfig,
) -> Option<String> {
    if !is_interactive_channel_main_lane(
        &lease.runtime_class,
        &lease.origin,
        lease.cron_run_id.as_deref(),
        class_config,
    ) {
        return None;
    }
    if let Some(key) = lease
        .session_lane_key
        .as_ref()
        .filter(|key| key.starts_with("v2:"))
    {
        return Some(key.clone());
    }
    Some(runtime_session_lane_key(
        &lease.runtime_class,
        &lease.agent_id,
        &lease.platform,
        lease.account_id.as_deref(),
        &lease.channel_id,
        lease.user_id.as_deref().unwrap_or(""),
        &lease.session_key,
    ))
}

fn same_session_fifo_blocker(
    pending_items: &[PendingQueueItem],
    item: &PendingQueueItem,
    terminal_run_ids: &HashSet<String>,
    config: &RuntimeDispatchConfig,
) -> Option<String> {
    let class_config = config.classes.get(&item.runtime_class)?;
    if !class_config.session_fifo {
        return None;
    }
    let lane_key = item_session_lane_key(item, class_config)?;
    pending_items
        .iter()
        .filter(|candidate| candidate.queue_id != item.queue_id)
        .filter(|candidate| !terminal_run_ids.contains(&candidate.queue_id))
        .filter(|candidate| {
            candidate.created_at_ms < item.created_at_ms
                || (candidate.created_at_ms == item.created_at_ms
                    && candidate.queue_id < item.queue_id)
        })
        .find(|candidate| {
            config
                .classes
                .get(&candidate.runtime_class)
                .and_then(|candidate_config| item_session_lane_key(candidate, candidate_config))
                .as_deref()
                == Some(lane_key.as_str())
        })
        .map(|candidate| {
            format!(
                "session-fifo for `{lane_key}` waiting on older queue item `{}`",
                candidate.queue_id
            )
        })
}

fn retry_schedule_dispatch_blocker(
    index: &RuntimeQueueStateIndex,
    queue_id: &str,
    now_ms: i64,
) -> Option<String> {
    let entry = index.queues.get(queue_id)?;
    if entry.latest_status.as_deref() != Some("retry-pending") {
        return None;
    }
    let schedule = entry.retry_schedule.as_ref()?;
    (now_ms < schedule.next_eligible_at_ms).then(|| {
        format!(
            "durable retry backoff until {} (lineage `{}`; attempt {}/{})",
            schedule.next_eligible_at_ms,
            schedule.lineage_id,
            schedule.attempt,
            schedule.max_attempts
        )
    })
}

fn cron_runtime_dispatch_blocker_for_item(
    harness_home: &Path,
    item: &PendingQueueItem,
    now_ms: i64,
) -> io::Result<Option<String>> {
    let Some(run_id) = item.cron_run_id.as_deref() else {
        return Ok(None);
    };
    cron_run_runtime_dispatch_blocker(harness_home, run_id, &item.queue_id, now_ms)
}

fn select_pending_item(
    pending_items: Vec<PendingQueueItem>,
    requested_queue_id: Option<&str>,
    prepared_ids: &HashSet<String>,
    terminal_run_ids: &HashSet<String>,
    auth_deferred_run_ids: &HashSet<String>,
    run_once_index: &RuntimeQueueStateIndex,
    lease_state: &RuntimeQueueLeaseState,
    runtime_class: &str,
    harness_home: &Path,
    queue_dir: &Path,
    now_ms: i64,
    warnings: &mut Vec<String>,
) -> io::Result<Option<PendingQueueItem>> {
    let mut pending_items = pending_items
        .into_iter()
        .filter(|item| item.runtime_class == runtime_class)
        .collect::<Vec<_>>();
    if runtime_class == "cron" {
        let round_by_queue_id = cron_agent_rounds(&pending_items);
        pending_items.sort_by(|left, right| {
            let left_round = round_by_queue_id.get(&left.queue_id).copied().unwrap_or(0);
            let right_round = round_by_queue_id.get(&right.queue_id).copied().unwrap_or(0);
            left_round
                .cmp(&right_round)
                .then_with(|| left.created_at_ms.cmp(&right.created_at_ms))
                .then_with(|| left.agent_id.cmp(&right.agent_id))
                .then_with(|| left.queue_id.cmp(&right.queue_id))
        });
    } else {
        pending_items.sort_by(|left, right| {
            runtime_selection_key(left)
                .cmp(&runtime_selection_key(right))
                .then_with(|| left.queue_id.cmp(&right.queue_id))
        });
    }
    let config = load_runtime_dispatch_config(harness_home)?;
    for item in pending_items.iter() {
        if requested_queue_id.is_some_and(|requested| requested != item.queue_id) {
            continue;
        }
        if let QueueTerminalControl::Terminal(control) =
            resolve_queue_terminal_control(harness_home, &item.queue_id, Some(&item.session_key))?
        {
            warnings.push(format!(
                "runtime queue item `{}` suppressed by terminal control {}; skipping",
                item.queue_id,
                control.source.as_str()
            ));
            record_terminal_control_suppression(
                harness_home,
                &item.queue_id,
                Some(&item.runtime_class),
                Some(&item.origin),
                item.cron_run_id.as_deref(),
                item.scheduled_for_ms,
                &item.continuation,
                &control,
            )?;
            continue;
        }
        if prepared_ids.contains(&item.queue_id) {
            warnings.push(format!(
                "runtime queue item `{}` already has a prepared receipt; skipping",
                item.queue_id
            ));
            continue;
        }
        if terminal_run_ids.contains(&item.queue_id) {
            warnings.push(format!(
                "runtime queue item `{}` already has a terminal run receipt; skipping",
                item.queue_id
            ));
            continue;
        }
        if auth_deferred_run_ids.contains(&item.queue_id) {
            warnings.push(format!(
                "runtime queue item `{}` is waiting for operator authentication; skipping until its retry-pending wake",
                item.queue_id
            ));
            continue;
        }
        if let Some(blocker) =
            retry_schedule_dispatch_blocker(run_once_index, &item.queue_id, now_ms)
        {
            warnings.push(format!(
                "runtime queue item `{}` blocked by {}; skipping",
                item.queue_id, blocker
            ));
            continue;
        }
        if lease_state.leases.contains_key(&item.queue_id) {
            warnings.push(format!(
                "runtime queue item `{}` is already leased; skipping",
                item.queue_id
            ));
            continue;
        }
        if let Some(blocker) = cron_runtime_dispatch_blocker_for_item(harness_home, item, now_ms)? {
            warnings.push(format!(
                "runtime queue item `{}` blocked by {}; tombstoning",
                item.queue_id, blocker
            ));
            tombstone_runtime_queue_item_skipped(queue_dir, &item, &blocker)?;
            continue;
        }
        if let Some(blocker) = runtime_capacity_blocker(harness_home, lease_state, item)? {
            warnings.push(format!(
                "runtime queue item `{}` blocked by {}; skipping",
                item.queue_id, blocker
            ));
            continue;
        }
        if let Some(blocker) =
            same_session_fifo_blocker(&pending_items, item, terminal_run_ids, &config)
        {
            warnings.push(format!(
                "runtime queue item `{}` blocked by {}; skipping",
                item.queue_id, blocker
            ));
            continue;
        }
        return Ok(Some(item.clone()));
    }
    Ok(None)
}

fn runtime_selection_key(item: &PendingQueueItem) -> (usize, String, i64) {
    let class_rank = match item.runtime_class.as_str() {
        "interactive" => 0,
        "cron" => 1,
        "worker" => 2,
        _ => 3,
    };
    (class_rank, item.agent_id.clone(), item.created_at_ms)
}

fn cron_agent_rounds(pending_items: &[PendingQueueItem]) -> HashMap<String, usize> {
    let mut ordered = pending_items.iter().collect::<Vec<_>>();
    ordered.sort_by(|left, right| {
        left.created_at_ms
            .cmp(&right.created_at_ms)
            .then_with(|| left.queue_id.cmp(&right.queue_id))
    });
    let mut next_round_by_agent = HashMap::<String, usize>::new();
    let mut round_by_queue_id = HashMap::<String, usize>::new();
    for item in ordered {
        let round = next_round_by_agent
            .entry(item.agent_id.clone())
            .or_insert(0);
        round_by_queue_id.insert(item.queue_id.clone(), *round);
        *round += 1;
    }
    round_by_queue_id
}

fn execution_receipt_status_exists(
    receipts_file: &Path,
    queue_id: &str,
    status: RuntimeExecutionReceiptStatus,
) -> bool {
    let Ok(text) = fs::read_to_string(receipts_file) else {
        return false;
    };
    text.lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .any(|value| {
            string_field(&value, &["queueId", "queue_id"]) == Some(queue_id)
                && value
                    .get("status")
                    .and_then(Value::as_str)
                    .is_some_and(|value| {
                        serde_json::from_value::<RuntimeExecutionReceiptStatus>(Value::String(
                            value.to_string(),
                        ))
                        .ok()
                            == Some(status)
                    })
        })
}

fn canonicalize_pending_items_for_dispatch(
    harness_home: &Path,
    execution_receipts_file: &Path,
    items: &mut Vec<PendingQueueItem>,
    now_ms: i64,
    warnings: &mut Vec<String>,
) -> io::Result<()> {
    let mut canonical = Vec::with_capacity(items.len());
    for mut item in items.drain(..) {
        let account_id = item
            .account_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or("default")
            .to_string();
        match crate::channel_session_key::CanonicalChannelSessionKey::bind_for_lane(
            &item.session_key,
            &item.platform,
            &account_id,
            &item.channel_id,
            &item.user_id,
            &item.agent_id,
        ) {
            Ok(parsed) => {
                let canonical_session_key = parsed.canonical_string();
                let changed = canonical_session_key != item.session_key
                    || item.account_id.as_deref() != Some(account_id.as_str());
                item.session_key = canonical_session_key.clone();
                item.account_id = Some(account_id.clone());
                if changed
                    && !execution_receipt_status_exists(
                        execution_receipts_file,
                        &item.queue_id,
                        RuntimeExecutionReceiptStatus::SessionIdentityNormalized,
                    )
                {
                    let lane = runtime_session_lane_key(
                        &item.runtime_class,
                        &item.agent_id,
                        &item.platform,
                        Some(&account_id),
                        &item.channel_id,
                        &item.user_id,
                        &canonical_session_key,
                    );
                    append_json_line(
                        execution_receipts_file,
                        &serde_json::json!({
                            "queueId": item.queue_id.clone(),
                            "status": RuntimeExecutionReceiptStatus::SessionIdentityNormalized,
                            "runtimeClass": item.runtime_class.clone(),
                            "origin": item.origin.clone(),
                            "canonicalLaneDigest": format!(
                                "{:016x}",
                                runtime_queue_bytes_digest(lane.as_bytes())
                            ),
                            "atMs": now_ms,
                            "reason": "legacy pending session normalized to exact canonical lane before dispatch"
                        }),
                    )?;
                    warnings.push(format!(
                        "runtime queue item `{}` session identity normalized before dispatch",
                        item.queue_id
                    ));
                }
                canonical.push(item);
            }
            Err(error) => {
                let reason = format!(
                    "runtime queue item `{}` session identity is invalid for its exact lane: {error}",
                    item.queue_id
                );
                write_runtime_queue_quarantine_marker(
                    harness_home,
                    &item.queue_id,
                    &reason,
                    now_ms,
                )?;
                if !execution_receipt_status_exists(
                    execution_receipts_file,
                    &item.queue_id,
                    RuntimeExecutionReceiptStatus::InvalidCanonicalLaneQuarantined,
                ) {
                    append_json_line(
                        execution_receipts_file,
                        &serde_json::json!({
                            "queueId": item.queue_id.clone(),
                            "status": RuntimeExecutionReceiptStatus::InvalidCanonicalLaneQuarantined,
                            "runtimeClass": item.runtime_class.clone(),
                            "origin": item.origin.clone(),
                            "atMs": now_ms,
                            "reason": reason,
                        }),
                    )?;
                }
                warnings.push(reason);
            }
        }
    }
    *items = canonical;
    Ok(())
}

fn read_pending_items(
    queue_file: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<Vec<PendingQueueItem>> {
    recover_runtime_queue_ledger_compaction_if_needed(queue_file, warnings)?;
    if !queue_file.is_file() {
        warnings.push(format!(
            "runtime queue file not found at {}",
            queue_file.display()
        ));
        return Ok(Vec::new());
    }

    let queue_dir = queue_file.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "runtime pending queue file has no parent directory: {}",
                queue_file.display()
            ),
        )
    })?;
    let pending_values = read_queued_pending_values_from_index(queue_dir, warnings)?;
    let mut items = Vec::new();
    for value in pending_values {
        let Some(queue_id) = string_field(&value, &["queueId", "queue_id"]) else {
            // The source-authoritative pending projection excludes rows with
            // no queue id.  Keep this defensive guard in case an older or
            // manually repaired sidecar is encountered.
            warnings.push("runtime pending index row has no queue id; skipping".to_string());
            continue;
        };
        match parse_pending_item(&value) {
            Ok(item) => items.push(item),
            Err(error) => {
                let has_execution_snapshot = [
                    "provider",
                    "model",
                    "reasoningPreference",
                    "reasoning_preference",
                    "backendReasoningPolicy",
                    "backend_reasoning_policy",
                ]
                .iter()
                .any(|key| value.get(*key).is_some_and(|value| !value.is_null()));
                let reason = if has_execution_snapshot {
                    format!(
                        "runtime queue item `{queue_id}` has an invalid execution snapshot: {error}"
                    )
                } else {
                    format!("runtime queue item `{queue_id}` is invalid: {error}")
                };
                if has_execution_snapshot
                    && let Some(harness_home) = queue_file
                        .parent()
                        .and_then(Path::parent)
                        .and_then(Path::parent)
                    && let Err(marker_error) = write_runtime_queue_quarantine_marker(
                        harness_home,
                        queue_id,
                        &reason,
                        current_log_time_ms().unwrap_or_default(),
                    )
                {
                    warnings.push(format!(
                        "failed to quarantine malformed runtime queue item `{queue_id}`: {marker_error}"
                    ));
                }
                warnings.push(reason);
            }
        }
    }

    Ok(items)
}

pub(crate) fn terminal_run_once_ids_from_index(index: &RuntimeQueueStateIndex) -> HashSet<String> {
    index
        .queues
        .iter()
        .filter_map(|(queue_id, entry)| entry.terminal_run_once_ever.then_some(queue_id.clone()))
        .collect()
}

/// Resolves terminal state only for the caller's exact current queue IDs.
///
/// The hot materialized index is authoritative for live receipts, while the
/// committed cold history supplies compacted terminal tombstones. This avoids
/// replaying the append-only JSONL ledger in latency-sensitive callers such as
/// typing/working-indicator setup.
pub fn resolve_runtime_queue_terminal_ids(
    harness_home: &Path,
    candidate_queue_ids: &BTreeSet<String>,
) -> io::Result<BTreeSet<String>> {
    if candidate_queue_ids.is_empty() {
        return Ok(BTreeSet::new());
    }
    let queue_dir = harness_home.join("state").join("runtime-queue");
    let hot_index = refresh_runtime_queue_state_index(&queue_dir, &mut Vec::new())?;
    let mut terminal_ids = candidate_queue_ids
        .iter()
        .filter(|queue_id| {
            hot_index
                .queues
                .get(queue_id.as_str())
                .is_some_and(|entry| entry.terminal_run_once_ever)
        })
        .cloned()
        .collect::<BTreeSet<_>>();
    for record in find_runtime_queue_terminal_history(&queue_dir, candidate_queue_ids)? {
        if candidate_queue_ids.contains(&record.queue_id) {
            terminal_ids.insert(record.queue_id);
        }
    }
    Ok(terminal_ids)
}

/// Resolves terminal state for exact queue IDs without waiting behind an
/// active receipt append or compaction.  A committed hot index is preferred;
/// if that index is momentarily unavailable, an empty terminal set is a
/// deliberate best-effort fallback for provider-visible activity indicators.
/// The normal scheduler continues to use the blocking authoritative variant.
pub fn resolve_runtime_queue_terminal_ids_nonblocking(
    harness_home: &Path,
    candidate_queue_ids: &BTreeSet<String>,
) -> io::Result<BTreeSet<String>> {
    if candidate_queue_ids.is_empty() {
        return Ok(BTreeSet::new());
    }
    let queue_dir = harness_home.join("state").join("runtime-queue");
    let hot_index = match refresh_runtime_queue_state_index_nonblocking(&queue_dir, &mut Vec::new())
    {
        Ok(index) => index,
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(BTreeSet::new()),
        Err(error) => return Err(error),
    };
    let mut terminal_ids = candidate_queue_ids
        .iter()
        .filter(|queue_id| {
            hot_index
                .queues
                .get(queue_id.as_str())
                .is_some_and(|entry| entry.terminal_run_once_ever)
        })
        .cloned()
        .collect::<BTreeSet<_>>();
    match find_runtime_queue_terminal_history_nonblocking(&queue_dir, candidate_queue_ids) {
        Ok(records) => {
            for record in records {
                if candidate_queue_ids.contains(&record.queue_id) {
                    terminal_ids.insert(record.queue_id);
                }
            }
        }
        // A cold-history maintenance transaction must not hold up a typing
        // update.  The committed hot index above is still safe evidence.
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
        Err(error) => return Err(error),
    }
    Ok(terminal_ids)
}

/// Returns the first still-runnable queue's provider routing context through
/// lock-first materialized projections.  This is the only supported hot path
/// for typing/working indicators; it never replays the pending JSONL ledger.
pub fn resolve_runtime_queue_typing_context_nonblocking(
    harness_home: &Path,
    requested_queue_id: Option<&str>,
) -> io::Result<Option<RuntimeQueueTypingContext>> {
    let queue_dir = harness_home.join("state").join("runtime-queue");
    let pending_values =
        read_queued_pending_values_from_index_nonblocking(&queue_dir, &mut Vec::new())?;
    let candidate_queue_ids = pending_values
        .iter()
        .filter_map(|value| {
            let queue_id = string_field(value, &["queueId", "queue_id"])?;
            if requested_queue_id.is_some_and(|requested| requested != queue_id)
                || string_field(value, &["status"]) != Some("queued")
            {
                return None;
            }
            Some(queue_id.to_string())
        })
        .collect::<BTreeSet<_>>();
    let terminal_ids =
        resolve_runtime_queue_terminal_ids_nonblocking(harness_home, &candidate_queue_ids)?;
    for value in pending_values {
        let queue_id = string_field(&value, &["queueId", "queue_id"]);
        if let Some(requested_queue_id) = requested_queue_id
            && queue_id != Some(requested_queue_id)
        {
            continue;
        }
        if string_field(&value, &["status"]) != Some("queued")
            || queue_id.is_some_and(|queue_id| terminal_ids.contains(queue_id))
        {
            continue;
        }
        let Some(agent_id) = string_field(&value, &["agentId", "agent_id"]) else {
            continue;
        };
        let Some(platform) = string_field(&value, &["platform"]) else {
            continue;
        };
        let Some(channel_id) = string_field(&value, &["channelId", "channel_id"]) else {
            continue;
        };
        return Ok(Some(RuntimeQueueTypingContext {
            agent_id: agent_id.to_string(),
            platform: platform.to_string(),
            channel_id: channel_id.to_string(),
        }));
    }
    Ok(None)
}

/// Returns the latest hot-ledger receipt materialized for one exact queue ID.
/// This is the normal reader path for current runtime and virtual-session
/// state; it never replays the JSONL ledger.
pub(crate) fn latest_runtime_queue_hot_receipt_from_index(
    index: &RuntimeQueueStateIndex,
    queue_id: &str,
) -> Option<RuntimeQueueHotReceiptRecord> {
    let entry = index.queues.get(queue_id)?;
    let status = entry.latest_status.clone()?;
    Some(RuntimeQueueHotReceiptRecord {
        queue_id: queue_id.to_string(),
        status,
        reason: entry.latest_reason.clone(),
        runtime_class: entry.latest_runtime_class.clone(),
        origin: entry.latest_origin.clone(),
        transcript_file: entry.latest_transcript_file.clone(),
        occurred_at_ms: entry.latest_occurred_at_ms,
    })
}

/// Returns the latest actual terminal receipt materialized for one exact queue
/// ID. Suppression receipts retain their existing terminal-control semantics
/// and are not promoted into worker completion records here.
pub(crate) fn terminal_runtime_queue_hot_receipt_from_index(
    index: &RuntimeQueueStateIndex,
    queue_id: &str,
) -> Option<RuntimeQueueHotReceiptRecord> {
    let entry = index.queues.get(queue_id)?;
    let status = entry.terminal_status.clone()?;
    Some(RuntimeQueueHotReceiptRecord {
        queue_id: queue_id.to_string(),
        status,
        reason: entry.terminal_reason.clone(),
        runtime_class: entry.terminal_runtime_class.clone(),
        origin: entry.terminal_origin.clone(),
        transcript_file: entry.terminal_transcript_file.clone(),
        occurred_at_ms: entry.terminal_occurred_at_ms,
    })
}

/// Returns the latest materialized hot receipt for each requested queue ID.
/// The requested set makes this an exact bounded lookup even when the hot
/// ledger itself is large.
pub(crate) fn latest_runtime_queue_hot_receipts_from_index(
    index: &RuntimeQueueStateIndex,
    queue_ids: &BTreeSet<String>,
) -> Vec<RuntimeQueueHotReceiptRecord> {
    queue_ids
        .iter()
        .filter_map(|queue_id| latest_runtime_queue_hot_receipt_from_index(index, queue_id))
        .collect()
}

pub(crate) fn runtime_queue_status_count_from_index(
    index: &RuntimeQueueStateIndex,
    queue_id: &str,
    expected_status: &str,
) -> usize {
    index
        .queues
        .get(queue_id)
        .and_then(|entry| entry.run_once_status_counts.get(expected_status))
        .copied()
        .unwrap_or_default()
}

/// Aggregates hot-ledger status counts from the incrementally maintained
/// index. Metrics and diagnostics use this instead of reparsing the ledger.
pub(crate) fn runtime_queue_status_counts_from_index(
    index: &RuntimeQueueStateIndex,
) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::<String, usize>::new();
    for entry in index.queues.values() {
        for (status, status_count) in &entry.run_once_status_counts {
            let current = counts.entry(status.clone()).or_default();
            *current = current.saturating_add(*status_count);
        }
    }
    counts
}

pub(crate) fn runtime_queue_terminal_receipt_count_from_index(
    index: &RuntimeQueueStateIndex,
) -> usize {
    index
        .queues
        .values()
        .flat_map(|entry| entry.run_once_status_counts.iter())
        .filter(|(status, _)| is_terminal_run_once_status(status.as_str()))
        .fold(0usize, |count, (_, status_count)| {
            count.saturating_add(*status_count)
        })
}

pub(crate) fn runtime_queue_prior_failure_count_from_index(
    index: &RuntimeQueueStateIndex,
    queue_id: &str,
) -> usize {
    index
        .queues
        .get(queue_id)
        .map(|entry| {
            entry
                .run_once_status_counts
                .iter()
                .filter(|(status, _)| {
                    let status = status.as_str();
                    !is_terminal_run_once_status(status)
                        && status != "completed"
                        && status != "no-work"
                        && status != "auth-deferred"
                })
                .fold(0usize, |count, (_, status_count)| {
                    count.saturating_add(*status_count)
                })
        })
        .unwrap_or_default()
}

fn retry_pending_run_once_ids_from_index(index: &RuntimeQueueStateIndex) -> HashSet<String> {
    index
        .queues
        .iter()
        .filter_map(|(queue_id, entry)| {
            (entry.latest_status.as_deref() == Some("retry-pending")).then_some(queue_id.clone())
        })
        .collect()
}

fn auth_deferred_run_once_ids_from_index(index: &RuntimeQueueStateIndex) -> HashSet<String> {
    index
        .queues
        .iter()
        .filter_map(|(queue_id, entry)| {
            (entry.latest_status.as_deref() == Some("auth-deferred")).then_some(queue_id.clone())
        })
        .collect()
}

fn is_terminal_run_once_status(status: &str) -> bool {
    matches!(
        status,
        "completed"
            | "timeout"
            | "failed-terminal"
            | "canceled"
            | "skipped"
            | "dead-letter"
            | "suppressed"
    )
}

fn parse_pending_item(value: &Value) -> Result<PendingQueueItem, String> {
    let source = value
        .get("source")
        .ok_or_else(|| "missing source object".to_string())?;
    let platform = string_field(value, &["platform"])
        .ok_or_else(|| "missing platform".to_string())?
        .to_string();
    let (provider, model, reasoning_preference, backend_reasoning_policy) =
        parse_pending_reasoning_snapshot(value)?;
    let runtime_class = string_field(value, &["runtimeClass", "runtime_class"])
        .map(ToString::to_string)
        .unwrap_or_else(|| default_runtime_class_for(&platform));
    let queue_id = string_field(value, &["queueId", "queue_id"])
        .ok_or_else(|| "missing queue id".to_string())?
        .to_string();
    let admission_queue_id =
        string_field(value, &["admissionQueueId", "admission_queue_id"]).map(ToString::to_string);
    let coordinator_resume = coordinator_resume_metadata_from_value(value, &queue_id)?;
    let queue_schema = string_field(value, &["schema"]).unwrap_or_default();
    if !queue_schema.is_empty()
        && queue_schema != "agent-harness.runtime-queue-item.v1"
        && queue_schema != "agent-harness.runtime-queue-item.v2"
    {
        return Err(format!("unsupported runtime queue schema `{queue_schema}`"));
    }
    let authorized_execution_mode = value
        .get("authorizedExecutionMode")
        .or_else(|| value.get("authorized_execution_mode"))
        .map(|raw| {
            serde_json::from_value::<AuthorizedExecutionModeSnapshotV2>(raw.clone())
                .map_err(|error| format!("invalid authorized execution-mode snapshot: {error}"))
        })
        .transpose()?;
    let is_v2 = queue_schema == "agent-harness.runtime-queue-item.v2";
    let has_admission_queue_id = admission_queue_id.is_some();
    let has_execution_snapshot = authorized_execution_mode.is_some();
    if is_v2 && !has_admission_queue_id && (has_execution_snapshot || coordinator_resume.is_none())
    {
        return Err("runtime queue item v2 is missing admissionQueueId".to_string());
    }
    if is_v2 && !has_execution_snapshot && (has_admission_queue_id || coordinator_resume.is_none())
    {
        return Err("runtime queue item v2 is missing authorizedExecutionMode".to_string());
    }
    if queue_schema == "agent-harness.runtime-queue-item.v1"
        && (has_admission_queue_id || has_execution_snapshot)
    {
        return Err("runtime queue item v1 must not carry V2 admission fields".to_string());
    }
    let origin = string_field(value, &["origin"])
        .map(ToString::to_string)
        .unwrap_or_else(|| {
            if platform == "native-cron" {
                "cron-scheduler".to_string()
            } else {
                "channel".to_string()
            }
        });
    if (origin == "coordinator-resume") != coordinator_resume.is_some() {
        return Err(
            "coordinator-resume origin and typed coordinatorResume metadata must appear together"
                .to_string(),
        );
    }
    let pending = PendingQueueItem {
        queue_id,
        admission_queue_id,
        created_at_ms: i64_field(value, &["createdAtMs", "created_at_ms"]).unwrap_or(0),
        agent_id: string_field(value, &["agentId", "agent_id"])
            .ok_or_else(|| "missing agent id".to_string())?
            .to_string(),
        session_key: string_field(value, &["sessionKey", "session_key"])
            .ok_or_else(|| "missing session key".to_string())?
            .to_string(),
        runtime_class,
        origin,
        cron_run_id: string_field(value, &["cronRunId", "cron_run_id"]).map(ToString::to_string),
        scheduled_for_ms: i64_field(value, &["scheduledForMs", "scheduled_for_ms"]),
        platform,
        account_id: string_field(value, &["accountId", "account_id"]).map(ToString::to_string),
        channel_id: string_field(value, &["channelId", "channel_id"])
            .ok_or_else(|| "missing channel id".to_string())?
            .to_string(),
        user_id: string_field(value, &["userId", "user_id"])
            .ok_or_else(|| "missing user id".to_string())?
            .to_string(),
        message_text: string_field(value, &["messageText", "message_text"])
            .ok_or_else(|| "missing message text".to_string())?
            .to_string(),
        inbound_context: string_field(value, &["inboundContext", "inbound_context"])
            .map(ToString::to_string),
        inbound_media_artifacts: inbound_media_artifacts_field(
            value,
            &["inboundMediaArtifacts", "inbound_media_artifacts"],
        ),
        source_home: path_field(source, &["sourceHome", "source_home"])
            .ok_or_else(|| "missing source home".to_string())?,
        source_workspace: path_field(source, &["sourceWorkspace", "source_workspace"])
            .ok_or_else(|| "missing source workspace".to_string())?,
        runtime_workspace: path_field(source, &["runtimeWorkspace", "runtime_workspace"]),
        provider,
        model,
        reasoning_preference,
        backend_reasoning_policy,
        authorized_execution_mode,
        planned_transcript_file: path_field(
            value,
            &["plannedTranscriptFile", "planned_transcript_file"],
        )
        .ok_or_else(|| "missing planned transcript file".to_string())?,
        planned_trajectory_file: path_field(
            value,
            &["plannedTrajectoryFile", "planned_trajectory_file"],
        )
        .ok_or_else(|| "missing planned trajectory file".to_string())?,
        selected_skill_ids: string_array_field(value, &["selectedSkillIds", "selected_skill_ids"]),
        continuation: continuation_metadata_from_value(value),
        coordinator_resume,
    };
    if let Some(metadata) = pending.coordinator_resume.as_ref() {
        let account_id = pending.account_id.as_deref().ok_or_else(|| {
            "coordinator resume queue item is missing exact accountId".to_string()
        })?;
        metadata
            .validate_queued_lane(crate::coordinator_resume::CoordinatorResumeQueuedLaneV1 {
                platform: &pending.platform,
                account_id,
                channel_id: &pending.channel_id,
                user_id: &pending.user_id,
                agent_id: &pending.agent_id,
                runtime_class: &pending.runtime_class,
                session_key: &pending.session_key,
            })
            .map_err(|error| format!("invalid coordinator resume queued lane: {error}"))?;
    }
    revalidate_pending_execution_snapshot(&pending)?;
    Ok(pending)
}

fn revalidate_pending_execution_snapshot(item: &PendingQueueItem) -> Result<(), String> {
    let Some(snapshot) = &item.authorized_execution_mode else {
        return Ok(());
    };
    snapshot
        .validate()
        .map_err(|error| format!("invalid authorized execution-mode snapshot: {error}"))?;
    if snapshot.effective_mode() != STANDARD_EXECUTION_MODE {
        return Err(format!(
            "non-standard execution mode is not supported in this release: {}",
            snapshot.effective_mode()
        ));
    }
    if let Some(ReasoningPreference::Explicit { effort }) = &item.reasoning_preference
        && is_reserved_execution_mode_effort(effort)
    {
        return Err(
            "Ultra is reserved for execution mode and cannot be reasoning effort".to_string(),
        );
    }
    if let Some(policy) = &item.backend_reasoning_policy
        && is_reserved_execution_mode_effort(policy.effective_effort())
    {
        return Err("backend reasoning policy must not encode Ultra execution mode".to_string());
    }
    if let Some(execution_agent_id) = snapshot.execution_agent_id()
        && execution_agent_id != item.agent_id
    {
        return Err(format!(
            "execution snapshot agent mismatch: authorized `{execution_agent_id}`, queued `{}`",
            item.agent_id
        ));
    }
    if snapshot.effective_mode() == STANDARD_EXECUTION_MODE && snapshot.result_owner().is_none() {
        return Ok(());
    }
    if snapshot.execution_agent_id().is_none() {
        return Err("non-standard execution snapshot is missing execution agent".to_string());
    }
    let owner = snapshot.result_owner().ok_or_else(|| {
        "non-standard execution snapshot is missing exact result owner".to_string()
    })?;
    let admission_queue_id = item.admission_queue_id.as_deref().ok_or_else(|| {
        "authorized execution snapshot is missing immutable admissionQueueId".to_string()
    })?;
    if owner.source_queue_id != admission_queue_id {
        return Err(
            "execution snapshot owner is not bound to immutable admissionQueueId".to_string(),
        );
    }
    if item.origin == "channel" {
        let lane = &owner.lane;
        let account_id = item
            .account_id
            .as_deref()
            .ok_or_else(|| "execution snapshot requires exact queued accountId".to_string())?;
        let root_session = root_working_session_key(&item.session_key);
        let expected_virtual_session_id = derive_virtual_session_id(
            &item.platform,
            &item.channel_id,
            &item.user_id,
            &item.agent_id,
            &root_session,
        );
        if owner.virtual_session_id != expected_virtual_session_id {
            return Err("execution snapshot virtualSessionId mismatch".to_string());
        }
        let observed = [
            ("platform", lane.platform(), item.platform.as_str()),
            ("accountId", lane.account_id(), account_id),
            ("channelId", lane.channel_id(), item.channel_id.as_str()),
            ("userId", lane.user_id(), item.user_id.as_str()),
            ("agentId", lane.agent_id(), item.agent_id.as_str()),
            (
                "runtimeClass",
                lane.runtime_class(),
                item.runtime_class.as_str(),
            ),
            (
                "rootSession",
                lane.root_virtual_session(),
                root_session.as_str(),
            ),
            (
                "concreteSession",
                lane.concrete_session(),
                item.session_key.as_str(),
            ),
        ];
        if let Some((axis, expected, actual)) = observed
            .into_iter()
            .find(|(_, expected, actual)| expected != actual)
        {
            return Err(format!(
                "execution snapshot {axis} mismatch: expected `{expected}`, queued `{actual}`"
            ));
        }
        if owner.parent_queue_id.as_deref() != Some(admission_queue_id) {
            return Err(
                "channel execution snapshot parent is not bound to admissionQueueId".to_string(),
            );
        }
    }
    Ok(())
}

fn coordinator_resume_metadata_from_value(
    value: &Value,
    queue_id: &str,
) -> Result<Option<crate::coordinator_resume::CoordinatorResumeMetadataV1>, String> {
    let Some(raw) = value
        .get("coordinatorResume")
        .or_else(|| value.get("coordinator_resume"))
    else {
        return Ok(None);
    };
    let metadata =
        serde_json::from_value::<crate::coordinator_resume::CoordinatorResumeMetadataV1>(
            raw.clone(),
        )
        .map_err(|error| format!("invalid coordinator resume metadata: {error}"))?;
    metadata
        .validate()
        .map_err(|error| format!("invalid coordinator resume metadata: {error}"))?;
    if metadata.continuation_queue_id != queue_id {
        return Err(format!(
            "coordinator resume continuationQueueId {} does not match queueId {queue_id}",
            metadata.continuation_queue_id
        ));
    }
    Ok(Some(metadata))
}

fn consume_coordinator_resume_after_lease(
    harness_home: &Path,
    pending: &PendingQueueItem,
    now_ms: i64,
) -> io::Result<()> {
    let Some(metadata) = &pending.coordinator_resume else {
        return Ok(());
    };
    if pending.origin != "coordinator-resume" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "typed coordinator resume metadata requires coordinator-resume origin",
        ));
    }
    metadata.validate().map_err(io::Error::other)?;
    if metadata.continuation_queue_id != pending.queue_id {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "coordinator resume queue identity changed before lease acceptance",
        ));
    }
    let db_file = crate::worker_db_file(harness_home);
    let mut conn = rusqlite::Connection::open(&db_file).map_err(|error| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "could not open worker DB for coordinator resume {}: {error}",
                db_file.display()
            ),
        )
    })?;
    let transaction = conn.transaction().map_err(io::Error::other)?;
    crate::worker_resume::consume_resume_intent_in_transaction(
        &transaction,
        &metadata.owner,
        &metadata.intent_id,
        &metadata.continuation_queue_id,
        now_ms,
    )
    .map_err(io::Error::other)?;
    crate::worker_coordination::mark_worker_coordinator_wait_consumed_in_transaction(
        &transaction,
        &metadata.wait_id,
        &metadata.intent_id,
        now_ms,
    )
    .map_err(io::Error::other)?;
    transaction.commit().map_err(io::Error::other)
}

fn parse_pending_reasoning_snapshot(
    value: &Value,
) -> Result<
    (
        Option<String>,
        Option<String>,
        Option<ReasoningPreference>,
        Option<BackendReasoningPolicyV1>,
    ),
    String,
> {
    let preference_value = value
        .get("reasoningPreference")
        .or_else(|| value.get("reasoning_preference"))
        .filter(|value| !value.is_null());
    let policy_value = value
        .get("backendReasoningPolicy")
        .or_else(|| value.get("backend_reasoning_policy"))
        .filter(|value| !value.is_null());
    if preference_value.is_none() && policy_value.is_none() {
        return match (value.get("provider"), value.get("model")) {
            (None | Some(Value::Null), None | Some(Value::Null)) => Ok((None, None, None, None)),
            (Some(Value::String(provider)), Some(Value::String(model)))
                if !provider.is_empty()
                    && provider.trim() == provider
                    && !model.is_empty()
                    && model.trim() == model =>
            {
                Ok((Some(provider.clone()), Some(model.clone()), None, None))
            }
            _ => Err(
                "provider and model must be absent together or form a canonical complete route"
                    .to_string(),
            ),
        };
    }
    if preference_value.is_some() != policy_value.is_some() {
        return Err("reasoning preference and backend policy must be present together".to_string());
    }
    let provider = match value.get("provider") {
        Some(Value::String(value)) if !value.is_empty() && value.trim() == value => value.clone(),
        _ => {
            return Err("reasoning snapshot requires a canonical non-empty provider".to_string());
        }
    };
    let model = match value.get("model") {
        Some(Value::String(value)) if !value.is_empty() && value.trim() == value => value.clone(),
        _ => {
            return Err("reasoning snapshot requires a canonical non-empty model".to_string());
        }
    };
    let reasoning_preference = preference_value
        .map(|value| serde_json::from_value::<ReasoningPreference>(value.clone()))
        .transpose()
        .map_err(|error| format!("invalid reasoning preference: {error}"))?;
    let backend_reasoning_policy = policy_value
        .map(|value| serde_json::from_value::<BackendReasoningPolicyV1>(value.clone()))
        .transpose()
        .map_err(|error| format!("invalid backend reasoning policy: {error}"))?;
    if let (Some(preference), Some(policy)) = (
        reasoning_preference.as_ref(),
        backend_reasoning_policy.as_ref(),
    ) {
        if let Err(error) = policy.validate_for_route(&provider, &model) {
            return Err(format!("reasoning snapshot route mismatch: {error}"));
        }
        if preference
            .explicit_effort()
            .is_some_and(|effort| !effort.eq_ignore_ascii_case(policy.effective_effort()))
        {
            return Err(
                "explicit reasoning preference does not match backend policy effort".to_string(),
            );
        }
        if preference.validate().is_err()
            || preference
                .explicit_effort()
                .is_some_and(|effort| effort.trim() != effort)
        {
            return Err("reasoning preference is not canonical".to_string());
        }
    }
    Ok((
        Some(provider),
        Some(model),
        reasoning_preference,
        backend_reasoning_policy,
    ))
}

fn revalidate_pending_reasoning_snapshot(
    harness_home: &Path,
    item: &PendingQueueItem,
) -> Result<(), String> {
    let (Some(preference), Some(policy)) = (
        item.reasoning_preference.as_ref(),
        item.backend_reasoning_policy.as_ref(),
    ) else {
        return Ok(());
    };
    let provider = item.provider.as_deref().ok_or("missing queued provider")?;
    let model = item.model.as_deref().ok_or("missing queued model")?;
    policy
        .validate_for_execution_route(provider, model)
        .map_err(|error| format!("queued backend reasoning route mismatch: {error}"))?;
    if crate::model_catalog::model_catalog_rollout_mode_for_agent(
        Some(harness_home),
        Some(&item.agent_id),
    ) != crate::model_catalog::ModelCatalogRolloutMode::Authoritative
    {
        return Err("catalog rollout is not authoritative for the queued agent".into());
    }
    let cache_file = harness_home.join("codex-home").join("models_cache.json");
    let text = fs::read_to_string(&cache_file)
        .map_err(|error| format!("model catalog cache unavailable: {error}"))?;
    let catalog = crate::model_catalog::parse_codex_model_catalog(&text)?;
    let route = catalog
        .exact_route(provider, model)
        .ok_or_else(|| format!("exact route {provider}/{model} is absent from catalog"))?;
    let effort = match preference {
        ReasoningPreference::Default => route
            .default_reasoning_effort
            .as_deref()
            .ok_or("exact route has no default reasoning effort")?,
        ReasoningPreference::Explicit { effort } => effort,
    };
    if effort.eq_ignore_ascii_case("ultra") {
        return Err("ultra requires a matching delegation resource authorization receipt".into());
    }
    let current = crate::model_catalog::resolve_reasoning_effort(
        Some(&catalog),
        crate::model_catalog::ModelCatalogRolloutMode::Authoritative,
        provider,
        model,
        effort,
        crate::model_catalog::UnsupportedReasoningPolicy::Reject,
    );
    if !current.authoritative
        || current.status != crate::model_catalog::ReasoningResolutionStatus::Accepted
        || current.effective_effort.as_deref() != Some(policy.effective_effort())
    {
        return Err("queued reasoning policy is not valid for the current exact route".into());
    }
    Ok(())
}

fn continuation_metadata_from_value(value: &Value) -> RuntimeContinuationMetadata {
    RuntimeContinuationMetadata {
        virtual_session_id: string_field(value, &["virtualSessionId", "virtual_session_id"])
            .map(ToString::to_string),
        continuation_index: value
            .get("continuationIndex")
            .or_else(|| value.get("continuation_index"))
            .and_then(Value::as_u64),
        campaign_slice_generation: value
            .get("campaignSliceGeneration")
            .or_else(|| value.get("campaign_slice_generation"))
            .and_then(Value::as_u64),
        continuation_intent_key: string_field(
            value,
            &["continuationIntentKey", "continuation_intent_key"],
        )
        .map(ToString::to_string),
        completion_kind: string_field(value, &["completionKind", "completion_kind"])
            .map(ToString::to_string),
        task_terminal: value
            .get("taskTerminal")
            .or_else(|| value.get("task_terminal"))
            .and_then(Value::as_bool),
        suppress_self_improvement: value
            .get("suppressSelfImprovement")
            .or_else(|| value.get("suppress_self_improvement"))
            .and_then(Value::as_bool)
            .unwrap_or(false),
    }
}

fn default_runtime_class_for(platform: &str) -> String {
    match platform {
        "native-cron" => "cron".to_string(),
        "worker" | "worker-watchdog" => "worker".to_string(),
        _ => "interactive".to_string(),
    }
}

fn queue_execution_dir(harness_home: &Path, queue_id: &str) -> PathBuf {
    harness_home
        .join("state")
        .join("runtime-queue")
        .join("executions")
        .join(normalize_key_part(queue_id))
}

fn append_json_line(path: &Path, value: &impl Serialize) -> io::Result<()> {
    crate::append_jsonl_value(path, value)
}

fn tombstone_runtime_queue_item_skipped(
    queue_dir: &Path,
    item: &PendingQueueItem,
    reason: &str,
) -> io::Result<()> {
    append_json_line(
        &queue_dir.join("run-once-receipts.jsonl"),
        &RuntimeRunOnceSkipReceipt {
            schema: "agent-harness.runtime-run-once.v1",
            queue_id: Some(item.queue_id.clone()),
            status: "skipped",
            runtime_class: Some(item.runtime_class.clone()),
            origin: Some(item.origin.clone()),
            cron_run_id: item.cron_run_id.clone(),
            scheduled_for_ms: item.scheduled_for_ms,
            execution_dir: None,
            transcript_file: None,
            outbox_file: None,
            reason: reason.to_string(),
        },
    )
}

fn string_field<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a str> {
    for key in keys {
        if let Some(text) = value.get(*key).and_then(Value::as_str) {
            return Some(text);
        }
    }
    None
}

fn path_field(value: &Value, keys: &[&str]) -> Option<PathBuf> {
    string_field(value, keys).map(PathBuf::from)
}

fn i64_field(value: &Value, keys: &[&str]) -> Option<i64> {
    for key in keys {
        if let Some(number) = value.get(*key).and_then(Value::as_i64) {
            return Some(number);
        }
    }
    None
}

fn string_array_field(value: &Value, keys: &[&str]) -> Vec<String> {
    for key in keys {
        if let Some(array) = value.get(*key).and_then(Value::as_array) {
            return array
                .iter()
                .filter_map(Value::as_str)
                .map(ToString::to_string)
                .collect();
        }
    }
    Vec::new()
}

fn inbound_media_artifacts_field(value: &Value, keys: &[&str]) -> Vec<InboundMediaArtifact> {
    for key in keys {
        if let Some(artifacts) = value.get(*key) {
            return serde_json::from_value::<Vec<InboundMediaArtifact>>(artifacts.clone())
                .unwrap_or_default();
        }
    }
    Vec::new()
}

fn normalize_key_part(value: &str) -> String {
    let mut normalized = String::new();
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
            normalized.push(ch.to_ascii_lowercase());
        } else {
            normalized.push('_');
        }
    }
    if normalized.is_empty() {
        "unknown".to_string()
    } else {
        normalized
    }
}

fn maybe_apply_context_rollover_before_turn(
    harness_home: &Path,
    queue_file: &Path,
    pending: PendingQueueItem,
    now_ms: i64,
    warnings: &mut Vec<String>,
) -> io::Result<PendingQueueItem> {
    if pending.runtime_class != "interactive" || !runtime_origin_is_parent_channel(&pending.origin)
    {
        return Ok(pending);
    }

    let receipt = apply_context_rollover_before_turn(ContextRolloverBeforeTurnOptions {
        harness_home: harness_home.to_path_buf(),
        queue_id: pending.queue_id.clone(),
        runtime_class: pending.runtime_class.clone(),
        agent_id: pending.agent_id.clone(),
        platform: pending.platform.clone(),
        channel_id: pending.channel_id.clone(),
        user_id: pending.user_id.clone(),
        working_session_key: pending.session_key.clone(),
        now_ms,
    })?;

    match receipt.status {
        ContextRolloverStatus::Applied => {
            warnings.push(format!(
                "context rollover applied for runtime queue item `{}`: {} -> {}",
                pending.queue_id,
                pending.session_key,
                receipt
                    .new_working_session_key
                    .as_deref()
                    .unwrap_or("(unknown)")
            ));
            let mut reload_warnings = Vec::new();
            let items = read_pending_items(queue_file, &mut reload_warnings)?;
            warnings.extend(
                reload_warnings
                    .into_iter()
                    .map(|warning| format!("after context rollover: {warning}")),
            );
            if let Some(updated) = items
                .into_iter()
                .find(|item| item.queue_id == pending.queue_id)
            {
                Ok(updated)
            } else {
                warnings.push(format!(
                    "context rollover applied for `{}` but the rewritten pending item was not found; using original queue metadata",
                    pending.queue_id
                ));
                Ok(pending)
            }
        }
        ContextRolloverStatus::Disabled | ContextRolloverStatus::NotPending => Ok(pending),
        ContextRolloverStatus::BlockedPrepared | ContextRolloverStatus::BlockedLeased => {
            Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                format!(
                    "context rollover guard stopped runtime queue item `{}`: {:?}: {}",
                    pending.queue_id, receipt.status, receipt.reason
                ),
            ))
        }
        status => {
            warnings.push(format!(
                "context rollover was not applied for runtime queue item `{}`: {:?}: {}",
                pending.queue_id, status, receipt.reason
            ));
            Ok(pending)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ArtifactExtractionSummary, ChannelSessionState, ContextCompactAttemptOptions,
        ContextRolloverLane, InboundMediaArtifact, InboundMediaDownloadStatus,
        InboundMediaModelAttachmentStatus, InboundMediaSelectedVariant, RuntimeQueueControlAction,
        RuntimeQueueControlOptions, RuntimeQueueEnqueueOptions, ScopedStopOptions,
        ScopedStopTarget, TurnPlanInput, build_channel_step, build_source_skill_index,
        build_turn_plan, channel_session_state_file, control_runtime_queue_item,
        enqueue_channel_step, inbound_media_attachment_root, read_channel_session_state,
        record_context_compact_attempt, record_scoped_stop,
    };
    use std::io::Write;
    use std::sync::mpsc;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    #[test]
    fn refresh_runtime_queue_state_index_reuses_persisted_cursor_across_restart_and_observes_appends()
     {
        let root = temp_root(
            "refresh_runtime_queue_state_index_reuses_persisted_cursor_across_restart_and_observes_appends",
        );
        let harness_home = root.join(".agent-harness");
        let queue_dir = queue_dir(&harness_home);
        fs::create_dir_all(&queue_dir).unwrap();
        let receipts_file = queue_dir.join("run-once-receipts.jsonl");
        append_json_line(
            &receipts_file,
            &serde_json::json!({
                "queueId": "turn:historical",
                "status": "completed",
                "reason": "historical terminal receipt"
            }),
        )
        .unwrap();

        let initial = rebuild_runtime_queue_state_index(&queue_dir, &mut Vec::new()).unwrap();
        assert!(initial.queues["turn:historical"].terminal_ever);
        let index_file = queue_dir.join("queue-state-index.json");
        let index_modified_before_restart = fs::metadata(&index_file).unwrap().modified().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(1_100));

        // This call intentionally reconstructs no in-memory state. It must use
        // the persisted cursor and avoid replaying or rewriting an unchanged
        // historical receipt ledger.
        let restarted = refresh_runtime_queue_state_index(&queue_dir, &mut Vec::new()).unwrap();
        assert!(restarted.queues["turn:historical"].terminal_ever);
        assert_eq!(
            fs::metadata(&index_file).unwrap().modified().unwrap(),
            index_modified_before_restart,
            "an unchanged receipt ledger must not rewrite the persisted index"
        );

        append_json_line(
            &receipts_file,
            &serde_json::json!({
                "queueId": "turn:appended",
                "status": "completed",
                "reason": "appended terminal receipt"
            }),
        )
        .unwrap();
        let appended = refresh_runtime_queue_state_index(&queue_dir, &mut Vec::new()).unwrap();
        assert!(appended.queues["turn:historical"].terminal_ever);
        assert!(appended.queues["turn:appended"].terminal_ever);
        assert_eq!(
            appended.receipt_ledger.offset_bytes,
            fs::metadata(&receipts_file).unwrap().len()
        );

        // Terminal admission is sticky even if the latest receipt changes to a
        // retryable state, while lease retry handling follows the latest state.
        append_json_line(
            &receipts_file,
            &serde_json::json!({
                "queueId": "turn:historical",
                "status": "retry-pending",
                "reason": "transient retry"
            }),
        )
        .unwrap();
        let retry_pending = refresh_runtime_queue_state_index(&queue_dir, &mut Vec::new()).unwrap();
        assert!(terminal_run_once_ids_from_index(&retry_pending).contains("turn:historical"));
        assert!(retry_pending_run_once_ids_from_index(&retry_pending).contains("turn:historical"));

        append_json_line(
            &receipts_file,
            &serde_json::json!({
                "queueId": "turn:historical",
                "status": "timeout",
                "reason": "terminal after retry"
            }),
        )
        .unwrap();
        let terminal_after_retry =
            refresh_runtime_queue_state_index(&queue_dir, &mut Vec::new()).unwrap();
        assert!(
            terminal_run_once_ids_from_index(&terminal_after_retry).contains("turn:historical")
        );
        assert!(
            !retry_pending_run_once_ids_from_index(&terminal_after_retry)
                .contains("turn:historical")
        );

        let restarted_after_append =
            refresh_runtime_queue_state_index(&queue_dir, &mut Vec::new()).unwrap();
        assert!(restarted_after_append.queues["turn:appended"].terminal_ever);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn refresh_runtime_queue_state_index_recovers_from_receipt_truncate_and_index_corruption() {
        let root = temp_root(
            "refresh_runtime_queue_state_index_recovers_from_receipt_truncate_and_index_corruption",
        );
        let harness_home = root.join(".agent-harness");
        let queue_dir = queue_dir(&harness_home);
        fs::create_dir_all(&queue_dir).unwrap();
        let receipts_file = queue_dir.join("run-once-receipts.jsonl");
        append_json_line(
            &receipts_file,
            &serde_json::json!({
                "queueId": "turn:old-terminal",
                "status": "completed",
                "reason": "a deliberately long historical terminal receipt for truncate detection"
            }),
        )
        .unwrap();
        let _ = rebuild_runtime_queue_state_index(&queue_dir, &mut Vec::new()).unwrap();

        // A malformed complete record is consumed once with a warning, rather
        // than forcing every progress wake to retry the same corruption.
        fs::OpenOptions::new()
            .append(true)
            .open(&receipts_file)
            .unwrap()
            .write_all(b"{not-json}\n")
            .unwrap();
        let mut corruption_warnings = Vec::new();
        let after_corruption =
            refresh_runtime_queue_state_index(&queue_dir, &mut corruption_warnings).unwrap();
        assert!(after_corruption.queues["turn:old-terminal"].terminal_ever);
        assert!(corruption_warnings.iter().any(|warning| {
            warning.contains("runtime run-once receipt line")
                && warning.contains("not valid JSON while refreshing")
        }));
        let mut no_repeat_warnings = Vec::new();
        let _ = refresh_runtime_queue_state_index(&queue_dir, &mut no_repeat_warnings).unwrap();
        assert!(
            !no_repeat_warnings
                .iter()
                .any(|warning| warning.contains("not valid JSON while refreshing"))
        );

        // Simulate a truncate/rotation replacement with a shorter current
        // ledger. The old terminal control must be discarded, not leaked.
        fs::write(
            &receipts_file,
            b"{\"queueId\":\"turn:new\",\"status\":\"queued\"}\n",
        )
        .unwrap();
        let mut truncate_warnings = Vec::new();
        let truncated =
            refresh_runtime_queue_state_index(&queue_dir, &mut truncate_warnings).unwrap();
        assert!(!truncated.queues.contains_key("turn:old-terminal"));
        assert!(!truncated.queues["turn:new"].terminal_ever);
        assert!(
            truncate_warnings
                .iter()
                .any(|warning| warning.contains("receipt ledger was truncated"))
        );

        // If the materialized cursor/index itself is corrupt, rebuild from the
        // current ledger and preserve the current terminal semantics.
        fs::write(queue_dir.join("queue-state-index.json"), b"{not-json").unwrap();
        append_json_line(
            &receipts_file,
            &serde_json::json!({
                "queueId": "turn:new",
                "status": "completed",
                "reason": "current terminal receipt after index corruption"
            }),
        )
        .unwrap();
        let mut index_warnings = Vec::new();
        let repaired = refresh_runtime_queue_state_index(&queue_dir, &mut index_warnings).unwrap();
        assert!(repaired.queues["turn:new"].terminal_ever);
        assert!(
            index_warnings
                .iter()
                .any(|warning| warning.contains("queue-state index could not be read"))
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn refresh_runtime_queue_state_index_rebuilds_legacy_revision_zero_from_receipt_ledger() {
        let root = temp_root(
            "refresh_runtime_queue_state_index_rebuilds_legacy_revision_zero_from_receipt_ledger",
        );
        let harness_home = root.join(".agent-harness");
        let queue_dir = queue_dir(&harness_home);
        fs::create_dir_all(&queue_dir).unwrap();
        let receipts_file = queue_dir.join("run-once-receipts.jsonl");
        append_json_line(
            &receipts_file,
            &serde_json::json!({
                "schema": "agent-harness.runtime-run-once.v1",
                "queueId": "turn:ledger-terminal",
                "status": "completed",
                "reason": "authoritative terminal receipt"
            }),
        )
        .unwrap();

        // A pre-revision state file is a derived cache, not lifecycle evidence.
        // It may contain an old terminal entry that no longer appears in the
        // authoritative receipt ledger, so upgrade must rebuild rather than
        // retain that stale terminal control.
        fs::write(
            queue_dir.join("queue-state-index.json"),
            serde_json::to_vec(&serde_json::json!({
                "schema": RUNTIME_QUEUE_STATE_INDEX_SCHEMA,
                "queues": {
                    "turn:stale-index-only": {
                        "terminalEver": true,
                        "terminalRunOnceEver": true,
                        "terminalStatus": "completed"
                    }
                }
            }))
            .unwrap(),
        )
        .unwrap();

        let mut warnings = Vec::new();
        let rebuilt = refresh_runtime_queue_state_index(&queue_dir, &mut warnings).unwrap();

        assert_eq!(rebuilt.revision, RUNTIME_QUEUE_STATE_INDEX_REVISION);
        assert!(rebuilt.queues["turn:ledger-terminal"].terminal_ever);
        assert!(!rebuilt.queues.contains_key("turn:stale-index-only"));
        assert!(warnings.iter().any(|warning| {
            warning.contains("unsupported schema/revision") && warning.contains("/0")
        }));

        let persisted: serde_json::Value =
            serde_json::from_slice(&fs::read(queue_dir.join("queue-state-index.json")).unwrap())
                .unwrap();
        assert_eq!(
            persisted
                .get("revision")
                .and_then(serde_json::Value::as_u64),
            Some(RUNTIME_QUEUE_STATE_INDEX_REVISION.into())
        );
        assert!(persisted.get("receiptLedger").is_some());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn runtime_queue_receipt_compaction_bounds_archives_and_preserves_terminal_and_retry_state() {
        let root = temp_root(
            "runtime_queue_receipt_compaction_bounds_archives_and_preserves_terminal_and_retry_state",
        );
        let harness_home = root.join(".agent-harness");
        let queue_dir = queue_dir(&harness_home);
        fs::create_dir_all(&queue_dir).unwrap();
        let receipts_file = queue_dir.join("run-once-receipts.jsonl");

        for (queue_id, status, reason) in [
            (
                "turn:terminal",
                "completed",
                "terminal state must survive compaction",
            ),
            (
                "turn:suppressed",
                "suppressed",
                "suppression must survive compaction",
            ),
            (
                "turn:retry",
                "retry-pending",
                "latest retry state must survive compaction",
            ),
        ] {
            append_json_line(
                &receipts_file,
                &serde_json::json!({
                    "queueId": queue_id,
                    "status": status,
                    "reason": reason,
                    "terminalControlSource": "run-once-terminal",
                    "retrySchedule": (status == "retry-pending").then(|| serde_json::json!({
                        "lineageId": "runtime-retry:turn:retry",
                        "attempt": 1,
                        "maxAttempts": 3,
                        "delayMs": 1000,
                        "scheduledAtMs": 1000,
                        "nextEligibleAtMs": 2000,
                        "replayMode": "same-request-no-observable-mutation"
                    })),
                    "padding": "receipt-compaction-regression-padding".repeat(32)
                }),
            )
            .unwrap();
        }
        let pending_file = queue_dir.join("pending.jsonl");
        fs::write(
            &pending_file,
            [
                queued_item_value("turn:terminal").to_string(),
                queued_item_value("turn:suppressed").to_string(),
                queued_item_value("turn:retry").to_string(),
            ]
            .join("\n"),
        )
        .unwrap();
        let _ = rebuild_runtime_queue_state_index(&queue_dir, &mut Vec::new()).unwrap();

        let first =
            compact_runtime_queue_receipts_if_needed(RuntimeQueueReceiptCompactionOptions {
                harness_home: harness_home.clone(),
                max_bytes: 1,
                max_archives: 1,
                now_ms: 10,
            })
            .unwrap();
        assert_eq!(first.status, RuntimeQueueReceiptCompactionStatus::Compacted);
        assert!(
            first
                .archive_file
                .as_ref()
                .is_some_and(|path| path.is_file())
        );
        assert!(
            fs::metadata(&receipts_file).unwrap().len() < first.original_bytes,
            "the active journal must be a bounded snapshot rather than the old full receipt stream"
        );

        // A lost index must rebuild from the compacted active journals without
        // reviving terminal work. The terminal receipts are retained in the
        // bounded archive; only still-queued retry work remains active.
        fs::remove_file(queue_dir.join("queue-state-index.json")).unwrap();
        let rebuilt = refresh_runtime_queue_state_index(&queue_dir, &mut Vec::new()).unwrap();
        assert!(!rebuilt.queues.contains_key("turn:terminal"));
        assert!(!rebuilt.queues.contains_key("turn:suppressed"));
        assert!(retry_pending_run_once_ids_from_index(&rebuilt).contains("turn:retry"));
        assert_eq!(
            rebuilt.queues["turn:retry"]
                .retry_schedule
                .as_ref()
                .map(|schedule| schedule.next_eligible_at_ms),
            Some(2_000)
        );
        let compacted_pending = fs::read_to_string(&pending_file).unwrap();
        assert!(compacted_pending.contains("turn:retry"));
        assert!(!compacted_pending.contains("turn:terminal"));
        assert!(!compacted_pending.contains("turn:suppressed"));
        let archived_receipts = fs::read_to_string(first.archive_file.as_ref().unwrap()).unwrap();
        assert!(archived_receipts.contains("turn:terminal"));
        assert!(archived_receipts.contains("turn:suppressed"));

        append_json_line(
            &receipts_file,
            &serde_json::json!({
                "queueId": "turn:next",
                "status": "completed",
                "reason": "second archive bounds the first",
                "padding": "receipt-compaction-regression-padding".repeat(32)
            }),
        )
        .unwrap();
        let second =
            compact_runtime_queue_receipts_if_needed(RuntimeQueueReceiptCompactionOptions {
                harness_home: harness_home.clone(),
                max_bytes: 1,
                max_archives: 1,
                now_ms: 11,
            })
            .unwrap();
        assert_eq!(
            second.status,
            RuntimeQueueReceiptCompactionStatus::Compacted
        );
        assert_eq!(
            fs::read_dir(queue_dir.join("run-once-receipts-archive"))
                .unwrap()
                .count(),
            1,
            "archive retention must remain bounded"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn runtime_queue_receipt_compaction_recovers_interrupted_snapshot_with_full_digest_check() {
        let root = temp_root(
            "runtime_queue_receipt_compaction_recovers_interrupted_snapshot_with_full_digest_check",
        );
        let harness_home = root.join(".agent-harness");
        let queue_dir = queue_dir(&harness_home);
        fs::create_dir_all(&queue_dir).unwrap();
        let receipts_file = queue_dir.join("run-once-receipts.jsonl");
        let archive_dir = queue_dir.join("run-once-receipts-archive");
        fs::create_dir_all(&archive_dir).unwrap();

        let archived = serde_json::json!({
            "queueId": "turn:terminal",
            "status": "completed",
            "reason": "historical terminal record"
        })
        .to_string()
            + "\n";
        let snapshot = serde_json::json!({
            "queueId": "turn:retry",
            "status": "retry-pending",
            "reason": "active retry record",
            "compacted": true
        })
        .to_string()
            + "\n";
        let archive_file = archive_dir.join("run-once-receipts-10.jsonl");
        fs::write(&archive_file, &archived).unwrap();
        let compact_temp = runtime_queue_ledger_temp_file(&receipts_file, 11, "compact");
        fs::write(&compact_temp, &snapshot).unwrap();
        let expected_digest = runtime_queue_ledger_digest(&compact_temp).unwrap();

        // This is the crash window after the destination was replaced but
        // before the marker was removed. It has the same length and tail as
        // the intended snapshot, so recovery must use the full digest rather
        // than accepting a prefix-corrupted destination.
        let mut corrupted = snapshot.as_bytes().to_vec();
        corrupted[0] = b'[';
        fs::write(&receipts_file, corrupted).unwrap();
        let marker = RuntimeQueueLedgerCompactionPending {
            schema: RUNTIME_QUEUE_RECEIPT_COMPACTION_PENDING_SCHEMA.to_string(),
            ledger_file: receipts_file.clone(),
            archive_file: archive_file.clone(),
            temp_file: compact_temp,
            expected_bytes: snapshot.len() as u64,
            expected_digest,
            archive_expected_bytes: Some(archived.len() as u64),
            archive_expected_digest: Some(runtime_queue_ledger_digest(&archive_file).unwrap()),
            history_store_file: None,
            history_transaction_id: None,
        };
        write_json_atomic(
            &runtime_queue_ledger_compaction_marker_file(&receipts_file),
            &marker,
        )
        .unwrap();

        let mut warnings = Vec::new();
        let index = refresh_runtime_queue_state_index(&queue_dir, &mut warnings).unwrap();
        assert_eq!(fs::read_to_string(&receipts_file).unwrap(), snapshot);
        assert!(retry_pending_run_once_ids_from_index(&index).contains("turn:retry"));
        assert!(!index.queues.contains_key("turn:terminal"));
        assert!(warnings.iter().any(|warning| {
            warning.contains("recovered interrupted runtime queue ledger compaction")
        }));
        assert!(!runtime_queue_ledger_compaction_marker_file(&receipts_file).exists());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn receipt_compaction_recovery_commits_staged_history_after_snapshot_is_durable() {
        let root = temp_root(
            "receipt_compaction_recovery_commits_staged_history_after_snapshot_is_durable",
        );
        let harness_home = root.join(".agent-harness");
        let queue_dir = queue_dir(&harness_home);
        fs::create_dir_all(&queue_dir).unwrap();
        let receipts_file = queue_dir.join("run-once-receipts.jsonl");
        let archive_dir = queue_dir.join("run-once-receipts-archive");
        fs::create_dir_all(&archive_dir).unwrap();

        let archived = b"{\"queueId\":\"turn:historical\",\"traceId\":\"trace:historical\",\"status\":\"completed\",\"reason\":\"durable history recovery\"}\n{\"queueId\":\"turn:retry\",\"status\":\"retry-pending\"}\n";
        let snapshot = b"{\"queueId\":\"turn:retry\",\"status\":\"retry-pending\"}\n";
        let archive_file = archive_dir.join("run-once-receipts-history-recovery.jsonl");
        fs::write(&archive_file, archived).unwrap();
        fs::write(&receipts_file, snapshot).unwrap();
        let staging = crate::runtime_receipt_history::stage_runtime_queue_receipt_history(
            &queue_dir,
            "history-recovery-commit",
            archived,
            snapshot,
            &std::collections::HashSet::from(["turn:retry".to_string()]),
            100,
        )
        .unwrap();
        let temp_file = runtime_queue_ledger_temp_file(&receipts_file, 101, "compact");
        fs::write(&temp_file, snapshot).unwrap();
        let marker = RuntimeQueueLedgerCompactionPending {
            schema: RUNTIME_QUEUE_RECEIPT_COMPACTION_PENDING_SCHEMA.to_string(),
            ledger_file: receipts_file.clone(),
            archive_file: archive_file.clone(),
            temp_file,
            expected_bytes: snapshot.len() as u64,
            expected_digest: runtime_queue_ledger_digest(&receipts_file).unwrap(),
            archive_expected_bytes: Some(archived.len() as u64),
            archive_expected_digest: Some(runtime_queue_ledger_digest(&archive_file).unwrap()),
            history_store_file: Some(staging.store_file.clone()),
            history_transaction_id: Some(staging.transaction_id.clone()),
        };
        let marker_file = runtime_queue_ledger_compaction_marker_file(&receipts_file);
        write_json_atomic(&marker_file, &marker).unwrap();

        recover_runtime_queue_ledger_compaction_if_needed(&receipts_file, &mut Vec::new()).unwrap();

        let history = crate::runtime_receipt_history::find_runtime_queue_terminal_history(
            &queue_dir,
            &std::collections::BTreeSet::from(["trace:historical".to_string()]),
        )
        .unwrap();
        assert_eq!(
            history.len(),
            1,
            "the durable hot snapshot must expose staged history"
        );
        assert_eq!(history[0].queue_id, "turn:historical");
        assert!(!marker_file.exists());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn receipt_compaction_recovery_discards_staged_history_when_archive_is_restored() {
        let root = temp_root(
            "receipt_compaction_recovery_discards_staged_history_when_archive_is_restored",
        );
        let harness_home = root.join(".agent-harness");
        let queue_dir = queue_dir(&harness_home);
        fs::create_dir_all(&queue_dir).unwrap();
        let receipts_file = queue_dir.join("run-once-receipts.jsonl");
        let archive_dir = queue_dir.join("run-once-receipts-archive");
        fs::create_dir_all(&archive_dir).unwrap();

        let archived = b"{\"queueId\":\"turn:historical\",\"traceId\":\"trace:historical\",\"status\":\"completed\"}\n{\"queueId\":\"turn:retry\",\"status\":\"retry-pending\"}\n";
        let snapshot = b"{\"queueId\":\"turn:retry\",\"status\":\"retry-pending\"}\n";
        let archive_file = archive_dir.join("run-once-receipts-history-restore.jsonl");
        fs::write(&archive_file, archived).unwrap();
        let staging = crate::runtime_receipt_history::stage_runtime_queue_receipt_history(
            &queue_dir,
            "history-recovery-discard",
            archived,
            snapshot,
            &std::collections::HashSet::from(["turn:retry".to_string()]),
            100,
        )
        .unwrap();
        let temp_file = runtime_queue_ledger_temp_file(&receipts_file, 101, "compact");
        fs::write(&temp_file, b"broken snapshot\n").unwrap();
        fs::write(&receipts_file, b"broken destination\n").unwrap();
        let marker = RuntimeQueueLedgerCompactionPending {
            schema: RUNTIME_QUEUE_RECEIPT_COMPACTION_PENDING_SCHEMA.to_string(),
            ledger_file: receipts_file.clone(),
            archive_file: archive_file.clone(),
            temp_file,
            expected_bytes: snapshot.len() as u64,
            expected_digest: runtime_queue_bytes_digest(snapshot),
            archive_expected_bytes: Some(archived.len() as u64),
            archive_expected_digest: Some(runtime_queue_ledger_digest(&archive_file).unwrap()),
            history_store_file: Some(staging.store_file.clone()),
            history_transaction_id: Some(staging.transaction_id.clone()),
        };
        let marker_file = runtime_queue_ledger_compaction_marker_file(&receipts_file);
        write_json_atomic(&marker_file, &marker).unwrap();

        recover_runtime_queue_ledger_compaction_if_needed(&receipts_file, &mut Vec::new()).unwrap();

        assert_eq!(fs::read(&receipts_file).unwrap(), archived);
        assert!(
            crate::runtime_receipt_history::find_runtime_queue_terminal_history(
                &queue_dir,
                &std::collections::BTreeSet::from(["trace:historical".to_string()]),
            )
            .unwrap()
            .is_empty(),
            "restoring the pre-compaction hot ledger must keep staged history invisible"
        );
        assert!(!marker_file.exists());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn runtime_queue_receipt_compaction_recovery_accepts_legacy_v2_marker_with_valid_jsonl_archive()
    {
        let root = temp_root(
            "runtime_queue_receipt_compaction_recovery_accepts_legacy_v2_marker_with_valid_jsonl_archive",
        );
        let harness_home = root.join(".agent-harness");
        let queue_dir = queue_dir(&harness_home);
        fs::create_dir_all(&queue_dir).unwrap();
        let receipts_file = queue_dir.join("run-once-receipts.jsonl");
        let archive_dir = queue_dir.join("run-once-receipts-archive");
        fs::create_dir_all(&archive_dir).unwrap();
        let archive_file = archive_dir.join("run-once-receipts-legacy.jsonl");
        let archive = b"{\"queueId\":\"turn:legacy\",\"status\":\"completed\"}\n";
        fs::write(&archive_file, archive).unwrap();
        fs::write(&receipts_file, b"corrupted destination\n").unwrap();
        let compact_temp = runtime_queue_ledger_temp_file(&receipts_file, 11, "compact");
        fs::write(&compact_temp, b"corrupted snapshot\n").unwrap();
        let marker = RuntimeQueueLedgerCompactionPending {
            schema: RUNTIME_QUEUE_RECEIPT_COMPACTION_PENDING_SCHEMA_V2.to_string(),
            ledger_file: receipts_file.clone(),
            archive_file,
            temp_file: compact_temp,
            expected_bytes: 1,
            expected_digest: 1,
            archive_expected_bytes: None,
            archive_expected_digest: None,
            history_store_file: None,
            history_transaction_id: None,
        };
        let marker_file = runtime_queue_ledger_compaction_marker_file(&receipts_file);
        write_json_atomic(&marker_file, &marker).unwrap();

        let mut warnings = Vec::new();
        recover_runtime_queue_ledger_compaction_if_needed(&receipts_file, &mut warnings).unwrap();

        assert_eq!(fs::read(&receipts_file).unwrap(), archive);
        assert!(!marker_file.exists());
        assert!(
            warnings
                .iter()
                .any(|warning| warning.contains("accepted legacy v2"))
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn refresh_runtime_queue_state_index_detects_same_size_prefix_rewrite() {
        let root = temp_root("refresh_runtime_queue_state_index_detects_same_size_prefix_rewrite");
        let harness_home = root.join(".agent-harness");
        let queue_dir = queue_dir(&harness_home);
        fs::create_dir_all(&queue_dir).unwrap();
        let receipts_file = queue_dir.join("run-once-receipts.jsonl");
        let first = serde_json::json!({
            "queueId": "turn:first",
            "status": "completed",
            "reason": "same-size rewrite regression"
        })
        .to_string()
            + "\n";
        let second = serde_json::json!({
            "queueId": "turn:other",
            "status": "completed",
            "reason": "same-size rewrite regression"
        })
        .to_string()
            + "\n";
        assert_eq!(first.len(), second.len());
        fs::write(&receipts_file, &first).unwrap();
        let mut original = rebuild_runtime_queue_state_index(&queue_dir, &mut Vec::new()).unwrap();

        fs::write(&receipts_file, &second).unwrap();
        // Simulate a filesystem with coarse timestamps by preserving the new
        // timestamp in the persisted cursor. The tail fingerprint must still
        // force a rebuild instead of retaining the old terminal state.
        original.receipt_ledger.source_modified_at_unix_nanos =
            file_modified_at_unix_nanos(&fs::metadata(&receipts_file).unwrap());
        write_runtime_queue_state_index(&queue_dir, &original).unwrap();

        let mut warnings = Vec::new();
        let refreshed = refresh_runtime_queue_state_index(&queue_dir, &mut warnings).unwrap();
        assert!(!refreshed.queues.contains_key("turn:first"));
        assert!(refreshed.queues["turn:other"].terminal_run_once_ever);
        assert!(
            warnings
                .iter()
                .any(|warning| warning.contains("changed without an append"))
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn queue_state_index_materializes_terminal_metadata_and_per_queue_status_counts() {
        let root = temp_root(
            "queue_state_index_materializes_terminal_metadata_and_per_queue_status_counts",
        );
        let harness_home = root.join(".agent-harness");
        let queue_dir = queue_dir(&harness_home);
        fs::create_dir_all(&queue_dir).unwrap();
        fs::write(
            queue_dir.join("run-once-receipts.jsonl"),
            [
                serde_json::json!({
                    "queueId":"turn:indexed",
                    "status":"failed-retryable"
                })
                .to_string(),
                serde_json::json!({
                    "queueId":"turn:indexed",
                    "status":"no-prepared-execution"
                })
                .to_string(),
                serde_json::json!({
                    "queueId":"turn:indexed",
                    "status":"completed",
                    "reason":"terminal metadata must survive cursor refresh",
                    "runtimeClass":"worker",
                    "origin":"worker",
                    "transcriptFile":"worker-transcript.jsonl",
                    "completedAtMs":42
                })
                .to_string(),
                serde_json::json!({
                    "queueId":"turn:other",
                    "status":"no-prepared-execution"
                })
                .to_string(),
            ]
            .join("\n"),
        )
        .unwrap();

        let index = rebuild_runtime_queue_state_index(&queue_dir, &mut Vec::new()).unwrap();
        assert_eq!(runtime_queue_terminal_receipt_count_from_index(&index), 1);
        let value = serde_json::to_value(index).unwrap();
        let entry = &value["queues"]["turn:indexed"];
        assert_eq!(entry["runOnceStatusCounts"]["failed-retryable"], 1);
        assert_eq!(entry["runOnceStatusCounts"]["no-prepared-execution"], 1);
        assert_eq!(entry["terminalRuntimeClass"], "worker");
        assert_eq!(entry["terminalOrigin"], "worker");
        assert_eq!(entry["terminalTranscriptFile"], "worker-transcript.jsonl");
        assert_eq!(entry["terminalOccurredAtMs"], 42);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn terminal_queue_lookup_joins_hot_index_with_exact_cold_history() {
        let root = temp_root("terminal_queue_lookup_joins_hot_index_with_exact_cold_history");
        let harness_home = root.join(".agent-harness");
        let queue_dir = queue_dir(&harness_home);
        fs::create_dir_all(&queue_dir).unwrap();
        fs::write(
            queue_dir.join("run-once-receipts.jsonl"),
            r#"{"queueId":"queue-hot","status":"completed"}
"#,
        )
        .unwrap();
        rebuild_runtime_queue_state_index(&queue_dir, &mut Vec::new()).unwrap();
        let staged = stage_runtime_queue_receipt_history(
            &queue_dir,
            "terminal-lookup-cold-history",
            br#"{"queueId":"queue-cold","status":"failed-terminal"}
"#,
            b"",
            &HashSet::new(),
            100,
        )
        .unwrap();
        commit_runtime_queue_receipt_history(&staged, 101).unwrap();
        let requested = ["queue-hot", "queue-cold", "queue-open"]
            .into_iter()
            .map(ToString::to_string)
            .collect::<BTreeSet<_>>();

        let terminals = resolve_runtime_queue_terminal_ids(&harness_home, &requested).unwrap();

        assert!(terminals.contains("queue-hot"));
        assert!(terminals.contains("queue-cold"));
        assert!(!terminals.contains("queue-open"));
        assert!(matches!(
            resolve_queue_terminal_control(&harness_home, "queue-cold", None).unwrap(),
            QueueTerminalControl::Terminal(QueueTerminalControlMatch {
                source: TerminalControlSource::RunOnceTerminal,
                ..
            })
        ));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn runtime_queue_receipt_compaction_cadence_defers_only_unchanged_large_active_ledgers() {
        let state = RuntimeQueueReceiptCompactionState {
            schema: RUNTIME_QUEUE_RECEIPT_COMPACTION_STATE_SCHEMA.to_string(),
            last_attempt_at_ms: 10_000,
            last_attempt_bytes: 20 * 1024 * 1024,
        };
        assert!(runtime_queue_receipt_compaction_retry_is_deferred(
            10_001,
            state.last_attempt_bytes + 512 * 1024,
            Some(&state)
        ));
        assert!(!runtime_queue_receipt_compaction_retry_is_deferred(
            10_001,
            state.last_attempt_bytes + RUNTIME_QUEUE_RECEIPT_COMPACTION_RETRY_GROWTH_BYTES,
            Some(&state)
        ));
        assert!(!runtime_queue_receipt_compaction_retry_is_deferred(
            state.last_attempt_at_ms + RUNTIME_QUEUE_RECEIPT_COMPACTION_RETRY_INTERVAL_MS,
            state.last_attempt_bytes,
            Some(&state)
        ));
    }

    #[test]
    fn runtime_queue_receipt_compaction_returns_busy_without_touching_ledgers_when_transaction_is_owned()
     {
        let root = temp_root(
            "runtime_queue_receipt_compaction_returns_busy_without_touching_ledgers_when_transaction_is_owned",
        );
        let harness_home = root.join(".agent-harness");
        let queue_dir = queue_dir(&harness_home);
        fs::create_dir_all(&queue_dir).unwrap();
        let receipts_file = queue_dir.join("run-once-receipts.jsonl");
        append_json_line(
            &receipts_file,
            &serde_json::json!({
                "queueId": "turn:transaction-owner",
                "status": "completed",
                "padding": "transaction-gate-regression-padding".repeat(32)
            }),
        )
        .unwrap();
        let original = fs::read(&receipts_file).unwrap();
        let transaction_lock = runtime_queue_receipt_compaction_lock_file(&queue_dir);

        crate::logging::with_jsonl_append_lock(&transaction_lock, || {
            let report =
                compact_runtime_queue_receipts_if_needed(RuntimeQueueReceiptCompactionOptions {
                    harness_home: harness_home.clone(),
                    max_bytes: 1,
                    max_archives: 1,
                    now_ms: 10,
                })
                .unwrap();
            assert_eq!(
                report.status,
                RuntimeQueueReceiptCompactionStatus::Busy,
                "a second compactor must not enter the cross-ledger transaction"
            );
            assert_eq!(
                fs::read(&receipts_file).unwrap(),
                original,
                "a busy compactor must not mutate the ledger owned by another transaction"
            );
            Ok(())
        })
        .unwrap();

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn automatic_receipt_compaction_busy_does_not_defer_the_next_terminal_retry() {
        let root =
            temp_root("automatic_receipt_compaction_busy_does_not_defer_the_next_terminal_retry");
        let harness_home = root.join(".agent-harness");
        let queue_dir = queue_dir(&harness_home);
        fs::create_dir_all(&queue_dir).unwrap();
        let receipts_file = queue_dir.join("run-once-receipts.jsonl");
        fs::write(
            &receipts_file,
            vec![b'x'; RUNTIME_QUEUE_RECEIPT_COMPACTION_DEFAULT_MAX_BYTES as usize + 1],
        )
        .unwrap();
        let transaction_lock = runtime_queue_receipt_compaction_lock_file(&queue_dir);

        crate::logging::with_jsonl_append_lock(&transaction_lock, || {
            let report =
                maybe_compact_runtime_queue_receipts_after_terminal(harness_home.clone(), 10_000)?
                    .expect("an oversized ledger must attempt compaction");
            assert_eq!(report.status, RuntimeQueueReceiptCompactionStatus::Busy);
            assert!(
                !runtime_queue_receipt_compaction_state_file(&queue_dir).exists(),
                "lock contention must not consume the automatic retry cadence"
            );
            Ok(())
        })
        .unwrap();

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn progress_refresh_returns_cached_index_while_live_compaction_owns_receipt_lock() {
        let root = temp_root(
            "progress_refresh_returns_cached_index_while_live_compaction_owns_receipt_lock",
        );
        let harness_home = root.join(".agent-harness");
        let queue_dir = queue_dir(&harness_home);
        fs::create_dir_all(&queue_dir).unwrap();
        let receipts_file = queue_dir.join("run-once-receipts.jsonl");
        append_json_line(
            &receipts_file,
            &serde_json::json!({
                "queueId": "turn:cached-terminal",
                "status": "completed",
                "reason": "the materialized index must remain available to progress"
            }),
        )
        .unwrap();
        let cached = rebuild_runtime_queue_state_index(&queue_dir, &mut Vec::new()).unwrap();
        assert!(cached.queues["turn:cached-terminal"].terminal_ever);

        let archive_dir = queue_dir.join("run-once-receipts-archive");
        fs::create_dir_all(&archive_dir).unwrap();
        let archive_file = archive_dir.join("run-once-receipts-10.jsonl");
        let archive_contents = fs::read(&receipts_file).unwrap();
        fs::write(&archive_file, &archive_contents).unwrap();
        let compact_temp = runtime_queue_ledger_temp_file(&receipts_file, 11, "compact");
        let compact_snapshot = b"{\"queueId\":\"turn:active\",\"status\":\"retry-pending\"}\n";
        fs::write(&compact_temp, compact_snapshot).unwrap();
        let marker = RuntimeQueueLedgerCompactionPending {
            schema: RUNTIME_QUEUE_RECEIPT_COMPACTION_PENDING_SCHEMA.to_string(),
            ledger_file: receipts_file.clone(),
            archive_file: archive_file.clone(),
            temp_file: compact_temp.clone(),
            expected_bytes: compact_snapshot.len() as u64,
            expected_digest: runtime_queue_ledger_digest(&compact_temp).unwrap(),
            archive_expected_bytes: Some(archive_contents.len() as u64),
            archive_expected_digest: Some(runtime_queue_ledger_digest(&archive_file).unwrap()),
            history_store_file: None,
            history_transaction_id: None,
        };
        let marker_file = runtime_queue_ledger_compaction_marker_file(&receipts_file);
        let (ready_sender, ready_receiver) = mpsc::channel();
        let (release_sender, release_receiver) = mpsc::channel();
        let receipt_lock_file = receipts_file.clone();
        let marker_for_thread = marker.clone();
        let marker_file_for_thread = marker_file.clone();
        let holder = std::thread::spawn(move || {
            crate::logging::with_jsonl_append_lock(&receipt_lock_file, || {
                write_json_atomic(&marker_file_for_thread, &marker_for_thread)?;
                ready_sender
                    .send(())
                    .expect("test must observe the live compaction marker");
                release_receiver
                    .recv_timeout(Duration::from_secs(5))
                    .expect("test must release the live compaction lock");
                Ok(())
            })
        });
        ready_receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("test compaction lock holder must start");

        let started = std::time::Instant::now();
        let refreshed =
            refresh_runtime_queue_state_index_nonblocking(&queue_dir, &mut Vec::new()).unwrap();
        let elapsed = started.elapsed();

        release_sender.send(()).unwrap();
        holder.join().unwrap().unwrap();

        assert!(
            elapsed < Duration::from_secs(1),
            "progress refresh must return its cached index instead of waiting for compaction; elapsed={elapsed:?}"
        );
        assert!(
            refreshed.queues["turn:cached-terminal"].terminal_ever,
            "the safe cached index must remain usable while the ledger is being replaced"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn runtime_queue_receipt_compaction_recovery_refuses_corrupted_archive_without_valid_snapshot()
    {
        let root = temp_root(
            "runtime_queue_receipt_compaction_recovery_refuses_corrupted_archive_without_valid_snapshot",
        );
        let harness_home = root.join(".agent-harness");
        let queue_dir = queue_dir(&harness_home);
        fs::create_dir_all(&queue_dir).unwrap();
        let receipts_file = queue_dir.join("run-once-receipts.jsonl");
        let archive_dir = queue_dir.join("run-once-receipts-archive");
        fs::create_dir_all(&archive_dir).unwrap();
        let archive_file = archive_dir.join("run-once-receipts-10.jsonl");
        let original_archive = b"{\"queueId\":\"turn:historical\",\"status\":\"completed\"}\n";
        fs::write(&archive_file, original_archive).unwrap();
        let archive_expected_digest = runtime_queue_ledger_digest(&archive_file).unwrap();

        let compact_temp = runtime_queue_ledger_temp_file(&receipts_file, 11, "compact");
        let expected_snapshot = b"{\"queueId\":\"turn:retry\",\"status\":\"retry-pending\"}\n";
        fs::write(&compact_temp, expected_snapshot).unwrap();
        let expected_digest = runtime_queue_ledger_digest(&compact_temp).unwrap();
        fs::write(&compact_temp, b"not-a-valid-snapshot\n").unwrap();
        let corrupted_destination = b"not-a-valid-destination\n";
        fs::write(&receipts_file, corrupted_destination).unwrap();
        fs::write(&archive_file, b"corrupted-archive\n").unwrap();

        let marker = RuntimeQueueLedgerCompactionPending {
            schema: RUNTIME_QUEUE_RECEIPT_COMPACTION_PENDING_SCHEMA.to_string(),
            ledger_file: receipts_file.clone(),
            archive_file,
            temp_file: compact_temp,
            expected_bytes: expected_snapshot.len() as u64,
            expected_digest,
            archive_expected_bytes: Some(original_archive.len() as u64),
            archive_expected_digest: Some(archive_expected_digest),
            history_store_file: None,
            history_transaction_id: None,
        };
        let marker_file = runtime_queue_ledger_compaction_marker_file(&receipts_file);
        write_json_atomic(&marker_file, &marker).unwrap();

        let error =
            recover_runtime_queue_ledger_compaction_if_needed(&receipts_file, &mut Vec::new())
                .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(fs::read(&receipts_file).unwrap(), corrupted_destination);
        assert!(
            marker_file.is_file(),
            "an unverifiable archive must remain marked for explicit recovery instead of becoming active state"
        );

        let _ = fs::remove_dir_all(root);
    }

    fn queued_item_value(queue_id: &str) -> Value {
        serde_json::json!({
            "schema": "agent-harness.runtime-queue-item.v1",
            "queueId": queue_id,
            "status": "queued",
            "createdAtMs": 10,
            "agentId": "main",
            "sessionKey": "telegram:dm:user:main",
            "runtimeClass": "interactive",
            "origin": "channel",
            "platform": "telegram",
            "channelId": "dm",
            "userId": "user",
            "messageText": "continue",
            "source": {
                "sourceHome": "source",
                "sourceWorkspace": "workspace"
            },
            "plannedTranscriptFile": "transcript.jsonl",
            "plannedTrajectoryFile": "trajectory.jsonl",
            "selectedSkillIds": []
        })
    }

    fn queued_ultra_v2_value(queue_id: &str, admission_queue_id: &str) -> Value {
        let session_key = "telegram:dm:user:main";
        let lane = crate::lane::FullLaneKeyV1::new(
            "telegram",
            "primary",
            "dm",
            "user",
            "main",
            "interactive",
            session_key,
            session_key,
        )
        .unwrap();
        let owner = crate::worker_result_mailbox::ExactWorkerResultOwnerV1::new(
            lane,
            derive_virtual_session_id("telegram", "dm", "user", "main", session_key),
            None,
            Some(admission_queue_id.to_string()),
            admission_queue_id,
            None,
            None,
        )
        .unwrap();
        let preference = crate::execution_mode::ExecutionModePreference::explicit("ultra").unwrap();
        let policy = crate::execution_mode::ExecutionModePolicyV1::new(
            crate::execution_mode::ExecutionModeSource::ChildAdmission,
            &preference,
            "ultra",
            "main",
            "auth-v1",
            "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            2,
            6,
            300_000,
        )
        .unwrap();
        let readiness = crate::execution_mode::SafeResumeReadinessReceiptV1::new(
            &owner,
            "durability-v1",
            true,
            true,
            true,
            true,
        )
        .unwrap();
        let snapshot = crate::execution_mode::AuthorizedExecutionModeSnapshotV2::new(
            preference,
            Some(policy),
            Some(crate::worker_result_mailbox::WorkerResultOwnerV1::Exact(
                owner,
            )),
            Some(readiness),
        )
        .unwrap();
        let mut value = queued_item_value(queue_id);
        let object = value.as_object_mut().unwrap();
        object.insert(
            "schema".to_string(),
            Value::String("agent-harness.runtime-queue-item.v2".to_string()),
        );
        object.insert(
            "admissionQueueId".to_string(),
            Value::String(admission_queue_id.to_string()),
        );
        object.insert(
            "accountId".to_string(),
            Value::String("primary".to_string()),
        );
        object.insert(
            "authorizedExecutionMode".to_string(),
            serde_json::to_value(snapshot).unwrap(),
        );
        value
    }

    fn coordinator_resume_item_value() -> Value {
        let queue_id = "coordinator-resume:intent-exact";
        let session_key = "telegram:dm:user:main:root-session:cont-1";
        let root_session = root_working_session_key(session_key);
        let lane = crate::lane::FullLaneKeyV1::new(
            "telegram",
            "account-a",
            "dm",
            "user",
            "main",
            "interactive",
            &root_session,
            session_key,
        )
        .unwrap();
        let owner = crate::worker_result_mailbox::ExactWorkerResultOwnerV1::new(
            lane,
            derive_virtual_session_id("telegram", "dm", "user", "main", &root_session),
            None,
            Some("turn:parent-exact".to_string()),
            "turn:parent-exact",
            None,
            None,
        )
        .unwrap();
        let metadata = crate::coordinator_resume::CoordinatorResumeMetadataV1::new(
            "intent-exact",
            "wait-exact",
            queue_id,
            owner,
        )
        .unwrap();
        let mut value = queued_item_value(queue_id);
        let object = value.as_object_mut().unwrap();
        object.insert(
            "schema".to_string(),
            Value::String("agent-harness.runtime-queue-item.v2".to_string()),
        );
        object.insert(
            "sessionKey".to_string(),
            Value::String(session_key.to_string()),
        );
        object.insert(
            "accountId".to_string(),
            Value::String("account-a".to_string()),
        );
        object.insert(
            "origin".to_string(),
            Value::String("coordinator-resume".to_string()),
        );
        object.insert(
            "coordinatorResume".to_string(),
            serde_json::to_value(metadata).unwrap(),
        );
        value
    }

    #[test]
    fn coordinator_resume_pending_item_rejects_each_owner_lane_axis_mismatch() {
        let valid = coordinator_resume_item_value();
        assert!(parse_pending_item(&valid).is_ok());

        let mutations = [
            ("platform", "discord", "platform"),
            ("accountId", "account-b", "accountId"),
            ("channelId", "dm-other", "channelId"),
            ("userId", "user-other", "userId"),
            ("agentId", "audit-worker", "agentId"),
            ("runtimeClass", "worker", "runtimeClass"),
            (
                "sessionKey",
                "telegram:dm:user:main:other-root:cont-1",
                "rootVirtualSession",
            ),
            (
                "sessionKey",
                "telegram:dm:user:main:root-session:cont-2",
                "concreteSession",
            ),
        ];

        for (field, replacement, expected_axis) in mutations {
            let mut mutated = valid.clone();
            mutated
                .as_object_mut()
                .unwrap()
                .insert(field.to_string(), Value::String(replacement.to_string()));
            let error = parse_pending_item(&mutated).unwrap_err();
            assert!(
                error.contains(expected_axis),
                "mutation {field}={replacement} should reject {expected_axis}, got: {error}"
            );
        }

        let mut mismatched_virtual_session = valid;
        mismatched_virtual_session["coordinatorResume"]["owner"]["virtualSessionId"] =
            Value::String("telegram:dm:user:main:vsession-other".to_string());
        let error = parse_pending_item(&mismatched_virtual_session).unwrap_err();
        assert!(error.contains("virtualSessionId"), "{error}");
    }

    #[test]
    fn pending_v2_rejects_non_standard_execution_snapshot() {
        let error = parse_pending_item(&queued_ultra_v2_value(
            "retry:attempt-2",
            "turn:admission-1",
        ))
        .unwrap_err();
        assert!(
            error.contains("non-standard execution mode is not supported"),
            "{error}"
        );
    }

    fn exact_reasoning_policy(effort: &str) -> BackendReasoningPolicyV1 {
        BackendReasoningPolicyV1::new(
            crate::backend_reasoning::BackendReasoningSource::ChannelCommand,
            crate::model_catalog::ReasoningResolutionReceipt {
                schema_version: crate::model_catalog::REASONING_RESOLUTION_RECEIPT_SCHEMA_VERSION,
                requested_provider: "openai".to_string(),
                requested_model: "gpt-5.6-sol".to_string(),
                effective_provider: Some("openai".to_string()),
                effective_model: Some("gpt-5.6-sol".to_string()),
                requested_effort: effort.to_string(),
                effective_effort: Some(effort.to_string()),
                catalog_effective_effort: Some(effort.to_string()),
                catalog_revision: Some("test-revision".to_string()),
                status: crate::model_catalog::ReasoningResolutionStatus::Accepted,
                authoritative: true,
                reason: "test exact route".to_string(),
            },
        )
        .unwrap()
    }

    fn add_reasoning_snapshot(value: &mut Value, effort: &str) {
        let object = value.as_object_mut().unwrap();
        object.insert("provider".to_string(), Value::String("openai".to_string()));
        object.insert(
            "model".to_string(),
            Value::String("gpt-5.6-sol".to_string()),
        );
        object.insert(
            "reasoningPreference".to_string(),
            serde_json::to_value(ReasoningPreference::explicit(effort).unwrap()).unwrap(),
        );
        object.insert(
            "backendReasoningPolicy".to_string(),
            serde_json::to_value(exact_reasoning_policy(effort)).unwrap(),
        );
    }

    #[test]
    fn pending_reasoning_snapshot_parser_is_strict_and_legacy_compatible() {
        let legacy = parse_pending_item(&queued_item_value("legacy")).unwrap();
        assert_eq!(legacy.reasoning_preference, None);
        assert_eq!(legacy.backend_reasoning_policy, None);

        let mut route_only = queued_item_value("route-only");
        route_only
            .as_object_mut()
            .unwrap()
            .insert("provider".to_string(), Value::String("openai".to_string()));
        route_only.as_object_mut().unwrap().insert(
            "model".to_string(),
            Value::String("gpt-5.6-sol".to_string()),
        );
        let route_only = parse_pending_item(&route_only).unwrap();
        assert_eq!(route_only.provider.as_deref(), Some("openai"));
        assert_eq!(route_only.model.as_deref(), Some("gpt-5.6-sol"));
        assert_eq!(route_only.reasoning_preference, None);

        let mut partial_route = queued_item_value("partial-route");
        partial_route
            .as_object_mut()
            .unwrap()
            .insert("provider".to_string(), Value::String("openai".to_string()));
        assert!(parse_pending_item(&partial_route).is_err());

        let mut exact = queued_item_value("exact");
        add_reasoning_snapshot(&mut exact, "max");
        let parsed = parse_pending_item(&exact).unwrap();
        assert_eq!(
            parsed
                .reasoning_preference
                .as_ref()
                .and_then(ReasoningPreference::explicit_effort),
            Some("max")
        );
        assert_eq!(
            parsed
                .backend_reasoning_policy
                .as_ref()
                .map(BackendReasoningPolicyV1::effective_effort),
            Some("max")
        );

        let mut policy_only = exact.clone();
        policy_only
            .as_object_mut()
            .unwrap()
            .remove("reasoningPreference");
        assert!(parse_pending_item(&policy_only).is_err());

        let mut wrong_route = exact;
        wrong_route.as_object_mut().unwrap().insert(
            "model".to_string(),
            Value::String("gpt-5.6-luna".to_string()),
        );
        assert!(parse_pending_item(&wrong_route).is_err());
    }

    #[test]
    fn pending_queue_parser_rejects_unknown_schema_version() {
        let mut future = queued_item_value("future-schema");
        future.as_object_mut().unwrap().insert(
            "schema".to_string(),
            Value::String("agent-harness.runtime-queue-item.v99".to_string()),
        );
        let error = parse_pending_item(&future).unwrap_err();
        assert!(
            error.contains("unsupported runtime queue schema"),
            "{error}"
        );
    }

    #[test]
    fn malformed_reasoning_snapshot_is_quarantined_durably() {
        let root = temp_root("malformed_reasoning_snapshot_is_quarantined");
        let harness_home = root.join(".agent-harness");
        let queue_file = harness_home
            .join("state")
            .join("runtime-queue")
            .join("pending.jsonl");
        fs::create_dir_all(queue_file.parent().unwrap()).unwrap();
        let mut malformed = queued_item_value("malformed-reasoning");
        malformed.as_object_mut().unwrap().insert(
            "reasoningPreference".to_string(),
            serde_json::to_value(ReasoningPreference::explicit("max").unwrap()).unwrap(),
        );
        let mut partial_route = queued_item_value("partial-route");
        partial_route
            .as_object_mut()
            .unwrap()
            .insert("provider".to_string(), Value::String("openai".to_string()));
        fs::write(
            &queue_file,
            format!(
                "{}\n{}\n",
                serde_json::to_string(&malformed).unwrap(),
                serde_json::to_string(&partial_route).unwrap()
            ),
        )
        .unwrap();
        let mut warnings = Vec::new();
        assert!(
            read_pending_items(&queue_file, &mut warnings)
                .unwrap()
                .is_empty()
        );
        assert!(warnings.iter().any(|warning| {
            warning.contains("invalid execution snapshot")
                && warning.contains("must be present together")
        }));
        assert!(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("quarantine")
                .join("malformed-reasoning.json")
                .is_file()
        );
        assert!(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("quarantine")
                .join("partial-route.json")
                .is_file()
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn pending_reasoning_admission_rechecks_catalog_and_rollout() {
        let root = temp_root("pending_reasoning_admission_rechecks_catalog");
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(harness_home.join("codex-home")).unwrap();
        fs::write(
            harness_home.join(crate::HARNESS_CONFIG_FILE_NAME),
            r#"{"orchestration":{"features":{"modelCatalogV2":{"mode":"authoritative","enabledAgentIds":["main"]}}}}"#,
        )
        .unwrap();
        fs::write(
            harness_home.join("codex-home").join("models_cache.json"),
            r#"{"models":[{"slug":"gpt-5.6-sol","default_reasoning_level":"low","supported_reasoning_levels":[{"effort":"low"},{"effort":"max"}]}]}"#,
        )
        .unwrap();
        let mut value = queued_item_value("admission");
        add_reasoning_snapshot(&mut value, "max");
        let pending = parse_pending_item(&value).unwrap();
        revalidate_pending_reasoning_snapshot(&harness_home, &pending).unwrap();

        fs::write(
            harness_home.join(crate::HARNESS_CONFIG_FILE_NAME),
            r#"{"orchestration":{"features":{"modelCatalogV2":{"mode":"off"}}}}"#,
        )
        .unwrap();
        assert!(revalidate_pending_reasoning_snapshot(&harness_home, &pending).is_err());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn runtime_queue_lease_owner_envelope_serializes_generation_metadata() {
        let acquired_at_ms = 1_782_029_223_481;
        let process_start_time_ms = 1_782_029_220_000;
        let generation_id = "runtime-loop-supervised-1234".to_string();
        let owner = runtime_queue_lease_owner_from_metadata(
            acquired_at_ms,
            Some(generation_id.clone()),
            Some(process_start_time_ms),
            Some("rust-supervisor-run".to_string()),
        );

        let value = serde_json::to_value(&owner).unwrap();
        assert_eq!(value["kind"], "supervisor-child");
        assert_eq!(value["serviceId"], RUNTIME_LOOP_SERVICE_ID);
        assert_eq!(value["generationId"], generation_id);
        assert_eq!(value["pid"], i64::from(std::process::id()));
        assert_eq!(value["processStartTimeMs"], process_start_time_ms);
        assert_eq!(value["acquiredAtMs"], acquired_at_ms);

        let parsed: RuntimeQueueLeaseOwner = serde_json::from_value(value).unwrap();
        assert_eq!(parsed.process_id(), Some(i64::from(std::process::id())));
        let legacy: RuntimeQueueLeaseOwner = serde_json::from_str(r#""pid:0""#).unwrap();
        assert_eq!(legacy.process_id(), Some(0));
        assert_eq!(
            serde_json::to_value(legacy).unwrap(),
            Value::String("pid:0".to_string())
        );
    }

    #[test]
    fn runtime_queue_lease_observation_distinguishes_released_active_expired_and_dead_owner() {
        let root = temp_root("runtime_queue_lease_observation_states");
        let harness_home = root.join(".agent-harness");
        let queue_id = "queue-observation";
        let observed_at_ms = 10_000;

        let released = observe_runtime_queue_lease(RuntimeQueueLeaseObservationOptions {
            harness_home: harness_home.clone(),
            queue_id: queue_id.to_string(),
            observed_at_ms,
        });
        assert_eq!(released.queue_id, queue_id);
        assert_eq!(
            released.status,
            RuntimeQueueLeaseObservationStatus::Released
        );
        assert_eq!(released.observed_at_ms, observed_at_ms);

        write_observation_lease(
            &harness_home,
            "interactive",
            queue_id,
            observed_at_ms + 1_000,
            RuntimeQueueLeaseOwner::Legacy(format!("pid:{}", std::process::id())),
        );
        let active = observe_runtime_queue_lease(RuntimeQueueLeaseObservationOptions {
            harness_home: harness_home.clone(),
            queue_id: queue_id.to_string(),
            observed_at_ms,
        });
        assert_eq!(active.status, RuntimeQueueLeaseObservationStatus::Active);
        assert_eq!(active.runtime_class.as_deref(), Some("interactive"));

        write_observation_lease(
            &harness_home,
            "interactive",
            queue_id,
            observed_at_ms,
            RuntimeQueueLeaseOwner::Legacy(format!("pid:{}", std::process::id())),
        );
        let expired = observe_runtime_queue_lease(RuntimeQueueLeaseObservationOptions {
            harness_home: harness_home.clone(),
            queue_id: queue_id.to_string(),
            observed_at_ms,
        });
        assert_eq!(expired.status, RuntimeQueueLeaseObservationStatus::Expired);

        write_observation_lease(
            &harness_home,
            "interactive",
            queue_id,
            observed_at_ms + 1_000,
            RuntimeQueueLeaseOwner::Legacy("pid:0".to_string()),
        );
        let dead = observe_runtime_queue_lease(RuntimeQueueLeaseObservationOptions {
            harness_home: harness_home.clone(),
            queue_id: queue_id.to_string(),
            observed_at_ms,
        });
        assert_eq!(dead.status, RuntimeQueueLeaseObservationStatus::DeadOwner);
        assert!(dead.reason.contains("non-running"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn runtime_queue_lease_observation_fails_closed_for_malformed_and_unreadable_state() {
        for (label, make_bad_state) in [("malformed", false), ("unreadable", true)] {
            let root = temp_root(&format!("runtime_queue_lease_observation_{label}"));
            let harness_home = root.join(".agent-harness");
            let queue_dir = queue_dir(&harness_home);
            let leases_file = runtime_queue_leases_file(&queue_dir, "interactive");
            fs::create_dir_all(leases_file.parent().unwrap()).unwrap();
            if make_bad_state {
                fs::create_dir_all(&leases_file).unwrap();
            } else {
                fs::write(&leases_file, "{malformed").unwrap();
            }

            let receipt = observe_runtime_queue_lease(RuntimeQueueLeaseObservationOptions {
                harness_home: harness_home.clone(),
                queue_id: "queue-unknown".to_string(),
                observed_at_ms: 10_000,
            });
            assert_eq!(receipt.status, RuntimeQueueLeaseObservationStatus::Unknown);
            assert!(receipt.reason.contains("interactive"));
            let _ = fs::remove_dir_all(root);
        }
    }

    #[test]
    fn runtime_queue_lease_observation_fails_closed_when_any_class_lock_is_busy() {
        let root = temp_root("runtime_queue_lease_observation_lock_busy");
        let harness_home = root.join(".agent-harness");
        let queue_dir = queue_dir(&harness_home);
        fs::create_dir_all(&queue_dir).unwrap();
        let _held_lock = create_runtime_queue_lease_lock(
            &runtime_queue_lease_lock_file(&queue_dir, "interactive"),
            10_000,
        )
        .unwrap();

        let receipt = observe_runtime_queue_lease(RuntimeQueueLeaseObservationOptions {
            harness_home: harness_home.clone(),
            queue_id: "queue-lock-busy".to_string(),
            observed_at_ms: 10_001,
        });
        assert_eq!(receipt.status, RuntimeQueueLeaseObservationStatus::Unknown);
        assert!(receipt.reason.contains("lock is busy"));

        drop(_held_lock);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn exact_lane_activity_observation_is_virtual_session_scoped_and_fail_closed() {
        let root = temp_root("exact_lane_activity_observation");
        let harness_home = root.join(".agent-harness");
        let session_key = "telegram:account-a:channel-a:user-a:main";
        let virtual_session_id =
            derive_virtual_session_id("telegram", "channel-a", "user-a", "main", session_key);
        let owner = crate::worker_result_mailbox::ExactWorkerResultOwnerV1::new(
            FullLaneKeyV1::new(
                "telegram",
                "account-a",
                "channel-a",
                "user-a",
                "main",
                "interactive",
                session_key,
                session_key,
            )
            .unwrap(),
            virtual_session_id,
            None,
            Some("parent-queue".to_string()),
            "child-queue",
            None,
            None,
        )
        .unwrap();

        write_observation_lease(
            &harness_home,
            "interactive",
            "newer-queue",
            20_000,
            RuntimeQueueLeaseOwner::Legacy(format!("pid:{}", std::process::id())),
        );
        let active =
            observe_runtime_queue_lane_activity(RuntimeQueueLaneActivityObservationOptions {
                harness_home: harness_home.clone(),
                owner: owner.clone(),
                observed_at_ms: 10_000,
            });
        assert!(active.lane_active);
        assert!(active.blocker_reason.unwrap().contains("newer-queue"));

        let mut other_virtual_session_owner = owner.clone();
        other_virtual_session_owner.virtual_session_id = "different-virtual-session".to_string();
        let other_virtual_session =
            observe_runtime_queue_lane_activity(RuntimeQueueLaneActivityObservationOptions {
                harness_home: harness_home.clone(),
                owner: other_virtual_session_owner,
                observed_at_ms: 10_000,
            });
        assert!(!other_virtual_session.lane_active);

        let queue_dir = queue_dir(&harness_home);
        fs::write(
            runtime_queue_leases_file(&queue_dir, "interactive"),
            "{malformed",
        )
        .unwrap();
        let unknown =
            observe_runtime_queue_lane_activity(RuntimeQueueLaneActivityObservationOptions {
                harness_home: harness_home.clone(),
                owner,
                observed_at_ms: 10_001,
            });
        assert!(unknown.lane_active);
        assert!(unknown.blocker_reason.unwrap().contains("unknown"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn exact_lane_activity_observation_fails_closed_when_a_class_lock_is_busy() {
        let root = temp_root("exact_lane_activity_observation_lock_busy");
        let harness_home = root.join(".agent-harness");
        let queue_dir = queue_dir(&harness_home);
        fs::create_dir_all(&queue_dir).unwrap();
        let _held_lock = create_runtime_queue_lease_lock(
            &runtime_queue_lease_lock_file(&queue_dir, "interactive"),
            10_000,
        )
        .unwrap();
        let session_key = "telegram:account-a:channel-a:user-a:main";
        let owner = crate::worker_result_mailbox::ExactWorkerResultOwnerV1::new(
            FullLaneKeyV1::new(
                "telegram",
                "account-a",
                "channel-a",
                "user-a",
                "main",
                "interactive",
                session_key,
                session_key,
            )
            .unwrap(),
            derive_virtual_session_id("telegram", "channel-a", "user-a", "main", session_key),
            None,
            Some("parent-queue".to_string()),
            "child-queue",
            None,
            None,
        )
        .unwrap();

        let receipt =
            observe_runtime_queue_lane_activity(RuntimeQueueLaneActivityObservationOptions {
                harness_home: harness_home.clone(),
                owner,
                observed_at_ms: 10_001,
            });
        assert!(receipt.lane_active);
        assert!(receipt.blocker_reason.unwrap().contains("lock is busy"));

        drop(_held_lock);
        let _ = fs::remove_dir_all(root);
    }

    fn write_observation_lease(
        harness_home: &Path,
        runtime_class: &str,
        queue_id: &str,
        lease_expires_at_ms: i64,
        owner: RuntimeQueueLeaseOwner,
    ) {
        let queue_dir = queue_dir(harness_home);
        let mut state = RuntimeQueueLeaseState {
            schema: runtime_queue_leases_schema(),
            leases: BTreeMap::new(),
        };
        state.leases.insert(
            queue_id.to_string(),
            RuntimeQueueLease {
                queue_id: queue_id.to_string(),
                agent_id: "main".to_string(),
                runtime_class: runtime_class.to_string(),
                origin: "channel".to_string(),
                cron_run_id: None,
                platform: "telegram".to_string(),
                account_id: Some("account-a".to_string()),
                channel_id: "channel-a".to_string(),
                user_id: Some("user-a".to_string()),
                session_key: "telegram:account-a:channel-a:user-a:main".to_string(),
                virtual_session_id: None,
                session_lane_key: None,
                owner,
                started_at_ms: 9_000,
                lease_expires_at_ms,
            },
        );
        write_runtime_queue_leases(&queue_dir, runtime_class, &state).unwrap();
    }

    #[test]
    fn prepare_runtime_queue_item_writes_prompt_bundle_and_receipts() {
        let root = temp_root("prepare_runtime_queue_item_writes_prompt_bundle_and_receipts");
        let source = write_worker_source(&root);
        let harness_home = root.join(".agent-harness");
        enqueue_fixture_turn(&source, &harness_home);
        let pending_text =
            fs::read_to_string(queue_dir(&harness_home).join("pending.jsonl")).unwrap();
        assert!(!pending_text.contains("inboundMediaArtifacts"));

        let report = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();

        assert_eq!(
            report.receipt.status,
            RuntimeExecutionReceiptStatus::Prepared
        );
        assert!(report.execution_receipts_file.is_file());
        let item = report.item.unwrap();
        assert!(item.inbound_media_artifacts.is_empty());
        assert_eq!(item.agent_id, "main");
        assert_eq!(item.provider.as_deref(), Some("openai"));
        assert_eq!(item.model.as_deref(), Some("gpt-5"));
        assert!(item.prompt_bundle_json.is_file());
        assert!(item.prompt_markdown.is_file());
        assert!(item.receipt_file.is_file());
        let bundle_json: Value =
            serde_json::from_slice(&fs::read(item.prompt_bundle_json).unwrap()).unwrap();
        assert_eq!(bundle_json["summary"]["userMessagesIncluded"], 1);
        assert_eq!(bundle_json["agentId"], "main");
        assert!(
            fs::read_to_string(item.prompt_markdown)
                .unwrap()
                .contains("repair memory cron")
        );
        let receipt_json: Value =
            serde_json::from_slice(&fs::read(item.receipt_file).unwrap()).unwrap();
        assert_eq!(receipt_json["status"], "prepared");
        let lease_state =
            read_runtime_queue_leases(&queue_dir(&harness_home), "interactive", &mut Vec::new())
                .unwrap();
        let lease = lease_state.leases.get(&item.queue_id).unwrap();
        let RuntimeQueueLeaseOwner::Envelope(owner) = &lease.owner else {
            panic!("new runtime queue lease should use an owner envelope");
        };
        assert_eq!(owner.service_id, RUNTIME_LOOP_SERVICE_ID);
        assert_eq!(owner.pid, i64::from(std::process::id()));
        assert!(owner.generation_id.starts_with("runtime-loop-"));
        assert!(owner.process_start_time_ms > 0);
        assert_eq!(owner.acquired_at_ms, lease.started_at_ms);

        let _ = fs::remove_dir_all(root);
    }

    // Contract: SK-F1 shadow routing is observability-only at the real runtime
    // preparation seam.
    // Source: frozen Skill Ecosystem F1 handoff, synthetic exact-lane replay.
    // Fails on: the dormant router is never invoked by runtime preparation.
    // Asserts: enabling shadow writes one explainable receipt while active v4
    // selection and the model-facing prompt remain byte-for-byte unchanged.
    #[test]
    fn skill_router_v2_shadow_runtime_receipt_has_zero_serving_side_effect() {
        let root = temp_root("skill_router_v2_shadow_zero_side_effect");
        let source = write_worker_source(&root);
        let harness_home = root.join("harness");
        enqueue_fixture_turn_for_account(
            &source,
            &harness_home,
            "default",
            "repair memory cron",
            1234,
        );
        let off = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap()
        .item
        .unwrap();
        let off_selected_skill_ids = off.selected_skill_ids;
        let off_prompt_markdown = fs::read(&off.prompt_markdown).unwrap();
        assert!(
            !harness_home
                .join("state")
                .join("skills")
                .join("shadow-routing")
                .exists(),
            "feature-off preparation must not create shadow receipts"
        );

        fs::remove_dir_all(&harness_home).unwrap();
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            r#"{"skills":{"matcher":{"shadowV2Enabled":true}}}"#,
        )
        .unwrap();
        enqueue_fixture_turn_for_account(
            &source,
            &harness_home,
            "default",
            "repair memory cron",
            1234,
        );
        let on = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap()
        .item
        .unwrap();

        assert_eq!(off_selected_skill_ids, on.selected_skill_ids);
        assert_eq!(
            off_prompt_markdown,
            fs::read(&on.prompt_markdown).unwrap(),
            "shadow routing must not change model-facing prompt bytes"
        );
        let receipt_dir = harness_home
            .join("state")
            .join("skills")
            .join("shadow-routing");
        let receipt_files = fs::read_dir(&receipt_dir)
            .expect("shadow-enabled runtime must write a receipt")
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(receipt_files.len(), 1);
        let receipt_bytes = fs::read(receipt_files[0].path()).unwrap();
        let receipt_text = String::from_utf8(receipt_bytes.clone()).unwrap();
        assert!(!receipt_text.contains("repair memory cron"));
        assert!(!receipt_text.contains("dm-42"));
        assert!(!receipt_text.contains("user-7"));
        let receipt: crate::SkillRoutingReceiptV2 = serde_json::from_slice(&receipt_bytes).unwrap();
        receipt.validate().unwrap();
        assert!(receipt.shadow);
        assert_eq!(receipt.channel, "telegram");
        assert_eq!(receipt.identity.agent_id, "main");
        assert_eq!(receipt.task_text_bytes, "repair memory cron".len());

        assert!(!crate::virtual_skill_manifest_dir(&harness_home).exists());
        assert!(
            !harness_home
                .join("state")
                .join("skills")
                .join("delivery-receipts")
                .exists()
        );

        assert!(!crate::skill_usage_events_file(&harness_home).exists());
        assert!(!crate::skill_proposals_file(&harness_home).exists());
        assert!(!crate::self_improvement_review_receipts_file(&harness_home).exists());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn virtual_skill_manifest_observer_records_delivery_without_serving_side_effect() {
        let root = temp_root("virtual_skill_manifest_observer_zero_side_effect");
        let source = write_worker_source(&root);
        let harness_home = root.join("harness");
        enqueue_fixture_turn_for_account(
            &source,
            &harness_home,
            "default",
            "repair memory cron",
            1234,
        );
        let off = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap()
        .item
        .unwrap();
        let off_selected_skill_ids = off.selected_skill_ids;
        let off_prompt_markdown = fs::read(&off.prompt_markdown).unwrap();

        fs::remove_dir_all(&harness_home).unwrap();
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            r#"{
              "skills": {
                "matcher": {"shadowV2Enabled": true},
                "virtualManifest": {"observeEnabled": true}
              }
            }"#,
        )
        .unwrap();
        enqueue_fixture_turn_for_account(
            &source,
            &harness_home,
            "default",
            "repair memory cron",
            1234,
        );
        let on = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap()
        .item
        .unwrap();

        assert_eq!(off_selected_skill_ids, on.selected_skill_ids);
        assert_eq!(off_prompt_markdown, fs::read(&on.prompt_markdown).unwrap());
        let manifest_file = on
            .virtual_skill_manifest_file
            .as_ref()
            .expect("observe-enabled manifest file");
        let manifest: crate::VirtualSkillManifestV1 =
            serde_json::from_slice(&fs::read(manifest_file).unwrap()).unwrap();
        manifest.validate().unwrap();
        assert_eq!(
            manifest
                .skills
                .iter()
                .map(|entry| entry.skill_id.as_str())
                .collect::<BTreeSet<_>>(),
            on.selected_skill_ids
                .iter()
                .map(String::as_str)
                .collect::<BTreeSet<_>>()
        );
        assert_eq!(
            manifest.deliveries.len(),
            on.skill_delivery_receipt_files.len()
        );
        assert_eq!(manifest.deliveries.len(), on.selected_skill_ids.len());
        assert!(
            on.skill_delivery_receipt_files
                .iter()
                .all(|path| path.is_file())
        );
        assert!(!crate::skill_usage_events_file(&harness_home).exists());
        assert!(!crate::skill_proposals_file(&harness_home).exists());
        assert!(!crate::self_improvement_review_receipts_file(&harness_home).exists());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn skill_router_v2_shadow_runtime_canonicalizes_partial_lane_without_blocking_serving() {
        let root = temp_root("skill_router_v2_shadow_partial_lane");
        let source = write_worker_source(&root);
        let harness_home = root.join("harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            r#"{"skills":{"matcher":{"shadowV2Enabled":true}}}"#,
        )
        .unwrap();
        enqueue_fixture_turn_with_text(&source, &harness_home, "repair memory cron", 1234);

        let report = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();
        assert!(report.item.is_some(), "serving preparation must continue");
        assert!(
            !report
                .warnings
                .iter()
                .any(|warning| warning.contains("shadow skipped"))
        );
        assert!(
            harness_home
                .join("state")
                .join("skills")
                .join("shadow-routing")
                .exists()
        );
        assert!(!crate::skill_usage_events_file(&harness_home).exists());
        assert!(!crate::skill_proposals_file(&harness_home).exists());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prepare_runtime_queue_item_records_lease_acquired_before_prepared_receipt() {
        let root = temp_root("prepare_runtime_queue_item_records_lease_acquired_before_prepared");
        let source = write_worker_source(&root);
        let harness_home = root.join(".agent-harness");
        enqueue_fixture_turn(&source, &harness_home);

        let report = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();

        assert_eq!(
            report.receipt.status,
            RuntimeExecutionReceiptStatus::Prepared
        );
        let receipts = fs::read_to_string(&report.execution_receipts_file).unwrap();
        let statuses = receipts
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .map(|value| value["status"].as_str().unwrap().to_string())
            .collect::<Vec<_>>();
        let normalized = statuses
            .iter()
            .position(|status| status == "session-identity-normalized")
            .expect("legacy fixture identity is normalized before lease acquisition");
        let leased = statuses
            .iter()
            .position(|status| status == "lease-acquired")
            .expect("lease acquisition receipt");
        assert!(normalized < leased);
        assert_eq!(statuses.last().map(String::as_str), Some("prepared"));
        assert_eq!(
            statuses
                .iter()
                .filter(|status| *status == "prepared")
                .count(),
            1
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prepare_runtime_queue_item_round_trips_inbound_media_artifacts_to_prompt() {
        let root =
            temp_root("prepare_runtime_queue_item_round_trips_inbound_media_artifacts_to_prompt");
        let source = write_worker_source(&root);
        let harness_home = root.join(".agent-harness");
        let local_path = inbound_media_attachment_root(&harness_home)
            .join("turn-1234")
            .join("0.jpg");
        let registry = load_agent_registry(&source).unwrap();
        let skills = build_source_skill_index(&source).unwrap();
        let turn = build_turn_plan(
            &source,
            &registry,
            &skills,
            TurnPlanInput {
                harness_home: Some(harness_home.clone()),
                platform: "telegram".to_string(),
                channel_id: "dm-42".to_string(),
                user_id: "user-7".to_string(),
                text: "what is in this image?".to_string(),
                inbound_context: None,
                inbound_media_artifacts: vec![InboundMediaArtifact {
                    platform: "telegram".to_string(),
                    kind: "photo".to_string(),
                    message_id: Some("99".to_string()),
                    variant_count: Some(4),
                    selected_variant: Some(InboundMediaSelectedVariant {
                        width: Some(961),
                        height: Some(1280),
                        file_size: Some(179414),
                    }),
                    local_path: Some(local_path.clone()),
                    artifact_uri: Some("agent-harness://inbound-media/turn-1234/0.jpg".to_string()),
                    mime: Some("image/jpeg".to_string()),
                    sha256: Some("abc123".to_string()),
                    source: "https://api.telegram.org/botTOKEN/getFile?file_id=secret".to_string(),
                    caption_preview: Some(
                        "data:image/jpeg;base64,AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
                            .to_string(),
                    ),
                    lifecycle_status: Some("summarized".to_string()),
                    extraction_summary: Some(ArtifactExtractionSummary {
                        artifact_class: Some("image".to_string()),
                        modality: Some("photo".to_string()),
                        summary: Some(
                            "Reference image extracted into subject, pose, wardrobe, and composition facts."
                                .to_string(),
                        ),
                        facts: vec![
                            "subject pose is visible".to_string(),
                            "raw pixels remain in artifact storage".to_string(),
                        ],
                        uncertainty: Some("fine details require vision tool lookup".to_string()),
                    }),
                    download_status: InboundMediaDownloadStatus::Downloaded,
                    model_attachment_status: InboundMediaModelAttachmentStatus::PromptOnly,
                    warnings: vec!["file_id=secret".to_string()],
                    ..InboundMediaArtifact::default()
                }],
                requested_agent_id: Some("main".to_string()),
                session_hint: None,
                skill_limit: 3,
            },
        )
        .unwrap();
        let step = build_channel_step(&registry, &turn);
        enqueue_channel_step(
            &step,
            RuntimeQueueEnqueueOptions {
                harness_home: harness_home.clone(),
                runtime_workspace: None,
                inbound_canonical_id: None,
                now_ms: 1234,
            },
        )
        .unwrap();

        let report = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();

        let item = report.item.unwrap();
        assert_eq!(item.inbound_media_artifacts.len(), 1);
        assert_eq!(
            item.inbound_media_artifacts[0].sha256.as_deref(),
            Some("abc123")
        );
        let bundle_json: Value =
            serde_json::from_slice(&fs::read(&item.prompt_bundle_json).unwrap()).unwrap();
        assert_eq!(bundle_json["summary"]["inboundMediaSectionsIncluded"], 1);
        let prompt_markdown = fs::read_to_string(item.prompt_markdown).unwrap();
        assert!(prompt_markdown.contains("## InboundMedia: Telegram attachments"));
        assert!(
            prompt_markdown
                .contains("localPath=state/channels/telegram-attachments/turn-1234/0.jpg")
        );
        assert!(
            prompt_markdown.contains("artifactUri=agent-harness://inbound-media/turn-1234/0.jpg")
        );
        assert!(prompt_markdown.contains("mime=image/jpeg"));
        assert!(prompt_markdown.contains("sha256=abc123"));
        assert!(prompt_markdown.contains("width=961"));
        assert!(prompt_markdown.contains("height=1280"));
        assert!(prompt_markdown.contains("downloadStatus=downloaded"));
        assert!(prompt_markdown.contains("modelAttachmentStatus=vision-tool-available"));
        assert!(prompt_markdown.contains("artifactClass=image"));
        assert!(prompt_markdown.contains("lifecycleStatus=summarized"));
        assert!(prompt_markdown.contains("extractionSummary=Reference image extracted"));
        assert!(prompt_markdown.contains("captionPreview=redacted-artifact-payload"));
        assert!(!prompt_markdown.contains("file_id=secret"));
        assert!(!prompt_markdown.contains("botTOKEN"));
        assert!(!prompt_markdown.contains("api.telegram.org/file"));
        assert!(!prompt_markdown.contains("data:image"));
        assert!(!prompt_markdown.contains("base64"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prepare_runtime_queue_item_uses_imported_prompt_workspace_when_runtime_workspace_drifts() {
        let root = temp_root(
            "prepare_runtime_queue_item_uses_imported_prompt_workspace_when_runtime_workspace_drifts",
        );
        let source = write_worker_source(&root);
        let harness_home = root.join(".agent-harness");
        let drift_workspace = root.join("runtime-workspace");
        fs::create_dir_all(&drift_workspace).unwrap();

        let registry = load_agent_registry(&source).unwrap();
        let skills = build_source_skill_index(&source).unwrap();
        let turn = build_turn_plan(
            &source,
            &registry,
            &skills,
            TurnPlanInput {
                harness_home: None,
                platform: "telegram".to_string(),
                channel_id: "dm-42".to_string(),
                user_id: "user-7".to_string(),
                text: "repair memory cron".to_string(),
                inbound_context: None,
                inbound_media_artifacts: Vec::new(),
                requested_agent_id: Some("main".to_string()),
                session_hint: None,
                skill_limit: 3,
            },
        )
        .unwrap();
        let mut step = build_channel_step(&registry, &turn);
        step.source_workspace = drift_workspace.clone();
        enqueue_channel_step(
            &step,
            RuntimeQueueEnqueueOptions {
                harness_home: harness_home.clone(),
                runtime_workspace: Some(drift_workspace.clone()),
                inbound_canonical_id: None,
                now_ms: 1234,
            },
        )
        .unwrap();

        let report = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();

        assert!(
            report
                .warnings
                .iter()
                .any(|warning| warning.contains("using imported prompt workspace"))
        );
        assert_eq!(
            report.receipt.runtime_workspace.as_deref(),
            Some(drift_workspace.as_path())
        );
        let item = report.item.unwrap();
        let bundle_json: Value =
            serde_json::from_slice(&fs::read(item.prompt_bundle_json).unwrap()).unwrap();
        assert_eq!(
            bundle_json["sourceWorkspace"].as_str(),
            Some(source.workspace.to_string_lossy().as_ref())
        );
        assert_eq!(bundle_json["summary"]["promptFilesIncluded"], 1);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prepare_runtime_queue_item_reports_no_pending_item() {
        let root = temp_root("prepare_runtime_queue_item_reports_no_pending_item");
        let harness_home = root.join(".agent-harness");

        let report = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home,
            queue_id: Some("missing".to_string()),
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();

        assert!(report.item.is_none());
        assert_eq!(
            report.receipt.status,
            RuntimeExecutionReceiptStatus::NoPendingItem
        );
        assert!(report.execution_receipts_file.is_file());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prepare_runtime_queue_item_respects_agent_channel_lease_limit() {
        let root = temp_root("prepare_runtime_queue_item_respects_agent_channel_lease_limit");
        let source = write_worker_source(&root);
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            r#"{"workerDispatch":{"globalConcurrencyLimit":2,"groupConcurrencyLimit":2,"channelConcurrencyLimit":1,"laneConcurrencyLimits":{"llm":2}}}"#,
        )
        .unwrap();
        enqueue_fixture_turn_with_session(
            &source,
            &harness_home,
            "telegram",
            "dm-42",
            "user-7",
            "session-a",
            "first turn",
            1234,
        );
        enqueue_fixture_turn_with_session(
            &source,
            &harness_home,
            "telegram",
            "dm-42",
            "user-7",
            "session-b",
            "second turn",
            1235,
        );

        let first = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();
        assert_eq!(
            first.receipt.status,
            RuntimeExecutionReceiptStatus::Prepared
        );
        let first_queue_id = first.receipt.queue_id.clone().unwrap();

        let blocked = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();
        assert_eq!(
            blocked.receipt.status,
            RuntimeExecutionReceiptStatus::NoPendingItem
        );
        assert!(
            blocked
                .warnings
                .iter()
                .any(|warning| warning.contains("agent-channel limit"))
        );

        let run_once_receipts = harness_home
            .join("state")
            .join("runtime-queue")
            .join("run-once-receipts.jsonl");
        let mut run_once_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&run_once_receipts)
            .unwrap();
        writeln!(
            run_once_file,
            "{}",
            serde_json::json!({
                "queueId": first_queue_id,
                "status": "completed",
                "reason": "test terminal receipt"
            })
        )
        .unwrap();
        release_runtime_queue_lease(&harness_home, &first_queue_id).unwrap();
        let second = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();
        assert_eq!(
            second.receipt.status,
            RuntimeExecutionReceiptStatus::Prepared
        );
        assert_ne!(second.receipt.queue_id, Some(first_queue_id));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prepare_runtime_queue_item_serializes_same_channel_session_even_with_channel_capacity() {
        let root = temp_root(
            "prepare_runtime_queue_item_serializes_same_channel_session_even_with_channel_capacity",
        );
        let source = write_worker_source(&root);
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            r#"{"workerDispatch":{"globalConcurrencyLimit":3,"groupConcurrencyLimit":3,"channelConcurrencyLimit":3,"laneConcurrencyLimits":{"llm":3}}}"#,
        )
        .unwrap();
        enqueue_fixture_turn_with_text(&source, &harness_home, "first turn", 1234);
        enqueue_fixture_turn_with_text(&source, &harness_home, "second turn", 1235);

        let first = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();
        assert_eq!(
            first.receipt.status,
            RuntimeExecutionReceiptStatus::Prepared
        );
        let first_queue_id = first.receipt.queue_id.clone().unwrap();

        let blocked = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();
        assert_eq!(
            blocked.receipt.status,
            RuntimeExecutionReceiptStatus::NoPendingItem
        );
        assert!(
            blocked
                .warnings
                .iter()
                .any(|warning| warning.contains("session-active"))
        );

        append_json_line(
            &queue_dir(&harness_home).join("run-once-receipts.jsonl"),
            &serde_json::json!({
                "queueId": first_queue_id,
                "status": "completed",
                "reason": "test terminal receipt"
            }),
        )
        .unwrap();
        release_runtime_queue_lease(&harness_home, &first_queue_id).unwrap();

        let second = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();
        assert_eq!(
            second.receipt.status,
            RuntimeExecutionReceiptStatus::Prepared
        );
        assert_ne!(
            second.receipt.queue_id.as_deref(),
            Some(first_queue_id.as_str())
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prepare_runtime_queue_item_allows_different_sessions_when_channel_capacity_allows() {
        let root = temp_root(
            "prepare_runtime_queue_item_allows_different_sessions_when_channel_capacity_allows",
        );
        let source = write_worker_source(&root);
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            r#"{"workerDispatch":{"globalConcurrencyLimit":3,"groupConcurrencyLimit":3,"channelConcurrencyLimit":3,"laneConcurrencyLimits":{"llm":3}}}"#,
        )
        .unwrap();
        enqueue_fixture_turn_with_session(
            &source,
            &harness_home,
            "telegram",
            "dm-42",
            "user-7",
            "session-a",
            "first turn",
            1234,
        );
        enqueue_fixture_turn_with_session(
            &source,
            &harness_home,
            "telegram",
            "dm-42",
            "user-7",
            "session-b",
            "second turn",
            1235,
        );

        let first = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();
        let second = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();

        assert_eq!(
            first.receipt.status,
            RuntimeExecutionReceiptStatus::Prepared
        );
        assert_eq!(
            second.receipt.status,
            RuntimeExecutionReceiptStatus::Prepared
        );
        assert_ne!(first.receipt.queue_id, second.receipt.queue_id);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn explicit_runtime_queue_id_cannot_overtake_older_same_session_turn() {
        let root = temp_root("explicit_runtime_queue_id_cannot_overtake_older_same_session_turn");
        let source = write_worker_source(&root);
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            r#"{"workerDispatch":{"globalConcurrencyLimit":3,"groupConcurrencyLimit":3,"channelConcurrencyLimit":3,"laneConcurrencyLimits":{"llm":3}}}"#,
        )
        .unwrap();
        enqueue_fixture_turn_with_text(&source, &harness_home, "first turn", 1234);
        enqueue_fixture_turn_with_text(&source, &harness_home, "second turn", 1235);
        let pending_text =
            fs::read_to_string(queue_dir(&harness_home).join("pending.jsonl")).unwrap();
        let queue_ids = pending_text
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .map(|value| value["queueId"].as_str().unwrap().to_string())
            .collect::<Vec<_>>();
        assert_eq!(queue_ids.len(), 2);

        let blocked = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: Some(queue_ids[1].clone()),
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();
        assert!(blocked.item.is_none());
        assert_eq!(
            blocked.receipt.status,
            RuntimeExecutionReceiptStatus::NoPendingItem
        );
        assert!(
            blocked
                .warnings
                .iter()
                .any(|warning| warning.contains("session-fifo"))
        );

        let first = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();
        assert_eq!(
            first.receipt.queue_id.as_deref(),
            Some(queue_ids[0].as_str())
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn queue_terminal_control_preserves_continuation_disposition() {
        let root = temp_root("queue_terminal_control_preserves_continuation_disposition");
        let harness_home = root.join(".agent-harness");
        let queue_dir = queue_dir(&harness_home);
        fs::create_dir_all(&queue_dir).unwrap();
        fs::write(
            queue_dir.join("run-once-receipts.jsonl"),
            r#"{"queueId":"parent","status":"skipped","reason":"continued","terminalDisposition":"continuation-handoff","continuationLink":{"parentQueueId":"parent","childQueueId":"child","continuationIndex":1}}
"#,
        )
        .unwrap();
        rebuild_runtime_queue_state_index(&queue_dir, &mut Vec::new()).unwrap();

        let QueueTerminalControl::Terminal(control) =
            resolve_queue_terminal_control(&harness_home, "parent", None).unwrap()
        else {
            panic!("typed skipped receipt must be terminal queue control");
        };
        assert_eq!(
            control.terminal_disposition,
            Some(crate::RuntimeTerminalDispositionV1::ContinuationHandoff)
        );
        assert_eq!(
            control
                .continuation_link
                .as_ref()
                .map(|link| link.child_queue_id.as_str()),
            Some("child")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn canonical_continuation_followup_cannot_bypass_active_limit_or_fifo() {
        let root = temp_root("canonical_continuation_followup_cannot_bypass_active_limit_or_fifo");
        let source = write_worker_source(&root);
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            r#"{"workerDispatch":{"globalConcurrencyLimit":3,"groupConcurrencyLimit":3,"channelConcurrencyLimit":3,"laneConcurrencyLimits":{"llm":3}}}"#,
        )
        .unwrap();

        let bound = crate::channel_session_key::CanonicalChannelSessionKey::bind_for_lane(
            "synthetic-root",
            "telegram",
            "account-a",
            "dm-42",
            "user-7",
            "main",
        )
        .unwrap();
        let continuation = bound.continuation(1).unwrap().canonical_string();
        let legacy_duplicate = format!(
            "{continuation}:{}",
            crate::channel_session_key::expected_account_binding(
                "telegram",
                "account-a",
                "dm-42",
                "user-7",
                "main",
            )
        );
        enqueue_fixture_turn_for_account_with_session(
            &source,
            &harness_home,
            "account-a",
            Some(&continuation),
            "continuation",
            1234,
        );
        enqueue_fixture_turn_for_account_with_session(
            &source,
            &harness_home,
            "account-a",
            Some(&legacy_duplicate),
            "ordinary follow-up",
            1235,
        );

        let pending = fs::read_to_string(queue_dir(&harness_home).join("pending.jsonl")).unwrap();
        let rows = pending
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["sessionKey"], continuation);
        assert_eq!(rows[1]["sessionKey"], continuation);

        let first = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();
        let first_queue_id = first.receipt.queue_id.clone().unwrap();
        assert_eq!(
            first.receipt.status,
            RuntimeExecutionReceiptStatus::Prepared
        );

        let blocked = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();
        assert!(blocked.item.is_none());
        assert!(
            blocked
                .warnings
                .iter()
                .any(|warning| warning.contains("session-active"))
        );

        release_runtime_queue_lease(&harness_home, &first_queue_id).unwrap();
        fs::write(
            queue_dir(&harness_home).join("run-once-receipts.jsonl"),
            format!(
                "{}\n",
                serde_json::json!({
                    "queueId": first_queue_id,
                    "status": "completed",
                    "reason": "synthetic continuation completed"
                })
            ),
        )
        .unwrap();

        let follow_up = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();
        assert_eq!(
            follow_up.receipt.status,
            RuntimeExecutionReceiptStatus::Prepared
        );
        assert_ne!(follow_up.receipt.queue_id, Some(first_queue_id));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn lease_reconcile_groups_legacy_duplicate_key_under_one_canonical_lane() {
        let root =
            temp_root("lease_reconcile_groups_legacy_duplicate_key_under_one_canonical_lane");
        let queue_dir = root.join("state").join("runtime-queue");
        fs::create_dir_all(&queue_dir).unwrap();
        let receipts_file = queue_dir.join("execution-receipts.jsonl");
        let bound = crate::channel_session_key::CanonicalChannelSessionKey::bind_for_lane(
            "synthetic-root",
            "telegram",
            "account-a",
            "channel-a",
            "user-a",
            "main",
        )
        .unwrap();
        let continuation = bound.continuation(1).unwrap().canonical_string();
        let duplicate = format!(
            "{continuation}:{}",
            crate::channel_session_key::expected_account_binding(
                "telegram",
                "account-a",
                "channel-a",
                "user-a",
                "main",
            )
        );
        let lease = |queue_id: &str, session_key: String, started_at_ms: i64| RuntimeQueueLease {
            queue_id: queue_id.to_string(),
            agent_id: "main".to_string(),
            runtime_class: "interactive".to_string(),
            origin: "channel".to_string(),
            cron_run_id: None,
            platform: "telegram".to_string(),
            account_id: Some("account-a".to_string()),
            channel_id: "channel-a".to_string(),
            user_id: Some("user-a".to_string()),
            session_key,
            virtual_session_id: None,
            session_lane_key: None,
            owner: RuntimeQueueLeaseOwner::Legacy("synthetic-owner".to_string()),
            started_at_ms,
            lease_expires_at_ms: 10_000,
        };
        let mut state = RuntimeQueueLeaseState {
            schema: runtime_queue_leases_schema(),
            leases: BTreeMap::from([
                (
                    "queue-first".to_string(),
                    lease("queue-first", continuation, 1_000),
                ),
                (
                    "queue-duplicate".to_string(),
                    lease("queue-duplicate", duplicate, 1_001),
                ),
            ]),
        };

        let mut warnings = Vec::new();
        purge_runtime_queue_leases(
            &mut state,
            2_000,
            &HashSet::new(),
            &HashSet::new(),
            Some(&receipts_file),
            &mut warnings,
        )
        .unwrap();

        assert_eq!(state.leases.len(), 1);
        assert!(state.leases.contains_key("queue-first"));
        assert!(
            warnings
                .iter()
                .any(|warning| warning.contains("duplicate canonical active lease reconciled"))
        );
        let receipts = fs::read_to_string(receipts_file).unwrap();
        assert!(receipts.contains("canonical-collision-reconciled"));
        assert!(receipts.contains("canonicalLaneDigest"));
        assert!(!receipts.contains("synthetic-root"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn session_identity_inventory_is_read_only_and_privacy_safe() {
        let root = temp_root("session_identity_inventory_is_read_only_and_privacy_safe");
        let harness_home = root.join(".agent-harness");
        let queue_dir = queue_dir(&harness_home);
        fs::create_dir_all(&queue_dir).unwrap();
        let bound = crate::channel_session_key::CanonicalChannelSessionKey::bind_for_lane(
            "private-synthetic-root",
            "telegram",
            "account-a",
            "channel-a",
            "user-a",
            "main",
        )
        .unwrap();
        let canonical = bound.continuation(1).unwrap().canonical_string();
        let duplicate = format!(
            "{canonical}:{}",
            crate::channel_session_key::expected_account_binding(
                "telegram",
                "account-a",
                "channel-a",
                "user-a",
                "main",
            )
        );
        let pending_file = queue_dir.join("pending.jsonl");
        append_json_line(
            &pending_file,
            &serde_json::json!({
                "queueId": "queue-pending",
                "status": "queued",
                "createdAtMs": 1,
                "agentId": "main",
                "sessionKey": "private-synthetic-root",
                "runtimeClass": "interactive",
                "origin": "channel",
                "platform": "telegram",
                "accountId": "account-a",
                "channelId": "channel-a",
                "userId": "user-a",
                "messageText": "synthetic",
                "source": {
                    "sourceHome": ".",
                    "sourceWorkspace": "."
                },
                "plannedTranscriptFile": "transcript.jsonl",
                "plannedTrajectoryFile": "trajectory.jsonl"
            }),
        )
        .unwrap();
        let lease = |queue_id: &str, session_key: String| RuntimeQueueLease {
            queue_id: queue_id.to_string(),
            agent_id: "main".to_string(),
            runtime_class: "interactive".to_string(),
            origin: "channel".to_string(),
            cron_run_id: None,
            platform: "telegram".to_string(),
            account_id: Some("account-a".to_string()),
            channel_id: "channel-a".to_string(),
            user_id: Some("user-a".to_string()),
            session_key,
            virtual_session_id: None,
            session_lane_key: None,
            owner: RuntimeQueueLeaseOwner::Legacy("synthetic-owner".to_string()),
            started_at_ms: 1,
            lease_expires_at_ms: 10_000,
        };
        let lease_state = RuntimeQueueLeaseState {
            schema: runtime_queue_leases_schema(),
            leases: BTreeMap::from([
                ("queue-a".to_string(), lease("queue-a", canonical)),
                ("queue-b".to_string(), lease("queue-b", duplicate)),
            ]),
        };
        write_runtime_queue_leases(&queue_dir, "interactive", &lease_state).unwrap();
        let pending_before = fs::read(&pending_file).unwrap();
        let leases_file = runtime_queue_leases_file(&queue_dir, "interactive");
        let leases_before = fs::read(&leases_file).unwrap();

        let report = inspect_runtime_session_identity(&harness_home).unwrap();
        assert_eq!(report.collision_groups.len(), 1);
        assert!(
            report.entries.iter().any(|entry| {
                entry.queue_id == "queue-pending"
                    && entry.status == RuntimeSessionIdentityInventoryStatus::NeedsNormalization
            }),
            "inventory entries: {:?}; warnings: {:?}",
            report.entries,
            report.warnings
        );
        let serialized = serde_json::to_string(&report).unwrap();
        assert!(!serialized.contains("private-synthetic-root"));
        assert_eq!(fs::read(&pending_file).unwrap(), pending_before);
        assert_eq!(fs::read(&leases_file).unwrap(), leases_before);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn pending_session_identity_normalizes_before_dispatch_and_mismatch_fails_closed() {
        let root = temp_root(
            "pending_session_identity_normalizes_before_dispatch_and_mismatch_fails_closed",
        );
        let harness_home = root.join(".agent-harness");
        let queue_dir = queue_dir(&harness_home);
        fs::create_dir_all(&queue_dir).unwrap();
        let mut value = queued_item_value("queue-normalize");
        let object = value.as_object_mut().unwrap();
        object.insert(
            "platform".to_string(),
            Value::String("telegram".to_string()),
        );
        object.insert(
            "accountId".to_string(),
            Value::String("account-a".to_string()),
        );
        object.insert(
            "channelId".to_string(),
            Value::String("channel-a".to_string()),
        );
        object.insert("userId".to_string(), Value::String("user-a".to_string()));
        object.insert("agentId".to_string(), Value::String("main".to_string()));
        object.insert(
            "sessionKey".to_string(),
            Value::String("synthetic-root".to_string()),
        );
        let valid = parse_pending_item(&value).unwrap();
        let mut invalid_value = value;
        invalid_value["queueId"] = Value::String("queue-invalid".to_string());
        invalid_value["sessionKey"] = Value::String("synthetic-root:acct-other".to_string());
        let invalid = parse_pending_item(&invalid_value).unwrap();
        let execution_receipts_file = queue_dir.join("execution-receipts.jsonl");
        let mut items = vec![valid, invalid];
        let mut warnings = Vec::new();

        canonicalize_pending_items_for_dispatch(
            &harness_home,
            &execution_receipts_file,
            &mut items,
            2_000,
            &mut warnings,
        )
        .unwrap();

        assert_eq!(items.len(), 1);
        assert_eq!(items[0].queue_id, "queue-normalize");
        assert!(items[0].session_key.contains(":acct-"));
        let receipts = fs::read_to_string(execution_receipts_file).unwrap();
        assert!(receipts.contains("session-identity-normalized"));
        assert!(receipts.contains("invalid-canonical-lane-quarantined"));
        assert!(!receipts.contains("synthetic-root"));
        assert!(matches!(
            resolve_queue_terminal_control(
                &harness_home,
                "queue-invalid",
                Some("synthetic-root:acct-other")
            )
            .unwrap(),
            QueueTerminalControl::Terminal(_)
        ));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn worker_lane_is_not_collapsed_by_interactive_session_mutex() {
        let root = temp_root("worker_lane_is_not_collapsed_by_interactive_session_mutex");
        let source = write_worker_source(&root);
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            r#"{"workerDispatch":{"globalConcurrencyLimit":3,"groupConcurrencyLimit":3,"channelConcurrencyLimit":3,"laneConcurrencyLimits":{"llm":3}},"runtimeDispatch":{"interactiveReserve":0}}"#,
        )
        .unwrap();
        append_worker_pending_runtime_item(&source, &harness_home, "turn:worker:one", 1234);
        append_worker_pending_runtime_item(&source, &harness_home, "turn:worker:two", 1235);

        let first = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();
        let second = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();

        assert_eq!(first.receipt.runtime_class.as_deref(), Some("worker"));
        assert_eq!(second.receipt.runtime_class.as_deref(), Some("worker"));
        assert_eq!(
            first.receipt.status,
            RuntimeExecutionReceiptStatus::Prepared
        );
        assert_eq!(
            second.receipt.status,
            RuntimeExecutionReceiptStatus::Prepared
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn read_pending_items_materializes_the_bounded_active_pending_index() {
        let root = temp_root("read_pending_items_materializes_active_pending_index");
        let source = write_worker_source(&root);
        let harness_home = root.join(".agent-harness");
        let queue_dir = queue_dir(&harness_home);
        fs::create_dir_all(&queue_dir).unwrap();
        append_worker_pending_runtime_item(&source, &harness_home, "turn:pending:indexed", 1234);

        let mut warnings = Vec::new();
        let items = read_pending_items(&queue_dir.join("pending.jsonl"), &mut warnings).unwrap();

        assert_eq!(items.len(), 1);
        assert_eq!(items[0].queue_id, "turn:pending:indexed");
        assert!(
            crate::runtime_pending_index::runtime_pending_index_file(&queue_dir).is_file(),
            "normal worker selection must use the bounded active pending sidecar rather than replaying pending.jsonl"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn typing_context_uses_the_nonblocking_pending_projection() {
        let root = temp_root("typing_context_uses_nonblocking_pending_projection");
        let source = write_worker_source(&root);
        let harness_home = root.join(".agent-harness");
        let queue_dir = queue_dir(&harness_home);
        fs::create_dir_all(&queue_dir).unwrap();
        append_worker_pending_runtime_item(&source, &harness_home, "turn:typing:indexed", 1234);

        let context = resolve_runtime_queue_typing_context_nonblocking(
            &harness_home,
            Some("turn:typing:indexed"),
        )
        .unwrap()
        .expect("queued item should provide typing routing");

        assert!(!context.agent_id.is_empty());
        assert!(!context.platform.is_empty());
        assert!(!context.channel_id.is_empty());
        assert!(
            crate::runtime_pending_index::runtime_pending_index_file(&queue_dir).is_file(),
            "typing must materialize/read the bounded pending projection rather than replaying pending.jsonl"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn terminal_lookup_uses_committed_index_after_one_hundred_thousand_runtime_receipts() {
        let root = temp_root(
            "terminal_lookup_uses_committed_index_after_one_hundred_thousand_runtime_receipts",
        );
        let harness_home = root.join(".agent-harness");
        let queue_dir = queue_dir(&harness_home);
        fs::create_dir_all(&queue_dir).unwrap();
        let historical =
            "{\"queueId\":\"turn:historic\",\"status\":\"completed\",\"reason\":\"historic\"}\n";
        let mut receipts = historical.repeat(100_000);
        receipts.push_str(
            "{\"queueId\":\"turn:target\",\"status\":\"completed\",\"reason\":\"target\"}\n",
        );
        fs::write(queue_dir.join("run-once-receipts.jsonl"), receipts).unwrap();

        refresh_runtime_queue_state_index(&queue_dir, &mut Vec::new()).unwrap();
        let candidates = std::collections::BTreeSet::from(["turn:target".to_string()]);
        let started = Instant::now();
        let terminal =
            resolve_runtime_queue_terminal_ids_nonblocking(&harness_home, &candidates).unwrap();

        assert_eq!(terminal, candidates);
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "the current runtime receipt index must answer an exact terminal lookup without replaying 100k historical rows"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn terminal_lookup_uses_last_committed_index_while_runtime_receipt_append_is_locked() {
        let root = temp_root(
            "terminal_lookup_uses_last_committed_index_while_runtime_receipt_append_is_locked",
        );
        let harness_home = root.join(".agent-harness");
        let queue_dir = queue_dir(&harness_home);
        fs::create_dir_all(&queue_dir).unwrap();
        let receipts_file = queue_dir.join("run-once-receipts.jsonl");
        append_json_line(
            &receipts_file,
            &serde_json::json!({
                "queueId": "turn:committed",
                "status": "completed",
                "reason": "committed before lock"
            }),
        )
        .unwrap();
        refresh_runtime_queue_state_index(&queue_dir, &mut Vec::new()).unwrap();
        let committed = std::collections::BTreeSet::from(["turn:committed".to_string()]);

        crate::logging::with_jsonl_append_lock(&receipts_file, || {
            let mut file = OpenOptions::new().append(true).open(&receipts_file)?;
            writeln!(
                file,
                "{}",
                serde_json::json!({
                    "queueId": "turn:locked",
                    "status": "completed",
                    "reason": "not committed to the sidecar yet"
                })
            )?;
            file.flush()?;

            let started = Instant::now();
            let observed =
                resolve_runtime_queue_terminal_ids_nonblocking(&harness_home, &committed)?;
            assert_eq!(observed, committed);
            assert!(
                started.elapsed() < Duration::from_secs(2),
                "a live runtime receipt append must return the committed snapshot rather than wait"
            );
            Ok(())
        })
        .unwrap();

        let locked = std::collections::BTreeSet::from(["turn:locked".to_string()]);
        assert_eq!(
            resolve_runtime_queue_terminal_ids_nonblocking(&harness_home, &locked).unwrap(),
            locked
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prepare_runtime_queue_item_selects_cron_when_old_interactive_is_terminal() {
        let root =
            temp_root("prepare_runtime_queue_item_selects_cron_when_old_interactive_is_terminal");
        let source = write_worker_source(&root);
        let harness_home = root.join(".agent-harness");
        enqueue_fixture_turn_with_text(&source, &harness_home, "old interactive turn", 1000);
        let queue_dir = harness_home.join("state").join("runtime-queue");
        let pending_text = fs::read_to_string(queue_dir.join("pending.jsonl")).unwrap();
        let interactive_item: Value =
            serde_json::from_str(pending_text.lines().next().unwrap()).unwrap();
        let interactive_queue_id = interactive_item["queueId"].as_str().unwrap().to_string();
        append_json_line(
            &queue_dir.join("run-once-receipts.jsonl"),
            &serde_json::json!({
                "queueId": interactive_queue_id,
                "status": "completed",
                "reason": "old interactive turn completed"
            }),
        )
        .unwrap();

        let cron_queue_id = "turn:cron:daily:1001";
        let cron_run = crate::admit_cron_run(crate::CronRunAdmitOptions {
            harness_home: harness_home.clone(),
            source_kind: "native-cron".to_string(),
            source_id: "source-1".to_string(),
            entry_id: "daily".to_string(),
            agent_id: "main".to_string(),
            scheduled_for_ms: 1001,
            runtime_class: "cron".to_string(),
            session_key: "cron:main:daily:1001".to_string(),
            session_policy: "one-shot".to_string(),
            max_attempts: 3,
            now_ms: 1001,
        })
        .unwrap();
        crate::mark_cron_run_runtime_enqueued(&harness_home, &cron_run.run_id, cron_queue_id, 1002)
            .unwrap();
        append_cron_pending_runtime_item(
            &source,
            &harness_home,
            cron_queue_id,
            &cron_run.run_id,
            "main",
            1001,
        );

        let report = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();

        assert_eq!(
            report.receipt.status,
            RuntimeExecutionReceiptStatus::Prepared
        );
        assert_eq!(report.receipt.queue_id.as_deref(), Some(cron_queue_id));
        assert_eq!(report.receipt.runtime_class.as_deref(), Some("cron"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn queue_skip_receipt_is_sticky_terminal_after_later_non_terminal_receipt() {
        let root = temp_root("queue_skip_receipt_is_sticky_terminal");
        let source = write_worker_source(&root);
        let harness_home = root.join(".agent-harness");
        enqueue_fixture_turn_with_text(&source, &harness_home, "stale duplicate turn", 1000);
        let queue_dir = queue_dir(&harness_home);
        let pending_text = fs::read_to_string(queue_dir.join("pending.jsonl")).unwrap();
        let item: Value = serde_json::from_str(pending_text.lines().next().unwrap()).unwrap();
        let queue_id = item["queueId"].as_str().unwrap().to_string();

        append_json_line(
            &queue_dir.join("run-once-receipts.jsonl"),
            &serde_json::json!({
                "queueId": queue_id,
                "status": "skipped",
                "reason": "operator skipped stale duplicate"
            }),
        )
        .unwrap();
        append_json_line(
            &queue_dir.join("run-once-receipts.jsonl"),
            &serde_json::json!({
                "queueId": queue_id,
                "status": "no-prepared-execution",
                "reason": "late churn after operator skip"
            }),
        )
        .unwrap();

        let report = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: Some(queue_id.clone()),
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();

        assert!(report.item.is_none());
        assert_eq!(
            report.receipt.status,
            RuntimeExecutionReceiptStatus::NoPendingItem
        );
        assert!(
            report.receipt.reason.contains("terminal control")
                || report.receipt.reason.contains("terminal run receipt"),
            "{}",
            report.receipt.reason
        );
        let leases = read_runtime_queue_leases(&queue_dir, "interactive", &mut Vec::new()).unwrap();
        assert!(!leases.leases.contains_key(&queue_id));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn queue_skip_receipt_is_sticky_terminal() {
        queue_skip_receipt_is_sticky_terminal_after_later_non_terminal_receipt();
    }

    #[test]
    fn prepared_queue_skip_control_blocks_explicit_resume() {
        let root = temp_root("prepared_queue_skip_control_blocks_explicit_resume");
        let source = write_worker_source(&root);
        let harness_home = root.join(".agent-harness");
        enqueue_fixture_turn(&source, &harness_home);
        let queue_dir = queue_dir(&harness_home);

        let prepared = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();
        let queue_id = prepared.receipt.queue_id.clone().unwrap();
        assert_eq!(
            prepared.receipt.status,
            RuntimeExecutionReceiptStatus::Prepared
        );
        release_runtime_queue_lease(&harness_home, &queue_id).unwrap();
        append_json_line(
            &queue_dir.join("control-receipts.jsonl"),
            &serde_json::json!({
                "schema": "agent-harness.runtime-queue-control.v1",
                "action": "skip",
                "status": "skipped",
                "originalQueueId": queue_id,
                "reason": "manual operator request"
            }),
        )
        .unwrap();
        append_json_line(
            &queue_dir.join("run-once-receipts.jsonl"),
            &serde_json::json!({
                "queueId": queue_id,
                "status": "no-prepared-execution",
                "reason": "late no-prepared churn after skip"
            }),
        )
        .unwrap();

        let report = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: Some(queue_id.clone()),
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();

        assert!(report.item.is_none());
        assert_eq!(
            report.receipt.status,
            RuntimeExecutionReceiptStatus::NoPendingItem
        );
        assert_eq!(report.receipt.terminal_control_matched, Some(true));
        assert_eq!(
            report.receipt.terminal_control_source.as_deref(),
            Some(TerminalControlSource::QueueSkip.as_str())
        );
        let run_once_receipts =
            fs::read_to_string(queue_dir.join("run-once-receipts.jsonl")).unwrap();
        assert_eq!(
            count_run_once_status(&run_once_receipts, &queue_id, "suppressed"),
            1
        );
        let execution_receipts =
            fs::read_to_string(queue_dir.join("execution-receipts.jsonl")).unwrap();
        assert!(
            !execution_receipts
                .contains("runtime queue lease acquired for requested prepared item resume")
        );
        let leases = read_runtime_queue_leases(&queue_dir, "interactive", &mut Vec::new()).unwrap();
        assert!(!leases.leases.contains_key(&queue_id));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn scoped_stop_marker_blocks_selection_prepare_and_lease() {
        let root = temp_root("scoped_stop_marker_blocks_selection_prepare_and_lease");
        let source = write_worker_source(&root);
        let harness_home = root.join(".agent-harness");
        enqueue_fixture_turn_with_session(
            &source,
            &harness_home,
            "telegram",
            "dm-42",
            "user-7",
            "telegram:dm-42:user-7:main",
            "stop this queued turn",
            1000,
        );
        let queue_dir = queue_dir(&harness_home);
        let pending_text = fs::read_to_string(queue_dir.join("pending.jsonl")).unwrap();
        let item: Value = serde_json::from_str(pending_text.lines().next().unwrap()).unwrap();
        let queue_id = item["queueId"].as_str().unwrap().to_string();
        record_scoped_stop(ScopedStopOptions {
            harness_home: harness_home.clone(),
            target: ScopedStopTarget::QueueItem {
                queue_id: queue_id.clone(),
            },
            reason: "operator stopped stale queued turn".to_string(),
            now_ms: 1100,
        })
        .unwrap();

        let report = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();

        assert!(report.item.is_none());
        assert_eq!(
            report.receipt.status,
            RuntimeExecutionReceiptStatus::NoPendingItem
        );
        assert!(report.receipt.reason.contains("terminal control"));
        let leases = read_runtime_queue_leases(&queue_dir, "interactive", &mut Vec::new()).unwrap();
        assert!(!leases.leases.contains_key(&queue_id));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn suppressed_receipt_emitted_at_most_once() {
        let root = temp_root("suppressed_receipt_emitted_at_most_once");
        let source = write_worker_source(&root);
        let harness_home = root.join(".agent-harness");
        enqueue_fixture_turn_with_text(&source, &harness_home, "stale duplicate turn", 1000);
        let queue_dir = queue_dir(&harness_home);
        let pending_text = fs::read_to_string(queue_dir.join("pending.jsonl")).unwrap();
        let item: Value = serde_json::from_str(pending_text.lines().next().unwrap()).unwrap();
        let queue_id = item["queueId"].as_str().unwrap().to_string();
        record_scoped_stop(ScopedStopOptions {
            harness_home: harness_home.clone(),
            target: ScopedStopTarget::QueueItem {
                queue_id: queue_id.clone(),
            },
            reason: "operator stopped stale queued turn".to_string(),
            now_ms: 1100,
        })
        .unwrap();

        for _ in 0..3 {
            let report = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
                harness_home: harness_home.clone(),
                queue_id: Some(queue_id.clone()),
                prompt_options: PromptAssemblyOptions::default(),
            })
            .unwrap();
            assert!(report.item.is_none());
        }

        let run_once_receipts =
            fs::read_to_string(queue_dir.join("run-once-receipts.jsonl")).unwrap();
        assert_eq!(
            count_run_once_status(&run_once_receipts, &queue_id, "suppressed"),
            1
        );
        assert!(run_once_receipts.contains(r#""terminalControlMatched":true"#));
        assert!(
            run_once_receipts.contains(r#""suppressedRunOnceReason":"terminal-control-present""#)
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn lease_reconcile_respects_terminal_controls() {
        let root = temp_root("lease_reconcile_respects_terminal_controls");
        let source = write_worker_source(&root);
        let harness_home = root.join(".agent-harness");
        enqueue_fixture_turn_with_text(&source, &harness_home, "stale leased turn", 1000);
        let queue_dir = queue_dir(&harness_home);
        let pending_text = fs::read_to_string(queue_dir.join("pending.jsonl")).unwrap();
        let item: Value = serde_json::from_str(pending_text.lines().next().unwrap()).unwrap();
        let queue_id = item["queueId"].as_str().unwrap().to_string();
        let generation_id = "runtime-loop-supervised-terminal-control-test";
        let now_ms = current_log_time_ms().unwrap();
        let mut lease_state = RuntimeQueueLeaseState {
            schema: runtime_queue_leases_schema(),
            leases: BTreeMap::new(),
        };
        lease_state.leases.insert(
            queue_id.clone(),
            RuntimeQueueLease {
                queue_id: queue_id.clone(),
                agent_id: "main".to_string(),
                runtime_class: "interactive".to_string(),
                origin: "channel".to_string(),
                cron_run_id: None,
                platform: "telegram".to_string(),
                account_id: None,
                channel_id: "dm-42".to_string(),
                user_id: Some("user-7".to_string()),
                session_key: "telegram:dm-42:user-7:main".to_string(),
                virtual_session_id: None,
                session_lane_key: None,
                owner: RuntimeQueueLeaseOwner::Envelope(RuntimeQueueLeaseOwnerEnvelope {
                    kind: "supervisor-child".to_string(),
                    service_id: RUNTIME_LOOP_SERVICE_ID.to_string(),
                    generation_id: generation_id.to_string(),
                    pid: 12_345,
                    process_start_time_ms: now_ms.saturating_sub(1_000),
                    acquired_at_ms: now_ms,
                }),
                started_at_ms: now_ms,
                lease_expires_at_ms: now_ms.saturating_add(60_000),
            },
        );
        write_runtime_queue_leases(&queue_dir, "interactive", &lease_state).unwrap();
        append_json_line(
            &queue_dir.join("run-once-receipts.jsonl"),
            &serde_json::json!({
                "queueId": queue_id,
                "status": "skipped",
                "reason": "operator skipped stale leased turn"
            }),
        )
        .unwrap();

        let report = reconcile_runtime_queue_leases_for_generation(
            &harness_home,
            RUNTIME_LOOP_SERVICE_ID,
            generation_id,
            now_ms.saturating_add(10),
        )
        .unwrap();
        assert_eq!(report.reaped_leases.len(), 1);

        let prepare = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: Some(queue_id.clone()),
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();

        assert!(prepare.item.is_none());
        assert_eq!(
            prepare.receipt.status,
            RuntimeExecutionReceiptStatus::NoPendingItem
        );
        assert!(prepare.receipt.reason.contains("terminal control"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn queue_retry_of_terminal_item_creates_fresh_runnable_id_only() {
        let root = temp_root("queue_retry_of_terminal_item_creates_fresh_runnable_id_only");
        let source = write_worker_source(&root);
        let harness_home = root.join(".agent-harness");
        enqueue_fixture_turn_with_text(&source, &harness_home, "retry stale turn", 1000);
        let queue_dir = queue_dir(&harness_home);
        let pending_text = fs::read_to_string(queue_dir.join("pending.jsonl")).unwrap();
        let item: Value = serde_json::from_str(pending_text.lines().next().unwrap()).unwrap();
        let original_queue_id = item["queueId"].as_str().unwrap().to_string();
        control_runtime_queue_item(RuntimeQueueControlOptions {
            harness_home: harness_home.clone(),
            queue_id: original_queue_id.clone(),
            action: RuntimeQueueControlAction::Skip,
            reason: "operator confirmed original is stale".to_string(),
            now_ms: 1100,
        })
        .unwrap();
        let retry = control_runtime_queue_item(RuntimeQueueControlOptions {
            harness_home: harness_home.clone(),
            queue_id: original_queue_id.clone(),
            action: RuntimeQueueControlAction::Retry,
            reason: "operator requested fresh retry".to_string(),
            now_ms: 1200,
        })
        .unwrap();
        let retry_queue_id = retry.new_queue_id.unwrap();
        append_json_line(
            &queue_dir.join("run-once-receipts.jsonl"),
            &serde_json::json!({
                "queueId": original_queue_id,
                "status": "no-prepared-execution",
                "reason": "late churn after original skip"
            }),
        )
        .unwrap();

        let report = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();

        assert_eq!(
            report.receipt.status,
            RuntimeExecutionReceiptStatus::Prepared
        );
        assert_eq!(
            report.receipt.queue_id.as_deref(),
            Some(retry_queue_id.as_str())
        );
        assert_ne!(
            report.receipt.queue_id.as_deref(),
            Some(original_queue_id.as_str())
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prepare_runtime_queue_item_rekeys_pending_turn_when_rollover_is_pending() {
        let root =
            temp_root("prepare_runtime_queue_item_rekeys_pending_turn_when_rollover_pending");
        let source = write_worker_source(&root);
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            r#"{"codexContext":{"maxSuccessfulCompactsBeforeRollover":1}}"#,
        )
        .unwrap();
        let old_session = crate::channel_session_key::CanonicalChannelSessionKey::bind_for_lane(
            "telegram:dm-42:user-7:main",
            "telegram",
            "default",
            "dm-42",
            "user-7",
            "main",
        )
        .unwrap()
        .canonical_string();
        enqueue_fixture_turn_with_session(
            &source,
            &harness_home,
            "telegram",
            "dm-42",
            "user-7",
            &old_session,
            "continue after compact",
            1234,
        );
        let state_file = channel_session_state_file(&harness_home, "telegram", "dm-42", "user-7");
        fs::create_dir_all(state_file.parent().unwrap()).unwrap();
        write_json_atomic(
            &state_file,
            &ChannelSessionState {
                schema: "agent-harness.channel-session-state.v1".to_string(),
                platform: "telegram".to_string(),
                account_id: None,
                channel_id: "dm-42".to_string(),
                user_id: "user-7".to_string(),
                active_session_key: old_session.to_string(),
                agent_id: Some("main".to_string()),
                config_revision: None,
                provider: None,
                model: None,
                session_topic: None,
                model_override: None,
                model_override_provider: None,
                model_override_model: None,
                thinking_enabled: false,
                thinking_level: None,
                thinking_instruction: None,
                reasoning_preference: None,
                backend_reasoning_policy: None,
                fast_mode: None,
                stop_requested: false,
                stop_reason: None,
                steering_notes: Vec::new(),
                btw_notes: Vec::new(),
                last_command: None,
                updated_at_ms: 1234,
            },
        )
        .unwrap();
        record_context_compact_attempt(ContextCompactAttemptOptions {
            harness_home: harness_home.clone(),
            lane: ContextRolloverLane {
                runtime_class: "interactive".to_string(),
                agent_id: "main".to_string(),
                platform: "telegram".to_string(),
                channel_id: "dm-42".to_string(),
                user_id: "user-7".to_string(),
                working_session_key: old_session.to_string(),
                virtual_session_id: None,
                continuation_index: 0,
            },
            compact_succeeded: true,
            rewrote_active_context: true,
            compact_thread_id: Some("thread-after-compact".to_string()),
            compact_attempt_key: None,
            max_successful_compacts_before_rollover: 1,
            now_ms: 1235,
        })
        .unwrap();

        let report = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();

        assert_eq!(
            report.receipt.status,
            RuntimeExecutionReceiptStatus::Prepared
        );
        let item = report.item.unwrap();
        assert_eq!(
            item.session_key,
            crate::context_rollover::continuation_session_key(&old_session, 1)
        );
        assert_eq!(item.continuation.continuation_index, Some(1));
        assert_eq!(
            item.continuation.completion_kind.as_deref(),
            Some("continuation-rollover")
        );
        assert!(item.continuation.suppress_self_improvement);
        assert_eq!(report.receipt.continuation, item.continuation);
        let state = read_channel_session_state(&harness_home, "telegram", "dm-42", "user-7")
            .unwrap()
            .unwrap();
        assert_eq!(state.active_session_key, item.session_key);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn context_rollover_blocked_leased_stops_prepare_path() {
        let root = temp_root("context_rollover_blocked_leased_stops_prepare_path");
        let source = write_worker_source(&root);
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(queue_dir(&harness_home).join("classes").join("interactive")).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            r#"{"codexContext":{"maxSuccessfulCompactsBeforeRollover":1}}"#,
        )
        .unwrap();
        let old_session = "telegram:dm-42:user-7:main";
        let queue_file = queue_dir(&harness_home).join("pending.jsonl");
        append_json_line(
            &queue_file,
            &serde_json::json!({
                "schema": "agent-harness.runtime-queue-item.v1",
                "queueId": "queue-1",
                "status": "queued",
                "runtimeClass": "interactive",
                "origin": "channel",
                "source": {
                    "kind": "channel",
                    "sourceHome": source.home,
                    "sourceWorkspace": source.workspace
                },
                "createdAtMs": 1234,
                "agentId": "main",
                "sessionKey": old_session,
                "platform": "telegram",
                "channelId": "dm-42",
                "userId": "user-7",
                "messageText": "continue after compact",
                "plannedTranscriptFile": "old.jsonl",
                "plannedTrajectoryFile": "old.trajectory.jsonl"
            }),
        )
        .unwrap();
        let state_file = channel_session_state_file(&harness_home, "telegram", "dm-42", "user-7");
        fs::create_dir_all(state_file.parent().unwrap()).unwrap();
        write_json_atomic(
            &state_file,
            &ChannelSessionState {
                schema: "agent-harness.channel-session-state.v1".to_string(),
                platform: "telegram".to_string(),
                account_id: None,
                channel_id: "dm-42".to_string(),
                user_id: "user-7".to_string(),
                active_session_key: old_session.to_string(),
                agent_id: Some("main".to_string()),
                config_revision: None,
                provider: None,
                model: None,
                session_topic: None,
                model_override: None,
                model_override_provider: None,
                model_override_model: None,
                thinking_enabled: false,
                thinking_level: None,
                thinking_instruction: None,
                reasoning_preference: None,
                backend_reasoning_policy: None,
                fast_mode: None,
                stop_requested: false,
                stop_reason: None,
                steering_notes: Vec::new(),
                btw_notes: Vec::new(),
                last_command: None,
                updated_at_ms: 1234,
            },
        )
        .unwrap();
        write_json_atomic(
            &queue_dir(&harness_home)
                .join("classes")
                .join("interactive")
                .join("runtime-leases.json"),
            &serde_json::json!({
                "schema": "agent-harness.runtime-queue-leases.v1",
                "leases": {"queue-1": {"queueId": "queue-1"}}
            }),
        )
        .unwrap();
        record_context_compact_attempt(ContextCompactAttemptOptions {
            harness_home: harness_home.clone(),
            lane: ContextRolloverLane {
                runtime_class: "interactive".to_string(),
                agent_id: "main".to_string(),
                platform: "telegram".to_string(),
                channel_id: "dm-42".to_string(),
                user_id: "user-7".to_string(),
                working_session_key: old_session.to_string(),
                virtual_session_id: None,
                continuation_index: 0,
            },
            compact_succeeded: true,
            rewrote_active_context: true,
            compact_thread_id: Some("thread-after-compact".to_string()),
            compact_attempt_key: None,
            max_successful_compacts_before_rollover: 1,
            now_ms: 1235,
        })
        .unwrap();
        let pending = PendingQueueItem {
            queue_id: "queue-1".to_string(),
            admission_queue_id: None,
            created_at_ms: 1234,
            agent_id: "main".to_string(),
            session_key: old_session.to_string(),
            runtime_class: "interactive".to_string(),
            origin: "channel".to_string(),
            cron_run_id: None,
            scheduled_for_ms: None,
            platform: "telegram".to_string(),
            account_id: None,
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            message_text: "continue after compact".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            provider: None,
            model: None,
            reasoning_preference: None,
            backend_reasoning_policy: None,
            authorized_execution_mode: None,
            source_home: root.join(".openclaw"),
            source_workspace: root.join(".openclaw").join("workspace"),
            runtime_workspace: None,
            planned_transcript_file: PathBuf::from("old.jsonl"),
            planned_trajectory_file: PathBuf::from("old.trajectory.jsonl"),
            selected_skill_ids: Vec::new(),
            continuation: RuntimeContinuationMetadata::legacy(),
            coordinator_resume: None,
        };

        let error = maybe_apply_context_rollover_before_turn(
            &harness_home,
            &queue_file,
            pending,
            1236,
            &mut Vec::new(),
        )
        .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        assert!(error.to_string().contains("BlockedLeased"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prepare_runtime_queue_item_tombstones_skipped_cron_run() {
        let root = temp_root("prepare_runtime_queue_item_tombstones_skipped_cron_run");
        let source = write_worker_source(&root);
        let harness_home = root.join(".agent-harness");
        let cron_queue_id = "turn:cron:daily:1001";
        let cron_run = crate::admit_cron_run(crate::CronRunAdmitOptions {
            harness_home: harness_home.clone(),
            source_kind: "native-cron".to_string(),
            source_id: "source-1".to_string(),
            entry_id: "daily".to_string(),
            agent_id: "main".to_string(),
            scheduled_for_ms: 1001,
            runtime_class: "cron".to_string(),
            session_key: "cron:main:daily:1001".to_string(),
            session_policy: "one-shot".to_string(),
            max_attempts: 3,
            now_ms: 1001,
        })
        .unwrap();
        crate::mark_cron_run_runtime_enqueued(&harness_home, &cron_run.run_id, cron_queue_id, 1002)
            .unwrap();
        append_cron_pending_runtime_item(
            &source,
            &harness_home,
            cron_queue_id,
            &cron_run.run_id,
            "main",
            1001,
        );
        crate::control_cron_run(crate::CronRunControlOptions {
            harness_home: harness_home.clone(),
            action: crate::CronRunControlAction::Skip,
            run_id: Some(cron_run.run_id.clone()),
            agent_id: None,
            entry_id: None,
            reason: "operator skip before runtime dispatch".to_string(),
            now_ms: 1003,
        })
        .unwrap();

        let report = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: Some(cron_queue_id.to_string()),
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();

        assert!(report.item.is_none());
        assert_eq!(
            report.receipt.status,
            RuntimeExecutionReceiptStatus::NoPendingItem
        );
        let run_once_receipts =
            fs::read_to_string(queue_dir(&harness_home).join("run-once-receipts.jsonl")).unwrap();
        assert!(run_once_receipts.contains("\"queueId\":\"turn:cron:daily:1001\""));
        assert!(run_once_receipts.contains("\"status\":\"skipped\""));
        let runs = crate::collect_cron_run_summary(&harness_home).unwrap();
        assert_eq!(
            runs.runs.first().unwrap().status,
            crate::CronRunStatus::Skipped
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn inspect_runtime_queue_capacity_returns_channel_aware_claimable_ids() {
        let root = temp_root("inspect_runtime_queue_capacity_returns_channel_aware_claimable_ids");
        let source = write_worker_source(&root);
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            r#"{"workerDispatch":{"globalConcurrencyLimit":3,"groupConcurrencyLimit":3,"channelConcurrencyLimit":1,"laneConcurrencyLimits":{"llm":3}}}"#,
        )
        .unwrap();
        enqueue_fixture_turn_with_platform_channel(
            &source,
            &harness_home,
            "telegram",
            "tg-dm",
            "user-7",
            "first tg",
            1234,
        );
        enqueue_fixture_turn_with_platform_channel(
            &source,
            &harness_home,
            "telegram",
            "tg-dm",
            "user-7",
            "second tg",
            1235,
        );
        enqueue_fixture_turn_with_platform_channel(
            &source,
            &harness_home,
            "discord",
            "discord-dm",
            "user-7",
            "first discord",
            1236,
        );

        let capacity = inspect_runtime_queue_capacity(RuntimeQueueCapacityOptions {
            harness_home: harness_home.clone(),
        })
        .unwrap();

        assert_eq!(capacity.claimable_items, 2);
        assert_eq!(capacity.claimable_queue_ids.len(), 2);
        assert!(
            capacity
                .claimable_queue_ids
                .iter()
                .any(|queue_id| queue_id.contains(":telegram:tg-dm:"))
        );
        assert!(
            capacity
                .claimable_queue_ids
                .iter()
                .any(|queue_id| queue_id.contains(":discord:discord-dm:"))
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prepare_runtime_queue_item_counts_legacy_root_leases() {
        let root = temp_root("prepare_runtime_queue_item_counts_legacy_root_leases");
        let source = write_worker_source(&root);
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            r#"{"workerDispatch":{"globalConcurrencyLimit":3,"groupConcurrencyLimit":3,"channelConcurrencyLimit":1,"laneConcurrencyLimits":{"llm":3}}}"#,
        )
        .unwrap();
        enqueue_fixture_turn_with_platform_channel(
            &source,
            &harness_home,
            "telegram",
            "tg-dm",
            "user-7",
            "pending tg",
            1234,
        );
        let queue_dir = harness_home.join("state").join("runtime-queue");
        let now_ms = current_log_time_ms().unwrap();
        let mut legacy_state = RuntimeQueueLeaseState {
            schema: runtime_queue_leases_schema(),
            leases: BTreeMap::new(),
        };
        legacy_state.leases.insert(
            "legacy-active".to_string(),
            RuntimeQueueLease {
                queue_id: "legacy-active".to_string(),
                agent_id: "main".to_string(),
                runtime_class: "interactive".to_string(),
                origin: "channel".to_string(),
                cron_run_id: None,
                platform: "telegram".to_string(),
                account_id: None,
                channel_id: "tg-dm".to_string(),
                user_id: Some("user-7".to_string()),
                session_key: "main:telegram:tg-dm".to_string(),
                virtual_session_id: None,
                session_lane_key: None,
                owner: RuntimeQueueLeaseOwner::Legacy("legacy-worker".to_string()),
                started_at_ms: now_ms,
                lease_expires_at_ms: now_ms.saturating_add(60_000),
            },
        );
        write_runtime_queue_leases(&queue_dir, "legacy", &legacy_state).unwrap();

        let report = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();

        assert!(report.item.is_none());
        assert_eq!(
            report.receipt.status,
            RuntimeExecutionReceiptStatus::NoPendingItem
        );
        assert!(
            report
                .warnings
                .iter()
                .any(|warning| warning.contains("agent-channel limit"))
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prepare_runtime_queue_item_reaps_dead_pid_owner_before_capacity_check() {
        let root =
            temp_root("prepare_runtime_queue_item_reaps_dead_pid_owner_before_capacity_check");
        let source = write_worker_source(&root);
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            r#"{"workerDispatch":{"globalConcurrencyLimit":1,"groupConcurrencyLimit":1,"channelConcurrencyLimit":1,"laneConcurrencyLimits":{"llm":1}}}"#,
        )
        .unwrap();
        enqueue_fixture_turn_with_platform_channel(
            &source,
            &harness_home,
            "telegram",
            "tg-dm",
            "user-7",
            "pending tg",
            1234,
        );
        let queue_dir = harness_home.join("state").join("runtime-queue");
        let now_ms = current_log_time_ms().unwrap();
        let mut lease_state = RuntimeQueueLeaseState {
            schema: runtime_queue_leases_schema(),
            leases: BTreeMap::new(),
        };
        lease_state.leases.insert(
            "dead-owner".to_string(),
            RuntimeQueueLease {
                queue_id: "dead-owner".to_string(),
                agent_id: "main".to_string(),
                runtime_class: "interactive".to_string(),
                origin: "channel".to_string(),
                cron_run_id: None,
                platform: "telegram".to_string(),
                account_id: None,
                channel_id: "tg-dm".to_string(),
                user_id: Some("user-7".to_string()),
                session_key: "main:telegram:tg-dm".to_string(),
                virtual_session_id: None,
                session_lane_key: None,
                owner: RuntimeQueueLeaseOwner::Legacy("pid:0".to_string()),
                started_at_ms: now_ms,
                lease_expires_at_ms: now_ms.saturating_add(60_000),
            },
        );
        write_runtime_queue_leases(&queue_dir, "interactive", &lease_state).unwrap();

        let report = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();

        assert_eq!(
            report.receipt.status,
            RuntimeExecutionReceiptStatus::Prepared
        );
        let updated_state =
            read_runtime_queue_leases(&queue_dir, "interactive", &mut Vec::new()).unwrap();
        assert!(!updated_state.leases.contains_key("dead-owner"));
        let receipts = fs::read_to_string(&report.execution_receipts_file).unwrap();
        assert!(receipts.contains(r#""status":"stale-owner-reaped""#));
        assert!(receipts.contains(r#""pid:0""#));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn reconcile_runtime_queue_leases_for_generation_reaps_owned_leases() {
        let root = temp_root("reconcile_runtime_queue_leases_for_generation");
        let harness_home = root.join(".agent-harness");
        let queue_dir = harness_home.join("state").join("runtime-queue");
        fs::create_dir_all(&queue_dir).unwrap();
        let generation_id = "runtime-loop-supervised-test-generation";
        let now_ms = current_log_time_ms().unwrap();
        let mut lease_state = RuntimeQueueLeaseState {
            schema: runtime_queue_leases_schema(),
            leases: BTreeMap::new(),
        };
        lease_state.leases.insert(
            "owned-generation".to_string(),
            RuntimeQueueLease {
                queue_id: "owned-generation".to_string(),
                agent_id: "main".to_string(),
                runtime_class: "interactive".to_string(),
                origin: "channel".to_string(),
                cron_run_id: None,
                platform: "telegram".to_string(),
                account_id: None,
                channel_id: "tg-dm".to_string(),
                user_id: Some("user-7".to_string()),
                session_key: "main:telegram:tg-dm".to_string(),
                virtual_session_id: None,
                session_lane_key: None,
                owner: RuntimeQueueLeaseOwner::Envelope(RuntimeQueueLeaseOwnerEnvelope {
                    kind: "supervisor-child".to_string(),
                    service_id: RUNTIME_LOOP_SERVICE_ID.to_string(),
                    generation_id: generation_id.to_string(),
                    pid: 12_345,
                    process_start_time_ms: now_ms.saturating_sub(1_000),
                    acquired_at_ms: now_ms,
                }),
                started_at_ms: now_ms,
                lease_expires_at_ms: now_ms.saturating_add(60_000),
            },
        );
        lease_state.leases.insert(
            "other-generation".to_string(),
            RuntimeQueueLease {
                queue_id: "other-generation".to_string(),
                agent_id: "main".to_string(),
                runtime_class: "interactive".to_string(),
                origin: "channel".to_string(),
                cron_run_id: None,
                platform: "telegram".to_string(),
                account_id: None,
                channel_id: "tg-dm".to_string(),
                user_id: Some("user-7".to_string()),
                session_key: "main:telegram:tg-dm".to_string(),
                virtual_session_id: None,
                session_lane_key: None,
                owner: RuntimeQueueLeaseOwner::Envelope(RuntimeQueueLeaseOwnerEnvelope {
                    kind: "supervisor-child".to_string(),
                    service_id: RUNTIME_LOOP_SERVICE_ID.to_string(),
                    generation_id: "different-generation".to_string(),
                    pid: 12_346,
                    process_start_time_ms: now_ms.saturating_sub(1_000),
                    acquired_at_ms: now_ms,
                }),
                started_at_ms: now_ms,
                lease_expires_at_ms: now_ms.saturating_add(60_000),
            },
        );
        write_runtime_queue_leases(&queue_dir, "interactive", &lease_state).unwrap();

        let report = reconcile_runtime_queue_leases_for_generation(
            &harness_home,
            RUNTIME_LOOP_SERVICE_ID,
            generation_id,
            now_ms.saturating_add(10),
        )
        .unwrap();

        assert_eq!(report.reaped_leases.len(), 1);
        assert_eq!(report.reaped_leases[0].queue_id, "owned-generation");
        let updated_state =
            read_runtime_queue_leases(&queue_dir, "interactive", &mut Vec::new()).unwrap();
        assert!(!updated_state.leases.contains_key("owned-generation"));
        assert!(updated_state.leases.contains_key("other-generation"));
        let receipts = fs::read_to_string(queue_dir.join("execution-receipts.jsonl")).unwrap();
        assert!(receipts.contains(r#""status":"stale-owner-reaped""#));
        assert!(receipts.contains(r#""serviceId":"runtime-loop""#));
        assert!(receipts.contains(generation_id));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn cron_runtime_selection_interleaves_agents() {
        let root = temp_root("cron_runtime_selection_interleaves_agents");
        let harness_home = root.join(".agent-harness");
        let queue_dir = harness_home.join("state").join("runtime-queue");
        fs::create_dir_all(&queue_dir).unwrap();
        let pending = vec![
            pending_cron_item("cron-a-1", "agent-a", 1000),
            pending_cron_item("cron-a-2", "agent-a", 1001),
            pending_cron_item("cron-b-1", "agent-b", 1002),
        ];
        let lease_state = RuntimeQueueLeaseState::default();
        let terminal_ids = HashSet::new();
        let auth_deferred_ids = HashSet::new();
        let run_once_index = RuntimeQueueStateIndex::default();
        let mut prepared_ids = HashSet::new();
        let mut warnings = Vec::new();

        let first = select_pending_item(
            pending.clone(),
            None,
            &prepared_ids,
            &terminal_ids,
            &auth_deferred_ids,
            &run_once_index,
            &lease_state,
            "cron",
            &harness_home,
            &queue_dir,
            2000,
            &mut warnings,
        )
        .unwrap()
        .unwrap();
        assert_eq!(first.queue_id, "cron-a-1");
        prepared_ids.insert(first.queue_id);

        let second = select_pending_item(
            pending,
            None,
            &prepared_ids,
            &terminal_ids,
            &auth_deferred_ids,
            &run_once_index,
            &lease_state,
            "cron",
            &harness_home,
            &queue_dir,
            2001,
            &mut warnings,
        )
        .unwrap()
        .unwrap();
        assert_eq!(second.queue_id, "cron-b-1");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn durable_retry_schedule_survives_restart_and_does_not_block_other_lanes() {
        let root =
            temp_root("durable_retry_schedule_survives_restart_and_does_not_block_other_lanes");
        let harness_home = root.join(".agent-harness");
        let queue_dir = harness_home.join("state").join("runtime-queue");
        fs::create_dir_all(&queue_dir).unwrap();
        let mut index = RuntimeQueueStateIndex {
            schema: runtime_queue_state_index_schema(),
            revision: RUNTIME_QUEUE_STATE_INDEX_REVISION,
            ..RuntimeQueueStateIndex::default()
        };
        index.queues.insert(
            "cron-delayed".to_string(),
            RuntimeQueueStateIndexEntry {
                latest_status: Some("retry-pending".to_string()),
                retry_schedule: Some(crate::RuntimeRetryScheduleV1 {
                    lineage_id: "runtime-retry:cron-delayed".to_string(),
                    attempt: 1,
                    max_attempts: 3,
                    delay_ms: 1_000,
                    scheduled_at_ms: 1_000,
                    next_eligible_at_ms: 2_000,
                    replay_mode: crate::RuntimeRetryReplayModeV1::SameRequestNoObservableMutation,
                }),
                ..RuntimeQueueStateIndexEntry::default()
            },
        );
        write_runtime_queue_state_index(&queue_dir, &index).unwrap();

        let restarted = read_runtime_queue_state_index(&queue_dir).unwrap().unwrap();
        assert!(retry_schedule_dispatch_blocker(&restarted, "cron-delayed", 1_500).is_some());
        let pending = vec![
            pending_cron_item("cron-delayed", "agent-a", 1_000),
            pending_cron_item("cron-ready", "agent-b", 1_001),
        ];
        let selected = select_pending_item(
            pending.clone(),
            None,
            &HashSet::new(),
            &HashSet::new(),
            &HashSet::new(),
            &restarted,
            &RuntimeQueueLeaseState::default(),
            "cron",
            &harness_home,
            &queue_dir,
            1_500,
            &mut Vec::new(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(selected.queue_id, "cron-ready");

        let selected_after_backoff = select_pending_item(
            pending,
            None,
            &HashSet::new(),
            &HashSet::new(),
            &HashSet::new(),
            &restarted,
            &RuntimeQueueLeaseState::default(),
            "cron",
            &harness_home,
            &queue_dir,
            2_000,
            &mut Vec::new(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(selected_after_backoff.queue_id, "cron-delayed");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn retry_pending_item_is_ineligible_until_persisted_not_before() {
        let index = RuntimeQueueStateIndex {
            queues: BTreeMap::from([(
                "queue-retry".to_string(),
                RuntimeQueueStateIndexEntry {
                    latest_status: Some("retry-pending".to_string()),
                    retry_schedule: Some(crate::RuntimeRetryScheduleV1 {
                        lineage_id: "runtime-retry:queue-retry".to_string(),
                        attempt: 1,
                        max_attempts: 3,
                        delay_ms: 1_000,
                        scheduled_at_ms: 1_000,
                        next_eligible_at_ms: 2_000,
                        replay_mode:
                            crate::RuntimeRetryReplayModeV1::SameRequestNoObservableMutation,
                    }),
                    ..RuntimeQueueStateIndexEntry::default()
                },
            )]),
            ..RuntimeQueueStateIndex::default()
        };
        assert!(retry_schedule_dispatch_blocker(&index, "queue-retry", 1_999).is_some());
        assert!(retry_schedule_dispatch_blocker(&index, "queue-retry", 2_000).is_none());
    }

    #[test]
    fn retry_not_before_survives_restart_and_does_not_block_other_lane() {
        durable_retry_schedule_survives_restart_and_does_not_block_other_lanes();
    }

    #[test]
    fn inspect_runtime_queue_capacity_treats_busy_lease_lock_as_zero_capacity() {
        let root = temp_root("inspect_runtime_queue_capacity_treats_busy_lease_lock_as_zero");
        let harness_home = root.join(".agent-harness");
        let queue_dir = harness_home.join("state").join("runtime-queue");
        fs::create_dir_all(&queue_dir).unwrap();
        let now_ms = current_log_time_ms().unwrap();
        let _held_lock = create_runtime_queue_lease_lock(
            &runtime_queue_lease_lock_file(&queue_dir, "interactive"),
            now_ms,
        )
        .unwrap();

        let report = inspect_runtime_queue_capacity(RuntimeQueueCapacityOptions {
            harness_home: harness_home.clone(),
        })
        .unwrap();

        assert_eq!(report.claimable_items, 0);
        assert!(report.claimable_queue_ids.is_empty());
        assert!(
            report
                .warnings
                .iter()
                .any(|warning| warning.contains("lease lock is busy"))
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prepare_runtime_queue_item_reports_lease_busy_when_lock_busy() {
        let root = temp_root("prepare_runtime_queue_item_reports_lease_busy_when_lock_busy");
        let harness_home = root.join(".agent-harness");
        let queue_dir = harness_home.join("state").join("runtime-queue");
        fs::create_dir_all(&queue_dir).unwrap();
        let now_ms = current_log_time_ms().unwrap();
        let _held_lock = create_runtime_queue_lease_lock(
            &runtime_queue_lease_lock_file(&queue_dir, "interactive"),
            now_ms,
        )
        .unwrap();

        let report = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: Some("turn:busy".to_string()),
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();

        assert!(report.item.is_none());
        assert_eq!(
            report.receipt.status,
            RuntimeExecutionReceiptStatus::LeaseBusy
        );
        assert_eq!(report.receipt.queue_id.as_deref(), Some("turn:busy"));
        let receipts = fs::read_to_string(report.execution_receipts_file).unwrap();
        assert!(receipts.contains("\"status\":\"lease-busy\""));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prepare_runtime_queue_item_does_not_let_cron_class_lock_block_interactive() {
        let root =
            temp_root("prepare_runtime_queue_item_does_not_let_cron_class_lock_block_interactive");
        let source = write_worker_source(&root);
        let harness_home = root.join(".agent-harness");
        enqueue_fixture_turn_with_platform_channel(
            &source,
            &harness_home,
            "telegram",
            "tg-dm",
            "user-7",
            "interactive turn",
            1234,
        );
        let queue_dir = harness_home.join("state").join("runtime-queue");
        let mut pending = OpenOptions::new()
            .create(true)
            .append(true)
            .open(queue_dir.join("pending.jsonl"))
            .unwrap();
        writeln!(
            pending,
            "{}",
            serde_json::json!({
                "queueId": "turn:cron:blocked",
                "createdAtMs": 1233,
                "agentId": "main@cron",
                "sessionKey": "cron:main:hourly:1233",
                "runtimeClass": "cron",
                "origin": "cron-scheduler",
                "cronRunId": "cronrun:native-cron:main:hourly:1233",
                "scheduledForMs": 1233,
                "platform": "native-cron",
                "channelId": "cron",
                "userId": "scheduler",
                "messageText": "cron turn",
                "source": {
                    "sourceHome": source.home.clone(),
                    "sourceWorkspace": source.workspace.clone(),
                    "runtimeWorkspace": source.workspace.clone()
                },
                "plannedTranscriptFile": harness_home.join("agents").join("main").join("cron-sessions").join("hourly").join("transcript.jsonl"),
                "plannedTrajectoryFile": harness_home.join("agents").join("main").join("cron-sessions").join("hourly").join("trajectory.jsonl"),
                "selectedSkillIds": []
            })
        )
        .unwrap();
        let now_ms = current_log_time_ms().unwrap();
        let _cron_lock = create_runtime_queue_lease_lock(
            &runtime_queue_lease_lock_file(&queue_dir, "cron"),
            now_ms,
        )
        .unwrap();

        let report = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();

        assert_eq!(
            report.receipt.status,
            RuntimeExecutionReceiptStatus::Prepared
        );
        assert_eq!(report.receipt.runtime_class.as_deref(), Some("interactive"));
        assert_eq!(report.item.as_ref().unwrap().runtime_class, "interactive");
        assert_ne!(
            report.receipt.queue_id.as_deref(),
            Some("turn:cron:blocked")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prepare_runtime_queue_item_is_idempotent_for_prepared_items() {
        let root = temp_root("prepare_runtime_queue_item_is_idempotent_for_prepared_items");
        let source = write_worker_source(&root);
        let harness_home = root.join(".agent-harness");
        enqueue_fixture_turn(&source, &harness_home);

        let first = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();
        let queue_id = first.receipt.queue_id.clone().unwrap();
        assert_eq!(
            first.receipt.status,
            RuntimeExecutionReceiptStatus::Prepared
        );

        let second = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();
        assert!(second.item.is_none());
        assert_eq!(
            second.receipt.status,
            RuntimeExecutionReceiptStatus::NoPendingItem
        );
        assert_eq!(second.receipt.queue_id, None);

        release_runtime_queue_lease(&harness_home, &queue_id).unwrap();
        let resumed = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();
        assert!(resumed.item.is_none());
        assert_eq!(
            resumed.receipt.status,
            RuntimeExecutionReceiptStatus::AlreadyPrepared
        );
        assert_eq!(resumed.receipt.queue_id.as_deref(), Some(queue_id.as_str()));

        let run_once_receipts = harness_home
            .join("state")
            .join("runtime-queue")
            .join("run-once-receipts.jsonl");
        let mut run_once_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&run_once_receipts)
            .unwrap();
        writeln!(
            run_once_file,
            "{}",
            serde_json::json!({
                "queueId": queue_id.clone(),
                "status": "auth-deferred",
                "reason": "needs-operator-auth"
            })
        )
        .unwrap();

        let deferred_capacity = inspect_runtime_queue_capacity(RuntimeQueueCapacityOptions {
            harness_home: harness_home.clone(),
        })
        .unwrap();
        assert_eq!(deferred_capacity.claimable_items, 0);
        assert!(deferred_capacity.claimable_queue_ids.is_empty());
        let deferred_index =
            refresh_runtime_queue_state_index(&queue_dir(&harness_home), &mut Vec::new()).unwrap();
        assert_eq!(
            runtime_queue_prior_failure_count_from_index(&deferred_index, &queue_id),
            0,
            "auth deferral must not consume retry budget"
        );
        let deferred_prepare = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: Some(queue_id.clone()),
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();
        assert!(deferred_prepare.item.is_none());
        assert_eq!(
            deferred_prepare.receipt.status,
            RuntimeExecutionReceiptStatus::NoPendingItem
        );
        assert!(
            deferred_prepare
                .receipt
                .reason
                .contains("waiting for operator authentication")
        );

        writeln!(
            run_once_file,
            "{}",
            serde_json::json!({
                "queueId": queue_id.clone(),
                "status": "retry-pending",
                "reason": "transient reconnect; retry with the same session"
            })
        )
        .unwrap();

        let retry_capacity = inspect_runtime_queue_capacity(RuntimeQueueCapacityOptions {
            harness_home: harness_home.clone(),
        })
        .unwrap();
        assert_eq!(retry_capacity.claimable_items, 1);
        assert_eq!(retry_capacity.claimable_queue_ids, vec![queue_id.clone()]);
        let retry_resumed = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();
        assert!(retry_resumed.item.is_none());
        assert_eq!(
            retry_resumed.receipt.status,
            RuntimeExecutionReceiptStatus::AlreadyPrepared
        );
        assert_eq!(
            retry_resumed.receipt.queue_id.as_deref(),
            Some(queue_id.as_str())
        );
        release_runtime_queue_lease(&harness_home, &queue_id).unwrap();

        writeln!(
            run_once_file,
            "{}",
            serde_json::json!({
                "queueId": queue_id.clone(),
                "status": "timeout",
                "reason": "test retryable receipt"
            })
        )
        .unwrap();

        let after_timeout = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();
        assert!(after_timeout.item.is_none());
        assert_eq!(
            after_timeout.receipt.status,
            RuntimeExecutionReceiptStatus::NoPendingItem
        );
        assert_eq!(
            after_timeout.receipt.queue_id.as_deref(),
            Some(queue_id.as_str())
        );
        assert_eq!(
            after_timeout.receipt.terminal_control_source.as_deref(),
            Some("run-once-terminal")
        );

        writeln!(
            run_once_file,
            "{}",
            serde_json::json!({
                "queueId": queue_id,
                "status": "completed",
                "reason": "test terminal receipt"
            })
        )
        .unwrap();

        let explicit = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: resumed.receipt.queue_id.clone(),
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();
        assert!(explicit.item.is_none());
        assert_eq!(
            explicit.receipt.status,
            RuntimeExecutionReceiptStatus::NoPendingItem
        );
        assert!(explicit.receipt.execution_dir.is_none());
        assert!(explicit.receipt.reason.contains("terminal control"));
        assert_eq!(
            explicit.receipt.terminal_control_source.as_deref(),
            Some("run-once-terminal")
        );

        let after_terminal = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home,
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();
        assert!(after_terminal.item.is_none());
        assert_eq!(
            after_terminal.receipt.status,
            RuntimeExecutionReceiptStatus::NoPendingItem
        );

        let _ = fs::remove_dir_all(root);
    }

    fn enqueue_fixture_turn(source: &AgentSource, harness_home: &Path) {
        enqueue_fixture_turn_with_text(source, harness_home, "repair memory cron", 1234);
    }

    fn enqueue_fixture_turn_for_account(
        source: &AgentSource,
        harness_home: &Path,
        account_id: &str,
        text: &str,
        now_ms: i64,
    ) {
        enqueue_fixture_turn_for_account_with_session(
            source,
            harness_home,
            account_id,
            None,
            text,
            now_ms,
        );
    }

    fn enqueue_fixture_turn_for_account_with_session(
        source: &AgentSource,
        harness_home: &Path,
        account_id: &str,
        session_key: Option<&str>,
        text: &str,
        now_ms: i64,
    ) {
        let registry = load_agent_registry(source).unwrap();
        let skills = build_source_skill_index(source).unwrap();
        let turn = crate::build_turn_plan_for_account(
            source,
            &registry,
            &skills,
            TurnPlanInput {
                harness_home: None,
                platform: "telegram".to_string(),
                channel_id: "dm-42".to_string(),
                user_id: "user-7".to_string(),
                text: text.to_string(),
                inbound_context: None,
                inbound_media_artifacts: Vec::new(),
                requested_agent_id: Some("main".to_string()),
                session_hint: session_key.map(ToString::to_string),
                skill_limit: 3,
            },
            Some(account_id.to_string()),
        )
        .unwrap();
        let step = build_channel_step(&registry, &turn);
        enqueue_channel_step(
            &step,
            RuntimeQueueEnqueueOptions {
                harness_home: harness_home.to_path_buf(),
                runtime_workspace: None,
                inbound_canonical_id: None,
                now_ms,
            },
        )
        .unwrap();
    }

    fn enqueue_fixture_turn_with_text(
        source: &AgentSource,
        harness_home: &Path,
        text: &str,
        now_ms: i64,
    ) {
        enqueue_fixture_turn_with_platform_channel(
            source,
            harness_home,
            "telegram",
            "dm-42",
            "user-7",
            text,
            now_ms,
        );
    }

    fn enqueue_fixture_turn_with_platform_channel(
        source: &AgentSource,
        harness_home: &Path,
        platform: &str,
        channel_id: &str,
        user_id: &str,
        text: &str,
        now_ms: i64,
    ) {
        enqueue_fixture_turn_with_optional_session(
            source,
            harness_home,
            platform,
            channel_id,
            user_id,
            None,
            text,
            now_ms,
        );
    }

    fn enqueue_fixture_turn_with_session(
        source: &AgentSource,
        harness_home: &Path,
        platform: &str,
        channel_id: &str,
        user_id: &str,
        session_key: &str,
        text: &str,
        now_ms: i64,
    ) {
        enqueue_fixture_turn_with_optional_session(
            source,
            harness_home,
            platform,
            channel_id,
            user_id,
            Some(session_key),
            text,
            now_ms,
        );
    }

    fn enqueue_fixture_turn_with_optional_session(
        source: &AgentSource,
        harness_home: &Path,
        platform: &str,
        channel_id: &str,
        user_id: &str,
        session_key: Option<&str>,
        text: &str,
        now_ms: i64,
    ) {
        let registry = load_agent_registry(source).unwrap();
        let skills = build_source_skill_index(source).unwrap();
        let turn = build_turn_plan(
            source,
            &registry,
            &skills,
            TurnPlanInput {
                harness_home: None,
                platform: platform.to_string(),
                channel_id: channel_id.to_string(),
                user_id: user_id.to_string(),
                text: text.to_string(),
                inbound_context: None,
                inbound_media_artifacts: Vec::new(),
                requested_agent_id: Some("main".to_string()),
                session_hint: session_key.map(ToString::to_string),
                skill_limit: 3,
            },
        )
        .unwrap();
        let step = build_channel_step(&registry, &turn);
        enqueue_channel_step(
            &step,
            RuntimeQueueEnqueueOptions {
                harness_home: harness_home.to_path_buf(),
                runtime_workspace: None,
                inbound_canonical_id: None,
                now_ms,
            },
        )
        .unwrap();
    }

    fn append_worker_pending_runtime_item(
        source: &AgentSource,
        harness_home: &Path,
        queue_id: &str,
        created_at_ms: i64,
    ) {
        append_json_line(
            &queue_dir(harness_home).join("pending.jsonl"),
            &serde_json::json!({
                "queueId": queue_id,
                "status": "queued",
                "createdAtMs": created_at_ms,
                "agentId": "main",
                "sessionKey": "shared-worker-session",
                "runtimeClass": "worker",
                "origin": "worker-dispatch",
                "platform": "worker",
                "channelId": "worker-channel",
                "userId": "worker-user",
                "messageText": "run worker turn",
                "source": {
                    "sourceHome": source.home.clone(),
                    "sourceWorkspace": source.workspace.clone(),
                    "runtimeWorkspace": source.workspace.clone()
                },
                "plannedTranscriptFile": harness_home
                    .join("agents")
                    .join("main")
                    .join("worker-sessions")
                    .join(format!("{}.jsonl", normalize_key_part(queue_id))),
                "plannedTrajectoryFile": harness_home
                    .join("agents")
                    .join("main")
                    .join("worker-sessions")
                    .join(format!("{}.trajectory.jsonl", normalize_key_part(queue_id))),
                "selectedSkillIds": []
            }),
        )
        .unwrap();
    }

    fn append_cron_pending_runtime_item(
        source: &AgentSource,
        harness_home: &Path,
        queue_id: &str,
        cron_run_id: &str,
        agent_id: &str,
        scheduled_for_ms: i64,
    ) {
        append_json_line(
            &queue_dir(harness_home).join("pending.jsonl"),
            &serde_json::json!({
                "queueId": queue_id,
                "status": "queued",
                "createdAtMs": scheduled_for_ms,
                "agentId": agent_id,
                "sessionKey": format!("cron:{agent_id}:daily:{scheduled_for_ms}"),
                "runtimeClass": "cron",
                "origin": "cron-scheduler",
                "cronRunId": cron_run_id,
                "scheduledForMs": scheduled_for_ms,
                "platform": "native-cron",
                "channelId": "daily",
                "userId": "cron-scheduler",
                "messageText": "run daily cron",
                "source": {
                    "sourceHome": source.home.clone(),
                    "sourceWorkspace": source.workspace.clone(),
                    "runtimeWorkspace": source.workspace.clone()
                },
                "plannedTranscriptFile": harness_home
                    .join("agents")
                    .join(agent_id)
                    .join("cron-sessions")
                    .join(format!("{}.jsonl", normalize_key_part(queue_id))),
                "plannedTrajectoryFile": harness_home
                    .join("agents")
                    .join(agent_id)
                    .join("cron-sessions")
                    .join(format!("{}.trajectory.jsonl", normalize_key_part(queue_id))),
                "selectedSkillIds": []
            }),
        )
        .unwrap();
    }

    fn queue_dir(harness_home: &Path) -> PathBuf {
        harness_home.join("state").join("runtime-queue")
    }

    fn pending_cron_item(queue_id: &str, agent_id: &str, created_at_ms: i64) -> PendingQueueItem {
        PendingQueueItem {
            queue_id: queue_id.to_string(),
            admission_queue_id: None,
            created_at_ms,
            agent_id: agent_id.to_string(),
            session_key: format!("cron:{agent_id}:{queue_id}:{created_at_ms}"),
            runtime_class: "cron".to_string(),
            origin: "cron-scheduler".to_string(),
            cron_run_id: None,
            scheduled_for_ms: Some(created_at_ms),
            platform: "native-cron".to_string(),
            account_id: None,
            channel_id: queue_id.to_string(),
            user_id: "cron-scheduler".to_string(),
            message_text: format!("run {queue_id}"),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            source_home: PathBuf::from("source"),
            source_workspace: PathBuf::from("workspace"),
            runtime_workspace: None,
            provider: None,
            model: None,
            reasoning_preference: None,
            backend_reasoning_policy: None,
            authorized_execution_mode: None,
            planned_transcript_file: PathBuf::from(format!("{queue_id}.jsonl")),
            planned_trajectory_file: PathBuf::from(format!("{queue_id}.trajectory.jsonl")),
            selected_skill_ids: Vec::new(),
            continuation: RuntimeContinuationMetadata::legacy(),
            coordinator_resume: None,
        }
    }

    #[test]
    fn prompt_runtime_context_uses_exact_lane_and_codex_thread_generation() {
        let root = temp_root("prompt_runtime_context_exact_lane_generation");
        fs::create_dir_all(&root).unwrap();
        let transcript = root.join("session.jsonl");
        let binding = root.join("session.jsonl.codex-app-server.json");
        fs::write(&binding, r#"{"threadId":"thread-a"}"#).unwrap();

        let mut pending = pending_cron_item("queue-a", "main", 1234);
        pending.platform = "telegram".to_string();
        pending.account_id = Some("account-a".to_string());
        pending.channel_id = "channel-a".to_string();
        pending.user_id = "user-a".to_string();
        pending.runtime_class = "interactive".to_string();
        pending.session_key = "telegram:dm:user-a:main".to_string();
        pending.planned_transcript_file = transcript;

        let mut warnings = Vec::new();
        let (main_lane, main_generation) =
            derive_prompt_runtime_context(&pending, &mut warnings).unwrap();
        assert!(warnings.is_empty());
        assert!(main_lane.is_some());
        assert_eq!(main_generation, "codex-thread:thread-a:policy=unmanaged");

        let mut sibling = pending.clone();
        sibling.agent_id = "reviewer".to_string();
        let (sibling_lane, _) = derive_prompt_runtime_context(&sibling, &mut warnings).unwrap();
        assert_ne!(main_lane, sibling_lane);

        fs::write(&binding, r#"{"threadId":"thread-b"}"#).unwrap();
        let (_, rebound_generation) =
            derive_prompt_runtime_context(&pending, &mut warnings).unwrap();
        assert_eq!(rebound_generation, "codex-thread:thread-b:policy=unmanaged");

        fs::remove_file(&binding).unwrap();
        let (_, unbound_generation) =
            derive_prompt_runtime_context(&pending, &mut warnings).unwrap();
        assert_eq!(unbound_generation, "codex-unbound:queue-a:policy=unmanaged");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prepare_prompt_manifest_isolated_by_full_lane_root_and_backend_generation() {
        let root = temp_root("prepare_prompt_manifest_full_lane_root_generation");
        let source = write_worker_source(&root);
        let harness_home = root.join(".agent-harness");
        fs::write(source.workspace.join("SOUL.md"), "# Soul prompt v1").unwrap();
        fs::write(
            source.workspace.join("MEMORY.md"),
            "# Main agent persistent memory policy",
        )
        .unwrap();
        let reviewer_home = source.home.join("agents").join("reviewer");
        fs::create_dir_all(reviewer_home.join("sessions")).unwrap();
        fs::create_dir_all(reviewer_home.join("workspace")).unwrap();
        fs::write(
            reviewer_home.join("workspace").join("AGENTS.md"),
            "# Reviewer agent prompt",
        )
        .unwrap();
        fs::write(
            reviewer_home.join("workspace").join("MEMORY.md"),
            "# Reviewer-only persistent memory policy",
        )
        .unwrap();
        fs::write(reviewer_home.join("sessions").join("sessions.json"), "{}").unwrap();
        fs::write(
            source.home.join("openclaw.json"),
            r#"{
              "agents": {
                "defaults": { "provider": "openai", "model": "codex" },
                "list": [
                  { "id": "main", "model": "gpt-5", "enabled": true },
                  { "id": "reviewer", "model": "gpt-5", "enabled": true }
                ]
              },
              "models": {
                "providers": {
                  "openai": { "apiKey": "${OPENAI_API_KEY}" }
                }
              }
            }"#,
        )
        .unwrap();

        let old_root_session =
            crate::channel_session_key::CanonicalChannelSessionKey::bind_for_lane(
                "session-root-a",
                "telegram",
                "account-a",
                "dm-a",
                "user-a",
                "main",
            )
            .unwrap()
            .canonical_string();
        let old_root_lane = FullLaneKeyV1::new(
            "telegram",
            "account-a",
            "dm-a",
            "user-a",
            "main",
            "interactive",
            root_working_session_key(&old_root_session),
            &old_root_session,
        )
        .unwrap();
        crate::operation_plan::create_operation_plan_v2(
            crate::operation_plan::CreateOperationPlanOptionsV2 {
                options: crate::operation_plan::CreateOperationPlanOptions {
                    harness_home: harness_home.clone(),
                    plan_id: "old-root-plan".to_string(),
                    origin_queue_id: None,
                    session_key: old_root_session.clone(),
                    agent_id: "main".to_string(),
                    goal: "old root only".to_string(),
                    acceptance_criteria: None,
                    constraints: None,
                    max_open_items: None,
                    max_fanout: None,
                    now_ms: 1_000,
                },
                lane_digest: old_root_lane.identity_hash().unwrap(),
            },
        )
        .unwrap();

        let first = enqueue_and_prepare_prompt_manifest(
            &source,
            &harness_home,
            "telegram",
            "account-a",
            "dm-a",
            "user-a",
            "main",
            "session-root-a",
            "thread-a",
            2_000,
        );
        let first_agents = prompt_manifest_entry(&first.manifest, "AGENTS.md");
        assert_eq!(
            first_agents.status,
            crate::AgentPromptManifestStatusV1::Included
        );
        assert_eq!(
            first_agents.change,
            Some(crate::AgentPromptManifestChangeV1::Added)
        );
        let first_memory = prompt_manifest_entry(&first.manifest, "MEMORY.md");
        assert_eq!(
            first_memory.status,
            crate::AgentPromptManifestStatusV1::Included
        );
        assert!(
            first
                .markdown
                .contains("Main agent persistent memory policy")
        );
        assert!(first.markdown.contains("planId=old-root-plan"));
        assert_eq!(
            first.manifest.backend_context_generation.as_deref(),
            Some("codex-thread:thread-a:policy=unmanaged")
        );

        let unchanged = enqueue_and_prepare_prompt_manifest(
            &source,
            &harness_home,
            "telegram",
            "account-a",
            "dm-a",
            "user-a",
            "main",
            "session-root-a",
            "thread-a",
            2_001,
        );
        assert_eq!(first.manifest.lane_digest, unchanged.manifest.lane_digest);
        let unchanged_agents = prompt_manifest_entry(&unchanged.manifest, "AGENTS.md");
        assert_eq!(
            unchanged_agents.status,
            crate::AgentPromptManifestStatusV1::Reused
        );
        assert_eq!(unchanged_agents.change, None);

        fs::write(source.workspace.join("AGENTS.md"), "# Agent prompt v2").unwrap();
        let modified = enqueue_and_prepare_prompt_manifest(
            &source,
            &harness_home,
            "telegram",
            "account-a",
            "dm-a",
            "user-a",
            "main",
            "session-root-a",
            "thread-a",
            2_002,
        );
        let modified_agents = prompt_manifest_entry(&modified.manifest, "AGENTS.md");
        assert_eq!(
            modified_agents.status,
            crate::AgentPromptManifestStatusV1::Included
        );
        assert_eq!(
            modified_agents.change,
            Some(crate::AgentPromptManifestChangeV1::Modified)
        );

        let rebound = enqueue_and_prepare_prompt_manifest(
            &source,
            &harness_home,
            "telegram",
            "account-a",
            "dm-a",
            "user-a",
            "main",
            "session-root-a",
            "thread-b",
            2_003,
        );
        let rebound_agents = prompt_manifest_entry(&rebound.manifest, "AGENTS.md");
        assert_eq!(
            rebound_agents.status,
            crate::AgentPromptManifestStatusV1::Included
        );
        assert_eq!(
            rebound_agents.change,
            Some(crate::AgentPromptManifestChangeV1::BackendContextGenerationChanged)
        );
        assert_eq!(
            rebound.manifest.backend_context_generation.as_deref(),
            Some("codex-thread:thread-b:policy=unmanaged")
        );

        fs::remove_file(source.workspace.join("SOUL.md")).unwrap();
        let removed = enqueue_and_prepare_prompt_manifest(
            &source,
            &harness_home,
            "telegram",
            "account-a",
            "dm-a",
            "user-a",
            "main",
            "session-root-a",
            "thread-b",
            2_004,
        );
        let removed_soul = prompt_manifest_entry(&removed.manifest, "SOUL.md");
        assert_eq!(
            removed_soul.status,
            crate::AgentPromptManifestStatusV1::Removed
        );
        assert_eq!(
            removed_soul.change,
            Some(crate::AgentPromptManifestChangeV1::Removed)
        );
        assert!(removed.manifest.requires_fresh_backend_thread);
        let account_bound_baseline_session = crate::turns::bind_session_key_to_account(
            "session-root-a",
            "telegram",
            "account-a",
            "dm-a",
            "user-a",
            Some("main"),
        );
        assert!(
            crate::prompt::acknowledge_fresh_backend_thread(
                &harness_home,
                Some("main"),
                &account_bound_baseline_session,
                &old_root_lane,
                removed
                    .manifest
                    .static_config_revision
                    .as_deref()
                    .expect("removed bundle static-config revision"),
            )
            .unwrap()
        );

        let baseline_digest = removed.manifest.lane_digest.clone().unwrap();
        let variants = [
            (
                "platform",
                "discord",
                "account-a",
                "dm-a",
                "user-a",
                "main",
                "session-root-a",
            ),
            (
                "account",
                "telegram",
                "account-b",
                "dm-a",
                "user-a",
                "main",
                "session-root-a",
            ),
            (
                "channel",
                "telegram",
                "account-a",
                "dm-b",
                "user-a",
                "main",
                "session-root-a",
            ),
            (
                "user",
                "telegram",
                "account-a",
                "dm-a",
                "user-b",
                "main",
                "session-root-a",
            ),
            (
                "agent",
                "telegram",
                "account-a",
                "dm-a",
                "user-a",
                "reviewer",
                "session-root-a",
            ),
            (
                "concrete-session",
                "telegram",
                "account-a",
                "dm-a",
                "user-a",
                "main",
                "session-root-a:cont-1",
            ),
            (
                "new-root",
                "telegram",
                "account-a",
                "dm-a",
                "user-a",
                "main",
                "session-root-b",
            ),
        ];
        let mut lane_digests = std::collections::BTreeSet::from([baseline_digest.clone()]);
        for (index, (axis, platform, account, channel, user, agent, session)) in
            variants.into_iter().enumerate()
        {
            let prepared = enqueue_and_prepare_prompt_manifest(
                &source,
                &harness_home,
                platform,
                account,
                channel,
                user,
                agent,
                session,
                "thread-b",
                2_100 + index as i64,
            );
            let digest = prepared.manifest.lane_digest.clone().unwrap();
            assert!(
                lane_digests.insert(digest),
                "{axis} must select a distinct exact-lane prompt ledger"
            );
            let agents = prompt_manifest_entry(&prepared.manifest, "AGENTS.md");
            assert_eq!(
                agents.status,
                crate::AgentPromptManifestStatusV1::Included,
                "{axis} must not inherit the baseline prompt ledger"
            );
            assert_eq!(
                agents.change,
                Some(crate::AgentPromptManifestChangeV1::Added),
                "{axis} must start with an independent manifest"
            );
            if axis == "new-root" {
                assert!(!prepared.markdown.contains("planId=old-root-plan"));
                assert!(
                    prepared
                        .markdown
                        .contains("Active OperationPlans: none visible")
                );
            }
            if axis == "agent" {
                assert_eq!(
                    prompt_manifest_entry(&prepared.manifest, "MEMORY.md").status,
                    crate::AgentPromptManifestStatusV1::Included
                );
                assert!(
                    prepared
                        .markdown
                        .contains("Reviewer-only persistent memory policy")
                );
                assert!(
                    !prepared
                        .markdown
                        .contains("Main agent persistent memory policy")
                );
            }
        }

        let after_sibling = enqueue_and_prepare_prompt_manifest(
            &source,
            &harness_home,
            "telegram",
            "account-a",
            "dm-a",
            "user-a",
            "main",
            "session-root-a",
            "thread-b",
            2_200,
        );
        assert_eq!(
            after_sibling.manifest.lane_digest.as_deref(),
            Some(baseline_digest.as_str())
        );
        assert_eq!(
            prompt_manifest_entry(&after_sibling.manifest, "AGENTS.md").status,
            crate::AgentPromptManifestStatusV1::Reused
        );

        let _ = fs::remove_dir_all(root);
    }

    struct PreparedPromptManifestFixture {
        manifest: crate::AgentPromptManifestV1,
        markdown: String,
    }

    #[allow(clippy::too_many_arguments)]
    fn enqueue_and_prepare_prompt_manifest(
        source: &AgentSource,
        harness_home: &Path,
        platform: &str,
        account_id: &str,
        channel_id: &str,
        user_id: &str,
        agent_id: &str,
        session_key: &str,
        codex_thread_id: &str,
        now_ms: i64,
    ) -> PreparedPromptManifestFixture {
        let registry = load_agent_registry(source).unwrap();
        let skills = build_source_skill_index(source).unwrap();
        let turn = build_turn_plan(
            source,
            &registry,
            &skills,
            TurnPlanInput {
                harness_home: None,
                platform: platform.to_string(),
                channel_id: channel_id.to_string(),
                user_id: user_id.to_string(),
                text: "continue exact work".to_string(),
                inbound_context: None,
                inbound_media_artifacts: Vec::new(),
                requested_agent_id: Some(agent_id.to_string()),
                session_hint: Some(session_key.to_string()),
                skill_limit: 0,
            },
        )
        .unwrap();
        let mut step = build_channel_step(&registry, &turn);
        step.account_id = Some(account_id.to_string());
        let queued = enqueue_channel_step(
            &step,
            RuntimeQueueEnqueueOptions {
                harness_home: harness_home.to_path_buf(),
                runtime_workspace: None,
                inbound_canonical_id: None,
                now_ms,
            },
        )
        .unwrap();
        let queued_item = queued.item.unwrap();
        assert_eq!(queued_item.platform, platform);
        assert_eq!(queued_item.account_id.as_deref(), Some(account_id));
        assert_eq!(queued_item.channel_id, channel_id);
        assert_eq!(queued_item.user_id, user_id);
        assert_eq!(queued_item.agent_id, agent_id);
        assert_eq!(queued_item.session_key, session_key);
        fs::write(
            prompt_codex_binding_file(&queued_item.planned_transcript_file),
            serde_json::json!({ "threadId": codex_thread_id }).to_string(),
        )
        .unwrap();

        let prepared = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.to_path_buf(),
            queue_id: Some(queued_item.queue_id.clone()),
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();
        assert_eq!(
            prepared.receipt.status,
            RuntimeExecutionReceiptStatus::Prepared
        );
        let item = prepared.item.unwrap();
        let bundle: Value =
            serde_json::from_slice(&fs::read(&item.prompt_bundle_json).unwrap()).unwrap();
        let manifest: crate::AgentPromptManifestV1 =
            serde_json::from_value(bundle["promptManifest"].clone()).unwrap();
        let canonical_session =
            crate::channel_session_key::CanonicalChannelSessionKey::bind_for_lane(
                session_key,
                platform,
                account_id,
                channel_id,
                user_id,
                agent_id,
            )
            .unwrap()
            .canonical_string();
        let expected_lane = FullLaneKeyV1::new(
            platform,
            account_id,
            channel_id,
            user_id,
            agent_id,
            "interactive",
            root_working_session_key(&canonical_session),
            &canonical_session,
        )
        .unwrap();
        let expected_lane_digest = expected_lane.identity_hash().unwrap();
        assert_eq!(
            manifest.lane_digest.as_deref(),
            Some(expected_lane_digest.as_str())
        );
        let markdown = fs::read_to_string(&item.prompt_markdown).unwrap();

        append_json_line(
            &queue_dir(harness_home).join("run-once-receipts.jsonl"),
            &serde_json::json!({
                "queueId": item.queue_id,
                "status": "completed",
                "reason": "prompt manifest scenario terminal receipt"
            }),
        )
        .unwrap();
        release_runtime_queue_lease(harness_home, &queued_item.queue_id).unwrap();

        PreparedPromptManifestFixture { manifest, markdown }
    }

    fn prompt_manifest_entry<'a>(
        manifest: &'a crate::AgentPromptManifestV1,
        canonical_name: &str,
    ) -> &'a crate::AgentPromptManifestEntryV1 {
        manifest
            .entries
            .iter()
            .find(|entry| entry.canonical_name == canonical_name)
            .unwrap_or_else(|| panic!("missing manifest entry {canonical_name}"))
    }

    fn write_worker_source(root: &Path) -> AgentSource {
        let home = root.join(".openclaw");
        let workspace = home.join("workspace");
        let skill = workspace.join("skills").join("memory-cron");
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(&skill).unwrap();
        fs::create_dir_all(home.join("agents").join("main").join("sessions")).unwrap();
        fs::write(workspace.join("AGENTS.md"), "# Agent").unwrap();
        fs::write(
            skill.join(crate::SKILL_FILE_NAME),
            "# Memory Cron\n\nRepair openclaw-mem cron jobs.",
        )
        .unwrap();
        fs::write(
            home.join("openclaw.json"),
            r#"{
              "agents": {
                "defaults": { "provider": "openai", "model": "codex" },
                "list": [
                  { "id": "main", "model": "gpt-5", "enabled": true }
                ]
              },
              "models": {
                "providers": {
                  "openai": { "apiKey": "${OPENAI_API_KEY}" }
                }
              }
            }"#,
        )
        .unwrap();
        fs::write(
            home.join("agents")
                .join("main")
                .join("sessions")
                .join("sessions.json"),
            "{}",
        )
        .unwrap();
        AgentSource::with_workspace(home, workspace)
    }

    #[test]
    fn receipt_compaction_commits_cold_history_and_preserves_retry_receipt_sequence() {
        let root = temp_root(
            "receipt_compaction_commits_cold_history_and_preserves_retry_receipt_sequence",
        );
        let harness_home = root.join(".agent-harness");
        let queue_dir = queue_dir(&harness_home);
        fs::create_dir_all(&queue_dir).unwrap();
        fs::write(
            queue_dir.join("pending.jsonl"),
            serde_json::json!({"queueId":"turn:retry","status":"queued"}).to_string(),
        )
        .unwrap();
        fs::write(
            queue_dir.join("run-once-receipts.jsonl"),
            [
                serde_json::json!({
                    "schema":"agent-harness.runtime-run-once.v1",
                    "queueId":"turn:historical",
                    "traceId":"trace:historical",
                    "sessionKey":"session:historical",
                    "status":"completed",
                    "reason":"old terminal",
                    "runtimeClass":"interactive",
                    "origin":"channel",
                    "completedAtMs":10
                })
                .to_string(),
                serde_json::json!({
                    "schema":"agent-harness.runtime-run-once.v1",
                    "queueId":"turn:retry",
                    "status":"failed-retryable",
                    "reason":"first retry failure"
                })
                .to_string(),
                serde_json::json!({
                    "schema":"agent-harness.runtime-run-once.v1",
                    "queueId":"turn:retry",
                    "status":"retry-pending",
                    "reason":"retry remains runnable"
                })
                .to_string(),
            ]
            .join("\n"),
        )
        .unwrap();

        let report =
            compact_runtime_queue_receipts_if_needed(RuntimeQueueReceiptCompactionOptions {
                harness_home: harness_home.clone(),
                max_bytes: 1,
                max_archives: 1,
                now_ms: 100,
            })
            .unwrap();
        assert_eq!(
            report.status,
            RuntimeQueueReceiptCompactionStatus::Compacted
        );

        let hot = fs::read_to_string(queue_dir.join("run-once-receipts.jsonl")).unwrap();
        assert_eq!(
            count_run_once_status(&hot, "turn:retry", "failed-retryable"),
            1,
            "retry accounting must retain prior failure events for a runnable queue"
        );
        assert_eq!(
            count_run_once_status(&hot, "turn:retry", "retry-pending"),
            1
        );
        assert!(!hot.contains("turn:historical"));

        let history = crate::runtime_receipt_history::find_runtime_queue_terminal_history(
            &queue_dir,
            &std::collections::BTreeSet::from(["trace:historical".to_string()]),
        )
        .unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].queue_id, "turn:historical");
        assert_eq!(history[0].status, "completed");
        let status_counts =
            crate::runtime_receipt_history::read_runtime_queue_receipt_history_status_counts(
                &queue_dir,
            )
            .unwrap();
        assert_eq!(status_counts.get("completed"), Some(&1));
        assert!(status_counts.get("failed-retryable").is_none());

        let _ = fs::remove_dir_all(root);
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-runtime-worker-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }

    fn count_run_once_status(receipts: &str, queue_id: &str, status: &str) -> usize {
        receipts
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .filter(|value| {
                value.get("queueId").and_then(Value::as_str) == Some(queue_id)
                    && value.get("status").and_then(Value::as_str) == Some(status)
            })
            .count()
    }
}
