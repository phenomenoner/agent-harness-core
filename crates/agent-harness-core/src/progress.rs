use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config::harness_config_candidates;
use crate::progress_event_index::{
    ProgressEventDeliverySnapshot, latest_progress_event_for_queue,
    progress_event_delivery_snapshot,
};
use crate::runtime_worker::{
    QueueTerminalControl, QueueTerminalControlMatch, refresh_runtime_queue_state_index_nonblocking,
    resolve_queue_terminal_control_from_index,
};
use crate::write_json_atomic;

#[path = "progress_history.rs"]
mod progress_history;

const AGENT_PROGRESS_EVENT_SCHEMA: &str = "agent-harness.progress-event.v2";
const AGENT_PROGRESS_DELIVERY_PLAN_SCHEMA: &str = "agent-harness.progress-delivery-plan.v1";
const AGENT_PROGRESS_DELIVERY_STATE_SCHEMA: &str = "agent-harness.progress-delivery-state.v1";
const AGENT_PROGRESS_DELIVERY_RECEIPT_SCHEMA: &str = "agent-harness.progress-delivery-receipt.v1";
const AGENT_PROGRESS_SURFACE_CLAIM_SCHEMA: &str = "agent-harness.progress-surface-claim.v1";
const AGENT_PROGRESS_SESSION_SUPERSEDE_SCHEMA: &str = "agent-harness.progress-session-supersede.v1";
const DEFAULT_PREVIEW_CHARS: usize = 120;
const DEFAULT_CURRENT_STEP_CHARS: usize = 1200;
const DEFAULT_MAX_NONTERMINAL_UPDATES_PER_LANE: usize = 6;
const DEFAULT_STATUS_HEARTBEAT_AFTER_BODY_CAP_MS: i64 = 5 * 60 * 1000;
const TERMINAL_PROGRESS_STATE_RETENTION_MS: i64 = 10 * 60 * 1000;
/// A new provider progress surface is only meaningful while the latest
/// canonical event is still live.  This is deliberately aligned with the
/// terminal surface retention window: recovery may edit an already-known
/// surface, but it must never create a late message for retained history.
const MAX_FRESH_PROGRESS_SURFACE_AGE_MS: i64 = TERMINAL_PROGRESS_STATE_RETENTION_MS;
const PROGRESS_SURFACE_CLAIM_TTL_MS: i64 = 2 * 60 * 1000;
const SENSITIVE_PREVIEW: &str = "[redacted sensitive preview]";
const MAX_LEGACY_COMPACTED_QUEUE_FALLBACK: usize = 1_024;
const MAX_LEGACY_COMPACTED_EVENTS_PER_QUEUE: usize = 34;
const AGENT_PROGRESS_HISTORY_COMPACTION_SCHEMA: &str =
    "agent-harness.progress-history-compaction.v1";
const AGENT_PROGRESS_HISTORY_MAINTENANCE_STATE_SCHEMA: &str =
    "agent-harness.progress-history-maintenance-state.v1";
/// Bounded hot-journal policy for background maintenance.  Interactive
/// progress reads never invoke compaction; the maintenance owner schedules it
/// independently so a user-visible update cannot wait behind a SQLite move.
pub const DEFAULT_AGENT_PROGRESS_HOT_MAX_BYTES: u64 = 32 * 1024 * 1024;
pub const DEFAULT_AGENT_PROGRESS_HOT_RETAINED_TERMINAL_QUEUES: usize = 1_024;
static PROGRESS_EVENT_ID_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentProgressContext {
    pub queue_id: String,
    pub agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    pub session_key: String,
    pub platform: String,
    pub channel_id: String,
    pub user_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentProgressEvent {
    #[serde(default = "agent_progress_event_schema")]
    pub schema: String,
    /// v2 canonical event identity.  It is assigned immediately before the
    /// JSONL append; old v1 rows intentionally remain line-addressable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
    pub at_ms: i64,
    pub queue_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    pub session_key: String,
    pub platform: String,
    pub channel_id: String,
    pub user_id: String,
    pub kind: AgentProgressKind,
    pub label: String,
    pub preview: String,
    pub status: AgentProgressStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub elapsed_ms: Option<u128>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
}

/// Maintenance policy for moving already-closed v2 progress queues from the
/// hot JSONL journal into the committed SQLite history store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentProgressHistoryCompactionOptions {
    pub harness_home: PathBuf,
    pub now_ms: i64,
    pub max_hot_bytes: u64,
    pub max_terminal_queues: usize,
}

impl Default for AgentProgressHistoryCompactionOptions {
    fn default() -> Self {
        Self {
            harness_home: PathBuf::new(),
            now_ms: 0,
            max_hot_bytes: DEFAULT_AGENT_PROGRESS_HOT_MAX_BYTES,
            max_terminal_queues: DEFAULT_AGENT_PROGRESS_HOT_RETAINED_TERMINAL_QUEUES,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentProgressHistoryCompactionReport {
    pub schema: &'static str,
    pub events_file: PathBuf,
    pub history_file: PathBuf,
    pub marker_file: PathBuf,
    pub scanned_lines: usize,
    pub hot_bytes_before: u64,
    pub hot_bytes_after: u64,
    pub compacted_queues: usize,
    pub compacted_events: usize,
    pub retained_open_queues: usize,
    pub retained_undelivered_queues: usize,
    pub retained_legacy_queues: usize,
    pub retained_terminal_queues: usize,
    pub retained_unclassified_lines: usize,
    pub backpressure: bool,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentProgressHistoryStorage {
    Hot,
    Cold,
}

/// One exact progress-history event.  This API is intentionally queue-scoped;
/// normal delivery planning continues to use the bounded hot projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentProgressHistoryRecord {
    pub storage: AgentProgressHistoryStorage,
    pub original_line: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
    pub event: AgentProgressEvent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentProgressHistoryLookupReport {
    pub events_file: PathBuf,
    pub history_file: PathBuf,
    pub records: Vec<AgentProgressHistoryRecord>,
    pub warnings: Vec<String>,
}

/// One durable observation made by the background maintenance owner. It
/// prevents an unchanged oversized legacy/open ledger from being rescanned on
/// every maintenance wake while still retrying as soon as either source or
/// delivery evidence changes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AgentProgressHistoryMaintenanceState {
    schema: String,
    hot_bytes: u64,
    delivery_receipt_bytes: u64,
    observed_at_ms: i64,
}

/// Stable progress identity for cross-process delivery scheduling.  v1
/// histories expose `event_id: None`, in which case `event_line` remains the
/// compatibility discriminator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentProgressEventIdentity {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
    pub event_line: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentProgressKind {
    Runtime,
    SkillView,
    Todo,
    Terminal,
    SearchFiles,
    ReadFile,
    ExecuteCode,
    ToolCall,
    AssistantStream,
    AssistantNarration,
    MemoryRecall,
    Delivery,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentProgressStatus {
    Started,
    Progress,
    Completed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentProgressDeliveryPlanOptions {
    pub harness_home: PathBuf,
    pub platform: Option<String>,
    pub now_ms: i64,
    pub min_update_interval_ms: i64,
    pub max_nonterminal_updates_per_lane: usize,
    pub status_heartbeat_after_body_cap_ms: i64,
    pub max_events_per_panel: usize,
    pub max_preview_chars: usize,
    pub current_step_max_chars: usize,
}

impl Default for AgentProgressDeliveryPlanOptions {
    fn default() -> Self {
        Self {
            harness_home: PathBuf::new(),
            platform: None,
            now_ms: 0,
            min_update_interval_ms: 2_500,
            max_nonterminal_updates_per_lane: DEFAULT_MAX_NONTERMINAL_UPDATES_PER_LANE,
            status_heartbeat_after_body_cap_ms: DEFAULT_STATUS_HEARTBEAT_AFTER_BODY_CAP_MS,
            max_events_per_panel: 8,
            max_preview_chars: DEFAULT_PREVIEW_CHARS,
            current_step_max_chars: DEFAULT_CURRENT_STEP_CHARS,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentProgressDeliveryPlanReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub events_file: PathBuf,
    pub state_file: PathBuf,
    pub receipts_file: PathBuf,
    pub pending: Vec<AgentProgressDeliveryPending>,
    pub summary: AgentProgressDeliveryPlanSummary,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentProgressDeliveryPlanSummary {
    pub total_events: usize,
    pub new_events: usize,
    pub cached_events: usize,
    pub queues: usize,
    pub pending: usize,
    pub delivered_current: usize,
    pub rate_limited: usize,
    pub volume_limited: usize,
    pub invalid_lines: usize,
    pub skipped_platform: usize,
    pub skipped_muted: usize,
    /// Cached events held back because the canonical source/index pair was
    /// not freshly read on this pass.  The cache remains durable for a later
    /// fresh projection rather than being replayed to a provider.
    #[serde(default)]
    pub deferred_cached_events: usize,
    /// Queue-level fresh sends prevented because no provider surface exists
    /// and the latest canonical event is already historical.
    #[serde(default)]
    pub skipped_stale_fresh_sends: usize,
    pub read_from_byte: u64,
    pub read_to_byte: u64,
    #[serde(default)]
    pub index_source_line: usize,
    #[serde(default)]
    pub index_total_lines: usize,
    #[serde(default)]
    pub index_projection_generation: u64,
    #[serde(default)]
    pub index_valid_lines: usize,
    #[serde(default)]
    pub index_invalid_lines: usize,
    #[serde(default)]
    pub index_source_valid_delta: usize,
    #[serde(default)]
    pub index_projected_delta_events: usize,
    #[serde(default)]
    pub index_snapshot_used: bool,
    #[serde(default)]
    pub index_available: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentProgressSessionSupersedeOptions {
    pub harness_home: PathBuf,
    pub platform: String,
    pub account_id: Option<String>,
    pub channel_id: String,
    pub user_id: String,
    pub agent_id: Option<String>,
    pub session_key: String,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentProgressSessionSupersedeReport {
    pub schema: &'static str,
    pub state_file: PathBuf,
    pub receipts_file: PathBuf,
    pub platform: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account_id: Option<String>,
    pub channel_id: String,
    pub user_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    pub session_key: String,
    pub removed_queue_ids: Vec<String>,
    pub removed_claim_files: Vec<PathBuf>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentProgressDeliveryPending {
    pub queue_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    pub platform: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account_id: Option<String>,
    pub channel_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    pub user_id: String,
    pub session_key: String,
    pub message_kind: AgentProgressDeliveryMessageKind,
    pub action: AgentProgressDeliveryAction,
    pub provider_message_id: Option<String>,
    pub event_line: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
    pub terminal: bool,
    pub text: String,
    pub text_hash: String,
    pub started_at_ms: i64,
    pub latest_at_ms: i64,
    pub status_surface_key: String,
    pub idempotency_hit: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub progress_suppressed_reason: Option<String>,
    pub fresh_send_reason: Option<AgentProgressDeliveryFreshSendReason>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentProgressDeliveryMessageKind {
    Body,
    Status,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentProgressDeliveryAction {
    Send,
    Edit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentProgressDeliveryRecordOptions {
    pub harness_home: PathBuf,
    pub queue_id: String,
    pub platform: String,
    pub account_id: Option<String>,
    pub channel_id: String,
    pub thread_id: Option<String>,
    pub user_id: String,
    pub session_key: String,
    pub message_kind: AgentProgressDeliveryMessageKind,
    pub action: AgentProgressDeliveryAction,
    pub status: AgentProgressDeliveryStatus,
    pub provider_message_id: Option<String>,
    pub event_line: usize,
    pub text_hash: String,
    pub terminal: bool,
    pub policy_decision: Option<String>,
    pub error: Option<String>,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentProgressDeliveryRecordContext {
    pub status_surface_key: String,
    /// Stable identity emitted by the trusted delivery planner.  Provider
    /// adapters pass this through unchanged; it is never model-authored.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub existing_provider_message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fresh_send_reason: Option<AgentProgressDeliveryFreshSendReason>,
    #[serde(default)]
    pub idempotency_hit: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress_suppressed_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentProgressDeliveryReceipt {
    pub schema: String,
    pub at_ms: i64,
    pub queue_id: String,
    pub platform: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_id: Option<String>,
    pub channel_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    pub user_id: String,
    pub session_key: String,
    pub message_kind: AgentProgressDeliveryMessageKind,
    pub action: AgentProgressDeliveryAction,
    pub status: AgentProgressDeliveryStatus,
    pub provider_message_id: Option<String>,
    pub event_line: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
    pub text_hash: String,
    pub terminal: bool,
    #[serde(default)]
    pub status_surface_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub existing_provider_message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_decision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fresh_send_reason: Option<AgentProgressDeliveryFreshSendReason>,
    #[serde(default)]
    pub idempotency_hit: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress_suppressed_reason: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentProgressDeliveryFreshSendReason {
    NoExistingStatusSurface,
    ExistingProviderMessageExpired,
    EditProviderNotFound,
    EditProviderRejectedTerminal,
    ExplicitNewSurfaceRequested,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentProgressDeliveryStatus {
    Delivered,
    Failed,
    SkippedDenied,
    SkippedPermanent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AgentProgressDeliveryMode {
    On,
    Off,
}

impl Default for AgentProgressDeliveryMode {
    fn default() -> Self {
        Self::On
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct AgentProgressDeliveryMuteConfig {
    default_mode: AgentProgressDeliveryMode,
    agent_modes: BTreeMap<String, AgentProgressDeliveryMode>,
    channel_modes: BTreeMap<String, AgentProgressDeliveryMode>,
    max_nonterminal_updates_per_lane: Option<usize>,
    status_heartbeat_after_body_cap_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AgentProgressDeliveryState {
    schema: String,
    #[serde(default)]
    queues: BTreeMap<String, AgentProgressDeliveryCursor>,
    #[serde(default)]
    ledger: AgentProgressDeliveryLedgerCursor,
    #[serde(default)]
    compacted_events: BTreeMap<String, Vec<StoredProgressEvent>>,
    #[serde(default)]
    superseded_sessions: BTreeMap<String, AgentProgressSupersededSession>,
}

impl Default for AgentProgressDeliveryState {
    fn default() -> Self {
        Self {
            schema: AGENT_PROGRESS_DELIVERY_STATE_SCHEMA.to_string(),
            queues: BTreeMap::new(),
            ledger: AgentProgressDeliveryLedgerCursor::default(),
            compacted_events: BTreeMap::new(),
            superseded_sessions: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AgentProgressSupersededSession {
    platform: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    account_id: Option<String>,
    channel_id: String,
    user_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    agent_id: Option<String>,
    session_key: String,
    superseded_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AgentProgressSurfaceClaim {
    schema: String,
    status_surface_key: String,
    queue_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    agent_id: Option<String>,
    platform: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    account_id: Option<String>,
    channel_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    thread_id: Option<String>,
    user_id: String,
    session_key: String,
    message_kind: AgentProgressDeliveryMessageKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    provider_message_id: Option<String>,
    event_line: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    event_id: Option<String>,
    text_hash: String,
    terminal: bool,
    claimed_at_ms: i64,
    expires_at_ms: i64,
    updated_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ProgressSurfaceClaimOutcome {
    Claimed {
        fresh_send_reason: AgentProgressDeliveryFreshSendReason,
    },
    ExistingProvider {
        provider_message_id: String,
    },
    ActivePending,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AgentProgressDeliveryLedgerCursor {
    #[serde(default)]
    offset_bytes: u64,
    #[serde(default)]
    line_number: usize,
    #[serde(default)]
    index_projection_generation: u64,
    #[serde(default)]
    index_valid_lines: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AgentProgressDeliveryCursor {
    platform: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    account_id: Option<String>,
    channel_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    thread_id: Option<String>,
    user_id: String,
    session_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    body_provider_message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    status_provider_message_id: Option<String>,
    #[serde(default, skip_serializing_if = "is_zero_usize")]
    body_last_event_line: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    body_last_event_id: Option<String>,
    #[serde(default, skip_serializing_if = "is_zero_usize")]
    status_last_event_line: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    status_last_event_id: Option<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    body_last_text_hash: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    status_last_text_hash: String,
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    body_last_sent_at_ms: i64,
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    status_last_sent_at_ms: i64,
    #[serde(default, skip_serializing_if = "is_zero_usize")]
    body_nonterminal_deliveries: usize,
    #[serde(default, skip_serializing_if = "is_zero_usize")]
    status_nonterminal_deliveries: usize,
    #[serde(default, skip_serializing_if = "is_false")]
    body_terminal: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    status_terminal: bool,
    #[serde(default, skip_serializing_if = "is_zero_usize")]
    terminal_event_line: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    terminal_event_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    provider_message_id: Option<String>,
    #[serde(default, skip_serializing_if = "is_zero_usize")]
    last_event_line: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_event_id: Option<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    last_text_hash: String,
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    last_sent_at_ms: i64,
    #[serde(default)]
    terminal: bool,
}

impl AgentProgressDeliveryCursor {
    fn new(platform: String, channel_id: String, user_id: String, session_key: String) -> Self {
        Self {
            platform,
            account_id: None,
            channel_id,
            thread_id: None,
            user_id,
            session_key,
            body_provider_message_id: None,
            status_provider_message_id: None,
            body_last_event_line: 0,
            body_last_event_id: None,
            status_last_event_line: 0,
            status_last_event_id: None,
            body_last_text_hash: String::new(),
            status_last_text_hash: String::new(),
            body_last_sent_at_ms: 0,
            status_last_sent_at_ms: 0,
            body_nonterminal_deliveries: 0,
            status_nonterminal_deliveries: 0,
            body_terminal: false,
            status_terminal: false,
            terminal_event_line: 0,
            terminal_event_id: None,
            provider_message_id: None,
            last_event_line: 0,
            last_event_id: None,
            last_text_hash: String::new(),
            last_sent_at_ms: 0,
            terminal: false,
        }
    }

    fn provider_message_id_for(
        &self,
        message_kind: AgentProgressDeliveryMessageKind,
    ) -> Option<&String> {
        match message_kind {
            AgentProgressDeliveryMessageKind::Body => self
                .body_provider_message_id
                .as_ref()
                .or(self.provider_message_id.as_ref()),
            AgentProgressDeliveryMessageKind::Status => self.status_provider_message_id.as_ref(),
        }
    }

    fn last_text_hash_for(&self, message_kind: AgentProgressDeliveryMessageKind) -> &str {
        match message_kind {
            AgentProgressDeliveryMessageKind::Body => {
                if self.body_last_text_hash.is_empty() {
                    &self.last_text_hash
                } else {
                    &self.body_last_text_hash
                }
            }
            AgentProgressDeliveryMessageKind::Status => &self.status_last_text_hash,
        }
    }

    fn last_sent_at_ms_for(&self, message_kind: AgentProgressDeliveryMessageKind) -> i64 {
        match message_kind {
            AgentProgressDeliveryMessageKind::Body => {
                if self.body_last_sent_at_ms == 0 {
                    self.last_sent_at_ms
                } else {
                    self.body_last_sent_at_ms
                }
            }
            AgentProgressDeliveryMessageKind::Status => self.status_last_sent_at_ms,
        }
    }

    fn terminal_recorded_for(&self, message_kind: AgentProgressDeliveryMessageKind) -> bool {
        match message_kind {
            AgentProgressDeliveryMessageKind::Body => {
                self.body_terminal
                    || (self.terminal
                        // A pre-v2 cursor had one shared terminal surface.
                        // Preserve that fallback only when it has no
                        // body-lane data at all.  A delivered terminal status
                        // must not make a failed body terminal look delivered.
                        && self.body_provider_message_id.is_none()
                        && self.body_last_event_line == 0
                        && self.body_last_event_id.is_none()
                        && self.body_last_text_hash.is_empty()
                        && self.body_last_sent_at_ms == 0
                        && (self.provider_message_id.is_some()
                            || self.last_event_line > 0
                            || !self.last_text_hash.is_empty()
                            || self.last_sent_at_ms > 0))
            }
            AgentProgressDeliveryMessageKind::Status => self.status_terminal,
        }
    }

    fn terminal_closed_event_line(&self) -> usize {
        self.terminal_event_line
            .max(if self.body_terminal {
                self.body_last_event_line
            } else {
                0
            })
            .max(if self.status_terminal {
                self.status_last_event_line
            } else {
                0
            })
            .max(if self.terminal {
                self.last_event_line
            } else {
                0
            })
    }

    #[cfg(test)]
    fn record_lane(
        &mut self,
        message_kind: AgentProgressDeliveryMessageKind,
        provider_message_id: Option<String>,
        event_line: usize,
        text_hash: String,
        sent_at_ms: i64,
        terminal: bool,
        delivered: bool,
    ) {
        self.record_lane_with_identity(
            message_kind,
            provider_message_id,
            event_line,
            None,
            text_hash,
            sent_at_ms,
            terminal,
            delivered,
        );
    }

    fn record_lane_with_identity(
        &mut self,
        message_kind: AgentProgressDeliveryMessageKind,
        provider_message_id: Option<String>,
        event_line: usize,
        event_id: Option<String>,
        text_hash: String,
        sent_at_ms: i64,
        terminal: bool,
        delivered: bool,
    ) {
        let stable_event_id = event_id.clone();
        match message_kind {
            AgentProgressDeliveryMessageKind::Body => {
                self.body_provider_message_id = provider_message_id;
                self.body_last_event_line = event_line;
                self.body_last_event_id = event_id.clone();
                self.body_last_text_hash = text_hash;
                self.body_last_sent_at_ms = sent_at_ms;
                if delivered && !terminal {
                    self.body_nonterminal_deliveries =
                        self.body_nonterminal_deliveries.saturating_add(1);
                }
                self.body_terminal = self.body_terminal || terminal;
            }
            AgentProgressDeliveryMessageKind::Status => {
                self.status_provider_message_id = provider_message_id;
                self.status_last_event_line = event_line;
                self.status_last_event_id = event_id.clone();
                self.status_last_text_hash = text_hash;
                self.status_last_sent_at_ms = sent_at_ms;
                if delivered && !terminal {
                    self.status_nonterminal_deliveries =
                        self.status_nonterminal_deliveries.saturating_add(1);
                }
                self.status_terminal = self.status_terminal || terminal;
            }
        }
        if terminal {
            self.terminal_event_line = self.terminal_event_line.max(event_line);
            if event_id.is_some() {
                self.terminal_event_id = event_id;
            }
        }
        self.last_event_line = event_line;
        if stable_event_id.is_some() {
            self.last_event_id = stable_event_id;
        }
        self.terminal = self.terminal || terminal;
    }

    fn terminal_closed_before_event(&self, event_id: Option<&str>, event_line: usize) -> bool {
        if let Some(event_id) = valid_progress_event_id(event_id) {
            let closed_event_id = self
                .terminal_event_id
                .as_deref()
                .or_else(|| {
                    self.body_terminal
                        .then_some(self.body_last_event_id.as_deref())
                        .flatten()
                })
                .or_else(|| {
                    self.status_terminal
                        .then_some(self.status_last_event_id.as_deref())
                        .flatten()
                })
                .or_else(|| {
                    self.terminal
                        .then_some(self.last_event_id.as_deref())
                        .flatten()
                });
            return closed_event_id
                .and_then(|closed_event_id| valid_progress_event_id(Some(closed_event_id)))
                .is_some_and(|closed_event_id| closed_event_id != event_id);
        }
        let terminal_closed_event_line = self.terminal_closed_event_line();
        terminal_closed_event_line > 0 && event_line > terminal_closed_event_line
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StoredProgressEvent {
    line_number: usize,
    event: AgentProgressEvent,
}

impl AgentProgressEvent {
    pub fn new(
        context: &AgentProgressContext,
        kind: AgentProgressKind,
        label: impl Into<String>,
        preview: impl AsRef<str>,
        status: AgentProgressStatus,
        at_ms: i64,
    ) -> Self {
        Self::new_with_preview_limit(context, kind, label, preview, status, at_ms, 512)
    }

    pub fn new_with_preview_limit(
        context: &AgentProgressContext,
        kind: AgentProgressKind,
        label: impl Into<String>,
        preview: impl AsRef<str>,
        status: AgentProgressStatus,
        at_ms: i64,
        max_preview_chars: usize,
    ) -> Self {
        Self {
            schema: AGENT_PROGRESS_EVENT_SCHEMA.to_string(),
            event_id: None,
            at_ms,
            queue_id: context.queue_id.clone(),
            agent_id: context.agent_id.clone(),
            account_id: context.account_id.clone(),
            thread_id: context.thread_id.clone(),
            session_key: context.session_key.clone(),
            platform: context.platform.clone(),
            channel_id: context.channel_id.clone(),
            user_id: context.user_id.clone(),
            kind,
            label: label.into(),
            preview: sanitize_progress_preview(preview.as_ref(), max_preview_chars.max(1)),
            status,
            elapsed_ms: None,
            source: None,
        }
    }

    pub fn source(mut self, source: impl Into<String>) -> Self {
        self.source = Some(source.into());
        self
    }

    pub fn elapsed_ms(mut self, elapsed_ms: u128) -> Self {
        self.elapsed_ms = Some(elapsed_ms);
        self
    }

    fn ensure_canonical_identity(&mut self) {
        if valid_progress_event_id(self.event_id.as_deref()).is_none() {
            self.event_id = Some(new_progress_event_id(self));
        }
        self.schema = AGENT_PROGRESS_EVENT_SCHEMA.to_string();
    }
}

pub fn agent_progress_events_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("runtime-queue")
        .join("progress-events.jsonl")
}

/// Returns the cold exact-history store paired with `progress-events.jsonl`.
/// The file is a committed-history sidecar, never an interactive delivery
/// source.
pub fn agent_progress_history_file(harness_home: impl AsRef<Path>) -> PathBuf {
    progress_history::progress_history_file(harness_home.as_ref())
}

/// Returns the recovery marker for an interrupted progress-history move.
/// Operators normally only inspect this file; appenders and exact readers
/// resolve it automatically before using the hot source.
pub fn agent_progress_history_marker_file(harness_home: impl AsRef<Path>) -> PathBuf {
    progress_history::progress_history_marker_file(harness_home.as_ref())
}

/// Moves only fully closed and delivery-resolved canonical v2 queues into cold
/// history.  Unsafe data is retained in hot storage and reported as explicit
/// backpressure rather than evicted.
pub fn compact_agent_progress_history(
    options: AgentProgressHistoryCompactionOptions,
) -> io::Result<AgentProgressHistoryCompactionReport> {
    let state_file = agent_progress_delivery_state_file(&options.harness_home);
    let receipts_file = agent_progress_delivery_receipts_file(&options.harness_home);
    let mut warnings = Vec::new();
    let state = read_delivery_state(&state_file, &mut warnings)?;
    let delivered_terminal_events = progress_terminal_delivery_evidence(
        &options.harness_home,
        &state,
        &receipts_file,
        &mut warnings,
    )?;
    let result = progress_history::compact_progress_history(
        &options.harness_home,
        progress_history::ProgressHistoryCompactionPolicy {
            max_hot_bytes: options.max_hot_bytes,
            max_terminal_queues: options.max_terminal_queues,
        },
        &delivered_terminal_events,
        options.now_ms,
        &mut warnings,
    )?;
    Ok(AgentProgressHistoryCompactionReport {
        schema: AGENT_PROGRESS_HISTORY_COMPACTION_SCHEMA,
        events_file: result.events_file,
        history_file: result.history_file,
        marker_file: result.marker_file,
        scanned_lines: result.scanned_lines,
        hot_bytes_before: result.hot_bytes_before,
        hot_bytes_after: result.hot_bytes_after,
        compacted_queues: result.compacted_queues,
        compacted_events: result.compacted_events,
        retained_open_queues: result.retained_open_queues,
        retained_undelivered_queues: result.retained_undelivered_queues,
        retained_legacy_queues: result.retained_legacy_queues,
        retained_terminal_queues: result.retained_terminal_queues,
        retained_unclassified_lines: result.retained_unclassified_lines,
        backpressure: result.backpressure,
        warnings,
    })
}

/// Performs the inexpensive gate for background progress-history maintenance.
///
/// Normal progress planning never calls this path. The gate reads only file
/// metadata and a tiny durable observation record; a full hot-ledger scan is
/// allowed only after the bounded byte threshold is crossed, an interrupted
/// compaction needs recovery, an operator forces maintenance, or the relevant
/// source/delivery evidence changed since the last attempted scan.
pub fn compact_agent_progress_history_if_needed(
    harness_home: impl AsRef<Path>,
    now_ms: i64,
    force: bool,
) -> io::Result<Option<AgentProgressHistoryCompactionReport>> {
    let harness_home = harness_home.as_ref();
    let events_file = agent_progress_events_file(harness_home);
    let events_metadata = match fs::metadata(&events_file) {
        Ok(metadata) if metadata.is_file() => metadata,
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "progress event path is not a file: {}",
                    events_file.display()
                ),
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let marker_file = agent_progress_history_marker_file(harness_home);
    let recovery_required = marker_file.is_file();
    if !force && !recovery_required && events_metadata.len() <= DEFAULT_AGENT_PROGRESS_HOT_MAX_BYTES
    {
        return Ok(None);
    }

    let receipts_file = agent_progress_delivery_receipts_file(harness_home);
    let receipt_bytes = fs::metadata(&receipts_file)
        .map(|metadata| metadata.len())
        .unwrap_or_default();
    let state_file = agent_progress_history_maintenance_state_file(harness_home);
    if !force
        && !recovery_required
        && let Some(previous) = read_agent_progress_history_maintenance_state(&state_file)?
        && previous.hot_bytes == events_metadata.len()
        && previous.delivery_receipt_bytes == receipt_bytes
    {
        return Ok(None);
    }

    let report = compact_agent_progress_history(AgentProgressHistoryCompactionOptions {
        harness_home: harness_home.to_path_buf(),
        now_ms,
        ..AgentProgressHistoryCompactionOptions::default()
    })?;
    let observed_hot_bytes = fs::metadata(&events_file)
        .map(|metadata| metadata.len())
        .unwrap_or(report.hot_bytes_after);
    let observed_receipt_bytes = fs::metadata(&receipts_file)
        .map(|metadata| metadata.len())
        .unwrap_or(receipt_bytes);
    write_json_atomic(
        &state_file,
        &AgentProgressHistoryMaintenanceState {
            schema: AGENT_PROGRESS_HISTORY_MAINTENANCE_STATE_SCHEMA.to_string(),
            hot_bytes: observed_hot_bytes,
            delivery_receipt_bytes: observed_receipt_bytes,
            observed_at_ms: now_ms,
        },
    )?;
    Ok(Some(report))
}

/// Durable state used only by the background compaction gate. It is not a
/// progress-delivery input and may be recreated if unavailable.
pub fn agent_progress_history_maintenance_state_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("runtime-queue")
        .join("progress-history-maintenance-state.json")
}

fn read_agent_progress_history_maintenance_state(
    state_file: &Path,
) -> io::Result<Option<AgentProgressHistoryMaintenanceState>> {
    let text = match fs::read_to_string(state_file) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    if text.trim().is_empty() {
        return Ok(None);
    }
    let state =
        serde_json::from_str::<AgentProgressHistoryMaintenanceState>(&text).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "invalid progress history maintenance state {}: {error}",
                    state_file.display()
                ),
            )
        })?;
    if state.schema != AGENT_PROGRESS_HISTORY_MAINTENANCE_STATE_SCHEMA {
        return Ok(None);
    }
    Ok(Some(state))
}

/// Performs an exact queue-scoped history lookup across both the committed
/// cold store and the current hot ledger.  It is intentionally not used by
/// interactive progress delivery, whose normal path stays on the bounded
/// projection sidecar.
pub fn read_agent_progress_history_for_queue_ids(
    harness_home: impl AsRef<Path>,
    queue_ids: &BTreeSet<String>,
) -> io::Result<AgentProgressHistoryLookupReport> {
    let harness_home = harness_home.as_ref();
    let mut warnings = Vec::new();
    let records = progress_history::read_progress_history_for_queue_ids(
        harness_home,
        queue_ids,
        &mut warnings,
    )?
    .into_iter()
    .map(|record| AgentProgressHistoryRecord {
        storage: if record.cold {
            AgentProgressHistoryStorage::Cold
        } else {
            AgentProgressHistoryStorage::Hot
        },
        original_line: record.original_line,
        event_id: valid_progress_event_id(record.event.event_id.as_deref()).map(str::to_string),
        event: record.event,
    })
    .collect();
    Ok(AgentProgressHistoryLookupReport {
        events_file: agent_progress_events_file(harness_home),
        history_file: agent_progress_history_file(harness_home),
        records,
        warnings,
    })
}

pub fn agent_progress_delivery_state_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("channels")
        .join("progress-delivery-state.json")
}

pub fn agent_progress_delivery_receipts_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("channels")
        .join("progress-delivery-receipts.jsonl")
}

pub fn agent_progress_session_supersede_receipts_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("progress")
        .join("session-supersede-receipts.jsonl")
}

pub fn append_agent_progress_event(
    harness_home: impl AsRef<Path>,
    event: &AgentProgressEvent,
) -> io::Result<PathBuf> {
    let harness_home = harness_home.as_ref();
    // A crash between the hot replacement and cold transaction commit leaves
    // a marker.  Resolve it before accepting a new append so the append is
    // never written into an ambiguous source generation.
    let _ = progress_history::recover_progress_history_compaction_if_needed(
        harness_home,
        &mut Vec::new(),
    )?;
    let file = agent_progress_events_file(harness_home);
    let mut canonical = event.clone();
    canonical.ensure_canonical_identity();
    append_json_line(&file, &canonical)?;
    // Timestamp collection is diagnostic only.  It uses a bounded projection
    // and deliberately cannot turn an already-durable progress append into a
    // retryable interactive failure.
    let _ = crate::latency::record_latency_stage(
        crate::latency::latency_receipts_file(harness_home),
        &canonical.queue_id,
        "progress",
        crate::latency::LatencyStage::FirstProgressEvent,
        Some(canonical.at_ms),
    );
    if is_terminal_event(&canonical) {
        let _ = crate::latency::record_latency_stage(
            crate::latency::latency_receipts_file(harness_home),
            &canonical.queue_id,
            "progress",
            crate::latency::LatencyStage::TerminalEvent,
            Some(canonical.at_ms),
        );
    }
    let wake_file = harness_home
        .join("state")
        .join("wake")
        .join("progress-delivery.json");
    let _ = crate::wake::signal_wake(
        harness_home,
        wake_file,
        "progress-delivery",
        "agent progress event appended",
    );
    Ok(file)
}

pub fn supersede_agent_progress_session_surfaces(
    options: AgentProgressSessionSupersedeOptions,
) -> io::Result<AgentProgressSessionSupersedeReport> {
    let state_file = agent_progress_delivery_state_file(&options.harness_home);
    let receipts_file = agent_progress_session_supersede_receipts_file(&options.harness_home);
    let mut warnings = Vec::new();
    let mut state = read_delivery_state(&state_file, &mut warnings)?;
    let superseded = AgentProgressSupersededSession {
        platform: options.platform.clone(),
        account_id: options.account_id.clone(),
        channel_id: options.channel_id.clone(),
        user_id: options.user_id.clone(),
        agent_id: options.agent_id.clone(),
        session_key: options.session_key.clone(),
        superseded_at_ms: options.now_ms,
    };
    state.superseded_sessions.insert(
        progress_session_key(
            &options.platform,
            options.account_id.as_deref(),
            &options.channel_id,
            &options.user_id,
            options.agent_id.as_deref(),
            &options.session_key,
        ),
        superseded.clone(),
    );

    let removed_queue_ids = state
        .queues
        .iter()
        .filter_map(|(queue_id, cursor)| {
            progress_cursor_matches_superseded_session(cursor, &superseded)
                .then(|| queue_id.clone())
        })
        .collect::<Vec<_>>();
    for queue_id in &removed_queue_ids {
        state.queues.remove(queue_id);
        state.compacted_events.remove(queue_id);
    }
    state.compacted_events.retain(|_, events| {
        events.retain(|stored| {
            !progress_event_matches_superseded_session(&stored.event, &superseded)
        });
        !events.is_empty()
    });

    let mut removed_claim_files = Vec::new();
    let claims_dir = progress_surface_claims_dir(&options.harness_home);
    if claims_dir.is_dir() {
        for entry in fs::read_dir(&claims_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("json") {
                continue;
            }
            match read_progress_surface_claim(&path) {
                Ok(Some(claim))
                    if progress_claim_matches_superseded_session(&claim, &superseded) =>
                {
                    match fs::remove_file(&path) {
                        Ok(()) => removed_claim_files.push(path),
                        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                        Err(error) => return Err(error),
                    }
                }
                Ok(_) => {}
                Err(error) => warnings.push(format!(
                    "progress surface claim could not be read during supersede at {}: {}",
                    path.display(),
                    error
                )),
            }
        }
    }

    write_delivery_state(&state_file, &state)?;
    let report = AgentProgressSessionSupersedeReport {
        schema: AGENT_PROGRESS_SESSION_SUPERSEDE_SCHEMA,
        state_file,
        receipts_file: receipts_file.clone(),
        platform: options.platform,
        account_id: options.account_id,
        channel_id: options.channel_id,
        user_id: options.user_id,
        agent_id: options.agent_id,
        session_key: options.session_key,
        removed_queue_ids,
        removed_claim_files,
        warnings,
    };
    append_json_line(&receipts_file, &report)?;
    Ok(report)
}

pub fn plan_agent_progress_delivery(
    options: AgentProgressDeliveryPlanOptions,
) -> io::Result<AgentProgressDeliveryPlanReport> {
    let events_file = agent_progress_events_file(&options.harness_home);
    let state_file = agent_progress_delivery_state_file(&options.harness_home);
    let receipts_file = agent_progress_delivery_receipts_file(&options.harness_home);
    let mut warnings = Vec::new();
    let mute_config = load_progress_delivery_mute_config(&options.harness_home, &mut warnings)?;
    let max_nonterminal_updates_per_lane = mute_config
        .max_nonterminal_updates_per_lane
        .unwrap_or(options.max_nonterminal_updates_per_lane);
    // Kept for backwards-compatible config parsing only. Once the body panel
    // is capped, status remains a live activity surface and is governed by the
    // normal short update interval rather than a multi-minute heartbeat.
    let _legacy_status_heartbeat_after_body_cap_ms = mute_config
        .status_heartbeat_after_body_cap_ms
        .unwrap_or(options.status_heartbeat_after_body_cap_ms)
        .max(0);
    let mut state = read_delivery_state(&state_file, &mut warnings)?;
    prune_old_terminal_delivery_state(&mut state, options.now_ms);
    let trimmed_legacy_cache =
        bound_legacy_compacted_progress_state(&mut state, options.max_events_per_panel);
    if trimmed_legacy_cache > 0 {
        warnings.push(format!(
            "trimmed {trimmed_legacy_cache} stale progress delivery cache queue(s); indexed progress state is authoritative"
        ));
    }
    let progress_history_recovery =
        progress_history::try_recover_progress_history_compaction_if_needed(
            &options.harness_home,
            &mut warnings,
        )?;
    let progress_history_busy = matches!(
        progress_history_recovery,
        progress_history::ProgressHistoryCompactionRecovery::Busy
    );
    if progress_history_busy {
        warnings.push(
            "progress history compaction owns the hot append lock; skipped the progress projection for this pass rather than reading a stale source/index pair"
                .to_string(),
        );
    }
    let queue_dir = options.harness_home.join("state").join("runtime-queue");
    let terminal_control_index =
        match refresh_runtime_queue_state_index_nonblocking(&queue_dir, &mut warnings) {
            Ok(index) => Some(index),
            Err(error) => {
                warnings.push(format!(
                    "progress delivery terminal-control index rebuild failed open: {error}"
                ));
                None
            }
        };
    let previous_ledger = state.ledger.clone();
    let known_queue_ids = state
        .queues
        .keys()
        .chain(state.compacted_events.keys())
        .filter(|queue_id| !queue_id.trim().is_empty())
        .cloned()
        .collect::<BTreeSet<_>>();
    let progress_snapshot = if progress_history_busy {
        ProgressEventDeliverySnapshot::default()
    } else {
        progress_event_delivery_snapshot(
            &options.harness_home,
            &known_queue_ids,
            previous_ledger.line_number,
            previous_ledger.index_projection_generation,
            previous_ledger.index_valid_lines,
            previous_ledger.index_projection_generation == 0,
            &mut warnings,
        )?
    };
    let reset_cached_events = progress_snapshot.fresh && progress_snapshot.reset;
    let cached_events = if reset_cached_events {
        BTreeMap::new()
    } else {
        state.compacted_events.clone()
    };
    let cached_event_count = cached_events.values().map(Vec::len).sum();
    let mut all_by_queue = cached_events;
    for (queue_id, indexed_events) in &progress_snapshot.events {
        let current_events = indexed_events
            .iter()
            .map(|indexed| StoredProgressEvent {
                line_number: indexed.line_number,
                event: indexed.event.clone(),
            })
            .collect::<Vec<_>>();
        all_by_queue.insert(queue_id.clone(), current_events);
    }
    for queue_events in all_by_queue.values_mut() {
        queue_events.sort_by_key(|stored| stored.line_number);
    }
    let mut summary = AgentProgressDeliveryPlanSummary {
        total_events: all_by_queue.values().map(Vec::len).sum(),
        // Preserve the legacy meaning of `newEvents` as source-valid delta
        // count, while exposing the smaller projected delivery delta
        // separately.  A large historical backlog must not be materialized
        // merely to make this counter precise.
        new_events: progress_snapshot.source_valid_delta,
        cached_events: cached_event_count,
        read_from_byte: previous_ledger.offset_bytes,
        read_to_byte: if progress_snapshot.available {
            progress_snapshot.cursor.offset_bytes
        } else {
            previous_ledger.offset_bytes
        },
        invalid_lines: progress_snapshot.cursor.invalid_lines,
        index_source_line: progress_snapshot.cursor.line_number,
        index_total_lines: progress_snapshot.cursor.total_lines,
        index_projection_generation: progress_snapshot.cursor.projection_generation,
        index_valid_lines: progress_snapshot.cursor.valid_lines,
        index_invalid_lines: progress_snapshot.cursor.invalid_lines,
        index_source_valid_delta: progress_snapshot.source_valid_delta,
        index_projected_delta_events: progress_snapshot.observed_delta_events,
        index_snapshot_used: progress_snapshot.used_last_committed_snapshot,
        index_available: progress_snapshot.available,
        ..AgentProgressDeliveryPlanSummary::default()
    };
    if !progress_snapshot.fresh {
        summary.deferred_cached_events = cached_event_count;
        warnings.push(
            "deferred cached progress delivery because the progress source index snapshot is not fresh; a fresh source pass must rebuild the delivery plan"
                .to_string(),
        );
        // Preserve the bounded compatibility cache.  A non-fresh snapshot is
        // useful for diagnostics, but it is not authority to create or edit
        // a real provider surface.  In particular, a restart must not replay
        // retained historical progress while an append/index transaction is
        // still in flight.
        write_delivery_state(&state_file, &state)?;
        return Ok(AgentProgressDeliveryPlanReport {
            schema: AGENT_PROGRESS_DELIVERY_PLAN_SCHEMA,
            harness_home: options.harness_home,
            events_file,
            state_file,
            receipts_file,
            pending: Vec::new(),
            summary,
            warnings,
        });
    }
    let mut by_queue = BTreeMap::<String, Vec<StoredProgressEvent>>::new();
    let mut terminal_control_close_reasons = BTreeMap::<String, String>::new();
    for (queue_id, queue_events) in all_by_queue {
        let mut kept = Vec::new();
        for stored in queue_events {
            if options
                .platform
                .as_ref()
                .is_some_and(|platform| platform != &stored.event.platform)
            {
                summary.skipped_platform += 1;
                continue;
            }
            if progress_event_session_superseded(&state, &stored.event) {
                summary.delivered_current += 1;
                continue;
            }
            if !progress_delivery_enabled_for_event(&mute_config, &stored.event) {
                summary.skipped_muted += 1;
                continue;
            }
            kept.push(stored);
        }
        if !kept.is_empty() {
            let has_existing_provider_surface = state.queues.get(&queue_id).is_some_and(|cursor| {
                cursor
                    .provider_message_id_for(AgentProgressDeliveryMessageKind::Body)
                    .is_some()
                    || cursor
                        .provider_message_id_for(AgentProgressDeliveryMessageKind::Status)
                        .is_some()
            });
            if !has_existing_provider_surface
                && kept.last().is_some_and(|stored| {
                    progress_event_is_historical_for_fresh_surface(&stored.event, options.now_ms)
                })
            {
                // Do not retain this queue in the compatibility cache.  Once
                // the fresh projection advances, it should not be reselected
                // merely to keep suppressing the same historical send.
                summary.skipped_stale_fresh_sends += 1;
                continue;
            }
            let has_terminal_event = kept.iter().any(|stored| is_terminal_event(&stored.event));
            if state
                .queues
                .get(&queue_id)
                .is_some_and(|cursor| cursor.terminal)
                && !has_terminal_event
            {
                // A queue terminal is a monotonic boundary.  In particular,
                // a v2 terminal receipt must not be re-opened merely because
                // source compaction relocated older non-terminal lines.
                summary.delivered_current += kept.len();
                continue;
            }
            if !has_terminal_event {
                let session_key = kept.last().map(|stored| stored.event.session_key.as_str());
                if let Some(control) = terminal_control_for_progress_queue(
                    &queue_dir,
                    terminal_control_index.as_ref(),
                    &queue_id,
                    session_key,
                    &mut warnings,
                ) {
                    let _ =
                        remove_progress_surface_claims_for_queue(&options.harness_home, &queue_id)?;
                    let cursor = state.queues.get(&queue_id);
                    let has_open_surface = cursor.is_some_and(|cursor| {
                        cursor
                            .provider_message_id_for(AgentProgressDeliveryMessageKind::Body)
                            .is_some()
                            || cursor
                                .provider_message_id_for(AgentProgressDeliveryMessageKind::Status)
                                .is_some()
                    });
                    summary.delivered_current += kept.len();
                    if !has_open_surface {
                        state.queues.remove(&queue_id);
                        state.compacted_events.remove(&queue_id);
                        continue;
                    }
                    let synthetic = terminal_control_suppressed_progress_events(
                        kept.last().expect("kept is not empty"),
                        &control,
                        options.now_ms,
                    );
                    kept.clear();
                    kept.extend(synthetic);
                    terminal_control_close_reasons
                        .insert(queue_id.clone(), "terminal-control-present".to_string());
                }
            }
            by_queue.insert(queue_id, kept);
        }
    }
    if summary.skipped_stale_fresh_sends > 0 {
        warnings.push(format!(
            "suppressed {} historical fresh progress surface queue(s) without an existing provider surface",
            summary.skipped_stale_fresh_sends
        ));
    }
    if progress_snapshot.fresh {
        state.ledger = AgentProgressDeliveryLedgerCursor {
            offset_bytes: progress_snapshot.cursor.offset_bytes,
            line_number: progress_snapshot.cursor.line_number,
            index_projection_generation: progress_snapshot.cursor.projection_generation,
            index_valid_lines: progress_snapshot.cursor.valid_lines,
        };
    }
    state.compacted_events =
        compact_progress_events_by_queue(&by_queue, options.max_events_per_panel);
    write_delivery_state(&state_file, &state)?;
    summary.queues = by_queue.len();

    let mut pending = Vec::new();
    for (queue_id, mut queue_events) in by_queue {
        queue_events.sort_by_key(|stored| stored.line_number);
        let Some(first) = queue_events.first() else {
            continue;
        };
        let Some(latest) = queue_events.last() else {
            continue;
        };
        let event_refs = queue_events
            .iter()
            .map(|stored| &stored.event)
            .collect::<Vec<_>>();
        let terminal = latest_terminal_event(event_refs.as_slice()).is_some();
        let cursor = state.queues.get(&queue_id);
        if terminal
            && final_source_delivery_is_still_pending(
                &options.harness_home,
                &queue_id,
                &latest.event,
                &mut warnings,
            )?
        {
            continue;
        }
        let body_cap_reached = cursor.is_some_and(|cursor| {
            !terminal
                && max_nonterminal_updates_per_lane > 0
                && cursor.body_nonterminal_deliveries >= max_nonterminal_updates_per_lane
                && cursor
                    .provider_message_id_for(AgentProgressDeliveryMessageKind::Body)
                    .is_some()
        });
        let lanes = [
            (
                AgentProgressDeliveryMessageKind::Body,
                render_agent_progress_actions(
                    event_refs.as_slice(),
                    options.max_events_per_panel,
                    options.max_preview_chars,
                ),
            ),
            (
                AgentProgressDeliveryMessageKind::Status,
                render_agent_progress_status(
                    event_refs.as_slice(),
                    options.now_ms,
                    options.max_preview_chars,
                    options.current_step_max_chars,
                    body_cap_reached,
                ),
            ),
        ];

        for (message_kind, text) in lanes {
            if text.trim().is_empty() {
                continue;
            }
            if let Some(cursor) = cursor
                && cursor.terminal
                && terminal
            {
                if cursor.terminal_recorded_for(message_kind)
                    || cursor.terminal_closed_before_event(
                        latest.event.event_id.as_deref(),
                        latest.line_number,
                    )
                {
                    summary.delivered_current += 1;
                    continue;
                }
            }
            let text_hash = fnv1a_64_hex(&text);
            let provider_message_id =
                cursor.and_then(|cursor| cursor.provider_message_id_for(message_kind).cloned());
            if cursor.is_some_and(|cursor| {
                cursor.last_text_hash_for(message_kind) == text_hash && cursor.terminal == terminal
            }) {
                summary.delivered_current += 1;
                continue;
            }
            if let Some(cursor) = cursor
                && provider_message_id.is_some()
                && !terminal
                && options
                    .now_ms
                    .saturating_sub(cursor.last_sent_at_ms_for(message_kind))
                    < progress_delivery_min_interval_for_lane(options.min_update_interval_ms)
            {
                summary.rate_limited += 1;
                continue;
            }
            if let Some(cursor) = cursor
                && provider_message_id.is_some()
                && !terminal
                && message_kind == AgentProgressDeliveryMessageKind::Body
                && max_nonterminal_updates_per_lane > 0
                && cursor.body_nonterminal_deliveries >= max_nonterminal_updates_per_lane
            {
                summary.volume_limited += 1;
                continue;
            }
            let mut provider_message_id = provider_message_id;
            let mut action = if provider_message_id.is_some() {
                AgentProgressDeliveryAction::Edit
            } else {
                AgentProgressDeliveryAction::Send
            };
            let progress_suppressed_reason = terminal_control_close_reasons.get(&queue_id).cloned();
            if progress_suppressed_reason.is_some() && provider_message_id.is_none() {
                summary.delivered_current += 1;
                continue;
            }
            let status_surface_key =
                status_surface_key_for_event(&latest.event, &queue_id, message_kind);
            let mut idempotency_hit = false;
            let fresh_send_reason = if provider_message_id.is_none() {
                match acquire_progress_surface_claim(
                    &options.harness_home,
                    &status_surface_key,
                    &queue_id,
                    &latest.event,
                    message_kind,
                    latest.line_number,
                    &text_hash,
                    terminal,
                    options.now_ms,
                    &mut warnings,
                )? {
                    ProgressSurfaceClaimOutcome::Claimed { fresh_send_reason } => {
                        Some(fresh_send_reason)
                    }
                    ProgressSurfaceClaimOutcome::ExistingProvider {
                        provider_message_id: existing_provider_message_id,
                    } => {
                        provider_message_id = Some(existing_provider_message_id);
                        action = AgentProgressDeliveryAction::Edit;
                        idempotency_hit = true;
                        None
                    }
                    ProgressSurfaceClaimOutcome::ActivePending => {
                        summary.delivered_current += 1;
                        continue;
                    }
                }
            } else {
                None
            };
            pending.push(AgentProgressDeliveryPending {
                queue_id: queue_id.clone(),
                agent_id: latest.event.agent_id.clone(),
                platform: latest.event.platform.clone(),
                account_id: latest.event.account_id.clone(),
                channel_id: latest.event.channel_id.clone(),
                thread_id: latest.event.thread_id.clone(),
                user_id: latest.event.user_id.clone(),
                session_key: latest.event.session_key.clone(),
                message_kind,
                action,
                provider_message_id,
                event_line: latest.line_number,
                event_id: valid_progress_event_id(latest.event.event_id.as_deref())
                    .map(str::to_string),
                terminal,
                text,
                text_hash,
                started_at_ms: first.event.at_ms,
                latest_at_ms: latest.event.at_ms,
                status_surface_key,
                idempotency_hit,
                progress_suppressed_reason,
                fresh_send_reason,
            });
        }
    }
    summary.pending = pending.len();

    Ok(AgentProgressDeliveryPlanReport {
        schema: AGENT_PROGRESS_DELIVERY_PLAN_SCHEMA,
        harness_home: options.harness_home,
        events_file,
        state_file,
        receipts_file,
        pending,
        summary,
        warnings,
    })
}

fn load_progress_delivery_mute_config(
    harness_home: impl AsRef<Path>,
    warnings: &mut Vec<String>,
) -> io::Result<AgentProgressDeliveryMuteConfig> {
    let harness_home = harness_home.as_ref();
    let Some(config_file) = harness_config_candidates(harness_home)
        .into_iter()
        .find(|path| path.is_file())
    else {
        return Ok(AgentProgressDeliveryMuteConfig::default());
    };
    let text = fs::read_to_string(&config_file)?;
    let value = match serde_json::from_str::<Value>(&text) {
        Ok(value) => value,
        Err(error) => {
            warnings.push(format!(
                "failed to parse {} for progress delivery mute config: {error}; using defaults",
                config_file.display()
            ));
            return Ok(AgentProgressDeliveryMuteConfig::default());
        }
    };
    let Some(response) = value.get("response").and_then(Value::as_object) else {
        return Ok(AgentProgressDeliveryMuteConfig::default());
    };

    let mut config = AgentProgressDeliveryMuteConfig::default();
    if let Some(mode) = response
        .get("progressDeliveryMode")
        .or_else(|| response.get("progress_delivery_mode"))
    {
        match parse_progress_delivery_mode_value(mode) {
            Some(mode) => config.default_mode = mode,
            None => warnings.push(format!(
                "unknown response.progressDeliveryMode in {}; using {}",
                config_file.display(),
                progress_delivery_mode_name(config.default_mode)
            )),
        }
    }
    load_progress_delivery_mode_map(
        response
            .get("progressDeliveryAgentModes")
            .or_else(|| response.get("progress_delivery_agent_modes")),
        &mut config.agent_modes,
        "response.progressDeliveryAgentModes",
        &config_file,
        warnings,
    );
    load_progress_delivery_mode_map(
        response
            .get("progressDeliveryChannelModes")
            .or_else(|| response.get("progress_delivery_channel_modes")),
        &mut config.channel_modes,
        "response.progressDeliveryChannelModes",
        &config_file,
        warnings,
    );
    if let Some(value) = response
        .get("progressDeliveryMaxNonterminalUpdatesPerLane")
        .or_else(|| response.get("progress_delivery_max_nonterminal_updates_per_lane"))
        .or_else(|| response.get("progressDeliveryMaxNonterminalBodyUpdatesPerQueue"))
        .or_else(|| response.get("progress_delivery_max_nonterminal_body_updates_per_queue"))
    {
        match value.as_u64().and_then(|value| usize::try_from(value).ok()) {
            Some(value) => config.max_nonterminal_updates_per_lane = Some(value),
            None => warnings.push(format!(
                "response.progressDeliveryMaxNonterminalUpdatesPerLane in {} must be a non-negative integer; using {}",
                config_file.display(),
                DEFAULT_MAX_NONTERMINAL_UPDATES_PER_LANE
            )),
        }
    }
    if let Some(value) = response
        .get("progressDeliveryStatusHeartbeatAfterBodyCapMs")
        .or_else(|| response.get("progress_delivery_status_heartbeat_after_body_cap_ms"))
    {
        match value.as_i64().or_else(|| value.as_u64().and_then(|v| i64::try_from(v).ok())) {
            Some(value) if value >= 0 => {
                config.status_heartbeat_after_body_cap_ms = Some(value);
            }
            _ => warnings.push(format!(
                "response.progressDeliveryStatusHeartbeatAfterBodyCapMs in {} must be a non-negative integer; using {}",
                config_file.display(),
                DEFAULT_STATUS_HEARTBEAT_AFTER_BODY_CAP_MS
            )),
        }
    }
    Ok(config)
}

fn progress_delivery_min_interval_for_lane(min_update_interval_ms: i64) -> i64 {
    min_update_interval_ms
}

fn load_progress_delivery_mode_map(
    value: Option<&Value>,
    target: &mut BTreeMap<String, AgentProgressDeliveryMode>,
    label: &str,
    config_file: &Path,
    warnings: &mut Vec<String>,
) {
    let Some(value) = value else {
        return;
    };
    let Some(object) = value.as_object() else {
        warnings.push(format!(
            "{label} in {} must be an object; ignoring it",
            config_file.display()
        ));
        return;
    };
    for (key, value) in object {
        let key = key.trim();
        if key.is_empty() {
            warnings.push(format!(
                "{label} in {} contains an empty key; ignoring it",
                config_file.display()
            ));
            continue;
        }
        match parse_progress_delivery_mode_value(value) {
            Some(mode) => {
                target.insert(key.to_string(), mode);
            }
            None => warnings.push(format!(
                "unknown {label}.{key} in {}; ignoring it",
                config_file.display()
            )),
        }
    }
}

fn parse_progress_delivery_mode_value(value: &Value) -> Option<AgentProgressDeliveryMode> {
    value.as_str().and_then(parse_progress_delivery_mode)
}

fn parse_progress_delivery_mode(value: &str) -> Option<AgentProgressDeliveryMode> {
    match value.trim().to_ascii_lowercase().as_str() {
        "on" | "enabled" | "enable" | "true" | "progress_panel" | "progress-panel" => {
            Some(AgentProgressDeliveryMode::On)
        }
        "off" | "none" | "hidden" | "disabled" | "disable" | "false" | "mute" | "muted" => {
            Some(AgentProgressDeliveryMode::Off)
        }
        _ => None,
    }
}

fn progress_delivery_mode_name(mode: AgentProgressDeliveryMode) -> &'static str {
    match mode {
        AgentProgressDeliveryMode::On => "on",
        AgentProgressDeliveryMode::Off => "off",
    }
}

fn progress_delivery_enabled_for_event(
    config: &AgentProgressDeliveryMuteConfig,
    event: &AgentProgressEvent,
) -> bool {
    for key in progress_delivery_channel_match_keys(event) {
        if let Some(mode) = config.channel_modes.get(&key) {
            return *mode == AgentProgressDeliveryMode::On;
        }
    }
    if let Some(agent_id) = event
        .agent_id
        .as_deref()
        .map(str::trim)
        .filter(|agent_id| !agent_id.is_empty())
        && let Some(mode) = config.agent_modes.get(agent_id)
    {
        return *mode == AgentProgressDeliveryMode::On;
    }
    config.default_mode == AgentProgressDeliveryMode::On
}

fn progress_delivery_channel_match_keys(event: &AgentProgressEvent) -> Vec<String> {
    let platform = event.platform.trim();
    let channel_id = event.channel_id.trim();
    let thread_id = event
        .thread_id
        .as_deref()
        .map(str::trim)
        .filter(|thread_id| !thread_id.is_empty());
    let mut keys = Vec::new();
    // Most specific keys win: platform+thread, platform+channel, raw thread, raw channel.
    if !platform.is_empty() && !channel_id.is_empty() {
        if let Some(thread_id) = thread_id {
            keys.push(format!("{platform}:{channel_id}:thread:{thread_id}"));
        }
        keys.push(format!("{platform}:{channel_id}"));
    }
    if !channel_id.is_empty() {
        if let Some(thread_id) = thread_id {
            keys.push(format!("{channel_id}:thread:{thread_id}"));
        }
        keys.push(channel_id.to_string());
    }
    keys
}

pub fn record_agent_progress_delivery(
    options: AgentProgressDeliveryRecordOptions,
) -> io::Result<AgentProgressDeliveryReceipt> {
    let status_surface_key = status_surface_key_from_parts(
        &options.platform,
        options.account_id.as_deref(),
        &options.channel_id,
        &options.user_id,
        None,
        &options.session_key,
        &options.queue_id,
        options.message_kind,
    );
    let status_surface_key =
        resolve_progress_surface_key_for_record(&options, &status_surface_key)?;
    let fresh_send_reason = if options.action == AgentProgressDeliveryAction::Send {
        Some(AgentProgressDeliveryFreshSendReason::NoExistingStatusSurface)
    } else {
        None
    };
    let decision = Some(
        match options.action {
            AgentProgressDeliveryAction::Send => "send",
            AgentProgressDeliveryAction::Edit => "edit",
        }
        .to_string(),
    );
    record_agent_progress_delivery_with_context(
        options,
        AgentProgressDeliveryRecordContext {
            status_surface_key,
            event_id: None,
            existing_provider_message_id: None,
            decision,
            fresh_send_reason,
            idempotency_hit: false,
            progress_suppressed_reason: None,
        },
    )
}

pub fn record_agent_progress_delivery_with_context(
    options: AgentProgressDeliveryRecordOptions,
    context: AgentProgressDeliveryRecordContext,
) -> io::Result<AgentProgressDeliveryReceipt> {
    let harness_home = options.harness_home.clone();
    let state_file = agent_progress_delivery_state_file(&options.harness_home);
    let receipts_file = agent_progress_delivery_receipts_file(&options.harness_home);
    let mut warnings = Vec::new();
    let mut state = read_delivery_state(&state_file, &mut warnings)?;
    let event_id = context
        .event_id
        .as_deref()
        .and_then(|event_id| valid_progress_event_id(Some(event_id)))
        .map(str::to_string)
        .or(progress_event_id_for_record(
            &options,
            &context.status_surface_key,
        )?);
    let fresh_send_reason = if options.action == AgentProgressDeliveryAction::Send {
        context.fresh_send_reason.or(Some(
            AgentProgressDeliveryFreshSendReason::NoExistingStatusSurface,
        ))
    } else {
        None
    };
    let decision = context.decision.or_else(|| {
        Some(
            match options.action {
                AgentProgressDeliveryAction::Send => "send",
                AgentProgressDeliveryAction::Edit => "edit",
            }
            .to_string(),
        )
    });
    let mut receipt = AgentProgressDeliveryReceipt {
        schema: AGENT_PROGRESS_DELIVERY_RECEIPT_SCHEMA.to_string(),
        at_ms: options.now_ms,
        queue_id: options.queue_id,
        platform: options.platform,
        account_id: options.account_id,
        channel_id: options.channel_id,
        thread_id: options.thread_id,
        user_id: options.user_id,
        session_key: options.session_key,
        message_kind: options.message_kind,
        action: options.action,
        status: options.status,
        provider_message_id: options.provider_message_id,
        event_line: options.event_line,
        event_id,
        text_hash: options.text_hash,
        terminal: options.terminal,
        status_surface_key: context.status_surface_key,
        existing_provider_message_id: context.existing_provider_message_id,
        decision,
        policy_decision: options.policy_decision,
        fresh_send_reason,
        idempotency_hit: context.idempotency_hit,
        progress_suppressed_reason: context.progress_suppressed_reason,
        error: options.error,
    };

    if matches!(
        receipt.status,
        AgentProgressDeliveryStatus::Delivered
            | AgentProgressDeliveryStatus::SkippedDenied
            | AgentProgressDeliveryStatus::SkippedPermanent
    ) {
        let cursor = state
            .queues
            .entry(receipt.queue_id.clone())
            .or_insert_with(|| {
                AgentProgressDeliveryCursor::new(
                    receipt.platform.clone(),
                    receipt.channel_id.clone(),
                    receipt.user_id.clone(),
                    receipt.session_key.clone(),
                )
            });
        cursor.platform = receipt.platform.clone();
        cursor.account_id = receipt.account_id.clone();
        cursor.channel_id = receipt.channel_id.clone();
        cursor.thread_id = receipt.thread_id.clone();
        cursor.user_id = receipt.user_id.clone();
        cursor.session_key = receipt.session_key.clone();
        receipt.terminal = cursor.terminal || receipt.terminal;
        cursor.record_lane_with_identity(
            receipt.message_kind,
            receipt.provider_message_id.clone(),
            receipt.event_line,
            receipt.event_id.clone(),
            receipt.text_hash.clone(),
            receipt.at_ms,
            receipt.terminal,
            receipt.status == AgentProgressDeliveryStatus::Delivered,
        );
        if receipt.progress_suppressed_reason.as_deref() == Some("terminal-control-present")
            && progress_cursor_all_terminal_surfaces_recorded(cursor)
        {
            state.compacted_events.remove(&receipt.queue_id);
        }
        write_delivery_state(&state_file, &state)?;
    }
    update_progress_surface_claim_from_receipt(&harness_home, &receipt)?;
    append_json_line(&receipts_file, &receipt)?;
    if receipt.status == AgentProgressDeliveryStatus::Delivered && !receipt.terminal {
        // This receipt is recorded only after the CLI/provider path has
        // accepted the status surface. Keep instrumentation best-effort so a
        // diagnostic lock can never delay an acknowledged Working update.
        let _ = crate::latency::record_latency_stage(
            crate::latency::latency_receipts_file(&harness_home),
            &receipt.queue_id,
            "progress-delivery",
            crate::latency::LatencyStage::FirstProviderProgressSurface,
            Some(receipt.at_ms),
        );
        if receipt.message_kind == AgentProgressDeliveryMessageKind::Status {
            let _ = crate::latency::record_latency_stage(
                crate::latency::latency_receipts_file(&harness_home),
                &receipt.queue_id,
                "progress-delivery",
                crate::latency::LatencyStage::WorkingIndicator,
                Some(receipt.at_ms),
            );
        }
    }
    if receipt.terminal {
        // Retention is deliberately owned by a separate loop.  A progress
        // delivery acknowledgement is latency-sensitive and must not replay
        // or rewrite the history ledger after it has reached the provider.
        let _ = crate::request_ledger_maintenance(
            &harness_home,
            "terminal progress delivery receipt recorded",
        );
    }
    Ok(receipt)
}

pub fn render_agent_progress_panel(
    events: &[&AgentProgressEvent],
    now_ms: i64,
    max_events: usize,
    max_preview_chars: usize,
) -> String {
    let actions = render_agent_progress_actions(events, max_events, max_preview_chars);
    let status = render_agent_progress_status(
        events,
        now_ms,
        max_preview_chars,
        DEFAULT_CURRENT_STEP_CHARS,
        false,
    );
    if actions.trim().is_empty() {
        status
    } else {
        format!("{actions}\n\n{status}")
    }
}

fn render_agent_progress_actions(
    events: &[&AgentProgressEvent],
    max_events: usize,
    max_preview_chars: usize,
) -> String {
    let mut actions = Vec::<RenderedAction>::new();
    let start = events.len().saturating_sub(max_events.max(1));
    for event in events.iter().skip(start) {
        if !is_rendered_action_event(event) {
            continue;
        }
        let line = render_action_line(event, max_preview_chars);
        if let Some(last) = actions.last_mut()
            && last.line == line
        {
            last.count += 1;
            continue;
        }
        actions.push(RenderedAction { line, count: 1 });
    }

    let mut out = String::new();
    for action in &actions {
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str(&action.line);
        if action.count > 1 {
            out.push_str(&format!(" (x{})", action.count));
        }
    }
    out
}

fn render_agent_progress_status(
    events: &[&AgentProgressEvent],
    now_ms: i64,
    max_preview_chars: usize,
    current_step_max_chars: usize,
    body_cap_reached: bool,
) -> String {
    if events.is_empty() {
        return "⏳ Working — <1 min — starting".to_string();
    }
    let first_at_ms = events.first().map(|event| event.at_ms).unwrap_or(now_ms);
    let Some(latest) = latest_terminal_event(events).or_else(|| latest_status_event(events)) else {
        return format!(
            "⏳ Working — {} — working",
            format_elapsed(now_ms.saturating_sub(first_at_ms))
        );
    };
    let terminal = is_terminal_event(latest);
    let elapsed = format_elapsed(progress_status_elapsed_ms(
        first_at_ms,
        latest,
        now_ms,
        terminal,
    ));
    if terminal {
        match latest.status {
            AgentProgressStatus::Failed => {
                format!(
                    "⚠️ Failed — {} — {}",
                    elapsed,
                    quote_safe_preview(&latest.preview, max_preview_chars)
                )
            }
            _ => format!("✅ Done — {elapsed}"),
        }
    } else {
        let mut status = format!("⏳ Working — {} — {}", elapsed, status_phrase(latest));
        if let Some(narration) = latest_current_step_event(events) {
            status.push_str("\nCurrent step: ");
            status.push_str(&quote_safe_preview(
                &narration.preview,
                current_step_max_chars,
            ));
        }
        if body_cap_reached {
            if let Some(activity) = latest_rendered_activity_event(events) {
                status.push_str("\nLatest activity: ");
                status.push_str(&quote_safe_preview(&activity.preview, max_preview_chars));
            }
            status.push_str("\nUpdates capped; still working.");
            status.push_str(&format!(
                "\nLatest internal event: {}ms",
                events.last().map(|event| event.at_ms).unwrap_or(now_ms)
            ));
        }
        status
    }
}

fn is_rendered_action_event(event: &AgentProgressEvent) -> bool {
    matches!(
        event.kind,
        AgentProgressKind::SkillView
            | AgentProgressKind::Todo
            | AgentProgressKind::Terminal
            | AgentProgressKind::SearchFiles
            | AgentProgressKind::ExecuteCode
            | AgentProgressKind::ToolCall
    )
}

fn latest_status_event<'a>(events: &[&'a AgentProgressEvent]) -> Option<&'a AgentProgressEvent> {
    events.iter().rev().copied().find(|event| {
        !matches!(
            event.kind,
            AgentProgressKind::AssistantStream | AgentProgressKind::AssistantNarration
        )
    })
}

fn latest_terminal_event<'a>(events: &[&'a AgentProgressEvent]) -> Option<&'a AgentProgressEvent> {
    events
        .iter()
        .rev()
        .copied()
        .find(|event| is_terminal_event(event))
}

fn latest_narration_event<'a>(events: &[&'a AgentProgressEvent]) -> Option<&'a AgentProgressEvent> {
    events
        .iter()
        .rev()
        .copied()
        .find(|event| event.kind == AgentProgressKind::AssistantNarration)
}

fn latest_current_step_event<'a>(
    events: &[&'a AgentProgressEvent],
) -> Option<&'a AgentProgressEvent> {
    latest_narration_event(events)
}

/// After the action/body lane reaches its cap, the status lane remains the
/// bounded live activity surface.  Keep tool text out of the general
/// `Current step` field (which is intentionally narration-only), but expose a
/// sanitized latest rendered action so the user can still see forward motion.
fn latest_rendered_activity_event<'a>(
    events: &[&'a AgentProgressEvent],
) -> Option<&'a AgentProgressEvent> {
    events
        .iter()
        .rev()
        .copied()
        .find(|event| is_rendered_action_event(event))
}

pub fn sanitize_progress_preview(value: &str, max_chars: usize) -> String {
    let flattened = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if looks_sensitive(&flattened) {
        return SENSITIVE_PREVIEW.to_string();
    }
    truncate_chars(&flattened, max_chars)
}

#[cfg(test)]
fn read_progress_events(
    events_file: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<Vec<StoredProgressEvent>> {
    if !events_file.is_file() {
        return Ok(Vec::new());
    }
    let text = fs::read_to_string(events_file)?;
    let mut events = Vec::new();
    for (index, line) in text.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        match serde_json::from_str::<AgentProgressEvent>(trimmed) {
            Ok(event) => events.push(StoredProgressEvent {
                line_number: index + 1,
                event,
            }),
            Err(error) => warnings.push(format!(
                "progress event line {} is not valid JSON: {}",
                index + 1,
                error
            )),
        }
    }
    Ok(events)
}

fn compact_progress_events_by_queue(
    by_queue: &BTreeMap<String, Vec<StoredProgressEvent>>,
    max_events_per_panel: usize,
) -> BTreeMap<String, Vec<StoredProgressEvent>> {
    let retain_recent = max_events_per_panel.max(16);
    let mut compacted = BTreeMap::new();
    for (queue_id, queue_events) in by_queue {
        if queue_events.is_empty() {
            continue;
        }
        let coalesced = coalesce_stored_progress_events(queue_events);
        let mut selected = BTreeMap::<usize, StoredProgressEvent>::new();
        if let Some(first) = queue_events.first() {
            selected.insert(first.line_number, first.clone());
        }
        for stored in &coalesced {
            if is_terminal_event(&stored.event) {
                selected.insert(stored.line_number, stored.clone());
            }
        }
        for stored in coalesced.iter().rev().take(retain_recent) {
            selected.insert(stored.line_number, stored.clone());
        }
        compacted.insert(queue_id.clone(), selected.into_values().collect());
    }
    compacted
}

fn coalesce_stored_progress_events(events: &[StoredProgressEvent]) -> Vec<StoredProgressEvent> {
    let mut coalesced = Vec::<StoredProgressEvent>::new();
    for stored in events {
        if let Some(last) = coalesced.last_mut()
            && can_coalesce_progress_event(&last.event, &stored.event)
        {
            *last = stored.clone();
            continue;
        }
        coalesced.push(stored.clone());
    }
    coalesced
}

fn can_coalesce_progress_event(left: &AgentProgressEvent, right: &AgentProgressEvent) -> bool {
    if is_terminal_event(left) || is_terminal_event(right) {
        return false;
    }
    matches!(
        left.kind,
        AgentProgressKind::ToolCall
            | AgentProgressKind::AssistantStream
            | AgentProgressKind::AssistantNarration
            | AgentProgressKind::SearchFiles
            | AgentProgressKind::ReadFile
            | AgentProgressKind::ExecuteCode
    ) && left.queue_id == right.queue_id
        && left.kind == right.kind
        && left.label == right.label
        && left.preview == right.preview
        && left.status == right.status
        && left.platform == right.platform
        && left.channel_id == right.channel_id
        && left.user_id == right.user_id
        && left.session_key == right.session_key
}

fn read_delivery_state(
    state_file: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<AgentProgressDeliveryState> {
    if !state_file.is_file() {
        return Ok(AgentProgressDeliveryState::default());
    }
    let text = fs::read_to_string(state_file)?;
    match serde_json::from_str::<AgentProgressDeliveryState>(&text) {
        Ok(state) => Ok(state),
        Err(error) => {
            warnings.push(format!(
                "progress delivery state file is invalid at {}: {}",
                state_file.display(),
                error
            ));
            Ok(AgentProgressDeliveryState::default())
        }
    }
}

fn prune_old_terminal_delivery_state(state: &mut AgentProgressDeliveryState, now_ms: i64) {
    if now_ms <= 0 {
        return;
    }
    let cutoff_ms = now_ms.saturating_sub(TERMINAL_PROGRESS_STATE_RETENTION_MS);
    let old_terminal_queues = state
        .queues
        .iter()
        .filter_map(|(queue_id, cursor)| {
            let latest_sent_at_ms = cursor
                .body_last_sent_at_ms
                .max(cursor.status_last_sent_at_ms)
                .max(cursor.last_sent_at_ms);
            (cursor.terminal && latest_sent_at_ms > 0 && latest_sent_at_ms <= cutoff_ms)
                .then(|| queue_id.clone())
        })
        .collect::<Vec<_>>();
    for queue_id in old_terminal_queues {
        state.queues.remove(&queue_id);
        state.compacted_events.remove(&queue_id);
    }
}

/// The JSON delivery-state cache predates the source-authoritative SQLite
/// projection.  Keep it only as a bounded compatibility fallback for queues
/// that are still delivering or are absent from a temporarily unavailable
/// sidecar; new normal reads replace it from the index.
fn bound_legacy_compacted_progress_state(
    state: &mut AgentProgressDeliveryState,
    max_events_per_panel: usize,
) -> usize {
    let original_queue_count = state.compacted_events.len();
    let retain_recent = max_events_per_panel
        .max(16)
        .min(MAX_LEGACY_COMPACTED_EVENTS_PER_QUEUE);
    let active_queue_ids = state.queues.keys().cloned().collect::<BTreeSet<_>>();
    let mut retained_stale_queues = 0usize;
    let mut bounded = BTreeMap::new();
    for (queue_id, events) in &state.compacted_events {
        let active = active_queue_ids.contains(queue_id);
        if !active && retained_stale_queues >= MAX_LEGACY_COMPACTED_QUEUE_FALLBACK {
            continue;
        }
        if !active {
            retained_stale_queues = retained_stale_queues.saturating_add(1);
        }
        let events = bounded_legacy_progress_events(events, retain_recent);
        if !events.is_empty() {
            bounded.insert(queue_id.clone(), events);
        }
    }
    state.compacted_events = bounded;
    original_queue_count.saturating_sub(state.compacted_events.len())
}

fn bounded_legacy_progress_events(
    events: &[StoredProgressEvent],
    retain_recent: usize,
) -> Vec<StoredProgressEvent> {
    if events.len() <= retain_recent.saturating_add(2) {
        return events.to_vec();
    }
    let mut selected = BTreeMap::<usize, StoredProgressEvent>::new();
    if let Some(first) = events.first() {
        selected.insert(first.line_number, first.clone());
    }
    for event in events {
        if is_terminal_event(&event.event) {
            selected.insert(event.line_number, event.clone());
        }
    }
    for event in events.iter().rev().take(retain_recent) {
        selected.insert(event.line_number, event.clone());
    }
    selected.into_values().collect()
}

fn progress_cursor_all_terminal_surfaces_recorded(cursor: &AgentProgressDeliveryCursor) -> bool {
    cursor.terminal
        && cursor
            .provider_message_id_for(AgentProgressDeliveryMessageKind::Body)
            .is_none_or(|_| cursor.terminal_recorded_for(AgentProgressDeliveryMessageKind::Body))
        && cursor
            .provider_message_id_for(AgentProgressDeliveryMessageKind::Status)
            .is_none_or(|_| cursor.terminal_recorded_for(AgentProgressDeliveryMessageKind::Status))
}

fn progress_event_is_historical_for_fresh_surface(event: &AgentProgressEvent, now_ms: i64) -> bool {
    now_ms > 0
        && (event.at_ms <= 0
            || now_ms.saturating_sub(event.at_ms) > MAX_FRESH_PROGRESS_SURFACE_AGE_MS)
}

/// Produces durable proof that a specific canonical terminal event no longer
/// needs a hot delivery pass.  The live state is the fast proof; once it ages
/// out, both terminal delivery lanes in the append-only receipt journal are a
/// conservative historical proof.  A missing/legacy identity is never
/// inferred from a physical line number here.
fn progress_terminal_delivery_evidence(
    harness_home: &Path,
    state: &AgentProgressDeliveryState,
    receipts_file: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<BTreeSet<(String, String)>> {
    let mut evidence = BTreeSet::new();
    for (queue_id, cursor) in &state.queues {
        if !progress_cursor_all_terminal_surfaces_recorded(cursor) {
            continue;
        }
        if let Some(event_id) = progress_cursor_terminal_event_id(cursor) {
            evidence.insert((queue_id.clone(), event_id.to_string()));
        }
    }

    let mut receipt_lanes = BTreeMap::<(String, String), (bool, bool)>::new();
    if receipts_file.is_file() {
        let file = File::open(receipts_file)?;
        let mut warning_budget = 16usize;
        for (line_index, line) in BufReader::new(file).lines().enumerate() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let receipt = match serde_json::from_str::<AgentProgressDeliveryReceipt>(&line) {
                Ok(receipt) => receipt,
                Err(error) => {
                    if warning_budget > 0 {
                        warning_budget = warning_budget.saturating_sub(1);
                        warnings.push(format!(
                            "progress history compaction ignored invalid delivery receipt line {} at {}: {}",
                            line_index.saturating_add(1),
                            receipts_file.display(),
                            error
                        ));
                    }
                    continue;
                }
            };
            if !receipt.terminal
                || !matches!(
                    receipt.status,
                    AgentProgressDeliveryStatus::Delivered
                        | AgentProgressDeliveryStatus::SkippedDenied
                        | AgentProgressDeliveryStatus::SkippedPermanent
                )
            {
                continue;
            }
            let Some(event_id) = valid_progress_event_id(receipt.event_id.as_deref()) else {
                continue;
            };
            let lane = receipt_lanes
                .entry((receipt.queue_id, event_id.to_string()))
                .or_default();
            match receipt.message_kind {
                AgentProgressDeliveryMessageKind::Body => lane.0 = true,
                AgentProgressDeliveryMessageKind::Status => lane.1 = true,
            }
        }
    }
    for ((queue_id, event_id), (body_terminal, status_terminal)) in receipt_lanes {
        if body_terminal && status_terminal {
            evidence.insert((queue_id, event_id));
        }
    }
    // A provider-less surface claim denotes an in-flight send/edit decision.
    // Keep that queue hot even when an older terminal receipt exists: moving
    // it would discard data that a concurrent dispatcher still owns.
    let claims_dir = progress_surface_claims_dir(harness_home);
    if claims_dir.is_dir() {
        for entry in fs::read_dir(&claims_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("json") {
                continue;
            }
            let Some(claim) = read_progress_surface_claim(&path)? else {
                continue;
            };
            if claim
                .provider_message_id
                .as_deref()
                .is_none_or(str::is_empty)
            {
                let queue_id = claim.queue_id;
                evidence.retain(|(candidate_queue_id, _)| candidate_queue_id != &queue_id);
                warnings.push(format!(
                    "progress history compaction retained queue `{queue_id}` because an active surface claim is still pending"
                ));
            }
        }
    }
    Ok(evidence)
}

fn progress_cursor_terminal_event_id(cursor: &AgentProgressDeliveryCursor) -> Option<&str> {
    cursor
        .terminal_event_id
        .as_deref()
        .or_else(|| {
            cursor
                .body_terminal
                .then_some(cursor.body_last_event_id.as_deref())
                .flatten()
        })
        .or_else(|| {
            cursor
                .status_terminal
                .then_some(cursor.status_last_event_id.as_deref())
                .flatten()
        })
        .or_else(|| {
            cursor
                .terminal
                .then_some(cursor.last_event_id.as_deref())
                .flatten()
        })
        .and_then(|event_id| valid_progress_event_id(Some(event_id)))
}

fn final_source_delivery_is_still_pending(
    harness_home: &Path,
    queue_id: &str,
    event: &AgentProgressEvent,
    warnings: &mut Vec<String>,
) -> io::Result<bool> {
    let channel_dir = harness_home.join("state").join("channels");
    let deliveries =
        crate::channel_delivery_index::channel_delivery_states_for_source_queue_in_lane(
            &channel_dir,
            queue_id,
            &event.platform,
            event.account_id.as_deref(),
            &event.channel_id,
            &event.user_id,
            &event.session_key,
            warnings,
        )?;
    // A source queue can emit more than one final outbox record (for example a
    // rich presentation with a fallback).  Keep the terminal progress surface
    // behind every source-owned final delivery, rather than treating a single
    // completed row as permission to overtake another pending row.
    Ok(deliveries.iter().any(|delivery| {
        !matches!(
            delivery.terminal_status.or(delivery.last_status),
            Some(crate::ChannelDeliveryStatus::Delivered)
                | Some(crate::ChannelDeliveryStatus::SkippedPermanent)
        )
    }))
}

fn write_delivery_state(state_file: &Path, state: &AgentProgressDeliveryState) -> io::Result<()> {
    write_json_atomic(state_file, state)
}

fn progress_surface_claims_dir(harness_home: &Path) -> PathBuf {
    harness_home
        .join("state")
        .join("progress")
        .join("surface-claims")
}

fn progress_surface_claim_file(harness_home: &Path, status_surface_key: &str) -> PathBuf {
    progress_surface_claims_dir(harness_home)
        .join(format!("{}.json", fnv1a_64_hex(status_surface_key)))
}

pub fn latest_agent_progress_event_line_for_queue(
    harness_home: impl AsRef<Path>,
    queue_id: &str,
) -> io::Result<Option<usize>> {
    Ok(
        latest_agent_progress_event_identity_for_queue(harness_home, queue_id)?
            .map(|identity| identity.event_line),
    )
}

/// Returns the canonical v2 event identity for queue preemption/deduplication.
/// `None` deliberately means a legacy v1 row, for which callers must retain
/// their existing `eventLine` fallback rather than inventing a new identity.
pub fn latest_agent_progress_event_id_for_queue(
    harness_home: impl AsRef<Path>,
    queue_id: &str,
) -> io::Result<Option<String>> {
    Ok(
        latest_agent_progress_event_identity_for_queue(harness_home, queue_id)?
            .and_then(|identity| identity.event_id),
    )
}

/// Returns both the v2 opaque identity and the legacy physical-line fallback
/// from one indexed lookup so queue preemption cannot race two source tails.
pub fn latest_agent_progress_event_identity_for_queue(
    harness_home: impl AsRef<Path>,
    queue_id: &str,
) -> io::Result<Option<AgentProgressEventIdentity>> {
    let harness_home = harness_home.as_ref();
    let mut warnings = Vec::new();
    let _ = progress_history::recover_progress_history_compaction_if_needed(
        harness_home,
        &mut warnings,
    )?;
    Ok(
        latest_progress_event_for_queue(harness_home, queue_id, &mut warnings)?.map(|indexed| {
            AgentProgressEventIdentity {
                event_id: valid_progress_event_id(indexed.event.event_id.as_deref())
                    .map(str::to_string),
                event_line: indexed.line_number,
            }
        }),
    )
}

pub fn release_agent_progress_surface_claim(
    harness_home: impl AsRef<Path>,
    status_surface_key: &str,
    queue_id: &str,
) -> io::Result<bool> {
    let path = progress_surface_claim_file(harness_home.as_ref(), status_surface_key);
    let Some(claim) = read_progress_surface_claim(&path)? else {
        return Ok(false);
    };
    if claim.queue_id == queue_id
        && claim
            .provider_message_id
            .as_deref()
            .is_none_or(str::is_empty)
    {
        match fs::remove_file(path) {
            Ok(()) => return Ok(true),
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error),
        }
    }
    Ok(false)
}

fn remove_progress_surface_claims_for_queue(
    harness_home: &Path,
    queue_id: &str,
) -> io::Result<usize> {
    let claims_dir = progress_surface_claims_dir(harness_home);
    if !claims_dir.is_dir() {
        return Ok(0);
    }
    let mut removed = 0usize;
    for entry in fs::read_dir(claims_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        let Some(claim) = read_progress_surface_claim(&path)? else {
            continue;
        };
        if claim.queue_id == queue_id {
            match fs::remove_file(path) {
                Ok(()) => removed += 1,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
    }
    Ok(removed)
}

fn resolve_progress_surface_key_for_record(
    options: &AgentProgressDeliveryRecordOptions,
    fallback: &str,
) -> io::Result<String> {
    let claims_dir = progress_surface_claims_dir(&options.harness_home);
    if !claims_dir.is_dir() {
        return Ok(fallback.to_string());
    }
    for entry in fs::read_dir(claims_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        let Some(claim) = read_progress_surface_claim(&path)? else {
            continue;
        };
        if progress_surface_claim_matches_record(&claim, options) {
            return Ok(claim.status_surface_key);
        }
    }
    Ok(fallback.to_string())
}

fn progress_event_id_for_record(
    options: &AgentProgressDeliveryRecordOptions,
    status_surface_key: &str,
) -> io::Result<Option<String>> {
    let path = progress_surface_claim_file(&options.harness_home, status_surface_key);
    if let Some(claim) = read_progress_surface_claim(&path)?
        && progress_surface_claim_matches_record(&claim, options)
        && claim.event_line == options.event_line
        && claim.text_hash == options.text_hash
    {
        return Ok(valid_progress_event_id(claim.event_id.as_deref()).map(str::to_string));
    }

    // Existing provider surfaces intentionally retain their original claim
    // until a delivery receipt succeeds.  An edit therefore needs a bounded
    // exact-current-event lookup rather than inheriting the old claim's ID.
    let mut warnings = Vec::new();
    let Some(indexed) =
        latest_progress_event_for_queue(&options.harness_home, &options.queue_id, &mut warnings)?
    else {
        return Ok(None);
    };
    let event = &indexed.event;
    if indexed.line_number != options.event_line
        || event.platform != options.platform
        || event.account_id != options.account_id
        || event.channel_id != options.channel_id
        || event.thread_id != options.thread_id
        || event.user_id != options.user_id
        || event.session_key != options.session_key
    {
        return Ok(None);
    }
    Ok(valid_progress_event_id(event.event_id.as_deref()).map(str::to_string))
}

fn progress_surface_claim_matches_record(
    claim: &AgentProgressSurfaceClaim,
    options: &AgentProgressDeliveryRecordOptions,
) -> bool {
    claim.queue_id == options.queue_id
        && claim.platform == options.platform
        && claim.account_id == options.account_id
        && claim.channel_id == options.channel_id
        && claim.thread_id == options.thread_id
        && claim.user_id == options.user_id
        && claim.session_key == options.session_key
        && claim.message_kind == options.message_kind
}

fn progress_session_key(
    platform: &str,
    account_id: Option<&str>,
    channel_id: &str,
    user_id: &str,
    agent_id: Option<&str>,
    session_key: &str,
) -> String {
    [
        normalize_surface_part(platform),
        normalize_surface_part(account_id.unwrap_or("*")),
        normalize_surface_part(channel_id),
        normalize_surface_part(user_id),
        normalize_surface_part(agent_id.unwrap_or("*")),
        normalize_surface_part(session_key),
    ]
    .join(":")
}

fn progress_event_session_superseded(
    state: &AgentProgressDeliveryState,
    event: &AgentProgressEvent,
) -> bool {
    state
        .superseded_sessions
        .values()
        .any(|superseded| progress_event_matches_superseded_session(event, superseded))
}

fn progress_event_matches_superseded_session(
    event: &AgentProgressEvent,
    superseded: &AgentProgressSupersededSession,
) -> bool {
    event.platform == superseded.platform
        && option_filter_matches(&superseded.account_id, &event.account_id)
        && event.channel_id == superseded.channel_id
        && event.user_id == superseded.user_id
        && option_filter_matches(&superseded.agent_id, &event.agent_id)
        && event.session_key == superseded.session_key
}

fn progress_cursor_matches_superseded_session(
    cursor: &AgentProgressDeliveryCursor,
    superseded: &AgentProgressSupersededSession,
) -> bool {
    cursor.platform == superseded.platform
        && option_filter_matches(&superseded.account_id, &cursor.account_id)
        && cursor.channel_id == superseded.channel_id
        && cursor.user_id == superseded.user_id
        && cursor.session_key == superseded.session_key
}

fn progress_claim_matches_superseded_session(
    claim: &AgentProgressSurfaceClaim,
    superseded: &AgentProgressSupersededSession,
) -> bool {
    claim.platform == superseded.platform
        && option_filter_matches(&superseded.account_id, &claim.account_id)
        && claim.channel_id == superseded.channel_id
        && claim.user_id == superseded.user_id
        && option_filter_matches(&superseded.agent_id, &claim.agent_id)
        && claim.session_key == superseded.session_key
}

fn option_filter_matches(filter: &Option<String>, value: &Option<String>) -> bool {
    match filter {
        Some(filter) => value.as_deref() == Some(filter.as_str()),
        None => true,
    }
}

fn terminal_control_for_progress_queue(
    queue_dir: &Path,
    index: Option<&crate::runtime_worker::RuntimeQueueStateIndex>,
    queue_id: &str,
    session_key: Option<&str>,
    warnings: &mut Vec<String>,
) -> Option<QueueTerminalControlMatch> {
    let Some(index) = index else {
        return None;
    };
    match resolve_queue_terminal_control_from_index(
        queue_dir,
        queue_id,
        session_key,
        index.queues.get(queue_id),
    ) {
        Ok(QueueTerminalControl::Terminal(control)) => Some(control),
        Ok(QueueTerminalControl::Runnable) => None,
        Err(error) => {
            warnings.push(format!(
                "progress delivery terminal-control resolution failed open for queue `{queue_id}`: {error}"
            ));
            None
        }
    }
}

fn terminal_control_suppressed_progress_events(
    latest: &StoredProgressEvent,
    control: &QueueTerminalControlMatch,
    now_ms: i64,
) -> Vec<StoredProgressEvent> {
    let context = AgentProgressContext {
        queue_id: latest.event.queue_id.clone(),
        agent_id: latest.event.agent_id.clone(),
        account_id: latest.event.account_id.clone(),
        thread_id: latest.event.thread_id.clone(),
        session_key: latest.event.session_key.clone(),
        platform: latest.event.platform.clone(),
        channel_id: latest.event.channel_id.clone(),
        user_id: latest.event.user_id.clone(),
    };
    let preview = format!(
        "suppressed by terminal control: {}",
        control.source.as_str()
    );
    vec![
        StoredProgressEvent {
            line_number: latest.line_number,
            event: AgentProgressEvent::new(
                &context,
                AgentProgressKind::Terminal,
                "terminal-control",
                &preview,
                AgentProgressStatus::Progress,
                now_ms,
            )
            .source("progress-delivery"),
        },
        StoredProgressEvent {
            line_number: latest.line_number.saturating_add(1),
            event: AgentProgressEvent::new(
                &context,
                AgentProgressKind::Runtime,
                "run",
                preview,
                AgentProgressStatus::Completed,
                now_ms,
            )
            .source("progress-delivery"),
        },
    ]
}

fn acquire_progress_surface_claim(
    harness_home: &Path,
    status_surface_key: &str,
    queue_id: &str,
    event: &AgentProgressEvent,
    message_kind: AgentProgressDeliveryMessageKind,
    event_line: usize,
    text_hash: &str,
    terminal: bool,
    now_ms: i64,
    warnings: &mut Vec<String>,
) -> io::Result<ProgressSurfaceClaimOutcome> {
    let path = progress_surface_claim_file(harness_home, status_surface_key);
    let claim = progress_surface_claim_from_event(
        status_surface_key,
        queue_id,
        event,
        message_kind,
        event_line,
        text_hash,
        terminal,
        now_ms,
    );
    match create_progress_surface_claim(&path, &claim) {
        Ok(()) => {
            return Ok(ProgressSurfaceClaimOutcome::Claimed {
                fresh_send_reason: AgentProgressDeliveryFreshSendReason::NoExistingStatusSurface,
            });
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error),
    }

    let existing = match read_progress_surface_claim(&path) {
        Ok(Some(existing)) => existing,
        Ok(None) => {
            warnings.push(format!(
                "progress surface claim disappeared before read: {}",
                path.display()
            ));
            return Ok(ProgressSurfaceClaimOutcome::ActivePending);
        }
        Err(error) => {
            warnings.push(format!(
                "progress surface claim could not be read at {}: {}",
                path.display(),
                error
            ));
            return Ok(ProgressSurfaceClaimOutcome::ActivePending);
        }
    };
    if existing.status_surface_key != status_surface_key {
        warnings.push(format!(
            "progress surface claim key mismatch at {}: expected {}, found {}",
            path.display(),
            status_surface_key,
            existing.status_surface_key
        ));
        return Ok(ProgressSurfaceClaimOutcome::ActivePending);
    }
    if let Some(provider_message_id) = existing
        .provider_message_id
        .as_ref()
        .filter(|id| !id.is_empty())
        .cloned()
    {
        if existing.queue_id == queue_id {
            let mut refreshed = claim;
            refreshed.provider_message_id = Some(provider_message_id.clone());
            refreshed.expires_at_ms = 0;
            refreshed.updated_at_ms = now_ms;
            write_progress_surface_claim(&path, &refreshed)?;
        }
        return Ok(ProgressSurfaceClaimOutcome::ExistingProvider {
            provider_message_id,
        });
    }
    if progress_surface_claim_expired(&existing, now_ms) {
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        match create_progress_surface_claim(&path, &claim) {
            Ok(()) => {
                return Ok(ProgressSurfaceClaimOutcome::Claimed {
                    fresh_send_reason:
                        AgentProgressDeliveryFreshSendReason::ExistingProviderMessageExpired,
                });
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                return Ok(ProgressSurfaceClaimOutcome::ActivePending);
            }
            Err(error) => return Err(error),
        }
    }
    if existing.queue_id == queue_id {
        write_progress_surface_claim(&path, &claim)?;
        return Ok(ProgressSurfaceClaimOutcome::Claimed {
            fresh_send_reason: AgentProgressDeliveryFreshSendReason::NoExistingStatusSurface,
        });
    }

    Ok(ProgressSurfaceClaimOutcome::ActivePending)
}

fn progress_surface_claim_from_event(
    status_surface_key: &str,
    queue_id: &str,
    event: &AgentProgressEvent,
    message_kind: AgentProgressDeliveryMessageKind,
    event_line: usize,
    text_hash: &str,
    terminal: bool,
    now_ms: i64,
) -> AgentProgressSurfaceClaim {
    AgentProgressSurfaceClaim {
        schema: AGENT_PROGRESS_SURFACE_CLAIM_SCHEMA.to_string(),
        status_surface_key: status_surface_key.to_string(),
        queue_id: queue_id.to_string(),
        agent_id: event.agent_id.clone(),
        platform: event.platform.clone(),
        account_id: event.account_id.clone(),
        channel_id: event.channel_id.clone(),
        thread_id: event.thread_id.clone(),
        user_id: event.user_id.clone(),
        session_key: event.session_key.clone(),
        message_kind,
        provider_message_id: None,
        event_line,
        event_id: valid_progress_event_id(event.event_id.as_deref()).map(str::to_string),
        text_hash: text_hash.to_string(),
        terminal,
        claimed_at_ms: now_ms,
        expires_at_ms: now_ms.saturating_add(PROGRESS_SURFACE_CLAIM_TTL_MS),
        updated_at_ms: now_ms,
    }
}

fn progress_surface_claim_from_receipt(
    receipt: &AgentProgressDeliveryReceipt,
) -> AgentProgressSurfaceClaim {
    AgentProgressSurfaceClaim {
        schema: AGENT_PROGRESS_SURFACE_CLAIM_SCHEMA.to_string(),
        status_surface_key: receipt.status_surface_key.clone(),
        queue_id: receipt.queue_id.clone(),
        agent_id: None,
        platform: receipt.platform.clone(),
        account_id: receipt.account_id.clone(),
        channel_id: receipt.channel_id.clone(),
        thread_id: receipt.thread_id.clone(),
        user_id: receipt.user_id.clone(),
        session_key: receipt.session_key.clone(),
        message_kind: receipt.message_kind,
        provider_message_id: receipt.provider_message_id.clone(),
        event_line: receipt.event_line,
        event_id: receipt.event_id.clone(),
        text_hash: receipt.text_hash.clone(),
        terminal: receipt.terminal,
        claimed_at_ms: receipt.at_ms,
        expires_at_ms: 0,
        updated_at_ms: receipt.at_ms,
    }
}

fn create_progress_surface_claim(path: &Path, claim: &AgentProgressSurfaceClaim) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let bytes = serde_json::to_vec_pretty(claim).map_err(io::Error::other)?;
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?;
    file.write_all(&bytes)
}

fn read_progress_surface_claim(path: &Path) -> io::Result<Option<AgentProgressSurfaceClaim>> {
    if !path.is_file() {
        return Ok(None);
    }
    let bytes = fs::read(path)?;
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(io::Error::other)
}

fn write_progress_surface_claim(path: &Path, claim: &AgentProgressSurfaceClaim) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    write_json_atomic(path, claim)
}

fn progress_surface_claim_expired(claim: &AgentProgressSurfaceClaim, now_ms: i64) -> bool {
    claim.provider_message_id.is_none() && claim.expires_at_ms > 0 && now_ms >= claim.expires_at_ms
}

fn update_progress_surface_claim_from_receipt(
    harness_home: &Path,
    receipt: &AgentProgressDeliveryReceipt,
) -> io::Result<()> {
    let path = progress_surface_claim_file(harness_home, &receipt.status_surface_key);
    if receipt.status != AgentProgressDeliveryStatus::Delivered {
        if receipt.provider_message_id.is_none() {
            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        return Ok(());
    }
    let Some(provider_message_id) = receipt.provider_message_id.clone() else {
        return Ok(());
    };
    let mut claim = read_progress_surface_claim(&path)?
        .unwrap_or_else(|| progress_surface_claim_from_receipt(receipt));
    claim.schema = AGENT_PROGRESS_SURFACE_CLAIM_SCHEMA.to_string();
    claim.status_surface_key = receipt.status_surface_key.clone();
    claim.queue_id = receipt.queue_id.clone();
    claim.platform = receipt.platform.clone();
    claim.account_id = receipt.account_id.clone();
    claim.channel_id = receipt.channel_id.clone();
    claim.thread_id = receipt.thread_id.clone();
    claim.user_id = receipt.user_id.clone();
    claim.session_key = receipt.session_key.clone();
    claim.message_kind = receipt.message_kind;
    claim.provider_message_id = Some(provider_message_id);
    claim.event_line = receipt.event_line;
    claim.event_id = receipt.event_id.clone();
    claim.text_hash = receipt.text_hash.clone();
    claim.terminal = receipt.terminal;
    claim.expires_at_ms = 0;
    claim.updated_at_ms = receipt.at_ms;
    write_progress_surface_claim(&path, &claim)
}

fn append_json_line(path: &Path, value: &impl Serialize) -> io::Result<()> {
    crate::append_jsonl_value(path, value)
}

fn render_action_line(event: &AgentProgressEvent, max_preview_chars: usize) -> String {
    if is_internal_worker_result_event(event) {
        return format!(
            "{} {}: \"internal worker result received; awaiting main-agent summary\"",
            progress_icon(event.kind),
            event.label
        );
    }
    let preview = quote_safe_preview(&event.preview, max_preview_chars);
    format!(
        "{} {}: \"{}\"",
        progress_icon(event.kind),
        event.label,
        preview
    )
}

fn is_internal_worker_result_event(event: &AgentProgressEvent) -> bool {
    if event.kind != AgentProgressKind::ToolCall {
        return false;
    }
    let source = event
        .source
        .as_deref()
        .unwrap_or_default()
        .to_ascii_lowercase();
    let label = event.label.to_ascii_lowercase();
    let preview = event.preview.trim_start().to_ascii_lowercase();
    source.contains("subagent")
        || source.contains("multi_agent")
        || source.contains("explorer")
        || label.contains("wait_agent")
        || label.contains("spawn_agent")
        || label.contains("subagent")
        || preview.starts_with("current answer:")
        || preview.starts_with("current answer：")
        || preview.starts_with("completed status")
        || preview_mentions_internal_worker_result(&preview)
}

fn preview_mentions_internal_worker_result(preview: &str) -> bool {
    preview.starts_with("<subagent_notification")
        || (preview.contains("\"agent_path\"")
            && (preview.contains("\"completed\"")
                || preview.contains("\"status\"")
                || preview.contains("\"current_answer\"")))
}

fn quote_safe_preview(value: &str, max_preview_chars: usize) -> String {
    truncate_chars(&value.replace('"', "'"), max_preview_chars)
}

fn status_phrase(event: &AgentProgressEvent) -> &'static str {
    match event.kind {
        AgentProgressKind::AssistantStream => "receiving stream response",
        AgentProgressKind::AssistantNarration => "working",
        AgentProgressKind::Terminal
        | AgentProgressKind::SearchFiles
        | AgentProgressKind::ReadFile
        | AgentProgressKind::ExecuteCode
        | AgentProgressKind::ToolCall => "running tools",
        AgentProgressKind::SkillView
        | AgentProgressKind::Todo
        | AgentProgressKind::MemoryRecall => "preparing context",
        AgentProgressKind::Delivery => "delivering response",
        AgentProgressKind::Runtime => match event.status {
            AgentProgressStatus::Started => "starting",
            AgentProgressStatus::Progress => "working",
            AgentProgressStatus::Completed => "finishing",
            AgentProgressStatus::Failed => "failed",
        },
    }
}

fn is_terminal_event(event: &AgentProgressEvent) -> bool {
    event.kind == AgentProgressKind::Runtime
        && matches!(
            event.status,
            AgentProgressStatus::Completed | AgentProgressStatus::Failed
        )
}

fn progress_status_elapsed_ms(
    first_at_ms: i64,
    latest: &AgentProgressEvent,
    now_ms: i64,
    terminal: bool,
) -> i64 {
    if terminal {
        latest
            .elapsed_ms
            .and_then(|elapsed| i64::try_from(elapsed).ok())
            .unwrap_or_else(|| latest.at_ms.saturating_sub(first_at_ms))
    } else {
        now_ms.saturating_sub(first_at_ms)
    }
}

fn progress_icon(kind: AgentProgressKind) -> &'static str {
    match kind {
        AgentProgressKind::Runtime => "⚙️",
        AgentProgressKind::SkillView => "📚",
        AgentProgressKind::Todo => "📋",
        AgentProgressKind::Terminal => "💻",
        AgentProgressKind::SearchFiles => "🔎",
        AgentProgressKind::ReadFile => "📖",
        AgentProgressKind::ExecuteCode => "🐍",
        AgentProgressKind::ToolCall => "🛠️",
        AgentProgressKind::AssistantStream => "💬",
        AgentProgressKind::AssistantNarration => "📝",
        AgentProgressKind::MemoryRecall => "🧠",
        AgentProgressKind::Delivery => "📨",
    }
}

fn format_elapsed(elapsed_ms: i64) -> String {
    let minutes = elapsed_ms.max(0) / 60_000;
    if minutes <= 0 {
        "<1 min".to_string()
    } else if minutes == 1 {
        "1 min".to_string()
    } else {
        format!("{minutes} min")
    }
}

fn looks_sensitive(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    if [
        "sk-",
        "ghp_",
        "gho_",
        "ghu_",
        "ghs_",
        "github_pat_",
        "xoxb-",
        "xoxp-",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
    {
        return true;
    }
    let mentions_secret = [
        "token",
        "secret",
        "password",
        "passwd",
        "api_key",
        "api-key",
        "apikey",
        "access_token",
        "refresh_token",
        "authorization",
        "bearer ",
        "--token",
        "--api-key",
        "--apikey",
    ]
    .iter()
    .any(|needle| lower.contains(needle));
    mentions_secret
        && (value.contains('=')
            || value.contains(':')
            || value.contains("--")
            || lower.contains("bearer ")
            || lower.split_whitespace().any(|part| {
                matches!(
                    trim_ascii_punctuation(part),
                    "token" | "secret" | "password" | "passwd"
                )
            }))
}

fn trim_ascii_punctuation(value: &str) -> &str {
    value.trim_matches(|ch: char| ch.is_ascii_punctuation())
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    let max_chars = max_chars.max(8);
    let mut out = String::new();
    for (index, ch) in value.chars().enumerate() {
        if index >= max_chars {
            out.push_str("...");
            return out;
        }
        out.push(ch);
    }
    out
}

fn status_surface_key_for_event(
    event: &AgentProgressEvent,
    queue_id: &str,
    message_kind: AgentProgressDeliveryMessageKind,
) -> String {
    status_surface_key_from_parts(
        &event.platform,
        event.account_id.as_deref(),
        &event.channel_id,
        &event.user_id,
        event.agent_id.as_deref(),
        &event.session_key,
        queue_id,
        message_kind,
    )
}

fn status_surface_key_from_parts(
    platform: &str,
    account_id: Option<&str>,
    channel_id: &str,
    user_id: &str,
    agent_id: Option<&str>,
    session_key: &str,
    queue_id: &str,
    message_kind: AgentProgressDeliveryMessageKind,
) -> String {
    let message_kind = match message_kind {
        AgentProgressDeliveryMessageKind::Body => "body",
        AgentProgressDeliveryMessageKind::Status => "status",
    };
    [
        normalize_surface_part(platform),
        normalize_surface_part(account_id.unwrap_or("default")),
        normalize_surface_part(channel_id),
        normalize_surface_part(user_id),
        normalize_surface_part(agent_id.unwrap_or("default")),
        normalize_surface_part(session_key),
        normalize_surface_part(queue_id),
        message_kind.to_string(),
    ]
    .join(":")
}

fn normalize_surface_part(value: &str) -> String {
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

fn fnv1a_64_hex(value: &str) -> String {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

fn valid_progress_event_id(value: Option<&str>) -> Option<&str> {
    let value = value?.trim();
    (value.len() >= 16
        && value.len() <= 160
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')))
    .then_some(value)
}

fn new_progress_event_id(event: &AgentProgressEvent) -> String {
    let sequence = PROGRESS_EVENT_ID_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let material = format!(
        "{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}",
        event.queue_id,
        event.session_key,
        event.at_ms,
        event.label,
        event.preview,
        std::process::id(),
        sequence,
        now_nanos,
    );
    let first = fnv1a_64_hex(&material);
    let second = fnv1a_64_hex(&format!("{now_nanos}:{sequence}:{first}"));
    format!("pe2-{first}{second}")
}

fn is_zero_usize(value: &usize) -> bool {
    *value == 0
}

fn is_zero_i64(value: &i64) -> bool {
    *value == 0
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn agent_progress_event_schema() -> String {
    AGENT_PROGRESS_EVENT_SCHEMA.to_string()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RenderedAction {
    line: String,
    count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::HARNESS_CONFIG_FILE_NAME;
    use crate::{
        ScopedStopOptions, ScopedStopTarget,
        latency::{LatencyStage, latency_receipts_file, read_latest_queue_receipt},
        record_scoped_stop,
    };
    use std::{
        sync::mpsc,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn append_agent_progress_event_signals_delivery_wake() {
        let root = temp_root("append_agent_progress_event_signals_delivery_wake");
        let harness_home = root.join(".agent-harness");
        let wake_file = harness_home
            .join("state")
            .join("wake")
            .join("progress-delivery.json");
        let context = context();

        append_agent_progress_event(
            &harness_home,
            &AgentProgressEvent::new(
                &context,
                AgentProgressKind::Terminal,
                "terminal",
                "cargo test",
                AgentProgressStatus::Started,
                1000,
            ),
        )
        .unwrap();
        assert_eq!(crate::wake::read_wake_sequence(&wake_file).unwrap(), 1);

        append_agent_progress_event(
            &harness_home,
            &AgentProgressEvent::new(
                &context,
                AgentProgressKind::Runtime,
                "runtime",
                "completed",
                AgentProgressStatus::Completed,
                2000,
            ),
        )
        .unwrap();
        assert_eq!(crate::wake::read_wake_sequence(&wake_file).unwrap(), 2);
        let latency =
            read_latest_queue_receipt(latency_receipts_file(&harness_home), &context.queue_id)
                .unwrap()
                .expect("a durable progress append records latency stages");
        assert_eq!(
            latency
                .stages
                .get(&LatencyStage::FirstProgressEvent)
                .copied(),
            Some(1000)
        );
        assert_eq!(
            latency.stages.get(&LatencyStage::TerminalEvent).copied(),
            Some(2000)
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn delivered_status_surface_records_provider_progress_and_working_latency() {
        let root =
            temp_root("delivered_status_surface_records_provider_progress_and_working_latency");
        let harness_home = root.join(".agent-harness");
        let context = context();

        let receipt = record_agent_progress_delivery(AgentProgressDeliveryRecordOptions {
            harness_home: harness_home.clone(),
            queue_id: context.queue_id.clone(),
            platform: context.platform.clone(),
            account_id: context.account_id.clone(),
            channel_id: context.channel_id.clone(),
            thread_id: context.thread_id.clone(),
            user_id: context.user_id.clone(),
            session_key: context.session_key.clone(),
            message_kind: AgentProgressDeliveryMessageKind::Status,
            action: AgentProgressDeliveryAction::Send,
            status: AgentProgressDeliveryStatus::Delivered,
            provider_message_id: Some("provider-working-1".to_string()),
            event_line: 1,
            text_hash: "working-status".to_string(),
            terminal: false,
            policy_decision: Some("test".to_string()),
            error: None,
            now_ms: 2_000,
        })
        .unwrap();
        assert_eq!(receipt.status, AgentProgressDeliveryStatus::Delivered);

        let latency =
            read_latest_queue_receipt(latency_receipts_file(&harness_home), &context.queue_id)
                .unwrap()
                .expect("a delivered provider status surface records latency stages");
        assert_eq!(
            latency
                .stages
                .get(&LatencyStage::FirstProviderProgressSurface)
                .copied(),
            Some(2_000)
        );
        assert_eq!(
            latency.stages.get(&LatencyStage::WorkingIndicator).copied(),
            Some(2_000)
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn renders_safe_operation_action_stream_separate_from_status_by_default() {
        let context = context();
        let events = vec![
            AgentProgressEvent::new(
                &context,
                AgentProgressKind::SkillView,
                "skill_view",
                "codebase-inspection",
                AgentProgressStatus::Completed,
                1000,
            ),
            AgentProgressEvent::new(
                &context,
                AgentProgressKind::Terminal,
                "terminal",
                "cargo test -p agent-harness-core",
                AgentProgressStatus::Started,
                61_000,
            ),
            AgentProgressEvent::new(
                &context,
                AgentProgressKind::AssistantStream,
                "assistant_stream",
                "receiving stream response",
                AgentProgressStatus::Progress,
                120_000,
            ),
        ];
        let refs = events.iter().collect::<Vec<_>>();
        let actions = render_agent_progress_actions(&refs, 8, 120);
        let status = render_agent_progress_status(&refs, 9 * 60_000 + 1000, 120, 1200, false);
        let panel = render_agent_progress_panel(&refs, 9 * 60_000 + 1000, 8, 120);

        assert!(actions.contains("skill_view: \"codebase-inspection\""));
        assert!(actions.contains("terminal: \"cargo test -p agent-harness-core\""));
        assert!(!actions.contains("assistant_stream"));
        assert_eq!(status, "⏳ Working — 9 min — running tools");
        assert_eq!(panel, format!("{actions}\n\n{status}"));
    }

    #[test]
    fn action_stream_excludes_private_or_non_operation_events_by_default() {
        let context = context();
        let events = vec![
            AgentProgressEvent::new(
                &context,
                AgentProgressKind::Runtime,
                "run",
                "prepared runtime item",
                AgentProgressStatus::Started,
                1000,
            ),
            AgentProgressEvent::new(
                &context,
                AgentProgressKind::ReadFile,
                "read_file",
                "D:\\private\\notes.md",
                AgentProgressStatus::Completed,
                2000,
            ),
            AgentProgressEvent::new(
                &context,
                AgentProgressKind::MemoryRecall,
                "memory_recall",
                "remembered private preference",
                AgentProgressStatus::Completed,
                3000,
            ),
            AgentProgressEvent::new(
                &context,
                AgentProgressKind::AssistantNarration,
                "current_step",
                "checking state",
                AgentProgressStatus::Progress,
                4000,
            ),
            AgentProgressEvent::new(
                &context,
                AgentProgressKind::Delivery,
                "delivery",
                "sending provider update",
                AgentProgressStatus::Progress,
                5000,
            ),
        ];
        let refs = events.iter().collect::<Vec<_>>();
        let actions = render_agent_progress_actions(&refs, 8, 120);

        assert!(actions.is_empty());
        assert!(!actions.contains("private"));
        assert!(!actions.contains("delivery"));
    }

    #[test]
    fn action_stream_summarizes_internal_worker_results_instead_of_raw_final_text() {
        let context = context();
        let event = AgentProgressEvent::new(
            &context,
            AgentProgressKind::ToolCall,
            "wait_agent",
            "Current answer: default handling is refs/summaries only in prompt, working context, queue metadata, and rollover continuity. Evidence: private path and English worker text.",
            AgentProgressStatus::Completed,
            1000,
        )
        .source("multi_agent.wait_agent");
        let events = [event];
        let refs = events.iter().collect::<Vec<_>>();

        let actions = render_agent_progress_actions(&refs, 8, 240);

        assert!(actions.contains("internal worker result received"));
        assert!(!actions.contains("Current answer"));
        assert!(!actions.contains("default handling is refs"));
        assert!(!actions.contains("private path"));
    }

    #[test]
    fn action_stream_summarizes_structured_subagent_notifications_without_source_label() {
        let context = context();
        let event = AgentProgressEvent::new(
            &context,
            AgentProgressKind::ToolCall,
            "tool_result",
            r#"<subagent_notification>
{"agent_path":"019f19c4-0111","status":{"completed":"Current answer: raw worker handoff with owner-only implementation details"}}
</subagent_notification>"#,
            AgentProgressStatus::Completed,
            1000,
        );
        let events = [event];
        let refs = events.iter().collect::<Vec<_>>();

        let actions = render_agent_progress_actions(&refs, 8, 240);

        assert!(actions.contains("internal worker result received"));
        assert!(!actions.contains("raw worker handoff"));
        assert!(!actions.contains("owner-only implementation details"));
    }

    #[test]
    fn terminal_status_uses_completion_elapsed_instead_of_wall_clock_age() {
        let context = context();
        let events = vec![
            AgentProgressEvent::new(
                &context,
                AgentProgressKind::Todo,
                "todo",
                "planning 1 task(s)",
                AgentProgressStatus::Started,
                1000,
            ),
            AgentProgressEvent::new(
                &context,
                AgentProgressKind::Runtime,
                "run",
                "completed",
                AgentProgressStatus::Completed,
                61_000,
            ),
        ];
        let refs = events.iter().collect::<Vec<_>>();

        assert_eq!(
            render_agent_progress_status(&refs, 44 * 60_000 + 1000, 120, 1200, false),
            "✅ Done — 1 min"
        );
        assert_eq!(
            render_agent_progress_status(&refs, 45 * 60_000 + 1000, 120, 1200, false),
            "✅ Done — 1 min"
        );
    }

    #[test]
    fn assistant_narration_renders_as_current_step_only() {
        let context = context();
        let events = vec![
            AgentProgressEvent::new(
                &context,
                AgentProgressKind::Terminal,
                "terminal",
                "pwsh: agent-harness status",
                AgentProgressStatus::Started,
                1000,
            ),
            AgentProgressEvent::new(
                &context,
                AgentProgressKind::AssistantNarration,
                "current_step",
                "verifying skills-index readback",
                AgentProgressStatus::Progress,
                4000,
            ),
        ];
        let refs = events.iter().collect::<Vec<_>>();
        let actions = render_agent_progress_actions(&refs, 8, 120);
        let status = render_agent_progress_status(&refs, 61_000, 120, 1200, false);

        assert_eq!(actions, "💻 terminal: \"pwsh: agent-harness status\"");
        assert!(!actions.contains("verifying skills-index"));
        assert_eq!(
            status,
            "⏳ Working — 1 min — running tools\nCurrent step: verifying skills-index readback"
        );
    }

    #[test]
    fn current_step_does_not_fall_back_to_runtime_progress_when_narration_is_absent() {
        let context = context();
        let events = vec![
            AgentProgressEvent::new(
                &context,
                AgentProgressKind::Terminal,
                "terminal",
                "codex.cmd app-server",
                AgentProgressStatus::Started,
                1000,
            ),
            AgentProgressEvent::new(
                &context,
                AgentProgressKind::Runtime,
                "run",
                "transient runtime failure; preserving session for retry",
                AgentProgressStatus::Progress,
                5000,
            ),
        ];
        let refs = events.iter().collect::<Vec<_>>();

        assert_eq!(
            render_agent_progress_status(&refs, 65_000, 120, 1200, false),
            "⏳ Working — 1 min — working"
        );
    }

    #[test]
    fn current_step_does_not_fall_back_to_latest_tool_when_narration_is_absent() {
        let context = context();
        let events = vec![AgentProgressEvent::new(
            &context,
            AgentProgressKind::ToolCall,
            "tool_call",
            "pwsh: Get-ChildItem .agent-harness",
            AgentProgressStatus::Started,
            1000,
        )];
        let refs = events.iter().collect::<Vec<_>>();

        assert_eq!(
            render_agent_progress_status(&refs, 61_000, 120, 1200, false),
            "⏳ Working — 1 min — running tools"
        );
    }

    #[test]
    fn current_step_does_not_render_codex_apps_tool_name_without_narration() {
        let context = context();
        let events = vec![AgentProgressEvent::new(
            &context,
            AgentProgressKind::ToolCall,
            "tool_call",
            "codex_apps",
            AgentProgressStatus::Started,
            1000,
        )];
        let refs = events.iter().collect::<Vec<_>>();
        let status = render_agent_progress_status(&refs, 61_000, 120, 1200, false);

        assert_eq!(status, "⏳ Working — 1 min — running tools");
        assert!(!status.contains("Current step"));
        assert!(!status.contains("codex_apps"));
    }

    #[test]
    fn assistant_narration_preview_can_exceed_general_preview_cap() {
        let context = context();
        let long_step = format!("{}complete", "checking reconnect recovery ".repeat(35));
        let tool_event = AgentProgressEvent::new(
            &context,
            AgentProgressKind::Terminal,
            "terminal",
            "cargo test -p agent-harness-core",
            AgentProgressStatus::Started,
            1000,
        );
        let narration_event = AgentProgressEvent::new_with_preview_limit(
            &context,
            AgentProgressKind::AssistantNarration,
            "assistant_narration",
            &long_step,
            AgentProgressStatus::Progress,
            2000,
            1200,
        );

        assert_eq!(narration_event.preview, long_step);
        let refs = vec![&tool_event, &narration_event];
        let status = render_agent_progress_status(&refs, 61_000, 24, 1200, false);
        assert!(status.contains("complete"));
        assert!(!status.contains("..."));
    }

    #[test]
    fn assistant_narration_preview_still_respects_its_own_limit() {
        let context = context();
        let long_step = format!("{}complete", "checking reconnect recovery ".repeat(35));
        let event = AgentProgressEvent::new_with_preview_limit(
            &context,
            AgentProgressKind::AssistantNarration,
            "assistant_narration",
            &long_step,
            AgentProgressStatus::Progress,
            1000,
            64,
        );

        assert!(event.preview.ends_with("..."));
        assert!(!event.preview.contains("complete"));
    }

    #[test]
    fn current_step_uses_separate_preview_limit() {
        let context = context();
        let long_step = "checking reconnect recovery while preserving the existing Telegram session binding and avoiding a duplicate final reply";
        let events = vec![
            AgentProgressEvent::new(
                &context,
                AgentProgressKind::Terminal,
                "terminal",
                "pwsh: agent-harness status",
                AgentProgressStatus::Started,
                1000,
            ),
            AgentProgressEvent::new(
                &context,
                AgentProgressKind::AssistantNarration,
                "current_step",
                long_step,
                AgentProgressStatus::Progress,
                4000,
            ),
        ];
        let refs = events.iter().collect::<Vec<_>>();

        let status = render_agent_progress_status(&refs, 61_000, 24, 200, false);

        assert!(status.contains(long_step));
        assert!(!status.contains("..."));
    }

    #[test]
    fn delivery_plan_uses_send_then_edit_and_rate_limits() {
        let root = temp_root("delivery_plan_uses_send_then_edit_and_rate_limits");
        let harness_home = root.join(".agent-harness");
        let context = context();
        append_agent_progress_event(
            &harness_home,
            &AgentProgressEvent::new(
                &context,
                AgentProgressKind::Todo,
                "todo",
                "planning 1 task(s)",
                AgentProgressStatus::Started,
                1000,
            ),
        )
        .unwrap();

        let plan = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 2000,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert_eq!(plan.pending.len(), 2);
        assert_eq!(
            plan.pending[0].message_kind,
            AgentProgressDeliveryMessageKind::Body
        );
        assert_eq!(plan.pending[0].action, AgentProgressDeliveryAction::Send);
        assert_eq!(
            plan.pending[1].message_kind,
            AgentProgressDeliveryMessageKind::Status
        );
        assert_eq!(plan.pending[1].action, AgentProgressDeliveryAction::Send);

        for (index, pending) in plan.pending.iter().cloned().enumerate() {
            record_agent_progress_delivery(AgentProgressDeliveryRecordOptions {
                harness_home: harness_home.clone(),
                queue_id: pending.queue_id,
                platform: pending.platform,
                account_id: pending.account_id,
                channel_id: pending.channel_id,
                thread_id: pending.thread_id,
                user_id: pending.user_id,
                session_key: pending.session_key,
                message_kind: pending.message_kind,
                action: pending.action,
                status: AgentProgressDeliveryStatus::Delivered,
                provider_message_id: Some(format!("provider-{}", index + 1)),
                event_line: pending.event_line,
                text_hash: pending.text_hash,
                terminal: pending.terminal,
                policy_decision: Some("test".to_string()),
                error: None,
                now_ms: 2000,
            })
            .unwrap();
        }

        append_agent_progress_event(
            &harness_home,
            &AgentProgressEvent::new(
                &context,
                AgentProgressKind::Terminal,
                "terminal",
                "codex app-server",
                AgentProgressStatus::Started,
                2100,
            ),
        )
        .unwrap();
        let rate_limited = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 2200,
            min_update_interval_ms: 2500,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert_eq!(rate_limited.pending.len(), 0);
        assert_eq!(rate_limited.summary.rate_limited, 2);

        let edit = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home,
            platform: Some("telegram".to_string()),
            now_ms: 5000,
            min_update_interval_ms: 2500,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert_eq!(edit.pending.len(), 2);
        assert_eq!(edit.pending[0].action, AgentProgressDeliveryAction::Edit);
        assert_eq!(
            edit.pending[0].message_kind,
            AgentProgressDeliveryMessageKind::Body
        );
        assert_eq!(
            edit.pending[0].provider_message_id.as_deref(),
            Some("provider-1")
        );
        assert_eq!(edit.pending[1].action, AgentProgressDeliveryAction::Edit);
        assert_eq!(
            edit.pending[1].message_kind,
            AgentProgressDeliveryMessageKind::Status
        );
        assert_eq!(
            edit.pending[1].provider_message_id.as_deref(),
            Some("provider-2")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn progress_delivery_successful_edit_does_not_fresh_send() {
        delivery_plan_uses_send_then_edit_and_rate_limits();
    }

    #[test]
    fn delivery_plan_volume_limits_nonterminal_updates_but_allows_terminal_convergence() {
        let root = temp_root(
            "delivery_plan_volume_limits_nonterminal_updates_but_allows_terminal_convergence",
        );
        let harness_home = root.join(".agent-harness");
        let context = context();
        append_agent_progress_event(
            &harness_home,
            &AgentProgressEvent::new(
                &context,
                AgentProgressKind::Todo,
                "todo",
                "planning 1 task(s)",
                AgentProgressStatus::Started,
                1000,
            ),
        )
        .unwrap();

        let initial = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 2000,
            min_update_interval_ms: 0,
            max_nonterminal_updates_per_lane: 1,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert_eq!(initial.pending.len(), 2);
        for (index, pending) in initial.pending.into_iter().enumerate() {
            record_agent_progress_delivery(AgentProgressDeliveryRecordOptions {
                harness_home: harness_home.clone(),
                queue_id: pending.queue_id,
                platform: pending.platform,
                account_id: pending.account_id,
                channel_id: pending.channel_id,
                thread_id: pending.thread_id,
                user_id: pending.user_id,
                session_key: pending.session_key,
                message_kind: pending.message_kind,
                action: pending.action,
                status: AgentProgressDeliveryStatus::Delivered,
                provider_message_id: Some(format!("provider-{}", index + 1)),
                event_line: pending.event_line,
                text_hash: pending.text_hash,
                terminal: pending.terminal,
                policy_decision: Some("test".to_string()),
                error: None,
                now_ms: 2000,
            })
            .unwrap();
        }

        append_agent_progress_event(
            &harness_home,
            &AgentProgressEvent::new(
                &context,
                AgentProgressKind::ToolCall,
                "tool_call",
                "cargo test -p agent-harness-core",
                AgentProgressStatus::Progress,
                3000,
            ),
        )
        .unwrap();
        let limited = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 4000,
            min_update_interval_ms: 0,
            max_nonterminal_updates_per_lane: 1,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert_eq!(limited.pending.len(), 1);
        assert_eq!(
            limited.pending[0].message_kind,
            AgentProgressDeliveryMessageKind::Status
        );
        assert!(
            limited.pending[0]
                .text
                .contains("cargo test -p agent-harness-core")
        );
        assert_eq!(limited.summary.volume_limited, 1);
        assert_eq!(limited.summary.rate_limited, 0);

        let heartbeat = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 302_000,
            min_update_interval_ms: 0,
            max_nonterminal_updates_per_lane: 1,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert_eq!(heartbeat.pending.len(), 1);
        let status = heartbeat
            .pending
            .iter()
            .find(|pending| pending.message_kind == AgentProgressDeliveryMessageKind::Status)
            .unwrap();
        assert_eq!(status.action, AgentProgressDeliveryAction::Edit);
        assert!(status.text.contains("Updates capped; still working."));
        assert!(status.text.contains("Latest internal event: 3000ms"));
        assert_eq!(heartbeat.summary.volume_limited, 1);

        append_agent_progress_event(
            &harness_home,
            &AgentProgressEvent::new(
                &context,
                AgentProgressKind::Runtime,
                "runtime",
                "completed",
                AgentProgressStatus::Completed,
                5000,
            ),
        )
        .unwrap();
        let terminal = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 6000,
            min_update_interval_ms: 0,
            max_nonterminal_updates_per_lane: 1,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert_eq!(terminal.pending.len(), 2);
        assert!(terminal.pending.iter().all(|pending| pending.terminal));
        assert!(
            terminal
                .pending
                .iter()
                .all(|pending| pending.action == AgentProgressDeliveryAction::Edit)
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn delivery_plan_status_heartbeat_after_body_cap_is_channel_agnostic() {
        for platform in ["telegram", "discord"] {
            let root = temp_root(&format!(
                "delivery_plan_status_heartbeat_after_body_cap_{platform}"
            ));
            let harness_home = root.join(".agent-harness");
            let context = AgentProgressContext {
                queue_id: format!("turn:{platform}:1"),
                platform: platform.to_string(),
                channel_id: format!("{platform}-dm"),
                session_key: format!("{platform}:dm:user:main"),
                ..context()
            };
            append_agent_progress_event(
                &harness_home,
                &AgentProgressEvent::new(
                    &context,
                    AgentProgressKind::Todo,
                    "todo",
                    "planning 1 task(s)",
                    AgentProgressStatus::Started,
                    1000,
                ),
            )
            .unwrap();
            let initial = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
                harness_home: harness_home.clone(),
                platform: Some(platform.to_string()),
                now_ms: 2000,
                min_update_interval_ms: 0,
                max_nonterminal_updates_per_lane: 1,
                ..AgentProgressDeliveryPlanOptions::default()
            })
            .unwrap();
            assert_eq!(initial.pending.len(), 2);
            for (index, pending) in initial.pending.into_iter().enumerate() {
                record_agent_progress_delivery(AgentProgressDeliveryRecordOptions {
                    harness_home: harness_home.clone(),
                    queue_id: pending.queue_id,
                    platform: pending.platform,
                    account_id: pending.account_id,
                    channel_id: pending.channel_id,
                    thread_id: pending.thread_id,
                    user_id: pending.user_id,
                    session_key: pending.session_key,
                    message_kind: pending.message_kind,
                    action: pending.action,
                    status: AgentProgressDeliveryStatus::Delivered,
                    provider_message_id: Some(format!("{platform}-provider-{}", index + 1)),
                    event_line: pending.event_line,
                    text_hash: pending.text_hash,
                    terminal: pending.terminal,
                    policy_decision: Some("test".to_string()),
                    error: None,
                    now_ms: 2000,
                })
                .unwrap();
            }
            append_agent_progress_event(
                &harness_home,
                &AgentProgressEvent::new(
                    &context,
                    AgentProgressKind::AssistantNarration,
                    "assistant_narration",
                    "Checking artifact prompt hygiene and progress heartbeat tests.",
                    AgentProgressStatus::Progress,
                    3000,
                ),
            )
            .unwrap();
            append_agent_progress_event(
                &harness_home,
                &AgentProgressEvent::new(
                    &context,
                    AgentProgressKind::ToolCall,
                    "tool_call",
                    "cargo test -p agent-harness-core",
                    AgentProgressStatus::Progress,
                    4000,
                ),
            )
            .unwrap();

            let heartbeat = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
                harness_home: harness_home.clone(),
                platform: Some(platform.to_string()),
                now_ms: 302_000,
                min_update_interval_ms: 0,
                max_nonterminal_updates_per_lane: 1,
                ..AgentProgressDeliveryPlanOptions::default()
            })
            .unwrap();
            assert_eq!(heartbeat.pending.len(), 1, "platform={platform}");
            let pending = heartbeat.pending[0].clone();
            assert_eq!(pending.platform, platform);
            assert_eq!(
                pending.message_kind,
                AgentProgressDeliveryMessageKind::Status
            );
            assert!(pending.text.contains("Updates capped; still working."));
            assert!(pending.text.contains("Latest internal event: 4000ms"));
            assert!(
                pending
                    .text
                    .contains("Current step: Checking artifact prompt hygiene")
            );
            assert_eq!(heartbeat.summary.volume_limited, 1);

            record_agent_progress_delivery(AgentProgressDeliveryRecordOptions {
                harness_home: harness_home.clone(),
                queue_id: pending.queue_id,
                platform: pending.platform,
                account_id: pending.account_id,
                channel_id: pending.channel_id,
                thread_id: pending.thread_id,
                user_id: pending.user_id,
                session_key: pending.session_key,
                message_kind: pending.message_kind,
                action: pending.action,
                status: AgentProgressDeliveryStatus::Delivered,
                provider_message_id: pending.provider_message_id,
                event_line: pending.event_line,
                text_hash: pending.text_hash,
                terminal: pending.terminal,
                policy_decision: Some("test".to_string()),
                error: None,
                now_ms: 302_000,
            })
            .unwrap();
            let deduped = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
                harness_home: harness_home.clone(),
                platform: Some(platform.to_string()),
                now_ms: 302_001,
                min_update_interval_ms: 0,
                max_nonterminal_updates_per_lane: 1,
                ..AgentProgressDeliveryPlanOptions::default()
            })
            .unwrap();
            assert!(deduped.pending.is_empty(), "platform={platform}");

            let _ = fs::remove_dir_all(root);
        }
    }

    #[test]
    fn delivery_plan_status_updates_for_tool_activity_after_body_cap() {
        let root = temp_root(
            "delivery_plan_status_updates_immediately_for_new_current_step_after_body_cap",
        );
        let harness_home = root.join(".agent-harness");
        let context = context();
        append_agent_progress_event(
            &harness_home,
            &AgentProgressEvent::new(
                &context,
                AgentProgressKind::Todo,
                "todo",
                "planning 1 task(s)",
                AgentProgressStatus::Started,
                1000,
            ),
        )
        .unwrap();
        let initial = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 2000,
            min_update_interval_ms: 0,
            max_nonterminal_updates_per_lane: 1,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert_eq!(initial.pending.len(), 2);
        for (index, pending) in initial.pending.into_iter().enumerate() {
            record_agent_progress_delivery(AgentProgressDeliveryRecordOptions {
                harness_home: harness_home.clone(),
                queue_id: pending.queue_id,
                platform: pending.platform,
                account_id: pending.account_id,
                channel_id: pending.channel_id,
                thread_id: pending.thread_id,
                user_id: pending.user_id,
                session_key: pending.session_key,
                message_kind: pending.message_kind,
                action: pending.action,
                status: AgentProgressDeliveryStatus::Delivered,
                provider_message_id: Some(format!("provider-{}", index + 1)),
                event_line: pending.event_line,
                text_hash: pending.text_hash,
                terminal: pending.terminal,
                policy_decision: Some("test".to_string()),
                error: None,
                now_ms: 2000,
            })
            .unwrap();
        }
        append_agent_progress_event(
            &harness_home,
            &AgentProgressEvent::new(
                &context,
                AgentProgressKind::ToolCall,
                "tool_call",
                "cargo test -p agent-harness-core",
                AgentProgressStatus::Progress,
                3000,
            ),
        )
        .unwrap();
        let capped = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 4000,
            min_update_interval_ms: 0,
            max_nonterminal_updates_per_lane: 1,
            status_heartbeat_after_body_cap_ms: 300_000,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert_eq!(capped.pending.len(), 1);
        let capped_status = &capped.pending[0];
        assert_eq!(
            capped_status.message_kind,
            AgentProgressDeliveryMessageKind::Status
        );
        assert_eq!(capped_status.action, AgentProgressDeliveryAction::Edit);
        assert!(
            capped_status
                .text
                .contains("cargo test -p agent-harness-core")
        );

        append_agent_progress_event(
            &harness_home,
            &AgentProgressEvent::new(
                &context,
                AgentProgressKind::AssistantNarration,
                "assistant_narration",
                "healthz age differs only because collect_status reads time later; relaxing age.",
                AgentProgressStatus::Progress,
                5000,
            ),
        )
        .unwrap();
        let realtime_summary = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 5001,
            min_update_interval_ms: 0,
            max_nonterminal_updates_per_lane: 1,
            status_heartbeat_after_body_cap_ms: 300_000,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();

        assert_eq!(realtime_summary.pending.len(), 1);
        let pending = &realtime_summary.pending[0];
        assert_eq!(
            pending.message_kind,
            AgentProgressDeliveryMessageKind::Status
        );
        assert_eq!(pending.action, AgentProgressDeliveryAction::Edit);
        assert!(pending.text.contains("Current step: healthz age differs"));
        assert!(pending.text.contains("Updates capped; still working."));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn progress_surface_volume_replay_converges_without_post_terminal_churn() {
        for platform in ["telegram", "discord"] {
            let root = temp_root(&format!("progress_surface_volume_replay_{platform}"));
            let harness_home = root.join(".agent-harness");
            let context = AgentProgressContext {
                queue_id: format!("turn:{platform}:progress-surface"),
                platform: platform.to_string(),
                channel_id: format!("{platform}-dm"),
                session_key: format!("{platform}:dm:user:main"),
                ..context()
            };
            append_agent_progress_event(
                &harness_home,
                &AgentProgressEvent::new(
                    &context,
                    AgentProgressKind::Todo,
                    "todo",
                    "planning progress replay",
                    AgentProgressStatus::Started,
                    1000,
                ),
            )
            .unwrap();

            let initial = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
                harness_home: harness_home.clone(),
                platform: Some(platform.to_string()),
                now_ms: 2000,
                min_update_interval_ms: 0,
                max_nonterminal_updates_per_lane: 1,
                ..AgentProgressDeliveryPlanOptions::default()
            })
            .unwrap();
            assert_eq!(initial.pending.len(), 2);
            for (index, pending) in initial.pending.into_iter().enumerate() {
                record_agent_progress_delivery(AgentProgressDeliveryRecordOptions {
                    harness_home: harness_home.clone(),
                    queue_id: pending.queue_id,
                    platform: pending.platform,
                    account_id: pending.account_id,
                    channel_id: pending.channel_id,
                    thread_id: pending.thread_id,
                    user_id: pending.user_id,
                    session_key: pending.session_key,
                    message_kind: pending.message_kind,
                    action: pending.action,
                    status: AgentProgressDeliveryStatus::Delivered,
                    provider_message_id: Some(format!("{platform}-provider-{}", index + 1)),
                    event_line: pending.event_line,
                    text_hash: pending.text_hash,
                    terminal: pending.terminal,
                    policy_decision: Some("scenario-matrix".to_string()),
                    error: None,
                    now_ms: 2000,
                })
                .unwrap();
            }

            append_agent_progress_event(
                &harness_home,
                &AgentProgressEvent::new(
                    &context,
                    AgentProgressKind::AssistantNarration,
                    "assistant_narration",
                    "Running focused progress replay.",
                    AgentProgressStatus::Progress,
                    3000,
                ),
            )
            .unwrap();
            append_agent_progress_event(
                &harness_home,
                &AgentProgressEvent::new(
                    &context,
                    AgentProgressKind::ToolCall,
                    "tool_call",
                    "cargo test -p agent-harness-core progress",
                    AgentProgressStatus::Progress,
                    4000,
                ),
            )
            .unwrap();
            let capped = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
                harness_home: harness_home.clone(),
                platform: Some(platform.to_string()),
                now_ms: 5000,
                min_update_interval_ms: 0,
                max_nonterminal_updates_per_lane: 1,
                status_heartbeat_after_body_cap_ms: 300_000,
                ..AgentProgressDeliveryPlanOptions::default()
            })
            .unwrap();
            assert_eq!(capped.pending.len(), 1);
            assert_eq!(capped.summary.volume_limited, 1);
            assert_eq!(capped.summary.rate_limited, 0);
            let immediate_status = capped
                .pending
                .iter()
                .find(|pending| pending.message_kind == AgentProgressDeliveryMessageKind::Status)
                .unwrap();
            assert_eq!(immediate_status.action, AgentProgressDeliveryAction::Edit);
            assert!(!immediate_status.terminal);
            assert!(
                immediate_status
                    .text
                    .contains("Current step: Running focused progress replay.")
            );
            record_agent_progress_delivery(AgentProgressDeliveryRecordOptions {
                harness_home: harness_home.clone(),
                queue_id: immediate_status.queue_id.clone(),
                platform: immediate_status.platform.clone(),
                account_id: immediate_status.account_id.clone(),
                channel_id: immediate_status.channel_id.clone(),
                thread_id: immediate_status.thread_id.clone(),
                user_id: immediate_status.user_id.clone(),
                session_key: immediate_status.session_key.clone(),
                message_kind: immediate_status.message_kind,
                action: immediate_status.action,
                status: AgentProgressDeliveryStatus::Delivered,
                provider_message_id: immediate_status.provider_message_id.clone(),
                event_line: immediate_status.event_line,
                text_hash: immediate_status.text_hash.clone(),
                terminal: immediate_status.terminal,
                policy_decision: Some("scenario-matrix".to_string()),
                error: None,
                now_ms: 5000,
            })
            .unwrap();

            let heartbeat = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
                harness_home: harness_home.clone(),
                platform: Some(platform.to_string()),
                now_ms: 305_000,
                min_update_interval_ms: 0,
                max_nonterminal_updates_per_lane: 1,
                status_heartbeat_after_body_cap_ms: 300_000,
                ..AgentProgressDeliveryPlanOptions::default()
            })
            .unwrap();
            assert_eq!(heartbeat.pending.len(), 1);
            let status = heartbeat
                .pending
                .iter()
                .find(|pending| pending.message_kind == AgentProgressDeliveryMessageKind::Status)
                .unwrap();
            assert_eq!(status.action, AgentProgressDeliveryAction::Edit);
            assert!(!status.terminal);
            assert!(
                status
                    .text
                    .contains("Current step: Running focused progress replay.")
            );
            assert!(status.text.contains("Updates capped; still working."));
            record_agent_progress_delivery(AgentProgressDeliveryRecordOptions {
                harness_home: harness_home.clone(),
                queue_id: status.queue_id.clone(),
                platform: status.platform.clone(),
                account_id: status.account_id.clone(),
                channel_id: status.channel_id.clone(),
                thread_id: status.thread_id.clone(),
                user_id: status.user_id.clone(),
                session_key: status.session_key.clone(),
                message_kind: status.message_kind,
                action: status.action,
                status: AgentProgressDeliveryStatus::Delivered,
                provider_message_id: status.provider_message_id.clone(),
                event_line: status.event_line,
                text_hash: status.text_hash.clone(),
                terminal: status.terminal,
                policy_decision: Some("scenario-matrix".to_string()),
                error: None,
                now_ms: 305_000,
            })
            .unwrap();

            append_agent_progress_event(
                &harness_home,
                &AgentProgressEvent::new(
                    &context,
                    AgentProgressKind::Runtime,
                    "runtime",
                    "completed",
                    AgentProgressStatus::Completed,
                    306_000,
                ),
            )
            .unwrap();
            let terminal = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
                harness_home: harness_home.clone(),
                platform: Some(platform.to_string()),
                now_ms: 307_000,
                min_update_interval_ms: 0,
                max_nonterminal_updates_per_lane: 1,
                ..AgentProgressDeliveryPlanOptions::default()
            })
            .unwrap();
            assert_eq!(terminal.pending.len(), 2);
            assert!(terminal.pending.iter().all(|pending| pending.terminal));
            assert!(
                terminal
                    .pending
                    .iter()
                    .all(|pending| pending.action == AgentProgressDeliveryAction::Edit)
            );
            for pending in terminal.pending {
                record_agent_progress_delivery(AgentProgressDeliveryRecordOptions {
                    harness_home: harness_home.clone(),
                    queue_id: pending.queue_id,
                    platform: pending.platform,
                    account_id: pending.account_id,
                    channel_id: pending.channel_id,
                    thread_id: pending.thread_id,
                    user_id: pending.user_id,
                    session_key: pending.session_key,
                    message_kind: pending.message_kind,
                    action: pending.action,
                    status: AgentProgressDeliveryStatus::Delivered,
                    provider_message_id: pending.provider_message_id,
                    event_line: pending.event_line,
                    text_hash: pending.text_hash,
                    terminal: pending.terminal,
                    policy_decision: Some("scenario-matrix".to_string()),
                    error: None,
                    now_ms: 307_000,
                })
                .unwrap();
            }

            append_agent_progress_event(
                &harness_home,
                &AgentProgressEvent::new(
                    &context,
                    AgentProgressKind::ToolCall,
                    "tool_call",
                    "late tool output after terminal",
                    AgentProgressStatus::Progress,
                    308_000,
                ),
            )
            .unwrap();
            let late = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
                harness_home: harness_home.clone(),
                platform: Some(platform.to_string()),
                now_ms: 309_000,
                min_update_interval_ms: 0,
                max_nonterminal_updates_per_lane: 1,
                ..AgentProgressDeliveryPlanOptions::default()
            })
            .unwrap();
            assert!(late.pending.is_empty());

            let _ = fs::remove_dir_all(root);
        }
    }

    #[test]
    fn progress_delivery_repeated_events_converge_to_one_provider_message() {
        progress_surface_volume_replay_converges_without_post_terminal_churn();
    }

    #[test]
    fn delivery_plan_reads_volume_limit_from_response_config() {
        let root = temp_root("delivery_plan_reads_volume_limit_from_response_config");
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            r#"{"response":{"progressDeliveryMaxNonterminalUpdatesPerLane":1}}"#,
        )
        .unwrap();
        let context = context();
        append_agent_progress_event(
            &harness_home,
            &AgentProgressEvent::new(
                &context,
                AgentProgressKind::Todo,
                "todo",
                "planning 1 task(s)",
                AgentProgressStatus::Started,
                1000,
            ),
        )
        .unwrap();
        let initial = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 2000,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert_eq!(initial.pending.len(), 2);
        for (index, pending) in initial.pending.into_iter().enumerate() {
            record_agent_progress_delivery(AgentProgressDeliveryRecordOptions {
                harness_home: harness_home.clone(),
                queue_id: pending.queue_id,
                platform: pending.platform,
                account_id: pending.account_id,
                channel_id: pending.channel_id,
                thread_id: pending.thread_id,
                user_id: pending.user_id,
                session_key: pending.session_key,
                message_kind: pending.message_kind,
                action: pending.action,
                status: AgentProgressDeliveryStatus::Delivered,
                provider_message_id: Some(format!("provider-{}", index + 1)),
                event_line: pending.event_line,
                text_hash: pending.text_hash,
                terminal: pending.terminal,
                policy_decision: Some("test".to_string()),
                error: None,
                now_ms: 2000,
            })
            .unwrap();
        }
        append_agent_progress_event(
            &harness_home,
            &AgentProgressEvent::new(
                &context,
                AgentProgressKind::ToolCall,
                "tool_call",
                "cargo test",
                AgentProgressStatus::Progress,
                3000,
            ),
        )
        .unwrap();

        let limited = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 4000,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();

        assert_eq!(limited.pending.len(), 1);
        assert_eq!(
            limited.pending[0].message_kind,
            AgentProgressDeliveryMessageKind::Status
        );
        assert!(limited.pending[0].text.contains("cargo test"));
        assert_eq!(limited.summary.volume_limited, 1);
        assert_eq!(limited.summary.rate_limited, 0);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn skipped_denied_progress_delivery_advances_cursor() {
        let root = temp_root("skipped_denied_progress_delivery_advances_cursor");
        let harness_home = root.join(".agent-harness");
        let context = context();
        append_agent_progress_event(
            &harness_home,
            &AgentProgressEvent::new(
                &context,
                AgentProgressKind::Terminal,
                "terminal",
                "pwsh: agent-harness status",
                AgentProgressStatus::Started,
                1000,
            ),
        )
        .unwrap();

        let plan = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 2000,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert_eq!(plan.pending.len(), 2);

        for pending in plan.pending {
            record_agent_progress_delivery(AgentProgressDeliveryRecordOptions {
                harness_home: harness_home.clone(),
                queue_id: pending.queue_id,
                platform: pending.platform,
                account_id: pending.account_id,
                channel_id: pending.channel_id,
                thread_id: pending.thread_id,
                user_id: pending.user_id,
                session_key: pending.session_key,
                message_kind: pending.message_kind,
                action: pending.action,
                status: AgentProgressDeliveryStatus::SkippedDenied,
                provider_message_id: None,
                event_line: pending.event_line,
                text_hash: pending.text_hash,
                terminal: pending.terminal,
                policy_decision: Some("test".to_string()),
                error: Some("local/offline progress event".to_string()),
                now_ms: 2000,
            })
            .unwrap();
        }

        let next = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 3000,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert_eq!(next.pending.len(), 0);
        assert_eq!(next.summary.delivered_current, 2);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn skipped_permanent_progress_delivery_advances_cursor() {
        let root = temp_root("skipped_permanent_progress_delivery_advances_cursor");
        let harness_home = root.join(".agent-harness");
        let context = context();
        append_agent_progress_event(
            &harness_home,
            &AgentProgressEvent::new(
                &context,
                AgentProgressKind::ToolCall,
                "tool_call",
                "provider edit rejected permanently",
                AgentProgressStatus::Started,
                1000,
            ),
        )
        .unwrap();

        let plan = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 2000,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert_eq!(plan.pending.len(), 2);

        for pending in plan.pending {
            record_agent_progress_delivery(AgentProgressDeliveryRecordOptions {
                harness_home: harness_home.clone(),
                queue_id: pending.queue_id,
                platform: pending.platform,
                account_id: pending.account_id,
                channel_id: pending.channel_id,
                thread_id: pending.thread_id,
                user_id: pending.user_id,
                session_key: pending.session_key,
                message_kind: pending.message_kind,
                action: pending.action,
                status: AgentProgressDeliveryStatus::SkippedPermanent,
                provider_message_id: None,
                event_line: pending.event_line,
                text_hash: pending.text_hash,
                terminal: pending.terminal,
                policy_decision: Some("test".to_string()),
                error: Some("permanent provider edit failure".to_string()),
                now_ms: 2000,
            })
            .unwrap();
        }

        let next = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 3000,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert_eq!(next.pending.len(), 0);
        assert_eq!(next.summary.delivered_current, 2);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn terminal_progress_state_is_monotonic_after_late_events() {
        let root = temp_root("terminal_progress_state_is_monotonic_after_late_events");
        let harness_home = root.join(".agent-harness");
        let context = context();
        append_agent_progress_event(
            &harness_home,
            &AgentProgressEvent::new(
                &context,
                AgentProgressKind::Runtime,
                "run",
                "timed out",
                AgentProgressStatus::Failed,
                1000,
            ),
        )
        .unwrap();

        let first = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 2000,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert!(!first.pending.is_empty());
        assert!(first.pending.iter().all(|pending| pending.terminal));
        assert!(
            first
                .pending
                .iter()
                .any(|pending| pending.text.contains("Failed"))
        );
        for pending in first.pending {
            record_agent_progress_delivery(AgentProgressDeliveryRecordOptions {
                harness_home: harness_home.clone(),
                queue_id: pending.queue_id,
                platform: pending.platform,
                account_id: pending.account_id,
                channel_id: pending.channel_id,
                thread_id: pending.thread_id,
                user_id: pending.user_id,
                session_key: pending.session_key,
                message_kind: pending.message_kind,
                action: pending.action,
                status: AgentProgressDeliveryStatus::Delivered,
                provider_message_id: Some(format!("{:?}", pending.message_kind)),
                event_line: pending.event_line,
                text_hash: pending.text_hash,
                terminal: pending.terminal,
                policy_decision: Some("test".to_string()),
                error: None,
                now_ms: 2000,
            })
            .unwrap();
        }

        append_agent_progress_event(
            &harness_home,
            &AgentProgressEvent::new(
                &context,
                AgentProgressKind::Terminal,
                "terminal",
                "late background output",
                AgentProgressStatus::Started,
                3000,
            ),
        )
        .unwrap();

        let after_late_event = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 4000,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert!(after_late_event.pending.is_empty());
        assert_eq!(after_late_event.summary.delivered_current, 2);
        let mut warnings = Vec::new();
        let stored_events =
            read_progress_events(&agent_progress_events_file(&harness_home), &mut warnings)
                .unwrap();
        assert!(warnings.is_empty());
        let event_refs = stored_events
            .iter()
            .map(|stored| &stored.event)
            .collect::<Vec<_>>();
        assert!(
            render_agent_progress_status(event_refs.as_slice(), 4000, 120, 1200, false)
                .contains("Failed")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn progress_delivery_plans_new_queue_after_prior_terminal_queue() {
        let root = temp_root("progress_delivery_plans_new_queue_after_prior_terminal_queue");
        let harness_home = root.join(".agent-harness");
        let first_context = context();
        append_agent_progress_event(
            &harness_home,
            &AgentProgressEvent::new(
                &first_context,
                AgentProgressKind::Terminal,
                "terminal",
                "pwsh: agent-harness status",
                AgentProgressStatus::Started,
                1000,
            ),
        )
        .unwrap();

        let first = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 2000,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert_eq!(first.pending.len(), 2);
        for pending in first.pending {
            record_agent_progress_delivery(AgentProgressDeliveryRecordOptions {
                harness_home: harness_home.clone(),
                queue_id: pending.queue_id,
                platform: pending.platform,
                account_id: pending.account_id,
                channel_id: pending.channel_id,
                thread_id: pending.thread_id,
                user_id: pending.user_id,
                session_key: pending.session_key,
                message_kind: pending.message_kind,
                action: pending.action,
                status: AgentProgressDeliveryStatus::Delivered,
                provider_message_id: Some(format!("provider-{:?}", pending.message_kind)),
                event_line: pending.event_line,
                text_hash: pending.text_hash,
                terminal: pending.terminal,
                policy_decision: Some("test".to_string()),
                error: None,
                now_ms: 2000,
            })
            .unwrap();
        }

        let mut second_context = context();
        second_context.queue_id = "turn:2".to_string();
        append_agent_progress_event(
            &harness_home,
            &AgentProgressEvent::new(
                &second_context,
                AgentProgressKind::Todo,
                "todo",
                "checking progress delivery",
                AgentProgressStatus::Started,
                3000,
            ),
        )
        .unwrap();

        let second = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 4000,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert_eq!(second.pending.len(), 2);
        assert!(
            second
                .pending
                .iter()
                .all(|pending| pending.queue_id == "turn:2")
        );
        assert_eq!(second.summary.delivered_current, 2);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn terminal_cursor_does_not_suppress_unrecorded_lane_retry() {
        let root = temp_root("terminal_cursor_does_not_suppress_unrecorded_lane_retry");
        let harness_home = root.join(".agent-harness");
        let context = context();
        append_agent_progress_event(
            &harness_home,
            &AgentProgressEvent::new(
                &context,
                AgentProgressKind::Terminal,
                "terminal",
                "cargo test -p agent-harness-core",
                AgentProgressStatus::Started,
                1000,
            ),
        )
        .unwrap();
        append_agent_progress_event(
            &harness_home,
            &AgentProgressEvent::new(
                &context,
                AgentProgressKind::ToolCall,
                "tool_call",
                "cargo test",
                AgentProgressStatus::Completed,
                2500,
            ),
        )
        .unwrap();
        append_agent_progress_event(
            &harness_home,
            &AgentProgressEvent::new(
                &context,
                AgentProgressKind::Runtime,
                "run",
                "completed",
                AgentProgressStatus::Completed,
                2000,
            ),
        )
        .unwrap();

        let first = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 3000,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert_eq!(first.pending.len(), 2);
        assert!(first.pending.iter().all(|pending| pending.terminal));

        for pending in first.pending {
            let status = if pending.message_kind == AgentProgressDeliveryMessageKind::Body {
                AgentProgressDeliveryStatus::Delivered
            } else {
                AgentProgressDeliveryStatus::Failed
            };
            record_agent_progress_delivery(AgentProgressDeliveryRecordOptions {
                harness_home: harness_home.clone(),
                queue_id: pending.queue_id,
                platform: pending.platform,
                account_id: pending.account_id,
                channel_id: pending.channel_id,
                thread_id: pending.thread_id,
                user_id: pending.user_id,
                session_key: pending.session_key,
                message_kind: pending.message_kind,
                action: pending.action,
                status,
                provider_message_id: (status == AgentProgressDeliveryStatus::Delivered)
                    .then_some("provider-body".to_string()),
                event_line: pending.event_line,
                text_hash: pending.text_hash,
                terminal: pending.terminal,
                policy_decision: Some("test".to_string()),
                error: (status == AgentProgressDeliveryStatus::Failed)
                    .then_some("retryable provider failure".to_string()),
                now_ms: 3000,
            })
            .unwrap();
        }

        let retry = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 4000,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert_eq!(retry.pending.len(), 1);
        assert_eq!(
            retry.pending[0].message_kind,
            AgentProgressDeliveryMessageKind::Status
        );
        assert_eq!(retry.summary.delivered_current, 1);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn terminal_status_delivery_does_not_suppress_failed_body_retry() {
        let root = temp_root("terminal_status_delivery_does_not_suppress_failed_body_retry");
        let harness_home = root.join(".agent-harness");
        let context = context();
        append_agent_progress_event(
            &harness_home,
            &AgentProgressEvent::new(
                &context,
                AgentProgressKind::Terminal,
                "terminal",
                "cargo test -p agent-harness-core",
                AgentProgressStatus::Started,
                1000,
            ),
        )
        .unwrap();

        let initial = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 2000,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert_eq!(initial.pending.len(), 2);
        for pending in initial.pending {
            record_agent_progress_delivery(AgentProgressDeliveryRecordOptions {
                harness_home: harness_home.clone(),
                queue_id: pending.queue_id,
                platform: pending.platform,
                account_id: pending.account_id,
                channel_id: pending.channel_id,
                thread_id: pending.thread_id,
                user_id: pending.user_id,
                session_key: pending.session_key,
                message_kind: pending.message_kind,
                action: pending.action,
                status: AgentProgressDeliveryStatus::Delivered,
                provider_message_id: Some(format!("provider-{:?}", pending.message_kind)),
                event_line: pending.event_line,
                text_hash: pending.text_hash,
                terminal: pending.terminal,
                policy_decision: Some("test".to_string()),
                error: None,
                now_ms: 2000,
            })
            .unwrap();
        }

        append_agent_progress_event(
            &harness_home,
            &AgentProgressEvent::new(
                &context,
                AgentProgressKind::ToolCall,
                "tool_call",
                "cargo test",
                AgentProgressStatus::Completed,
                2500,
            ),
        )
        .unwrap();
        append_agent_progress_event(
            &harness_home,
            &AgentProgressEvent::new(
                &context,
                AgentProgressKind::Runtime,
                "run",
                "completed",
                AgentProgressStatus::Completed,
                3000,
            ),
        )
        .unwrap();

        let terminal = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 4000,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert_eq!(terminal.pending.len(), 2);
        let terminal_status = terminal
            .pending
            .iter()
            .find(|pending| pending.message_kind == AgentProgressDeliveryMessageKind::Status)
            .unwrap()
            .clone();
        let terminal_body = terminal
            .pending
            .iter()
            .find(|pending| pending.message_kind == AgentProgressDeliveryMessageKind::Body)
            .unwrap()
            .clone();

        record_agent_progress_delivery(AgentProgressDeliveryRecordOptions {
            harness_home: harness_home.clone(),
            queue_id: terminal_status.queue_id,
            platform: terminal_status.platform,
            account_id: terminal_status.account_id,
            channel_id: terminal_status.channel_id,
            thread_id: terminal_status.thread_id,
            user_id: terminal_status.user_id,
            session_key: terminal_status.session_key,
            message_kind: terminal_status.message_kind,
            action: terminal_status.action,
            status: AgentProgressDeliveryStatus::Delivered,
            provider_message_id: terminal_status.provider_message_id,
            event_line: terminal_status.event_line,
            text_hash: terminal_status.text_hash,
            terminal: terminal_status.terminal,
            policy_decision: Some("test".to_string()),
            error: None,
            now_ms: 4000,
        })
        .unwrap();
        record_agent_progress_delivery(AgentProgressDeliveryRecordOptions {
            harness_home: harness_home.clone(),
            queue_id: terminal_body.queue_id,
            platform: terminal_body.platform,
            account_id: terminal_body.account_id,
            channel_id: terminal_body.channel_id,
            thread_id: terminal_body.thread_id,
            user_id: terminal_body.user_id,
            session_key: terminal_body.session_key,
            message_kind: terminal_body.message_kind,
            action: terminal_body.action,
            status: AgentProgressDeliveryStatus::Failed,
            provider_message_id: terminal_body.provider_message_id,
            event_line: terminal_body.event_line,
            text_hash: terminal_body.text_hash,
            terminal: terminal_body.terminal,
            policy_decision: Some("test".to_string()),
            error: Some("retryable provider failure".to_string()),
            now_ms: 4000,
        })
        .unwrap();

        let retry = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 5000,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert_eq!(retry.pending.len(), 1);
        assert_eq!(
            retry.pending[0].message_kind,
            AgentProgressDeliveryMessageKind::Body
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn terminal_progress_waits_until_source_final_delivery_is_delivered() {
        let root = temp_root("terminal_progress_waits_until_source_final_delivery_is_delivered");
        let harness_home = root.join(".agent-harness");
        let context = context();
        append_agent_progress_event(
            &harness_home,
            &AgentProgressEvent::new(
                &context,
                AgentProgressKind::Todo,
                "todo",
                "planning 1 task(s)",
                AgentProgressStatus::Started,
                1000,
            ),
        )
        .unwrap();

        let initial = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 2000,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert_eq!(initial.pending.len(), 2);
        for (index, pending) in initial.pending.into_iter().enumerate() {
            record_agent_progress_delivery(AgentProgressDeliveryRecordOptions {
                harness_home: harness_home.clone(),
                queue_id: pending.queue_id,
                platform: pending.platform,
                account_id: pending.account_id,
                channel_id: pending.channel_id,
                thread_id: pending.thread_id,
                user_id: pending.user_id,
                session_key: pending.session_key,
                message_kind: pending.message_kind,
                action: pending.action,
                status: AgentProgressDeliveryStatus::Delivered,
                provider_message_id: Some(format!("progress-{index}")),
                event_line: pending.event_line,
                text_hash: pending.text_hash,
                terminal: pending.terminal,
                policy_decision: Some("test".to_string()),
                error: None,
                now_ms: 2000,
            })
            .unwrap();
        }

        append_agent_progress_event(
            &harness_home,
            &AgentProgressEvent::new(
                &context,
                AgentProgressKind::Runtime,
                "run",
                "completed",
                AgentProgressStatus::Completed,
                3000,
            ),
        )
        .unwrap();
        let outbox_dir = harness_home.join("state").join("channels");
        fs::create_dir_all(&outbox_dir).unwrap();
        crate::append_jsonl_value(
            &outbox_dir.join("outbox.jsonl"),
            &crate::ChannelOutboundMessage {
                platform: context.platform.clone(),
                account_id: context.account_id.clone(),
                channel_id: context.channel_id.clone(),
                user_id: context.user_id.clone(),
                session_key: context.session_key.clone(),
                delivery_id: None,
                kind: crate::ChannelOutboundMessageKind::AgentReply,
                source_queue_id: Some(context.queue_id.clone()),
                source_completion_file: None,
                text: "final reply".to_string(),
                presentation: None,
                delivery_intent: None,
                attachments: Vec::new(),
            },
        )
        .unwrap();
        let final_pending = crate::plan_channel_outbox(crate::ChannelOutboxPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            limit: 10,
        })
        .unwrap();
        let delivery = final_pending
            .pending
            .iter()
            .find(|pending| {
                pending.message.source_queue_id.as_deref() == Some(context.queue_id.as_str())
            })
            .expect("source final outbox should be pending");

        let delayed_terminal = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 4000,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert!(
            delayed_terminal.pending.is_empty(),
            "terminal progress must wait for source final provider delivery: {:?}",
            delayed_terminal.pending
        );

        crate::record_channel_delivery(crate::ChannelDeliveryRecordOptions {
            harness_home: harness_home.clone(),
            delivery_id: delivery.delivery_id.clone(),
            status: crate::ChannelDeliveryStatus::Delivered,
            platform: context.platform.clone(),
            account_id: context.account_id.clone(),
            channel_id: context.channel_id.clone(),
            user_id: context.user_id.clone(),
            session_key: context.session_key.clone(),
            provider_message_id: Some("final-provider-message".to_string()),
            error: None,
            now_ms: 5000,
            rendered_units: Vec::new(),
            presentation: None,
        })
        .unwrap();

        let after_final = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 6000,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert_eq!(after_final.pending.len(), 2);
        assert!(after_final.pending.iter().all(|pending| pending.terminal));
        assert!(
            after_final.pending.iter().any(|pending| {
                pending.message_kind == AgentProgressDeliveryMessageKind::Status
            })
        );
        assert!(
            after_final
                .pending
                .iter()
                .any(|pending| { pending.message_kind == AgentProgressDeliveryMessageKind::Body })
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn progress_delivery_cursor_reads_only_new_events_after_offset() {
        let root = temp_root("progress_delivery_cursor_reads_only_new_events_after_offset");
        let harness_home = root.join(".agent-harness");
        let context = context();
        append_agent_progress_event(
            &harness_home,
            &AgentProgressEvent::new(
                &context,
                AgentProgressKind::Runtime,
                "runtime",
                "starting",
                AgentProgressStatus::Started,
                1000,
            ),
        )
        .unwrap();

        let first = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 2000,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert_eq!(first.summary.read_from_byte, 0);
        assert_eq!(first.summary.new_events, 1);
        assert!(first.summary.read_to_byte > 0);

        append_agent_progress_event(
            &harness_home,
            &AgentProgressEvent::new(
                &context,
                AgentProgressKind::ToolCall,
                "tool_call",
                "cargo test",
                AgentProgressStatus::Started,
                3000,
            ),
        )
        .unwrap();

        let second = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 4000,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();

        assert_eq!(second.summary.read_from_byte, first.summary.read_to_byte);
        assert!(second.summary.read_to_byte > second.summary.read_from_byte);
        assert_eq!(second.summary.new_events, 1);
        assert_eq!(second.summary.cached_events, 1);
        assert_eq!(second.summary.total_events, 2);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn progress_delivery_defers_cached_events_while_index_snapshot_is_not_fresh() {
        let root =
            temp_root("progress_delivery_defers_cached_events_while_index_snapshot_is_not_fresh");
        let harness_home = root.join(".agent-harness");
        let context = context();
        let stale_event = AgentProgressEvent::new(
            &context,
            AgentProgressKind::Runtime,
            "runtime",
            "historical work",
            AgentProgressStatus::Started,
            1_000,
        );
        let mut state = AgentProgressDeliveryState::default();
        state.compacted_events.insert(
            context.queue_id.clone(),
            vec![StoredProgressEvent {
                line_number: 1,
                event: stale_event,
            }],
        );
        let state_file = agent_progress_delivery_state_file(&harness_home);
        fs::create_dir_all(state_file.parent().unwrap()).unwrap();
        write_delivery_state(&state_file, &state).unwrap();

        let events_file = agent_progress_events_file(&harness_home);
        fs::create_dir_all(events_file.parent().unwrap()).unwrap();
        fs::write(&events_file, b"").unwrap();
        let (ready_sender, ready_receiver) = mpsc::channel();
        let (release_sender, release_receiver) = mpsc::channel();
        let held_events_file = events_file.clone();
        let holder = std::thread::spawn(move || {
            crate::logging::with_jsonl_append_lock(&held_events_file, || {
                ready_sender.send(()).unwrap();
                release_receiver
                    .recv_timeout(Duration::from_secs(5))
                    .expect("test must release the progress append lock");
                Ok(())
            })
        });
        ready_receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("test progress append lock holder must start");

        let plan = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 2_000,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();

        release_sender.send(()).unwrap();
        holder.join().unwrap().unwrap();

        assert!(plan.pending.is_empty(), "{plan:?}");
        assert!(
            plan.warnings
                .iter()
                .any(|warning| warning.contains("deferred cached progress delivery")),
            "{plan:?}"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn progress_delivery_does_not_fresh_send_historical_event_without_existing_surface() {
        let root = temp_root(
            "progress_delivery_does_not_fresh_send_historical_event_without_existing_surface",
        );
        let harness_home = root.join(".agent-harness");
        let context = context();
        append_agent_progress_event(
            &harness_home,
            &AgentProgressEvent::new(
                &context,
                AgentProgressKind::Runtime,
                "runtime",
                "historical work",
                AgentProgressStatus::Completed,
                1_000,
            ),
        )
        .unwrap();

        let plan = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 1_000 + 10 * 60 * 1_000 + 1,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();

        assert!(plan.pending.is_empty(), "{plan:?}");
        assert_eq!(plan.summary.skipped_stale_fresh_sends, 1, "{plan:?}");
        assert!(
            plan.warnings
                .iter()
                .any(|warning| warning.contains("historical fresh progress surface")),
            "{plan:?}"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn progress_delivery_compacts_repeated_cached_tool_events() {
        let root = temp_root("progress_delivery_compacts_repeated_cached_tool_events");
        let harness_home = root.join(".agent-harness");
        let context = context();
        for index in 0..80 {
            append_agent_progress_event(
                &harness_home,
                &AgentProgressEvent::new(
                    &context,
                    AgentProgressKind::ToolCall,
                    "tool_call",
                    "cargo test",
                    AgentProgressStatus::Progress,
                    1000 + index,
                ),
            )
            .unwrap();
        }
        append_agent_progress_event(
            &harness_home,
            &AgentProgressEvent::new(
                &context,
                AgentProgressKind::Runtime,
                "runtime",
                "completed",
                AgentProgressStatus::Completed,
                2000,
            ),
        )
        .unwrap();

        let plan = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 3000,
            min_update_interval_ms: 0,
            max_events_per_panel: 8,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert_eq!(plan.summary.new_events, 81);

        let mut warnings = Vec::new();
        let state = read_delivery_state(
            &agent_progress_delivery_state_file(&harness_home),
            &mut warnings,
        )
        .unwrap();
        assert!(warnings.is_empty());
        let compacted = state.compacted_events.get("turn:1").unwrap();
        assert!(compacted.len() < 20, "compacted len={}", compacted.len());
        assert!(
            compacted
                .iter()
                .any(|stored| is_terminal_event(&stored.event))
        );
        assert_eq!(state.ledger.line_number, 81);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn progress_delivery_prunes_old_delivered_terminal_queue_cache() {
        let root = temp_root("progress_delivery_prunes_old_delivered_terminal_queue_cache");
        let harness_home = root.join(".agent-harness");
        let mut state = AgentProgressDeliveryState::default();
        for index in 0..40 {
            let mut old_context = context();
            old_context.queue_id = format!("turn:old:{index}");
            old_context.session_key = format!("telegram:dm:user:main:old-{index}");
            let line_number = index + 1;
            let event = AgentProgressEvent::new(
                &old_context,
                AgentProgressKind::Runtime,
                "runtime",
                "completed",
                AgentProgressStatus::Completed,
                1_000 + index as i64,
            );
            state.compacted_events.insert(
                old_context.queue_id.clone(),
                vec![StoredProgressEvent { line_number, event }],
            );
            let mut cursor = AgentProgressDeliveryCursor::new(
                old_context.platform.clone(),
                old_context.channel_id.clone(),
                old_context.user_id.clone(),
                old_context.session_key.clone(),
            );
            cursor.record_lane(
                AgentProgressDeliveryMessageKind::Body,
                Some(format!("body-{index}")),
                line_number,
                format!("body-hash-{index}"),
                2_000 + index as i64,
                true,
                true,
            );
            cursor.record_lane(
                AgentProgressDeliveryMessageKind::Status,
                Some(format!("status-{index}")),
                line_number,
                format!("status-hash-{index}"),
                2_000 + index as i64,
                true,
                true,
            );
            state.queues.insert(old_context.queue_id.clone(), cursor);
        }

        let mut active_context = context();
        active_context.queue_id = "turn:active".to_string();
        let active_event = AgentProgressEvent::new(
            &active_context,
            AgentProgressKind::Todo,
            "todo",
            "planning 1 task(s)",
            AgentProgressStatus::Started,
            9_900_000,
        );
        state.compacted_events.insert(
            active_context.queue_id.clone(),
            vec![StoredProgressEvent {
                line_number: 41,
                event: active_event,
            }],
        );
        state.queues.insert(
            active_context.queue_id.clone(),
            AgentProgressDeliveryCursor::new(
                active_context.platform.clone(),
                active_context.channel_id.clone(),
                active_context.user_id.clone(),
                active_context.session_key.clone(),
            ),
        );
        let state_file = agent_progress_delivery_state_file(&harness_home);
        fs::create_dir_all(state_file.parent().unwrap()).unwrap();
        write_delivery_state(&state_file, &state).unwrap();

        let plan = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 10_000_000,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();

        assert_eq!(plan.summary.queues, 1);
        let mut warnings = Vec::new();
        let pruned = read_delivery_state(&state_file, &mut warnings).unwrap();
        assert!(warnings.is_empty());
        assert!(pruned.compacted_events.contains_key("turn:active"));
        assert!(pruned.queues.contains_key("turn:active"));
        assert!(
            pruned
                .compacted_events
                .keys()
                .all(|queue_id| !queue_id.starts_with("turn:old:")),
            "{:?}",
            pruned.compacted_events.keys().collect::<Vec<_>>()
        );
        assert!(
            pruned
                .queues
                .keys()
                .all(|queue_id| !queue_id.starts_with("turn:old:")),
            "{:?}",
            pruned.queues.keys().collect::<Vec<_>>()
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn progress_delivery_agent_mute_suppresses_pending_and_cache() {
        let root = temp_root("progress_delivery_agent_mute_suppresses_pending_and_cache");
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join(HARNESS_CONFIG_FILE_NAME),
            r#"{
              "response": {
                "progressDeliveryAgentModes": { "xiaoxiaoli": "off" }
              }
            }"#,
        )
        .unwrap();
        let mut context = context();
        context.agent_id = Some("xiaoxiaoli".to_string());
        append_agent_progress_event(
            &harness_home,
            &AgentProgressEvent::new(
                &context,
                AgentProgressKind::Todo,
                "todo",
                "planning 1 task(s)",
                AgentProgressStatus::Started,
                1000,
            ),
        )
        .unwrap();

        let plan = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 2000,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();

        assert!(plan.pending.is_empty());
        assert_eq!(plan.summary.skipped_muted, 1);

        let mut warnings = Vec::new();
        let state = read_delivery_state(
            &agent_progress_delivery_state_file(&harness_home),
            &mut warnings,
        )
        .unwrap();
        assert!(warnings.is_empty());
        assert!(state.compacted_events.is_empty());
        assert_eq!(state.ledger.line_number, 1);

        let next = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 3000,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert!(next.pending.is_empty());
        assert_eq!(next.summary.skipped_muted, 0);
        assert_eq!(next.summary.cached_events, 0);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn progress_delivery_global_off_suppresses_pending() {
        let root = temp_root("progress_delivery_global_off_suppresses_pending");
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join(HARNESS_CONFIG_FILE_NAME),
            r#"{
              "response": {
                "progressDeliveryMode": "off"
              }
            }"#,
        )
        .unwrap();
        let context = context();
        append_agent_progress_event(
            &harness_home,
            &AgentProgressEvent::new(
                &context,
                AgentProgressKind::Todo,
                "todo",
                "planning 1 task(s)",
                AgentProgressStatus::Started,
                1000,
            ),
        )
        .unwrap();

        let plan = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 2000,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();

        assert!(plan.pending.is_empty());
        assert_eq!(plan.summary.skipped_muted, 1);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn progress_delivery_channel_mode_overrides_agent_mute() {
        let root = temp_root("progress_delivery_channel_mode_overrides_agent_mute");
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join(HARNESS_CONFIG_FILE_NAME),
            r#"{
              "response": {
                "progressDeliveryAgentModes": { "xiaoxiaoli": "off" },
                "progressDeliveryChannelModes": { "telegram:group-alpha": "on" }
              }
            }"#,
        )
        .unwrap();
        let mut context = context();
        context.agent_id = Some("xiaoxiaoli".to_string());
        context.channel_id = "group-alpha".to_string();
        append_agent_progress_event(
            &harness_home,
            &AgentProgressEvent::new(
                &context,
                AgentProgressKind::Todo,
                "todo",
                "planning 1 task(s)",
                AgentProgressStatus::Started,
                1000,
            ),
        )
        .unwrap();

        let plan = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 2000,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();

        assert_eq!(plan.pending.len(), 2);
        assert_eq!(plan.summary.skipped_muted, 0);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn progress_surface_claim_reclaims_same_queue_orphan_without_ttl() {
        let root = temp_root("progress_surface_claim_reclaims_same_queue_orphan_without_ttl");
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        append_agent_progress_event(
            &harness_home,
            &AgentProgressEvent::new(
                &context(),
                AgentProgressKind::Todo,
                "todo",
                "planning 1 task(s)",
                AgentProgressStatus::Started,
                1000,
            ),
        )
        .unwrap();

        let first = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 2000,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert_eq!(first.pending.len(), 2);
        assert!(first.pending.iter().all(|pending| {
            pending.action == AgentProgressDeliveryAction::Send
                && !pending.idempotency_hit
                && pending.fresh_send_reason
                    == Some(AgentProgressDeliveryFreshSendReason::NoExistingStatusSurface)
        }));

        let second = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 2001,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert_eq!(second.pending.len(), 2);
        assert!(second.pending.iter().all(|pending| {
            pending.action == AgentProgressDeliveryAction::Send
                && !pending.idempotency_hit
                && pending.fresh_send_reason
                    == Some(AgentProgressDeliveryFreshSendReason::NoExistingStatusSurface)
        }));

        let expired = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 2000 + PROGRESS_SURFACE_CLAIM_TTL_MS + 1,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert_eq!(expired.pending.len(), 2);
        assert!(expired.pending.iter().all(|pending| {
            pending.fresh_send_reason
                == Some(AgentProgressDeliveryFreshSendReason::ExistingProviderMessageExpired)
        }));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn progress_delivery_duplicate_runtime_events_are_idempotent() {
        progress_surface_claim_reclaims_same_queue_orphan_without_ttl();
    }

    #[test]
    fn fresh_send_requires_enumerated_reason_in_receipt() {
        progress_surface_claim_reclaims_same_queue_orphan_without_ttl();
    }

    #[test]
    fn terminal_control_marker_suppresses_cached_nonterminal_ghost_queue() {
        let root = temp_root("terminal_control_marker_suppresses_cached_nonterminal_ghost_queue");
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        let context = context();
        append_agent_progress_event(
            &harness_home,
            &AgentProgressEvent::new(
                &context,
                AgentProgressKind::Todo,
                "todo",
                "planning stale ghost work",
                AgentProgressStatus::Started,
                1000,
            ),
        )
        .unwrap();

        let initial = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 2000,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert_eq!(initial.pending.len(), 2);
        for (index, pending) in initial.pending.into_iter().enumerate() {
            record_agent_progress_delivery(AgentProgressDeliveryRecordOptions {
                harness_home: harness_home.clone(),
                queue_id: pending.queue_id,
                platform: pending.platform,
                account_id: pending.account_id,
                channel_id: pending.channel_id,
                thread_id: pending.thread_id,
                user_id: pending.user_id,
                session_key: pending.session_key,
                message_kind: pending.message_kind,
                action: pending.action,
                status: AgentProgressDeliveryStatus::Delivered,
                provider_message_id: Some(format!("ghost-provider-{}", index + 1)),
                event_line: pending.event_line,
                text_hash: pending.text_hash,
                terminal: pending.terminal,
                policy_decision: Some("test".to_string()),
                error: None,
                now_ms: 2100,
            })
            .unwrap();
        }
        record_scoped_stop(ScopedStopOptions {
            harness_home: harness_home.clone(),
            target: ScopedStopTarget::QueueItem {
                queue_id: context.queue_id.clone(),
            },
            reason: "operator stopped stale ghost queue".to_string(),
            now_ms: 3000,
        })
        .unwrap();

        let close = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 4000,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();

        assert!(!close.pending.is_empty(), "{:#?}", close.pending);
        assert_eq!(close.pending.len(), 2, "{:#?}", close.pending);
        assert!(
            close.pending.iter().all(|pending| {
                pending.queue_id == context.queue_id
                    && pending.terminal
                    && pending.action == AgentProgressDeliveryAction::Edit
                    && pending.provider_message_id.is_some()
                    && pending.progress_suppressed_reason.as_deref()
                        == Some("terminal-control-present")
            }),
            "{:#?}",
            close.pending
        );
        for pending in close.pending {
            let receipt = record_agent_progress_delivery_with_context(
                AgentProgressDeliveryRecordOptions {
                    harness_home: harness_home.clone(),
                    queue_id: pending.queue_id.clone(),
                    platform: pending.platform.clone(),
                    account_id: pending.account_id.clone(),
                    channel_id: pending.channel_id.clone(),
                    thread_id: pending.thread_id.clone(),
                    user_id: pending.user_id.clone(),
                    session_key: pending.session_key.clone(),
                    message_kind: pending.message_kind,
                    action: pending.action,
                    status: AgentProgressDeliveryStatus::Delivered,
                    provider_message_id: pending.provider_message_id.clone(),
                    event_line: pending.event_line,
                    text_hash: pending.text_hash.clone(),
                    terminal: pending.terminal,
                    policy_decision: Some("test".to_string()),
                    error: None,
                    now_ms: 4100,
                },
                AgentProgressDeliveryRecordContext {
                    status_surface_key: pending.status_surface_key.clone(),
                    event_id: pending.event_id.clone(),
                    existing_provider_message_id: pending.provider_message_id.clone(),
                    decision: Some("edit".to_string()),
                    fresh_send_reason: pending.fresh_send_reason,
                    idempotency_hit: pending.idempotency_hit,
                    progress_suppressed_reason: pending.progress_suppressed_reason.clone(),
                },
            )
            .unwrap();
            assert_eq!(
                receipt.progress_suppressed_reason.as_deref(),
                Some("terminal-control-present")
            );
        }

        let next = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 5000,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert!(next.pending.is_empty(), "{:#?}", next.pending);
        let state: Value = serde_json::from_str(
            &fs::read_to_string(agent_progress_delivery_state_file(&harness_home)).unwrap(),
        )
        .unwrap();
        assert!(
            state["compactedEvents"].get(&context.queue_id).is_none(),
            "{state:#?}"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn delivery_plan_reuses_unchanged_terminal_receipt_index_and_observes_appends() {
        let root =
            temp_root("delivery_plan_reuses_unchanged_terminal_receipt_index_and_observes_appends");
        let harness_home = root.join(".agent-harness");
        let queue_dir = harness_home.join("state").join("runtime-queue");
        fs::create_dir_all(&queue_dir).unwrap();
        let context = context();
        append_agent_progress_event(
            &harness_home,
            &AgentProgressEvent::new(
                &context,
                AgentProgressKind::Todo,
                "todo",
                "planning progress delivery",
                AgentProgressStatus::Started,
                1000,
            ),
        )
        .unwrap();
        append_json_line(
            &queue_dir.join("run-once-receipts.jsonl"),
            &serde_json::json!({
                "queueId": "historical:completed",
                "status": "completed",
                "reason": "unrelated historical terminal receipt"
            }),
        )
        .unwrap();

        let initial = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 2000,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert_eq!(initial.pending.len(), 2);
        for (index, pending) in initial.pending.into_iter().enumerate() {
            record_agent_progress_delivery(AgentProgressDeliveryRecordOptions {
                harness_home: harness_home.clone(),
                queue_id: pending.queue_id,
                platform: pending.platform,
                account_id: pending.account_id,
                channel_id: pending.channel_id,
                thread_id: pending.thread_id,
                user_id: pending.user_id,
                session_key: pending.session_key,
                message_kind: pending.message_kind,
                action: pending.action,
                status: AgentProgressDeliveryStatus::Delivered,
                provider_message_id: Some(format!("progress-provider-{index}")),
                event_line: pending.event_line,
                text_hash: pending.text_hash,
                terminal: pending.terminal,
                policy_decision: Some("test".to_string()),
                error: None,
                now_ms: 2100,
            })
            .unwrap();
        }

        let index_file = queue_dir.join("queue-state-index.json");
        let modified_before_unchanged_plan = fs::metadata(&index_file).unwrap().modified().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(1_100));

        let unchanged = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 3000,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert!(unchanged.pending.is_empty(), "{:#?}", unchanged.pending);
        assert_eq!(
            fs::metadata(&index_file).unwrap().modified().unwrap(),
            modified_before_unchanged_plan,
            "unchanged receipt ledger must not rewrite its index"
        );

        append_json_line(
            &queue_dir.join("run-once-receipts.jsonl"),
            &serde_json::json!({
                "queueId": context.queue_id,
                "status": "completed",
                "reason": "new terminal receipt"
            }),
        )
        .unwrap();
        let appended = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 4000,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert_eq!(appended.pending.len(), 2, "{:#?}", appended.pending);
        assert!(appended.pending.iter().all(|pending| {
            pending.terminal
                && pending.action == AgentProgressDeliveryAction::Edit
                && pending.progress_suppressed_reason.as_deref() == Some("terminal-control-present")
        }));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn status_surface_key_matches_pending_receipt_and_claim_with_agent_id() {
        let root = temp_root("status_surface_key_matches_pending_receipt_and_claim_with_agent_id");
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        let mut context = context();
        context.agent_id = Some("xiaoxiaoli".to_string());
        append_agent_progress_event(
            &harness_home,
            &AgentProgressEvent::new(
                &context,
                AgentProgressKind::Todo,
                "todo",
                "planning 1 task(s)",
                AgentProgressStatus::Started,
                1000,
            ),
        )
        .unwrap();

        let plan = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 2000,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        let pending = plan
            .pending
            .iter()
            .find(|pending| pending.message_kind == AgentProgressDeliveryMessageKind::Status)
            .expect("status pending")
            .clone();
        assert!(pending.status_surface_key.contains("xiaoxiaoli"));

        let receipt = record_agent_progress_delivery_with_context(
            AgentProgressDeliveryRecordOptions {
                harness_home: harness_home.clone(),
                queue_id: pending.queue_id.clone(),
                platform: pending.platform.clone(),
                account_id: pending.account_id.clone(),
                channel_id: pending.channel_id.clone(),
                thread_id: pending.thread_id.clone(),
                user_id: pending.user_id.clone(),
                session_key: pending.session_key.clone(),
                message_kind: pending.message_kind,
                action: pending.action,
                status: AgentProgressDeliveryStatus::Delivered,
                provider_message_id: Some("provider-status-1".to_string()),
                event_line: pending.event_line,
                text_hash: pending.text_hash.clone(),
                terminal: pending.terminal,
                policy_decision: Some("allowed".to_string()),
                error: None,
                now_ms: 2100,
            },
            AgentProgressDeliveryRecordContext {
                status_surface_key: pending.status_surface_key.clone(),
                event_id: pending.event_id.clone(),
                existing_provider_message_id: pending.provider_message_id.clone(),
                decision: Some("send".to_string()),
                fresh_send_reason: pending.fresh_send_reason,
                idempotency_hit: pending.idempotency_hit,
                progress_suppressed_reason: pending.progress_suppressed_reason.clone(),
            },
        )
        .unwrap();
        assert_eq!(receipt.status_surface_key, pending.status_surface_key);
        assert_eq!(
            receipt.fresh_send_reason,
            Some(AgentProgressDeliveryFreshSendReason::NoExistingStatusSurface)
        );

        let claim = read_progress_surface_claim(&progress_surface_claim_file(
            &harness_home,
            &pending.status_surface_key,
        ))
        .unwrap()
        .expect("surface claim");
        assert_eq!(claim.agent_id.as_deref(), Some("xiaoxiaoli"));
        assert_eq!(
            claim.provider_message_id.as_deref(),
            Some("provider-status-1")
        );

        append_agent_progress_event(
            &harness_home,
            &AgentProgressEvent::new(
                &context,
                AgentProgressKind::ReadFile,
                "read",
                "reading progress.rs",
                AgentProgressStatus::Started,
                2200,
            ),
        )
        .unwrap();
        let next = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 3000,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        let edit = next
            .pending
            .iter()
            .find(|pending| pending.message_kind == AgentProgressDeliveryMessageKind::Status)
            .expect("status edit pending");
        assert_eq!(edit.action, AgentProgressDeliveryAction::Edit);
        assert_eq!(
            edit.provider_message_id.as_deref(),
            Some("provider-status-1")
        );
        assert_eq!(edit.status_surface_key, pending.status_surface_key);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn new_session_supersedes_previous_progress_surfaces() {
        let root = temp_root("new_session_supersedes_previous_progress_surfaces");
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        let context = context();
        append_agent_progress_event(
            &harness_home,
            &AgentProgressEvent::new(
                &context,
                AgentProgressKind::Todo,
                "todo",
                "planning 1 task(s)",
                AgentProgressStatus::Started,
                1000,
            ),
        )
        .unwrap();

        let initial = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 2000,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert_eq!(initial.pending.len(), 2);

        let report =
            supersede_agent_progress_session_surfaces(AgentProgressSessionSupersedeOptions {
                harness_home: harness_home.clone(),
                platform: context.platform.clone(),
                account_id: None,
                channel_id: context.channel_id.clone(),
                user_id: context.user_id.clone(),
                agent_id: context.agent_id.clone(),
                session_key: context.session_key.clone(),
                now_ms: 2500,
            })
            .unwrap();
        assert_eq!(report.removed_claim_files.len(), 2);
        assert!(report.receipts_file.is_file());

        append_agent_progress_event(
            &harness_home,
            &AgentProgressEvent::new(
                &context,
                AgentProgressKind::ReadFile,
                "read",
                "reading old session",
                AgentProgressStatus::Started,
                3000,
            ),
        )
        .unwrap();
        let old_session = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 4000,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert!(old_session.pending.is_empty());

        let mut new_context = context.clone();
        new_context.session_key = "telegram:dm:user:main:session-new".to_string();
        append_agent_progress_event(
            &harness_home,
            &AgentProgressEvent::new(
                &new_context,
                AgentProgressKind::Todo,
                "todo",
                "new session task",
                AgentProgressStatus::Started,
                5000,
            ),
        )
        .unwrap();
        let new_session = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 6000,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert_eq!(new_session.pending.len(), 2);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn new_session_hides_old_lane_ghost_status() {
        new_session_supersedes_previous_progress_surfaces();
    }

    #[test]
    fn canonical_progress_append_assigns_and_preserves_v2_event_ids() {
        let root = temp_root("canonical_progress_append_assigns_and_preserves_v2_event_ids");
        let harness_home = root.join(".agent-harness");
        let context = context();
        append_agent_progress_event(
            &harness_home,
            &AgentProgressEvent::new(
                &context,
                AgentProgressKind::Runtime,
                "runtime",
                "starting",
                AgentProgressStatus::Started,
                1_000,
            ),
        )
        .unwrap();
        let mut supplied = AgentProgressEvent::new(
            &context,
            AgentProgressKind::ToolCall,
            "tool_call",
            "preserve supplied id",
            AgentProgressStatus::Progress,
            1_001,
        );
        supplied.event_id = Some("progress-event-supplied-0001".to_string());
        append_agent_progress_event(&harness_home, &supplied).unwrap();

        let mut warnings = Vec::new();
        let events =
            read_progress_events(&agent_progress_events_file(&harness_home), &mut warnings)
                .unwrap();
        assert!(warnings.is_empty());
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event.schema, AGENT_PROGRESS_EVENT_SCHEMA);
        assert!(valid_progress_event_id(events[0].event.event_id.as_deref()).is_some());
        assert_eq!(
            events[1].event.event_id.as_deref(),
            Some("progress-event-supplied-0001")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn legacy_line_identity_remains_compatible_when_no_event_id_exists() {
        let root = temp_root("legacy_line_identity_remains_compatible_when_no_event_id_exists");
        let harness_home = root.join(".agent-harness");
        let mut legacy = AgentProgressEvent::new(
            &context(),
            AgentProgressKind::Runtime,
            "runtime",
            "legacy progress",
            AgentProgressStatus::Started,
            1_000,
        );
        legacy.schema = "agent-harness.progress-event.v1".to_string();
        legacy.event_id = None;
        append_json_line(&agent_progress_events_file(&harness_home), &legacy).unwrap();

        let plan = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 2_000,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        let pending = plan
            .pending
            .into_iter()
            .find(|pending| pending.message_kind == AgentProgressDeliveryMessageKind::Status)
            .expect("legacy status pending");
        assert_eq!(pending.event_line, 1);
        let receipt = record_pending_delivery(&harness_home, &pending, "legacy-provider", 2_100);
        assert_eq!(receipt.event_line, 1);
        assert!(receipt.event_id.is_none());
        let serialized = serde_json::to_value(&receipt).unwrap();
        assert!(serialized.get("eventId").is_none());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn terminal_event_id_prevents_resurrection_after_source_relocation() {
        let root = temp_root("terminal_event_id_prevents_resurrection_after_source_relocation");
        let harness_home = root.join(".agent-harness");
        let context = context();
        append_agent_progress_event(
            &harness_home,
            &AgentProgressEvent::new(
                &context,
                AgentProgressKind::Terminal,
                "terminal",
                "preparing final progress surface",
                AgentProgressStatus::Started,
                900,
            ),
        )
        .unwrap();
        append_agent_progress_event(
            &harness_home,
            &AgentProgressEvent::new(
                &context,
                AgentProgressKind::Runtime,
                "runtime",
                "completed",
                AgentProgressStatus::Completed,
                1_000,
            ),
        )
        .unwrap();

        let initial = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 2_000,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert_eq!(initial.pending.len(), 2);
        for (index, pending) in initial.pending.iter().enumerate() {
            let receipt = record_pending_delivery(
                &harness_home,
                pending,
                &format!("terminal-provider-{index}"),
                2_100 + index as i64,
            );
            assert!(
                valid_progress_event_id(receipt.event_id.as_deref()).is_some(),
                "canonical terminal deliveries carry a stable event identity"
            );
        }

        let events_file = agent_progress_events_file(&harness_home);
        let canonical_terminal = fs::read_to_string(&events_file).unwrap();
        let leading = AgentProgressEvent::new(
            &context,
            AgentProgressKind::AssistantNarration,
            "narration",
            "older progress relocated during compaction",
            AgentProgressStatus::Progress,
            900,
        );
        fs::write(
            &events_file,
            format!(
                "{}\n{}",
                serde_json::to_string(&leading).unwrap(),
                canonical_terminal
            ),
        )
        .unwrap();

        let relocated = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home: harness_home.clone(),
            platform: Some("telegram".to_string()),
            now_ms: 3_000,
            min_update_interval_ms: 0,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert!(
            relocated.pending.is_empty(),
            "the terminal surface must not reopen just because its event moved to a new JSONL line: {:#?}",
            relocated.pending
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn sensitive_preview_is_redacted() {
        let preview = sanitize_progress_preview("OPENAI_API_KEY=sk-abc cargo test", 120);
        assert_eq!(preview, SENSITIVE_PREVIEW);
        assert_eq!(
            sanitize_progress_preview("curl -H 'Authorization: Bearer secret-value'", 120),
            SENSITIVE_PREVIEW
        );
        assert_eq!(
            sanitize_progress_preview("gh api --header token ghp_1234567890", 120),
            SENSITIVE_PREVIEW
        );
        assert_eq!(
            sanitize_progress_preview("tool --api-key sk-test-value", 120),
            SENSITIVE_PREVIEW
        );
    }

    fn context() -> AgentProgressContext {
        AgentProgressContext {
            queue_id: "turn:1".to_string(),
            agent_id: Some("main".to_string()),
            account_id: Some("default".to_string()),
            thread_id: None,
            session_key: "telegram:dm:user:main".to_string(),
            platform: "telegram".to_string(),
            channel_id: "dm".to_string(),
            user_id: "user".to_string(),
        }
    }

    fn record_pending_delivery(
        harness_home: &Path,
        pending: &AgentProgressDeliveryPending,
        provider_message_id: &str,
        now_ms: i64,
    ) -> AgentProgressDeliveryReceipt {
        record_agent_progress_delivery_with_context(
            AgentProgressDeliveryRecordOptions {
                harness_home: harness_home.to_path_buf(),
                queue_id: pending.queue_id.clone(),
                platform: pending.platform.clone(),
                account_id: pending.account_id.clone(),
                channel_id: pending.channel_id.clone(),
                thread_id: pending.thread_id.clone(),
                user_id: pending.user_id.clone(),
                session_key: pending.session_key.clone(),
                message_kind: pending.message_kind,
                action: pending.action,
                status: AgentProgressDeliveryStatus::Delivered,
                provider_message_id: Some(provider_message_id.to_string()),
                event_line: pending.event_line,
                text_hash: pending.text_hash.clone(),
                terminal: pending.terminal,
                policy_decision: Some("test".to_string()),
                error: None,
                now_ms,
            },
            AgentProgressDeliveryRecordContext {
                status_surface_key: pending.status_surface_key.clone(),
                event_id: pending.event_id.clone(),
                existing_provider_message_id: pending.provider_message_id.clone(),
                decision: Some(
                    match pending.action {
                        AgentProgressDeliveryAction::Send => "send",
                        AgentProgressDeliveryAction::Edit => "edit",
                    }
                    .to_string(),
                ),
                fresh_send_reason: pending.fresh_send_reason,
                idempotency_hit: pending.idempotency_hit,
                progress_suppressed_reason: pending.progress_suppressed_reason.clone(),
            },
        )
        .unwrap()
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-progress-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
