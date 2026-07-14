use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};

use ring::digest;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::channel_state::{
    ChannelStateLane, channel_session_state_file, migrate_legacy_channel_session_state_to_v2,
    read_channel_session_state, read_channel_session_state_v2, write_channel_session_state_v2,
};
use crate::config::HARNESS_CONFIG_FILE_NAME;
use crate::lane::FullLaneKeyV1;
use crate::runtime_execution_receipt_index::has_prepared_execution_receipt_from_index;
use crate::{append_jsonl_value, write_json_atomic};

const CONTEXT_COMPACT_COUNTER_SCHEMA: &str = "agent-harness.context-compact-counter.v1";
const CONTEXT_COMPACT_COUNTER_V2_SCHEMA: &str = "agent-harness.context-compact-counter.v2";
const VIRTUAL_SESSION_SCHEMA: &str = "agent-harness.virtual-session.v1";
const VIRTUAL_SESSION_V2_SCHEMA: &str = "agent-harness.virtual-session.v2";
const WORKING_SET_MEMORY_SCHEMA: &str = "agent-harness.working-set-memory.v1";
const WORKING_SET_MEMORY_V2_SCHEMA: &str = "agent-harness.working-set-memory.v2";
const CONTEXT_ROLLOVER_RECEIPT_SCHEMA: &str = "agent-harness.context-rollover-receipt.v1";
const CONTEXT_ROLLOVER_RECEIPT_V2_SCHEMA: &str = "agent-harness.context-rollover-receipt.v2";
const WORKING_SET_SESSION_INDEX_SCHEMA: &str = "agent-harness.working-set-session-index.v1";
const WORKING_SET_SESSION_INDEX_V2_SCHEMA: &str = "agent-harness.working-set-session-index.v2";
const CONTEXT_ROLLOVER_EPISODE_SCHEMA: &str = "agent-harness.context-rollover-episode.v1";
const CONTEXT_ROLLOVER_EPISODE_V2_SCHEMA: &str = "agent-harness.context-rollover-episode.v2";
const CONTEXT_ROLLOVER_PREPARED_REQUEUE_SCHEMA: &str =
    "agent-harness.context-rollover-prepared-requeue.v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ContextRolloverMode {
    WorkingSetMemory,
    Disabled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextRolloverConfig {
    pub max_successful_compacts_before_rollover: u64,
    #[serde(default = "default_max_continuation_depth")]
    pub max_continuation_depth: u64,
    #[serde(default = "default_stream_unstable_continuation_min_attempts")]
    pub stream_unstable_continuation_min_attempts: usize,
    #[serde(default = "default_stream_unstable_continuation_token_limit")]
    pub stream_unstable_continuation_token_limit: u64,
    pub rollover_mode: ContextRolloverMode,
    pub cooperative_mid_turn_drain: bool,
}

impl Default for ContextRolloverConfig {
    fn default() -> Self {
        Self {
            max_successful_compacts_before_rollover: 2,
            max_continuation_depth: 2,
            stream_unstable_continuation_min_attempts: 2,
            stream_unstable_continuation_token_limit: 80_000,
            rollover_mode: ContextRolloverMode::WorkingSetMemory,
            cooperative_mid_turn_drain: false,
        }
    }
}

fn default_max_continuation_depth() -> u64 {
    ContextRolloverConfig::default().max_continuation_depth
}

fn default_stream_unstable_continuation_min_attempts() -> usize {
    ContextRolloverConfig::default().stream_unstable_continuation_min_attempts
}

fn default_stream_unstable_continuation_token_limit() -> u64 {
    ContextRolloverConfig::default().stream_unstable_continuation_token_limit
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeContinuationMetadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub virtual_session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation_index: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_terminal: Option<bool>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub suppress_self_improvement: bool,
}

impl RuntimeContinuationMetadata {
    pub fn legacy() -> Self {
        Self::default()
    }

    pub fn should_suppress_self_improvement(&self) -> bool {
        self.suppress_self_improvement
            || is_rollover_completion_kind(self.completion_kind.as_deref())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextRolloverLane {
    pub runtime_class: String,
    pub agent_id: String,
    pub platform: String,
    pub channel_id: String,
    pub user_id: String,
    pub working_session_key: String,
    pub virtual_session_id: Option<String>,
    pub continuation_index: u64,
}

impl ContextRolloverLane {
    pub fn lane_key(&self) -> String {
        [
            self.runtime_class.as_str(),
            self.agent_id.as_str(),
            self.platform.as_str(),
            self.channel_id.as_str(),
            self.user_id.as_str(),
            self.working_session_key.as_str(),
        ]
        .join(":")
    }
}

/// Exact account-aware rollover identity for new callers. The v1 lane type is
/// deliberately retained for accountless legacy artifacts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextRolloverLaneV2 {
    pub runtime_class: String,
    pub channel_lane: ChannelStateLane,
    pub working_session_key: String,
    pub virtual_session_id: Option<String>,
    pub continuation_index: u64,
}

impl ContextRolloverLaneV2 {
    pub fn lane_key(&self) -> String {
        format!(
            "v2:{}:{}:{}",
            channel_state_lane_storage_key(&self.channel_lane),
            safe_path_segment(&self.runtime_class),
            safe_path_segment(&self.working_session_key),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextCompactCounter {
    pub schema: String,
    pub lane_key: String,
    pub lane_hash: String,
    pub platform: String,
    pub channel_id: String,
    pub user_id: String,
    pub agent_id: String,
    pub working_session_key: String,
    pub virtual_session_id: Option<String>,
    pub continuation_index: u64,
    pub successful_compact_count: u64,
    pub rollover_pending: bool,
    pub last_compact_thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_successful_compact_attempt_key: Option<String>,
    pub last_rollover_receipt: Option<PathBuf>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextCompactCounterOptions {
    pub harness_home: PathBuf,
    pub lane: ContextRolloverLane,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextCompactAttemptOptions {
    pub harness_home: PathBuf,
    pub lane: ContextRolloverLane,
    pub compact_succeeded: bool,
    pub rewrote_active_context: bool,
    pub compact_thread_id: Option<String>,
    pub compact_attempt_key: Option<String>,
    pub max_successful_compacts_before_rollover: u64,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextCompactCounterV2Options {
    pub harness_home: PathBuf,
    pub lane: ContextRolloverLaneV2,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextCompactAttemptV2Options {
    pub harness_home: PathBuf,
    pub lane: ContextRolloverLaneV2,
    pub compact_succeeded: bool,
    pub rewrote_active_context: bool,
    pub compact_thread_id: Option<String>,
    pub compact_attempt_key: Option<String>,
    pub max_successful_compacts_before_rollover: u64,
    pub now_ms: i64,
}

/// Exact-lane rollover input. Unlike the v1 options this cannot be created
/// without a normalized provider/account/channel/user/agent boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextRolloverBeforeTurnV2Options {
    pub harness_home: PathBuf,
    pub queue_id: String,
    pub runtime_class: String,
    pub channel_lane: ChannelStateLane,
    pub working_session_key: String,
    pub now_ms: i64,
}

/// Exact-lane completed-turn snapshot input. v1 remains available for legacy
/// callers, while new account-aware callers use this type and never resolve an
/// accountless working-set index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletedTurnWorkingSetSnapshotV2Options {
    pub harness_home: PathBuf,
    pub channel_lane: ChannelStateLane,
    pub working_session_key: String,
    pub queue_id: Option<String>,
    pub message_text: Option<String>,
    pub status: String,
    pub run_once_receipt_file: Option<PathBuf>,
    pub outbox_file: Option<PathBuf>,
    pub completion_file: Option<PathBuf>,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ContextRolloverStatus {
    Applied,
    NotPending,
    NotFound,
    BlockedPrepared,
    BlockedLeased,
    BlockedChannelStateMissing,
    BlockedChannelStateMismatch,
    Disabled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextRolloverReceipt {
    pub schema: String,
    pub status: ContextRolloverStatus,
    pub queue_id: Option<String>,
    pub runtime_class: String,
    pub platform: String,
    pub channel_id: String,
    pub user_id: String,
    pub agent_id: String,
    pub virtual_session_id: Option<String>,
    pub previous_working_session_key: String,
    pub new_working_session_key: Option<String>,
    pub continuation_index: u64,
    pub working_set_file: Option<PathBuf>,
    pub virtual_session_file: Option<PathBuf>,
    pub receipt_file: PathBuf,
    pub reason: String,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextRolloverBeforeTurnOptions {
    pub harness_home: PathBuf,
    pub queue_id: String,
    pub runtime_class: String,
    pub agent_id: String,
    pub platform: String,
    pub channel_id: String,
    pub user_id: String,
    pub working_session_key: String,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextRolloverRequeuePreparedOptions {
    pub harness_home: PathBuf,
    pub queue_id: String,
    pub new_working_session_key: String,
    pub reason: String,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextRolloverPreparedRequeueReport {
    pub schema: String,
    pub queue_id: String,
    pub requeued_queue_id: String,
    pub previous_working_session_key: Option<String>,
    pub new_working_session_key: String,
    pub virtual_session_id: Option<String>,
    pub continuation_index: u64,
    pub pending_queue_file: PathBuf,
    pub run_once_receipts_file: PathBuf,
    pub prepared_execution_dir: Option<PathBuf>,
    pub report_file: PathBuf,
    pub requeued: bool,
    pub reason: String,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextWorkingSetMemory {
    pub schema: String,
    pub virtual_session_id: String,
    pub working_session_key: String,
    pub previous_working_session_key: Option<String>,
    pub continuation_index: u64,
    pub goal: ContextWorkingSetGoal,
    pub active_plan_refs: Vec<String>,
    pub pending_queue_item: Option<Value>,
    pub constraints: Vec<String>,
    pub decisions: Vec<String>,
    pub recent_files: Vec<String>,
    pub validation: Vec<String>,
    pub blockers: Vec<String>,
    pub static_record_refs: ContextStaticRecordRefs,
    pub agent_continuation_note: Option<String>,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextWorkingSetGoal {
    pub objective: Option<String>,
    pub status: String,
    pub budget_usage: Option<String>,
    pub completion_criteria: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextStaticRecordRefs {
    pub transcript_file: Option<PathBuf>,
    pub trajectory_file: Option<PathBuf>,
    pub codex_binding_file: Option<PathBuf>,
    pub prompt_bundle_json: Option<PathBuf>,
    pub runtime_receipts: Vec<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextVirtualSessionRecord {
    pub schema: String,
    pub virtual_session_id: String,
    pub platform: String,
    pub channel_id: String,
    pub user_id: String,
    pub agent_id: String,
    pub status: String,
    pub root_session_key: String,
    pub active_working_session_key: String,
    pub continuation_index: u64,
    pub working_sessions: Vec<ContextWorkingSessionRef>,
    pub active_goal_ref: Option<String>,
    pub working_set_file: PathBuf,
    pub episode_index_file: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextWorkingSessionRef {
    pub session_key: String,
    pub continuation_index: u64,
    pub codex_thread_id: Option<String>,
    pub started_at_ms: i64,
    pub ended_at_ms: Option<i64>,
    pub ended_by: Option<String>,
    pub working_set_file: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextRolloverEpisode {
    pub schema: String,
    pub virtual_session_id: String,
    pub queue_id: Option<String>,
    pub previous_working_session_key: String,
    pub new_working_session_key: String,
    pub continuation_index: u64,
    pub working_set_file: PathBuf,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WorkingSetSessionIndex {
    pub(crate) schema: String,
    pub(crate) session_key: String,
    pub(crate) virtual_session_id: String,
    pub(crate) continuation_index: u64,
    pub(crate) working_set_file: PathBuf,
    pub(crate) updated_at_ms: i64,
}

/// Durable v2 index metadata. The public legacy index intentionally remains
/// accountless for source compatibility; v2 storage has a separate schema and
/// requires every ChannelStateLane axis on every read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WorkingSetSessionIndexV2 {
    schema: String,
    platform: String,
    account_id: String,
    channel_id: String,
    user_id: String,
    agent_id: String,
    session_key: String,
    virtual_session_id: String,
    continuation_index: u64,
    working_set_file: PathBuf,
    updated_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletedTurnWorkingSetSnapshotOptions {
    pub harness_home: PathBuf,
    pub platform: String,
    pub channel_id: String,
    pub user_id: String,
    pub agent_id: String,
    pub working_session_key: String,
    pub queue_id: Option<String>,
    pub message_text: Option<String>,
    pub status: String,
    pub run_once_receipt_file: Option<PathBuf>,
    pub outbox_file: Option<PathBuf>,
    pub completion_file: Option<PathBuf>,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VirtualSessionTaskBoundaryCloseOptions {
    pub harness_home: PathBuf,
    pub previous_session_key: String,
    pub ended_by: String,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VirtualSessionThreadBackfillOptions {
    pub harness_home: PathBuf,
    pub session_key: String,
    pub thread_id: String,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VirtualSessionTerminalOptions {
    pub harness_home: PathBuf,
    pub session_key: String,
    pub ended_by: String,
    pub now_ms: i64,
}

/// Exact-lane lifecycle close input. Use this counterpart when a full channel
/// lane is available; the v1 type above remains an accountless legacy wrapper.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VirtualSessionTaskBoundaryCloseV2Options {
    pub harness_home: PathBuf,
    pub channel_lane: ChannelStateLane,
    pub previous_session_key: String,
    pub ended_by: String,
    pub now_ms: i64,
}

/// Exact-lane Codex-thread backfill input. A caller must pass the same lane
/// that created the snapshot; no session-only fallback is performed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VirtualSessionThreadBackfillV2Options {
    pub harness_home: PathBuf,
    pub channel_lane: ChannelStateLane,
    pub session_key: String,
    pub thread_id: String,
    pub now_ms: i64,
}

/// Exact-lane terminal-state input. This is the v2 counterpart for runtime
/// pipeline failure/terminal handling and keeps records account-isolated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VirtualSessionTerminalV2Options {
    pub harness_home: PathBuf,
    pub channel_lane: ChannelStateLane,
    pub session_key: String,
    pub ended_by: String,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CompletedTurnWorkingSetSnapshotReport {
    pub schema: String,
    pub virtual_session_id: String,
    pub working_session_key: String,
    pub continuation_index: u64,
    pub working_set_file: PathBuf,
    pub virtual_session_file: PathBuf,
}

pub fn load_context_rollover_config(harness_home: &Path) -> io::Result<ContextRolloverConfig> {
    let mut config = ContextRolloverConfig::default();
    for path in [
        harness_home.join(HARNESS_CONFIG_FILE_NAME),
        harness_home.join("config").join(HARNESS_CONFIG_FILE_NAME),
    ] {
        if !path.is_file() {
            continue;
        }
        let text = fs::read_to_string(&path)?;
        let value = serde_json::from_str::<Value>(&text).map_err(io::Error::other)?;
        let Some(context) = value
            .get("codexContext")
            .or_else(|| value.get("codex_context"))
            .and_then(Value::as_object)
        else {
            break;
        };
        if let Some(value) = json_u64(
            context,
            &[
                "maxSuccessfulCompactsBeforeRollover",
                "max_successful_compacts_before_rollover",
            ],
        )
        .filter(|value| *value > 0)
        {
            config.max_successful_compacts_before_rollover = value;
        }
        if let Some(value) = json_u64(context, &["maxContinuationDepth", "max_continuation_depth"])
            .filter(|value| *value > 0)
        {
            config.max_continuation_depth = value;
        }
        if let Some(value) = json_u64(
            context,
            &[
                "streamUnstableContinuationMinAttempts",
                "stream_unstable_continuation_min_attempts",
            ],
        )
        .filter(|value| *value > 0)
        {
            config.stream_unstable_continuation_min_attempts =
                value.min(usize::MAX as u64) as usize;
        }
        if let Some(value) = json_u64(
            context,
            &[
                "streamUnstableContinuationTokenLimit",
                "stream_unstable_continuation_token_limit",
            ],
        )
        .filter(|value| *value > 0)
        {
            config.stream_unstable_continuation_token_limit = value;
        }
        if let Some(value) = json_string(context, &["rolloverMode", "rollover_mode"]) {
            config.rollover_mode = parse_rollover_mode(&value);
        }
        if let Some(value) = json_bool(
            context,
            &["cooperativeMidTurnDrain", "cooperative_mid_turn_drain"],
        ) {
            config.cooperative_mid_turn_drain = value;
        }
        break;
    }
    Ok(config)
}

pub fn parse_rollover_mode(value: &str) -> ContextRolloverMode {
    match value.trim().to_ascii_lowercase().replace('_', "-").as_str() {
        "disabled" => ContextRolloverMode::Disabled,
        _ => ContextRolloverMode::WorkingSetMemory,
    }
}

pub fn context_compact_counter_file(harness_home: impl AsRef<Path>, lane_key: &str) -> PathBuf {
    let lane_hash = fnv1a_64_hex(lane_key);
    harness_home
        .as_ref()
        .join("state")
        .join("context-rollover")
        .join("compact-counters")
        .join(format!("{lane_hash}.json"))
}

pub fn load_or_create_context_compact_counter(
    options: ContextCompactCounterOptions,
) -> io::Result<ContextCompactCounter> {
    let lane_key = options.lane.lane_key();
    let path = context_compact_counter_file(&options.harness_home, &lane_key);
    if path.is_file() {
        let text = fs::read_to_string(&path)?;
        let mut counter =
            serde_json::from_str::<ContextCompactCounter>(&text).map_err(io::Error::other)?;
        if counter.schema.is_empty() {
            counter.schema = CONTEXT_COMPACT_COUNTER_SCHEMA.to_string();
        }
        return Ok(counter);
    }
    let counter = new_context_compact_counter(&options.lane, &lane_key, options.now_ms);
    write_json_atomic(&path, &counter)?;
    Ok(counter)
}

pub fn record_context_compact_attempt(
    options: ContextCompactAttemptOptions,
) -> io::Result<ContextCompactCounter> {
    let lane_key = options.lane.lane_key();
    let path = context_compact_counter_file(&options.harness_home, &lane_key);
    let mut counter = load_or_create_context_compact_counter(ContextCompactCounterOptions {
        harness_home: options.harness_home.clone(),
        lane: options.lane,
        now_ms: options.now_ms,
    })?;
    let duplicate_attempt = options.compact_attempt_key.is_some()
        && options.compact_attempt_key.as_deref()
            == counter.last_successful_compact_attempt_key.as_deref();
    if options.compact_succeeded && options.rewrote_active_context && !duplicate_attempt {
        counter.successful_compact_count = counter.successful_compact_count.saturating_add(1);
        counter.last_compact_thread_id = options.compact_thread_id;
        counter.last_successful_compact_attempt_key = options.compact_attempt_key;
    }
    if counter.successful_compact_count >= options.max_successful_compacts_before_rollover {
        counter.rollover_pending = true;
    }
    counter.updated_at_ms = options.now_ms;
    write_json_atomic(&path, &counter)?;
    Ok(counter)
}

/// Deterministic v2 compact-counter path. It is keyed by the complete
/// ChannelStateLane, so two provider accounts can never share a counter merely
/// because their channel/user ids are alike.
pub fn context_compact_counter_v2_file(
    harness_home: impl AsRef<Path>,
    lane: &ContextRolloverLaneV2,
) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("context-rollover")
        .join("compact-counters-v2")
        .join(format!(
            "{}.json",
            sha256_hex(&canonical_identity_bytes(&[
                "context-compact-counter-v2",
                &lane.lane_key(),
            ]))
        ))
}

/// Reads only the exact v2 counter. A malformed or mismatched v2 file is an
/// error, not an opportunity to fall back to an accountless counter.
pub fn load_or_create_context_compact_counter_for_lane(
    options: ContextCompactCounterV2Options,
) -> io::Result<ContextCompactCounter> {
    let lane_key = options.lane.lane_key();
    let path = context_compact_counter_v2_file(&options.harness_home, &options.lane);
    if path.is_file() {
        let value = read_json_value(&path)?;
        let counter = serde_json::from_value::<ContextCompactCounter>(value.clone())
            .map_err(io::Error::other)?;
        if !counter_matches_lane_v2(&counter, &value, &options.lane, &lane_key) {
            return Err(invalid_v2_artifact(&path, "compact counter lane mismatch"));
        }
        return Ok(counter);
    }
    let counter = new_context_compact_counter_v2(&options.lane, &lane_key, options.now_ms);
    write_context_compact_counter_v2(&path, &counter, &options.lane)?;
    Ok(counter)
}

pub fn record_context_compact_attempt_for_lane(
    options: ContextCompactAttemptV2Options,
) -> io::Result<ContextCompactCounter> {
    let lane_key = options.lane.lane_key();
    let path = context_compact_counter_v2_file(&options.harness_home, &options.lane);
    let mut counter =
        load_or_create_context_compact_counter_for_lane(ContextCompactCounterV2Options {
            harness_home: options.harness_home,
            lane: options.lane.clone(),
            now_ms: options.now_ms,
        })?;
    let duplicate_attempt = options.compact_attempt_key.is_some()
        && options.compact_attempt_key.as_deref()
            == counter.last_successful_compact_attempt_key.as_deref();
    if options.compact_succeeded && options.rewrote_active_context && !duplicate_attempt {
        counter.successful_compact_count = counter.successful_compact_count.saturating_add(1);
        counter.last_compact_thread_id = options.compact_thread_id;
        counter.last_successful_compact_attempt_key = options.compact_attempt_key;
    }
    if counter.successful_compact_count >= options.max_successful_compacts_before_rollover {
        counter.rollover_pending = true;
    }
    counter.updated_at_ms = options.now_ms;
    counter.lane_key = lane_key;
    write_context_compact_counter_v2(&path, &counter, &options.lane)?;
    Ok(counter)
}

pub fn apply_context_rollover_before_turn(
    options: ContextRolloverBeforeTurnOptions,
) -> io::Result<ContextRolloverReceipt> {
    let config = load_context_rollover_config(&options.harness_home)?;
    if config.rollover_mode == ContextRolloverMode::Disabled {
        return write_rollover_receipt(ContextRolloverReceipt {
            schema: CONTEXT_ROLLOVER_RECEIPT_SCHEMA.to_string(),
            status: ContextRolloverStatus::Disabled,
            queue_id: Some(options.queue_id),
            runtime_class: options.runtime_class,
            platform: options.platform,
            channel_id: options.channel_id,
            user_id: options.user_id,
            agent_id: options.agent_id,
            virtual_session_id: None,
            previous_working_session_key: options.working_session_key,
            new_working_session_key: None,
            continuation_index: 0,
            working_set_file: None,
            virtual_session_file: None,
            receipt_file: context_rollover_receipts_file(&options.harness_home),
            reason: "context rollover mode disabled".to_string(),
            created_at_ms: options.now_ms,
        });
    }

    let lane = ContextRolloverLane {
        runtime_class: options.runtime_class.clone(),
        agent_id: options.agent_id.clone(),
        platform: options.platform.clone(),
        channel_id: options.channel_id.clone(),
        user_id: options.user_id.clone(),
        working_session_key: options.working_session_key.clone(),
        virtual_session_id: None,
        continuation_index: 0,
    };
    let counter = load_or_create_context_compact_counter(ContextCompactCounterOptions {
        harness_home: options.harness_home.clone(),
        lane: lane.clone(),
        now_ms: options.now_ms,
    })?;
    if !counter.rollover_pending
        && counter.successful_compact_count < config.max_successful_compacts_before_rollover
    {
        return write_rollover_receipt(ContextRolloverReceipt {
            schema: CONTEXT_ROLLOVER_RECEIPT_SCHEMA.to_string(),
            status: ContextRolloverStatus::NotPending,
            queue_id: Some(options.queue_id),
            runtime_class: options.runtime_class,
            platform: options.platform,
            channel_id: options.channel_id,
            user_id: options.user_id,
            agent_id: options.agent_id,
            virtual_session_id: counter.virtual_session_id,
            previous_working_session_key: options.working_session_key,
            new_working_session_key: None,
            continuation_index: counter.continuation_index,
            working_set_file: None,
            virtual_session_file: None,
            receipt_file: context_rollover_receipts_file(&options.harness_home),
            reason: "compact counter is below rollover threshold".to_string(),
            created_at_ms: options.now_ms,
        });
    }

    let continuation_index = counter.continuation_index.saturating_add(1);
    let root_session_key = root_working_session_key(&options.working_session_key);
    let new_working_session_key = continuation_session_key(&root_session_key, continuation_index);
    let virtual_session_id = counter.virtual_session_id.clone().unwrap_or_else(|| {
        derive_virtual_session_id(
            &options.platform,
            &options.channel_id,
            &options.user_id,
            &options.agent_id,
            &root_session_key,
        )
    });
    let queue_dir = options.harness_home.join("state").join("runtime-queue");
    let queue_file = queue_dir.join("pending.jsonl");

    if queue_item_has_prepared_receipt(&queue_dir, &options.queue_id)? {
        return write_rollover_receipt(blocked_receipt(
            &options,
            ContextRolloverStatus::BlockedPrepared,
            Some(virtual_session_id),
            continuation_index,
            "pending queue item already has a prepared execution receipt",
        ));
    }
    if queue_item_is_leased(&queue_dir, &options.runtime_class, &options.queue_id)? {
        return write_rollover_receipt(blocked_receipt(
            &options,
            ContextRolloverStatus::BlockedLeased,
            Some(virtual_session_id),
            continuation_index,
            "pending queue item is currently leased",
        ));
    }

    let state = read_channel_session_state(
        &options.harness_home,
        &options.platform,
        &options.channel_id,
        &options.user_id,
    )?;
    let Some(mut state) = state else {
        return write_rollover_receipt(blocked_receipt(
            &options,
            ContextRolloverStatus::BlockedChannelStateMissing,
            Some(virtual_session_id),
            continuation_index,
            "channel session state is missing; refusing rollover",
        ));
    };
    if state.active_session_key != options.working_session_key {
        return write_rollover_receipt(blocked_receipt(
            &options,
            ContextRolloverStatus::BlockedChannelStateMismatch,
            Some(virtual_session_id),
            continuation_index,
            "channel active session did not match the pending item session",
        ));
    }

    let (planned_transcript_file, planned_trajectory_file) = planned_session_files(
        &options.harness_home,
        &options.agent_id,
        &new_working_session_key,
    );
    if find_pending_queue_item(&queue_file, &options.queue_id)?.is_none() {
        return write_rollover_receipt(ContextRolloverReceipt {
            schema: CONTEXT_ROLLOVER_RECEIPT_SCHEMA.to_string(),
            status: ContextRolloverStatus::NotFound,
            queue_id: Some(options.queue_id),
            runtime_class: options.runtime_class,
            platform: options.platform,
            channel_id: options.channel_id,
            user_id: options.user_id,
            agent_id: options.agent_id,
            virtual_session_id: Some(virtual_session_id),
            previous_working_session_key: options.working_session_key,
            new_working_session_key: Some(new_working_session_key),
            continuation_index,
            working_set_file: None,
            virtual_session_file: None,
            receipt_file: context_rollover_receipts_file(&options.harness_home),
            reason: "pending queue item was not found".to_string(),
            created_at_ms: options.now_ms,
        });
    }

    let previous_state = state.clone();
    state.active_session_key = new_working_session_key.clone();
    state.updated_at_ms = options.now_ms;
    let state_file = channel_session_state_file(
        &options.harness_home,
        &options.platform,
        &options.channel_id,
        &options.user_id,
    );
    let original_queue_text = fs::read_to_string(&queue_file)?;
    write_json_atomic(&state_file, &state)?;

    let rewrite = match rewrite_pending_queue_item_session(
        &queue_file,
        &options.queue_id,
        &new_working_session_key,
        &virtual_session_id,
        continuation_index,
        &planned_transcript_file,
        &planned_trajectory_file,
    ) {
        Ok(rewrite) => rewrite,
        Err(error) => {
            let _ = write_json_atomic(&state_file, &previous_state);
            let _ = write_text_atomic(&queue_file, &original_queue_text);
            return Err(error);
        }
    };
    let Some(updated_queue_item) = rewrite else {
        write_json_atomic(&state_file, &previous_state)?;
        write_text_atomic(&queue_file, &original_queue_text)?;
        return write_rollover_receipt(ContextRolloverReceipt {
            schema: CONTEXT_ROLLOVER_RECEIPT_SCHEMA.to_string(),
            status: ContextRolloverStatus::NotFound,
            queue_id: Some(options.queue_id),
            runtime_class: options.runtime_class,
            platform: options.platform,
            channel_id: options.channel_id,
            user_id: options.user_id,
            agent_id: options.agent_id,
            virtual_session_id: Some(virtual_session_id),
            previous_working_session_key: options.working_session_key,
            new_working_session_key: Some(new_working_session_key),
            continuation_index,
            working_set_file: None,
            virtual_session_file: None,
            receipt_file: context_rollover_receipts_file(&options.harness_home),
            reason: "pending queue item was not found".to_string(),
            created_at_ms: options.now_ms,
        });
    };

    let post_state_result = (|| -> io::Result<(PathBuf, PathBuf)> {
        let working_set_file = write_working_set_memory(
            &options.harness_home,
            &options.agent_id,
            &virtual_session_id,
            &new_working_session_key,
            Some(&options.working_session_key),
            continuation_index,
            Some(updated_queue_item),
            options.now_ms,
            WorkingSetWriteOptions {
                append_decision: Some(
                    "context rollover re-keyed an unprepared pending queue item".to_string(),
                ),
                inherit_parent: true,
                ..WorkingSetWriteOptions::default()
            },
        )?;
        let virtual_session_file = write_virtual_session_record(
            &options.harness_home,
            &virtual_session_id,
            &options.platform,
            &options.channel_id,
            &options.user_id,
            &options.agent_id,
            &root_session_key,
            &new_working_session_key,
            continuation_index,
            &working_set_file,
            options.now_ms,
        )?;
        write_context_rollover_episode(
            &options.harness_home,
            &virtual_session_id,
            Some(&options.queue_id),
            &options.working_session_key,
            &new_working_session_key,
            continuation_index,
            &working_set_file,
            options.now_ms,
        )?;
        write_working_set_session_index(
            &options.harness_home,
            &new_working_session_key,
            &virtual_session_id,
            continuation_index,
            &working_set_file,
            options.now_ms,
        )?;
        let new_lane = ContextRolloverLane {
            runtime_class: options.runtime_class.clone(),
            agent_id: options.agent_id.clone(),
            platform: options.platform.clone(),
            channel_id: options.channel_id.clone(),
            user_id: options.user_id.clone(),
            working_session_key: new_working_session_key.clone(),
            virtual_session_id: Some(virtual_session_id.clone()),
            continuation_index,
        };
        let new_counter =
            new_context_compact_counter(&new_lane, &new_lane.lane_key(), options.now_ms);
        write_json_atomic(
            &context_compact_counter_file(&options.harness_home, &new_lane.lane_key()),
            &new_counter,
        )?;
        Ok((working_set_file, virtual_session_file))
    })();
    let (working_set_file, virtual_session_file) = match post_state_result {
        Ok(files) => files,
        Err(error) => {
            let _ = write_json_atomic(&state_file, &previous_state);
            let _ = write_text_atomic(&queue_file, &original_queue_text);
            return Err(error);
        }
    };

    write_rollover_receipt(ContextRolloverReceipt {
        schema: CONTEXT_ROLLOVER_RECEIPT_SCHEMA.to_string(),
        status: ContextRolloverStatus::Applied,
        queue_id: Some(options.queue_id),
        runtime_class: options.runtime_class,
        platform: options.platform,
        channel_id: options.channel_id,
        user_id: options.user_id,
        agent_id: options.agent_id,
        virtual_session_id: Some(virtual_session_id),
        previous_working_session_key: options.working_session_key,
        new_working_session_key: Some(new_working_session_key),
        continuation_index,
        working_set_file: Some(working_set_file),
        virtual_session_file: Some(virtual_session_file),
        receipt_file: context_rollover_receipts_file(&options.harness_home),
        reason: "context rollover re-keyed an unprepared pending queue item".to_string(),
        created_at_ms: options.now_ms,
    })
}

pub fn load_working_set_continuity_section(
    harness_home: &Path,
    session_key: &str,
) -> io::Result<Option<String>> {
    let Some((index, working_set)) =
        read_working_set_memory_for_session(harness_home, session_key)?
    else {
        return Ok(None);
    };
    Ok(Some(render_working_set_continuity(
        &index.working_set_file,
        &working_set,
    )))
}

pub(crate) fn read_working_set_memory_for_session(
    harness_home: &Path,
    session_key: &str,
) -> io::Result<Option<(WorkingSetSessionIndex, ContextWorkingSetMemory)>> {
    let index_file = working_set_session_index_file(harness_home, session_key);
    if !index_file.is_file() {
        return Ok(None);
    }
    let index_text = fs::read_to_string(&index_file)?;
    let index =
        serde_json::from_str::<WorkingSetSessionIndex>(&index_text).map_err(io::Error::other)?;
    let working_set_text = fs::read_to_string(&index.working_set_file)?;
    let working_set = serde_json::from_str::<ContextWorkingSetMemory>(&working_set_text)
        .map_err(io::Error::other)?;
    Ok(Some((index, working_set)))
}

/// Reads only the account-aware v2 working-set index for `lane`. It never
/// scans or falls back to v1 artifacts: callers that want a deliberately
/// checked default-account migration must invoke the migration function below.
pub(crate) fn read_working_set_memory_for_session_for_lane(
    harness_home: &Path,
    lane: &ChannelStateLane,
    session_key: &str,
) -> io::Result<Option<(WorkingSetSessionIndex, ContextWorkingSetMemory)>> {
    let index_file = working_set_session_index_v2_file(harness_home, lane, session_key);
    if !index_file.is_file() {
        return Ok(None);
    }
    let index_value = read_json_value(&index_file)?;
    let index = serde_json::from_value::<WorkingSetSessionIndexV2>(index_value.clone())
        .map_err(io::Error::other)?;
    if !working_set_index_matches_lane_v2(&index, lane, session_key) {
        return Err(invalid_v2_artifact(
            &index_file,
            "working-set index lane mismatch",
        ));
    }

    let memory_value = read_json_value(&index.working_set_file)?;
    let memory = serde_json::from_value::<ContextWorkingSetMemory>(memory_value.clone())
        .map_err(io::Error::other)?;
    if !working_set_memory_matches_lane_v2(
        &memory,
        &memory_value,
        lane,
        &index.virtual_session_id,
        session_key,
        index.continuation_index,
    ) {
        return Err(invalid_v2_artifact(
            &index.working_set_file,
            "working-set memory lane mismatch",
        ));
    }

    let Some((_record_file, record)) =
        read_virtual_session_record_for_lane(harness_home, lane, &index.virtual_session_id)?
    else {
        return Err(invalid_v2_artifact(
            &virtual_session_file(harness_home, &index.virtual_session_id),
            "working-set index has no matching virtual-session record",
        ));
    };
    if record.working_set_file != index.working_set_file
        || record.virtual_session_id != index.virtual_session_id
        || record.root_session_key != root_working_session_key(session_key)
    {
        return Err(invalid_v2_artifact(
            &virtual_session_file(harness_home, &index.virtual_session_id),
            "virtual-session record does not match working-set index",
        ));
    }

    Ok(Some((
        WorkingSetSessionIndex {
            schema: index.schema,
            session_key: index.session_key,
            virtual_session_id: index.virtual_session_id,
            continuation_index: index.continuation_index,
            working_set_file: index.working_set_file,
            updated_at_ms: index.updated_at_ms,
        },
        memory,
    )))
}

/// Performs the only supported v1-to-v2 working-set migration. It is limited
/// to the explicit default account and requires the legacy virtual record to
/// name exactly the same platform/channel/user/agent lane. Missing identity is
/// denial, never a wildcard match.
pub(crate) fn migrate_legacy_working_set_memory_for_session_for_lane(
    harness_home: &Path,
    lane: &ChannelStateLane,
    session_key: &str,
) -> io::Result<Option<(WorkingSetSessionIndex, ContextWorkingSetMemory)>> {
    if lane.account_id() != "default" {
        return Ok(None);
    }
    if let Some(existing) =
        read_working_set_memory_for_session_for_lane(harness_home, lane, session_key)?
    {
        return Ok(Some(existing));
    }
    let Some((legacy_index, legacy_memory)) =
        read_working_set_memory_for_session(harness_home, session_key)?
    else {
        return Ok(None);
    };
    if legacy_index.schema != WORKING_SET_SESSION_INDEX_SCHEMA
        || legacy_memory.schema != WORKING_SET_MEMORY_SCHEMA
        || legacy_index.session_key != session_key
        || legacy_memory.working_session_key != session_key
        || legacy_memory.virtual_session_id != legacy_index.virtual_session_id
    {
        return Ok(None);
    }
    let legacy_record_file = virtual_session_file(harness_home, &legacy_index.virtual_session_id);
    if !legacy_record_file.is_file() {
        return Ok(None);
    }
    let legacy_record_value = read_json_value(&legacy_record_file)?;
    let mut legacy_record =
        serde_json::from_value::<ContextVirtualSessionRecord>(legacy_record_value.clone())
            .map_err(io::Error::other)?;
    if legacy_record.schema != VIRTUAL_SESSION_SCHEMA
        || !legacy_virtual_record_matches_default_lane(&legacy_record_value, &legacy_record, lane)
        || legacy_record.virtual_session_id != legacy_index.virtual_session_id
        || legacy_record.working_set_file != legacy_index.working_set_file
        || legacy_record.root_session_key != root_working_session_key(session_key)
    {
        return Ok(None);
    }

    let virtual_session_id = derive_virtual_session_id_v2(lane, &legacy_record.root_session_key);
    let working_set_file = working_set_file(
        harness_home,
        &virtual_session_id,
        legacy_index.continuation_index,
    );
    let mut memory = legacy_memory;
    memory.schema = WORKING_SET_MEMORY_V2_SCHEMA.to_string();
    memory.virtual_session_id = virtual_session_id.clone();
    write_working_set_memory_v2_value(&working_set_file, &memory, lane)?;

    legacy_record.schema = VIRTUAL_SESSION_V2_SCHEMA.to_string();
    legacy_record.virtual_session_id = virtual_session_id.clone();
    legacy_record.working_set_file = working_set_file.clone();
    for session in &mut legacy_record.working_sessions {
        if session.session_key == session_key {
            session.working_set_file = Some(working_set_file.clone());
        }
    }
    write_virtual_session_record_v2_value(harness_home, &legacy_record, lane)?;
    write_working_set_session_index_v2(
        harness_home,
        lane,
        session_key,
        &virtual_session_id,
        legacy_index.continuation_index,
        &working_set_file,
        legacy_index.updated_at_ms,
    )?;
    read_working_set_memory_for_session_for_lane(harness_home, lane, session_key)
}

/// Reads a v2 virtual-session record and validates every lane axis before it
/// is returned. A same-id record with a mismatched account or agent is treated
/// as corrupt/foreign state rather than an eligible fallback.
pub(crate) fn read_virtual_session_record_for_lane(
    harness_home: &Path,
    lane: &ChannelStateLane,
    virtual_session_id: &str,
) -> io::Result<Option<(PathBuf, ContextVirtualSessionRecord)>> {
    let file = virtual_session_file(harness_home, virtual_session_id);
    if !file.is_file() {
        return Ok(None);
    }
    let value = read_json_value(&file)?;
    let record = serde_json::from_value::<ContextVirtualSessionRecord>(value.clone())
        .map_err(io::Error::other)?;
    if record.schema != VIRTUAL_SESSION_V2_SCHEMA
        || record.virtual_session_id != virtual_session_id
        || !v2_value_matches_lane(&value, lane)
    {
        return Err(invalid_v2_artifact(
            &file,
            "virtual-session record lane mismatch",
        ));
    }
    Ok(Some((file, record)))
}

fn working_set_index_for_session_or_root_for_lane(
    harness_home: &Path,
    lane: &ChannelStateLane,
    session_key: &str,
) -> io::Result<Option<WorkingSetSessionIndex>> {
    let direct = read_working_set_memory_for_session_for_lane(harness_home, lane, session_key)?;
    if let Some((index, _memory)) = direct {
        return Ok(Some(index));
    }
    if let Some((index, _memory)) =
        migrate_legacy_working_set_memory_for_session_for_lane(harness_home, lane, session_key)?
    {
        return Ok(Some(index));
    }
    let root_session_key = root_working_session_key(session_key);
    if root_session_key == session_key {
        return Ok(None);
    }
    let root = read_working_set_memory_for_session_for_lane(harness_home, lane, &root_session_key)?;
    if let Some((index, _memory)) = root {
        return Ok(Some(index));
    }
    migrate_legacy_working_set_memory_for_session_for_lane(harness_home, lane, &root_session_key)
        .map(|maybe| maybe.map(|(index, _memory)| index))
}

fn working_set_record_for_session_or_root_for_lane(
    harness_home: &Path,
    lane: &ChannelStateLane,
    session_key: &str,
) -> io::Result<Option<(WorkingSetSessionIndex, ContextVirtualSessionRecord)>> {
    let Some(index) =
        working_set_index_for_session_or_root_for_lane(harness_home, lane, session_key)?
    else {
        return Ok(None);
    };
    let Some((_file, record)) =
        read_virtual_session_record_for_lane(harness_home, lane, &index.virtual_session_id)?
    else {
        return Err(invalid_v2_artifact(
            &virtual_session_file(harness_home, &index.virtual_session_id),
            "working-set index has no matching virtual-session record",
        ));
    };
    if record.working_set_file != index.working_set_file {
        return Err(invalid_v2_artifact(
            &virtual_session_file(harness_home, &index.virtual_session_id),
            "virtual-session record does not match working-set index",
        ));
    }
    Ok(Some((index, record)))
}

pub fn close_virtual_session_for_task_boundary(
    options: VirtualSessionTaskBoundaryCloseOptions,
) -> io::Result<Option<PathBuf>> {
    let Some((file, mut record, _index)) = read_virtual_session_record_for_session(
        &options.harness_home,
        &options.previous_session_key,
    )?
    else {
        return Ok(None);
    };
    record.status = "closed".to_string();
    close_open_working_sessions(&mut record, &options.ended_by, options.now_ms);
    write_json_atomic(&file, &record)?;
    Ok(Some(file))
}

pub fn backfill_virtual_session_codex_thread_id(
    options: VirtualSessionThreadBackfillOptions,
) -> io::Result<Option<PathBuf>> {
    let thread_id = options.thread_id.trim();
    if thread_id.is_empty() {
        return Ok(None);
    }
    let Some((file, mut record, index)) =
        read_virtual_session_record_for_session(&options.harness_home, &options.session_key)?
    else {
        return Ok(None);
    };
    if let Some(session) = record
        .working_sessions
        .iter_mut()
        .find(|session| session.session_key == options.session_key)
    {
        session.codex_thread_id = Some(thread_id.to_string());
    } else {
        record.working_sessions.push(ContextWorkingSessionRef {
            session_key: options.session_key,
            continuation_index: index.continuation_index,
            codex_thread_id: Some(thread_id.to_string()),
            started_at_ms: options.now_ms,
            ended_at_ms: None,
            ended_by: None,
            working_set_file: Some(index.working_set_file),
        });
    }
    write_json_atomic(&file, &record)?;
    Ok(Some(file))
}

pub fn mark_virtual_session_terminal(
    options: VirtualSessionTerminalOptions,
) -> io::Result<Option<PathBuf>> {
    let Some((file, mut record, _index)) =
        read_virtual_session_record_for_session(&options.harness_home, &options.session_key)?
    else {
        return Ok(None);
    };
    record.status = "terminal-failed".to_string();
    close_open_working_sessions(&mut record, &options.ended_by, options.now_ms);
    write_json_atomic(&file, &record)?;
    Ok(Some(file))
}

/// Closes only the v2 virtual record addressed by this exact lane. A missing
/// v2 record is a no-op; a mismatched/corrupt record is returned as an error
/// and is never redirected to an accountless session record.
pub fn close_virtual_session_for_task_boundary_for_lane(
    options: VirtualSessionTaskBoundaryCloseV2Options,
) -> io::Result<Option<PathBuf>> {
    let Some((_index, mut record)) = working_set_record_for_session_or_root_for_lane(
        &options.harness_home,
        &options.channel_lane,
        &options.previous_session_key,
    )?
    else {
        return Ok(None);
    };
    record.status = "closed".to_string();
    close_open_working_sessions(&mut record, &options.ended_by, options.now_ms);
    write_virtual_session_record_v2_value(&options.harness_home, &record, &options.channel_lane)
        .map(Some)
}

/// Backfills a Codex thread id only into the exact v2 lane record. The thread
/// id is intentionally ignored when blank, preserving the v1 API behavior.
pub fn backfill_virtual_session_codex_thread_id_for_lane(
    options: VirtualSessionThreadBackfillV2Options,
) -> io::Result<Option<PathBuf>> {
    let thread_id = options.thread_id.trim();
    if thread_id.is_empty() {
        return Ok(None);
    }
    let Some((index, mut record)) = working_set_record_for_session_or_root_for_lane(
        &options.harness_home,
        &options.channel_lane,
        &options.session_key,
    )?
    else {
        return Ok(None);
    };
    if let Some(session) = record
        .working_sessions
        .iter_mut()
        .find(|session| session.session_key == options.session_key)
    {
        session.codex_thread_id = Some(thread_id.to_string());
    } else {
        record.working_sessions.push(ContextWorkingSessionRef {
            session_key: options.session_key,
            continuation_index: index.continuation_index,
            codex_thread_id: Some(thread_id.to_string()),
            started_at_ms: options.now_ms,
            ended_at_ms: None,
            ended_by: None,
            working_set_file: Some(index.working_set_file),
        });
    }
    write_virtual_session_record_v2_value(&options.harness_home, &record, &options.channel_lane)
        .map(Some)
}

/// Marks only the exact v2 lane's virtual record terminal. This is the API
/// that runtime pipeline terminal handling should use after it has a concrete
/// FullLane/ChannelStateLane identity.
pub fn mark_virtual_session_terminal_for_lane(
    options: VirtualSessionTerminalV2Options,
) -> io::Result<Option<PathBuf>> {
    let Some((_index, mut record)) = working_set_record_for_session_or_root_for_lane(
        &options.harness_home,
        &options.channel_lane,
        &options.session_key,
    )?
    else {
        return Ok(None);
    };
    record.status = "terminal-failed".to_string();
    close_open_working_sessions(&mut record, &options.ended_by, options.now_ms);
    write_virtual_session_record_v2_value(&options.harness_home, &record, &options.channel_lane)
        .map(Some)
}

pub fn record_completed_turn_working_set_snapshot(
    options: CompletedTurnWorkingSetSnapshotOptions,
) -> io::Result<CompletedTurnWorkingSetSnapshotReport> {
    let root_session_key = root_working_session_key(&options.working_session_key);
    let continuation_index =
        continuation_index_from_session_key(&options.working_session_key).unwrap_or(0);
    let virtual_session_id =
        read_working_set_memory_for_session(&options.harness_home, &options.working_session_key)?
            .map(|(index, _)| index.virtual_session_id)
            .or_else(|| {
                read_working_set_memory_for_session(&options.harness_home, &root_session_key)
                    .ok()
                    .flatten()
                    .map(|(index, _)| index.virtual_session_id)
            })
            .unwrap_or_else(|| {
                derive_virtual_session_id(
                    &options.platform,
                    &options.channel_id,
                    &options.user_id,
                    &options.agent_id,
                    &root_session_key,
                )
            });
    let predecessor = predecessor_session_key(&root_session_key, continuation_index);
    let pending_queue_item = options.queue_id.as_ref().map(|queue_id| {
        serde_json::json!({
            "schema": "agent-harness.runtime-queue-item.v1",
            "queueId": queue_id,
            "status": &options.status,
            "runtimeClass": "interactive",
            "origin": "channel",
            "agentId": &options.agent_id,
            "sessionKey": &options.working_session_key,
            "platform": &options.platform,
            "channelId": &options.channel_id,
            "userId": &options.user_id,
            "messageText": options.message_text.as_deref().unwrap_or_default(),
        })
    });
    let mut recent_files = Vec::new();
    if let Some(path) = options.outbox_file.as_ref() {
        recent_files.push(format!("final-outbox:{}", path.display()));
    }
    if let Some(path) = options.completion_file.as_ref() {
        recent_files.push(format!("completion:{}", path.display()));
    }
    let mut validation = Vec::new();
    if let Some(queue_id) = options.queue_id.as_ref() {
        validation.push(format!("run-once:{queue_id}:{}", options.status));
    }
    if let Some(path) = options.run_once_receipt_file.as_ref() {
        validation.push(format!("run-once-receipts:{}", path.display()));
    }
    let working_set_file = write_working_set_memory(
        &options.harness_home,
        &options.agent_id,
        &virtual_session_id,
        &options.working_session_key,
        predecessor.as_deref(),
        continuation_index,
        pending_queue_item,
        options.now_ms,
        WorkingSetWriteOptions {
            objective: options
                .message_text
                .as_deref()
                .and_then(|value| bounded_line(value, 200)),
            status: Some("active".to_string()),
            recent_files,
            validation,
            inherit_parent: true,
            ..WorkingSetWriteOptions::default()
        },
    )?;
    let virtual_session_file = write_virtual_session_record(
        &options.harness_home,
        &virtual_session_id,
        &options.platform,
        &options.channel_id,
        &options.user_id,
        &options.agent_id,
        &root_session_key,
        &options.working_session_key,
        continuation_index,
        &working_set_file,
        options.now_ms,
    )?;
    write_working_set_session_index(
        &options.harness_home,
        &options.working_session_key,
        &virtual_session_id,
        continuation_index,
        &working_set_file,
        options.now_ms,
    )?;

    Ok(CompletedTurnWorkingSetSnapshotReport {
        schema: WORKING_SET_MEMORY_SCHEMA.to_string(),
        virtual_session_id,
        working_session_key: options.working_session_key,
        continuation_index,
        working_set_file,
        virtual_session_file,
    })
}

/// Account-aware rollover path. The legacy entry point above remains intact;
/// new callers must use this version so the channel state, queue mutation,
/// counter, working-set index, and virtual record all share one exact lane.
pub fn apply_context_rollover_before_turn_for_lane(
    options: ContextRolloverBeforeTurnV2Options,
) -> io::Result<ContextRolloverReceipt> {
    let config = load_context_rollover_config(&options.harness_home)?;
    let make_receipt = |status: ContextRolloverStatus,
                        virtual_session_id: Option<String>,
                        continuation_index: u64,
                        new_working_session_key: Option<String>,
                        working_set_file: Option<PathBuf>,
                        virtual_session_file: Option<PathBuf>,
                        reason: &str| ContextRolloverReceipt {
        schema: CONTEXT_ROLLOVER_RECEIPT_V2_SCHEMA.to_string(),
        status,
        queue_id: Some(options.queue_id.clone()),
        runtime_class: options.runtime_class.clone(),
        platform: options.channel_lane.platform().to_string(),
        channel_id: options.channel_lane.channel_id().to_string(),
        user_id: options.channel_lane.user_id().to_string(),
        agent_id: options.channel_lane.agent_id().to_string(),
        virtual_session_id,
        previous_working_session_key: options.working_session_key.clone(),
        new_working_session_key,
        continuation_index,
        working_set_file,
        virtual_session_file,
        receipt_file: context_rollover_receipts_file(&options.harness_home),
        reason: reason.to_string(),
        created_at_ms: options.now_ms,
    };
    if config.rollover_mode == ContextRolloverMode::Disabled {
        return write_rollover_receipt_for_lane(
            make_receipt(
                ContextRolloverStatus::Disabled,
                None,
                0,
                None,
                None,
                None,
                "context rollover mode disabled",
            ),
            &options.channel_lane,
        );
    }

    let lane = ContextRolloverLaneV2 {
        runtime_class: options.runtime_class.clone(),
        channel_lane: options.channel_lane.clone(),
        working_session_key: options.working_session_key.clone(),
        virtual_session_id: None,
        continuation_index: 0,
    };
    let counter =
        load_or_create_context_compact_counter_for_lane(ContextCompactCounterV2Options {
            harness_home: options.harness_home.clone(),
            lane,
            now_ms: options.now_ms,
        })?;
    if !counter.rollover_pending
        && counter.successful_compact_count < config.max_successful_compacts_before_rollover
    {
        return write_rollover_receipt_for_lane(
            make_receipt(
                ContextRolloverStatus::NotPending,
                counter.virtual_session_id,
                counter.continuation_index,
                None,
                None,
                None,
                "compact counter is below rollover threshold",
            ),
            &options.channel_lane,
        );
    }

    let continuation_index = counter.continuation_index.saturating_add(1);
    let root_session_key = root_working_session_key(&options.working_session_key);
    let new_working_session_key = continuation_session_key(&root_session_key, continuation_index);
    let virtual_session_id = counter
        .virtual_session_id
        .unwrap_or_else(|| derive_virtual_session_id_v2(&options.channel_lane, &root_session_key));
    let queue_dir = options.harness_home.join("state").join("runtime-queue");
    let queue_file = queue_dir.join("pending.jsonl");
    if queue_item_has_prepared_receipt(&queue_dir, &options.queue_id)? {
        return write_rollover_receipt_for_lane(
            make_receipt(
                ContextRolloverStatus::BlockedPrepared,
                Some(virtual_session_id),
                continuation_index,
                None,
                None,
                None,
                "pending queue item already has a prepared execution receipt",
            ),
            &options.channel_lane,
        );
    }
    if queue_item_is_leased(&queue_dir, &options.runtime_class, &options.queue_id)? {
        return write_rollover_receipt_for_lane(
            make_receipt(
                ContextRolloverStatus::BlockedLeased,
                Some(virtual_session_id),
                continuation_index,
                None,
                None,
                None,
                "pending queue item is currently leased",
            ),
            &options.channel_lane,
        );
    }

    let state = match read_channel_session_state_v2(&options.harness_home, &options.channel_lane)? {
        Some(state) => Some(state),
        None => {
            migrate_legacy_channel_session_state_to_v2(
                &options.harness_home,
                &options.channel_lane,
            )?
            .state
        }
    };
    let Some(mut state) = state else {
        return write_rollover_receipt_for_lane(
            make_receipt(
                ContextRolloverStatus::BlockedChannelStateMissing,
                Some(virtual_session_id),
                continuation_index,
                None,
                None,
                None,
                "exact v2 channel state is unavailable; legacy migration was not eligible",
            ),
            &options.channel_lane,
        );
    };
    if state.active_session_key != options.working_session_key {
        return write_rollover_receipt_for_lane(
            make_receipt(
                ContextRolloverStatus::BlockedChannelStateMismatch,
                Some(virtual_session_id),
                continuation_index,
                None,
                None,
                None,
                "channel active session did not match the pending item session",
            ),
            &options.channel_lane,
        );
    }
    let Some(_pending) =
        find_pending_queue_item_for_lane(&queue_file, &options.queue_id, &options.channel_lane)?
    else {
        return write_rollover_receipt_for_lane(
            make_receipt(
                ContextRolloverStatus::NotFound,
                Some(virtual_session_id),
                continuation_index,
                Some(new_working_session_key),
                None,
                None,
                "pending queue item was not found in the exact channel lane",
            ),
            &options.channel_lane,
        );
    };
    let (planned_transcript_file, planned_trajectory_file) = planned_session_files(
        &options.harness_home,
        options.channel_lane.agent_id(),
        &new_working_session_key,
    );
    let previous_state = state.clone();
    let original_queue_text = fs::read_to_string(&queue_file)?;
    state.active_session_key = new_working_session_key.clone();
    state.updated_at_ms = options.now_ms;
    write_channel_session_state_v2(&options.harness_home, &options.channel_lane, &state)?;
    let rewrite = rewrite_pending_queue_item_session_for_lane(
        &queue_file,
        &options.queue_id,
        &options.channel_lane,
        &new_working_session_key,
        &virtual_session_id,
        continuation_index,
        &planned_transcript_file,
        &planned_trajectory_file,
    );
    let updated_queue_item = match rewrite {
        Ok(Some(value)) => value,
        Ok(None) => {
            write_channel_session_state_v2(
                &options.harness_home,
                &options.channel_lane,
                &previous_state,
            )?;
            write_text_atomic(&queue_file, &original_queue_text)?;
            return write_rollover_receipt_for_lane(
                make_receipt(
                    ContextRolloverStatus::NotFound,
                    Some(virtual_session_id),
                    continuation_index,
                    Some(new_working_session_key),
                    None,
                    None,
                    "pending queue item changed before exact-lane rollover",
                ),
                &options.channel_lane,
            );
        }
        Err(error) => {
            let _ = write_channel_session_state_v2(
                &options.harness_home,
                &options.channel_lane,
                &previous_state,
            );
            let _ = write_text_atomic(&queue_file, &original_queue_text);
            return Err(error);
        }
    };

    let post_state_result = (|| -> io::Result<(PathBuf, PathBuf)> {
        let working_set_file = write_working_set_memory_for_lane(
            &options.harness_home,
            &options.channel_lane,
            &virtual_session_id,
            &new_working_session_key,
            Some(&options.working_session_key),
            continuation_index,
            Some(updated_queue_item),
            options.now_ms,
            WorkingSetWriteOptions {
                append_decision: Some(
                    "context rollover re-keyed an unprepared pending queue item".to_string(),
                ),
                inherit_parent: true,
                ..WorkingSetWriteOptions::default()
            },
        )?;
        let virtual_session_file = write_virtual_session_record_for_lane(
            &options.harness_home,
            &options.channel_lane,
            &virtual_session_id,
            &root_session_key,
            &new_working_session_key,
            continuation_index,
            &working_set_file,
            options.now_ms,
        )?;
        write_context_rollover_episode_for_lane(
            &options.harness_home,
            &options.channel_lane,
            &virtual_session_id,
            Some(&options.queue_id),
            &options.working_session_key,
            &new_working_session_key,
            continuation_index,
            &working_set_file,
            options.now_ms,
        )?;
        write_working_set_session_index_v2(
            &options.harness_home,
            &options.channel_lane,
            &new_working_session_key,
            &virtual_session_id,
            continuation_index,
            &working_set_file,
            options.now_ms,
        )?;
        let new_lane = ContextRolloverLaneV2 {
            runtime_class: options.runtime_class.clone(),
            channel_lane: options.channel_lane.clone(),
            working_session_key: new_working_session_key.clone(),
            virtual_session_id: Some(virtual_session_id.clone()),
            continuation_index,
        };
        let new_counter =
            new_context_compact_counter_v2(&new_lane, &new_lane.lane_key(), options.now_ms);
        write_context_compact_counter_v2(
            &context_compact_counter_v2_file(&options.harness_home, &new_lane),
            &new_counter,
            &new_lane,
        )?;
        Ok((working_set_file, virtual_session_file))
    })();
    let (working_set_file, virtual_session_file) = match post_state_result {
        Ok(files) => files,
        Err(error) => {
            let _ = write_channel_session_state_v2(
                &options.harness_home,
                &options.channel_lane,
                &previous_state,
            );
            let _ = write_text_atomic(&queue_file, &original_queue_text);
            return Err(error);
        }
    };
    write_rollover_receipt_for_lane(
        make_receipt(
            ContextRolloverStatus::Applied,
            Some(virtual_session_id),
            continuation_index,
            Some(new_working_session_key),
            Some(working_set_file),
            Some(virtual_session_file),
            "context rollover re-keyed an unprepared pending queue item",
        ),
        &options.channel_lane,
    )
}

/// Account-aware snapshot writer. This is the v2 entry point used by the
/// virtual-session path; it never consults a v1 index except through the
/// deliberately constrained default-account migration helper.
pub fn record_completed_turn_working_set_snapshot_for_lane(
    options: CompletedTurnWorkingSetSnapshotV2Options,
) -> io::Result<CompletedTurnWorkingSetSnapshotReport> {
    let root_session_key = root_working_session_key(&options.working_session_key);
    let continuation_index =
        continuation_index_from_session_key(&options.working_session_key).unwrap_or(0);
    let current = read_working_set_memory_for_session_for_lane(
        &options.harness_home,
        &options.channel_lane,
        &options.working_session_key,
    )?;
    let existing = match current {
        Some(value) => Some(value),
        None => migrate_legacy_working_set_memory_for_session_for_lane(
            &options.harness_home,
            &options.channel_lane,
            &options.working_session_key,
        )?,
    };
    let root_existing = if existing.is_none() {
        let root = read_working_set_memory_for_session_for_lane(
            &options.harness_home,
            &options.channel_lane,
            &root_session_key,
        )?;
        match root {
            Some(value) => Some(value),
            None => migrate_legacy_working_set_memory_for_session_for_lane(
                &options.harness_home,
                &options.channel_lane,
                &root_session_key,
            )?,
        }
    } else {
        None
    };
    let virtual_session_id = existing
        .or(root_existing)
        .map(|(index, _)| index.virtual_session_id)
        .unwrap_or_else(|| derive_virtual_session_id_v2(&options.channel_lane, &root_session_key));
    let predecessor = predecessor_session_key(&root_session_key, continuation_index);
    let pending_queue_item = options.queue_id.as_ref().map(|queue_id| {
        serde_json::json!({
            "schema": "agent-harness.runtime-queue-item.v2",
            "queueId": queue_id,
            "status": &options.status,
            "runtimeClass": "interactive",
            "origin": "channel",
            "platform": options.channel_lane.platform(),
            "accountId": options.channel_lane.account_id(),
            "channelId": options.channel_lane.channel_id(),
            "userId": options.channel_lane.user_id(),
            "agentId": options.channel_lane.agent_id(),
            "sessionKey": &options.working_session_key,
            "messageText": options.message_text.as_deref().unwrap_or_default(),
        })
    });
    let mut recent_files = Vec::new();
    if let Some(path) = options.outbox_file.as_ref() {
        recent_files.push(format!("final-outbox:{}", path.display()));
    }
    if let Some(path) = options.completion_file.as_ref() {
        recent_files.push(format!("completion:{}", path.display()));
    }
    let mut validation = Vec::new();
    if let Some(queue_id) = options.queue_id.as_ref() {
        validation.push(format!("run-once:{queue_id}:{}", options.status));
    }
    if let Some(path) = options.run_once_receipt_file.as_ref() {
        validation.push(format!("run-once-receipts:{}", path.display()));
    }
    let working_set_file = write_working_set_memory_for_lane(
        &options.harness_home,
        &options.channel_lane,
        &virtual_session_id,
        &options.working_session_key,
        predecessor.as_deref(),
        continuation_index,
        pending_queue_item,
        options.now_ms,
        WorkingSetWriteOptions {
            objective: options
                .message_text
                .as_deref()
                .and_then(|value| bounded_line(value, 200)),
            status: Some("active".to_string()),
            recent_files,
            validation,
            inherit_parent: true,
            ..WorkingSetWriteOptions::default()
        },
    )?;
    let virtual_session_file = write_virtual_session_record_for_lane(
        &options.harness_home,
        &options.channel_lane,
        &virtual_session_id,
        &root_session_key,
        &options.working_session_key,
        continuation_index,
        &working_set_file,
        options.now_ms,
    )?;
    write_working_set_session_index_v2(
        &options.harness_home,
        &options.channel_lane,
        &options.working_session_key,
        &virtual_session_id,
        continuation_index,
        &working_set_file,
        options.now_ms,
    )?;
    Ok(CompletedTurnWorkingSetSnapshotReport {
        schema: WORKING_SET_MEMORY_V2_SCHEMA.to_string(),
        virtual_session_id,
        working_session_key: options.working_session_key,
        continuation_index,
        working_set_file,
        virtual_session_file,
    })
}

pub fn requeue_prepared_context_rollover(
    options: ContextRolloverRequeuePreparedOptions,
) -> io::Result<ContextRolloverPreparedRequeueReport> {
    let queue_file = options
        .harness_home
        .join("state")
        .join("runtime-queue")
        .join("pending.jsonl");
    if let Some(pending_item) = find_pending_queue_item(&queue_file, &options.queue_id)?
        && let Some(channel_lane) = exact_channel_state_lane_from_pending_item(&pending_item)?
    {
        return requeue_prepared_context_rollover_for_lane(options, channel_lane);
    }

    requeue_prepared_context_rollover_legacy(options)
}

fn requeue_prepared_context_rollover_legacy(
    options: ContextRolloverRequeuePreparedOptions,
) -> io::Result<ContextRolloverPreparedRequeueReport> {
    let queue_dir = options.harness_home.join("state").join("runtime-queue");
    let queue_file = queue_dir.join("pending.jsonl");
    let execution_receipts_file = queue_dir.join("execution-receipts.jsonl");
    let run_once_receipts_file = queue_dir.join("run-once-receipts.jsonl");
    fs::create_dir_all(&queue_dir)?;

    let pending_item =
        find_pending_queue_item(&queue_file, &options.queue_id)?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("pending queue item {} was not found", options.queue_id),
            )
        })?;
    let prepared_receipt =
        find_latest_prepared_receipt(&execution_receipts_file, &options.queue_id)?.ok_or_else(
            || {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!(
                        "prepared execution receipt for queue item {} was not found",
                        options.queue_id
                    ),
                )
            },
        )?;

    let previous_working_session_key =
        string_field(&pending_item, &["sessionKey", "session_key"]).map(ToString::to_string);
    let agent_id = string_field(&pending_item, &["agentId", "agent_id"]).unwrap_or("main");
    let platform = string_field(&pending_item, &["platform"]).unwrap_or("unknown");
    let channel_id = string_field(&pending_item, &["channelId", "channel_id"]).unwrap_or("unknown");
    let user_id = string_field(&pending_item, &["userId", "user_id"]).unwrap_or("unknown");
    let root_session_key = root_working_session_key(&options.new_working_session_key);
    let virtual_session_id =
        string_field(&pending_item, &["virtualSessionId", "virtual_session_id"])
            .map(ToString::to_string)
            .or_else(|| {
                Some(derive_virtual_session_id(
                    platform,
                    channel_id,
                    user_id,
                    agent_id,
                    &root_session_key,
                ))
            });
    let continuation_index = continuation_index_from_session_key(&options.new_working_session_key)
        .or_else(|| {
            pending_item
                .get("continuationIndex")
                .or_else(|| pending_item.get("continuation_index"))
                .and_then(Value::as_u64)
                .map(|value| value.saturating_add(1))
        })
        .unwrap_or(1);
    let requeued_queue_id = format!("{}:rollover-requeue-{}", options.queue_id, options.now_ms);
    let (planned_transcript_file, planned_trajectory_file) = planned_session_files(
        &options.harness_home,
        agent_id,
        &options.new_working_session_key,
    );
    let prepared_execution_dir =
        path_string_field(&prepared_receipt, &["executionDir", "execution_dir"])
            .or_else(|| path_string_field(&pending_item, &["previousExecutionDir"]));
    let state_file =
        channel_session_state_file(&options.harness_home, platform, channel_id, user_id);
    let mut previous_state =
        read_channel_session_state(&options.harness_home, platform, channel_id, user_id)?
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    "channel session state is missing; refusing prepared rollover requeue",
                )
            })?;
    let original_state = previous_state.clone();
    let previous_session = previous_working_session_key.as_deref().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "pending queue item has no sessionKey; refusing prepared rollover requeue",
        )
    })?;
    if previous_state.active_session_key != previous_session
        && previous_state.active_session_key != options.new_working_session_key
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "channel active session `{}` did not match pending session `{}` or new session `{}`",
                previous_state.active_session_key,
                previous_session,
                options.new_working_session_key
            ),
        ));
    }

    let mut new_item = pending_item.clone();
    if let Some(object) = new_item.as_object_mut() {
        object.insert(
            "queueId".to_string(),
            Value::String(requeued_queue_id.clone()),
        );
        object.insert("status".to_string(), Value::String("queued".to_string()));
        object.insert(
            "createdAtMs".to_string(),
            Value::Number(serde_json::Number::from(options.now_ms)),
        );
        object.insert(
            "sessionKey".to_string(),
            Value::String(options.new_working_session_key.clone()),
        );
        if let Some(virtual_session_id) = virtual_session_id.as_ref() {
            object.insert(
                "virtualSessionId".to_string(),
                Value::String(virtual_session_id.clone()),
            );
        }
        object.insert(
            "continuationIndex".to_string(),
            Value::Number(serde_json::Number::from(continuation_index)),
        );
        object.insert(
            "completionKind".to_string(),
            Value::String("continuation-rollover".to_string()),
        );
        object.insert("taskTerminal".to_string(), Value::Bool(false));
        object.insert("suppressSelfImprovement".to_string(), Value::Bool(true));
        object.insert(
            "plannedTranscriptFile".to_string(),
            Value::String(planned_transcript_file.display().to_string()),
        );
        object.insert(
            "plannedTrajectoryFile".to_string(),
            Value::String(planned_trajectory_file.display().to_string()),
        );
        object.insert(
            "requeuedFromQueueId".to_string(),
            Value::String(options.queue_id.clone()),
        );
        object.insert(
            "requeueReason".to_string(),
            Value::String(options.reason.clone()),
        );
        if let Some(execution_dir) = prepared_execution_dir.as_ref() {
            object.insert(
                "previousExecutionDir".to_string(),
                Value::String(execution_dir.display().to_string()),
            );
        }
    }

    append_jsonl_value(&queue_file, &new_item)?;
    previous_state.active_session_key = options.new_working_session_key.clone();
    previous_state.agent_id = Some(agent_id.to_string());
    previous_state.updated_at_ms = options.now_ms;
    if let Err(error) = write_json_atomic(&state_file, &previous_state) {
        let _ = write_json_atomic(&state_file, &original_state);
        return Err(error);
    }
    if let Err(error) = append_jsonl_value(
        &run_once_receipts_file,
        &serde_json::json!({
            "schema": "agent-harness.runtime-run-once.v1",
            "queueId": options.queue_id,
            "status": "skipped",
            "runtimeClass": string_field(&pending_item, &["runtimeClass", "runtime_class"])
                .or_else(|| string_field(&prepared_receipt, &["runtimeClass", "runtime_class"])),
            "origin": string_field(&pending_item, &["origin"])
                .or_else(|| string_field(&prepared_receipt, &["origin"])),
            "cronRunId": string_field(&pending_item, &["cronRunId", "cron_run_id"])
                .or_else(|| string_field(&prepared_receipt, &["cronRunId", "cron_run_id"])),
            "scheduledForMs": pending_item
                .get("scheduledForMs")
                .or_else(|| pending_item.get("scheduled_for_ms"))
                .or_else(|| prepared_receipt.get("scheduledForMs"))
                .or_else(|| prepared_receipt.get("scheduled_for_ms")),
            "executionDir": prepared_execution_dir
                .as_ref()
                .map(|path| path.display().to_string()),
            "transcriptFile": Value::Null,
            "outboxFile": Value::Null,
            "reason": format!("context rollover requeued prepared item: {}", options.reason),
        }),
    ) {
        let _ = write_json_atomic(&state_file, &original_state);
        return Err(error);
    }

    if let Some(virtual_session_id) = virtual_session_id.as_ref() {
        let file = write_working_set_memory(
            &options.harness_home,
            agent_id,
            virtual_session_id,
            &options.new_working_session_key,
            previous_working_session_key.as_deref(),
            continuation_index,
            Some(new_item.clone()),
            options.now_ms,
            WorkingSetWriteOptions {
                append_decision: Some(format!(
                    "context rollover requeued prepared item: {}",
                    options.reason
                )),
                inherit_parent: true,
                ..WorkingSetWriteOptions::default()
            },
        )?;
        write_virtual_session_record(
            &options.harness_home,
            virtual_session_id,
            platform,
            channel_id,
            user_id,
            agent_id,
            &root_session_key,
            &options.new_working_session_key,
            continuation_index,
            &file,
            options.now_ms,
        )?;
        write_context_rollover_episode(
            &options.harness_home,
            virtual_session_id,
            Some(&requeued_queue_id),
            previous_session,
            &options.new_working_session_key,
            continuation_index,
            &file,
            options.now_ms,
        )?;
        write_working_set_session_index(
            &options.harness_home,
            &options.new_working_session_key,
            virtual_session_id,
            continuation_index,
            &file,
            options.now_ms,
        )?;
    }

    let report_file = context_rollover_prepared_requeues_file(&options.harness_home);
    let report = ContextRolloverPreparedRequeueReport {
        schema: CONTEXT_ROLLOVER_PREPARED_REQUEUE_SCHEMA.to_string(),
        queue_id: options.queue_id,
        requeued_queue_id,
        previous_working_session_key,
        new_working_session_key: options.new_working_session_key,
        virtual_session_id,
        continuation_index,
        pending_queue_file: queue_file,
        run_once_receipts_file,
        prepared_execution_dir,
        report_file: report_file.clone(),
        requeued: true,
        reason: options.reason,
        created_at_ms: options.now_ms,
    };
    append_jsonl_value(&report_file, &report)?;
    Ok(report)
}

/// Requeues a prepared item through the exact provider/account/channel/user/
/// agent lane. This path deliberately never reads or updates a legacy channel
/// state file once the queued item declares an account axis.
fn requeue_prepared_context_rollover_for_lane(
    options: ContextRolloverRequeuePreparedOptions,
    channel_lane: ChannelStateLane,
) -> io::Result<ContextRolloverPreparedRequeueReport> {
    let queue_dir = options.harness_home.join("state").join("runtime-queue");
    let queue_file = queue_dir.join("pending.jsonl");
    let execution_receipts_file = queue_dir.join("execution-receipts.jsonl");
    let run_once_receipts_file = queue_dir.join("run-once-receipts.jsonl");
    fs::create_dir_all(&queue_dir)?;

    let pending_item =
        find_pending_queue_item_for_lane(&queue_file, &options.queue_id, &channel_lane)?
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!(
                        "pending queue item {} was not found in the exact channel lane",
                        options.queue_id
                    ),
                )
            })?;
    // Execution receipts are keyed by queue id and older prepared receipts can
    // intentionally be sparse. The exact pending item above is the ownership
    // gate; never infer its lane from a sparse receipt.
    let prepared_receipt =
        find_latest_prepared_receipt(&execution_receipts_file, &options.queue_id)?.ok_or_else(
            || {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!(
                        "prepared execution receipt for queue item {} was not found",
                        options.queue_id
                    ),
                )
            },
        )?;

    let previous_working_session_key =
        string_field(&pending_item, &["sessionKey", "session_key"]).map(ToString::to_string);
    let previous_session = previous_working_session_key.as_deref().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "pending queue item has no sessionKey; refusing exact-lane prepared rollover requeue",
        )
    })?;
    let root_session_key = root_working_session_key(&options.new_working_session_key);
    let virtual_session_id = derive_virtual_session_id_v2(&channel_lane, &root_session_key);
    let continuation_index = continuation_index_from_session_key(&options.new_working_session_key)
        .or_else(|| {
            pending_item
                .get("continuationIndex")
                .or_else(|| pending_item.get("continuation_index"))
                .and_then(Value::as_u64)
                .map(|value| value.saturating_add(1))
        })
        .unwrap_or(1);
    let requeued_queue_id = format!("{}:rollover-requeue-{}", options.queue_id, options.now_ms);
    let (planned_transcript_file, planned_trajectory_file) = planned_session_files(
        &options.harness_home,
        channel_lane.agent_id(),
        &options.new_working_session_key,
    );
    let prepared_execution_dir =
        path_string_field(&prepared_receipt, &["executionDir", "execution_dir"])
            .or_else(|| path_string_field(&pending_item, &["previousExecutionDir"]));
    let mut previous_state =
        match read_channel_session_state_v2(&options.harness_home, &channel_lane)? {
            Some(state) => Some(state),
            None => {
                migrate_legacy_channel_session_state_to_v2(&options.harness_home, &channel_lane)?
                    .state
            }
        }
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "exact v2 channel session state is unavailable; legacy migration was not eligible",
            )
        })?;
    let original_state = previous_state.clone();
    if previous_state.active_session_key != previous_session
        && previous_state.active_session_key != options.new_working_session_key
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "exact channel active session `{}` did not match pending session `{}` or new session `{}`",
                previous_state.active_session_key,
                previous_session,
                options.new_working_session_key
            ),
        ));
    }

    let mut new_item = pending_item.clone();
    let object = new_item.as_object_mut().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "exact-lane pending queue item is not an object",
        )
    })?;
    object.insert(
        "queueId".to_string(),
        Value::String(requeued_queue_id.clone()),
    );
    object.insert("status".to_string(), Value::String("queued".to_string()));
    object.insert(
        "createdAtMs".to_string(),
        Value::Number(serde_json::Number::from(options.now_ms)),
    );
    object.insert(
        "sessionKey".to_string(),
        Value::String(options.new_working_session_key.clone()),
    );
    object.insert(
        "virtualSessionId".to_string(),
        Value::String(virtual_session_id.clone()),
    );
    object.insert(
        "continuationIndex".to_string(),
        Value::Number(serde_json::Number::from(continuation_index)),
    );
    object.insert(
        "completionKind".to_string(),
        Value::String("continuation-rollover".to_string()),
    );
    object.insert("taskTerminal".to_string(), Value::Bool(false));
    object.insert("suppressSelfImprovement".to_string(), Value::Bool(true));
    object.insert(
        "plannedTranscriptFile".to_string(),
        Value::String(planned_transcript_file.display().to_string()),
    );
    object.insert(
        "plannedTrajectoryFile".to_string(),
        Value::String(planned_trajectory_file.display().to_string()),
    );
    object.insert(
        "requeuedFromQueueId".to_string(),
        Value::String(options.queue_id.clone()),
    );
    object.insert(
        "requeueReason".to_string(),
        Value::String(options.reason.clone()),
    );
    if let Some(execution_dir) = prepared_execution_dir.as_ref() {
        object.insert(
            "previousExecutionDir".to_string(),
            Value::String(execution_dir.display().to_string()),
        );
    }
    insert_v2_lane_metadata(&mut new_item, &channel_lane)?;

    append_jsonl_value(&queue_file, &new_item)?;
    previous_state.active_session_key = options.new_working_session_key.clone();
    previous_state.updated_at_ms = options.now_ms;
    if let Err(error) =
        write_channel_session_state_v2(&options.harness_home, &channel_lane, &previous_state)
    {
        let _ =
            write_channel_session_state_v2(&options.harness_home, &channel_lane, &original_state);
        return Err(error);
    }
    if let Err(error) = append_jsonl_value(
        &run_once_receipts_file,
        &serde_json::json!({
            "schema": "agent-harness.runtime-run-once.v1",
            "queueId": options.queue_id,
            "status": "skipped",
            "runtimeClass": string_field(&pending_item, &["runtimeClass", "runtime_class"])
                .or_else(|| string_field(&prepared_receipt, &["runtimeClass", "runtime_class"])),
            "origin": string_field(&pending_item, &["origin"])
                .or_else(|| string_field(&prepared_receipt, &["origin"])),
            "cronRunId": string_field(&pending_item, &["cronRunId", "cron_run_id"])
                .or_else(|| string_field(&prepared_receipt, &["cronRunId", "cron_run_id"])),
            "scheduledForMs": pending_item
                .get("scheduledForMs")
                .or_else(|| pending_item.get("scheduled_for_ms"))
                .or_else(|| prepared_receipt.get("scheduledForMs"))
                .or_else(|| prepared_receipt.get("scheduled_for_ms")),
            "platform": channel_lane.platform(),
            "accountId": channel_lane.account_id(),
            "channelId": channel_lane.channel_id(),
            "userId": channel_lane.user_id(),
            "agentId": channel_lane.agent_id(),
            "sessionKey": previous_session,
            "executionDir": prepared_execution_dir
                .as_ref()
                .map(|path| path.display().to_string()),
            "transcriptFile": Value::Null,
            "outboxFile": Value::Null,
            "reason": format!("context rollover requeued prepared item: {}", options.reason),
        }),
    ) {
        let _ =
            write_channel_session_state_v2(&options.harness_home, &channel_lane, &original_state);
        return Err(error);
    }

    let working_set_file = write_working_set_memory_for_lane(
        &options.harness_home,
        &channel_lane,
        &virtual_session_id,
        &options.new_working_session_key,
        previous_working_session_key.as_deref(),
        continuation_index,
        Some(new_item.clone()),
        options.now_ms,
        WorkingSetWriteOptions {
            append_decision: Some(format!(
                "context rollover requeued prepared item: {}",
                options.reason
            )),
            inherit_parent: true,
            ..WorkingSetWriteOptions::default()
        },
    )?;
    write_virtual_session_record_for_lane(
        &options.harness_home,
        &channel_lane,
        &virtual_session_id,
        &root_session_key,
        &options.new_working_session_key,
        continuation_index,
        &working_set_file,
        options.now_ms,
    )?;
    write_context_rollover_episode_for_lane(
        &options.harness_home,
        &channel_lane,
        &virtual_session_id,
        Some(&requeued_queue_id),
        previous_session,
        &options.new_working_session_key,
        continuation_index,
        &working_set_file,
        options.now_ms,
    )?;
    write_working_set_session_index_v2(
        &options.harness_home,
        &channel_lane,
        &options.new_working_session_key,
        &virtual_session_id,
        continuation_index,
        &working_set_file,
        options.now_ms,
    )?;

    let report_file = context_rollover_prepared_requeues_file(&options.harness_home);
    let report = ContextRolloverPreparedRequeueReport {
        schema: CONTEXT_ROLLOVER_PREPARED_REQUEUE_SCHEMA.to_string(),
        queue_id: options.queue_id,
        requeued_queue_id,
        previous_working_session_key,
        new_working_session_key: options.new_working_session_key,
        virtual_session_id: Some(virtual_session_id),
        continuation_index,
        pending_queue_file: queue_file,
        run_once_receipts_file,
        prepared_execution_dir,
        report_file: report_file.clone(),
        requeued: true,
        reason: options.reason,
        created_at_ms: options.now_ms,
    };
    append_jsonl_value(&report_file, &report)?;
    Ok(report)
}

pub fn requeue_prepared_context_rollover_if_no_parent_siblings(
    options: ContextRolloverRequeuePreparedOptions,
) -> io::Result<ContextRolloverPreparedRequeueReport> {
    let queue_file = options
        .harness_home
        .join("state")
        .join("runtime-queue")
        .join("pending.jsonl");
    let pending_item =
        find_pending_queue_item(&queue_file, &options.queue_id)?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("pending queue item {} was not found", options.queue_id),
            )
        })?;
    let parent_session = string_field(&pending_item, &["sessionKey", "session_key"])
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "pending queue item has no sessionKey; refusing auto rollover requeue",
            )
        })?
        .to_string();
    if let Some(channel_lane) = exact_channel_state_lane_from_pending_item(&pending_item)? {
        if queued_parent_session_sibling_exists_for_lane(
            &queue_file,
            &options.queue_id,
            &parent_session,
            &channel_lane,
        )? {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "another pending item targets the parent working session in the exact channel lane; refusing auto rollover requeue",
            ));
        }
        return requeue_prepared_context_rollover_for_lane(options, channel_lane);
    }
    let agent_id = string_field(&pending_item, &["agentId", "agent_id"]).unwrap_or("main");
    let platform = string_field(&pending_item, &["platform"]).unwrap_or("unknown");
    let channel_id = string_field(&pending_item, &["channelId", "channel_id"]).unwrap_or("unknown");
    let user_id = string_field(&pending_item, &["userId", "user_id"]).unwrap_or("unknown");

    if queued_parent_session_sibling_exists(
        &queue_file,
        &options.queue_id,
        &parent_session,
        agent_id,
        platform,
        channel_id,
        user_id,
    )? {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "another pending item targets the parent working session; refusing auto rollover requeue",
        ));
    }

    requeue_prepared_context_rollover_legacy(options)
}

pub fn context_rollover_receipts_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("context-rollover")
        .join("receipts.jsonl")
}

pub fn context_rollover_prepared_requeues_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("context-rollover")
        .join("prepared-requeues.jsonl")
}

pub fn context_rollover_episode_index_file(
    harness_home: impl AsRef<Path>,
    virtual_session_id: &str,
) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("context-rollover")
        .join("episodes")
        .join(format!("{}.jsonl", safe_path_segment(virtual_session_id)))
}

pub fn working_set_session_index_file(
    harness_home: impl AsRef<Path>,
    session_key: &str,
) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("context-rollover")
        .join("session-index")
        .join(format!("{}.json", safe_path_segment(session_key)))
}

/// V2 working-set lookup path. The digest includes every normalized lane axis
/// and the exact working session key; it is intentionally not a lossy path
/// segment derived from a user- or provider-controlled identifier.
pub(crate) fn working_set_session_index_v2_file(
    harness_home: impl AsRef<Path>,
    lane: &ChannelStateLane,
    session_key: &str,
) -> PathBuf {
    let key = sha256_hex(&canonical_identity_bytes(&[
        "working-set-session-index-v2",
        &channel_state_lane_storage_key(lane),
        session_key,
    ]));
    harness_home
        .as_ref()
        .join("state")
        .join("context-rollover")
        .join("session-index-v2")
        .join(format!("{key}.json"))
}

pub fn planned_session_files(
    harness_home: &Path,
    agent_id: &str,
    session_key: &str,
) -> (PathBuf, PathBuf) {
    let transcript = harness_home
        .join("agents")
        .join(safe_path_segment(agent_id))
        .join("sessions")
        .join(format!("{}.jsonl", safe_path_segment(session_key)));
    let trajectory = transcript.with_file_name(format!(
        "{}.trajectory.jsonl",
        safe_path_segment(session_key)
    ));
    (transcript, trajectory)
}

pub fn continuation_session_key(root_session_key: &str, continuation_index: u64) -> String {
    if continuation_index == 0 {
        root_session_key.to_string()
    } else {
        format!("{root_session_key}:cont-{continuation_index}")
    }
}

pub fn root_working_session_key(session_key: &str) -> String {
    if let Some((root, suffix)) = session_key.rsplit_once(":cont-")
        && suffix.chars().all(|ch| ch.is_ascii_digit())
    {
        return root.to_string();
    }
    session_key.to_string()
}

pub fn is_rollover_completion_kind(value: Option<&str>) -> bool {
    matches!(
        value,
        Some("rollover-prep" | "rollover-pickup" | "continuation-rollover")
    )
}

fn new_context_compact_counter(
    lane: &ContextRolloverLane,
    lane_key: &str,
    now_ms: i64,
) -> ContextCompactCounter {
    ContextCompactCounter {
        schema: CONTEXT_COMPACT_COUNTER_SCHEMA.to_string(),
        lane_key: lane_key.to_string(),
        lane_hash: fnv1a_64_hex(lane_key),
        platform: lane.platform.clone(),
        channel_id: lane.channel_id.clone(),
        user_id: lane.user_id.clone(),
        agent_id: lane.agent_id.clone(),
        working_session_key: lane.working_session_key.clone(),
        virtual_session_id: lane.virtual_session_id.clone(),
        continuation_index: lane.continuation_index,
        successful_compact_count: 0,
        rollover_pending: false,
        last_compact_thread_id: None,
        last_successful_compact_attempt_key: None,
        last_rollover_receipt: None,
        created_at_ms: now_ms,
        updated_at_ms: now_ms,
    }
}

fn new_context_compact_counter_v2(
    lane: &ContextRolloverLaneV2,
    lane_key: &str,
    now_ms: i64,
) -> ContextCompactCounter {
    ContextCompactCounter {
        schema: CONTEXT_COMPACT_COUNTER_V2_SCHEMA.to_string(),
        lane_key: lane_key.to_string(),
        lane_hash: sha256_hex(&canonical_identity_bytes(&["compact-counter-v2", lane_key])),
        platform: lane.channel_lane.platform().to_string(),
        channel_id: lane.channel_lane.channel_id().to_string(),
        user_id: lane.channel_lane.user_id().to_string(),
        agent_id: lane.channel_lane.agent_id().to_string(),
        working_session_key: lane.working_session_key.clone(),
        virtual_session_id: lane.virtual_session_id.clone(),
        continuation_index: lane.continuation_index,
        successful_compact_count: 0,
        rollover_pending: false,
        last_compact_thread_id: None,
        last_successful_compact_attempt_key: None,
        last_rollover_receipt: None,
        created_at_ms: now_ms,
        updated_at_ms: now_ms,
    }
}

fn counter_matches_lane_v2(
    counter: &ContextCompactCounter,
    value: &Value,
    lane: &ContextRolloverLaneV2,
    lane_key: &str,
) -> bool {
    counter.schema == CONTEXT_COMPACT_COUNTER_V2_SCHEMA
        && counter.lane_key == lane_key
        && counter.platform == lane.channel_lane.platform()
        && counter.channel_id == lane.channel_lane.channel_id()
        && counter.user_id == lane.channel_lane.user_id()
        && counter.agent_id == lane.channel_lane.agent_id()
        && counter.working_session_key == lane.working_session_key
        && v2_value_matches_lane(value, &lane.channel_lane)
}

fn write_context_compact_counter_v2(
    path: &Path,
    counter: &ContextCompactCounter,
    lane: &ContextRolloverLaneV2,
) -> io::Result<()> {
    let mut value = serde_json::to_value(counter).map_err(io::Error::other)?;
    insert_v2_lane_metadata(&mut value, &lane.channel_lane)?;
    write_json_atomic(path, &value)
}

fn blocked_receipt(
    options: &ContextRolloverBeforeTurnOptions,
    status: ContextRolloverStatus,
    virtual_session_id: Option<String>,
    continuation_index: u64,
    reason: &str,
) -> ContextRolloverReceipt {
    ContextRolloverReceipt {
        schema: CONTEXT_ROLLOVER_RECEIPT_SCHEMA.to_string(),
        status,
        queue_id: Some(options.queue_id.clone()),
        runtime_class: options.runtime_class.clone(),
        platform: options.platform.clone(),
        channel_id: options.channel_id.clone(),
        user_id: options.user_id.clone(),
        agent_id: options.agent_id.clone(),
        virtual_session_id,
        previous_working_session_key: options.working_session_key.clone(),
        new_working_session_key: None,
        continuation_index,
        working_set_file: None,
        virtual_session_file: None,
        receipt_file: context_rollover_receipts_file(&options.harness_home),
        reason: reason.to_string(),
        created_at_ms: options.now_ms,
    }
}

fn write_rollover_receipt(
    mut receipt: ContextRolloverReceipt,
) -> io::Result<ContextRolloverReceipt> {
    let receipt_file = receipt.receipt_file.clone();
    receipt.receipt_file = receipt_file.clone();
    append_jsonl_value(&receipt_file, &receipt)?;
    Ok(receipt)
}

fn write_rollover_receipt_for_lane(
    mut receipt: ContextRolloverReceipt,
    lane: &ChannelStateLane,
) -> io::Result<ContextRolloverReceipt> {
    let receipt_file = receipt.receipt_file.clone();
    receipt.receipt_file = receipt_file.clone();
    let mut value = serde_json::to_value(&receipt).map_err(io::Error::other)?;
    insert_v2_lane_metadata(&mut value, lane)?;
    append_jsonl_value(&receipt_file, &value)?;
    Ok(receipt)
}

fn queue_item_has_prepared_receipt(queue_dir: &Path, queue_id: &str) -> io::Result<bool> {
    has_prepared_execution_receipt_from_index(queue_dir, queue_id, &mut Vec::new())
}

fn queue_item_is_leased(queue_dir: &Path, runtime_class: &str, queue_id: &str) -> io::Result<bool> {
    for path in [
        runtime_queue_leases_file(queue_dir, runtime_class),
        runtime_queue_leases_file(queue_dir, "legacy"),
    ] {
        if !path.is_file() {
            continue;
        }
        let text = fs::read_to_string(path)?;
        let Ok(value) = serde_json::from_str::<Value>(&text) else {
            continue;
        };
        if value
            .get("leases")
            .and_then(Value::as_object)
            .is_some_and(|leases| leases.contains_key(queue_id))
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn runtime_queue_leases_file(queue_dir: &Path, runtime_class: &str) -> PathBuf {
    if runtime_class == "legacy" {
        return queue_dir.join("runtime-leases.json");
    }
    queue_dir
        .join("classes")
        .join(safe_path_segment(runtime_class))
        .join("runtime-leases.json")
}

fn rewrite_pending_queue_item_session(
    queue_file: &Path,
    queue_id: &str,
    new_working_session_key: &str,
    virtual_session_id: &str,
    continuation_index: u64,
    planned_transcript_file: &Path,
    planned_trajectory_file: &Path,
) -> io::Result<Option<Value>> {
    if !queue_file.is_file() {
        return Ok(None);
    }
    let text = fs::read_to_string(queue_file)?;
    let mut changed = false;
    let mut updated_item = None;
    let mut lines = Vec::new();
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            lines.push(line.to_string());
            continue;
        }
        let Ok(mut value) = serde_json::from_str::<Value>(trimmed) else {
            lines.push(line.to_string());
            continue;
        };
        let is_target = string_field(&value, &["queueId", "queue_id"]) == Some(queue_id)
            && string_field(&value, &["status"]) == Some("queued");
        if is_target && let Some(object) = value.as_object_mut() {
            object.insert(
                "sessionKey".to_string(),
                Value::String(new_working_session_key.to_string()),
            );
            object.insert(
                "virtualSessionId".to_string(),
                Value::String(virtual_session_id.to_string()),
            );
            object.insert(
                "continuationIndex".to_string(),
                Value::Number(serde_json::Number::from(continuation_index)),
            );
            object.insert(
                "completionKind".to_string(),
                Value::String("continuation-rollover".to_string()),
            );
            object.insert("taskTerminal".to_string(), Value::Bool(false));
            object.insert("suppressSelfImprovement".to_string(), Value::Bool(true));
            object.insert(
                "plannedTranscriptFile".to_string(),
                Value::String(planned_transcript_file.display().to_string()),
            );
            object.insert(
                "plannedTrajectoryFile".to_string(),
                Value::String(planned_trajectory_file.display().to_string()),
            );
            updated_item = Some(value.clone());
            changed = true;
            lines.push(serde_json::to_string(&value).map_err(io::Error::other)?);
        } else {
            lines.push(line.to_string());
        }
    }
    if changed {
        let mut out = lines.join("\n");
        out.push('\n');
        write_text_atomic(queue_file, &out)?;
    }
    Ok(updated_item)
}

fn exact_channel_state_lane_from_pending_item(
    pending_item: &Value,
) -> io::Result<Option<ChannelStateLane>> {
    let Some(account_id) = string_field(pending_item, &["accountId", "account_id"]) else {
        return Ok(None);
    };
    let platform = string_field(pending_item, &["platform"]).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "account-scoped pending queue item has no platform",
        )
    })?;
    let channel_id = string_field(pending_item, &["channelId", "channel_id"]).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "account-scoped pending queue item has no channelId",
        )
    })?;
    let user_id = string_field(pending_item, &["userId", "user_id"]).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "account-scoped pending queue item has no userId",
        )
    })?;
    let agent_id = string_field(pending_item, &["agentId", "agent_id"]).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "account-scoped pending queue item has no agentId",
        )
    })?;
    ChannelStateLane::new(platform, Some(account_id), channel_id, user_id, agent_id).map(Some)
}

fn find_pending_queue_item(queue_file: &Path, queue_id: &str) -> io::Result<Option<Value>> {
    if !queue_file.is_file() {
        return Ok(None);
    }
    let text = fs::read_to_string(queue_file)?;
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
            continue;
        };
        if string_field(&value, &["queueId", "queue_id"]) == Some(queue_id)
            && string_field(&value, &["status"]) == Some("queued")
        {
            return Ok(Some(value));
        }
    }
    Ok(None)
}

fn find_pending_queue_item_for_lane(
    queue_file: &Path,
    queue_id: &str,
    lane: &ChannelStateLane,
) -> io::Result<Option<Value>> {
    if !queue_file.is_file() {
        return Ok(None);
    }
    let text = fs::read_to_string(queue_file)?;
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
            continue;
        };
        if string_field(&value, &["queueId", "queue_id"]) == Some(queue_id)
            && string_field(&value, &["status"]) == Some("queued")
            && v2_value_matches_lane(&value, lane)
        {
            return Ok(Some(value));
        }
    }
    Ok(None)
}

#[allow(clippy::too_many_arguments)]
fn rewrite_pending_queue_item_session_for_lane(
    queue_file: &Path,
    queue_id: &str,
    lane: &ChannelStateLane,
    new_working_session_key: &str,
    virtual_session_id: &str,
    continuation_index: u64,
    planned_transcript_file: &Path,
    planned_trajectory_file: &Path,
) -> io::Result<Option<Value>> {
    let text = fs::read_to_string(queue_file)?;
    let mut lines = Vec::new();
    let mut updated_item = None;
    let mut changed = false;
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(mut value) = serde_json::from_str::<Value>(trimmed) else {
            lines.push(line.to_string());
            continue;
        };
        let is_target = string_field(&value, &["queueId", "queue_id"]) == Some(queue_id)
            && string_field(&value, &["status"]) == Some("queued")
            && v2_value_matches_lane(&value, lane);
        if is_target && let Some(object) = value.as_object_mut() {
            object.insert(
                "sessionKey".to_string(),
                Value::String(new_working_session_key.to_string()),
            );
            object.insert(
                "virtualSessionId".to_string(),
                Value::String(virtual_session_id.to_string()),
            );
            object.insert(
                "continuationIndex".to_string(),
                Value::Number(serde_json::Number::from(continuation_index)),
            );
            object.insert(
                "completionKind".to_string(),
                Value::String("continuation-rollover".to_string()),
            );
            object.insert("taskTerminal".to_string(), Value::Bool(false));
            object.insert("suppressSelfImprovement".to_string(), Value::Bool(true));
            object.insert(
                "plannedTranscriptFile".to_string(),
                Value::String(planned_transcript_file.display().to_string()),
            );
            object.insert(
                "plannedTrajectoryFile".to_string(),
                Value::String(planned_trajectory_file.display().to_string()),
            );
            updated_item = Some(value.clone());
            changed = true;
            lines.push(serde_json::to_string(&value).map_err(io::Error::other)?);
        } else {
            lines.push(line.to_string());
        }
    }
    if changed {
        let mut out = lines.join("\n");
        out.push('\n');
        write_text_atomic(queue_file, &out)?;
    }
    Ok(updated_item)
}

fn queued_parent_session_sibling_exists(
    queue_file: &Path,
    queue_id: &str,
    parent_session: &str,
    agent_id: &str,
    platform: &str,
    channel_id: &str,
    user_id: &str,
) -> io::Result<bool> {
    if !queue_file.is_file() {
        return Ok(false);
    }
    let text = fs::read_to_string(queue_file)?;
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
            continue;
        };
        if string_field(&value, &["queueId", "queue_id"]) == Some(queue_id) {
            continue;
        }
        if string_field(&value, &["status"]) != Some("queued") {
            continue;
        }
        if string_field(&value, &["sessionKey", "session_key"]) == Some(parent_session)
            && string_field(&value, &["agentId", "agent_id"]) == Some(agent_id)
            && string_field(&value, &["platform"]) == Some(platform)
            && string_field(&value, &["channelId", "channel_id"]) == Some(channel_id)
            && string_field(&value, &["userId", "user_id"]) == Some(user_id)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn queued_parent_session_sibling_exists_for_lane(
    queue_file: &Path,
    queue_id: &str,
    parent_session: &str,
    lane: &ChannelStateLane,
) -> io::Result<bool> {
    if !queue_file.is_file() {
        return Ok(false);
    }
    let text = fs::read_to_string(queue_file)?;
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
            continue;
        };
        if string_field(&value, &["queueId", "queue_id"]) == Some(queue_id) {
            continue;
        }
        if string_field(&value, &["status"]) != Some("queued") {
            continue;
        }
        if string_field(&value, &["sessionKey", "session_key"]) == Some(parent_session)
            && v2_value_matches_lane(&value, lane)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn find_latest_prepared_receipt(receipts_file: &Path, queue_id: &str) -> io::Result<Option<Value>> {
    if !receipts_file.is_file() {
        return Ok(None);
    }
    let text = fs::read_to_string(receipts_file)?;
    let mut receipt = None;
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
            continue;
        };
        if string_field(&value, &["queueId", "queue_id"]) == Some(queue_id)
            && string_field(&value, &["status"]) == Some("prepared")
        {
            receipt = Some(value);
        }
    }
    Ok(receipt)
}

#[derive(Debug, Clone, Default)]
struct WorkingSetWriteOptions {
    objective: Option<String>,
    status: Option<String>,
    recent_files: Vec<String>,
    validation: Vec<String>,
    append_decision: Option<String>,
    inherit_parent: bool,
}

fn write_working_set_memory(
    harness_home: &Path,
    agent_id: &str,
    virtual_session_id: &str,
    working_session_key: &str,
    previous_working_session_key: Option<&str>,
    continuation_index: u64,
    pending_queue_item: Option<Value>,
    now_ms: i64,
    options: WorkingSetWriteOptions,
) -> io::Result<PathBuf> {
    let file = working_set_file(harness_home, virtual_session_id, continuation_index);
    let (transcript_file, trajectory_file) =
        planned_session_files(harness_home, agent_id, working_session_key);
    let active_plan_refs =
        collect_active_operation_plan_refs(harness_home, agent_id, working_session_key)?;
    let inherited = if options.inherit_parent {
        previous_working_session_key
            .map(|session| read_working_set_memory_for_session(harness_home, session))
            .transpose()?
            .flatten()
            .map(|(_, memory)| memory)
    } else {
        None
    };
    let inherited_goal = inherited.as_ref().map(|memory| memory.goal.clone());
    let objective = options
        .objective
        .or_else(|| {
            inherited_goal
                .as_ref()
                .and_then(|goal| goal.objective.clone())
        })
        .or_else(|| {
            pending_queue_item
                .as_ref()
                .and_then(|value| string_field(value, &["messageText", "message_text"]))
                .and_then(|value| bounded_line(value, 200))
        });
    let goal_status = options
        .status
        .or_else(|| inherited_goal.as_ref().map(|goal| goal.status.clone()))
        .unwrap_or_else(|| "active".to_string());
    let mut constraints = inherited
        .as_ref()
        .map(|memory| memory.constraints.clone())
        .unwrap_or_default();
    let mut decisions = inherited
        .as_ref()
        .map(|memory| memory.decisions.clone())
        .unwrap_or_default();
    if let Some(decision) = options
        .append_decision
        .and_then(|value| bounded_line(&value, 240))
    {
        if !decisions.iter().any(|existing| existing == &decision) {
            decisions.push(decision);
        }
    }
    let mut recent_files = inherited
        .as_ref()
        .map(|memory| memory.recent_files.clone())
        .unwrap_or_default();
    recent_files.extend(options.recent_files);
    let mut validation = inherited
        .as_ref()
        .map(|memory| memory.validation.clone())
        .unwrap_or_default();
    validation.extend(options.validation);
    let blockers = inherited
        .as_ref()
        .map(|memory| memory.blockers.clone())
        .unwrap_or_default();
    cap_strings(&mut constraints, 8);
    cap_strings(&mut decisions, 12);
    cap_strings(&mut recent_files, 8);
    cap_strings(&mut validation, 8);
    let queue_dir = harness_home.join("state").join("runtime-queue");
    let prompt_bundle_json = pending_queue_item
        .as_ref()
        .and_then(|value| string_field(value, &["queueId", "queue_id"]))
        .map(|queue_id| {
            queue_dir
                .join("executions")
                .join(safe_path_segment(queue_id))
                .join("prompt-bundle.json")
        });
    let memory = ContextWorkingSetMemory {
        schema: WORKING_SET_MEMORY_SCHEMA.to_string(),
        virtual_session_id: virtual_session_id.to_string(),
        working_session_key: working_session_key.to_string(),
        previous_working_session_key: previous_working_session_key.map(ToString::to_string),
        continuation_index,
        goal: ContextWorkingSetGoal {
            objective,
            status: goal_status,
            budget_usage: inherited_goal
                .as_ref()
                .and_then(|goal| goal.budget_usage.clone()),
            completion_criteria: inherited_goal
                .map(|goal| goal.completion_criteria)
                .unwrap_or_default(),
        },
        active_plan_refs,
        pending_queue_item,
        constraints,
        decisions,
        recent_files,
        validation,
        blockers,
        static_record_refs: ContextStaticRecordRefs {
            transcript_file: Some(transcript_file.clone()),
            trajectory_file: Some(trajectory_file.clone()),
            codex_binding_file: Some(trajectory_file.with_file_name(format!(
                "{}.codex-binding.json",
                safe_path_segment(working_session_key)
            ))),
            prompt_bundle_json,
            runtime_receipts: vec![
                queue_dir.join("execution-receipts.jsonl"),
                queue_dir.join("run-once-receipts.jsonl"),
            ],
        },
        agent_continuation_note: None,
        created_at_ms: now_ms,
    };
    write_json_atomic(&file, &memory)?;
    Ok(file)
}

#[allow(clippy::too_many_arguments)]
fn write_working_set_memory_for_lane(
    harness_home: &Path,
    lane: &ChannelStateLane,
    virtual_session_id: &str,
    working_session_key: &str,
    previous_working_session_key: Option<&str>,
    continuation_index: u64,
    pending_queue_item: Option<Value>,
    now_ms: i64,
    options: WorkingSetWriteOptions,
) -> io::Result<PathBuf> {
    let file = working_set_file(harness_home, virtual_session_id, continuation_index);
    let (transcript_file, trajectory_file) =
        planned_session_files(harness_home, lane.agent_id(), working_session_key);
    let active_plan_refs =
        collect_active_operation_plan_refs_for_lane(harness_home, lane, working_session_key)?;
    let inherited = if options.inherit_parent {
        if let Some(previous_session_key) = previous_working_session_key {
            let current = read_working_set_memory_for_session_for_lane(
                harness_home,
                lane,
                previous_session_key,
            )?;
            let previous = match current {
                Some(value) => Some(value),
                None => migrate_legacy_working_set_memory_for_session_for_lane(
                    harness_home,
                    lane,
                    previous_session_key,
                )?,
            };
            previous.map(|(_, memory)| memory)
        } else {
            None
        }
    } else {
        None
    };
    let inherited_goal = inherited.as_ref().map(|memory| memory.goal.clone());
    let objective = options
        .objective
        .or_else(|| {
            inherited_goal
                .as_ref()
                .and_then(|goal| goal.objective.clone())
        })
        .or_else(|| {
            pending_queue_item
                .as_ref()
                .and_then(|value| string_field(value, &["messageText", "message_text"]))
                .and_then(|value| bounded_line(value, 200))
        });
    let goal_status = options
        .status
        .or_else(|| inherited_goal.as_ref().map(|goal| goal.status.clone()))
        .unwrap_or_else(|| "active".to_string());
    let mut constraints = inherited
        .as_ref()
        .map(|memory| memory.constraints.clone())
        .unwrap_or_default();
    let mut decisions = inherited
        .as_ref()
        .map(|memory| memory.decisions.clone())
        .unwrap_or_default();
    if let Some(decision) = options
        .append_decision
        .and_then(|value| bounded_line(&value, 240))
    {
        if !decisions.iter().any(|existing| existing == &decision) {
            decisions.push(decision);
        }
    }
    let mut recent_files = inherited
        .as_ref()
        .map(|memory| memory.recent_files.clone())
        .unwrap_or_default();
    recent_files.extend(options.recent_files);
    let mut validation = inherited
        .as_ref()
        .map(|memory| memory.validation.clone())
        .unwrap_or_default();
    validation.extend(options.validation);
    let blockers = inherited
        .as_ref()
        .map(|memory| memory.blockers.clone())
        .unwrap_or_default();
    cap_strings(&mut constraints, 8);
    cap_strings(&mut decisions, 12);
    cap_strings(&mut recent_files, 8);
    cap_strings(&mut validation, 8);
    let queue_dir = harness_home.join("state").join("runtime-queue");
    let prompt_bundle_json = pending_queue_item
        .as_ref()
        .and_then(|value| string_field(value, &["queueId", "queue_id"]))
        .map(|queue_id| {
            queue_dir
                .join("executions")
                .join(safe_path_segment(queue_id))
                .join("prompt-bundle.json")
        });
    let memory = ContextWorkingSetMemory {
        schema: WORKING_SET_MEMORY_V2_SCHEMA.to_string(),
        virtual_session_id: virtual_session_id.to_string(),
        working_session_key: working_session_key.to_string(),
        previous_working_session_key: previous_working_session_key.map(ToString::to_string),
        continuation_index,
        goal: ContextWorkingSetGoal {
            objective,
            status: goal_status,
            budget_usage: inherited_goal
                .as_ref()
                .and_then(|goal| goal.budget_usage.clone()),
            completion_criteria: inherited_goal
                .map(|goal| goal.completion_criteria)
                .unwrap_or_default(),
        },
        active_plan_refs,
        pending_queue_item,
        constraints,
        decisions,
        recent_files,
        validation,
        blockers,
        static_record_refs: ContextStaticRecordRefs {
            transcript_file: Some(transcript_file.clone()),
            trajectory_file: Some(trajectory_file.clone()),
            codex_binding_file: Some(trajectory_file.with_file_name(format!(
                "{}.codex-binding.json",
                safe_path_segment(working_session_key)
            ))),
            prompt_bundle_json,
            runtime_receipts: vec![
                queue_dir.join("execution-receipts.jsonl"),
                queue_dir.join("run-once-receipts.jsonl"),
            ],
        },
        agent_continuation_note: None,
        created_at_ms: now_ms,
    };
    write_working_set_memory_v2_value(&file, &memory, lane)?;
    Ok(file)
}

#[allow(clippy::too_many_arguments)]
fn write_virtual_session_record(
    harness_home: &Path,
    virtual_session_id: &str,
    platform: &str,
    channel_id: &str,
    user_id: &str,
    agent_id: &str,
    root_session_key: &str,
    active_working_session_key: &str,
    continuation_index: u64,
    working_set_file: &Path,
    now_ms: i64,
) -> io::Result<PathBuf> {
    let file = virtual_session_file(harness_home, virtual_session_id);
    let episode_index_file = harness_home
        .join("state")
        .join("context-rollover")
        .join("episodes")
        .join(format!("{}.jsonl", safe_path_segment(virtual_session_id)));
    let mut record = if file.is_file() {
        let text = fs::read_to_string(&file)?;
        serde_json::from_str::<ContextVirtualSessionRecord>(&text).map_err(io::Error::other)?
    } else {
        ContextVirtualSessionRecord {
            schema: VIRTUAL_SESSION_SCHEMA.to_string(),
            virtual_session_id: virtual_session_id.to_string(),
            platform: platform.to_string(),
            channel_id: channel_id.to_string(),
            user_id: user_id.to_string(),
            agent_id: agent_id.to_string(),
            status: "active".to_string(),
            root_session_key: root_session_key.to_string(),
            active_working_session_key: active_working_session_key.to_string(),
            continuation_index,
            working_sessions: Vec::new(),
            active_goal_ref: None,
            working_set_file: working_set_file.to_path_buf(),
            episode_index_file,
        }
    };
    if !record
        .working_sessions
        .iter()
        .any(|session| session.session_key == active_working_session_key)
    {
        record.working_sessions.push(ContextWorkingSessionRef {
            session_key: active_working_session_key.to_string(),
            continuation_index,
            codex_thread_id: None,
            started_at_ms: now_ms,
            ended_at_ms: None,
            ended_by: None,
            working_set_file: Some(working_set_file.to_path_buf()),
        });
    }
    record.active_working_session_key = active_working_session_key.to_string();
    record.continuation_index = continuation_index;
    record.working_set_file = working_set_file.to_path_buf();
    write_json_atomic(&file, &record)?;
    Ok(file)
}

#[allow(clippy::too_many_arguments)]
fn write_virtual_session_record_for_lane(
    harness_home: &Path,
    lane: &ChannelStateLane,
    virtual_session_id: &str,
    root_session_key: &str,
    active_working_session_key: &str,
    continuation_index: u64,
    working_set_file: &Path,
    now_ms: i64,
) -> io::Result<PathBuf> {
    let file = virtual_session_file(harness_home, virtual_session_id);
    let episode_index_file = harness_home
        .join("state")
        .join("context-rollover")
        .join("episodes")
        .join(format!("{}.jsonl", safe_path_segment(virtual_session_id)));
    let mut record =
        match read_virtual_session_record_for_lane(harness_home, lane, virtual_session_id)? {
            Some((_file, record)) => record,
            None => ContextVirtualSessionRecord {
                schema: VIRTUAL_SESSION_V2_SCHEMA.to_string(),
                virtual_session_id: virtual_session_id.to_string(),
                platform: lane.platform().to_string(),
                channel_id: lane.channel_id().to_string(),
                user_id: lane.user_id().to_string(),
                agent_id: lane.agent_id().to_string(),
                status: "active".to_string(),
                root_session_key: root_session_key.to_string(),
                active_working_session_key: active_working_session_key.to_string(),
                continuation_index,
                working_sessions: Vec::new(),
                active_goal_ref: None,
                working_set_file: working_set_file.to_path_buf(),
                episode_index_file,
            },
        };
    if record.root_session_key != root_session_key {
        return Err(invalid_v2_artifact(&file, "virtual-session root mismatch"));
    }
    if !record
        .working_sessions
        .iter()
        .any(|session| session.session_key == active_working_session_key)
    {
        record.working_sessions.push(ContextWorkingSessionRef {
            session_key: active_working_session_key.to_string(),
            continuation_index,
            codex_thread_id: None,
            started_at_ms: now_ms,
            ended_at_ms: None,
            ended_by: None,
            working_set_file: Some(working_set_file.to_path_buf()),
        });
    }
    record.schema = VIRTUAL_SESSION_V2_SCHEMA.to_string();
    record.active_working_session_key = active_working_session_key.to_string();
    record.continuation_index = continuation_index;
    record.working_set_file = working_set_file.to_path_buf();
    write_virtual_session_record_v2_value(harness_home, &record, lane)
}

fn read_virtual_session_record_for_session(
    harness_home: &Path,
    session_key: &str,
) -> io::Result<Option<(PathBuf, ContextVirtualSessionRecord, WorkingSetSessionIndex)>> {
    let Some(index) = working_set_index_for_session_or_root(harness_home, session_key)? else {
        return Ok(None);
    };
    let file = virtual_session_file(harness_home, &index.virtual_session_id);
    if !file.is_file() {
        return Ok(None);
    }
    let text = fs::read_to_string(&file)?;
    let record =
        serde_json::from_str::<ContextVirtualSessionRecord>(&text).map_err(io::Error::other)?;
    Ok(Some((file, record, index)))
}

fn working_set_index_for_session_or_root(
    harness_home: &Path,
    session_key: &str,
) -> io::Result<Option<WorkingSetSessionIndex>> {
    if let Some((index, _)) = read_working_set_memory_for_session(harness_home, session_key)? {
        return Ok(Some(index));
    }
    let root_session_key = root_working_session_key(session_key);
    if root_session_key == session_key {
        return Ok(None);
    }
    read_working_set_memory_for_session(harness_home, &root_session_key)
        .map(|maybe| maybe.map(|(index, _)| index))
}

fn close_open_working_sessions(
    record: &mut ContextVirtualSessionRecord,
    ended_by: &str,
    now_ms: i64,
) {
    for session in &mut record.working_sessions {
        if session.ended_at_ms.is_none() {
            session.ended_at_ms = Some(now_ms);
            session.ended_by = Some(ended_by.to_string());
        }
    }
}

fn write_context_rollover_episode(
    harness_home: &Path,
    virtual_session_id: &str,
    queue_id: Option<&str>,
    previous_working_session_key: &str,
    new_working_session_key: &str,
    continuation_index: u64,
    working_set_file: &Path,
    now_ms: i64,
) -> io::Result<PathBuf> {
    let file = context_rollover_episode_index_file(harness_home, virtual_session_id);
    append_jsonl_value(
        &file,
        &ContextRolloverEpisode {
            schema: CONTEXT_ROLLOVER_EPISODE_SCHEMA.to_string(),
            virtual_session_id: virtual_session_id.to_string(),
            queue_id: queue_id.map(ToString::to_string),
            previous_working_session_key: previous_working_session_key.to_string(),
            new_working_session_key: new_working_session_key.to_string(),
            continuation_index,
            working_set_file: working_set_file.to_path_buf(),
            created_at_ms: now_ms,
        },
    )?;
    Ok(file)
}

fn write_working_set_session_index(
    harness_home: &Path,
    session_key: &str,
    virtual_session_id: &str,
    continuation_index: u64,
    working_set_file: &Path,
    now_ms: i64,
) -> io::Result<()> {
    write_json_atomic(
        &working_set_session_index_file(harness_home, session_key),
        &WorkingSetSessionIndex {
            schema: WORKING_SET_SESSION_INDEX_SCHEMA.to_string(),
            session_key: session_key.to_string(),
            virtual_session_id: virtual_session_id.to_string(),
            continuation_index,
            working_set_file: working_set_file.to_path_buf(),
            updated_at_ms: now_ms,
        },
    )
}

fn render_working_set_continuity(
    working_set_file: &Path,
    working_set: &ContextWorkingSetMemory,
) -> String {
    let previous = working_set
        .previous_working_session_key
        .as_deref()
        .unwrap_or("(none)");
    let objective = working_set
        .goal
        .objective
        .as_deref()
        .unwrap_or("(not recorded)");
    let pending_queue_id = working_set
        .pending_queue_item
        .as_ref()
        .and_then(|value| string_field(value, &["queueId", "queue_id"]))
        .unwrap_or("(none)");
    format!(
        "Working Set Continuity\n\
         virtualSessionId: {}\n\
         workingSessionKey: {}\n\
         continuationIndex: {}\n\
         predecessorSession: {}\n\
         workingSetFile: {}\n\
         goalStatus: {}\n\
         goalObjective: {}\n\
         pendingQueueId: {}\n\
         Deterministic harness state outranks any narrative continuation note.",
        working_set.virtual_session_id,
        working_set.working_session_key,
        working_set.continuation_index,
        previous,
        working_set_file.display(),
        working_set.goal.status,
        objective,
        pending_queue_id
    )
}

pub(crate) fn working_set_file(
    harness_home: &Path,
    virtual_session_id: &str,
    continuation_index: u64,
) -> PathBuf {
    harness_home
        .join("state")
        .join("context-rollover")
        .join("working-sets")
        .join(safe_path_segment(virtual_session_id))
        .join(format!("{continuation_index}.json"))
}

pub(crate) fn virtual_session_file(harness_home: &Path, virtual_session_id: &str) -> PathBuf {
    harness_home
        .join("state")
        .join("context-rollover")
        .join("virtual-sessions")
        .join(format!("{}.json", safe_path_segment(virtual_session_id)))
}

pub(crate) fn derive_virtual_session_id(
    platform: &str,
    channel_id: &str,
    user_id: &str,
    agent_id: &str,
    root_session_key: &str,
) -> String {
    let input = format!("{platform}:{channel_id}:{user_id}:{agent_id}:{root_session_key}");
    format!(
        "{}:{}:{}:{}:vsession-{}",
        safe_path_segment(platform),
        safe_path_segment(channel_id),
        safe_path_segment(user_id),
        safe_path_segment(agent_id),
        fnv1a_64_hex(&input)
    )
}

/// V2 virtual identifiers incorporate the exact account-aware lane through a
/// length-delimited SHA-256 encoding. The v1 helper above stays available for
/// legacy artifacts only.
pub(crate) fn derive_virtual_session_id_v2(
    lane: &ChannelStateLane,
    root_session_key: &str,
) -> String {
    format!(
        "vsession-v2-{}",
        sha256_hex(&canonical_identity_bytes(&[
            "virtual-session-v2",
            &channel_state_lane_storage_key(lane),
            root_session_key,
        ]))
    )
}

fn write_working_set_session_index_v2(
    harness_home: &Path,
    lane: &ChannelStateLane,
    session_key: &str,
    virtual_session_id: &str,
    continuation_index: u64,
    working_set_file: &Path,
    now_ms: i64,
) -> io::Result<()> {
    let index = WorkingSetSessionIndexV2 {
        schema: WORKING_SET_SESSION_INDEX_V2_SCHEMA.to_string(),
        platform: lane.platform().to_string(),
        account_id: lane.account_id().to_string(),
        channel_id: lane.channel_id().to_string(),
        user_id: lane.user_id().to_string(),
        agent_id: lane.agent_id().to_string(),
        session_key: session_key.to_string(),
        virtual_session_id: virtual_session_id.to_string(),
        continuation_index,
        working_set_file: working_set_file.to_path_buf(),
        updated_at_ms: now_ms,
    };
    write_json_atomic(
        &working_set_session_index_v2_file(harness_home, lane, session_key),
        &index,
    )
}

fn json_bool(object: &Map<String, Value>, keys: &[&str]) -> Option<bool> {
    keys.iter()
        .find_map(|key| object.get(*key).and_then(Value::as_bool))
}

fn json_u64(object: &Map<String, Value>, keys: &[&str]) -> Option<u64> {
    keys.iter()
        .find_map(|key| object.get(*key).and_then(Value::as_u64))
}

fn json_string(object: &Map<String, Value>, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        object
            .get(*key)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string)
    })
}

pub(crate) fn collect_active_operation_plan_refs(
    harness_home: &Path,
    agent_id: &str,
    working_session_key: &str,
) -> io::Result<Vec<String>> {
    let plans_dir = harness_home.join("state").join("operation-plans");
    if !plans_dir.is_dir() {
        return Ok(Vec::new());
    }
    let root_session = root_working_session_key(working_session_key);
    let mut refs = BTreeSet::new();
    for entry in fs::read_dir(plans_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let plan_file = entry.path().join("plan.json");
        if !plan_file.is_file() {
            continue;
        }
        let Ok(text) = fs::read_to_string(&plan_file) else {
            continue;
        };
        let Ok(value) = serde_json::from_str::<Value>(&text) else {
            continue;
        };
        let plan_agent = string_field(&value, &["agentId", "agent_id"]).unwrap_or_default();
        if plan_agent != agent_id {
            continue;
        }
        let plan_session = string_field(&value, &["sessionKey", "session_key"]).unwrap_or_default();
        if plan_session != working_session_key
            && root_working_session_key(plan_session) != root_session
        {
            continue;
        }
        let status = string_field(&value, &["status"]).unwrap_or("open");
        if matches!(status, "completed" | "canceled") {
            continue;
        }
        let plan_id = string_field(&value, &["planId", "plan_id"])
            .map(ToString::to_string)
            .or_else(|| {
                entry
                    .file_name()
                    .to_str()
                    .filter(|value| !value.is_empty())
                    .map(ToString::to_string)
            })
            .unwrap_or_else(|| "unknown".to_string());
        refs.insert(format!("operation-plan:{plan_id}:{status}"));
    }
    Ok(refs.into_iter().collect())
}

/// Exact-lane variant for v2 working-set persistence. A legacy operation plan
/// without an account id is intentionally not inherited into a v2 prompt.
pub(crate) fn collect_active_operation_plan_refs_for_lane(
    harness_home: &Path,
    lane: &ChannelStateLane,
    working_session_key: &str,
) -> io::Result<Vec<String>> {
    let plans_dir = harness_home.join("state").join("operation-plans");
    if !plans_dir.is_dir() {
        return Ok(Vec::new());
    }
    let root_session = root_working_session_key(working_session_key);
    let mut refs = BTreeSet::new();
    for entry in fs::read_dir(plans_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let plan_file = entry.path().join("plan.json");
        if !plan_file.is_file() {
            continue;
        }
        let Ok(value) = read_json_value(&plan_file) else {
            continue;
        };
        let plan_session = string_field(&value, &["sessionKey", "session_key"]).unwrap_or_default();
        if plan_session != working_session_key
            && root_working_session_key(plan_session) != root_session
        {
            continue;
        }
        if !operation_plan_v2_matches_lane(&value, lane, plan_session) {
            continue;
        }
        let status = string_field(&value, &["status"]).unwrap_or("open");
        if matches!(status, "completed" | "canceled") {
            continue;
        }
        let plan_id = string_field(&value, &["planId", "plan_id"])
            .map(ToString::to_string)
            .or_else(|| {
                entry
                    .file_name()
                    .to_str()
                    .filter(|value| !value.is_empty())
                    .map(ToString::to_string)
            })
            .unwrap_or_else(|| "unknown".to_string());
        refs.insert(format!("operation-plan:{plan_id}:{status}"));
    }
    Ok(refs.into_iter().collect())
}

/// Operation plans persist their full-lane owner as a digest rather than raw
/// provider identifiers. Rebuild that digest from the plan's own concrete
/// session (not the caller's continuation) so a root-scoped plan remains
/// visible across its same-lane continuations without becoming cross-account
/// or cross-agent context.
fn operation_plan_v2_matches_lane(
    value: &Value,
    lane: &ChannelStateLane,
    plan_session_key: &str,
) -> bool {
    let Some(persisted_digest) = string_field(value, &["laneDigest", "lane_digest"]) else {
        return false;
    };
    if string_field(value, &["agentId", "agent_id"]) != Some(lane.agent_id()) {
        return false;
    }
    let plan_lane = match FullLaneKeyV1::new(
        lane.platform(),
        lane.account_id(),
        lane.channel_id(),
        lane.user_id(),
        lane.agent_id(),
        "interactive",
        root_working_session_key(plan_session_key),
        plan_session_key,
    ) {
        Ok(lane) => lane,
        Err(_) => return false,
    };
    plan_lane
        .identity_hash()
        .is_ok_and(|expected_digest| expected_digest == persisted_digest)
}

fn channel_state_lane_storage_key(lane: &ChannelStateLane) -> String {
    sha256_hex(&canonical_identity_bytes(&[
        "channel-state-lane-v2",
        lane.platform(),
        lane.account_id(),
        lane.channel_id(),
        lane.user_id(),
        lane.agent_id(),
    ]))
}

fn canonical_identity_bytes(parts: &[&str]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for part in parts {
        let raw = part.as_bytes();
        bytes.extend_from_slice(&(raw.len() as u64).to_be_bytes());
        bytes.extend_from_slice(raw);
    }
    bytes
}

fn sha256_hex(bytes: &[u8]) -> String {
    digest::digest(&digest::SHA256, bytes)
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn read_json_value(path: &Path) -> io::Result<Value> {
    let text = fs::read_to_string(path)?;
    serde_json::from_str(&text).map_err(io::Error::other)
}

fn invalid_v2_artifact(path: &Path, reason: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("v2 context-rollover artifact {}: {reason}", path.display()),
    )
}

fn v2_value_matches_lane(value: &Value, lane: &ChannelStateLane) -> bool {
    string_field(value, &["platform"]) == Some(lane.platform())
        && string_field(value, &["accountId", "account_id"]) == Some(lane.account_id())
        && string_field(value, &["channelId", "channel_id"]) == Some(lane.channel_id())
        && string_field(value, &["userId", "user_id"]) == Some(lane.user_id())
        && string_field(value, &["agentId", "agent_id"]) == Some(lane.agent_id())
}

fn working_set_index_matches_lane_v2(
    index: &WorkingSetSessionIndexV2,
    lane: &ChannelStateLane,
    session_key: &str,
) -> bool {
    index.schema == WORKING_SET_SESSION_INDEX_V2_SCHEMA
        && index.platform == lane.platform()
        && index.account_id == lane.account_id()
        && index.channel_id == lane.channel_id()
        && index.user_id == lane.user_id()
        && index.agent_id == lane.agent_id()
        && index.session_key == session_key
}

fn working_set_memory_matches_lane_v2(
    memory: &ContextWorkingSetMemory,
    value: &Value,
    lane: &ChannelStateLane,
    virtual_session_id: &str,
    session_key: &str,
    continuation_index: u64,
) -> bool {
    memory.schema == WORKING_SET_MEMORY_V2_SCHEMA
        && memory.virtual_session_id == virtual_session_id
        && memory.working_session_key == session_key
        && memory.continuation_index == continuation_index
        && v2_value_matches_lane(value, lane)
}

fn legacy_virtual_record_matches_default_lane(
    value: &Value,
    record: &ContextVirtualSessionRecord,
    lane: &ChannelStateLane,
) -> bool {
    let legacy_account = string_field(value, &["accountId", "account_id"]);
    record.platform == lane.platform()
        && record.channel_id == lane.channel_id()
        && record.user_id == lane.user_id()
        && record.agent_id == lane.agent_id()
        && matches!(legacy_account, None | Some("default"))
}

fn write_working_set_memory_v2_value(
    path: &Path,
    memory: &ContextWorkingSetMemory,
    lane: &ChannelStateLane,
) -> io::Result<()> {
    let mut value = serde_json::to_value(memory).map_err(io::Error::other)?;
    insert_v2_lane_metadata(&mut value, lane)?;
    write_json_atomic(path, &value)
}

fn write_virtual_session_record_v2_value(
    harness_home: &Path,
    record: &ContextVirtualSessionRecord,
    lane: &ChannelStateLane,
) -> io::Result<PathBuf> {
    let file = virtual_session_file(harness_home, &record.virtual_session_id);
    if file.is_file() {
        let _ =
            read_virtual_session_record_for_lane(harness_home, lane, &record.virtual_session_id)?;
    }
    let mut value = serde_json::to_value(record).map_err(io::Error::other)?;
    insert_v2_lane_metadata(&mut value, lane)?;
    write_json_atomic(&file, &value)?;
    Ok(file)
}

#[allow(clippy::too_many_arguments)]
fn write_context_rollover_episode_for_lane(
    harness_home: &Path,
    lane: &ChannelStateLane,
    virtual_session_id: &str,
    queue_id: Option<&str>,
    previous_working_session_key: &str,
    new_working_session_key: &str,
    continuation_index: u64,
    working_set_file: &Path,
    now_ms: i64,
) -> io::Result<PathBuf> {
    let file = context_rollover_episode_index_file(harness_home, virtual_session_id);
    let mut value = serde_json::to_value(ContextRolloverEpisode {
        schema: CONTEXT_ROLLOVER_EPISODE_V2_SCHEMA.to_string(),
        virtual_session_id: virtual_session_id.to_string(),
        queue_id: queue_id.map(ToString::to_string),
        previous_working_session_key: previous_working_session_key.to_string(),
        new_working_session_key: new_working_session_key.to_string(),
        continuation_index,
        working_set_file: working_set_file.to_path_buf(),
        created_at_ms: now_ms,
    })
    .map_err(io::Error::other)?;
    insert_v2_lane_metadata(&mut value, lane)?;
    append_jsonl_value(&file, &value)?;
    Ok(file)
}

fn insert_v2_lane_metadata(value: &mut Value, lane: &ChannelStateLane) -> io::Result<()> {
    let object = value.as_object_mut().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "v2 rollover artifact is not an object",
        )
    })?;
    object.insert(
        "platform".to_string(),
        Value::String(lane.platform().to_string()),
    );
    object.insert(
        "accountId".to_string(),
        Value::String(lane.account_id().to_string()),
    );
    object.insert(
        "channelId".to_string(),
        Value::String(lane.channel_id().to_string()),
    );
    object.insert(
        "userId".to_string(),
        Value::String(lane.user_id().to_string()),
    );
    object.insert(
        "agentId".to_string(),
        Value::String(lane.agent_id().to_string()),
    );
    Ok(())
}

pub(crate) fn string_field<'a>(value: &'a Value, names: &[&str]) -> Option<&'a str> {
    names.iter().find_map(|name| value.get(*name)?.as_str())
}

fn path_string_field(value: &Value, names: &[&str]) -> Option<PathBuf> {
    string_field(value, names).map(PathBuf::from)
}

pub(crate) fn continuation_index_from_session_key(session_key: &str) -> Option<u64> {
    let (_, suffix) = session_key.rsplit_once(":cont-")?;
    suffix.parse::<u64>().ok()
}

fn predecessor_session_key(root_session_key: &str, continuation_index: u64) -> Option<String> {
    match continuation_index {
        0 => None,
        1 => Some(root_session_key.to_string()),
        value => Some(continuation_session_key(
            root_session_key,
            value.saturating_sub(1),
        )),
    }
}

fn bounded_line(value: &str, max_chars: usize) -> Option<String> {
    let line = value
        .lines()
        .find(|line| !line.trim().is_empty())?
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if line.is_empty() {
        return None;
    }
    if line.chars().count() <= max_chars {
        return Some(line);
    }
    let mut out = line
        .chars()
        .take(max_chars.saturating_sub(3))
        .collect::<String>();
    out.push_str("...");
    Some(out)
}

fn cap_strings(values: &mut Vec<String>, limit: usize) {
    for value in values.iter_mut() {
        if let Some(bounded) = bounded_line(value, 300) {
            *value = bounded;
        }
    }
    values.sort();
    values.dedup();
    if values.len() > limit {
        let keep_from = values.len().saturating_sub(limit);
        values.drain(0..keep_from);
    }
}

fn write_text_atomic(path: &Path, contents: &str) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("state.txt");
    let tmp_path = path.with_file_name(format!(
        ".{file_name}.{}.{}.tmp",
        std::process::id(),
        fnv1a_64_hex(contents)
    ));
    {
        let mut file = fs::File::create(&tmp_path)?;
        file.write_all(contents.as_bytes())?;
        file.sync_all()?;
    }
    match fs::rename(&tmp_path, path) {
        Ok(()) => Ok(()),
        Err(error) if matches!(error.kind(), io::ErrorKind::AlreadyExists) || path.exists() => {
            match fs::remove_file(path) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => {
                    let _ = fs::remove_file(&tmp_path);
                    return Err(error);
                }
            }
            match fs::rename(&tmp_path, path) {
                Ok(()) => Ok(()),
                Err(error) => {
                    let _ = fs::remove_file(&tmp_path);
                    Err(error)
                }
            }
        }
        Err(error) => {
            let _ = fs::remove_file(&tmp_path);
            Err(error)
        }
    }
}

pub(crate) fn safe_path_segment(value: &str) -> String {
    let mut out = String::new();
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        "unknown".to_string()
    } else {
        out
    }
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn fnv1a_64_hex(value: &str) -> String {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel_state::ChannelSessionState;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_root(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "agent_harness_context_rollover_{}_{}_{}",
            name,
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    fn test_lane(session_key: &str) -> ContextRolloverLane {
        ContextRolloverLane {
            runtime_class: "interactive".to_string(),
            agent_id: "main".to_string(),
            platform: "telegram".to_string(),
            channel_id: "dm".to_string(),
            user_id: "user".to_string(),
            working_session_key: session_key.to_string(),
            virtual_session_id: None,
            continuation_index: 0,
        }
    }

    #[test]
    fn compact_counter_uses_one_file_per_session_lane() {
        let root = temp_root("counter_files");
        let harness_home = root.join(".agent-harness");
        let left = test_lane("telegram:dm:user:main");
        let right = test_lane("telegram:dm:user:main:other");

        let left_counter = load_or_create_context_compact_counter(ContextCompactCounterOptions {
            harness_home: harness_home.clone(),
            lane: left.clone(),
            now_ms: 10,
        })
        .unwrap();
        let right_counter = load_or_create_context_compact_counter(ContextCompactCounterOptions {
            harness_home: harness_home.clone(),
            lane: right.clone(),
            now_ms: 11,
        })
        .unwrap();

        assert_ne!(left_counter.lane_hash, right_counter.lane_hash);
        assert_ne!(
            context_compact_counter_file(&harness_home, &left.lane_key()),
            context_compact_counter_file(&harness_home, &right.lane_key())
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn context_rollover_config_separates_compact_count_from_continuation_depth() {
        let root = temp_root("config_separates_compact_count_from_continuation_depth");
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join(HARNESS_CONFIG_FILE_NAME),
            r#"{"codexContext":{"maxSuccessfulCompactsBeforeRollover":7,"maxContinuationDepth":3,"streamUnstableContinuationMinAttempts":4,"streamUnstableContinuationTokenLimit":123456}}"#,
        )
        .unwrap();

        let config = load_context_rollover_config(&harness_home).unwrap();

        assert_eq!(config.max_successful_compacts_before_rollover, 7);
        assert_eq!(config.max_continuation_depth, 3);
        assert_eq!(config.stream_unstable_continuation_min_attempts, 4);
        assert_eq!(config.stream_unstable_continuation_token_limit, 123456);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn compact_counter_counts_only_successful_compacts() {
        let root = temp_root("counter_success");
        let harness_home = root.join(".agent-harness");
        let lane = test_lane("telegram:dm:user:main");

        record_context_compact_attempt(ContextCompactAttemptOptions {
            harness_home: harness_home.clone(),
            lane: lane.clone(),
            compact_succeeded: false,
            rewrote_active_context: true,
            compact_thread_id: Some("thread-failed".to_string()),
            compact_attempt_key: None,
            max_successful_compacts_before_rollover: 2,
            now_ms: 10,
        })
        .unwrap();
        let counter = record_context_compact_attempt(ContextCompactAttemptOptions {
            harness_home: harness_home.clone(),
            lane,
            compact_succeeded: true,
            rewrote_active_context: true,
            compact_thread_id: Some("thread-ok".to_string()),
            compact_attempt_key: None,
            max_successful_compacts_before_rollover: 2,
            now_ms: 11,
        })
        .unwrap();

        assert_eq!(counter.successful_compact_count, 1);
        assert!(!counter.rollover_pending);
        assert_eq!(counter.last_compact_thread_id.as_deref(), Some("thread-ok"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn compact_counter_deduplicates_successful_attempt_key() {
        let root = temp_root("counter_idempotent_key");
        let harness_home = root.join(".agent-harness");
        let lane = test_lane("telegram:dm:user:main");
        let key = Some("queue-1:thread-ok:compact-before-turn".to_string());

        record_context_compact_attempt(ContextCompactAttemptOptions {
            harness_home: harness_home.clone(),
            lane: lane.clone(),
            compact_succeeded: true,
            rewrote_active_context: true,
            compact_thread_id: Some("thread-ok".to_string()),
            compact_attempt_key: key.clone(),
            max_successful_compacts_before_rollover: 2,
            now_ms: 10,
        })
        .unwrap();
        let counter = record_context_compact_attempt(ContextCompactAttemptOptions {
            harness_home: harness_home.clone(),
            lane,
            compact_succeeded: true,
            rewrote_active_context: true,
            compact_thread_id: Some("thread-ok".to_string()),
            compact_attempt_key: key.clone(),
            max_successful_compacts_before_rollover: 2,
            now_ms: 11,
        })
        .unwrap();

        assert_eq!(counter.successful_compact_count, 1);
        assert_eq!(counter.last_successful_compact_attempt_key, key);
        assert!(!counter.rollover_pending);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn compact_counter_reaches_limit_marks_rollover_pending() {
        let root = temp_root("counter_limit");
        let harness_home = root.join(".agent-harness");
        let lane = test_lane("telegram:dm:user:main");

        let counter = record_context_compact_attempt(ContextCompactAttemptOptions {
            harness_home: harness_home.clone(),
            lane,
            compact_succeeded: true,
            rewrote_active_context: true,
            compact_thread_id: Some("thread-ok".to_string()),
            compact_attempt_key: None,
            max_successful_compacts_before_rollover: 1,
            now_ms: 10,
        })
        .unwrap();

        assert_eq!(counter.successful_compact_count, 1);
        assert!(counter.rollover_pending);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn rollover_updates_channel_active_session_key_before_requeue() {
        let root = temp_root("before_turn_rekey");
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(harness_home.join("state").join("runtime-queue")).unwrap();
        fs::write(
            harness_home.join(HARNESS_CONFIG_FILE_NAME),
            r#"{"codexContext":{"maxSuccessfulCompactsBeforeRollover":1}}"#,
        )
        .unwrap();
        let old_session = "telegram:dm:user:main";
        let state_file = channel_session_state_file(&harness_home, "telegram", "dm", "user");
        fs::create_dir_all(state_file.parent().unwrap()).unwrap();
        write_json_atomic(
            &state_file,
            &ChannelSessionState {
                schema: "agent-harness.channel-session-state.v1".to_string(),
                platform: "telegram".to_string(),
                account_id: None,
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
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
                updated_at_ms: 0,
            },
        )
        .unwrap();
        let queue_file = harness_home
            .join("state")
            .join("runtime-queue")
            .join("pending.jsonl");
        append_jsonl_value(
            &queue_file,
            &serde_json::json!({
                "schema": "agent-harness.runtime-queue-item.v1",
                "queueId": "queue-1",
                "status": "queued",
                "runtimeClass": "interactive",
                "origin": "channel",
                "source": {
                    "kind": "channel",
                    "sourceHome": root,
                    "sourceWorkspace": root
                },
                "createdAtMs": 1,
                "agentId": "main",
                "sessionKey": old_session,
                "platform": "telegram",
                "channelId": "dm",
                "userId": "user",
                "messageText": "continue",
                "provider": null,
                "model": null,
                "promptFilesPresent": 0,
                "promptFilesTotal": 0,
                "selectedSkillIds": [],
                "plannedTranscriptFile": "old.jsonl",
                "plannedTrajectoryFile": "old.trajectory.jsonl"
            }),
        )
        .unwrap();
        record_context_compact_attempt(ContextCompactAttemptOptions {
            harness_home: harness_home.clone(),
            lane: test_lane(old_session),
            compact_succeeded: true,
            rewrote_active_context: true,
            compact_thread_id: Some("thread-ok".to_string()),
            compact_attempt_key: None,
            max_successful_compacts_before_rollover: 1,
            now_ms: 5,
        })
        .unwrap();

        let receipt = apply_context_rollover_before_turn(ContextRolloverBeforeTurnOptions {
            harness_home: harness_home.clone(),
            queue_id: "queue-1".to_string(),
            runtime_class: "interactive".to_string(),
            agent_id: "main".to_string(),
            platform: "telegram".to_string(),
            channel_id: "dm".to_string(),
            user_id: "user".to_string(),
            working_session_key: old_session.to_string(),
            now_ms: 6,
        })
        .unwrap();

        assert_eq!(receipt.status, ContextRolloverStatus::Applied);
        let state = read_channel_session_state(&harness_home, "telegram", "dm", "user")
            .unwrap()
            .unwrap();
        assert_eq!(state.active_session_key, "telegram:dm:user:main:cont-1");
        let queue_text = fs::read_to_string(queue_file).unwrap();
        assert!(queue_text.contains("\"sessionKey\":\"telegram:dm:user:main:cont-1\""));
        assert!(queue_text.contains("\"suppressSelfImprovement\":true"));
        assert!(receipt.working_set_file.unwrap().is_file());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn rollover_does_not_rewrite_prepared_queue_item() {
        let root = temp_root("prepared_guard");
        let harness_home = root.join(".agent-harness");
        let queue_dir = harness_home.join("state").join("runtime-queue");
        fs::create_dir_all(&queue_dir).unwrap();
        fs::write(
            harness_home.join(HARNESS_CONFIG_FILE_NAME),
            r#"{"codexContext":{"maxSuccessfulCompactsBeforeRollover":1}}"#,
        )
        .unwrap();
        append_jsonl_value(
            &queue_dir.join("execution-receipts.jsonl"),
            &serde_json::json!({"queueId":"queue-1","status":"prepared"}),
        )
        .unwrap();
        record_context_compact_attempt(ContextCompactAttemptOptions {
            harness_home: harness_home.clone(),
            lane: test_lane("telegram:dm:user:main"),
            compact_succeeded: true,
            rewrote_active_context: true,
            compact_thread_id: None,
            compact_attempt_key: None,
            max_successful_compacts_before_rollover: 1,
            now_ms: 5,
        })
        .unwrap();

        let receipt = apply_context_rollover_before_turn(ContextRolloverBeforeTurnOptions {
            harness_home,
            queue_id: "queue-1".to_string(),
            runtime_class: "interactive".to_string(),
            agent_id: "main".to_string(),
            platform: "telegram".to_string(),
            channel_id: "dm".to_string(),
            user_id: "user".to_string(),
            working_session_key: "telegram:dm:user:main".to_string(),
            now_ms: 6,
        })
        .unwrap();

        assert_eq!(receipt.status, ContextRolloverStatus::BlockedPrepared);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn rollover_does_not_rewrite_leased_queue_item() {
        let root = temp_root("leased_guard");
        let harness_home = root.join(".agent-harness");
        let queue_dir = harness_home.join("state").join("runtime-queue");
        fs::create_dir_all(queue_dir.join("classes").join("interactive")).unwrap();
        fs::write(
            harness_home.join(HARNESS_CONFIG_FILE_NAME),
            r#"{"codexContext":{"maxSuccessfulCompactsBeforeRollover":1}}"#,
        )
        .unwrap();
        write_json_atomic(
            &queue_dir
                .join("classes")
                .join("interactive")
                .join("runtime-leases.json"),
            &serde_json::json!({
                "schema": "agent-harness.runtime-queue-leases.v1",
                "leases": {"queue-1": {"queueId": "queue-1"}}
            }),
        )
        .unwrap();
        append_jsonl_value(
            &queue_dir.join("pending.jsonl"),
            &serde_json::json!({
                "schema": "agent-harness.runtime-queue-item.v1",
                "queueId": "queue-1",
                "status": "queued",
                "runtimeClass": "interactive",
                "origin": "channel",
                "source": {"kind": "channel", "sourceHome": root, "sourceWorkspace": root},
                "createdAtMs": 1,
                "agentId": "main",
                "sessionKey": "telegram:dm:user:main",
                "platform": "telegram",
                "channelId": "dm",
                "userId": "user",
                "messageText": "continue",
                "plannedTranscriptFile": "old.jsonl",
                "plannedTrajectoryFile": "old.trajectory.jsonl"
            }),
        )
        .unwrap();
        record_context_compact_attempt(ContextCompactAttemptOptions {
            harness_home: harness_home.clone(),
            lane: test_lane("telegram:dm:user:main"),
            compact_succeeded: true,
            rewrote_active_context: true,
            compact_thread_id: None,
            compact_attempt_key: None,
            max_successful_compacts_before_rollover: 1,
            now_ms: 5,
        })
        .unwrap();

        let receipt = apply_context_rollover_before_turn(ContextRolloverBeforeTurnOptions {
            harness_home,
            queue_id: "queue-1".to_string(),
            runtime_class: "interactive".to_string(),
            agent_id: "main".to_string(),
            platform: "telegram".to_string(),
            channel_id: "dm".to_string(),
            user_id: "user".to_string(),
            working_session_key: "telegram:dm:user:main".to_string(),
            now_ms: 6,
        })
        .unwrap();

        assert_eq!(receipt.status, ContextRolloverStatus::BlockedLeased);
        let queue_text =
            fs::read_to_string(root.join(".agent-harness/state/runtime-queue/pending.jsonl"))
                .unwrap();
        assert!(queue_text.contains("\"sessionKey\":\"telegram:dm:user:main\""));
        assert!(!queue_text.contains("cont-1"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prepared_requeue_preserves_old_execution_dir() {
        let root = temp_root("prepared_requeue");
        let harness_home = root.join(".agent-harness");
        let queue_dir = harness_home.join("state").join("runtime-queue");
        fs::create_dir_all(&queue_dir).unwrap();
        let old_execution_dir = queue_dir.join("executions").join("queue-1");
        fs::create_dir_all(&old_execution_dir).unwrap();
        let state_file = channel_session_state_file(&harness_home, "telegram", "dm", "user");
        fs::create_dir_all(state_file.parent().unwrap()).unwrap();
        write_json_atomic(
            &state_file,
            &ChannelSessionState {
                schema: "agent-harness.channel-session-state.v1".to_string(),
                platform: "telegram".to_string(),
                account_id: None,
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                active_session_key: "telegram:dm:user:assistant".to_string(),
                agent_id: Some("assistant".to_string()),
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
                updated_at_ms: 1,
            },
        )
        .unwrap();
        append_jsonl_value(
            &queue_dir.join("pending.jsonl"),
            &serde_json::json!({
                "schema": "agent-harness.runtime-queue-item.v1",
                "queueId": "queue-1",
                "status": "queued",
                "runtimeClass": "interactive",
                "origin": "channel",
                "source": {"kind": "channel", "sourceHome": root, "sourceWorkspace": root},
                "createdAtMs": 1,
                "agentId": "assistant",
                "sessionKey": "telegram:dm:user:assistant",
                "platform": "telegram",
                "channelId": "dm",
                "userId": "user",
                "messageText": "continue",
                "plannedTranscriptFile": "old.jsonl",
                "plannedTrajectoryFile": "old.trajectory.jsonl"
            }),
        )
        .unwrap();
        append_jsonl_value(
            &queue_dir.join("execution-receipts.jsonl"),
            &serde_json::json!({
                "queueId": "queue-1",
                "status": "prepared",
                "runtimeClass": "interactive",
                "origin": "channel",
                "executionDir": old_execution_dir,
                "promptBundleJson": "old-prompt-bundle.json"
            }),
        )
        .unwrap();

        let report = requeue_prepared_context_rollover(ContextRolloverRequeuePreparedOptions {
            harness_home: harness_home.clone(),
            queue_id: "queue-1".to_string(),
            new_working_session_key: "telegram:dm:user:assistant:cont-1".to_string(),
            reason: "operator rollover recovery".to_string(),
            now_ms: 42,
        })
        .unwrap();

        assert!(report.requeued);
        assert_eq!(report.requeued_queue_id, "queue-1:rollover-requeue-42");
        assert_eq!(
            report.prepared_execution_dir,
            Some(old_execution_dir.clone())
        );
        assert!(old_execution_dir.is_dir());
        let queue_text = fs::read_to_string(queue_dir.join("pending.jsonl")).unwrap();
        assert!(queue_text.contains("\"queueId\":\"queue-1:rollover-requeue-42\""));
        assert!(queue_text.contains("\"previousExecutionDir\""));
        assert!(queue_text.contains("assistant:cont-1"));
        let index_file =
            working_set_session_index_file(&harness_home, "telegram:dm:user:assistant:cont-1");
        assert!(index_file.is_file());
        let index: WorkingSetSessionIndex =
            serde_json::from_slice(&fs::read(index_file).unwrap()).unwrap();
        let working_set: ContextWorkingSetMemory =
            serde_json::from_slice(&fs::read(index.working_set_file).unwrap()).unwrap();
        assert_eq!(
            working_set
                .pending_queue_item
                .as_ref()
                .and_then(|value| { string_field(value, &["queueId", "queue_id"]) }),
            Some("queue-1:rollover-requeue-42")
        );
        assert!(
            working_set
                .decisions
                .iter()
                .any(|entry| entry.contains("operator rollover recovery"))
        );
        let run_once = fs::read_to_string(queue_dir.join("run-once-receipts.jsonl")).unwrap();
        assert!(run_once.contains("\"queueId\":\"queue-1\""));
        assert!(run_once.contains("\"status\":\"skipped\""));
        let state = read_channel_session_state(&harness_home, "telegram", "dm", "user")
            .unwrap()
            .unwrap();
        assert_eq!(
            state.active_session_key,
            "telegram:dm:user:assistant:cont-1"
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prepared_auto_requeue_blocks_parent_session_sibling() {
        let root = temp_root("prepared_auto_requeue_blocks_parent_session_sibling");
        let harness_home = root.join(".agent-harness");
        let queue_dir = harness_home.join("state").join("runtime-queue");
        fs::create_dir_all(&queue_dir).unwrap();
        let state_file = channel_session_state_file(&harness_home, "telegram", "dm", "user");
        fs::create_dir_all(state_file.parent().unwrap()).unwrap();
        write_json_atomic(
            &state_file,
            &ChannelSessionState {
                schema: "agent-harness.channel-session-state.v1".to_string(),
                platform: "telegram".to_string(),
                account_id: None,
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                active_session_key: "telegram:dm:user:main".to_string(),
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
                updated_at_ms: 1,
            },
        )
        .unwrap();
        for queue_id in ["queue-1", "queue-2"] {
            append_jsonl_value(
                &queue_dir.join("pending.jsonl"),
                &serde_json::json!({
                    "schema": "agent-harness.runtime-queue-item.v1",
                    "queueId": queue_id,
                    "status": "queued",
                    "runtimeClass": "interactive",
                    "origin": "channel",
                    "source": {"kind": "channel", "sourceHome": root, "sourceWorkspace": root},
                    "createdAtMs": 1,
                    "agentId": "main",
                    "sessionKey": "telegram:dm:user:main",
                    "platform": "telegram",
                    "channelId": "dm",
                    "userId": "user",
                    "messageText": "continue",
                    "plannedTranscriptFile": "old.jsonl",
                    "plannedTrajectoryFile": "old.trajectory.jsonl"
                }),
            )
            .unwrap();
        }
        append_jsonl_value(
            &queue_dir.join("execution-receipts.jsonl"),
            &serde_json::json!({
                "queueId": "queue-1",
                "status": "prepared",
                "runtimeClass": "interactive",
                "origin": "channel",
                "executionDir": queue_dir.join("executions").join("queue-1"),
                "promptBundleJson": "old-prompt-bundle.json"
            }),
        )
        .unwrap();

        let error = requeue_prepared_context_rollover_if_no_parent_siblings(
            ContextRolloverRequeuePreparedOptions {
                harness_home: harness_home.clone(),
                queue_id: "queue-1".to_string(),
                new_working_session_key: "telegram:dm:user:main:cont-1".to_string(),
                reason: "auto polluted thread recovery".to_string(),
                now_ms: 42,
            },
        )
        .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("another pending item"));
        let state = read_channel_session_state(&harness_home, "telegram", "dm", "user")
            .unwrap()
            .unwrap();
        assert_eq!(state.active_session_key, "telegram:dm:user:main");
        let queue_text = fs::read_to_string(queue_dir.join("pending.jsonl")).unwrap();
        assert!(!queue_text.contains(":cont-1"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn missing_virtual_session_fields_default_to_legacy_continuation_zero() {
        let metadata: RuntimeContinuationMetadata =
            serde_json::from_value(serde_json::json!({})).unwrap();

        assert_eq!(metadata.virtual_session_id, None);
        assert_eq!(metadata.continuation_index.unwrap_or(0), 0);
        assert!(!metadata.should_suppress_self_improvement());
    }

    #[test]
    fn working_set_includes_operation_plan_refs_and_static_record_refs() {
        let root = temp_root("working_set_refs");
        let harness_home = root.join(".agent-harness");
        let queue_dir = harness_home.join("state").join("runtime-queue");
        fs::create_dir_all(&queue_dir).unwrap();
        fs::write(
            harness_home.join(HARNESS_CONFIG_FILE_NAME),
            r#"{"codexContext":{"maxSuccessfulCompactsBeforeRollover":1}}"#,
        )
        .unwrap();
        let old_session = "telegram:dm:user:assistant";
        let state_file = channel_session_state_file(&harness_home, "telegram", "dm", "user");
        fs::create_dir_all(state_file.parent().unwrap()).unwrap();
        write_json_atomic(
            &state_file,
            &ChannelSessionState {
                schema: "agent-harness.channel-session-state.v1".to_string(),
                platform: "telegram".to_string(),
                account_id: None,
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                active_session_key: old_session.to_string(),
                agent_id: Some("assistant".to_string()),
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
                updated_at_ms: 0,
            },
        )
        .unwrap();
        fs::create_dir_all(harness_home.join("state/operation-plans/plan-1")).unwrap();
        write_json_atomic(
            &harness_home.join("state/operation-plans/plan-1/plan.json"),
            &serde_json::json!({
                "schema": "agent-harness.operation-plan.v1",
                "planId": "plan-1",
                "sessionKey": old_session,
                "agentId": "assistant",
                "goal": "finish rollover",
                "status": "open",
                "createdAtMs": 1,
                "updatedAtMs": 1,
                "version": 1
            }),
        )
        .unwrap();
        append_jsonl_value(
            &queue_dir.join("pending.jsonl"),
            &serde_json::json!({
                "schema": "agent-harness.runtime-queue-item.v1",
                "queueId": "queue-1",
                "status": "queued",
                "runtimeClass": "interactive",
                "origin": "channel",
                "source": {"kind": "channel", "sourceHome": root, "sourceWorkspace": root},
                "createdAtMs": 1,
                "agentId": "assistant",
                "sessionKey": old_session,
                "platform": "telegram",
                "channelId": "dm",
                "userId": "user",
                "messageText": "continue",
                "plannedTranscriptFile": "old.jsonl",
                "plannedTrajectoryFile": "old.trajectory.jsonl"
            }),
        )
        .unwrap();
        record_context_compact_attempt(ContextCompactAttemptOptions {
            harness_home: harness_home.clone(),
            lane: ContextRolloverLane {
                agent_id: "assistant".to_string(),
                working_session_key: old_session.to_string(),
                ..test_lane(old_session)
            },
            compact_succeeded: true,
            rewrote_active_context: true,
            compact_thread_id: Some("thread-ok".to_string()),
            compact_attempt_key: None,
            max_successful_compacts_before_rollover: 1,
            now_ms: 5,
        })
        .unwrap();

        let receipt = apply_context_rollover_before_turn(ContextRolloverBeforeTurnOptions {
            harness_home: harness_home.clone(),
            queue_id: "queue-1".to_string(),
            runtime_class: "interactive".to_string(),
            agent_id: "assistant".to_string(),
            platform: "telegram".to_string(),
            channel_id: "dm".to_string(),
            user_id: "user".to_string(),
            working_session_key: old_session.to_string(),
            now_ms: 6,
        })
        .unwrap();

        assert_eq!(receipt.status, ContextRolloverStatus::Applied);
        let working_set_file = receipt.working_set_file.unwrap();
        let working_set: ContextWorkingSetMemory =
            serde_json::from_slice(&fs::read(&working_set_file).unwrap()).unwrap();
        assert_eq!(
            working_set.active_plan_refs,
            vec!["operation-plan:plan-1:open".to_string()]
        );
        let refs = working_set.static_record_refs;
        assert!(
            refs.transcript_file
                .unwrap()
                .display()
                .to_string()
                .contains("assistant")
        );
        assert!(
            refs.prompt_bundle_json
                .unwrap()
                .display()
                .to_string()
                .contains("queue-1")
        );
        assert_eq!(refs.runtime_receipts.len(), 2);
        let episode_file = context_rollover_episode_index_file(
            &harness_home,
            receipt.virtual_session_id.as_deref().unwrap(),
        );
        assert!(episode_file.is_file());
        let episode_text = fs::read_to_string(episode_file).unwrap();
        assert!(episode_text.contains("\"queueId\":\"queue-1\""));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn three_turn_compact_rollover_replay_writes_continuation_working_set() {
        let root = temp_root("three_turn_compact_rollover_replay");
        let harness_home = root.join(".agent-harness");
        let queue_dir = harness_home.join("state").join("runtime-queue");
        fs::create_dir_all(&queue_dir).unwrap();
        fs::write(
            harness_home.join(HARNESS_CONFIG_FILE_NAME),
            r#"{"codexContext":{"maxSuccessfulCompactsBeforeRollover":2}}"#,
        )
        .unwrap();
        let old_session = "discord:channel-42:user-7:main";
        let state_file =
            channel_session_state_file(&harness_home, "discord", "channel-42", "user-7");
        fs::create_dir_all(state_file.parent().unwrap()).unwrap();
        write_json_atomic(
            &state_file,
            &ChannelSessionState {
                schema: "agent-harness.channel-session-state.v1".to_string(),
                platform: "discord".to_string(),
                account_id: None,
                channel_id: "channel-42".to_string(),
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
                thinking_enabled: true,
                thinking_level: Some("xhigh".to_string()),
                thinking_instruction: None,
                reasoning_preference: None,
                backend_reasoning_policy: None,
                fast_mode: None,
                stop_requested: false,
                stop_reason: None,
                steering_notes: Vec::new(),
                btw_notes: Vec::new(),
                last_command: None,
                updated_at_ms: 100,
            },
        )
        .unwrap();
        append_jsonl_value(
            &queue_dir.join("pending.jsonl"),
            &serde_json::json!({
                "schema": "agent-harness.runtime-queue-item.v1",
                "queueId": "queue-third-turn",
                "status": "queued",
                "runtimeClass": "interactive",
                "origin": "channel",
                "source": {"kind": "channel", "sourceHome": root, "sourceWorkspace": root},
                "createdAtMs": 300,
                "agentId": "main",
                "sessionKey": old_session,
                "platform": "discord",
                "channelId": "channel-42",
                "userId": "user-7",
                "messageText": "third high-context turn",
                "plannedTranscriptFile": "third.jsonl",
                "plannedTrajectoryFile": "third.trajectory.jsonl"
            }),
        )
        .unwrap();

        let lane = ContextRolloverLane {
            runtime_class: "interactive".to_string(),
            agent_id: "main".to_string(),
            platform: "discord".to_string(),
            channel_id: "channel-42".to_string(),
            user_id: "user-7".to_string(),
            working_session_key: old_session.to_string(),
            virtual_session_id: None,
            continuation_index: 0,
        };
        let first = record_context_compact_attempt(ContextCompactAttemptOptions {
            harness_home: harness_home.clone(),
            lane: lane.clone(),
            compact_succeeded: true,
            rewrote_active_context: true,
            compact_thread_id: Some("thread-after-first-compact".to_string()),
            compact_attempt_key: Some("queue-first:thread-original".to_string()),
            max_successful_compacts_before_rollover: 2,
            now_ms: 101,
        })
        .unwrap();
        assert_eq!(first.successful_compact_count, 1);
        assert!(!first.rollover_pending);
        let second = record_context_compact_attempt(ContextCompactAttemptOptions {
            harness_home: harness_home.clone(),
            lane,
            compact_succeeded: true,
            rewrote_active_context: true,
            compact_thread_id: Some("thread-after-second-compact".to_string()),
            compact_attempt_key: Some("queue-second:thread-after-first-compact".to_string()),
            max_successful_compacts_before_rollover: 2,
            now_ms: 201,
        })
        .unwrap();
        assert_eq!(second.successful_compact_count, 2);
        assert!(second.rollover_pending);

        let receipt = apply_context_rollover_before_turn(ContextRolloverBeforeTurnOptions {
            harness_home: harness_home.clone(),
            queue_id: "queue-third-turn".to_string(),
            runtime_class: "interactive".to_string(),
            agent_id: "main".to_string(),
            platform: "discord".to_string(),
            channel_id: "channel-42".to_string(),
            user_id: "user-7".to_string(),
            working_session_key: old_session.to_string(),
            now_ms: 301,
        })
        .unwrap();

        assert_eq!(receipt.status, ContextRolloverStatus::Applied);
        assert_eq!(receipt.previous_working_session_key, old_session);
        assert_eq!(
            receipt.new_working_session_key.as_deref(),
            Some("discord:channel-42:user-7:main:cont-1")
        );
        assert_eq!(receipt.continuation_index, 1);
        assert!(receipt.working_set_file.as_ref().unwrap().is_file());
        assert!(receipt.virtual_session_file.as_ref().unwrap().is_file());

        let state = read_channel_session_state(&harness_home, "discord", "channel-42", "user-7")
            .unwrap()
            .unwrap();
        assert_eq!(
            state.active_session_key,
            "discord:channel-42:user-7:main:cont-1"
        );
        assert_eq!(state.agent_id.as_deref(), Some("main"));
        let continuity = load_working_set_continuity_section(
            &harness_home,
            "discord:channel-42:user-7:main:cont-1",
        )
        .unwrap()
        .unwrap();
        assert!(continuity.contains("virtualSessionId:"));
        assert!(continuity.contains("workingSessionKey: discord:channel-42:user-7:main:cont-1"));
        assert!(continuity.contains("pendingQueueId: queue-third-turn"));

        let queue_text = fs::read_to_string(queue_dir.join("pending.jsonl")).unwrap();
        assert!(queue_text.contains("\"sessionKey\":\"discord:channel-42:user-7:main:cont-1\""));
        assert!(queue_text.contains("\"completionKind\":\"continuation-rollover\""));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn completed_turn_updates_root_working_set_snapshot() {
        let root = temp_root("completed_turn_updates_root_working_set_snapshot");
        let harness_home = root.join(".agent-harness");
        let session_key = "telegram:dm:user:main";
        let queue_dir = harness_home.join("state").join("runtime-queue");
        fs::create_dir_all(&queue_dir).unwrap();
        let run_once_file = queue_dir.join("run-once-receipts.jsonl");
        let outbox_file = harness_home
            .join("state")
            .join("channels")
            .join("outbox.jsonl");
        let completion_file = queue_dir
            .join("executions")
            .join("queue-1")
            .join("completion.json");

        let report =
            record_completed_turn_working_set_snapshot(CompletedTurnWorkingSetSnapshotOptions {
                harness_home: harness_home.clone(),
                platform: "telegram".to_string(),
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                agent_id: "main".to_string(),
                working_session_key: session_key.to_string(),
                queue_id: Some("queue-1".to_string()),
                message_text: Some(
                    "finish Zhongxiao Dunhua continuity plan\nextra ignored".to_string(),
                ),
                status: "completed".to_string(),
                run_once_receipt_file: Some(run_once_file.clone()),
                outbox_file: Some(outbox_file.clone()),
                completion_file: Some(completion_file.clone()),
                now_ms: 77,
            })
            .unwrap();

        assert_eq!(report.continuation_index, 0);
        assert!(report.working_set_file.is_file());
        assert!(report.virtual_session_file.is_file());
        assert!(working_set_session_index_file(&harness_home, session_key).is_file());
        let working_set: ContextWorkingSetMemory =
            serde_json::from_slice(&fs::read(&report.working_set_file).unwrap()).unwrap();
        assert_eq!(
            working_set.goal.objective.as_deref(),
            Some("finish Zhongxiao Dunhua continuity plan")
        );
        assert!(
            working_set
                .validation
                .iter()
                .any(|entry| entry.contains("queue-1") && entry.contains("completed"))
        );
        assert!(
            working_set
                .recent_files
                .iter()
                .any(|entry| entry.contains("outbox.jsonl"))
        );
        let continuity = load_working_set_continuity_section(&harness_home, session_key)
            .unwrap()
            .unwrap();
        assert!(continuity.contains("goalObjective: finish Zhongxiao Dunhua continuity plan"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn virtual_session_thread_backfill_updates_matching_working_session() {
        let root = temp_root("virtual_session_thread_backfill_updates_matching_session");
        let harness_home = root.join(".agent-harness");
        let session_key = "telegram:dm:user:main";
        let snapshot =
            record_completed_turn_working_set_snapshot(CompletedTurnWorkingSetSnapshotOptions {
                harness_home: harness_home.clone(),
                platform: "telegram".to_string(),
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                agent_id: "main".to_string(),
                working_session_key: session_key.to_string(),
                queue_id: Some("queue-thread".to_string()),
                message_text: Some("record thread id in virtual session".to_string()),
                status: "completed".to_string(),
                run_once_receipt_file: None,
                outbox_file: None,
                completion_file: None,
                now_ms: 77,
            })
            .unwrap();

        let updated =
            backfill_virtual_session_codex_thread_id(VirtualSessionThreadBackfillOptions {
                harness_home: harness_home.clone(),
                session_key: session_key.to_string(),
                thread_id: "thread-recorded".to_string(),
                now_ms: 88,
            })
            .unwrap()
            .expect("virtual-session record should be updated");

        assert_eq!(updated, snapshot.virtual_session_file);
        let record: ContextVirtualSessionRecord =
            serde_json::from_slice(&fs::read(updated).unwrap()).unwrap();
        let working_session = record
            .working_sessions
            .iter()
            .find(|session| session.session_key == session_key)
            .expect("working session ref");
        assert_eq!(
            working_session.codex_thread_id.as_deref(),
            Some("thread-recorded")
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn virtual_session_terminal_mark_closes_open_working_session() {
        let root = temp_root("virtual_session_terminal_mark_closes_open_working_session");
        let harness_home = root.join(".agent-harness");
        let session_key = "telegram:dm:user:main:cont-3";
        let snapshot =
            record_completed_turn_working_set_snapshot(CompletedTurnWorkingSetSnapshotOptions {
                harness_home: harness_home.clone(),
                platform: "telegram".to_string(),
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                agent_id: "main".to_string(),
                working_session_key: session_key.to_string(),
                queue_id: Some("queue-terminal".to_string()),
                message_text: Some("terminal continuation depth reached".to_string()),
                status: "active".to_string(),
                run_once_receipt_file: None,
                outbox_file: None,
                completion_file: None,
                now_ms: 77,
            })
            .unwrap();

        let updated = mark_virtual_session_terminal(VirtualSessionTerminalOptions {
            harness_home: harness_home.clone(),
            session_key: session_key.to_string(),
            ended_by: "max-continuation-depth:3".to_string(),
            now_ms: 88,
        })
        .unwrap()
        .expect("virtual-session record should be marked terminal-failed");

        assert_eq!(updated, snapshot.virtual_session_file);
        let record: ContextVirtualSessionRecord =
            serde_json::from_slice(&fs::read(updated).unwrap()).unwrap();
        assert_eq!(record.status, "terminal-failed");
        let working_session = record
            .working_sessions
            .iter()
            .find(|session| session.session_key == session_key)
            .expect("working session ref");
        assert_eq!(working_session.ended_at_ms, Some(88));
        assert_eq!(
            working_session.ended_by.as_deref(),
            Some("max-continuation-depth:3")
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn rollover_child_working_set_inherits_parent_goal_and_decisions() {
        let root = temp_root("rollover_child_working_set_inherits_parent_goal_and_decisions");
        let harness_home = root.join(".agent-harness");
        let queue_dir = harness_home.join("state").join("runtime-queue");
        fs::create_dir_all(&queue_dir).unwrap();
        fs::write(
            harness_home.join(HARNESS_CONFIG_FILE_NAME),
            r#"{"codexContext":{"maxSuccessfulCompactsBeforeRollover":1}}"#,
        )
        .unwrap();
        let old_session = "telegram:dm:user:main";
        let state_file = channel_session_state_file(&harness_home, "telegram", "dm", "user");
        fs::create_dir_all(state_file.parent().unwrap()).unwrap();
        write_json_atomic(
            &state_file,
            &ChannelSessionState {
                schema: "agent-harness.channel-session-state.v1".to_string(),
                platform: "telegram".to_string(),
                account_id: None,
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
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
                updated_at_ms: 1,
            },
        )
        .unwrap();

        let parent =
            record_completed_turn_working_set_snapshot(CompletedTurnWorkingSetSnapshotOptions {
                harness_home: harness_home.clone(),
                platform: "telegram".to_string(),
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                agent_id: "main".to_string(),
                working_session_key: old_session.to_string(),
                queue_id: Some("queue-parent".to_string()),
                message_text: Some("preserve the virtual session objective".to_string()),
                status: "completed".to_string(),
                run_once_receipt_file: None,
                outbox_file: None,
                completion_file: None,
                now_ms: 2,
            })
            .unwrap();
        let mut parent_memory: ContextWorkingSetMemory =
            serde_json::from_slice(&fs::read(&parent.working_set_file).unwrap()).unwrap();
        parent_memory.constraints.push("same lane only".to_string());
        parent_memory
            .decisions
            .push("use resolver envelope".to_string());
        parent_memory.blockers.push("needs live bake".to_string());
        write_json_atomic(&parent.working_set_file, &parent_memory).unwrap();

        append_jsonl_value(
            &queue_dir.join("pending.jsonl"),
            &serde_json::json!({
                "schema": "agent-harness.runtime-queue-item.v1",
                "queueId": "queue-child",
                "status": "queued",
                "runtimeClass": "interactive",
                "origin": "channel",
                "source": {"kind": "channel", "sourceHome": root, "sourceWorkspace": root},
                "createdAtMs": 3,
                "agentId": "main",
                "sessionKey": old_session,
                "platform": "telegram",
                "channelId": "dm",
                "userId": "user",
                "messageText": "continue",
                "plannedTranscriptFile": "child.jsonl",
                "plannedTrajectoryFile": "child.trajectory.jsonl"
            }),
        )
        .unwrap();
        record_context_compact_attempt(ContextCompactAttemptOptions {
            harness_home: harness_home.clone(),
            lane: test_lane(old_session),
            compact_succeeded: true,
            rewrote_active_context: true,
            compact_thread_id: Some("thread-after-compact".to_string()),
            compact_attempt_key: Some("queue-parent:thread".to_string()),
            max_successful_compacts_before_rollover: 1,
            now_ms: 4,
        })
        .unwrap();

        let receipt = apply_context_rollover_before_turn(ContextRolloverBeforeTurnOptions {
            harness_home: harness_home.clone(),
            queue_id: "queue-child".to_string(),
            runtime_class: "interactive".to_string(),
            agent_id: "main".to_string(),
            platform: "telegram".to_string(),
            channel_id: "dm".to_string(),
            user_id: "user".to_string(),
            working_session_key: old_session.to_string(),
            now_ms: 5,
        })
        .unwrap();

        let child: ContextWorkingSetMemory =
            serde_json::from_slice(&fs::read(receipt.working_set_file.unwrap()).unwrap()).unwrap();
        assert_eq!(
            child.goal.objective.as_deref(),
            Some("preserve the virtual session objective")
        );
        assert_eq!(child.constraints, vec!["same lane only".to_string()]);
        assert!(
            child
                .decisions
                .contains(&"use resolver envelope".to_string())
        );
        assert!(
            child
                .decisions
                .iter()
                .any(|entry| entry.contains("context rollover"))
        );
        assert_eq!(child.blockers, vec!["needs live bake".to_string()]);
        let _ = fs::remove_dir_all(root);
    }
}
