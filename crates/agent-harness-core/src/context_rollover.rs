use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::channel_state::{channel_session_state_file, read_channel_session_state};
use crate::config::HARNESS_CONFIG_FILE_NAME;
use crate::{append_jsonl_value, write_json_atomic};

const CONTEXT_COMPACT_COUNTER_SCHEMA: &str = "agent-harness.context-compact-counter.v1";
const VIRTUAL_SESSION_SCHEMA: &str = "agent-harness.virtual-session.v1";
const WORKING_SET_MEMORY_SCHEMA: &str = "agent-harness.working-set-memory.v1";
const CONTEXT_ROLLOVER_RECEIPT_SCHEMA: &str = "agent-harness.context-rollover-receipt.v1";
const WORKING_SET_SESSION_INDEX_SCHEMA: &str = "agent-harness.working-set-session-index.v1";
const CONTEXT_ROLLOVER_EPISODE_SCHEMA: &str = "agent-harness.context-rollover-episode.v1";
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
    pub rollover_mode: ContextRolloverMode,
    pub cooperative_mid_turn_drain: bool,
}

impl Default for ContextRolloverConfig {
    fn default() -> Self {
        Self {
            max_successful_compacts_before_rollover: 2,
            rollover_mode: ContextRolloverMode::WorkingSetMemory,
            cooperative_mid_turn_drain: false,
        }
    }
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
struct WorkingSetSessionIndex {
    schema: String,
    session_key: String,
    virtual_session_id: String,
    continuation_index: u64,
    working_set_file: PathBuf,
    updated_at_ms: i64,
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

    if queue_item_has_prepared_receipt(
        &queue_dir.join("execution-receipts.jsonl"),
        &options.queue_id,
    )? {
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
    Ok(Some(render_working_set_continuity(
        &index.working_set_file,
        &working_set,
    )))
}

pub fn requeue_prepared_context_rollover(
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

    requeue_prepared_context_rollover(options)
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

fn queue_item_has_prepared_receipt(receipts_file: &Path, queue_id: &str) -> io::Result<bool> {
    if !receipts_file.is_file() {
        return Ok(false);
    }
    let text = fs::read_to_string(receipts_file)?;
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
            return Ok(true);
        }
    }
    Ok(false)
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

fn write_working_set_memory(
    harness_home: &Path,
    agent_id: &str,
    virtual_session_id: &str,
    working_session_key: &str,
    previous_working_session_key: Option<&str>,
    continuation_index: u64,
    pending_queue_item: Option<Value>,
    now_ms: i64,
) -> io::Result<PathBuf> {
    let file = working_set_file(harness_home, virtual_session_id, continuation_index);
    let (transcript_file, trajectory_file) =
        planned_session_files(harness_home, agent_id, working_session_key);
    let active_plan_refs =
        collect_active_operation_plan_refs(harness_home, agent_id, working_session_key)?;
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
            objective: None,
            status: "active".to_string(),
            budget_usage: None,
            completion_criteria: Vec::new(),
        },
        active_plan_refs,
        pending_queue_item,
        constraints: Vec::new(),
        decisions: Vec::new(),
        recent_files: Vec::new(),
        validation: Vec::new(),
        blockers: Vec::new(),
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

fn working_set_file(
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

fn virtual_session_file(harness_home: &Path, virtual_session_id: &str) -> PathBuf {
    harness_home
        .join("state")
        .join("context-rollover")
        .join("virtual-sessions")
        .join(format!("{}.json", safe_path_segment(virtual_session_id)))
}

fn derive_virtual_session_id(
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

fn collect_active_operation_plan_refs(
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

fn string_field<'a>(value: &'a Value, names: &[&str]) -> Option<&'a str> {
    names.iter().find_map(|name| value.get(*name)?.as_str())
}

fn path_string_field(value: &Value, names: &[&str]) -> Option<PathBuf> {
    string_field(value, names).map(PathBuf::from)
}

fn continuation_index_from_session_key(session_key: &str) -> Option<u64> {
    let (_, suffix) = session_key.rsplit_once(":cont-")?;
    suffix.parse::<u64>().ok()
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

fn safe_path_segment(value: &str) -> String {
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
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                active_session_key: old_session.to_string(),
                agent_id: Some("main".to_string()),
                provider: None,
                model: None,
                session_topic: None,
                model_override: None,
                model_override_provider: None,
                model_override_model: None,
                thinking_enabled: false,
                thinking_level: None,
                thinking_instruction: None,
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
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                active_session_key: "telegram:dm:user:assistant".to_string(),
                agent_id: Some("assistant".to_string()),
                provider: None,
                model: None,
                session_topic: None,
                model_override: None,
                model_override_provider: None,
                model_override_model: None,
                thinking_enabled: false,
                thinking_level: None,
                thinking_instruction: None,
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
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                active_session_key: "telegram:dm:user:main".to_string(),
                agent_id: Some("main".to_string()),
                provider: None,
                model: None,
                session_topic: None,
                model_override: None,
                model_override_provider: None,
                model_override_model: None,
                thinking_enabled: false,
                thinking_level: None,
                thinking_instruction: None,
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
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                active_session_key: old_session.to_string(),
                agent_id: Some("assistant".to_string()),
                provider: None,
                model: None,
                session_topic: None,
                model_override: None,
                model_override_provider: None,
                model_override_model: None,
                thinking_enabled: false,
                thinking_level: None,
                thinking_instruction: None,
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
}
