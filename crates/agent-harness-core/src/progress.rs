use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config::harness_config_candidates;
use crate::write_json_atomic;

const AGENT_PROGRESS_EVENT_SCHEMA: &str = "agent-harness.progress-event.v1";
const AGENT_PROGRESS_DELIVERY_PLAN_SCHEMA: &str = "agent-harness.progress-delivery-plan.v1";
const AGENT_PROGRESS_DELIVERY_STATE_SCHEMA: &str = "agent-harness.progress-delivery-state.v1";
const AGENT_PROGRESS_DELIVERY_RECEIPT_SCHEMA: &str = "agent-harness.progress-delivery-receipt.v1";
const DEFAULT_PREVIEW_CHARS: usize = 120;
const DEFAULT_CURRENT_STEP_CHARS: usize = 1200;
const DEFAULT_MAX_NONTERMINAL_UPDATES_PER_LANE: usize = 6;
const DEFAULT_STATUS_HEARTBEAT_AFTER_BODY_CAP_MS: i64 = 5 * 60 * 1000;
const TERMINAL_PROGRESS_STATE_RETENTION_MS: i64 = 10 * 60 * 1000;
const SENSITIVE_PREVIEW: &str = "[redacted sensitive preview]";

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
    pub read_from_byte: u64,
    pub read_to_byte: u64,
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
    pub terminal: bool,
    pub text: String,
    pub text_hash: String,
    pub started_at_ms: i64,
    pub latest_at_ms: i64,
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
    pub text_hash: String,
    pub terminal: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_decision: Option<String>,
    pub error: Option<String>,
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
}

impl Default for AgentProgressDeliveryState {
    fn default() -> Self {
        Self {
            schema: AGENT_PROGRESS_DELIVERY_STATE_SCHEMA.to_string(),
            queues: BTreeMap::new(),
            ledger: AgentProgressDeliveryLedgerCursor::default(),
            compacted_events: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AgentProgressDeliveryLedgerCursor {
    #[serde(default)]
    offset_bytes: u64,
    #[serde(default)]
    line_number: usize,
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
    #[serde(default, skip_serializing_if = "is_zero_usize")]
    status_last_event_line: usize,
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
    provider_message_id: Option<String>,
    #[serde(default, skip_serializing_if = "is_zero_usize")]
    last_event_line: usize,
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
            status_last_event_line: 0,
            body_last_text_hash: String::new(),
            status_last_text_hash: String::new(),
            body_last_sent_at_ms: 0,
            status_last_sent_at_ms: 0,
            body_nonterminal_deliveries: 0,
            status_nonterminal_deliveries: 0,
            body_terminal: false,
            status_terminal: false,
            terminal_event_line: 0,
            provider_message_id: None,
            last_event_line: 0,
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
        match message_kind {
            AgentProgressDeliveryMessageKind::Body => {
                self.body_provider_message_id = provider_message_id;
                self.body_last_event_line = event_line;
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
        }
        self.terminal = self.terminal || terminal;
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StoredProgressEvent {
    line_number: usize,
    event: AgentProgressEvent,
}

#[derive(Debug, Clone)]
struct ProgressEventReadResult {
    events: Vec<StoredProgressEvent>,
    cursor: AgentProgressDeliveryLedgerCursor,
    reset: bool,
    new_events: usize,
    read_from_byte: u64,
    read_to_byte: u64,
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
}

pub fn agent_progress_events_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("runtime-queue")
        .join("progress-events.jsonl")
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

pub fn append_agent_progress_event(
    harness_home: impl AsRef<Path>,
    event: &AgentProgressEvent,
) -> io::Result<PathBuf> {
    let harness_home = harness_home.as_ref();
    let file = agent_progress_events_file(harness_home);
    append_json_line(&file, event)?;
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
    let status_heartbeat_after_body_cap_ms = mute_config
        .status_heartbeat_after_body_cap_ms
        .unwrap_or(options.status_heartbeat_after_body_cap_ms)
        .max(0);
    let mut state = read_delivery_state(&state_file, &mut warnings)?;
    prune_old_terminal_delivery_state(&mut state, options.now_ms);
    let read_result =
        read_progress_events_since_cursor(&events_file, &state.ledger, &mut warnings)?;
    let cached_events = if read_result.reset {
        BTreeMap::new()
    } else {
        state.compacted_events.clone()
    };
    let cached_event_count = cached_events.values().map(Vec::len).sum();
    let mut all_by_queue = cached_events;
    for stored in read_result.events {
        all_by_queue
            .entry(stored.event.queue_id.clone())
            .or_default()
            .push(stored);
    }
    for queue_events in all_by_queue.values_mut() {
        queue_events.sort_by_key(|stored| stored.line_number);
    }
    let mut summary = AgentProgressDeliveryPlanSummary {
        total_events: all_by_queue.values().map(Vec::len).sum(),
        new_events: read_result.new_events,
        cached_events: cached_event_count,
        read_from_byte: read_result.read_from_byte,
        read_to_byte: read_result.read_to_byte,
        invalid_lines: warnings
            .iter()
            .filter(|warning| warning.contains("progress event line"))
            .count(),
        ..AgentProgressDeliveryPlanSummary::default()
    };
    let mut by_queue = BTreeMap::<String, Vec<StoredProgressEvent>>::new();
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
            if !progress_delivery_enabled_for_event(&mute_config, &stored.event) {
                summary.skipped_muted += 1;
                continue;
            }
            kept.push(stored);
        }
        if !kept.is_empty() {
            by_queue.insert(queue_id, kept);
        }
    }
    state.ledger = read_result.cursor;
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
        let body_cap_reached = cursor.is_some_and(|cursor| {
            !terminal
                && max_nonterminal_updates_per_lane > 0
                && cursor.body_nonterminal_deliveries >= max_nonterminal_updates_per_lane
                && cursor
                    .provider_message_id_for(AgentProgressDeliveryMessageKind::Body)
                    .is_some()
        });
        let latest_current_step_line = latest_current_step_stored_line(&queue_events);
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
                let terminal_closed_event_line = cursor.terminal_closed_event_line();
                if cursor.terminal_recorded_for(message_kind)
                    || (terminal_closed_event_line > 0
                        && latest.line_number > terminal_closed_event_line)
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
                    < progress_delivery_min_interval_for_lane(
                        message_kind,
                        body_cap_reached,
                        options.min_update_interval_ms,
                        status_heartbeat_after_body_cap_ms,
                    )
                && !status_has_new_current_step_after_body_cap(
                    message_kind,
                    body_cap_reached,
                    latest_current_step_line,
                    cursor,
                )
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
            let action = if provider_message_id.is_some() {
                AgentProgressDeliveryAction::Edit
            } else {
                AgentProgressDeliveryAction::Send
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
                terminal,
                text,
                text_hash,
                started_at_ms: first.event.at_ms,
                latest_at_ms: latest.event.at_ms,
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

fn latest_current_step_stored_line(queue_events: &[StoredProgressEvent]) -> Option<usize> {
    queue_events
        .iter()
        .rev()
        .find(|stored| stored.event.kind == AgentProgressKind::AssistantNarration)
        .map(|stored| stored.line_number)
}

fn status_has_new_current_step_after_body_cap(
    message_kind: AgentProgressDeliveryMessageKind,
    body_cap_reached: bool,
    latest_current_step_line: Option<usize>,
    cursor: &AgentProgressDeliveryCursor,
) -> bool {
    message_kind == AgentProgressDeliveryMessageKind::Status
        && body_cap_reached
        && latest_current_step_line.is_some_and(|line| line > cursor.status_last_event_line)
}

fn progress_delivery_min_interval_for_lane(
    message_kind: AgentProgressDeliveryMessageKind,
    body_cap_reached: bool,
    min_update_interval_ms: i64,
    status_heartbeat_after_body_cap_ms: i64,
) -> i64 {
    if message_kind == AgentProgressDeliveryMessageKind::Status && body_cap_reached {
        status_heartbeat_after_body_cap_ms
    } else {
        min_update_interval_ms
    }
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
    let state_file = agent_progress_delivery_state_file(&options.harness_home);
    let receipts_file = agent_progress_delivery_receipts_file(&options.harness_home);
    let mut warnings = Vec::new();
    let mut state = read_delivery_state(&state_file, &mut warnings)?;
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
        text_hash: options.text_hash,
        terminal: options.terminal,
        policy_decision: options.policy_decision,
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
        cursor.record_lane(
            receipt.message_kind,
            receipt.provider_message_id.clone(),
            receipt.event_line,
            receipt.text_hash.clone(),
            receipt.at_ms,
            receipt.terminal,
            receipt.status == AgentProgressDeliveryStatus::Delivered,
        );
        write_delivery_state(&state_file, &state)?;
    }
    append_json_line(&receipts_file, &receipt)?;
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

fn read_progress_events_since_cursor(
    events_file: &Path,
    cursor: &AgentProgressDeliveryLedgerCursor,
    warnings: &mut Vec<String>,
) -> io::Result<ProgressEventReadResult> {
    if !events_file.is_file() {
        return Ok(ProgressEventReadResult {
            events: Vec::new(),
            cursor: AgentProgressDeliveryLedgerCursor::default(),
            reset: cursor.offset_bytes > 0 || cursor.line_number > 0,
            new_events: 0,
            read_from_byte: 0,
            read_to_byte: 0,
        });
    }
    let file = File::open(events_file)?;
    let len = file.metadata()?.len();
    let reset = cursor.offset_bytes > len;
    let read_from_byte = if reset { 0 } else { cursor.offset_bytes };
    let mut reader = BufReader::new(file);
    reader.seek(SeekFrom::Start(read_from_byte))?;
    let mut line_number = if reset { 0 } else { cursor.line_number };
    let mut offset_bytes = read_from_byte;
    let mut events = Vec::new();
    loop {
        let mut line = String::new();
        let bytes_read = reader.read_line(&mut line)?;
        if bytes_read == 0 {
            break;
        }
        line_number += 1;
        offset_bytes = offset_bytes.saturating_add(bytes_read as u64);
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        match serde_json::from_str::<AgentProgressEvent>(trimmed) {
            Ok(event) => events.push(StoredProgressEvent { line_number, event }),
            Err(error) => warnings.push(format!(
                "progress event line {} is not valid JSON: {}",
                line_number, error
            )),
        }
    }
    let new_events = events.len();
    Ok(ProgressEventReadResult {
        events,
        cursor: AgentProgressDeliveryLedgerCursor {
            offset_bytes,
            line_number,
        },
        reset,
        new_events,
        read_from_byte,
        read_to_byte: offset_bytes,
    })
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

fn write_delivery_state(state_file: &Path, state: &AgentProgressDeliveryState) -> io::Result<()> {
    write_json_atomic(state_file, state)
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

fn fnv1a_64_hex(value: &str) -> String {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
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
    use std::time::{SystemTime, UNIX_EPOCH};

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
        assert!(limited.pending.is_empty());
        assert_eq!(limited.summary.volume_limited, 1);
        assert_eq!(limited.summary.rate_limited, 1);

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
    fn delivery_plan_status_updates_immediately_for_new_current_step_after_body_cap() {
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
        assert!(capped.pending.is_empty());

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

        assert!(limited.pending.is_empty());
        assert_eq!(limited.summary.volume_limited, 1);
        assert_eq!(limited.summary.rate_limited, 1);

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
                "progressDeliveryChannelModes": { "telegram:-1003968507595": "on" }
              }
            }"#,
        )
        .unwrap();
        let mut context = context();
        context.agent_id = Some("xiaoxiaoli".to_string());
        context.channel_id = "-1003968507595".to_string();
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
