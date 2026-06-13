use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::write_json_atomic;

const AGENT_PROGRESS_EVENT_SCHEMA: &str = "agent-harness.progress-event.v1";
const AGENT_PROGRESS_DELIVERY_PLAN_SCHEMA: &str = "agent-harness.progress-delivery-plan.v1";
const AGENT_PROGRESS_DELIVERY_STATE_SCHEMA: &str = "agent-harness.progress-delivery-state.v1";
const AGENT_PROGRESS_DELIVERY_RECEIPT_SCHEMA: &str = "agent-harness.progress-delivery-receipt.v1";
const DEFAULT_PREVIEW_CHARS: usize = 120;
const DEFAULT_CURRENT_STEP_CHARS: usize = 1200;
const SENSITIVE_PREVIEW: &str = "[redacted sensitive preview]";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentProgressContext {
    pub queue_id: String,
    pub agent_id: Option<String>,
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
    pub queues: usize,
    pub pending: usize,
    pub delivered_current: usize,
    pub rate_limited: usize,
    pub invalid_lines: usize,
    pub skipped_platform: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentProgressDeliveryPending {
    pub queue_id: String,
    pub platform: String,
    pub channel_id: String,
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
    pub channel_id: String,
    pub user_id: String,
    pub session_key: String,
    pub message_kind: AgentProgressDeliveryMessageKind,
    pub action: AgentProgressDeliveryAction,
    pub status: AgentProgressDeliveryStatus,
    pub provider_message_id: Option<String>,
    pub event_line: usize,
    pub text_hash: String,
    pub terminal: bool,
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
    pub channel_id: String,
    pub user_id: String,
    pub session_key: String,
    pub message_kind: AgentProgressDeliveryMessageKind,
    pub action: AgentProgressDeliveryAction,
    pub status: AgentProgressDeliveryStatus,
    pub provider_message_id: Option<String>,
    pub event_line: usize,
    pub text_hash: String,
    pub terminal: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentProgressDeliveryStatus {
    Delivered,
    Failed,
    SkippedDenied,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AgentProgressDeliveryState {
    schema: String,
    #[serde(default)]
    queues: BTreeMap<String, AgentProgressDeliveryCursor>,
}

impl Default for AgentProgressDeliveryState {
    fn default() -> Self {
        Self {
            schema: AGENT_PROGRESS_DELIVERY_STATE_SCHEMA.to_string(),
            queues: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AgentProgressDeliveryCursor {
    platform: String,
    channel_id: String,
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
            channel_id,
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

    fn record_lane(
        &mut self,
        message_kind: AgentProgressDeliveryMessageKind,
        provider_message_id: Option<String>,
        event_line: usize,
        text_hash: String,
        sent_at_ms: i64,
        terminal: bool,
    ) {
        match message_kind {
            AgentProgressDeliveryMessageKind::Body => {
                self.body_provider_message_id = provider_message_id;
                self.body_last_event_line = event_line;
                self.body_last_text_hash = text_hash;
                self.body_last_sent_at_ms = sent_at_ms;
            }
            AgentProgressDeliveryMessageKind::Status => {
                self.status_provider_message_id = provider_message_id;
                self.status_last_event_line = event_line;
                self.status_last_text_hash = text_hash;
                self.status_last_sent_at_ms = sent_at_ms;
            }
        }
        self.terminal = self.terminal || terminal;
    }
}

#[derive(Debug, Clone)]
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
            at_ms,
            queue_id: context.queue_id.clone(),
            agent_id: context.agent_id.clone(),
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
    let file = agent_progress_events_file(harness_home);
    append_json_line(&file, event)?;
    Ok(file)
}

pub fn plan_agent_progress_delivery(
    options: AgentProgressDeliveryPlanOptions,
) -> io::Result<AgentProgressDeliveryPlanReport> {
    let events_file = agent_progress_events_file(&options.harness_home);
    let state_file = agent_progress_delivery_state_file(&options.harness_home);
    let receipts_file = agent_progress_delivery_receipts_file(&options.harness_home);
    let mut warnings = Vec::new();
    let events = read_progress_events(&events_file, &mut warnings)?;
    let state = read_delivery_state(&state_file, &mut warnings)?;
    let mut summary = AgentProgressDeliveryPlanSummary {
        total_events: events.len(),
        invalid_lines: warnings
            .iter()
            .filter(|warning| warning.contains("progress event line"))
            .count(),
        ..AgentProgressDeliveryPlanSummary::default()
    };
    let mut by_queue = BTreeMap::<String, Vec<StoredProgressEvent>>::new();
    for stored in events {
        if options
            .platform
            .as_ref()
            .is_some_and(|platform| platform != &stored.event.platform)
        {
            summary.skipped_platform += 1;
            continue;
        }
        by_queue
            .entry(stored.event.queue_id.clone())
            .or_default()
            .push(stored);
    }
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
                ),
            ),
        ];

        for (message_kind, text) in lanes {
            if text.trim().is_empty() {
                continue;
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
                    < options.min_update_interval_ms
            {
                summary.rate_limited += 1;
                continue;
            }
            let action = if provider_message_id.is_some() {
                AgentProgressDeliveryAction::Edit
            } else {
                AgentProgressDeliveryAction::Send
            };
            pending.push(AgentProgressDeliveryPending {
                queue_id: queue_id.clone(),
                platform: latest.event.platform.clone(),
                channel_id: latest.event.channel_id.clone(),
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
        channel_id: options.channel_id,
        user_id: options.user_id,
        session_key: options.session_key,
        message_kind: options.message_kind,
        action: options.action,
        status: options.status,
        provider_message_id: options.provider_message_id,
        event_line: options.event_line,
        text_hash: options.text_hash,
        terminal: options.terminal,
        error: options.error,
    };

    if matches!(
        receipt.status,
        AgentProgressDeliveryStatus::Delivered | AgentProgressDeliveryStatus::SkippedDenied
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
        cursor.channel_id = receipt.channel_id.clone();
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
        status
    }
}

fn is_rendered_action_event(event: &AgentProgressEvent) -> bool {
    !matches!(
        event.kind,
        AgentProgressKind::Runtime
            | AgentProgressKind::AssistantStream
            | AgentProgressKind::AssistantNarration
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

fn write_delivery_state(state_file: &Path, state: &AgentProgressDeliveryState) -> io::Result<()> {
    write_json_atomic(state_file, state)
}

fn append_json_line(path: &Path, value: &impl Serialize) -> io::Result<()> {
    crate::append_jsonl_value(path, value)
}

fn render_action_line(event: &AgentProgressEvent, max_preview_chars: usize) -> String {
    let preview = quote_safe_preview(&event.preview, max_preview_chars);
    format!(
        "{} {}: \"{}\"",
        progress_icon(event.kind),
        event.label,
        preview
    )
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
    let mentions_secret = [
        "token",
        "secret",
        "password",
        "passwd",
        "api_key",
        "apikey",
        "authorization",
        "bearer ",
    ]
    .iter()
    .any(|needle| lower.contains(needle));
    mentions_secret && (value.contains('=') || value.contains(':') || lower.contains("bearer "))
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
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn renders_actions_and_status_as_separate_messages() {
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
        let status = render_agent_progress_status(&refs, 9 * 60_000 + 1000, 120, 1200);
        let panel = render_agent_progress_panel(&refs, 9 * 60_000 + 1000, 8, 120);

        assert!(actions.contains("📚 skill_view: \"codebase-inspection\""));
        assert!(actions.contains("💻 terminal: \"cargo test -p agent-harness-core\""));
        assert!(!actions.contains("⏳ Working"));
        assert!(!actions.contains("assistant_stream"));
        assert_eq!(status, "⏳ Working — 9 min — running tools");
        assert_eq!(panel, format!("{actions}\n\n{status}"));
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
            render_agent_progress_status(&refs, 44 * 60_000 + 1000, 120, 1200),
            "✅ Done — 1 min"
        );
        assert_eq!(
            render_agent_progress_status(&refs, 45 * 60_000 + 1000, 120, 1200),
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
        let status = render_agent_progress_status(&refs, 61_000, 120, 1200);

        assert!(actions.contains("pwsh: agent-harness status"));
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
            render_agent_progress_status(&refs, 65_000, 120, 1200),
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
            render_agent_progress_status(&refs, 61_000, 120, 1200),
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
        let status = render_agent_progress_status(&refs, 61_000, 120, 1200);

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
        let status = render_agent_progress_status(&refs, 61_000, 24, 1200);
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

        let status = render_agent_progress_status(&refs, 61_000, 24, 200);

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
        assert_eq!(
            plan.pending[1].message_kind,
            AgentProgressDeliveryMessageKind::Status
        );
        assert_eq!(plan.pending[0].action, AgentProgressDeliveryAction::Send);
        assert_eq!(plan.pending[1].action, AgentProgressDeliveryAction::Send);

        for (index, pending) in plan.pending.iter().cloned().enumerate() {
            record_agent_progress_delivery(AgentProgressDeliveryRecordOptions {
                harness_home: harness_home.clone(),
                queue_id: pending.queue_id,
                platform: pending.platform,
                channel_id: pending.channel_id,
                user_id: pending.user_id,
                session_key: pending.session_key,
                message_kind: pending.message_kind,
                action: pending.action,
                status: AgentProgressDeliveryStatus::Delivered,
                provider_message_id: Some(format!("provider-{}", index + 1)),
                event_line: pending.event_line,
                text_hash: pending.text_hash,
                terminal: pending.terminal,
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
        assert_eq!(edit.pending[1].action, AgentProgressDeliveryAction::Edit);
        assert_eq!(
            edit.pending[0].provider_message_id.as_deref(),
            Some("provider-1")
        );
        assert_eq!(
            edit.pending[1].provider_message_id.as_deref(),
            Some("provider-2")
        );

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
                channel_id: pending.channel_id,
                user_id: pending.user_id,
                session_key: pending.session_key,
                message_kind: pending.message_kind,
                action: pending.action,
                status: AgentProgressDeliveryStatus::SkippedDenied,
                provider_message_id: None,
                event_line: pending.event_line,
                text_hash: pending.text_hash,
                terminal: pending.terminal,
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
                channel_id: pending.channel_id,
                user_id: pending.user_id,
                session_key: pending.session_key,
                message_kind: pending.message_kind,
                action: pending.action,
                status: AgentProgressDeliveryStatus::Delivered,
                provider_message_id: Some(format!("{:?}", pending.message_kind)),
                event_line: pending.event_line,
                text_hash: pending.text_hash,
                terminal: pending.terminal,
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
        assert!(!after_late_event.pending.is_empty());
        assert!(
            after_late_event
                .pending
                .iter()
                .all(|pending| pending.terminal)
        );
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
            render_agent_progress_status(event_refs.as_slice(), 4000, 120, 1200).contains("Failed")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn sensitive_preview_is_redacted() {
        let preview = sanitize_progress_preview("OPENAI_API_KEY=sk-abc cargo test", 120);
        assert_eq!(preview, SENSITIVE_PREVIEW);
    }

    fn context() -> AgentProgressContext {
        AgentProgressContext {
            queue_id: "turn:1".to_string(),
            agent_id: Some("main".to_string()),
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
