use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

const AGENT_PROGRESS_EVENT_SCHEMA: &str = "openclaw-harness.progress-event.v1";
const AGENT_PROGRESS_DELIVERY_PLAN_SCHEMA: &str = "openclaw-harness.progress-delivery-plan.v1";
const AGENT_PROGRESS_DELIVERY_STATE_SCHEMA: &str = "openclaw-harness.progress-delivery-state.v1";
const AGENT_PROGRESS_DELIVERY_RECEIPT_SCHEMA: &str =
    "openclaw-harness.progress-delivery-receipt.v1";
const DEFAULT_PREVIEW_CHARS: usize = 120;
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
    provider_message_id: Option<String>,
    last_event_line: usize,
    last_text_hash: String,
    last_sent_at_ms: i64,
    terminal: bool,
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
            preview: sanitize_progress_preview(preview.as_ref(), 512),
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
        let terminal = is_terminal_event(&latest.event);
        let text = render_agent_progress_panel(
            queue_events
                .iter()
                .map(|stored| &stored.event)
                .collect::<Vec<_>>()
                .as_slice(),
            options.now_ms,
            options.max_events_per_panel,
            options.max_preview_chars,
        );
        let text_hash = fnv1a_64_hex(&text);
        let cursor = state.queues.get(&queue_id);
        if cursor.is_some_and(|cursor| {
            cursor.last_event_line >= latest.line_number
                && cursor.last_text_hash == text_hash
                && cursor.terminal == terminal
        }) {
            summary.delivered_current += 1;
            continue;
        }
        if let Some(cursor) = cursor
            && cursor.provider_message_id.is_some()
            && !terminal
            && options.now_ms.saturating_sub(cursor.last_sent_at_ms)
                < options.min_update_interval_ms
        {
            summary.rate_limited += 1;
            continue;
        }
        let action = if cursor
            .and_then(|cursor| cursor.provider_message_id.as_ref())
            .is_some()
        {
            AgentProgressDeliveryAction::Edit
        } else {
            AgentProgressDeliveryAction::Send
        };
        pending.push(AgentProgressDeliveryPending {
            queue_id,
            platform: latest.event.platform.clone(),
            channel_id: latest.event.channel_id.clone(),
            user_id: latest.event.user_id.clone(),
            session_key: latest.event.session_key.clone(),
            action,
            provider_message_id: cursor.and_then(|cursor| cursor.provider_message_id.clone()),
            event_line: latest.line_number,
            terminal,
            text,
            text_hash,
            started_at_ms: first.event.at_ms,
            latest_at_ms: latest.event.at_ms,
        });
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
    let receipt = AgentProgressDeliveryReceipt {
        schema: AGENT_PROGRESS_DELIVERY_RECEIPT_SCHEMA.to_string(),
        at_ms: options.now_ms,
        queue_id: options.queue_id,
        platform: options.platform,
        channel_id: options.channel_id,
        user_id: options.user_id,
        session_key: options.session_key,
        action: options.action,
        status: options.status,
        provider_message_id: options.provider_message_id,
        event_line: options.event_line,
        text_hash: options.text_hash,
        terminal: options.terminal,
        error: options.error,
    };

    if receipt.status == AgentProgressDeliveryStatus::Delivered {
        state.queues.insert(
            receipt.queue_id.clone(),
            AgentProgressDeliveryCursor {
                platform: receipt.platform.clone(),
                channel_id: receipt.channel_id.clone(),
                user_id: receipt.user_id.clone(),
                session_key: receipt.session_key.clone(),
                provider_message_id: receipt.provider_message_id.clone(),
                last_event_line: receipt.event_line,
                last_text_hash: receipt.text_hash.clone(),
                last_sent_at_ms: receipt.at_ms,
                terminal: receipt.terminal,
            },
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
    if events.is_empty() {
        return "⏳ Working — <1 min — starting".to_string();
    }
    let first_at_ms = events.first().map(|event| event.at_ms).unwrap_or(now_ms);
    let latest = events.last().copied().unwrap_or(events[0]);
    let terminal = is_terminal_event(latest);
    let mut actions = Vec::<RenderedAction>::new();
    let start = events.len().saturating_sub(max_events.max(1));
    for event in events.iter().skip(start) {
        if event.kind == AgentProgressKind::Runtime {
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
    if !out.is_empty() {
        out.push_str("\n\n");
    }
    let elapsed = format_elapsed(now_ms.saturating_sub(first_at_ms));
    if terminal {
        match latest.status {
            AgentProgressStatus::Failed => {
                out.push_str(&format!(
                    "⚠️ Failed — {} — {}",
                    elapsed,
                    quote_safe_preview(&latest.preview, max_preview_chars)
                ));
            }
            _ => {
                out.push_str(&format!("✅ Done — {elapsed}"));
            }
        }
    } else {
        out.push_str(&format!(
            "⏳ Working — {} — {}",
            elapsed,
            status_phrase(latest)
        ));
    }
    out
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
    if let Some(parent) = state_file.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(
        state_file,
        serde_json::to_vec_pretty(state).map_err(io::Error::other)?,
    )
}

fn append_json_line(path: &Path, value: &impl Serialize) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    let line = serde_json::to_string(value).map_err(io::Error::other)?;
    writeln!(file, "{line}")?;
    Ok(())
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
    fn render_panel_keeps_status_line_at_bottom() {
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
                "cargo test -p openclaw-harness-core",
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
        let panel = render_agent_progress_panel(&refs, 9 * 60_000 + 1000, 8, 120);

        assert!(panel.contains("📚 skill_view: \"codebase-inspection\""));
        assert!(panel.contains("💻 terminal: \"cargo test -p openclaw-harness-core\""));
        assert!(panel.ends_with("⏳ Working — 9 min — receiving stream response"));
    }

    #[test]
    fn delivery_plan_uses_send_then_edit_and_rate_limits() {
        let root = temp_root("delivery_plan_uses_send_then_edit_and_rate_limits");
        let harness_home = root.join(".openclaw-harness");
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
        assert_eq!(plan.pending.len(), 1);
        assert_eq!(plan.pending[0].action, AgentProgressDeliveryAction::Send);

        let pending = plan.pending[0].clone();
        record_agent_progress_delivery(AgentProgressDeliveryRecordOptions {
            harness_home: harness_home.clone(),
            queue_id: pending.queue_id,
            platform: pending.platform,
            channel_id: pending.channel_id,
            user_id: pending.user_id,
            session_key: pending.session_key,
            action: pending.action,
            status: AgentProgressDeliveryStatus::Delivered,
            provider_message_id: Some("provider-1".to_string()),
            event_line: pending.event_line,
            text_hash: pending.text_hash,
            terminal: pending.terminal,
            error: None,
            now_ms: 2000,
        })
        .unwrap();

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
        assert_eq!(rate_limited.summary.rate_limited, 1);

        let edit = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
            harness_home,
            platform: Some("telegram".to_string()),
            now_ms: 5000,
            min_update_interval_ms: 2500,
            ..AgentProgressDeliveryPlanOptions::default()
        })
        .unwrap();
        assert_eq!(edit.pending.len(), 1);
        assert_eq!(edit.pending[0].action, AgentProgressDeliveryAction::Edit);
        assert_eq!(
            edit.pending[0].provider_message_id.as_deref(),
            Some("provider-1")
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
            "openclaw-harness-progress-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
