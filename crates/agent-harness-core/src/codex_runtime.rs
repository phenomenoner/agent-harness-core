use std::env;
use std::fs::{self, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    AgentProgressContext, AgentProgressEvent, AgentProgressKind, AgentProgressStatus,
    HarnessLogEvent, HarnessLogLevel, InboundMediaArtifact, InboundMediaInputPlan,
    InboundMediaInputPlanOptions, append_agent_progress_event, append_harness_log,
    config::{
        HarnessConfigValidationReport, HarnessConfigValidationStatus, validate_harness_config,
    },
    current_log_time_ms,
    live_control::{classify_approval_request, is_live_harness_home},
    plan_inbound_media_inputs, write_json_atomic,
};

const CODEX_RUNTIME_PLAN_SCHEMA: &str = "agent-harness.codex-runtime-plan.v1";
const CODEX_RUNTIME_PREFLIGHT_SCHEMA: &str = "agent-harness.codex-runtime-preflight.v1";
const CODEX_RUNTIME_LAUNCH_PROBE_SCHEMA: &str = "agent-harness.codex-runtime-launch-probe.v1";
const CODEX_RUNTIME_RUN_SCHEMA: &str = "agent-harness.codex-runtime-run.v1";
const CODEX_RUNTIME_COMPLETION_SCHEMA: &str = "agent-harness.codex-runtime-completion.v1";
const CODEX_CONTEXT_PREFLIGHT_SCHEMA: &str = "agent-harness.codex-context-preflight.v1";
const CODEX_CONTEXT_CHECKPOINT_SCHEMA: &str = "agent-harness.codex-context-checkpoint.v1";
const CODEX_CONTEXT_ROLLOVER_SCHEMA: &str = "agent-harness.codex-context-rollover.v1";
const CODEX_TRANSCRIPT_MESSAGE_SCHEMA: &str = "agent-harness.transcript-message.v1";
const CODEX_TRAJECTORY_EVENT_SCHEMA: &str = "agent-harness.trajectory-event.v1";
const CODEX_BINDING_SCHEMA: &str = "agent-harness.codex-binding.v1";
const CODEX_APP_SERVER_DEVELOPER_INSTRUCTIONS: &str = "\
This Codex app-server thread backs an imported agent harness session. Codex owns \
the backend system prompt, tool schemas, MCP/tool inventory, sandbox, approvals, \
and thread continuity. The chat-facing agent identity and operating context come \
from the Agent prompt bundle passed as turn input. Do not treat the Rust harness \
development repository as the chat user's agent identity.\n\n\
If you discover an agent-harness-core or live gateway bug while working for a \
chat user, do not patch or restart the live gateway from inside this session. \
Write a technical note describing the problem, user scenario, evidence, and \
recommended change, notify the user that an operator patch is needed, and pause \
the original task until the user gives further instructions.";
const HARNESS_CONFIG_FILE_NAME: &str = "harness-config.json";
pub(crate) const CODEX_APPROVAL_POLICY_ENV: &str = "AGENT_HARNESS_CODEX_APPROVAL_POLICY";
pub(crate) const CODEX_SANDBOX_ENV: &str = "AGENT_HARNESS_CODEX_SANDBOX";
pub(crate) const CODEX_SANDBOX_POLICY_ENV: &str = "AGENT_HARNESS_CODEX_SANDBOX_POLICY";
const DEFAULT_CODEX_SANDBOX: &str = "elevated";
const DEFAULT_CODEX_SANDBOX_POLICY: &str = "workspaceWrite";
const RUNTIME_CANCEL_REQUEST_MAX_AGE_MS: i64 = 60_000;
const CODEX_CHILD_TERMINATE_TIMEOUT_MS: u64 = 2_000;
const CODEX_STDOUT_READER_JOIN_TIMEOUT_MS: u64 = 2_000;
const DEFAULT_ASSISTANT_NARRATION_MAX_CHARS: usize = 1200;
const CODEX_THREAD_INLINE_IMAGE_LAST_TURN_BYTES_LIMIT: u64 = 1_000_000;
const CODEX_THREAD_INLINE_IMAGE_TOTAL_BYTES_LIMIT: u64 = 3_000_000;
const CODEX_THREAD_TOOL_OUTPUT_BYTES_LIMIT: u64 = 1_000_000;
const CODEX_ROLLOUT_SCAN_MAX_FILES: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexRuntimePlanOptions {
    pub harness_home: PathBuf,
    pub execution_dir: Option<PathBuf>,
    pub codex_executable: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexRuntimePreflightOptions {
    pub harness_home: PathBuf,
    pub execution_dir: Option<PathBuf>,
    pub plan_file: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexRuntimeLaunchProbeOptions {
    pub harness_home: PathBuf,
    pub execution_dir: Option<PathBuf>,
    pub plan_file: Option<PathBuf>,
    pub startup_probe_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexRuntimeRunOptions {
    pub harness_home: PathBuf,
    pub execution_dir: Option<PathBuf>,
    pub plan_file: Option<PathBuf>,
    pub timeout_ms: u64,
    pub idle_timeout_ms: u64,
    pub progress_context: Option<AgentProgressContext>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexRuntimeCompletionOptions {
    pub harness_home: PathBuf,
    pub execution_dir: Option<PathBuf>,
    pub plan_file: Option<PathBuf>,
    pub assistant_message: String,
    pub assistant_narration: Vec<CodexAssistantNarration>,
    pub assistant_narration_mode: AssistantNarrationMode,
    pub thread_id: Option<String>,
    pub finished_at_ms: i64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AssistantNarrationMode {
    Off,
    #[default]
    ProgressPanel,
    InlinePreface,
}

impl AssistantNarrationMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::ProgressPanel => "progress_panel",
            Self::InlinePreface => "inline_preface",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AssistantNarrationConfig {
    pub mode: AssistantNarrationMode,
    pub max_chars: usize,
    pub progress_min_update_ms: i64,
    pub final_prefix: String,
    pub source: String,
    pub configured: bool,
    pub config_file: PathBuf,
    pub warnings: Vec<String>,
}

impl Default for AssistantNarrationConfig {
    fn default() -> Self {
        Self {
            mode: AssistantNarrationMode::ProgressPanel,
            max_chars: DEFAULT_ASSISTANT_NARRATION_MAX_CHARS,
            progress_min_update_ms: 2_500,
            final_prefix: "Work log".to_string(),
            source: "default".to_string(),
            configured: false,
            config_file: PathBuf::new(),
            warnings: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexAssistantNarration {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_id: Option<String>,
    pub phase: String,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexRuntimePlanReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub execution_dir: Option<PathBuf>,
    pub plan_file: Option<PathBuf>,
    pub receipts_file: PathBuf,
    pub plan: Option<CodexRuntimePlan>,
    pub receipt: CodexRuntimeReceipt,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexRuntimePreflightReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub execution_dir: Option<PathBuf>,
    pub plan_file: Option<PathBuf>,
    pub preflight_file: Option<PathBuf>,
    pub receipts_file: PathBuf,
    pub receipt: CodexRuntimePreflightReceipt,
    pub checks: Vec<CodexRuntimePreflightCheck>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexRuntimeLaunchProbeReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub execution_dir: Option<PathBuf>,
    pub plan_file: Option<PathBuf>,
    pub preflight_file: Option<PathBuf>,
    pub launch_file: Option<PathBuf>,
    pub receipts_file: PathBuf,
    pub receipt: CodexRuntimeLaunchProbeReceipt,
    pub process: Option<CodexRuntimeLaunchProcess>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexRuntimeRunReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub execution_dir: Option<PathBuf>,
    pub plan_file: Option<PathBuf>,
    pub run_file: Option<PathBuf>,
    pub receipts_file: PathBuf,
    pub receipt: CodexRuntimeRunReceipt,
    pub completion: Option<CodexRuntimeCompletionReport>,
    pub stdout_log: Option<PathBuf>,
    pub stderr_log: Option<PathBuf>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexRuntimeCompletionReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub execution_dir: Option<PathBuf>,
    pub plan_file: Option<PathBuf>,
    pub completion_file: Option<PathBuf>,
    pub receipts_file: PathBuf,
    pub receipt: CodexRuntimeCompletionReceipt,
    pub transcript_file: Option<PathBuf>,
    pub trajectory_file: Option<PathBuf>,
    pub codex_binding_file: Option<PathBuf>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexRuntimePlan {
    pub queue_id: Option<String>,
    pub agent_id: Option<String>,
    pub session_key: String,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub prompt_bundle_json: PathBuf,
    pub prompt_markdown: PathBuf,
    pub media_plan: InboundMediaInputPlan,
    pub invocation: CodexInvocationPlan,
    pub outputs: CodexOutputPlan,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexInvocationPlan {
    pub executable: PathBuf,
    pub transport: CodexTransportPlan,
    pub arguments: Vec<String>,
    pub working_directory: PathBuf,
    pub codex_home: Option<PathBuf>,
    pub prompt_input_file: PathBuf,
    pub env_requirements: Vec<CodexEnvRequirement>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_config: Option<CodexProviderConfig>,
    pub model_argument: Option<String>,
    pub thread_id: Option<String>,
    #[serde(default)]
    pub app_server_approval_policy: String,
    #[serde(default)]
    pub app_server_sandbox: String,
    #[serde(default)]
    pub approval_policy: CodexApprovalPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexProviderConfig {
    pub provider: String,
    pub display_name: String,
    pub base_url: String,
    pub env_key: String,
    pub wire_api: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CodexTransportPlan {
    StdioJsonRpcAppServer,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CodexApprovalPolicy {
    #[default]
    Deny,
    Accept,
}

impl CodexApprovalPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Deny => "deny",
            Self::Accept => "accept",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexApprovalPolicyInspection {
    pub policy: CodexApprovalPolicy,
    pub source: String,
    pub configured: bool,
    pub config_file: PathBuf,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexSandboxInspection {
    pub sandbox: String,
    pub source: String,
    pub configured: bool,
    pub config_file: PathBuf,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexEnvRequirement {
    pub name: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexOutputPlan {
    pub transcript_file: PathBuf,
    pub trajectory_file: PathBuf,
    pub codex_binding_file: PathBuf,
    pub runtime_receipt_file: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexRuntimePreflightReceipt {
    pub queue_id: Option<String>,
    pub status: CodexRuntimePreflightStatus,
    pub execution_dir: Option<PathBuf>,
    pub plan_file: Option<PathBuf>,
    pub preflight_file: Option<PathBuf>,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CodexRuntimePreflightStatus {
    Ready,
    Blocked,
    NoRuntimePlan,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexRuntimePreflightCheck {
    pub name: String,
    pub status: CodexRuntimePreflightCheckStatus,
    pub detail: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CodexRuntimePreflightCheckStatus {
    Pass,
    Fail,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexRuntimeLaunchProbeReceipt {
    pub queue_id: Option<String>,
    pub status: CodexRuntimeLaunchProbeStatus,
    pub execution_dir: Option<PathBuf>,
    pub plan_file: Option<PathBuf>,
    pub launch_file: Option<PathBuf>,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexRuntimeCompletionReceipt {
    pub queue_id: Option<String>,
    pub status: CodexRuntimeCompletionStatus,
    pub execution_dir: Option<PathBuf>,
    pub plan_file: Option<PathBuf>,
    pub completion_file: Option<PathBuf>,
    pub transcript_file: Option<PathBuf>,
    pub trajectory_file: Option<PathBuf>,
    pub codex_binding_file: Option<PathBuf>,
    pub reason: String,
    #[serde(default)]
    pub assistant_narration_mode: AssistantNarrationMode,
    #[serde(default)]
    pub assistant_final_chars: usize,
    #[serde(default)]
    pub assistant_narration_items: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CodexRuntimeLaunchProbeStatus {
    StartedAndStopped,
    ExitedEarly,
    PreflightBlocked,
    NoRuntimePlan,
    SpawnFailed,
    TerminationFailed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CodexRuntimeCompletionStatus {
    Recorded,
    AlreadyRecorded,
    NoRuntimePlan,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexRuntimeLaunchProcess {
    pub executable: PathBuf,
    pub arguments: Vec<String>,
    pub working_directory: PathBuf,
    pub pid: Option<u32>,
    pub startup_probe_ms: u64,
    pub elapsed_ms: u128,
    pub exit_status: Option<String>,
    pub terminated: bool,
    pub stdout_log: Option<PathBuf>,
    pub stderr_log: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexRuntimeRunReceipt {
    pub queue_id: Option<String>,
    pub status: CodexRuntimeRunStatus,
    pub execution_dir: Option<PathBuf>,
    pub plan_file: Option<PathBuf>,
    pub run_file: Option<PathBuf>,
    pub completion_file: Option<PathBuf>,
    pub transcript_file: Option<PathBuf>,
    pub trajectory_file: Option<PathBuf>,
    pub codex_binding_file: Option<PathBuf>,
    pub reason: String,
    pub elapsed_ms: u128,
    pub event_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<CodexRuntimeUsage>,
    #[serde(default, skip_serializing_if = "InboundMediaInputPlan::is_empty")]
    pub media_plan: InboundMediaInputPlan,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_recovery: Option<CodexContextRecoveryReceipt>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexRuntimeUsage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub total_tokens: Option<u64>,
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexContextRecoveryReceipt {
    pub status: String,
    pub queue_id: Option<String>,
    pub session_key: String,
    pub original_thread_id: Option<String>,
    pub recovered_thread_id: Option<String>,
    pub official_compact_attempts: usize,
    pub retry_attempted: bool,
    pub fallback_policy: String,
    pub fresh_thread_attempted: bool,
    pub fresh_thread_succeeded: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_file: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rollover_file: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binding_backup_file: Option<PathBuf>,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct CodexContextPolicy {
    enabled: bool,
    prefer_official_compact: bool,
    auto_compact_before_turn: bool,
    retry_once_after_compact: bool,
    fallback_on_compact_failure: String,
    warn_at_active_context_ratio: f64,
    compact_at_active_context_ratio: f64,
    manual_recovery_allowed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    model_context_window: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    model_auto_compact_token_limit: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    model_auto_compact_token_limit_scope: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tool_output_token_limit: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    compact_prompt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    experimental_compact_prompt_file: Option<PathBuf>,
    source: String,
    configured: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct CodexThreadHealthReport {
    thread_id: Option<String>,
    rollout_file: Option<PathBuf>,
    data_image_count: usize,
    inline_image_bytes: u64,
    last_turn_inline_image_bytes: u64,
    oversized_tool_output_count: usize,
    oversized_tool_output_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    latest_usage: Option<CodexRuntimeUsage>,
    compact_recommended: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
}

impl Default for CodexContextPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            prefer_official_compact: true,
            auto_compact_before_turn: true,
            retry_once_after_compact: true,
            fallback_on_compact_failure: "checkpoint-and-new-thread".to_string(),
            warn_at_active_context_ratio: 0.75,
            compact_at_active_context_ratio: 0.85,
            manual_recovery_allowed: true,
            model_context_window: None,
            model_auto_compact_token_limit: None,
            model_auto_compact_token_limit_scope: None,
            tool_output_token_limit: None,
            compact_prompt: None,
            experimental_compact_prompt_file: None,
            source: "default".to_string(),
            configured: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct CodexContextPreflightReceipt {
    schema: &'static str,
    queue_id: Option<String>,
    session_key: String,
    agent_id: Option<String>,
    execution_dir: Option<PathBuf>,
    plan_file: PathBuf,
    preflight_file: Option<PathBuf>,
    prompt_markdown_bytes: u64,
    prompt_bundle_bytes: u64,
    transcript_lines: usize,
    transcript_bytes: u64,
    codex_binding_file: PathBuf,
    thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    latest_usage: Option<CodexRuntimeUsage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    model_context_window: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    active_context_ratio: Option<f64>,
    thread_health: CodexThreadHealthReport,
    compact_before_turn: bool,
    reason: String,
    policy: CodexContextPolicy,
}

struct CodexContextPreflightRun {
    receipt: CodexContextPreflightReceipt,
    warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct CodexContextCheckpoint {
    schema: &'static str,
    queue_id: Option<String>,
    session_key: String,
    agent_id: Option<String>,
    previous_thread_id: Option<String>,
    codex_binding_file: PathBuf,
    transcript_file: PathBuf,
    trajectory_file: PathBuf,
    prompt_bundle_json: PathBuf,
    prompt_markdown: PathBuf,
    reason: String,
    created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct CodexContextRolloverReceipt {
    schema: &'static str,
    queue_id: Option<String>,
    session_key: String,
    previous_thread_id: Option<String>,
    codex_binding_file: PathBuf,
    binding_backup_file: Option<PathBuf>,
    reason: String,
    created_at_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CodexRuntimeRunStatus {
    Completed,
    PreflightBlocked,
    NoRuntimePlan,
    SpawnFailed,
    ProtocolError,
    ContextExhausted,
    Timeout,
    Canceled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexRuntimeReceipt {
    pub queue_id: Option<String>,
    pub status: CodexRuntimeReceiptStatus,
    pub execution_dir: Option<PathBuf>,
    pub plan_file: Option<PathBuf>,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CodexRuntimeReceiptStatus {
    Planned,
    NoPreparedExecution,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CodexRuntimePlanFile {
    pub queue_id: Option<String>,
    pub agent_id: Option<String>,
    pub session_key: String,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub prompt_bundle_json: PathBuf,
    pub prompt_markdown: PathBuf,
    #[serde(default)]
    pub media_plan: InboundMediaInputPlan,
    pub invocation: CodexInvocationPlan,
    pub outputs: CodexOutputPlan,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct CodexTranscriptMessage {
    schema: &'static str,
    queue_id: Option<String>,
    session_key: String,
    agent_id: Option<String>,
    role: &'static str,
    content: String,
    provider: Option<String>,
    model: Option<String>,
    source: &'static str,
    at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct CodexTrajectoryEvent {
    schema: &'static str,
    queue_id: Option<String>,
    session_key: String,
    agent_id: Option<String>,
    event: &'static str,
    role: Option<&'static str>,
    provider: Option<String>,
    model: Option<String>,
    at_ms: i64,
    detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct CodexBindingRecord {
    schema: &'static str,
    queue_id: Option<String>,
    session_key: String,
    agent_id: Option<String>,
    provider: Option<String>,
    model: Option<String>,
    thread_id: Option<String>,
    working_directory: PathBuf,
    prompt_bundle_json: PathBuf,
    prompt_markdown: PathBuf,
    transcript_file: PathBuf,
    trajectory_file: PathBuf,
    completion_file: PathBuf,
    completed_at_ms: i64,
}

pub fn plan_codex_runtime(options: CodexRuntimePlanOptions) -> io::Result<CodexRuntimePlanReport> {
    ensure_harness_config_valid(&options.harness_home)?;
    let queue_dir = options.harness_home.join("state").join("runtime-queue");
    let receipts_file = queue_dir.join("codex-runtime-receipts.jsonl");
    fs::create_dir_all(&queue_dir)?;
    let mut warnings = Vec::new();

    let Some(execution_dir) = resolve_execution_dir(&options, &mut warnings)? else {
        let receipt = CodexRuntimeReceipt {
            queue_id: None,
            status: CodexRuntimeReceiptStatus::NoPreparedExecution,
            execution_dir: None,
            plan_file: None,
            reason: "no prepared runtime execution directory found".to_string(),
        };
        append_json_line(&receipts_file, &receipt)?;
        return Ok(CodexRuntimePlanReport {
            schema: CODEX_RUNTIME_PLAN_SCHEMA,
            harness_home: options.harness_home,
            execution_dir: None,
            plan_file: None,
            receipts_file,
            plan: None,
            receipt,
            warnings,
        });
    };

    let prepared_receipt = read_json_file(&execution_dir.join("execution-receipt.json"))?;
    let prompt_bundle_json = path_field(
        &prepared_receipt,
        &["promptBundleJson", "prompt_bundle_json"],
    )
    .unwrap_or_else(|| execution_dir.join("prompt-bundle.json"));
    let prompt_markdown = path_field(&prepared_receipt, &["promptMarkdown", "prompt_markdown"])
        .unwrap_or_else(|| execution_dir.join("prompt.md"));
    let bundle = read_json_file(&prompt_bundle_json)?;
    let queue_id =
        string_field(&prepared_receipt, &["queueId", "queue_id"]).map(ToString::to_string);
    let session_key = string_field(&bundle, &["sessionKey", "session_key"])
        .unwrap_or("unknown")
        .to_string();
    let agent_id = string_field(&bundle, &["agentId", "agent_id"]).map(ToString::to_string);
    let provider = string_field(&bundle, &["provider"]).map(ToString::to_string);
    let model = string_field(&bundle, &["model"]).map(ToString::to_string);
    let inbound_media_artifacts =
        inbound_media_artifacts_from_prepared_receipt(&prepared_receipt, &mut warnings);
    let native_image_input_enabled = codex_native_media_input_enabled();
    let media_plan = plan_inbound_media_inputs(
        InboundMediaInputPlanOptions {
            harness_home: options.harness_home.clone(),
            native_image_input_enabled,
            vision_tool_available: true,
        },
        &inbound_media_artifacts,
    );
    warnings.extend(media_plan.warnings.clone());
    let transcript_file = transcript_file(&options.harness_home, agent_id.as_deref(), &session_key);
    let trajectory_file = trajectory_file(&transcript_file);
    let codex_binding_file = codex_binding_file(&transcript_file);
    let runtime_receipt_file = execution_dir.join("codex-runtime-receipt.json");
    let runtime_workspace = path_field(
        &prepared_receipt,
        &["runtimeWorkspace", "runtime_workspace"],
    );
    let working_directory = runtime_working_directory(
        runtime_workspace.as_deref(),
        &bundle,
        &execution_dir,
        &mut warnings,
    );
    let thread_id = read_existing_codex_thread_id(&codex_binding_file, &mut warnings)?;
    let executable = options
        .codex_executable
        .unwrap_or_else(|| PathBuf::from("codex"));
    let approval_policy = resolve_codex_approval_policy(&options.harness_home, &mut warnings);
    let app_server_approval_policy = if is_live_harness_home(&options.harness_home) {
        "on-request".to_string()
    } else {
        codex_app_server_approval_policy(approval_policy).to_string()
    };
    let app_server_sandbox = resolve_codex_sandbox_policy(&options.harness_home, &mut warnings);
    let provider_config = codex_provider_config(provider.as_deref());
    let codex_home = harness_codex_home(&options.harness_home, provider_config.as_ref());
    ensure_harness_codex_config(
        codex_home.as_deref(),
        &working_directory,
        &options.harness_home,
        provider_config.as_ref(),
        &mut warnings,
    )?;
    let invocation = CodexInvocationPlan {
        executable,
        transport: CodexTransportPlan::StdioJsonRpcAppServer,
        arguments: vec!["app-server".to_string()],
        working_directory,
        codex_home,
        prompt_input_file: prompt_markdown.clone(),
        env_requirements: env_requirements(provider.as_deref()),
        provider_config,
        model_argument: model.clone(),
        thread_id,
        app_server_approval_policy,
        app_server_sandbox,
        approval_policy,
    };
    let outputs = CodexOutputPlan {
        transcript_file,
        trajectory_file,
        codex_binding_file,
        runtime_receipt_file,
    };
    let plan = CodexRuntimePlan {
        queue_id: queue_id.clone(),
        agent_id,
        session_key,
        provider,
        model,
        prompt_bundle_json,
        prompt_markdown,
        media_plan,
        invocation,
        outputs,
    };
    let plan_file = execution_dir.join("codex-runtime-plan.json");
    let plan_json = serde_json::to_string_pretty(&plan).map_err(io::Error::other)?;
    fs::write(&plan_file, plan_json)?;
    let receipt = CodexRuntimeReceipt {
        queue_id,
        status: CodexRuntimeReceiptStatus::Planned,
        execution_dir: Some(execution_dir.clone()),
        plan_file: Some(plan_file.clone()),
        reason: "Codex app-server invocation planned; process not started".to_string(),
    };
    let receipt_json = serde_json::to_string_pretty(&receipt).map_err(io::Error::other)?;
    fs::write(&plan.outputs.runtime_receipt_file, receipt_json)?;
    append_json_line(&receipts_file, &receipt)?;

    Ok(CodexRuntimePlanReport {
        schema: CODEX_RUNTIME_PLAN_SCHEMA,
        harness_home: options.harness_home,
        execution_dir: Some(execution_dir),
        plan_file: Some(plan_file),
        receipts_file,
        plan: Some(plan),
        receipt,
        warnings,
    })
}

pub fn preflight_codex_runtime(
    options: CodexRuntimePreflightOptions,
) -> io::Result<CodexRuntimePreflightReport> {
    let queue_dir = options.harness_home.join("state").join("runtime-queue");
    let receipts_file = queue_dir.join("codex-runtime-preflight-receipts.jsonl");
    fs::create_dir_all(&queue_dir)?;
    let mut warnings = Vec::new();

    let Some(plan_file) = resolve_preflight_plan_file(&options, &mut warnings)? else {
        let receipt = CodexRuntimePreflightReceipt {
            queue_id: None,
            status: CodexRuntimePreflightStatus::NoRuntimePlan,
            execution_dir: None,
            plan_file: None,
            preflight_file: None,
            reason: "no codex runtime plan found; run codex-plan first".to_string(),
        };
        append_json_line(&receipts_file, &receipt)?;
        return Ok(CodexRuntimePreflightReport {
            schema: CODEX_RUNTIME_PREFLIGHT_SCHEMA,
            harness_home: options.harness_home,
            execution_dir: None,
            plan_file: None,
            preflight_file: None,
            receipts_file,
            receipt,
            checks: Vec::new(),
            warnings,
        });
    };

    let plan: CodexRuntimePlanFile = read_json_file_as(&plan_file)?;
    let execution_dir = plan_file
        .parent()
        .map(Path::to_path_buf)
        .or(options.execution_dir);
    let preflight_file = execution_dir
        .as_ref()
        .map(|dir| dir.join("codex-runtime-preflight.json"));
    let mut checks = Vec::new();
    checks.push(pass_check(
        "runtime-plan",
        format!("loaded {}", plan_file.display()),
    ));
    checks.push(check_executable(&plan.invocation.executable));
    checks.push(check_existing_file(
        "prompt-bundle-json",
        &plan.prompt_bundle_json,
    ));
    checks.push(check_existing_file(
        "prompt-markdown",
        &plan.prompt_markdown,
    ));
    checks.push(check_existing_file(
        "prompt-input-file",
        &plan.invocation.prompt_input_file,
    ));
    checks.extend(check_output_paths(&options.harness_home, &plan.outputs)?);
    checks.extend(check_env_requirements(
        &options.harness_home,
        &plan.invocation.env_requirements,
    ));
    let has_failures = checks
        .iter()
        .any(|check| check.status == CodexRuntimePreflightCheckStatus::Fail);
    let status = if has_failures {
        CodexRuntimePreflightStatus::Blocked
    } else {
        CodexRuntimePreflightStatus::Ready
    };
    let failed_count = checks
        .iter()
        .filter(|check| check.status == CodexRuntimePreflightCheckStatus::Fail)
        .count();
    let reason = match status {
        CodexRuntimePreflightStatus::Ready => {
            "codex runtime plan passed local preflight checks".to_string()
        }
        CodexRuntimePreflightStatus::Blocked => {
            format!("codex runtime preflight blocked by {failed_count} failed check(s)")
        }
        CodexRuntimePreflightStatus::NoRuntimePlan => unreachable!(),
    };
    let receipt = CodexRuntimePreflightReceipt {
        queue_id: plan.queue_id,
        status,
        execution_dir: execution_dir.clone(),
        plan_file: Some(plan_file.clone()),
        preflight_file: preflight_file.clone(),
        reason,
    };
    let report = CodexRuntimePreflightReport {
        schema: CODEX_RUNTIME_PREFLIGHT_SCHEMA,
        harness_home: options.harness_home,
        execution_dir,
        plan_file: Some(plan_file),
        preflight_file: preflight_file.clone(),
        receipts_file: receipts_file.clone(),
        receipt,
        checks,
        warnings,
    };
    if let Some(preflight_file) = preflight_file {
        let report_json = serde_json::to_string_pretty(&report).map_err(io::Error::other)?;
        fs::write(preflight_file, report_json)?;
    }
    append_json_line(&receipts_file, &report.receipt)?;

    Ok(report)
}

pub fn probe_codex_runtime_launch(
    options: CodexRuntimeLaunchProbeOptions,
) -> io::Result<CodexRuntimeLaunchProbeReport> {
    let queue_dir = options.harness_home.join("state").join("runtime-queue");
    let receipts_file = queue_dir.join("codex-runtime-launch-receipts.jsonl");
    fs::create_dir_all(&queue_dir)?;

    let preflight = preflight_codex_runtime(CodexRuntimePreflightOptions {
        harness_home: options.harness_home.clone(),
        execution_dir: options.execution_dir.clone(),
        plan_file: options.plan_file.clone(),
    })?;
    let launch_file = preflight
        .execution_dir
        .as_ref()
        .map(|dir| dir.join("codex-runtime-launch-probe.json"));
    let mut warnings = preflight.warnings.clone();

    match preflight.receipt.status {
        CodexRuntimePreflightStatus::NoRuntimePlan => {
            let receipt = CodexRuntimeLaunchProbeReceipt {
                queue_id: preflight.receipt.queue_id,
                status: CodexRuntimeLaunchProbeStatus::NoRuntimePlan,
                execution_dir: preflight.execution_dir,
                plan_file: preflight.plan_file,
                launch_file: launch_file.clone(),
                reason: "no codex runtime plan found; run codex-plan first".to_string(),
            };
            return write_launch_probe_report(
                CodexRuntimeLaunchProbeReport {
                    schema: CODEX_RUNTIME_LAUNCH_PROBE_SCHEMA,
                    harness_home: options.harness_home,
                    execution_dir: receipt.execution_dir.clone(),
                    plan_file: receipt.plan_file.clone(),
                    preflight_file: preflight.preflight_file,
                    launch_file,
                    receipts_file,
                    receipt,
                    process: None,
                    warnings,
                },
                true,
            );
        }
        CodexRuntimePreflightStatus::Blocked => {
            let receipt = CodexRuntimeLaunchProbeReceipt {
                queue_id: preflight.receipt.queue_id,
                status: CodexRuntimeLaunchProbeStatus::PreflightBlocked,
                execution_dir: preflight.execution_dir,
                plan_file: preflight.plan_file,
                launch_file: launch_file.clone(),
                reason: "codex runtime launch blocked by preflight failures".to_string(),
            };
            return write_launch_probe_report(
                CodexRuntimeLaunchProbeReport {
                    schema: CODEX_RUNTIME_LAUNCH_PROBE_SCHEMA,
                    harness_home: options.harness_home,
                    execution_dir: receipt.execution_dir.clone(),
                    plan_file: receipt.plan_file.clone(),
                    preflight_file: preflight.preflight_file,
                    launch_file,
                    receipts_file,
                    receipt,
                    process: None,
                    warnings,
                },
                true,
            );
        }
        CodexRuntimePreflightStatus::Ready => {}
    }

    let Some(plan_file) = preflight.plan_file.clone() else {
        warnings.push("preflight reported ready without a runtime plan file".to_string());
        let receipt = CodexRuntimeLaunchProbeReceipt {
            queue_id: preflight.receipt.queue_id,
            status: CodexRuntimeLaunchProbeStatus::NoRuntimePlan,
            execution_dir: preflight.execution_dir,
            plan_file: None,
            launch_file: launch_file.clone(),
            reason: "preflight reported ready without a runtime plan file".to_string(),
        };
        return write_launch_probe_report(
            CodexRuntimeLaunchProbeReport {
                schema: CODEX_RUNTIME_LAUNCH_PROBE_SCHEMA,
                harness_home: options.harness_home,
                execution_dir: receipt.execution_dir.clone(),
                plan_file: None,
                preflight_file: preflight.preflight_file,
                launch_file,
                receipts_file,
                receipt,
                process: None,
                warnings,
            },
            true,
        );
    };

    let plan: CodexRuntimePlanFile = read_json_file_as(&plan_file)?;
    let probe_result = spawn_launch_probe(&options.harness_home, &plan, options.startup_probe_ms)?;
    let status = match probe_result.status {
        LaunchProbeProcessStatus::StartedAndStopped => {
            CodexRuntimeLaunchProbeStatus::StartedAndStopped
        }
        LaunchProbeProcessStatus::ExitedEarly => CodexRuntimeLaunchProbeStatus::ExitedEarly,
        LaunchProbeProcessStatus::SpawnFailed => CodexRuntimeLaunchProbeStatus::SpawnFailed,
        LaunchProbeProcessStatus::TerminationFailed => {
            CodexRuntimeLaunchProbeStatus::TerminationFailed
        }
    };
    let reason = match status {
        CodexRuntimeLaunchProbeStatus::StartedAndStopped => {
            "codex app-server process started and was stopped after launch probe".to_string()
        }
        CodexRuntimeLaunchProbeStatus::ExitedEarly => {
            "codex app-server process exited before the launch probe window elapsed".to_string()
        }
        CodexRuntimeLaunchProbeStatus::SpawnFailed => {
            "failed to spawn codex app-server process".to_string()
        }
        CodexRuntimeLaunchProbeStatus::TerminationFailed => {
            "codex app-server process started but could not be terminated cleanly".to_string()
        }
        CodexRuntimeLaunchProbeStatus::PreflightBlocked
        | CodexRuntimeLaunchProbeStatus::NoRuntimePlan => unreachable!(),
    };
    let receipt = CodexRuntimeLaunchProbeReceipt {
        queue_id: plan.queue_id,
        status,
        execution_dir: preflight.execution_dir,
        plan_file: Some(plan_file),
        launch_file: launch_file.clone(),
        reason,
    };
    write_launch_probe_report(
        CodexRuntimeLaunchProbeReport {
            schema: CODEX_RUNTIME_LAUNCH_PROBE_SCHEMA,
            harness_home: options.harness_home,
            execution_dir: receipt.execution_dir.clone(),
            plan_file: receipt.plan_file.clone(),
            preflight_file: preflight.preflight_file,
            launch_file,
            receipts_file,
            receipt,
            process: Some(probe_result.process),
            warnings,
        },
        true,
    )
}

pub fn run_codex_runtime(options: CodexRuntimeRunOptions) -> io::Result<CodexRuntimeRunReport> {
    let queue_dir = options.harness_home.join("state").join("runtime-queue");
    let receipts_file = queue_dir.join("codex-runtime-run-receipts.jsonl");
    fs::create_dir_all(&queue_dir)?;

    let preflight = preflight_codex_runtime(CodexRuntimePreflightOptions {
        harness_home: options.harness_home.clone(),
        execution_dir: options.execution_dir.clone(),
        plan_file: options.plan_file.clone(),
    })?;
    let run_file = preflight
        .execution_dir
        .as_ref()
        .map(|dir| dir.join("codex-runtime-run.json"));
    let stdout_log = preflight
        .execution_dir
        .as_ref()
        .map(|dir| dir.join("codex-runtime-run.stdout.jsonl"));
    let stderr_log = preflight
        .execution_dir
        .as_ref()
        .map(|dir| dir.join("codex-runtime-run.stderr.log"));
    let mut warnings = preflight.warnings.clone();

    match preflight.receipt.status {
        CodexRuntimePreflightStatus::NoRuntimePlan => {
            let receipt = CodexRuntimeRunReceipt {
                queue_id: preflight.receipt.queue_id,
                status: CodexRuntimeRunStatus::NoRuntimePlan,
                execution_dir: preflight.execution_dir,
                plan_file: preflight.plan_file,
                run_file: run_file.clone(),
                completion_file: None,
                transcript_file: None,
                trajectory_file: None,
                codex_binding_file: None,
                reason: "no codex runtime plan found; run codex-plan first".to_string(),
                elapsed_ms: 0,
                event_count: 0,
                usage: None,
                media_plan: InboundMediaInputPlan::default(),
                context_recovery: None,
            };
            append_codex_run_log(
                &options.harness_home,
                HarnessLogLevel::Warn,
                "codex.run.no-plan",
                &receipt.reason,
                None,
                None,
            )?;
            return write_codex_runtime_run_report(
                CodexRuntimeRunReport {
                    schema: CODEX_RUNTIME_RUN_SCHEMA,
                    harness_home: options.harness_home,
                    execution_dir: receipt.execution_dir.clone(),
                    plan_file: receipt.plan_file.clone(),
                    run_file,
                    receipts_file,
                    receipt,
                    completion: None,
                    stdout_log,
                    stderr_log,
                    warnings,
                },
                true,
            );
        }
        CodexRuntimePreflightStatus::Blocked => {
            let receipt = CodexRuntimeRunReceipt {
                queue_id: preflight.receipt.queue_id,
                status: CodexRuntimeRunStatus::PreflightBlocked,
                execution_dir: preflight.execution_dir,
                plan_file: preflight.plan_file,
                run_file: run_file.clone(),
                completion_file: None,
                transcript_file: None,
                trajectory_file: None,
                codex_binding_file: None,
                reason: "codex runtime run blocked by preflight failures".to_string(),
                elapsed_ms: 0,
                event_count: 0,
                usage: None,
                media_plan: InboundMediaInputPlan::default(),
                context_recovery: None,
            };
            append_codex_run_log(
                &options.harness_home,
                HarnessLogLevel::Warn,
                "codex.run.preflight-blocked",
                &receipt.reason,
                None,
                None,
            )?;
            return write_codex_runtime_run_report(
                CodexRuntimeRunReport {
                    schema: CODEX_RUNTIME_RUN_SCHEMA,
                    harness_home: options.harness_home,
                    execution_dir: receipt.execution_dir.clone(),
                    plan_file: receipt.plan_file.clone(),
                    run_file,
                    receipts_file,
                    receipt,
                    completion: None,
                    stdout_log,
                    stderr_log,
                    warnings,
                },
                true,
            );
        }
        CodexRuntimePreflightStatus::Ready => {}
    }

    let Some(plan_file) = preflight.plan_file.clone() else {
        warnings.push("preflight reported ready without a runtime plan file".to_string());
        let receipt = CodexRuntimeRunReceipt {
            queue_id: preflight.receipt.queue_id,
            status: CodexRuntimeRunStatus::NoRuntimePlan,
            execution_dir: preflight.execution_dir,
            plan_file: None,
            run_file: run_file.clone(),
            completion_file: None,
            transcript_file: None,
            trajectory_file: None,
            codex_binding_file: None,
            reason: "preflight reported ready without a runtime plan file".to_string(),
            elapsed_ms: 0,
            event_count: 0,
            usage: None,
            media_plan: InboundMediaInputPlan::default(),
            context_recovery: None,
        };
        append_codex_run_log(
            &options.harness_home,
            HarnessLogLevel::Warn,
            "codex.run.no-plan",
            &receipt.reason,
            None,
            None,
        )?;
        return write_codex_runtime_run_report(
            CodexRuntimeRunReport {
                schema: CODEX_RUNTIME_RUN_SCHEMA,
                harness_home: options.harness_home,
                execution_dir: receipt.execution_dir.clone(),
                plan_file: None,
                run_file,
                receipts_file,
                receipt,
                completion: None,
                stdout_log,
                stderr_log,
                warnings,
            },
            true,
        );
    };

    let plan: CodexRuntimePlanFile = read_json_file_as(&plan_file)?;
    let execution_dir = plan_file.parent().map(Path::to_path_buf);
    if plan_completion_recorded(&plan)? {
        let receipt = CodexRuntimeRunReceipt {
            queue_id: plan.queue_id.clone(),
            status: CodexRuntimeRunStatus::Completed,
            execution_dir,
            plan_file: Some(plan_file),
            run_file: run_file.clone(),
            completion_file: Some(
                plan.invocation
                    .working_directory
                    .join("codex-runtime-completion-receipt.json"),
            ),
            transcript_file: Some(plan.outputs.transcript_file.clone()),
            trajectory_file: Some(plan.outputs.trajectory_file.clone()),
            codex_binding_file: Some(plan.outputs.codex_binding_file.clone()),
            reason: "codex runtime completion was already recorded; model request skipped"
                .to_string(),
            elapsed_ms: 0,
            event_count: 0,
            usage: None,
            media_plan: plan.media_plan.clone(),
            context_recovery: None,
        };
        append_codex_run_log(
            &options.harness_home,
            HarnessLogLevel::Info,
            "codex.run.already-completed",
            &receipt.reason,
            Some(&plan),
            receipt.transcript_file.clone(),
        )?;
        return write_codex_runtime_run_report(
            CodexRuntimeRunReport {
                schema: CODEX_RUNTIME_RUN_SCHEMA,
                harness_home: options.harness_home,
                execution_dir: receipt.execution_dir.clone(),
                plan_file: receipt.plan_file.clone(),
                run_file,
                receipts_file,
                receipt,
                completion: None,
                stdout_log,
                stderr_log,
                warnings,
            },
            true,
        );
    }

    let context_preflight = preflight_codex_context(
        &options.harness_home,
        &plan,
        &plan_file,
        execution_dir.as_deref(),
    )?;
    warnings.extend(context_preflight.warnings.clone());
    if context_preflight.receipt.compact_before_turn {
        warnings.push(format!(
            "Codex context preflight requested official compact before turn/start: {}",
            context_preflight.receipt.reason
        ));
    }
    let context_policy = context_preflight.receipt.policy.clone();
    let narration_config = load_assistant_narration_config(&options.harness_home)?;
    warnings.extend(narration_config.warnings.clone());
    if let Some(stdout_log_path) = stdout_log.as_ref()
        && let Some(recovered) = recover_completed_codex_run_from_stdout_log(
            &plan,
            stdout_log_path,
            stderr_log.clone(),
            &narration_config,
        )?
    {
        warnings.push(format!(
            "recovered completed Codex app-server turn from existing stdout log {}",
            stdout_log_path.display()
        ));
        return finish_codex_runtime_run(
            &options.harness_home,
            &plan,
            plan_file,
            execution_dir,
            run_file,
            receipts_file,
            recovered,
            0,
            current_log_time_ms()?,
            &narration_config,
            warnings,
        );
    }
    let started = Instant::now();
    let mut run_result = drive_codex_app_server(
        &options.harness_home,
        &plan,
        options.timeout_ms,
        options.idle_timeout_ms,
        options.progress_context.clone(),
        &narration_config,
        &context_policy,
        context_preflight.receipt.compact_before_turn,
    )?;
    if should_recover_compact_before_turn_failure(&run_result, &context_policy) {
        run_result = recover_compact_before_turn_failure(
            &options.harness_home,
            &plan,
            run_result,
            options.timeout_ms,
            options.idle_timeout_ms,
            options.progress_context.clone(),
            &narration_config,
            &context_policy,
        )?;
    }
    if run_result.status == CodexRuntimeRunStatus::ContextExhausted {
        run_result = recover_codex_context_exhaustion(
            &options.harness_home,
            &plan,
            run_result,
            options.timeout_ms,
            options.idle_timeout_ms,
            options.progress_context.clone(),
            &narration_config,
            &context_policy,
        )?;
    }
    if should_recover_thread_health_protocol_error(&run_result, &context_preflight.receipt) {
        run_result = recover_thread_health_protocol_error(
            &options.harness_home,
            &plan,
            run_result,
            options.timeout_ms,
            options.idle_timeout_ms,
            options.progress_context.clone(),
            &narration_config,
            &context_policy,
            &context_preflight.receipt,
        )?;
    }
    let elapsed_ms = started.elapsed().as_millis();
    let finished_at_ms = current_log_time_ms()?;
    finish_codex_runtime_run(
        &options.harness_home,
        &plan,
        plan_file,
        execution_dir,
        run_file,
        receipts_file,
        run_result,
        elapsed_ms,
        finished_at_ms,
        &narration_config,
        warnings,
    )
}

fn finish_codex_runtime_run(
    harness_home: &Path,
    plan: &CodexRuntimePlanFile,
    plan_file: PathBuf,
    execution_dir: Option<PathBuf>,
    run_file: Option<PathBuf>,
    receipts_file: PathBuf,
    mut run_result: CodexAppServerRunResult,
    elapsed_ms: u128,
    finished_at_ms: i64,
    narration_config: &AssistantNarrationConfig,
    mut warnings: Vec<String>,
) -> io::Result<CodexRuntimeRunReport> {
    let status = run_result.status;
    let reason = run_result.reason.clone();
    warnings.append(&mut run_result.warnings);
    let completion = if status == CodexRuntimeRunStatus::Completed {
        let assistant_message = if run_result.assistant_message.trim().is_empty() {
            warnings.push("Codex app-server completed without captured assistant text".to_string());
            "(no assistant text captured from Codex app-server events)".to_string()
        } else {
            std::mem::take(&mut run_result.assistant_message)
        };
        if !run_result.assistant_final_found && !run_result.assistant_raw_message.trim().is_empty()
        {
            warnings.push(
                "Codex app-server output had no final_answer agent message; raw assistant text was used as the final reply fallback"
                    .to_string(),
            );
        }
        Some(record_codex_runtime_completion(
            CodexRuntimeCompletionOptions {
                harness_home: harness_home.to_path_buf(),
                execution_dir: execution_dir.clone(),
                plan_file: Some(plan_file.clone()),
                assistant_message,
                assistant_narration: run_result.assistant_narration.clone(),
                assistant_narration_mode: narration_config.mode,
                thread_id: run_result.thread_id.clone(),
                finished_at_ms,
            },
        )?)
    } else {
        None
    };
    let receipt = CodexRuntimeRunReceipt {
        queue_id: plan.queue_id.clone(),
        status,
        execution_dir,
        plan_file: Some(plan_file),
        run_file: run_file.clone(),
        completion_file: completion
            .as_ref()
            .and_then(|report| report.completion_file.clone()),
        transcript_file: completion
            .as_ref()
            .and_then(|report| report.transcript_file.clone()),
        trajectory_file: completion
            .as_ref()
            .and_then(|report| report.trajectory_file.clone()),
        codex_binding_file: completion
            .as_ref()
            .and_then(|report| report.codex_binding_file.clone()),
        reason,
        elapsed_ms,
        event_count: run_result.event_count,
        usage: run_result.usage,
        media_plan: plan.media_plan.clone(),
        context_recovery: run_result.context_recovery,
    };
    let log_level = match receipt.status {
        CodexRuntimeRunStatus::Completed => HarnessLogLevel::Info,
        CodexRuntimeRunStatus::Timeout
        | CodexRuntimeRunStatus::ProtocolError
        | CodexRuntimeRunStatus::ContextExhausted => HarnessLogLevel::Error,
        CodexRuntimeRunStatus::Canceled => HarnessLogLevel::Warn,
        CodexRuntimeRunStatus::SpawnFailed
        | CodexRuntimeRunStatus::PreflightBlocked
        | CodexRuntimeRunStatus::NoRuntimePlan => HarnessLogLevel::Warn,
    };
    let log_event = match receipt.status {
        CodexRuntimeRunStatus::Completed => "codex.run.completed",
        CodexRuntimeRunStatus::Timeout => "codex.run.timeout",
        CodexRuntimeRunStatus::ProtocolError => "codex.run.protocol-error",
        CodexRuntimeRunStatus::ContextExhausted => "codex.run.context-exhausted",
        CodexRuntimeRunStatus::SpawnFailed => "codex.run.spawn-failed",
        CodexRuntimeRunStatus::PreflightBlocked => "codex.run.preflight-blocked",
        CodexRuntimeRunStatus::NoRuntimePlan => "codex.run.no-plan",
        CodexRuntimeRunStatus::Canceled => "codex.run.canceled",
    };
    append_codex_run_log(
        harness_home,
        log_level,
        log_event,
        &receipt.reason,
        Some(plan),
        receipt.transcript_file.clone(),
    )?;

    write_codex_runtime_run_report(
        CodexRuntimeRunReport {
            schema: CODEX_RUNTIME_RUN_SCHEMA,
            harness_home: harness_home.to_path_buf(),
            execution_dir: receipt.execution_dir.clone(),
            plan_file: receipt.plan_file.clone(),
            run_file,
            receipts_file,
            completion,
            stdout_log: run_result.stdout_log,
            stderr_log: run_result.stderr_log,
            receipt,
            warnings,
        },
        true,
    )
}

fn recover_completed_codex_run_from_stdout_log(
    plan: &CodexRuntimePlanFile,
    stdout_log: &Path,
    stderr_log: Option<PathBuf>,
    narration_config: &AssistantNarrationConfig,
) -> io::Result<Option<CodexAppServerRunResult>> {
    if !stdout_log.is_file() || fs::metadata(stdout_log)?.len() == 0 {
        return Ok(None);
    }

    let file = fs::File::open(stdout_log)?;
    let reader = BufReader::new(file);
    let mut state = CodexProtocolState::default();
    let mut progress = None;
    let mut event_count = 0usize;
    let mut completed = false;
    let mut terminal_error = None;
    let mut terminal_failure_reason = None;
    let mut thread_id = plan.invocation.thread_id.clone();

    for line in reader.lines() {
        let line = line?;
        event_count += 1;
        match serde_json::from_str::<Value>(&line) {
            Ok(value) => {
                record_protocol_usage_event(&value, &mut state);
                if let Some(extracted_thread_id) = extract_thread_id(&value) {
                    thread_id = Some(extracted_thread_id);
                }
                if let Some(error) = protocol_error(&value) {
                    terminal_error = Some(error);
                }
                collect_agent_output(&value, &mut state, &mut progress, narration_config);
                if is_turn_completed(&value) {
                    record_turn_usage(&value, &mut state);
                    if let Some(reason) =
                        turn_completed_failure_reason(&value).or_else(|| terminal_error.take())
                    {
                        completed = false;
                        terminal_failure_reason = Some(reason);
                    } else {
                        completed = true;
                        terminal_failure_reason = None;
                    }
                }
            }
            Err(error) => state.warnings.push(format!(
                "stdout recovery skipped non-JSON Codex app-server line: {error}"
            )),
        }
    }

    if !completed {
        if let Some(reason) = terminal_failure_reason {
            return Ok(Some(CodexAppServerRunResult {
                status: codex_status_for_protocol_failure(&reason),
                reason,
                assistant_message: state.assistant_message_with_harness_notices(),
                assistant_narration: state.assistant_narration_records(),
                assistant_raw_message: state.assistant_raw_message(),
                assistant_final_found: state.assistant_final_found(),
                thread_id,
                event_count,
                usage: state.usage,
                stdout_log: Some(stdout_log.to_path_buf()),
                stderr_log,
                context_recovery: None,
                warnings: state.warnings,
            }));
        }
        return Ok(None);
    }

    Ok(Some(CodexAppServerRunResult {
        status: CodexRuntimeRunStatus::Completed,
        reason: "recovered completed Codex app-server turn from stdout log".to_string(),
        assistant_message: state.assistant_message_with_harness_notices(),
        assistant_narration: state.assistant_narration_records(),
        assistant_raw_message: state.assistant_raw_message(),
        assistant_final_found: state.assistant_final_found(),
        thread_id,
        event_count,
        usage: state.usage,
        stdout_log: Some(stdout_log.to_path_buf()),
        stderr_log,
        context_recovery: None,
        warnings: state.warnings,
    }))
}

pub fn record_codex_runtime_completion(
    options: CodexRuntimeCompletionOptions,
) -> io::Result<CodexRuntimeCompletionReport> {
    let queue_dir = options.harness_home.join("state").join("runtime-queue");
    let receipts_file = queue_dir.join("codex-runtime-completion-receipts.jsonl");
    fs::create_dir_all(&queue_dir)?;
    let mut warnings = Vec::new();
    let Some(plan_file) = resolve_preflight_plan_file(
        &CodexRuntimePreflightOptions {
            harness_home: options.harness_home.clone(),
            execution_dir: options.execution_dir.clone(),
            plan_file: options.plan_file.clone(),
        },
        &mut warnings,
    )?
    else {
        let receipt = CodexRuntimeCompletionReceipt {
            queue_id: None,
            status: CodexRuntimeCompletionStatus::NoRuntimePlan,
            execution_dir: options.execution_dir,
            plan_file: options.plan_file,
            completion_file: None,
            transcript_file: None,
            trajectory_file: None,
            codex_binding_file: None,
            reason: "no codex runtime plan found; run codex-plan first".to_string(),
            assistant_narration_mode: options.assistant_narration_mode,
            assistant_final_chars: options.assistant_message.chars().count(),
            assistant_narration_items: options.assistant_narration.len(),
        };
        append_json_line(&receipts_file, &receipt)?;
        append_harness_log(
            &options.harness_home,
            &HarnessLogEvent::new(
                current_log_time_ms()?,
                HarnessLogLevel::Warn,
                "codex-runtime",
                "codex.complete.no-plan",
                receipt.reason.clone(),
            ),
        )?;
        return Ok(CodexRuntimeCompletionReport {
            schema: CODEX_RUNTIME_COMPLETION_SCHEMA,
            harness_home: options.harness_home,
            execution_dir: receipt.execution_dir.clone(),
            plan_file: receipt.plan_file.clone(),
            completion_file: None,
            receipts_file,
            receipt,
            transcript_file: None,
            trajectory_file: None,
            codex_binding_file: None,
            warnings,
        });
    };

    let plan: CodexRuntimePlanFile = read_json_file_as(&plan_file)?;
    let execution_dir = plan_file.parent().map(Path::to_path_buf);
    let completion_file = execution_dir
        .as_ref()
        .map(|dir| dir.join("codex-runtime-completion-receipt.json"));
    if let Some(existing_file) = &completion_file
        && existing_file.is_file()
    {
        let existing: CodexRuntimeCompletionReceipt = read_json_file_as(existing_file)?;
        if existing.status == CodexRuntimeCompletionStatus::Recorded {
            let receipt = CodexRuntimeCompletionReceipt {
                queue_id: existing.queue_id.clone(),
                status: CodexRuntimeCompletionStatus::AlreadyRecorded,
                execution_dir: existing.execution_dir.clone(),
                plan_file: Some(plan_file),
                completion_file: Some(existing_file.clone()),
                transcript_file: existing.transcript_file.clone(),
                trajectory_file: existing.trajectory_file.clone(),
                codex_binding_file: existing.codex_binding_file.clone(),
                reason: "codex runtime completion was already recorded".to_string(),
                assistant_narration_mode: existing.assistant_narration_mode,
                assistant_final_chars: existing.assistant_final_chars,
                assistant_narration_items: existing.assistant_narration_items,
            };
            append_json_line(&receipts_file, &receipt)?;
            append_harness_log(
                &options.harness_home,
                &HarnessLogEvent::new(
                    current_log_time_ms()?,
                    HarnessLogLevel::Info,
                    "codex-runtime",
                    "codex.complete.already-recorded",
                    receipt.reason.clone(),
                )
                .queue_id(receipt.queue_id.clone())
                .session_key(Some(plan.session_key.clone()))
                .agent_id(plan.agent_id.clone())
                .path(receipt.transcript_file.clone()),
            )?;
            return Ok(CodexRuntimeCompletionReport {
                schema: CODEX_RUNTIME_COMPLETION_SCHEMA,
                harness_home: options.harness_home,
                execution_dir: receipt.execution_dir.clone(),
                plan_file: receipt.plan_file.clone(),
                completion_file: receipt.completion_file.clone(),
                receipts_file,
                transcript_file: receipt.transcript_file.clone(),
                trajectory_file: receipt.trajectory_file.clone(),
                codex_binding_file: receipt.codex_binding_file.clone(),
                receipt,
                warnings,
            });
        }
    }

    record_completion_outputs(&plan, &options)?;
    let receipt = CodexRuntimeCompletionReceipt {
        queue_id: plan.queue_id.clone(),
        status: CodexRuntimeCompletionStatus::Recorded,
        execution_dir: execution_dir.clone(),
        plan_file: Some(plan_file),
        completion_file: completion_file.clone(),
        transcript_file: Some(plan.outputs.transcript_file.clone()),
        trajectory_file: Some(plan.outputs.trajectory_file.clone()),
        codex_binding_file: Some(plan.outputs.codex_binding_file.clone()),
        reason: "codex runtime completion recorded to transcript and trajectory".to_string(),
        assistant_narration_mode: options.assistant_narration_mode,
        assistant_final_chars: options.assistant_message.chars().count(),
        assistant_narration_items: options.assistant_narration.len(),
    };
    if let Some(completion_file) = &completion_file {
        fs::write(
            completion_file,
            serde_json::to_string_pretty(&receipt).map_err(io::Error::other)?,
        )?;
    }
    append_json_line(&receipts_file, &receipt)?;
    append_harness_log(
        &options.harness_home,
        &HarnessLogEvent::new(
            options.finished_at_ms,
            HarnessLogLevel::Info,
            "codex-runtime",
            "codex.complete.recorded",
            receipt.reason.clone(),
        )
        .queue_id(receipt.queue_id.clone())
        .session_key(Some(plan.session_key.clone()))
        .agent_id(plan.agent_id.clone())
        .path(receipt.transcript_file.clone()),
    )?;

    Ok(CodexRuntimeCompletionReport {
        schema: CODEX_RUNTIME_COMPLETION_SCHEMA,
        harness_home: options.harness_home,
        execution_dir,
        plan_file: receipt.plan_file.clone(),
        completion_file,
        receipts_file,
        transcript_file: receipt.transcript_file.clone(),
        trajectory_file: receipt.trajectory_file.clone(),
        codex_binding_file: receipt.codex_binding_file.clone(),
        receipt,
        warnings,
    })
}

fn write_launch_probe_report(
    report: CodexRuntimeLaunchProbeReport,
    append_receipt: bool,
) -> io::Result<CodexRuntimeLaunchProbeReport> {
    if let Some(launch_file) = &report.launch_file {
        let report_json = serde_json::to_string_pretty(&report).map_err(io::Error::other)?;
        fs::write(launch_file, report_json)?;
    }
    if append_receipt {
        append_json_line(&report.receipts_file, &report.receipt)?;
    }
    Ok(report)
}

fn write_codex_runtime_run_report(
    report: CodexRuntimeRunReport,
    append_receipt: bool,
) -> io::Result<CodexRuntimeRunReport> {
    if let Some(run_file) = &report.run_file {
        write_json_atomic(run_file, &report)?;
    }
    if append_receipt {
        append_json_line(&report.receipts_file, &report.receipt)?;
    }
    Ok(report)
}

fn append_codex_run_log(
    harness_home: &Path,
    level: HarnessLogLevel,
    event: &'static str,
    message: &str,
    plan: Option<&CodexRuntimePlanFile>,
    path: Option<PathBuf>,
) -> io::Result<()> {
    let mut log = HarnessLogEvent::new(
        current_log_time_ms()?,
        level,
        "codex-runtime",
        event,
        message.to_string(),
    );
    if let Some(plan) = plan {
        log = log
            .queue_id(plan.queue_id.clone())
            .session_key(Some(plan.session_key.clone()))
            .agent_id(plan.agent_id.clone());
    }
    if let Some(path) = path {
        log = log.path(Some(path));
    }
    append_harness_log(harness_home, &log).map(|_| ())
}

fn plan_completion_recorded(plan: &CodexRuntimePlanFile) -> io::Result<bool> {
    let completion_file = completion_receipt_file(plan);
    if !completion_file.is_file() {
        return Ok(false);
    }
    let receipt: CodexRuntimeCompletionReceipt = read_json_file_as(&completion_file)?;
    Ok(receipt.status == CodexRuntimeCompletionStatus::Recorded)
}

fn preflight_codex_context(
    harness_home: &Path,
    plan: &CodexRuntimePlanFile,
    plan_file: &Path,
    execution_dir: Option<&Path>,
) -> io::Result<CodexContextPreflightRun> {
    let queue_dir = harness_home.join("state").join("runtime-queue");
    let receipts_file = queue_dir.join("codex-context-preflight-receipts.jsonl");
    fs::create_dir_all(&queue_dir)?;
    let mut warnings = Vec::new();
    let policy = match load_codex_context_policy(harness_home) {
        Ok(policy) => policy,
        Err(error) => {
            warnings.push(format!(
                "failed to load codexContext policy: {error}; using defaults"
            ));
            CodexContextPolicy::default()
        }
    };
    let prompt_markdown_bytes = file_len_or_zero(&plan.prompt_markdown);
    let prompt_bundle_bytes = file_len_or_zero(&plan.prompt_bundle_json);
    let (transcript_lines, transcript_bytes) = transcript_stats(&plan.outputs.transcript_file);
    let thread_health = scan_codex_thread_health(harness_home, plan, &mut warnings);
    let latest_usage = latest_codex_runtime_usage(
        harness_home,
        &plan.outputs.codex_binding_file,
        &mut warnings,
    )
    .or_else(|| thread_health.latest_usage.clone());
    let latest_tokens = latest_usage.as_ref().and_then(codex_usage_total_tokens);
    let model_context_window = policy.model_context_window;
    let active_context_ratio = latest_tokens.and_then(|tokens| {
        model_context_window
            .filter(|window| *window > 0)
            .map(|window| tokens as f64 / window as f64)
    });
    let has_existing_thread = plan.invocation.thread_id.is_some();
    let (compact_before_turn, reason) = if !policy.enabled {
        (false, "codexContext policy disabled".to_string())
    } else if !policy.prefer_official_compact {
        (
            false,
            "codexContext policy does not prefer official compact".to_string(),
        )
    } else if !policy.auto_compact_before_turn {
        (
            false,
            "codexContext autoCompactBeforeTurn disabled".to_string(),
        )
    } else if !has_existing_thread {
        (
            false,
            "no existing Codex thread is bound to this harness session".to_string(),
        )
    } else if thread_health.compact_recommended {
        (
            true,
            thread_health.reason.clone().unwrap_or_else(|| {
                "bound Codex thread health guard recommended compact before turn".to_string()
            }),
        )
    } else if let (Some(tokens), Some(limit)) =
        (latest_tokens, policy.model_auto_compact_token_limit)
        && tokens >= limit
    {
        (
            true,
            format!(
                "latest usage {tokens} token(s) is at or above model_auto_compact_token_limit {limit}"
            ),
        )
    } else if let Some(ratio) = active_context_ratio
        && ratio >= policy.compact_at_active_context_ratio
    {
        (
            true,
            format!(
                "latest usage ratio {:.3} is at or above compactAtActiveContextRatio {:.3}",
                ratio, policy.compact_at_active_context_ratio
            ),
        )
    } else if let Some(ratio) = active_context_ratio
        && ratio >= policy.warn_at_active_context_ratio
    {
        warnings.push(format!(
            "latest usage ratio {:.3} is at or above warnAtActiveContextRatio {:.3}",
            ratio, policy.warn_at_active_context_ratio
        ));
        (
            false,
            "known usage is above warning threshold but below compact threshold".to_string(),
        )
    } else {
        (
            false,
            "no known context usage threshold requires official compact before turn".to_string(),
        )
    };
    let preflight_file = execution_dir.map(|dir| dir.join("codex-context-preflight.json"));
    let receipt = CodexContextPreflightReceipt {
        schema: CODEX_CONTEXT_PREFLIGHT_SCHEMA,
        queue_id: plan.queue_id.clone(),
        session_key: plan.session_key.clone(),
        agent_id: plan.agent_id.clone(),
        execution_dir: execution_dir.map(Path::to_path_buf),
        plan_file: plan_file.to_path_buf(),
        preflight_file: preflight_file.clone(),
        prompt_markdown_bytes,
        prompt_bundle_bytes,
        transcript_lines,
        transcript_bytes,
        codex_binding_file: plan.outputs.codex_binding_file.clone(),
        thread_id: plan.invocation.thread_id.clone(),
        latest_usage,
        model_context_window,
        active_context_ratio,
        thread_health,
        compact_before_turn,
        reason,
        policy,
    };
    if let Some(preflight_file) = &preflight_file {
        write_json_atomic(preflight_file, &receipt)?;
    }
    append_json_line(&receipts_file, &receipt)?;
    Ok(CodexContextPreflightRun { receipt, warnings })
}

fn file_len_or_zero(path: &Path) -> u64 {
    fs::metadata(path)
        .map(|metadata| metadata.len())
        .unwrap_or(0)
}

fn transcript_stats(path: &Path) -> (usize, u64) {
    let bytes = file_len_or_zero(path);
    let lines = fs::read_to_string(path)
        .map(|text| text.lines().count())
        .unwrap_or(0);
    (lines, bytes)
}

fn codex_usage_total_tokens(usage: &CodexRuntimeUsage) -> Option<u64> {
    usage
        .total_tokens
        .or_else(|| match (usage.input_tokens, usage.output_tokens) {
            (Some(input), Some(output)) => Some(input.saturating_add(output)),
            (Some(input), None) => Some(input),
            (None, Some(output)) => Some(output),
            (None, None) => None,
        })
}

fn latest_codex_runtime_usage(
    harness_home: &Path,
    codex_binding_file: &Path,
    warnings: &mut Vec<String>,
) -> Option<CodexRuntimeUsage> {
    let receipts_file = harness_home
        .join("state")
        .join("runtime-queue")
        .join("codex-runtime-run-receipts.jsonl");
    let text = match fs::read_to_string(&receipts_file) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return None,
        Err(error) => {
            warnings.push(format!(
                "failed to read {} for context usage preflight: {error}",
                receipts_file.display()
            ));
            return None;
        }
    };
    let target_binding = normalize_path_text_for_match(&codex_binding_file.to_string_lossy());
    let mut fallback = None;
    for line in text.lines().rev() {
        if line.trim().is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let Some(usage) = value
            .get("usage")
            .and_then(|usage| serde_json::from_value::<CodexRuntimeUsage>(usage.clone()).ok())
        else {
            continue;
        };
        if fallback.is_none() {
            fallback = Some(usage.clone());
        }
        let matches_binding = value
            .get("codexBindingFile")
            .and_then(Value::as_str)
            .map(normalize_path_text_for_match)
            .as_deref()
            == Some(target_binding.as_str());
        if matches_binding {
            return Some(usage);
        }
    }
    fallback
}

fn scan_codex_thread_health(
    harness_home: &Path,
    plan: &CodexRuntimePlanFile,
    warnings: &mut Vec<String>,
) -> CodexThreadHealthReport {
    let thread_id = plan.invocation.thread_id.clone();
    let Some(thread_id_text) = thread_id.as_deref() else {
        return CodexThreadHealthReport {
            thread_id,
            ..CodexThreadHealthReport::default()
        };
    };
    let codex_home = plan
        .invocation
        .codex_home
        .clone()
        .unwrap_or_else(|| harness_home.join("codex-home"));
    let sessions_root = codex_home.join("sessions");
    let mut files = Vec::new();
    collect_thread_rollout_files(&sessions_root, thread_id_text, &mut files, warnings);
    files.sort();
    let Some(rollout_file) = files.pop() else {
        return CodexThreadHealthReport {
            thread_id,
            ..CodexThreadHealthReport::default()
        };
    };
    scan_codex_rollout_file_for_thread_health(thread_id, rollout_file, warnings)
}

fn collect_thread_rollout_files(
    dir: &Path,
    thread_id: &str,
    files: &mut Vec<PathBuf>,
    warnings: &mut Vec<String>,
) {
    if files.len() >= CODEX_ROLLOUT_SCAN_MAX_FILES {
        return;
    }
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return,
        Err(error) => {
            warnings.push(format!(
                "failed to scan Codex rollout directory {}: {error}",
                dir.display()
            ));
            return;
        }
    };
    for entry in entries.flatten() {
        if files.len() >= CODEX_ROLLOUT_SCAN_MAX_FILES {
            return;
        }
        let path = entry.path();
        if path.is_dir() {
            collect_thread_rollout_files(&path, thread_id, files, warnings);
            continue;
        }
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default();
        if file_name.starts_with("rollout-")
            && file_name.ends_with(".jsonl")
            && file_name.contains(thread_id)
        {
            files.push(path);
        }
    }
}

fn scan_codex_rollout_file_for_thread_health(
    thread_id: Option<String>,
    rollout_file: PathBuf,
    warnings: &mut Vec<String>,
) -> CodexThreadHealthReport {
    let file = match fs::File::open(&rollout_file) {
        Ok(file) => file,
        Err(error) => {
            warnings.push(format!(
                "failed to open Codex rollout file {} for thread health scan: {error}",
                rollout_file.display()
            ));
            return CodexThreadHealthReport {
                thread_id,
                rollout_file: Some(rollout_file),
                ..CodexThreadHealthReport::default()
            };
        }
    };
    let mut report = CodexThreadHealthReport {
        thread_id,
        rollout_file: Some(rollout_file.clone()),
        ..CodexThreadHealthReport::default()
    };
    let mut current_turn_inline_image_bytes = 0u64;
    for line in BufReader::new(file).lines() {
        let Ok(line) = line else {
            warnings.push(format!(
                "failed to read a line from Codex rollout file {}",
                rollout_file.display()
            ));
            continue;
        };
        let line_bytes = u64::try_from(line.len()).unwrap_or(u64::MAX);
        if line.contains("\"turn/start\"")
            || line.contains("\"turn/started\"")
            || line.contains("\"turn_context\"")
        {
            report.last_turn_inline_image_bytes = current_turn_inline_image_bytes;
            current_turn_inline_image_bytes = 0;
        }
        if line.contains("data:image") {
            let count = line.matches("data:image").count();
            report.data_image_count = report.data_image_count.saturating_add(count);
            report.inline_image_bytes = report.inline_image_bytes.saturating_add(line_bytes);
            current_turn_inline_image_bytes =
                current_turn_inline_image_bytes.saturating_add(line_bytes);
        }
        if line_bytes >= CODEX_THREAD_TOOL_OUTPUT_BYTES_LIMIT
            && (line.contains("function_call_output") || line.contains("tool_output"))
        {
            report.oversized_tool_output_count =
                report.oversized_tool_output_count.saturating_add(1);
            report.oversized_tool_output_bytes = report
                .oversized_tool_output_bytes
                .saturating_add(line_bytes);
        }
        if (line.contains("token_count") || line.contains("tokenUsage"))
            && let Ok(value) = serde_json::from_str::<Value>(&line)
            && let Some(usage) = extract_protocol_usage(&value)
        {
            report.latest_usage = Some(usage);
        }
    }
    report.last_turn_inline_image_bytes = current_turn_inline_image_bytes;
    if report.last_turn_inline_image_bytes >= CODEX_THREAD_INLINE_IMAGE_LAST_TURN_BYTES_LIMIT {
        report.compact_recommended = true;
        report.reason = Some(format!(
            "bound Codex thread has {} inline image byte(s) in the latest observed turn, above limit {}",
            report.last_turn_inline_image_bytes, CODEX_THREAD_INLINE_IMAGE_LAST_TURN_BYTES_LIMIT
        ));
    } else if report.inline_image_bytes >= CODEX_THREAD_INLINE_IMAGE_TOTAL_BYTES_LIMIT {
        report.compact_recommended = true;
        report.reason = Some(format!(
            "bound Codex thread has {} total inline image byte(s), above limit {}",
            report.inline_image_bytes, CODEX_THREAD_INLINE_IMAGE_TOTAL_BYTES_LIMIT
        ));
    } else if report.oversized_tool_output_bytes >= CODEX_THREAD_TOOL_OUTPUT_BYTES_LIMIT {
        report.compact_recommended = true;
        report.reason = Some(format!(
            "bound Codex thread has {} oversized tool-output byte(s), above limit {}",
            report.oversized_tool_output_bytes, CODEX_THREAD_TOOL_OUTPUT_BYTES_LIMIT
        ));
    }
    report
}

fn normalize_path_text_for_match(path: &str) -> String {
    path.replace('\\', "/").to_ascii_lowercase()
}

pub fn load_assistant_narration_config(
    harness_home: impl AsRef<Path>,
) -> io::Result<AssistantNarrationConfig> {
    let harness_home = harness_home.as_ref();
    ensure_harness_config_valid(harness_home)?;
    let mut config = AssistantNarrationConfig {
        config_file: harness_home.join(HARNESS_CONFIG_FILE_NAME),
        ..AssistantNarrationConfig::default()
    };

    for path in [
        harness_home.join(HARNESS_CONFIG_FILE_NAME),
        harness_home.join("config").join(HARNESS_CONFIG_FILE_NAME),
    ] {
        if !path.is_file() {
            continue;
        }
        config.config_file = path.clone();
        let text = fs::read_to_string(&path)?;
        let value = serde_json::from_str::<Value>(&text).map_err(io::Error::other)?;
        let response = value.get("response").unwrap_or(&value);
        let mut configured = false;
        if let Some(mode) = response
            .get("assistantNarrationMode")
            .or_else(|| response.get("assistant_narration_mode"))
            .and_then(Value::as_str)
        {
            configured = true;
            match parse_assistant_narration_mode(mode) {
                Some(mode) => config.mode = mode,
                None => config.warnings.push(format!(
                    "unknown response.assistantNarrationMode `{mode}`; using {}",
                    config.mode.as_str()
                )),
            }
        }
        if let Some(max_chars) = response
            .get("assistantNarrationMaxChars")
            .or_else(|| response.get("assistant_narration_max_chars"))
            .and_then(Value::as_u64)
            .and_then(|value| usize::try_from(value).ok())
            .filter(|value| *value > 0)
        {
            configured = true;
            config.max_chars = max_chars;
        }
        if let Some(min_update_ms) = response
            .get("assistantNarrationProgressMinUpdateMs")
            .or_else(|| response.get("assistant_narration_progress_min_update_ms"))
            .and_then(Value::as_i64)
            .filter(|value| *value > 0)
        {
            configured = true;
            config.progress_min_update_ms = min_update_ms;
        }
        if let Some(prefix) = response
            .get("assistantNarrationFinalPrefix")
            .or_else(|| response.get("assistant_narration_final_prefix"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            configured = true;
            config.final_prefix = prefix.to_string();
        }
        config.configured = configured;
        config.source = if configured {
            format!("config:{}", path.display())
        } else {
            "default".to_string()
        };
        break;
    }

    Ok(config)
}

fn load_codex_context_policy(harness_home: &Path) -> io::Result<CodexContextPolicy> {
    ensure_harness_config_valid(harness_home)?;
    let mut policy = CodexContextPolicy::default();
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
        policy.configured = true;
        policy.source = format!("config:{}", path.display());
        if let Some(value) = context_bool(context, &["enabled"]) {
            policy.enabled = value;
        }
        if let Some(value) = context_bool(
            context,
            &["preferOfficialCompact", "prefer_official_compact"],
        ) {
            policy.prefer_official_compact = value;
        }
        if let Some(value) = context_bool(
            context,
            &["autoCompactBeforeTurn", "auto_compact_before_turn"],
        ) {
            policy.auto_compact_before_turn = value;
        }
        if let Some(value) = context_bool(
            context,
            &["retryOnceAfterCompact", "retry_once_after_compact"],
        ) {
            policy.retry_once_after_compact = value;
        }
        if let Some(value) = context_bool(
            context,
            &["manualRecoveryAllowed", "manual_recovery_allowed"],
        ) {
            policy.manual_recovery_allowed = value;
        }
        if let Some(value) = context_string(
            context,
            &["fallbackOnCompactFailure", "fallback_on_compact_failure"],
        ) {
            policy.fallback_on_compact_failure = normalize_context_fallback_policy(&value);
        }
        if let Some(value) = context_f64(
            context,
            &["warnAtActiveContextRatio", "warn_at_active_context_ratio"],
        )
        .filter(|value| *value > 0.0 && *value <= 1.0)
        {
            policy.warn_at_active_context_ratio = value;
        }
        if let Some(value) = context_f64(
            context,
            &[
                "compactAtActiveContextRatio",
                "compact_at_active_context_ratio",
            ],
        )
        .filter(|value| *value > 0.0 && *value <= 1.0)
        {
            policy.compact_at_active_context_ratio = value;
        }
        policy.model_context_window =
            context_u64(context, &["modelContextWindow", "model_context_window"])
                .filter(|value| *value > 0);
        policy.model_auto_compact_token_limit = context_u64(
            context,
            &[
                "modelAutoCompactTokenLimit",
                "model_auto_compact_token_limit",
            ],
        )
        .filter(|value| *value > 0);
        policy.model_auto_compact_token_limit_scope = context_string(
            context,
            &[
                "modelAutoCompactTokenLimitScope",
                "model_auto_compact_token_limit_scope",
            ],
        );
        policy.tool_output_token_limit = context_u64(
            context,
            &["toolOutputTokenLimit", "tool_output_token_limit"],
        )
        .filter(|value| *value > 0);
        policy.compact_prompt = context_string(context, &["compactPrompt", "compact_prompt"]);
        policy.experimental_compact_prompt_file = context_string(
            context,
            &[
                "experimentalCompactPromptFile",
                "experimental_compact_prompt_file",
            ],
        )
        .map(PathBuf::from);
        break;
    }
    Ok(policy)
}

fn context_bool(object: &serde_json::Map<String, Value>, keys: &[&str]) -> Option<bool> {
    keys.iter()
        .find_map(|key| object.get(*key).and_then(Value::as_bool))
}

fn context_u64(object: &serde_json::Map<String, Value>, keys: &[&str]) -> Option<u64> {
    keys.iter()
        .find_map(|key| object.get(*key).and_then(Value::as_u64))
}

fn context_f64(object: &serde_json::Map<String, Value>, keys: &[&str]) -> Option<f64> {
    keys.iter()
        .find_map(|key| object.get(*key).and_then(Value::as_f64))
}

fn context_string(object: &serde_json::Map<String, Value>, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        object
            .get(*key)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string)
    })
}

fn normalize_context_fallback_policy(value: &str) -> String {
    match value.trim().to_ascii_lowercase().replace('_', "-").as_str() {
        "checkpoint-and-new-thread" | "manual" | "disabled" => {
            value.trim().to_ascii_lowercase().replace('_', "-")
        }
        _ => "checkpoint-and-new-thread".to_string(),
    }
}

fn ensure_harness_config_valid(harness_home: &Path) -> io::Result<()> {
    let validation = validate_harness_config(harness_home)?;
    if validation.status == HarnessConfigValidationStatus::Invalid {
        return Err(invalid_harness_config_error(&validation));
    }
    Ok(())
}

fn invalid_harness_config_error(report: &HarnessConfigValidationReport) -> io::Error {
    let path = report
        .config_file
        .as_ref()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "harness-config.json".to_string());
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("invalid {path}: {}", report.errors.join("; ")),
    )
}

fn parse_assistant_narration_mode(value: &str) -> Option<AssistantNarrationMode> {
    let normalized = value.trim().to_ascii_lowercase().replace(['-', ' '], "_");
    match normalized.as_str() {
        "off" | "none" | "hidden" | "final_only" | "finalonly" => Some(AssistantNarrationMode::Off),
        "progress" | "progress_panel" | "progresspanel" => {
            Some(AssistantNarrationMode::ProgressPanel)
        }
        "inline" | "inline_preface" | "inlinepreface" | "preface" => {
            Some(AssistantNarrationMode::InlinePreface)
        }
        _ => None,
    }
}

struct CodexAppServerRunResult {
    status: CodexRuntimeRunStatus,
    reason: String,
    assistant_message: String,
    assistant_narration: Vec<CodexAssistantNarration>,
    assistant_raw_message: String,
    assistant_final_found: bool,
    thread_id: Option<String>,
    event_count: usize,
    usage: Option<CodexRuntimeUsage>,
    stdout_log: Option<PathBuf>,
    stderr_log: Option<PathBuf>,
    context_recovery: Option<CodexContextRecoveryReceipt>,
    warnings: Vec<String>,
}

struct StdoutReaderHandle {
    handle: Option<thread::JoinHandle<()>>,
    done_rx: mpsc::Receiver<()>,
}

impl StdoutReaderHandle {
    fn join_for(&mut self, timeout: Duration) -> bool {
        match self.done_rx.recv_timeout(timeout) {
            Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                if let Some(handle) = self.handle.take() {
                    let _ = handle.join();
                }
                true
            }
            Err(mpsc::RecvTimeoutError::Timeout) => false,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RuntimeCancelRequest {
    at_ms: i64,
    reason: Option<String>,
}

struct RuntimeCancelCheck {
    path: PathBuf,
}

impl RuntimeCancelCheck {
    fn new(harness_home: &Path, session_key: &str) -> Self {
        Self {
            path: harness_home
                .join("state")
                .join("runtime-queue")
                .join("cancel-requests")
                .join(format!("{}.json", normalize_key_part(session_key))),
        }
    }

    fn poll(&self) -> io::Result<Option<String>> {
        let bytes = match fs::read(&self.path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let request: RuntimeCancelRequest =
            serde_json::from_slice(&bytes).map_err(io::Error::other)?;
        let now_ms = current_log_time_ms()?;
        if now_ms.saturating_sub(request.at_ms) > RUNTIME_CANCEL_REQUEST_MAX_AGE_MS {
            return Ok(None);
        }
        let _ = fs::remove_file(&self.path);
        Ok(Some(
            request
                .reason
                .filter(|reason| !reason.trim().is_empty())
                .unwrap_or_else(|| "operator requested stop".to_string()),
        ))
    }
}

fn app_server_approval_policy_for_plan(harness_home: &Path, plan: &CodexRuntimePlanFile) -> String {
    if is_live_harness_home(harness_home) {
        return "on-request".to_string();
    }
    let configured = plan.invocation.app_server_approval_policy.trim();
    if configured.is_empty() {
        codex_app_server_approval_policy(plan.invocation.approval_policy).to_string()
    } else {
        configured.to_string()
    }
}

fn app_server_sandbox_for_plan(plan: &CodexRuntimePlanFile) -> String {
    let configured = plan.invocation.app_server_sandbox.trim();
    if configured.is_empty() {
        DEFAULT_CODEX_SANDBOX_POLICY.to_string()
    } else {
        configured.to_string()
    }
}

fn app_server_sandbox_mode_value(sandbox: &str) -> &'static str {
    match normalize_codex_sandbox_policy(sandbox).as_str() {
        "dangerfullaccess" => "danger-full-access",
        "readonly" => "read-only",
        _ => "workspace-write",
    }
}

fn app_server_sandbox_policy_value(sandbox: &str, runtime_workspace_root: &str) -> Value {
    match normalize_codex_sandbox_policy(sandbox).as_str() {
        "dangerfullaccess" => json!({
            "type": "dangerFullAccess"
        }),
        "readonly" => json!({
            "type": "readOnly",
            "networkAccess": false
        }),
        _ => json!({
            "type": "workspaceWrite",
            "writableRoots": [runtime_workspace_root],
            "networkAccess": false
        }),
    }
}

fn codex_app_server_approval_policy(policy: CodexApprovalPolicy) -> &'static str {
    match policy {
        CodexApprovalPolicy::Accept => "never",
        CodexApprovalPolicy::Deny => "on-request",
    }
}

fn drive_codex_app_server(
    harness_home: &Path,
    plan: &CodexRuntimePlanFile,
    timeout_ms: u64,
    idle_timeout_ms: u64,
    progress_context: Option<AgentProgressContext>,
    narration_config: &AssistantNarrationConfig,
    context_policy: &CodexContextPolicy,
    compact_before_turn: bool,
) -> io::Result<CodexAppServerRunResult> {
    let execution_dir = runtime_execution_dir(plan);
    fs::create_dir_all(&execution_dir)?;
    fs::create_dir_all(&plan.invocation.working_directory)?;
    let stdout_log = execution_dir.join("codex-runtime-run.stdout.jsonl");
    let stderr_log = execution_dir.join("codex-runtime-run.stderr.log");
    let stderr = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&stderr_log)?;
    let mut command = Command::new(&plan.invocation.executable);
    command
        .args(&plan.invocation.arguments)
        .current_dir(&plan.invocation.working_directory)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::from(stderr));
    apply_required_secret_env(
        &mut command,
        harness_home,
        &plan.invocation.env_requirements,
    );
    apply_codex_home_env(&mut command, plan);
    apply_live_agent_session_env(&mut command, harness_home);

    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) => {
            return Ok(CodexAppServerRunResult {
                status: CodexRuntimeRunStatus::SpawnFailed,
                reason: format!("failed to spawn codex app-server process: {error}"),
                assistant_message: String::new(),
                assistant_narration: Vec::new(),
                assistant_raw_message: String::new(),
                assistant_final_found: false,
                thread_id: None,
                event_count: 0,
                usage: None,
                stdout_log: Some(stdout_log),
                stderr_log: Some(stderr_log),
                context_recovery: None,
                warnings: Vec::new(),
            });
        }
    };
    let Some(mut stdin) = child.stdin.take() else {
        let _ = terminate_child(&mut child);
        return Ok(CodexAppServerRunResult {
            status: CodexRuntimeRunStatus::ProtocolError,
            reason: "codex app-server stdin pipe was unavailable".to_string(),
            assistant_message: String::new(),
            assistant_narration: Vec::new(),
            assistant_raw_message: String::new(),
            assistant_final_found: false,
            thread_id: None,
            event_count: 0,
            usage: None,
            stdout_log: Some(stdout_log),
            stderr_log: Some(stderr_log),
            context_recovery: None,
            warnings: Vec::new(),
        });
    };
    let Some(stdout) = child.stdout.take() else {
        let _ = terminate_child(&mut child);
        return Ok(CodexAppServerRunResult {
            status: CodexRuntimeRunStatus::ProtocolError,
            reason: "codex app-server stdout pipe was unavailable".to_string(),
            assistant_message: String::new(),
            assistant_narration: Vec::new(),
            assistant_raw_message: String::new(),
            assistant_final_found: false,
            thread_id: None,
            event_count: 0,
            usage: None,
            stdout_log: Some(stdout_log),
            stderr_log: Some(stderr_log),
            context_recovery: None,
            warnings: Vec::new(),
        });
    };
    let (line_rx, mut reader_handle) = spawn_stdout_reader(stdout, stdout_log.clone());
    let mut timeouts = CodexProtocolTimeouts::new(timeout_ms, idle_timeout_ms);
    let mut state = CodexProtocolState::default();
    let cancel_check = RuntimeCancelCheck::new(harness_home, &plan.session_key);
    let mut progress =
        progress_context.map(|context| CodexProgressEmitter::new(harness_home, context));

    write_json_rpc(
        &mut stdin,
        &json!({
            "id": 0,
            "method": "initialize",
            "params": {
                "clientInfo": {
                    "name": "agent_harness",
                    "title": "Agent Harness",
                    "version": env!("CARGO_PKG_VERSION")
                },
                "capabilities": {
                    "experimentalApi": true
                }
            }
        }),
    )?;
    write_json_rpc(
        &mut stdin,
        &json!({
            "method": "initialized",
            "params": {}
        }),
    )?;
    let mut thread_params = json!({});
    let app_server_approval_policy = app_server_approval_policy_for_plan(harness_home, plan);
    let app_server_sandbox = app_server_sandbox_for_plan(plan);
    let runtime_workspace_root = plan
        .invocation
        .working_directory
        .to_string_lossy()
        .to_string();
    if let Some(model) = &plan.model {
        thread_params["model"] = json!(model);
    }
    if let Some(provider) = app_server_model_provider(plan) {
        thread_params["modelProvider"] = json!(provider);
    }
    thread_params["cwd"] = json!(runtime_workspace_root.clone());
    thread_params["approvalPolicy"] = json!(app_server_approval_policy.clone());
    thread_params["sandbox"] = json!(app_server_sandbox_mode_value(&app_server_sandbox));
    thread_params["runtimeWorkspaceRoots"] = json!([runtime_workspace_root.clone()]);
    thread_params["developerInstructions"] = json!(CODEX_APP_SERVER_DEVELOPER_INSTRUCTIONS);
    let thread_method = if let Some(thread_id) = &plan.invocation.thread_id {
        thread_params["threadId"] = json!(thread_id);
        "thread/resume"
    } else {
        "thread/start"
    };
    write_json_rpc(
        &mut stdin,
        &json!({
            "id": 1,
            "method": thread_method,
            "params": thread_params
        }),
    )?;

    let thread_id = match wait_for_thread_start(
        &line_rx,
        &mut child,
        &mut stdin,
        &mut state,
        &mut progress,
        &mut timeouts,
        plan.invocation.approval_policy,
        Some(&cancel_check),
        narration_config,
    )? {
        ProtocolWait::ThreadStarted(thread_id) => thread_id,
        ProtocolWait::TimedOut(reason) => {
            finish_codex_child_and_stdout_reader(
                &mut child,
                &mut reader_handle,
                &mut state.warnings,
                "thread start timed out",
            );
            return Ok(CodexAppServerRunResult {
                status: CodexRuntimeRunStatus::Timeout,
                reason,
                assistant_message: state.assistant_message_with_harness_notices(),
                assistant_narration: state.assistant_narration_records(),
                assistant_raw_message: state.assistant_raw_message(),
                assistant_final_found: state.assistant_final_found(),
                thread_id: None,
                event_count: state.event_count,
                usage: state.usage.clone(),
                stdout_log: Some(stdout_log),
                stderr_log: Some(stderr_log),
                context_recovery: None,
                warnings: state.warnings,
            });
        }
        ProtocolWait::Failed(reason) => {
            finish_codex_child_and_stdout_reader(
                &mut child,
                &mut reader_handle,
                &mut state.warnings,
                "thread start failed",
            );
            return Ok(CodexAppServerRunResult {
                status: codex_status_for_protocol_failure(&reason),
                reason,
                assistant_message: state.assistant_message_with_harness_notices(),
                assistant_narration: state.assistant_narration_records(),
                assistant_raw_message: state.assistant_raw_message(),
                assistant_final_found: state.assistant_final_found(),
                thread_id: None,
                event_count: state.event_count,
                usage: state.usage.clone(),
                stdout_log: Some(stdout_log),
                stderr_log: Some(stderr_log),
                context_recovery: None,
                warnings: state.warnings,
            });
        }
        ProtocolWait::TurnCompleted => {
            finish_codex_child_and_stdout_reader(
                &mut child,
                &mut reader_handle,
                &mut state.warnings,
                "turn completed before thread start",
            );
            return Ok(CodexAppServerRunResult {
                status: CodexRuntimeRunStatus::ProtocolError,
                reason: "codex app-server reported turn completion before thread start".to_string(),
                assistant_message: state.assistant_message_with_harness_notices(),
                assistant_narration: state.assistant_narration_records(),
                assistant_raw_message: state.assistant_raw_message(),
                assistant_final_found: state.assistant_final_found(),
                thread_id: None,
                event_count: state.event_count,
                usage: state.usage.clone(),
                stdout_log: Some(stdout_log),
                stderr_log: Some(stderr_log),
                context_recovery: None,
                warnings: state.warnings,
            });
        }
        ProtocolWait::CompactCompleted => {
            finish_codex_child_and_stdout_reader(
                &mut child,
                &mut reader_handle,
                &mut state.warnings,
                "compact completed before thread start",
            );
            return Ok(CodexAppServerRunResult {
                status: CodexRuntimeRunStatus::ProtocolError,
                reason: "codex app-server reported context compaction before thread start"
                    .to_string(),
                assistant_message: state.assistant_message_with_harness_notices(),
                assistant_narration: state.assistant_narration_records(),
                assistant_raw_message: state.assistant_raw_message(),
                assistant_final_found: state.assistant_final_found(),
                thread_id: None,
                event_count: state.event_count,
                usage: state.usage.clone(),
                stdout_log: Some(stdout_log),
                stderr_log: Some(stderr_log),
                context_recovery: None,
                warnings: state.warnings,
            });
        }
        ProtocolWait::Canceled(reason) => {
            finish_codex_child_and_stdout_reader(
                &mut child,
                &mut reader_handle,
                &mut state.warnings,
                "thread start canceled",
            );
            return Ok(CodexAppServerRunResult {
                status: CodexRuntimeRunStatus::Canceled,
                reason,
                assistant_message: state.assistant_message_with_harness_notices(),
                assistant_narration: state.assistant_narration_records(),
                assistant_raw_message: state.assistant_raw_message(),
                assistant_final_found: state.assistant_final_found(),
                thread_id: None,
                event_count: state.event_count,
                usage: state.usage.clone(),
                stdout_log: Some(stdout_log),
                stderr_log: Some(stderr_log),
                context_recovery: None,
                warnings: state.warnings,
            });
        }
    };
    let mut context_recovery = None;
    if compact_before_turn {
        match run_official_context_compact(
            &line_rx,
            &mut child,
            &mut stdin,
            &mut state,
            &mut progress,
            &mut timeouts,
            plan.invocation.approval_policy,
            Some(&cancel_check),
            narration_config,
            &thread_id,
            2,
        )? {
            ProtocolWait::CompactCompleted => {
                context_recovery = Some(CodexContextRecoveryReceipt {
                    status: "compact-before-turn".to_string(),
                    queue_id: plan.queue_id.clone(),
                    session_key: plan.session_key.clone(),
                    original_thread_id: Some(thread_id.clone()),
                    recovered_thread_id: Some(thread_id.clone()),
                    official_compact_attempts: 1,
                    retry_attempted: false,
                    fallback_policy: context_policy.fallback_on_compact_failure.clone(),
                    fresh_thread_attempted: false,
                    fresh_thread_succeeded: false,
                    checkpoint_file: None,
                    rollover_file: None,
                    binding_backup_file: None,
                    reason: "Codex official context compaction completed before turn/start"
                        .to_string(),
                });
            }
            ProtocolWait::TimedOut(reason) => {
                finish_codex_child_and_stdout_reader(
                    &mut child,
                    &mut reader_handle,
                    &mut state.warnings,
                    "context compact timed out",
                );
                let recovery = context_recovery_failure_receipt(
                    plan,
                    Some(thread_id.clone()),
                    None,
                    context_policy,
                    "compact-before-turn-timeout",
                    format!("Codex official context compact timed out: {reason}"),
                    1,
                    false,
                );
                return Ok(CodexAppServerRunResult {
                    status: CodexRuntimeRunStatus::Timeout,
                    reason,
                    assistant_message: state.assistant_message_with_harness_notices(),
                    assistant_narration: state.assistant_narration_records(),
                    assistant_raw_message: state.assistant_raw_message(),
                    assistant_final_found: state.assistant_final_found(),
                    thread_id: Some(thread_id.clone()),
                    event_count: state.event_count,
                    usage: state.usage.clone(),
                    stdout_log: Some(stdout_log),
                    stderr_log: Some(stderr_log),
                    context_recovery: Some(recovery),
                    warnings: state.warnings,
                });
            }
            ProtocolWait::Failed(reason) => {
                finish_codex_child_and_stdout_reader(
                    &mut child,
                    &mut reader_handle,
                    &mut state.warnings,
                    "context compact failed",
                );
                let status = codex_status_for_protocol_failure(&reason);
                let recovery = context_recovery_failure_receipt(
                    plan,
                    Some(thread_id.clone()),
                    None,
                    context_policy,
                    "compact-before-turn-failed",
                    format!("Codex official context compact failed: {reason}"),
                    1,
                    false,
                );
                return Ok(CodexAppServerRunResult {
                    status,
                    reason,
                    assistant_message: state.assistant_message_with_harness_notices(),
                    assistant_narration: state.assistant_narration_records(),
                    assistant_raw_message: state.assistant_raw_message(),
                    assistant_final_found: state.assistant_final_found(),
                    thread_id: Some(thread_id.clone()),
                    event_count: state.event_count,
                    usage: state.usage.clone(),
                    stdout_log: Some(stdout_log),
                    stderr_log: Some(stderr_log),
                    context_recovery: Some(recovery),
                    warnings: state.warnings,
                });
            }
            ProtocolWait::Canceled(reason) => {
                finish_codex_child_and_stdout_reader(
                    &mut child,
                    &mut reader_handle,
                    &mut state.warnings,
                    "context compact canceled",
                );
                let recovery = context_recovery_failure_receipt(
                    plan,
                    Some(thread_id.clone()),
                    None,
                    context_policy,
                    "compact-before-turn-canceled",
                    format!("Codex official context compact canceled: {reason}"),
                    1,
                    false,
                );
                return Ok(CodexAppServerRunResult {
                    status: CodexRuntimeRunStatus::Canceled,
                    reason,
                    assistant_message: state.assistant_message_with_harness_notices(),
                    assistant_narration: state.assistant_narration_records(),
                    assistant_raw_message: state.assistant_raw_message(),
                    assistant_final_found: state.assistant_final_found(),
                    thread_id: Some(thread_id.clone()),
                    event_count: state.event_count,
                    usage: state.usage.clone(),
                    stdout_log: Some(stdout_log),
                    stderr_log: Some(stderr_log),
                    context_recovery: Some(recovery),
                    warnings: state.warnings,
                });
            }
            ProtocolWait::ThreadStarted(_) | ProtocolWait::TurnCompleted => {
                finish_codex_child_and_stdout_reader(
                    &mut child,
                    &mut reader_handle,
                    &mut state.warnings,
                    "unexpected protocol response during context compact",
                );
                let reason =
                    "codex app-server returned an unexpected response during context compact"
                        .to_string();
                let recovery = context_recovery_failure_receipt(
                    plan,
                    Some(thread_id.clone()),
                    None,
                    context_policy,
                    "compact-before-turn-failed",
                    reason.clone(),
                    1,
                    false,
                );
                return Ok(CodexAppServerRunResult {
                    status: CodexRuntimeRunStatus::ProtocolError,
                    reason,
                    assistant_message: state.assistant_message_with_harness_notices(),
                    assistant_narration: state.assistant_narration_records(),
                    assistant_raw_message: state.assistant_raw_message(),
                    assistant_final_found: state.assistant_final_found(),
                    thread_id: Some(thread_id.clone()),
                    event_count: state.event_count,
                    usage: state.usage.clone(),
                    stdout_log: Some(stdout_log),
                    stderr_log: Some(stderr_log),
                    context_recovery: Some(recovery),
                    warnings: state.warnings,
                });
            }
        }
    }
    let prompt_input = fs::read_to_string(&plan.invocation.prompt_input_file)?;
    let turn_sandbox_policy =
        app_server_sandbox_policy_value(&app_server_sandbox, &runtime_workspace_root);
    let mut input_parts = vec![json!({
        "type": "text",
        "text": prompt_input
    })];
    for part in &plan.media_plan.native_input_parts {
        input_parts.push(json!({
            "type": "image",
            "path": part.local_path.to_string_lossy().to_string(),
            "mimeType": part.mime.clone(),
            "artifactUri": part.artifact_uri.clone(),
            "sha256": part.sha256.clone()
        }));
    }
    let turn_params = json!({
        "threadId": thread_id.clone(),
        "cwd": runtime_workspace_root,
        "approvalPolicy": app_server_approval_policy,
        "sandboxPolicy": turn_sandbox_policy,
        "runtimeWorkspaceRoots": [
            plan.invocation
                .working_directory
                .to_string_lossy()
                .to_string()
        ],
        "input": input_parts
    });
    write_json_rpc(
        &mut stdin,
        &json!({
            "id": if compact_before_turn { 3 } else { 2 },
            "method": "turn/start",
            "params": turn_params
        }),
    )?;

    let status = match wait_for_turn_completed(
        &line_rx,
        &mut child,
        &mut stdin,
        &mut state,
        &mut progress,
        &mut timeouts,
        plan.invocation.approval_policy,
        Some(&cancel_check),
        narration_config,
    )? {
        ProtocolWait::TurnCompleted => CodexRuntimeRunStatus::Completed,
        ProtocolWait::TimedOut(reason) => {
            finish_codex_child_and_stdout_reader(
                &mut child,
                &mut reader_handle,
                &mut state.warnings,
                "turn timed out",
            );
            return Ok(CodexAppServerRunResult {
                status: CodexRuntimeRunStatus::Timeout,
                reason,
                assistant_message: state.assistant_message_with_harness_notices(),
                assistant_narration: state.assistant_narration_records(),
                assistant_raw_message: state.assistant_raw_message(),
                assistant_final_found: state.assistant_final_found(),
                thread_id: Some(thread_id.clone()),
                event_count: state.event_count,
                usage: state.usage.clone(),
                stdout_log: Some(stdout_log),
                stderr_log: Some(stderr_log),
                context_recovery: context_recovery.clone(),
                warnings: state.warnings,
            });
        }
        ProtocolWait::Failed(reason) => {
            finish_codex_child_and_stdout_reader(
                &mut child,
                &mut reader_handle,
                &mut state.warnings,
                "turn failed",
            );
            return Ok(CodexAppServerRunResult {
                status: codex_status_for_protocol_failure(&reason),
                reason,
                assistant_message: state.assistant_message_with_harness_notices(),
                assistant_narration: state.assistant_narration_records(),
                assistant_raw_message: state.assistant_raw_message(),
                assistant_final_found: state.assistant_final_found(),
                thread_id: Some(thread_id.clone()),
                event_count: state.event_count,
                usage: state.usage.clone(),
                stdout_log: Some(stdout_log),
                stderr_log: Some(stderr_log),
                context_recovery: context_recovery.clone(),
                warnings: state.warnings,
            });
        }
        ProtocolWait::ThreadStarted(_) => {
            finish_codex_child_and_stdout_reader(
                &mut child,
                &mut reader_handle,
                &mut state.warnings,
                "unexpected second thread start response",
            );
            return Ok(CodexAppServerRunResult {
                status: CodexRuntimeRunStatus::ProtocolError,
                reason: "codex app-server returned a second thread/start response during turn"
                    .to_string(),
                assistant_message: state.assistant_message_with_harness_notices(),
                assistant_narration: state.assistant_narration_records(),
                assistant_raw_message: state.assistant_raw_message(),
                assistant_final_found: state.assistant_final_found(),
                thread_id: Some(thread_id.clone()),
                event_count: state.event_count,
                usage: state.usage.clone(),
                stdout_log: Some(stdout_log),
                stderr_log: Some(stderr_log),
                context_recovery: context_recovery.clone(),
                warnings: state.warnings,
            });
        }
        ProtocolWait::CompactCompleted => {
            finish_codex_child_and_stdout_reader(
                &mut child,
                &mut reader_handle,
                &mut state.warnings,
                "unexpected context compact response during turn",
            );
            return Ok(CodexAppServerRunResult {
                status: CodexRuntimeRunStatus::ProtocolError,
                reason: "codex app-server returned context compaction completion during turn"
                    .to_string(),
                assistant_message: state.assistant_message_with_harness_notices(),
                assistant_narration: state.assistant_narration_records(),
                assistant_raw_message: state.assistant_raw_message(),
                assistant_final_found: state.assistant_final_found(),
                thread_id: Some(thread_id.clone()),
                event_count: state.event_count,
                usage: state.usage.clone(),
                stdout_log: Some(stdout_log),
                stderr_log: Some(stderr_log),
                context_recovery: context_recovery.clone(),
                warnings: state.warnings,
            });
        }
        ProtocolWait::Canceled(reason) => {
            let _ = write_json_rpc(
                &mut stdin,
                &json!({
                    "method": "turn/interrupt",
                    "params": {
                        "threadId": thread_id.clone()
                    }
                }),
            );
            finish_codex_child_and_stdout_reader(
                &mut child,
                &mut reader_handle,
                &mut state.warnings,
                "turn canceled",
            );
            return Ok(CodexAppServerRunResult {
                status: CodexRuntimeRunStatus::Canceled,
                reason,
                assistant_message: state.assistant_message_with_harness_notices(),
                assistant_narration: state.assistant_narration_records(),
                assistant_raw_message: state.assistant_raw_message(),
                assistant_final_found: state.assistant_final_found(),
                thread_id: Some(thread_id.clone()),
                event_count: state.event_count,
                usage: state.usage.clone(),
                stdout_log: Some(stdout_log),
                stderr_log: Some(stderr_log),
                context_recovery: context_recovery.clone(),
                warnings: state.warnings,
            });
        }
    };
    finish_codex_child_and_stdout_reader(
        &mut child,
        &mut reader_handle,
        &mut state.warnings,
        "turn completed",
    );
    Ok(CodexAppServerRunResult {
        status,
        reason: "codex app-server turn completed and assistant output was captured".to_string(),
        assistant_message: state.assistant_message_with_harness_notices(),
        assistant_narration: state.assistant_narration_records(),
        assistant_raw_message: state.assistant_raw_message(),
        assistant_final_found: state.assistant_final_found(),
        thread_id: Some(thread_id),
        event_count: state.event_count,
        usage: state.usage.clone(),
        stdout_log: Some(stdout_log),
        stderr_log: Some(stderr_log),
        context_recovery,
        warnings: state.warnings,
    })
}

fn app_server_model_provider(plan: &CodexRuntimePlanFile) -> Option<String> {
    plan.invocation
        .provider_config
        .as_ref()
        .map(|config| config.provider.clone())
        .or_else(|| plan.provider.clone())
        .map(|provider| provider.trim().to_string())
        .filter(|provider| !provider.is_empty())
}

fn context_recovery_failure_receipt(
    plan: &CodexRuntimePlanFile,
    original_thread_id: Option<String>,
    recovered_thread_id: Option<String>,
    policy: &CodexContextPolicy,
    status: &str,
    reason: String,
    official_compact_attempts: usize,
    retry_attempted: bool,
) -> CodexContextRecoveryReceipt {
    CodexContextRecoveryReceipt {
        status: status.to_string(),
        queue_id: plan.queue_id.clone(),
        session_key: plan.session_key.clone(),
        original_thread_id,
        recovered_thread_id,
        official_compact_attempts,
        retry_attempted,
        fallback_policy: policy.fallback_on_compact_failure.clone(),
        fresh_thread_attempted: false,
        fresh_thread_succeeded: false,
        checkpoint_file: None,
        rollover_file: None,
        binding_backup_file: None,
        reason,
    }
}

fn recover_codex_context_exhaustion(
    harness_home: &Path,
    plan: &CodexRuntimePlanFile,
    mut current: CodexAppServerRunResult,
    timeout_ms: u64,
    idle_timeout_ms: u64,
    progress_context: Option<AgentProgressContext>,
    narration_config: &AssistantNarrationConfig,
    policy: &CodexContextPolicy,
) -> io::Result<CodexAppServerRunResult> {
    let original_thread_id = current
        .thread_id
        .clone()
        .or_else(|| plan.invocation.thread_id.clone());
    if !policy.enabled {
        current.context_recovery = Some(context_recovery_failure_receipt(
            plan,
            original_thread_id,
            None,
            policy,
            "compact-disabled",
            "codexContext policy disabled; context exhaustion was not auto-recovered".to_string(),
            current
                .context_recovery
                .as_ref()
                .map(|receipt| receipt.official_compact_attempts)
                .unwrap_or(0),
            false,
        ));
        return Ok(current);
    }

    if policy.prefer_official_compact && policy.retry_once_after_compact {
        if let Some(thread_id) = original_thread_id.clone() {
            let original_reason = current.reason.clone();
            let original_events = current.event_count;
            let mut retry_plan = plan.clone();
            retry_plan.invocation.thread_id = Some(thread_id.clone());
            let mut retry_result = drive_codex_app_server(
                harness_home,
                &retry_plan,
                timeout_ms,
                idle_timeout_ms,
                progress_context.clone(),
                narration_config,
                policy,
                true,
            )?;
            retry_result.event_count = retry_result.event_count.saturating_add(original_events);
            retry_result.warnings.push(format!(
                "retried queue item once after Codex context exhaustion; original reason: {original_reason}"
            ));
            let compact_attempts = retry_result
                .context_recovery
                .as_ref()
                .map(|receipt| receipt.official_compact_attempts)
                .unwrap_or(1);
            let succeeded = retry_result.status == CodexRuntimeRunStatus::Completed;
            retry_result.context_recovery = Some(CodexContextRecoveryReceipt {
                status: if succeeded {
                    "compact-retry-succeeded".to_string()
                } else {
                    "compact-retry-failed".to_string()
                },
                queue_id: plan.queue_id.clone(),
                session_key: plan.session_key.clone(),
                original_thread_id: Some(thread_id),
                recovered_thread_id: retry_result.thread_id.clone(),
                official_compact_attempts: compact_attempts,
                retry_attempted: true,
                fallback_policy: policy.fallback_on_compact_failure.clone(),
                fresh_thread_attempted: false,
                fresh_thread_succeeded: false,
                checkpoint_file: None,
                rollover_file: None,
                binding_backup_file: None,
                reason: if succeeded {
                    format!(
                        "Codex context exhausted; official compact completed and retry succeeded. Original reason: {original_reason}"
                    )
                } else {
                    format!(
                        "Codex context exhausted; official compact retry failed. Original reason: {original_reason}; retry reason: {}",
                        retry_result.reason
                    )
                },
            });
            if succeeded {
                return Ok(retry_result);
            }
            current = retry_result;
        } else {
            current.context_recovery = Some(context_recovery_failure_receipt(
                plan,
                None,
                None,
                policy,
                "compact-unavailable",
                "Codex context exhausted, but no thread id was available for official compact"
                    .to_string(),
                current
                    .context_recovery
                    .as_ref()
                    .map(|receipt| receipt.official_compact_attempts)
                    .unwrap_or(0),
                false,
            ));
        }
    }

    if policy.fallback_on_compact_failure == "checkpoint-and-new-thread" {
        return run_context_checkpoint_fallback(
            harness_home,
            plan,
            current,
            original_thread_id,
            timeout_ms,
            idle_timeout_ms,
            progress_context,
            narration_config,
            policy,
        );
    }

    if current.context_recovery.is_none() {
        current.context_recovery = Some(context_recovery_failure_receipt(
            plan,
            original_thread_id,
            None,
            policy,
            "manual-recovery-required",
            "Codex context exhausted and automatic fallback recovery is not enabled".to_string(),
            0,
            false,
        ));
    }
    Ok(current)
}

#[allow(clippy::too_many_arguments)]
fn recover_compact_before_turn_failure(
    harness_home: &Path,
    plan: &CodexRuntimePlanFile,
    prior: CodexAppServerRunResult,
    timeout_ms: u64,
    idle_timeout_ms: u64,
    progress_context: Option<AgentProgressContext>,
    narration_config: &AssistantNarrationConfig,
    policy: &CodexContextPolicy,
) -> io::Result<CodexAppServerRunResult> {
    let original_thread_id = plan
        .invocation
        .thread_id
        .clone()
        .or_else(|| prior.thread_id.clone())
        .or_else(|| {
            prior
                .context_recovery
                .as_ref()
                .and_then(|receipt| receipt.original_thread_id.clone())
        });
    let prior_reason = prior.reason.clone();
    let prior_recovery_status = prior
        .context_recovery
        .as_ref()
        .map(|receipt| receipt.status.clone())
        .unwrap_or_else(|| "compact-before-turn-failed".to_string());
    let mut recovered = run_context_checkpoint_fallback(
        harness_home,
        plan,
        prior,
        original_thread_id,
        timeout_ms,
        idle_timeout_ms,
        progress_context,
        narration_config,
        policy,
    )?;
    let succeeded = recovered.status == CodexRuntimeRunStatus::Completed;
    if let Some(recovery) = recovered.context_recovery.as_mut() {
        recovery.status = if succeeded {
            "compact-before-turn-fallback-succeeded".to_string()
        } else {
            "compact-before-turn-fallback-failed".to_string()
        };
        recovery.reason = if succeeded {
            format!(
                "Codex compact-before-turn recovery status {prior_recovery_status}; checkpoint fallback opened a fresh Codex thread. Prior reason: {prior_reason}"
            )
        } else {
            format!(
                "Codex compact-before-turn recovery status {prior_recovery_status}; checkpoint fallback also failed. Prior reason: {prior_reason}; fallback reason: {}",
                recovered.reason
            )
        };
    }
    recovered.warnings.push(format!(
        "Codex compact-before-turn recovery status {prior_recovery_status} used fresh-thread checkpoint fallback: {prior_reason}"
    ));
    Ok(recovered)
}

fn should_recover_compact_before_turn_failure(
    result: &CodexAppServerRunResult,
    policy: &CodexContextPolicy,
) -> bool {
    if policy.fallback_on_compact_failure != "checkpoint-and-new-thread" {
        return false;
    }
    if result.status == CodexRuntimeRunStatus::Canceled {
        return false;
    }
    let Some(recovery) = result.context_recovery.as_ref() else {
        return false;
    };
    matches!(
        recovery.status.as_str(),
        "compact-before-turn-timeout" | "compact-before-turn-failed"
    )
}

#[allow(clippy::too_many_arguments)]
fn recover_thread_health_protocol_error(
    harness_home: &Path,
    plan: &CodexRuntimePlanFile,
    prior: CodexAppServerRunResult,
    timeout_ms: u64,
    idle_timeout_ms: u64,
    progress_context: Option<AgentProgressContext>,
    narration_config: &AssistantNarrationConfig,
    policy: &CodexContextPolicy,
    preflight: &CodexContextPreflightReceipt,
) -> io::Result<CodexAppServerRunResult> {
    let original_thread_id = plan
        .invocation
        .thread_id
        .clone()
        .or_else(|| prior.thread_id.clone());
    let guard_reason = preflight
        .thread_health
        .reason
        .clone()
        .unwrap_or_else(|| "bound Codex thread health guard recommended rollover".to_string());
    let prior_reason = prior.reason.clone();
    let mut recovered = run_context_checkpoint_fallback(
        harness_home,
        plan,
        prior,
        original_thread_id,
        timeout_ms,
        idle_timeout_ms,
        progress_context,
        narration_config,
        policy,
    )?;
    let succeeded = recovered.status == CodexRuntimeRunStatus::Completed;
    if let Some(recovery) = recovered.context_recovery.as_mut() {
        recovery.status = if succeeded {
            "thread-health-rollover-succeeded".to_string()
        } else {
            "thread-health-rollover-failed".to_string()
        };
        recovery.reason = if succeeded {
            format!(
                "retryable Codex ProtocolError after unhealthy bound thread; checkpoint fallback opened a fresh Codex thread. Guard reason: {guard_reason}. Prior reason: {prior_reason}"
            )
        } else {
            format!(
                "retryable Codex ProtocolError after unhealthy bound thread; checkpoint fallback also failed. Guard reason: {guard_reason}. Prior reason: {prior_reason}; fallback reason: {}",
                recovered.reason
            )
        };
    }
    recovered.warnings.push(format!(
        "retryable Codex ProtocolError used fresh-thread rollover because bound thread health guard triggered: {guard_reason}"
    ));
    Ok(recovered)
}

fn should_recover_thread_health_protocol_error(
    result: &CodexAppServerRunResult,
    preflight: &CodexContextPreflightReceipt,
) -> bool {
    result.status == CodexRuntimeRunStatus::ProtocolError
        && preflight.thread_health.compact_recommended
        && preflight.policy.fallback_on_compact_failure == "checkpoint-and-new-thread"
        && is_retryable_stream_disconnect_protocol_error(&result.reason)
        && !result.assistant_final_found
        && result.assistant_raw_message.trim().is_empty()
}

fn is_retryable_stream_disconnect_protocol_error(reason: &str) -> bool {
    let lower = reason.to_ascii_lowercase();
    lower.contains("stream disconnected before completion")
        || lower.contains("websocket closed by server before response.completed")
        || lower.contains("reconnecting...")
}

#[allow(clippy::too_many_arguments)]
fn run_context_checkpoint_fallback(
    harness_home: &Path,
    plan: &CodexRuntimePlanFile,
    prior: CodexAppServerRunResult,
    original_thread_id: Option<String>,
    timeout_ms: u64,
    idle_timeout_ms: u64,
    progress_context: Option<AgentProgressContext>,
    narration_config: &AssistantNarrationConfig,
    policy: &CodexContextPolicy,
) -> io::Result<CodexAppServerRunResult> {
    let execution_dir = runtime_execution_dir(plan);
    fs::create_dir_all(&execution_dir)?;
    let checkpoint_file = write_context_checkpoint(
        plan,
        original_thread_id.clone(),
        &prior.reason,
        &execution_dir,
    )?;
    let rollover = write_context_rollover_receipt(
        plan,
        original_thread_id.clone(),
        &prior.reason,
        &execution_dir,
    )?;
    let fallback_prompt_file =
        write_context_fallback_prompt(plan, &prior.reason, &checkpoint_file, &execution_dir)?;
    let mut fallback_plan = plan.clone();
    fallback_plan.invocation.thread_id = None;
    fallback_plan.invocation.prompt_input_file = fallback_prompt_file;
    let mut fallback_result = drive_codex_app_server(
        harness_home,
        &fallback_plan,
        timeout_ms,
        idle_timeout_ms,
        progress_context,
        narration_config,
        policy,
        false,
    )?;
    fallback_result.event_count = fallback_result
        .event_count
        .saturating_add(prior.event_count);
    let succeeded = fallback_result.status == CodexRuntimeRunStatus::Completed;
    let official_compact_attempts = prior
        .context_recovery
        .as_ref()
        .map(|receipt| receipt.official_compact_attempts)
        .unwrap_or(0);
    fallback_result.warnings.push(format!(
        "Codex context fallback wrote checkpoint {} and opened a fresh Codex thread",
        checkpoint_file.display()
    ));
    fallback_result.context_recovery = Some(CodexContextRecoveryReceipt {
        status: if succeeded {
            "fresh-thread-succeeded".to_string()
        } else {
            "fresh-thread-failed".to_string()
        },
        queue_id: plan.queue_id.clone(),
        session_key: plan.session_key.clone(),
        original_thread_id,
        recovered_thread_id: fallback_result.thread_id.clone(),
        official_compact_attempts,
        retry_attempted: prior
            .context_recovery
            .as_ref()
            .map(|receipt| receipt.retry_attempted)
            .unwrap_or(false),
        fallback_policy: policy.fallback_on_compact_failure.clone(),
        fresh_thread_attempted: true,
        fresh_thread_succeeded: succeeded,
        checkpoint_file: Some(checkpoint_file),
        rollover_file: Some(rollover.rollover_file),
        binding_backup_file: rollover.binding_backup_file,
        reason: if succeeded {
            format!(
                "Codex thread recovery opened a fresh Codex thread after official compact recovery failed or was unavailable. Prior reason: {}",
                prior.reason
            )
        } else {
            format!(
                "Codex thread recovery checkpoint fallback also failed. Prior reason: {}; fallback reason: {}",
                prior.reason, fallback_result.reason
            )
        },
    });
    Ok(fallback_result)
}

struct ContextRolloverFiles {
    rollover_file: PathBuf,
    binding_backup_file: Option<PathBuf>,
}

fn write_context_checkpoint(
    plan: &CodexRuntimePlanFile,
    previous_thread_id: Option<String>,
    reason: &str,
    execution_dir: &Path,
) -> io::Result<PathBuf> {
    let checkpoint_file = execution_dir.join("codex-context-checkpoint.json");
    let checkpoint = CodexContextCheckpoint {
        schema: CODEX_CONTEXT_CHECKPOINT_SCHEMA,
        queue_id: plan.queue_id.clone(),
        session_key: plan.session_key.clone(),
        agent_id: plan.agent_id.clone(),
        previous_thread_id,
        codex_binding_file: plan.outputs.codex_binding_file.clone(),
        transcript_file: plan.outputs.transcript_file.clone(),
        trajectory_file: plan.outputs.trajectory_file.clone(),
        prompt_bundle_json: plan.prompt_bundle_json.clone(),
        prompt_markdown: plan.prompt_markdown.clone(),
        reason: reason.to_string(),
        created_at_ms: current_log_time_ms()?,
    };
    write_json_atomic(&checkpoint_file, &checkpoint)?;
    Ok(checkpoint_file)
}

fn write_context_rollover_receipt(
    plan: &CodexRuntimePlanFile,
    previous_thread_id: Option<String>,
    reason: &str,
    execution_dir: &Path,
) -> io::Result<ContextRolloverFiles> {
    let rollover_file = execution_dir.join("codex-context-rollover.json");
    let binding_backup_file = if plan.outputs.codex_binding_file.is_file() {
        let backup_file = execution_dir.join("codex-binding-before-context-rollover.json");
        fs::copy(&plan.outputs.codex_binding_file, &backup_file)?;
        Some(backup_file)
    } else {
        None
    };
    let receipt = CodexContextRolloverReceipt {
        schema: CODEX_CONTEXT_ROLLOVER_SCHEMA,
        queue_id: plan.queue_id.clone(),
        session_key: plan.session_key.clone(),
        previous_thread_id,
        codex_binding_file: plan.outputs.codex_binding_file.clone(),
        binding_backup_file: binding_backup_file.clone(),
        reason: reason.to_string(),
        created_at_ms: current_log_time_ms()?,
    };
    write_json_atomic(&rollover_file, &receipt)?;
    Ok(ContextRolloverFiles {
        rollover_file,
        binding_backup_file,
    })
}

fn write_context_fallback_prompt(
    plan: &CodexRuntimePlanFile,
    reason: &str,
    checkpoint_file: &Path,
    execution_dir: &Path,
) -> io::Result<PathBuf> {
    let original_prompt = fs::read_to_string(&plan.invocation.prompt_input_file)?;
    let fallback_prompt_file = execution_dir.join("prompt.context-fallback.md");
    let previous_thread = plan.invocation.thread_id.as_deref().unwrap_or("(unknown)");
    let prompt = format!(
        "[Harness context recovery]\n\
         The previous Codex thread could not safely complete this queued turn.\n\
         Previous Codex thread id: {previous_thread}\n\
         Checkpoint artifact: {}\n\
         Recovery reason: {}\n\
         Continue the same harness session using the checkpoint and the original queued prompt below.\n\n\
         [Original queued prompt]\n\
         {original_prompt}",
        checkpoint_file.display(),
        truncate_for_notice(reason, 600)
    );
    fs::write(&fallback_prompt_file, prompt)?;
    Ok(fallback_prompt_file)
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct AssistantOutputCapture {
    raw_text: String,
    items: Vec<AssistantOutputItem>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AssistantOutputItem {
    item_id: Option<String>,
    phase: AssistantOutputPhase,
    text: String,
    started_at_ms: Option<i64>,
    completed_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AssistantOutputPhase {
    Narration,
    Final,
    Unknown,
}

impl AssistantOutputPhase {
    fn from_str(value: Option<&str>) -> Self {
        match value
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .replace(['-', ' '], "_")
            .as_str()
        {
            "commentary" | "progress" | "narration" => Self::Narration,
            "final" | "final_answer" | "answer" => Self::Final,
            _ => Self::Unknown,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Narration => "commentary",
            Self::Final => "final_answer",
            Self::Unknown => "unknown",
        }
    }
}

impl AssistantOutputCapture {
    fn push_delta(&mut self, item_id: Option<String>, delta: String) {
        self.raw_text.push_str(&delta);
        if let Some(item_id) = item_id {
            let item = self.item_mut(Some(item_id));
            item.text.push_str(&delta);
        }
    }

    fn note_item_started(
        &mut self,
        item_id: Option<String>,
        phase: AssistantOutputPhase,
        text: Option<String>,
        at_ms: Option<i64>,
    ) {
        let item = self.item_mut(item_id);
        if item.phase == AssistantOutputPhase::Unknown {
            item.phase = phase;
        }
        item.started_at_ms = at_ms.or(item.started_at_ms);
        if let Some(text) = text.filter(|text| !text.is_empty())
            && item.text.is_empty()
        {
            item.text = text;
        }
    }

    fn note_item_completed(
        &mut self,
        item_id: Option<String>,
        phase: AssistantOutputPhase,
        text: Option<String>,
        at_ms: Option<i64>,
    ) -> Option<AssistantOutputItem> {
        let text = text.filter(|text| !text.is_empty());
        if self.raw_text.is_empty()
            && let Some(text) = text.as_ref()
        {
            self.raw_text.push_str(text);
        }
        let item = self.item_mut(item_id);
        if item.phase == AssistantOutputPhase::Unknown {
            item.phase = phase;
        }
        item.completed_at_ms = at_ms.or(item.completed_at_ms);
        if let Some(text) = text {
            item.text = text;
        }
        Some(item.clone())
    }

    fn final_text(&self) -> Option<String> {
        self.items
            .iter()
            .rev()
            .find(|item| item.phase == AssistantOutputPhase::Final && !item.text.trim().is_empty())
            .map(|item| item.text.clone())
    }

    fn final_text_or_raw(&self) -> String {
        self.final_text().unwrap_or_else(|| self.raw_text.clone())
    }

    fn final_found(&self) -> bool {
        self.final_text().is_some()
    }

    fn narration_records(&self) -> Vec<CodexAssistantNarration> {
        self.items
            .iter()
            .filter(|item| {
                item.phase == AssistantOutputPhase::Narration && !item.text.trim().is_empty()
            })
            .map(|item| CodexAssistantNarration {
                item_id: item.item_id.clone(),
                phase: item.phase.as_str().to_string(),
                text: item.text.clone(),
            })
            .collect()
    }

    fn raw_text(&self) -> String {
        self.raw_text.clone()
    }

    fn item_mut(&mut self, item_id: Option<String>) -> &mut AssistantOutputItem {
        if let Some(index) = self
            .items
            .iter()
            .position(|item| item.item_id == item_id && item.item_id.is_some())
        {
            return &mut self.items[index];
        }
        self.items.push(AssistantOutputItem {
            item_id,
            phase: AssistantOutputPhase::Unknown,
            text: String::new(),
            started_at_ms: None,
            completed_at_ms: None,
        });
        self.items.last_mut().unwrap()
    }
}

#[derive(Default)]
struct CodexProtocolState {
    assistant_output: AssistantOutputCapture,
    event_count: usize,
    usage: Option<CodexRuntimeUsage>,
    warnings: Vec<String>,
    denied_approval_requests: Vec<String>,
}

impl CodexProtocolState {
    fn assistant_message_with_harness_notices(&self) -> String {
        let mut message = self.assistant_output.final_text_or_raw();
        if self.denied_approval_requests.is_empty() {
            return message;
        }

        let mut notice = format!(
            "[Harness safety] {} Codex approval request(s) were cancelled by codexApprovalPolicy=deny.",
            self.denied_approval_requests.len()
        );
        if let Some(first) = self.denied_approval_requests.first() {
            notice.push_str(" First cancelled request: ");
            notice.push_str(first);
            notice.push('.');
        }
        notice.push_str(
            " Set AGENT_HARNESS_CODEX_APPROVAL_POLICY=accept or security.codexApprovalPolicy=\"accept\" in harness-config.json to allow unattended tool execution.",
        );

        if message.trim().is_empty() {
            notice
        } else {
            message.push_str("\n\n");
            message.push_str(&notice);
            message
        }
    }

    fn assistant_narration_records(&self) -> Vec<CodexAssistantNarration> {
        self.assistant_output.narration_records()
    }

    fn assistant_raw_message(&self) -> String {
        self.assistant_output.raw_text()
    }

    fn assistant_final_found(&self) -> bool {
        self.assistant_output.final_found()
    }
}

struct CodexProgressEmitter {
    harness_home: PathBuf,
    context: AgentProgressContext,
}

impl CodexProgressEmitter {
    fn new(harness_home: &Path, context: AgentProgressContext) -> Self {
        Self {
            harness_home: harness_home.to_path_buf(),
            context,
        }
    }

    fn append(&self, event: AgentProgressEvent) -> io::Result<()> {
        append_agent_progress_event(&self.harness_home, &event).map(|_| ())
    }
}

fn emit_codex_progress(
    progress: &mut Option<CodexProgressEmitter>,
    value: &Value,
    state: &mut CodexProtocolState,
) {
    let Some(emitter) = progress.as_ref() else {
        return;
    };
    let Ok(at_ms) = current_log_time_ms() else {
        state
            .warnings
            .push("progress event timestamp could not be read".to_string());
        return;
    };
    let Some(event) = codex_progress_event_from_json(&emitter.context, value, at_ms) else {
        return;
    };
    if let Err(error) = emitter.append(event) {
        state
            .warnings
            .push(format!("progress event write failed: {error}"));
    }
}

fn codex_progress_event_from_json(
    context: &AgentProgressContext,
    value: &Value,
    at_ms: i64,
) -> Option<AgentProgressEvent> {
    let method = json_method(value)?;
    let method_lower = method.to_ascii_lowercase();
    if method_lower.contains("agentmessage")
        || method_lower.contains("agent_message")
        || method_lower.contains("agent-message")
        || method_lower.contains("message/delta")
        || method_lower == "turn/completed"
    {
        return None;
    }
    let (kind, label) = codex_progress_kind_and_label(method, &method_lower)?;
    let preview = compact_codex_progress_preview(kind, &codex_progress_preview(value, kind)?);
    Some(
        AgentProgressEvent::new(
            context,
            kind,
            label,
            preview,
            AgentProgressStatus::Started,
            at_ms,
        )
        .source("codex-runtime"),
    )
}

fn codex_progress_kind_and_label(
    method: &str,
    method_lower: &str,
) -> Option<(AgentProgressKind, &'static str)> {
    if method_lower.contains("commandexecution")
        || method_lower.contains("exec_command")
        || method_lower.contains("execcommand")
        || method_lower.contains("terminal")
        || method_lower.contains("shell")
    {
        return Some((AgentProgressKind::Terminal, "terminal"));
    }
    if method_lower.contains("execute_code")
        || method_lower.contains("python")
        || method_lower.contains("code_interpreter")
    {
        return Some((AgentProgressKind::ExecuteCode, "execute_code"));
    }
    if method_lower.contains("search")
        || method_lower.contains("grep")
        || method_lower.contains("glob")
        || method_lower.contains("rg")
    {
        return Some((AgentProgressKind::SearchFiles, "search_files"));
    }
    if (method_lower.contains("file") && method_lower.contains("read"))
        || method_lower.contains("read_file")
        || method_lower.contains("view_file")
    {
        return Some((AgentProgressKind::ReadFile, "read_file"));
    }
    if method_lower.contains("skill") {
        return Some((AgentProgressKind::SkillView, "skill_view"));
    }
    if method_lower.contains("todo") || method_lower.contains("plan_update") {
        return Some((AgentProgressKind::Todo, "todo"));
    }
    if method_lower.contains("tool")
        || method_lower.contains("mcp")
        || method_lower.contains("function")
        || method.starts_with("item/")
    {
        return Some((AgentProgressKind::ToolCall, "tool_call"));
    }
    None
}

fn codex_progress_preview(value: &Value, kind: AgentProgressKind) -> Option<String> {
    let pointers: &[&str] = match kind {
        AgentProgressKind::Terminal | AgentProgressKind::ExecuteCode => &[
            "/params/command",
            "/params/cmd",
            "/params/argv",
            "/params/args",
            "/params/arguments",
            "/params/item/command",
            "/params/item/cmd",
            "/params/item/argv",
            "/params/item/args",
            "/params/item/arguments",
        ],
        AgentProgressKind::SearchFiles => &[
            "/params/query",
            "/params/pattern",
            "/params/path",
            "/params/file",
            "/params/input",
            "/params/item/query",
            "/params/item/pattern",
            "/params/item/path",
            "/params/item/file",
        ],
        AgentProgressKind::ReadFile => &[
            "/params/path",
            "/params/file",
            "/params/name",
            "/params/input",
            "/params/item/path",
            "/params/item/file",
            "/params/item/name",
        ],
        AgentProgressKind::SkillView => &[
            "/params/name",
            "/params/skill",
            "/params/skillName",
            "/params/input",
            "/params/item/name",
            "/params/item/skill",
            "/params/item/skillName",
        ],
        AgentProgressKind::Todo => &[
            "/params/title",
            "/params/todo",
            "/params/text",
            "/params/input",
            "/params/item/title",
            "/params/item/todo",
            "/params/item/text",
        ],
        AgentProgressKind::ToolCall => &[
            "/params/toolName",
            "/params/name",
            "/params/tool/name",
            "/params/function/name",
            "/params/command",
            "/params/cmd",
            "/params/arguments",
            "/params/input",
            "/params/item/toolName",
            "/params/item/name",
            "/params/item/tool/name",
            "/params/item/function/name",
            "/params/item/command",
            "/params/item/path",
            "/params/item/query",
        ],
        AgentProgressKind::AssistantStream
        | AgentProgressKind::AssistantNarration
        | AgentProgressKind::Delivery
        | AgentProgressKind::MemoryRecall
        | AgentProgressKind::Runtime => &["/params/text", "/params/input", "/params/name"],
    };
    for pointer in [
        "/params/display",
        "/params/preview",
        "/params/summary",
        "/params/item/display",
        "/params/item/preview",
        "/params/item/summary",
    ]
    .iter()
    .chain(pointers.iter())
    {
        if let Some(text) = progress_value_text(value.pointer(pointer)) {
            return Some(text);
        }
    }
    None
}

fn compact_codex_progress_preview(kind: AgentProgressKind, preview: &str) -> String {
    let flattened = preview.split_whitespace().collect::<Vec<_>>().join(" ");
    let compact = if matches!(
        kind,
        AgentProgressKind::Terminal | AgentProgressKind::ExecuteCode | AgentProgressKind::ToolCall
    ) {
        compact_shell_command_preview(&flattened).unwrap_or(flattened)
    } else {
        flattened
    };
    truncate_for_notice(&compact, 96)
}

fn compact_shell_command_preview(value: &str) -> Option<String> {
    let lowered = value.to_ascii_lowercase();
    if lowered.contains("pwsh.exe") || lowered.contains("powershell.exe") {
        if let Some(command) = shell_command_argument(value, "-Command")
            .or_else(|| shell_command_argument(value, "-c"))
        {
            return Some(format!("pwsh: {}", compact_inner_command(&command)));
        }
        return Some("pwsh".to_string());
    }
    let compact = compact_inner_command(value);
    (compact != value).then_some(compact)
}

fn shell_command_argument(value: &str, flag: &str) -> Option<String> {
    let lowered = value.to_ascii_lowercase();
    let flag_lower = flag.to_ascii_lowercase();
    let start = lowered.find(&flag_lower)? + flag.len();
    let rest = value[start..].trim();
    if rest.is_empty() {
        return None;
    }
    Some(trim_wrapping_quotes(rest).to_string())
}

fn compact_inner_command(value: &str) -> String {
    let mut text = trim_wrapping_quotes(value).to_string();
    text = replace_path_command_name(&text, "agent-harness.exe", "agent-harness");
    text = replace_path_command_name(&text, "codex.cmd", "codex");
    text = replace_path_command_name(&text, "codex.exe", "codex");
    summarize_shell_command(&text).unwrap_or(text)
}

fn summarize_shell_command(value: &str) -> Option<String> {
    let tokens = shell_like_tokens(value);
    let lowered = value.to_ascii_lowercase();
    if lowered.contains("get-content") {
        return Some(format!(
            "read file{}",
            first_pathish_token(&tokens)
                .map(|path| format!(" {path}"))
                .unwrap_or_default()
        ));
    }
    if lowered.contains("get-date") {
        return Some("get date".to_string());
    }
    if lowered.contains("select-string") {
        return Some("search files".to_string());
    }
    if let Some(index) = tokens
        .iter()
        .position(|token| token.eq_ignore_ascii_case("agent-harness"))
    {
        return Some(format!(
            "agent-harness{}",
            tokens
                .get(index + 1)
                .filter(|token| !token.starts_with('-'))
                .map(|token| format!(" {token}"))
                .unwrap_or_default()
        ));
    }
    if let Some(index) = tokens
        .iter()
        .position(|token| token.eq_ignore_ascii_case("cargo"))
    {
        return Some(format!(
            "cargo{}",
            tokens
                .get(index + 1)
                .map(|token| format!(" {token}"))
                .unwrap_or_default()
        ));
    }
    None
}

fn shell_like_tokens(value: &str) -> Vec<String> {
    value
        .split_whitespace()
        .map(|token| {
            trim_wrapping_quotes(token)
                .trim_start_matches('&')
                .to_string()
        })
        .filter(|token| !token.is_empty())
        .collect()
}

fn first_pathish_token(tokens: &[String]) -> Option<String> {
    tokens
        .iter()
        .find(|token| {
            token.contains('\\')
                || token.contains('/')
                || token.ends_with(".md")
                || token.ends_with(".json")
                || token.ends_with(".jsonl")
        })
        .map(|token| {
            Path::new(token)
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or(token)
                .to_string()
        })
}

fn trim_wrapping_quotes(value: &str) -> &str {
    let trimmed = value.trim();
    if trimmed.len() >= 2 {
        let first = trimmed.as_bytes()[0];
        let last = trimmed.as_bytes()[trimmed.len() - 1];
        if (first == b'\'' && last == b'\'') || (first == b'"' && last == b'"') {
            return &trimmed[1..trimmed.len() - 1];
        }
    }
    trimmed
}

fn replace_path_command_name(value: &str, needle: &str, replacement: &str) -> String {
    value
        .split_whitespace()
        .map(|part| {
            if part
                .to_ascii_lowercase()
                .contains(&needle.to_ascii_lowercase())
            {
                let prefix = part
                    .chars()
                    .take_while(|ch| matches!(ch, '\'' | '"' | '&'))
                    .collect::<String>();
                let suffix = part
                    .chars()
                    .rev()
                    .take_while(|ch| matches!(ch, '\'' | '"'))
                    .collect::<String>()
                    .chars()
                    .rev()
                    .collect::<String>();
                format!("{prefix}{replacement}{suffix}")
            } else {
                part.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn progress_value_text(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) => clean_progress_text(text),
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        Value::Array(values) => {
            if values.is_empty() {
                None
            } else {
                let parts = values
                    .iter()
                    .filter_map(|value| progress_value_text(Some(value)))
                    .collect::<Vec<_>>();
                (!parts.is_empty()).then(|| parts.join(" "))
            }
        }
        Value::Object(map) => [
            "command",
            "cmd",
            "argv",
            "args",
            "arguments",
            "path",
            "file",
            "query",
            "pattern",
            "name",
            "toolName",
            "title",
            "text",
        ]
        .iter()
        .find_map(|key| {
            map.get(*key)
                .and_then(|value| progress_value_text(Some(value)))
        }),
        Value::Null => None,
    }
}

fn clean_progress_text(text: &str) -> Option<String> {
    let trimmed = text.trim();
    if trimmed.is_empty() || looks_like_progress_event_payload(trimmed) {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn looks_like_progress_event_payload(text: &str) -> bool {
    let looks_structured =
        text.starts_with('{') || text.starts_with("[{") || text.starts_with("{'");
    looks_structured
        && [
            "\"delta\"",
            "'delta'",
            "\"item\"",
            "'item'",
            "\"itemId\"",
            "'itemId'",
            "\"threadId\"",
            "'threadId'",
            "\"turnId\"",
            "'turnId'",
        ]
        .iter()
        .any(|needle| text.contains(needle))
}

enum ProtocolWait {
    ThreadStarted(String),
    TurnCompleted,
    CompactCompleted,
    Canceled(String),
    Failed(String),
    TimedOut(String),
}

struct CodexProtocolTimeouts {
    absolute_deadline: Instant,
    idle_deadline: Instant,
    idle_timeout: Duration,
    max_turn_ms: u64,
    idle_timeout_ms: u64,
}

impl CodexProtocolTimeouts {
    fn new(max_turn_ms: u64, idle_timeout_ms: u64) -> Self {
        let now = Instant::now();
        let max_turn_ms = max_turn_ms.max(1);
        let idle_timeout_ms = idle_timeout_ms.max(1);
        let idle_timeout = Duration::from_millis(idle_timeout_ms);
        Self {
            absolute_deadline: now + Duration::from_millis(max_turn_ms),
            idle_deadline: now + idle_timeout,
            idle_timeout,
            max_turn_ms,
            idle_timeout_ms,
        }
    }

    fn note_event(&mut self) {
        self.idle_deadline = Instant::now() + self.idle_timeout;
    }

    fn timed_out_reason(&self, now: Instant) -> Option<String> {
        if now >= self.absolute_deadline {
            Some(format!(
                "timed out waiting for Codex app-server completion after {}ms",
                self.max_turn_ms
            ))
        } else if now >= self.idle_deadline {
            Some(format!(
                "timed out waiting for Codex app-server JSONL event after {}ms of inactivity",
                self.idle_timeout_ms
            ))
        } else {
            None
        }
    }

    fn next_poll_timeout(&self, now: Instant) -> Duration {
        self.absolute_deadline
            .saturating_duration_since(now)
            .min(self.idle_deadline.saturating_duration_since(now))
            .min(Duration::from_millis(250))
    }
}

fn wait_for_thread_start(
    line_rx: &mpsc::Receiver<Result<String, String>>,
    child: &mut std::process::Child,
    stdin: &mut impl Write,
    state: &mut CodexProtocolState,
    progress: &mut Option<CodexProgressEmitter>,
    timeouts: &mut CodexProtocolTimeouts,
    approval_policy: CodexApprovalPolicy,
    cancel_check: Option<&RuntimeCancelCheck>,
    narration_config: &AssistantNarrationConfig,
) -> io::Result<ProtocolWait> {
    loop {
        match receive_protocol_event(line_rx, child, state, timeouts, cancel_check)? {
            ProtocolEvent::Json(value) => {
                if let Some(error) = protocol_error(&value) {
                    return Ok(ProtocolWait::Failed(error));
                }
                emit_codex_progress(progress, &value, state);
                if answer_unattended_server_request(&value, stdin, state, approval_policy)? {
                    continue;
                }
                if json_id(&value) == Some(1) {
                    return Ok(match extract_thread_id(&value) {
                        Some(thread_id) => ProtocolWait::ThreadStarted(thread_id),
                        None => ProtocolWait::Failed(
                            "thread/start response did not include a thread id".to_string(),
                        ),
                    });
                }
                if is_turn_completed(&value) {
                    record_turn_usage(&value, state);
                    if let Some(reason) = turn_completed_failure_reason(&value) {
                        return Ok(ProtocolWait::Failed(reason));
                    }
                    return Ok(ProtocolWait::TurnCompleted);
                }
                collect_agent_output(&value, state, progress, narration_config);
            }
            ProtocolEvent::TimedOut(reason) => return Ok(ProtocolWait::TimedOut(reason)),
            ProtocolEvent::Failed(reason) => return Ok(ProtocolWait::Failed(reason)),
            ProtocolEvent::Canceled(reason) => return Ok(ProtocolWait::Canceled(reason)),
        }
    }
}

fn run_official_context_compact(
    line_rx: &mpsc::Receiver<Result<String, String>>,
    child: &mut std::process::Child,
    stdin: &mut impl Write,
    state: &mut CodexProtocolState,
    progress: &mut Option<CodexProgressEmitter>,
    timeouts: &mut CodexProtocolTimeouts,
    approval_policy: CodexApprovalPolicy,
    cancel_check: Option<&RuntimeCancelCheck>,
    narration_config: &AssistantNarrationConfig,
    thread_id: &str,
    request_id: i64,
) -> io::Result<ProtocolWait> {
    write_json_rpc(
        stdin,
        &json!({
            "id": request_id,
            "method": "thread/compact/start",
            "params": {
                "threadId": thread_id
            }
        }),
    )?;
    wait_for_context_compaction_completed(
        line_rx,
        child,
        stdin,
        state,
        progress,
        timeouts,
        approval_policy,
        cancel_check,
        narration_config,
        request_id,
    )
}

fn wait_for_context_compaction_completed(
    line_rx: &mpsc::Receiver<Result<String, String>>,
    child: &mut std::process::Child,
    stdin: &mut impl Write,
    state: &mut CodexProtocolState,
    progress: &mut Option<CodexProgressEmitter>,
    timeouts: &mut CodexProtocolTimeouts,
    approval_policy: CodexApprovalPolicy,
    cancel_check: Option<&RuntimeCancelCheck>,
    narration_config: &AssistantNarrationConfig,
    request_id: i64,
) -> io::Result<ProtocolWait> {
    let mut acknowledged = false;
    let mut compaction_started = false;
    loop {
        match receive_protocol_event(line_rx, child, state, timeouts, cancel_check)? {
            ProtocolEvent::Json(value) => {
                if let Some(error) = protocol_error(&value) {
                    return Ok(ProtocolWait::Failed(error));
                }
                emit_codex_progress(progress, &value, state);
                if answer_unattended_server_request(&value, stdin, state, approval_policy)? {
                    continue;
                }
                if json_id(&value) == Some(request_id) {
                    acknowledged = true;
                }
                if is_context_compaction_started(&value) {
                    compaction_started = true;
                    state
                        .warnings
                        .push("Codex official context compaction started".to_string());
                    continue;
                }
                if is_context_compaction_completed(&value) || is_thread_compacted(&value) {
                    state
                        .warnings
                        .push("Codex official context compaction completed".to_string());
                    return Ok(ProtocolWait::CompactCompleted);
                }
                if is_turn_completed(&value) {
                    record_turn_usage(&value, state);
                    if let Some(reason) = turn_completed_failure_reason(&value) {
                        return Ok(ProtocolWait::Failed(reason));
                    }
                    if acknowledged || compaction_started {
                        return Ok(ProtocolWait::CompactCompleted);
                    }
                    return Ok(ProtocolWait::TurnCompleted);
                }
                collect_agent_output(&value, state, progress, narration_config);
            }
            ProtocolEvent::TimedOut(reason) => return Ok(ProtocolWait::TimedOut(reason)),
            ProtocolEvent::Failed(reason) => return Ok(ProtocolWait::Failed(reason)),
            ProtocolEvent::Canceled(reason) => return Ok(ProtocolWait::Canceled(reason)),
        }
    }
}

fn wait_for_turn_completed(
    line_rx: &mpsc::Receiver<Result<String, String>>,
    child: &mut std::process::Child,
    stdin: &mut impl Write,
    state: &mut CodexProtocolState,
    progress: &mut Option<CodexProgressEmitter>,
    timeouts: &mut CodexProtocolTimeouts,
    approval_policy: CodexApprovalPolicy,
    cancel_check: Option<&RuntimeCancelCheck>,
    narration_config: &AssistantNarrationConfig,
) -> io::Result<ProtocolWait> {
    loop {
        match receive_protocol_event(line_rx, child, state, timeouts, cancel_check)? {
            ProtocolEvent::Json(value) => {
                if let Some(error) = protocol_error(&value) {
                    return Ok(ProtocolWait::Failed(error));
                }
                emit_codex_progress(progress, &value, state);
                if answer_unattended_server_request(&value, stdin, state, approval_policy)? {
                    continue;
                }
                collect_agent_output(&value, state, progress, narration_config);
                if is_turn_completed(&value) {
                    record_turn_usage(&value, state);
                    if let Some(reason) = turn_completed_failure_reason(&value) {
                        return Ok(ProtocolWait::Failed(reason));
                    }
                    return Ok(ProtocolWait::TurnCompleted);
                }
                if json_id(&value) == Some(1)
                    && let Some(thread_id) = extract_thread_id(&value)
                {
                    return Ok(ProtocolWait::ThreadStarted(thread_id));
                }
            }
            ProtocolEvent::TimedOut(reason) => return Ok(ProtocolWait::TimedOut(reason)),
            ProtocolEvent::Failed(reason) => return Ok(ProtocolWait::Failed(reason)),
            ProtocolEvent::Canceled(reason) => return Ok(ProtocolWait::Canceled(reason)),
        }
    }
}

enum ProtocolEvent {
    Json(Value),
    TimedOut(String),
    Failed(String),
    Canceled(String),
}

fn receive_protocol_event(
    line_rx: &mpsc::Receiver<Result<String, String>>,
    child: &mut std::process::Child,
    state: &mut CodexProtocolState,
    timeouts: &mut CodexProtocolTimeouts,
    cancel_check: Option<&RuntimeCancelCheck>,
) -> io::Result<ProtocolEvent> {
    loop {
        if let Some(cancel_check) = cancel_check
            && let Some(reason) = cancel_check.poll()?
        {
            return Ok(ProtocolEvent::Canceled(reason));
        }
        let now = Instant::now();
        if let Some(reason) = timeouts.timed_out_reason(now) {
            return Ok(ProtocolEvent::TimedOut(reason));
        }
        let timeout = timeouts.next_poll_timeout(now);
        match line_rx.recv_timeout(timeout) {
            Ok(Ok(line)) => {
                state.event_count += 1;
                timeouts.note_event();
                return match serde_json::from_str::<Value>(&line) {
                    Ok(value) => {
                        record_protocol_usage_event(&value, state);
                        Ok(ProtocolEvent::Json(value))
                    }
                    Err(error) => Ok(ProtocolEvent::Failed(format!(
                        "codex app-server stdout line was not valid JSON: {error}"
                    ))),
                };
            }
            Ok(Err(error)) => {
                return Ok(ProtocolEvent::Failed(format!(
                    "failed to read codex app-server stdout: {error}"
                )));
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if let Some(status) = child.try_wait()? {
                    return Ok(ProtocolEvent::Failed(format!(
                        "codex app-server exited before completing protocol: {status}"
                    )));
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return if let Some(status) = child.try_wait()? {
                    Ok(ProtocolEvent::Failed(format!(
                        "codex app-server stdout closed before completion: {status}"
                    )))
                } else {
                    Ok(ProtocolEvent::Failed(
                        "codex app-server stdout reader disconnected".to_string(),
                    ))
                };
            }
        }
    }
}

fn finish_codex_child_and_stdout_reader(
    child: &mut std::process::Child,
    reader_handle: &mut StdoutReaderHandle,
    warnings: &mut Vec<String>,
    context: &str,
) {
    match terminate_child_with_timeout(
        child,
        Duration::from_millis(CODEX_CHILD_TERMINATE_TIMEOUT_MS),
    ) {
        Ok(true) => {}
        Ok(false) => warnings.push(format!(
            "codex app-server child did not exit within {CODEX_CHILD_TERMINATE_TIMEOUT_MS}ms after {context}; terminal receipt will still be written"
        )),
        Err(error) => warnings.push(format!(
            "codex app-server child termination failed after {context}: {error}"
        )),
    }
    if !reader_handle.join_for(Duration::from_millis(CODEX_STDOUT_READER_JOIN_TIMEOUT_MS)) {
        warnings.push(format!(
            "codex app-server stdout reader did not finish within {CODEX_STDOUT_READER_JOIN_TIMEOUT_MS}ms after {context}; detached so terminal receipt can be written"
        ));
    }
}

fn spawn_stdout_reader<R: Read + Send + 'static>(
    stdout: R,
    stdout_log: PathBuf,
) -> (mpsc::Receiver<Result<String, String>>, StdoutReaderHandle) {
    let (line_tx, line_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let _ = (|| -> Result<(), ()> {
            let log = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&stdout_log);
            let mut log = match log {
                Ok(log) => log,
                Err(error) => {
                    let _ = line_tx.send(Err(format!(
                        "failed to open stdout log {}: {error}",
                        stdout_log.display()
                    )));
                    return Err(());
                }
            };
            let reader = BufReader::new(stdout);
            for line in reader.lines() {
                match line {
                    Ok(line) => {
                        if let Err(error) = writeln!(log, "{line}") {
                            let _ = line_tx.send(Err(format!(
                                "failed to write stdout log {}: {error}",
                                stdout_log.display()
                            )));
                            return Err(());
                        }
                        if line_tx.send(Ok(line)).is_err() {
                            return Err(());
                        }
                    }
                    Err(error) => {
                        let _ = line_tx.send(Err(error.to_string()));
                        return Err(());
                    }
                }
            }
            Ok(())
        })();
        let _ = done_tx.send(());
    });
    (
        line_rx,
        StdoutReaderHandle {
            handle: Some(handle),
            done_rx,
        },
    )
}

fn write_json_rpc(stdin: &mut impl Write, value: &Value) -> io::Result<()> {
    serde_json::to_writer(&mut *stdin, value).map_err(io::Error::other)?;
    writeln!(stdin)?;
    stdin.flush()
}

fn terminate_child(child: &mut std::process::Child) -> io::Result<()> {
    let _ = terminate_child_with_timeout(
        child,
        Duration::from_millis(CODEX_CHILD_TERMINATE_TIMEOUT_MS),
    )?;
    Ok(())
}

fn terminate_child_with_timeout(
    child: &mut std::process::Child,
    timeout: Duration,
) -> io::Result<bool> {
    if child.try_wait()?.is_some() {
        return Ok(true);
    }

    terminate_child_process_tree(child);
    let started = Instant::now();
    loop {
        if child.try_wait()?.is_some() {
            return Ok(true);
        }
        if started.elapsed() >= timeout {
            return Ok(false);
        }
        thread::sleep(Duration::from_millis(25));
    }
}

#[cfg(windows)]
fn terminate_child_process_tree(child: &mut std::process::Child) {
    let _ = Command::new("taskkill")
        .args(["/PID", &child.id().to_string(), "/T", "/F"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn();
    let _ = child.kill();
}

#[cfg(not(windows))]
fn terminate_child_process_tree(child: &mut std::process::Child) {
    let _ = child.kill();
}

fn json_id(value: &Value) -> Option<i64> {
    value.get("id").and_then(Value::as_i64)
}

fn json_method(value: &Value) -> Option<&str> {
    value.get("method").and_then(Value::as_str)
}

fn answer_unattended_server_request(
    value: &Value,
    stdin: &mut impl Write,
    state: &mut CodexProtocolState,
    approval_policy: CodexApprovalPolicy,
) -> io::Result<bool> {
    let Some(method) = json_method(value) else {
        return Ok(false);
    };
    let Some(id) = value.get("id").cloned() else {
        return Ok(false);
    };
    if let Some(intent) = classify_approval_request(value) {
        if intent.action.destructive() {
            let Some(result) = unattended_approval_result(value, CodexApprovalPolicy::Deny) else {
                return Ok(false);
            };
            write_json_rpc(
                stdin,
                &json!({
                    "id": id,
                    "result": result
                }),
            )?;
            let summary = approval_request_summary(value);
            state.denied_approval_requests.push(summary);
            state.warnings.push(format!(
                "blocked protected live gateway control request {method}: {}",
                intent.reason
            ));
            return Ok(true);
        }
    }
    let Some(result) = unattended_approval_result(value, approval_policy) else {
        return Ok(false);
    };
    write_json_rpc(
        stdin,
        &json!({
            "id": id,
            "result": result
        }),
    )?;
    match approval_policy {
        CodexApprovalPolicy::Deny => {
            let summary = approval_request_summary(value);
            state.denied_approval_requests.push(summary);
            state.warnings.push(format!(
                "auto-cancelled Codex app-server request {method} by approval policy deny"
            ));
        }
        CodexApprovalPolicy::Accept => {
            state.warnings.push(format!(
                "auto-accepted Codex app-server request {method} by approval policy accept"
            ));
        }
    }
    Ok(true)
}

fn unattended_approval_result(
    value: &Value,
    approval_policy: CodexApprovalPolicy,
) -> Option<Value> {
    let method = json_method(value)?;
    if method == "item/permissions/requestApproval" {
        return Some(unattended_permissions_result(value, approval_policy));
    }
    unattended_approval_decision(value, method, approval_policy)
        .map(|decision| json!({ "decision": decision }))
}

fn unattended_permissions_result(value: &Value, approval_policy: CodexApprovalPolicy) -> Value {
    match approval_policy {
        CodexApprovalPolicy::Accept => {
            let permissions = value
                .pointer("/params/permissions")
                .cloned()
                .unwrap_or_else(|| json!({}));
            json!({
                "scope": "session",
                "permissions": permissions
            })
        }
        CodexApprovalPolicy::Deny => json!({
            "scope": "turn",
            "permissions": {}
        }),
    }
}

fn unattended_approval_decision(
    value: &Value,
    method: &str,
    approval_policy: CodexApprovalPolicy,
) -> Option<Value> {
    match (method, approval_policy) {
        ("execCommandApproval" | "applyPatchApproval", CodexApprovalPolicy::Deny) => {
            Some(json!("denied"))
        }
        ("execCommandApproval" | "applyPatchApproval", CodexApprovalPolicy::Accept) => {
            Some(json!("approved"))
        }
        (
            "item/commandExecution/requestApproval" | "item/fileChange/requestApproval",
            CodexApprovalPolicy::Deny,
        ) => Some(deny_decision(value)),
        ("item/commandExecution/requestApproval", CodexApprovalPolicy::Accept) => {
            Some(accept_command_decision(value))
        }
        ("item/fileChange/requestApproval", CodexApprovalPolicy::Accept) => {
            Some(accept_file_change_decision(value))
        }
        _ => None,
    }
}

fn deny_decision(value: &Value) -> Value {
    if available_decision_string(value, "decline") && !available_decision_string(value, "cancel") {
        json!("decline")
    } else {
        json!("cancel")
    }
}

fn accept_command_decision(value: &Value) -> Value {
    if let Some(decision) = available_decision_object(value, "acceptWithExecpolicyAmendment") {
        return decision;
    }
    if available_decision_string(value, "acceptForSession") {
        return json!("acceptForSession");
    }
    json!("accept")
}

fn accept_file_change_decision(value: &Value) -> Value {
    if available_decision_string(value, "acceptForSession") {
        json!("acceptForSession")
    } else {
        json!("accept")
    }
}

fn available_decision_string(value: &Value, expected: &str) -> bool {
    value
        .pointer("/params/availableDecisions")
        .and_then(Value::as_array)
        .is_some_and(|decisions| {
            decisions
                .iter()
                .any(|decision| decision.as_str() == Some(expected))
        })
}

fn available_decision_object(value: &Value, expected_key: &str) -> Option<Value> {
    value
        .pointer("/params/availableDecisions")
        .and_then(Value::as_array)
        .and_then(|decisions| {
            decisions.iter().find_map(|decision| {
                decision
                    .as_object()
                    .and_then(|object| object.contains_key(expected_key).then(|| decision.clone()))
            })
        })
}

fn approval_request_summary(value: &Value) -> String {
    let method = json_method(value).unwrap_or("unknown approval request");
    let mut parts = vec![method.to_string()];
    if let Some(cwd) = value.pointer("/params/cwd").and_then(Value::as_str) {
        parts.push(format!("cwd={}", truncate_for_notice(cwd, 120)));
    }
    if let Some(reason) = value.pointer("/params/reason").and_then(Value::as_str) {
        parts.push(format!("reason={}", truncate_for_notice(reason, 160)));
    }
    parts.join(", ")
}

fn truncate_for_notice(value: &str, max_chars: usize) -> String {
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

fn extract_thread_id(value: &Value) -> Option<String> {
    for pointer in [
        "/result/thread/id",
        "/result/threadId",
        "/result/id",
        "/params/thread/id",
        "/params/threadId",
    ] {
        if let Some(text) = value.pointer(pointer).and_then(Value::as_str) {
            return Some(text.to_string());
        }
    }
    None
}

fn collect_agent_output(
    value: &Value,
    state: &mut CodexProtocolState,
    progress: &mut Option<CodexProgressEmitter>,
    narration_config: &AssistantNarrationConfig,
) {
    if let Some(event) = extract_agent_message_item_event(value) {
        if event.completed {
            if let Some(item) = state.assistant_output.note_item_completed(
                event.item_id,
                event.phase,
                event.text,
                event.at_ms,
            ) {
                emit_assistant_narration_progress(progress, state, narration_config, &item);
            }
        } else {
            state.assistant_output.note_item_started(
                event.item_id,
                event.phase,
                event.text,
                event.at_ms,
            );
        }
    }
    if let Some(delta) = extract_agent_delta(value) {
        state.assistant_output.push_delta(delta.item_id, delta.text);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AgentDelta {
    item_id: Option<String>,
    text: String,
}

fn extract_agent_delta(value: &Value) -> Option<AgentDelta> {
    let method = json_method(value)?;
    let is_agent_delta = method.contains("agentMessage")
        || method.contains("agent_message")
        || method.contains("agent-message")
        || method.contains("message/delta");
    if !is_agent_delta {
        return None;
    }
    for pointer in [
        "/params/delta",
        "/params/text",
        "/params/message/delta",
        "/params/message/text",
        "/params/item/delta",
        "/params/item/text",
        "/params/item/content",
    ] {
        if let Some(candidate) = value.pointer(pointer)
            && let Some(text) = string_or_nested_text(candidate)
        {
            return Some(AgentDelta {
                item_id: first_string_pointer(
                    value,
                    &[
                        "/params/itemId",
                        "/params/item/id",
                        "/params/message/id",
                        "/params/id",
                    ],
                ),
                text,
            });
        }
    }
    None
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AgentMessageItemEvent {
    completed: bool,
    item_id: Option<String>,
    phase: AssistantOutputPhase,
    text: Option<String>,
    at_ms: Option<i64>,
}

fn extract_agent_message_item_event(value: &Value) -> Option<AgentMessageItemEvent> {
    let method = json_method(value)?;
    let completed = match method {
        "item/started" => false,
        "item/completed" => true,
        _ => return None,
    };
    let item = value.pointer("/params/item")?;
    let item_type = string_field(item, &["type"])?;
    if !item_type.eq_ignore_ascii_case("agentMessage")
        && !item_type.eq_ignore_ascii_case("agent_message")
        && !item_type.eq_ignore_ascii_case("agent-message")
    {
        return None;
    }
    let text = ["/params/item/text", "/params/item/content"]
        .iter()
        .find_map(|pointer| value.pointer(pointer).and_then(string_or_nested_text));
    let at_ms = if completed {
        first_i64_pointer(
            value,
            &["/params/completedAtMs", "/params/item/completedAtMs"],
        )
    } else {
        first_i64_pointer(value, &["/params/startedAtMs", "/params/item/startedAtMs"])
    };
    Some(AgentMessageItemEvent {
        completed,
        item_id: first_string_pointer(
            value,
            &[
                "/params/item/id",
                "/params/itemId",
                "/params/message/id",
                "/params/id",
            ],
        ),
        phase: AssistantOutputPhase::from_str(string_field(item, &["phase"])),
        text,
        at_ms,
    })
}

fn emit_assistant_narration_progress(
    progress: &mut Option<CodexProgressEmitter>,
    state: &mut CodexProtocolState,
    config: &AssistantNarrationConfig,
    item: &AssistantOutputItem,
) {
    if config.mode != AssistantNarrationMode::ProgressPanel
        || item.phase != AssistantOutputPhase::Narration
    {
        return;
    }
    let Some(emitter) = progress.as_ref() else {
        return;
    };
    let preview = compact_assistant_narration_preview(&item.text, config.max_chars);
    if preview.is_empty() {
        return;
    }
    let at_ms = match item.completed_at_ms {
        Some(at_ms) => at_ms,
        None => match current_log_time_ms() {
            Ok(at_ms) => at_ms,
            Err(error) => {
                state.warnings.push(format!(
                    "assistant narration progress timestamp could not be read: {error}"
                ));
                return;
            }
        },
    };
    let event = AgentProgressEvent::new_with_preview_limit(
        &emitter.context,
        AgentProgressKind::AssistantNarration,
        "assistant_narration",
        preview,
        AgentProgressStatus::Progress,
        at_ms,
        config.max_chars,
    )
    .source("codex-runtime");
    if let Err(error) = emitter.append(event) {
        state.warnings.push(format!(
            "assistant narration progress write failed: {error}"
        ));
    }
}

fn compact_assistant_narration_preview(text: &str, max_chars: usize) -> String {
    let compact = text.split_whitespace().collect::<Vec<_>>().join(" ");
    truncate_for_notice(&compact, max_chars.max(1))
}

fn first_string_pointer(value: &Value, pointers: &[&str]) -> Option<String> {
    pointers
        .iter()
        .find_map(|pointer| value.pointer(pointer).and_then(Value::as_str))
        .map(ToString::to_string)
}

fn first_i64_pointer(value: &Value, pointers: &[&str]) -> Option<i64> {
    pointers.iter().find_map(|pointer| {
        value
            .pointer(pointer)
            .and_then(|value| value.as_i64().or_else(|| value.as_u64()?.try_into().ok()))
    })
}

fn string_or_nested_text(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Array(items) => {
            let mut out = String::new();
            for item in items {
                if let Some(text) = string_or_nested_text(item) {
                    out.push_str(&text);
                }
            }
            (!out.is_empty()).then_some(out)
        }
        Value::Object(object) => {
            for key in ["text", "content", "delta"] {
                if let Some(value) = object.get(key)
                    && let Some(text) = string_or_nested_text(value)
                {
                    return Some(text);
                }
            }
            None
        }
        _ => None,
    }
}

fn is_turn_completed(value: &Value) -> bool {
    matches!(json_method(value), Some("turn/completed"))
}

fn is_thread_compacted(value: &Value) -> bool {
    matches!(json_method(value), Some("thread/compacted"))
}

fn is_context_compaction_started(value: &Value) -> bool {
    matches!(json_method(value), Some("item/started")) && is_context_compaction_item(value)
}

fn is_context_compaction_completed(value: &Value) -> bool {
    matches!(json_method(value), Some("item/completed")) && is_context_compaction_item(value)
}

fn is_context_compaction_item(value: &Value) -> bool {
    first_string_pointer(
        value,
        &[
            "/params/item/type",
            "/params/item/item/type",
            "/params/item/kind",
            "/params/type",
            "/params/kind",
        ],
    )
    .map(|kind| normalize_compaction_kind(&kind) == "contextcompaction")
    .unwrap_or(false)
}

fn normalize_compaction_kind(value: &str) -> String {
    value
        .trim()
        .to_ascii_lowercase()
        .replace(['_', '-', ' '], "")
}

fn turn_completed_failure_reason(value: &Value) -> Option<String> {
    if !is_turn_completed(value) {
        return None;
    }
    let status = value
        .pointer("/params/turn/status")
        .or_else(|| value.pointer("/params/status"))
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or_default();
    if status.is_empty()
        || matches!(
            status.to_ascii_lowercase().as_str(),
            "completed" | "complete" | "succeeded" | "success" | "ok"
        )
    {
        return None;
    }

    let detail = value
        .pointer("/params/turn/error")
        .or_else(|| value.pointer("/params/error"))
        .map(render_protocol_error)
        .or_else(|| {
            value
                .pointer("/params/turn/error/message")
                .or_else(|| value.pointer("/params/error/message"))
                .and_then(Value::as_str)
                .map(ToString::to_string)
        });
    Some(match detail {
        Some(detail) if !detail.trim().is_empty() => {
            format!("codex app-server turn completed with status `{status}`: {detail}")
        }
        _ => format!("codex app-server turn completed with status `{status}`"),
    })
}

fn record_turn_usage(value: &Value, state: &mut CodexProtocolState) {
    if let Some(usage) = extract_turn_usage(value) {
        state.usage = Some(usage);
    }
}

fn record_protocol_usage_event(value: &Value, state: &mut CodexProtocolState) {
    if let Some(usage) = extract_protocol_usage(value) {
        state.usage = Some(usage);
    }
}

fn extract_protocol_usage(value: &Value) -> Option<CodexRuntimeUsage> {
    extract_turn_usage(value).or_else(|| extract_token_usage_event(value))
}

fn extract_turn_usage(value: &Value) -> Option<CodexRuntimeUsage> {
    for pointer in [
        "/params/usage",
        "/params/turn/usage",
        "/params/output/usage",
        "/params/turn/output/usage",
    ] {
        let Some(usage) = value.pointer(pointer) else {
            continue;
        };
        let input_tokens = usage_u64(
            usage,
            &[
                "inputTokens",
                "input_tokens",
                "promptTokens",
                "prompt_tokens",
            ],
        );
        let output_tokens = usage_u64(
            usage,
            &[
                "outputTokens",
                "output_tokens",
                "completionTokens",
                "completion_tokens",
            ],
        );
        let total_tokens = usage_u64(usage, &["totalTokens", "total_tokens"]);
        let raw = serde_json::to_string(usage)
            .ok()
            .map(|text| truncate_for_notice(&text, 512));
        if input_tokens.is_some()
            || output_tokens.is_some()
            || total_tokens.is_some()
            || usage.is_object()
        {
            return Some(CodexRuntimeUsage {
                input_tokens,
                output_tokens,
                total_tokens,
                source: pointer.trim_start_matches('/').replace('/', "."),
                raw,
            });
        }
    }
    None
}

fn extract_token_usage_event(value: &Value) -> Option<CodexRuntimeUsage> {
    let method = json_method(value).unwrap_or_default();
    let likely_usage_event = method.eq_ignore_ascii_case("thread/tokenUsage/updated")
        || method.to_ascii_lowercase().contains("tokenusage")
        || method.to_ascii_lowercase().contains("token_usage")
        || value
            .get("type")
            .and_then(Value::as_str)
            .is_some_and(|kind| kind.eq_ignore_ascii_case("token_count"))
        || value
            .pointer("/msg/type")
            .and_then(Value::as_str)
            .is_some_and(|kind| kind.eq_ignore_ascii_case("event_msg"))
        || value
            .pointer("/msg/payload/type")
            .and_then(Value::as_str)
            .is_some_and(|kind| kind.eq_ignore_ascii_case("token_count"))
        || value
            .pointer("/payload/type")
            .and_then(Value::as_str)
            .is_some_and(|kind| kind.eq_ignore_ascii_case("token_count"));
    if !likely_usage_event {
        return None;
    }
    for pointer in [
        "/params/usage",
        "/params/tokenUsage",
        "/params/token_usage",
        "/params",
        "/params/thread",
        "/msg/payload",
        "/payload",
        "/info",
    ] {
        if let Some(candidate) = value.pointer(pointer)
            && let Some(usage) = token_usage_from_value(
                candidate,
                &format!(
                    "{}.{}",
                    pointer.trim_start_matches('/').replace('/', "."),
                    "token"
                ),
            )
        {
            return Some(usage);
        }
    }
    token_usage_from_value(value, "token-usage-event")
}

fn token_usage_from_value(value: &Value, source: &str) -> Option<CodexRuntimeUsage> {
    if let Some(usage) = direct_token_usage(value, source) {
        return Some(usage);
    }
    match value {
        Value::Object(object) => object
            .values()
            .find_map(|value| token_usage_from_value(value, source)),
        Value::Array(values) => values
            .iter()
            .find_map(|value| token_usage_from_value(value, source)),
        _ => None,
    }
}

fn direct_token_usage(value: &Value, source: &str) -> Option<CodexRuntimeUsage> {
    let input_tokens = usage_u64(
        value,
        &[
            "inputTokens",
            "input_tokens",
            "promptTokens",
            "prompt_tokens",
            "input",
        ],
    );
    let output_tokens = usage_u64(
        value,
        &[
            "outputTokens",
            "output_tokens",
            "completionTokens",
            "completion_tokens",
            "output",
        ],
    );
    let total_tokens = usage_u64(
        value,
        &[
            "totalTokens",
            "total_tokens",
            "total",
            "totalTokenUsage",
            "total_token_usage",
        ],
    );
    if input_tokens.is_none() && output_tokens.is_none() && total_tokens.is_none() {
        return None;
    }
    let raw = serde_json::to_string(value)
        .ok()
        .map(|text| truncate_for_notice(&text, 512));
    Some(CodexRuntimeUsage {
        input_tokens,
        output_tokens,
        total_tokens,
        source: source.to_string(),
        raw,
    })
}

fn usage_u64(value: &Value, keys: &[&str]) -> Option<u64> {
    keys.iter().find_map(|key| {
        value.get(*key).and_then(|value| {
            value
                .as_u64()
                .or_else(|| value.as_i64().and_then(|n| n.try_into().ok()))
        })
    })
}

fn protocol_error(value: &Value) -> Option<String> {
    let error = if matches!(json_method(value), Some("error")) {
        value
            .pointer("/params/error")
            .or_else(|| value.get("error"))
            .or_else(|| value.get("params"))
            .unwrap_or(value)
    } else {
        value.get("error")?
    };
    Some(render_protocol_error(error))
}

fn codex_status_for_protocol_failure(reason: &str) -> CodexRuntimeRunStatus {
    if is_context_window_exhaustion(reason) {
        CodexRuntimeRunStatus::ContextExhausted
    } else {
        CodexRuntimeRunStatus::ProtocolError
    }
}

fn is_context_window_exhaustion(reason: &str) -> bool {
    let normalized = reason
        .to_ascii_lowercase()
        .replace(['_', '-', '`', '"', '\''], " ");
    [
        "contextwindowexceeded",
        "context window exceeded",
        "context length exceeded",
        "context limit",
        "context window",
        "maximum context length",
        "max context length",
        "ran out of room",
        "too many tokens",
    ]
    .iter()
    .any(|needle| normalized.contains(needle))
        || reason
            .to_ascii_lowercase()
            .contains("context_length_exceeded")
}

fn render_protocol_error(error: &Value) -> String {
    if let Some(text) = error.as_str() {
        return text.to_string();
    }
    if let Some(message) = error.get("message").and_then(Value::as_str) {
        if let Some(details) = error.get("additionalDetails").and_then(Value::as_str)
            && !details.trim().is_empty()
        {
            return format!("{message}; {}", details.trim());
        }
        return message.to_string();
    }
    error.to_string()
}

fn record_completion_outputs(
    plan: &CodexRuntimePlanFile,
    options: &CodexRuntimeCompletionOptions,
) -> io::Result<()> {
    fs::create_dir_all(parent_dir(&plan.outputs.transcript_file)?)?;
    fs::create_dir_all(parent_dir(&plan.outputs.trajectory_file)?)?;
    fs::create_dir_all(parent_dir(&plan.outputs.codex_binding_file)?)?;

    if let Some(user_message) = prompt_bundle_user_message(&plan.prompt_bundle_json)? {
        append_json_line(
            &plan.outputs.transcript_file,
            &CodexTranscriptMessage {
                schema: CODEX_TRANSCRIPT_MESSAGE_SCHEMA,
                queue_id: plan.queue_id.clone(),
                session_key: plan.session_key.clone(),
                agent_id: plan.agent_id.clone(),
                role: "user",
                content: user_message,
                provider: plan.provider.clone(),
                model: plan.model.clone(),
                source: "prompt-bundle",
                at_ms: options.finished_at_ms,
            },
        )?;
        append_json_line(
            &plan.outputs.trajectory_file,
            &CodexTrajectoryEvent {
                schema: CODEX_TRAJECTORY_EVENT_SCHEMA,
                queue_id: plan.queue_id.clone(),
                session_key: plan.session_key.clone(),
                agent_id: plan.agent_id.clone(),
                event: "user-message-recorded",
                role: Some("user"),
                provider: plan.provider.clone(),
                model: plan.model.clone(),
                at_ms: options.finished_at_ms,
                detail: "inbound message copied from prompt bundle".to_string(),
            },
        )?;
    }
    for narration in &options.assistant_narration {
        append_json_line(
            &plan.outputs.transcript_file,
            &CodexTranscriptMessage {
                schema: CODEX_TRANSCRIPT_MESSAGE_SCHEMA,
                queue_id: plan.queue_id.clone(),
                session_key: plan.session_key.clone(),
                agent_id: plan.agent_id.clone(),
                role: "assistant_narration",
                content: narration.text.clone(),
                provider: plan.provider.clone(),
                model: plan.model.clone(),
                source: "codex-runtime-narration",
                at_ms: options.finished_at_ms,
            },
        )?;
    }
    if !options.assistant_narration.is_empty() {
        append_json_line(
            &plan.outputs.trajectory_file,
            &CodexTrajectoryEvent {
                schema: CODEX_TRAJECTORY_EVENT_SCHEMA,
                queue_id: plan.queue_id.clone(),
                session_key: plan.session_key.clone(),
                agent_id: plan.agent_id.clone(),
                event: "assistant-narration-recorded",
                role: Some("assistant_narration"),
                provider: plan.provider.clone(),
                model: plan.model.clone(),
                at_ms: options.finished_at_ms,
                detail: format!(
                    "{} narration item(s) captured; mode={}",
                    options.assistant_narration.len(),
                    options.assistant_narration_mode.as_str()
                ),
            },
        )?;
    }
    append_json_line(
        &plan.outputs.transcript_file,
        &CodexTranscriptMessage {
            schema: CODEX_TRANSCRIPT_MESSAGE_SCHEMA,
            queue_id: plan.queue_id.clone(),
            session_key: plan.session_key.clone(),
            agent_id: plan.agent_id.clone(),
            role: "assistant",
            content: options.assistant_message.clone(),
            provider: plan.provider.clone(),
            model: plan.model.clone(),
            source: "codex-runtime-completion",
            at_ms: options.finished_at_ms,
        },
    )?;
    append_json_line(
        &plan.outputs.trajectory_file,
        &CodexTrajectoryEvent {
            schema: CODEX_TRAJECTORY_EVENT_SCHEMA,
            queue_id: plan.queue_id.clone(),
            session_key: plan.session_key.clone(),
            agent_id: plan.agent_id.clone(),
            event: "assistant-message-recorded",
            role: Some("assistant"),
            provider: plan.provider.clone(),
            model: plan.model.clone(),
            at_ms: options.finished_at_ms,
            detail: "assistant message recorded by codex completion sink".to_string(),
        },
    )?;
    let completion_file = completion_receipt_file(plan);
    fs::write(
        &plan.outputs.codex_binding_file,
        serde_json::to_string_pretty(&CodexBindingRecord {
            schema: CODEX_BINDING_SCHEMA,
            queue_id: plan.queue_id.clone(),
            session_key: plan.session_key.clone(),
            agent_id: plan.agent_id.clone(),
            provider: plan.provider.clone(),
            model: plan.model.clone(),
            thread_id: options.thread_id.clone(),
            working_directory: plan.invocation.working_directory.clone(),
            prompt_bundle_json: plan.prompt_bundle_json.clone(),
            prompt_markdown: plan.prompt_markdown.clone(),
            transcript_file: plan.outputs.transcript_file.clone(),
            trajectory_file: plan.outputs.trajectory_file.clone(),
            completion_file,
            completed_at_ms: options.finished_at_ms,
        })
        .map_err(io::Error::other)?,
    )?;
    Ok(())
}

fn parent_dir(path: &Path) -> io::Result<&Path> {
    path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("path has no parent: {}", path.display()),
        )
    })
}

fn prompt_bundle_user_message(prompt_bundle_json: &Path) -> io::Result<Option<String>> {
    let value = read_json_file(prompt_bundle_json)?;
    let Some(sections) = value.get("sections").and_then(Value::as_array) else {
        return Ok(None);
    };
    Ok(sections.iter().find_map(|section| {
        (string_field(section, &["kind"]) == Some("user-message"))
            .then(|| string_field(section, &["content"]).map(ToString::to_string))
            .flatten()
    }))
}

struct LaunchProbeProcessResult {
    status: LaunchProbeProcessStatus,
    process: CodexRuntimeLaunchProcess,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LaunchProbeProcessStatus {
    StartedAndStopped,
    ExitedEarly,
    SpawnFailed,
    TerminationFailed,
}

fn spawn_launch_probe(
    harness_home: &Path,
    plan: &CodexRuntimePlanFile,
    startup_probe_ms: u64,
) -> io::Result<LaunchProbeProcessResult> {
    let execution_dir = runtime_execution_dir(plan);
    fs::create_dir_all(&execution_dir)?;
    fs::create_dir_all(&plan.invocation.working_directory)?;
    let stdout_log = execution_dir.join("codex-runtime-launch.stdout.log");
    let stderr_log = execution_dir.join("codex-runtime-launch.stderr.log");
    let stdout = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&stdout_log)?;
    let stderr = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&stderr_log)?;
    let started = Instant::now();
    let mut command = Command::new(&plan.invocation.executable);
    command
        .args(&plan.invocation.arguments)
        .current_dir(&plan.invocation.working_directory)
        .stdin(Stdio::piped())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    apply_required_secret_env(
        &mut command,
        harness_home,
        &plan.invocation.env_requirements,
    );
    apply_codex_home_env(&mut command, plan);
    apply_live_agent_session_env(&mut command, harness_home);

    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) => {
            return Ok(LaunchProbeProcessResult {
                status: LaunchProbeProcessStatus::SpawnFailed,
                process: CodexRuntimeLaunchProcess {
                    executable: plan.invocation.executable.clone(),
                    arguments: plan.invocation.arguments.clone(),
                    working_directory: plan.invocation.working_directory.clone(),
                    pid: None,
                    startup_probe_ms,
                    elapsed_ms: started.elapsed().as_millis(),
                    exit_status: Some(error.to_string()),
                    terminated: false,
                    stdout_log: Some(stdout_log),
                    stderr_log: Some(stderr_log),
                },
            });
        }
    };
    let pid = child.id();
    let probe_window = Duration::from_millis(startup_probe_ms);
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(LaunchProbeProcessResult {
                status: LaunchProbeProcessStatus::ExitedEarly,
                process: CodexRuntimeLaunchProcess {
                    executable: plan.invocation.executable.clone(),
                    arguments: plan.invocation.arguments.clone(),
                    working_directory: plan.invocation.working_directory.clone(),
                    pid: Some(pid),
                    startup_probe_ms,
                    elapsed_ms: started.elapsed().as_millis(),
                    exit_status: Some(status.to_string()),
                    terminated: false,
                    stdout_log: Some(stdout_log),
                    stderr_log: Some(stderr_log),
                },
            });
        }
        if started.elapsed() >= probe_window {
            break;
        }
        thread::sleep(Duration::from_millis(25));
    }

    let kill_result = child.kill();
    let wait_result = child.wait();
    match (kill_result, wait_result) {
        (Ok(()), Ok(status)) => Ok(LaunchProbeProcessResult {
            status: LaunchProbeProcessStatus::StartedAndStopped,
            process: CodexRuntimeLaunchProcess {
                executable: plan.invocation.executable.clone(),
                arguments: plan.invocation.arguments.clone(),
                working_directory: plan.invocation.working_directory.clone(),
                pid: Some(pid),
                startup_probe_ms,
                elapsed_ms: started.elapsed().as_millis(),
                exit_status: Some(status.to_string()),
                terminated: true,
                stdout_log: Some(stdout_log),
                stderr_log: Some(stderr_log),
            },
        }),
        (Err(error), _) | (_, Err(error)) => Ok(LaunchProbeProcessResult {
            status: LaunchProbeProcessStatus::TerminationFailed,
            process: CodexRuntimeLaunchProcess {
                executable: plan.invocation.executable.clone(),
                arguments: plan.invocation.arguments.clone(),
                working_directory: plan.invocation.working_directory.clone(),
                pid: Some(pid),
                startup_probe_ms,
                elapsed_ms: started.elapsed().as_millis(),
                exit_status: Some(error.to_string()),
                terminated: false,
                stdout_log: Some(stdout_log),
                stderr_log: Some(stderr_log),
            },
        }),
    }
}

fn resolve_execution_dir(
    options: &CodexRuntimePlanOptions,
    warnings: &mut Vec<String>,
) -> io::Result<Option<PathBuf>> {
    if let Some(execution_dir) = &options.execution_dir {
        if execution_dir.join("execution-receipt.json").is_file() {
            return Ok(Some(execution_dir.clone()));
        }
        warnings.push(format!(
            "execution receipt not found under {}",
            execution_dir.display()
        ));
        return Ok(None);
    }

    let receipts_file = options
        .harness_home
        .join("state")
        .join("runtime-queue")
        .join("execution-receipts.jsonl");
    if !receipts_file.is_file() {
        warnings.push(format!(
            "execution receipts file not found at {}",
            receipts_file.display()
        ));
        return Ok(None);
    }
    let text = fs::read_to_string(&receipts_file)?;
    let mut latest = None;
    for (index, line) in text.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let value: Value = match serde_json::from_str(trimmed) {
            Ok(value) => value,
            Err(error) => {
                warnings.push(format!(
                    "execution receipt line {} is not valid JSON: {}",
                    index + 1,
                    error
                ));
                continue;
            }
        };
        if string_field(&value, &["status"]) == Some("prepared")
            && let Some(path) = path_field(&value, &["executionDir", "execution_dir"])
        {
            latest = Some(path);
        }
    }
    if latest.is_none() {
        warnings.push("no prepared execution receipt found".to_string());
    }
    Ok(latest)
}

fn resolve_preflight_plan_file(
    options: &CodexRuntimePreflightOptions,
    warnings: &mut Vec<String>,
) -> io::Result<Option<PathBuf>> {
    if let Some(plan_file) = &options.plan_file {
        if plan_file.is_file() {
            return Ok(Some(plan_file.clone()));
        }
        warnings.push(format!(
            "codex runtime plan file not found at {}",
            plan_file.display()
        ));
        return Ok(None);
    }

    if let Some(execution_dir) = &options.execution_dir {
        let plan_file = execution_dir.join("codex-runtime-plan.json");
        if plan_file.is_file() {
            return Ok(Some(plan_file));
        }
        warnings.push(format!(
            "codex runtime plan file not found under {}",
            execution_dir.display()
        ));
        return Ok(None);
    }

    let plan_options = CodexRuntimePlanOptions {
        harness_home: options.harness_home.clone(),
        execution_dir: None,
        codex_executable: None,
    };
    let Some(execution_dir) = resolve_execution_dir(&plan_options, warnings)? else {
        return Ok(None);
    };
    let plan_file = execution_dir.join("codex-runtime-plan.json");
    if plan_file.is_file() {
        return Ok(Some(plan_file));
    }
    warnings.push(format!(
        "latest prepared execution has no codex runtime plan at {}",
        plan_file.display()
    ));
    Ok(None)
}

fn codex_provider_config(provider: Option<&str>) -> Option<CodexProviderConfig> {
    match provider.map(str::to_ascii_lowercase).as_deref() {
        Some(provider) if provider.contains("openrouter") => Some(CodexProviderConfig {
            provider: "openrouter".to_string(),
            display_name: "OpenRouter".to_string(),
            base_url: "https://openrouter.ai/api/v1".to_string(),
            env_key: "OPENROUTER_API_KEY".to_string(),
            wire_api: "responses".to_string(),
        }),
        _ => None,
    }
}

fn env_requirements(provider: Option<&str>) -> Vec<CodexEnvRequirement> {
    match provider.map(str::to_ascii_lowercase).as_deref() {
        Some(provider) if provider.contains("openrouter") => vec![CodexEnvRequirement {
            name: "OPENROUTER_API_KEY".to_string(),
            reason: "queued agent turn uses an OpenRouter/OpenAI-compatible provider".to_string(),
        }],
        _ => vec![CodexEnvRequirement {
            name: "OPENAI_API_KEY".to_string(),
            reason:
                "Codex/OpenAI app-server execution requires OpenAI API key or Codex OAuth auth state"
                    .to_string(),
        }],
    }
}

pub fn inspect_codex_approval_policy(harness_home: &Path) -> CodexApprovalPolicyInspection {
    let mut warnings = Vec::new();
    let (policy, source, configured) =
        resolve_codex_approval_policy_with_source(harness_home, &mut warnings);
    CodexApprovalPolicyInspection {
        policy,
        source,
        configured,
        config_file: harness_home.join(HARNESS_CONFIG_FILE_NAME),
        warnings,
    }
}

fn resolve_codex_approval_policy(
    harness_home: &Path,
    warnings: &mut Vec<String>,
) -> CodexApprovalPolicy {
    let (policy, _, _) = resolve_codex_approval_policy_with_source(harness_home, warnings);
    policy
}

fn resolve_codex_approval_policy_with_source(
    harness_home: &Path,
    warnings: &mut Vec<String>,
) -> (CodexApprovalPolicy, String, bool) {
    if let Ok(raw) = env::var(CODEX_APPROVAL_POLICY_ENV) {
        return match parse_codex_approval_policy(&raw) {
            Some(policy) => (policy, CODEX_APPROVAL_POLICY_ENV.to_string(), true),
            None => {
                warnings.push(format!(
                    "invalid {CODEX_APPROVAL_POLICY_ENV}={raw:?}; defaulting Codex approval policy to deny"
                ));
                (
                    CodexApprovalPolicy::Deny,
                    CODEX_APPROVAL_POLICY_ENV.to_string(),
                    true,
                )
            }
        };
    }

    let config_file = harness_home.join(HARNESS_CONFIG_FILE_NAME);
    let text = match fs::read_to_string(&config_file) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return (CodexApprovalPolicy::Deny, "default".to_string(), false);
        }
        Err(error) => {
            warnings.push(format!(
                "failed to read {}: {error}; defaulting Codex approval policy to deny",
                config_file.display()
            ));
            return (
                CodexApprovalPolicy::Deny,
                config_file.display().to_string(),
                false,
            );
        }
    };
    let value = match serde_json::from_str::<Value>(&text) {
        Ok(value) => value,
        Err(error) => {
            warnings.push(format!(
                "failed to parse {} as JSON: {error}; defaulting Codex approval policy to deny",
                config_file.display()
            ));
            return (
                CodexApprovalPolicy::Deny,
                config_file.display().to_string(),
                false,
            );
        }
    };
    for pointer in [
        "/security/codexApprovalPolicy",
        "/security/codexApprovals",
        "/codex/approvalPolicy",
        "/codex/approvals",
        "/runtime/codexApprovalPolicy",
    ] {
        if let Some(raw_value) = value.pointer(pointer) {
            if let Some(policy) = codex_approval_policy_from_json(raw_value) {
                return (policy, format!("{}:{pointer}", config_file.display()), true);
            }
            warnings.push(format!(
                "invalid Codex approval policy at {}:{pointer}; defaulting to deny",
                config_file.display()
            ));
            return (
                CodexApprovalPolicy::Deny,
                format!("{}:{pointer}", config_file.display()),
                true,
            );
        }
    }
    (
        CodexApprovalPolicy::Deny,
        config_file.display().to_string(),
        false,
    )
}

fn codex_approval_policy_from_json(value: &Value) -> Option<CodexApprovalPolicy> {
    match value {
        Value::String(text) => parse_codex_approval_policy(text),
        Value::Bool(true) => Some(CodexApprovalPolicy::Accept),
        Value::Bool(false) => Some(CodexApprovalPolicy::Deny),
        _ => None,
    }
}

fn parse_codex_approval_policy(value: &str) -> Option<CodexApprovalPolicy> {
    let normalized = value.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "accept" | "allow" | "approve" | "approved" | "auto-accept" | "unattended-accept"
        | "open" | "enabled" | "on" | "true" => Some(CodexApprovalPolicy::Accept),
        "deny" | "decline" | "cancel" | "closed" | "disabled" | "off" | "false" => {
            Some(CodexApprovalPolicy::Deny)
        }
        _ => None,
    }
}

pub fn inspect_codex_sandbox(harness_home: &Path) -> CodexSandboxInspection {
    let mut warnings = Vec::new();
    let (sandbox, source, configured) =
        resolve_codex_sandbox_with_source(harness_home, &mut warnings);
    CodexSandboxInspection {
        sandbox,
        source,
        configured,
        config_file: harness_home.join(HARNESS_CONFIG_FILE_NAME),
        warnings,
    }
}

fn resolve_codex_sandbox(harness_home: &Path, warnings: &mut Vec<String>) -> String {
    let (sandbox, _, _) = resolve_codex_sandbox_with_source(harness_home, warnings);
    sandbox
}

pub fn inspect_codex_sandbox_policy(harness_home: &Path) -> CodexSandboxInspection {
    let mut warnings = Vec::new();
    let (sandbox, source, configured) =
        resolve_codex_sandbox_policy_with_source(harness_home, &mut warnings);
    CodexSandboxInspection {
        sandbox,
        source,
        configured,
        config_file: harness_home.join(HARNESS_CONFIG_FILE_NAME),
        warnings,
    }
}

fn resolve_codex_sandbox_policy(harness_home: &Path, warnings: &mut Vec<String>) -> String {
    let (sandbox, _, _) = resolve_codex_sandbox_policy_with_source(harness_home, warnings);
    sandbox
}

fn resolve_codex_sandbox_with_source(
    harness_home: &Path,
    warnings: &mut Vec<String>,
) -> (String, String, bool) {
    if let Ok(raw) = env::var(CODEX_SANDBOX_ENV) {
        return match parse_codex_sandbox(&raw) {
            Some(sandbox) => (sandbox, CODEX_SANDBOX_ENV.to_string(), true),
            None => {
                warnings.push(format!(
                    "invalid {CODEX_SANDBOX_ENV}={raw:?}; defaulting Codex sandbox to {DEFAULT_CODEX_SANDBOX}"
                ));
                (
                    DEFAULT_CODEX_SANDBOX.to_string(),
                    CODEX_SANDBOX_ENV.to_string(),
                    true,
                )
            }
        };
    }

    let config_file = harness_home.join(HARNESS_CONFIG_FILE_NAME);
    let text = match fs::read_to_string(&config_file) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return (
                DEFAULT_CODEX_SANDBOX.to_string(),
                "default".to_string(),
                false,
            );
        }
        Err(error) => {
            warnings.push(format!(
                "failed to read {}: {error}; defaulting Codex sandbox to {DEFAULT_CODEX_SANDBOX}",
                config_file.display()
            ));
            return (
                DEFAULT_CODEX_SANDBOX.to_string(),
                config_file.display().to_string(),
                false,
            );
        }
    };
    let value = match serde_json::from_str::<Value>(&text) {
        Ok(value) => value,
        Err(error) => {
            warnings.push(format!(
                "failed to parse {} as JSON: {error}; defaulting Codex sandbox to {DEFAULT_CODEX_SANDBOX}",
                config_file.display()
            ));
            return (
                DEFAULT_CODEX_SANDBOX.to_string(),
                config_file.display().to_string(),
                false,
            );
        }
    };
    for pointer in [
        "/security/codexSandbox",
        "/security/codexSandboxMode",
        "/codex/sandbox",
        "/codex/sandboxMode",
        "/runtime/codexSandbox",
    ] {
        if let Some(raw_value) = value.pointer(pointer) {
            if let Some(sandbox) = codex_sandbox_from_json(raw_value) {
                return (
                    sandbox,
                    format!("{}:{pointer}", config_file.display()),
                    true,
                );
            }
            warnings.push(format!(
                "invalid Codex sandbox at {}:{pointer}; defaulting to {DEFAULT_CODEX_SANDBOX}",
                config_file.display()
            ));
            return (
                DEFAULT_CODEX_SANDBOX.to_string(),
                format!("{}:{pointer}", config_file.display()),
                true,
            );
        }
    }
    (
        DEFAULT_CODEX_SANDBOX.to_string(),
        config_file.display().to_string(),
        false,
    )
}

fn codex_sandbox_from_json(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => parse_codex_sandbox(text),
        Value::Bool(true) => Some(DEFAULT_CODEX_SANDBOX.to_string()),
        Value::Bool(false) => Some("danger-full-access".to_string()),
        _ => None,
    }
}

fn parse_codex_sandbox(value: &str) -> Option<String> {
    let normalized = value.trim().to_ascii_lowercase().replace('_', "-");
    let sandbox = match normalized.as_str() {
        "" => return None,
        "default" | "windows-elevated" => DEFAULT_CODEX_SANDBOX,
        "readonly" => "read-only",
        "workspace" | "workspace-write" => "workspace-write",
        "full-access" | "full" | "none" | "off" | "disabled" | "false" => "danger-full-access",
        "elevated" | "read-only" | "danger-full-access" => normalized.as_str(),
        other if is_safe_codex_sandbox_value(other) => other,
        _ => return None,
    };
    Some(sandbox.to_string())
}

fn is_safe_codex_sandbox_value(value: &str) -> bool {
    value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
}

fn resolve_codex_sandbox_policy_with_source(
    harness_home: &Path,
    warnings: &mut Vec<String>,
) -> (String, String, bool) {
    if let Ok(raw) = env::var(CODEX_SANDBOX_POLICY_ENV) {
        return match parse_codex_sandbox_policy(&raw) {
            Some(sandbox) => (sandbox, CODEX_SANDBOX_POLICY_ENV.to_string(), true),
            None => {
                warnings.push(format!(
                    "invalid {CODEX_SANDBOX_POLICY_ENV}={raw:?}; defaulting Codex app-server sandbox policy to {DEFAULT_CODEX_SANDBOX_POLICY}"
                ));
                (
                    DEFAULT_CODEX_SANDBOX_POLICY.to_string(),
                    CODEX_SANDBOX_POLICY_ENV.to_string(),
                    true,
                )
            }
        };
    }

    let config_file = harness_home.join(HARNESS_CONFIG_FILE_NAME);
    let text = match fs::read_to_string(&config_file) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return (
                DEFAULT_CODEX_SANDBOX_POLICY.to_string(),
                "default".to_string(),
                false,
            );
        }
        Err(error) => {
            warnings.push(format!(
                "failed to read {}: {error}; defaulting Codex app-server sandbox policy to {DEFAULT_CODEX_SANDBOX_POLICY}",
                config_file.display()
            ));
            return (
                DEFAULT_CODEX_SANDBOX_POLICY.to_string(),
                config_file.display().to_string(),
                false,
            );
        }
    };
    let value = match serde_json::from_str::<Value>(&text) {
        Ok(value) => value,
        Err(error) => {
            warnings.push(format!(
                "failed to parse {} as JSON: {error}; defaulting Codex app-server sandbox policy to {DEFAULT_CODEX_SANDBOX_POLICY}",
                config_file.display()
            ));
            return (
                DEFAULT_CODEX_SANDBOX_POLICY.to_string(),
                config_file.display().to_string(),
                false,
            );
        }
    };
    for pointer in [
        "/security/codexSandboxPolicy",
        "/security/codexFilesystemSandbox",
        "/codex/sandboxPolicy",
        "/codex/filesystemSandbox",
        "/runtime/codexSandboxPolicy",
    ] {
        if let Some(raw_value) = value.pointer(pointer) {
            if let Some(sandbox) = codex_sandbox_policy_from_json(raw_value) {
                return (
                    sandbox,
                    format!("{}:{pointer}", config_file.display()),
                    true,
                );
            }
            warnings.push(format!(
                "invalid Codex app-server sandbox policy at {}:{pointer}; defaulting to {DEFAULT_CODEX_SANDBOX_POLICY}",
                config_file.display()
            ));
            return (
                DEFAULT_CODEX_SANDBOX_POLICY.to_string(),
                format!("{}:{pointer}", config_file.display()),
                true,
            );
        }
    }
    (
        DEFAULT_CODEX_SANDBOX_POLICY.to_string(),
        config_file.display().to_string(),
        false,
    )
}

fn codex_sandbox_policy_from_json(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => parse_codex_sandbox_policy(text),
        Value::Bool(true) => Some(DEFAULT_CODEX_SANDBOX_POLICY.to_string()),
        Value::Bool(false) => Some("dangerFullAccess".to_string()),
        _ => None,
    }
}

fn parse_codex_sandbox_policy(value: &str) -> Option<String> {
    let normalized = value
        .trim()
        .to_ascii_lowercase()
        .replace('_', "-")
        .replace(' ', "-");
    let sandbox = match normalized.as_str() {
        "" => return None,
        "default" | "workspace" | "workspace-write" | "workspacewrite" => "workspaceWrite",
        "readonly" | "read-only" | "read" => "readOnly",
        "dangerfullaccess" | "danger-full-access" | "full-access" | "full" | "none" | "off"
        | "disabled" | "false" => "dangerFullAccess",
        other if is_safe_codex_sandbox_value(other) => other,
        _ => return None,
    };
    Some(sandbox.to_string())
}

fn normalize_codex_sandbox_policy(value: &str) -> String {
    value
        .trim()
        .to_ascii_lowercase()
        .replace('-', "")
        .replace('_', "")
        .replace(' ', "")
}

fn check_executable(executable: &Path) -> CodexRuntimePreflightCheck {
    match resolve_executable(executable) {
        Some(path) => pass_check(
            "codex-executable",
            format!("resolved {} to {}", executable.display(), path.display()),
        ),
        None => fail_check(
            "codex-executable",
            format!("could not resolve executable {}", executable.display()),
        ),
    }
}

fn resolve_executable(executable: &Path) -> Option<PathBuf> {
    if executable.components().count() > 1 || executable.is_absolute() {
        return executable.is_file().then(|| executable.to_path_buf());
    }

    let paths = env::var_os("PATH")?;
    for dir in env::split_paths(&paths) {
        for candidate in executable_candidates(&dir, executable) {
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    None
}

fn executable_candidates(dir: &Path, executable: &Path) -> Vec<PathBuf> {
    let direct = dir.join(executable);
    #[cfg(windows)]
    {
        if executable.extension().is_some() {
            return vec![direct];
        }
        let pathext = env::var_os("PATHEXT")
            .map(|value| {
                value
                    .to_string_lossy()
                    .split(';')
                    .filter(|ext| !ext.is_empty())
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_else(|| vec![".EXE".to_string(), ".CMD".to_string(), ".BAT".to_string()]);
        let mut candidates = vec![direct];
        let name = executable.to_string_lossy();
        candidates.extend(
            pathext
                .into_iter()
                .map(|ext| dir.join(format!("{name}{ext}"))),
        );
        candidates
    }
    #[cfg(not(windows))]
    {
        vec![direct]
    }
}

fn check_existing_file(name: &str, path: &Path) -> CodexRuntimePreflightCheck {
    if path.is_file() {
        pass_check(name, format!("found {}", path.display()))
    } else {
        fail_check(name, format!("file not found at {}", path.display()))
    }
}

fn check_output_paths(
    harness_home: &Path,
    outputs: &CodexOutputPlan,
) -> io::Result<Vec<CodexRuntimePreflightCheck>> {
    let mut checks = Vec::new();
    for (name, path) in [
        ("transcript-output", &outputs.transcript_file),
        ("trajectory-output", &outputs.trajectory_file),
        ("codex-binding-output", &outputs.codex_binding_file),
        ("runtime-receipt-output", &outputs.runtime_receipt_file),
    ] {
        checks.push(check_output_parent_writable(harness_home, name, path)?);
    }
    Ok(checks)
}

fn check_output_parent_writable(
    harness_home: &Path,
    name: &str,
    path: &Path,
) -> io::Result<CodexRuntimePreflightCheck> {
    let Some(parent) = path.parent() else {
        return Ok(fail_check(
            name,
            format!("output path has no parent: {}", path.display()),
        ));
    };
    if !path_within(parent, harness_home)? {
        return Ok(fail_check(
            name,
            format!(
                "refusing preflight write outside harness home: {}",
                parent.display()
            ),
        ));
    }
    fs::create_dir_all(parent)?;
    let probe = parent.join(".agent-harness-preflight.tmp");
    match fs::write(&probe, b"preflight") {
        Ok(()) => {
            let _ = fs::remove_file(&probe);
            Ok(pass_check(
                name,
                format!("parent directory is writable: {}", parent.display()),
            ))
        }
        Err(error) => Ok(fail_check(
            name,
            format!(
                "parent directory is not writable: {} ({error})",
                parent.display()
            ),
        )),
    }
}

fn path_within(candidate: &Path, root: &Path) -> io::Result<bool> {
    let candidate = absolute_lexical_path(candidate)?;
    let root = absolute_lexical_path(root)?;
    Ok(candidate.starts_with(root))
}

fn absolute_lexical_path(path: &Path) -> io::Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        env::current_dir()?.join(path)
    };
    Ok(normalize_lexical_path(&absolute))
}

fn normalize_lexical_path(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            std::path::Component::RootDir => normalized.push(component.as_os_str()),
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            std::path::Component::Normal(part) => normalized.push(part),
        }
    }
    normalized
}

fn check_env_requirements(
    harness_home: &Path,
    requirements: &[CodexEnvRequirement],
) -> Vec<CodexRuntimePreflightCheck> {
    if requirements.is_empty() {
        return vec![pass_check(
            "environment",
            "runtime plan declares no required environment variables",
        )];
    }
    requirements
        .iter()
        .flat_map(|requirement| check_credential_requirement(harness_home, requirement))
        .collect()
}

fn check_credential_requirement(
    harness_home: &Path,
    requirement: &CodexEnvRequirement,
) -> Vec<CodexRuntimePreflightCheck> {
    if requirement.name == "OPENAI_API_KEY" {
        return check_openai_or_codex_oauth_requirement(requirement);
    }
    vec![check_env_requirement(harness_home, requirement)]
}

fn check_openai_or_codex_oauth_requirement(
    requirement: &CodexEnvRequirement,
) -> Vec<CodexRuntimePreflightCheck> {
    if env::var_os("OPENAI_API_KEY").is_some() {
        return vec![pass_check(
            "credential:openai-or-codex-oauth",
            "OPENAI_API_KEY is present",
        )];
    }
    let candidates = codex_oauth_auth_candidates();
    check_openai_or_codex_oauth_requirement_with_candidates(requirement, &candidates)
}

fn check_openai_or_codex_oauth_requirement_with_candidates(
    requirement: &CodexEnvRequirement,
    candidates: &[PathBuf],
) -> Vec<CodexRuntimePreflightCheck> {
    if let Some(path) = candidates.iter().find(|path| path.is_file()) {
        return vec![pass_check(
            "credential:openai-or-codex-oauth",
            format!("Codex OAuth auth state found at {}", path.display()),
        )];
    }
    vec![fail_check(
        "credential:openai-or-codex-oauth",
        format!(
            "{} is missing and no Codex OAuth auth state was found at {}: {}",
            requirement.name,
            candidates
                .iter()
                .map(|path| path.display().to_string())
                .collect::<Vec<_>>()
                .join(", "),
            requirement.reason
        ),
    )]
}

fn check_env_requirement(
    harness_home: &Path,
    requirement: &CodexEnvRequirement,
) -> CodexRuntimePreflightCheck {
    if env::var_os(&requirement.name).is_some() {
        pass_check(
            format!("env:{}", requirement.name),
            format!("{} is present", requirement.name),
        )
    } else if secret_env_value(harness_home, &requirement.name).is_some() {
        pass_check(
            format!("env:{}", requirement.name),
            format!(
                "{} is present in harness secrets env; value is not disclosed",
                requirement.name
            ),
        )
    } else {
        fail_check(
            format!("env:{}", requirement.name),
            format!("{} is missing: {}", requirement.name, requirement.reason),
        )
    }
}

fn apply_required_secret_env(
    command: &mut Command,
    harness_home: &Path,
    requirements: &[CodexEnvRequirement],
) {
    for requirement in requirements {
        if env::var_os(&requirement.name).is_none() {
            if let Some(value) = secret_env_value(harness_home, &requirement.name) {
                command.env(&requirement.name, value);
            }
        }
    }
}

fn secret_env_value(harness_home: &Path, name: &str) -> Option<String> {
    for relative in [
        ["secrets", "channel-credentials.env"],
        ["secrets", "memory-credentials.env"],
    ] {
        let path = relative
            .iter()
            .fold(harness_home.to_path_buf(), |path, part| path.join(part));
        if let Some(value) = secret_env_file_value(&path, name) {
            return Some(value);
        }
    }
    None
}

fn secret_env_file_value(path: &Path, name: &str) -> Option<String> {
    let text = fs::read_to_string(path).ok()?;
    let prefix = format!("{name}=");
    text.lines().find_map(|line| {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            return None;
        }
        trimmed
            .strip_prefix(&prefix)
            .map(|value| value.trim_matches('"').to_string())
            .filter(|value| !value.is_empty())
    })
}

fn codex_oauth_auth_candidates() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Some(home) = env::var_os("CODEX_HOME") {
        roots.push(PathBuf::from(home));
    }
    if let Some(profile) = env::var_os("USERPROFILE") {
        roots.push(PathBuf::from(profile).join(".codex"));
    }
    if let Some(home) = env::var_os("HOME") {
        roots.push(PathBuf::from(home).join(".codex"));
    }
    roots.dedup();
    roots
        .into_iter()
        .flat_map(|root| [root.join("auth.json"), root.join("auth.toml")])
        .collect()
}

fn pass_check(name: impl Into<String>, detail: impl Into<String>) -> CodexRuntimePreflightCheck {
    CodexRuntimePreflightCheck {
        name: name.into(),
        status: CodexRuntimePreflightCheckStatus::Pass,
        detail: detail.into(),
    }
}

fn fail_check(name: impl Into<String>, detail: impl Into<String>) -> CodexRuntimePreflightCheck {
    CodexRuntimePreflightCheck {
        name: name.into(),
        status: CodexRuntimePreflightCheckStatus::Fail,
        detail: detail.into(),
    }
}

fn transcript_file(harness_home: &Path, agent_id: Option<&str>, session_key: &str) -> PathBuf {
    let session_dir = if session_key.starts_with("cron:") {
        "cron-sessions"
    } else {
        "sessions"
    };
    harness_home
        .join("agents")
        .join(agent_id.unwrap_or("unknown"))
        .join(session_dir)
        .join(format!("{}.jsonl", normalize_key_part(session_key)))
}

fn trajectory_file(transcript_file: &Path) -> PathBuf {
    let mut out = transcript_file.to_path_buf();
    let name = transcript_file
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("session.jsonl");
    let stem = name.strip_suffix(".jsonl").unwrap_or(name);
    out.set_file_name(format!("{stem}.trajectory.jsonl"));
    out
}

fn codex_binding_file(transcript_file: &Path) -> PathBuf {
    with_appended_file_name(transcript_file, ".codex-app-server.json")
}

fn with_appended_file_name(path: &Path, suffix: &str) -> PathBuf {
    let mut out = path.to_path_buf();
    let name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("session");
    out.set_file_name(format!("{name}{suffix}"));
    out
}

fn read_json_file(path: &Path) -> io::Result<Value> {
    let text = fs::read_to_string(path)?;
    serde_json::from_str(&text).map_err(io::Error::other)
}

fn read_json_file_as<T: for<'de> Deserialize<'de>>(path: &Path) -> io::Result<T> {
    let text = fs::read_to_string(path)?;
    serde_json::from_str(&text).map_err(io::Error::other)
}

fn inbound_media_artifacts_from_prepared_receipt(
    prepared_receipt: &Value,
    warnings: &mut Vec<String>,
) -> Vec<InboundMediaArtifact> {
    let Some(value) = prepared_receipt
        .get("inboundMediaArtifacts")
        .or_else(|| prepared_receipt.get("inbound_media_artifacts"))
    else {
        return Vec::new();
    };
    match serde_json::from_value::<Vec<InboundMediaArtifact>>(value.clone()) {
        Ok(artifacts) => artifacts,
        Err(error) => {
            warnings.push(format!(
                "prepared execution receipt inboundMediaArtifacts could not be parsed; media input planner disabled: {error}"
            ));
            Vec::new()
        }
    }
}

fn codex_native_media_input_enabled() -> bool {
    env::var("AGENT_HARNESS_CODEX_NATIVE_MEDIA_INPUT")
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "enabled" | "on"
            )
        })
        .unwrap_or(false)
}

fn append_json_line(path: &Path, value: &impl Serialize) -> io::Result<()> {
    crate::append_jsonl_value(path, value)
}

fn runtime_working_directory(
    runtime_workspace: Option<&Path>,
    bundle: &Value,
    execution_dir: &Path,
    warnings: &mut Vec<String>,
) -> PathBuf {
    if let Some(runtime_workspace) = runtime_workspace {
        if runtime_workspace.is_dir() {
            return runtime_workspace.to_path_buf();
        }
        warnings.push(format!(
            "runtime workspace does not exist; falling back to prompt source workspace: {}",
            runtime_workspace.display()
        ));
    }
    if let Some(source_workspace) = path_field(bundle, &["sourceWorkspace", "source_workspace"]) {
        if source_workspace.is_dir() {
            return source_workspace;
        }
        warnings.push(format!(
            "prompt bundle source workspace does not exist; falling back to execution dir: {}",
            source_workspace.display()
        ));
    } else {
        warnings.push(
            "prompt bundle did not include sourceWorkspace; falling back to execution dir"
                .to_string(),
        );
    }
    execution_dir.to_path_buf()
}

fn read_existing_codex_thread_id(
    codex_binding_file: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<Option<String>> {
    if !codex_binding_file.is_file() {
        return Ok(None);
    }
    let value = match read_json_file(codex_binding_file) {
        Ok(value) => value,
        Err(error) => {
            warnings.push(format!(
                "could not read existing Codex binding for thread resume at {}: {}",
                codex_binding_file.display(),
                error
            ));
            return Ok(None);
        }
    };
    Ok(
        string_field(&value, &["threadId", "thread_id", "codexThreadId"])
            .map(str::trim)
            .filter(|thread_id| !thread_id.is_empty())
            .map(ToString::to_string),
    )
}

fn runtime_execution_dir(plan: &CodexRuntimePlanFile) -> PathBuf {
    plan.outputs
        .runtime_receipt_file
        .parent()
        .map(Path::to_path_buf)
        .or_else(|| {
            plan.invocation
                .prompt_input_file
                .parent()
                .map(Path::to_path_buf)
        })
        .unwrap_or_else(|| plan.invocation.working_directory.clone())
}

fn completion_receipt_file(plan: &CodexRuntimePlanFile) -> PathBuf {
    runtime_execution_dir(plan).join("codex-runtime-completion-receipt.json")
}

fn harness_codex_home(
    harness_home: &Path,
    provider_config: Option<&CodexProviderConfig>,
) -> Option<PathBuf> {
    let harness_home = absolute_for_config(harness_home);
    if let Some(provider_config) = provider_config {
        return Some(
            harness_home
                .join("codex-home-providers")
                .join(safe_path_component(&provider_config.provider)),
        );
    }
    let codex_home = harness_home.join("codex-home");
    if [codex_home.join("auth.json"), codex_home.join("auth.toml")]
        .iter()
        .any(|path| path.is_file())
    {
        Some(codex_home)
    } else {
        None
    }
}

fn ensure_harness_codex_config(
    codex_home: Option<&Path>,
    working_directory: &Path,
    harness_home: &Path,
    provider_config: Option<&CodexProviderConfig>,
    warnings: &mut Vec<String>,
) -> io::Result<()> {
    let Some(codex_home) = codex_home else {
        return Ok(());
    };
    let sandbox = resolve_codex_sandbox(harness_home, warnings);
    let context_policy = match load_codex_context_policy(harness_home) {
        Ok(policy) => policy,
        Err(error) => {
            warnings.push(format!(
                "failed to load codexContext policy for Codex config generation: {error}; using defaults"
            ));
            CodexContextPolicy::default()
        }
    };
    let config_file = codex_home.join("config.toml");
    if config_file.is_file() {
        let existing = fs::read_to_string(&config_file)?;
        if let Some(provider_config) = provider_config {
            let updated = ensure_codex_context_config_in_toml(
                &ensure_provider_config_in_toml(&existing, provider_config),
                &context_policy,
            );
            if updated != existing {
                fs::write(&config_file, updated)?;
                warnings.push(format!(
                    "updated harness-local Codex config provider/context stanza at {}",
                    config_file.display()
                ));
            }
            return Ok(());
        }
        if is_generated_harness_codex_config(&existing) {
            let desired = harness_codex_config_toml(
                working_directory,
                harness_home,
                &sandbox,
                provider_config,
                &context_policy,
            );
            if existing != desired {
                fs::write(&config_file, desired)?;
                warnings.push(format!(
                    "updated generated harness-local Codex config at {}",
                    config_file.display()
                ));
            }
        }
        return Ok(());
    }

    fs::create_dir_all(codex_home)?;
    let config = harness_codex_config_toml(
        working_directory,
        harness_home,
        &sandbox,
        provider_config,
        &context_policy,
    );
    fs::write(&config_file, config)?;
    warnings.push(format!(
        "created harness-local Codex config at {} with Windows sandbox={sandbox:?} and trusted runtime workspace",
        config_file.display(),
    ));
    Ok(())
}

fn is_generated_harness_codex_config(existing: &str) -> bool {
    existing.starts_with("# Generated by agent-harness.")
        || existing.starts_with("# Generated by openclaw-harness.")
}

fn ensure_provider_config_in_toml(existing: &str, provider_config: &CodexProviderConfig) -> String {
    let mut config = ensure_top_level_toml_key(
        existing,
        "model_provider",
        &toml_basic_string(&provider_config.provider),
    );
    let table_header = format!("[model_providers.{}]", provider_config.provider);
    if !config.lines().any(|line| line.trim() == table_header) {
        if !config.ends_with('\n') {
            config.push('\n');
        }
        config.push('\n');
        config.push_str(&table_header);
        config.push('\n');
        config.push_str("name = ");
        config.push_str(&toml_basic_string(&provider_config.display_name));
        config.push_str("\nbase_url = ");
        config.push_str(&toml_basic_string(&provider_config.base_url));
        config.push_str("\nenv_key = ");
        config.push_str(&toml_basic_string(&provider_config.env_key));
        config.push_str("\nwire_api = ");
        config.push_str(&toml_basic_string(&provider_config.wire_api));
        config.push('\n');
    } else {
        config = ensure_table_toml_key(
            &config,
            &table_header,
            "name",
            &toml_basic_string(&provider_config.display_name),
        );
        config = ensure_table_toml_key(
            &config,
            &table_header,
            "base_url",
            &toml_basic_string(&provider_config.base_url),
        );
        config = ensure_table_toml_key(
            &config,
            &table_header,
            "env_key",
            &toml_basic_string(&provider_config.env_key),
        );
        config = ensure_table_toml_key(
            &config,
            &table_header,
            "wire_api",
            &toml_basic_string(&provider_config.wire_api),
        );
    }
    config
}

fn ensure_codex_context_config_in_toml(existing: &str, policy: &CodexContextPolicy) -> String {
    let mut config = existing.to_string();
    for (key, value) in codex_context_toml_entries(policy) {
        config = ensure_top_level_toml_key(&config, &key, &value);
    }
    config
}

fn ensure_top_level_toml_key(existing: &str, key: &str, rendered_value: &str) -> String {
    let desired_line = format!("{key} = {rendered_value}");
    let mut lines: Vec<String> = existing.lines().map(ToString::to_string).collect();
    for line in &mut lines {
        let trimmed = line.trim_start();
        if trimmed.starts_with(&format!("{key} ")) || trimmed.starts_with(&format!("{key}=")) {
            *line = desired_line;
            return join_toml_lines(lines, existing.ends_with('\n'));
        }
        if trimmed.starts_with('[') {
            break;
        }
    }

    let insert_at = lines
        .iter()
        .position(|line| {
            let trimmed = line.trim();
            !(trimmed.is_empty() || trimmed.starts_with('#'))
        })
        .unwrap_or(lines.len());
    lines.insert(insert_at, desired_line);
    join_toml_lines(lines, true)
}

fn ensure_table_toml_key(
    existing: &str,
    table_header: &str,
    key: &str,
    rendered_value: &str,
) -> String {
    let desired_line = format!("{key} = {rendered_value}");
    let mut lines: Vec<String> = existing.lines().map(ToString::to_string).collect();
    let Some(start) = lines.iter().position(|line| line.trim() == table_header) else {
        return existing.to_string();
    };
    let end = lines
        .iter()
        .enumerate()
        .skip(start + 1)
        .find_map(|(index, line)| line.trim_start().starts_with('[').then_some(index))
        .unwrap_or(lines.len());
    for line in lines.iter_mut().take(end).skip(start + 1) {
        let trimmed = line.trim_start();
        if trimmed.starts_with(&format!("{key} ")) || trimmed.starts_with(&format!("{key}=")) {
            *line = desired_line;
            return join_toml_lines(lines, existing.ends_with('\n'));
        }
    }
    lines.insert(end, desired_line);
    join_toml_lines(lines, true)
}

fn join_toml_lines(lines: Vec<String>, trailing_newline: bool) -> String {
    let mut text = lines.join("\n");
    if trailing_newline && !text.ends_with('\n') {
        text.push('\n');
    }
    text
}

fn harness_codex_config_toml(
    working_directory: &Path,
    harness_home: &Path,
    sandbox: &str,
    provider_config: Option<&CodexProviderConfig>,
    context_policy: &CodexContextPolicy,
) -> String {
    let mut project_roots = vec![
        absolute_for_config(working_directory),
        absolute_for_config(harness_home),
    ];
    project_roots.sort();
    project_roots.dedup();

    let mut config = String::from(
        "# Generated by agent-harness. Contains no secrets.\n\
         # Codex OAuth state stays in auth.json/auth.toml.\n\
         \n",
    );
    if let Some(provider_config) = provider_config {
        config.push_str("model_provider = ");
        config.push_str(&toml_basic_string(&provider_config.provider));
        config.push_str("\n\n");
    }
    for (key, value) in codex_context_toml_entries(context_policy) {
        config.push_str(&key);
        config.push_str(" = ");
        config.push_str(&value);
        config.push('\n');
    }
    if !codex_context_toml_entries(context_policy).is_empty() {
        config.push('\n');
    }
    config.push_str(&format!(
        "[windows]\n\
         sandbox = {}\n\
         \n\
         [features]\n\
         multi_agent = true\n\
         memories = true\n",
        toml_basic_string(sandbox)
    ));
    if let Some(provider_config) = provider_config {
        config.push_str("\n[model_providers.");
        config.push_str(&provider_config.provider);
        config.push_str("]\n");
        config.push_str("name = ");
        config.push_str(&toml_basic_string(&provider_config.display_name));
        config.push_str("\nbase_url = ");
        config.push_str(&toml_basic_string(&provider_config.base_url));
        config.push_str("\nenv_key = ");
        config.push_str(&toml_basic_string(&provider_config.env_key));
        config.push_str("\nwire_api = ");
        config.push_str(&toml_basic_string(&provider_config.wire_api));
        config.push('\n');
    }
    for root in project_roots {
        config.push_str("\n[projects.");
        config.push_str(&toml_basic_string(&root.to_string_lossy()));
        config.push_str("]\ntrust_level = \"trusted\"\n");
    }
    config
}

fn codex_context_toml_entries(policy: &CodexContextPolicy) -> Vec<(String, String)> {
    let mut entries = Vec::new();
    if let Some(value) = policy.model_context_window {
        entries.push(("model_context_window".to_string(), value.to_string()));
    }
    if let Some(value) = policy.model_auto_compact_token_limit {
        entries.push((
            "model_auto_compact_token_limit".to_string(),
            value.to_string(),
        ));
    }
    if let Some(value) = policy.model_auto_compact_token_limit_scope.as_deref() {
        entries.push((
            "model_auto_compact_token_limit_scope".to_string(),
            toml_basic_string(value),
        ));
    }
    if let Some(value) = policy.tool_output_token_limit {
        entries.push(("tool_output_token_limit".to_string(), value.to_string()));
    }
    if let Some(value) = policy.compact_prompt.as_deref() {
        entries.push(("compact_prompt".to_string(), toml_basic_string(value)));
    }
    if let Some(value) = policy.experimental_compact_prompt_file.as_ref() {
        entries.push((
            "experimental_compact_prompt_file".to_string(),
            toml_basic_string(&value.to_string_lossy()),
        ));
    }
    entries
}

fn absolute_for_config(path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
    }
}

fn safe_path_component(value: &str) -> String {
    let component: String = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_') {
                ch
            } else {
                '_'
            }
        })
        .collect();
    if component.is_empty() {
        "provider".to_string()
    } else {
        component
    }
}

fn toml_basic_string(value: &str) -> String {
    let mut out = String::from("\"");
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ch if ch.is_control() => {
                out.push_str(&format!("\\u{:04X}", ch as u32));
            }
            ch => out.push(ch),
        }
    }
    out.push('"');
    out
}

fn apply_codex_home_env(command: &mut Command, plan: &CodexRuntimePlanFile) {
    if let Some(codex_home) = &plan.invocation.codex_home {
        command.env("CODEX_HOME", codex_home);
    }
}

fn apply_live_agent_session_env(command: &mut Command, harness_home: &Path) {
    if is_live_harness_home(harness_home) {
        command.env("AGENT_HARNESS_LIVE_SESSION", "1");
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        InboundMediaModelAttachmentStatus, PromptAssemblyOptions, RuntimeQueueEnqueueOptions,
        RuntimeQueuePrepareOptions, TurnPlanInput, build_channel_step, build_source_skill_index,
        build_turn_plan, enqueue_channel_step, load_agent_registry, prepare_runtime_queue_item,
        runtime_worker::release_runtime_queue_lease,
    };
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn token_usage_event_updates_usage_without_turn_completed() {
        let event = json!({
            "method": "thread/tokenUsage/updated",
            "params": {
                "tokenUsage": {
                    "input": 1200,
                    "output": 34,
                    "total": 1234
                }
            }
        });

        let usage = extract_protocol_usage(&event).unwrap();

        assert_eq!(usage.input_tokens, Some(1200));
        assert_eq!(usage.output_tokens, Some(34));
        assert_eq!(usage.total_tokens, Some(1234));
        assert!(usage.source.contains("params.tokenUsage"));
    }

    #[test]
    fn rollout_thread_health_flags_inline_image_bloat_and_token_count() {
        let root = temp_root("rollout_thread_health_flags_inline_image_bloat_and_token_count");
        let rollout_file = root.join("rollout-2026-06-24T11-06-52-019ef798-thread-health.jsonl");
        fs::create_dir_all(rollout_file.parent().unwrap()).unwrap();
        let image_payload = "A".repeat(
            usize::try_from(CODEX_THREAD_INLINE_IMAGE_LAST_TURN_BYTES_LIMIT).unwrap() + 128,
        );
        let image_line = json!({
            "type": "function_call_output",
            "output": format!("data:image/png;base64,{image_payload}")
        })
        .to_string();
        let token_line = json!({
            "msg": {
                "type": "event_msg",
                "payload": {
                    "type": "token_count",
                    "input": 6379328_u64,
                    "total": 6414501_u64
                }
            }
        })
        .to_string();
        fs::write(&rollout_file, format!("{image_line}\n{token_line}\n")).unwrap();
        let mut warnings = Vec::new();

        let report = scan_codex_rollout_file_for_thread_health(
            Some("019ef798-thread-health".to_string()),
            rollout_file,
            &mut warnings,
        );

        assert!(warnings.is_empty());
        assert!(report.compact_recommended);
        assert_eq!(report.data_image_count, 1);
        assert!(
            report.last_turn_inline_image_bytes > CODEX_THREAD_INLINE_IMAGE_LAST_TURN_BYTES_LIMIT
        );
        assert_eq!(report.latest_usage.unwrap().total_tokens, Some(6414501));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn context_preflight_compacts_for_bound_thread_inline_image_bloat() {
        let root = temp_root("context_preflight_compacts_for_bound_thread_inline_image_bloat");
        let source = write_codex_runtime_source(&root);
        let harness_home = root.join(".agent-harness");
        let thread_id = "019ef798-346b-7ae2-b341-60a7a20a8d96";
        let rollout_dir = harness_home
            .join("codex-home")
            .join("sessions")
            .join("2026")
            .join("06")
            .join("24");
        fs::create_dir_all(&rollout_dir).unwrap();
        let image_payload = "B".repeat(
            usize::try_from(CODEX_THREAD_INLINE_IMAGE_LAST_TURN_BYTES_LIMIT).unwrap() + 128,
        );
        let rollout_file =
            rollout_dir.join(format!("rollout-2026-06-24T11-06-52-{thread_id}.jsonl"));
        fs::write(
            &rollout_file,
            format!(
                "{}\n",
                json!({
                    "type": "function_call_output",
                    "output": format!("data:image/png;base64,{image_payload}")
                })
            ),
        )
        .unwrap();
        enqueue_and_prepare(&source, &harness_home);
        let plan_report = plan_codex_runtime(CodexRuntimePlanOptions {
            harness_home: harness_home.clone(),
            execution_dir: None,
            codex_executable: Some(PathBuf::from("custom-codex.exe")),
        })
        .unwrap();
        let plan_file = plan_report.plan_file.as_ref().unwrap();
        replace_invocation_thread_id(plan_file, Some(thread_id));
        let plan: CodexRuntimePlanFile =
            serde_json::from_slice(&fs::read(plan_file).unwrap()).unwrap();

        let preflight =
            preflight_codex_context(&harness_home, &plan, plan_file, plan_file.parent()).unwrap();

        assert!(preflight.receipt.compact_before_turn);
        assert!(preflight.receipt.reason.contains("inline image"));
        assert!(preflight.receipt.thread_health.compact_recommended);
        assert_eq!(
            preflight.receipt.thread_health.thread_id.as_deref(),
            Some(thread_id)
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn retryable_protocol_error_after_bloated_thread_rolls_over_to_fresh_thread() {
        let root =
            temp_root("retryable_protocol_error_after_bloated_thread_rolls_over_to_fresh_thread");
        let source = write_codex_runtime_source(&root);
        let harness_home = root.join(".agent-harness");
        let thread_id = "019ef798-346b-7ae2-b341-60a7a20a8d96";
        let rollout_dir = harness_home
            .join("codex-home")
            .join("sessions")
            .join("2026")
            .join("06")
            .join("24");
        fs::create_dir_all(&rollout_dir).unwrap();
        let image_payload = "C".repeat(
            usize::try_from(CODEX_THREAD_INLINE_IMAGE_LAST_TURN_BYTES_LIMIT).unwrap() + 128,
        );
        fs::write(
            rollout_dir.join(format!("rollout-2026-06-24T11-06-52-{thread_id}.jsonl")),
            format!(
                "{}\n",
                json!({
                    "type": "function_call_output",
                    "output": format!("data:image/png;base64,{image_payload}")
                })
            ),
        )
        .unwrap();
        enqueue_and_prepare(&source, &harness_home);
        let plan_report = plan_codex_runtime(CodexRuntimePlanOptions {
            harness_home: harness_home.clone(),
            execution_dir: None,
            codex_executable: Some(std::env::current_exe().unwrap()),
        })
        .unwrap();
        let plan_file = plan_report.plan_file.as_ref().unwrap();
        replace_env_requirements(plan_file, serde_json::json!([]));
        replace_invocation_thread_id(plan_file, Some(thread_id));
        let (executable, arguments, events_file) =
            stream_disconnect_then_fresh_thread_app_server_command(&root);
        replace_invocation(plan_file, executable, arguments);

        let report = run_codex_runtime(CodexRuntimeRunOptions {
            harness_home: harness_home.clone(),
            execution_dir: None,
            plan_file: None,
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            progress_context: None,
        })
        .unwrap();

        assert_eq!(report.receipt.status, CodexRuntimeRunStatus::Completed);
        assert_eq!(
            report.receipt.context_recovery.as_ref().unwrap().status,
            "thread-health-rollover-succeeded"
        );
        assert!(
            report
                .receipt
                .context_recovery
                .as_ref()
                .unwrap()
                .fresh_thread_attempted
        );
        let completion = report.completion.as_ref().unwrap();
        let transcript = fs::read_to_string(completion.transcript_file.as_ref().unwrap()).unwrap();
        assert!(transcript.contains("Fresh fallback reply"));
        let events = fs::read_to_string(events_file).unwrap();
        assert!(events.contains("thread/resume"));
        assert!(events.contains("thread/compact/start"));
        assert!(events.contains("thread/start"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn codex_progress_skips_terminal_delta_event_wrappers() {
        let context = progress_context();
        let event = codex_progress_event_from_json(
            &context,
            &json!({
                "method": "commandExecution/delta",
                "params": {
                    "delta": "\r\n",
                    "itemId": "call-1",
                    "threadId": "thread-1",
                    "turnId": "turn-1"
                }
            }),
            1234,
        );

        assert!(event.is_none());
    }

    #[test]
    fn codex_progress_extracts_compact_terminal_command_preview() {
        let context = progress_context();
        let event = codex_progress_event_from_json(
            &context,
            &json!({
                "method": "exec_command/start",
                "params": {
                    "command": "pwsh.exe -Command Get-Item -LiteralPath README.md",
                    "itemId": "call-1",
                    "threadId": "thread-1",
                    "turnId": "turn-1"
                }
            }),
            1234,
        )
        .unwrap();

        assert_eq!(event.kind, AgentProgressKind::Terminal);
        assert_eq!(event.preview, "pwsh: Get-Item -LiteralPath README.md");
        assert!(!event.preview.contains("threadId"));
    }

    #[test]
    fn codex_progress_compacts_powershell_wrapped_harness_command() {
        let context = progress_context();
        let event = codex_progress_event_from_json(
            &context,
            &json!({
                "method": "item/started",
                "params": {
                    "command": "'C:\\Program Files\\WindowsApps\\Microsoft.PowerShell_7.6.2.0_x64__8wekyb3d8bbwe\\pwsh.exe' -Command '.\\target\\debug\\agent-harness.exe status --harness-home .\\.agent-harness --json'"
                }
            }),
            1234,
        )
        .unwrap();

        assert_eq!(event.kind, AgentProgressKind::ToolCall);
        assert_eq!(event.preview, "pwsh: agent-harness status");
    }

    #[test]
    fn codex_progress_skips_item_message_wrappers_without_tool_preview() {
        let context = progress_context();
        let event = codex_progress_event_from_json(
            &context,
            &json!({
                "method": "item/completed",
                "params": {
                    "item": {
                        "id": "msg-1",
                        "phase": "commentary",
                        "text": "working update"
                    }
                }
            }),
            1234,
        );

        assert!(event.is_none());
    }

    #[test]
    fn plan_codex_runtime_writes_plan_and_receipts() {
        let root = temp_root("plan_codex_runtime_writes_plan_and_receipts");
        let source = write_codex_runtime_source(&root);
        let harness_home = root.join(".agent-harness");
        enqueue_and_prepare(&source, &harness_home);

        let report = plan_codex_runtime(CodexRuntimePlanOptions {
            harness_home: harness_home.clone(),
            execution_dir: None,
            codex_executable: Some(PathBuf::from("custom-codex.exe")),
        })
        .unwrap();

        assert_eq!(report.receipt.status, CodexRuntimeReceiptStatus::Planned);
        assert!(report.receipts_file.is_file());
        let plan = report.plan.unwrap();
        assert_eq!(plan.agent_id.as_deref(), Some("main"));
        assert_eq!(plan.provider.as_deref(), Some("openai"));
        assert_eq!(plan.model.as_deref(), Some("gpt-5"));
        assert_eq!(plan.invocation.arguments, vec!["app-server"]);
        assert_eq!(plan.invocation.working_directory, source.workspace);
        assert_eq!(plan.invocation.thread_id, None);
        assert_eq!(
            plan.invocation.executable,
            PathBuf::from("custom-codex.exe")
        );
        assert_eq!(
            plan.invocation.transport,
            CodexTransportPlan::StdioJsonRpcAppServer
        );
        assert_eq!(plan.invocation.approval_policy, CodexApprovalPolicy::Deny);
        assert_eq!(plan.invocation.app_server_approval_policy, "on-request");
        assert_eq!(plan.invocation.app_server_sandbox, "workspaceWrite");
        assert!(
            plan.invocation
                .env_requirements
                .iter()
                .any(|requirement| requirement.name == "OPENAI_API_KEY")
        );
        assert!(plan.prompt_bundle_json.is_file());
        assert!(plan.prompt_markdown.is_file());
        assert!(plan.outputs.runtime_receipt_file.is_file());
        assert!(report.plan_file.unwrap().is_file());
        assert!(
            plan.outputs
                .codex_binding_file
                .to_string_lossy()
                .ends_with(".jsonl.codex-app-server.json")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn codex_media_plan_records_vision_fallback_from_prepared_receipt() {
        let root = temp_root("codex_media_plan_records_vision_fallback_from_prepared_receipt");
        let source = write_codex_runtime_source(&root);
        let harness_home = root.join(".agent-harness");
        enqueue_and_prepare(&source, &harness_home);
        let execution_dir = latest_prepared_execution_dir(&harness_home);
        let attachment = crate::inbound_media_attachment_root(&harness_home)
            .join("update-1")
            .join("0.png");
        fs::create_dir_all(attachment.parent().unwrap()).unwrap();
        fs::write(&attachment, b"\x89PNG\r\n\x1a\n0000000000000000").unwrap();
        let receipt_file = execution_dir.join("execution-receipt.json");
        let mut receipt: Value = serde_json::from_slice(&fs::read(&receipt_file).unwrap()).unwrap();
        receipt["inboundMediaArtifacts"] = json!([
            {
                "platform": "telegram",
                "kind": "photo",
                "localPath": attachment,
                "artifactUri": "agent-harness://inbound-media/telegram/update-1/0.png",
                "mime": "image/png",
                "downloadStatus": "downloaded",
                "modelAttachmentStatus": "prompt-only",
                "source": "telegram.getFile"
            }
        ]);
        fs::write(
            &receipt_file,
            serde_json::to_string_pretty(&receipt).unwrap(),
        )
        .unwrap();

        let report = plan_codex_runtime(CodexRuntimePlanOptions {
            harness_home: harness_home.clone(),
            execution_dir: Some(execution_dir),
            codex_executable: Some(PathBuf::from("custom-codex.exe")),
        })
        .unwrap();

        let plan = report.plan.unwrap();
        assert_eq!(plan.media_plan.artifacts.len(), 1);
        assert!(plan.media_plan.native_input_parts.is_empty());
        assert_eq!(
            plan.media_plan.artifacts[0].model_attachment_status,
            InboundMediaModelAttachmentStatus::VisionToolAvailable
        );
        let plan_file: Value =
            serde_json::from_slice(&fs::read(report.plan_file.unwrap()).unwrap()).unwrap();
        assert_eq!(
            plan_file["mediaPlan"]["artifacts"][0]["modelAttachmentStatus"],
            "vision-tool-available"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn plan_codex_runtime_writes_openrouter_provider_config() {
        let root = temp_root("plan_codex_runtime_writes_openrouter_provider_config");
        let source = write_openrouter_codex_runtime_source(&root);
        let harness_home = root.join(".agent-harness");
        enqueue_and_prepare(&source, &harness_home);

        let report = plan_codex_runtime(CodexRuntimePlanOptions {
            harness_home: harness_home.clone(),
            execution_dir: None,
            codex_executable: Some(PathBuf::from("custom-codex.exe")),
        })
        .unwrap();

        let plan = report.plan.unwrap();
        assert_eq!(plan.provider.as_deref(), Some("openrouter"));
        assert_eq!(plan.model.as_deref(), Some("anthropic/claude-sonnet-4"));
        assert!(
            plan.invocation
                .env_requirements
                .iter()
                .any(|requirement| requirement.name == "OPENROUTER_API_KEY")
        );
        assert!(
            !plan
                .invocation
                .env_requirements
                .iter()
                .any(|requirement| requirement.name == "OPENAI_API_KEY")
        );
        let provider_config = plan.invocation.provider_config.as_ref().unwrap();
        assert_eq!(provider_config.provider, "openrouter");
        assert_eq!(provider_config.env_key, "OPENROUTER_API_KEY");
        assert_eq!(provider_config.base_url, "https://openrouter.ai/api/v1");
        assert_eq!(provider_config.wire_api, "responses");

        let codex_home = plan.invocation.codex_home.as_ref().unwrap();
        assert!(codex_home.ends_with(PathBuf::from("codex-home-providers").join("openrouter")));
        let config = fs::read_to_string(codex_home.join("config.toml")).unwrap();
        assert!(config.contains("model_provider = \"openrouter\""));
        assert!(config.contains("[model_providers.openrouter]"));
        assert!(config.contains("base_url = \"https://openrouter.ai/api/v1\""));
        assert!(config.contains("env_key = \"OPENROUTER_API_KEY\""));
        assert!(!config.contains("sk-"));
        assert!(!harness_home.join("codex-home").join("config.toml").exists());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn plan_codex_runtime_isolates_openrouter_from_shared_codex_home() {
        let root = temp_root("plan_codex_runtime_isolates_openrouter_from_shared_codex_home");
        let harness_home = root.join(".agent-harness");
        let openrouter_source = write_openrouter_codex_runtime_source(&root.join("openrouter"));
        let shared_codex_home = harness_home.join("codex-home");
        fs::create_dir_all(&shared_codex_home).unwrap();
        fs::write(shared_codex_home.join("auth.json"), "{}").unwrap();
        fs::write(
            shared_codex_home.join("config.toml"),
            "# Generated by openclaw-harness. Contains no secrets.\n\
             # Codex OAuth state stays in auth.json/auth.toml.\n\
             \n\
             model_provider = \"openrouter\"\n\
             [windows]\n\
             sandbox = \"elevated\"\n\
             \n\
             [model_providers.openrouter]\n\
             name = \"OpenRouter\"\n\
             base_url = \"https://openrouter.ai/api/v1\"\n\
             env_key = \"OPENROUTER_API_KEY\"\n\
             wire_api = \"responses\"\n",
        )
        .unwrap();
        enqueue_and_prepare(&openrouter_source, &harness_home);
        let openrouter_execution_dir = latest_prepared_execution_dir(&harness_home);

        let openrouter_plan = plan_codex_runtime(CodexRuntimePlanOptions {
            harness_home: harness_home.clone(),
            execution_dir: Some(openrouter_execution_dir),
            codex_executable: Some(PathBuf::from("custom-codex.exe")),
        })
        .unwrap()
        .plan
        .unwrap();

        assert!(
            openrouter_plan
                .invocation
                .codex_home
                .as_ref()
                .unwrap()
                .ends_with(PathBuf::from("codex-home-providers").join("openrouter"))
        );
        let shared_after_openrouter =
            fs::read_to_string(shared_codex_home.join("config.toml")).unwrap();
        assert!(shared_after_openrouter.contains("model_provider = \"openrouter\""));
        crate::append_jsonl_value(
            &harness_home
                .join("state")
                .join("runtime-queue")
                .join("run-once-receipts.jsonl"),
            &serde_json::json!({
                "queueId": openrouter_plan.queue_id.as_deref().unwrap(),
                "status": "completed",
                "reason": "test terminal receipt"
            }),
        )
        .unwrap();
        release_runtime_queue_lease(&harness_home, openrouter_plan.queue_id.as_deref().unwrap())
            .unwrap();

        let main_source = write_codex_runtime_source(&root.join("main"));
        enqueue_and_prepare_at(&main_source, &harness_home, 1235);
        let main_execution_dir = latest_prepared_execution_dir(&harness_home);
        let main_plan = plan_codex_runtime(CodexRuntimePlanOptions {
            harness_home: harness_home.clone(),
            execution_dir: Some(main_execution_dir),
            codex_executable: Some(PathBuf::from("custom-codex.exe")),
        })
        .unwrap()
        .plan
        .unwrap();

        assert_eq!(main_plan.provider.as_deref(), Some("openai"));
        assert_eq!(
            main_plan.invocation.codex_home.as_deref(),
            Some(shared_codex_home.as_path())
        );
        let shared_after_main = fs::read_to_string(shared_codex_home.join("config.toml")).unwrap();
        assert!(shared_after_main.starts_with("# Generated by agent-harness."));
        assert!(!shared_after_main.contains("model_provider = \"openrouter\""));
        assert!(!shared_after_main.contains("[model_providers.openrouter]"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn ensure_provider_config_updates_existing_openrouter_table() {
        let provider_config = codex_provider_config(Some("openrouter")).unwrap();
        let stale = "# Generated by agent-harness. Contains no secrets.\n\
             model_provider = \"openrouter\"\n\
             [model_providers.openrouter]\n\
             name = \"Old Router\"\n\
             base_url = \"https://old.example/v1\"\n\
             env_key = \"OLD_OPENROUTER_KEY\"\n\
             wire_api = \"chat\"\n";

        let updated = ensure_provider_config_in_toml(stale, &provider_config);

        assert!(updated.contains("name = \"OpenRouter\""));
        assert!(updated.contains("base_url = \"https://openrouter.ai/api/v1\""));
        assert!(updated.contains("env_key = \"OPENROUTER_API_KEY\""));
        assert!(updated.contains("wire_api = \"responses\""));
        assert!(!updated.contains("Old Router"));
        assert!(!updated.contains("https://old.example/v1"));
        assert!(!updated.contains("OLD_OPENROUTER_KEY"));
        assert!(!updated.contains("wire_api = \"chat\""));
    }

    fn progress_context() -> AgentProgressContext {
        AgentProgressContext {
            queue_id: "queue-1".to_string(),
            agent_id: Some("main".to_string()),
            account_id: Some("default".to_string()),
            thread_id: None,
            session_key: "telegram:dm:user:main".to_string(),
            platform: "telegram".to_string(),
            channel_id: "dm".to_string(),
            user_id: "user".to_string(),
        }
    }

    #[test]
    fn plan_codex_runtime_reads_approval_policy_from_harness_config() {
        let root = temp_root("plan_codex_runtime_reads_approval_policy_from_harness_config");
        let source = write_codex_runtime_source(&root);
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join(HARNESS_CONFIG_FILE_NAME),
            r#"{"security":{"codexApprovalPolicy":"accept","codexSandboxPolicy":"dangerFullAccess"}}"#,
        )
        .unwrap();
        enqueue_and_prepare(&source, &harness_home);

        let report = plan_codex_runtime(CodexRuntimePlanOptions {
            harness_home: harness_home.clone(),
            execution_dir: None,
            codex_executable: Some(PathBuf::from("custom-codex.exe")),
        })
        .unwrap();

        let plan = report.plan.unwrap();
        assert_eq!(plan.invocation.approval_policy, CodexApprovalPolicy::Accept);
        assert_eq!(plan.invocation.app_server_approval_policy, "on-request");
        assert_eq!(plan.invocation.app_server_sandbox, "dangerFullAccess");

        let plan_file = report.plan_file.unwrap();
        let plan_json: Value = read_json_file(&plan_file).unwrap();
        assert_eq!(
            plan_json["invocation"]["approvalPolicy"],
            serde_json::json!("accept")
        );
        assert_eq!(
            plan_json["invocation"]["appServerApprovalPolicy"],
            serde_json::json!("on-request")
        );
        assert_eq!(
            plan_json["invocation"]["appServerSandbox"],
            serde_json::json!("dangerFullAccess")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn plan_codex_runtime_reports_no_prepared_execution() {
        let root = temp_root("plan_codex_runtime_reports_no_prepared_execution");
        let harness_home = root.join(".agent-harness");

        let report = plan_codex_runtime(CodexRuntimePlanOptions {
            harness_home,
            execution_dir: None,
            codex_executable: None,
        })
        .unwrap();

        assert!(report.plan.is_none());
        assert_eq!(
            report.receipt.status,
            CodexRuntimeReceiptStatus::NoPreparedExecution
        );
        assert!(report.receipts_file.is_file());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn plan_codex_runtime_uses_harness_codex_home_when_auth_is_present() {
        let root = temp_root("plan_codex_runtime_uses_harness_codex_home_when_auth_is_present");
        let source = write_codex_runtime_source(&root);
        let harness_home = root.join(".agent-harness");
        let codex_home = harness_home.join("codex-home");
        fs::create_dir_all(&codex_home).unwrap();
        fs::write(codex_home.join("auth.json"), "{}").unwrap();
        enqueue_and_prepare(&source, &harness_home);

        let report = plan_codex_runtime(CodexRuntimePlanOptions {
            harness_home: harness_home.clone(),
            execution_dir: None,
            codex_executable: Some(PathBuf::from("custom-codex.exe")),
        })
        .unwrap();

        let plan = report.plan.unwrap();
        assert_eq!(plan.invocation.codex_home, Some(codex_home));
        let config = fs::read_to_string(
            plan.invocation
                .codex_home
                .as_ref()
                .unwrap()
                .join("config.toml"),
        )
        .unwrap();
        assert!(config.contains("sandbox = \"elevated\""));
        assert!(config.contains("multi_agent = true"));
        assert!(config.contains("trust_level = \"trusted\""));
        assert!(config.contains(&toml_basic_string(&source.workspace.to_string_lossy())));
        assert!(config.contains(&toml_basic_string(&harness_home.to_string_lossy())));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn plan_codex_runtime_reads_sandbox_from_harness_config() {
        let root = temp_root("plan_codex_runtime_reads_sandbox_from_harness_config");
        let source = write_codex_runtime_source(&root);
        let harness_home = root.join(".agent-harness");
        let codex_home = harness_home.join("codex-home");
        fs::create_dir_all(&codex_home).unwrap();
        fs::write(codex_home.join("auth.json"), "{}").unwrap();
        fs::write(
            harness_home.join(HARNESS_CONFIG_FILE_NAME),
            r#"{"security":{"codexSandbox":"read-only"}}"#,
        )
        .unwrap();
        enqueue_and_prepare(&source, &harness_home);

        let report = plan_codex_runtime(CodexRuntimePlanOptions {
            harness_home: harness_home.clone(),
            execution_dir: None,
            codex_executable: Some(PathBuf::from("custom-codex.exe")),
        })
        .unwrap();

        let plan = report.plan.unwrap();
        let config = fs::read_to_string(
            plan.invocation
                .codex_home
                .as_ref()
                .unwrap()
                .join("config.toml"),
        )
        .unwrap();
        assert!(config.contains("sandbox = \"read-only\""));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn plan_codex_runtime_preserves_existing_harness_codex_config() {
        let root = temp_root("plan_codex_runtime_preserves_existing_harness_codex_config");
        let source = write_codex_runtime_source(&root);
        let harness_home = root.join(".agent-harness");
        let codex_home = harness_home.join("codex-home");
        fs::create_dir_all(&codex_home).unwrap();
        fs::write(codex_home.join("auth.json"), "{}").unwrap();
        fs::write(codex_home.join("config.toml"), "# custom operator config\n").unwrap();
        enqueue_and_prepare(&source, &harness_home);

        let report = plan_codex_runtime(CodexRuntimePlanOptions {
            harness_home: harness_home.clone(),
            execution_dir: None,
            codex_executable: Some(PathBuf::from("custom-codex.exe")),
        })
        .unwrap();

        assert_eq!(
            fs::read_to_string(codex_home.join("config.toml")).unwrap(),
            "# custom operator config\n"
        );
        assert!(report.warnings.is_empty());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn harness_codex_config_uses_absolute_project_paths() {
        let cwd = env::current_dir().unwrap();
        let config = harness_codex_config_toml(
            Path::new("relative-runtime"),
            Path::new("relative-harness"),
            DEFAULT_CODEX_SANDBOX,
            None,
            &CodexContextPolicy::default(),
        );

        assert!(config.contains(&toml_basic_string(
            &cwd.join("relative-runtime").to_string_lossy()
        )));
        assert!(config.contains(&toml_basic_string(
            &cwd.join("relative-harness").to_string_lossy()
        )));
        assert!(!config.contains("[projects.\"relative-runtime\"]"));
        assert!(!config.contains("[projects.\"relative-harness\"]"));
    }

    #[test]
    fn harness_codex_home_uses_absolute_path_for_relative_harness_home() {
        let root = PathBuf::from("target").join(format!(
            "tmp-agent-harness-codex-home-{}",
            current_log_time_ms().unwrap()
        ));
        let harness_home = root.join(".agent-harness");
        let codex_home = harness_home.join("codex-home");
        fs::create_dir_all(&codex_home).unwrap();
        fs::write(codex_home.join("auth.json"), "{}").unwrap();

        let resolved = harness_codex_home(&harness_home, None).unwrap();

        assert!(resolved.is_absolute());
        assert!(resolved.ends_with(harness_home.join("codex-home")));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn plan_codex_runtime_uses_runtime_workspace_when_provided() {
        let root = temp_root("plan_codex_runtime_uses_runtime_workspace_when_provided");
        let source = write_codex_runtime_source(&root);
        let runtime_workspace = root.join("mounted-workspace");
        fs::create_dir_all(&runtime_workspace).unwrap();
        let harness_home = root.join(".agent-harness");
        enqueue_and_prepare_with_runtime_workspace(
            &source,
            &harness_home,
            Some(runtime_workspace.clone()),
        );

        let report = plan_codex_runtime(CodexRuntimePlanOptions {
            harness_home: harness_home.clone(),
            execution_dir: None,
            codex_executable: Some(PathBuf::from("custom-codex.exe")),
        })
        .unwrap();

        let plan = report.plan.unwrap();
        assert_eq!(plan.invocation.working_directory, runtime_workspace);
        let bundle: Value =
            serde_json::from_slice(&fs::read(plan.prompt_bundle_json).unwrap()).unwrap();
        assert_eq!(
            bundle["sourceWorkspace"],
            source.workspace.to_string_lossy().to_string()
        );
        assert_eq!(bundle["summary"]["promptFilesIncluded"], 1);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn preflight_codex_runtime_reports_ready_when_local_gates_pass() {
        let root = temp_root("preflight_codex_runtime_reports_ready_when_local_gates_pass");
        let source = write_codex_runtime_source(&root);
        let harness_home = root.join(".agent-harness");
        enqueue_and_prepare(&source, &harness_home);
        let plan_report = plan_codex_runtime(CodexRuntimePlanOptions {
            harness_home: harness_home.clone(),
            execution_dir: None,
            codex_executable: Some(std::env::current_exe().unwrap()),
        })
        .unwrap();
        let plan_file = plan_report.plan_file.as_ref().unwrap();
        replace_env_requirements(plan_file, serde_json::json!([]));

        let report = preflight_codex_runtime(CodexRuntimePreflightOptions {
            harness_home,
            execution_dir: None,
            plan_file: None,
        })
        .unwrap();

        assert_eq!(report.receipt.status, CodexRuntimePreflightStatus::Ready);
        assert!(report.receipts_file.is_file());
        assert!(report.preflight_file.unwrap().is_file());
        assert!(
            report
                .checks
                .iter()
                .all(|check| check.status == CodexRuntimePreflightCheckStatus::Pass)
        );
        assert!(
            report
                .checks
                .iter()
                .any(|check| check.name == "codex-executable")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn preflight_codex_runtime_blocks_missing_environment() {
        let root = temp_root("preflight_codex_runtime_blocks_missing_environment");
        let source = write_codex_runtime_source(&root);
        let harness_home = root.join(".agent-harness");
        enqueue_and_prepare(&source, &harness_home);
        let plan_report = plan_codex_runtime(CodexRuntimePlanOptions {
            harness_home: harness_home.clone(),
            execution_dir: None,
            codex_executable: Some(std::env::current_exe().unwrap()),
        })
        .unwrap();
        let missing_env = format!("AGENT_HARNESS_TEST_MISSING_ENV_{}", std::process::id());
        let plan_file = plan_report.plan_file.as_ref().unwrap();
        replace_env_requirements(
            plan_file,
            serde_json::json!([
                {
                    "name": missing_env,
                    "reason": "test missing env"
                }
            ]),
        );

        let report = preflight_codex_runtime(CodexRuntimePreflightOptions {
            harness_home,
            execution_dir: None,
            plan_file: None,
        })
        .unwrap();

        assert_eq!(report.receipt.status, CodexRuntimePreflightStatus::Blocked);
        assert!(report.checks.iter().any(|check| {
            check.name.starts_with("env:") && check.status == CodexRuntimePreflightCheckStatus::Fail
        }));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn preflight_codex_runtime_reports_no_runtime_plan() {
        let root = temp_root("preflight_codex_runtime_reports_no_runtime_plan");
        let harness_home = root.join(".agent-harness");

        let report = preflight_codex_runtime(CodexRuntimePreflightOptions {
            harness_home,
            execution_dir: None,
            plan_file: None,
        })
        .unwrap();

        assert_eq!(
            report.receipt.status,
            CodexRuntimePreflightStatus::NoRuntimePlan
        );
        assert!(report.receipts_file.is_file());
        assert!(report.preflight_file.is_none());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn openai_codex_credential_gate_accepts_oauth_auth_file() {
        let root = temp_root("openai_codex_credential_gate_accepts_oauth_auth_file");
        let auth_file = root.join("auth.json");
        fs::create_dir_all(&root).unwrap();
        fs::write(&auth_file, "{}").unwrap();
        let requirement = CodexEnvRequirement {
            name: "OPENAI_API_KEY".to_string(),
            reason: "test credential".to_string(),
        };

        let checks =
            check_openai_or_codex_oauth_requirement_with_candidates(&requirement, &[auth_file]);

        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].status, CodexRuntimePreflightCheckStatus::Pass);
        assert_eq!(checks[0].name, "credential:openai-or-codex-oauth");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn probe_codex_runtime_launch_starts_and_stops_process() {
        let root = temp_root("probe_codex_runtime_launch_starts_and_stops_process");
        let source = write_codex_runtime_source(&root);
        let harness_home = root.join(".agent-harness");
        enqueue_and_prepare(&source, &harness_home);
        let plan_report = plan_codex_runtime(CodexRuntimePlanOptions {
            harness_home: harness_home.clone(),
            execution_dir: None,
            codex_executable: Some(std::env::current_exe().unwrap()),
        })
        .unwrap();
        let plan_file = plan_report.plan_file.as_ref().unwrap();
        replace_env_requirements(plan_file, serde_json::json!([]));
        let (executable, arguments) = long_running_probe_command();
        replace_invocation(plan_file, executable, arguments);

        let report = probe_codex_runtime_launch(CodexRuntimeLaunchProbeOptions {
            harness_home,
            execution_dir: None,
            plan_file: None,
            startup_probe_ms: 150,
        })
        .unwrap();

        assert_eq!(
            report.receipt.status,
            CodexRuntimeLaunchProbeStatus::StartedAndStopped
        );
        assert!(report.receipts_file.is_file());
        assert!(report.launch_file.unwrap().is_file());
        let process = report.process.unwrap();
        assert!(process.pid.is_some());
        assert!(process.terminated);
        assert!(process.stdout_log.unwrap().is_file());
        assert!(process.stderr_log.unwrap().is_file());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn probe_codex_runtime_launch_respects_preflight_block() {
        let root = temp_root("probe_codex_runtime_launch_respects_preflight_block");
        let source = write_codex_runtime_source(&root);
        let harness_home = root.join(".agent-harness");
        enqueue_and_prepare(&source, &harness_home);
        let plan_report = plan_codex_runtime(CodexRuntimePlanOptions {
            harness_home: harness_home.clone(),
            execution_dir: None,
            codex_executable: Some(std::env::current_exe().unwrap()),
        })
        .unwrap();
        let plan_file = plan_report.plan_file.as_ref().unwrap();
        replace_env_requirements(
            plan_file,
            serde_json::json!([
                {
                    "name": format!("AGENT_HARNESS_TEST_MISSING_ENV_{}", std::process::id()),
                    "reason": "test missing env"
                }
            ]),
        );

        let report = probe_codex_runtime_launch(CodexRuntimeLaunchProbeOptions {
            harness_home,
            execution_dir: None,
            plan_file: None,
            startup_probe_ms: 50,
        })
        .unwrap();

        assert_eq!(
            report.receipt.status,
            CodexRuntimeLaunchProbeStatus::PreflightBlocked
        );
        assert!(report.process.is_none());
        assert!(report.launch_file.unwrap().is_file());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn probe_codex_runtime_launch_reports_no_runtime_plan() {
        let root = temp_root("probe_codex_runtime_launch_reports_no_runtime_plan");
        let harness_home = root.join(".agent-harness");

        let report = probe_codex_runtime_launch(CodexRuntimeLaunchProbeOptions {
            harness_home,
            execution_dir: None,
            plan_file: None,
            startup_probe_ms: 50,
        })
        .unwrap();

        assert_eq!(
            report.receipt.status,
            CodexRuntimeLaunchProbeStatus::NoRuntimePlan
        );
        assert!(report.process.is_none());
        assert!(report.receipts_file.is_file());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn run_codex_runtime_drives_fake_app_server_and_records_outputs() {
        let root = temp_root("run_codex_runtime_drives_fake_app_server_and_records_outputs");
        let source = write_codex_runtime_source(&root);
        let harness_home = root.join(".agent-harness");
        enqueue_and_prepare(&source, &harness_home);
        let plan_report = plan_codex_runtime(CodexRuntimePlanOptions {
            harness_home: harness_home.clone(),
            execution_dir: None,
            codex_executable: Some(std::env::current_exe().unwrap()),
        })
        .unwrap();
        let plan_file = plan_report.plan_file.as_ref().unwrap();
        replace_env_requirements(plan_file, serde_json::json!([]));
        let (executable, arguments) = fake_app_server_command(&root);
        replace_invocation(plan_file, executable, arguments);

        let report = run_codex_runtime(CodexRuntimeRunOptions {
            harness_home: harness_home.clone(),
            execution_dir: None,
            plan_file: None,
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            progress_context: None,
        })
        .unwrap();

        assert_eq!(report.receipt.status, CodexRuntimeRunStatus::Completed);
        assert_eq!(report.receipt.event_count, 4);
        assert_eq!(
            report
                .receipt
                .usage
                .as_ref()
                .and_then(|usage| usage.total_tokens),
            Some(42)
        );
        assert!(report.run_file.unwrap().is_file());
        assert!(report.stdout_log.as_ref().unwrap().is_file());
        assert!(report.stderr_log.as_ref().unwrap().is_file());
        let stdout = fs::read_to_string(report.stdout_log.unwrap()).unwrap();
        assert!(stdout.contains("item/agentMessage/delta"));
        let completion = report.completion.unwrap();
        assert_eq!(
            completion.receipt.status,
            CodexRuntimeCompletionStatus::Recorded
        );
        let transcript_file = completion.transcript_file.clone().unwrap();
        let transcript = fs::read_to_string(&transcript_file).unwrap();
        assert!(transcript.contains("Fake assistant reply."));
        assert!(completion.trajectory_file.unwrap().is_file());
        let binding_file = completion.codex_binding_file.unwrap();
        assert!(binding_file.is_file());
        let binding: Value = serde_json::from_slice(&fs::read(binding_file).unwrap()).unwrap();
        assert_eq!(binding["threadId"], "thread-test");
        let harness_log = harness_home
            .join("state")
            .join("logs")
            .join("harness.jsonl");
        assert!(
            fs::read_to_string(harness_log)
                .unwrap()
                .contains("codex.run.completed")
        );

        let second = run_codex_runtime(CodexRuntimeRunOptions {
            harness_home,
            execution_dir: None,
            plan_file: None,
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            progress_context: None,
        })
        .unwrap();
        assert_eq!(second.receipt.status, CodexRuntimeRunStatus::Completed);
        assert_eq!(second.receipt.event_count, 0);
        assert!(second.receipt.reason.contains("already recorded"));
        assert_eq!(
            fs::read_to_string(transcript_file).unwrap().lines().count(),
            2
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn run_codex_runtime_recovers_completed_stdout_without_relaunch() {
        let root = temp_root("run_codex_runtime_recovers_completed_stdout_without_relaunch");
        let source = write_codex_runtime_source(&root);
        let harness_home = root.join(".agent-harness");
        enqueue_and_prepare(&source, &harness_home);
        let plan_report = plan_codex_runtime(CodexRuntimePlanOptions {
            harness_home: harness_home.clone(),
            execution_dir: None,
            codex_executable: Some(std::env::current_exe().unwrap()),
        })
        .unwrap();
        let plan_file = plan_report.plan_file.as_ref().unwrap();
        replace_env_requirements(plan_file, serde_json::json!([]));
        let execution_dir = plan_report.execution_dir.as_ref().unwrap();
        fs::write(
            execution_dir.join("codex-runtime-run.stdout.jsonl"),
            r#"{"id":1,"result":{"thread":{"id":"thread-recovered"}}}
{"method":"item/completed","params":{"item":{"type":"agentMessage","id":"msg-recovered","text":"Recovered final reply.","phase":"final_answer"},"threadId":"thread-recovered","completedAtMs":1234}}
{"method":"turn/completed","params":{"threadId":"thread-recovered","turn":{"id":"turn-recovered","status":"completed","usage":{"inputTokens":10,"outputTokens":5,"totalTokens":15}}}}
"#,
        )
        .unwrap();

        let report = run_codex_runtime(CodexRuntimeRunOptions {
            harness_home,
            execution_dir: None,
            plan_file: None,
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            progress_context: None,
        })
        .unwrap();

        assert_eq!(report.receipt.status, CodexRuntimeRunStatus::Completed);
        assert!(report.receipt.reason.contains("recovered completed"));
        assert_eq!(report.receipt.event_count, 3);
        assert_eq!(
            report
                .receipt
                .usage
                .as_ref()
                .and_then(|usage| usage.total_tokens),
            Some(15)
        );
        let transcript_file = report.completion.unwrap().transcript_file.unwrap();
        let transcript = fs::read_to_string(transcript_file).unwrap();
        assert!(transcript.contains("Recovered final reply."));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn run_codex_runtime_does_not_block_on_open_stdout_after_terminal_event() {
        let root =
            temp_root("run_codex_runtime_does_not_block_on_open_stdout_after_terminal_event");
        let source = write_codex_runtime_source(&root);
        let harness_home = root.join(".agent-harness");
        enqueue_and_prepare(&source, &harness_home);
        let plan_report = plan_codex_runtime(CodexRuntimePlanOptions {
            harness_home: harness_home.clone(),
            execution_dir: None,
            codex_executable: Some(std::env::current_exe().unwrap()),
        })
        .unwrap();
        let plan_file = plan_report.plan_file.as_ref().unwrap();
        replace_env_requirements(plan_file, serde_json::json!([]));
        let (executable, arguments) = stdout_holding_app_server_command(&root);
        replace_invocation(plan_file, executable, arguments);

        let started = Instant::now();
        let report = run_codex_runtime(CodexRuntimeRunOptions {
            harness_home,
            execution_dir: None,
            plan_file: None,
            timeout_ms: 10_000,
            idle_timeout_ms: 10_000,
            progress_context: None,
        })
        .unwrap();

        assert_eq!(report.receipt.status, CodexRuntimeRunStatus::Completed);
        assert!(
            started.elapsed() < Duration::from_secs(8),
            "runtime should not wait for an inherited stdout handle to close"
        );
        let transcript_file = report.completion.unwrap().transcript_file.unwrap();
        let transcript = fs::read_to_string(transcript_file).unwrap();
        assert!(transcript.contains("stdout holder reply"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn run_codex_runtime_treats_app_server_error_as_protocol_failure() {
        let root = temp_root("run_codex_runtime_treats_app_server_error_as_protocol_failure");
        let source = write_codex_runtime_source(&root);
        let harness_home = root.join(".agent-harness");
        enqueue_and_prepare(&source, &harness_home);
        let plan_report = plan_codex_runtime(CodexRuntimePlanOptions {
            harness_home: harness_home.clone(),
            execution_dir: None,
            codex_executable: Some(std::env::current_exe().unwrap()),
        })
        .unwrap();
        let plan_file = plan_report.plan_file.as_ref().unwrap();
        replace_env_requirements(plan_file, serde_json::json!([]));
        let (executable, arguments) = failing_app_server_command(&root);
        replace_invocation(plan_file, executable, arguments);

        let report = run_codex_runtime(CodexRuntimeRunOptions {
            harness_home,
            execution_dir: None,
            plan_file: None,
            timeout_ms: 10_000,
            idle_timeout_ms: 10_000,
            progress_context: None,
        })
        .unwrap();

        assert_eq!(report.receipt.status, CodexRuntimeRunStatus::ProtocolError);
        assert!(report.receipt.reason.contains("OPENROUTER_API_KEY"));
        assert!(report.completion.is_none());
        let run_file = report.run_file.unwrap();
        let run_json = fs::read_to_string(run_file).unwrap();
        assert!(!run_json.contains("(no assistant text captured"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn run_codex_runtime_context_error_maps_to_context_exhausted_when_recovery_disabled() {
        let root = temp_root(
            "run_codex_runtime_context_error_maps_to_context_exhausted_when_recovery_disabled",
        );
        let source = write_codex_runtime_source(&root);
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join(HARNESS_CONFIG_FILE_NAME),
            r#"{"codexContext":{"retryOnceAfterCompact":false,"fallbackOnCompactFailure":"disabled"}}"#,
        )
        .unwrap();
        enqueue_and_prepare(&source, &harness_home);
        let plan_report = plan_codex_runtime(CodexRuntimePlanOptions {
            harness_home: harness_home.clone(),
            execution_dir: None,
            codex_executable: Some(std::env::current_exe().unwrap()),
        })
        .unwrap();
        let plan_file = plan_report.plan_file.as_ref().unwrap();
        replace_env_requirements(plan_file, serde_json::json!([]));
        let (executable, arguments, _events) =
            context_error_then_compact_success_app_server_command(&root);
        replace_invocation(plan_file, executable, arguments);

        let report = run_codex_runtime(CodexRuntimeRunOptions {
            harness_home,
            execution_dir: None,
            plan_file: None,
            timeout_ms: 10_000,
            idle_timeout_ms: 10_000,
            progress_context: None,
        })
        .unwrap();

        assert_eq!(
            report.receipt.status,
            CodexRuntimeRunStatus::ContextExhausted
        );
        assert!(report.receipt.reason.contains("ContextWindowExceeded"));
        assert_eq!(
            report
                .receipt
                .context_recovery
                .as_ref()
                .map(|receipt| receipt.status.as_str()),
            Some("manual-recovery-required")
        );
        assert!(report.completion.is_none());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn run_codex_runtime_preflight_compacts_existing_thread_before_turn() {
        let root = temp_root("run_codex_runtime_preflight_compacts_existing_thread_before_turn");
        let source = write_codex_runtime_source(&root);
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join(HARNESS_CONFIG_FILE_NAME),
            r#"{"codexContext":{"modelContextWindow":1000,"compactAtActiveContextRatio":0.5,"modelAutoCompactTokenLimit":900}}"#,
        )
        .unwrap();
        enqueue_and_prepare(&source, &harness_home);
        let plan_report = plan_codex_runtime(CodexRuntimePlanOptions {
            harness_home: harness_home.clone(),
            execution_dir: None,
            codex_executable: Some(std::env::current_exe().unwrap()),
        })
        .unwrap();
        let plan = plan_report.plan.as_ref().unwrap();
        let plan_file = plan_report.plan_file.as_ref().unwrap();
        replace_env_requirements(plan_file, serde_json::json!([]));
        replace_invocation_thread_id(plan_file, Some("thread-existing"));
        let receipts_file = harness_home
            .join("state")
            .join("runtime-queue")
            .join("codex-runtime-run-receipts.jsonl");
        fs::write(
            &receipts_file,
            format!(
                "{}\n",
                serde_json::json!({
                    "codexBindingFile": plan.outputs.codex_binding_file.to_string_lossy(),
                    "usage": {
                        "inputTokens": 920,
                        "outputTokens": 10,
                        "totalTokens": 930,
                        "source": "test"
                    }
                })
            ),
        )
        .unwrap();
        let (executable, arguments, events_file) = compact_tracking_app_server_command(&root);
        replace_invocation(plan_file, executable, arguments);

        let report = run_codex_runtime(CodexRuntimeRunOptions {
            harness_home: harness_home.clone(),
            execution_dir: None,
            plan_file: None,
            timeout_ms: 10_000,
            idle_timeout_ms: 10_000,
            progress_context: None,
        })
        .unwrap();

        assert_eq!(report.receipt.status, CodexRuntimeRunStatus::Completed);
        assert_eq!(
            report
                .receipt
                .context_recovery
                .as_ref()
                .map(|receipt| receipt.status.as_str()),
            Some("compact-before-turn")
        );
        let events = fs::read_to_string(events_file).unwrap();
        let compact_index = events.find("thread/compact/start").unwrap();
        let turn_index = events.find("turn/start").unwrap();
        assert!(compact_index < turn_index);
        let preflight = fs::read_to_string(
            plan_report
                .execution_dir
                .as_ref()
                .unwrap()
                .join("codex-context-preflight.json"),
        )
        .unwrap();
        assert!(preflight.contains(r#""compactBeforeTurn": true"#));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn run_codex_runtime_preflight_compact_failure_falls_back_to_fresh_thread() {
        assert_preflight_compact_problem_falls_back("failed", 10_000);
    }

    #[test]
    fn run_codex_runtime_preflight_compact_timeout_falls_back_to_fresh_thread() {
        assert_preflight_compact_problem_falls_back("timeout", 5_000);
    }

    #[test]
    fn run_codex_runtime_preflight_compact_unexpected_response_falls_back_to_fresh_thread() {
        assert_preflight_compact_problem_falls_back("unexpected", 10_000);
    }

    fn assert_preflight_compact_problem_falls_back(scenario: &str, idle_timeout_ms: u64) {
        let root = temp_root(&format!(
            "run_codex_runtime_preflight_compact_{scenario}_falls_back_to_fresh_thread"
        ));
        let source = write_codex_runtime_source(&root);
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join(HARNESS_CONFIG_FILE_NAME),
            r#"{"codexContext":{"modelContextWindow":1000,"compactAtActiveContextRatio":0.5,"modelAutoCompactTokenLimit":900,"fallbackOnCompactFailure":"checkpoint-and-new-thread"}}"#,
        )
        .unwrap();
        enqueue_and_prepare(&source, &harness_home);
        let plan_report = plan_codex_runtime(CodexRuntimePlanOptions {
            harness_home: harness_home.clone(),
            execution_dir: None,
            codex_executable: Some(std::env::current_exe().unwrap()),
        })
        .unwrap();
        let plan = plan_report.plan.as_ref().unwrap();
        let plan_file = plan_report.plan_file.as_ref().unwrap();
        replace_env_requirements(plan_file, serde_json::json!([]));
        replace_invocation_thread_id(plan_file, Some("thread-existing"));
        let receipts_file = harness_home
            .join("state")
            .join("runtime-queue")
            .join("codex-runtime-run-receipts.jsonl");
        fs::write(
            &receipts_file,
            format!(
                "{}\n",
                serde_json::json!({
                    "codexBindingFile": plan.outputs.codex_binding_file.to_string_lossy(),
                    "usage": {
                        "inputTokens": 920,
                        "outputTokens": 10,
                        "totalTokens": 930,
                        "source": "test"
                    }
                })
            ),
        )
        .unwrap();
        let (executable, arguments, events_file) =
            preflight_compact_problem_then_fresh_thread_app_server_command(&root, scenario);
        replace_invocation(plan_file, executable, arguments);

        let report = run_codex_runtime(CodexRuntimeRunOptions {
            harness_home: harness_home.clone(),
            execution_dir: None,
            plan_file: None,
            timeout_ms: 10_000,
            idle_timeout_ms,
            progress_context: None,
        })
        .unwrap();

        assert_eq!(report.receipt.status, CodexRuntimeRunStatus::Completed);
        let recovery = report.receipt.context_recovery.as_ref().unwrap();
        assert_eq!(
            recovery.status.as_str(),
            "compact-before-turn-fallback-succeeded"
        );
        assert_eq!(recovery.official_compact_attempts, 1);
        assert!(recovery.fresh_thread_attempted);
        assert!(recovery.fresh_thread_succeeded);
        assert!(recovery.checkpoint_file.as_ref().unwrap().is_file());
        assert!(recovery.rollover_file.as_ref().unwrap().is_file());
        assert!(recovery.reason.contains("compact-before-turn"));
        let events = fs::read_to_string(events_file).unwrap();
        assert!(events.contains("thread/compact/start"));
        assert!(events.matches("turn/start").count() >= 1);
        let transcript =
            fs::read_to_string(report.completion.unwrap().transcript_file.unwrap()).unwrap();
        assert!(transcript.contains("Fresh fallback reply."));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn run_codex_runtime_context_error_compacts_and_retries_once() {
        let root = temp_root("run_codex_runtime_context_error_compacts_and_retries_once");
        let source = write_codex_runtime_source(&root);
        let harness_home = root.join(".agent-harness");
        enqueue_and_prepare(&source, &harness_home);
        let plan_report = plan_codex_runtime(CodexRuntimePlanOptions {
            harness_home: harness_home.clone(),
            execution_dir: None,
            codex_executable: Some(std::env::current_exe().unwrap()),
        })
        .unwrap();
        let plan_file = plan_report.plan_file.as_ref().unwrap();
        replace_env_requirements(plan_file, serde_json::json!([]));
        let (executable, arguments, events_file) =
            context_error_then_compact_success_app_server_command(&root);
        replace_invocation(plan_file, executable, arguments);

        let report = run_codex_runtime(CodexRuntimeRunOptions {
            harness_home,
            execution_dir: None,
            plan_file: None,
            timeout_ms: 10_000,
            idle_timeout_ms: 10_000,
            progress_context: None,
        })
        .unwrap();

        assert_eq!(report.receipt.status, CodexRuntimeRunStatus::Completed);
        let recovery = report.receipt.context_recovery.as_ref().unwrap();
        assert_eq!(recovery.status, "compact-retry-succeeded");
        assert_eq!(recovery.official_compact_attempts, 1);
        assert!(recovery.retry_attempted);
        assert!(recovery.checkpoint_file.is_none());
        let events = fs::read_to_string(events_file).unwrap();
        assert_eq!(events.matches("turn/start").count(), 2);
        assert!(events.contains("thread/compact/start"));
        let transcript =
            fs::read_to_string(report.completion.unwrap().transcript_file.unwrap()).unwrap();
        assert!(transcript.contains("Recovered after compact."));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn run_codex_runtime_context_error_falls_back_to_checkpoint_fresh_thread() {
        let root =
            temp_root("run_codex_runtime_context_error_falls_back_to_checkpoint_fresh_thread");
        let source = write_codex_runtime_source(&root);
        let harness_home = root.join(".agent-harness");
        enqueue_and_prepare(&source, &harness_home);
        let plan_report = plan_codex_runtime(CodexRuntimePlanOptions {
            harness_home: harness_home.clone(),
            execution_dir: None,
            codex_executable: Some(std::env::current_exe().unwrap()),
        })
        .unwrap();
        let plan = plan_report.plan.as_ref().unwrap();
        fs::create_dir_all(plan.outputs.codex_binding_file.parent().unwrap()).unwrap();
        fs::write(
            &plan.outputs.codex_binding_file,
            serde_json::to_string_pretty(&serde_json::json!({
                "schema": CODEX_BINDING_SCHEMA,
                "queueId": "previous",
                "sessionKey": plan.session_key,
                "threadId": "thread-existing"
            }))
            .unwrap(),
        )
        .unwrap();
        let plan_file = plan_report.plan_file.as_ref().unwrap();
        replace_env_requirements(plan_file, serde_json::json!([]));
        replace_invocation_thread_id(plan_file, Some("thread-existing"));
        let (executable, arguments, events_file) =
            context_error_then_compact_failure_app_server_command(&root);
        replace_invocation(plan_file, executable, arguments);

        let report = run_codex_runtime(CodexRuntimeRunOptions {
            harness_home,
            execution_dir: None,
            plan_file: None,
            timeout_ms: 10_000,
            idle_timeout_ms: 10_000,
            progress_context: None,
        })
        .unwrap();

        assert_eq!(report.receipt.status, CodexRuntimeRunStatus::Completed);
        let recovery = report.receipt.context_recovery.as_ref().unwrap();
        assert_eq!(recovery.status, "fresh-thread-succeeded");
        assert!(recovery.fresh_thread_attempted);
        assert!(recovery.fresh_thread_succeeded);
        assert!(recovery.checkpoint_file.as_ref().unwrap().is_file());
        assert!(recovery.rollover_file.as_ref().unwrap().is_file());
        assert!(recovery.binding_backup_file.as_ref().unwrap().is_file());
        let events = fs::read_to_string(events_file).unwrap();
        assert!(events.contains("thread/compact/start"));
        let transcript =
            fs::read_to_string(report.completion.unwrap().transcript_file.unwrap()).unwrap();
        assert!(transcript.contains("Fresh fallback reply."));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn run_codex_runtime_resumes_existing_thread_binding() {
        let root = temp_root("run_codex_runtime_resumes_existing_thread_binding");
        let source = write_codex_runtime_source(&root);
        let harness_home = root.join(".agent-harness");
        enqueue_and_prepare(&source, &harness_home);
        let plan_report = plan_codex_runtime(CodexRuntimePlanOptions {
            harness_home: harness_home.clone(),
            execution_dir: None,
            codex_executable: Some(std::env::current_exe().unwrap()),
        })
        .unwrap();
        let plan_file = plan_report.plan_file.as_ref().unwrap();
        replace_env_requirements(plan_file, serde_json::json!([]));
        let (executable, arguments) = fake_app_server_command(&root);
        replace_invocation(plan_file, executable, arguments);
        replace_invocation_thread_id(plan_file, Some("thread-existing"));

        let report = run_codex_runtime(CodexRuntimeRunOptions {
            harness_home,
            execution_dir: None,
            plan_file: None,
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            progress_context: None,
        })
        .unwrap();

        assert_eq!(report.receipt.status, CodexRuntimeRunStatus::Completed);
        let transcript_file = report.completion.unwrap().transcript_file.unwrap();
        let transcript = fs::read_to_string(transcript_file).unwrap();
        assert!(transcript.contains("method=thread/resume"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn run_codex_runtime_renews_idle_timeout_after_jsonl_events() {
        let root = temp_root("run_codex_runtime_renews_idle_timeout_after_jsonl_events");
        let source = write_codex_runtime_source(&root);
        let harness_home = root.join(".agent-harness");
        enqueue_and_prepare(&source, &harness_home);
        let plan_report = plan_codex_runtime(CodexRuntimePlanOptions {
            harness_home: harness_home.clone(),
            execution_dir: None,
            codex_executable: Some(std::env::current_exe().unwrap()),
        })
        .unwrap();
        let plan_file = plan_report.plan_file.as_ref().unwrap();
        replace_env_requirements(plan_file, serde_json::json!([]));
        let (executable, arguments) = slow_stream_app_server_command(&root);
        replace_invocation(plan_file, executable, arguments);

        let report = run_codex_runtime(CodexRuntimeRunOptions {
            harness_home,
            execution_dir: None,
            plan_file: None,
            timeout_ms: 20_000,
            idle_timeout_ms: 6_000,
            progress_context: None,
        })
        .unwrap();

        assert_eq!(report.receipt.status, CodexRuntimeRunStatus::Completed);
        assert!(report.receipt.event_count >= 5);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn run_codex_runtime_honors_fresh_cancel_marker() {
        let root = temp_root("run_codex_runtime_honors_fresh_cancel_marker");
        let source = write_codex_runtime_source(&root);
        let harness_home = root.join(".agent-harness");
        enqueue_and_prepare(&source, &harness_home);
        let plan_report = plan_codex_runtime(CodexRuntimePlanOptions {
            harness_home: harness_home.clone(),
            execution_dir: None,
            codex_executable: Some(std::env::current_exe().unwrap()),
        })
        .unwrap();
        let plan_file = plan_report.plan_file.as_ref().unwrap();
        replace_env_requirements(plan_file, serde_json::json!([]));
        let (executable, arguments) = fake_app_server_command(&root);
        replace_invocation(plan_file, executable, arguments);
        let session_key = "telegram:dm-42:user-7:main";
        let cancel_file = harness_home
            .join("state")
            .join("runtime-queue")
            .join("cancel-requests")
            .join(format!("{}.json", normalize_key_part(session_key)));
        write_json_atomic(
            &cancel_file,
            &serde_json::json!({
                "schema": "agent-harness.runtime-cancel-request.v1",
                "atMs": current_log_time_ms().unwrap(),
                "sessionKey": session_key,
                "reason": "test stop"
            }),
        )
        .unwrap();

        let report = run_codex_runtime(CodexRuntimeRunOptions {
            harness_home: harness_home.clone(),
            execution_dir: None,
            plan_file: None,
            timeout_ms: 30_000,
            idle_timeout_ms: 30_000,
            progress_context: None,
        })
        .unwrap();

        assert_eq!(report.receipt.status, CodexRuntimeRunStatus::Canceled);
        assert!(report.receipt.reason.contains("test stop"));
        assert!(report.completion.is_none());
        assert!(!cancel_file.exists());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn answer_unattended_server_request_declines_approval_requests() {
        let mut state = CodexProtocolState::default();
        let mut out = Vec::new();

        let handled = answer_unattended_server_request(
            &serde_json::json!({
                "id": 7,
                "method": "item/commandExecution/requestApproval",
                "params": {}
            }),
            &mut out,
            &mut state,
            CodexApprovalPolicy::Deny,
        )
        .unwrap();

        assert!(handled);
        assert!(
            state
                .warnings
                .iter()
                .any(|warning| { warning.contains("item/commandExecution/requestApproval") })
        );
        let response: Value =
            serde_json::from_slice(String::from_utf8(out).unwrap().trim().as_bytes()).unwrap();
        assert_eq!(response["id"], 7);
        assert_eq!(response["result"]["decision"], "cancel");
        assert_eq!(state.denied_approval_requests.len(), 1);
        assert!(
            state
                .assistant_message_with_harness_notices()
                .contains("codexApprovalPolicy=deny")
        );
    }

    #[test]
    fn answer_unattended_server_request_accepts_approval_requests_when_policy_allows() {
        let mut state = CodexProtocolState::default();
        let mut out = Vec::new();

        let handled = answer_unattended_server_request(
            &serde_json::json!({
                "id": 8,
                "method": "item/commandExecution/requestApproval",
                "params": {
                    "cwd": "D:\\Warehouse\\Research\\OpenClaw_WSL"
                }
            }),
            &mut out,
            &mut state,
            CodexApprovalPolicy::Accept,
        )
        .unwrap();

        assert!(handled);
        assert!(state.denied_approval_requests.is_empty());
        assert!(
            state
                .warnings
                .iter()
                .any(|warning| { warning.contains("auto-accepted") })
        );
        let response: Value =
            serde_json::from_slice(String::from_utf8(out).unwrap().trim().as_bytes()).unwrap();
        assert_eq!(response["id"], 8);
        assert_eq!(response["result"]["decision"], "accept");
    }

    #[test]
    fn answer_unattended_server_request_blocks_live_gateway_control_even_when_policy_allows() {
        let mut state = CodexProtocolState::default();
        let mut out = Vec::new();

        let handled = answer_unattended_server_request(
            &serde_json::json!({
                "id": 88,
                "method": "item/commandExecution/requestApproval",
                "params": {
                    "command": ".\\harness.ps1 gateway restart",
                    "cwd": "D:\\Warehouse\\Rust-OpenClaw-Core"
                }
            }),
            &mut out,
            &mut state,
            CodexApprovalPolicy::Accept,
        )
        .unwrap();

        assert!(handled);
        let response: Value =
            serde_json::from_slice(String::from_utf8(out).unwrap().trim().as_bytes()).unwrap();
        assert_eq!(response["id"], 88);
        assert_eq!(response["result"]["decision"], "cancel");
        assert_eq!(state.denied_approval_requests.len(), 1);
        assert!(
            state
                .warnings
                .iter()
                .any(|warning| warning.contains("blocked protected live gateway control request"))
        );
    }

    #[test]
    fn answer_unattended_server_request_accepts_execpolicy_amendment_decisions() {
        let mut state = CodexProtocolState::default();
        let mut out = Vec::new();

        let handled = answer_unattended_server_request(
            &serde_json::json!({
                "id": 9,
                "method": "item/commandExecution/requestApproval",
                "params": {
                    "availableDecisions": [
                        "accept",
                        {
                            "acceptWithExecpolicyAmendment": {
                                "execpolicy_amendment": [
                                    {
                                        "match": { "program": "git" },
                                        "decision": "allow"
                                    }
                                ]
                            }
                        },
                        "cancel"
                    ]
                }
            }),
            &mut out,
            &mut state,
            CodexApprovalPolicy::Accept,
        )
        .unwrap();

        assert!(handled);
        let response: Value =
            serde_json::from_slice(String::from_utf8(out).unwrap().trim().as_bytes()).unwrap();
        assert_eq!(response["id"], 9);
        assert!(response["result"]["decision"]["acceptWithExecpolicyAmendment"].is_object());
    }

    #[test]
    fn app_server_sandbox_payloads_match_installed_schema() {
        assert_eq!(
            app_server_sandbox_mode_value("dangerFullAccess"),
            "danger-full-access"
        );
        assert_eq!(
            app_server_sandbox_policy_value("danger-full-access", "D:\\Workspace"),
            serde_json::json!({
                "type": "dangerFullAccess"
            })
        );
        assert_eq!(
            app_server_sandbox_policy_value("workspaceWrite", "D:\\Workspace"),
            serde_json::json!({
                "type": "workspaceWrite",
                "writableRoots": ["D:\\Workspace"],
                "networkAccess": false
            })
        );
    }

    #[test]
    fn assistant_output_capture_splits_narration_and_final_items() {
        let mut state = CodexProtocolState::default();
        let mut progress = None;
        let config = AssistantNarrationConfig::default();

        for value in [
            serde_json::json!({
                "method": "item/started",
                "params": {
                    "item": {
                        "type": "agentMessage",
                        "id": "msg-narration",
                        "text": "",
                        "phase": "commentary"
                    },
                    "startedAtMs": 1000
                }
            }),
            serde_json::json!({
                "method": "item/agentMessage/delta",
                "params": {
                    "itemId": "msg-narration",
                    "delta": "Checking config"
                }
            }),
            serde_json::json!({
                "method": "item/completed",
                "params": {
                    "item": {
                        "type": "agentMessage",
                        "id": "msg-narration",
                        "text": "Checking config",
                        "phase": "commentary"
                    },
                    "completedAtMs": 1100
                }
            }),
            serde_json::json!({
                "method": "item/started",
                "params": {
                    "item": {
                        "type": "agentMessage",
                        "id": "msg-final",
                        "text": "",
                        "phase": "final_answer"
                    },
                    "startedAtMs": 1200
                }
            }),
            serde_json::json!({
                "method": "item/agentMessage/delta",
                "params": {
                    "itemId": "msg-final",
                    "delta": "Done."
                }
            }),
            serde_json::json!({
                "method": "item/completed",
                "params": {
                    "item": {
                        "type": "agentMessage",
                        "id": "msg-final",
                        "text": "Done.",
                        "phase": "final_answer"
                    },
                    "completedAtMs": 1300
                }
            }),
        ] {
            collect_agent_output(&value, &mut state, &mut progress, &config);
        }

        assert_eq!(state.assistant_message_with_harness_notices(), "Done.");
        assert!(state.assistant_final_found());
        assert_eq!(
            state.assistant_narration_records(),
            vec![CodexAssistantNarration {
                item_id: Some("msg-narration".to_string()),
                phase: "commentary".to_string(),
                text: "Checking config".to_string(),
            }]
        );
    }

    #[test]
    fn assistant_output_capture_preserves_raw_delta_without_final_boundary() {
        let mut state = CodexProtocolState::default();
        let mut progress = None;
        let config = AssistantNarrationConfig::default();

        collect_agent_output(
            &serde_json::json!({
                "method": "item/agentMessage/delta",
                "params": {
                    "delta": "Legacy fake reply."
                }
            }),
            &mut state,
            &mut progress,
            &config,
        );

        assert_eq!(
            state.assistant_message_with_harness_notices(),
            "Legacy fake reply."
        );
        assert!(!state.assistant_final_found());
        assert!(state.assistant_narration_records().is_empty());
    }

    #[test]
    fn answer_unattended_server_request_declines_legacy_approval_requests() {
        let mut state = CodexProtocolState::default();
        let mut out = Vec::new();

        let handled = answer_unattended_server_request(
            &serde_json::json!({
                "id": "approval-1",
                "method": "execCommandApproval",
                "params": {}
            }),
            &mut out,
            &mut state,
            CodexApprovalPolicy::Deny,
        )
        .unwrap();

        assert!(handled);
        let response: Value =
            serde_json::from_slice(String::from_utf8(out).unwrap().trim().as_bytes()).unwrap();
        assert_eq!(response["id"], "approval-1");
        assert_eq!(response["result"]["decision"], "denied");
    }

    #[test]
    fn record_codex_runtime_completion_writes_outputs_idempotently() {
        let root = temp_root("record_codex_runtime_completion_writes_outputs_idempotently");
        let source = write_codex_runtime_source(&root);
        let harness_home = root.join(".agent-harness");
        enqueue_and_prepare(&source, &harness_home);
        plan_codex_runtime(CodexRuntimePlanOptions {
            harness_home: harness_home.clone(),
            execution_dir: None,
            codex_executable: Some(PathBuf::from("custom-codex.exe")),
        })
        .unwrap();

        let report = record_codex_runtime_completion(CodexRuntimeCompletionOptions {
            harness_home: harness_home.clone(),
            execution_dir: None,
            plan_file: None,
            assistant_message: "Recorded assistant reply.".to_string(),
            assistant_narration: Vec::new(),
            assistant_narration_mode: AssistantNarrationMode::ProgressPanel,
            thread_id: Some("thread-recorded".to_string()),
            finished_at_ms: 12345,
        })
        .unwrap();

        assert_eq!(
            report.receipt.status,
            CodexRuntimeCompletionStatus::Recorded
        );
        let transcript_file = report.transcript_file.clone().unwrap();
        let trajectory_file = report.trajectory_file.clone().unwrap();
        let binding_file = report.codex_binding_file.clone().unwrap();
        assert!(transcript_file.is_file());
        assert!(trajectory_file.is_file());
        assert!(binding_file.is_file());
        let transcript = fs::read_to_string(&transcript_file).unwrap();
        assert_eq!(transcript.lines().count(), 2);
        assert!(transcript.contains("\"role\":\"user\""));
        assert!(transcript.contains("\"role\":\"assistant\""));
        assert!(transcript.contains("Recorded assistant reply."));
        let binding: Value = serde_json::from_slice(&fs::read(binding_file).unwrap()).unwrap();
        assert_eq!(binding["schema"], CODEX_BINDING_SCHEMA);
        assert_eq!(binding["sessionKey"], "telegram:dm-42:user-7:main");
        assert_eq!(binding["threadId"], "thread-recorded");

        let second = record_codex_runtime_completion(CodexRuntimeCompletionOptions {
            harness_home: harness_home.clone(),
            execution_dir: None,
            plan_file: None,
            assistant_message: "Should not be duplicated.".to_string(),
            assistant_narration: Vec::new(),
            assistant_narration_mode: AssistantNarrationMode::ProgressPanel,
            thread_id: Some("ignored-thread".to_string()),
            finished_at_ms: 12346,
        })
        .unwrap();
        assert_eq!(
            second.receipt.status,
            CodexRuntimeCompletionStatus::AlreadyRecorded
        );
        assert_eq!(
            fs::read_to_string(transcript_file).unwrap().lines().count(),
            2
        );

        enqueue_and_prepare(&source, &harness_home);
        let resumed_plan = plan_codex_runtime(CodexRuntimePlanOptions {
            harness_home: harness_home.clone(),
            execution_dir: None,
            codex_executable: Some(PathBuf::from("custom-codex.exe")),
        })
        .unwrap()
        .plan
        .unwrap();
        assert_eq!(
            resumed_plan.invocation.thread_id.as_deref(),
            Some("thread-recorded")
        );
        assert_eq!(resumed_plan.invocation.working_directory, source.workspace);

        let _ = fs::remove_dir_all(root);
    }

    fn enqueue_and_prepare(source: &crate::AgentSource, harness_home: &Path) {
        enqueue_and_prepare_with_runtime_workspace(source, harness_home, None);
    }

    fn enqueue_and_prepare_at(source: &crate::AgentSource, harness_home: &Path, now_ms: i64) {
        enqueue_and_prepare_with_runtime_workspace_at(source, harness_home, None, now_ms);
    }

    fn enqueue_and_prepare_with_runtime_workspace(
        source: &crate::AgentSource,
        harness_home: &Path,
        runtime_workspace: Option<PathBuf>,
    ) {
        enqueue_and_prepare_with_runtime_workspace_at(
            source,
            harness_home,
            runtime_workspace,
            1234,
        );
    }

    fn enqueue_and_prepare_with_runtime_workspace_at(
        source: &crate::AgentSource,
        harness_home: &Path,
        runtime_workspace: Option<PathBuf>,
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
        let step = build_channel_step(&registry, &turn);
        enqueue_channel_step(
            &step,
            RuntimeQueueEnqueueOptions {
                harness_home: harness_home.to_path_buf(),
                runtime_workspace,
                now_ms,
            },
        )
        .unwrap();
        prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.to_path_buf(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();
    }

    fn replace_env_requirements(plan_file: &Path, requirements: Value) {
        let mut value: Value = serde_json::from_slice(&fs::read(plan_file).unwrap()).unwrap();
        value["invocation"]["envRequirements"] = requirements;
        fs::write(plan_file, serde_json::to_string_pretty(&value).unwrap()).unwrap();
    }

    fn latest_prepared_execution_dir(harness_home: &Path) -> PathBuf {
        fs::read_to_string(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("execution-receipts.jsonl"),
        )
        .unwrap()
        .lines()
        .rev()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find_map(|value| path_field(&value, &["executionDir", "execution_dir"]))
        .unwrap()
    }

    fn replace_invocation(plan_file: &Path, executable: PathBuf, arguments: Vec<String>) {
        let mut value: Value = serde_json::from_slice(&fs::read(plan_file).unwrap()).unwrap();
        value["invocation"]["executable"] = serde_json::json!(executable);
        value["invocation"]["arguments"] = serde_json::json!(arguments);
        fs::write(plan_file, serde_json::to_string_pretty(&value).unwrap()).unwrap();
    }

    fn replace_invocation_thread_id(plan_file: &Path, thread_id: Option<&str>) {
        let mut value: Value = serde_json::from_slice(&fs::read(plan_file).unwrap()).unwrap();
        value["invocation"]["threadId"] = match thread_id {
            Some(thread_id) => serde_json::json!(thread_id),
            None => Value::Null,
        };
        fs::write(plan_file, serde_json::to_string_pretty(&value).unwrap()).unwrap();
    }

    #[cfg(windows)]
    fn long_running_probe_command() -> (PathBuf, Vec<String>) {
        let system_cmd = PathBuf::from(r"C:\Windows\System32\cmd.exe");
        let executable = if system_cmd.is_file() {
            system_cmd
        } else {
            PathBuf::from("cmd.exe")
        };
        (executable, vec!["/K".to_string()])
    }

    #[cfg(not(windows))]
    fn long_running_probe_command() -> (PathBuf, Vec<String>) {
        (
            PathBuf::from("sh"),
            vec!["-c".to_string(), "while true; do sleep 1; done".to_string()],
        )
    }

    #[cfg(windows)]
    fn fake_app_server_command(root: &Path) -> (PathBuf, Vec<String>) {
        let script = root.join("fake-app-server.ps1");
        fs::write(
            &script,
            r#"
while ($true) {
    if ($null -eq $threadMethod) { $threadMethod = 'unknown' }
    $line = [Console]::In.ReadLine()
    if ($null -eq $line) { break }
    try {
        $msg = $line | ConvertFrom-Json
    } catch {
        continue
    }
    if ($msg.id -eq 0) {
        [Console]::Out.WriteLine('{"id":0,"result":{"ok":true}}')
        [Console]::Out.Flush()
    } elseif ($msg.method -eq 'thread/start') {
        $threadMethod = 'thread/start'
        [Console]::Out.WriteLine('{"id":1,"result":{"thread":{"id":"thread-test"}}}')
        [Console]::Out.Flush()
    } elseif ($msg.method -eq 'thread/resume') {
        $threadMethod = 'thread/resume'
        [Console]::Out.WriteLine('{"id":1,"result":{"thread":{"id":"thread-test"}}}')
        [Console]::Out.Flush()
    } elseif ($msg.method -eq 'turn/start') {
        [Console]::Out.WriteLine(('{"method":"item/agentMessage/delta","params":{"delta":"Fake assistant reply. method=' + $threadMethod + '"}}'))
        [Console]::Out.WriteLine('{"method":"turn/completed","params":{"turn":{"id":"turn-test","status":"completed","usage":{"inputTokens":30,"outputTokens":12,"totalTokens":42}}}}')
        [Console]::Out.Flush()
        break
    }
}
"#,
        )
        .unwrap();
        let system_powershell =
            PathBuf::from(r"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe");
        let executable = if system_powershell.is_file() {
            system_powershell
        } else {
            PathBuf::from("powershell.exe")
        };
        (
            executable,
            vec![
                "-NoProfile".to_string(),
                "-ExecutionPolicy".to_string(),
                "Bypass".to_string(),
                "-File".to_string(),
                script.display().to_string(),
            ],
        )
    }

    #[cfg(windows)]
    fn slow_stream_app_server_command(root: &Path) -> (PathBuf, Vec<String>) {
        let script = root.join("slow-stream-app-server.ps1");
        fs::write(
            &script,
            r#"
while ($true) {
    $line = [Console]::In.ReadLine()
    if ($null -eq $line) { break }
    try {
        $msg = $line | ConvertFrom-Json
    } catch {
        continue
    }
    if ($msg.id -eq 0) {
        [Console]::Out.WriteLine('{"id":0,"result":{"ok":true}}')
        [Console]::Out.Flush()
    } elseif ($msg.method -eq 'thread/start') {
        [Console]::Out.WriteLine('{"id":1,"result":{"thread":{"id":"thread-test"}}}')
        [Console]::Out.Flush()
    } elseif ($msg.method -eq 'turn/start') {
        Start-Sleep -Milliseconds 2500
        [Console]::Out.WriteLine('{"method":"item/agentMessage/delta","params":{"delta":"Slow "}}')
        [Console]::Out.Flush()
        Start-Sleep -Milliseconds 2500
        [Console]::Out.WriteLine('{"method":"item/agentMessage/delta","params":{"delta":"stream reply."}}')
        [Console]::Out.Flush()
        Start-Sleep -Milliseconds 2500
        [Console]::Out.WriteLine('{"method":"turn/completed","params":{"turn":{"id":"turn-test","status":"completed"}}}')
        [Console]::Out.Flush()
        break
    }
}
"#,
        )
        .unwrap();
        let system_powershell =
            PathBuf::from(r"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe");
        let executable = if system_powershell.is_file() {
            system_powershell
        } else {
            PathBuf::from("powershell.exe")
        };
        (
            executable,
            vec![
                "-NoProfile".to_string(),
                "-ExecutionPolicy".to_string(),
                "Bypass".to_string(),
                "-File".to_string(),
                script.display().to_string(),
            ],
        )
    }

    #[cfg(windows)]
    fn stdout_holding_app_server_command(root: &Path) -> (PathBuf, Vec<String>) {
        let script = root.join("stdout-holding-app-server.ps1");
        fs::write(
            &script,
            r#"
while ($true) {
    $line = [Console]::In.ReadLine()
    if ($null -eq $line) { break }
    try {
        $msg = $line | ConvertFrom-Json
    } catch {
        continue
    }
    if ($msg.id -eq 0) {
        [Console]::Out.WriteLine('{"id":0,"result":{"ok":true}}')
        [Console]::Out.Flush()
    } elseif ($msg.method -eq 'thread/start') {
        [Console]::Out.WriteLine('{"id":1,"result":{"thread":{"id":"thread-test"}}}')
        [Console]::Out.Flush()
    } elseif ($msg.method -eq 'turn/start') {
        [Console]::Out.WriteLine('{"method":"item/completed","params":{"item":{"type":"agentMessage","id":"msg-test","text":"stdout holder reply","phase":"final_answer"},"threadId":"thread-test","completedAtMs":1234}}')
        [Console]::Out.WriteLine('{"method":"turn/completed","params":{"threadId":"thread-test","turn":{"id":"turn-test","status":"completed"}}}')
        [Console]::Out.Flush()
        $holder = Join-Path $PSScriptRoot 'stdout-holder.ps1'
        Set-Content -LiteralPath $holder -Value 'Start-Sleep -Seconds 5'
        $powershell = (Get-Command powershell.exe).Source
        Start-Process -FilePath $powershell -ArgumentList @('-NoProfile','-ExecutionPolicy','Bypass','-File',$holder) -NoNewWindow
        break
    }
}
"#,
        )
        .unwrap();
        let system_powershell =
            PathBuf::from(r"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe");
        let executable = if system_powershell.is_file() {
            system_powershell
        } else {
            PathBuf::from("powershell.exe")
        };
        (
            executable,
            vec![
                "-NoProfile".to_string(),
                "-ExecutionPolicy".to_string(),
                "Bypass".to_string(),
                "-File".to_string(),
                script.display().to_string(),
            ],
        )
    }

    #[cfg(windows)]
    fn failing_app_server_command(root: &Path) -> (PathBuf, Vec<String>) {
        let script = root.join("failing-app-server.ps1");
        fs::write(
            &script,
            r#"
while ($true) {
    $line = [Console]::In.ReadLine()
    if ($null -eq $line) { break }
    try {
        $msg = $line | ConvertFrom-Json
    } catch {
        continue
    }
    if ($msg.id -eq 0) {
        [Console]::Out.WriteLine('{"id":0,"result":{"ok":true}}')
        [Console]::Out.Flush()
    } elseif ($msg.method -eq 'thread/start') {
        [Console]::Out.WriteLine('{"id":1,"result":{"thread":{"id":"thread-test"}}}')
        [Console]::Out.Flush()
    } elseif ($msg.method -eq 'turn/start') {
        [Console]::Out.WriteLine('{"method":"error","params":{"error":{"message":"Missing environment variable: `OPENROUTER_API_KEY`.","codexErrorInfo":"other","additionalDetails":null},"willRetry":false,"threadId":"thread-test","turnId":"turn-failed"}}')
        [Console]::Out.WriteLine('{"method":"turn/completed","params":{"threadId":"thread-test","turn":{"id":"turn-failed","status":"failed","error":{"message":"Missing environment variable: `OPENROUTER_API_KEY`."}}}}')
        [Console]::Out.Flush()
        break
    }
}
"#,
        )
        .unwrap();
        let system_powershell =
            PathBuf::from(r"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe");
        let executable = if system_powershell.is_file() {
            system_powershell
        } else {
            PathBuf::from("powershell.exe")
        };
        (
            executable,
            vec![
                "-NoProfile".to_string(),
                "-ExecutionPolicy".to_string(),
                "Bypass".to_string(),
                "-File".to_string(),
                script.display().to_string(),
            ],
        )
    }

    #[cfg(windows)]
    fn compact_tracking_app_server_command(root: &Path) -> (PathBuf, Vec<String>, PathBuf) {
        let script = root.join("compact-tracking-app-server.ps1");
        let events = root.join("compact-events.jsonl");
        fs::write(
            &script,
            r#"
$events = Join-Path $PSScriptRoot 'compact-events.jsonl'
while ($true) {
    $line = [Console]::In.ReadLine()
    if ($null -eq $line) { break }
    try { $msg = $line | ConvertFrom-Json } catch { continue }
    if ($msg.method) { Add-Content -LiteralPath $events -Value $msg.method }
    if ($msg.id -eq 0) {
        [Console]::Out.WriteLine('{"id":0,"result":{"ok":true}}')
        [Console]::Out.Flush()
    } elseif ($msg.method -eq 'thread/start') {
        [Console]::Out.WriteLine('{"id":1,"result":{"thread":{"id":"thread-test"}}}')
        [Console]::Out.Flush()
    } elseif ($msg.method -eq 'thread/resume') {
        [Console]::Out.WriteLine('{"id":1,"result":{"thread":{"id":"thread-test"}}}')
        [Console]::Out.Flush()
    } elseif ($msg.method -eq 'thread/compact/start') {
        [Console]::Out.WriteLine('{"id":2,"result":{}}')
        [Console]::Out.WriteLine('{"method":"item/started","params":{"item":{"type":"contextCompaction","id":"compact-1"},"threadId":"thread-test"}}')
        [Console]::Out.WriteLine('{"method":"item/completed","params":{"item":{"type":"contextCompaction","id":"compact-1"},"threadId":"thread-test"}}')
        [Console]::Out.Flush()
    } elseif ($msg.method -eq 'turn/start') {
        [Console]::Out.WriteLine('{"method":"item/completed","params":{"item":{"type":"agentMessage","id":"msg-test","text":"Compacted reply.","phase":"final_answer"},"threadId":"thread-test","completedAtMs":1234}}')
        [Console]::Out.WriteLine('{"method":"turn/completed","params":{"threadId":"thread-test","turn":{"id":"turn-test","status":"completed","usage":{"inputTokens":30,"outputTokens":12,"totalTokens":42}}}}')
        [Console]::Out.Flush()
        break
    }
}
"#,
        )
        .unwrap();
        let system_powershell =
            PathBuf::from(r"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe");
        let executable = if system_powershell.is_file() {
            system_powershell
        } else {
            PathBuf::from("powershell.exe")
        };
        (
            executable,
            vec![
                "-NoProfile".to_string(),
                "-ExecutionPolicy".to_string(),
                "Bypass".to_string(),
                "-File".to_string(),
                script.display().to_string(),
            ],
            events,
        )
    }

    #[cfg(windows)]
    fn stream_disconnect_then_fresh_thread_app_server_command(
        root: &Path,
    ) -> (PathBuf, Vec<String>, PathBuf) {
        let script = root.join("stream-disconnect-then-fresh-thread.ps1");
        let events = root.join("stream-disconnect-events.jsonl");
        fs::write(
            &script,
            r#"
$events = Join-Path $PSScriptRoot 'stream-disconnect-events.jsonl'
$first = Join-Path $PSScriptRoot 'stream-disconnect-first.marker'
while ($true) {
    $line = [Console]::In.ReadLine()
    if ($null -eq $line) { break }
    try { $msg = $line | ConvertFrom-Json } catch { continue }
    if ($msg.method) { Add-Content -LiteralPath $events -Value $msg.method }
    if ($msg.id -eq 0) {
        [Console]::Out.WriteLine('{"id":0,"result":{"ok":true}}')
        [Console]::Out.Flush()
    } elseif ($msg.method -eq 'thread/resume') {
        [Console]::Out.WriteLine('{"id":1,"result":{"thread":{"id":"thread-bloated"}}}')
        [Console]::Out.Flush()
    } elseif ($msg.method -eq 'thread/start') {
        [Console]::Out.WriteLine('{"id":1,"result":{"thread":{"id":"thread-fresh"}}}')
        [Console]::Out.Flush()
    } elseif ($msg.method -eq 'thread/compact/start') {
        [Console]::Out.WriteLine('{"id":2,"result":{}}')
        [Console]::Out.WriteLine('{"method":"item/started","params":{"item":{"type":"contextCompaction","id":"compact-1"},"threadId":"thread-bloated"}}')
        [Console]::Out.WriteLine('{"method":"item/completed","params":{"item":{"type":"contextCompaction","id":"compact-1"},"threadId":"thread-bloated"}}')
        [Console]::Out.Flush()
    } elseif ($msg.method -eq 'turn/start') {
        if (!(Test-Path -LiteralPath $first)) {
            Set-Content -LiteralPath $first -Value 'failed'
            [Console]::Out.WriteLine('{"method":"error","params":{"error":{"message":"Reconnecting... 2/5","additionalDetails":"stream disconnected before completion: websocket closed by server before response.completed"},"willRetry":true,"threadId":"thread-bloated","turnId":"turn-failed"}}')
            [Console]::Out.Flush()
            break
        }
        [Console]::Out.WriteLine('{"method":"item/completed","params":{"item":{"type":"agentMessage","id":"msg-fresh","text":"Fresh fallback reply.","phase":"final_answer"},"threadId":"thread-fresh","completedAtMs":1234}}')
        [Console]::Out.WriteLine('{"method":"turn/completed","params":{"threadId":"thread-fresh","turn":{"id":"turn-fresh","status":"completed","usage":{"inputTokens":18,"outputTokens":7,"totalTokens":25}}}}')
        [Console]::Out.Flush()
        break
    }
}
"#,
        )
        .unwrap();
        let system_powershell =
            PathBuf::from(r"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe");
        let executable = if system_powershell.is_file() {
            system_powershell
        } else {
            PathBuf::from("powershell.exe")
        };
        (
            executable,
            vec![
                "-NoProfile".to_string(),
                "-ExecutionPolicy".to_string(),
                "Bypass".to_string(),
                "-File".to_string(),
                script.display().to_string(),
            ],
            events,
        )
    }

    #[cfg(windows)]
    fn preflight_compact_problem_then_fresh_thread_app_server_command(
        root: &Path,
        scenario: &str,
    ) -> (PathBuf, Vec<String>, PathBuf) {
        let script = root.join("preflight-compact-problem.ps1");
        let events = root.join("preflight-compact-problem-events.jsonl");
        fs::write(
            &script,
            r#"
$scenario = $args[0]
if ([string]::IsNullOrWhiteSpace($scenario)) { $scenario = 'failed' }
$events = Join-Path $PSScriptRoot 'preflight-compact-problem-events.jsonl'
$marker = Join-Path $PSScriptRoot ("preflight-compact-" + $scenario + ".marker")
while ($true) {
    $line = [Console]::In.ReadLine()
    if ($null -eq $line) { break }
    try { $msg = $line | ConvertFrom-Json } catch { continue }
    if ($msg.method) { Add-Content -LiteralPath $events -Value $msg.method }
    if ($msg.id -eq 0) {
        [Console]::Out.WriteLine('{"id":0,"result":{"ok":true}}')
        [Console]::Out.Flush()
    } elseif ($msg.method -eq 'thread/start' -or $msg.method -eq 'thread/resume') {
        [Console]::Out.WriteLine('{"id":1,"result":{"thread":{"id":"thread-test"}}}')
        [Console]::Out.Flush()
    } elseif ($msg.method -eq 'thread/compact/start') {
        Set-Content -LiteralPath $marker -Value 'compact-problem'
        if ($scenario -eq 'timeout') {
            Start-Sleep -Seconds 10
            break
        } elseif ($scenario -eq 'unexpected') {
            [Console]::Out.WriteLine('{"method":"turn/completed","params":{"threadId":"thread-test","turn":{"id":"turn-unexpected","status":"completed"}}}')
            [Console]::Out.Flush()
            break
        } else {
            [Console]::Out.WriteLine('{"method":"error","params":{"error":{"message":"ContextWindowExceeded: compact failed before turn."},"threadId":"thread-test","turnId":"compact-failed"}}')
            [Console]::Out.Flush()
            break
        }
    } elseif ($msg.method -eq 'turn/start') {
        [Console]::Out.WriteLine('{"method":"item/completed","params":{"item":{"type":"agentMessage","id":"msg-fresh","text":"Fresh fallback reply.","phase":"final_answer"},"threadId":"thread-test","completedAtMs":1234}}')
        [Console]::Out.WriteLine('{"method":"turn/completed","params":{"threadId":"thread-test","turn":{"id":"turn-fresh","status":"completed","usage":{"inputTokens":18,"outputTokens":7,"totalTokens":25}}}}')
        [Console]::Out.Flush()
        break
    }
}
"#,
        )
        .unwrap();
        let system_powershell =
            PathBuf::from(r"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe");
        let executable = if system_powershell.is_file() {
            system_powershell
        } else {
            PathBuf::from("powershell.exe")
        };
        (
            executable,
            vec![
                "-NoProfile".to_string(),
                "-ExecutionPolicy".to_string(),
                "Bypass".to_string(),
                "-File".to_string(),
                script.display().to_string(),
                scenario.to_string(),
            ],
            events,
        )
    }

    #[cfg(windows)]
    fn context_error_then_compact_success_app_server_command(
        root: &Path,
    ) -> (PathBuf, Vec<String>, PathBuf) {
        let script = root.join("context-error-then-compact-success.ps1");
        let events = root.join("context-retry-events.jsonl");
        fs::write(
            &script,
            r#"
$events = Join-Path $PSScriptRoot 'context-retry-events.jsonl'
$marker = Join-Path $PSScriptRoot 'context-first-failed.marker'
while ($true) {
    $line = [Console]::In.ReadLine()
    if ($null -eq $line) { break }
    try { $msg = $line | ConvertFrom-Json } catch { continue }
    if ($msg.method) { Add-Content -LiteralPath $events -Value $msg.method }
    if ($msg.id -eq 0) {
        [Console]::Out.WriteLine('{"id":0,"result":{"ok":true}}')
        [Console]::Out.Flush()
    } elseif ($msg.method -eq 'thread/start' -or $msg.method -eq 'thread/resume') {
        [Console]::Out.WriteLine('{"id":1,"result":{"thread":{"id":"thread-test"}}}')
        [Console]::Out.Flush()
    } elseif ($msg.method -eq 'thread/compact/start') {
        [Console]::Out.WriteLine('{"id":2,"result":{}}')
        [Console]::Out.WriteLine('{"method":"item/started","params":{"item":{"type":"contextCompaction","id":"compact-1"},"threadId":"thread-test"}}')
        [Console]::Out.WriteLine('{"method":"item/completed","params":{"item":{"type":"contextCompaction","id":"compact-1"},"threadId":"thread-test"}}')
        [Console]::Out.Flush()
    } elseif ($msg.method -eq 'turn/start') {
        if (!(Test-Path -LiteralPath $marker)) {
            Set-Content -LiteralPath $marker -Value 'failed'
            [Console]::Out.WriteLine('{"method":"error","params":{"error":{"message":"ContextWindowExceeded: ran out of room in the model context window."},"threadId":"thread-test","turnId":"turn-failed"}}')
            [Console]::Out.WriteLine('{"method":"turn/completed","params":{"threadId":"thread-test","turn":{"id":"turn-failed","status":"failed","error":{"message":"ContextWindowExceeded: ran out of room in the model context window."}}}}')
            [Console]::Out.Flush()
            break
        }
        [Console]::Out.WriteLine('{"method":"item/completed","params":{"item":{"type":"agentMessage","id":"msg-test","text":"Recovered after compact.","phase":"final_answer"},"threadId":"thread-test","completedAtMs":1234}}')
        [Console]::Out.WriteLine('{"method":"turn/completed","params":{"threadId":"thread-test","turn":{"id":"turn-test","status":"completed","usage":{"inputTokens":20,"outputTokens":8,"totalTokens":28}}}}')
        [Console]::Out.Flush()
        break
    }
}
"#,
        )
        .unwrap();
        let system_powershell =
            PathBuf::from(r"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe");
        let executable = if system_powershell.is_file() {
            system_powershell
        } else {
            PathBuf::from("powershell.exe")
        };
        (
            executable,
            vec![
                "-NoProfile".to_string(),
                "-ExecutionPolicy".to_string(),
                "Bypass".to_string(),
                "-File".to_string(),
                script.display().to_string(),
            ],
            events,
        )
    }

    #[cfg(windows)]
    fn context_error_then_compact_failure_app_server_command(
        root: &Path,
    ) -> (PathBuf, Vec<String>, PathBuf) {
        let script = root.join("context-error-then-compact-failure.ps1");
        let events = root.join("context-fallback-events.jsonl");
        fs::write(
            &script,
            r#"
$events = Join-Path $PSScriptRoot 'context-fallback-events.jsonl'
$first = Join-Path $PSScriptRoot 'context-first-failed.marker'
$compact = Join-Path $PSScriptRoot 'compact-failed.marker'
while ($true) {
    $line = [Console]::In.ReadLine()
    if ($null -eq $line) { break }
    try { $msg = $line | ConvertFrom-Json } catch { continue }
    if ($msg.method) { Add-Content -LiteralPath $events -Value $msg.method }
    if ($msg.id -eq 0) {
        [Console]::Out.WriteLine('{"id":0,"result":{"ok":true}}')
        [Console]::Out.Flush()
    } elseif ($msg.method -eq 'thread/start' -or $msg.method -eq 'thread/resume') {
        [Console]::Out.WriteLine('{"id":1,"result":{"thread":{"id":"thread-test"}}}')
        [Console]::Out.Flush()
    } elseif ($msg.method -eq 'thread/compact/start') {
        Set-Content -LiteralPath $compact -Value 'failed'
        [Console]::Out.WriteLine('{"method":"error","params":{"error":{"message":"ContextWindowExceeded: compact failed inside the model context window."},"threadId":"thread-test","turnId":"compact-failed"}}')
        [Console]::Out.Flush()
        break
    } elseif ($msg.method -eq 'turn/start') {
        if (!(Test-Path -LiteralPath $first)) {
            Set-Content -LiteralPath $first -Value 'failed'
            [Console]::Out.WriteLine('{"method":"error","params":{"error":{"message":"ContextWindowExceeded: ran out of room in the model context window."},"threadId":"thread-test","turnId":"turn-failed"}}')
            [Console]::Out.WriteLine('{"method":"turn/completed","params":{"threadId":"thread-test","turn":{"id":"turn-failed","status":"failed","error":{"message":"ContextWindowExceeded: ran out of room in the model context window."}}}}')
            [Console]::Out.Flush()
            break
        }
        [Console]::Out.WriteLine('{"method":"item/completed","params":{"item":{"type":"agentMessage","id":"msg-fresh","text":"Fresh fallback reply.","phase":"final_answer"},"threadId":"thread-test","completedAtMs":1234}}')
        [Console]::Out.WriteLine('{"method":"turn/completed","params":{"threadId":"thread-test","turn":{"id":"turn-fresh","status":"completed","usage":{"inputTokens":18,"outputTokens":7,"totalTokens":25}}}}')
        [Console]::Out.Flush()
        break
    }
}
"#,
        )
        .unwrap();
        let system_powershell =
            PathBuf::from(r"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe");
        let executable = if system_powershell.is_file() {
            system_powershell
        } else {
            PathBuf::from("powershell.exe")
        };
        (
            executable,
            vec![
                "-NoProfile".to_string(),
                "-ExecutionPolicy".to_string(),
                "Bypass".to_string(),
                "-File".to_string(),
                script.display().to_string(),
            ],
            events,
        )
    }

    #[cfg(not(windows))]
    fn fake_app_server_command(root: &Path) -> (PathBuf, Vec<String>) {
        let script = root.join("fake-app-server.sh");
        fs::write(
            &script,
            r#"
while IFS= read -r line; do
    case "$line" in
        *'"id":0'*)
            printf '%s\n' '{"id":0,"result":{"ok":true}}'
            ;;
        *'"method":"thread/start"'*)
            thread_method='thread/start'
            printf '%s\n' '{"id":1,"result":{"thread":{"id":"thread-test"}}}'
            ;;
        *'"method":"thread/resume"'*)
            thread_method='thread/resume'
            printf '%s\n' '{"id":1,"result":{"thread":{"id":"thread-test"}}}'
            ;;
        *'"method":"turn/start"'*)
            printf '%s\n' "{\"method\":\"item/agentMessage/delta\",\"params\":{\"delta\":\"Fake assistant reply. method=${thread_method:-unknown}\"}}"
            printf '%s\n' '{"method":"turn/completed","params":{"turn":{"id":"turn-test","status":"completed","usage":{"inputTokens":30,"outputTokens":12,"totalTokens":42}}}}'
            exit 0
            ;;
    esac
done
"#,
        )
        .unwrap();
        (PathBuf::from("sh"), vec![script.display().to_string()])
    }

    #[cfg(not(windows))]
    fn slow_stream_app_server_command(root: &Path) -> (PathBuf, Vec<String>) {
        let script = root.join("slow-stream-app-server.sh");
        fs::write(
            &script,
            r#"
while IFS= read -r line; do
    case "$line" in
        *'"id":0'*)
            printf '%s\n' '{"id":0,"result":{"ok":true}}'
            ;;
        *'"method":"thread/start"'*)
            printf '%s\n' '{"id":1,"result":{"thread":{"id":"thread-test"}}}'
            ;;
        *'"method":"turn/start"'*)
            sleep 2.5
            printf '%s\n' '{"method":"item/agentMessage/delta","params":{"delta":"Slow "}}'
            sleep 2.5
            printf '%s\n' '{"method":"item/agentMessage/delta","params":{"delta":"stream reply."}}'
            sleep 2.5
            printf '%s\n' '{"method":"turn/completed","params":{"turn":{"id":"turn-test","status":"completed"}}}'
            break
            ;;
    esac
done
"#,
        )
        .unwrap();
        make_executable(&script);
        (PathBuf::from("sh"), vec![script.display().to_string()])
    }

    #[cfg(not(windows))]
    fn stdout_holding_app_server_command(root: &Path) -> (PathBuf, Vec<String>) {
        let script = root.join("stdout-holding-app-server.sh");
        fs::write(
            &script,
            r#"
while IFS= read -r line; do
    case "$line" in
        *'"id":0'*)
            printf '%s\n' '{"id":0,"result":{"ok":true}}'
            ;;
        *'"method":"thread/start"'*)
            printf '%s\n' '{"id":1,"result":{"thread":{"id":"thread-test"}}}'
            ;;
        *'"method":"turn/start"'*)
            printf '%s\n' '{"method":"item/completed","params":{"item":{"type":"agentMessage","id":"msg-test","text":"stdout holder reply","phase":"final_answer"},"threadId":"thread-test","completedAtMs":1234}}'
            printf '%s\n' '{"method":"turn/completed","params":{"threadId":"thread-test","turn":{"id":"turn-test","status":"completed"}}}'
            sleep 5 &
            exit 0
            ;;
    esac
done
"#,
        )
        .unwrap();
        make_executable(&script);
        (PathBuf::from("sh"), vec![script.display().to_string()])
    }

    #[cfg(not(windows))]
    fn failing_app_server_command(root: &Path) -> (PathBuf, Vec<String>) {
        let script = root.join("failing-app-server.sh");
        fs::write(
            &script,
            r#"
while IFS= read -r line; do
    case "$line" in
        *'"id":0'*)
            printf '%s\n' '{"id":0,"result":{"ok":true}}'
            ;;
        *'"method":"thread/start"'*)
            printf '%s\n' '{"id":1,"result":{"thread":{"id":"thread-test"}}}'
            ;;
        *'"method":"turn/start"'*)
            printf '%s\n' '{"method":"error","params":{"error":{"message":"Missing environment variable: `OPENROUTER_API_KEY`.","codexErrorInfo":"other","additionalDetails":null},"willRetry":false,"threadId":"thread-test","turnId":"turn-failed"}}'
            printf '%s\n' '{"method":"turn/completed","params":{"threadId":"thread-test","turn":{"id":"turn-failed","status":"failed","error":{"message":"Missing environment variable: `OPENROUTER_API_KEY`."}}}}'
            exit 0
            ;;
    esac
done
"#,
        )
        .unwrap();
        make_executable(&script);
        (PathBuf::from("sh"), vec![script.display().to_string()])
    }

    #[cfg(not(windows))]
    fn compact_tracking_app_server_command(root: &Path) -> (PathBuf, Vec<String>, PathBuf) {
        let script = root.join("compact-tracking-app-server.sh");
        let events = root.join("compact-events.jsonl");
        fs::write(
            &script,
            r#"
events="$(dirname "$0")/compact-events.jsonl"
while IFS= read -r line; do
    method="$(printf '%s' "$line" | sed -n 's/.*"method":"\([^"]*\)".*/\1/p')"
    [ -n "$method" ] && printf '%s\n' "$method" >> "$events"
    case "$line" in
        *'"id":0'*)
            printf '%s\n' '{"id":0,"result":{"ok":true}}'
            ;;
        *'"method":"thread/start"'*|*'"method":"thread/resume"'*)
            printf '%s\n' '{"id":1,"result":{"thread":{"id":"thread-test"}}}'
            ;;
        *'"method":"thread/compact/start"'*)
            printf '%s\n' '{"id":2,"result":{}}'
            printf '%s\n' '{"method":"item/started","params":{"item":{"type":"contextCompaction","id":"compact-1"},"threadId":"thread-test"}}'
            printf '%s\n' '{"method":"item/completed","params":{"item":{"type":"contextCompaction","id":"compact-1"},"threadId":"thread-test"}}'
            ;;
        *'"method":"turn/start"'*)
            printf '%s\n' '{"method":"item/completed","params":{"item":{"type":"agentMessage","id":"msg-test","text":"Compacted reply.","phase":"final_answer"},"threadId":"thread-test","completedAtMs":1234}}'
            printf '%s\n' '{"method":"turn/completed","params":{"threadId":"thread-test","turn":{"id":"turn-test","status":"completed","usage":{"inputTokens":30,"outputTokens":12,"totalTokens":42}}}}'
            exit 0
            ;;
    esac
done
"#,
        )
        .unwrap();
        make_executable(&script);
        (
            PathBuf::from("sh"),
            vec![script.display().to_string()],
            events,
        )
    }

    #[cfg(not(windows))]
    fn stream_disconnect_then_fresh_thread_app_server_command(
        root: &Path,
    ) -> (PathBuf, Vec<String>, PathBuf) {
        let script = root.join("stream-disconnect-then-fresh-thread.sh");
        let events = root.join("stream-disconnect-events.jsonl");
        fs::write(
            &script,
            r#"
dir="$(dirname "$0")"
events="$dir/stream-disconnect-events.jsonl"
first="$dir/stream-disconnect-first.marker"
while IFS= read -r line; do
    method="$(printf '%s' "$line" | sed -n 's/.*"method":"\([^"]*\)".*/\1/p')"
    [ -n "$method" ] && printf '%s\n' "$method" >> "$events"
    case "$line" in
        *'"id":0'*)
            printf '%s\n' '{"id":0,"result":{"ok":true}}'
            ;;
        *'"method":"thread/resume"'*)
            printf '%s\n' '{"id":1,"result":{"thread":{"id":"thread-bloated"}}}'
            ;;
        *'"method":"thread/start"'*)
            printf '%s\n' '{"id":1,"result":{"thread":{"id":"thread-fresh"}}}'
            ;;
        *'"method":"thread/compact/start"'*)
            printf '%s\n' '{"id":2,"result":{}}'
            printf '%s\n' '{"method":"item/started","params":{"item":{"type":"contextCompaction","id":"compact-1"},"threadId":"thread-bloated"}}'
            printf '%s\n' '{"method":"item/completed","params":{"item":{"type":"contextCompaction","id":"compact-1"},"threadId":"thread-bloated"}}'
            ;;
        *'"method":"turn/start"'*)
            if [ ! -f "$first" ]; then
                printf failed > "$first"
                printf '%s\n' '{"method":"error","params":{"error":{"message":"Reconnecting... 2/5","additionalDetails":"stream disconnected before completion: websocket closed by server before response.completed"},"willRetry":true,"threadId":"thread-bloated","turnId":"turn-failed"}}'
                exit 0
            fi
            printf '%s\n' '{"method":"item/completed","params":{"item":{"type":"agentMessage","id":"msg-fresh","text":"Fresh fallback reply.","phase":"final_answer"},"threadId":"thread-fresh","completedAtMs":1234}}'
            printf '%s\n' '{"method":"turn/completed","params":{"threadId":"thread-fresh","turn":{"id":"turn-fresh","status":"completed","usage":{"inputTokens":18,"outputTokens":7,"totalTokens":25}}}}'
            exit 0
            ;;
    esac
done
"#,
        )
        .unwrap();
        make_executable(&script);
        (
            PathBuf::from("sh"),
            vec![script.display().to_string()],
            events,
        )
    }

    #[cfg(not(windows))]
    fn preflight_compact_problem_then_fresh_thread_app_server_command(
        root: &Path,
        scenario: &str,
    ) -> (PathBuf, Vec<String>, PathBuf) {
        let script = root.join("preflight-compact-problem.sh");
        let events = root.join("preflight-compact-problem-events.jsonl");
        fs::write(
            &script,
            r#"
scenario="$1"
[ -n "$scenario" ] || scenario="failed"
dir="$(dirname "$0")"
events="$dir/preflight-compact-problem-events.jsonl"
marker="$dir/preflight-compact-$scenario.marker"
while IFS= read -r line; do
    method="$(printf '%s' "$line" | sed -n 's/.*"method":"\([^"]*\)".*/\1/p')"
    [ -n "$method" ] && printf '%s\n' "$method" >> "$events"
    case "$line" in
        *'"id":0'*)
            printf '%s\n' '{"id":0,"result":{"ok":true}}'
            ;;
        *'"method":"thread/start"'*|*'"method":"thread/resume"'*)
            printf '%s\n' '{"id":1,"result":{"thread":{"id":"thread-test"}}}'
            ;;
        *'"method":"thread/compact/start"'*)
            printf compact-problem > "$marker"
            case "$scenario" in
                timeout)
                    sleep 10
                    exit 0
                    ;;
                unexpected)
                    printf '%s\n' '{"method":"turn/completed","params":{"threadId":"thread-test","turn":{"id":"turn-unexpected","status":"completed"}}}'
                    exit 0
                    ;;
                *)
                    printf '%s\n' '{"method":"error","params":{"error":{"message":"ContextWindowExceeded: compact failed before turn."},"threadId":"thread-test","turnId":"compact-failed"}}'
                    exit 0
                    ;;
            esac
            ;;
        *'"method":"turn/start"'*)
            printf '%s\n' '{"method":"item/completed","params":{"item":{"type":"agentMessage","id":"msg-fresh","text":"Fresh fallback reply.","phase":"final_answer"},"threadId":"thread-test","completedAtMs":1234}}'
            printf '%s\n' '{"method":"turn/completed","params":{"threadId":"thread-test","turn":{"id":"turn-fresh","status":"completed","usage":{"inputTokens":18,"outputTokens":7,"totalTokens":25}}}}'
            exit 0
            ;;
    esac
done
"#,
        )
        .unwrap();
        make_executable(&script);
        (
            PathBuf::from("sh"),
            vec![script.display().to_string(), scenario.to_string()],
            events,
        )
    }

    #[cfg(not(windows))]
    fn context_error_then_compact_success_app_server_command(
        root: &Path,
    ) -> (PathBuf, Vec<String>, PathBuf) {
        let script = root.join("context-error-then-compact-success.sh");
        let events = root.join("context-retry-events.jsonl");
        fs::write(
            &script,
            r#"
dir="$(dirname "$0")"
events="$dir/context-retry-events.jsonl"
marker="$dir/context-first-failed.marker"
while IFS= read -r line; do
    method="$(printf '%s' "$line" | sed -n 's/.*"method":"\([^"]*\)".*/\1/p')"
    [ -n "$method" ] && printf '%s\n' "$method" >> "$events"
    case "$line" in
        *'"id":0'*)
            printf '%s\n' '{"id":0,"result":{"ok":true}}'
            ;;
        *'"method":"thread/start"'*|*'"method":"thread/resume"'*)
            printf '%s\n' '{"id":1,"result":{"thread":{"id":"thread-test"}}}'
            ;;
        *'"method":"thread/compact/start"'*)
            printf '%s\n' '{"id":2,"result":{}}'
            printf '%s\n' '{"method":"item/started","params":{"item":{"type":"contextCompaction","id":"compact-1"},"threadId":"thread-test"}}'
            printf '%s\n' '{"method":"item/completed","params":{"item":{"type":"contextCompaction","id":"compact-1"},"threadId":"thread-test"}}'
            ;;
        *'"method":"turn/start"'*)
            if [ ! -f "$marker" ]; then
                printf failed > "$marker"
                printf '%s\n' '{"method":"error","params":{"error":{"message":"ContextWindowExceeded: ran out of room in the model context window."},"threadId":"thread-test","turnId":"turn-failed"}}'
                printf '%s\n' '{"method":"turn/completed","params":{"threadId":"thread-test","turn":{"id":"turn-failed","status":"failed","error":{"message":"ContextWindowExceeded: ran out of room in the model context window."}}}}'
                exit 0
            fi
            printf '%s\n' '{"method":"item/completed","params":{"item":{"type":"agentMessage","id":"msg-test","text":"Recovered after compact.","phase":"final_answer"},"threadId":"thread-test","completedAtMs":1234}}'
            printf '%s\n' '{"method":"turn/completed","params":{"threadId":"thread-test","turn":{"id":"turn-test","status":"completed","usage":{"inputTokens":20,"outputTokens":8,"totalTokens":28}}}}'
            exit 0
            ;;
    esac
done
"#,
        )
        .unwrap();
        make_executable(&script);
        (
            PathBuf::from("sh"),
            vec![script.display().to_string()],
            events,
        )
    }

    #[cfg(not(windows))]
    fn context_error_then_compact_failure_app_server_command(
        root: &Path,
    ) -> (PathBuf, Vec<String>, PathBuf) {
        let script = root.join("context-error-then-compact-failure.sh");
        let events = root.join("context-fallback-events.jsonl");
        fs::write(
            &script,
            r#"
dir="$(dirname "$0")"
events="$dir/context-fallback-events.jsonl"
first="$dir/context-first-failed.marker"
while IFS= read -r line; do
    method="$(printf '%s' "$line" | sed -n 's/.*"method":"\([^"]*\)".*/\1/p')"
    [ -n "$method" ] && printf '%s\n' "$method" >> "$events"
    case "$line" in
        *'"id":0'*)
            printf '%s\n' '{"id":0,"result":{"ok":true}}'
            ;;
        *'"method":"thread/start"'*|*'"method":"thread/resume"'*)
            printf '%s\n' '{"id":1,"result":{"thread":{"id":"thread-test"}}}'
            ;;
        *'"method":"thread/compact/start"'*)
            printf '%s\n' '{"method":"error","params":{"error":{"message":"ContextWindowExceeded: compact failed inside the model context window."},"threadId":"thread-test","turnId":"compact-failed"}}'
            exit 0
            ;;
        *'"method":"turn/start"'*)
            if [ ! -f "$first" ]; then
                printf failed > "$first"
                printf '%s\n' '{"method":"error","params":{"error":{"message":"ContextWindowExceeded: ran out of room in the model context window."},"threadId":"thread-test","turnId":"turn-failed"}}'
                printf '%s\n' '{"method":"turn/completed","params":{"threadId":"thread-test","turn":{"id":"turn-failed","status":"failed","error":{"message":"ContextWindowExceeded: ran out of room in the model context window."}}}}'
                exit 0
            fi
            printf '%s\n' '{"method":"item/completed","params":{"item":{"type":"agentMessage","id":"msg-fresh","text":"Fresh fallback reply.","phase":"final_answer"},"threadId":"thread-test","completedAtMs":1234}}'
            printf '%s\n' '{"method":"turn/completed","params":{"threadId":"thread-test","turn":{"id":"turn-fresh","status":"completed","usage":{"inputTokens":18,"outputTokens":7,"totalTokens":25}}}}'
            exit 0
            ;;
    esac
done
"#,
        )
        .unwrap();
        make_executable(&script);
        (
            PathBuf::from("sh"),
            vec![script.display().to_string()],
            events,
        )
    }

    fn write_codex_runtime_source(root: &Path) -> crate::AgentSource {
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
        crate::AgentSource::with_workspace(home, workspace)
    }

    fn write_openrouter_codex_runtime_source(root: &Path) -> crate::AgentSource {
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
                "defaults": {
                  "provider": "openrouter",
                  "model": "anthropic/claude-sonnet-4"
                },
                "list": [
                  { "id": "main", "enabled": true }
                ]
              },
              "models": {
                "providers": {
                  "openrouter": {
                    "baseUrl": "https://openrouter.ai/api/v1",
                    "apiKey": "${OPENROUTER_API_KEY}",
                    "models": ["anthropic/claude-sonnet-4"]
                  }
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
        crate::AgentSource::with_workspace(home, workspace)
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-codex-runtime-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
