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
    HarnessLogEvent, HarnessLogLevel, append_agent_progress_event, append_harness_log,
    config::{
        HarnessConfigValidationReport, HarnessConfigValidationStatus, validate_harness_config,
    },
    current_log_time_ms, write_json_atomic,
};

const CODEX_RUNTIME_PLAN_SCHEMA: &str = "agent-harness.codex-runtime-plan.v1";
const CODEX_RUNTIME_PREFLIGHT_SCHEMA: &str = "agent-harness.codex-runtime-preflight.v1";
const CODEX_RUNTIME_LAUNCH_PROBE_SCHEMA: &str = "agent-harness.codex-runtime-launch-probe.v1";
const CODEX_RUNTIME_RUN_SCHEMA: &str = "agent-harness.codex-runtime-run.v1";
const CODEX_RUNTIME_COMPLETION_SCHEMA: &str = "agent-harness.codex-runtime-completion.v1";
const CODEX_TRANSCRIPT_MESSAGE_SCHEMA: &str = "agent-harness.transcript-message.v1";
const CODEX_TRAJECTORY_EVENT_SCHEMA: &str = "agent-harness.trajectory-event.v1";
const CODEX_BINDING_SCHEMA: &str = "agent-harness.codex-binding.v1";
const CODEX_APP_SERVER_DEVELOPER_INSTRUCTIONS: &str = "\
This Codex app-server thread backs an imported agent harness session. Codex owns \
the backend system prompt, tool schemas, MCP/tool inventory, sandbox, approvals, \
and thread continuity. The chat-facing agent identity and operating context come \
from the Agent prompt bundle passed as turn input. Do not treat the Rust harness \
development repository as the chat user's agent identity.";
const HARNESS_CONFIG_FILE_NAME: &str = "harness-config.json";
pub(crate) const CODEX_APPROVAL_POLICY_ENV: &str = "AGENT_HARNESS_CODEX_APPROVAL_POLICY";
pub(crate) const CODEX_SANDBOX_ENV: &str = "AGENT_HARNESS_CODEX_SANDBOX";
pub(crate) const CODEX_SANDBOX_POLICY_ENV: &str = "AGENT_HARNESS_CODEX_SANDBOX_POLICY";
const DEFAULT_CODEX_SANDBOX: &str = "elevated";
const DEFAULT_CODEX_SANDBOX_POLICY: &str = "workspaceWrite";
const RUNTIME_CANCEL_REQUEST_MAX_AGE_MS: i64 = 60_000;
const CODEX_CHILD_TERMINATE_TIMEOUT_MS: u64 = 2_000;
const CODEX_STDOUT_READER_JOIN_TIMEOUT_MS: u64 = 2_000;

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
            max_chars: 500,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexRuntimeUsage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub total_tokens: Option<u64>,
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CodexRuntimeRunStatus {
    Completed,
    PreflightBlocked,
    NoRuntimePlan,
    SpawnFailed,
    ProtocolError,
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
    let app_server_approval_policy = codex_app_server_approval_policy(approval_policy).to_string();
    let app_server_sandbox = resolve_codex_sandbox_policy(&options.harness_home, &mut warnings);
    let provider_config = codex_provider_config(provider.as_deref());
    let codex_home = harness_codex_home(&options.harness_home, provider_config.is_some());
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
    checks.extend(check_env_requirements(&plan.invocation.env_requirements));
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
    let probe_result = spawn_launch_probe(&plan, options.startup_probe_ms)?;
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
    let run_result = drive_codex_app_server(
        &options.harness_home,
        &plan,
        options.timeout_ms,
        options.idle_timeout_ms,
        options.progress_context,
        &narration_config,
    )?;
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
    };
    let log_level = match receipt.status {
        CodexRuntimeRunStatus::Completed => HarnessLogLevel::Info,
        CodexRuntimeRunStatus::Timeout | CodexRuntimeRunStatus::ProtocolError => {
            HarnessLogLevel::Error
        }
        CodexRuntimeRunStatus::Canceled => HarnessLogLevel::Warn,
        CodexRuntimeRunStatus::SpawnFailed
        | CodexRuntimeRunStatus::PreflightBlocked
        | CodexRuntimeRunStatus::NoRuntimePlan => HarnessLogLevel::Warn,
    };
    let log_event = match receipt.status {
        CodexRuntimeRunStatus::Completed => "codex.run.completed",
        CodexRuntimeRunStatus::Timeout => "codex.run.timeout",
        CodexRuntimeRunStatus::ProtocolError => "codex.run.protocol-error",
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
    let mut thread_id = plan.invocation.thread_id.clone();

    for line in reader.lines() {
        let line = line?;
        event_count += 1;
        match serde_json::from_str::<Value>(&line) {
            Ok(value) => {
                if let Some(extracted_thread_id) = extract_thread_id(&value) {
                    thread_id = Some(extracted_thread_id);
                }
                collect_agent_output(&value, &mut state, &mut progress, narration_config);
                if is_turn_completed(&value) {
                    record_turn_usage(&value, &mut state);
                    completed = true;
                }
            }
            Err(error) => state.warnings.push(format!(
                "stdout recovery skipped non-JSON Codex app-server line: {error}"
            )),
        }
    }

    if !completed {
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

fn app_server_approval_policy_for_plan(plan: &CodexRuntimePlanFile) -> String {
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
    apply_codex_home_env(&mut command, plan);

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
    let app_server_approval_policy = app_server_approval_policy_for_plan(plan);
    let app_server_sandbox = app_server_sandbox_for_plan(plan);
    let runtime_workspace_root = plan
        .invocation
        .working_directory
        .to_string_lossy()
        .to_string();
    if let Some(model) = &plan.model {
        thread_params["model"] = json!(model);
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
                status: CodexRuntimeRunStatus::ProtocolError,
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
                warnings: state.warnings,
            });
        }
    };
    let prompt_input = fs::read_to_string(&plan.invocation.prompt_input_file)?;
    let turn_sandbox_policy =
        app_server_sandbox_policy_value(&app_server_sandbox, &runtime_workspace_root);
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
        "input": [
            {
                "type": "text",
                "text": prompt_input
            }
        ]
    });
    write_json_rpc(
        &mut stdin,
        &json!({
            "id": 2,
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
        warnings: state.warnings,
    })
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
                    Ok(value) => Ok(ProtocolEvent::Json(value)),
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
    let event = AgentProgressEvent::new(
        &emitter.context,
        AgentProgressKind::AssistantNarration,
        "assistant_narration",
        preview,
        AgentProgressStatus::Progress,
        at_ms,
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

fn record_turn_usage(value: &Value, state: &mut CodexProtocolState) {
    if let Some(usage) = extract_turn_usage(value) {
        state.usage = Some(usage);
    }
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
    let error = value.get("error")?;
    if let Some(text) = error.as_str() {
        return Some(text.to_string());
    }
    if let Some(message) = error.get("message").and_then(Value::as_str) {
        return Some(message.to_string());
    }
    Some(error.to_string())
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
    apply_codex_home_env(&mut command, plan);

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
            wire_api: "chat".to_string(),
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

fn check_env_requirements(requirements: &[CodexEnvRequirement]) -> Vec<CodexRuntimePreflightCheck> {
    if requirements.is_empty() {
        return vec![pass_check(
            "environment",
            "runtime plan declares no required environment variables",
        )];
    }
    requirements
        .iter()
        .flat_map(check_credential_requirement)
        .collect()
}

fn check_credential_requirement(
    requirement: &CodexEnvRequirement,
) -> Vec<CodexRuntimePreflightCheck> {
    if requirement.name == "OPENAI_API_KEY" {
        return check_openai_or_codex_oauth_requirement(requirement);
    }
    vec![check_env_requirement(requirement)]
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

fn check_env_requirement(requirement: &CodexEnvRequirement) -> CodexRuntimePreflightCheck {
    if env::var_os(&requirement.name).is_some() {
        pass_check(
            format!("env:{}", requirement.name),
            format!("{} is present", requirement.name),
        )
    } else {
        fail_check(
            format!("env:{}", requirement.name),
            format!("{} is missing: {}", requirement.name, requirement.reason),
        )
    }
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
    harness_home
        .join("agents")
        .join(agent_id.unwrap_or("unknown"))
        .join("sessions")
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

fn append_json_line(path: &Path, value: &impl Serialize) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    let line = serde_json::to_string(value).map_err(io::Error::other)?;
    writeln!(file, "{line}")?;
    Ok(())
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

fn harness_codex_home(harness_home: &Path, force_generated_config: bool) -> Option<PathBuf> {
    let codex_home = absolute_for_config(harness_home).join("codex-home");
    if force_generated_config
        || [codex_home.join("auth.json"), codex_home.join("auth.toml")]
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
    let config_file = codex_home.join("config.toml");
    if config_file.is_file() {
        let existing = fs::read_to_string(&config_file)?;
        if existing.starts_with("# Generated by agent-harness.") {
            let desired = harness_codex_config_toml(
                working_directory,
                harness_home,
                &sandbox,
                provider_config,
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
    let config =
        harness_codex_config_toml(working_directory, harness_home, &sandbox, provider_config);
    fs::write(&config_file, config)?;
    warnings.push(format!(
        "created harness-local Codex config at {} with Windows sandbox={sandbox:?} and trusted runtime workspace",
        config_file.display(),
    ));
    Ok(())
}

fn harness_codex_config_toml(
    working_directory: &Path,
    harness_home: &Path,
    sandbox: &str,
    provider_config: Option<&CodexProviderConfig>,
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

fn absolute_for_config(path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
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
        PromptAssemblyOptions, RuntimeQueueEnqueueOptions, RuntimeQueuePrepareOptions,
        TurnPlanInput, build_channel_step, build_source_skill_index, build_turn_plan,
        enqueue_channel_step, load_agent_registry, prepare_runtime_queue_item,
    };
    use std::time::{SystemTime, UNIX_EPOCH};

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
        assert_eq!(provider_config.wire_api, "chat");

        let codex_home = plan.invocation.codex_home.as_ref().unwrap();
        let config = fs::read_to_string(codex_home.join("config.toml")).unwrap();
        assert!(config.contains("model_provider = \"openrouter\""));
        assert!(config.contains("[model_providers.openrouter]"));
        assert!(config.contains("base_url = \"https://openrouter.ai/api/v1\""));
        assert!(config.contains("env_key = \"OPENROUTER_API_KEY\""));
        assert!(!config.contains("sk-"));

        let _ = fs::remove_dir_all(root);
    }

    fn progress_context() -> AgentProgressContext {
        AgentProgressContext {
            queue_id: "queue-1".to_string(),
            agent_id: Some("main".to_string()),
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
        assert_eq!(plan.invocation.app_server_approval_policy, "never");
        assert_eq!(plan.invocation.app_server_sandbox, "dangerFullAccess");

        let plan_file = report.plan_file.unwrap();
        let plan_json: Value = read_json_file(&plan_file).unwrap();
        assert_eq!(
            plan_json["invocation"]["approvalPolicy"],
            serde_json::json!("accept")
        );
        assert_eq!(
            plan_json["invocation"]["appServerApprovalPolicy"],
            serde_json::json!("never")
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

        let resolved = harness_codex_home(&harness_home, false).unwrap();

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
            timeout_ms: 5_000,
            idle_timeout_ms: 5_000,
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
            timeout_ms: 5_000,
            idle_timeout_ms: 5_000,
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
            timeout_ms: 5_000,
            idle_timeout_ms: 5_000,
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
            timeout_ms: 5_000,
            idle_timeout_ms: 5_000,
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
            timeout_ms: 5_000,
            idle_timeout_ms: 5_000,
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

    fn enqueue_and_prepare_with_runtime_workspace(
        source: &crate::AgentSource,
        harness_home: &Path,
        runtime_workspace: Option<PathBuf>,
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
                now_ms: 1234,
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
