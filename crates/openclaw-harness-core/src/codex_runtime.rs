use std::env;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::Value;

const CODEX_RUNTIME_PLAN_SCHEMA: &str = "openclaw-harness.codex-runtime-plan.v1";
const CODEX_RUNTIME_PREFLIGHT_SCHEMA: &str = "openclaw-harness.codex-runtime-preflight.v1";
const CODEX_RUNTIME_LAUNCH_PROBE_SCHEMA: &str = "openclaw-harness.codex-runtime-launch-probe.v1";

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
    pub prompt_input_file: PathBuf,
    pub env_requirements: Vec<CodexEnvRequirement>,
    pub model_argument: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CodexTransportPlan {
    StdioJsonRpcAppServer,
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

pub fn plan_codex_runtime(options: CodexRuntimePlanOptions) -> io::Result<CodexRuntimePlanReport> {
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
    let executable = options
        .codex_executable
        .unwrap_or_else(|| PathBuf::from("codex"));
    let invocation = CodexInvocationPlan {
        executable,
        transport: CodexTransportPlan::StdioJsonRpcAppServer,
        arguments: vec!["app-server".to_string()],
        working_directory: execution_dir.clone(),
        prompt_input_file: prompt_markdown.clone(),
        env_requirements: env_requirements(provider.as_deref()),
        model_argument: model.clone(),
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
    fs::create_dir_all(&plan.invocation.working_directory)?;
    let stdout_log = plan
        .invocation
        .working_directory
        .join("codex-runtime-launch.stdout.log");
    let stderr_log = plan
        .invocation
        .working_directory
        .join("codex-runtime-launch.stderr.log");
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
    let probe = parent.join(".openclaw-harness-preflight.tmp");
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
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    let line = serde_json::to_string(value).map_err(io::Error::other)?;
    writeln!(file, "{line}")?;
    Ok(())
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
    fn plan_codex_runtime_writes_plan_and_receipts() {
        let root = temp_root("plan_codex_runtime_writes_plan_and_receipts");
        let source = write_codex_runtime_source(&root);
        let harness_home = root.join(".openclaw-harness");
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
        assert_eq!(
            plan.invocation.executable,
            PathBuf::from("custom-codex.exe")
        );
        assert_eq!(
            plan.invocation.transport,
            CodexTransportPlan::StdioJsonRpcAppServer
        );
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
    fn plan_codex_runtime_reports_no_prepared_execution() {
        let root = temp_root("plan_codex_runtime_reports_no_prepared_execution");
        let harness_home = root.join(".openclaw-harness");

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
    fn preflight_codex_runtime_reports_ready_when_local_gates_pass() {
        let root = temp_root("preflight_codex_runtime_reports_ready_when_local_gates_pass");
        let source = write_codex_runtime_source(&root);
        let harness_home = root.join(".openclaw-harness");
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
        let harness_home = root.join(".openclaw-harness");
        enqueue_and_prepare(&source, &harness_home);
        let plan_report = plan_codex_runtime(CodexRuntimePlanOptions {
            harness_home: harness_home.clone(),
            execution_dir: None,
            codex_executable: Some(std::env::current_exe().unwrap()),
        })
        .unwrap();
        let missing_env = format!("OPENCLAW_HARNESS_TEST_MISSING_ENV_{}", std::process::id());
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
        let harness_home = root.join(".openclaw-harness");

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
        let harness_home = root.join(".openclaw-harness");
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
        let harness_home = root.join(".openclaw-harness");
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
                    "name": format!("OPENCLAW_HARNESS_TEST_MISSING_ENV_{}", std::process::id()),
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
        let harness_home = root.join(".openclaw-harness");

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

    fn enqueue_and_prepare(source: &crate::OpenClawSource, harness_home: &Path) {
        let registry = load_agent_registry(source).unwrap();
        let skills = build_source_skill_index(source).unwrap();
        let turn = build_turn_plan(
            source,
            &registry,
            &skills,
            TurnPlanInput {
                platform: "telegram".to_string(),
                channel_id: "dm-42".to_string(),
                user_id: "user-7".to_string(),
                text: "repair memory cron".to_string(),
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

    fn write_codex_runtime_source(root: &Path) -> crate::OpenClawSource {
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
        crate::OpenClawSource::with_workspace(home, workspace)
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "openclaw-harness-codex-runtime-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
