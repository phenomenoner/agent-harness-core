use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::Value;

const CODEX_RUNTIME_PLAN_SCHEMA: &str = "openclaw-harness.codex-runtime-plan.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexRuntimePlanOptions {
    pub harness_home: PathBuf,
    pub execution_dir: Option<PathBuf>,
    pub codex_executable: Option<PathBuf>,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CodexTransportPlan {
    StdioJsonRpcAppServer,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexEnvRequirement {
    pub name: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexOutputPlan {
    pub transcript_file: PathBuf,
    pub trajectory_file: PathBuf,
    pub codex_binding_file: PathBuf,
    pub runtime_receipt_file: PathBuf,
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

fn env_requirements(provider: Option<&str>) -> Vec<CodexEnvRequirement> {
    match provider.map(str::to_ascii_lowercase).as_deref() {
        Some(provider) if provider.contains("openrouter") => vec![CodexEnvRequirement {
            name: "OPENROUTER_API_KEY".to_string(),
            reason: "queued agent turn uses an OpenRouter/OpenAI-compatible provider".to_string(),
        }],
        _ => vec![CodexEnvRequirement {
            name: "OPENAI_API_KEY".to_string(),
            reason: "Codex/OpenAI app-server execution requires OpenAI credentials".to_string(),
        }],
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
