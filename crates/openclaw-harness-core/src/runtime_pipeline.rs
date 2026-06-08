use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::Value;

use crate::{
    ChannelOutboundMessage, ChannelOutboundMessageKind, CodexRuntimePlanOptions,
    CodexRuntimePlanReport, CodexRuntimeReceiptStatus, CodexRuntimeRunOptions,
    CodexRuntimeRunReport, CodexRuntimeRunStatus, HarnessLogEvent, HarnessLogLevel,
    PromptAssemblyOptions, RuntimeExecutionReceiptStatus, RuntimeQueuePrepareOptions,
    RuntimeQueuePrepareReport, RuntimeQueuePreparedItem, append_harness_log, current_log_time_ms,
    plan_codex_runtime, prepare_runtime_queue_item, run_codex_runtime,
};

const RUNTIME_RUN_ONCE_SCHEMA: &str = "openclaw-harness.runtime-run-once.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeRunOnceOptions {
    pub harness_home: PathBuf,
    pub queue_id: Option<String>,
    pub codex_executable: Option<PathBuf>,
    pub timeout_ms: u64,
    pub prompt_options: PromptAssemblyOptions,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeRunOnceReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub report_file: PathBuf,
    pub receipts_file: PathBuf,
    pub receipt: RuntimeRunOnceReceipt,
    pub prepare: Option<RuntimeQueuePrepareReport>,
    pub plan: Option<CodexRuntimePlanReport>,
    pub run: Option<CodexRuntimeRunReport>,
    pub outbox_file: Option<PathBuf>,
    pub outbound_message: Option<ChannelOutboundMessage>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeRunOnceReceipt {
    pub queue_id: Option<String>,
    pub status: RuntimeRunOnceStatus,
    pub execution_dir: Option<PathBuf>,
    pub transcript_file: Option<PathBuf>,
    pub outbox_file: Option<PathBuf>,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum RuntimeRunOnceStatus {
    Completed,
    NoWork,
    NoPreparedExecution,
    NoRuntimePlan,
    PreflightBlocked,
    SpawnFailed,
    ProtocolError,
    Timeout,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct QueueChannelContext {
    platform: String,
    channel_id: String,
    user_id: String,
    session_key: String,
}

pub fn run_runtime_queue_once(options: RuntimeRunOnceOptions) -> io::Result<RuntimeRunOnceReport> {
    let queue_dir = options.harness_home.join("state").join("runtime-queue");
    let report_file = queue_dir.join("run-once-last.json");
    let receipts_file = queue_dir.join("run-once-receipts.jsonl");
    fs::create_dir_all(&queue_dir)?;

    let prepare = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
        harness_home: options.harness_home.clone(),
        queue_id: options.queue_id.clone(),
        prompt_options: options.prompt_options,
    })?;
    let mut warnings = prepare.warnings.clone();
    let mut channel_context = prepare
        .item
        .as_ref()
        .map(channel_context_from_prepared_item);

    if prepare.receipt.status == RuntimeExecutionReceiptStatus::NoPendingItem {
        let requested_queue = options.queue_id;
        let requested_specific_queue = requested_queue.is_some();
        let receipt = RuntimeRunOnceReceipt {
            queue_id: requested_queue,
            status: RuntimeRunOnceStatus::NoWork,
            execution_dir: None,
            transcript_file: None,
            outbox_file: None,
            reason: if requested_specific_queue {
                "requested queue item was not pending or prepared".to_string()
            } else {
                "no pending or prepared runtime queue item is available".to_string()
            },
        };
        append_runtime_run_once_log(
            &options.harness_home,
            HarnessLogLevel::Warn,
            "runtime.run-once.no-work",
            &receipt,
        )?;
        return write_runtime_run_once_report(
            RuntimeRunOnceReport {
                schema: RUNTIME_RUN_ONCE_SCHEMA,
                harness_home: options.harness_home,
                report_file,
                receipts_file,
                receipt,
                prepare: Some(prepare),
                plan: None,
                run: None,
                outbox_file: None,
                outbound_message: None,
                warnings,
            },
            true,
        );
    }

    let plan = plan_codex_runtime(CodexRuntimePlanOptions {
        harness_home: options.harness_home.clone(),
        execution_dir: prepare.receipt.execution_dir.clone(),
        codex_executable: options.codex_executable,
    })?;
    warnings.extend(plan.warnings.clone());

    if plan.receipt.status == CodexRuntimeReceiptStatus::NoPreparedExecution {
        let receipt = RuntimeRunOnceReceipt {
            queue_id: prepare.receipt.queue_id.clone(),
            status: RuntimeRunOnceStatus::NoPreparedExecution,
            execution_dir: plan.execution_dir.clone(),
            transcript_file: None,
            outbox_file: None,
            reason: "no prepared runtime execution is available to run".to_string(),
        };
        append_runtime_run_once_log(
            &options.harness_home,
            HarnessLogLevel::Warn,
            "runtime.run-once.no-prepared-execution",
            &receipt,
        )?;
        return write_runtime_run_once_report(
            RuntimeRunOnceReport {
                schema: RUNTIME_RUN_ONCE_SCHEMA,
                harness_home: options.harness_home,
                report_file,
                receipts_file,
                receipt,
                prepare: Some(prepare),
                plan: Some(plan),
                run: None,
                outbox_file: None,
                outbound_message: None,
                warnings,
            },
            true,
        );
    }

    if channel_context.is_none()
        && let Some(queue_id) = plan.receipt.queue_id.as_deref()
    {
        channel_context =
            find_queue_channel_context(&options.harness_home, queue_id, &mut warnings)?;
    }

    let run = run_codex_runtime(CodexRuntimeRunOptions {
        harness_home: options.harness_home.clone(),
        execution_dir: plan.execution_dir.clone(),
        plan_file: plan.plan_file.clone(),
        timeout_ms: options.timeout_ms,
    })?;
    warnings.extend(run.warnings.clone());

    let mut outbox_file = None;
    let mut outbound_message = None;
    if run.receipt.status == CodexRuntimeRunStatus::Completed {
        if run.receipt.event_count == 0 && run.receipt.reason.contains("already recorded") {
            warnings.push(
                "codex-run reported an already recorded completion; outbox write skipped to avoid duplicate delivery"
                    .to_string(),
            );
        } else if let Some(context) = channel_context {
            if let Some(transcript_file) = run.receipt.transcript_file.as_ref() {
                match latest_assistant_message(transcript_file)? {
                    Some(text) => {
                        let message = ChannelOutboundMessage {
                            platform: context.platform,
                            channel_id: context.channel_id,
                            user_id: context.user_id,
                            session_key: context.session_key,
                            kind: ChannelOutboundMessageKind::AgentReply,
                            text,
                        };
                        let file = append_outbound_message(&options.harness_home, &message)?;
                        outbox_file = Some(file);
                        outbound_message = Some(message);
                    }
                    None => warnings.push(format!(
                        "no assistant message found in transcript {}",
                        transcript_file.display()
                    )),
                }
            }
        } else {
            warnings.push(
                "queue channel context was unavailable; assistant reply was not written to outbox"
                    .to_string(),
            );
        }
    }

    let receipt = RuntimeRunOnceReceipt {
        queue_id: run.receipt.queue_id.clone(),
        status: map_run_once_status(run.receipt.status),
        execution_dir: run.receipt.execution_dir.clone(),
        transcript_file: run.receipt.transcript_file.clone(),
        outbox_file: outbox_file.clone(),
        reason: run.receipt.reason.clone(),
    };
    let log_level = match receipt.status {
        RuntimeRunOnceStatus::Completed => HarnessLogLevel::Info,
        RuntimeRunOnceStatus::Timeout
        | RuntimeRunOnceStatus::ProtocolError
        | RuntimeRunOnceStatus::SpawnFailed => HarnessLogLevel::Error,
        RuntimeRunOnceStatus::NoWork
        | RuntimeRunOnceStatus::NoPreparedExecution
        | RuntimeRunOnceStatus::NoRuntimePlan
        | RuntimeRunOnceStatus::PreflightBlocked => HarnessLogLevel::Warn,
    };
    let log_event = match receipt.status {
        RuntimeRunOnceStatus::Completed => "runtime.run-once.completed",
        RuntimeRunOnceStatus::NoWork => "runtime.run-once.no-work",
        RuntimeRunOnceStatus::NoPreparedExecution => "runtime.run-once.no-prepared-execution",
        RuntimeRunOnceStatus::NoRuntimePlan => "runtime.run-once.no-runtime-plan",
        RuntimeRunOnceStatus::PreflightBlocked => "runtime.run-once.preflight-blocked",
        RuntimeRunOnceStatus::SpawnFailed => "runtime.run-once.spawn-failed",
        RuntimeRunOnceStatus::ProtocolError => "runtime.run-once.protocol-error",
        RuntimeRunOnceStatus::Timeout => "runtime.run-once.timeout",
    };
    append_runtime_run_once_log(&options.harness_home, log_level, log_event, &receipt)?;

    write_runtime_run_once_report(
        RuntimeRunOnceReport {
            schema: RUNTIME_RUN_ONCE_SCHEMA,
            harness_home: options.harness_home,
            report_file,
            receipts_file,
            receipt,
            prepare: Some(prepare),
            plan: Some(plan),
            run: Some(run),
            outbox_file,
            outbound_message,
            warnings,
        },
        true,
    )
}

fn map_run_once_status(status: CodexRuntimeRunStatus) -> RuntimeRunOnceStatus {
    match status {
        CodexRuntimeRunStatus::Completed => RuntimeRunOnceStatus::Completed,
        CodexRuntimeRunStatus::PreflightBlocked => RuntimeRunOnceStatus::PreflightBlocked,
        CodexRuntimeRunStatus::NoRuntimePlan => RuntimeRunOnceStatus::NoRuntimePlan,
        CodexRuntimeRunStatus::SpawnFailed => RuntimeRunOnceStatus::SpawnFailed,
        CodexRuntimeRunStatus::ProtocolError => RuntimeRunOnceStatus::ProtocolError,
        CodexRuntimeRunStatus::Timeout => RuntimeRunOnceStatus::Timeout,
    }
}

fn write_runtime_run_once_report(
    report: RuntimeRunOnceReport,
    append_receipt: bool,
) -> io::Result<RuntimeRunOnceReport> {
    let report_json = serde_json::to_string_pretty(&report).map_err(io::Error::other)?;
    fs::write(&report.report_file, report_json)?;
    if append_receipt {
        append_json_line(&report.receipts_file, &report.receipt)?;
    }
    Ok(report)
}

fn append_runtime_run_once_log(
    harness_home: &Path,
    level: HarnessLogLevel,
    event: &'static str,
    receipt: &RuntimeRunOnceReceipt,
) -> io::Result<()> {
    append_harness_log(
        harness_home,
        &HarnessLogEvent::new(
            current_log_time_ms()?,
            level,
            "runtime-queue",
            event,
            receipt.reason.clone(),
        )
        .queue_id(receipt.queue_id.clone())
        .path(receipt.execution_dir.clone()),
    )
    .map(|_| ())
}

fn channel_context_from_prepared_item(item: &RuntimeQueuePreparedItem) -> QueueChannelContext {
    QueueChannelContext {
        platform: item.platform.clone(),
        channel_id: item.channel_id.clone(),
        user_id: item.user_id.clone(),
        session_key: item.session_key.clone(),
    }
}

fn find_queue_channel_context(
    harness_home: &Path,
    queue_id: &str,
    warnings: &mut Vec<String>,
) -> io::Result<Option<QueueChannelContext>> {
    let queue_file = harness_home
        .join("state")
        .join("runtime-queue")
        .join("pending.jsonl");
    if !queue_file.is_file() {
        warnings.push(format!(
            "runtime queue file not found while resolving channel context: {}",
            queue_file.display()
        ));
        return Ok(None);
    }
    let text = fs::read_to_string(&queue_file)?;
    for (index, line) in text.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let value: Value = match serde_json::from_str(trimmed) {
            Ok(value) => value,
            Err(error) => {
                warnings.push(format!(
                    "runtime queue line {} is not valid JSON while resolving channel context: {}",
                    index + 1,
                    error
                ));
                continue;
            }
        };
        if string_field(&value, &["queueId", "queue_id"]) != Some(queue_id) {
            continue;
        }
        return Ok(Some(QueueChannelContext {
            platform: string_field(&value, &["platform"])
                .unwrap_or("unknown")
                .to_string(),
            channel_id: string_field(&value, &["channelId", "channel_id"])
                .unwrap_or("unknown")
                .to_string(),
            user_id: string_field(&value, &["userId", "user_id"])
                .unwrap_or("unknown")
                .to_string(),
            session_key: string_field(&value, &["sessionKey", "session_key"])
                .unwrap_or("unknown")
                .to_string(),
        }));
    }
    warnings.push(format!(
        "runtime queue item `{queue_id}` was not found while resolving channel context"
    ));
    Ok(None)
}

fn latest_assistant_message(transcript_file: &Path) -> io::Result<Option<String>> {
    let text = fs::read_to_string(transcript_file)?;
    for line in text.lines().rev() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let value: Value = match serde_json::from_str(trimmed) {
            Ok(value) => value,
            Err(_) => continue,
        };
        if string_field(&value, &["role"]) == Some("assistant")
            && let Some(content) = string_field(&value, &["content"])
        {
            return Ok(Some(content.to_string()));
        }
    }
    Ok(None)
}

fn append_outbound_message(
    harness_home: &Path,
    message: &ChannelOutboundMessage,
) -> io::Result<PathBuf> {
    let outbox_file = harness_home
        .join("state")
        .join("channels")
        .join("outbox.jsonl");
    append_json_line(&outbox_file, message)?;
    Ok(outbox_file)
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

fn string_field<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a str> {
    for key in keys {
        if let Some(text) = value.get(*key).and_then(Value::as_str) {
            return Some(text);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ChannelReceiveOptions, ChannelReceiveStatus, OpenClawSource, build_source_skill_index,
        receive_channel_message,
    };
    use std::ffi::OsString;
    use std::sync::Mutex;
    use std::time::{SystemTime, UNIX_EPOCH};

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn run_runtime_queue_once_records_agent_reply_outbox() {
        let _guard = ENV_LOCK.lock().unwrap();
        let root = temp_root("run_runtime_queue_once_records_agent_reply_outbox");
        let source = write_pipeline_source(&root);
        let harness_home = root.join(".openclaw-harness");
        let skills = build_source_skill_index(&source).unwrap();
        let receive = receive_channel_message(ChannelReceiveOptions {
            source,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "telegram".to_string(),
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "repair memory cron".to_string(),
            skill_limit: 3,
            now_ms: 1234,
        })
        .unwrap();
        assert_eq!(receive.status, ChannelReceiveStatus::AgentTurnQueued);
        let fake_codex = fake_codex_executable(&root);
        let codex_home = root.join("codex-home");
        fs::create_dir_all(&codex_home).unwrap();
        fs::write(codex_home.join("auth.json"), "{}").unwrap();
        let _env = EnvGuard::set("CODEX_HOME", codex_home.into_os_string());

        let report = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            codex_executable: Some(fake_codex),
            timeout_ms: 5_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();

        assert_eq!(report.receipt.status, RuntimeRunOnceStatus::Completed);
        assert_eq!(
            report.prepare.as_ref().unwrap().receipt.status,
            RuntimeExecutionReceiptStatus::Prepared
        );
        assert!(
            report
                .plan
                .as_ref()
                .unwrap()
                .plan_file
                .as_ref()
                .unwrap()
                .is_file()
        );
        assert_eq!(
            report.run.as_ref().unwrap().receipt.status,
            CodexRuntimeRunStatus::Completed
        );
        let outbound = report.outbound_message.unwrap();
        assert_eq!(outbound.kind, ChannelOutboundMessageKind::AgentReply);
        assert_eq!(outbound.platform, "telegram");
        assert_eq!(outbound.channel_id, "dm-42");
        assert_eq!(outbound.user_id, "user-7");
        assert_eq!(outbound.text, "Pipeline fake reply.");
        let outbox_file = report.outbox_file.unwrap();
        let outbox = fs::read_to_string(outbox_file).unwrap();
        assert!(outbox.contains("\"kind\":\"agent-reply\""));
        assert!(outbox.contains("Pipeline fake reply."));
        let log = fs::read_to_string(
            harness_home
                .join("state")
                .join("logs")
                .join("harness.jsonl"),
        )
        .unwrap();
        assert!(log.contains("runtime.run-once.completed"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn run_runtime_queue_once_stops_when_only_terminal_queue_items_remain() {
        let _guard = ENV_LOCK.lock().unwrap();
        let root = temp_root("no_work_after_terminal");
        let source = write_pipeline_source(&root);
        let harness_home = root.join(".openclaw-harness");
        let skills = build_source_skill_index(&source).unwrap();
        let receive = receive_channel_message(ChannelReceiveOptions {
            source,
            harness_home: harness_home.clone(),
            skill_index: skills,
            platform: "telegram".to_string(),
            channel_id: "dm-42".to_string(),
            user_id: "user-7".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "repair memory cron".to_string(),
            skill_limit: 3,
            now_ms: 1234,
        })
        .unwrap();
        assert_eq!(receive.status, ChannelReceiveStatus::AgentTurnQueued);
        let fake_codex = fake_codex_executable(&root);
        let codex_home = root.join("codex-home");
        fs::create_dir_all(&codex_home).unwrap();
        fs::write(codex_home.join("auth.json"), "{}").unwrap();
        let _env = EnvGuard::set("CODEX_HOME", codex_home.into_os_string());

        let first = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            codex_executable: Some(fake_codex.clone()),
            timeout_ms: 5_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();
        assert_eq!(first.receipt.status, RuntimeRunOnceStatus::Completed);
        assert!(first.outbox_file.is_some());

        let second = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            codex_executable: Some(fake_codex),
            timeout_ms: 5_000,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        })
        .unwrap();

        assert_eq!(second.receipt.status, RuntimeRunOnceStatus::NoWork);
        assert_eq!(
            second.prepare.as_ref().unwrap().receipt.status,
            RuntimeExecutionReceiptStatus::NoPendingItem
        );
        assert!(second.plan.is_none());
        assert!(second.run.is_none());
        assert!(second.outbound_message.is_none());
        let outbox = fs::read_to_string(
            harness_home
                .join("state")
                .join("channels")
                .join("outbox.jsonl"),
        )
        .unwrap();
        assert_eq!(outbox.lines().count(), 1);

        let _ = fs::remove_dir_all(root);
    }

    struct EnvGuard {
        key: &'static str,
        old: Option<OsString>,
    }

    impl EnvGuard {
        fn set(key: &'static str, value: OsString) -> Self {
            let old = std::env::var_os(key);
            unsafe {
                std::env::set_var(key, value);
            }
            Self { key, old }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            unsafe {
                match &self.old {
                    Some(value) => std::env::set_var(self.key, value),
                    None => std::env::remove_var(self.key),
                }
            }
        }
    }

    #[cfg(windows)]
    fn fake_codex_executable(root: &Path) -> PathBuf {
        let script = root.join("fake-app-server.ps1");
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
        [Console]::Out.WriteLine('{"id":1,"result":{"thread":{"id":"thread-pipeline"}}}')
        [Console]::Out.Flush()
    } elseif ($msg.method -eq 'turn/start') {
        [Console]::Out.WriteLine('{"method":"item/agentMessage/delta","params":{"delta":"Pipeline fake reply."}}')
        [Console]::Out.WriteLine('{"method":"turn/completed","params":{"turn":{"id":"turn-pipeline","status":"completed"}}}')
        [Console]::Out.Flush()
        break
    }
}
"#,
        )
        .unwrap();
        let cmd = root.join("fake-codex.cmd");
        fs::write(
            &cmd,
            format!(
                "@echo off\r\npowershell -NoProfile -ExecutionPolicy Bypass -File \"{}\"\r\n",
                script.display()
            ),
        )
        .unwrap();
        cmd
    }

    #[cfg(not(windows))]
    fn fake_codex_executable(root: &Path) -> PathBuf {
        let script = root.join("fake-codex");
        fs::write(
            &script,
            r#"#!/bin/sh
while IFS= read -r line; do
    case "$line" in
        *'"id":0'*)
            printf '%s\n' '{"id":0,"result":{"ok":true}}'
            ;;
        *'"method":"thread/start"'*)
            printf '%s\n' '{"id":1,"result":{"thread":{"id":"thread-pipeline"}}}'
            ;;
        *'"method":"turn/start"'*)
            printf '%s\n' '{"method":"item/agentMessage/delta","params":{"delta":"Pipeline fake reply."}}'
            printf '%s\n' '{"method":"turn/completed","params":{"turn":{"id":"turn-pipeline","status":"completed"}}}'
            exit 0
            ;;
    esac
done
"#,
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = fs::metadata(&script).unwrap().permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&script, permissions).unwrap();
        }
        script
    }

    fn write_pipeline_source(root: &Path) -> OpenClawSource {
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
        OpenClawSource::with_workspace(home, workspace)
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "openclaw-harness-runtime-pipeline-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
