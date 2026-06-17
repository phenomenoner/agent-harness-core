use std::io;
use std::path::PathBuf;

use serde::Serialize;

use crate::{
    AgentSource, ChannelOutboxPlanOptions, ChannelOutboxPlanReport, ChannelReceiveOptions,
    ChannelReceiveReport, ChannelReceiveStatus, HarnessLogEvent, HarnessLogLevel,
    InboundMediaArtifact, PromptAssemblyOptions, RuntimeRunOnceOptions, RuntimeRunOnceReport,
    append_harness_log, build_runtime_skill_index, current_log_time_ms, plan_channel_outbox,
    receive_channel_message, run_runtime_queue_once,
};

const CHANNEL_RUN_ONCE_SCHEMA: &str = "agent-harness.channel-run-once.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelRunOnceOptions {
    pub source: AgentSource,
    pub runtime_workspace: Option<PathBuf>,
    pub harness_home: PathBuf,
    pub platform: String,
    pub account_id: Option<String>,
    pub channel_id: String,
    pub user_id: String,
    pub agent_id: Option<String>,
    pub session_key: Option<String>,
    pub message: String,
    pub inbound_context: Option<String>,
    pub inbound_media_artifacts: Vec<InboundMediaArtifact>,
    pub skill_limit: usize,
    pub now_ms: i64,
    pub codex_executable: Option<PathBuf>,
    pub timeout_ms: u64,
    pub idle_timeout_ms: u64,
    pub prompt_options: PromptAssemblyOptions,
    pub outbox_limit: usize,
    pub run_runtime: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelRunOnceReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub status: ChannelRunOnceStatus,
    pub receive: ChannelReceiveReport,
    pub runtime: Option<RuntimeRunOnceReport>,
    pub outbox: ChannelOutboxPlanReport,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ChannelRunOnceStatus {
    CommandHandled,
    AgentTurnCompleted,
    AgentTurnQueued,
    ErrorReplied,
    Skipped,
}

pub fn run_channel_once(options: ChannelRunOnceOptions) -> io::Result<ChannelRunOnceReport> {
    let skill_index = build_runtime_skill_index(&options.source, &options.harness_home)?;
    let receive = receive_channel_message(ChannelReceiveOptions {
        source: options.source,
        runtime_workspace: options.runtime_workspace,
        harness_home: options.harness_home.clone(),
        skill_index,
        platform: options.platform.clone(),
        account_id: options.account_id.clone(),
        channel_id: options.channel_id,
        user_id: options.user_id,
        agent_id: options.agent_id,
        session_key: options.session_key,
        message: options.message,
        inbound_context: options.inbound_context,
        inbound_media_artifacts: options.inbound_media_artifacts,
        skill_limit: options.skill_limit,
        now_ms: options.now_ms,
    })?;
    let mut warnings = receive.warnings.clone();
    let runtime = if receive.status == ChannelReceiveStatus::AgentTurnQueued && options.run_runtime
    {
        let run = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: options.harness_home.clone(),
            queue_id: receive.queue_id.clone(),
            codex_executable: options.codex_executable,
            timeout_ms: options.timeout_ms,
            idle_timeout_ms: options.idle_timeout_ms,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(options.harness_home.clone()),
                ..options.prompt_options
            },
        })?;
        warnings.extend(run.warnings.clone());
        Some(run)
    } else {
        None
    };
    let outbox = plan_channel_outbox(ChannelOutboxPlanOptions {
        harness_home: options.harness_home.clone(),
        platform: Some(options.platform),
        limit: options.outbox_limit,
    })?;
    warnings.extend(outbox.warnings.clone());
    let status = match receive.status {
        ChannelReceiveStatus::CommandApplied => ChannelRunOnceStatus::CommandHandled,
        ChannelReceiveStatus::AgentTurnQueued => {
            if runtime.as_ref().is_some_and(|run| {
                matches!(run.receipt.status, crate::RuntimeRunOnceStatus::Completed)
            }) {
                ChannelRunOnceStatus::AgentTurnCompleted
            } else {
                ChannelRunOnceStatus::AgentTurnQueued
            }
        }
        ChannelReceiveStatus::ErrorReplied => ChannelRunOnceStatus::ErrorReplied,
        ChannelReceiveStatus::Skipped => ChannelRunOnceStatus::Skipped,
    };
    append_harness_log(
        &options.harness_home,
        &HarnessLogEvent::new(
            current_log_time_ms()?,
            match status {
                ChannelRunOnceStatus::CommandHandled
                | ChannelRunOnceStatus::AgentTurnCompleted
                | ChannelRunOnceStatus::AgentTurnQueued => HarnessLogLevel::Info,
                ChannelRunOnceStatus::ErrorReplied => HarnessLogLevel::Warn,
                ChannelRunOnceStatus::Skipped => HarnessLogLevel::Debug,
            },
            "channel",
            "channel.run-once",
            format!("channel run once finished with status {status:?}"),
        )
        .queue_id(receive.queue_id.clone())
        .session_key(Some(receive.session_key.clone()))
        .channel(
            receive.platform.clone(),
            receive.channel_id.clone(),
            receive.user_id.clone(),
        ),
    )?;

    Ok(ChannelRunOnceReport {
        schema: CHANNEL_RUN_ONCE_SCHEMA,
        harness_home: options.harness_home,
        status,
        receive,
        runtime,
        outbox,
        warnings,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AgentSource, ChannelOutboundMessageKind, ChannelRunOnceStatus, RuntimeRunOnceStatus,
    };
    use std::ffi::OsString;
    use std::fs;
    use std::path::Path;
    use std::sync::Mutex;
    use std::time::{SystemTime, UNIX_EPOCH};

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn channel_run_once_handles_command_without_runtime() {
        let root = temp_root("channel_run_once_handles_command_without_runtime");
        let source = write_channel_pipeline_source(&root);
        let harness_home = root.join(".agent-harness");

        let report = run_channel_once(ChannelRunOnceOptions {
            source,
            runtime_workspace: None,
            harness_home,
            platform: "telegram".to_string(),
            account_id: None,
            channel_id: "dm-1".to_string(),
            user_id: "user-1".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "/status".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            skill_limit: 3,
            now_ms: 1234,
            codex_executable: None,
            timeout_ms: 5_000,
            idle_timeout_ms: 5_000,
            prompt_options: PromptAssemblyOptions::default(),
            outbox_limit: 10,
            run_runtime: true,
        })
        .unwrap();

        assert_eq!(report.status, ChannelRunOnceStatus::CommandHandled);
        assert!(report.runtime.is_none());
        assert_eq!(report.outbox.pending.len(), 1);
        assert_eq!(
            report.outbox.pending[0].message.kind,
            ChannelOutboundMessageKind::CommandReply
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn channel_run_once_runs_agent_turn_and_plans_reply_delivery() {
        let _guard = ENV_LOCK.lock().unwrap();
        let root = temp_root("channel_run_once_runs_agent_turn_and_plans_reply_delivery");
        let source = write_channel_pipeline_source(&root);
        let harness_home = root.join(".agent-harness");
        let fake_codex = fake_codex_executable(&root);
        let codex_home = root.join("codex-home");
        fs::create_dir_all(&codex_home).unwrap();
        fs::write(codex_home.join("auth.json"), "{}").unwrap();
        let _env = EnvGuard::set("CODEX_HOME", codex_home.into_os_string());

        let report = run_channel_once(ChannelRunOnceOptions {
            source,
            runtime_workspace: None,
            harness_home: harness_home.clone(),
            platform: "telegram".to_string(),
            account_id: None,
            channel_id: "dm-2".to_string(),
            user_id: "user-2".to_string(),
            agent_id: Some("main".to_string()),
            session_key: None,
            message: "repair memory cron".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            skill_limit: 3,
            now_ms: 1235,
            codex_executable: Some(fake_codex),
            timeout_ms: 15_000,
            idle_timeout_ms: 15_000,
            prompt_options: PromptAssemblyOptions::default(),
            outbox_limit: 10,
            run_runtime: true,
        })
        .unwrap();

        assert_eq!(report.status, ChannelRunOnceStatus::AgentTurnCompleted);
        assert_eq!(
            report.runtime.as_ref().unwrap().receipt.status,
            RuntimeRunOnceStatus::Completed
        );
        assert_eq!(report.outbox.pending.len(), 1);
        assert_eq!(
            report.outbox.pending[0].message.kind,
            ChannelOutboundMessageKind::AgentReply
        );
        assert_eq!(report.outbox.pending[0].message.text, "Channel fake reply.");
        let log = fs::read_to_string(
            harness_home
                .join("state")
                .join("logs")
                .join("harness.jsonl"),
        )
        .unwrap();
        assert!(log.contains("channel.run-once"));

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
        let script = root.join("fake-channel-app-server.ps1");
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
        [Console]::Out.WriteLine('{"id":1,"result":{"thread":{"id":"thread-channel"}}}')
        [Console]::Out.Flush()
    } elseif ($msg.method -eq 'turn/start') {
        [Console]::Out.WriteLine('{"method":"item/agentMessage/delta","params":{"delta":"Channel fake reply."}}')
        [Console]::Out.WriteLine('{"method":"turn/completed","params":{"turn":{"id":"turn-channel","status":"completed"}}}')
        [Console]::Out.Flush()
        break
    }
}
"#,
        )
        .unwrap();
        let cmd = root.join("fake-channel-codex.cmd");
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
        let script = root.join("fake-channel-codex");
        fs::write(
            &script,
            r#"#!/bin/sh
while IFS= read -r line; do
    case "$line" in
        *'"id":0'*)
            printf '%s\n' '{"id":0,"result":{"ok":true}}'
            ;;
        *'"method":"thread/start"'*)
            printf '%s\n' '{"id":1,"result":{"thread":{"id":"thread-channel"}}}'
            ;;
        *'"method":"turn/start"'*)
            printf '%s\n' '{"method":"item/agentMessage/delta","params":{"delta":"Channel fake reply."}}'
            printf '%s\n' '{"method":"turn/completed","params":{"turn":{"id":"turn-channel","status":"completed"}}}'
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

    fn write_channel_pipeline_source(root: &Path) -> AgentSource {
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
        AgentSource::with_workspace(home, workspace)
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-channel-pipeline-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
