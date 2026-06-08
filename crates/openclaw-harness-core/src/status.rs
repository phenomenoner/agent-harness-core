use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::Value;

use crate::{
    ActivationReadinessOptions, ActivationReadinessReport, ActivationReadinessStatus,
    ChannelOutboxPlanOptions, ChannelOutboxPlanSummary, check_activation_readiness,
    harness_log_file, plan_channel_outbox,
};

const HARNESS_STATUS_SCHEMA: &str = "openclaw-harness.status.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarnessStatusOptions {
    pub harness_home: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessStatusReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub ready: bool,
    pub readiness: ActivationReadinessReport,
    pub runtime: HarnessRuntimeStatus,
    pub channels: HarnessChannelStatus,
    pub memory: HarnessMemoryStatus,
    pub plugins: HarnessPluginStatus,
    pub logs: HarnessOperationalLogStatus,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessRuntimeStatus {
    pub pending_file: PathBuf,
    pub queued_items: usize,
    pub prepared_items: usize,
    pub completed_items: usize,
    pub open_items: usize,
    pub pending_invalid_lines: usize,
    pub execution_receipts: HarnessJsonlStatus,
    pub run_once_receipts: HarnessJsonlStatus,
    pub codex_plan_receipts: HarnessJsonlStatus,
    pub codex_run_receipts: HarnessJsonlStatus,
    pub codex_completion_receipts: HarnessJsonlStatus,
    pub codex_launch_receipts: HarnessJsonlStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessChannelStatus {
    pub outbox: HarnessOutboxStatus,
    pub telegram_offset_file: PathBuf,
    pub telegram_offset_present: bool,
    pub telegram_poll_log_present: bool,
    pub discord_send_log_present: bool,
    pub discord_event_log_present: bool,
    pub discord_gateway_probe: HarnessJsonlStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessOutboxStatus {
    pub all: ChannelOutboxPlanSummary,
    pub telegram: ChannelOutboxPlanSummary,
    pub discord: ChannelOutboxPlanSummary,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessMemoryStatus {
    pub memory_dir: PathBuf,
    pub exists: bool,
    pub qdrant_edge: bool,
    pub lancedb: bool,
    pub openclaw_mem_sqlite: bool,
    pub regular_files: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessPluginStatus {
    pub catalog_file: PathBuf,
    pub catalog_present: bool,
    pub catalog_tools: usize,
    pub sidecar_execution_receipts: HarnessJsonlStatus,
    pub sidecar_probe_receipts: HarnessJsonlStatus,
    pub sidecar_bridge_receipts: HarnessJsonlStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessOperationalLogStatus {
    pub log_file: PathBuf,
    pub exists: bool,
    pub lines: usize,
    pub invalid_lines: usize,
    pub latest_event: Option<String>,
    pub event_present: BTreeMap<String, bool>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessJsonlStatus {
    pub path: PathBuf,
    pub exists: bool,
    pub lines: usize,
    pub invalid_lines: usize,
    pub latest_status: Option<String>,
    pub latest_method: Option<String>,
    pub latest_reason: Option<String>,
}

pub fn collect_harness_status(options: HarnessStatusOptions) -> io::Result<HarnessStatusReport> {
    let readiness = check_activation_readiness(ActivationReadinessOptions {
        harness_home: options.harness_home.clone(),
    })?;
    let mut warnings = Vec::new();
    let channels = channel_status(&options.harness_home, &readiness, &mut warnings)?;
    let logs = log_status(&options.harness_home)?;
    let runtime = runtime_status(&options.harness_home)?;
    let memory = memory_status(&options.harness_home)?;
    let plugins = plugin_status(&options.harness_home)?;

    Ok(HarnessStatusReport {
        schema: HARNESS_STATUS_SCHEMA,
        harness_home: options.harness_home,
        ready: readiness.ready,
        readiness,
        runtime,
        channels,
        memory,
        plugins,
        logs,
        warnings,
    })
}

fn runtime_status(harness_home: &Path) -> io::Result<HarnessRuntimeStatus> {
    let queue_dir = harness_home.join("state").join("runtime-queue");
    let pending_file = queue_dir.join("pending.jsonl");
    let pending = read_queue_ids(&pending_file)?;
    let prepared_ids = read_receipt_queue_ids_with_status(
        &queue_dir.join("execution-receipts.jsonl"),
        &["prepared", "already-prepared"],
    )?;
    let completed_ids = read_receipt_queue_ids_with_status(
        &queue_dir.join("codex-runtime-completion-receipts.jsonl"),
        &["recorded", "already-recorded"],
    )?;
    let open_items = pending.queue_ids.difference(&completed_ids).count();
    Ok(HarnessRuntimeStatus {
        pending_file,
        queued_items: pending.queue_ids.len(),
        prepared_items: pending.queue_ids.intersection(&prepared_ids).count(),
        completed_items: pending.queue_ids.intersection(&completed_ids).count(),
        open_items,
        pending_invalid_lines: pending.invalid_lines,
        execution_receipts: jsonl_status(queue_dir.join("execution-receipts.jsonl"))?,
        run_once_receipts: jsonl_status(queue_dir.join("run-once-receipts.jsonl"))?,
        codex_plan_receipts: jsonl_status(queue_dir.join("codex-runtime-receipts.jsonl"))?,
        codex_run_receipts: jsonl_status(queue_dir.join("codex-runtime-run-receipts.jsonl"))?,
        codex_completion_receipts: jsonl_status(
            queue_dir.join("codex-runtime-completion-receipts.jsonl"),
        )?,
        codex_launch_receipts: jsonl_status(queue_dir.join("codex-runtime-launch-receipts.jsonl"))?,
    })
}

fn channel_status(
    harness_home: &Path,
    readiness: &ActivationReadinessReport,
    warnings: &mut Vec<String>,
) -> io::Result<HarnessChannelStatus> {
    let all = plan_channel_outbox(ChannelOutboxPlanOptions {
        harness_home: harness_home.to_path_buf(),
        platform: None,
        limit: usize::MAX,
    })?;
    warnings.extend(all.warnings.clone());
    let telegram = plan_channel_outbox(ChannelOutboxPlanOptions {
        harness_home: harness_home.to_path_buf(),
        platform: Some("telegram".to_string()),
        limit: usize::MAX,
    })?;
    warnings.extend(telegram.warnings.clone());
    let discord = plan_channel_outbox(ChannelOutboxPlanOptions {
        harness_home: harness_home.to_path_buf(),
        platform: Some("discord".to_string()),
        limit: usize::MAX,
    })?;
    warnings.extend(discord.warnings.clone());
    let telegram_offset_file = harness_home
        .join("state")
        .join("channels")
        .join("telegram-offset.json");
    let channel_dir = harness_home.join("state").join("channels");

    Ok(HarnessChannelStatus {
        outbox: HarnessOutboxStatus {
            all: all.summary,
            telegram: telegram.summary,
            discord: discord.summary,
        },
        telegram_offset_present: telegram_offset_file.is_file(),
        telegram_offset_file,
        telegram_poll_log_present: readiness_check_passed(readiness, "telegram-poll-log"),
        discord_send_log_present: readiness_check_passed(readiness, "discord-send-log"),
        discord_event_log_present: readiness_check_passed(readiness, "discord-event-log"),
        discord_gateway_probe: jsonl_status(
            channel_dir.join("discord-gateway-probe-receipts.jsonl"),
        )?,
    })
}

fn memory_status(harness_home: &Path) -> io::Result<HarnessMemoryStatus> {
    let memory_dir = harness_home.join("memory");
    Ok(HarnessMemoryStatus {
        exists: memory_dir.is_dir(),
        qdrant_edge: memory_dir.join("qdrant-edge").is_dir(),
        lancedb: memory_dir.join("lancedb").is_dir(),
        openclaw_mem_sqlite: memory_dir.join("openclaw-mem.sqlite").is_file(),
        regular_files: count_regular_files(&memory_dir)?,
        memory_dir,
    })
}

fn plugin_status(harness_home: &Path) -> io::Result<HarnessPluginStatus> {
    let sidecar_dir = harness_home.join("state").join("plugin-sidecar");
    let catalog_file = sidecar_dir.join("catalog.json");
    let catalog_tools = match fs::read_to_string(&catalog_file) {
        Ok(text) => serde_json::from_str::<Value>(&text)
            .ok()
            .and_then(|value| value.get("tools").and_then(Value::as_array).map(Vec::len))
            .unwrap_or(0),
        Err(error) if error.kind() == io::ErrorKind::NotFound => 0,
        Err(error) => return Err(error),
    };
    Ok(HarnessPluginStatus {
        catalog_present: catalog_file.is_file(),
        catalog_tools,
        catalog_file,
        sidecar_execution_receipts: jsonl_status(sidecar_dir.join("execution-receipts.jsonl"))?,
        sidecar_probe_receipts: jsonl_status(sidecar_dir.join("probe-receipts.jsonl"))?,
        sidecar_bridge_receipts: jsonl_status(sidecar_dir.join("bridge-receipts.jsonl"))?,
    })
}

fn log_status(harness_home: &Path) -> io::Result<HarnessOperationalLogStatus> {
    const EVENTS: &[&str] = &[
        "activation.enable-check",
        "telegram.poll-once",
        "discord.outbox-send-once",
        "discord.event-run-once",
        "channel.receive",
        "runtime.run-once.completed",
        "codex.run.completed",
        "codex.complete.recorded",
        "channel.delivery.delivered",
    ];

    let log_file = harness_log_file(harness_home);
    let mut lines = 0;
    let mut invalid_lines = 0;
    let mut latest_event = None;
    let mut event_present = EVENTS
        .iter()
        .map(|event| ((*event).to_string(), false))
        .collect::<BTreeMap<_, _>>();

    match fs::read_to_string(&log_file) {
        Ok(text) => {
            for line in text.lines() {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                match serde_json::from_str::<Value>(trimmed) {
                    Ok(value) => {
                        lines += 1;
                        if let Some(event) = value.get("event").and_then(Value::as_str) {
                            latest_event = Some(event.to_string());
                            if let Some(present) = event_present.get_mut(event) {
                                *present = true;
                            }
                        }
                    }
                    Err(_) => invalid_lines += 1,
                }
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }

    Ok(HarnessOperationalLogStatus {
        exists: log_file.is_file(),
        log_file,
        lines,
        invalid_lines,
        latest_event,
        event_present,
    })
}

fn jsonl_status(path: PathBuf) -> io::Result<HarnessJsonlStatus> {
    let mut status = HarnessJsonlStatus {
        path,
        ..HarnessJsonlStatus::default()
    };
    let text = match fs::read_to_string(&status.path) {
        Ok(text) => {
            status.exists = true;
            text
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(status),
        Err(error) => return Err(error),
    };
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        match serde_json::from_str::<Value>(trimmed) {
            Ok(value) => {
                status.lines += 1;
                status.latest_status = string_path(&value, &["status"]);
                status.latest_method = string_path(&value, &["method"]);
                status.latest_reason = string_path(&value, &["reason"]);
            }
            Err(_) => status.invalid_lines += 1,
        }
    }
    Ok(status)
}

#[derive(Default)]
struct QueueIdRead {
    queue_ids: BTreeSet<String>,
    invalid_lines: usize,
}

fn read_queue_ids(path: &Path) -> io::Result<QueueIdRead> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(QueueIdRead::default()),
        Err(error) => return Err(error),
    };
    let mut read = QueueIdRead::default();
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        match serde_json::from_str::<Value>(trimmed) {
            Ok(value) => {
                if let Some(queue_id) = queue_id_from_value(&value) {
                    read.queue_ids.insert(queue_id);
                } else {
                    read.invalid_lines += 1;
                }
            }
            Err(_) => read.invalid_lines += 1,
        }
    }
    Ok(read)
}

fn read_receipt_queue_ids_with_status(
    path: &Path,
    statuses: &[&str],
) -> io::Result<BTreeSet<String>> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(BTreeSet::new()),
        Err(error) => return Err(error),
    };
    let mut queue_ids = BTreeSet::new();
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
            continue;
        };
        let status = value.get("status").and_then(Value::as_str);
        if status.is_some_and(|status| statuses.contains(&status))
            && let Some(queue_id) = queue_id_from_value(&value)
        {
            queue_ids.insert(queue_id);
        }
    }
    Ok(queue_ids)
}

fn queue_id_from_value(value: &Value) -> Option<String> {
    value
        .get("queueId")
        .or_else(|| value.get("queue_id"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

fn count_regular_files(root: &Path) -> io::Result<usize> {
    if !root.exists() {
        return Ok(0);
    }
    let mut count = 0;
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            count += count_regular_files(&entry.path())?;
        } else if file_type.is_file() {
            count += 1;
        }
    }
    Ok(count)
}

fn readiness_check_passed(readiness: &ActivationReadinessReport, name: &str) -> bool {
    readiness
        .checks
        .iter()
        .any(|check| check.name == name && check.status == ActivationReadinessStatus::Pass)
}

fn string_path(value: &Value, path: &[&str]) -> Option<String> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    current.as_str().map(ToString::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn collect_status_summarizes_runtime_channel_and_logs() {
        let root = temp_root("collect_status_summarizes_runtime_channel_and_logs");
        let harness_home = root.join(".openclaw-harness");
        fs::create_dir_all(harness_home.join("state").join("runtime-queue")).unwrap();
        fs::create_dir_all(harness_home.join("state").join("channels")).unwrap();
        fs::create_dir_all(harness_home.join("state").join("logs")).unwrap();
        fs::create_dir_all(harness_home.join("state").join("plugin-sidecar")).unwrap();
        fs::create_dir_all(harness_home.join("memory").join("qdrant-edge")).unwrap();
        fs::write(
            harness_home.join("state").join("harness-registry.json"),
            r#"{
              "agents": [{"id":"main","enabled":true}],
              "providers": [],
              "channels": {"telegram": false, "discord": false},
              "plugins": []
            }"#,
        )
        .unwrap();
        fs::write(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("pending.jsonl"),
            r#"{"queueId":"q1"}"#,
        )
        .unwrap();
        fs::write(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("run-once-receipts.jsonl"),
            r#"{"status":"completed","reason":"ok"}"#,
        )
        .unwrap();
        fs::write(
            harness_home.join("state").join("channels").join("outbox.jsonl"),
            r#"{"kind":"command-reply","platform":"telegram","channelId":"dm","userId":"user","sessionKey":"telegram:dm:user:main","text":"ok"}"#,
        )
        .unwrap();
        fs::write(
            harness_home
                .join("state")
                .join("logs")
                .join("harness.jsonl"),
            r#"{"event":"channel.receive"}"#,
        )
        .unwrap();

        let report = collect_harness_status(HarnessStatusOptions {
            harness_home: harness_home.clone(),
        })
        .unwrap();

        assert_eq!(report.runtime.queued_items, 1);
        assert_eq!(report.runtime.open_items, 1);
        assert_eq!(
            report.runtime.run_once_receipts.latest_status.as_deref(),
            Some("completed")
        );
        assert_eq!(report.channels.outbox.all.pending, 1);
        assert!(report.memory.qdrant_edge);
        assert_eq!(
            report.logs.event_present.get("channel.receive").copied(),
            Some(true)
        );

        let _ = fs::remove_dir_all(root);
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "openclaw-harness-status-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
