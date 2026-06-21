use std::fs;
use std::io;
use std::path::PathBuf;

use serde::Serialize;

use crate::{HarnessStatusOptions, collect_harness_status};

const HEALTHZ_SCHEMA: &str = "agent-harness.healthz.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthzOptions {
    pub harness_home: PathBuf,
    pub now_ms: i64,
    pub loop_stale_ms: i64,
    pub require_writable_state: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthzReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub ready: bool,
    pub live: bool,
    pub readiness_ready: bool,
    pub queue: HealthzQueue,
    pub outbox: HealthzOutbox,
    pub loops: Vec<HealthzLoop>,
    pub state: HealthzState,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthzQueue {
    pub queued: usize,
    pub open: usize,
    pub prepared: usize,
    pub completed: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthzOutbox {
    pub pending: usize,
    pub retryable: usize,
    pub delivered: usize,
    pub invalid: usize,
    pub sampled: bool,
    pub sampled_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthzLoop {
    pub name: String,
    pub present: bool,
    pub corrupt: bool,
    pub parse_error: Option<String>,
    pub stale: bool,
    pub age_ms: Option<i64>,
    pub status: Option<String>,
    pub process_id: Option<i64>,
    pub process_alive: Option<bool>,
    pub stop_file_present: bool,
    pub stop_file_reason: Option<String>,
    pub stop_file_service_id: Option<String>,
    pub stop_file_created_by: Option<String>,
    pub stop_file_created_at_ms: Option<i64>,
    pub stop_file_expires_at_ms: Option<i64>,
    pub stop_file_persistent: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthzState {
    pub state_dir: PathBuf,
    pub writable: bool,
    pub disk_free_bytes: Option<u64>,
}

pub fn collect_healthz(options: HealthzOptions) -> io::Result<HealthzReport> {
    let status = collect_harness_status(HarnessStatusOptions {
        harness_home: options.harness_home.clone(),
    })?;
    let mut warnings = status.warnings.clone();
    let state = state_health(&options)?;
    if options.require_writable_state && !state.writable {
        warnings.push("state directory is not writable".to_string());
    }
    let loops: Vec<_> = status
        .loops
        .heartbeats
        .iter()
        .map(|loop_status| {
            let unhealthy_status = loop_status.status.as_deref().is_some_and(|status| {
                status == "stopped"
                    || status == "closed"
                    || status.contains("error")
                    || status.contains("fail")
            });
            let stale = !loop_status.present
                || loop_status.corrupt
                || loop_status
                    .age_ms
                    .is_some_and(|age_ms| age_ms > options.loop_stale_ms)
                || unhealthy_status
                || loop_status.process_alive == Some(false)
                || loop_status.stop_file_present;
            HealthzLoop {
                name: loop_status.name.clone(),
                present: loop_status.present,
                corrupt: loop_status.corrupt,
                parse_error: loop_status.parse_error.clone(),
                stale,
                age_ms: loop_status.age_ms,
                status: loop_status.status.clone(),
                process_id: loop_status.process_id,
                process_alive: loop_status.process_alive,
                stop_file_present: loop_status.stop_file_present,
                stop_file_reason: loop_status.stop_file_reason.clone(),
                stop_file_service_id: loop_status.stop_file_service_id.clone(),
                stop_file_created_by: loop_status.stop_file_created_by.clone(),
                stop_file_created_at_ms: loop_status.stop_file_created_at_ms,
                stop_file_expires_at_ms: loop_status.stop_file_expires_at_ms,
                stop_file_persistent: loop_status.stop_file_persistent,
            }
        })
        .collect();
    let runtime_loop_unhealthy = loops.iter().any(|item| {
        item.name == "runtime-loop"
            && (item.stale
                || item
                    .status
                    .as_deref()
                    .is_some_and(|status| matches!(status, "error" | "stopped" | "stopping")))
    });
    if loops
        .iter()
        .any(|item| item.name == "runtime-loop" && item.status.as_deref() == Some("safe-mode"))
    {
        warnings.push("runtime-loop is in safe-mode with reduced concurrency".to_string());
    }
    if runtime_loop_unhealthy {
        warnings.push(
            "runtime-loop is not ready; channel ingress may queue without replies".to_string(),
        );
    }
    if loops
        .iter()
        .any(|item| item.name == "progress-delivery-loop" && item.stale)
    {
        warnings.push(
            "progress-delivery-loop is degraded; final reply delivery health is evaluated separately"
                .to_string(),
        );
    }
    let live = loops
        .iter()
        .filter(|item| item.name != "progress-delivery-loop")
        .all(|item| !item.stale)
        && !runtime_loop_unhealthy;
    let ready = status.ready && live && (!options.require_writable_state || state.writable);
    Ok(HealthzReport {
        schema: HEALTHZ_SCHEMA,
        harness_home: options.harness_home,
        ready,
        live,
        readiness_ready: status.ready,
        queue: HealthzQueue {
            queued: status.runtime.queued_items,
            open: status.runtime.open_items,
            prepared: status.runtime.prepared_items,
            completed: status.runtime.completed_items,
        },
        outbox: HealthzOutbox {
            pending: status.channels.outbox.all.pending,
            retryable: status.channels.outbox.all.failed_retryable,
            delivered: status.channels.outbox.all.delivered,
            invalid: status.channels.outbox.all.invalid_lines,
            sampled: status.channels.outbox.all.sampled,
            sampled_bytes: status.channels.outbox.all.sampled_bytes,
        },
        loops,
        state,
        warnings,
    })
}

fn state_health(options: &HealthzOptions) -> io::Result<HealthzState> {
    let state_dir = options.harness_home.join("state");
    fs::create_dir_all(&state_dir)?;
    let probe = state_dir.join(".healthz-write-probe.tmp");
    let writable = match fs::write(&probe, format!("{}\n", options.now_ms)) {
        Ok(()) => {
            let _ = fs::remove_file(&probe);
            true
        }
        Err(_) => false,
    };
    Ok(HealthzState {
        state_dir,
        writable,
        disk_free_bytes: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::write_json_atomic;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn healthz_reports_stale_loop_and_queue_depth() {
        let root = temp_root("healthz_reports_stale_loop_and_queue_depth");
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(harness_home.join("state").join("runtime-queue")).unwrap();
        fs::create_dir_all(
            harness_home
                .join("state")
                .join("supervisor")
                .join("loop-heartbeats"),
        )
        .unwrap();
        fs::write(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("pending.jsonl"),
            r#"{"queueId":"q-1","status":"queued"}"#,
        )
        .unwrap();
        write_json_atomic(
            &harness_home
                .join("state")
                .join("supervisor")
                .join("loop-heartbeats")
                .join("runtime-loop.json"),
            &serde_json::json!({
                "status": "running",
                "iteration": 7,
                "processId": 123,
                "atMs": 1_000
            }),
        )
        .unwrap();

        let report = collect_healthz(HealthzOptions {
            harness_home: harness_home.clone(),
            now_ms: 10_000,
            loop_stale_ms: 1_000,
            require_writable_state: true,
        })
        .unwrap();

        assert!(!report.ready);
        assert!(!report.live);
        assert_eq!(report.queue.queued, 1);
        assert!(report.state.writable);
        assert!(
            report
                .loops
                .iter()
                .any(|item| item.name == "runtime-loop" && item.stale)
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn healthz_reports_error_loop_as_not_live_even_when_fresh() {
        let root = temp_root("healthz_reports_error_loop_as_not_live_even_when_fresh");
        let harness_home = root.join(".agent-harness");
        let state = harness_home.join("state");
        fs::create_dir_all(state.join("runtime-queue")).unwrap();
        fs::create_dir_all(state.join("channels")).unwrap();
        fs::create_dir_all(state.join("logs")).unwrap();
        fs::create_dir_all(state.join("plugin-sidecar")).unwrap();
        fs::create_dir_all(state.join("memory")).unwrap();
        fs::create_dir_all(state.join("supervisor").join("loop-heartbeats")).unwrap();
        write_json_atomic(
            &state
                .join("supervisor")
                .join("loop-heartbeats")
                .join("runtime-loop.json"),
            &serde_json::json!({
                "status": "error",
                "iteration": 3,
                "processId": 42,
                "atMs": 10_000,
                "detail": "consecutiveErrors=5/5"
            }),
        )
        .unwrap();

        let report = collect_healthz(HealthzOptions {
            harness_home,
            now_ms: 10_500,
            loop_stale_ms: 120_000,
            require_writable_state: false,
        })
        .unwrap();

        assert!(!report.live);
        assert!(!report.ready);
        assert!(
            report
                .loops
                .iter()
                .any(|item| item.name == "runtime-loop" && item.stale)
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn healthz_reports_corrupt_heartbeat_loop_as_not_live() {
        let root = temp_root("healthz_reports_corrupt_heartbeat_loop_as_not_live");
        let harness_home = root.join(".agent-harness");
        let state = harness_home.join("state");
        fs::create_dir_all(state.join("runtime-queue")).unwrap();
        fs::create_dir_all(state.join("channels")).unwrap();
        fs::create_dir_all(state.join("logs")).unwrap();
        fs::create_dir_all(state.join("plugin-sidecar")).unwrap();
        fs::create_dir_all(state.join("memory")).unwrap();
        let heartbeat_dir = state.join("supervisor").join("loop-heartbeats");
        fs::create_dir_all(&heartbeat_dir).unwrap();
        fs::write(heartbeat_dir.join("runtime-loop.json"), b"\0\0\0").unwrap();

        let report = collect_healthz(HealthzOptions {
            harness_home,
            now_ms: 10_500,
            loop_stale_ms: 120_000,
            require_writable_state: false,
        })
        .unwrap();

        assert!(!report.live);
        assert!(!report.ready);
        let runtime_loop = report
            .loops
            .iter()
            .find(|item| item.name == "runtime-loop")
            .unwrap();
        assert!(runtime_loop.present);
        assert!(runtime_loop.corrupt);
        assert!(runtime_loop.parse_error.is_some());
        assert!(runtime_loop.stale);
        assert!(report.warnings.iter().any(
            |warning| warning.contains("runtime-loop heartbeat") && warning.contains("corrupt")
        ));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn healthz_keeps_live_when_only_progress_delivery_is_stale() {
        let root = temp_root("healthz_keeps_live_when_only_progress_delivery_is_stale");
        let harness_home = root.join(".agent-harness");
        let state = harness_home.join("state");
        fs::create_dir_all(state.join("runtime-queue")).unwrap();
        fs::create_dir_all(state.join("channels")).unwrap();
        fs::create_dir_all(state.join("logs")).unwrap();
        fs::create_dir_all(state.join("plugin-sidecar")).unwrap();
        fs::create_dir_all(state.join("memory")).unwrap();
        let heartbeat_dir = state.join("supervisor").join("loop-heartbeats");
        fs::create_dir_all(&heartbeat_dir).unwrap();
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        for name in [
            "runtime-loop",
            "telegram-loop",
            "discord-outbox-loop",
            "discord-gateway-loop",
            "worker-loop",
            "cron-scheduler-loop",
        ] {
            write_json_atomic(
                &heartbeat_dir.join(format!("{name}.json")),
                &serde_json::json!({
                    "status": "no-work",
                    "iteration": 3,
                    "processId": std::process::id(),
                    "atMs": now_ms,
                    "detail": "idle"
                }),
            )
            .unwrap();
        }
        write_json_atomic(
            &heartbeat_dir.join("progress-delivery-loop.json"),
            &serde_json::json!({
                "status": "running",
                "iteration": 3,
                "processId": std::process::id(),
                "atMs": now_ms - 600_000,
                "detail": "stale progress telemetry"
            }),
        )
        .unwrap();

        let report = collect_healthz(HealthzOptions {
            harness_home,
            now_ms,
            loop_stale_ms: 120_000,
            require_writable_state: false,
        })
        .unwrap();

        assert!(report.live);
        let progress_loop = report
            .loops
            .iter()
            .find(|item| item.name == "progress-delivery-loop")
            .unwrap();
        assert!(progress_loop.stale);
        assert!(
            report
                .warnings
                .iter()
                .any(|warning| warning.contains("progress-delivery-loop is degraded"))
        );

        let _ = fs::remove_dir_all(root);
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-healthz-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
