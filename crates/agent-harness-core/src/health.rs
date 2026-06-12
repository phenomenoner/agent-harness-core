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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthzLoop {
    pub name: String,
    pub present: bool,
    pub stale: bool,
    pub age_ms: Option<i64>,
    pub status: Option<String>,
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
            let stale = !loop_status.present
                || loop_status
                    .age_ms
                    .is_some_and(|age_ms| age_ms > options.loop_stale_ms);
            HealthzLoop {
                name: loop_status.name.clone(),
                present: loop_status.present,
                stale,
                age_ms: loop_status.age_ms,
                status: loop_status.status.clone(),
            }
        })
        .collect();
    let live = loops.iter().all(|item| !item.stale);
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
