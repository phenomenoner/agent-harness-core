use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::Value;

use crate::{HarnessStatusOptions, collect_harness_status};

const HARNESS_METRICS_SCHEMA: &str = "agent-harness.metrics.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarnessMetricsOptions {
    pub harness_home: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessMetricsReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub counters: BTreeMap<String, i64>,
    pub runtime_status_counts: BTreeMap<String, usize>,
    pub warnings: Vec<String>,
}

pub fn collect_harness_metrics(options: HarnessMetricsOptions) -> io::Result<HarnessMetricsReport> {
    let status = collect_harness_status(HarnessStatusOptions {
        harness_home: options.harness_home.clone(),
    })?;
    let queue_dir = options.harness_home.join("state").join("runtime-queue");
    let mut counters = BTreeMap::new();
    counters.insert(
        "runtime.queue.queued".to_string(),
        status.runtime.queued_items as i64,
    );
    counters.insert(
        "runtime.queue.open".to_string(),
        status.runtime.open_items as i64,
    );
    counters.insert(
        "runtime.queue.prepared".to_string(),
        status.runtime.prepared_items as i64,
    );
    counters.insert(
        "runtime.queue.completed".to_string(),
        status.runtime.completed_items as i64,
    );
    counters.insert(
        "channels.outbox.pending".to_string(),
        status.channels.outbox.all.pending as i64,
    );
    counters.insert(
        "channels.outbox.delivered".to_string(),
        status.channels.outbox.all.delivered as i64,
    );
    counters.insert(
        "channels.outbox.retryable".to_string(),
        status.channels.outbox.all.failed_retryable as i64,
    );
    counters.insert(
        "supervisor.loops.stale".to_string(),
        status
            .loops
            .heartbeats
            .iter()
            .filter(|heartbeat| {
                !heartbeat.present || heartbeat.age_ms.is_some_and(|age| age > 120_000)
            })
            .count() as i64,
    );
    let runtime_status_counts = count_statuses(&queue_dir.join("run-once-receipts.jsonl"))?;
    Ok(HarnessMetricsReport {
        schema: HARNESS_METRICS_SCHEMA,
        harness_home: options.harness_home,
        counters,
        runtime_status_counts,
        warnings: status.warnings,
    })
}

fn count_statuses(path: &Path) -> io::Result<BTreeMap<String, usize>> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(error) => return Err(error),
    };
    let mut counts = BTreeMap::new();
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
            continue;
        };
        if let Some(status) = value.get("status").and_then(Value::as_str) {
            *counts.entry(status.to_string()).or_insert(0) += 1;
        }
    }
    Ok(counts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn metrics_counts_runtime_statuses_and_depths() {
        let root = temp_root("metrics_counts_runtime_statuses_and_depths");
        let harness_home = root.join(".agent-harness");
        let queue = harness_home.join("state").join("runtime-queue");
        fs::create_dir_all(&queue).unwrap();
        fs::write(
            queue.join("pending.jsonl"),
            r#"{"queueId":"q-1","status":"queued"}"#,
        )
        .unwrap();
        fs::write(
            queue.join("run-once-receipts.jsonl"),
            "{\"queueId\":\"q-1\",\"status\":\"retry-pending\"}\n{\"queueId\":\"q-2\",\"status\":\"dead-letter\"}\n",
        )
        .unwrap();

        let report = collect_harness_metrics(HarnessMetricsOptions { harness_home }).unwrap();

        assert_eq!(report.counters["runtime.queue.queued"], 1);
        assert_eq!(report.runtime_status_counts["retry-pending"], 1);
        assert_eq!(report.runtime_status_counts["dead-letter"], 1);

        let _ = fs::remove_dir_all(root);
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-metrics-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
