use std::collections::BTreeMap;
use std::io;
use std::path::PathBuf;

use serde::Serialize;

use crate::runtime_receipt_history::read_runtime_queue_receipt_history_status_counts;
use crate::runtime_worker::{
    refresh_runtime_queue_state_index, runtime_queue_status_counts_from_index,
};
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
    let mut runtime_status_counts = read_runtime_queue_receipt_history_status_counts(&queue_dir)?;
    let hot_index = refresh_runtime_queue_state_index(&queue_dir, &mut Vec::new())?;
    merge_status_counts(
        &mut runtime_status_counts,
        runtime_queue_status_counts_from_index(&hot_index),
    );
    Ok(HarnessMetricsReport {
        schema: HARNESS_METRICS_SCHEMA,
        harness_home: options.harness_home,
        counters,
        runtime_status_counts,
        warnings: status.warnings,
    })
}

fn merge_status_counts(into: &mut BTreeMap<String, usize>, additional: BTreeMap<String, usize>) {
    for (status, count) in additional {
        let current = into.entry(status).or_default();
        *current = current.saturating_add(count);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::fs;
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

    #[test]
    fn metrics_combines_committed_history_status_deltas_with_the_hot_ledger() {
        let root =
            temp_root("metrics_combines_committed_history_status_deltas_with_the_hot_ledger");
        let harness_home = root.join(".agent-harness");
        let queue = harness_home.join("state").join("runtime-queue");
        fs::create_dir_all(&queue).unwrap();
        let staged = crate::runtime_receipt_history::stage_runtime_queue_receipt_history(
            &queue,
            "metrics-history",
            br#"{"queueId":"queue-history","status":"lease-busy"}
{"queueId":"queue-history","status":"completed","reason":"historical completion"}
"#,
            b"",
            &HashSet::new(),
            100,
        )
        .unwrap();
        crate::runtime_receipt_history::commit_runtime_queue_receipt_history(&staged, 101).unwrap();
        fs::write(
            queue.join("pending.jsonl"),
            r#"{"queueId":"queue-live","status":"queued"}
"#,
        )
        .unwrap();
        fs::write(
            queue.join("run-once-receipts.jsonl"),
            r#"{"queueId":"queue-live","status":"retry-pending"}
"#,
        )
        .unwrap();

        let report = collect_harness_metrics(HarnessMetricsOptions { harness_home }).unwrap();

        assert_eq!(report.runtime_status_counts["lease-busy"], 1);
        assert_eq!(report.runtime_status_counts["completed"], 1);
        assert_eq!(report.runtime_status_counts["retry-pending"], 1);

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
