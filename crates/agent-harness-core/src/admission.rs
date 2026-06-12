use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use serde::Serialize;

const ADMISSION_DECISION_SCHEMA: &str = "agent-harness.admission-decision.v1";
const SCOPED_STOP_SCHEMA: &str = "agent-harness.scoped-stop.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionDecisionOptions {
    pub harness_home: PathBuf,
    pub queue_open_items: usize,
    pub queue_depth_limit: usize,
    pub provider_backpressure: bool,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdmissionDecisionReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub receipt_file: PathBuf,
    pub accepted: bool,
    pub reason: String,
    pub queue_open_items: usize,
    pub queue_depth_limit: usize,
    pub provider_backpressure: bool,
    pub at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopedStopOptions {
    pub harness_home: PathBuf,
    pub target: ScopedStopTarget,
    pub reason: String,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScopedStopReceipt {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub receipt_file: PathBuf,
    pub marker_file: PathBuf,
    pub target: ScopedStopTarget,
    pub reason: String,
    pub at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ScopedStopTarget {
    Turn { session_key: String },
    QueueItem { queue_id: String },
    WorkerJob { job_id: String },
    AllJobs { agent_id: Option<String> },
}

pub fn evaluate_admission(
    options: AdmissionDecisionOptions,
) -> io::Result<AdmissionDecisionReport> {
    let receipt_file = options
        .harness_home
        .join("state")
        .join("admission")
        .join("admission-receipts.jsonl");
    let accepted = options.queue_depth_limit == 0
        || (options.queue_open_items < options.queue_depth_limit && !options.provider_backpressure);
    let reason = if accepted {
        "admitted".to_string()
    } else if options.provider_backpressure {
        "provider backpressure is active; refusing quickly".to_string()
    } else {
        format!(
            "queue depth {} reached configured limit {}",
            options.queue_open_items, options.queue_depth_limit
        )
    };
    let report = AdmissionDecisionReport {
        schema: ADMISSION_DECISION_SCHEMA,
        harness_home: options.harness_home,
        receipt_file: receipt_file.clone(),
        accepted,
        reason,
        queue_open_items: options.queue_open_items,
        queue_depth_limit: options.queue_depth_limit,
        provider_backpressure: options.provider_backpressure,
        at_ms: options.now_ms,
    };
    append_json_line(&receipt_file, &report)?;
    Ok(report)
}

pub fn record_scoped_stop(options: ScopedStopOptions) -> io::Result<ScopedStopReceipt> {
    let stop_dir = options
        .harness_home
        .join("state")
        .join("runtime-queue")
        .join("cancel");
    fs::create_dir_all(&stop_dir)?;
    let marker_file = stop_dir.join(scoped_stop_marker_name(&options.target));
    let receipt_file = options
        .harness_home
        .join("state")
        .join("runtime-queue")
        .join("scoped-stop-receipts.jsonl");
    let receipt = ScopedStopReceipt {
        schema: SCOPED_STOP_SCHEMA,
        harness_home: options.harness_home,
        receipt_file: receipt_file.clone(),
        marker_file: marker_file.clone(),
        target: options.target,
        reason: options.reason,
        at_ms: options.now_ms,
    };
    fs::write(
        &marker_file,
        serde_json::to_vec_pretty(&receipt).map_err(io::Error::other)?,
    )?;
    append_json_line(&receipt_file, &receipt)?;
    Ok(receipt)
}

fn scoped_stop_marker_name(target: &ScopedStopTarget) -> String {
    match target {
        ScopedStopTarget::Turn { session_key } => format!("turn-{}.stop", normalize(session_key)),
        ScopedStopTarget::QueueItem { queue_id } => format!("queue-{}.stop", normalize(queue_id)),
        ScopedStopTarget::WorkerJob { job_id } => format!("job-{}.stop", normalize(job_id)),
        ScopedStopTarget::AllJobs { agent_id } => format!(
            "jobs-{}.stop",
            agent_id
                .as_deref()
                .map(normalize)
                .unwrap_or_else(|| "all".to_string())
        ),
    }
}

fn normalize(value: &str) -> String {
    let mut out = String::new();
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        "unknown".to_string()
    } else {
        out
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn admission_refuses_queue_flood_and_scoped_stop_writes_marker() {
        let root = temp_root("admission_refuses_queue_flood_and_scoped_stop_writes_marker");
        let harness_home = root.join(".agent-harness");

        let admission = evaluate_admission(AdmissionDecisionOptions {
            harness_home: harness_home.clone(),
            queue_open_items: 10,
            queue_depth_limit: 10,
            provider_backpressure: false,
            now_ms: 123,
        })
        .unwrap();
        assert!(!admission.accepted);
        assert!(admission.reason.contains("queue depth"));

        let stop = record_scoped_stop(ScopedStopOptions {
            harness_home,
            target: ScopedStopTarget::QueueItem {
                queue_id: "turn:abc/def".to_string(),
            },
            reason: "operator stop queue item".to_string(),
            now_ms: 124,
        })
        .unwrap();
        assert!(stop.marker_file.is_file());
        let receipt_text = fs::read_to_string(stop.receipt_file).unwrap();
        assert!(receipt_text.contains("operator stop queue item"));

        let _ = fs::remove_dir_all(root);
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-admission-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
