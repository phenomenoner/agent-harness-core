use std::fs;
use std::io;
use std::path::PathBuf;

use serde::Serialize;
use serde_json::Value;

const TRACE_REPORT_SCHEMA: &str = "agent-harness.trace.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceOptions {
    pub harness_home: PathBuf,
    pub id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub id: String,
    pub records: Vec<TraceRecord>,
    pub diagnostics: Vec<String>,
    pub terminal: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceRecord {
    pub source: PathBuf,
    pub line_number: usize,
    pub trace_id: Option<String>,
    pub queue_id: Option<String>,
    pub status: Option<String>,
    pub event: Option<String>,
    pub summary: String,
}

pub fn trace_harness_event(options: TraceOptions) -> io::Result<TraceReport> {
    let sources = trace_sources(&options.harness_home);
    let mut diagnostics = Vec::new();
    let mut records = Vec::new();
    for source in sources {
        match read_matching_records(&source, &options.id) {
            Ok(mut matches) => records.append(&mut matches),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => diagnostics.push(format!("{}: {error}", source.display())),
        }
    }
    records.sort_by(|left, right| {
        left.source
            .cmp(&right.source)
            .then(left.line_number.cmp(&right.line_number))
    });
    if records.is_empty() {
        diagnostics.push(format!(
            "no receipt/log records found for trace or queue id `{}`",
            options.id
        ));
    }
    let terminal = records.iter().any(|record| {
        record.status.as_deref().is_some_and(|status| {
            matches!(
                status,
                "completed"
                    | "delivered"
                    | "failed-terminal"
                    | "dead-letter"
                    | "timeout"
                    | "skipped"
                    | "canceled"
            )
        })
    });
    if !terminal {
        diagnostics.push("no terminal delivery/error/dead-letter record found".to_string());
    }
    Ok(TraceReport {
        schema: TRACE_REPORT_SCHEMA,
        harness_home: options.harness_home,
        id: options.id,
        records,
        diagnostics,
        terminal,
    })
}

fn trace_sources(harness_home: &std::path::Path) -> Vec<PathBuf> {
    let state = harness_home.join("state");
    let channels = state.join("channels");
    let queue = state.join("runtime-queue");
    vec![
        channels.join("receive-receipts.jsonl"),
        channels.join("outbox.jsonl"),
        channels.join("delivery-receipts.jsonl"),
        queue.join("pending.jsonl"),
        queue.join("receipts.jsonl"),
        queue.join("control-receipts.jsonl"),
        queue.join("execution-receipts.jsonl"),
        queue.join("codex-runtime-receipts.jsonl"),
        queue.join("codex-runtime-run-receipts.jsonl"),
        queue.join("codex-runtime-completion-receipts.jsonl"),
        queue.join("run-once-receipts.jsonl"),
        queue.join("dead-letter-receipts.jsonl"),
        queue.join("progress-events.jsonl"),
        state.join("logs").join("harness.jsonl"),
    ]
}

fn read_matching_records(path: &PathBuf, id: &str) -> io::Result<Vec<TraceRecord>> {
    let text = fs::read_to_string(path)?;
    let mut records = Vec::new();
    for (index, line) in text.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
            continue;
        };
        if !value_matches_id(&value, id) {
            continue;
        }
        records.push(TraceRecord {
            source: path.clone(),
            line_number: index + 1,
            trace_id: string_path(&value, &["traceId", "trace_id"]),
            queue_id: string_path(&value, &["queueId", "queue_id"]),
            status: string_path(&value, &["status"]),
            event: string_path(&value, &["event"]),
            summary: summarize_record(&value),
        });
    }
    Ok(records)
}

fn value_matches_id(value: &Value, id: &str) -> bool {
    match value {
        Value::String(raw) => raw == id,
        Value::Array(values) => values.iter().any(|value| value_matches_id(value, id)),
        Value::Object(object) => object.iter().any(|(key, value)| {
            matches!(
                key.as_str(),
                "traceId"
                    | "trace_id"
                    | "queueId"
                    | "queue_id"
                    | "deliveryId"
                    | "delivery_id"
                    | "sessionKey"
                    | "session_key"
            ) && value.as_str() == Some(id)
                || value_matches_id(value, id)
        }),
        _ => false,
    }
}

fn summarize_record(value: &Value) -> String {
    for key in ["reason", "message", "status", "event", "method"] {
        if let Some(raw) = value.get(key).and_then(Value::as_str) {
            return raw.chars().take(240).collect();
        }
    }
    "matched trace record".to_string()
}

fn string_path(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(Value::as_str))
        .map(ToString::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn trace_collects_legacy_queue_id_chain_and_terminal_status() {
        let root = temp_root("trace_collects_legacy_queue_id_chain_and_terminal_status");
        let harness_home = root.join(".agent-harness");
        let queue = harness_home.join("state").join("runtime-queue");
        let channels = harness_home.join("state").join("channels");
        fs::create_dir_all(&queue).unwrap();
        fs::create_dir_all(&channels).unwrap();
        fs::write(
            queue.join("pending.jsonl"),
            r#"{"queueId":"q-1","status":"queued","reason":"queued"}"#,
        )
        .unwrap();
        fs::write(
            queue.join("run-once-receipts.jsonl"),
            r#"{"queueId":"q-1","status":"dead-letter","reason":"provider timeout"}"#,
        )
        .unwrap();
        fs::write(
            channels.join("delivery-receipts.jsonl"),
            r#"{"deliveryId":"d-1","queueId":"q-1","status":"delivered","reason":"error delivered"}"#,
        )
        .unwrap();

        let report = trace_harness_event(TraceOptions {
            harness_home,
            id: "q-1".to_string(),
        })
        .unwrap();

        assert_eq!(report.records.len(), 3);
        assert!(report.terminal);
        assert!(report.diagnostics.is_empty());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn trace_treats_timeout_status_as_terminal() {
        let root = temp_root("trace_treats_timeout_status_as_terminal");
        let harness_home = root.join(".agent-harness");
        let queue = harness_home.join("state").join("runtime-queue");
        fs::create_dir_all(&queue).unwrap();
        fs::write(
            queue.join("run-once-receipts.jsonl"),
            r#"{"queueId":"q-timeout","status":"timeout","reason":"runtime timeout"}"#,
        )
        .unwrap();

        let report = trace_harness_event(TraceOptions {
            harness_home,
            id: "q-timeout".to_string(),
        })
        .unwrap();

        assert_eq!(report.records.len(), 1);
        assert!(report.terminal);
        assert!(report.diagnostics.is_empty());

        let _ = fs::remove_dir_all(root);
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-trace-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
