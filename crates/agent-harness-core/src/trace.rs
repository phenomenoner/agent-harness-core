use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::path::PathBuf;

use serde::Serialize;
use serde_json::Value;

use crate::runtime_receipt_history::{
    find_runtime_queue_terminal_history, runtime_queue_receipt_history_file,
};

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
    pub source_queue_id: Option<String>,
    pub delivery_id: Option<String>,
    pub source_final_expectation: Option<String>,
    pub final_outbox_disposition: Option<String>,
    pub authority_digest: Option<String>,
    pub phase: Option<String>,
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
    let queue_dir = options.harness_home.join("state").join("runtime-queue");
    let history_identifiers = trace_history_identifiers(&options.id, &records);
    match find_runtime_queue_terminal_history(&queue_dir, &history_identifiers) {
        Ok(history) => {
            let history_file = runtime_queue_receipt_history_file(&queue_dir);
            for historical in history {
                let summary = historical.reason.unwrap_or_else(|| {
                    format!(
                        "historical terminal runtime receipt with status `{}`",
                        historical.status
                    )
                });
                records.push(TraceRecord {
                    source: history_file.clone(),
                    line_number: usize::try_from(historical.row_id).unwrap_or(usize::MAX),
                    trace_id: historical.trace_id,
                    queue_id: Some(historical.queue_id),
                    status: Some(historical.status),
                    event: Some("runtime-receipt-history".to_string()),
                    source_queue_id: None,
                    delivery_id: None,
                    source_final_expectation: None,
                    final_outbox_disposition: None,
                    authority_digest: None,
                    phase: None,
                    summary: summary.chars().take(240).collect(),
                });
            }
        }
        Err(error) => diagnostics.push(format!(
            "{}: {error}",
            runtime_queue_receipt_history_file(&queue_dir).display()
        )),
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
                    | "suppressed"
                    | "external-effect-denied"
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

fn trace_history_identifiers(id: &str, records: &[TraceRecord]) -> BTreeSet<String> {
    let mut identifiers = BTreeSet::new();
    if !id.trim().is_empty() {
        identifiers.insert(id.to_string());
    }
    for record in records {
        if let Some(trace_id) = record
            .trace_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        {
            identifiers.insert(trace_id.to_string());
        }
        if let Some(queue_id) = record
            .queue_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        {
            identifiers.insert(queue_id.to_string());
        }
    }
    identifiers
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
        state.join("goal-closure").join("receipts.jsonl"),
        state
            .join("channel-session-transitions")
            .join("receipts.jsonl"),
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
            source_queue_id: string_path(&value, &["sourceQueueId", "source_queue_id"]),
            delivery_id: string_path(&value, &["deliveryId", "delivery_id"]),
            source_final_expectation: string_path(
                &value,
                &["sourceFinalExpectation", "source_final_expectation"],
            ),
            final_outbox_disposition: string_path(
                &value,
                &["finalOutboxDisposition", "final_outbox_disposition"],
            ),
            authority_digest: string_path(&value, &["authorityDigest", "authority_digest"]),
            phase: string_path(&value, &["phase"]),
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
    use std::collections::HashSet;
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

    #[test]
    fn trace_exposes_typed_final_and_closure_authority_without_payload_text() {
        let root = temp_root("trace_exposes_typed_final_and_closure_authority");
        let harness_home = root.join(".agent-harness");
        let queue = harness_home.join("state").join("runtime-queue");
        let closures = harness_home.join("state").join("goal-closure");
        fs::create_dir_all(&queue).unwrap();
        fs::create_dir_all(&closures).unwrap();
        fs::write(
            queue.join("run-once-receipts.jsonl"),
            r#"{"queueId":"q-final","sourceQueueId":"q-final","deliveryId":"delivery:v2:abc","status":"completed","sourceFinalExpectation":"required","finalOutboxDisposition":"appended"}"#,
        )
        .unwrap();
        fs::write(
            closures.join("receipts.jsonl"),
            r#"{"closureId":"closure-q-final","queueId":"q-final","authorityDigest":"sha256:authority","phase":"terminal-converged","status":"completed"}"#,
        )
        .unwrap();

        let report = trace_harness_event(TraceOptions {
            harness_home,
            id: "q-final".to_string(),
        })
        .unwrap();

        assert_eq!(report.records.len(), 2, "{report:#?}");
        let final_record = report
            .records
            .iter()
            .find(|record| record.delivery_id.is_some())
            .unwrap();
        assert_eq!(final_record.source_queue_id.as_deref(), Some("q-final"));
        assert_eq!(
            final_record.source_final_expectation.as_deref(),
            Some("required")
        );
        assert_eq!(
            final_record.final_outbox_disposition.as_deref(),
            Some("appended")
        );
        let closure = report
            .records
            .iter()
            .find(|record| record.phase.is_some())
            .unwrap();
        assert_eq!(
            closure.authority_digest.as_deref(),
            Some("sha256:authority")
        );
        assert_eq!(closure.phase.as_deref(), Some("terminal-converged"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn trace_reads_committed_terminal_history_without_scanning_raw_archives() {
        let root =
            temp_root("trace_reads_committed_terminal_history_without_scanning_raw_archives");
        let harness_home = root.join(".agent-harness");
        let queue = harness_home.join("state").join("runtime-queue");
        fs::create_dir_all(&queue).unwrap();
        let staged = crate::runtime_receipt_history::stage_runtime_queue_receipt_history(
            &queue,
            "trace-history",
            br#"{"queueId":"queue-history","traceId":"trace-history","sessionKey":"session-history","status":"completed","reason":"historical completion","runtimeClass":"interactive","origin":"channel","completedAtMs":42}
"#,
            b"",
            &HashSet::new(),
            100,
        )
        .unwrap();
        crate::runtime_receipt_history::commit_runtime_queue_receipt_history(&staged, 101).unwrap();
        let archive_dir = queue.join("run-once-receipts-archive");
        fs::create_dir_all(&archive_dir).unwrap();
        fs::write(
            archive_dir.join("run-once-receipts-old.jsonl"),
            r#"{"queueId":"queue-history","traceId":"trace-history","status":"failed-terminal","reason":"archive must not be scanned"}
"#,
        )
        .unwrap();

        let report = trace_harness_event(TraceOptions {
            harness_home,
            id: "trace-history".to_string(),
        })
        .unwrap();

        assert!(report.terminal, "{report:#?}");
        assert_eq!(report.records.len(), 1, "{report:#?}");
        assert_eq!(report.records[0].queue_id.as_deref(), Some("queue-history"));
        assert_eq!(report.records[0].trace_id.as_deref(), Some("trace-history"));
        assert_eq!(
            report.records[0].source,
            crate::runtime_receipt_history::runtime_queue_receipt_history_file(&queue)
        );

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
