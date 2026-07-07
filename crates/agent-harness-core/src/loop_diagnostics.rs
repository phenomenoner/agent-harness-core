use std::collections::HashSet;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::Value;

const LOOP_ERROR_DIAGNOSTICS_SCHEMA: &str = "agent-harness.loop-error-diagnostics.v1";
const RESOURCE_EXHAUSTION_READBACK_SCHEMA: &str = "agent-harness.resource-exhaustion-readback.v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LoopErrorDiagnosticsReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub component: String,
    pub at_ms: i64,
    pub process: ProcessMemorySnapshot,
    pub runtime_queue: RuntimeQueueActivitySnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProcessMemorySnapshot {
    pub available: bool,
    pub pid: u32,
    pub working_set_bytes: Option<u64>,
    pub peak_working_set_bytes: Option<u64>,
    pub commit_bytes: Option<u64>,
    pub private_bytes: Option<u64>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeQueueActivitySnapshot {
    pub queue_dir: PathBuf,
    pub pending_file: PathBuf,
    pub run_once_receipts_file: PathBuf,
    pub pending_items: usize,
    pub open_items: usize,
    pub terminal_receipts: usize,
    pub terminal_queue_ids: usize,
    pub active_leases: usize,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceExhaustionReadbackOptions {
    pub limit: usize,
    pub max_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceExhaustionReadbackReport {
    pub schema: &'static str,
    pub supported: bool,
    pub status: String,
    pub limit: usize,
    pub max_bytes: usize,
    pub command: Vec<String>,
    pub exit_code: Option<i32>,
    pub event_count_hint: usize,
    pub truncated: bool,
    pub raw_output: String,
    pub stderr: Option<String>,
}

pub fn collect_loop_error_diagnostics(
    harness_home: &Path,
    component: impl Into<String>,
    at_ms: i64,
) -> LoopErrorDiagnosticsReport {
    LoopErrorDiagnosticsReport {
        schema: LOOP_ERROR_DIAGNOSTICS_SCHEMA,
        harness_home: harness_home.to_path_buf(),
        component: component.into(),
        at_ms,
        process: current_process_memory_snapshot(),
        runtime_queue: collect_runtime_queue_activity(harness_home),
    }
}

pub fn collect_resource_exhaustion_readback(
    options: ResourceExhaustionReadbackOptions,
) -> io::Result<ResourceExhaustionReadbackReport> {
    let limit = options.limit.clamp(1, 20);
    let max_bytes = options.max_bytes.clamp(1_024, 262_144);
    let query = "*[System[Provider[@Name='Microsoft-Windows-Resource-Exhaustion-Detector'] and (EventID=2004)]]";
    let command = vec![
        "wevtutil".to_string(),
        "qe".to_string(),
        "System".to_string(),
        format!("/q:{query}"),
        "/rd:true".to_string(),
        format!("/c:{limit}"),
        "/f:text".to_string(),
    ];

    #[cfg(not(windows))]
    {
        Ok(ResourceExhaustionReadbackReport {
            schema: RESOURCE_EXHAUSTION_READBACK_SCHEMA,
            supported: false,
            status: "unsupported-platform".to_string(),
            limit,
            max_bytes,
            command,
            exit_code: None,
            event_count_hint: 0,
            truncated: false,
            raw_output: String::new(),
            stderr: None,
        })
    }

    #[cfg(windows)]
    {
        let output = std::process::Command::new("wevtutil")
            .args(&command[1..])
            .output();
        let output = match output {
            Ok(output) => output,
            Err(error) => {
                return Ok(ResourceExhaustionReadbackReport {
                    schema: RESOURCE_EXHAUSTION_READBACK_SCHEMA,
                    supported: true,
                    status: "command-failed".to_string(),
                    limit,
                    max_bytes,
                    command,
                    exit_code: None,
                    event_count_hint: 0,
                    truncated: false,
                    raw_output: String::new(),
                    stderr: Some(error.to_string()),
                });
            }
        };
        let mut raw_output = String::from_utf8_lossy(&output.stdout).to_string();
        let event_count_hint = raw_output.matches("Event[").count();
        let (truncated, raw_output) = truncate_utf8(raw_output.as_mut_str(), max_bytes);
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        Ok(ResourceExhaustionReadbackReport {
            schema: RESOURCE_EXHAUSTION_READBACK_SCHEMA,
            supported: true,
            status: if output.status.success() {
                "ok".to_string()
            } else {
                "command-exit".to_string()
            },
            limit,
            max_bytes,
            command,
            exit_code: output.status.code(),
            event_count_hint,
            truncated,
            raw_output,
            stderr: (!stderr.is_empty()).then_some(stderr),
        })
    }
}

#[cfg(windows)]
fn current_process_memory_snapshot() -> ProcessMemorySnapshot {
    use windows_sys::Win32::System::ProcessStatus::{
        K32GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS, PROCESS_MEMORY_COUNTERS_EX,
    };
    use windows_sys::Win32::System::Threading::GetCurrentProcess;

    let mut counters: PROCESS_MEMORY_COUNTERS_EX = unsafe { std::mem::zeroed() };
    counters.cb = std::mem::size_of::<PROCESS_MEMORY_COUNTERS_EX>() as u32;
    let ok = unsafe {
        K32GetProcessMemoryInfo(
            GetCurrentProcess(),
            &mut counters as *mut PROCESS_MEMORY_COUNTERS_EX as *mut PROCESS_MEMORY_COUNTERS,
            counters.cb,
        )
    };
    if ok == 0 {
        return ProcessMemorySnapshot {
            available: false,
            pid: std::process::id(),
            working_set_bytes: None,
            peak_working_set_bytes: None,
            commit_bytes: None,
            private_bytes: None,
            error: Some(io::Error::last_os_error().to_string()),
        };
    }
    ProcessMemorySnapshot {
        available: true,
        pid: std::process::id(),
        working_set_bytes: Some(counters.WorkingSetSize as u64),
        peak_working_set_bytes: Some(counters.PeakWorkingSetSize as u64),
        commit_bytes: Some(counters.PagefileUsage as u64),
        private_bytes: Some(counters.PrivateUsage as u64),
        error: None,
    }
}

#[cfg(not(windows))]
fn current_process_memory_snapshot() -> ProcessMemorySnapshot {
    ProcessMemorySnapshot {
        available: false,
        pid: std::process::id(),
        working_set_bytes: None,
        peak_working_set_bytes: None,
        commit_bytes: None,
        private_bytes: None,
        error: Some("unsupported-platform".to_string()),
    }
}

fn collect_runtime_queue_activity(harness_home: &Path) -> RuntimeQueueActivitySnapshot {
    let queue_dir = harness_home.join("state").join("runtime-queue");
    let pending_file = queue_dir.join("pending.jsonl");
    let run_once_receipts_file = queue_dir.join("run-once-receipts.jsonl");
    let mut warnings = Vec::new();
    let pending_queue_ids = read_queue_ids(&pending_file, "pending queue", &mut warnings);
    let (terminal_receipts, terminal_queue_ids) =
        read_terminal_receipts(&run_once_receipts_file, &mut warnings);
    let open_items = pending_queue_ids
        .iter()
        .filter(|queue_id| !terminal_queue_ids.contains(*queue_id))
        .count();
    let active_leases = count_runtime_leases(&queue_dir, &mut warnings);
    RuntimeQueueActivitySnapshot {
        queue_dir,
        pending_file,
        run_once_receipts_file,
        pending_items: pending_queue_ids.len(),
        open_items,
        terminal_receipts,
        terminal_queue_ids: terminal_queue_ids.len(),
        active_leases,
        warnings,
    }
}

fn read_queue_ids(path: &Path, label: &str, warnings: &mut Vec<String>) -> Vec<String> {
    read_jsonl_values(path, label, warnings)
        .into_iter()
        .filter_map(|value| string_field(&value, "queueId").map(ToString::to_string))
        .collect()
}

fn read_terminal_receipts(path: &Path, warnings: &mut Vec<String>) -> (usize, HashSet<String>) {
    let mut receipts = 0usize;
    let mut queue_ids = HashSet::new();
    for value in read_jsonl_values(path, "run-once receipts", warnings) {
        let status = string_field(&value, "status").unwrap_or_default();
        if !is_terminal_status(status) {
            continue;
        }
        receipts += 1;
        if let Some(queue_id) = string_field(&value, "queueId") {
            queue_ids.insert(queue_id.to_string());
        }
    }
    (receipts, queue_ids)
}

fn count_runtime_leases(queue_dir: &Path, warnings: &mut Vec<String>) -> usize {
    let mut files = Vec::new();
    files.push(queue_dir.join("runtime-leases.json"));
    let classes_dir = queue_dir.join("classes");
    if let Ok(entries) = fs::read_dir(&classes_dir) {
        for entry in entries.flatten() {
            let path = entry.path().join("runtime-leases.json");
            if path.is_file() {
                files.push(path);
            }
        }
    }

    files
        .into_iter()
        .map(|path| {
            let text = match fs::read_to_string(&path) {
                Ok(text) => text,
                Err(error) if error.kind() == io::ErrorKind::NotFound => return 0,
                Err(error) => {
                    warnings.push(format!(
                        "failed to read runtime lease file {}: {error}",
                        path.display()
                    ));
                    return 0;
                }
            };
            let value: Value = match serde_json::from_str(&text) {
                Ok(value) => value,
                Err(error) => {
                    warnings.push(format!(
                        "failed to parse runtime lease file {}: {error}",
                        path.display()
                    ));
                    return 0;
                }
            };
            value
                .get("leases")
                .and_then(Value::as_object)
                .map(|leases| leases.len())
                .unwrap_or(0)
        })
        .sum()
}

fn read_jsonl_values(path: &Path, label: &str, warnings: &mut Vec<String>) -> Vec<Value> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Vec::new(),
        Err(error) => {
            warnings.push(format!(
                "failed to read {label} {}: {error}",
                path.display()
            ));
            return Vec::new();
        }
    };
    text.lines()
        .enumerate()
        .filter_map(|(index, line)| {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                return None;
            }
            match serde_json::from_str(trimmed) {
                Ok(value) => Some(value),
                Err(error) => {
                    warnings.push(format!(
                        "failed to parse {label} {} line {}: {error}",
                        path.display(),
                        index + 1
                    ));
                    None
                }
            }
        })
        .collect()
}

fn string_field<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value.get(key).and_then(Value::as_str)
}

fn is_terminal_status(status: &str) -> bool {
    matches!(
        status,
        "completed"
            | "timeout"
            | "failed-terminal"
            | "canceled"
            | "skipped"
            | "dead-letter"
            | "suppressed"
    )
}

fn truncate_utf8(value: &str, max_bytes: usize) -> (bool, String) {
    if value.len() <= max_bytes {
        return (false, value.to_string());
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    (true, value[..end].to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn loop_error_diagnostics_records_memory_and_queue_context() {
        let root = temp_root("loop_error_diagnostics_records_memory_and_queue_context");
        let harness_home = root.join(".agent-harness");
        let queue_dir = harness_home.join("state").join("runtime-queue");
        fs::create_dir_all(queue_dir.join("classes").join("interactive")).unwrap();
        fs::write(
            queue_dir.join("pending.jsonl"),
            [
                serde_json::json!({"queueId": "queue-open", "status": "queued"}).to_string(),
                serde_json::json!({"queueId": "queue-terminal", "status": "queued"}).to_string(),
            ]
            .join("\n"),
        )
        .unwrap();
        fs::write(
            queue_dir.join("run-once-receipts.jsonl"),
            serde_json::json!({"queueId": "queue-terminal", "status": "completed"}).to_string(),
        )
        .unwrap();
        fs::write(
            queue_dir
                .join("classes")
                .join("interactive")
                .join("runtime-leases.json"),
            serde_json::json!({
                "schema": "agent-harness.runtime-queue-leases.v1",
                "leases": {
                    "queue-open": {
                        "queueId": "queue-open",
                        "leaseExpiresAtMs": 999999
                    }
                }
            })
            .to_string(),
        )
        .unwrap();

        let report = collect_loop_error_diagnostics(&harness_home, "runtime", 12_345);

        assert_eq!(report.schema, LOOP_ERROR_DIAGNOSTICS_SCHEMA);
        assert_eq!(report.component, "runtime");
        assert_eq!(report.process.pid, std::process::id());
        #[cfg(windows)]
        {
            assert!(report.process.available, "{:?}", report.process.error);
            assert!(report.process.working_set_bytes.unwrap_or_default() > 0);
            assert!(report.process.commit_bytes.unwrap_or_default() > 0);
        }
        assert_eq!(report.runtime_queue.pending_items, 2);
        assert_eq!(report.runtime_queue.open_items, 1);
        assert_eq!(report.runtime_queue.terminal_receipts, 1);
        assert_eq!(report.runtime_queue.terminal_queue_ids, 1);
        assert_eq!(report.runtime_queue.active_leases, 1);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn resource_exhaustion_readback_truncation_preserves_utf8_boundary() {
        let input = "abc日def";
        let (truncated, output) = truncate_utf8(input, 5);
        assert!(truncated);
        assert_eq!(output, "abc");
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-core-loop-diagnostics-{test_name}-{nanos}"
        ))
    }
}
