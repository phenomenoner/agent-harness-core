use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::Value;

use crate::{
    ActivationReadinessOptions, ActivationReadinessReport, check_activation_readiness,
    current_log_time_ms,
};

const OPS_BACKUP_SCHEMA: &str = "agent-harness.ops-backup.v1";
const OPS_CUTOVER_RECEIPT_SCHEMA: &str = "agent-harness.ops-cutover-receipt.v1";
const OPS_CONTROL_RECEIPT_SCHEMA: &str = "agent-harness.ops-control-receipt.v1";
const DEFAULT_BACKUP_MAX_FILE_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpsBackupOptions {
    pub harness_home: PathBuf,
    pub label: Option<String>,
    pub max_file_bytes: u64,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpsBackupReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub backup_dir: PathBuf,
    pub manifest_file: PathBuf,
    pub copied_files: usize,
    pub skipped_files: usize,
    pub bytes_copied: u64,
    pub entries: Vec<OpsBackupEntry>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpsBackupEntry {
    pub source: PathBuf,
    pub target: Option<PathBuf>,
    pub bytes: u64,
    pub copied: bool,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpsCutoverReceiptOptions {
    pub harness_home: PathBuf,
    pub note: Option<String>,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpsCutoverReceiptReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub receipt_file: PathBuf,
    pub status: String,
    pub ready: bool,
    pub summary: crate::ActivationReadinessSummary,
    pub note: Option<String>,
    pub at_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum OpsControlAction {
    Stop,
    Start,
    Status,
}

impl std::str::FromStr for OpsControlAction {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "stop" => Ok(Self::Stop),
            "start" => Ok(Self::Start),
            "status" => Ok(Self::Status),
            other => Err(format!("unsupported ops control action: {other}")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpsControlOptions {
    pub harness_home: PathBuf,
    pub action: OpsControlAction,
    pub reason: Option<String>,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpsControlReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub action: OpsControlAction,
    pub receipt_file: PathBuf,
    pub status: String,
    pub reason: String,
    pub stop_files: Vec<OpsStopFileStatus>,
    pub at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpsStopFileStatus {
    pub component: String,
    pub stop_file: PathBuf,
    pub present: bool,
}

pub fn create_ops_backup(options: OpsBackupOptions) -> io::Result<OpsBackupReport> {
    let max_file_bytes = if options.max_file_bytes == 0 {
        DEFAULT_BACKUP_MAX_FILE_BYTES
    } else {
        options.max_file_bytes
    };
    let label = options
        .label
        .unwrap_or_else(|| format!("{}", options.now_ms));
    let backup_dir = options
        .harness_home
        .join("state")
        .join("backups")
        .join(safe_label(&label));
    let files_dir = backup_dir.join("files");
    fs::create_dir_all(&files_dir)?;

    let mut report = OpsBackupReport {
        schema: OPS_BACKUP_SCHEMA,
        harness_home: options.harness_home.clone(),
        backup_dir: backup_dir.clone(),
        manifest_file: backup_dir.join("backup-manifest.json"),
        copied_files: 0,
        skipped_files: 0,
        bytes_copied: 0,
        entries: Vec::new(),
        warnings: Vec::new(),
    };

    for relative in [
        "state/harness-registry.json",
        "state/runtime-queue",
        "state/workers",
        "state/memory",
        "state/channels",
        "state/plugin-sidecar",
        "state/supervisor",
        "agents",
    ] {
        let source = options.harness_home.join(relative);
        copy_backup_path(
            &options.harness_home,
            &source,
            &files_dir,
            max_file_bytes,
            &mut report,
        )?;
    }

    if let Some(parent) = report.manifest_file.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(
        &report.manifest_file,
        serde_json::to_string_pretty(&report).map_err(io::Error::other)?,
    )?;
    Ok(report)
}

pub fn record_ops_cutover_receipt(
    options: OpsCutoverReceiptOptions,
) -> io::Result<OpsCutoverReceiptReport> {
    let readiness = check_activation_readiness(ActivationReadinessOptions {
        harness_home: options.harness_home.clone(),
    })?;
    let receipt_file = options
        .harness_home
        .join("state")
        .join("cutover")
        .join("cutover-receipts.jsonl");
    let status = cutover_status(&readiness);
    let report = OpsCutoverReceiptReport {
        schema: OPS_CUTOVER_RECEIPT_SCHEMA,
        harness_home: options.harness_home,
        receipt_file: receipt_file.clone(),
        status,
        ready: readiness.ready,
        summary: readiness.summary,
        note: options.note,
        at_ms: options.now_ms,
    };
    append_json_line(&receipt_file, &report)?;
    Ok(report)
}

pub fn record_ops_control(options: OpsControlOptions) -> io::Result<OpsControlReport> {
    let stop_files = discover_stop_files(&options.harness_home)?;
    match options.action {
        OpsControlAction::Stop => {
            for stop in &stop_files {
                if let Some(parent) = stop.stop_file.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::write(
                    &stop.stop_file,
                    b"stop requested by agent-harness ops-control\n",
                )?;
            }
        }
        OpsControlAction::Start => {
            for stop in &stop_files {
                match fs::remove_file(&stop.stop_file) {
                    Ok(()) => {}
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error),
                }
            }
        }
        OpsControlAction::Status => {}
    }
    let after = discover_stop_files(&options.harness_home)?;
    let receipt_file = options
        .harness_home
        .join("state")
        .join("ops")
        .join("control-receipts.jsonl");
    let status = match options.action {
        OpsControlAction::Stop if after.iter().all(|stop| stop.present) => "stop-requested",
        OpsControlAction::Start if after.iter().all(|stop| !stop.present) => "start-ready",
        OpsControlAction::Status => "observed",
        OpsControlAction::Stop => "partial-stop-request",
        OpsControlAction::Start => "partial-start-ready",
    }
    .to_string();
    let reason = options.reason.unwrap_or_else(|| match options.action {
        OpsControlAction::Stop => {
            "stop files created; loops will exit at their next poll".to_string()
        }
        OpsControlAction::Start => {
            "stop files cleared; run supervisor start script or scheduled tasks to launch loops"
                .to_string()
        }
        OpsControlAction::Status => "supervisor stop-file status observed".to_string(),
    });
    let report = OpsControlReport {
        schema: OPS_CONTROL_RECEIPT_SCHEMA,
        harness_home: options.harness_home,
        action: options.action,
        receipt_file: receipt_file.clone(),
        status,
        reason,
        stop_files: after,
        at_ms: options.now_ms,
    };
    append_json_line(&receipt_file, &report)?;
    Ok(report)
}

fn copy_backup_path(
    harness_home: &Path,
    source: &Path,
    files_dir: &Path,
    max_file_bytes: u64,
    report: &mut OpsBackupReport,
) -> io::Result<()> {
    if !source.exists() {
        report.warnings.push(format!(
            "backup source not found and skipped: {}",
            source.display()
        ));
        return Ok(());
    }
    if source.is_dir() {
        for entry in fs::read_dir(source)? {
            let entry = entry?;
            let path = entry.path();
            if should_skip_backup_path(&path) {
                continue;
            }
            copy_backup_path(harness_home, &path, files_dir, max_file_bytes, report)?;
        }
        return Ok(());
    }
    if !source.is_file() {
        return Ok(());
    }
    let metadata = fs::metadata(source)?;
    if metadata.len() > max_file_bytes {
        report.skipped_files += 1;
        report.entries.push(OpsBackupEntry {
            source: source.to_path_buf(),
            target: None,
            bytes: metadata.len(),
            copied: false,
            reason: format!("file exceeds maxFileBytes={max_file_bytes}"),
        });
        return Ok(());
    }
    let relative = source.strip_prefix(harness_home).unwrap_or(source);
    let target = files_dir.join(relative);
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::copy(source, &target)?;
    report.copied_files += 1;
    report.bytes_copied = report.bytes_copied.saturating_add(metadata.len());
    report.entries.push(OpsBackupEntry {
        source: source.to_path_buf(),
        target: Some(target),
        bytes: metadata.len(),
        copied: true,
        reason: "copied non-secret harness state".to_string(),
    });
    Ok(())
}

fn should_skip_backup_path(path: &Path) -> bool {
    path.components().any(|component| {
        component
            .as_os_str()
            .to_str()
            .is_some_and(|part| matches!(part, "secrets" | "backups" | "logs"))
    })
}

fn discover_stop_files(harness_home: &Path) -> io::Result<Vec<OpsStopFileStatus>> {
    let plan_file = harness_home
        .join("state")
        .join("supervisor")
        .join("windows-scheduled-tasks")
        .join("supervisor-plan.json");
    if let Ok(text) = fs::read_to_string(&plan_file) {
        if let Ok(value) = serde_json::from_str::<Value>(&text) {
            let mut stops = value
                .get("tasks")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|task| {
                    let component = task.get("component").and_then(Value::as_str)?;
                    let stop_file = task.get("stopFile").and_then(Value::as_str)?;
                    Some(stop_file_status(
                        component.to_string(),
                        PathBuf::from(stop_file),
                    ))
                })
                .collect::<Vec<_>>();
            if !stops.is_empty() {
                stops.sort_by(|left, right| left.component.cmp(&right.component));
                return Ok(stops);
            }
        }
    }

    let stop_dir = harness_home
        .join("state")
        .join("supervisor")
        .join("windows-scheduled-tasks")
        .join("stop");
    let mut stops = Vec::new();
    match fs::read_dir(&stop_dir) {
        Ok(entries) => {
            for entry in entries {
                let entry = entry?;
                let path = entry.path();
                if path
                    .extension()
                    .and_then(|ext| ext.to_str())
                    .is_some_and(|ext| ext.eq_ignore_ascii_case("stop"))
                {
                    let component = path
                        .file_stem()
                        .and_then(|name| name.to_str())
                        .unwrap_or("unknown")
                        .to_string();
                    stops.push(stop_file_status(component, path));
                }
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    stops.sort_by(|left, right| left.component.cmp(&right.component));
    Ok(stops)
}

fn stop_file_status(component: String, stop_file: PathBuf) -> OpsStopFileStatus {
    let present = stop_file.is_file();
    OpsStopFileStatus {
        component,
        stop_file,
        present,
    }
}

fn cutover_status(readiness: &ActivationReadinessReport) -> String {
    if readiness.ready {
        "ready".to_string()
    } else if readiness.summary.failed > 0 {
        "blocked".to_string()
    } else {
        "warnings".to_string()
    }
}

fn safe_label(value: &str) -> String {
    let normalized = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    if normalized.is_empty() {
        current_log_time_ms()
            .map(|now| now.to_string())
            .unwrap_or_else(|_| "backup".to_string())
    } else {
        normalized
    }
}

fn append_json_line(path: &Path, value: &impl Serialize) -> io::Result<()> {
    crate::append_jsonl_value(path, value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn backup_copies_state_and_skips_secrets() {
        let root = temp_root("backup_copies_state_and_skips_secrets");
        let harness_home = root.join("harness");
        fs::create_dir_all(harness_home.join("state").join("memory")).unwrap();
        fs::create_dir_all(harness_home.join("secrets")).unwrap();
        fs::write(
            harness_home
                .join("state")
                .join("memory")
                .join("hook-receipts.jsonl"),
            "{}\n",
        )
        .unwrap();
        fs::write(
            harness_home.join("secrets").join("token.env"),
            "TOKEN=secret\n",
        )
        .unwrap();

        let report = create_ops_backup(OpsBackupOptions {
            harness_home: harness_home.clone(),
            label: Some("test".to_string()),
            max_file_bytes: DEFAULT_BACKUP_MAX_FILE_BYTES,
            now_ms: 1000,
        })
        .unwrap();

        assert_eq!(report.copied_files, 1);
        assert!(report.manifest_file.is_file());
        assert!(!report.entries.iter().any(|entry| {
            entry
                .source
                .components()
                .any(|component| component.as_os_str() == "secrets")
        }));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn ops_control_creates_and_clears_stop_files() {
        let root = temp_root("ops_control_creates_and_clears_stop_files");
        let harness_home = root.join("harness");
        let supervisor = harness_home
            .join("state")
            .join("supervisor")
            .join("windows-scheduled-tasks");
        fs::create_dir_all(supervisor.join("stop")).unwrap();
        let stop_file = supervisor.join("stop").join("worker-loop.stop");
        fs::write(
            supervisor.join("supervisor-plan.json"),
            serde_json::to_string(&serde_json::json!({
                "tasks": [
                    { "component": "worker-loop", "stopFile": stop_file }
                ]
            }))
            .unwrap(),
        )
        .unwrap();

        let stop = record_ops_control(OpsControlOptions {
            harness_home: harness_home.clone(),
            action: OpsControlAction::Stop,
            reason: None,
            now_ms: 1000,
        })
        .unwrap();
        assert_eq!(stop.status, "stop-requested");
        assert!(stop.stop_files[0].present);

        let start = record_ops_control(OpsControlOptions {
            harness_home,
            action: OpsControlAction::Start,
            reason: None,
            now_ms: 1001,
        })
        .unwrap();
        assert_eq!(start.status, "start-ready");
        assert!(!start.stop_files[0].present);

        let _ = fs::remove_dir_all(root);
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-ops-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
