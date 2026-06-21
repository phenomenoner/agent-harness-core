use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use ring::rand::{SecureRandom, SystemRandom};
use serde::Serialize;
use serde_json::Value;

use crate::{
    ActivationReadinessOptions, ActivationReadinessReport, check_activation_readiness,
    current_log_time_ms,
    live_control::{
        LIVE_CONTROL_TOKEN_SCHEMA, LiveControlAction, LiveControlTokenRecord,
        LiveControlTokenStatus, LiveControlTokenValidation, env_live_control_token,
        hash_live_control_token, is_live_agent_session_env, is_live_harness_home,
        live_control_tokens_file, validate_live_control_token,
    },
};

const OPS_BACKUP_SCHEMA: &str = "agent-harness.ops-backup.v1";
const OPS_CUTOVER_RECEIPT_SCHEMA: &str = "agent-harness.ops-cutover-receipt.v1";
const OPS_CUTOVER_REQUEST_SCHEMA: &str = "agent-harness.ops-cutover-request.v1";
const OPS_CUTOVER_APPROVAL_SCHEMA: &str = "agent-harness.ops-cutover-approval.v1";
const OPS_CUTOVER_APPLY_SCHEMA: &str = "agent-harness.ops-cutover-apply.v1";
const OPS_CUTOVER_STATUS_SCHEMA: &str = "agent-harness.ops-cutover-status.v1";
const OPS_CONTROL_RECEIPT_SCHEMA: &str = "agent-harness.ops-control-receipt.v1";
const DEFAULT_BACKUP_MAX_FILE_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_LIVE_CONTROL_TOKEN_TTL_SECONDS: i64 = 15 * 60;

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
    pub live_control_token: Option<String>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub live_control: Option<LiveControlTokenValidation>,
    pub at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpsStopFileStatus {
    pub component: String,
    pub stop_file: PathBuf,
    pub present: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpsCutoverRequestOptions {
    pub harness_home: PathBuf,
    pub action: LiveControlAction,
    pub summary: Option<String>,
    pub candidate_binary: Option<PathBuf>,
    pub staging_home: Option<PathBuf>,
    pub test_notes: Vec<String>,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpsCutoverRequestReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub ticket_id: String,
    pub action: LiveControlAction,
    pub request_file: PathBuf,
    pub receipt_file: PathBuf,
    pub status: String,
    pub summary: Option<String>,
    pub candidate_binary: Option<PathBuf>,
    pub staging_home: Option<PathBuf>,
    pub test_notes: Vec<String>,
    pub at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpsCutoverApproveOptions {
    pub harness_home: PathBuf,
    pub ticket_id: String,
    pub action: LiveControlAction,
    pub issued_to: Option<String>,
    pub ttl_seconds: Option<i64>,
    pub reason: Option<String>,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpsCutoverApproveReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub ticket_id: String,
    pub action: LiveControlAction,
    pub status: String,
    pub reason: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<i64>,
    pub receipt_file: PathBuf,
    pub token_file: PathBuf,
    pub at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpsCutoverApplyOptions {
    pub harness_home: PathBuf,
    pub ticket_id: String,
    pub action: LiveControlAction,
    pub live_control_token: Option<String>,
    pub note: Option<String>,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpsCutoverApplyReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub ticket_id: String,
    pub action: LiveControlAction,
    pub status: String,
    pub reason: String,
    pub live_control: LiveControlTokenValidation,
    pub receipt_file: PathBuf,
    pub note: Option<String>,
    pub at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpsCutoverStatusOptions {
    pub harness_home: PathBuf,
    pub action: Option<LiveControlAction>,
    pub live_control_token: Option<String>,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpsCutoverStatusReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub status: String,
    pub reason: String,
    pub action: Option<LiveControlAction>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub live_control: Option<LiveControlTokenValidation>,
    pub request_count: usize,
    pub receipt_file: PathBuf,
    pub at_ms: i64,
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

pub fn record_ops_cutover_request(
    options: OpsCutoverRequestOptions,
) -> io::Result<OpsCutoverRequestReport> {
    let ticket_id = format!("cutover-{}", options.now_ms);
    let cutover_dir = options.harness_home.join("state").join("cutover");
    let requests_dir = cutover_dir.join("requests");
    fs::create_dir_all(&requests_dir)?;
    let request_file = requests_dir.join(format!("{ticket_id}.json"));
    let receipt_file = cutover_dir.join("cutover-requests.jsonl");
    let report = OpsCutoverRequestReport {
        schema: OPS_CUTOVER_REQUEST_SCHEMA,
        harness_home: options.harness_home,
        ticket_id,
        action: options.action,
        request_file: request_file.clone(),
        receipt_file: receipt_file.clone(),
        status: "requested".to_string(),
        summary: options.summary,
        candidate_binary: options.candidate_binary,
        staging_home: options.staging_home,
        test_notes: options.test_notes,
        at_ms: options.now_ms,
    };
    fs::write(
        &request_file,
        serde_json::to_string_pretty(&report).map_err(io::Error::other)?,
    )?;
    append_json_line(&receipt_file, &report)?;
    Ok(report)
}

pub fn record_ops_cutover_approval(
    options: OpsCutoverApproveOptions,
) -> io::Result<OpsCutoverApproveReport> {
    let receipt_file = options
        .harness_home
        .join("state")
        .join("cutover")
        .join("cutover-approvals.jsonl");
    let token_file = live_control_tokens_file(&options.harness_home);
    let request_file = options
        .harness_home
        .join("state")
        .join("cutover")
        .join("requests")
        .join(format!("{}.json", options.ticket_id));
    let mut blocked_reason = None;
    if is_live_agent_session_env() && is_live_harness_home(&options.harness_home) {
        blocked_reason = Some(
            "live agent sessions cannot issue live-control tokens; request operator approval"
                .to_string(),
        );
    } else {
        match fs::read_to_string(&request_file) {
            Ok(text) => match serde_json::from_str::<Value>(&text) {
                Ok(request) => {
                    let request_action = request
                        .get("action")
                        .and_then(Value::as_str)
                        .and_then(|value| value.parse::<LiveControlAction>().ok());
                    if request_action != Some(options.action) {
                        blocked_reason = Some(format!(
                            "cutover request action does not match approval action: {:?} vs {:?}",
                            request_action, options.action
                        ));
                    }
                }
                Err(error) => {
                    blocked_reason = Some(format!("cutover request file is invalid JSON: {error}"));
                }
            },
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                blocked_reason = Some(format!(
                    "cutover request ticket was not found: {}",
                    options.ticket_id
                ));
            }
            Err(error) => return Err(error),
        }
    }
    if let Some(reason) = blocked_reason {
        let report = OpsCutoverApproveReport {
            schema: OPS_CUTOVER_APPROVAL_SCHEMA,
            harness_home: options.harness_home,
            ticket_id: options.ticket_id,
            action: options.action,
            status: "blocked".to_string(),
            reason,
            token: None,
            expires_at_ms: None,
            receipt_file: receipt_file.clone(),
            token_file,
            at_ms: options.now_ms,
        };
        append_json_line(&receipt_file, &report)?;
        return Ok(report);
    }

    let token = generate_live_control_token()?;
    let ttl_seconds = options
        .ttl_seconds
        .unwrap_or(DEFAULT_LIVE_CONTROL_TOKEN_TTL_SECONDS)
        .max(1);
    let expires_at_ms = options
        .now_ms
        .saturating_add(ttl_seconds.saturating_mul(1000));
    let record = LiveControlTokenRecord {
        schema: LIVE_CONTROL_TOKEN_SCHEMA.to_string(),
        token_hash: hash_live_control_token(&token),
        ticket_id: Some(options.ticket_id.clone()),
        action: options.action,
        issued_to: options.issued_to,
        issued_at_ms: options.now_ms,
        expires_at_ms,
        revoked: false,
        reason: options.reason.clone(),
    };
    append_json_line(&token_file, &record)?;
    let report = OpsCutoverApproveReport {
        schema: OPS_CUTOVER_APPROVAL_SCHEMA,
        harness_home: options.harness_home,
        ticket_id: options.ticket_id,
        action: options.action,
        status: "approved".to_string(),
        reason: options
            .reason
            .unwrap_or_else(|| "live-control token issued".to_string()),
        token: Some(token),
        expires_at_ms: Some(expires_at_ms),
        receipt_file: receipt_file.clone(),
        token_file,
        at_ms: options.now_ms,
    };
    let mut receipt = serde_json::to_value(&report).map_err(io::Error::other)?;
    if let Some(token) = receipt.get_mut("token") {
        *token = Value::String("<redacted>".to_string());
    }
    append_json_line(&receipt_file, &receipt)?;
    Ok(report)
}

pub fn record_ops_cutover_apply(
    options: OpsCutoverApplyOptions,
) -> io::Result<OpsCutoverApplyReport> {
    let env_token = env_live_control_token();
    let token = options
        .live_control_token
        .as_deref()
        .or(env_token.as_deref());
    let validation =
        validate_live_control_token(&options.harness_home, token, options.action, options.now_ms)?;
    let receipt_file = options
        .harness_home
        .join("state")
        .join("cutover")
        .join("cutover-apply-receipts.jsonl");
    let token_ticket_matches = validation.ticket_id.as_deref() == Some(options.ticket_id.as_str());
    let (status, reason) =
        if validation.status == LiveControlTokenStatus::Valid && token_ticket_matches {
            (
                "ready".to_string(),
                "cutover token is valid; operator-controlled stop/start may proceed".to_string(),
            )
        } else if validation.status == LiveControlTokenStatus::Valid {
            (
                "blocked".to_string(),
                "cutover token ticket does not match apply ticket".to_string(),
            )
        } else {
            (
                "blocked".to_string(),
                format!("cutover token invalid: {}", validation.reason),
            )
        };
    let report = OpsCutoverApplyReport {
        schema: OPS_CUTOVER_APPLY_SCHEMA,
        harness_home: options.harness_home,
        ticket_id: options.ticket_id,
        action: options.action,
        status,
        reason,
        live_control: validation,
        receipt_file: receipt_file.clone(),
        note: options.note,
        at_ms: options.now_ms,
    };
    append_json_line(&receipt_file, &report)?;
    Ok(report)
}

pub fn collect_ops_cutover_status(
    options: OpsCutoverStatusOptions,
) -> io::Result<OpsCutoverStatusReport> {
    let cutover_dir = options.harness_home.join("state").join("cutover");
    let requests_dir = cutover_dir.join("requests");
    let request_count = match fs::read_dir(&requests_dir) {
        Ok(entries) => entries
            .filter_map(Result::ok)
            .filter(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some("json"))
            .count(),
        Err(error) if error.kind() == io::ErrorKind::NotFound => 0,
        Err(error) => return Err(error),
    };
    let env_token = env_live_control_token();
    let live_control = match options.action {
        Some(action) => Some(validate_live_control_token(
            &options.harness_home,
            options
                .live_control_token
                .as_deref()
                .or(env_token.as_deref()),
            action,
            options.now_ms,
        )?),
        None => None,
    };
    let status = match &live_control {
        Some(validation) if validation.status == LiveControlTokenStatus::Valid => "ready",
        Some(_) => "blocked",
        None => "observed",
    }
    .to_string();
    let reason = match &live_control {
        Some(validation) => validation.reason.clone(),
        None => "cutover status observed".to_string(),
    };
    Ok(OpsCutoverStatusReport {
        schema: OPS_CUTOVER_STATUS_SCHEMA,
        harness_home: options.harness_home,
        status,
        reason,
        action: options.action,
        live_control,
        request_count,
        receipt_file: cutover_dir.join("cutover-requests.jsonl"),
        at_ms: options.now_ms,
    })
}

pub fn record_ops_control(options: OpsControlOptions) -> io::Result<OpsControlReport> {
    let stop_files = discover_stop_files(&options.harness_home)?;
    let mut live_control = None;
    if options.action != OpsControlAction::Status
        && is_live_agent_session_env()
        && is_live_harness_home(&options.harness_home)
    {
        let action = match options.action {
            OpsControlAction::Stop => LiveControlAction::Stop,
            OpsControlAction::Start => LiveControlAction::Start,
            OpsControlAction::Status => LiveControlAction::Status,
        };
        let env_token = env_live_control_token();
        let token = options
            .live_control_token
            .as_deref()
            .or(env_token.as_deref());
        let validation =
            validate_live_control_token(&options.harness_home, token, action, options.now_ms)?;
        if validation.status != LiveControlTokenStatus::Valid {
            let receipt_file = options
                .harness_home
                .join("state")
                .join("ops")
                .join("control-receipts.jsonl");
            let report = OpsControlReport {
                schema: OPS_CONTROL_RECEIPT_SCHEMA,
                harness_home: options.harness_home,
                action: options.action,
                receipt_file: receipt_file.clone(),
                status: "blocked-live-control".to_string(),
                reason: format!(
                    "live gateway control from a live agent session requires a valid token: {}",
                    validation.reason
                ),
                stop_files,
                live_control: Some(validation),
                at_ms: options.now_ms,
            };
            append_json_line(&receipt_file, &report)?;
            return Ok(report);
        }
        live_control = Some(validation);
    }
    match options.action {
        OpsControlAction::Stop => {
            for stop in &stop_files {
                if let Some(parent) = stop.stop_file.parent() {
                    fs::create_dir_all(parent)?;
                }
                let reason = options
                    .reason
                    .as_deref()
                    .unwrap_or("stop files created; loops will exit at their next poll");
                let stop_file = serde_json::json!({
                    "schema": "agent-harness.supervisor-stop-file.v1",
                    "serviceId": stop.component,
                    "reason": reason,
                    "createdBy": "ops-control",
                    "createdAtMs": options.now_ms,
                    "persistent": true
                });
                crate::write_json_atomic(&stop.stop_file, &stop_file)?;
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
        live_control,
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

fn generate_live_control_token() -> io::Result<String> {
    let rng = SystemRandom::new();
    let mut bytes = [0u8; 32];
    rng.fill(&mut bytes)
        .map_err(|_| io::Error::other("failed to generate live-control token"))?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
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
            live_control_token: None,
            now_ms: 1000,
        })
        .unwrap();
        assert_eq!(stop.status, "stop-requested");
        assert!(stop.stop_files[0].present);
        let stop_file_json: Value =
            serde_json::from_slice(&fs::read(&stop.stop_files[0].stop_file).unwrap()).unwrap();
        assert_eq!(
            stop_file_json["schema"],
            "agent-harness.supervisor-stop-file.v1"
        );
        assert_eq!(stop_file_json["serviceId"], "worker-loop");
        assert_eq!(stop_file_json["createdBy"], "ops-control");
        assert_eq!(stop_file_json["createdAtMs"], 1000);
        assert_eq!(stop_file_json["persistent"], true);

        let start = record_ops_control(OpsControlOptions {
            harness_home,
            action: OpsControlAction::Start,
            reason: None,
            live_control_token: None,
            now_ms: 1001,
        })
        .unwrap();
        assert_eq!(start.status, "start-ready");
        assert!(!start.stop_files[0].present);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn cutover_approval_token_validates_apply() {
        let root = temp_root("cutover_approval_token_validates_apply");
        let harness_home = root.join("harness");
        let request = record_ops_cutover_request(OpsCutoverRequestOptions {
            harness_home: harness_home.clone(),
            action: LiveControlAction::Cutover,
            summary: Some("test cutover".to_string()),
            candidate_binary: None,
            staging_home: None,
            test_notes: vec!["tests passed".to_string()],
            now_ms: 1000,
        })
        .unwrap();
        let approval = record_ops_cutover_approval(OpsCutoverApproveOptions {
            harness_home: harness_home.clone(),
            ticket_id: request.ticket_id.clone(),
            action: LiveControlAction::Cutover,
            issued_to: Some("operator".to_string()),
            ttl_seconds: Some(60),
            reason: None,
            now_ms: 1001,
        })
        .unwrap();
        let apply = record_ops_cutover_apply(OpsCutoverApplyOptions {
            harness_home,
            ticket_id: request.ticket_id,
            action: LiveControlAction::Stop,
            live_control_token: approval.token,
            note: None,
            now_ms: 1002,
        })
        .unwrap();

        assert_eq!(apply.status, "ready");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn cutover_approval_requires_existing_matching_request() {
        let root = temp_root("cutover_approval_requires_existing_matching_request");
        let harness_home = root.join("harness");

        let missing = record_ops_cutover_approval(OpsCutoverApproveOptions {
            harness_home: harness_home.clone(),
            ticket_id: "cutover-missing".to_string(),
            action: LiveControlAction::Cutover,
            issued_to: Some("operator".to_string()),
            ttl_seconds: Some(60),
            reason: None,
            now_ms: 1000,
        })
        .unwrap();
        assert_eq!(missing.status, "blocked");
        assert!(missing.token.is_none());

        let request = record_ops_cutover_request(OpsCutoverRequestOptions {
            harness_home: harness_home.clone(),
            action: LiveControlAction::Stop,
            summary: None,
            candidate_binary: None,
            staging_home: None,
            test_notes: Vec::new(),
            now_ms: 1001,
        })
        .unwrap();
        let mismatched = record_ops_cutover_approval(OpsCutoverApproveOptions {
            harness_home,
            ticket_id: request.ticket_id,
            action: LiveControlAction::Start,
            issued_to: Some("operator".to_string()),
            ttl_seconds: Some(60),
            reason: None,
            now_ms: 1002,
        })
        .unwrap();
        assert_eq!(mismatched.status, "blocked");
        assert!(mismatched.token.is_none());

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
