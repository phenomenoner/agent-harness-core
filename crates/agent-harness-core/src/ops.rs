use std::collections::BTreeSet;
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
const RECONCILE_KEEPER_STOP_COMPONENT: &str = "supervisor-reconcile";
// Scheduled-task control historically also wrote the direct supervisor markers.
// Keep that fallback for backward compatibility, but never use it as the desired
// topology for reconcile-managed ownership.
const LEGACY_SCHEDULED_DIRECT_STOP_COMPONENTS: &[&str] = &[
    "cron-scheduler-loop",
    "discord-gateway-loop",
    "discord-outbox-loop",
    "progress-delivery-loop",
    "runtime-loop",
    "telegram-loop",
    "telegram-loop-xiaoxiaoli",
    "worker-loop",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OpsSupervisorOwnershipMode {
    ScheduledTask,
    ReconcileManaged,
}

#[derive(Debug, Clone)]
struct OpsStopFileDiscovery {
    ownership_mode: OpsSupervisorOwnershipMode,
    stop_files: Vec<OpsStopFileStatus>,
}

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpsKeeperPlanOwnerEvidence {
    pub state_file: PathBuf,
    pub owner_kind: String,
    pub process_field: String,
    pub process_id: i64,
    pub process_alive: Option<bool>,
    pub process_instance_matches: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpsKeeperPlanPreflightReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub ready: bool,
    pub reason: String,
    pub stop_files: Vec<OpsStopFileStatus>,
    pub live_owners: Vec<OpsKeeperPlanOwnerEvidence>,
    pub blockers: Vec<String>,
}

pub fn path_targets_live_harness_home(path: &Path) -> bool {
    is_live_harness_home(path)
        || fs::canonicalize(path)
            .ok()
            .as_deref()
            .is_some_and(is_live_harness_home)
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
    let requested_reason = options.reason.clone();
    let receipt_file = options
        .harness_home
        .join("state")
        .join("ops")
        .join("control-receipts.jsonl");
    let mut live_control = None;
    if options.action != OpsControlAction::Status
        && path_targets_live_harness_home(&options.harness_home)
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
            let (discovery, discovery_errors) =
                discover_stop_files_for_control(&options.harness_home);
            let mut reason = format!(
                "destructive control against a live harness home requires a valid token: {}",
                validation.reason
            );
            if !discovery_errors.is_empty() {
                reason.push_str("; control topology readback warnings: ");
                reason.push_str(&discovery_errors.join("; "));
            }
            let report = OpsControlReport {
                schema: OPS_CONTROL_RECEIPT_SCHEMA,
                harness_home: options.harness_home,
                action: options.action,
                receipt_file: receipt_file.clone(),
                status: "blocked-live-control".to_string(),
                reason,
                stop_files: discovery.stop_files,
                live_control: Some(validation),
                at_ms: options.now_ms,
            };
            return Ok(report);
        }
        live_control = Some(validation);
    }

    let mut mutation_errors = Vec::new();
    let keeper_evidence_before = reconcile_keeper_evidence_exists(&options.harness_home);
    if options.action == OpsControlAction::Stop && keeper_evidence_before {
        let reason = requested_reason
            .as_deref()
            .unwrap_or("root keeper hold committed before child topology discovery");
        if let Err(error) = write_supervisor_stop_marker(
            &options.harness_home,
            RECONCILE_KEEPER_STOP_COMPONENT,
            reason,
            options.now_ms,
        ) {
            mutation_errors.push(format!(
                "failed to precommit the supervisor-reconcile root hold: {error}"
            ));
        }
    }

    let (discovery, discovery_errors) = discover_stop_files_for_control(&options.harness_home);
    mutation_errors.extend(discovery_errors);
    let initial_ownership_mode = discovery.ownership_mode;
    let stop_files = discovery.stop_files;
    match options.action {
        OpsControlAction::Stop => {
            for stop in ordered_control_targets(
                &stop_files,
                options.action,
                initial_ownership_mode == OpsSupervisorOwnershipMode::ReconcileManaged,
            ) {
                let reason = requested_reason
                    .as_deref()
                    .unwrap_or("stop files created; loops will exit at their next poll");
                let result = write_supervisor_stop_marker_at(
                    &stop.stop_file,
                    &stop.component,
                    reason,
                    options.now_ms,
                );
                if let Err(error) = result {
                    mutation_errors.push(format!(
                        "failed to create stop marker for `{}` at {}: {error}",
                        stop.component,
                        stop.stop_file.display()
                    ));
                }
            }
            signal_stop_wake_lanes(&options.harness_home, &stop_files);
        }
        OpsControlAction::Start => {
            if mutation_errors.is_empty() {
                for stop in ordered_control_targets(
                    &stop_files,
                    options.action,
                    initial_ownership_mode == OpsSupervisorOwnershipMode::ReconcileManaged,
                ) {
                    if initial_ownership_mode == OpsSupervisorOwnershipMode::ReconcileManaged
                        && stop.component == RECONCILE_KEEPER_STOP_COMPONENT
                        && !mutation_errors.is_empty()
                    {
                        mutation_errors.push(
                            "retained supervisor-reconcile stop marker because one or more child stop markers could not be cleared"
                                .to_string(),
                        );
                        continue;
                    }
                    match fs::remove_file(&stop.stop_file) {
                        Ok(()) => {}
                        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                        Err(error) => mutation_errors.push(format!(
                            "failed to clear stop marker for `{}` at {}: {error}",
                            stop.component,
                            stop.stop_file.display()
                        )),
                    }
                }
            } else if initial_ownership_mode == OpsSupervisorOwnershipMode::ReconcileManaged {
                mutation_errors.push(
                    "refused to clear any stop marker because control topology validation failed"
                        .to_string(),
                );
            }
            if initial_ownership_mode == OpsSupervisorOwnershipMode::ReconcileManaged
                && !mutation_errors.is_empty()
            {
                if let Err(error) = write_supervisor_stop_marker(
                    &options.harness_home,
                    RECONCILE_KEEPER_STOP_COMPONENT,
                    "start remains fenced because one or more control targets failed validation or mutation",
                    options.now_ms,
                ) {
                    mutation_errors.push(format!(
                        "failed to preserve the supervisor-reconcile root hold after partial start: {error}"
                    ));
                }
            }
        }
        OpsControlAction::Status => {}
    }
    let (after_discovery, after_errors) = discover_stop_files_for_control(&options.harness_home);
    mutation_errors.extend(
        after_errors
            .into_iter()
            .map(|error| format!("post-mutation topology readback: {error}")),
    );
    let target_set_unchanged = initial_ownership_mode == after_discovery.ownership_mode
        && same_stop_target_set(&stop_files, &after_discovery.stop_files);
    let after = if target_set_unchanged {
        after_discovery.stop_files
    } else {
        refresh_stop_file_statuses(&stop_files)
    };
    let status = match options.action {
        OpsControlAction::Stop
            if mutation_errors.is_empty()
                && target_set_unchanged
                && after.iter().all(|stop| stop.present) =>
        {
            "stop-requested"
        }
        OpsControlAction::Start
            if mutation_errors.is_empty()
                && target_set_unchanged
                && after.iter().all(|stop| !stop.present) =>
        {
            "start-ready"
        }
        OpsControlAction::Status => "observed",
        OpsControlAction::Stop => "partial-stop-request",
        OpsControlAction::Start => "partial-start-ready",
    }
    .to_string();
    let mut reason = requested_reason.unwrap_or_else(|| match options.action {
        OpsControlAction::Stop => {
            "stop files created; loops will exit at their next poll".to_string()
        }
        OpsControlAction::Start => {
            "stop files cleared; run supervisor start script or scheduled tasks to launch loops"
                .to_string()
        }
        OpsControlAction::Status => "supervisor stop-file status observed".to_string(),
    });
    if !target_set_unchanged {
        reason.push_str("; supervisor control target set changed during the operation");
    }
    if !mutation_errors.is_empty() {
        reason.push_str("; ");
        reason.push_str(&mutation_errors.join("; "));
    }
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

/// Read-only cutover preflight for replacing the live Windows reconcile-keeper
/// plan. A valid token is intentionally checked by the CLI before this helper
/// runs; this report proves that the current root and every derived/observed
/// child lane are held and that no state-bound keeper or child owner is alive.
pub fn collect_ops_keeper_plan_preflight(harness_home: &Path) -> OpsKeeperPlanPreflightReport {
    let (discovery, discovery_errors) = discover_stop_files_for_control(harness_home);
    let mut blockers = discovery_errors;
    if discovery.ownership_mode != OpsSupervisorOwnershipMode::ReconcileManaged {
        blockers.push(
            "live keeper-plan promotion requires reconcile-managed supervisor topology".to_string(),
        );
    }
    if discovery.stop_files.is_empty() {
        blockers.push("current supervisor topology has no provable stop targets".to_string());
    }
    if !discovery
        .stop_files
        .iter()
        .any(|entry| entry.component == RECONCILE_KEEPER_STOP_COMPONENT)
    {
        blockers.push("supervisor-reconcile root hold is absent from current topology".to_string());
    }
    for entry in &discovery.stop_files {
        if !entry.present {
            blockers.push(format!(
                "current topology is not held: missing `{}` stop marker at {}",
                entry.component,
                entry.stop_file.display()
            ));
        }
    }

    let supervisor_dir = harness_home.join("state").join("supervisor");
    let mut live_owners = Vec::new();
    inspect_keeper_plan_owner_state(
        &supervisor_dir.join("reconcile-keeper.json"),
        "reconcile-keeper",
        &mut live_owners,
        &mut blockers,
    );
    for (directory, owner_kind) in [
        (supervisor_dir.join("services"), "service-owner"),
        (supervisor_dir.join("loop-heartbeats"), "loop-heartbeat"),
    ] {
        inspect_keeper_plan_owner_directory(
            &directory,
            owner_kind,
            &mut live_owners,
            &mut blockers,
        );
    }

    let ready = blockers.is_empty() && live_owners.is_empty();
    let reason = if ready {
        "current reconcile topology is fully held and no keeper or child owner is alive".to_string()
    } else {
        let mut reasons = blockers.clone();
        reasons.extend(live_owners.iter().map(|owner| {
            format!(
                "{} `{}` still references a live or unprovable process instance {} via {}",
                owner.owner_kind,
                owner.state_file.display(),
                owner.process_id,
                owner.process_field
            )
        }));
        reasons.join("; ")
    };
    OpsKeeperPlanPreflightReport {
        schema: "agent-harness.ops-keeper-plan-preflight.v1",
        harness_home: harness_home.to_path_buf(),
        ready,
        reason,
        stop_files: discovery.stop_files,
        live_owners,
        blockers,
    }
}

fn inspect_keeper_plan_owner_directory(
    directory: &Path,
    owner_kind: &str,
    live_owners: &mut Vec<OpsKeeperPlanOwnerEvidence>,
    blockers: &mut Vec<String>,
) {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return,
        Err(error) => {
            blockers.push(format!(
                "failed to enumerate {owner_kind} state at {}: {error}",
                directory.display()
            ));
            return;
        }
    };
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                blockers.push(format!(
                    "failed to enumerate one {owner_kind} state at {}: {error}",
                    directory.display()
                ));
                continue;
            }
        };
        let path = entry.path();
        if path.is_file() && path.extension().and_then(|value| value.to_str()) == Some("json") {
            inspect_keeper_plan_owner_state(&path, owner_kind, live_owners, blockers);
        }
    }
}

fn inspect_keeper_plan_owner_state(
    state_file: &Path,
    owner_kind: &str,
    live_owners: &mut Vec<OpsKeeperPlanOwnerEvidence>,
    blockers: &mut Vec<String>,
) {
    let bytes = match fs::read(state_file) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return,
        Err(error) => {
            blockers.push(format!(
                "failed to read {owner_kind} state at {}: {error}",
                state_file.display()
            ));
            return;
        }
    };
    let value = match serde_json::from_slice::<Value>(&bytes) {
        Ok(value) => value,
        Err(error) => {
            blockers.push(format!(
                "{owner_kind} state is invalid JSON at {}: {error}",
                state_file.display()
            ));
            return;
        }
    };
    let recorded_start = value.get("processStartTimeMs").and_then(Value::as_i64);
    let primary_process = value
        .get("processId")
        .and_then(Value::as_i64)
        .map(|process_id| ("processId", process_id))
        .or_else(|| {
            value
                .get("pid")
                .and_then(Value::as_i64)
                .map(|process_id| ("pid", process_id))
        });
    let mut candidates = Vec::new();
    if let Some(candidate) = primary_process {
        candidates.push(candidate);
    }
    for field in ["supervisorPid", "parentPid"] {
        if let Some(process_id) = value.get(field).and_then(Value::as_i64) {
            candidates.push((field, process_id));
        }
    }
    candidates.sort_unstable();
    candidates.dedup();

    for (field, process_id) in candidates {
        if process_id <= 0 {
            continue;
        }
        let process_alive = crate::loop_health::process_alive_for_pid(process_id);
        let process_instance_matches = if field == "processId" || field == "pid" {
            recorded_start.map(|expected| {
                crate::loop_health::process_start_time_ms_for_pid(process_id) == Some(expected)
            })
        } else {
            None
        };
        let blocks = match (process_alive, process_instance_matches) {
            (Some(false), _) => false,
            (Some(true), Some(false)) => false,
            (Some(true), _) | (None, _) => true,
        };
        if blocks {
            live_owners.push(OpsKeeperPlanOwnerEvidence {
                state_file: state_file.to_path_buf(),
                owner_kind: owner_kind.to_string(),
                process_field: field.to_string(),
                process_id,
                process_alive,
                process_instance_matches,
            });
        }
    }
}

fn write_supervisor_stop_marker(
    harness_home: &Path,
    component: &str,
    reason: &str,
    now_ms: i64,
) -> io::Result<()> {
    let stop_file = crate::loop_health::supervisor_stop_file_path(harness_home, component);
    write_supervisor_stop_marker_at(&stop_file, component, reason, now_ms)
}

fn write_supervisor_stop_marker_at(
    stop_file_path: &Path,
    component: &str,
    reason: &str,
    now_ms: i64,
) -> io::Result<()> {
    if let Some(parent) = stop_file_path.parent() {
        fs::create_dir_all(parent)?;
    }
    crate::write_json_atomic(
        stop_file_path,
        &serde_json::json!({
            "schema": "agent-harness.supervisor-stop-file.v1",
            "serviceId": component,
            "reason": reason,
            "createdBy": "ops-control",
            "createdAtMs": now_ms,
            "persistent": true
        }),
    )
}

fn signal_stop_wake_lanes(harness_home: &Path, stop_files: &[OpsStopFileStatus]) {
    let mut lanes = BTreeSet::new();
    for stop in stop_files {
        if let Some(lane) = wake_lane_for_stop_component(&stop.component) {
            lanes.insert(lane);
        }
    }
    for lane in lanes {
        let wake_file = harness_home
            .join("state")
            .join("wake")
            .join(format!("{lane}.json"));
        let _ =
            crate::wake::signal_wake(harness_home, wake_file, lane, "ops-control stop requested");
    }
}

fn wake_lane_for_stop_component(component: &str) -> Option<&'static str> {
    match component {
        "discord-outbox-loop" => Some("final-outbox"),
        "progress-delivery-loop" => Some("progress-delivery"),
        "runtime-loop" => Some("runtime"),
        "worker-loop" => Some("worker"),
        _ => None,
    }
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

fn discover_stop_files(harness_home: &Path) -> io::Result<OpsStopFileDiscovery> {
    if let Some(config) = read_ops_harness_config(harness_home)? {
        let reconcile_managed = config
            .get("supervisor")
            .and_then(|value| value.get("manageAllLoops"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if reconcile_managed {
            let mut stops = crate::supervisor_topology::derive_supervisor_topology(
                &config,
                &crate::supervisor_topology::SupervisorTopologyOverrides::default(),
            )?
            .into_iter()
            .map(|(component, _)| {
                let stop_file =
                    crate::loop_health::supervisor_stop_file_path(harness_home, &component);
                stop_file_status(component, stop_file)
            })
            .collect::<Vec<_>>();
            stops.push(stop_file_status(
                RECONCILE_KEEPER_STOP_COMPONENT.to_string(),
                crate::loop_health::supervisor_stop_file_path(
                    harness_home,
                    RECONCILE_KEEPER_STOP_COMPONENT,
                ),
            ));
            return Ok(OpsStopFileDiscovery {
                ownership_mode: OpsSupervisorOwnershipMode::ReconcileManaged,
                stop_files: normalize_stop_files(stops),
            });
        }
    }

    discover_scheduled_task_stop_files(harness_home)
}

fn discover_stop_files_for_control(harness_home: &Path) -> (OpsStopFileDiscovery, Vec<String>) {
    let keeper_evidence = reconcile_keeper_evidence_exists(harness_home);
    let mut errors = Vec::new();
    let mut discovery = match discover_stop_files(harness_home) {
        Ok(discovery) => discovery,
        Err(error) => {
            errors.push(format!(
                "failed to derive current supervisor control topology: {error}"
            ));
            match discover_scheduled_task_stop_files(harness_home) {
                Ok(discovery) => discovery,
                Err(fallback_error) => {
                    errors.push(format!(
                        "failed to derive scheduled-task fallback control topology: {fallback_error}"
                    ));
                    OpsStopFileDiscovery {
                        ownership_mode: OpsSupervisorOwnershipMode::ScheduledTask,
                        stop_files: Vec::new(),
                    }
                }
            }
        }
    };

    if discovery.ownership_mode == OpsSupervisorOwnershipMode::ReconcileManaged || keeper_evidence {
        let (observed, observed_errors) = discover_observed_reconcile_stop_files(harness_home);
        discovery.stop_files.extend(observed);
        errors.extend(observed_errors);
    }
    if keeper_evidence {
        discovery.ownership_mode = OpsSupervisorOwnershipMode::ReconcileManaged;
        discovery.stop_files.push(stop_file_status(
            RECONCILE_KEEPER_STOP_COMPONENT.to_string(),
            crate::loop_health::supervisor_stop_file_path(
                harness_home,
                RECONCILE_KEEPER_STOP_COMPONENT,
            ),
        ));
    }
    discovery.stop_files = normalize_stop_files(discovery.stop_files);
    (discovery, errors)
}

fn reconcile_keeper_evidence_exists(harness_home: &Path) -> bool {
    let supervisor = harness_home.join("state").join("supervisor");
    [
        supervisor.join("reconcile-keeper.json"),
        supervisor
            .join("windows-reconcile-keeper")
            .join("keeper-plan.json"),
        supervisor
            .join("windows-reconcile-keeper")
            .join("readback-latest.json"),
        crate::loop_health::supervisor_stop_file_path(
            harness_home,
            RECONCILE_KEEPER_STOP_COMPONENT,
        ),
    ]
    .iter()
    .any(|path| path.is_file())
}

fn discover_observed_reconcile_stop_files(
    harness_home: &Path,
) -> (Vec<OpsStopFileStatus>, Vec<String>) {
    let services_dir = harness_home
        .join("state")
        .join("supervisor")
        .join("services");
    let entries = match fs::read_dir(&services_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return (Vec::new(), Vec::new());
        }
        Err(error) => {
            return (
                Vec::new(),
                vec![format!(
                    "failed to read observed supervisor service identities from {}: {error}",
                    services_dir.display()
                )],
            );
        }
    };
    let mut stops = Vec::new();
    let mut errors = Vec::new();
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                errors.push(format!(
                    "failed to enumerate an observed supervisor service state: {error}"
                ));
                continue;
            }
        };
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        // `write_json_atomic` stages service state beside the canonical file as
        // `.<name>.json.<pid>.<time>.<sequence>.tmp` and may also leave its
        // `.atomic.lock` visible during replacement. Only `<service-id>.json`
        // is authoritative supervisor topology; transient siblings must never
        // become control targets or turn a safe stop into a partial receipt.
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        let value = match fs::read_to_string(&path) {
            Ok(text) => match serde_json::from_str::<Value>(&text) {
                Ok(value) => Some(value),
                Err(error) => {
                    errors.push(format!(
                        "observed supervisor service state is invalid JSON at {}: {error}",
                        path.display()
                    ));
                    None
                }
            },
            Err(error) => {
                errors.push(format!(
                    "failed to read observed supervisor service state at {}: {error}",
                    path.display()
                ));
                None
            }
        };
        let service_id = value
            .as_ref()
            .and_then(|value| value.get("serviceId"))
            .and_then(Value::as_str)
            .map(ToString::to_string)
            .or_else(|| {
                path.file_stem()
                    .and_then(|name| name.to_str())
                    .map(ToString::to_string)
            });
        let Some(service_id) = service_id else {
            errors.push(format!(
                "observed supervisor service state has no safe identity: {}",
                path.display()
            ));
            continue;
        };
        if let Some(file_stem) = path.file_stem().and_then(|name| name.to_str()) {
            if value.is_some() && file_stem != service_id {
                errors.push(format!(
                    "observed supervisor service identity `{service_id}` does not match state filename `{file_stem}` at {}",
                    path.display()
                ));
            }
        }
        if let Err(error) = crate::supervisor_topology::validate_supervisor_service_id(&service_id)
        {
            errors.push(format!(
                "ignored unsafe observed supervisor service identity in {}: {error}",
                path.display()
            ));
            continue;
        }
        stops.push(stop_file_status(
            service_id.clone(),
            crate::loop_health::supervisor_stop_file_path(harness_home, &service_id),
        ));
    }
    (stops, errors)
}

fn read_ops_harness_config(harness_home: &Path) -> io::Result<Option<Value>> {
    let Some(config_file) = crate::config::harness_config_candidates(harness_home)
        .into_iter()
        .find(|path| path.is_file())
    else {
        return Ok(None);
    };
    let text = fs::read_to_string(&config_file)?;
    serde_json::from_str(&text).map(Some).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid JSON in {}: {error}", config_file.display()),
        )
    })
}

fn discover_scheduled_task_stop_files(harness_home: &Path) -> io::Result<OpsStopFileDiscovery> {
    let mut stops = Vec::new();
    let plan_file = harness_home
        .join("state")
        .join("supervisor")
        .join("windows-scheduled-tasks")
        .join("supervisor-plan.json");
    if let Ok(text) = fs::read_to_string(&plan_file) {
        if let Ok(value) = serde_json::from_str::<Value>(&text) {
            stops.extend(
                value
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
                    }),
            );
        }
    }

    let direct_stop_dir = harness_home.join("state").join("supervisor").join("stop");
    for component in LEGACY_SCHEDULED_DIRECT_STOP_COMPONENTS {
        stops.push(stop_file_status(
            (*component).to_string(),
            direct_stop_dir.join(format!("{component}.stop")),
        ));
    }

    let stop_dirs = [
        harness_home
            .join("state")
            .join("supervisor")
            .join("windows-scheduled-tasks")
            .join("stop"),
        harness_home.join("state").join("supervisor").join("stop"),
    ];
    for stop_dir in stop_dirs {
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
    }
    Ok(OpsStopFileDiscovery {
        ownership_mode: OpsSupervisorOwnershipMode::ScheduledTask,
        stop_files: normalize_stop_files(stops),
    })
}

fn normalize_stop_files(mut stops: Vec<OpsStopFileStatus>) -> Vec<OpsStopFileStatus> {
    stops.sort_by(|left, right| {
        left.component
            .cmp(&right.component)
            .then_with(|| left.stop_file.cmp(&right.stop_file))
    });
    stops.dedup_by(|left, right| {
        left.component == right.component && left.stop_file == right.stop_file
    });
    stops
}

fn refresh_stop_file_statuses(stops: &[OpsStopFileStatus]) -> Vec<OpsStopFileStatus> {
    stops
        .iter()
        .map(|stop| stop_file_status(stop.component.clone(), stop.stop_file.clone()))
        .collect()
}

fn same_stop_target_set(left: &[OpsStopFileStatus], right: &[OpsStopFileStatus]) -> bool {
    let targets = |stops: &[OpsStopFileStatus]| {
        stops
            .iter()
            .map(|stop| (stop.component.clone(), stop.stop_file.clone()))
            .collect::<BTreeSet<_>>()
    };
    targets(left) == targets(right)
}

fn ordered_control_targets(
    stop_files: &[OpsStopFileStatus],
    action: OpsControlAction,
    reconcile_managed: bool,
) -> Vec<&OpsStopFileStatus> {
    let mut targets = stop_files.iter().collect::<Vec<_>>();
    targets.sort_by(|left, right| {
        let keeper_rank = |component: &str| match (reconcile_managed, action) {
            (true, OpsControlAction::Stop) => {
                usize::from(component != RECONCILE_KEEPER_STOP_COMPONENT)
            }
            (true, OpsControlAction::Start) => {
                usize::from(component == RECONCILE_KEEPER_STOP_COMPONENT)
            }
            _ => 0,
        };
        keeper_rank(&left.component)
            .cmp(&keeper_rank(&right.component))
            .then_with(|| left.component.cmp(&right.component))
            .then_with(|| left.stop_file.cmp(&right.stop_file))
    });
    targets
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
        fs::write(
            harness_home.join("harness-config.json"),
            r#"{"supervisor":{"manageAllLoops":false}}"#,
        )
        .unwrap();
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
        assert!(
            stop.stop_files
                .iter()
                .all(|entry| entry.component != RECONCILE_KEEPER_STOP_COMPONENT)
        );
        let scheduled_worker_stop = stop
            .stop_files
            .iter()
            .find(|entry| entry.stop_file == stop_file)
            .unwrap();
        assert!(scheduled_worker_stop.present);
        let direct_worker_stop_file = harness_home
            .join("state")
            .join("supervisor")
            .join("stop")
            .join("worker-loop.stop");
        let direct_worker_stop = stop
            .stop_files
            .iter()
            .find(|entry| entry.stop_file == direct_worker_stop_file)
            .unwrap();
        assert!(direct_worker_stop.present);
        let stop_file_json: Value =
            serde_json::from_slice(&fs::read(&direct_worker_stop.stop_file).unwrap()).unwrap();
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
        assert!(start.stop_files.iter().all(|entry| !entry.present));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn live_home_ops_control_requires_token_from_an_ordinary_shell() {
        let root = temp_root("live_home_ops_control_requires_token_from_an_ordinary_shell");
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "supervisor": {
                    "manageAllLoops": true,
                    "runtimeLoop": { "enabled": true },
                    "workerLoop": { "enabled": false },
                    "cronSchedulerLoop": { "enabled": false },
                    "progressDeliveryLoop": { "enabled": false },
                    "ledgerMaintenanceLoop": { "enabled": false },
                    "telegramLoop": { "enabled": false },
                    "discordOutboxLoop": { "enabled": false },
                    "discordGatewayLoop": { "enabled": false }
                }
            }))
            .unwrap(),
        )
        .unwrap();

        for token in [None, Some("not-a-live-control-token".to_string())] {
            let blocked = record_ops_control(OpsControlOptions {
                harness_home: harness_home.clone(),
                action: OpsControlAction::Stop,
                reason: None,
                live_control_token: token,
                now_ms: 1000,
            })
            .unwrap();
            assert_eq!(blocked.status, "blocked-live-control");
            assert!(
                blocked
                    .reason
                    .contains("destructive control against a live harness home")
            );
            assert!(blocked.stop_files.iter().all(|entry| !entry.present));
            assert!(
                !blocked.receipt_file.exists(),
                "invalid live-control authorization must not create even an ops receipt"
            );
        }

        let stop_request = record_ops_cutover_request(OpsCutoverRequestOptions {
            harness_home: harness_home.clone(),
            action: LiveControlAction::Stop,
            summary: Some("authorize exact stop".to_string()),
            candidate_binary: None,
            staging_home: None,
            test_notes: vec!["normal-shell bypass regression".to_string()],
            now_ms: 1001,
        })
        .unwrap();
        let stop_approval = record_ops_cutover_approval(OpsCutoverApproveOptions {
            harness_home: harness_home.clone(),
            ticket_id: stop_request.ticket_id,
            action: LiveControlAction::Stop,
            issued_to: Some("operator".to_string()),
            ttl_seconds: Some(60),
            reason: None,
            now_ms: 1002,
        })
        .unwrap();
        let stop_token = stop_approval.token.unwrap();
        let stopped = record_ops_control(OpsControlOptions {
            harness_home: harness_home.clone(),
            action: OpsControlAction::Stop,
            reason: None,
            live_control_token: Some(stop_token.clone()),
            now_ms: 1003,
        })
        .unwrap();
        assert_eq!(stopped.status, "stop-requested");
        assert!(stopped.stop_files.iter().all(|entry| entry.present));

        let wrong_action = record_ops_control(OpsControlOptions {
            harness_home: harness_home.clone(),
            action: OpsControlAction::Start,
            reason: None,
            live_control_token: Some(stop_token),
            now_ms: 1004,
        })
        .unwrap();
        assert_eq!(wrong_action.status, "blocked-live-control");
        assert!(wrong_action.stop_files.iter().all(|entry| entry.present));

        let cutover_request = record_ops_cutover_request(OpsCutoverRequestOptions {
            harness_home: harness_home.clone(),
            action: LiveControlAction::Cutover,
            summary: Some("authorize guarded start".to_string()),
            candidate_binary: None,
            staging_home: None,
            test_notes: vec!["cutover token covers start".to_string()],
            now_ms: 1005,
        })
        .unwrap();
        let cutover_approval = record_ops_cutover_approval(OpsCutoverApproveOptions {
            harness_home: harness_home.clone(),
            ticket_id: cutover_request.ticket_id,
            action: LiveControlAction::Cutover,
            issued_to: Some("operator".to_string()),
            ttl_seconds: Some(60),
            reason: None,
            now_ms: 1006,
        })
        .unwrap();
        let started = record_ops_control(OpsControlOptions {
            harness_home: harness_home.clone(),
            action: OpsControlAction::Start,
            reason: None,
            live_control_token: cutover_approval.token,
            now_ms: 1007,
        })
        .unwrap();
        assert_eq!(started.status, "start-ready");
        assert!(started.stop_files.iter().all(|entry| !entry.present));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn keeper_plan_preflight_requires_every_hold_and_no_live_owner() {
        let root = temp_root("keeper_plan_preflight_requires_hold_and_no_owner");
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "supervisor": {
                    "manageAllLoops": true,
                    "runtimeLoop": { "enabled": true },
                    "workerLoop": { "enabled": false },
                    "cronSchedulerLoop": { "enabled": false },
                    "progressDeliveryLoop": { "enabled": false },
                    "ledgerMaintenanceLoop": { "enabled": false },
                    "telegramLoop": { "enabled": false },
                    "discordOutboxLoop": { "enabled": false },
                    "discordGatewayLoop": { "enabled": false }
                }
            }))
            .unwrap(),
        )
        .unwrap();

        let unheld = collect_ops_keeper_plan_preflight(&harness_home);
        assert!(!unheld.ready);
        assert!(unheld.live_owners.is_empty());
        assert!(
            unheld
                .reason
                .contains("current topology is not held: missing")
        );

        for component in [RECONCILE_KEEPER_STOP_COMPONENT, "runtime-loop"] {
            write_supervisor_stop_marker(&harness_home, component, "fixture hold", 1000).unwrap();
        }
        let held = collect_ops_keeper_plan_preflight(&harness_home);
        assert!(held.ready, "{}", held.reason);
        assert_eq!(held.stop_files.len(), 2);

        let state_file = harness_home
            .join("state")
            .join("supervisor")
            .join("reconcile-keeper.json");
        let process_id = i64::from(std::process::id());
        crate::write_json_atomic(
            &state_file,
            &serde_json::json!({
                "schema": "agent-harness.supervisor-reconcile-keeper.v1",
                "pid": process_id,
                "processStartTimeMs":
                    crate::loop_health::process_start_time_ms_for_pid(process_id)
                        .expect("current process start time")
            }),
        )
        .unwrap();
        let running = collect_ops_keeper_plan_preflight(&harness_home);
        assert!(!running.ready);
        assert_eq!(running.live_owners.len(), 1);
        assert_eq!(running.live_owners[0].owner_kind, "reconcile-keeper");
        assert_eq!(running.live_owners[0].process_id, process_id);
        assert_eq!(running.live_owners[0].process_instance_matches, Some(true));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn keeper_plan_preflight_fails_closed_on_corrupt_owner_state() {
        let root = temp_root("keeper_plan_preflight_corrupt_owner_state");
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(
            harness_home
                .join("state")
                .join("supervisor")
                .join("services"),
        )
        .unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            br#"{"supervisor":{"manageAllLoops":true}}"#,
        )
        .unwrap();
        for component in [
            RECONCILE_KEEPER_STOP_COMPONENT,
            "cron-scheduler-loop",
            "discord-gateway-loop",
            "discord-outbox-loop",
            "ledger-maintenance-loop",
            "progress-delivery-loop",
            "runtime-loop",
            "telegram-loop",
            "worker-loop",
        ] {
            write_supervisor_stop_marker(&harness_home, component, "fixture hold", 1000).unwrap();
        }
        fs::write(
            harness_home
                .join("state")
                .join("supervisor")
                .join("services")
                .join("runtime-loop.json"),
            b"{not-json}",
        )
        .unwrap();

        let report = collect_ops_keeper_plan_preflight(&harness_home);
        assert!(!report.ready);
        assert!(
            report
                .reason
                .contains("service-owner state is invalid JSON")
        );

        let _ = fs::remove_dir_all(root);
    }

    // Contract: I26 reconcile-managed control follows the configured desired topology.
    // Source: GitHub issue #17, reduced to a hermetic config/file-state replay.
    // Fails on: the legacy eight-component fallback omits ledger maintenance.
    // Asserts: stop/status/start cover every configured child plus the reconcile keeper.
    #[test]
    fn ops_control_reconcile_managed_targets_follow_configured_topology() {
        let root = temp_root("ops_control_reconcile_managed_targets_follow_configured_topology");
        let harness_home = root.join("harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "supervisor": {
                    "manageAllLoops": true,
                    "telegramLoops": [
                        {
                            "enabled": true,
                            "serviceId": "telegram-loop-xiaoxiaoli",
                            "telegramAccount": "xiaoxiaoli"
                        }
                    ]
                }
            }))
            .unwrap(),
        )
        .unwrap();

        let expected = vec![
            "cron-scheduler-loop",
            "discord-gateway-loop",
            "discord-outbox-loop",
            "ledger-maintenance-loop",
            "progress-delivery-loop",
            "runtime-loop",
            "supervisor-reconcile",
            "telegram-loop",
            "telegram-loop-xiaoxiaoli",
            "worker-loop",
        ];
        let stop = record_ops_control(OpsControlOptions {
            harness_home: harness_home.clone(),
            action: OpsControlAction::Stop,
            reason: None,
            live_control_token: None,
            now_ms: 1000,
        })
        .unwrap();
        assert_eq!(stop.status, "stop-requested");
        assert_eq!(
            stop.stop_files
                .iter()
                .map(|entry| entry.component.as_str())
                .collect::<Vec<_>>(),
            expected
        );
        assert!(stop.stop_files.iter().all(|entry| entry.present));
        assert!(
            harness_home
                .join("state")
                .join("supervisor")
                .join("stop")
                .join("ledger-maintenance-loop.stop")
                .is_file()
        );
        assert!(
            harness_home
                .join("state")
                .join("supervisor")
                .join("stop")
                .join("supervisor-reconcile.stop")
                .is_file()
        );

        let status = record_ops_control(OpsControlOptions {
            harness_home: harness_home.clone(),
            action: OpsControlAction::Status,
            reason: None,
            live_control_token: None,
            now_ms: 1001,
        })
        .unwrap();
        assert_eq!(status.status, "observed");
        assert_eq!(
            status
                .stop_files
                .iter()
                .map(|entry| entry.component.as_str())
                .collect::<Vec<_>>(),
            expected
        );
        assert!(status.stop_files.iter().all(|entry| entry.present));

        let start = record_ops_control(OpsControlOptions {
            harness_home,
            action: OpsControlAction::Start,
            reason: None,
            live_control_token: None,
            now_ms: 1002,
        })
        .unwrap();
        assert_eq!(start.status, "start-ready");
        assert_eq!(start.stop_files.len(), expected.len());
        assert!(start.stop_files.iter().all(|entry| !entry.present));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn ops_control_reconcile_managed_targets_honor_overrides_and_telegram_accounts() {
        let root = temp_root(
            "ops_control_reconcile_managed_targets_honor_overrides_and_telegram_accounts",
        );
        let harness_home = root.join("harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "supervisor": {
                    "manageAllLoops": true,
                    "services": [
                        {
                            "enabled": true,
                            "serviceId": "runtime-loop",
                            "serviceKind": "runtime",
                            "args": [],
                            "priority": "critical",
                            "restartDelayMs": 1000,
                            "heartbeatTimeoutMs": 120000
                        },
                        {
                            "enabled": false,
                            "serviceId": "ledger-maintenance-loop",
                            "serviceKind": "ledger-maintenance",
                            "args": [],
                            "priority": "low",
                            "restartDelayMs": 1000,
                            "heartbeatTimeoutMs": 120000
                        }
                    ],
                    "runtimeLoop": { "enabled": false },
                    "workerLoop": { "enabled": true },
                    "cronSchedulerLoop": { "enabled": false },
                    "progressDeliveryLoop": { "enabled": true },
                    "ledgerMaintenanceLoop": { "enabled": false },
                    "telegramLoop": { "enabled": false },
                    "discordOutboxLoop": { "enabled": true },
                    "discordGatewayLoop": { "enabled": false },
                    "telegramLoops": [
                        { "enabled": true, "account": "North Desk" },
                        { "enabled": true, "serviceId": "telegram-loop-blue" },
                        { "enabled": false, "serviceId": "telegram-loop-disabled" }
                    ]
                }
            }))
            .unwrap(),
        )
        .unwrap();

        let stop = record_ops_control(OpsControlOptions {
            harness_home,
            action: OpsControlAction::Stop,
            reason: None,
            live_control_token: None,
            now_ms: 1000,
        })
        .unwrap();
        assert_eq!(stop.status, "stop-requested");
        assert_eq!(
            stop.stop_files
                .iter()
                .map(|entry| entry.component.as_str())
                .collect::<Vec<_>>(),
            vec![
                "discord-outbox-loop",
                "progress-delivery-loop",
                "runtime-loop",
                "supervisor-reconcile",
                "telegram-loop-blue",
                "telegram-loop-north-desk",
                "worker-loop",
            ]
        );

        let _ = fs::remove_dir_all(root);
    }

    // Contract: partial live-control mutation must produce a fail-closed receipt.
    // Source: GitHub issue #17 acceptance case, with one deterministic blocked marker.
    // Fails on: the first filesystem error aborts without recording partial control.
    // Asserts: the receipt is partial and the reconcile keeper remains held on failed start.
    #[test]
    fn ops_control_reconcile_managed_receipt_fails_closed_on_partial_mutation() {
        let root =
            temp_root("ops_control_reconcile_managed_receipt_fails_closed_on_partial_mutation");
        let harness_home = root.join("harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "supervisor": {
                    "manageAllLoops": true,
                    "runtimeLoop": { "enabled": true },
                    "workerLoop": { "enabled": false },
                    "cronSchedulerLoop": { "enabled": false },
                    "progressDeliveryLoop": { "enabled": false },
                    "ledgerMaintenanceLoop": { "enabled": true },
                    "telegramLoop": { "enabled": false },
                    "discordOutboxLoop": { "enabled": false },
                    "discordGatewayLoop": { "enabled": false }
                }
            }))
            .unwrap(),
        )
        .unwrap();
        let blocked_target = harness_home
            .join("state")
            .join("supervisor")
            .join("stop")
            .join("ledger-maintenance-loop.stop");
        fs::create_dir_all(&blocked_target).unwrap();

        let stop = record_ops_control(OpsControlOptions {
            harness_home: harness_home.clone(),
            action: OpsControlAction::Stop,
            reason: None,
            live_control_token: None,
            now_ms: 1000,
        })
        .unwrap();
        assert_eq!(stop.status, "partial-stop-request");
        assert_eq!(stop.stop_files.len(), 3);
        assert!(
            stop.stop_files
                .iter()
                .find(|entry| entry.component == "runtime-loop")
                .is_some_and(|entry| entry.present)
        );
        assert!(
            stop.stop_files
                .iter()
                .find(|entry| entry.component == "ledger-maintenance-loop")
                .is_some_and(|entry| !entry.present)
        );
        assert!(stop.receipt_file.is_file());

        let start = record_ops_control(OpsControlOptions {
            harness_home: harness_home.clone(),
            action: OpsControlAction::Start,
            reason: None,
            live_control_token: None,
            now_ms: 1001,
        })
        .unwrap();
        assert_eq!(start.status, "partial-start-ready");
        assert!(
            start
                .stop_files
                .iter()
                .find(|entry| entry.component == RECONCILE_KEEPER_STOP_COMPONENT)
                .is_some_and(|entry| entry.present)
        );
        assert!(
            start
                .reason
                .contains("retained supervisor-reconcile stop marker")
        );
        assert!(
            crate::loop_health::supervisor_stop_file_path(
                &harness_home,
                RECONCILE_KEEPER_STOP_COMPONENT
            )
            .is_file(),
            "partial start must preserve or recreate the global keeper hold"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn ops_control_stop_precommits_keeper_hold_when_config_is_malformed() {
        let root = temp_root("ops_control_stop_precommits_keeper_hold_on_bad_config");
        let harness_home = root.join("harness");
        fs::create_dir_all(
            harness_home
                .join("state")
                .join("supervisor")
                .join("windows-reconcile-keeper"),
        )
        .unwrap();
        fs::write(harness_home.join("harness-config.json"), b"{not-json}").unwrap();
        fs::write(
            harness_home
                .join("state")
                .join("supervisor")
                .join("windows-reconcile-keeper")
                .join("keeper-plan.json"),
            b"{}",
        )
        .unwrap();
        let services = harness_home
            .join("state")
            .join("supervisor")
            .join("services");
        fs::create_dir_all(&services).unwrap();
        fs::write(
            services.join("custom-loop.json"),
            br#"{"serviceId":"custom-loop"}"#,
        )
        .unwrap();

        let report = record_ops_control(OpsControlOptions {
            harness_home: harness_home.clone(),
            action: OpsControlAction::Stop,
            reason: None,
            live_control_token: None,
            now_ms: 2000,
        })
        .unwrap();

        assert_eq!(report.status, "partial-stop-request");
        assert!(report.reason.contains("invalid JSON"));
        for component in [RECONCILE_KEEPER_STOP_COMPONENT, "custom-loop"] {
            assert!(
                crate::loop_health::supervisor_stop_file_path(&harness_home, component).is_file(),
                "missing fail-safe stop marker for {component}"
            );
        }
        let keeper_marker: Value = serde_json::from_slice(
            &fs::read(crate::loop_health::supervisor_stop_file_path(
                &harness_home,
                RECONCILE_KEEPER_STOP_COMPONENT,
            ))
            .unwrap(),
        )
        .unwrap();
        assert_eq!(keeper_marker["createdAtMs"], 2000);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn ops_control_start_refuses_all_clears_when_topology_is_invalid() {
        let root = temp_root("ops_control_start_refuses_invalid_topology");
        let harness_home = root.join("harness");
        fs::create_dir_all(
            harness_home
                .join("state")
                .join("supervisor")
                .join("windows-reconcile-keeper"),
        )
        .unwrap();
        fs::write(harness_home.join("harness-config.json"), b"{not-json}").unwrap();
        fs::write(
            harness_home
                .join("state")
                .join("supervisor")
                .join("windows-reconcile-keeper")
                .join("keeper-plan.json"),
            b"{}",
        )
        .unwrap();
        for component in [RECONCILE_KEEPER_STOP_COMPONENT, "runtime-loop"] {
            write_supervisor_stop_marker(&harness_home, component, "fixture", 1000).unwrap();
        }

        let report = record_ops_control(OpsControlOptions {
            harness_home: harness_home.clone(),
            action: OpsControlAction::Start,
            reason: None,
            live_control_token: None,
            now_ms: 2000,
        })
        .unwrap();

        assert_eq!(report.status, "partial-start-ready");
        assert!(report.reason.contains("refused to clear any stop marker"));
        for component in [RECONCILE_KEEPER_STOP_COMPONENT, "runtime-loop"] {
            assert!(
                crate::loop_health::supervisor_stop_file_path(&harness_home, component).is_file()
            );
        }

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn ops_control_start_refuses_all_clears_when_observed_identity_is_malformed() {
        let root = temp_root("ops_control_start_refuses_malformed_observed_identity");
        let harness_home = root.join("harness");
        fs::create_dir_all(
            harness_home
                .join("state")
                .join("supervisor")
                .join("windows-reconcile-keeper"),
        )
        .unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            br#"{"supervisor":{"manageAllLoops":true}}"#,
        )
        .unwrap();
        fs::write(
            harness_home
                .join("state")
                .join("supervisor")
                .join("windows-reconcile-keeper")
                .join("keeper-plan.json"),
            b"{}",
        )
        .unwrap();
        let services = harness_home
            .join("state")
            .join("supervisor")
            .join("services");
        fs::create_dir_all(&services).unwrap();
        fs::write(services.join("custom-loop.json"), b"{not-json}").unwrap();
        for component in [RECONCILE_KEEPER_STOP_COMPONENT, "runtime-loop"] {
            write_supervisor_stop_marker(&harness_home, component, "fixture", 1000).unwrap();
        }

        let report = record_ops_control(OpsControlOptions {
            harness_home: harness_home.clone(),
            action: OpsControlAction::Start,
            reason: None,
            live_control_token: None,
            now_ms: 2000,
        })
        .unwrap();

        assert_eq!(report.status, "partial-start-ready");
        assert!(
            report
                .reason
                .contains("observed supervisor service state is invalid JSON")
        );
        assert!(report.reason.contains("refused to clear any stop marker"));
        for component in [RECONCILE_KEEPER_STOP_COMPONENT, "runtime-loop"] {
            assert!(
                crate::loop_health::supervisor_stop_file_path(&harness_home, component).is_file()
            );
        }

        let _ = fs::remove_dir_all(root);
    }

    // Contract: atomic-write staging files are not authoritative supervisor topology.
    // Source: the Windows keeper stop replay observed a valid atomic temp file between
    // write and rename while the canonical service state remained present.
    // Fails on: every regular file in the services directory is parsed as canonical state.
    // Asserts: only canonical `<service-id>.json` files influence fail-closed control.
    #[test]
    fn ops_control_ignores_atomic_service_state_temp_files() {
        let root = temp_root("ops_control_ignores_atomic_service_state_temp_files");
        let harness_home = root.join("harness");
        fs::create_dir_all(
            harness_home
                .join("state")
                .join("supervisor")
                .join("windows-reconcile-keeper"),
        )
        .unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            br#"{"supervisor":{"manageAllLoops":true}}"#,
        )
        .unwrap();
        fs::write(
            harness_home
                .join("state")
                .join("supervisor")
                .join("windows-reconcile-keeper")
                .join("keeper-plan.json"),
            b"{}",
        )
        .unwrap();
        let services = harness_home
            .join("state")
            .join("supervisor")
            .join("services");
        fs::create_dir_all(&services).unwrap();
        let canonical = br#"{"serviceId":"telegram-loop"}"#;
        fs::write(services.join("telegram-loop.json"), canonical).unwrap();
        fs::write(
            services.join(".telegram-loop.json.4242.2000.1.tmp"),
            canonical,
        )
        .unwrap();

        let report = record_ops_control(OpsControlOptions {
            harness_home: harness_home.clone(),
            action: OpsControlAction::Stop,
            reason: None,
            live_control_token: None,
            now_ms: 2000,
        })
        .unwrap();

        assert_eq!(report.status, "stop-requested", "{}", report.reason);
        assert_eq!(
            report
                .stop_files
                .iter()
                .filter(|entry| entry.component == "telegram-loop")
                .count(),
            1
        );
        for component in [RECONCILE_KEEPER_STOP_COMPONENT, "telegram-loop"] {
            assert!(
                crate::loop_health::supervisor_stop_file_path(&harness_home, component).is_file()
            );
        }

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn ops_control_rejects_canonical_service_state_identity_mismatch() {
        let root = temp_root("ops_control_rejects_canonical_service_state_identity_mismatch");
        let harness_home = root.join("harness");
        fs::create_dir_all(
            harness_home
                .join("state")
                .join("supervisor")
                .join("windows-reconcile-keeper"),
        )
        .unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            br#"{"supervisor":{"manageAllLoops":true}}"#,
        )
        .unwrap();
        fs::write(
            harness_home
                .join("state")
                .join("supervisor")
                .join("windows-reconcile-keeper")
                .join("keeper-plan.json"),
            b"{}",
        )
        .unwrap();
        let services = harness_home
            .join("state")
            .join("supervisor")
            .join("services");
        fs::create_dir_all(&services).unwrap();
        fs::write(
            services.join("telegram-loop.json"),
            br#"{"serviceId":"discord-gateway-loop"}"#,
        )
        .unwrap();

        let report = record_ops_control(OpsControlOptions {
            harness_home: harness_home.clone(),
            action: OpsControlAction::Stop,
            reason: None,
            live_control_token: None,
            now_ms: 2000,
        })
        .unwrap();

        assert_eq!(report.status, "partial-stop-request");
        assert!(report.reason.contains(
            "observed supervisor service identity `discord-gateway-loop` does not match state filename `telegram-loop`"
        ));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn ops_control_preserves_old_keeper_and_observed_children_after_mode_switch() {
        let root = temp_root("ops_control_preserves_keeper_after_mode_switch");
        let harness_home = root.join("harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            br#"{"supervisor":{"manageAllLoops":false}}"#,
        )
        .unwrap();
        fs::create_dir_all(harness_home.join("state").join("supervisor")).unwrap();
        fs::write(
            harness_home
                .join("state")
                .join("supervisor")
                .join("reconcile-keeper.json"),
            b"{}",
        )
        .unwrap();
        let services = harness_home
            .join("state")
            .join("supervisor")
            .join("services");
        fs::create_dir_all(&services).unwrap();
        fs::write(
            services.join("retired-custom-loop.json"),
            br#"{"serviceId":"retired-custom-loop"}"#,
        )
        .unwrap();

        let report = record_ops_control(OpsControlOptions {
            harness_home: harness_home.clone(),
            action: OpsControlAction::Stop,
            reason: None,
            live_control_token: None,
            now_ms: 2000,
        })
        .unwrap();

        assert_eq!(report.status, "stop-requested");
        for component in [RECONCILE_KEEPER_STOP_COMPONENT, "retired-custom-loop"] {
            assert!(
                crate::loop_health::supervisor_stop_file_path(&harness_home, component).is_file()
            );
        }

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn ops_control_reconcile_keeper_orders_stop_before_children_and_start_after_children() {
        let stop_dir = PathBuf::from("state").join("supervisor").join("stop");
        let targets = vec![
            stop_file_status("worker-loop".to_string(), stop_dir.join("worker-loop.stop")),
            stop_file_status(
                RECONCILE_KEEPER_STOP_COMPONENT.to_string(),
                stop_dir.join("supervisor-reconcile.stop"),
            ),
            stop_file_status(
                "runtime-loop".to_string(),
                stop_dir.join("runtime-loop.stop"),
            ),
        ];

        assert_eq!(
            ordered_control_targets(&targets, OpsControlAction::Stop, true)
                .into_iter()
                .map(|target| target.component.as_str())
                .collect::<Vec<_>>(),
            vec!["supervisor-reconcile", "runtime-loop", "worker-loop"]
        );
        assert_eq!(
            ordered_control_targets(&targets, OpsControlAction::Start, true)
                .into_iter()
                .map(|target| target.component.as_str())
                .collect::<Vec<_>>(),
            vec!["runtime-loop", "worker-loop", "supervisor-reconcile"]
        );
    }

    #[test]
    fn ops_control_stop_signals_delivery_wake_lanes() {
        let root = temp_root("ops_control_stop_signals_delivery_wake_lanes");
        let harness_home = root.join("harness");

        let stop = record_ops_control(OpsControlOptions {
            harness_home: harness_home.clone(),
            action: OpsControlAction::Stop,
            reason: None,
            live_control_token: None,
            now_ms: 1000,
        })
        .unwrap();

        assert_eq!(stop.status, "stop-requested");
        assert_eq!(
            crate::wake::read_wake_sequence(
                harness_home
                    .join("state")
                    .join("wake")
                    .join("progress-delivery.json")
            )
            .unwrap(),
            1
        );
        assert_eq!(
            crate::wake::read_wake_sequence(
                harness_home
                    .join("state")
                    .join("wake")
                    .join("final-outbox.json")
            )
            .unwrap(),
            1
        );

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
