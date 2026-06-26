use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

const SUBAGENT_LIFECYCLE_SCHEMA: &str = "agent-harness.subagent-lifecycle.v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SubagentLifecycleState {
    Queued,
    Running,
    Completed,
    Failed,
    TimedOut,
    Expired,
    AlreadyClosed,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubagentLifecycleShowOptions {
    pub harness_home: PathBuf,
    pub subagent_id: String,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubagentLifecycleRecordOptions {
    pub harness_home: PathBuf,
    pub subagent_id: String,
    pub state: SubagentLifecycleState,
    pub source: Option<String>,
    pub operation_plan_id: Option<String>,
    pub operation_plan_item_id: Option<String>,
    pub worker_job_id: Option<String>,
    pub runtime_queue_id: Option<String>,
    pub requested_model: Option<String>,
    pub resolved_model: Option<String>,
    pub provider: Option<String>,
    pub auth_lane: Option<String>,
    pub changed_files: Vec<String>,
    pub terminal_receipt_file: Option<PathBuf>,
    pub reason: String,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubagentLifecycleCloseOptions {
    pub harness_home: PathBuf,
    pub subagent_id: String,
    pub reason: String,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubagentLifecycleReceipt {
    pub schema: String,
    pub subagent_id: String,
    pub source: String,
    pub operation_plan_id: Option<String>,
    pub operation_plan_item_id: Option<String>,
    pub worker_job_id: Option<String>,
    pub runtime_queue_id: Option<String>,
    pub requested_model: Option<String>,
    pub resolved_model: Option<String>,
    pub provider: Option<String>,
    pub auth_lane: Option<String>,
    pub auth_visibility: String,
    #[serde(default = "default_auth_visibility_reason")]
    pub auth_visibility_reason: String,
    pub state: SubagentLifecycleState,
    pub cleanup: SubagentLifecycleCleanup,
    pub changed_files: Vec<String>,
    pub terminal_receipt_file: Option<PathBuf>,
    pub reason: String,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubagentLifecycleCleanup {
    pub close_requested: bool,
    pub cleanup_proven: bool,
    pub owner_path: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostic: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubagentLifecycleShowReport {
    pub receipt: SubagentLifecycleReceipt,
    pub receipts_file: PathBuf,
    pub snapshot_file: PathBuf,
}

pub fn subagent_lifecycle_receipts_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("subagents")
        .join("lifecycle-receipts.jsonl")
}

pub fn subagent_lifecycle_snapshot_file(
    harness_home: impl AsRef<Path>,
    subagent_id: &str,
) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("subagents")
        .join("lifecycle")
        .join(format!(
            "{}-{}.json",
            safe_file_part(subagent_id),
            fnv1a_64_hex(subagent_id)
        ))
}

pub fn show_subagent_lifecycle(
    options: SubagentLifecycleShowOptions,
) -> io::Result<SubagentLifecycleShowReport> {
    let snapshot_file =
        subagent_lifecycle_snapshot_file(&options.harness_home, &options.subagent_id);
    let receipts_file = subagent_lifecycle_receipts_file(&options.harness_home);
    let receipt = if snapshot_file.is_file() {
        let text = fs::read_to_string(&snapshot_file)?;
        serde_json::from_str::<SubagentLifecycleReceipt>(&text).map_err(io::Error::other)?
    } else if let Some(receipt) = latest_receipt_from_ledger(&receipts_file, &options.subagent_id)?
    {
        receipt
    } else {
        unknown_receipt(&options.harness_home, &options.subagent_id, options.now_ms)
    };

    Ok(SubagentLifecycleShowReport {
        receipt,
        receipts_file,
        snapshot_file,
    })
}

pub fn record_subagent_lifecycle(
    options: SubagentLifecycleRecordOptions,
) -> io::Result<SubagentLifecycleShowReport> {
    let snapshot_file =
        subagent_lifecycle_snapshot_file(&options.harness_home, &options.subagent_id);
    let receipts_file = subagent_lifecycle_receipts_file(&options.harness_home);
    let existing = read_existing_receipt(&snapshot_file)?;
    let created_at_ms = existing
        .as_ref()
        .map(|receipt| receipt.created_at_ms)
        .unwrap_or(options.now_ms);
    let provider = merge_option(
        options.provider,
        existing
            .as_ref()
            .and_then(|receipt| receipt.provider.clone()),
    );
    let auth_lane = merge_option(
        options.auth_lane,
        existing
            .as_ref()
            .and_then(|receipt| receipt.auth_lane.clone()),
    );
    let auth_visibility =
        auth_visibility(existing.as_ref(), provider.as_deref(), auth_lane.as_deref());
    let auth_visibility_reason = auth_visibility_reason(
        existing.as_ref(),
        &auth_visibility,
        provider.as_deref(),
        auth_lane.as_deref(),
    );

    let receipt = SubagentLifecycleReceipt {
        schema: SUBAGENT_LIFECYCLE_SCHEMA.to_string(),
        subagent_id: options.subagent_id,
        source: options
            .source
            .or_else(|| existing.as_ref().map(|receipt| receipt.source.clone()))
            .unwrap_or_else(|| "worker-dispatch".to_string()),
        operation_plan_id: merge_option(
            options.operation_plan_id,
            existing
                .as_ref()
                .and_then(|receipt| receipt.operation_plan_id.clone()),
        ),
        operation_plan_item_id: merge_option(
            options.operation_plan_item_id,
            existing
                .as_ref()
                .and_then(|receipt| receipt.operation_plan_item_id.clone()),
        ),
        worker_job_id: merge_option(
            options.worker_job_id,
            existing
                .as_ref()
                .and_then(|receipt| receipt.worker_job_id.clone()),
        ),
        runtime_queue_id: merge_option(
            options.runtime_queue_id,
            existing
                .as_ref()
                .and_then(|receipt| receipt.runtime_queue_id.clone()),
        ),
        requested_model: merge_option(
            options.requested_model,
            existing
                .as_ref()
                .and_then(|receipt| receipt.requested_model.clone()),
        ),
        resolved_model: merge_option(
            options.resolved_model,
            existing
                .as_ref()
                .and_then(|receipt| receipt.resolved_model.clone()),
        ),
        provider,
        auth_lane,
        auth_visibility,
        auth_visibility_reason,
        state: options.state,
        cleanup: existing
            .as_ref()
            .map(|receipt| receipt.cleanup.clone())
            .unwrap_or_else(|| SubagentLifecycleCleanup {
                close_requested: false,
                cleanup_proven: false,
                owner_path: worker_store_owner_path(&options.harness_home),
                diagnostic: None,
            }),
        changed_files: if options.changed_files.is_empty() {
            existing
                .as_ref()
                .map(|receipt| receipt.changed_files.clone())
                .unwrap_or_default()
        } else {
            options.changed_files
        },
        terminal_receipt_file: merge_option(
            options.terminal_receipt_file,
            existing
                .as_ref()
                .and_then(|receipt| receipt.terminal_receipt_file.clone()),
        ),
        reason: options.reason,
        created_at_ms,
        updated_at_ms: options.now_ms,
    };

    crate::write_json_atomic(&snapshot_file, &receipt)?;
    crate::append_jsonl_value(&receipts_file, &receipt)?;
    Ok(SubagentLifecycleShowReport {
        receipt,
        receipts_file,
        snapshot_file,
    })
}

pub fn close_subagent_lifecycle(
    options: SubagentLifecycleCloseOptions,
) -> io::Result<SubagentLifecycleShowReport> {
    let current = show_subagent_lifecycle(SubagentLifecycleShowOptions {
        harness_home: options.harness_home.clone(),
        subagent_id: options.subagent_id.clone(),
        now_ms: options.now_ms,
    })?;
    let mut receipt = current.receipt;
    let previous_state = receipt.state;
    let already_closed =
        receipt.cleanup.close_requested || receipt.state == SubagentLifecycleState::AlreadyClosed;
    let cleanup_proven = receipt.cleanup.cleanup_proven
        || terminal_receipt_file_exists(
            &options.harness_home,
            receipt.terminal_receipt_file.as_ref(),
        );
    receipt.schema = SUBAGENT_LIFECYCLE_SCHEMA.to_string();
    receipt.cleanup.close_requested = true;
    receipt.cleanup.cleanup_proven = cleanup_proven;
    receipt.cleanup.diagnostic = Some(close_diagnostic(
        previous_state,
        cleanup_proven,
        current.snapshot_file.is_file(),
        receipt.terminal_receipt_file.as_ref(),
    ));
    receipt.state = SubagentLifecycleState::AlreadyClosed;
    let mut reason = if already_closed {
        format!(
            "subagent lifecycle close already recorded: {}",
            options.reason
        )
    } else {
        options.reason
    };
    if !cleanup_proven {
        reason = format!("{reason}; cleanup proof unavailable");
    }
    receipt.reason = reason;
    receipt.updated_at_ms = options.now_ms;
    if receipt.created_at_ms == 0 {
        receipt.created_at_ms = options.now_ms;
    }

    let snapshot_file =
        subagent_lifecycle_snapshot_file(&options.harness_home, &options.subagent_id);
    let receipts_file = subagent_lifecycle_receipts_file(&options.harness_home);
    crate::write_json_atomic(&snapshot_file, &receipt)?;
    crate::append_jsonl_value(&receipts_file, &receipt)?;
    Ok(SubagentLifecycleShowReport {
        receipt,
        receipts_file,
        snapshot_file,
    })
}

fn terminal_receipt_file_exists(
    harness_home: &Path,
    terminal_receipt_file: Option<&PathBuf>,
) -> bool {
    terminal_receipt_file
        .map(|path| {
            let path = if path.is_absolute() {
                path.clone()
            } else {
                harness_home.join(path)
            };
            path.is_file()
        })
        .unwrap_or(false)
}

fn read_existing_receipt(path: &Path) -> io::Result<Option<SubagentLifecycleReceipt>> {
    if !path.is_file() {
        return Ok(None);
    }
    let text = fs::read_to_string(path)?;
    serde_json::from_str::<SubagentLifecycleReceipt>(&text)
        .map(Some)
        .map_err(io::Error::other)
}

fn unknown_receipt(
    harness_home: &Path,
    subagent_id: &str,
    now_ms: i64,
) -> SubagentLifecycleReceipt {
    SubagentLifecycleReceipt {
        schema: SUBAGENT_LIFECYCLE_SCHEMA.to_string(),
        subagent_id: subagent_id.to_string(),
        source: "external".to_string(),
        operation_plan_id: None,
        operation_plan_item_id: None,
        worker_job_id: None,
        runtime_queue_id: None,
        requested_model: None,
        resolved_model: None,
        provider: None,
        auth_lane: None,
        auth_visibility: "unverified".to_string(),
        auth_visibility_reason: default_auth_visibility_reason(),
        state: SubagentLifecycleState::Unknown,
        cleanup: SubagentLifecycleCleanup {
            close_requested: false,
            cleanup_proven: false,
            owner_path: worker_store_owner_path(harness_home),
            diagnostic: None,
        },
        changed_files: Vec::new(),
        terminal_receipt_file: None,
        reason: "no durable subagent lifecycle snapshot found for this id".to_string(),
        created_at_ms: now_ms,
        updated_at_ms: now_ms,
    }
}

fn worker_store_owner_path(harness_home: &Path) -> PathBuf {
    harness_home
        .join("state")
        .join("workers")
        .join("worker-jobs.sqlite")
}

fn latest_receipt_from_ledger(
    receipts_file: &Path,
    subagent_id: &str,
) -> io::Result<Option<SubagentLifecycleReceipt>> {
    if !receipts_file.is_file() {
        return Ok(None);
    }
    let text = fs::read_to_string(receipts_file)?;
    let mut latest = None;
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let receipt =
            serde_json::from_str::<SubagentLifecycleReceipt>(trimmed).map_err(io::Error::other)?;
        if receipt.subagent_id == subagent_id {
            latest = Some(receipt);
        }
    }
    Ok(latest)
}

fn merge_option<T>(new_value: Option<T>, existing_value: Option<T>) -> Option<T> {
    new_value.or(existing_value)
}

fn auth_visibility(
    existing: Option<&SubagentLifecycleReceipt>,
    provider: Option<&str>,
    auth_lane: Option<&str>,
) -> String {
    if provider.is_some() && auth_lane.is_some() {
        "receipt-visible".to_string()
    } else if provider.is_some() || auth_lane.is_some() {
        "partial".to_string()
    } else {
        existing
            .map(|receipt| receipt.auth_visibility.clone())
            .unwrap_or_else(|| "unverified".to_string())
    }
}

fn default_auth_visibility_reason() -> String {
    "provider/auth lane unavailable in lifecycle payload; Codex-auth status is unverified"
        .to_string()
}

fn auth_visibility_reason(
    existing: Option<&SubagentLifecycleReceipt>,
    visibility: &str,
    provider: Option<&str>,
    auth_lane: Option<&str>,
) -> String {
    match visibility {
        "receipt-visible" => {
            "provider and auth lane were recorded in lifecycle payload".to_string()
        }
        "partial" => format!(
            "provider/auth lane partially recorded in lifecycle payload; providerVisible={} authLaneVisible={}",
            provider.is_some(),
            auth_lane.is_some()
        ),
        _ => existing
            .map(|receipt| receipt.auth_visibility_reason.clone())
            .filter(|reason| !reason.trim().is_empty())
            .unwrap_or_else(default_auth_visibility_reason),
    }
}

fn close_diagnostic(
    previous_state: SubagentLifecycleState,
    cleanup_proven: bool,
    snapshot_exists: bool,
    terminal_receipt_file: Option<&PathBuf>,
) -> String {
    if cleanup_proven {
        return "close accepted idempotently; terminal receipt file exists".to_string();
    }
    if matches!(
        previous_state,
        SubagentLifecycleState::Completed
            | SubagentLifecycleState::Expired
            | SubagentLifecycleState::AlreadyClosed
    ) {
        return format!(
            "close accepted idempotently for terminal state {previous_state:?}; cleanup proof unavailable; snapshotExists={snapshot_exists} terminalReceiptFile={}",
            terminal_receipt_file
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "-".to_string())
        );
    }
    if previous_state == SubagentLifecycleState::Unknown {
        return format!(
            "close accepted idempotently for unknown or already-cleaned worker id; no durable owner was found; snapshotExists={snapshot_exists}"
        );
    }
    format!(
        "close requested for non-terminal state {previous_state:?}; cleanup proof unavailable; snapshotExists={snapshot_exists}"
    )
}

fn safe_file_part(value: &str) -> String {
    let mut out = String::new();
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        "subagent".to_string()
    } else {
        out
    }
}

fn fnv1a_64_hex(value: &str) -> String {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_root(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "agent_harness_subagent_lifecycle_{}_{}_{}",
            name,
            std::process::id(),
            crate::current_log_time_ms().unwrap_or(0)
        ))
    }

    #[test]
    fn subagent_lifecycle_show_returns_unknown_with_owner_for_missing_external_id() {
        let root = temp_root("show_unknown");
        let harness_home = root.join(".agent-harness");

        let report = show_subagent_lifecycle(SubagentLifecycleShowOptions {
            harness_home: harness_home.clone(),
            subagent_id: "external-tool-id".to_string(),
            now_ms: 10,
        })
        .unwrap();

        assert_eq!(report.receipt.state, SubagentLifecycleState::Unknown);
        assert_eq!(report.receipt.source, "external");
        assert_eq!(report.receipt.auth_visibility, "unverified");
        assert!(
            report
                .receipt
                .auth_visibility_reason
                .contains("Codex-auth status is unverified")
        );
        assert_eq!(
            report.receipt.cleanup.owner_path,
            harness_home
                .join("state")
                .join("workers")
                .join("worker-jobs.sqlite")
        );
        assert!(!report.snapshot_file.exists());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn subagent_lifecycle_close_is_idempotent() {
        let root = temp_root("close_idempotent");
        let harness_home = root.join(".agent-harness");

        record_subagent_lifecycle(SubagentLifecycleRecordOptions {
            harness_home: harness_home.clone(),
            subagent_id: "subagent:close-1".to_string(),
            state: SubagentLifecycleState::Running,
            source: Some("worker-dispatch".to_string()),
            operation_plan_id: None,
            operation_plan_item_id: None,
            worker_job_id: Some("job-1".to_string()),
            runtime_queue_id: Some("worker:1".to_string()),
            requested_model: None,
            resolved_model: None,
            provider: None,
            auth_lane: None,
            changed_files: Vec::new(),
            terminal_receipt_file: None,
            reason: "running".to_string(),
            now_ms: 11,
        })
        .unwrap();

        let first = close_subagent_lifecycle(SubagentLifecycleCloseOptions {
            harness_home: harness_home.clone(),
            subagent_id: "subagent:close-1".to_string(),
            reason: "operator close".to_string(),
            now_ms: 12,
        })
        .unwrap();
        let second = close_subagent_lifecycle(SubagentLifecycleCloseOptions {
            harness_home: harness_home.clone(),
            subagent_id: "subagent:close-1".to_string(),
            reason: "operator close".to_string(),
            now_ms: 13,
        })
        .unwrap();

        assert_eq!(first.receipt.state, SubagentLifecycleState::AlreadyClosed);
        assert!(first.receipt.cleanup.close_requested);
        assert!(!first.receipt.cleanup.cleanup_proven);
        assert!(
            first
                .receipt
                .cleanup
                .diagnostic
                .as_deref()
                .unwrap_or_default()
                .contains("non-terminal state Running")
        );
        assert!(first.receipt.reason.contains("cleanup proof unavailable"));
        assert_eq!(second.receipt.state, SubagentLifecycleState::AlreadyClosed);
        assert!(second.receipt.reason.contains("already recorded"));
        assert!(second.receipt.reason.contains("cleanup proof unavailable"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn subagent_lifecycle_close_uses_receipt_ledger_when_snapshot_was_cleaned() {
        let root = temp_root("close_from_ledger");
        let harness_home = root.join(".agent-harness");

        let completed = record_subagent_lifecycle(SubagentLifecycleRecordOptions {
            harness_home: harness_home.clone(),
            subagent_id: "subagent:completed-cleaned".to_string(),
            state: SubagentLifecycleState::Completed,
            source: Some("worker-dispatch".to_string()),
            operation_plan_id: None,
            operation_plan_item_id: None,
            worker_job_id: Some("job-1".to_string()),
            runtime_queue_id: Some("worker:1".to_string()),
            requested_model: Some("gpt-5.3-codex-spark".to_string()),
            resolved_model: Some("gpt-5.3-codex-spark".to_string()),
            provider: None,
            auth_lane: None,
            changed_files: Vec::new(),
            terminal_receipt_file: None,
            reason: "completed; provider/auth lane unavailable in worker receipt".to_string(),
            now_ms: 11,
        })
        .unwrap();
        fs::remove_file(&completed.snapshot_file).unwrap();

        let closed = close_subagent_lifecycle(SubagentLifecycleCloseOptions {
            harness_home: harness_home.clone(),
            subagent_id: "subagent:completed-cleaned".to_string(),
            reason: "operator close after completed worker disappeared".to_string(),
            now_ms: 12,
        })
        .unwrap();

        assert_eq!(closed.receipt.state, SubagentLifecycleState::AlreadyClosed);
        assert_eq!(closed.receipt.worker_job_id.as_deref(), Some("job-1"));
        assert_eq!(
            closed.receipt.auth_visibility_reason,
            default_auth_visibility_reason()
        );
        assert!(closed.receipt.cleanup.close_requested);
        assert!(!closed.receipt.cleanup.cleanup_proven);
        assert!(
            closed
                .receipt
                .cleanup
                .diagnostic
                .as_deref()
                .unwrap_or_default()
                .contains("terminal state Completed")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn subagent_lifecycle_close_unknown_id_is_idempotent_diagnostic() {
        let root = temp_root("close_unknown");
        let harness_home = root.join(".agent-harness");

        let closed = close_subagent_lifecycle(SubagentLifecycleCloseOptions {
            harness_home: harness_home.clone(),
            subagent_id: "subagent:already-cleaned".to_string(),
            reason: "operator close for already-cleaned worker".to_string(),
            now_ms: 12,
        })
        .unwrap();

        assert_eq!(closed.receipt.state, SubagentLifecycleState::AlreadyClosed);
        assert!(closed.receipt.cleanup.close_requested);
        assert!(
            closed
                .receipt
                .cleanup
                .diagnostic
                .as_deref()
                .unwrap_or_default()
                .contains("unknown or already-cleaned worker id")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn subagent_lifecycle_records_provider_auth_visibility() {
        let root = temp_root("auth_visibility");
        let harness_home = root.join(".agent-harness");

        let report = record_subagent_lifecycle(SubagentLifecycleRecordOptions {
            harness_home: harness_home.clone(),
            subagent_id: "subagent:auth-1".to_string(),
            state: SubagentLifecycleState::Queued,
            source: Some("worker-dispatch".to_string()),
            operation_plan_id: None,
            operation_plan_item_id: None,
            worker_job_id: Some("job-1".to_string()),
            runtime_queue_id: None,
            requested_model: Some("gpt-5.3-codex-spark".to_string()),
            resolved_model: Some("gpt-5.3-codex-spark".to_string()),
            provider: Some("openai".to_string()),
            auth_lane: Some("codex-oauth".to_string()),
            changed_files: Vec::new(),
            terminal_receipt_file: None,
            reason: "queued".to_string(),
            now_ms: 11,
        })
        .unwrap();

        assert_eq!(report.receipt.provider.as_deref(), Some("openai"));
        assert_eq!(report.receipt.auth_lane.as_deref(), Some("codex-oauth"));
        assert_eq!(report.receipt.auth_visibility, "receipt-visible");
        assert!(
            report
                .receipt
                .auth_visibility_reason
                .contains("provider and auth lane were recorded")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn subagent_lifecycle_snapshot_filenames_do_not_collide_after_sanitizing() {
        let root = temp_root("snapshot_collision");
        let harness_home = root.join(".agent-harness");

        let colon = subagent_lifecycle_snapshot_file(&harness_home, "subagent:abc");
        let underscore = subagent_lifecycle_snapshot_file(&harness_home, "subagent_abc");

        assert_ne!(colon, underscore);
        assert!(
            colon
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_default()
                .starts_with("subagent_abc-")
        );

        let _ = fs::remove_dir_all(root);
    }
}
