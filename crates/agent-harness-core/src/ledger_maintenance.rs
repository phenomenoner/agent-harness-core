//! Isolated owner for expensive receipt/history retention work.
//!
//! Interactive ingress, runtime completion, progress delivery, and final
//! channel delivery only signal this owner after their durable append. They
//! never synchronously replay or rewrite a large ledger merely because a turn
//! finished. The owner uses each ledger's source-authoritative maintenance
//! gate, so normal wakes stay metadata/index bounded.

use std::io;
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::{
    AgentProgressHistoryCompactionReport, ChannelDeliveryReceiptCompactionOptions,
    ChannelDeliveryReceiptCompactionReport, RuntimeQueueReceiptCompactionOptions,
    RuntimeQueueReceiptCompactionReport, compact_agent_progress_history_if_needed,
    compact_channel_delivery_receipts_if_needed, compact_runtime_queue_receipts_if_needed,
};

pub const LEDGER_MAINTENANCE_WAKE_LANE: &str = "ledger-maintenance";

const LEDGER_MAINTENANCE_SCHEMA: &str = "agent-harness.ledger-maintenance.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerMaintenanceRunOptions {
    pub harness_home: PathBuf,
    pub now_ms: i64,
    /// Operator-only drain/verification mode. The normal background loop is
    /// intentionally false and relies on cheap source-aware gates.
    pub force: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LedgerMaintenanceRunReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub force: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime_queue_receipts: Option<RuntimeQueueReceiptCompactionReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel_delivery_receipts: Option<ChannelDeliveryReceiptCompactionReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub progress_history: Option<AgentProgressHistoryCompactionReport>,
    pub warnings: Vec<String>,
}

pub fn ledger_maintenance_wake_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("wake")
        .join("ledger-maintenance.json")
}

/// Coalesces a retention wake after a durable terminal append. Callers should
/// deliberately ignore a wake failure: retention failure must never turn an
/// already-delivered provider event into a retryable delivery failure.
pub fn request_ledger_maintenance(
    harness_home: impl AsRef<Path>,
    reason: &str,
) -> io::Result<crate::wake::WakeReceipt> {
    let harness_home = harness_home.as_ref();
    crate::wake::signal_wake(
        harness_home,
        ledger_maintenance_wake_file(harness_home),
        LEDGER_MAINTENANCE_WAKE_LANE,
        reason,
    )
}

/// Runs one isolated maintenance pass. A problem in one ledger is retained as
/// a warning so the owner can continue maintaining independent ledgers; every
/// individual compactor remains fail-closed about source data and history.
pub fn run_ledger_maintenance_once(
    options: LedgerMaintenanceRunOptions,
) -> io::Result<LedgerMaintenanceRunReport> {
    let mut warnings = Vec::new();
    let runtime_queue_receipts = if options.force {
        match compact_runtime_queue_receipts_if_needed(
            RuntimeQueueReceiptCompactionOptions::with_defaults(
                options.harness_home.clone(),
                options.now_ms,
            ),
        ) {
            Ok(report) => Some(report),
            Err(error) => {
                warnings.push(format!("runtime receipt maintenance failed: {error}"));
                None
            }
        }
    } else {
        match crate::runtime_worker::maybe_compact_runtime_queue_receipts_after_terminal(
            options.harness_home.clone(),
            options.now_ms,
        ) {
            Ok(report) => report,
            Err(error) => {
                warnings.push(format!("runtime receipt maintenance failed: {error}"));
                None
            }
        }
    };
    let channel_delivery_receipts = if options.force {
        match compact_channel_delivery_receipts_if_needed(
            ChannelDeliveryReceiptCompactionOptions::with_defaults(
                options.harness_home.clone(),
                options.now_ms,
            ),
        ) {
            Ok(report) => Some(report),
            Err(error) => {
                warnings.push(format!(
                    "channel delivery receipt maintenance failed: {error}"
                ));
                None
            }
        }
    } else {
        match crate::channel_delivery_index::maybe_compact_channel_delivery_receipts_after_terminal(
            options.harness_home.clone(),
            options.now_ms,
        ) {
            Ok(report) => report,
            Err(error) => {
                warnings.push(format!(
                    "channel delivery receipt maintenance failed: {error}"
                ));
                None
            }
        }
    };
    let progress_history = match compact_agent_progress_history_if_needed(
        &options.harness_home,
        options.now_ms,
        options.force,
    ) {
        Ok(report) => report,
        Err(error) => {
            warnings.push(format!("progress history maintenance failed: {error}"));
            None
        }
    };
    Ok(LedgerMaintenanceRunReport {
        schema: LEDGER_MAINTENANCE_SCHEMA,
        harness_home: options.harness_home,
        force: options.force,
        runtime_queue_receipts,
        channel_delivery_receipts,
        progress_history,
        warnings,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn maintenance_wake_coalesces_sequence_and_latest_reason() {
        let root = temp_root("maintenance_wake_coalesces_sequence_and_latest_reason");
        let harness_home = root.join(".agent-harness");
        let wake_file = ledger_maintenance_wake_file(&harness_home);

        let first = request_ledger_maintenance(&harness_home, "runtime terminal appended").unwrap();
        let second = request_ledger_maintenance(&harness_home, "final delivery appended").unwrap();

        assert_eq!(first.lane, LEDGER_MAINTENANCE_WAKE_LANE);
        assert_eq!(second.lane, LEDGER_MAINTENANCE_WAKE_LANE);
        assert_eq!(first.sequence, 1);
        assert_eq!(second.sequence, 2);
        assert_eq!(crate::wake::read_wake_sequence(&wake_file).unwrap(), 2);

        let record: serde_json::Value =
            serde_json::from_slice(&fs::read(&wake_file).unwrap()).unwrap();
        assert_eq!(record["lane"], LEDGER_MAINTENANCE_WAKE_LANE);
        assert_eq!(record["sequence"], 2);
        assert_eq!(record["reason"], "final delivery appended");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn normal_maintenance_leaves_missing_ledgers_untouched() {
        let root = temp_root("normal_maintenance_leaves_missing_ledgers_untouched");
        let harness_home = root.join(".agent-harness");

        let report = run_ledger_maintenance_once(LedgerMaintenanceRunOptions {
            harness_home: harness_home.clone(),
            now_ms: 1_000,
            force: false,
        })
        .unwrap();

        assert!(report.runtime_queue_receipts.is_none());
        assert!(report.channel_delivery_receipts.is_none());
        assert!(report.progress_history.is_none());
        assert!(report.warnings.is_empty());
        assert!(
            !harness_home.join("state").exists(),
            "a normal background wake must stop at the metadata gate when all ledgers are absent"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn forced_maintenance_contains_one_ledger_failure_and_continues_independently() {
        let root =
            temp_root("forced_maintenance_contains_one_ledger_failure_and_continues_independently");
        let harness_home = root.join(".agent-harness");
        let invalid_runtime_receipt_path = harness_home
            .join("state")
            .join("runtime-queue")
            .join("run-once-receipts.jsonl");
        fs::create_dir_all(&invalid_runtime_receipt_path).unwrap();

        let report = run_ledger_maintenance_once(LedgerMaintenanceRunOptions {
            harness_home,
            now_ms: 2_000,
            force: true,
        })
        .unwrap();

        assert!(report.runtime_queue_receipts.is_none());
        assert!(
            report.channel_delivery_receipts.is_some(),
            "the independent channel receipt ledger must still be maintained"
        );
        assert!(report.progress_history.is_none());
        assert!(
            report
                .warnings
                .iter()
                .any(|warning| warning.contains("runtime receipt maintenance failed"))
        );

        let _ = fs::remove_dir_all(root);
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-ledger-maintenance-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
