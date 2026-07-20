use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

pub const TASK_BUDGET_LEDGER_SCHEMA: &str = "agent-harness.task-budget-ledger.v1";
pub const DEFAULT_TASK_WALL_CLOCK_BUDGET_MS: u64 = 8 * 60 * 60 * 1_000;
pub const DEFAULT_MAX_TASK_SLICES: u64 = 16;
pub const DEFAULT_MAX_TASK_TOKENS: u64 = 2_000_000;
pub const DEFAULT_MAX_NO_PROGRESS_SLICES: u64 = 2;
pub const DEFAULT_MAX_RECOVERY_SLICES: u64 = 4;
pub const DEFAULT_MAX_DISPOSITION_RECOVERY: u64 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskBudgetSliceV1 {
    pub schema: String,
    pub family_id: String,
    pub slice_generation: u64,
    pub wall_time_ms: u64,
    pub total_tokens: u64,
    pub progress_digest: String,
    pub recovery_slice: bool,
    pub disposition_recovery: bool,
    pub observed_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskBudgetStatusV1 {
    pub family_id: String,
    pub slices: u64,
    pub cumulative_wall_time_ms: u64,
    pub cumulative_tokens: u64,
    pub consecutive_no_progress_slices: u64,
    pub recovery_slices: u64,
    pub disposition_recoveries: u64,
    pub disposition_recovery_exhausted: bool,
    pub exhausted: bool,
    pub reason_code: &'static str,
}

pub fn record_task_budget_slice(
    harness_home: &Path,
    slice: TaskBudgetSliceV1,
) -> io::Result<TaskBudgetStatusV1> {
    validate_slice(&slice)?;
    let file = task_budget_ledger_file(harness_home, &slice.family_id);
    let mut by_generation = read_slices(&file)?;
    if let Some(existing) = by_generation.get(&slice.slice_generation) {
        if !same_slice_accounting(existing, &slice) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "task budget slice generation was replayed with different accounting",
            ));
        }
        return Ok(status(&slice.family_id, by_generation.values()));
    }
    crate::append_jsonl_value(&file, &slice)?;
    by_generation.insert(slice.slice_generation, slice.clone());
    Ok(status(&slice.family_id, by_generation.values()))
}

pub fn task_budget_status(harness_home: &Path, family_id: &str) -> io::Result<TaskBudgetStatusV1> {
    let file = task_budget_ledger_file(harness_home, family_id);
    let slices = read_slices(&file)?;
    Ok(status(family_id, slices.values()))
}

fn task_budget_ledger_file(harness_home: &Path, family_id: &str) -> PathBuf {
    harness_home
        .join("state")
        .join("runtime-queue")
        .join("task-budgets")
        .join(format!("{family_id}.jsonl"))
}

fn read_slices(file: &Path) -> io::Result<BTreeMap<u64, TaskBudgetSliceV1>> {
    let text = match fs::read_to_string(file) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(error) => return Err(error),
    };
    let mut slices = BTreeMap::new();
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let slice: TaskBudgetSliceV1 = serde_json::from_str(line).map_err(io::Error::other)?;
        validate_slice(&slice)?;
        if let Some(existing) = slices.insert(slice.slice_generation, slice.clone())
            && !same_slice_accounting(&existing, &slice)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "task budget ledger contains conflicting slice generations",
            ));
        }
    }
    Ok(slices)
}

fn same_slice_accounting(left: &TaskBudgetSliceV1, right: &TaskBudgetSliceV1) -> bool {
    left.schema == right.schema
        && left.family_id == right.family_id
        && left.slice_generation == right.slice_generation
        && left.wall_time_ms == right.wall_time_ms
        && left.total_tokens == right.total_tokens
        && left.progress_digest == right.progress_digest
        && left.recovery_slice == right.recovery_slice
        && left.disposition_recovery == right.disposition_recovery
}

fn validate_slice(slice: &TaskBudgetSliceV1) -> io::Result<()> {
    if slice.schema != TASK_BUDGET_LEDGER_SCHEMA
        || slice.family_id.len() != 64
        || slice.progress_digest.len() != 64
        || !lower_hex(&slice.family_id)
        || !lower_hex(&slice.progress_digest)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "task budget slice has invalid schema or digest identity",
        ));
    }
    Ok(())
}

fn status<'a>(
    family_id: &str,
    slices: impl Iterator<Item = &'a TaskBudgetSliceV1>,
) -> TaskBudgetStatusV1 {
    let mut ordered = slices.collect::<Vec<_>>();
    ordered.sort_by_key(|slice| slice.slice_generation);
    let cumulative_wall_time_ms = ordered
        .iter()
        .fold(0u64, |sum, slice| sum.saturating_add(slice.wall_time_ms));
    let cumulative_tokens = ordered
        .iter()
        .fold(0u64, |sum, slice| sum.saturating_add(slice.total_tokens));
    let recovery_slices = ordered.iter().filter(|slice| slice.recovery_slice).count() as u64;
    let disposition_recoveries = ordered
        .iter()
        .filter(|slice| slice.disposition_recovery)
        .count() as u64;
    let mut consecutive_no_progress_slices = 0u64;
    let mut previous = None;
    for slice in &ordered {
        if previous == Some(slice.progress_digest.as_str()) {
            consecutive_no_progress_slices = consecutive_no_progress_slices.saturating_add(1);
        } else {
            consecutive_no_progress_slices = 0;
        }
        previous = Some(slice.progress_digest.as_str());
    }
    let slices_count = ordered.len() as u64;
    let reason_code = if cumulative_wall_time_ms >= DEFAULT_TASK_WALL_CLOCK_BUDGET_MS {
        "wall-clock-budget-exhausted"
    } else if slices_count >= DEFAULT_MAX_TASK_SLICES {
        "slice-budget-exhausted"
    } else if cumulative_tokens >= DEFAULT_MAX_TASK_TOKENS {
        "token-budget-exhausted"
    } else if consecutive_no_progress_slices >= DEFAULT_MAX_NO_PROGRESS_SLICES {
        "no-progress-budget-exhausted"
    } else if recovery_slices >= DEFAULT_MAX_RECOVERY_SLICES {
        "recovery-budget-exhausted"
    } else {
        "within-budget"
    };
    TaskBudgetStatusV1 {
        family_id: family_id.to_string(),
        slices: slices_count,
        cumulative_wall_time_ms,
        cumulative_tokens,
        consecutive_no_progress_slices,
        recovery_slices,
        disposition_recoveries,
        disposition_recovery_exhausted: disposition_recoveries >= DEFAULT_MAX_DISPOSITION_RECOVERY,
        exhausted: reason_code != "within-budget",
        reason_code,
    }
}

fn lower_hex(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_budget_is_idempotent_and_detects_no_progress() {
        let root = std::env::temp_dir().join(format!("task-budget-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let family_id = "a".repeat(64);
        let digest = "b".repeat(64);
        for generation in 0..=2 {
            let slice = TaskBudgetSliceV1 {
                schema: TASK_BUDGET_LEDGER_SCHEMA.to_string(),
                family_id: family_id.clone(),
                slice_generation: generation,
                wall_time_ms: 1,
                total_tokens: 1,
                progress_digest: digest.clone(),
                recovery_slice: false,
                disposition_recovery: false,
                observed_at_ms: generation as i64,
            };
            let status = record_task_budget_slice(&root, slice.clone()).unwrap();
            let replay = record_task_budget_slice(&root, slice).unwrap();
            assert_eq!(status, replay);
        }
        let status = task_budget_status(&root, &family_id).unwrap();
        assert!(status.exhausted);
        assert_eq!(status.reason_code, "no-progress-budget-exhausted");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn task_budget_replay_ignores_observation_timestamp_but_not_accounting() {
        let root = std::env::temp_dir().join(format!(
            "task-budget-replay-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let family_id = "c".repeat(64);
        let original = TaskBudgetSliceV1 {
            schema: TASK_BUDGET_LEDGER_SCHEMA.to_string(),
            family_id: family_id.clone(),
            slice_generation: 1,
            wall_time_ms: 10,
            total_tokens: 20,
            progress_digest: "d".repeat(64),
            recovery_slice: false,
            disposition_recovery: true,
            observed_at_ms: 1,
        };
        let first = record_task_budget_slice(&root, original.clone()).unwrap();
        let mut replay = original.clone();
        replay.observed_at_ms = 99;
        assert_eq!(first, record_task_budget_slice(&root, replay).unwrap());

        let mut conflict = original;
        conflict.total_tokens = 21;
        conflict.observed_at_ms = 100;
        assert!(record_task_budget_slice(&root, conflict).is_err());
        let _ = fs::remove_dir_all(root);
    }
}
