use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use ring::digest;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::goal_transition::GoalTransitionEventKind;
use crate::{append_jsonl_value, current_log_time_ms};

pub const GOAL_CAMPAIGN_BUDGET_SCHEMA: &str = "agent-harness.goal-campaign-budget.v1";
pub const GOAL_CAMPAIGN_STATUS_SCHEMA: &str = "agent-harness.goal-campaign-status.v1";

pub const DEFAULT_GOAL_SLICE_HARD_TIMEOUT_MS: u64 = 30 * 60 * 1_000;
pub const DEFAULT_GOAL_SLICE_IDLE_TIMEOUT_MS: u64 = 5 * 60 * 1_000;
pub const DEFAULT_GOAL_SLICE_DRAIN_WINDOW_MS: u64 = 3 * 60 * 1_000;
pub const DEFAULT_GOAL_WALL_CLOCK_BUDGET_MS: u64 = 48 * 60 * 60 * 1_000;
pub const DEFAULT_GOAL_MAX_SLICES: u64 = 96;
pub const DEFAULT_GOAL_MAX_TOTAL_TOKENS: u64 = 100_000_000;
pub const DEFAULT_GOAL_MAX_NO_PROGRESS_SLICES: u64 = 6;
pub const DEFAULT_GOAL_MAX_RECOVERY_SLICES: u64 = 12;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GoalCampaignBudgetBoundary {
    WithinBudget,
    WallClock,
    SliceCount,
    TokenCost,
    NoProgress,
    RecoveryCount,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GoalCampaignPolicyV1 {
    pub mode: String,
    pub active_lane_digests: Vec<String>,
    pub slice_hard_timeout_ms: u64,
    pub slice_idle_timeout_ms: u64,
    pub slice_drain_window_ms: u64,
    pub wall_clock_budget_ms: u64,
    pub max_slices: u64,
    pub max_total_tokens: u64,
    pub max_no_progress_slices: u64,
    pub max_recovery_slices: u64,
}

impl Default for GoalCampaignPolicyV1 {
    fn default() -> Self {
        Self {
            mode: "observe".to_string(),
            active_lane_digests: Vec::new(),
            slice_hard_timeout_ms: DEFAULT_GOAL_SLICE_HARD_TIMEOUT_MS,
            slice_idle_timeout_ms: DEFAULT_GOAL_SLICE_IDLE_TIMEOUT_MS,
            slice_drain_window_ms: DEFAULT_GOAL_SLICE_DRAIN_WINDOW_MS,
            wall_clock_budget_ms: DEFAULT_GOAL_WALL_CLOCK_BUDGET_MS,
            max_slices: DEFAULT_GOAL_MAX_SLICES,
            max_total_tokens: DEFAULT_GOAL_MAX_TOTAL_TOKENS,
            max_no_progress_slices: DEFAULT_GOAL_MAX_NO_PROGRESS_SLICES,
            max_recovery_slices: DEFAULT_GOAL_MAX_RECOVERY_SLICES,
        }
    }
}

impl GoalCampaignPolicyV1 {
    pub fn validate(&self) -> io::Result<()> {
        if !matches!(self.mode.as_str(), "disabled" | "observe" | "active") {
            return Err(invalid_policy("mode must be disabled, observe, or active"));
        }
        if !(30 * 60 * 1_000..=45 * 60 * 1_000).contains(&self.slice_hard_timeout_ms) {
            return Err(invalid_policy(
                "sliceHardTimeoutMs must be between 30 and 45 minutes",
            ));
        }
        if self.slice_idle_timeout_ms < 60_000
            || self.slice_idle_timeout_ms >= self.slice_hard_timeout_ms
        {
            return Err(invalid_policy(
                "sliceIdleTimeoutMs must be at least 60000 and below sliceHardTimeoutMs",
            ));
        }
        let derived_drain = (self.slice_hard_timeout_ms / 10).min(3 * 60 * 1_000);
        if self.slice_drain_window_ms != derived_drain {
            return Err(invalid_policy(
                "sliceDrainWindowMs must equal min(sliceHardTimeoutMs / 10, 180000)",
            ));
        }
        if self.wall_clock_budget_ms < self.slice_hard_timeout_ms
            || self.wall_clock_budget_ms > DEFAULT_GOAL_WALL_CLOCK_BUDGET_MS
        {
            return Err(invalid_policy(
                "wallClockBudgetMs must cover one slice and may not exceed 48 hours",
            ));
        }
        if self.max_slices == 0
            || self.max_total_tokens == 0
            || self.max_no_progress_slices == 0
            || self.max_recovery_slices == 0
        {
            return Err(invalid_policy(
                "campaign slice/token/no-progress/recovery budgets must be non-zero",
            ));
        }
        if self.mode == "active" && self.active_lane_digests.is_empty() {
            return Err(invalid_policy(
                "active mode requires at least one exact activeLaneDigests entry",
            ));
        }
        if self.active_lane_digests.iter().any(|value| {
            value.len() != 64
                || !value
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        }) {
            return Err(invalid_policy(
                "activeLaneDigests entries must be lowercase 64-character SHA-256 digests",
            ));
        }
        Ok(())
    }

    pub fn effective_timeouts(&self, requested_hard_ms: u64, requested_idle_ms: u64) -> (u64, u64) {
        (
            requested_hard_ms.min(self.slice_hard_timeout_ms),
            requested_idle_ms.min(self.slice_idle_timeout_ms),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoalCampaignBudgetInput<'a> {
    pub campaign_family_id: &'a str,
    pub lane_digest: &'a str,
    pub virtual_session_id: &'a str,
    pub queue_id: Option<&'a str>,
    pub goal_checksum: Option<&'a str>,
    pub source_slice_generation: u64,
    pub event: GoalTransitionEventKind,
    pub slice_elapsed_ms: u64,
    pub slice_tokens: u64,
    pub output_tokens: u64,
    pub event_count: usize,
    pub runtime_status: &'a str,
    pub recovery_slice: bool,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GoalCampaignBudgetReceiptV1 {
    pub schema: String,
    pub campaign_family_id: String,
    pub lane_digest: String,
    pub virtual_session_id: String,
    pub queue_id: Option<String>,
    pub goal_checksum: Option<String>,
    pub source_slice_generation: u64,
    pub event: GoalTransitionEventKind,
    pub mode: String,
    pub slice_hard_timeout_ms: u64,
    pub slice_idle_timeout_ms: u64,
    pub slice_drain_window_ms: u64,
    pub wall_clock_budget_ms: u64,
    pub max_slices: u64,
    pub max_total_tokens: u64,
    pub max_no_progress_slices: u64,
    pub max_recovery_slices: u64,
    pub campaign_started_at_ms: i64,
    pub campaign_elapsed_ms: u64,
    pub slices_observed: u64,
    pub total_tokens_observed: u64,
    pub consecutive_no_progress_slices: u64,
    pub recovery_slices_observed: u64,
    pub slice_elapsed_ms: u64,
    pub slice_tokens: u64,
    pub progress_fingerprint: String,
    pub boundary: GoalCampaignBudgetBoundary,
    pub budget_exhausted: bool,
    pub no_progress_exhausted: bool,
    pub observed_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GoalCampaignStatusReportV1 {
    pub schema: String,
    pub read_only: bool,
    pub policy: GoalCampaignPolicyV1,
    pub receipts_file: PathBuf,
    pub campaigns: Vec<GoalCampaignBudgetReceiptV1>,
    pub observed_at_ms: i64,
}

pub fn load_goal_campaign_policy(
    harness_home: impl AsRef<Path>,
) -> io::Result<GoalCampaignPolicyV1> {
    let file = harness_home.as_ref().join("harness-config.json");
    let value = match fs::read(&file) {
        Ok(bytes) => serde_json::from_slice::<Value>(&bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(GoalCampaignPolicyV1::default());
        }
        Err(error) => return Err(error),
    };
    let mut policy = GoalCampaignPolicyV1::default();
    let Some(object) = value.get("goalAutonomy").and_then(Value::as_object) else {
        return Ok(policy);
    };
    if let Some(value) = object.get("mode").and_then(Value::as_str) {
        policy.mode = value.to_ascii_lowercase();
    }
    if let Some(values) = object.get("activeLaneDigests").and_then(Value::as_array) {
        policy.active_lane_digests = values
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect();
    }
    apply_u64(
        object,
        "sliceHardTimeoutMs",
        &mut policy.slice_hard_timeout_ms,
    )?;
    apply_u64(
        object,
        "sliceIdleTimeoutMs",
        &mut policy.slice_idle_timeout_ms,
    )?;
    apply_u64(
        object,
        "sliceDrainWindowMs",
        &mut policy.slice_drain_window_ms,
    )?;
    apply_u64(
        object,
        "wallClockBudgetMs",
        &mut policy.wall_clock_budget_ms,
    )?;
    apply_u64(object, "maxSlices", &mut policy.max_slices)?;
    apply_u64(object, "maxTotalTokens", &mut policy.max_total_tokens)?;
    apply_u64(
        object,
        "maxNoProgressSlices",
        &mut policy.max_no_progress_slices,
    )?;
    apply_u64(object, "maxRecoverySlices", &mut policy.max_recovery_slices)?;
    policy.validate()?;
    Ok(policy)
}

pub fn goal_campaign_budget_receipts_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("goal-lineage")
        .join("campaign-budget-receipts.jsonl")
}

pub fn evaluate_goal_campaign_budget(
    harness_home: impl AsRef<Path>,
    policy: &GoalCampaignPolicyV1,
    input: GoalCampaignBudgetInput<'_>,
) -> io::Result<GoalCampaignBudgetReceiptV1> {
    policy.validate()?;
    for (name, value) in [
        ("campaignFamilyId", input.campaign_family_id),
        ("laneDigest", input.lane_digest),
        ("virtualSessionId", input.virtual_session_id),
    ] {
        if value.trim().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("goal campaign budget lacks exact {name}"),
            ));
        }
    }
    let file = goal_campaign_budget_receipts_file(harness_home);
    let receipts = read_budget_receipts(&file)?;
    if let Some(existing) = receipts.iter().rev().find(|receipt| {
        receipt.campaign_family_id == input.campaign_family_id
            && receipt.queue_id.as_deref() == input.queue_id
            && receipt.source_slice_generation == input.source_slice_generation
            && receipt.goal_checksum.as_deref() == input.goal_checksum
    }) {
        return Ok(existing.clone());
    }
    let previous = receipts
        .iter()
        .rev()
        .find(|receipt| receipt.campaign_family_id == input.campaign_family_id);
    let slice_tokens = input.slice_tokens;
    let progress_fingerprint = progress_fingerprint(&input);
    let campaign_started_at_ms = previous
        .map(|receipt| receipt.campaign_started_at_ms)
        .unwrap_or(input.now_ms);
    let campaign_elapsed_ms = input.now_ms.saturating_sub(campaign_started_at_ms).max(0) as u64;
    let slices_observed = previous
        .map(|receipt| receipt.slices_observed)
        .unwrap_or(0)
        .saturating_add(1);
    let total_tokens_observed = previous
        .map(|receipt| receipt.total_tokens_observed)
        .unwrap_or(0)
        .saturating_add(slice_tokens);
    let repeated_fingerprint = previous.is_some_and(|receipt| {
        receipt.progress_fingerprint == progress_fingerprint || input.event_count == 0
    });
    let consecutive_no_progress_slices = if repeated_fingerprint {
        previous
            .map(|receipt| receipt.consecutive_no_progress_slices)
            .unwrap_or(0)
            .saturating_add(1)
    } else {
        0
    };
    let recovery_slice = input.recovery_slice
        || matches!(
            input.event,
            GoalTransitionEventKind::CompactRollover
                | GoalTransitionEventKind::ProcessRestart
                | GoalTransitionEventKind::ToolTimeout
        );
    let recovery_slices_observed = previous
        .map(|receipt| receipt.recovery_slices_observed)
        .unwrap_or(0)
        .saturating_add(u64::from(recovery_slice));
    let boundary = if campaign_elapsed_ms >= policy.wall_clock_budget_ms {
        GoalCampaignBudgetBoundary::WallClock
    } else if slices_observed >= policy.max_slices {
        GoalCampaignBudgetBoundary::SliceCount
    } else if total_tokens_observed >= policy.max_total_tokens {
        GoalCampaignBudgetBoundary::TokenCost
    } else if consecutive_no_progress_slices >= policy.max_no_progress_slices {
        GoalCampaignBudgetBoundary::NoProgress
    } else if recovery_slices_observed >= policy.max_recovery_slices {
        GoalCampaignBudgetBoundary::RecoveryCount
    } else {
        GoalCampaignBudgetBoundary::WithinBudget
    };
    let receipt = GoalCampaignBudgetReceiptV1 {
        schema: GOAL_CAMPAIGN_BUDGET_SCHEMA.to_string(),
        campaign_family_id: input.campaign_family_id.to_string(),
        lane_digest: input.lane_digest.to_string(),
        virtual_session_id: input.virtual_session_id.to_string(),
        queue_id: input.queue_id.map(str::to_string),
        goal_checksum: input.goal_checksum.map(str::to_string),
        source_slice_generation: input.source_slice_generation,
        event: input.event,
        mode: policy.mode.clone(),
        slice_hard_timeout_ms: policy.slice_hard_timeout_ms,
        slice_idle_timeout_ms: policy.slice_idle_timeout_ms,
        slice_drain_window_ms: policy.slice_drain_window_ms,
        wall_clock_budget_ms: policy.wall_clock_budget_ms,
        max_slices: policy.max_slices,
        max_total_tokens: policy.max_total_tokens,
        max_no_progress_slices: policy.max_no_progress_slices,
        max_recovery_slices: policy.max_recovery_slices,
        campaign_started_at_ms,
        campaign_elapsed_ms,
        slices_observed,
        total_tokens_observed,
        consecutive_no_progress_slices,
        recovery_slices_observed,
        slice_elapsed_ms: input.slice_elapsed_ms,
        slice_tokens,
        progress_fingerprint,
        boundary,
        budget_exhausted: matches!(
            boundary,
            GoalCampaignBudgetBoundary::WallClock
                | GoalCampaignBudgetBoundary::SliceCount
                | GoalCampaignBudgetBoundary::TokenCost
                | GoalCampaignBudgetBoundary::RecoveryCount
        ),
        no_progress_exhausted: boundary == GoalCampaignBudgetBoundary::NoProgress,
        observed_at_ms: input.now_ms,
    };
    append_jsonl_value(&file, &receipt)?;
    Ok(receipt)
}

pub fn current_goal_campaign_timeouts(
    harness_home: impl AsRef<Path>,
    requested_hard_ms: u64,
    requested_idle_ms: u64,
) -> io::Result<(GoalCampaignPolicyV1, u64, u64)> {
    let policy = load_goal_campaign_policy(harness_home)?;
    let (hard, idle) = policy.effective_timeouts(requested_hard_ms, requested_idle_ms);
    Ok((policy, hard, idle))
}

pub fn collect_goal_campaign_status(
    harness_home: impl AsRef<Path>,
) -> io::Result<GoalCampaignStatusReportV1> {
    let harness_home = harness_home.as_ref();
    let policy = load_goal_campaign_policy(harness_home)?;
    let receipts_file = goal_campaign_budget_receipts_file(harness_home);
    let mut latest = BTreeMap::<String, GoalCampaignBudgetReceiptV1>::new();
    for receipt in read_budget_receipts(&receipts_file)? {
        latest.insert(receipt.campaign_family_id.clone(), receipt);
    }
    Ok(GoalCampaignStatusReportV1 {
        schema: GOAL_CAMPAIGN_STATUS_SCHEMA.to_string(),
        read_only: true,
        policy,
        receipts_file,
        campaigns: latest.into_values().collect(),
        observed_at_ms: current_log_time_ms()?,
    })
}

fn progress_fingerprint(input: &GoalCampaignBudgetInput<'_>) -> String {
    let canonical = format!(
        "{}\u{1f}{}\u{1f}{}\u{1f}{}",
        input.goal_checksum.unwrap_or_default(),
        input.output_tokens,
        input.event_count,
        input.runtime_status
    );
    let hash = digest::digest(&digest::SHA256, canonical.as_bytes());
    hash.as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn read_budget_receipts(file: &Path) -> io::Result<Vec<GoalCampaignBudgetReceiptV1>> {
    let text = match fs::read_to_string(file) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    text.lines()
        .enumerate()
        .filter(|(_, line)| !line.trim().is_empty())
        .map(|(index, line)| {
            let receipt =
                serde_json::from_str::<GoalCampaignBudgetReceiptV1>(line).map_err(|error| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "invalid goal campaign budget receipt at line {}: {error}",
                            index + 1
                        ),
                    )
                })?;
            if receipt.schema != GOAL_CAMPAIGN_BUDGET_SCHEMA {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unsupported goal campaign budget schema {}", receipt.schema),
                ));
            }
            Ok(receipt)
        })
        .collect()
}

fn apply_u64(
    object: &serde_json::Map<String, Value>,
    key: &str,
    target: &mut u64,
) -> io::Result<()> {
    if let Some(value) = object.get(key) {
        *target = value.as_u64().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("goalAutonomy.{key} must be an unsigned integer"),
            )
        })?;
    }
    Ok(())
}

fn invalid_policy(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

pub fn goal_budget_now_ms() -> io::Result<i64> {
    current_log_time_ms()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn default_policy_is_observe_and_bounds_48_hours_into_30_minute_slices() {
        let policy = GoalCampaignPolicyV1::default();
        policy.validate().unwrap();
        assert_eq!(policy.mode, "observe");
        assert_eq!(policy.slice_hard_timeout_ms, 30 * 60 * 1_000);
        assert_eq!(policy.slice_drain_window_ms, 3 * 60 * 1_000);
        assert_eq!(policy.wall_clock_budget_ms, 48 * 60 * 60 * 1_000);
        assert_eq!(policy.max_slices, 96);
        assert_eq!(
            policy.effective_timeouts(60 * 60 * 1_000, 10 * 60 * 1_000),
            (30 * 60 * 1_000, 5 * 60 * 1_000)
        );
    }

    #[test]
    fn budget_ledger_stops_at_slice_token_recovery_and_wall_clock_boundaries() {
        let root = temp_root("budget-boundaries");
        let mut policy = GoalCampaignPolicyV1 {
            max_slices: 3,
            max_total_tokens: 100,
            max_no_progress_slices: 10,
            max_recovery_slices: 10,
            ..GoalCampaignPolicyV1::default()
        };
        let first = evaluate_goal_campaign_budget(
            &root,
            &policy,
            observation("q1", 0, 0, 10, 1, "completed", false, 1_000),
        )
        .unwrap();
        assert_eq!(first.boundary, GoalCampaignBudgetBoundary::WithinBudget);
        let second = evaluate_goal_campaign_budget(
            &root,
            &policy,
            observation("q2", 1, 1, 10, 2, "completed", false, 2_000),
        )
        .unwrap();
        assert_eq!(second.boundary, GoalCampaignBudgetBoundary::WithinBudget);
        let third = evaluate_goal_campaign_budget(
            &root,
            &policy,
            observation("q3", 2, 2, 10, 3, "completed", false, 3_000),
        )
        .unwrap();
        assert_eq!(third.boundary, GoalCampaignBudgetBoundary::SliceCount);
        let replay = evaluate_goal_campaign_budget(
            &root,
            &policy,
            observation("q3", 2, 2, 10, 3, "completed", false, 3_000),
        )
        .unwrap();
        assert_eq!(replay, third);
        assert_eq!(
            fs::read_to_string(goal_campaign_budget_receipts_file(&root))
                .unwrap()
                .lines()
                .count(),
            3
        );

        let token_root = temp_root("token-boundary");
        policy.max_slices = 100;
        let token = evaluate_goal_campaign_budget(
            &token_root,
            &policy,
            observation("q-token", 0, 1, 100, 2, "completed", false, 1_000),
        )
        .unwrap();
        assert_eq!(token.boundary, GoalCampaignBudgetBoundary::TokenCost);

        let recovery_root = temp_root("recovery-boundary");
        policy.max_total_tokens = 1_000;
        policy.max_recovery_slices = 1;
        let mut recovery = observation("q-recovery", 0, 1, 1, 2, "timeout", true, 1_000);
        recovery.event = GoalTransitionEventKind::CompactRollover;
        let recovery = evaluate_goal_campaign_budget(&recovery_root, &policy, recovery).unwrap();
        assert_eq!(recovery.boundary, GoalCampaignBudgetBoundary::RecoveryCount);

        let wall_root = temp_root("wall-boundary");
        policy.max_recovery_slices = 10;
        policy.wall_clock_budget_ms = policy.slice_hard_timeout_ms;
        evaluate_goal_campaign_budget(
            &wall_root,
            &policy,
            observation("q-wall-1", 0, 1, 1, 2, "completed", false, 1_000),
        )
        .unwrap();
        let wall = evaluate_goal_campaign_budget(
            &wall_root,
            &policy,
            observation(
                "q-wall-2",
                1,
                2,
                1,
                3,
                "completed",
                false,
                1_000 + policy.wall_clock_budget_ms as i64,
            ),
        )
        .unwrap();
        assert_eq!(wall.boundary, GoalCampaignBudgetBoundary::WallClock);

        let _ = fs::remove_dir_all(root);
        let _ = fs::remove_dir_all(token_root);
        let _ = fs::remove_dir_all(recovery_root);
        let _ = fs::remove_dir_all(wall_root);
    }

    #[test]
    fn identical_progress_fingerprints_trip_no_progress_breaker() {
        let root = temp_root("no-progress-boundary");
        let policy = GoalCampaignPolicyV1 {
            max_slices: 100,
            max_no_progress_slices: 2,
            ..GoalCampaignPolicyV1::default()
        };
        for index in 0..2 {
            let receipt = evaluate_goal_campaign_budget(
                &root,
                &policy,
                observation(
                    &format!("q-{index}"),
                    index,
                    1,
                    10,
                    5,
                    "completed",
                    false,
                    1_000 + index as i64,
                ),
            )
            .unwrap();
            assert_eq!(receipt.boundary, GoalCampaignBudgetBoundary::WithinBudget);
        }
        let stopped = evaluate_goal_campaign_budget(
            &root,
            &policy,
            observation("q-2", 2, 1, 10, 5, "completed", false, 1_002),
        )
        .unwrap();
        assert_eq!(stopped.boundary, GoalCampaignBudgetBoundary::NoProgress);
        assert!(stopped.no_progress_exhausted);
        assert!(!stopped.budget_exhausted);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn campaign_status_is_read_only_and_reports_effective_policy() {
        let root = temp_root("campaign-status");
        fs::create_dir_all(&root).unwrap();
        fs::write(
            root.join("harness-config.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "goalAutonomy": {
                    "mode": "observe",
                    "activeLaneDigests": [],
                    "sliceHardTimeoutMs": 2_700_000,
                    "sliceIdleTimeoutMs": 600_000,
                    "sliceDrainWindowMs": 180_000,
                    "wallClockBudgetMs": 172_800_000,
                    "maxSlices": 64,
                    "maxTotalTokens": 10_000_000,
                    "maxNoProgressSlices": 4,
                    "maxRecoverySlices": 8
                }
            }))
            .unwrap(),
        )
        .unwrap();
        let before = fs::read_dir(&root).unwrap().count();
        let empty = collect_goal_campaign_status(&root).unwrap();
        assert!(empty.read_only);
        assert_eq!(empty.policy.slice_hard_timeout_ms, 2_700_000);
        assert!(empty.campaigns.is_empty());
        assert_eq!(fs::read_dir(&root).unwrap().count(), before);

        let policy = load_goal_campaign_policy(&root).unwrap();
        evaluate_goal_campaign_budget(
            &root,
            &policy,
            observation("q-status", 0, 1, 2, 3, "completed", false, 1_000),
        )
        .unwrap();
        let status = collect_goal_campaign_status(&root).unwrap();
        assert_eq!(status.campaigns.len(), 1);
        assert_eq!(status.campaigns[0].slices_observed, 1);
        let _ = fs::remove_dir_all(root);
    }

    fn observation<'a>(
        queue_id: &'a str,
        source_slice_generation: u64,
        output_tokens: u64,
        slice_tokens: u64,
        event_count: usize,
        runtime_status: &'a str,
        recovery_slice: bool,
        now_ms: i64,
    ) -> GoalCampaignBudgetInput<'a> {
        GoalCampaignBudgetInput {
            campaign_family_id: "campaign-a",
            lane_digest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            virtual_session_id: "virtual-a",
            queue_id: Some(queue_id),
            goal_checksum: Some("sha256:goal-a"),
            source_slice_generation,
            event: GoalTransitionEventKind::NormalCompletion,
            slice_elapsed_ms: 1_000,
            slice_tokens,
            output_tokens,
            event_count,
            runtime_status,
            recovery_slice,
            now_ms,
        }
    }

    fn temp_root(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("agent-harness-{name}-{nanos}"))
    }
}
