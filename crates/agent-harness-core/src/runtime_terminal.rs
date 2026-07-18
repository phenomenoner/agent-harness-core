use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RuntimeTerminalDispositionV1 {
    LogicalSuccess,
    LogicalFailure,
    LogicalCanceled,
    NeedsUser,
    ContinuationHandoff,
    TerminalSuppression,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeContinuationLinkV1 {
    pub parent_queue_id: String,
    pub child_queue_id: String,
    pub continuation_index: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub virtual_lane_digest: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RuntimeRetryReplayModeV1 {
    SameRequestNoObservableMutation,
    SameQueueLegacyPolicy,
    ExactLaneContinuation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeRetryScheduleV1 {
    pub lineage_id: String,
    pub attempt: usize,
    pub max_attempts: usize,
    pub delay_ms: i64,
    pub scheduled_at_ms: i64,
    pub next_eligible_at_ms: i64,
    pub replay_mode: RuntimeRetryReplayModeV1,
}
