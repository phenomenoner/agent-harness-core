use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::append_jsonl_value;

pub const GOAL_TRANSITION_SCHEMA: &str = "agent-harness.goal-transition.v1";
pub const GOAL_SLICE_SCHEMA: &str = "agent-harness.goal-slice.v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GoalTransitionEventKind {
    NormalCompletion,
    DrainCompletion,
    AbsoluteTimeout,
    IdleTimeout,
    ToolTimeout,
    CompactRollover,
    AuthDeferred,
    ProcessRestart,
    OperatorStop,
    NewerSteer,
    OlderCompletion,
    RuntimeFailure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GoalTransitionAuthority {
    NotApplicable,
    Ready,
    Missing,
    Stale,
    Conflict,
    Invalid,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GoalTransitionRelation {
    CurrentGoalSlice,
    AuthorizedCampaignContinuation,
    FreshUnrelatedTurn,
    HistoricalState,
    #[default]
    #[serde(other)]
    Unproven,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GoalTransitionDecision {
    Continue,
    Complete,
    Pause,
    Stop,
    NeedsUser,
    NeedsAuthority,
    NeedsOperatorAuth,
    Rollover,
    BudgetExhausted,
    NoProgressExhausted,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GoalTransitionSurface {
    CampaignFinal,
    OrdinaryFinal,
    ProgressOnly,
    TerminalNotice,
    FailureNotice,
    SuppressStale,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoalTransitionInput {
    pub queue_id: Option<String>,
    pub event: GoalTransitionEventKind,
    pub runtime_status: String,
    pub runtime_reason: String,
    pub goal_status: Option<String>,
    pub lineage_id: Option<String>,
    pub campaign_family_id: Option<String>,
    pub lane_digest: Option<String>,
    pub virtual_session_id: Option<String>,
    pub backend_context_generation: Option<String>,
    pub source_thread_id: Option<String>,
    pub source_turn_id: Option<String>,
    pub goal_checksum: Option<String>,
    pub source_slice_generation: u64,
    pub decision_generation: u64,
    pub authority: GoalTransitionAuthority,
    pub relation: GoalTransitionRelation,
    pub retryable_failure: bool,
    pub context_rollover_required: bool,
    pub budget_exhausted: bool,
    pub no_progress_exhausted: bool,
    pub observed_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GoalTransitionReceiptV1 {
    pub schema: String,
    pub slice_schema: String,
    pub queue_id: Option<String>,
    pub event: GoalTransitionEventKind,
    pub runtime_status: String,
    pub runtime_reason: String,
    pub goal_status: Option<String>,
    pub lineage_id: Option<String>,
    pub campaign_family_id: Option<String>,
    pub lane_digest: Option<String>,
    pub virtual_session_id: Option<String>,
    pub backend_context_generation: Option<String>,
    pub source_thread_id: Option<String>,
    pub source_turn_id: Option<String>,
    pub goal_checksum: Option<String>,
    pub source_slice_generation: u64,
    pub decision_generation: u64,
    pub authority: GoalTransitionAuthority,
    #[serde(default)]
    pub relation: GoalTransitionRelation,
    pub decision: GoalTransitionDecision,
    pub surface: GoalTransitionSurface,
    pub schedule_continuation: bool,
    pub allow_campaign_final: bool,
    pub terminal: bool,
    pub reason: String,
    pub observed_at_ms: i64,
}

pub fn goal_transition_receipts_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("goal-lineage")
        .join("transition-receipts.jsonl")
}

pub fn evaluate_goal_transition(input: GoalTransitionInput) -> GoalTransitionReceiptV1 {
    let normalized_goal_status = input
        .goal_status
        .as_deref()
        .map(normalize_status)
        .unwrap_or_default();
    let goal_status_is_active = matches!(
        normalized_goal_status.as_str(),
        "active" | "running" | "inprogress"
    );
    let campaign_authorized = input.authority == GoalTransitionAuthority::Ready
        && matches!(
            input.relation,
            GoalTransitionRelation::CurrentGoalSlice
                | GoalTransitionRelation::AuthorizedCampaignContinuation
        );
    let active_goal = goal_status_is_active && campaign_authorized;
    let (decision, surface, schedule_continuation, terminal, reason) = if matches!(
        input.event,
        GoalTransitionEventKind::NewerSteer | GoalTransitionEventKind::OlderCompletion
    ) {
        (
            GoalTransitionDecision::Stop,
            GoalTransitionSurface::SuppressStale,
            false,
            true,
            if input.event == GoalTransitionEventKind::OlderCompletion {
                "a newer authoritative goal lineage suppresses this older slice completion"
                    .to_string()
            } else {
                "newer user steer preempts the older slice and suppresses its stale final"
                    .to_string()
            },
        )
    } else if input.event == GoalTransitionEventKind::OperatorStop {
        (
            GoalTransitionDecision::Stop,
            GoalTransitionSurface::TerminalNotice,
            false,
            true,
            "operator stop terminates this goal slice and campaign continuation".to_string(),
        )
    } else if input.event == GoalTransitionEventKind::AuthDeferred {
        (
            GoalTransitionDecision::NeedsOperatorAuth,
            GoalTransitionSurface::ProgressOnly,
            false,
            false,
            "backend auth is not ready; defer without a normal-channel credential or final surface"
                .to_string(),
        )
    } else if campaign_authorized
        && matches!(
            normalized_goal_status.as_str(),
            "completed" | "complete" | "succeeded" | "achieved"
        )
    {
        (
            GoalTransitionDecision::Complete,
            GoalTransitionSurface::CampaignFinal,
            false,
            true,
            "authoritative goal status is complete".to_string(),
        )
    } else if campaign_authorized && matches!(normalized_goal_status.as_str(), "paused" | "pause") {
        (
            GoalTransitionDecision::Pause,
            GoalTransitionSurface::TerminalNotice,
            false,
            true,
            "authoritative goal status is paused".to_string(),
        )
    } else if campaign_authorized
        && matches!(
            normalized_goal_status.as_str(),
            "stopped" | "stop" | "canceled" | "cancelled"
        )
    {
        (
            GoalTransitionDecision::Stop,
            GoalTransitionSurface::TerminalNotice,
            false,
            true,
            "authoritative goal status is stopped".to_string(),
        )
    } else if campaign_authorized
        && matches!(
            normalized_goal_status.as_str(),
            "needsuser" | "needsinput" | "needsapproval"
        )
    {
        (
            GoalTransitionDecision::NeedsUser,
            GoalTransitionSurface::TerminalNotice,
            false,
            true,
            "authoritative goal status requires user input or authority".to_string(),
        )
    } else if campaign_authorized
        && matches!(
            normalized_goal_status.as_str(),
            "needsauthority" | "needsauthorization" | "needsauthorisation"
        )
    {
        (
            GoalTransitionDecision::NeedsAuthority,
            GoalTransitionSurface::TerminalNotice,
            false,
            true,
            "authoritative goal status requires additional authority".to_string(),
        )
    } else if campaign_authorized
        && matches!(
            normalized_goal_status.as_str(),
            "needsoperatorauth" | "operatorauthrequired" | "authenticationrequired"
        )
    {
        (
            GoalTransitionDecision::NeedsOperatorAuth,
            GoalTransitionSurface::TerminalNotice,
            false,
            true,
            "authoritative goal status requires operator authentication".to_string(),
        )
    } else if goal_status_is_active && !campaign_authorized {
        (
            GoalTransitionDecision::NeedsAuthority,
            GoalTransitionSurface::ProgressOnly,
            false,
            false,
            format!(
                "active goal cannot transition autonomously because exact authority is {:?}",
                input.authority
            ),
        )
    } else if input.budget_exhausted {
        (
            GoalTransitionDecision::BudgetExhausted,
            GoalTransitionSurface::TerminalNotice,
            false,
            true,
            "campaign budget is exhausted".to_string(),
        )
    } else if input.no_progress_exhausted {
        (
            GoalTransitionDecision::NoProgressExhausted,
            GoalTransitionSurface::TerminalNotice,
            false,
            true,
            "campaign no-progress breaker is exhausted".to_string(),
        )
    } else if active_goal {
        match input.event {
            GoalTransitionEventKind::CompactRollover => (
                GoalTransitionDecision::Rollover,
                GoalTransitionSurface::ProgressOnly,
                true,
                false,
                "active goal crosses a verified compact/rollover boundary".to_string(),
            ),
            GoalTransitionEventKind::ToolTimeout if input.context_rollover_required => (
                GoalTransitionDecision::Rollover,
                GoalTransitionSurface::ProgressOnly,
                true,
                false,
                "active goal tool timeout requires a fresh context generation".to_string(),
            ),
            GoalTransitionEventKind::RuntimeFailure if !input.retryable_failure => (
                GoalTransitionDecision::Failed,
                GoalTransitionSurface::FailureNotice,
                false,
                true,
                "active goal hit a non-retryable runtime failure".to_string(),
            ),
            _ => (
                GoalTransitionDecision::Continue,
                GoalTransitionSurface::ProgressOnly,
                true,
                false,
                "slice ended while the authoritative goal remains active".to_string(),
            ),
        }
    } else {
        match input.event {
            GoalTransitionEventKind::NormalCompletion
            | GoalTransitionEventKind::DrainCompletion => (
                GoalTransitionDecision::Complete,
                GoalTransitionSurface::OrdinaryFinal,
                false,
                true,
                "completed non-goal turn owns its normal final surface".to_string(),
            ),
            GoalTransitionEventKind::ProcessRestart
                if input.runtime_status.eq_ignore_ascii_case("completed") =>
            {
                (
                    GoalTransitionDecision::Complete,
                    GoalTransitionSurface::OrdinaryFinal,
                    false,
                    true,
                    "restart reconciliation recovered an already completed non-goal turn"
                        .to_string(),
                )
            }
            GoalTransitionEventKind::CompactRollover => (
                GoalTransitionDecision::Rollover,
                GoalTransitionSurface::ProgressOnly,
                true,
                false,
                "runtime crossed a verified compact/rollover boundary".to_string(),
            ),
            GoalTransitionEventKind::ProcessRestart if input.retryable_failure => (
                GoalTransitionDecision::Continue,
                GoalTransitionSurface::ProgressOnly,
                true,
                false,
                "restart reconciliation found resumable slice work".to_string(),
            ),
            GoalTransitionEventKind::AbsoluteTimeout
            | GoalTransitionEventKind::IdleTimeout
            | GoalTransitionEventKind::ToolTimeout
            | GoalTransitionEventKind::RuntimeFailure
                if input.retryable_failure =>
            {
                (
                    GoalTransitionDecision::Continue,
                    GoalTransitionSurface::ProgressOnly,
                    true,
                    false,
                    "legacy non-goal work remains retryable under the unified transition"
                        .to_string(),
                )
            }
            _ => (
                GoalTransitionDecision::Failed,
                GoalTransitionSurface::FailureNotice,
                false,
                true,
                "runtime slice failed without an active authoritative goal".to_string(),
            ),
        }
    };
    GoalTransitionReceiptV1 {
        schema: GOAL_TRANSITION_SCHEMA.to_string(),
        slice_schema: GOAL_SLICE_SCHEMA.to_string(),
        queue_id: input.queue_id,
        event: input.event,
        runtime_status: input.runtime_status,
        runtime_reason: input.runtime_reason,
        goal_status: input.goal_status,
        lineage_id: input.lineage_id,
        campaign_family_id: input.campaign_family_id,
        lane_digest: input.lane_digest,
        virtual_session_id: input.virtual_session_id,
        backend_context_generation: input.backend_context_generation,
        source_thread_id: input.source_thread_id,
        source_turn_id: input.source_turn_id,
        goal_checksum: input.goal_checksum,
        source_slice_generation: input.source_slice_generation,
        decision_generation: input.decision_generation,
        authority: input.authority,
        relation: input.relation,
        decision,
        surface,
        schedule_continuation,
        allow_campaign_final: surface == GoalTransitionSurface::CampaignFinal,
        terminal,
        reason,
        observed_at_ms: input.observed_at_ms,
    }
}

pub fn record_goal_transition(
    harness_home: impl AsRef<Path>,
    receipt: &GoalTransitionReceiptV1,
) -> io::Result<PathBuf> {
    let file = goal_transition_receipts_file(harness_home);
    append_jsonl_value(&file, receipt)?;
    Ok(file)
}

fn normalize_status(value: &str) -> String {
    value
        .trim()
        .to_ascii_lowercase()
        .replace(['-', '_', ' '], "")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unified_transition_table_covers_all_incident_events() {
        let cases = [
            (
                GoalTransitionEventKind::NormalCompletion,
                true,
                GoalTransitionDecision::Continue,
            ),
            (
                GoalTransitionEventKind::DrainCompletion,
                true,
                GoalTransitionDecision::Continue,
            ),
            (
                GoalTransitionEventKind::AbsoluteTimeout,
                true,
                GoalTransitionDecision::Continue,
            ),
            (
                GoalTransitionEventKind::IdleTimeout,
                true,
                GoalTransitionDecision::Continue,
            ),
            (
                GoalTransitionEventKind::ToolTimeout,
                true,
                GoalTransitionDecision::Continue,
            ),
            (
                GoalTransitionEventKind::CompactRollover,
                true,
                GoalTransitionDecision::Rollover,
            ),
            (
                GoalTransitionEventKind::ProcessRestart,
                true,
                GoalTransitionDecision::Continue,
            ),
            (
                GoalTransitionEventKind::RuntimeFailure,
                true,
                GoalTransitionDecision::Continue,
            ),
        ];
        for (event, retryable, expected) in cases {
            let receipt = evaluate_goal_transition(input(event, Some("active"), retryable));
            assert_eq!(receipt.decision, expected, "event={event:?}");
            assert!(receipt.schedule_continuation);
            assert!(!receipt.allow_campaign_final);
        }
        let auth = evaluate_goal_transition(input(
            GoalTransitionEventKind::AuthDeferred,
            Some("active"),
            true,
        ));
        assert_eq!(auth.decision, GoalTransitionDecision::NeedsOperatorAuth);
        assert!(!auth.schedule_continuation);
        assert!(!auth.allow_campaign_final);
        let stop = evaluate_goal_transition(input(
            GoalTransitionEventKind::OperatorStop,
            Some("active"),
            false,
        ));
        assert_eq!(stop.decision, GoalTransitionDecision::Stop);
        assert_eq!(stop.surface, GoalTransitionSurface::TerminalNotice);
        let steer = evaluate_goal_transition(input(
            GoalTransitionEventKind::NewerSteer,
            Some("active"),
            false,
        ));
        assert_eq!(steer.surface, GoalTransitionSurface::SuppressStale);
    }

    #[test]
    fn active_slice_never_becomes_campaign_final_and_terminal_goal_does() {
        let active = evaluate_goal_transition(input(
            GoalTransitionEventKind::NormalCompletion,
            Some("active"),
            false,
        ));
        assert_eq!(active.surface, GoalTransitionSurface::ProgressOnly);
        assert!(!active.terminal);
        let complete = evaluate_goal_transition(input(
            GoalTransitionEventKind::NormalCompletion,
            Some("completed"),
            false,
        ));
        assert_eq!(complete.decision, GoalTransitionDecision::Complete);
        assert!(complete.allow_campaign_final);
        assert!(complete.terminal);
        let missing = evaluate_goal_transition(GoalTransitionInput {
            authority: GoalTransitionAuthority::Missing,
            ..input(
                GoalTransitionEventKind::NormalCompletion,
                Some("active"),
                false,
            )
        });
        assert_eq!(missing.decision, GoalTransitionDecision::NeedsAuthority);
        assert!(!missing.schedule_continuation);
    }

    #[test]
    fn historical_completed_goal_does_not_authorize_campaign_final() {
        let historical = evaluate_goal_transition(GoalTransitionInput {
            authority: GoalTransitionAuthority::Missing,
            relation: GoalTransitionRelation::HistoricalState,
            ..input(
                GoalTransitionEventKind::NormalCompletion,
                Some("completed"),
                false,
            )
        });

        assert_ne!(historical.surface, GoalTransitionSurface::CampaignFinal);
        assert!(!historical.allow_campaign_final);
    }

    fn input(
        event: GoalTransitionEventKind,
        goal_status: Option<&str>,
        retryable_failure: bool,
    ) -> GoalTransitionInput {
        GoalTransitionInput {
            queue_id: Some("queue".to_string()),
            event,
            runtime_status: "fixture".to_string(),
            runtime_reason: "fixture".to_string(),
            goal_status: goal_status.map(ToString::to_string),
            lineage_id: Some("lineage".to_string()),
            campaign_family_id: Some("campaign".to_string()),
            lane_digest: Some("lane".to_string()),
            virtual_session_id: Some("virtual".to_string()),
            backend_context_generation: Some("generation".to_string()),
            source_thread_id: Some("thread".to_string()),
            source_turn_id: Some("turn".to_string()),
            goal_checksum: Some("goal".to_string()),
            source_slice_generation: 1,
            decision_generation: 1,
            authority: GoalTransitionAuthority::Ready,
            relation: GoalTransitionRelation::CurrentGoalSlice,
            retryable_failure,
            context_rollover_required: false,
            budget_exhausted: false,
            no_progress_exhausted: false,
            observed_at_ms: 1,
        }
    }
}
