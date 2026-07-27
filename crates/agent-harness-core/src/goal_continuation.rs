use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use ring::digest;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::append_jsonl_value;
use crate::channel_state::{ChannelStateLane, read_channel_session_state_v2};
use crate::config::harness_config_candidates;
use crate::context_rollover::{
    ContextRolloverRequeuePreparedOptions, RuntimeContinuationMetadata,
    derive_virtual_session_id_v2, requeue_prepared_context_rollover, root_working_session_key,
};
use crate::goal_closure::{GoalClosureIntentV1, goal_closure_intents_file};
use crate::goal_lineage::{
    GoalLineageDisposition, GoalLineageDoctorOptions, GoalLineageDoctorStatus,
    GoalProjectionTurnRelationV1, run_goal_lineage_doctor,
};
use crate::goal_transition::{
    GoalTransitionAuthority, GoalTransitionDecision, GoalTransitionReceiptV1,
    GoalTransitionRelation, goal_transition_receipts_file,
};
use crate::lane::FullLaneKeyV1;
use crate::runtime_worker::RuntimeQueuePreparedItem;
use crate::{
    ChannelOutboundMessage, ChannelOutboundMessageKind, ChannelOutboxAppendOutcome,
    append_channel_outbox_message,
};

pub const GOAL_CONTINUATION_INTENT_SCHEMA: &str = "agent-harness.goal-continuation-intent.v1";
pub const TASK_CONTINUATION_INTENT_SCHEMA: &str = "agent-harness.task-continuation-intent.v1";
pub const GOAL_CONTINUATION_RECONCILIATION_ERROR_SCHEMA: &str =
    "agent-harness.goal-continuation-reconciliation-error.v1";
pub const GOAL_CONTINUATION_FENCE_SCHEMA: &str = "agent-harness.goal-continuation-fence.v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GoalAutonomyMode {
    Disabled,
    Observe,
    Active,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoalAutonomyActivation {
    pub mode: GoalAutonomyMode,
    pub configured_mode: GoalAutonomyMode,
    pub lane_allowed: bool,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GoalContinuationIntentStatus {
    Committed,
    Enqueued,
    Acknowledged,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GoalContinuationFenceV1 {
    pub schema: String,
    pub fence_key: String,
    pub intent_key: String,
    pub parent_queue_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_queue_id: Option<String>,
    pub lane_digest: String,
    pub virtual_session_id: String,
    pub disposition: String,
    pub reason: String,
    pub observed_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GoalContinuationLeaseGate {
    NotContinuation,
    Acknowledged(GoalContinuationIntentV1),
    Suppressed(GoalContinuationFenceV1),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GoalContinuationReconciliationErrorV1 {
    pub schema: String,
    pub error_key: String,
    pub disposition: String,
    pub error_kind: String,
    pub record_digest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record_line: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub intent_key: Option<String>,
    pub reason: String,
    pub observed_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GoalContinuationIntentV1 {
    pub schema: String,
    pub intent_key: String,
    pub status: GoalContinuationIntentStatus,
    pub parent_queue_id: String,
    pub child_queue_id: Option<String>,
    pub working_session_key: String,
    pub lane_digest: String,
    pub virtual_session_id: String,
    pub lineage_id: String,
    pub goal_checksum: String,
    pub campaign_family_id: String,
    pub backend_context_generation: String,
    pub source_slice_generation: u64,
    pub decision_generation: u64,
    pub campaign_slice_generation: u64,
    pub recovery_continuation_index: u64,
    pub decision: GoalTransitionDecision,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub human_input_generation: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub goal_relation_at_commit: Option<GoalTransitionRelation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub goal_status_at_commit: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority_kind: Option<crate::ContinuationAuthorityKindV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority_version: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority_checksum: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_item_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_item_version: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disposition_recovery_depth: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shell_recovery_depth: Option<u64>,
    pub reason: String,
    pub observed_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GoalContinuationDowngradeReadinessV1 {
    pub schema: &'static str,
    pub safe_for_legacy_binary: bool,
    pub open_new_semantics_intent_keys: Vec<String>,
    pub fenced_open_intent_keys: Vec<String>,
    pub corrupt_recoverable_intent_keys: Vec<String>,
    pub blockers: Vec<String>,
}

pub fn goal_continuation_intents_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("goal-lineage")
        .join("continuation-intents.jsonl")
}

pub fn goal_continuation_reconciliation_errors_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("goal-lineage")
        .join("continuation-reconciliation-errors.jsonl")
}

pub fn goal_continuation_fences_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("goal-lineage")
        .join("continuation-fences.jsonl")
}

pub fn assess_goal_continuation_downgrade_readiness(
    harness_home: impl AsRef<Path>,
) -> io::Result<GoalContinuationDowngradeReadinessV1> {
    let harness_home = harness_home.as_ref();
    let (latest, quarantines) = latest_intents_for_reconciliation(harness_home)?;
    let open = latest
        .values()
        .filter(|intent| {
            matches!(
                intent.status,
                GoalContinuationIntentStatus::Committed | GoalContinuationIntentStatus::Enqueued
            )
        })
        .collect::<Vec<_>>();
    let mut open_new_semantics_intent_keys = open
        .iter()
        .filter(|intent| {
            intent.human_input_generation.is_some()
                || intent.goal_relation_at_commit.is_some()
                || intent.goal_status_at_commit.is_some()
        })
        .map(|intent| intent.intent_key.clone())
        .collect::<Vec<_>>();
    let mut fenced_open_intent_keys = Vec::new();
    for intent in &open {
        if latest_goal_continuation_fence(harness_home, &intent.intent_key)?.is_some() {
            fenced_open_intent_keys.push(intent.intent_key.clone());
        }
    }
    let open_keys = open
        .iter()
        .map(|intent| intent.intent_key.as_str())
        .collect::<BTreeSet<_>>();
    let mut corrupt_recoverable_intent_keys = quarantines
        .into_iter()
        .filter_map(|quarantine| quarantine.intent_key)
        .filter(|key| open_keys.contains(key.as_str()))
        .collect::<Vec<_>>();
    open_new_semantics_intent_keys.sort();
    open_new_semantics_intent_keys.dedup();
    fenced_open_intent_keys.sort();
    fenced_open_intent_keys.dedup();
    corrupt_recoverable_intent_keys.sort();
    corrupt_recoverable_intent_keys.dedup();
    let mut blockers = Vec::new();
    if !open_new_semantics_intent_keys.is_empty() {
        blockers.push(format!(
            "{} open continuation intent(s) require human-generation/current-authority semantics ignored by the legacy binary",
            open_new_semantics_intent_keys.len()
        ));
    }
    if !fenced_open_intent_keys.is_empty() {
        blockers.push(format!(
            "{} open continuation intent(s) rely on the separate fence overlay ignored by the legacy binary",
            fenced_open_intent_keys.len()
        ));
    }
    if !corrupt_recoverable_intent_keys.is_empty() {
        blockers.push(format!(
            "{} recoverable open continuation intent(s) have later ledger corruption that must be settled before downgrade",
            corrupt_recoverable_intent_keys.len()
        ));
    }
    Ok(GoalContinuationDowngradeReadinessV1 {
        schema: "agent-harness.goal-continuation-downgrade-readiness.v1",
        safe_for_legacy_binary: blockers.is_empty(),
        open_new_semantics_intent_keys,
        fenced_open_intent_keys,
        corrupt_recoverable_intent_keys,
        blockers,
    })
}

pub fn load_goal_autonomy_activation(
    harness_home: impl AsRef<Path>,
    lane_digest: Option<&str>,
) -> io::Result<GoalAutonomyActivation> {
    load_goal_autonomy_activation_scoped(harness_home.as_ref(), lane_digest, false)
}

pub fn load_goal_autonomy_activation_for_exact_goal(
    harness_home: impl AsRef<Path>,
    lane_digest: Option<&str>,
    exact_current_authority: bool,
) -> io::Result<GoalAutonomyActivation> {
    load_goal_autonomy_activation_scoped(
        harness_home.as_ref(),
        lane_digest,
        exact_current_authority,
    )
}

fn load_goal_autonomy_activation_scoped(
    harness_home: &Path,
    lane_digest: Option<&str>,
    exact_current_authority: bool,
) -> io::Result<GoalAutonomyActivation> {
    let mut configured_mode = GoalAutonomyMode::Observe;
    let mut active_lane_digests = Vec::new();
    let mut goal_autonomy_configured = false;
    for candidate in harness_config_candidates(harness_home) {
        let text = match fs::read_to_string(&candidate) {
            Ok(text) => text,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        let value: Value = serde_json::from_str(&text).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid harness config {}: {error}", candidate.display()),
            )
        })?;
        if let Some(goal) = value.get("goalAutonomy") {
            goal_autonomy_configured = true;
            configured_mode = match goal.get("mode").and_then(Value::as_str) {
                Some("disabled") => GoalAutonomyMode::Disabled,
                Some("active") => GoalAutonomyMode::Active,
                Some("observe") | None => GoalAutonomyMode::Observe,
                Some(other) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unsupported goalAutonomy.mode `{other}`"),
                    ));
                }
            };
            active_lane_digests = goal
                .get("activeLaneDigests")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(ToString::to_string)
                .collect();
        }
        break;
    }
    if !goal_autonomy_configured && exact_current_authority && lane_digest.is_some() {
        return Ok(GoalAutonomyActivation {
            mode: GoalAutonomyMode::Active,
            configured_mode: GoalAutonomyMode::Active,
            lane_allowed: true,
            reason: "goal autonomy is implicitly active for an exact current-owned authorized goal"
                .to_string(),
        });
    }
    let lane_allowed = lane_digest
        .map(|digest| {
            active_lane_digests
                .iter()
                .any(|candidate| candidate == digest)
        })
        .unwrap_or(false);
    let (mode, reason) = match configured_mode {
        GoalAutonomyMode::Disabled => (
            GoalAutonomyMode::Disabled,
            "goal autonomy is explicitly disabled".to_string(),
        ),
        GoalAutonomyMode::Observe => (
            GoalAutonomyMode::Observe,
            "goal autonomy defaults to observation-only behavior".to_string(),
        ),
        GoalAutonomyMode::Active if lane_allowed => (
            GoalAutonomyMode::Active,
            "goal autonomy is active for the exact configured lane digest".to_string(),
        ),
        GoalAutonomyMode::Active => (
            GoalAutonomyMode::Observe,
            "goal autonomy requested active mode but the exact lane is not in activeLaneDigests"
                .to_string(),
        ),
    };
    Ok(GoalAutonomyActivation {
        mode,
        configured_mode,
        lane_allowed,
        reason,
    })
}

pub fn commit_goal_continuation_intent(
    harness_home: impl AsRef<Path>,
    transition: &GoalTransitionReceiptV1,
    working_session_key: &str,
    continuation: &RuntimeContinuationMetadata,
    now_ms: i64,
) -> io::Result<GoalContinuationIntentV1> {
    let exact_current_authority = transition.authority == GoalTransitionAuthority::Ready
        && matches!(
            transition.relation,
            GoalTransitionRelation::CurrentGoalSlice
                | GoalTransitionRelation::AuthorizedCampaignContinuation
        )
        && transition
            .goal_status
            .as_deref()
            .is_some_and(goal_status_is_active);
    if !transition.schedule_continuation || !exact_current_authority {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "goal continuation intent requires a scheduled active transition with exact current-owned ready authority",
        ));
    }
    let parent_queue_id = required(&transition.queue_id, "queueId")?;
    let lane_digest = required(&transition.lane_digest, "laneDigest")?;
    let virtual_session_id = required(&transition.virtual_session_id, "virtualSessionId")?;
    let lineage_id = required(&transition.lineage_id, "lineageId")?;
    let goal_checksum = required(&transition.goal_checksum, "goalChecksum")?;
    let campaign_family_id = required(&transition.campaign_family_id, "campaignFamilyId")?;
    let backend_context_generation = required(
        &transition.backend_context_generation,
        "backendContextGeneration",
    )?;
    if working_session_key.trim().is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "workingSessionKey is required for goal continuation",
        ));
    }
    let source_slice_generation = transition.source_slice_generation;
    let decision_generation = transition.decision_generation;
    let campaign_slice_generation = source_slice_generation.saturating_add(1);
    let recovery_continuation_index = continuation.continuation_index.unwrap_or(0);
    let human_input_generation =
        human_input_generation_at_parent(harness_home.as_ref(), parent_queue_id, lane_digest)?.0;
    let intent_key = sha256_hex(&canonical_identity_bytes(&[
        "goal-continuation-intent-v1",
        lane_digest,
        virtual_session_id,
        lineage_id,
        goal_checksum,
        &source_slice_generation.to_string(),
        &decision_generation.to_string(),
    ]));
    let receipt = GoalContinuationIntentV1 {
        schema: GOAL_CONTINUATION_INTENT_SCHEMA.to_string(),
        intent_key,
        status: GoalContinuationIntentStatus::Committed,
        parent_queue_id: parent_queue_id.to_string(),
        child_queue_id: None,
        working_session_key: working_session_key.to_string(),
        lane_digest: lane_digest.to_string(),
        virtual_session_id: virtual_session_id.to_string(),
        lineage_id: lineage_id.to_string(),
        goal_checksum: goal_checksum.to_string(),
        campaign_family_id: campaign_family_id.to_string(),
        backend_context_generation: backend_context_generation.to_string(),
        source_slice_generation,
        decision_generation,
        campaign_slice_generation,
        recovery_continuation_index,
        decision: transition.decision,
        human_input_generation: Some(human_input_generation),
        goal_relation_at_commit: Some(transition.relation),
        goal_status_at_commit: transition.goal_status.clone(),
        authority_kind: Some(crate::ContinuationAuthorityKindV1::Goal),
        authority_id: Some(lineage_id.to_string()),
        authority_version: Some(source_slice_generation),
        authority_checksum: Some(goal_checksum.to_string()),
        checkpoint_digest: None,
        active_item_id: None,
        active_item_version: None,
        disposition_recovery_depth: None,
        shell_recovery_depth: continuation.shell_recovery_depth,
        reason: "continuation intent committed before child enqueue".to_string(),
        observed_at_ms: now_ms,
    };
    if let Some(existing) =
        latest_goal_continuation_intent(harness_home.as_ref(), &receipt.intent_key)?
    {
        ensure_same_intent(&existing, &receipt)?;
        return Ok(existing);
    }
    append_jsonl_value(&goal_continuation_intents_file(harness_home), &receipt)?;
    Ok(receipt)
}

pub fn commit_task_continuation_intent(
    harness_home: impl AsRef<Path>,
    parent_queue_id: &str,
    working_session_key: &str,
    continuation: &RuntimeContinuationMetadata,
    authority: &crate::ContinuationAuthoritySnapshotV1,
    source_slice_generation: u64,
    decision_generation: u64,
    disposition_recovery: bool,
    now_ms: i64,
) -> io::Result<GoalContinuationIntentV1> {
    if parent_queue_id.trim().is_empty() || working_session_key.trim().is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "task continuation requires parent queue and working session identities",
        ));
    }
    let campaign_slice_generation = source_slice_generation.saturating_add(1);
    let recovery_continuation_index = continuation.continuation_index.unwrap_or(0);
    let human_input_generation = human_input_generation_at_parent(
        harness_home.as_ref(),
        parent_queue_id,
        &authority.exact_lane_digest,
    )?
    .0;
    let authority_kind = match authority.kind {
        crate::ContinuationAuthorityKindV1::Goal => "goal",
        crate::ContinuationAuthorityKindV1::OperationPlan => "operation-plan",
        crate::ContinuationAuthorityKindV1::ExplicitCheckpoint => "explicit-checkpoint",
    };
    let intent_key = sha256_hex(&canonical_identity_bytes(&[
        "task-continuation-intent-v1",
        &authority.exact_lane_digest,
        &authority.virtual_session_id,
        parent_queue_id,
        &source_slice_generation.to_string(),
        authority_kind,
        &authority.authority_id,
        &authority.authority_version.to_string(),
        &authority.checkpoint_digest,
        if disposition_recovery {
            "disposition-recovery"
        } else {
            "task-continuation"
        },
    ]));
    let receipt = GoalContinuationIntentV1 {
        schema: TASK_CONTINUATION_INTENT_SCHEMA.to_string(),
        intent_key,
        status: GoalContinuationIntentStatus::Committed,
        parent_queue_id: parent_queue_id.to_string(),
        child_queue_id: None,
        working_session_key: working_session_key.to_string(),
        lane_digest: authority.exact_lane_digest.clone(),
        virtual_session_id: authority.virtual_session_id.clone(),
        lineage_id: authority.authority_id.clone(),
        goal_checksum: authority.authority_checksum.clone(),
        campaign_family_id: format!("task:{}", authority.authority_id),
        backend_context_generation: authority.checkpoint_digest.clone(),
        source_slice_generation,
        decision_generation,
        campaign_slice_generation,
        recovery_continuation_index,
        decision: GoalTransitionDecision::Continue,
        human_input_generation: Some(human_input_generation),
        goal_relation_at_commit: None,
        goal_status_at_commit: None,
        authority_kind: Some(authority.kind),
        authority_id: Some(authority.authority_id.clone()),
        authority_version: Some(authority.authority_version),
        authority_checksum: Some(authority.authority_checksum.clone()),
        checkpoint_digest: Some(authority.checkpoint_digest.clone()),
        active_item_id: authority.active_item_id.clone(),
        active_item_version: authority.active_item_version,
        disposition_recovery_depth: disposition_recovery.then_some(1),
        shell_recovery_depth: continuation.shell_recovery_depth,
        reason: if disposition_recovery {
            "disposition recovery intent committed before observation-only child enqueue"
                .to_string()
        } else {
            "task continuation intent committed before child enqueue".to_string()
        },
        observed_at_ms: now_ms,
    };
    if let Some(existing) =
        latest_goal_continuation_intent(harness_home.as_ref(), &receipt.intent_key)?
    {
        ensure_same_intent(&existing, &receipt)?;
        return Ok(existing);
    }
    append_jsonl_value(&goal_continuation_intents_file(harness_home), &receipt)?;
    Ok(receipt)
}

pub fn ensure_goal_continuation_enqueued(
    harness_home: impl AsRef<Path>,
    intent_key: &str,
    now_ms: i64,
) -> io::Result<GoalContinuationIntentV1> {
    let harness_home = harness_home.as_ref();
    match reconcile_one_goal_continuation_intent(harness_home, intent_key, now_ms)? {
        EnsureGoalContinuationOutcome::Intent(receipt) => Ok(receipt),
        EnsureGoalContinuationOutcome::Fenced(fence) => Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "goal continuation intent {} is durably fenced: {}",
                fence.intent_key, fence.reason
            ),
        )),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum EnsureGoalContinuationOutcome {
    Intent(GoalContinuationIntentV1),
    Fenced(GoalContinuationFenceV1),
}

fn reconcile_one_goal_continuation_intent(
    harness_home: &Path,
    intent_key: &str,
    now_ms: i64,
) -> io::Result<EnsureGoalContinuationOutcome> {
    let transaction_file = goal_continuation_transaction_file(harness_home, intent_key);
    crate::logging::with_jsonl_append_lock(&transaction_file, || {
        let RecoverableIntentRead { latest, corruption } =
            latest_recoverable_intent_for_key(harness_home, intent_key)?;
        let latest = latest.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                corruption
                    .clone()
                    .unwrap_or_else(|| "goal continuation intent is missing".to_string()),
            )
        })?;
        if let Some(reason) = corruption {
            let child_queue_id = latest
                .child_queue_id
                .clone()
                .or(find_child_queue_for_intent(harness_home, intent_key)?);
            let fence = settle_revalidation_failure_and_fence(
                harness_home,
                &latest,
                child_queue_id.as_deref(),
                IntentRevalidationFailure::FailVisible(format!(
                    "continuation intent ledger is corrupt after its last recoverable state: {reason}"
                )),
                "continuation ledger corruption failed closed",
                now_ms,
            )?;
            return Ok(EnsureGoalContinuationOutcome::Fenced(fence));
        }
        ensure_loaded_goal_continuation_enqueued(harness_home, latest, now_ms)
    })
}

fn ensure_loaded_goal_continuation_enqueued(
    harness_home: &Path,
    mut latest: GoalContinuationIntentV1,
    now_ms: i64,
) -> io::Result<EnsureGoalContinuationOutcome> {
    let discovered_child_queue_id = match latest.child_queue_id.clone() {
        Some(child_queue_id) => Some(child_queue_id),
        None => find_child_queue_for_intent(harness_home, &latest.intent_key)?,
    };
    if let Some(fence) = latest_goal_continuation_fence(harness_home, &latest.intent_key)? {
        ensure_preexisting_fence_is_settled(harness_home, &latest, &fence, now_ms)?;
        if let Some(child_queue_id) = discovered_child_queue_id.as_deref() {
            quarantine_fenced_child(harness_home, child_queue_id, &fence.reason, now_ms)?;
        }
        return Ok(EnsureGoalContinuationOutcome::Fenced(fence));
    }
    if latest.status == GoalContinuationIntentStatus::Acknowledged {
        return Ok(EnsureGoalContinuationOutcome::Intent(latest));
    }
    match upgrade_open_legacy_intent(harness_home, &latest) {
        Ok((upgraded, true)) => {
            let status = upgraded.status;
            let child_queue_id = upgraded.child_queue_id.clone();
            latest = append_intent_state(
                harness_home,
                upgraded,
                status,
                child_queue_id,
                "legacy v1 continuation additive fields reconstructed from exact durable boundaries",
                now_ms,
            )?;
        }
        Ok((_, false)) => {}
        Err(failure) => {
            let fence = settle_revalidation_failure_and_fence(
                harness_home,
                &latest,
                discovered_child_queue_id.as_deref(),
                failure,
                "legacy continuation upgrade failed closed",
                now_ms,
            )?;
            return Ok(EnsureGoalContinuationOutcome::Fenced(fence));
        }
    }
    if let Err(failure) = validate_parent_lane_for_intent(harness_home, &latest) {
        let fence = settle_revalidation_failure_and_fence(
            harness_home,
            &latest,
            discovered_child_queue_id.as_deref(),
            failure,
            "continuation intent failed during restart revalidation",
            now_ms,
        )?;
        return Ok(EnsureGoalContinuationOutcome::Fenced(fence));
    }
    if let Some(child_queue_id) = discovered_child_queue_id {
        if latest.child_queue_id.as_deref() == Some(child_queue_id.as_str())
            && latest.status == GoalContinuationIntentStatus::Enqueued
        {
            return Ok(EnsureGoalContinuationOutcome::Intent(latest));
        }
        return append_intent_state(
            harness_home,
            latest,
            GoalContinuationIntentStatus::Enqueued,
            Some(child_queue_id),
            "reconciled an existing deterministic child after commit/enqueue interruption",
            now_ms,
        )
        .map(EnsureGoalContinuationOutcome::Intent);
    }
    let task_family = if latest.schema == TASK_CONTINUATION_INTENT_SCHEMA
        && latest.authority_kind == Some(crate::ContinuationAuthorityKindV1::ExplicitCheckpoint)
    {
        Some(crate::task_transition::read_task_family(
            harness_home,
            latest.authority_id.as_deref().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "task intent has no family id")
            })?,
        )?)
    } else {
        None
    };
    let report = requeue_prepared_context_rollover(ContextRolloverRequeuePreparedOptions {
        harness_home: harness_home.to_path_buf(),
        queue_id: latest.parent_queue_id.clone(),
        new_working_session_key: latest.working_session_key.clone(),
        reason: format!(
            "task continuation intent {} for campaign slice {}",
            latest.intent_key, latest.campaign_slice_generation
        ),
        now_ms,
        preserve_continuation_index: true,
        campaign_slice_generation: (latest.schema != TASK_CONTINUATION_INTENT_SCHEMA)
            .then_some(latest.campaign_slice_generation),
        task_slice_generation: (latest.schema == TASK_CONTINUATION_INTENT_SCHEMA
            && latest.authority_kind
                == Some(crate::ContinuationAuthorityKindV1::ExplicitCheckpoint))
        .then_some(latest.campaign_slice_generation),
        task_family_id: task_family.as_ref().map(|family| family.family_id.clone()),
        task_family_version: task_family.as_ref().map(|family| family.authority_version),
        task_root_queue_id: task_family
            .as_ref()
            .map(|family| family.root_queue_id.clone()),
        disposition_recovery_depth: latest.disposition_recovery_depth,
        shell_recovery_depth: latest.shell_recovery_depth,
        replacement_message_text: latest.disposition_recovery_depth.map(|_| {
            "Disposition recovery only: inspect the current task-family state without starting new long work or authorizing/replaying any external effect. Return exactly one valid agent-harness.drain-disposition.v1 marker for the current outcome."
                .to_string()
        }),
        continuation_intent_key: Some(latest.intent_key.clone()),
        completion_kind: Some(
            if latest.schema == TASK_CONTINUATION_INTENT_SCHEMA {
                match latest.authority_kind {
                    Some(crate::ContinuationAuthorityKindV1::ExplicitCheckpoint)
                        if latest.disposition_recovery_depth.is_some() => {
                            "task-disposition-recovery"
                        }
                    Some(crate::ContinuationAuthorityKindV1::ExplicitCheckpoint) => {
                        "task-checkpoint-continuation"
                    }
                    _ => "operation-plan-continuation",
                }
            } else {
                "goal-continuation"
            }
            .to_string(),
        ),
        allow_exact_state_bootstrap: true,
    })?;
    append_intent_state(
        harness_home,
        latest,
        GoalContinuationIntentStatus::Enqueued,
        Some(report.requeued_queue_id),
        "deterministic child was enqueued after the committed intent",
        now_ms,
    )
    .map(EnsureGoalContinuationOutcome::Intent)
}

pub fn reconcile_goal_continuation_intents(
    harness_home: impl AsRef<Path>,
    now_ms: i64,
) -> io::Result<Vec<GoalContinuationIntentV1>> {
    let harness_home = harness_home.as_ref();
    let (latest, quarantines) = latest_intents_for_reconciliation(harness_home)?;
    for quarantine in quarantines {
        let _ = append_reconciliation_error_once(harness_home, quarantine.receipt(now_ms));
    }
    let mut reconciled = Vec::new();
    for intent in latest.into_values() {
        if matches!(
            intent.status,
            GoalContinuationIntentStatus::Committed | GoalContinuationIntentStatus::Enqueued
        ) {
            match reconcile_one_goal_continuation_intent(harness_home, &intent.intent_key, now_ms) {
                Ok(EnsureGoalContinuationOutcome::Intent(receipt)) if receipt != intent => {
                    reconciled.push(receipt);
                }
                Ok(EnsureGoalContinuationOutcome::Intent(_))
                | Ok(EnsureGoalContinuationOutcome::Fenced(_)) => {}
                Err(error) => {
                    let deferred = ReconciliationQuarantine::for_intent_error(&intent, &error);
                    let _ =
                        append_reconciliation_error_once(harness_home, deferred.receipt(now_ms));
                }
            }
        }
    }
    Ok(reconciled)
}

pub fn acknowledge_goal_continuation_after_lease(
    harness_home: impl AsRef<Path>,
    item: &RuntimeQueuePreparedItem,
    now_ms: i64,
) -> io::Result<Option<GoalContinuationIntentV1>> {
    match gate_goal_continuation_after_lease(harness_home, item, now_ms)? {
        GoalContinuationLeaseGate::NotContinuation => Ok(None),
        GoalContinuationLeaseGate::Acknowledged(receipt) => Ok(Some(receipt)),
        GoalContinuationLeaseGate::Suppressed(fence) => Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "leased continuation child is durably fenced before execution: {}",
                fence.reason
            ),
        )),
    }
}

pub fn gate_goal_continuation_after_lease(
    harness_home: impl AsRef<Path>,
    item: &RuntimeQueuePreparedItem,
    now_ms: i64,
) -> io::Result<GoalContinuationLeaseGate> {
    let Some(intent_key) = item.continuation.continuation_intent_key.as_deref() else {
        return Ok(GoalContinuationLeaseGate::NotContinuation);
    };
    let harness_home = harness_home.as_ref();
    let transaction_file = goal_continuation_transaction_file(harness_home, intent_key);
    crate::logging::with_jsonl_append_lock(&transaction_file, || {
        let RecoverableIntentRead { latest, corruption } =
            latest_recoverable_intent_for_key(harness_home, intent_key)?;
        let mut latest = latest.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                corruption.clone().unwrap_or_else(|| {
                    "prepared goal child has no committed continuation intent".to_string()
                }),
            )
        })?;
        let owned_child = latest.child_queue_id.as_deref() == Some(item.queue_id.as_str())
            || find_child_queue_for_intent(harness_home, intent_key)?.as_deref()
                == Some(item.queue_id.as_str());
        if !owned_child {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "prepared goal child does not match the intent-owned queue id",
            ));
        }
        if let Some(reason) = corruption {
            let fence = settle_revalidation_failure_and_fence(
                harness_home,
                &latest,
                Some(&item.queue_id),
                IntentRevalidationFailure::FailVisible(format!(
                    "continuation intent ledger is corrupt after its last recoverable state: {reason}"
                )),
                "leased continuation ledger corruption failed closed",
                now_ms,
            )?;
            return Ok(GoalContinuationLeaseGate::Suppressed(fence));
        }
        if let Some(fence) = latest_goal_continuation_fence(harness_home, intent_key)? {
            ensure_preexisting_fence_is_settled(harness_home, &latest, &fence, now_ms)?;
            quarantine_fenced_child(harness_home, &item.queue_id, &fence.reason, now_ms)?;
            return Ok(GoalContinuationLeaseGate::Suppressed(fence));
        }
        match upgrade_open_legacy_intent(harness_home, &latest) {
            Ok((upgraded, true)) => {
                let status = upgraded.status;
                let child_queue_id = upgraded.child_queue_id.clone();
                latest = append_intent_state(
                    harness_home,
                    upgraded,
                    status,
                    child_queue_id,
                    "legacy v1 continuation additive fields reconstructed at the lease boundary",
                    now_ms,
                )?;
            }
            Ok((_, false)) => {}
            Err(failure) => {
                let fence = settle_revalidation_failure_and_fence(
                    harness_home,
                    &latest,
                    Some(&item.queue_id),
                    failure,
                    "leased legacy continuation upgrade failed closed",
                    now_ms,
                )?;
                return Ok(GoalContinuationLeaseGate::Suppressed(fence));
            }
        }
        if let Err(failure) = validate_parent_lane_for_intent(harness_home, &latest) {
            let fence = settle_revalidation_failure_and_fence(
                harness_home,
                &latest,
                Some(&item.queue_id),
                failure,
                "leased continuation child failed at the execution boundary",
                now_ms,
            )?;
            return Ok(GoalContinuationLeaseGate::Suppressed(fence));
        }
        if let crate::QueueTerminalControl::Terminal(control) =
            crate::runtime_worker::resolve_queue_terminal_control(
                harness_home,
                &item.queue_id,
                Some(item.session_key.as_str()),
            )?
        {
            let reason = format!(
                "leased continuation child has terminal control {} before execution: {}",
                control.source.as_str(),
                control.reason
            );
            let fence = settle_revalidation_failure_and_fence(
                harness_home,
                &latest,
                Some(&item.queue_id),
                IntentRevalidationFailure::PolicyBlocked(reason),
                "leased continuation child already has terminal control",
                now_ms,
            )?;
            return Ok(GoalContinuationLeaseGate::Suppressed(fence));
        }
        if latest.status == GoalContinuationIntentStatus::Acknowledged {
            return Ok(GoalContinuationLeaseGate::Acknowledged(latest));
        }
        if latest.status == GoalContinuationIntentStatus::Committed {
            latest = append_intent_state(
                harness_home,
                latest,
                GoalContinuationIntentStatus::Enqueued,
                Some(item.queue_id.clone()),
                "leased child recovered the committed-to-enqueued crash window",
                now_ms,
            )?;
        }
        let acknowledged = append_intent_state(
            harness_home,
            latest,
            GoalContinuationIntentStatus::Acknowledged,
            Some(item.queue_id.clone()),
            "child passed lease-to-execution authority and terminal-control revalidation",
            now_ms,
        )?;
        Ok(GoalContinuationLeaseGate::Acknowledged(acknowledged))
    })
}

pub fn latest_goal_continuation_intent(
    harness_home: impl AsRef<Path>,
    intent_key: &str,
) -> io::Result<Option<GoalContinuationIntentV1>> {
    latest_intent_for_key(harness_home.as_ref(), intent_key)
}

fn latest_intent_for_key(
    harness_home: &Path,
    intent_key: &str,
) -> io::Result<Option<GoalContinuationIntentV1>> {
    let recovered = latest_recoverable_intent_for_key(harness_home, intent_key)?;
    if let Some(reason) = recovered.corruption {
        return Err(io::Error::new(io::ErrorKind::InvalidData, reason));
    }
    Ok(recovered.latest)
}

#[derive(Debug)]
struct RecoverableIntentRead {
    latest: Option<GoalContinuationIntentV1>,
    corruption: Option<String>,
}

fn latest_recoverable_intent_for_key(
    harness_home: &Path,
    intent_key: &str,
) -> io::Result<RecoverableIntentRead> {
    let text = match fs::read_to_string(goal_continuation_intents_file(harness_home)) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(RecoverableIntentRead {
                latest: None,
                corruption: None,
            });
        }
        Err(error) => return Err(error),
    };
    let mut latest: Option<GoalContinuationIntentV1> = None;
    let mut corruption = None;
    for (index, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let value = match serde_json::from_str::<Value>(line) {
            Ok(value) => value,
            Err(error)
                if recover_intent_key_from_malformed_line(line).as_deref() == Some(intent_key) =>
            {
                corruption.get_or_insert_with(|| {
                    format!(
                        "target continuation intent has malformed JSON at line {}: {error}",
                        index + 1
                    )
                });
                continue;
            }
            Err(_) => continue,
        };
        if value.get("intentKey").and_then(Value::as_str) != Some(intent_key) {
            continue;
        }
        if corruption.is_some() {
            continue;
        }
        let receipt: GoalContinuationIntentV1 = match serde_json::from_value(value) {
            Ok(receipt) => receipt,
            Err(error) => {
                corruption = Some(format!(
                    "invalid target continuation intent at line {}: {error}",
                    index + 1
                ));
                continue;
            }
        };
        if !matches!(
            receipt.schema.as_str(),
            GOAL_CONTINUATION_INTENT_SCHEMA | TASK_CONTINUATION_INTENT_SCHEMA
        ) {
            corruption = Some(format!(
                "target continuation intent has unsupported schema at line {}",
                index + 1
            ));
            continue;
        }
        if let Err(reason) = validate_intent_ledger_receipt(&receipt) {
            corruption = Some(format!(
                "invalid target continuation intent at line {}: {reason}",
                index + 1
            ));
            continue;
        }
        if let Some(existing) = latest.as_ref() {
            if let Err(error) =
                ensure_authorized_intent_ledger_transition(harness_home, existing, &receipt)
            {
                corruption = Some(format!(
                    "invalid target continuation identity transition at line {}: {error}",
                    index + 1
                ));
                continue;
            }
            if !valid_intent_status_transition(existing.status, receipt.status) {
                corruption = Some(format!(
                    "invalid target continuation status transition at line {}",
                    index + 1
                ));
                continue;
            }
        }
        latest = Some(receipt);
    }
    Ok(RecoverableIntentRead { latest, corruption })
}

fn recover_intent_key_from_malformed_line(line: &str) -> Option<String> {
    let marker = "\"intentKey\"";
    let tail = line.get(line.find(marker)? + marker.len()..)?;
    let tail = tail.trim_start();
    let tail = tail.strip_prefix(':')?.trim_start();
    if !tail.starts_with('"') {
        return None;
    }
    let mut escaped = false;
    for (offset, ch) in tail[1..].char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match ch {
            '\\' => escaped = true,
            '"' => {
                let encoded = &tail[..offset + 2];
                return serde_json::from_str::<String>(encoded)
                    .ok()
                    .filter(|value| !value.trim().is_empty());
            }
            _ => {}
        }
    }
    None
}

fn goal_continuation_transaction_file(harness_home: &Path, intent_key: &str) -> PathBuf {
    harness_home
        .join("state")
        .join("goal-lineage")
        .join("continuation-transactions")
        .join(format!("{}.transaction", sha256_hex(intent_key.as_bytes())))
}

fn quarantine_fenced_child(
    harness_home: &Path,
    child_queue_id: &str,
    reason: &str,
    now_ms: i64,
) -> io::Result<()> {
    crate::write_runtime_queue_quarantine_marker(
        harness_home,
        child_queue_id,
        &format!("continuation child suppressed by durable intent fence: {reason}"),
        now_ms,
    )
    .map(|_| ())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ContinuationSettlementKind {
    SupersededByHuman,
    PolicyBlocked,
    RecoveryFailure,
}

impl ContinuationSettlementKind {
    fn from_revalidation_failure(failure: &IntentRevalidationFailure) -> Option<Self> {
        match failure {
            IntentRevalidationFailure::SupersededByHuman(_) => Some(Self::SupersededByHuman),
            IntentRevalidationFailure::PolicyBlocked(_) => Some(Self::PolicyBlocked),
            IntentRevalidationFailure::FailVisible(_) => Some(Self::RecoveryFailure),
            IntentRevalidationFailure::Retry(_) => None,
        }
    }

    fn status(self) -> &'static str {
        match self {
            Self::SupersededByHuman | Self::PolicyBlocked => "suppressed",
            Self::RecoveryFailure => "failed-terminal",
        }
    }

    fn terminal_disposition(self) -> &'static str {
        match self {
            Self::SupersededByHuman | Self::PolicyBlocked => "terminal-suppression",
            Self::RecoveryFailure => "logical-failure",
        }
    }

    fn source_closure_kind(self) -> &'static str {
        match self {
            Self::PolicyBlocked => "parked-policy-denied",
            Self::SupersededByHuman | Self::RecoveryFailure => "suppressed-exact-owner-mismatch",
        }
    }

    fn source_closure_reason(self) -> &'static str {
        match self {
            Self::SupersededByHuman => "continuation-superseded-by-newer-human",
            Self::PolicyBlocked => "continuation-policy-or-authority-revoked",
            Self::RecoveryFailure => "continuation-recovery-proof-failed",
        }
    }

    fn provider_notice(self) -> Option<&'static str> {
        match self {
            Self::SupersededByHuman => None,
            Self::PolicyBlocked => Some(
                "Automatic continuation is parked because its exact Goal or task authority is no longer active. No autonomous child will run. Review the current policy or authority, then send a new instruction or explicitly re-authorize continuation.",
            ),
            Self::RecoveryFailure => Some(
                "Automatic continuation stopped before execution because its durable recovery proof or intent ledger is inconsistent. No autonomous child will run. The channel remains available for a new instruction after operator review.",
            ),
        }
    }
}

#[derive(Debug)]
struct SettlementDelivery {
    disposition: &'static str,
    canonical_source_queue_id: Option<String>,
    delivery_id: Option<String>,
}

fn record_continuation_settlement_once(
    harness_home: &Path,
    intent: &GoalContinuationIntentV1,
    kind: ContinuationSettlementKind,
    reason: &str,
    now_ms: i64,
) -> io::Result<bool> {
    crate::write_runtime_queue_quarantine_marker(
        harness_home,
        &intent.parent_queue_id,
        &format!("continuation parent settled before unsafe autonomous replay: {reason}"),
        now_ms,
    )?;
    let queue_dir = harness_home.join("state").join("runtime-queue");
    fs::create_dir_all(&queue_dir)?;
    let receipts_file = queue_dir.join("run-once-receipts.jsonl");
    let settlement_key = sha256_hex(&canonical_identity_bytes(&[
        "continuation-non-runnable-settlement-v1",
        &intent.intent_key,
    ]));
    let (runtime_class, origin) =
        parent_runtime_attribution(harness_home, &intent.parent_queue_id)?;
    crate::logging::with_jsonl_append_lock(&receipts_file, || {
        let existing = match fs::read_to_string(&receipts_file) {
            Ok(text) => text,
            Err(error) if error.kind() == io::ErrorKind::NotFound => String::new(),
            Err(error) => return Err(error),
        };
        if existing.lines().any(|line| {
            serde_json::from_str::<Value>(line)
                .ok()
                .and_then(|value| {
                    value
                        .get("continuationSettlementKey")
                        .and_then(Value::as_str)
                        .map(|key| key == settlement_key)
                })
                .unwrap_or(false)
        }) {
            return Ok(false);
        }
        let delivery = append_continuation_settlement_notice_if_current(
            harness_home,
            intent,
            kind,
            &settlement_key,
        )?;
        let source_final_expectation = if delivery.delivery_id.is_some() {
            "required"
        } else {
            "explicit-non-delivery"
        };
        append_jsonl_value_while_locked(
            &receipts_file,
            &serde_json::json!({
                "schema": "agent-harness.runtime-run-once.v1",
                "queueId": intent.parent_queue_id,
                "status": kind.status(),
                "runtimeClass": runtime_class,
                "origin": origin,
                "continuationIntentKey": intent.intent_key,
                "terminalDisposition": kind.terminal_disposition(),
                "preparedExecutionTerminalizationReason":
                    kind.source_closure_reason(),
                "sourceFinalExpectation": source_final_expectation,
                "sourceClosureKind": kind.source_closure_kind(),
                "sourceClosureReason": kind.source_closure_reason(),
                "finalOutboxDisposition": delivery.disposition,
                "canonicalSourceQueueId": delivery.canonical_source_queue_id,
                "finalDeliveryId": delivery.delivery_id,
                "sourceFinalLaneDigest": intent.lane_digest,
                "operatorActionRequired": kind != ContinuationSettlementKind::SupersededByHuman,
                "continuationSettlementKey": settlement_key,
                "observedAtMs": now_ms,
                "reason": format!(
                    "continuation intent was settled before unsafe autonomous replay: {reason}"
                )
            }),
            &existing,
        )?;
        Ok(true)
    })
}

fn append_continuation_settlement_notice_if_current(
    harness_home: &Path,
    intent: &GoalContinuationIntentV1,
    kind: ContinuationSettlementKind,
    settlement_key: &str,
) -> io::Result<SettlementDelivery> {
    let Some(text) = kind.provider_notice() else {
        return Ok(SettlementDelivery {
            disposition: "explicit-non-delivery",
            canonical_source_queue_id: None,
            delivery_id: None,
        });
    };
    let Some(context) = exact_current_parent_channel_context(harness_home, intent)? else {
        return Ok(SettlementDelivery {
            disposition: "explicit-non-delivery",
            canonical_source_queue_id: None,
            delivery_id: None,
        });
    };
    let delivery_id = format!(
        "delivery:v2:{}",
        &sha256_hex(settlement_key.as_bytes())[..32]
    );
    let mut message = ChannelOutboundMessage {
        platform: context.platform,
        account_id: context.account_id,
        channel_id: context.channel_id,
        user_id: context.user_id,
        session_key: context.session_key,
        delivery_id: Some(delivery_id.clone()),
        kind: ChannelOutboundMessageKind::ErrorReply,
        source_queue_id: Some(intent.parent_queue_id.clone()),
        source_completion_file: None,
        text: text.to_string(),
        presentation: None,
        delivery_intent: None,
        attachments: Vec::new(),
    };
    let report = append_channel_outbox_message(harness_home, &mut message)?;
    let wake_file = harness_home
        .join("state")
        .join("wake")
        .join("final-outbox.json");
    let _ = crate::wake::signal_wake(
        harness_home,
        wake_file,
        "final-outbox",
        "continuation settlement notice appended",
    );
    Ok(SettlementDelivery {
        disposition: match report.outcome {
            ChannelOutboxAppendOutcome::AlreadyPresent => "already-present",
            ChannelOutboxAppendOutcome::Appended
            | ChannelOutboxAppendOutcome::AppendedIndexDeferred => "appended",
        },
        canonical_source_queue_id: Some(intent.parent_queue_id.clone()),
        delivery_id: Some(delivery_id),
    })
}

#[derive(Debug)]
struct ParentChannelContext {
    platform: String,
    account_id: Option<String>,
    channel_id: String,
    user_id: String,
    session_key: String,
}

fn settle_revalidation_failure_and_fence(
    harness_home: &Path,
    intent: &GoalContinuationIntentV1,
    child_queue_id: Option<&str>,
    failure: IntentRevalidationFailure,
    boundary: &str,
    now_ms: i64,
) -> io::Result<GoalContinuationFenceV1> {
    let kind = match ContinuationSettlementKind::from_revalidation_failure(&failure) {
        Some(kind) => kind,
        None => {
            return match failure {
                IntentRevalidationFailure::Retry(error) => Err(error),
                _ => Err(io::Error::other(
                    "non-retry continuation settlement kind is missing",
                )),
            };
        }
    };
    let reason = match failure {
        IntentRevalidationFailure::SupersededByHuman(reason)
        | IntentRevalidationFailure::PolicyBlocked(reason)
        | IntentRevalidationFailure::FailVisible(reason) => reason,
        IntentRevalidationFailure::Retry(error) => return Err(error),
    };
    if let Some(child_queue_id) = child_queue_id {
        quarantine_fenced_child(harness_home, child_queue_id, &reason, now_ms)?;
    }
    record_continuation_settlement_once(harness_home, intent, kind, &reason, now_ms)?;
    append_goal_continuation_fence_once(
        harness_home,
        intent,
        child_queue_id,
        &format!("{boundary}: {reason}"),
        now_ms,
    )
}

fn exact_current_parent_channel_context(
    harness_home: &Path,
    intent: &GoalContinuationIntentV1,
) -> io::Result<Option<ParentChannelContext>> {
    let file = harness_home
        .join("state")
        .join("runtime-queue")
        .join("pending.jsonl");
    let text = match fs::read_to_string(file) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let matches = text
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|value| {
            value.get("queueId").and_then(Value::as_str) == Some(intent.parent_queue_id.as_str())
        })
        .collect::<Vec<_>>();
    if matches.len() != 1 {
        return Ok(None);
    }
    let value = &matches[0];
    let field = |name: &str| {
        value
            .get(name)
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
    };
    if field("runtimeClass") != Some("interactive")
        || field("origin") != Some("channel")
        || field("sessionKey") != Some(intent.working_session_key.as_str())
    {
        return Ok(None);
    }
    let Some(
        [
            platform,
            account_id,
            channel_id,
            user_id,
            agent_id,
            _,
            session_key,
        ],
    ) = pending_lane_axes(value)
    else {
        return Ok(None);
    };
    if pending_exact_lane_digest(value)?.as_deref() != Some(intent.lane_digest.as_str()) {
        return Ok(None);
    }
    let lane = ChannelStateLane::new(platform, Some(account_id), channel_id, user_id, agent_id)?;
    if read_channel_session_state_v2(harness_home, &lane)?
        .is_some_and(|state| state.active_session_key != session_key)
    {
        return Ok(None);
    }
    Ok(Some(ParentChannelContext {
        platform: platform.to_string(),
        account_id: Some(account_id.to_string()),
        channel_id: channel_id.to_string(),
        user_id: user_id.to_string(),
        session_key: session_key.to_string(),
    }))
}

fn parent_runtime_attribution(
    harness_home: &Path,
    parent_queue_id: &str,
) -> io::Result<(Option<String>, Option<String>)> {
    let file = harness_home
        .join("state")
        .join("runtime-queue")
        .join("pending.jsonl");
    let text = match fs::read_to_string(file) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok((None, None));
        }
        Err(error) => return Err(error),
    };
    let value = text.lines().find_map(|line| {
        serde_json::from_str::<Value>(line)
            .ok()
            .filter(|value| value.get("queueId").and_then(Value::as_str) == Some(parent_queue_id))
    });
    Ok(match value {
        Some(value) => (
            value
                .get("runtimeClass")
                .and_then(Value::as_str)
                .map(ToString::to_string),
            value
                .get("origin")
                .and_then(Value::as_str)
                .map(ToString::to_string),
        ),
        None => (None, None),
    })
}

fn ensure_preexisting_fence_is_settled(
    harness_home: &Path,
    intent: &GoalContinuationIntentV1,
    fence: &GoalContinuationFenceV1,
    now_ms: i64,
) -> io::Result<()> {
    if !matches!(
        intent.status,
        GoalContinuationIntentStatus::Committed | GoalContinuationIntentStatus::Enqueued
    ) {
        return Ok(());
    }
    let kind = settlement_kind_for_existing_fence(&fence.reason);
    record_continuation_settlement_once(
        harness_home,
        intent,
        kind,
        &format!(
            "pre-existing durable fence {} prevents automatic recovery: {}",
            fence.fence_key, fence.reason
        ),
        now_ms,
    )
    .map(|_| ())
}

fn settlement_kind_for_existing_fence(reason: &str) -> ContinuationSettlementKind {
    let normalized = reason.to_ascii_lowercase();
    if normalized.contains("newer same-exact-lane human")
        || normalized.contains("superseded-by-newer-human")
    {
        ContinuationSettlementKind::SupersededByHuman
    } else if normalized.contains("policy")
        || normalized.contains("autonomy")
        || normalized.contains("authority changed")
        || normalized.contains("terminal control")
        || normalized.contains("operator stop")
    {
        ContinuationSettlementKind::PolicyBlocked
    } else {
        ContinuationSettlementKind::RecoveryFailure
    }
}

pub fn is_goal_continuation_intent_fenced(
    harness_home: impl AsRef<Path>,
    intent_key: &str,
) -> io::Result<bool> {
    Ok(latest_goal_continuation_fence(harness_home.as_ref(), intent_key)?.is_some())
}

pub fn latest_goal_continuation_fence(
    harness_home: impl AsRef<Path>,
    intent_key: &str,
) -> io::Result<Option<GoalContinuationFenceV1>> {
    let text = match fs::read_to_string(goal_continuation_fences_file(harness_home)) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let mut latest = None;
    for (index, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let value = match serde_json::from_str::<Value>(line) {
            Ok(value) => value,
            Err(error) if line.contains(intent_key) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "target continuation fence has malformed JSON at line {}: {error}",
                        index + 1
                    ),
                ));
            }
            Err(_) => continue,
        };
        if value.get("intentKey").and_then(Value::as_str) != Some(intent_key) {
            continue;
        }
        let fence: GoalContinuationFenceV1 = serde_json::from_value(value).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "invalid target continuation fence at line {}: {error}",
                    index + 1
                ),
            )
        })?;
        if fence.schema != GOAL_CONTINUATION_FENCE_SCHEMA
            || fence.disposition != "fenced"
            || fence.intent_key.trim().is_empty()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid target continuation fence at line {}", index + 1),
            ));
        }
        latest = Some(fence);
    }
    Ok(latest)
}

fn append_goal_continuation_fence_once(
    harness_home: &Path,
    intent: &GoalContinuationIntentV1,
    child_queue_id: Option<&str>,
    reason: &str,
    now_ms: i64,
) -> io::Result<GoalContinuationFenceV1> {
    let file = goal_continuation_fences_file(harness_home);
    crate::logging::with_jsonl_append_lock(&file, || {
        if let Some(existing) = latest_goal_continuation_fence(harness_home, &intent.intent_key)? {
            return Ok(existing);
        }
        let fence_key = sha256_hex(&canonical_identity_bytes(&[
            "goal-continuation-fence-v1",
            &intent.intent_key,
            &intent.parent_queue_id,
            &intent.lane_digest,
            &intent.virtual_session_id,
        ]));
        let receipt = GoalContinuationFenceV1 {
            schema: GOAL_CONTINUATION_FENCE_SCHEMA.to_string(),
            fence_key,
            intent_key: intent.intent_key.clone(),
            parent_queue_id: intent.parent_queue_id.clone(),
            child_queue_id: child_queue_id.map(ToString::to_string),
            lane_digest: intent.lane_digest.clone(),
            virtual_session_id: intent.virtual_session_id.clone(),
            disposition: "fenced".to_string(),
            reason: reason.to_string(),
            observed_at_ms: now_ms,
        };
        let existing = match fs::read_to_string(&file) {
            Ok(text) => text,
            Err(error) if error.kind() == io::ErrorKind::NotFound => String::new(),
            Err(error) => return Err(error),
        };
        append_jsonl_value_while_locked(&file, &receipt, &existing)?;
        Ok(receipt)
    })
}

fn append_jsonl_value_while_locked(
    path: &Path,
    value: &impl Serialize,
    existing: &str,
) -> io::Result<()> {
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    if !existing.is_empty() && !existing.ends_with('\n') {
        file.write_all(b"\n")?;
    }
    serde_json::to_writer(&mut file, value).map_err(io::Error::other)?;
    file.write_all(b"\n")?;
    file.flush()
}

#[derive(Debug, Clone)]
struct ReconciliationQuarantine {
    disposition: &'static str,
    error_kind: &'static str,
    record_digest: String,
    record_line: Option<u64>,
    intent_key: Option<String>,
    reason: String,
}

impl ReconciliationQuarantine {
    fn for_record(
        line_number: usize,
        line: &str,
        intent_key: Option<String>,
        error_kind: &'static str,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            disposition: "quarantined",
            error_kind,
            record_digest: sha256_hex(line.as_bytes()),
            record_line: Some(line_number as u64),
            intent_key,
            reason: reason.into(),
        }
    }

    fn for_intent_error(intent: &GoalContinuationIntentV1, error: &io::Error) -> Self {
        let bytes =
            serde_json::to_vec(intent).unwrap_or_else(|_| intent.intent_key.as_bytes().to_vec());
        Self {
            disposition: "retry-deferred",
            error_kind: "intent-reconciliation-failed",
            record_digest: sha256_hex(&bytes),
            record_line: None,
            intent_key: Some(intent.intent_key.clone()),
            reason: error.to_string(),
        }
    }

    fn receipt(self, now_ms: i64) -> GoalContinuationReconciliationErrorV1 {
        let line = self.record_line.map(|value| value.to_string());
        let error_key = sha256_hex(&canonical_identity_bytes(&[
            "goal-continuation-reconciliation-error-v1",
            self.disposition,
            self.error_kind,
            &self.record_digest,
            line.as_deref().unwrap_or("none"),
            self.intent_key.as_deref().unwrap_or("none"),
        ]));
        GoalContinuationReconciliationErrorV1 {
            schema: GOAL_CONTINUATION_RECONCILIATION_ERROR_SCHEMA.to_string(),
            error_key,
            disposition: self.disposition.to_string(),
            error_kind: self.error_kind.to_string(),
            record_digest: self.record_digest,
            record_line: self.record_line,
            intent_key: self.intent_key,
            reason: self.reason,
            observed_at_ms: now_ms,
        }
    }
}

fn latest_intents_for_reconciliation(
    harness_home: &Path,
) -> io::Result<(
    BTreeMap<String, GoalContinuationIntentV1>,
    Vec<ReconciliationQuarantine>,
)> {
    let text = match fs::read_to_string(goal_continuation_intents_file(harness_home)) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok((BTreeMap::new(), Vec::new()));
        }
        Err(error) => return Err(error),
    };
    let mut latest: BTreeMap<String, GoalContinuationIntentV1> = BTreeMap::new();
    let mut blocked_keys = BTreeSet::new();
    let mut quarantines = Vec::new();
    for (index, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let line_number = index + 1;
        let value = match serde_json::from_str::<Value>(line) {
            Ok(value) => value,
            Err(error) => {
                let intent_key = recover_intent_key_from_malformed_line(line);
                if let Some(key) = intent_key.as_ref() {
                    blocked_keys.insert(key.clone());
                }
                quarantines.push(ReconciliationQuarantine::for_record(
                    line_number,
                    line,
                    intent_key,
                    "malformed-json",
                    format!("continuation intent ledger record is not valid JSON: {error}"),
                ));
                continue;
            }
        };
        let intent_key = value
            .get("intentKey")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .map(ToString::to_string);
        let schema = value.get("schema").and_then(Value::as_str);
        if !matches!(
            schema,
            Some(GOAL_CONTINUATION_INTENT_SCHEMA | TASK_CONTINUATION_INTENT_SCHEMA)
        ) {
            if let Some(key) = intent_key.as_ref() {
                blocked_keys.insert(key.clone());
            }
            quarantines.push(ReconciliationQuarantine::for_record(
                line_number,
                line,
                intent_key,
                "unsupported-schema",
                "continuation intent ledger record has no supported schema",
            ));
            continue;
        }
        let receipt = match serde_json::from_value::<GoalContinuationIntentV1>(value) {
            Ok(receipt) => receipt,
            Err(error) => {
                if let Some(key) = intent_key.as_ref() {
                    blocked_keys.insert(key.clone());
                }
                quarantines.push(ReconciliationQuarantine::for_record(
                    line_number,
                    line,
                    intent_key,
                    "malformed-intent",
                    format!("continuation intent ledger record is invalid: {error}"),
                ));
                continue;
            }
        };
        if let Err(reason) = validate_intent_ledger_receipt(&receipt) {
            blocked_keys.insert(receipt.intent_key.clone());
            quarantines.push(ReconciliationQuarantine::for_record(
                line_number,
                line,
                Some(receipt.intent_key),
                "invalid-intent",
                reason,
            ));
            continue;
        }
        if blocked_keys.contains(&receipt.intent_key) {
            continue;
        }
        if let Some(existing) = latest.get(&receipt.intent_key) {
            let identity_error =
                ensure_authorized_intent_ledger_transition(harness_home, existing, &receipt).err();
            if identity_error.is_some()
                || !valid_intent_status_transition(existing.status, receipt.status)
            {
                let reason = identity_error.map_or_else(
                    || {
                        "continuation intent ledger contains an invalid status transition"
                            .to_string()
                    },
                    |error| error.to_string(),
                );
                blocked_keys.insert(receipt.intent_key.clone());
                quarantines.push(ReconciliationQuarantine::for_record(
                    line_number,
                    line,
                    Some(receipt.intent_key),
                    "intent-collision",
                    reason,
                ));
                continue;
            }
        }
        latest.insert(receipt.intent_key.clone(), receipt);
    }
    Ok((latest, quarantines))
}

fn append_reconciliation_error_once(
    harness_home: &Path,
    receipt: GoalContinuationReconciliationErrorV1,
) -> io::Result<bool> {
    let file = goal_continuation_reconciliation_errors_file(harness_home);
    crate::logging::with_jsonl_append_lock(&file, || {
        let existing = match fs::read_to_string(&file) {
            Ok(text) => text,
            Err(error) if error.kind() == io::ErrorKind::NotFound => String::new(),
            Err(error) => return Err(error),
        };
        if existing.lines().any(|line| {
            serde_json::from_str::<Value>(line)
                .ok()
                .and_then(|value| {
                    value
                        .get("errorKey")
                        .and_then(Value::as_str)
                        .map(ToString::to_string)
                })
                .as_deref()
                == Some(receipt.error_key.as_str())
        }) {
            return Ok(false);
        }
        append_jsonl_value_while_locked(&file, &receipt, &existing)?;
        Ok(true)
    })
}

fn validate_intent_ledger_receipt(receipt: &GoalContinuationIntentV1) -> Result<(), String> {
    for (name, value) in [
        ("intentKey", receipt.intent_key.as_str()),
        ("parentQueueId", receipt.parent_queue_id.as_str()),
        ("workingSessionKey", receipt.working_session_key.as_str()),
        ("laneDigest", receipt.lane_digest.as_str()),
        ("virtualSessionId", receipt.virtual_session_id.as_str()),
        ("lineageId", receipt.lineage_id.as_str()),
        ("goalChecksum", receipt.goal_checksum.as_str()),
        ("campaignFamilyId", receipt.campaign_family_id.as_str()),
        (
            "backendContextGeneration",
            receipt.backend_context_generation.as_str(),
        ),
    ] {
        if value.trim().is_empty() {
            return Err(format!("continuation intent has no {name}"));
        }
    }
    match receipt.status {
        GoalContinuationIntentStatus::Committed if receipt.child_queue_id.is_some() => {
            return Err("committed continuation intent already names a child queue".to_string());
        }
        GoalContinuationIntentStatus::Enqueued | GoalContinuationIntentStatus::Acknowledged
            if receipt.child_queue_id.as_deref().is_none_or(str::is_empty) =>
        {
            return Err(
                "enqueued or acknowledged continuation intent has no child queue".to_string(),
            );
        }
        _ => {}
    }
    Ok(())
}

fn valid_intent_status_transition(
    previous: GoalContinuationIntentStatus,
    next: GoalContinuationIntentStatus,
) -> bool {
    previous == next
        || matches!(
            (previous, next),
            (
                GoalContinuationIntentStatus::Committed,
                GoalContinuationIntentStatus::Enqueued
            ) | (
                GoalContinuationIntentStatus::Enqueued,
                GoalContinuationIntentStatus::Acknowledged
            )
        )
}

fn find_child_queue_for_intent(
    harness_home: &Path,
    intent_key: &str,
) -> io::Result<Option<String>> {
    let file = harness_home
        .join("state")
        .join("runtime-queue")
        .join("pending.jsonl");
    let text = match fs::read_to_string(file) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let mut found = None;
    for line in text.lines() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if value.get("continuationIntentKey").and_then(Value::as_str) != Some(intent_key) {
            continue;
        }
        let queue_id = value
            .get("queueId")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "goal child has no queueId")
            })?;
        if found
            .as_deref()
            .is_some_and(|existing| existing != queue_id)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "one continuation intent owns more than one logical child",
            ));
        }
        found = Some(queue_id.to_string());
    }
    Ok(found)
}

fn human_input_generation_at_parent(
    harness_home: &Path,
    parent_queue_id: &str,
    exact_lane_digest: &str,
) -> io::Result<(u64, u64)> {
    let file = harness_home
        .join("state")
        .join("runtime-queue")
        .join("pending.jsonl");
    let text = fs::read_to_string(file)?;
    let values = text
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .collect::<Vec<_>>();
    let parent = values
        .iter()
        .find(|value| value.get("queueId").and_then(Value::as_str) == Some(parent_queue_id))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "continuation parent has no exact-lane human-input generation",
            )
        })?;
    let parent_axes = pending_lane_axes(parent).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "continuation parent has incomplete exact-lane axes",
        )
    })?;
    let parent_lane_digest = pending_exact_lane_digest(parent)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "continuation parent has no valid exact-lane digest",
        )
    })?;
    if parent_lane_digest != exact_lane_digest {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "continuation parent exact-lane digest does not match its committed authority",
        ));
    }
    let mut current_generation = 0_u64;
    let mut parent_generation = None;
    for value in &values {
        let Some(axes) = pending_lane_axes(value) else {
            continue;
        };
        if !pending_lane_axes_match(&axes, &parent_axes) {
            continue;
        }
        let Some(lane_digest) = pending_exact_lane_digest(&value)? else {
            continue;
        };
        if lane_digest != exact_lane_digest {
            continue;
        }
        let non_continuation_human = value.get("origin").and_then(Value::as_str) == Some("channel")
            && value
                .get("continuationIntentKey")
                .and_then(Value::as_str)
                .is_none_or(str::is_empty);
        if non_continuation_human {
            current_generation = current_generation.saturating_add(1);
        }
        if value.get("queueId").and_then(Value::as_str) == Some(parent_queue_id) {
            parent_generation = Some(current_generation);
        }
    }
    let parent_generation = parent_generation.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "continuation parent has no exact-lane human-input generation",
        )
    })?;
    Ok((parent_generation, current_generation))
}

fn pending_lane_axes(value: &Value) -> Option<[&str; 7]> {
    let field = |name: &str| {
        value
            .get(name)
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
    };
    Some([
        field("platform")?,
        field("accountId")?,
        field("channelId")?,
        field("userId")?,
        field("agentId")?,
        field("runtimeClass")?,
        field("sessionKey")?,
    ])
}

fn pending_lane_axes_match(left: &[&str; 7], right: &[&str; 7]) -> bool {
    left.iter()
        .zip(right.iter())
        .all(|(left, right)| left.trim() == right.trim())
}

fn pending_exact_lane_digest(value: &Value) -> io::Result<Option<String>> {
    let Some(
        [
            platform,
            account_id,
            channel_id,
            user_id,
            agent_id,
            runtime_class,
            session_key,
        ],
    ) = pending_lane_axes(value)
    else {
        return Ok(None);
    };
    let lane = FullLaneKeyV1::new(
        platform,
        account_id,
        channel_id,
        user_id,
        agent_id,
        runtime_class,
        root_working_session_key(session_key),
        session_key,
    )
    .map_err(io::Error::other)?;
    lane.identity_hash().map(Some).map_err(io::Error::other)
}

#[derive(Debug)]
enum IntentRevalidationFailure {
    SupersededByHuman(String),
    PolicyBlocked(String),
    FailVisible(String),
    Retry(io::Error),
}

impl IntentRevalidationFailure {
    fn from_state_error(error: io::Error) -> Self {
        match error.kind() {
            io::ErrorKind::NotFound | io::ErrorKind::InvalidData | io::ErrorKind::InvalidInput => {
                Self::FailVisible(error.to_string())
            }
            _ => Self::Retry(error),
        }
    }

    fn from_policy_state_error(error: io::Error) -> Self {
        match error.kind() {
            io::ErrorKind::NotFound | io::ErrorKind::InvalidData | io::ErrorKind::InvalidInput => {
                Self::PolicyBlocked(error.to_string())
            }
            _ => Self::Retry(error),
        }
    }
}

fn legacy_intent_missing_additive_fields(intent: &GoalContinuationIntentV1) -> bool {
    intent.human_input_generation.is_none()
        || (intent.schema == GOAL_CONTINUATION_INTENT_SCHEMA
            && (intent.goal_relation_at_commit.is_none() || intent.goal_status_at_commit.is_none()))
}

fn upgrade_open_legacy_intent(
    harness_home: &Path,
    intent: &GoalContinuationIntentV1,
) -> Result<(GoalContinuationIntentV1, bool), IntentRevalidationFailure> {
    if !matches!(
        intent.status,
        GoalContinuationIntentStatus::Committed | GoalContinuationIntentStatus::Enqueued
    ) || !legacy_intent_missing_additive_fields(intent)
    {
        return Ok((intent.clone(), false));
    }

    let mut upgraded = intent.clone();
    if upgraded.human_input_generation.is_none() {
        let (parent_generation, _) = human_input_generation_at_parent(
            harness_home,
            &upgraded.parent_queue_id,
            &upgraded.lane_digest,
        )
        .map_err(IntentRevalidationFailure::from_state_error)?;
        upgraded.human_input_generation = Some(parent_generation);
    }

    if upgraded.schema == GOAL_CONTINUATION_INTENT_SCHEMA
        && (upgraded.goal_relation_at_commit.is_none() || upgraded.goal_status_at_commit.is_none())
    {
        let proof = exact_legacy_goal_transition_proof(harness_home, &upgraded)?;
        if upgraded
            .goal_relation_at_commit
            .is_some_and(|relation| relation != proof.relation)
        {
            return Err(IntentRevalidationFailure::FailVisible(
                "legacy goal intent relation conflicts with its exact transition proof".to_string(),
            ));
        }
        if upgraded
            .goal_status_at_commit
            .as_deref()
            .is_some_and(|status| proof.goal_status.as_deref() != Some(status))
        {
            return Err(IntentRevalidationFailure::FailVisible(
                "legacy goal intent status conflicts with its exact transition proof".to_string(),
            ));
        }
        upgraded.goal_relation_at_commit = Some(proof.relation);
        upgraded.goal_status_at_commit = proof.goal_status;
    }

    Ok((upgraded, true))
}

fn exact_legacy_goal_transition_proof(
    harness_home: &Path,
    intent: &GoalContinuationIntentV1,
) -> Result<GoalTransitionReceiptV1, IntentRevalidationFailure> {
    let file = goal_transition_receipts_file(harness_home);
    let text = match fs::read_to_string(&file) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Err(IntentRevalidationFailure::FailVisible(format!(
                "legacy goal intent has no pre-existing transition receipt at {}",
                file.display()
            )));
        }
        Err(error) => return Err(IntentRevalidationFailure::Retry(error)),
    };
    let mut exact = Vec::new();
    let mut corrupt_parent_records = 0_u64;
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if value.get("queueId").and_then(Value::as_str) != Some(intent.parent_queue_id.as_str()) {
            continue;
        }
        let receipt = match serde_json::from_value::<GoalTransitionReceiptV1>(value) {
            Ok(receipt) if receipt.schema == crate::goal_transition::GOAL_TRANSITION_SCHEMA => {
                receipt
            }
            Ok(_) | Err(_) => {
                corrupt_parent_records = corrupt_parent_records.saturating_add(1);
                continue;
            }
        };
        let exact_identity = receipt.lineage_id.as_deref() == Some(intent.lineage_id.as_str())
            && receipt.campaign_family_id.as_deref() == Some(intent.campaign_family_id.as_str())
            && receipt.lane_digest.as_deref() == Some(intent.lane_digest.as_str())
            && receipt.virtual_session_id.as_deref() == Some(intent.virtual_session_id.as_str())
            && receipt.backend_context_generation.as_deref()
                == Some(intent.backend_context_generation.as_str())
            && receipt.goal_checksum.as_deref() == Some(intent.goal_checksum.as_str())
            && receipt.source_slice_generation == intent.source_slice_generation
            && receipt.decision_generation == intent.decision_generation
            && intent.campaign_slice_generation
                == receipt.source_slice_generation.saturating_add(1)
            && receipt.decision == intent.decision
            && receipt.schedule_continuation
            && receipt.authority == GoalTransitionAuthority::Ready
            && matches!(
                receipt.relation,
                GoalTransitionRelation::CurrentGoalSlice
                    | GoalTransitionRelation::AuthorizedCampaignContinuation
            )
            && receipt
                .goal_status
                .as_deref()
                .is_some_and(goal_status_is_active)
            && receipt.observed_at_ms <= intent.observed_at_ms;
        if exact_identity {
            exact.push(receipt);
        }
    }
    if corrupt_parent_records > 0 {
        return Err(IntentRevalidationFailure::FailVisible(format!(
            "legacy goal intent transition proof is ambiguous because {corrupt_parent_records} parent-bound record(s) are corrupt"
        )));
    }
    match exact.len() {
        1 => Ok(exact.pop().unwrap()),
        0 => Err(IntentRevalidationFailure::FailVisible(
            "legacy goal intent has no exact matching active scheduled transition proof"
                .to_string(),
        )),
        count => Err(IntentRevalidationFailure::FailVisible(format!(
            "legacy goal intent has {count} exact transition proofs; reconstruction is ambiguous"
        ))),
    }
}

fn revalidate_current_goal_authority(
    harness_home: &Path,
    intent: &GoalContinuationIntentV1,
) -> Result<(), IntentRevalidationFailure> {
    let report = run_goal_lineage_doctor(GoalLineageDoctorOptions {
        harness_home: harness_home.to_path_buf(),
        lane_digest: Some(intent.lane_digest.clone()),
        virtual_session_id: Some(intent.virtual_session_id.clone()),
    })
    .map_err(IntentRevalidationFailure::from_state_error)?;
    match report.status {
        GoalLineageDoctorStatus::Ready => {}
        GoalLineageDoctorStatus::Empty => {
            return Err(IntentRevalidationFailure::PolicyBlocked(
                "current exact-lane Goal lineage is empty".to_string(),
            ));
        }
        GoalLineageDoctorStatus::ReconciliationRequired => {
            return Err(IntentRevalidationFailure::PolicyBlocked(
                "current exact-lane Goal lineage requires reconciliation".to_string(),
            ));
        }
        GoalLineageDoctorStatus::Blocked => {
            return Err(IntentRevalidationFailure::FailVisible(format!(
                "current exact-lane Goal lineage is blocked: {}",
                report
                    .blockers
                    .iter()
                    .chain(report.warnings.iter())
                    .cloned()
                    .collect::<Vec<_>>()
                    .join("; ")
            )));
        }
    }
    let matches = report
        .lineages
        .iter()
        .filter(|lineage| lineage.lineage_id == intent.lineage_id)
        .collect::<Vec<_>>();
    if matches.len() != 1 {
        return Err(IntentRevalidationFailure::PolicyBlocked(format!(
            "current exact-lane Goal lineage contains {} rows for committed lineage {}",
            matches.len(),
            intent.lineage_id
        )));
    }
    let current = matches[0];
    let exact_identity = current.campaign_family_id == intent.campaign_family_id
        && current.goal_checksum == intent.goal_checksum
        && current.source_session_key == intent.working_session_key
        && current.lane_digest.as_deref() == Some(intent.lane_digest.as_str())
        && current.virtual_session_id.as_deref() == Some(intent.virtual_session_id.as_str())
        && current.backend_context_generation.as_deref()
            == Some(intent.backend_context_generation.as_str());
    if !exact_identity {
        return Err(IntentRevalidationFailure::PolicyBlocked(
            "current durable Goal lineage no longer matches the committed lineage/campaign/checksum/lane/session/backend authority"
                .to_string(),
        ));
    }
    if !goal_parent_descends_from_current_lineage(
        harness_home,
        intent,
        current.queue_id.as_deref(),
    )? {
        return Err(IntentRevalidationFailure::PolicyBlocked(
            "continuation parent is not an acknowledged exact-authority descendant of the current durable Goal lineage"
                .to_string(),
        ));
    }
    if !current.runnable
        || current.disposition != GoalLineageDisposition::Runnable
        || !goal_status_is_active(&current.goal_status)
        || !matches!(
            current.turn_relation,
            GoalProjectionTurnRelationV1::CurrentOwnedTurn
                | GoalProjectionTurnRelationV1::ExplicitCampaignContinuation
        )
    {
        return Err(IntentRevalidationFailure::PolicyBlocked(format!(
            "committed Goal lineage is no longer active and runnable: disposition={:?} status={} relation={:?}",
            current.disposition, current.goal_status, current.turn_relation
        )));
    }
    let campaign = report
        .campaigns
        .iter()
        .find(|campaign| campaign.campaign_family_id == intent.campaign_family_id)
        .ok_or_else(|| {
            IntentRevalidationFailure::PolicyBlocked(
                "committed Goal campaign is no longer present in current durable lineage"
                    .to_string(),
            )
        })?;
    if campaign.reconciliation_required
        || campaign.runnable_lineages != 1
        || campaign.canonical_lineage_id.as_deref() != Some(intent.lineage_id.as_str())
    {
        return Err(IntentRevalidationFailure::PolicyBlocked(
            "committed Goal lineage is no longer the unique canonical runnable lineage in its campaign"
                .to_string(),
        ));
    }
    if report
        .lineages
        .iter()
        .any(|lineage| lineage.runnable && lineage.lineage_id != intent.lineage_id)
    {
        return Err(IntentRevalidationFailure::PolicyBlocked(
            "a different canonical runnable Goal lineage now owns the exact lane/virtual session"
                .to_string(),
        ));
    }
    if exact_goal_closure_breaker(harness_home, intent, current)? {
        return Err(IntentRevalidationFailure::PolicyBlocked(
            "an exact durable Goal closure intent or receipt supersedes continuation".to_string(),
        ));
    }
    Ok(())
}

fn goal_parent_descends_from_current_lineage(
    harness_home: &Path,
    intent: &GoalContinuationIntentV1,
    current_lineage_queue_id: Option<&str>,
) -> Result<bool, IntentRevalidationFailure> {
    let Some(current_lineage_queue_id) =
        current_lineage_queue_id.filter(|queue_id| !queue_id.is_empty())
    else {
        return Ok(false);
    };
    if intent.parent_queue_id == current_lineage_queue_id {
        return Ok(true);
    }

    let pending_file = harness_home
        .join("state")
        .join("runtime-queue")
        .join("pending.jsonl");
    let pending_text =
        fs::read_to_string(&pending_file).map_err(IntentRevalidationFailure::from_state_error)?;
    let mut pending_by_queue: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    for line in pending_text.lines().filter(|line| !line.trim().is_empty()) {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let Some(queue_id) = value
            .get("queueId")
            .and_then(Value::as_str)
            .filter(|queue_id| !queue_id.is_empty())
        else {
            continue;
        };
        pending_by_queue
            .entry(queue_id.to_string())
            .or_default()
            .push(value);
    }

    let mut descendant_queue_id = intent.parent_queue_id.clone();
    let mut expected_slice_generation = intent.source_slice_generation;
    let mut visited = BTreeSet::new();
    while descendant_queue_id != current_lineage_queue_id {
        if !visited.insert(descendant_queue_id.clone()) {
            return Err(IntentRevalidationFailure::FailVisible(
                "goal continuation parent chain contains a cycle".to_string(),
            ));
        }
        let rows = pending_by_queue
            .get(&descendant_queue_id)
            .map(Vec::as_slice)
            .unwrap_or_default();
        if rows.len() != 1 {
            return Err(IntentRevalidationFailure::FailVisible(format!(
                "goal continuation parent chain has {} durable queue rows for {}",
                rows.len(),
                descendant_queue_id
            )));
        }
        let row = &rows[0];
        let prior_intent_key = row
            .get("continuationIntentKey")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                IntentRevalidationFailure::PolicyBlocked(format!(
                    "goal continuation parent {} has no prior intent authority",
                    descendant_queue_id
                ))
            })?;
        let requeued_from_queue_id = row
            .get("requeuedFromQueueId")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                IntentRevalidationFailure::PolicyBlocked(format!(
                    "goal continuation parent {} has no durable predecessor",
                    descendant_queue_id
                ))
            })?;
        let recovered = latest_recoverable_intent_for_key(harness_home, prior_intent_key)
            .map_err(IntentRevalidationFailure::from_state_error)?;
        if let Some(reason) = recovered.corruption {
            return Err(IntentRevalidationFailure::FailVisible(format!(
                "goal continuation parent chain intent {prior_intent_key} is corrupt: {reason}"
            )));
        }
        let prior = recovered.latest.ok_or_else(|| {
            IntentRevalidationFailure::PolicyBlocked(format!(
                "goal continuation parent chain intent {prior_intent_key} is missing"
            ))
        })?;
        let exact_prior_authority = prior.status == GoalContinuationIntentStatus::Acknowledged
            && prior.child_queue_id.as_deref() == Some(descendant_queue_id.as_str())
            && prior.parent_queue_id == requeued_from_queue_id
            && prior.schema == GOAL_CONTINUATION_INTENT_SCHEMA
            && prior.working_session_key == intent.working_session_key
            && prior.lane_digest == intent.lane_digest
            && prior.virtual_session_id == intent.virtual_session_id
            && prior.lineage_id == intent.lineage_id
            && prior.goal_checksum == intent.goal_checksum
            && prior.campaign_family_id == intent.campaign_family_id
            && prior.backend_context_generation == intent.backend_context_generation
            && prior.human_input_generation == intent.human_input_generation
            && prior.campaign_slice_generation == expected_slice_generation
            && prior.campaign_slice_generation == prior.source_slice_generation.saturating_add(1)
            && prior.authority_version == Some(prior.source_slice_generation)
            && prior.authority_id == intent.authority_id
            && prior.authority_checksum == intent.authority_checksum
            && goal_intent_has_exact_current_authority(&prior);
        if !exact_prior_authority {
            return Ok(false);
        }
        descendant_queue_id = prior.parent_queue_id;
        expected_slice_generation = prior.source_slice_generation;
    }
    Ok(true)
}

fn exact_goal_closure_breaker(
    harness_home: &Path,
    intent: &GoalContinuationIntentV1,
    current: &crate::goal_lineage::GoalLineageV1,
) -> Result<bool, IntentRevalidationFailure> {
    let file = goal_closure_intents_file(harness_home);
    let text = match fs::read_to_string(file) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(IntentRevalidationFailure::Retry(error)),
    };
    for (index, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let closure = serde_json::from_str::<GoalClosureIntentV1>(line).map_err(|error| {
            IntentRevalidationFailure::FailVisible(format!(
                "goal closure intent ledger is invalid at line {}: {error}",
                index + 1
            ))
        })?;
        closure.validate().map_err(|error| {
            IntentRevalidationFailure::FailVisible(format!(
                "goal closure intent ledger failed checksum validation at line {}: {error}",
                index + 1
            ))
        })?;
        if closure.authority.lane_digest == intent.lane_digest
            && closure.authority.concrete_session_key == intent.working_session_key
            && closure.authority.virtual_session_id == intent.virtual_session_id
            && closure.authority.backend_context_generation == intent.backend_context_generation
            && closure.goal_generation == intent.lineage_id
            && closure.goal_identity == current.goal_reference
            && closure.expected_projection_checksum.as_deref()
                == Some(current.projection_checksum.as_str())
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn validate_parent_lane_for_intent(
    harness_home: &Path,
    intent: &GoalContinuationIntentV1,
) -> Result<(), IntentRevalidationFailure> {
    let file = harness_home
        .join("state")
        .join("runtime-queue")
        .join("pending.jsonl");
    let text = fs::read_to_string(file).map_err(IntentRevalidationFailure::from_state_error)?;
    let pending = text
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find(|value| {
            value.get("queueId").and_then(Value::as_str) == Some(intent.parent_queue_id.as_str())
        })
        .ok_or_else(|| {
            IntentRevalidationFailure::FailVisible(
                "goal continuation parent queue item is missing".to_string(),
            )
        })?;
    let field = |name: &str| {
        pending
            .get(name)
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                IntentRevalidationFailure::FailVisible(format!(
                    "goal continuation parent has no exact {name}"
                ))
            })
    };
    let session_key = field("sessionKey")?;
    if session_key != intent.working_session_key {
        return Err(IntentRevalidationFailure::FailVisible(
            "goal continuation intent session does not match its exact parent".to_string(),
        ));
    }
    let channel_lane = ChannelStateLane::new(
        field("platform")?,
        Some(field("accountId")?),
        field("channelId")?,
        field("userId")?,
        field("agentId")?,
    )
    .map_err(IntentRevalidationFailure::from_state_error)?;
    let full_lane = FullLaneKeyV1::new(
        channel_lane.platform(),
        channel_lane.account_id(),
        channel_lane.channel_id(),
        channel_lane.user_id(),
        channel_lane.agent_id(),
        field("runtimeClass")?,
        root_working_session_key(session_key),
        session_key,
    )
    .map_err(|error| IntentRevalidationFailure::FailVisible(error.to_string()))?;
    let actual_lane_digest = full_lane
        .identity_hash()
        .map_err(|error| IntentRevalidationFailure::FailVisible(error.to_string()))?;
    let actual_virtual_session_id =
        derive_virtual_session_id_v2(&channel_lane, &root_working_session_key(session_key));
    if actual_lane_digest != intent.lane_digest
        || actual_virtual_session_id != intent.virtual_session_id
    {
        return Err(IntentRevalidationFailure::FailVisible(
            "goal continuation intent does not match the exact parent lane/virtual session"
                .to_string(),
        ));
    }
    let snapshot_generation = intent.human_input_generation.ok_or_else(|| {
        IntentRevalidationFailure::FailVisible(
            "continuation intent has no durable human-input generation snapshot".to_string(),
        )
    })?;
    let (parent_generation, current_generation) = human_input_generation_at_parent(
        harness_home,
        &intent.parent_queue_id,
        &intent.lane_digest,
    )
    .map_err(IntentRevalidationFailure::from_state_error)?;
    if parent_generation != snapshot_generation {
        return Err(IntentRevalidationFailure::FailVisible(format!(
            "continuation human-input generation changed at its parent: committed={snapshot_generation} current={parent_generation}"
        )));
    }
    if current_generation > snapshot_generation {
        return Err(IntentRevalidationFailure::SupersededByHuman(format!(
            "newer same-exact-lane human input generation {current_generation} supersedes continuation generation {snapshot_generation}"
        )));
    }
    if intent.schema == TASK_CONTINUATION_INTENT_SCHEMA {
        crate::task_transition::revalidate_continuation_snapshot(
            harness_home,
            &crate::ContinuationAuthoritySnapshotV1 {
                kind: intent.authority_kind.ok_or_else(|| {
                    IntentRevalidationFailure::FailVisible(
                        "task intent has no authority kind".to_string(),
                    )
                })?,
                authority_id: intent.authority_id.clone().ok_or_else(|| {
                    IntentRevalidationFailure::FailVisible(
                        "task intent has no authority id".to_string(),
                    )
                })?,
                authority_version: intent.authority_version.ok_or_else(|| {
                    IntentRevalidationFailure::FailVisible(
                        "task intent has no authority version".to_string(),
                    )
                })?,
                authority_checksum: intent.authority_checksum.clone().ok_or_else(|| {
                    IntentRevalidationFailure::FailVisible(
                        "task intent has no authority checksum".to_string(),
                    )
                })?,
                exact_lane_digest: intent.lane_digest.clone(),
                virtual_session_id: intent.virtual_session_id.clone(),
                active_item_id: intent.active_item_id.clone(),
                active_item_version: intent.active_item_version,
                checkpoint_digest: intent.checkpoint_digest.clone().ok_or_else(|| {
                    IntentRevalidationFailure::FailVisible(
                        "task intent has no checkpoint digest".to_string(),
                    )
                })?,
            },
            session_key,
            channel_lane.agent_id(),
        )
        .map_err(IntentRevalidationFailure::from_policy_state_error)?;
    } else {
        if !goal_intent_has_exact_current_authority(intent) {
            return Err(IntentRevalidationFailure::FailVisible(
                "goal continuation intent has no durable exact current-owned authority proof"
                    .to_string(),
            ));
        }
        revalidate_current_goal_authority(harness_home, intent)?;
        let activation = load_goal_autonomy_activation_for_exact_goal(
            harness_home,
            Some(&intent.lane_digest),
            true,
        )
        .map_err(IntentRevalidationFailure::from_policy_state_error)?;
        if activation.mode != GoalAutonomyMode::Active {
            return Err(IntentRevalidationFailure::PolicyBlocked(format!(
                "exact-lane goal autonomy is no longer active: {}",
                activation.reason
            )));
        }
    }
    Ok(())
}

fn append_intent_state(
    harness_home: &Path,
    mut receipt: GoalContinuationIntentV1,
    status: GoalContinuationIntentStatus,
    child_queue_id: Option<String>,
    reason: &str,
    now_ms: i64,
) -> io::Result<GoalContinuationIntentV1> {
    receipt.status = status;
    receipt.child_queue_id = child_queue_id;
    receipt.reason = reason.to_string();
    receipt.observed_at_ms = now_ms;
    append_jsonl_value(&goal_continuation_intents_file(harness_home), &receipt)?;
    Ok(receipt)
}

fn ensure_same_intent(
    existing: &GoalContinuationIntentV1,
    candidate: &GoalContinuationIntentV1,
) -> io::Result<()> {
    if existing.schema != candidate.schema
        || existing.intent_key != candidate.intent_key
        || existing.parent_queue_id != candidate.parent_queue_id
        || existing.working_session_key != candidate.working_session_key
        || existing.lane_digest != candidate.lane_digest
        || existing.virtual_session_id != candidate.virtual_session_id
        || existing.lineage_id != candidate.lineage_id
        || existing.goal_checksum != candidate.goal_checksum
        || existing.campaign_family_id != candidate.campaign_family_id
        || existing.backend_context_generation != candidate.backend_context_generation
        || existing.source_slice_generation != candidate.source_slice_generation
        || existing.decision_generation != candidate.decision_generation
        || existing.campaign_slice_generation != candidate.campaign_slice_generation
        || existing.recovery_continuation_index != candidate.recovery_continuation_index
        || !legacy_additive_field_compatible(
            &existing.human_input_generation,
            &candidate.human_input_generation,
        )
        || !legacy_additive_field_compatible(
            &existing.goal_relation_at_commit,
            &candidate.goal_relation_at_commit,
        )
        || !legacy_additive_field_compatible(
            &existing.goal_status_at_commit,
            &candidate.goal_status_at_commit,
        )
        || existing.authority_kind != candidate.authority_kind
        || existing.authority_id != candidate.authority_id
        || existing.authority_version != candidate.authority_version
        || existing.authority_checksum != candidate.authority_checksum
        || existing.checkpoint_digest != candidate.checkpoint_digest
        || existing.active_item_id != candidate.active_item_id
        || existing.active_item_version != candidate.active_item_version
        || existing.disposition_recovery_depth != candidate.disposition_recovery_depth
        || existing.shell_recovery_depth != candidate.shell_recovery_depth
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "deterministic goal continuation key collided with different immutable authority",
        ));
    }
    Ok(())
}

fn ensure_authorized_intent_ledger_transition(
    harness_home: &Path,
    existing: &GoalContinuationIntentV1,
    candidate: &GoalContinuationIntentV1,
) -> io::Result<()> {
    ensure_same_intent(existing, candidate)?;
    let additive_fields_changed = existing.human_input_generation
        != candidate.human_input_generation
        || existing.goal_relation_at_commit != candidate.goal_relation_at_commit
        || existing.goal_status_at_commit != candidate.goal_status_at_commit;
    if !additive_fields_changed {
        return Ok(());
    }
    let (recovered, changed) =
        upgrade_open_legacy_intent(harness_home, existing).map_err(|failure| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                match failure {
                    IntentRevalidationFailure::SupersededByHuman(reason)
                    | IntentRevalidationFailure::PolicyBlocked(reason)
                    | IntentRevalidationFailure::FailVisible(reason) => format!(
                        "continuation additive-field transition has no exact recovery proof: {reason}"
                    ),
                    IntentRevalidationFailure::Retry(error) => format!(
                        "continuation additive-field transition proof could not be read: {error}"
                    ),
                },
            )
        })?;
    if !changed
        || recovered.human_input_generation != candidate.human_input_generation
        || recovered.goal_relation_at_commit != candidate.goal_relation_at_commit
        || recovered.goal_status_at_commit != candidate.goal_status_at_commit
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "continuation additive fields do not equal exact reconstructed legacy values",
        ));
    }
    Ok(())
}

fn legacy_additive_field_compatible<T: PartialEq>(
    existing: &Option<T>,
    candidate: &Option<T>,
) -> bool {
    match (existing, candidate) {
        (Some(existing), Some(candidate)) => existing == candidate,
        (Some(_), None) => false,
        (None, _) => true,
    }
}

fn required<'a>(value: &'a Option<String>, field: &str) -> io::Result<&'a str> {
    value
        .as_deref()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{field} is required for goal continuation intent"),
            )
        })
}

fn goal_status_is_active(status: &str) -> bool {
    matches!(
        status
            .trim()
            .to_ascii_lowercase()
            .replace(['-', '_', ' '], "")
            .as_str(),
        "active" | "running" | "inprogress"
    )
}

fn goal_intent_has_exact_current_authority(intent: &GoalContinuationIntentV1) -> bool {
    intent.schema == GOAL_CONTINUATION_INTENT_SCHEMA
        && intent.authority_kind == Some(crate::ContinuationAuthorityKindV1::Goal)
        && intent.authority_id.as_deref() == Some(intent.lineage_id.as_str())
        && intent.authority_checksum.as_deref() == Some(intent.goal_checksum.as_str())
        && matches!(
            intent.goal_relation_at_commit,
            Some(
                GoalTransitionRelation::CurrentGoalSlice
                    | GoalTransitionRelation::AuthorizedCampaignContinuation
            )
        )
        && intent
            .goal_status_at_commit
            .as_deref()
            .is_some_and(goal_status_is_active)
}

fn canonical_identity_bytes(parts: &[&str]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for part in parts {
        bytes.extend_from_slice(&(part.len() as u64).to_be_bytes());
        bytes.extend_from_slice(part.as_bytes());
    }
    bytes
}

fn sha256_hex(bytes: &[u8]) -> String {
    digest::digest(&digest::SHA256, bytes)
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::goal_transition::{GoalTransitionEventKind, GoalTransitionSurface};

    #[test]
    fn intent_key_is_idempotent_and_slice_count_is_not_recovery_depth() {
        let root =
            std::env::temp_dir().join(format!("goal-continuation-intent-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let prepared = prepare_goal_intent(&root, "queue-a", "dm-a", "user-a");
        let mut transition = transition();
        transition.queue_id = Some(prepared.parent_queue_id.clone());
        transition.lane_digest = Some(prepared.lane_digest.clone());
        transition.virtual_session_id = Some(prepared.virtual_session_id.clone());
        transition.lineage_id = Some(prepared.lineage_id.clone());
        transition.campaign_family_id = Some(prepared.campaign_family_id.clone());
        transition.goal_checksum = Some(prepared.goal_checksum.clone());
        let session_key = prepared.working_session_key.clone();
        fs::remove_file(goal_continuation_intents_file(&root)).unwrap();
        let continuation = RuntimeContinuationMetadata {
            continuation_index: Some(7),
            campaign_slice_generation: Some(3),
            ..RuntimeContinuationMetadata::legacy()
        };
        let first =
            commit_goal_continuation_intent(&root, &transition, &session_key, &continuation, 10)
                .unwrap();
        let replay =
            commit_goal_continuation_intent(&root, &transition, &session_key, &continuation, 11)
                .unwrap();
        assert_eq!(first.intent_key, replay.intent_key);
        assert_eq!(first.campaign_slice_generation, 4);
        assert_eq!(first.recovery_continuation_index, 7);
        let lines = fs::read_to_string(goal_continuation_intents_file(&root))
            .unwrap()
            .lines()
            .count();
        assert_eq!(lines, 1, "replay must not append a second logical commit");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn active_mode_requires_an_exact_lane_cohort() {
        let root = std::env::temp_dir().join(format!("goal-autonomy-mode-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        fs::write(
            root.join("harness-config.json"),
            r#"{"goalAutonomy":{"mode":"active","activeLaneDigests":["lane-a"]}}"#,
        )
        .unwrap();
        assert_eq!(
            load_goal_autonomy_activation(&root, Some("lane-a"))
                .unwrap()
                .mode,
            GoalAutonomyMode::Active
        );
        assert_eq!(
            load_goal_autonomy_activation(&root, Some("lane-b"))
                .unwrap()
                .mode,
            GoalAutonomyMode::Observe
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn implicit_goal_autonomy_requires_exact_authority_and_explicit_observe_wins() {
        let root = test_root("goal-autonomy-implicit-exact-authority");
        fs::create_dir_all(&root).unwrap();
        assert_eq!(
            load_goal_autonomy_activation(&root, Some("lane-a"))
                .unwrap()
                .mode,
            GoalAutonomyMode::Observe,
            "generic or historical callers do not gain implicit authority"
        );
        assert_eq!(
            load_goal_autonomy_activation_for_exact_goal(&root, Some("lane-a"), true)
                .unwrap()
                .mode,
            GoalAutonomyMode::Active
        );
        assert_eq!(
            load_goal_autonomy_activation_for_exact_goal(&root, None, true)
                .unwrap()
                .mode,
            GoalAutonomyMode::Observe,
            "implicit authority still requires an exact lane"
        );
        fs::write(
            root.join("harness-config.json"),
            br#"{"goalAutonomy":{"mode":"observe"}}"#,
        )
        .unwrap();
        assert_eq!(
            load_goal_autonomy_activation_for_exact_goal(&root, Some("lane-a"), true)
                .unwrap()
                .mode,
            GoalAutonomyMode::Observe,
            "an explicit observe block is an opt-out"
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn historical_or_unproven_goal_state_never_gains_implicit_authority() {
        let root = test_root("goal-autonomy-unproven-state");
        let mut historical = transition();
        historical.relation = GoalTransitionRelation::HistoricalState;
        let error = commit_goal_continuation_intent(
            &root,
            &historical,
            "session-a",
            &RuntimeContinuationMetadata::legacy(),
            10,
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);

        let committed = prepare_goal_intent(&root, "queue-unproven", "dm-unproven", "user-a");
        let intents_file = goal_continuation_intents_file(&root);
        let mut unproven: Value =
            serde_json::from_str(fs::read_to_string(&intents_file).unwrap().trim()).unwrap();
        unproven
            .as_object_mut()
            .unwrap()
            .remove("goalRelationAtCommit");
        unproven
            .as_object_mut()
            .unwrap()
            .remove("goalStatusAtCommit");
        fs::write(
            &intents_file,
            format!("{}\n", serde_json::to_string(&unproven).unwrap()),
        )
        .unwrap();
        assert!(
            reconcile_goal_continuation_intents(&root, 20)
                .unwrap()
                .is_empty()
        );
        assert!(is_goal_continuation_intent_fenced(&root, &committed.intent_key).unwrap());
        assert!(
            find_child_queue_for_intent(&root, &committed.intent_key)
                .unwrap()
                .is_none()
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn exact_authorized_intent_reconciles_without_config_after_restart() {
        let root = test_root("goal-continuation-restart");
        let committed = prepare_goal_intent_with_metadata(
            &root,
            "queue-a",
            "dm-a",
            "user-a",
            5,
            RuntimeContinuationMetadata {
                continuation_index: Some(5),
                campaign_slice_generation: Some(3),
                ..RuntimeContinuationMetadata::legacy()
            },
        );
        let queue_dir = root.join("state").join("runtime-queue");
        assert!(
            find_child_queue_for_intent(&root, &committed.intent_key)
                .unwrap()
                .is_none()
        );
        let reconciled = reconcile_goal_continuation_intents(&root, 11).unwrap();
        assert_eq!(reconciled.len(), 1);
        assert_eq!(reconciled[0].status, GoalContinuationIntentStatus::Enqueued);
        assert_eq!(reconciled[0].recovery_continuation_index, 5);
        assert_eq!(
            reconcile_goal_continuation_intents(&root, 12)
                .unwrap()
                .len(),
            0
        );
        let pending = fs::read_to_string(queue_dir.join("pending.jsonl")).unwrap();
        assert_eq!(
            pending
                .lines()
                .filter_map(|line| serde_json::from_str::<Value>(line).ok())
                .filter(|value| {
                    value.get("continuationIntentKey").and_then(Value::as_str)
                        == Some(committed.intent_key.as_str())
                })
                .count(),
            1
        );
        let child: Value = pending
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .find(|value| {
                value.get("continuationIntentKey").and_then(Value::as_str)
                    == Some(committed.intent_key.as_str())
            })
            .unwrap();
        assert_eq!(
            child.get("continuationIndex").and_then(Value::as_u64),
            Some(5)
        );
        assert_eq!(
            child.get("campaignSliceGeneration").and_then(Value::as_u64),
            Some(4)
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn committed_goal_intent_is_fenced_when_exact_lane_activation_is_revoked() {
        let root = test_root("goal-continuation-config-revoked");
        let committed = prepare_goal_intent(&root, "queue-revoked", "dm-revoked", "user-a");
        write_goal_autonomy_config(&root, &[]);

        let reconciled = reconcile_goal_continuation_intents(&root, 20).unwrap();
        assert!(reconciled.is_empty());
        assert!(is_goal_continuation_intent_fenced(&root, &committed.intent_key).unwrap());
        assert_eq!(
            latest_goal_continuation_intent(&root, &committed.intent_key)
                .unwrap()
                .unwrap()
                .status,
            GoalContinuationIntentStatus::Committed
        );
        assert!(
            find_child_queue_for_intent(&root, &committed.intent_key)
                .unwrap()
                .is_none()
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn stale_committed_intent_does_not_block_unrelated_valid_intent() {
        let root = test_root("goal-continuation-stale-isolation");
        let valid = prepare_goal_intent(&root, "queue-valid", "dm-valid", "user-valid");
        let stale = prepare_goal_intent(&root, "queue-missing", "dm-missing", "user-missing");
        let pending_file = root
            .join("state")
            .join("runtime-queue")
            .join("pending.jsonl");
        let retained = fs::read_to_string(&pending_file)
            .unwrap()
            .lines()
            .filter(|line| {
                serde_json::from_str::<Value>(line)
                    .ok()
                    .and_then(|value| {
                        value
                            .get("queueId")
                            .and_then(Value::as_str)
                            .map(|queue_id| queue_id != "queue-missing")
                    })
                    .unwrap_or(true)
            })
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(&pending_file, format!("{retained}\n")).unwrap();
        write_goal_autonomy_config(
            &root,
            &[valid.lane_digest.as_str(), stale.lane_digest.as_str()],
        );

        let reconciled = reconcile_goal_continuation_intents(&root, 20).unwrap();
        assert_eq!(reconciled.len(), 1);
        assert!(is_goal_continuation_intent_fenced(&root, &stale.intent_key).unwrap());
        assert!(reconciled.iter().any(|receipt| {
            receipt.intent_key == valid.intent_key
                && receipt.status == GoalContinuationIntentStatus::Enqueued
        }));
        assert!(
            find_child_queue_for_intent(&root, &valid.intent_key)
                .unwrap()
                .is_some()
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn malformed_intent_record_is_quarantined_without_blocking_valid_intent() {
        let root = test_root("goal-continuation-malformed-isolation");
        let valid = prepare_goal_intent(&root, "queue-valid", "dm-valid", "user-valid");
        write_goal_autonomy_config(&root, &[valid.lane_digest.as_str()]);
        let intents_file = goal_continuation_intents_file(&root);
        let mut ledger = fs::read_to_string(&intents_file).unwrap();
        ledger.push_str("{not-valid-json}\n");
        fs::write(&intents_file, ledger).unwrap();

        let reconciled = reconcile_goal_continuation_intents(&root, 20).unwrap();
        assert!(reconciled.iter().any(|receipt| {
            receipt.intent_key == valid.intent_key
                && receipt.status == GoalContinuationIntentStatus::Enqueued
        }));
        let errors =
            fs::read_to_string(goal_continuation_reconciliation_errors_file(&root)).unwrap();
        let receipts = errors
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(receipts.len(), 1);
        assert_eq!(
            receipts[0].get("schema").and_then(Value::as_str),
            Some(GOAL_CONTINUATION_RECONCILIATION_ERROR_SCHEMA)
        );
        assert_eq!(
            receipts[0].get("disposition").and_then(Value::as_str),
            Some("quarantined")
        );
        assert_eq!(
            reconcile_goal_continuation_intents(&root, 21)
                .unwrap()
                .len(),
            0
        );
        assert_eq!(
            fs::read_to_string(goal_continuation_reconciliation_errors_file(&root))
                .unwrap()
                .lines()
                .count(),
            1,
            "the same malformed ledger record must have one durable quarantine receipt"
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn fenced_intent_is_terminal_and_idempotent_across_restarts() {
        let root = test_root("goal-continuation-fence-idempotent");
        let committed = prepare_goal_intent(&root, "queue-fenced", "dm-fenced", "user-a");
        write_goal_autonomy_config(&root, &[]);

        let first = reconcile_goal_continuation_intents(&root, 20).unwrap();
        assert!(first.is_empty());
        assert_eq!(
            reconcile_goal_continuation_intents(&root, 21)
                .unwrap()
                .len(),
            0
        );
        let fenced_count = fs::read_to_string(goal_continuation_fences_file(&root))
            .unwrap()
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .filter(|value| {
                value.get("intentKey").and_then(Value::as_str)
                    == Some(committed.intent_key.as_str())
                    && value.get("disposition").and_then(Value::as_str) == Some("fenced")
            })
            .count();
        assert_eq!(fenced_count, 1);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn new_fence_evidence_keeps_the_v1_intent_ledger_readable_by_old_readers() {
        #[derive(Debug, Deserialize)]
        #[serde(rename_all = "kebab-case")]
        enum OldIntentStatus {
            Committed,
            Enqueued,
            Acknowledged,
        }

        #[derive(Debug, Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct OldIntentReader {
            status: OldIntentStatus,
        }

        let root = test_root("goal-continuation-old-reader-compatibility");
        let committed = prepare_goal_intent(&root, "queue-old-reader", "dm-old", "user-a");
        write_goal_autonomy_config(&root, &[]);
        reconcile_goal_continuation_intents(&root, 20).unwrap();

        let old_reader_results = fs::read_to_string(goal_continuation_intents_file(&root))
            .unwrap()
            .lines()
            .map(serde_json::from_str::<OldIntentReader>)
            .collect::<Result<Vec<_>, _>>();
        assert!(
            old_reader_results.is_ok(),
            "new fence evidence must never add a status unknown to a v1 reader"
        );
        let old_reader_results = old_reader_results.unwrap();
        assert_eq!(
            old_reader_results.len(),
            1,
            "the durable fence belongs in a separate overlay, not the v1 intent ledger"
        );
        assert!(matches!(
            old_reader_results[0].status,
            OldIntentStatus::Committed
        ));
        assert!(goal_continuation_fences_file(&root).is_file());
        assert!(is_goal_continuation_intent_fenced(&root, &committed.intent_key).unwrap());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn unrelated_malformed_record_does_not_block_targeted_enqueue_lookup() {
        let root = test_root("goal-continuation-targeted-malformed-isolation");
        let committed = prepare_goal_intent(&root, "queue-targeted", "dm-targeted", "user-a");
        write_goal_autonomy_config(&root, &[committed.lane_digest.as_str()]);
        let intents_file = goal_continuation_intents_file(&root);
        let mut ledger = fs::read_to_string(&intents_file).unwrap();
        ledger.push_str("{unrelated-malformed-json}\n");
        fs::write(&intents_file, ledger).unwrap();

        let enqueued = ensure_goal_continuation_enqueued(&root, &committed.intent_key, 20).unwrap();
        assert_eq!(enqueued.status, GoalContinuationIntentStatus::Enqueued);
        assert!(enqueued.child_queue_id.is_some());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn unrelated_invalid_pending_lane_does_not_poison_targeted_generation_lookup() {
        let root = test_root("goal-continuation-pending-lane-isolation");
        let committed = prepare_goal_intent(&root, "queue-targeted", "dm-targeted", "user-a");
        append_jsonl_value(
            &root
                .join("state")
                .join("runtime-queue")
                .join("pending.jsonl"),
            &serde_json::json!({
                "queueId": "queue-unrelated-invalid",
                "status": "queued",
                "runtimeClass": "interactive",
                "origin": "channel",
                "platform": "telegram",
                "accountId": "acct-unrelated",
                "channelId": "unrelated\u{0001}",
                "userId": "user-unrelated",
                "agentId": "main",
                "sessionKey": "session-unrelated",
                "messageText": "must not poison another exact lane"
            }),
        )
        .unwrap();

        let enqueued = ensure_goal_continuation_enqueued(&root, &committed.intent_key, 20).unwrap();

        assert_eq!(enqueued.status, GoalContinuationIntentStatus::Enqueued);
        assert!(enqueued.child_queue_id.is_some());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn parallel_reconciliation_creates_one_deterministic_child_lifecycle() {
        let root = test_root("goal-continuation-parallel-child");
        let committed = prepare_goal_intent(&root, "queue-parallel", "dm-parallel", "user-a");
        write_goal_autonomy_config(&root, &[committed.lane_digest.as_str()]);
        let workers = 8;
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(workers));
        let root = std::sync::Arc::new(root);
        let handles = (0..workers)
            .map(|_| {
                let barrier = std::sync::Arc::clone(&barrier);
                let root = std::sync::Arc::clone(&root);
                std::thread::spawn(move || {
                    barrier.wait();
                    reconcile_goal_continuation_intents(root.as_ref(), 20)
                })
            })
            .collect::<Vec<_>>();
        for handle in handles {
            handle.join().unwrap().unwrap();
        }

        let pending = fs::read_to_string(
            root.join("state")
                .join("runtime-queue")
                .join("pending.jsonl"),
        )
        .unwrap();
        assert_eq!(
            pending
                .lines()
                .filter_map(|line| serde_json::from_str::<Value>(line).ok())
                .filter(|value| {
                    value.get("continuationIntentKey").and_then(Value::as_str)
                        == Some(committed.intent_key.as_str())
                })
                .count(),
            1
        );
        let target_states = fs::read_to_string(goal_continuation_intents_file(root.as_ref()))
            .unwrap()
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .filter(|value| {
                value.get("intentKey").and_then(Value::as_str)
                    == Some(committed.intent_key.as_str())
            })
            .filter_map(|value| {
                value
                    .get("status")
                    .and_then(Value::as_str)
                    .map(ToString::to_string)
            })
            .collect::<Vec<_>>();
        assert_eq!(target_states, vec!["committed", "enqueued"]);
        let _ = fs::remove_dir_all(root.as_ref());
    }

    #[test]
    fn parallel_malformed_reconciliation_writes_one_error_receipt() {
        let root = test_root("goal-continuation-parallel-error");
        let intents_file = goal_continuation_intents_file(&root);
        fs::create_dir_all(intents_file.parent().unwrap()).unwrap();
        fs::write(&intents_file, "{malformed-json}\n").unwrap();
        let workers = 8;
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(workers));
        let root = std::sync::Arc::new(root);
        let handles = (0..workers)
            .map(|_| {
                let barrier = std::sync::Arc::clone(&barrier);
                let root = std::sync::Arc::clone(&root);
                std::thread::spawn(move || {
                    barrier.wait();
                    reconcile_goal_continuation_intents(root.as_ref(), 20)
                })
            })
            .collect::<Vec<_>>();
        for handle in handles {
            handle.join().unwrap().unwrap();
        }
        assert_eq!(
            fs::read_to_string(goal_continuation_reconciliation_errors_file(root.as_ref()))
                .unwrap()
                .lines()
                .count(),
            1
        );
        let _ = fs::remove_dir_all(root.as_ref());
    }

    #[test]
    fn newer_same_lane_human_input_fences_an_unleased_continuation() {
        let root = test_root("goal-continuation-human-before-enqueue");
        let committed = prepare_goal_intent(&root, "queue-parent", "dm-human", "user-a");
        append_same_exact_lane_human(&root, "queue-parent", "queue-human");

        let reconciled = reconcile_goal_continuation_intents(&root, 20).unwrap();

        assert!(reconciled.is_empty());
        let fence = latest_goal_continuation_fence(&root, &committed.intent_key)
            .unwrap()
            .unwrap();
        assert!(fence.reason.contains("newer same-exact-lane human input"));
        assert!(
            find_child_queue_for_intent(&root, &committed.intent_key)
                .unwrap()
                .is_none()
        );
        let pending = fs::read_to_string(
            root.join("state")
                .join("runtime-queue")
                .join("pending.jsonl"),
        )
        .unwrap();
        assert!(pending.lines().any(|line| {
            serde_json::from_str::<Value>(line)
                .ok()
                .and_then(|value| {
                    value
                        .get("queueId")
                        .and_then(Value::as_str)
                        .map(|queue_id| queue_id == "queue-human")
                })
                .unwrap_or(false)
        }));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn lease_gate_never_acknowledges_an_intent_fenced_after_lease() {
        let root = test_root("goal-continuation-lease-fence");
        let committed = prepare_goal_intent(&root, "queue-parent", "dm-lease", "user-a");
        write_goal_autonomy_config(&root, &[committed.lane_digest.as_str()]);
        let enqueued = ensure_goal_continuation_enqueued(&root, &committed.intent_key, 20).unwrap();
        let item = continuation_gate_item(&root, &enqueued);

        write_goal_autonomy_config(&root, &[]);
        reconcile_goal_continuation_intents(&root, 30).unwrap();
        let gate = gate_goal_continuation_after_lease(&root, &item, 31).unwrap();
        assert!(matches!(gate, GoalContinuationLeaseGate::Suppressed(_)));
        let target_states = fs::read_to_string(goal_continuation_intents_file(&root))
            .unwrap()
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .filter(|value| {
                value.get("intentKey").and_then(Value::as_str)
                    == Some(committed.intent_key.as_str())
            })
            .filter_map(|value| {
                value
                    .get("status")
                    .and_then(Value::as_str)
                    .map(ToString::to_string)
            })
            .collect::<Vec<_>>();
        assert_eq!(target_states, vec!["committed", "enqueued"]);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn newer_same_lane_human_input_suppresses_a_leased_child_before_acknowledgement() {
        let root = test_root("goal-continuation-human-after-lease");
        let committed = prepare_goal_intent(&root, "queue-parent", "dm-lease-human", "user-a");
        let enqueued = ensure_goal_continuation_enqueued(&root, &committed.intent_key, 20).unwrap();
        let item = continuation_gate_item(&root, &enqueued);
        append_same_exact_lane_human(&root, "queue-parent", "queue-human");

        let gate = gate_goal_continuation_after_lease(&root, &item, 30).unwrap();

        let GoalContinuationLeaseGate::Suppressed(fence) = gate else {
            panic!("newer human input must suppress the leased continuation child");
        };
        assert!(fence.reason.contains("newer same-exact-lane human input"));
        let target_states = fs::read_to_string(goal_continuation_intents_file(&root))
            .unwrap()
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .filter(|value| {
                value.get("intentKey").and_then(Value::as_str)
                    == Some(committed.intent_key.as_str())
            })
            .filter_map(|value| {
                value
                    .get("status")
                    .and_then(Value::as_str)
                    .map(ToString::to_string)
            })
            .collect::<Vec<_>>();
        assert_eq!(target_states, vec!["committed", "enqueued"]);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn newer_same_lane_human_input_prevents_an_acknowledged_slice_from_chaining() {
        let root = test_root("goal-continuation-human-during-slice");
        let committed = prepare_goal_intent(&root, "queue-parent", "dm-running-human", "user-a");
        let enqueued = ensure_goal_continuation_enqueued(&root, &committed.intent_key, 20).unwrap();
        let child_queue_id = enqueued.child_queue_id.clone().unwrap();
        let item = continuation_gate_item(&root, &enqueued);
        assert!(matches!(
            gate_goal_continuation_after_lease(&root, &item, 30).unwrap(),
            GoalContinuationLeaseGate::Acknowledged(_)
        ));
        append_same_exact_lane_human(&root, "queue-parent", "queue-human");

        let mut next_transition = transition();
        next_transition.queue_id = Some(child_queue_id.clone());
        next_transition.lineage_id = Some(committed.lineage_id.clone());
        next_transition.campaign_family_id = Some(committed.campaign_family_id.clone());
        next_transition.goal_checksum = Some(committed.goal_checksum.clone());
        next_transition.lane_digest = Some(committed.lane_digest.clone());
        next_transition.virtual_session_id = Some(committed.virtual_session_id.clone());
        next_transition.source_slice_generation = committed.campaign_slice_generation;
        next_transition.decision_generation = committed.decision_generation.saturating_add(1);
        let next = commit_goal_continuation_intent(
            &root,
            &next_transition,
            &item.session_key,
            &item.continuation,
            40,
        )
        .unwrap();
        let error = ensure_goal_continuation_enqueued(&root, &next.intent_key, 41).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        let fence = latest_goal_continuation_fence(&root, &next.intent_key)
            .unwrap()
            .unwrap();
        assert!(fence.reason.contains("newer same-exact-lane human input"));
        assert!(
            find_child_queue_for_intent(&root, &next.intent_key)
                .unwrap()
                .is_none(),
            "the completed bounded slice must not chain another continuation"
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn revoked_enqueued_intent_quarantines_its_child_before_fencing() {
        let root = test_root("goal-continuation-enqueued-child-fence");
        let committed = prepare_goal_intent(&root, "queue-parent", "dm-fence", "user-a");
        write_goal_autonomy_config(&root, &[committed.lane_digest.as_str()]);
        let enqueued = ensure_goal_continuation_enqueued(&root, &committed.intent_key, 20).unwrap();
        let child_queue_id = enqueued.child_queue_id.clone().unwrap();

        write_goal_autonomy_config(&root, &[]);
        let reconciled = reconcile_goal_continuation_intents(&root, 30).unwrap();

        assert!(reconciled.is_empty());
        let fence = latest_goal_continuation_fence(&root, &committed.intent_key)
            .unwrap()
            .unwrap();
        assert_eq!(
            fence.child_queue_id.as_deref(),
            Some(child_queue_id.as_str())
        );
        let control =
            crate::runtime_worker::resolve_queue_terminal_control(&root, &child_queue_id, None)
                .unwrap();
        assert!(matches!(
            control,
            crate::QueueTerminalControl::Terminal(crate::QueueTerminalControlMatch {
                source: crate::TerminalControlSource::Quarantine,
                ..
            })
        ));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn crash_window_child_is_discovered_and_quarantined_before_committed_intent_is_fenced() {
        let root = test_root("goal-continuation-crash-window-child-fence");
        let committed = prepare_goal_intent(&root, "queue-parent", "dm-crash", "user-a");
        write_goal_autonomy_config(&root, &[committed.lane_digest.as_str()]);
        let intents_file = goal_continuation_intents_file(&root);
        let committed_ledger = fs::read_to_string(&intents_file).unwrap();
        let enqueued = ensure_goal_continuation_enqueued(&root, &committed.intent_key, 20).unwrap();
        let child_queue_id = enqueued.child_queue_id.clone().unwrap();
        fs::write(&intents_file, committed_ledger).unwrap();

        write_goal_autonomy_config(&root, &[]);
        let reconciled = reconcile_goal_continuation_intents(&root, 30).unwrap();

        assert!(reconciled.is_empty());
        let fence = latest_goal_continuation_fence(&root, &committed.intent_key)
            .unwrap()
            .unwrap();
        assert_eq!(
            fence.child_queue_id.as_deref(),
            Some(child_queue_id.as_str())
        );
        assert!(matches!(
            crate::runtime_worker::resolve_queue_terminal_control(&root, &child_queue_id, None,)
                .unwrap(),
            crate::QueueTerminalControl::Terminal(crate::QueueTerminalControlMatch {
                source: crate::TerminalControlSource::Quarantine,
                ..
            })
        ));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn origin_v1_goal_committed_and_enqueued_restart_replay_recovers_only_exact_additive_fields() {
        for initially_enqueued in [false, true] {
            let root = test_root(if initially_enqueued {
                "origin-v1-goal-enqueued-replay"
            } else {
                "origin-v1-goal-committed-replay"
            });
            let committed = prepare_goal_intent(&root, "queue-parent", "dm-origin-v1", "user-a");
            let before = if initially_enqueued {
                ensure_goal_continuation_enqueued(&root, &committed.intent_key, 20).unwrap()
            } else {
                committed.clone()
            };
            append_exact_transition_proof(&root, &committed);
            strip_origin_v1_additive_fields(&root, &committed.intent_key);

            reconcile_goal_continuation_intents(&root, 30).unwrap();
            reconcile_goal_continuation_intents(&root, 31).unwrap();

            let recovered = latest_goal_continuation_intent(&root, &committed.intent_key)
                .unwrap()
                .unwrap();
            assert_eq!(recovered.status, GoalContinuationIntentStatus::Enqueued);
            assert_eq!(recovered.human_input_generation, Some(1));
            assert_eq!(
                recovered.goal_relation_at_commit,
                Some(GoalTransitionRelation::CurrentGoalSlice)
            );
            assert_eq!(recovered.goal_status_at_commit.as_deref(), Some("active"));
            if initially_enqueued {
                assert_eq!(recovered.child_queue_id, before.child_queue_id);
            }
            assert!(
                latest_goal_continuation_fence(&root, &committed.intent_key)
                    .unwrap()
                    .is_none()
            );
            let children = fs::read_to_string(
                root.join("state")
                    .join("runtime-queue")
                    .join("pending.jsonl"),
            )
            .unwrap()
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .filter(|value| {
                value.get("continuationIntentKey").and_then(Value::as_str)
                    == Some(committed.intent_key.as_str())
            })
            .count();
            assert_eq!(children, 1);
            let _ = fs::remove_dir_all(root);
        }
    }

    #[test]
    fn origin_v1_goal_mismatch_fails_visibly_and_later_human_is_runnable() {
        let root = test_root("origin-v1-goal-proof-mismatch");
        let committed = prepare_goal_intent(&root, "queue-parent", "dm-mismatch", "user-a");
        let mut wrong = committed.clone();
        wrong.goal_checksum = "mismatched-proof-checksum".to_string();
        append_exact_transition_proof(&root, &wrong);
        strip_origin_v1_additive_fields(&root, &committed.intent_key);

        reconcile_goal_continuation_intents(&root, 30).unwrap();
        reconcile_goal_continuation_intents(&root, 31).unwrap();

        assert!(
            find_child_queue_for_intent(&root, &committed.intent_key)
                .unwrap()
                .is_none()
        );
        assert_visible_settlement_once(
            &root,
            &committed,
            "failed-terminal",
            "suppressed-exact-owner-mismatch",
        );
        append_same_exact_lane_human(&root, "queue-parent", "queue-human");
        assert_human_queue_is_runnable(&root, "queue-human", &committed.working_session_key);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn origin_v1_missing_human_generation_is_inferred_then_new_human_supersedes_without_outbox() {
        let root = test_root("origin-v1-new-human-supersession");
        let committed = prepare_goal_intent(&root, "queue-parent", "dm-new-human", "user-a");
        append_exact_transition_proof(&root, &committed);
        strip_origin_v1_additive_fields(&root, &committed.intent_key);
        append_same_exact_lane_human(&root, "queue-parent", "queue-human");

        reconcile_goal_continuation_intents(&root, 30).unwrap();
        reconcile_goal_continuation_intents(&root, 31).unwrap();

        let receipts = continuation_settlement_receipts(&root, &committed.intent_key);
        assert_eq!(receipts.len(), 1);
        assert_eq!(
            receipts[0]
                .get("sourceFinalExpectation")
                .and_then(Value::as_str),
            Some("explicit-non-delivery")
        );
        assert_eq!(
            receipts[0]
                .get("finalOutboxDisposition")
                .and_then(Value::as_str),
            Some("explicit-non-delivery")
        );
        assert!(source_outbox_messages(&root, &committed.parent_queue_id).is_empty());
        assert_human_queue_is_runnable(&root, "queue-human", &committed.working_session_key);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn origin_v1_task_committed_and_enqueued_restart_replay_recovers_human_generation() {
        for initially_enqueued in [false, true] {
            let root = test_root(if initially_enqueued {
                "origin-v1-task-enqueued-replay"
            } else {
                "origin-v1-task-committed-replay"
            });
            let committed =
                prepare_task_intent(&root, "queue-task-parent", "dm-origin-v1-task", "user-a");
            let before = if initially_enqueued {
                ensure_goal_continuation_enqueued(&root, &committed.intent_key, 20).unwrap()
            } else {
                committed.clone()
            };
            strip_origin_v1_additive_fields(&root, &committed.intent_key);

            reconcile_goal_continuation_intents(&root, 30).unwrap();
            reconcile_goal_continuation_intents(&root, 31).unwrap();

            let recovered = latest_goal_continuation_intent(&root, &committed.intent_key)
                .unwrap()
                .unwrap();
            assert_eq!(recovered.schema, TASK_CONTINUATION_INTENT_SCHEMA);
            assert_eq!(recovered.status, GoalContinuationIntentStatus::Enqueued);
            assert_eq!(recovered.human_input_generation, Some(1));
            assert_eq!(recovered.authority_kind, committed.authority_kind);
            assert_eq!(recovered.authority_id, committed.authority_id);
            assert_eq!(recovered.authority_version, committed.authority_version);
            assert_eq!(recovered.checkpoint_digest, committed.checkpoint_digest);
            if initially_enqueued {
                assert_eq!(recovered.child_queue_id, before.child_queue_id);
            }
            assert!(
                latest_goal_continuation_fence(&root, &committed.intent_key)
                    .unwrap()
                    .is_none()
            );
            let child_count = fs::read_to_string(
                root.join("state")
                    .join("runtime-queue")
                    .join("pending.jsonl"),
            )
            .unwrap()
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .filter(|value| {
                value.get("continuationIntentKey").and_then(Value::as_str)
                    == Some(committed.intent_key.as_str())
            })
            .count();
            assert_eq!(child_count, 1);
            let _ = fs::remove_dir_all(root);
        }
    }

    #[test]
    fn origin_v1_task_new_human_supersedes_without_outbox_or_child() {
        let root = test_root("origin-v1-task-new-human");
        let committed = prepare_task_intent(&root, "queue-task-parent", "dm-task-human", "user-a");
        strip_origin_v1_additive_fields(&root, &committed.intent_key);
        append_same_exact_lane_human(&root, "queue-task-parent", "queue-human");

        reconcile_goal_continuation_intents(&root, 30).unwrap();
        reconcile_goal_continuation_intents(&root, 31).unwrap();

        assert!(
            find_child_queue_for_intent(&root, &committed.intent_key)
                .unwrap()
                .is_none()
        );
        let receipts = continuation_settlement_receipts(&root, &committed.intent_key);
        assert_eq!(receipts.len(), 1);
        assert_eq!(
            receipts[0]
                .get("sourceFinalExpectation")
                .and_then(Value::as_str),
            Some("explicit-non-delivery")
        );
        assert_eq!(
            receipts[0]
                .get("finalOutboxDisposition")
                .and_then(Value::as_str),
            Some("explicit-non-delivery")
        );
        assert!(source_outbox_messages(&root, &committed.parent_queue_id).is_empty());
        assert_human_queue_is_runnable(&root, "queue-human", &committed.working_session_key);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn origin_v1_task_snapshot_mismatch_parks_visibly_without_child() {
        let root = test_root("origin-v1-task-snapshot-mismatch");
        let committed =
            prepare_task_intent(&root, "queue-task-parent", "dm-task-mismatch", "user-a");
        strip_origin_v1_additive_fields(&root, &committed.intent_key);
        crate::operation_plan::update_operation_plan_item(
            crate::operation_plan::OperationPlanUpdateItemOptions {
                harness_home: root.clone(),
                plan_id: committed.authority_id.clone().unwrap(),
                item_id: committed.active_item_id.clone().unwrap(),
                expected_item_version: committed.active_item_version,
                status: None,
                title: None,
                body: Some("authority changed after the continuation commit".to_string()),
                depends_on: None,
                assignee: None,
                worker_job_id: None,
                queue_id: None,
                risk: None,
                evidence: None,
                replace_evidence: false,
                add_evidence: Vec::new(),
                now_ms: 25,
            },
        )
        .unwrap();

        reconcile_goal_continuation_intents(&root, 30).unwrap();
        reconcile_goal_continuation_intents(&root, 31).unwrap();

        assert!(
            find_child_queue_for_intent(&root, &committed.intent_key)
                .unwrap()
                .is_none()
        );
        assert_visible_settlement_once(&root, &committed, "suppressed", "parked-policy-denied");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn current_goal_terminal_or_supersession_before_enqueue_suppresses_and_settles_once() {
        for terminal in [true, false] {
            let root = test_root(if terminal {
                "goal-terminal-before-enqueue"
            } else {
                "goal-superseded-before-enqueue"
            });
            let committed = prepare_goal_intent(&root, "queue-parent", "dm-current-goal", "user-a");
            if terminal {
                append_terminal_goal_projection(&root, &committed);
            } else {
                append_superseding_goal_lineage(&root, &committed);
            }

            reconcile_goal_continuation_intents(&root, 30).unwrap();
            reconcile_goal_continuation_intents(&root, 31).unwrap();

            assert!(
                find_child_queue_for_intent(&root, &committed.intent_key)
                    .unwrap()
                    .is_none()
            );
            assert_visible_settlement_once(&root, &committed, "suppressed", "parked-policy-denied");
            append_same_exact_lane_human(&root, "queue-parent", "queue-human");
            assert_human_queue_is_runnable(&root, "queue-human", &committed.working_session_key);
            let _ = fs::remove_dir_all(root);
        }
    }

    #[test]
    fn current_goal_terminal_or_supersession_after_enqueue_suppresses_before_execution() {
        for terminal in [true, false] {
            let root = test_root(if terminal {
                "goal-terminal-before-lease-gate"
            } else {
                "goal-superseded-before-lease-gate"
            });
            let committed =
                prepare_goal_intent(&root, "queue-parent", "dm-current-goal-lease", "user-a");
            let enqueued =
                ensure_goal_continuation_enqueued(&root, &committed.intent_key, 20).unwrap();
            let item = continuation_gate_item(&root, &enqueued);
            if terminal {
                append_terminal_goal_projection(&root, &committed);
            } else {
                append_superseding_goal_lineage(&root, &committed);
            }

            assert!(matches!(
                gate_goal_continuation_after_lease(&root, &item, 30).unwrap(),
                GoalContinuationLeaseGate::Suppressed(_)
            ));
            assert!(matches!(
                gate_goal_continuation_after_lease(&root, &item, 31).unwrap(),
                GoalContinuationLeaseGate::Suppressed(_)
            ));
            assert!(matches!(
                crate::runtime_worker::resolve_queue_terminal_control(
                    &root,
                    &item.queue_id,
                    Some(&item.session_key),
                )
                .unwrap(),
                crate::QueueTerminalControl::Terminal(_)
            ));
            let recovered = latest_recoverable_intent_for_key(&root, &committed.intent_key)
                .unwrap()
                .latest
                .unwrap();
            assert_ne!(recovered.status, GoalContinuationIntentStatus::Acknowledged);
            assert_visible_settlement_once(&root, &committed, "suppressed", "parked-policy-denied");
            let _ = fs::remove_dir_all(root);
        }
    }

    #[test]
    fn descendant_goal_parent_requires_acknowledged_exact_intent_chain() {
        let root = test_root("goal-descendant-parent-exact-chain");
        let committed = prepare_goal_intent(&root, "queue-parent", "dm-descendant-chain", "user-a");
        let enqueued = ensure_goal_continuation_enqueued(&root, &committed.intent_key, 20).unwrap();
        let child_queue_id = enqueued.child_queue_id.clone().unwrap();
        let mut descendant = enqueued.clone();
        descendant.intent_key = "descendant-intent".to_string();
        descendant.status = GoalContinuationIntentStatus::Committed;
        descendant.parent_queue_id = child_queue_id.clone();
        descendant.child_queue_id = None;
        descendant.source_slice_generation = enqueued.campaign_slice_generation;
        descendant.campaign_slice_generation = descendant.source_slice_generation.saturating_add(1);
        descendant.authority_version = Some(descendant.source_slice_generation);

        assert!(matches!(
            revalidate_current_goal_authority(&root, &descendant),
            Err(IntentRevalidationFailure::PolicyBlocked(_))
        ));

        let child = continuation_gate_item(&root, &enqueued);
        assert!(matches!(
            gate_goal_continuation_after_lease(&root, &child, 21).unwrap(),
            GoalContinuationLeaseGate::Acknowledged(_)
        ));
        revalidate_current_goal_authority(&root, &descendant).unwrap();

        let pending_file = root
            .join("state")
            .join("runtime-queue")
            .join("pending.jsonl");
        let mut forged_row = fs::read_to_string(&pending_file)
            .unwrap()
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .find(|value| {
                value.get("queueId").and_then(Value::as_str) == Some(child_queue_id.as_str())
            })
            .unwrap();
        let forged_queue_id = "queue-forged-same-lane";
        forged_row["queueId"] = Value::String(forged_queue_id.to_string());
        append_jsonl_value(&pending_file, &forged_row).unwrap();
        let mut forged = descendant;
        forged.parent_queue_id = forged_queue_id.to_string();
        assert!(matches!(
            revalidate_current_goal_authority(&root, &forged),
            Err(IntentRevalidationFailure::PolicyBlocked(_))
        ));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn same_key_malformed_after_committed_or_enqueued_preserves_identity_and_settles_once() {
        for initially_enqueued in [false, true] {
            let root = test_root(if initially_enqueued {
                "same-key-malformed-after-enqueued"
            } else {
                "same-key-malformed-after-committed"
            });
            let committed =
                prepare_goal_intent(&root, "queue-parent", "dm-ledger-corrupt", "user-a");
            let latest = if initially_enqueued {
                ensure_goal_continuation_enqueued(&root, &committed.intent_key, 20).unwrap()
            } else {
                committed.clone()
            };
            let mut file = OpenOptions::new()
                .append(true)
                .open(goal_continuation_intents_file(&root))
                .unwrap();
            writeln!(
                file,
                "{{\"schema\":\"{}\",\"intentKey\":\"{}\",\"status\":",
                GOAL_CONTINUATION_INTENT_SCHEMA, committed.intent_key
            )
            .unwrap();

            reconcile_goal_continuation_intents(&root, 30).unwrap();
            reconcile_goal_continuation_intents(&root, 31).unwrap();

            assert_visible_settlement_once(
                &root,
                &committed,
                "failed-terminal",
                "suppressed-exact-owner-mismatch",
            );
            if let Some(child_queue_id) = latest.child_queue_id.as_deref() {
                assert!(matches!(
                    crate::runtime_worker::resolve_queue_terminal_control(
                        &root,
                        child_queue_id,
                        None,
                    )
                    .unwrap(),
                    crate::QueueTerminalControl::Terminal(_)
                ));
            }
            append_same_exact_lane_human(&root, "queue-parent", "queue-human");
            assert_human_queue_is_runnable(&root, "queue-human", &committed.working_session_key);
            let _ = fs::remove_dir_all(root);
        }
    }

    #[test]
    fn same_key_supported_schema_collision_preserves_last_valid_owner_and_settles_once() {
        let root = test_root("same-key-supported-schema-collision");
        let committed = prepare_goal_intent(&root, "queue-parent", "dm-ledger-collision", "user-a");
        let mut collision = committed.clone();
        collision.parent_queue_id = "queue-unrelated".to_string();
        collision.reason = "same key with a different claimed owner".to_string();
        collision.observed_at_ms = 20;
        append_jsonl_value(&goal_continuation_intents_file(&root), &collision).unwrap();

        reconcile_goal_continuation_intents(&root, 30).unwrap();
        reconcile_goal_continuation_intents(&root, 31).unwrap();

        assert_visible_settlement_once(
            &root,
            &committed,
            "failed-terminal",
            "suppressed-exact-owner-mismatch",
        );
        let fence = latest_goal_continuation_fence(&root, &committed.intent_key)
            .unwrap()
            .unwrap();
        assert_eq!(fence.parent_queue_id, committed.parent_queue_id);
        assert_eq!(fence.lane_digest, committed.lane_digest);
        assert_eq!(fence.virtual_session_id, committed.virtual_session_id);
        assert!(
            find_child_queue_for_intent(&root, &committed.intent_key)
                .unwrap()
                .is_none()
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn downgrade_readiness_blocks_open_new_semantics_and_fence_overlay() {
        let root = test_root("goal-continuation-downgrade-readiness");
        let committed = prepare_goal_intent(&root, "queue-parent", "dm-downgrade", "user-a");
        let before = assess_goal_continuation_downgrade_readiness(&root).unwrap();
        assert!(!before.safe_for_legacy_binary);
        assert_eq!(
            before.open_new_semantics_intent_keys,
            vec![committed.intent_key.clone()]
        );

        write_goal_autonomy_config(&root, &[]);
        reconcile_goal_continuation_intents(&root, 30).unwrap();
        let after = assess_goal_continuation_downgrade_readiness(&root).unwrap();
        assert!(!after.safe_for_legacy_binary);
        assert_eq!(
            after.fenced_open_intent_keys,
            vec![committed.intent_key.clone()]
        );
        assert!(
            after
                .blockers
                .iter()
                .any(|blocker| blocker.contains("fence overlay"))
        );
        let _ = fs::remove_dir_all(root);
    }

    fn strip_origin_v1_additive_fields(root: &Path, intent_key: &str) {
        let file = goal_continuation_intents_file(root);
        let rewritten = fs::read_to_string(&file)
            .unwrap()
            .lines()
            .map(|line| {
                let mut value: Value = serde_json::from_str(line).unwrap();
                if value.get("intentKey").and_then(Value::as_str) == Some(intent_key) {
                    let object = value.as_object_mut().unwrap();
                    object.remove("humanInputGeneration");
                    object.remove("goalRelationAtCommit");
                    object.remove("goalStatusAtCommit");
                }
                serde_json::to_string(&value).unwrap()
            })
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(file, format!("{rewritten}\n")).unwrap();
    }

    fn append_exact_transition_proof(root: &Path, intent: &GoalContinuationIntentV1) {
        append_jsonl_value(
            &goal_transition_receipts_file(root),
            &GoalTransitionReceiptV1 {
                schema: crate::goal_transition::GOAL_TRANSITION_SCHEMA.to_string(),
                slice_schema: "agent-harness.goal-slice.v1".to_string(),
                queue_id: Some(intent.parent_queue_id.clone()),
                event: GoalTransitionEventKind::NormalCompletion,
                runtime_status: "completed".to_string(),
                runtime_reason: "origin-v1 exact transition proof".to_string(),
                goal_status: Some("active".to_string()),
                lineage_id: Some(intent.lineage_id.clone()),
                campaign_family_id: Some(intent.campaign_family_id.clone()),
                lane_digest: Some(intent.lane_digest.clone()),
                virtual_session_id: Some(intent.virtual_session_id.clone()),
                backend_context_generation: Some(intent.backend_context_generation.clone()),
                source_thread_id: Some(format!("thread-{}", intent.parent_queue_id)),
                source_turn_id: Some(format!("turn-{}", intent.parent_queue_id)),
                backend_goal_ref: Some(format!("goal-{}", intent.parent_queue_id)),
                correlation_source:
                    crate::goal_transition::GoalTransitionCorrelationSourceV1::BackendGoal,
                goal_checksum: Some(intent.goal_checksum.clone()),
                source_slice_generation: intent.source_slice_generation,
                decision_generation: intent.decision_generation,
                authority: GoalTransitionAuthority::Ready,
                relation: GoalTransitionRelation::CurrentGoalSlice,
                decision: intent.decision,
                surface: GoalTransitionSurface::ProgressOnly,
                schedule_continuation: true,
                allow_campaign_final: false,
                terminal: false,
                reason: "continue".to_string(),
                observed_at_ms: intent.observed_at_ms.saturating_sub(1),
            },
        )
        .unwrap();
    }

    fn append_terminal_goal_projection(root: &Path, intent: &GoalContinuationIntentV1) {
        append_jsonl_value(
            &root
                .join("state")
                .join("runtime-queue")
                .join("codex-goal-projection-receipts.jsonl"),
            &serde_json::json!({
                "schema": "agent-harness.codex-goal-projection.v1",
                "queueId": intent.parent_queue_id,
                "sessionKey": intent.working_session_key,
                "sourceThreadId": format!("thread-{}", intent.parent_queue_id),
                "sourceTurnId": format!("terminal-turn-{}", intent.parent_queue_id),
                "goalReference": format!("goal-{}", intent.parent_queue_id),
                "backendGoalRef": format!("goal-{}", intent.parent_queue_id),
                "laneDigest": intent.lane_digest,
                "backendContextGeneration": intent.backend_context_generation,
                "objective": format!("finish {}", intent.parent_queue_id),
                "status": "completed",
                "observationPhase": "governed-closure",
                "turnRelation": "authoritative-goal-closure",
                "sourceFinalEligible": true,
                "goalChecksum": intent.goal_checksum,
                "projectionChecksum": format!("terminal-projection-{}", intent.parent_queue_id),
                "projectionComplete": true,
                "observationOrder": 2,
                "observedAtMs": 20
            }),
        )
        .unwrap();
    }

    fn append_superseding_goal_lineage(root: &Path, intent: &GoalContinuationIntentV1) -> String {
        append_jsonl_value(
            &root
                .join("state")
                .join("runtime-queue")
                .join("codex-goal-projection-receipts.jsonl"),
            &serde_json::json!({
                "schema": "agent-harness.codex-goal-projection.v1",
                "queueId": intent.parent_queue_id,
                "sessionKey": intent.working_session_key,
                "sourceThreadId": format!("thread-{}", intent.parent_queue_id),
                "sourceTurnId": format!("superseding-turn-{}", intent.parent_queue_id),
                "goalReference": format!("replacement-goal-{}", intent.parent_queue_id),
                "backendGoalRef": format!("replacement-goal-{}", intent.parent_queue_id),
                "laneDigest": intent.lane_digest,
                "backendContextGeneration": intent.backend_context_generation,
                "objective": format!("finish {}", intent.parent_queue_id),
                "status": "active",
                "observationPhase": "owned-turn",
                "turnRelation": "current-owned-turn",
                "sourceFinalEligible": true,
                "goalChecksum": format!("replacement-checksum-{}", intent.parent_queue_id),
                "projectionChecksum": format!("replacement-projection-{}", intent.parent_queue_id),
                "projectionComplete": true,
                "observationOrder": 2,
                "observedAtMs": 20
            }),
        )
        .unwrap();
        let report = run_goal_lineage_doctor(GoalLineageDoctorOptions {
            harness_home: root.to_path_buf(),
            lane_digest: Some(intent.lane_digest.clone()),
            virtual_session_id: Some(intent.virtual_session_id.clone()),
        })
        .unwrap();
        let winner = report
            .lineages
            .iter()
            .find(|lineage| {
                lineage.lineage_id != intent.lineage_id
                    && lineage.goal_reference
                        == format!("replacement-goal-{}", intent.parent_queue_id)
            })
            .unwrap()
            .lineage_id
            .clone();
        crate::goal_lineage::append_goal_lineage_supersession(
            crate::goal_lineage::GoalLineageSupersessionOptions {
                harness_home: root.to_path_buf(),
                winner_lineage_id: winner.clone(),
                superseded_lineage_id: intent.lineage_id.clone(),
                reason: "new canonical Goal lineage supersedes the committed continuation"
                    .to_string(),
            },
        )
        .unwrap();
        winner
    }

    fn continuation_settlement_receipts(
        root: &Path,
        intent_key: &str,
    ) -> Vec<serde_json::Map<String, Value>> {
        fs::read_to_string(
            root.join("state")
                .join("runtime-queue")
                .join("run-once-receipts.jsonl"),
        )
        .unwrap()
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|value| {
            value.get("continuationIntentKey").and_then(Value::as_str) == Some(intent_key)
                && value.get("continuationSettlementKey").is_some()
        })
        .filter_map(|value| value.as_object().cloned())
        .collect()
    }

    fn source_outbox_messages(root: &Path, source_queue_id: &str) -> Vec<Value> {
        fs::read_to_string(root.join("state").join("channels").join("outbox.jsonl"))
            .unwrap_or_default()
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .filter(|value| {
                value.get("sourceQueueId").and_then(Value::as_str) == Some(source_queue_id)
            })
            .collect()
    }

    fn assert_visible_settlement_once(
        root: &Path,
        intent: &GoalContinuationIntentV1,
        expected_status: &str,
        expected_closure_kind: &str,
    ) {
        let receipts = continuation_settlement_receipts(root, &intent.intent_key);
        assert_eq!(receipts.len(), 1);
        let receipt = &receipts[0];
        assert_eq!(
            receipt.get("status").and_then(Value::as_str),
            Some(expected_status)
        );
        assert_eq!(
            receipt.get("sourceClosureKind").and_then(Value::as_str),
            Some(expected_closure_kind)
        );
        assert_eq!(
            receipt
                .get("sourceFinalExpectation")
                .and_then(Value::as_str),
            Some("required")
        );
        assert!(matches!(
            receipt
                .get("finalOutboxDisposition")
                .and_then(Value::as_str),
            Some("appended" | "already-present")
        ));
        assert_eq!(
            source_outbox_messages(root, &intent.parent_queue_id).len(),
            1
        );
    }

    fn assert_human_queue_is_runnable(root: &Path, queue_id: &str, session_key: &str) {
        assert!(matches!(
            crate::runtime_worker::resolve_queue_terminal_control(
                root,
                queue_id,
                Some(session_key),
            )
            .unwrap(),
            crate::QueueTerminalControl::Runnable
        ));
    }

    fn test_root(prefix: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!("{prefix}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        root
    }

    fn append_same_exact_lane_human(root: &Path, template_queue_id: &str, human_queue_id: &str) {
        let pending_file = root
            .join("state")
            .join("runtime-queue")
            .join("pending.jsonl");
        let mut human = fs::read_to_string(&pending_file)
            .unwrap()
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .find(|value| value.get("queueId").and_then(Value::as_str) == Some(template_queue_id))
            .unwrap();
        let object = human.as_object_mut().unwrap();
        object.insert(
            "queueId".to_string(),
            Value::String(human_queue_id.to_string()),
        );
        object.insert("status".to_string(), Value::String("queued".to_string()));
        object.insert("origin".to_string(), Value::String("channel".to_string()));
        object.insert(
            "messageText".to_string(),
            Value::String("newer operator input".to_string()),
        );
        object.insert(
            "createdAtMs".to_string(),
            Value::Number(serde_json::Number::from(1_000)),
        );
        object.insert(
            "continuationIndex".to_string(),
            Value::Number(serde_json::Number::from(0)),
        );
        for field in [
            "continuationIntentKey",
            "continuationParentQueueId",
            "campaignSliceGeneration",
        ] {
            object.remove(field);
        }
        append_jsonl_value(&pending_file, &human).unwrap();
    }

    fn continuation_gate_item(
        root: &Path,
        intent: &GoalContinuationIntentV1,
    ) -> RuntimeQueuePreparedItem {
        let mut continuation = RuntimeContinuationMetadata::legacy();
        continuation.continuation_intent_key = Some(intent.intent_key.clone());
        continuation.campaign_slice_generation = Some(intent.campaign_slice_generation);
        continuation.continuation_index = Some(intent.recovery_continuation_index);
        let execution_dir = root.join("execution-child");
        RuntimeQueuePreparedItem {
            queue_id: intent.child_queue_id.clone().unwrap(),
            admission_queue_id: None,
            agent_id: "main".to_string(),
            session_key: intent.working_session_key.clone(),
            runtime_class: "interactive".to_string(),
            origin: "channel".to_string(),
            cron_run_id: None,
            scheduled_for_ms: None,
            platform: "telegram".to_string(),
            account_id: Some("acct-a".to_string()),
            channel_id: "fixture-channel".to_string(),
            user_id: "fixture-user".to_string(),
            message_text: "bounded continuation".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            provider: None,
            model: None,
            reasoning_preference: None,
            backend_reasoning_policy: None,
            authorized_execution_mode: None,
            prompt_bundle_json: execution_dir.join("prompt-bundle.json"),
            prompt_markdown: execution_dir.join("prompt.md"),
            receipt_file: execution_dir.join("receipt.json"),
            planned_transcript_file: execution_dir.join("transcript.jsonl"),
            planned_trajectory_file: execution_dir.join("trajectory.jsonl"),
            execution_dir,
            selected_skill_ids: Vec::new(),
            virtual_skill_manifest_file: None,
            skill_delivery_receipt_files: Vec::new(),
            continuation,
        }
    }

    fn prepare_goal_intent(
        root: &Path,
        queue_id: &str,
        channel_id: &str,
        user_id: &str,
    ) -> GoalContinuationIntentV1 {
        prepare_goal_intent_with_metadata(
            root,
            queue_id,
            channel_id,
            user_id,
            0,
            RuntimeContinuationMetadata::legacy(),
        )
    }

    fn prepare_goal_intent_with_metadata(
        root: &Path,
        queue_id: &str,
        channel_id: &str,
        user_id: &str,
        parent_continuation_index: u64,
        continuation: RuntimeContinuationMetadata,
    ) -> GoalContinuationIntentV1 {
        let queue_dir = root.join("state").join("runtime-queue");
        fs::create_dir_all(&queue_dir).unwrap();
        let (source_home, source_workspace) = write_goal_test_source(root);
        let session_key = crate::channel_session_key::CanonicalChannelSessionKey::bind_for_lane(
            &format!("telegram:{channel_id}:{user_id}:main"),
            "telegram",
            "acct-a",
            channel_id,
            user_id,
            "main",
        )
        .unwrap()
        .canonical_string();
        append_jsonl_value(
            &queue_dir.join("pending.jsonl"),
            &serde_json::json!({
                "schema": "agent-harness.runtime-queue-item.v1",
                "queueId": queue_id,
                "status": "queued",
                "createdAtMs": 1,
                "runtimeClass": "interactive",
                "origin": "channel",
                "platform": "telegram",
                "accountId": "acct-a",
                "channelId": channel_id,
                "userId": user_id,
                "agentId": "main",
                "sessionKey": session_key,
                "continuationIndex": parent_continuation_index,
                "messageText": "continue",
                "source": {
                    "kind": "channel",
                    "sourceHome": source_home,
                    "sourceWorkspace": source_workspace
                },
                "plannedTranscriptFile": root.join("agents").join("main").join("sessions").join("fixture.jsonl"),
                "plannedTrajectoryFile": root.join("agents").join("main").join("sessions").join("fixture.trajectory.jsonl"),
                "selectedSkillIds": []
            }),
        )
        .unwrap();
        append_jsonl_value(
            &queue_dir.join("execution-receipts.jsonl"),
            &serde_json::json!({
                "schema": "agent-harness.runtime-execution-receipt.v1",
                "queueId": queue_id,
                "status": "prepared",
                "runtimeClass": "interactive",
                "origin": "channel",
                "executionDir": root.join("execution").display().to_string()
            }),
        )
        .unwrap();
        append_jsonl_value(
            &queue_dir.join("run-once-receipts.jsonl"),
            &serde_json::json!({
                "queueId": queue_id,
                "status": "completed",
                "runtimeClass": "interactive",
                "origin": "channel",
                "reason": "fixture parent completed before continuation scheduling"
            }),
        )
        .unwrap();
        let channel_lane =
            ChannelStateLane::new("telegram", Some("acct-a"), channel_id, user_id, "main").unwrap();
        let full_lane = FullLaneKeyV1::new(
            "telegram",
            "acct-a",
            channel_id,
            user_id,
            "main",
            "interactive",
            root_working_session_key(&session_key),
            &session_key,
        )
        .unwrap();
        let lane_digest = full_lane.identity_hash().unwrap();
        let virtual_session_id =
            derive_virtual_session_id_v2(&channel_lane, &root_working_session_key(&session_key));
        let lineage = write_current_goal_lineage_fixture(
            root,
            queue_id,
            &session_key,
            &lane_digest,
            &virtual_session_id,
        );
        let mut transition = transition();
        transition.queue_id = Some(queue_id.to_string());
        transition.lineage_id = Some(lineage.lineage_id);
        transition.campaign_family_id = Some(lineage.campaign_family_id);
        transition.goal_checksum = Some(lineage.goal_checksum);
        transition.lane_digest = Some(lane_digest);
        transition.virtual_session_id = Some(virtual_session_id);
        transition.backend_context_generation = lineage.backend_context_generation;
        transition.source_thread_id = Some(lineage.source_thread_id);
        transition.goal_status = Some(lineage.goal_status);
        transition.observed_at_ms = 2;
        commit_goal_continuation_intent(root, &transition, &session_key, &continuation, 10).unwrap()
    }

    fn prepare_task_intent(
        root: &Path,
        queue_id: &str,
        channel_id: &str,
        user_id: &str,
    ) -> GoalContinuationIntentV1 {
        let (source_home, source_workspace) = write_goal_test_source(root);
        let session_key = crate::channel_session_key::CanonicalChannelSessionKey::bind_for_lane(
            &format!("telegram:{channel_id}:{user_id}:main"),
            "telegram",
            "acct-a",
            channel_id,
            user_id,
            "main",
        )
        .unwrap()
        .canonical_string();
        let lane_digest = FullLaneKeyV1::new(
            "telegram",
            "acct-a",
            channel_id,
            user_id,
            "main",
            "interactive",
            root_working_session_key(&session_key),
            &session_key,
        )
        .unwrap()
        .identity_hash()
        .unwrap();
        let channel_lane =
            ChannelStateLane::new("telegram", Some("acct-a"), channel_id, user_id, "main").unwrap();
        let virtual_session_id =
            derive_virtual_session_id_v2(&channel_lane, &root_working_session_key(&session_key));
        let plan_id = format!("plan-{queue_id}");
        crate::operation_plan::create_operation_plan_v2(
            crate::operation_plan::CreateOperationPlanOptionsV2 {
                options: crate::operation_plan::CreateOperationPlanOptions {
                    harness_home: root.to_path_buf(),
                    plan_id: plan_id.clone(),
                    origin_queue_id: Some(queue_id.to_string()),
                    session_key: session_key.clone(),
                    agent_id: "main".to_string(),
                    goal: "finish the restart-safe task".to_string(),
                    acceptance_criteria: None,
                    constraints: None,
                    max_open_items: None,
                    max_fanout: None,
                    now_ms: 1,
                },
                lane_digest: lane_digest.clone(),
            },
        )
        .unwrap();
        crate::operation_plan::add_operation_plan_item(
            crate::operation_plan::OperationPlanAddItemOptions {
                harness_home: root.to_path_buf(),
                plan_id: plan_id.clone(),
                item_id: "item".to_string(),
                title: "continue task".to_string(),
                body: "task remains in progress".to_string(),
                depends_on: Vec::new(),
                acceptance_criteria: None,
                risk: None,
                now_ms: 2,
            },
        )
        .unwrap();
        let ready = crate::operation_plan::update_operation_plan_item(
            crate::operation_plan::OperationPlanUpdateItemOptions {
                harness_home: root.to_path_buf(),
                plan_id: plan_id.clone(),
                item_id: "item".to_string(),
                expected_item_version: Some(1),
                status: Some(crate::operation_plan::OperationPlanItemStatus::Ready),
                title: None,
                body: None,
                depends_on: None,
                assignee: None,
                worker_job_id: None,
                queue_id: None,
                risk: None,
                evidence: None,
                replace_evidence: false,
                add_evidence: Vec::new(),
                now_ms: 3,
            },
        )
        .unwrap();
        let running = crate::operation_plan::update_operation_plan_item(
            crate::operation_plan::OperationPlanUpdateItemOptions {
                harness_home: root.to_path_buf(),
                plan_id: plan_id.clone(),
                item_id: "item".to_string(),
                expected_item_version: Some(ready.item.version),
                status: Some(crate::operation_plan::OperationPlanItemStatus::Running),
                title: None,
                body: None,
                depends_on: None,
                assignee: None,
                worker_job_id: None,
                queue_id: None,
                risk: None,
                evidence: None,
                replace_evidence: false,
                add_evidence: Vec::new(),
                now_ms: 4,
            },
        )
        .unwrap();
        let report = crate::operation_plan::show_operation_plan(
            crate::operation_plan::OperationPlanShowOptions {
                harness_home: root.to_path_buf(),
                plan_id: plan_id.clone(),
            },
        )
        .unwrap();
        let queue_dir = root.join("state").join("runtime-queue");
        fs::create_dir_all(&queue_dir).unwrap();
        append_jsonl_value(
            &queue_dir.join("pending.jsonl"),
            &serde_json::json!({
                "schema": "agent-harness.runtime-queue-item.v1",
                "queueId": queue_id,
                "status": "queued",
                "createdAtMs": 1,
                "runtimeClass": "interactive",
                "origin": "channel",
                "platform": "telegram",
                "accountId": "acct-a",
                "channelId": channel_id,
                "userId": user_id,
                "agentId": "main",
                "sessionKey": session_key,
                "continuationIndex": 0,
                "messageText": "continue the active task",
                "source": {
                    "kind": "channel",
                    "sourceHome": source_home,
                    "sourceWorkspace": source_workspace
                },
                "plannedTranscriptFile": root.join("agents").join("main").join("sessions").join("task-fixture.jsonl"),
                "plannedTrajectoryFile": root.join("agents").join("main").join("sessions").join("task-fixture.trajectory.jsonl"),
                "selectedSkillIds": []
            }),
        )
        .unwrap();
        append_jsonl_value(
            &queue_dir.join("execution-receipts.jsonl"),
            &serde_json::json!({
                "schema": "agent-harness.runtime-execution-receipt.v1",
                "queueId": queue_id,
                "status": "prepared",
                "runtimeClass": "interactive",
                "origin": "channel",
                "executionDir": root.join("execution").display().to_string()
            }),
        )
        .unwrap();
        append_jsonl_value(
            &queue_dir.join("run-once-receipts.jsonl"),
            &serde_json::json!({
                "queueId": queue_id,
                "status": "completed",
                "runtimeClass": "interactive",
                "origin": "channel",
                "reason": "fixture parent completed before task continuation scheduling"
            }),
        )
        .unwrap();
        let checkpoint_text = "checkpoint: task remains active".to_string();
        let checkpoint = crate::TaskContinuationCheckpointV1 {
            schema: crate::task_transition::TASK_CONTINUATION_CHECKPOINT_SCHEMA.to_string(),
            authority_kind: crate::ContinuationAuthorityKindV1::OperationPlan,
            authority_id: plan_id,
            authority_version: report.plan.version,
            active_item_id: Some(running.item.item_id.clone()),
            active_item_version: Some(running.item.version),
            checkpoint_digest: crate::task_transition::sha256_hex(checkpoint_text.as_bytes()),
            checkpoint: checkpoint_text,
        };
        let evaluation = crate::task_transition::evaluate_operation_plan_drain(
            crate::task_transition::OperationPlanAuthorityOptions {
                harness_home: root.to_path_buf(),
                checkpoint,
                exact_lane_digest: lane_digest,
                virtual_session_id,
                working_session_key: session_key.clone(),
                agent_id: "main".to_string(),
                breakers: crate::TaskContinuationBreakers::default(),
            },
        );
        let crate::DrainDispositionV1::ContinuationRequired(authority) = evaluation.disposition
        else {
            panic!("fixture must produce exact operation-plan continuation authority");
        };
        commit_task_continuation_intent(
            root,
            queue_id,
            &session_key,
            &RuntimeContinuationMetadata {
                continuation_index: Some(3),
                campaign_slice_generation: Some(4),
                ..RuntimeContinuationMetadata::legacy()
            },
            &authority,
            3,
            4,
            false,
            10,
        )
        .unwrap()
    }

    fn write_current_goal_lineage_fixture(
        root: &Path,
        queue_id: &str,
        session_key: &str,
        lane_digest: &str,
        virtual_session_id: &str,
    ) -> crate::goal_lineage::GoalLineageV1 {
        let authority_file = root
            .join("state")
            .join("context-rollover")
            .join("virtual-session-authority-receipts.jsonl");
        append_jsonl_value(
            &authority_file,
            &serde_json::json!({
                "schema": "agent-harness.virtual-session-authority.v1",
                "queueId": queue_id,
                "virtualSessionId": virtual_session_id,
                "workingSessionKey": session_key,
                "laneDigest": lane_digest,
                "backendContextGeneration": "generation-a",
                "status": "authoritative-v2",
                "updatedAtMs": 1
            }),
        )
        .unwrap();
        let projection_file = root
            .join("state")
            .join("runtime-queue")
            .join("codex-goal-projection-receipts.jsonl");
        append_jsonl_value(
            &projection_file,
            &serde_json::json!({
                "schema": "agent-harness.codex-goal-projection.v1",
                "queueId": queue_id,
                "sessionKey": session_key,
                "sourceThreadId": format!("thread-{queue_id}"),
                "sourceTurnId": format!("turn-{queue_id}"),
                "goalReference": format!("goal-{queue_id}"),
                "backendGoalRef": format!("goal-{queue_id}"),
                "laneDigest": lane_digest,
                "backendContextGeneration": "generation-a",
                "objective": format!("finish {queue_id}"),
                "status": "active",
                "observationPhase": "owned-turn",
                "turnRelation": "current-owned-turn",
                "sourceFinalEligible": true,
                "goalChecksum": format!("checksum-{queue_id}"),
                "projectionChecksum": format!("projection-{queue_id}"),
                "projectionComplete": true,
                "observationOrder": 1,
                "observedAtMs": 1
            }),
        )
        .unwrap();
        let report = run_goal_lineage_doctor(GoalLineageDoctorOptions {
            harness_home: root.to_path_buf(),
            lane_digest: Some(lane_digest.to_string()),
            virtual_session_id: Some(virtual_session_id.to_string()),
        })
        .unwrap();
        assert_eq!(report.status, GoalLineageDoctorStatus::Ready);
        report
            .lineages
            .into_iter()
            .find(|lineage| lineage.queue_id.as_deref() == Some(queue_id))
            .unwrap()
    }

    fn write_goal_test_source(root: &Path) -> (PathBuf, PathBuf) {
        let source_home = root.join("source-home");
        let source_workspace = source_home.join("workspace");
        fs::create_dir_all(&source_workspace).unwrap();
        fs::create_dir_all(source_home.join("agents").join("main").join("sessions")).unwrap();
        fs::write(source_workspace.join("AGENTS.md"), "# Goal fixture").unwrap();
        fs::write(
            source_home.join("openclaw.json"),
            r#"{
              "agents": {
                "defaults": { "provider": "openai", "model": "codex" },
                "list": [{ "id": "main", "model": "gpt-5", "enabled": true }]
              },
              "models": {
                "providers": { "openai": { "apiKey": "${OPENAI_API_KEY}" } }
              }
            }"#,
        )
        .unwrap();
        fs::write(
            source_home
                .join("agents")
                .join("main")
                .join("sessions")
                .join("sessions.json"),
            "{}",
        )
        .unwrap();
        (source_home, source_workspace)
    }

    fn write_goal_autonomy_config(root: &Path, active_lane_digests: &[&str]) {
        fs::create_dir_all(root).unwrap();
        fs::write(
            root.join("harness-config.json"),
            serde_json::to_vec(&serde_json::json!({
                "goalAutonomy": {
                    "mode": "active",
                    "activeLaneDigests": active_lane_digests
                }
            }))
            .unwrap(),
        )
        .unwrap();
    }

    fn transition() -> GoalTransitionReceiptV1 {
        GoalTransitionReceiptV1 {
            schema: "agent-harness.goal-transition.v1".to_string(),
            slice_schema: "agent-harness.goal-slice.v1".to_string(),
            queue_id: Some("queue-a".to_string()),
            event: GoalTransitionEventKind::NormalCompletion,
            runtime_status: "completed".to_string(),
            runtime_reason: "slice completed".to_string(),
            goal_status: Some("active".to_string()),
            lineage_id: Some("lineage-a".to_string()),
            campaign_family_id: Some("campaign-a".to_string()),
            lane_digest: Some("lane-a".to_string()),
            virtual_session_id: Some("vsession-a".to_string()),
            backend_context_generation: Some("generation-a".to_string()),
            source_thread_id: Some("thread-a".to_string()),
            source_turn_id: Some("turn-a".to_string()),
            backend_goal_ref: Some("goal-a".to_string()),
            correlation_source:
                crate::goal_transition::GoalTransitionCorrelationSourceV1::BackendGoal,
            goal_checksum: Some("checksum-a".to_string()),
            source_slice_generation: 3,
            decision_generation: 4,
            authority: GoalTransitionAuthority::Ready,
            relation: crate::goal_transition::GoalTransitionRelation::CurrentGoalSlice,
            decision: GoalTransitionDecision::Continue,
            surface: GoalTransitionSurface::ProgressOnly,
            schedule_continuation: true,
            allow_campaign_final: false,
            terminal: false,
            reason: "continue".to_string(),
            observed_at_ms: 1,
        }
    }
}
