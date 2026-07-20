use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use ring::digest;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::append_jsonl_value;
use crate::channel_state::ChannelStateLane;
use crate::config::harness_config_candidates;
use crate::context_rollover::{
    ContextRolloverRequeuePreparedOptions, RuntimeContinuationMetadata,
    derive_virtual_session_id_v2, requeue_prepared_context_rollover, root_working_session_key,
};
use crate::goal_transition::{
    GoalTransitionAuthority, GoalTransitionDecision, GoalTransitionReceiptV1,
};
use crate::lane::FullLaneKeyV1;
use crate::runtime_worker::RuntimeQueuePreparedItem;

pub const GOAL_CONTINUATION_INTENT_SCHEMA: &str = "agent-harness.goal-continuation-intent.v1";
pub const TASK_CONTINUATION_INTENT_SCHEMA: &str = "agent-harness.task-continuation-intent.v1";

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
    pub reason: String,
    pub observed_at_ms: i64,
}

pub fn goal_continuation_intents_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("goal-lineage")
        .join("continuation-intents.jsonl")
}

pub fn load_goal_autonomy_activation(
    harness_home: impl AsRef<Path>,
    lane_digest: Option<&str>,
) -> io::Result<GoalAutonomyActivation> {
    let mut configured_mode = GoalAutonomyMode::Observe;
    let mut active_lane_digests = Vec::new();
    for candidate in harness_config_candidates(harness_home.as_ref()) {
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
    if !transition.schedule_continuation || transition.authority != GoalTransitionAuthority::Ready {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "goal continuation intent requires a scheduled transition with ready authority",
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
        authority_kind: Some(crate::ContinuationAuthorityKindV1::Goal),
        authority_id: Some(lineage_id.to_string()),
        authority_version: Some(source_slice_generation),
        authority_checksum: Some(goal_checksum.to_string()),
        checkpoint_digest: None,
        active_item_id: None,
        active_item_version: None,
        disposition_recovery_depth: None,
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
        authority_kind: Some(authority.kind),
        authority_id: Some(authority.authority_id.clone()),
        authority_version: Some(authority.authority_version),
        authority_checksum: Some(authority.authority_checksum.clone()),
        checkpoint_digest: Some(authority.checkpoint_digest.clone()),
        active_item_id: authority.active_item_id.clone(),
        active_item_version: authority.active_item_version,
        disposition_recovery_depth: disposition_recovery.then_some(1),
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
    let latest = latest_goal_continuation_intent(harness_home, intent_key)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "goal continuation intent is missing",
        )
    })?;
    if latest.status == GoalContinuationIntentStatus::Acknowledged {
        return Ok(latest);
    }
    if let Some(child_queue_id) = find_child_queue_for_intent(harness_home, intent_key)? {
        if latest.child_queue_id.as_deref() == Some(child_queue_id.as_str())
            && latest.status == GoalContinuationIntentStatus::Enqueued
        {
            return Ok(latest);
        }
        return append_intent_state(
            harness_home,
            latest,
            GoalContinuationIntentStatus::Enqueued,
            Some(child_queue_id),
            "reconciled an existing deterministic child after commit/enqueue interruption",
            now_ms,
        );
    }
    validate_parent_lane_for_intent(harness_home, &latest)?;
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
}

pub fn reconcile_goal_continuation_intents(
    harness_home: impl AsRef<Path>,
    now_ms: i64,
) -> io::Result<Vec<GoalContinuationIntentV1>> {
    let harness_home = harness_home.as_ref();
    let latest = latest_intents(harness_home)?;
    let mut reconciled = Vec::new();
    for intent in latest.into_values() {
        if intent.status == GoalContinuationIntentStatus::Committed {
            reconciled.push(ensure_goal_continuation_enqueued(
                harness_home,
                &intent.intent_key,
                now_ms,
            )?);
        }
    }
    Ok(reconciled)
}

pub fn acknowledge_goal_continuation_after_lease(
    harness_home: impl AsRef<Path>,
    item: &RuntimeQueuePreparedItem,
    now_ms: i64,
) -> io::Result<Option<GoalContinuationIntentV1>> {
    let Some(intent_key) = item.continuation.continuation_intent_key.as_deref() else {
        return Ok(None);
    };
    let harness_home = harness_home.as_ref();
    let latest = latest_goal_continuation_intent(harness_home, intent_key)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "prepared goal child has no committed continuation intent",
        )
    })?;
    if latest.child_queue_id.as_deref() != Some(item.queue_id.as_str()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "prepared goal child does not match the intent-owned queue id",
        ));
    }
    if latest.status == GoalContinuationIntentStatus::Acknowledged {
        return Ok(Some(latest));
    }
    let acknowledged = append_intent_state(
        harness_home,
        latest,
        GoalContinuationIntentStatus::Acknowledged,
        Some(item.queue_id.clone()),
        "child acquired the runtime lane lease and acknowledged the continuation intent",
        now_ms,
    )?;
    Ok(Some(acknowledged))
}

pub fn latest_goal_continuation_intent(
    harness_home: impl AsRef<Path>,
    intent_key: &str,
) -> io::Result<Option<GoalContinuationIntentV1>> {
    Ok(latest_intents(harness_home.as_ref())?.remove(intent_key))
}

fn latest_intents(harness_home: &Path) -> io::Result<BTreeMap<String, GoalContinuationIntentV1>> {
    let text = match fs::read_to_string(goal_continuation_intents_file(harness_home)) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(error) => return Err(error),
    };
    let mut latest = BTreeMap::new();
    for (index, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let receipt: GoalContinuationIntentV1 = serde_json::from_str(line).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "invalid goal continuation intent at line {}: {error}",
                    index + 1
                ),
            )
        })?;
        if matches!(
            receipt.schema.as_str(),
            GOAL_CONTINUATION_INTENT_SCHEMA | TASK_CONTINUATION_INTENT_SCHEMA
        ) {
            latest.insert(receipt.intent_key.clone(), receipt);
        }
    }
    Ok(latest)
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

fn validate_parent_lane_for_intent(
    harness_home: &Path,
    intent: &GoalContinuationIntentV1,
) -> io::Result<()> {
    let file = harness_home
        .join("state")
        .join("runtime-queue")
        .join("pending.jsonl");
    let text = fs::read_to_string(file)?;
    let pending = text
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find(|value| {
            value.get("queueId").and_then(Value::as_str) == Some(intent.parent_queue_id.as_str())
        })
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "goal continuation parent queue item is missing",
            )
        })?;
    let field = |name: &str| {
        pending
            .get(name)
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("goal continuation parent has no exact {name}"),
                )
            })
    };
    let session_key = field("sessionKey")?;
    if session_key != intent.working_session_key {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "goal continuation intent session does not match its exact parent",
        ));
    }
    let channel_lane = ChannelStateLane::new(
        field("platform")?,
        Some(field("accountId")?),
        field("channelId")?,
        field("userId")?,
        field("agentId")?,
    )?;
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
    .map_err(io::Error::other)?;
    let actual_lane_digest = full_lane.identity_hash().map_err(io::Error::other)?;
    let actual_virtual_session_id =
        derive_virtual_session_id_v2(&channel_lane, &root_working_session_key(session_key));
    if actual_lane_digest != intent.lane_digest
        || actual_virtual_session_id != intent.virtual_session_id
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "goal continuation intent does not match the exact parent lane/virtual session",
        ));
    }
    if intent.schema == TASK_CONTINUATION_INTENT_SCHEMA {
        crate::task_transition::revalidate_continuation_snapshot(
            harness_home,
            &crate::ContinuationAuthoritySnapshotV1 {
                kind: intent.authority_kind.ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "task intent has no authority kind",
                    )
                })?,
                authority_id: intent.authority_id.clone().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "task intent has no authority id",
                    )
                })?,
                authority_version: intent.authority_version.ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "task intent has no authority version",
                    )
                })?,
                authority_checksum: intent.authority_checksum.clone().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "task intent has no authority checksum",
                    )
                })?,
                exact_lane_digest: intent.lane_digest.clone(),
                virtual_session_id: intent.virtual_session_id.clone(),
                active_item_id: intent.active_item_id.clone(),
                active_item_version: intent.active_item_version,
                checkpoint_digest: intent.checkpoint_digest.clone().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "task intent has no checkpoint digest",
                    )
                })?,
            },
            session_key,
            channel_lane.agent_id(),
        )?;
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
    if existing.intent_key != candidate.intent_key
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
        || existing.recovery_continuation_index != candidate.recovery_continuation_index
        || existing.authority_kind != candidate.authority_kind
        || existing.authority_id != candidate.authority_id
        || existing.authority_version != candidate.authority_version
        || existing.authority_checksum != candidate.authority_checksum
        || existing.checkpoint_digest != candidate.checkpoint_digest
        || existing.active_item_id != candidate.active_item_id
        || existing.active_item_version != candidate.active_item_version
        || existing.disposition_recovery_depth != candidate.disposition_recovery_depth
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "deterministic goal continuation key collided with different immutable authority",
        ));
    }
    Ok(())
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
        let transition = transition();
        let continuation = RuntimeContinuationMetadata {
            continuation_index: Some(7),
            campaign_slice_generation: Some(3),
            ..RuntimeContinuationMetadata::legacy()
        };
        let first =
            commit_goal_continuation_intent(&root, &transition, "session-a", &continuation, 10)
                .unwrap();
        let replay =
            commit_goal_continuation_intent(&root, &transition, "session-a", &continuation, 11)
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
    fn committed_intent_reconciles_after_restart_without_duplicate_child() {
        let root =
            std::env::temp_dir().join(format!("goal-continuation-restart-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let queue_dir = root.join("state").join("runtime-queue");
        fs::create_dir_all(&queue_dir).unwrap();
        let session_key = crate::channel_session_key::CanonicalChannelSessionKey::bind_for_lane(
            "telegram:dm-a:user-a:main",
            "telegram",
            "acct-a",
            "dm-a",
            "user-a",
            "main",
        )
        .unwrap()
        .canonical_string();
        append_jsonl_value(
            &queue_dir.join("pending.jsonl"),
            &serde_json::json!({
                "queueId": "queue-a",
                "status": "queued",
                "runtimeClass": "interactive",
                "origin": "channel",
                "platform": "telegram",
                "accountId": "acct-a",
                "channelId": "dm-a",
                "userId": "user-a",
                "agentId": "main",
                "sessionKey": session_key,
                "continuationIndex": 5,
                "message": "continue"
            }),
        )
        .unwrap();
        append_jsonl_value(
            &queue_dir.join("execution-receipts.jsonl"),
            &serde_json::json!({
                "schema": "agent-harness.runtime-execution-receipt.v1",
                "queueId": "queue-a",
                "status": "prepared",
                "runtimeClass": "interactive",
                "origin": "channel",
                "executionDir": root.join("execution").display().to_string()
            }),
        )
        .unwrap();
        let channel_lane =
            ChannelStateLane::new("telegram", Some("acct-a"), "dm-a", "user-a", "main").unwrap();
        let full_lane = FullLaneKeyV1::new(
            "telegram",
            "acct-a",
            "dm-a",
            "user-a",
            "main",
            "interactive",
            crate::context_rollover::root_working_session_key(&session_key),
            &session_key,
        )
        .unwrap();
        let mut transition = transition();
        transition.lane_digest = Some(full_lane.identity_hash().unwrap());
        transition.virtual_session_id =
            Some(derive_virtual_session_id_v2(&channel_lane, &session_key));
        let committed = commit_goal_continuation_intent(
            &root,
            &transition,
            &session_key,
            &RuntimeContinuationMetadata {
                continuation_index: Some(5),
                campaign_slice_generation: Some(3),
                ..RuntimeContinuationMetadata::legacy()
            },
            10,
        )
        .unwrap();
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
            goal_checksum: Some("checksum-a".to_string()),
            source_slice_generation: 3,
            decision_generation: 4,
            authority: GoalTransitionAuthority::Ready,
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
