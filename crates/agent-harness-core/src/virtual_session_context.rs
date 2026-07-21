use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::channel_delivery_index::channel_delivery_states_for_source_queue_in_channel;
use crate::progress_event_index::{progress_event_index_file, progress_events_for_queue_ids};
use crate::runtime_execution_receipt_index::{
    execution_receipt_evidence_for_queue_ids, runtime_execution_receipt_index_file,
};
use crate::runtime_receipt_history::{
    find_runtime_queue_terminal_history, runtime_queue_receipt_history_file,
};
use crate::runtime_worker::{
    latest_runtime_queue_hot_receipts_from_index, refresh_runtime_queue_state_index,
    runtime_queue_state_index_file,
};
use crate::virtual_session_runtime_index::{
    VirtualSessionRuntimeLaneQuery, latest_runtime_interruption_evidence_for_queue_ids,
    recent_runtime_queue_ids_for_virtual_session_lane,
};

use crate::channel_state::{
    ChannelStateLane, migrate_legacy_channel_session_state_to_v2, read_channel_session_state,
    read_channel_session_state_v2,
};
use crate::context_rollover::{
    collect_active_operation_plan_refs, collect_active_operation_plan_refs_for_lane,
    continuation_index_from_session_key, derive_virtual_session_id, derive_virtual_session_id_v2,
    migrate_legacy_working_set_memory_for_session_for_lane, read_virtual_session_record_for_lane,
    read_working_set_memory_for_session, read_working_set_memory_for_session_for_lane,
    root_working_session_key, string_field, virtual_session_file,
};
use crate::lane::FullLaneKeyV1;

pub const VIRTUAL_SESSION_WORKING_CONTEXT_SCHEMA: &str =
    "agent-harness.virtual-session-working-context.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VirtualSessionContextQuery {
    pub harness_home: PathBuf,
    pub platform: String,
    pub account_id: Option<String>,
    pub channel_id: String,
    pub user_id: String,
    pub agent_id: String,
    pub session_key: Option<String>,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VirtualSessionWorkingContext {
    pub schema: String,
    pub lane: VirtualSessionLane,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lane_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub virtual_session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_session_key: Option<String>,
    pub continuation_index: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predecessor_session_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub working_set_file: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_interruption: Option<String>,
    pub recent_queue_ids: Vec<String>,
    pub evidence_anchors: VirtualSessionEvidenceAnchors,
    pub operation_plans: Vec<String>,
    pub scope_decision: VirtualSessionScopeDecision,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VirtualSessionLane {
    pub platform: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_id: Option<String>,
    pub channel_id: String,
    pub user_id: String,
    pub agent_id: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VirtualSessionEvidenceAnchors {
    pub run_once_receipts: Vec<VirtualSessionEvidenceAnchor>,
    pub execution_receipts: Vec<VirtualSessionEvidenceAnchor>,
    pub outbox_rows: Vec<VirtualSessionEvidenceAnchor>,
    pub delivery_receipts: Vec<VirtualSessionEvidenceAnchor>,
    pub progress_receipts: Vec<VirtualSessionEvidenceAnchor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VirtualSessionEvidenceAnchor {
    pub queue_id: String,
    pub status: String,
    pub file: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub at_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VirtualSessionScopeDecision {
    pub status: String,
    pub reason: String,
    pub fallback_used: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub denied_candidates: Vec<String>,
}

pub fn resolve_virtual_session_working_context(
    query: VirtualSessionContextQuery,
) -> io::Result<VirtualSessionWorkingContext> {
    let lane = VirtualSessionLane {
        platform: query.platform.clone(),
        account_id: query.account_id.clone(),
        channel_id: query.channel_id.clone(),
        user_id: query.user_id.clone(),
        agent_id: query.agent_id.clone(),
    };
    let channel_state = read_channel_session_state(
        &query.harness_home,
        &query.platform,
        &query.channel_id,
        &query.user_id,
    )?;
    let current_session_key = match query.session_key.clone() {
        Some(session_key) => Some(session_key),
        None => channel_state
            .as_ref()
            .map(|state| state.active_session_key.clone()),
    };

    if query.session_key.is_none()
        && let Some(state) = channel_state.as_ref()
    {
        let active_agent = state.agent_id.as_deref().unwrap_or("main");
        if active_agent != query.agent_id {
            return Ok(empty_envelope(
                lane,
                current_session_key,
                query.now_ms,
                VirtualSessionScopeDecision {
                    status: "denied".to_string(),
                    reason: format!(
                        "active channel state belongs to agent `{active_agent}`, not `{}`",
                        query.agent_id
                    ),
                    fallback_used: false,
                    denied_candidates: vec![state.active_session_key.clone()],
                },
            ));
        }
    }

    let Some(current_session_key) = current_session_key else {
        return Ok(empty_envelope(
            lane,
            None,
            query.now_ms,
            VirtualSessionScopeDecision {
                status: "no-active-session".to_string(),
                reason: "no active channel session was found for the exact lane".to_string(),
                fallback_used: false,
                denied_candidates: Vec::new(),
            },
        ));
    };

    let root_session_key = root_working_session_key(&current_session_key);
    let indexed = read_working_set_memory_for_session(&query.harness_home, &current_session_key)?
        .or_else(|| {
            read_working_set_memory_for_session(&query.harness_home, &root_session_key)
                .ok()
                .flatten()
        });
    let continuation_index = indexed
        .as_ref()
        .map(|(index, _)| index.continuation_index)
        .or_else(|| continuation_index_from_session_key(&current_session_key))
        .unwrap_or(0);
    let virtual_session_id = indexed
        .as_ref()
        .map(|(index, _)| index.virtual_session_id.clone())
        .unwrap_or_else(|| {
            derive_virtual_session_id(
                &query.platform,
                &query.channel_id,
                &query.user_id,
                &query.agent_id,
                &root_session_key,
            )
        });
    let working_set_file = indexed
        .as_ref()
        .map(|(index, _)| index.working_set_file.clone());
    let mut last_interruption = indexed.as_ref().and_then(|(_, memory)| {
        memory
            .decisions
            .iter()
            .rev()
            .find(|decision| decision.contains("interrupted long-task"))
            .cloned()
    });

    if let Some(record) = read_virtual_session_record(&query.harness_home, &virtual_session_id)? {
        if string_field(&record, &["platform"]) != Some(query.platform.as_str())
            || string_field(&record, &["channelId", "channel_id"])
                != Some(query.channel_id.as_str())
            || string_field(&record, &["userId", "user_id"]) != Some(query.user_id.as_str())
            || string_field(&record, &["agentId", "agent_id"]) != Some(query.agent_id.as_str())
        {
            return Ok(empty_envelope(
                lane,
                Some(current_session_key),
                query.now_ms,
                VirtualSessionScopeDecision {
                    status: "denied".to_string(),
                    reason: "virtual session record did not match the requested lane axes"
                        .to_string(),
                    fallback_used: false,
                    denied_candidates: vec![virtual_session_id],
                },
            ));
        }
    }

    let predecessor_session_key = indexed
        .as_ref()
        .and_then(|(_, memory)| memory.previous_working_session_key.clone())
        .or_else(|| predecessor_session_key(&root_session_key, continuation_index));
    let mut recent_queue_ids = Vec::new();
    if let Some((_, memory)) = indexed.as_ref() {
        if let Some(queue_id) = memory
            .pending_queue_item
            .as_ref()
            .and_then(|value| string_field(value, &["queueId", "queue_id"]))
        {
            push_unique(&mut recent_queue_ids, queue_id.to_string(), 5);
        }
        for entry in &memory.validation {
            if let Some(queue_id) = queue_id_from_validation(entry) {
                push_unique(&mut recent_queue_ids, queue_id, 5);
            }
        }
    }
    collect_pending_queue_ids(
        &query.harness_home,
        &query,
        &root_session_key,
        &mut recent_queue_ids,
    )?;
    if let Some(interruption) =
        latest_structured_interruption(&query.harness_home, &recent_queue_ids)?
    {
        last_interruption = Some(interruption);
    }
    let evidence_anchors =
        collect_evidence_anchors(&query.harness_home, &query, &recent_queue_ids)?;
    let operation_plans = collect_active_operation_plan_refs(
        &query.harness_home,
        &query.agent_id,
        &current_session_key,
    )?;

    Ok(VirtualSessionWorkingContext {
        schema: VIRTUAL_SESSION_WORKING_CONTEXT_SCHEMA.to_string(),
        lane,
        lane_digest: None,
        virtual_session_id: Some(virtual_session_id),
        current_session_key: Some(current_session_key),
        continuation_index,
        predecessor_session_key,
        working_set_file,
        last_interruption,
        recent_queue_ids,
        evidence_anchors,
        operation_plans,
        scope_decision: VirtualSessionScopeDecision {
            status: "same-virtual-session".to_string(),
            reason: "resolved by exact platform/channel/user/agent/session axes".to_string(),
            fallback_used: false,
            denied_candidates: Vec::new(),
        },
        created_at_ms: query.now_ms,
    })
}

/// Exact-lane resolver used by the full-lane prompt path. It intentionally
/// reads only v2 state/artifacts; a v1 artifact can enter only through the
/// named default-account migration APIs, which prove all non-account axes.
fn resolve_virtual_session_working_context_v2(
    mut query: VirtualSessionContextQuery,
    exact_lane: ChannelStateLane,
) -> io::Result<VirtualSessionWorkingContext> {
    // Downstream queue/evidence indexes consume the query object. Canonicalize
    // its account before they run so a legacy `None` cannot mean "any account"
    // after the full-lane boundary has selected the explicit default account.
    query.platform = exact_lane.platform().to_string();
    query.account_id = Some(exact_lane.account_id().to_string());
    let lane = VirtualSessionLane {
        platform: exact_lane.platform().to_string(),
        account_id: Some(exact_lane.account_id().to_string()),
        channel_id: exact_lane.channel_id().to_string(),
        user_id: exact_lane.user_id().to_string(),
        agent_id: exact_lane.agent_id().to_string(),
    };
    let channel_state = match read_channel_session_state_v2(&query.harness_home, &exact_lane)? {
        Some(state) => Some(state),
        None => migrate_legacy_channel_session_state_to_v2(&query.harness_home, &exact_lane)?.state,
    };
    let current_session_key = match query.session_key.clone() {
        Some(session_key) => Some(session_key),
        None => channel_state
            .as_ref()
            .map(|state| state.active_session_key.clone()),
    };
    let Some(current_session_key) = current_session_key else {
        return Ok(empty_envelope(
            lane,
            None,
            query.now_ms,
            VirtualSessionScopeDecision {
                status: "no-active-session".to_string(),
                reason: "no active v2 channel session was found for the exact lane".to_string(),
                fallback_used: false,
                denied_candidates: Vec::new(),
            },
        ));
    };
    let root_session_key = root_working_session_key(&current_session_key);
    let current_indexed = match read_working_set_memory_for_session_for_lane(
        &query.harness_home,
        &exact_lane,
        &current_session_key,
    ) {
        Ok(value) => value,
        Err(error) if error.kind() == io::ErrorKind::InvalidData => {
            return Ok(empty_envelope(
                lane,
                Some(current_session_key),
                query.now_ms,
                VirtualSessionScopeDecision {
                    status: "denied".to_string(),
                    reason: "v2 working-set artifacts did not match the requested exact lane"
                        .to_string(),
                    fallback_used: false,
                    denied_candidates: Vec::new(),
                },
            ));
        }
        Err(error) => return Err(error),
    };
    let indexed = match current_indexed {
        Some(value) => Some(value),
        None => match migrate_legacy_working_set_memory_for_session_for_lane(
            &query.harness_home,
            &exact_lane,
            &current_session_key,
        )? {
            Some(value) => Some(value),
            None if root_session_key != current_session_key => {
                let root_indexed = match read_working_set_memory_for_session_for_lane(
                    &query.harness_home,
                    &exact_lane,
                    &root_session_key,
                ) {
                    Ok(value) => value,
                    Err(error) if error.kind() == io::ErrorKind::InvalidData => {
                        return Ok(empty_envelope(
                            lane,
                            Some(current_session_key),
                            query.now_ms,
                            VirtualSessionScopeDecision {
                                status: "denied".to_string(),
                                reason: "v2 root working-set artifacts did not match the requested exact lane"
                                    .to_string(),
                                fallback_used: false,
                                denied_candidates: Vec::new(),
                            },
                        ));
                    }
                    Err(error) => return Err(error),
                };
                match root_indexed {
                    Some(value) => Some(value),
                    None => migrate_legacy_working_set_memory_for_session_for_lane(
                        &query.harness_home,
                        &exact_lane,
                        &root_session_key,
                    )?,
                }
            }
            None => None,
        },
    };
    let continuation_index = indexed
        .as_ref()
        .map(|(index, _)| index.continuation_index)
        .or_else(|| continuation_index_from_session_key(&current_session_key))
        .unwrap_or(0);
    let virtual_session_id = indexed
        .as_ref()
        .map(|(index, _)| index.virtual_session_id.clone())
        .unwrap_or_else(|| derive_virtual_session_id_v2(&exact_lane, &root_session_key));
    if let Err(error) =
        read_virtual_session_record_for_lane(&query.harness_home, &exact_lane, &virtual_session_id)
    {
        if error.kind() == io::ErrorKind::InvalidData {
            return Ok(empty_envelope(
                lane,
                Some(current_session_key),
                query.now_ms,
                VirtualSessionScopeDecision {
                    status: "denied".to_string(),
                    reason: "v2 virtual session record did not match the requested exact lane"
                        .to_string(),
                    fallback_used: false,
                    denied_candidates: vec![virtual_session_id],
                },
            ));
        }
        return Err(error);
    }
    let working_set_file = indexed
        .as_ref()
        .map(|(index, _)| index.working_set_file.clone());
    let mut last_interruption = indexed.as_ref().and_then(|(_, memory)| {
        memory
            .decisions
            .iter()
            .rev()
            .find(|decision| decision.contains("interrupted long-task"))
            .cloned()
    });
    let predecessor_session_key = indexed
        .as_ref()
        .and_then(|(_, memory)| memory.previous_working_session_key.clone())
        .or_else(|| predecessor_session_key(&root_session_key, continuation_index));
    let mut recent_queue_ids = Vec::new();
    if let Some((_, memory)) = indexed.as_ref() {
        if let Some(queue_id) = memory
            .pending_queue_item
            .as_ref()
            .and_then(|value| string_field(value, &["queueId", "queue_id"]))
        {
            push_unique(&mut recent_queue_ids, queue_id.to_string(), 5);
        }
        for entry in &memory.validation {
            if let Some(queue_id) = queue_id_from_validation(entry) {
                push_unique(&mut recent_queue_ids, queue_id, 5);
            }
        }
    }
    collect_pending_queue_ids(
        &query.harness_home,
        &query,
        &root_session_key,
        &mut recent_queue_ids,
    )?;
    if let Some(interruption) =
        latest_structured_interruption(&query.harness_home, &recent_queue_ids)?
    {
        last_interruption = Some(interruption);
    }
    let evidence_anchors =
        collect_evidence_anchors(&query.harness_home, &query, &recent_queue_ids)?;
    let operation_plans = collect_active_operation_plan_refs_for_lane(
        &query.harness_home,
        &exact_lane,
        &current_session_key,
    )?;
    Ok(VirtualSessionWorkingContext {
        schema: VIRTUAL_SESSION_WORKING_CONTEXT_SCHEMA.to_string(),
        lane,
        lane_digest: None,
        virtual_session_id: Some(virtual_session_id),
        current_session_key: Some(current_session_key),
        continuation_index,
        predecessor_session_key,
        working_set_file,
        last_interruption,
        recent_queue_ids,
        evidence_anchors,
        operation_plans,
        scope_decision: VirtualSessionScopeDecision {
            status: "same-virtual-session".to_string(),
            reason: "resolved by exact platform/account/channel/user/agent/session axes"
                .to_string(),
            fallback_used: false,
            denied_candidates: Vec::new(),
        },
        created_at_ms: query.now_ms,
    })
}

/// Resolves working context under an optional exact full-lane boundary.
///
/// Legacy callers retain the existing bounded four-axis behavior. New callers
/// should provide a concrete `FullLaneKeyV1`; legacy-unknown axes are denied
/// rather than interpreted as wildcards.
pub fn resolve_virtual_session_working_context_for_lane(
    query: VirtualSessionContextQuery,
    full_lane: Option<&FullLaneKeyV1>,
) -> io::Result<VirtualSessionWorkingContext> {
    let Some(full_lane) = full_lane else {
        return resolve_virtual_session_working_context(query);
    };
    let digest = full_lane.identity_hash().map_err(io::Error::other)?;
    let lane = VirtualSessionLane {
        platform: query.platform.clone(),
        account_id: query.account_id.clone(),
        channel_id: query.channel_id.clone(),
        user_id: query.user_id.clone(),
        agent_id: query.agent_id.clone(),
    };
    let exact_axes_match = !full_lane.has_legacy_unknowns()
        && full_lane.platform() == query.platform
        && virtual_session_account_matches_full_lane(query.account_id.as_deref(), full_lane)
        && full_lane.channel_id() == query.channel_id
        && full_lane.user_id() == query.user_id
        && full_lane.agent_id() == query.agent_id
        && query
            .session_key
            .as_deref()
            .is_some_and(|session| session == full_lane.concrete_session());
    if !exact_axes_match {
        let mut context = empty_envelope(
            lane,
            query.session_key,
            query.now_ms,
            VirtualSessionScopeDecision {
                status: "denied".to_string(),
                reason:
                    "full lane did not exactly match query axes; unknown axes never wildcard-match"
                        .to_string(),
                fallback_used: false,
                denied_candidates: vec![digest.clone()],
            },
        );
        context.lane_digest = Some(digest);
        return Ok(context);
    }

    let exact_lane = ChannelStateLane::new(
        &query.platform,
        query.account_id.as_deref(),
        &query.channel_id,
        &query.user_id,
        &query.agent_id,
    )?;
    let mut context = resolve_virtual_session_working_context_v2(query, exact_lane)?;
    context.lane_digest = Some(digest.clone());
    if context.scope_decision.status == "same-virtual-session"
        && context
            .current_session_key
            .as_deref()
            .is_some_and(|session| {
                root_working_session_key(session) != full_lane.root_virtual_session()
            })
    {
        context.virtual_session_id = None;
        context.predecessor_session_key = None;
        context.working_set_file = None;
        context.last_interruption = None;
        context.recent_queue_ids.clear();
        context.evidence_anchors = VirtualSessionEvidenceAnchors::default();
        context.operation_plans.clear();
        context.scope_decision = VirtualSessionScopeDecision {
            status: "denied".to_string(),
            reason: "full lane root virtual session did not match resolved root".to_string(),
            fallback_used: false,
            denied_candidates: vec![digest.clone()],
        };
    }
    if context.scope_decision.status == "same-virtual-session" && context.working_set_file.is_some()
    {
        // Prompt rendering consumes this field. Do not expose a local absolute
        // artifact path to the model; the durable file was already resolved
        // under the exact lane before this opaque reference is emitted.
        context.working_set_file = Some(opaque_working_set_reference(
            &digest,
            context.continuation_index,
        ));
    }
    Ok(context)
}

fn virtual_session_account_matches_full_lane(
    account_id: Option<&str>,
    full_lane: &FullLaneKeyV1,
) -> bool {
    let normalized = account_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("default");
    normalized == full_lane.account_id()
}

fn opaque_working_set_reference(lane_digest: &str, continuation_index: u64) -> PathBuf {
    PathBuf::from("artifact-ref")
        .join("working-set")
        .join(format!("{lane_digest}-{continuation_index}"))
}

fn empty_envelope(
    lane: VirtualSessionLane,
    current_session_key: Option<String>,
    now_ms: i64,
    scope_decision: VirtualSessionScopeDecision,
) -> VirtualSessionWorkingContext {
    VirtualSessionWorkingContext {
        schema: VIRTUAL_SESSION_WORKING_CONTEXT_SCHEMA.to_string(),
        lane,
        lane_digest: None,
        virtual_session_id: None,
        current_session_key,
        continuation_index: 0,
        predecessor_session_key: None,
        working_set_file: None,
        last_interruption: None,
        recent_queue_ids: Vec::new(),
        evidence_anchors: VirtualSessionEvidenceAnchors::default(),
        operation_plans: Vec::new(),
        scope_decision,
        created_at_ms: now_ms,
    }
}

fn predecessor_session_key(root_session_key: &str, continuation_index: u64) -> Option<String> {
    match continuation_index {
        0 => None,
        1 => Some(root_session_key.to_string()),
        value => Some(format!(
            "{}:cont-{}",
            root_session_key,
            value.saturating_sub(1)
        )),
    }
}

fn read_virtual_session_record(
    harness_home: &Path,
    virtual_session_id: &str,
) -> io::Result<Option<Value>> {
    let file = virtual_session_file(harness_home, virtual_session_id);
    let text = match fs::read_to_string(file) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    serde_json::from_str::<Value>(&text)
        .map(Some)
        .map_err(io::Error::other)
}

fn collect_pending_queue_ids(
    harness_home: &Path,
    query: &VirtualSessionContextQuery,
    root_session_key: &str,
    recent_queue_ids: &mut Vec<String>,
) -> io::Result<()> {
    let lane = VirtualSessionRuntimeLaneQuery {
        platform: query.platform.clone(),
        account_id: query.account_id.clone(),
        channel_id: query.channel_id.clone(),
        user_id: query.user_id.clone(),
        agent_id: query.agent_id.clone(),
        root_session_key: root_session_key.to_string(),
    };
    let mut warnings = Vec::new();
    let queue_ids =
        recent_runtime_queue_ids_for_virtual_session_lane(harness_home, &lane, 5, &mut warnings)?;
    // The index returns newest-first for the bounded SQL query. Preserve the
    // previous resolver's chronological recent-anchor presentation order.
    for queue_id in queue_ids.into_iter().rev() {
        push_unique(recent_queue_ids, queue_id, 5);
    }
    Ok(())
}

fn latest_structured_interruption(
    harness_home: &Path,
    queue_ids: &[String],
) -> io::Result<Option<String>> {
    if queue_ids.is_empty() {
        return Ok(None);
    }
    let queue_set = queue_ids.iter().cloned().collect::<BTreeSet<_>>();
    let mut warnings = Vec::new();
    let Some(interruption) = latest_runtime_interruption_evidence_for_queue_ids(
        harness_home,
        &queue_set,
        &mut warnings,
    )?
    else {
        return Ok(None);
    };
    let method = interruption.method.as_deref().unwrap_or("tool");
    let item_type = interruption.item_type.as_deref().unwrap_or("unknown");
    let preview = interruption.preview.as_deref().unwrap_or("(no preview)");
    let resume_action = if interruption.safe_to_rerun {
        "resumeAction=verification-rerun-eligible"
    } else {
        "resumeAction=explicit-review-required"
    };
    Ok(Some(bounded_text(
        &format!(
            "{}; method={method} itemType={item_type} preview={} {resume_action}",
            interruption.interruption_reason,
            bounded_text(preview, 120)
        ),
        240,
    )))
}

fn collect_evidence_anchors(
    harness_home: &Path,
    query: &VirtualSessionContextQuery,
    queue_ids: &[String],
) -> io::Result<VirtualSessionEvidenceAnchors> {
    let queue_set = queue_ids.iter().cloned().collect::<BTreeSet<_>>();
    let queue_dir = harness_home.join("state").join("runtime-queue");
    let channels_dir = harness_home.join("state").join("channels");
    let mut run_once_receipts = if queue_set.is_empty() {
        Vec::new()
    } else {
        let hot_index = refresh_runtime_queue_state_index(&queue_dir, &mut Vec::new())?;
        let hot_index_file = runtime_queue_state_index_file(&queue_dir);
        latest_runtime_queue_hot_receipts_from_index(&hot_index, &queue_set)
            .into_iter()
            .map(|record| VirtualSessionEvidenceAnchor {
                queue_id: record.queue_id,
                status: bounded_text(&record.status, 80),
                file: hot_index_file.clone(),
                at_ms: record.occurred_at_ms,
                reason: record
                    .reason
                    .as_deref()
                    .map(|reason| bounded_text(reason, 180)),
            })
            .collect()
    };
    let history_file = runtime_queue_receipt_history_file(&queue_dir);
    for record in find_runtime_queue_terminal_history(&queue_dir, &queue_set)? {
        if run_once_receipts
            .iter()
            .any(|anchor| anchor.queue_id == record.queue_id)
        {
            continue;
        }
        run_once_receipts.push(VirtualSessionEvidenceAnchor {
            queue_id: record.queue_id,
            status: bounded_text(&record.status, 80),
            file: history_file.clone(),
            at_ms: record.occurred_at_ms.or(Some(record.compacted_at_ms)),
            reason: record
                .reason
                .as_deref()
                .map(|reason| bounded_text(reason, 180)),
        });
    }
    run_once_receipts.sort_by_key(|anchor| anchor.at_ms.unwrap_or_default());
    if run_once_receipts.len() > 3 {
        run_once_receipts.drain(0..run_once_receipts.len().saturating_sub(3));
    }

    let mut warnings = Vec::new();
    let execution_file = runtime_execution_receipt_index_file(&queue_dir);
    let mut execution_receipts =
        execution_receipt_evidence_for_queue_ids(&queue_dir, &queue_set, &mut warnings)?
            .into_iter()
            .map(|record| VirtualSessionEvidenceAnchor {
                queue_id: record.queue_id,
                status: bounded_text(&record.status, 80),
                file: execution_file.clone(),
                at_ms: record.at_ms,
                reason: record
                    .reason
                    .as_deref()
                    .map(|reason| bounded_text(reason, 180)),
            })
            .collect::<Vec<_>>();
    retain_recent_anchors(&mut execution_receipts, 3);

    let delivery_index_file =
        crate::channel_delivery_index::channel_delivery_state_index_file(&channels_dir);
    let mut outbox_rows = Vec::new();
    let mut delivery_receipts = Vec::new();
    for queue_id in &queue_set {
        let states = channel_delivery_states_for_source_queue_in_channel(
            &channels_dir,
            queue_id,
            &query.platform,
            query.account_id.as_deref(),
            &query.channel_id,
            &query.user_id,
            &mut warnings,
        )?;
        for state in states {
            outbox_rows.push(VirtualSessionEvidenceAnchor {
                queue_id: queue_id.clone(),
                status: outbound_message_kind_label(&state.message.kind).to_string(),
                file: delivery_index_file.clone(),
                at_ms: None,
                reason: None,
            });
            if let Some(status) = state.terminal_status.or(state.last_status) {
                delivery_receipts.push(VirtualSessionEvidenceAnchor {
                    queue_id: queue_id.clone(),
                    status: delivery_status_label(status).to_string(),
                    file: delivery_index_file.clone(),
                    at_ms: None,
                    reason: None,
                });
            }
        }
    }
    retain_recent_anchors(&mut outbox_rows, 3);
    retain_recent_anchors(&mut delivery_receipts, 3);

    let progress_file = progress_event_index_file(harness_home);
    let progress_by_queue =
        progress_events_for_queue_ids(harness_home, &queue_set, 3, &mut warnings)?;
    let mut progress_events = progress_by_queue
        .into_values()
        .flatten()
        .collect::<Vec<_>>();
    progress_events.sort_by_key(|indexed| indexed.line_number);
    let mut progress_receipts = progress_events
        .into_iter()
        .map(|indexed| VirtualSessionEvidenceAnchor {
            queue_id: indexed.event.queue_id,
            status: progress_status_label(indexed.event.status).to_string(),
            file: progress_file.clone(),
            at_ms: Some(indexed.event.at_ms),
            reason: indexed
                .event
                .source
                .as_deref()
                .map(|source| bounded_text(source, 180)),
        })
        .collect::<Vec<_>>();
    retain_recent_anchors(&mut progress_receipts, 3);

    Ok(VirtualSessionEvidenceAnchors {
        run_once_receipts,
        execution_receipts,
        outbox_rows,
        delivery_receipts,
        progress_receipts,
    })
}

fn retain_recent_anchors(anchors: &mut Vec<VirtualSessionEvidenceAnchor>, limit: usize) {
    if anchors.len() > limit {
        anchors.drain(0..anchors.len().saturating_sub(limit));
    }
}

fn outbound_message_kind_label(kind: &crate::ChannelOutboundMessageKind) -> &'static str {
    match kind {
        crate::ChannelOutboundMessageKind::CommandReply => "command-reply",
        crate::ChannelOutboundMessageKind::AgentReply => "agent-reply",
        crate::ChannelOutboundMessageKind::ApprovalRequest => "approval-request",
        crate::ChannelOutboundMessageKind::ErrorReply => "error-reply",
    }
}

fn delivery_status_label(status: crate::ChannelDeliveryStatus) -> &'static str {
    match status {
        crate::ChannelDeliveryStatus::Delivered => "delivered",
        crate::ChannelDeliveryStatus::Failed => "failed",
        crate::ChannelDeliveryStatus::SkippedPermanent => "skipped-permanent",
    }
}

fn progress_status_label(status: crate::AgentProgressStatus) -> &'static str {
    match status {
        crate::AgentProgressStatus::Started => "started",
        crate::AgentProgressStatus::Progress => "progress",
        crate::AgentProgressStatus::Completed => "completed",
        crate::AgentProgressStatus::Failed => "failed",
    }
}

fn queue_id_from_validation(entry: &str) -> Option<String> {
    let rest = entry.strip_prefix("run-once:")?;
    let queue_id = rest
        .rsplit_once(':')
        .map(|(queue_id, _status)| queue_id)
        .unwrap_or(rest);
    if queue_id.is_empty() {
        None
    } else {
        Some(queue_id.to_string())
    }
}

fn push_unique(values: &mut Vec<String>, value: String, limit: usize) {
    if values.iter().any(|existing| existing == &value) {
        return;
    }
    values.push(value);
    if values.len() > limit {
        values.remove(0);
    }
}

fn bounded_text(value: &str, max_chars: usize) -> String {
    let scrubbed = value
        .replace("https://", "https://[redacted]/")
        .replace("http://", "http://[redacted]/")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if scrubbed.chars().count() <= max_chars {
        return scrubbed;
    }
    let mut out = scrubbed
        .chars()
        .take(max_chars.saturating_sub(3))
        .collect::<String>();
    out.push_str("...");
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lane::LegacyLaneKeyV0;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn exact_lane_denies_legacy_unknown_axes_instead_of_wildcard_matching() {
        let root = temp_root("exact_lane_denies_legacy_unknown_axes");
        let lane = FullLaneKeyV1::from_legacy(LegacyLaneKeyV0 {
            platform: Some("telegram".to_string()),
            channel_id: Some("dm".to_string()),
            user_id: Some("user".to_string()),
            agent_id: Some("main".to_string()),
            concrete_session: Some("session-1".to_string()),
            ..LegacyLaneKeyV0::default()
        })
        .unwrap();
        let context =
            resolve_virtual_session_working_context_for_lane(query(root, "session-1"), Some(&lane))
                .unwrap();

        assert_eq!(context.scope_decision.status, "denied");
        assert!(
            context
                .scope_decision
                .reason
                .contains("never wildcard-match")
        );
        assert_eq!(context.lane_digest, Some(lane.identity_hash().unwrap()));
    }

    #[test]
    fn exact_lane_digest_changes_with_account_and_runtime_and_root_mismatch_is_denied() {
        let root = temp_root("exact_lane_digest_changes");
        let lane = exact_lane("account-a", "interactive", "session-1");
        let account_other = exact_lane("account-b", "interactive", "session-1");
        let runtime_other = exact_lane("account-a", "worker", "session-1");
        assert_ne!(
            lane.identity_hash().unwrap(),
            account_other.identity_hash().unwrap()
        );
        assert_ne!(
            lane.identity_hash().unwrap(),
            runtime_other.identity_hash().unwrap()
        );

        let wrong_root = exact_lane("account-a", "interactive", "different-root");
        let mut exact_query = query(root, "session-1");
        exact_query.account_id = Some("account-a".to_string());
        let context =
            resolve_virtual_session_working_context_for_lane(exact_query, Some(&wrong_root))
                .unwrap();
        assert_eq!(context.scope_decision.status, "denied");
        assert!(
            context
                .scope_decision
                .reason
                .contains("root virtual session")
        );
        assert!(context.recent_queue_ids.is_empty());
    }

    #[test]
    fn virtual_session_evidence_uses_committed_terminal_history_after_hot_compaction() {
        let root = temp_root("virtual_session_evidence_uses_committed_terminal_history");
        let harness_home = root.join(".agent-harness");
        let queue_dir = harness_home.join("state").join("runtime-queue");
        fs::create_dir_all(&queue_dir).unwrap();
        fs::write(
            queue_dir.join("pending.jsonl"),
            serde_json::json!({
                "queueId":"queue-history",
                "platform":"telegram",
                "channelId":"dm",
                "userId":"user",
                "agentId":"main",
                "sessionKey":"session-1",
                "status":"queued"
            })
            .to_string(),
        )
        .unwrap();
        fs::write(queue_dir.join("run-once-receipts.jsonl"), "").unwrap();
        let staged = crate::runtime_receipt_history::stage_runtime_queue_receipt_history(
            &queue_dir,
            "virtual-session-history",
            br#"{"queueId":"queue-history","sessionKey":"session-1","status":"completed","reason":"previous turn complete","completedAtMs":42}
"#,
            b"",
            &std::collections::HashSet::new(),
            100,
        )
        .unwrap();
        crate::runtime_receipt_history::commit_runtime_queue_receipt_history(&staged, 101).unwrap();

        let context =
            resolve_virtual_session_working_context(query(harness_home, "session-1")).unwrap();
        assert!(
            context
                .evidence_anchors
                .run_once_receipts
                .iter()
                .any(|anchor| {
                    anchor.queue_id == "queue-history"
                        && anchor.status == "completed"
                        && anchor.file
                            == crate::runtime_receipt_history::runtime_queue_receipt_history_file(
                                &queue_dir,
                            )
                })
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn virtual_session_evidence_reads_current_receipt_from_hot_index_before_cold_history() {
        let root = temp_root("virtual_session_evidence_reads_current_receipt_from_hot_index");
        let harness_home = root.join(".agent-harness");
        let queue_dir = harness_home.join("state").join("runtime-queue");
        fs::create_dir_all(&queue_dir).unwrap();
        fs::write(
            queue_dir.join("pending.jsonl"),
            serde_json::json!({
                "queueId":"queue-current",
                "platform":"telegram",
                "channelId":"dm",
                "userId":"user",
                "agentId":"main",
                "sessionKey":"session-1",
                "status":"queued"
            })
            .to_string(),
        )
        .unwrap();
        fs::write(
            queue_dir.join("run-once-receipts.jsonl"),
            serde_json::json!({
                "queueId":"queue-current",
                "sessionKey":"session-1",
                "status":"completed",
                "reason":"current hot result",
                "runtimeClass":"interactive",
                "origin":"channel",
                "completedAtMs":99
            })
            .to_string(),
        )
        .unwrap();
        crate::runtime_worker::rebuild_runtime_queue_state_index(&queue_dir, &mut Vec::new())
            .unwrap();
        let staged = crate::runtime_receipt_history::stage_runtime_queue_receipt_history(
            &queue_dir,
            "virtual-session-cold-fallback",
            br#"{"queueId":"queue-current","sessionKey":"session-1","status":"failed-terminal","reason":"older cold result"}
"#,
            b"",
            &std::collections::HashSet::new(),
            100,
        )
        .unwrap();
        crate::runtime_receipt_history::commit_runtime_queue_receipt_history(&staged, 101).unwrap();

        let context =
            resolve_virtual_session_working_context(query(harness_home, "session-1")).unwrap();
        let anchor = context
            .evidence_anchors
            .run_once_receipts
            .iter()
            .find(|anchor| anchor.queue_id == "queue-current")
            .expect("current queue must retain an evidence anchor");
        assert_eq!(anchor.status, "completed");
        assert_eq!(anchor.file, queue_dir.join("queue-state-index.json"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn exact_v2_lane_isolates_accounts_agents_and_redacts_prompt_working_set_path() {
        let root = temp_root("exact_v2_lane_isolates_accounts_agents");
        let harness_home = root.join(".agent-harness");
        let account_a = snapshot_for_lane(&harness_home, "account-a", "main");
        let account_b = snapshot_for_lane(&harness_home, "account-b", "main");
        let other_agent = snapshot_for_lane(&harness_home, "account-a", "worker");
        assert_ne!(account_a.virtual_session_id, account_b.virtual_session_id);
        assert_ne!(account_a.virtual_session_id, other_agent.virtual_session_id);

        let mut account_a_query = query(harness_home.clone(), "session-1");
        account_a_query.account_id = Some("account-a".to_string());
        let account_a_context = resolve_virtual_session_working_context_for_lane(
            account_a_query,
            Some(&full_lane_for("account-a", "main")),
        )
        .unwrap();
        assert_eq!(
            account_a_context.virtual_session_id.as_deref(),
            Some(account_a.virtual_session_id.as_str())
        );
        let prompt_ref = account_a_context
            .working_set_file
            .as_ref()
            .expect("v2 prompt context must include an opaque working-set reference");
        assert!(!prompt_ref.is_absolute());
        assert!(prompt_ref.starts_with("artifact-ref"));
        assert!(
            !prompt_ref
                .display()
                .to_string()
                .contains(&harness_home.display().to_string())
        );

        let mut account_b_query = query(harness_home.clone(), "session-1");
        account_b_query.account_id = Some("account-b".to_string());
        let account_b_context = resolve_virtual_session_working_context_for_lane(
            account_b_query,
            Some(&full_lane_for("account-b", "main")),
        )
        .unwrap();
        assert_eq!(
            account_b_context.virtual_session_id.as_deref(),
            Some(account_b.virtual_session_id.as_str())
        );
        assert_ne!(
            account_a_context.virtual_session_id,
            account_b_context.virtual_session_id
        );

        let mut agent_query = query(harness_home.clone(), "session-1");
        agent_query.account_id = Some("account-a".to_string());
        agent_query.agent_id = "worker".to_string();
        let agent_context = resolve_virtual_session_working_context_for_lane(
            agent_query,
            Some(&full_lane_for("account-a", "worker")),
        )
        .unwrap();
        assert_eq!(
            agent_context.virtual_session_id.as_deref(),
            Some(other_agent.virtual_session_id.as_str())
        );
        assert_ne!(
            account_a_context.virtual_session_id,
            agent_context.virtual_session_id
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn non_default_full_lane_does_not_read_legacy_working_set() {
        let root = temp_root("non_default_full_lane_denies_legacy_working_set");
        let harness_home = root.join(".agent-harness");
        let legacy = crate::context_rollover::record_completed_turn_working_set_snapshot(
            crate::context_rollover::CompletedTurnWorkingSetSnapshotOptions {
                harness_home: harness_home.clone(),
                platform: "telegram".to_string(),
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                agent_id: "main".to_string(),
                working_session_key: "session-1".to_string(),
                queue_id: None,
                message_text: Some("legacy-only".to_string()),
                status: "completed".to_string(),
                run_once_receipt_file: None,
                outbox_file: None,
                completion_file: None,
                now_ms: 1,
            },
        )
        .unwrap();
        let mut exact_query = query(harness_home.clone(), "session-1");
        exact_query.account_id = Some("account-b".to_string());
        let context = resolve_virtual_session_working_context_for_lane(
            exact_query,
            Some(&full_lane_for("account-b", "main")),
        )
        .unwrap();
        assert_eq!(context.scope_decision.status, "same-virtual-session");
        assert!(context.working_set_file.is_none());
        assert_ne!(
            context.virtual_session_id.as_deref(),
            Some(legacy.virtual_session_id.as_str())
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn default_full_lane_migrates_only_an_exact_legacy_record() {
        let root = temp_root("default_full_lane_migrates_exact_legacy_record");
        let harness_home = root.join(".agent-harness");
        crate::context_rollover::record_completed_turn_working_set_snapshot(
            crate::context_rollover::CompletedTurnWorkingSetSnapshotOptions {
                harness_home: harness_home.clone(),
                platform: "telegram".to_string(),
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                agent_id: "main".to_string(),
                working_session_key: "session-1".to_string(),
                queue_id: None,
                message_text: Some("eligible-default-legacy".to_string()),
                status: "completed".to_string(),
                run_once_receipt_file: None,
                outbox_file: None,
                completion_file: None,
                now_ms: 1,
            },
        )
        .unwrap();
        let context = resolve_virtual_session_working_context_for_lane(
            query(harness_home.clone(), "session-1"),
            Some(&full_lane_for("default", "main")),
        )
        .unwrap();
        assert_eq!(context.scope_decision.status, "same-virtual-session");
        assert!(context.working_set_file.is_some());
        let virtual_session_id = context.virtual_session_id.expect("v2 id");
        let record: Value = serde_json::from_str(
            &fs::read_to_string(crate::context_rollover::virtual_session_file(
                &harness_home,
                &virtual_session_id,
            ))
            .unwrap(),
        )
        .unwrap();
        assert_eq!(
            string_field(&record, &["accountId"]),
            Some("default"),
            "the migrated artifact must have an explicit account"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn mismatched_v2_virtual_record_is_denied_without_fallback() {
        let root = temp_root("mismatched_v2_virtual_record_is_denied");
        let harness_home = root.join(".agent-harness");
        let report = snapshot_for_lane(&harness_home, "account-a", "main");
        let mut value: Value =
            serde_json::from_str(&fs::read_to_string(&report.virtual_session_file).unwrap())
                .unwrap();
        value.as_object_mut().unwrap().insert(
            "accountId".to_string(),
            Value::String("account-b".to_string()),
        );
        fs::write(
            &report.virtual_session_file,
            serde_json::to_string(&value).unwrap(),
        )
        .unwrap();

        let mut exact_query = query(harness_home.clone(), "session-1");
        exact_query.account_id = Some("account-a".to_string());
        let context = resolve_virtual_session_working_context_for_lane(
            exact_query,
            Some(&full_lane_for("account-a", "main")),
        )
        .unwrap();
        assert_eq!(context.scope_decision.status, "denied");
        assert!(context.working_set_file.is_none());
        assert!(context.recent_queue_ids.is_empty());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn exact_v2_lifecycle_updates_only_its_own_virtual_record() {
        let root = temp_root("exact_v2_lifecycle_updates_only_its_own_record");
        let harness_home = root.join(".agent-harness");
        let report = snapshot_for_lane(&harness_home, "account-a", "main");
        let lane =
            ChannelStateLane::new("telegram", Some("account-a"), "dm", "user", "main").unwrap();
        crate::context_rollover::backfill_virtual_session_codex_thread_id_for_lane(
            crate::context_rollover::VirtualSessionThreadBackfillV2Options {
                harness_home: harness_home.clone(),
                channel_lane: lane.clone(),
                session_key: "session-1".to_string(),
                thread_id: "thread-v2".to_string(),
                now_ms: 2,
            },
        )
        .unwrap()
        .expect("exact v2 record");
        crate::context_rollover::close_virtual_session_for_task_boundary_for_lane(
            crate::context_rollover::VirtualSessionTaskBoundaryCloseV2Options {
                harness_home: harness_home.clone(),
                channel_lane: lane.clone(),
                previous_session_key: "session-1".to_string(),
                ended_by: "task-boundary".to_string(),
                now_ms: 3,
            },
        )
        .unwrap()
        .expect("exact v2 record");
        crate::context_rollover::mark_virtual_session_terminal_for_lane(
            crate::context_rollover::VirtualSessionTerminalV2Options {
                harness_home: harness_home.clone(),
                channel_lane: lane.clone(),
                session_key: "session-1".to_string(),
                ended_by: "terminal".to_string(),
                now_ms: 4,
            },
        )
        .unwrap()
        .expect("exact v2 record");

        let foreign_lane =
            ChannelStateLane::new("telegram", Some("account-b"), "dm", "user", "main").unwrap();
        assert!(
            crate::context_rollover::mark_virtual_session_terminal_for_lane(
                crate::context_rollover::VirtualSessionTerminalV2Options {
                    harness_home: harness_home.clone(),
                    channel_lane: foreign_lane,
                    session_key: "session-1".to_string(),
                    ended_by: "foreign".to_string(),
                    now_ms: 5,
                },
            )
            .unwrap()
            .is_none()
        );

        let record: Value =
            serde_json::from_str(&fs::read_to_string(report.virtual_session_file).unwrap())
                .unwrap();
        assert_eq!(string_field(&record, &["status"]), Some("terminal-failed"));
        assert_eq!(string_field(&record, &["accountId"]), Some("account-a"));
        assert!(
            record
                .get("workingSessions")
                .and_then(Value::as_array)
                .is_some_and(|sessions| sessions.iter().any(|session| {
                    string_field(session, &["sessionKey"]) == Some("session-1")
                        && string_field(session, &["codexThreadId"]) == Some("thread-v2")
                }))
        );

        let _ = fs::remove_dir_all(root);
    }

    fn snapshot_for_lane(
        harness_home: &Path,
        account_id: &str,
        agent_id: &str,
    ) -> crate::context_rollover::CompletedTurnWorkingSetSnapshotReport {
        let lane =
            ChannelStateLane::new("telegram", Some(account_id), "dm", "user", agent_id).unwrap();
        crate::context_rollover::record_completed_turn_working_set_snapshot_for_lane(
            crate::context_rollover::CompletedTurnWorkingSetSnapshotV2Options {
                harness_home: harness_home.to_path_buf(),
                channel_lane: lane,
                working_session_key: "session-1".to_string(),
                queue_id: None,
                message_text: Some(format!("{account_id}-{agent_id}")),
                status: "completed".to_string(),
                run_once_receipt_file: None,
                outbox_file: None,
                completion_file: None,
                now_ms: 1,
            },
        )
        .unwrap()
    }

    fn full_lane_for(account: &str, agent: &str) -> FullLaneKeyV1 {
        FullLaneKeyV1::new(
            "telegram",
            account,
            "dm",
            "user",
            agent,
            "interactive",
            "session-1",
            "session-1",
        )
        .unwrap()
    }

    fn exact_lane(account: &str, runtime: &str, root: &str) -> FullLaneKeyV1 {
        FullLaneKeyV1::new(
            "telegram",
            account,
            "dm",
            "user",
            "main",
            runtime,
            root,
            "session-1",
        )
        .unwrap()
    }

    fn query(harness_home: PathBuf, session: &str) -> VirtualSessionContextQuery {
        VirtualSessionContextQuery {
            harness_home,
            platform: "telegram".to_string(),
            account_id: None,
            channel_id: "dm".to_string(),
            user_id: "user".to_string(),
            agent_id: "main".to_string(),
            session_key: Some(session.to_string()),
            now_ms: 1,
        }
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-virtual-session-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
