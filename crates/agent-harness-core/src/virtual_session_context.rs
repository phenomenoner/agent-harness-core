use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::channel_state::read_channel_session_state;
use crate::context_rollover::{
    collect_active_operation_plan_refs, continuation_index_from_session_key,
    derive_virtual_session_id, read_working_set_memory_for_session, root_working_session_key,
    string_field, virtual_session_file,
};
use crate::lane::FullLaneKeyV1;

pub const VIRTUAL_SESSION_WORKING_CONTEXT_SCHEMA: &str =
    "agent-harness.virtual-session-working-context.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VirtualSessionContextQuery {
    pub harness_home: PathBuf,
    pub platform: String,
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
    let evidence_anchors = collect_evidence_anchors(&query.harness_home, &recent_queue_ids)?;
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
        channel_id: query.channel_id.clone(),
        user_id: query.user_id.clone(),
        agent_id: query.agent_id.clone(),
    };
    let exact_axes_match = !full_lane.has_legacy_unknowns()
        && full_lane.platform() == query.platform
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

    let mut context = resolve_virtual_session_working_context(query)?;
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
            denied_candidates: vec![digest],
        };
    }
    Ok(context)
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
    let queue_file = harness_home
        .join("state")
        .join("runtime-queue")
        .join("pending.jsonl");
    for value in read_jsonl_values(&queue_file)? {
        if string_field(&value, &["platform"]) != Some(query.platform.as_str())
            || string_field(&value, &["channelId", "channel_id"]) != Some(query.channel_id.as_str())
            || string_field(&value, &["userId", "user_id"]) != Some(query.user_id.as_str())
            || string_field(&value, &["agentId", "agent_id"]) != Some(query.agent_id.as_str())
        {
            continue;
        }
        let Some(session_key) = string_field(&value, &["sessionKey", "session_key"]) else {
            continue;
        };
        if root_working_session_key(session_key) != root_session_key {
            continue;
        }
        if let Some(queue_id) = string_field(&value, &["queueId", "queue_id"]) {
            push_unique(recent_queue_ids, queue_id.to_string(), 5);
        }
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
    let file = harness_home
        .join("state")
        .join("runtime-queue")
        .join("codex-runtime-run-receipts.jsonl");
    let mut latest = None;
    for value in read_jsonl_values(&file)? {
        let Some(queue_id) = string_field(&value, &["queueId", "queue_id"]) else {
            continue;
        };
        if !queue_set.contains(queue_id) {
            continue;
        }
        let Some(reason) = string_field(&value, &["interruptionReason", "interruption_reason"])
        else {
            continue;
        };
        let Some(tool) = value
            .get("interruptedToolUses")
            .or_else(|| value.get("interrupted_tool_uses"))
            .and_then(Value::as_array)
            .and_then(|tools| tools.first())
        else {
            continue;
        };
        let method = string_field(tool, &["method"]).unwrap_or("tool");
        let item_type = string_field(tool, &["itemType", "item_type"]).unwrap_or("unknown");
        let preview = string_field(tool, &["preview"]).unwrap_or("(no preview)");
        let safe_to_rerun = tool
            .get("safeToRerun")
            .or_else(|| tool.get("safe_to_rerun"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let resume_action = if safe_to_rerun {
            "resumeAction=verification-rerun-eligible"
        } else {
            "resumeAction=explicit-review-required"
        };
        latest = Some(bounded_text(
            &format!(
                "{reason}; method={method} itemType={item_type} preview={} {resume_action}",
                bounded_text(preview, 120)
            ),
            240,
        ));
    }
    Ok(latest)
}

fn collect_evidence_anchors(
    harness_home: &Path,
    queue_ids: &[String],
) -> io::Result<VirtualSessionEvidenceAnchors> {
    let queue_set = queue_ids.iter().cloned().collect::<BTreeSet<_>>();
    let queue_dir = harness_home.join("state").join("runtime-queue");
    let channels_dir = harness_home.join("state").join("channels");
    let progress_dir = harness_home.join("state").join("progress");
    Ok(VirtualSessionEvidenceAnchors {
        run_once_receipts: collect_anchors(
            &queue_dir.join("run-once-receipts.jsonl"),
            &queue_set,
            &["queueId", "queue_id"],
            &["status"],
        )?,
        execution_receipts: collect_anchors(
            &queue_dir.join("execution-receipts.jsonl"),
            &queue_set,
            &["queueId", "queue_id"],
            &["status"],
        )?,
        outbox_rows: collect_anchors(
            &channels_dir.join("outbox.jsonl"),
            &queue_set,
            &["sourceQueueId", "source_queue_id"],
            &["kind"],
        )?,
        delivery_receipts: collect_anchors(
            &channels_dir.join("delivery-receipts.jsonl"),
            &queue_set,
            &["sourceQueueId", "source_queue_id", "queueId", "queue_id"],
            &["status"],
        )?,
        progress_receipts: collect_anchors(
            &progress_dir.join("events.jsonl"),
            &queue_set,
            &["queueId", "queue_id"],
            &["status", "kind"],
        )?,
    })
}

fn collect_anchors(
    file: &Path,
    queue_ids: &BTreeSet<String>,
    queue_keys: &[&str],
    status_keys: &[&str],
) -> io::Result<Vec<VirtualSessionEvidenceAnchor>> {
    if queue_ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut anchors = Vec::new();
    for value in read_jsonl_values(file)? {
        let Some(queue_id) = string_field(&value, queue_keys) else {
            continue;
        };
        if !queue_ids.contains(queue_id) {
            continue;
        }
        let status = string_field(&value, status_keys).unwrap_or("recorded");
        anchors.push(VirtualSessionEvidenceAnchor {
            queue_id: queue_id.to_string(),
            status: bounded_text(status, 80),
            file: file.to_path_buf(),
            at_ms: value
                .get("atMs")
                .or_else(|| value.get("createdAtMs"))
                .or_else(|| value.get("updatedAtMs"))
                .and_then(Value::as_i64),
            reason: string_field(&value, &["reason", "error"])
                .map(|reason| bounded_text(reason, 180)),
        });
    }
    if anchors.len() > 3 {
        anchors.drain(0..anchors.len().saturating_sub(3));
    }
    Ok(anchors)
}

fn read_jsonl_values(file: &Path) -> io::Result<Vec<Value>> {
    let text = match fs::read_to_string(file) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    Ok(text
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line.trim()).ok())
        .collect())
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
        let context = resolve_virtual_session_working_context_for_lane(
            query(root, "session-1"),
            Some(&wrong_root),
        )
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
