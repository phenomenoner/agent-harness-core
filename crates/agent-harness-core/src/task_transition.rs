use std::io;
use std::path::{Path, PathBuf};

use ring::digest::{SHA256, digest};
use serde::{Deserialize, Serialize};

use crate::context_rollover::root_working_session_key;
use crate::operation_plan::{
    OperationPlanItem, OperationPlanItemStatus, OperationPlanShowOptions, OperationPlanStatus,
    show_operation_plan,
};

pub const TASK_CONTINUATION_CHECKPOINT_SCHEMA: &str =
    "agent-harness.task-continuation-checkpoint.v1";
pub const TASK_TRANSITION_SCHEMA: &str = "agent-harness.task-transition.v1";
pub const TASK_CONTINUATION_MARKER_OPEN: &str = "<agent-harness-continuation-checkpoint>";
pub const TASK_CONTINUATION_MARKER_CLOSE: &str = "</agent-harness-continuation-checkpoint>";
const MAX_CHECKPOINT_BYTES: usize = 2 * 1024;
const MAX_MARKER_BYTES: usize = 4 * 1024;
pub const DEFAULT_MAX_TASK_CONTINUATION_DEPTH: u64 = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ContinuationAuthorityKindV1 {
    Goal,
    OperationPlan,
    ExplicitCheckpoint,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskContinuationCheckpointV1 {
    pub schema: String,
    pub authority_kind: ContinuationAuthorityKindV1,
    pub authority_id: String,
    pub authority_version: u64,
    pub active_item_id: Option<String>,
    pub active_item_version: Option<u64>,
    pub checkpoint: String,
    pub checkpoint_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContinuationAuthoritySnapshotV1 {
    pub kind: ContinuationAuthorityKindV1,
    pub authority_id: String,
    pub authority_version: u64,
    pub authority_checksum: String,
    pub exact_lane_digest: String,
    pub virtual_session_id: String,
    pub active_item_id: Option<String>,
    pub active_item_version: Option<u64>,
    pub checkpoint_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DrainDispositionV1 {
    LogicalComplete,
    ContinuationRequired(ContinuationAuthoritySnapshotV1),
    NeedsUser,
    NeedsAuthority,
    Blocked,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TaskContinuationBreakers {
    pub operator_stop: bool,
    pub newer_steer: bool,
    pub budget_exhausted: bool,
    pub no_progress_exhausted: bool,
    pub continuation_depth: u64,
    pub max_continuation_depth: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationPlanAuthorityOptions {
    pub harness_home: PathBuf,
    pub checkpoint: TaskContinuationCheckpointV1,
    pub exact_lane_digest: String,
    pub virtual_session_id: String,
    pub working_session_key: String,
    pub agent_id: String,
    pub breakers: TaskContinuationBreakers,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskDrainEvaluationV1 {
    pub schema: String,
    pub disposition: DrainDispositionV1,
    pub schedule_continuation: bool,
    pub allow_logical_final: bool,
    pub reason: String,
}

pub fn extract_task_continuation_checkpoint(
    assistant_text: &str,
) -> io::Result<(String, Option<TaskContinuationCheckpointV1>)> {
    let Some(open_at) = assistant_text.find(TASK_CONTINUATION_MARKER_OPEN) else {
        return Ok((assistant_text.to_string(), None));
    };
    if assistant_text[open_at + TASK_CONTINUATION_MARKER_OPEN.len()..]
        .contains(TASK_CONTINUATION_MARKER_OPEN)
    {
        return Err(invalid("multiple task continuation checkpoint markers"));
    }
    let json_start = open_at + TASK_CONTINUATION_MARKER_OPEN.len();
    let close_relative = assistant_text[json_start..]
        .find(TASK_CONTINUATION_MARKER_CLOSE)
        .ok_or_else(|| invalid("task continuation checkpoint marker is not closed"))?;
    let json_end = json_start + close_relative;
    if json_end - json_start > MAX_MARKER_BYTES {
        return Err(invalid(
            "task continuation checkpoint marker exceeds its bound",
        ));
    }
    let trailing = &assistant_text[json_end + TASK_CONTINUATION_MARKER_CLOSE.len()..];
    if trailing.contains(TASK_CONTINUATION_MARKER_CLOSE) {
        return Err(invalid(
            "multiple task continuation checkpoint close markers",
        ));
    }
    let checkpoint: TaskContinuationCheckpointV1 =
        serde_json::from_str(assistant_text[json_start..json_end].trim()).map_err(|error| {
            invalid(format!(
                "invalid task continuation checkpoint JSON: {error}"
            ))
        })?;
    validate_checkpoint(&checkpoint)?;
    let visible = format!("{}{}", &assistant_text[..open_at], trailing)
        .trim()
        .to_string();
    if visible.is_empty() {
        return Err(invalid(
            "task continuation checkpoint cannot replace the entire assistant handoff",
        ));
    }
    Ok((visible, Some(checkpoint)))
}

pub fn evaluate_operation_plan_drain(
    options: OperationPlanAuthorityOptions,
) -> TaskDrainEvaluationV1 {
    match resolve_operation_plan_authority(&options) {
        Ok(snapshot) => TaskDrainEvaluationV1 {
            schema: TASK_TRANSITION_SCHEMA.to_string(),
            disposition: DrainDispositionV1::ContinuationRequired(snapshot),
            schedule_continuation: true,
            allow_logical_final: false,
            reason: "deadline drain has current exact-lane OperationPlan authority and a typed checkpoint"
                .to_string(),
        },
        Err(error) => TaskDrainEvaluationV1 {
            schema: TASK_TRANSITION_SCHEMA.to_string(),
            disposition: if options.breakers.operator_stop
                || options.breakers.newer_steer
                || options.breakers.budget_exhausted
                || options.breakers.no_progress_exhausted
                || depth_exhausted(options.breakers)
            {
                DrainDispositionV1::Blocked
            } else {
                DrainDispositionV1::NeedsAuthority
            },
            schedule_continuation: false,
            allow_logical_final: false,
            reason: error.to_string(),
        },
    }
}

pub fn logical_complete_drain() -> TaskDrainEvaluationV1 {
    TaskDrainEvaluationV1 {
        schema: TASK_TRANSITION_SCHEMA.to_string(),
        disposition: DrainDispositionV1::LogicalComplete,
        schedule_continuation: false,
        allow_logical_final: true,
        reason: "ordinary drain completion has no typed continuation authority".to_string(),
    }
}

pub fn revalidate_operation_plan_snapshot(
    harness_home: &Path,
    snapshot: &ContinuationAuthoritySnapshotV1,
    working_session_key: &str,
    agent_id: &str,
) -> io::Result<()> {
    if snapshot.kind != ContinuationAuthorityKindV1::OperationPlan {
        return Err(invalid("task snapshot is not OperationPlan authority"));
    }
    let report = show_operation_plan(OperationPlanShowOptions {
        harness_home: harness_home.to_path_buf(),
        plan_id: snapshot.authority_id.clone(),
    })?;
    if report.plan.status != OperationPlanStatus::Open
        || report.plan.version != snapshot.authority_version
        || report.plan.agent_id != agent_id
        || report.plan.lane_digest.as_deref() != Some(snapshot.exact_lane_digest.as_str())
        || root_working_session_key(&report.plan.session_key)
            != root_working_session_key(working_session_key)
    {
        return Err(invalid(
            "OperationPlan authority changed before child admission",
        ));
    }
    let item_id = snapshot
        .active_item_id
        .as_deref()
        .ok_or_else(|| invalid("task snapshot has no active item"))?;
    let item = report
        .items
        .iter()
        .find(|item| item.item_id == item_id)
        .ok_or_else(|| invalid("task snapshot item is missing"))?;
    if !matches!(
        item.status,
        OperationPlanItemStatus::Running | OperationPlanItemStatus::Review
    ) || snapshot.active_item_version != Some(item.version)
        || snapshot.authority_checksum != authority_checksum(&report.plan, item)?
    {
        return Err(invalid(
            "OperationPlan item/checksum changed before child admission",
        ));
    }
    Ok(())
}

fn resolve_operation_plan_authority(
    options: &OperationPlanAuthorityOptions,
) -> io::Result<ContinuationAuthoritySnapshotV1> {
    validate_checkpoint(&options.checkpoint)?;
    if options.checkpoint.authority_kind != ContinuationAuthorityKindV1::OperationPlan {
        return Err(invalid("checkpoint authority is not an OperationPlan"));
    }
    if options.exact_lane_digest.len() != 64 || !is_lower_hex(&options.exact_lane_digest) {
        return Err(invalid("exact lane digest is unavailable or non-canonical"));
    }
    if options.virtual_session_id.trim().is_empty() {
        return Err(invalid("stable virtual session authority is unavailable"));
    }
    if options.breakers.operator_stop {
        return Err(invalid("operator stop fences task continuation"));
    }
    if options.breakers.newer_steer {
        return Err(invalid("newer steer fences the drained task slice"));
    }
    if options.breakers.budget_exhausted {
        return Err(invalid("task continuation budget is exhausted"));
    }
    if options.breakers.no_progress_exhausted {
        return Err(invalid(
            "task continuation no-progress breaker is exhausted",
        ));
    }
    if depth_exhausted(options.breakers) {
        return Err(invalid("task continuation depth breaker is exhausted"));
    }

    let report = show_operation_plan(OperationPlanShowOptions {
        harness_home: options.harness_home.clone(),
        plan_id: options.checkpoint.authority_id.clone(),
    })?;
    let plan = &report.plan;
    if plan.status != OperationPlanStatus::Open {
        return Err(invalid("OperationPlan is not open"));
    }
    if plan.agent_id != options.agent_id {
        return Err(invalid(
            "OperationPlan agent does not own the current exact lane",
        ));
    }
    if plan.lane_digest.as_deref() != Some(options.exact_lane_digest.as_str()) {
        return Err(invalid(
            "OperationPlan lane digest does not match the current slice",
        ));
    }
    if root_working_session_key(&plan.session_key)
        != root_working_session_key(&options.working_session_key)
    {
        return Err(invalid(
            "OperationPlan session root does not match the current task",
        ));
    }
    if plan.version != options.checkpoint.authority_version {
        return Err(invalid(
            "OperationPlan version changed after the checkpoint",
        ));
    }
    let item_id = options
        .checkpoint
        .active_item_id
        .as_deref()
        .ok_or_else(|| invalid("OperationPlan checkpoint has no active item"))?;
    let item = report
        .items
        .iter()
        .find(|item| item.item_id == item_id)
        .ok_or_else(|| invalid("checkpoint active item is missing"))?;
    validate_active_item(item, &options.checkpoint)?;
    let authority_checksum = authority_checksum(plan, item)?;
    Ok(ContinuationAuthoritySnapshotV1 {
        kind: ContinuationAuthorityKindV1::OperationPlan,
        authority_id: plan.plan_id.clone(),
        authority_version: plan.version,
        authority_checksum,
        exact_lane_digest: options.exact_lane_digest.clone(),
        virtual_session_id: options.virtual_session_id.clone(),
        active_item_id: Some(item.item_id.clone()),
        active_item_version: Some(item.version),
        checkpoint_digest: options.checkpoint.checkpoint_digest.clone(),
    })
}

fn validate_active_item(
    item: &OperationPlanItem,
    checkpoint: &TaskContinuationCheckpointV1,
) -> io::Result<()> {
    if !matches!(
        item.status,
        OperationPlanItemStatus::Running | OperationPlanItemStatus::Review
    ) {
        return Err(invalid("OperationPlan item is not running or in review"));
    }
    if checkpoint.active_item_version != Some(item.version) {
        return Err(invalid(
            "OperationPlan item version changed after the checkpoint",
        ));
    }
    Ok(())
}

fn validate_checkpoint(checkpoint: &TaskContinuationCheckpointV1) -> io::Result<()> {
    if checkpoint.schema != TASK_CONTINUATION_CHECKPOINT_SCHEMA {
        return Err(invalid("unsupported task continuation checkpoint schema"));
    }
    if checkpoint.authority_id.trim().is_empty() {
        return Err(invalid("checkpoint authority id is empty"));
    }
    if checkpoint.checkpoint.is_empty() || checkpoint.checkpoint.len() > MAX_CHECKPOINT_BYTES {
        return Err(invalid("checkpoint body is empty or exceeds its bound"));
    }
    if checkpoint.checkpoint_digest.len() != 64 || !is_lower_hex(&checkpoint.checkpoint_digest) {
        return Err(invalid("checkpoint digest is non-canonical"));
    }
    if sha256_hex(checkpoint.checkpoint.as_bytes()) != checkpoint.checkpoint_digest {
        return Err(invalid("checkpoint digest does not match its bounded body"));
    }
    Ok(())
}

fn authority_checksum(
    plan: &crate::operation_plan::OperationPlan,
    item: &OperationPlanItem,
) -> io::Result<String> {
    let bytes = serde_json::to_vec(&(plan, item)).map_err(io::Error::other)?;
    Ok(sha256_hex(&bytes))
}

fn depth_exhausted(breakers: TaskContinuationBreakers) -> bool {
    let max = if breakers.max_continuation_depth == 0 {
        DEFAULT_MAX_TASK_CONTINUATION_DEPTH
    } else {
        breakers.max_continuation_depth
    };
    breakers.continuation_depth >= max
}

fn is_lower_hex(value: &str) -> bool {
    value
        .as_bytes()
        .iter()
        .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    digest(&SHA256, bytes)
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;
    use crate::operation_plan::{
        CreateOperationPlanOptions, CreateOperationPlanOptionsV2, OperationPlanAddItemOptions,
        OperationPlanUpdateItemOptions, add_operation_plan_item, create_operation_plan_v2,
        update_operation_plan_item,
    };

    #[test]
    fn open_operation_plan_running_item_and_typed_checkpoint_require_continuation() {
        let (root, options) = fixture(OperationPlanItemStatus::Running);
        let working_session_key = options.working_session_key.clone();
        let evaluation = evaluate_operation_plan_drain(options);
        assert!(evaluation.schedule_continuation, "{}", evaluation.reason);
        assert!(!evaluation.allow_logical_final);
        let DrainDispositionV1::ContinuationRequired(authority) = evaluation.disposition else {
            panic!("expected continuation authority");
        };
        let continuation = crate::RuntimeContinuationMetadata {
            continuation_index: Some(1),
            campaign_slice_generation: Some(2),
            ..crate::RuntimeContinuationMetadata::legacy()
        };
        let intent = crate::commit_task_continuation_intent(
            &root,
            "queue",
            &working_session_key,
            &continuation,
            &authority,
            2,
            7,
            10,
        )
        .unwrap();
        let first =
            crate::ensure_goal_continuation_enqueued(&root, &intent.intent_key, 11).unwrap();
        let replay =
            crate::ensure_goal_continuation_enqueued(&root, &intent.intent_key, 12).unwrap();
        assert_eq!(first.child_queue_id, replay.child_queue_id);
        let pending = fs::read_to_string(
            root.join("state")
                .join("runtime-queue")
                .join("pending.jsonl"),
        )
        .unwrap();
        assert_eq!(
            pending
                .lines()
                .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
                .filter(|value| {
                    value
                        .get("continuationIntentKey")
                        .and_then(serde_json::Value::as_str)
                        == Some(intent.intent_key.as_str())
                })
                .count(),
            1
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn todo_ready_or_terminal_plan_work_does_not_auto_loop() {
        for status in [
            OperationPlanItemStatus::Todo,
            OperationPlanItemStatus::Ready,
        ] {
            let (root, options) = fixture(status);
            let evaluation = evaluate_operation_plan_drain(options);
            assert!(!evaluation.schedule_continuation, "status={status:?}");
            let _ = fs::remove_dir_all(root);
        }
    }

    #[test]
    fn wrong_lane_stale_authority_or_breakers_fail_closed() {
        let (root, options) = fixture(OperationPlanItemStatus::Running);
        let mut rejected = Vec::new();

        let mut wrong_lane = options.clone();
        wrong_lane.exact_lane_digest = "f".repeat(64);
        rejected.push(("wrong-lane", evaluate_operation_plan_drain(wrong_lane)));

        let mut wrong_agent = options.clone();
        wrong_agent.agent_id = "another-agent".to_string();
        rejected.push(("wrong-agent", evaluate_operation_plan_drain(wrong_agent)));

        let mut wrong_session = options.clone();
        wrong_session.working_session_key = "telegram:other:user:main".to_string();
        rejected.push((
            "wrong-session",
            evaluate_operation_plan_drain(wrong_session),
        ));

        let mut stale_plan = options.clone();
        stale_plan.checkpoint.authority_version =
            stale_plan.checkpoint.authority_version.saturating_sub(1);
        rejected.push(("stale-plan", evaluate_operation_plan_drain(stale_plan)));

        let mut stale_item = options.clone();
        stale_item.checkpoint.active_item_version = Some(999);
        rejected.push(("stale-item", evaluate_operation_plan_drain(stale_item)));

        let mut no_virtual_session = options.clone();
        no_virtual_session.virtual_session_id.clear();
        rejected.push((
            "missing-virtual-session",
            evaluate_operation_plan_drain(no_virtual_session),
        ));

        for (label, evaluation) in rejected {
            assert!(!evaluation.schedule_continuation, "{label}");
            assert!(!evaluation.allow_logical_final, "{label}");
            assert!(
                matches!(evaluation.disposition, DrainDispositionV1::NeedsAuthority),
                "{label}: {:?}",
                evaluation.disposition
            );
        }

        for (label, breakers) in [
            (
                "operator-stop",
                TaskContinuationBreakers {
                    operator_stop: true,
                    ..TaskContinuationBreakers::default()
                },
            ),
            (
                "newer-steer",
                TaskContinuationBreakers {
                    newer_steer: true,
                    ..TaskContinuationBreakers::default()
                },
            ),
            (
                "budget",
                TaskContinuationBreakers {
                    budget_exhausted: true,
                    ..TaskContinuationBreakers::default()
                },
            ),
            (
                "no-progress",
                TaskContinuationBreakers {
                    no_progress_exhausted: true,
                    ..TaskContinuationBreakers::default()
                },
            ),
            (
                "depth",
                TaskContinuationBreakers {
                    continuation_depth: 2,
                    max_continuation_depth: 2,
                    ..TaskContinuationBreakers::default()
                },
            ),
        ] {
            let mut fenced = options.clone();
            fenced.breakers = breakers;
            let evaluation = evaluate_operation_plan_drain(fenced);
            assert!(!evaluation.schedule_continuation, "{label}");
            assert!(!evaluation.allow_logical_final, "{label}");
            assert!(
                matches!(evaluation.disposition, DrainDispositionV1::Blocked),
                "{label}: {:?}",
                evaluation.disposition
            );
        }
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn blocked_completed_or_canceled_plan_never_auto_loops() {
        for status in ["blocked", "completed", "canceled"] {
            let (root, options) = fixture(OperationPlanItemStatus::Running);
            let plan_file = root
                .join("state")
                .join("operation-plans")
                .join("plan")
                .join("plan.json");
            let mut value: serde_json::Value =
                serde_json::from_slice(&fs::read(&plan_file).unwrap()).unwrap();
            value["status"] = serde_json::Value::String(status.to_string());
            value["version"] =
                serde_json::Value::from(value["version"].as_u64().unwrap().saturating_add(1));
            crate::write_json_atomic(&plan_file, &value).unwrap();

            let evaluation = evaluate_operation_plan_drain(options);
            assert!(!evaluation.schedule_continuation, "status={status}");
            assert!(!evaluation.allow_logical_final, "status={status}");
            let _ = fs::remove_dir_all(root);
        }
    }

    #[test]
    fn plan_change_before_child_admission_invalidates_snapshot() {
        let (root, options) = fixture(OperationPlanItemStatus::Running);
        let evaluation = evaluate_operation_plan_drain(options.clone());
        let DrainDispositionV1::ContinuationRequired(snapshot) = evaluation.disposition else {
            panic!("expected continuation authority");
        };
        update_operation_plan_item(OperationPlanUpdateItemOptions {
            harness_home: root.clone(),
            plan_id: "plan".to_string(),
            item_id: "item".to_string(),
            expected_item_version: options.checkpoint.active_item_version,
            status: Some(OperationPlanItemStatus::Review),
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
            now_ms: 10,
        })
        .unwrap();

        assert!(
            revalidate_operation_plan_snapshot(
                &root,
                &snapshot,
                &options.working_session_key,
                &options.agent_id,
            )
            .is_err()
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn ordinary_non_task_drain_remains_logically_terminal() {
        let evaluation = logical_complete_drain();
        assert!(!evaluation.schedule_continuation);
        assert!(evaluation.allow_logical_final);
        assert!(matches!(
            evaluation.disposition,
            DrainDispositionV1::LogicalComplete
        ));
    }

    #[test]
    fn marker_is_bounded_verified_and_removed_from_visible_handoff() {
        let checkpoint = checkpoint(2, 3);
        let marker = format!(
            "Work is checkpointed.\n{TASK_CONTINUATION_MARKER_OPEN}{}{TASK_CONTINUATION_MARKER_CLOSE}",
            serde_json::to_string(&checkpoint).unwrap()
        );
        let (visible, parsed) = extract_task_continuation_checkpoint(&marker).unwrap();
        assert_eq!(visible, "Work is checkpointed.");
        assert_eq!(parsed, Some(checkpoint));
    }

    fn fixture(target_status: OperationPlanItemStatus) -> (PathBuf, OperationPlanAuthorityOptions) {
        let root = std::env::temp_dir().join(format!(
            "task-transition-{}-{target_status:?}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        let session_key = crate::channel_session_key::CanonicalChannelSessionKey::bind_for_lane(
            "telegram:dm:user:main",
            "telegram",
            "default",
            "dm",
            "user",
            "main",
        )
        .unwrap()
        .canonical_string();
        let lane_digest = crate::lane::FullLaneKeyV1::new(
            "telegram",
            "default",
            "dm",
            "user",
            "main",
            "interactive",
            root_working_session_key(&session_key),
            &session_key,
        )
        .unwrap()
        .identity_hash()
        .unwrap();
        create_operation_plan_v2(CreateOperationPlanOptionsV2 {
            options: CreateOperationPlanOptions {
                harness_home: root.clone(),
                plan_id: "plan".to_string(),
                origin_queue_id: Some("queue".to_string()),
                session_key: session_key.clone(),
                agent_id: "main".to_string(),
                goal: "finish task".to_string(),
                acceptance_criteria: None,
                constraints: None,
                max_open_items: None,
                max_fanout: None,
                now_ms: 1,
            },
            lane_digest: lane_digest.clone(),
        })
        .unwrap();
        add_operation_plan_item(OperationPlanAddItemOptions {
            harness_home: root.clone(),
            plan_id: "plan".to_string(),
            item_id: "item".to_string(),
            title: "item".to_string(),
            body: "body".to_string(),
            depends_on: Vec::new(),
            acceptance_criteria: None,
            risk: None,
            now_ms: 2,
        })
        .unwrap();
        let mut item_version = 1;
        for status in match target_status {
            OperationPlanItemStatus::Todo => Vec::new(),
            OperationPlanItemStatus::Ready => vec![OperationPlanItemStatus::Ready],
            OperationPlanItemStatus::Running => vec![
                OperationPlanItemStatus::Ready,
                OperationPlanItemStatus::Running,
            ],
            OperationPlanItemStatus::Review => vec![
                OperationPlanItemStatus::Ready,
                OperationPlanItemStatus::Running,
                OperationPlanItemStatus::Review,
            ],
            _ => panic!("unsupported fixture status"),
        } {
            let updated = update_operation_plan_item(OperationPlanUpdateItemOptions {
                harness_home: root.clone(),
                plan_id: "plan".to_string(),
                item_id: "item".to_string(),
                expected_item_version: Some(item_version),
                status: Some(status),
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
                now_ms: 2 + item_version as i64,
            })
            .unwrap();
            item_version = updated.item.version;
        }
        let report = show_operation_plan(OperationPlanShowOptions {
            harness_home: root.clone(),
            plan_id: "plan".to_string(),
        })
        .unwrap();
        let queue_dir = root.join("state").join("runtime-queue");
        fs::create_dir_all(&queue_dir).unwrap();
        crate::append_jsonl_value(
            &queue_dir.join("pending.jsonl"),
            &serde_json::json!({
                "queueId": "queue",
                "status": "queued",
                "runtimeClass": "interactive",
                "origin": "channel",
                "platform": "telegram",
                "accountId": "default",
                "channelId": "dm",
                "userId": "user",
                "agentId": "main",
                "sessionKey": session_key.clone(),
                "continuationIndex": 1,
                "message": "continue the plan"
            }),
        )
        .unwrap();
        crate::append_jsonl_value(
            &queue_dir.join("execution-receipts.jsonl"),
            &serde_json::json!({
                "schema": "agent-harness.runtime-execution-receipt.v1",
                "queueId": "queue",
                "status": "prepared",
                "runtimeClass": "interactive",
                "origin": "channel",
                "executionDir": root.join("execution").display().to_string()
            }),
        )
        .unwrap();
        let checkpoint = checkpoint(report.plan.version, item_version);
        (
            root.clone(),
            OperationPlanAuthorityOptions {
                harness_home: root,
                checkpoint,
                exact_lane_digest: lane_digest,
                virtual_session_id: crate::context_rollover::derive_virtual_session_id_v2(
                    &crate::ChannelStateLane::new(
                        "telegram",
                        Some("default"),
                        "dm",
                        "user",
                        "main",
                    )
                    .unwrap(),
                    &root_working_session_key(&session_key),
                ),
                working_session_key: session_key,
                agent_id: "main".to_string(),
                breakers: TaskContinuationBreakers::default(),
            },
        )
    }

    fn checkpoint(plan_version: u64, item_version: u64) -> TaskContinuationCheckpointV1 {
        let body = "checkpoint: implementation remains active".to_string();
        TaskContinuationCheckpointV1 {
            schema: TASK_CONTINUATION_CHECKPOINT_SCHEMA.to_string(),
            authority_kind: ContinuationAuthorityKindV1::OperationPlan,
            authority_id: "plan".to_string(),
            authority_version: plan_version,
            active_item_id: Some("item".to_string()),
            active_item_version: Some(item_version),
            checkpoint_digest: sha256_hex(body.as_bytes()),
            checkpoint: body,
        }
    }
}
