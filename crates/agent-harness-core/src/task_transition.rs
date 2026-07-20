use std::fs;
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
pub const TASK_FAMILY_SCHEMA: &str = "agent-harness.task-family.v1";
pub const DRAIN_DISPOSITION_SCHEMA: &str = "agent-harness.drain-disposition.v1";
pub const TASK_CONTINUATION_MARKER_OPEN: &str = "<agent-harness-continuation-checkpoint>";
pub const TASK_CONTINUATION_MARKER_CLOSE: &str = "</agent-harness-continuation-checkpoint>";
pub const DRAIN_DISPOSITION_MARKER_OPEN: &str = "<agent-harness-drain-disposition>";
pub const DRAIN_DISPOSITION_MARKER_CLOSE: &str = "</agent-harness-drain-disposition>";
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
#[serde(rename_all = "camelCase")]
pub struct TaskFamilyV1 {
    pub schema: String,
    pub family_id: String,
    pub authority_version: u64,
    pub root_queue_id: String,
    pub exact_lane_digest: String,
    pub virtual_session_id: String,
    pub root_working_session_key: String,
    pub agent_id: String,
    #[serde(default = "default_interactive_runtime_class")]
    pub runtime_class: String,
    pub prompt_digest: String,
    pub policy_digest: String,
    #[serde(default = "default_runnable_task_family_status")]
    pub status: String,
    pub created_at_ms: i64,
    #[serde(default)]
    pub updated_at_ms: i64,
    #[serde(default)]
    pub observation_order: u64,
}

fn default_interactive_runtime_class() -> String {
    "interactive".to_string()
}

fn default_runnable_task_family_status() -> String {
    "runnable".to_string()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DrainDispositionV1 {
    LogicalComplete,
    ContinuationRequired(ContinuationAuthoritySnapshotV1),
    NeedsUser,
    NeedsAuthority,
    Blocked,
    Indeterminate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DrainDispositionKindV1 {
    LogicalComplete,
    ContinuationRequired,
    NeedsUser,
    NeedsAuthority,
    Blocked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DrainDispositionMarkerV1 {
    pub schema: String,
    pub disposition: DrainDispositionKindV1,
    pub observed_deadline_generation: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority_kind: Option<ContinuationAuthorityKindV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority_version: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_family_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_family_version: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_item_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_item_version: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub question: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_hint: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub completion_evidence_digests: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DrainDispositionCaptureV1 {
    Missing,
    Valid(DrainDispositionMarkerV1),
    Invalid { reason_code: String },
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplicitCheckpointAuthorityOptions {
    pub harness_home: PathBuf,
    pub checkpoint: TaskContinuationCheckpointV1,
    pub expected_family_id: String,
    pub expected_root_queue_id: String,
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

pub fn extract_drain_disposition_marker(
    assistant_text: &str,
) -> (String, DrainDispositionCaptureV1) {
    let open_count = assistant_text
        .matches(DRAIN_DISPOSITION_MARKER_OPEN)
        .count();
    let close_count = assistant_text
        .matches(DRAIN_DISPOSITION_MARKER_CLOSE)
        .count();
    if open_count == 0 && close_count == 0 {
        return (
            assistant_text.to_string(),
            DrainDispositionCaptureV1::Missing,
        );
    }
    let open_at = assistant_text.find(DRAIN_DISPOSITION_MARKER_OPEN);
    let close_at = assistant_text.rfind(DRAIN_DISPOSITION_MARKER_CLOSE);
    let visible = match (open_at, close_at) {
        (Some(open), Some(close)) if close >= open => format!(
            "{}{}",
            &assistant_text[..open],
            &assistant_text[close + DRAIN_DISPOSITION_MARKER_CLOSE.len()..]
        )
        .trim()
        .to_string(),
        (Some(open), _) => assistant_text[..open].trim().to_string(),
        (_, Some(close)) => format!(
            "{}{}",
            &assistant_text[..close],
            &assistant_text[close + DRAIN_DISPOSITION_MARKER_CLOSE.len()..]
        )
        .trim()
        .to_string(),
        _ => assistant_text.to_string(),
    };
    if open_count != 1 || close_count != 1 {
        return (
            visible,
            DrainDispositionCaptureV1::Invalid {
                reason_code: "multiple-or-unbalanced-disposition-markers".to_string(),
            },
        );
    }
    let Some(open_at) = open_at else {
        return (
            visible,
            DrainDispositionCaptureV1::Invalid {
                reason_code: "disposition-marker-close-without-open".to_string(),
            },
        );
    };
    let json_start = open_at + DRAIN_DISPOSITION_MARKER_OPEN.len();
    let Some(close_at) = assistant_text[json_start..]
        .find(DRAIN_DISPOSITION_MARKER_CLOSE)
        .map(|relative| json_start + relative)
    else {
        return (
            visible,
            DrainDispositionCaptureV1::Invalid {
                reason_code: "disposition-marker-not-closed".to_string(),
            },
        );
    };
    if close_at.saturating_sub(json_start) > MAX_MARKER_BYTES {
        return (
            visible,
            DrainDispositionCaptureV1::Invalid {
                reason_code: "disposition-marker-oversized".to_string(),
            },
        );
    }
    let marker = match serde_json::from_str::<DrainDispositionMarkerV1>(
        assistant_text[json_start..close_at].trim(),
    ) {
        Ok(marker) => marker,
        Err(_) => {
            return (
                visible,
                DrainDispositionCaptureV1::Invalid {
                    reason_code: "disposition-marker-invalid-json".to_string(),
                },
            );
        }
    };
    match validate_drain_disposition_marker(&marker) {
        Ok(()) => (visible, DrainDispositionCaptureV1::Valid(marker)),
        Err(error) => (
            visible,
            DrainDispositionCaptureV1::Invalid {
                reason_code: error.to_string(),
            },
        ),
    }
}

pub fn drain_marker_checkpoint(
    marker: &DrainDispositionMarkerV1,
) -> io::Result<TaskContinuationCheckpointV1> {
    if marker.disposition != DrainDispositionKindV1::ContinuationRequired {
        return Err(invalid("drain disposition is not continuation-required"));
    }
    let checkpoint = TaskContinuationCheckpointV1 {
        schema: TASK_CONTINUATION_CHECKPOINT_SCHEMA.to_string(),
        authority_kind: marker
            .authority_kind
            .ok_or_else(|| invalid("continuation disposition has no authority kind"))?,
        authority_id: marker
            .authority_id
            .clone()
            .ok_or_else(|| invalid("continuation disposition has no authority id"))?,
        authority_version: marker
            .authority_version
            .ok_or_else(|| invalid("continuation disposition has no authority version"))?,
        active_item_id: marker.active_item_id.clone(),
        active_item_version: marker.active_item_version,
        checkpoint: marker
            .checkpoint
            .clone()
            .ok_or_else(|| invalid("continuation disposition has no checkpoint"))?,
        checkpoint_digest: marker
            .checkpoint_digest
            .clone()
            .ok_or_else(|| invalid("continuation disposition has no checkpoint digest"))?,
    };
    validate_checkpoint(&checkpoint)?;
    Ok(checkpoint)
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

pub fn ensure_task_family(
    harness_home: &Path,
    root_queue_id: &str,
    exact_lane_digest: &str,
    virtual_session_id: &str,
    working_session_key: &str,
    agent_id: &str,
    prompt_digest: &str,
) -> io::Result<TaskFamilyV1> {
    for (name, value) in [
        ("root queue id", root_queue_id),
        ("exact lane digest", exact_lane_digest),
        ("virtual session id", virtual_session_id),
        ("working session key", working_session_key),
        ("agent id", agent_id),
        ("prompt digest", prompt_digest),
    ] {
        if value.trim().is_empty() || value.chars().any(char::is_control) {
            return Err(invalid(format!("task family {name} is unavailable")));
        }
    }
    if exact_lane_digest.len() != 64 || !is_lower_hex(exact_lane_digest) {
        return Err(invalid("task family exact lane digest is non-canonical"));
    }
    let root_working_session_key = root_working_session_key(working_session_key);
    let policy_digest = sha256_hex(
        format!("task-family-policy-v1\0max-depth={DEFAULT_MAX_TASK_CONTINUATION_DEPTH}")
            .as_bytes(),
    );
    let family_id = sha256_hex(
        format!(
            "task-family-v1\0{exact_lane_digest}\0{virtual_session_id}\0{root_queue_id}\0{prompt_digest}"
        )
        .as_bytes(),
    );
    let now_ms = crate::current_log_time_ms()?;
    let candidate = TaskFamilyV1 {
        schema: TASK_FAMILY_SCHEMA.to_string(),
        family_id: family_id.clone(),
        authority_version: 1,
        root_queue_id: root_queue_id.to_string(),
        exact_lane_digest: exact_lane_digest.to_string(),
        virtual_session_id: virtual_session_id.to_string(),
        root_working_session_key,
        agent_id: agent_id.to_string(),
        runtime_class: "interactive".to_string(),
        prompt_digest: prompt_digest.to_string(),
        policy_digest,
        status: "runnable".to_string(),
        created_at_ms: now_ms,
        updated_at_ms: now_ms,
        observation_order: 1,
    };
    let file = task_family_file(harness_home, &family_id);
    if file.is_file() {
        let existing: TaskFamilyV1 =
            serde_json::from_slice(&fs::read(&file)?).map_err(io::Error::other)?;
        if task_family_identity(&existing) != task_family_identity(&candidate) {
            return Err(invalid(
                "existing task family authority does not match exact identity",
            ));
        }
        return Ok(existing);
    }
    crate::write_json_atomic(&file, &candidate)?;
    Ok(candidate)
}

pub fn find_task_family_for_root_queue(
    harness_home: &Path,
    root_queue_id: &str,
) -> io::Result<Option<TaskFamilyV1>> {
    if root_queue_id.trim().is_empty() {
        return Ok(None);
    }
    let dir = harness_home
        .join("state")
        .join("runtime-queue")
        .join("task-families");
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let mut found = None;
    for entry in entries {
        let path = entry?.path();
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        let family: TaskFamilyV1 =
            serde_json::from_slice(&fs::read(path)?).map_err(io::Error::other)?;
        if family.schema != TASK_FAMILY_SCHEMA || family.root_queue_id != root_queue_id {
            continue;
        }
        if found.is_some() {
            return Err(invalid(
                "multiple task families claim the same root queue authority",
            ));
        }
        found = Some(family);
    }
    Ok(found)
}

pub fn evaluate_explicit_checkpoint_drain(
    options: ExplicitCheckpointAuthorityOptions,
) -> TaskDrainEvaluationV1 {
    match resolve_explicit_checkpoint_authority(&options) {
        Ok(snapshot) => TaskDrainEvaluationV1 {
            schema: TASK_TRANSITION_SCHEMA.to_string(),
            disposition: DrainDispositionV1::ContinuationRequired(snapshot),
            schedule_continuation: true,
            allow_logical_final: false,
            reason: "deadline drain has exact harness-owned task-family authority and a typed checkpoint"
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

pub fn revalidate_continuation_snapshot(
    harness_home: &Path,
    snapshot: &ContinuationAuthoritySnapshotV1,
    working_session_key: &str,
    agent_id: &str,
) -> io::Result<()> {
    match snapshot.kind {
        ContinuationAuthorityKindV1::OperationPlan => revalidate_operation_plan_snapshot(
            harness_home,
            snapshot,
            working_session_key,
            agent_id,
        ),
        ContinuationAuthorityKindV1::ExplicitCheckpoint => revalidate_explicit_checkpoint_snapshot(
            harness_home,
            snapshot,
            working_session_key,
            agent_id,
        ),
        ContinuationAuthorityKindV1::Goal => {
            Err(invalid("task snapshot cannot use Goal authority"))
        }
    }
}

pub fn revalidate_explicit_checkpoint_snapshot(
    harness_home: &Path,
    snapshot: &ContinuationAuthoritySnapshotV1,
    working_session_key: &str,
    agent_id: &str,
) -> io::Result<()> {
    if snapshot.kind != ContinuationAuthorityKindV1::ExplicitCheckpoint {
        return Err(invalid("task snapshot is not ExplicitCheckpoint authority"));
    }
    let family = read_task_family(harness_home, &snapshot.authority_id)?;
    if family.authority_version != snapshot.authority_version
        || family.agent_id != agent_id
        || family.exact_lane_digest != snapshot.exact_lane_digest
        || family.virtual_session_id != snapshot.virtual_session_id
        || family.root_working_session_key != root_working_session_key(working_session_key)
        || family.runtime_class != "interactive"
        || family.status != "runnable"
        || task_family_checksum(&family)? != snapshot.authority_checksum
    {
        return Err(invalid(
            "ExplicitCheckpoint task-family authority changed before child admission",
        ));
    }
    Ok(())
}

fn resolve_explicit_checkpoint_authority(
    options: &ExplicitCheckpointAuthorityOptions,
) -> io::Result<ContinuationAuthoritySnapshotV1> {
    validate_checkpoint(&options.checkpoint)?;
    if options.checkpoint.authority_kind != ContinuationAuthorityKindV1::ExplicitCheckpoint {
        return Err(invalid("checkpoint authority is not ExplicitCheckpoint"));
    }
    if options.checkpoint.authority_id != options.expected_family_id {
        return Err(invalid(
            "checkpoint did not name the harness-owned task family",
        ));
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
    let family = read_task_family(&options.harness_home, &options.expected_family_id)?;
    if family.authority_version != options.checkpoint.authority_version
        || family.root_queue_id != options.expected_root_queue_id
        || family.agent_id != options.agent_id
        || family.exact_lane_digest != options.exact_lane_digest
        || family.virtual_session_id != options.virtual_session_id
        || family.root_working_session_key != root_working_session_key(&options.working_session_key)
        || family.runtime_class != "interactive"
        || family.status != "runnable"
    {
        return Err(invalid(
            "task family does not own the current exact lane/slice",
        ));
    }
    if options.checkpoint.active_item_id.is_some()
        || options.checkpoint.active_item_version.is_some()
    {
        return Err(invalid(
            "ExplicitCheckpoint authority cannot claim an OperationPlan item",
        ));
    }
    Ok(ContinuationAuthoritySnapshotV1 {
        kind: ContinuationAuthorityKindV1::ExplicitCheckpoint,
        authority_id: family.family_id.clone(),
        authority_version: family.authority_version,
        authority_checksum: task_family_checksum(&family)?,
        exact_lane_digest: family.exact_lane_digest.clone(),
        virtual_session_id: family.virtual_session_id.clone(),
        active_item_id: None,
        active_item_version: None,
        checkpoint_digest: options.checkpoint.checkpoint_digest.clone(),
    })
}

fn task_family_file(harness_home: &Path, family_id: &str) -> PathBuf {
    harness_home
        .join("state")
        .join("runtime-queue")
        .join("task-families")
        .join(format!("{family_id}.json"))
}

pub(crate) fn read_task_family(harness_home: &Path, family_id: &str) -> io::Result<TaskFamilyV1> {
    if family_id.len() != 64 || !is_lower_hex(family_id) {
        return Err(invalid("task family id is non-canonical"));
    }
    let family: TaskFamilyV1 =
        serde_json::from_slice(&fs::read(task_family_file(harness_home, family_id))?)
            .map_err(io::Error::other)?;
    if family.schema != TASK_FAMILY_SCHEMA
        || family.family_id != family_id
        || family.runtime_class != "interactive"
        || family.status != "runnable"
    {
        return Err(invalid("task family file has mismatched identity/schema"));
    }
    Ok(family)
}

pub fn task_family_checksum(family: &TaskFamilyV1) -> io::Result<String> {
    Ok(sha256_hex(
        &serde_json::to_vec(family).map_err(io::Error::other)?,
    ))
}

fn task_family_identity(
    family: &TaskFamilyV1,
) -> (
    &str,
    u64,
    &str,
    &str,
    &str,
    &str,
    &str,
    &str,
    &str,
    &str,
    &str,
) {
    (
        &family.schema,
        family.authority_version,
        &family.root_queue_id,
        &family.exact_lane_digest,
        &family.virtual_session_id,
        &family.root_working_session_key,
        &family.agent_id,
        &family.runtime_class,
        &family.prompt_digest,
        &family.policy_digest,
        &family.status,
    )
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

fn validate_drain_disposition_marker(marker: &DrainDispositionMarkerV1) -> io::Result<()> {
    if marker.schema != DRAIN_DISPOSITION_SCHEMA {
        return Err(invalid("unsupported-drain-disposition-schema"));
    }
    for value in [
        marker.reason_code.as_deref(),
        marker.question.as_deref(),
        marker.recovery_hint.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        if value.is_empty() || value.len() > 512 || value.chars().any(char::is_control) {
            return Err(invalid("disposition-bounded-text-invalid"));
        }
    }
    if marker.completion_evidence_digests.len() > 16
        || marker
            .completion_evidence_digests
            .iter()
            .any(|digest| digest.len() != 64 || !is_lower_hex(digest))
    {
        return Err(invalid("completion-evidence-digest-invalid"));
    }
    match marker.disposition {
        DrainDispositionKindV1::ContinuationRequired => {
            let checkpoint = drain_marker_checkpoint(marker)?;
            if checkpoint.authority_kind == ContinuationAuthorityKindV1::ExplicitCheckpoint
                && (marker.task_family_id.as_deref() != Some(checkpoint.authority_id.as_str())
                    || marker.task_family_version != Some(checkpoint.authority_version))
            {
                return Err(invalid("explicit-checkpoint-task-family-mismatch"));
            }
        }
        DrainDispositionKindV1::LogicalComplete => {
            if marker.authority_kind.is_some()
                || marker.authority_id.is_some()
                || marker.authority_version.is_some()
                || marker.checkpoint.is_some()
                || marker.checkpoint_digest.is_some()
            {
                return Err(invalid(
                    "logical-complete-cannot-claim-continuation-authority",
                ));
            }
        }
        DrainDispositionKindV1::NeedsUser => {
            if marker.question.is_none() || marker.reason_code.is_none() {
                return Err(invalid("needs-user-requires-question-and-reason"));
            }
        }
        DrainDispositionKindV1::NeedsAuthority | DrainDispositionKindV1::Blocked => {
            if marker.reason_code.is_none() {
                return Err(invalid("parked-disposition-requires-reason"));
            }
        }
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
            false,
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

    #[test]
    fn harness_owned_task_family_authorizes_exact_explicit_checkpoint_once() {
        let (root, operation) = fixture(OperationPlanItemStatus::Running);
        let family = ensure_task_family(
            &root,
            "root-queue",
            &operation.exact_lane_digest,
            &operation.virtual_session_id,
            &operation.working_session_key,
            &operation.agent_id,
            &sha256_hex(b"root prompt"),
        )
        .unwrap();
        let body = "bounded ordinary-task checkpoint".to_string();
        let checkpoint = TaskContinuationCheckpointV1 {
            schema: TASK_CONTINUATION_CHECKPOINT_SCHEMA.to_string(),
            authority_kind: ContinuationAuthorityKindV1::ExplicitCheckpoint,
            authority_id: family.family_id.clone(),
            authority_version: family.authority_version,
            active_item_id: None,
            active_item_version: None,
            checkpoint_digest: sha256_hex(body.as_bytes()),
            checkpoint: body,
        };
        let evaluation = evaluate_explicit_checkpoint_drain(ExplicitCheckpointAuthorityOptions {
            harness_home: root.clone(),
            checkpoint,
            expected_family_id: family.family_id.clone(),
            expected_root_queue_id: family.root_queue_id.clone(),
            exact_lane_digest: operation.exact_lane_digest.clone(),
            virtual_session_id: operation.virtual_session_id.clone(),
            working_session_key: operation.working_session_key.clone(),
            agent_id: operation.agent_id.clone(),
            breakers: TaskContinuationBreakers::default(),
        });
        let DrainDispositionV1::ContinuationRequired(snapshot) = evaluation.disposition else {
            panic!("explicit checkpoint should schedule one exact task child");
        };
        assert!(evaluation.schedule_continuation);
        assert!(!evaluation.allow_logical_final);
        revalidate_continuation_snapshot(
            &root,
            &snapshot,
            &operation.working_session_key,
            &operation.agent_id,
        )
        .unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn explicit_checkpoint_rejects_wrong_root_or_breaker() {
        let (root, operation) = fixture(OperationPlanItemStatus::Running);
        let family = ensure_task_family(
            &root,
            "root-queue",
            &operation.exact_lane_digest,
            &operation.virtual_session_id,
            &operation.working_session_key,
            &operation.agent_id,
            &sha256_hex(b"root prompt"),
        )
        .unwrap();
        let body = "checkpoint".to_string();
        let checkpoint = TaskContinuationCheckpointV1 {
            schema: TASK_CONTINUATION_CHECKPOINT_SCHEMA.to_string(),
            authority_kind: ContinuationAuthorityKindV1::ExplicitCheckpoint,
            authority_id: family.family_id.clone(),
            authority_version: 1,
            active_item_id: None,
            active_item_version: None,
            checkpoint_digest: sha256_hex(body.as_bytes()),
            checkpoint: body,
        };
        for (root_queue, stopped) in [("wrong-root", false), ("root-queue", true)] {
            let evaluation =
                evaluate_explicit_checkpoint_drain(ExplicitCheckpointAuthorityOptions {
                    harness_home: root.clone(),
                    checkpoint: checkpoint.clone(),
                    expected_family_id: family.family_id.clone(),
                    expected_root_queue_id: root_queue.to_string(),
                    exact_lane_digest: operation.exact_lane_digest.clone(),
                    virtual_session_id: operation.virtual_session_id.clone(),
                    working_session_key: operation.working_session_key.clone(),
                    agent_id: operation.agent_id.clone(),
                    breakers: TaskContinuationBreakers {
                        operator_stop: stopped,
                        ..TaskContinuationBreakers::default()
                    },
                });
            assert!(!evaluation.schedule_continuation);
            assert!(!evaluation.allow_logical_final);
        }
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn typed_drain_disposition_strips_control_data_and_preserves_exact_authority() {
        let checkpoint = "bounded remaining work";
        let family_id = "a".repeat(64);
        let marker = DrainDispositionMarkerV1 {
            schema: DRAIN_DISPOSITION_SCHEMA.to_string(),
            disposition: DrainDispositionKindV1::ContinuationRequired,
            observed_deadline_generation: 2,
            authority_kind: Some(ContinuationAuthorityKindV1::ExplicitCheckpoint),
            authority_id: Some(family_id.clone()),
            authority_version: Some(3),
            task_family_id: Some(family_id.clone()),
            task_family_version: Some(3),
            active_item_id: None,
            active_item_version: None,
            checkpoint: Some(checkpoint.to_string()),
            checkpoint_digest: Some(sha256_hex(checkpoint.as_bytes())),
            reason_code: None,
            question: None,
            recovery_hint: None,
            completion_evidence_digests: Vec::new(),
        };
        let assistant = format!(
            "Visible handoff\n{DRAIN_DISPOSITION_MARKER_OPEN}{}{DRAIN_DISPOSITION_MARKER_CLOSE}",
            serde_json::to_string(&marker).unwrap()
        );
        let (visible, capture) = extract_drain_disposition_marker(&assistant);
        assert_eq!(visible, "Visible handoff");
        let DrainDispositionCaptureV1::Valid(parsed) = capture else {
            panic!("valid typed disposition must be captured");
        };
        assert_eq!(parsed, marker);
        let checkpoint = drain_marker_checkpoint(&parsed).unwrap();
        assert_eq!(checkpoint.authority_id, family_id);
        assert_eq!(checkpoint.authority_version, 3);
    }

    #[test]
    fn typed_drain_disposition_rejects_authority_on_logical_completion() {
        let marker = serde_json::json!({
            "schema": DRAIN_DISPOSITION_SCHEMA,
            "disposition": "logical-complete",
            "observedDeadlineGeneration": 0,
            "authorityKind": "explicit-checkpoint",
            "authorityId": "a".repeat(64),
            "authorityVersion": 1,
            "completionEvidenceDigests": []
        });
        let assistant = format!(
            "Visible\n{DRAIN_DISPOSITION_MARKER_OPEN}{marker}{DRAIN_DISPOSITION_MARKER_CLOSE}"
        );
        let (visible, capture) = extract_drain_disposition_marker(&assistant);
        assert_eq!(visible, "Visible");
        assert!(matches!(capture, DrainDispositionCaptureV1::Invalid { .. }));
    }

    #[test]
    fn malformed_drain_disposition_is_hidden_and_fail_closed() {
        let assistant = format!(
            "Visible\n{DRAIN_DISPOSITION_MARKER_OPEN}{{bad json{DRAIN_DISPOSITION_MARKER_CLOSE}"
        );
        let (visible, capture) = extract_drain_disposition_marker(&assistant);
        assert_eq!(visible, "Visible");
        assert_eq!(
            capture,
            DrainDispositionCaptureV1::Invalid {
                reason_code: "disposition-marker-invalid-json".to_string()
            }
        );
    }

    #[test]
    fn duplicate_and_oversized_drain_dispositions_are_hidden_and_fail_closed() {
        let valid = serde_json::json!({
            "schema": DRAIN_DISPOSITION_SCHEMA,
            "disposition": "needs-user",
            "observedDeadlineGeneration": 1,
            "reasonCode": "missing-input",
            "question": "Which target should be used?"
        });
        let duplicate = format!(
            "Visible\n{DRAIN_DISPOSITION_MARKER_OPEN}{valid}{DRAIN_DISPOSITION_MARKER_CLOSE}\n{DRAIN_DISPOSITION_MARKER_OPEN}{valid}{DRAIN_DISPOSITION_MARKER_CLOSE}"
        );
        let (visible, capture) = extract_drain_disposition_marker(&duplicate);
        assert_eq!(visible, "Visible");
        assert_eq!(
            capture,
            DrainDispositionCaptureV1::Invalid {
                reason_code: "multiple-or-unbalanced-disposition-markers".to_string()
            }
        );

        let oversized = format!(
            "Visible\n{DRAIN_DISPOSITION_MARKER_OPEN}{}{DRAIN_DISPOSITION_MARKER_CLOSE}",
            "x".repeat(MAX_MARKER_BYTES + 1)
        );
        let (visible, capture) = extract_drain_disposition_marker(&oversized);
        assert_eq!(visible, "Visible");
        assert_eq!(
            capture,
            DrainDispositionCaptureV1::Invalid {
                reason_code: "disposition-marker-oversized".to_string()
            }
        );
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
