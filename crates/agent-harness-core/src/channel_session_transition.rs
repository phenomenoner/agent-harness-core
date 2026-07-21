use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use ring::digest;
use serde::{Deserialize, Serialize};

use crate::append_jsonl_value_once_by_event_key;
use crate::goal_closure::{
    GoalClosureAuthorityV1, GoalClosureDispositionV1, GoalClosureIntentV1, GoalClosurePhaseInputV1,
    GoalClosurePhaseV1, GoalClosureResultV1, GoalClosureTargetCandidateV1,
    GoalClosureTargetResolutionV1, GoalClosureTriggerV1, goal_closure_intents_file,
    goal_closure_receipts_for_id, record_goal_closure_intent, record_goal_closure_phase,
    resolve_goal_closure_target,
};
use crate::goal_lineage::{
    GoalLineageDoctorOptions, GoalLineageDoctorStatus, run_goal_lineage_doctor,
};

pub const CHANNEL_SESSION_TRANSITION_INTENT_SCHEMA: &str =
    "agent-harness.channel-session-transition-intent.v1";
pub const CHANNEL_SESSION_TRANSITION_RECEIPT_SCHEMA: &str =
    "agent-harness.channel-session-transition-receipt.v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ChannelSessionTransitionCommandV1 {
    Stop,
    New,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ChannelGoalBoundaryV1 {
    NotApplicable,
    ClosureProven,
    ClosurePending,
    Ambiguous,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ChannelSessionTransitionPhaseV1 {
    IntentRecorded,
    GoalClosurePending,
    BoundaryReady,
    RetryPending,
    BoundaryCommitted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelSessionAdmissionV1 {
    Open,
    HoldPendingNew,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrepareChannelSessionTransitionOptionsV1 {
    pub harness_home: PathBuf,
    pub command: ChannelSessionTransitionCommandV1,
    pub lane_digest: String,
    pub old_session_key: String,
    pub proposed_new_session_key: Option<String>,
    pub topic: Option<String>,
    pub reason: Option<String>,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelSessionTransitionIntentV1 {
    pub schema: String,
    pub event_key: String,
    pub transition_id: String,
    pub command_effect_identity: String,
    pub command: ChannelSessionTransitionCommandV1,
    pub lane_digest: String,
    pub old_session_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frozen_new_session_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub goal_boundary: ChannelGoalBoundaryV1,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub goal_closure_id: Option<String>,
    pub goal_evidence_digest: String,
    pub requested_at_ms: i64,
    pub intent_checksum: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelSessionTransitionReceiptV1 {
    pub schema: String,
    pub event_key: String,
    pub transition_id: String,
    pub command_effect_identity: String,
    pub lane_digest: String,
    pub old_session_key_digest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frozen_new_session_key_digest: Option<String>,
    pub phase: ChannelSessionTransitionPhaseV1,
    pub goal_boundary: ChannelGoalBoundaryV1,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub goal_closure_id: Option<String>,
    pub evidence_digest: String,
    pub recorded_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedChannelSessionTransitionV1 {
    pub intent: ChannelSessionTransitionIntentV1,
    pub replayed: bool,
    pub boundary_ready: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelSessionTransitionClosureMaterialV1 {
    pub transition: ChannelSessionTransitionIntentV1,
    pub goal_closure_intent: GoalClosureIntentV1,
    pub resolution: GoalClosureTargetResolutionV1,
}

impl ChannelSessionTransitionIntentV1 {
    pub fn boundary_is_ready(&self) -> bool {
        matches!(
            self.goal_boundary,
            ChannelGoalBoundaryV1::NotApplicable | ChannelGoalBoundaryV1::ClosureProven
        )
    }
}

pub fn channel_session_transition_intents_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("channel-session-transitions")
        .join("protected-intents.jsonl")
}

pub fn channel_session_transition_receipts_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("channel-session-transitions")
        .join("receipts.jsonl")
}

pub fn prepare_channel_session_transition(
    options: PrepareChannelSessionTransitionOptionsV1,
) -> io::Result<PreparedChannelSessionTransitionV1> {
    validate_options(&options)?;
    let transition_id = transition_identity(
        options.command,
        &options.lane_digest,
        &options.old_session_key,
    )?;
    let file = channel_session_transition_intents_file(&options.harness_home);
    if let Some(intent) = read_intents(&file)?
        .into_iter()
        .find(|intent| intent.transition_id == transition_id)
    {
        validate_intent(&intent)?;
        let boundary_ready = durable_goal_boundary_is_ready(&options.harness_home, &intent)?;
        return Ok(PreparedChannelSessionTransitionV1 {
            intent,
            replayed: true,
            boundary_ready,
        });
    }

    let assessment = assess_goal_boundary(
        &options.harness_home,
        options.command,
        &options.lane_digest,
        &options.old_session_key,
        &transition_id,
        options.now_ms,
    )?;
    let frozen_new_session_key = match options.command {
        ChannelSessionTransitionCommandV1::Stop => None,
        ChannelSessionTransitionCommandV1::New => options.proposed_new_session_key.clone(),
    };
    let mut intent = ChannelSessionTransitionIntentV1 {
        schema: CHANNEL_SESSION_TRANSITION_INTENT_SCHEMA.to_string(),
        event_key: transition_id.clone(),
        transition_id: transition_id.clone(),
        command_effect_identity: transition_id,
        command: options.command,
        lane_digest: options.lane_digest,
        old_session_key: options.old_session_key,
        frozen_new_session_key,
        topic: options.topic,
        reason: options.reason,
        goal_boundary: assessment.goal_boundary,
        goal_closure_id: assessment.goal_closure_id,
        goal_evidence_digest: assessment.goal_evidence_digest,
        requested_at_ms: options.now_ms,
        intent_checksum: String::new(),
    };
    intent.intent_checksum = checksum_json(&IntentChecksumPayload::from(&intent))?;
    let appended = append_jsonl_value_once_by_event_key(&file, &intent)?;
    let stored = read_intents(&file)?
        .into_iter()
        .find(|candidate| candidate.transition_id == intent.transition_id)
        .ok_or_else(|| invalid_data("session transition intent append was not durable"))?;
    validate_intent(&stored)?;
    if stored.intent_checksum != intent.intent_checksum {
        return Err(invalid_data(
            "session transition identity already has different protected intent evidence",
        ));
    }
    record_channel_session_transition_phase(
        &options.harness_home,
        &stored,
        ChannelSessionTransitionPhaseV1::IntentRecorded,
        &stored.goal_evidence_digest,
        options.now_ms,
    )?;
    let next_phase = if stored.boundary_is_ready() {
        ChannelSessionTransitionPhaseV1::BoundaryReady
    } else {
        ChannelSessionTransitionPhaseV1::GoalClosurePending
    };
    record_channel_session_transition_phase(
        &options.harness_home,
        &stored,
        next_phase,
        &stored.goal_evidence_digest,
        options.now_ms,
    )?;
    Ok(PreparedChannelSessionTransitionV1 {
        boundary_ready: stored.boundary_is_ready(),
        intent: stored,
        replayed: !appended,
    })
}

fn durable_goal_boundary_is_ready(
    harness_home: &Path,
    intent: &ChannelSessionTransitionIntentV1,
) -> io::Result<bool> {
    if intent.boundary_is_ready() {
        return Ok(true);
    }
    let Some(closure_id) = intent.goal_closure_id.as_deref() else {
        return Ok(false);
    };
    Ok(goal_closure_receipts_for_id(harness_home, closure_id)?
        .iter()
        .any(|receipt| {
            receipt.phase == GoalClosurePhaseV1::Completed
                && receipt.result == GoalClosureResultV1::Succeeded
        }))
}

pub fn channel_session_transition_boundary_is_ready(
    harness_home: impl AsRef<Path>,
    intent: &ChannelSessionTransitionIntentV1,
) -> io::Result<bool> {
    validate_intent(intent)?;
    durable_goal_boundary_is_ready(harness_home.as_ref(), intent)
}

pub fn pending_channel_session_transition_intents(
    harness_home: impl AsRef<Path>,
    max_transitions: usize,
) -> io::Result<Vec<ChannelSessionTransitionIntentV1>> {
    if max_transitions == 0 {
        return Err(invalid_input(
            "session transition reconciliation limit must be positive",
        ));
    }
    let harness_home = harness_home.as_ref();
    let receipts = read_receipts(&channel_session_transition_receipts_file(harness_home))?;
    let mut intents = read_intents(&channel_session_transition_intents_file(harness_home))?;
    for intent in &intents {
        validate_intent(intent)?;
    }
    intents.sort_by(|left, right| {
        left.requested_at_ms
            .cmp(&right.requested_at_ms)
            .then_with(|| left.transition_id.cmp(&right.transition_id))
    });
    intents.retain(|intent| {
        !receipts.iter().any(|receipt| {
            receipt.transition_id == intent.transition_id
                && receipt.phase == ChannelSessionTransitionPhaseV1::BoundaryCommitted
        })
    });
    intents.truncate(max_transitions);
    Ok(intents)
}

pub fn channel_session_transition_closure_material(
    harness_home: impl AsRef<Path>,
    transition: &ChannelSessionTransitionIntentV1,
) -> io::Result<Option<ChannelSessionTransitionClosureMaterialV1>> {
    validate_intent(transition)?;
    let Some(closure_id) = transition.goal_closure_id.as_deref() else {
        return Ok(None);
    };
    let intents = read_jsonl::<GoalClosureIntentV1>(
        &goal_closure_intents_file(harness_home),
        "goal closure intent",
    )?;
    let mut matches = intents
        .into_iter()
        .filter(|intent| intent.closure_id == closure_id)
        .collect::<Vec<_>>();
    if matches.len() != 1 {
        return Err(invalid_data(format!(
            "session transition closure `{closure_id}` resolved {} protected intents",
            matches.len()
        )));
    }
    let intent = matches.remove(0);
    intent.validate()?;
    if intent.authority.lane_digest != transition.lane_digest
        || intent.authority.concrete_session_key != transition.old_session_key
    {
        return Err(invalid_data(
            "session transition closure authority does not match the frozen old lane",
        ));
    }
    let candidate = GoalClosureTargetCandidateV1 {
        authority: intent.authority.clone(),
        goal_identity: intent.goal_identity.clone(),
        goal_generation: intent.goal_generation.clone(),
        projection_checksum: intent
            .expected_projection_checksum
            .clone()
            .ok_or_else(|| invalid_data("channel goal closure has no expected projection"))?,
        active: true,
        original_binding: true,
        latest_authoritative_projection: true,
        latest_authoritative_lineage: true,
    };
    let resolution = resolve_goal_closure_target(&intent, &[candidate]);
    Ok(Some(ChannelSessionTransitionClosureMaterialV1 {
        transition: transition.clone(),
        goal_closure_intent: intent,
        resolution,
    }))
}

pub fn record_channel_session_transition_phase(
    harness_home: impl AsRef<Path>,
    intent: &ChannelSessionTransitionIntentV1,
    phase: ChannelSessionTransitionPhaseV1,
    evidence: &str,
    recorded_at_ms: i64,
) -> io::Result<ChannelSessionTransitionReceiptV1> {
    validate_intent(intent)?;
    let harness_home = harness_home.as_ref();
    if evidence.trim().is_empty() {
        return Err(invalid_input(
            "session transition phase evidence must not be empty",
        ));
    }
    let evidence_digest = checksum_json(&evidence)?;
    let event_key = checksum_json(&(
        intent.transition_id.as_str(),
        phase,
        evidence_digest.as_str(),
    ))?;
    let goal_boundary = if phase == ChannelSessionTransitionPhaseV1::BoundaryCommitted
        && intent.goal_boundary == ChannelGoalBoundaryV1::ClosurePending
        && durable_goal_boundary_is_ready(harness_home, intent)?
    {
        ChannelGoalBoundaryV1::ClosureProven
    } else {
        intent.goal_boundary
    };
    let receipt = ChannelSessionTransitionReceiptV1 {
        schema: CHANNEL_SESSION_TRANSITION_RECEIPT_SCHEMA.to_string(),
        event_key,
        transition_id: intent.transition_id.clone(),
        command_effect_identity: intent.command_effect_identity.clone(),
        lane_digest: intent.lane_digest.clone(),
        old_session_key_digest: checksum_json(&intent.old_session_key)?,
        frozen_new_session_key_digest: intent
            .frozen_new_session_key
            .as_ref()
            .map(checksum_json)
            .transpose()?,
        phase,
        goal_boundary,
        goal_closure_id: intent.goal_closure_id.clone(),
        evidence_digest,
        recorded_at_ms,
    };
    let file = channel_session_transition_receipts_file(harness_home);
    append_jsonl_value_once_by_event_key(&file, &receipt)?;
    Ok(receipt)
}

pub fn channel_session_transition_admission(
    harness_home: impl AsRef<Path>,
    lane_digest: &str,
    active_session_key: &str,
) -> io::Result<ChannelSessionAdmissionV1> {
    let receipts = read_receipts(&channel_session_transition_receipts_file(&harness_home))?;
    let intents = read_intents(&channel_session_transition_intents_file(harness_home))?;
    for intent in intents.iter().rev() {
        if intent.command != ChannelSessionTransitionCommandV1::New
            || intent.lane_digest != lane_digest
            || intent.old_session_key != active_session_key
        {
            continue;
        }
        validate_intent(intent)?;
        let committed = receipts.iter().any(|receipt| {
            receipt.transition_id == intent.transition_id
                && receipt.phase == ChannelSessionTransitionPhaseV1::BoundaryCommitted
        });
        if !committed {
            return Ok(ChannelSessionAdmissionV1::HoldPendingNew);
        }
    }
    Ok(ChannelSessionAdmissionV1::Open)
}

struct GoalBoundaryAssessment {
    goal_boundary: ChannelGoalBoundaryV1,
    goal_closure_id: Option<String>,
    goal_evidence_digest: String,
}

fn assess_goal_boundary(
    harness_home: &Path,
    command: ChannelSessionTransitionCommandV1,
    lane_digest: &str,
    old_session_key: &str,
    caller_effect_identity: &str,
    now_ms: i64,
) -> io::Result<GoalBoundaryAssessment> {
    let report = run_goal_lineage_doctor(GoalLineageDoctorOptions {
        harness_home: harness_home.to_path_buf(),
        lane_digest: Some(lane_digest.to_string()),
        virtual_session_id: None,
    })?;
    let all_active = report
        .lineages
        .iter()
        .filter(|lineage| {
            matches!(
                lineage
                    .goal_status
                    .trim()
                    .to_ascii_lowercase()
                    .replace(['-', '_', ' '], "")
                    .as_str(),
                "active" | "running" | "inprogress"
            )
        })
        .collect::<Vec<_>>();
    let active = all_active
        .iter()
        .copied()
        .filter(|lineage| lineage.source_session_key == old_session_key)
        .collect::<Vec<_>>();
    if report.status == GoalLineageDoctorStatus::Empty
        || (report.status == GoalLineageDoctorStatus::Ready && all_active.is_empty())
    {
        return Ok(GoalBoundaryAssessment {
            goal_boundary: ChannelGoalBoundaryV1::NotApplicable,
            goal_closure_id: None,
            goal_evidence_digest: checksum_json(&serde_json::json!({
                "doctorStatus": report.status,
                "laneDigest": lane_digest,
                "oldSessionKeyDigest": checksum_json(&old_session_key)?,
                "projectionsSeen": report.projections_seen,
                "lineagesSeen": report.lineages.len(),
            }))?,
        });
    }
    if report.status != GoalLineageDoctorStatus::Ready || active.len() != 1 || !active[0].runnable {
        return Ok(GoalBoundaryAssessment {
            goal_boundary: ChannelGoalBoundaryV1::Ambiguous,
            goal_closure_id: None,
            goal_evidence_digest: checksum_json(&serde_json::json!({
                "doctorStatus": report.status,
                "laneDigest": lane_digest,
                "oldSessionKeyDigest": checksum_json(&old_session_key)?,
                "activeCandidates": active.len(),
                "blockers": report.blockers,
                "warnings": report.warnings,
            }))?,
        });
    }

    let lineage = active[0];
    let virtual_session_id = lineage
        .virtual_session_id
        .clone()
        .ok_or_else(|| invalid_data("active goal has no exact virtual-session authority"))?;
    let backend_context_generation = lineage
        .backend_context_generation
        .clone()
        .ok_or_else(|| invalid_data("active goal has no backend context generation"))?;
    let intent = GoalClosureIntentV1::new(
        match command {
            ChannelSessionTransitionCommandV1::Stop => GoalClosureTriggerV1::ChannelStop,
            ChannelSessionTransitionCommandV1::New => GoalClosureTriggerV1::ChannelNew,
        },
        GoalClosureDispositionV1::Canceled,
        GoalClosureAuthorityV1 {
            lane_digest: lane_digest.to_string(),
            concrete_session_key: old_session_key.to_string(),
            virtual_session_id,
            backend_context_generation,
            source_thread_id: lineage.source_thread_id.clone(),
        },
        lineage.goal_reference.clone(),
        lineage.lineage_id.clone(),
        Some(lineage.projection_checksum.clone()),
        caller_effect_identity.to_string(),
        "channel session boundary requested goal cancellation",
    )?;
    let recorded = record_goal_closure_intent(harness_home, &intent)?.intent;
    record_goal_closure_phase(
        harness_home,
        &recorded,
        GoalClosurePhaseV1::IntentRecorded,
        GoalClosurePhaseInputV1 {
            result: GoalClosureResultV1::Pending,
            projection_checksum: None,
            lineage_checksum: None,
            result_evidence_digest: Some(checksum_json(&serde_json::json!({
                "commandEffectIdentity": caller_effect_identity,
            }))?),
            recorded_at_ms: now_ms,
        },
    )?;
    let receipts = goal_closure_receipts_for_id(harness_home, &recorded.closure_id)?;
    let completed = receipts.iter().any(|receipt| {
        receipt.phase == GoalClosurePhaseV1::Completed
            && receipt.result == GoalClosureResultV1::Succeeded
    });
    Ok(GoalBoundaryAssessment {
        goal_boundary: if completed {
            ChannelGoalBoundaryV1::ClosureProven
        } else {
            ChannelGoalBoundaryV1::ClosurePending
        },
        goal_closure_id: Some(recorded.closure_id),
        goal_evidence_digest: recorded.intent_checksum,
    })
}

fn validate_options(options: &PrepareChannelSessionTransitionOptionsV1) -> io::Result<()> {
    validate_nonempty("lane digest", &options.lane_digest)?;
    validate_nonempty("old session key", &options.old_session_key)?;
    match (options.command, options.proposed_new_session_key.as_deref()) {
        (ChannelSessionTransitionCommandV1::New, Some(value)) => {
            validate_nonempty("frozen new session key", value)?;
            if value == options.old_session_key {
                return Err(invalid_input(
                    "new session transition target must differ from the old session",
                ));
            }
        }
        (ChannelSessionTransitionCommandV1::New, None) => {
            return Err(invalid_input(
                "new session transition requires a frozen target",
            ));
        }
        (ChannelSessionTransitionCommandV1::Stop, Some(_)) => {
            return Err(invalid_input(
                "stop transition cannot carry a new session target",
            ));
        }
        (ChannelSessionTransitionCommandV1::Stop, None) => {}
    }
    Ok(())
}

fn validate_intent(intent: &ChannelSessionTransitionIntentV1) -> io::Result<()> {
    if intent.schema != CHANNEL_SESSION_TRANSITION_INTENT_SCHEMA
        || intent.event_key != intent.transition_id
        || intent.command_effect_identity != intent.transition_id
    {
        return Err(invalid_data(
            "session transition intent identity is invalid",
        ));
    }
    let expected =
        transition_identity(intent.command, &intent.lane_digest, &intent.old_session_key)?;
    if expected != intent.transition_id
        || checksum_json(&IntentChecksumPayload::from(intent))? != intent.intent_checksum
    {
        return Err(invalid_data(
            "session transition intent checksum is invalid",
        ));
    }
    Ok(())
}

fn transition_identity(
    command: ChannelSessionTransitionCommandV1,
    lane_digest: &str,
    old_session_key: &str,
) -> io::Result<String> {
    checksum_json(&(command, lane_digest, old_session_key))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct IntentChecksumPayload<'a> {
    transition_id: &'a str,
    command: ChannelSessionTransitionCommandV1,
    lane_digest: &'a str,
    old_session_key: &'a str,
    frozen_new_session_key: Option<&'a str>,
    topic: Option<&'a str>,
    reason: Option<&'a str>,
    goal_boundary: ChannelGoalBoundaryV1,
    goal_closure_id: Option<&'a str>,
    goal_evidence_digest: &'a str,
}

impl<'a> From<&'a ChannelSessionTransitionIntentV1> for IntentChecksumPayload<'a> {
    fn from(intent: &'a ChannelSessionTransitionIntentV1) -> Self {
        Self {
            transition_id: &intent.transition_id,
            command: intent.command,
            lane_digest: &intent.lane_digest,
            old_session_key: &intent.old_session_key,
            frozen_new_session_key: intent.frozen_new_session_key.as_deref(),
            topic: intent.topic.as_deref(),
            reason: intent.reason.as_deref(),
            goal_boundary: intent.goal_boundary,
            goal_closure_id: intent.goal_closure_id.as_deref(),
            goal_evidence_digest: &intent.goal_evidence_digest,
        }
    }
}

fn read_intents(path: &Path) -> io::Result<Vec<ChannelSessionTransitionIntentV1>> {
    read_jsonl(path, "session transition intent")
}

fn read_receipts(path: &Path) -> io::Result<Vec<ChannelSessionTransitionReceiptV1>> {
    read_jsonl(path, "session transition receipt")
}

fn read_jsonl<T>(path: &Path, label: &str) -> io::Result<Vec<T>>
where
    T: for<'de> Deserialize<'de>,
{
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    text.lines()
        .enumerate()
        .filter(|(_, line)| !line.trim().is_empty())
        .map(|(index, line)| {
            serde_json::from_str(line).map_err(|error| {
                invalid_data(format!(
                    "invalid {label} JSONL at line {}: {error}",
                    index + 1
                ))
            })
        })
        .collect()
}

fn checksum_json(value: &impl Serialize) -> io::Result<String> {
    let bytes = serde_json::to_vec(value).map_err(io::Error::other)?;
    let hash = digest::digest(&digest::SHA256, &bytes);
    let mut result = String::with_capacity(71);
    result.push_str("sha256:");
    for byte in hash.as_ref() {
        use std::fmt::Write as _;
        let _ = write!(result, "{byte:02x}");
    }
    Ok(result)
}

fn validate_nonempty(label: &str, value: &str) -> io::Result<()> {
    if value.trim().is_empty() {
        Err(invalid_input(format!("{label} must not be empty")))
    } else {
        Ok(())
    }
}

fn invalid_input(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn duplicate_stop_and_new_are_idempotent_across_restart() {
        let home = temp_home("duplicate-boundaries");
        let first = prepare_channel_session_transition(options(
            &home,
            ChannelSessionTransitionCommandV1::New,
            Some("session-new-1"),
        ))
        .unwrap();
        let replay = prepare_channel_session_transition(options(
            &home,
            ChannelSessionTransitionCommandV1::New,
            Some("session-new-2"),
        ))
        .unwrap();
        assert!(!first.replayed);
        assert!(replay.replayed);
        assert_eq!(first.intent.transition_id, replay.intent.transition_id);
        assert_eq!(
            replay.intent.frozen_new_session_key.as_deref(),
            Some("session-new-1")
        );

        let stop = prepare_channel_session_transition(options(
            &home,
            ChannelSessionTransitionCommandV1::Stop,
            None,
        ))
        .unwrap();
        let stop_replay = prepare_channel_session_transition(options(
            &home,
            ChannelSessionTransitionCommandV1::Stop,
            None,
        ))
        .unwrap();
        assert_eq!(stop.intent.transition_id, stop_replay.intent.transition_id);
        assert!(stop_replay.replayed);
        let _ = fs::remove_dir_all(home);
    }

    #[test]
    fn message_during_new_transition_does_not_start_model_turn() {
        let home = temp_home("pending-admission");
        let prepared = prepare_channel_session_transition(options(
            &home,
            ChannelSessionTransitionCommandV1::New,
            Some("session-new"),
        ))
        .unwrap();
        assert_eq!(
            channel_session_transition_admission(&home, "lane", "session-old").unwrap(),
            ChannelSessionAdmissionV1::HoldPendingNew
        );
        record_channel_session_transition_phase(
            &home,
            &prepared.intent,
            ChannelSessionTransitionPhaseV1::BoundaryCommitted,
            "state-write-committed",
            2,
        )
        .unwrap();
        assert_eq!(
            channel_session_transition_admission(&home, "lane", "session-old").unwrap(),
            ChannelSessionAdmissionV1::Open
        );
        let _ = fs::remove_dir_all(home);
    }

    #[test]
    fn new_failure_or_ambiguity_does_not_rotate_session() {
        let home = temp_home("retry-pending");
        let prepared = prepare_channel_session_transition(options(
            &home,
            ChannelSessionTransitionCommandV1::New,
            Some("session-new"),
        ))
        .unwrap();
        record_channel_session_transition_phase(
            &home,
            &prepared.intent,
            ChannelSessionTransitionPhaseV1::RetryPending,
            "virtual-session-close-failed",
            2,
        )
        .unwrap();
        assert_eq!(
            channel_session_transition_admission(&home, "lane", "session-old").unwrap(),
            ChannelSessionAdmissionV1::HoldPendingNew
        );
        let _ = fs::remove_dir_all(home);
    }

    fn options(
        home: &Path,
        command: ChannelSessionTransitionCommandV1,
        target: Option<&str>,
    ) -> PrepareChannelSessionTransitionOptionsV1 {
        PrepareChannelSessionTransitionOptionsV1 {
            harness_home: home.to_path_buf(),
            command,
            lane_digest: "lane".to_string(),
            old_session_key: "session-old".to_string(),
            proposed_new_session_key: target.map(str::to_string),
            topic: Some("first topic".to_string()),
            reason: Some("test boundary".to_string()),
            now_ms: crate::current_log_time_ms().unwrap(),
        }
    }

    fn temp_home(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("agent-harness-session-transition-{name}-{nanos}"))
    }
}
