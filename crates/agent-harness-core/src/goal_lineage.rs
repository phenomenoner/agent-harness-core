use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use ring::digest;
use serde::{Deserialize, Serialize};

use crate::{append_jsonl_value, current_log_time_ms};

pub const GOAL_LINEAGE_SCHEMA: &str = "agent-harness.goal-lineage.v1";
pub const GOAL_LINEAGE_DOCTOR_SCHEMA: &str = "agent-harness.goal-lineage-doctor.v1";
pub const GOAL_LINEAGE_SUPERSESSION_SCHEMA: &str = "agent-harness.goal-lineage-supersession.v1";

const GOAL_PROJECTION_SCHEMA: &str = "agent-harness.codex-goal-projection.v1";
const VIRTUAL_SESSION_AUTHORITY_SCHEMA: &str = "agent-harness.virtual-session-authority.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoalLineageDoctorOptions {
    pub harness_home: PathBuf,
    pub lane_digest: Option<String>,
    pub virtual_session_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GoalLineageDoctorStatus {
    Ready,
    Empty,
    ReconciliationRequired,
    Blocked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GoalLineageDisposition {
    Runnable,
    Inactive,
    Superseded,
    StaleGeneration,
    MissingAuthority,
    InvalidIdentity,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GoalProjectionObservationPhaseV1 {
    ResumeSettle,
    OwnedTurn,
    GoalRehydration,
    CompactMaintenance,
    StdoutRecovery,
    GovernedClosure,
    #[default]
    #[serde(other)]
    LegacyUnknown,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GoalProjectionTurnRelationV1 {
    CurrentOwnedTurn,
    ExplicitCampaignContinuation,
    HistoricalThreadState,
    AuthoritativeGoalClosure,
    #[default]
    #[serde(other)]
    Uncorrelated,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GoalLineageV1 {
    pub schema: String,
    pub lineage_id: String,
    pub campaign_family_id: String,
    pub queue_id: Option<String>,
    pub source_session_key: String,
    pub virtual_session_id: Option<String>,
    pub lane_digest: Option<String>,
    pub backend_context_generation: Option<String>,
    pub source_thread_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_turn_id: Option<String>,
    pub goal_reference: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend_goal_ref: Option<String>,
    pub objective_checksum: String,
    pub goal_checksum: String,
    pub projection_checksum: String,
    pub goal_status: String,
    #[serde(default)]
    pub observation_phase: GoalProjectionObservationPhaseV1,
    #[serde(default)]
    pub turn_relation: GoalProjectionTurnRelationV1,
    #[serde(default)]
    pub source_final_eligible: bool,
    pub observation_order: u64,
    pub observed_at_ms: i64,
    pub authority_observed_at_ms: Option<i64>,
    pub disposition: GoalLineageDisposition,
    pub runnable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub superseded_by_lineage_id: Option<String>,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GoalLineageCampaignV1 {
    pub campaign_family_id: String,
    pub objective_checksum: String,
    pub lane_digest: Option<String>,
    pub virtual_session_id: Option<String>,
    pub active_lineages: usize,
    pub runnable_lineages: usize,
    pub unresolved_active_lineages: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub canonical_lineage_id: Option<String>,
    pub reconciliation_required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GoalLineageDoctorReport {
    pub schema: String,
    pub status: GoalLineageDoctorStatus,
    pub read_only: bool,
    pub projection_file: PathBuf,
    pub authority_file: PathBuf,
    pub supersession_file: PathBuf,
    pub projections_seen: usize,
    pub authority_receipts_seen: usize,
    pub supersession_receipts_seen: usize,
    pub campaigns: Vec<GoalLineageCampaignV1>,
    pub lineages: Vec<GoalLineageV1>,
    pub blockers: Vec<String>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoalLineageSupersessionOptions {
    pub harness_home: PathBuf,
    pub winner_lineage_id: String,
    pub superseded_lineage_id: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GoalLineageSupersessionV1 {
    pub schema: String,
    pub supersession_id: String,
    pub campaign_family_id: String,
    pub winner_lineage_id: String,
    pub superseded_lineage_id: String,
    pub lane_digest: String,
    pub virtual_session_id: String,
    pub reason: String,
    pub recorded_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoalProjectionHint {
    pub queue_id: Option<String>,
    pub session_key: String,
    pub source_thread_id: String,
    pub source_turn_id: Option<String>,
    pub goal_reference: String,
    pub lane_digest: Option<String>,
    pub backend_context_generation: Option<String>,
    pub status: String,
    pub observation_phase: GoalProjectionObservationPhaseV1,
    pub turn_relation: GoalProjectionTurnRelationV1,
    pub source_final_eligible: bool,
    pub goal_checksum: String,
    pub projection_checksum: String,
    pub projection_complete: bool,
    pub observation_order: u64,
    pub observed_at_ms: i64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GoalProjectionReceipt {
    schema: String,
    queue_id: Option<String>,
    session_key: String,
    source_thread_id: String,
    #[serde(default)]
    source_turn_id: Option<String>,
    #[serde(default)]
    goal_reference: String,
    #[serde(default)]
    backend_goal_ref: Option<String>,
    #[serde(default)]
    lane_digest: Option<String>,
    #[serde(default)]
    backend_context_generation: Option<String>,
    objective: String,
    status: String,
    #[serde(default)]
    observation_phase: GoalProjectionObservationPhaseV1,
    #[serde(default)]
    turn_relation: GoalProjectionTurnRelationV1,
    #[serde(default)]
    source_final_eligible: bool,
    goal_checksum: String,
    projection_checksum: String,
    #[serde(default = "goal_projection_complete_default")]
    projection_complete: bool,
    #[serde(default)]
    observation_order: u64,
    observed_at_ms: i64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct VirtualSessionAuthorityReceipt {
    schema: String,
    queue_id: Option<String>,
    virtual_session_id: String,
    working_session_key: String,
    lane_digest: String,
    #[serde(default)]
    backend_context_generation: Option<String>,
    status: String,
    updated_at_ms: i64,
}

pub fn goal_lineage_supersessions_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("goal-lineage")
        .join("supersession-receipts.jsonl")
}

pub fn latest_goal_projection_for_queue(
    harness_home: impl AsRef<Path>,
    queue_id: &str,
) -> io::Result<Option<GoalProjectionHint>> {
    let file = harness_home
        .as_ref()
        .join("state")
        .join("runtime-queue")
        .join("codex-goal-projection-receipts.jsonl");
    let (values, blockers) =
        read_jsonl::<GoalProjectionReceipt>(&file, GOAL_PROJECTION_SCHEMA, "goal projection")?;
    if !blockers.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            blockers.join("; "),
        ));
    }
    Ok(values
        .into_iter()
        .filter(|projection| projection.queue_id.as_deref() == Some(queue_id))
        .max_by_key(|projection| (projection.observed_at_ms, projection.observation_order))
        .map(|projection| GoalProjectionHint {
            queue_id: projection.queue_id,
            session_key: projection.session_key,
            source_thread_id: projection.source_thread_id,
            source_turn_id: projection.source_turn_id,
            goal_reference: if projection.goal_reference.trim().is_empty() {
                projection.goal_checksum.clone()
            } else {
                projection.goal_reference
            },
            lane_digest: projection.lane_digest,
            backend_context_generation: projection.backend_context_generation,
            status: projection.status,
            observation_phase: projection.observation_phase,
            turn_relation: projection.turn_relation,
            source_final_eligible: projection_source_final_is_eligible(
                projection.source_final_eligible,
                projection.observation_phase,
                projection.turn_relation,
            ),
            goal_checksum: projection.goal_checksum,
            projection_checksum: projection.projection_checksum,
            projection_complete: projection.projection_complete,
            observation_order: projection.observation_order,
            observed_at_ms: projection.observed_at_ms,
        }))
}

pub fn run_goal_lineage_doctor(
    options: GoalLineageDoctorOptions,
) -> io::Result<GoalLineageDoctorReport> {
    let projection_file = options
        .harness_home
        .join("state")
        .join("runtime-queue")
        .join("codex-goal-projection-receipts.jsonl");
    let authority_file = options
        .harness_home
        .join("state")
        .join("context-rollover")
        .join("virtual-session-authority-receipts.jsonl");
    let supersession_file = goal_lineage_supersessions_file(&options.harness_home);

    let (projections, mut blockers) = read_jsonl::<GoalProjectionReceipt>(
        &projection_file,
        GOAL_PROJECTION_SCHEMA,
        "goal projection",
    )?;
    let (authorities, authority_blockers) = read_jsonl::<VirtualSessionAuthorityReceipt>(
        &authority_file,
        VIRTUAL_SESSION_AUTHORITY_SCHEMA,
        "virtual-session authority",
    )?;
    blockers.extend(authority_blockers);
    let (supersessions, supersession_blockers) = read_jsonl::<GoalLineageSupersessionV1>(
        &supersession_file,
        GOAL_LINEAGE_SUPERSESSION_SCHEMA,
        "goal-lineage supersession",
    )?;
    blockers.extend(supersession_blockers);

    let latest_authorities = latest_authorities(&authorities);
    let mut latest_projections = BTreeMap::<String, GoalProjectionReceipt>::new();
    for projection in &projections {
        let key = format!(
            "{}\u{1f}{}\u{1f}{}",
            projection.source_thread_id,
            projection
                .backend_context_generation
                .as_deref()
                .unwrap_or_default(),
            projection_goal_identity(projection)
        );
        let replace = latest_projections.get(&key).is_none_or(|current| {
            let current_is_closure = is_authoritative_terminal_closure(current);
            let incoming_is_closure = is_authoritative_terminal_closure(projection);
            (incoming_is_closure && !current_is_closure)
                || (incoming_is_closure == current_is_closure
                    && (projection.observed_at_ms, projection.observation_order)
                        >= (current.observed_at_ms, current.observation_order))
        });
        if replace {
            latest_projections.insert(key, projection.clone());
        }
    }

    let mut lineages = Vec::new();
    for projection in latest_projections.into_values() {
        let authority = projection_authority(&projection, &latest_authorities);
        let authority_lane = authority.map(|value| value.lane_digest.clone());
        let authority_virtual_session = authority.map(|value| value.virtual_session_id.clone());
        let authority_generation =
            authority.and_then(|value| value.backend_context_generation.clone());
        let exact_identity = authority.is_some()
            && projection.projection_complete
            && !projection.objective.trim().is_empty()
            && projection.lane_digest == authority_lane
            && projection.backend_context_generation == authority_generation
            && authority.is_some_and(|value| value.status == "authoritative-v2");

        let objective_checksum = checksum_json(&projection.objective)?;
        let family_payload = serde_json::json!({
            "laneDigest": authority_lane,
            "virtualSessionId": authority_virtual_session,
            "objectiveChecksum": objective_checksum,
        });
        let campaign_family_id = checksum_json(&family_payload)?;
        // Use the same durable identity for both projection selection and the
        // emitted lineage. Newer receipts carry `backendGoalRef` without the
        // legacy `goalReference` field, so falling straight back to the
        // checksum would make an otherwise runnable goal appear unrelated.
        let goal_reference = projection_goal_identity(&projection).to_string();
        let lineage_payload = serde_json::json!({
            "campaignFamilyId": campaign_family_id,
            "backendContextGeneration": projection.backend_context_generation,
            "sourceThreadId": projection.source_thread_id,
            "goalReference": goal_reference,
        });
        let lineage_id = checksum_json(&lineage_payload)?;
        let active = is_active_status(&projection.status);
        let (disposition, runnable, reason) = if authority.is_none() {
            (
                GoalLineageDisposition::MissingAuthority,
                false,
                "no exact-v2 virtual-session authority exists for this queue/session".to_string(),
            )
        } else if !exact_identity {
            let stale = projection.lane_digest == authority_lane
                && projection.backend_context_generation != authority_generation;
            if stale {
                (
                    GoalLineageDisposition::StaleGeneration,
                    false,
                    "backend goal projection belongs to a stale backend generation".to_string(),
                )
            } else {
                (
                    GoalLineageDisposition::InvalidIdentity,
                    false,
                    "backend goal projection does not match exact lane/virtual-session authority"
                        .to_string(),
                )
            }
        } else if !active {
            (
                GoalLineageDisposition::Inactive,
                false,
                "exact-authority backend goal projection is not active".to_string(),
            )
        } else {
            (
                GoalLineageDisposition::Runnable,
                true,
                "active goal matches current exact-v2 lane and backend generation".to_string(),
            )
        };
        lineages.push(GoalLineageV1 {
            schema: GOAL_LINEAGE_SCHEMA.to_string(),
            lineage_id,
            campaign_family_id,
            queue_id: projection.queue_id,
            source_session_key: projection.session_key,
            virtual_session_id: authority_virtual_session,
            lane_digest: authority_lane,
            backend_context_generation: projection.backend_context_generation,
            source_thread_id: projection.source_thread_id,
            source_turn_id: projection.source_turn_id,
            goal_reference,
            backend_goal_ref: projection.backend_goal_ref,
            objective_checksum,
            goal_checksum: projection.goal_checksum,
            projection_checksum: projection.projection_checksum,
            goal_status: projection.status,
            observation_phase: projection.observation_phase,
            turn_relation: projection.turn_relation,
            source_final_eligible: projection_source_final_is_eligible(
                projection.source_final_eligible,
                projection.observation_phase,
                projection.turn_relation,
            ),
            observation_order: projection.observation_order,
            observed_at_ms: projection.observed_at_ms,
            authority_observed_at_ms: authority.map(|value| value.updated_at_ms),
            disposition,
            runnable,
            superseded_by_lineage_id: None,
            reason,
        });
    }

    let superseded_by = valid_supersession_map(&supersessions, &lineages, &mut blockers)?;
    for lineage in &mut lineages {
        if let Some(winner) = superseded_by.get(&lineage.lineage_id) {
            lineage.disposition = GoalLineageDisposition::Superseded;
            lineage.runnable = false;
            lineage.superseded_by_lineage_id = Some(winner.clone());
            lineage.reason = format!(
                "append-only reconciliation superseded this backend goal row with {winner}"
            );
        }
    }

    if let Some(expected) = options.lane_digest.as_deref() {
        lineages.retain(|lineage| lineage.lane_digest.as_deref() == Some(expected));
    }
    if let Some(expected) = options.virtual_session_id.as_deref() {
        lineages.retain(|lineage| lineage.virtual_session_id.as_deref() == Some(expected));
    }
    lineages.sort_by(|left, right| {
        left.campaign_family_id
            .cmp(&right.campaign_family_id)
            .then_with(|| left.observed_at_ms.cmp(&right.observed_at_ms))
            .then_with(|| left.lineage_id.cmp(&right.lineage_id))
    });
    let warnings = lineages
        .iter()
        .filter(|lineage| {
            is_active_status(&lineage.goal_status)
                && !lineage.runnable
                && lineage.disposition != GoalLineageDisposition::Superseded
        })
        .map(|lineage| {
            format!(
                "active lineage {} is fail-closed: {}",
                lineage.lineage_id, lineage.reason
            )
        })
        .collect::<Vec<_>>();

    let mut grouped = BTreeMap::<String, Vec<&GoalLineageV1>>::new();
    for lineage in &lineages {
        grouped
            .entry(lineage.campaign_family_id.clone())
            .or_default()
            .push(lineage);
    }
    let mut campaigns = Vec::new();
    let mut reconciliation_required = false;
    let mut identity_blocked = false;
    for (campaign_family_id, rows) in grouped {
        let active = rows
            .iter()
            .filter(|row| is_active_status(&row.goal_status))
            .count();
        let runnable = rows.iter().filter(|row| row.runnable).count();
        let unresolved = rows
            .iter()
            .filter(|row| {
                is_active_status(&row.goal_status)
                    && row.disposition != GoalLineageDisposition::Superseded
            })
            .count();
        let needs_reconciliation = unresolved > 1;
        reconciliation_required |= needs_reconciliation || runnable > 1;
        identity_blocked |= rows.iter().any(|row| {
            is_active_status(&row.goal_status)
                && matches!(
                    row.disposition,
                    GoalLineageDisposition::MissingAuthority
                        | GoalLineageDisposition::InvalidIdentity
                )
        }) || (active > 0 && runnable == 0);
        campaigns.push(GoalLineageCampaignV1 {
            campaign_family_id,
            objective_checksum: rows[0].objective_checksum.clone(),
            lane_digest: rows[0].lane_digest.clone(),
            virtual_session_id: rows[0].virtual_session_id.clone(),
            active_lineages: active,
            runnable_lineages: runnable,
            unresolved_active_lineages: unresolved,
            canonical_lineage_id: (runnable == 1).then(|| {
                rows.iter()
                    .find(|row| row.runnable)
                    .unwrap()
                    .lineage_id
                    .clone()
            }),
            reconciliation_required: needs_reconciliation,
        });
    }
    let status = if !blockers.is_empty() || identity_blocked {
        GoalLineageDoctorStatus::Blocked
    } else if reconciliation_required {
        GoalLineageDoctorStatus::ReconciliationRequired
    } else if campaigns.is_empty() {
        GoalLineageDoctorStatus::Empty
    } else {
        GoalLineageDoctorStatus::Ready
    };

    Ok(GoalLineageDoctorReport {
        schema: GOAL_LINEAGE_DOCTOR_SCHEMA.to_string(),
        status,
        read_only: true,
        projection_file,
        authority_file,
        supersession_file,
        projections_seen: projections.len(),
        authority_receipts_seen: authorities.len(),
        supersession_receipts_seen: supersessions.len(),
        campaigns,
        lineages,
        blockers,
        warnings,
    })
}

pub fn append_goal_lineage_supersession(
    options: GoalLineageSupersessionOptions,
) -> io::Result<GoalLineageSupersessionV1> {
    let winner = options.winner_lineage_id.trim();
    let superseded = options.superseded_lineage_id.trim();
    let reason = options.reason.trim();
    if winner.is_empty() || superseded.is_empty() || reason.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "winner, superseded lineage, and reason are required",
        ));
    }
    if winner == superseded {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "a lineage cannot supersede itself",
        ));
    }
    if reason.chars().count() > 240 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "supersession reason exceeds 240 characters",
        ));
    }

    let report = run_goal_lineage_doctor(GoalLineageDoctorOptions {
        harness_home: options.harness_home.clone(),
        lane_digest: None,
        virtual_session_id: None,
    })?;
    let winner_row = report
        .lineages
        .iter()
        .find(|row| row.lineage_id == winner)
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "winner lineage not found"))?;
    let superseded_row = report
        .lineages
        .iter()
        .find(|row| row.lineage_id == superseded)
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "superseded lineage not found"))?;
    if winner_row.campaign_family_id != superseded_row.campaign_family_id {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "winner and superseded lineage are not in the same campaign family",
        ));
    }
    let lane_digest = winner_row.lane_digest.clone().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "winner lacks exact lane authority",
        )
    })?;
    let virtual_session_id = winner_row.virtual_session_id.clone().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "winner lacks virtual-session authority",
        )
    })?;
    if !winner_row.runnable {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "winner lineage is not the current exact-lane runnable row",
        ));
    }

    let supersession_id = checksum_json(&serde_json::json!({
        "campaignFamilyId": winner_row.campaign_family_id,
        "winnerLineageId": winner,
        "supersededLineageId": superseded,
    }))?;
    let file = goal_lineage_supersessions_file(&options.harness_home);
    let (existing, _) = read_jsonl::<GoalLineageSupersessionV1>(
        &file,
        GOAL_LINEAGE_SUPERSESSION_SCHEMA,
        "goal-lineage supersession",
    )?;
    if let Some(receipt) = existing
        .into_iter()
        .find(|receipt| receipt.supersession_id == supersession_id)
    {
        return Ok(receipt);
    }
    let receipt = GoalLineageSupersessionV1 {
        schema: GOAL_LINEAGE_SUPERSESSION_SCHEMA.to_string(),
        supersession_id,
        campaign_family_id: winner_row.campaign_family_id.clone(),
        winner_lineage_id: winner.to_string(),
        superseded_lineage_id: superseded.to_string(),
        lane_digest,
        virtual_session_id,
        reason: reason.to_string(),
        recorded_at_ms: current_log_time_ms()?,
    };
    append_jsonl_value(&file, &receipt)?;
    Ok(receipt)
}

fn latest_authorities(
    values: &[VirtualSessionAuthorityReceipt],
) -> BTreeMap<String, VirtualSessionAuthorityReceipt> {
    let mut latest = BTreeMap::new();
    for value in values {
        let key = authority_key(value.queue_id.as_deref(), &value.working_session_key);
        if latest
            .get(&key)
            .is_none_or(|current: &VirtualSessionAuthorityReceipt| {
                value.updated_at_ms >= current.updated_at_ms
            })
        {
            latest.insert(key, value.clone());
        }
    }
    latest
}

fn projection_authority<'a>(
    projection: &GoalProjectionReceipt,
    authorities: &'a BTreeMap<String, VirtualSessionAuthorityReceipt>,
) -> Option<&'a VirtualSessionAuthorityReceipt> {
    authorities.get(&authority_key(
        projection.queue_id.as_deref(),
        &projection.session_key,
    ))
}

fn authority_key(queue_id: Option<&str>, session_key: &str) -> String {
    format!("{}\u{1f}{session_key}", queue_id.unwrap_or_default())
}

fn valid_supersession_map(
    receipts: &[GoalLineageSupersessionV1],
    lineages: &[GoalLineageV1],
    blockers: &mut Vec<String>,
) -> io::Result<BTreeMap<String, String>> {
    let mut map = BTreeMap::new();
    let mut ids = BTreeSet::new();
    let superseded_ids = receipts
        .iter()
        .map(|receipt| receipt.superseded_lineage_id.as_str())
        .collect::<BTreeSet<_>>();
    for receipt in receipts {
        if !ids.insert(receipt.supersession_id.clone()) {
            continue;
        }
        if receipt.winner_lineage_id == receipt.superseded_lineage_id {
            blockers.push(format!(
                "invalid self-supersession receipt {}",
                receipt.supersession_id
            ));
            continue;
        }
        let Some(winner) = lineages
            .iter()
            .find(|lineage| lineage.lineage_id == receipt.winner_lineage_id)
        else {
            blockers.push(format!(
                "supersession receipt {} references a missing winner",
                receipt.supersession_id
            ));
            continue;
        };
        let Some(loser) = lineages
            .iter()
            .find(|lineage| lineage.lineage_id == receipt.superseded_lineage_id)
        else {
            blockers.push(format!(
                "supersession receipt {} references a missing superseded lineage",
                receipt.supersession_id
            ));
            continue;
        };
        let expected_id = checksum_json(&serde_json::json!({
            "campaignFamilyId": winner.campaign_family_id,
            "winnerLineageId": winner.lineage_id,
            "supersededLineageId": loser.lineage_id,
        }))?;
        let identity_valid = winner.campaign_family_id == loser.campaign_family_id
            && receipt.campaign_family_id == winner.campaign_family_id
            && winner.lane_digest.as_deref() == Some(receipt.lane_digest.as_str())
            && winner.virtual_session_id.as_deref() == Some(receipt.virtual_session_id.as_str())
            && receipt.supersession_id == expected_id
            && winner.runnable
            && !superseded_ids.contains(receipt.winner_lineage_id.as_str());
        if !identity_valid {
            blockers.push(format!(
                "supersession receipt {} failed same-campaign/exact-authority/winner validation",
                receipt.supersession_id
            ));
            continue;
        }
        if let Some(existing) = map.insert(
            receipt.superseded_lineage_id.clone(),
            receipt.winner_lineage_id.clone(),
        ) && existing != receipt.winner_lineage_id
        {
            blockers.push(format!(
                "conflicting supersession winners for lineage {}",
                receipt.superseded_lineage_id
            ));
        }
    }
    Ok(map)
}

fn read_jsonl<T>(
    path: &Path,
    expected_schema: &str,
    label: &str,
) -> io::Result<(Vec<T>, Vec<String>)>
where
    T: for<'de> Deserialize<'de> + SchemaValue,
{
    if !path.is_file() {
        return Ok((Vec::new(), Vec::new()));
    }
    let text = fs::read_to_string(path)?;
    let mut values = Vec::new();
    let mut blockers = Vec::new();
    for (index, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<T>(line) {
            Ok(value) if value.schema() == expected_schema => values.push(value),
            Ok(_) => {}
            Err(error) => blockers.push(format!(
                "malformed {label} receipt at {}:{}: {error}",
                path.display(),
                index + 1
            )),
        }
    }
    Ok((values, blockers))
}

trait SchemaValue {
    fn schema(&self) -> &str;
}

impl SchemaValue for GoalProjectionReceipt {
    fn schema(&self) -> &str {
        &self.schema
    }
}

impl SchemaValue for VirtualSessionAuthorityReceipt {
    fn schema(&self) -> &str {
        &self.schema
    }
}

impl SchemaValue for GoalLineageSupersessionV1 {
    fn schema(&self) -> &str {
        &self.schema
    }
}

fn is_active_status(status: &str) -> bool {
    matches!(
        status
            .trim()
            .to_ascii_lowercase()
            .replace(['-', '_', ' '], "")
            .as_str(),
        "active" | "running" | "inprogress"
    )
}

fn goal_projection_complete_default() -> bool {
    true
}

fn projection_goal_identity(projection: &GoalProjectionReceipt) -> &str {
    projection
        .backend_goal_ref
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            (!projection.goal_reference.trim().is_empty())
                .then_some(projection.goal_reference.as_str())
        })
        .unwrap_or(projection.goal_checksum.as_str())
}

fn is_authoritative_terminal_closure(projection: &GoalProjectionReceipt) -> bool {
    projection.observation_phase == GoalProjectionObservationPhaseV1::GovernedClosure
        && projection.turn_relation == GoalProjectionTurnRelationV1::AuthoritativeGoalClosure
        && !projection.source_final_eligible
        && !is_active_status(&projection.status)
}

fn projection_source_final_is_eligible(
    claimed_eligible: bool,
    observation_phase: GoalProjectionObservationPhaseV1,
    turn_relation: GoalProjectionTurnRelationV1,
) -> bool {
    claimed_eligible
        && observation_phase != GoalProjectionObservationPhaseV1::LegacyUnknown
        && matches!(
            turn_relation,
            GoalProjectionTurnRelationV1::CurrentOwnedTurn
                | GoalProjectionTurnRelationV1::ExplicitCampaignContinuation
        )
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn doctor_is_read_only_and_requires_explicit_supersession_for_three_active_rows() {
        let root = temp_root("three_active");
        let home = root.join("home");
        let projection_file = home
            .join("state")
            .join("runtime-queue")
            .join("codex-goal-projection-receipts.jsonl");
        let authority_file = home
            .join("state")
            .join("context-rollover")
            .join("virtual-session-authority-receipts.jsonl");
        write_authority(
            &authority_file,
            "q-old-1",
            "session",
            "lane",
            "gen-old-1",
            10,
        );
        write_authority(
            &authority_file,
            "q-old-2",
            "session",
            "lane",
            "gen-old-2",
            20,
        );
        write_authority(
            &authority_file,
            "q-current",
            "session",
            "lane",
            "gen-current",
            30,
        );
        write_projection(&projection_file, "q-old-1", "thread-old-1", "gen-old-1", 11);
        write_projection(&projection_file, "q-old-2", "thread-old-2", "gen-old-2", 21);
        write_projection(
            &projection_file,
            "q-current",
            "thread-current",
            "gen-current",
            31,
        );

        let before_projection = fs::read(&projection_file).unwrap();
        let before_authority = fs::read(&authority_file).unwrap();
        let before_files = list_files(&home);
        let first = run_goal_lineage_doctor(GoalLineageDoctorOptions {
            harness_home: home.clone(),
            lane_digest: None,
            virtual_session_id: None,
        })
        .unwrap();
        assert!(first.read_only);
        assert_eq!(
            first.status,
            GoalLineageDoctorStatus::ReconciliationRequired
        );
        assert_eq!(first.lineages.len(), 3);
        assert_eq!(first.campaigns.len(), 1);
        assert_eq!(first.campaigns[0].active_lineages, 3);
        assert_eq!(first.campaigns[0].runnable_lineages, 3);
        assert!(first.campaigns[0].reconciliation_required);
        assert_eq!(fs::read(&projection_file).unwrap(), before_projection);
        assert_eq!(fs::read(&authority_file).unwrap(), before_authority);
        assert_eq!(list_files(&home), before_files);

        let winner = first
            .lineages
            .iter()
            .find(|row| row.source_thread_id == "thread-current")
            .unwrap()
            .lineage_id
            .clone();
        let losers = first
            .lineages
            .iter()
            .filter(|row| row.lineage_id != winner)
            .map(|row| row.lineage_id.clone())
            .collect::<Vec<_>>();
        for loser in &losers {
            append_goal_lineage_supersession(GoalLineageSupersessionOptions {
                harness_home: home.clone(),
                winner_lineage_id: winner.clone(),
                superseded_lineage_id: loser.clone(),
                reason: "operator-reviewed duplicate active backend row".to_string(),
            })
            .unwrap();
        }
        append_goal_lineage_supersession(GoalLineageSupersessionOptions {
            harness_home: home.clone(),
            winner_lineage_id: winner.clone(),
            superseded_lineage_id: losers[0].clone(),
            reason: "a repeated request returns the original receipt".to_string(),
        })
        .unwrap();
        let reconciled = run_goal_lineage_doctor(GoalLineageDoctorOptions {
            harness_home: home.clone(),
            lane_digest: None,
            virtual_session_id: None,
        })
        .unwrap();
        assert_eq!(reconciled.status, GoalLineageDoctorStatus::Ready);
        assert_eq!(reconciled.campaigns[0].runnable_lineages, 1);
        assert_eq!(reconciled.campaigns[0].unresolved_active_lineages, 1);
        assert_eq!(
            reconciled
                .lineages
                .iter()
                .filter(|row| row.disposition == GoalLineageDisposition::Superseded)
                .count(),
            2
        );
        assert_eq!(fs::read(&projection_file).unwrap(), before_projection);
        assert_eq!(fs::read(&authority_file).unwrap(), before_authority);
        assert_eq!(
            fs::read_to_string(goal_lineage_supersessions_file(&home))
                .unwrap()
                .lines()
                .count(),
            2
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn doctor_rejects_stale_generation_and_tampered_supersession_receipt() {
        let root = temp_root("stale_and_tampered");
        let home = root.join("home");
        let projection_file = home
            .join("state")
            .join("runtime-queue")
            .join("codex-goal-projection-receipts.jsonl");
        let authority_file = home
            .join("state")
            .join("context-rollover")
            .join("virtual-session-authority-receipts.jsonl");
        write_authority(&authority_file, "queue", "session", "lane", "gen-old", 10);
        write_projection(&projection_file, "queue", "thread-old", "gen-old", 11);
        write_authority(
            &authority_file,
            "queue",
            "session",
            "lane",
            "gen-current",
            20,
        );
        write_projection(
            &projection_file,
            "queue",
            "thread-current",
            "gen-current",
            21,
        );
        let first = run_goal_lineage_doctor(GoalLineageDoctorOptions {
            harness_home: home.clone(),
            lane_digest: None,
            virtual_session_id: None,
        })
        .unwrap();
        assert_eq!(
            first.status,
            GoalLineageDoctorStatus::ReconciliationRequired
        );
        let stale = first
            .lineages
            .iter()
            .find(|row| row.source_thread_id == "thread-old")
            .unwrap();
        let current = first
            .lineages
            .iter()
            .find(|row| row.source_thread_id == "thread-current")
            .unwrap();
        assert_eq!(stale.disposition, GoalLineageDisposition::StaleGeneration);
        assert!(current.runnable);
        append_jsonl_value(
            &goal_lineage_supersessions_file(&home),
            &json!({
                "schema": GOAL_LINEAGE_SUPERSESSION_SCHEMA,
                "supersessionId": "sha256:tampered",
                "campaignFamilyId": current.campaign_family_id,
                "winnerLineageId": current.lineage_id,
                "supersededLineageId": stale.lineage_id,
                "laneDigest": "lane",
                "virtualSessionId": "virtual-session",
                "reason": "tampered fixture",
                "recordedAtMs": 22
            }),
        )
        .unwrap();
        let tampered = run_goal_lineage_doctor(GoalLineageDoctorOptions {
            harness_home: home.clone(),
            lane_digest: None,
            virtual_session_id: None,
        })
        .unwrap();
        assert_eq!(tampered.status, GoalLineageDoctorStatus::Blocked);
        assert!(
            tampered
                .blockers
                .iter()
                .any(|value| value.contains("failed same-campaign"))
        );
        assert!(
            tampered
                .lineages
                .iter()
                .find(|row| row.source_thread_id == "thread-old")
                .is_some_and(|row| row.disposition == GoalLineageDisposition::StaleGeneration)
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn doctor_rejects_wrong_lane_and_supersession_rejects_other_campaign() {
        let root = temp_root("wrong_lane");
        let home = root.join("home");
        let projection_file = home
            .join("state")
            .join("runtime-queue")
            .join("codex-goal-projection-receipts.jsonl");
        let authority_file = home
            .join("state")
            .join("context-rollover")
            .join("virtual-session-authority-receipts.jsonl");
        write_authority(&authority_file, "q-good", "session", "lane", "gen", 10);
        write_projection(&projection_file, "q-good", "thread-good", "gen", 11);
        let wrong = json!({
            "schema": GOAL_PROJECTION_SCHEMA,
            "queueId": "q-wrong",
            "sessionKey": "session",
            "sourceThreadId": "thread-wrong",
            "laneDigest": "wrong-lane",
            "backendContextGeneration": "gen-other",
            "objective": "a different objective",
            "status": "active",
            "goalChecksum": "goal-other",
            "projectionChecksum": "projection-other",
            "observationOrder": 2,
            "observedAtMs": 12
        });
        append_jsonl_value(&projection_file, &wrong).unwrap();
        write_authority(
            &authority_file,
            "q-wrong",
            "session",
            "lane",
            "gen-other",
            12,
        );
        let report = run_goal_lineage_doctor(GoalLineageDoctorOptions {
            harness_home: home.clone(),
            lane_digest: None,
            virtual_session_id: None,
        })
        .unwrap();
        let good = report
            .lineages
            .iter()
            .find(|row| row.source_thread_id == "thread-good")
            .unwrap();
        let wrong = report
            .lineages
            .iter()
            .find(|row| row.source_thread_id == "thread-wrong")
            .unwrap();
        assert_eq!(wrong.disposition, GoalLineageDisposition::InvalidIdentity);
        let error = append_goal_lineage_supersession(GoalLineageSupersessionOptions {
            harness_home: home.clone(),
            winner_lineage_id: good.lineage_id.clone(),
            superseded_lineage_id: wrong.lineage_id.clone(),
            reason: "must fail".to_string(),
        })
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(!goal_lineage_supersessions_file(&home).exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn projection_reader_preserves_provenance_and_fails_closed_for_legacy_or_unknown_rows() {
        let root = temp_root("projection_reader_preserves_provenance");
        let home = root.join(".agent-harness");
        let projection_file = home
            .join("state")
            .join("runtime-queue")
            .join("codex-goal-projection-receipts.jsonl");
        let base = |queue: &str| {
            json!({
                "schema": GOAL_PROJECTION_SCHEMA,
                "queueId": queue,
                "sessionKey": "session",
                "sourceThreadId": "thread",
                "sourceTurnId": "turn",
                "goalReference": "goal-ref",
                "backendGoalRef": "backend-goal-ref",
                "laneDigest": "lane",
                "backendContextGeneration": "generation",
                "objective": "ship the integrated campaign",
                "status": "active",
                "goalChecksum": "goal-checksum",
                "projectionChecksum": "projection-checksum",
                "projectionComplete": true,
                "observationOrder": 1,
                "observedAtMs": 10
            })
        };
        let mut current = base("queue-current");
        current["observationPhase"] = json!("owned-turn");
        current["turnRelation"] = json!("current-owned-turn");
        current["sourceFinalEligible"] = json!(true);
        append_jsonl_value(&projection_file, &current).unwrap();
        append_jsonl_value(&projection_file, &base("queue-legacy")).unwrap();
        let mut unknown = base("queue-unknown");
        unknown["observationPhase"] = json!("future-maintenance-phase");
        unknown["turnRelation"] = json!("future-turn-relation");
        unknown["sourceFinalEligible"] = json!(true);
        append_jsonl_value(&projection_file, &unknown).unwrap();

        let current = latest_goal_projection_for_queue(&home, "queue-current")
            .unwrap()
            .unwrap();
        assert_eq!(
            current.observation_phase,
            GoalProjectionObservationPhaseV1::OwnedTurn
        );
        assert_eq!(
            current.turn_relation,
            GoalProjectionTurnRelationV1::CurrentOwnedTurn
        );
        assert!(current.source_final_eligible);

        let legacy = latest_goal_projection_for_queue(&home, "queue-legacy")
            .unwrap()
            .unwrap();
        assert_eq!(
            legacy.observation_phase,
            GoalProjectionObservationPhaseV1::LegacyUnknown
        );
        assert_eq!(
            legacy.turn_relation,
            GoalProjectionTurnRelationV1::Uncorrelated
        );
        assert!(!legacy.source_final_eligible);

        let unknown = latest_goal_projection_for_queue(&home, "queue-unknown")
            .unwrap()
            .unwrap();
        assert_eq!(
            unknown.observation_phase,
            GoalProjectionObservationPhaseV1::LegacyUnknown
        );
        assert_eq!(
            unknown.turn_relation,
            GoalProjectionTurnRelationV1::Uncorrelated
        );
        assert!(
            !unknown.source_final_eligible,
            "unknown provenance must override a claimed eligibility bit"
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn late_active_projection_cannot_reopen_closed_generation() {
        let root = temp_root("terminal_closure_precedence");
        let home = root.join("home");
        let projection_file = home
            .join("state")
            .join("runtime-queue")
            .join("codex-goal-projection-receipts.jsonl");
        let authority_file = home
            .join("state")
            .join("context-rollover")
            .join("virtual-session-authority-receipts.jsonl");
        write_authority(&authority_file, "queue", "session", "lane", "gen", 1);

        let projection = |goal_ref: &str,
                          status: &str,
                          phase: &str,
                          relation: &str,
                          eligible: bool,
                          order: u64,
                          at: i64| {
            json!({
                "schema": GOAL_PROJECTION_SCHEMA,
                "queueId": "queue",
                "sessionKey": "session",
                "sourceThreadId": "thread",
                "sourceTurnId": "turn",
                "backendGoalRef": goal_ref,
                "laneDigest": "lane",
                "backendContextGeneration": "gen",
                "objective": "ship the integrated campaign",
                "status": status,
                "observationPhase": phase,
                "turnRelation": relation,
                "sourceFinalEligible": eligible,
                "goalChecksum": format!("checksum-{goal_ref}"),
                "projectionChecksum": format!("projection-{order}"),
                "projectionComplete": true,
                "observationOrder": order,
                "observedAtMs": at
            })
        };
        append_jsonl_value(
            &projection_file,
            &projection(
                "goal-1",
                "active",
                "owned-turn",
                "current-owned-turn",
                true,
                1,
                10,
            ),
        )
        .unwrap();
        append_jsonl_value(
            &projection_file,
            &projection(
                "goal-1",
                "completed",
                "governed-closure",
                "authoritative-goal-closure",
                false,
                2,
                20,
            ),
        )
        .unwrap();
        append_jsonl_value(
            &projection_file,
            &projection(
                "goal-1",
                "active",
                "owned-turn",
                "current-owned-turn",
                true,
                3,
                30,
            ),
        )
        .unwrap();

        let closed = run_goal_lineage_doctor(GoalLineageDoctorOptions {
            harness_home: home.clone(),
            lane_digest: None,
            virtual_session_id: None,
        })
        .unwrap();
        assert_eq!(closed.lineages.len(), 1);
        assert_eq!(closed.lineages[0].goal_status, "completed");
        assert_eq!(
            closed.lineages[0].observation_phase,
            GoalProjectionObservationPhaseV1::GovernedClosure
        );
        assert!(!closed.lineages[0].runnable);

        append_jsonl_value(
            &projection_file,
            &projection(
                "goal-2",
                "active",
                "owned-turn",
                "current-owned-turn",
                true,
                4,
                40,
            ),
        )
        .unwrap();
        let next_goal = run_goal_lineage_doctor(GoalLineageDoctorOptions {
            harness_home: home.clone(),
            lane_digest: None,
            virtual_session_id: None,
        })
        .unwrap();
        assert_eq!(next_goal.lineages.len(), 2);
        assert_eq!(
            next_goal.lineages.iter().filter(|row| row.runnable).count(),
            1
        );
        assert!(
            next_goal
                .lineages
                .iter()
                .any(|row| row.goal_reference == "goal-2" && row.runnable),
            "new goal lineage should remain runnable: {:#?}",
            next_goal.lineages
        );
        let _ = fs::remove_dir_all(root);
    }

    fn write_projection(path: &Path, queue: &str, thread: &str, generation: &str, at: i64) {
        append_jsonl_value(
            path,
            &json!({
                "schema": GOAL_PROJECTION_SCHEMA,
                "queueId": queue,
                "sessionKey": "session",
                "sourceThreadId": thread,
                "sourceTurnId": format!("turn-{thread}"),
                "backendGoalRef": "goal-ref",
                "laneDigest": "lane",
                "backendContextGeneration": generation,
                "objective": "ship the integrated campaign",
                "status": "active",
                "observationPhase": "owned-turn",
                "turnRelation": "current-owned-turn",
                "sourceFinalEligible": true,
                "goalChecksum": "goal-checksum",
                "projectionChecksum": "projection-checksum",
                "observationOrder": 1,
                "observedAtMs": at
            }),
        )
        .unwrap();
    }

    fn write_authority(
        path: &Path,
        queue: &str,
        session: &str,
        lane: &str,
        generation: &str,
        at: i64,
    ) {
        append_jsonl_value(
            path,
            &json!({
                "schema": VIRTUAL_SESSION_AUTHORITY_SCHEMA,
                "queueId": queue,
                "virtualSessionId": "virtual-session",
                "workingSessionKey": session,
                "laneDigest": lane,
                "backendContextGeneration": generation,
                "workingSetFile": "opaque-working-set-ref",
                "status": "authoritative-v2",
                "reason": "fixture",
                "updatedAtMs": at
            }),
        )
        .unwrap();
    }

    fn list_files(root: &Path) -> Vec<PathBuf> {
        let mut pending = vec![root.to_path_buf()];
        let mut files = Vec::new();
        while let Some(path) = pending.pop() {
            if let Ok(entries) = fs::read_dir(path) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_dir() {
                        pending.push(path);
                    } else {
                        files.push(path);
                    }
                }
            }
        }
        files.sort();
        files
    }

    fn temp_root(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("agent-harness-goal-lineage-{name}-{nanos}"))
    }
}
