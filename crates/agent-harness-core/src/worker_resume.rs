//! Durable, exact-owner safe-resume intents for worker result delivery.
//!
//! This module deliberately does not inspect runtime/session files. The caller
//! supplies a lane-activity receipt, and the store records the receipt used to
//! allow or block scheduling. Mailbox claims and intent creation can be made in
//! one SQLite transaction so a crash cannot leave a claimed result without a
//! durable continuation intent.

use std::error::Error;
use std::fmt;

use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};

#[cfg(test)]
use crate::worker_result_mailbox::coalesced_unread_for_exact_owner;
use crate::worker_result_mailbox::{
    ExactWorkerResultOwnerV1, WorkerResultMailboxClaimOptionsV1, WorkerResultMailboxError,
    WorkerResultOwnerV1, acknowledge_coordinator_claim_in_transaction,
    claim_unread_for_coordinator_in_transaction, coalesced_unread_for_coordinator,
};

pub const WORKER_RESUME_INTENT_SCHEMA: &str = "agent-harness.worker-resume-intent.v1";
pub const LANE_ACTIVITY_RECEIPT_SCHEMA: &str = "agent-harness.worker-resume-lane-activity.v1";

const RESUME_INTENT_TABLE: &str = "worker_resume_intents_v1";
const MAX_RESUME_BATCH_ITEMS: usize = 100;
const MAX_RESUME_LEASE_MS: i64 = 24 * 60 * 60 * 1000;

#[derive(Debug)]
pub enum WorkerResumeError {
    Sqlite(rusqlite::Error),
    Json(serde_json::Error),
    Mailbox(WorkerResultMailboxError),
    InvalidInput(String),
    InvalidStoredData(String),
    LegacyOwnerDenied,
    LeaseTokenConflict { lease_token: String },
    InvalidTransition { intent_id: String, reason: String },
}

impl fmt::Display for WorkerResumeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sqlite(error) => write!(formatter, "resume SQLite error: {error}"),
            Self::Json(error) => write!(formatter, "resume JSON error: {error}"),
            Self::Mailbox(error) => write!(formatter, "resume mailbox error: {error}"),
            Self::InvalidInput(reason) => write!(formatter, "invalid resume input: {reason}"),
            Self::InvalidStoredData(reason) => {
                write!(formatter, "invalid stored resume intent: {reason}")
            }
            Self::LegacyOwnerDenied => write!(
                formatter,
                "legacy-incomplete worker result owners cannot schedule automatic resume"
            ),
            Self::LeaseTokenConflict { lease_token } => write!(
                formatter,
                "resume lease token `{lease_token}` is already bound to another intent"
            ),
            Self::InvalidTransition { intent_id, reason } => {
                write!(
                    formatter,
                    "resume intent `{intent_id}` cannot transition: {reason}"
                )
            }
        }
    }
}

impl Error for WorkerResumeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Sqlite(error) => Some(error),
            Self::Json(error) => Some(error),
            Self::Mailbox(error) => Some(error),
            _ => None,
        }
    }
}

impl From<rusqlite::Error> for WorkerResumeError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sqlite(error)
    }
}

impl From<serde_json::Error> for WorkerResumeError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

impl From<WorkerResultMailboxError> for WorkerResumeError {
    fn from(error: WorkerResultMailboxError) -> Self {
        Self::Mailbox(error)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LaneActivityReceiptV1 {
    pub schema: String,
    pub lane_active: bool,
    pub observed_at_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocker_reason: Option<String>,
}

impl LaneActivityReceiptV1 {
    pub fn idle(observed_at_ms: i64) -> Result<Self, WorkerResumeError> {
        let receipt = Self {
            schema: LANE_ACTIVITY_RECEIPT_SCHEMA.to_string(),
            lane_active: false,
            observed_at_ms,
            blocker_reason: None,
        };
        receipt.validate()?;
        Ok(receipt)
    }

    pub fn active(
        observed_at_ms: i64,
        blocker_reason: impl Into<String>,
    ) -> Result<Self, WorkerResumeError> {
        let receipt = Self {
            schema: LANE_ACTIVITY_RECEIPT_SCHEMA.to_string(),
            lane_active: true,
            observed_at_ms,
            blocker_reason: Some(blocker_reason.into()),
        };
        receipt.validate()?;
        Ok(receipt)
    }

    pub fn validate(&self) -> Result<(), WorkerResumeError> {
        if self.schema != LANE_ACTIVITY_RECEIPT_SCHEMA {
            return Err(invalid_input(format!(
                "unsupported lane activity receipt schema `{}`",
                self.schema
            )));
        }
        validate_timestamp("observedAtMs", self.observed_at_ms)?;
        match (self.lane_active, self.blocker_reason.as_deref()) {
            (true, Some(reason)) => validate_text("blockerReason", reason, 1024),
            (true, None) => Err(invalid_input(
                "an active lane receipt must include blockerReason",
            )),
            (false, Some(_)) => Err(invalid_input(
                "an idle lane receipt cannot include blockerReason",
            )),
            (false, None) => Ok(()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkerResumeIntentStateV1 {
    Pending,
    Leased,
    Enqueued,
    Consumed,
    Quarantined,
}

impl WorkerResumeIntentStateV1 {
    fn as_db(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Leased => "leased",
            Self::Enqueued => "enqueued",
            Self::Consumed => "consumed",
            Self::Quarantined => "quarantined",
        }
    }

    fn from_db(value: &str) -> Result<Self, WorkerResumeError> {
        match value {
            "pending" => Ok(Self::Pending),
            "leased" => Ok(Self::Leased),
            "enqueued" => Ok(Self::Enqueued),
            "consumed" => Ok(Self::Consumed),
            "quarantined" => Ok(Self::Quarantined),
            other => Err(WorkerResumeError::InvalidStoredData(format!(
                "unsupported state `{other}`"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerResumeIntentLeaseV1 {
    pub lease_token: String,
    pub lease_owner: String,
    pub lease_expires_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerResumeIntentV1 {
    pub schema: String,
    pub intent_id: String,
    pub owner_key: String,
    pub owner: ExactWorkerResultOwnerV1,
    pub virtual_session_id: String,
    pub generation: i64,
    pub mailbox_cursor: i64,
    pub mailbox_ids: Vec<i64>,
    pub mailbox_claim_tokens: Vec<String>,
    pub state: WorkerResumeIntentStateV1,
    pub lane_activity: LaneActivityReceiptV1,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease: Option<WorkerResumeIntentLeaseV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation_queue_id: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enqueued_at_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumed_at_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerResumeCreateOptionsV1 {
    pub claim_token: String,
    pub claimant_id: String,
    pub now_ms: i64,
    pub mailbox_lease_ms: i64,
    pub limit: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerResumeCreateDispositionV1 {
    NoUnreadResults,
    Created,
    Coalesced,
    Replayed,
    BlockedByActiveLane,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerResumeCreateOutcomeV1 {
    pub disposition: WorkerResumeCreateDispositionV1,
    pub intent: Option<WorkerResumeIntentV1>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerResumeLeaseOptionsV1 {
    pub lease_token: String,
    pub lease_owner: String,
    pub now_ms: i64,
    pub lease_ms: i64,
}

/// Adds the resume-intent table to the same SQLite database used by workers.
/// This migration is additive and safe to repeat.
pub fn initialize_worker_resume_schema(conn: &Connection) -> Result<(), WorkerResumeError> {
    conn.execute_batch(&format!(
        "
        CREATE TABLE IF NOT EXISTS {RESUME_INTENT_TABLE} (
            intent_id TEXT PRIMARY KEY,
            schema TEXT NOT NULL,
            owner_key TEXT NOT NULL,
            owner_json TEXT NOT NULL,
            virtual_session_id TEXT NOT NULL,
            generation INTEGER NOT NULL,
            mailbox_cursor INTEGER NOT NULL,
            mailbox_ids_json TEXT NOT NULL,
            mailbox_claim_tokens_json TEXT NOT NULL,
            state TEXT NOT NULL,
            lane_activity_json TEXT NOT NULL,
            blocker_reason TEXT,
            lease_token TEXT,
            lease_owner TEXT,
            lease_expires_at_ms INTEGER,
            continuation_queue_id TEXT,
            created_at_ms INTEGER NOT NULL,
            updated_at_ms INTEGER NOT NULL,
            enqueued_at_ms INTEGER,
            consumed_at_ms INTEGER,
            UNIQUE(owner_key, virtual_session_id, generation),
            CHECK (schema = '{WORKER_RESUME_INTENT_SCHEMA}'),
            CHECK (state IN ('pending', 'leased', 'enqueued', 'consumed', 'quarantined')),
            CHECK (
                (state = 'leased' AND lease_token IS NOT NULL AND lease_owner IS NOT NULL
                    AND lease_expires_at_ms IS NOT NULL)
                OR
                (state != 'leased' AND lease_token IS NULL AND lease_owner IS NULL
                    AND lease_expires_at_ms IS NULL)
            ),
            CHECK (
                (state IN ('enqueued', 'consumed') AND continuation_queue_id IS NOT NULL
                    AND enqueued_at_ms IS NOT NULL)
                OR
                (state NOT IN ('enqueued', 'consumed') AND continuation_queue_id IS NULL
                    AND enqueued_at_ms IS NULL)
            ),
            CHECK (
                (state = 'consumed' AND consumed_at_ms IS NOT NULL)
                OR
                (state != 'consumed' AND consumed_at_ms IS NULL)
            ),
            CHECK (
                (state = 'quarantined' AND blocker_reason IS NOT NULL)
                OR
                (state != 'quarantined' AND blocker_reason IS NULL)
            )
        );
        CREATE INDEX IF NOT EXISTS worker_resume_intents_schedule_idx
            ON {RESUME_INTENT_TABLE}(owner_key, virtual_session_id, state, generation);
        CREATE UNIQUE INDEX IF NOT EXISTS worker_resume_intents_lease_token_idx
            ON {RESUME_INTENT_TABLE}(lease_token) WHERE lease_token IS NOT NULL;
        "
    ))?;
    Ok(())
}

/// Atomically observes/claims unread mailbox rows and creates a durable intent.
/// `LegacyIncomplete` is always rejected before any mailbox mutation.
pub fn create_or_coalesce_resume_intent_in_transaction(
    transaction: &Transaction<'_>,
    owner: &WorkerResultOwnerV1,
    lane_activity: &LaneActivityReceiptV1,
    options: &WorkerResumeCreateOptionsV1,
) -> Result<WorkerResumeCreateOutcomeV1, WorkerResumeError> {
    initialize_worker_resume_schema(transaction)?;
    lane_activity.validate()?;
    validate_create_options(options)?;
    let exact_owner = require_exact_owner(owner)?;
    let owner_key = exact_owner.coordinator_key()?;

    if lane_activity.lane_active {
        let unread = coalesced_unread_for_coordinator(transaction, exact_owner, options.limit)?;
        if unread.records.is_empty() {
            return Ok(no_unread());
        }
        let mailbox_ids = unread
            .records
            .iter()
            .map(|record| record.mailbox_id)
            .collect::<Vec<_>>();
        let cursor = mailbox_ids.iter().copied().max().unwrap_or(0);
        if let Some(existing) = latest_intent_for_owner_state(
            transaction,
            &owner_key,
            &exact_owner.virtual_session_id,
            WorkerResumeIntentStateV1::Quarantined,
        )? && existing.mailbox_ids == mailbox_ids
            && existing.lane_activity == *lane_activity
        {
            return Ok(WorkerResumeCreateOutcomeV1 {
                disposition: WorkerResumeCreateDispositionV1::Replayed,
                intent: Some(existing),
            });
        }
        let intent = insert_intent(
            transaction,
            exact_owner,
            &owner_key,
            next_generation(transaction, &owner_key, &exact_owner.virtual_session_id)?,
            cursor,
            mailbox_ids,
            Vec::new(),
            WorkerResumeIntentStateV1::Quarantined,
            lane_activity,
            options.now_ms,
        )?;
        return Ok(WorkerResumeCreateOutcomeV1 {
            disposition: WorkerResumeCreateDispositionV1::BlockedByActiveLane,
            intent: Some(intent),
        });
    }

    let claim = claim_unread_for_coordinator_in_transaction(
        transaction,
        exact_owner,
        &WorkerResultMailboxClaimOptionsV1 {
            claim_token: options.claim_token.clone(),
            claimant_id: options.claimant_id.clone(),
            now_ms: options.now_ms,
            lease_ms: options.mailbox_lease_ms,
            limit: options.limit,
        },
    )?;
    if claim.records.is_empty() {
        return Ok(no_unread());
    }

    let claimed_ids = claim
        .records
        .iter()
        .map(|record| record.mailbox_id)
        .collect::<Vec<_>>();
    if let Some(mut pending) = latest_intent_for_owner_state(
        transaction,
        &owner_key,
        &exact_owner.virtual_session_id,
        WorkerResumeIntentStateV1::Pending,
    )? {
        let previous_ids = pending.mailbox_ids.clone();
        merge_sorted_unique(&mut pending.mailbox_ids, claimed_ids);
        merge_sorted_unique(
            &mut pending.mailbox_claim_tokens,
            vec![options.claim_token.clone()],
        );
        pending.mailbox_cursor = pending.mailbox_ids.iter().copied().max().unwrap_or(0);
        if pending.mailbox_ids == previous_ids {
            return Ok(WorkerResumeCreateOutcomeV1 {
                disposition: WorkerResumeCreateDispositionV1::Replayed,
                intent: Some(pending),
            });
        }
        transaction.execute(
            &format!(
                "UPDATE {RESUME_INTENT_TABLE}
                 SET mailbox_cursor=?1, mailbox_ids_json=?2,
                     mailbox_claim_tokens_json=?3, updated_at_ms=?4
                 WHERE intent_id=?5 AND state='pending'"
            ),
            params![
                pending.mailbox_cursor,
                serde_json::to_string(&pending.mailbox_ids)?,
                serde_json::to_string(&pending.mailbox_claim_tokens)?,
                options.now_ms,
                pending.intent_id,
            ],
        )?;
        pending.updated_at_ms = options.now_ms;
        return Ok(WorkerResumeCreateOutcomeV1 {
            disposition: WorkerResumeCreateDispositionV1::Coalesced,
            intent: Some(pending),
        });
    }

    let cursor = claimed_ids.iter().copied().max().unwrap_or(0);
    let intent = insert_intent(
        transaction,
        exact_owner,
        &owner_key,
        next_generation(transaction, &owner_key, &exact_owner.virtual_session_id)?,
        cursor,
        claimed_ids,
        vec![options.claim_token.clone()],
        WorkerResumeIntentStateV1::Pending,
        lane_activity,
        options.now_ms,
    )?;
    Ok(WorkerResumeCreateOutcomeV1 {
        disposition: if claim.replayed_existing_claim {
            WorkerResumeCreateDispositionV1::Replayed
        } else {
            WorkerResumeCreateDispositionV1::Created
        },
        intent: Some(intent),
    })
}

/// Claims the oldest pending intent for an exact owner. Expired intent leases
/// are returned to pending first, making the operation restart safe.
pub fn claim_next_resume_intent_in_transaction(
    transaction: &Transaction<'_>,
    owner: &ExactWorkerResultOwnerV1,
    options: &WorkerResumeLeaseOptionsV1,
) -> Result<Option<WorkerResumeIntentV1>, WorkerResumeError> {
    initialize_worker_resume_schema(transaction)?;
    validate_lease_options(options)?;
    let owner_key = owner.coordinator_key()?;
    release_expired_resume_leases_in_transaction(transaction, options.now_ms)?;

    if let Some(bound_intent) = transaction
        .query_row(
            &format!("SELECT intent_id FROM {RESUME_INTENT_TABLE} WHERE lease_token=?1 LIMIT 1"),
            params![options.lease_token],
            |row| row.get::<_, String>(0),
        )
        .optional()?
    {
        let intent = find_intent_in_transaction(transaction, &bound_intent)?
            .ok_or_else(|| WorkerResumeError::InvalidStoredData("lease index is stale".into()))?;
        if intent.owner_key != owner_key
            || intent
                .lease
                .as_ref()
                .map(|lease| lease.lease_owner.as_str())
                != Some(options.lease_owner.as_str())
        {
            return Err(WorkerResumeError::LeaseTokenConflict {
                lease_token: options.lease_token.clone(),
            });
        }
        return Ok(Some(intent));
    }

    let intent_id = transaction
        .query_row(
            &format!(
                "SELECT intent_id FROM {RESUME_INTENT_TABLE}
                 WHERE owner_key=?1 AND virtual_session_id=?2 AND state='pending'
                 ORDER BY generation ASC LIMIT 1"
            ),
            params![owner_key, owner.virtual_session_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    let Some(intent_id) = intent_id else {
        return Ok(None);
    };
    let expiry = options.now_ms.saturating_add(options.lease_ms);
    let updated = transaction.execute(
        &format!(
            "UPDATE {RESUME_INTENT_TABLE}
             SET state='leased', lease_token=?1, lease_owner=?2,
                 lease_expires_at_ms=?3, updated_at_ms=?4
             WHERE intent_id=?5 AND state='pending'"
        ),
        params![
            options.lease_token,
            options.lease_owner,
            expiry,
            options.now_ms,
            intent_id,
        ],
    )?;
    if updated != 1 {
        return Ok(None);
    }
    find_intent_in_transaction(transaction, &intent_id)
}

pub fn release_expired_resume_leases_in_transaction(
    transaction: &Transaction<'_>,
    now_ms: i64,
) -> Result<usize, WorkerResumeError> {
    validate_timestamp("nowMs", now_ms)?;
    Ok(transaction.execute(
        &format!(
            "UPDATE {RESUME_INTENT_TABLE}
             SET state='pending', lease_token=NULL, lease_owner=NULL,
                 lease_expires_at_ms=NULL, updated_at_ms=?1
             WHERE state='leased' AND lease_expires_at_ms <= ?1"
        ),
        params![now_ms],
    )?)
}

pub fn release_resume_intent_in_transaction(
    transaction: &Transaction<'_>,
    owner: &ExactWorkerResultOwnerV1,
    intent_id: &str,
    lease_token: &str,
    now_ms: i64,
) -> Result<bool, WorkerResumeError> {
    validate_identifier("intentId", intent_id)?;
    validate_identifier("leaseToken", lease_token)?;
    validate_timestamp("nowMs", now_ms)?;
    let owner_key = owner.coordinator_key()?;
    let transitioned = transaction.execute(
        &format!(
            "UPDATE {RESUME_INTENT_TABLE}
             SET state='pending', lease_token=NULL, lease_owner=NULL,
                 lease_expires_at_ms=NULL, updated_at_ms=?1
             WHERE intent_id=?2 AND owner_key=?3 AND state='leased' AND lease_token=?4"
        ),
        params![now_ms, intent_id, owner_key, lease_token],
    )?;
    Ok(transitioned == 1)
}

/// Integration seam for the runtime continuation enqueue. Insert the runtime
/// queue item and call this function in the same worker SQLite transaction.
pub fn mark_resume_intent_enqueued_in_transaction(
    transaction: &Transaction<'_>,
    owner: &ExactWorkerResultOwnerV1,
    intent_id: &str,
    lease_token: &str,
    continuation_queue_id: &str,
    now_ms: i64,
) -> Result<WorkerResumeIntentV1, WorkerResumeError> {
    validate_identifier("intentId", intent_id)?;
    validate_identifier("leaseToken", lease_token)?;
    validate_identifier("continuationQueueId", continuation_queue_id)?;
    validate_timestamp("nowMs", now_ms)?;
    let owner_key = owner.coordinator_key()?;
    let transitioned = transaction.execute(
        &format!(
            "UPDATE {RESUME_INTENT_TABLE}
             SET state='enqueued', lease_token=NULL, lease_owner=NULL,
                 lease_expires_at_ms=NULL, continuation_queue_id=?1,
                 enqueued_at_ms=?2, updated_at_ms=?2
             WHERE intent_id=?3 AND owner_key=?4 AND state='leased' AND lease_token=?5"
        ),
        params![
            continuation_queue_id,
            now_ms,
            intent_id,
            owner_key,
            lease_token,
        ],
    )?;
    if transitioned != 1 {
        if let Some(existing) = find_intent_in_transaction(transaction, intent_id)?
            && existing.owner_key == owner_key
            && existing.state == WorkerResumeIntentStateV1::Enqueued
            && existing.continuation_queue_id.as_deref() == Some(continuation_queue_id)
        {
            return Ok(existing);
        }
        return Err(WorkerResumeError::InvalidTransition {
            intent_id: intent_id.to_string(),
            reason: "expected matching leased state".to_string(),
        });
    }
    find_intent_in_transaction(transaction, intent_id)?.ok_or_else(|| {
        WorkerResumeError::InvalidStoredData("enqueued intent disappeared".to_string())
    })
}

/// Marks a continuation consumed and acknowledges all mailbox claims atomically.
pub fn consume_resume_intent_in_transaction(
    transaction: &Transaction<'_>,
    owner: &ExactWorkerResultOwnerV1,
    intent_id: &str,
    continuation_queue_id: &str,
    now_ms: i64,
) -> Result<WorkerResumeIntentV1, WorkerResumeError> {
    validate_identifier("intentId", intent_id)?;
    validate_identifier("continuationQueueId", continuation_queue_id)?;
    validate_timestamp("nowMs", now_ms)?;
    let owner_key = owner.coordinator_key()?;
    let intent = find_intent_in_transaction(transaction, intent_id)?.ok_or_else(|| {
        WorkerResumeError::InvalidTransition {
            intent_id: intent_id.to_string(),
            reason: "intent does not exist".to_string(),
        }
    })?;
    if intent.owner_key != owner_key
        || intent.continuation_queue_id.as_deref() != Some(continuation_queue_id)
    {
        return Err(WorkerResumeError::InvalidTransition {
            intent_id: intent_id.to_string(),
            reason: "owner or continuation queue does not match".to_string(),
        });
    }
    if intent.state == WorkerResumeIntentStateV1::Consumed {
        return Ok(intent);
    }
    if intent.state != WorkerResumeIntentStateV1::Enqueued {
        return Err(WorkerResumeError::InvalidTransition {
            intent_id: intent_id.to_string(),
            reason: "expected enqueued state".to_string(),
        });
    }
    for claim_token in &intent.mailbox_claim_tokens {
        acknowledge_coordinator_claim_in_transaction(transaction, owner, claim_token, now_ms)?;
    }
    transaction.execute(
        &format!(
            "UPDATE {RESUME_INTENT_TABLE}
             SET state='consumed', consumed_at_ms=?1, updated_at_ms=?1
             WHERE intent_id=?2 AND owner_key=?3 AND state='enqueued'
               AND continuation_queue_id=?4"
        ),
        params![now_ms, intent_id, owner_key, continuation_queue_id],
    )?;
    find_intent_in_transaction(transaction, intent_id)?.ok_or_else(|| {
        WorkerResumeError::InvalidStoredData("consumed intent disappeared".to_string())
    })
}

pub fn find_intent_in_transaction(
    transaction: &Transaction<'_>,
    intent_id: &str,
) -> Result<Option<WorkerResumeIntentV1>, WorkerResumeError> {
    validate_identifier("intentId", intent_id)?;
    raw_intent_by_id(transaction, intent_id)?
        .map(RawResumeIntent::decode)
        .transpose()
}

#[allow(clippy::too_many_arguments)]
fn insert_intent(
    transaction: &Transaction<'_>,
    owner: &ExactWorkerResultOwnerV1,
    owner_key: &str,
    generation: i64,
    mailbox_cursor: i64,
    mut mailbox_ids: Vec<i64>,
    mut mailbox_claim_tokens: Vec<String>,
    state: WorkerResumeIntentStateV1,
    lane_activity: &LaneActivityReceiptV1,
    now_ms: i64,
) -> Result<WorkerResumeIntentV1, WorkerResumeError> {
    mailbox_ids.sort_unstable();
    mailbox_ids.dedup();
    mailbox_claim_tokens.sort();
    mailbox_claim_tokens.dedup();
    let intent_id = format!("resume/{owner_key}/{generation}");
    transaction.execute(
        &format!(
            "INSERT INTO {RESUME_INTENT_TABLE} (
                intent_id, schema, owner_key, owner_json, virtual_session_id,
                generation, mailbox_cursor, mailbox_ids_json,
                mailbox_claim_tokens_json, state, lane_activity_json,
                blocker_reason, created_at_ms, updated_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?13)"
        ),
        params![
            intent_id,
            WORKER_RESUME_INTENT_SCHEMA,
            owner_key,
            serde_json::to_string(owner)?,
            owner.virtual_session_id,
            generation,
            mailbox_cursor,
            serde_json::to_string(&mailbox_ids)?,
            serde_json::to_string(&mailbox_claim_tokens)?,
            state.as_db(),
            serde_json::to_string(lane_activity)?,
            lane_activity.blocker_reason,
            now_ms,
        ],
    )?;
    find_intent_in_transaction(transaction, &intent_id)?.ok_or_else(|| {
        WorkerResumeError::InvalidStoredData("inserted intent disappeared".to_string())
    })
}

fn latest_intent_for_owner_state(
    conn: &Connection,
    owner_key: &str,
    virtual_session_id: &str,
    state: WorkerResumeIntentStateV1,
) -> Result<Option<WorkerResumeIntentV1>, WorkerResumeError> {
    let raw = conn
        .query_row(
            &format!(
                "SELECT intent_id, schema, owner_key, owner_json, virtual_session_id,
                        generation, mailbox_cursor, mailbox_ids_json,
                        mailbox_claim_tokens_json, state, lane_activity_json,
                        blocker_reason, lease_token, lease_owner, lease_expires_at_ms,
                        continuation_queue_id, created_at_ms, updated_at_ms,
                        enqueued_at_ms, consumed_at_ms
                 FROM {RESUME_INTENT_TABLE}
                 WHERE owner_key=?1 AND virtual_session_id=?2 AND state=?3
                 ORDER BY generation DESC LIMIT 1"
            ),
            params![owner_key, virtual_session_id, state.as_db()],
            RawResumeIntent::from_row,
        )
        .optional()?;
    raw.map(RawResumeIntent::decode).transpose()
}

fn raw_intent_by_id(
    conn: &Connection,
    intent_id: &str,
) -> Result<Option<RawResumeIntent>, WorkerResumeError> {
    Ok(conn
        .query_row(
            &format!(
                "SELECT intent_id, schema, owner_key, owner_json, virtual_session_id,
                        generation, mailbox_cursor, mailbox_ids_json,
                        mailbox_claim_tokens_json, state, lane_activity_json,
                        blocker_reason, lease_token, lease_owner, lease_expires_at_ms,
                        continuation_queue_id, created_at_ms, updated_at_ms,
                        enqueued_at_ms, consumed_at_ms
                 FROM {RESUME_INTENT_TABLE} WHERE intent_id=?1"
            ),
            params![intent_id],
            RawResumeIntent::from_row,
        )
        .optional()?)
}

fn next_generation(
    conn: &Connection,
    owner_key: &str,
    virtual_session_id: &str,
) -> Result<i64, WorkerResumeError> {
    let current: i64 = conn.query_row(
        &format!(
            "SELECT COALESCE(MAX(generation), 0) FROM {RESUME_INTENT_TABLE}
             WHERE owner_key=?1 AND virtual_session_id=?2"
        ),
        params![owner_key, virtual_session_id],
        |row| row.get(0),
    )?;
    Ok(current.saturating_add(1))
}

struct RawResumeIntent {
    intent_id: String,
    schema: String,
    owner_key: String,
    owner_json: String,
    virtual_session_id: String,
    generation: i64,
    mailbox_cursor: i64,
    mailbox_ids_json: String,
    mailbox_claim_tokens_json: String,
    state: String,
    lane_activity_json: String,
    blocker_reason: Option<String>,
    lease_token: Option<String>,
    lease_owner: Option<String>,
    lease_expires_at_ms: Option<i64>,
    continuation_queue_id: Option<String>,
    created_at_ms: i64,
    updated_at_ms: i64,
    enqueued_at_ms: Option<i64>,
    consumed_at_ms: Option<i64>,
}

impl RawResumeIntent {
    fn from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            intent_id: row.get(0)?,
            schema: row.get(1)?,
            owner_key: row.get(2)?,
            owner_json: row.get(3)?,
            virtual_session_id: row.get(4)?,
            generation: row.get(5)?,
            mailbox_cursor: row.get(6)?,
            mailbox_ids_json: row.get(7)?,
            mailbox_claim_tokens_json: row.get(8)?,
            state: row.get(9)?,
            lane_activity_json: row.get(10)?,
            blocker_reason: row.get(11)?,
            lease_token: row.get(12)?,
            lease_owner: row.get(13)?,
            lease_expires_at_ms: row.get(14)?,
            continuation_queue_id: row.get(15)?,
            created_at_ms: row.get(16)?,
            updated_at_ms: row.get(17)?,
            enqueued_at_ms: row.get(18)?,
            consumed_at_ms: row.get(19)?,
        })
    }

    fn decode(self) -> Result<WorkerResumeIntentV1, WorkerResumeError> {
        if self.schema != WORKER_RESUME_INTENT_SCHEMA {
            return Err(WorkerResumeError::InvalidStoredData(format!(
                "unsupported schema `{}`",
                self.schema
            )));
        }
        let owner: ExactWorkerResultOwnerV1 = serde_json::from_str(&self.owner_json)?;
        owner.validate()?;
        let expected_owner_key = owner.coordinator_key()?;
        if expected_owner_key != self.owner_key
            || owner.virtual_session_id != self.virtual_session_id
        {
            return Err(WorkerResumeError::InvalidStoredData(
                "owner columns do not match exact owner JSON".to_string(),
            ));
        }
        let lane_activity: LaneActivityReceiptV1 = serde_json::from_str(&self.lane_activity_json)?;
        lane_activity.validate()?;
        if lane_activity.blocker_reason != self.blocker_reason {
            return Err(WorkerResumeError::InvalidStoredData(
                "blockerReason column does not match lane activity receipt".to_string(),
            ));
        }
        let state = WorkerResumeIntentStateV1::from_db(&self.state)?;
        let lease = if state == WorkerResumeIntentStateV1::Leased {
            Some(WorkerResumeIntentLeaseV1 {
                lease_token: self.lease_token.ok_or_else(|| {
                    WorkerResumeError::InvalidStoredData("leased intent has no token".to_string())
                })?,
                lease_owner: self.lease_owner.ok_or_else(|| {
                    WorkerResumeError::InvalidStoredData("leased intent has no owner".to_string())
                })?,
                lease_expires_at_ms: self.lease_expires_at_ms.ok_or_else(|| {
                    WorkerResumeError::InvalidStoredData("leased intent has no expiry".to_string())
                })?,
            })
        } else {
            None
        };
        let mailbox_ids: Vec<i64> = serde_json::from_str(&self.mailbox_ids_json)?;
        let mailbox_claim_tokens: Vec<String> =
            serde_json::from_str(&self.mailbox_claim_tokens_json)?;
        if mailbox_ids.is_empty() {
            return Err(WorkerResumeError::InvalidStoredData(
                "intent has no mailbox ids".to_string(),
            ));
        }
        if mailbox_ids.iter().copied().max() != Some(self.mailbox_cursor) {
            return Err(WorkerResumeError::InvalidStoredData(
                "mailboxCursor is not the maximum mailbox id".to_string(),
            ));
        }
        if state != WorkerResumeIntentStateV1::Quarantined && mailbox_claim_tokens.is_empty() {
            return Err(WorkerResumeError::InvalidStoredData(
                "schedulable intent has no mailbox claim tokens".to_string(),
            ));
        }
        Ok(WorkerResumeIntentV1 {
            schema: self.schema,
            intent_id: self.intent_id,
            owner_key: self.owner_key,
            owner,
            virtual_session_id: self.virtual_session_id,
            generation: self.generation,
            mailbox_cursor: self.mailbox_cursor,
            mailbox_ids,
            mailbox_claim_tokens,
            state,
            lane_activity,
            lease,
            continuation_queue_id: self.continuation_queue_id,
            created_at_ms: self.created_at_ms,
            updated_at_ms: self.updated_at_ms,
            enqueued_at_ms: self.enqueued_at_ms,
            consumed_at_ms: self.consumed_at_ms,
        })
    }
}

fn require_exact_owner(
    owner: &WorkerResultOwnerV1,
) -> Result<&ExactWorkerResultOwnerV1, WorkerResumeError> {
    match owner {
        WorkerResultOwnerV1::Exact(owner) => {
            owner.validate()?;
            Ok(owner)
        }
        WorkerResultOwnerV1::LegacyIncomplete(_) => Err(WorkerResumeError::LegacyOwnerDenied),
    }
}

fn validate_create_options(options: &WorkerResumeCreateOptionsV1) -> Result<(), WorkerResumeError> {
    validate_identifier("claimToken", &options.claim_token)?;
    validate_identifier("claimantId", &options.claimant_id)?;
    validate_timestamp("nowMs", options.now_ms)?;
    validate_lease("mailboxLeaseMs", options.mailbox_lease_ms)?;
    if options.limit == 0 || options.limit > MAX_RESUME_BATCH_ITEMS {
        return Err(invalid_input(format!(
            "limit must be between 1 and {MAX_RESUME_BATCH_ITEMS}"
        )));
    }
    Ok(())
}

fn validate_lease_options(options: &WorkerResumeLeaseOptionsV1) -> Result<(), WorkerResumeError> {
    validate_identifier("leaseToken", &options.lease_token)?;
    validate_identifier("leaseOwner", &options.lease_owner)?;
    validate_timestamp("nowMs", options.now_ms)?;
    validate_lease("leaseMs", options.lease_ms)
}

fn validate_lease(field: &str, value: i64) -> Result<(), WorkerResumeError> {
    if !(1..=MAX_RESUME_LEASE_MS).contains(&value) {
        return Err(invalid_input(format!(
            "{field} must be between 1 and {MAX_RESUME_LEASE_MS}"
        )));
    }
    Ok(())
}

fn validate_timestamp(field: &str, value: i64) -> Result<(), WorkerResumeError> {
    if value < 0 {
        return Err(invalid_input(format!("{field} cannot be negative")));
    }
    Ok(())
}

fn validate_identifier(field: &str, value: &str) -> Result<(), WorkerResumeError> {
    validate_text(field, value, 1024)
}

fn validate_text(field: &str, value: &str, max_bytes: usize) -> Result<(), WorkerResumeError> {
    if value.is_empty() {
        return Err(invalid_input(format!("{field} is empty")));
    }
    if value.len() > max_bytes {
        return Err(invalid_input(format!(
            "{field} is {} bytes; maximum is {max_bytes}",
            value.len()
        )));
    }
    if value.chars().any(char::is_control) {
        return Err(invalid_input(format!(
            "{field} contains a control character"
        )));
    }
    Ok(())
}

fn merge_sorted_unique<T: Ord>(target: &mut Vec<T>, incoming: Vec<T>) {
    target.extend(incoming);
    target.sort();
    target.dedup();
}

fn no_unread() -> WorkerResumeCreateOutcomeV1 {
    WorkerResumeCreateOutcomeV1 {
        disposition: WorkerResumeCreateDispositionV1::NoUnreadResults,
        intent: None,
    }
}

fn invalid_input(reason: impl Into<String>) -> WorkerResumeError {
    WorkerResumeError::InvalidInput(reason.into())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::lane::FullLaneKeyV1;
    use crate::worker_result_mailbox::{
        LEGACY_WORKER_RESULT_OWNER_SCHEMA, LegacyIncompleteWorkerResultOwnerV0,
        LegacyOwnerMissingAxisV1, WorkerResultEnvelopeV1, WorkerResultMailboxInsertV1,
        WorkerResultOutcomeV1, initialize_worker_result_mailbox_schema, insert_terminal_result,
    };

    use super::*;

    #[test]
    fn active_parent_quarantines_without_claiming_then_idle_schedules_once() {
        let mut conn = setup();
        let owner = owner("telegram", "account-a", "dm-a", "agent-a", "root-a");
        insert_result(&conn, &owner, "terminal/active/1", 100);

        let transaction = conn.transaction().unwrap();
        let blocked = create_or_coalesce_resume_intent_in_transaction(
            &transaction,
            &WorkerResultOwnerV1::Exact(owner.clone()),
            &LaneActivityReceiptV1::active(110, "parent lane still executing").unwrap(),
            &create_options("claim/active", 110),
        )
        .unwrap();
        transaction.commit().unwrap();
        assert_eq!(
            blocked.disposition,
            WorkerResumeCreateDispositionV1::BlockedByActiveLane
        );
        let blocked_intent = blocked.intent.unwrap();
        assert_eq!(blocked_intent.state, WorkerResumeIntentStateV1::Quarantined);
        assert_eq!(
            blocked_intent.lane_activity.blocker_reason.as_deref(),
            Some("parent lane still executing")
        );
        assert_eq!(
            coalesced_unread_for_exact_owner(&conn, &owner, 10)
                .unwrap()
                .records
                .len(),
            1
        );

        let transaction = conn.transaction().unwrap();
        let scheduled = create_or_coalesce_resume_intent_in_transaction(
            &transaction,
            &WorkerResultOwnerV1::Exact(owner.clone()),
            &LaneActivityReceiptV1::idle(120).unwrap(),
            &create_options("claim/idle", 120),
        )
        .unwrap();
        transaction.commit().unwrap();
        assert_eq!(
            scheduled.disposition,
            WorkerResumeCreateDispositionV1::Created
        );
        assert_eq!(
            scheduled.intent.unwrap().state,
            WorkerResumeIntentStateV1::Pending
        );
    }

    #[test]
    fn two_unread_results_coalesce_into_one_monotonic_intent() {
        let mut conn = setup();
        let owner = owner("discord", "account-a", "channel-a", "agent-a", "root-a");
        let mut sibling_owner = owner.clone();
        sibling_owner.source_queue_id = "child/queue-b".to_string();
        sibling_owner.validate().unwrap();
        insert_result(&conn, &owner, "terminal/coalesce/1", 100);
        insert_result(&conn, &sibling_owner, "terminal/coalesce/2", 101);

        let transaction = conn.transaction().unwrap();
        let outcome = create_or_coalesce_resume_intent_in_transaction(
            &transaction,
            &WorkerResultOwnerV1::Exact(owner),
            &LaneActivityReceiptV1::idle(110).unwrap(),
            &create_options("claim/coalesce", 110),
        )
        .unwrap();
        transaction.commit().unwrap();
        let intent = outcome.intent.unwrap();
        assert_eq!(intent.generation, 1);
        assert_eq!(intent.mailbox_ids.len(), 2);
        assert_eq!(intent.mailbox_cursor, *intent.mailbox_ids.last().unwrap());
    }

    #[test]
    fn restart_replays_same_claim_and_intent_without_duplication() {
        let path = temp_db("restart");
        let owner = owner("discord", "account-a", "channel-a", "agent-a", "root-a");
        let first_id;
        {
            let mut conn = Connection::open(&path).unwrap();
            initialize_worker_result_mailbox_schema(&conn).unwrap();
            initialize_worker_resume_schema(&conn).unwrap();
            insert_result(&conn, &owner, "terminal/restart/1", 100);
            let transaction = conn.transaction().unwrap();
            let outcome = create_or_coalesce_resume_intent_in_transaction(
                &transaction,
                &WorkerResultOwnerV1::Exact(owner.clone()),
                &LaneActivityReceiptV1::idle(110).unwrap(),
                &create_options("claim/restart", 110),
            )
            .unwrap();
            first_id = outcome.intent.unwrap().intent_id;
            transaction.commit().unwrap();
        }
        {
            let mut conn = Connection::open(&path).unwrap();
            let transaction = conn.transaction().unwrap();
            let replay = create_or_coalesce_resume_intent_in_transaction(
                &transaction,
                &WorkerResultOwnerV1::Exact(owner),
                &LaneActivityReceiptV1::idle(120).unwrap(),
                &create_options("claim/restart", 120),
            )
            .unwrap();
            assert_eq!(
                replay.disposition,
                WorkerResumeCreateDispositionV1::Replayed
            );
            assert_eq!(replay.intent.unwrap().intent_id, first_id);
            let count: i64 = transaction
                .query_row(
                    &format!("SELECT COUNT(*) FROM {RESUME_INTENT_TABLE}"),
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(count, 1);
            transaction.commit().unwrap();
        }
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn expired_resume_lease_is_reclaimed_after_restart() {
        let mut conn = setup();
        let owner = owner("telegram", "account-a", "dm-a", "agent-a", "root-a");
        insert_result(&conn, &owner, "terminal/lease/1", 100);
        let transaction = conn.transaction().unwrap();
        create_or_coalesce_resume_intent_in_transaction(
            &transaction,
            &WorkerResultOwnerV1::Exact(owner.clone()),
            &LaneActivityReceiptV1::idle(110).unwrap(),
            &create_options("claim/lease", 110),
        )
        .unwrap();
        let first = claim_next_resume_intent_in_transaction(
            &transaction,
            &owner,
            &WorkerResumeLeaseOptionsV1 {
                lease_token: "intent-lease/one".into(),
                lease_owner: "watchdog/one".into(),
                now_ms: 120,
                lease_ms: 10,
            },
        )
        .unwrap()
        .unwrap();
        assert_eq!(first.state, WorkerResumeIntentStateV1::Leased);
        transaction.commit().unwrap();

        let transaction = conn.transaction().unwrap();
        let reclaimed = claim_next_resume_intent_in_transaction(
            &transaction,
            &owner,
            &WorkerResumeLeaseOptionsV1 {
                lease_token: "intent-lease/two".into(),
                lease_owner: "watchdog/two".into(),
                now_ms: 131,
                lease_ms: 10,
            },
        )
        .unwrap()
        .unwrap();
        assert_eq!(reclaimed.intent_id, first.intent_id);
        assert_eq!(reclaimed.lease.unwrap().lease_token, "intent-lease/two");
        transaction.commit().unwrap();
    }

    #[test]
    fn complete_lane_axes_and_virtual_roots_never_cross_claims() {
        let mut conn = setup();
        let owners = vec![
            owner("telegram", "account-a", "dm-a", "agent-a", "root-a"),
            owner("discord", "account-a", "dm-a", "agent-a", "root-a"),
            owner("telegram", "account-b", "dm-a", "agent-a", "root-a"),
            owner("telegram", "account-a", "dm-b", "agent-a", "root-a"),
            owner("telegram", "account-a", "dm-a", "agent-b", "root-a"),
            owner("telegram", "account-a", "dm-a", "agent-a", "root-b"),
        ];
        for (index, owner) in owners.iter().enumerate() {
            insert_result(
                &conn,
                owner,
                &format!("terminal/isolation/{index}"),
                100 + index as i64,
            );
        }
        let distinct = owners
            .iter()
            .map(|owner| owner.coordinator_key().unwrap())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(distinct.len(), owners.len());

        let transaction = conn.transaction().unwrap();
        let first = create_or_coalesce_resume_intent_in_transaction(
            &transaction,
            &WorkerResultOwnerV1::Exact(owners[0].clone()),
            &LaneActivityReceiptV1::idle(200).unwrap(),
            &create_options("claim/isolation", 200),
        )
        .unwrap()
        .intent
        .unwrap();
        transaction.commit().unwrap();
        assert_eq!(first.mailbox_ids.len(), 1);
        for owner in owners.iter().skip(1) {
            assert_eq!(
                coalesced_unread_for_exact_owner(&conn, owner, 10)
                    .unwrap()
                    .records
                    .len(),
                1
            );
        }
    }

    #[test]
    fn legacy_incomplete_owner_is_denied_before_mailbox_claim() {
        let mut conn = setup();
        let legacy = WorkerResultOwnerV1::LegacyIncomplete(LegacyIncompleteWorkerResultOwnerV0 {
            schema: LEGACY_WORKER_RESULT_OWNER_SCHEMA.to_string(),
            legacy_owner_ref: "legacy/job-1".to_string(),
            lane: None,
            virtual_session_id: None,
            parent_worker_job_id: None,
            parent_queue_id: None,
            source_queue_id: None,
            operation_plan_id: None,
            operation_plan_item_id: None,
            missing_identity_axes: vec![LegacyOwnerMissingAxisV1::AgentId],
        });
        let transaction = conn.transaction().unwrap();
        let error = create_or_coalesce_resume_intent_in_transaction(
            &transaction,
            &legacy,
            &LaneActivityReceiptV1::idle(100).unwrap(),
            &create_options("claim/legacy", 100),
        )
        .unwrap_err();
        assert!(matches!(error, WorkerResumeError::LegacyOwnerDenied));
        transaction.commit().unwrap();
    }

    fn setup() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        initialize_worker_result_mailbox_schema(&conn).unwrap();
        initialize_worker_resume_schema(&conn).unwrap();
        conn
    }

    fn owner(
        platform: &str,
        account_id: &str,
        channel_id: &str,
        agent_id: &str,
        root: &str,
    ) -> ExactWorkerResultOwnerV1 {
        ExactWorkerResultOwnerV1::new(
            FullLaneKeyV1::new(
                platform,
                account_id,
                channel_id,
                "user-a",
                agent_id,
                "codex",
                root,
                "concrete-a",
            )
            .unwrap(),
            format!("virtual/{root}"),
            Some("master/job-a".to_string()),
            Some("master/queue-a".to_string()),
            "child/queue-a",
            Some("plan-a".to_string()),
            Some("item-a".to_string()),
        )
        .unwrap()
    }

    fn insert_result(
        conn: &Connection,
        owner: &ExactWorkerResultOwnerV1,
        terminal_event_key: &str,
        terminal_at_ms: i64,
    ) {
        insert_terminal_result(
            conn,
            &WorkerResultMailboxInsertV1 {
                terminal_event_key: terminal_event_key.to_string(),
                owner: WorkerResultOwnerV1::Exact(owner.clone()),
                envelope: WorkerResultEnvelopeV1::new(
                    WorkerResultOutcomeV1::Succeeded,
                    "bounded terminal summary",
                    Vec::new(),
                )
                .unwrap(),
                source_worker_job_id: Some("child/job-a".to_string()),
                terminal_at_ms,
            },
        )
        .unwrap();
    }

    fn create_options(claim_token: &str, now_ms: i64) -> WorkerResumeCreateOptionsV1 {
        WorkerResumeCreateOptionsV1 {
            claim_token: claim_token.to_string(),
            claimant_id: "resume/watchdog".to_string(),
            now_ms,
            mailbox_lease_ms: 10_000,
            limit: 100,
        }
    }

    fn temp_db(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-worker-resume-{label}-{}-{nonce}.sqlite3",
            std::process::id()
        ))
    }
}
