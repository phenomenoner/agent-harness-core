use std::error::Error;
use std::fmt;

use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};

use crate::worker_result_mailbox::ExactWorkerResultOwnerV1;

pub const WORKER_COORDINATOR_WAIT_SCHEMA: &str = "agent-harness.worker-coordinator-wait.v1";
const WAIT_TABLE: &str = "worker_coordinator_waits_v1";

#[derive(Debug)]
pub enum WorkerCoordinationError {
    Sqlite(rusqlite::Error),
    Json(serde_json::Error),
    InvalidInput(String),
    InvalidStoredData(String),
    Conflict(String),
}

impl fmt::Display for WorkerCoordinationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sqlite(error) => write!(formatter, "coordinator wait SQLite error: {error}"),
            Self::Json(error) => write!(formatter, "coordinator wait JSON error: {error}"),
            Self::InvalidInput(reason) => write!(formatter, "invalid coordinator wait: {reason}"),
            Self::InvalidStoredData(reason) => {
                write!(formatter, "invalid stored coordinator wait: {reason}")
            }
            Self::Conflict(reason) => write!(formatter, "coordinator wait conflict: {reason}"),
        }
    }
}

impl Error for WorkerCoordinationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Sqlite(error) => Some(error),
            Self::Json(error) => Some(error),
            _ => None,
        }
    }
}

impl From<rusqlite::Error> for WorkerCoordinationError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sqlite(error)
    }
}

impl From<serde_json::Error> for WorkerCoordinationError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkerCoordinatorWaitStateV1 {
    WaitingForChildren,
    ResumeScheduled,
    Consumed,
    Quarantined,
}

impl WorkerCoordinatorWaitStateV1 {
    fn as_str(self) -> &'static str {
        match self {
            Self::WaitingForChildren => "waiting-for-children",
            Self::ResumeScheduled => "resume-scheduled",
            Self::Consumed => "consumed",
            Self::Quarantined => "quarantined",
        }
    }

    fn parse(value: &str) -> Result<Self, WorkerCoordinationError> {
        match value {
            "waiting-for-children" => Ok(Self::WaitingForChildren),
            "resume-scheduled" => Ok(Self::ResumeScheduled),
            "consumed" => Ok(Self::Consumed),
            "quarantined" => Ok(Self::Quarantined),
            other => Err(WorkerCoordinationError::InvalidStoredData(format!(
                "unknown wait state `{other}`"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerCoordinatorWaitV1 {
    pub schema: String,
    pub wait_id: String,
    pub coordinator_key: String,
    pub owner: ExactWorkerResultOwnerV1,
    pub parent_queue_id: String,
    pub child_group_id: String,
    pub expected_child_job_ids: Vec<String>,
    pub state: WorkerCoordinatorWaitStateV1,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume_intent_id: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerCoordinatorWaitCreateOptionsV1 {
    pub wait_id: String,
    pub owner: ExactWorkerResultOwnerV1,
    pub child_group_id: String,
    pub expected_child_job_ids: Vec<String>,
    pub now_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerCoordinatorWaitDispositionV1 {
    Created,
    Replayed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerCoordinatorWaitCreateOutcomeV1 {
    pub disposition: WorkerCoordinatorWaitDispositionV1,
    pub wait: WorkerCoordinatorWaitV1,
}

pub fn initialize_worker_coordination_schema(
    conn: &Connection,
) -> Result<(), WorkerCoordinationError> {
    conn.execute_batch(&format!(
        "
        CREATE TABLE IF NOT EXISTS {WAIT_TABLE} (
            wait_id TEXT PRIMARY KEY,
            schema TEXT NOT NULL CHECK (schema = '{WORKER_COORDINATOR_WAIT_SCHEMA}'),
            coordinator_key TEXT NOT NULL,
            owner_json TEXT NOT NULL,
            parent_queue_id TEXT NOT NULL,
            child_group_id TEXT NOT NULL,
            expected_child_job_ids_json TEXT NOT NULL,
            state TEXT NOT NULL CHECK (state IN ('waiting-for-children','resume-scheduled','consumed','quarantined')),
            resume_intent_id TEXT,
            created_at_ms INTEGER NOT NULL,
            updated_at_ms INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS worker_coordinator_waits_owner_idx
            ON {WAIT_TABLE}(coordinator_key, state, created_at_ms);
        CREATE UNIQUE INDEX IF NOT EXISTS worker_coordinator_waits_active_group_idx
            ON {WAIT_TABLE}(coordinator_key, child_group_id)
            WHERE state IN ('waiting-for-children','resume-scheduled');
        "
    ))?;
    Ok(())
}

pub fn persist_waiting_for_children_in_transaction(
    transaction: &Transaction<'_>,
    options: &WorkerCoordinatorWaitCreateOptionsV1,
) -> Result<WorkerCoordinatorWaitCreateOutcomeV1, WorkerCoordinationError> {
    initialize_worker_coordination_schema(transaction)?;
    let wait = canonical_wait(options)?;
    if let Some(existing) = load_worker_coordinator_wait(transaction, &wait.wait_id)? {
        if existing == wait {
            return Ok(WorkerCoordinatorWaitCreateOutcomeV1 {
                disposition: WorkerCoordinatorWaitDispositionV1::Replayed,
                wait: existing,
            });
        }
        return Err(WorkerCoordinationError::Conflict(format!(
            "waitId `{}` already exists with different immutable ownership or children",
            wait.wait_id
        )));
    }

    let owner_json = serde_json::to_string(&wait.owner)?;
    let child_ids_json = serde_json::to_string(&wait.expected_child_job_ids)?;
    match transaction.execute(
        &format!(
            "INSERT INTO {WAIT_TABLE} (
                wait_id, schema, coordinator_key, owner_json, parent_queue_id,
                child_group_id, expected_child_job_ids_json, state,
                resume_intent_id, created_at_ms, updated_at_ms
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,NULL,?9,?9)"
        ),
        params![
            wait.wait_id,
            wait.schema,
            wait.coordinator_key,
            owner_json,
            wait.parent_queue_id,
            wait.child_group_id,
            child_ids_json,
            wait.state.as_str(),
            wait.created_at_ms,
        ],
    ) {
        Ok(_) => Ok(WorkerCoordinatorWaitCreateOutcomeV1 {
            disposition: WorkerCoordinatorWaitDispositionV1::Created,
            wait,
        }),
        Err(rusqlite::Error::SqliteFailure(_, _)) => Err(WorkerCoordinationError::Conflict(
            "another active wait already owns this coordinator/group".to_string(),
        )),
        Err(error) => Err(error.into()),
    }
}

pub fn load_worker_coordinator_wait(
    conn: &Connection,
    wait_id: &str,
) -> Result<Option<WorkerCoordinatorWaitV1>, WorkerCoordinationError> {
    validate_identifier("waitId", wait_id)?;
    initialize_worker_coordination_schema(conn)?;
    conn.query_row(
        &format!(
            "SELECT wait_id, schema, coordinator_key, owner_json, parent_queue_id,
                    child_group_id, expected_child_job_ids_json, state,
                    resume_intent_id, created_at_ms, updated_at_ms
             FROM {WAIT_TABLE} WHERE wait_id=?1"
        ),
        [wait_id],
        decode_wait_row,
    )
    .optional()
    .map_err(Into::into)
}

pub fn mark_worker_coordinator_resume_scheduled_in_transaction(
    transaction: &Transaction<'_>,
    wait_id: &str,
    intent_id: &str,
    now_ms: i64,
) -> Result<WorkerCoordinatorWaitV1, WorkerCoordinationError> {
    transition_wait_in_transaction(
        transaction,
        wait_id,
        intent_id,
        WorkerCoordinatorWaitStateV1::WaitingForChildren,
        WorkerCoordinatorWaitStateV1::ResumeScheduled,
        now_ms,
    )
}

pub fn mark_worker_coordinator_wait_consumed_in_transaction(
    transaction: &Transaction<'_>,
    wait_id: &str,
    intent_id: &str,
    now_ms: i64,
) -> Result<WorkerCoordinatorWaitV1, WorkerCoordinationError> {
    transition_wait_in_transaction(
        transaction,
        wait_id,
        intent_id,
        WorkerCoordinatorWaitStateV1::ResumeScheduled,
        WorkerCoordinatorWaitStateV1::Consumed,
        now_ms,
    )
}

fn transition_wait_in_transaction(
    transaction: &Transaction<'_>,
    wait_id: &str,
    intent_id: &str,
    from: WorkerCoordinatorWaitStateV1,
    to: WorkerCoordinatorWaitStateV1,
    now_ms: i64,
) -> Result<WorkerCoordinatorWaitV1, WorkerCoordinationError> {
    validate_identifier("waitId", wait_id)?;
    validate_identifier("intentId", intent_id)?;
    if now_ms < 0 {
        return Err(WorkerCoordinationError::InvalidInput(
            "nowMs must be non-negative".to_string(),
        ));
    }
    let existing = load_worker_coordinator_wait(transaction, wait_id)?.ok_or_else(|| {
        WorkerCoordinationError::InvalidInput(format!("waitId `{wait_id}` does not exist"))
    })?;
    if existing.state == to && existing.resume_intent_id.as_deref() == Some(intent_id) {
        return Ok(existing);
    }
    if existing.state != from
        || existing
            .resume_intent_id
            .as_deref()
            .is_some_and(|bound| bound != intent_id)
    {
        return Err(WorkerCoordinationError::Conflict(format!(
            "waitId `{wait_id}` cannot transition from {} to {} for intent `{intent_id}`",
            existing.state.as_str(),
            to.as_str()
        )));
    }
    let changed = transaction.execute(
        &format!(
            "UPDATE {WAIT_TABLE} SET state=?1, resume_intent_id=?2, updated_at_ms=?3
             WHERE wait_id=?4 AND state=?5
               AND (resume_intent_id IS NULL OR resume_intent_id=?2)"
        ),
        params![to.as_str(), intent_id, now_ms, wait_id, from.as_str()],
    )?;
    if changed != 1 {
        return Err(WorkerCoordinationError::Conflict(format!(
            "waitId `{wait_id}` changed concurrently"
        )));
    }
    load_worker_coordinator_wait(transaction, wait_id)?.ok_or_else(|| {
        WorkerCoordinationError::InvalidStoredData(format!(
            "waitId `{wait_id}` disappeared after transition"
        ))
    })
}

pub fn list_worker_coordinator_waits(
    conn: &Connection,
) -> Result<Vec<WorkerCoordinatorWaitV1>, WorkerCoordinationError> {
    initialize_worker_coordination_schema(conn)?;
    let mut statement = conn.prepare(&format!(
        "SELECT wait_id, schema, coordinator_key, owner_json, parent_queue_id,
                child_group_id, expected_child_job_ids_json, state,
                resume_intent_id, created_at_ms, updated_at_ms
         FROM {WAIT_TABLE} ORDER BY created_at_ms, wait_id"
    ))?;
    let rows = statement.query_map([], decode_wait_row)?;
    rows.map(|row| row.map_err(Into::into)).collect()
}

fn canonical_wait(
    options: &WorkerCoordinatorWaitCreateOptionsV1,
) -> Result<WorkerCoordinatorWaitV1, WorkerCoordinationError> {
    validate_identifier("waitId", &options.wait_id)?;
    validate_identifier("childGroupId", &options.child_group_id)?;
    if options.now_ms < 0 {
        return Err(WorkerCoordinationError::InvalidInput(
            "nowMs must be non-negative".to_string(),
        ));
    }
    options
        .owner
        .validate()
        .map_err(|error| WorkerCoordinationError::InvalidInput(error.to_string()))?;
    let parent_queue_id = options.owner.parent_queue_id.clone().ok_or_else(|| {
        WorkerCoordinationError::InvalidInput(
            "exact coordinator owner requires parentQueueId".to_string(),
        )
    })?;
    let coordinator_key = options
        .owner
        .coordinator_key()
        .map_err(|error| WorkerCoordinationError::InvalidInput(error.to_string()))?;
    let mut child_ids = options.expected_child_job_ids.clone();
    if child_ids.is_empty() {
        return Err(WorkerCoordinationError::InvalidInput(
            "expectedChildJobIds must not be empty".to_string(),
        ));
    }
    if child_ids.len() > 256 {
        return Err(WorkerCoordinationError::InvalidInput(
            "expectedChildJobIds exceeds 256 entries".to_string(),
        ));
    }
    for child_id in &child_ids {
        validate_identifier("expectedChildJobId", child_id)?;
    }
    child_ids.sort();
    let before = child_ids.len();
    child_ids.dedup();
    if child_ids.len() != before {
        return Err(WorkerCoordinationError::InvalidInput(
            "expectedChildJobIds contains duplicates".to_string(),
        ));
    }
    Ok(WorkerCoordinatorWaitV1 {
        schema: WORKER_COORDINATOR_WAIT_SCHEMA.to_string(),
        wait_id: options.wait_id.clone(),
        coordinator_key,
        owner: options.owner.clone(),
        parent_queue_id,
        child_group_id: options.child_group_id.clone(),
        expected_child_job_ids: child_ids,
        state: WorkerCoordinatorWaitStateV1::WaitingForChildren,
        resume_intent_id: None,
        created_at_ms: options.now_ms,
        updated_at_ms: options.now_ms,
    })
}

fn decode_wait_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<WorkerCoordinatorWaitV1> {
    let owner_json: String = row.get(3)?;
    let child_ids_json: String = row.get(6)?;
    let state_text: String = row.get(7)?;
    let owner = serde_json::from_str(&owner_json).map_err(json_from_sql)?;
    let expected_child_job_ids = serde_json::from_str(&child_ids_json).map_err(json_from_sql)?;
    let state = WorkerCoordinatorWaitStateV1::parse(&state_text).map_err(coordination_from_sql)?;
    let wait = WorkerCoordinatorWaitV1 {
        wait_id: row.get(0)?,
        schema: row.get(1)?,
        coordinator_key: row.get(2)?,
        owner,
        parent_queue_id: row.get(4)?,
        child_group_id: row.get(5)?,
        expected_child_job_ids,
        state,
        resume_intent_id: row.get(8)?,
        created_at_ms: row.get(9)?,
        updated_at_ms: row.get(10)?,
    };
    validate_stored_wait(&wait).map_err(coordination_from_sql)?;
    Ok(wait)
}

fn validate_stored_wait(wait: &WorkerCoordinatorWaitV1) -> Result<(), WorkerCoordinationError> {
    if wait.schema != WORKER_COORDINATOR_WAIT_SCHEMA {
        return Err(WorkerCoordinationError::InvalidStoredData(format!(
            "unsupported schema `{}`",
            wait.schema
        )));
    }
    let expected = canonical_wait(&WorkerCoordinatorWaitCreateOptionsV1 {
        wait_id: wait.wait_id.clone(),
        owner: wait.owner.clone(),
        child_group_id: wait.child_group_id.clone(),
        expected_child_job_ids: wait.expected_child_job_ids.clone(),
        now_ms: wait.created_at_ms,
    })?;
    if wait.coordinator_key != expected.coordinator_key
        || wait.parent_queue_id != expected.parent_queue_id
        || wait.expected_child_job_ids != expected.expected_child_job_ids
        || wait.updated_at_ms < wait.created_at_ms
    {
        return Err(WorkerCoordinationError::InvalidStoredData(
            "stored wait does not match its exact owner or canonical children".to_string(),
        ));
    }
    Ok(())
}

fn validate_identifier(field: &str, value: &str) -> Result<(), WorkerCoordinationError> {
    if value.is_empty()
        || value != value.trim()
        || value.len() > 512
        || value.chars().any(char::is_control)
    {
        return Err(WorkerCoordinationError::InvalidInput(format!(
            "{field} is missing, non-canonical, or too long"
        )));
    }
    Ok(())
}

fn json_from_sql(error: serde_json::Error) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
}

fn coordination_from_sql(error: WorkerCoordinationError) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lane::FullLaneKeyV1;
    use crate::worker_result_mailbox::ExactWorkerResultOwnerV1;
    use rusqlite::Connection;

    #[test]
    fn exact_waiting_for_children_is_idempotent_and_conflicts_fail_closed() {
        let mut conn = Connection::open_in_memory().unwrap();
        initialize_worker_coordination_schema(&conn).unwrap();
        let owner = exact_owner("parent-queue", "child-source");
        let options = WorkerCoordinatorWaitCreateOptionsV1 {
            wait_id: "wait-parent-queue".to_string(),
            owner,
            child_group_id: "group-a".to_string(),
            expected_child_job_ids: vec!["child-b".to_string(), "child-a".to_string()],
            now_ms: 1_000,
        };

        let tx = conn.transaction().unwrap();
        let first = persist_waiting_for_children_in_transaction(&tx, &options).unwrap();
        tx.commit().unwrap();
        assert_eq!(
            first.disposition,
            WorkerCoordinatorWaitDispositionV1::Created
        );
        assert_eq!(
            first.wait.state,
            WorkerCoordinatorWaitStateV1::WaitingForChildren
        );
        assert_eq!(
            first.wait.expected_child_job_ids,
            vec!["child-a", "child-b"]
        );

        let tx = conn.transaction().unwrap();
        let replay = persist_waiting_for_children_in_transaction(&tx, &options).unwrap();
        tx.commit().unwrap();
        assert_eq!(
            replay.disposition,
            WorkerCoordinatorWaitDispositionV1::Replayed
        );
        assert_eq!(replay.wait, first.wait);

        let tx = conn.transaction().unwrap();
        let scheduled = mark_worker_coordinator_resume_scheduled_in_transaction(
            &tx,
            &options.wait_id,
            "intent-a",
            1_100,
        )
        .unwrap();
        tx.commit().unwrap();
        assert_eq!(
            scheduled.state,
            WorkerCoordinatorWaitStateV1::ResumeScheduled
        );
        assert_eq!(scheduled.resume_intent_id.as_deref(), Some("intent-a"));

        let tx = conn.transaction().unwrap();
        let consumed = mark_worker_coordinator_wait_consumed_in_transaction(
            &tx,
            &options.wait_id,
            "intent-a",
            1_200,
        )
        .unwrap();
        tx.commit().unwrap();
        assert_eq!(consumed.state, WorkerCoordinatorWaitStateV1::Consumed);

        let mut conflicting = options;
        conflicting.expected_child_job_ids = vec!["child-c".to_string()];
        let tx = conn.transaction().unwrap();
        let error = persist_waiting_for_children_in_transaction(&tx, &conflicting).unwrap_err();
        assert!(matches!(error, WorkerCoordinationError::Conflict(_)));
    }

    #[test]
    fn coordinator_wait_owner_isolated_by_every_full_lane_axis() {
        let mut conn = Connection::open_in_memory().unwrap();
        initialize_worker_coordination_schema(&conn).unwrap();
        let first = exact_owner("parent-queue", "child-source-a");
        let second = ExactWorkerResultOwnerV1::new(
            FullLaneKeyV1::new(
                "discord",
                "account",
                "channel",
                "user",
                "main",
                "interactive",
                "root-session",
                "concrete-session",
            )
            .unwrap(),
            "virtual-session",
            None,
            Some("parent-queue".to_string()),
            "child-source-b",
            None,
            None,
        )
        .unwrap();

        for (wait_id, owner) in [("wait-a", first), ("wait-b", second)] {
            let tx = conn.transaction().unwrap();
            persist_waiting_for_children_in_transaction(
                &tx,
                &WorkerCoordinatorWaitCreateOptionsV1 {
                    wait_id: wait_id.to_string(),
                    owner,
                    child_group_id: "group-a".to_string(),
                    expected_child_job_ids: vec![format!("child-{wait_id}")],
                    now_ms: 1_000,
                },
            )
            .unwrap();
            tx.commit().unwrap();
        }

        let waits = list_worker_coordinator_waits(&conn).unwrap();
        assert_eq!(waits.len(), 2);
        assert_ne!(waits[0].coordinator_key, waits[1].coordinator_key);
    }

    fn exact_owner(parent_queue_id: &str, source_queue_id: &str) -> ExactWorkerResultOwnerV1 {
        ExactWorkerResultOwnerV1::new(
            FullLaneKeyV1::new(
                "telegram",
                "account",
                "channel",
                "user",
                "main",
                "interactive",
                "root-session",
                "concrete-session",
            )
            .unwrap(),
            "virtual-session",
            None,
            Some(parent_queue_id.to_string()),
            source_queue_id,
            None,
            None,
        )
        .unwrap()
    }
}
