//! Durable, owner-scoped terminal results for worker/subagent execution.
//!
//! The mailbox is deliberately provider-neutral. It stores a bounded, redacted
//! terminal summary plus opaque artifact references; raw model output, prompts,
//! provider events, credentials, and absolute filesystem paths do not belong in
//! this table.
//!
//! `insert_terminal_result_in_transaction` is the integration seam for worker
//! completion: callers can update the worker terminal state and append its
//! mailbox result in the same SQLite transaction. Only an exact owner can be
//! claimed for automatic resume. Migrated owners with incomplete identity are
//! retained for audit through `find_by_terminal_event_key`, but are intentionally
//! excluded from every automatic-resume query.

use std::error::Error;
use std::fmt;

use ring::digest;
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde::{Deserialize, Serialize};

use crate::lane::FullLaneKeyV1;

pub const WORKER_RESULT_MAILBOX_SCHEMA: &str = "agent-harness.worker-result-mailbox.v1";
pub const WORKER_RESULT_OWNER_SCHEMA: &str = "agent-harness.worker-result-owner.v1";
pub const LEGACY_WORKER_RESULT_OWNER_SCHEMA: &str =
    "agent-harness.worker-result-owner.legacy-incomplete.v0";
pub const WORKER_RESULT_ENVELOPE_SCHEMA: &str = "agent-harness.worker-result-envelope.v1";
pub const MAX_RESULT_SUMMARY_BYTES: usize = 4 * 1024;
pub const MAX_RESULT_ARTIFACTS: usize = 16;
pub const MAX_MAILBOX_REFERENCE_BYTES: usize = 1024;
pub const MAX_MAILBOX_BATCH_ITEMS: usize = 100;
pub const MAX_MAILBOX_CLAIM_LEASE_MS: i64 = 24 * 60 * 60 * 1000;

const MAILBOX_TABLE: &str = "worker_result_mailbox_v1";
const OWNER_HASH_DOMAIN: &[u8] = b"agent-harness/worker-result-owner/v1";
const EVENT_HASH_DOMAIN: &[u8] = b"agent-harness/worker-result-terminal-event/v1";

#[derive(Debug)]
pub enum WorkerResultMailboxError {
    Sqlite(rusqlite::Error),
    Json(serde_json::Error),
    InvalidInput(String),
    InvalidStoredData(String),
    IdempotencyConflict { terminal_event_key: String },
    ClaimTokenConflict { claim_token: String },
}

impl fmt::Display for WorkerResultMailboxError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sqlite(error) => write!(formatter, "mailbox SQLite error: {error}"),
            Self::Json(error) => write!(formatter, "mailbox JSON error: {error}"),
            Self::InvalidInput(reason) => write!(formatter, "invalid mailbox input: {reason}"),
            Self::InvalidStoredData(reason) => {
                write!(formatter, "invalid stored mailbox record: {reason}")
            }
            Self::IdempotencyConflict { terminal_event_key } => write!(
                formatter,
                "terminal event key `{terminal_event_key}` already binds different mailbox content"
            ),
            Self::ClaimTokenConflict { claim_token } => write!(
                formatter,
                "claim token `{claim_token}` is already bound to another owner or claimant"
            ),
        }
    }
}

impl Error for WorkerResultMailboxError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Sqlite(error) => Some(error),
            Self::Json(error) => Some(error),
            _ => None,
        }
    }
}

impl From<rusqlite::Error> for WorkerResultMailboxError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sqlite(error)
    }
}

impl From<serde_json::Error> for WorkerResultMailboxError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExactWorkerResultOwnerV1 {
    pub schema: String,
    pub lane: FullLaneKeyV1,
    pub virtual_session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_worker_job_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_queue_id: Option<String>,
    pub source_queue_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_plan_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_plan_item_id: Option<String>,
}

impl ExactWorkerResultOwnerV1 {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        lane: FullLaneKeyV1,
        virtual_session_id: impl Into<String>,
        parent_worker_job_id: Option<String>,
        parent_queue_id: Option<String>,
        source_queue_id: impl Into<String>,
        operation_plan_id: Option<String>,
        operation_plan_item_id: Option<String>,
    ) -> Result<Self, WorkerResultMailboxError> {
        let owner = Self {
            schema: WORKER_RESULT_OWNER_SCHEMA.to_string(),
            lane,
            virtual_session_id: virtual_session_id.into(),
            parent_worker_job_id,
            parent_queue_id,
            source_queue_id: source_queue_id.into(),
            operation_plan_id,
            operation_plan_item_id,
        };
        owner.validate()?;
        Ok(owner)
    }

    pub fn validate(&self) -> Result<(), WorkerResultMailboxError> {
        if self.schema != WORKER_RESULT_OWNER_SCHEMA {
            return Err(invalid_input(format!(
                "unsupported exact owner schema `{}`",
                self.schema
            )));
        }
        self.lane
            .validate()
            .map_err(|error| invalid_input(format!("invalid full lane: {error}")))?;
        if self.lane.has_legacy_unknowns() {
            return Err(invalid_input(
                "exact mailbox owner contains legacy-unknown lane axes",
            ));
        }
        validate_identifier("virtualSessionId", &self.virtual_session_id, true)?;
        validate_optional_identifier("parentWorkerJobId", self.parent_worker_job_id.as_deref())?;
        validate_optional_identifier("parentQueueId", self.parent_queue_id.as_deref())?;
        validate_identifier("sourceQueueId", &self.source_queue_id, true)?;
        validate_optional_identifier("operationPlanId", self.operation_plan_id.as_deref())?;
        validate_optional_identifier(
            "operationPlanItemId",
            self.operation_plan_item_id.as_deref(),
        )?;
        if self.operation_plan_item_id.is_some() && self.operation_plan_id.is_none() {
            return Err(invalid_input(
                "operationPlanItemId requires operationPlanId",
            ));
        }
        Ok(())
    }

    pub fn owner_key(&self) -> Result<String, WorkerResultMailboxError> {
        self.validate()?;
        let lane_hash = self
            .lane
            .identity_hash()
            .map_err(|error| invalid_input(format!("invalid full lane: {error}")))?;
        let mut components = Vec::with_capacity(8);
        components.push(("schema", self.schema.as_str()));
        components.push(("laneHash", lane_hash.as_str()));
        components.push(("virtualSessionId", self.virtual_session_id.as_str()));
        components.push((
            "parentWorkerJobId",
            self.parent_worker_job_id.as_deref().unwrap_or(""),
        ));
        components.push((
            "parentQueueId",
            self.parent_queue_id.as_deref().unwrap_or(""),
        ));
        components.push(("sourceQueueId", self.source_queue_id.as_str()));
        components.push((
            "operationPlanId",
            self.operation_plan_id.as_deref().unwrap_or(""),
        ));
        components.push((
            "operationPlanItemId",
            self.operation_plan_item_id.as_deref().unwrap_or(""),
        ));
        Ok(hash_named_components(OWNER_HASH_DOMAIN, &components))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum LegacyOwnerMissingAxisV1 {
    Platform,
    AccountId,
    ChannelId,
    UserId,
    AgentId,
    RuntimeClass,
    RootVirtualSession,
    ConcreteSession,
    VirtualSessionId,
    SourceQueueId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LegacyIncompleteWorkerResultOwnerV0 {
    pub schema: String,
    pub legacy_owner_ref: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lane: Option<FullLaneKeyV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub virtual_session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_worker_job_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_queue_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_queue_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_plan_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_plan_item_id: Option<String>,
    pub missing_identity_axes: Vec<LegacyOwnerMissingAxisV1>,
}

impl LegacyIncompleteWorkerResultOwnerV0 {
    pub fn validate(&self) -> Result<(), WorkerResultMailboxError> {
        if self.schema != LEGACY_WORKER_RESULT_OWNER_SCHEMA {
            return Err(invalid_input(format!(
                "unsupported legacy owner schema `{}`",
                self.schema
            )));
        }
        validate_identifier("legacyOwnerRef", &self.legacy_owner_ref, true)?;
        if let Some(lane) = &self.lane {
            lane.validate()
                .map_err(|error| invalid_input(format!("invalid legacy full lane: {error}")))?;
        }
        validate_optional_identifier("virtualSessionId", self.virtual_session_id.as_deref())?;
        validate_optional_identifier("parentWorkerJobId", self.parent_worker_job_id.as_deref())?;
        validate_optional_identifier("parentQueueId", self.parent_queue_id.as_deref())?;
        validate_optional_identifier("sourceQueueId", self.source_queue_id.as_deref())?;
        validate_optional_identifier("operationPlanId", self.operation_plan_id.as_deref())?;
        validate_optional_identifier(
            "operationPlanItemId",
            self.operation_plan_item_id.as_deref(),
        )?;
        if self.missing_identity_axes.is_empty() {
            return Err(invalid_input(
                "legacy incomplete owner must identify at least one missing identity axis",
            ));
        }
        let mut axes = self.missing_identity_axes.clone();
        axes.sort_by_key(|axis| *axis as u8);
        axes.dedup();
        if axes.len() != self.missing_identity_axes.len() {
            return Err(invalid_input(
                "legacy incomplete owner repeats a missing identity axis",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "ownerKind", content = "owner", rename_all = "kebab-case")]
pub enum WorkerResultOwnerV1 {
    Exact(ExactWorkerResultOwnerV1),
    LegacyIncomplete(LegacyIncompleteWorkerResultOwnerV0),
}

impl WorkerResultOwnerV1 {
    pub fn validate(&self) -> Result<(), WorkerResultMailboxError> {
        match self {
            Self::Exact(owner) => owner.validate(),
            Self::LegacyIncomplete(owner) => owner.validate(),
        }
    }

    pub fn is_auto_resumable(&self) -> bool {
        matches!(self, Self::Exact(_))
    }

    fn owner_kind(&self) -> &'static str {
        match self {
            Self::Exact(_) => "exact",
            Self::LegacyIncomplete(_) => "legacy-incomplete",
        }
    }

    fn owner_key(&self) -> Result<Option<String>, WorkerResultMailboxError> {
        match self {
            Self::Exact(owner) => owner.owner_key().map(Some),
            Self::LegacyIncomplete(_) => Ok(None),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkerResultOutcomeV1 {
    Succeeded,
    Failed,
    Canceled,
    Expired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkerResultContentPolicyV1 {
    RedactedSummaryAndOpaquePointersOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkerResultArtifactKindV1 {
    TerminalReceipt,
    Transcript,
    Trajectory,
    Diff,
    TestReport,
    Audit,
    Artifact,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerResultArtifactPointerV1 {
    pub kind: WorkerResultArtifactKindV1,
    pub reference: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
}

impl WorkerResultArtifactPointerV1 {
    pub fn validate(&self) -> Result<(), WorkerResultMailboxError> {
        validate_opaque_artifact_reference(&self.reference)?;
        if let Some(sha256) = self.sha256.as_deref() {
            validate_sha256("artifact sha256", sha256)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerResultEnvelopeV1 {
    pub schema: String,
    pub outcome: WorkerResultOutcomeV1,
    pub content_policy: WorkerResultContentPolicyV1,
    pub redacted_summary: String,
    #[serde(default)]
    pub artifacts: Vec<WorkerResultArtifactPointerV1>,
}

impl WorkerResultEnvelopeV1 {
    pub fn new(
        outcome: WorkerResultOutcomeV1,
        redacted_summary: impl Into<String>,
        artifacts: Vec<WorkerResultArtifactPointerV1>,
    ) -> Result<Self, WorkerResultMailboxError> {
        let envelope = Self {
            schema: WORKER_RESULT_ENVELOPE_SCHEMA.to_string(),
            outcome,
            content_policy: WorkerResultContentPolicyV1::RedactedSummaryAndOpaquePointersOnly,
            redacted_summary: redacted_summary.into(),
            artifacts,
        };
        envelope.validate()?;
        Ok(envelope)
    }

    pub fn validate(&self) -> Result<(), WorkerResultMailboxError> {
        if self.schema != WORKER_RESULT_ENVELOPE_SCHEMA {
            return Err(invalid_input(format!(
                "unsupported result envelope schema `{}`",
                self.schema
            )));
        }
        if self.redacted_summary.is_empty() {
            return Err(invalid_input("redactedSummary is empty"));
        }
        if self.redacted_summary.len() > MAX_RESULT_SUMMARY_BYTES {
            return Err(invalid_input(format!(
                "redactedSummary is {} bytes; maximum is {MAX_RESULT_SUMMARY_BYTES}",
                self.redacted_summary.len()
            )));
        }
        if self.redacted_summary.chars().any(char::is_control) {
            return Err(invalid_input(
                "redactedSummary contains a control character; detailed output belongs in an opaque artifact",
            ));
        }
        if self.artifacts.len() > MAX_RESULT_ARTIFACTS {
            return Err(invalid_input(format!(
                "result contains {} artifact pointers; maximum is {MAX_RESULT_ARTIFACTS}",
                self.artifacts.len()
            )));
        }
        for artifact in &self.artifacts {
            artifact.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerResultMailboxInsertV1 {
    pub terminal_event_key: String,
    pub owner: WorkerResultOwnerV1,
    pub envelope: WorkerResultEnvelopeV1,
    pub source_worker_job_id: Option<String>,
    pub terminal_at_ms: i64,
}

impl WorkerResultMailboxInsertV1 {
    pub fn validate(&self) -> Result<(), WorkerResultMailboxError> {
        validate_identifier("terminalEventKey", &self.terminal_event_key, true)?;
        self.owner.validate()?;
        self.envelope.validate()?;
        validate_optional_identifier("sourceWorkerJobId", self.source_worker_job_id.as_deref())?;
        validate_timestamp("terminalAtMs", self.terminal_at_ms)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkerResultMailboxStateV1 {
    Unread,
    Claimed,
    Acknowledged,
}

impl WorkerResultMailboxStateV1 {
    fn as_db(self) -> &'static str {
        match self {
            Self::Unread => "unread",
            Self::Claimed => "claimed",
            Self::Acknowledged => "acknowledged",
        }
    }

    fn from_db(value: &str) -> Result<Self, WorkerResultMailboxError> {
        match value {
            "unread" => Ok(Self::Unread),
            "claimed" => Ok(Self::Claimed),
            "acknowledged" => Ok(Self::Acknowledged),
            other => Err(WorkerResultMailboxError::InvalidStoredData(format!(
                "unsupported mailbox state `{other}`"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerResultMailboxClaimV1 {
    pub claim_token: String,
    pub claimant_id: String,
    pub lease_expires_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerResultMailboxRecordV1 {
    pub schema: String,
    pub mailbox_id: i64,
    pub terminal_event_key: String,
    pub owner: WorkerResultOwnerV1,
    pub auto_resumable: bool,
    pub envelope: WorkerResultEnvelopeV1,
    pub state: WorkerResultMailboxStateV1,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claim: Option<WorkerResultMailboxClaimV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_worker_job_id: Option<String>,
    pub terminal_at_ms: i64,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acknowledged_at_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerResultMailboxInsertOutcomeV1 {
    pub mailbox_id: i64,
    pub inserted: bool,
    pub auto_resumable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerResultMailboxBatchV1 {
    pub schema: &'static str,
    pub owner_key: String,
    pub records: Vec<WorkerResultMailboxRecordV1>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerResultMailboxClaimOptionsV1 {
    pub claim_token: String,
    pub claimant_id: String,
    pub now_ms: i64,
    pub lease_ms: i64,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerResultMailboxClaimBatchV1 {
    pub schema: &'static str,
    pub owner_key: String,
    pub claim_token: String,
    pub claimant_id: String,
    pub lease_expires_at_ms: i64,
    pub replayed_existing_claim: bool,
    pub records: Vec<WorkerResultMailboxRecordV1>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerResultMailboxTransitionV1 {
    pub matched_records: usize,
    pub transitioned_records: usize,
}

/// Adds the mailbox tables and indexes without changing existing worker rows.
/// It is safe to call repeatedly and can also be called through a transaction
/// (`initialize_worker_result_mailbox_schema(&transaction)`).
pub fn initialize_worker_result_mailbox_schema(
    conn: &Connection,
) -> Result<(), WorkerResultMailboxError> {
    conn.execute_batch(&format!(
        "
        CREATE TABLE IF NOT EXISTS {MAILBOX_TABLE} (
            mailbox_id INTEGER PRIMARY KEY AUTOINCREMENT,
            schema TEXT NOT NULL,
            terminal_event_key TEXT NOT NULL UNIQUE,
            owner_kind TEXT NOT NULL,
            owner_key TEXT,
            owner_json TEXT NOT NULL,
            auto_resumable INTEGER NOT NULL,
            envelope_json TEXT NOT NULL,
            event_digest TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT 'unread',
            claim_token TEXT,
            claim_owner TEXT,
            claim_expires_at_ms INTEGER,
            source_worker_job_id TEXT,
            terminal_at_ms INTEGER NOT NULL,
            created_at_ms INTEGER NOT NULL,
            updated_at_ms INTEGER NOT NULL,
            acknowledged_at_ms INTEGER,
            CHECK (schema = '{WORKER_RESULT_MAILBOX_SCHEMA}'),
            CHECK (owner_kind IN ('exact', 'legacy-incomplete')),
            CHECK (auto_resumable IN (0, 1)),
            CHECK (
                (auto_resumable = 1 AND owner_kind = 'exact' AND owner_key IS NOT NULL)
                OR
                (auto_resumable = 0 AND owner_kind = 'legacy-incomplete' AND owner_key IS NULL)
            ),
            CHECK (state IN ('unread', 'claimed', 'acknowledged')),
            CHECK (
                (state = 'unread' AND claim_token IS NULL AND claim_owner IS NULL AND claim_expires_at_ms IS NULL)
                OR
                (state = 'claimed' AND claim_token IS NOT NULL AND claim_owner IS NOT NULL AND claim_expires_at_ms IS NOT NULL)
                OR
                (state = 'acknowledged' AND acknowledged_at_ms IS NOT NULL)
            )
        );
        CREATE INDEX IF NOT EXISTS worker_result_mailbox_owner_unread_idx
            ON {MAILBOX_TABLE}(owner_key, auto_resumable, state, terminal_at_ms, mailbox_id);
        CREATE INDEX IF NOT EXISTS worker_result_mailbox_claim_idx
            ON {MAILBOX_TABLE}(claim_token, state);
        CREATE TABLE IF NOT EXISTS worker_result_mailbox_meta_v1 (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        INSERT OR IGNORE INTO worker_result_mailbox_meta_v1(key, value)
            VALUES ('schema', '{WORKER_RESULT_MAILBOX_SCHEMA}');
        "
    ))?;
    Ok(())
}

/// Inserts a terminal event as one atomic SQLite statement. For coupling with a
/// worker status update, prefer `insert_terminal_result_in_transaction`.
pub fn insert_terminal_result(
    conn: &Connection,
    input: &WorkerResultMailboxInsertV1,
) -> Result<WorkerResultMailboxInsertOutcomeV1, WorkerResultMailboxError> {
    insert_terminal_result_on_connection(conn, input)
}

/// Transaction-taking integration seam. A caller can write the worker terminal
/// status and its mailbox event in the same transaction, then commit once.
pub fn insert_terminal_result_in_transaction(
    transaction: &Transaction<'_>,
    input: &WorkerResultMailboxInsertV1,
) -> Result<WorkerResultMailboxInsertOutcomeV1, WorkerResultMailboxError> {
    insert_terminal_result_on_connection(transaction, input)
}

fn insert_terminal_result_on_connection(
    conn: &Connection,
    input: &WorkerResultMailboxInsertV1,
) -> Result<WorkerResultMailboxInsertOutcomeV1, WorkerResultMailboxError> {
    input.validate()?;
    let owner_json = serde_json::to_string(&input.owner)?;
    let envelope_json = serde_json::to_string(&input.envelope)?;
    let owner_key = input.owner.owner_key()?;
    let auto_resumable = input.owner.is_auto_resumable();
    let event_digest = terminal_event_digest(
        &input.terminal_event_key,
        &owner_json,
        &envelope_json,
        input.source_worker_job_id.as_deref(),
        input.terminal_at_ms,
    );

    let inserted = conn.execute(
        &format!(
            "INSERT OR IGNORE INTO {MAILBOX_TABLE} (
                schema, terminal_event_key, owner_kind, owner_key, owner_json,
                auto_resumable, envelope_json, event_digest, state,
                source_worker_job_id, terminal_at_ms, created_at_ms, updated_at_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'unread', ?9, ?10, ?10, ?10)"
        ),
        params![
            WORKER_RESULT_MAILBOX_SCHEMA,
            input.terminal_event_key,
            input.owner.owner_kind(),
            owner_key,
            owner_json,
            if auto_resumable { 1_i64 } else { 0_i64 },
            envelope_json,
            event_digest,
            input.source_worker_job_id,
            input.terminal_at_ms,
        ],
    )?;

    let existing: (i64, i64, String) = conn.query_row(
        &format!(
            "SELECT mailbox_id, auto_resumable, event_digest FROM {MAILBOX_TABLE}
             WHERE terminal_event_key = ?1"
        ),
        params![input.terminal_event_key],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    if existing.2 != event_digest {
        return Err(WorkerResultMailboxError::IdempotencyConflict {
            terminal_event_key: input.terminal_event_key.clone(),
        });
    }
    Ok(WorkerResultMailboxInsertOutcomeV1 {
        mailbox_id: existing.0,
        inserted: inserted == 1,
        auto_resumable: existing.1 == 1,
    })
}

pub fn find_by_terminal_event_key(
    conn: &Connection,
    terminal_event_key: &str,
) -> Result<Option<WorkerResultMailboxRecordV1>, WorkerResultMailboxError> {
    validate_identifier("terminalEventKey", terminal_event_key, true)?;
    let raw = conn
        .query_row(
            &format!(
                "SELECT mailbox_id, terminal_event_key, owner_kind, owner_key, owner_json,
                        auto_resumable, envelope_json, state, claim_token, claim_owner,
                        claim_expires_at_ms, source_worker_job_id, terminal_at_ms,
                        created_at_ms, updated_at_ms, acknowledged_at_ms
                 FROM {MAILBOX_TABLE} WHERE terminal_event_key = ?1"
            ),
            params![terminal_event_key],
            RawMailboxRecord::from_row,
        )
        .optional()?;
    raw.map(RawMailboxRecord::decode).transpose()
}

/// Returns unread records only for the complete, byte-exact owner contract.
/// Legacy/incomplete records have no owner key and cannot enter this path.
pub fn coalesced_unread_for_exact_owner(
    conn: &Connection,
    owner: &ExactWorkerResultOwnerV1,
    limit: usize,
) -> Result<WorkerResultMailboxBatchV1, WorkerResultMailboxError> {
    validate_batch_limit(limit)?;
    let owner_key = owner.owner_key()?;
    let records = records_for_owner_state(conn, &owner_key, "unread", limit)?;
    Ok(WorkerResultMailboxBatchV1 {
        schema: WORKER_RESULT_MAILBOX_SCHEMA,
        owner_key,
        records,
    })
}

pub fn claim_unread_for_exact_owner(
    conn: &mut Connection,
    owner: &ExactWorkerResultOwnerV1,
    options: &WorkerResultMailboxClaimOptionsV1,
) -> Result<WorkerResultMailboxClaimBatchV1, WorkerResultMailboxError> {
    let transaction = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let batch = claim_unread_for_exact_owner_in_transaction(&transaction, owner, options)?;
    transaction.commit()?;
    Ok(batch)
}

/// Claims a coalesced unread batch inside the caller's transaction. Expired
/// claims for this exact owner are released first, making process restart safe.
pub fn claim_unread_for_exact_owner_in_transaction(
    transaction: &Transaction<'_>,
    owner: &ExactWorkerResultOwnerV1,
    options: &WorkerResultMailboxClaimOptionsV1,
) -> Result<WorkerResultMailboxClaimBatchV1, WorkerResultMailboxError> {
    validate_claim_options(options)?;
    let owner_key = owner.owner_key()?;
    let lease_expires_at_ms = options.now_ms.saturating_add(options.lease_ms);

    transaction.execute(
        &format!(
            "UPDATE {MAILBOX_TABLE}
             SET state = 'unread', claim_token = NULL, claim_owner = NULL,
                 claim_expires_at_ms = NULL, updated_at_ms = ?1
             WHERE owner_key = ?2 AND auto_resumable = 1 AND state = 'claimed'
               AND claim_expires_at_ms <= ?1"
        ),
        params![options.now_ms, owner_key],
    )?;

    if let Some((bound_owner, bound_claimant, bound_state, bound_expiry)) = transaction
        .query_row(
            &format!(
                "SELECT owner_key, claim_owner, state, claim_expires_at_ms FROM {MAILBOX_TABLE}
                 WHERE claim_token = ?1 ORDER BY mailbox_id ASC LIMIT 1"
            ),
            params![options.claim_token],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                ))
            },
        )
        .optional()?
    {
        if bound_owner.as_deref() != Some(owner_key.as_str())
            || bound_claimant.as_deref() != Some(options.claimant_id.as_str())
        {
            return Err(WorkerResultMailboxError::ClaimTokenConflict {
                claim_token: options.claim_token.clone(),
            });
        }
        let records = if bound_state == "claimed" {
            records_for_claim(transaction, &owner_key, &options.claim_token)?
        } else {
            Vec::new()
        };
        return Ok(WorkerResultMailboxClaimBatchV1 {
            schema: WORKER_RESULT_MAILBOX_SCHEMA,
            owner_key,
            claim_token: options.claim_token.clone(),
            claimant_id: options.claimant_id.clone(),
            lease_expires_at_ms: bound_expiry.unwrap_or(options.now_ms),
            replayed_existing_claim: true,
            records,
        });
    }

    let ids = unread_ids_for_owner(transaction, &owner_key, options.limit)?;
    for mailbox_id in ids {
        transaction.execute(
            &format!(
                "UPDATE {MAILBOX_TABLE}
                 SET state = 'claimed', claim_token = ?1, claim_owner = ?2,
                     claim_expires_at_ms = ?3, updated_at_ms = ?4
                 WHERE mailbox_id = ?5 AND owner_key = ?6 AND auto_resumable = 1
                   AND state = 'unread'"
            ),
            params![
                options.claim_token,
                options.claimant_id,
                lease_expires_at_ms,
                options.now_ms,
                mailbox_id,
                owner_key,
            ],
        )?;
    }
    let records = records_for_claim(transaction, &owner_key, &options.claim_token)?;
    Ok(WorkerResultMailboxClaimBatchV1 {
        schema: WORKER_RESULT_MAILBOX_SCHEMA,
        owner_key,
        claim_token: options.claim_token.clone(),
        claimant_id: options.claimant_id.clone(),
        lease_expires_at_ms,
        replayed_existing_claim: false,
        records,
    })
}

pub fn acknowledge_claim(
    conn: &mut Connection,
    owner: &ExactWorkerResultOwnerV1,
    claim_token: &str,
    now_ms: i64,
) -> Result<WorkerResultMailboxTransitionV1, WorkerResultMailboxError> {
    let transaction = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let outcome = acknowledge_claim_in_transaction(&transaction, owner, claim_token, now_ms)?;
    transaction.commit()?;
    Ok(outcome)
}

pub fn acknowledge_claim_in_transaction(
    transaction: &Transaction<'_>,
    owner: &ExactWorkerResultOwnerV1,
    claim_token: &str,
    now_ms: i64,
) -> Result<WorkerResultMailboxTransitionV1, WorkerResultMailboxError> {
    validate_identifier("claimToken", claim_token, true)?;
    validate_timestamp("nowMs", now_ms)?;
    let owner_key = owner.owner_key()?;
    let already_acknowledged = count_records_for_claim_state(
        transaction,
        &owner_key,
        claim_token,
        WorkerResultMailboxStateV1::Acknowledged,
    )?;
    let transitioned = transaction.execute(
        &format!(
            "UPDATE {MAILBOX_TABLE}
             SET state = 'acknowledged', claim_expires_at_ms = NULL,
                 acknowledged_at_ms = ?1, updated_at_ms = ?1
             WHERE owner_key = ?2 AND auto_resumable = 1
               AND claim_token = ?3 AND state = 'claimed'"
        ),
        params![now_ms, owner_key, claim_token],
    )?;
    Ok(WorkerResultMailboxTransitionV1 {
        matched_records: already_acknowledged.saturating_add(transitioned),
        transitioned_records: transitioned,
    })
}

pub fn release_claim(
    conn: &mut Connection,
    owner: &ExactWorkerResultOwnerV1,
    claim_token: &str,
    now_ms: i64,
) -> Result<WorkerResultMailboxTransitionV1, WorkerResultMailboxError> {
    let transaction = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let outcome = release_claim_in_transaction(&transaction, owner, claim_token, now_ms)?;
    transaction.commit()?;
    Ok(outcome)
}

pub fn release_claim_in_transaction(
    transaction: &Transaction<'_>,
    owner: &ExactWorkerResultOwnerV1,
    claim_token: &str,
    now_ms: i64,
) -> Result<WorkerResultMailboxTransitionV1, WorkerResultMailboxError> {
    validate_identifier("claimToken", claim_token, true)?;
    validate_timestamp("nowMs", now_ms)?;
    let owner_key = owner.owner_key()?;
    let matched = count_records_for_claim_state(
        transaction,
        &owner_key,
        claim_token,
        WorkerResultMailboxStateV1::Claimed,
    )?;
    let transitioned = transaction.execute(
        &format!(
            "UPDATE {MAILBOX_TABLE}
             SET state = 'unread', claim_token = NULL, claim_owner = NULL,
                 claim_expires_at_ms = NULL, updated_at_ms = ?1
             WHERE owner_key = ?2 AND auto_resumable = 1
               AND claim_token = ?3 AND state = 'claimed'"
        ),
        params![now_ms, owner_key, claim_token],
    )?;
    Ok(WorkerResultMailboxTransitionV1 {
        matched_records: matched,
        transitioned_records: transitioned,
    })
}

fn records_for_owner_state(
    conn: &Connection,
    owner_key: &str,
    state: &str,
    limit: usize,
) -> Result<Vec<WorkerResultMailboxRecordV1>, WorkerResultMailboxError> {
    let mut statement = conn.prepare(&format!(
        "SELECT mailbox_id, terminal_event_key, owner_kind, owner_key, owner_json,
                auto_resumable, envelope_json, state, claim_token, claim_owner,
                claim_expires_at_ms, source_worker_job_id, terminal_at_ms,
                created_at_ms, updated_at_ms, acknowledged_at_ms
         FROM {MAILBOX_TABLE}
         WHERE owner_key = ?1 AND auto_resumable = 1 AND state = ?2
         ORDER BY terminal_at_ms ASC, mailbox_id ASC LIMIT ?3"
    ))?;
    let mut rows = statement.query(params![owner_key, state, limit as i64])?;
    decode_rows(&mut rows)
}

fn unread_ids_for_owner(
    conn: &Connection,
    owner_key: &str,
    limit: usize,
) -> Result<Vec<i64>, WorkerResultMailboxError> {
    let mut statement = conn.prepare(&format!(
        "SELECT mailbox_id FROM {MAILBOX_TABLE}
         WHERE owner_key = ?1 AND auto_resumable = 1 AND state = 'unread'
         ORDER BY terminal_at_ms ASC, mailbox_id ASC LIMIT ?2"
    ))?;
    let ids = statement
        .query_map(params![owner_key, limit as i64], |row| row.get(0))?
        .collect::<Result<Vec<i64>, _>>()?;
    Ok(ids)
}

fn records_for_claim(
    conn: &Connection,
    owner_key: &str,
    claim_token: &str,
) -> Result<Vec<WorkerResultMailboxRecordV1>, WorkerResultMailboxError> {
    let mut statement = conn.prepare(&format!(
        "SELECT mailbox_id, terminal_event_key, owner_kind, owner_key, owner_json,
                auto_resumable, envelope_json, state, claim_token, claim_owner,
                claim_expires_at_ms, source_worker_job_id, terminal_at_ms,
                created_at_ms, updated_at_ms, acknowledged_at_ms
         FROM {MAILBOX_TABLE}
         WHERE owner_key = ?1 AND auto_resumable = 1 AND state = 'claimed'
           AND claim_token = ?2
         ORDER BY terminal_at_ms ASC, mailbox_id ASC"
    ))?;
    let mut rows = statement.query(params![owner_key, claim_token])?;
    decode_rows(&mut rows)
}

fn count_records_for_claim_state(
    conn: &Connection,
    owner_key: &str,
    claim_token: &str,
    state: WorkerResultMailboxStateV1,
) -> Result<usize, WorkerResultMailboxError> {
    let count: i64 = conn.query_row(
        &format!(
            "SELECT COUNT(*) FROM {MAILBOX_TABLE}
             WHERE owner_key = ?1 AND auto_resumable = 1
               AND claim_token = ?2 AND state = ?3"
        ),
        params![owner_key, claim_token, state.as_db()],
        |row| row.get(0),
    )?;
    Ok(usize::try_from(count).unwrap_or(usize::MAX))
}

fn decode_rows(
    rows: &mut rusqlite::Rows<'_>,
) -> Result<Vec<WorkerResultMailboxRecordV1>, WorkerResultMailboxError> {
    let mut records = Vec::new();
    while let Some(row) = rows.next()? {
        records.push(RawMailboxRecord::from_row(row)?.decode()?);
    }
    Ok(records)
}

struct RawMailboxRecord {
    mailbox_id: i64,
    terminal_event_key: String,
    owner_kind: String,
    owner_key: Option<String>,
    owner_json: String,
    auto_resumable: i64,
    envelope_json: String,
    state: String,
    claim_token: Option<String>,
    claim_owner: Option<String>,
    claim_expires_at_ms: Option<i64>,
    source_worker_job_id: Option<String>,
    terminal_at_ms: i64,
    created_at_ms: i64,
    updated_at_ms: i64,
    acknowledged_at_ms: Option<i64>,
}

impl RawMailboxRecord {
    fn from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            mailbox_id: row.get(0)?,
            terminal_event_key: row.get(1)?,
            owner_kind: row.get(2)?,
            owner_key: row.get(3)?,
            owner_json: row.get(4)?,
            auto_resumable: row.get(5)?,
            envelope_json: row.get(6)?,
            state: row.get(7)?,
            claim_token: row.get(8)?,
            claim_owner: row.get(9)?,
            claim_expires_at_ms: row.get(10)?,
            source_worker_job_id: row.get(11)?,
            terminal_at_ms: row.get(12)?,
            created_at_ms: row.get(13)?,
            updated_at_ms: row.get(14)?,
            acknowledged_at_ms: row.get(15)?,
        })
    }

    fn decode(self) -> Result<WorkerResultMailboxRecordV1, WorkerResultMailboxError> {
        let owner: WorkerResultOwnerV1 = serde_json::from_str(&self.owner_json)?;
        owner.validate().map_err(|error| {
            WorkerResultMailboxError::InvalidStoredData(format!("owner validation failed: {error}"))
        })?;
        if owner.owner_kind() != self.owner_kind {
            return Err(WorkerResultMailboxError::InvalidStoredData(
                "ownerKind column does not match owner JSON".to_string(),
            ));
        }
        let expected_owner_key = owner.owner_key().map_err(|error| {
            WorkerResultMailboxError::InvalidStoredData(format!(
                "owner key derivation failed: {error}"
            ))
        })?;
        if expected_owner_key != self.owner_key {
            return Err(WorkerResultMailboxError::InvalidStoredData(
                "ownerKey column does not match exact owner JSON".to_string(),
            ));
        }
        let auto_resumable = match self.auto_resumable {
            0 => false,
            1 => true,
            other => {
                return Err(WorkerResultMailboxError::InvalidStoredData(format!(
                    "invalid autoResumable value `{other}`"
                )));
            }
        };
        if auto_resumable != owner.is_auto_resumable() {
            return Err(WorkerResultMailboxError::InvalidStoredData(
                "autoResumable column does not match owner completeness".to_string(),
            ));
        }
        let envelope: WorkerResultEnvelopeV1 = serde_json::from_str(&self.envelope_json)?;
        envelope.validate().map_err(|error| {
            WorkerResultMailboxError::InvalidStoredData(format!(
                "envelope validation failed: {error}"
            ))
        })?;
        let state = WorkerResultMailboxStateV1::from_db(&self.state)?;
        let claim = if state == WorkerResultMailboxStateV1::Claimed {
            Some(WorkerResultMailboxClaimV1 {
                claim_token: self.claim_token.ok_or_else(|| {
                    WorkerResultMailboxError::InvalidStoredData(
                        "claimed record is missing claimToken".to_string(),
                    )
                })?,
                claimant_id: self.claim_owner.ok_or_else(|| {
                    WorkerResultMailboxError::InvalidStoredData(
                        "claimed record is missing claimOwner".to_string(),
                    )
                })?,
                lease_expires_at_ms: self.claim_expires_at_ms.ok_or_else(|| {
                    WorkerResultMailboxError::InvalidStoredData(
                        "claimed record is missing claim expiry".to_string(),
                    )
                })?,
            })
        } else {
            None
        };
        Ok(WorkerResultMailboxRecordV1 {
            schema: WORKER_RESULT_MAILBOX_SCHEMA.to_string(),
            mailbox_id: self.mailbox_id,
            terminal_event_key: self.terminal_event_key,
            owner,
            auto_resumable,
            envelope,
            state,
            claim,
            source_worker_job_id: self.source_worker_job_id,
            terminal_at_ms: self.terminal_at_ms,
            created_at_ms: self.created_at_ms,
            updated_at_ms: self.updated_at_ms,
            acknowledged_at_ms: self.acknowledged_at_ms,
        })
    }
}

fn terminal_event_digest(
    terminal_event_key: &str,
    owner_json: &str,
    envelope_json: &str,
    source_worker_job_id: Option<&str>,
    terminal_at_ms: i64,
) -> String {
    let terminal_at = terminal_at_ms.to_string();
    hash_named_components(
        EVENT_HASH_DOMAIN,
        &[
            ("terminalEventKey", terminal_event_key),
            ("ownerJson", owner_json),
            ("envelopeJson", envelope_json),
            (
                "sourceWorkerJobId",
                source_worker_job_id.unwrap_or_default(),
            ),
            ("terminalAtMs", terminal_at.as_str()),
        ],
    )
}

fn hash_named_components(domain: &[u8], components: &[(&str, &str)]) -> String {
    let mut context = digest::Context::new(&digest::SHA256);
    append_digest_component(&mut context, b"domain", domain);
    for (name, value) in components {
        append_digest_component(&mut context, name.as_bytes(), value.as_bytes());
    }
    lower_hex(context.finish().as_ref())
}

fn append_digest_component(context: &mut digest::Context, name: &[u8], value: &[u8]) {
    context.update(&(name.len() as u64).to_be_bytes());
    context.update(name);
    context.update(&(value.len() as u64).to_be_bytes());
    context.update(value);
}

fn lower_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn validate_claim_options(
    options: &WorkerResultMailboxClaimOptionsV1,
) -> Result<(), WorkerResultMailboxError> {
    validate_identifier("claimToken", &options.claim_token, true)?;
    validate_identifier("claimantId", &options.claimant_id, true)?;
    validate_timestamp("nowMs", options.now_ms)?;
    if !(1..=MAX_MAILBOX_CLAIM_LEASE_MS).contains(&options.lease_ms) {
        return Err(invalid_input(format!(
            "leaseMs must be in 1..={MAX_MAILBOX_CLAIM_LEASE_MS}"
        )));
    }
    validate_batch_limit(options.limit)
}

fn validate_batch_limit(limit: usize) -> Result<(), WorkerResultMailboxError> {
    if !(1..=MAX_MAILBOX_BATCH_ITEMS).contains(&limit) {
        return Err(invalid_input(format!(
            "mailbox batch limit must be in 1..={MAX_MAILBOX_BATCH_ITEMS}"
        )));
    }
    Ok(())
}

fn validate_timestamp(field: &str, value: i64) -> Result<(), WorkerResultMailboxError> {
    if value < 0 {
        return Err(invalid_input(format!("{field} cannot be negative")));
    }
    Ok(())
}

fn validate_optional_identifier(
    field: &str,
    value: Option<&str>,
) -> Result<(), WorkerResultMailboxError> {
    if let Some(value) = value {
        validate_identifier(field, value, true)?;
    }
    Ok(())
}

fn validate_identifier(
    field: &str,
    value: &str,
    required: bool,
) -> Result<(), WorkerResultMailboxError> {
    if required && value.is_empty() {
        return Err(invalid_input(format!("{field} is empty")));
    }
    if value != value.trim() {
        return Err(invalid_input(format!("{field} is not canonical")));
    }
    if value.len() > MAX_MAILBOX_REFERENCE_BYTES {
        return Err(invalid_input(format!(
            "{field} is {} bytes; maximum is {MAX_MAILBOX_REFERENCE_BYTES}",
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

fn validate_opaque_artifact_reference(value: &str) -> Result<(), WorkerResultMailboxError> {
    validate_identifier("artifact reference", value, true)?;
    let allowed_scheme = ["artifact:", "receipt:", "harness:", "sha256:"]
        .iter()
        .any(|prefix| value.starts_with(prefix));
    if !allowed_scheme {
        return Err(invalid_input(
            "artifact reference must use artifact:, receipt:, harness:, or sha256: opaque scheme",
        ));
    }
    if value.contains("..")
        || value.contains('\\')
        || value.contains('@')
        || value.contains('?')
        || value.contains('#')
    {
        return Err(invalid_input(
            "artifact reference contains path traversal, credentials, or query material",
        ));
    }
    Ok(())
}

fn validate_sha256(field: &str, value: &str) -> Result<(), WorkerResultMailboxError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid_input(format!(
            "{field} must be a lowercase hexadecimal SHA-256 digest"
        )));
    }
    Ok(())
}

fn invalid_input(reason: impl Into<String>) -> WorkerResultMailboxError {
    WorkerResultMailboxError::InvalidInput(reason.into())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::lane::{FullLaneKeyV1, LegacyLaneKeyV0};

    fn lane(agent_id: &str) -> FullLaneKeyV1 {
        FullLaneKeyV1::new(
            "discord",
            "account-1",
            "channel-1",
            "user-1",
            agent_id,
            "codex",
            "virtual-root-1",
            "concrete-1",
        )
        .unwrap()
    }

    fn owner(agent_id: &str) -> ExactWorkerResultOwnerV1 {
        ExactWorkerResultOwnerV1::new(
            lane(agent_id),
            "virtual-session-1",
            Some("parent-job-1".to_string()),
            Some("parent-queue-1".to_string()),
            format!("source-queue-{agent_id}"),
            Some("plan-1".to_string()),
            Some("item-1".to_string()),
        )
        .unwrap()
    }

    fn envelope(summary: &str) -> WorkerResultEnvelopeV1 {
        WorkerResultEnvelopeV1::new(
            WorkerResultOutcomeV1::Succeeded,
            summary,
            vec![WorkerResultArtifactPointerV1 {
                kind: WorkerResultArtifactKindV1::TerminalReceipt,
                reference: "receipt:terminal/receipt-1".to_string(),
                sha256: Some("a".repeat(64)),
            }],
        )
        .unwrap()
    }

    fn insert(event: &str, owner: WorkerResultOwnerV1, at: i64) -> WorkerResultMailboxInsertV1 {
        WorkerResultMailboxInsertV1 {
            terminal_event_key: event.to_string(),
            owner,
            envelope: envelope("redacted child result"),
            source_worker_job_id: Some("worker-job-1".to_string()),
            terminal_at_ms: at,
        }
    }

    #[test]
    fn terminal_insert_is_idempotent_and_conflicts_on_rebinding() {
        let conn = Connection::open_in_memory().unwrap();
        initialize_worker_result_mailbox_schema(&conn).unwrap();
        let input = insert(
            "worker-terminal/job-1/1",
            WorkerResultOwnerV1::Exact(owner("main")),
            100,
        );

        let first = insert_terminal_result(&conn, &input).unwrap();
        let replay = insert_terminal_result(&conn, &input).unwrap();
        assert!(first.inserted);
        assert!(!replay.inserted);
        assert_eq!(first.mailbox_id, replay.mailbox_id);

        let mut rebound = input.clone();
        rebound.envelope = envelope("different redacted result");
        assert!(matches!(
            insert_terminal_result(&conn, &rebound),
            Err(WorkerResultMailboxError::IdempotencyConflict { .. })
        ));
    }

    #[test]
    fn transaction_seam_rolls_back_with_the_parent_worker_update() {
        let mut conn = Connection::open_in_memory().unwrap();
        initialize_worker_result_mailbox_schema(&conn).unwrap();
        let input = insert(
            "worker-terminal/transactional/1",
            WorkerResultOwnerV1::Exact(owner("main")),
            100,
        );
        let transaction = conn.transaction().unwrap();
        let inserted = insert_terminal_result_in_transaction(&transaction, &input).unwrap();
        assert!(inserted.inserted);
        transaction.rollback().unwrap();

        assert!(
            find_by_terminal_event_key(&conn, "worker-terminal/transactional/1")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn unread_lookup_and_claim_are_isolated_by_every_exact_owner_axis() {
        let mut conn = Connection::open_in_memory().unwrap();
        initialize_worker_result_mailbox_schema(&conn).unwrap();
        let main_owner = owner("main");
        let research_owner = owner("research");
        insert_terminal_result(
            &conn,
            &insert(
                "worker-terminal/main/1",
                WorkerResultOwnerV1::Exact(main_owner.clone()),
                100,
            ),
        )
        .unwrap();
        insert_terminal_result(
            &conn,
            &insert(
                "worker-terminal/research/1",
                WorkerResultOwnerV1::Exact(research_owner.clone()),
                101,
            ),
        )
        .unwrap();

        let main_unread = coalesced_unread_for_exact_owner(&conn, &main_owner, 10).unwrap();
        assert_eq!(main_unread.records.len(), 1);
        assert_eq!(
            main_unread.records[0].terminal_event_key,
            "worker-terminal/main/1"
        );

        let claimed = claim_unread_for_exact_owner(
            &mut conn,
            &main_owner,
            &WorkerResultMailboxClaimOptionsV1 {
                claim_token: "claim-main-1".to_string(),
                claimant_id: "master-main".to_string(),
                now_ms: 200,
                lease_ms: 100,
                limit: 10,
            },
        )
        .unwrap();
        assert_eq!(claimed.records.len(), 1);
        assert_eq!(
            coalesced_unread_for_exact_owner(&conn, &research_owner, 10)
                .unwrap()
                .records
                .len(),
            1
        );
    }

    #[test]
    fn claim_release_ack_and_restart_preserve_delivery_state() {
        let root = temp_root("restart");
        fs::create_dir_all(&root).unwrap();
        let db = root.join("mailbox.sqlite");
        let exact_owner = owner("main");
        {
            let conn = Connection::open(&db).unwrap();
            initialize_worker_result_mailbox_schema(&conn).unwrap();
            for sequence in 1..=3 {
                insert_terminal_result(
                    &conn,
                    &insert(
                        &format!("worker-terminal/job-{sequence}/1"),
                        WorkerResultOwnerV1::Exact(exact_owner.clone()),
                        100 + sequence,
                    ),
                )
                .unwrap();
            }
        }

        {
            let mut conn = Connection::open(&db).unwrap();
            let claimed = claim_unread_for_exact_owner(
                &mut conn,
                &exact_owner,
                &WorkerResultMailboxClaimOptionsV1 {
                    claim_token: "claim-a".to_string(),
                    claimant_id: "master-main".to_string(),
                    now_ms: 200,
                    lease_ms: 100,
                    limit: 2,
                },
            )
            .unwrap();
            assert_eq!(claimed.records.len(), 2);
        }

        {
            let mut conn = Connection::open(&db).unwrap();
            let replay = claim_unread_for_exact_owner(
                &mut conn,
                &exact_owner,
                &WorkerResultMailboxClaimOptionsV1 {
                    claim_token: "claim-a".to_string(),
                    claimant_id: "master-main".to_string(),
                    now_ms: 250,
                    lease_ms: 100,
                    limit: 2,
                },
            )
            .unwrap();
            assert!(replay.replayed_existing_claim);
            assert_eq!(replay.records.len(), 2);
            assert_eq!(
                coalesced_unread_for_exact_owner(&conn, &exact_owner, 10)
                    .unwrap()
                    .records
                    .len(),
                1
            );

            let released = release_claim(&mut conn, &exact_owner, "claim-a", 260).unwrap();
            assert_eq!(released.transitioned_records, 2);
            assert_eq!(
                coalesced_unread_for_exact_owner(&conn, &exact_owner, 10)
                    .unwrap()
                    .records
                    .len(),
                3
            );

            let claimed = claim_unread_for_exact_owner(
                &mut conn,
                &exact_owner,
                &WorkerResultMailboxClaimOptionsV1 {
                    claim_token: "claim-b".to_string(),
                    claimant_id: "master-main".to_string(),
                    now_ms: 270,
                    lease_ms: 100,
                    limit: 3,
                },
            )
            .unwrap();
            assert_eq!(claimed.records.len(), 3);
            let acknowledged = acknowledge_claim(&mut conn, &exact_owner, "claim-b", 280).unwrap();
            assert_eq!(acknowledged.transitioned_records, 3);
            let replayed_ack = acknowledge_claim(&mut conn, &exact_owner, "claim-b", 281).unwrap();
            assert_eq!(replayed_ack.transitioned_records, 0);
            assert_eq!(replayed_ack.matched_records, 3);
        }

        {
            let conn = Connection::open(&db).unwrap();
            assert!(
                coalesced_unread_for_exact_owner(&conn, &exact_owner, 10)
                    .unwrap()
                    .records
                    .is_empty()
            );
            let record = find_by_terminal_event_key(&conn, "worker-terminal/job-1/1")
                .unwrap()
                .unwrap();
            assert_eq!(record.state, WorkerResultMailboxStateV1::Acknowledged);
        }
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn bounded_redacted_payload_and_opaque_pointer_contract_is_enforced() {
        assert!(
            WorkerResultEnvelopeV1::new(
                WorkerResultOutcomeV1::Succeeded,
                "x".repeat(MAX_RESULT_SUMMARY_BYTES + 1),
                Vec::new(),
            )
            .is_err()
        );

        let too_many = vec![
            WorkerResultArtifactPointerV1 {
                kind: WorkerResultArtifactKindV1::Artifact,
                reference: "artifact:item".to_string(),
                sha256: None,
            };
            MAX_RESULT_ARTIFACTS + 1
        ];
        assert!(
            WorkerResultEnvelopeV1::new(WorkerResultOutcomeV1::Succeeded, "redacted", too_many,)
                .is_err()
        );

        assert!(
            WorkerResultArtifactPointerV1 {
                kind: WorkerResultArtifactKindV1::Audit,
                reference: "C:\\private\\raw-output.json".to_string(),
                sha256: None,
            }
            .validate()
            .is_err()
        );
        assert!(
            WorkerResultArtifactPointerV1 {
                kind: WorkerResultArtifactKindV1::Audit,
                reference: "https://user:secret@example.invalid/output".to_string(),
                sha256: None,
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn legacy_incomplete_owner_is_auditable_but_never_auto_resumable() {
        let conn = Connection::open_in_memory().unwrap();
        initialize_worker_result_mailbox_schema(&conn).unwrap();
        let legacy_lane = FullLaneKeyV1::from_legacy(LegacyLaneKeyV0 {
            platform: Some("discord".to_string()),
            agent_id: Some("main".to_string()),
            ..LegacyLaneKeyV0::default()
        })
        .unwrap();
        let legacy_owner = LegacyIncompleteWorkerResultOwnerV0 {
            schema: LEGACY_WORKER_RESULT_OWNER_SCHEMA.to_string(),
            legacy_owner_ref: "legacy-record-1".to_string(),
            lane: Some(legacy_lane),
            virtual_session_id: None,
            parent_worker_job_id: Some("parent-job-1".to_string()),
            parent_queue_id: None,
            source_queue_id: None,
            operation_plan_id: None,
            operation_plan_item_id: None,
            missing_identity_axes: vec![
                LegacyOwnerMissingAxisV1::AccountId,
                LegacyOwnerMissingAxisV1::VirtualSessionId,
                LegacyOwnerMissingAxisV1::SourceQueueId,
            ],
        };
        let outcome = insert_terminal_result(
            &conn,
            &insert(
                "worker-terminal/legacy/1",
                WorkerResultOwnerV1::LegacyIncomplete(legacy_owner),
                100,
            ),
        )
        .unwrap();
        assert!(!outcome.auto_resumable);

        let audited = find_by_terminal_event_key(&conn, "worker-terminal/legacy/1")
            .unwrap()
            .unwrap();
        assert!(!audited.auto_resumable);
        assert!(matches!(
            audited.owner,
            WorkerResultOwnerV1::LegacyIncomplete(_)
        ));
        assert!(
            coalesced_unread_for_exact_owner(&conn, &owner("main"), 10)
                .unwrap()
                .records
                .is_empty()
        );
    }

    fn temp_root(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-worker-result-mailbox-{label}-{}-{nonce}",
            std::process::id()
        ))
    }
}
