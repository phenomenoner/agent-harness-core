use std::error::Error;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::context_rollover::{derive_virtual_session_id, root_working_session_key};
use crate::worker_result_mailbox::ExactWorkerResultOwnerV1;

pub const COORDINATOR_RESUME_METADATA_SCHEMA: &str = "agent-harness.coordinator-resume.v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CoordinatorResumeMetadataV1 {
    pub schema: String,
    pub intent_id: String,
    pub wait_id: String,
    pub continuation_queue_id: String,
    pub owner: ExactWorkerResultOwnerV1,
}

/// Immutable queued lane fields that a coordinator continuation must match.
/// The metadata owner is authoritative; queue payload values are untrusted at
/// both the worker-dispatch and durable-runtime boundaries.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CoordinatorResumeQueuedLaneV1<'a> {
    pub platform: &'a str,
    pub account_id: &'a str,
    pub channel_id: &'a str,
    pub user_id: &'a str,
    pub agent_id: &'a str,
    pub runtime_class: &'a str,
    pub session_key: &'a str,
}

impl CoordinatorResumeMetadataV1 {
    pub fn new(
        intent_id: impl Into<String>,
        wait_id: impl Into<String>,
        continuation_queue_id: impl Into<String>,
        owner: ExactWorkerResultOwnerV1,
    ) -> Result<Self, CoordinatorResumeMetadataError> {
        let metadata = Self {
            schema: COORDINATOR_RESUME_METADATA_SCHEMA.to_string(),
            intent_id: intent_id.into(),
            wait_id: wait_id.into(),
            continuation_queue_id: continuation_queue_id.into(),
            owner,
        };
        metadata.validate()?;
        Ok(metadata)
    }

    pub fn validate(&self) -> Result<(), CoordinatorResumeMetadataError> {
        if self.schema != COORDINATOR_RESUME_METADATA_SCHEMA {
            return Err(CoordinatorResumeMetadataError::InvalidSchema(
                self.schema.clone(),
            ));
        }
        validate_identifier("intentId", &self.intent_id)?;
        validate_identifier("waitId", &self.wait_id)?;
        validate_identifier("continuationQueueId", &self.continuation_queue_id)?;
        self.owner
            .validate()
            .map_err(|error| CoordinatorResumeMetadataError::InvalidOwner(error.to_string()))?;
        if self.owner.parent_queue_id.is_none() {
            return Err(CoordinatorResumeMetadataError::InvalidOwner(
                "parentQueueId is required".to_string(),
            ));
        }
        Ok(())
    }

    /// Rejects a continuation whose mutable queue representation differs from
    /// its exact parent owner.  All eight full-lane axes, plus the derived
    /// virtual session, are checked before the continuation can run.
    pub(crate) fn validate_queued_lane(
        &self,
        queued: CoordinatorResumeQueuedLaneV1<'_>,
    ) -> Result<(), CoordinatorResumeMetadataError> {
        self.validate()?;
        let root_session = root_working_session_key(queued.session_key);
        let observed = [
            ("platform", self.owner.lane.platform(), queued.platform),
            ("accountId", self.owner.lane.account_id(), queued.account_id),
            ("channelId", self.owner.lane.channel_id(), queued.channel_id),
            ("userId", self.owner.lane.user_id(), queued.user_id),
            ("agentId", self.owner.lane.agent_id(), queued.agent_id),
            (
                "runtimeClass",
                self.owner.lane.runtime_class(),
                queued.runtime_class,
            ),
            (
                "rootVirtualSession",
                self.owner.lane.root_virtual_session(),
                root_session.as_str(),
            ),
            (
                "concreteSession",
                self.owner.lane.concrete_session(),
                queued.session_key,
            ),
        ];
        if let Some((axis, _, _)) = observed
            .into_iter()
            .find(|(_, expected, actual)| expected != actual)
        {
            return Err(CoordinatorResumeMetadataError::QueuedLaneMismatch(axis));
        }

        let expected_virtual_session_id = derive_virtual_session_id(
            queued.platform,
            queued.channel_id,
            queued.user_id,
            queued.agent_id,
            &root_session,
        );
        if self.owner.virtual_session_id != expected_virtual_session_id {
            return Err(CoordinatorResumeMetadataError::QueuedLaneMismatch(
                "virtualSessionId",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoordinatorResumeMetadataError {
    InvalidSchema(String),
    InvalidIdentifier(&'static str),
    InvalidOwner(String),
    QueuedLaneMismatch(&'static str),
}

impl fmt::Display for CoordinatorResumeMetadataError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSchema(schema) => {
                write!(formatter, "unsupported coordinator resume schema {schema}")
            }
            Self::InvalidIdentifier(field) => {
                write!(formatter, "coordinator resume {field} is invalid")
            }
            Self::InvalidOwner(reason) => {
                write!(formatter, "coordinator resume owner is invalid: {reason}")
            }
            Self::QueuedLaneMismatch(axis) => {
                write!(
                    formatter,
                    "coordinator resume owner does not match queued {axis}"
                )
            }
        }
    }
}

impl Error for CoordinatorResumeMetadataError {}

fn validate_identifier(
    field: &'static str,
    value: &str,
) -> Result<(), CoordinatorResumeMetadataError> {
    if value.is_empty()
        || value != value.trim()
        || value.len() > 512
        || value.chars().any(char::is_control)
    {
        return Err(CoordinatorResumeMetadataError::InvalidIdentifier(field));
    }
    Ok(())
}
