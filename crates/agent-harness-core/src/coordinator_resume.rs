use std::error::Error;
use std::fmt;

use serde::{Deserialize, Serialize};

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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoordinatorResumeMetadataError {
    InvalidSchema(String),
    InvalidIdentifier(&'static str),
    InvalidOwner(String),
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
