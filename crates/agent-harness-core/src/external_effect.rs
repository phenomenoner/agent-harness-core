use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use ring::digest;
use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::channel_action::{
    ChannelApprovalDecisionV1, ChannelInboundActionV1, public_approval_action_id,
    validate_public_approval_action_id,
};
use crate::{current_log_time_ms, write_json_atomic};

pub const EXTERNAL_EFFECT_INTENT_SCHEMA: &str = "agent-harness.external-effect-intent.v1";
pub const EXTERNAL_EFFECT_TRANSITION_SCHEMA: &str = "agent-harness.external-effect-transition.v1";
pub const CONNECTOR_APPROVAL_TOKEN_SCHEMA: &str = "agent-harness.connector-approval-token.v1";
pub const GITHUB_ISSUE_READBACK_EVIDENCE_SCHEMA: &str =
    "agent-harness.github-issue-readback-evidence.v1";
pub const PROVIDER_IDEMPOTENCY_READBACK_EVIDENCE_SCHEMA: &str =
    "agent-harness.provider-idempotency-readback-evidence.v1";
const DEFAULT_APPROVAL_TOKEN_TTL_MS: i64 = 15 * 60 * 1_000;
const MAX_CONNECTOR_BYTES: usize = 96;
const MAX_ACTION_BYTES: usize = 160;
const MAX_SUMMARY_BYTES: usize = 240;
const MAX_EFFECT_RECORD_BYTES: u64 = 32 * 1024;
pub const MAX_EXTERNAL_EFFECT_EXPIRY_SCAN_ROWS: usize = 256;
const EXTERNAL_EFFECT_PUBLIC_ACTION_INDEX_SCHEMA: &str =
    "agent-harness.external-effect-public-action-index.v1";

static APPROVAL_RESOLUTION_LOCK: Mutex<()> = Mutex::new(());

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ExternalEffectPublicActionIndexV1 {
    schema: String,
    public_action_id: String,
    effect_id: String,
    approval_generation: u64,
    decision: ChannelApprovalDecisionV1,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ConnectorApprovalModeV1 {
    Deny,
    #[default]
    NeedsUser,
    ExplicitActionToken,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConnectorApprovalRuleV1 {
    pub connector: String,
    #[serde(default)]
    pub actions: Vec<String>,
    pub mode: ConnectorApprovalModeV1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConnectorApprovalPolicyV1 {
    #[serde(default)]
    pub default: ConnectorApprovalModeV1,
    #[serde(default)]
    pub rules: Vec<ConnectorApprovalRuleV1>,
}

impl Default for ConnectorApprovalPolicyV1 {
    fn default() -> Self {
        Self {
            default: ConnectorApprovalModeV1::NeedsUser,
            rules: Vec::new(),
        }
    }
}

impl ConnectorApprovalPolicyV1 {
    pub fn mode_for(&self, connector: &str, action: &str) -> ConnectorApprovalModeV1 {
        self.rules
            .iter()
            .find(|rule| {
                rule.connector.eq_ignore_ascii_case(connector)
                    && (rule.actions.is_empty()
                        || rule
                            .actions
                            .iter()
                            .any(|candidate| candidate.eq_ignore_ascii_case(action)))
            })
            .map(|rule| rule.mode)
            .unwrap_or(self.default)
    }

    pub fn validate(&self) -> io::Result<()> {
        for rule in &self.rules {
            validate_bounded_identifier(&rule.connector, "connector", MAX_CONNECTOR_BYTES)?;
            for action in &rule.actions {
                validate_bounded_identifier(action, "action", MAX_ACTION_BYTES)?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ExternalEffectStateV1 {
    Requested,
    ApprovalRequired,
    Approved,
    Denied,
    Submitted,
    Confirmed,
    Ambiguous,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectorApprovalTokenV1 {
    pub schema: String,
    /// Raw capability material is retained only in the protected latest-state
    /// snapshot. Generic receipts and command effects serialize the digest and
    /// binding metadata, never the bearer token itself.
    #[serde(skip_serializing)]
    pub token: String,
    pub token_digest: String,
    pub effect_id: String,
    pub exact_lane_digest: String,
    pub params_digest: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub source_session_key_digest: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub approval_authority_digest: String,
    pub approval_generation: u64,
    pub expires_at_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumed_at_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExternalEffectIntentV1 {
    pub schema: String,
    pub effect_id: String,
    pub exact_lane_digest: String,
    pub logical_lineage_id: String,
    pub source_queue_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub source_session_key_digest: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub approval_authority_digest: String,
    pub connector: String,
    pub action: String,
    pub params_digest: String,
    pub approval_generation: u64,
    pub state: ExternalEffectStateV1,
    pub action_summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_token: Option<ConnectorApprovalTokenV1>,
    pub requested_at_ms: i64,
    pub updated_at_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expiry_resolution_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expiry_notice_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExternalEffectTransitionV1 {
    pub schema: String,
    pub effect_id: String,
    pub from_state: Option<ExternalEffectStateV1>,
    pub to_state: ExternalEffectStateV1,
    pub approval_generation: u64,
    pub at_ms: i64,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notice_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalEffectRequestContextV1 {
    pub exact_lane_digest: String,
    pub logical_lineage_id: String,
    pub source_queue_id: String,
    pub source_session_key_digest: String,
    pub approval_authority_digest: String,
}

pub fn external_effect_source_session_key_digest(session_key: &str) -> io::Result<String> {
    if session_key.trim().is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "external effect source session key must not be empty",
        ));
    }
    Ok(sha256_tagged(
        format!("agent-harness.external-effect-source-session.v1\0{session_key}").as_bytes(),
    ))
}

pub fn external_effect_approval_authority_digest(
    exact_lane_digest: &str,
    source_session_key_digest: &str,
    logical_lineage_id: &str,
    source_queue_id: &str,
) -> io::Result<String> {
    validate_digest(exact_lane_digest, "exact lane digest")?;
    validate_digest(source_session_key_digest, "source session key digest")?;
    if logical_lineage_id.trim().is_empty() || source_queue_id.trim().is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "external effect approval authority requires lineage and source queue identity",
        ));
    }
    let payload = serde_json::json!({
        "schema": "agent-harness.external-effect-approval-authority.v1",
        "exactLaneDigest": exact_lane_digest,
        "sourceSessionKeyDigest": source_session_key_digest,
        "logicalLineageId": logical_lineage_id,
        "sourceQueueId": source_queue_id,
    });
    Ok(sha256_tagged(
        &serde_json::to_vec(&payload).map_err(io::Error::other)?,
    ))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalEffectExpiryReconcileRequestV1 {
    pub now_ms: i64,
    pub max_rows: usize,
    pub after_effect_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalEffectExpiryResolutionV1 {
    pub effect_id: String,
    pub approval_generation: u64,
    pub resolution_id: String,
    pub notice_id: String,
    pub newly_transitioned: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalEffectExpiryReconcileReportV1 {
    pub scanned_rows: usize,
    pub resolutions: Vec<ExternalEffectExpiryResolutionV1>,
    pub next_after_effect_id: Option<String>,
    pub exhausted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExternalEffectContinuationV1 {
    pub effect_id: String,
    pub source_queue_id: String,
    pub child_queue_id: String,
    pub exact_lane_digest: String,
    pub requeued: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpElicitationDescriptorV1 {
    pub connector: String,
    pub action: String,
    pub params_digest: String,
    pub action_summary: String,
    pub mode: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExternalEffectAdmissionV1 {
    Authorized(ExternalEffectIntentV1),
    NeedsUser {
        intent: ExternalEffectIntentV1,
        token: String,
    },
    Denied(ExternalEffectIntentV1),
    AlreadyConfirmed(ExternalEffectIntentV1),
    Ambiguous(ExternalEffectIntentV1),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExternalEffectApprovalDecisionV1 {
    Approve,
    Deny,
}

pub fn external_effect_mutation_evidence(
    state: ExternalEffectStateV1,
) -> crate::RuntimeMutationEvidenceClass {
    match state {
        ExternalEffectStateV1::Requested
        | ExternalEffectStateV1::ApprovalRequired
        | ExternalEffectStateV1::Approved
        | ExternalEffectStateV1::Denied => {
            crate::RuntimeMutationEvidenceClass::NoObservableMutation
        }
        ExternalEffectStateV1::Submitted | ExternalEffectStateV1::Ambiguous => {
            crate::RuntimeMutationEvidenceClass::Unknown
        }
        ExternalEffectStateV1::Confirmed => crate::RuntimeMutationEvidenceClass::MutationObserved,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExternalEffectReadbackV1 {
    Present,
    Absent,
    Unprovable,
}

pub trait ExternalEffectReadbackAdapter {
    fn readback(&self, intent: &ExternalEffectIntentV1) -> io::Result<ExternalEffectReadbackV1>;
}

/// Sanitized result of a complete or partial GitHub issue marker lookup.
///
/// The adapter deliberately accepts no title, body, repository URL, issue id,
/// or account metadata. A caller that cannot prove its marker query covered the
/// exact approved scope must leave `query_complete` false, which can never
/// authorize a resubmission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GithubIssueReadbackEvidenceV1 {
    pub schema: String,
    pub params_digest: String,
    pub query_complete: bool,
    #[serde(default)]
    pub observed_effect_markers: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GithubIssueReadbackAdapterV1 {
    evidence: GithubIssueReadbackEvidenceV1,
}

impl GithubIssueReadbackAdapterV1 {
    pub fn new(evidence: GithubIssueReadbackEvidenceV1) -> io::Result<Self> {
        if evidence.schema != GITHUB_ISSUE_READBACK_EVIDENCE_SCHEMA {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unsupported GitHub issue readback evidence schema",
            ));
        }
        validate_digest(&evidence.params_digest, "GitHub readback parameter digest")?;
        if evidence.observed_effect_markers.len() > 256 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "GitHub issue readback evidence contains too many markers",
            ));
        }
        for marker in &evidence.observed_effect_markers {
            validate_effect_marker(marker)?;
        }
        Ok(Self { evidence })
    }
}

impl ExternalEffectReadbackAdapter for GithubIssueReadbackAdapterV1 {
    fn readback(&self, intent: &ExternalEffectIntentV1) -> io::Result<ExternalEffectReadbackV1> {
        if !intent.connector.eq_ignore_ascii_case("github")
            || !matches!(
                intent.action.to_ascii_lowercase().as_str(),
                "create_issue" | "create-issue" | "issue/create"
            )
        {
            return Ok(ExternalEffectReadbackV1::Unprovable);
        }
        if self.evidence.params_digest != intent.params_digest {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "GitHub issue readback evidence belongs to different parameters",
            ));
        }
        let marker = github_issue_effect_marker(intent).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "GitHub issue effect has no deterministic readback marker",
            )
        })?;
        if self
            .evidence
            .observed_effect_markers
            .iter()
            .any(|observed| observed == &marker)
        {
            return Ok(ExternalEffectReadbackV1::Present);
        }
        Ok(if self.evidence.query_complete {
            ExternalEffectReadbackV1::Absent
        } else {
            ExternalEffectReadbackV1::Unprovable
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ProviderIdempotencyReadbackStateV1 {
    Present,
    Absent,
    Unknown,
}

/// Sanitized readback from a connector that natively supports an idempotency
/// key. The stable key and parameter digest must both match the durable effect;
/// a mismatched observation is rejected instead of being treated as absence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProviderIdempotencyReadbackEvidenceV1 {
    pub schema: String,
    pub connector: String,
    pub action: String,
    pub params_digest: String,
    pub idempotency_key: String,
    pub state: ProviderIdempotencyReadbackStateV1,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderIdempotencyReadbackAdapterV1 {
    evidence: ProviderIdempotencyReadbackEvidenceV1,
}

impl ProviderIdempotencyReadbackAdapterV1 {
    pub fn new(evidence: ProviderIdempotencyReadbackEvidenceV1) -> io::Result<Self> {
        if evidence.schema != PROVIDER_IDEMPOTENCY_READBACK_EVIDENCE_SCHEMA {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unsupported provider idempotency readback evidence schema",
            ));
        }
        validate_bounded_identifier(&evidence.connector, "connector", MAX_CONNECTOR_BYTES)?;
        validate_bounded_identifier(&evidence.action, "action", MAX_ACTION_BYTES)?;
        validate_digest(
            &evidence.params_digest,
            "provider readback parameter digest",
        )?;
        validate_bounded_identifier(&evidence.idempotency_key, "idempotency key", 96)?;
        Ok(Self { evidence })
    }
}

impl ExternalEffectReadbackAdapter for ProviderIdempotencyReadbackAdapterV1 {
    fn readback(&self, intent: &ExternalEffectIntentV1) -> io::Result<ExternalEffectReadbackV1> {
        if !self
            .evidence
            .connector
            .eq_ignore_ascii_case(&intent.connector)
            || !self.evidence.action.eq_ignore_ascii_case(&intent.action)
            || self.evidence.params_digest != intent.params_digest
            || self.evidence.idempotency_key != external_effect_idempotency_key(intent)
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "provider idempotency readback evidence is not bound to the durable effect",
            ));
        }
        Ok(match self.evidence.state {
            ProviderIdempotencyReadbackStateV1::Present => ExternalEffectReadbackV1::Present,
            ProviderIdempotencyReadbackStateV1::Absent => ExternalEffectReadbackV1::Absent,
            ProviderIdempotencyReadbackStateV1::Unknown => ExternalEffectReadbackV1::Unprovable,
        })
    }
}

pub fn external_effect_idempotency_key(intent: &ExternalEffectIntentV1) -> String {
    format!("ahx-{}", intent.effect_id)
}

pub fn github_issue_effect_marker(intent: &ExternalEffectIntentV1) -> Option<String> {
    (intent.connector.eq_ignore_ascii_case("github")
        && matches!(
            intent.action.to_ascii_lowercase().as_str(),
            "create_issue" | "create-issue" | "issue/create"
        ))
    .then(|| {
        format!(
            "<!-- agent-harness-effect:{} -->",
            external_effect_idempotency_key(intent)
        )
    })
}

pub fn load_connector_approval_policy(
    harness_home: &Path,
) -> io::Result<ConnectorApprovalPolicyV1> {
    let config_file = harness_home.join("harness-config.json");
    let text = match fs::read_to_string(&config_file) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(ConnectorApprovalPolicyV1::default());
        }
        Err(error) => return Err(error),
    };
    let root: Value = serde_json::from_str(&text).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid harness config {}: {error}", config_file.display()),
        )
    })?;
    let Some(value) = root.pointer("/security/connectorApprovalPolicy") else {
        return Ok(ConnectorApprovalPolicyV1::default());
    };
    let policy: ConnectorApprovalPolicyV1 =
        serde_json::from_value(value.clone()).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid security.connectorApprovalPolicy: {error}"),
            )
        })?;
    policy.validate()?;
    Ok(policy)
}

pub fn parse_mcp_elicitation_descriptor(
    value: &Value,
    active_tool_preview: Option<&str>,
) -> io::Result<McpElicitationDescriptorV1> {
    if value.get("method").and_then(Value::as_str) != Some("mcpServer/elicitation/request") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "not an MCP elicitation request",
        ));
    }
    let params = value.get("params").ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "MCP elicitation params are missing",
        )
    })?;
    let connector = first_string(
        params,
        &["/serverName", "/server", "/connectorName", "/connector"],
    )
    .ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "MCP elicitation serverName is missing",
        )
    })?;
    let mode = first_string(params, &["/mode"]).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "MCP elicitation mode is missing",
        )
    })?;
    if !matches!(mode.as_str(), "form" | "openai/form" | "url") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported MCP elicitation mode {mode}"),
        ));
    }
    let action = first_string(
        params,
        &[
            "/_meta/actionName",
            "/_meta/action",
            "/actionName",
            "/action",
        ],
    )
    .or_else(|| active_tool_preview.map(str::to_string))
    .unwrap_or_else(|| format!("{mode}-elicitation"));
    validate_bounded_identifier(&connector, "connector", MAX_CONNECTOR_BYTES)?;
    let action = sanitize_bounded(&action, MAX_ACTION_BYTES, "connector-action");
    let params_bytes = serde_json::to_vec(params).map_err(io::Error::other)?;
    let params_digest = sha256_tagged(&params_bytes);
    let message = first_string(params, &["/message"])
        .unwrap_or_else(|| "connector action approval".to_string());
    let action_summary = sanitize_bounded(
        &format!("{connector}/{action}: {message}"),
        MAX_SUMMARY_BYTES,
        "connector approval",
    );
    Ok(McpElicitationDescriptorV1 {
        connector,
        action,
        params_digest,
        action_summary,
        mode,
    })
}

pub fn begin_external_effect_request(
    harness_home: &Path,
    context: &ExternalEffectRequestContextV1,
    descriptor: &McpElicitationDescriptorV1,
    policy: &ConnectorApprovalPolicyV1,
) -> io::Result<ExternalEffectAdmissionV1> {
    validate_digest(&context.exact_lane_digest, "exact lane digest")?;
    validate_digest(
        &context.source_session_key_digest,
        "source session key digest",
    )?;
    validate_digest(
        &context.approval_authority_digest,
        "approval authority digest",
    )?;
    validate_digest(&descriptor.params_digest, "parameter digest")?;
    let effect_id = effect_id(context, descriptor);
    if let Some(latest) = load_external_effect_intent(harness_home, &effect_id)? {
        if !latest.source_session_key_digest.is_empty()
            && (latest.source_session_key_digest != context.source_session_key_digest
                || latest.approval_authority_digest != context.approval_authority_digest)
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "existing external effect belongs to another approval authority",
            ));
        }
        return match latest.state {
            ExternalEffectStateV1::Approved => Ok(ExternalEffectAdmissionV1::Authorized(latest)),
            ExternalEffectStateV1::Requested => {
                advance_requested_external_effect(harness_home, latest, policy)
            }
            ExternalEffectStateV1::ApprovalRequired => {
                ensure_external_effect_public_action_indexes(harness_home, &latest)?;
                let token = latest
                    .approval_token
                    .as_ref()
                    .map(|token| token.token.clone())
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "approval-required effect is missing its action token",
                        )
                    })?;
                Ok(ExternalEffectAdmissionV1::NeedsUser {
                    intent: latest,
                    token,
                })
            }
            ExternalEffectStateV1::Denied => Ok(ExternalEffectAdmissionV1::Denied(latest)),
            ExternalEffectStateV1::Confirmed => {
                Ok(ExternalEffectAdmissionV1::AlreadyConfirmed(latest))
            }
            ExternalEffectStateV1::Submitted | ExternalEffectStateV1::Ambiguous => {
                Ok(ExternalEffectAdmissionV1::Ambiguous(latest))
            }
        };
    }

    let now = current_log_time_ms()?;
    let intent = ExternalEffectIntentV1 {
        schema: EXTERNAL_EFFECT_INTENT_SCHEMA.to_string(),
        effect_id,
        exact_lane_digest: context.exact_lane_digest.clone(),
        logical_lineage_id: sanitize_bounded(&context.logical_lineage_id, 160, "logical-lineage"),
        source_queue_id: sanitize_bounded(&context.source_queue_id, 160, "source-queue"),
        source_session_key_digest: context.source_session_key_digest.clone(),
        approval_authority_digest: context.approval_authority_digest.clone(),
        connector: descriptor.connector.clone(),
        action: descriptor.action.clone(),
        params_digest: descriptor.params_digest.clone(),
        approval_generation: 1,
        state: ExternalEffectStateV1::Requested,
        action_summary: descriptor.action_summary.clone(),
        approval_token: None,
        requested_at_ms: now,
        updated_at_ms: now,
        reason: Some("MCP elicitation observed before any protocol response".to_string()),
        expiry_resolution_id: None,
        expiry_notice_id: None,
    };
    persist_transition(harness_home, None, &intent)?;

    advance_requested_external_effect(harness_home, intent, policy)
}

fn advance_requested_external_effect(
    harness_home: &Path,
    mut intent: ExternalEffectIntentV1,
    policy: &ConnectorApprovalPolicyV1,
) -> io::Result<ExternalEffectAdmissionV1> {
    if intent.state != ExternalEffectStateV1::Requested {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "only a requested external effect can advance through approval policy",
        ));
    }
    let now = current_log_time_ms()?;
    let mode =
        if protected_connector_action(&intent.connector, &intent.action, &intent.action_summary) {
            ConnectorApprovalModeV1::Deny
        } else {
            policy.mode_for(&intent.connector, &intent.action)
        };
    match mode {
        ConnectorApprovalModeV1::Deny => {
            intent.state = ExternalEffectStateV1::Denied;
            intent.updated_at_ms = current_log_time_ms()?;
            intent.reason = Some("connector approval policy denied the action".to_string());
            persist_transition(
                harness_home,
                Some(ExternalEffectStateV1::Requested),
                &intent,
            )?;
            Ok(ExternalEffectAdmissionV1::Denied(intent))
        }
        ConnectorApprovalModeV1::NeedsUser | ConnectorApprovalModeV1::ExplicitActionToken => {
            let token = new_approval_token(&intent, now + DEFAULT_APPROVAL_TOKEN_TTL_MS)?;
            let raw = token.token.clone();
            intent.approval_token = Some(token);
            intent.state = ExternalEffectStateV1::ApprovalRequired;
            intent.updated_at_ms = current_log_time_ms()?;
            intent.reason = Some("explicit lane-bound user approval is required".to_string());
            persist_transition(
                harness_home,
                Some(ExternalEffectStateV1::Requested),
                &intent,
            )?;
            ensure_external_effect_public_action_indexes(harness_home, &intent)?;
            Ok(ExternalEffectAdmissionV1::NeedsUser { intent, token: raw })
        }
    }
}

pub fn resolve_external_effect_approval(
    harness_home: &Path,
    token: &str,
    exact_lane_digest: &str,
    decision: ExternalEffectApprovalDecisionV1,
) -> io::Result<ExternalEffectIntentV1> {
    validate_digest(exact_lane_digest, "exact lane digest")?;
    let _guard = approval_resolution_guard()?;
    let token_digest = sha256_tagged(token.as_bytes());
    let intent = find_external_effect_by_token(harness_home, &token_digest)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "connector approval token was not found",
        )
    })?;
    resolve_external_effect_approval_locked(
        harness_home,
        intent,
        token,
        exact_lane_digest,
        decision,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn resolve_external_effect_public_channel_action(
    harness_home: &Path,
    provider_event_id: &str,
    provider_message_id: Option<String>,
    public_action_id: &str,
    exact_lane_digest: &str,
    source_session_key_digest: &str,
    expected_decision: Option<ChannelApprovalDecisionV1>,
) -> io::Result<ExternalEffectIntentV1> {
    resolve_external_effect_public_channel_action_inner(
        harness_home,
        provider_event_id,
        provider_message_id,
        public_action_id,
        exact_lane_digest,
        source_session_key_digest,
        expected_decision,
    )
}

/// Resolves a provider-native approval action after channel ingress has already
/// established the concrete exact lane. Unlike the legacy text-token path,
/// native actions require the complete session and authority digest binding.
pub fn resolve_external_effect_channel_action(
    harness_home: &Path,
    action: &ChannelInboundActionV1,
    exact_lane_digest: &str,
) -> io::Result<ExternalEffectIntentV1> {
    action.validate().map_err(|error| {
        io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("invalid channel approval action: {error}"),
        )
    })?;
    validate_digest(exact_lane_digest, "exact lane digest")?;
    let _guard = approval_resolution_guard()?;
    let intent =
        load_external_effect_intent(harness_home, &action.effect_id)?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "channel approval action effect was not found",
            )
        })?;
    let decision = match action.decision {
        ChannelApprovalDecisionV1::Approve => ExternalEffectApprovalDecisionV1::Approve,
        ChannelApprovalDecisionV1::Deny => ExternalEffectApprovalDecisionV1::Deny,
    };
    resolve_external_effect_approval_locked(
        harness_home,
        intent,
        action.expose_callback_token(),
        exact_lane_digest,
        decision,
        Some(action),
    )
}

fn resolve_external_effect_approval_locked(
    harness_home: &Path,
    mut intent: ExternalEffectIntentV1,
    token: &str,
    exact_lane_digest: &str,
    decision: ExternalEffectApprovalDecisionV1,
    native_action: Option<&ChannelInboundActionV1>,
) -> io::Result<ExternalEffectIntentV1> {
    let token_digest = sha256_tagged(token.as_bytes());
    if intent.exact_lane_digest != exact_lane_digest {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "connector approval token belongs to another exact lane",
        ));
    }
    if let Some(action) = native_action {
        if action.effect_id != intent.effect_id
            || action.approval_generation != intent.approval_generation
            || intent.source_session_key_digest.is_empty()
            || intent.approval_authority_digest.is_empty()
            || action.source_session_key_digest != intent.source_session_key_digest
            || action.source_authority_digest != intent.approval_authority_digest
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "channel approval action authority binding does not match the effect",
            ));
        }
    }
    let expiry_ids = expiry_resolution_identities(&intent);
    let now = current_log_time_ms()?;
    let approval_token = intent.approval_token.as_mut().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "effect approval token is missing",
        )
    })?;
    if approval_token.schema != CONNECTOR_APPROVAL_TOKEN_SCHEMA
        || approval_token.token_digest != token_digest
        || approval_token.effect_id != intent.effect_id
        || approval_token.exact_lane_digest != intent.exact_lane_digest
        || approval_token.params_digest != intent.params_digest
        || approval_token.approval_generation != intent.approval_generation
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "connector approval token binding does not match the effect",
        ));
    }
    if native_action.is_some()
        && (approval_token.source_session_key_digest.is_empty()
            || approval_token.approval_authority_digest.is_empty()
            || approval_token.source_session_key_digest != intent.source_session_key_digest
            || approval_token.approval_authority_digest != intent.approval_authority_digest)
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "native approval token is missing its full authority binding",
        ));
    }
    if intent.expiry_resolution_id.is_some() {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "connector approval token has expired",
        ));
    }
    if approval_token.expires_at_ms <= now {
        let (resolution_id, notice_id) = expiry_ids;
        approval_token.consumed_at_ms = Some(now);
        let from = intent.state;
        intent.state = ExternalEffectStateV1::Denied;
        intent.updated_at_ms = now;
        intent.reason = Some("connector approval token expired and was fenced".to_string());
        intent.expiry_resolution_id = Some(resolution_id);
        intent.expiry_notice_id = Some(notice_id);
        if from == ExternalEffectStateV1::ApprovalRequired {
            persist_transition(harness_home, Some(from), &intent)?;
        }
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "connector approval token has expired",
        ));
    }
    if approval_token.consumed_at_ms.is_some() {
        let same_decision = matches!(
            (decision, intent.state),
            (
                ExternalEffectApprovalDecisionV1::Approve,
                ExternalEffectStateV1::Approved
            ) | (
                ExternalEffectApprovalDecisionV1::Deny,
                ExternalEffectStateV1::Denied
            )
        );
        if same_decision {
            return Ok(intent);
        }
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "connector approval token was already consumed for another disposition",
        ));
    }
    if intent.state != ExternalEffectStateV1::ApprovalRequired {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "connector approval token has been fenced by a later effect state",
        ));
    }
    approval_token.consumed_at_ms = Some(now);
    let from = intent.state;
    intent.state = match decision {
        ExternalEffectApprovalDecisionV1::Approve => ExternalEffectStateV1::Approved,
        ExternalEffectApprovalDecisionV1::Deny => ExternalEffectStateV1::Denied,
    };
    intent.updated_at_ms = now;
    intent.reason = Some(match decision {
        ExternalEffectApprovalDecisionV1::Approve => {
            "explicit action token approved once".to_string()
        }
        ExternalEffectApprovalDecisionV1::Deny => {
            "explicit action token denied and fenced".to_string()
        }
    });
    persist_transition(harness_home, Some(from), &intent)?;
    Ok(intent)
}

fn approval_resolution_guard() -> io::Result<std::sync::MutexGuard<'static, ()>> {
    APPROVAL_RESOLUTION_LOCK
        .lock()
        .map_err(|_| io::Error::other("external effect approval resolution lock was poisoned"))
}

#[allow(clippy::too_many_arguments)]
fn resolve_external_effect_public_channel_action_inner(
    harness_home: &Path,
    provider_event_id: &str,
    provider_message_id: Option<String>,
    public_action_id: &str,
    exact_lane_digest: &str,
    source_session_key_digest: &str,
    expected_decision: Option<ChannelApprovalDecisionV1>,
) -> io::Result<ExternalEffectIntentV1> {
    validate_public_action_id_shape(public_action_id)?;
    validate_digest(exact_lane_digest, "exact lane digest")?;
    validate_digest(source_session_key_digest, "source session key digest")?;
    let bytes = fs::read(external_effect_public_action_index_file(
        harness_home,
        public_action_id,
    ))
    .map_err(|error| {
        if error.kind() == io::ErrorKind::NotFound {
            io::Error::new(
                io::ErrorKind::NotFound,
                "channel approval action reference was not found",
            )
        } else {
            error
        }
    })?;
    if bytes.len() > MAX_EFFECT_RECORD_BYTES as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "channel approval action index is unbounded",
        ));
    }
    let index: ExternalEffectPublicActionIndexV1 =
        serde_json::from_slice(&bytes).map_err(io::Error::other)?;
    validate_external_effect_public_action_index(&index)?;
    if index.public_action_id != public_action_id {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "channel approval action index does not match the requested reference",
        ));
    }
    if expected_decision.is_some_and(|expected| expected != index.decision) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "public approval action decision does not match the requested command",
        ));
    }
    let intent = load_external_effect_intent(harness_home, &index.effect_id)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "channel approval action effect was not found",
        )
    })?;
    if intent.approval_generation != index.approval_generation
        || intent.exact_lane_digest != exact_lane_digest
        || intent.source_session_key_digest != source_session_key_digest
        || intent.approval_authority_digest.is_empty()
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "channel approval action authority changed after prompt creation",
        ));
    }
    let raw_token = intent
        .approval_token
        .as_ref()
        .map(|token| token.token.clone())
        .filter(|token| !token.is_empty())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "channel approval action has no protected bearer binding",
            )
        })?;
    let action = ChannelInboundActionV1::new(
        provider_event_id,
        provider_message_id,
        index.effect_id,
        index.approval_generation,
        index.decision,
        source_session_key_digest,
        intent.approval_authority_digest.clone(),
        raw_token,
    )
    .map_err(|error| io::Error::new(io::ErrorKind::PermissionDenied, error))?;
    resolve_external_effect_channel_action(harness_home, &action, exact_lane_digest)
}

/// Advances at most `max_rows` protected snapshots from an expired approval
/// wait to a durable denial. The caller owns the interval and persists the
/// returned cursor between bounded runtime-loop iterations.
pub fn reconcile_expired_external_effect_approvals(
    harness_home: &Path,
    request: &ExternalEffectExpiryReconcileRequestV1,
) -> io::Result<ExternalEffectExpiryReconcileReportV1> {
    if request.now_ms < 0
        || request.max_rows == 0
        || request.max_rows > MAX_EXTERNAL_EFFECT_EXPIRY_SCAN_ROWS
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "external effect expiry scan request is outside its bounded policy",
        ));
    }
    if let Some(cursor) = request.after_effect_id.as_deref() {
        validate_effect_id(cursor)?;
    }

    let dir = external_effect_latest_dir(harness_home);
    let entries = match fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(ExternalEffectExpiryReconcileReportV1 {
                scanned_rows: 0,
                resolutions: Vec::new(),
                next_after_effect_id: None,
                exhausted: true,
            });
        }
        Err(error) => return Err(error),
    };
    let mut effect_ids = Vec::new();
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_file() || entry.metadata()?.len() > MAX_EFFECT_RECORD_BYTES {
            continue;
        }
        let path = entry.path();
        let Some(effect_id) = path.file_stem().and_then(|name| name.to_str()) else {
            continue;
        };
        if validate_effect_id(effect_id).is_ok()
            && request
                .after_effect_id
                .as_deref()
                .is_none_or(|cursor| effect_id > cursor)
        {
            effect_ids.push(effect_id.to_string());
        }
    }
    effect_ids.sort_unstable();
    let exhausted = effect_ids.len() <= request.max_rows;
    effect_ids.truncate(request.max_rows);
    let next_after_effect_id = if exhausted {
        None
    } else {
        effect_ids.last().cloned()
    };

    let _guard = approval_resolution_guard()?;
    let mut resolutions = Vec::new();
    for effect_id in &effect_ids {
        let Some(mut intent) = load_external_effect_intent(harness_home, effect_id)? else {
            continue;
        };
        if intent.state == ExternalEffectStateV1::Denied {
            if let (Some(resolution_id), Some(notice_id)) = (
                intent.expiry_resolution_id.clone(),
                intent.expiry_notice_id.clone(),
            ) {
                resolutions.push(ExternalEffectExpiryResolutionV1 {
                    effect_id: intent.effect_id,
                    approval_generation: intent.approval_generation,
                    resolution_id,
                    notice_id,
                    newly_transitioned: false,
                });
            }
            continue;
        }
        if intent.state != ExternalEffectStateV1::ApprovalRequired {
            continue;
        }
        let expires_at_ms = intent
            .approval_token
            .as_ref()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "approval-required effect is missing its action token",
                )
            })?
            .expires_at_ms;
        if expires_at_ms > request.now_ms {
            continue;
        }
        let (resolution_id, notice_id) = expiry_resolution_identities(&intent);
        let token = intent.approval_token.as_mut().expect("checked above");
        token.consumed_at_ms = Some(request.now_ms);
        let from = intent.state;
        intent.state = ExternalEffectStateV1::Denied;
        intent.updated_at_ms = request.now_ms;
        intent.reason = Some("connector approval token expired and was fenced".to_string());
        intent.expiry_resolution_id = Some(resolution_id.clone());
        intent.expiry_notice_id = Some(notice_id.clone());
        persist_transition(harness_home, Some(from), &intent)?;
        resolutions.push(ExternalEffectExpiryResolutionV1 {
            effect_id: intent.effect_id,
            approval_generation: intent.approval_generation,
            resolution_id,
            notice_id,
            newly_transitioned: true,
        });
    }

    Ok(ExternalEffectExpiryReconcileReportV1 {
        scanned_rows: effect_ids.len(),
        resolutions,
        next_after_effect_id,
        exhausted,
    })
}

fn expiry_resolution_identities(intent: &ExternalEffectIntentV1) -> (String, String) {
    let canonical = format!(
        "external-effect-approval-expiry.v1\0{}\0{}",
        intent.effect_id, intent.approval_generation
    );
    let digest = hex(digest::digest(&digest::SHA256, canonical.as_bytes()).as_ref());
    (format!("ahex1_{digest}"), format!("ahen1_{digest}"))
}

fn validate_effect_id(effect_id: &str) -> io::Result<()> {
    if effect_id.len() != 64
        || !effect_id
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "external effect ID must be a lowercase SHA-256 digest",
        ));
    }
    Ok(())
}

pub fn fence_external_effects_for_lane(
    harness_home: &Path,
    exact_lane_digest: &str,
    reason: &str,
) -> io::Result<Vec<ExternalEffectIntentV1>> {
    validate_digest(exact_lane_digest, "exact lane digest")?;
    let dir = external_effect_latest_dir(harness_home);
    let entries = match fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let now = current_log_time_ms()?;
    let mut fenced = Vec::new();
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_file() || entry.metadata()?.len() > MAX_EFFECT_RECORD_BYTES {
            continue;
        }
        let text = fs::read_to_string(entry.path())?;
        let Ok(mut intent) = serde_json::from_str::<ExternalEffectIntentV1>(&text) else {
            continue;
        };
        if intent.exact_lane_digest != exact_lane_digest
            || !matches!(
                intent.state,
                ExternalEffectStateV1::ApprovalRequired | ExternalEffectStateV1::Approved
            )
        {
            continue;
        }
        let from = intent.state;
        if let Some(token) = intent.approval_token.as_mut() {
            token.consumed_at_ms = Some(now);
        }
        intent.state = ExternalEffectStateV1::Denied;
        intent.updated_at_ms = now;
        intent.reason = Some(sanitize_bounded(reason, MAX_SUMMARY_BYTES, "fenced"));
        persist_transition(harness_home, Some(from), &intent)?;
        fenced.push(intent);
    }
    Ok(fenced)
}

pub fn ensure_external_effect_continuation(
    harness_home: &Path,
    intent: &ExternalEffectIntentV1,
) -> io::Result<ExternalEffectContinuationV1> {
    if intent.state != ExternalEffectStateV1::Approved {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "only an approved external effect can schedule a continuation",
        ));
    }
    let queue_file = harness_home
        .join("state")
        .join("runtime-queue")
        .join("pending.jsonl");
    let text = fs::read_to_string(&queue_file)?;
    let mut parent_session_key = None;
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let value: Value = serde_json::from_str(line).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid runtime pending queue JSONL: {error}"),
            )
        })?;
        if value.get("continuationIntentKey").and_then(Value::as_str)
            == Some(intent.effect_id.as_str())
        {
            let child_queue_id = value
                .get("queueId")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "external-effect continuation has no queueId",
                    )
                })?;
            return Ok(ExternalEffectContinuationV1 {
                effect_id: intent.effect_id.clone(),
                source_queue_id: intent.source_queue_id.clone(),
                child_queue_id: child_queue_id.to_string(),
                exact_lane_digest: intent.exact_lane_digest.clone(),
                requeued: false,
            });
        }
        if value.get("queueId").and_then(Value::as_str) == Some(intent.source_queue_id.as_str()) {
            let lane = crate::ChannelStateLane::new(
                value.get("platform").and_then(Value::as_str).unwrap_or(""),
                value.get("accountId").and_then(Value::as_str),
                value.get("channelId").and_then(Value::as_str).unwrap_or(""),
                value.get("userId").and_then(Value::as_str).unwrap_or(""),
                value.get("agentId").and_then(Value::as_str).unwrap_or(""),
            )?;
            if lane.exact_lane_digest() != intent.exact_lane_digest {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "external effect source queue moved to another exact lane",
                ));
            }
            parent_session_key = value
                .get("sessionKey")
                .and_then(Value::as_str)
                .map(str::to_string);
        }
    }
    let parent_session_key = parent_session_key.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "external effect source queue was not found",
        )
    })?;
    let idempotency_instruction = github_issue_effect_marker(intent)
        .map(|marker| format!("; preserve this opaque marker in the issue body: {marker}"))
        .unwrap_or_else(|| {
            format!(
                "; preserve provider idempotency key {} when the connector supports one",
                external_effect_idempotency_key(intent)
            )
        });
    let report =
        crate::requeue_prepared_context_rollover(crate::ContextRolloverRequeuePreparedOptions {
            harness_home: harness_home.to_path_buf(),
            queue_id: intent.source_queue_id.clone(),
            new_working_session_key: parent_session_key,
            reason: format!(
                "approved external effect {} resumes once in the exact lane{}",
                intent.effect_id, idempotency_instruction
            ),
            now_ms: current_log_time_ms()?,
            preserve_continuation_index: true,
            campaign_slice_generation: None,
            task_slice_generation: None,
            task_family_id: None,
            task_family_version: None,
            task_root_queue_id: None,
            disposition_recovery_depth: None,
            replacement_message_text: None,
            continuation_intent_key: Some(intent.effect_id.clone()),
            completion_kind: Some("external-effect-continuation".to_string()),
            allow_exact_state_bootstrap: false,
        })?;
    Ok(ExternalEffectContinuationV1 {
        effect_id: intent.effect_id.clone(),
        source_queue_id: intent.source_queue_id.clone(),
        child_queue_id: report.requeued_queue_id,
        exact_lane_digest: intent.exact_lane_digest.clone(),
        requeued: report.requeued,
    })
}

pub fn transition_external_effect(
    harness_home: &Path,
    effect_id: &str,
    next: ExternalEffectStateV1,
    reason: &str,
) -> io::Result<ExternalEffectIntentV1> {
    let mut intent = load_external_effect_intent(harness_home, effect_id)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "external effect intent was not found",
        )
    })?;
    if intent.state == next {
        return Ok(intent);
    }
    if !valid_transition(intent.state, next) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "invalid external effect transition {:?} -> {next:?}",
                intent.state
            ),
        ));
    }
    let from = intent.state;
    intent.state = next;
    intent.updated_at_ms = current_log_time_ms()?;
    intent.reason = Some(sanitize_bounded(reason, 240, "effect-transition"));
    persist_transition(harness_home, Some(from), &intent)?;
    Ok(intent)
}

pub fn reconcile_external_effect(
    harness_home: &Path,
    effect_id: &str,
    adapter: &impl ExternalEffectReadbackAdapter,
) -> io::Result<ExternalEffectIntentV1> {
    let intent = load_external_effect_intent(harness_home, effect_id)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "external effect intent was not found",
        )
    })?;
    if !matches!(
        intent.state,
        ExternalEffectStateV1::Submitted | ExternalEffectStateV1::Ambiguous
    ) {
        return Ok(intent);
    }
    match adapter.readback(&intent)? {
        ExternalEffectReadbackV1::Present => transition_external_effect(
            harness_home,
            effect_id,
            ExternalEffectStateV1::Confirmed,
            "connector readback confirmed the stable external effect",
        ),
        ExternalEffectReadbackV1::Absent => transition_external_effect(
            harness_home,
            effect_id,
            ExternalEffectStateV1::Approved,
            "connector readback proved absence; one bounded resubmission is authorized",
        ),
        ExternalEffectReadbackV1::Unprovable => transition_external_effect(
            harness_home,
            effect_id,
            ExternalEffectStateV1::Ambiguous,
            "connector readback could not prove presence or absence; user authority is required",
        ),
    }
}

pub fn load_external_effect_intent(
    harness_home: &Path,
    effect_id: &str,
) -> io::Result<Option<ExternalEffectIntentV1>> {
    let file = external_effect_latest_dir(harness_home).join(format!("{effect_id}.json"));
    let text = match fs::read_to_string(&file) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    if text.len() as u64 > MAX_EFFECT_RECORD_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "external effect snapshot exceeds the bounded record limit",
        ));
    }
    let intent: ExternalEffectIntentV1 = serde_json::from_str(&text).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "invalid external effect snapshot {}: {error}",
                file.display()
            ),
        )
    })?;
    validate_intent(&intent)?;
    Ok(Some(intent))
}

pub fn external_effect_transition_file(harness_home: &Path) -> PathBuf {
    harness_home
        .join("state")
        .join("external-effects")
        .join("transitions.jsonl")
}

fn external_effect_latest_dir(harness_home: &Path) -> PathBuf {
    harness_home
        .join("state")
        .join("external-effects")
        .join("latest")
}

fn external_effect_public_action_index_file(
    harness_home: &Path,
    public_action_id: &str,
) -> PathBuf {
    harness_home
        .join("state")
        .join("external-effects")
        .join("public-actions")
        .join(format!("{public_action_id}.json"))
}

fn ensure_external_effect_public_action_indexes(
    harness_home: &Path,
    intent: &ExternalEffectIntentV1,
) -> io::Result<()> {
    if intent.source_session_key_digest.is_empty()
        || intent.approval_authority_digest.is_empty()
        || intent.state != ExternalEffectStateV1::ApprovalRequired
    {
        return Ok(());
    }
    for decision in [
        ChannelApprovalDecisionV1::Approve,
        ChannelApprovalDecisionV1::Deny,
    ] {
        let index = ExternalEffectPublicActionIndexV1 {
            schema: EXTERNAL_EFFECT_PUBLIC_ACTION_INDEX_SCHEMA.to_string(),
            public_action_id: public_approval_action_id(
                &intent.effect_id,
                intent.approval_generation,
                decision,
            )
            .map_err(io::Error::other)?,
            effect_id: intent.effect_id.clone(),
            approval_generation: intent.approval_generation,
            decision,
        };
        validate_external_effect_public_action_index(&index)?;
        let file = external_effect_public_action_index_file(harness_home, &index.public_action_id);
        if file.exists() {
            let existing: ExternalEffectPublicActionIndexV1 =
                serde_json::from_slice(&fs::read(&file)?).map_err(io::Error::other)?;
            validate_external_effect_public_action_index(&existing)?;
            if existing != index {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "public approval action index collision",
                ));
            }
        } else {
            write_json_atomic(&file, &index)?;
        }
    }
    Ok(())
}

fn validate_external_effect_public_action_index(
    index: &ExternalEffectPublicActionIndexV1,
) -> io::Result<()> {
    if index.schema != EXTERNAL_EFFECT_PUBLIC_ACTION_INDEX_SCHEMA {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported public approval action index schema",
        ));
    }
    validate_effect_id(&index.effect_id)?;
    validate_public_approval_action_id(
        &index.public_action_id,
        &index.effect_id,
        index.approval_generation,
        index.decision,
    )
    .map_err(io::Error::other)
}

fn validate_public_action_id_shape(public_action_id: &str) -> io::Result<()> {
    let Some(suffix) = public_action_id.strip_prefix("ahpa1_") else {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "unsupported public approval action reference",
        ));
    };
    if suffix.len() != 32
        || !suffix
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "malformed public approval action reference",
        ));
    }
    Ok(())
}

fn persist_transition(
    harness_home: &Path,
    from_state: Option<ExternalEffectStateV1>,
    intent: &ExternalEffectIntentV1,
) -> io::Result<()> {
    validate_intent(intent)?;
    let transition = ExternalEffectTransitionV1 {
        schema: EXTERNAL_EFFECT_TRANSITION_SCHEMA.to_string(),
        effect_id: intent.effect_id.clone(),
        from_state,
        to_state: intent.state,
        approval_generation: intent.approval_generation,
        at_ms: intent.updated_at_ms,
        reason: intent.reason.clone().unwrap_or_default(),
        resolution_id: intent.expiry_resolution_id.clone(),
        notice_id: intent.expiry_notice_id.clone(),
    };
    let file = external_effect_transition_file(harness_home);
    if let Some(parent) = file.parent() {
        fs::create_dir_all(parent)?;
    }
    let line = serde_json::to_vec(&transition).map_err(io::Error::other)?;
    if line.len() as u64 > MAX_EFFECT_RECORD_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "external effect transition exceeds the bounded record limit",
        ));
    }
    let mut handle = OpenOptions::new().create(true).append(true).open(&file)?;
    handle.write_all(&line)?;
    handle.write_all(b"\n")?;
    handle.sync_all()?;
    let snapshot =
        external_effect_latest_dir(harness_home).join(format!("{}.json", intent.effect_id));
    let mut protected_snapshot = serde_json::to_value(intent).map_err(io::Error::other)?;
    if let Some(approval_token) = intent.approval_token.as_ref()
        && let Some(token_object) = protected_snapshot
            .get_mut("approvalToken")
            .and_then(Value::as_object_mut)
    {
        token_object.insert(
            "token".to_string(),
            Value::String(approval_token.token.clone()),
        );
    }
    write_json_atomic(&snapshot, &protected_snapshot)?;
    Ok(())
}

fn find_external_effect_by_token(
    harness_home: &Path,
    token_digest: &str,
) -> io::Result<Option<ExternalEffectIntentV1>> {
    let dir = external_effect_latest_dir(harness_home);
    let entries = match fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        if entry.metadata()?.len() > MAX_EFFECT_RECORD_BYTES {
            continue;
        }
        let text = fs::read_to_string(entry.path())?;
        let Ok(intent) = serde_json::from_str::<ExternalEffectIntentV1>(&text) else {
            continue;
        };
        if intent
            .approval_token
            .as_ref()
            .is_some_and(|token| token.token_digest == token_digest)
        {
            return Ok(Some(intent));
        }
    }
    Ok(None)
}

fn new_approval_token(
    intent: &ExternalEffectIntentV1,
    expires_at_ms: i64,
) -> io::Result<ConnectorApprovalTokenV1> {
    let mut bytes = [0_u8; 24];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| io::Error::other("secure connector approval token generation failed"))?;
    let token = format!("ahx1_{}", hex(&bytes));
    Ok(ConnectorApprovalTokenV1 {
        schema: CONNECTOR_APPROVAL_TOKEN_SCHEMA.to_string(),
        token_digest: sha256_tagged(token.as_bytes()),
        token,
        effect_id: intent.effect_id.clone(),
        exact_lane_digest: intent.exact_lane_digest.clone(),
        params_digest: intent.params_digest.clone(),
        source_session_key_digest: intent.source_session_key_digest.clone(),
        approval_authority_digest: intent.approval_authority_digest.clone(),
        approval_generation: intent.approval_generation,
        expires_at_ms,
        consumed_at_ms: None,
    })
}

fn effect_id(
    context: &ExternalEffectRequestContextV1,
    descriptor: &McpElicitationDescriptorV1,
) -> String {
    let mut bytes = Vec::new();
    for value in [
        context.exact_lane_digest.as_str(),
        context.logical_lineage_id.as_str(),
        context.source_queue_id.as_str(),
        descriptor.connector.as_str(),
        descriptor.action.as_str(),
        descriptor.params_digest.as_str(),
        "1",
    ] {
        bytes.extend_from_slice(&(value.len() as u64).to_be_bytes());
        bytes.extend_from_slice(value.as_bytes());
    }
    hex(digest::digest(&digest::SHA256, &bytes).as_ref())
}

fn valid_transition(from: ExternalEffectStateV1, to: ExternalEffectStateV1) -> bool {
    matches!(
        (from, to),
        (
            ExternalEffectStateV1::Requested,
            ExternalEffectStateV1::ApprovalRequired
        ) | (
            ExternalEffectStateV1::Requested,
            ExternalEffectStateV1::Denied
        ) | (
            ExternalEffectStateV1::ApprovalRequired,
            ExternalEffectStateV1::Approved
        ) | (
            ExternalEffectStateV1::ApprovalRequired,
            ExternalEffectStateV1::Denied
        ) | (
            ExternalEffectStateV1::Approved,
            ExternalEffectStateV1::Denied
        ) | (
            ExternalEffectStateV1::Approved,
            ExternalEffectStateV1::Submitted
        ) | (
            ExternalEffectStateV1::Submitted,
            ExternalEffectStateV1::Confirmed
        ) | (
            ExternalEffectStateV1::Submitted,
            ExternalEffectStateV1::Ambiguous
        ) | (
            ExternalEffectStateV1::Submitted,
            ExternalEffectStateV1::Approved
        ) | (
            ExternalEffectStateV1::Ambiguous,
            ExternalEffectStateV1::Approved
        ) | (
            ExternalEffectStateV1::Ambiguous,
            ExternalEffectStateV1::Confirmed
        )
    )
}

fn validate_intent(intent: &ExternalEffectIntentV1) -> io::Result<()> {
    if intent.schema != EXTERNAL_EFFECT_INTENT_SCHEMA {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported external effect intent schema",
        ));
    }
    if intent.effect_id.len() != 64
        || !intent
            .effect_id
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "external effect id is not a SHA-256 hex digest",
        ));
    }
    validate_digest(&intent.exact_lane_digest, "exact lane digest")?;
    validate_digest(&intent.params_digest, "parameter digest")?;
    validate_legacy_compatible_authority_binding(
        &intent.source_session_key_digest,
        &intent.approval_authority_digest,
    )?;
    validate_bounded_identifier(&intent.connector, "connector", MAX_CONNECTOR_BYTES)?;
    if intent.action.is_empty() || intent.action.len() > MAX_ACTION_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "external effect action is empty or unbounded",
        ));
    }
    if intent.action_summary.len() > MAX_SUMMARY_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "external effect action summary is unbounded",
        ));
    }
    if let Some(token) = intent.approval_token.as_ref() {
        if token.schema != CONNECTOR_APPROVAL_TOKEN_SCHEMA
            || token.effect_id != intent.effect_id
            || token.exact_lane_digest != intent.exact_lane_digest
            || token.params_digest != intent.params_digest
            || token.approval_generation != intent.approval_generation
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "external effect approval token binding does not match its intent",
            ));
        }
        validate_digest(&token.token_digest, "approval token digest")?;
        validate_legacy_compatible_authority_binding(
            &token.source_session_key_digest,
            &token.approval_authority_digest,
        )?;
        if (token.source_session_key_digest.is_empty()
            != intent.source_session_key_digest.is_empty())
            || (!token.source_session_key_digest.is_empty()
                && (token.source_session_key_digest != intent.source_session_key_digest
                    || token.approval_authority_digest != intent.approval_authority_digest))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "external effect approval authority binding is inconsistent",
            ));
        }
    }
    Ok(())
}

fn validate_legacy_compatible_authority_binding(
    source_session_key_digest: &str,
    approval_authority_digest: &str,
) -> io::Result<()> {
    if source_session_key_digest.is_empty() && approval_authority_digest.is_empty() {
        return Ok(());
    }
    if source_session_key_digest.is_empty() || approval_authority_digest.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "external effect approval authority binding is partial",
        ));
    }
    validate_digest(source_session_key_digest, "source session key digest")?;
    validate_digest(approval_authority_digest, "approval authority digest")
}

fn validate_digest(value: &str, label: &str) -> io::Result<()> {
    let hex = value.strip_prefix("sha256:").unwrap_or(value);
    if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{label} must be a SHA-256 digest"),
        ));
    }
    Ok(())
}

fn validate_effect_marker(value: &str) -> io::Result<()> {
    let Some(key) = value
        .strip_prefix("<!-- agent-harness-effect:ahx-")
        .and_then(|value| value.strip_suffix(" -->"))
    else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "connector effect marker is not canonical",
        ));
    };
    if key.len() != 64 || !key.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "connector effect marker does not contain an exact effect digest",
        ));
    }
    Ok(())
}

fn validate_bounded_identifier(value: &str, label: &str, max: usize) -> io::Result<()> {
    if value.trim() != value
        || value.is_empty()
        || value.len() > max
        || value.chars().any(char::is_control)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{label} is empty, unbounded, or not normalized"),
        ));
    }
    Ok(())
}

fn first_string(value: &Value, pointers: &[&str]) -> Option<String> {
    pointers.iter().find_map(|pointer| {
        value
            .pointer(pointer)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    })
}

fn sanitize_bounded(value: &str, max: usize, fallback: &str) -> String {
    let flattened = value
        .chars()
        .map(|ch| if ch.is_control() { ' ' } else { ch })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if flattened.is_empty() {
        return fallback.to_string();
    }
    flattened.chars().take(max).collect()
}

fn protected_connector_action(connector: &str, action: &str, summary: &str) -> bool {
    let text = format!("{connector} {action} {summary}").to_ascii_lowercase();
    [
        "gateway restart",
        "gateway stop",
        "live cutover",
        "live rollback",
        "agent-harness restart",
        "agent harness restart",
    ]
    .iter()
    .any(|needle| text.contains(needle))
}

fn sha256_tagged(bytes: &[u8]) -> String {
    format!(
        "sha256:{}",
        hex(digest::digest(&digest::SHA256, bytes).as_ref())
    )
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(HEX[usize::from(*byte >> 4)]));
        out.push(char::from(HEX[usize::from(*byte & 0x0f)]));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "agent-harness-external-effect-{name}-{}",
            std::process::id()
        ))
    }

    fn context() -> ExternalEffectRequestContextV1 {
        ExternalEffectRequestContextV1 {
            exact_lane_digest: format!("sha256:{}", "1".repeat(64)),
            logical_lineage_id: "virtual-session-1".to_string(),
            source_queue_id: "queue-1".to_string(),
            source_session_key_digest: format!("sha256:{}", "3".repeat(64)),
            approval_authority_digest: format!("sha256:{}", "4".repeat(64)),
        }
    }

    fn descriptor() -> McpElicitationDescriptorV1 {
        McpElicitationDescriptorV1 {
            connector: "github".to_string(),
            action: "create_issue".to_string(),
            params_digest: format!("sha256:{}", "2".repeat(64)),
            action_summary: "github/create_issue: create a tracked issue".to_string(),
            mode: "form".to_string(),
        }
    }

    #[test]
    fn missing_authority_persists_one_stable_approval_required_effect() {
        let root = root("missing-authority");
        let harness_home = root.join(".agent-harness");
        let first = begin_external_effect_request(
            &harness_home,
            &context(),
            &descriptor(),
            &ConnectorApprovalPolicyV1::default(),
        )
        .unwrap();
        let second = begin_external_effect_request(
            &harness_home,
            &context(),
            &descriptor(),
            &ConnectorApprovalPolicyV1::default(),
        )
        .unwrap();
        let (first_intent, first_token) = match first {
            ExternalEffectAdmissionV1::NeedsUser { intent, token } => (intent, token),
            other => panic!("unexpected admission {other:?}"),
        };
        let (second_intent, second_token) = match second {
            ExternalEffectAdmissionV1::NeedsUser { intent, token } => (intent, token),
            other => panic!("unexpected admission {other:?}"),
        };
        assert_eq!(first_intent.effect_id, second_intent.effect_id);
        assert_eq!(first_token, second_token);
        assert_eq!(first_intent.state, ExternalEffectStateV1::ApprovalRequired);
        let public_receipt = serde_json::to_string(&first_intent).unwrap();
        assert!(!public_receipt.contains(&first_token));
        let protected_snapshot = fs::read_to_string(
            external_effect_latest_dir(&harness_home)
                .join(format!("{}.json", first_intent.effect_id)),
        )
        .unwrap();
        assert!(protected_snapshot.contains(&first_token));
        let transitions =
            fs::read_to_string(external_effect_transition_file(&harness_home)).unwrap();
        assert_eq!(transitions.lines().count(), 2);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn restart_at_every_effect_state_converges_without_duplicate_submission() {
        struct Fake(ExternalEffectReadbackV1);
        impl ExternalEffectReadbackAdapter for Fake {
            fn readback(
                &self,
                _intent: &ExternalEffectIntentV1,
            ) -> io::Result<ExternalEffectReadbackV1> {
                Ok(self.0)
            }
        }

        let root = root("every-state-restart");
        let harness_home = root.join(".agent-harness");
        let request_context = context();
        let request_descriptor = descriptor();
        let now = current_log_time_ms().unwrap();
        let requested = ExternalEffectIntentV1 {
            schema: EXTERNAL_EFFECT_INTENT_SCHEMA.to_string(),
            effect_id: effect_id(&request_context, &request_descriptor),
            exact_lane_digest: request_context.exact_lane_digest.clone(),
            logical_lineage_id: request_context.logical_lineage_id.clone(),
            source_queue_id: request_context.source_queue_id.clone(),
            source_session_key_digest: request_context.source_session_key_digest.clone(),
            approval_authority_digest: request_context.approval_authority_digest.clone(),
            connector: request_descriptor.connector.clone(),
            action: request_descriptor.action.clone(),
            params_digest: request_descriptor.params_digest.clone(),
            approval_generation: 1,
            state: ExternalEffectStateV1::Requested,
            action_summary: request_descriptor.action_summary.clone(),
            approval_token: None,
            requested_at_ms: now,
            updated_at_ms: now,
            reason: Some("synthetic crash after requested snapshot".to_string()),
            expiry_resolution_id: None,
            expiry_notice_id: None,
        };
        persist_transition(&harness_home, None, &requested).unwrap();
        assert_eq!(
            load_external_effect_intent(&harness_home, &requested.effect_id)
                .unwrap()
                .unwrap()
                .state,
            ExternalEffectStateV1::Requested
        );

        let admission = begin_external_effect_request(
            &harness_home,
            &request_context,
            &request_descriptor,
            &ConnectorApprovalPolicyV1::default(),
        )
        .unwrap();
        let (effect_id, token) = match admission {
            ExternalEffectAdmissionV1::NeedsUser { intent, token } => {
                assert_eq!(intent.state, ExternalEffectStateV1::ApprovalRequired);
                (intent.effect_id, token)
            }
            other => panic!("requested recovery did not park safely: {other:?}"),
        };
        let replayed_token = match begin_external_effect_request(
            &harness_home,
            &request_context,
            &request_descriptor,
            &ConnectorApprovalPolicyV1::default(),
        )
        .unwrap()
        {
            ExternalEffectAdmissionV1::NeedsUser { token, .. } => token,
            other => panic!("approval-required restart changed disposition: {other:?}"),
        };
        assert_eq!(replayed_token, token);

        let approved = resolve_external_effect_approval(
            &harness_home,
            &token,
            &request_context.exact_lane_digest,
            ExternalEffectApprovalDecisionV1::Approve,
        )
        .unwrap();
        assert_eq!(approved.state, ExternalEffectStateV1::Approved);
        assert!(matches!(
            begin_external_effect_request(
                &harness_home,
                &request_context,
                &request_descriptor,
                &ConnectorApprovalPolicyV1::default(),
            )
            .unwrap(),
            ExternalEffectAdmissionV1::Authorized(_)
        ));

        transition_external_effect(
            &harness_home,
            &effect_id,
            ExternalEffectStateV1::Submitted,
            "synthetic connector submission",
        )
        .unwrap();
        assert!(matches!(
            begin_external_effect_request(
                &harness_home,
                &request_context,
                &request_descriptor,
                &ConnectorApprovalPolicyV1::default(),
            )
            .unwrap(),
            ExternalEffectAdmissionV1::Ambiguous(_)
        ));
        let ambiguous = reconcile_external_effect(
            &harness_home,
            &effect_id,
            &Fake(ExternalEffectReadbackV1::Unprovable),
        )
        .unwrap();
        assert_eq!(ambiguous.state, ExternalEffectStateV1::Ambiguous);
        assert!(matches!(
            begin_external_effect_request(
                &harness_home,
                &request_context,
                &request_descriptor,
                &ConnectorApprovalPolicyV1::default(),
            )
            .unwrap(),
            ExternalEffectAdmissionV1::Ambiguous(_)
        ));

        let confirmed = reconcile_external_effect(
            &harness_home,
            &effect_id,
            &Fake(ExternalEffectReadbackV1::Present),
        )
        .unwrap();
        assert_eq!(confirmed.state, ExternalEffectStateV1::Confirmed);
        assert!(matches!(
            begin_external_effect_request(
                &harness_home,
                &request_context,
                &request_descriptor,
                &ConnectorApprovalPolicyV1::default(),
            )
            .unwrap(),
            ExternalEffectAdmissionV1::AlreadyConfirmed(_)
        ));

        let transitions = fs::read_to_string(external_effect_transition_file(&harness_home))
            .unwrap()
            .lines()
            .map(|line| {
                serde_json::from_str::<ExternalEffectTransitionV1>(line)
                    .unwrap()
                    .to_state
            })
            .collect::<Vec<_>>();
        assert_eq!(
            transitions,
            vec![
                ExternalEffectStateV1::Requested,
                ExternalEffectStateV1::ApprovalRequired,
                ExternalEffectStateV1::Approved,
                ExternalEffectStateV1::Submitted,
                ExternalEffectStateV1::Ambiguous,
                ExternalEffectStateV1::Confirmed,
            ]
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn action_token_is_exact_lane_expiring_and_single_use() {
        let root = root("single-use");
        let harness_home = root.join(".agent-harness");
        let admission = begin_external_effect_request(
            &harness_home,
            &context(),
            &descriptor(),
            &ConnectorApprovalPolicyV1::default(),
        )
        .unwrap();
        let token = match admission {
            ExternalEffectAdmissionV1::NeedsUser { token, .. } => token,
            other => panic!("unexpected admission {other:?}"),
        };
        let wrong_lane = format!("sha256:{}", "9".repeat(64));
        assert!(
            resolve_external_effect_approval(
                &harness_home,
                &token,
                &wrong_lane,
                ExternalEffectApprovalDecisionV1::Approve,
            )
            .is_err()
        );
        let approved = resolve_external_effect_approval(
            &harness_home,
            &token,
            &context().exact_lane_digest,
            ExternalEffectApprovalDecisionV1::Approve,
        )
        .unwrap();
        assert_eq!(approved.state, ExternalEffectStateV1::Approved);
        let repeated = resolve_external_effect_approval(
            &harness_home,
            &token,
            &context().exact_lane_digest,
            ExternalEffectApprovalDecisionV1::Approve,
        )
        .unwrap();
        assert_eq!(repeated.state, ExternalEffectStateV1::Approved);
        assert!(
            resolve_external_effect_approval(
                &harness_home,
                &token,
                &context().exact_lane_digest,
                ExternalEffectApprovalDecisionV1::Deny,
            )
            .is_err()
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn native_channel_action_requires_full_authority_binding_and_consumes_once() {
        let root = root("native-authority-binding");
        let harness_home = root.join("runtime-home");
        let request_context = context();
        let (intent, token) = match begin_external_effect_request(
            &harness_home,
            &request_context,
            &descriptor(),
            &ConnectorApprovalPolicyV1::default(),
        )
        .unwrap()
        {
            ExternalEffectAdmissionV1::NeedsUser { intent, token } => (intent, token),
            other => panic!("unexpected admission {other:?}"),
        };
        let action = ChannelInboundActionV1::new(
            "provider-event-1",
            Some("provider-message-1".to_string()),
            &intent.effect_id,
            intent.approval_generation,
            ChannelApprovalDecisionV1::Approve,
            &request_context.source_session_key_digest,
            &request_context.approval_authority_digest,
            &token,
        )
        .unwrap();
        let approved = resolve_external_effect_channel_action(
            &harness_home,
            &action,
            &request_context.exact_lane_digest,
        )
        .unwrap();
        assert_eq!(approved.state, ExternalEffectStateV1::Approved);
        assert_eq!(
            resolve_external_effect_channel_action(
                &harness_home,
                &action,
                &request_context.exact_lane_digest,
            )
            .unwrap()
            .state,
            ExternalEffectStateV1::Approved
        );

        let wrong_authority = ChannelInboundActionV1::new(
            "provider-event-2",
            None,
            &intent.effect_id,
            intent.approval_generation,
            ChannelApprovalDecisionV1::Approve,
            &request_context.source_session_key_digest,
            format!("sha256:{}", "9".repeat(64)),
            &token,
        )
        .unwrap();
        assert_eq!(
            resolve_external_effect_channel_action(
                &harness_home,
                &wrong_authority,
                &request_context.exact_lane_digest,
            )
            .unwrap_err()
            .kind(),
            io::ErrorKind::PermissionDenied
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn public_channel_action_resolves_through_protected_index_without_bearer_input() {
        let root = root("public-action-index");
        let harness_home = root.join("runtime-home");
        let request_context = context();
        let intent = match begin_external_effect_request(
            &harness_home,
            &request_context,
            &descriptor(),
            &ConnectorApprovalPolicyV1::default(),
        )
        .unwrap()
        {
            ExternalEffectAdmissionV1::NeedsUser { intent, .. } => intent,
            other => panic!("unexpected admission {other:?}"),
        };
        let public_action_id = public_approval_action_id(
            &intent.effect_id,
            intent.approval_generation,
            ChannelApprovalDecisionV1::Approve,
        )
        .unwrap();
        let index_file = external_effect_public_action_index_file(&harness_home, &public_action_id);
        let index_text = fs::read_to_string(&index_file).unwrap();
        assert!(index_text.contains(&public_action_id));
        assert!(!index_text.contains("ahx1_"));

        let mismatch = resolve_external_effect_public_channel_action(
            &harness_home,
            "provider-event-mismatch",
            None,
            &public_action_id,
            &request_context.exact_lane_digest,
            &request_context.source_session_key_digest,
            Some(ChannelApprovalDecisionV1::Deny),
        )
        .unwrap_err();
        assert_eq!(mismatch.kind(), io::ErrorKind::PermissionDenied);
        assert_eq!(
            load_external_effect_intent(&harness_home, &intent.effect_id)
                .unwrap()
                .unwrap()
                .state,
            ExternalEffectStateV1::ApprovalRequired
        );

        let approved = resolve_external_effect_public_channel_action(
            &harness_home,
            "provider-event-approve",
            Some("provider-message-1".to_string()),
            &public_action_id,
            &request_context.exact_lane_digest,
            &request_context.source_session_key_digest,
            Some(ChannelApprovalDecisionV1::Approve),
        )
        .unwrap();
        assert_eq!(approved.state, ExternalEffectStateV1::Approved);
        let replay = resolve_external_effect_public_channel_action(
            &harness_home,
            "provider-event-approve-replay",
            Some("provider-message-1".to_string()),
            &public_action_id,
            &request_context.exact_lane_digest,
            &request_context.source_session_key_digest,
            None,
        )
        .unwrap();
        assert_eq!(replay.state, ExternalEffectStateV1::Approved);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn legacy_approval_token_remains_resolvable_only_through_text_path() {
        let root = root("legacy-token-continuity");
        let harness_home = root.join("runtime-home");
        let request_context = context();
        let (intent, token) = match begin_external_effect_request(
            &harness_home,
            &request_context,
            &descriptor(),
            &ConnectorApprovalPolicyV1::default(),
        )
        .unwrap()
        {
            ExternalEffectAdmissionV1::NeedsUser { intent, token } => (intent, token),
            other => panic!("unexpected admission {other:?}"),
        };
        let snapshot =
            external_effect_latest_dir(&harness_home).join(format!("{}.json", intent.effect_id));
        let mut value: Value = serde_json::from_slice(&fs::read(&snapshot).unwrap()).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .remove("sourceSessionKeyDigest");
        value
            .as_object_mut()
            .unwrap()
            .remove("approvalAuthorityDigest");
        value["approvalToken"]
            .as_object_mut()
            .unwrap()
            .remove("sourceSessionKeyDigest");
        value["approvalToken"]
            .as_object_mut()
            .unwrap()
            .remove("approvalAuthorityDigest");
        write_json_atomic(&snapshot, &value).unwrap();

        let native = ChannelInboundActionV1::new(
            "provider-event-legacy",
            None,
            &intent.effect_id,
            intent.approval_generation,
            ChannelApprovalDecisionV1::Approve,
            &request_context.source_session_key_digest,
            &request_context.approval_authority_digest,
            &token,
        )
        .unwrap();
        assert_eq!(
            resolve_external_effect_channel_action(
                &harness_home,
                &native,
                &request_context.exact_lane_digest,
            )
            .unwrap_err()
            .kind(),
            io::ErrorKind::PermissionDenied
        );
        assert_eq!(
            resolve_external_effect_approval(
                &harness_home,
                &token,
                &request_context.exact_lane_digest,
                ExternalEffectApprovalDecisionV1::Approve,
            )
            .unwrap()
            .state,
            ExternalEffectStateV1::Approved
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn external_effect_expiry_reconciliation_is_bounded_and_idempotent() {
        let root = root("bounded-expiry");
        let harness_home = root.join("runtime-home");
        let mut expected_effect_ids = Vec::new();
        for digit in ['5', '6'] {
            let mut request_descriptor = descriptor();
            request_descriptor.params_digest = format!("sha256:{}", digit.to_string().repeat(64));
            let intent = match begin_external_effect_request(
                &harness_home,
                &context(),
                &request_descriptor,
                &ConnectorApprovalPolicyV1::default(),
            )
            .unwrap()
            {
                ExternalEffectAdmissionV1::NeedsUser { intent, .. } => intent,
                other => panic!("unexpected admission {other:?}"),
            };
            let snapshot = external_effect_latest_dir(&harness_home)
                .join(format!("{}.json", intent.effect_id));
            let mut value: Value = serde_json::from_slice(&fs::read(&snapshot).unwrap()).unwrap();
            value["approvalToken"]["expiresAtMs"] = Value::from(100);
            write_json_atomic(&snapshot, &value).unwrap();
            expected_effect_ids.push(intent.effect_id);
        }
        expected_effect_ids.sort_unstable();

        let first = reconcile_expired_external_effect_approvals(
            &harness_home,
            &ExternalEffectExpiryReconcileRequestV1 {
                now_ms: 100,
                max_rows: 1,
                after_effect_id: None,
            },
        )
        .unwrap();
        assert_eq!(first.scanned_rows, 1);
        assert!(!first.exhausted);
        assert_eq!(first.resolutions.len(), 1);
        assert!(first.resolutions[0].newly_transitioned);
        let second = reconcile_expired_external_effect_approvals(
            &harness_home,
            &ExternalEffectExpiryReconcileRequestV1 {
                now_ms: 100,
                max_rows: 1,
                after_effect_id: first.next_after_effect_id,
            },
        )
        .unwrap();
        assert!(second.exhausted);
        assert_eq!(second.resolutions.len(), 1);
        assert!(second.resolutions[0].newly_transitioned);

        let replay = reconcile_expired_external_effect_approvals(
            &harness_home,
            &ExternalEffectExpiryReconcileRequestV1 {
                now_ms: 101,
                max_rows: MAX_EXTERNAL_EFFECT_EXPIRY_SCAN_ROWS,
                after_effect_id: None,
            },
        )
        .unwrap();
        assert_eq!(replay.resolutions.len(), 2);
        assert!(
            replay
                .resolutions
                .iter()
                .all(|resolution| !resolution.newly_transitioned)
        );
        assert_eq!(
            replay
                .resolutions
                .iter()
                .map(|resolution| resolution.effect_id.clone())
                .collect::<Vec<_>>(),
            expected_effect_ids
        );
        let transitions =
            fs::read_to_string(external_effect_transition_file(&harness_home)).unwrap();
        for resolution in replay.resolutions {
            assert!(transitions.contains(&resolution.resolution_id));
            assert!(transitions.contains(&resolution.notice_id));
        }
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn external_effect_expiry_and_native_decision_have_one_terminal_winner() {
        use std::sync::{Arc, Barrier};

        let root = root("expiry-action-race");
        let harness_home = root.join("runtime-home");
        let request_context = context();
        let (intent, token) = match begin_external_effect_request(
            &harness_home,
            &request_context,
            &descriptor(),
            &ConnectorApprovalPolicyV1::default(),
        )
        .unwrap()
        {
            ExternalEffectAdmissionV1::NeedsUser { intent, token } => (intent, token),
            other => panic!("unexpected admission {other:?}"),
        };
        let action = ChannelInboundActionV1::new(
            "provider-event-race",
            None,
            &intent.effect_id,
            intent.approval_generation,
            ChannelApprovalDecisionV1::Approve,
            &request_context.source_session_key_digest,
            &request_context.approval_authority_digest,
            token,
        )
        .unwrap();
        let barrier = Arc::new(Barrier::new(3));
        let action_barrier = Arc::clone(&barrier);
        let action_home = harness_home.clone();
        let lane = request_context.exact_lane_digest.clone();
        let action_thread = std::thread::spawn(move || {
            action_barrier.wait();
            resolve_external_effect_channel_action(&action_home, &action, &lane)
                .map(|resolved| resolved.state)
                .map_err(|error| error.kind())
        });
        let expiry_barrier = Arc::clone(&barrier);
        let expiry_home = harness_home.clone();
        let expiry_thread = std::thread::spawn(move || {
            expiry_barrier.wait();
            reconcile_expired_external_effect_approvals(
                &expiry_home,
                &ExternalEffectExpiryReconcileRequestV1 {
                    now_ms: i64::MAX,
                    max_rows: MAX_EXTERNAL_EFFECT_EXPIRY_SCAN_ROWS,
                    after_effect_id: None,
                },
            )
        });
        barrier.wait();
        let action_result = action_thread.join().unwrap();
        let expiry_result = expiry_thread.join().unwrap().unwrap();
        let final_intent = load_external_effect_intent(&harness_home, &intent.effect_id)
            .unwrap()
            .unwrap();
        assert!(matches!(
            final_intent.state,
            ExternalEffectStateV1::Approved | ExternalEffectStateV1::Denied
        ));
        match final_intent.state {
            ExternalEffectStateV1::Approved => {
                assert_eq!(action_result.unwrap(), ExternalEffectStateV1::Approved);
                assert!(expiry_result.resolutions.is_empty());
            }
            ExternalEffectStateV1::Denied => {
                assert_eq!(action_result.unwrap_err(), io::ErrorKind::TimedOut);
                assert_eq!(expiry_result.resolutions.len(), 1);
            }
            _ => unreachable!(),
        }
        let terminal_transition_count =
            fs::read_to_string(external_effect_transition_file(&harness_home))
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str::<ExternalEffectTransitionV1>(line).unwrap())
                .filter(|transition| {
                    transition.from_state == Some(ExternalEffectStateV1::ApprovalRequired)
                        && matches!(
                            transition.to_state,
                            ExternalEffectStateV1::Approved | ExternalEffectStateV1::Denied
                        )
                })
                .count();
        assert_eq!(terminal_transition_count, 1);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn denied_connector_action_is_terminal_for_its_generation() {
        let root = root("denied");
        let harness_home = root.join(".agent-harness");
        let policy = ConnectorApprovalPolicyV1 {
            default: ConnectorApprovalModeV1::Deny,
            rules: Vec::new(),
        };
        let first =
            begin_external_effect_request(&harness_home, &context(), &descriptor(), &policy)
                .unwrap();
        let effect_id = match first {
            ExternalEffectAdmissionV1::Denied(intent) => {
                assert_eq!(intent.state, ExternalEffectStateV1::Denied);
                intent.effect_id
            }
            other => panic!("unexpected admission {other:?}"),
        };
        let second = begin_external_effect_request(
            &harness_home,
            &context(),
            &descriptor(),
            &ConnectorApprovalPolicyV1::default(),
        )
        .unwrap();
        match second {
            ExternalEffectAdmissionV1::Denied(intent) => assert_eq!(intent.effect_id, effect_id),
            other => panic!("denied generation was not terminal: {other:?}"),
        }
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn submitted_effect_requires_readback_before_resubmission() {
        struct Fake(ExternalEffectReadbackV1);
        impl ExternalEffectReadbackAdapter for Fake {
            fn readback(
                &self,
                _intent: &ExternalEffectIntentV1,
            ) -> io::Result<ExternalEffectReadbackV1> {
                Ok(self.0)
            }
        }

        let root = root("readback");
        let harness_home = root.join(".agent-harness");
        let admission = begin_external_effect_request(
            &harness_home,
            &context(),
            &descriptor(),
            &ConnectorApprovalPolicyV1::default(),
        )
        .unwrap();
        let (effect_id, token) = match admission {
            ExternalEffectAdmissionV1::NeedsUser { intent, token } => (intent.effect_id, token),
            other => panic!("unexpected admission {other:?}"),
        };
        resolve_external_effect_approval(
            &harness_home,
            &token,
            &context().exact_lane_digest,
            ExternalEffectApprovalDecisionV1::Approve,
        )
        .unwrap();
        transition_external_effect(
            &harness_home,
            &effect_id,
            ExternalEffectStateV1::Submitted,
            "protocol acceptance written",
        )
        .unwrap();
        let absent = reconcile_external_effect(
            &harness_home,
            &effect_id,
            &Fake(ExternalEffectReadbackV1::Absent),
        )
        .unwrap();
        assert_eq!(absent.state, ExternalEffectStateV1::Approved);
        transition_external_effect(
            &harness_home,
            &effect_id,
            ExternalEffectStateV1::Submitted,
            "one bounded resubmission after proven absence",
        )
        .unwrap();
        let ambiguous = reconcile_external_effect(
            &harness_home,
            &effect_id,
            &Fake(ExternalEffectReadbackV1::Unprovable),
        )
        .unwrap();
        assert_eq!(ambiguous.state, ExternalEffectStateV1::Ambiguous);
        let confirmed = reconcile_external_effect(
            &harness_home,
            &effect_id,
            &Fake(ExternalEffectReadbackV1::Present),
        )
        .unwrap();
        assert_eq!(confirmed.state, ExternalEffectStateV1::Confirmed);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn connector_specific_readback_adapters_are_exactly_bound_and_fail_closed() {
        let root = root("connector-readback-adapters");
        let harness_home = root.join(".agent-harness");
        let admission = begin_external_effect_request(
            &harness_home,
            &context(),
            &descriptor(),
            &ConnectorApprovalPolicyV1::default(),
        )
        .unwrap();
        let intent = match admission {
            ExternalEffectAdmissionV1::NeedsUser { intent, .. } => intent,
            other => panic!("unexpected admission {other:?}"),
        };
        let marker = github_issue_effect_marker(&intent).unwrap();
        assert_eq!(
            marker,
            format!(
                "<!-- agent-harness-effect:{} -->",
                external_effect_idempotency_key(&intent)
            )
        );

        let github_present = GithubIssueReadbackAdapterV1::new(GithubIssueReadbackEvidenceV1 {
            schema: GITHUB_ISSUE_READBACK_EVIDENCE_SCHEMA.to_string(),
            params_digest: intent.params_digest.clone(),
            query_complete: true,
            observed_effect_markers: vec![marker],
        })
        .unwrap();
        assert_eq!(
            github_present.readback(&intent).unwrap(),
            ExternalEffectReadbackV1::Present
        );
        let github_absent = GithubIssueReadbackAdapterV1::new(GithubIssueReadbackEvidenceV1 {
            schema: GITHUB_ISSUE_READBACK_EVIDENCE_SCHEMA.to_string(),
            params_digest: intent.params_digest.clone(),
            query_complete: true,
            observed_effect_markers: Vec::new(),
        })
        .unwrap();
        assert_eq!(
            github_absent.readback(&intent).unwrap(),
            ExternalEffectReadbackV1::Absent
        );
        let github_partial = GithubIssueReadbackAdapterV1::new(GithubIssueReadbackEvidenceV1 {
            schema: GITHUB_ISSUE_READBACK_EVIDENCE_SCHEMA.to_string(),
            params_digest: intent.params_digest.clone(),
            query_complete: false,
            observed_effect_markers: Vec::new(),
        })
        .unwrap();
        assert_eq!(
            github_partial.readback(&intent).unwrap(),
            ExternalEffectReadbackV1::Unprovable
        );

        for (state, expected) in [
            (
                ProviderIdempotencyReadbackStateV1::Present,
                ExternalEffectReadbackV1::Present,
            ),
            (
                ProviderIdempotencyReadbackStateV1::Absent,
                ExternalEffectReadbackV1::Absent,
            ),
            (
                ProviderIdempotencyReadbackStateV1::Unknown,
                ExternalEffectReadbackV1::Unprovable,
            ),
        ] {
            let adapter =
                ProviderIdempotencyReadbackAdapterV1::new(ProviderIdempotencyReadbackEvidenceV1 {
                    schema: PROVIDER_IDEMPOTENCY_READBACK_EVIDENCE_SCHEMA.to_string(),
                    connector: intent.connector.clone(),
                    action: intent.action.clone(),
                    params_digest: intent.params_digest.clone(),
                    idempotency_key: external_effect_idempotency_key(&intent),
                    state,
                })
                .unwrap();
            assert_eq!(adapter.readback(&intent).unwrap(), expected);
        }
        let wrong_key =
            ProviderIdempotencyReadbackAdapterV1::new(ProviderIdempotencyReadbackEvidenceV1 {
                schema: PROVIDER_IDEMPOTENCY_READBACK_EVIDENCE_SCHEMA.to_string(),
                connector: intent.connector.clone(),
                action: intent.action.clone(),
                params_digest: intent.params_digest.clone(),
                idempotency_key: format!("ahx-{}", "f".repeat(64)),
                state: ProviderIdempotencyReadbackStateV1::Absent,
            })
            .unwrap();
        assert_eq!(
            wrong_key.readback(&intent).unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn stop_or_expiry_fences_pending_capabilities_without_cross_lane_effects() {
        let root = root("fencing");
        let harness_home = root.join(".agent-harness");
        let admission = begin_external_effect_request(
            &harness_home,
            &context(),
            &descriptor(),
            &ConnectorApprovalPolicyV1::default(),
        )
        .unwrap();
        let (intent, token) = match admission {
            ExternalEffectAdmissionV1::NeedsUser { intent, token } => (intent, token),
            other => panic!("unexpected admission {other:?}"),
        };
        assert!(
            fence_external_effects_for_lane(
                &harness_home,
                &format!("sha256:{}", "9".repeat(64)),
                "another lane stopped",
            )
            .unwrap()
            .is_empty()
        );
        let fenced = fence_external_effects_for_lane(
            &harness_home,
            &context().exact_lane_digest,
            "operator stop",
        )
        .unwrap();
        assert_eq!(fenced.len(), 1);
        assert_eq!(fenced[0].state, ExternalEffectStateV1::Denied);
        assert!(
            resolve_external_effect_approval(
                &harness_home,
                &token,
                &context().exact_lane_digest,
                ExternalEffectApprovalDecisionV1::Approve,
            )
            .is_err()
        );

        let mut second_descriptor = descriptor();
        second_descriptor.params_digest = format!("sha256:{}", "3".repeat(64));
        let second = begin_external_effect_request(
            &harness_home,
            &context(),
            &second_descriptor,
            &ConnectorApprovalPolicyV1::default(),
        )
        .unwrap();
        let (second_intent, second_token) = match second {
            ExternalEffectAdmissionV1::NeedsUser { intent, token } => (intent, token),
            other => panic!("unexpected admission {other:?}"),
        };
        let snapshot = external_effect_latest_dir(&harness_home)
            .join(format!("{}.json", second_intent.effect_id));
        let mut value: Value = serde_json::from_slice(&fs::read(&snapshot).unwrap()).unwrap();
        value["approvalToken"]["expiresAtMs"] = Value::from(0);
        write_json_atomic(&snapshot, &value).unwrap();
        let error = resolve_external_effect_approval(
            &harness_home,
            &second_token,
            &context().exact_lane_digest,
            ExternalEffectApprovalDecisionV1::Approve,
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert_eq!(
            load_external_effect_intent(&harness_home, &second_intent.effect_id)
                .unwrap()
                .unwrap()
                .state,
            ExternalEffectStateV1::Denied
        );
        assert_eq!(intent.state, ExternalEffectStateV1::ApprovalRequired);
        let _ = fs::remove_dir_all(root);
    }
}
