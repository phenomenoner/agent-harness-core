use std::fmt;

use ring::digest::{SHA256, digest};
use serde::{Deserialize, Serialize};

pub const CHANNEL_APPROVAL_PROMPT_SCHEMA: &str = "agent-harness.channel-approval-prompt.v1";
pub const CHANNEL_INBOUND_ACTION_SCHEMA: &str = "agent-harness.channel-inbound-action.v1";
pub const CHANNEL_INBOUND_ACTION_EVIDENCE_SCHEMA: &str =
    "agent-harness.channel-inbound-action-evidence.v1";

const PUBLIC_ACTION_ID_PREFIX: &str = "ahpa1_";
const APPROVAL_BEARER_PREFIX: &str = "ahx1_";
const MAX_EFFECT_ID_BYTES: usize = 160;
const MAX_ACTION_SUMMARY_BYTES: usize = 240;
const MAX_ACTION_LABEL_BYTES: usize = 48;
const MAX_PROVIDER_ID_BYTES: usize = 192;
const MAX_CALLBACK_TOKEN_BYTES: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ChannelApprovalDecisionV1 {
    Approve,
    Deny,
}

impl ChannelApprovalDecisionV1 {
    fn stable_name(self) -> &'static str {
        match self {
            Self::Approve => "approve",
            Self::Deny => "deny",
        }
    }

    fn default_label(self) -> &'static str {
        match self {
            Self::Approve => "Approve",
            Self::Deny => "Deny",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ChannelApprovalActionV1 {
    pub public_action_id: String,
    pub decision: ChannelApprovalDecisionV1,
    pub label: String,
}

impl ChannelApprovalActionV1 {
    pub fn new(
        effect_id: &str,
        approval_generation: u64,
        decision: ChannelApprovalDecisionV1,
    ) -> Result<Self, ChannelActionError> {
        validate_effect_binding(effect_id, approval_generation)?;
        Ok(Self {
            public_action_id: public_approval_action_id(effect_id, approval_generation, decision)?,
            decision,
            label: decision.default_label().to_string(),
        })
    }

    fn validate_for(
        &self,
        effect_id: &str,
        approval_generation: u64,
    ) -> Result<(), ChannelActionError> {
        validate_bounded_text(&self.label, "action label", MAX_ACTION_LABEL_BYTES)?;
        validate_public_approval_action_id(
            &self.public_action_id,
            effect_id,
            approval_generation,
            self.decision,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ChannelApprovalPromptV1 {
    pub schema: String,
    pub effect_id: String,
    pub approval_generation: u64,
    pub source_session_key_digest: String,
    pub source_authority_digest: String,
    pub action_summary: String,
    pub expires_at_ms: i64,
    pub actions: Vec<ChannelApprovalActionV1>,
}

impl ChannelApprovalPromptV1 {
    pub fn new(
        effect_id: impl Into<String>,
        approval_generation: u64,
        source_session_key_digest: impl Into<String>,
        source_authority_digest: impl Into<String>,
        action_summary: impl Into<String>,
        expires_at_ms: i64,
    ) -> Result<Self, ChannelActionError> {
        let effect_id = effect_id.into();
        let prompt = Self {
            schema: CHANNEL_APPROVAL_PROMPT_SCHEMA.to_string(),
            actions: vec![
                ChannelApprovalActionV1::new(
                    &effect_id,
                    approval_generation,
                    ChannelApprovalDecisionV1::Approve,
                )?,
                ChannelApprovalActionV1::new(
                    &effect_id,
                    approval_generation,
                    ChannelApprovalDecisionV1::Deny,
                )?,
            ],
            effect_id,
            approval_generation,
            source_session_key_digest: source_session_key_digest.into(),
            source_authority_digest: source_authority_digest.into(),
            action_summary: action_summary.into(),
            expires_at_ms,
        };
        prompt.validate()?;
        Ok(prompt)
    }

    /// Validates durable prompt structure without applying a wall-clock policy.
    pub fn validate(&self) -> Result<(), ChannelActionError> {
        if self.schema != CHANNEL_APPROVAL_PROMPT_SCHEMA {
            return Err(ChannelActionError::UnsupportedSchema);
        }
        validate_effect_binding(&self.effect_id, self.approval_generation)?;
        validate_sha256_digest(&self.source_session_key_digest, "source session key digest")?;
        validate_sha256_digest(&self.source_authority_digest, "source authority digest")?;
        validate_bounded_text(
            &self.action_summary,
            "action summary",
            MAX_ACTION_SUMMARY_BYTES,
        )?;
        if self.expires_at_ms <= 0 {
            return Err(ChannelActionError::InvalidField("expiresAtMs"));
        }
        if self.actions.len() != 2 {
            return Err(ChannelActionError::InvalidActions);
        }
        let mut approve_count = 0;
        let mut deny_count = 0;
        for action in &self.actions {
            action.validate_for(&self.effect_id, self.approval_generation)?;
            match action.decision {
                ChannelApprovalDecisionV1::Approve => approve_count += 1,
                ChannelApprovalDecisionV1::Deny => deny_count += 1,
            }
        }
        if approve_count != 1 || deny_count != 1 {
            return Err(ChannelActionError::InvalidActions);
        }
        Ok(())
    }

    /// Applies the expiry gate after structural validation. Equality is expired.
    pub fn validate_at(&self, now_ms: i64) -> Result<(), ChannelActionError> {
        self.validate()?;
        if now_ms >= self.expires_at_ms {
            return Err(ChannelActionError::Expired);
        }
        Ok(())
    }
}

/// Native provider input containing bearer material only in memory.
///
/// This type intentionally implements neither `Serialize` nor `Deserialize`.
/// Its custom `Debug` implementation also omits the raw callback token.
#[derive(Clone, PartialEq, Eq)]
pub struct ChannelInboundActionV1 {
    pub schema: String,
    pub provider_event_id: String,
    pub provider_message_id: Option<String>,
    pub effect_id: String,
    pub approval_generation: u64,
    pub decision: ChannelApprovalDecisionV1,
    pub source_session_key_digest: String,
    pub source_authority_digest: String,
    callback_token: CallbackToken,
}

impl fmt::Debug for ChannelInboundActionV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChannelInboundActionV1")
            .field("schema", &self.schema)
            .field("provider_event_id", &self.provider_event_id)
            .field("provider_message_id", &self.provider_message_id)
            .field("effect_id", &self.effect_id)
            .field("approval_generation", &self.approval_generation)
            .field("decision", &self.decision)
            .field("source_session_key_digest", &self.source_session_key_digest)
            .field("source_authority_digest", &self.source_authority_digest)
            .field("callback_token", &"[REDACTED]")
            .finish()
    }
}

impl ChannelInboundActionV1 {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        provider_event_id: impl Into<String>,
        provider_message_id: Option<String>,
        effect_id: impl Into<String>,
        approval_generation: u64,
        decision: ChannelApprovalDecisionV1,
        source_session_key_digest: impl Into<String>,
        source_authority_digest: impl Into<String>,
        callback_token: impl Into<String>,
    ) -> Result<Self, ChannelActionError> {
        let action = Self {
            schema: CHANNEL_INBOUND_ACTION_SCHEMA.to_string(),
            provider_event_id: provider_event_id.into(),
            provider_message_id,
            effect_id: effect_id.into(),
            approval_generation,
            decision,
            source_session_key_digest: source_session_key_digest.into(),
            source_authority_digest: source_authority_digest.into(),
            callback_token: CallbackToken(callback_token.into()),
        };
        action.validate()?;
        Ok(action)
    }

    pub fn validate(&self) -> Result<(), ChannelActionError> {
        if self.schema != CHANNEL_INBOUND_ACTION_SCHEMA {
            return Err(ChannelActionError::UnsupportedSchema);
        }
        validate_bounded_text(
            &self.provider_event_id,
            "provider event ID",
            MAX_PROVIDER_ID_BYTES,
        )?;
        if let Some(provider_message_id) = self.provider_message_id.as_deref() {
            validate_bounded_text(
                provider_message_id,
                "provider message ID",
                MAX_PROVIDER_ID_BYTES,
            )?;
        }
        validate_effect_binding(&self.effect_id, self.approval_generation)?;
        validate_sha256_digest(&self.source_session_key_digest, "source session key digest")?;
        validate_sha256_digest(&self.source_authority_digest, "source authority digest")?;
        validate_callback_token(&self.callback_token.0)
    }

    /// Exposes the bearer only to the shared resolver after ingress validation.
    pub fn expose_callback_token(&self) -> &str {
        &self.callback_token.0
    }

    pub fn redacted_evidence(&self) -> Result<ChannelInboundActionEvidenceV1, ChannelActionError> {
        self.validate()?;
        Ok(ChannelInboundActionEvidenceV1 {
            schema: CHANNEL_INBOUND_ACTION_EVIDENCE_SCHEMA.to_string(),
            provider_event_id: self.provider_event_id.clone(),
            provider_message_id: self.provider_message_id.clone(),
            effect_id: self.effect_id.clone(),
            approval_generation: self.approval_generation,
            decision: self.decision,
            public_action_id: public_approval_action_id(
                &self.effect_id,
                self.approval_generation,
                self.decision,
            )?,
            callback_token_digest: sha256_tagged(self.callback_token.0.as_bytes()),
            source_session_key_digest: self.source_session_key_digest.clone(),
            source_authority_digest: self.source_authority_digest.clone(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ChannelInboundActionEvidenceV1 {
    pub schema: String,
    pub provider_event_id: String,
    pub provider_message_id: Option<String>,
    pub effect_id: String,
    pub approval_generation: u64,
    pub decision: ChannelApprovalDecisionV1,
    pub public_action_id: String,
    pub callback_token_digest: String,
    pub source_session_key_digest: String,
    pub source_authority_digest: String,
}

#[derive(Clone, PartialEq, Eq)]
struct CallbackToken(String);

impl fmt::Debug for CallbackToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[REDACTED]")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelActionError {
    UnsupportedSchema,
    InvalidField(&'static str),
    InvalidActions,
    Expired,
}

impl fmt::Display for ChannelActionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedSchema => formatter.write_str("unsupported channel action schema"),
            Self::InvalidField(field) => write!(formatter, "invalid channel action {field}"),
            Self::InvalidActions => formatter.write_str("invalid channel approval action set"),
            Self::Expired => formatter.write_str("channel approval prompt is expired"),
        }
    }
}

impl std::error::Error for ChannelActionError {}

pub fn public_approval_action_id(
    effect_id: &str,
    approval_generation: u64,
    decision: ChannelApprovalDecisionV1,
) -> Result<String, ChannelActionError> {
    validate_effect_binding(effect_id, approval_generation)?;
    let canonical = format!(
        "channel-approval-action.v1\0{}\0{}\0{}",
        effect_id,
        approval_generation,
        decision.stable_name()
    );
    let digest = sha256_hex(canonical.as_bytes());
    Ok(format!("{PUBLIC_ACTION_ID_PREFIX}{}", &digest[..32]))
}

pub fn validate_public_approval_action_id(
    public_action_id: &str,
    effect_id: &str,
    approval_generation: u64,
    decision: ChannelApprovalDecisionV1,
) -> Result<(), ChannelActionError> {
    if public_action_id
        != public_approval_action_id(effect_id, approval_generation, decision)?.as_str()
        || public_action_id.contains(APPROVAL_BEARER_PREFIX)
    {
        return Err(ChannelActionError::InvalidField("public action ID"));
    }
    Ok(())
}

fn validate_effect_binding(
    effect_id: &str,
    approval_generation: u64,
) -> Result<(), ChannelActionError> {
    validate_bounded_text(effect_id, "effect ID", MAX_EFFECT_ID_BYTES)?;
    if approval_generation == 0 {
        return Err(ChannelActionError::InvalidField("approval generation"));
    }
    Ok(())
}

fn validate_bounded_text(
    value: &str,
    field: &'static str,
    max_bytes: usize,
) -> Result<(), ChannelActionError> {
    if value.trim().is_empty()
        || value.len() > max_bytes
        || value.chars().any(|character| character.is_control())
    {
        return Err(ChannelActionError::InvalidField(field));
    }
    Ok(())
}

fn validate_sha256_digest(value: &str, field: &'static str) -> Result<(), ChannelActionError> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(ChannelActionError::InvalidField(field));
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ChannelActionError::InvalidField(field));
    }
    Ok(())
}

fn validate_callback_token(value: &str) -> Result<(), ChannelActionError> {
    if !value.starts_with(APPROVAL_BEARER_PREFIX)
        || value.len() > MAX_CALLBACK_TOKEN_BYTES
        || value.len() <= APPROVAL_BEARER_PREFIX.len()
        || !value.bytes().all(|byte| byte.is_ascii_graphic())
    {
        return Err(ChannelActionError::InvalidField("callback token"));
    }
    Ok(())
}

fn sha256_hex(bytes: &[u8]) -> String {
    digest(&SHA256, bytes)
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn sha256_tagged(bytes: &[u8]) -> String {
    format!("sha256:{}", sha256_hex(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SESSION_DIGEST: &str =
        "sha256:1111111111111111111111111111111111111111111111111111111111111111";
    const AUTHORITY_DIGEST: &str =
        "sha256:2222222222222222222222222222222222222222222222222222222222222222";
    const RAW_TOKEN: &str = "ahx1_0123456789abcdefghijklmnopqrstuvwxyz-ABCDEFGH";

    fn prompt() -> ChannelApprovalPromptV1 {
        ChannelApprovalPromptV1::new(
            "effect-42",
            7,
            SESSION_DIGEST,
            AUTHORITY_DIGEST,
            "Create the requested issue",
            2_000,
        )
        .unwrap()
    }

    #[test]
    fn approval_prompt_round_trips_and_validates() {
        let prompt = prompt();
        prompt.validate_at(1_999).unwrap();
        assert_eq!(prompt.validate_at(2_000), Err(ChannelActionError::Expired));

        let json = serde_json::to_string(&prompt).unwrap();
        let decoded: ChannelApprovalPromptV1 = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, prompt);
        decoded.validate().unwrap();
        assert!(!json.contains(APPROVAL_BEARER_PREFIX));
    }

    #[test]
    fn public_action_id_not_bearer_and_is_bound_to_semantics() {
        let approve =
            public_approval_action_id("effect-42", 7, ChannelApprovalDecisionV1::Approve).unwrap();
        let deny =
            public_approval_action_id("effect-42", 7, ChannelApprovalDecisionV1::Deny).unwrap();

        assert!(approve.starts_with(PUBLIC_ACTION_ID_PREFIX));
        assert_eq!(approve.len(), PUBLIC_ACTION_ID_PREFIX.len() + 32);
        assert!(!approve.contains(APPROVAL_BEARER_PREFIX));
        assert_ne!(approve, deny);
        assert_ne!(
            approve,
            public_approval_action_id("effect-42", 8, ChannelApprovalDecisionV1::Approve).unwrap()
        );
        validate_public_approval_action_id(
            &approve,
            "effect-42",
            7,
            ChannelApprovalDecisionV1::Approve,
        )
        .unwrap();
    }

    #[test]
    fn no_raw_token_serialization_or_debug_output() {
        let inbound = ChannelInboundActionV1::new(
            "provider-event-1",
            Some("provider-message-1".to_string()),
            "effect-42",
            7,
            ChannelApprovalDecisionV1::Approve,
            SESSION_DIGEST,
            AUTHORITY_DIGEST,
            RAW_TOKEN,
        )
        .unwrap();

        assert_eq!(inbound.expose_callback_token(), RAW_TOKEN);
        let debug = format!("{inbound:?}");
        assert!(!debug.contains(RAW_TOKEN));
        assert!(debug.contains("[REDACTED]"));

        let evidence_json = serde_json::to_string(&inbound.redacted_evidence().unwrap()).unwrap();
        assert!(!evidence_json.contains(RAW_TOKEN));
        assert!(!evidence_json.contains(APPROVAL_BEARER_PREFIX));
        assert!(evidence_json.contains("callbackTokenDigest"));
        assert!(evidence_json.contains(PUBLIC_ACTION_ID_PREFIX));
    }

    #[test]
    fn approval_prompt_rejects_tampered_or_ambiguous_action_sets() {
        let mut tampered_id = prompt();
        tampered_id.actions[0].public_action_id = "ahpa1_tampered".to_string();
        assert_eq!(
            tampered_id.validate(),
            Err(ChannelActionError::InvalidField("public action ID"))
        );

        let mut duplicate_decision = prompt();
        duplicate_decision.actions[1] =
            ChannelApprovalActionV1::new("effect-42", 7, ChannelApprovalDecisionV1::Approve)
                .unwrap();
        assert_eq!(
            duplicate_decision.validate(),
            Err(ChannelActionError::InvalidActions)
        );
    }

    #[test]
    fn approval_prompt_requires_full_session_authority_digests() {
        let mut missing_session_authority = prompt();
        missing_session_authority.source_session_key_digest = "sha256:short".to_string();
        assert_eq!(
            missing_session_authority.validate(),
            Err(ChannelActionError::InvalidField(
                "source session key digest"
            ))
        );

        let mut uppercase_authority = prompt();
        uppercase_authority.source_authority_digest = format!("sha256:{}", "A".repeat(64));
        assert_eq!(
            uppercase_authority.validate(),
            Err(ChannelActionError::InvalidField("source authority digest"))
        );
    }
}
