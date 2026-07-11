use std::error::Error;
use std::fmt;

use ring::digest;
use serde::{Deserialize, Deserializer, Serialize};

pub const FULL_LANE_KEY_SCHEMA: &str = "agent-harness.full-lane-key.v1";
pub const MAX_FULL_LANE_AXIS_BYTES: usize = 4096;

const CANONICAL_DOMAIN: &[u8] = b"agent-harness/full-lane-key/v1";
const LEGACY_UNKNOWN_PREFIX: &str = "urn:agent-harness:lane:legacy-unknown:v1:";

/// Every identity axis that participates in a full runtime lane boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FullLaneAxisV1 {
    Platform,
    AccountId,
    ChannelId,
    UserId,
    AgentId,
    RuntimeClass,
    RootVirtualSession,
    ConcreteSession,
}

impl FullLaneAxisV1 {
    pub const ALL: [Self; 8] = [
        Self::Platform,
        Self::AccountId,
        Self::ChannelId,
        Self::UserId,
        Self::AgentId,
        Self::RuntimeClass,
        Self::RootVirtualSession,
        Self::ConcreteSession,
    ];

    pub const fn field_name(self) -> &'static str {
        match self {
            Self::Platform => "platform",
            Self::AccountId => "accountId",
            Self::ChannelId => "channelId",
            Self::UserId => "userId",
            Self::AgentId => "agentId",
            Self::RuntimeClass => "runtimeClass",
            Self::RootVirtualSession => "rootVirtualSession",
            Self::ConcreteSession => "concreteSession",
        }
    }

    const fn unknown_value(self) -> &'static str {
        match self {
            Self::Platform => "urn:agent-harness:lane:legacy-unknown:v1:platform",
            Self::AccountId => "urn:agent-harness:lane:legacy-unknown:v1:accountId",
            Self::ChannelId => "urn:agent-harness:lane:legacy-unknown:v1:channelId",
            Self::UserId => "urn:agent-harness:lane:legacy-unknown:v1:userId",
            Self::AgentId => "urn:agent-harness:lane:legacy-unknown:v1:agentId",
            Self::RuntimeClass => "urn:agent-harness:lane:legacy-unknown:v1:runtimeClass",
            Self::RootVirtualSession => {
                "urn:agent-harness:lane:legacy-unknown:v1:rootVirtualSession"
            }
            Self::ConcreteSession => "urn:agent-harness:lane:legacy-unknown:v1:concreteSession",
        }
    }
}

/// Versioned, non-wildcard identity for a channel/runtime/session lane.
///
/// Values are case-preserving because several upstream identifiers are
/// case-sensitive. Constructors trim surrounding whitespace once, while
/// deserialization accepts only already-canonical values.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FullLaneKeyV1 {
    schema: String,
    platform: String,
    account_id: String,
    channel_id: String,
    user_id: String,
    agent_id: String,
    runtime_class: String,
    root_virtual_session: String,
    concrete_session: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FullLaneKeyV1Wire {
    schema: String,
    platform: String,
    #[serde(alias = "account_id")]
    account_id: String,
    #[serde(alias = "channel_id")]
    channel_id: String,
    #[serde(alias = "user_id")]
    user_id: String,
    #[serde(alias = "agent_id")]
    agent_id: String,
    #[serde(alias = "runtime_class")]
    runtime_class: String,
    #[serde(
        alias = "root_virtual_session",
        alias = "rootSession",
        alias = "root_session"
    )]
    root_virtual_session: String,
    #[serde(alias = "concrete_session")]
    concrete_session: String,
}

impl<'de> Deserialize<'de> for FullLaneKeyV1 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = FullLaneKeyV1Wire::deserialize(deserializer)?;
        let lane = Self {
            schema: wire.schema,
            platform: wire.platform,
            account_id: wire.account_id,
            channel_id: wire.channel_id,
            user_id: wire.user_id,
            agent_id: wire.agent_id,
            runtime_class: wire.runtime_class,
            root_virtual_session: wire.root_virtual_session,
            concrete_session: wire.concrete_session,
        };
        lane.validate().map_err(serde::de::Error::custom)?;
        Ok(lane)
    }
}

/// Partial legacy identity. Omitted axes become axis-specific unknown values.
/// Unknown input fields are intentionally tolerated for additive migrations.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LegacyLaneKeyV0 {
    #[serde(default)]
    pub platform: Option<String>,
    #[serde(default, alias = "account_id")]
    pub account_id: Option<String>,
    #[serde(default, alias = "channel_id")]
    pub channel_id: Option<String>,
    #[serde(default, alias = "user_id")]
    pub user_id: Option<String>,
    #[serde(default, alias = "agent_id")]
    pub agent_id: Option<String>,
    #[serde(default, alias = "runtime_class")]
    pub runtime_class: Option<String>,
    #[serde(
        default,
        alias = "root_virtual_session",
        alias = "rootSession",
        alias = "root_session"
    )]
    pub root_virtual_session: Option<String>,
    #[serde(default, alias = "concrete_session")]
    pub concrete_session: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FullLaneKeyError {
    UnsupportedSchema(String),
    EmptyAxis(&'static str),
    NonCanonicalAxis(&'static str),
    ControlCharacter(&'static str),
    AxisTooLong {
        axis: &'static str,
        actual_bytes: usize,
        max_bytes: usize,
    },
    ReservedUnknownValue(&'static str),
}

impl fmt::Display for FullLaneKeyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedSchema(schema) => {
                write!(formatter, "unsupported full-lane schema `{schema}`")
            }
            Self::EmptyAxis(axis) => write!(formatter, "full-lane axis `{axis}` is empty"),
            Self::NonCanonicalAxis(axis) => {
                write!(formatter, "full-lane axis `{axis}` is not canonical")
            }
            Self::ControlCharacter(axis) => {
                write!(
                    formatter,
                    "full-lane axis `{axis}` contains a control character"
                )
            }
            Self::AxisTooLong {
                axis,
                actual_bytes,
                max_bytes,
            } => write!(
                formatter,
                "full-lane axis `{axis}` is {actual_bytes} bytes; maximum is {max_bytes}"
            ),
            Self::ReservedUnknownValue(axis) => write!(
                formatter,
                "full-lane axis `{axis}` uses a reserved legacy-unknown value"
            ),
        }
    }
}

impl Error for FullLaneKeyError {}

impl FullLaneKeyV1 {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        platform: impl Into<String>,
        account_id: impl Into<String>,
        channel_id: impl Into<String>,
        user_id: impl Into<String>,
        agent_id: impl Into<String>,
        runtime_class: impl Into<String>,
        root_virtual_session: impl Into<String>,
        concrete_session: impl Into<String>,
    ) -> Result<Self, FullLaneKeyError> {
        Ok(Self {
            schema: FULL_LANE_KEY_SCHEMA.to_string(),
            platform: normalize_known(FullLaneAxisV1::Platform, platform.into())?,
            account_id: normalize_known(FullLaneAxisV1::AccountId, account_id.into())?,
            channel_id: normalize_known(FullLaneAxisV1::ChannelId, channel_id.into())?,
            user_id: normalize_known(FullLaneAxisV1::UserId, user_id.into())?,
            agent_id: normalize_known(FullLaneAxisV1::AgentId, agent_id.into())?,
            runtime_class: normalize_known(FullLaneAxisV1::RuntimeClass, runtime_class.into())?,
            root_virtual_session: normalize_known(
                FullLaneAxisV1::RootVirtualSession,
                root_virtual_session.into(),
            )?,
            concrete_session: normalize_known(
                FullLaneAxisV1::ConcreteSession,
                concrete_session.into(),
            )?,
        })
    }

    pub fn from_legacy(legacy: LegacyLaneKeyV0) -> Result<Self, FullLaneKeyError> {
        Ok(Self {
            schema: FULL_LANE_KEY_SCHEMA.to_string(),
            platform: normalize_legacy(FullLaneAxisV1::Platform, legacy.platform)?,
            account_id: normalize_legacy(FullLaneAxisV1::AccountId, legacy.account_id)?,
            channel_id: normalize_legacy(FullLaneAxisV1::ChannelId, legacy.channel_id)?,
            user_id: normalize_legacy(FullLaneAxisV1::UserId, legacy.user_id)?,
            agent_id: normalize_legacy(FullLaneAxisV1::AgentId, legacy.agent_id)?,
            runtime_class: normalize_legacy(FullLaneAxisV1::RuntimeClass, legacy.runtime_class)?,
            root_virtual_session: normalize_legacy(
                FullLaneAxisV1::RootVirtualSession,
                legacy.root_virtual_session,
            )?,
            concrete_session: normalize_legacy(
                FullLaneAxisV1::ConcreteSession,
                legacy.concrete_session,
            )?,
        })
    }

    pub fn validate(&self) -> Result<(), FullLaneKeyError> {
        if self.schema != FULL_LANE_KEY_SCHEMA {
            return Err(FullLaneKeyError::UnsupportedSchema(self.schema.clone()));
        }

        for (axis, value) in self.axes() {
            validate_canonical(axis, value)?;
        }
        Ok(())
    }

    pub fn has_legacy_unknowns(&self) -> bool {
        self.axes()
            .iter()
            .any(|(axis, value)| *value == axis.unknown_value())
    }

    pub fn schema(&self) -> &str {
        &self.schema
    }

    pub fn platform(&self) -> &str {
        &self.platform
    }

    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    pub fn channel_id(&self) -> &str {
        &self.channel_id
    }

    pub fn user_id(&self) -> &str {
        &self.user_id
    }

    pub fn agent_id(&self) -> &str {
        &self.agent_id
    }

    pub fn runtime_class(&self) -> &str {
        &self.runtime_class
    }

    pub fn root_virtual_session(&self) -> &str {
        &self.root_virtual_session
    }

    pub fn concrete_session(&self) -> &str {
        &self.concrete_session
    }

    /// Collision-resistant canonical bytes. Every component is named and
    /// length-prefixed; separators inside an identifier have no special role.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, FullLaneKeyError> {
        self.validate()?;

        let mut bytes = Vec::with_capacity(256);
        append_component(&mut bytes, b"domain", CANONICAL_DOMAIN);
        append_component(&mut bytes, b"schema", self.schema.as_bytes());
        for (axis, value) in self.axes() {
            append_component(&mut bytes, axis.field_name().as_bytes(), value.as_bytes());
        }
        Ok(bytes)
    }

    pub fn identity_hash(&self) -> Result<String, FullLaneKeyError> {
        let bytes = self.canonical_bytes()?;
        let hash = digest::digest(&digest::SHA256, &bytes);
        Ok(lower_hex(hash.as_ref()))
    }

    fn axes(&self) -> [(FullLaneAxisV1, &str); 8] {
        [
            (FullLaneAxisV1::Platform, self.platform.as_str()),
            (FullLaneAxisV1::AccountId, self.account_id.as_str()),
            (FullLaneAxisV1::ChannelId, self.channel_id.as_str()),
            (FullLaneAxisV1::UserId, self.user_id.as_str()),
            (FullLaneAxisV1::AgentId, self.agent_id.as_str()),
            (FullLaneAxisV1::RuntimeClass, self.runtime_class.as_str()),
            (
                FullLaneAxisV1::RootVirtualSession,
                self.root_virtual_session.as_str(),
            ),
            (
                FullLaneAxisV1::ConcreteSession,
                self.concrete_session.as_str(),
            ),
        ]
    }
}

fn normalize_known(axis: FullLaneAxisV1, value: String) -> Result<String, FullLaneKeyError> {
    let normalized = value.trim();
    validate_common(axis, normalized)?;
    if normalized.starts_with(LEGACY_UNKNOWN_PREFIX) {
        return Err(FullLaneKeyError::ReservedUnknownValue(axis.field_name()));
    }
    Ok(normalized.to_string())
}

fn normalize_legacy(
    axis: FullLaneAxisV1,
    value: Option<String>,
) -> Result<String, FullLaneKeyError> {
    match value {
        Some(value) => normalize_known(axis, value),
        None => Ok(axis.unknown_value().to_string()),
    }
}

fn validate_canonical(axis: FullLaneAxisV1, value: &str) -> Result<(), FullLaneKeyError> {
    validate_common(axis, value)?;
    if value != value.trim() {
        return Err(FullLaneKeyError::NonCanonicalAxis(axis.field_name()));
    }
    if value.starts_with(LEGACY_UNKNOWN_PREFIX) && value != axis.unknown_value() {
        return Err(FullLaneKeyError::ReservedUnknownValue(axis.field_name()));
    }
    Ok(())
}

fn validate_common(axis: FullLaneAxisV1, value: &str) -> Result<(), FullLaneKeyError> {
    if value.is_empty() {
        return Err(FullLaneKeyError::EmptyAxis(axis.field_name()));
    }
    if value.chars().any(char::is_control) {
        return Err(FullLaneKeyError::ControlCharacter(axis.field_name()));
    }
    if value.len() > MAX_FULL_LANE_AXIS_BYTES {
        return Err(FullLaneKeyError::AxisTooLong {
            axis: axis.field_name(),
            actual_bytes: value.len(),
            max_bytes: MAX_FULL_LANE_AXIS_BYTES,
        });
    }
    Ok(())
}

fn append_component(bytes: &mut Vec<u8>, name: &[u8], value: &[u8]) {
    bytes.extend_from_slice(&(name.len() as u64).to_be_bytes());
    bytes.extend_from_slice(name);
    bytes.extend_from_slice(&(value.len() as u64).to_be_bytes());
    bytes.extend_from_slice(value);
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn lane() -> FullLaneKeyV1 {
        FullLaneKeyV1::new(
            "discord",
            "account-1",
            "channel-1",
            "user-1",
            "main",
            "interactive",
            "root-1",
            "session-1",
        )
        .unwrap()
    }

    #[test]
    fn full_lane_roundtrips_with_versioned_camel_case_schema() {
        let expected = lane();
        let value = serde_json::to_value(&expected).unwrap();

        assert_eq!(value["schema"], FULL_LANE_KEY_SCHEMA);
        assert_eq!(value["accountId"], "account-1");
        assert_eq!(value["runtimeClass"], "interactive");
        assert_eq!(value["rootVirtualSession"], "root-1");
        assert_eq!(value["concreteSession"], "session-1");
        assert_eq!(
            serde_json::from_value::<FullLaneKeyV1>(value).unwrap(),
            expected
        );
    }

    #[test]
    fn identity_hash_changes_for_every_lane_axis() {
        let expected = lane();
        let baseline = expected.identity_hash().unwrap();

        for axis in FullLaneAxisV1::ALL {
            let mut changed = expected.clone();
            match axis {
                FullLaneAxisV1::Platform => changed.platform.push_str("-other"),
                FullLaneAxisV1::AccountId => changed.account_id.push_str("-other"),
                FullLaneAxisV1::ChannelId => changed.channel_id.push_str("-other"),
                FullLaneAxisV1::UserId => changed.user_id.push_str("-other"),
                FullLaneAxisV1::AgentId => changed.agent_id.push_str("-other"),
                FullLaneAxisV1::RuntimeClass => changed.runtime_class.push_str("-other"),
                FullLaneAxisV1::RootVirtualSession => {
                    changed.root_virtual_session.push_str("-other")
                }
                FullLaneAxisV1::ConcreteSession => changed.concrete_session.push_str("-other"),
            }
            assert_ne!(changed.identity_hash().unwrap(), baseline, "{axis:?}");
        }
    }

    #[test]
    fn legacy_adapter_uses_axis_specific_unknowns_without_matching_concrete_lane() {
        let legacy = FullLaneKeyV1::from_legacy(LegacyLaneKeyV0 {
            platform: Some(" discord ".to_string()),
            channel_id: Some("channel-1".to_string()),
            user_id: Some("user-1".to_string()),
            agent_id: Some("main".to_string()),
            concrete_session: Some("session-1".to_string()),
            ..LegacyLaneKeyV0::default()
        })
        .unwrap();

        assert_eq!(legacy.platform, "discord");
        assert_eq!(legacy.account_id, FullLaneAxisV1::AccountId.unknown_value());
        assert_eq!(
            legacy.runtime_class,
            FullLaneAxisV1::RuntimeClass.unknown_value()
        );
        assert_eq!(
            legacy.root_virtual_session,
            FullLaneAxisV1::RootVirtualSession.unknown_value()
        );
        assert!(legacy.has_legacy_unknowns());
        assert_ne!(legacy, lane());
        assert_ne!(
            legacy.identity_hash().unwrap(),
            lane().identity_hash().unwrap()
        );
    }

    #[test]
    fn legacy_wire_adapter_tolerates_additive_fields_without_guessing_missing_axes() {
        let legacy: LegacyLaneKeyV0 = serde_json::from_value(json!({
            "platform": "telegram",
            "channelId": "channel-2",
            "concreteSession": "session-2",
            "futureAxis": "must-not-be-guessed"
        }))
        .unwrap();
        let lane = FullLaneKeyV1::from_legacy(legacy).unwrap();

        assert_eq!(lane.platform, "telegram");
        assert_eq!(lane.channel_id, "channel-2");
        assert_eq!(lane.concrete_session, "session-2");
        assert_eq!(lane.user_id, FullLaneAxisV1::UserId.unknown_value());
        assert_eq!(lane.agent_id, FullLaneAxisV1::AgentId.unknown_value());
    }

    #[test]
    fn empty_or_noncanonical_required_axes_are_rejected() {
        assert_eq!(
            FullLaneKeyV1::new(" ", "a", "c", "u", "g", "r", "root", "session"),
            Err(FullLaneKeyError::EmptyAxis("platform"))
        );
        assert_eq!(
            FullLaneKeyV1::from_legacy(LegacyLaneKeyV0 {
                account_id: Some("\t".to_string()),
                ..LegacyLaneKeyV0::default()
            }),
            Err(FullLaneKeyError::EmptyAxis("accountId"))
        );

        let mut value = serde_json::to_value(lane()).unwrap();
        value["channelId"] = json!(" channel-1 ");
        assert!(serde_json::from_value::<FullLaneKeyV1>(value).is_err());
    }

    #[test]
    fn canonical_hash_does_not_collide_when_values_contain_separators() {
        let left = FullLaneKeyV1::new("a|b", "c", "d:e", "f/g", "h", "i", "j", "k").unwrap();
        let right = FullLaneKeyV1::new("a", "b|c", "d", "e:f/g", "h", "i", "j", "k").unwrap();

        assert_ne!(
            left.canonical_bytes().unwrap(),
            right.canonical_bytes().unwrap()
        );
        assert_ne!(
            left.identity_hash().unwrap(),
            right.identity_hash().unwrap()
        );
    }

    #[test]
    fn reserved_unknown_markers_cannot_be_claimed_by_concrete_callers() {
        let error = FullLaneKeyV1::new(
            FullLaneAxisV1::Platform.unknown_value(),
            "a",
            "c",
            "u",
            "g",
            "r",
            "root",
            "session",
        )
        .unwrap_err();

        assert_eq!(error, FullLaneKeyError::ReservedUnknownValue("platform"));
    }

    #[test]
    fn full_lane_readers_accept_snake_case_aliases_but_writer_stays_camel_case() {
        let current: FullLaneKeyV1 = serde_json::from_value(json!({
            "schema": FULL_LANE_KEY_SCHEMA,
            "platform": "discord",
            "account_id": "account-1",
            "channel_id": "channel-1",
            "user_id": "user-1",
            "agent_id": "main",
            "runtime_class": "interactive",
            "root_virtual_session": "root-1",
            "concrete_session": "session-1"
        }))
        .unwrap();
        assert_eq!(current, lane());

        let legacy: LegacyLaneKeyV0 = serde_json::from_value(json!({
            "platform": "telegram",
            "account_id": "account-2",
            "channel_id": "channel-2",
            "user_id": "user-2",
            "agent_id": "worker",
            "runtime_class": "worker",
            "root_virtual_session": "root-2",
            "concrete_session": "session-2"
        }))
        .unwrap();
        let adapted = FullLaneKeyV1::from_legacy(legacy).unwrap();
        assert_eq!(adapted.account_id, "account-2");
        assert_eq!(adapted.root_virtual_session, "root-2");
        assert!(!adapted.has_legacy_unknowns());

        let canonical = serde_json::to_value(current).unwrap();
        assert!(canonical.get("accountId").is_some());
        assert!(canonical.get("rootVirtualSession").is_some());
        assert!(canonical.get("account_id").is_none());
        assert!(canonical.get("root_virtual_session").is_none());
    }

    #[test]
    fn oversized_lane_axis_is_rejected_before_canonical_allocation() {
        assert!(
            FullLaneKeyV1::new(
                "x".repeat(4097),
                "account",
                "channel",
                "user",
                "agent",
                "interactive",
                "root",
                "session",
            )
            .is_err()
        );
    }

    #[test]
    fn canonical_bytes_and_hash_match_the_v1_golden_vector() {
        let lane = lane();
        assert_eq!(lane.canonical_bytes().unwrap().len(), 377);
        assert_eq!(
            lane.identity_hash().unwrap(),
            "50d82b31e68722d7a13ce618f13452bc8597d8118bcfce657c54498817ff50d2"
        );
    }

    #[test]
    fn opaque_axis_case_and_unicode_forms_remain_distinct_while_outer_space_is_trimmed() {
        let trimmed = FullLaneKeyV1::new(
            " discord ",
            " account-1 ",
            "channel-1",
            "user-1",
            "main",
            "interactive",
            "root-1",
            "session-1",
        )
        .unwrap();
        assert_eq!(trimmed, lane());

        let different_case = FullLaneKeyV1::new(
            "Discord",
            "account-1",
            "channel-1",
            "user-1",
            "main",
            "interactive",
            "root-1",
            "session-1",
        )
        .unwrap();
        assert_ne!(
            different_case.identity_hash().unwrap(),
            lane().identity_hash().unwrap()
        );

        let composed = FullLaneKeyV1::new(
            "discord",
            "Å",
            "channel-1",
            "user-1",
            "main",
            "interactive",
            "root-1",
            "session-1",
        )
        .unwrap();
        let decomposed = FullLaneKeyV1::new(
            "discord",
            "A\u{030a}",
            "channel-1",
            "user-1",
            "main",
            "interactive",
            "root-1",
            "session-1",
        )
        .unwrap();
        assert_ne!(
            composed.identity_hash().unwrap(),
            decomposed.identity_hash().unwrap()
        );
    }
}
