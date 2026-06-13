use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config::harness_config_candidates;

const CHANNEL_IDENTITY_CHECK_SCHEMA: &str = "agent-harness.channel-identity-check.v1";
const CHANNEL_IDENTITY_REGISTRY_SCHEMA: &str = "agent-harness.channel-identity-registry.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelIdentityLookup {
    pub harness_home: PathBuf,
    pub platform: String,
    pub account_id: String,
    pub chat_id: String,
    pub thread_id: Option<String>,
    pub requested_agent_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelIdentityResolution {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub registry_files: Vec<PathBuf>,
    pub status: ChannelIdentityResolutionStatus,
    pub platform: String,
    pub account_id: String,
    pub chat_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requested_agent_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret_ref: Option<String>,
    pub enabled: bool,
    pub reason: String,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ChannelIdentityResolutionStatus {
    Bound,
    NoRegistry,
    NoBinding,
    Conflict,
    AgentMismatch,
    Disabled,
}

impl ChannelIdentityResolution {
    pub fn is_bound(&self) -> bool {
        self.status == ChannelIdentityResolutionStatus::Bound
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelIdentityRegistry {
    #[serde(default = "default_registry_schema")]
    pub schema: String,
    #[serde(default)]
    pub bindings: Vec<ChannelIdentityBinding>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelIdentityBinding {
    pub platform: String,
    #[serde(default = "default_account_id")]
    pub account_id: String,
    pub chat_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    pub agent_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_ref: Option<String>,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

pub fn resolve_channel_identity(
    lookup: ChannelIdentityLookup,
) -> io::Result<ChannelIdentityResolution> {
    let mut warnings = Vec::new();
    let (registry, registry_files) =
        load_channel_identity_registry(&lookup.harness_home, &mut warnings)?;
    let mut resolution = ChannelIdentityResolution {
        schema: CHANNEL_IDENTITY_CHECK_SCHEMA,
        harness_home: lookup.harness_home.clone(),
        registry_files,
        status: ChannelIdentityResolutionStatus::NoBinding,
        platform: normalize_key(&lookup.platform),
        account_id: normalize_account_id(&lookup.account_id),
        chat_id: lookup.chat_id.clone(),
        thread_id: lookup.thread_id.clone(),
        requested_agent_id: lookup.requested_agent_id.clone(),
        agent_id: None,
        secret_ref: None,
        enabled: false,
        reason: "no channel identity binding matched this platform/account/chat".to_string(),
        warnings,
    };

    let Some(registry) = registry else {
        resolution.status = ChannelIdentityResolutionStatus::NoRegistry;
        resolution.reason =
            "channel identity registry is missing; inbound channel traffic fails closed"
                .to_string();
        return Ok(resolution);
    };

    let matches = registry
        .bindings
        .iter()
        .filter(|binding| binding_matches(binding, &lookup))
        .collect::<Vec<_>>();
    if matches.is_empty() {
        return Ok(resolution);
    }
    if matches.len() > 1 {
        resolution.status = ChannelIdentityResolutionStatus::Conflict;
        resolution.reason = format!(
            "{} channel identity bindings matched; refusing ambiguous agent dispatch",
            matches.len()
        );
        return Ok(resolution);
    }

    let binding = matches[0];
    resolution.agent_id = Some(binding.agent_id.clone());
    resolution.secret_ref = binding.secret_ref.clone();
    resolution.enabled = binding.enabled;
    if !binding.enabled {
        resolution.status = ChannelIdentityResolutionStatus::Disabled;
        resolution.reason = "matched channel identity binding is disabled".to_string();
        return Ok(resolution);
    }

    if let Some(requested) = lookup.requested_agent_id.as_deref()
        && requested != binding.agent_id
    {
        resolution.status = ChannelIdentityResolutionStatus::AgentMismatch;
        resolution.reason = format!(
            "requested agent `{requested}` does not match bound agent `{}`",
            binding.agent_id
        );
        return Ok(resolution);
    }

    resolution.status = ChannelIdentityResolutionStatus::Bound;
    resolution.reason = format!(
        "{} account `{}` chat `{}` is bound to agent `{}`",
        normalize_key(&lookup.platform),
        normalize_account_id(&lookup.account_id),
        lookup.chat_id,
        binding.agent_id
    );
    Ok(resolution)
}

pub fn channel_identity_registry_candidates(harness_home: impl AsRef<Path>) -> [PathBuf; 3] {
    let harness_home = harness_home.as_ref();
    [
        harness_home
            .join("config")
            .join("channel-identity-bindings.json"),
        harness_home.join("channel-identity-bindings.json"),
        harness_home
            .join("state")
            .join("channels")
            .join("channel-identity-bindings.json"),
    ]
}

fn load_channel_identity_registry(
    harness_home: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<(Option<ChannelIdentityRegistry>, Vec<PathBuf>)> {
    let mut registry_files = Vec::new();
    let mut bindings = Vec::new();

    for candidate in channel_identity_registry_candidates(harness_home) {
        if !candidate.is_file() {
            continue;
        }
        registry_files.push(candidate.clone());
        let text = fs::read_to_string(&candidate)?;
        match serde_json::from_str::<ChannelIdentityRegistry>(&text) {
            Ok(registry) => bindings.extend(registry.bindings),
            Err(error) => warnings.push(format!(
                "channel identity registry {} is not valid JSON: {error}",
                candidate.display()
            )),
        }
    }

    for candidate in harness_config_candidates(harness_home) {
        if !candidate.is_file() {
            continue;
        }
        let text = fs::read_to_string(&candidate)?;
        match serde_json::from_str::<Value>(&text) {
            Ok(value) => {
                if let Some(section) = value.get("channelIdentity") {
                    registry_files.push(candidate.clone());
                    match serde_json::from_value::<ChannelIdentityRegistry>(section.clone()) {
                        Ok(registry) => bindings.extend(registry.bindings),
                        Err(error) => warnings.push(format!(
                            "harness-config channelIdentity section in {} is invalid: {error}",
                            candidate.display()
                        )),
                    }
                }
            }
            Err(error) => warnings.push(format!(
                "harness-config {} is not valid JSON while loading channel identity: {error}",
                candidate.display()
            )),
        }
        break;
    }

    if registry_files.is_empty() {
        Ok((None, registry_files))
    } else {
        Ok((
            Some(ChannelIdentityRegistry {
                schema: CHANNEL_IDENTITY_REGISTRY_SCHEMA.to_string(),
                bindings,
            }),
            registry_files,
        ))
    }
}

fn binding_matches(binding: &ChannelIdentityBinding, lookup: &ChannelIdentityLookup) -> bool {
    normalize_key(&binding.platform) == normalize_key(&lookup.platform)
        && normalize_account_id(&binding.account_id) == normalize_account_id(&lookup.account_id)
        && binding.chat_id == lookup.chat_id
        && match (binding.thread_id.as_deref(), lookup.thread_id.as_deref()) {
            (Some(left), Some(right)) => left == right,
            (Some(_), None) => false,
            (None, _) => true,
        }
}

fn default_registry_schema() -> String {
    CHANNEL_IDENTITY_REGISTRY_SCHEMA.to_string()
}

fn default_account_id() -> String {
    "default".to_string()
}

fn default_enabled() -> bool {
    true
}

fn normalize_account_id(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        "default".to_string()
    } else {
        trimmed.to_ascii_lowercase()
    }
}

fn normalize_key(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn resolves_exact_binding_and_rejects_agent_mismatch() {
        let root = temp_root("resolves_exact_binding_and_rejects_agent_mismatch");
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(harness_home.join("config")).unwrap();
        fs::write(
            harness_home
                .join("config")
                .join("channel-identity-bindings.json"),
            r#"{
              "bindings": [
                {
                  "platform": "telegram",
                  "accountId": "ops",
                  "chatId": "123",
                  "agentId": "main",
                  "secretRef": "AGENT_HARNESS_TELEGRAM_ACCOUNT_OPS_BOT_TOKEN"
                }
              ]
            }"#,
        )
        .unwrap();

        let bound = resolve_channel_identity(ChannelIdentityLookup {
            harness_home: harness_home.clone(),
            platform: "telegram".to_string(),
            account_id: "ops".to_string(),
            chat_id: "123".to_string(),
            thread_id: None,
            requested_agent_id: Some("main".to_string()),
        })
        .unwrap();
        assert_eq!(bound.status, ChannelIdentityResolutionStatus::Bound);
        assert_eq!(bound.agent_id.as_deref(), Some("main"));
        assert_eq!(
            bound.secret_ref.as_deref(),
            Some("AGENT_HARNESS_TELEGRAM_ACCOUNT_OPS_BOT_TOKEN")
        );

        let mismatch = resolve_channel_identity(ChannelIdentityLookup {
            harness_home,
            platform: "telegram".to_string(),
            account_id: "ops".to_string(),
            chat_id: "123".to_string(),
            thread_id: None,
            requested_agent_id: Some("other".to_string()),
        })
        .unwrap();
        assert_eq!(
            mismatch.status,
            ChannelIdentityResolutionStatus::AgentMismatch
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn missing_registry_fails_closed() {
        let root = temp_root("missing_registry_fails_closed");
        let report = resolve_channel_identity(ChannelIdentityLookup {
            harness_home: root.join(".agent-harness"),
            platform: "discord".to_string(),
            account_id: "default".to_string(),
            chat_id: "channel".to_string(),
            thread_id: None,
            requested_agent_id: None,
        })
        .unwrap();

        assert_eq!(report.status, ChannelIdentityResolutionStatus::NoRegistry);
        assert!(!report.is_bound());

        let _ = fs::remove_dir_all(root);
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-channel-identity-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
