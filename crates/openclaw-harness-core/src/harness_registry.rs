use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::importer::ConflictPolicy;
use crate::{
    AgentProfile, AgentProfileSource, AgentRegistry, ChannelRegistry, PluginProfile,
    ProviderProfile,
};

const HARNESS_REGISTRY_SCHEMA: &str = "openclaw-harness.target-registry.v1";
const HARNESS_REGISTRY_RECEIPTS_SCHEMA: &str = "openclaw-harness.target-registry-receipts.v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessRegistry {
    pub schema: &'static str,
    pub source_home: PathBuf,
    pub source_workspace: PathBuf,
    pub agents: Vec<HarnessAgent>,
    pub providers: Vec<HarnessProvider>,
    pub plugins: Vec<HarnessPlugin>,
    pub channels: ChannelRegistry,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessAgent {
    pub id: String,
    pub enabled: bool,
    pub source: AgentProfileSource,
    pub source_directory: PathBuf,
    pub workspace: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub session_index_present: bool,
    pub local_models_present: bool,
    pub credential_status: CredentialStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CredentialStatus {
    LocalAuthFilesDetected,
    NotDetected,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessProvider {
    pub id: String,
    pub source: String,
    pub base_url_configured: bool,
    pub credential_ref: String,
    pub credential_status: CredentialStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessPlugin {
    pub id: String,
    pub enabled: Option<bool>,
    pub sidecar_required: bool,
    pub memory_related: bool,
    pub channel_related: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessRegistryExport {
    pub registry_file: PathBuf,
    pub receipts_file: PathBuf,
    pub wrote_files: bool,
    pub conflicts: usize,
    pub receipts: Vec<HarnessRegistryReceipt>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessRegistryReceiptFile {
    pub schema: &'static str,
    pub target_home: PathBuf,
    pub receipts: Vec<HarnessRegistryReceipt>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HarnessRegistryReceipt {
    pub kind: HarnessRegistryReceiptKind,
    pub path: PathBuf,
    pub status: HarnessRegistryReceiptStatus,
    pub reason: String,
    pub backup_path: Option<PathBuf>,
    pub sensitive: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum HarnessRegistryReceiptKind {
    Registry,
    Receipts,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum HarnessRegistryReceiptStatus {
    Written,
    Conflict,
    BackedUpAndWritten,
    RenamedAndWritten,
}

pub fn build_harness_registry(registry: &AgentRegistry) -> HarnessRegistry {
    HarnessRegistry {
        schema: HARNESS_REGISTRY_SCHEMA,
        source_home: registry.source_home.clone(),
        source_workspace: registry.source_workspace.clone(),
        agents: registry.agents.iter().map(harness_agent).collect(),
        providers: registry.providers.iter().map(harness_provider).collect(),
        plugins: registry.plugins.iter().map(harness_plugin).collect(),
        channels: registry.channels.clone(),
        warnings: registry.warnings.clone(),
    }
}

pub fn export_harness_registry_files(
    registry: &AgentRegistry,
    target_home: impl AsRef<Path>,
    conflict_policy: ConflictPolicy,
) -> io::Result<HarnessRegistryExport> {
    let target_home = target_home.as_ref();
    let state_dir = target_home.join("state");
    let registry_file = state_dir.join("harness-registry.json");
    let receipts_file = state_dir.join("harness-registry-receipts.json");

    let registry_target = resolve_output_path(
        &registry_file,
        HarnessRegistryReceiptKind::Registry,
        conflict_policy,
    )?;
    let receipts_target = resolve_output_path(
        &receipts_file,
        HarnessRegistryReceiptKind::Receipts,
        conflict_policy,
    )?;

    let mut receipts = vec![registry_target.receipt, receipts_target.receipt];
    let conflicts = receipts
        .iter()
        .filter(|receipt| receipt.status == HarnessRegistryReceiptStatus::Conflict)
        .count();
    if conflicts > 0 {
        return Ok(HarnessRegistryExport {
            registry_file,
            receipts_file,
            wrote_files: false,
            conflicts,
            receipts,
        });
    }

    fs::create_dir_all(&state_dir)?;
    if let Some(backup_path) = &registry_target.backup_path {
        fs::copy(&registry_file, backup_path)?;
    }
    if let Some(backup_path) = &receipts_target.backup_path {
        fs::copy(&receipts_file, backup_path)?;
    }

    let harness_registry = build_harness_registry(registry);
    let registry_json =
        serde_json::to_string_pretty(&harness_registry).map_err(io::Error::other)?;
    fs::write(&registry_target.path, registry_json)?;

    let receipt_file = HarnessRegistryReceiptFile {
        schema: HARNESS_REGISTRY_RECEIPTS_SCHEMA,
        target_home: target_home.to_path_buf(),
        receipts: receipts.clone(),
    };
    let receipts_json = serde_json::to_string_pretty(&receipt_file).map_err(io::Error::other)?;
    fs::write(&receipts_target.path, receipts_json)?;

    receipts[0].path = registry_target.path.clone();
    receipts[1].path = receipts_target.path.clone();

    Ok(HarnessRegistryExport {
        registry_file: registry_target.path,
        receipts_file: receipts_target.path,
        wrote_files: true,
        conflicts: 0,
        receipts,
    })
}

fn harness_agent(agent: &AgentProfile) -> HarnessAgent {
    HarnessAgent {
        id: agent.id.clone(),
        enabled: agent.enabled.unwrap_or(true),
        source: agent.source,
        source_directory: agent.directory.clone(),
        workspace: agent.workspace.clone(),
        provider: agent.provider.clone(),
        model: agent.model.clone(),
        session_index_present: agent.sessions_index_exists,
        local_models_present: agent.local_models_file,
        credential_status: if agent.auth_file || agent.auth_profiles_file || agent.auth_state_file {
            CredentialStatus::LocalAuthFilesDetected
        } else {
            CredentialStatus::NotDetected
        },
    }
}

fn harness_provider(provider: &ProviderProfile) -> HarnessProvider {
    HarnessProvider {
        id: provider.id.clone(),
        source: provider.source.clone(),
        base_url_configured: provider.has_base_url,
        credential_ref: format!("provider:{}", provider.id),
        credential_status: if provider.has_api_key_reference {
            CredentialStatus::LocalAuthFilesDetected
        } else {
            CredentialStatus::NotDetected
        },
    }
}

fn harness_plugin(plugin: &PluginProfile) -> HarnessPlugin {
    HarnessPlugin {
        id: plugin.id.clone(),
        enabled: plugin.enabled,
        sidecar_required: true,
        memory_related: plugin.memory_related,
        channel_related: plugin.channel_related,
    }
}

struct OutputTarget {
    path: PathBuf,
    backup_path: Option<PathBuf>,
    receipt: HarnessRegistryReceipt,
}

fn resolve_output_path(
    path: &Path,
    kind: HarnessRegistryReceiptKind,
    conflict_policy: ConflictPolicy,
) -> io::Result<OutputTarget> {
    if !path.exists() {
        return Ok(OutputTarget {
            path: path.to_path_buf(),
            backup_path: None,
            receipt: HarnessRegistryReceipt {
                kind,
                path: path.to_path_buf(),
                status: HarnessRegistryReceiptStatus::Written,
                reason: "destination available".to_string(),
                backup_path: None,
                sensitive: false,
            },
        });
    }

    match conflict_policy {
        ConflictPolicy::Skip => Ok(OutputTarget {
            path: path.to_path_buf(),
            backup_path: None,
            receipt: HarnessRegistryReceipt {
                kind,
                path: path.to_path_buf(),
                status: HarnessRegistryReceiptStatus::Conflict,
                reason: "destination exists; choose overwrite or rename".to_string(),
                backup_path: None,
                sensitive: false,
            },
        }),
        ConflictPolicy::Overwrite => {
            let backup_path = available_suffixed_path(path, ".bak")?;
            Ok(OutputTarget {
                path: path.to_path_buf(),
                backup_path: Some(backup_path.clone()),
                receipt: HarnessRegistryReceipt {
                    kind,
                    path: path.to_path_buf(),
                    status: HarnessRegistryReceiptStatus::BackedUpAndWritten,
                    reason: "destination exists; backed up before overwrite".to_string(),
                    backup_path: Some(backup_path),
                    sensitive: false,
                },
            })
        }
        ConflictPolicy::Rename => {
            let renamed = available_suffixed_path(path, "-imported")?;
            Ok(OutputTarget {
                path: renamed.clone(),
                backup_path: None,
                receipt: HarnessRegistryReceipt {
                    kind,
                    path: renamed,
                    status: HarnessRegistryReceiptStatus::RenamedAndWritten,
                    reason: "destination exists; writing renamed output".to_string(),
                    backup_path: None,
                    sensitive: false,
                },
            })
        }
    }
}

fn available_suffixed_path(path: &Path, suffix: &str) -> io::Result<PathBuf> {
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "path has no file name"))?;

    for index in 1.. {
        let indexed_suffix = if index == 1 {
            suffix.to_string()
        } else {
            format!("{suffix}-{index}")
        };
        let candidate = if let Some((stem, extension)) = file_name.rsplit_once('.') {
            path.with_file_name(format!("{stem}{indexed_suffix}.{extension}"))
        } else {
            path.with_file_name(format!("{file_name}{indexed_suffix}"))
        };

        if !candidate.exists() {
            return Ok(candidate);
        }
    }

    unreachable!("unbounded suffix search should always return");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AgentProfile, AgentProfileSource};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn builds_harness_registry_without_raw_secrets() {
        let source_home = PathBuf::from("source-home");
        let registry = AgentRegistry {
            source_home: source_home.clone(),
            source_workspace: PathBuf::from("workspace"),
            agents: vec![AgentProfile {
                id: "main".to_string(),
                enabled: Some(true),
                workspace: Some("/workspace".to_string()),
                provider: Some("openai".to_string()),
                model: Some("gpt-5".to_string()),
                source: AgentProfileSource::ConfigAndDirectory,
                directory: source_home.join("agents").join("main"),
                directory_exists: true,
                sessions_index_exists: true,
                local_models_file: true,
                auth_profiles_file: true,
                auth_state_file: false,
                auth_file: false,
            }],
            providers: vec![ProviderProfile {
                id: "openai".to_string(),
                source: "models.providers".to_string(),
                has_base_url: false,
                has_api_key_reference: true,
            }],
            plugins: vec![PluginProfile {
                id: "telegram".to_string(),
                enabled: Some(true),
                source: "plugins".to_string(),
                memory_related: false,
                channel_related: true,
            }],
            channels: ChannelRegistry {
                telegram: true,
                discord: false,
            },
            ..AgentRegistry::default()
        };

        let harness = build_harness_registry(&registry);

        assert_eq!(harness.schema, HARNESS_REGISTRY_SCHEMA);
        assert_eq!(harness.agents.len(), 1);
        assert_eq!(
            harness.agents[0].credential_status,
            CredentialStatus::LocalAuthFilesDetected
        );
        assert_eq!(harness.providers[0].credential_ref, "provider:openai");
        assert_eq!(
            harness.providers[0].credential_status,
            CredentialStatus::LocalAuthFilesDetected
        );
        assert!(harness.plugins[0].sidecar_required);
    }

    #[test]
    fn exports_harness_registry_files_with_receipts() {
        let root = temp_root("exports_harness_registry_files_with_receipts");
        let registry = AgentRegistry {
            source_home: root.join(".openclaw"),
            source_workspace: root.join(".openclaw").join("workspace"),
            agents: vec![],
            providers: vec![],
            plugins: vec![],
            channels: ChannelRegistry::default(),
            ..AgentRegistry::default()
        };

        let export =
            export_harness_registry_files(&registry, root.join("target"), ConflictPolicy::Skip)
                .unwrap();

        assert!(export.wrote_files);
        assert_eq!(export.conflicts, 0);
        assert!(export.registry_file.is_file());
        assert!(export.receipts_file.is_file());
        let registry_json: serde_json::Value =
            serde_json::from_slice(&fs::read(&export.registry_file).unwrap()).unwrap();
        assert_eq!(registry_json["schema"], HARNESS_REGISTRY_SCHEMA);
        let receipts_json: serde_json::Value =
            serde_json::from_slice(&fs::read(&export.receipts_file).unwrap()).unwrap();
        assert_eq!(receipts_json["schema"], HARNESS_REGISTRY_RECEIPTS_SCHEMA);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn export_respects_conflict_policy() {
        let root = temp_root("export_respects_conflict_policy");
        let target = root.join("target");
        let state = target.join("state");
        fs::create_dir_all(&state).unwrap();
        fs::write(state.join("harness-registry.json"), "{}").unwrap();
        fs::write(state.join("harness-registry-receipts.json"), "{}").unwrap();
        let registry = AgentRegistry::default();

        let skipped = export_harness_registry_files(&registry, &target, ConflictPolicy::Skip)
            .expect("skip conflict should not error");
        assert!(!skipped.wrote_files);
        assert_eq!(skipped.conflicts, 2);

        let renamed = export_harness_registry_files(&registry, &target, ConflictPolicy::Rename)
            .expect("rename conflict should write renamed files");
        assert!(renamed.wrote_files);
        assert!(
            renamed
                .registry_file
                .ends_with("harness-registry-imported.json")
        );
        assert!(
            renamed
                .receipts_file
                .ends_with("harness-registry-receipts-imported.json")
        );

        let overwritten =
            export_harness_registry_files(&registry, &target, ConflictPolicy::Overwrite)
                .expect("overwrite conflict should backup and write");
        assert!(overwritten.wrote_files);
        assert!(
            overwritten
                .receipts
                .iter()
                .any(|receipt| receipt.backup_path.is_some())
        );

        let _ = fs::remove_dir_all(root);
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "openclaw-harness-target-registry-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
