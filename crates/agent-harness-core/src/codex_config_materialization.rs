use std::collections::BTreeSet;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use ring::digest;
use serde::{Deserialize, Serialize};
use toml_edit::DocumentMut;

#[cfg(not(windows))]
use std::fs::File;

use crate::backend_auth::{
    enforce_private_file_permissions, enforce_provider_home_permissions,
    private_file_permissions_are_restrictive, provider_home_permissions_are_restrictive,
};

pub const CODEX_CONFIG_MATERIALIZATION_SCHEMA: &str =
    "agent-harness.codex-config-materialization.v1";
pub const HARNESS_MANAGED_CODEX_CONFIG_ROOTS: &[&str] = &[
    "model_provider",
    "model_context_window",
    "model_auto_compact_token_limit",
    "model_auto_compact_token_limit_scope",
    "tool_output_token_limit",
    "compact_prompt",
    "experimental_compact_prompt_file",
    "windows",
    "features",
    "model_providers",
    "projects",
];

const CONFIG_FILE: &str = "config.toml";
const ORIGINAL_FILE: &str = "config.toml.agent-harness-original";
const ORIGINAL_ABSENT_FILE: &str = "config.toml.agent-harness-original.absent";
static MATERIALIZATION_NONCE: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CodexConfigMaterializationSourceV1 {
    Generated,
    Reused,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CodexConfigMaterializationResultV1 {
    Created,
    Updated,
    Unchanged,
}

#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodexConfigMaterializationFailpoint {
    AfterTempSync,
    AfterReplace,
}

#[derive(Debug, Clone)]
pub struct CodexConfigMaterializationOptions<'a> {
    pub codex_home: &'a Path,
    pub desired_config: &'a str,
    pub agent_id: &'a str,
    pub provider: &'a str,
    pub preserve_unmanaged_roots: bool,
    pub failpoint: Option<CodexConfigMaterializationFailpoint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexConfigMaterializationReceiptV1 {
    pub schema: String,
    pub source: CodexConfigMaterializationSourceV1,
    pub result: CodexConfigMaterializationResultV1,
    pub config_hash_before: Option<String>,
    pub config_hash_after: String,
    pub agent_id_digest: String,
    pub provider_digest: String,
    pub codex_home_digest: String,
    pub managed_roots: Vec<String>,
    pub preserved_roots: Vec<String>,
    pub extension_root_count: usize,
    pub unknown_root_count: usize,
    pub permissions_restrictive: bool,
    pub rollback_available: bool,
}

pub fn materialize_codex_config(
    options: CodexConfigMaterializationOptions<'_>,
) -> io::Result<CodexConfigMaterializationReceiptV1> {
    let codex_home_created = !options.codex_home.exists();
    fs::create_dir_all(options.codex_home)?;
    if codex_home_created {
        enforce_provider_home_permissions(options.codex_home)?;
    } else if !provider_home_permissions_are_restrictive(options.codex_home)? {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "existing Codex home permissions are not restrictive; migrate them only in an authorized stopped-state operation",
        ));
    }
    let config_file = options.codex_home.join(CONFIG_FILE);
    let before = fs::read(&config_file).map(Some).or_else(|error| {
        if error.kind() == io::ErrorKind::NotFound {
            Ok(None)
        } else {
            Err(error)
        }
    })?;
    let existing_text = before
        .as_deref()
        .map(|bytes| {
            std::str::from_utf8(bytes)
                .map(str::to_string)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
        })
        .transpose()?;
    let (rendered, preserved_roots) = merge_codex_config_documents(
        existing_text.as_deref(),
        options.desired_config,
        options.preserve_unmanaged_roots,
    )?;
    let after = rendered.into_bytes();
    let before_hash = before.as_deref().map(sha256_prefixed);
    let after_hash = sha256_prefixed(&after);
    let result = match before.as_deref() {
        None => CodexConfigMaterializationResultV1::Created,
        Some(bytes) if bytes == after.as_slice() => CodexConfigMaterializationResultV1::Unchanged,
        Some(_) => CodexConfigMaterializationResultV1::Updated,
    };
    let rollback_available = ensure_original_snapshot(options.codex_home, before.as_deref())?;

    if result != CodexConfigMaterializationResultV1::Unchanged {
        transactional_replace(&config_file, &after, before.as_deref(), options.failpoint)?;
    }
    enforce_private_file_permissions(&config_file)?;
    let readback = fs::read(&config_file)?;
    if readback != after {
        return Err(io::Error::other(
            "Codex config materialization readback did not match committed bytes",
        ));
    }
    let permissions_restrictive = provider_home_permissions_are_restrictive(options.codex_home)?
        && private_file_permissions_are_restrictive(&config_file)?;
    if !permissions_restrictive {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "Codex config materialization did not retain restrictive permissions",
        ));
    }

    let managed_roots = HARNESS_MANAGED_CODEX_CONFIG_ROOTS
        .iter()
        .map(|root| (*root).to_string())
        .collect::<Vec<_>>();
    let extension_root_count = preserved_roots
        .iter()
        .filter(|root| root.as_str() == "mcp_servers")
        .count();
    let unknown_root_count = preserved_roots.len().saturating_sub(extension_root_count);
    Ok(CodexConfigMaterializationReceiptV1 {
        schema: CODEX_CONFIG_MATERIALIZATION_SCHEMA.to_string(),
        source: if result == CodexConfigMaterializationResultV1::Unchanged {
            CodexConfigMaterializationSourceV1::Reused
        } else {
            CodexConfigMaterializationSourceV1::Generated
        },
        result,
        config_hash_before: before_hash,
        config_hash_after: after_hash,
        agent_id_digest: identity_digest(options.agent_id),
        provider_digest: identity_digest(options.provider),
        codex_home_digest: identity_digest(&options.codex_home.to_string_lossy()),
        managed_roots,
        preserved_roots,
        extension_root_count,
        unknown_root_count,
        permissions_restrictive,
        rollback_available,
    })
}

pub fn restore_original_codex_config(codex_home: &Path) -> io::Result<()> {
    let config_file = codex_home.join(CONFIG_FILE);
    let original_file = codex_home.join(ORIGINAL_FILE);
    let absent_file = codex_home.join(ORIGINAL_ABSENT_FILE);
    match (original_file.is_file(), absent_file.is_file()) {
        (true, false) => {
            let original = fs::read(&original_file)?;
            transactional_replace(&config_file, &original, None, None)?;
            enforce_private_file_permissions(&config_file)
        }
        (false, true) => {
            if config_file.exists() {
                fs::remove_file(config_file)?;
            }
            Ok(())
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Codex config original recovery state is missing or ambiguous",
        )),
    }
}

fn merge_codex_config_documents(
    existing: Option<&str>,
    desired: &str,
    preserve_unmanaged_roots: bool,
) -> io::Result<(String, Vec<String>)> {
    let mut current_existing = existing.map(str::to_string);
    for _ in 0..8 {
        let mut desired_document = parse_document(desired, "desired")?;
        let mut preserved = BTreeSet::new();
        if let Some(existing) = current_existing.as_deref() {
            let existing_document = parse_document(existing, "existing")?;
            validate_external_mcp_root(&existing_document)?;
            if preserve_unmanaged_roots {
                for (root, item) in existing_document.iter() {
                    if !HARNESS_MANAGED_CODEX_CONFIG_ROOTS.contains(&root) {
                        desired_document.as_table_mut().insert(root, item.clone());
                        preserved.insert(root.to_string());
                    }
                }
            }
        }
        validate_external_mcp_root(&desired_document)?;
        let rendered = desired_document.to_string();
        if current_existing.as_deref() == Some(rendered.as_str()) {
            return Ok((rendered, preserved.into_iter().collect()));
        }
        current_existing = Some(rendered);
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "Codex config semantic merge did not converge to a byte-idempotent document",
    ))
}

fn parse_document(value: &str, label: &str) -> io::Result<DocumentMut> {
    value.parse::<DocumentMut>().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{label} Codex config is not valid unambiguous TOML: {error}"),
        )
    })
}

fn validate_external_mcp_root(document: &DocumentMut) -> io::Result<()> {
    let Some(root) = document.get("mcp_servers") else {
        return Ok(());
    };
    let Some(servers) = root.as_table_like() else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "mcp_servers must be a TOML table",
        ));
    };
    for (server_id, server) in servers.iter() {
        if server_id.trim().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "mcp_servers contains an empty server identifier",
            ));
        }
        let Some(server) = server.as_table_like() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("mcp_servers.{server_id} must be a TOML table"),
            ));
        };
        let has_command = server
            .get("command")
            .and_then(|item| item.as_str())
            .is_some();
        let has_url = server.get("url").and_then(|item| item.as_str()).is_some();
        if has_url {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!(
                    "mcp_servers.{server_id} uses an unsupported transport; Phase 0 admits stdio command servers only"
                ),
            ));
        }
        if !has_command {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("mcp_servers.{server_id} must declare a stdio command"),
            ));
        }
    }
    Ok(())
}

fn ensure_original_snapshot(codex_home: &Path, original: Option<&[u8]>) -> io::Result<bool> {
    let original_file = codex_home.join(ORIGINAL_FILE);
    let absent_file = codex_home.join(ORIGINAL_ABSENT_FILE);
    match (original_file.is_file(), absent_file.is_file()) {
        (true, true) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Codex config original recovery state is ambiguous",
            ));
        }
        (true, false) | (false, true) => return Ok(true),
        (false, false) => {}
    }
    match original {
        Some(bytes) => write_new_private_file(&original_file, bytes)?,
        None => write_new_private_file(&absent_file, b"original-config-absent\n")?,
    }
    Ok(true)
}

fn transactional_replace(
    target: &Path,
    desired: &[u8],
    original: Option<&[u8]>,
    failpoint: Option<CodexConfigMaterializationFailpoint>,
) -> io::Result<()> {
    let parent = target.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "Codex config target has no parent directory",
        )
    })?;
    fs::create_dir_all(parent)?;
    let temp = sibling_transaction_path(target, "tmp");
    write_new_private_file(&temp, desired)?;
    if failpoint == Some(CodexConfigMaterializationFailpoint::AfterTempSync) {
        let _ = fs::remove_file(&temp);
        return Err(io::Error::other(
            "injected Codex config failure after temporary sync",
        ));
    }
    replace_file_atomic(&temp, target)?;
    if failpoint == Some(CodexConfigMaterializationFailpoint::AfterReplace) {
        let restore_result = match original {
            Some(bytes) => {
                let restore = sibling_transaction_path(target, "restore");
                write_new_private_file(&restore, bytes)?;
                replace_file_atomic(&restore, target)
            }
            None => fs::remove_file(target),
        };
        restore_result?;
        return Err(io::Error::other(
            "injected Codex config failure after replacement",
        ));
    }
    sync_parent_directory(parent)?;
    Ok(())
}

fn sibling_transaction_path(target: &Path, role: &str) -> PathBuf {
    let nonce = MATERIALIZATION_NONCE.fetch_add(1, Ordering::Relaxed);
    let file_name = target
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(CONFIG_FILE);
    target.with_file_name(format!(
        ".{file_name}.agent-harness-{role}-{}-{nonce}",
        std::process::id()
    ))
}

fn write_new_private_file(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
    file.write_all(bytes)?;
    file.flush()?;
    file.sync_all()?;
    drop(file);
    enforce_private_file_permissions(path)
}

#[cfg(not(windows))]
fn replace_file_atomic(source: &Path, target: &Path) -> io::Result<()> {
    fs::rename(source, target)
}

#[cfg(windows)]
fn replace_file_atomic(source: &Path, target: &Path) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{
        MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
    };

    let source = source
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let target = target
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    // SAFETY: both buffers are live, null-terminated UTF-16 paths.
    if unsafe {
        MoveFileExW(
            source.as_ptr(),
            target.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(not(windows))]
fn sync_parent_directory(parent: &Path) -> io::Result<()> {
    File::open(parent)?.sync_all()
}

#[cfg(windows)]
fn sync_parent_directory(_parent: &Path) -> io::Result<()> {
    Ok(())
}

fn identity_digest(value: &str) -> String {
    sha256_prefixed(value.as_bytes())
}

fn sha256_prefixed(value: &[u8]) -> String {
    let digest = digest::digest(&digest::SHA256, value);
    let mut result = String::with_capacity(71);
    result.push_str("sha256:");
    for byte in digest.as_ref() {
        use std::fmt::Write as _;
        write!(&mut result, "{byte:02x}").expect("writing a digest to String cannot fail");
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_root(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("codex-config-materialization-{name}-{nonce}"))
    }

    fn desired(root: &Path) -> String {
        format!(
            "# Generated by agent-harness. May preserve extension-owned secrets.\n\
             [features]\n\
             multi_agent = true\n\
             memories = true\n\
             \n\
             [projects.{}]\n\
             trust_level = \"trusted\"\n",
            serde_json::to_string(&root.to_string_lossy()).unwrap()
        )
    }

    fn options<'a>(
        home: &'a Path,
        desired_config: &'a str,
    ) -> CodexConfigMaterializationOptions<'a> {
        CodexConfigMaterializationOptions {
            codex_home: home,
            desired_config,
            agent_id: "main",
            provider: "openai",
            preserve_unmanaged_roots: true,
            failpoint: None,
        }
    }

    fn create_restrictive_home(home: &Path) {
        fs::create_dir_all(home).unwrap();
        enforce_provider_home_permissions(home).unwrap();
    }

    #[test]
    fn semantic_merge_preserves_mcp_and_unknown_roots_then_is_byte_idempotent() {
        let root = temp_root("semantic");
        let home = root.join("codex-home");
        create_restrictive_home(&home);
        fs::write(
            home.join(CONFIG_FILE),
            "# Generated by agent-harness.\n\
             [features]\n\
             multi_agent = false\n\
             \n\
             [mcp_servers.\"generic-µ\"]\n\
             command = \"mock-mcp\"\n\
             args = [\"--stdio\"]\n\
             env = { TOKEN = \"fixture-secret\" }\n\
             \n\
             [extension_metadata]\n\
             owner = \"fixture\"\n",
        )
        .unwrap();
        let desired = desired(&root);
        let first = materialize_codex_config(options(&home, &desired)).unwrap();
        let first_bytes = fs::read(home.join(CONFIG_FILE)).unwrap();
        let second = materialize_codex_config(options(&home, &desired)).unwrap();
        let second_bytes = fs::read(home.join(CONFIG_FILE)).unwrap();

        assert_eq!(first.result, CodexConfigMaterializationResultV1::Updated);
        assert_eq!(first.extension_root_count, 1);
        assert_eq!(first.unknown_root_count, 1);
        assert_eq!(second.result, CodexConfigMaterializationResultV1::Unchanged);
        assert_eq!(first_bytes, second_bytes);
        let text = String::from_utf8(second_bytes).unwrap();
        assert!(text.contains("[mcp_servers.\"generic-µ\"]"));
        assert!(text.contains("TOKEN = \"fixture-secret\""));
        assert!(text.contains("[extension_metadata]"));
        assert!(text.contains("multi_agent = true"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn supported_toml_corpus_round_trips_semantically_and_byte_idempotently() {
        let cases = [
            (
                "quoted-unicode",
                "[mcp_servers.\"quoted server µ\"]\ncommand = \"fixture\"\nargs = [\"--stdio\"]\n",
            ),
            (
                "comments-whitespace",
                "# operator comment\n[mcp_servers.spaced] # inline comment\ncommand    =    \"fixture\"\nargs = [\n  \"--one\", # retained comment\n  \"--two\",\n]\n",
            ),
            (
                "nested-environment",
                "[mcp_servers.nested]\ncommand = \"fixture\"\n[mcp_servers.nested.env]\nTOKEN = \"secret-value\"\nMODE = \"fixture\"\n",
            ),
            (
                "multiline-unknown-root",
                "[mcp_servers.multiline]\ncommand = \"fixture\"\n\n[extension_metadata]\nnotes = \"\"\"\nline one\nline two µ\n\"\"\"\n",
            ),
        ];

        for (name, existing) in cases {
            let root = temp_root(name);
            let home = root.join("codex-home");
            create_restrictive_home(&home);
            fs::write(home.join(CONFIG_FILE), existing).unwrap();
            let desired = desired(&root);
            let first = materialize_codex_config(options(&home, &desired)).unwrap();
            let first_bytes = fs::read(home.join(CONFIG_FILE)).unwrap();
            let second = materialize_codex_config(options(&home, &desired)).unwrap();
            let second_bytes = fs::read(home.join(CONFIG_FILE)).unwrap();

            assert_eq!(
                first.result,
                CodexConfigMaterializationResultV1::Updated,
                "case={name}"
            );
            assert_eq!(
                second.result,
                CodexConfigMaterializationResultV1::Unchanged,
                "case={name}"
            );
            assert_eq!(first_bytes, second_bytes, "case={name}");
            let parsed = String::from_utf8(second_bytes)
                .unwrap()
                .parse::<DocumentMut>()
                .unwrap();
            validate_external_mcp_root(&parsed).unwrap();
            let _ = fs::remove_dir_all(root);
        }
    }

    #[test]
    fn generated_receipt_and_permissions_do_not_expose_extension_secret() {
        let root = temp_root("secret-receipt");
        let home = root.join("codex-home");
        create_restrictive_home(&home);
        let secret = "phase0-private-fixture-token";
        fs::write(
            home.join(CONFIG_FILE),
            format!("[mcp_servers.fixture]\ncommand=\"fixture\"\nenv={{TOKEN=\"{secret}\"}}\n"),
        )
        .unwrap();
        assert!(
            !private_file_permissions_are_restrictive(&home.join(CONFIG_FILE)).unwrap(),
            "fixture must begin with an inherited or otherwise non-exact file permission shape"
        );
        let desired = desired(&root);
        let receipt = materialize_codex_config(options(&home, &desired)).unwrap();
        let serialized = serde_json::to_string(&receipt).unwrap();

        assert!(!serialized.contains(secret));
        assert!(receipt.permissions_restrictive);
        assert!(private_file_permissions_are_restrictive(&home.join(CONFIG_FILE)).unwrap());
        assert!(private_file_permissions_are_restrictive(&home.join(ORIGINAL_FILE)).unwrap());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn generated_server_name_corpus_preserves_every_registration() {
        let root = temp_root("generated-server-names");
        let home = root.join("codex-home");
        create_restrictive_home(&home);
        let mut existing = String::new();
        for index in 0..64 {
            let server_id = format!("fixture {index:02} µ");
            existing.push_str(&format!(
                "[mcp_servers.{}]\ncommand = \"fixture-{index:02}\"\nargs = [\"--stdio\"]\n\n",
                serde_json::to_string(&server_id).unwrap()
            ));
        }
        fs::write(home.join(CONFIG_FILE), &existing).unwrap();
        let desired = desired(&root);
        materialize_codex_config(options(&home, &desired)).unwrap();
        let first = fs::read(home.join(CONFIG_FILE)).unwrap();
        materialize_codex_config(options(&home, &desired)).unwrap();
        let second = fs::read(home.join(CONFIG_FILE)).unwrap();
        let parsed = String::from_utf8(second.clone())
            .unwrap()
            .parse::<DocumentMut>()
            .unwrap();
        let servers = parsed["mcp_servers"].as_table_like().unwrap();

        assert_eq!(servers.len(), 64);
        assert_eq!(first, second);
        for index in 0..64 {
            assert!(servers.contains_key(&format!("fixture {index:02} µ")));
        }
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn malformed_or_unsupported_config_fails_without_touching_original_bytes() {
        for (name, existing) in [
            (
                "duplicate",
                "[mcp_servers.mock]\ncommand=\"one\"\ncommand=\"two\"\n",
            ),
            ("malformed", "[mcp_servers.mock\ncommand=\"one\"\n"),
            (
                "unsupported-http",
                "[mcp_servers.mock]\nurl=\"https://fixture.invalid/mcp\"\n",
            ),
        ] {
            let root = temp_root(name);
            let home = root.join("codex-home");
            create_restrictive_home(&home);
            fs::write(home.join(CONFIG_FILE), existing).unwrap();
            let desired = desired(&root);
            assert!(materialize_codex_config(options(&home, &desired)).is_err());
            assert_eq!(
                fs::read(home.join(CONFIG_FILE)).unwrap(),
                existing.as_bytes()
            );
            let _ = fs::remove_dir_all(root);
        }
    }

    #[test]
    fn permissive_existing_home_fails_without_implicit_migration_or_config_mutation() {
        let root = temp_root("permissive-existing-home");
        let home = root.join("codex-home");
        fs::create_dir_all(&home).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&home, fs::Permissions::from_mode(0o755)).unwrap();
        }
        let original = "[mcp_servers.fixture]\ncommand=\"fixture\"\n";
        fs::write(home.join(CONFIG_FILE), original).unwrap();
        assert!(!provider_home_permissions_are_restrictive(&home).unwrap());

        let error = materialize_codex_config(options(&home, &desired(&root))).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert_eq!(
            fs::read(home.join(CONFIG_FILE)).unwrap(),
            original.as_bytes()
        );
        assert!(!provider_home_permissions_are_restrictive(&home).unwrap());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn transaction_failpoints_restore_exact_prewrite_bytes() {
        for failpoint in [
            CodexConfigMaterializationFailpoint::AfterTempSync,
            CodexConfigMaterializationFailpoint::AfterReplace,
        ] {
            let root = temp_root(&format!("{failpoint:?}"));
            let home = root.join("codex-home");
            create_restrictive_home(&home);
            let original = "# Generated by agent-harness.\n[features]\nmulti_agent=false\n";
            fs::write(home.join(CONFIG_FILE), original).unwrap();
            let desired = desired(&root);
            let mut options = options(&home, &desired);
            options.failpoint = Some(failpoint);
            assert!(materialize_codex_config(options).is_err());
            assert_eq!(
                fs::read(home.join(CONFIG_FILE)).unwrap(),
                original.as_bytes()
            );
            let _ = fs::remove_dir_all(root);
        }
    }

    #[test]
    fn durable_original_snapshot_restores_pre_harness_config() {
        let root = temp_root("restore");
        let home = root.join("codex-home");
        create_restrictive_home(&home);
        let original = "# operator config\n[mcp_servers.mock]\ncommand=\"fixture-mcp\"\n";
        fs::write(home.join(CONFIG_FILE), original).unwrap();
        let desired = desired(&root);
        materialize_codex_config(options(&home, &desired)).unwrap();
        restore_original_codex_config(&home).unwrap();
        assert_eq!(
            fs::read(home.join(CONFIG_FILE)).unwrap(),
            original.as_bytes()
        );
        let _ = fs::remove_dir_all(root);
    }
}
