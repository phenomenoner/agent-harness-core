use std::fs::{self, File};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::process::Command;

use ring::digest;
use serde::{Deserialize, Serialize};

use crate::write_json_atomic;

pub const CODEX_BACKEND_PROVENANCE_SCHEMA: &str = "agent-harness.codex-backend-provenance.v1";
pub const REQUIRED_CODEX_BACKEND_VERSION: &str = "0.144.5";

#[derive(Debug, Clone)]
pub struct CodexBackendProvenanceProbeOptions {
    pub configured_path: PathBuf,
    pub deployment_owned_roots: Vec<PathBuf>,
    pub expected_executable_sha256: Option<String>,
    pub candidate_id: String,
    pub phase: String,
    pub codex_home: Option<PathBuf>,
    pub config_digest: Option<String>,
    pub parent_supervisor_id: Option<String>,
    pub parent_started_at_ms: Option<i64>,
    pub child_pid: Option<u32>,
    pub child_started_at_ms: Option<i64>,
    pub probed_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexBackendProvenanceReceiptV1 {
    pub schema: String,
    pub phase: String,
    pub candidate_id: String,
    pub configured_path: PathBuf,
    pub canonical_path: PathBuf,
    pub executable_sha256: String,
    pub required_version: String,
    pub observed_version: String,
    pub version_output: String,
    pub package_name: Option<String>,
    pub package_version: Option<String>,
    pub package_integrity: Option<String>,
    pub native_package_name: Option<String>,
    pub native_package_version: Option<String>,
    pub native_package_integrity: Option<String>,
    pub platform: String,
    pub architecture: String,
    pub codex_home: Option<PathBuf>,
    pub codex_home_digest: Option<String>,
    pub config_digest: Option<String>,
    pub parent_supervisor_id: Option<String>,
    pub parent_started_at_ms: Option<i64>,
    pub child_pid: Option<u32>,
    pub child_started_at_ms: Option<i64>,
    pub probed_at_ms: i64,
    pub probe_result: String,
}

#[derive(Debug, Clone, Default)]
struct NpmPackageProvenance {
    package_name: Option<String>,
    package_version: Option<String>,
    package_integrity: Option<String>,
    native_package_name: Option<String>,
    native_package_version: Option<String>,
    native_package_integrity: Option<String>,
}

pub fn probe_codex_backend_provenance(
    options: CodexBackendProvenanceProbeOptions,
) -> io::Result<CodexBackendProvenanceReceiptV1> {
    let canonical_path = fs::canonicalize(&options.configured_path)?;
    require_owned_path(&canonical_path, &options.deployment_owned_roots)?;
    let executable_sha256 = sha256_file(&canonical_path)?;
    if let Some(expected) = options.expected_executable_sha256.as_deref()
        && !executable_sha256.eq_ignore_ascii_case(expected)
    {
        return Err(io::Error::other(format!(
            "Codex executable SHA-256 drift: expected {}, observed {}",
            expected, executable_sha256
        )));
    }
    let output = Command::new(&canonical_path).arg("--version").output()?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "Codex version probe exited with {}",
            output.status
        )));
    }
    let version_output = String::from_utf8(output.stdout)
        .map_err(io::Error::other)?
        .trim()
        .to_string();
    let observed_version = parse_codex_version(&version_output)?;
    if observed_version != REQUIRED_CODEX_BACKEND_VERSION {
        return Err(io::Error::other(format!(
            "Codex version drift: required {}, observed {}",
            REQUIRED_CODEX_BACKEND_VERSION, observed_version
        )));
    }
    let npm = read_npm_package_provenance(&canonical_path).unwrap_or_default();
    if let Some(version) = npm.package_version.as_deref()
        && version != REQUIRED_CODEX_BACKEND_VERSION
    {
        return Err(io::Error::other(format!(
            "Codex package version drift: required {}, observed {}",
            REQUIRED_CODEX_BACKEND_VERSION, version
        )));
    }
    let codex_home = options
        .codex_home
        .as_deref()
        .map(fs::canonicalize)
        .transpose()?;
    let codex_home_digest = codex_home
        .as_deref()
        .map(|path| sha256_bytes(path.as_os_str().to_string_lossy().as_bytes()));
    validate_phase_requirements(
        &options.phase,
        options.expected_executable_sha256.as_deref(),
        &npm,
        codex_home.as_deref(),
        options.config_digest.as_deref(),
        options.parent_supervisor_id.as_deref(),
        options.parent_started_at_ms,
        options.child_pid,
        options.child_started_at_ms,
    )?;
    Ok(CodexBackendProvenanceReceiptV1 {
        schema: CODEX_BACKEND_PROVENANCE_SCHEMA.to_string(),
        phase: options.phase,
        candidate_id: options.candidate_id,
        configured_path: options.configured_path,
        canonical_path,
        executable_sha256,
        required_version: REQUIRED_CODEX_BACKEND_VERSION.to_string(),
        observed_version,
        version_output,
        package_name: npm.package_name,
        package_version: npm.package_version,
        package_integrity: npm.package_integrity,
        native_package_name: npm.native_package_name,
        native_package_version: npm.native_package_version,
        native_package_integrity: npm.native_package_integrity,
        platform: std::env::consts::OS.to_string(),
        architecture: std::env::consts::ARCH.to_string(),
        codex_home,
        codex_home_digest,
        config_digest: options.config_digest,
        parent_supervisor_id: options.parent_supervisor_id,
        parent_started_at_ms: options.parent_started_at_ms,
        child_pid: options.child_pid,
        child_started_at_ms: options.child_started_at_ms,
        probed_at_ms: options.probed_at_ms,
        probe_result: "ready".to_string(),
    })
}

#[allow(clippy::too_many_arguments)]
fn validate_phase_requirements(
    phase: &str,
    expected_executable_sha256: Option<&str>,
    npm: &NpmPackageProvenance,
    codex_home: Option<&Path>,
    config_digest: Option<&str>,
    parent_supervisor_id: Option<&str>,
    parent_started_at_ms: Option<i64>,
    child_pid: Option<u32>,
    child_started_at_ms: Option<i64>,
) -> io::Result<()> {
    if !matches!(phase, "install" | "candidate" | "startup") {
        return Err(io::Error::other(format!(
            "unsupported Codex provenance phase: {phase}"
        )));
    }
    if phase == "install" {
        return Ok(());
    }
    if expected_executable_sha256.is_none()
        || codex_home.is_none()
        || config_digest.is_none_or(str::is_empty)
        || npm.package_version.is_none()
        || npm.package_integrity.is_none()
        || npm.native_package_version.is_none()
        || npm.native_package_integrity.is_none()
    {
        return Err(io::Error::other(
            "candidate/startup Codex provenance requires an expected executable SHA-256, npm/native package metadata, provider-scoped CODEX_HOME, and config digest",
        ));
    }
    if phase == "startup"
        && (parent_supervisor_id.is_none_or(str::is_empty)
            || parent_started_at_ms.is_none()
            || child_pid.is_none()
            || child_started_at_ms.is_none())
    {
        return Err(io::Error::other(
            "startup Codex provenance requires correlated parent and child lifecycle identity",
        ));
    }
    Ok(())
}

pub fn write_codex_backend_provenance_receipt(
    receipt_dir: impl AsRef<Path>,
    receipt: &CodexBackendProvenanceReceiptV1,
) -> io::Result<PathBuf> {
    if receipt.schema != CODEX_BACKEND_PROVENANCE_SCHEMA
        || receipt.observed_version != REQUIRED_CODEX_BACKEND_VERSION
        || receipt.probe_result != "ready"
    {
        return Err(io::Error::other("invalid Codex backend provenance receipt"));
    }
    let file = receipt_dir.as_ref().join(format!(
        "{}-{}.json",
        safe_component(&receipt.phase),
        &receipt.executable_sha256[..16]
    ));
    write_json_atomic(&file, receipt)?;
    Ok(file)
}

fn require_owned_path(path: &Path, roots: &[PathBuf]) -> io::Result<()> {
    if roots.is_empty() {
        return Err(io::Error::other(
            "no deployment-owned executable root was supplied",
        ));
    }
    for root in roots {
        let Ok(root) = fs::canonicalize(root) else {
            continue;
        };
        if path.starts_with(root) {
            return Ok(());
        }
    }
    Err(io::Error::other(format!(
        "Codex executable {} is outside every deployment-owned root",
        path.display()
    )))
}

fn parse_codex_version(output: &str) -> io::Result<String> {
    let Some(version) = output.trim().strip_prefix("codex-cli ") else {
        return Err(io::Error::other(format!(
            "unexpected Codex version output: {output}"
        )));
    };
    if version.is_empty()
        || !version
            .split('.')
            .all(|part| !part.is_empty() && part.chars().all(|ch| ch.is_ascii_digit()))
    {
        return Err(io::Error::other(format!(
            "invalid Codex semantic version: {version}"
        )));
    }
    Ok(version.to_string())
}

fn read_npm_package_provenance(executable: &Path) -> io::Result<NpmPackageProvenance> {
    let node_modules = executable
        .ancestors()
        .find(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.eq_ignore_ascii_case("node_modules"))
        })
        .ok_or_else(|| io::Error::other("Codex executable is not under node_modules"))?;
    let lock_file = node_modules
        .parent()
        .ok_or_else(|| io::Error::other("node_modules has no package root"))?
        .join("package-lock.json");
    let value: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(lock_file)?).map_err(io::Error::other)?;
    let packages = value
        .get("packages")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| io::Error::other("package-lock has no packages object"))?;
    let base = packages.get("node_modules/@openai/codex");
    let (npm_os, npm_arch) = npm_platform_key();
    let native_key = format!("node_modules/@openai/codex-{npm_os}-{npm_arch}");
    let native = packages.get(&native_key);
    Ok(NpmPackageProvenance {
        package_name: Some("@openai/codex".to_string()),
        package_version: json_string(base, "version"),
        package_integrity: json_string(base, "integrity"),
        native_package_name: json_string(native, "name"),
        native_package_version: json_string(native, "version"),
        native_package_integrity: json_string(native, "integrity"),
    })
}

fn npm_platform_key() -> (&'static str, &'static str) {
    let os = match std::env::consts::OS {
        "windows" => "win32",
        other => other,
    };
    let arch = match std::env::consts::ARCH {
        "x86_64" => "x64",
        "aarch64" => "arm64",
        other => other,
    };
    (os, arch)
}

fn json_string(value: Option<&serde_json::Value>, field: &str) -> Option<String> {
    value?
        .get(field)
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string)
}

fn sha256_file(path: &Path) -> io::Result<String> {
    let mut file = File::open(path)?;
    let mut context = digest::Context::new(&digest::SHA256);
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        context.update(&buffer[..read]);
    }
    Ok(lower_hex(context.finish().as_ref()))
}

fn sha256_bytes(value: &[u8]) -> String {
    lower_hex(digest::digest(&digest::SHA256, value).as_ref())
}

fn lower_hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

fn safe_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_') {
                ch
            } else {
                '_'
            }
        })
        .take(80)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_root(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("codex-backend-provenance-{name}-{nanos}"))
    }

    #[test]
    fn exact_version_parser_rejects_drift_and_suffixes() {
        assert_eq!(parse_codex_version("codex-cli 0.144.5").unwrap(), "0.144.5");
        assert!(parse_codex_version("codex-cli 0.144.5-beta").is_err());
        assert!(parse_codex_version("0.144.5").is_err());
    }

    #[test]
    fn owned_path_gate_rejects_global_or_sibling_executable() {
        let root = temp_root("owned-path");
        let owned = root.join("deployment");
        let sibling = root.join("global");
        fs::create_dir_all(&owned).unwrap();
        fs::create_dir_all(&sibling).unwrap();
        let owned_exe = owned.join("codex.exe");
        let global_exe = sibling.join("codex.exe");
        fs::write(&owned_exe, b"owned").unwrap();
        fs::write(&global_exe, b"global").unwrap();
        assert!(require_owned_path(&fs::canonicalize(owned_exe).unwrap(), &[owned]).is_ok());
        assert!(
            require_owned_path(
                &fs::canonicalize(global_exe).unwrap(),
                &[root.join("deployment")]
            )
            .is_err()
        );
        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn candidate_and_startup_phase_requirements_fail_closed() {
        let npm = NpmPackageProvenance::default();
        assert!(
            validate_phase_requirements(
                "candidate",
                None,
                &npm,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .is_err()
        );
        assert!(
            validate_phase_requirements(
                "unexpected",
                None,
                &npm,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .is_err()
        );
    }
}
