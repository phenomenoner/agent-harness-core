use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use serde::Serialize;

const SUPERVISE_DEPLOY_CANARY_SCHEMA: &str = "agent-harness.supervise-deploy-canary.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SuperviseDeployCanaryOptions {
    pub harness_home: PathBuf,
    pub current_binary: PathBuf,
    pub candidate_binary: PathBuf,
    pub fake_canary_passed: bool,
    pub live_canary_passed: Option<bool>,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SuperviseDeployCanaryReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub receipt_file: PathBuf,
    pub decision: SuperviseDeployDecision,
    pub current_binary: PathBuf,
    pub current_hash: Option<String>,
    pub candidate_binary: PathBuf,
    pub candidate_hash: Option<String>,
    pub fake_canary_passed: bool,
    pub live_canary_passed: Option<bool>,
    pub alert_required: bool,
    pub reason: String,
    pub at_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum SuperviseDeployDecision {
    Commit,
    Rollback,
}

pub fn record_supervise_deploy_canary(
    options: SuperviseDeployCanaryOptions,
) -> io::Result<SuperviseDeployCanaryReport> {
    let current_hash = file_hash(&options.current_binary)?;
    let candidate_hash = file_hash(&options.candidate_binary)?;
    let live_gate = options.live_canary_passed.unwrap_or(true);
    let decision = if options.fake_canary_passed && live_gate {
        SuperviseDeployDecision::Commit
    } else {
        SuperviseDeployDecision::Rollback
    };
    let reason = match decision {
        SuperviseDeployDecision::Commit => {
            "fake canary passed and optional live canary did not fail".to_string()
        }
        SuperviseDeployDecision::Rollback if !options.fake_canary_passed => {
            "fake canary failed; rollback required".to_string()
        }
        SuperviseDeployDecision::Rollback => {
            "optional live canary failed; rollback required".to_string()
        }
    };
    let receipt_file = options
        .harness_home
        .join("state")
        .join("supervisor")
        .join("deploy-canary-receipts.jsonl");
    let report = SuperviseDeployCanaryReport {
        schema: SUPERVISE_DEPLOY_CANARY_SCHEMA,
        harness_home: options.harness_home,
        receipt_file: receipt_file.clone(),
        decision,
        current_binary: options.current_binary,
        current_hash,
        candidate_binary: options.candidate_binary,
        candidate_hash,
        fake_canary_passed: options.fake_canary_passed,
        live_canary_passed: options.live_canary_passed,
        alert_required: decision == SuperviseDeployDecision::Rollback,
        reason,
        at_ms: options.now_ms,
    };
    append_json_line(&receipt_file, &report)?;
    Ok(report)
}

fn file_hash(path: &Path) -> io::Result<Option<String>> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    Ok(Some(fnv1a_64_hex(&bytes)))
}

fn fnv1a_64_hex(bytes: &[u8]) -> String {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

fn append_json_line(path: &Path, value: &impl Serialize) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    let line = serde_json::to_string(value).map_err(io::Error::other)?;
    writeln!(file, "{line}")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn canary_failure_records_rollback_with_hashes() {
        let root = temp_root("canary_failure_records_rollback_with_hashes");
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&root).unwrap();
        let current = root.join("agent-harness-current.exe");
        let candidate = root.join("agent-harness-candidate.exe");
        fs::write(&current, b"current").unwrap();
        fs::write(&candidate, b"candidate").unwrap();

        let report = record_supervise_deploy_canary(SuperviseDeployCanaryOptions {
            harness_home,
            current_binary: current,
            candidate_binary: candidate,
            fake_canary_passed: false,
            live_canary_passed: None,
            now_ms: 123,
        })
        .unwrap();

        assert_eq!(report.decision, SuperviseDeployDecision::Rollback);
        assert!(report.alert_required);
        assert!(report.current_hash.is_some());
        assert!(report.candidate_hash.is_some());
        let receipt_text = fs::read_to_string(report.receipt_file).unwrap();
        assert!(receipt_text.contains("\"decision\":\"rollback\""));

        let _ = fs::remove_dir_all(root);
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-deploy-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
