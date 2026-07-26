use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::fs;
use std::io::{self, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::str::FromStr;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::logging::{append_jsonl_value, write_json_atomic};
use crate::loop_health::{process_alive_for_pid, process_start_time_ms_for_pid};
use crate::process_launch::{
    MasterProcessTreeContainment, ProcessTreeContainmentCapability,
    configure_noninteractive_command,
};
use crate::supervision::SupervisorRestartPolicy;
use crate::task_transition::sha256_hex;

pub const SUPERVISOR_MASTER_STATE_SCHEMA: &str = "agent-harness.supervisor-master-state.v1";
pub const SUPERVISOR_MASTER_EVENT_SCHEMA: &str = "agent-harness.supervisor-master-event.v1";
pub const SUPERVISOR_MASTER_LEASE_SCHEMA: &str = "agent-harness.supervisor-owner-lease.v1";
pub const SUPERVISOR_MASTER_OBSERVATION_SCHEMA: &str =
    "agent-harness.supervisor-master-observation.v1";
pub const SUPERVISOR_COMPATIBILITY_IMPORT_SCHEMA: &str =
    "agent-harness.supervisor-compatibility-import.v1";
pub const SUPERVISOR_COMPATIBILITY_IMPORT_RECEIPT_SCHEMA: &str =
    "agent-harness.supervisor-compatibility-import-receipt.v1";
pub const SUPERVISOR_CONTROL_REQUEST_SCHEMA: &str = "agent-harness.supervisor-control-request.v1";
pub const SUPERVISOR_CONTROL_RESPONSE_SCHEMA: &str = "agent-harness.supervisor-control-response.v1";
pub const STANDALONE_MASTER_LAUNCH_OWNER: &str = "rust-standalone-master";
pub const STANDALONE_MASTER_STATE_WRITER_ENV: &str =
    "AGENT_HARNESS_SUPERVISOR_SERVICE_STATE_WRITER";
const SUPERVISOR_OWNERSHIP_LEASE_FILE: &str = "ownership.lock";
const HEARTBEAT_FUTURE_SKEW_FLOOR_MS: i64 = 5_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SupervisorOwnershipMode {
    Compatibility,
    Observe,
    DirectOwner,
}

impl SupervisorOwnershipMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Compatibility => "compatibility",
            Self::Observe => "observe",
            Self::DirectOwner => "direct-owner",
        }
    }
}

impl fmt::Display for SupervisorOwnershipMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for SupervisorOwnershipMode {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "compatibility" => Ok(Self::Compatibility),
            "observe" => Ok(Self::Observe),
            "direct-owner" => Ok(Self::DirectOwner),
            other => Err(format!(
                "unsupported supervisor ownership mode `{other}`; expected compatibility, observe, or direct-owner"
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SupervisorMasterPhase {
    Starting,
    Running,
    Draining,
    Stopping,
    Stopped,
    Failed,
    Observed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SupervisorChildActualState {
    Absent,
    Starting,
    Running,
    Unhealthy,
    Backoff,
    BreakerOpen,
    Draining,
    Stopping,
    Stopped,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SupervisorControlAction {
    Status,
    Stop,
    Restart,
    Drain,
}

/// Transport-neutral S0 control contract. Host-specific named-pipe and Unix
/// socket transports must authenticate this envelope and must never persist
/// `auth_proof`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorControlRequestV1 {
    pub schema: String,
    pub request_id: String,
    pub expected_master_generation_id: String,
    pub action: SupervisorControlAction,
    pub service_id: Option<String>,
    pub idempotency_key: String,
    pub issued_at_ms: i64,
    pub auth_proof: String,
}

impl SupervisorControlRequestV1 {
    pub fn validate_shape(&self) -> Result<(), String> {
        if self.schema != SUPERVISOR_CONTROL_REQUEST_SCHEMA {
            return Err("supervisor control request schema is unsupported".to_string());
        }
        for (label, value, max) in [
            ("requestId", self.request_id.as_str(), 128usize),
            (
                "expectedMasterGenerationId",
                self.expected_master_generation_id.as_str(),
                256,
            ),
            ("idempotencyKey", self.idempotency_key.as_str(), 256),
            ("authProof", self.auth_proof.as_str(), 1024),
        ] {
            if value.trim().is_empty() || value.len() > max {
                return Err(format!("{label} must contain between 1 and {max} bytes"));
            }
        }
        if let Some(service_id) = &self.service_id {
            validate_service_id(service_id)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorControlResponseV1 {
    pub schema: String,
    pub request_id: String,
    pub master_generation_id: String,
    pub accepted: bool,
    pub status: String,
    pub reason: String,
    pub completed_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorMasterIdentity {
    pub pid: u32,
    pub process_start_time_ms: i64,
    pub generation_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StandaloneMasterChildSpec {
    pub service_id: String,
    pub service_kind: String,
    pub executable: PathBuf,
    pub args: Vec<String>,
    pub working_directory: Option<PathBuf>,
    pub stop_file: PathBuf,
    pub heartbeat_ttl_ms: i64,
    pub restart: SupervisorRestartPolicy,
    pub environment: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StandaloneMasterOptions {
    pub harness_home: PathBuf,
    pub mode: SupervisorOwnershipMode,
    pub stop_file: PathBuf,
    pub poll_interval_ms: u64,
    pub drain_timeout_ms: u64,
    /// Zero means run until a stop request.
    pub max_iterations: usize,
    /// Direct-owner production execution requires `TreeKillOnOwnerExit`.
    pub require_tree_containment: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StandaloneMasterRunReport {
    pub schema: &'static str,
    pub mode: SupervisorOwnershipMode,
    pub generation_id: String,
    pub pid: u32,
    pub process_start_time_ms: i64,
    pub containment: ProcessTreeContainmentCapability,
    pub desired_service_count: usize,
    pub stopped_service_count: usize,
    pub forced_service_count: usize,
    pub final_phase: SupervisorMasterPhase,
    pub state_file: PathBuf,
    pub event_file: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorMasterObservation {
    pub schema: &'static str,
    pub mode: SupervisorOwnershipMode,
    pub desired_service_count: usize,
    pub desired_service_ids: Vec<String>,
    pub spec_digest: String,
    pub compatibility: CompatibilityImportAssessment,
    pub at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CompatibilityImportAssessment {
    pub schema: &'static str,
    pub status: CompatibilityImportStatus,
    pub inspected_file_count: usize,
    pub accepted_file_count: usize,
    pub blockers: Vec<CompatibilityImportBlocker>,
    pub snapshots: Vec<CompatibilityImportSnapshot>,
    pub at_ms: i64,
}

impl CompatibilityImportAssessment {
    pub fn is_safe_for_direct_owner(&self) -> bool {
        self.status == CompatibilityImportStatus::Accepted && self.blockers.is_empty()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CompatibilityImportStatus {
    Accepted,
    Blocked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CompatibilityImportBlocker {
    pub service_id: Option<String>,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CompatibilityImportSnapshot {
    pub service_id: String,
    pub source_digest: String,
    pub prior_launch_owner: Option<String>,
    pub prior_actual_state: Option<String>,
    pub owner_pid: Option<i64>,
    pub child_pid: Option<i64>,
    pub adopted_process_identity: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SupervisorOwnershipLeaseKind {
    CompatibilityShared,
    DirectExclusive,
}

#[derive(Debug)]
pub struct SupervisorOwnershipLease {
    file: fs::File,
    #[cfg(unix)]
    locked: bool,
}

impl SupervisorOwnershipLease {
    pub fn acquire(
        harness_home: &Path,
        kind: SupervisorOwnershipLeaseKind,
        identity: &SupervisorMasterIdentity,
    ) -> Result<Self, String> {
        let path = harness_home
            .join("state")
            .join("supervisor")
            .join(SUPERVISOR_OWNERSHIP_LEASE_FILE);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                format!(
                    "failed to create supervisor ownership lease directory {}: {error}",
                    parent.display()
                )
            })?;
        }

        #[cfg(windows)]
        let mut file = {
            use std::os::windows::fs::OpenOptionsExt;
            const FILE_SHARE_READ_WRITE_DELETE: u32 = 0x0000_0001 | 0x0000_0002 | 0x0000_0004;
            let share_mode = match kind {
                SupervisorOwnershipLeaseKind::CompatibilityShared => FILE_SHARE_READ_WRITE_DELETE,
                SupervisorOwnershipLeaseKind::DirectExclusive => 0,
            };
            fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .share_mode(share_mode)
                .open(&path)
                .map_err(|error| {
                    format!(
                        "supervisor ownership lease {kind:?} is already held or unavailable at {}: {error}",
                        path.display()
                    )
                })?
        };

        #[cfg(unix)]
        let mut file = {
            use std::os::fd::AsRawFd;

            let file = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&path)
                .map_err(|error| {
                    format!(
                        "failed to open supervisor ownership lease {}: {error}",
                        path.display()
                    )
                })?;
            const LOCK_SH: i32 = 1;
            const LOCK_EX: i32 = 2;
            const LOCK_NB: i32 = 4;
            unsafe extern "C" {
                fn flock(fd: i32, operation: i32) -> i32;
            }
            let operation = match kind {
                SupervisorOwnershipLeaseKind::CompatibilityShared => LOCK_SH,
                SupervisorOwnershipLeaseKind::DirectExclusive => LOCK_EX,
            };
            if unsafe { flock(file.as_raw_fd(), operation | LOCK_NB) } != 0 {
                return Err(format!(
                    "supervisor ownership lease {kind:?} is already held or unavailable at {}: {}",
                    path.display(),
                    io::Error::last_os_error()
                ));
            }
            file
        };

        #[cfg(not(any(windows, unix)))]
        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|error| {
                format!(
                    "supervisor ownership lease {kind:?} is already held or unavailable at {}: {error}",
                    path.display()
                )
            })?;

        if kind == SupervisorOwnershipLeaseKind::DirectExclusive {
            let envelope = serde_json::to_vec_pretty(&json!({
                "schema": SUPERVISOR_MASTER_LEASE_SCHEMA,
                "kind": "direct-exclusive",
                "pid": identity.pid,
                "processStartTimeMs": identity.process_start_time_ms,
                "generationId": identity.generation_id,
            }))
            .map_err(|error| format!("failed to serialize supervisor ownership lease: {error}"))?;
            file.set_len(0)
                .and_then(|()| file.seek(SeekFrom::Start(0)).map(|_| ()))
                .and_then(|()| file.write_all(&envelope))
                .and_then(|()| file.sync_all())
                .map_err(|error| {
                    format!(
                        "failed to persist supervisor ownership lease {}: {error}",
                        path.display()
                    )
                })?;
        }
        Ok(Self {
            file,
            #[cfg(unix)]
            locked: true,
        })
    }
}

impl Drop for SupervisorOwnershipLease {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            const LOCK_UN: i32 = 8;
            unsafe extern "C" {
                fn flock(fd: i32, operation: i32) -> i32;
            }
            if self.locked {
                let _ = unsafe { flock(self.file.as_raw_fd(), LOCK_UN) };
                self.locked = false;
            }
        }
        let _ = self.file.sync_all();
        // Any informational envelope intentionally remains. Ownership comes
        // from the OS-held handle/lock, never from file existence.
    }
}

#[derive(Debug)]
struct ManagedChild {
    spec: StandaloneMasterChildSpec,
    child: Option<Child>,
    attempt_ordinal: usize,
    attempt_generation_id: Option<String>,
    process_id: Option<u32>,
    process_start_time_ms: Option<i64>,
    started_at_ms: Option<i64>,
    last_heartbeat_at_ms: Option<i64>,
    last_exit_at_ms: Option<i64>,
    last_exit_code: Option<i32>,
    restart_count: usize,
    recent_failures: VecDeque<i64>,
    backoff_until_ms: Option<i64>,
    actual_state: SupervisorChildActualState,
    detail: String,
    forced: bool,
    ephemeral_stop_created: bool,
}

impl ManagedChild {
    fn new(spec: StandaloneMasterChildSpec) -> Self {
        Self {
            spec,
            child: None,
            attempt_ordinal: 0,
            attempt_generation_id: None,
            process_id: None,
            process_start_time_ms: None,
            started_at_ms: None,
            last_heartbeat_at_ms: None,
            last_exit_at_ms: None,
            last_exit_code: None,
            restart_count: 0,
            recent_failures: VecDeque::new(),
            backoff_until_ms: None,
            actual_state: SupervisorChildActualState::Absent,
            detail: "child has not started".to_string(),
            forced: false,
            ephemeral_stop_created: false,
        }
    }

    fn breaker_open(&mut self, now_ms: i64) -> bool {
        let window_start = now_ms.saturating_sub(self.spec.restart.crash_loop_window_ms.max(0));
        while self
            .recent_failures
            .front()
            .is_some_and(|failure| *failure < window_start)
        {
            self.recent_failures.pop_front();
        }
        self.recent_failures.len() >= self.spec.restart.crash_loop_threshold.max(1)
    }

    fn schedule_restart(&mut self, now_ms: i64, detail: String) {
        self.restart_count = self.restart_count.saturating_add(1);
        self.recent_failures.push_back(now_ms);
        if !self.spec.restart.enabled || self.breaker_open(now_ms) {
            self.actual_state = SupervisorChildActualState::BreakerOpen;
            self.backoff_until_ms = None;
            self.detail = format!("{detail}; crash-loop breaker is open");
            return;
        }
        let shift = self.restart_count.saturating_sub(1).min(20) as u32;
        let delay = self
            .spec
            .restart
            .base_backoff_ms
            .max(0)
            .saturating_mul(1_i64 << shift)
            .min(self.spec.restart.max_backoff_ms.max(0));
        self.backoff_until_ms = Some(now_ms.saturating_add(delay));
        self.actual_state = SupervisorChildActualState::Backoff;
        self.detail = format!("{detail}; restart {0} after {delay}ms", self.restart_count);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DirectOwnerConfigFence {
    digest: String,
}

impl DirectOwnerConfigFence {
    fn read(harness_home: &Path) -> Result<Self, String> {
        let config_file = harness_home.join("harness-config.json");
        let bytes = fs::read(&config_file).map_err(|error| {
            format!(
                "failed to read direct-owner configuration {}: {error}",
                config_file.display()
            )
        })?;
        let value: Value = serde_json::from_slice(&bytes).map_err(|error| {
            format!(
                "direct-owner configuration is invalid JSON at {}: {error}",
                config_file.display()
            )
        })?;
        let mode = value
            .pointer("/supervisor/ownershipMode")
            .and_then(Value::as_str)
            .unwrap_or("compatibility");
        if mode != SupervisorOwnershipMode::DirectOwner.as_str() {
            return Err(format!(
                "direct-owner runtime fence requires supervisor.ownershipMode=direct-owner, observed {mode}"
            ));
        }
        if value
            .pointer("/supervisor/autoReconcile")
            .and_then(Value::as_bool)
            == Some(true)
        {
            return Err(
                "direct-owner runtime fence requires supervisor.autoReconcile=false".to_string(),
            );
        }
        Ok(Self {
            digest: format!("sha256:{}", sha256_hex(&bytes)),
        })
    }

    fn verify(&self, harness_home: &Path) -> Result<(), String> {
        let current = Self::read(harness_home)?;
        if current.digest != self.digest {
            return Err(format!(
                "direct-owner runtime fence detected a configuration generation change: expected {}, observed {}",
                self.digest, current.digest
            ));
        }
        Ok(())
    }
}

pub fn inspect_compatibility_state(
    harness_home: &Path,
    now_ms: i64,
) -> CompatibilityImportAssessment {
    let services_dir = harness_home
        .join("state")
        .join("supervisor")
        .join("services");
    let mut assessment = CompatibilityImportAssessment {
        schema: SUPERVISOR_COMPATIBILITY_IMPORT_SCHEMA,
        status: CompatibilityImportStatus::Accepted,
        inspected_file_count: 0,
        accepted_file_count: 0,
        blockers: Vec::new(),
        snapshots: Vec::new(),
        at_ms: now_ms,
    };
    let entries = match fs::read_dir(&services_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return assessment,
        Err(error) => {
            assessment.blockers.push(CompatibilityImportBlocker {
                service_id: None,
                reason: format!(
                    "failed to enumerate compatibility service state {}: {error}",
                    services_dir.display()
                ),
            });
            assessment.status = CompatibilityImportStatus::Blocked;
            return assessment;
        }
    };

    let mut paths = entries
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.extension().and_then(|extension| extension.to_str()) == Some("json")
                && path.is_file()
        })
        .collect::<Vec<_>>();
    paths.sort();

    for path in paths {
        let blocker_count_before = assessment.blockers.len();
        assessment.inspected_file_count += 1;
        let file_stem = path
            .file_stem()
            .and_then(|name| name.to_str())
            .map(ToString::to_string);
        let text = match fs::read_to_string(&path) {
            Ok(text) => text,
            Err(error) => {
                assessment.blockers.push(CompatibilityImportBlocker {
                    service_id: file_stem,
                    reason: format!(
                        "failed to read canonical compatibility state {}: {error}",
                        path.display()
                    ),
                });
                continue;
            }
        };
        let value = match serde_json::from_str::<Value>(&text) {
            Ok(value) => value,
            Err(error) => {
                assessment.blockers.push(CompatibilityImportBlocker {
                    service_id: file_stem,
                    reason: format!(
                        "canonical compatibility state is malformed at {}: {error}",
                        path.display()
                    ),
                });
                continue;
            }
        };
        let service_id = value
            .get("serviceId")
            .and_then(Value::as_str)
            .map(ToString::to_string);
        if value.get("schema").and_then(Value::as_str)
            != Some("agent-harness.supervisor-service-state.v1")
        {
            assessment.blockers.push(CompatibilityImportBlocker {
                service_id,
                reason: format!(
                    "canonical compatibility state has an unsupported schema at {}",
                    path.display()
                ),
            });
            continue;
        }
        let Some(service_id) = service_id else {
            assessment.blockers.push(CompatibilityImportBlocker {
                service_id: None,
                reason: format!(
                    "canonical compatibility state has no serviceId at {}",
                    path.display()
                ),
            });
            continue;
        };
        if file_stem.as_deref() != Some(service_id.as_str()) {
            assessment.blockers.push(CompatibilityImportBlocker {
                service_id: Some(service_id),
                reason: format!(
                    "canonical compatibility state identity does not match filename at {}",
                    path.display()
                ),
            });
            continue;
        }
        let prior_actual_state = value
            .get("actualState")
            .and_then(Value::as_str)
            .map(ToString::to_string);
        let prior_launch_owner = value
            .get("launchOwner")
            .and_then(Value::as_str)
            .map(ToString::to_string);
        if !matches!(
            prior_launch_owner.as_deref(),
            Some("rust-supervisor-run") | Some(STANDALONE_MASTER_LAUNCH_OWNER)
        ) {
            assessment.blockers.push(CompatibilityImportBlocker {
                service_id: Some(service_id),
                reason: format!(
                    "canonical supervisor state at {} is not provenance-bound to an allowed managed owner",
                    path.display()
                ),
            });
            continue;
        }
        let owner_pid = value
            .get("supervisorPid")
            .and_then(Value::as_i64)
            .or_else(|| value.get("parentPid").and_then(Value::as_i64));
        let owner_start = value
            .get("supervisorProcessStartTimeMs")
            .and_then(Value::as_i64);
        let child_pid = value
            .get("processId")
            .and_then(Value::as_i64)
            .or_else(|| value.get("pid").and_then(Value::as_i64));
        let child_start = value.get("processStartTimeMs").and_then(Value::as_i64);
        let prior_child_exit_proven = prior_actual_state.as_deref() == Some("draining")
            && child_pid.is_none()
            && child_start.is_none()
            && value
                .get("lastExitAtMs")
                .and_then(Value::as_i64)
                .is_some_and(|at_ms| at_ms > 0)
            && value.get("lastExitCode").and_then(Value::as_i64).is_some();
        let is_stopped_state = prior_actual_state.as_deref() == Some("stopped");
        let is_known_direct_crash_state = matches!(
            prior_actual_state.as_deref(),
            Some(
                "starting"
                    | "running"
                    | "restart-backoff"
                    | "unhealthy"
                    | "breaker-open"
                    | "draining"
            )
        );
        if !is_stopped_state
            && (prior_launch_owner.as_deref() != Some(STANDALONE_MASTER_LAUNCH_OWNER)
                || !is_known_direct_crash_state)
        {
            assessment.blockers.push(CompatibilityImportBlocker {
                service_id: Some(service_id),
                reason: format!(
                    "canonical compatibility state at {} is not in the stopped-state allowlist or a known direct-owner crash-recovery state",
                    path.display()
                ),
            });
            continue;
        }
        if !is_stopped_state {
            if owner_pid.filter(|pid| *pid > 0).is_none()
                || owner_start.filter(|started_at| *started_at > 0).is_none()
            {
                assessment.blockers.push(CompatibilityImportBlocker {
                    service_id: Some(service_id.clone()),
                    reason: format!(
                        "direct-owner crash state at {} lacks an exact prior master identity",
                        path.display()
                    ),
                });
            }
            let child_identity_required = matches!(
                prior_actual_state.as_deref(),
                Some("starting" | "running" | "unhealthy")
            ) || (prior_actual_state.as_deref() == Some("draining")
                && !prior_child_exit_proven);
            if child_identity_required
                && (child_pid.filter(|pid| *pid > 0).is_none()
                    || child_start.filter(|started_at| *started_at > 0).is_none())
            {
                assessment.blockers.push(CompatibilityImportBlocker {
                    service_id: Some(service_id.clone()),
                    reason: format!(
                        "direct-owner crash state at {} lacks an exact prior child identity",
                        path.display()
                    ),
                });
            } else if child_pid.filter(|pid| *pid > 0).is_some()
                && child_start.filter(|started_at| *started_at > 0).is_none()
            {
                assessment.blockers.push(CompatibilityImportBlocker {
                    service_id: Some(service_id.clone()),
                    reason: format!(
                        "direct-owner crash state at {} has a child PID without process-start identity",
                        path.display()
                    ),
                });
            }
        }
        for (role, pid, expected_start) in [
            ("owner", owner_pid, owner_start),
            ("child", child_pid, child_start),
        ] {
            let Some(pid) = pid.filter(|pid| *pid > 0) else {
                continue;
            };
            match process_alive_for_pid(pid) {
                Some(false) => {}
                Some(true) => {
                    let observed_start = process_start_time_ms_for_pid(pid);
                    let identity = match (expected_start, observed_start) {
                        (Some(expected), Some(observed)) if expected == observed => "exact-live",
                        (Some(_), Some(_)) => "pid-reused",
                        _ => "live-identity-ambiguous",
                    };
                    assessment.blockers.push(CompatibilityImportBlocker {
                        service_id: Some(service_id.clone()),
                        reason: format!(
                            "managed {role} pid {pid} is {identity}; direct-owner import cannot adopt or coexist with a live process"
                        ),
                    });
                }
                None => assessment.blockers.push(CompatibilityImportBlocker {
                    service_id: Some(service_id.clone()),
                    reason: format!(
                        "managed {role} pid {pid} liveness is unknown; direct-owner import fails closed"
                    ),
                }),
            }
        }
        if assessment.blockers.len() != blocker_count_before {
            continue;
        }
        assessment.accepted_file_count += 1;
        assessment.snapshots.push(CompatibilityImportSnapshot {
            service_id,
            source_digest: format!("sha256:{}", sha256_hex(text.as_bytes())),
            prior_launch_owner,
            prior_actual_state,
            owner_pid,
            child_pid,
            adopted_process_identity: false,
        });
    }
    if !assessment.blockers.is_empty() {
        assessment.status = CompatibilityImportStatus::Blocked;
    }
    assessment
}

pub fn observe_standalone_master(
    harness_home: &Path,
    specs: &[StandaloneMasterChildSpec],
) -> Result<SupervisorMasterObservation, String> {
    validate_specs(specs)?;
    let now_ms = current_time_ms()?;
    let observation = SupervisorMasterObservation {
        schema: SUPERVISOR_MASTER_OBSERVATION_SCHEMA,
        mode: SupervisorOwnershipMode::Observe,
        desired_service_count: specs.len(),
        desired_service_ids: specs.iter().map(|spec| spec.service_id.clone()).collect(),
        spec_digest: master_spec_digest(specs)?,
        compatibility: inspect_compatibility_state(harness_home, now_ms),
        at_ms: now_ms,
    };
    let observation_dir = master_state_dir(harness_home).join("observations");
    fs::create_dir_all(&observation_dir).map_err(|error| {
        format!(
            "failed to create standalone master observation directory {}: {error}",
            observation_dir.display()
        )
    })?;
    let observation_file = observation_dir.join(format!("observation-{now_ms}.json"));
    write_json_atomic(&observation_file, &observation).map_err(|error| {
        format!(
            "failed to write standalone master observation {}: {error}",
            observation_file.display()
        )
    })?;
    Ok(observation)
}

pub fn run_standalone_master(
    options: StandaloneMasterOptions,
    specs: Vec<StandaloneMasterChildSpec>,
) -> Result<StandaloneMasterRunReport, String> {
    if options.mode != SupervisorOwnershipMode::DirectOwner {
        return Err(format!(
            "standalone master execution requires mode direct-owner, got {}",
            options.mode
        ));
    }
    if options.poll_interval_ms == 0 {
        return Err("standalone master poll interval must be greater than zero".to_string());
    }
    if options.drain_timeout_ms == 0 {
        return Err("standalone master drain timeout must be greater than zero".to_string());
    }
    validate_specs(&specs)?;
    #[cfg(not(windows))]
    if options.mode == SupervisorOwnershipMode::DirectOwner {
        return Err(
            "direct-owner activation requires tree-kill-on-owner-exit containment; this host rejects the mode before state mutation"
                .to_string(),
        );
    }
    for spec in &specs {
        if spec.stop_file.is_file() {
            return Err(format!(
                "standalone master refused start because service hold exists at {}",
                spec.stop_file.display()
            ));
        }
    }
    if options.stop_file.is_file() {
        return Err(format!(
            "standalone master refused start because master hold exists at {}",
            options.stop_file.display()
        ));
    }

    let config_fence = DirectOwnerConfigFence::read(&options.harness_home)?;
    let spec_digest = master_spec_digest(&specs)?;
    let binary_digest = current_executable_digest()?;
    let identity = current_master_identity()?;
    let _lease = SupervisorOwnershipLease::acquire(
        &options.harness_home,
        SupervisorOwnershipLeaseKind::DirectExclusive,
        &identity,
    )?;
    config_fence.verify(&options.harness_home)?;

    let compatibility = inspect_compatibility_state(&options.harness_home, current_time_ms()?);
    let master_dir = master_state_dir(&options.harness_home);
    fs::create_dir_all(&master_dir).map_err(|error| {
        format!(
            "failed to create standalone master state directory {}: {error}",
            master_dir.display()
        )
    })?;
    let import_dir = master_dir.join("compatibility-imports");
    fs::create_dir_all(&import_dir).map_err(|error| {
        format!(
            "failed to create compatibility import receipt directory {}: {error}",
            import_dir.display()
        )
    })?;
    let import_file = import_dir.join(format!(
        "compatibility-import-{}.json",
        identity.generation_id
    ));
    let import_receipt = json!({
        "schema": SUPERVISOR_COMPATIBILITY_IMPORT_RECEIPT_SCHEMA,
        "masterGenerationId": identity.generation_id,
        "specDigest": spec_digest,
        "configDigest": config_fence.digest,
        "binaryDigest": binary_digest,
        "assessment": compatibility,
        "atMs": current_time_ms()?,
    });
    write_json_atomic(&import_file, &import_receipt).map_err(|error| {
        format!(
            "failed to write compatibility import receipt {}: {error}",
            import_file.display()
        )
    })?;
    if !compatibility.is_safe_for_direct_owner() {
        return Err(format!(
            "standalone master compatibility import is blocked by {} issue(s)",
            compatibility.blockers.len()
        ));
    }

    let mut containment = MasterProcessTreeContainment::enter()
        .map_err(|error| format!("failed to enter master process-tree containment: {error}"))?;
    let capability = containment.capability();
    if options.require_tree_containment
        && capability != ProcessTreeContainmentCapability::TreeKillOnOwnerExit
    {
        containment
            .disarm_after_children_exit()
            .map_err(|error| format!("failed to disarm unused containment boundary: {error}"))?;
        return Err(format!(
            "direct-owner activation requires tree-kill-on-owner-exit containment; this host provides {capability:?}"
        ));
    }

    let state_file = master_dir.join("master-state.json");
    let event_file = master_dir.join("master-events.jsonl");
    let mut children = specs.into_iter().map(ManagedChild::new).collect::<Vec<_>>();
    write_master_state(
        &state_file,
        &identity,
        capability,
        SupervisorMasterPhase::Starting,
        0,
        &children,
        "standalone master acquired singleton and containment authority",
    )?;
    append_master_event(
        &event_file,
        &identity,
        "master-starting",
        None,
        "standalone master acquired singleton and containment authority",
    )?;

    let run_result = run_master_loop(
        &options,
        &config_fence,
        &identity,
        capability,
        &state_file,
        &event_file,
        &mut children,
    );
    let drain_result = drain_children(
        &options,
        &identity,
        capability,
        &state_file,
        &event_file,
        &mut children,
    );

    let run_error = run_result.err();
    let drain_error = drain_result.err();
    if let Some(error) = drain_error {
        let _ = write_master_state(
            &state_file,
            &identity,
            capability,
            SupervisorMasterPhase::Failed,
            0,
            &children,
            &error,
        );
        let _ = append_master_event(&event_file, &identity, "master-failed", None, &error);
        // Keeping the Job armed is intentional. Dropping it is the final
        // fail-closed tree cleanup if retained-handle drain proof failed.
        return Err(error);
    }

    if let Err(error) = containment.disarm_after_children_exit() {
        let error = format!("failed to prove/disarm empty master process tree: {error}");
        let _ = write_master_state(
            &state_file,
            &identity,
            capability,
            SupervisorMasterPhase::Failed,
            0,
            &children,
            &error,
        );
        let _ = append_master_event(&event_file, &identity, "master-failed", None, &error);
        return Err(error);
    }
    let final_phase = if run_error.is_some() {
        SupervisorMasterPhase::Failed
    } else {
        SupervisorMasterPhase::Stopped
    };
    let final_detail = run_error
        .as_deref()
        .unwrap_or("standalone master drained every retained child");
    write_master_state(
        &state_file,
        &identity,
        capability,
        final_phase,
        0,
        &children,
        final_detail,
    )?;
    append_master_event(
        &event_file,
        &identity,
        if run_error.is_some() {
            "master-failed"
        } else {
            "master-stopped"
        },
        None,
        final_detail,
    )?;
    if let Some(error) = run_error {
        return Err(error);
    }

    Ok(StandaloneMasterRunReport {
        schema: SUPERVISOR_MASTER_STATE_SCHEMA,
        mode: options.mode,
        generation_id: identity.generation_id,
        pid: identity.pid,
        process_start_time_ms: identity.process_start_time_ms,
        containment: capability,
        desired_service_count: children.len(),
        stopped_service_count: children
            .iter()
            .filter(|child| child.actual_state == SupervisorChildActualState::Stopped)
            .count(),
        forced_service_count: children.iter().filter(|child| child.forced).count(),
        final_phase,
        state_file,
        event_file,
    })
}

fn run_master_loop(
    options: &StandaloneMasterOptions,
    config_fence: &DirectOwnerConfigFence,
    identity: &SupervisorMasterIdentity,
    capability: ProcessTreeContainmentCapability,
    state_file: &Path,
    event_file: &Path,
    children: &mut [ManagedChild],
) -> Result<(), String> {
    for child in children.iter_mut() {
        start_managed_child(options, config_fence, identity, event_file, child)?;
        write_managed_child_state(&options.harness_home, identity, child)?;
    }
    let mut iteration = 0usize;
    loop {
        iteration = iteration.saturating_add(1);
        if options.stop_file.is_file() {
            return Ok(());
        }
        config_fence.verify(&options.harness_home)?;
        let now_ms = current_time_ms()?;
        for child in children.iter_mut() {
            poll_managed_child(&options.harness_home, identity, event_file, child, now_ms)?;
            if child.child.is_none()
                && child.actual_state == SupervisorChildActualState::Backoff
                && child
                    .backoff_until_ms
                    .is_some_and(|deadline| deadline <= now_ms)
                && !child.spec.stop_file.is_file()
            {
                start_managed_child(options, config_fence, identity, event_file, child)?;
            }
            write_managed_child_state(&options.harness_home, identity, child)?;
        }
        write_master_state(
            state_file,
            identity,
            capability,
            SupervisorMasterPhase::Running,
            iteration,
            children,
            "standalone master owns the desired direct child set",
        )?;
        if options.max_iterations > 0 && iteration >= options.max_iterations {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(options.poll_interval_ms));
    }
}

fn start_managed_child(
    options: &StandaloneMasterOptions,
    config_fence: &DirectOwnerConfigFence,
    identity: &SupervisorMasterIdentity,
    event_file: &Path,
    child: &mut ManagedChild,
) -> Result<(), String> {
    verify_child_spawn_authority(options, config_fence, child)?;
    if let Err(error) = spawn_managed_child(identity, event_file, child) {
        append_master_event(
            event_file,
            identity,
            "child-spawn-failed",
            Some(&child.spec.service_id),
            &error,
        )?;
        child.schedule_restart(current_time_ms()?, error);
        return Ok(());
    }
    if let Err(error) = verify_child_spawn_authority(options, config_fence, child) {
        terminate_spawned_child(child, current_time_ms()?)?;
        append_master_event(
            event_file,
            identity,
            "child-spawn-fenced",
            Some(&child.spec.service_id),
            &error,
        )?;
        return Err(error);
    }
    Ok(())
}

fn verify_child_spawn_authority(
    options: &StandaloneMasterOptions,
    config_fence: &DirectOwnerConfigFence,
    child: &ManagedChild,
) -> Result<(), String> {
    config_fence.verify(&options.harness_home)?;
    if options.stop_file.is_file() {
        return Err(format!(
            "standalone master stop intent appeared before spawning service `{}`",
            child.spec.service_id
        ));
    }
    if child.spec.stop_file.is_file() {
        return Err(format!(
            "service stop intent appeared before spawning service `{}` at {}",
            child.spec.service_id,
            child.spec.stop_file.display()
        ));
    }
    Ok(())
}

fn spawn_managed_child(
    identity: &SupervisorMasterIdentity,
    event_file: &Path,
    child: &mut ManagedChild,
) -> Result<(), String> {
    child.attempt_ordinal = child.attempt_ordinal.saturating_add(1);
    let started_at_ms = current_time_ms()?;
    let attempt_generation_id = format!(
        "{}:{}:{}",
        identity.generation_id, child.spec.service_id, child.attempt_ordinal
    );
    let mut command = Command::new(&child.spec.executable);
    command.envs(&child.spec.environment);
    command
        .args(&child.spec.args)
        .env(
            "AGENT_HARNESS_SERVICE_GENERATION_ID",
            &attempt_generation_id,
        )
        .env(
            "AGENT_HARNESS_SERVICE_STARTED_AT_MS",
            started_at_ms.to_string(),
        )
        .env(
            "AGENT_HARNESS_SUPERVISOR_LAUNCH_OWNER",
            STANDALONE_MASTER_LAUNCH_OWNER,
        )
        .env("AGENT_HARNESS_SUPERVISOR_OBSERVED_ONLY", "false")
        .env(
            "AGENT_HARNESS_SUPERVISOR_PARENT_PID",
            identity.pid.to_string(),
        )
        .env(
            "AGENT_HARNESS_SUPERVISOR_PROCESS_START_TIME_MS",
            identity.process_start_time_ms.to_string(),
        )
        .env(
            "AGENT_HARNESS_SUPERVISOR_STOP_FILE",
            child.spec.stop_file.display().to_string(),
        )
        .env(STANDALONE_MASTER_STATE_WRITER_ENV, "master")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let Some(working_directory) = &child.spec.working_directory {
        command.current_dir(working_directory);
    }
    configure_noninteractive_command(&mut command);
    let spawned = command.spawn().map_err(|error| {
        format!(
            "failed to start direct child {} with {}: {error}",
            child.spec.service_id,
            child.spec.executable.display()
        )
    })?;
    let process_id = spawned.id();
    let process_start_time_ms = process_start_time_ms_for_pid(i64::from(process_id));
    #[cfg(windows)]
    if process_start_time_ms.is_none() {
        let mut spawned = spawned;
        let _ = spawned.kill();
        let _ = spawned.wait();
        return Err(format!(
            "cannot prove exact direct child process start identity for {} pid={process_id}",
            child.spec.service_id
        ));
    }
    child.child = Some(spawned);
    child.process_id = Some(process_id);
    child.process_start_time_ms = process_start_time_ms;
    child.started_at_ms = Some(started_at_ms);
    child.last_heartbeat_at_ms = None;
    child.last_exit_at_ms = None;
    child.last_exit_code = None;
    child.backoff_until_ms = None;
    child.attempt_generation_id = Some(attempt_generation_id);
    child.actual_state = SupervisorChildActualState::Starting;
    child.detail = "direct child started and retained by standalone master".to_string();
    append_master_event(
        event_file,
        identity,
        "child-started",
        Some(&child.spec.service_id),
        &format!(
            "direct child pid={process_id} attempt={}",
            child.attempt_ordinal
        ),
    )
}

fn terminate_spawned_child(child: &mut ManagedChild, now_ms: i64) -> Result<(), String> {
    if let Some(process) = child.child.as_mut() {
        process.kill().map_err(|error| {
            format!(
                "failed to terminate fenced direct child {}: {error}",
                child.spec.service_id
            )
        })?;
        let status = process.wait().map_err(|error| {
            format!(
                "failed to reap fenced direct child {}: {error}",
                child.spec.service_id
            )
        })?;
        child.last_exit_code = status.code();
    }
    child.child = None;
    child.process_id = None;
    child.process_start_time_ms = None;
    child.last_exit_at_ms = Some(now_ms);
    child.actual_state = SupervisorChildActualState::Stopped;
    child.detail = "spawned child was reaped because authority changed".to_string();
    Ok(())
}

fn poll_managed_child(
    harness_home: &Path,
    identity: &SupervisorMasterIdentity,
    event_file: &Path,
    child: &mut ManagedChild,
    now_ms: i64,
) -> Result<(), String> {
    if let Some(process) = child.child.as_mut() {
        match process.try_wait().map_err(|error| {
            format!(
                "failed to poll direct child {}: {error}",
                child.spec.service_id
            )
        })? {
            Some(status) => {
                let exit_code = status.code();
                child.child = None;
                child.last_exit_at_ms = Some(now_ms);
                child.last_exit_code = exit_code;
                child.process_id = None;
                child.process_start_time_ms = None;
                let detail = format!(
                    "direct child exited with {}",
                    exit_code
                        .map(|code| code.to_string())
                        .unwrap_or_else(|| "no exit code".to_string())
                );
                append_master_event(
                    event_file,
                    identity,
                    "child-exited",
                    Some(&child.spec.service_id),
                    &detail,
                )?;
                if child.spec.stop_file.is_file() {
                    child.actual_state = SupervisorChildActualState::Stopped;
                    child.detail = format!("{detail}; stop intent was present");
                } else {
                    child.schedule_restart(now_ms, detail);
                }
                return Ok(());
            }
            None => {}
        }
    } else {
        return Ok(());
    }

    let heartbeat = read_matching_heartbeat(harness_home, child)?;
    let heartbeat_ttl_ms = child.spec.heartbeat_ttl_ms.max(1);
    let mut stale_detail = None;
    if let Some(at_ms) = heartbeat {
        child.last_heartbeat_at_ms = Some(at_ms);
        let future_skew_ms = heartbeat_ttl_ms.max(HEARTBEAT_FUTURE_SKEW_FLOOR_MS);
        if at_ms > now_ms.saturating_add(future_skew_ms) {
            stale_detail = Some(format!(
                "direct child heartbeat is {}ms beyond the allowed future skew",
                at_ms.saturating_sub(now_ms)
            ));
        } else if now_ms.saturating_sub(at_ms) > heartbeat_ttl_ms {
            stale_detail = Some(format!(
                "direct child heartbeat is stale by {}ms (ttl={}ms)",
                now_ms.saturating_sub(at_ms),
                heartbeat_ttl_ms
            ));
        } else {
            child.actual_state = SupervisorChildActualState::Running;
            child.detail = "direct child heartbeat matches master attempt and is fresh".to_string();
            return Ok(());
        }
    }
    let started_at_ms = child.started_at_ms.unwrap_or(now_ms);
    if heartbeat.is_none() && now_ms.saturating_sub(started_at_ms) > heartbeat_ttl_ms {
        stale_detail = Some(format!(
            "direct child produced no matching heartbeat within {heartbeat_ttl_ms}ms"
        ));
    }
    if let Some(detail) = stale_detail {
        let process_id = child.process_id;
        if let Some(process) = child.child.as_mut() {
            process.kill().map_err(|error| {
                format!(
                    "failed to terminate stale direct child {} pid={process_id:?}: {error}",
                    child.spec.service_id
                )
            })?;
            let status = process.wait().map_err(|error| {
                format!(
                    "failed to reap stale direct child {} pid={process_id:?}: {error}",
                    child.spec.service_id
                )
            })?;
            child.last_exit_code = status.code();
        }
        child.child = None;
        child.process_id = None;
        child.process_start_time_ms = None;
        child.last_exit_at_ms = Some(now_ms);
        child.actual_state = SupervisorChildActualState::Unhealthy;
        append_master_event(
            event_file,
            identity,
            "child-heartbeat-stale",
            Some(&child.spec.service_id),
            &detail,
        )?;
        child.schedule_restart(now_ms, detail);
    }
    Ok(())
}

fn read_matching_heartbeat(
    harness_home: &Path,
    child: &ManagedChild,
) -> Result<Option<i64>, String> {
    let heartbeat_file = harness_home
        .join("state")
        .join("supervisor")
        .join("loop-heartbeats")
        .join(format!("{}.json", child.spec.service_id));
    let text = match fs::read_to_string(&heartbeat_file) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(format!(
                "failed to read direct child heartbeat {}: {error}",
                heartbeat_file.display()
            ));
        }
    };
    let value: Value = serde_json::from_str(&text).map_err(|error| {
        format!(
            "direct child heartbeat is invalid JSON at {}: {error}",
            heartbeat_file.display()
        )
    })?;
    let matches = value.get("schema").and_then(Value::as_str)
        == Some("agent-harness.loop-heartbeat.v1")
        && value.get("serviceId").and_then(Value::as_str) == Some(child.spec.service_id.as_str())
        && value.get("generationId").and_then(Value::as_str)
            == child.attempt_generation_id.as_deref()
        && value.get("processId").and_then(Value::as_u64) == child.process_id.map(u64::from);
    if !matches {
        return Ok(None);
    }
    Ok(value.get("atMs").and_then(Value::as_i64))
}

fn drain_children(
    options: &StandaloneMasterOptions,
    identity: &SupervisorMasterIdentity,
    capability: ProcessTreeContainmentCapability,
    state_file: &Path,
    event_file: &Path,
    children: &mut [ManagedChild],
) -> Result<(), String> {
    write_master_state(
        state_file,
        identity,
        capability,
        SupervisorMasterPhase::Draining,
        0,
        children,
        "standalone master stopped admission and began bounded child drain",
    )?;
    append_master_event(
        event_file,
        identity,
        "master-draining",
        None,
        "standalone master stopped admission and began bounded child drain",
    )?;
    for child in children.iter_mut() {
        child.actual_state = SupervisorChildActualState::Draining;
        child.detail = "master drain requested through watched stop file".to_string();
        child.ephemeral_stop_created = write_supervisor_stop_file(
            &child.spec.stop_file,
            &child.spec.service_id,
            &identity.generation_id,
        )?;
        write_managed_child_state(&options.harness_home, identity, child)?;
    }

    let started = std::time::Instant::now();
    let timeout = Duration::from_millis(options.drain_timeout_ms);
    loop {
        let mut remaining = 0usize;
        for child in children.iter_mut() {
            let Some(process) = child.child.as_mut() else {
                continue;
            };
            match process.try_wait().map_err(|error| {
                format!(
                    "failed to poll draining direct child {}: {error}",
                    child.spec.service_id
                )
            })? {
                Some(status) => {
                    child.last_exit_at_ms = Some(current_time_ms()?);
                    child.last_exit_code = status.code();
                    child.child = None;
                    child.process_id = None;
                    child.process_start_time_ms = None;
                    child.actual_state = SupervisorChildActualState::Stopped;
                    child.detail = "direct child acknowledged bounded drain".to_string();
                    write_managed_child_state(&options.harness_home, identity, child)?;
                }
                None => remaining += 1,
            }
        }
        if remaining == 0 {
            break;
        }
        if started.elapsed() >= timeout {
            break;
        }
        thread::sleep(Duration::from_millis(25));
    }

    for child in children.iter_mut() {
        let Some(process) = child.child.as_mut() else {
            continue;
        };
        child.actual_state = SupervisorChildActualState::Stopping;
        child.detail = "cooperative drain deadline expired; forcing retained child".to_string();
        process.kill().map_err(|error| {
            format!(
                "failed to force direct child {} after drain deadline: {error}",
                child.spec.service_id
            )
        })?;
        let status = process.wait().map_err(|error| {
            format!(
                "failed to reap forced direct child {}: {error}",
                child.spec.service_id
            )
        })?;
        child.last_exit_at_ms = Some(current_time_ms()?);
        child.last_exit_code = status.code();
        child.child = None;
        child.process_id = None;
        child.process_start_time_ms = None;
        child.actual_state = SupervisorChildActualState::Stopped;
        child.detail = "direct child was forced after bounded drain deadline".to_string();
        child.forced = true;
        append_master_event(
            event_file,
            identity,
            "child-forced",
            Some(&child.spec.service_id),
            "direct child was forced after bounded drain deadline",
        )?;
        write_managed_child_state(&options.harness_home, identity, child)?;
    }
    if children.iter().any(|child| child.child.is_some()) {
        return Err(
            "standalone master could not prove every retained child was reaped".to_string(),
        );
    }
    for child in children.iter_mut() {
        if child.ephemeral_stop_created {
            remove_master_ephemeral_stop(
                &child.spec.stop_file,
                &child.spec.service_id,
                &identity.generation_id,
            )?;
            child.ephemeral_stop_created = false;
        }
    }
    Ok(())
}

fn write_supervisor_stop_file(
    path: &Path,
    service_id: &str,
    master_generation_id: &str,
) -> Result<bool, String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            format!(
                "failed to create direct child stop directory {}: {error}",
                parent.display()
            )
        })?;
    }
    let bytes = serde_json::to_vec_pretty(&json!({
        "schema": "agent-harness.supervisor-stop-file.v1",
        "serviceId": service_id,
        "persistent": false,
        "reason": "standalone-master-drain",
        "masterGenerationId": master_generation_id,
        "atMs": current_time_ms()?,
    }))
    .map_err(|error| format!("failed to serialize direct child stop file: {error}"))?;
    let mut file = match fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => return Ok(false),
        Err(error) => {
            return Err(format!(
                "failed to create direct child stop file {}: {error}",
                path.display()
            ));
        }
    };
    file.write_all(&bytes)
        .and_then(|()| file.sync_all())
        .map_err(|error| {
            format!(
                "failed to write direct child stop file {}: {error}",
                path.display()
            )
        })?;
    Ok(true)
}

fn remove_master_ephemeral_stop(
    path: &Path,
    service_id: &str,
    master_generation_id: &str,
) -> Result<(), String> {
    let text = fs::read_to_string(path).map_err(|error| {
        format!(
            "failed to read master-created ephemeral stop file {} before cleanup: {error}",
            path.display()
        )
    })?;
    let value: Value = serde_json::from_str(&text).map_err(|error| {
        format!(
            "master-created ephemeral stop file changed shape before cleanup at {}: {error}",
            path.display()
        )
    })?;
    let matches = value.get("schema").and_then(Value::as_str)
        == Some("agent-harness.supervisor-stop-file.v1")
        && value.get("serviceId").and_then(Value::as_str) == Some(service_id)
        && value.get("persistent").and_then(Value::as_bool) == Some(false)
        && value.get("reason").and_then(Value::as_str) == Some("standalone-master-drain")
        && value.get("masterGenerationId").and_then(Value::as_str) == Some(master_generation_id);
    if !matches {
        return Err(format!(
            "refused to remove changed or foreign stop file {}",
            path.display()
        ));
    }
    fs::remove_file(path).map_err(|error| {
        format!(
            "failed to remove master-created ephemeral stop file {}: {error}",
            path.display()
        )
    })
}

fn write_managed_child_state(
    harness_home: &Path,
    identity: &SupervisorMasterIdentity,
    child: &ManagedChild,
) -> Result<(), String> {
    let services_dir = harness_home
        .join("state")
        .join("supervisor")
        .join("services");
    fs::create_dir_all(&services_dir).map_err(|error| {
        format!(
            "failed to create direct child state directory {}: {error}",
            services_dir.display()
        )
    })?;
    let state_file = services_dir.join(format!("{}.json", child.spec.service_id));
    let desired_state = if matches!(
        child.actual_state,
        SupervisorChildActualState::Draining
            | SupervisorChildActualState::Stopping
            | SupervisorChildActualState::Stopped
    ) {
        "stopped"
    } else {
        "running"
    };
    let value = json!({
        "schema": "agent-harness.supervisor-service-state.v1",
        "serviceId": child.spec.service_id,
        "serviceKind": child.spec.service_kind,
        "generationId": child.attempt_generation_id,
        "masterGenerationId": identity.generation_id,
        "attemptOrdinal": child.attempt_ordinal,
        "pid": child.process_id,
        "processId": child.process_id,
        "processStartTimeMs": child.process_start_time_ms,
        "startedAtMs": child.started_at_ms,
        "supervisorPid": identity.pid,
        "supervisorProcessStartTimeMs": identity.process_start_time_ms,
        "parentPid": identity.pid,
        "lastHeartbeatAtMs": child.last_heartbeat_at_ms,
        "lastSuccessfulIterationAtMs": child.last_heartbeat_at_ms,
        "lastExitAtMs": child.last_exit_at_ms,
        "lastExitCode": child.last_exit_code,
        "restartCount": child.restart_count,
        "backoffUntilMs": child.backoff_until_ms,
        "status": child.actual_state,
        "desiredState": desired_state,
        "actualState": child.actual_state,
        "detail": child.detail,
        "launchOwner": STANDALONE_MASTER_LAUNCH_OWNER,
        "observedOnly": false,
        "watchedStopFile": child.spec.stop_file,
        "heartbeatTtlMs": child.spec.heartbeat_ttl_ms,
        "commandDigest": child_spec_digest(&child.spec)?,
    });
    write_json_atomic(&state_file, &value).map_err(|error| {
        format!(
            "failed to write direct child state {}: {error}",
            state_file.display()
        )
    })
}

fn write_master_state(
    path: &Path,
    identity: &SupervisorMasterIdentity,
    containment: ProcessTreeContainmentCapability,
    phase: SupervisorMasterPhase,
    iteration: usize,
    children: &[ManagedChild],
    detail: &str,
) -> Result<(), String> {
    let running = children
        .iter()
        .filter(|child| child.child.is_some())
        .count();
    let unhealthy = children
        .iter()
        .filter(|child| {
            matches!(
                child.actual_state,
                SupervisorChildActualState::Unhealthy
                    | SupervisorChildActualState::Backoff
                    | SupervisorChildActualState::BreakerOpen
                    | SupervisorChildActualState::Failed
            )
        })
        .count();
    let state = json!({
        "schema": SUPERVISOR_MASTER_STATE_SCHEMA,
        "mode": SupervisorOwnershipMode::DirectOwner,
        "phase": phase,
        "generationId": identity.generation_id,
        "pid": identity.pid,
        "processStartTimeMs": identity.process_start_time_ms,
        "containment": containment,
        "iteration": iteration,
        "desiredServiceCount": children.len(),
        "runningServiceCount": running,
        "unhealthyServiceCount": unhealthy,
        "serviceIds": children.iter().map(|child| child.spec.service_id.clone()).collect::<Vec<_>>(),
        "lastHeartbeatAtMs": current_time_ms()?,
        "detail": detail,
    });
    write_json_atomic(path, &state).map_err(|error| {
        format!(
            "failed to write standalone master state {}: {error}",
            path.display()
        )
    })
}

fn append_master_event(
    path: &Path,
    identity: &SupervisorMasterIdentity,
    event: &str,
    service_id: Option<&str>,
    detail: &str,
) -> Result<(), String> {
    append_jsonl_value(
        path,
        &json!({
            "schema": SUPERVISOR_MASTER_EVENT_SCHEMA,
            "masterGenerationId": identity.generation_id,
            "masterPid": identity.pid,
            "event": event,
            "serviceId": service_id,
            "detail": detail,
            "atMs": current_time_ms()?,
        }),
    )
    .map_err(|error| {
        format!(
            "failed to append standalone master event {}: {error}",
            path.display()
        )
    })
}

fn current_master_identity() -> Result<SupervisorMasterIdentity, String> {
    let pid = std::process::id();
    let process_start_time_ms = process_start_time_ms_for_pid(i64::from(pid));
    #[cfg(windows)]
    let process_start_time_ms = process_start_time_ms.ok_or_else(|| {
        format!("cannot prove exact standalone master process start time for pid {pid}")
    })?;
    #[cfg(not(windows))]
    let process_start_time_ms = process_start_time_ms.unwrap_or(current_time_ms()?);
    let generation_id = format!(
        "standalone-master-{pid}-{process_start_time_ms}-{}",
        current_time_ms()?
    );
    Ok(SupervisorMasterIdentity {
        pid,
        process_start_time_ms,
        generation_id,
    })
}

fn master_state_dir(harness_home: &Path) -> PathBuf {
    harness_home.join("state").join("supervisor").join("master")
}

fn validate_specs(specs: &[StandaloneMasterChildSpec]) -> Result<(), String> {
    if specs.is_empty() {
        return Err("standalone master requires at least one enabled service".to_string());
    }
    let mut ids = BTreeSet::new();
    for spec in specs {
        validate_service_id(&spec.service_id)?;
        if !ids.insert(spec.service_id.clone()) {
            return Err(format!(
                "standalone master service id `{}` is duplicated",
                spec.service_id
            ));
        }
        if spec.service_kind.trim().is_empty() || spec.service_kind.len() > 128 {
            return Err(format!(
                "standalone master service `{}` has invalid serviceKind",
                spec.service_id
            ));
        }
        for reserved in [
            "AGENT_HARNESS_SERVICE_GENERATION_ID",
            "AGENT_HARNESS_SERVICE_STARTED_AT_MS",
            "AGENT_HARNESS_SUPERVISOR_LAUNCH_OWNER",
            "AGENT_HARNESS_SUPERVISOR_OBSERVED_ONLY",
            "AGENT_HARNESS_SUPERVISOR_PARENT_PID",
            "AGENT_HARNESS_SUPERVISOR_PROCESS_START_TIME_MS",
            "AGENT_HARNESS_SUPERVISOR_STOP_FILE",
            STANDALONE_MASTER_STATE_WRITER_ENV,
        ] {
            if spec.environment.contains_key(reserved) {
                return Err(format!(
                    "standalone master service `{}` environment may not override reserved key {reserved}",
                    spec.service_id
                ));
            }
        }
        if !spec.executable.is_absolute() {
            return Err(format!(
                "standalone master service `{}` executable must be absolute: {}",
                spec.service_id,
                spec.executable.display()
            ));
        }
        if !spec.executable.is_file() {
            return Err(format!(
                "standalone master service `{}` executable does not exist: {}",
                spec.service_id,
                spec.executable.display()
            ));
        }
        if !spec.stop_file.is_absolute() {
            return Err(format!(
                "standalone master service `{}` stop file must be absolute",
                spec.service_id
            ));
        }
        if spec.heartbeat_ttl_ms <= 0 {
            return Err(format!(
                "standalone master service `{}` heartbeat TTL must be positive",
                spec.service_id
            ));
        }
        if spec.restart.crash_loop_threshold == 0 {
            return Err(format!(
                "standalone master service `{}` crash-loop threshold must be positive",
                spec.service_id
            ));
        }
        if spec.restart.base_backoff_ms < 0
            || spec.restart.max_backoff_ms < spec.restart.base_backoff_ms
            || spec.restart.crash_loop_window_ms <= 0
        {
            return Err(format!(
                "standalone master service `{}` restart policy is invalid",
                spec.service_id
            ));
        }
    }
    Ok(())
}

fn validate_service_id(service_id: &str) -> Result<(), String> {
    let valid = !service_id.is_empty()
        && service_id.len() <= 128
        && service_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'));
    if valid {
        Ok(())
    } else {
        Err(format!(
            "supervisor service id `{service_id}` must contain only ASCII letters, digits, dash, underscore, or dot"
        ))
    }
}

fn master_spec_digest(specs: &[StandaloneMasterChildSpec]) -> Result<String, String> {
    let values = specs.iter().map(spec_digest_value).collect::<Vec<_>>();
    let bytes = serde_json::to_vec(&values)
        .map_err(|error| format!("failed to serialize standalone master specs: {error}"))?;
    Ok(format!("sha256:{}", sha256_hex(&bytes)))
}

fn child_spec_digest(spec: &StandaloneMasterChildSpec) -> Result<String, String> {
    let bytes = serde_json::to_vec(&spec_digest_value(spec))
        .map_err(|error| format!("failed to serialize standalone child spec: {error}"))?;
    Ok(format!("sha256:{}", sha256_hex(&bytes)))
}

fn spec_digest_value(spec: &StandaloneMasterChildSpec) -> Value {
    json!({
        "serviceId": spec.service_id,
        "serviceKind": spec.service_kind,
        "executable": spec.executable,
        "args": spec.args,
        "workingDirectory": spec.working_directory,
        "stopFile": spec.stop_file,
        "heartbeatTtlMs": spec.heartbeat_ttl_ms,
        "restart": spec.restart,
        "environmentKeys": spec.environment.keys().collect::<Vec<_>>(),
    })
}

fn current_executable_digest() -> Result<String, String> {
    let executable = std::env::current_exe()
        .map_err(|error| format!("failed to resolve standalone master executable: {error}"))?;
    let bytes = fs::read(&executable).map_err(|error| {
        format!(
            "failed to read standalone master executable {} for provenance: {error}",
            executable.display()
        )
    })?;
    Ok(format!("sha256:{}", sha256_hex(&bytes)))
}

fn current_time_ms() -> Result<i64, String> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("system clock is before Unix epoch: {error}"))?
        .as_millis();
    i64::try_from(millis).map_err(|_| "current time does not fit i64 milliseconds".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_contract_is_versioned_bounded_and_generation_fenced() {
        let request = SupervisorControlRequestV1 {
            schema: SUPERVISOR_CONTROL_REQUEST_SCHEMA.to_string(),
            request_id: "request-1".to_string(),
            expected_master_generation_id: "master-1".to_string(),
            action: SupervisorControlAction::Drain,
            service_id: Some("runtime-loop".to_string()),
            idempotency_key: "stable-key".to_string(),
            issued_at_ms: 1,
            auth_proof: "opaque-proof".to_string(),
        };
        request.validate_shape().unwrap();

        let mut invalid = request.clone();
        invalid.auth_proof.clear();
        assert!(invalid.validate_shape().is_err());
        invalid = request;
        invalid.expected_master_generation_id = "x".repeat(257);
        assert!(invalid.validate_shape().is_err());
    }

    #[test]
    fn compatibility_import_is_idempotent_and_never_adopts_process_identity() {
        let root = temp_root("compatibility-import");
        let services = root.join("state").join("supervisor").join("services");
        fs::create_dir_all(&services).unwrap();
        write_json_atomic(
            &services.join("runtime-loop.json"),
            &json!({
                "schema": "agent-harness.supervisor-service-state.v1",
                "serviceId": "runtime-loop",
                "launchOwner": "rust-supervisor-run",
                "actualState": "stopped",
                "supervisorPid": 999_999_991_i64,
                "processId": 999_999_992_i64,
            }),
        )
        .unwrap();

        let first = inspect_compatibility_state(&root, 10);
        let second = inspect_compatibility_state(&root, 10);
        assert_eq!(first, second);
        assert!(first.is_safe_for_direct_owner());
        assert_eq!(first.snapshots.len(), 1);
        assert!(!first.snapshots[0].adopted_process_identity);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn compatibility_import_rejects_live_ambiguous_or_corrupt_owner() {
        let root = temp_root("compatibility-import-blocked");
        let services = root.join("state").join("supervisor").join("services");
        fs::create_dir_all(&services).unwrap();
        fs::write(services.join("runtime-loop.json"), b"{not-json").unwrap();
        let assessment = inspect_compatibility_state(&root, 10);
        assert_eq!(assessment.status, CompatibilityImportStatus::Blocked);
        assert_eq!(assessment.blockers.len(), 1);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn compatibility_import_rejects_running_state_without_positive_pids() {
        let root = temp_root("compatibility-import-running-without-pids");
        let services = root.join("state").join("supervisor").join("services");
        fs::create_dir_all(&services).unwrap();
        write_json_atomic(
            &services.join("runtime-loop.json"),
            &json!({
                "schema": "agent-harness.supervisor-service-state.v1",
                "serviceId": "runtime-loop",
                "launchOwner": "rust-supervisor-run",
                "actualState": "running",
                "supervisorPid": null,
                "processId": null,
            }),
        )
        .unwrap();

        let assessment = inspect_compatibility_state(&root, 10);
        assert_eq!(assessment.status, CompatibilityImportStatus::Blocked);
        assert_eq!(assessment.accepted_file_count, 0);
        assert!(assessment.snapshots.is_empty());
        assert!(
            assessment.blockers[0]
                .reason
                .contains("stopped-state allowlist")
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn direct_owner_crash_state_with_dead_exact_identities_is_recoverable() {
        let root = temp_root("direct-owner-crash-recovery");
        let services = root.join("state").join("supervisor").join("services");
        fs::create_dir_all(&services).unwrap();
        write_json_atomic(
            &services.join("runtime-loop.json"),
            &json!({
                "schema": "agent-harness.supervisor-service-state.v1",
                "serviceId": "runtime-loop",
                "launchOwner": STANDALONE_MASTER_LAUNCH_OWNER,
                "actualState": "running",
                "supervisorPid": 999_999_981_i64,
                "supervisorProcessStartTimeMs": 100_i64,
                "processId": 999_999_982_i64,
                "processStartTimeMs": 101_i64,
            }),
        )
        .unwrap();

        let assessment = inspect_compatibility_state(&root, 10);
        assert_eq!(assessment.status, CompatibilityImportStatus::Accepted);
        assert_eq!(assessment.accepted_file_count, 1);
        assert_eq!(
            assessment.snapshots[0].prior_actual_state.as_deref(),
            Some("running")
        );
        assert!(!assessment.snapshots[0].adopted_process_identity);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn direct_owner_draining_state_with_dead_master_and_child_exit_proof_is_recoverable() {
        let root = temp_root("direct-owner-draining-after-child-exit");
        let services = root.join("state").join("supervisor").join("services");
        fs::create_dir_all(&services).unwrap();
        write_json_atomic(
            &services.join("discord-gateway-loop.json"),
            &json!({
                "schema": "agent-harness.supervisor-service-state.v1",
                "serviceId": "discord-gateway-loop",
                "launchOwner": STANDALONE_MASTER_LAUNCH_OWNER,
                "actualState": "draining",
                "supervisorPid": 999_999_971_i64,
                "supervisorProcessStartTimeMs": 100_i64,
                "processId": null,
                "processStartTimeMs": null,
                "lastExitAtMs": 200_i64,
                "lastExitCode": 0_i64,
            }),
        )
        .unwrap();

        let assessment = inspect_compatibility_state(&root, 10);
        assert_eq!(assessment.status, CompatibilityImportStatus::Accepted);
        assert_eq!(assessment.accepted_file_count, 1);
        assert_eq!(
            assessment.snapshots[0].prior_actual_state.as_deref(),
            Some("draining")
        );
        assert_eq!(assessment.snapshots[0].child_pid, None);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn direct_owner_draining_state_without_child_identity_or_exit_proof_is_blocked() {
        let root = temp_root("direct-owner-draining-without-child-proof");
        let services = root.join("state").join("supervisor").join("services");
        fs::create_dir_all(&services).unwrap();
        write_json_atomic(
            &services.join("discord-gateway-loop.json"),
            &json!({
                "schema": "agent-harness.supervisor-service-state.v1",
                "serviceId": "discord-gateway-loop",
                "launchOwner": STANDALONE_MASTER_LAUNCH_OWNER,
                "actualState": "draining",
                "supervisorPid": 999_999_961_i64,
                "supervisorProcessStartTimeMs": 100_i64,
                "processId": null,
                "processStartTimeMs": null,
                "lastExitAtMs": null,
                "lastExitCode": null,
            }),
        )
        .unwrap();

        let assessment = inspect_compatibility_state(&root, 10);
        assert_eq!(assessment.status, CompatibilityImportStatus::Blocked);
        assert_eq!(assessment.accepted_file_count, 0);
        assert!(assessment.blockers.iter().any(|blocker| {
            blocker
                .reason
                .contains("lacks an exact prior child identity")
        }));
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(any(windows, unix))]
    #[test]
    fn master_lease_rejects_second_generation() {
        let root = temp_root("master-lease");
        let identity = SupervisorMasterIdentity {
            pid: std::process::id(),
            process_start_time_ms: 1,
            generation_id: "generation-1".to_string(),
        };
        let first = SupervisorOwnershipLease::acquire(
            &root,
            SupervisorOwnershipLeaseKind::DirectExclusive,
            &identity,
        )
        .unwrap();
        let second = SupervisorOwnershipLease::acquire(
            &root,
            SupervisorOwnershipLeaseKind::DirectExclusive,
            &identity,
        );
        assert!(second.is_err());
        drop(first);
        SupervisorOwnershipLease::acquire(
            &root,
            SupervisorOwnershipLeaseKind::DirectExclusive,
            &identity,
        )
        .unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(any(windows, unix))]
    #[test]
    fn compatibility_leases_coexist_and_fence_direct_owner() {
        let root = temp_root("shared-compatibility-lease");
        let identity = SupervisorMasterIdentity {
            pid: std::process::id(),
            process_start_time_ms: 1,
            generation_id: "generation-1".to_string(),
        };
        let first = SupervisorOwnershipLease::acquire(
            &root,
            SupervisorOwnershipLeaseKind::CompatibilityShared,
            &identity,
        )
        .unwrap();
        let second = SupervisorOwnershipLease::acquire(
            &root,
            SupervisorOwnershipLeaseKind::CompatibilityShared,
            &identity,
        )
        .unwrap();
        assert!(
            SupervisorOwnershipLease::acquire(
                &root,
                SupervisorOwnershipLeaseKind::DirectExclusive,
                &identity,
            )
            .is_err()
        );
        drop(first);
        drop(second);
        SupervisorOwnershipLease::acquire(
            &root,
            SupervisorOwnershipLeaseKind::DirectExclusive,
            &identity,
        )
        .unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(not(windows))]
    #[test]
    fn direct_owner_tree_containment_rejects_before_state_mutation() {
        let root = temp_root("unsupported-tree-containment");
        let harness_home = root.join(".agent-harness");
        let spec = StandaloneMasterChildSpec {
            service_id: "progress-delivery-loop".to_string(),
            service_kind: "progress-delivery".to_string(),
            executable: std::env::current_exe().unwrap(),
            args: Vec::new(),
            working_directory: None,
            stop_file: harness_home
                .join("state")
                .join("supervisor")
                .join("stop")
                .join("progress-delivery-loop.stop"),
            heartbeat_ttl_ms: 1_000,
            restart: SupervisorRestartPolicy {
                enabled: true,
                base_backoff_ms: 10,
                max_backoff_ms: 100,
                crash_loop_window_ms: 1_000,
                crash_loop_threshold: 2,
            },
            environment: BTreeMap::new(),
        };
        for require_tree_containment in [true, false] {
            let error = run_standalone_master(
                StandaloneMasterOptions {
                    harness_home: harness_home.clone(),
                    mode: SupervisorOwnershipMode::DirectOwner,
                    stop_file: harness_home
                        .join("state")
                        .join("supervisor")
                        .join("stop")
                        .join("supervisor-master.stop"),
                    poll_interval_ms: 10,
                    drain_timeout_ms: 100,
                    max_iterations: 1,
                    require_tree_containment,
                },
                vec![spec.clone()],
            )
            .unwrap_err();
            assert!(error.contains("rejects the mode before state mutation"));
            assert!(
                !harness_home.exists(),
                "unsupported direct ownership wrote canonical state before rejecting the host"
            );
        }
    }

    fn temp_root(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-supervisor-master-{label}-{}-{nonce}",
            std::process::id()
        ))
    }
}
