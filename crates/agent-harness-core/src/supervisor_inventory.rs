use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::loop_health::process_alive_for_pid;

const SUPERVISOR_INVENTORY_REPORT_SCHEMA: &str = "agent-harness.supervisor-inventory.v1";
const DEFAULT_HEARTBEAT_TIMEOUT_MS: i64 = 120_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorInventoryServiceConfig {
    pub enabled: bool,
    pub service_id: String,
    pub service_kind: String,
    #[serde(default)]
    pub args: Vec<String>,
    pub priority: String,
    pub restart_delay_ms: i64,
    pub heartbeat_timeout_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupervisorInventoryOptions {
    pub harness_home: PathBuf,
    pub desired_services: Vec<SupervisorInventoryServiceConfig>,
    pub now_ms: Option<i64>,
    pub default_heartbeat_timeout_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorInventoryServiceSummary {
    pub service_id: String,
    pub service_kind: String,
    pub service_file: PathBuf,
    pub heartbeat_file: PathBuf,
    pub priority: String,
    pub present: bool,
    pub corrupt: bool,
    pub parse_error: Option<String>,
    pub heartbeat_present: bool,
    pub heartbeat_corrupt: bool,
    pub heartbeat_parse_error: Option<String>,
    pub process_id: Option<i64>,
    pub process_alive: Option<bool>,
    pub parent_pid: Option<i64>,
    pub process_start_time_ms: Option<i64>,
    pub watched_stop_file: Option<PathBuf>,
    pub status: Option<String>,
    pub desired_state: Option<String>,
    pub actual_state: Option<String>,
    pub last_heartbeat_at_ms: Option<i64>,
    pub age_ms: Option<i64>,
    pub generation_id: Option<String>,
    pub heartbeat_generation_id: Option<String>,
    pub generation_mismatch: bool,
    pub launch_owner: Option<String>,
    pub observed_only: Option<bool>,
    pub ownership_conflict: bool,
    pub ownership_conflict_reason: Option<String>,
    pub restart_delay_ms: i64,
    pub heartbeat_timeout_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorLaunchCommand {
    pub service_id: String,
    pub command: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorInventoryReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub services_dir: PathBuf,
    pub heartbeat_dir: PathBuf,
    pub now_ms: i64,
    pub missing: Vec<SupervisorInventoryServiceSummary>,
    pub stale: Vec<SupervisorInventoryServiceSummary>,
    pub running: Vec<SupervisorInventoryServiceSummary>,
    pub disabled: Vec<SupervisorInventoryServiceSummary>,
    pub launch_commands: Vec<SupervisorLaunchCommand>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum SupervisorInventoryStatus {
    Missing,
    Stale,
    Running,
    Disabled,
}

#[derive(Debug, Clone)]
struct ServiceState {
    service_id: String,
    service_kind: Option<String>,
    process_id: Option<i64>,
    parent_pid: Option<i64>,
    process_start_time_ms: Option<i64>,
    watched_stop_file: Option<PathBuf>,
    status: Option<String>,
    desired_state: Option<String>,
    actual_state: Option<String>,
    last_heartbeat_at_ms: Option<i64>,
    generation_id: Option<String>,
    launch_owner: Option<String>,
    observed_only: Option<bool>,
    corrupt: bool,
    parse_error: Option<String>,
}

#[derive(Debug, Clone)]
struct HeartbeatState {
    present: bool,
    at_ms: Option<i64>,
    process_id: Option<i64>,
    process_alive: Option<bool>,
    parent_pid: Option<i64>,
    process_start_time_ms: Option<i64>,
    watched_stop_file: Option<PathBuf>,
    status: Option<String>,
    generation_id: Option<String>,
    launch_owner: Option<String>,
    observed_only: Option<bool>,
    corrupt: bool,
    parse_error: Option<String>,
}

pub fn reconcile_supervisor_inventory(
    options: SupervisorInventoryOptions,
) -> io::Result<SupervisorInventoryReport> {
    let services_dir = options
        .harness_home
        .join("state")
        .join("supervisor")
        .join("services");
    let heartbeat_dir = options
        .harness_home
        .join("state")
        .join("supervisor")
        .join("loop-heartbeats");
    let now_ms = options.now_ms.unwrap_or_else(current_ms);

    let desired_services = validate_supervisor_desired_services(options.desired_services)?;
    let service_map = read_supervisor_service_states(&services_dir, now_ms)?;
    let mut missing = Vec::new();
    let mut stale = Vec::new();
    let mut running = Vec::new();
    let mut disabled = Vec::new();
    let mut launch_commands = Vec::new();

    for desired in desired_services {
        let service_file = services_dir.join(format!("{}.json", desired.service_id));
        let heartbeat_file = heartbeat_dir.join(format!("{}.json", desired.service_id));
        let heartbeat = read_service_heartbeat(&heartbeat_file, now_ms)?;
        let heartbeat_timeout_ms = desired
            .heartbeat_timeout_ms
            .or(options.default_heartbeat_timeout_ms)
            .unwrap_or(DEFAULT_HEARTBEAT_TIMEOUT_MS)
            .max(1);
        let service_state = service_map.get(&desired.service_id);
        let summary = build_summary(
            &desired,
            &service_file,
            &heartbeat_file,
            service_state,
            &heartbeat,
            heartbeat_timeout_ms,
            now_ms,
        );

        let status = if !desired.enabled {
            SupervisorInventoryStatus::Disabled
        } else {
            classify_supervisor_inventory_status(&summary)
        };

        match status {
            SupervisorInventoryStatus::Disabled => disabled.push(summary),
            SupervisorInventoryStatus::Missing => {
                launch_commands.push(build_launch_command(&options.harness_home, &desired));
                missing.push(summary);
            }
            SupervisorInventoryStatus::Stale => {
                launch_commands.push(build_launch_command(&options.harness_home, &desired));
                stale.push(summary);
            }
            SupervisorInventoryStatus::Running => running.push(summary),
        }
    }

    Ok(SupervisorInventoryReport {
        schema: SUPERVISOR_INVENTORY_REPORT_SCHEMA,
        harness_home: options.harness_home,
        services_dir,
        heartbeat_dir,
        now_ms,
        missing,
        stale,
        running,
        disabled,
        launch_commands,
    })
}

fn validate_supervisor_desired_services(
    services: Vec<SupervisorInventoryServiceConfig>,
) -> io::Result<Vec<SupervisorInventoryServiceConfig>> {
    let mut seen = BTreeSet::new();
    for service in &services {
        validate_supervisor_service_id(&service.service_id)?;
        if !seen.insert(service.service_id.clone()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("duplicate supervisor serviceId `{}`", service.service_id),
            ));
        }
        let expected_kind =
            expected_supervisor_service_kind(&service.service_id).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unsupported supervisor serviceId `{}`", service.service_id),
                )
            })?;
        if service.service_kind != expected_kind {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "serviceKind `{}` does not match expected `{expected_kind}` for serviceId `{}`",
                    service.service_kind, service.service_id
                ),
            ));
        }
        if service.restart_delay_ms < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "restartDelayMs must be non-negative for serviceId `{}`",
                    service.service_id
                ),
            ));
        }
        if service.heartbeat_timeout_ms.is_some_and(|value| value <= 0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "heartbeatTimeoutMs must be greater than zero for serviceId `{}`",
                    service.service_id
                ),
            ));
        }
    }
    Ok(services)
}

fn validate_supervisor_service_id(service_id: &str) -> io::Result<()> {
    if service_id.trim().is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "supervisor serviceId must not be empty",
        ));
    }
    if !service_id
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.'))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("supervisor serviceId `{service_id}` contains unsupported characters"),
        ));
    }
    Ok(())
}

fn expected_supervisor_service_kind(service_id: &str) -> Option<&'static str> {
    match service_id {
        "runtime-loop" => Some("runtime"),
        "worker-loop" => Some("worker"),
        "cron-scheduler-loop" => Some("cron"),
        "progress-delivery-loop" => Some("progress-delivery"),
        "telegram-loop" => Some("telegram-ingress"),
        "discord-outbox-loop" => Some("final-outbox"),
        "discord-gateway-loop" => Some("discord-gateway"),
        _ if service_id.starts_with("telegram-loop-") => Some("telegram-ingress"),
        _ => None,
    }
}

fn build_summary(
    desired: &SupervisorInventoryServiceConfig,
    service_file: &Path,
    heartbeat_file: &Path,
    service_state: Option<&ServiceState>,
    heartbeat: &HeartbeatState,
    heartbeat_timeout_ms: i64,
    now_ms: i64,
) -> SupervisorInventoryServiceSummary {
    let service_kind = service_state
        .and_then(|state| state.service_kind.clone())
        .unwrap_or_else(|| desired.service_kind.clone());

    let service_process_id = service_state.and_then(|state| state.process_id);
    let service_process_alive = service_process_id.and_then(process_alive_for_pid);
    let service_last_heartbeat_at_ms = service_state.and_then(|state| state.last_heartbeat_at_ms);
    let heartbeat_is_newer = match (heartbeat.at_ms, service_last_heartbeat_at_ms) {
        (Some(heartbeat_at_ms), Some(service_at_ms)) => heartbeat_at_ms > service_at_ms,
        (Some(_), None) => true,
        _ => false,
    };
    let process_id = if heartbeat_is_newer {
        heartbeat.process_id.or(service_process_id)
    } else {
        service_process_id.or(heartbeat.process_id)
    };
    let process_alive = process_id
        .and_then(process_alive_for_pid)
        .or(if heartbeat_is_newer {
            heartbeat.process_alive.or(service_process_alive)
        } else {
            service_process_alive.or(heartbeat.process_alive)
        });
    let parent_pid = if heartbeat_is_newer {
        heartbeat
            .parent_pid
            .or_else(|| service_state.and_then(|state| state.parent_pid))
    } else {
        service_state
            .and_then(|state| state.parent_pid)
            .or(heartbeat.parent_pid)
    };
    let process_start_time_ms = if heartbeat_is_newer {
        heartbeat
            .process_start_time_ms
            .or_else(|| service_state.and_then(|state| state.process_start_time_ms))
    } else {
        service_state
            .and_then(|state| state.process_start_time_ms)
            .or(heartbeat.process_start_time_ms)
    };
    let watched_stop_file = if heartbeat_is_newer {
        heartbeat
            .watched_stop_file
            .clone()
            .or_else(|| service_state.and_then(|state| state.watched_stop_file.clone()))
    } else {
        service_state
            .and_then(|state| state.watched_stop_file.clone())
            .or_else(|| heartbeat.watched_stop_file.clone())
    };
    let last_heartbeat_at_ms = if heartbeat_is_newer {
        heartbeat.at_ms.or(service_last_heartbeat_at_ms)
    } else {
        service_last_heartbeat_at_ms.or(heartbeat.at_ms)
    };
    let age_ms = last_heartbeat_at_ms.map(|at_ms| now_ms.saturating_sub(at_ms));
    let status = if heartbeat_is_newer {
        heartbeat
            .status
            .clone()
            .or_else(|| service_state.and_then(|state| state.status.clone()))
    } else {
        service_state
            .and_then(|state| state.status.clone())
            .or_else(|| heartbeat.status.clone())
    };
    let generation_id = service_state.and_then(|state| state.generation_id.clone());
    let heartbeat_generation_id = heartbeat.generation_id.clone();
    let generation_mismatch = generation_id.is_some()
        && heartbeat_generation_id.is_some()
        && generation_id != heartbeat_generation_id;
    let launch_owner = if heartbeat_is_newer {
        heartbeat
            .launch_owner
            .clone()
            .or_else(|| service_state.and_then(|state| state.launch_owner.clone()))
    } else {
        service_state
            .and_then(|state| state.launch_owner.clone())
            .or_else(|| heartbeat.launch_owner.clone())
    };
    let observed_only = if heartbeat_is_newer {
        heartbeat
            .observed_only
            .or_else(|| service_state.and_then(|state| state.observed_only))
    } else {
        service_state
            .and_then(|state| state.observed_only)
            .or(heartbeat.observed_only)
    };
    let ownership_conflict_reason = if generation_mismatch {
        Some("generation-mismatch".to_string())
    } else if observed_only == Some(true) {
        Some("observed-only-owner".to_string())
    } else {
        None
    };
    let ownership_conflict = ownership_conflict_reason.is_some();

    SupervisorInventoryServiceSummary {
        service_id: desired.service_id.clone(),
        service_kind,
        service_file: service_file.to_path_buf(),
        heartbeat_file: heartbeat_file.to_path_buf(),
        priority: desired.priority.clone(),
        present: service_state.is_some(),
        corrupt: service_state.is_some_and(|state| state.corrupt),
        parse_error: service_state.and_then(|state| state.parse_error.clone()),
        heartbeat_present: heartbeat.present,
        heartbeat_corrupt: heartbeat.corrupt,
        heartbeat_parse_error: heartbeat.parse_error.clone(),
        process_id,
        process_alive,
        parent_pid,
        process_start_time_ms,
        watched_stop_file,
        status,
        desired_state: service_state.and_then(|state| state.desired_state.clone()),
        actual_state: service_state.and_then(|state| state.actual_state.clone()),
        last_heartbeat_at_ms,
        age_ms,
        generation_id,
        heartbeat_generation_id,
        generation_mismatch,
        launch_owner,
        observed_only,
        ownership_conflict,
        ownership_conflict_reason,
        restart_delay_ms: desired.restart_delay_ms,
        heartbeat_timeout_ms,
    }
}

fn classify_supervisor_inventory_status(
    summary: &SupervisorInventoryServiceSummary,
) -> SupervisorInventoryStatus {
    if !summary.present {
        return SupervisorInventoryStatus::Missing;
    }
    if summary.corrupt || summary.heartbeat_corrupt {
        return SupervisorInventoryStatus::Stale;
    }
    if summary.parse_error.is_some() || summary.heartbeat_parse_error.is_some() {
        return SupervisorInventoryStatus::Stale;
    }
    if summary.ownership_conflict {
        return SupervisorInventoryStatus::Stale;
    }
    if summary.process_alive == Some(false) {
        return SupervisorInventoryStatus::Stale;
    }
    if !summary.heartbeat_present {
        return SupervisorInventoryStatus::Stale;
    }
    if is_unhealthy_state(summary.status.as_deref())
        || is_unhealthy_state(summary.desired_state.as_deref())
        || is_unhealthy_state(summary.actual_state.as_deref())
    {
        return SupervisorInventoryStatus::Stale;
    }
    if summary
        .age_ms
        .is_some_and(|age_ms| age_ms > summary.heartbeat_timeout_ms)
    {
        return SupervisorInventoryStatus::Stale;
    }
    SupervisorInventoryStatus::Running
}

fn build_launch_command(
    harness_home: &Path,
    config: &SupervisorInventoryServiceConfig,
) -> SupervisorLaunchCommand {
    let mut command = vec![
        "agent-harness-cli".to_string(),
        "supervisor-run".to_string(),
        "--service".to_string(),
        config.service_id.clone(),
        "--harness-home".to_string(),
        harness_home.to_string_lossy().to_string(),
        "--restart-delay-ms".to_string(),
        config.restart_delay_ms.to_string(),
    ];
    command.extend(config.args.iter().cloned());
    SupervisorLaunchCommand {
        service_id: config.service_id.clone(),
        command,
    }
}

fn read_supervisor_service_states(
    services_dir: &Path,
    now_ms: i64,
) -> io::Result<BTreeMap<String, ServiceState>> {
    let mut service_map = BTreeMap::new();
    let entries = match fs::read_dir(services_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(service_map),
        Err(error) => return Err(error),
    };

    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        let fallback_service_id = path
            .file_stem()
            .and_then(|value| value.to_str())
            .unwrap_or("")
            .to_string();
        let service = read_service_state(&path, &fallback_service_id, now_ms)?;
        service_map.insert(service.service_id.clone(), service);
    }

    Ok(service_map)
}

fn read_service_state(
    path: &Path,
    fallback_service_id: &str,
    _now_ms: i64,
) -> io::Result<ServiceState> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(default_service_state(fallback_service_id));
        }
        Err(error) => return Err(error),
    };

    let value = match serde_json::from_str::<Value>(&text) {
        Ok(value) => value,
        Err(error) => {
            return Ok(ServiceState {
                service_id: fallback_service_id.to_string(),
                service_kind: None,
                process_id: None,
                parent_pid: None,
                process_start_time_ms: None,
                watched_stop_file: None,
                status: None,
                desired_state: None,
                actual_state: None,
                last_heartbeat_at_ms: None,
                generation_id: None,
                launch_owner: None,
                observed_only: None,
                corrupt: true,
                parse_error: Some(error.to_string()),
            });
        }
    };

    let service_id = string_path(&value, &["serviceId"])
        .filter(|value| !value.trim().is_empty())
        .or_else(|| (!fallback_service_id.is_empty()).then(|| fallback_service_id.to_string()));
    let service_id = match service_id {
        Some(service_id) => service_id,
        None => fallback_service_id.to_string(),
    };
    let service_kind = string_path(&value, &["serviceKind"]);
    let process_id = i64_path(&value, &["pid"]).or_else(|| i64_path(&value, &["processId"]));
    let parent_pid =
        i64_path(&value, &["parentPid"]).or_else(|| i64_path(&value, &["supervisorPid"]));
    let last_heartbeat_at_ms =
        i64_path(&value, &["lastHeartbeatAtMs"]).or_else(|| i64_path(&value, &["heartbeatAtMs"]));
    Ok(ServiceState {
        service_id,
        service_kind,
        process_id,
        parent_pid,
        process_start_time_ms: i64_path(&value, &["processStartTimeMs"]),
        watched_stop_file: pathbuf_path(&value, &["watchedStopFile"]),
        status: string_path(&value, &["status"]),
        desired_state: string_path(&value, &["desiredState"]),
        actual_state: string_path(&value, &["actualState"]),
        last_heartbeat_at_ms,
        generation_id: string_path(&value, &["generationId"]),
        launch_owner: string_path(&value, &["launchOwner"]),
        observed_only: bool_path(&value, &["observedOnly"]),
        corrupt: false,
        parse_error: None,
    })
}

fn read_service_heartbeat(path: &Path, _now_ms: i64) -> io::Result<HeartbeatState> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(HeartbeatState {
                present: false,
                at_ms: None,
                process_id: None,
                process_alive: None,
                parent_pid: None,
                process_start_time_ms: None,
                watched_stop_file: None,
                status: None,
                generation_id: None,
                launch_owner: None,
                observed_only: None,
                corrupt: false,
                parse_error: None,
            });
        }
        Err(error) => return Err(error),
    };
    let value = match serde_json::from_str::<Value>(&text) {
        Ok(value) => value,
        Err(error) => {
            return Ok(HeartbeatState {
                present: true,
                at_ms: None,
                process_id: None,
                process_alive: None,
                parent_pid: None,
                process_start_time_ms: None,
                watched_stop_file: None,
                status: None,
                generation_id: None,
                launch_owner: None,
                observed_only: None,
                corrupt: true,
                parse_error: Some(error.to_string()),
            });
        }
    };
    let at_ms = i64_path(&value, &["atMs"]);
    let process_id = i64_path(&value, &["processId"]).or_else(|| i64_path(&value, &["pid"]));
    Ok(HeartbeatState {
        present: true,
        at_ms,
        process_id,
        process_alive: process_id.and_then(process_alive_for_pid),
        parent_pid: i64_path(&value, &["parentPid"]),
        process_start_time_ms: i64_path(&value, &["processStartTimeMs"]),
        watched_stop_file: pathbuf_path(&value, &["watchedStopFile"]),
        status: string_path(&value, &["status"]),
        generation_id: string_path(&value, &["generationId"]),
        launch_owner: string_path(&value, &["launchOwner"]),
        observed_only: bool_path(&value, &["observedOnly"]),
        corrupt: false,
        parse_error: None,
    })
}

fn is_unhealthy_state(value: Option<&str>) -> bool {
    let Some(value) = value else {
        return false;
    };
    let normalized = value.trim().to_lowercase();
    matches!(
        normalized.as_str(),
        "stopped" | "closed" | "failed" | "failing" | "error"
    ) || normalized.contains("fail")
}

fn default_service_state(fallback_service_id: &str) -> ServiceState {
    ServiceState {
        service_id: fallback_service_id.to_string(),
        service_kind: None,
        process_id: None,
        parent_pid: None,
        process_start_time_ms: None,
        watched_stop_file: None,
        status: None,
        desired_state: None,
        actual_state: None,
        last_heartbeat_at_ms: None,
        generation_id: None,
        launch_owner: None,
        observed_only: None,
        corrupt: false,
        parse_error: None,
    }
}

fn current_ms() -> i64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(now.as_millis()).unwrap_or(i64::MAX)
}

fn string_path(value: &Value, path: &[&str]) -> Option<String> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    current.as_str().map(ToString::to_string)
}

fn i64_path(value: &Value, path: &[&str]) -> Option<i64> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    current.as_i64()
}

fn pathbuf_path(value: &Value, path: &[&str]) -> Option<PathBuf> {
    string_path(value, path).map(PathBuf::from)
}

fn bool_path(value: &Value, path: &[&str]) -> Option<bool> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    current.as_bool()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inventory_marks_enabled_service_without_state_as_missing() {
        let root = temp_root("inventory_marks_enabled_service_without_state_as_missing");
        let harness_home = root.join(".agent-harness");

        let report = reconcile_supervisor_inventory(SupervisorInventoryOptions {
            harness_home: harness_home.clone(),
            desired_services: vec![SupervisorInventoryServiceConfig {
                enabled: true,
                service_id: "cron-scheduler-loop".to_string(),
                service_kind: "cron".to_string(),
                args: vec!["--source-home".to_string(), "/opt/source".to_string()],
                priority: "standard".to_string(),
                restart_delay_ms: 15_000,
                heartbeat_timeout_ms: Some(5_000),
            }],
            now_ms: Some(1_000),
            default_heartbeat_timeout_ms: Some(120_000),
        })
        .unwrap();

        assert_eq!(report.missing.len(), 1);
        assert_eq!(
            report.missing.first().map(|item| item.service_id.as_str()),
            Some("cron-scheduler-loop")
        );
        assert_eq!(report.launch_commands.len(), 1);
        let command = &report.launch_commands.first().unwrap().command;
        assert_eq!(command[0], "agent-harness-cli");
        assert_eq!(command[1], "supervisor-run");
        assert!(command.contains(&"--service".to_string()));
        assert!(
            command
                .windows(2)
                .any(|pair| { pair[0] == "--service" && pair[1] == "cron-scheduler-loop" })
        );
        assert!(command.windows(2).any(|pair| {
            pair[0] == "--harness-home" && pair[1] == harness_home.to_string_lossy()
        }));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn inventory_rejects_duplicate_or_unsupported_desired_services() {
        let root = temp_root("inventory_rejects_duplicate_or_unsupported_desired_services");
        let harness_home = root.join(".agent-harness");

        let duplicate = reconcile_supervisor_inventory(SupervisorInventoryOptions {
            harness_home: harness_home.clone(),
            desired_services: vec![
                SupervisorInventoryServiceConfig {
                    enabled: true,
                    service_id: "runtime-loop".to_string(),
                    service_kind: "runtime".to_string(),
                    args: Vec::new(),
                    priority: "standard".to_string(),
                    restart_delay_ms: 15_000,
                    heartbeat_timeout_ms: Some(5_000),
                },
                SupervisorInventoryServiceConfig {
                    enabled: true,
                    service_id: "runtime-loop".to_string(),
                    service_kind: "runtime".to_string(),
                    args: Vec::new(),
                    priority: "standard".to_string(),
                    restart_delay_ms: 15_000,
                    heartbeat_timeout_ms: Some(5_000),
                },
            ],
            now_ms: Some(1_100),
            default_heartbeat_timeout_ms: Some(120_000),
        });
        assert!(duplicate.is_err());

        let unsupported = reconcile_supervisor_inventory(SupervisorInventoryOptions {
            harness_home: harness_home.clone(),
            desired_services: vec![SupervisorInventoryServiceConfig {
                enabled: true,
                service_id: "runtime-loop/escape".to_string(),
                service_kind: "runtime".to_string(),
                args: Vec::new(),
                priority: "standard".to_string(),
                restart_delay_ms: 15_000,
                heartbeat_timeout_ms: Some(5_000),
            }],
            now_ms: Some(1_100),
            default_heartbeat_timeout_ms: Some(120_000),
        });
        assert!(unsupported.is_err());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn inventory_does_not_launch_disabled_service() {
        let root = temp_root("inventory_does_not_launch_disabled_service");
        let harness_home = root.join(".agent-harness");
        write_service_state(
            &harness_home,
            "runtime-loop",
            serde_json::json!({
                "schema": "agent-harness.supervisor-service-state.v1",
                "serviceId": "runtime-loop",
                "serviceKind": "runtime",
                "status": "running",
                "desiredState": "running",
                "actualState": "running",
                "lastHeartbeatAtMs": 1_000,
            }),
        );

        let report = reconcile_supervisor_inventory(SupervisorInventoryOptions {
            harness_home: harness_home.clone(),
            desired_services: vec![SupervisorInventoryServiceConfig {
                enabled: false,
                service_id: "runtime-loop".to_string(),
                service_kind: "runtime".to_string(),
                args: Vec::new(),
                priority: "standard".to_string(),
                restart_delay_ms: 15_000,
                heartbeat_timeout_ms: Some(5_000),
            }],
            now_ms: Some(1_100),
            default_heartbeat_timeout_ms: Some(120_000),
        })
        .unwrap();

        assert_eq!(report.disabled.len(), 1);
        assert!(report.missing.is_empty());
        assert!(report.stale.is_empty());
        assert!(report.running.is_empty());
        assert!(report.launch_commands.is_empty());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn inventory_marks_stale_heartbeat_as_restartable() {
        let root = temp_root("inventory_marks_stale_heartbeat_as_restartable");
        let harness_home = root.join(".agent-harness");
        let pid = i64::from(std::process::id());
        let now_ms = 10_000;
        write_service_state(
            &harness_home,
            "progress-delivery-loop",
            serde_json::json!({
                "schema": "agent-harness.supervisor-service-state.v1",
                "serviceId": "progress-delivery-loop",
                "serviceKind": "progress",
                "status": "running",
                "desiredState": "running",
                "actualState": "running",
                "pid": pid,
                "lastHeartbeatAtMs": now_ms - 10_000,
            }),
        );
        write_heartbeat_state(
            &harness_home,
            "progress-delivery-loop",
            serde_json::json!({
                "status": "running",
                "processId": pid,
                "atMs": now_ms - 10_000,
                "detail": "stale heartbeat",
            }),
        );

        let report = reconcile_supervisor_inventory(SupervisorInventoryOptions {
            harness_home: harness_home.clone(),
            desired_services: vec![SupervisorInventoryServiceConfig {
                enabled: true,
                service_id: "progress-delivery-loop".to_string(),
                service_kind: "progress-delivery".to_string(),
                args: Vec::new(),
                priority: "standard".to_string(),
                restart_delay_ms: 15_000,
                heartbeat_timeout_ms: Some(1_000),
            }],
            now_ms: Some(now_ms),
            default_heartbeat_timeout_ms: Some(120_000),
        })
        .unwrap();

        assert_eq!(report.stale.len(), 1);
        assert_eq!(report.launch_commands.len(), 1);
        assert!(
            report
                .launch_commands
                .iter()
                .any(|item| item.service_id == "progress-delivery-loop")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn dead_pid_desired_running_classified_restartable() {
        let root = temp_root("dead_pid_desired_running_classified_restartable");
        let harness_home = root.join(".agent-harness");
        let now_ms = 10_000;
        write_service_state(
            &harness_home,
            "progress-delivery-loop",
            serde_json::json!({
                "schema": "agent-harness.supervisor-service-state.v1",
                "serviceId": "progress-delivery-loop",
                "serviceKind": "progress",
                "status": "running",
                "desiredState": "running",
                "actualState": "running",
                "pid": -1,
                "lastHeartbeatAtMs": now_ms,
            }),
        );

        let report = reconcile_supervisor_inventory(SupervisorInventoryOptions {
            harness_home: harness_home.clone(),
            desired_services: vec![SupervisorInventoryServiceConfig {
                enabled: true,
                service_id: "progress-delivery-loop".to_string(),
                service_kind: "progress-delivery".to_string(),
                args: Vec::new(),
                priority: "standard".to_string(),
                restart_delay_ms: 15_000,
                heartbeat_timeout_ms: Some(120_000),
            }],
            now_ms: Some(now_ms),
            default_heartbeat_timeout_ms: Some(120_000),
        })
        .unwrap();

        assert_eq!(report.stale.len(), 1);
        let summary = report.stale.first().unwrap();
        assert_eq!(summary.process_id, Some(-1));
        assert_eq!(summary.process_alive, Some(false));
        assert_eq!(report.launch_commands.len(), 1);
        assert_eq!(
            report
                .launch_commands
                .first()
                .map(|item| item.service_id.as_str()),
            Some("progress-delivery-loop")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn inventory_reports_running_when_heartbeat_is_recent() {
        let root = temp_root("inventory_reports_running_when_heartbeat_is_recent");
        let harness_home = root.join(".agent-harness");
        let pid = i64::from(std::process::id());
        let now_ms = 10_000;
        write_service_state(
            &harness_home,
            "worker-loop",
            serde_json::json!({
                "schema": "agent-harness.supervisor-service-state.v1",
                "serviceId": "worker-loop",
                "serviceKind": "worker",
                "status": "running",
                "desiredState": "running",
                "actualState": "running",
                "pid": pid,
                "lastHeartbeatAtMs": now_ms - 100,
            }),
        );
        write_heartbeat_state(
            &harness_home,
            "worker-loop",
            serde_json::json!({
                "status": "running",
                "processId": pid,
                "atMs": now_ms - 100,
                "detail": "fresh heartbeat",
            }),
        );

        let report = reconcile_supervisor_inventory(SupervisorInventoryOptions {
            harness_home: harness_home.clone(),
            desired_services: vec![SupervisorInventoryServiceConfig {
                enabled: true,
                service_id: "worker-loop".to_string(),
                service_kind: "worker".to_string(),
                args: Vec::new(),
                priority: "standard".to_string(),
                restart_delay_ms: 15_000,
                heartbeat_timeout_ms: Some(10_000),
            }],
            now_ms: Some(now_ms),
            default_heartbeat_timeout_ms: Some(120_000),
        })
        .unwrap();

        assert_eq!(report.running.len(), 1);
        assert!(report.launch_commands.is_empty());
        assert_eq!(
            report.running.first().map(|item| item.service_id.as_str()),
            Some("worker-loop")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn inventory_restarts_mismatched_generation_even_with_fresh_heartbeat() {
        let root = temp_root("inventory_restarts_mismatched_generation_even_with_fresh_heartbeat");
        let harness_home = root.join(".agent-harness");
        let pid = i64::from(std::process::id());
        let now_ms = 10_000;
        write_service_state(
            &harness_home,
            "discord-gateway-loop",
            serde_json::json!({
                "schema": "agent-harness.supervisor-service-state.v1",
                "serviceId": "discord-gateway-loop",
                "serviceKind": "discord-gateway",
                "status": "running",
                "desiredState": "running",
                "actualState": "running",
                "pid": pid,
                "generationId": "generation-old",
                "launchOwner": "windows-runtime-runner",
                "observedOnly": false,
                "lastHeartbeatAtMs": now_ms - 100,
            }),
        );
        write_heartbeat_state(
            &harness_home,
            "discord-gateway-loop",
            serde_json::json!({
                "status": "heartbeat",
                "processId": pid,
                "generationId": "generation-new",
                "launchOwner": "windows-runtime-runner",
                "observedOnly": false,
                "atMs": now_ms - 50,
                "detail": "fresh wrong-generation heartbeat",
            }),
        );

        let report = reconcile_supervisor_inventory(SupervisorInventoryOptions {
            harness_home: harness_home.clone(),
            desired_services: vec![SupervisorInventoryServiceConfig {
                enabled: true,
                service_id: "discord-gateway-loop".to_string(),
                service_kind: "discord-gateway".to_string(),
                args: Vec::new(),
                priority: "standard".to_string(),
                restart_delay_ms: 15_000,
                heartbeat_timeout_ms: Some(1_000),
            }],
            now_ms: Some(now_ms),
            default_heartbeat_timeout_ms: Some(120_000),
        })
        .unwrap();

        assert_eq!(report.stale.len(), 1);
        assert!(report.running.is_empty());
        assert_eq!(report.launch_commands.len(), 1);
        let summary = report.stale.first().unwrap();
        assert_eq!(summary.generation_id.as_deref(), Some("generation-old"));
        assert_eq!(
            summary.heartbeat_generation_id.as_deref(),
            Some("generation-new")
        );
        assert!(summary.generation_mismatch);
        assert!(summary.ownership_conflict);
        assert_eq!(
            summary.ownership_conflict_reason.as_deref(),
            Some("generation-mismatch")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn two_live_parents_surface_ownership_conflict() {
        inventory_restarts_mismatched_generation_even_with_fresh_heartbeat();
    }

    #[test]
    fn inventory_marks_observed_only_service_as_not_managed_owner() {
        let root = temp_root("inventory_marks_observed_only_service_as_not_managed_owner");
        let harness_home = root.join(".agent-harness");
        let pid = i64::from(std::process::id());
        let now_ms = 10_000;
        write_service_state(
            &harness_home,
            "telegram-loop",
            serde_json::json!({
                "schema": "agent-harness.supervisor-service-state.v1",
                "serviceId": "telegram-loop",
                "serviceKind": "telegram-ingress",
                "status": "running",
                "desiredState": "running",
                "actualState": "running",
                "pid": pid,
                "generationId": "observed-generation",
                "launchOwner": "manual-observer",
                "observedOnly": true,
                "lastHeartbeatAtMs": now_ms - 100,
            }),
        );
        write_heartbeat_state(
            &harness_home,
            "telegram-loop",
            serde_json::json!({
                "status": "heartbeat",
                "processId": pid,
                "generationId": "observed-generation",
                "launchOwner": "manual-observer",
                "observedOnly": true,
                "atMs": now_ms - 50,
                "detail": "fresh observe-only loop heartbeat",
            }),
        );

        let report = reconcile_supervisor_inventory(SupervisorInventoryOptions {
            harness_home: harness_home.clone(),
            desired_services: vec![SupervisorInventoryServiceConfig {
                enabled: true,
                service_id: "telegram-loop".to_string(),
                service_kind: "telegram-ingress".to_string(),
                args: Vec::new(),
                priority: "standard".to_string(),
                restart_delay_ms: 15_000,
                heartbeat_timeout_ms: Some(1_000),
            }],
            now_ms: Some(now_ms),
            default_heartbeat_timeout_ms: Some(120_000),
        })
        .unwrap();

        assert_eq!(report.stale.len(), 1);
        assert!(report.running.is_empty());
        assert_eq!(report.launch_commands.len(), 1);
        let summary = report.stale.first().unwrap();
        assert_eq!(summary.launch_owner.as_deref(), Some("manual-observer"));
        assert_eq!(summary.observed_only, Some(true));
        assert!(summary.ownership_conflict);
        assert_eq!(
            summary.ownership_conflict_reason.as_deref(),
            Some("observed-only-owner")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn inventory_prefers_fresh_loop_heartbeat_over_stale_service_state() {
        let root = temp_root("inventory_prefers_fresh_loop_heartbeat_over_stale_service_state");
        let harness_home = root.join(".agent-harness");
        let pid = i64::from(std::process::id());
        let now_ms = 10_000;
        let stale_stop_file = harness_home.join("stale-discord-gateway.stop");
        let fresh_stop_file = harness_home.join("fresh-discord-gateway.stop");
        write_service_state(
            &harness_home,
            "discord-gateway-loop",
            serde_json::json!({
                "schema": "agent-harness.supervisor-service-state.v1",
                "serviceId": "discord-gateway-loop",
                "serviceKind": "discord-gateway",
                "status": "spawning",
                "desiredState": "running",
                "actualState": "spawning",
                "pid": pid,
                "parentPid": 111,
                "processStartTimeMs": now_ms - 20_000,
                "watchedStopFile": stale_stop_file,
                "lastHeartbeatAtMs": now_ms - 10_000,
            }),
        );
        write_heartbeat_state(
            &harness_home,
            "discord-gateway-loop",
            serde_json::json!({
                "status": "heartbeat",
                "processId": pid,
                "parentPid": 222,
                "processStartTimeMs": now_ms - 500,
                "watchedStopFile": fresh_stop_file,
                "atMs": now_ms - 100,
                "detail": "fresh gateway loop heartbeat",
            }),
        );

        let report = reconcile_supervisor_inventory(SupervisorInventoryOptions {
            harness_home: harness_home.clone(),
            desired_services: vec![SupervisorInventoryServiceConfig {
                enabled: true,
                service_id: "discord-gateway-loop".to_string(),
                service_kind: "discord-gateway".to_string(),
                args: Vec::new(),
                priority: "standard".to_string(),
                restart_delay_ms: 15_000,
                heartbeat_timeout_ms: Some(1_000),
            }],
            now_ms: Some(now_ms),
            default_heartbeat_timeout_ms: Some(120_000),
        })
        .unwrap();

        assert_eq!(report.running.len(), 1);
        assert!(report.stale.is_empty());
        assert!(report.launch_commands.is_empty());
        let summary = report.running.first().unwrap();
        assert_eq!(summary.last_heartbeat_at_ms, Some(now_ms - 100));
        assert_eq!(summary.age_ms, Some(100));
        assert_eq!(summary.status.as_deref(), Some("heartbeat"));
        assert_eq!(summary.parent_pid, Some(222));
        assert_eq!(summary.process_start_time_ms, Some(now_ms - 500));
        assert_eq!(summary.watched_stop_file.as_ref(), Some(&fresh_stop_file));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn child_heartbeat_same_generation_promotes_spawning_to_running() {
        inventory_prefers_fresh_loop_heartbeat_over_stale_service_state();
    }

    fn write_service_state(harness_home: &Path, service_id: &str, value: Value) {
        let dir = harness_home
            .join("state")
            .join("supervisor")
            .join("services");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join(format!("{service_id}.json")),
            serde_json::to_string(&value).unwrap(),
        )
        .unwrap();
    }

    fn write_heartbeat_state(harness_home: &Path, service_id: &str, value: Value) {
        let dir = harness_home
            .join("state")
            .join("supervisor")
            .join("loop-heartbeats");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join(format!("{service_id}.json")),
            serde_json::to_string(&value).unwrap(),
        )
        .unwrap();
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-core-supervisor-inventory-{test_name}-{nanos}"
        ))
    }
}
