use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::Value;

use crate::{
    HarnessGovernedTransitionStatus, HarnessStatusOptions, SkillDoctorStatus, SkillDoctorSummary,
    collect_harness_status, harness_config_candidates,
    loop_health::{process_alive_for_pid, process_start_time_ms_for_pid},
    supervisor_topology::{SupervisorTopologyOverrides, derive_supervisor_topology},
};

const HEALTHZ_SCHEMA: &str = "agent-harness.healthz.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthzOptions {
    pub harness_home: PathBuf,
    pub now_ms: i64,
    pub loop_stale_ms: i64,
    pub require_writable_state: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthzReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub ready: bool,
    pub live: bool,
    pub readiness_ready: bool,
    pub queue: HealthzQueue,
    pub goal_closures: HarnessGovernedTransitionStatus,
    pub session_transitions: HarnessGovernedTransitionStatus,
    pub outbox: HealthzOutbox,
    pub loops: Vec<HealthzLoop>,
    pub supervisor_services: Vec<HealthzSupervisorService>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reconcile_keeper: Option<HealthzReconcileKeeper>,
    pub skills: HealthzSkills,
    pub state: HealthzState,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthzQueue {
    pub queued: usize,
    pub open: usize,
    pub waiting: usize,
    pub prepared: usize,
    pub completed: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthzOutbox {
    pub pending: usize,
    pub retryable: usize,
    pub delivered: usize,
    pub invalid: usize,
    pub sampled: bool,
    pub sampled_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthzLoop {
    pub name: String,
    pub present: bool,
    pub corrupt: bool,
    pub parse_error: Option<String>,
    pub stale: bool,
    pub age_ms: Option<i64>,
    pub status: Option<String>,
    pub process_id: Option<i64>,
    pub process_alive: Option<bool>,
    pub stop_file_present: bool,
    pub stop_file_reason: Option<String>,
    pub stop_file_service_id: Option<String>,
    pub stop_file_created_by: Option<String>,
    pub stop_file_created_at_ms: Option<i64>,
    pub stop_file_expires_at_ms: Option<i64>,
    pub stop_file_persistent: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthzSupervisorService {
    pub service_id: String,
    pub service_kind: Option<String>,
    pub generation_id: Option<String>,
    pub process_id: Option<i64>,
    pub process_alive: Option<bool>,
    pub supervisor_process_id: Option<i64>,
    pub parent_pid: Option<i64>,
    pub watched_stop_file: Option<PathBuf>,
    pub corrupt: bool,
    pub parse_error: Option<String>,
    pub stale: bool,
    pub age_ms: Option<i64>,
    pub status: Option<String>,
    pub desired_state: Option<String>,
    pub actual_state: Option<String>,
    pub iteration: Option<i64>,
    pub started_at_ms: Option<i64>,
    pub last_heartbeat_at_ms: Option<i64>,
    pub last_successful_iteration_at_ms: Option<i64>,
    pub last_exit_at_ms: Option<i64>,
    pub last_exit_code: Option<i64>,
    pub last_error_class: Option<String>,
    pub restart_count: Option<i64>,
    pub backoff_until_ms: Option<i64>,
    pub service_priority: Option<String>,
    pub delivery_lane: Option<String>,
    pub restart_delay_ms: Option<i64>,
    pub memory_gate_action: Option<String>,
    pub memory_gate_reason: Option<String>,
    pub observed_only: Option<bool>,
    pub ownership_conflict: bool,
    pub ownership_conflict_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthzReconcileKeeper {
    pub required: bool,
    pub plan_file: PathBuf,
    pub state_file: PathBuf,
    pub plan_present: bool,
    pub plan_valid: bool,
    pub state_matches_plan: bool,
    pub singleton_lease_held: bool,
    pub present: bool,
    pub corrupt: bool,
    pub parse_error: Option<String>,
    pub stale: bool,
    pub age_ms: Option<i64>,
    pub status: Option<String>,
    pub process_id: Option<i64>,
    pub process_alive: Option<bool>,
    pub process_instance_matches: bool,
    pub hold_requested: bool,
    pub auto_reconcile: bool,
    pub applied: bool,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthzState {
    pub state_dir: PathBuf,
    pub writable: bool,
    pub disk_free_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthzSkills {
    pub ready: bool,
    pub status: SkillDoctorStatus,
    pub summary: SkillDoctorSummary,
    pub findings: usize,
    pub error_findings: usize,
    pub warning_findings: usize,
    pub doctor_receipts_file: PathBuf,
}

pub fn collect_healthz(options: HealthzOptions) -> io::Result<HealthzReport> {
    let status = collect_harness_status(HarnessStatusOptions {
        harness_home: options.harness_home.clone(),
    })?;
    let mut warnings = status.warnings.clone();
    let state = state_health(&options)?;
    if options.require_writable_state && !state.writable {
        warnings.push("state directory is not writable".to_string());
    }
    let loops: Vec<_> = status
        .loops
        .heartbeats
        .iter()
        .map(|loop_status| {
            let unhealthy_status = loop_status.status.as_deref().is_some_and(|status| {
                status == "stopped"
                    || status == "closed"
                    || status.contains("error")
                    || status.contains("fail")
            });
            let stale = !loop_status.present
                || loop_status.corrupt
                || loop_status
                    .age_ms
                    .is_some_and(|age_ms| age_ms > options.loop_stale_ms)
                || unhealthy_status
                || loop_status.process_alive == Some(false)
                || loop_status.stop_file_present;
            HealthzLoop {
                name: loop_status.name.clone(),
                present: loop_status.present,
                corrupt: loop_status.corrupt,
                parse_error: loop_status.parse_error.clone(),
                stale,
                age_ms: loop_status.age_ms,
                status: loop_status.status.clone(),
                process_id: loop_status.process_id,
                process_alive: loop_status.process_alive,
                stop_file_present: loop_status.stop_file_present,
                stop_file_reason: loop_status.stop_file_reason.clone(),
                stop_file_service_id: loop_status.stop_file_service_id.clone(),
                stop_file_created_by: loop_status.stop_file_created_by.clone(),
                stop_file_created_at_ms: loop_status.stop_file_created_at_ms,
                stop_file_expires_at_ms: loop_status.stop_file_expires_at_ms,
                stop_file_persistent: loop_status.stop_file_persistent,
            }
        })
        .collect();
    let supervisor_services: Vec<_> = status
        .loops
        .services
        .iter()
        .map(|service| {
            let unhealthy_state = service
                .actual_state
                .as_deref()
                .or(service.status.as_deref())
                .is_some_and(|state| {
                    state == "stopped"
                        || state == "closed"
                        || state.contains("error")
                        || state.contains("fail")
                });
            let stale = service.corrupt
                || service
                    .age_ms
                    .is_some_and(|age_ms| age_ms > options.loop_stale_ms)
                || unhealthy_state
                || service.process_alive == Some(false)
                || service.ownership_conflict;
            HealthzSupervisorService {
                service_id: service.service_id.clone(),
                service_kind: service.service_kind.clone(),
                generation_id: service.generation_id.clone(),
                process_id: service.process_id,
                process_alive: service.process_alive,
                supervisor_process_id: service.supervisor_process_id,
                parent_pid: service.parent_pid,
                watched_stop_file: service.watched_stop_file.clone(),
                corrupt: service.corrupt,
                parse_error: service.parse_error.clone(),
                stale,
                age_ms: service.age_ms,
                status: service.status.clone(),
                desired_state: service.desired_state.clone(),
                actual_state: service.actual_state.clone(),
                iteration: service.iteration,
                started_at_ms: service.started_at_ms,
                last_heartbeat_at_ms: service.last_heartbeat_at_ms,
                last_successful_iteration_at_ms: service.last_successful_iteration_at_ms,
                last_exit_at_ms: service.last_exit_at_ms,
                last_exit_code: service.last_exit_code,
                last_error_class: service.last_error_class.clone(),
                restart_count: service.restart_count,
                backoff_until_ms: service.backoff_until_ms,
                service_priority: service.service_priority.clone(),
                delivery_lane: service.delivery_lane.clone(),
                restart_delay_ms: service.restart_delay_ms,
                memory_gate_action: service.memory_gate_action.clone(),
                memory_gate_reason: service.memory_gate_reason.clone(),
                observed_only: service.observed_only,
                ownership_conflict: service.ownership_conflict,
                ownership_conflict_reason: service.ownership_conflict_reason.clone(),
            }
        })
        .collect();
    let (supervisor_topology_exact, supervisor_topology_findings) =
        exact_reconcile_managed_topology(&options.harness_home, &supervisor_services)?;
    warnings.extend(supervisor_topology_findings);
    let reconcile_keeper =
        reconcile_keeper_health(&options.harness_home, options.now_ms, options.loop_stale_ms)?;
    if reconcile_keeper.as_ref().is_some_and(|keeper| keeper.stale) {
        warnings.push(
            "supervisor reconcile keeper is not live; all child loops can disappear together and remain down"
                .to_string(),
        );
    }
    let runtime_loop_unhealthy = loops.iter().any(|item| {
        item.name == "runtime-loop"
            && (item.stale
                || item
                    .status
                    .as_deref()
                    .is_some_and(|status| matches!(status, "error" | "stopped" | "stopping")))
    });
    if loops
        .iter()
        .any(|item| item.name == "runtime-loop" && item.status.as_deref() == Some("safe-mode"))
    {
        warnings.push("runtime-loop is in safe-mode with reduced concurrency".to_string());
    }
    if runtime_loop_unhealthy {
        warnings.push(
            "runtime-loop is not ready; channel ingress may queue without replies".to_string(),
        );
    }
    for (label, transition) in [
        ("goal closure", &status.runtime.goal_closures),
        (
            "channel session transition",
            &status.runtime.session_transitions,
        ),
    ] {
        if transition.failed > 0 {
            warnings.push(format!(
                "{label} reconciliation has {} retry-pending or failed item(s)",
                transition.failed
            ));
        }
        if transition
            .oldest_pending_age_ms
            .is_some_and(|age_ms| age_ms > options.loop_stale_ms)
        {
            warnings.push(format!(
                "{label} reconciliation has pending work older than {} ms",
                options.loop_stale_ms
            ));
        }
    }
    let skills_ready = status.skills.status != SkillDoctorStatus::Error;
    if !skills_ready {
        warnings.push("skill doctor is not ready; autonomous skill apply is blocked".to_string());
    }
    if loops
        .iter()
        .any(|item| item.name == "progress-delivery-loop" && item.stale)
    {
        warnings.push(
            "progress-delivery-loop is degraded; final reply delivery health is evaluated separately"
                .to_string(),
        );
    }
    let live = loops
        .iter()
        .filter(|item| item.name != "progress-delivery-loop")
        .all(|item| !item.stale)
        && !runtime_loop_unhealthy
        && supervisor_topology_exact
        && reconcile_keeper.as_ref().is_none_or(|keeper| !keeper.stale);
    let ready =
        status.ready && live && skills_ready && (!options.require_writable_state || state.writable);
    Ok(HealthzReport {
        schema: HEALTHZ_SCHEMA,
        harness_home: options.harness_home,
        ready,
        live,
        readiness_ready: status.ready,
        queue: HealthzQueue {
            queued: status.runtime.queued_items,
            open: status.runtime.open_items,
            waiting: status.runtime.waiting_items,
            prepared: status.runtime.prepared_items,
            completed: status.runtime.completed_items,
        },
        goal_closures: status.runtime.goal_closures.clone(),
        session_transitions: status.runtime.session_transitions.clone(),
        outbox: HealthzOutbox {
            pending: status.channels.outbox.all.pending,
            retryable: status.channels.outbox.all.failed_retryable,
            delivered: status.channels.outbox.all.delivered,
            invalid: status.channels.outbox.all.invalid_lines,
            sampled: status.channels.outbox.all.sampled,
            sampled_bytes: status.channels.outbox.all.sampled_bytes,
        },
        loops,
        supervisor_services,
        reconcile_keeper,
        skills: HealthzSkills {
            ready: skills_ready,
            status: status.skills.status,
            summary: status.skills.summary.clone(),
            findings: status.skills.findings,
            error_findings: status.skills.error_findings,
            warning_findings: status.skills.warning_findings,
            doctor_receipts_file: status.skills.doctor_receipts_file.clone(),
        },
        state,
        warnings,
    })
}

fn exact_reconcile_managed_topology(
    harness_home: &Path,
    services: &[HealthzSupervisorService],
) -> io::Result<(bool, Vec<String>)> {
    let Some(config_file) = harness_config_candidates(harness_home)
        .into_iter()
        .find(|path| path.is_file())
    else {
        return Ok((true, Vec::new()));
    };
    let config: Value = serde_json::from_slice(&fs::read(&config_file)?).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("harness config is not valid JSON: {error}"),
        )
    })?;
    let reconcile_managed = config
        .pointer("/supervisor/manageAllLoops")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if !reconcile_managed {
        return Ok((true, Vec::new()));
    }
    let expected = derive_supervisor_topology(&config, &SupervisorTopologyOverrides::default())?
        .into_iter()
        .map(|(service_id, entry)| (service_id, entry.service_kind))
        .collect::<BTreeMap<_, _>>();
    let mut findings = Vec::new();
    if expected.is_empty() {
        findings.push(
            "reconcile-managed shared topology is empty; health cannot pass vacuously".to_string(),
        );
    }
    let mut actual = BTreeMap::new();
    for service in services {
        if service.service_id.trim().is_empty() {
            findings.push("supervisor service inventory contains an empty serviceId".to_string());
            continue;
        }
        if actual
            .insert(
                service.service_id.clone(),
                service.service_kind.clone().unwrap_or_default(),
            )
            .is_some()
        {
            findings.push(format!(
                "supervisor service inventory contains duplicate `{}`",
                service.service_id
            ));
        }
        if service.corrupt
            || service.observed_only != Some(false)
            || service.ownership_conflict
            || service.desired_state.as_deref() != Some("running")
            || service.stale
        {
            findings.push(format!(
                "supervisor service `{}` is invalid, stale, observed-only, or not deployment-owned desired-running",
                service.service_id
            ));
        }
    }
    let expected_ids = expected.keys().cloned().collect::<BTreeSet<_>>();
    let actual_ids = actual.keys().cloned().collect::<BTreeSet<_>>();
    let missing = expected_ids
        .difference(&actual_ids)
        .cloned()
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        findings.push(format!(
            "reconcile-managed supervisor inventory is missing: {}",
            missing.join(", ")
        ));
    }
    let unexpected = actual_ids
        .difference(&expected_ids)
        .cloned()
        .collect::<Vec<_>>();
    if !unexpected.is_empty() {
        findings.push(format!(
            "reconcile-managed supervisor inventory has unexpected identities: {}",
            unexpected.join(", ")
        ));
    }
    for (service_id, expected_kind) in &expected {
        if let Some(actual_kind) = actual.get(service_id)
            && actual_kind != expected_kind
        {
            findings.push(format!(
                "supervisor service `{service_id}` kind mismatch: expected `{expected_kind}`, found `{actual_kind}`"
            ));
        }
    }
    Ok((findings.is_empty(), findings))
}

fn reconcile_keeper_health(
    harness_home: &Path,
    now_ms: i64,
    stale_after_ms: i64,
) -> io::Result<Option<HealthzReconcileKeeper>> {
    let manage_all_loops = harness_config_candidates(harness_home)
        .into_iter()
        .find(|path| path.is_file())
        .map(fs::read_to_string)
        .transpose()?
        .map(|text| {
            serde_json::from_str::<Value>(&text).map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("harness config is not valid JSON: {error}"),
                )
            })
        })
        .transpose()?
        .and_then(|config| {
            config
                .pointer("/supervisor/manageAllLoops")
                .and_then(Value::as_bool)
        })
        .unwrap_or(false);
    if !manage_all_loops {
        return Ok(None);
    }

    let state_file = harness_home
        .join("state")
        .join("supervisor")
        .join("reconcile-keeper.json");
    let plan_file = harness_home
        .join("state")
        .join("supervisor")
        .join("windows-reconcile-keeper")
        .join("keeper-plan.json");
    let plan_present_on_disk = plan_file.is_file();
    let text = match fs::read_to_string(&state_file) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(Some(HealthzReconcileKeeper {
                required: true,
                plan_file,
                state_file,
                plan_present: plan_present_on_disk,
                plan_valid: false,
                state_matches_plan: false,
                singleton_lease_held: false,
                present: false,
                corrupt: false,
                parse_error: None,
                stale: true,
                age_ms: None,
                status: None,
                process_id: None,
                process_alive: None,
                process_instance_matches: false,
                hold_requested: false,
                auto_reconcile: false,
                applied: false,
                reason: Some("reconcile keeper state is missing".to_string()),
            }));
        }
        Err(error) => return Err(error),
    };
    let state = match serde_json::from_str::<Value>(&text) {
        Ok(state) => state,
        Err(error) => {
            return Ok(Some(HealthzReconcileKeeper {
                required: true,
                plan_file,
                state_file,
                plan_present: plan_present_on_disk,
                plan_valid: false,
                state_matches_plan: false,
                singleton_lease_held: false,
                present: true,
                corrupt: true,
                parse_error: Some(error.to_string()),
                stale: true,
                age_ms: None,
                status: None,
                process_id: None,
                process_alive: None,
                process_instance_matches: false,
                hold_requested: false,
                auto_reconcile: false,
                applied: false,
                reason: Some("reconcile keeper state is corrupt".to_string()),
            }));
        }
    };
    let schema_valid = state.get("schema").and_then(Value::as_str)
        == Some("agent-harness.supervisor-reconcile-keeper.v1");
    let status = state
        .get("status")
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let process_id = state.get("pid").and_then(Value::as_i64);
    let process_alive = process_id.and_then(process_alive_for_pid);
    let recorded_start = state.get("processStartTimeMs").and_then(Value::as_i64);
    let observed_start = process_id.and_then(process_start_time_ms_for_pid);
    let process_instance_matches =
        recorded_start.is_some() && observed_start.is_some() && recorded_start == observed_start;
    let heartbeat_at_ms = state.get("lastHeartbeatAtMs").and_then(Value::as_i64);
    let age_ms = heartbeat_at_ms.map(|at_ms| now_ms.saturating_sub(at_ms));
    let heartbeat_fresh =
        age_ms.is_some_and(|age_ms| (-10_000..=stale_after_ms.max(0)).contains(&age_ms));
    let hold_requested = state
        .get("holdRequested")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        || harness_home
            .join("state/supervisor/stop/supervisor-reconcile.stop")
            .is_file();
    let auto_reconcile = state
        .get("autoReconcile")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let applied = state
        .get("applied")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let plan = fs::read_to_string(&plan_file)
        .ok()
        .and_then(|text| serde_json::from_str::<Value>(&text).ok());
    let plan_present = plan_file.is_file();
    let plan_valid = plan.as_ref().is_some_and(|plan| {
        plan.get("schema").and_then(Value::as_str)
            == Some("agent-harness.windows-reconcile-keeper-plan.v1")
            && crate::supervisor_keeper::validate_live_windows_reconcile_keeper_plan(plan).is_ok()
    });
    let state_matches_plan = plan.as_ref().is_some_and(|plan| {
        state.get("planDigest").and_then(Value::as_str)
            == plan.get("planDigest").and_then(Value::as_str)
            && state.get("actionDigest").and_then(Value::as_str)
                == plan.pointer("/action/digest").and_then(Value::as_str)
            && state.get("taskName").and_then(Value::as_str)
                == plan.pointer("/task/name").and_then(Value::as_str)
            && state.get("leaseFile").and_then(Value::as_str)
                == plan.get("leaseFile").and_then(Value::as_str)
            && state.get("ownershipMode").and_then(Value::as_str) == Some("scheduled-task-plan")
    });
    let singleton_lease_held = plan
        .as_ref()
        .and_then(|plan| plan.get("leaseFile"))
        .and_then(Value::as_str)
        .is_some_and(|path| keeper_singleton_lease_is_held(Path::new(path)));
    let stale = !schema_valid
        || status.as_deref() != Some("running")
        || process_alive != Some(true)
        || !process_instance_matches
        || !heartbeat_fresh
        || hold_requested
        || !auto_reconcile
        || !applied
        || !plan_present
        || !plan_valid
        || !state_matches_plan
        || !singleton_lease_held;
    Ok(Some(HealthzReconcileKeeper {
        required: true,
        plan_file,
        state_file,
        plan_present,
        plan_valid,
        state_matches_plan,
        singleton_lease_held,
        present: true,
        corrupt: !schema_valid,
        parse_error: (!schema_valid).then(|| "unsupported reconcile keeper schema".to_string()),
        stale,
        age_ms,
        status,
        process_id,
        process_alive,
        process_instance_matches,
        hold_requested,
        auto_reconcile,
        applied,
        reason: state
            .get("reason")
            .and_then(Value::as_str)
            .map(ToString::to_string),
    }))
}

#[cfg(windows)]
fn keeper_singleton_lease_is_held(path: &Path) -> bool {
    if !path.is_file() {
        return false;
    }
    match fs::OpenOptions::new()
        .read(true)
        .write(true)
        .share_mode(0)
        .open(path)
    {
        Ok(_) => false,
        Err(error) => matches!(error.raw_os_error(), Some(32) | Some(33)),
    }
}

#[cfg(not(windows))]
fn keeper_singleton_lease_is_held(path: &Path) -> bool {
    path.is_file()
}

fn state_health(options: &HealthzOptions) -> io::Result<HealthzState> {
    let state_dir = options.harness_home.join("state");
    fs::create_dir_all(&state_dir)?;
    let probe = state_dir.join(".healthz-write-probe.tmp");
    let writable = match fs::write(&probe, format!("{}\n", options.now_ms)) {
        Ok(()) => {
            let _ = fs::remove_file(&probe);
            true
        }
        Err(_) => false,
    };
    Ok(HealthzState {
        state_dir,
        writable,
        disk_free_bytes: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::write_json_atomic;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn reconcile_managed_keeper_health_rejects_empty_service_inventory_without_vacuous_pass() {
        let root = temp_root("health_exact_topology_empty");
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            r#"{"supervisor":{"manageAllLoops":true}}"#,
        )
        .unwrap();
        let (exact, findings) = exact_reconcile_managed_topology(&harness_home, &[]).unwrap();
        assert!(!exact);
        assert!(findings.iter().any(|finding| {
            finding.contains("missing")
                && finding.contains("runtime-loop")
                && finding.contains("discord-gateway-loop")
        }));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn healthz_reports_stale_loop_and_queue_depth() {
        let root = temp_root("healthz_reports_stale_loop_and_queue_depth");
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(harness_home.join("state").join("runtime-queue")).unwrap();
        fs::create_dir_all(
            harness_home
                .join("state")
                .join("supervisor")
                .join("loop-heartbeats"),
        )
        .unwrap();
        fs::write(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("pending.jsonl"),
            r#"{"queueId":"q-1","status":"queued"}"#,
        )
        .unwrap();
        write_json_atomic(
            &harness_home
                .join("state")
                .join("supervisor")
                .join("loop-heartbeats")
                .join("runtime-loop.json"),
            &serde_json::json!({
                "status": "running",
                "iteration": 7,
                "processId": 123,
                "atMs": 1_000
            }),
        )
        .unwrap();

        let report = collect_healthz(HealthzOptions {
            harness_home: harness_home.clone(),
            now_ms: 10_000,
            loop_stale_ms: 1_000,
            require_writable_state: true,
        })
        .unwrap();

        assert!(!report.ready);
        assert!(!report.live);
        assert_eq!(report.queue.queued, 1);
        assert!(report.state.writable);
        assert!(
            report
                .loops
                .iter()
                .any(|item| item.name == "runtime-loop" && item.stale)
        );
        assert!(report.skills.ready);
        assert_eq!(report.skills.status, SkillDoctorStatus::Warn);
        assert_eq!(report.skills.summary.total_skills, 0);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn healthz_includes_observe_only_supervisor_services() {
        let root = temp_root("healthz_includes_observe_only_supervisor_services");
        let harness_home = root.join(".agent-harness");
        let state = harness_home.join("state");
        fs::create_dir_all(state.join("runtime-queue")).unwrap();
        fs::create_dir_all(state.join("channels")).unwrap();
        fs::create_dir_all(state.join("logs")).unwrap();
        fs::create_dir_all(state.join("plugin-sidecar")).unwrap();
        fs::create_dir_all(state.join("memory")).unwrap();
        let services_dir = state.join("supervisor").join("services");
        fs::create_dir_all(&services_dir).unwrap();
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let watched_stop_file = harness_home
            .join("state")
            .join("supervisor")
            .join("stop")
            .join("runtime-loop.live.stop");
        write_json_atomic(
            &services_dir.join("runtime-loop.json"),
            &serde_json::json!({
                "schema": "agent-harness.supervisor-service-state.v1",
                "serviceId": "runtime-loop",
                "serviceKind": "runtime",
                "generationId": "runtime-loop-test-generation",
                "pid": std::process::id(),
                "supervisorPid": 4242,
                "parentPid": 4343,
                "startedAtMs": now_ms - 1_000,
                "processStartTimeMs": now_ms - 1_000,
                "watchedStopFile": watched_stop_file,
                "lastHeartbeatAtMs": now_ms,
                "lastSuccessfulIterationAtMs": now_ms,
                "lastExitAtMs": now_ms - 100,
                "lastExitCode": 1,
                "lastErrorClass": "process-exit",
                "restartCount": 2,
                "backoffUntilMs": now_ms + 60_000,
                "servicePriority": "final-delivery",
                "deliveryLane": "final-outbox",
                "restartDelayMs": 15_000,
                "memoryGateDecision": {
                    "action": "pause-low-priority-service",
                    "reason": "resource-exhausted"
                },
                "iteration": 23,
                "status": "no-work",
                "desiredState": "running",
                "actualState": "no-work",
                "observedOnly": true
            }),
        )
        .unwrap();

        let report = collect_healthz(HealthzOptions {
            harness_home,
            now_ms,
            loop_stale_ms: 120_000,
            require_writable_state: false,
        })
        .unwrap();

        let runtime_service = report
            .supervisor_services
            .iter()
            .find(|service| service.service_id == "runtime-loop")
            .unwrap();
        assert!(!runtime_service.corrupt);
        assert!(runtime_service.stale);
        assert_eq!(runtime_service.service_kind.as_deref(), Some("runtime"));
        assert_eq!(
            runtime_service.generation_id.as_deref(),
            Some("runtime-loop-test-generation")
        );
        assert_eq!(
            runtime_service.process_id,
            Some(i64::from(std::process::id()))
        );
        assert_eq!(runtime_service.process_alive, Some(true));
        assert_eq!(runtime_service.supervisor_process_id, Some(4242));
        assert_eq!(runtime_service.parent_pid, Some(4343));
        assert_eq!(
            runtime_service.watched_stop_file.as_ref(),
            Some(&watched_stop_file)
        );
        assert_eq!(runtime_service.last_exit_at_ms, Some(now_ms - 100));
        assert_eq!(runtime_service.last_exit_code, Some(1));
        assert_eq!(
            runtime_service.last_error_class.as_deref(),
            Some("process-exit")
        );
        assert_eq!(runtime_service.restart_count, Some(2));
        assert_eq!(runtime_service.backoff_until_ms, Some(now_ms + 60_000));
        assert_eq!(
            runtime_service.service_priority.as_deref(),
            Some("final-delivery")
        );
        assert_eq!(
            runtime_service.delivery_lane.as_deref(),
            Some("final-outbox")
        );
        assert_eq!(runtime_service.restart_delay_ms, Some(15_000));
        assert_eq!(
            runtime_service.memory_gate_action.as_deref(),
            Some("pause-low-priority-service")
        );
        assert_eq!(
            runtime_service.memory_gate_reason.as_deref(),
            Some("resource-exhausted")
        );
        assert_eq!(runtime_service.iteration, Some(23));
        assert_eq!(runtime_service.status.as_deref(), Some("no-work"));
        assert_eq!(runtime_service.desired_state.as_deref(), Some("running"));
        assert_eq!(runtime_service.actual_state.as_deref(), Some("no-work"));
        assert_eq!(runtime_service.observed_only, Some(true));
        assert!(runtime_service.ownership_conflict);
        assert_eq!(
            runtime_service.ownership_conflict_reason.as_deref(),
            Some("observed-only-owner")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn healthz_prefers_fresh_loop_heartbeat_over_spawning_service_state() {
        let root = temp_root("healthz_prefers_fresh_loop_heartbeat_over_spawning_service_state");
        let harness_home = root.join(".agent-harness");
        let state = harness_home.join("state");
        fs::create_dir_all(state.join("runtime-queue")).unwrap();
        fs::create_dir_all(state.join("channels")).unwrap();
        fs::create_dir_all(state.join("logs")).unwrap();
        fs::create_dir_all(state.join("plugin-sidecar")).unwrap();
        fs::create_dir_all(state.join("memory")).unwrap();
        let services_dir = state.join("supervisor").join("services");
        let heartbeat_dir = state.join("supervisor").join("loop-heartbeats");
        fs::create_dir_all(&services_dir).unwrap();
        fs::create_dir_all(&heartbeat_dir).unwrap();
        let pid = i64::from(std::process::id());
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        write_json_atomic(
            &services_dir.join("discord-gateway-loop.json"),
            &serde_json::json!({
                "schema": "agent-harness.supervisor-service-state.v1",
                "serviceId": "discord-gateway-loop",
                "serviceKind": "discord-gateway",
                "generationId": "discord-gateway-loop-supervised-test",
                "pid": 0,
                "processId": 0,
                "supervisorPid": pid,
                "startedAtMs": now_ms - 10_000,
                "processStartTimeMs": now_ms - 10_000,
                "lastHeartbeatAtMs": now_ms - 10_000,
                "status": "spawning",
                "desiredState": "running",
                "actualState": "spawning",
                "detail": "starting Discord gateway subprocess",
                "launchOwner": "rust-supervisor-run",
                "observedOnly": false
            }),
        )
        .unwrap();
        write_json_atomic(
            &heartbeat_dir.join("discord-gateway-loop.json"),
            &serde_json::json!({
                "schema": "agent-harness.loop-heartbeat.v1",
                "name": "discord-gateway-loop",
                "status": "heartbeat",
                "processId": pid,
                "atMs": now_ms - 100,
                "detail": "Discord heartbeat ack"
            }),
        )
        .unwrap();

        let report = collect_healthz(HealthzOptions {
            harness_home,
            now_ms,
            loop_stale_ms: 1_000,
            require_writable_state: false,
        })
        .unwrap();

        let gateway_service = report
            .supervisor_services
            .iter()
            .find(|service| service.service_id == "discord-gateway-loop")
            .unwrap();
        assert!(!gateway_service.stale);
        assert_eq!(gateway_service.process_id, Some(pid));
        assert_eq!(gateway_service.process_alive, Some(true));
        assert_eq!(gateway_service.status.as_deref(), Some("heartbeat"));
        assert_eq!(gateway_service.actual_state.as_deref(), Some("running"));
        assert_eq!(gateway_service.last_heartbeat_at_ms, Some(now_ms - 100));
        assert!(
            gateway_service
                .age_ms
                .is_some_and(|age_ms| (100..120_000).contains(&age_ms))
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn healthz_reports_error_loop_as_not_live_even_when_fresh() {
        let root = temp_root("healthz_reports_error_loop_as_not_live_even_when_fresh");
        let harness_home = root.join(".agent-harness");
        let state = harness_home.join("state");
        fs::create_dir_all(state.join("runtime-queue")).unwrap();
        fs::create_dir_all(state.join("channels")).unwrap();
        fs::create_dir_all(state.join("logs")).unwrap();
        fs::create_dir_all(state.join("plugin-sidecar")).unwrap();
        fs::create_dir_all(state.join("memory")).unwrap();
        fs::create_dir_all(state.join("supervisor").join("loop-heartbeats")).unwrap();
        write_json_atomic(
            &state
                .join("supervisor")
                .join("loop-heartbeats")
                .join("runtime-loop.json"),
            &serde_json::json!({
                "status": "error",
                "iteration": 3,
                "processId": 42,
                "atMs": 10_000,
                "detail": "consecutiveErrors=5/5"
            }),
        )
        .unwrap();

        let report = collect_healthz(HealthzOptions {
            harness_home,
            now_ms: 10_500,
            loop_stale_ms: 120_000,
            require_writable_state: false,
        })
        .unwrap();

        assert!(!report.live);
        assert!(!report.ready);
        assert!(
            report
                .loops
                .iter()
                .any(|item| item.name == "runtime-loop" && item.stale)
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn healthz_reports_corrupt_heartbeat_loop_as_not_live() {
        let root = temp_root("healthz_reports_corrupt_heartbeat_loop_as_not_live");
        let harness_home = root.join(".agent-harness");
        let state = harness_home.join("state");
        fs::create_dir_all(state.join("runtime-queue")).unwrap();
        fs::create_dir_all(state.join("channels")).unwrap();
        fs::create_dir_all(state.join("logs")).unwrap();
        fs::create_dir_all(state.join("plugin-sidecar")).unwrap();
        fs::create_dir_all(state.join("memory")).unwrap();
        let heartbeat_dir = state.join("supervisor").join("loop-heartbeats");
        fs::create_dir_all(&heartbeat_dir).unwrap();
        fs::write(heartbeat_dir.join("runtime-loop.json"), b"\0\0\0").unwrap();

        let report = collect_healthz(HealthzOptions {
            harness_home,
            now_ms: 10_500,
            loop_stale_ms: 120_000,
            require_writable_state: false,
        })
        .unwrap();

        assert!(!report.live);
        assert!(!report.ready);
        let runtime_loop = report
            .loops
            .iter()
            .find(|item| item.name == "runtime-loop")
            .unwrap();
        assert!(runtime_loop.present);
        assert!(runtime_loop.corrupt);
        assert!(runtime_loop.parse_error.is_some());
        assert!(runtime_loop.stale);
        assert!(report.warnings.iter().any(
            |warning| warning.contains("runtime-loop heartbeat") && warning.contains("corrupt")
        ));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn healthz_keeps_live_when_only_progress_delivery_is_stale() {
        let root = temp_root("healthz_keeps_live_when_only_progress_delivery_is_stale");
        let harness_home = root.join(".agent-harness");
        let state = harness_home.join("state");
        fs::create_dir_all(state.join("runtime-queue")).unwrap();
        fs::create_dir_all(state.join("channels")).unwrap();
        fs::create_dir_all(state.join("logs")).unwrap();
        fs::create_dir_all(state.join("plugin-sidecar")).unwrap();
        fs::create_dir_all(state.join("memory")).unwrap();
        let heartbeat_dir = state.join("supervisor").join("loop-heartbeats");
        fs::create_dir_all(&heartbeat_dir).unwrap();
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        for name in [
            "runtime-loop",
            "telegram-loop",
            "discord-outbox-loop",
            "discord-gateway-loop",
            "worker-loop",
            "cron-scheduler-loop",
        ] {
            write_json_atomic(
                &heartbeat_dir.join(format!("{name}.json")),
                &serde_json::json!({
                    "status": "no-work",
                    "iteration": 3,
                    "processId": std::process::id(),
                    "atMs": now_ms,
                    "detail": "idle"
                }),
            )
            .unwrap();
        }
        write_json_atomic(
            &heartbeat_dir.join("progress-delivery-loop.json"),
            &serde_json::json!({
                "status": "running",
                "iteration": 3,
                "processId": std::process::id(),
                "atMs": now_ms - 600_000,
                "detail": "stale progress telemetry"
            }),
        )
        .unwrap();

        let report = collect_healthz(HealthzOptions {
            harness_home,
            now_ms,
            loop_stale_ms: 120_000,
            require_writable_state: false,
        })
        .unwrap();

        assert!(report.live);
        let progress_loop = report
            .loops
            .iter()
            .find(|item| item.name == "progress-delivery-loop")
            .unwrap();
        assert!(progress_loop.stale);
        assert!(
            report
                .warnings
                .iter()
                .any(|warning| warning.contains("progress-delivery-loop is degraded"))
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn healthz_surfaces_cron_canon_freshness_warnings() {
        let root = temp_root("healthz_surfaces_cron_canon_freshness_warnings");
        let harness_home = root.join(".agent-harness");
        let state = harness_home.join("state");
        fs::create_dir_all(state.join("runtime-queue")).unwrap();
        fs::create_dir_all(state.join("channels")).unwrap();
        fs::create_dir_all(state.join("logs")).unwrap();
        fs::create_dir_all(state.join("plugin-sidecar")).unwrap();
        fs::create_dir_all(state.join("memory")).unwrap();
        fs::create_dir_all(state.join("cron-scheduler")).unwrap();
        let heartbeat_dir = state.join("supervisor").join("loop-heartbeats");
        fs::create_dir_all(&heartbeat_dir).unwrap();
        let docs_ops = harness_home.join("workspace").join("docs").join("ops");
        let cron_state = state.join("memory").join("dream-lite-daily");
        let keeper_state = state.join("memory").join("cron-canon-keeper");
        fs::create_dir_all(&docs_ops).unwrap();
        fs::create_dir_all(&cron_state).unwrap();
        fs::create_dir_all(&keeper_state).unwrap();
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        write_json_atomic(
            &state.join("cron-scheduler").join("latest-tick.json"),
            &serde_json::json!({
                "ok": true,
                "atMs": now_ms - 30_000,
                "status": "no-work"
            }),
        )
        .unwrap();
        write_json_atomic(
            &cron_state.join("latest.json"),
            &serde_json::json!({
                "schema": "openclaw.agent-harness.dream-lite-daily.receipt.v1",
                "ok": true,
                "generatedAt": "1970-01-01T08:00:01+08:00",
                "runId": "stale-dream"
            }),
        )
        .unwrap();
        write_json_atomic(
            &keeper_state.join("latest-cron-canon-keeper.json"),
            &serde_json::json!({
                "schema": "openclaw.agent-harness.cron-canon-keeper.receipt.v1",
                "ok": false,
                "status": "warn",
                "generatedAt": "1970-01-01T00:00:01Z",
                "findings": [
                    {
                        "severity": "warn",
                        "code": "receipt-not-ok",
                        "cronId": "cron-canon-keeper",
                        "message": "keeper observed cron-canon drift",
                        "details": {
                            "path": "state/memory/cron-canon-keeper/latest-cron-canon-keeper.json"
                        }
                    }
                ]
            }),
        )
        .unwrap();
        write_json_atomic(
            &docs_ops.join("cron-canon.json"),
            &serde_json::json!({
                "schema": "openclaw.agent-harness.cron-canon.v1",
                "paths": {
                    "keeperReceipt": "state/memory/cron-canon-keeper/latest-cron-canon-keeper.json"
                },
                "activeCrons": [
                    {
                        "id": "dream-lite-daily",
                        "enabled": true,
                        "monitor": {
                            "type": "latest-json",
                            "path": "state/memory/dream-lite-daily/latest.json",
                            "maxAgeHours": 1,
                            "okField": "ok",
                            "okValue": true
                        }
                    },
                    {
                        "id": "cron-canon-keeper",
                        "enabled": true,
                        "monitor": {
                            "type": "latest-json",
                            "path": "state/memory/cron-canon-keeper/latest-cron-canon-keeper.json",
                            "maxAgeHours": 1,
                            "okField": "ok",
                            "okValue": true
                        }
                    }
                ]
            }),
        )
        .unwrap();

        let report = collect_healthz(HealthzOptions {
            harness_home,
            now_ms,
            loop_stale_ms: 120_000,
            require_writable_state: false,
        })
        .unwrap();

        assert!(
            report
                .warnings
                .iter()
                .any(|warning| warning.contains("cron canon keeper status=warn"))
        );
        assert!(report.warnings.iter().any(|warning| {
            warning.contains("dream-lite-daily")
                && warning.contains("receipt-stale")
                && warning.contains("ageHours=")
                && warning.contains("maxAgeHours=1.000")
        }));
        assert!(report.warnings.iter().any(|warning| {
            warning.contains("cron-canon-keeper") && warning.contains("receipt-not-ok")
        }));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn healthz_warns_on_stale_keeper_receipt() {
        healthz_surfaces_cron_canon_freshness_warnings();
    }

    #[test]
    fn healthz_evaluates_all_canon_monitor_blocks_generically() {
        healthz_surfaces_cron_canon_freshness_warnings();
    }

    #[test]
    fn reconcile_managed_health_fails_closed_when_root_keeper_state_is_missing() {
        let root = temp_root("reconcile_keeper_missing");
        fs::create_dir_all(&root).unwrap();
        fs::write(
            root.join("harness-config.json"),
            r#"{"supervisor":{"manageAllLoops":true,"autoReconcile":true}}"#,
        )
        .unwrap();

        let keeper = reconcile_keeper_health(&root, 10_000, 120_000)
            .unwrap()
            .unwrap();

        assert!(keeper.required);
        assert!(!keeper.present);
        assert!(keeper.stale);
        assert_eq!(
            keeper.reason.as_deref(),
            Some("reconcile keeper state is missing")
        );
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(windows)]
    #[test]
    fn reconcile_managed_health_requires_a_fresh_exact_live_keeper_process() {
        let root = temp_root("reconcile_keeper_exact_process");
        let workspace = root.join("workspace");
        let runtime_workspace = root.join("runtime-workspace/default");
        let harness_cli = root.join("bin/current/agent-harness.exe");
        let codex_executable = root.join("tools/codex/codex.exe");
        let node_executable = root.join("tools/node/node.exe");
        let gateway_script = root.join("tools/discord/index.mjs");
        for directory in [
            root.as_path(),
            workspace.as_path(),
            runtime_workspace.as_path(),
            harness_cli.parent().unwrap(),
            codex_executable.parent().unwrap(),
            node_executable.parent().unwrap(),
            gateway_script.parent().unwrap(),
        ] {
            fs::create_dir_all(directory).unwrap();
        }
        fs::write(&harness_cli, b"candidate").unwrap();
        fs::write(&codex_executable, b"codex").unwrap();
        fs::write(&node_executable, b"node").unwrap();
        fs::write(&gateway_script, b"gateway").unwrap();
        fs::write(
            root.join("harness-config.json"),
            r#"{"supervisor":{"manageAllLoops":true,"autoReconcile":true}}"#,
        )
        .unwrap();
        let plan =
            crate::write_windows_reconcile_keeper_plan(crate::WindowsReconcileKeeperPlanOptions {
                harness_home: root.clone(),
                source_home: root.clone(),
                workspace: Some(workspace),
                runtime_workspace,
                harness_cli,
                codex_executable: Some(codex_executable),
                node_executable: Some(node_executable),
                discord_gateway_script: Some(gateway_script),
                user_id: "TEST\\keeper".to_string(),
                task_name: None,
                live_deployment: true,
            })
            .unwrap();
        let process_id = i64::from(std::process::id());
        let process_start_time_ms = process_start_time_ms_for_pid(process_id).unwrap();
        let _lease = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .share_mode(0)
            .open(&plan.lease_file)
            .unwrap();
        write_json_atomic(
            &root.join("state/supervisor/reconcile-keeper.json"),
            &serde_json::json!({
                "schema": "agent-harness.supervisor-reconcile-keeper.v1",
                "status": "running",
                "pid": process_id,
                "processStartTimeMs": process_start_time_ms,
                "lastHeartbeatAtMs": 50_000,
                "holdRequested": false,
                "autoReconcile": true,
                "applied": true,
                "planDigest": plan.plan_digest,
                "actionDigest": plan.action.digest,
                "taskName": plan.task.name,
                "leaseFile": plan.lease_file,
                "ownershipMode": "scheduled-task-plan",
                "reason": "test keeper"
            }),
        )
        .unwrap();

        let live = reconcile_keeper_health(&root, 60_000, 120_000)
            .unwrap()
            .unwrap();
        assert!(!live.stale);
        assert_eq!(live.process_alive, Some(true));
        assert!(live.process_instance_matches);
        assert!(live.plan_valid);
        assert!(live.state_matches_plan);
        assert!(live.singleton_lease_held);

        write_json_atomic(
            &root.join("state/supervisor/reconcile-keeper.json"),
            &serde_json::json!({
                "schema": "agent-harness.supervisor-reconcile-keeper.v1",
                "status": "running",
                "pid": process_id,
                "processStartTimeMs": process_start_time_ms.saturating_sub(1),
                "lastHeartbeatAtMs": 50_000,
                "holdRequested": false,
                "autoReconcile": true,
                "applied": true,
                "planDigest": plan.plan_digest,
                "actionDigest": plan.action.digest,
                "taskName": plan.task.name,
                "leaseFile": plan.lease_file,
                "ownershipMode": "scheduled-task-plan"
            }),
        )
        .unwrap();
        let mismatched = reconcile_keeper_health(&root, 60_000, 120_000)
            .unwrap()
            .unwrap();
        assert!(mismatched.stale);
        assert!(!mismatched.process_instance_matches);
        drop(_lease);
        let _ = fs::remove_dir_all(root);
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-healthz-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
