use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

const SUPERVISION_EVALUATION_SCHEMA: &str = "agent-harness.supervision-evaluation.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupervisionEvaluateOptions {
    pub harness_home: PathBuf,
    pub run_id: String,
    pub now_ms: i64,
    pub specs: Vec<SupervisorChildSpec>,
    pub states: Vec<SupervisorChildState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorChildSpec {
    pub name: String,
    pub component: String,
    pub command: Vec<String>,
    pub heartbeat_ttl_ms: i64,
    pub restart: SupervisorRestartPolicy,
    pub alert_channel: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorRestartPolicy {
    pub enabled: bool,
    pub base_backoff_ms: i64,
    pub max_backoff_ms: i64,
    pub crash_loop_window_ms: i64,
    pub crash_loop_threshold: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorChildState {
    pub name: String,
    pub process_id: Option<u32>,
    pub started_at_ms: Option<i64>,
    pub last_heartbeat_ms: Option<i64>,
    pub last_exit_ms: Option<i64>,
    pub last_exit_code: Option<i32>,
    pub restart_count: usize,
    pub recent_restart_ms: Vec<i64>,
    pub stop_intent: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisionEvaluationReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub run_id: String,
    pub receipt_file: PathBuf,
    pub summary: SupervisionSummary,
    pub children: Vec<SupervisorChildEvaluation>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisionSummary {
    pub healthy: usize,
    pub stale: usize,
    pub stopped: usize,
    pub restart_scheduled: usize,
    pub breaker_tripped: usize,
    pub stop_intent: usize,
    pub alerts: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorChildEvaluation {
    pub name: String,
    pub component: String,
    pub status: SupervisorChildStatus,
    pub process_id: Option<u32>,
    pub heartbeat_age_ms: Option<i64>,
    pub restart_count: usize,
    pub next_restart_after_ms: Option<i64>,
    pub alert: Option<SupervisorAlert>,
    pub detail: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum SupervisorChildStatus {
    Healthy,
    Stale,
    Stopped,
    RestartScheduled,
    BreakerTripped,
    StopIntent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorAlert {
    pub severity: String,
    pub message: String,
    pub channel: Option<String>,
}

impl Default for SupervisorRestartPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            base_backoff_ms: 1_000,
            max_backoff_ms: 60_000,
            crash_loop_window_ms: 300_000,
            crash_loop_threshold: 5,
        }
    }
}

pub fn default_supervisor_child_specs() -> Vec<SupervisorChildSpec> {
    [
        "runtime-loop",
        "worker-loop",
        "progress-delivery-loop",
        "telegram-loop",
        "discord-outbox-loop",
        "discord-gateway-loop",
    ]
    .into_iter()
    .map(|name| SupervisorChildSpec {
        name: name.to_string(),
        component: name.to_string(),
        command: vec![name.to_string()],
        heartbeat_ttl_ms: 120_000,
        restart: SupervisorRestartPolicy::default(),
        alert_channel: None,
    })
    .collect()
}

pub fn evaluate_supervisor_children(
    options: SupervisionEvaluateOptions,
) -> io::Result<SupervisionEvaluationReport> {
    let receipt_file = options
        .harness_home
        .join("state")
        .join("supervisor")
        .join("supervision-receipts.jsonl");
    let mut children = Vec::with_capacity(options.specs.len());
    for spec in &options.specs {
        let state = options.states.iter().find(|state| state.name == spec.name);
        children.push(evaluate_child(spec, state, options.now_ms));
    }
    let summary = summarize(&children);
    let report = SupervisionEvaluationReport {
        schema: SUPERVISION_EVALUATION_SCHEMA,
        harness_home: options.harness_home,
        run_id: options.run_id,
        receipt_file: receipt_file.clone(),
        summary,
        children,
    };
    append_json_line(&receipt_file, &report)?;
    Ok(report)
}

fn evaluate_child(
    spec: &SupervisorChildSpec,
    state: Option<&SupervisorChildState>,
    now_ms: i64,
) -> SupervisorChildEvaluation {
    let Some(state) = state else {
        return restart_or_stopped(spec, None, 0, "child has no persisted state".to_string());
    };
    if state.stop_intent {
        return SupervisorChildEvaluation {
            name: spec.name.clone(),
            component: spec.component.clone(),
            status: SupervisorChildStatus::StopIntent,
            process_id: state.process_id,
            heartbeat_age_ms: heartbeat_age(state, now_ms),
            restart_count: state.restart_count,
            next_restart_after_ms: None,
            alert: None,
            detail: "operator stop intent is active".to_string(),
        };
    }
    if crash_loop_tripped(spec, state, now_ms) {
        return SupervisorChildEvaluation {
            name: spec.name.clone(),
            component: spec.component.clone(),
            status: SupervisorChildStatus::BreakerTripped,
            process_id: state.process_id,
            heartbeat_age_ms: heartbeat_age(state, now_ms),
            restart_count: state.restart_count,
            next_restart_after_ms: None,
            alert: Some(SupervisorAlert {
                severity: "error".to_string(),
                message: format!(
                    "{} tripped crash-loop breaker after {} restart(s)",
                    spec.name, state.restart_count
                ),
                channel: spec.alert_channel.clone(),
            }),
            detail: "restart threshold exceeded inside crash-loop window".to_string(),
        };
    }
    if state.process_id.is_none() {
        return restart_or_stopped(
            spec,
            Some(state),
            state.restart_count,
            "child process is not running".to_string(),
        );
    }
    let age = heartbeat_age(state, now_ms);
    if age.is_some_and(|age| age > spec.heartbeat_ttl_ms) {
        return SupervisorChildEvaluation {
            name: spec.name.clone(),
            component: spec.component.clone(),
            status: SupervisorChildStatus::Stale,
            process_id: state.process_id,
            heartbeat_age_ms: age,
            restart_count: state.restart_count,
            next_restart_after_ms: restart_backoff_ms(spec, state.restart_count),
            alert: Some(SupervisorAlert {
                severity: "warn".to_string(),
                message: format!("{} heartbeat is stale", spec.name),
                channel: spec.alert_channel.clone(),
            }),
            detail: format!(
                "heartbeat age {}ms exceeds ttl {}ms",
                age.unwrap_or_default(),
                spec.heartbeat_ttl_ms
            ),
        };
    }
    SupervisorChildEvaluation {
        name: spec.name.clone(),
        component: spec.component.clone(),
        status: SupervisorChildStatus::Healthy,
        process_id: state.process_id,
        heartbeat_age_ms: age,
        restart_count: state.restart_count,
        next_restart_after_ms: None,
        alert: None,
        detail: "heartbeat is fresh".to_string(),
    }
}

fn restart_or_stopped(
    spec: &SupervisorChildSpec,
    state: Option<&SupervisorChildState>,
    restart_count: usize,
    detail: String,
) -> SupervisorChildEvaluation {
    let restart_after = state.and_then(|_| restart_backoff_ms(spec, restart_count));
    let status = if spec.restart.enabled {
        SupervisorChildStatus::RestartScheduled
    } else {
        SupervisorChildStatus::Stopped
    };
    SupervisorChildEvaluation {
        name: spec.name.clone(),
        component: spec.component.clone(),
        status,
        process_id: state.and_then(|state| state.process_id),
        heartbeat_age_ms: None,
        restart_count,
        next_restart_after_ms: restart_after,
        alert: (status == SupervisorChildStatus::Stopped).then(|| SupervisorAlert {
            severity: "warn".to_string(),
            message: format!("{} is stopped and restart is disabled", spec.name),
            channel: spec.alert_channel.clone(),
        }),
        detail,
    }
}

fn restart_backoff_ms(spec: &SupervisorChildSpec, restart_count: usize) -> Option<i64> {
    if !spec.restart.enabled {
        return None;
    }
    let shift = restart_count.min(20) as u32;
    let backoff = spec.restart.base_backoff_ms.saturating_mul(1_i64 << shift);
    Some(backoff.min(spec.restart.max_backoff_ms).max(0))
}

fn crash_loop_tripped(
    spec: &SupervisorChildSpec,
    state: &SupervisorChildState,
    now_ms: i64,
) -> bool {
    if !spec.restart.enabled {
        return false;
    }
    let window_start = now_ms.saturating_sub(spec.restart.crash_loop_window_ms.max(0));
    let recent = state
        .recent_restart_ms
        .iter()
        .filter(|at_ms| **at_ms >= window_start && **at_ms <= now_ms)
        .count();
    recent >= spec.restart.crash_loop_threshold
}

fn heartbeat_age(state: &SupervisorChildState, now_ms: i64) -> Option<i64> {
    state
        .last_heartbeat_ms
        .map(|at_ms| now_ms.saturating_sub(at_ms))
}

fn summarize(children: &[SupervisorChildEvaluation]) -> SupervisionSummary {
    let mut summary = SupervisionSummary::default();
    for child in children {
        match child.status {
            SupervisorChildStatus::Healthy => summary.healthy += 1,
            SupervisorChildStatus::Stale => summary.stale += 1,
            SupervisorChildStatus::Stopped => summary.stopped += 1,
            SupervisorChildStatus::RestartScheduled => summary.restart_scheduled += 1,
            SupervisorChildStatus::BreakerTripped => summary.breaker_tripped += 1,
            SupervisorChildStatus::StopIntent => summary.stop_intent += 1,
        }
        if child.alert.is_some() {
            summary.alerts += 1;
        }
    }
    summary
}

fn append_json_line(path: &Path, value: &impl Serialize) -> io::Result<()> {
    crate::append_jsonl_value(path, value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn evaluates_stale_child_and_crash_loop_breaker() {
        let root = temp_root("evaluates_stale_child_and_crash_loop_breaker");
        let harness_home = root.join(".agent-harness");
        let specs = vec![
            SupervisorChildSpec {
                name: "runtime-loop".to_string(),
                component: "runtime-loop".to_string(),
                command: vec!["agent-harness".to_string(), "runtime-loop".to_string()],
                heartbeat_ttl_ms: 1_000,
                restart: SupervisorRestartPolicy {
                    base_backoff_ms: 500,
                    max_backoff_ms: 5_000,
                    crash_loop_threshold: 3,
                    ..SupervisorRestartPolicy::default()
                },
                alert_channel: Some("operator".to_string()),
            },
            SupervisorChildSpec {
                name: "worker-loop".to_string(),
                component: "worker-loop".to_string(),
                command: vec!["agent-harness".to_string(), "worker-loop".to_string()],
                heartbeat_ttl_ms: 1_000,
                restart: SupervisorRestartPolicy {
                    crash_loop_threshold: 3,
                    crash_loop_window_ms: 10_000,
                    ..SupervisorRestartPolicy::default()
                },
                alert_channel: Some("operator".to_string()),
            },
        ];
        let states = vec![
            SupervisorChildState {
                name: "runtime-loop".to_string(),
                process_id: Some(42),
                started_at_ms: Some(1_000),
                last_heartbeat_ms: Some(2_000),
                last_exit_ms: None,
                last_exit_code: None,
                restart_count: 2,
                recent_restart_ms: vec![1_000, 1_500],
                stop_intent: false,
            },
            SupervisorChildState {
                name: "worker-loop".to_string(),
                process_id: None,
                started_at_ms: Some(1_000),
                last_heartbeat_ms: Some(2_900),
                last_exit_ms: Some(3_000),
                last_exit_code: Some(1),
                restart_count: 3,
                recent_restart_ms: vec![1_000, 2_000, 2_900],
                stop_intent: false,
            },
        ];

        let report = evaluate_supervisor_children(SupervisionEvaluateOptions {
            harness_home: harness_home.clone(),
            run_id: "run-1".to_string(),
            now_ms: 4_000,
            specs,
            states,
        })
        .unwrap();

        assert_eq!(report.summary.stale, 1);
        assert_eq!(report.summary.breaker_tripped, 1);
        assert_eq!(report.summary.alerts, 2);
        assert!(report.receipt_file.is_file());
        let receipt_text = fs::read_to_string(report.receipt_file).unwrap();
        assert!(receipt_text.contains("\"status\":\"stale\""));
        assert!(receipt_text.contains("\"status\":\"breaker-tripped\""));

        let _ = fs::remove_dir_all(root);
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-supervision-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
