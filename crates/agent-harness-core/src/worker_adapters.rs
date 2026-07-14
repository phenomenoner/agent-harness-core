use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::json;

use crate::backend_reasoning::{
    BackendReasoningPolicyV1, BackendReasoningSource, ReasoningPreference,
};
use crate::child_execution_policy::{
    ChildExecutionPolicyV1, ChildExecutionPolicyV1Input, ChildExecutionPolicyV2,
};
use crate::lane::FullLaneKeyV1;
use crate::model_catalog::{
    ModelCatalogRolloutMode, ReasoningResolutionStatus, UnsupportedReasoningPolicy,
    model_catalog_rollout_mode_for_agent, parse_codex_model_catalog, resolve_reasoning_effort,
};
use crate::worker_coordination::{
    WorkerCoordinatorWaitCreateOptionsV1, persist_waiting_for_children_in_transaction,
};
use crate::worker_result_mailbox::{ExactWorkerResultOwnerV1, WorkerResultOwnerV1};
use crate::workers::{
    WorkerEnqueueOptionsV4, WorkerEnqueueTransactionReport, enqueue_worker_job_v4,
    enqueue_worker_job_v4_in_transaction,
};
use crate::{
    AgentSource, DeterministicCronPlanAction, DeterministicCronPlanInput, NativeCronPlanAction,
    NativeCronPlanInput, SubagentPlanAction, SubagentPlanEntry, SubagentPlanInput,
    WorkerEnqueueOptions, WorkerEnqueueOptionsV2, WorkerEnqueueOptionsV3, WorkerEnqueueReport,
    WorkerJobKind, current_log_time_ms, enqueue_worker_job, enqueue_worker_job_v3,
    load_agent_registry, load_deterministic_cron_store, load_native_cron_store,
    load_subagent_ledger, plan_deterministic_cron, plan_native_cron, plan_subagents,
};

const WORKER_ADAPTER_ENQUEUE_SCHEMA: &str = "agent-harness.worker-adapter-enqueue.v1";
const DEFAULT_WORKER_TIMEOUT_MS: u64 = 300_000;
const DEFAULT_WATCHDOG_TIMEOUT_MS: u64 = 900_000;
const CONTROLLED_COORDINATOR_SMOKE_CHILD_COUNT: usize = 2;
const CONTROLLED_COORDINATOR_SMOKE_PROVIDER: &str = "openai";
const CONTROLLED_COORDINATOR_SMOKE_SOURCE_MARKER: &str = "t4-coordinator-smoke.json";
const CONTROLLED_COORDINATOR_SMOKE_SOURCE_SCHEMA: &str =
    "agent-harness.controlled-coordinator-smoke-source.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeterministicCronWorkerEnqueueOptions {
    pub harness_home: PathBuf,
    pub source: AgentSource,
    pub allow_deterministic_run: bool,
    pub dry_run_shell: bool,
    pub master_agent_id: Option<String>,
    pub master_session_key: Option<String>,
    pub runtime_workspace: Option<PathBuf>,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeCronWorkerEnqueueOptions {
    pub harness_home: PathBuf,
    pub source: AgentSource,
    pub resume_cron: bool,
    pub include_registered_cron: bool,
    pub master_agent_id: Option<String>,
    pub master_session_key: Option<String>,
    pub runtime_workspace: Option<PathBuf>,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubagentWorkerEnqueueOptions {
    pub harness_home: PathBuf,
    pub source: AgentSource,
    pub resume_subagents: bool,
    pub master_agent_id: Option<String>,
    pub master_session_key: Option<String>,
    pub runtime_workspace: Option<PathBuf>,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubagentWorkerEnqueueOptionsV2 {
    pub options: SubagentWorkerEnqueueOptions,
    /// Immutable child execution policies keyed by the imported subagent run id.
    pub child_policies_by_run_id: BTreeMap<String, ChildExecutionPolicyV1>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubagentWorkerEnqueueOptionsV3 {
    pub options: SubagentWorkerEnqueueOptionsV2,
    /// Exact result owners keyed by the imported subagent run id.
    pub result_owners_by_run_id: BTreeMap<String, WorkerResultOwnerV1>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubagentCoordinatorResumeOptionsV1 {
    pub wait_id: String,
    pub owner: ExactWorkerResultOwnerV1,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubagentWorkerEnqueueOptionsV4 {
    pub options: SubagentWorkerEnqueueOptionsV3,
    pub coordinator: Option<SubagentCoordinatorResumeOptionsV1>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubagentWorkerEnqueueOptionsV5 {
    pub options: SubagentWorkerEnqueueOptionsV4,
    /// Independent V2 policy/snapshot pairs keyed by imported subagent run id.
    pub child_policies_v2_by_run_id: BTreeMap<String, ChildExecutionPolicyV2>,
}

/// Exact live lane used by the intentionally narrow coordinator replay probe.
/// The adapter only permits the main interactive Telegram or Discord lane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlledCoordinatorSmokeLaneV1 {
    pub platform: String,
    pub account_id: String,
    pub channel_id: String,
    pub user_id: String,
    pub agent_id: String,
    pub session_key: String,
}

/// One catalog-admitted child in the intentionally fixed two-child replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlledCoordinatorSmokeChildV1 {
    pub run_id: String,
    pub model: String,
    pub effort: String,
}

/// Operator-only admission for a bounded, durable child-to-master replay.
///
/// This does not invoke a provider itself. It validates the exact source ledger
/// and authoritative catalog, then delegates atomically to the existing V5
/// worker adapter so the normal worker, mailbox, watchdog, and runtime paths
/// remain the only execution path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlledCoordinatorSmokeOptionsV1 {
    pub harness_home: PathBuf,
    pub source: AgentSource,
    pub runtime_workspace: Option<PathBuf>,
    pub lane: ControlledCoordinatorSmokeLaneV1,
    pub children: Vec<ControlledCoordinatorSmokeChildV1>,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerAdapterEnqueueReport {
    pub schema: &'static str,
    pub adapter: String,
    pub harness_home: PathBuf,
    pub source_home: PathBuf,
    pub source_workspace: PathBuf,
    pub job_group_id: String,
    pub summary: WorkerAdapterEnqueueSummary,
    pub jobs: Vec<WorkerAdapterJobRef>,
    pub watchdog: Option<WorkerAdapterJobRef>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerAdapterEnqueueSummary {
    pub plan_entries: usize,
    pub candidate_jobs: usize,
    pub inserted_jobs: usize,
    pub existing_jobs: usize,
    pub watchdog_inserted: bool,
    pub watchdog_existing: bool,
    pub skipped_entries: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerAdapterJobRef {
    pub job_id: String,
    pub kind: String,
    pub lane: String,
    pub inserted: bool,
    pub idempotency_key: String,
    pub source: Option<String>,
    pub action: String,
}

pub fn enqueue_deterministic_cron_workers(
    options: DeterministicCronWorkerEnqueueOptions,
) -> io::Result<WorkerAdapterEnqueueReport> {
    let store = load_deterministic_cron_store(&options.source)?;
    let plan = plan_deterministic_cron(
        &store,
        DeterministicCronPlanInput {
            allow_deterministic_run: options.allow_deterministic_run,
        },
    );
    let group_id = stable_group_id("deterministic-cron", &options.source);
    let master_agent = options
        .master_agent_id
        .clone()
        .unwrap_or_else(|| "main".to_string());
    let master_session = options
        .master_session_key
        .clone()
        .unwrap_or_else(|| format!("worker-group:{group_id}"));
    let concurrency_group = master_concurrency_group(&master_agent, &master_session);
    let mut warnings = plan.warnings.clone();
    let mut jobs = Vec::new();
    let mut summary = WorkerAdapterEnqueueSummary {
        plan_entries: plan.entries.len(),
        ..WorkerAdapterEnqueueSummary::default()
    };

    for entry in &plan.entries {
        if entry.action != DeterministicCronPlanAction::ReadyCommand {
            summary.skipped_entries += 1;
            continue;
        }
        let Some(script_path) = entry.script_path.clone() else {
            summary.skipped_entries += 1;
            warnings.push(format!(
                "deterministic cron entry {} has no script path",
                entry.entry_id
            ));
            continue;
        };
        let cwd = script_path.parent().unwrap_or(Path::new(".")).to_path_buf();
        let payload = json!({
            "adapter": "deterministic-cron",
            "entryId": entry.entry_id,
            "runnerKind": entry.runner_kind,
            "command": entry.command,
            "scriptPath": script_path,
            "cwd": cwd,
            "argv": [],
            "dryRun": options.dry_run_shell,
            "sourceHome": &options.source.home,
            "sourceWorkspace": &options.source.workspace,
            "runtimeWorkspace": &options.runtime_workspace,
        });
        let report = enqueue_worker_job(WorkerEnqueueOptions {
            harness_home: options.harness_home.clone(),
            kind: WorkerJobKind::DeterministicShell,
            lane: Some("shell".to_string()),
            payload,
            idempotency_key: Some(format!(
                "deterministic-cron:{}:{}",
                entry.entry_id, options.dry_run_shell
            )),
            parent_job_id: None,
            job_group_id: Some(group_id.clone()),
            master_agent_id: Some(master_agent.clone()),
            master_session_key: Some(master_session.clone()),
            wake_policy: None,
            source: Some("deterministic-cron-adapter".to_string()),
            priority: 0,
            available_at_ms: Some(options.now_ms),
            max_attempts: 3,
            timeout_ms: Some(DEFAULT_WORKER_TIMEOUT_MS),
            cascade_timeout_ms: Some(DEFAULT_WATCHDOG_TIMEOUT_MS),
            rate_key: None,
            concurrency_group_key: Some(concurrency_group.clone()),
            now_ms: options.now_ms,
        })?;
        push_job_ref(&mut summary, &mut jobs, report, "ready-command");
    }

    let watchdog = enqueue_watchdog_if_needed(
        &options.harness_home,
        &options.source,
        options.runtime_workspace.as_ref(),
        &group_id,
        &master_agent,
        &master_session,
        "deterministic-cron-adapter",
        jobs.is_empty(),
        options.now_ms,
        None,
    )?;
    set_watchdog_summary(&mut summary, watchdog.as_ref());

    Ok(WorkerAdapterEnqueueReport {
        schema: WORKER_ADAPTER_ENQUEUE_SCHEMA,
        adapter: "deterministic-cron".to_string(),
        harness_home: options.harness_home,
        source_home: options.source.home,
        source_workspace: options.source.workspace,
        job_group_id: group_id,
        summary,
        jobs,
        watchdog,
        warnings,
    })
}

pub fn enqueue_native_cron_workers(
    options: NativeCronWorkerEnqueueOptions,
) -> io::Result<WorkerAdapterEnqueueReport> {
    let store = load_native_cron_store(&options.source)?;
    let registry = load_agent_registry(&options.source)?;
    let plan = plan_native_cron(
        &store,
        &registry,
        NativeCronPlanInput {
            now_ms: options.now_ms,
            resume_enabled: options.resume_cron,
        },
    );
    let group_id = stable_group_id("native-cron", &options.source);
    let master_agent = options
        .master_agent_id
        .clone()
        .unwrap_or_else(|| "main".to_string());
    let master_session = options
        .master_session_key
        .clone()
        .unwrap_or_else(|| format!("worker-group:{group_id}"));
    let concurrency_group = master_concurrency_group(&master_agent, &master_session);
    let mut warnings = plan.warnings.clone();
    let mut jobs = Vec::new();
    let mut summary = WorkerAdapterEnqueueSummary {
        plan_entries: plan.entries.len(),
        ..WorkerAdapterEnqueueSummary::default()
    };

    for entry in &plan.entries {
        let action_label = match entry.action {
            NativeCronPlanAction::EnqueueAgentTurn => "enqueue-agent-turn",
            NativeCronPlanAction::CronRegistered if options.include_registered_cron => {
                "cron-registered"
            }
            _ => {
                summary.skipped_entries += 1;
                continue;
            }
        };
        let Some(agent_id) = entry.agent_id.clone() else {
            summary.skipped_entries += 1;
            warnings.push(format!("native cron job {} has no agent id", entry.job_id));
            continue;
        };
        let message_text = entry
            .message_text
            .clone()
            .unwrap_or_else(|| format!("Run native cron job {}", entry.job_id));
        let session_key = entry.session_key.clone().unwrap_or_else(|| {
            format!(
                "cron:{}:{}",
                normalize_key_part(&entry.job_id),
                normalize_key_part(&agent_id)
            )
        });
        let payload = json!({
            "adapter": "native-cron",
            "jobId": entry.job_id,
            "action": entry.action,
            "sourceHome": &options.source.home,
            "sourceWorkspace": &options.source.workspace,
            "runtimeWorkspace": &options.runtime_workspace,
            "agentId": &agent_id,
            "sessionKey": session_key,
            "platform": "native-cron",
            "channelId": entry.job_id,
            "userId": "native-cron-adapter",
            "messageText": message_text,
            "inboundContext": serde_json::to_string(entry).unwrap_or_default()
        });
        let report = enqueue_worker_job(WorkerEnqueueOptions {
            harness_home: options.harness_home.clone(),
            kind: WorkerJobKind::LlmSubagent,
            lane: Some("llm".to_string()),
            payload,
            idempotency_key: Some(format!(
                "native-cron:{}:{}:{}",
                entry.job_id, action_label, options.now_ms
            )),
            parent_job_id: None,
            job_group_id: Some(group_id.clone()),
            master_agent_id: Some(master_agent.clone()),
            master_session_key: Some(master_session.clone()),
            wake_policy: None,
            source: Some("native-cron-adapter".to_string()),
            priority: 0,
            available_at_ms: Some(options.now_ms),
            max_attempts: 3,
            timeout_ms: Some(DEFAULT_WORKER_TIMEOUT_MS),
            cascade_timeout_ms: Some(DEFAULT_WATCHDOG_TIMEOUT_MS),
            rate_key: Some(format!("llm:{agent_id}")),
            concurrency_group_key: Some(concurrency_group.clone()),
            now_ms: options.now_ms,
        })?;
        push_job_ref(&mut summary, &mut jobs, report, action_label);
    }

    let watchdog = enqueue_watchdog_if_needed(
        &options.harness_home,
        &options.source,
        options.runtime_workspace.as_ref(),
        &group_id,
        &master_agent,
        &master_session,
        "native-cron-adapter",
        jobs.is_empty(),
        options.now_ms,
        None,
    )?;
    set_watchdog_summary(&mut summary, watchdog.as_ref());

    Ok(WorkerAdapterEnqueueReport {
        schema: WORKER_ADAPTER_ENQUEUE_SCHEMA,
        adapter: "native-cron".to_string(),
        harness_home: options.harness_home,
        source_home: options.source.home,
        source_workspace: options.source.workspace,
        job_group_id: group_id,
        summary,
        jobs,
        watchdog,
        warnings,
    })
}

pub fn enqueue_subagent_workers(
    options: SubagentWorkerEnqueueOptions,
) -> io::Result<WorkerAdapterEnqueueReport> {
    enqueue_subagent_workers_v2(SubagentWorkerEnqueueOptionsV2 {
        options,
        child_policies_by_run_id: BTreeMap::new(),
    })
}

pub fn enqueue_subagent_workers_v2(
    options: SubagentWorkerEnqueueOptionsV2,
) -> io::Result<WorkerAdapterEnqueueReport> {
    enqueue_subagent_workers_v3(SubagentWorkerEnqueueOptionsV3 {
        options,
        result_owners_by_run_id: BTreeMap::new(),
    })
}

pub fn enqueue_subagent_workers_v3(
    options: SubagentWorkerEnqueueOptionsV3,
) -> io::Result<WorkerAdapterEnqueueReport> {
    enqueue_subagent_workers_v4(SubagentWorkerEnqueueOptionsV4 {
        options,
        coordinator: None,
    })
}

pub fn enqueue_subagent_workers_v5(
    options: SubagentWorkerEnqueueOptionsV5,
) -> io::Result<WorkerAdapterEnqueueReport> {
    let SubagentWorkerEnqueueOptionsV5 {
        options,
        child_policies_v2_by_run_id,
    } = options;
    let SubagentWorkerEnqueueOptionsV4 {
        options,
        coordinator,
    } = options;
    let SubagentWorkerEnqueueOptionsV3 {
        options,
        result_owners_by_run_id,
    } = options;
    let SubagentWorkerEnqueueOptionsV2 {
        options,
        child_policies_by_run_id,
    } = options;

    if let Some(run_id) = child_policies_by_run_id
        .keys()
        .find(|run_id| child_policies_v2_by_run_id.contains_key(*run_id))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("runId `{run_id}` appears in both V1 and V2 child policy maps"),
        ));
    }

    let ledger = load_subagent_ledger(&options.source)?;
    let plan = plan_subagents(
        &ledger,
        SubagentPlanInput {
            resume_subagents: options.resume_subagents,
        },
    );
    let group_id = stable_group_id("subagents", &options.source);
    let master_agent = options
        .master_agent_id
        .clone()
        .unwrap_or_else(|| "main".to_string());
    let master_session = options
        .master_session_key
        .clone()
        .unwrap_or_else(|| format!("worker-group:{group_id}"));
    let concurrency_group = master_concurrency_group(&master_agent, &master_session);
    let warnings = plan.warnings.clone();
    let mut jobs = Vec::new();
    let mut summary = WorkerAdapterEnqueueSummary {
        plan_entries: plan.entries.len(),
        ..WorkerAdapterEnqueueSummary::default()
    };

    validate_durable_coordinator_owners(
        coordinator.as_ref(),
        &plan.entries,
        &result_owners_by_run_id,
    )?;

    let db_file = crate::init_worker_store(&options.harness_home)?;
    let mut conn = rusqlite::Connection::open(&db_file).map_err(io::Error::other)?;
    let transaction = conn.transaction().map_err(io::Error::other)?;
    let mut finalizers = Vec::new();

    for entry in &plan.entries {
        if entry.action != SubagentPlanAction::ResumeCandidate {
            summary.skipped_entries += 1;
            continue;
        }
        let agent_id = entry
            .agent_id
            .clone()
            .unwrap_or_else(|| master_agent.clone());
        let message_text = entry
            .task
            .clone()
            .unwrap_or_else(|| format!("Resume imported subagent run {}", entry.run_id));
        let session_key = entry.session_key.clone().unwrap_or_else(|| {
            format!(
                "subagent:{}:{}",
                normalize_key_part(&entry.run_id),
                normalize_key_part(&agent_id)
            )
        });
        let payload = json!({
            "adapter": "subagent-ledger",
            "runId": entry.run_id,
            "sourceHome": &options.source.home,
            "sourceWorkspace": &options.source.workspace,
            "runtimeWorkspace": &options.runtime_workspace,
            "agentId": &agent_id,
            "sessionKey": session_key,
            "platform": "subagent-ledger",
            "channelId": entry.run_id,
            "userId": entry.parent_agent_id.clone().unwrap_or_else(|| master_agent.clone()),
            "messageText": message_text,
            "inboundContext": serde_json::to_string(entry).unwrap_or_default()
        });
        let v2_policy = child_policies_v2_by_run_id.get(entry.run_id.as_str());
        if let Some(policy) = v2_policy {
            policy.validate().map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "invalid V2 child policy for runId `{}`: {error}",
                        entry.run_id
                    ),
                )
            })?;
        }
        let child_policy = v2_policy
            .map(|policy| policy.child_policy().clone())
            .or_else(|| child_policies_by_run_id.get(entry.run_id.as_str()).cloned());
        let authorized_execution_mode = v2_policy
            .and_then(ChildExecutionPolicyV2::authorized_execution_mode)
            .cloned();
        let enqueue_options = WorkerEnqueueOptionsV4 {
            options: WorkerEnqueueOptionsV3 {
                options: WorkerEnqueueOptionsV2 {
                    options: WorkerEnqueueOptions {
                        harness_home: options.harness_home.clone(),
                        kind: WorkerJobKind::LlmSubagent,
                        lane: Some("llm".to_string()),
                        payload,
                        idempotency_key: Some(format!("subagent-resume:{}", entry.run_id)),
                        parent_job_id: None,
                        job_group_id: Some(group_id.clone()),
                        master_agent_id: Some(master_agent.clone()),
                        master_session_key: Some(master_session.clone()),
                        wake_policy: None,
                        source: Some("subagent-ledger-adapter".to_string()),
                        priority: 0,
                        available_at_ms: Some(options.now_ms),
                        max_attempts: 3,
                        timeout_ms: Some(DEFAULT_WORKER_TIMEOUT_MS),
                        cascade_timeout_ms: Some(DEFAULT_WATCHDOG_TIMEOUT_MS),
                        rate_key: Some(format!("llm:{agent_id}")),
                        concurrency_group_key: Some(concurrency_group.clone()),
                        now_ms: options.now_ms,
                    },
                    child_policy,
                },
                result_owner: result_owners_by_run_id.get(entry.run_id.as_str()).cloned(),
            },
            authorized_execution_mode,
        };
        let transaction_report =
            enqueue_worker_job_v4_in_transaction(&transaction, enqueue_options.clone())?;
        push_transaction_job_ref(
            &mut summary,
            &mut jobs,
            &transaction_report,
            "resume-candidate",
        );
        finalizers.push(enqueue_options);
    }

    if let Some(coordinator) = &coordinator
        && !jobs.is_empty()
    {
        persist_waiting_for_children_in_transaction(
            &transaction,
            &WorkerCoordinatorWaitCreateOptionsV1 {
                wait_id: coordinator.wait_id.clone(),
                owner: coordinator.owner.clone(),
                child_group_id: group_id.clone(),
                expected_child_job_ids: jobs.iter().map(|job| job.job_id.clone()).collect(),
                now_ms: options.now_ms,
            },
        )
        .map_err(io::Error::other)?;
    }

    let (watchdog, watchdog_finalizer) = if jobs.is_empty() {
        (None, None)
    } else {
        let worker_options = watchdog_worker_options(
            &options.harness_home,
            &options.source,
            options.runtime_workspace.as_ref(),
            &group_id,
            &master_agent,
            &master_session,
            "subagent-ledger-adapter",
            options.now_ms,
            coordinator.as_ref(),
        );
        let enqueue_options = WorkerEnqueueOptionsV4 {
            options: WorkerEnqueueOptionsV3 {
                options: WorkerEnqueueOptionsV2 {
                    options: worker_options,
                    child_policy: None,
                },
                result_owner: None,
            },
            authorized_execution_mode: None,
        };
        let transaction_report =
            enqueue_worker_job_v4_in_transaction(&transaction, enqueue_options.clone())?;
        let job_ref = transaction_job_ref(&transaction_report, "watchdog");
        (Some(job_ref), Some(enqueue_options))
    };
    set_watchdog_summary(&mut summary, watchdog.as_ref());
    transaction.commit().map_err(io::Error::other)?;

    // Re-enter the public idempotent path only after the atomic commit so
    // lifecycle and wake filesystem side effects can never precede durability.
    for finalize in finalizers.into_iter().chain(watchdog_finalizer.into_iter()) {
        let _ = enqueue_worker_job_v4(finalize)?;
    }

    Ok(WorkerAdapterEnqueueReport {
        schema: WORKER_ADAPTER_ENQUEUE_SCHEMA,
        adapter: "subagents".to_string(),
        harness_home: options.harness_home,
        source_home: options.source.home,
        source_workspace: options.source.workspace,
        job_group_id: group_id,
        summary,
        jobs,
        watchdog,
        warnings,
    })
}

/// Enqueues one bounded, real-lane coordinator replay without introducing a
/// second dispatch path. The source ledger must contain exactly the requested
/// two queued children; when exact channel state exists the lane session must
/// be current, and the main lane and every planned child agent, plus each child
/// route and effort, must be accepted by the current authoritative Codex model
/// catalog before the worker store is opened.
pub fn enqueue_controlled_coordinator_smoke(
    options: ControlledCoordinatorSmokeOptionsV1,
) -> io::Result<WorkerAdapterEnqueueReport> {
    validate_controlled_coordinator_smoke_lane(&options.lane)?;
    validate_controlled_coordinator_smoke_current_session(&options.harness_home, &options.lane)?;
    if options.children.len() != CONTROLLED_COORDINATOR_SMOKE_CHILD_COUNT {
        return Err(controlled_smoke_invalid_input(format!(
            "controlled coordinator smoke requires exactly {CONTROLLED_COORDINATOR_SMOKE_CHILD_COUNT} children"
        )));
    }

    let mut requested_children = BTreeMap::new();
    for child in &options.children {
        if child.run_id.trim().is_empty() || child.run_id != child.run_id.trim() {
            return Err(controlled_smoke_invalid_input(
                "controlled coordinator smoke child runId must be non-empty and canonical",
            ));
        }
        if requested_children
            .insert(child.run_id.clone(), child)
            .is_some()
        {
            return Err(controlled_smoke_invalid_input(format!(
                "controlled coordinator smoke repeats child runId `{}`",
                child.run_id
            )));
        }
    }
    if !options.children.iter().any(|child| child.effort == "max") {
        return Err(controlled_smoke_invalid_input(
            "controlled coordinator smoke requires at least one exact `max` child",
        ));
    }
    if options.children[0].model == options.children[1].model {
        return Err(controlled_smoke_invalid_input(
            "controlled coordinator smoke requires heterogeneous child models",
        ));
    }
    validate_controlled_coordinator_smoke_source(&options.source)?;

    // The intentionally scoped probe must never be pointed at a historical or
    // broad imported ledger. Prove the two caller-provided ids are the complete
    // resume plan before considering catalog or worker-store state.
    let ledger = load_subagent_ledger(&options.source)?;
    let plan = plan_subagents(
        &ledger,
        SubagentPlanInput {
            resume_subagents: true,
        },
    );
    let mut planned_run_ids = plan
        .entries
        .iter()
        .filter(|entry| entry.action == SubagentPlanAction::ResumeCandidate)
        .map(|entry| entry.run_id.clone())
        .collect::<Vec<_>>();
    planned_run_ids.sort();
    let requested_run_ids = requested_children.keys().cloned().collect::<Vec<_>>();
    if plan.entries.len() != CONTROLLED_COORDINATOR_SMOKE_CHILD_COUNT
        || planned_run_ids.len() != CONTROLLED_COORDINATOR_SMOKE_CHILD_COUNT
        || planned_run_ids != requested_run_ids
        || !plan.warnings.is_empty()
    {
        return Err(controlled_smoke_invalid_input(
            "controlled coordinator smoke source must plan exactly its two requested resume candidates without warnings",
        ));
    }

    // `enqueue_subagent_workers_v5` resolves a missing source agent to the
    // master agent, but otherwise dispatches under the source entry's agent.
    // Gate that exact execution identity here, before the worker store is
    // opened, so a catalog rollout cannot admit a child which runtime would
    // later quarantine fail-closed.
    let mut rollout_agent_ids = BTreeSet::from([options.lane.agent_id.clone()]);
    rollout_agent_ids.extend(
        plan.entries
            .iter()
            .filter(|entry| entry.action == SubagentPlanAction::ResumeCandidate)
            .map(|entry| {
                entry
                    .agent_id
                    .clone()
                    .unwrap_or_else(|| options.lane.agent_id.clone())
            }),
    );
    for agent_id in rollout_agent_ids {
        if model_catalog_rollout_mode_for_agent(Some(&options.harness_home), Some(&agent_id))
            != ModelCatalogRolloutMode::Authoritative
        {
            let scope = if agent_id == options.lane.agent_id {
                "the exact main lane".to_string()
            } else {
                format!("planned child agent `{agent_id}`")
            };
            return Err(controlled_smoke_invalid_input(format!(
                "controlled coordinator smoke requires authoritative modelCatalogV2 for {scope} before persisting workers",
            )));
        }
    }
    let catalog_file = options
        .harness_home
        .join("codex-home")
        .join("models_cache.json");
    let catalog_text = fs::read_to_string(&catalog_file).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!(
                "controlled coordinator smoke could not read authoritative Codex model catalog {}: {error}",
                catalog_file.display()
            ),
        )
    })?;
    let catalog = parse_codex_model_catalog(&catalog_text).map_err(|error| {
        controlled_smoke_invalid_input(format!(
            "controlled coordinator smoke rejected authoritative Codex model catalog: {error}"
        ))
    })?;

    let root_session_key = crate::root_working_session_key(&options.lane.session_key);
    let full_lane = FullLaneKeyV1::new(
        options.lane.platform.clone(),
        options.lane.account_id.clone(),
        options.lane.channel_id.clone(),
        options.lane.user_id.clone(),
        options.lane.agent_id.clone(),
        "interactive",
        root_session_key.clone(),
        options.lane.session_key.clone(),
    )
    .map_err(|error| {
        controlled_smoke_invalid_input(format!("invalid exact smoke lane: {error}"))
    })?;
    let virtual_session_id = crate::context_rollover::derive_virtual_session_id(
        &options.lane.platform,
        &options.lane.channel_id,
        &options.lane.user_id,
        &options.lane.agent_id,
        &root_session_key,
    );
    let replay_key = format!(
        "{}\n{}\n{}\n{}\n{}",
        options.source.home.display(),
        options.source.workspace.display(),
        options.lane.session_key,
        options.now_ms,
        requested_run_ids.join(",")
    );
    let replay_id = fnv1a_64_hex(&replay_key);
    let parent_queue_id = format!("t4-coordinator-smoke-parent-{replay_id}");
    let wait_id = format!("t4-coordinator-smoke-wait-{replay_id}");

    let mut child_policies_v2_by_run_id = BTreeMap::new();
    let mut result_owners_by_run_id = BTreeMap::new();
    let mut coordinator_owner = None;
    for (index, child) in options.children.iter().enumerate() {
        validate_controlled_smoke_route(child)?;
        let resolution = resolve_reasoning_effort(
            Some(&catalog),
            ModelCatalogRolloutMode::Authoritative,
            CONTROLLED_COORDINATOR_SMOKE_PROVIDER,
            &child.model,
            &child.effort,
            UnsupportedReasoningPolicy::Reject,
        );
        if resolution.status != ReasoningResolutionStatus::Accepted
            || !resolution.authoritative
            || resolution.effective_effort.as_deref() != Some(child.effort.as_str())
            || resolution.effective_provider.as_deref()
                != Some(CONTROLLED_COORDINATOR_SMOKE_PROVIDER)
            || resolution.effective_model.as_deref() != Some(child.model.as_str())
        {
            return Err(controlled_smoke_invalid_input(format!(
                "controlled coordinator smoke child `{}` route {}/{} effort `{}` was not advertised by the authoritative catalog: {}",
                child.run_id,
                CONTROLLED_COORDINATOR_SMOKE_PROVIDER,
                child.model,
                child.effort,
                resolution.reason
            )));
        }
        let policy_revision = u64::try_from(options.now_ms)
            .unwrap_or(1)
            .max(1)
            .saturating_add(u64::try_from(index).unwrap_or(0));
        let child_policy = ChildExecutionPolicyV1::new(ChildExecutionPolicyV1Input {
            policy_revision,
            provider: Some(CONTROLLED_COORDINATOR_SMOKE_PROVIDER.to_string()),
            model: Some(child.model.clone()),
            reasoning_preference: Some(
                ReasoningPreference::explicit(child.effort.clone()).map_err(|error| {
                    controlled_smoke_invalid_input(format!(
                        "controlled coordinator smoke child `{}` has invalid effort: {error}",
                        child.run_id
                    ))
                })?,
            ),
            backend_reasoning_policy: Some(
                BackendReasoningPolicyV1::new(BackendReasoningSource::ChildAdmission, resolution)
                    .map_err(|error| {
                        controlled_smoke_invalid_input(format!(
                            "controlled coordinator smoke child `{}` cannot bind an authoritative backend policy: {error}",
                            child.run_id
                        ))
                    })?,
            ),
            catalog_revision: Some(catalog.revision.clone()),
            tools_profile: "default".to_string(),
            sandbox_profile: "workspace-write".to_string(),
            timeout_ms: DEFAULT_WORKER_TIMEOUT_MS,
            heartbeat_timeout_ms: 60_000,
            max_attempts: 3,
            token_or_cost_budget: None,
            delegation_limit: None,
            result_contract: "child-result-envelope-v1".to_string(),
        })
        .map_err(|error| {
            controlled_smoke_invalid_input(format!(
                "controlled coordinator smoke child `{}` has invalid execution policy: {error}",
                child.run_id
            ))
        })?;
        let owner = ExactWorkerResultOwnerV1::new(
            full_lane.clone(),
            virtual_session_id.clone(),
            None,
            Some(parent_queue_id.clone()),
            format!(
                "t4-coordinator-smoke-source-{}-{replay_id}",
                normalize_key_part(&child.run_id)
            ),
            None,
            None,
        )
        .map_err(|error| {
            controlled_smoke_invalid_input(format!(
                "controlled coordinator smoke child `{}` has invalid exact owner: {error}",
                child.run_id
            ))
        })?;
        if coordinator_owner.is_none() {
            coordinator_owner = Some(owner.clone());
        }
        child_policies_v2_by_run_id.insert(
            child.run_id.clone(),
            ChildExecutionPolicyV2::new(child_policy, None).map_err(|error| {
                controlled_smoke_invalid_input(format!(
                    "controlled coordinator smoke child `{}` could not wrap its policy: {error}",
                    child.run_id
                ))
            })?,
        );
        result_owners_by_run_id.insert(child.run_id.clone(), WorkerResultOwnerV1::Exact(owner));
    }

    let coordinator_owner = coordinator_owner.ok_or_else(|| {
        controlled_smoke_invalid_input(
            "controlled coordinator smoke has no exact coordinator owner",
        )
    })?;
    enqueue_subagent_workers_v5(SubagentWorkerEnqueueOptionsV5 {
        options: SubagentWorkerEnqueueOptionsV4 {
            options: SubagentWorkerEnqueueOptionsV3 {
                options: SubagentWorkerEnqueueOptionsV2 {
                    options: SubagentWorkerEnqueueOptions {
                        harness_home: options.harness_home,
                        source: options.source,
                        resume_subagents: true,
                        master_agent_id: Some(options.lane.agent_id),
                        master_session_key: Some(options.lane.session_key),
                        runtime_workspace: options.runtime_workspace,
                        now_ms: options.now_ms,
                    },
                    child_policies_by_run_id: BTreeMap::new(),
                },
                result_owners_by_run_id,
            },
            coordinator: Some(SubagentCoordinatorResumeOptionsV1 {
                wait_id,
                owner: coordinator_owner,
            }),
        },
        child_policies_v2_by_run_id,
    })
}

fn controlled_smoke_invalid_input(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

fn validate_controlled_coordinator_smoke_lane(
    lane: &ControlledCoordinatorSmokeLaneV1,
) -> io::Result<()> {
    if !matches!(lane.platform.as_str(), "telegram" | "discord") {
        return Err(controlled_smoke_invalid_input(
            "controlled coordinator smoke only permits telegram or discord interactive lanes",
        ));
    }
    if lane.agent_id != "main" {
        return Err(controlled_smoke_invalid_input(
            "controlled coordinator smoke only permits the exact main agent lane",
        ));
    }
    for (label, value) in [
        ("accountId", lane.account_id.as_str()),
        ("channelId", lane.channel_id.as_str()),
        ("userId", lane.user_id.as_str()),
        ("sessionKey", lane.session_key.as_str()),
    ] {
        if value.trim().is_empty() || value != value.trim() {
            return Err(controlled_smoke_invalid_input(format!(
                "controlled coordinator smoke {label} must be non-empty and canonical"
            )));
        }
    }
    Ok(())
}

fn validate_controlled_coordinator_smoke_current_session(
    harness_home: &Path,
    lane: &ControlledCoordinatorSmokeLaneV1,
) -> io::Result<()> {
    let channel_lane = crate::ChannelStateLane::new(
        &lane.platform,
        Some(&lane.account_id),
        &lane.channel_id,
        &lane.user_id,
        &lane.agent_id,
    )
    .map_err(|error| {
        controlled_smoke_invalid_input(format!(
            "controlled coordinator smoke has invalid exact channel lane: {error}"
        ))
    })?;
    let state =
        crate::read_channel_session_state_v2(harness_home, &channel_lane).map_err(|error| {
            controlled_smoke_invalid_input(format!(
                "controlled coordinator smoke could not read exact channel session state: {error}"
            ))
        })?;
    if state.is_some_and(|state| state.active_session_key != lane.session_key) {
        return Err(controlled_smoke_invalid_input(
            "controlled coordinator smoke requires the exact active channel session when v2 state exists",
        ));
    }
    Ok(())
}

fn validate_controlled_coordinator_smoke_source(source: &AgentSource) -> io::Result<()> {
    let marker_file = source.home.join(CONTROLLED_COORDINATOR_SMOKE_SOURCE_MARKER);
    let marker_text = fs::read_to_string(&marker_file).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!(
                "controlled coordinator smoke requires its explicit source marker {}: {error}",
                marker_file.display()
            ),
        )
    })?;
    let marker = serde_json::from_str::<serde_json::Value>(&marker_text).map_err(|error| {
        controlled_smoke_invalid_input(format!(
            "controlled coordinator smoke source marker is not valid JSON: {error}"
        ))
    })?;
    if marker.get("schema").and_then(serde_json::Value::as_str)
        != Some(CONTROLLED_COORDINATOR_SMOKE_SOURCE_SCHEMA)
        || marker.get("purpose").and_then(serde_json::Value::as_str)
            != Some("bounded-two-child-replay")
    {
        return Err(controlled_smoke_invalid_input(
            "controlled coordinator smoke source marker has an unexpected schema or purpose",
        ));
    }
    Ok(())
}

fn validate_controlled_smoke_route(child: &ControlledCoordinatorSmokeChildV1) -> io::Result<()> {
    let canonical_model = child.model.trim().to_ascii_lowercase();
    if child.model != canonical_model || !child.model.starts_with("gpt-5.6-") {
        return Err(controlled_smoke_invalid_input(format!(
            "controlled coordinator smoke child `{}` must use a canonical gpt-5.6 model id",
            child.run_id
        )));
    }
    let canonical_effort = child.effort.trim().to_ascii_lowercase();
    if child.effort != canonical_effort || child.effort == "ultra" {
        return Err(controlled_smoke_invalid_input(format!(
            "controlled coordinator smoke child `{}` must use a canonical non-ultra effort",
            child.run_id
        )));
    }
    Ok(())
}

pub fn enqueue_subagent_workers_v4(
    options: SubagentWorkerEnqueueOptionsV4,
) -> io::Result<WorkerAdapterEnqueueReport> {
    let SubagentWorkerEnqueueOptionsV4 {
        options,
        coordinator,
    } = options;
    let SubagentWorkerEnqueueOptionsV3 {
        options,
        result_owners_by_run_id,
    } = options;
    let SubagentWorkerEnqueueOptionsV2 {
        options,
        child_policies_by_run_id,
    } = options;
    let ledger = load_subagent_ledger(&options.source)?;
    let plan = plan_subagents(
        &ledger,
        SubagentPlanInput {
            resume_subagents: options.resume_subagents,
        },
    );
    let group_id = stable_group_id("subagents", &options.source);
    let master_agent = options
        .master_agent_id
        .clone()
        .unwrap_or_else(|| "main".to_string());
    let master_session = options
        .master_session_key
        .clone()
        .unwrap_or_else(|| format!("worker-group:{group_id}"));
    let concurrency_group = master_concurrency_group(&master_agent, &master_session);
    let warnings = plan.warnings.clone();
    let mut jobs = Vec::new();
    let mut summary = WorkerAdapterEnqueueSummary {
        plan_entries: plan.entries.len(),
        ..WorkerAdapterEnqueueSummary::default()
    };

    validate_durable_coordinator_owners(
        coordinator.as_ref(),
        &plan.entries,
        &result_owners_by_run_id,
    )?;

    for entry in &plan.entries {
        if entry.action != SubagentPlanAction::ResumeCandidate {
            summary.skipped_entries += 1;
            continue;
        }
        let agent_id = entry
            .agent_id
            .clone()
            .unwrap_or_else(|| master_agent.clone());
        let message_text = entry
            .task
            .clone()
            .unwrap_or_else(|| format!("Resume imported subagent run {}", entry.run_id));
        let session_key = entry.session_key.clone().unwrap_or_else(|| {
            format!(
                "subagent:{}:{}",
                normalize_key_part(&entry.run_id),
                normalize_key_part(&agent_id)
            )
        });
        let payload = json!({
            "adapter": "subagent-ledger",
            "runId": entry.run_id,
            "sourceHome": &options.source.home,
            "sourceWorkspace": &options.source.workspace,
            "runtimeWorkspace": &options.runtime_workspace,
            "agentId": &agent_id,
            "sessionKey": session_key,
            "platform": "subagent-ledger",
            "channelId": entry.run_id,
            "userId": entry.parent_agent_id.clone().unwrap_or_else(|| master_agent.clone()),
            "messageText": message_text,
            "inboundContext": serde_json::to_string(entry).unwrap_or_default()
        });
        let report = enqueue_worker_job_v3(WorkerEnqueueOptionsV3 {
            options: WorkerEnqueueOptionsV2 {
                options: WorkerEnqueueOptions {
                    harness_home: options.harness_home.clone(),
                    kind: WorkerJobKind::LlmSubagent,
                    lane: Some("llm".to_string()),
                    payload,
                    idempotency_key: Some(format!("subagent-resume:{}", entry.run_id)),
                    parent_job_id: None,
                    job_group_id: Some(group_id.clone()),
                    master_agent_id: Some(master_agent.clone()),
                    master_session_key: Some(master_session.clone()),
                    wake_policy: None,
                    source: Some("subagent-ledger-adapter".to_string()),
                    priority: 0,
                    available_at_ms: Some(options.now_ms),
                    max_attempts: 3,
                    timeout_ms: Some(DEFAULT_WORKER_TIMEOUT_MS),
                    cascade_timeout_ms: Some(DEFAULT_WATCHDOG_TIMEOUT_MS),
                    rate_key: Some(format!("llm:{agent_id}")),
                    concurrency_group_key: Some(concurrency_group.clone()),
                    now_ms: options.now_ms,
                },
                child_policy: child_policies_by_run_id.get(entry.run_id.as_str()).cloned(),
            },
            result_owner: result_owners_by_run_id.get(entry.run_id.as_str()).cloned(),
        })?;
        push_job_ref(&mut summary, &mut jobs, report, "resume-candidate");
    }

    if let Some(coordinator) = &coordinator
        && !jobs.is_empty()
    {
        let db_file = crate::init_worker_store(&options.harness_home)?;
        let mut conn = rusqlite::Connection::open(db_file).map_err(io::Error::other)?;
        let transaction = conn.transaction().map_err(io::Error::other)?;
        persist_waiting_for_children_in_transaction(
            &transaction,
            &WorkerCoordinatorWaitCreateOptionsV1 {
                wait_id: coordinator.wait_id.clone(),
                owner: coordinator.owner.clone(),
                child_group_id: group_id.clone(),
                expected_child_job_ids: jobs.iter().map(|job| job.job_id.clone()).collect(),
                now_ms: options.now_ms,
            },
        )
        .map_err(io::Error::other)?;
        transaction.commit().map_err(io::Error::other)?;
    }

    let watchdog = enqueue_watchdog_if_needed(
        &options.harness_home,
        &options.source,
        options.runtime_workspace.as_ref(),
        &group_id,
        &master_agent,
        &master_session,
        "subagent-ledger-adapter",
        jobs.is_empty(),
        options.now_ms,
        coordinator.as_ref(),
    )?;
    set_watchdog_summary(&mut summary, watchdog.as_ref());

    Ok(WorkerAdapterEnqueueReport {
        schema: WORKER_ADAPTER_ENQUEUE_SCHEMA,
        adapter: "subagents".to_string(),
        harness_home: options.harness_home,
        source_home: options.source.home,
        source_workspace: options.source.workspace,
        job_group_id: group_id,
        summary,
        jobs,
        watchdog,
        warnings,
    })
}

fn validate_durable_coordinator_owners(
    coordinator: Option<&SubagentCoordinatorResumeOptionsV1>,
    entries: &[SubagentPlanEntry],
    result_owners_by_run_id: &BTreeMap<String, WorkerResultOwnerV1>,
) -> io::Result<()> {
    let Some(coordinator) = coordinator else {
        return Ok(());
    };
    let expected_key = coordinator.owner.coordinator_key().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid durable coordinator owner: {error}"),
        )
    })?;
    for entry in entries
        .iter()
        .filter(|entry| entry.action == SubagentPlanAction::ResumeCandidate)
    {
        let owner = result_owners_by_run_id
            .get(entry.run_id.as_str())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "durable coordinator requires an exact result owner for runId `{}`",
                        entry.run_id
                    ),
                )
            })?;
        let WorkerResultOwnerV1::Exact(owner) = owner else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "durable coordinator rejects legacy result owner for runId `{}`",
                    entry.run_id
                ),
            ));
        };
        let observed_key = owner.coordinator_key().map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid result owner for runId `{}`: {error}", entry.run_id),
            )
        })?;
        if observed_key != expected_key {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "result owner for runId `{}` belongs to a different parent coordinator",
                    entry.run_id
                ),
            ));
        }
    }
    Ok(())
}

fn enqueue_watchdog_if_needed(
    harness_home: &Path,
    source: &AgentSource,
    runtime_workspace: Option<&PathBuf>,
    group_id: &str,
    master_agent: &str,
    master_session: &str,
    source_label: &str,
    no_children: bool,
    now_ms: i64,
    coordinator: Option<&SubagentCoordinatorResumeOptionsV1>,
) -> io::Result<Option<WorkerAdapterJobRef>> {
    if no_children {
        return Ok(None);
    }
    let report = enqueue_worker_job(watchdog_worker_options(
        harness_home,
        source,
        runtime_workspace,
        group_id,
        master_agent,
        master_session,
        source_label,
        now_ms,
        coordinator,
    ))?;
    Ok(Some(job_ref(report, "watchdog")))
}

#[allow(clippy::too_many_arguments)]
fn watchdog_worker_options(
    harness_home: &Path,
    source: &AgentSource,
    runtime_workspace: Option<&PathBuf>,
    group_id: &str,
    master_agent: &str,
    master_session: &str,
    source_label: &str,
    now_ms: i64,
    coordinator: Option<&SubagentCoordinatorResumeOptionsV1>,
) -> WorkerEnqueueOptions {
    let mut payload = json!({
        "sourceHome": &source.home,
        "sourceWorkspace": &source.workspace,
        "runtimeWorkspace": runtime_workspace,
        "jobGroupId": group_id,
        "masterAgentId": master_agent,
        "masterSessionKey": master_session,
    });
    if let Some(coordinator) = coordinator {
        payload["coordinationMode"] = json!("durable-v1");
        payload["coordinatorWaitId"] = json!(coordinator.wait_id);
    }
    WorkerEnqueueOptions {
        harness_home: harness_home.to_path_buf(),
        kind: WorkerJobKind::Watchdog,
        lane: Some("watchdog".to_string()),
        payload,
        idempotency_key: Some(format!("{source_label}:watchdog:{group_id}")),
        parent_job_id: None,
        job_group_id: Some(group_id.to_string()),
        master_agent_id: Some(master_agent.to_string()),
        master_session_key: Some(master_session.to_string()),
        wake_policy: Some(json!({
            "mode": "all_completed",
            "deadlineMs": now_ms.saturating_add(i64::try_from(DEFAULT_WATCHDOG_TIMEOUT_MS).unwrap_or(i64::MAX)),
        })),
        source: Some(source_label.to_string()),
        priority: 10,
        available_at_ms: Some(now_ms),
        max_attempts: 10,
        timeout_ms: Some(DEFAULT_WORKER_TIMEOUT_MS),
        cascade_timeout_ms: Some(DEFAULT_WATCHDOG_TIMEOUT_MS),
        rate_key: None,
        concurrency_group_key: Some(master_concurrency_group(master_agent, master_session)),
        now_ms,
    }
}

fn push_job_ref(
    summary: &mut WorkerAdapterEnqueueSummary,
    jobs: &mut Vec<WorkerAdapterJobRef>,
    report: WorkerEnqueueReport,
    action: &str,
) {
    summary.candidate_jobs += 1;
    if report.inserted {
        summary.inserted_jobs += 1;
    } else {
        summary.existing_jobs += 1;
    }
    jobs.push(job_ref(report, action));
}

fn push_transaction_job_ref(
    summary: &mut WorkerAdapterEnqueueSummary,
    jobs: &mut Vec<WorkerAdapterJobRef>,
    report: &WorkerEnqueueTransactionReport,
    action: &str,
) {
    summary.candidate_jobs += 1;
    if report.inserted {
        summary.inserted_jobs += 1;
    } else {
        summary.existing_jobs += 1;
    }
    jobs.push(transaction_job_ref(report, action));
}

fn transaction_job_ref(
    report: &WorkerEnqueueTransactionReport,
    action: &str,
) -> WorkerAdapterJobRef {
    WorkerAdapterJobRef {
        job_id: report.job.job_id.clone(),
        kind: report.job.kind.as_str().to_string(),
        lane: report.job.lane.clone(),
        inserted: report.inserted,
        idempotency_key: report.job.idempotency_key.clone().unwrap_or_default(),
        source: report.job.source.clone(),
        action: action.to_string(),
    }
}

fn set_watchdog_summary(
    summary: &mut WorkerAdapterEnqueueSummary,
    watchdog: Option<&WorkerAdapterJobRef>,
) {
    if let Some(watchdog) = watchdog {
        if watchdog.inserted {
            summary.watchdog_inserted = true;
        } else {
            summary.watchdog_existing = true;
        }
    }
}

fn job_ref(report: WorkerEnqueueReport, action: &str) -> WorkerAdapterJobRef {
    WorkerAdapterJobRef {
        job_id: report.job.job_id,
        kind: report.job.kind.as_str().to_string(),
        lane: report.job.lane,
        inserted: report.inserted,
        idempotency_key: report.job.idempotency_key.unwrap_or_default(),
        source: report.job.source,
        action: action.to_string(),
    }
}

fn stable_group_id(prefix: &str, source: &AgentSource) -> String {
    format!(
        "{}:{}",
        prefix,
        fnv1a_64_hex(&format!(
            "{}\n{}",
            source.home.display(),
            source.workspace.display()
        ))
    )
}

fn master_concurrency_group(master_agent: &str, master_session: &str) -> String {
    format!(
        "master:{}:{}",
        normalize_key_part(master_agent),
        normalize_key_part(master_session)
    )
}

fn normalize_key_part(value: &str) -> String {
    let mut normalized = String::new();
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
            normalized.push(ch.to_ascii_lowercase());
        } else {
            normalized.push('_');
        }
    }
    if normalized.is_empty() {
        "unknown".to_string()
    } else {
        normalized
    }
}

fn fnv1a_64_hex(value: &str) -> String {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

#[allow(dead_code)]
fn _now_ms_fallback() -> i64 {
    current_log_time_ms().unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::backend_reasoning::{
        BackendReasoningPolicyV1, BackendReasoningSource, ReasoningPreference,
    };
    use crate::child_execution_policy::ChildExecutionPolicyV1Input;
    use crate::lane::FullLaneKeyV1;
    use crate::model_catalog::{
        REASONING_RESOLUTION_RECEIPT_SCHEMA_VERSION, ReasoningResolutionReceipt,
        ReasoningResolutionStatus,
    };
    use crate::{
        WorkerRunOnceOptions, WorkerRunOnceStatus, WorkerStatusOptions, collect_worker_status,
        run_worker_once,
    };

    #[test]
    fn deterministic_cron_adapter_enqueues_shell_job_and_watchdog() {
        let root = temp_root("deterministic_cron_adapter_enqueues_shell_job_and_watchdog");
        let source = write_deterministic_source(&root);
        let harness_home = root.join("harness");

        let report = enqueue_deterministic_cron_workers(DeterministicCronWorkerEnqueueOptions {
            harness_home: harness_home.clone(),
            source,
            allow_deterministic_run: true,
            dry_run_shell: true,
            master_agent_id: Some("main".to_string()),
            master_session_key: Some("master-session".to_string()),
            runtime_workspace: None,
            now_ms: 1000,
        })
        .unwrap();

        assert_eq!(report.summary.candidate_jobs, 1);
        assert_eq!(report.summary.inserted_jobs, 1);
        assert!(report.watchdog.is_some());
        let status = collect_worker_status(WorkerStatusOptions { harness_home }).unwrap();
        assert_eq!(status.totals.pending, 2);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn subagent_adapter_enqueues_resume_candidate() {
        let root = temp_root("subagent_adapter_enqueues_resume_candidate");
        let source = write_subagent_source(&root);
        let harness_home = root.join("harness");

        let report = enqueue_subagent_workers(SubagentWorkerEnqueueOptions {
            harness_home: harness_home.clone(),
            source,
            resume_subagents: true,
            master_agent_id: Some("main".to_string()),
            master_session_key: Some("master-session".to_string()),
            runtime_workspace: None,
            now_ms: 1000,
        })
        .unwrap();

        assert_eq!(report.summary.candidate_jobs, 1);
        assert_eq!(report.summary.inserted_jobs, 1);
        let status = collect_worker_status(WorkerStatusOptions { harness_home }).unwrap();
        assert_eq!(status.totals.pending, 2);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn subagent_adapter_applies_heterogeneous_policies_by_immutable_run_id() {
        let root = temp_root("subagent_adapter_applies_heterogeneous_policies_by_run_id");
        let source = write_heterogeneous_subagent_source(&root);
        let harness_home = root.join("harness");
        let sol_policy = managed_child_policy(1, "gpt-5.6-sol", "max");
        let terra_policy = managed_child_policy(2, "gpt-5.6-terra", "high");

        let report = enqueue_subagent_workers_v2(SubagentWorkerEnqueueOptionsV2 {
            options: SubagentWorkerEnqueueOptions {
                harness_home: harness_home.clone(),
                source,
                resume_subagents: true,
                master_agent_id: Some("main".to_string()),
                master_session_key: Some("master-session".to_string()),
                runtime_workspace: None,
                now_ms: 1000,
            },
            child_policies_by_run_id: BTreeMap::from([
                ("queued-1".to_string(), sol_policy.clone()),
                ("queued-2".to_string(), terra_policy.clone()),
            ]),
        })
        .unwrap();
        assert_eq!(report.summary.inserted_jobs, 2);

        let mut observed_policies = Vec::new();
        for now_ms in [1001, 1002] {
            let run = run_worker_once(WorkerRunOnceOptions {
                harness_home: harness_home.clone(),
                lane: Some("llm".to_string()),
                worker_id: format!("adapter-policy-worker-{now_ms}"),
                lease_ms: 300_000,
                now_ms,
            })
            .unwrap();
            assert_eq!(run.status, WorkerRunOnceStatus::Dispatched);
            observed_policies.push(run.job.unwrap().child_policy.unwrap());
        }
        assert!(observed_policies.contains(&sol_policy));
        assert!(observed_policies.contains(&terra_policy));

        let queue_text = fs::read_to_string(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("pending.jsonl"),
        )
        .unwrap();
        let queued = queue_text
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert!(queued.iter().any(|item| {
            item["sessionKey"] == "subagent:queued-1:researcher"
                && item["model"] == "gpt-5.6-sol"
                && item["reasoningPreference"]["effort"] == "max"
        }));
        assert!(queued.iter().any(|item| {
            item["sessionKey"] == "subagent:queued-2:coder"
                && item["model"] == "gpt-5.6-terra"
                && item["reasoningPreference"]["effort"] == "high"
        }));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn subagent_adapter_applies_exact_result_owners_by_run_id() {
        let root = temp_root("subagent_adapter_applies_exact_result_owners_by_run_id");
        let source = write_heterogeneous_subagent_source(&root);
        let harness_home = root.join("harness");
        let owner_one = exact_owner("channel-1", "virtual-1", "queue-1");
        let owner_two = exact_owner("channel-2", "virtual-2", "queue-2");

        let report = enqueue_subagent_workers_v3(SubagentWorkerEnqueueOptionsV3 {
            options: SubagentWorkerEnqueueOptionsV2 {
                options: SubagentWorkerEnqueueOptions {
                    harness_home: harness_home.clone(),
                    source,
                    resume_subagents: true,
                    master_agent_id: Some("main".to_string()),
                    master_session_key: Some("master-session".to_string()),
                    runtime_workspace: None,
                    now_ms: 1000,
                },
                child_policies_by_run_id: BTreeMap::new(),
            },
            result_owners_by_run_id: BTreeMap::from([
                ("queued-1".to_string(), owner_one.clone()),
                ("queued-2".to_string(), owner_two.clone()),
            ]),
        })
        .unwrap();
        assert_eq!(report.summary.inserted_jobs, 2);

        let conn = rusqlite::Connection::open(crate::worker_db_file(&harness_home)).unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT payload_json, result_owner_json FROM jobs WHERE kind='llm_subagent' ORDER BY job_id",
            )
            .unwrap();
        let observed = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .unwrap()
            .map(|row| {
                let (payload, owner) = row.unwrap();
                let run_id = serde_json::from_str::<serde_json::Value>(&payload).unwrap()["runId"]
                    .as_str()
                    .unwrap()
                    .to_string();
                let owner = serde_json::from_str::<WorkerResultOwnerV1>(&owner).unwrap();
                (run_id, owner)
            })
            .collect::<BTreeMap<_, _>>();
        assert_eq!(observed.get("queued-1"), Some(&owner_one));
        assert_eq!(observed.get("queued-2"), Some(&owner_two));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn subagent_adapter_v4_persists_exact_wait_before_durable_watchdog() {
        let root = temp_root("subagent_adapter_v4_persists_exact_wait");
        let source = write_heterogeneous_subagent_source(&root);
        let harness_home = root.join("harness");
        let owner_one = exact_parent_owner("child-queue-1");
        let owner_two = exact_parent_owner("child-queue-2");
        let coordinator_owner = match &owner_one {
            WorkerResultOwnerV1::Exact(owner) => owner.clone(),
            WorkerResultOwnerV1::LegacyIncomplete(_) => unreachable!(),
        };

        let report = enqueue_subagent_workers_v4(SubagentWorkerEnqueueOptionsV4 {
            options: SubagentWorkerEnqueueOptionsV3 {
                options: SubagentWorkerEnqueueOptionsV2 {
                    options: SubagentWorkerEnqueueOptions {
                        harness_home: harness_home.clone(),
                        source,
                        resume_subagents: true,
                        master_agent_id: Some("main".to_string()),
                        master_session_key: Some("discord:channel-1:user-1:main".to_string()),
                        runtime_workspace: None,
                        now_ms: 1000,
                    },
                    child_policies_by_run_id: BTreeMap::new(),
                },
                result_owners_by_run_id: BTreeMap::from([
                    ("queued-1".to_string(), owner_one),
                    ("queued-2".to_string(), owner_two),
                ]),
            },
            coordinator: Some(SubagentCoordinatorResumeOptionsV1 {
                wait_id: "wait-parent-queue-1".to_string(),
                owner: coordinator_owner,
            }),
        })
        .unwrap();
        assert_eq!(report.summary.inserted_jobs, 2);
        let watchdog = report.watchdog.as_ref().unwrap();

        let conn = rusqlite::Connection::open(crate::worker_db_file(&harness_home)).unwrap();
        let wait =
            crate::worker_coordination::load_worker_coordinator_wait(&conn, "wait-parent-queue-1")
                .unwrap()
                .unwrap();
        let mut expected_ids = report
            .jobs
            .iter()
            .map(|job| job.job_id.clone())
            .collect::<Vec<_>>();
        expected_ids.sort();
        assert_eq!(wait.expected_child_job_ids, expected_ids);

        let watchdog_payload: String = conn
            .query_row(
                "SELECT payload_json FROM jobs WHERE job_id=?1",
                [&watchdog.job_id],
                |row| row.get(0),
            )
            .unwrap();
        let watchdog_payload: serde_json::Value = serde_json::from_str(&watchdog_payload).unwrap();
        assert_eq!(watchdog_payload["coordinationMode"], "durable-v1");
        assert_eq!(watchdog_payload["coordinatorWaitId"], "wait-parent-queue-1");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn subagent_adapter_v5_atomically_preserves_heterogeneous_models_and_efforts() {
        let root = temp_root("subagent_adapter_v5_heterogeneous_models_efforts");
        let source = write_heterogeneous_subagent_source(&root);
        let harness_home = root.join("harness");
        let owner_one = exact_parent_owner("child-queue-1");
        let owner_two = exact_parent_owner("child-queue-2");
        let exact_one = match &owner_one {
            WorkerResultOwnerV1::Exact(owner) => owner.clone(),
            WorkerResultOwnerV1::LegacyIncomplete(_) => unreachable!(),
        };
        let coordinator_owner = exact_one.clone();
        let sol = ChildExecutionPolicyV2::new(managed_child_policy(11, "gpt-5.6-sol", "max"), None)
            .unwrap();
        let standard =
            ChildExecutionPolicyV2::new(managed_child_policy(12, "gpt-5.6-terra", "high"), None)
                .unwrap();
        let options = SubagentWorkerEnqueueOptionsV5 {
            options: SubagentWorkerEnqueueOptionsV4 {
                options: SubagentWorkerEnqueueOptionsV3 {
                    options: SubagentWorkerEnqueueOptionsV2 {
                        options: SubagentWorkerEnqueueOptions {
                            harness_home: harness_home.clone(),
                            source: source.clone(),
                            resume_subagents: true,
                            master_agent_id: Some("main".to_string()),
                            master_session_key: Some("discord:channel-1:user-1:main".to_string()),
                            runtime_workspace: None,
                            now_ms: 1000,
                        },
                        child_policies_by_run_id: BTreeMap::new(),
                    },
                    result_owners_by_run_id: BTreeMap::from([
                        ("queued-1".to_string(), owner_one.clone()),
                        ("queued-2".to_string(), owner_two.clone()),
                    ]),
                },
                coordinator: Some(SubagentCoordinatorResumeOptionsV1 {
                    wait_id: "wait-v5-parent".to_string(),
                    owner: coordinator_owner,
                }),
            },
            child_policies_v2_by_run_id: BTreeMap::from([
                ("queued-1".to_string(), sol.clone()),
                ("queued-2".to_string(), standard.clone()),
            ]),
        };

        let report = enqueue_subagent_workers_v5(options.clone()).unwrap();
        assert_eq!(report.summary.inserted_jobs, 2);
        assert!(report.summary.watchdog_inserted);
        let duplicate = enqueue_subagent_workers_v5(options).unwrap();
        assert_eq!(duplicate.summary.inserted_jobs, 0);
        assert_eq!(duplicate.summary.existing_jobs, 2);
        assert!(duplicate.summary.watchdog_existing);

        let conn = rusqlite::Connection::open(crate::worker_db_file(&harness_home)).unwrap();
        let mut statement = conn
            .prepare(
                "SELECT payload_json, child_policy_json, execution_mode_json FROM jobs WHERE kind='llm_subagent'",
            )
            .unwrap();
        let stored = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            })
            .unwrap()
            .map(|row| {
                let (payload, policy, execution) = row.unwrap();
                let run_id = serde_json::from_str::<serde_json::Value>(&payload).unwrap()["runId"]
                    .as_str()
                    .unwrap()
                    .to_string();
                (
                    run_id,
                    (
                        serde_json::from_str::<ChildExecutionPolicyV1>(&policy).unwrap(),
                        execution.map(|value| {
                            serde_json::from_str::<crate::AuthorizedExecutionModeSnapshotV2>(&value)
                                .unwrap()
                        }),
                    ),
                )
            })
            .collect::<BTreeMap<_, _>>();
        assert_eq!(stored["queued-1"].0, *sol.child_policy());
        assert!(stored["queued-1"].1.is_none());
        assert_eq!(stored["queued-2"].0, *standard.child_policy());
        assert!(stored["queued-2"].1.is_none());
        drop(statement);
        drop(conn);

        for now_ms in [1001, 1002] {
            let run = run_worker_once(WorkerRunOnceOptions {
                harness_home: harness_home.clone(),
                lane: Some("llm".to_string()),
                worker_id: format!("v5-worker-{now_ms}"),
                lease_ms: 300_000,
                now_ms,
            })
            .unwrap();
            assert_eq!(run.status, WorkerRunOnceStatus::Dispatched);
        }
        let queued = fs::read_to_string(
            harness_home
                .join("state")
                .join("runtime-queue")
                .join("pending.jsonl"),
        )
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
        let sol = queued
            .iter()
            .find(|item| item["agentId"] == "researcher")
            .unwrap();
        assert_eq!(sol["schema"], "agent-harness.runtime-queue-item.v1");
        assert_eq!(sol["model"], "gpt-5.6-sol");
        assert_eq!(sol["reasoningPreference"]["effort"], "max");
        assert!(sol.get("authorizedExecutionMode").is_none());
        let terra = queued
            .iter()
            .find(|item| item["agentId"] == "coder")
            .unwrap();
        assert_eq!(terra["schema"], "agent-harness.runtime-queue-item.v1");
        assert_eq!(terra["model"], "gpt-5.6-terra");
        assert_eq!(terra["reasoningPreference"]["effort"], "high");
        assert!(terra.get("authorizedExecutionMode").is_none());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn controlled_coordinator_smoke_enqueues_only_two_catalog_admitted_children() {
        let root = temp_root("controlled_coordinator_smoke_catalog_admission");
        let source = write_main_controlled_coordinator_source(&root);
        write_controlled_coordinator_smoke_source_marker(&source);
        let harness_home = root.join("harness");
        write_authoritative_model_catalog_fixture(&harness_home);
        write_controlled_coordinator_smoke_active_session(
            &harness_home,
            "telegram:channel-1:user-1:main:t4-smoke",
        );

        let report = enqueue_controlled_coordinator_smoke(ControlledCoordinatorSmokeOptionsV1 {
            harness_home: harness_home.clone(),
            source,
            runtime_workspace: None,
            lane: ControlledCoordinatorSmokeLaneV1 {
                platform: "telegram".to_string(),
                account_id: "account-1".to_string(),
                channel_id: "channel-1".to_string(),
                user_id: "user-1".to_string(),
                agent_id: "main".to_string(),
                session_key: "telegram:channel-1:user-1:main:t4-smoke".to_string(),
            },
            children: vec![
                ControlledCoordinatorSmokeChildV1 {
                    run_id: "queued-1".to_string(),
                    model: "gpt-5.6-terra".to_string(),
                    effort: "max".to_string(),
                },
                ControlledCoordinatorSmokeChildV1 {
                    run_id: "queued-2".to_string(),
                    model: "gpt-5.6-sol".to_string(),
                    effort: "high".to_string(),
                },
            ],
            now_ms: 1_000,
        })
        .unwrap();

        assert_eq!(report.summary.plan_entries, 2);
        assert_eq!(report.summary.inserted_jobs, 2);
        assert!(report.summary.watchdog_inserted);

        let conn = rusqlite::Connection::open(crate::worker_db_file(&harness_home)).unwrap();
        let mut statement = conn
            .prepare(
                "SELECT payload_json, child_policy_json, result_owner_json FROM jobs WHERE kind='llm_subagent' ORDER BY job_id",
            )
            .unwrap();
        let stored = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .unwrap()
            .map(|row| {
                let (payload, policy, owner) = row.unwrap();
                let run_id = serde_json::from_str::<serde_json::Value>(&payload).unwrap()["runId"]
                    .as_str()
                    .unwrap()
                    .to_string();
                (
                    run_id,
                    (
                        serde_json::from_str::<ChildExecutionPolicyV1>(&policy).unwrap(),
                        serde_json::from_str::<WorkerResultOwnerV1>(&owner).unwrap(),
                    ),
                )
            })
            .collect::<BTreeMap<_, _>>();
        assert_eq!(stored.len(), 2);
        assert_eq!(stored["queued-1"].0.model(), Some("gpt-5.6-terra"));
        assert_eq!(
            stored["queued-1"].0.reasoning_preference(),
            Some(&ReasoningPreference::explicit("max").unwrap())
        );
        assert_eq!(stored["queued-2"].0.model(), Some("gpt-5.6-sol"));
        assert_eq!(
            stored["queued-2"].0.reasoning_preference(),
            Some(&ReasoningPreference::explicit("high").unwrap())
        );
        let coordinator_keys = stored
            .values()
            .map(|(_, owner)| match owner {
                WorkerResultOwnerV1::Exact(owner) => owner.coordinator_key().unwrap(),
                WorkerResultOwnerV1::LegacyIncomplete(_) => {
                    panic!("controlled coordinator smoke must use exact owners")
                }
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(coordinator_keys.len(), 1);
        assert!(report.watchdog.is_some());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn controlled_coordinator_smoke_rejects_stale_live_session_before_persisting_workers() {
        let root = temp_root("controlled_coordinator_smoke_rejects_stale_live_session");
        let source = write_main_controlled_coordinator_source(&root);
        write_controlled_coordinator_smoke_source_marker(&source);
        let harness_home = root.join("harness");
        write_authoritative_model_catalog_fixture(&harness_home);
        write_controlled_coordinator_smoke_active_session(
            &harness_home,
            "telegram:channel-1:user-1:main:currently-active",
        );

        let error = enqueue_controlled_coordinator_smoke(ControlledCoordinatorSmokeOptionsV1 {
            harness_home: harness_home.clone(),
            source,
            runtime_workspace: None,
            lane: ControlledCoordinatorSmokeLaneV1 {
                platform: "telegram".to_string(),
                account_id: "account-1".to_string(),
                channel_id: "channel-1".to_string(),
                user_id: "user-1".to_string(),
                agent_id: "main".to_string(),
                session_key: "telegram:channel-1:user-1:main:t4-smoke".to_string(),
            },
            children: vec![
                ControlledCoordinatorSmokeChildV1 {
                    run_id: "queued-1".to_string(),
                    model: "gpt-5.6-terra".to_string(),
                    effort: "max".to_string(),
                },
                ControlledCoordinatorSmokeChildV1 {
                    run_id: "queued-2".to_string(),
                    model: "gpt-5.6-sol".to_string(),
                    effort: "high".to_string(),
                },
            ],
            now_ms: 1_000,
        })
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("requires the exact active channel session"),
            "{error}"
        );
        assert!(
            !crate::worker_db_file(&harness_home).exists(),
            "stale live-session rejection must occur before the worker store is opened"
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn controlled_coordinator_smoke_rejects_non_authoritative_child_agent_before_persisting_workers()
     {
        let root = temp_root("controlled_coordinator_smoke_rejects_non_authoritative_child_agent");
        let source = write_heterogeneous_subagent_source(&root);
        write_controlled_coordinator_smoke_source_marker(&source);
        let harness_home = root.join("harness");
        write_authoritative_model_catalog_fixture(&harness_home);

        let error = enqueue_controlled_coordinator_smoke(ControlledCoordinatorSmokeOptionsV1 {
            harness_home: harness_home.clone(),
            source,
            runtime_workspace: None,
            lane: ControlledCoordinatorSmokeLaneV1 {
                platform: "telegram".to_string(),
                account_id: "account-1".to_string(),
                channel_id: "channel-1".to_string(),
                user_id: "user-1".to_string(),
                agent_id: "main".to_string(),
                session_key: "telegram:channel-1:user-1:main:t4-smoke".to_string(),
            },
            children: vec![
                ControlledCoordinatorSmokeChildV1 {
                    run_id: "queued-1".to_string(),
                    model: "gpt-5.6-terra".to_string(),
                    effort: "max".to_string(),
                },
                ControlledCoordinatorSmokeChildV1 {
                    run_id: "queued-2".to_string(),
                    model: "gpt-5.6-sol".to_string(),
                    effort: "high".to_string(),
                },
            ],
            now_ms: 1_000,
        })
        .unwrap_err();

        assert!(
            error.to_string().contains("planned child agent `"),
            "{error}"
        );
        assert!(
            !crate::worker_db_file(&harness_home).exists(),
            "child-agent catalog rejection must occur before the worker store is opened"
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn controlled_coordinator_smoke_rejects_ultra_before_persisting_workers() {
        let root = temp_root("controlled_coordinator_smoke_rejects_ultra");
        let source = write_main_controlled_coordinator_source(&root);
        write_controlled_coordinator_smoke_source_marker(&source);
        let harness_home = root.join("harness");
        write_authoritative_model_catalog_fixture(&harness_home);

        let error = enqueue_controlled_coordinator_smoke(ControlledCoordinatorSmokeOptionsV1 {
            harness_home: harness_home.clone(),
            source,
            runtime_workspace: None,
            lane: ControlledCoordinatorSmokeLaneV1 {
                platform: "telegram".to_string(),
                account_id: "account-1".to_string(),
                channel_id: "channel-1".to_string(),
                user_id: "user-1".to_string(),
                agent_id: "main".to_string(),
                session_key: "telegram:channel-1:user-1:main:t4-smoke".to_string(),
            },
            children: vec![
                ControlledCoordinatorSmokeChildV1 {
                    run_id: "queued-1".to_string(),
                    model: "gpt-5.6-terra".to_string(),
                    effort: "max".to_string(),
                },
                ControlledCoordinatorSmokeChildV1 {
                    run_id: "queued-2".to_string(),
                    model: "gpt-5.6-sol".to_string(),
                    effort: "ultra".to_string(),
                },
            ],
            now_ms: 1_000,
        })
        .unwrap_err();

        assert!(error.to_string().contains("non-ultra"), "{error}");
        assert!(
            !crate::worker_db_file(&harness_home).exists(),
            "catalog rejection must occur before the worker store is opened"
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn subagent_adapter_v5_rolls_back_children_wait_and_watchdog_together() {
        let root = temp_root("subagent_adapter_v5_atomic_rollback");
        let source = write_heterogeneous_subagent_source(&root);
        let harness_home = root.join("harness");
        let owner_one = exact_parent_owner("child-queue-1");
        let owner_two = exact_parent_owner("child-queue-2");
        let exact_one = match &owner_one {
            WorkerResultOwnerV1::Exact(owner) => owner.clone(),
            WorkerResultOwnerV1::LegacyIncomplete(_) => unreachable!(),
        };
        let mismatched_coder_snapshot = authorized_child_standard_snapshot("coder", &exact_one);
        let error = enqueue_subagent_workers_v5(SubagentWorkerEnqueueOptionsV5 {
            options: SubagentWorkerEnqueueOptionsV4 {
                options: SubagentWorkerEnqueueOptionsV3 {
                    options: SubagentWorkerEnqueueOptionsV2 {
                        options: SubagentWorkerEnqueueOptions {
                            harness_home: harness_home.clone(),
                            source,
                            resume_subagents: true,
                            master_agent_id: Some("main".to_string()),
                            master_session_key: Some("discord:channel-1:user-1:main".to_string()),
                            runtime_workspace: None,
                            now_ms: 1000,
                        },
                        child_policies_by_run_id: BTreeMap::new(),
                    },
                    result_owners_by_run_id: BTreeMap::from([
                        ("queued-1".to_string(), owner_one.clone()),
                        ("queued-2".to_string(), owner_two),
                    ]),
                },
                coordinator: Some(SubagentCoordinatorResumeOptionsV1 {
                    wait_id: "wait-v5-rollback".to_string(),
                    owner: exact_one.clone(),
                }),
            },
            child_policies_v2_by_run_id: BTreeMap::from([
                (
                    "queued-1".to_string(),
                    ChildExecutionPolicyV2::new(
                        managed_child_policy(21, "gpt-5.6-sol", "max"),
                        Some(authorized_child_standard_snapshot("researcher", &exact_one)),
                    )
                    .unwrap(),
                ),
                (
                    "queued-2".to_string(),
                    ChildExecutionPolicyV2::new(
                        managed_child_policy(22, "gpt-5.6-terra", "max"),
                        Some(mismatched_coder_snapshot),
                    )
                    .unwrap(),
                ),
            ]),
        })
        .unwrap_err();
        assert!(error.to_string().contains("snapshot owner must equal"));
        let conn = rusqlite::Connection::open(crate::worker_db_file(&harness_home)).unwrap();
        let jobs: i64 = conn
            .query_row("SELECT COUNT(*) FROM jobs", [], |row| row.get(0))
            .unwrap();
        assert_eq!(
            jobs, 0,
            "the caller-owned transaction must roll back all jobs"
        );
        assert!(
            crate::worker_coordination::load_worker_coordinator_wait(&conn, "wait-v5-rollback")
                .unwrap()
                .is_none()
        );
        let _ = fs::remove_dir_all(root);
    }

    fn managed_child_policy(
        policy_revision: u64,
        model: &str,
        effort: &str,
    ) -> ChildExecutionPolicyV1 {
        let resolution = ReasoningResolutionReceipt {
            schema_version: REASONING_RESOLUTION_RECEIPT_SCHEMA_VERSION,
            requested_provider: "openai".to_string(),
            requested_model: model.to_string(),
            effective_provider: Some("openai".to_string()),
            effective_model: Some(model.to_string()),
            requested_effort: effort.to_string(),
            effective_effort: Some(effort.to_string()),
            catalog_effective_effort: Some(effort.to_string()),
            catalog_revision: Some("catalog-test".to_string()),
            status: ReasoningResolutionStatus::Accepted,
            authoritative: true,
            reason: "adapter child policy test".to_string(),
        };
        ChildExecutionPolicyV1::new(ChildExecutionPolicyV1Input {
            policy_revision,
            provider: Some("openai".to_string()),
            model: Some(model.to_string()),
            reasoning_preference: Some(ReasoningPreference::explicit(effort).unwrap()),
            backend_reasoning_policy: Some(
                BackendReasoningPolicyV1::new(BackendReasoningSource::ChildAdmission, resolution)
                    .unwrap(),
            ),
            catalog_revision: Some("catalog-test".to_string()),
            tools_profile: "default".to_string(),
            sandbox_profile: "workspace-write".to_string(),
            timeout_ms: 300_000,
            heartbeat_timeout_ms: 60_000,
            max_attempts: 3,
            token_or_cost_budget: None,
            delegation_limit: None,
            result_contract: "child-result-envelope-v1".to_string(),
        })
        .unwrap()
    }

    fn exact_owner(
        channel_id: &str,
        virtual_session_id: &str,
        source_queue_id: &str,
    ) -> WorkerResultOwnerV1 {
        WorkerResultOwnerV1::Exact(
            crate::worker_result_mailbox::ExactWorkerResultOwnerV1::new(
                FullLaneKeyV1::new(
                    "discord",
                    "account-1",
                    channel_id,
                    "user-1",
                    "main",
                    "codex",
                    "root-session",
                    "concrete-session",
                )
                .unwrap(),
                virtual_session_id,
                None,
                None,
                source_queue_id,
                None,
                None,
            )
            .unwrap(),
        )
    }

    fn exact_parent_owner(source_queue_id: &str) -> WorkerResultOwnerV1 {
        WorkerResultOwnerV1::Exact(
            crate::worker_result_mailbox::ExactWorkerResultOwnerV1::new(
                FullLaneKeyV1::new(
                    "discord",
                    "account-1",
                    "channel-1",
                    "user-1",
                    "main",
                    "interactive",
                    "root-session",
                    "discord:channel-1:user-1:main",
                )
                .unwrap(),
                "virtual-session-1",
                None,
                Some("parent-queue-1".to_string()),
                source_queue_id,
                None,
                None,
            )
            .unwrap(),
        )
    }

    fn authorized_child_standard_snapshot(
        execution_agent_id: &str,
        result_owner: &ExactWorkerResultOwnerV1,
    ) -> crate::AuthorizedExecutionModeSnapshotV2 {
        let preference = crate::ExecutionModePreference::explicit("standard").unwrap();
        let policy = crate::ExecutionModePolicyV1::new(
            crate::ExecutionModeSource::ChildAdmission,
            &preference,
            "standard",
            execution_agent_id,
            "auth-v1",
            "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            2,
            6,
            300_000,
        )
        .unwrap();
        let readiness = crate::SafeResumeReadinessReceiptV1::new(
            result_owner,
            "durability-v1",
            true,
            true,
            true,
            true,
        )
        .unwrap();
        crate::AuthorizedExecutionModeSnapshotV2::new(
            preference,
            Some(policy),
            Some(WorkerResultOwnerV1::Exact(result_owner.clone())),
            Some(readiness),
        )
        .unwrap()
    }

    fn write_deterministic_source(root: &Path) -> AgentSource {
        let home = root.join(".openclaw");
        let workspace = root.join("workspace");
        let runner = workspace.join("tools").join("cron-runner");
        fs::create_dir_all(runner.join("crontab")).unwrap();
        fs::create_dir_all(runner.join("jobs")).unwrap();
        fs::create_dir_all(workspace.join("tools").join("backup-cron-runner")).unwrap();
        fs::write(
            runner.join("crontab").join("agent.crontab"),
            "* * * * * jobs/rotate.ps1\n",
        )
        .unwrap();
        fs::write(runner.join("jobs").join("rotate.ps1"), "Write-Output ok\n").unwrap();
        fs::create_dir_all(&home).unwrap();
        AgentSource::with_workspace(home, workspace)
    }

    fn write_subagent_source(root: &Path) -> AgentSource {
        let home = root.join(".openclaw");
        let workspace = root.join("workspace");
        fs::create_dir_all(home.join("subagents")).unwrap();
        fs::create_dir_all(&workspace).unwrap();
        fs::write(
            home.join("subagents").join("runs.json"),
            r#"{
              "runs": [
                {
                  "id": "queued-1",
                  "agentId": "researcher",
                  "parentAgentId": "main",
                  "status": "queued",
                  "task": "continue research"
                }
              ]
            }"#,
        )
        .unwrap();
        AgentSource::with_workspace(home, workspace)
    }

    fn write_heterogeneous_subagent_source(root: &Path) -> AgentSource {
        let home = root.join(".openclaw");
        let workspace = root.join("workspace");
        fs::create_dir_all(home.join("subagents")).unwrap();
        fs::create_dir_all(&workspace).unwrap();
        fs::write(
            home.join("subagents").join("runs.json"),
            r#"{
              "runs": [
                {
                  "id": "queued-1",
                  "agentId": "researcher",
                  "parentAgentId": "main",
                  "status": "queued",
                  "task": "continue research"
                },
                {
                  "id": "queued-2",
                  "agentId": "coder",
                  "parentAgentId": "main",
                  "status": "queued",
                  "task": "continue implementation"
                }
              ]
            }"#,
        )
        .unwrap();
        AgentSource::with_workspace(home, workspace)
    }

    fn write_main_controlled_coordinator_source(root: &Path) -> AgentSource {
        let home = root.join(".openclaw");
        let workspace = root.join("workspace");
        fs::create_dir_all(home.join("subagents")).unwrap();
        fs::create_dir_all(&workspace).unwrap();
        fs::write(
            home.join("subagents").join("runs.json"),
            r#"{
              "runs": [
                {
                  "id": "queued-1",
                  "agentId": "main",
                  "parentAgentId": "main",
                  "status": "queued",
                  "task": "continue controlled coordinator smoke one"
                },
                {
                  "id": "queued-2",
                  "agentId": "main",
                  "parentAgentId": "main",
                  "status": "queued",
                  "task": "continue controlled coordinator smoke two"
                }
              ]
            }"#,
        )
        .unwrap();
        AgentSource::with_workspace(home, workspace)
    }

    fn write_controlled_coordinator_smoke_active_session(harness_home: &Path, session_key: &str) {
        let lane = crate::ChannelStateLane::new(
            "telegram",
            Some("account-1"),
            "channel-1",
            "user-1",
            "main",
        )
        .unwrap();
        let mut state = crate::ChannelSessionState {
            schema: "agent-harness.channel-session-state.v2".to_string(),
            platform: "telegram".to_string(),
            account_id: Some("account-1".to_string()),
            channel_id: "channel-1".to_string(),
            user_id: "user-1".to_string(),
            active_session_key: session_key.to_string(),
            agent_id: Some("main".to_string()),
            config_revision: None,
            provider: None,
            model: None,
            session_topic: None,
            model_override: None,
            model_override_provider: None,
            model_override_model: None,
            thinking_enabled: false,
            thinking_level: None,
            thinking_instruction: None,
            reasoning_preference: None,
            backend_reasoning_policy: None,
            fast_mode: None,
            stop_requested: false,
            stop_reason: None,
            steering_notes: Vec::new(),
            btw_notes: Vec::new(),
            last_command: None,
            updated_at_ms: 1_000,
        };
        crate::bind_channel_session_state_to_lane_v2(&mut state, &lane);
        crate::write_channel_session_state_v2(harness_home, &lane, &state).unwrap();
    }

    fn write_authoritative_model_catalog_fixture(harness_home: &Path) {
        fs::create_dir_all(harness_home.join("codex-home")).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            r#"{
              "orchestration": {
                "features": {
                  "modelCatalogV2": {
                    "mode": "authoritative",
                    "enabledAgentIds": ["main"]
                  }
                }
              }
            }"#,
        )
        .unwrap();
        fs::write(
            harness_home.join("codex-home").join("models_cache.json"),
            r#"{
              "models": [
                {
                  "slug": "gpt-5.6-terra",
                  "default_reasoning_level": "medium",
                  "supported_reasoning_levels": ["low", "medium", "high", "xhigh", "max"]
                },
                {
                  "slug": "gpt-5.6-sol",
                  "default_reasoning_level": "low",
                  "supported_reasoning_levels": ["low", "medium", "high", "xhigh", "max"]
                }
              ]
            }"#,
        )
        .unwrap();
    }

    fn write_controlled_coordinator_smoke_source_marker(source: &AgentSource) {
        fs::write(
            source.home.join(CONTROLLED_COORDINATOR_SMOKE_SOURCE_MARKER),
            r#"{
              "schema": "agent-harness.controlled-coordinator-smoke-source.v1",
              "purpose": "bounded-two-child-replay"
            }"#,
        )
        .unwrap();
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-worker-adapters-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
