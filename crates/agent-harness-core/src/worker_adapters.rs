use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::json;

use crate::child_execution_policy::ChildExecutionPolicyV1;
use crate::{
    AgentSource, DeterministicCronPlanAction, DeterministicCronPlanInput, NativeCronPlanAction,
    NativeCronPlanInput, SubagentPlanAction, SubagentPlanInput, WorkerEnqueueOptions,
    WorkerEnqueueOptionsV2, WorkerEnqueueReport, WorkerJobKind, current_log_time_ms,
    enqueue_worker_job, enqueue_worker_job_v2, load_agent_registry, load_deterministic_cron_store,
    load_native_cron_store, load_subagent_ledger, plan_deterministic_cron, plan_native_cron,
    plan_subagents,
};

const WORKER_ADAPTER_ENQUEUE_SCHEMA: &str = "agent-harness.worker-adapter-enqueue.v1";
const DEFAULT_WORKER_TIMEOUT_MS: u64 = 300_000;
const DEFAULT_WATCHDOG_TIMEOUT_MS: u64 = 900_000;

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
        let report = enqueue_worker_job_v2(WorkerEnqueueOptionsV2 {
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
        })?;
        push_job_ref(&mut summary, &mut jobs, report, "resume-candidate");
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
) -> io::Result<Option<WorkerAdapterJobRef>> {
    if no_children {
        return Ok(None);
    }
    let payload = json!({
        "sourceHome": &source.home,
        "sourceWorkspace": &source.workspace,
        "runtimeWorkspace": runtime_workspace,
        "jobGroupId": group_id,
        "masterAgentId": master_agent,
        "masterSessionKey": master_session,
    });
    let report = enqueue_worker_job(WorkerEnqueueOptions {
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
    })?;
    Ok(Some(job_ref(report, "watchdog")))
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
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::backend_reasoning::{
        BackendReasoningPolicyV1, BackendReasoningSource, ReasoningPreference,
    };
    use crate::child_execution_policy::ChildExecutionPolicyV1Input;
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
        let terra_policy = managed_child_policy(2, "gpt-5.6-terra", "ultra");

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
                && item["reasoningPreference"]["effort"] == "ultra"
        }));

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
