use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
#[cfg(not(windows))]
use std::time::SystemTime;

use serde::{Deserialize, Serialize};
use serde_json::Value;
#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt;

use crate::{
    AgentSource, HarnessLogEvent, HarnessLogLevel, PromptAssemblyOptions, append_harness_log,
    assemble_prompt_bundle, build_runtime_skill_index, build_turn_plan, current_log_time_ms,
    load_agent_registry, load_worker_dispatch_config, write_json_atomic, write_prompt_bundle,
};

const RUNTIME_QUEUE_PREPARE_REPORT_SCHEMA: &str = "agent-harness.runtime-queue-prepare.v1";
const RUNTIME_QUEUE_LEASES_SCHEMA: &str = "agent-harness.runtime-queue-leases.v1";
const DEFAULT_RUNTIME_LEASE_MS: i64 = 30 * 60 * 1000;
#[cfg(not(windows))]
const RUNTIME_LEASE_LOCK_STALE_MS: i64 = 30_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeQueuePrepareOptions {
    pub harness_home: PathBuf,
    pub queue_id: Option<String>,
    pub prompt_options: PromptAssemblyOptions,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeQueueCapacityOptions {
    pub harness_home: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeQueueCapacityReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub queue_file: PathBuf,
    pub leases_file: PathBuf,
    pub claimable_items: usize,
    pub claimable_queue_ids: Vec<String>,
    pub leased_items: usize,
    pub global_limit: usize,
    pub agent_limit: usize,
    pub agent_channel_limit: usize,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeQueuePrepareReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub queue_file: PathBuf,
    pub execution_receipts_file: PathBuf,
    pub item: Option<RuntimeQueuePreparedItem>,
    pub receipt: RuntimeExecutionReceipt,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeQueuePreparedItem {
    pub queue_id: String,
    pub agent_id: String,
    pub session_key: String,
    pub platform: String,
    pub channel_id: String,
    pub user_id: String,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub execution_dir: PathBuf,
    pub prompt_bundle_json: PathBuf,
    pub prompt_markdown: PathBuf,
    pub receipt_file: PathBuf,
    pub planned_transcript_file: PathBuf,
    pub planned_trajectory_file: PathBuf,
    pub selected_skill_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeExecutionReceipt {
    pub queue_id: Option<String>,
    pub status: RuntimeExecutionReceiptStatus,
    pub execution_dir: Option<PathBuf>,
    pub prompt_bundle_json: Option<PathBuf>,
    pub prompt_markdown: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_workspace: Option<PathBuf>,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RuntimeExecutionReceiptStatus {
    Prepared,
    AlreadyPrepared,
    LeaseBusy,
    NoPendingItem,
}

#[derive(Debug, Clone)]
struct PendingQueueItem {
    queue_id: String,
    agent_id: String,
    session_key: String,
    platform: String,
    channel_id: String,
    user_id: String,
    message_text: String,
    inbound_context: Option<String>,
    source_home: PathBuf,
    source_workspace: PathBuf,
    runtime_workspace: Option<PathBuf>,
    planned_transcript_file: PathBuf,
    planned_trajectory_file: PathBuf,
    selected_skill_ids: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RuntimeQueueLeaseState {
    #[serde(default = "runtime_queue_leases_schema")]
    schema: String,
    #[serde(default)]
    leases: BTreeMap<String, RuntimeQueueLease>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RuntimeQueueLease {
    queue_id: String,
    agent_id: String,
    platform: String,
    channel_id: String,
    session_key: String,
    owner: String,
    started_at_ms: i64,
    lease_expires_at_ms: i64,
}

struct RuntimeQueueLeaseLock {
    path: PathBuf,
    _file: fs::File,
}

impl Drop for RuntimeQueueLeaseLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

pub fn prepare_runtime_queue_item(
    options: RuntimeQueuePrepareOptions,
) -> io::Result<RuntimeQueuePrepareReport> {
    let queue_dir = options.harness_home.join("state").join("runtime-queue");
    let queue_file = queue_dir.join("pending.jsonl");
    let execution_receipts_file = queue_dir.join("execution-receipts.jsonl");
    fs::create_dir_all(&queue_dir)?;

    let mut warnings = Vec::new();
    let now_ms = current_log_time_ms()?;
    let lease_owner = format!("pid:{}", std::process::id());
    let _lease_lock = match acquire_runtime_queue_lease_lock(&queue_dir, now_ms)? {
        Some(lock) => lock,
        None => {
            let receipt = RuntimeExecutionReceipt {
                queue_id: options.queue_id,
                status: RuntimeExecutionReceiptStatus::LeaseBusy,
                execution_dir: None,
                prompt_bundle_json: None,
                prompt_markdown: None,
                runtime_workspace: None,
                reason: "runtime queue lease lock is busy".to_string(),
            };
            append_json_line(&execution_receipts_file, &receipt)?;
            return Ok(RuntimeQueuePrepareReport {
                schema: RUNTIME_QUEUE_PREPARE_REPORT_SCHEMA,
                harness_home: options.harness_home,
                queue_file,
                execution_receipts_file,
                item: None,
                receipt,
                warnings,
            });
        }
    };
    let prepared_receipts = read_prepared_receipts(&execution_receipts_file, &mut warnings)?;
    let terminal_run_ids =
        read_terminal_run_once_ids(&queue_dir.join("run-once-receipts.jsonl"), &mut warnings)?;
    let mut lease_state = read_runtime_queue_leases(&queue_dir, &mut warnings)?;
    purge_runtime_queue_leases(&mut lease_state, now_ms, &terminal_run_ids);
    let pending_items = read_pending_items(&queue_file, &mut warnings)?;
    let pending_by_id = pending_items
        .iter()
        .cloned()
        .map(|item| (item.queue_id.clone(), item))
        .collect::<HashMap<_, _>>();
    if let Some(requested_queue_id) = options.queue_id.as_deref()
        && let Some(prepared) = prepared_receipts.get(requested_queue_id)
    {
        if lease_state.leases.contains_key(requested_queue_id) {
            let receipt = RuntimeExecutionReceipt {
                queue_id: Some(requested_queue_id.to_string()),
                status: RuntimeExecutionReceiptStatus::NoPendingItem,
                execution_dir: None,
                prompt_bundle_json: None,
                prompt_markdown: None,
                runtime_workspace: None,
                reason: "requested runtime queue item is already leased".to_string(),
            };
            write_runtime_queue_leases(&queue_dir, &lease_state)?;
            append_json_line(&execution_receipts_file, &receipt)?;
            return Ok(RuntimeQueuePrepareReport {
                schema: RUNTIME_QUEUE_PREPARE_REPORT_SCHEMA,
                harness_home: options.harness_home,
                queue_file,
                execution_receipts_file,
                item: None,
                receipt,
                warnings,
            });
        }
        if let Some(pending) = pending_by_id.get(requested_queue_id) {
            if let Some(blocker) =
                runtime_capacity_blocker(&options.harness_home, &lease_state, pending)?
            {
                let receipt = RuntimeExecutionReceipt {
                    queue_id: Some(requested_queue_id.to_string()),
                    status: RuntimeExecutionReceiptStatus::NoPendingItem,
                    execution_dir: None,
                    prompt_bundle_json: None,
                    prompt_markdown: None,
                    runtime_workspace: None,
                    reason: format!("runtime queue capacity blocked by {blocker}"),
                };
                write_runtime_queue_leases(&queue_dir, &lease_state)?;
                append_json_line(&execution_receipts_file, &receipt)?;
                return Ok(RuntimeQueuePrepareReport {
                    schema: RUNTIME_QUEUE_PREPARE_REPORT_SCHEMA,
                    harness_home: options.harness_home,
                    queue_file,
                    execution_receipts_file,
                    item: None,
                    receipt,
                    warnings,
                });
            }
            lease_runtime_queue_item(&mut lease_state, pending, &lease_owner, now_ms);
        }
        write_runtime_queue_leases(&queue_dir, &lease_state)?;
        let receipt = RuntimeExecutionReceipt {
            queue_id: Some(requested_queue_id.to_string()),
            status: RuntimeExecutionReceiptStatus::AlreadyPrepared,
            execution_dir: prepared.execution_dir.clone(),
            prompt_bundle_json: prepared.prompt_bundle_json.clone(),
            prompt_markdown: prepared.prompt_markdown.clone(),
            runtime_workspace: prepared.runtime_workspace.clone(),
            reason: "requested runtime queue item was already prepared".to_string(),
        };
        append_json_line(&execution_receipts_file, &receipt)?;
        append_harness_log(
            &options.harness_home,
            &HarnessLogEvent::new(
                current_log_time_ms()?,
                HarnessLogLevel::Info,
                "runtime-queue",
                "queue.prepare.already-prepared",
                receipt.reason.clone(),
            )
            .queue_id(receipt.queue_id.clone())
            .path(receipt.execution_dir.clone()),
        )?;
        return Ok(RuntimeQueuePrepareReport {
            schema: RUNTIME_QUEUE_PREPARE_REPORT_SCHEMA,
            harness_home: options.harness_home,
            queue_file,
            execution_receipts_file,
            item: None,
            receipt,
            warnings,
        });
    }
    if options.queue_id.is_none() {
        for (queue_id, prepared) in prepared_receipts.iter().filter(|(queue_id, _)| {
            !terminal_run_ids.contains(*queue_id) && !lease_state.leases.contains_key(*queue_id)
        }) {
            if let Some(pending) = pending_by_id.get(queue_id) {
                if let Some(blocker) =
                    runtime_capacity_blocker(&options.harness_home, &lease_state, pending)?
                {
                    warnings.push(format!(
                    "prepared runtime queue item `{queue_id}` blocked by {blocker}; checking queued items"
                ));
                } else {
                    lease_runtime_queue_item(&mut lease_state, pending, &lease_owner, now_ms);
                    write_runtime_queue_leases(&queue_dir, &lease_state)?;
                    let receipt = RuntimeExecutionReceipt {
                    queue_id: Some(queue_id.clone()),
                    status: RuntimeExecutionReceiptStatus::AlreadyPrepared,
                    execution_dir: prepared.execution_dir.clone(),
                    prompt_bundle_json: prepared.prompt_bundle_json.clone(),
                    prompt_markdown: prepared.prompt_markdown.clone(),
                    runtime_workspace: prepared.runtime_workspace.clone(),
                    reason:
                        "resuming previously prepared runtime queue item without terminal run receipt"
                            .to_string(),
                };
                    append_json_line(&execution_receipts_file, &receipt)?;
                    append_harness_log(
                        &options.harness_home,
                        &HarnessLogEvent::new(
                            current_log_time_ms()?,
                            HarnessLogLevel::Info,
                            "runtime-queue",
                            "queue.prepare.resume-prepared",
                            receipt.reason.clone(),
                        )
                        .queue_id(receipt.queue_id.clone())
                        .path(receipt.execution_dir.clone()),
                    )?;
                    return Ok(RuntimeQueuePrepareReport {
                        schema: RUNTIME_QUEUE_PREPARE_REPORT_SCHEMA,
                        harness_home: options.harness_home,
                        queue_file,
                        execution_receipts_file,
                        item: None,
                        receipt,
                        warnings,
                    });
                }
            } else {
                warnings.push(format!(
                    "prepared runtime queue item `{queue_id}` had no pending queue metadata; skipping automatic resume"
                ));
            }
        }
    }
    let prepared_ids = prepared_receipts.keys().cloned().collect::<HashSet<_>>();
    let Some(pending) = select_pending_item(
        pending_items,
        options.queue_id.as_deref(),
        &prepared_ids,
        &lease_state,
        &options.harness_home,
        &mut warnings,
    )?
    else {
        write_runtime_queue_leases(&queue_dir, &lease_state)?;
        let receipt = RuntimeExecutionReceipt {
            queue_id: options.queue_id,
            status: RuntimeExecutionReceiptStatus::NoPendingItem,
            execution_dir: None,
            prompt_bundle_json: None,
            prompt_markdown: None,
            runtime_workspace: None,
            reason: "no matching queued runtime item found".to_string(),
        };
        append_json_line(&execution_receipts_file, &receipt)?;
        append_harness_log(
            &options.harness_home,
            &HarnessLogEvent::new(
                current_log_time_ms()?,
                HarnessLogLevel::Info,
                "runtime-queue",
                "queue.prepare.no-pending",
                receipt.reason.clone(),
            )
            .queue_id(receipt.queue_id.clone()),
        )?;
        return Ok(RuntimeQueuePrepareReport {
            schema: RUNTIME_QUEUE_PREPARE_REPORT_SCHEMA,
            harness_home: options.harness_home,
            queue_file,
            execution_receipts_file,
            item: None,
            receipt,
            warnings,
        });
    };
    lease_runtime_queue_item(&mut lease_state, &pending, &lease_owner, now_ms);
    write_runtime_queue_leases(&queue_dir, &lease_state)?;

    let prompt_workspace = prompt_source_workspace(&pending.source_home, &pending.source_workspace);
    if !paths_equivalent(&prompt_workspace, &pending.source_workspace) {
        warnings.push(format!(
            "using imported prompt workspace {} instead of queued source workspace {}",
            prompt_workspace.display(),
            pending.source_workspace.display()
        ));
    }
    let source = AgentSource::with_workspace(&pending.source_home, &prompt_workspace);
    let registry = load_agent_registry(&source)?;
    let skill_index = build_runtime_skill_index(&source, &options.harness_home)?;
    let plan = build_turn_plan(
        &source,
        &registry,
        &skill_index,
        crate::TurnPlanInput {
            harness_home: Some(options.harness_home.clone()),
            platform: pending.platform.clone(),
            channel_id: pending.channel_id.clone(),
            user_id: pending.user_id.clone(),
            text: pending.message_text.clone(),
            inbound_context: pending.inbound_context.clone(),
            requested_agent_id: Some(pending.agent_id.clone()),
            session_hint: Some(pending.session_key.clone()),
            skill_limit: pending.selected_skill_ids.len().max(5),
        },
    )?;
    let actual_skill_ids = plan
        .selected_skills
        .iter()
        .map(|skill| skill.skill_id.clone())
        .collect::<Vec<_>>();
    if !pending.selected_skill_ids.is_empty() && pending.selected_skill_ids != actual_skill_ids {
        warnings.push(format!(
            "prepared skill selection differs from queued selection: queued={:?}, prepared={:?}",
            pending.selected_skill_ids, actual_skill_ids
        ));
    }
    let bundle = assemble_prompt_bundle(
        &plan,
        PromptAssemblyOptions {
            harness_home: Some(options.harness_home.clone()),
            ..options.prompt_options
        },
    )?;

    let execution_dir = queue_execution_dir(&options.harness_home, &pending.queue_id);
    fs::create_dir_all(&execution_dir)?;
    let prompt_files = write_prompt_bundle(&bundle, &execution_dir)?;
    let receipt_file = execution_dir.join("execution-receipt.json");
    let item = RuntimeQueuePreparedItem {
        queue_id: pending.queue_id.clone(),
        agent_id: pending.agent_id.clone(),
        session_key: pending.session_key.clone(),
        platform: pending.platform.clone(),
        channel_id: pending.channel_id.clone(),
        user_id: pending.user_id.clone(),
        provider: bundle.provider.clone(),
        model: bundle.model.clone(),
        execution_dir: execution_dir.clone(),
        prompt_bundle_json: prompt_files.json.clone(),
        prompt_markdown: prompt_files.markdown.clone(),
        receipt_file: receipt_file.clone(),
        planned_transcript_file: pending.planned_transcript_file,
        planned_trajectory_file: pending.planned_trajectory_file,
        selected_skill_ids: actual_skill_ids,
    };
    let receipt = RuntimeExecutionReceipt {
        queue_id: Some(pending.queue_id),
        status: RuntimeExecutionReceiptStatus::Prepared,
        execution_dir: Some(execution_dir),
        prompt_bundle_json: Some(prompt_files.json),
        prompt_markdown: Some(prompt_files.markdown),
        runtime_workspace: pending.runtime_workspace,
        reason: "prompt bundle prepared; Codex runtime adapter not invoked yet".to_string(),
    };
    write_json_atomic(&receipt_file, &receipt)?;
    append_json_line(&execution_receipts_file, &receipt)?;
    append_harness_log(
        &options.harness_home,
        &HarnessLogEvent::new(
            current_log_time_ms()?,
            HarnessLogLevel::Info,
            "runtime-queue",
            "queue.prepare.prepared",
            receipt.reason.clone(),
        )
        .queue_id(receipt.queue_id.clone())
        .session_key(Some(item.session_key.clone()))
        .agent_id(Some(item.agent_id.clone()))
        .path(Some(item.execution_dir.clone())),
    )?;

    Ok(RuntimeQueuePrepareReport {
        schema: RUNTIME_QUEUE_PREPARE_REPORT_SCHEMA,
        harness_home: options.harness_home,
        queue_file,
        execution_receipts_file,
        item: Some(item),
        receipt,
        warnings,
    })
}

pub fn inspect_runtime_queue_capacity(
    options: RuntimeQueueCapacityOptions,
) -> io::Result<RuntimeQueueCapacityReport> {
    let queue_dir = options.harness_home.join("state").join("runtime-queue");
    let queue_file = queue_dir.join("pending.jsonl");
    let execution_receipts_file = queue_dir.join("execution-receipts.jsonl");
    fs::create_dir_all(&queue_dir)?;

    let config = load_worker_dispatch_config(&options.harness_home)?;
    let mut warnings = Vec::new();
    let now_ms = current_log_time_ms()?;
    let Some(_lease_lock) = acquire_runtime_queue_lease_lock(&queue_dir, now_ms)? else {
        warnings.push("runtime queue lease lock is busy; capacity assumed zero".to_string());
        return Ok(RuntimeQueueCapacityReport {
            schema: "agent-harness.runtime-queue-capacity.v1",
            harness_home: options.harness_home,
            queue_file,
            leases_file: runtime_queue_leases_file(&queue_dir),
            claimable_items: 0,
            claimable_queue_ids: Vec::new(),
            leased_items: 0,
            global_limit: config.global_concurrency_limit,
            agent_limit: config.group_concurrency_limit,
            agent_channel_limit: config.channel_concurrency_limit,
            warnings,
        });
    };

    let prepared_receipts = read_prepared_receipts(&execution_receipts_file, &mut warnings)?;
    let terminal_run_ids =
        read_terminal_run_once_ids(&queue_dir.join("run-once-receipts.jsonl"), &mut warnings)?;
    let mut lease_state = read_runtime_queue_leases(&queue_dir, &mut warnings)?;
    purge_runtime_queue_leases(&mut lease_state, now_ms, &terminal_run_ids);
    write_runtime_queue_leases(&queue_dir, &lease_state)?;
    let pending_items = read_pending_items(&queue_file, &mut warnings)?;
    let pending_by_id = pending_items
        .iter()
        .cloned()
        .map(|item| (item.queue_id.clone(), item))
        .collect::<HashMap<_, _>>();

    let mut simulated = lease_state.clone();
    let mut claimable_items = 0usize;
    let mut claimable_queue_ids = Vec::new();
    let prepared_candidates = prepared_receipts
        .keys()
        .filter(|queue_id| {
            !terminal_run_ids.contains(*queue_id) && !lease_state.leases.contains_key(*queue_id)
        })
        .cloned()
        .collect::<Vec<_>>();
    for queue_id in prepared_candidates {
        if let Some(pending) = pending_by_id.get(&queue_id)
            && runtime_capacity_blocker(&options.harness_home, &simulated, pending)?.is_none()
        {
            claimable_items += 1;
            claimable_queue_ids.push(queue_id);
            lease_runtime_queue_item(&mut simulated, pending, "capacity-inspect", now_ms);
        }
    }

    let prepared_ids = prepared_receipts.keys().cloned().collect::<HashSet<_>>();
    for pending in pending_items {
        if prepared_ids.contains(&pending.queue_id)
            || terminal_run_ids.contains(&pending.queue_id)
            || simulated.leases.contains_key(&pending.queue_id)
        {
            continue;
        }
        if runtime_capacity_blocker(&options.harness_home, &simulated, &pending)?.is_none() {
            claimable_items += 1;
            claimable_queue_ids.push(pending.queue_id.clone());
            lease_runtime_queue_item(&mut simulated, &pending, "capacity-inspect", now_ms);
        }
    }

    Ok(RuntimeQueueCapacityReport {
        schema: "agent-harness.runtime-queue-capacity.v1",
        harness_home: options.harness_home,
        queue_file,
        leases_file: runtime_queue_leases_file(&queue_dir),
        claimable_items,
        claimable_queue_ids,
        leased_items: lease_state.leases.len(),
        global_limit: config.global_concurrency_limit,
        agent_limit: config.group_concurrency_limit,
        agent_channel_limit: config.channel_concurrency_limit,
        warnings,
    })
}

fn prompt_source_workspace(source_home: &Path, queued_source_workspace: &Path) -> PathBuf {
    let imported_workspace = source_home.join("workspace");
    if imported_workspace.is_dir() {
        imported_workspace
    } else {
        queued_source_workspace.to_path_buf()
    }
}

fn paths_equivalent(left: &Path, right: &Path) -> bool {
    if left == right {
        return true;
    }
    match (fs::canonicalize(left), fs::canonicalize(right)) {
        (Ok(left), Ok(right)) => left == right,
        _ => false,
    }
}

fn runtime_queue_leases_schema() -> String {
    RUNTIME_QUEUE_LEASES_SCHEMA.to_string()
}

fn runtime_queue_leases_file(queue_dir: &Path) -> PathBuf {
    queue_dir.join("runtime-leases.json")
}

fn runtime_queue_lease_lock_file(queue_dir: &Path) -> PathBuf {
    queue_dir.join("runtime-leases.lock")
}

fn acquire_runtime_queue_lease_lock(
    queue_dir: &Path,
    now_ms: i64,
) -> io::Result<Option<RuntimeQueueLeaseLock>> {
    let lock_file = runtime_queue_lease_lock_file(queue_dir);
    match create_runtime_queue_lease_lock(&lock_file, now_ms) {
        Ok(lock) => return Ok(Some(lock)),
        Err(error) if runtime_queue_lease_lock_is_busy(&error) => {}
        Err(error) => return Err(error),
    }

    #[cfg(windows)]
    {
        Ok(None)
    }
    #[cfg(not(windows))]
    {
        if runtime_queue_lease_lock_is_stale(&lock_file, now_ms) {
            let _ = fs::remove_file(&lock_file);
            return match create_runtime_queue_lease_lock(&lock_file, now_ms) {
                Ok(lock) => Ok(Some(lock)),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(None),
                Err(error) => Err(error),
            };
        }

        Ok(None)
    }
}

fn runtime_queue_lease_lock_is_busy(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::AlreadyExists | io::ErrorKind::PermissionDenied | io::ErrorKind::WouldBlock
    ) || {
        #[cfg(windows)]
        {
            // ERROR_SHARING_VIOLATION: another loop thread/process has the
            // exclusive Windows lock file open.
            error.raw_os_error() == Some(32)
        }
        #[cfg(not(windows))]
        {
            false
        }
    }
}

fn create_runtime_queue_lease_lock(
    lock_file: &Path,
    now_ms: i64,
) -> io::Result<RuntimeQueueLeaseLock> {
    let mut options = OpenOptions::new();
    options.create(true).write(true).truncate(true);
    #[cfg(not(windows))]
    {
        options.create_new(true);
    }
    #[cfg(windows)]
    {
        options.share_mode(0);
    }
    let mut file = options.open(lock_file)?;
    writeln!(file, "{now_ms}")?;
    Ok(RuntimeQueueLeaseLock {
        path: lock_file.to_path_buf(),
        _file: file,
    })
}

#[cfg(not(windows))]
fn runtime_queue_lease_lock_is_stale(lock_file: &Path, now_ms: i64) -> bool {
    if let Ok(text) = fs::read_to_string(lock_file)
        && let Ok(created_at_ms) = text.trim().parse::<i64>()
    {
        return now_ms.saturating_sub(created_at_ms) > RUNTIME_LEASE_LOCK_STALE_MS;
    }
    lock_file
        .metadata()
        .and_then(|metadata| metadata.modified())
        .ok()
        .and_then(|modified| SystemTime::now().duration_since(modified).ok())
        .is_some_and(|age| age.as_millis() > u128::from(RUNTIME_LEASE_LOCK_STALE_MS as u64))
}

fn read_runtime_queue_leases(
    queue_dir: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<RuntimeQueueLeaseState> {
    let leases_file = runtime_queue_leases_file(queue_dir);
    if !leases_file.is_file() {
        return Ok(RuntimeQueueLeaseState {
            schema: runtime_queue_leases_schema(),
            leases: BTreeMap::new(),
        });
    }
    let text = fs::read_to_string(&leases_file)?;
    match serde_json::from_str::<RuntimeQueueLeaseState>(&text) {
        Ok(mut state) => {
            if state.schema.trim().is_empty() {
                state.schema = runtime_queue_leases_schema();
            }
            Ok(state)
        }
        Err(error) => {
            warnings.push(format!(
                "runtime queue leases file {} is not valid JSON: {error}; starting with empty lease state",
                leases_file.display()
            ));
            Ok(RuntimeQueueLeaseState {
                schema: runtime_queue_leases_schema(),
                leases: BTreeMap::new(),
            })
        }
    }
}

fn write_runtime_queue_leases(queue_dir: &Path, state: &RuntimeQueueLeaseState) -> io::Result<()> {
    write_json_atomic(&runtime_queue_leases_file(queue_dir), state)
}

fn purge_runtime_queue_leases(
    state: &mut RuntimeQueueLeaseState,
    now_ms: i64,
    terminal_run_ids: &HashSet<String>,
) {
    state.leases.retain(|queue_id, lease| {
        lease.lease_expires_at_ms > now_ms && !terminal_run_ids.contains(queue_id)
    });
}

pub fn release_runtime_queue_lease(
    harness_home: impl AsRef<Path>,
    queue_id: &str,
) -> io::Result<()> {
    let queue_dir = harness_home.as_ref().join("state").join("runtime-queue");
    fs::create_dir_all(&queue_dir)?;
    let now_ms = current_log_time_ms()?;
    let Some(_lease_lock) = acquire_runtime_queue_lease_lock(&queue_dir, now_ms)? else {
        return Ok(());
    };
    let mut warnings = Vec::new();
    let mut state = read_runtime_queue_leases(&queue_dir, &mut warnings)?;
    state.leases.remove(queue_id);
    write_runtime_queue_leases(&queue_dir, &state)
}

fn runtime_capacity_blocker(
    harness_home: &Path,
    state: &RuntimeQueueLeaseState,
    item: &PendingQueueItem,
) -> io::Result<Option<String>> {
    let config = load_worker_dispatch_config(harness_home)?;
    let executing_global = state.leases.len();
    if executing_global >= config.global_concurrency_limit {
        return Ok(Some("global limit".to_string()));
    }

    let executing_agent = state
        .leases
        .values()
        .filter(|lease| lease.agent_id == item.agent_id)
        .count();
    if executing_agent >= config.group_concurrency_limit {
        return Ok(Some(format!("agent limit for `{}`", item.agent_id)));
    }

    let channel_key = runtime_channel_key(&item.agent_id, &item.platform, &item.channel_id);
    let executing_channel = state
        .leases
        .values()
        .filter(|lease| {
            runtime_channel_key(&lease.agent_id, &lease.platform, &lease.channel_id) == channel_key
        })
        .count();
    if executing_channel >= config.channel_concurrency_limit {
        return Ok(Some(format!("agent-channel limit for `{}`", channel_key)));
    }

    Ok(None)
}

fn lease_runtime_queue_item(
    state: &mut RuntimeQueueLeaseState,
    item: &PendingQueueItem,
    owner: &str,
    now_ms: i64,
) {
    state.leases.insert(
        item.queue_id.clone(),
        RuntimeQueueLease {
            queue_id: item.queue_id.clone(),
            agent_id: item.agent_id.clone(),
            platform: item.platform.clone(),
            channel_id: item.channel_id.clone(),
            session_key: item.session_key.clone(),
            owner: owner.to_string(),
            started_at_ms: now_ms,
            lease_expires_at_ms: now_ms.saturating_add(DEFAULT_RUNTIME_LEASE_MS),
        },
    );
}

fn runtime_channel_key(agent_id: &str, platform: &str, channel_id: &str) -> String {
    format!(
        "{}:{}:{}",
        normalize_key_part(agent_id),
        normalize_key_part(platform),
        normalize_key_part(channel_id)
    )
}

fn select_pending_item(
    pending_items: Vec<PendingQueueItem>,
    requested_queue_id: Option<&str>,
    prepared_ids: &HashSet<String>,
    lease_state: &RuntimeQueueLeaseState,
    harness_home: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<Option<PendingQueueItem>> {
    for item in pending_items {
        if requested_queue_id.is_some_and(|requested| requested != item.queue_id) {
            continue;
        }
        if prepared_ids.contains(&item.queue_id) {
            warnings.push(format!(
                "runtime queue item `{}` already has a prepared receipt; skipping",
                item.queue_id
            ));
            continue;
        }
        if lease_state.leases.contains_key(&item.queue_id) {
            warnings.push(format!(
                "runtime queue item `{}` is already leased; skipping",
                item.queue_id
            ));
            continue;
        }
        if let Some(blocker) = runtime_capacity_blocker(harness_home, lease_state, &item)? {
            warnings.push(format!(
                "runtime queue item `{}` blocked by {}; skipping",
                item.queue_id, blocker
            ));
            continue;
        }
        return Ok(Some(item));
    }
    Ok(None)
}

fn read_pending_items(
    queue_file: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<Vec<PendingQueueItem>> {
    if !queue_file.is_file() {
        warnings.push(format!(
            "runtime queue file not found at {}",
            queue_file.display()
        ));
        return Ok(Vec::new());
    }

    let text = fs::read_to_string(queue_file)?;
    let mut items = Vec::new();
    for (index, line) in text.lines().enumerate() {
        let line_number = index + 1;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let value: Value = match serde_json::from_str(trimmed) {
            Ok(value) => value,
            Err(error) => {
                warnings.push(format!(
                    "runtime queue line {line_number} is not valid JSON: {error}"
                ));
                continue;
            }
        };
        let Some(queue_id) = string_field(&value, &["queueId", "queue_id"]) else {
            warnings.push(format!("runtime queue line {line_number} has no queue id"));
            continue;
        };
        if string_field(&value, &["status"]) != Some("queued") {
            warnings.push(format!(
                "runtime queue item `{queue_id}` is not queued; skipping"
            ));
            continue;
        }
        match parse_pending_item(&value) {
            Some(item) => items.push(item),
            None => warnings.push(format!(
                "runtime queue item `{queue_id}` is missing required fields"
            )),
        }
    }

    Ok(items)
}

fn read_prepared_receipts(
    receipts_file: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<HashMap<String, RuntimeExecutionReceipt>> {
    let mut receipts = HashMap::new();
    if !receipts_file.is_file() {
        return Ok(receipts);
    }

    let text = fs::read_to_string(receipts_file)?;
    for (index, line) in text.lines().enumerate() {
        let line_number = index + 1;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let receipt: RuntimeExecutionReceipt = match serde_json::from_str(trimmed) {
            Ok(receipt) => receipt,
            Err(error) => {
                warnings.push(format!(
                    "runtime execution receipt line {line_number} is not valid JSON: {error}"
                ));
                continue;
            }
        };
        if receipt.status == RuntimeExecutionReceiptStatus::Prepared && receipt.queue_id.is_some() {
            let queue_id = receipt.queue_id.clone().unwrap();
            receipts.insert(queue_id, receipt);
        }
    }
    Ok(receipts)
}

fn read_terminal_run_once_ids(
    receipts_file: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<HashSet<String>> {
    let mut ids = HashSet::new();
    if !receipts_file.is_file() {
        return Ok(ids);
    }

    let text = fs::read_to_string(receipts_file)?;
    for (index, line) in text.lines().enumerate() {
        let line_number = index + 1;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let value: Value = match serde_json::from_str(trimmed) {
            Ok(value) => value,
            Err(error) => {
                warnings.push(format!(
                    "runtime run-once receipt line {line_number} is not valid JSON: {error}"
                ));
                continue;
            }
        };
        if let Some(queue_id) = string_field(&value, &["queueId", "queue_id"])
            && let Some(status) = string_field(&value, &["status"])
            && is_terminal_run_once_status(status)
        {
            ids.insert(queue_id.to_string());
        }
    }
    Ok(ids)
}

fn is_terminal_run_once_status(status: &str) -> bool {
    matches!(
        status,
        "completed" | "failed-terminal" | "canceled" | "skipped" | "dead-letter"
    )
}

fn parse_pending_item(value: &Value) -> Option<PendingQueueItem> {
    let source = value.get("source")?;
    Some(PendingQueueItem {
        queue_id: string_field(value, &["queueId", "queue_id"])?.to_string(),
        agent_id: string_field(value, &["agentId", "agent_id"])?.to_string(),
        session_key: string_field(value, &["sessionKey", "session_key"])?.to_string(),
        platform: string_field(value, &["platform"])?.to_string(),
        channel_id: string_field(value, &["channelId", "channel_id"])?.to_string(),
        user_id: string_field(value, &["userId", "user_id"])?.to_string(),
        message_text: string_field(value, &["messageText", "message_text"])?.to_string(),
        inbound_context: string_field(value, &["inboundContext", "inbound_context"])
            .map(ToString::to_string),
        source_home: path_field(source, &["sourceHome", "source_home"])?,
        source_workspace: path_field(source, &["sourceWorkspace", "source_workspace"])?,
        runtime_workspace: path_field(source, &["runtimeWorkspace", "runtime_workspace"]),
        planned_transcript_file: path_field(
            value,
            &["plannedTranscriptFile", "planned_transcript_file"],
        )?,
        planned_trajectory_file: path_field(
            value,
            &["plannedTrajectoryFile", "planned_trajectory_file"],
        )?,
        selected_skill_ids: string_array_field(value, &["selectedSkillIds", "selected_skill_ids"]),
    })
}

fn queue_execution_dir(harness_home: &Path, queue_id: &str) -> PathBuf {
    harness_home
        .join("state")
        .join("runtime-queue")
        .join("executions")
        .join(normalize_key_part(queue_id))
}

fn append_json_line(path: &Path, value: &impl Serialize) -> io::Result<()> {
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    let line = serde_json::to_string(value).map_err(io::Error::other)?;
    writeln!(file, "{line}")?;
    Ok(())
}

fn string_field<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a str> {
    for key in keys {
        if let Some(text) = value.get(*key).and_then(Value::as_str) {
            return Some(text);
        }
    }
    None
}

fn path_field(value: &Value, keys: &[&str]) -> Option<PathBuf> {
    string_field(value, keys).map(PathBuf::from)
}

fn string_array_field(value: &Value, keys: &[&str]) -> Vec<String> {
    for key in keys {
        if let Some(array) = value.get(*key).and_then(Value::as_array) {
            return array
                .iter()
                .filter_map(Value::as_str)
                .map(ToString::to_string)
                .collect();
        }
    }
    Vec::new()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        RuntimeQueueEnqueueOptions, TurnPlanInput, build_channel_step, build_source_skill_index,
        build_turn_plan, enqueue_channel_step,
    };
    use std::io::Write;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn prepare_runtime_queue_item_writes_prompt_bundle_and_receipts() {
        let root = temp_root("prepare_runtime_queue_item_writes_prompt_bundle_and_receipts");
        let source = write_worker_source(&root);
        let harness_home = root.join(".agent-harness");
        enqueue_fixture_turn(&source, &harness_home);

        let report = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();

        assert_eq!(
            report.receipt.status,
            RuntimeExecutionReceiptStatus::Prepared
        );
        assert!(report.execution_receipts_file.is_file());
        let item = report.item.unwrap();
        assert_eq!(item.agent_id, "main");
        assert_eq!(item.provider.as_deref(), Some("openai"));
        assert_eq!(item.model.as_deref(), Some("gpt-5"));
        assert!(item.prompt_bundle_json.is_file());
        assert!(item.prompt_markdown.is_file());
        assert!(item.receipt_file.is_file());
        let bundle_json: Value =
            serde_json::from_slice(&fs::read(item.prompt_bundle_json).unwrap()).unwrap();
        assert_eq!(bundle_json["summary"]["userMessagesIncluded"], 1);
        assert_eq!(bundle_json["agentId"], "main");
        assert!(
            fs::read_to_string(item.prompt_markdown)
                .unwrap()
                .contains("repair memory cron")
        );
        let receipt_json: Value =
            serde_json::from_slice(&fs::read(item.receipt_file).unwrap()).unwrap();
        assert_eq!(receipt_json["status"], "prepared");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prepare_runtime_queue_item_uses_imported_prompt_workspace_when_runtime_workspace_drifts() {
        let root = temp_root(
            "prepare_runtime_queue_item_uses_imported_prompt_workspace_when_runtime_workspace_drifts",
        );
        let source = write_worker_source(&root);
        let harness_home = root.join(".agent-harness");
        let drift_workspace = root.join("runtime-workspace");
        fs::create_dir_all(&drift_workspace).unwrap();

        let registry = load_agent_registry(&source).unwrap();
        let skills = build_source_skill_index(&source).unwrap();
        let turn = build_turn_plan(
            &source,
            &registry,
            &skills,
            TurnPlanInput {
                harness_home: None,
                platform: "telegram".to_string(),
                channel_id: "dm-42".to_string(),
                user_id: "user-7".to_string(),
                text: "repair memory cron".to_string(),
                inbound_context: None,
                requested_agent_id: Some("main".to_string()),
                session_hint: None,
                skill_limit: 3,
            },
        )
        .unwrap();
        let mut step = build_channel_step(&registry, &turn);
        step.source_workspace = drift_workspace.clone();
        enqueue_channel_step(
            &step,
            RuntimeQueueEnqueueOptions {
                harness_home: harness_home.clone(),
                runtime_workspace: Some(drift_workspace.clone()),
                now_ms: 1234,
            },
        )
        .unwrap();

        let report = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();

        assert!(
            report
                .warnings
                .iter()
                .any(|warning| warning.contains("using imported prompt workspace"))
        );
        assert_eq!(
            report.receipt.runtime_workspace.as_deref(),
            Some(drift_workspace.as_path())
        );
        let item = report.item.unwrap();
        let bundle_json: Value =
            serde_json::from_slice(&fs::read(item.prompt_bundle_json).unwrap()).unwrap();
        assert_eq!(
            bundle_json["sourceWorkspace"].as_str(),
            Some(source.workspace.to_string_lossy().as_ref())
        );
        assert_eq!(bundle_json["summary"]["promptFilesIncluded"], 1);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prepare_runtime_queue_item_reports_no_pending_item() {
        let root = temp_root("prepare_runtime_queue_item_reports_no_pending_item");
        let harness_home = root.join(".agent-harness");

        let report = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home,
            queue_id: Some("missing".to_string()),
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();

        assert!(report.item.is_none());
        assert_eq!(
            report.receipt.status,
            RuntimeExecutionReceiptStatus::NoPendingItem
        );
        assert!(report.execution_receipts_file.is_file());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prepare_runtime_queue_item_respects_agent_channel_lease_limit() {
        let root = temp_root("prepare_runtime_queue_item_respects_agent_channel_lease_limit");
        let source = write_worker_source(&root);
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            r#"{"workerDispatch":{"globalConcurrencyLimit":2,"groupConcurrencyLimit":2,"channelConcurrencyLimit":1,"laneConcurrencyLimits":{"llm":2}}}"#,
        )
        .unwrap();
        enqueue_fixture_turn_with_text(&source, &harness_home, "first turn", 1234);
        enqueue_fixture_turn_with_text(&source, &harness_home, "second turn", 1235);

        let first = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();
        assert_eq!(
            first.receipt.status,
            RuntimeExecutionReceiptStatus::Prepared
        );
        let first_queue_id = first.receipt.queue_id.clone().unwrap();

        let blocked = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();
        assert_eq!(
            blocked.receipt.status,
            RuntimeExecutionReceiptStatus::NoPendingItem
        );
        assert!(
            blocked
                .warnings
                .iter()
                .any(|warning| warning.contains("agent-channel limit"))
        );

        let run_once_receipts = harness_home
            .join("state")
            .join("runtime-queue")
            .join("run-once-receipts.jsonl");
        let mut run_once_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&run_once_receipts)
            .unwrap();
        writeln!(
            run_once_file,
            "{}",
            serde_json::json!({
                "queueId": first_queue_id,
                "status": "completed",
                "reason": "test terminal receipt"
            })
        )
        .unwrap();
        release_runtime_queue_lease(&harness_home, &first_queue_id).unwrap();
        let second = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();
        assert_eq!(
            second.receipt.status,
            RuntimeExecutionReceiptStatus::Prepared
        );
        assert_ne!(second.receipt.queue_id, Some(first_queue_id));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn inspect_runtime_queue_capacity_returns_channel_aware_claimable_ids() {
        let root = temp_root("inspect_runtime_queue_capacity_returns_channel_aware_claimable_ids");
        let source = write_worker_source(&root);
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            r#"{"workerDispatch":{"globalConcurrencyLimit":3,"groupConcurrencyLimit":3,"channelConcurrencyLimit":1,"laneConcurrencyLimits":{"llm":3}}}"#,
        )
        .unwrap();
        enqueue_fixture_turn_with_platform_channel(
            &source,
            &harness_home,
            "telegram",
            "tg-dm",
            "user-7",
            "first tg",
            1234,
        );
        enqueue_fixture_turn_with_platform_channel(
            &source,
            &harness_home,
            "telegram",
            "tg-dm",
            "user-7",
            "second tg",
            1235,
        );
        enqueue_fixture_turn_with_platform_channel(
            &source,
            &harness_home,
            "discord",
            "discord-dm",
            "user-7",
            "first discord",
            1236,
        );

        let capacity = inspect_runtime_queue_capacity(RuntimeQueueCapacityOptions {
            harness_home: harness_home.clone(),
        })
        .unwrap();

        assert_eq!(capacity.claimable_items, 2);
        assert_eq!(capacity.claimable_queue_ids.len(), 2);
        assert!(
            capacity
                .claimable_queue_ids
                .iter()
                .any(|queue_id| queue_id.contains(":telegram:tg-dm:"))
        );
        assert!(
            capacity
                .claimable_queue_ids
                .iter()
                .any(|queue_id| queue_id.contains(":discord:discord-dm:"))
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn inspect_runtime_queue_capacity_treats_busy_lease_lock_as_zero_capacity() {
        let root = temp_root("inspect_runtime_queue_capacity_treats_busy_lease_lock_as_zero");
        let harness_home = root.join(".agent-harness");
        let queue_dir = harness_home.join("state").join("runtime-queue");
        fs::create_dir_all(&queue_dir).unwrap();
        let now_ms = current_log_time_ms().unwrap();
        let _held_lock =
            create_runtime_queue_lease_lock(&runtime_queue_lease_lock_file(&queue_dir), now_ms)
                .unwrap();

        let report = inspect_runtime_queue_capacity(RuntimeQueueCapacityOptions {
            harness_home: harness_home.clone(),
        })
        .unwrap();

        assert_eq!(report.claimable_items, 0);
        assert!(report.claimable_queue_ids.is_empty());
        assert!(
            report
                .warnings
                .iter()
                .any(|warning| warning.contains("lease lock is busy"))
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prepare_runtime_queue_item_reports_lease_busy_when_lock_busy() {
        let root = temp_root("prepare_runtime_queue_item_reports_lease_busy_when_lock_busy");
        let harness_home = root.join(".agent-harness");
        let queue_dir = harness_home.join("state").join("runtime-queue");
        fs::create_dir_all(&queue_dir).unwrap();
        let now_ms = current_log_time_ms().unwrap();
        let _held_lock =
            create_runtime_queue_lease_lock(&runtime_queue_lease_lock_file(&queue_dir), now_ms)
                .unwrap();

        let report = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: Some("turn:busy".to_string()),
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();

        assert!(report.item.is_none());
        assert_eq!(
            report.receipt.status,
            RuntimeExecutionReceiptStatus::LeaseBusy
        );
        assert_eq!(report.receipt.queue_id.as_deref(), Some("turn:busy"));
        let receipts = fs::read_to_string(report.execution_receipts_file).unwrap();
        assert!(receipts.contains("\"status\":\"lease-busy\""));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prepare_runtime_queue_item_is_idempotent_for_prepared_items() {
        let root = temp_root("prepare_runtime_queue_item_is_idempotent_for_prepared_items");
        let source = write_worker_source(&root);
        let harness_home = root.join(".agent-harness");
        enqueue_fixture_turn(&source, &harness_home);

        let first = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();
        let queue_id = first.receipt.queue_id.clone().unwrap();
        assert_eq!(
            first.receipt.status,
            RuntimeExecutionReceiptStatus::Prepared
        );

        let second = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();
        assert!(second.item.is_none());
        assert_eq!(
            second.receipt.status,
            RuntimeExecutionReceiptStatus::NoPendingItem
        );
        assert_eq!(second.receipt.queue_id, None);

        release_runtime_queue_lease(&harness_home, &queue_id).unwrap();
        let resumed = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();
        assert!(resumed.item.is_none());
        assert_eq!(
            resumed.receipt.status,
            RuntimeExecutionReceiptStatus::AlreadyPrepared
        );
        assert_eq!(resumed.receipt.queue_id.as_deref(), Some(queue_id.as_str()));
        release_runtime_queue_lease(&harness_home, &queue_id).unwrap();

        let run_once_receipts = harness_home
            .join("state")
            .join("runtime-queue")
            .join("run-once-receipts.jsonl");
        let mut run_once_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&run_once_receipts)
            .unwrap();
        writeln!(
            run_once_file,
            "{}",
            serde_json::json!({
                "queueId": queue_id,
                "status": "timeout",
                "reason": "test retryable receipt"
            })
        )
        .unwrap();

        let after_timeout = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();
        assert!(after_timeout.item.is_none());
        assert_eq!(
            after_timeout.receipt.status,
            RuntimeExecutionReceiptStatus::AlreadyPrepared
        );
        assert_eq!(
            after_timeout.receipt.queue_id.as_deref(),
            Some(queue_id.as_str())
        );

        writeln!(
            run_once_file,
            "{}",
            serde_json::json!({
                "queueId": queue_id,
                "status": "completed",
                "reason": "test terminal receipt"
            })
        )
        .unwrap();

        let explicit = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: resumed.receipt.queue_id.clone(),
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();
        assert!(explicit.item.is_none());
        assert_eq!(
            explicit.receipt.status,
            RuntimeExecutionReceiptStatus::AlreadyPrepared
        );
        assert!(explicit.receipt.execution_dir.is_some());
        assert!(explicit.receipt.prompt_bundle_json.is_some());

        let after_terminal = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home,
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();
        assert!(after_terminal.item.is_none());
        assert_eq!(
            after_terminal.receipt.status,
            RuntimeExecutionReceiptStatus::NoPendingItem
        );

        let _ = fs::remove_dir_all(root);
    }

    fn enqueue_fixture_turn(source: &AgentSource, harness_home: &Path) {
        enqueue_fixture_turn_with_text(source, harness_home, "repair memory cron", 1234);
    }

    fn enqueue_fixture_turn_with_text(
        source: &AgentSource,
        harness_home: &Path,
        text: &str,
        now_ms: i64,
    ) {
        enqueue_fixture_turn_with_platform_channel(
            source,
            harness_home,
            "telegram",
            "dm-42",
            "user-7",
            text,
            now_ms,
        );
    }

    fn enqueue_fixture_turn_with_platform_channel(
        source: &AgentSource,
        harness_home: &Path,
        platform: &str,
        channel_id: &str,
        user_id: &str,
        text: &str,
        now_ms: i64,
    ) {
        let registry = load_agent_registry(source).unwrap();
        let skills = build_source_skill_index(source).unwrap();
        let turn = build_turn_plan(
            source,
            &registry,
            &skills,
            TurnPlanInput {
                harness_home: None,
                platform: platform.to_string(),
                channel_id: channel_id.to_string(),
                user_id: user_id.to_string(),
                text: text.to_string(),
                inbound_context: None,
                requested_agent_id: Some("main".to_string()),
                session_hint: None,
                skill_limit: 3,
            },
        )
        .unwrap();
        let step = build_channel_step(&registry, &turn);
        enqueue_channel_step(
            &step,
            RuntimeQueueEnqueueOptions {
                harness_home: harness_home.to_path_buf(),
                runtime_workspace: None,
                now_ms,
            },
        )
        .unwrap();
    }

    fn write_worker_source(root: &Path) -> AgentSource {
        let home = root.join(".openclaw");
        let workspace = home.join("workspace");
        let skill = workspace.join("skills").join("memory-cron");
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(&skill).unwrap();
        fs::create_dir_all(home.join("agents").join("main").join("sessions")).unwrap();
        fs::write(workspace.join("AGENTS.md"), "# Agent").unwrap();
        fs::write(
            skill.join(crate::SKILL_FILE_NAME),
            "# Memory Cron\n\nRepair openclaw-mem cron jobs.",
        )
        .unwrap();
        fs::write(
            home.join("openclaw.json"),
            r#"{
              "agents": {
                "defaults": { "provider": "openai", "model": "codex" },
                "list": [
                  { "id": "main", "model": "gpt-5", "enabled": true }
                ]
              },
              "models": {
                "providers": {
                  "openai": { "apiKey": "${OPENAI_API_KEY}" }
                }
              }
            }"#,
        )
        .unwrap();
        fs::write(
            home.join("agents")
                .join("main")
                .join("sessions")
                .join("sessions.json"),
            "{}",
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
            "agent-harness-runtime-worker-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
