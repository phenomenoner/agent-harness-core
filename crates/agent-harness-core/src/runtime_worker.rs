use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::thread;
#[cfg(not(windows))]
use std::time::SystemTime;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::Value;
#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt;

use crate::{
    AgentSource, HarnessLogEvent, HarnessLogLevel, PromptAssemblyOptions, append_harness_log,
    assemble_prompt_bundle, build_runtime_skill_index, build_turn_plan,
    cron_run_runtime_dispatch_blocker, current_log_time_ms, load_agent_registry,
    load_worker_dispatch_config, write_json_atomic, write_prompt_bundle,
};

const RUNTIME_QUEUE_PREPARE_REPORT_SCHEMA: &str = "agent-harness.runtime-queue-prepare.v1";
const RUNTIME_QUEUE_LEASES_SCHEMA: &str = "agent-harness.runtime-queue-leases.v1";
const DEFAULT_RUNTIME_LEASE_MS: i64 = 30 * 60 * 1000;
const RUNTIME_LEASE_ACQUIRE_LOCK_RETRY_MS: u64 = 2_000;
const RUNTIME_LEASE_RELEASE_LOCK_RETRY_MS: u64 = 2_000;
const RUNTIME_LEASE_RELEASE_LOCK_RETRY_SLEEP_MS: u64 = 25;
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
    pub classes: Vec<RuntimeQueueClassCapacity>,
    pub claimable_items: usize,
    pub claimable_queue_ids: Vec<String>,
    pub leased_items: usize,
    pub global_limit: usize,
    pub agent_limit: usize,
    pub agent_channel_limit: usize,
    pub session_limit: usize,
    pub lease_lock_busy: bool,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeDispatchConfig {
    pub global_concurrency_limit: usize,
    pub interactive_reserve: usize,
    pub classes: BTreeMap<String, RuntimeDispatchClassConfig>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeDispatchClassConfig {
    pub max_active: usize,
    pub per_agent_max_active: usize,
    pub per_channel_max_active: usize,
    pub per_session_max_active: usize,
    pub session_fifo: bool,
    pub same_session_main_agent_serialization: bool,
    pub per_job_max_active: usize,
    pub max_queued_per_agent: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeQueueClassCapacity {
    pub runtime_class: String,
    pub leases_file: PathBuf,
    pub lock_file: PathBuf,
    pub leased_items: usize,
    pub claimable_items: usize,
    pub lock_busy: bool,
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
    pub runtime_class: String,
    pub origin: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cron_run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scheduled_for_ms: Option<i64>,
    pub platform: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account_id: Option<String>,
    pub channel_id: String,
    pub user_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inbound_context: Option<String>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_class: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cron_run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scheduled_for_ms: Option<i64>,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct RuntimeRunOnceSkipReceipt {
    schema: &'static str,
    queue_id: Option<String>,
    status: &'static str,
    runtime_class: Option<String>,
    origin: Option<String>,
    cron_run_id: Option<String>,
    scheduled_for_ms: Option<i64>,
    execution_dir: Option<PathBuf>,
    transcript_file: Option<PathBuf>,
    outbox_file: Option<PathBuf>,
    reason: String,
}

#[derive(Debug, Clone)]
struct PendingQueueItem {
    queue_id: String,
    created_at_ms: i64,
    agent_id: String,
    session_key: String,
    runtime_class: String,
    origin: String,
    cron_run_id: Option<String>,
    scheduled_for_ms: Option<i64>,
    platform: String,
    account_id: Option<String>,
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
    #[serde(default = "default_interactive_runtime_class")]
    runtime_class: String,
    #[serde(default = "default_channel_origin")]
    origin: String,
    #[serde(default)]
    cron_run_id: Option<String>,
    platform: String,
    #[serde(default)]
    account_id: Option<String>,
    channel_id: String,
    #[serde(default)]
    user_id: Option<String>,
    session_key: String,
    #[serde(default)]
    session_lane_key: Option<String>,
    owner: String,
    started_at_ms: i64,
    lease_expires_at_ms: i64,
}

struct RuntimeQueueLeaseLock {
    path: PathBuf,
    file: Option<fs::File>,
}

impl Drop for RuntimeQueueLeaseLock {
    fn drop(&mut self) {
        let _ = self.file.take();
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
    let preliminary_pending_items = read_pending_items(&queue_file, &mut warnings)?;
    let prepared_receipts = read_prepared_receipts(&execution_receipts_file, &mut warnings)?;
    let run_once_receipts_file = queue_dir.join("run-once-receipts.jsonl");
    let terminal_run_ids = read_terminal_run_once_ids(&run_once_receipts_file, &mut warnings)?;
    let retry_pending_run_ids =
        read_retry_pending_run_once_ids(&run_once_receipts_file, &mut warnings)?;
    let lock_runtime_class = select_lock_runtime_class(
        options.queue_id.as_deref(),
        &preliminary_pending_items,
        &prepared_receipts,
        &terminal_run_ids,
    );
    let lease_owner = format!("pid:{}", std::process::id());
    let _lease_lock = match acquire_runtime_queue_lease_lock_with_retry(
        &queue_dir,
        &lock_runtime_class,
        Duration::from_millis(RUNTIME_LEASE_ACQUIRE_LOCK_RETRY_MS),
    )? {
        Some(lock) => lock,
        None => {
            let receipt = RuntimeExecutionReceipt {
                queue_id: options.queue_id,
                status: RuntimeExecutionReceiptStatus::LeaseBusy,
                runtime_class: Some(lock_runtime_class.clone()),
                origin: None,
                cron_run_id: None,
                scheduled_for_ms: None,
                execution_dir: None,
                prompt_bundle_json: None,
                prompt_markdown: None,
                runtime_workspace: None,
                reason: format!(
                    "runtime queue lease lock is busy for class `{lock_runtime_class}`"
                ),
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
    let mut lease_state =
        read_runtime_queue_leases(&queue_dir, &lock_runtime_class, &mut warnings)?;
    purge_runtime_queue_leases(
        &mut lease_state,
        now_ms,
        &terminal_run_ids,
        &retry_pending_run_ids,
    );
    let pending_items = read_pending_items(&queue_file, &mut warnings)?;
    let pending_by_id = pending_items
        .iter()
        .cloned()
        .map(|item| (item.queue_id.clone(), item))
        .collect::<HashMap<_, _>>();
    if let Some(requested_queue_id) = options.queue_id.as_deref()
        && terminal_run_ids.contains(requested_queue_id)
    {
        write_runtime_queue_leases(&queue_dir, &lock_runtime_class, &lease_state)?;
        let receipt = RuntimeExecutionReceipt {
            queue_id: Some(requested_queue_id.to_string()),
            status: RuntimeExecutionReceiptStatus::NoPendingItem,
            runtime_class: pending_by_id
                .get(requested_queue_id)
                .map(|item| item.runtime_class.clone()),
            origin: pending_by_id
                .get(requested_queue_id)
                .map(|item| item.origin.clone()),
            cron_run_id: pending_by_id
                .get(requested_queue_id)
                .and_then(|item| item.cron_run_id.clone()),
            scheduled_for_ms: pending_by_id
                .get(requested_queue_id)
                .and_then(|item| item.scheduled_for_ms),
            execution_dir: None,
            prompt_bundle_json: None,
            prompt_markdown: None,
            runtime_workspace: None,
            reason: "requested runtime queue item already has a terminal run receipt".to_string(),
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
    if let Some(requested_queue_id) = options.queue_id.as_deref()
        && let Some(prepared) = prepared_receipts.get(requested_queue_id)
    {
        if lease_state.leases.contains_key(requested_queue_id) {
            let receipt = RuntimeExecutionReceipt {
                queue_id: Some(requested_queue_id.to_string()),
                status: RuntimeExecutionReceiptStatus::NoPendingItem,
                runtime_class: prepared.runtime_class.clone(),
                origin: prepared.origin.clone(),
                cron_run_id: prepared.cron_run_id.clone(),
                scheduled_for_ms: prepared.scheduled_for_ms,
                execution_dir: None,
                prompt_bundle_json: None,
                prompt_markdown: None,
                runtime_workspace: None,
                reason: "requested runtime queue item is already leased".to_string(),
            };
            write_runtime_queue_leases(&queue_dir, &lock_runtime_class, &lease_state)?;
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
                cron_runtime_dispatch_blocker_for_item(&options.harness_home, pending, now_ms)?
            {
                tombstone_runtime_queue_item_skipped(&queue_dir, pending, &blocker)?;
                let receipt = RuntimeExecutionReceipt {
                    queue_id: Some(requested_queue_id.to_string()),
                    status: RuntimeExecutionReceiptStatus::NoPendingItem,
                    runtime_class: Some(pending.runtime_class.clone()),
                    origin: Some(pending.origin.clone()),
                    cron_run_id: pending.cron_run_id.clone(),
                    scheduled_for_ms: pending.scheduled_for_ms,
                    execution_dir: None,
                    prompt_bundle_json: None,
                    prompt_markdown: None,
                    runtime_workspace: None,
                    reason: format!("runtime queue item blocked by {blocker}"),
                };
                write_runtime_queue_leases(&queue_dir, &lock_runtime_class, &lease_state)?;
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
            if let Some(blocker) =
                runtime_capacity_blocker(&options.harness_home, &lease_state, pending)?
            {
                let receipt = RuntimeExecutionReceipt {
                    queue_id: Some(requested_queue_id.to_string()),
                    status: RuntimeExecutionReceiptStatus::NoPendingItem,
                    runtime_class: Some(pending.runtime_class.clone()),
                    origin: Some(pending.origin.clone()),
                    cron_run_id: pending.cron_run_id.clone(),
                    scheduled_for_ms: pending.scheduled_for_ms,
                    execution_dir: None,
                    prompt_bundle_json: None,
                    prompt_markdown: None,
                    runtime_workspace: None,
                    reason: format!("runtime queue capacity blocked by {blocker}"),
                };
                write_runtime_queue_leases(&queue_dir, &lock_runtime_class, &lease_state)?;
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
            if let Some(blocker) = same_session_fifo_blocker(
                &pending_items,
                pending,
                &terminal_run_ids,
                &load_runtime_dispatch_config(&options.harness_home)?,
            ) {
                let receipt = RuntimeExecutionReceipt {
                    queue_id: Some(requested_queue_id.to_string()),
                    status: RuntimeExecutionReceiptStatus::NoPendingItem,
                    runtime_class: Some(pending.runtime_class.clone()),
                    origin: Some(pending.origin.clone()),
                    cron_run_id: pending.cron_run_id.clone(),
                    scheduled_for_ms: pending.scheduled_for_ms,
                    execution_dir: None,
                    prompt_bundle_json: None,
                    prompt_markdown: None,
                    runtime_workspace: None,
                    reason: format!("runtime queue item blocked by {blocker}"),
                };
                write_runtime_queue_leases(&queue_dir, &lock_runtime_class, &lease_state)?;
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
        write_runtime_queue_leases(&queue_dir, &lock_runtime_class, &lease_state)?;
        let receipt = RuntimeExecutionReceipt {
            queue_id: Some(requested_queue_id.to_string()),
            status: RuntimeExecutionReceiptStatus::AlreadyPrepared,
            runtime_class: prepared.runtime_class.clone(),
            origin: prepared.origin.clone(),
            cron_run_id: prepared.cron_run_id.clone(),
            scheduled_for_ms: prepared.scheduled_for_ms,
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
                if pending.runtime_class != lock_runtime_class {
                    continue;
                }
                if let Some(blocker) =
                    cron_runtime_dispatch_blocker_for_item(&options.harness_home, pending, now_ms)?
                {
                    warnings.push(format!(
                        "prepared runtime queue item `{queue_id}` blocked by {blocker}; tombstoning"
                    ));
                    tombstone_runtime_queue_item_skipped(&queue_dir, pending, &blocker)?;
                    continue;
                }
                if let Some(blocker) =
                    runtime_capacity_blocker(&options.harness_home, &lease_state, pending)?
                {
                    warnings.push(format!(
                    "prepared runtime queue item `{queue_id}` blocked by {blocker}; checking queued items"
                ));
                } else {
                    if let Some(blocker) = same_session_fifo_blocker(
                        &pending_items,
                        pending,
                        &terminal_run_ids,
                        &load_runtime_dispatch_config(&options.harness_home)?,
                    ) {
                        warnings.push(format!(
                            "prepared runtime queue item `{queue_id}` blocked by {blocker}; checking queued items"
                        ));
                        continue;
                    }
                    lease_runtime_queue_item(&mut lease_state, pending, &lease_owner, now_ms);
                    write_runtime_queue_leases(&queue_dir, &lock_runtime_class, &lease_state)?;
                    let receipt = RuntimeExecutionReceipt {
                    queue_id: Some(queue_id.clone()),
                    status: RuntimeExecutionReceiptStatus::AlreadyPrepared,
                    runtime_class: prepared.runtime_class.clone(),
                    origin: prepared.origin.clone(),
                    cron_run_id: prepared.cron_run_id.clone(),
                    scheduled_for_ms: prepared.scheduled_for_ms,
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
        &terminal_run_ids,
        &lease_state,
        &lock_runtime_class,
        &options.harness_home,
        &queue_dir,
        now_ms,
        &mut warnings,
    )?
    else {
        write_runtime_queue_leases(&queue_dir, &lock_runtime_class, &lease_state)?;
        let receipt = RuntimeExecutionReceipt {
            queue_id: options.queue_id,
            status: RuntimeExecutionReceiptStatus::NoPendingItem,
            runtime_class: None,
            origin: None,
            cron_run_id: None,
            scheduled_for_ms: None,
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
    write_runtime_queue_leases(&queue_dir, &lock_runtime_class, &lease_state)?;

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
    let receipt_runtime_class = pending.runtime_class.clone();
    let receipt_origin = pending.origin.clone();
    let receipt_cron_run_id = pending.cron_run_id.clone();
    let receipt_scheduled_for_ms = pending.scheduled_for_ms;
    let item = RuntimeQueuePreparedItem {
        queue_id: pending.queue_id.clone(),
        agent_id: pending.agent_id.clone(),
        session_key: pending.session_key.clone(),
        runtime_class: pending.runtime_class.clone(),
        origin: pending.origin.clone(),
        cron_run_id: pending.cron_run_id.clone(),
        scheduled_for_ms: pending.scheduled_for_ms,
        platform: pending.platform.clone(),
        account_id: pending.account_id.clone(),
        channel_id: pending.channel_id.clone(),
        user_id: pending.user_id.clone(),
        inbound_context: pending.inbound_context.clone(),
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
        runtime_class: Some(receipt_runtime_class),
        origin: Some(receipt_origin),
        cron_run_id: receipt_cron_run_id,
        scheduled_for_ms: receipt_scheduled_for_ms,
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

    let config = load_runtime_dispatch_config(&options.harness_home)?;
    let mut warnings = Vec::new();
    let now_ms = current_log_time_ms()?;
    let prepared_receipts = read_prepared_receipts(&execution_receipts_file, &mut warnings)?;
    let run_once_receipts_file = queue_dir.join("run-once-receipts.jsonl");
    let terminal_run_ids = read_terminal_run_once_ids(&run_once_receipts_file, &mut warnings)?;
    let retry_pending_run_ids =
        read_retry_pending_run_once_ids(&run_once_receipts_file, &mut warnings)?;
    let pending_items = read_pending_items(&queue_file, &mut warnings)?;
    let pending_by_id = pending_items
        .iter()
        .cloned()
        .map(|item| (item.queue_id.clone(), item))
        .collect::<HashMap<_, _>>();
    let mut claimable_items = 0usize;
    let mut claimable_queue_ids = Vec::new();
    let mut leased_items = 0usize;
    let mut classes = Vec::new();
    let mut any_lock_busy = false;
    let runtime_classes = runtime_classes_for_capacity(&pending_items);
    for runtime_class in runtime_classes {
        let Some(_lease_lock) = acquire_runtime_queue_lease_lock_with_retry(
            &queue_dir,
            &runtime_class,
            Duration::from_millis(RUNTIME_LEASE_ACQUIRE_LOCK_RETRY_MS),
        )?
        else {
            any_lock_busy = true;
            warnings.push(format!(
                "runtime queue lease lock is busy for class `{runtime_class}`; class capacity assumed zero"
            ));
            classes.push(RuntimeQueueClassCapacity {
                leases_file: runtime_queue_leases_file(&queue_dir, &runtime_class),
                lock_file: runtime_queue_lease_lock_file(&queue_dir, &runtime_class),
                runtime_class,
                leased_items: 0,
                claimable_items: 0,
                lock_busy: true,
            });
            continue;
        };
        let mut lease_state = read_runtime_queue_leases(&queue_dir, &runtime_class, &mut warnings)?;
        purge_runtime_queue_leases(
            &mut lease_state,
            now_ms,
            &terminal_run_ids,
            &retry_pending_run_ids,
        );
        write_runtime_queue_leases(&queue_dir, &runtime_class, &lease_state)?;
        leased_items += lease_state.leases.len();
        let mut simulated = lease_state.clone();
        let mut class_claimable = 0usize;
        let prepared_candidates = prepared_receipts
            .keys()
            .filter(|queue_id| {
                !terminal_run_ids.contains(*queue_id) && !lease_state.leases.contains_key(*queue_id)
            })
            .cloned()
            .collect::<Vec<_>>();
        for queue_id in prepared_candidates {
            let Some(pending) = pending_by_id.get(&queue_id) else {
                continue;
            };
            if pending.runtime_class != runtime_class {
                continue;
            }
            if let Some(blocker) =
                same_session_fifo_blocker(&pending_items, pending, &terminal_run_ids, &config)
            {
                warnings.push(format!(
                    "prepared runtime queue item `{queue_id}` blocked by {blocker}; capacity excludes it"
                ));
                continue;
            }
            if let Some(blocker) =
                cron_runtime_dispatch_blocker_for_item(&options.harness_home, pending, now_ms)?
            {
                warnings.push(format!(
                    "prepared runtime queue item `{queue_id}` blocked by {blocker}; capacity excludes it"
                ));
                continue;
            }
            if let Some(blocker) =
                runtime_capacity_blocker(&options.harness_home, &simulated, pending)?
            {
                warnings.push(format!(
                    "prepared runtime queue item `{queue_id}` blocked by {blocker}; capacity excludes it"
                ));
                continue;
            }
            claimable_items += 1;
            class_claimable += 1;
            claimable_queue_ids.push(queue_id);
            lease_runtime_queue_item(&mut simulated, pending, "capacity-inspect", now_ms);
        }

        let prepared_ids = prepared_receipts.keys().cloned().collect::<HashSet<_>>();
        for pending in pending_items
            .iter()
            .filter(|pending| pending.runtime_class == runtime_class)
        {
            if prepared_ids.contains(&pending.queue_id)
                || terminal_run_ids.contains(&pending.queue_id)
                || simulated.leases.contains_key(&pending.queue_id)
            {
                continue;
            }
            if let Some(blocker) =
                cron_runtime_dispatch_blocker_for_item(&options.harness_home, pending, now_ms)?
            {
                warnings.push(format!(
                    "runtime queue item `{}` blocked by {}; capacity excludes it",
                    pending.queue_id, blocker
                ));
                continue;
            }
            if let Some(blocker) =
                same_session_fifo_blocker(&pending_items, pending, &terminal_run_ids, &config)
            {
                warnings.push(format!(
                    "runtime queue item `{}` blocked by {}; capacity excludes it",
                    pending.queue_id, blocker
                ));
                continue;
            }
            if let Some(blocker) =
                runtime_capacity_blocker(&options.harness_home, &simulated, pending)?
            {
                warnings.push(format!(
                    "runtime queue item `{}` blocked by {}; capacity excludes it",
                    pending.queue_id, blocker
                ));
                continue;
            }
            claimable_items += 1;
            class_claimable += 1;
            claimable_queue_ids.push(pending.queue_id.clone());
            lease_runtime_queue_item(&mut simulated, pending, "capacity-inspect", now_ms);
        }
        classes.push(RuntimeQueueClassCapacity {
            leases_file: runtime_queue_leases_file(&queue_dir, &runtime_class),
            lock_file: runtime_queue_lease_lock_file(&queue_dir, &runtime_class),
            runtime_class,
            leased_items: lease_state.leases.len(),
            claimable_items: class_claimable,
            lock_busy: false,
        });
    }

    Ok(RuntimeQueueCapacityReport {
        schema: "agent-harness.runtime-queue-capacity.v1",
        harness_home: options.harness_home,
        queue_file,
        leases_file: runtime_queue_leases_file(&queue_dir, "interactive"),
        classes,
        claimable_items,
        claimable_queue_ids,
        leased_items,
        global_limit: config.global_concurrency_limit,
        agent_limit: config
            .classes
            .get("interactive")
            .map(|class| class.per_agent_max_active)
            .unwrap_or(config.global_concurrency_limit),
        agent_channel_limit: config
            .classes
            .get("interactive")
            .map(|class| class.per_channel_max_active)
            .unwrap_or(config.global_concurrency_limit),
        session_limit: config
            .classes
            .get("interactive")
            .map(|class| class.per_session_max_active)
            .unwrap_or(1),
        lease_lock_busy: any_lock_busy,
        warnings,
    })
}

pub fn load_runtime_dispatch_config(
    harness_home: impl AsRef<Path>,
) -> io::Result<RuntimeDispatchConfig> {
    let harness_home = harness_home.as_ref();
    let worker = load_worker_dispatch_config(harness_home)?;
    let mut config = RuntimeDispatchConfig {
        global_concurrency_limit: worker.global_concurrency_limit,
        interactive_reserve: worker.global_concurrency_limit.min(2),
        classes: BTreeMap::from([
            (
                "interactive".to_string(),
                RuntimeDispatchClassConfig {
                    max_active: worker.global_concurrency_limit,
                    per_agent_max_active: worker.group_concurrency_limit,
                    per_channel_max_active: worker.channel_concurrency_limit,
                    per_session_max_active: 1,
                    session_fifo: true,
                    same_session_main_agent_serialization: true,
                    per_job_max_active: usize::MAX,
                    max_queued_per_agent: usize::MAX,
                },
            ),
            (
                "cron".to_string(),
                RuntimeDispatchClassConfig {
                    max_active: worker.global_concurrency_limit.min(4),
                    per_agent_max_active: 1,
                    per_channel_max_active: 1,
                    per_session_max_active: 1,
                    session_fifo: true,
                    same_session_main_agent_serialization: false,
                    per_job_max_active: 1,
                    max_queued_per_agent: 20,
                },
            ),
            (
                "worker".to_string(),
                RuntimeDispatchClassConfig {
                    max_active: worker.global_concurrency_limit.min(2),
                    per_agent_max_active: worker.group_concurrency_limit.min(2),
                    per_channel_max_active: worker.channel_concurrency_limit.min(2),
                    per_session_max_active: usize::MAX,
                    session_fifo: false,
                    same_session_main_agent_serialization: false,
                    per_job_max_active: usize::MAX,
                    max_queued_per_agent: usize::MAX,
                },
            ),
            (
                "maintenance".to_string(),
                RuntimeDispatchClassConfig {
                    max_active: worker.global_concurrency_limit.min(1),
                    per_agent_max_active: 1,
                    per_channel_max_active: 1,
                    per_session_max_active: 1,
                    session_fifo: true,
                    same_session_main_agent_serialization: false,
                    per_job_max_active: usize::MAX,
                    max_queued_per_agent: usize::MAX,
                },
            ),
        ]),
        warnings: worker.warnings.clone(),
    };

    for path in crate::config::harness_config_candidates(harness_home) {
        if !path.is_file() {
            continue;
        }
        let text = fs::read_to_string(&path)?;
        let value = serde_json::from_str::<Value>(&text).map_err(io::Error::other)?;
        let Some(dispatch) = value.get("runtimeDispatch") else {
            continue;
        };
        if let Some(limit) = dispatch
            .get("globalConcurrencyLimit")
            .and_then(Value::as_u64)
            .and_then(|value| usize::try_from(value).ok())
        {
            config.global_concurrency_limit = limit;
        }
        if let Some(reserve) = dispatch
            .get("interactiveReserve")
            .or_else(|| dispatch.get("interactiveReserved"))
            .and_then(Value::as_u64)
            .and_then(|value| usize::try_from(value).ok())
        {
            config.interactive_reserve = reserve;
        }
        if let Some(classes) = dispatch.get("classes").and_then(Value::as_object) {
            for (runtime_class, class_value) in classes {
                let mut class_config = config.classes.get(runtime_class).cloned().unwrap_or(
                    RuntimeDispatchClassConfig {
                        max_active: config.global_concurrency_limit,
                        per_agent_max_active: config.global_concurrency_limit,
                        per_channel_max_active: config.global_concurrency_limit,
                        per_session_max_active: usize::MAX,
                        session_fifo: false,
                        same_session_main_agent_serialization: false,
                        per_job_max_active: usize::MAX,
                        max_queued_per_agent: usize::MAX,
                    },
                );
                if let Some(max_active) = class_value
                    .get("maxActive")
                    .and_then(Value::as_u64)
                    .and_then(|value| usize::try_from(value).ok())
                {
                    class_config.max_active = max_active;
                }
                if let Some(max_active) = class_value
                    .get("perAgentMaxActive")
                    .and_then(Value::as_u64)
                    .and_then(|value| usize::try_from(value).ok())
                {
                    class_config.per_agent_max_active = max_active;
                }
                if let Some(max_active) = class_value
                    .get("perChannelMaxActive")
                    .or_else(|| class_value.get("perAgentChannelMaxActive"))
                    .and_then(Value::as_u64)
                    .and_then(|value| usize::try_from(value).ok())
                {
                    class_config.per_channel_max_active = max_active;
                }
                if let Some(max_active) = class_value
                    .get("perSessionMaxActive")
                    .or_else(|| class_value.get("perSessionLaneMaxActive"))
                    .and_then(Value::as_u64)
                    .and_then(|value| usize::try_from(value).ok())
                {
                    class_config.per_session_max_active = max_active;
                }
                if let Some(session_fifo) = class_value.get("sessionFifo").and_then(Value::as_bool)
                {
                    class_config.session_fifo = session_fifo;
                }
                if let Some(enabled) = class_value
                    .get("sameSessionMainAgentSerialization")
                    .and_then(Value::as_bool)
                {
                    class_config.same_session_main_agent_serialization = enabled;
                }
                if let Some(max_active) = class_value
                    .get("perJobMaxActive")
                    .and_then(Value::as_u64)
                    .and_then(|value| usize::try_from(value).ok())
                {
                    class_config.per_job_max_active = max_active;
                }
                if let Some(max_queued) = class_value
                    .get("maxQueuedPerAgent")
                    .and_then(Value::as_u64)
                    .and_then(|value| usize::try_from(value).ok())
                {
                    class_config.max_queued_per_agent = max_queued;
                }
                config.classes.insert(runtime_class.clone(), class_config);
            }
        }
        break;
    }

    if config.interactive_reserve > config.global_concurrency_limit {
        config.warnings.push(format!(
            "runtimeDispatch.interactiveReserve ({}) exceeds globalConcurrencyLimit ({}); reserve is capped",
            config.interactive_reserve, config.global_concurrency_limit
        ));
        config.interactive_reserve = config.global_concurrency_limit;
    }
    Ok(config)
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

fn default_interactive_runtime_class() -> String {
    "interactive".to_string()
}

fn default_channel_origin() -> String {
    "channel".to_string()
}

fn select_lock_runtime_class(
    requested_queue_id: Option<&str>,
    pending_items: &[PendingQueueItem],
    prepared_receipts: &HashMap<String, RuntimeExecutionReceipt>,
    terminal_run_ids: &HashSet<String>,
) -> String {
    if let Some(requested_queue_id) = requested_queue_id
        && let Some(item) = pending_items
            .iter()
            .find(|item| item.queue_id == requested_queue_id)
    {
        return item.runtime_class.clone();
    }
    if let Some(requested_queue_id) = requested_queue_id
        && let Some(prepared) = prepared_receipts.get(requested_queue_id)
        && let Some(runtime_class) = prepared.runtime_class.as_ref()
    {
        return runtime_class.clone();
    }

    let prepared_ids = prepared_receipts.keys().cloned().collect::<HashSet<_>>();
    let mut prepared_candidates = pending_items
        .iter()
        .filter(|item| {
            prepared_ids.contains(&item.queue_id) && !terminal_run_ids.contains(&item.queue_id)
        })
        .collect::<Vec<_>>();
    prepared_candidates.sort_by(|left, right| {
        runtime_selection_key(left)
            .cmp(&runtime_selection_key(right))
            .then_with(|| left.queue_id.cmp(&right.queue_id))
    });
    if let Some(item) = prepared_candidates.first() {
        return item.runtime_class.clone();
    }

    let mut queued_candidates = pending_items
        .iter()
        .filter(|item| {
            !prepared_ids.contains(&item.queue_id) && !terminal_run_ids.contains(&item.queue_id)
        })
        .collect::<Vec<_>>();
    queued_candidates.sort_by(|left, right| {
        runtime_selection_key(left)
            .cmp(&runtime_selection_key(right))
            .then_with(|| left.queue_id.cmp(&right.queue_id))
    });
    if let Some(item) = queued_candidates.first() {
        return item.runtime_class.clone();
    }
    "interactive".to_string()
}

fn runtime_classes_for_capacity(pending_items: &[PendingQueueItem]) -> Vec<String> {
    let mut classes = vec!["interactive".to_string(), "cron".to_string()];
    for item in pending_items {
        if !classes.contains(&item.runtime_class) {
            classes.push(item.runtime_class.clone());
        }
    }
    classes
}

fn runtime_class_state_dir(queue_dir: &Path, runtime_class: &str) -> PathBuf {
    queue_dir
        .join("classes")
        .join(normalize_key_part(runtime_class))
}

fn runtime_queue_leases_file(queue_dir: &Path, runtime_class: &str) -> PathBuf {
    if runtime_class == "legacy" {
        return queue_dir.join("runtime-leases.json");
    }
    runtime_class_state_dir(queue_dir, runtime_class).join("runtime-leases.json")
}

fn runtime_queue_lease_lock_file(queue_dir: &Path, runtime_class: &str) -> PathBuf {
    if runtime_class == "legacy" {
        return queue_dir.join("runtime-leases.lock");
    }
    runtime_class_state_dir(queue_dir, runtime_class).join("runtime-leases.lock")
}

fn acquire_runtime_queue_lease_lock(
    queue_dir: &Path,
    runtime_class: &str,
    now_ms: i64,
) -> io::Result<Option<RuntimeQueueLeaseLock>> {
    let lock_file = runtime_queue_lease_lock_file(queue_dir, runtime_class);
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

fn acquire_runtime_queue_lease_lock_with_retry(
    queue_dir: &Path,
    runtime_class: &str,
    timeout: Duration,
) -> io::Result<Option<RuntimeQueueLeaseLock>> {
    let started = Instant::now();
    loop {
        let now_ms = current_log_time_ms()?;
        if let Some(lock) = acquire_runtime_queue_lease_lock(queue_dir, runtime_class, now_ms)? {
            return Ok(Some(lock));
        }
        if started.elapsed() >= timeout {
            return Ok(None);
        }
        thread::sleep(Duration::from_millis(
            RUNTIME_LEASE_RELEASE_LOCK_RETRY_SLEEP_MS,
        ));
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
    if let Some(parent) = lock_file.parent() {
        fs::create_dir_all(parent)?;
    }
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
        file: Some(file),
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
    runtime_class: &str,
    warnings: &mut Vec<String>,
) -> io::Result<RuntimeQueueLeaseState> {
    let leases_file = runtime_queue_leases_file(queue_dir, runtime_class);
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

fn write_runtime_queue_leases(
    queue_dir: &Path,
    runtime_class: &str,
    state: &RuntimeQueueLeaseState,
) -> io::Result<()> {
    write_json_atomic(&runtime_queue_leases_file(queue_dir, runtime_class), state)
}

fn purge_runtime_queue_leases(
    state: &mut RuntimeQueueLeaseState,
    now_ms: i64,
    terminal_run_ids: &HashSet<String>,
    retry_pending_run_ids: &HashSet<String>,
) {
    state.leases.retain(|queue_id, lease| {
        lease.lease_expires_at_ms > now_ms
            && !terminal_run_ids.contains(queue_id)
            && !retry_pending_run_ids.contains(queue_id)
    });
}

pub fn release_runtime_queue_lease(
    harness_home: impl AsRef<Path>,
    queue_id: &str,
) -> io::Result<()> {
    let queue_dir = harness_home.as_ref().join("state").join("runtime-queue");
    fs::create_dir_all(&queue_dir)?;
    let mut last_busy = None;
    for runtime_class in runtime_classes_for_release(&queue_dir) {
        let Some(_lease_lock) = acquire_runtime_queue_lease_lock_with_retry(
            &queue_dir,
            &runtime_class,
            Duration::from_millis(RUNTIME_LEASE_RELEASE_LOCK_RETRY_MS),
        )?
        else {
            last_busy = Some(runtime_class);
            continue;
        };
        let mut warnings = Vec::new();
        let mut state = read_runtime_queue_leases(&queue_dir, &runtime_class, &mut warnings)?;
        if state.leases.remove(queue_id).is_some() {
            write_runtime_queue_leases(&queue_dir, &runtime_class, &state)?;
            return Ok(());
        }
    }
    if let Some(runtime_class) = last_busy {
        return Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            format!(
                "runtime queue lease lock stayed busy for class `{runtime_class}` while releasing queue lease `{queue_id}`"
            ),
        ));
    }
    Ok(())
}

fn runtime_classes_for_release(queue_dir: &Path) -> Vec<String> {
    let mut classes = vec![
        "interactive".to_string(),
        "cron".to_string(),
        "worker".to_string(),
        "maintenance".to_string(),
    ];
    if queue_dir.join("runtime-leases.json").is_file() {
        classes.push("legacy".to_string());
    }
    let classes_dir = queue_dir.join("classes");
    if let Ok(entries) = fs::read_dir(classes_dir) {
        for entry in entries.flatten() {
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if !file_type.is_dir() {
                continue;
            }
            if let Some(name) = entry.file_name().to_str()
                && !classes.iter().any(|class| class == name)
            {
                classes.push(name.to_string());
            }
        }
    }
    classes
}

fn runtime_capacity_blocker(
    harness_home: &Path,
    state: &RuntimeQueueLeaseState,
    item: &PendingQueueItem,
) -> io::Result<Option<String>> {
    let config = load_runtime_dispatch_config(harness_home)?;
    let all_leases =
        read_all_runtime_queue_leases_with_override(harness_home, state, &item.runtime_class)?;
    let executing_global = all_leases.len();
    if executing_global >= config.global_concurrency_limit {
        return Ok(Some("global runtime limit".to_string()));
    }

    if item.runtime_class != "interactive" {
        let non_reserved_limit = config
            .global_concurrency_limit
            .saturating_sub(config.interactive_reserve);
        if executing_global >= non_reserved_limit {
            return Ok(Some("interactive reserve".to_string()));
        }
    }

    let class_config =
        config
            .classes
            .get(&item.runtime_class)
            .cloned()
            .unwrap_or(RuntimeDispatchClassConfig {
                max_active: config.global_concurrency_limit,
                per_agent_max_active: config.global_concurrency_limit,
                per_channel_max_active: config.global_concurrency_limit,
                per_session_max_active: usize::MAX,
                session_fifo: false,
                same_session_main_agent_serialization: false,
                per_job_max_active: usize::MAX,
                max_queued_per_agent: usize::MAX,
            });

    let executing_class = all_leases
        .iter()
        .filter(|lease| lease.runtime_class == item.runtime_class)
        .count();
    if executing_class >= class_config.max_active {
        return Ok(Some(format!(
            "runtime class `{}` limit",
            item.runtime_class
        )));
    }

    let executing_agent = all_leases
        .iter()
        .filter(|lease| {
            lease.runtime_class == item.runtime_class && lease.agent_id == item.agent_id
        })
        .count();
    if executing_agent >= class_config.per_agent_max_active {
        return Ok(Some(format!(
            "runtime class `{}` agent limit for `{}`",
            item.runtime_class, item.agent_id
        )));
    }

    if let Some(cron_run_id) = item.cron_run_id.as_deref() {
        let executing_job = all_leases
            .iter()
            .filter(|lease| lease.cron_run_id.as_deref() == Some(cron_run_id))
            .count();
        if executing_job >= class_config.per_job_max_active {
            return Ok(Some(format!("cron job active limit for `{cron_run_id}`")));
        }
    }

    let channel_key = runtime_channel_key(&item.agent_id, &item.platform, &item.channel_id);
    if let Some(session_lane_key) = item_session_lane_key(item, &class_config) {
        let executing_session = all_leases
            .iter()
            .filter(|lease| {
                lease_session_lane_key(lease, &class_config).as_deref()
                    == Some(session_lane_key.as_str())
            })
            .count();
        if executing_session >= class_config.per_session_max_active {
            return Ok(Some(format!(
                "session-active limit for `{session_lane_key}`"
            )));
        }
    }

    let executing_channel = all_leases
        .iter()
        .filter(|lease| {
            runtime_channel_key(&lease.agent_id, &lease.platform, &lease.channel_id) == channel_key
        })
        .count();
    if executing_channel >= class_config.per_channel_max_active {
        return Ok(Some(format!("agent-channel limit for `{}`", channel_key)));
    }

    Ok(None)
}

fn read_all_runtime_queue_leases_with_override(
    harness_home: &Path,
    override_state: &RuntimeQueueLeaseState,
    override_class: &str,
) -> io::Result<Vec<RuntimeQueueLease>> {
    let queue_dir = harness_home.join("state").join("runtime-queue");
    let mut classes = runtime_classes_for_release(&queue_dir);
    if !classes.iter().any(|class| class == override_class) {
        classes.push(override_class.to_string());
    }
    let mut warnings = Vec::new();
    let mut leases = Vec::new();
    let now_ms = current_log_time_ms().unwrap_or(0);
    for runtime_class in classes {
        if runtime_class == override_class {
            leases.extend(
                override_state
                    .leases
                    .values()
                    .filter(|lease| lease.lease_expires_at_ms > now_ms)
                    .cloned(),
            );
            continue;
        }
        let state = read_runtime_queue_leases(&queue_dir, &runtime_class, &mut warnings)?;
        leases.extend(
            state
                .leases
                .values()
                .filter(|lease| lease.lease_expires_at_ms > now_ms)
                .cloned(),
        );
    }
    Ok(leases)
}

fn lease_runtime_queue_item(
    state: &mut RuntimeQueueLeaseState,
    item: &PendingQueueItem,
    owner: &str,
    now_ms: i64,
) {
    let session_lane_key = default_runtime_class_config_for_item(item)
        .and_then(|class_config| item_session_lane_key(item, &class_config));
    state.leases.insert(
        item.queue_id.clone(),
        RuntimeQueueLease {
            queue_id: item.queue_id.clone(),
            agent_id: item.agent_id.clone(),
            runtime_class: item.runtime_class.clone(),
            origin: item.origin.clone(),
            cron_run_id: item.cron_run_id.clone(),
            platform: item.platform.clone(),
            account_id: item.account_id.clone(),
            channel_id: item.channel_id.clone(),
            user_id: Some(item.user_id.clone()),
            session_key: item.session_key.clone(),
            session_lane_key,
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

fn runtime_session_lane_key(
    runtime_class: &str,
    agent_id: &str,
    platform: &str,
    channel_id: &str,
    user_id: &str,
    session_key: &str,
) -> String {
    [
        normalize_key_part(runtime_class),
        normalize_key_part(agent_id),
        normalize_key_part(platform),
        normalize_key_part(channel_id),
        normalize_key_part(user_id),
        normalize_key_part(session_key),
    ]
    .join(":")
}

fn default_runtime_class_config_for_item(
    item: &PendingQueueItem,
) -> Option<RuntimeDispatchClassConfig> {
    Some(match item.runtime_class.as_str() {
        "interactive" => RuntimeDispatchClassConfig {
            max_active: usize::MAX,
            per_agent_max_active: usize::MAX,
            per_channel_max_active: usize::MAX,
            per_session_max_active: 1,
            session_fifo: true,
            same_session_main_agent_serialization: true,
            per_job_max_active: usize::MAX,
            max_queued_per_agent: usize::MAX,
        },
        _ => RuntimeDispatchClassConfig {
            max_active: usize::MAX,
            per_agent_max_active: usize::MAX,
            per_channel_max_active: usize::MAX,
            per_session_max_active: usize::MAX,
            session_fifo: false,
            same_session_main_agent_serialization: false,
            per_job_max_active: usize::MAX,
            max_queued_per_agent: usize::MAX,
        },
    })
}

fn is_interactive_channel_main_lane(
    runtime_class: &str,
    origin: &str,
    cron_run_id: Option<&str>,
    class_config: &RuntimeDispatchClassConfig,
) -> bool {
    class_config.same_session_main_agent_serialization
        && runtime_class == "interactive"
        && origin == "channel"
        && cron_run_id.is_none()
        && class_config.per_session_max_active > 0
}

fn item_session_lane_key(
    item: &PendingQueueItem,
    class_config: &RuntimeDispatchClassConfig,
) -> Option<String> {
    is_interactive_channel_main_lane(
        &item.runtime_class,
        &item.origin,
        item.cron_run_id.as_deref(),
        class_config,
    )
    .then(|| {
        runtime_session_lane_key(
            &item.runtime_class,
            &item.agent_id,
            &item.platform,
            &item.channel_id,
            &item.user_id,
            &item.session_key,
        )
    })
}

fn lease_session_lane_key(
    lease: &RuntimeQueueLease,
    class_config: &RuntimeDispatchClassConfig,
) -> Option<String> {
    if !is_interactive_channel_main_lane(
        &lease.runtime_class,
        &lease.origin,
        lease.cron_run_id.as_deref(),
        class_config,
    ) {
        return None;
    }
    if let Some(key) = lease
        .session_lane_key
        .as_ref()
        .filter(|key| !key.trim().is_empty())
    {
        return Some(key.clone());
    }
    Some(runtime_session_lane_key(
        &lease.runtime_class,
        &lease.agent_id,
        &lease.platform,
        &lease.channel_id,
        lease.user_id.as_deref().unwrap_or(""),
        &lease.session_key,
    ))
}

fn same_session_fifo_blocker(
    pending_items: &[PendingQueueItem],
    item: &PendingQueueItem,
    terminal_run_ids: &HashSet<String>,
    config: &RuntimeDispatchConfig,
) -> Option<String> {
    let class_config = config.classes.get(&item.runtime_class)?;
    if !class_config.session_fifo {
        return None;
    }
    let lane_key = item_session_lane_key(item, class_config)?;
    pending_items
        .iter()
        .filter(|candidate| candidate.queue_id != item.queue_id)
        .filter(|candidate| !terminal_run_ids.contains(&candidate.queue_id))
        .filter(|candidate| {
            candidate.created_at_ms < item.created_at_ms
                || (candidate.created_at_ms == item.created_at_ms
                    && candidate.queue_id < item.queue_id)
        })
        .find(|candidate| {
            config
                .classes
                .get(&candidate.runtime_class)
                .and_then(|candidate_config| item_session_lane_key(candidate, candidate_config))
                .as_deref()
                == Some(lane_key.as_str())
        })
        .map(|candidate| {
            format!(
                "session-fifo for `{lane_key}` waiting on older queue item `{}`",
                candidate.queue_id
            )
        })
}

fn cron_runtime_dispatch_blocker_for_item(
    harness_home: &Path,
    item: &PendingQueueItem,
    now_ms: i64,
) -> io::Result<Option<String>> {
    let Some(run_id) = item.cron_run_id.as_deref() else {
        return Ok(None);
    };
    cron_run_runtime_dispatch_blocker(harness_home, run_id, &item.queue_id, now_ms)
}

fn select_pending_item(
    pending_items: Vec<PendingQueueItem>,
    requested_queue_id: Option<&str>,
    prepared_ids: &HashSet<String>,
    terminal_run_ids: &HashSet<String>,
    lease_state: &RuntimeQueueLeaseState,
    runtime_class: &str,
    harness_home: &Path,
    queue_dir: &Path,
    now_ms: i64,
    warnings: &mut Vec<String>,
) -> io::Result<Option<PendingQueueItem>> {
    let mut pending_items = pending_items
        .into_iter()
        .filter(|item| item.runtime_class == runtime_class)
        .collect::<Vec<_>>();
    if runtime_class == "cron" {
        let round_by_queue_id = cron_agent_rounds(&pending_items);
        pending_items.sort_by(|left, right| {
            let left_round = round_by_queue_id.get(&left.queue_id).copied().unwrap_or(0);
            let right_round = round_by_queue_id.get(&right.queue_id).copied().unwrap_or(0);
            left_round
                .cmp(&right_round)
                .then_with(|| left.created_at_ms.cmp(&right.created_at_ms))
                .then_with(|| left.agent_id.cmp(&right.agent_id))
                .then_with(|| left.queue_id.cmp(&right.queue_id))
        });
    } else {
        pending_items.sort_by(|left, right| {
            runtime_selection_key(left)
                .cmp(&runtime_selection_key(right))
                .then_with(|| left.queue_id.cmp(&right.queue_id))
        });
    }
    let config = load_runtime_dispatch_config(harness_home)?;
    for item in pending_items.iter() {
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
        if terminal_run_ids.contains(&item.queue_id) {
            warnings.push(format!(
                "runtime queue item `{}` already has a terminal run receipt; skipping",
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
        if let Some(blocker) = cron_runtime_dispatch_blocker_for_item(harness_home, item, now_ms)? {
            warnings.push(format!(
                "runtime queue item `{}` blocked by {}; tombstoning",
                item.queue_id, blocker
            ));
            tombstone_runtime_queue_item_skipped(queue_dir, &item, &blocker)?;
            continue;
        }
        if let Some(blocker) = runtime_capacity_blocker(harness_home, lease_state, item)? {
            warnings.push(format!(
                "runtime queue item `{}` blocked by {}; skipping",
                item.queue_id, blocker
            ));
            continue;
        }
        if let Some(blocker) =
            same_session_fifo_blocker(&pending_items, item, terminal_run_ids, &config)
        {
            warnings.push(format!(
                "runtime queue item `{}` blocked by {}; skipping",
                item.queue_id, blocker
            ));
            continue;
        }
        return Ok(Some(item.clone()));
    }
    Ok(None)
}

fn runtime_selection_key(item: &PendingQueueItem) -> (usize, String, i64) {
    let class_rank = match item.runtime_class.as_str() {
        "interactive" => 0,
        "cron" => 1,
        "worker" => 2,
        _ => 3,
    };
    (class_rank, item.agent_id.clone(), item.created_at_ms)
}

fn cron_agent_rounds(pending_items: &[PendingQueueItem]) -> HashMap<String, usize> {
    let mut ordered = pending_items.iter().collect::<Vec<_>>();
    ordered.sort_by(|left, right| {
        left.created_at_ms
            .cmp(&right.created_at_ms)
            .then_with(|| left.queue_id.cmp(&right.queue_id))
    });
    let mut next_round_by_agent = HashMap::<String, usize>::new();
    let mut round_by_queue_id = HashMap::<String, usize>::new();
    for item in ordered {
        let round = next_round_by_agent
            .entry(item.agent_id.clone())
            .or_insert(0);
        round_by_queue_id.insert(item.queue_id.clone(), *round);
        *round += 1;
    }
    round_by_queue_id
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
    read_run_once_ids_by_latest_status(receipts_file, warnings, is_terminal_run_once_status)
}

fn read_retry_pending_run_once_ids(
    receipts_file: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<HashSet<String>> {
    read_run_once_ids_by_latest_status(receipts_file, warnings, |status| status == "retry-pending")
}

fn read_run_once_ids_by_latest_status<F>(
    receipts_file: &Path,
    warnings: &mut Vec<String>,
    predicate: F,
) -> io::Result<HashSet<String>>
where
    F: Fn(&str) -> bool,
{
    let mut latest_status_by_id = HashMap::new();
    if !receipts_file.is_file() {
        return Ok(HashSet::new());
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
        {
            latest_status_by_id.insert(queue_id.to_string(), status.to_string());
        }
    }
    Ok(latest_status_by_id
        .into_iter()
        .filter_map(|(queue_id, status)| predicate(&status).then_some(queue_id))
        .collect())
}

fn is_terminal_run_once_status(status: &str) -> bool {
    matches!(
        status,
        "completed" | "timeout" | "failed-terminal" | "canceled" | "skipped" | "dead-letter"
    )
}

fn parse_pending_item(value: &Value) -> Option<PendingQueueItem> {
    let source = value.get("source")?;
    let platform = string_field(value, &["platform"])?.to_string();
    let runtime_class = string_field(value, &["runtimeClass", "runtime_class"])
        .map(ToString::to_string)
        .unwrap_or_else(|| default_runtime_class_for(&platform));
    Some(PendingQueueItem {
        queue_id: string_field(value, &["queueId", "queue_id"])?.to_string(),
        created_at_ms: i64_field(value, &["createdAtMs", "created_at_ms"]).unwrap_or(0),
        agent_id: string_field(value, &["agentId", "agent_id"])?.to_string(),
        session_key: string_field(value, &["sessionKey", "session_key"])?.to_string(),
        runtime_class,
        origin: string_field(value, &["origin"])
            .map(ToString::to_string)
            .unwrap_or_else(|| {
                if platform == "native-cron" {
                    "cron-scheduler".to_string()
                } else {
                    "channel".to_string()
                }
            }),
        cron_run_id: string_field(value, &["cronRunId", "cron_run_id"]).map(ToString::to_string),
        scheduled_for_ms: i64_field(value, &["scheduledForMs", "scheduled_for_ms"]),
        platform,
        account_id: string_field(value, &["accountId", "account_id"]).map(ToString::to_string),
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

fn default_runtime_class_for(platform: &str) -> String {
    match platform {
        "native-cron" => "cron".to_string(),
        "worker" | "worker-watchdog" => "worker".to_string(),
        _ => "interactive".to_string(),
    }
}

fn queue_execution_dir(harness_home: &Path, queue_id: &str) -> PathBuf {
    harness_home
        .join("state")
        .join("runtime-queue")
        .join("executions")
        .join(normalize_key_part(queue_id))
}

fn append_json_line(path: &Path, value: &impl Serialize) -> io::Result<()> {
    crate::append_jsonl_value(path, value)
}

fn tombstone_runtime_queue_item_skipped(
    queue_dir: &Path,
    item: &PendingQueueItem,
    reason: &str,
) -> io::Result<()> {
    append_json_line(
        &queue_dir.join("run-once-receipts.jsonl"),
        &RuntimeRunOnceSkipReceipt {
            schema: "agent-harness.runtime-run-once.v1",
            queue_id: Some(item.queue_id.clone()),
            status: "skipped",
            runtime_class: Some(item.runtime_class.clone()),
            origin: Some(item.origin.clone()),
            cron_run_id: item.cron_run_id.clone(),
            scheduled_for_ms: item.scheduled_for_ms,
            execution_dir: None,
            transcript_file: None,
            outbox_file: None,
            reason: reason.to_string(),
        },
    )
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

fn i64_field(value: &Value, keys: &[&str]) -> Option<i64> {
    for key in keys {
        if let Some(number) = value.get(*key).and_then(Value::as_i64) {
            return Some(number);
        }
    }
    None
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
        enqueue_fixture_turn_with_session(
            &source,
            &harness_home,
            "telegram",
            "dm-42",
            "user-7",
            "session-a",
            "first turn",
            1234,
        );
        enqueue_fixture_turn_with_session(
            &source,
            &harness_home,
            "telegram",
            "dm-42",
            "user-7",
            "session-b",
            "second turn",
            1235,
        );

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
    fn prepare_runtime_queue_item_serializes_same_channel_session_even_with_channel_capacity() {
        let root = temp_root(
            "prepare_runtime_queue_item_serializes_same_channel_session_even_with_channel_capacity",
        );
        let source = write_worker_source(&root);
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            r#"{"workerDispatch":{"globalConcurrencyLimit":3,"groupConcurrencyLimit":3,"channelConcurrencyLimit":3,"laneConcurrencyLimits":{"llm":3}}}"#,
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
                .any(|warning| warning.contains("session-active"))
        );

        append_json_line(
            &queue_dir(&harness_home).join("run-once-receipts.jsonl"),
            &serde_json::json!({
                "queueId": first_queue_id,
                "status": "completed",
                "reason": "test terminal receipt"
            }),
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
        assert_ne!(
            second.receipt.queue_id.as_deref(),
            Some(first_queue_id.as_str())
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prepare_runtime_queue_item_allows_different_sessions_when_channel_capacity_allows() {
        let root = temp_root(
            "prepare_runtime_queue_item_allows_different_sessions_when_channel_capacity_allows",
        );
        let source = write_worker_source(&root);
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            r#"{"workerDispatch":{"globalConcurrencyLimit":3,"groupConcurrencyLimit":3,"channelConcurrencyLimit":3,"laneConcurrencyLimits":{"llm":3}}}"#,
        )
        .unwrap();
        enqueue_fixture_turn_with_session(
            &source,
            &harness_home,
            "telegram",
            "dm-42",
            "user-7",
            "session-a",
            "first turn",
            1234,
        );
        enqueue_fixture_turn_with_session(
            &source,
            &harness_home,
            "telegram",
            "dm-42",
            "user-7",
            "session-b",
            "second turn",
            1235,
        );

        let first = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();
        let second = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();

        assert_eq!(
            first.receipt.status,
            RuntimeExecutionReceiptStatus::Prepared
        );
        assert_eq!(
            second.receipt.status,
            RuntimeExecutionReceiptStatus::Prepared
        );
        assert_ne!(first.receipt.queue_id, second.receipt.queue_id);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn explicit_runtime_queue_id_cannot_overtake_older_same_session_turn() {
        let root = temp_root("explicit_runtime_queue_id_cannot_overtake_older_same_session_turn");
        let source = write_worker_source(&root);
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            r#"{"workerDispatch":{"globalConcurrencyLimit":3,"groupConcurrencyLimit":3,"channelConcurrencyLimit":3,"laneConcurrencyLimits":{"llm":3}}}"#,
        )
        .unwrap();
        enqueue_fixture_turn_with_text(&source, &harness_home, "first turn", 1234);
        enqueue_fixture_turn_with_text(&source, &harness_home, "second turn", 1235);
        let pending_text =
            fs::read_to_string(queue_dir(&harness_home).join("pending.jsonl")).unwrap();
        let queue_ids = pending_text
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .map(|value| value["queueId"].as_str().unwrap().to_string())
            .collect::<Vec<_>>();
        assert_eq!(queue_ids.len(), 2);

        let blocked = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: Some(queue_ids[1].clone()),
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();
        assert!(blocked.item.is_none());
        assert_eq!(
            blocked.receipt.status,
            RuntimeExecutionReceiptStatus::NoPendingItem
        );
        assert!(
            blocked
                .warnings
                .iter()
                .any(|warning| warning.contains("session-fifo"))
        );

        let first = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();
        assert_eq!(
            first.receipt.queue_id.as_deref(),
            Some(queue_ids[0].as_str())
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn worker_lane_is_not_collapsed_by_interactive_session_mutex() {
        let root = temp_root("worker_lane_is_not_collapsed_by_interactive_session_mutex");
        let source = write_worker_source(&root);
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            r#"{"workerDispatch":{"globalConcurrencyLimit":3,"groupConcurrencyLimit":3,"channelConcurrencyLimit":3,"laneConcurrencyLimits":{"llm":3}},"runtimeDispatch":{"interactiveReserve":0}}"#,
        )
        .unwrap();
        append_worker_pending_runtime_item(&source, &harness_home, "turn:worker:one", 1234);
        append_worker_pending_runtime_item(&source, &harness_home, "turn:worker:two", 1235);

        let first = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();
        let second = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();

        assert_eq!(first.receipt.runtime_class.as_deref(), Some("worker"));
        assert_eq!(second.receipt.runtime_class.as_deref(), Some("worker"));
        assert_eq!(
            first.receipt.status,
            RuntimeExecutionReceiptStatus::Prepared
        );
        assert_eq!(
            second.receipt.status,
            RuntimeExecutionReceiptStatus::Prepared
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prepare_runtime_queue_item_selects_cron_when_old_interactive_is_terminal() {
        let root =
            temp_root("prepare_runtime_queue_item_selects_cron_when_old_interactive_is_terminal");
        let source = write_worker_source(&root);
        let harness_home = root.join(".agent-harness");
        enqueue_fixture_turn_with_text(&source, &harness_home, "old interactive turn", 1000);
        let queue_dir = harness_home.join("state").join("runtime-queue");
        let pending_text = fs::read_to_string(queue_dir.join("pending.jsonl")).unwrap();
        let interactive_item: Value =
            serde_json::from_str(pending_text.lines().next().unwrap()).unwrap();
        let interactive_queue_id = interactive_item["queueId"].as_str().unwrap().to_string();
        append_json_line(
            &queue_dir.join("run-once-receipts.jsonl"),
            &serde_json::json!({
                "queueId": interactive_queue_id,
                "status": "completed",
                "reason": "old interactive turn completed"
            }),
        )
        .unwrap();

        let cron_queue_id = "turn:cron:daily:1001";
        let cron_run = crate::admit_cron_run(crate::CronRunAdmitOptions {
            harness_home: harness_home.clone(),
            source_kind: "native-cron".to_string(),
            source_id: "source-1".to_string(),
            entry_id: "daily".to_string(),
            agent_id: "main".to_string(),
            scheduled_for_ms: 1001,
            runtime_class: "cron".to_string(),
            session_key: "cron:main:daily:1001".to_string(),
            session_policy: "one-shot".to_string(),
            max_attempts: 3,
            now_ms: 1001,
        })
        .unwrap();
        crate::mark_cron_run_runtime_enqueued(&harness_home, &cron_run.run_id, cron_queue_id, 1002)
            .unwrap();
        append_cron_pending_runtime_item(
            &source,
            &harness_home,
            cron_queue_id,
            &cron_run.run_id,
            "main",
            1001,
        );

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
        assert_eq!(report.receipt.queue_id.as_deref(), Some(cron_queue_id));
        assert_eq!(report.receipt.runtime_class.as_deref(), Some("cron"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prepare_runtime_queue_item_tombstones_skipped_cron_run() {
        let root = temp_root("prepare_runtime_queue_item_tombstones_skipped_cron_run");
        let source = write_worker_source(&root);
        let harness_home = root.join(".agent-harness");
        let cron_queue_id = "turn:cron:daily:1001";
        let cron_run = crate::admit_cron_run(crate::CronRunAdmitOptions {
            harness_home: harness_home.clone(),
            source_kind: "native-cron".to_string(),
            source_id: "source-1".to_string(),
            entry_id: "daily".to_string(),
            agent_id: "main".to_string(),
            scheduled_for_ms: 1001,
            runtime_class: "cron".to_string(),
            session_key: "cron:main:daily:1001".to_string(),
            session_policy: "one-shot".to_string(),
            max_attempts: 3,
            now_ms: 1001,
        })
        .unwrap();
        crate::mark_cron_run_runtime_enqueued(&harness_home, &cron_run.run_id, cron_queue_id, 1002)
            .unwrap();
        append_cron_pending_runtime_item(
            &source,
            &harness_home,
            cron_queue_id,
            &cron_run.run_id,
            "main",
            1001,
        );
        crate::control_cron_run(crate::CronRunControlOptions {
            harness_home: harness_home.clone(),
            action: crate::CronRunControlAction::Skip,
            run_id: Some(cron_run.run_id.clone()),
            agent_id: None,
            entry_id: None,
            reason: "operator skip before runtime dispatch".to_string(),
            now_ms: 1003,
        })
        .unwrap();

        let report = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: Some(cron_queue_id.to_string()),
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();

        assert!(report.item.is_none());
        assert_eq!(
            report.receipt.status,
            RuntimeExecutionReceiptStatus::NoPendingItem
        );
        let run_once_receipts =
            fs::read_to_string(queue_dir(&harness_home).join("run-once-receipts.jsonl")).unwrap();
        assert!(run_once_receipts.contains("\"queueId\":\"turn:cron:daily:1001\""));
        assert!(run_once_receipts.contains("\"status\":\"skipped\""));
        let runs = crate::collect_cron_run_summary(&harness_home).unwrap();
        assert_eq!(
            runs.runs.first().unwrap().status,
            crate::CronRunStatus::Skipped
        );

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
    fn prepare_runtime_queue_item_counts_legacy_root_leases() {
        let root = temp_root("prepare_runtime_queue_item_counts_legacy_root_leases");
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
            "pending tg",
            1234,
        );
        let queue_dir = harness_home.join("state").join("runtime-queue");
        let now_ms = current_log_time_ms().unwrap();
        let mut legacy_state = RuntimeQueueLeaseState {
            schema: runtime_queue_leases_schema(),
            leases: BTreeMap::new(),
        };
        legacy_state.leases.insert(
            "legacy-active".to_string(),
            RuntimeQueueLease {
                queue_id: "legacy-active".to_string(),
                agent_id: "main".to_string(),
                runtime_class: "interactive".to_string(),
                origin: "channel".to_string(),
                cron_run_id: None,
                platform: "telegram".to_string(),
                account_id: None,
                channel_id: "tg-dm".to_string(),
                user_id: Some("user-7".to_string()),
                session_key: "main:telegram:tg-dm".to_string(),
                session_lane_key: None,
                owner: "legacy-worker".to_string(),
                started_at_ms: now_ms,
                lease_expires_at_ms: now_ms.saturating_add(60_000),
            },
        );
        write_runtime_queue_leases(&queue_dir, "legacy", &legacy_state).unwrap();

        let report = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();

        assert!(report.item.is_none());
        assert_eq!(
            report.receipt.status,
            RuntimeExecutionReceiptStatus::NoPendingItem
        );
        assert!(
            report
                .warnings
                .iter()
                .any(|warning| warning.contains("agent-channel limit"))
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn cron_runtime_selection_interleaves_agents() {
        let root = temp_root("cron_runtime_selection_interleaves_agents");
        let harness_home = root.join(".agent-harness");
        let queue_dir = harness_home.join("state").join("runtime-queue");
        fs::create_dir_all(&queue_dir).unwrap();
        let pending = vec![
            pending_cron_item("cron-a-1", "agent-a", 1000),
            pending_cron_item("cron-a-2", "agent-a", 1001),
            pending_cron_item("cron-b-1", "agent-b", 1002),
        ];
        let lease_state = RuntimeQueueLeaseState::default();
        let terminal_ids = HashSet::new();
        let mut prepared_ids = HashSet::new();
        let mut warnings = Vec::new();

        let first = select_pending_item(
            pending.clone(),
            None,
            &prepared_ids,
            &terminal_ids,
            &lease_state,
            "cron",
            &harness_home,
            &queue_dir,
            2000,
            &mut warnings,
        )
        .unwrap()
        .unwrap();
        assert_eq!(first.queue_id, "cron-a-1");
        prepared_ids.insert(first.queue_id);

        let second = select_pending_item(
            pending,
            None,
            &prepared_ids,
            &terminal_ids,
            &lease_state,
            "cron",
            &harness_home,
            &queue_dir,
            2001,
            &mut warnings,
        )
        .unwrap()
        .unwrap();
        assert_eq!(second.queue_id, "cron-b-1");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn inspect_runtime_queue_capacity_treats_busy_lease_lock_as_zero_capacity() {
        let root = temp_root("inspect_runtime_queue_capacity_treats_busy_lease_lock_as_zero");
        let harness_home = root.join(".agent-harness");
        let queue_dir = harness_home.join("state").join("runtime-queue");
        fs::create_dir_all(&queue_dir).unwrap();
        let now_ms = current_log_time_ms().unwrap();
        let _held_lock = create_runtime_queue_lease_lock(
            &runtime_queue_lease_lock_file(&queue_dir, "interactive"),
            now_ms,
        )
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
        let _held_lock = create_runtime_queue_lease_lock(
            &runtime_queue_lease_lock_file(&queue_dir, "interactive"),
            now_ms,
        )
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
    fn prepare_runtime_queue_item_does_not_let_cron_class_lock_block_interactive() {
        let root =
            temp_root("prepare_runtime_queue_item_does_not_let_cron_class_lock_block_interactive");
        let source = write_worker_source(&root);
        let harness_home = root.join(".agent-harness");
        enqueue_fixture_turn_with_platform_channel(
            &source,
            &harness_home,
            "telegram",
            "tg-dm",
            "user-7",
            "interactive turn",
            1234,
        );
        let queue_dir = harness_home.join("state").join("runtime-queue");
        let mut pending = OpenOptions::new()
            .create(true)
            .append(true)
            .open(queue_dir.join("pending.jsonl"))
            .unwrap();
        writeln!(
            pending,
            "{}",
            serde_json::json!({
                "queueId": "turn:cron:blocked",
                "createdAtMs": 1233,
                "agentId": "main@cron",
                "sessionKey": "cron:main:hourly:1233",
                "runtimeClass": "cron",
                "origin": "cron-scheduler",
                "cronRunId": "cronrun:native-cron:main:hourly:1233",
                "scheduledForMs": 1233,
                "platform": "native-cron",
                "channelId": "cron",
                "userId": "scheduler",
                "messageText": "cron turn",
                "source": {
                    "sourceHome": source.home.clone(),
                    "sourceWorkspace": source.workspace.clone(),
                    "runtimeWorkspace": source.workspace.clone()
                },
                "plannedTranscriptFile": harness_home.join("agents").join("main").join("cron-sessions").join("hourly").join("transcript.jsonl"),
                "plannedTrajectoryFile": harness_home.join("agents").join("main").join("cron-sessions").join("hourly").join("trajectory.jsonl"),
                "selectedSkillIds": []
            })
        )
        .unwrap();
        let now_ms = current_log_time_ms().unwrap();
        let _cron_lock = create_runtime_queue_lease_lock(
            &runtime_queue_lease_lock_file(&queue_dir, "cron"),
            now_ms,
        )
        .unwrap();

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
        assert_eq!(report.receipt.runtime_class.as_deref(), Some("interactive"));
        assert_eq!(report.item.as_ref().unwrap().runtime_class, "interactive");
        assert_ne!(
            report.receipt.queue_id.as_deref(),
            Some("turn:cron:blocked")
        );

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
                "queueId": queue_id.clone(),
                "status": "retry-pending",
                "reason": "transient reconnect; retry with the same session"
            })
        )
        .unwrap();

        let retry_capacity = inspect_runtime_queue_capacity(RuntimeQueueCapacityOptions {
            harness_home: harness_home.clone(),
        })
        .unwrap();
        assert_eq!(retry_capacity.claimable_items, 1);
        assert_eq!(retry_capacity.claimable_queue_ids, vec![queue_id.clone()]);
        let retry_resumed = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
            harness_home: harness_home.clone(),
            queue_id: None,
            prompt_options: PromptAssemblyOptions::default(),
        })
        .unwrap();
        assert!(retry_resumed.item.is_none());
        assert_eq!(
            retry_resumed.receipt.status,
            RuntimeExecutionReceiptStatus::AlreadyPrepared
        );
        assert_eq!(
            retry_resumed.receipt.queue_id.as_deref(),
            Some(queue_id.as_str())
        );
        release_runtime_queue_lease(&harness_home, &queue_id).unwrap();

        writeln!(
            run_once_file,
            "{}",
            serde_json::json!({
                "queueId": queue_id.clone(),
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
            RuntimeExecutionReceiptStatus::NoPendingItem
        );
        assert_eq!(after_timeout.receipt.queue_id, None);

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
            RuntimeExecutionReceiptStatus::NoPendingItem
        );
        assert!(explicit.receipt.execution_dir.is_none());
        assert!(explicit.receipt.reason.contains("terminal run receipt"));

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
        enqueue_fixture_turn_with_optional_session(
            source,
            harness_home,
            platform,
            channel_id,
            user_id,
            None,
            text,
            now_ms,
        );
    }

    fn enqueue_fixture_turn_with_session(
        source: &AgentSource,
        harness_home: &Path,
        platform: &str,
        channel_id: &str,
        user_id: &str,
        session_key: &str,
        text: &str,
        now_ms: i64,
    ) {
        enqueue_fixture_turn_with_optional_session(
            source,
            harness_home,
            platform,
            channel_id,
            user_id,
            Some(session_key),
            text,
            now_ms,
        );
    }

    fn enqueue_fixture_turn_with_optional_session(
        source: &AgentSource,
        harness_home: &Path,
        platform: &str,
        channel_id: &str,
        user_id: &str,
        session_key: Option<&str>,
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
                session_hint: session_key.map(ToString::to_string),
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

    fn append_worker_pending_runtime_item(
        source: &AgentSource,
        harness_home: &Path,
        queue_id: &str,
        created_at_ms: i64,
    ) {
        append_json_line(
            &queue_dir(harness_home).join("pending.jsonl"),
            &serde_json::json!({
                "queueId": queue_id,
                "status": "queued",
                "createdAtMs": created_at_ms,
                "agentId": "main",
                "sessionKey": "shared-worker-session",
                "runtimeClass": "worker",
                "origin": "worker-dispatch",
                "platform": "worker",
                "channelId": "worker-channel",
                "userId": "worker-user",
                "messageText": "run worker turn",
                "source": {
                    "sourceHome": source.home.clone(),
                    "sourceWorkspace": source.workspace.clone(),
                    "runtimeWorkspace": source.workspace.clone()
                },
                "plannedTranscriptFile": harness_home
                    .join("agents")
                    .join("main")
                    .join("worker-sessions")
                    .join(format!("{}.jsonl", normalize_key_part(queue_id))),
                "plannedTrajectoryFile": harness_home
                    .join("agents")
                    .join("main")
                    .join("worker-sessions")
                    .join(format!("{}.trajectory.jsonl", normalize_key_part(queue_id))),
                "selectedSkillIds": []
            }),
        )
        .unwrap();
    }

    fn append_cron_pending_runtime_item(
        source: &AgentSource,
        harness_home: &Path,
        queue_id: &str,
        cron_run_id: &str,
        agent_id: &str,
        scheduled_for_ms: i64,
    ) {
        append_json_line(
            &queue_dir(harness_home).join("pending.jsonl"),
            &serde_json::json!({
                "queueId": queue_id,
                "status": "queued",
                "createdAtMs": scheduled_for_ms,
                "agentId": agent_id,
                "sessionKey": format!("cron:{agent_id}:daily:{scheduled_for_ms}"),
                "runtimeClass": "cron",
                "origin": "cron-scheduler",
                "cronRunId": cron_run_id,
                "scheduledForMs": scheduled_for_ms,
                "platform": "native-cron",
                "channelId": "daily",
                "userId": "cron-scheduler",
                "messageText": "run daily cron",
                "source": {
                    "sourceHome": source.home.clone(),
                    "sourceWorkspace": source.workspace.clone(),
                    "runtimeWorkspace": source.workspace.clone()
                },
                "plannedTranscriptFile": harness_home
                    .join("agents")
                    .join(agent_id)
                    .join("cron-sessions")
                    .join(format!("{}.jsonl", normalize_key_part(queue_id))),
                "plannedTrajectoryFile": harness_home
                    .join("agents")
                    .join(agent_id)
                    .join("cron-sessions")
                    .join(format!("{}.trajectory.jsonl", normalize_key_part(queue_id))),
                "selectedSkillIds": []
            }),
        )
        .unwrap();
    }

    fn queue_dir(harness_home: &Path) -> PathBuf {
        harness_home.join("state").join("runtime-queue")
    }

    fn pending_cron_item(queue_id: &str, agent_id: &str, created_at_ms: i64) -> PendingQueueItem {
        PendingQueueItem {
            queue_id: queue_id.to_string(),
            created_at_ms,
            agent_id: agent_id.to_string(),
            session_key: format!("cron:{agent_id}:{queue_id}:{created_at_ms}"),
            runtime_class: "cron".to_string(),
            origin: "cron-scheduler".to_string(),
            cron_run_id: None,
            scheduled_for_ms: Some(created_at_ms),
            platform: "native-cron".to_string(),
            account_id: None,
            channel_id: queue_id.to_string(),
            user_id: "cron-scheduler".to_string(),
            message_text: format!("run {queue_id}"),
            inbound_context: None,
            source_home: PathBuf::from("source"),
            source_workspace: PathBuf::from("workspace"),
            runtime_workspace: None,
            planned_transcript_file: PathBuf::from(format!("{queue_id}.jsonl")),
            planned_trajectory_file: PathBuf::from(format!("{queue_id}.trajectory.jsonl")),
            selected_skill_ids: Vec::new(),
        }
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
