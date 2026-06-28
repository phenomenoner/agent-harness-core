use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::env;
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ring::digest;
use serde::Serialize;

mod telegram_media;

use agent_harness_core::{
    ActivationReadinessOptions, ActivationReadinessReport, AdmissionDecisionOptions,
    AgentProgressDeliveryAction, AgentProgressDeliveryPending, AgentProgressDeliveryPlanOptions,
    AgentProgressDeliveryRecordOptions, AgentProgressDeliveryStatus, AgentRegistry, AgentSource,
    ArtifactExtractionSummary, AssistantNarrationMode, BackgroundTaskListOptions,
    BackgroundTaskRecord, BackgroundTaskUpsertOptions, BudgetAcquireOptions,
    BuiltinHarnessSkillSyncOptions, BuiltinHarnessSkillSyncReport, ChannelCommand,
    ChannelCommandApplyOptions, ChannelCommandApplyReport, ChannelDeliveryIntentKind,
    ChannelDeliveryReceipt, ChannelDeliveryRecordOptions, ChannelDeliveryStatus,
    ChannelIdentityLookup, ChannelIdentityResolutionStatus, ChannelOutboundAttachment,
    ChannelOutboundAttachmentKind, ChannelOutboundMessage, ChannelOutboxPlanOptions,
    ChannelOutboxPlanReport, ChannelReceiveOptions, ChannelReceiveReport, ChannelRunOnceOptions,
    ChannelRunOnceReport, ChannelStep, CodexRuntimeCompletionOptions, CodexRuntimeCompletionReport,
    CodexRuntimeLaunchProbeOptions, CodexRuntimeLaunchProbeReport, CodexRuntimePlanOptions,
    CodexRuntimePlanReport, CodexRuntimePreflightOptions, CodexRuntimePreflightReport,
    CodexRuntimeRunOptions, CodexRuntimeRunReport, ConflictPolicy, ContextPackParseOptions,
    ContextRolloverRequeuePreparedOptions, CreateOperationPlanOptions, CronRunControlAction,
    CronRunControlOptions, CronRunListOptions, CronSchedulerLintStatus,
    CronSchedulerRunOnceOptions, CronSchedulerTickStatus, DEFAULT_INBOUND_MEDIA_MAX_BYTES_PER_ITEM,
    DEFAULT_MEMORY_BACKFILL_BATCH_SIZE, DEFAULT_MEMORY_BACKFILL_COVERAGE_THRESHOLD_BPS,
    DEFAULT_MEMORY_BACKFILL_MAX_ITEMS, DEFAULT_MEMORY_BACKFILL_RATE_LIMIT_PER_MINUTE,
    DEFAULT_MEMORY_BACKFILL_RETRY_CAP, DEFAULT_MEMORY_BACKFILL_VECTOR_DIMENSION,
    DEFAULT_MEMORY_OWNER_HEARTBEAT_MAX_AGE_MS, DeterministicCronPlan, DeterministicCronPlanInput,
    DeterministicCronWorkerEnqueueOptions, DriftCheckOptions, DryRunImportOptions,
    ExecuteImportOptions, HarnessLogEvent, HarnessLogLevel, HarnessLogRotationOptions,
    HarnessMetricsOptions, HarnessStatusOptions, HarnessStatusReport, HealthzOptions,
    ImportPhaseStatus, ImportReport, InboundMediaArtifact, InboundMediaDownloadStatus,
    InboundMediaModelAttachmentStatus, InboundMediaSelectedVariant, LearningProposalOptions,
    LiveControlAction, McpRequestOptions, MemoryCanvasWorkerOptions, MemoryCanvasWorkerReport,
    MemoryCanvasWorkerStatus, MemoryCredentialsExportOptions, MemoryCredentialsExportReport,
    MemoryEmbeddingBackfillLane, MemoryEmbeddingBackfillOptions, MemoryEmbeddingBackfillReport,
    MemoryHookAdapterOptions, MemoryHookKind, MemoryOwnerEndpointProbeOptions,
    MemoryOwnerEnsureOptions, MemoryOwnerHeartbeatOptions, MemoryOwnerPromotionOptions,
    MemoryOwnerRecoveryOptions, MemoryOwnerShadowKind, MemoryOwnerShadowOptions,
    MemoryOwnerTrustScopeOptions, MemorySearchOptions, MemorySearchReport,
    MemoryVectorRecallOptions, MemoryVectorRecallReport, MemoryVectorRecallStatus, NativeCronPlan,
    NativeCronPlanInput, NativeCronWorkerEnqueueOptions, OpenClawMemLocalOwnerPrepareOptions,
    OpenClawMemReadPathSmokeOptions, OpenClawMemServiceProposeOptions,
    OpenClawMemServiceRecallOptions, OpenClawMemServiceStatus, OpenClawMemServiceStatusOptions,
    OpenClawMemServiceStoreOptions, OperationPlanAddItemOptions, OperationPlanBlockOptions,
    OperationPlanCommentOptions, OperationPlanCompleteOptions, OperationPlanDelegateItemOptions,
    OperationPlanItemStatus, OperationPlanPromoteDependenciesOptions, OperationPlanShowOptions,
    OperationPlanUpdateItemOptions, OpsBackupOptions, OpsControlAction, OpsControlOptions,
    OpsCutoverApplyOptions, OpsCutoverApproveOptions, OpsCutoverReceiptOptions,
    OpsCutoverRequestOptions, OpsCutoverStatusOptions, PromptAssemblyOptions, PromptBundle,
    PromptReductionOptions, PublicHygieneOptions, QueueShadowCompareOptions,
    QueueShadowRecordOptions, RuntimeQueueCapacityOptions, RuntimeQueueControlAction,
    RuntimeQueueControlOptions, RuntimeQueueEnqueueOptions, RuntimeQueueEnqueueReport,
    RuntimeQueuePrepareOptions, RuntimeQueuePrepareReport, RuntimeRunOnceOptions,
    RuntimeRunOnceReport, RuntimeRunOnceStatus, ScopedStopOptions, ScopedStopTarget,
    SecurityScanOptions, SkillApplyOptions, SkillArchiveOptions, SkillIndex,
    SkillLearningProposalOperation, SkillLearningProposalStatus, SkillLearningSignal,
    SkillProposalActionOptions, SkillProposalListOptions, SkillProposeOptions, SkillSelectionQuery,
    SubagentLifecycleCloseOptions, SubagentLifecycleRecordOptions, SubagentLifecycleShowOptions,
    SubagentLifecycleShowReport, SubagentLifecycleState, SubagentPlan, SubagentPlanInput,
    SubagentWorkerEnqueueOptions, SuperviseDeployCanaryOptions, SupervisionEvaluateOptions,
    SupervisorChildState, SupervisorInventoryOptions, SupervisorInventoryServiceConfig,
    SupervisorLaunchCommand, TaskEntityOptions, TaskStatus, TokenEfficiencyOptions, TraceOptions,
    TurnPlan, TurnPlanInput, VaultGetOptions, VaultPutOptions, WindowsSupervisorPlanOptions,
    WindowsSupervisorPlanReport, WorkerCancelOptions, WorkerEnqueueOptions, WorkerEnqueueReport,
    WorkerJobKind, WorkerReapStaleOptions, WorkerRunOnceOptions, WorkerRunOnceReport,
    WorkerRunOnceStatus, WorkerStatusOptions, acquire_budget, add_operation_plan_item,
    append_harness_log, append_jsonl_value, apply_channel_command_step, apply_skill_proposal,
    assemble_prompt_bundle, block_operation_plan, build_channel_step, build_dry_run_report,
    build_harness_skill_index, build_import_plan, build_runtime_skill_index,
    build_source_skill_index, build_turn_plan, cancel_worker_job, check_activation_readiness,
    check_config_drift, check_tool_description_pin, collect_harness_metrics,
    collect_harness_status, collect_healthz, collect_inbound_media_cache_report,
    collect_ops_cutover_status, collect_token_efficiency, collect_worker_status,
    comment_on_operation_plan, compare_channel_turn_shadow, complete_operation_plan,
    control_cron_run, control_runtime_queue_item, create_learning_proposal, create_operation_plan,
    create_ops_backup, create_skill_archive_proposal, create_skill_learning_proposal,
    current_log_time_ms, default_supervisor_child_specs, delegate_operation_plan_item,
    enqueue_channel_step, enqueue_deterministic_cron_workers, enqueue_native_cron_workers,
    enqueue_subagent_workers, enqueue_worker_job, ensure_memory_owner_state, evaluate_admission,
    evaluate_prompt_reduction, evaluate_supervisor_children, execute_import,
    export_harness_registry_files, export_memory_credentials, get_vault_secret, handle_mcp_request,
    inspect_openclaw_mem_service, inspect_runtime_queue_capacity, invariant_catalog, inventory,
    lint_cron_scheduler, list_background_tasks, list_cron_runs, list_operation_plans,
    list_skill_proposals, load_agent_registry, load_deterministic_cron_store,
    load_native_cron_store, load_subagent_ledger, parse_channel_command, parse_context_pack,
    plan_agent_progress_delivery, plan_channel_outbox, plan_codex_runtime, plan_deterministic_cron,
    plan_native_cron, plan_subagents, preflight_codex_runtime, prepare_openclaw_mem_local_owner,
    prepare_runtime_queue_item, probe_codex_runtime_launch,
    promote_operation_plan_items_from_dependencies, propose_openclaw_mem_service_memory,
    put_vault_secret, reap_stale_worker_jobs, recall_openclaw_mem_service, receive_channel_message,
    reconcile_supervisor_inventory, record_agent_progress_delivery, record_channel_delivery,
    record_channel_turn_shadow, record_codex_runtime_completion,
    record_memory_owner_endpoint_probe, record_memory_owner_heartbeat,
    record_memory_owner_shadow_receipt, record_memory_owner_trust_scope_receipt,
    record_ops_control, record_ops_cutover_apply, record_ops_cutover_approval,
    record_ops_cutover_receipt, record_ops_cutover_request, record_scoped_stop,
    record_subagent_lifecycle, record_supervise_deploy_canary, recover_memory_owner_state,
    reject_skill_proposal, release_checklist, request_memory_owner_promotion,
    requeue_prepared_context_rollover, resolve_channel_identity, rotate_harness_log_if_needed,
    run_channel_once, run_codex_runtime, run_cron_scheduler_once, run_memory_canvas_worker,
    run_memory_embedding_backfill, run_memory_hook_adapter, run_openclaw_mem_read_path_smoke,
    run_public_hygiene, run_runtime_queue_once, run_worker_once,
    runtime_worker::reconcile_runtime_queue_leases_for_generation, scan_security_boundaries,
    schema_registry_entries, search_imported_memory, search_imported_vector_memory, select_skills,
    show_operation_plan, show_subagent_lifecycle, store_openclaw_mem_service_memory,
    subagent_lifecycle_receipts_file, subagent_lifecycle_snapshot_file,
    sync_builtin_harness_skills, tool_description_hash, trace_harness_event,
    update_operation_plan_item, upsert_background_task, validate_harness_config,
    write_channel_step, write_deterministic_cron_plan, write_json_atomic,
    write_memory_search_receipt, write_memory_vector_recall_receipt, write_native_cron_plan,
    write_prompt_bundle, write_report_files, write_skill_index, write_subagent_plan,
    write_task_entity, write_turn_plan, write_windows_supervisor_plan,
};

const DEFAULT_CODEX_TIMEOUT_MS: u64 = 30 * 60 * 1000;
const DEFAULT_CODEX_IDLE_TIMEOUT_MS: u64 = 5 * 60 * 1000;
const DEFAULT_RUNTIME_SAFE_MODE_RESTART_MS: u64 = 60_000;
const DISCORD_ATTACHMENT_TEXT_EXTRACT_MAX_BYTES: usize = 16 * 1024;
const DISCORD_ATTACHMENT_DOWNLOAD_MAX_BYTES: usize =
    DEFAULT_INBOUND_MEDIA_MAX_BYTES_PER_ITEM as usize;

fn main() {
    let mut args = env::args().skip(1);
    let command = args.next().unwrap_or_else(|| "help".to_string());
    let rest: Vec<String> = args.collect();

    let result = match command.as_str() {
        "doctor" => run_doctor(&rest),
        "import-plan" => run_import_plan(&rest),
        "import-dry-run" => run_import_dry_run(&rest),
        "import-execute" => run_import_execute(&rest),
        "channel-credentials-export" => run_channel_credentials_export(&rest),
        "registry" => run_registry(&rest),
        "registry-export" => run_registry_export(&rest),
        "enable-check" => run_enable_check(&rest),
        "status" | "harness-status" => run_harness_status(&rest),
        "round7-receipt" => run_round7_receipt(&rest),
        "cron-runs" | "cron-run-list" => run_cron_runs(&rest),
        "cron-run-control" => run_cron_run_control(&rest),
        "config-validate" => run_config_validate(&rest),
        "log-rotate" => run_log_rotate(&rest),
        "queue-retry" => run_runtime_queue_control(&rest, RuntimeQueueControlAction::Retry),
        "queue-skip" => run_runtime_queue_control(&rest, RuntimeQueueControlAction::Skip),
        "healthz" => run_healthz(&rest),
        "trace" => run_trace(&rest),
        "metrics" => run_metrics(&rest),
        "supervise-evaluate" => run_supervise_evaluate(&rest),
        "deploy-canary-record" => run_deploy_canary_record(&rest),
        "queue-shadow-record" => run_queue_shadow_record(&rest),
        "queue-shadow-compare" => run_queue_shadow_compare(&rest),
        "admission-check" => run_admission_check(&rest),
        "scoped-stop" => run_scoped_stop(&rest),
        "background-list" => run_background_list(&rest),
        "background-upsert" => run_background_upsert(&rest),
        "token-efficiency" => run_token_efficiency(&rest),
        "prompt-reduction" => run_prompt_reduction(&rest),
        "task-write" => run_task_write(&rest),
        "budget-acquire" => run_budget_acquire(&rest),
        "learning-propose" => run_learning_propose(&rest),
        "drift-check" => run_drift_check(&rest),
        "vault-put" => run_vault_put(&rest),
        "vault-get" => run_vault_get(&rest),
        "mcp-request" => run_mcp_request(&rest),
        "security-scan" => run_security_scan(&rest),
        "context-pack-validate" => run_context_pack_validate(&rest),
        "tool-description-hash" => run_tool_description_hash(&rest),
        "tool-pin-check" => run_tool_pin_check(&rest),
        "invariants" => run_invariants(&rest),
        "schema-registry" => run_schema_registry(&rest),
        "release-checklist" => run_release_checklist(&rest),
        "public-hygiene" => run_public_hygiene_cli(&rest),
        "jsonl-repair" => run_jsonl_repair(&rest),
        "memory-credentials-export" => run_memory_credentials_export(&rest),
        "memory-search" => run_memory_search(&rest),
        "memory-vector-search" => run_memory_vector_search(&rest),
        "memory-canvas-run" => run_memory_canvas_run(&rest),
        "memory-embedding-backfill" => run_memory_embedding_backfill_cmd(&rest),
        "memory-hook" => run_memory_hook(&rest),
        "memory-service-status" => run_memory_service_status(&rest),
        "memory-service-recall" => run_memory_service_recall(&rest),
        "memory-service-propose" => run_memory_service_propose(&rest),
        "memory-service-store" => run_memory_service_store(&rest),
        "memory-read-path-smoke" => run_memory_read_path_smoke(&rest),
        "memory-owner-ensure" => run_memory_owner_ensure(&rest),
        "memory-owner-endpoint-probe" => run_memory_owner_endpoint_probe(&rest),
        "memory-owner-heartbeat" => run_memory_owner_heartbeat(&rest),
        "memory-owner-shadow" => run_memory_owner_shadow(&rest),
        "memory-owner-trust-scope" => run_memory_owner_trust_scope(&rest),
        "memory-owner-local-prepare" => run_memory_owner_local_prepare(&rest),
        "memory-owner-promote" => run_memory_owner_promote(&rest),
        "memory-owner-recover" => run_memory_owner_recover(&rest),
        "ops-backup" => run_ops_backup(&rest),
        "ops-cutover-request" => run_ops_cutover_request(&rest),
        "ops-cutover-approve" => run_ops_cutover_approve(&rest),
        "ops-cutover-apply" => run_ops_cutover_apply(&rest),
        "ops-cutover-status" => run_ops_cutover_status(&rest),
        "ops-cutover-receipt" => run_ops_cutover_receipt(&rest),
        "ops-control" => run_ops_control(&rest),
        "supervisor-reconcile" => run_supervisor_reconcile(&rest),
        "supervisor-run" => run_supervisor_run(&rest),
        "supervisor-plan" => run_supervisor_plan(&rest),
        "harness-skills-sync" => run_harness_skills_sync(&rest),
        "skills" => run_skills(&rest),
        "skill-propose" => run_skill_propose(&rest),
        "skill-proposals" => run_skill_proposals(&rest),
        "skill-apply" => run_skill_apply(&rest),
        "skill-reject" => run_skill_reject(&rest),
        "skill-archive" => run_skill_archive(&rest),
        "turn-plan" => run_turn_plan(&rest),
        "operation-plan" | "op-plan" => run_operation_plan(&rest),
        "latency-status" => run_latency_status(&rest),
        "channel-step" => run_channel_step(&rest),
        "channel-apply" => run_channel_apply(&rest),
        "channel-receive" => run_channel_receive(&rest),
        "channel-run-once" => run_channel_run_once(&rest),
        "channel-outbox-plan" => run_channel_outbox_plan(&rest),
        "channel-delivery-record" => run_channel_delivery_record(&rest),
        "channel-identity-check" => run_channel_identity_check(&rest),
        "media-cache-status" => run_media_cache_status(&rest),
        "progress-delivery-once" => run_progress_delivery_once(&rest),
        "progress-delivery-loop" => run_progress_delivery_loop(&rest),
        "telegram-probe" => run_telegram_probe(&rest),
        "telegram-poll-once" => run_telegram_poll_once(&rest),
        "telegram-loop" => run_telegram_loop(&rest),
        "discord-outbox-send-once" => run_discord_outbox_send_once(&rest),
        "discord-outbox-loop" => run_discord_outbox_loop(&rest),
        "discord-dm-probe" => run_discord_dm_probe(&rest),
        "discord-dm-history-probe" => run_discord_dm_history_probe(&rest),
        "discord-event-run-once" => run_discord_event_run_once(&rest),
        "discord-gateway-probe" => run_discord_gateway_probe(&rest),
        "discord-gateway-loop" => run_discord_gateway_loop(&rest),
        "plugin-sidecar-probe" => run_plugin_sidecar_probe(&rest),
        "plugin-sidecar-call" => run_plugin_sidecar_call(&rest),
        "queue-enqueue" => run_queue_enqueue(&rest),
        "queue-prepare" => run_queue_prepare(&rest),
        "runtime-run-once" => run_runtime_run_once(&rest),
        "runtime-loop" => run_runtime_loop(&rest),
        "runtime-lease-reconcile" => run_runtime_lease_reconcile(&rest),
        "worker-enqueue" => run_worker_enqueue(&rest),
        "worker-run-once" => run_worker_run_once(&rest),
        "worker-loop" => run_worker_loop(&rest),
        "worker-status" => run_worker_status(&rest),
        "worker-cancel" => run_worker_cancel(&rest),
        "worker-reap-stale" => run_worker_reap_stale(&rest),
        "codex-plan" => run_codex_plan(&rest),
        "codex-preflight" => run_codex_preflight(&rest),
        "codex-launch-probe" => run_codex_launch_probe(&rest),
        "codex-run" => run_codex_run(&rest),
        "codex-complete" => run_codex_complete(&rest),
        "prompt-bundle" => run_prompt_bundle(&rest),
        "cron-plan" => run_cron_plan(&rest),
        "native-cron-enqueue" => run_native_cron_enqueue(&rest),
        "deterministic-cron-plan" => run_deterministic_cron_plan(&rest),
        "deterministic-cron-enqueue" => run_deterministic_cron_enqueue(&rest),
        "cron-scheduler-lint" => run_cron_scheduler_lint(&rest),
        "cron-scheduler-run-once" => run_cron_scheduler_run_once(&rest),
        "cron-scheduler-loop" => run_cron_scheduler_loop(&rest),
        "context-rollover" => run_context_rollover(&rest),
        "subagent-plan" => run_subagent_plan(&rest),
        "subagent-enqueue" => run_subagent_enqueue(&rest),
        "subagent-lifecycle" => run_subagent_lifecycle(&rest),
        "help" | "-h" | "--help" => {
            print_help();
            Ok(())
        }
        unknown => Err(format!("unknown command: {unknown}")),
    };

    if let Err(error) = result {
        eprintln!("error: {error}");
        eprintln!();
        print_help();
        std::process::exit(2);
    }
}

fn run_doctor(args: &[String]) -> Result<(), String> {
    let source = source_from_args(args)?;
    println!("Source home: {}", source.home.display());
    println!("Workspace: {}", source.workspace.display());

    let inv = inventory(source).map_err(|err| err.to_string())?;
    if inv.is_empty() {
        println!("No agent source data detected at this source.");
        return Ok(());
    }

    println!("Config: {}", yes_no(inv.has_config));
    println!("Prompt files: {}", inv.prompt_files.len());
    println!("Agent directories: {}", inv.agent_dirs);
    println!("Agent config/auth/model files: {}", inv.agent_config_files);
    println!("Session indexes: {}", inv.session_indexes.len());
    println!("Transcript files: {}", inv.transcript_files);
    println!("Trajectory files: {}", inv.trajectory_files);
    println!("Codex binding mirrors: {}", inv.codex_binding_files);
    println!("Workspace skill directories: {}", inv.workspace_skill_dirs);
    println!("Managed skill directories: {}", inv.managed_skill_dirs);
    println!(
        "Project .agents skill directories: {}",
        inv.project_agent_skill_dirs
    );
    println!("Native cron jobs file: {}", yes_no(inv.native_cron_jobs));
    println!("Native cron state file: {}", yes_no(inv.native_cron_state));
    println!("Native cron run logs: {}", inv.native_cron_run_logs);
    println!("Deterministic crontabs: {}", inv.deterministic_crontabs);
    println!(
        "Deterministic cron job scripts: {}",
        inv.deterministic_cron_job_scripts
    );
    println!("Deterministic cron logs: {}", inv.deterministic_cron_logs);
    println!("Subagent state files: {}", inv.subagent_state_files);
    println!("Memory files: {}", inv.memory_files);
    println!("Memory qdrant-edge: {}", yes_no(inv.memory_qdrant_edge));
    println!("Memory LanceDB: {}", yes_no(inv.memory_lancedb));
    println!(
        "Memory openclaw-mem.sqlite: {}",
        yes_no(inv.memory_legacy_mem_sqlite)
    );
    println!(
        "Plugin install record: {}",
        yes_no(inv.plugin_install_record)
    );
    println!("Plugin state DB: {}", yes_no(inv.plugin_state_db));
    Ok(())
}

fn run_import_plan(args: &[String]) -> Result<(), String> {
    let source = source_from_args(args)?;
    let inv = inventory(source).map_err(|err| err.to_string())?;
    let plan = build_import_plan(&inv);

    println!("Import plan for {}", inv.source.home.display());
    for phase in plan.phases {
        println!();
        println!(
            "- {} [{}{}]",
            phase.name,
            format_status(&phase.status),
            if phase.required { ", required" } else { "" }
        );
        for note in phase.notes {
            println!("  {note}");
        }
    }

    Ok(())
}

fn run_import_dry_run(args: &[String]) -> Result<(), String> {
    let args = dry_run_args_from_args(args)?;
    let report = build_dry_run_report(DryRunImportOptions {
        source: args.source,
        destination_home: args.target_home,
        conflict_policy: args.conflict_policy,
    })
    .map_err(|err| err.to_string())?;

    print_dry_run_summary(&report);

    if let Some(output_dir) = args.output_dir {
        let files = write_report_files(&report, output_dir).map_err(|err| err.to_string())?;
        println!("Report JSON: {}", files.json.display());
        println!("Report summary: {}", files.summary.display());
    }

    Ok(())
}

fn run_import_execute(args: &[String]) -> Result<(), String> {
    let args = execute_args_from_args(args)?;
    let report = execute_import(ExecuteImportOptions {
        source: args.source,
        destination_home: args.target_home,
        conflict_policy: args.conflict_policy,
        include_sensitive: args.include_sensitive,
    })
    .map_err(|err| err.to_string())?;

    println!("Agent source import execute");
    println!("Target home: {}", report.destination_home.display());
    println!("Receipts file: {}", report.receipts_file.display());
    println!("Items: {}", report.summary.total_items);
    println!("Copied: {}", report.summary.copied);
    println!(
        "Backed up and copied: {}",
        report.summary.backed_up_and_copied
    );
    println!("Already matches: {}", report.summary.already_matches);
    println!("Skipped conflicts: {}", report.summary.skipped_conflicts);
    println!(
        "Skipped sensitive items: {}",
        report.summary.skipped_sensitive
    );
    println!(
        "Skipped sensitive files: {}",
        report.summary.skipped_sensitive_files
    );

    Ok(())
}

fn run_channel_credentials_export(args: &[String]) -> Result<(), String> {
    let args = channel_credentials_export_args_from_args(args)?;
    let report = export_channel_credentials(&args)?;

    println!("Agent channel credentials export");
    println!("Target env file: {}", report.env_file.display());
    println!("Receipt file: {}", report.receipt_file.display());
    println!(
        "Sensitive values written: {}",
        yes_no(args.include_sensitive)
    );
    println!("Entries: {}", report.entries.len());
    for entry in report.entries {
        println!(
            "- {}: exported={} sensitive={} length={} source={} ({})",
            entry.env_name,
            yes_no(entry.exported),
            yes_no(entry.sensitive),
            entry.length,
            entry.source_path,
            entry.reason
        );
    }

    Ok(())
}

fn run_registry(args: &[String]) -> Result<(), String> {
    let source = source_from_args(args)?;
    let registry = load_agent_registry(&source).map_err(|err| err.to_string())?;
    print_registry(&registry);
    Ok(())
}

fn run_registry_export(args: &[String]) -> Result<(), String> {
    let args = registry_export_args_from_args(args)?;
    let registry = load_agent_registry(&args.source).map_err(|err| err.to_string())?;
    let export = export_harness_registry_files(&registry, args.target_home, args.conflict_policy)
        .map_err(|err| err.to_string())?;

    println!("Harness registry export");
    println!("Wrote files: {}", yes_no(export.wrote_files));
    println!("Conflicts: {}", export.conflicts);
    println!("Registry file: {}", export.registry_file.display());
    println!("Receipts file: {}", export.receipts_file.display());
    for receipt in export.receipts {
        println!(
            "- {:?}: {:?} {} ({})",
            receipt.kind,
            receipt.status,
            receipt.path.display(),
            receipt.reason
        );
    }

    Ok(())
}

fn run_enable_check(args: &[String]) -> Result<(), String> {
    let args = enable_check_args_from_args(args)?;
    let report = check_activation_readiness(ActivationReadinessOptions {
        harness_home: args.target_home.clone(),
    })
    .map_err(|err| err.to_string())?;
    append_harness_log(
        &args.target_home,
        &HarnessLogEvent::new(
            current_log_time_ms().map_err(|err| err.to_string())?,
            if report.ready {
                HarnessLogLevel::Info
            } else {
                HarnessLogLevel::Warn
            },
            "activation",
            "activation.enable-check",
            format!(
                "ready={} passed={} warnings={} failed={}",
                yes_no(report.ready),
                report.summary.passed,
                report.summary.warnings,
                report.summary.failed
            ),
        ),
    )
    .map_err(|err| err.to_string())?;

    print_activation_readiness_report(&report);
    Ok(())
}

fn run_harness_status(args: &[String]) -> Result<(), String> {
    let args = harness_status_args_from_args(args)?;
    let report = collect_harness_status(HarnessStatusOptions {
        harness_home: args.target_home,
    })
    .map_err(|err| err.to_string())?;
    if args.json {
        let json = serde_json::to_string_pretty(&report).map_err(|err| err.to_string())?;
        println!("{json}");
    } else {
        print_harness_status_report(&report);
    }
    Ok(())
}

fn run_round7_receipt(args: &[String]) -> Result<(), String> {
    let args = harness_status_args_from_args(args)?;
    let harness_home = args.target_home.clone();
    let report = collect_harness_status(HarnessStatusOptions {
        harness_home: harness_home.clone(),
    })
    .map_err(|err| err.to_string())?;
    let now_ms = current_log_time_ms().map_err(|err| err.to_string())?;
    let latest_cutover_receipt = latest_ops_cutover_receipt(&harness_home);
    let live_cutover_performed = latest_cutover_receipt
        .as_ref()
        .and_then(|receipt| receipt.get("ready"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
        || latest_cutover_receipt
            .as_ref()
            .and_then(|receipt| receipt.get("status"))
            .and_then(serde_json::Value::as_str)
            == Some("ready");
    let receipt_dir = harness_home.join("state").join("round7");
    fs::create_dir_all(&receipt_dir).map_err(|err| err.to_string())?;
    let receipt_path = receipt_dir.join(format!("round7-receipt-{now_ms}.json"));
    let receipt = serde_json::json!({
        "schema": "agent-harness.round7-receipt.v1",
        "receiptPurpose": "support-plane-evidence",
        "createdAtMs": now_ms,
        "harnessHome": harness_home,
        "liveCutoverPerformed": live_cutover_performed,
        "liveCutoverRequired": !live_cutover_performed,
        "liveCutoverStatusSource": "state/cutover/cutover-receipts.jsonl",
        "latestOpsCutoverReceipt": latest_cutover_receipt,
        "activeRoots": {
            "harnessHome": report.harness_home,
            "memoryDir": report.memory.memory_dir,
        },
        "readiness": {
            "ready": report.ready,
            "summary": report.readiness.summary,
        },
        "openclawMem": {
            "supportPlane": report.memory.support_plane,
            "summary": report.memory.summary,
        },
        "runtime": {
            "openItems": report.runtime.open_items,
            "latestNonIdleRunOnce": report.runtime.latest_non_idle_run_once,
        },
        "warnings": report.warnings,
        "externalCutoverNotes": [
            "Build/use a fresh agent-harness binary outside the live gateway session.",
            "Regenerate supervisor scripts only during the operator-approved cutover.",
            "Do not rewrite historical .codex-app-server.json session metadata; treat stale legacy cwd entries as historical evidence.",
            "After cutover, rerun status and compare memory.supportPlane against this receipt."
        ]
    });
    let text = serde_json::to_string_pretty(&receipt).map_err(|err| err.to_string())?;
    fs::write(&receipt_path, format!("{text}\n")).map_err(|err| err.to_string())?;
    if args.json {
        println!("{text}");
    } else {
        println!("Round7 receipt: {}", receipt_path.display());
    }
    Ok(())
}

fn latest_ops_cutover_receipt(harness_home: &Path) -> Option<serde_json::Value> {
    let path = harness_home
        .join("state")
        .join("cutover")
        .join("cutover-receipts.jsonl");
    fs::read_to_string(path)
        .ok()?
        .lines()
        .rev()
        .find_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
}

fn run_cron_runs(args: &[String]) -> Result<(), String> {
    let options = SimpleOptions::parse(
        args,
        "cron-runs",
        &["--agent-id", "--entry-id", "--status", "--limit"],
        &[],
    )?;
    let report = list_cron_runs(CronRunListOptions {
        harness_home: options.target_home.clone(),
        agent_id: options.optional("--agent-id").map(ToString::to_string),
        entry_id: options.optional("--entry-id").map(ToString::to_string),
        status: options.optional("--status").map(ToString::to_string),
        limit: options.optional_usize("--limit")?.unwrap_or(50),
    })
    .map_err(|err| err.to_string())?;
    print_json(&report)
}

fn run_cron_run_control(args: &[String]) -> Result<(), String> {
    let options = SimpleOptions::parse(
        args,
        "cron-run-control",
        &[
            "--action",
            "--run-id",
            "--agent-id",
            "--entry-id",
            "--reason",
        ],
        &[],
    )?;
    let action = options
        .required("--action")?
        .parse::<CronRunControlAction>()?;
    let reason = options
        .optional("--reason")
        .unwrap_or("manual operator request")
        .to_string();
    let report = control_cron_run(CronRunControlOptions {
        harness_home: options.target_home.clone(),
        action,
        run_id: options.optional("--run-id").map(ToString::to_string),
        agent_id: options.optional("--agent-id").map(ToString::to_string),
        entry_id: options.optional("--entry-id").map(ToString::to_string),
        reason,
        now_ms: current_log_time_ms().map_err(|err| err.to_string())?,
    })
    .map_err(|err| err.to_string())?;
    print_json(&report)
}

fn run_config_validate(args: &[String]) -> Result<(), String> {
    let options = SimpleOptions::parse(args, "config-validate", &[], &[])?;
    let report = validate_harness_config(&options.target_home).map_err(|err| err.to_string())?;
    print_json(&report)?;
    if report.is_valid() {
        Ok(())
    } else {
        Err("harness config validation failed".to_string())
    }
}

fn run_log_rotate(args: &[String]) -> Result<(), String> {
    let options =
        SimpleOptions::parse(args, "log-rotate", &["--max-bytes", "--max-archives"], &[])?;
    let max_bytes = options
        .optional_u64("--max-bytes")?
        .unwrap_or(10 * 1024 * 1024);
    let max_archives = options.optional_usize("--max-archives")?.unwrap_or(7);
    let report = rotate_harness_log_if_needed(HarnessLogRotationOptions {
        harness_home: options.target_home,
        max_bytes,
        max_archives,
        now_ms: current_time_ms()?,
    })
    .map_err(|err| err.to_string())?;
    print_json(&report)
}

fn run_runtime_queue_control(
    args: &[String],
    action: RuntimeQueueControlAction,
) -> Result<(), String> {
    let options = SimpleOptions::parse(args, "queue-control", &["--queue-id", "--reason"], &[])?;
    let queue_id = options.required("--queue-id")?;
    let reason = options
        .optional("--reason")
        .unwrap_or("manual operator request")
        .to_string();
    let report = control_runtime_queue_item(RuntimeQueueControlOptions {
        harness_home: options.target_home,
        queue_id,
        action,
        reason,
        now_ms: current_time_ms()?,
    })
    .map_err(|err| err.to_string())?;
    print_json(&report)
}

fn run_healthz(args: &[String]) -> Result<(), String> {
    let options = SimpleOptions::parse(
        args,
        "healthz",
        &["--loop-stale-ms"],
        &["--require-writable-state"],
    )?;
    let report = collect_healthz(HealthzOptions {
        harness_home: options.target_home.clone(),
        now_ms: current_time_ms()?,
        loop_stale_ms: options.optional_i64("--loop-stale-ms")?.unwrap_or(120_000),
        require_writable_state: options.has_flag("--require-writable-state"),
    })
    .map_err(|err| err.to_string())?;
    print_json(&report)?;
    if report.ready && report.live {
        Ok(())
    } else {
        Err("healthz is not ready/live".to_string())
    }
}

fn run_trace(args: &[String]) -> Result<(), String> {
    let options = SimpleOptions::parse(args, "trace", &["--id"], &[])?;
    let report = trace_harness_event(TraceOptions {
        harness_home: options.target_home.clone(),
        id: options.required("--id")?,
    })
    .map_err(|err| err.to_string())?;
    print_json(&report)
}

fn run_metrics(args: &[String]) -> Result<(), String> {
    let options = SimpleOptions::parse(args, "metrics", &[], &[])?;
    let report = collect_harness_metrics(HarnessMetricsOptions {
        harness_home: options.target_home,
    })
    .map_err(|err| err.to_string())?;
    print_json(&report)
}

fn run_supervise_evaluate(args: &[String]) -> Result<(), String> {
    let options = SimpleOptions::parse(
        args,
        "supervise-evaluate",
        &["--run-id", "--states-file"],
        &[],
    )?;
    let states = if let Some(path) = options.optional("--states-file") {
        serde_json::from_str::<Vec<SupervisorChildState>>(&read_text_file(path)?)
            .map_err(|err| format!("--states-file must be a supervisor state array: {err}"))?
    } else {
        Vec::new()
    };
    let report = evaluate_supervisor_children(SupervisionEvaluateOptions {
        harness_home: options.target_home.clone(),
        run_id: options
            .optional("--run-id")
            .map(ToString::to_string)
            .unwrap_or_else(|| "manual-evaluation".to_string()),
        now_ms: current_time_ms()?,
        specs: default_supervisor_child_specs(),
        states,
    })
    .map_err(|err| err.to_string())?;
    print_json(&report)
}

fn run_deploy_canary_record(args: &[String]) -> Result<(), String> {
    let options = SimpleOptions::parse(
        args,
        "deploy-canary-record",
        &[
            "--current-binary",
            "--candidate-binary",
            "--fake-canary-passed",
            "--live-canary-passed",
        ],
        &[],
    )?;
    let live_canary_passed = options
        .optional("--live-canary-passed")
        .map(|value| parse_bool_value(value, "--live-canary-passed"))
        .transpose()?;
    let report = record_supervise_deploy_canary(SuperviseDeployCanaryOptions {
        harness_home: options.target_home.clone(),
        current_binary: PathBuf::from(options.required("--current-binary")?),
        candidate_binary: PathBuf::from(options.required("--candidate-binary")?),
        fake_canary_passed: parse_bool_value(
            &options.required("--fake-canary-passed")?,
            "--fake-canary-passed",
        )?,
        live_canary_passed,
        now_ms: current_time_ms()?,
    })
    .map_err(|err| err.to_string())?;
    print_json(&report)
}

fn run_queue_shadow_record(args: &[String]) -> Result<(), String> {
    let options = SimpleOptions::parse(
        args,
        "queue-shadow-record",
        &["--queue-id", "--item-json", "--item-file"],
        &[],
    )?;
    let item = options.json_input("--item-json", "--item-file")?;
    let report = record_channel_turn_shadow(QueueShadowRecordOptions {
        harness_home: options.target_home.clone(),
        queue_id: options.required("--queue-id")?,
        item,
        now_ms: current_time_ms()?,
    })
    .map_err(|err| err.to_string())?;
    print_json(&report)
}

fn run_queue_shadow_compare(args: &[String]) -> Result<(), String> {
    let options = SimpleOptions::parse(args, "queue-shadow-compare", &[], &[])?;
    let report = compare_channel_turn_shadow(QueueShadowCompareOptions {
        harness_home: options.target_home,
    })
    .map_err(|err| err.to_string())?;
    print_json(&report)?;
    if report.divergences.is_empty() {
        Ok(())
    } else {
        Err("queue shadow divergences found".to_string())
    }
}

fn run_admission_check(args: &[String]) -> Result<(), String> {
    let options = SimpleOptions::parse(
        args,
        "admission-check",
        &["--queue-open-items", "--queue-depth-limit"],
        &["--provider-backpressure"],
    )?;
    let report = evaluate_admission(AdmissionDecisionOptions {
        harness_home: options.target_home.clone(),
        queue_open_items: options.required_usize("--queue-open-items")?,
        queue_depth_limit: options.required_usize("--queue-depth-limit")?,
        provider_backpressure: options.has_flag("--provider-backpressure"),
        now_ms: current_time_ms()?,
    })
    .map_err(|err| err.to_string())?;
    print_json(&report)?;
    if report.accepted {
        Ok(())
    } else {
        Err(report.reason)
    }
}

fn run_scoped_stop(args: &[String]) -> Result<(), String> {
    let options = SimpleOptions::parse(
        args,
        "scoped-stop",
        &["--target-kind", "--target-id", "--reason"],
        &[],
    )?;
    let target_kind = options.required("--target-kind")?;
    let target_id = options.optional("--target-id").map(ToString::to_string);
    let target = match target_kind.as_str() {
        "turn" => ScopedStopTarget::Turn {
            session_key: target_id.ok_or_else(|| "--target-id is required for turn".to_string())?,
        },
        "queue-item" => ScopedStopTarget::QueueItem {
            queue_id: target_id
                .ok_or_else(|| "--target-id is required for queue-item".to_string())?,
        },
        "worker-job" => ScopedStopTarget::WorkerJob {
            job_id: target_id
                .ok_or_else(|| "--target-id is required for worker-job".to_string())?,
        },
        "all-jobs" => ScopedStopTarget::AllJobs {
            agent_id: target_id,
        },
        other => {
            return Err(format!(
                "unsupported --target-kind {other}; expected turn, queue-item, worker-job, all-jobs"
            ));
        }
    };
    let receipt = record_scoped_stop(ScopedStopOptions {
        harness_home: options.target_home.clone(),
        target,
        reason: options
            .optional("--reason")
            .unwrap_or("manual operator scoped stop")
            .to_string(),
        now_ms: current_time_ms()?,
    })
    .map_err(|err| err.to_string())?;
    print_json(&receipt)
}

fn run_background_list(args: &[String]) -> Result<(), String> {
    let options = SimpleOptions::parse(args, "background-list", &[], &[])?;
    let report = list_background_tasks(BackgroundTaskListOptions {
        harness_home: options.target_home,
        now_ms: current_time_ms()?,
    })
    .map_err(|err| err.to_string())?;
    print_json(&report)
}

fn run_background_upsert(args: &[String]) -> Result<(), String> {
    let options = SimpleOptions::parse(
        args,
        "background-upsert",
        &["--task-json", "--task-file"],
        &[],
    )?;
    let task = serde_json::from_value::<BackgroundTaskRecord>(
        options.json_input("--task-json", "--task-file")?,
    )
    .map_err(|err| format!("background task JSON is invalid: {err}"))?;
    let report = upsert_background_task(BackgroundTaskUpsertOptions {
        harness_home: options.target_home,
        task,
    })
    .map_err(|err| err.to_string())?;
    print_json(&report)
}

fn run_token_efficiency(args: &[String]) -> Result<(), String> {
    let options = SimpleOptions::parse(args, "token-efficiency", &[], &[])?;
    let report = collect_token_efficiency(TokenEfficiencyOptions {
        harness_home: options.target_home,
    })
    .map_err(|err| err.to_string())?;
    print_json(&report)
}

fn run_prompt_reduction(args: &[String]) -> Result<(), String> {
    let options = SimpleOptions::parse(
        args,
        "prompt-reduction",
        &[
            "--first",
            "--first-file",
            "--second",
            "--second-file",
            "--min-reduction-percent",
        ],
        &[],
    )?;
    let report = evaluate_prompt_reduction(PromptReductionOptions {
        first_prompt: options.text_input("--first", "--first-file")?,
        second_prompt: options.text_input("--second", "--second-file")?,
        min_reduction_percent: options
            .optional_u64("--min-reduction-percent")?
            .unwrap_or(20)
            .try_into()
            .map_err(|_| "--min-reduction-percent must fit in u8".to_string())?,
    });
    print_json(&report)?;
    if report.passed {
        Ok(())
    } else {
        Err("prompt reduction gate failed".to_string())
    }
}

fn run_task_write(args: &[String]) -> Result<(), String> {
    let options = SimpleOptions::parse(
        args,
        "task-write",
        &["--task-id", "--title", "--owner", "--status", "--trace-id"],
        &[],
    )?;
    let task = write_task_entity(TaskEntityOptions {
        harness_home: options.target_home.clone(),
        task_id: options.required("--task-id")?,
        title: options.required("--title")?,
        owner: options.required("--owner")?,
        status: parse_task_status(options.optional("--status").unwrap_or("open"))?,
        trace_id: options.optional("--trace-id").map(ToString::to_string),
        now_ms: current_time_ms()?,
    })
    .map_err(|err| err.to_string())?;
    print_json(&task)
}

fn run_operation_plan(args: &[String]) -> Result<(), String> {
    let options = SimpleOptions::parse(
        args,
        "operation-plan",
        &[
            "--action",
            "--plan-id",
            "--origin-queue-id",
            "--session-key",
            "--agent",
            "--agent-id",
            "--goal",
            "--acceptance",
            "--constraints",
            "--max-open-items",
            "--max-fanout",
            "--item-id",
            "--title",
            "--body",
            "--body-file",
            "--depends-on",
            "--status",
            "--expected-version",
            "--assignee",
            "--worker-job-id",
            "--queue-id",
            "--risk",
            "--evidence",
            "--add-evidence",
            "--idempotency-key",
            "--reason",
            "--author",
            "--now-ms",
        ],
        &["--replace-evidence"],
    )?;
    let now_ms = options
        .optional_i64("--now-ms")?
        .unwrap_or(current_time_ms()?);
    match options.optional("--action").unwrap_or("list") {
        "list" => {
            let plans = list_operation_plans(options.target_home).map_err(|err| err.to_string())?;
            print_json(&plans)
        }
        "create" => {
            let report = create_operation_plan(CreateOperationPlanOptions {
                harness_home: options.target_home.clone(),
                plan_id: options.required("--plan-id")?,
                origin_queue_id: options
                    .optional("--origin-queue-id")
                    .map(ToString::to_string),
                session_key: options
                    .optional("--session-key")
                    .unwrap_or("manual")
                    .to_string(),
                agent_id: options
                    .optional("--agent")
                    .or_else(|| options.optional("--agent-id"))
                    .unwrap_or("main")
                    .to_string(),
                goal: options.required("--goal")?,
                acceptance_criteria: options.optional("--acceptance").map(ToString::to_string),
                constraints: options.optional("--constraints").map(ToString::to_string),
                max_open_items: options.optional_usize("--max-open-items")?,
                max_fanout: options.optional_usize("--max-fanout")?,
                now_ms,
            })
            .map_err(|err| err.to_string())?;
            print_json(&report)
        }
        "show" => {
            let report = show_operation_plan(OperationPlanShowOptions {
                harness_home: options.target_home.clone(),
                plan_id: options.required("--plan-id")?,
            })
            .map_err(|err| err.to_string())?;
            print_json(&report)
        }
        "add-item" => {
            let report = add_operation_plan_item(OperationPlanAddItemOptions {
                harness_home: options.target_home.clone(),
                plan_id: options.required("--plan-id")?,
                item_id: options.required("--item-id")?,
                title: options.required("--title")?,
                body: options.text_input("--body", "--body-file")?,
                depends_on: cli_list_values(&options, "--depends-on"),
                acceptance_criteria: options.optional("--acceptance").map(ToString::to_string),
                risk: options.optional("--risk").map(ToString::to_string),
                now_ms,
            })
            .map_err(|err| err.to_string())?;
            print_json(&report)
        }
        "update-item" => {
            let depends_on = if options.values.contains_key("--depends-on") {
                Some(cli_list_values(&options, "--depends-on"))
            } else {
                None
            };
            let evidence = if options.values.contains_key("--evidence") {
                Some(cli_list_values(&options, "--evidence"))
            } else {
                None
            };
            let status = options
                .optional("--status")
                .map(parse_operation_plan_item_status)
                .transpose()?;
            let report = update_operation_plan_item(OperationPlanUpdateItemOptions {
                harness_home: options.target_home.clone(),
                plan_id: options.required("--plan-id")?,
                item_id: options.required("--item-id")?,
                expected_item_version: options.optional_u64("--expected-version")?,
                status,
                title: options.optional("--title").map(ToString::to_string),
                body: options.optional_text_input("--body", "--body-file")?,
                depends_on,
                assignee: options.optional("--assignee").map(ToString::to_string),
                worker_job_id: options.optional("--worker-job-id").map(ToString::to_string),
                queue_id: options.optional("--queue-id").map(ToString::to_string),
                risk: options.optional("--risk").map(ToString::to_string),
                evidence,
                replace_evidence: options.has_flag("--replace-evidence"),
                add_evidence: cli_list_values(&options, "--add-evidence"),
                now_ms,
            })
            .map_err(|err| err.to_string())?;
            print_json(&report)
        }
        "delegate" => {
            let report = delegate_operation_plan_item(OperationPlanDelegateItemOptions {
                harness_home: options.target_home.clone(),
                plan_id: options.required("--plan-id")?,
                item_id: options.required("--item-id")?,
                expected_item_version: options.optional_u64("--expected-version")?,
                idempotency_key: options.required("--idempotency-key")?,
                assignee: options.required("--assignee")?,
                worker_job_id: options.optional("--worker-job-id").map(ToString::to_string),
                queue_id: options.optional("--queue-id").map(ToString::to_string),
                now_ms,
            })
            .map_err(|err| err.to_string())?;
            print_json(&report)
        }
        "promote" | "promote-dependencies" => {
            let report = promote_operation_plan_items_from_dependencies(
                OperationPlanPromoteDependenciesOptions {
                    harness_home: options.target_home.clone(),
                    plan_id: options.required("--plan-id")?,
                    now_ms,
                },
            )
            .map_err(|err| err.to_string())?;
            print_json(&report)
        }
        "comment" => {
            let report = comment_on_operation_plan(OperationPlanCommentOptions {
                harness_home: options.target_home.clone(),
                plan_id: options.required("--plan-id")?,
                author: options.optional("--author").map(ToString::to_string),
                body: options.text_input("--body", "--body-file")?,
                now_ms,
            })
            .map_err(|err| err.to_string())?;
            print_json(&report)
        }
        "block" => {
            let report = block_operation_plan(OperationPlanBlockOptions {
                harness_home: options.target_home.clone(),
                plan_id: options.required("--plan-id")?,
                reason: options.optional("--reason").map(ToString::to_string),
                now_ms,
            })
            .map_err(|err| err.to_string())?;
            print_json(&report)
        }
        "complete" => {
            let report = complete_operation_plan(OperationPlanCompleteOptions {
                harness_home: options.target_home.clone(),
                plan_id: options.required("--plan-id")?,
                reason: options.optional("--reason").map(ToString::to_string),
                now_ms,
            })
            .map_err(|err| err.to_string())?;
            print_json(&report)
        }
        other => Err(format!(
            "operation-plan: unknown --action {other}; expected list, create, show, add-item, update-item, delegate, promote, comment, block, or complete"
        )),
    }
}

fn run_latency_status(args: &[String]) -> Result<(), String> {
    let options = SimpleOptions::parse(args, "latency-status", &["--queue-id"], &[])?;
    let queue_id = options.required("--queue-id")?;
    let receipts_file = agent_harness_core::latency::latency_receipts_file(&options.target_home);
    let receipt = agent_harness_core::latency::read_latest_queue_receipt(&receipts_file, &queue_id)
        .map_err(|err| err.to_string())?;
    let found = receipt.is_some();
    let summary = receipt.as_ref().map(|receipt| {
        agent_harness_core::latency::latency_summary(
            receipt,
            agent_harness_core::latency::default_latency_stages(),
        )
    });
    let report = serde_json::json!({
        "schema": "agent-harness.latency-status.v1",
        "harnessHome": options.target_home,
        "queueId": queue_id,
        "receiptsFile": receipts_file,
        "found": found,
        "receipt": receipt,
        "summary": summary,
    });
    print_json(&report)
}

fn run_budget_acquire(args: &[String]) -> Result<(), String> {
    let options = SimpleOptions::parse(
        args,
        "budget-acquire",
        &["--scope", "--limit", "--amount"],
        &[],
    )?;
    let report = acquire_budget(BudgetAcquireOptions {
        harness_home: options.target_home.clone(),
        scope: options.required("--scope")?,
        limit: options.required_i64("--limit")?,
        amount: options.required_i64("--amount")?,
        now_ms: current_time_ms()?,
    })
    .map_err(|err| err.to_string())?;
    print_json(&report)?;
    if report.accepted {
        Ok(())
    } else {
        Err(report.reason)
    }
}

fn run_learning_propose(args: &[String]) -> Result<(), String> {
    let options = SimpleOptions::parse(
        args,
        "learning-propose",
        &[
            "--proposal-id",
            "--target",
            "--content",
            "--content-file",
            "--context",
            "--context-file",
        ],
        &["--auto-apply"],
    )?;
    let report = create_learning_proposal(LearningProposalOptions {
        harness_home: options.target_home.clone(),
        proposal_id: options.required("--proposal-id")?,
        target: options.required("--target")?,
        content: options.text_input("--content", "--content-file")?,
        context: options
            .optional_text_input("--context", "--context-file")?
            .unwrap_or_default(),
        auto_apply: options.has_flag("--auto-apply"),
        now_ms: current_time_ms()?,
    })
    .map_err(|err| err.to_string())?;
    print_json(&report)
}

fn run_drift_check(args: &[String]) -> Result<(), String> {
    let options = SimpleOptions::parse(
        args,
        "drift-check",
        &[
            "--intended-json",
            "--intended-file",
            "--active-json",
            "--active-file",
        ],
        &[],
    )?;
    let report = check_config_drift(DriftCheckOptions {
        harness_home: options.target_home.clone(),
        intended: options.json_input("--intended-json", "--intended-file")?,
        active: options.json_input("--active-json", "--active-file")?,
    });
    print_json(&report)?;
    if report.drifted {
        Err("configuration drift detected".to_string())
    } else {
        Ok(())
    }
}

fn run_vault_put(args: &[String]) -> Result<(), String> {
    let options = SimpleOptions::parse(
        args,
        "vault-put",
        &[
            "--vault-file",
            "--passphrase",
            "--name",
            "--secret",
            "--secret-file",
        ],
        &[],
    )?;
    let name = options.required("--name")?;
    let secret = options
        .text_input("--secret", "--secret-file")?
        .into_bytes();
    let vault_file = options.vault_file();
    let vault = put_vault_secret(VaultPutOptions {
        vault_file: vault_file.clone(),
        passphrase: vault_passphrase(&options)?,
        name: name.clone(),
        secret,
    })
    .map_err(|err| err.to_string())?;
    let names: Vec<_> = vault
        .records
        .iter()
        .map(|record| record.name.clone())
        .collect();
    print_json(&serde_json::json!({
        "schema": "agent-harness.vault-put-cli.v1",
        "vaultFile": vault_file,
        "name": name,
        "recordCount": vault.records.len(),
        "records": names
    }))
}

fn run_vault_get(args: &[String]) -> Result<(), String> {
    let options = SimpleOptions::parse(
        args,
        "vault-get",
        &["--vault-file", "--passphrase", "--name"],
        &[],
    )?;
    let name = options.required("--name")?;
    let vault_file = options.vault_file();
    let secret = get_vault_secret(VaultGetOptions {
        vault_file: vault_file.clone(),
        passphrase: vault_passphrase(&options)?,
        name: name.clone(),
    })
    .map_err(|err| err.to_string())?;
    let secret_bytes = secret.as_ref().map(Vec::len);
    print_json(&serde_json::json!({
        "schema": "agent-harness.vault-get-cli.v1",
        "vaultFile": vault_file,
        "name": name,
        "found": secret.is_some(),
        "secretBytes": secret_bytes
    }))
}

fn run_mcp_request(args: &[String]) -> Result<(), String> {
    let options = SimpleOptions::parse(
        args,
        "mcp-request",
        &["--request-json", "--request-file", "--allow-tool"],
        &[],
    )?;
    let request = options.json_input("--request-json", "--request-file")?;
    let allowed_tools = options.values("--allow-tool").into_iter().collect();
    let (response, receipt) = handle_mcp_request(McpRequestOptions {
        request,
        allowed_tools,
        harness_home: Some(options.target_home.clone()),
    });
    print_json(&serde_json::json!({
        "schema": "agent-harness.mcp-cli-response.v1",
        "response": response,
        "receipt": receipt
    }))
}

fn run_media_cache_status(args: &[String]) -> Result<(), String> {
    let options = SimpleOptions::parse(args, "media-cache-status", &[], &[])?;
    let report =
        collect_inbound_media_cache_report(&options.target_home).map_err(|err| err.to_string())?;
    print_json(&report)
}

fn run_security_scan(args: &[String]) -> Result<(), String> {
    let options = SimpleOptions::parse(
        args,
        "security-scan",
        &["--text", "--text-file", "--shell-path", "--allowed-root"],
        &[],
    )?;
    let text = options
        .optional_text_input("--text", "--text-file")?
        .unwrap_or_default();
    let report = scan_security_boundaries(SecurityScanOptions {
        text,
        shell_path: options.optional("--shell-path").map(PathBuf::from),
        allowed_roots: options
            .values("--allowed-root")
            .into_iter()
            .map(PathBuf::from)
            .collect(),
    })
    .map_err(|err| err.to_string())?;
    print_json(&report)
}

fn run_context_pack_validate(args: &[String]) -> Result<(), String> {
    let options = SimpleOptions::parse(
        args,
        "context-pack-validate",
        &["--pack-json", "--pack-file", "--max-bytes"],
        &[],
    )?;
    let report = parse_context_pack(ContextPackParseOptions {
        raw_json: options.text_input("--pack-json", "--pack-file")?,
        max_bytes: options.optional_usize("--max-bytes")?.unwrap_or(64 * 1024),
    });
    print_json(&report)?;
    if report.accepted {
        Ok(())
    } else {
        Err("context pack validation failed".to_string())
    }
}

fn run_tool_description_hash(args: &[String]) -> Result<(), String> {
    let options = SimpleOptions::parse(
        args,
        "tool-description-hash",
        &["--description", "--description-file"],
        &[],
    )?;
    let description = options.text_input("--description", "--description-file")?;
    print_json(&serde_json::json!({
        "schema": "agent-harness.tool-description-hash-cli.v1",
        "hash": tool_description_hash(&description)
    }))
}

fn run_tool_pin_check(args: &[String]) -> Result<(), String> {
    let options = SimpleOptions::parse(
        args,
        "tool-pin-check",
        &[
            "--tool",
            "--description",
            "--description-file",
            "--expected-hash",
        ],
        &[],
    )?;
    let description = options.text_input("--description", "--description-file")?;
    let report = check_tool_description_pin(
        options.required("--tool")?,
        &description,
        options.required("--expected-hash")?,
    );
    print_json(&report)?;
    if report.matched {
        Ok(())
    } else {
        Err("tool description pin mismatch".to_string())
    }
}

fn run_invariants(args: &[String]) -> Result<(), String> {
    SimpleOptions::parse(args, "invariants", &[], &[])?;
    print_json(&invariant_catalog())
}

fn run_schema_registry(args: &[String]) -> Result<(), String> {
    SimpleOptions::parse(args, "schema-registry", &[], &[])?;
    print_json(&schema_registry_entries())
}

fn run_release_checklist(args: &[String]) -> Result<(), String> {
    SimpleOptions::parse(args, "release-checklist", &[], &[])?;
    print_json(&release_checklist())
}

fn run_public_hygiene_cli(args: &[String]) -> Result<(), String> {
    let options = SimpleOptions::parse(args, "public-hygiene", &["--root"], &[])?;
    let root = options
        .optional("--root")
        .map(PathBuf::from)
        .unwrap_or_else(|| env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));
    let report =
        run_public_hygiene(PublicHygieneOptions { root }).map_err(|err| err.to_string())?;
    print_json(&report)?;
    if report.passed {
        Ok(())
    } else {
        Err("public hygiene scan found forbidden paths".to_string())
    }
}

fn run_jsonl_repair(args: &[String]) -> Result<(), String> {
    let args = jsonl_repair_args_from_args(args)?;
    let report = repair_jsonl_file(&args)?;
    println!("Agent JSONL repair");
    println!("Path: {}", report.path.display());
    println!("Output: {}", report.output.display());
    println!("Invalid output: {}", report.invalid_output.display());
    if let Some(backup) = &report.backup {
        println!("Backup: {}", backup.display());
    }
    println!(
        "Lines: total={} valid={} output={} recoveredLines={} recoveredValues={} invalid={}",
        report.total_lines,
        report.valid_lines,
        report.output_lines,
        report.recovered_lines,
        report.recovered_values,
        report.invalid_lines
    );
    println!("Applied: {}", yes_no(report.applied));
    Ok(())
}

fn run_memory_search(args: &[String]) -> Result<(), String> {
    let args = memory_search_args_from_args(args)?;
    let report = search_imported_memory(MemorySearchOptions {
        harness_home: args.target_home.clone(),
        query: args.query,
        limit: args.limit,
        max_file_bytes: args.max_file_bytes,
    })
    .map_err(|err| err.to_string())?;
    if args.write_receipt {
        write_memory_search_receipt(&report).map_err(|err| err.to_string())?;
    }
    append_harness_log(
        &args.target_home,
        &HarnessLogEvent::new(
            current_log_time_ms().map_err(|err| err.to_string())?,
            if report.status.as_str() == "ready" {
                HarnessLogLevel::Info
            } else {
                HarnessLogLevel::Warn
            },
            "memory",
            "memory.search",
            format!(
                "status={} hits={} searchedFiles={} skippedFiles={}",
                report.status.as_str(),
                report.hits.len(),
                report.searched_files,
                report.skipped_files
            ),
        ),
    )
    .map_err(|err| err.to_string())?;
    if args.json {
        print_memory_search_report_json(&report)?;
    } else {
        print_memory_search_report(&report);
    }
    if report.status.as_str() == "ready" {
        Ok(())
    } else {
        Err(report.reason)
    }
}

fn run_memory_credentials_export(args: &[String]) -> Result<(), String> {
    let args = memory_credentials_export_args_from_args(args)?;
    let report = export_memory_credentials(MemoryCredentialsExportOptions {
        source_home: args.source_home,
        harness_home: args.target_home.clone(),
        include_sensitive: args.include_sensitive,
    })
    .map_err(|err| err.to_string())?;
    append_harness_log(
        &args.target_home,
        &HarnessLogEvent::new(
            current_log_time_ms().map_err(|err| err.to_string())?,
            HarnessLogLevel::Info,
            "memory",
            "memory.credentials-export",
            format!(
                "entries={} includeSensitive={} envFile={}",
                report.entries.len(),
                yes_no(args.include_sensitive),
                report.env_file.display()
            ),
        ),
    )
    .map_err(|err| err.to_string())?;
    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|err| err.to_string())?
        );
    } else {
        print_memory_credentials_export_report(&report, args.include_sensitive);
    }
    Ok(())
}

fn run_memory_vector_search(args: &[String]) -> Result<(), String> {
    let args = memory_vector_search_args_from_args(args)?;
    let report = search_imported_vector_memory(MemoryVectorRecallOptions {
        harness_home: args.target_home.clone(),
        query: args.query,
        limit: args.limit,
    })
    .map_err(|err| err.to_string())?;
    if args.write_receipt {
        write_memory_vector_recall_receipt(&report).map_err(|err| err.to_string())?;
    }
    append_harness_log(
        &args.target_home,
        &HarnessLogEvent::new(
            current_log_time_ms().map_err(|err| err.to_string())?,
            if matches!(
                report.status,
                MemoryVectorRecallStatus::Ready | MemoryVectorRecallStatus::NoHits
            ) {
                HarnessLogLevel::Info
            } else {
                HarnessLogLevel::Warn
            },
            "memory",
            "memory.vector-search",
            format!(
                "status={:?} hits={} backend={} dim={}",
                report.status,
                report.hits.len(),
                report.backend,
                report.query_embedding_dim
            ),
        ),
    )
    .map_err(|err| err.to_string())?;
    if args.json {
        print_memory_vector_report_json(&report)?;
    } else {
        print_memory_vector_report(&report);
    }
    match report.status {
        MemoryVectorRecallStatus::Ready | MemoryVectorRecallStatus::NoHits => Ok(()),
        MemoryVectorRecallStatus::Skipped | MemoryVectorRecallStatus::Failed => Err(report.reason),
    }
}

fn run_memory_canvas_run(args: &[String]) -> Result<(), String> {
    let args = memory_canvas_run_args_from_args(args)?;
    let report = run_memory_canvas_worker(MemoryCanvasWorkerOptions {
        harness_home: args.target_home.clone(),
        agent_id: args.agent_id.clone(),
        now_ms: current_log_time_ms().map_err(|err| err.to_string())?,
    })
    .map_err(|err| err.to_string())?;
    append_harness_log(
        &args.target_home,
        &HarnessLogEvent::new(
            current_log_time_ms().map_err(|err| err.to_string())?,
            if report.status == MemoryCanvasWorkerStatus::Failed {
                HarnessLogLevel::Warn
            } else {
                HarnessLogLevel::Info
            },
            "memory",
            "memory.canvas-run",
            format!(
                "status={:?} candidates={} episodes={}",
                report.status, report.candidates_read, report.episodes_read
            ),
        ),
    )
    .map_err(|err| err.to_string())?;
    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|err| err.to_string())?
        );
    } else {
        print_memory_canvas_report(&report);
    }
    if report.status == MemoryCanvasWorkerStatus::Failed {
        Err(report.reason)
    } else {
        Ok(())
    }
}

fn run_memory_embedding_backfill_cmd(args: &[String]) -> Result<(), String> {
    let args = memory_embedding_backfill_args_from_args(args)?;
    let report = run_memory_embedding_backfill(MemoryEmbeddingBackfillOptions {
        harness_home: args.target_home.clone(),
        lane: args.lane,
        model: args.model,
        vector_dimension: args.vector_dimension,
        batch_size: args.batch_size,
        max_items: args.max_items,
        rate_limit_per_minute: args.rate_limit_per_minute,
        retry_cap: args.retry_cap,
        coverage_threshold_bps: args.coverage_threshold_bps,
        now_ms: current_log_time_ms().map_err(|err| err.to_string())?,
    })
    .map_err(|err| err.to_string())?;
    append_harness_log(
        &args.target_home,
        &HarnessLogEvent::new(
            current_log_time_ms().map_err(|err| err.to_string())?,
            if report.status == "blocked" || report.status == "dimension-mismatch" {
                HarnessLogLevel::Warn
            } else {
                HarnessLogLevel::Info
            },
            "memory",
            "memory.embedding-backfill",
            format!(
                "status={} lane={} selected={} coverageBefore={:?}",
                report.status,
                report.lane.as_str(),
                report.selected_item_ids.len(),
                report.coverage_before_bps
            ),
        ),
    )
    .map_err(|err| err.to_string())?;
    if args.json {
        print_json(&report)
    } else {
        print_memory_embedding_backfill_report(&report);
        Ok(())
    }
}

fn run_memory_hook(args: &[String]) -> Result<(), String> {
    let args = memory_hook_args_from_args(args)?;
    let report = run_memory_hook_adapter(MemoryHookAdapterOptions {
        harness_home: args.target_home.clone(),
        hook: args.hook,
        agent_id: args.agent_id,
        session_key: args.session_key,
        query: args.query,
        prompt_bundle_json: args.prompt_bundle_json,
        assistant_text: args.assistant_text,
        success: args.success,
        slot: args.slot,
        operation: args.operation,
        payload: args.payload,
        now_ms: args.now_ms,
        limit: args.limit,
        max_file_bytes: args.max_file_bytes,
    })
    .map_err(|err| err.to_string())?;
    append_harness_log(
        &args.target_home,
        &HarnessLogEvent::new(
            current_log_time_ms().map_err(|err| err.to_string())?,
            HarnessLogLevel::Info,
            "memory",
            "memory.hook",
            format!("hook={} status={:?}", args.hook.as_str(), report.status),
        ),
    )
    .map_err(|err| err.to_string())?;
    print_json(&report)
}

fn run_memory_service_status(args: &[String]) -> Result<(), String> {
    let args = memory_service_status_args_from_args(args)?;
    let report = inspect_openclaw_mem_service(OpenClawMemServiceStatusOptions {
        harness_home: args.target_home.clone(),
        agent_id: args.agent_id,
    })
    .map_err(|err| err.to_string())?;
    append_harness_log(
        &args.target_home,
        &HarnessLogEvent::new(
            current_log_time_ms().map_err(|err| err.to_string())?,
            if report.status == OpenClawMemServiceStatus::Blocked {
                HarnessLogLevel::Warn
            } else {
                HarnessLogLevel::Info
            },
            "memory",
            "memory.openclaw-mem-service.status",
            format!(
                "status={:?} mode={} qdrantEdgeMode={}",
                report.status, report.service_mode, report.qdrant_edge_mode
            ),
        ),
    )
    .map_err(|err| err.to_string())?;
    if args.json {
        print_json(&report)?;
    } else {
        println!("OpenClaw memory service status");
        println!("Harness home: {}", report.harness_home.display());
        println!(
            "Agent: {}",
            report.agent_id.as_deref().unwrap_or("(global)")
        );
        println!("Status: {:?}", report.status);
        println!("Mode: {}", report.service_mode);
        println!("Active slot owner: {}", report.active_slot_owner);
        println!("Recall provider: {}", report.recall_provider);
        println!("Retrieval backend: {}", report.retrieval_backend);
        println!(
            "Bridge: reachable={} latencyMs={} timeouts={} lastReceipt={} lastError={}",
            yes_no(report.bridge_reachable.unwrap_or(false)),
            display_opt_u64(report.bridge_latency_ms),
            report.bridge_timeouts,
            report.last_mem_engine_receipt_id.as_deref().unwrap_or("-"),
            report.last_mem_engine_error_code.as_deref().unwrap_or("-")
        );
        println!(
            "Fallback: used={} backend={} reason={}",
            report.fallback_used.map(yes_no).unwrap_or("unknown"),
            report.fallback_backend.as_deref().unwrap_or("-"),
            report.fallback_reason.as_deref().unwrap_or("-")
        );
        println!("Policy source: {}", report.policy_source);
        println!("Qdrant edge mode: {}", report.qdrant_edge_mode);
        println!(
            "Credential bridge: apiKeyPresent={} model={} baseUrl={} keyLength={}",
            report.credential_bridge.api_key_present,
            report.credential_bridge.model,
            report.credential_bridge.base_url,
            report.credential_bridge.api_key_length
        );
        let direct_cli_targets = report
            .credential_bridge
            .direct_cli_env_mappings
            .iter()
            .map(|mapping| format!("{}->{}", mapping.source_env, mapping.target_env))
            .collect::<Vec<_>>()
            .join(", ");
        println!(
            "Direct CLI bridge: required={} mappings=[{}]",
            yes_no(report.credential_bridge.direct_cli_env_bridge_required),
            direct_cli_targets
        );
        println!(
            "Embedding coverage: observations={}/{} episodic={}/{} docs={}/{}",
            display_opt_u64(report.embedding_coverage.observation_embeddings),
            display_opt_u64(report.embedding_coverage.observations),
            display_opt_u64(report.embedding_coverage.episodic_event_embeddings),
            display_opt_u64(report.embedding_coverage.episodic_events),
            display_opt_u64(report.embedding_coverage.docs_embeddings),
            display_opt_u64(report.embedding_coverage.docs_chunks)
        );
        println!(
            "Scope policy: {} crossAgentPrivateRecallAllowed={}",
            report.scope_policy.default_scope,
            report.scope_policy.cross_agent_private_recall_allowed
        );
        println!("Trust policy: {}", report.trust_policy.mode);
        println!(
            "Graph readiness: {} readyForAutonomousMatch={}",
            report.graph_readiness.verdict, report.graph_readiness.ready_for_autonomous_match
        );
        println!("Mem-engine canary: {}", report.mem_engine_canary.status);
        if let Some(path) = &report.qdrant_edge_dir {
            println!("Qdrant edge: {}", path.display());
        }
        if let Some(path) = &report.sqlite_database {
            println!("SQLite snapshot: {}", path.display());
        }
        println!("Store file: {}", report.agent_store_file.display());
        println!("Reason: {}", report.reason);
        for warning in &report.warnings {
            println!("Warning: {warning}");
        }
    }
    if report.status == OpenClawMemServiceStatus::Blocked {
        Err(report.reason)
    } else {
        Ok(())
    }
}

fn run_memory_service_recall(args: &[String]) -> Result<(), String> {
    let args = memory_service_recall_args_from_args(args)?;
    let report = recall_openclaw_mem_service(OpenClawMemServiceRecallOptions {
        harness_home: args.target_home.clone(),
        agent_id: args.agent_id,
        query: args.query,
        limit: args.limit,
        max_file_bytes: args.max_file_bytes,
    })
    .map_err(|err| err.to_string())?;
    append_harness_log(
        &args.target_home,
        &HarnessLogEvent::new(
            current_log_time_ms().map_err(|err| err.to_string())?,
            HarnessLogLevel::Info,
            "memory",
            "memory.openclaw-mem-service.recall",
            format!(
                "status={:?} hits={} provider={} backend={} fallbackUsed={}",
                report.status,
                report.hit_count,
                report.recall_provider,
                report.backend,
                report.fallback_used
            ),
        ),
    )
    .map_err(|err| err.to_string())?;
    if args.json {
        print_json(&report)?;
    } else {
        println!("OpenClaw memory service recall");
        println!("Harness home: {}", report.harness_home.display());
        println!(
            "Agent: {}",
            report.agent_id.as_deref().unwrap_or("(global)")
        );
        println!("Status: {:?}", report.status);
        println!("Provider: {}", report.recall_provider);
        println!("Backend: {}", report.backend);
        println!("Retrieval backend: {}", report.retrieval_backend);
        println!(
            "Fallback: used={} backend={} reason={}",
            yes_no(report.fallback_used),
            report.fallback_backend.as_deref().unwrap_or("-"),
            report.fallback_reason.as_deref().unwrap_or("-")
        );
        println!(
            "Bridge: reachable={} latencyMs={} lastReceipt={} lastError={}",
            yes_no(report.bridge_reachable),
            display_opt_u64(report.bridge_latency_ms),
            report.last_mem_engine_receipt_id.as_deref().unwrap_or("-"),
            report.last_mem_engine_error_code.as_deref().unwrap_or("-")
        );
        println!(
            "Writes: performed={} canonicalAllowed={}",
            yes_no(report.writes_performed),
            report
                .canonical_writes_allowed
                .map(yes_no)
                .unwrap_or("unknown")
        );
        println!("Policy source: {}", report.policy_source);
        println!("Hits: {}", report.hit_count);
        println!("Reason: {}", report.reason);
        for hit in &report.hits {
            println!(
                "- [{}] {:.4} {} :: {}",
                hit.lane, hit.score, hit.title, hit.text
            );
        }
        for warning in &report.warnings {
            println!("Warning: {warning}");
        }
    }
    Ok(())
}

fn run_memory_service_propose(args: &[String]) -> Result<(), String> {
    let args = memory_service_propose_args_from_args(args)?;
    let report = propose_openclaw_mem_service_memory(OpenClawMemServiceProposeOptions {
        harness_home: args.target_home.clone(),
        agent_id: args.agent_id,
        session_key: args.session_key,
        text: args.text,
        payload: args.payload,
        now_ms: current_time_ms()?,
    })
    .map_err(|err| err.to_string())?;
    append_harness_log(
        &args.target_home,
        &HarnessLogEvent::new(
            current_log_time_ms().map_err(|err| err.to_string())?,
            HarnessLogLevel::Info,
            "memory",
            "memory.openclaw-mem-service.propose",
            format!(
                "status={:?} proposalId={}",
                report.status,
                report.proposal_id.as_deref().unwrap_or("-")
            ),
        ),
    )
    .map_err(|err| err.to_string())?;
    if args.json {
        print_json(&report)
    } else {
        println!("OpenClaw memory service proposal");
        println!("Status: {:?}", report.status);
        println!("Reason: {}", report.reason);
        println!("Proposal file: {}", report.proposal_file.display());
        if let Some(id) = &report.proposal_id {
            println!("Proposal id: {id}");
        }
        Ok(())
    }
}

fn run_memory_service_store(args: &[String]) -> Result<(), String> {
    let args = memory_service_store_args_from_args(args)?;
    let report = store_openclaw_mem_service_memory(OpenClawMemServiceStoreOptions {
        harness_home: args.target_home.clone(),
        agent_id: args.agent_id,
        session_key: args.session_key,
        text: args.text,
        payload: args.payload,
        approved: args.approved,
        now_ms: current_time_ms()?,
    })
    .map_err(|err| err.to_string())?;
    append_harness_log(
        &args.target_home,
        &HarnessLogEvent::new(
            current_log_time_ms().map_err(|err| err.to_string())?,
            if args.approved {
                HarnessLogLevel::Info
            } else {
                HarnessLogLevel::Warn
            },
            "memory",
            "memory.openclaw-mem-service.store",
            format!(
                "status={:?} storeId={}",
                report.status,
                report.store_id.as_deref().unwrap_or("-")
            ),
        ),
    )
    .map_err(|err| err.to_string())?;
    if args.json {
        print_json(&report)?;
    } else {
        println!("OpenClaw memory service store");
        println!("Status: {:?}", report.status);
        println!("Reason: {}", report.reason);
        println!("Store file: {}", report.store_file.display());
        if let Some(id) = &report.store_id {
            println!("Store id: {id}");
        }
    }
    Ok(())
}

fn run_ops_backup(args: &[String]) -> Result<(), String> {
    let args = ops_backup_args_from_args(args)?;
    let report = create_ops_backup(OpsBackupOptions {
        harness_home: args.target_home,
        label: args.label,
        max_file_bytes: args.max_file_bytes,
        now_ms: current_time_ms()?,
    })
    .map_err(|err| err.to_string())?;
    if args.summary_only {
        print_json(&serde_json::json!({
            "schema": "agent-harness.ops-backup-summary.v1",
            "harnessHome": report.harness_home,
            "backupDir": report.backup_dir,
            "manifestFile": report.manifest_file,
            "copiedFiles": report.copied_files,
            "skippedFiles": report.skipped_files,
            "bytesCopied": report.bytes_copied,
            "warnings": report.warnings,
        }))
    } else {
        print_json(&report)
    }
}

fn run_ops_cutover_request(args: &[String]) -> Result<(), String> {
    let args = ops_cutover_request_args_from_args(args)?;
    let report = record_ops_cutover_request(OpsCutoverRequestOptions {
        harness_home: args.target_home,
        action: args.action,
        summary: args.summary,
        candidate_binary: args.candidate_binary,
        staging_home: args.staging_home,
        test_notes: args.test_notes,
        now_ms: current_time_ms()?,
    })
    .map_err(|err| err.to_string())?;
    print_json(&report)
}

fn run_ops_cutover_approve(args: &[String]) -> Result<(), String> {
    let args = ops_cutover_approve_args_from_args(args)?;
    let report = record_ops_cutover_approval(OpsCutoverApproveOptions {
        harness_home: args.target_home,
        ticket_id: args.ticket_id,
        action: args.action,
        issued_to: args.issued_to,
        ttl_seconds: args.ttl_seconds,
        reason: args.reason,
        now_ms: current_time_ms()?,
    })
    .map_err(|err| err.to_string())?;
    print_json(&report)
}

fn run_ops_cutover_apply(args: &[String]) -> Result<(), String> {
    let args = ops_cutover_apply_args_from_args(args)?;
    let report = record_ops_cutover_apply(OpsCutoverApplyOptions {
        harness_home: args.target_home,
        ticket_id: args.ticket_id,
        action: args.action,
        live_control_token: args.live_control_token,
        note: args.note,
        now_ms: current_time_ms()?,
    })
    .map_err(|err| err.to_string())?;
    print_json(&report)
}

fn run_ops_cutover_status(args: &[String]) -> Result<(), String> {
    let args = ops_cutover_status_args_from_args(args)?;
    let report = collect_ops_cutover_status(OpsCutoverStatusOptions {
        harness_home: args.target_home,
        action: args.action,
        live_control_token: args.live_control_token,
        now_ms: current_time_ms()?,
    })
    .map_err(|err| err.to_string())?;
    print_json(&report)
}

fn run_ops_cutover_receipt(args: &[String]) -> Result<(), String> {
    let args = ops_cutover_receipt_args_from_args(args)?;
    let report = record_ops_cutover_receipt(OpsCutoverReceiptOptions {
        harness_home: args.target_home,
        note: args.note,
        now_ms: current_time_ms()?,
    })
    .map_err(|err| err.to_string())?;
    print_json(&report)
}

fn run_ops_control(args: &[String]) -> Result<(), String> {
    let args = ops_control_args_from_args(args)?;
    let report = record_ops_control(OpsControlOptions {
        harness_home: args.target_home,
        action: args.action,
        reason: args.reason,
        live_control_token: args.live_control_token,
        now_ms: current_time_ms()?,
    })
    .map_err(|err| err.to_string())?;
    print_json(&report)
}

fn run_supervisor_plan(args: &[String]) -> Result<(), String> {
    let args = supervisor_plan_args_from_args(args)?;
    let report = write_windows_supervisor_plan(WindowsSupervisorPlanOptions {
        harness_home: args.target_home,
        source_home: args.source_home,
        workspace: args.workspace,
        runtime_workspace: args.runtime_workspace,
        harness_cli: args.harness_cli,
        codex_executable: args.codex_exe,
        node_executable: args.node_exe,
        discord_gateway_script: args.gateway_script,
        agent_id: args.agent_id,
        output_dir: args.output_dir,
        task_prefix: args.task_prefix,
        include_runtime: args.include_runtime,
        runtime_workers: args.runtime_workers,
        include_worker: args.include_worker,
        include_cron_scheduler: args.include_cron_scheduler,
        include_progress: args.include_progress,
        include_telegram: args.include_telegram,
        include_discord: args.include_discord,
        idle_ms: args.idle_ms,
        runtime_timeout_ms: args.runtime_timeout_ms,
        runtime_idle_timeout_ms: args.runtime_idle_timeout_ms,
        max_consecutive_errors: args.max_consecutive_errors,
        telegram_poll_timeout_seconds: args.telegram_poll_timeout_seconds,
        telegram_max_updates: args.telegram_max_updates,
        telegram_outbox_limit: args.telegram_outbox_limit,
    })
    .map_err(|err| err.to_string())?;

    print_windows_supervisor_plan_report(&report);
    Ok(())
}

fn run_harness_skills_sync(args: &[String]) -> Result<(), String> {
    let args = harness_skills_sync_args_from_args(args)?;
    let report = sync_builtin_harness_skills(BuiltinHarnessSkillSyncOptions {
        harness_home: args.target_home,
        force: args.force,
    })
    .map_err(|err| err.to_string())?;

    print_builtin_harness_skill_sync_report(&report);
    Ok(())
}

fn run_skills(args: &[String]) -> Result<(), String> {
    let args = skills_args_from_args(args)?;
    let index = match &args.harness_home {
        Some(harness_home) => build_harness_skill_index(harness_home),
        None => build_source_skill_index(&args.source),
    }
    .map_err(|err| err.to_string())?;

    print_skill_index(&index);

    if let Some(output_dir) = args.output_dir {
        let file = write_skill_index(&index, output_dir).map_err(|err| err.to_string())?;
        println!("Skill index JSON: {}", file.json.display());
    }

    if let Some(query) = args.query {
        let selections = select_skills(
            &index,
            &SkillSelectionQuery {
                text: query,
                agent_id: args.agent_id,
                channel: args.channel,
                workspace: args.match_workspace,
                agent_mode: None,
                available_tools: Vec::new(),
                available_toolsets: Vec::new(),
                fts_enabled: false,
                vector_tie_break_enabled: false,
                limit: args.limit,
            },
        );
        println!();
        println!("Matched skills: {}", selections.len());
        for selection in selections {
            println!(
                "- {} [{:?}] score={} mode={} checksum={} title={}",
                selection.skill_id,
                selection.source_kind,
                selection.score,
                selection.delivery_mode.as_str(),
                selection.body_checksum,
                selection.title
            );
            if !selection.reasons.is_empty() {
                println!("  {}", selection.reasons.join("; "));
            }
            println!("  {}", selection.directory.display());
        }
    }

    Ok(())
}

fn run_skill_propose(args: &[String]) -> Result<(), String> {
    let mut harness_home = None;
    let mut skill_id = None;
    let mut target_path = None;
    let mut operation = SkillLearningProposalOperation::Replace;
    let mut body = None;
    let mut diff = None;
    let mut signal = None;
    let mut source_turn = None;
    let mut risk_class = "low".to_string();
    let mut status = SkillLearningProposalStatus::Proposed;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                harness_home = Some(parse_harness_home_path(args, i, flag)?);
            }
            "--skill" | "--target-skill" => {
                i += 1;
                skill_id = Some(required_arg(args, i, "--skill")?.to_string());
            }
            "--target-path" => {
                i += 1;
                target_path = Some(PathBuf::from(required_arg(args, i, "--target-path")?));
            }
            "--operation" => {
                i += 1;
                operation = parse_skill_operation(required_arg(args, i, "--operation")?)?;
            }
            "--body" => {
                i += 1;
                body = Some(required_arg(args, i, "--body")?.to_string());
            }
            "--body-file" => {
                i += 1;
                body = Some(
                    fs::read_to_string(required_arg(args, i, "--body-file")?)
                        .map_err(|err| err.to_string())?,
                );
            }
            "--diff" => {
                i += 1;
                diff = Some(required_arg(args, i, "--diff")?.to_string());
            }
            "--signal" => {
                i += 1;
                signal = Some(required_arg(args, i, "--signal")?.to_string());
            }
            "--source-turn" => {
                i += 1;
                source_turn = Some(required_arg(args, i, "--source-turn")?.to_string());
            }
            "--risk" | "--risk-class" => {
                i += 1;
                risk_class = required_arg(args, i, "--risk")?.to_string();
            }
            "--quarantine" | "--quarantined" => {
                status = SkillLearningProposalStatus::Quarantined;
            }
            other => return Err(format!("unknown skill-propose arg: {other}")),
        }
        i += 1;
    }
    let harness_home = harness_home.ok_or_else(|| "--harness-home is required".to_string())?;
    let skill_id = skill_id.ok_or_else(|| "--skill is required".to_string())?;
    let target_path =
        target_path.unwrap_or_else(|| resolve_skill_target_path(&harness_home, &skill_id));
    let proposal = create_skill_learning_proposal(SkillProposeOptions {
        harness_home,
        target_skill_id: skill_id,
        target_path,
        operation,
        replacement_body: body,
        support_files: Vec::new(),
        diff,
        signals: signal
            .map(|text| SkillLearningSignal {
                kind: "operator-command".to_string(),
                signal_hash: tool_description_hash(&text),
                text,
                trust: Some("operator".to_string()),
            })
            .into_iter()
            .collect(),
        source_turn,
        risk_class,
        status,
        now_ms: current_log_time_ms().map_err(|err| err.to_string())?,
    })
    .map_err(|err| err.to_string())?;
    println!("Skill proposal: {}", proposal.proposal_id);
    println!("Status: {}", proposal.status.as_str());
    println!("Target: {}", proposal.target_path.display());
    Ok(())
}

fn run_skill_proposals(args: &[String]) -> Result<(), String> {
    let harness_home = parse_required_harness_home(args)?;
    let report = list_skill_proposals(SkillProposalListOptions { harness_home })
        .map_err(|err| err.to_string())?;
    println!("Skill proposals: {}", report.proposals.len());
    println!("File: {}", report.proposals_file.display());
    for proposal in report.proposals {
        println!(
            "- {} {} {} {:?} {}",
            proposal.proposal_id,
            proposal.status.as_str(),
            proposal.target_skill_id,
            proposal.operation,
            proposal.target_path.display()
        );
    }
    Ok(())
}

fn run_skill_apply(args: &[String]) -> Result<(), String> {
    let mut harness_home = None;
    let mut proposal_id = None;
    let mut operator = None;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                harness_home = Some(parse_harness_home_path(args, i, flag)?);
            }
            "--proposal" => {
                i += 1;
                proposal_id = Some(required_arg(args, i, "--proposal")?.to_string());
            }
            "--operator" => {
                i += 1;
                operator = Some(required_arg(args, i, "--operator")?.to_string());
            }
            other => return Err(format!("unknown skill-apply arg: {other}")),
        }
        i += 1;
    }
    let report = apply_skill_proposal(SkillApplyOptions {
        harness_home: harness_home.ok_or_else(|| "--harness-home is required".to_string())?,
        proposal_id: proposal_id.ok_or_else(|| "--proposal is required".to_string())?,
        operator,
        now_ms: current_log_time_ms().map_err(|err| err.to_string())?,
    })
    .map_err(|err| err.to_string())?;
    println!("Skill apply: {}", report.status.as_str());
    println!("Reason: {}", report.reason);
    if let Some(target) = report.target_path {
        println!("Target: {}", target.display());
    }
    if let Some(backup) = report.backup_dir {
        println!("Backup: {}", backup.display());
    }
    Ok(())
}

fn run_skill_reject(args: &[String]) -> Result<(), String> {
    let mut harness_home = None;
    let mut proposal_id = None;
    let mut reason = "rejected by operator".to_string();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                harness_home = Some(parse_harness_home_path(args, i, flag)?);
            }
            "--proposal" => {
                i += 1;
                proposal_id = Some(required_arg(args, i, "--proposal")?.to_string());
            }
            "--reason" => {
                i += 1;
                reason = required_arg(args, i, "--reason")?.to_string();
            }
            other => return Err(format!("unknown skill-reject arg: {other}")),
        }
        i += 1;
    }
    let report = reject_skill_proposal(SkillProposalActionOptions {
        harness_home: harness_home.ok_or_else(|| "--harness-home is required".to_string())?,
        proposal_id: proposal_id.ok_or_else(|| "--proposal is required".to_string())?,
        reason,
        now_ms: current_log_time_ms().map_err(|err| err.to_string())?,
    })
    .map_err(|err| err.to_string())?;
    println!("Skill reject: {:?}", report.status);
    println!("Reason: {}", report.reason);
    Ok(())
}

fn run_skill_archive(args: &[String]) -> Result<(), String> {
    let mut harness_home = None;
    let mut skill_id = None;
    let mut target_path = None;
    let mut reason = None;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                harness_home = Some(parse_harness_home_path(args, i, flag)?);
            }
            "--skill" => {
                i += 1;
                skill_id = Some(required_arg(args, i, "--skill")?.to_string());
            }
            "--target-path" => {
                i += 1;
                target_path = Some(PathBuf::from(required_arg(args, i, "--target-path")?));
            }
            "--reason" => {
                i += 1;
                reason = Some(required_arg(args, i, "--reason")?.to_string());
            }
            other => return Err(format!("unknown skill-archive arg: {other}")),
        }
        i += 1;
    }
    let harness_home = harness_home.ok_or_else(|| "--harness-home is required".to_string())?;
    let skill_id = skill_id.ok_or_else(|| "--skill is required".to_string())?;
    let proposal = create_skill_archive_proposal(SkillArchiveOptions {
        target_path: target_path
            .unwrap_or_else(|| resolve_skill_target_path(&harness_home, &skill_id)),
        harness_home,
        target_skill_id: skill_id,
        reason: reason.ok_or_else(|| "--reason is required".to_string())?,
        now_ms: current_log_time_ms().map_err(|err| err.to_string())?,
    })
    .map_err(|err| err.to_string())?;
    println!("Skill archive proposal: {}", proposal.proposal_id);
    println!("Target: {}", proposal.target_path.display());
    Ok(())
}

fn run_turn_plan(args: &[String]) -> Result<(), String> {
    let args = turn_plan_args_from_args(args)?;
    let registry = load_agent_registry(&args.source).map_err(|err| err.to_string())?;
    let skill_index = match &args.harness_home {
        Some(harness_home) => build_runtime_skill_index(&args.source, harness_home),
        None => build_source_skill_index(&args.source),
    }
    .map_err(|err| err.to_string())?;
    let plan = build_turn_plan(
        &args.source,
        &registry,
        &skill_index,
        TurnPlanInput {
            harness_home: args.harness_home.clone(),
            platform: args.platform,
            channel_id: args.channel_id,
            user_id: args.user_id,
            text: args.message,
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            requested_agent_id: args.agent_id,
            session_hint: args.session_key,
            skill_limit: args.skill_limit,
        },
    )
    .map_err(|err| err.to_string())?;

    print_turn_plan(&plan);

    if let Some(output_dir) = args.output_dir {
        let file = write_turn_plan(&plan, output_dir).map_err(|err| err.to_string())?;
        println!("Turn plan JSON: {}", file.json.display());
    }

    Ok(())
}

fn run_channel_step(args: &[String]) -> Result<(), String> {
    let args = turn_plan_args_from_args(args)?;
    let registry = load_agent_registry(&args.source).map_err(|err| err.to_string())?;
    let skill_index = match &args.harness_home {
        Some(harness_home) => build_runtime_skill_index(&args.source, harness_home),
        None => build_source_skill_index(&args.source),
    }
    .map_err(|err| err.to_string())?;
    let plan = build_turn_plan(
        &args.source,
        &registry,
        &skill_index,
        TurnPlanInput {
            harness_home: args.harness_home.clone(),
            platform: args.platform,
            channel_id: args.channel_id,
            user_id: args.user_id,
            text: args.message,
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            requested_agent_id: args.agent_id,
            session_hint: args.session_key,
            skill_limit: args.skill_limit,
        },
    )
    .map_err(|err| err.to_string())?;
    let step = build_channel_step(&registry, &plan);

    print_channel_step(&step);

    if let Some(output_dir) = args.output_dir {
        let file = write_channel_step(&step, output_dir).map_err(|err| err.to_string())?;
        println!("Channel step JSON: {}", file.json.display());
    }

    Ok(())
}

fn run_channel_apply(args: &[String]) -> Result<(), String> {
    let args = queue_enqueue_args_from_args(args)?;
    let registry = load_agent_registry(&args.turn.source).map_err(|err| err.to_string())?;
    let skill_index = build_runtime_skill_index(&args.turn.source, &args.target_home)
        .map_err(|err| err.to_string())?;
    let plan = build_turn_plan(
        &args.turn.source,
        &registry,
        &skill_index,
        TurnPlanInput {
            harness_home: Some(args.target_home.clone()),
            platform: args.turn.platform,
            channel_id: args.turn.channel_id,
            user_id: args.turn.user_id,
            text: args.turn.message,
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            requested_agent_id: args.turn.agent_id,
            session_hint: args.turn.session_key,
            skill_limit: args.turn.skill_limit,
        },
    )
    .map_err(|err| err.to_string())?;
    let step = build_channel_step(&registry, &plan);
    let report = apply_channel_command_step(
        &step,
        ChannelCommandApplyOptions {
            harness_home: args.target_home,
            now_ms: args.now_ms,
        },
    )
    .map_err(|err| err.to_string())?;

    print_channel_command_apply_report(&report);
    Ok(())
}

fn run_channel_receive(args: &[String]) -> Result<(), String> {
    let args = queue_enqueue_args_from_args(args)?;
    let skill_index = build_runtime_skill_index(&args.turn.source, &args.target_home)
        .map_err(|err| err.to_string())?;
    let report = receive_channel_message(ChannelReceiveOptions {
        source: args.turn.source,
        runtime_workspace: None,
        harness_home: args.target_home,
        skill_index,
        platform: args.turn.platform,
        account_id: None,
        channel_id: args.turn.channel_id,
        user_id: args.turn.user_id,
        agent_id: args.turn.agent_id,
        session_key: args.turn.session_key,
        message: args.turn.message,
        inbound_context: None,
        inbound_media_artifacts: Vec::new(),
        skill_limit: args.turn.skill_limit,
        now_ms: args.now_ms,
    })
    .map_err(|err| err.to_string())?;

    print_channel_receive_report(&report);
    Ok(())
}

fn run_channel_run_once(args: &[String]) -> Result<(), String> {
    let args = channel_run_once_args_from_args(args)?;
    let report = run_channel_once(ChannelRunOnceOptions {
        source: args.turn.source,
        runtime_workspace: args.runtime_workspace,
        harness_home: args.target_home.clone(),
        platform: args.turn.platform,
        account_id: None,
        channel_id: args.turn.channel_id,
        user_id: args.turn.user_id,
        agent_id: args.turn.agent_id,
        session_key: args.turn.session_key,
        message: args.turn.message,
        inbound_context: None,
        inbound_media_artifacts: Vec::new(),
        skill_limit: args.turn.skill_limit,
        now_ms: args.now_ms,
        codex_executable: args.codex_exe,
        timeout_ms: args.timeout_ms,
        idle_timeout_ms: args.idle_timeout_ms,
        prompt_options: PromptAssemblyOptions {
            max_prompt_file_bytes: args.turn.max_prompt_file_bytes,
            max_skill_file_bytes: args.turn.max_skill_file_bytes,
            harness_home: Some(args.target_home),
            ..PromptAssemblyOptions::default()
        },
        outbox_limit: args.outbox_limit,
        run_runtime: true,
    })
    .map_err(|err| err.to_string())?;

    print_channel_run_once_report(&report);
    Ok(())
}

fn run_channel_outbox_plan(args: &[String]) -> Result<(), String> {
    let args = channel_outbox_plan_args_from_args(args)?;
    let report = plan_channel_outbox(ChannelOutboxPlanOptions {
        harness_home: args.target_home,
        platform: args.platform,
        limit: args.limit,
    })
    .map_err(|err| err.to_string())?;

    print_channel_outbox_plan_report(&report);
    Ok(())
}

fn run_channel_delivery_record(args: &[String]) -> Result<(), String> {
    let args = channel_delivery_record_args_from_args(args)?;
    let receipt = record_channel_delivery(ChannelDeliveryRecordOptions {
        harness_home: args.target_home,
        delivery_id: args.delivery_id,
        status: args.status,
        platform: args.platform,
        account_id: None,
        channel_id: args.channel_id,
        user_id: args.user_id,
        session_key: args.session_key,
        provider_message_id: args.provider_message_id,
        error: args.error,
        now_ms: args.now_ms,
    })
    .map_err(|err| err.to_string())?;

    print_channel_delivery_receipt(&receipt);
    Ok(())
}

fn run_channel_identity_check(args: &[String]) -> Result<(), String> {
    let args = channel_identity_check_args_from_args(args)?;
    let report = resolve_channel_identity(ChannelIdentityLookup {
        harness_home: args.target_home,
        platform: args.platform,
        account_id: args.account_id,
        chat_id: args.chat_id,
        thread_id: args.thread_id,
        requested_agent_id: args.agent_id,
    })
    .map_err(|err| err.to_string())?;
    if args.json {
        print_json(&report)
    } else {
        println!("Status: {:?}", report.status);
        println!("Reason: {}", report.reason);
        if let Some(agent_id) = report.agent_id {
            println!("Agent: {agent_id}");
        }
        if let Some(secret_ref) = report.secret_ref {
            println!("Secret: {secret_ref}");
        }
        for warning in report.warnings {
            println!("Warning: {warning}");
        }
        if report.status == ChannelIdentityResolutionStatus::Bound {
            Ok(())
        } else {
            Err("channel identity check failed closed".to_string())
        }
    }
}

fn run_memory_read_path_smoke(args: &[String]) -> Result<(), String> {
    let args = memory_read_path_smoke_args_from_args(args)?;
    let report = run_openclaw_mem_read_path_smoke(OpenClawMemReadPathSmokeOptions {
        harness_home: args.target_home.clone(),
        agent_id: args.agent_id,
        query: args.query,
        limit: args.limit,
    })
    .map_err(|err| err.to_string())?;
    append_harness_log(
        &args.target_home,
        &HarnessLogEvent::new(
            current_log_time_ms().map_err(|err| err.to_string())?,
            if report.status == OpenClawMemServiceStatus::Blocked {
                HarnessLogLevel::Warn
            } else {
                HarnessLogLevel::Info
            },
            "memory",
            "memory.openclaw-mem-read-path-smoke",
            format!(
                "status={:?} recall={:?} provider={} backend={} fallbackUsed={} bom={} noBom={} embedding={}",
                report.status,
                report.recall_report.status,
                report.recall_report.recall_provider,
                report.recall_report.backend,
                report.recall_report.fallback_used,
                report.bom_jsonl_smoke_ok,
                report.no_bom_jsonl_smoke_ok,
                report.embedding_smoke_status
            ),
        ),
    )
    .map_err(|err| err.to_string())?;
    if args.json {
        print_json(&report)
    } else {
        println!("OpenClaw memory read-path smoke");
        println!("Harness home: {}", report.harness_home.display());
        println!(
            "Agent: {}",
            report.agent_id.as_deref().unwrap_or("(global)")
        );
        println!("Status: {:?}", report.status);
        println!("Recall: {:?}", report.recall_report.status);
        println!("Recall provider: {}", report.recall_report.recall_provider);
        println!(
            "Retrieval backend: {}",
            report.recall_report.retrieval_backend
        );
        println!(
            "Fallback: used={} backend={} reason={}",
            yes_no(report.recall_report.fallback_used),
            report
                .recall_report
                .fallback_backend
                .as_deref()
                .unwrap_or("-"),
            report
                .recall_report
                .fallback_reason
                .as_deref()
                .unwrap_or("-")
        );
        println!(
            "Bridge: reachable={} lastReceipt={} lastError={}",
            yes_no(report.recall_report.bridge_reachable),
            report
                .recall_report
                .last_mem_engine_receipt_id
                .as_deref()
                .unwrap_or("-"),
            report
                .recall_report
                .last_mem_engine_error_code
                .as_deref()
                .unwrap_or("-")
        );
        println!(
            "Writes performed: {}",
            yes_no(report.recall_report.writes_performed)
        );
        println!("Embedding smoke: {}", report.embedding_smoke_status);
        println!("BOM JSONL smoke: {}", report.bom_jsonl_smoke_ok);
        println!("No-BOM JSONL smoke: {}", report.no_bom_jsonl_smoke_ok);
        println!("Scope/trust smoke: {}", report.scope_trust_smoke_ok);
        if !report.scope_trust_smoke_findings.is_empty() {
            println!(
                "Scope/trust findings: {}",
                report.scope_trust_smoke_findings.join(", ")
            );
        }
        println!(
            "Graph verdict: {}",
            report.status_report.graph_readiness.verdict
        );
        println!(
            "Mem-engine canary: {}",
            report.status_report.mem_engine_canary.status
        );
        for warning in &report.warnings {
            println!("Warning: {warning}");
        }
        Ok(())
    }
}

fn run_memory_owner_ensure(args: &[String]) -> Result<(), String> {
    let options = SimpleOptions::parse(args, "memory-owner-ensure", &[], &["--json"])?;
    let report = ensure_memory_owner_state(MemoryOwnerEnsureOptions {
        harness_home: options.target_home,
        now_ms: current_time_ms()?,
    })
    .map_err(|err| err.to_string())?;
    print_json(&report)
}

fn run_memory_owner_endpoint_probe(args: &[String]) -> Result<(), String> {
    let options = SimpleOptions::parse(
        args,
        "memory-owner-endpoint-probe",
        &["--endpoint", "--observed-contract"],
        &["--json"],
    )?;
    let report = record_memory_owner_endpoint_probe(MemoryOwnerEndpointProbeOptions {
        harness_home: options.target_home.clone(),
        endpoint: options.optional("--endpoint").map(ToString::to_string),
        observed_contract: options
            .optional("--observed-contract")
            .map(ToString::to_string),
        now_ms: current_time_ms()?,
    })
    .map_err(|err| err.to_string())?;
    print_json(&report)
}

fn run_memory_owner_heartbeat(args: &[String]) -> Result<(), String> {
    let options = SimpleOptions::parse(
        args,
        "memory-owner-heartbeat",
        &["--lease-id", "--lease-ttl-ms"],
        &["--json"],
    )?;
    let report = record_memory_owner_heartbeat(MemoryOwnerHeartbeatOptions {
        harness_home: options.target_home.clone(),
        lease_id: options.required("--lease-id")?,
        lease_ttl_ms: options
            .optional_i64("--lease-ttl-ms")?
            .unwrap_or(DEFAULT_MEMORY_OWNER_HEARTBEAT_MAX_AGE_MS),
        now_ms: current_time_ms()?,
    })
    .map_err(|err| err.to_string())?;
    print_json(&report)
}

fn run_memory_owner_shadow(args: &[String]) -> Result<(), String> {
    let options = SimpleOptions::parse(
        args,
        "memory-owner-shadow",
        &[
            "--kind",
            "--input-id",
            "--snapshot-status",
            "--mem-engine-status",
            "--snapshot-digest",
            "--mem-engine-digest",
        ],
        &["--json"],
    )?;
    let report = record_memory_owner_shadow_receipt(MemoryOwnerShadowOptions {
        harness_home: options.target_home.clone(),
        kind: parse_memory_owner_shadow_kind(&options.required("--kind")?)?,
        input_id: options.required("--input-id")?,
        snapshot_status: options.required("--snapshot-status")?,
        mem_engine_status: options.required("--mem-engine-status")?,
        snapshot_digest: options.required("--snapshot-digest")?,
        mem_engine_digest: options.required("--mem-engine-digest")?,
        now_ms: current_time_ms()?,
    })
    .map_err(|err| err.to_string())?;
    print_json(&report)
}

fn run_memory_owner_trust_scope(args: &[String]) -> Result<(), String> {
    let options =
        SimpleOptions::parse(args, "memory-owner-trust-scope", &["--passed"], &["--json"])?;
    let report = record_memory_owner_trust_scope_receipt(MemoryOwnerTrustScopeOptions {
        harness_home: options.target_home.clone(),
        passed: parse_bool_value(&options.required("--passed")?, "--passed")?,
        now_ms: current_time_ms()?,
    })
    .map_err(|err| err.to_string())?;
    print_json(&report)
}

fn run_memory_owner_local_prepare(args: &[String]) -> Result<(), String> {
    let options = SimpleOptions::parse(
        args,
        "memory-owner-local-prepare",
        &["--agent", "--query", "--lease-id", "--lease-ttl-ms"],
        &["--json"],
    )?;
    let report = prepare_openclaw_mem_local_owner(OpenClawMemLocalOwnerPrepareOptions {
        harness_home: options.target_home.clone(),
        agent_id: options.optional("--agent").map(ToString::to_string),
        query: options
            .optional("--query")
            .unwrap_or("openclaw memory owner local adapter smoke")
            .to_string(),
        lease_id: options.optional("--lease-id").map(ToString::to_string),
        lease_ttl_ms: options
            .optional_i64("--lease-ttl-ms")?
            .unwrap_or(DEFAULT_MEMORY_OWNER_HEARTBEAT_MAX_AGE_MS),
        now_ms: current_time_ms()?,
    })
    .map_err(|err| err.to_string())?;
    print_json(&report)
}

fn run_memory_owner_promote(args: &[String]) -> Result<(), String> {
    let options = SimpleOptions::parse(
        args,
        "memory-owner-promote",
        &["--heartbeat-max-age-ms"],
        &["--operator-approved", "--json"],
    )?;
    let report = request_memory_owner_promotion(MemoryOwnerPromotionOptions {
        harness_home: options.target_home.clone(),
        operator_approved: options.has_flag("--operator-approved"),
        heartbeat_max_age_ms: options
            .optional_i64("--heartbeat-max-age-ms")?
            .unwrap_or(DEFAULT_MEMORY_OWNER_HEARTBEAT_MAX_AGE_MS),
        now_ms: current_time_ms()?,
    })
    .map_err(|err| err.to_string())?;
    print_json(&report)
}

fn run_memory_owner_recover(args: &[String]) -> Result<(), String> {
    let options = SimpleOptions::parse(
        args,
        "memory-owner-recover",
        &["--heartbeat-max-age-ms"],
        &["--json"],
    )?;
    let report = recover_memory_owner_state(MemoryOwnerRecoveryOptions {
        harness_home: options.target_home.clone(),
        heartbeat_max_age_ms: options
            .optional_i64("--heartbeat-max-age-ms")?
            .unwrap_or(DEFAULT_MEMORY_OWNER_HEARTBEAT_MAX_AGE_MS),
        now_ms: current_time_ms()?,
    })
    .map_err(|err| err.to_string())?;
    print_json(&report)
}

fn run_progress_delivery_once(args: &[String]) -> Result<(), String> {
    let args = progress_delivery_once_args_from_args(args)?;
    let report = execute_progress_delivery_once(&args)?;
    print_progress_delivery_once_report(&report);
    Ok(())
}

fn run_progress_delivery_loop(args: &[String]) -> Result<(), String> {
    let args = progress_delivery_loop_args_from_args(args)?;
    let mut iterations = 0usize;
    let mut consecutive_errors = 0usize;

    loop {
        let progress_wake_sequence =
            read_loop_wake_sequence(&args.send.target_home, "progress-delivery");
        if stop_file_requested(args.stop_file.as_deref()) {
            append_loop_stop_log(
                &args.send.target_home,
                "progress",
                "progress.delivery-loop-stopped",
                iterations,
                "stop file requested",
            )?;
            write_loop_heartbeat(
                &args.send.target_home,
                "progress-delivery-loop",
                "stopped",
                iterations,
                "stop file requested",
            )?;
            println!("Progress delivery loop stop requested after {iterations} iteration(s)");
            break;
        }
        iterations += 1;
        write_loop_heartbeat(
            &args.send.target_home,
            "progress-delivery-loop",
            "running",
            iterations,
            "checking progress events",
        )?;
        let mut send_args = args.send.clone();
        send_args.preempt_after_wake_sequence = Some(progress_wake_sequence);
        match execute_progress_delivery_once(&send_args) {
            Ok(report) => {
                consecutive_errors = 0;
                write_loop_heartbeat(
                    &args.send.target_home,
                    "progress-delivery-loop",
                    "ok",
                    iterations,
                    &format!(
                        "pending={} sent={} edited={} failed={}",
                        report.pending_count,
                        report.sent_messages,
                        report.edited_messages,
                        report.failed_deliveries
                    ),
                )?;
                println!("Progress delivery loop iteration: {iterations}");
                print_progress_delivery_once_report(&report);
            }
            Err(error) => {
                consecutive_errors += 1;
                write_loop_heartbeat(
                    &args.send.target_home,
                    "progress-delivery-loop",
                    "error",
                    iterations,
                    &format!("consecutiveErrors={consecutive_errors} error={error}"),
                )?;
                eprintln!(
                    "progress-delivery-loop iteration {iterations} failed ({consecutive_errors}/{}): {error}",
                    args.max_consecutive_errors
                );
                append_harness_log(
                    &args.send.target_home,
                    &HarnessLogEvent::new(
                        current_log_time_ms().map_err(|err| err.to_string())?,
                        HarnessLogLevel::Warn,
                        "progress",
                        "progress.delivery-loop-error",
                        format!(
                            "iteration={iterations} consecutiveErrors={consecutive_errors}/{} error={error}",
                            args.max_consecutive_errors
                        ),
                    ),
                )
                .map_err(|err| err.to_string())?;
                if consecutive_errors >= args.max_consecutive_errors {
                    return Err(format!(
                        "progress-delivery-loop exceeded {} consecutive errors; last error: {error}",
                        args.max_consecutive_errors
                    ));
                }
            }
        }

        if args.iterations > 0 && iterations >= args.iterations {
            break;
        }
        wait_for_loop_wake_since(
            &args.send.target_home,
            "progress-delivery",
            progress_wake_sequence,
            args.idle_ms,
        );
    }
    Ok(())
}

fn run_supervisor_reconcile(args: &[String]) -> Result<(), String> {
    let args = supervisor_reconcile_args_from_args(args)?;
    if args.apply && args.dry_run {
        return Err("--apply and --dry-run are mutually exclusive".to_string());
    }
    if args.apply && !args.explicit_desired_input && !args.allow_default_apply {
        return Err(
            "supervisor-reconcile --apply requires --all, --desired-services-json, --desired-services-file, or supervisor.enabled=true in harness-config.json"
                .to_string(),
        );
    }

    let mut iteration = 0usize;
    loop {
        iteration += 1;
        let now_ms = current_time_ms()?;
        let desired_services = supervisor_reconcile_desired_services(&args)?;
        let report = reconcile_supervisor_inventory(SupervisorInventoryOptions {
            harness_home: args.target_home.clone(),
            desired_services,
            now_ms: Some(now_ms),
            default_heartbeat_timeout_ms: Some(args.default_heartbeat_timeout_ms),
        })
        .map_err(|err| err.to_string())?;
        let launches = if args.apply {
            launch_supervisor_reconcile_commands(&args, &report.launch_commands)?
        } else {
            Vec::new()
        };
        let output = serde_json::json!({
            "schema": "agent-harness.supervisor-reconcile.v1",
            "iteration": iteration,
            "dryRun": !args.apply,
            "applied": args.apply,
            "launches": launches,
            "inventory": report,
        });
        print_json(&output)?;

        if args.iterations > 0 && iteration >= args.iterations {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(args.idle_ms));
    }
}

fn run_supervisor_run(args: &[String]) -> Result<(), String> {
    let args = supervisor_run_args_from_args(args)?;
    let mut restarts = 0usize;
    let mut launches = 0usize;

    loop {
        if stop_file_requested(args.stop_file.as_deref()) {
            write_supervisor_run_state(&args, None, None, "stopped", "stopped by stop file")?;
            println!(
                "supervisor-run {} stop requested after {launches} launch(es)",
                args.service
            );
            return Ok(());
        }

        launches += 1;
        let started_at_ms = current_time_ms()?;
        let generation_id = format!(
            "{}-supervised-{}-{}-{}",
            args.service,
            std::process::id(),
            started_at_ms,
            launches
        );
        let mut command = Command::new(&args.harness_cli);
        command
            .args(supervisor_child_args(&args))
            .env("AGENT_HARNESS_SERVICE_GENERATION_ID", &generation_id)
            .env(
                "AGENT_HARNESS_SERVICE_STARTED_AT_MS",
                started_at_ms.to_string(),
            )
            .env(
                "AGENT_HARNESS_SUPERVISOR_LAUNCH_OWNER",
                "rust-supervisor-run",
            )
            .env("AGENT_HARNESS_SUPERVISOR_OBSERVED_ONLY", "false")
            .env(
                "AGENT_HARNESS_SUPERVISOR_PARENT_PID",
                std::process::id().to_string(),
            )
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit());

        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(err) => {
                let detail = format!(
                    "failed to start supervised {} child with {}: {err}",
                    args.service,
                    args.harness_cli.display()
                );
                let _ = write_supervisor_run_state(&args, None, None, "failed", &detail);
                return Err(detail);
            }
        };
        let child_pid = child.id();
        write_supervisor_run_state(
            &args,
            Some(SupervisedChildRuntimeState {
                pid: child_pid,
                generation_id: generation_id.clone(),
                started_at_ms,
                last_exit_at_ms: None,
                last_exit_code: None,
                last_error_class: None,
                restart_count: restarts,
                backoff_until_ms: None,
            }),
            None,
            "running",
            "supervisor child running",
        )?;

        let status = child.wait().map_err(|err| {
            format!(
                "failed waiting for supervised {} child pid={child_pid}: {err}",
                args.service
            )
        })?;
        let exit_code = status.code();
        let exit_code_text = exit_code
            .map(|code| code.to_string())
            .unwrap_or_else(|| "terminated".to_string());
        let exit_at_ms = current_time_ms()?;
        if status.success() {
            let (actual_state, detail) = if stop_file_requested(args.stop_file.as_deref()) {
                (
                    "stopped",
                    format!("child stopped by stop file with code {exit_code_text}"),
                )
            } else {
                (
                    "exited",
                    format!("child exited successfully with code {exit_code_text}"),
                )
            };
            write_supervisor_run_state(
                &args,
                Some(SupervisedChildRuntimeState {
                    pid: child_pid,
                    generation_id,
                    started_at_ms,
                    last_exit_at_ms: Some(exit_at_ms),
                    last_exit_code: exit_code,
                    last_error_class: None,
                    restart_count: restarts,
                    backoff_until_ms: None,
                }),
                None,
                actual_state,
                &detail,
            )?;
            return Ok(());
        }

        restarts += 1;
        let error_class = "process-exit".to_string();
        if args.max_restarts > 0 && restarts > args.max_restarts {
            write_supervisor_run_state(
                &args,
                Some(SupervisedChildRuntimeState {
                    pid: child_pid,
                    generation_id,
                    started_at_ms,
                    last_exit_at_ms: Some(exit_at_ms),
                    last_exit_code: exit_code,
                    last_error_class: Some(error_class),
                    restart_count: restarts,
                    backoff_until_ms: None,
                }),
                None,
                "failed",
                &format!("child exit code {exit_code_text}; restart limit exceeded"),
            )?;
            return Err(format!(
                "supervisor-run {} exceeded restart limit {}; last exit code {exit_code_text}",
                args.service, args.max_restarts
            ));
        }

        let backoff_until_ms = exit_at_ms.saturating_add(args.restart_delay_ms as i64);
        write_supervisor_run_state(
            &args,
            Some(SupervisedChildRuntimeState {
                pid: child_pid,
                generation_id,
                started_at_ms,
                last_exit_at_ms: Some(exit_at_ms),
                last_exit_code: exit_code,
                last_error_class: Some(error_class),
                restart_count: restarts,
                backoff_until_ms: Some(backoff_until_ms),
            }),
            None,
            "restart-backoff",
            &format!(
                "child exit code {exit_code_text}; restart {restarts} after {}ms",
                args.restart_delay_ms
            ),
        )?;
        if supervisor_backoff_stop_requested(args.restart_delay_ms, args.stop_file.as_deref()) {
            write_supervisor_run_state(&args, None, None, "stopped", "stopped by stop file")?;
            return Ok(());
        }
    }
}

fn supervisor_backoff_stop_requested(delay_ms: u64, stop_file: Option<&Path>) -> bool {
    let mut remaining_ms = delay_ms;
    while remaining_ms > 0 {
        if stop_file_requested(stop_file) {
            return true;
        }
        let sleep_ms = remaining_ms.min(1_000);
        thread::sleep(Duration::from_millis(sleep_ms));
        remaining_ms = remaining_ms.saturating_sub(sleep_ms);
    }
    stop_file_requested(stop_file)
}

fn supervisor_child_args(args: &SupervisorRunArgs) -> Vec<String> {
    match args.service.as_str() {
        "runtime-loop" => {
            let mut child_args = vec![
                "runtime-loop".to_string(),
                "--harness-home".to_string(),
                args.target_home.display().to_string(),
                "--loop-name".to_string(),
                args.loop_name
                    .clone()
                    .unwrap_or_else(|| args.service.clone()),
                "--iterations".to_string(),
                args.child_iterations.to_string(),
                "--idle-ms".to_string(),
                args.idle_ms.to_string(),
                "--runtime-concurrency".to_string(),
                args.runtime_concurrency.to_string(),
                "--timeout-ms".to_string(),
                args.timeout_ms.to_string(),
                "--idle-timeout-ms".to_string(),
                args.idle_timeout_ms.to_string(),
                "--max-prompt-file-bytes".to_string(),
                args.max_prompt_file_bytes.to_string(),
                "--max-skill-file-bytes".to_string(),
                args.max_skill_file_bytes.to_string(),
                "--max-consecutive-errors".to_string(),
                args.max_consecutive_errors.to_string(),
            ];
            push_optional_path_arg(&mut child_args, "--codex-exe", args.codex_exe.as_ref());
            push_optional_path_arg(&mut child_args, "--stop-file", args.stop_file.as_ref());
            child_args
        }
        "worker-loop" => {
            let mut child_args = vec![
                "worker-loop".to_string(),
                "--harness-home".to_string(),
                args.target_home.display().to_string(),
                "--worker-id".to_string(),
                args.worker_id
                    .clone()
                    .unwrap_or_else(|| format!("supervisor-run:{}", args.service)),
                "--lease-ms".to_string(),
                args.lease_ms.to_string(),
                "--iterations".to_string(),
                args.child_iterations.to_string(),
                "--idle-ms".to_string(),
                args.idle_ms.to_string(),
                "--max-consecutive-errors".to_string(),
                args.max_consecutive_errors.to_string(),
            ];
            push_optional_string_arg(&mut child_args, "--lane", args.lane.as_deref());
            push_optional_path_arg(&mut child_args, "--stop-file", args.stop_file.as_ref());
            child_args
        }
        "cron-scheduler-loop" => {
            let mut child_args = vec![
                "cron-scheduler-loop".to_string(),
                "--source-home".to_string(),
                args.source_home.display().to_string(),
                "--harness-home".to_string(),
                args.target_home.display().to_string(),
                "--iterations".to_string(),
                args.child_iterations.to_string(),
                "--idle-ms".to_string(),
                args.idle_ms.to_string(),
                "--max-consecutive-errors".to_string(),
                args.max_consecutive_errors.to_string(),
            ];
            push_optional_path_arg(&mut child_args, "--workspace", args.workspace.as_ref());
            push_optional_path_arg(
                &mut child_args,
                "--runtime-workspace",
                args.runtime_workspace.as_ref(),
            );
            push_optional_path_arg(&mut child_args, "--stop-file", args.stop_file.as_ref());
            child_args
        }
        "progress-delivery-loop" => {
            let mut child_args = vec![
                "progress-delivery-loop".to_string(),
                "--harness-home".to_string(),
                args.target_home.display().to_string(),
                "--iterations".to_string(),
                args.child_iterations.to_string(),
                "--idle-ms".to_string(),
                args.idle_ms.to_string(),
                "--max-consecutive-errors".to_string(),
                args.max_consecutive_errors.to_string(),
            ];
            if let Some(stop_file) = &args.stop_file {
                child_args.extend(["--stop-file".to_string(), stop_file.display().to_string()]);
            }
            child_args
        }
        service if service == "telegram-loop" || service.starts_with("telegram-loop-") => {
            let mut child_args = vec![
                "telegram-loop".to_string(),
                "--source-home".to_string(),
                args.source_home.display().to_string(),
                "--harness-home".to_string(),
                args.target_home.display().to_string(),
                "--loop-name".to_string(),
                args.loop_name
                    .clone()
                    .unwrap_or_else(|| service.to_string()),
                "--iterations".to_string(),
                args.child_iterations.to_string(),
                "--idle-ms".to_string(),
                args.idle_ms.to_string(),
                "--max-consecutive-errors".to_string(),
                args.max_consecutive_errors.to_string(),
                "--poll-timeout-seconds".to_string(),
                args.poll_timeout_seconds.to_string(),
                "--max-updates".to_string(),
                args.max_updates.to_string(),
                "--outbox-limit".to_string(),
                args.outbox_limit.to_string(),
                "--timeout-ms".to_string(),
                args.timeout_ms.to_string(),
                "--idle-timeout-ms".to_string(),
                args.idle_timeout_ms.to_string(),
            ];
            push_optional_path_arg(&mut child_args, "--workspace", args.workspace.as_ref());
            push_optional_path_arg(
                &mut child_args,
                "--runtime-workspace",
                args.runtime_workspace.as_ref(),
            );
            push_optional_string_arg(&mut child_args, "--agent", args.agent_id.as_deref());
            push_optional_string_arg(
                &mut child_args,
                "--telegram-account",
                args.telegram_account.as_deref(),
            );
            push_optional_path_arg(&mut child_args, "--codex-exe", args.codex_exe.as_ref());
            push_optional_path_arg(&mut child_args, "--stop-file", args.stop_file.as_ref());
            child_args
        }
        "discord-outbox-loop" => {
            let mut child_args = vec![
                "discord-outbox-loop".to_string(),
                "--harness-home".to_string(),
                args.target_home.display().to_string(),
                "--iterations".to_string(),
                args.child_iterations.to_string(),
                "--idle-ms".to_string(),
                args.idle_ms.to_string(),
                "--max-consecutive-errors".to_string(),
                args.max_consecutive_errors.to_string(),
                "--outbox-limit".to_string(),
                args.outbox_limit.to_string(),
            ];
            if let Some(discord_account) = &args.discord_account {
                child_args.extend(["--discord-account".to_string(), discord_account.to_string()]);
            }
            if let Some(stop_file) = &args.stop_file {
                child_args.extend(["--stop-file".to_string(), stop_file.display().to_string()]);
            }
            child_args
        }
        "discord-gateway-loop" => {
            let mut child_args = vec![
                "discord-gateway-loop".to_string(),
                "--source-home".to_string(),
                args.source_home.display().to_string(),
                "--harness-home".to_string(),
                args.target_home.display().to_string(),
                "--node-exe".to_string(),
                args.node_exe.display().to_string(),
                "--gateway-script".to_string(),
                args.gateway_script.display().to_string(),
                "--harness-cli".to_string(),
                args.harness_cli.display().to_string(),
            ];
            push_optional_path_arg(&mut child_args, "--workspace", args.workspace.as_ref());
            push_optional_path_arg(
                &mut child_args,
                "--runtime-workspace",
                args.runtime_workspace.as_ref(),
            );
            push_optional_string_arg(&mut child_args, "--agent", args.agent_id.as_deref());
            push_optional_string_arg(
                &mut child_args,
                "--discord-account",
                args.discord_account.as_deref(),
            );
            push_optional_path_arg(&mut child_args, "--codex-exe", args.codex_exe.as_ref());
            push_optional_path_arg(&mut child_args, "--stop-file", args.stop_file.as_ref());
            child_args
        }
        _ => Vec::new(),
    }
}

fn supervisor_run_supported_service(service: &str) -> bool {
    matches!(
        service,
        "runtime-loop"
            | "worker-loop"
            | "cron-scheduler-loop"
            | "progress-delivery-loop"
            | "discord-outbox-loop"
            | "discord-gateway-loop"
            | "telegram-loop"
    ) || service.starts_with("telegram-loop-")
}

fn push_optional_path_arg(args: &mut Vec<String>, flag: &str, value: Option<&PathBuf>) {
    if let Some(value) = value {
        args.extend([flag.to_string(), value.display().to_string()]);
    }
}

fn push_optional_string_arg(args: &mut Vec<String>, flag: &str, value: Option<&str>) {
    if let Some(value) = value {
        args.extend([flag.to_string(), value.to_string()]);
    }
}

fn write_supervisor_run_state(
    args: &SupervisorRunArgs,
    runtime: Option<SupervisedChildRuntimeState>,
    memory_gate_decision: Option<serde_json::Value>,
    actual_state: &str,
    detail: &str,
) -> Result<(), String> {
    let dir = args
        .target_home
        .join("state")
        .join("supervisor")
        .join("services");
    fs::create_dir_all(&dir).map_err(|err| err.to_string())?;
    let now_ms = current_time_ms()?;
    let desired_state = if actual_state == "stopped" {
        "stopped"
    } else {
        "running"
    };
    let generation_id = runtime
        .as_ref()
        .map(|runtime| runtime.generation_id.clone())
        .unwrap_or_else(|| format!("{}-supervisor-{}", args.service, std::process::id()));
    let service = serde_json::json!({
        "schema": "agent-harness.supervisor-service-state.v1",
        "serviceId": args.service,
        "serviceKind": loop_service_kind(&args.service),
        "generationId": generation_id,
        "pid": runtime.as_ref().map(|runtime| runtime.pid),
        "processId": runtime.as_ref().map(|runtime| runtime.pid),
        "supervisorPid": std::process::id(),
        "processStartTimeMs": runtime.as_ref().map(|runtime| runtime.started_at_ms),
        "startedAtMs": runtime.as_ref().map(|runtime| runtime.started_at_ms),
        "lastHeartbeatAtMs": now_ms,
        "lastSuccessfulIterationAtMs": if actual_state == "running" { Some(now_ms) } else { None },
        "lastExitAtMs": runtime.as_ref().and_then(|runtime| runtime.last_exit_at_ms),
        "lastExitCode": runtime.as_ref().and_then(|runtime| runtime.last_exit_code),
        "lastErrorClass": runtime.as_ref().and_then(|runtime| runtime.last_error_class.clone()),
        "restartCount": runtime.as_ref().map(|runtime| runtime.restart_count),
        "backoffUntilMs": runtime.as_ref().and_then(|runtime| runtime.backoff_until_ms),
        "iteration": runtime.as_ref().map(|runtime| runtime.restart_count),
        "status": actual_state,
        "desiredState": desired_state,
        "actualState": actual_state,
        "detail": detail,
        "launchOwner": "rust-supervisor-run",
        "observedOnly": false,
        "servicePriority": supervisor_service_priority(&args.service),
        "deliveryLane": supervisor_delivery_lane(&args.service),
        "restartDelayMs": args.restart_delay_ms,
        "maxRestarts": args.max_restarts,
        "memoryGateDecision": memory_gate_decision,
    });
    let file = dir.join(format!("{}.json", args.service));
    write_json_atomic(&file, &service).map_err(|err| {
        format!(
            "failed to write supervisor service state {}: {err}",
            file.display()
        )
    })
}

fn supervisor_service_priority(service: &str) -> &'static str {
    match service {
        "discord-outbox-loop" => "final-delivery",
        "progress-delivery-loop" => "telemetry",
        _ => "standard",
    }
}

fn supervisor_delivery_lane(service: &str) -> Option<&'static str> {
    match service {
        "discord-outbox-loop" => Some("final-outbox"),
        "progress-delivery-loop" => Some("progress-delivery"),
        _ => None,
    }
}

fn execute_progress_delivery_once(
    args: &ProgressDeliveryOnceArgs,
) -> Result<ProgressDeliveryOnceReport, String> {
    let plan = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
        harness_home: args.target_home.clone(),
        platform: args.platform.clone(),
        now_ms: current_time_ms()?,
        min_update_interval_ms: args.min_update_interval_ms,
        max_nonterminal_updates_per_lane: AgentProgressDeliveryPlanOptions::default()
            .max_nonterminal_updates_per_lane,
        status_heartbeat_after_body_cap_ms: AgentProgressDeliveryPlanOptions::default()
            .status_heartbeat_after_body_cap_ms,
        max_events_per_panel: args.max_events_per_panel,
        max_preview_chars: args.max_preview_chars,
        current_step_max_chars: args.current_step_max_chars,
    })
    .map_err(|err| err.to_string())?;
    let mut warnings = plan.warnings.clone();
    let policy = channel_access_policy(&args.target_home)?;
    let pending_count = plan.pending.len();
    let skipped_muted = plan.summary.skipped_muted;
    let volume_limited = plan.summary.volume_limited;
    let mut sent_messages = 0usize;
    let mut edited_messages = 0usize;
    let mut skipped_denied = 0usize;
    let mut skipped_permanent = 0usize;
    let mut failed_deliveries = 0usize;

    let mut pending_items = plan.pending;
    pending_items.sort_by_key(progress_delivery_pending_priority);
    for pending in pending_items {
        if let Some(sequence) = args.preempt_after_wake_sequence {
            let current_sequence = read_loop_wake_sequence(&args.target_home, "progress-delivery");
            if progress_delivery_should_preempt_stale_pending(&pending, sequence, current_sequence)
            {
                warnings.push(format!(
                    "progress delivery plan for {} was preempted by a newer progress wake; replanning before non-terminal delivery",
                    pending.queue_id
                ));
                break;
            }
        }
        let policy_decision = match progress_delivery_allowed(&policy, &pending, args) {
            Ok(decision) => decision,
            Err(reason) => {
                record_progress_delivery(
                    args,
                    &pending,
                    pending.action,
                    AgentProgressDeliveryStatus::SkippedDenied,
                    pending.provider_message_id.clone(),
                    Some("channel-access-denied".to_string()),
                    Some(reason.clone()),
                )?;
                warnings.push(format!(
                    "progress delivery for {} denied by channel access policy: {}",
                    pending.queue_id, reason
                ));
                skipped_denied += 1;
                continue;
            }
        };

        match deliver_progress_pending(args, &pending) {
            Ok((actual_action, provider_message_id)) => {
                record_progress_delivery(
                    args,
                    &pending,
                    actual_action,
                    AgentProgressDeliveryStatus::Delivered,
                    provider_message_id,
                    Some(policy_decision.clone()),
                    None,
                )?;
                match actual_action {
                    AgentProgressDeliveryAction::Send => sent_messages += 1,
                    AgentProgressDeliveryAction::Edit => edited_messages += 1,
                }
            }
            Err(error) => {
                let status = if progress_delivery_error_is_permanent(&error) {
                    skipped_permanent += 1;
                    AgentProgressDeliveryStatus::SkippedPermanent
                } else {
                    failed_deliveries += 1;
                    AgentProgressDeliveryStatus::Failed
                };
                record_progress_delivery(
                    args,
                    &pending,
                    pending.action,
                    status,
                    pending.provider_message_id.clone(),
                    Some(policy_decision.clone()),
                    Some(error.clone()),
                )?;
                warnings.push(error);
            }
        }
    }

    let report = ProgressDeliveryOnceReport {
        pending_count,
        skipped_muted,
        volume_limited,
        sent_messages,
        edited_messages,
        skipped_denied,
        skipped_permanent,
        failed_deliveries,
        warnings,
    };
    append_harness_log(
        &args.target_home,
        &HarnessLogEvent::new(
            current_log_time_ms().map_err(|err| err.to_string())?,
            if report.failed_deliveries == 0 {
                HarnessLogLevel::Info
            } else {
                HarnessLogLevel::Warn
            },
            "progress",
            "progress.delivery-once",
            format!(
                "pending={} muted={} sent={} edited={} denied={} permanent={} failed={}",
                report.pending_count,
                report.skipped_muted,
                report.sent_messages,
                report.edited_messages,
                report.skipped_denied,
                report.skipped_permanent,
                report.failed_deliveries
            ),
        ),
    )
    .map_err(|err| err.to_string())?;
    Ok(report)
}

fn progress_delivery_pending_priority(pending: &AgentProgressDeliveryPending) -> (u8, u8, usize) {
    let terminal_rank = if pending.terminal { 0 } else { 1 };
    let lane_rank = match (pending.terminal, pending.message_kind) {
        (true, agent_harness_core::AgentProgressDeliveryMessageKind::Status) => 0,
        (true, agent_harness_core::AgentProgressDeliveryMessageKind::Body) => 1,
        (false, agent_harness_core::AgentProgressDeliveryMessageKind::Body) => 0,
        (false, agent_harness_core::AgentProgressDeliveryMessageKind::Status) => 1,
    };
    (terminal_rank, lane_rank, pending.event_line)
}

fn progress_delivery_should_preempt_stale_pending(
    pending: &AgentProgressDeliveryPending,
    previous_sequence: u64,
    current_sequence: u64,
) -> bool {
    !pending.terminal && current_sequence > previous_sequence
}

fn progress_delivery_account_id<'a>(
    args: &'a ProgressDeliveryOnceArgs,
    pending: &'a AgentProgressDeliveryPending,
) -> Option<&'a str> {
    pending
        .account_id
        .as_deref()
        .map(str::trim)
        .filter(|account_id| !account_id.is_empty())
        .or_else(|| {
            args.telegram_account
                .as_deref()
                .map(str::trim)
                .filter(|account_id| !account_id.is_empty())
        })
}

fn deliver_progress_pending(
    args: &ProgressDeliveryOnceArgs,
    pending: &AgentProgressDeliveryPending,
) -> Result<(AgentProgressDeliveryAction, Option<String>), String> {
    match pending.platform.as_str() {
        "telegram" => {
            let token = telegram_bot_token(
                &args.target_home,
                progress_delivery_account_id(args, pending),
            )?;
            match pending.action {
                AgentProgressDeliveryAction::Send => telegram_send_message(
                    &token,
                    &pending.channel_id,
                    &pending.text,
                    TelegramSendOptions {
                        message_thread_id: pending.thread_id.as_deref(),
                        ..TelegramSendOptions::default()
                    },
                )
                .map(|id| (AgentProgressDeliveryAction::Send, id)),
                AgentProgressDeliveryAction::Edit => {
                    let Some(message_id) = pending.provider_message_id.as_deref() else {
                        return telegram_send_message(
                            &token,
                            &pending.channel_id,
                            &pending.text,
                            TelegramSendOptions {
                                message_thread_id: pending.thread_id.as_deref(),
                                ..TelegramSendOptions::default()
                            },
                        )
                        .map(|id| (AgentProgressDeliveryAction::Send, id));
                    };
                    telegram_edit_message_text(
                        &token,
                        &pending.channel_id,
                        message_id,
                        &pending.text,
                    )
                    .map(|id| (AgentProgressDeliveryAction::Edit, id.or(Some(message_id.to_string()))))
                    .or_else(|edit_error| {
                    telegram_send_message(
                        &token,
                        &pending.channel_id,
                        &pending.text,
                        TelegramSendOptions {
                            message_thread_id: pending.thread_id.as_deref(),
                            ..TelegramSendOptions::default()
                        },
                    )
                            .and_then(|id| {
                                id.map(|id| (AgentProgressDeliveryAction::Send, Some(id)))
                                    .ok_or_else(|| {
                                        format!(
                                            "Telegram progress edit failed ({edit_error}); replacement send did not return a message id"
                                        )
                                    })
                            })
                            .map_err(|send_error| {
                                format!(
                                    "Telegram progress edit failed ({edit_error}); replacement send failed ({send_error})"
                                )
                            })
                    })
                }
            }
        }
        "discord" => {
            let token = discord_bot_token(&args.target_home, pending.account_id.as_deref())?;
            match pending.action {
                AgentProgressDeliveryAction::Send => {
                    discord_send_message(&token, &pending.channel_id, &pending.text, None)
                        .map(|id| (AgentProgressDeliveryAction::Send, id))
                }
                AgentProgressDeliveryAction::Edit => {
                    let Some(message_id) = pending.provider_message_id.as_deref() else {
                        return discord_send_message(
                            &token,
                            &pending.channel_id,
                            &pending.text,
                            None,
                        )
                        .map(|id| (AgentProgressDeliveryAction::Send, id));
                    };
                    discord_edit_message(&token, &pending.channel_id, message_id, &pending.text)
                        .map(|id| {
                            (
                                AgentProgressDeliveryAction::Edit,
                                id.or(Some(message_id.to_string())),
                            )
                        })
                        .or_else(|edit_error| {
                    discord_send_message(&token, &pending.channel_id, &pending.text, None)
                                .map(|id| (AgentProgressDeliveryAction::Send, id))
                                .map_err(|send_error| {
                                    format!(
                                        "Discord progress edit failed ({edit_error}); replacement send failed ({send_error})"
                                    )
                                })
                        })
                }
            }
        }
        other => Err(format!(
            "progress delivery does not support platform `{other}`"
        )),
    }
}

fn record_progress_delivery(
    args: &ProgressDeliveryOnceArgs,
    pending: &AgentProgressDeliveryPending,
    action: AgentProgressDeliveryAction,
    status: AgentProgressDeliveryStatus,
    provider_message_id: Option<String>,
    policy_decision: Option<String>,
    error: Option<String>,
) -> Result<(), String> {
    record_agent_progress_delivery(AgentProgressDeliveryRecordOptions {
        harness_home: args.target_home.clone(),
        queue_id: pending.queue_id.clone(),
        platform: pending.platform.clone(),
        account_id: pending.account_id.clone(),
        channel_id: pending.channel_id.clone(),
        thread_id: pending.thread_id.clone(),
        user_id: pending.user_id.clone(),
        session_key: pending.session_key.clone(),
        message_kind: pending.message_kind,
        action,
        status,
        provider_message_id,
        event_line: pending.event_line,
        text_hash: pending.text_hash.clone(),
        terminal: pending.terminal,
        policy_decision,
        error,
        now_ms: current_time_ms()?,
    })
    .map(|_| ())
    .map_err(|err| err.to_string())
}

fn progress_delivery_allowed(
    policy: &ChannelAccessPolicy,
    pending: &AgentProgressDeliveryPending,
    args: &ProgressDeliveryOnceArgs,
) -> Result<String, String> {
    match pending.platform.as_str() {
        "telegram" => {
            if pending.channel_id.starts_with('-')
                && progress_delivery_account_id(args, pending).is_none()
            {
                return Err(
                    "Telegram group/topic progress target has no resolved account id".to_string(),
                );
            }
            if policy.telegram_group_chat_ids.contains(&pending.channel_id) {
                if telegram_user_is_admin(policy, &pending.user_id)
                    || policy
                        .telegram_group_admin_user_ids
                        .contains(&pending.user_id)
                    || policy
                        .telegram_group_allowed_user_ids
                        .contains(&pending.user_id)
                    || policy.telegram_group_open
                    || (policy.telegram_group_allowed_user_ids.is_empty()
                        && policy.telegram_group_admin_user_ids.is_empty()
                        && !policy.telegram_group_chat_ids.is_empty())
                {
                    return Ok(progress_policy_decision("telegram-group", pending, args));
                }
                return Err(
                    "Telegram group progress target is not authorized for this user id".to_string(),
                );
            }
            if telegram_user_is_admin(policy, &pending.user_id) {
                Ok(progress_policy_decision("telegram-dm", pending, args))
            } else {
                Err("Telegram DM progress target user id is not an admin/allowed user".to_string())
            }
        }
        "discord" => {
            if policy.discord_channel_ids.contains(&pending.channel_id) {
                if discord_user_is_admin(policy, &pending.user_id)
                    || policy
                        .discord_group_allowed_user_ids
                        .contains(&pending.user_id)
                    || policy.discord_group_open
                    || (policy.discord_group_allowed_user_ids.is_empty()
                        && !policy.discord_channel_ids.is_empty())
                {
                    return Ok(progress_policy_decision("discord-channel", pending, args));
                }
                return Err(
                    "Discord channel progress target is not authorized for this user id"
                        .to_string(),
                );
            }
            if discord_user_is_admin(policy, &pending.user_id) {
                Ok(progress_policy_decision("discord-dm", pending, args))
            } else {
                Err("Discord DM progress target user id is not an admin/allowed user".to_string())
            }
        }
        other => Err(format!(
            "progress delivery does not know access policy for platform `{other}`"
        )),
    }
}

fn progress_policy_decision(
    scope: &str,
    pending: &AgentProgressDeliveryPending,
    args: &ProgressDeliveryOnceArgs,
) -> String {
    let account = progress_delivery_account_id(args, pending).unwrap_or("default");
    let thread = pending.thread_id.as_deref().unwrap_or("-");
    format!(
        "allowed:{scope}:account={account}:thread={thread}:messageKind={:?}",
        pending.message_kind
    )
}

fn progress_delivery_error_is_permanent(error: &str) -> bool {
    let lower = error.to_ascii_lowercase();
    lower.contains("telegram http status 400")
        && (lower.contains("chat not found")
            || lower.contains("message thread not found")
            || lower.contains("message to edit not found"))
}

fn run_telegram_probe(args: &[String]) -> Result<(), String> {
    let args = telegram_probe_args_from_args(args)?;
    let report = match telegram_bot_token(&args.target_home, None) {
        Ok(token) => execute_telegram_probe(&args.target_home, &token)?,
        Err(reason) => write_telegram_probe_result(TelegramProbeReport {
            harness_home: args.target_home.clone(),
            receipt_file: telegram_probe_receipts_file(&args.target_home),
            status: "token-missing".to_string(),
            reason,
            bot_id: None,
            username: None,
            can_join_groups: None,
            can_read_all_group_messages: None,
            supports_inline_queries: None,
            at_ms: current_time_ms()?,
        })?,
    };
    print_telegram_probe_report(&report);
    if report.status == "ready" {
        Ok(())
    } else {
        Err(report.reason)
    }
}

fn run_telegram_poll_once(args: &[String]) -> Result<(), String> {
    let args = telegram_poll_once_args_from_args(args)?;
    let token = telegram_bot_token(&args.target_home, args.telegram_account.as_deref())?;
    let report = execute_telegram_poll_once(&args, &token)?;
    print_telegram_poll_once_report(&report);
    Ok(())
}

fn run_telegram_loop(args: &[String]) -> Result<(), String> {
    let args = telegram_loop_args_from_args(args)?;
    let token = telegram_bot_token(
        &args.poll.target_home,
        args.poll.telegram_account.as_deref(),
    )?;
    let mut iterations = 0usize;
    let mut consecutive_errors = 0usize;

    loop {
        let outbox_wake_sequence = read_loop_wake_sequence(&args.poll.target_home, "final-outbox");
        if stop_file_requested(args.stop_file.as_deref()) {
            append_loop_stop_log(
                &args.poll.target_home,
                "telegram",
                "telegram.loop-stopped",
                iterations,
                "stop file requested",
            )?;
            write_loop_heartbeat(
                &args.poll.target_home,
                &args.loop_name,
                "stopped",
                iterations,
                "stop file requested",
            )?;
            println!("Telegram loop stop requested after {iterations} iteration(s)");
            break;
        }
        iterations += 1;
        write_loop_heartbeat(
            &args.poll.target_home,
            &args.loop_name,
            "running",
            iterations,
            "polling Telegram updates",
        )?;
        match execute_telegram_poll_once(&args.poll, &token) {
            Ok(report) => {
                consecutive_errors = 0;
                write_loop_heartbeat(
                    &args.poll.target_home,
                    &args.loop_name,
                    "ok",
                    iterations,
                    &format!(
                        "updates={} handled={} delivered={} failed={}",
                        report.update_count,
                        report.handled_messages,
                        report.delivered_messages,
                        report.failed_deliveries
                    ),
                )?;
                println!("Telegram loop iteration: {iterations}");
                print_telegram_poll_once_report(&report);
            }
            Err(error) => {
                consecutive_errors += 1;
                write_loop_heartbeat(
                    &args.poll.target_home,
                    &args.loop_name,
                    "error",
                    iterations,
                    &format!("consecutiveErrors={consecutive_errors} error={error}"),
                )?;
                eprintln!(
                    "{} iteration {iterations} failed ({consecutive_errors}/{}): {error}",
                    args.loop_name, args.max_consecutive_errors
                );
                let _ = append_harness_log(
                    &args.poll.target_home,
                    &HarnessLogEvent::new(
                        current_log_time_ms().unwrap_or(0),
                        HarnessLogLevel::Warn,
                        "telegram",
                        "telegram.loop-error",
                        format!(
                            "iteration={iterations} consecutiveErrors={consecutive_errors} error={error}"
                        ),
                    ),
                );
                if consecutive_errors >= args.max_consecutive_errors {
                    return Err(format!(
                        "{} exceeded {} consecutive errors; last error: {error}",
                        args.loop_name, args.max_consecutive_errors
                    ));
                }
            }
        }

        if args.iterations > 0 && iterations >= args.iterations {
            break;
        }
        wait_for_loop_wake_since(
            &args.poll.target_home,
            "final-outbox",
            outbox_wake_sequence,
            args.idle_ms,
        );
    }

    Ok(())
}

fn execute_telegram_probe(harness_home: &Path, token: &str) -> Result<TelegramProbeReport, String> {
    let at_ms = current_time_ms()?;
    let report = match telegram_get_me(token) {
        Ok(bot) => TelegramProbeReport {
            harness_home: harness_home.to_path_buf(),
            receipt_file: telegram_probe_receipts_file(harness_home),
            status: "ready".to_string(),
            reason: "Telegram Bot API getMe succeeded without consuming updates".to_string(),
            bot_id: bot
                .get("id")
                .and_then(telegram_id_string)
                .filter(|id| !id.is_empty()),
            username: bot
                .get("username")
                .and_then(serde_json::Value::as_str)
                .map(ToString::to_string),
            can_join_groups: bot
                .get("can_join_groups")
                .and_then(serde_json::Value::as_bool),
            can_read_all_group_messages: bot
                .get("can_read_all_group_messages")
                .and_then(serde_json::Value::as_bool),
            supports_inline_queries: bot
                .get("supports_inline_queries")
                .and_then(serde_json::Value::as_bool),
            at_ms,
        },
        Err(reason) => TelegramProbeReport {
            harness_home: harness_home.to_path_buf(),
            receipt_file: telegram_probe_receipts_file(harness_home),
            status: "failed".to_string(),
            reason,
            bot_id: None,
            username: None,
            can_join_groups: None,
            can_read_all_group_messages: None,
            supports_inline_queries: None,
            at_ms,
        },
    };
    write_telegram_probe_result(report)
}

fn write_telegram_probe_result(report: TelegramProbeReport) -> Result<TelegramProbeReport, String> {
    let latest_file = telegram_probe_latest_file(&report.harness_home);
    if let Some(parent) = latest_file.parent() {
        fs::create_dir_all(parent).map_err(|err| err.to_string())?;
    }
    let receipt = serde_json::json!({
        "schema": "agent-harness.telegram-probe-receipt.v1",
        "status": report.status,
        "reason": report.reason,
        "botId": report.bot_id,
        "username": report.username,
        "canJoinGroups": report.can_join_groups,
        "canReadAllGroupMessages": report.can_read_all_group_messages,
        "supportsInlineQueries": report.supports_inline_queries,
        "atMs": report.at_ms,
    });
    fs::write(
        &latest_file,
        serde_json::to_string_pretty(&receipt).map_err(|err| err.to_string())?,
    )
    .map_err(|err| err.to_string())?;
    append_jsonl_value(&report.receipt_file, &receipt).map_err(|err| err.to_string())?;
    append_harness_log(
        &report.harness_home,
        &HarnessLogEvent::new(
            current_log_time_ms().map_err(|err| err.to_string())?,
            if report.status == "ready" {
                HarnessLogLevel::Info
            } else {
                HarnessLogLevel::Warn
            },
            "telegram",
            "telegram.probe",
            format!(
                "status={} botId={} username={} reason={}",
                report.status,
                report.bot_id.as_deref().unwrap_or("-"),
                report.username.as_deref().unwrap_or("-"),
                report.reason
            ),
        ),
    )
    .map_err(|err| err.to_string())?;
    Ok(report)
}

fn execute_telegram_poll_once(
    args: &TelegramPollOnceArgs,
    token: &str,
) -> Result<TelegramPollOnceReport, String> {
    let mut warnings = Vec::new();
    let offset_file = telegram_offset_file(&args.target_home, args.telegram_account.as_deref());
    let offset = read_telegram_offset(&offset_file, &mut warnings)?;
    let access_policy = channel_access_policy(&args.target_home)?;
    let updates = telegram_get_updates(token, offset, args.poll_timeout_seconds, args.max_updates)?;
    let media_fetcher = telegram_media::TelegramBotApiMediaFetcher::new(token);
    let account_id = telegram_account_id(args.telegram_account.as_deref());
    let mut handled_messages = 0;
    let mut skipped_updates = 0;
    let mut next_offset = offset;
    let due_media_groups = telegram_media::take_due_telegram_media_groups(
        &args.target_home,
        &account_id,
        current_time_ms()?,
        telegram_media::DEFAULT_TELEGRAM_MEDIA_GROUP_DEBOUNCE_MS,
        telegram_media::DEFAULT_TELEGRAM_MEDIA_GROUP_STALE_MS,
    )?;
    for flush in due_media_groups {
        if execute_telegram_media_group_flush(
            args,
            token,
            &access_policy,
            &media_fetcher,
            &flush,
            &mut warnings,
        )? {
            handled_messages += 1;
        } else {
            skipped_updates += flush.members.len().max(1);
        }
    }
    for update in &updates {
        let Some(update_id) = update.get("update_id").and_then(serde_json::Value::as_i64) else {
            skipped_updates += 1;
            warnings.push("Telegram update had no update_id".to_string());
            continue;
        };
        next_offset = Some(update_id + 1);
        let Some(message) = update
            .get("message")
            .or_else(|| update.get("edited_message"))
        else {
            skipped_updates += 1;
            write_telegram_offset(&offset_file, next_offset)?;
            continue;
        };
        let inbound_context = telegram_inbound_context(message);
        let Some(text) = telegram_message_text(message, inbound_context.is_some()) else {
            skipped_updates += 1;
            write_telegram_offset(&offset_file, next_offset)?;
            continue;
        };
        let Some(chat_id) = message
            .get("chat")
            .and_then(|chat| chat.get("id"))
            .and_then(telegram_id_string)
        else {
            skipped_updates += 1;
            warnings.push(format!("Telegram update {update_id} had no chat id"));
            write_telegram_offset(&offset_file, next_offset)?;
            continue;
        };
        let user_id = message
            .get("from")
            .and_then(|from| from.get("id"))
            .and_then(telegram_id_string)
            .unwrap_or_else(|| chat_id.clone());
        let chat_type = message
            .get("chat")
            .and_then(|chat| chat.get("type"))
            .and_then(serde_json::Value::as_str);
        let thread_id = message
            .get("message_thread_id")
            .and_then(telegram_id_string);
        let identity = resolve_channel_identity(ChannelIdentityLookup {
            harness_home: args.target_home.clone(),
            platform: "telegram".to_string(),
            account_id: account_id.clone(),
            chat_id: chat_id.clone(),
            thread_id: thread_id.clone(),
            requested_agent_id: args.agent_id.clone(),
        })
        .map_err(|err| err.to_string())?;
        if !identity.is_bound() {
            skipped_updates += 1;
            warnings.push(format!(
                "Telegram update {update_id} denied by channel identity registry: {}",
                identity.reason
            ));
            write_telegram_offset(&offset_file, next_offset)?;
            continue;
        }
        let permission =
            match telegram_access_decision(&access_policy, &chat_id, &user_id, chat_type) {
                ChannelAccessDecision::Allowed(permission) => permission,
                ChannelAccessDecision::Denied(reason) => {
                    skipped_updates += 1;
                    warnings.push(format!(
                        "Telegram update {update_id} denied by channel access policy: {reason}"
                    ));
                    write_telegram_offset(&offset_file, next_offset)?;
                    continue;
                }
            };
        if let Err(reason) = channel_permission_allows_text(permission, &text) {
            skipped_updates += 1;
            warnings.push(format!(
                "Telegram update {update_id} denied by channel command permission: {reason}"
            ));
            write_telegram_offset(&offset_file, next_offset)?;
            continue;
        }
        if let Some(media_group_id) = message
            .get("media_group_id")
            .and_then(serde_json::Value::as_str)
        {
            match telegram_media::buffer_telegram_media_group(
                &args.target_home,
                &account_id,
                &chat_id,
                media_group_id,
                update_id,
                message,
                current_time_ms()?,
                telegram_media::DEFAULT_TELEGRAM_MEDIA_GROUP_DEBOUNCE_MS,
            )? {
                telegram_media::TelegramMediaGroupDecision::Buffered(_) => {
                    write_telegram_offset(&offset_file, next_offset)?;
                    continue;
                }
                telegram_media::TelegramMediaGroupDecision::Flush(flush) => {
                    if execute_telegram_media_group_flush(
                        args,
                        token,
                        &access_policy,
                        &media_fetcher,
                        &flush,
                        &mut warnings,
                    )? {
                        handled_messages += 1;
                    } else {
                        skipped_updates += flush.members.len().max(1);
                    }
                    write_telegram_offset(&offset_file, next_offset)?;
                    continue;
                }
            }
        }
        let media = telegram_media::ingest_telegram_media(
            &args.target_home,
            update_id,
            message,
            &media_fetcher,
        )?;
        warnings.extend(media.warnings.clone());
        if let Err(error) = telegram_send_chat_action(token, &chat_id, "typing") {
            warnings.push(format!(
                "Telegram sendChatAction failed for update {update_id}: {error}"
            ));
        }
        run_channel_once(ChannelRunOnceOptions {
            source: args.source.clone(),
            runtime_workspace: args.runtime_workspace.clone(),
            harness_home: args.target_home.clone(),
            platform: "telegram".to_string(),
            account_id: Some(account_id.clone()),
            channel_id: chat_id,
            user_id,
            agent_id: identity.agent_id.clone(),
            session_key: None,
            message: text,
            inbound_context,
            inbound_media_artifacts: media.artifacts,
            skill_limit: args.skill_limit,
            now_ms: current_time_ms()?,
            codex_executable: args.codex_exe.clone(),
            timeout_ms: args.timeout_ms,
            idle_timeout_ms: args.idle_timeout_ms,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(args.target_home.clone()),
                ..PromptAssemblyOptions::default()
            },
            outbox_limit: args.outbox_limit,
            run_runtime: false,
        })
        .map_err(|err| err.to_string())?;
        handled_messages += 1;
        write_telegram_offset(&offset_file, next_offset)?;
    }
    write_telegram_offset(&offset_file, next_offset)?;

    let delivery = plan_channel_outbox(ChannelOutboxPlanOptions {
        harness_home: args.target_home.clone(),
        platform: Some("telegram".to_string()),
        limit: args.outbox_limit,
    })
    .map_err(|err| err.to_string())?;
    warnings.extend(delivery.warnings.clone());
    let mut delivered_messages = 0;
    let mut failed_deliveries = 0;
    for pending in delivery.pending {
        if outbound_message_account_id(&pending.message)
            != telegram_account_id(args.telegram_account.as_deref())
        {
            continue;
        }
        match telegram_send_outbound_message(&args.target_home, token, &pending.message) {
            Ok(provider_message_id) => {
                record_channel_delivery(ChannelDeliveryRecordOptions {
                    harness_home: args.target_home.clone(),
                    delivery_id: pending.delivery_id,
                    status: ChannelDeliveryStatus::Delivered,
                    platform: pending.message.platform.clone(),
                    account_id: pending.message.account_id.clone(),
                    channel_id: pending.message.channel_id.clone(),
                    user_id: pending.message.user_id.clone(),
                    session_key: pending.message.session_key.clone(),
                    provider_message_id,
                    error: None,
                    now_ms: current_time_ms()?,
                })
                .map_err(|err| err.to_string())?;
                delivered_messages += 1;
            }
            Err(error) => {
                record_channel_delivery(ChannelDeliveryRecordOptions {
                    harness_home: args.target_home.clone(),
                    delivery_id: pending.delivery_id,
                    status: ChannelDeliveryStatus::Failed,
                    platform: pending.message.platform.clone(),
                    account_id: pending.message.account_id.clone(),
                    channel_id: pending.message.channel_id.clone(),
                    user_id: pending.message.user_id.clone(),
                    session_key: pending.message.session_key.clone(),
                    provider_message_id: None,
                    error: Some(error.clone()),
                    now_ms: current_time_ms()?,
                })
                .map_err(|err| err.to_string())?;
                warnings.push(error);
                failed_deliveries += 1;
            }
        }
    }

    let report = TelegramPollOnceReport {
        update_count: updates.len(),
        handled_messages,
        skipped_updates,
        delivered_messages,
        failed_deliveries,
        next_offset,
        warnings,
    };
    append_harness_log(
        &args.target_home,
        &HarnessLogEvent::new(
            current_log_time_ms().map_err(|err| err.to_string())?,
            if report.failed_deliveries == 0 {
                HarnessLogLevel::Info
            } else {
                HarnessLogLevel::Warn
            },
            "telegram",
            "telegram.poll-once",
            format!(
                "updates={} handled={} skipped={} delivered={} failed={}",
                report.update_count,
                report.handled_messages,
                report.skipped_updates,
                report.delivered_messages,
                report.failed_deliveries
            ),
        )
        .path(Some(offset_file)),
    )
    .map_err(|err| err.to_string())?;
    Ok(report)
}

fn execute_telegram_media_group_flush<F: telegram_media::TelegramMediaFetcher>(
    args: &TelegramPollOnceArgs,
    token: &str,
    access_policy: &ChannelAccessPolicy,
    media_fetcher: &F,
    flush: &telegram_media::TelegramMediaGroupFlush,
    warnings: &mut Vec<String>,
) -> Result<bool, String> {
    let Some(first_member) = flush.members.first() else {
        telegram_media::record_telegram_media_group_discarded_no_agent(
            &args.target_home,
            flush,
            "media group had no members",
        )?;
        return Ok(false);
    };
    let first_message = &first_member.message;
    let chat_id = first_message
        .get("chat")
        .and_then(|chat| chat.get("id"))
        .and_then(telegram_id_string)
        .unwrap_or_else(|| flush.chat_id.clone());
    let user_id = first_message
        .get("from")
        .and_then(|from| from.get("id"))
        .and_then(telegram_id_string)
        .unwrap_or_else(|| chat_id.clone());
    let chat_type = first_message
        .get("chat")
        .and_then(|chat| chat.get("type"))
        .and_then(serde_json::Value::as_str);
    let thread_id = first_message
        .get("message_thread_id")
        .and_then(telegram_id_string);
    let identity = resolve_channel_identity(ChannelIdentityLookup {
        harness_home: args.target_home.clone(),
        platform: "telegram".to_string(),
        account_id: flush.account_id.clone(),
        chat_id: chat_id.clone(),
        thread_id,
        requested_agent_id: args.agent_id.clone(),
    })
    .map_err(|err| err.to_string())?;
    if !identity.is_bound() {
        let reason = format!(
            "Telegram media group {} denied by channel identity registry: {}",
            flush.media_group_id, identity.reason
        );
        warnings.push(reason.clone());
        telegram_media::record_telegram_media_group_discarded_no_agent(
            &args.target_home,
            flush,
            &reason,
        )?;
        return Ok(false);
    }
    let permission = match telegram_access_decision(access_policy, &chat_id, &user_id, chat_type) {
        ChannelAccessDecision::Allowed(permission) => permission,
        ChannelAccessDecision::Denied(reason) => {
            let reason = format!(
                "Telegram media group {} denied by channel access policy: {reason}",
                flush.media_group_id
            );
            warnings.push(reason.clone());
            telegram_media::record_telegram_media_group_discarded_no_agent(
                &args.target_home,
                flush,
                &reason,
            )?;
            return Ok(false);
        }
    };
    let text = telegram_media_group_message_text(flush);
    if let Err(reason) = channel_permission_allows_text(permission, &text) {
        let reason = format!(
            "Telegram media group {} denied by channel command permission: {reason}",
            flush.media_group_id
        );
        warnings.push(reason.clone());
        telegram_media::record_telegram_media_group_discarded_no_agent(
            &args.target_home,
            flush,
            &reason,
        )?;
        return Ok(false);
    }

    let mut inbound_media_artifacts = Vec::new();
    for member in &flush.members {
        let media = telegram_media::ingest_telegram_media(
            &args.target_home,
            member.update_id,
            &member.message,
            media_fetcher,
        )?;
        warnings.extend(media.warnings);
        inbound_media_artifacts.extend(media.artifacts);
    }

    if let Err(error) = telegram_send_chat_action(token, &chat_id, "typing") {
        warnings.push(format!(
            "Telegram sendChatAction failed for media group {}: {error}",
            flush.media_group_id
        ));
    }
    run_channel_once(ChannelRunOnceOptions {
        source: args.source.clone(),
        runtime_workspace: args.runtime_workspace.clone(),
        harness_home: args.target_home.clone(),
        platform: "telegram".to_string(),
        account_id: Some(flush.account_id.clone()),
        channel_id: chat_id,
        user_id,
        agent_id: identity.agent_id.clone(),
        session_key: None,
        message: text,
        inbound_context: telegram_media_group_inbound_context(flush),
        inbound_media_artifacts,
        skill_limit: args.skill_limit,
        now_ms: current_time_ms()?,
        codex_executable: args.codex_exe.clone(),
        timeout_ms: args.timeout_ms,
        idle_timeout_ms: args.idle_timeout_ms,
        prompt_options: PromptAssemblyOptions {
            harness_home: Some(args.target_home.clone()),
            ..PromptAssemblyOptions::default()
        },
        outbox_limit: args.outbox_limit,
        run_runtime: false,
    })
    .map_err(|err| err.to_string())?;
    Ok(true)
}

fn telegram_media_group_message_text(flush: &telegram_media::TelegramMediaGroupFlush) -> String {
    flush
        .members
        .iter()
        .find_map(|member| {
            member
                .message
                .get("caption")
                .and_then(serde_json::Value::as_str)
                .filter(|caption| !caption.trim().is_empty())
        })
        .map(ToString::to_string)
        .unwrap_or_else(|| "[telegram media group]".to_string())
}

fn telegram_media_group_inbound_context(
    flush: &telegram_media::TelegramMediaGroupFlush,
) -> Option<String> {
    let mut lines = Vec::new();
    lines.push("## InboundMedia: Telegram media group".to_string());
    lines.push(format!("- mediaGroupId: {}", flush.media_group_id));
    lines.push(format!("- flushStatus: {:?}", flush.status));
    lines.push(format!("- memberCount: {}", flush.members.len()));
    for (index, member) in flush.members.iter().enumerate() {
        let item = index + 1;
        let mut parts = vec![
            format!("item={item}"),
            format!("updateId={}", member.update_id),
        ];
        if let Some(message_id) = member.message_id.as_deref() {
            parts.push(format!("messageId={message_id}"));
        }
        if let Some(preview) = member.caption_preview.as_deref() {
            parts.push(format!(
                "captionPreview={}",
                compact_preview(preview, REPLY_CONTEXT_PREVIEW_MAX_CHARS)
            ));
        }
        lines.push(format!("- {}", parts.join(" ")));
    }

    for (index, member) in flush.members.iter().enumerate() {
        if let Some(context) = telegram_inbound_context(&member.message) {
            lines.push(String::new());
            lines.push(format!(
                "## InboundMedia: Telegram media group member {}",
                index + 1
            ));
            lines.push(context);
        }
    }

    (!flush.members.is_empty()).then(|| lines.join("\n"))
}

fn telegram_message_text(message: &serde_json::Value, has_inbound_context: bool) -> Option<String> {
    telegram_text_or_caption(message)
        .map(ToString::to_string)
        .or_else(|| {
            (has_inbound_context || telegram_has_media(message))
                .then(|| "[telegram media message]".to_string())
        })
}

fn telegram_text_or_caption(message: &serde_json::Value) -> Option<&str> {
    message
        .get("text")
        .and_then(serde_json::Value::as_str)
        .or_else(|| message.get("caption").and_then(serde_json::Value::as_str))
}

const REPLY_CONTEXT_PREVIEW_MAX_CHARS: usize = 700;
const REPLY_CONTEXT_FULL_TEXT_MAX_CHARS: usize = 4000;

#[derive(Debug, Clone, PartialEq, Eq)]
struct BoundedText {
    text: String,
    source_chars: usize,
    truncated: bool,
}

fn telegram_inbound_context(message: &serde_json::Value) -> Option<String> {
    let mut sections = Vec::new();
    if let Some(thread_id) = message
        .get("message_thread_id")
        .and_then(telegram_id_string)
    {
        sections.push(format!(
            "## InboundMessage: Telegram topic context\n- messageThreadId: {thread_id}"
        ));
    }
    if let Some(reply) = message.get("reply_to_message") {
        let mut lines = Vec::new();
        lines.push("## ReferencedMessage: Telegram reply context".to_string());
        if let Some(message_id) = reply.get("message_id").and_then(telegram_id_string) {
            lines.push(format!("- messageId: {message_id}"));
        }
        if let Some(user_id) = reply
            .get("from")
            .and_then(|from| from.get("id"))
            .and_then(telegram_id_string)
        {
            lines.push(format!("- authorUserId: {user_id}"));
        }
        if let Some(date) = reply.get("date").and_then(serde_json::Value::as_i64) {
            lines.push(format!("- unixDate: {date}"));
        }
        if let Some(text) = telegram_text_or_caption(reply) {
            let preview = compact_preview_text(text, REPLY_CONTEXT_PREVIEW_MAX_CHARS);
            let full_text = bounded_text(text, REPLY_CONTEXT_FULL_TEXT_MAX_CHARS);
            lines.push(format!(
                "- textPreview: {}",
                compact_preview_display(&preview)
            ));
            lines.push(format!("- textLength: {}", full_text.source_chars));
            lines.push(format!("- textTruncated: {}", full_text.truncated));
            lines.push(format!(
                "- textMaxChars: {}",
                REPLY_CONTEXT_FULL_TEXT_MAX_CHARS
            ));
            lines.push("- textSource: telegram.reply_to_message".to_string());
            push_indented_text_block(&mut lines, "text", &full_text.text);
        } else {
            lines.push("- textAvailable: false".to_string());
        }
        sections.push(lines.join("\n"));
    }

    let media_lines = telegram_media_context_lines(message);
    if !media_lines.is_empty() {
        sections.push(format!(
            "## InboundMedia: Telegram attachments\n{}",
            media_lines.join("\n")
        ));
    }

    (!sections.is_empty()).then(|| sections.join("\n\n"))
}

fn telegram_has_media(message: &serde_json::Value) -> bool {
    message.as_object().is_some_and(|object| {
        TELEGRAM_MEDIA_FIELDS
            .iter()
            .any(|field| object.contains_key(*field))
    })
}

const TELEGRAM_MEDIA_FIELDS: &[&str] = &[
    "photo",
    "document",
    "video",
    "audio",
    "voice",
    "sticker",
    "animation",
    "video_note",
];

fn telegram_media_context_lines(message: &serde_json::Value) -> Vec<String> {
    let mut lines = Vec::new();
    if let Some(photos) = message.get("photo").and_then(serde_json::Value::as_array) {
        let largest = photos.iter().max_by_key(|photo| {
            photo
                .get("file_size")
                .and_then(serde_json::Value::as_i64)
                .or_else(|| photo.get("width").and_then(serde_json::Value::as_i64))
                .unwrap_or(0)
        });
        let mut parts = vec![
            "kind=photo".to_string(),
            format!("variants={}", photos.len()),
            format!("fileIdPresent={}", yes_no(true)),
        ];
        if let Some(largest) = largest {
            push_json_i64_part(&mut parts, largest, "width");
            push_json_i64_part(&mut parts, largest, "height");
            push_json_i64_part(&mut parts, largest, "file_size");
        }
        lines.push(format!("- {}", parts.join(" ")));
    }

    for kind in TELEGRAM_MEDIA_FIELDS
        .iter()
        .copied()
        .filter(|kind| *kind != "photo")
    {
        if let Some(value) = message.get(kind) {
            let mut parts = vec![
                format!("kind={kind}"),
                format!("fileIdPresent={}", yes_no(value.get("file_id").is_some())),
            ];
            push_json_i64_part(&mut parts, value, "width");
            push_json_i64_part(&mut parts, value, "height");
            push_json_i64_part(&mut parts, value, "duration");
            push_json_i64_part(&mut parts, value, "file_size");
            push_json_string_preview_part(&mut parts, value, "mime_type", 160);
            push_json_string_preview_part(&mut parts, value, "file_name", 240);
            lines.push(format!("- {}", parts.join(" ")));
        }
    }

    if let Some(caption) = message.get("caption").and_then(serde_json::Value::as_str) {
        lines.push(format!(
            "- captionPreview: {}",
            compact_preview(caption, REPLY_CONTEXT_PREVIEW_MAX_CHARS)
        ));
    }
    lines
}

fn push_json_i64_part(parts: &mut Vec<String>, value: &serde_json::Value, key: &str) {
    if let Some(number) = value.get(key).and_then(serde_json::Value::as_i64) {
        parts.push(format!("{key}={number}"));
    }
}

fn push_json_string_preview_part(
    parts: &mut Vec<String>,
    value: &serde_json::Value,
    key: &str,
    max_chars: usize,
) {
    if let Some(text) = value.get(key).and_then(serde_json::Value::as_str) {
        parts.push(format!("{key}={}", compact_preview(text, max_chars)));
    }
}

fn bounded_text(value: &str, max_chars: usize) -> BoundedText {
    let source_chars = value.chars().count();
    let text = value.chars().take(max_chars).collect::<String>();
    BoundedText {
        text,
        source_chars,
        truncated: source_chars > max_chars,
    }
}

fn compact_preview_text(value: &str, max_chars: usize) -> BoundedText {
    let source_chars = value.chars().count();
    let mut out = String::new();
    let mut truncated = false;
    for (index, ch) in value.chars().enumerate() {
        if index >= max_chars {
            truncated = true;
            break;
        }
        if ch == '\r' || ch == '\n' || ch == '\t' {
            out.push(' ');
        } else {
            out.push(ch);
        }
    }
    let text = out.split_whitespace().collect::<Vec<_>>().join(" ");
    BoundedText {
        text,
        source_chars,
        truncated,
    }
}

fn compact_preview(value: &str, max_chars: usize) -> String {
    let preview = compact_preview_text(value, max_chars);
    compact_preview_display(&preview)
}

fn compact_preview_display(preview: &BoundedText) -> String {
    if preview.truncated {
        format!("{}...", preview.text)
    } else {
        preview.text.clone()
    }
}

fn push_indented_text_block(lines: &mut Vec<String>, field: &str, text: &str) {
    lines.push(format!("- {field}:"));
    if text.is_empty() {
        lines.push("  ".to_string());
        return;
    }
    for line in text.lines() {
        lines.push(format!("  {line}"));
    }
}

fn run_discord_outbox_send_once(args: &[String]) -> Result<(), String> {
    let args = discord_outbox_send_once_args_from_args(args)?;
    let token = discord_bot_token(&args.target_home, args.discord_account.as_deref())?;
    let report = execute_discord_outbox_send_once(&args, &token)?;
    print_discord_outbox_send_once_report(&report);
    Ok(())
}

fn run_discord_outbox_loop(args: &[String]) -> Result<(), String> {
    let args = discord_outbox_loop_args_from_args(args)?;
    let token = discord_bot_token(&args.send.target_home, args.send.discord_account.as_deref())?;
    let mut iterations = 0usize;
    let mut consecutive_errors = 0usize;

    loop {
        let outbox_wake_sequence = read_loop_wake_sequence(&args.send.target_home, "final-outbox");
        if stop_file_requested(args.stop_file.as_deref()) {
            append_loop_stop_log(
                &args.send.target_home,
                "discord",
                "discord.outbox-loop-stopped",
                iterations,
                "stop file requested",
            )?;
            write_loop_heartbeat(
                &args.send.target_home,
                "discord-outbox-loop",
                "stopped",
                iterations,
                "stop file requested",
            )?;
            println!("Discord outbox loop stop requested after {iterations} iteration(s)");
            break;
        }
        iterations += 1;
        write_loop_heartbeat(
            &args.send.target_home,
            "discord-outbox-loop",
            "running",
            iterations,
            "checking Discord outbox",
        )?;
        match execute_discord_outbox_send_once(&args.send, &token) {
            Ok(report) => {
                consecutive_errors = 0;
                write_loop_heartbeat(
                    &args.send.target_home,
                    "discord-outbox-loop",
                    "ok",
                    iterations,
                    &format!(
                        "pending={} delivered={} failed={}",
                        report.pending_count, report.delivered_messages, report.failed_deliveries
                    ),
                )?;
                println!("Discord outbox loop iteration: {iterations}");
                print_discord_outbox_send_once_report(&report);
            }
            Err(error) => {
                consecutive_errors += 1;
                write_loop_heartbeat(
                    &args.send.target_home,
                    "discord-outbox-loop",
                    "error",
                    iterations,
                    &format!("consecutiveErrors={consecutive_errors} error={error}"),
                )?;
                eprintln!(
                    "discord-outbox-loop iteration {iterations} failed ({consecutive_errors}/{}): {error}",
                    args.max_consecutive_errors
                );
                let _ = append_harness_log(
                    &args.send.target_home,
                    &HarnessLogEvent::new(
                        current_log_time_ms().unwrap_or(0),
                        HarnessLogLevel::Warn,
                        "discord",
                        "discord.outbox-loop-error",
                        format!(
                            "iteration={iterations} consecutiveErrors={consecutive_errors} error={error}"
                        ),
                    ),
                );
                if consecutive_errors >= args.max_consecutive_errors {
                    return Err(format!(
                        "discord-outbox-loop exceeded {} consecutive errors; last error: {error}",
                        args.max_consecutive_errors
                    ));
                }
            }
        }

        if args.iterations > 0 && iterations >= args.iterations {
            break;
        }
        wait_for_loop_wake_since(
            &args.send.target_home,
            "final-outbox",
            outbox_wake_sequence,
            args.idle_ms,
        );
    }

    Ok(())
}

fn run_discord_dm_probe(args: &[String]) -> Result<(), String> {
    let args = discord_dm_probe_args_from_args(args)?;
    let token = discord_bot_token(&args.target_home, None)?;
    let report = execute_discord_dm_probe(&args, &token);
    write_discord_dm_probe_report(&report)?;
    append_harness_log(
        &args.target_home,
        &HarnessLogEvent::new(
            current_log_time_ms().map_err(|err| err.to_string())?,
            if report.status == "ready" {
                HarnessLogLevel::Info
            } else {
                HarnessLogLevel::Warn
            },
            "discord",
            "discord.dm-probe",
            report.reason.clone(),
        ),
    )
    .map_err(|err| err.to_string())?;
    print_discord_dm_probe_report(&report);
    if report.status == "ready" {
        Ok(())
    } else {
        Err(report.reason)
    }
}

fn run_discord_dm_history_probe(args: &[String]) -> Result<(), String> {
    let args = discord_dm_history_probe_args_from_args(args)?;
    let token = discord_bot_token(&args.target_home, None)?;
    let report = execute_discord_dm_history_probe(&args, &token);
    write_discord_dm_history_probe_report(&report)?;
    append_harness_log(
        &args.target_home,
        &HarnessLogEvent::new(
            current_log_time_ms().map_err(|err| err.to_string())?,
            if report.status == "ready" {
                HarnessLogLevel::Info
            } else {
                HarnessLogLevel::Warn
            },
            "discord",
            "discord.dm-history-probe",
            report.reason.clone(),
        ),
    )
    .map_err(|err| err.to_string())?;
    print_discord_dm_history_probe_report(&report);
    if report.status == "ready" {
        Ok(())
    } else {
        Err(report.reason)
    }
}

fn execute_discord_dm_history_probe(
    args: &DiscordDmHistoryProbeArgs,
    token: &str,
) -> DiscordDmHistoryProbeReport {
    let mut warnings = Vec::new();
    let probe = if args.channel_id.is_none() || args.user_id.is_none() {
        match latest_discord_dm_probe(&args.target_home) {
            Ok(probe) => probe,
            Err(error) => {
                return DiscordDmHistoryProbeReport {
                    harness_home: args.target_home.clone(),
                    status: "failed".to_string(),
                    reason: error,
                    channel_id: args.channel_id.clone(),
                    user_id: args.user_id.clone(),
                    limit: args.limit,
                    message_count: 0,
                    user_message_count: 0,
                    bot_message_count: 0,
                    messages: Vec::new(),
                    warnings,
                };
            }
        }
    } else {
        DiscordDmProbeRef::default()
    };
    let channel_id = args
        .channel_id
        .clone()
        .or(probe.channel_id)
        .unwrap_or_default();
    let user_id = args.user_id.clone().or(probe.user_id);
    if channel_id.trim().is_empty() {
        return DiscordDmHistoryProbeReport {
            harness_home: args.target_home.clone(),
            status: "failed".to_string(),
            reason: "Discord DM channel id was not provided and latest discord-dm-probe has no channel id"
                .to_string(),
            channel_id: None,
            user_id,
            limit: args.limit,
            message_count: 0,
            user_message_count: 0,
            bot_message_count: 0,
            messages: Vec::new(),
            warnings,
        };
    }

    let messages = match discord_fetch_channel_messages(token, &channel_id, args.limit) {
        Ok(messages) => messages,
        Err(error) => {
            return DiscordDmHistoryProbeReport {
                harness_home: args.target_home.clone(),
                status: "failed".to_string(),
                reason: format!("failed to read Discord DM channel messages: {error}"),
                channel_id: Some(channel_id),
                user_id,
                limit: args.limit,
                message_count: 0,
                user_message_count: 0,
                bot_message_count: 0,
                messages: Vec::new(),
                warnings,
            };
        }
    };
    let message_count = messages.len();
    let bot_message_count = messages
        .iter()
        .filter(|message| message.author_bot.unwrap_or(false))
        .count();
    let user_message_count = messages
        .iter()
        .filter(|message| !message.author_bot.unwrap_or(false))
        .count();
    if user_message_count == 0 {
        warnings.push(
            "Discord DM history was readable but no non-bot messages were found in the sampled window"
                .to_string(),
        );
    }
    DiscordDmHistoryProbeReport {
        harness_home: args.target_home.clone(),
        status: "ready".to_string(),
        reason: format!(
            "Discord DM history was readable; messages={message_count}, userMessages={user_message_count}, botMessages={bot_message_count}"
        ),
        channel_id: Some(channel_id),
        user_id,
        limit: args.limit,
        message_count,
        user_message_count,
        bot_message_count,
        messages,
        warnings,
    }
}

fn execute_discord_dm_probe(args: &DiscordDmProbeArgs, token: &str) -> DiscordDmProbeReport {
    let mut warnings = Vec::new();
    let channel_id = match discord_create_dm_channel(token, &args.user_id) {
        Ok(channel_id) => channel_id,
        Err(error) => {
            return DiscordDmProbeReport {
                harness_home: args.target_home.clone(),
                status: "failed".to_string(),
                reason: format!("failed to create Discord DM channel: {error}"),
                user_id: args.user_id.clone(),
                channel_id: None,
                provider_message_id: None,
                sent_message: false,
                warnings,
            };
        }
    };

    let provider_message_id = if args.send_message {
        match discord_send_message(token, &channel_id, &args.message, None) {
            Ok(provider_message_id) => provider_message_id,
            Err(error) => {
                return DiscordDmProbeReport {
                    harness_home: args.target_home.clone(),
                    status: "failed".to_string(),
                    reason: format!(
                        "created Discord DM channel but failed to send probe message: {error}"
                    ),
                    user_id: args.user_id.clone(),
                    channel_id: Some(channel_id),
                    provider_message_id: None,
                    sent_message: false,
                    warnings,
                };
            }
        }
    } else {
        warnings.push("probe message send skipped by --no-send".to_string());
        None
    };

    DiscordDmProbeReport {
        harness_home: args.target_home.clone(),
        status: "ready".to_string(),
        reason: if args.send_message {
            "Discord DM channel was created and probe message was sent".to_string()
        } else {
            "Discord DM channel was created; probe message send skipped".to_string()
        },
        user_id: args.user_id.clone(),
        channel_id: Some(channel_id),
        provider_message_id,
        sent_message: args.send_message,
        warnings,
    }
}

fn execute_discord_outbox_send_once(
    args: &DiscordOutboxSendOnceArgs,
    token: &str,
) -> Result<DiscordOutboxSendOnceReport, String> {
    let delivery = plan_channel_outbox(ChannelOutboxPlanOptions {
        harness_home: args.target_home.clone(),
        platform: Some("discord".to_string()),
        limit: args.outbox_limit,
    })
    .map_err(|err| err.to_string())?;
    let mut warnings = delivery.warnings.clone();
    let pending_count = delivery.pending.len();
    let mut delivered_messages = 0;
    let mut failed_deliveries = 0;

    for pending in delivery.pending {
        if outbound_message_account_id(&pending.message)
            != discord_account_id(args.discord_account.as_deref())
        {
            continue;
        }
        match discord_send_outbound_message(&token, &pending.message) {
            Ok(provider_message_id) => {
                record_channel_delivery(ChannelDeliveryRecordOptions {
                    harness_home: args.target_home.clone(),
                    delivery_id: pending.delivery_id,
                    status: ChannelDeliveryStatus::Delivered,
                    platform: pending.message.platform.clone(),
                    account_id: pending.message.account_id.clone(),
                    channel_id: pending.message.channel_id.clone(),
                    user_id: pending.message.user_id.clone(),
                    session_key: pending.message.session_key.clone(),
                    provider_message_id,
                    error: None,
                    now_ms: current_time_ms()?,
                })
                .map_err(|err| err.to_string())?;
                delivered_messages += 1;
            }
            Err(error) => {
                record_channel_delivery(ChannelDeliveryRecordOptions {
                    harness_home: args.target_home.clone(),
                    delivery_id: pending.delivery_id,
                    status: ChannelDeliveryStatus::Failed,
                    platform: pending.message.platform.clone(),
                    account_id: pending.message.account_id.clone(),
                    channel_id: pending.message.channel_id.clone(),
                    user_id: pending.message.user_id.clone(),
                    session_key: pending.message.session_key.clone(),
                    provider_message_id: None,
                    error: Some(error.clone()),
                    now_ms: current_time_ms()?,
                })
                .map_err(|err| err.to_string())?;
                warnings.push(error);
                failed_deliveries += 1;
            }
        }
    }

    let report = DiscordOutboxSendOnceReport {
        pending_count,
        delivered_messages,
        failed_deliveries,
        warnings,
    };
    append_harness_log(
        &args.target_home,
        &HarnessLogEvent::new(
            current_log_time_ms().map_err(|err| err.to_string())?,
            if report.failed_deliveries == 0 {
                HarnessLogLevel::Info
            } else {
                HarnessLogLevel::Warn
            },
            "discord",
            "discord.outbox-send-once",
            format!(
                "pending={} delivered={} failed={}",
                report.pending_count, report.delivered_messages, report.failed_deliveries
            ),
        ),
    )
    .map_err(|err| err.to_string())?;
    Ok(report)
}

fn run_discord_event_run_once(args: &[String]) -> Result<(), String> {
    let args = discord_event_run_once_args_from_args(args)?;
    let event = read_discord_event_json(&args)?;
    let parsed = parse_discord_gateway_message(&event)?;
    let access_policy = channel_access_policy(&args.target_home)?;
    let report = match parsed {
        None => DiscordEventRunOnceReport {
            harness_home: args.target_home.clone(),
            status: "skipped".to_string(),
            reason: "event is not a Discord MESSAGE_CREATE payload".to_string(),
            message_id: None,
            guild_id: None,
            channel_id: None,
            user_id: None,
            run: None,
        },
        Some(message) if message.author_is_bot => {
            let report = DiscordEventRunOnceReport {
                harness_home: args.target_home.clone(),
                status: "skipped".to_string(),
                reason: "message author is a bot".to_string(),
                message_id: Some(message.message_id),
                guild_id: message.guild_id,
                channel_id: Some(message.channel_id),
                user_id: Some(message.user_id),
                run: None,
            };
            write_discord_event_receipt(&report)?;
            report
        }
        Some(message) if message.content.trim().is_empty() && message.inbound_context.is_none() => {
            let report = DiscordEventRunOnceReport {
                harness_home: args.target_home.clone(),
                status: "skipped".to_string(),
                reason: "message content is empty".to_string(),
                message_id: Some(message.message_id),
                guild_id: message.guild_id,
                channel_id: Some(message.channel_id),
                user_id: Some(message.user_id),
                run: None,
            };
            write_discord_event_receipt(&report)?;
            report
        }
        Some(mut message) => {
            let message_text = discord_message_text(&message);
            let report = match discord_access_decision(&access_policy, &message) {
                ChannelAccessDecision::Denied(reason) => DiscordEventRunOnceReport {
                    harness_home: args.target_home.clone(),
                    status: "denied".to_string(),
                    reason,
                    message_id: Some(message.message_id),
                    guild_id: message.guild_id,
                    channel_id: Some(message.channel_id),
                    user_id: Some(message.user_id),
                    run: None,
                },
                ChannelAccessDecision::Allowed(permission) => {
                    if let Err(reason) = channel_permission_allows_text(permission, &message_text) {
                        DiscordEventRunOnceReport {
                            harness_home: args.target_home.clone(),
                            status: "denied".to_string(),
                            reason,
                            message_id: Some(message.message_id),
                            guild_id: message.guild_id,
                            channel_id: Some(message.channel_id),
                            user_id: Some(message.user_id),
                            run: None,
                        }
                    } else if discord_event_seen(&args.target_home, &message.message_id)? {
                        DiscordEventRunOnceReport {
                            harness_home: args.target_home.clone(),
                            status: "duplicate".to_string(),
                            reason: "message id already has a Discord event receipt".to_string(),
                            message_id: Some(message.message_id),
                            guild_id: message.guild_id,
                            channel_id: Some(message.channel_id),
                            user_id: Some(message.user_id),
                            run: None,
                        }
                    } else {
                        let account_id = discord_account_id(args.discord_account.as_deref());
                        let identity = resolve_channel_identity(ChannelIdentityLookup {
                            harness_home: args.target_home.clone(),
                            platform: "discord".to_string(),
                            account_id: account_id.clone(),
                            chat_id: message.channel_id.clone(),
                            thread_id: None,
                            requested_agent_id: args.agent_id.clone(),
                        })
                        .map_err(|err| err.to_string())?;
                        if !identity.is_bound() {
                            DiscordEventRunOnceReport {
                                harness_home: args.target_home.clone(),
                                status: "denied".to_string(),
                                reason: format!(
                                    "channel identity registry denied event: {}",
                                    identity.reason
                                ),
                                message_id: Some(message.message_id),
                                guild_id: message.guild_id,
                                channel_id: Some(message.channel_id),
                                user_id: Some(message.user_id),
                                run: None,
                            }
                        } else {
                            attach_discord_message_artifacts(
                                &args.target_home,
                                &mut message,
                                &HttpDiscordAttachmentFetcher,
                            )?;
                            if let Ok(token) = discord_bot_token(
                                &args.target_home,
                                args.discord_account.as_deref(),
                            ) && let Err(error) =
                                discord_send_typing(&token, &message.channel_id)
                            {
                                let _ = append_harness_log(
                                    &args.target_home,
                                    &HarnessLogEvent::new(
                                        current_log_time_ms().unwrap_or(0),
                                        HarnessLogLevel::Warn,
                                        "discord",
                                        "discord.typing-failed",
                                        error,
                                    ),
                                );
                            }
                            let run = run_channel_once(ChannelRunOnceOptions {
                                source: args.source.clone(),
                                runtime_workspace: args.runtime_workspace.clone(),
                                harness_home: args.target_home.clone(),
                                platform: "discord".to_string(),
                                account_id: Some(account_id),
                                channel_id: message.channel_id.clone(),
                                user_id: message.user_id.clone(),
                                agent_id: identity.agent_id.clone(),
                                session_key: None,
                                message: message_text,
                                inbound_context: message.inbound_context.clone(),
                                inbound_media_artifacts: message.inbound_media_artifacts.clone(),
                                skill_limit: args.skill_limit,
                                now_ms: current_time_ms()?,
                                codex_executable: args.codex_exe.clone(),
                                timeout_ms: args.timeout_ms,
                                idle_timeout_ms: args.idle_timeout_ms,
                                prompt_options: PromptAssemblyOptions {
                                    harness_home: Some(args.target_home.clone()),
                                    ..PromptAssemblyOptions::default()
                                },
                                outbox_limit: args.outbox_limit,
                                run_runtime: false,
                            })
                            .map_err(|err| err.to_string())?;
                            if message.reply_context.is_some() {
                                write_discord_reply_context_receipt(
                                    &args.target_home,
                                    &message,
                                    "captured",
                                )?;
                            }
                            DiscordEventRunOnceReport {
                                harness_home: args.target_home.clone(),
                                status: "handled".to_string(),
                                reason: "Discord message normalized into channel-run-once"
                                    .to_string(),
                                message_id: Some(message.message_id),
                                guild_id: message.guild_id,
                                channel_id: Some(message.channel_id),
                                user_id: Some(message.user_id),
                                run: Some(run),
                            }
                        }
                    }
                }
            };
            write_discord_event_receipt(&report)?;
            report
        }
    };
    append_harness_log(
        &args.target_home,
        &HarnessLogEvent::new(
            current_log_time_ms().map_err(|err| err.to_string())?,
            if report.status == "handled" {
                HarnessLogLevel::Info
            } else {
                HarnessLogLevel::Warn
            },
            "discord",
            "discord.event-run-once",
            format!("status={} reason={}", report.status, report.reason),
        ),
    )
    .map_err(|err| err.to_string())?;
    print_discord_event_run_once_report(&report);
    Ok(())
}

fn run_discord_gateway_probe(args: &[String]) -> Result<(), String> {
    let args = discord_gateway_args_from_args(args)?;
    let output = discord_gateway_command(&args)
        .arg("--probe")
        .arg("--write-receipt")
        .output()
        .map_err(|err| {
            format!(
                "failed to spawn Discord gateway probe via {}: {err}",
                args.node_exe.display()
            )
        })?;
    println!("Agent Harness Discord gateway probe");
    println!("Harness home: {}", args.target_home.display());
    println!("Node executable: {}", args.node_exe.display());
    println!("Gateway script: {}", args.gateway_script.display());
    if !output.stdout.is_empty() {
        println!("{}", String::from_utf8_lossy(&output.stdout).trim_end());
    }
    if !output.stderr.is_empty() {
        eprintln!("{}", String::from_utf8_lossy(&output.stderr).trim_end());
    }
    if !output.status.success() {
        return Err(format!(
            "Discord gateway probe exited with {}",
            output.status
        ));
    }
    Ok(())
}

fn run_discord_gateway_loop(args: &[String]) -> Result<(), String> {
    let args = discord_gateway_args_from_args(args)?;
    if args.max_messages > 0 {
        let status = discord_gateway_command(&args).status().map_err(|err| {
            format!(
                "failed to spawn Discord gateway loop via {}: {err}",
                args.node_exe.display()
            )
        })?;
        if !status.success() {
            return Err(format!("Discord gateway loop exited with {status}"));
        }
        return Ok(());
    }

    let mut iterations = 0usize;
    let mut consecutive_errors = 0usize;
    let max_consecutive_errors = 5usize;
    loop {
        if stop_file_requested(args.stop_file.as_deref()) {
            append_loop_stop_log(
                &args.target_home,
                "discord",
                "discord.gateway-loop-stopped",
                iterations,
                "stop file requested",
            )?;
            write_loop_heartbeat(
                &args.target_home,
                "discord-gateway-loop",
                "stopped",
                iterations,
                "stop file requested",
            )?;
            println!("Discord gateway loop stop requested after {iterations} iteration(s)");
            break;
        }
        iterations += 1;
        if let Some(request) =
            consume_gateway_restart_request(&args.target_home, "discord-gateway-loop")?
        {
            write_loop_heartbeat(
                &args.target_home,
                "discord-gateway-loop",
                "restarting",
                iterations,
                &format!(
                    "gateway restart request consumed before spawn: {}",
                    request.detail
                ),
            )?;
        }
        write_loop_heartbeat(
            &args.target_home,
            "discord-gateway-loop",
            "spawning",
            iterations,
            "starting Discord gateway subprocess",
        )?;
        match run_discord_gateway_child_until_exit_or_restart(&args) {
            Ok(DiscordGatewayChildOutcome::StopRequested) => {
                append_loop_stop_log(
                    &args.target_home,
                    "discord",
                    "discord.gateway-loop-stopped",
                    iterations,
                    "stop file requested while gateway subprocess was running",
                )?;
                write_loop_heartbeat(
                    &args.target_home,
                    "discord-gateway-loop",
                    "stopped",
                    iterations,
                    "stop file requested while gateway subprocess was running",
                )?;
                break;
            }
            Ok(DiscordGatewayChildOutcome::RestartRequested(request)) => {
                consecutive_errors = 0;
                write_loop_heartbeat(
                    &args.target_home,
                    "discord-gateway-loop",
                    "restarting",
                    iterations,
                    &format!("gateway restart request consumed: {}", request.detail),
                )?;
                append_harness_log(
                    &args.target_home,
                    &HarnessLogEvent::new(
                        current_log_time_ms().map_err(|err| err.to_string())?,
                        HarnessLogLevel::Warn,
                        "discord",
                        "discord.gateway-loop-requested-restart",
                        format!(
                            "iteration={iterations} requestFile={}",
                            request.request_file.display()
                        ),
                    ),
                )
                .map_err(|err| err.to_string())?;
            }
            Ok(DiscordGatewayChildOutcome::Exited(status)) if status.success() => {
                consecutive_errors = 0;
                write_loop_heartbeat(
                    &args.target_home,
                    "discord-gateway-loop",
                    "restarting",
                    iterations,
                    "gateway subprocess exited cleanly; restarting",
                )?;
                append_harness_log(
                    &args.target_home,
                    &HarnessLogEvent::new(
                        current_log_time_ms().map_err(|err| err.to_string())?,
                        HarnessLogLevel::Warn,
                        "discord",
                        "discord.gateway-loop-restart",
                        format!("gateway subprocess exited cleanly on iteration {iterations}; restarting"),
                    ),
                )
                .map_err(|err| err.to_string())?;
            }
            Ok(DiscordGatewayChildOutcome::Exited(status)) => {
                consecutive_errors += 1;
                let error = format!("Discord gateway subprocess exited with {status}");
                write_loop_heartbeat(
                    &args.target_home,
                    "discord-gateway-loop",
                    "error",
                    iterations,
                    &format!("consecutiveErrors={consecutive_errors} error={error}"),
                )?;
                append_harness_log(
                    &args.target_home,
                    &HarnessLogEvent::new(
                        current_log_time_ms().map_err(|err| err.to_string())?,
                        HarnessLogLevel::Warn,
                        "discord",
                        "discord.gateway-loop-error",
                        format!(
                            "iteration={iterations} consecutiveErrors={consecutive_errors} error={error}"
                        ),
                    ),
                )
                .map_err(|err| err.to_string())?;
                eprintln!(
                    "discord-gateway-loop iteration {iterations} failed ({consecutive_errors}/{max_consecutive_errors}): {error}"
                );
                if consecutive_errors >= max_consecutive_errors {
                    return Err(format!(
                        "discord-gateway-loop exceeded {max_consecutive_errors} consecutive errors; last error: {error}"
                    ));
                }
            }
            Err(error) => {
                consecutive_errors += 1;
                let error = format!(
                    "failed to spawn Discord gateway loop via {}: {error}",
                    args.node_exe.display()
                );
                write_loop_heartbeat(
                    &args.target_home,
                    "discord-gateway-loop",
                    "error",
                    iterations,
                    &format!("consecutiveErrors={consecutive_errors} error={error}"),
                )?;
                append_harness_log(
                    &args.target_home,
                    &HarnessLogEvent::new(
                        current_log_time_ms().map_err(|err| err.to_string())?,
                        HarnessLogLevel::Warn,
                        "discord",
                        "discord.gateway-loop-error",
                        format!(
                            "iteration={iterations} consecutiveErrors={consecutive_errors} error={error}"
                        ),
                    ),
                )
                .map_err(|err| err.to_string())?;
                eprintln!(
                    "discord-gateway-loop iteration {iterations} failed ({consecutive_errors}/{max_consecutive_errors}): {error}"
                );
                if consecutive_errors >= max_consecutive_errors {
                    return Err(format!(
                        "discord-gateway-loop exceeded {max_consecutive_errors} consecutive errors; last error: {error}"
                    ));
                }
            }
        }
        thread::sleep(Duration::from_millis(1_000));
    }
    Ok(())
}

struct GatewayRestartConsumption {
    request_file: PathBuf,
    detail: String,
}

enum DiscordGatewayChildOutcome {
    Exited(std::process::ExitStatus),
    RestartRequested(GatewayRestartConsumption),
    StopRequested,
}

fn run_discord_gateway_child_until_exit_or_restart(
    args: &DiscordGatewayArgs,
) -> Result<DiscordGatewayChildOutcome, String> {
    let mut child = discord_gateway_command(args)
        .spawn()
        .map_err(|err| err.to_string())?;
    loop {
        if stop_file_requested(args.stop_file.as_deref()) {
            let _ = child.kill();
            let _ = child.wait();
            return Ok(DiscordGatewayChildOutcome::StopRequested);
        }
        if let Some(request) =
            consume_gateway_restart_request(&args.target_home, "discord-gateway-loop")?
        {
            let _ = child.kill();
            let _ = child.wait();
            return Ok(DiscordGatewayChildOutcome::RestartRequested(request));
        }
        match child.try_wait().map_err(|err| err.to_string())? {
            Some(status) => return Ok(DiscordGatewayChildOutcome::Exited(status)),
            None => thread::sleep(Duration::from_millis(1_000)),
        }
    }
}

fn consume_gateway_restart_request(
    harness_home: &Path,
    consumer: &str,
) -> Result<Option<GatewayRestartConsumption>, String> {
    let request_dir = harness_home
        .join("state")
        .join("supervisor")
        .join("gateway-restart-requests");
    let entries = match fs::read_dir(&request_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(format!(
                "failed to read gateway restart request dir {}: {error}",
                request_dir.display()
            ));
        }
    };
    let mut request_files = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|err| err.to_string())?;
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) == Some("json") {
            request_files.push(path);
        }
    }
    request_files.sort();
    for request_file in request_files {
        let text = fs::read_to_string(&request_file).map_err(|err| {
            format!(
                "failed to read gateway restart request {}: {err}",
                request_file.display()
            )
        })?;
        let mut value: serde_json::Value = serde_json::from_str(&text).map_err(|err| {
            format!(
                "invalid gateway restart request {}: {err}",
                request_file.display()
            )
        })?;
        let status = value
            .get("status")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("requested");
        if status != "requested" {
            continue;
        }
        let consumed_at_ms = current_time_ms()?;
        let reason = value
            .get("reason")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("gateway restart requested")
            .to_string();
        if let Some(object) = value.as_object_mut() {
            object.insert(
                "status".to_string(),
                serde_json::Value::String("consumed".to_string()),
            );
            object.insert(
                "consumedBy".to_string(),
                serde_json::Value::String(consumer.to_string()),
            );
            object.insert(
                "consumedAtMs".to_string(),
                serde_json::Value::Number(consumed_at_ms.into()),
            );
            object.insert(
                "consumedRequestFile".to_string(),
                serde_json::Value::String(request_file.display().to_string()),
            );
        }
        let consumed_dir = request_dir.join("consumed");
        fs::create_dir_all(&consumed_dir).map_err(|err| {
            format!(
                "failed to create consumed gateway restart dir {}: {err}",
                consumed_dir.display()
            )
        })?;
        let consumed_file = consumed_dir.join(format!(
            "{}.consumed.json",
            request_file
                .file_stem()
                .and_then(|value| value.to_str())
                .unwrap_or("gateway-restart-request")
        ));
        write_json_atomic(&consumed_file, &value).map_err(|err| {
            format!(
                "failed to write consumed gateway restart request {}: {err}",
                consumed_file.display()
            )
        })?;
        fs::remove_file(&request_file).map_err(|err| {
            format!(
                "failed to remove consumed gateway restart request {}: {err}",
                request_file.display()
            )
        })?;
        let receipt_file = harness_home
            .join("state")
            .join("supervisor")
            .join("gateway-restart-requests.jsonl");
        append_jsonl_value(&receipt_file, &value).map_err(|err| {
            format!(
                "failed to append gateway restart consumption receipt {}: {err}",
                receipt_file.display()
            )
        })?;
        return Ok(Some(GatewayRestartConsumption {
            request_file,
            detail: reason,
        }));
    }
    Ok(None)
}

fn discord_gateway_command(args: &DiscordGatewayArgs) -> Command {
    let mut command = Command::new(&args.node_exe);
    command
        .arg(&args.gateway_script)
        .arg("--harness-home")
        .arg(&args.target_home)
        .arg("--source-home")
        .arg(&args.source.home)
        .arg("--harness-cli")
        .arg(&args.harness_cli)
        .arg("--max-messages")
        .arg(args.max_messages.to_string());
    if args.source.workspace != args.source.home.join("workspace") {
        command.arg("--workspace").arg(&args.source.workspace);
    }
    if let Some(runtime_workspace) = &args.runtime_workspace {
        command.arg("--runtime-workspace").arg(runtime_workspace);
    }
    if let Some(agent_id) = &args.agent_id {
        command.arg("--agent").arg(agent_id);
    }
    if let Some(account_id) = &args.discord_account {
        command.arg("--discord-account").arg(account_id);
    }
    if let Some(codex_exe) = &args.codex_exe {
        command.arg("--codex-exe").arg(codex_exe);
    }
    if let Some(stop_file) = &args.stop_file {
        command.arg("--stop-file").arg(stop_file);
    }
    if env::var_os("DISCORD_BOT_TOKEN").is_none()
        && let Ok(token) = discord_bot_token(&args.target_home, args.discord_account.as_deref())
    {
        command.env("DISCORD_BOT_TOKEN", token);
    }
    command
}

fn run_plugin_sidecar_probe(args: &[String]) -> Result<(), String> {
    let args = plugin_sidecar_probe_args_from_args(args)?;
    let output = Command::new(&args.node_exe)
        .arg(&args.sidecar_script)
        .arg("--harness-home")
        .arg(&args.target_home)
        .arg("--probe")
        .arg("--write-receipt")
        .output()
        .map_err(|err| {
            format!(
                "failed to spawn plugin sidecar probe via {}: {err}",
                args.node_exe.display()
            )
        })?;

    println!("Agent Harness plugin sidecar probe");
    println!("Harness home: {}", args.target_home.display());
    println!("Node executable: {}", args.node_exe.display());
    println!("Sidecar script: {}", args.sidecar_script.display());
    if !output.stdout.is_empty() {
        println!("{}", String::from_utf8_lossy(&output.stdout).trim_end());
    }
    if !output.stderr.is_empty() {
        eprintln!("{}", String::from_utf8_lossy(&output.stderr).trim_end());
    }
    if !output.status.success() {
        return Err(format!(
            "plugin sidecar probe exited with {}",
            output.status
        ));
    }
    Ok(())
}

fn run_plugin_sidecar_call(args: &[String]) -> Result<(), String> {
    let args = plugin_sidecar_call_args_from_args(args)?;
    let request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": "agent-harness-cli",
        "method": args.method,
        "params": args.params
    });
    let mut child = Command::new(&args.node_exe)
        .arg(&args.sidecar_script)
        .arg("--harness-home")
        .arg(&args.target_home)
        .arg("--stdio")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|err| {
            format!(
                "failed to spawn plugin sidecar via {}: {err}",
                args.node_exe.display()
            )
        })?;
    {
        let stdin = child
            .stdin
            .as_mut()
            .ok_or_else(|| "plugin sidecar stdin was not available".to_string())?;
        stdin
            .write_all(format!("{request}\n").as_bytes())
            .map_err(|err| format!("failed to write JSON-RPC request: {err}"))?;
    }
    drop(child.stdin.take());

    let output = child
        .wait_with_output()
        .map_err(|err| format!("failed to wait for plugin sidecar response: {err}"))?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let response_line = stdout
        .lines()
        .find(|line| !line.trim().is_empty())
        .unwrap_or("");
    let response: serde_json::Value = if response_line.is_empty() {
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": "agent-harness-cli",
            "error": {
                "code": -32001,
                "message": "sidecar returned no JSON-RPC response"
            }
        })
    } else {
        serde_json::from_str(response_line)
            .map_err(|err| format!("invalid JSON-RPC response from plugin sidecar: {err}"))?
    };
    let status = if output.status.success() && response.get("error").is_none() {
        "ok"
    } else {
        "failed"
    };
    write_plugin_sidecar_bridge_receipt(
        &args.target_home,
        &args.method,
        status,
        &response,
        stderr.trim(),
    )?;

    println!("Agent Harness plugin sidecar call");
    println!("Harness home: {}", args.target_home.display());
    println!("Node executable: {}", args.node_exe.display());
    println!("Sidecar script: {}", args.sidecar_script.display());
    println!("Method: {}", args.method);
    println!("Status: {status}");
    println!(
        "{}",
        serde_json::to_string_pretty(&response).map_err(|err| err.to_string())?
    );
    if !stderr.trim().is_empty() {
        eprintln!("{}", stderr.trim());
    }
    if status != "ok" {
        return Err("plugin sidecar JSON-RPC call failed".to_string());
    }
    Ok(())
}

fn run_queue_enqueue(args: &[String]) -> Result<(), String> {
    let args = queue_enqueue_args_from_args(args)?;
    let registry = load_agent_registry(&args.turn.source).map_err(|err| err.to_string())?;
    let skill_index = build_runtime_skill_index(&args.turn.source, &args.target_home)
        .map_err(|err| err.to_string())?;
    let plan = build_turn_plan(
        &args.turn.source,
        &registry,
        &skill_index,
        TurnPlanInput {
            harness_home: Some(args.target_home.clone()),
            platform: args.turn.platform,
            channel_id: args.turn.channel_id,
            user_id: args.turn.user_id,
            text: args.turn.message,
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            requested_agent_id: args.turn.agent_id,
            session_hint: args.turn.session_key,
            skill_limit: args.turn.skill_limit,
        },
    )
    .map_err(|err| err.to_string())?;
    let step = build_channel_step(&registry, &plan);
    let report = enqueue_channel_step(
        &step,
        RuntimeQueueEnqueueOptions {
            harness_home: args.target_home,
            runtime_workspace: None,
            now_ms: args.now_ms,
        },
    )
    .map_err(|err| err.to_string())?;

    print_runtime_queue_enqueue_report(&report);
    Ok(())
}

fn run_queue_prepare(args: &[String]) -> Result<(), String> {
    let args = queue_prepare_args_from_args(args)?;
    let report = prepare_runtime_queue_item(RuntimeQueuePrepareOptions {
        harness_home: args.target_home.clone(),
        queue_id: args.queue_id,
        prompt_options: PromptAssemblyOptions {
            max_prompt_file_bytes: args.max_prompt_file_bytes,
            max_skill_file_bytes: args.max_skill_file_bytes,
            harness_home: Some(args.target_home.clone()),
            ..PromptAssemblyOptions::default()
        },
    })
    .map_err(|err| err.to_string())?;

    print_runtime_queue_prepare_report(&report);
    Ok(())
}

fn run_runtime_run_once(args: &[String]) -> Result<(), String> {
    let args = runtime_run_once_args_from_args(args)?;
    let _typing = start_runtime_typing_heartbeat(&args.target_home, args.queue_id.as_deref());
    let report = run_runtime_queue_once(RuntimeRunOnceOptions {
        harness_home: args.target_home.clone(),
        queue_id: args.queue_id,
        codex_executable: args.codex_exe,
        timeout_ms: args.timeout_ms,
        idle_timeout_ms: args.idle_timeout_ms,
        prompt_options: PromptAssemblyOptions {
            max_prompt_file_bytes: args.max_prompt_file_bytes,
            max_skill_file_bytes: args.max_skill_file_bytes,
            harness_home: Some(args.target_home),
            ..PromptAssemblyOptions::default()
        },
    })
    .map_err(|err| err.to_string())?;

    print_runtime_run_once_report(&report);
    Ok(())
}

fn run_runtime_lease_reconcile(args: &[String]) -> Result<(), String> {
    let options = SimpleOptions::parse(
        args,
        "runtime-lease-reconcile",
        &["--service", "--generation-id"],
        &[],
    )?;
    let service_id = options.required("--service")?;
    let generation_id = options.required("--generation-id")?;
    let report = reconcile_runtime_queue_leases_for_generation(
        options.target_home,
        &service_id,
        &generation_id,
        current_time_ms()?,
    )
    .map_err(|err| err.to_string())?;
    print_json(&report)
}

struct RuntimeLoopTaskResult {
    queue_id: String,
    result: Result<RuntimeRunOnceReport, String>,
}

fn run_runtime_loop(args: &[String]) -> Result<(), String> {
    let args = runtime_loop_args_from_args(args)?;
    let started_at_ms = current_log_time_ms().map_err(|err| err.to_string())?;
    let mut iterations = 0usize;
    let mut completed = 0usize;
    let mut idle = 0usize;
    let mut errors = 0usize;
    let mut consecutive_errors = 0usize;
    let mut safe_mode_restarts = 0usize;
    let mut last_status: Option<RuntimeRunOnceStatus> = None;
    let mut last_queue_id: Option<String> = None;
    let mut last_reason: Option<String> = None;
    let mut failed = false;
    let mut stop_reason: Option<String> = None;
    let mut stop_requested = false;
    let mut active = 0usize;
    let mut runtime_concurrency = args.runtime_concurrency.max(1);
    let mut active_queue_ids = HashSet::new();
    let (task_tx, task_rx) = mpsc::channel::<RuntimeLoopTaskResult>();

    loop {
        while let Ok(task) = task_rx.try_recv() {
            active = active.saturating_sub(1);
            active_queue_ids.remove(&task.queue_id);
            if handle_runtime_loop_task_result(
                &args,
                iterations,
                task,
                &mut completed,
                &mut idle,
                &mut errors,
                &mut consecutive_errors,
                &mut last_status,
                &mut last_queue_id,
                &mut last_reason,
            )? {
                if enter_runtime_loop_safe_mode(
                    &args,
                    iterations,
                    &mut consecutive_errors,
                    &mut safe_mode_restarts,
                    &mut runtime_concurrency,
                )? {
                    stop_requested = false;
                    stop_reason = None;
                } else {
                    failed = true;
                    stop_requested = true;
                    stop_reason = Some(format!(
                        "stopped after {} consecutive runtime errors",
                        args.max_consecutive_errors
                    ));
                }
            }
        }
        if failed && active == 0 {
            break;
        }
        let runtime_wake_sequence = read_loop_wake_sequence(&args.target_home, "runtime");

        if stop_file_requested(args.stop_file.as_deref()) {
            stop_requested = true;
            stop_reason.get_or_insert_with(|| "stopped after stop file request".to_string());
        }
        if stop_requested {
            if active == 0 {
                write_loop_heartbeat(
                    &args.target_home,
                    &args.loop_name,
                    "stopped",
                    iterations,
                    stop_reason.as_deref().unwrap_or("stop requested"),
                )?;
                break;
            }
            write_loop_heartbeat(
                &args.target_home,
                &args.loop_name,
                "stopping",
                iterations,
                &format!("waiting for {active} active runtime task(s)"),
            )?;
        } else {
            iterations += 1;
            write_loop_heartbeat(
                &args.target_home,
                &args.loop_name,
                "running",
                iterations,
                &format!(
                    "checking runtime queue active={active}/{}",
                    runtime_concurrency
                ),
            )?;

            let slots = runtime_concurrency.saturating_sub(active);
            let mut spawned = 0usize;
            if slots > 0 {
                match inspect_runtime_queue_capacity(RuntimeQueueCapacityOptions {
                    harness_home: args.target_home.clone(),
                }) {
                    Ok(capacity) => {
                        if capacity.lease_lock_busy {
                            let reason =
                                "runtime queue lease lock is busy during capacity inspection";
                            consecutive_errors = 0;
                            last_status = Some(RuntimeRunOnceStatus::LeaseBusy);
                            last_queue_id = None;
                            last_reason = Some(reason.to_string());
                            write_loop_heartbeat(
                                &args.target_home,
                                &args.loop_name,
                                "lease-busy",
                                iterations,
                                reason,
                            )?;
                        } else {
                            let queue_ids = capacity
                                .claimable_queue_ids
                                .into_iter()
                                .filter(|queue_id| !active_queue_ids.contains(queue_id))
                                .take(slots)
                                .collect::<Vec<_>>();
                            for queue_id in queue_ids {
                                active_queue_ids.insert(queue_id.clone());
                                spawn_runtime_loop_task(&args, queue_id, &task_tx);
                                active += 1;
                                spawned += 1;
                            }
                            if spawned == 0 && active == 0 {
                                idle += 1;
                                consecutive_errors = 0;
                                last_status = Some(RuntimeRunOnceStatus::NoWork);
                                last_queue_id = None;
                                last_reason = Some(
                                    "no pending or prepared runtime queue item is available"
                                        .to_string(),
                                );
                                write_loop_heartbeat(
                                    &args.target_home,
                                    &args.loop_name,
                                    "no-work",
                                    iterations,
                                    "no pending or prepared runtime queue item is available",
                                )?;
                                if args.stop_when_idle {
                                    stop_reason = Some(
                                        "stopped after idle runtime result no-work".to_string(),
                                    );
                                    break;
                                }
                            }
                        }
                    }
                    Err(error) => {
                        errors += 1;
                        consecutive_errors += 1;
                        let error = error.to_string();
                        write_loop_heartbeat(
                            &args.target_home,
                            &args.loop_name,
                            "error",
                            iterations,
                            &format!("consecutiveErrors={consecutive_errors} error={error}"),
                        )?;
                        eprintln!(
                            "{} capacity inspection failed ({consecutive_errors}/{}): {error}",
                            args.loop_name, args.max_consecutive_errors
                        );
                        last_status = None;
                        last_queue_id = None;
                        last_reason = Some(error.clone());
                        append_runtime_loop_error_log(
                            &args.target_home,
                            iterations,
                            consecutive_errors,
                            args.max_consecutive_errors,
                            &error,
                        )?;
                        if consecutive_errors >= args.max_consecutive_errors {
                            if enter_runtime_loop_safe_mode(
                                &args,
                                iterations,
                                &mut consecutive_errors,
                                &mut safe_mode_restarts,
                                &mut runtime_concurrency,
                            )? {
                                stop_requested = false;
                                stop_reason = None;
                            } else {
                                failed = true;
                                stop_requested = true;
                                stop_reason = Some(format!(
                                    "stopped after {} consecutive runtime errors",
                                    args.max_consecutive_errors
                                ));
                            }
                        }
                    }
                }
            }

            if args.iterations > 0 && iterations >= args.iterations {
                stop_reason = Some(format!(
                    "stopped after configured iteration limit {}",
                    args.iterations
                ));
                if active == 0 {
                    break;
                }
                stop_requested = true;
            }
        }

        if active > 0 {
            match task_rx.recv_timeout(Duration::from_millis(args.idle_ms)) {
                Ok(task) => {
                    active = active.saturating_sub(1);
                    active_queue_ids.remove(&task.queue_id);
                    if handle_runtime_loop_task_result(
                        &args,
                        iterations,
                        task,
                        &mut completed,
                        &mut idle,
                        &mut errors,
                        &mut consecutive_errors,
                        &mut last_status,
                        &mut last_queue_id,
                        &mut last_reason,
                    )? {
                        if enter_runtime_loop_safe_mode(
                            &args,
                            iterations,
                            &mut consecutive_errors,
                            &mut safe_mode_restarts,
                            &mut runtime_concurrency,
                        )? {
                            stop_requested = false;
                            stop_reason = None;
                        } else {
                            failed = true;
                            stop_requested = true;
                            stop_reason = Some(format!(
                                "stopped after {} consecutive runtime errors",
                                args.max_consecutive_errors
                            ));
                        }
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    failed = true;
                    stop_reason = Some(
                        "stopped after runtime worker result channel disconnected".to_string(),
                    );
                    break;
                }
            }
        } else {
            wait_for_loop_wake_since(
                &args.target_home,
                "runtime",
                runtime_wake_sequence,
                args.idle_ms,
            );
        }
        if failed && active == 0 {
            break;
        }
    }

    let summary = RuntimeLoopSummary {
        target_home: args.target_home.clone(),
        started_at_ms,
        finished_at_ms: current_log_time_ms().map_err(|err| err.to_string())?,
        iterations,
        completed,
        idle,
        errors,
        consecutive_errors,
        safe_mode_restarts,
        stop_reason: stop_reason.unwrap_or_else(|| "runtime loop stopped".to_string()),
        last_status,
        last_queue_id,
        last_reason,
        runtime_concurrency,
        report_file: args
            .target_home
            .join("state")
            .join("runtime-queue")
            .join("loop-last.json"),
    };
    write_runtime_loop_summary(&summary)?;
    append_runtime_loop_stopped_log(&summary, failed)?;
    print_runtime_loop_summary(&summary);

    if failed {
        return Err(summary.stop_reason);
    }
    Ok(())
}

fn enter_runtime_loop_safe_mode(
    args: &RuntimeLoopArgs,
    iteration: usize,
    consecutive_errors: &mut usize,
    safe_mode_restarts: &mut usize,
    runtime_concurrency: &mut usize,
) -> Result<bool, String> {
    let Some(restart_ms) = args.safe_mode_restart_ms else {
        return Ok(false);
    };
    *safe_mode_restarts += 1;
    *runtime_concurrency = 1;
    let detail = format!(
        "safeModeRestart={} after consecutiveErrors={}/{}; runtimeConcurrency=1; retrying in {} ms",
        *safe_mode_restarts, *consecutive_errors, args.max_consecutive_errors, restart_ms
    );
    write_loop_heartbeat(
        &args.target_home,
        &args.loop_name,
        "safe-mode",
        iteration,
        &detail,
    )?;
    append_runtime_loop_safe_mode_log(
        &args.target_home,
        iteration,
        *safe_mode_restarts,
        *consecutive_errors,
        args.max_consecutive_errors,
        *runtime_concurrency,
        restart_ms,
    )?;
    thread::sleep(Duration::from_millis(restart_ms));
    *consecutive_errors = 0;
    Ok(true)
}

fn spawn_runtime_loop_task(
    args: &RuntimeLoopArgs,
    queue_id: String,
    task_tx: &mpsc::Sender<RuntimeLoopTaskResult>,
) {
    let task_args = args.clone();
    let sender = task_tx.clone();
    thread::spawn(move || {
        let _typing =
            start_runtime_typing_heartbeat(&task_args.target_home, Some(queue_id.as_str()));
        let result = run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: task_args.target_home.clone(),
            queue_id: Some(queue_id.clone()),
            codex_executable: task_args.codex_exe,
            timeout_ms: task_args.timeout_ms,
            idle_timeout_ms: task_args.idle_timeout_ms,
            prompt_options: PromptAssemblyOptions {
                max_prompt_file_bytes: task_args.max_prompt_file_bytes,
                max_skill_file_bytes: task_args.max_skill_file_bytes,
                harness_home: Some(task_args.target_home),
                ..PromptAssemblyOptions::default()
            },
        })
        .map_err(|err| err.to_string());
        let _ = sender.send(RuntimeLoopTaskResult { queue_id, result });
    });
}

#[allow(clippy::too_many_arguments)]
fn handle_runtime_loop_task_result(
    args: &RuntimeLoopArgs,
    iteration: usize,
    task: RuntimeLoopTaskResult,
    completed: &mut usize,
    idle: &mut usize,
    errors: &mut usize,
    consecutive_errors: &mut usize,
    last_status: &mut Option<RuntimeRunOnceStatus>,
    last_queue_id: &mut Option<String>,
    last_reason: &mut Option<String>,
) -> Result<bool, String> {
    match task.result {
        Ok(report) => {
            let status = report.receipt.status;
            *last_status = Some(status);
            *last_queue_id = report.receipt.queue_id.clone();
            *last_reason = Some(report.receipt.reason.clone());
            write_loop_heartbeat(
                &args.target_home,
                &args.loop_name,
                runtime_run_once_status_label(status),
                iteration,
                &format!("{} queue={}", report.receipt.reason, task.queue_id),
            )?;
            println!(
                "Runtime loop iteration {iteration}: {} queue={} ({})",
                runtime_run_once_status_label(status),
                task.queue_id,
                report.receipt.reason
            );

            if status == RuntimeRunOnceStatus::LeaseBusy {
                *consecutive_errors = 0;
            } else if runtime_run_once_report_is_idle(&report) {
                *idle += 1;
                *consecutive_errors = 0;
            } else if matches!(
                status,
                RuntimeRunOnceStatus::Completed
                    | RuntimeRunOnceStatus::Timeout
                    | RuntimeRunOnceStatus::FailedTerminal
                    | RuntimeRunOnceStatus::Canceled
            ) {
                if status == RuntimeRunOnceStatus::Completed {
                    *completed += 1;
                } else {
                    *errors += 1;
                }
                *consecutive_errors = 0;
            } else {
                *errors += 1;
                *consecutive_errors += 1;
                append_runtime_loop_error_log(
                    &args.target_home,
                    iteration,
                    *consecutive_errors,
                    args.max_consecutive_errors,
                    &format!(
                        "{}: {}",
                        runtime_run_once_status_label(status),
                        report.receipt.reason
                    ),
                )?;
            }
        }
        Err(error) => {
            *errors += 1;
            *consecutive_errors += 1;
            write_loop_heartbeat(
                &args.target_home,
                &args.loop_name,
                "error",
                iteration,
                &format!(
                    "queue={} consecutiveErrors={}/{} error={error}",
                    task.queue_id, *consecutive_errors, args.max_consecutive_errors
                ),
            )?;
            eprintln!(
                "{} runtime task queue={} failed ({}/{}): {error}",
                args.loop_name, task.queue_id, *consecutive_errors, args.max_consecutive_errors
            );
            *last_status = None;
            *last_queue_id = Some(task.queue_id);
            *last_reason = Some(error.clone());
            append_runtime_loop_error_log(
                &args.target_home,
                iteration,
                *consecutive_errors,
                args.max_consecutive_errors,
                &error,
            )?;
        }
    }
    Ok(*consecutive_errors >= args.max_consecutive_errors)
}

fn run_worker_enqueue(args: &[String]) -> Result<(), String> {
    let args = worker_enqueue_args_from_args(args)?;
    let report = enqueue_worker_job(WorkerEnqueueOptions {
        harness_home: args.target_home,
        kind: args.kind,
        lane: args.lane,
        payload: args.payload,
        idempotency_key: args.idempotency_key,
        parent_job_id: args.parent_job_id,
        job_group_id: args.job_group_id,
        master_agent_id: args.master_agent_id,
        master_session_key: args.master_session_key,
        wake_policy: args.wake_policy,
        source: args.source,
        priority: args.priority,
        available_at_ms: args.available_at_ms,
        max_attempts: args.max_attempts,
        timeout_ms: args.timeout_ms,
        cascade_timeout_ms: args.cascade_timeout_ms,
        rate_key: args.rate_key,
        concurrency_group_key: args.concurrency_group_key,
        now_ms: current_time_ms()?,
    })
    .map_err(|err| err.to_string())?;
    print_json(&report)
}

fn run_worker_run_once(args: &[String]) -> Result<(), String> {
    let args = worker_run_once_args_from_args(args)?;
    let report = run_worker_once(WorkerRunOnceOptions {
        harness_home: args.target_home,
        lane: args.lane,
        worker_id: args.worker_id,
        lease_ms: args.lease_ms,
        now_ms: current_time_ms()?,
    })
    .map_err(|err| err.to_string())?;
    print_json(&report)
}

fn run_worker_loop(args: &[String]) -> Result<(), String> {
    let args = worker_loop_args_from_args(args)?;
    let mut iterations = 0usize;
    let mut completed = 0usize;
    let mut idle = 0usize;
    let mut errors = 0usize;
    let mut consecutive_errors = 0usize;
    let stop_reason;

    loop {
        let wake_lane = worker_loop_wake_lane(args.lane.as_deref());
        let worker_wake_sequence = read_loop_wake_sequence(&args.target_home, &wake_lane);
        if stop_file_requested(args.stop_file.as_deref()) {
            stop_reason = "stopped after stop file request".to_string();
            write_loop_heartbeat(
                &args.target_home,
                "worker-loop",
                "stopped",
                iterations,
                "stop file requested",
            )?;
            break;
        }
        iterations += 1;
        write_loop_heartbeat(
            &args.target_home,
            "worker-loop",
            "running",
            iterations,
            "checking worker queue",
        )?;
        match run_worker_once(WorkerRunOnceOptions {
            harness_home: args.target_home.clone(),
            lane: args.lane.clone(),
            worker_id: args.worker_id.clone(),
            lease_ms: args.lease_ms,
            now_ms: current_time_ms()?,
        }) {
            Ok(report) => {
                let label = worker_run_once_status_label(report.status);
                write_loop_heartbeat(
                    &args.target_home,
                    "worker-loop",
                    label,
                    iterations,
                    &report.reason,
                )?;
                println!(
                    "Worker loop iteration {iterations}: {label} ({})",
                    report.reason
                );
                if report.status == WorkerRunOnceStatus::NoWork {
                    idle += 1;
                    consecutive_errors = 0;
                    if args.stop_when_idle {
                        stop_reason = "stopped after idle worker result".to_string();
                        break;
                    }
                } else if report.status == WorkerRunOnceStatus::Completed
                    || report.status == WorkerRunOnceStatus::Rescheduled
                {
                    completed += 1;
                    consecutive_errors = 0;
                } else {
                    errors += 1;
                    consecutive_errors += 1;
                    if consecutive_errors >= args.max_consecutive_errors {
                        stop_reason = format!(
                            "stopped after {} consecutive worker errors",
                            args.max_consecutive_errors
                        );
                        break;
                    }
                }
            }
            Err(error) => {
                errors += 1;
                consecutive_errors += 1;
                let error = error.to_string();
                write_loop_heartbeat(
                    &args.target_home,
                    "worker-loop",
                    "error",
                    iterations,
                    &format!("consecutiveErrors={consecutive_errors} error={error}"),
                )?;
                eprintln!(
                    "worker-loop iteration {iterations} failed ({consecutive_errors}/{}): {error}",
                    args.max_consecutive_errors
                );
                if consecutive_errors >= args.max_consecutive_errors {
                    stop_reason = format!(
                        "stopped after {} consecutive worker errors",
                        args.max_consecutive_errors
                    );
                    break;
                }
            }
        }

        if args.iterations > 0 && iterations >= args.iterations {
            stop_reason = format!(
                "stopped after configured iteration limit {}",
                args.iterations
            );
            break;
        }
        wait_for_loop_wake_since(
            &args.target_home,
            &wake_lane,
            worker_wake_sequence,
            args.idle_ms,
        );
    }

    let summary = serde_json::json!({
        "schema": "agent-harness.worker-loop-summary.v1",
        "harnessHome": args.target_home,
        "iterations": iterations,
        "completed": completed,
        "idle": idle,
        "errors": errors,
        "consecutiveErrors": consecutive_errors,
        "stopReason": stop_reason
    });
    print_json(&summary)
}

fn run_worker_status(args: &[String]) -> Result<(), String> {
    let args = worker_status_args_from_args(args)?;
    let report = collect_worker_status(WorkerStatusOptions {
        harness_home: args.target_home,
    })
    .map_err(|err| err.to_string())?;
    print_json(&report)
}

fn run_worker_cancel(args: &[String]) -> Result<(), String> {
    let args = worker_cancel_args_from_args(args)?;
    let report = cancel_worker_job(WorkerCancelOptions {
        harness_home: args.target_home,
        job_id: args.job_id,
        now_ms: current_time_ms()?,
        reason: args.reason,
    })
    .map_err(|err| err.to_string())?;
    print_json(&report)
}

fn run_worker_reap_stale(args: &[String]) -> Result<(), String> {
    let args = worker_status_args_from_args(args)?;
    let report = reap_stale_worker_jobs(WorkerReapStaleOptions {
        harness_home: args.target_home,
        now_ms: current_time_ms()?,
    })
    .map_err(|err| err.to_string())?;
    print_json(&report)
}

fn run_codex_plan(args: &[String]) -> Result<(), String> {
    let args = codex_plan_args_from_args(args)?;
    let report = plan_codex_runtime(CodexRuntimePlanOptions {
        harness_home: args.target_home,
        execution_dir: args.execution_dir,
        codex_executable: args.codex_exe,
    })
    .map_err(|err| err.to_string())?;

    print_codex_runtime_plan_report(&report);
    Ok(())
}

fn run_codex_preflight(args: &[String]) -> Result<(), String> {
    let args = codex_preflight_args_from_args(args)?;
    let report = preflight_codex_runtime(CodexRuntimePreflightOptions {
        harness_home: args.target_home,
        execution_dir: args.execution_dir,
        plan_file: args.plan_file,
    })
    .map_err(|err| err.to_string())?;

    print_codex_runtime_preflight_report(&report);
    Ok(())
}

fn run_codex_launch_probe(args: &[String]) -> Result<(), String> {
    let args = codex_launch_probe_args_from_args(args)?;
    let report = probe_codex_runtime_launch(CodexRuntimeLaunchProbeOptions {
        harness_home: args.target_home,
        execution_dir: args.execution_dir,
        plan_file: args.plan_file,
        startup_probe_ms: args.startup_probe_ms,
    })
    .map_err(|err| err.to_string())?;

    print_codex_runtime_launch_probe_report(&report);
    Ok(())
}

fn run_codex_run(args: &[String]) -> Result<(), String> {
    let args = codex_run_args_from_args(args)?;
    let report = run_codex_runtime(CodexRuntimeRunOptions {
        harness_home: args.target_home,
        execution_dir: args.execution_dir,
        plan_file: args.plan_file,
        timeout_ms: args.timeout_ms,
        idle_timeout_ms: args.idle_timeout_ms,
        progress_context: None,
    })
    .map_err(|err| err.to_string())?;

    print_codex_runtime_run_report(&report);
    Ok(())
}

fn run_codex_complete(args: &[String]) -> Result<(), String> {
    let args = codex_complete_args_from_args(args)?;
    let report = record_codex_runtime_completion(CodexRuntimeCompletionOptions {
        harness_home: args.target_home,
        execution_dir: args.execution_dir,
        plan_file: args.plan_file,
        assistant_message: args.assistant_message,
        assistant_narration: Vec::new(),
        assistant_narration_mode: AssistantNarrationMode::ProgressPanel,
        thread_id: None,
        finished_at_ms: args.finished_at_ms,
    })
    .map_err(|err| err.to_string())?;

    print_codex_runtime_completion_report(&report);
    Ok(())
}

fn run_prompt_bundle(args: &[String]) -> Result<(), String> {
    let args = turn_plan_args_from_args(args)?;
    let registry = load_agent_registry(&args.source).map_err(|err| err.to_string())?;
    let skill_index = match &args.harness_home {
        Some(harness_home) => build_runtime_skill_index(&args.source, harness_home),
        None => build_source_skill_index(&args.source),
    }
    .map_err(|err| err.to_string())?;
    let plan = build_turn_plan(
        &args.source,
        &registry,
        &skill_index,
        TurnPlanInput {
            harness_home: args.harness_home.clone(),
            platform: args.platform,
            channel_id: args.channel_id,
            user_id: args.user_id,
            text: args.message,
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            requested_agent_id: args.agent_id,
            session_hint: args.session_key,
            skill_limit: args.skill_limit,
        },
    )
    .map_err(|err| err.to_string())?;
    let bundle = assemble_prompt_bundle(
        &plan,
        PromptAssemblyOptions {
            max_prompt_file_bytes: args.max_prompt_file_bytes,
            max_skill_file_bytes: args.max_skill_file_bytes,
            harness_home: args.harness_home.clone(),
            ..PromptAssemblyOptions::default()
        },
    )
    .map_err(|err| err.to_string())?;

    print_prompt_bundle(&bundle);

    if let Some(output_dir) = args.output_dir {
        let files = write_prompt_bundle(&bundle, output_dir).map_err(|err| err.to_string())?;
        println!("Prompt bundle JSON: {}", files.json.display());
        println!("Prompt markdown: {}", files.markdown.display());
    }

    Ok(())
}

fn run_cron_plan(args: &[String]) -> Result<(), String> {
    let args = cron_plan_args_from_args(args)?;
    let store = load_native_cron_store(&args.source).map_err(|err| err.to_string())?;
    let registry = load_agent_registry(&args.source).map_err(|err| err.to_string())?;
    let plan = plan_native_cron(
        &store,
        &registry,
        NativeCronPlanInput {
            now_ms: args.now_ms,
            resume_enabled: args.resume_cron,
        },
    );

    print_cron_plan(&plan, args.limit);

    if let Some(output_dir) = args.output_dir {
        let file = write_native_cron_plan(&plan, output_dir).map_err(|err| err.to_string())?;
        println!("Native cron plan JSON: {}", file.json.display());
    }

    Ok(())
}

fn run_native_cron_enqueue(args: &[String]) -> Result<(), String> {
    let args = worker_adapter_enqueue_args_from_args(args)?;
    let report = enqueue_native_cron_workers(NativeCronWorkerEnqueueOptions {
        harness_home: args.target_home,
        source: args.source,
        resume_cron: args.resume_cron,
        include_registered_cron: args.include_registered_cron,
        master_agent_id: args.master_agent_id,
        master_session_key: args.master_session_key,
        runtime_workspace: args.runtime_workspace,
        now_ms: args.now_ms,
    })
    .map_err(|err| err.to_string())?;
    print_json(&report)
}

fn run_deterministic_cron_plan(args: &[String]) -> Result<(), String> {
    let args = deterministic_cron_plan_args_from_args(args)?;
    let store = load_deterministic_cron_store(&args.source).map_err(|err| err.to_string())?;
    let plan = plan_deterministic_cron(
        &store,
        DeterministicCronPlanInput {
            allow_deterministic_run: args.allow_deterministic_run,
        },
    );

    print_deterministic_cron_plan(&plan, args.limit);

    if let Some(output_dir) = args.output_dir {
        let file =
            write_deterministic_cron_plan(&plan, output_dir).map_err(|err| err.to_string())?;
        println!("Deterministic cron plan JSON: {}", file.json.display());
    }

    Ok(())
}

fn run_deterministic_cron_enqueue(args: &[String]) -> Result<(), String> {
    let args = worker_adapter_enqueue_args_from_args(args)?;
    let report = enqueue_deterministic_cron_workers(DeterministicCronWorkerEnqueueOptions {
        harness_home: args.target_home,
        source: args.source,
        allow_deterministic_run: args.allow_deterministic_run,
        dry_run_shell: args.dry_run_shell,
        master_agent_id: args.master_agent_id,
        master_session_key: args.master_session_key,
        runtime_workspace: args.runtime_workspace,
        now_ms: args.now_ms,
    })
    .map_err(|err| err.to_string())?;
    print_json(&report)
}

fn run_cron_scheduler_lint(args: &[String]) -> Result<(), String> {
    let args = cron_scheduler_run_once_args_from_args(args)?;
    let report = lint_cron_scheduler(CronSchedulerRunOnceOptions {
        harness_home: args.target_home,
        source: args.source,
        runtime_workspace: args.runtime_workspace,
        now_ms: current_log_time_ms().map_err(|err| err.to_string())?,
        dry_run: true,
        enabled_override: args.enabled_override,
        native_enabled_override: args.native_enabled_override,
        deterministic_enabled_override: args.deterministic_enabled_override,
        resume_cron_override: args.resume_cron_override,
        include_registered_cron_override: args.include_registered_cron_override,
        allow_deterministic_run_override: args.allow_deterministic_run_override,
        execute_shell_override: args.execute_shell_override,
        max_catchup_per_tick_override: args.max_catchup_per_tick_override,
        max_enqueue_per_tick_override: args.max_enqueue_per_tick_override,
    })
    .map_err(|err| err.to_string())?;
    let status = report.status;
    print_json(&report)?;
    if status == CronSchedulerLintStatus::Error {
        Err("cron scheduler lint failed".to_string())
    } else {
        Ok(())
    }
}

fn run_cron_scheduler_run_once(args: &[String]) -> Result<(), String> {
    let args = cron_scheduler_run_once_args_from_args(args)?;
    let report = run_cron_scheduler_once(CronSchedulerRunOnceOptions {
        harness_home: args.target_home,
        source: args.source,
        runtime_workspace: args.runtime_workspace,
        now_ms: current_log_time_ms().map_err(|err| err.to_string())?,
        dry_run: args.dry_run,
        enabled_override: args.enabled_override,
        native_enabled_override: args.native_enabled_override,
        deterministic_enabled_override: args.deterministic_enabled_override,
        resume_cron_override: args.resume_cron_override,
        include_registered_cron_override: args.include_registered_cron_override,
        allow_deterministic_run_override: args.allow_deterministic_run_override,
        execute_shell_override: args.execute_shell_override,
        max_catchup_per_tick_override: args.max_catchup_per_tick_override,
        max_enqueue_per_tick_override: args.max_enqueue_per_tick_override,
    })
    .map_err(|err| err.to_string())?;
    print_json(&report)
}

fn run_cron_scheduler_loop(args: &[String]) -> Result<(), String> {
    let args = cron_scheduler_loop_args_from_args(args)?;
    let mut iterations = 0usize;
    let mut consecutive_errors = 0usize;
    loop {
        if stop_file_requested(args.stop_file.as_deref()) {
            append_loop_stop_log(
                &args.run_once.target_home,
                "cron-scheduler",
                "cron-scheduler.loop-stopped",
                iterations,
                "stop file requested",
            )?;
            write_loop_heartbeat(
                &args.run_once.target_home,
                "cron-scheduler-loop",
                "stopped",
                iterations,
                "stop file requested",
            )?;
            break;
        }
        if args.iterations != 0 && iterations >= args.iterations {
            write_loop_heartbeat(
                &args.run_once.target_home,
                "cron-scheduler-loop",
                "stopped",
                iterations,
                "iteration limit reached",
            )?;
            break;
        }
        iterations += 1;
        let report = run_cron_scheduler_once(CronSchedulerRunOnceOptions {
            harness_home: args.run_once.target_home.clone(),
            source: args.run_once.source.clone(),
            runtime_workspace: args.run_once.runtime_workspace.clone(),
            now_ms: current_log_time_ms().map_err(|err| err.to_string())?,
            dry_run: args.run_once.dry_run,
            enabled_override: args.run_once.enabled_override,
            native_enabled_override: args.run_once.native_enabled_override,
            deterministic_enabled_override: args.run_once.deterministic_enabled_override,
            resume_cron_override: args.run_once.resume_cron_override,
            include_registered_cron_override: args.run_once.include_registered_cron_override,
            allow_deterministic_run_override: args.run_once.allow_deterministic_run_override,
            execute_shell_override: args.run_once.execute_shell_override,
            max_catchup_per_tick_override: args.run_once.max_catchup_per_tick_override,
            max_enqueue_per_tick_override: args.run_once.max_enqueue_per_tick_override,
        });
        let next_sleep_ms = match report {
            Ok(report) => {
                let next_sleep_ms =
                    cron_scheduler_loop_sleep_ms(args.idle_ms, report.config.interval_ms);
                let detail = format!(
                    "status={:?} intervalMs={} sleepMs={} enqueued={} duplicate={} errors={}",
                    report.status,
                    report.config.interval_ms,
                    next_sleep_ms,
                    report.summary.enqueued,
                    report.summary.skipped_duplicate,
                    report.summary.errors
                );
                println!("{detail}");
                consecutive_errors = if report.status == CronSchedulerTickStatus::Error {
                    consecutive_errors.saturating_add(1)
                } else {
                    0
                };
                write_loop_heartbeat(
                    &args.run_once.target_home,
                    "cron-scheduler-loop",
                    if report.status == CronSchedulerTickStatus::Error {
                        "error"
                    } else {
                        "running"
                    },
                    iterations,
                    &detail,
                )?;
                next_sleep_ms
            }
            Err(error) => {
                let detail = error.to_string();
                consecutive_errors = consecutive_errors.saturating_add(1);
                append_harness_log(
                    &args.run_once.target_home,
                    &HarnessLogEvent::new(
                        current_log_time_ms().unwrap_or(0),
                        HarnessLogLevel::Warn,
                        "cron-scheduler",
                        "cron-scheduler.loop-error",
                        format!("iteration {iterations} failed: {detail}"),
                    ),
                )
                .map_err(|err| err.to_string())?;
                write_loop_heartbeat(
                    &args.run_once.target_home,
                    "cron-scheduler-loop",
                    "error",
                    iterations,
                    &detail,
                )?;
                cron_scheduler_loop_sleep_ms(args.idle_ms, 60_000)
            }
        };
        if consecutive_errors >= args.max_consecutive_errors {
            return Err(format!(
                "cron scheduler loop stopped after {consecutive_errors} consecutive errors"
            ));
        }
        thread::sleep(Duration::from_millis(next_sleep_ms));
    }
    Ok(())
}

fn cron_scheduler_loop_sleep_ms(idle_ms: Option<u64>, config_interval_ms: i64) -> u64 {
    let interval_ms = u64::try_from(config_interval_ms)
        .unwrap_or(60_000)
        .clamp(10_000, 3_600_000);
    idle_ms.unwrap_or(0).clamp(0, 3_600_000).max(interval_ms)
}

fn run_context_rollover(args: &[String]) -> Result<(), String> {
    let args = context_rollover_args_from_args(args)?;
    match args.action.as_str() {
        "requeue-prepared" => {
            let queue_id = args.queue_id.ok_or_else(|| {
                "context-rollover --action requeue-prepared requires --queue-id".to_string()
            })?;
            let new_working_session_key = args.new_working_session_key.ok_or_else(|| {
                "context-rollover --action requeue-prepared requires --new-working-session-key"
                    .to_string()
            })?;
            let report = requeue_prepared_context_rollover(ContextRolloverRequeuePreparedOptions {
                harness_home: args.target_home,
                queue_id,
                new_working_session_key,
                reason: args.reason.unwrap_or_else(|| {
                    "operator requested context rollover prepared requeue".to_string()
                }),
                now_ms: args.now_ms,
            })
            .map_err(|err| err.to_string())?;
            print_json(&report)
        }
        other => Err(format!(
            "context-rollover: unknown --action {other}; expected requeue-prepared"
        )),
    }
}

fn run_subagent_plan(args: &[String]) -> Result<(), String> {
    let args = subagent_plan_args_from_args(args)?;
    let ledger = load_subagent_ledger(&args.source).map_err(|err| err.to_string())?;
    let plan = plan_subagents(
        &ledger,
        SubagentPlanInput {
            resume_subagents: args.resume_subagents,
        },
    );

    print_subagent_plan(&plan, args.limit);

    if let Some(output_dir) = args.output_dir {
        let file = write_subagent_plan(&plan, output_dir).map_err(|err| err.to_string())?;
        println!("Subagent plan JSON: {}", file.json.display());
    }

    Ok(())
}

fn run_subagent_enqueue(args: &[String]) -> Result<(), String> {
    let args = worker_adapter_enqueue_args_from_args(args)?;
    let report = enqueue_subagent_workers(SubagentWorkerEnqueueOptions {
        harness_home: args.target_home,
        source: args.source,
        resume_subagents: args.resume_subagents,
        master_agent_id: args.master_agent_id,
        master_session_key: args.master_session_key,
        runtime_workspace: args.runtime_workspace,
        now_ms: args.now_ms,
    })
    .map_err(|err| err.to_string())?;
    print_json(&report)
}

fn run_subagent_lifecycle(args: &[String]) -> Result<(), String> {
    let args = subagent_lifecycle_args_from_args(args)?;
    match args.action.as_str() {
        "show" => {
            let subagent_id = args.subagent_id.ok_or_else(|| {
                "subagent-lifecycle --action show requires --subagent-id".to_string()
            })?;
            let report = show_subagent_lifecycle(SubagentLifecycleShowOptions {
                harness_home: args.target_home,
                subagent_id,
                now_ms: args.now_ms,
            })
            .map_err(|err| err.to_string())?;
            print_json(&report)
        }
        "close" => {
            let subagent_id = args.subagent_id.ok_or_else(|| {
                "subagent-lifecycle --action close requires --subagent-id".to_string()
            })?;
            let report =
                agent_harness_core::close_subagent_lifecycle(SubagentLifecycleCloseOptions {
                    harness_home: args.target_home,
                    subagent_id,
                    reason: args.reason.unwrap_or_else(|| {
                        "operator requested subagent lifecycle close".to_string()
                    }),
                    now_ms: args.now_ms,
                })
                .map_err(|err| err.to_string())?;
            print_json(&report)
        }
        "smoke" => {
            let report = run_subagent_lifecycle_smoke(&args)?;
            print_json(&report)?;
            if !report.workspace_clean {
                return Err(format!(
                    "subagent-lifecycle smoke observed workspace changes: {} entries",
                    report.workspace_diff.len()
                ));
            }
            Ok(())
        }
        other => Err(format!(
            "subagent-lifecycle: unknown --action {other}; expected smoke, show, or close"
        )),
    }
}

fn run_subagent_lifecycle_smoke(
    args: &SubagentLifecycleArgs,
) -> Result<SubagentLifecycleSmokeReport, String> {
    if !args.no_write {
        return Err("subagent-lifecycle --action smoke requires --no-write".to_string());
    }
    let workspace = absolute_path(&args.workspace)?;
    let target_home = absolute_path(&args.target_home)?;
    let source_home = absolute_path(&args.source_home)?;
    if path_identity_key(&workspace) == path_identity_key(&target_home) {
        return Err(
            "subagent-lifecycle smoke requires --target-home to be outside or below --workspace, not equal to it"
                .to_string(),
        );
    }
    let subagent_id = args
        .subagent_id
        .clone()
        .unwrap_or_else(|| format!("subagent:smoke-{}", args.now_ms));
    let run_id = subagent_id
        .strip_prefix("subagent:")
        .unwrap_or(&subagent_id)
        .to_string();
    let prompt = subagent_lifecycle_no_write_prompt();
    let idempotency_key = format!("subagent-lifecycle-smoke:{subagent_id}");

    let workspace_before = workspace_status_snapshot(&workspace, &target_home)?;
    let harness_before = filesystem_manifest_snapshot(&target_home, &[])?;

    let enqueue = enqueue_worker_job(WorkerEnqueueOptions {
        harness_home: target_home.clone(),
        kind: WorkerJobKind::LlmSubagent,
        lane: Some("llm".to_string()),
        payload: serde_json::json!({
            "runId": run_id,
            "subagentId": subagent_id.clone(),
            "sourceHome": source_home.clone(),
            "sourceWorkspace": workspace.clone(),
            "agentId": "subagent-lifecycle-smoke",
            "sessionKey": format!("{}:smoke", subagent_id),
            "messageText": prompt,
            "platform": "subagent-lifecycle-smoke",
            "channelId": "subagent-lifecycle-smoke",
            "userId": "operator",
            "model": args.model,
            "workspaceWritePolicy": "no-write",
            "timeoutMs": args.timeout_ms
        }),
        idempotency_key: Some(idempotency_key.clone()),
        parent_job_id: None,
        job_group_id: None,
        master_agent_id: Some("operator".to_string()),
        master_session_key: Some(format!("{}:smoke", subagent_id)),
        wake_policy: None,
        source: Some("subagent-lifecycle-smoke".to_string()),
        priority: 0,
        available_at_ms: Some(args.now_ms),
        max_attempts: 1,
        timeout_ms: Some(args.timeout_ms),
        cascade_timeout_ms: None,
        rate_key: None,
        concurrency_group_key: None,
        now_ms: args.now_ms,
    })
    .map_err(|err| err.to_string())?;

    let run = run_worker_once(WorkerRunOnceOptions {
        harness_home: target_home.clone(),
        lane: Some("llm".to_string()),
        worker_id: "subagent-lifecycle-smoke".to_string(),
        lease_ms: i64::try_from(args.timeout_ms).unwrap_or(i64::MAX).max(1),
        now_ms: args.now_ms.saturating_add(1),
    })
    .map_err(|err| err.to_string())?;

    let initial_lifecycle = show_subagent_lifecycle(SubagentLifecycleShowOptions {
        harness_home: target_home.clone(),
        subagent_id: subagent_id.clone(),
        now_ms: args.now_ms.saturating_add(2),
    })
    .map_err(|err| err.to_string())?;
    let runtime_terminal_receipt_file = record_subagent_lifecycle_smoke_terminal_receipt(
        &target_home,
        &initial_lifecycle,
        args.now_ms.saturating_add(3),
    )?;
    let completed_lifecycle = show_subagent_lifecycle(SubagentLifecycleShowOptions {
        harness_home: target_home.clone(),
        subagent_id: subagent_id.clone(),
        now_ms: args.now_ms.saturating_add(4),
    })
    .map_err(|err| err.to_string())?;
    let close_report =
        agent_harness_core::close_subagent_lifecycle(SubagentLifecycleCloseOptions {
            harness_home: target_home.clone(),
            subagent_id: subagent_id.clone(),
            reason: "subagent lifecycle no-write smoke close after completed wait".to_string(),
            now_ms: args.now_ms.saturating_add(5),
        })
        .map_err(|err| err.to_string())?;
    let workspace_after = workspace_status_snapshot(&workspace, &target_home)?;
    let harness_after = filesystem_manifest_snapshot(&target_home, &[])?;
    let workspace_diff = snapshot_diff(&workspace_before.entries, &workspace_after.entries);
    let harness_state_writes = snapshot_diff(&harness_before.entries, &harness_after.entries);
    let snapshot_id = subagent_id.clone();

    Ok(SubagentLifecycleSmokeReport {
        schema: "agent-harness.subagent-lifecycle-smoke.v1",
        harness_home: target_home.clone(),
        source_home,
        workspace,
        workspace_write_policy: "no-write".to_string(),
        model: args.model.clone(),
        timeout_ms: args.timeout_ms,
        subagent_id,
        idempotency_key,
        enqueue,
        run,
        runtime_execution_mode:
            "deterministic-terminal-receipt-no-model-execution".to_string(),
        runtime_terminal_receipt_file: Some(runtime_terminal_receipt_file.clone()),
        completed_lifecycle,
        close_report: close_report.clone(),
        lifecycle: close_report,
        workspace_status_method: workspace_after.method,
        workspace_clean: workspace_diff.is_empty(),
        workspace_diff,
        harness_state_writes,
        expected_harness_state_files: vec![
            agent_harness_core::worker_db_file(&target_home),
            subagent_lifecycle_receipts_file(&target_home),
            subagent_lifecycle_snapshot_file(&target_home, &snapshot_id),
            target_home
                .join("state")
                .join("runtime-queue")
                .join("pending.jsonl"),
            runtime_terminal_receipt_file,
            target_home.join("state").join("wake").join("worker.json"),
            target_home
                .join("state")
                .join("wake")
                .join("worker-llm.json"),
        ],
        prompt,
        reason: "no-write smoke enqueued a deterministic llm_subagent runtime turn, recorded a completed terminal receipt, and closed the lifecycle idempotently without invoking a model".to_string(),
    })
}

fn record_subagent_lifecycle_smoke_terminal_receipt(
    harness_home: &Path,
    lifecycle: &SubagentLifecycleShowReport,
    now_ms: i64,
) -> Result<PathBuf, String> {
    let receipt = &lifecycle.receipt;
    let queue_id = receipt.runtime_queue_id.clone().ok_or_else(|| {
        format!(
            "subagent-lifecycle smoke missing runtime queue id for {}",
            receipt.subagent_id
        )
    })?;
    let receipt_file = runtime_run_once_receipts_file(harness_home);
    append_jsonl_value(
        &receipt_file,
        &serde_json::json!({
            "schema": "agent-harness.runtime-run-once.v1",
            "queueId": queue_id,
            "status": "skipped",
            "runtimeClass": "worker",
            "origin": "subagent-lifecycle-smoke",
            "executionDir": null,
            "transcriptFile": null,
            "outboxFile": null,
            "reason": "subagent lifecycle no-write smoke recorded deterministic terminal receipt without model execution"
        }),
    )
    .map_err(|err| err.to_string())?;
    record_subagent_lifecycle(SubagentLifecycleRecordOptions {
        harness_home: harness_home.to_path_buf(),
        subagent_id: receipt.subagent_id.clone(),
        state: SubagentLifecycleState::Completed,
        source: Some(receipt.source.clone()),
        operation_plan_id: receipt.operation_plan_id.clone(),
        operation_plan_item_id: receipt.operation_plan_item_id.clone(),
        worker_job_id: receipt.worker_job_id.clone(),
        runtime_queue_id: Some(queue_id),
        requested_model: receipt.requested_model.clone(),
        resolved_model: receipt.resolved_model.clone(),
        provider: receipt.provider.clone(),
        auth_lane: receipt.auth_lane.clone(),
        changed_files: Vec::new(),
        terminal_receipt_file: Some(receipt_file.clone()),
        reason:
            "subagent lifecycle no-write smoke recorded deterministic terminal receipt without model execution"
                .to_string(),
        now_ms,
    })
    .map_err(|err| err.to_string())?;

    Ok(receipt_file)
}

fn runtime_run_once_receipts_file(harness_home: &Path) -> PathBuf {
    harness_home
        .join("state")
        .join("runtime-queue")
        .join("run-once-receipts.jsonl")
}

fn subagent_lifecycle_args_from_args(args: &[String]) -> Result<SubagentLifecycleArgs, String> {
    let options = SimpleOptions::parse(
        args,
        "subagent-lifecycle",
        &[
            "--action",
            "--subagent-id",
            "--source-home",
            "--workspace",
            "--model",
            "--timeout-ms",
            "--reason",
            "--now-ms",
        ],
        &["--no-write"],
    )?;
    let action = options.required("--action")?;
    let workspace = options
        .optional("--workspace")
        .map(PathBuf::from)
        .unwrap_or(env::current_dir().map_err(|err| err.to_string())?);
    let source_home = options
        .optional("--source-home")
        .map(PathBuf::from)
        .unwrap_or_else(|| workspace.clone());
    let timeout_ms = options.optional_u64("--timeout-ms")?.unwrap_or(180_000);
    let now_ms = options
        .optional_i64("--now-ms")?
        .unwrap_or(current_time_ms()?);

    Ok(SubagentLifecycleArgs {
        target_home: options.target_home.clone(),
        source_home,
        workspace,
        action,
        subagent_id: options.optional("--subagent-id").map(ToString::to_string),
        model: options
            .optional("--model")
            .unwrap_or("gpt-5.3-codex-spark")
            .to_string(),
        timeout_ms,
        no_write: options.has_flag("--no-write"),
        reason: options.optional("--reason").map(ToString::to_string),
        now_ms,
    })
}

fn context_rollover_args_from_args(args: &[String]) -> Result<ContextRolloverArgs, String> {
    let options = SimpleOptions::parse(
        args,
        "context-rollover",
        &[
            "--action",
            "--queue-id",
            "--new-working-session-key",
            "--reason",
            "--now-ms",
        ],
        &[],
    )?;
    let now_ms = options
        .optional_i64("--now-ms")?
        .unwrap_or(current_time_ms()?);

    Ok(ContextRolloverArgs {
        target_home: options.target_home.clone(),
        action: options.required("--action")?,
        queue_id: options.optional("--queue-id").map(ToString::to_string),
        new_working_session_key: options
            .optional("--new-working-session-key")
            .map(ToString::to_string),
        reason: options.optional("--reason").map(ToString::to_string),
        now_ms,
    })
}

fn subagent_lifecycle_no_write_prompt() -> String {
    [
        "Subagent lifecycle no-write smoke.",
        "Report cwd.",
        "Report the first AGENTS.md heading you can read.",
        "Report provider/auth visibility if available.",
        "Report changed files as none.",
        "Do not edit files.",
    ]
    .join("\n")
}

fn workspace_status_snapshot(
    workspace: &Path,
    harness_home: &Path,
) -> Result<WorkspaceStatusSnapshot, String> {
    if workspace.join(".git").is_dir() {
        let output = Command::new("git")
            .arg("-C")
            .arg(workspace)
            .arg("status")
            .arg("--porcelain")
            .arg("--untracked-files=all")
            .output();
        if let Ok(output) = output
            && output.status.success()
        {
            let target_prefix = harness_home
                .strip_prefix(workspace)
                .ok()
                .map(|path| normalized_relative_path(path).replace('\\', "/"));
            let entries = String::from_utf8_lossy(&output.stdout)
                .lines()
                .filter_map(|line| {
                    let status_path = git_status_path_from_line(line);
                    if let Some(prefix) = target_prefix.as_deref() {
                        if git_status_path_is_under(&status_path, prefix) {
                            return None;
                        }
                    }
                    let signature = workspace_status_entry_signature(workspace, &status_path);
                    Some((line.to_string(), format!("{line} | {signature}")))
                })
                .collect::<BTreeMap<_, _>>();
            return Ok(WorkspaceStatusSnapshot {
                method: "git-status-porcelain".to_string(),
                entries,
            });
        }
    }
    filesystem_manifest_snapshot(workspace, &[harness_home.to_path_buf()])
}

fn git_status_path_from_line(line: &str) -> String {
    let path = line.get(3..).unwrap_or(line).trim();
    let path = path.rsplit(" -> ").next().unwrap_or(path).trim();
    path.strip_prefix('"')
        .and_then(|path| path.strip_suffix('"'))
        .unwrap_or(path)
        .to_string()
}

fn git_status_path_is_under(path: &str, prefix: &str) -> bool {
    !prefix.is_empty() && (path == prefix || path.starts_with(&format!("{prefix}/")))
}

fn workspace_status_entry_signature(workspace: &Path, git_path: &str) -> String {
    let path = workspace.join(git_path);
    match fs::metadata(&path) {
        Ok(metadata) if metadata.is_file() => match fs::read(&path) {
            Ok(bytes) => format!(
                "file len={} fnv64={}",
                bytes.len(),
                fnv1a_64_hex_bytes(&bytes)
            ),
            Err(err) => format!("file-read-error {err}"),
        },
        Ok(metadata) if metadata.is_dir() => {
            format!("dir len={}", metadata.len())
        }
        Ok(metadata) => format!("other len={}", metadata.len()),
        Err(_) => "missing".to_string(),
    }
}

fn fnv1a_64_hex_bytes(bytes: &[u8]) -> String {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

fn filesystem_manifest_snapshot(
    root: &Path,
    excludes: &[PathBuf],
) -> Result<WorkspaceStatusSnapshot, String> {
    let root = absolute_path(root)?;
    let excludes = excludes
        .iter()
        .map(|path| absolute_path(path))
        .collect::<Result<Vec<_>, _>>()?;
    let mut entries = BTreeMap::new();
    if !root.exists() {
        return Ok(WorkspaceStatusSnapshot {
            method: "filesystem-manifest".to_string(),
            entries,
        });
    }

    let mut stack = vec![root.clone()];
    while let Some(path) = stack.pop() {
        if excludes.iter().any(|exclude| path.starts_with(exclude)) {
            continue;
        }
        let metadata = fs::metadata(&path).map_err(|err| err.to_string())?;
        if metadata.is_dir() {
            let mut children = fs::read_dir(&path)
                .map_err(|err| err.to_string())?
                .map(|entry| {
                    entry
                        .map(|entry| entry.path())
                        .map_err(|err| err.to_string())
                })
                .collect::<Result<Vec<_>, _>>()?;
            children.sort();
            stack.extend(children.into_iter().rev());
        } else if metadata.is_file() {
            let rel = path
                .strip_prefix(&root)
                .map(normalized_relative_path)
                .unwrap_or_else(|_| path.display().to_string());
            let modified_ms = metadata
                .modified()
                .ok()
                .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
                .map(|duration| duration.as_millis())
                .unwrap_or(0);
            entries.insert(
                rel,
                format!("len={} modifiedMs={modified_ms}", metadata.len()),
            );
        }
    }

    Ok(WorkspaceStatusSnapshot {
        method: "filesystem-manifest".to_string(),
        entries,
    })
}

fn snapshot_diff(
    before: &BTreeMap<String, String>,
    after: &BTreeMap<String, String>,
) -> Vec<SnapshotDiffEntry> {
    let mut keys = BTreeSet::new();
    keys.extend(before.keys().cloned());
    keys.extend(after.keys().cloned());
    keys.into_iter()
        .filter_map(|path| {
            let before_value = before.get(&path).cloned();
            let after_value = after.get(&path).cloned();
            (before_value != after_value).then_some(SnapshotDiffEntry {
                path,
                before: before_value,
                after: after_value,
            })
        })
        .collect()
}

fn absolute_path(path: &Path) -> Result<PathBuf, String> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(env::current_dir()
            .map_err(|err| err.to_string())?
            .join(path))
    }
}

fn path_identity_key(path: &Path) -> String {
    let mut key = path
        .components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/");
    while key.ends_with('/') {
        key.pop();
    }
    if cfg!(windows) {
        key.to_ascii_lowercase()
    } else {
        key
    }
}

fn normalized_relative_path(path: &Path) -> String {
    path.components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

fn source_from_args(args: &[String]) -> Result<AgentSource, String> {
    let mut home = default_source_home();
    let mut workspace = None;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "--source-home" => {
                i += 1;
                home = args
                    .get(i)
                    .map(PathBuf::from)
                    .ok_or_else(|| "--source-home requires a path".to_string())?;
            }
            "--workspace" => {
                i += 1;
                workspace = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--workspace requires a path".to_string())?,
                );
            }
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    Ok(match workspace {
        Some(workspace) => AgentSource::with_workspace(home, workspace),
        None => AgentSource::new(home),
    })
}

fn channel_identity_check_args_from_args(
    args: &[String],
) -> Result<ChannelIdentityCheckArgs, String> {
    let mut target_home = default_harness_home();
    let mut platform = None;
    let mut account_id = "default".to_string();
    let mut chat_id = None;
    let mut thread_id = None;
    let mut agent_id = None;
    let mut json = false;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--platform" => {
                i += 1;
                platform = Some(
                    args.get(i)
                        .cloned()
                        .ok_or_else(|| "--platform requires a value".to_string())?,
                );
            }
            "--account-id" | "--account" => {
                i += 1;
                account_id = args
                    .get(i)
                    .cloned()
                    .ok_or_else(|| "--account-id requires a value".to_string())?;
            }
            "--chat-id" | "--channel-id" => {
                i += 1;
                chat_id = Some(
                    args.get(i)
                        .cloned()
                        .ok_or_else(|| "--chat-id requires a value".to_string())?,
                );
            }
            "--thread-id" => {
                i += 1;
                thread_id = Some(
                    args.get(i)
                        .cloned()
                        .ok_or_else(|| "--thread-id requires a value".to_string())?,
                );
            }
            "--agent" => {
                i += 1;
                agent_id = Some(
                    args.get(i)
                        .cloned()
                        .ok_or_else(|| "--agent requires an id".to_string())?,
                );
            }
            "--json" => json = true,
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    Ok(ChannelIdentityCheckArgs {
        target_home,
        platform: platform.ok_or_else(|| "--platform is required".to_string())?,
        account_id,
        chat_id: chat_id.ok_or_else(|| "--chat-id is required".to_string())?,
        thread_id,
        agent_id,
        json,
    })
}

fn cron_scheduler_run_once_args_from_args(
    args: &[String],
) -> Result<CronSchedulerRunOnceArgs, String> {
    let mut home = default_source_home();
    let mut workspace = None;
    let mut runtime_workspace = None;
    let mut target_home = default_harness_home();
    let mut dry_run = false;
    let mut enabled_override = None;
    let mut native_enabled_override = None;
    let mut deterministic_enabled_override = None;
    let mut resume_cron_override = None;
    let mut include_registered_cron_override = None;
    let mut allow_deterministic_run_override = None;
    let mut execute_shell_override = None;
    let mut max_catchup_per_tick_override = None;
    let mut max_enqueue_per_tick_override = None;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "--source-home" => {
                i += 1;
                home = args
                    .get(i)
                    .map(PathBuf::from)
                    .ok_or_else(|| "--source-home requires a path".to_string())?;
            }
            "--workspace" => {
                i += 1;
                workspace = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--workspace requires a path".to_string())?,
                );
            }
            "--runtime-workspace" => {
                i += 1;
                runtime_workspace = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--runtime-workspace requires a path".to_string())?,
                );
            }
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--dry-run" => dry_run = true,
            "--enable" | "--enabled" => enabled_override = Some(true),
            "--disable" | "--disabled" => enabled_override = Some(false),
            "--native-cron" => native_enabled_override = Some(true),
            "--no-native-cron" => native_enabled_override = Some(false),
            "--deterministic-cron" => deterministic_enabled_override = Some(true),
            "--no-deterministic-cron" => deterministic_enabled_override = Some(false),
            "--resume-cron" => resume_cron_override = Some(true),
            "--hold-cron" => resume_cron_override = Some(false),
            "--include-registered-cron" => include_registered_cron_override = Some(true),
            "--no-include-registered-cron" => include_registered_cron_override = Some(false),
            "--allow-deterministic-run" => allow_deterministic_run_override = Some(true),
            "--hold-deterministic-run" => allow_deterministic_run_override = Some(false),
            "--execute-shell" => execute_shell_override = Some(true),
            "--dry-run-shell" => execute_shell_override = Some(false),
            "--max-catchup-per-tick" => {
                i += 1;
                max_catchup_per_tick_override = Some(
                    args.get(i)
                        .ok_or_else(|| {
                            "--max-catchup-per-tick requires a positive integer".to_string()
                        })
                        .and_then(|value| parse_limit(value))?,
                );
            }
            "--max-enqueue-per-tick" => {
                i += 1;
                max_enqueue_per_tick_override = Some(
                    args.get(i)
                        .ok_or_else(|| {
                            "--max-enqueue-per-tick requires a positive integer".to_string()
                        })
                        .and_then(|value| parse_limit(value))?,
                );
            }
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    let source = match workspace {
        Some(workspace) => AgentSource::with_workspace(home, workspace),
        None => AgentSource::new(home),
    };
    Ok(CronSchedulerRunOnceArgs {
        source,
        runtime_workspace,
        target_home,
        dry_run,
        enabled_override,
        native_enabled_override,
        deterministic_enabled_override,
        resume_cron_override,
        include_registered_cron_override,
        allow_deterministic_run_override,
        execute_shell_override,
        max_catchup_per_tick_override,
        max_enqueue_per_tick_override,
    })
}

fn cron_scheduler_loop_args_from_args(args: &[String]) -> Result<CronSchedulerLoopArgs, String> {
    let mut loop_only = Vec::new();
    let mut run_once_args = Vec::new();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--iterations" | "--idle-ms" | "--max-consecutive-errors" | "--stop-file" => {
                loop_only.push(args[i].clone());
                i += 1;
                loop_only.push(
                    args.get(i)
                        .cloned()
                        .ok_or_else(|| format!("{} requires a value", args[i - 1]))?,
                );
            }
            flag => {
                run_once_args.push(flag.to_string());
                if cron_scheduler_run_once_arg_requires_value(flag) {
                    i += 1;
                    run_once_args.push(
                        args.get(i)
                            .cloned()
                            .ok_or_else(|| format!("{flag} requires a value"))?,
                    );
                }
            }
        }
        i += 1;
    }

    let run_once = cron_scheduler_run_once_args_from_args(&run_once_args)?;
    let mut iterations = 0usize;
    let mut idle_ms = None;
    let mut max_consecutive_errors = 5usize;
    let mut stop_file = None;
    let mut i = 0;
    while i < loop_only.len() {
        match loop_only[i].as_str() {
            "--iterations" => {
                i += 1;
                iterations = loop_only
                    .get(i)
                    .ok_or_else(|| "--iterations requires a value".to_string())
                    .and_then(|value| parse_usize(value, "--iterations"))?;
            }
            "--idle-ms" => {
                i += 1;
                idle_ms = Some(
                    loop_only
                        .get(i)
                        .ok_or_else(|| "--idle-ms requires a value".to_string())
                        .and_then(|value| parse_u64(value, "--idle-ms"))?,
                );
            }
            "--max-consecutive-errors" => {
                i += 1;
                max_consecutive_errors = loop_only
                    .get(i)
                    .ok_or_else(|| "--max-consecutive-errors requires a value".to_string())
                    .and_then(|value| parse_limit(value))?;
            }
            "--stop-file" => {
                i += 1;
                stop_file = Some(
                    loop_only
                        .get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--stop-file requires a path".to_string())?,
                );
            }
            flag => return Err(format!("unknown loop argument: {flag}")),
        }
        i += 1;
    }

    Ok(CronSchedulerLoopArgs {
        run_once,
        iterations,
        idle_ms,
        max_consecutive_errors,
        stop_file,
    })
}

fn cron_scheduler_run_once_arg_requires_value(flag: &str) -> bool {
    matches!(
        flag,
        "--source-home"
            | "--workspace"
            | "--runtime-workspace"
            | "--harness-home"
            | "--target-home"
            | "--max-catchup-per-tick"
            | "--max-enqueue-per-tick"
    )
}

struct DryRunArgs {
    source: AgentSource,
    target_home: PathBuf,
    conflict_policy: ConflictPolicy,
    output_dir: Option<PathBuf>,
}

struct ExecuteArgs {
    source: AgentSource,
    target_home: PathBuf,
    conflict_policy: ConflictPolicy,
    include_sensitive: bool,
}

struct ChannelCredentialsExportArgs {
    source: AgentSource,
    target_home: PathBuf,
    include_sensitive: bool,
}

struct ChannelCredentialCandidate {
    env_name: String,
    value: String,
    source_path: String,
    sensitive: bool,
}

struct ChannelCredentialExportEntry {
    env_name: String,
    source_path: String,
    length: usize,
    sensitive: bool,
    exported: bool,
    reason: String,
}

struct ChannelCredentialExportReport {
    env_file: PathBuf,
    receipt_file: PathBuf,
    entries: Vec<ChannelCredentialExportEntry>,
}

struct RegistryExportArgs {
    source: AgentSource,
    target_home: PathBuf,
    conflict_policy: ConflictPolicy,
}

struct EnableCheckArgs {
    target_home: PathBuf,
}

struct WorkerEnqueueArgs {
    target_home: PathBuf,
    kind: WorkerJobKind,
    lane: Option<String>,
    payload: serde_json::Value,
    idempotency_key: Option<String>,
    parent_job_id: Option<String>,
    job_group_id: Option<String>,
    master_agent_id: Option<String>,
    master_session_key: Option<String>,
    wake_policy: Option<serde_json::Value>,
    source: Option<String>,
    priority: i64,
    available_at_ms: Option<i64>,
    max_attempts: i64,
    timeout_ms: Option<u64>,
    cascade_timeout_ms: Option<u64>,
    rate_key: Option<String>,
    concurrency_group_key: Option<String>,
}

struct WorkerRunOnceArgs {
    target_home: PathBuf,
    lane: Option<String>,
    worker_id: String,
    lease_ms: i64,
}

struct WorkerLoopArgs {
    target_home: PathBuf,
    lane: Option<String>,
    worker_id: String,
    lease_ms: i64,
    iterations: usize,
    idle_ms: u64,
    max_consecutive_errors: usize,
    stop_when_idle: bool,
    stop_file: Option<PathBuf>,
}

struct WorkerStatusArgs {
    target_home: PathBuf,
}

struct ContextRolloverArgs {
    target_home: PathBuf,
    action: String,
    queue_id: Option<String>,
    new_working_session_key: Option<String>,
    reason: Option<String>,
    now_ms: i64,
}

struct SubagentLifecycleArgs {
    target_home: PathBuf,
    source_home: PathBuf,
    workspace: PathBuf,
    action: String,
    subagent_id: Option<String>,
    model: String,
    timeout_ms: u64,
    no_write: bool,
    reason: Option<String>,
    now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct SubagentLifecycleSmokeReport {
    schema: &'static str,
    harness_home: PathBuf,
    source_home: PathBuf,
    workspace: PathBuf,
    workspace_write_policy: String,
    model: String,
    timeout_ms: u64,
    subagent_id: String,
    idempotency_key: String,
    enqueue: WorkerEnqueueReport,
    run: WorkerRunOnceReport,
    runtime_execution_mode: String,
    runtime_terminal_receipt_file: Option<PathBuf>,
    completed_lifecycle: SubagentLifecycleShowReport,
    close_report: SubagentLifecycleShowReport,
    lifecycle: SubagentLifecycleShowReport,
    workspace_status_method: String,
    workspace_clean: bool,
    workspace_diff: Vec<SnapshotDiffEntry>,
    harness_state_writes: Vec<SnapshotDiffEntry>,
    expected_harness_state_files: Vec<PathBuf>,
    prompt: String,
    reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WorkspaceStatusSnapshot {
    method: String,
    entries: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct SnapshotDiffEntry {
    path: String,
    before: Option<String>,
    after: Option<String>,
}

struct WorkerCancelArgs {
    target_home: PathBuf,
    job_id: String,
    reason: String,
}

struct HarnessStatusArgs {
    target_home: PathBuf,
    json: bool,
}

struct ChannelIdentityCheckArgs {
    target_home: PathBuf,
    platform: String,
    account_id: String,
    chat_id: String,
    thread_id: Option<String>,
    agent_id: Option<String>,
    json: bool,
}

#[derive(Clone)]
struct CronSchedulerRunOnceArgs {
    source: AgentSource,
    runtime_workspace: Option<PathBuf>,
    target_home: PathBuf,
    dry_run: bool,
    enabled_override: Option<bool>,
    native_enabled_override: Option<bool>,
    deterministic_enabled_override: Option<bool>,
    resume_cron_override: Option<bool>,
    include_registered_cron_override: Option<bool>,
    allow_deterministic_run_override: Option<bool>,
    execute_shell_override: Option<bool>,
    max_catchup_per_tick_override: Option<usize>,
    max_enqueue_per_tick_override: Option<usize>,
}

struct CronSchedulerLoopArgs {
    run_once: CronSchedulerRunOnceArgs,
    iterations: usize,
    idle_ms: Option<u64>,
    max_consecutive_errors: usize,
    stop_file: Option<PathBuf>,
}

struct JsonlRepairArgs {
    path: PathBuf,
    output: Option<PathBuf>,
    invalid_output: Option<PathBuf>,
    apply: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonlRepairReport {
    path: PathBuf,
    output: PathBuf,
    invalid_output: PathBuf,
    backup: Option<PathBuf>,
    total_lines: usize,
    valid_lines: usize,
    output_lines: usize,
    recovered_lines: usize,
    recovered_values: usize,
    invalid_lines: usize,
    applied: bool,
}

struct MemorySearchArgs {
    target_home: PathBuf,
    query: String,
    limit: usize,
    max_file_bytes: u64,
    json: bool,
    write_receipt: bool,
}

struct MemoryCredentialsExportArgs {
    source_home: PathBuf,
    target_home: PathBuf,
    include_sensitive: bool,
    json: bool,
}

struct MemoryVectorSearchArgs {
    target_home: PathBuf,
    query: String,
    limit: usize,
    json: bool,
    write_receipt: bool,
}

struct MemoryCanvasRunArgs {
    target_home: PathBuf,
    agent_id: Option<String>,
    json: bool,
}

struct MemoryEmbeddingBackfillArgs {
    target_home: PathBuf,
    lane: MemoryEmbeddingBackfillLane,
    model: String,
    vector_dimension: i64,
    batch_size: usize,
    max_items: usize,
    rate_limit_per_minute: usize,
    retry_cap: usize,
    coverage_threshold_bps: u64,
    json: bool,
}

struct MemoryServiceStatusArgs {
    target_home: PathBuf,
    agent_id: Option<String>,
    json: bool,
}

struct MemoryServiceRecallArgs {
    target_home: PathBuf,
    agent_id: Option<String>,
    query: String,
    limit: usize,
    max_file_bytes: u64,
    json: bool,
}

struct MemoryReadPathSmokeArgs {
    target_home: PathBuf,
    agent_id: Option<String>,
    query: String,
    limit: usize,
    json: bool,
}

struct MemoryServiceProposeArgs {
    target_home: PathBuf,
    agent_id: Option<String>,
    session_key: Option<String>,
    text: String,
    payload: serde_json::Value,
    json: bool,
}

struct MemoryServiceStoreArgs {
    target_home: PathBuf,
    agent_id: Option<String>,
    session_key: Option<String>,
    text: String,
    payload: serde_json::Value,
    approved: bool,
    json: bool,
}

#[derive(Debug, Clone)]
struct SupervisorRunArgs {
    target_home: PathBuf,
    source_home: PathBuf,
    workspace: Option<PathBuf>,
    runtime_workspace: Option<PathBuf>,
    service: String,
    harness_cli: PathBuf,
    codex_exe: Option<PathBuf>,
    node_exe: PathBuf,
    gateway_script: PathBuf,
    agent_id: Option<String>,
    telegram_account: Option<String>,
    loop_name: Option<String>,
    idle_ms: u64,
    max_consecutive_errors: usize,
    restart_delay_ms: u64,
    max_restarts: usize,
    child_iterations: usize,
    runtime_concurrency: usize,
    timeout_ms: u64,
    idle_timeout_ms: u64,
    max_prompt_file_bytes: usize,
    max_skill_file_bytes: usize,
    lane: Option<String>,
    worker_id: Option<String>,
    lease_ms: i64,
    poll_timeout_seconds: u64,
    max_updates: usize,
    outbox_limit: usize,
    discord_account: Option<String>,
    stop_file: Option<PathBuf>,
}

#[derive(Debug, Clone)]
struct SupervisedChildRuntimeState {
    pid: u32,
    generation_id: String,
    started_at_ms: i64,
    last_exit_at_ms: Option<i64>,
    last_exit_code: Option<i32>,
    last_error_class: Option<String>,
    restart_count: usize,
    backoff_until_ms: Option<i64>,
}

struct SupervisorPlanArgs {
    target_home: PathBuf,
    source_home: PathBuf,
    workspace: Option<PathBuf>,
    runtime_workspace: Option<PathBuf>,
    harness_cli: PathBuf,
    codex_exe: Option<PathBuf>,
    node_exe: PathBuf,
    gateway_script: PathBuf,
    agent_id: Option<String>,
    output_dir: Option<PathBuf>,
    task_prefix: String,
    include_runtime: bool,
    runtime_workers: usize,
    include_worker: bool,
    include_cron_scheduler: bool,
    include_progress: bool,
    include_telegram: bool,
    include_discord: bool,
    idle_ms: u64,
    runtime_timeout_ms: u64,
    runtime_idle_timeout_ms: u64,
    max_consecutive_errors: usize,
    telegram_poll_timeout_seconds: u64,
    telegram_max_updates: usize,
    telegram_outbox_limit: usize,
}

struct HarnessSkillsSyncArgs {
    target_home: PathBuf,
    force: bool,
}

struct SkillsArgs {
    source: AgentSource,
    harness_home: Option<PathBuf>,
    output_dir: Option<PathBuf>,
    query: Option<String>,
    agent_id: Option<String>,
    channel: Option<String>,
    match_workspace: Option<String>,
    limit: usize,
}

struct TurnPlanArgs {
    source: AgentSource,
    harness_home: Option<PathBuf>,
    output_dir: Option<PathBuf>,
    platform: String,
    channel_id: String,
    user_id: String,
    agent_id: Option<String>,
    session_key: Option<String>,
    message: String,
    skill_limit: usize,
    max_prompt_file_bytes: usize,
    max_skill_file_bytes: usize,
}

struct QueueEnqueueArgs {
    turn: TurnPlanArgs,
    target_home: PathBuf,
    now_ms: i64,
}

struct ChannelRunOnceArgs {
    turn: TurnPlanArgs,
    runtime_workspace: Option<PathBuf>,
    target_home: PathBuf,
    now_ms: i64,
    codex_exe: Option<PathBuf>,
    timeout_ms: u64,
    idle_timeout_ms: u64,
    outbox_limit: usize,
}

struct ChannelOutboxPlanArgs {
    target_home: PathBuf,
    platform: Option<String>,
    limit: usize,
}

struct ChannelDeliveryRecordArgs {
    target_home: PathBuf,
    delivery_id: String,
    status: ChannelDeliveryStatus,
    platform: String,
    channel_id: String,
    user_id: String,
    session_key: String,
    provider_message_id: Option<String>,
    error: Option<String>,
    now_ms: i64,
}

#[derive(Debug, Clone)]
struct ProgressDeliveryOnceArgs {
    target_home: PathBuf,
    platform: Option<String>,
    telegram_account: Option<String>,
    min_update_interval_ms: i64,
    max_events_per_panel: usize,
    max_preview_chars: usize,
    current_step_max_chars: usize,
    preempt_after_wake_sequence: Option<u64>,
}

#[derive(Debug, Clone)]
struct ProgressDeliveryLoopArgs {
    send: ProgressDeliveryOnceArgs,
    iterations: usize,
    idle_ms: u64,
    max_consecutive_errors: usize,
    stop_file: Option<PathBuf>,
}

#[derive(Debug, Clone)]
struct ProgressDeliveryOnceReport {
    pending_count: usize,
    skipped_muted: usize,
    volume_limited: usize,
    sent_messages: usize,
    edited_messages: usize,
    skipped_denied: usize,
    skipped_permanent: usize,
    failed_deliveries: usize,
    warnings: Vec<String>,
}

struct TelegramProbeArgs {
    target_home: PathBuf,
}

struct TelegramProbeReport {
    harness_home: PathBuf,
    receipt_file: PathBuf,
    status: String,
    reason: String,
    bot_id: Option<String>,
    username: Option<String>,
    can_join_groups: Option<bool>,
    can_read_all_group_messages: Option<bool>,
    supports_inline_queries: Option<bool>,
    at_ms: i64,
}

struct TelegramPollOnceArgs {
    source: AgentSource,
    runtime_workspace: Option<PathBuf>,
    target_home: PathBuf,
    agent_id: Option<String>,
    telegram_account: Option<String>,
    skill_limit: usize,
    codex_exe: Option<PathBuf>,
    timeout_ms: u64,
    idle_timeout_ms: u64,
    poll_timeout_seconds: u64,
    max_updates: usize,
    outbox_limit: usize,
}

struct TelegramPollOnceReport {
    update_count: usize,
    handled_messages: usize,
    skipped_updates: usize,
    delivered_messages: usize,
    failed_deliveries: usize,
    next_offset: Option<i64>,
    warnings: Vec<String>,
}

#[derive(Debug, Clone, Default)]
struct ChannelAccessPolicy {
    telegram_admin_user_ids: BTreeSet<String>,
    telegram_allowed_user_ids: BTreeSet<String>,
    telegram_group_admin_user_ids: BTreeSet<String>,
    telegram_group_allowed_user_ids: BTreeSet<String>,
    telegram_direct_chat_ids: BTreeSet<String>,
    telegram_group_chat_ids: BTreeSet<String>,
    telegram_group_open: bool,
    discord_admin_user_ids: BTreeSet<String>,
    discord_allowed_user_ids: BTreeSet<String>,
    discord_group_allowed_user_ids: BTreeSet<String>,
    discord_channel_ids: BTreeSet<String>,
    discord_guild_ids: BTreeSet<String>,
    discord_group_open: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ChannelAccessDecision {
    Allowed(ChannelPermission),
    Denied(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChannelPermission {
    Admin,
    Limited,
}

struct TelegramLoopArgs {
    poll: TelegramPollOnceArgs,
    loop_name: String,
    iterations: usize,
    idle_ms: u64,
    max_consecutive_errors: usize,
    stop_file: Option<PathBuf>,
}

struct DiscordOutboxSendOnceArgs {
    target_home: PathBuf,
    discord_account: Option<String>,
    outbox_limit: usize,
}

struct DiscordOutboxLoopArgs {
    send: DiscordOutboxSendOnceArgs,
    iterations: usize,
    idle_ms: u64,
    max_consecutive_errors: usize,
    stop_file: Option<PathBuf>,
}

struct DiscordDmProbeArgs {
    target_home: PathBuf,
    user_id: String,
    send_message: bool,
    message: String,
}

struct DiscordDmHistoryProbeArgs {
    target_home: PathBuf,
    channel_id: Option<String>,
    user_id: Option<String>,
    limit: usize,
}

struct DiscordEventRunOnceArgs {
    source: AgentSource,
    runtime_workspace: Option<PathBuf>,
    target_home: PathBuf,
    agent_id: Option<String>,
    discord_account: Option<String>,
    skill_limit: usize,
    codex_exe: Option<PathBuf>,
    timeout_ms: u64,
    idle_timeout_ms: u64,
    outbox_limit: usize,
    event_file: Option<PathBuf>,
    event_json: Option<String>,
}

struct DiscordGatewayMessage {
    message_id: String,
    guild_id: Option<String>,
    channel_id: String,
    user_id: String,
    content: String,
    inbound_context: Option<String>,
    inbound_media_artifacts: Vec<InboundMediaArtifact>,
    attachments: Vec<DiscordAttachmentMetadata>,
    reply_context: Option<DiscordReplyContext>,
    author_is_bot: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DiscordAttachmentMetadata {
    id: Option<String>,
    filename: Option<String>,
    content_type: Option<String>,
    size: Option<u64>,
    width: Option<u32>,
    height: Option<u32>,
    url: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DiscordReplyContext {
    referenced_message_id: Option<String>,
    referenced_channel_id: Option<String>,
    referenced_guild_id: Option<String>,
    referenced_author_id: Option<String>,
    referenced_text_preview: Option<String>,
    referenced_preview_length: Option<usize>,
    referenced_preview_truncated: Option<bool>,
    referenced_text: Option<BoundedText>,
    has_attachments: bool,
    attachment_count: usize,
    embeds_count: usize,
    source: String,
    source_available: bool,
}

struct DiscordEventRunOnceReport {
    harness_home: PathBuf,
    status: String,
    reason: String,
    message_id: Option<String>,
    guild_id: Option<String>,
    channel_id: Option<String>,
    user_id: Option<String>,
    run: Option<ChannelRunOnceReport>,
}

struct DiscordGatewayArgs {
    source: AgentSource,
    runtime_workspace: Option<PathBuf>,
    target_home: PathBuf,
    node_exe: PathBuf,
    gateway_script: PathBuf,
    harness_cli: PathBuf,
    agent_id: Option<String>,
    discord_account: Option<String>,
    codex_exe: Option<PathBuf>,
    max_messages: usize,
    stop_file: Option<PathBuf>,
}

struct PluginSidecarProbeArgs {
    target_home: PathBuf,
    node_exe: PathBuf,
    sidecar_script: PathBuf,
}

struct PluginSidecarCallArgs {
    target_home: PathBuf,
    node_exe: PathBuf,
    sidecar_script: PathBuf,
    method: String,
    params: serde_json::Value,
}

struct DiscordOutboxSendOnceReport {
    pending_count: usize,
    delivered_messages: usize,
    failed_deliveries: usize,
    warnings: Vec<String>,
}

struct DiscordDmProbeReport {
    harness_home: PathBuf,
    status: String,
    reason: String,
    user_id: String,
    channel_id: Option<String>,
    provider_message_id: Option<String>,
    sent_message: bool,
    warnings: Vec<String>,
}

#[derive(Default)]
struct DiscordDmProbeRef {
    channel_id: Option<String>,
    user_id: Option<String>,
}

struct DiscordDmHistoryProbeReport {
    harness_home: PathBuf,
    status: String,
    reason: String,
    channel_id: Option<String>,
    user_id: Option<String>,
    limit: usize,
    message_count: usize,
    user_message_count: usize,
    bot_message_count: usize,
    messages: Vec<DiscordMessageSummary>,
    warnings: Vec<String>,
}

#[derive(Clone)]
struct DiscordMessageSummary {
    message_id: String,
    author_id: Option<String>,
    author_bot: Option<bool>,
    timestamp: Option<String>,
    content_length: Option<usize>,
}

struct QueuePrepareArgs {
    target_home: PathBuf,
    queue_id: Option<String>,
    max_prompt_file_bytes: usize,
    max_skill_file_bytes: usize,
}

struct RuntimeRunOnceArgs {
    target_home: PathBuf,
    queue_id: Option<String>,
    codex_exe: Option<PathBuf>,
    timeout_ms: u64,
    idle_timeout_ms: u64,
    max_prompt_file_bytes: usize,
    max_skill_file_bytes: usize,
}

#[derive(Debug, Clone)]
struct RuntimeLoopArgs {
    target_home: PathBuf,
    loop_name: String,
    codex_exe: Option<PathBuf>,
    timeout_ms: u64,
    idle_timeout_ms: u64,
    max_prompt_file_bytes: usize,
    max_skill_file_bytes: usize,
    runtime_concurrency: usize,
    iterations: usize,
    idle_ms: u64,
    max_consecutive_errors: usize,
    safe_mode_restart_ms: Option<u64>,
    stop_when_idle: bool,
    stop_file: Option<PathBuf>,
}

struct RuntimeLoopSummary {
    target_home: PathBuf,
    started_at_ms: i64,
    finished_at_ms: i64,
    iterations: usize,
    completed: usize,
    idle: usize,
    errors: usize,
    consecutive_errors: usize,
    safe_mode_restarts: usize,
    runtime_concurrency: usize,
    stop_reason: String,
    last_status: Option<RuntimeRunOnceStatus>,
    last_queue_id: Option<String>,
    last_reason: Option<String>,
    report_file: PathBuf,
}

struct CodexPlanArgs {
    target_home: PathBuf,
    execution_dir: Option<PathBuf>,
    codex_exe: Option<PathBuf>,
}

struct CodexPreflightArgs {
    target_home: PathBuf,
    execution_dir: Option<PathBuf>,
    plan_file: Option<PathBuf>,
}

struct CodexLaunchProbeArgs {
    target_home: PathBuf,
    execution_dir: Option<PathBuf>,
    plan_file: Option<PathBuf>,
    startup_probe_ms: u64,
}

struct CodexRunArgs {
    target_home: PathBuf,
    execution_dir: Option<PathBuf>,
    plan_file: Option<PathBuf>,
    timeout_ms: u64,
    idle_timeout_ms: u64,
}

struct CodexCompleteArgs {
    target_home: PathBuf,
    execution_dir: Option<PathBuf>,
    plan_file: Option<PathBuf>,
    assistant_message: String,
    finished_at_ms: i64,
}

struct CronPlanArgs {
    source: AgentSource,
    output_dir: Option<PathBuf>,
    now_ms: i64,
    resume_cron: bool,
    limit: usize,
}

struct DeterministicCronPlanArgs {
    source: AgentSource,
    output_dir: Option<PathBuf>,
    allow_deterministic_run: bool,
    limit: usize,
}

struct SubagentPlanArgs {
    source: AgentSource,
    output_dir: Option<PathBuf>,
    resume_subagents: bool,
    limit: usize,
}

struct MemoryHookArgs {
    target_home: PathBuf,
    hook: MemoryHookKind,
    agent_id: Option<String>,
    session_key: Option<String>,
    query: Option<String>,
    prompt_bundle_json: Option<PathBuf>,
    assistant_text: Option<String>,
    success: bool,
    slot: Option<String>,
    operation: Option<String>,
    payload: serde_json::Value,
    now_ms: i64,
    limit: usize,
    max_file_bytes: u64,
}

struct OpsBackupArgs {
    target_home: PathBuf,
    label: Option<String>,
    max_file_bytes: u64,
    summary_only: bool,
}

struct OpsCutoverRequestArgs {
    target_home: PathBuf,
    action: LiveControlAction,
    summary: Option<String>,
    candidate_binary: Option<PathBuf>,
    staging_home: Option<PathBuf>,
    test_notes: Vec<String>,
}

struct OpsCutoverApproveArgs {
    target_home: PathBuf,
    ticket_id: String,
    action: LiveControlAction,
    issued_to: Option<String>,
    ttl_seconds: Option<i64>,
    reason: Option<String>,
}

struct OpsCutoverApplyArgs {
    target_home: PathBuf,
    ticket_id: String,
    action: LiveControlAction,
    live_control_token: Option<String>,
    note: Option<String>,
}

struct OpsCutoverStatusArgs {
    target_home: PathBuf,
    action: Option<LiveControlAction>,
    live_control_token: Option<String>,
}

struct OpsCutoverReceiptArgs {
    target_home: PathBuf,
    note: Option<String>,
}

struct OpsControlArgs {
    target_home: PathBuf,
    action: OpsControlAction,
    reason: Option<String>,
    live_control_token: Option<String>,
}

#[derive(Debug, Clone)]
struct SupervisorReconcileArgs {
    target_home: PathBuf,
    source_home: PathBuf,
    workspace: Option<PathBuf>,
    runtime_workspace: Option<PathBuf>,
    harness_cli: PathBuf,
    codex_exe: Option<PathBuf>,
    node_exe: PathBuf,
    gateway_script: PathBuf,
    agent_id: Option<String>,
    telegram_accounts: Vec<String>,
    discord_account: Option<String>,
    all: bool,
    include_runtime: Option<bool>,
    include_worker: Option<bool>,
    include_cron_scheduler: Option<bool>,
    include_progress: Option<bool>,
    include_telegram: Option<bool>,
    include_discord: Option<bool>,
    desired_services: Option<Vec<SupervisorInventoryServiceConfig>>,
    explicit_desired_input: bool,
    allow_default_apply: bool,
    apply: bool,
    dry_run: bool,
    iterations: usize,
    idle_ms: u64,
    restart_delay_ms: i64,
    default_heartbeat_timeout_ms: i64,
    max_consecutive_errors: usize,
    child_iterations: usize,
    runtime_concurrency: usize,
    timeout_ms: u64,
    idle_timeout_ms: u64,
    lane: Option<String>,
    worker_id: Option<String>,
    lease_ms: i64,
    poll_timeout_seconds: u64,
    max_updates: usize,
    outbox_limit: usize,
}

struct WorkerAdapterEnqueueArgs {
    source: AgentSource,
    target_home: PathBuf,
    resume_cron: bool,
    include_registered_cron: bool,
    allow_deterministic_run: bool,
    dry_run_shell: bool,
    resume_subagents: bool,
    master_agent_id: Option<String>,
    master_session_key: Option<String>,
    runtime_workspace: Option<PathBuf>,
    now_ms: i64,
}

#[derive(Debug, Clone)]
struct SimpleOptions {
    target_home: PathBuf,
    values: BTreeMap<String, Vec<String>>,
    flags: BTreeSet<String>,
}

impl SimpleOptions {
    fn parse(
        args: &[String],
        command: &str,
        value_options: &[&str],
        flag_options: &[&str],
    ) -> Result<Self, String> {
        let value_options: BTreeSet<&str> = value_options.iter().copied().collect();
        let flag_options: BTreeSet<&str> = flag_options.iter().copied().collect();
        let mut options = Self {
            target_home: default_harness_home(),
            values: BTreeMap::new(),
            flags: BTreeSet::new(),
        };
        let mut i = 0;
        while i < args.len() {
            let flag = args[i].as_str();
            if is_harness_home_arg(flag) {
                i += 1;
                options.target_home = parse_harness_home_path(args, i, flag)?;
            } else if value_options.contains(flag) {
                i += 1;
                let value = required_arg(args, i, flag)?.to_string();
                options
                    .values
                    .entry(flag.to_string())
                    .or_default()
                    .push(value);
            } else if flag_options.contains(flag) {
                options.flags.insert(flag.to_string());
            } else {
                return Err(format!("{command}: unknown argument: {flag}"));
            }
            i += 1;
        }
        Ok(options)
    }

    fn optional(&self, flag: &str) -> Option<&str> {
        self.values
            .get(flag)
            .and_then(|values| values.last())
            .map(String::as_str)
    }

    fn required(&self, flag: &str) -> Result<String, String> {
        self.optional(flag)
            .map(ToString::to_string)
            .ok_or_else(|| format!("{flag} is required"))
    }

    fn values(&self, flag: &str) -> Vec<String> {
        self.values.get(flag).cloned().unwrap_or_default()
    }

    fn has_flag(&self, flag: &str) -> bool {
        self.flags.contains(flag)
    }

    fn optional_i64(&self, flag: &str) -> Result<Option<i64>, String> {
        self.optional(flag)
            .map(|value| parse_i64(value, flag))
            .transpose()
    }

    fn required_i64(&self, flag: &str) -> Result<i64, String> {
        parse_i64(&self.required(flag)?, flag)
    }

    fn optional_u64(&self, flag: &str) -> Result<Option<u64>, String> {
        self.optional(flag)
            .map(|value| parse_u64(value, flag))
            .transpose()
    }

    fn optional_usize(&self, flag: &str) -> Result<Option<usize>, String> {
        self.optional(flag)
            .map(|value| parse_usize(value, flag))
            .transpose()
    }

    fn required_usize(&self, flag: &str) -> Result<usize, String> {
        parse_usize(&self.required(flag)?, flag)
    }

    fn text_input(&self, inline_flag: &str, file_flag: &str) -> Result<String, String> {
        self.optional_text_input(inline_flag, file_flag)?
            .ok_or_else(|| format!("{inline_flag} or {file_flag} is required"))
    }

    fn optional_text_input(
        &self,
        inline_flag: &str,
        file_flag: &str,
    ) -> Result<Option<String>, String> {
        match (self.optional(inline_flag), self.optional(file_flag)) {
            (Some(_), Some(_)) => Err(format!(
                "{inline_flag} and {file_flag} are mutually exclusive"
            )),
            (Some(value), None) => Ok(Some(value.to_string())),
            (None, Some(path)) => read_text_file(path).map(Some),
            (None, None) => Ok(None),
        }
    }

    fn json_input(&self, inline_flag: &str, file_flag: &str) -> Result<serde_json::Value, String> {
        let text = self.text_input(inline_flag, file_flag)?;
        serde_json::from_str(&text)
            .map_err(|err| format!("{inline_flag} or {file_flag} must contain JSON: {err}"))
    }

    fn vault_file(&self) -> PathBuf {
        self.optional("--vault-file")
            .map(PathBuf::from)
            .unwrap_or_else(|| self.target_home.join("secrets").join("vault.json"))
    }
}

fn read_text_file(path: &str) -> Result<String, String> {
    fs::read_to_string(path).map_err(|err| format!("failed to read {path}: {err}"))
}

fn parse_bool_value(value: &str, flag: &str) -> Result<bool, String> {
    match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" | "passed" | "pass" => Ok(true),
        "0" | "false" | "no" | "off" | "failed" | "fail" => Ok(false),
        _ => Err(format!("{flag} requires true/false, got: {value}")),
    }
}

fn parse_memory_owner_shadow_kind(value: &str) -> Result<MemoryOwnerShadowKind, String> {
    match value {
        "recall" => Ok(MemoryOwnerShadowKind::Recall),
        "store" => Ok(MemoryOwnerShadowKind::Store),
        "capture" => Ok(MemoryOwnerShadowKind::Capture),
        "store-propose" | "store_propose" => Ok(MemoryOwnerShadowKind::StorePropose),
        other => Err(format!(
            "unknown memory owner shadow kind: {other}; expected recall, store, capture, or store-propose"
        )),
    }
}

fn parse_task_status(value: &str) -> Result<TaskStatus, String> {
    match value {
        "open" => Ok(TaskStatus::Open),
        "blocked" => Ok(TaskStatus::Blocked),
        "completed" => Ok(TaskStatus::Completed),
        "canceled" => Ok(TaskStatus::Canceled),
        other => Err(format!(
            "unknown task status: {other}; expected open, blocked, completed, or canceled"
        )),
    }
}

fn parse_operation_plan_item_status(value: &str) -> Result<OperationPlanItemStatus, String> {
    match value {
        "todo" => Ok(OperationPlanItemStatus::Todo),
        "ready" => Ok(OperationPlanItemStatus::Ready),
        "running" => Ok(OperationPlanItemStatus::Running),
        "review" => Ok(OperationPlanItemStatus::Review),
        "done" => Ok(OperationPlanItemStatus::Done),
        "blocked" => Ok(OperationPlanItemStatus::Blocked),
        "canceled" | "cancelled" => Ok(OperationPlanItemStatus::Canceled),
        other => Err(format!(
            "unknown operation plan item status: {other}; expected todo, ready, running, review, done, blocked, or canceled"
        )),
    }
}

fn cli_list_values(options: &SimpleOptions, flag: &str) -> Vec<String> {
    options
        .values(flag)
        .into_iter()
        .flat_map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|part| !part.is_empty())
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
        .collect()
}

fn vault_passphrase(options: &SimpleOptions) -> Result<String, String> {
    options
        .optional("--passphrase")
        .map(ToString::to_string)
        .or_else(|| env::var("AGENT_HARNESS_VAULT_PASSPHRASE").ok())
        .ok_or_else(|| "--passphrase or AGENT_HARNESS_VAULT_PASSPHRASE is required".to_string())
}

fn dry_run_args_from_args(args: &[String]) -> Result<DryRunArgs, String> {
    let mut home = default_source_home();
    let mut workspace = None;
    let mut target_home = default_harness_home();
    let mut conflict_policy = ConflictPolicy::Skip;
    let mut output_dir = None;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "--source-home" => {
                i += 1;
                home = args
                    .get(i)
                    .map(PathBuf::from)
                    .ok_or_else(|| "--source-home requires a path".to_string())?;
            }
            "--workspace" => {
                i += 1;
                workspace = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--workspace requires a path".to_string())?,
                );
            }
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--conflict" => {
                i += 1;
                conflict_policy = args
                    .get(i)
                    .ok_or_else(|| "--conflict requires skip, overwrite, or rename".to_string())
                    .and_then(|value| parse_conflict_policy(value))?;
            }
            "--output" => {
                i += 1;
                output_dir = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--output requires a path".to_string())?,
                );
            }
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    let source = match workspace {
        Some(workspace) => AgentSource::with_workspace(home, workspace),
        None => AgentSource::new(home),
    };

    Ok(DryRunArgs {
        source,
        target_home,
        conflict_policy,
        output_dir,
    })
}

fn execute_args_from_args(args: &[String]) -> Result<ExecuteArgs, String> {
    let mut home = default_source_home();
    let mut workspace = None;
    let mut target_home = default_harness_home();
    let mut conflict_policy = ConflictPolicy::Skip;
    let mut include_sensitive = false;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "--source-home" => {
                i += 1;
                home = args
                    .get(i)
                    .map(PathBuf::from)
                    .ok_or_else(|| "--source-home requires a path".to_string())?;
            }
            "--workspace" => {
                i += 1;
                workspace = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--workspace requires a path".to_string())?,
                );
            }
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--conflict" => {
                i += 1;
                conflict_policy = args
                    .get(i)
                    .ok_or_else(|| "--conflict requires skip, overwrite, or rename".to_string())
                    .and_then(|value| parse_conflict_policy(value))?;
            }
            "--include-sensitive" => include_sensitive = true,
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    let source = match workspace {
        Some(workspace) => AgentSource::with_workspace(home, workspace),
        None => AgentSource::new(home),
    };

    Ok(ExecuteArgs {
        source,
        target_home,
        conflict_policy,
        include_sensitive,
    })
}

fn channel_credentials_export_args_from_args(
    args: &[String],
) -> Result<ChannelCredentialsExportArgs, String> {
    let mut home = default_source_home();
    let mut workspace = None;
    let mut target_home = default_harness_home();
    let mut include_sensitive = false;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "--source-home" => {
                i += 1;
                home = args
                    .get(i)
                    .map(PathBuf::from)
                    .ok_or_else(|| "--source-home requires a path".to_string())?;
            }
            "--workspace" => {
                i += 1;
                workspace = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--workspace requires a path".to_string())?,
                );
            }
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--include-sensitive" => include_sensitive = true,
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    let source = match workspace {
        Some(workspace) => AgentSource::with_workspace(home, workspace),
        None => AgentSource::new(home),
    };

    Ok(ChannelCredentialsExportArgs {
        source,
        target_home,
        include_sensitive,
    })
}

fn registry_export_args_from_args(args: &[String]) -> Result<RegistryExportArgs, String> {
    let mut home = default_source_home();
    let mut workspace = None;
    let mut target_home = default_harness_home();
    let mut conflict_policy = ConflictPolicy::Skip;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "--source-home" => {
                i += 1;
                home = args
                    .get(i)
                    .map(PathBuf::from)
                    .ok_or_else(|| "--source-home requires a path".to_string())?;
            }
            "--workspace" => {
                i += 1;
                workspace = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--workspace requires a path".to_string())?,
                );
            }
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--conflict" => {
                i += 1;
                conflict_policy = args
                    .get(i)
                    .ok_or_else(|| "--conflict requires skip, overwrite, or rename".to_string())
                    .and_then(|value| parse_conflict_policy(value))?;
            }
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    let source = match workspace {
        Some(workspace) => AgentSource::with_workspace(home, workspace),
        None => AgentSource::new(home),
    };

    Ok(RegistryExportArgs {
        source,
        target_home,
        conflict_policy,
    })
}

fn enable_check_args_from_args(args: &[String]) -> Result<EnableCheckArgs, String> {
    let mut target_home = default_harness_home();
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    Ok(EnableCheckArgs { target_home })
}

fn harness_status_args_from_args(args: &[String]) -> Result<HarnessStatusArgs, String> {
    let mut target_home = default_harness_home();
    let mut json = false;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--source-home" | "--workspace" | "--runtime-workspace" => {
                i += 1;
                args.get(i)
                    .ok_or_else(|| format!("{} requires a path", args[i - 1]))?;
            }
            "--json" => json = true,
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    Ok(HarnessStatusArgs { target_home, json })
}

fn jsonl_repair_args_from_args(args: &[String]) -> Result<JsonlRepairArgs, String> {
    let mut path = None;
    let mut output = None;
    let mut invalid_output = None;
    let mut apply = false;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "--path" => {
                i += 1;
                path = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--path requires a JSONL file path".to_string())?,
                );
            }
            "--output" => {
                i += 1;
                output = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--output requires a path".to_string())?,
                );
            }
            "--invalid-output" => {
                i += 1;
                invalid_output = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--invalid-output requires a path".to_string())?,
                );
            }
            "--apply" => apply = true,
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    Ok(JsonlRepairArgs {
        path: path.ok_or_else(|| "jsonl-repair requires --path".to_string())?,
        output,
        invalid_output,
        apply,
    })
}

fn worker_enqueue_args_from_args(args: &[String]) -> Result<WorkerEnqueueArgs, String> {
    let mut target_home = default_harness_home();
    let mut kind = None;
    let mut lane = None;
    let mut payload = None;
    let mut idempotency_key = None;
    let mut parent_job_id = None;
    let mut job_group_id = None;
    let mut master_agent_id = None;
    let mut master_session_key = None;
    let mut wake_policy = None;
    let mut source = None;
    let mut priority = 0i64;
    let mut available_at_ms = None;
    let mut max_attempts = 3i64;
    let mut timeout_ms = None;
    let mut cascade_timeout_ms = None;
    let mut rate_key = None;
    let mut concurrency_group_key = None;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--kind" => {
                i += 1;
                kind = Some(
                    args.get(i)
                        .ok_or_else(|| "--kind requires a worker job kind".to_string())?
                        .parse::<WorkerJobKind>()?,
                );
            }
            "--lane" => {
                i += 1;
                lane = Some(required_arg(args, i, "--lane")?.to_string());
            }
            "--payload" => {
                i += 1;
                payload = Some(parse_json_arg(
                    required_arg(args, i, "--payload")?,
                    "--payload",
                )?);
            }
            "--payload-file" => {
                i += 1;
                payload = Some(read_json_arg_file(required_arg(
                    args,
                    i,
                    "--payload-file",
                )?)?);
            }
            "--idempotency-key" => {
                i += 1;
                idempotency_key = Some(required_arg(args, i, "--idempotency-key")?.to_string());
            }
            "--parent-job-id" => {
                i += 1;
                parent_job_id = Some(required_arg(args, i, "--parent-job-id")?.to_string());
            }
            "--job-group-id" => {
                i += 1;
                job_group_id = Some(required_arg(args, i, "--job-group-id")?.to_string());
            }
            "--master-agent" | "--master-agent-id" => {
                i += 1;
                master_agent_id = Some(required_arg(args, i, "--master-agent")?.to_string());
            }
            "--master-session" | "--master-session-key" => {
                i += 1;
                master_session_key = Some(required_arg(args, i, "--master-session")?.to_string());
            }
            "--wake-policy" => {
                i += 1;
                wake_policy = Some(parse_json_arg(
                    required_arg(args, i, "--wake-policy")?,
                    "--wake-policy",
                )?);
            }
            "--wake-policy-file" => {
                i += 1;
                wake_policy = Some(read_json_arg_file(required_arg(
                    args,
                    i,
                    "--wake-policy-file",
                )?)?);
            }
            "--source" => {
                i += 1;
                source = Some(required_arg(args, i, "--source")?.to_string());
            }
            "--priority" => {
                i += 1;
                priority = parse_i64(required_arg(args, i, "--priority")?, "--priority")?;
            }
            "--available-at-ms" => {
                i += 1;
                available_at_ms = Some(parse_i64(
                    required_arg(args, i, "--available-at-ms")?,
                    "--available-at-ms",
                )?);
            }
            "--max-attempts" => {
                i += 1;
                max_attempts =
                    parse_i64(required_arg(args, i, "--max-attempts")?, "--max-attempts")?;
            }
            "--timeout-ms" => {
                i += 1;
                timeout_ms = Some(parse_u64(
                    required_arg(args, i, "--timeout-ms")?,
                    "--timeout-ms",
                )?);
            }
            "--cascade-timeout-ms" => {
                i += 1;
                cascade_timeout_ms = Some(parse_u64(
                    required_arg(args, i, "--cascade-timeout-ms")?,
                    "--cascade-timeout-ms",
                )?);
            }
            "--rate-key" => {
                i += 1;
                rate_key = Some(required_arg(args, i, "--rate-key")?.to_string());
            }
            "--concurrency-group" | "--concurrency-group-key" => {
                i += 1;
                concurrency_group_key =
                    Some(required_arg(args, i, "--concurrency-group")?.to_string());
            }
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    Ok(WorkerEnqueueArgs {
        target_home,
        kind: kind.ok_or_else(|| "--kind is required".to_string())?,
        lane,
        payload: payload.unwrap_or_else(|| serde_json::json!({})),
        idempotency_key,
        parent_job_id,
        job_group_id,
        master_agent_id,
        master_session_key,
        wake_policy,
        source,
        priority,
        available_at_ms,
        max_attempts,
        timeout_ms,
        cascade_timeout_ms,
        rate_key,
        concurrency_group_key,
    })
}

fn worker_run_once_args_from_args(args: &[String]) -> Result<WorkerRunOnceArgs, String> {
    let mut target_home = default_harness_home();
    let mut lane = None;
    let mut worker_id = format!("worker-{}", std::process::id());
    let mut lease_ms = 300_000i64;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--lane" => {
                i += 1;
                lane = Some(required_arg(args, i, "--lane")?.to_string());
            }
            "--worker-id" => {
                i += 1;
                worker_id = required_arg(args, i, "--worker-id")?.to_string();
            }
            "--lease-ms" => {
                i += 1;
                lease_ms = parse_i64(required_arg(args, i, "--lease-ms")?, "--lease-ms")?;
            }
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    Ok(WorkerRunOnceArgs {
        target_home,
        lane,
        worker_id,
        lease_ms,
    })
}

fn worker_loop_args_from_args(args: &[String]) -> Result<WorkerLoopArgs, String> {
    let mut run_once = worker_run_once_args_from_args(&[])?;
    let mut iterations = 0usize;
    let mut idle_ms = 1_000u64;
    let mut max_consecutive_errors = 5usize;
    let mut stop_when_idle = false;
    let mut stop_file = None;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                run_once.target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--lane" => {
                i += 1;
                run_once.lane = Some(required_arg(args, i, "--lane")?.to_string());
            }
            "--worker-id" => {
                i += 1;
                run_once.worker_id = required_arg(args, i, "--worker-id")?.to_string();
            }
            "--lease-ms" => {
                i += 1;
                run_once.lease_ms = parse_i64(required_arg(args, i, "--lease-ms")?, "--lease-ms")?;
            }
            "--iterations" => {
                i += 1;
                iterations = parse_usize(required_arg(args, i, "--iterations")?, "--iterations")?;
            }
            "--idle-ms" => {
                i += 1;
                idle_ms = parse_u64(required_arg(args, i, "--idle-ms")?, "--idle-ms")?;
            }
            "--max-consecutive-errors" => {
                i += 1;
                max_consecutive_errors =
                    parse_limit(required_arg(args, i, "--max-consecutive-errors")?)?;
            }
            "--stop-when-idle" => stop_when_idle = true,
            "--stop-file" => {
                i += 1;
                stop_file = Some(PathBuf::from(required_arg(args, i, "--stop-file")?));
            }
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    Ok(WorkerLoopArgs {
        target_home: run_once.target_home,
        lane: run_once.lane,
        worker_id: run_once.worker_id,
        lease_ms: run_once.lease_ms,
        iterations,
        idle_ms,
        max_consecutive_errors,
        stop_when_idle,
        stop_file,
    })
}

fn worker_status_args_from_args(args: &[String]) -> Result<WorkerStatusArgs, String> {
    let mut target_home = default_harness_home();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--json" => {}
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }
    Ok(WorkerStatusArgs { target_home })
}

fn worker_cancel_args_from_args(args: &[String]) -> Result<WorkerCancelArgs, String> {
    let mut target_home = default_harness_home();
    let mut job_id = None;
    let mut reason = "operator requested cancellation".to_string();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--job-id" => {
                i += 1;
                job_id = Some(required_arg(args, i, "--job-id")?.to_string());
            }
            "--reason" => {
                i += 1;
                reason = required_arg(args, i, "--reason")?.to_string();
            }
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }
    Ok(WorkerCancelArgs {
        target_home,
        job_id: job_id.ok_or_else(|| "--job-id is required".to_string())?,
        reason,
    })
}

fn memory_search_args_from_args(args: &[String]) -> Result<MemorySearchArgs, String> {
    let mut target_home = default_harness_home();
    let mut query = None;
    let mut limit = 8usize;
    let mut max_file_bytes = 1_000_000u64;
    let mut json = false;
    let mut write_receipt = true;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--query" | "-q" => {
                i += 1;
                query = Some(
                    args.get(i)
                        .ok_or_else(|| "--query requires text".to_string())?
                        .clone(),
                );
            }
            "--limit" => {
                i += 1;
                limit = args
                    .get(i)
                    .ok_or_else(|| "--limit requires a positive integer".to_string())
                    .and_then(|value| parse_limit(value))?;
            }
            "--max-file-bytes" => {
                i += 1;
                max_file_bytes = args
                    .get(i)
                    .ok_or_else(|| "--max-file-bytes requires a positive integer".to_string())
                    .and_then(|value| parse_u64(value, "--max-file-bytes"))?;
            }
            "--json" => json = true,
            "--no-receipt" => write_receipt = false,
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    let query = query.ok_or_else(|| "--query is required".to_string())?;
    if query.trim().is_empty() {
        return Err("--query must not be empty".to_string());
    }

    Ok(MemorySearchArgs {
        target_home,
        query,
        limit,
        max_file_bytes,
        json,
        write_receipt,
    })
}

fn memory_credentials_export_args_from_args(
    args: &[String],
) -> Result<MemoryCredentialsExportArgs, String> {
    let mut source_home = default_source_home();
    let mut target_home = default_harness_home();
    let mut include_sensitive = false;
    let mut json = false;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "--source-home" => {
                i += 1;
                source_home = args
                    .get(i)
                    .map(PathBuf::from)
                    .ok_or_else(|| "--source-home requires a path".to_string())?;
            }
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--workspace" | "--runtime-workspace" => {
                i += 1;
                args.get(i)
                    .ok_or_else(|| format!("{} requires a path", args[i - 1]))?;
            }
            "--include-sensitive" => include_sensitive = true,
            "--json" => json = true,
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    Ok(MemoryCredentialsExportArgs {
        source_home,
        target_home,
        include_sensitive,
        json,
    })
}

fn memory_vector_search_args_from_args(args: &[String]) -> Result<MemoryVectorSearchArgs, String> {
    let mut target_home = default_harness_home();
    let mut query = None;
    let mut limit = 5usize;
    let mut json = false;
    let mut write_receipt = true;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--query" | "-q" => {
                i += 1;
                query = Some(
                    args.get(i)
                        .ok_or_else(|| "--query requires text".to_string())?
                        .clone(),
                );
            }
            "--limit" => {
                i += 1;
                limit = args
                    .get(i)
                    .ok_or_else(|| "--limit requires a positive integer".to_string())
                    .and_then(|value| parse_limit(value))?;
            }
            "--json" => json = true,
            "--no-receipt" => write_receipt = false,
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    let query = query.ok_or_else(|| "--query is required".to_string())?;
    if query.trim().is_empty() {
        return Err("--query must not be empty".to_string());
    }

    Ok(MemoryVectorSearchArgs {
        target_home,
        query,
        limit,
        json,
        write_receipt,
    })
}

fn memory_canvas_run_args_from_args(args: &[String]) -> Result<MemoryCanvasRunArgs, String> {
    let mut target_home = default_harness_home();
    let mut agent_id = None;
    let mut json = false;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--agent" | "--agent-id" => {
                i += 1;
                agent_id = Some(required_arg(args, i, "--agent")?.to_string());
            }
            "--json" => json = true,
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    Ok(MemoryCanvasRunArgs {
        target_home,
        agent_id,
        json,
    })
}

fn memory_embedding_backfill_args_from_args(
    args: &[String],
) -> Result<MemoryEmbeddingBackfillArgs, String> {
    let mut target_home = default_harness_home();
    let mut lane = MemoryEmbeddingBackfillLane::default();
    let mut model = "text-embedding-3-small".to_string();
    let mut vector_dimension = DEFAULT_MEMORY_BACKFILL_VECTOR_DIMENSION;
    let mut batch_size = DEFAULT_MEMORY_BACKFILL_BATCH_SIZE;
    let mut max_items = DEFAULT_MEMORY_BACKFILL_MAX_ITEMS;
    let mut rate_limit_per_minute = DEFAULT_MEMORY_BACKFILL_RATE_LIMIT_PER_MINUTE;
    let mut retry_cap = DEFAULT_MEMORY_BACKFILL_RETRY_CAP;
    let mut coverage_threshold_bps = DEFAULT_MEMORY_BACKFILL_COVERAGE_THRESHOLD_BPS;
    let mut json = false;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--lane" => {
                i += 1;
                lane = required_arg(args, i, "--lane")?.parse::<MemoryEmbeddingBackfillLane>()?;
            }
            "--model" | "--embedding-model" => {
                i += 1;
                model = required_arg(args, i, "--model")?.to_string();
            }
            "--vector-dimension" | "--dim" => {
                i += 1;
                vector_dimension = parse_i64(
                    required_arg(args, i, "--vector-dimension")?,
                    "--vector-dimension",
                )?;
            }
            "--batch-size" => {
                i += 1;
                batch_size = parse_limit(required_arg(args, i, "--batch-size")?)?;
            }
            "--max-items" => {
                i += 1;
                max_items = parse_limit(required_arg(args, i, "--max-items")?)?;
            }
            "--rate-limit-per-minute" => {
                i += 1;
                rate_limit_per_minute =
                    parse_limit(required_arg(args, i, "--rate-limit-per-minute")?)?;
            }
            "--retry-cap" => {
                i += 1;
                retry_cap = parse_limit(required_arg(args, i, "--retry-cap")?)?;
            }
            "--coverage-threshold-bps" => {
                i += 1;
                coverage_threshold_bps = parse_u64(
                    required_arg(args, i, "--coverage-threshold-bps")?,
                    "--coverage-threshold-bps",
                )?;
            }
            "--json" => json = true,
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    Ok(MemoryEmbeddingBackfillArgs {
        target_home,
        lane,
        model,
        vector_dimension,
        batch_size,
        max_items,
        rate_limit_per_minute,
        retry_cap,
        coverage_threshold_bps,
        json,
    })
}

fn memory_hook_args_from_args(args: &[String]) -> Result<MemoryHookArgs, String> {
    let mut target_home = default_harness_home();
    let mut hook = None;
    let mut agent_id = None;
    let mut session_key = None;
    let mut query = None;
    let mut prompt_bundle_json = None;
    let mut assistant_text = None;
    let mut success = true;
    let mut slot = None;
    let mut operation = None;
    let mut payload = serde_json::json!({});
    let mut now_ms = current_time_ms()?;
    let mut limit = 5usize;
    let mut max_file_bytes = 0u64;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--hook" => {
                i += 1;
                hook = Some(required_arg(args, i, "--hook")?.parse::<MemoryHookKind>()?);
            }
            "--agent" | "--agent-id" => {
                i += 1;
                agent_id = Some(required_arg(args, i, "--agent")?.to_string());
            }
            "--session" | "--session-key" => {
                i += 1;
                session_key = Some(required_arg(args, i, "--session")?.to_string());
            }
            "--query" | "-q" => {
                i += 1;
                query = Some(required_arg(args, i, "--query")?.to_string());
            }
            "--prompt-bundle-json" | "--prompt-bundle" => {
                i += 1;
                prompt_bundle_json = Some(PathBuf::from(required_arg(
                    args,
                    i,
                    "--prompt-bundle-json",
                )?));
            }
            "--assistant-message" | "--assistant-text" => {
                i += 1;
                assistant_text = Some(required_arg(args, i, "--assistant-message")?.to_string());
            }
            "--assistant-message-file" | "--assistant-text-file" => {
                i += 1;
                let path = required_arg(args, i, "--assistant-message-file")?;
                assistant_text =
                    Some(fs::read_to_string(path).map_err(|err| format!("{path}: {err}"))?);
            }
            "--failed" => success = false,
            "--success" => success = true,
            "--slot" => {
                i += 1;
                slot = Some(required_arg(args, i, "--slot")?.to_string());
            }
            "--operation" => {
                i += 1;
                operation = Some(required_arg(args, i, "--operation")?.to_string());
            }
            "--payload" => {
                i += 1;
                payload = parse_json_arg(required_arg(args, i, "--payload")?, "--payload")?;
            }
            "--payload-file" => {
                i += 1;
                payload = read_json_arg_file(required_arg(args, i, "--payload-file")?)?;
            }
            "--now-ms" => {
                i += 1;
                now_ms = parse_i64(required_arg(args, i, "--now-ms")?, "--now-ms")?;
            }
            "--limit" => {
                i += 1;
                limit = parse_limit(required_arg(args, i, "--limit")?)?;
            }
            "--max-file-bytes" => {
                i += 1;
                max_file_bytes = parse_u64(
                    required_arg(args, i, "--max-file-bytes")?,
                    "--max-file-bytes",
                )?;
            }
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    Ok(MemoryHookArgs {
        target_home,
        hook: hook.ok_or_else(|| "--hook is required".to_string())?,
        agent_id,
        session_key,
        query,
        prompt_bundle_json,
        assistant_text,
        success,
        slot,
        operation,
        payload,
        now_ms,
        limit,
        max_file_bytes,
    })
}

fn memory_service_status_args_from_args(
    args: &[String],
) -> Result<MemoryServiceStatusArgs, String> {
    let mut target_home = default_harness_home();
    let mut agent_id = None;
    let mut json = false;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--agent" | "--agent-id" => {
                i += 1;
                agent_id = Some(required_arg(args, i, "--agent")?.to_string());
            }
            "--json" => json = true,
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    Ok(MemoryServiceStatusArgs {
        target_home,
        agent_id,
        json,
    })
}

fn memory_service_recall_args_from_args(
    args: &[String],
) -> Result<MemoryServiceRecallArgs, String> {
    let mut target_home = default_harness_home();
    let mut agent_id = None;
    let mut query = None;
    let mut limit = 5usize;
    let mut max_file_bytes = 0u64;
    let mut json = false;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--agent" | "--agent-id" => {
                i += 1;
                agent_id = Some(required_arg(args, i, "--agent")?.to_string());
            }
            "--query" | "-q" => {
                i += 1;
                query = Some(required_arg(args, i, "--query")?.to_string());
            }
            "--limit" => {
                i += 1;
                limit = parse_limit(required_arg(args, i, "--limit")?)?;
            }
            "--max-file-bytes" => {
                i += 1;
                max_file_bytes = parse_u64(
                    required_arg(args, i, "--max-file-bytes")?,
                    "--max-file-bytes",
                )?;
            }
            "--json" => json = true,
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    let query = query.ok_or_else(|| "--query is required".to_string())?;
    if query.trim().is_empty() {
        return Err("--query must not be empty".to_string());
    }
    Ok(MemoryServiceRecallArgs {
        target_home,
        agent_id,
        query,
        limit,
        max_file_bytes,
        json,
    })
}

fn memory_read_path_smoke_args_from_args(
    args: &[String],
) -> Result<MemoryReadPathSmokeArgs, String> {
    let mut target_home = default_harness_home();
    let mut agent_id = None;
    let mut query = "operator memory smoke".to_string();
    let mut limit = 3usize;
    let mut json = false;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--agent" | "--agent-id" => {
                i += 1;
                agent_id = Some(required_arg(args, i, "--agent")?.to_string());
            }
            "--query" | "-q" => {
                i += 1;
                query = required_arg(args, i, "--query")?.to_string();
            }
            "--limit" => {
                i += 1;
                limit = parse_limit(required_arg(args, i, "--limit")?)?;
            }
            "--json" => json = true,
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    if query.trim().is_empty() {
        return Err("--query must not be empty".to_string());
    }
    Ok(MemoryReadPathSmokeArgs {
        target_home,
        agent_id,
        query,
        limit,
        json,
    })
}

fn memory_service_propose_args_from_args(
    args: &[String],
) -> Result<MemoryServiceProposeArgs, String> {
    let mut target_home = default_harness_home();
    let mut agent_id = None;
    let mut session_key = None;
    let mut text = None;
    let mut payload = serde_json::json!({});
    let mut json = false;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--agent" | "--agent-id" => {
                i += 1;
                agent_id = Some(required_arg(args, i, "--agent")?.to_string());
            }
            "--session" | "--session-key" => {
                i += 1;
                session_key = Some(required_arg(args, i, "--session")?.to_string());
            }
            "--text" => {
                i += 1;
                text = Some(required_arg(args, i, "--text")?.to_string());
            }
            "--text-file" => {
                i += 1;
                let path = required_arg(args, i, "--text-file")?;
                text = Some(fs::read_to_string(path).map_err(|err| format!("{path}: {err}"))?);
            }
            "--payload" => {
                i += 1;
                payload = parse_json_arg(required_arg(args, i, "--payload")?, "--payload")?;
            }
            "--payload-file" => {
                i += 1;
                payload = read_json_arg_file(required_arg(args, i, "--payload-file")?)?;
            }
            "--json" => json = true,
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    let text = text.ok_or_else(|| "--text or --text-file is required".to_string())?;
    Ok(MemoryServiceProposeArgs {
        target_home,
        agent_id,
        session_key,
        text,
        payload,
        json,
    })
}

fn memory_service_store_args_from_args(args: &[String]) -> Result<MemoryServiceStoreArgs, String> {
    let mut target_home = default_harness_home();
    let mut agent_id = None;
    let mut session_key = None;
    let mut text = None;
    let mut payload = serde_json::json!({});
    let mut approved = false;
    let mut json = false;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--agent" | "--agent-id" => {
                i += 1;
                agent_id = Some(required_arg(args, i, "--agent")?.to_string());
            }
            "--session" | "--session-key" => {
                i += 1;
                session_key = Some(required_arg(args, i, "--session")?.to_string());
            }
            "--text" => {
                i += 1;
                text = Some(required_arg(args, i, "--text")?.to_string());
            }
            "--text-file" => {
                i += 1;
                let path = required_arg(args, i, "--text-file")?;
                text = Some(fs::read_to_string(path).map_err(|err| format!("{path}: {err}"))?);
            }
            "--payload" => {
                i += 1;
                payload = parse_json_arg(required_arg(args, i, "--payload")?, "--payload")?;
            }
            "--payload-file" => {
                i += 1;
                payload = read_json_arg_file(required_arg(args, i, "--payload-file")?)?;
            }
            "--approved" => approved = true,
            "--json" => json = true,
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    let text = text.ok_or_else(|| "--text or --text-file is required".to_string())?;
    Ok(MemoryServiceStoreArgs {
        target_home,
        agent_id,
        session_key,
        text,
        payload,
        approved,
        json,
    })
}

fn ops_backup_args_from_args(args: &[String]) -> Result<OpsBackupArgs, String> {
    let mut target_home = default_harness_home();
    let mut label = None;
    let mut max_file_bytes = 64 * 1024 * 1024u64;
    let mut summary_only = false;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--label" => {
                i += 1;
                label = Some(required_arg(args, i, "--label")?.to_string());
            }
            "--max-file-bytes" => {
                i += 1;
                max_file_bytes = parse_u64(
                    required_arg(args, i, "--max-file-bytes")?,
                    "--max-file-bytes",
                )?;
            }
            "--summary-only" => {
                summary_only = true;
            }
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    Ok(OpsBackupArgs {
        target_home,
        label,
        max_file_bytes,
        summary_only,
    })
}

fn ops_cutover_request_args_from_args(args: &[String]) -> Result<OpsCutoverRequestArgs, String> {
    let mut target_home = default_harness_home();
    let mut action = LiveControlAction::Cutover;
    let mut summary = None;
    let mut candidate_binary = None;
    let mut staging_home = None;
    let mut test_notes = Vec::new();
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--action" => {
                i += 1;
                action = required_arg(args, i, "--action")?.parse::<LiveControlAction>()?;
            }
            "--summary" => {
                i += 1;
                summary = Some(required_arg(args, i, "--summary")?.to_string());
            }
            "--candidate-binary" => {
                i += 1;
                candidate_binary =
                    Some(PathBuf::from(required_arg(args, i, "--candidate-binary")?));
            }
            "--staging-home" => {
                i += 1;
                staging_home = Some(PathBuf::from(required_arg(args, i, "--staging-home")?));
            }
            "--test-note" => {
                i += 1;
                test_notes.push(required_arg(args, i, "--test-note")?.to_string());
            }
            value if !value.starts_with('-') => {
                action = value.parse::<LiveControlAction>()?;
            }
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    Ok(OpsCutoverRequestArgs {
        target_home,
        action,
        summary,
        candidate_binary,
        staging_home,
        test_notes,
    })
}

fn ops_cutover_approve_args_from_args(args: &[String]) -> Result<OpsCutoverApproveArgs, String> {
    let mut target_home = default_harness_home();
    let mut ticket_id = None;
    let mut action = LiveControlAction::Cutover;
    let mut issued_to = None;
    let mut ttl_seconds = None;
    let mut reason = None;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--ticket-id" | "--ticket" => {
                i += 1;
                ticket_id = Some(required_arg(args, i, "--ticket-id")?.to_string());
            }
            "--action" => {
                i += 1;
                action = required_arg(args, i, "--action")?.parse::<LiveControlAction>()?;
            }
            "--issued-to" | "--operator" => {
                i += 1;
                issued_to = Some(required_arg(args, i, "--issued-to")?.to_string());
            }
            "--ttl-seconds" => {
                i += 1;
                let value = parse_i64(required_arg(args, i, "--ttl-seconds")?, "--ttl-seconds")?;
                ttl_seconds = Some(value.max(1));
            }
            "--reason" => {
                i += 1;
                reason = Some(required_arg(args, i, "--reason")?.to_string());
            }
            value if !value.starts_with('-') && ticket_id.is_none() => {
                ticket_id = Some(value.to_string());
            }
            value if !value.starts_with('-') => {
                action = value.parse::<LiveControlAction>()?;
            }
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    Ok(OpsCutoverApproveArgs {
        target_home,
        ticket_id: ticket_id
            .ok_or_else(|| "ops-cutover-approve requires --ticket-id".to_string())?,
        action,
        issued_to,
        ttl_seconds,
        reason,
    })
}

fn ops_cutover_apply_args_from_args(args: &[String]) -> Result<OpsCutoverApplyArgs, String> {
    let mut target_home = default_harness_home();
    let mut ticket_id = None;
    let mut action = LiveControlAction::Cutover;
    let mut live_control_token = None;
    let mut note = None;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--ticket-id" | "--ticket" => {
                i += 1;
                ticket_id = Some(required_arg(args, i, "--ticket-id")?.to_string());
            }
            "--action" => {
                i += 1;
                action = required_arg(args, i, "--action")?.parse::<LiveControlAction>()?;
            }
            "--live-control-token" => {
                i += 1;
                live_control_token =
                    Some(required_arg(args, i, "--live-control-token")?.to_string());
            }
            "--note" => {
                i += 1;
                note = Some(required_arg(args, i, "--note")?.to_string());
            }
            value if !value.starts_with('-') && ticket_id.is_none() => {
                ticket_id = Some(value.to_string());
            }
            value if !value.starts_with('-') => {
                action = value.parse::<LiveControlAction>()?;
            }
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    Ok(OpsCutoverApplyArgs {
        target_home,
        ticket_id: ticket_id.ok_or_else(|| "ops-cutover-apply requires --ticket-id".to_string())?,
        action,
        live_control_token,
        note,
    })
}

fn ops_cutover_status_args_from_args(args: &[String]) -> Result<OpsCutoverStatusArgs, String> {
    let mut target_home = default_harness_home();
    let mut action = None;
    let mut live_control_token = None;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--action" => {
                i += 1;
                action = Some(required_arg(args, i, "--action")?.parse::<LiveControlAction>()?);
            }
            "--live-control-token" => {
                i += 1;
                live_control_token =
                    Some(required_arg(args, i, "--live-control-token")?.to_string());
            }
            value if !value.starts_with('-') && action.is_none() => {
                action = Some(value.parse::<LiveControlAction>()?);
            }
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    Ok(OpsCutoverStatusArgs {
        target_home,
        action,
        live_control_token,
    })
}

fn ops_cutover_receipt_args_from_args(args: &[String]) -> Result<OpsCutoverReceiptArgs, String> {
    let mut target_home = default_harness_home();
    let mut note = None;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--note" => {
                i += 1;
                note = Some(required_arg(args, i, "--note")?.to_string());
            }
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    Ok(OpsCutoverReceiptArgs { target_home, note })
}

fn ops_control_args_from_args(args: &[String]) -> Result<OpsControlArgs, String> {
    let mut target_home = default_harness_home();
    let mut action = None;
    let mut reason = None;
    let mut live_control_token = None;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--action" => {
                i += 1;
                action = Some(required_arg(args, i, "--action")?.parse::<OpsControlAction>()?);
            }
            "--reason" => {
                i += 1;
                reason = Some(required_arg(args, i, "--reason")?.to_string());
            }
            "--live-control-token" => {
                i += 1;
                live_control_token =
                    Some(required_arg(args, i, "--live-control-token")?.to_string());
            }
            value if !value.starts_with('-') && action.is_none() => {
                action = Some(value.parse::<OpsControlAction>()?);
            }
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    Ok(OpsControlArgs {
        target_home,
        action: action.ok_or_else(|| "ops-control requires stop, start, or status".to_string())?,
        reason,
        live_control_token,
    })
}

fn supervisor_reconcile_args_from_args(args: &[String]) -> Result<SupervisorReconcileArgs, String> {
    let options = SimpleOptions::parse(
        args,
        "supervisor-reconcile",
        &[
            "--source-home",
            "--workspace",
            "--runtime-workspace",
            "--harness-cli",
            "--codex-exe",
            "--node-exe",
            "--gateway-script",
            "--agent",
            "--telegram-account",
            "--discord-account",
            "--account",
            "--desired-services-json",
            "--desired-services-file",
            "--iterations",
            "--idle-ms",
            "--restart-delay-ms",
            "--heartbeat-timeout-ms",
            "--max-consecutive-errors",
            "--child-iterations",
            "--runtime-concurrency",
            "--timeout-ms",
            "--idle-timeout-ms",
            "--lane",
            "--worker-id",
            "--lease-ms",
            "--poll-timeout-seconds",
            "--max-updates",
            "--outbox-limit",
        ],
        &[
            "--apply",
            "--dry-run",
            "--all",
            "--include-runtime",
            "--no-runtime",
            "--include-worker",
            "--no-worker",
            "--include-cron-scheduler",
            "--no-cron-scheduler",
            "--include-progress",
            "--no-progress",
            "--include-telegram",
            "--no-telegram",
            "--include-discord",
            "--no-discord",
        ],
    )?;
    let config = read_harness_config_json(&options.target_home)?;
    let supervisor_config = config.get("supervisor");
    let supervisor_enabled = supervisor_config
        .and_then(|value| bool_child(value, "enabled"))
        .unwrap_or(false);
    let config_manage_all = supervisor_config
        .and_then(|value| bool_child(value, "manageAllLoops"))
        .unwrap_or(false);
    let desired_services = options
        .optional_text_input("--desired-services-json", "--desired-services-file")?
        .map(|text| {
            serde_json::from_str::<Vec<SupervisorInventoryServiceConfig>>(&text)
                .map_err(|err| format!("desired services JSON is invalid: {err}"))
        })
        .transpose()?;
    let explicit_desired_input = desired_services.is_some()
        || options.has_flag("--all")
        || options.values.contains_key("--desired-services-json")
        || options.values.contains_key("--desired-services-file");
    let include_runtime = include_bool_flag(&options, "--include-runtime", "--no-runtime")?;
    let include_worker = include_bool_flag(&options, "--include-worker", "--no-worker")?;
    let include_cron_scheduler =
        include_bool_flag(&options, "--include-cron-scheduler", "--no-cron-scheduler")?;
    let include_progress = include_bool_flag(&options, "--include-progress", "--no-progress")?;
    let include_telegram = include_bool_flag(&options, "--include-telegram", "--no-telegram")?;
    let include_discord = include_bool_flag(&options, "--include-discord", "--no-discord")?;
    let explicit_desired_input = explicit_desired_input
        || include_runtime.is_some()
        || include_worker.is_some()
        || include_cron_scheduler.is_some()
        || include_progress.is_some()
        || include_telegram.is_some()
        || include_discord.is_some();
    let source_home = options
        .optional("--source-home")
        .map(PathBuf::from)
        .unwrap_or_else(|| options.target_home.clone());
    let inferred_workspace =
        infer_supervisor_reconcile_workspace(&options.target_home, &source_home)?;
    let workspace = options
        .optional("--workspace")
        .map(PathBuf::from)
        .map(|path| absolute_path(&path))
        .transpose()?
        .or(inferred_workspace);
    let runtime_workspace = options
        .optional("--runtime-workspace")
        .map(PathBuf::from)
        .map(|path| absolute_path(&path))
        .transpose()?
        .or_else(|| workspace.clone());
    let restart_delay_ms = options
        .optional_i64("--restart-delay-ms")?
        .unwrap_or(60_000);
    if restart_delay_ms < 0 {
        return Err("--restart-delay-ms must be non-negative".to_string());
    }
    let default_heartbeat_timeout_ms = options
        .optional_i64("--heartbeat-timeout-ms")?
        .unwrap_or(120_000)
        .max(1);
    let idle_ms = options.optional_u64("--idle-ms")?.unwrap_or(1_000).max(1);
    let iterations = options.optional_usize("--iterations")?.unwrap_or(1);
    let max_consecutive_errors = options
        .optional_usize("--max-consecutive-errors")?
        .unwrap_or(5)
        .max(1);
    let runtime_concurrency = options
        .optional_usize("--runtime-concurrency")?
        .unwrap_or(12)
        .max(1);
    let timeout_ms = options
        .optional_u64("--timeout-ms")?
        .unwrap_or(DEFAULT_CODEX_TIMEOUT_MS);
    let idle_timeout_ms = options
        .optional_u64("--idle-timeout-ms")?
        .unwrap_or(DEFAULT_CODEX_IDLE_TIMEOUT_MS);
    let lease_ms = options.optional_i64("--lease-ms")?.unwrap_or(120_000);
    if lease_ms <= 0 {
        return Err("--lease-ms must be greater than zero".to_string());
    }
    let poll_timeout_seconds = options
        .optional_u64("--poll-timeout-seconds")?
        .unwrap_or(1)
        .max(1);
    let max_updates = options
        .optional_usize("--max-updates")?
        .unwrap_or(10)
        .max(1);
    let outbox_limit = options.optional_usize("--outbox-limit")?.unwrap_or(20);
    if outbox_limit == 0 {
        return Err("--outbox-limit must be greater than zero".to_string());
    }
    Ok(SupervisorReconcileArgs {
        target_home: options.target_home.clone(),
        source_home,
        workspace,
        runtime_workspace,
        harness_cli: options
            .optional("--harness-cli")
            .map(PathBuf::from)
            .unwrap_or_else(default_harness_cli),
        codex_exe: options.optional("--codex-exe").map(PathBuf::from),
        node_exe: options
            .optional("--node-exe")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("node")),
        gateway_script: options
            .optional("--gateway-script")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                PathBuf::from("tools")
                    .join("agent-discord-gateway")
                    .join("index.mjs")
            }),
        agent_id: options.optional("--agent").map(ToString::to_string),
        telegram_accounts: options.values("--telegram-account"),
        discord_account: options
            .optional("--discord-account")
            .or_else(|| options.optional("--account"))
            .map(ToString::to_string),
        all: options.has_flag("--all") || config_manage_all,
        include_runtime,
        include_worker,
        include_cron_scheduler,
        include_progress,
        include_telegram,
        include_discord,
        desired_services,
        explicit_desired_input,
        allow_default_apply: supervisor_enabled,
        apply: options.has_flag("--apply"),
        dry_run: options.has_flag("--dry-run") || !options.has_flag("--apply"),
        iterations,
        idle_ms,
        restart_delay_ms,
        default_heartbeat_timeout_ms,
        max_consecutive_errors,
        child_iterations: options.optional_usize("--child-iterations")?.unwrap_or(0),
        runtime_concurrency,
        timeout_ms,
        idle_timeout_ms,
        lane: options.optional("--lane").map(ToString::to_string),
        worker_id: options.optional("--worker-id").map(ToString::to_string),
        lease_ms,
        poll_timeout_seconds,
        max_updates,
        outbox_limit,
    })
}

fn infer_supervisor_reconcile_workspace(
    target_home: &Path,
    source_home: &Path,
) -> Result<Option<PathBuf>, String> {
    for home in [source_home, target_home] {
        let home = absolute_path(home)?;
        if home.file_name().and_then(|name| name.to_str()) == Some(".agent-harness") {
            return Ok(home.parent().map(Path::to_path_buf));
        }
    }
    Ok(None)
}

fn supervisor_reconcile_desired_services(
    args: &SupervisorReconcileArgs,
) -> Result<Vec<SupervisorInventoryServiceConfig>, String> {
    if let Some(services) = &args.desired_services {
        return Ok(services.clone());
    }
    let config = read_harness_config_json(&args.target_home)?;
    let supervisor = config.get("supervisor");
    let mut services: BTreeMap<String, SupervisorInventoryServiceConfig> = BTreeMap::new();

    if let Some(raw_services) = supervisor.and_then(|value| value.get("services")) {
        let parsed =
            serde_json::from_value::<Vec<SupervisorInventoryServiceConfig>>(raw_services.clone())
                .map_err(|err| format!("supervisor.services is invalid: {err}"))?;
        for service in parsed {
            services.insert(service.service_id.clone(), service);
        }
    }

    let all = args.all
        || supervisor
            .and_then(|value| bool_child(value, "manageAllLoops"))
            .unwrap_or(false);
    if supervisor_loop_enabled(args.include_runtime, supervisor, "runtimeLoop", all) {
        insert_supervisor_default_service(&mut services, args, "runtime-loop", None);
    }
    if supervisor_loop_enabled(args.include_worker, supervisor, "workerLoop", all) {
        insert_supervisor_default_service(&mut services, args, "worker-loop", None);
    }
    let cron_enabled = args.include_cron_scheduler.unwrap_or_else(|| {
        supervisor
            .and_then(|value| loop_object_enabled(value, "cronSchedulerLoop"))
            .or_else(|| {
                config
                    .get("cronScheduler")
                    .and_then(|value| bool_child(value, "enabled"))
            })
            .unwrap_or(all)
    });
    if cron_enabled {
        insert_supervisor_default_service(&mut services, args, "cron-scheduler-loop", None);
    }
    if supervisor_loop_enabled(
        args.include_progress,
        supervisor,
        "progressDeliveryLoop",
        all,
    ) {
        insert_supervisor_default_service(&mut services, args, "progress-delivery-loop", None);
    }
    if supervisor_loop_enabled(args.include_telegram, supervisor, "telegramLoop", all) {
        let accounts = if args.telegram_accounts.is_empty() {
            vec![None]
        } else {
            args.telegram_accounts
                .iter()
                .map(|account| Some(account.as_str()))
                .collect()
        };
        for account in accounts {
            let service_id = account
                .map(|account| format!("telegram-loop-{}", service_id_suffix(account)))
                .unwrap_or_else(|| "telegram-loop".to_string());
            insert_supervisor_default_service(&mut services, args, &service_id, account);
        }
    }
    if let Some(telegram_loops) = supervisor
        .and_then(|value| value.get("telegramLoops"))
        .and_then(serde_json::Value::as_array)
    {
        for entry in telegram_loops {
            if bool_child(entry, "enabled").unwrap_or(true) {
                let account = string_child(entry, "account")
                    .or_else(|| string_child(entry, "telegramAccount"));
                let service_id = string_child(entry, "serviceId").unwrap_or_else(|| {
                    account
                        .as_deref()
                        .map(|account| format!("telegram-loop-{}", service_id_suffix(account)))
                        .unwrap_or_else(|| "telegram-loop".to_string())
                });
                let mut service = supervisor_default_service(args, &service_id, account.as_deref());
                if let Some(agent) =
                    string_child(entry, "agent").or_else(|| string_child(entry, "agentId"))
                {
                    service.args.extend(["--agent".to_string(), agent]);
                }
                services.insert(service.service_id.clone(), service);
            }
        }
    }
    if supervisor_loop_enabled(args.include_discord, supervisor, "discordOutboxLoop", all) {
        insert_supervisor_default_service(&mut services, args, "discord-outbox-loop", None);
    }
    if supervisor_loop_enabled(args.include_discord, supervisor, "discordGatewayLoop", all) {
        insert_supervisor_default_service(&mut services, args, "discord-gateway-loop", None);
    }

    Ok(services.into_values().collect())
}

fn insert_supervisor_default_service(
    services: &mut BTreeMap<String, SupervisorInventoryServiceConfig>,
    args: &SupervisorReconcileArgs,
    service_id: &str,
    account: Option<&str>,
) {
    let service = supervisor_default_service(args, service_id, account);
    services.insert(service.service_id.clone(), service);
}

fn supervisor_default_service(
    args: &SupervisorReconcileArgs,
    service_id: &str,
    account: Option<&str>,
) -> SupervisorInventoryServiceConfig {
    let mut service_args = vec![
        "--source-home".to_string(),
        args.source_home.display().to_string(),
        "--harness-cli".to_string(),
        args.harness_cli.display().to_string(),
        "--max-consecutive-errors".to_string(),
        args.max_consecutive_errors.to_string(),
        "--child-iterations".to_string(),
        args.child_iterations.to_string(),
        "--idle-ms".to_string(),
        args.idle_ms.to_string(),
    ];
    let supervisor_stop_file = args
        .target_home
        .join("state")
        .join("supervisor")
        .join("stop")
        .join(format!("{service_id}.stop"));
    push_optional_path_arg(
        &mut service_args,
        "--stop-file",
        Some(&supervisor_stop_file),
    );
    push_optional_path_arg(&mut service_args, "--workspace", args.workspace.as_ref());
    push_optional_path_arg(
        &mut service_args,
        "--runtime-workspace",
        args.runtime_workspace.as_ref(),
    );
    push_optional_path_arg(&mut service_args, "--codex-exe", args.codex_exe.as_ref());
    match service_id {
        "runtime-loop" => {
            service_args.extend([
                "--runtime-concurrency".to_string(),
                args.runtime_concurrency.to_string(),
                "--timeout-ms".to_string(),
                args.timeout_ms.to_string(),
                "--idle-timeout-ms".to_string(),
                args.idle_timeout_ms.to_string(),
            ]);
        }
        "worker-loop" => {
            service_args.extend(["--lease-ms".to_string(), args.lease_ms.to_string()]);
            push_optional_string_arg(&mut service_args, "--lane", args.lane.as_deref());
            push_optional_string_arg(&mut service_args, "--worker-id", args.worker_id.as_deref());
        }
        "progress-delivery-loop" => {}
        "cron-scheduler-loop" => {}
        "discord-outbox-loop" => {
            service_args.extend(["--outbox-limit".to_string(), args.outbox_limit.to_string()]);
            push_optional_string_arg(
                &mut service_args,
                "--discord-account",
                args.discord_account.as_deref(),
            );
        }
        "discord-gateway-loop" => {
            service_args.extend([
                "--node-exe".to_string(),
                args.node_exe.display().to_string(),
                "--gateway-script".to_string(),
                args.gateway_script.display().to_string(),
            ]);
            push_optional_string_arg(&mut service_args, "--agent", args.agent_id.as_deref());
            push_optional_string_arg(
                &mut service_args,
                "--discord-account",
                args.discord_account.as_deref(),
            );
        }
        service if service == "telegram-loop" || service.starts_with("telegram-loop-") => {
            service_args.extend([
                "--poll-timeout-seconds".to_string(),
                args.poll_timeout_seconds.to_string(),
                "--max-updates".to_string(),
                args.max_updates.to_string(),
                "--outbox-limit".to_string(),
                args.outbox_limit.to_string(),
                "--timeout-ms".to_string(),
                args.timeout_ms.to_string(),
                "--idle-timeout-ms".to_string(),
                args.idle_timeout_ms.to_string(),
            ]);
            push_optional_string_arg(&mut service_args, "--agent", args.agent_id.as_deref());
            push_optional_string_arg(&mut service_args, "--telegram-account", account);
        }
        _ => {}
    }
    SupervisorInventoryServiceConfig {
        enabled: true,
        service_id: service_id.to_string(),
        service_kind: loop_service_kind(service_id).to_string(),
        args: service_args,
        priority: supervisor_service_priority(service_id).to_string(),
        restart_delay_ms: args.restart_delay_ms,
        heartbeat_timeout_ms: Some(args.default_heartbeat_timeout_ms),
    }
}

fn launch_supervisor_reconcile_commands(
    args: &SupervisorReconcileArgs,
    launch_commands: &[SupervisorLaunchCommand],
) -> Result<Vec<serde_json::Value>, String> {
    let mut launches = Vec::new();
    for launch in launch_commands {
        if launch.command.len() < 2 {
            return Err(format!(
                "launch command for {} is malformed",
                launch.service_id
            ));
        }
        let child_args = &launch.command[1..];
        let child = Command::new(&args.harness_cli)
            .args(child_args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|err| {
                format!(
                    "failed to launch supervisor-run for {} with {}: {err}",
                    launch.service_id,
                    args.harness_cli.display()
                )
            })?;
        launches.push(serde_json::json!({
            "serviceId": launch.service_id,
            "pid": child.id(),
            "command": launch.command,
        }));
    }
    Ok(launches)
}

fn supervisor_loop_enabled(
    cli_value: Option<bool>,
    supervisor: Option<&serde_json::Value>,
    key: &str,
    all: bool,
) -> bool {
    cli_value
        .or_else(|| supervisor.and_then(|value| loop_object_enabled(value, key)))
        .unwrap_or(all)
}

fn loop_object_enabled(value: &serde_json::Value, key: &str) -> Option<bool> {
    value
        .get(key)
        .and_then(|child| bool_child(child, "enabled"))
}

fn include_bool_flag(
    options: &SimpleOptions,
    include_flag: &str,
    exclude_flag: &str,
) -> Result<Option<bool>, String> {
    match (
        options.has_flag(include_flag),
        options.has_flag(exclude_flag),
    ) {
        (true, true) => Err(format!(
            "{include_flag} and {exclude_flag} are mutually exclusive"
        )),
        (true, false) => Ok(Some(true)),
        (false, true) => Ok(Some(false)),
        (false, false) => Ok(None),
    }
}

fn read_harness_config_json(harness_home: &Path) -> Result<serde_json::Value, String> {
    let config_file = harness_home.join("harness-config.json");
    match fs::read_to_string(&config_file) {
        Ok(text) => serde_json::from_str(&text)
            .map_err(|err| format!("invalid JSON in {}: {err}", config_file.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(serde_json::Value::Null),
        Err(error) => Err(format!("failed to read {}: {error}", config_file.display())),
    }
}

fn bool_child(value: &serde_json::Value, key: &str) -> Option<bool> {
    value.get(key).and_then(serde_json::Value::as_bool)
}

fn service_id_suffix(value: &str) -> String {
    let mut suffix = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    while suffix.contains("--") {
        suffix = suffix.replace("--", "-");
    }
    suffix.trim_matches('-').to_string()
}

fn loop_wake_sequence_file(harness_home: &Path, lane: &str) -> PathBuf {
    let mut lane_key = service_id_suffix(lane);
    if lane_key.is_empty() {
        lane_key = "loop".to_string();
    }
    harness_home
        .join("state")
        .join("wake")
        .join(format!("{lane_key}.json"))
}

fn read_loop_wake_sequence(harness_home: &Path, lane: &str) -> u64 {
    agent_harness_core::wake::read_wake_sequence(loop_wake_sequence_file(harness_home, lane))
        .unwrap_or(0)
}

fn wait_for_loop_wake_since(
    harness_home: &Path,
    lane: &str,
    previous_sequence: u64,
    timeout_ms: u64,
) {
    let sequence_file = loop_wake_sequence_file(harness_home, lane);
    let event_name = agent_harness_core::wake::wake_event_name(harness_home, lane);
    let _ = agent_harness_core::wake::wait_for_wake(
        sequence_file,
        event_name,
        previous_sequence,
        timeout_ms,
    );
}

fn worker_loop_wake_lane(lane: Option<&str>) -> String {
    match lane {
        Some(lane) => {
            let mut lane_key = service_id_suffix(lane);
            if lane_key.is_empty() {
                lane_key = "unknown".to_string();
            }
            format!("worker-{lane_key}")
        }
        None => "worker".to_string(),
    }
}

fn supervisor_run_args_from_args(args: &[String]) -> Result<SupervisorRunArgs, String> {
    let options = SimpleOptions::parse(
        args,
        "supervisor-run",
        &[
            "--source-home",
            "--workspace",
            "--runtime-workspace",
            "--service",
            "--harness-cli",
            "--codex-exe",
            "--node-exe",
            "--gateway-script",
            "--agent",
            "--telegram-account",
            "--loop-name",
            "--idle-ms",
            "--max-consecutive-errors",
            "--restart-delay-ms",
            "--max-restarts",
            "--child-iterations",
            "--runtime-concurrency",
            "--timeout-ms",
            "--idle-timeout-ms",
            "--max-prompt-file-bytes",
            "--max-skill-file-bytes",
            "--lane",
            "--worker-id",
            "--lease-ms",
            "--poll-timeout-seconds",
            "--max-updates",
            "--outbox-limit",
            "--discord-account",
            "--account",
            "--stop-file",
        ],
        &[],
    )?;
    let service = options.required("--service")?;
    if !supervisor_run_supported_service(&service) {
        return Err(format!(
            "supervisor-run does not support service `{service}`"
        ));
    }
    let max_consecutive_errors = options
        .optional_usize("--max-consecutive-errors")?
        .unwrap_or(5);
    if max_consecutive_errors == 0 {
        return Err("--max-consecutive-errors must be greater than zero".to_string());
    }
    let harness_cli = options
        .optional("--harness-cli")
        .map(PathBuf::from)
        .unwrap_or_else(default_harness_cli);
    let source_home = options
        .optional("--source-home")
        .map(PathBuf::from)
        .unwrap_or_else(|| options.target_home.clone());
    let workspace = options.optional("--workspace").map(PathBuf::from);
    let runtime_workspace = options.optional("--runtime-workspace").map(PathBuf::from);
    let codex_exe = options.optional("--codex-exe").map(PathBuf::from);
    let node_exe = options
        .optional("--node-exe")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("node"));
    let gateway_script = options
        .optional("--gateway-script")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from("tools")
                .join("agent-discord-gateway")
                .join("index.mjs")
        });
    let agent_id = options.optional("--agent").map(ToString::to_string);
    let telegram_account = options
        .optional("--telegram-account")
        .map(ToString::to_string);
    let loop_name = options.optional("--loop-name").map(ToString::to_string);
    let idle_ms = options.optional_u64("--idle-ms")?.unwrap_or(1_000);
    let restart_delay_ms = options
        .optional_u64("--restart-delay-ms")?
        .unwrap_or(60_000);
    let max_restarts = options.optional_usize("--max-restarts")?.unwrap_or(0);
    let child_iterations = options.optional_usize("--child-iterations")?.unwrap_or(0);
    let runtime_concurrency = options
        .optional_usize("--runtime-concurrency")?
        .unwrap_or(1)
        .max(1);
    let timeout_ms = options
        .optional_u64("--timeout-ms")?
        .unwrap_or(DEFAULT_CODEX_TIMEOUT_MS);
    let idle_timeout_ms = options
        .optional_u64("--idle-timeout-ms")?
        .unwrap_or(DEFAULT_CODEX_IDLE_TIMEOUT_MS);
    let max_prompt_file_bytes = options
        .optional_usize("--max-prompt-file-bytes")?
        .unwrap_or_else(|| PromptAssemblyOptions::default().max_prompt_file_bytes);
    let max_skill_file_bytes = options
        .optional_usize("--max-skill-file-bytes")?
        .unwrap_or_else(|| PromptAssemblyOptions::default().max_skill_file_bytes);
    let lane = options.optional("--lane").map(ToString::to_string);
    let worker_id = options.optional("--worker-id").map(ToString::to_string);
    let lease_ms = options.optional_i64("--lease-ms")?.unwrap_or(120_000);
    if lease_ms <= 0 {
        return Err("--lease-ms must be greater than zero".to_string());
    }
    let poll_timeout_seconds = options
        .optional_u64("--poll-timeout-seconds")?
        .unwrap_or(1)
        .max(1);
    let max_updates = options
        .optional_usize("--max-updates")?
        .unwrap_or(10)
        .max(1);
    let outbox_limit = options.optional_usize("--outbox-limit")?.unwrap_or(20);
    if outbox_limit == 0 {
        return Err("--outbox-limit must be greater than zero".to_string());
    }
    let discord_account = options
        .optional("--discord-account")
        .or_else(|| options.optional("--account"))
        .map(ToString::to_string);
    let stop_file = options.optional("--stop-file").map(PathBuf::from);
    Ok(SupervisorRunArgs {
        target_home: options.target_home,
        source_home,
        workspace,
        runtime_workspace,
        service,
        harness_cli,
        codex_exe,
        node_exe,
        gateway_script,
        agent_id,
        telegram_account,
        loop_name,
        idle_ms,
        max_consecutive_errors,
        restart_delay_ms,
        max_restarts,
        child_iterations,
        runtime_concurrency,
        timeout_ms,
        idle_timeout_ms,
        max_prompt_file_bytes,
        max_skill_file_bytes,
        lane,
        worker_id,
        lease_ms,
        poll_timeout_seconds,
        max_updates,
        outbox_limit,
        discord_account,
        stop_file,
    })
}

fn supervisor_plan_args_from_args(args: &[String]) -> Result<SupervisorPlanArgs, String> {
    let mut target_home = default_harness_home();
    let mut source_home = None;
    let mut workspace = None;
    let mut runtime_workspace = None;
    let mut harness_cli = default_harness_cli();
    let mut codex_exe = None;
    let mut node_exe = PathBuf::from("node");
    let mut gateway_script = PathBuf::from("tools")
        .join("agent-discord-gateway")
        .join("index.mjs");
    let mut agent_id = None;
    let mut output_dir = None;
    let mut task_prefix = "AgentHarness".to_string();
    let mut include_runtime = true;
    let mut runtime_workers = 1usize;
    let mut include_worker = true;
    let mut include_cron_scheduler = false;
    let mut include_progress = true;
    let mut include_telegram = true;
    let mut include_discord = true;
    let mut idle_ms = 1_000;
    let mut runtime_timeout_ms = DEFAULT_CODEX_TIMEOUT_MS;
    let mut runtime_idle_timeout_ms = DEFAULT_CODEX_IDLE_TIMEOUT_MS;
    let mut max_consecutive_errors = 5;
    let mut telegram_poll_timeout_seconds = 1;
    let mut telegram_max_updates = 10;
    let mut telegram_outbox_limit = 20;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "--source-home" => {
                i += 1;
                source_home = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--source-home requires a path".to_string())?,
                );
            }
            "--workspace" => {
                i += 1;
                workspace = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--workspace requires a path".to_string())?,
                );
            }
            "--runtime-workspace" => {
                i += 1;
                runtime_workspace = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--runtime-workspace requires a path".to_string())?,
                );
            }
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--harness-cli" => {
                i += 1;
                harness_cli = args
                    .get(i)
                    .map(PathBuf::from)
                    .ok_or_else(|| "--harness-cli requires a path".to_string())?;
            }
            "--codex-exe" => {
                i += 1;
                codex_exe = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--codex-exe requires a path".to_string())?,
                );
            }
            "--node-exe" => {
                i += 1;
                node_exe = args
                    .get(i)
                    .map(PathBuf::from)
                    .ok_or_else(|| "--node-exe requires a path".to_string())?;
            }
            "--gateway-script" => {
                i += 1;
                gateway_script = args
                    .get(i)
                    .map(PathBuf::from)
                    .ok_or_else(|| "--gateway-script requires a path".to_string())?;
            }
            "--agent" => {
                i += 1;
                agent_id = Some(
                    args.get(i)
                        .cloned()
                        .ok_or_else(|| "--agent requires an id".to_string())?,
                );
            }
            "--output" => {
                i += 1;
                output_dir = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--output requires a path".to_string())?,
                );
            }
            "--task-prefix" => {
                i += 1;
                task_prefix = args
                    .get(i)
                    .cloned()
                    .ok_or_else(|| "--task-prefix requires a name".to_string())?;
            }
            "--no-runtime" => include_runtime = false,
            "--runtime-workers" => {
                i += 1;
                runtime_workers = args
                    .get(i)
                    .ok_or_else(|| "--runtime-workers requires a positive integer".to_string())
                    .and_then(|value| parse_limit(value))?
                    .max(1);
            }
            "--no-worker" => include_worker = false,
            "--include-cron-scheduler" => include_cron_scheduler = true,
            "--no-cron-scheduler" => include_cron_scheduler = false,
            "--no-progress" => include_progress = false,
            "--no-telegram" => include_telegram = false,
            "--no-discord" => include_discord = false,
            "--idle-ms" => {
                i += 1;
                idle_ms = args
                    .get(i)
                    .ok_or_else(|| "--idle-ms requires a positive integer".to_string())
                    .and_then(|value| parse_u64(value, "--idle-ms"))?;
            }
            "--runtime-timeout-ms" => {
                i += 1;
                runtime_timeout_ms = args
                    .get(i)
                    .ok_or_else(|| "--runtime-timeout-ms requires a positive integer".to_string())
                    .and_then(|value| parse_u64(value, "--runtime-timeout-ms"))?;
            }
            "--runtime-idle-timeout-ms" => {
                i += 1;
                runtime_idle_timeout_ms = args
                    .get(i)
                    .ok_or_else(|| {
                        "--runtime-idle-timeout-ms requires a positive integer".to_string()
                    })
                    .and_then(|value| parse_u64(value, "--runtime-idle-timeout-ms"))?;
            }
            "--max-consecutive-errors" => {
                i += 1;
                max_consecutive_errors = args
                    .get(i)
                    .ok_or_else(|| {
                        "--max-consecutive-errors requires a positive integer".to_string()
                    })
                    .and_then(|value| parse_limit(value))?;
            }
            "--poll-timeout-seconds" => {
                i += 1;
                telegram_poll_timeout_seconds = args
                    .get(i)
                    .ok_or_else(|| "--poll-timeout-seconds requires a positive integer".to_string())
                    .and_then(|value| parse_u64(value, "--poll-timeout-seconds"))?;
            }
            "--max-updates" => {
                i += 1;
                telegram_max_updates = args
                    .get(i)
                    .ok_or_else(|| "--max-updates requires a positive integer".to_string())
                    .and_then(|value| parse_limit(value))?;
            }
            "--outbox-limit" => {
                i += 1;
                telegram_outbox_limit = args
                    .get(i)
                    .ok_or_else(|| "--outbox-limit requires a positive integer".to_string())
                    .and_then(|value| parse_limit(value))?;
            }
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    let source_home = source_home.unwrap_or_else(|| target_home.clone());

    Ok(SupervisorPlanArgs {
        target_home,
        source_home,
        workspace,
        runtime_workspace,
        harness_cli,
        codex_exe,
        node_exe,
        gateway_script,
        agent_id,
        output_dir,
        task_prefix,
        include_runtime,
        runtime_workers,
        include_worker,
        include_cron_scheduler,
        include_progress,
        include_telegram,
        include_discord,
        idle_ms,
        runtime_timeout_ms,
        runtime_idle_timeout_ms,
        max_consecutive_errors,
        telegram_poll_timeout_seconds,
        telegram_max_updates,
        telegram_outbox_limit,
    })
}

fn harness_skills_sync_args_from_args(args: &[String]) -> Result<HarnessSkillsSyncArgs, String> {
    let mut target_home = default_harness_home();
    let mut force = false;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--force" => force = true,
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    Ok(HarnessSkillsSyncArgs { target_home, force })
}

fn skills_args_from_args(args: &[String]) -> Result<SkillsArgs, String> {
    let mut home = default_source_home();
    let mut workspace = None;
    let mut harness_home = None;
    let mut output_dir = None;
    let mut query = None;
    let mut agent_id = None;
    let mut channel = None;
    let mut match_workspace = None;
    let mut limit = 5;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "--source-home" => {
                i += 1;
                home = args
                    .get(i)
                    .map(PathBuf::from)
                    .ok_or_else(|| "--source-home requires a path".to_string())?;
            }
            "--workspace" => {
                i += 1;
                workspace = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--workspace requires a path".to_string())?,
                );
            }
            "--harness-home" => {
                i += 1;
                harness_home = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--harness-home requires a path".to_string())?,
                );
            }
            "--output" => {
                i += 1;
                output_dir = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--output requires a path".to_string())?,
                );
            }
            "--query" => {
                i += 1;
                query = Some(
                    args.get(i)
                        .cloned()
                        .ok_or_else(|| "--query requires text".to_string())?,
                );
            }
            "--agent" => {
                i += 1;
                agent_id = Some(
                    args.get(i)
                        .cloned()
                        .ok_or_else(|| "--agent requires an id".to_string())?,
                );
            }
            "--channel" => {
                i += 1;
                channel = Some(
                    args.get(i)
                        .cloned()
                        .ok_or_else(|| "--channel requires a name".to_string())?,
                );
            }
            "--match-workspace" => {
                i += 1;
                match_workspace = Some(
                    args.get(i)
                        .cloned()
                        .ok_or_else(|| "--match-workspace requires text".to_string())?,
                );
            }
            "--limit" => {
                i += 1;
                limit = args
                    .get(i)
                    .ok_or_else(|| "--limit requires a positive integer".to_string())
                    .and_then(|value| parse_limit(value))?;
            }
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    let source = match workspace {
        Some(workspace) => AgentSource::with_workspace(home, workspace),
        None => AgentSource::new(home),
    };

    Ok(SkillsArgs {
        source,
        harness_home,
        output_dir,
        query,
        agent_id,
        channel,
        match_workspace,
        limit,
    })
}

fn turn_plan_args_from_args(args: &[String]) -> Result<TurnPlanArgs, String> {
    let mut home = default_source_home();
    let mut workspace = None;
    let mut harness_home = None;
    let mut output_dir = None;
    let mut platform = "local".to_string();
    let mut channel_id = "local".to_string();
    let mut user_id = "operator".to_string();
    let mut agent_id = None;
    let mut session_key = None;
    let mut message = None;
    let mut skill_limit = 5;
    let mut max_prompt_file_bytes = PromptAssemblyOptions::default().max_prompt_file_bytes;
    let mut max_skill_file_bytes = PromptAssemblyOptions::default().max_skill_file_bytes;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "--source-home" => {
                i += 1;
                home = args
                    .get(i)
                    .map(PathBuf::from)
                    .ok_or_else(|| "--source-home requires a path".to_string())?;
            }
            "--workspace" => {
                i += 1;
                workspace = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--workspace requires a path".to_string())?,
                );
            }
            "--harness-home" => {
                i += 1;
                harness_home = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--harness-home requires a path".to_string())?,
                );
            }
            "--output" => {
                i += 1;
                output_dir = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--output requires a path".to_string())?,
                );
            }
            "--platform" => {
                i += 1;
                platform = args
                    .get(i)
                    .cloned()
                    .ok_or_else(|| "--platform requires a name".to_string())?;
            }
            "--channel-id" => {
                i += 1;
                channel_id = args
                    .get(i)
                    .cloned()
                    .ok_or_else(|| "--channel-id requires an id".to_string())?;
            }
            "--user-id" => {
                i += 1;
                user_id = args
                    .get(i)
                    .cloned()
                    .ok_or_else(|| "--user-id requires an id".to_string())?;
            }
            "--agent" => {
                i += 1;
                agent_id = Some(
                    args.get(i)
                        .cloned()
                        .ok_or_else(|| "--agent requires an id".to_string())?,
                );
            }
            "--session-key" => {
                i += 1;
                session_key = Some(
                    args.get(i)
                        .cloned()
                        .ok_or_else(|| "--session-key requires a value".to_string())?,
                );
            }
            "--message" => {
                i += 1;
                message = Some(
                    args.get(i)
                        .cloned()
                        .ok_or_else(|| "--message requires text".to_string())?,
                );
            }
            "--skill-limit" => {
                i += 1;
                skill_limit = args
                    .get(i)
                    .ok_or_else(|| "--skill-limit requires a positive integer".to_string())
                    .and_then(|value| parse_limit(value))?;
            }
            "--max-prompt-file-bytes" => {
                i += 1;
                max_prompt_file_bytes = args
                    .get(i)
                    .ok_or_else(|| {
                        "--max-prompt-file-bytes requires a positive integer".to_string()
                    })
                    .and_then(|value| parse_limit(value))?;
            }
            "--max-skill-file-bytes" => {
                i += 1;
                max_skill_file_bytes = args
                    .get(i)
                    .ok_or_else(|| "--max-skill-file-bytes requires a positive integer".to_string())
                    .and_then(|value| parse_limit(value))?;
            }
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    let source = match workspace {
        Some(workspace) => AgentSource::with_workspace(home, workspace),
        None => AgentSource::new(home),
    };

    Ok(TurnPlanArgs {
        source,
        harness_home,
        output_dir,
        platform,
        channel_id,
        user_id,
        agent_id,
        session_key,
        message: message.ok_or_else(|| "--message is required".to_string())?,
        skill_limit,
        max_prompt_file_bytes,
        max_skill_file_bytes,
    })
}

fn queue_enqueue_args_from_args(args: &[String]) -> Result<QueueEnqueueArgs, String> {
    let mut target_home = default_harness_home();
    let mut now_ms = None;
    let mut turn_args = Vec::new();
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--now-ms" => {
                i += 1;
                now_ms = Some(
                    args.get(i)
                        .ok_or_else(|| "--now-ms requires epoch milliseconds".to_string())
                        .and_then(|value| parse_i64(value, "--now-ms"))?,
                );
            }
            _ => turn_args.push(args[i].clone()),
        }
        i += 1;
    }

    let turn = turn_plan_args_from_args(&turn_args)?;
    let now_ms = match now_ms {
        Some(now_ms) => now_ms,
        None => current_time_ms()?,
    };

    Ok(QueueEnqueueArgs {
        turn,
        target_home,
        now_ms,
    })
}

fn channel_run_once_args_from_args(args: &[String]) -> Result<ChannelRunOnceArgs, String> {
    let mut target_home = default_harness_home();
    let mut runtime_workspace = None;
    let mut now_ms = None;
    let mut codex_exe = None;
    let mut timeout_ms = DEFAULT_CODEX_TIMEOUT_MS;
    let mut idle_timeout_ms = DEFAULT_CODEX_IDLE_TIMEOUT_MS;
    let mut outbox_limit = 20;
    let mut turn_args = Vec::new();
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--runtime-workspace" => {
                i += 1;
                runtime_workspace = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--runtime-workspace requires a path".to_string())?,
                );
            }
            "--now-ms" => {
                i += 1;
                now_ms = Some(
                    args.get(i)
                        .ok_or_else(|| "--now-ms requires epoch milliseconds".to_string())
                        .and_then(|value| parse_i64(value, "--now-ms"))?,
                );
            }
            "--codex-exe" => {
                i += 1;
                codex_exe = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--codex-exe requires a path".to_string())?,
                );
            }
            "--timeout-ms" => {
                i += 1;
                timeout_ms = args
                    .get(i)
                    .ok_or_else(|| "--timeout-ms requires a positive integer".to_string())
                    .and_then(|value| parse_u64(value, "--timeout-ms"))?;
            }
            "--idle-timeout-ms" => {
                i += 1;
                idle_timeout_ms = args
                    .get(i)
                    .ok_or_else(|| "--idle-timeout-ms requires a positive integer".to_string())
                    .and_then(|value| parse_u64(value, "--idle-timeout-ms"))?;
            }
            "--outbox-limit" => {
                i += 1;
                outbox_limit = args
                    .get(i)
                    .ok_or_else(|| "--outbox-limit requires a positive integer".to_string())
                    .and_then(|value| parse_limit(value))?;
            }
            _ => turn_args.push(args[i].clone()),
        }
        i += 1;
    }

    let turn = turn_plan_args_from_args(&turn_args)?;
    let now_ms = match now_ms {
        Some(now_ms) => now_ms,
        None => current_time_ms()?,
    };

    Ok(ChannelRunOnceArgs {
        turn,
        runtime_workspace,
        target_home,
        now_ms,
        codex_exe,
        timeout_ms,
        idle_timeout_ms,
        outbox_limit,
    })
}

fn channel_outbox_plan_args_from_args(args: &[String]) -> Result<ChannelOutboxPlanArgs, String> {
    let mut target_home = default_harness_home();
    let mut platform = None;
    let mut limit = 20;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--platform" => {
                i += 1;
                platform = Some(
                    args.get(i)
                        .cloned()
                        .ok_or_else(|| "--platform requires a name".to_string())?,
                );
            }
            "--limit" | "--outbox-limit" => {
                let flag = args[i].clone();
                i += 1;
                limit = args
                    .get(i)
                    .ok_or_else(|| format!("{flag} requires a positive integer"))
                    .and_then(|value| parse_limit(value))?;
            }
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    Ok(ChannelOutboxPlanArgs {
        target_home,
        platform,
        limit,
    })
}

fn channel_delivery_record_args_from_args(
    args: &[String],
) -> Result<ChannelDeliveryRecordArgs, String> {
    let mut target_home = default_harness_home();
    let mut delivery_id = None;
    let mut status = None;
    let mut platform = None;
    let mut channel_id = None;
    let mut user_id = None;
    let mut session_key = None;
    let mut provider_message_id = None;
    let mut error = None;
    let mut now_ms = None;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--delivery-id" => {
                i += 1;
                delivery_id = Some(
                    args.get(i)
                        .cloned()
                        .ok_or_else(|| "--delivery-id requires a value".to_string())?,
                );
            }
            "--status" => {
                i += 1;
                status = Some(
                    args.get(i)
                        .ok_or_else(|| "--status requires delivered or failed".to_string())
                        .and_then(|value| parse_delivery_status(value))?,
                );
            }
            "--platform" => {
                i += 1;
                platform = Some(
                    args.get(i)
                        .cloned()
                        .ok_or_else(|| "--platform requires a name".to_string())?,
                );
            }
            "--channel-id" => {
                i += 1;
                channel_id = Some(
                    args.get(i)
                        .cloned()
                        .ok_or_else(|| "--channel-id requires an id".to_string())?,
                );
            }
            "--user-id" => {
                i += 1;
                user_id = Some(
                    args.get(i)
                        .cloned()
                        .ok_or_else(|| "--user-id requires an id".to_string())?,
                );
            }
            "--session-key" => {
                i += 1;
                session_key = Some(
                    args.get(i)
                        .cloned()
                        .ok_or_else(|| "--session-key requires a value".to_string())?,
                );
            }
            "--provider-message-id" => {
                i += 1;
                provider_message_id = Some(
                    args.get(i)
                        .cloned()
                        .ok_or_else(|| "--provider-message-id requires a value".to_string())?,
                );
            }
            "--error" => {
                i += 1;
                error = Some(
                    args.get(i)
                        .cloned()
                        .ok_or_else(|| "--error requires text".to_string())?,
                );
            }
            "--now-ms" => {
                i += 1;
                now_ms = Some(
                    args.get(i)
                        .ok_or_else(|| "--now-ms requires epoch milliseconds".to_string())
                        .and_then(|value| parse_i64(value, "--now-ms"))?,
                );
            }
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    Ok(ChannelDeliveryRecordArgs {
        target_home,
        delivery_id: delivery_id.ok_or_else(|| "--delivery-id is required".to_string())?,
        status: status.ok_or_else(|| "--status is required".to_string())?,
        platform: platform.ok_or_else(|| "--platform is required".to_string())?,
        channel_id: channel_id.ok_or_else(|| "--channel-id is required".to_string())?,
        user_id: user_id.ok_or_else(|| "--user-id is required".to_string())?,
        session_key: session_key.ok_or_else(|| "--session-key is required".to_string())?,
        provider_message_id,
        error,
        now_ms: match now_ms {
            Some(now_ms) => now_ms,
            None => current_time_ms()?,
        },
    })
}

fn progress_delivery_once_args_from_args(
    args: &[String],
) -> Result<ProgressDeliveryOnceArgs, String> {
    let mut target_home = default_harness_home();
    let mut platform = None;
    let mut telegram_account = None;
    let mut min_update_interval_ms = 2_500_i64;
    let mut max_events_per_panel = 8usize;
    let mut max_preview_chars = 120usize;
    let mut current_step_max_chars = 1200usize;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--platform" => {
                i += 1;
                platform = Some(
                    args.get(i)
                        .cloned()
                        .ok_or_else(|| "--platform requires telegram or discord".to_string())?,
                );
            }
            "--telegram-account" => {
                i += 1;
                telegram_account = Some(
                    args.get(i)
                        .cloned()
                        .ok_or_else(|| "--telegram-account requires an account id".to_string())?,
                );
            }
            "--min-update-interval-ms" => {
                i += 1;
                min_update_interval_ms = args
                    .get(i)
                    .ok_or_else(|| {
                        "--min-update-interval-ms requires a non-negative integer".to_string()
                    })
                    .and_then(|value| parse_i64(value, "--min-update-interval-ms"))?;
                if min_update_interval_ms < 0 {
                    return Err("--min-update-interval-ms must be non-negative".to_string());
                }
            }
            "--max-events" => {
                i += 1;
                max_events_per_panel = args
                    .get(i)
                    .ok_or_else(|| "--max-events requires a positive integer".to_string())
                    .and_then(|value| parse_limit(value))?;
            }
            "--max-preview-chars" => {
                i += 1;
                max_preview_chars = args
                    .get(i)
                    .ok_or_else(|| "--max-preview-chars requires a positive integer".to_string())
                    .and_then(|value| parse_limit(value))?;
            }
            "--current-step-max-chars" => {
                i += 1;
                current_step_max_chars = args
                    .get(i)
                    .ok_or_else(|| {
                        "--current-step-max-chars requires a positive integer".to_string()
                    })
                    .and_then(|value| parse_limit(value))?;
            }
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    Ok(ProgressDeliveryOnceArgs {
        target_home,
        platform,
        telegram_account,
        min_update_interval_ms,
        max_events_per_panel,
        max_preview_chars,
        current_step_max_chars,
        preempt_after_wake_sequence: None,
    })
}

fn progress_delivery_loop_args_from_args(
    args: &[String],
) -> Result<ProgressDeliveryLoopArgs, String> {
    let mut send_args = Vec::new();
    let mut iterations = 0usize;
    let mut idle_ms = 1_000_u64;
    let mut max_consecutive_errors = 5usize;
    let mut stop_file = None;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "--iterations" => {
                i += 1;
                iterations = args
                    .get(i)
                    .ok_or_else(|| "--iterations requires a non-negative integer".to_string())
                    .and_then(|value| parse_usize(value, "--iterations"))?;
            }
            "--idle-ms" => {
                i += 1;
                idle_ms = args
                    .get(i)
                    .ok_or_else(|| "--idle-ms requires a positive integer".to_string())
                    .and_then(|value| parse_u64(value, "--idle-ms"))?;
            }
            "--max-consecutive-errors" => {
                i += 1;
                max_consecutive_errors = args
                    .get(i)
                    .ok_or_else(|| {
                        "--max-consecutive-errors requires a positive integer".to_string()
                    })
                    .and_then(|value| parse_limit(value))?;
            }
            "--stop-file" => {
                i += 1;
                stop_file = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--stop-file requires a path".to_string())?,
                );
            }
            flag => {
                send_args.push(flag.to_string());
                if option_takes_value(flag) {
                    i += 1;
                    send_args.push(
                        args.get(i)
                            .cloned()
                            .ok_or_else(|| format!("{flag} requires a value"))?,
                    );
                }
            }
        }
        i += 1;
    }

    Ok(ProgressDeliveryLoopArgs {
        send: progress_delivery_once_args_from_args(&send_args)?,
        iterations,
        idle_ms,
        max_consecutive_errors,
        stop_file,
    })
}

fn option_takes_value(flag: &str) -> bool {
    matches!(
        flag,
        "--target-home"
            | "--harness-home"
            | "--platform"
            | "--telegram-account"
            | "--min-update-interval-ms"
            | "--max-events"
            | "--max-preview-chars"
            | "--current-step-max-chars"
    )
}

fn telegram_probe_args_from_args(args: &[String]) -> Result<TelegramProbeArgs, String> {
    let mut target_home = default_harness_home();
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    Ok(TelegramProbeArgs { target_home })
}

fn telegram_poll_once_args_from_args(args: &[String]) -> Result<TelegramPollOnceArgs, String> {
    let mut home = default_source_home();
    let mut workspace = None;
    let mut runtime_workspace = None;
    let mut target_home = default_harness_home();
    let mut agent_id = None;
    let mut telegram_account = None;
    let mut skill_limit = 5;
    let mut codex_exe = None;
    let mut timeout_ms = DEFAULT_CODEX_TIMEOUT_MS;
    let mut idle_timeout_ms = DEFAULT_CODEX_IDLE_TIMEOUT_MS;
    let mut poll_timeout_seconds = 1;
    let mut max_updates = 10;
    let mut outbox_limit = 20;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "--source-home" => {
                i += 1;
                home = args
                    .get(i)
                    .map(PathBuf::from)
                    .ok_or_else(|| "--source-home requires a path".to_string())?;
            }
            "--workspace" => {
                i += 1;
                workspace = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--workspace requires a path".to_string())?,
                );
            }
            "--runtime-workspace" => {
                i += 1;
                runtime_workspace = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--runtime-workspace requires a path".to_string())?,
                );
            }
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--agent" => {
                i += 1;
                agent_id = Some(
                    args.get(i)
                        .cloned()
                        .ok_or_else(|| "--agent requires an id".to_string())?,
                );
            }
            "--telegram-account" => {
                i += 1;
                telegram_account = Some(
                    args.get(i)
                        .cloned()
                        .ok_or_else(|| "--telegram-account requires an account id".to_string())?,
                );
            }
            "--skill-limit" => {
                i += 1;
                skill_limit = args
                    .get(i)
                    .ok_or_else(|| "--skill-limit requires a positive integer".to_string())
                    .and_then(|value| parse_limit(value))?;
            }
            "--codex-exe" => {
                i += 1;
                codex_exe = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--codex-exe requires a path".to_string())?,
                );
            }
            "--timeout-ms" => {
                i += 1;
                timeout_ms = args
                    .get(i)
                    .ok_or_else(|| "--timeout-ms requires a positive integer".to_string())
                    .and_then(|value| parse_u64(value, "--timeout-ms"))?;
            }
            "--idle-timeout-ms" => {
                i += 1;
                idle_timeout_ms = args
                    .get(i)
                    .ok_or_else(|| "--idle-timeout-ms requires a positive integer".to_string())
                    .and_then(|value| parse_u64(value, "--idle-timeout-ms"))?;
            }
            "--poll-timeout-seconds" => {
                i += 1;
                poll_timeout_seconds = args
                    .get(i)
                    .ok_or_else(|| "--poll-timeout-seconds requires a positive integer".to_string())
                    .and_then(|value| parse_u64(value, "--poll-timeout-seconds"))?;
            }
            "--max-updates" => {
                i += 1;
                max_updates = args
                    .get(i)
                    .ok_or_else(|| "--max-updates requires a positive integer".to_string())
                    .and_then(|value| parse_limit(value))?;
            }
            "--outbox-limit" => {
                i += 1;
                outbox_limit = args
                    .get(i)
                    .ok_or_else(|| "--outbox-limit requires a positive integer".to_string())
                    .and_then(|value| parse_limit(value))?;
            }
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    let source = match workspace {
        Some(workspace) => AgentSource::with_workspace(home, workspace),
        None => AgentSource::new(home),
    };
    Ok(TelegramPollOnceArgs {
        source,
        runtime_workspace,
        target_home,
        agent_id,
        telegram_account,
        skill_limit,
        codex_exe,
        timeout_ms,
        idle_timeout_ms,
        poll_timeout_seconds,
        max_updates,
        outbox_limit,
    })
}

fn telegram_loop_args_from_args(args: &[String]) -> Result<TelegramLoopArgs, String> {
    let mut home = default_source_home();
    let mut workspace = None;
    let mut runtime_workspace = None;
    let mut target_home = default_harness_home();
    let mut agent_id = None;
    let mut telegram_account = None;
    let mut skill_limit = 5;
    let mut codex_exe = None;
    let mut timeout_ms = DEFAULT_CODEX_TIMEOUT_MS;
    let mut idle_timeout_ms = DEFAULT_CODEX_IDLE_TIMEOUT_MS;
    let mut poll_timeout_seconds = 1;
    let mut max_updates = 10;
    let mut outbox_limit = 20;
    let mut loop_name = "telegram-loop".to_string();
    let mut iterations = 0;
    let mut idle_ms = 1_000;
    let mut max_consecutive_errors = 5;
    let mut stop_file = None;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "--source-home" => {
                i += 1;
                home = args
                    .get(i)
                    .map(PathBuf::from)
                    .ok_or_else(|| "--source-home requires a path".to_string())?;
            }
            "--workspace" => {
                i += 1;
                workspace = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--workspace requires a path".to_string())?,
                );
            }
            "--runtime-workspace" => {
                i += 1;
                runtime_workspace = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--runtime-workspace requires a path".to_string())?,
                );
            }
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--agent" => {
                i += 1;
                agent_id = Some(
                    args.get(i)
                        .cloned()
                        .ok_or_else(|| "--agent requires an id".to_string())?,
                );
            }
            "--telegram-account" => {
                i += 1;
                telegram_account = Some(
                    args.get(i)
                        .cloned()
                        .ok_or_else(|| "--telegram-account requires an account id".to_string())?,
                );
            }
            "--skill-limit" => {
                i += 1;
                skill_limit = args
                    .get(i)
                    .ok_or_else(|| "--skill-limit requires a positive integer".to_string())
                    .and_then(|value| parse_limit(value))?;
            }
            "--codex-exe" => {
                i += 1;
                codex_exe = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--codex-exe requires a path".to_string())?,
                );
            }
            "--timeout-ms" => {
                i += 1;
                timeout_ms = args
                    .get(i)
                    .ok_or_else(|| "--timeout-ms requires a positive integer".to_string())
                    .and_then(|value| parse_u64(value, "--timeout-ms"))?;
            }
            "--idle-timeout-ms" => {
                i += 1;
                idle_timeout_ms = args
                    .get(i)
                    .ok_or_else(|| "--idle-timeout-ms requires a positive integer".to_string())
                    .and_then(|value| parse_u64(value, "--idle-timeout-ms"))?;
            }
            "--poll-timeout-seconds" => {
                i += 1;
                poll_timeout_seconds = args
                    .get(i)
                    .ok_or_else(|| "--poll-timeout-seconds requires a positive integer".to_string())
                    .and_then(|value| parse_u64(value, "--poll-timeout-seconds"))?;
            }
            "--max-updates" => {
                i += 1;
                max_updates = args
                    .get(i)
                    .ok_or_else(|| "--max-updates requires a positive integer".to_string())
                    .and_then(|value| parse_limit(value))?;
            }
            "--outbox-limit" => {
                i += 1;
                outbox_limit = args
                    .get(i)
                    .ok_or_else(|| "--outbox-limit requires a positive integer".to_string())
                    .and_then(|value| parse_limit(value))?;
            }
            "--loop-name" => {
                i += 1;
                loop_name = args
                    .get(i)
                    .cloned()
                    .ok_or_else(|| "--loop-name requires a name".to_string())?;
            }
            "--iterations" => {
                i += 1;
                iterations = args
                    .get(i)
                    .ok_or_else(|| "--iterations requires a non-negative integer".to_string())
                    .and_then(|value| parse_usize(value, "--iterations"))?;
            }
            "--idle-ms" => {
                i += 1;
                idle_ms = args
                    .get(i)
                    .ok_or_else(|| "--idle-ms requires a positive integer".to_string())
                    .and_then(|value| parse_u64(value, "--idle-ms"))?;
            }
            "--max-consecutive-errors" => {
                i += 1;
                max_consecutive_errors = args
                    .get(i)
                    .ok_or_else(|| {
                        "--max-consecutive-errors requires a positive integer".to_string()
                    })
                    .and_then(|value| parse_limit(value))?;
            }
            "--stop-file" => {
                i += 1;
                stop_file = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--stop-file requires a path".to_string())?,
                );
            }
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    let source = match workspace {
        Some(workspace) => AgentSource::with_workspace(home, workspace),
        None => AgentSource::new(home),
    };
    Ok(TelegramLoopArgs {
        poll: TelegramPollOnceArgs {
            source,
            runtime_workspace,
            target_home,
            agent_id,
            telegram_account,
            skill_limit,
            codex_exe,
            timeout_ms,
            idle_timeout_ms,
            poll_timeout_seconds,
            max_updates,
            outbox_limit,
        },
        loop_name,
        iterations,
        idle_ms,
        max_consecutive_errors,
        stop_file,
    })
}

fn discord_outbox_send_once_args_from_args(
    args: &[String],
) -> Result<DiscordOutboxSendOnceArgs, String> {
    let mut target_home = default_harness_home();
    let mut discord_account = None;
    let mut outbox_limit = 20;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--discord-account" | "--account" => {
                i += 1;
                discord_account = Some(
                    args.get(i)
                        .cloned()
                        .ok_or_else(|| "--discord-account requires an account id".to_string())?,
                );
            }
            "--outbox-limit" => {
                i += 1;
                outbox_limit = args
                    .get(i)
                    .ok_or_else(|| "--outbox-limit requires a positive integer".to_string())
                    .and_then(|value| parse_limit(value))?;
            }
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    Ok(DiscordOutboxSendOnceArgs {
        target_home,
        discord_account,
        outbox_limit,
    })
}

fn discord_outbox_loop_args_from_args(args: &[String]) -> Result<DiscordOutboxLoopArgs, String> {
    let mut target_home = default_harness_home();
    let mut discord_account = None;
    let mut outbox_limit = 20;
    let mut iterations = 0usize;
    let mut idle_ms = 1_000u64;
    let mut max_consecutive_errors = 5usize;
    let mut stop_file = None;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--discord-account" | "--account" => {
                i += 1;
                discord_account = Some(
                    args.get(i)
                        .cloned()
                        .ok_or_else(|| "--discord-account requires an account id".to_string())?,
                );
            }
            "--outbox-limit" => {
                i += 1;
                outbox_limit = args
                    .get(i)
                    .ok_or_else(|| "--outbox-limit requires a positive integer".to_string())
                    .and_then(|value| parse_limit(value))?;
            }
            "--iterations" => {
                i += 1;
                iterations = args
                    .get(i)
                    .ok_or_else(|| "--iterations requires a non-negative integer".to_string())
                    .and_then(|value| parse_usize(value, "--iterations"))?;
            }
            "--idle-ms" => {
                i += 1;
                idle_ms = args
                    .get(i)
                    .ok_or_else(|| "--idle-ms requires a positive integer".to_string())
                    .and_then(|value| parse_u64(value, "--idle-ms"))?;
            }
            "--max-consecutive-errors" => {
                i += 1;
                max_consecutive_errors = args
                    .get(i)
                    .ok_or_else(|| {
                        "--max-consecutive-errors requires a positive integer".to_string()
                    })
                    .and_then(|value| parse_limit(value))?;
            }
            "--stop-file" => {
                i += 1;
                stop_file = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--stop-file requires a path".to_string())?,
                );
            }
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    Ok(DiscordOutboxLoopArgs {
        send: DiscordOutboxSendOnceArgs {
            target_home,
            discord_account,
            outbox_limit,
        },
        iterations,
        idle_ms,
        max_consecutive_errors,
        stop_file,
    })
}

fn discord_dm_probe_args_from_args(args: &[String]) -> Result<DiscordDmProbeArgs, String> {
    let mut target_home = default_harness_home();
    let mut user_id = None;
    let mut send_message = true;
    let mut message = "Agent Harness Discord DM probe.".to_string();
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--user-id" => {
                i += 1;
                user_id = Some(
                    args.get(i)
                        .ok_or_else(|| "--user-id requires a Discord user id".to_string())?
                        .clone(),
                );
            }
            "--message" => {
                i += 1;
                message = args
                    .get(i)
                    .ok_or_else(|| "--message requires text".to_string())?
                    .clone();
            }
            "--no-send" => send_message = false,
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    let user_id = user_id.ok_or_else(|| "--user-id is required".to_string())?;
    if user_id.trim().is_empty() {
        return Err("--user-id must not be empty".to_string());
    }
    if message.chars().count() > 2_000 {
        return Err("--message exceeds Discord's 2000 character content limit".to_string());
    }

    Ok(DiscordDmProbeArgs {
        target_home,
        user_id,
        send_message,
        message,
    })
}

fn discord_dm_history_probe_args_from_args(
    args: &[String],
) -> Result<DiscordDmHistoryProbeArgs, String> {
    let mut target_home = default_harness_home();
    let mut channel_id = None;
    let mut user_id = None;
    let mut limit = 10usize;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--channel-id" => {
                i += 1;
                channel_id = Some(
                    args.get(i)
                        .ok_or_else(|| "--channel-id requires a Discord channel id".to_string())?
                        .clone(),
                );
            }
            "--user-id" => {
                i += 1;
                user_id = Some(
                    args.get(i)
                        .ok_or_else(|| "--user-id requires a Discord user id".to_string())?
                        .clone(),
                );
            }
            "--limit" => {
                i += 1;
                limit = args
                    .get(i)
                    .ok_or_else(|| "--limit requires a positive integer".to_string())
                    .and_then(|value| parse_limit(value))?;
            }
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    if let Some(channel_id) = &channel_id
        && channel_id.trim().is_empty()
    {
        return Err("--channel-id must not be empty".to_string());
    }
    if let Some(user_id) = &user_id
        && user_id.trim().is_empty()
    {
        return Err("--user-id must not be empty".to_string());
    }

    Ok(DiscordDmHistoryProbeArgs {
        target_home,
        channel_id,
        user_id,
        limit,
    })
}

fn discord_event_run_once_args_from_args(
    args: &[String],
) -> Result<DiscordEventRunOnceArgs, String> {
    let mut home = None;
    let mut workspace = None;
    let mut runtime_workspace = None;
    let mut target_home = default_harness_home();
    let mut agent_id = None;
    let mut discord_account = None;
    let mut skill_limit = 5;
    let mut codex_exe = None;
    let mut timeout_ms = DEFAULT_CODEX_TIMEOUT_MS;
    let mut idle_timeout_ms = DEFAULT_CODEX_IDLE_TIMEOUT_MS;
    let mut outbox_limit = 20;
    let mut event_file = None;
    let mut event_json = None;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "--source-home" => {
                i += 1;
                home = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--source-home requires a path".to_string())?,
                );
            }
            "--workspace" => {
                i += 1;
                workspace = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--workspace requires a path".to_string())?,
                );
            }
            "--runtime-workspace" => {
                i += 1;
                runtime_workspace = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--runtime-workspace requires a path".to_string())?,
                );
            }
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--agent" => {
                i += 1;
                agent_id = Some(
                    args.get(i)
                        .cloned()
                        .ok_or_else(|| "--agent requires an id".to_string())?,
                );
            }
            "--discord-account" | "--account" => {
                i += 1;
                discord_account = Some(
                    args.get(i)
                        .cloned()
                        .ok_or_else(|| "--discord-account requires an account id".to_string())?,
                );
            }
            "--skill-limit" => {
                i += 1;
                skill_limit = args
                    .get(i)
                    .ok_or_else(|| "--skill-limit requires a positive integer".to_string())
                    .and_then(|value| parse_limit(value))?;
            }
            "--codex-exe" => {
                i += 1;
                codex_exe = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--codex-exe requires a path".to_string())?,
                );
            }
            "--timeout-ms" => {
                i += 1;
                timeout_ms = args
                    .get(i)
                    .ok_or_else(|| "--timeout-ms requires a positive integer".to_string())
                    .and_then(|value| parse_u64(value, "--timeout-ms"))?;
            }
            "--idle-timeout-ms" => {
                i += 1;
                idle_timeout_ms = args
                    .get(i)
                    .ok_or_else(|| "--idle-timeout-ms requires a positive integer".to_string())
                    .and_then(|value| parse_u64(value, "--idle-timeout-ms"))?;
            }
            "--outbox-limit" => {
                i += 1;
                outbox_limit = args
                    .get(i)
                    .ok_or_else(|| "--outbox-limit requires a positive integer".to_string())
                    .and_then(|value| parse_limit(value))?;
            }
            "--event-file" => {
                i += 1;
                event_file = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--event-file requires a path".to_string())?,
                );
            }
            "--event-json" => {
                i += 1;
                event_json = Some(
                    args.get(i)
                        .cloned()
                        .ok_or_else(|| "--event-json requires JSON text".to_string())?,
                );
            }
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    if event_file.is_some() == event_json.is_some() {
        return Err("provide exactly one of --event-file or --event-json".to_string());
    }

    let home = home.unwrap_or_else(|| target_home.clone());
    let source = match workspace {
        Some(workspace) => AgentSource::with_workspace(home, workspace),
        None => AgentSource::new(home),
    };
    Ok(DiscordEventRunOnceArgs {
        source,
        runtime_workspace,
        target_home,
        agent_id,
        discord_account,
        skill_limit,
        codex_exe,
        timeout_ms,
        idle_timeout_ms,
        outbox_limit,
        event_file,
        event_json,
    })
}

fn discord_gateway_args_from_args(args: &[String]) -> Result<DiscordGatewayArgs, String> {
    let mut home = None;
    let mut workspace = None;
    let mut runtime_workspace = None;
    let mut target_home = default_harness_home();
    let mut node_exe = PathBuf::from("node");
    let mut gateway_script = PathBuf::from("tools")
        .join("agent-discord-gateway")
        .join("index.mjs");
    let mut harness_cli = default_harness_cli();
    let mut agent_id = None;
    let mut discord_account = None;
    let mut codex_exe = None;
    let mut max_messages = 0;
    let mut stop_file = None;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "--source-home" => {
                i += 1;
                home = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--source-home requires a path".to_string())?,
                );
            }
            "--workspace" => {
                i += 1;
                workspace = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--workspace requires a path".to_string())?,
                );
            }
            "--runtime-workspace" => {
                i += 1;
                runtime_workspace = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--runtime-workspace requires a path".to_string())?,
                );
            }
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--node-exe" => {
                i += 1;
                node_exe = args
                    .get(i)
                    .map(PathBuf::from)
                    .ok_or_else(|| "--node-exe requires a path".to_string())?;
            }
            "--gateway-script" => {
                i += 1;
                gateway_script = args
                    .get(i)
                    .map(PathBuf::from)
                    .ok_or_else(|| "--gateway-script requires a path".to_string())?;
            }
            "--harness-cli" => {
                i += 1;
                harness_cli = args
                    .get(i)
                    .map(PathBuf::from)
                    .ok_or_else(|| "--harness-cli requires a path".to_string())?;
            }
            "--agent" => {
                i += 1;
                agent_id = Some(
                    args.get(i)
                        .cloned()
                        .ok_or_else(|| "--agent requires an id".to_string())?,
                );
            }
            "--discord-account" | "--account" => {
                i += 1;
                discord_account = Some(
                    args.get(i)
                        .cloned()
                        .ok_or_else(|| "--discord-account requires an account id".to_string())?,
                );
            }
            "--codex-exe" => {
                i += 1;
                codex_exe = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--codex-exe requires a path".to_string())?,
                );
            }
            "--max-messages" => {
                i += 1;
                max_messages = args
                    .get(i)
                    .ok_or_else(|| "--max-messages requires a non-negative integer".to_string())
                    .and_then(|value| parse_usize(value, "--max-messages"))?;
            }
            "--stop-file" => {
                i += 1;
                stop_file = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--stop-file requires a path".to_string())?,
                );
            }
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    let home = home.unwrap_or_else(|| target_home.clone());
    let source = match workspace {
        Some(workspace) => AgentSource::with_workspace(home, workspace),
        None => AgentSource::new(home),
    };
    Ok(DiscordGatewayArgs {
        source,
        runtime_workspace,
        target_home,
        node_exe,
        gateway_script,
        harness_cli,
        agent_id,
        discord_account,
        codex_exe,
        max_messages,
        stop_file,
    })
}

fn plugin_sidecar_probe_args_from_args(args: &[String]) -> Result<PluginSidecarProbeArgs, String> {
    let mut target_home = default_harness_home();
    let mut node_exe = PathBuf::from("node");
    let mut sidecar_script = PathBuf::from("tools")
        .join("agent-plugin-sidecar")
        .join("index.mjs");
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--node-exe" => {
                i += 1;
                node_exe = args
                    .get(i)
                    .map(PathBuf::from)
                    .ok_or_else(|| "--node-exe requires a path".to_string())?;
            }
            "--sidecar-script" => {
                i += 1;
                sidecar_script = args
                    .get(i)
                    .map(PathBuf::from)
                    .ok_or_else(|| "--sidecar-script requires a path".to_string())?;
            }
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    let sidecar_script = resolve_sidecar_script_path(&target_home, &sidecar_script);
    Ok(PluginSidecarProbeArgs {
        target_home,
        node_exe,
        sidecar_script,
    })
}

fn plugin_sidecar_call_args_from_args(args: &[String]) -> Result<PluginSidecarCallArgs, String> {
    let mut target_home = default_harness_home();
    let mut node_exe = PathBuf::from("node");
    let mut sidecar_script = PathBuf::from("tools")
        .join("agent-plugin-sidecar")
        .join("index.mjs");
    let mut method = "sidecar.status".to_string();
    let mut params = serde_json::json!({});
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--node-exe" => {
                i += 1;
                node_exe = args
                    .get(i)
                    .map(PathBuf::from)
                    .ok_or_else(|| "--node-exe requires a path".to_string())?;
            }
            "--sidecar-script" => {
                i += 1;
                sidecar_script = args
                    .get(i)
                    .map(PathBuf::from)
                    .ok_or_else(|| "--sidecar-script requires a path".to_string())?;
            }
            "--method" => {
                i += 1;
                method = args
                    .get(i)
                    .cloned()
                    .ok_or_else(|| "--method requires a JSON-RPC method name".to_string())?;
            }
            "--params-json" => {
                i += 1;
                let text = args
                    .get(i)
                    .ok_or_else(|| "--params-json requires a JSON object".to_string())?;
                params = serde_json::from_str(text)
                    .map_err(|err| format!("--params-json must be valid JSON: {err}"))?;
                if !params.is_object() {
                    return Err("--params-json must be a JSON object".to_string());
                }
            }
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    let sidecar_script = resolve_sidecar_script_path(&target_home, &sidecar_script);
    Ok(PluginSidecarCallArgs {
        target_home,
        node_exe,
        sidecar_script,
        method,
        params,
    })
}

fn resolve_sidecar_script_path(target_home: &Path, sidecar_script: &Path) -> PathBuf {
    if sidecar_script.is_absolute() {
        return sidecar_script.to_path_buf();
    }
    let mut candidates = Vec::new();
    if let Some(parent) = target_home.parent() {
        candidates.push(parent.join(sidecar_script));
    }
    candidates.push(target_home.join("workspace").join(sidecar_script));
    if let Ok(cwd) = env::current_dir() {
        candidates.push(cwd.join(sidecar_script));
    }
    if let Ok(exe) = env::current_exe() {
        for ancestor in exe.ancestors().take(6) {
            candidates.push(ancestor.join(sidecar_script));
        }
    }
    candidates
        .into_iter()
        .find(|candidate| candidate.is_file())
        .unwrap_or_else(|| {
            target_home
                .parent()
                .map(|parent| parent.join(sidecar_script))
                .unwrap_or_else(|| sidecar_script.to_path_buf())
        })
}

fn queue_prepare_args_from_args(args: &[String]) -> Result<QueuePrepareArgs, String> {
    let mut target_home = default_harness_home();
    let mut queue_id = None;
    let mut max_prompt_file_bytes = PromptAssemblyOptions::default().max_prompt_file_bytes;
    let mut max_skill_file_bytes = PromptAssemblyOptions::default().max_skill_file_bytes;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--queue-id" => {
                i += 1;
                queue_id = Some(
                    args.get(i)
                        .cloned()
                        .ok_or_else(|| "--queue-id requires a value".to_string())?,
                );
            }
            "--max-prompt-file-bytes" => {
                i += 1;
                max_prompt_file_bytes = args
                    .get(i)
                    .ok_or_else(|| {
                        "--max-prompt-file-bytes requires a positive integer".to_string()
                    })
                    .and_then(|value| parse_limit(value))?;
            }
            "--max-skill-file-bytes" => {
                i += 1;
                max_skill_file_bytes = args
                    .get(i)
                    .ok_or_else(|| "--max-skill-file-bytes requires a positive integer".to_string())
                    .and_then(|value| parse_limit(value))?;
            }
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    Ok(QueuePrepareArgs {
        target_home,
        queue_id,
        max_prompt_file_bytes,
        max_skill_file_bytes,
    })
}

fn runtime_run_once_args_from_args(args: &[String]) -> Result<RuntimeRunOnceArgs, String> {
    let mut target_home = default_harness_home();
    let mut queue_id = None;
    let mut codex_exe = None;
    let mut timeout_ms = DEFAULT_CODEX_TIMEOUT_MS;
    let mut idle_timeout_ms = DEFAULT_CODEX_IDLE_TIMEOUT_MS;
    let mut max_prompt_file_bytes = PromptAssemblyOptions::default().max_prompt_file_bytes;
    let mut max_skill_file_bytes = PromptAssemblyOptions::default().max_skill_file_bytes;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--queue-id" => {
                i += 1;
                queue_id = Some(
                    args.get(i)
                        .cloned()
                        .ok_or_else(|| "--queue-id requires a value".to_string())?,
                );
            }
            "--codex-exe" => {
                i += 1;
                codex_exe = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--codex-exe requires a path".to_string())?,
                );
            }
            "--timeout-ms" => {
                i += 1;
                timeout_ms = args
                    .get(i)
                    .ok_or_else(|| "--timeout-ms requires a positive integer".to_string())
                    .and_then(|value| parse_u64(value, "--timeout-ms"))?;
            }
            "--idle-timeout-ms" => {
                i += 1;
                idle_timeout_ms = args
                    .get(i)
                    .ok_or_else(|| "--idle-timeout-ms requires a positive integer".to_string())
                    .and_then(|value| parse_u64(value, "--idle-timeout-ms"))?;
            }
            "--max-prompt-file-bytes" => {
                i += 1;
                max_prompt_file_bytes = args
                    .get(i)
                    .ok_or_else(|| {
                        "--max-prompt-file-bytes requires a positive integer".to_string()
                    })
                    .and_then(|value| parse_limit(value))?;
            }
            "--max-skill-file-bytes" => {
                i += 1;
                max_skill_file_bytes = args
                    .get(i)
                    .ok_or_else(|| "--max-skill-file-bytes requires a positive integer".to_string())
                    .and_then(|value| parse_limit(value))?;
            }
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    Ok(RuntimeRunOnceArgs {
        target_home,
        queue_id,
        codex_exe,
        timeout_ms,
        idle_timeout_ms,
        max_prompt_file_bytes,
        max_skill_file_bytes,
    })
}

fn runtime_loop_args_from_args(args: &[String]) -> Result<RuntimeLoopArgs, String> {
    let mut target_home = default_harness_home();
    let mut loop_name = "runtime-loop".to_string();
    let mut codex_exe = None;
    let mut timeout_ms = DEFAULT_CODEX_TIMEOUT_MS;
    let mut idle_timeout_ms = DEFAULT_CODEX_IDLE_TIMEOUT_MS;
    let mut max_prompt_file_bytes = PromptAssemblyOptions::default().max_prompt_file_bytes;
    let mut max_skill_file_bytes = PromptAssemblyOptions::default().max_skill_file_bytes;
    let mut runtime_concurrency = 1usize;
    let mut iterations = 0usize;
    let mut idle_ms = 1_000u64;
    let mut max_consecutive_errors = 5usize;
    let mut safe_mode_restart_ms = None;
    let mut safe_mode_restart_overridden = false;
    let mut stop_when_idle = false;
    let mut stop_file = None;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--loop-name" => {
                i += 1;
                loop_name = args
                    .get(i)
                    .cloned()
                    .ok_or_else(|| "--loop-name requires a name".to_string())?;
            }
            "--codex-exe" => {
                i += 1;
                codex_exe = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--codex-exe requires a path".to_string())?,
                );
            }
            "--timeout-ms" => {
                i += 1;
                timeout_ms = args
                    .get(i)
                    .ok_or_else(|| "--timeout-ms requires a positive integer".to_string())
                    .and_then(|value| parse_u64(value, "--timeout-ms"))?;
            }
            "--idle-timeout-ms" => {
                i += 1;
                idle_timeout_ms = args
                    .get(i)
                    .ok_or_else(|| "--idle-timeout-ms requires a positive integer".to_string())
                    .and_then(|value| parse_u64(value, "--idle-timeout-ms"))?;
            }
            "--max-prompt-file-bytes" => {
                i += 1;
                max_prompt_file_bytes = args
                    .get(i)
                    .ok_or_else(|| {
                        "--max-prompt-file-bytes requires a positive integer".to_string()
                    })
                    .and_then(|value| parse_limit(value))?;
            }
            "--max-skill-file-bytes" => {
                i += 1;
                max_skill_file_bytes = args
                    .get(i)
                    .ok_or_else(|| "--max-skill-file-bytes requires a positive integer".to_string())
                    .and_then(|value| parse_limit(value))?;
            }
            "--runtime-concurrency" | "--concurrency" => {
                let flag = args[i].clone();
                i += 1;
                runtime_concurrency = args
                    .get(i)
                    .ok_or_else(|| format!("{flag} requires a positive integer"))
                    .and_then(|value| parse_limit(value))?;
            }
            "--iterations" => {
                i += 1;
                iterations = args
                    .get(i)
                    .ok_or_else(|| "--iterations requires a non-negative integer".to_string())
                    .and_then(|value| parse_usize(value, "--iterations"))?;
            }
            "--idle-ms" => {
                i += 1;
                idle_ms = args
                    .get(i)
                    .ok_or_else(|| "--idle-ms requires a positive integer".to_string())
                    .and_then(|value| parse_u64(value, "--idle-ms"))?;
            }
            "--max-consecutive-errors" => {
                i += 1;
                max_consecutive_errors = args
                    .get(i)
                    .ok_or_else(|| {
                        "--max-consecutive-errors requires a positive integer".to_string()
                    })
                    .and_then(|value| parse_limit(value))?;
            }
            "--safe-mode-restart-ms" => {
                i += 1;
                safe_mode_restart_ms = Some(
                    args.get(i)
                        .ok_or_else(|| {
                            "--safe-mode-restart-ms requires a positive integer".to_string()
                        })
                        .and_then(|value| parse_u64(value, "--safe-mode-restart-ms"))?,
                );
                safe_mode_restart_overridden = true;
            }
            "--no-safe-mode-restart" => {
                safe_mode_restart_ms = None;
                safe_mode_restart_overridden = true;
            }
            "--stop-when-idle" => {
                stop_when_idle = true;
            }
            "--stop-file" => {
                i += 1;
                stop_file = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--stop-file requires a path".to_string())?,
                );
            }
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    if !safe_mode_restart_overridden && iterations == 0 && !stop_when_idle {
        safe_mode_restart_ms = Some(DEFAULT_RUNTIME_SAFE_MODE_RESTART_MS);
    }

    Ok(RuntimeLoopArgs {
        target_home,
        loop_name,
        codex_exe,
        timeout_ms,
        idle_timeout_ms,
        max_prompt_file_bytes,
        max_skill_file_bytes,
        runtime_concurrency,
        iterations,
        idle_ms,
        max_consecutive_errors,
        safe_mode_restart_ms,
        stop_when_idle,
        stop_file,
    })
}

fn codex_plan_args_from_args(args: &[String]) -> Result<CodexPlanArgs, String> {
    let mut target_home = default_harness_home();
    let mut execution_dir = None;
    let mut codex_exe = None;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--execution-dir" => {
                i += 1;
                execution_dir = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--execution-dir requires a path".to_string())?,
                );
            }
            "--codex-exe" => {
                i += 1;
                codex_exe = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--codex-exe requires a path".to_string())?,
                );
            }
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    Ok(CodexPlanArgs {
        target_home,
        execution_dir,
        codex_exe,
    })
}

fn codex_preflight_args_from_args(args: &[String]) -> Result<CodexPreflightArgs, String> {
    let mut target_home = default_harness_home();
    let mut execution_dir = None;
    let mut plan_file = None;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--execution-dir" => {
                i += 1;
                execution_dir = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--execution-dir requires a path".to_string())?,
                );
            }
            "--plan-file" => {
                i += 1;
                plan_file = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--plan-file requires a path".to_string())?,
                );
            }
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    Ok(CodexPreflightArgs {
        target_home,
        execution_dir,
        plan_file,
    })
}

fn codex_launch_probe_args_from_args(args: &[String]) -> Result<CodexLaunchProbeArgs, String> {
    let mut target_home = default_harness_home();
    let mut execution_dir = None;
    let mut plan_file = None;
    let mut startup_probe_ms = 750;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--execution-dir" => {
                i += 1;
                execution_dir = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--execution-dir requires a path".to_string())?,
                );
            }
            "--plan-file" => {
                i += 1;
                plan_file = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--plan-file requires a path".to_string())?,
                );
            }
            "--startup-probe-ms" => {
                i += 1;
                startup_probe_ms = args
                    .get(i)
                    .ok_or_else(|| "--startup-probe-ms requires a positive integer".to_string())
                    .and_then(|value| parse_u64(value, "--startup-probe-ms"))?;
            }
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    Ok(CodexLaunchProbeArgs {
        target_home,
        execution_dir,
        plan_file,
        startup_probe_ms,
    })
}

fn codex_run_args_from_args(args: &[String]) -> Result<CodexRunArgs, String> {
    let mut target_home = default_harness_home();
    let mut execution_dir = None;
    let mut plan_file = None;
    let mut timeout_ms = DEFAULT_CODEX_TIMEOUT_MS;
    let mut idle_timeout_ms = DEFAULT_CODEX_IDLE_TIMEOUT_MS;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--execution-dir" => {
                i += 1;
                execution_dir = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--execution-dir requires a path".to_string())?,
                );
            }
            "--plan-file" => {
                i += 1;
                plan_file = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--plan-file requires a path".to_string())?,
                );
            }
            "--timeout-ms" => {
                i += 1;
                timeout_ms = args
                    .get(i)
                    .ok_or_else(|| "--timeout-ms requires a positive integer".to_string())
                    .and_then(|value| parse_u64(value, "--timeout-ms"))?;
            }
            "--idle-timeout-ms" => {
                i += 1;
                idle_timeout_ms = args
                    .get(i)
                    .ok_or_else(|| "--idle-timeout-ms requires a positive integer".to_string())
                    .and_then(|value| parse_u64(value, "--idle-timeout-ms"))?;
            }
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    Ok(CodexRunArgs {
        target_home,
        execution_dir,
        plan_file,
        timeout_ms,
        idle_timeout_ms,
    })
}

fn codex_complete_args_from_args(args: &[String]) -> Result<CodexCompleteArgs, String> {
    let mut target_home = default_harness_home();
    let mut execution_dir = None;
    let mut plan_file = None;
    let mut assistant_message = None;
    let mut finished_at_ms = None;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--execution-dir" => {
                i += 1;
                execution_dir = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--execution-dir requires a path".to_string())?,
                );
            }
            "--plan-file" => {
                i += 1;
                plan_file = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--plan-file requires a path".to_string())?,
                );
            }
            "--assistant-message" => {
                i += 1;
                assistant_message = Some(
                    args.get(i)
                        .cloned()
                        .ok_or_else(|| "--assistant-message requires text".to_string())?,
                );
            }
            "--now-ms" => {
                i += 1;
                finished_at_ms = Some(
                    args.get(i)
                        .ok_or_else(|| "--now-ms requires epoch milliseconds".to_string())
                        .and_then(|value| parse_i64(value, "--now-ms"))?,
                );
            }
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    Ok(CodexCompleteArgs {
        target_home,
        execution_dir,
        plan_file,
        assistant_message: assistant_message
            .ok_or_else(|| "--assistant-message is required".to_string())?,
        finished_at_ms: match finished_at_ms {
            Some(now_ms) => now_ms,
            None => current_time_ms()?,
        },
    })
}

fn cron_plan_args_from_args(args: &[String]) -> Result<CronPlanArgs, String> {
    let mut home = default_source_home();
    let mut workspace = None;
    let mut output_dir = None;
    let mut now_ms = current_time_ms()?;
    let mut resume_cron = false;
    let mut limit = 20;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "--source-home" => {
                i += 1;
                home = args
                    .get(i)
                    .map(PathBuf::from)
                    .ok_or_else(|| "--source-home requires a path".to_string())?;
            }
            "--workspace" => {
                i += 1;
                workspace = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--workspace requires a path".to_string())?,
                );
            }
            "--output" => {
                i += 1;
                output_dir = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--output requires a path".to_string())?,
                );
            }
            "--now-ms" => {
                i += 1;
                now_ms = args
                    .get(i)
                    .ok_or_else(|| "--now-ms requires epoch milliseconds".to_string())
                    .and_then(|value| parse_i64(value, "--now-ms"))?;
            }
            "--resume-cron" => resume_cron = true,
            "--limit" => {
                i += 1;
                limit = args
                    .get(i)
                    .ok_or_else(|| "--limit requires a positive integer".to_string())
                    .and_then(|value| parse_limit(value))?;
            }
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    let source = match workspace {
        Some(workspace) => AgentSource::with_workspace(home, workspace),
        None => AgentSource::new(home),
    };

    Ok(CronPlanArgs {
        source,
        output_dir,
        now_ms,
        resume_cron,
        limit,
    })
}

fn worker_adapter_enqueue_args_from_args(
    args: &[String],
) -> Result<WorkerAdapterEnqueueArgs, String> {
    let mut home = default_source_home();
    let mut workspace = None;
    let mut target_home = default_harness_home();
    let mut runtime_workspace = None;
    let mut now_ms = current_time_ms()?;
    let mut resume_cron = false;
    let mut include_registered_cron = false;
    let mut allow_deterministic_run = false;
    let mut dry_run_shell = true;
    let mut resume_subagents = false;
    let mut master_agent_id = None;
    let mut master_session_key = None;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "--source-home" => {
                i += 1;
                home = args
                    .get(i)
                    .map(PathBuf::from)
                    .ok_or_else(|| "--source-home requires a path".to_string())?;
            }
            "--workspace" => {
                i += 1;
                workspace = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--workspace requires a path".to_string())?,
                );
            }
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--runtime-workspace" => {
                i += 1;
                runtime_workspace =
                    Some(PathBuf::from(required_arg(args, i, "--runtime-workspace")?));
            }
            "--now-ms" => {
                i += 1;
                now_ms = parse_i64(required_arg(args, i, "--now-ms")?, "--now-ms")?;
            }
            "--resume-cron" => resume_cron = true,
            "--include-registered-cron" => include_registered_cron = true,
            "--allow-deterministic-run" => allow_deterministic_run = true,
            "--dry-run-shell" => dry_run_shell = true,
            "--execute-shell" => dry_run_shell = false,
            "--resume-subagents" => resume_subagents = true,
            "--master-agent" | "--master-agent-id" => {
                i += 1;
                master_agent_id = Some(required_arg(args, i, "--master-agent")?.to_string());
            }
            "--master-session" | "--master-session-key" => {
                i += 1;
                master_session_key = Some(required_arg(args, i, "--master-session")?.to_string());
            }
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    let source = match workspace {
        Some(workspace) => AgentSource::with_workspace(home, workspace),
        None => AgentSource::new(home),
    };

    Ok(WorkerAdapterEnqueueArgs {
        source,
        target_home,
        resume_cron,
        include_registered_cron,
        allow_deterministic_run,
        dry_run_shell,
        resume_subagents,
        master_agent_id,
        master_session_key,
        runtime_workspace,
        now_ms,
    })
}

fn deterministic_cron_plan_args_from_args(
    args: &[String],
) -> Result<DeterministicCronPlanArgs, String> {
    let mut home = default_source_home();
    let mut workspace = None;
    let mut output_dir = None;
    let mut allow_deterministic_run = false;
    let mut limit = 20;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "--source-home" => {
                i += 1;
                home = args
                    .get(i)
                    .map(PathBuf::from)
                    .ok_or_else(|| "--source-home requires a path".to_string())?;
            }
            "--workspace" => {
                i += 1;
                workspace = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--workspace requires a path".to_string())?,
                );
            }
            "--output" => {
                i += 1;
                output_dir = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--output requires a path".to_string())?,
                );
            }
            "--allow-deterministic-run" => allow_deterministic_run = true,
            "--limit" => {
                i += 1;
                limit = args
                    .get(i)
                    .ok_or_else(|| "--limit requires a positive integer".to_string())
                    .and_then(|value| parse_limit(value))?;
            }
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    let source = match workspace {
        Some(workspace) => AgentSource::with_workspace(home, workspace),
        None => AgentSource::new(home),
    };

    Ok(DeterministicCronPlanArgs {
        source,
        output_dir,
        allow_deterministic_run,
        limit,
    })
}

fn subagent_plan_args_from_args(args: &[String]) -> Result<SubagentPlanArgs, String> {
    let mut home = default_source_home();
    let mut workspace = None;
    let mut output_dir = None;
    let mut resume_subagents = false;
    let mut limit = 20;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "--source-home" => {
                i += 1;
                home = args
                    .get(i)
                    .map(PathBuf::from)
                    .ok_or_else(|| "--source-home requires a path".to_string())?;
            }
            "--workspace" => {
                i += 1;
                workspace = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--workspace requires a path".to_string())?,
                );
            }
            "--output" => {
                i += 1;
                output_dir = Some(
                    args.get(i)
                        .map(PathBuf::from)
                        .ok_or_else(|| "--output requires a path".to_string())?,
                );
            }
            "--resume-subagents" => resume_subagents = true,
            "--limit" => {
                i += 1;
                limit = args
                    .get(i)
                    .ok_or_else(|| "--limit requires a positive integer".to_string())
                    .and_then(|value| parse_limit(value))?;
            }
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    let source = match workspace {
        Some(workspace) => AgentSource::with_workspace(home, workspace),
        None => AgentSource::new(home),
    };

    Ok(SubagentPlanArgs {
        source,
        output_dir,
        resume_subagents,
        limit,
    })
}

fn default_source_home() -> PathBuf {
    if let Ok(value) = env::var("AGENT_SOURCE_HOME") {
        return PathBuf::from(value);
    }

    if let Ok(value) = env::var("USERPROFILE") {
        return PathBuf::from(value).join(".openclaw");
    }

    PathBuf::from(".openclaw")
}

fn default_harness_home() -> PathBuf {
    if let Ok(value) = env::var("AGENT_HARNESS_HOME") {
        return PathBuf::from(value);
    }
    PathBuf::from(".agent-harness")
}

fn default_harness_cli() -> PathBuf {
    let exe = if cfg!(windows) {
        "agent-harness.exe"
    } else {
        "agent-harness"
    };
    PathBuf::from("target").join("debug").join(exe)
}

fn is_harness_home_arg(flag: &str) -> bool {
    matches!(flag, "--target-home" | "--harness-home")
}

fn parse_required_harness_home(args: &[String]) -> Result<PathBuf, String> {
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                return parse_harness_home_path(args, i, flag);
            }
            other => return Err(format!("unknown arg: {other}")),
        }
    }
    Err("--harness-home is required".to_string())
}

fn parse_harness_home_path(args: &[String], index: usize, flag: &str) -> Result<PathBuf, String> {
    args.get(index)
        .map(PathBuf::from)
        .ok_or_else(|| format!("{flag} requires a path"))
}

fn parse_skill_operation(value: &str) -> Result<SkillLearningProposalOperation, String> {
    match value {
        "create" => Ok(SkillLearningProposalOperation::Create),
        "patch" => Ok(SkillLearningProposalOperation::Patch),
        "replace" => Ok(SkillLearningProposalOperation::Replace),
        "archive" => Ok(SkillLearningProposalOperation::Archive),
        other => Err(format!("unsupported skill operation: {other}")),
    }
}

fn resolve_skill_target_path(harness_home: &Path, skill_id: &str) -> PathBuf {
    let suffix = skill_id
        .rsplit_once(':')
        .map(|(_, suffix)| suffix)
        .unwrap_or(skill_id);
    let candidates = [
        harness_home.join("skills"),
        harness_home.join("workspace").join("skills"),
        harness_home
            .join("workspace")
            .join(".agents")
            .join("skills"),
    ];
    for root in candidates {
        if let Some(path) = find_skill_file_by_dir_name(&root, suffix) {
            return path;
        }
    }
    harness_home.join("skills").join(suffix).join("SKILL.md")
}

fn find_skill_file_by_dir_name(root: &Path, suffix: &str) -> Option<PathBuf> {
    let entries = fs::read_dir(root).ok()?;
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if path
                .file_name()
                .and_then(|value| value.to_str())
                .is_some_and(|name| name.eq_ignore_ascii_case(suffix))
            {
                let skill_file = path.join("SKILL.md");
                if skill_file.is_file() {
                    return Some(skill_file);
                }
            }
            if let Some(found) = find_skill_file_by_dir_name(&path, suffix) {
                return Some(found);
            }
        }
    }
    None
}

fn write_plugin_sidecar_bridge_receipt(
    harness_home: &std::path::Path,
    method: &str,
    status: &str,
    response: &serde_json::Value,
    stderr: &str,
) -> Result<(), String> {
    let dir = harness_home.join("state").join("plugin-sidecar");
    fs::create_dir_all(&dir).map_err(|err| err.to_string())?;
    let receipts_file = dir.join("bridge-receipts.jsonl");
    let response_file = dir.join("last-bridge-response.json");
    fs::write(
        &response_file,
        serde_json::to_string_pretty(response).map_err(|err| err.to_string())?,
    )
    .map_err(|err| err.to_string())?;
    let reason = response
        .get("error")
        .and_then(|error| error.get("message"))
        .and_then(serde_json::Value::as_str)
        .or({
            if stderr.is_empty() {
                None
            } else {
                Some(stderr)
            }
        })
        .unwrap_or("plugin sidecar JSON-RPC call completed");
    let receipt = serde_json::json!({
        "schema": "agent-harness.plugin-sidecar-bridge-receipt.v1",
        "status": status,
        "method": method,
        "responseFile": response_file.display().to_string(),
        "reason": reason,
    });
    append_jsonl_value(&receipts_file, &receipt).map_err(|err| err.to_string())
}

fn read_discord_event_json(args: &DiscordEventRunOnceArgs) -> Result<serde_json::Value, String> {
    let text = match (&args.event_file, &args.event_json) {
        (Some(path), None) => fs::read_to_string(path).map_err(|err| err.to_string())?,
        (None, Some(text)) => text.clone(),
        _ => return Err("provide exactly one of --event-file or --event-json".to_string()),
    };
    serde_json::from_str(&text).map_err(|err| format!("invalid Discord event JSON: {err}"))
}

fn parse_discord_gateway_message(
    event: &serde_json::Value,
) -> Result<Option<DiscordGatewayMessage>, String> {
    if event
        .get("t")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|kind| kind != "MESSAGE_CREATE")
    {
        return Ok(None);
    }
    let payload = event.get("d").unwrap_or(event);
    let Some(message_id) = payload.get("id").and_then(serde_json::Value::as_str) else {
        return Ok(None);
    };
    let channel_id = payload
        .get("channel_id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "Discord MESSAGE_CREATE payload missing channel_id".to_string())?;
    let guild_id = payload
        .get("guild_id")
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string);
    let content = payload
        .get("content")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    let author = payload
        .get("author")
        .ok_or_else(|| "Discord MESSAGE_CREATE payload missing author".to_string())?;
    let user_id = author
        .get("id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "Discord MESSAGE_CREATE author missing id".to_string())?;
    let author_is_bot = author
        .get("bot")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let attachments = discord_attachment_metadata(payload);
    let reply_context = discord_reply_context(payload);
    let inbound_context = discord_inbound_context(payload);

    Ok(Some(DiscordGatewayMessage {
        message_id: message_id.to_string(),
        guild_id,
        channel_id: channel_id.to_string(),
        user_id: user_id.to_string(),
        content: content.to_string(),
        inbound_context,
        inbound_media_artifacts: Vec::new(),
        attachments,
        reply_context,
        author_is_bot,
    }))
}

fn discord_message_text(message: &DiscordGatewayMessage) -> String {
    if message.content.trim().is_empty() && message.inbound_context.is_some() {
        "[discord attachment message]".to_string()
    } else {
        message.content.clone()
    }
}

fn discord_inbound_context(payload: &serde_json::Value) -> Option<String> {
    let mut sections = Vec::new();
    if let Some(reply_context) = discord_reply_context(payload) {
        let mut lines = Vec::new();
        lines.push("## ReferencedMessage: Discord reply context".to_string());
        if let Some(message_id) = &reply_context.referenced_message_id {
            lines.push(format!("- referencedMessageId: {message_id}"));
        }
        if let Some(channel_id) = &reply_context.referenced_channel_id {
            lines.push(format!("- referencedChannelId: {channel_id}"));
        }
        if let Some(guild_id) = &reply_context.referenced_guild_id {
            lines.push(format!("- referencedGuildId: {guild_id}"));
        }
        if let Some(author_id) = &reply_context.referenced_author_id {
            lines.push(format!("- referencedAuthorId: {author_id}"));
        }
        lines.push(format!("- source: {}", reply_context.source));
        lines.push(format!(
            "- sourceAvailable: {}",
            reply_context.source_available
        ));
        if let Some(preview) = &reply_context.referenced_text_preview {
            lines.push(format!("- referencedTextPreview: {preview}"));
        }
        if let Some(length) = reply_context.referenced_preview_length {
            lines.push(format!("- referencedPreviewLength: {length}"));
        }
        if let Some(truncated) = reply_context.referenced_preview_truncated {
            lines.push(format!("- referencedPreviewTruncated: {truncated}"));
        }
        if let Some(text) = &reply_context.referenced_text {
            lines.push(format!("- referencedTextLength: {}", text.source_chars));
            lines.push(format!("- referencedTextTruncated: {}", text.truncated));
            lines.push(format!(
                "- referencedTextMaxChars: {}",
                REPLY_CONTEXT_FULL_TEXT_MAX_CHARS
            ));
            lines.push("- referencedTextSource: discord.referenced_message".to_string());
            push_indented_text_block(&mut lines, "referencedText", &text.text);
        } else {
            lines.push("- referencedTextAvailable: false".to_string());
        }
        lines.push(format!(
            "- referencedHasAttachments: {}",
            reply_context.has_attachments
        ));
        lines.push(format!(
            "- referencedAttachmentCount: {}",
            reply_context.attachment_count
        ));
        lines.push(format!(
            "- referencedEmbedsCount: {}",
            reply_context.embeds_count
        ));
        sections.push(lines.join("\n"));
    }

    let attachment_lines = discord_attachment_context_lines(payload);
    if !attachment_lines.is_empty() {
        sections.push(format!(
            "## InboundMedia: Discord attachments\n{}",
            attachment_lines.join("\n")
        ));
    }

    (!sections.is_empty()).then(|| sections.join("\n\n"))
}

fn discord_reply_context(payload: &serde_json::Value) -> Option<DiscordReplyContext> {
    let reference = payload.get("message_reference");
    let referenced = payload
        .get("referenced_message")
        .filter(|value| value.is_object());
    if reference.is_none() && referenced.is_none() {
        return None;
    }

    let referenced_message_id = reference
        .and_then(|reference| reference.get("message_id"))
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string);
    let referenced_channel_id = reference
        .and_then(|reference| reference.get("channel_id"))
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string);
    let referenced_guild_id = reference
        .and_then(|reference| reference.get("guild_id"))
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string);
    let referenced_author_id = referenced
        .and_then(|referenced| referenced.get("author"))
        .and_then(|author| author.get("id"))
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string);
    let content = referenced
        .and_then(|referenced| referenced.get("content"))
        .and_then(serde_json::Value::as_str)
        .filter(|content| !content.trim().is_empty());
    let preview = content.map(|content| {
        let preview = compact_preview_text(content, REPLY_CONTEXT_PREVIEW_MAX_CHARS);
        BoundedText {
            text: compact_preview_display(&preview),
            source_chars: preview.source_chars,
            truncated: preview.truncated,
        }
    });
    let referenced_text =
        content.map(|content| bounded_text(content, REPLY_CONTEXT_FULL_TEXT_MAX_CHARS));
    let attachment_count = referenced
        .and_then(|referenced| referenced.get("attachments"))
        .and_then(serde_json::Value::as_array)
        .map(Vec::len)
        .unwrap_or(0);
    let embeds_count = referenced
        .and_then(|referenced| referenced.get("embeds"))
        .and_then(serde_json::Value::as_array)
        .map(Vec::len)
        .unwrap_or(0);
    Some(DiscordReplyContext {
        referenced_message_id,
        referenced_channel_id,
        referenced_guild_id,
        referenced_author_id,
        referenced_text_preview: preview.as_ref().map(|preview| preview.text.clone()),
        referenced_preview_length: preview.as_ref().map(|preview| preview.text.chars().count()),
        referenced_preview_truncated: preview.as_ref().map(|preview| preview.truncated),
        referenced_text,
        has_attachments: attachment_count > 0,
        attachment_count,
        embeds_count,
        source: if referenced.is_some() {
            "discord.referenced_message".to_string()
        } else {
            "discord.message_reference".to_string()
        },
        source_available: referenced.is_some(),
    })
}

fn discord_attachment_context_lines(payload: &serde_json::Value) -> Vec<String> {
    let Some(attachments) = payload
        .get("attachments")
        .and_then(serde_json::Value::as_array)
    else {
        return Vec::new();
    };
    attachments
        .iter()
        .take(12)
        .enumerate()
        .map(|(index, attachment)| {
            let mut parts = vec![format!("index={index}")];
            push_json_string_preview_part(&mut parts, attachment, "id", 120);
            push_json_string_preview_part(&mut parts, attachment, "filename", 240);
            push_json_string_preview_part(&mut parts, attachment, "content_type", 160);
            push_json_i64_part(&mut parts, attachment, "size");
            push_json_i64_part(&mut parts, attachment, "width");
            push_json_i64_part(&mut parts, attachment, "height");
            parts.push(format!(
                "urlPresent={}",
                yes_no(attachment.get("url").is_some())
            ));
            format!("- {}", parts.join(" "))
        })
        .collect()
}

fn discord_attachment_metadata(payload: &serde_json::Value) -> Vec<DiscordAttachmentMetadata> {
    let Some(attachments) = payload
        .get("attachments")
        .and_then(serde_json::Value::as_array)
    else {
        return Vec::new();
    };
    attachments
        .iter()
        .take(12)
        .map(|attachment| DiscordAttachmentMetadata {
            id: attachment
                .get("id")
                .and_then(serde_json::Value::as_str)
                .map(ToString::to_string),
            filename: attachment
                .get("filename")
                .and_then(serde_json::Value::as_str)
                .map(ToString::to_string),
            content_type: attachment
                .get("content_type")
                .and_then(serde_json::Value::as_str)
                .map(ToString::to_string),
            size: attachment.get("size").and_then(serde_json::Value::as_u64),
            width: attachment
                .get("width")
                .and_then(serde_json::Value::as_u64)
                .and_then(|value| u32::try_from(value).ok()),
            height: attachment
                .get("height")
                .and_then(serde_json::Value::as_u64)
                .and_then(|value| u32::try_from(value).ok()),
            url: attachment
                .get("url")
                .and_then(serde_json::Value::as_str)
                .map(ToString::to_string),
        })
        .collect()
}

trait DiscordAttachmentFetcher {
    fn fetch_attachment(&self, url: &str, max_bytes: usize) -> Result<Vec<u8>, String>;
}

struct HttpDiscordAttachmentFetcher;

impl DiscordAttachmentFetcher for HttpDiscordAttachmentFetcher {
    fn fetch_attachment(&self, url: &str, max_bytes: usize) -> Result<Vec<u8>, String> {
        let response = channel_http_short_agent()
            .get(url)
            .call()
            .map_err(discord_http_error)?;
        read_response_with_limit(response, max_bytes)
    }
}

fn read_response_with_limit(response: ureq::Response, max_bytes: usize) -> Result<Vec<u8>, String> {
    let mut reader = response.into_reader().take((max_bytes + 1) as u64);
    let mut bytes = Vec::new();
    reader
        .read_to_end(&mut bytes)
        .map_err(|err| format!("Discord attachment response could not be read: {err}"))?;
    if bytes.len() > max_bytes {
        return Err(format!(
            "Discord attachment exceeded maxBytesPerItem={max_bytes}"
        ));
    }
    Ok(bytes)
}

fn attach_discord_message_artifacts<F: DiscordAttachmentFetcher>(
    harness_home: &Path,
    message: &mut DiscordGatewayMessage,
    fetcher: &F,
) -> Result<(), String> {
    let mut artifacts = Vec::new();
    for (index, attachment) in message.attachments.iter().enumerate() {
        artifacts.push(discord_attachment_artifact(
            harness_home,
            &message.message_id,
            index,
            attachment,
            fetcher,
        )?);
    }
    message.inbound_media_artifacts = artifacts;
    Ok(())
}

fn discord_attachment_artifact<F: DiscordAttachmentFetcher>(
    harness_home: &Path,
    message_id: &str,
    index: usize,
    attachment: &DiscordAttachmentMetadata,
    fetcher: &F,
) -> Result<InboundMediaArtifact, String> {
    let expected_mime = normalized_discord_attachment_mime(attachment.content_type.as_deref());
    let filename = attachment.filename.as_deref().unwrap_or("attachment");
    let mut artifact = InboundMediaArtifact {
        platform: "discord".to_string(),
        kind: discord_attachment_kind(expected_mime.as_deref()),
        message_id: Some(message_id.to_string()),
        selected_variant: Some(InboundMediaSelectedVariant {
            width: attachment.width,
            height: attachment.height,
            file_size: attachment.size,
        }),
        mime: expected_mime.clone(),
        byte_len: attachment.size,
        source: "discord.attachment".to_string(),
        model_attachment_status: InboundMediaModelAttachmentStatus::PromptOnly,
        ..InboundMediaArtifact::default()
    };
    if let Some(size) = attachment.size
        && size > DEFAULT_INBOUND_MEDIA_MAX_BYTES_PER_ITEM
    {
        artifact.download_status = InboundMediaDownloadStatus::DownloadFailed;
        artifact.warnings.push(format!(
            "discord attachment metadata exceeded maxBytesPerItem={DEFAULT_INBOUND_MEDIA_MAX_BYTES_PER_ITEM}"
        ));
        return Ok(artifact);
    }
    let Some(url) = attachment.url.as_deref() else {
        artifact.download_status = InboundMediaDownloadStatus::DetectedSkipped;
        artifact
            .warnings
            .push("discord attachment URL was not present in gateway payload".to_string());
        return Ok(artifact);
    };
    if !discord_attachment_url_allowed(url) {
        artifact.download_status = InboundMediaDownloadStatus::DownloadFailed;
        artifact.warnings.push(
            "discord attachment URL was outside the supported Discord media attachment hosts"
                .to_string(),
        );
        return Ok(artifact);
    }
    let bytes = match fetcher.fetch_attachment(url, DISCORD_ATTACHMENT_DOWNLOAD_MAX_BYTES) {
        Ok(bytes) => bytes,
        Err(_error) => {
            artifact.download_status = InboundMediaDownloadStatus::DownloadFailed;
            artifact.warnings.push(
                "discord attachment download failed; attachment content was not included"
                    .to_string(),
            );
            return Ok(artifact);
        }
    };
    artifact.byte_len = Some(bytes.len() as u64);
    let image_mime_from_bytes = discord_image_mime_from_bytes(&bytes);
    let detected_mime = detect_discord_attachment_mime(&bytes, expected_mime.as_deref());
    if let Some(mime) = detected_mime.as_deref() {
        artifact.mime = Some(mime.to_string());
        artifact.kind = discord_attachment_kind(Some(mime));
    }
    if expected_mime
        .as_deref()
        .is_some_and(|mime| mime.starts_with("image/"))
    {
        match image_mime_from_bytes {
            Some(mime) if Some(mime) == expected_mime.as_deref() => {}
            Some(_) => {
                artifact.download_status = InboundMediaDownloadStatus::DownloadFailed;
                artifact
                    .warnings
                    .push("downloaded image MIME did not match Discord metadata".to_string());
                return Ok(artifact);
            }
            None => {
                artifact.download_status = InboundMediaDownloadStatus::DownloadFailed;
                artifact
                    .warnings
                    .push("downloaded image bytes were not a supported image".to_string());
                return Ok(artifact);
            }
        }
    }
    let extension = discord_attachment_extension(filename, detected_mime.as_deref());
    let relative_name = format!(
        "discord/{}/{}.{}",
        safe_discord_artifact_segment(message_id),
        index,
        extension
    );
    let local_path = agent_harness_core::inbound_media_attachment_root(harness_home)
        .join(relative_name.replace('/', std::path::MAIN_SEPARATOR_STR));
    if let Some(parent) = local_path.parent() {
        fs::create_dir_all(parent).map_err(|err| err.to_string())?;
    }
    fs::write(&local_path, &bytes).map_err(|err| err.to_string())?;

    artifact.download_status = InboundMediaDownloadStatus::Downloaded;
    artifact.local_path = Some(local_path);
    artifact.artifact_uri = Some(format!(
        "agent-harness://inbound-media/discord/{}/{}.{}",
        safe_discord_artifact_segment(message_id),
        index,
        extension
    ));
    artifact.sha256 = Some(sha256_hex(&bytes));
    if is_discord_text_mime(detected_mime.as_deref()) {
        artifact.extraction_summary = Some(discord_text_attachment_extraction_summary(&bytes));
    }
    Ok(artifact)
}

fn discord_attachment_url_allowed(url: &str) -> bool {
    let trimmed = url.trim();
    let Some((scheme, rest)) = trimmed.split_once("://") else {
        return false;
    };
    if !scheme.eq_ignore_ascii_case("https") {
        return false;
    }
    let host_end = rest
        .find(|ch| matches!(ch, '/' | '?' | '#'))
        .unwrap_or(rest.len());
    let host = rest[..host_end].to_ascii_lowercase();
    if !matches!(host.as_str(), "cdn.discordapp.com" | "media.discordapp.net") {
        return false;
    }
    let path = &rest[host_end..];
    path.starts_with("/attachments/") || path.starts_with("/ephemeral-attachments/")
}

fn normalized_discord_attachment_mime(content_type: Option<&str>) -> Option<String> {
    content_type
        .and_then(|mime| mime.split(';').next())
        .map(str::trim)
        .filter(|mime| !mime.is_empty())
        .map(|mime| match mime.to_ascii_lowercase().as_str() {
            "image/jpg" => "image/jpeg".to_string(),
            normalized => normalized.to_string(),
        })
}

fn detect_discord_attachment_mime(bytes: &[u8], expected_mime: Option<&str>) -> Option<String> {
    discord_image_mime_from_bytes(bytes)
        .map(ToString::to_string)
        .or_else(|| {
            expected_mime
                .filter(|mime| is_discord_text_mime(Some(mime)))
                .map(ToString::to_string)
        })
        .or_else(|| expected_mime.map(ToString::to_string))
}

fn discord_attachment_kind(mime: Option<&str>) -> String {
    if is_discord_image_mime(mime) {
        "attachment-image".to_string()
    } else if is_discord_text_mime(mime) {
        "attachment-text".to_string()
    } else {
        "attachment-document".to_string()
    }
}

fn is_discord_image_mime(mime: Option<&str>) -> bool {
    matches!(
        mime,
        Some("image/jpeg" | "image/jpg" | "image/png" | "image/gif" | "image/webp")
    )
}

fn is_discord_text_mime(mime: Option<&str>) -> bool {
    mime.is_some_and(|mime| {
        mime == "text/plain"
            || mime == "text/markdown"
            || mime == "application/json"
            || mime.ends_with("+json")
    })
}

fn discord_image_mime_from_bytes(bytes: &[u8]) -> Option<&'static str> {
    if bytes.starts_with(&[0x89, b'P', b'N', b'G', b'\r', b'\n', 0x1a, b'\n']) {
        Some("image/png")
    } else if bytes.starts_with(&[0xff, 0xd8, 0xff]) {
        Some("image/jpeg")
    } else if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        Some("image/gif")
    } else if bytes.len() >= 12 && &bytes[0..4] == b"RIFF" && &bytes[8..12] == b"WEBP" {
        Some("image/webp")
    } else {
        None
    }
}

fn discord_attachment_extension(filename: &str, mime: Option<&str>) -> String {
    match mime {
        Some("image/jpeg" | "image/jpg") => "jpg".to_string(),
        Some("image/png") => "png".to_string(),
        Some("image/gif") => "gif".to_string(),
        Some("image/webp") => "webp".to_string(),
        Some("text/plain") => "txt".to_string(),
        Some("text/markdown") => "md".to_string(),
        Some("application/json") => "json".to_string(),
        Some(mime) if mime.ends_with("+json") => "json".to_string(),
        _ => Path::new(filename)
            .extension()
            .and_then(|value| value.to_str())
            .map(safe_discord_artifact_segment)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "bin".to_string()),
    }
}

fn safe_discord_artifact_segment(value: &str) -> String {
    let mut out = String::new();
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        "unknown".to_string()
    } else {
        out
    }
}

fn discord_text_attachment_extraction_summary(bytes: &[u8]) -> ArtifactExtractionSummary {
    let included = bytes
        .get(..bytes.len().min(DISCORD_ATTACHMENT_TEXT_EXTRACT_MAX_BYTES))
        .unwrap_or(bytes);
    let text = String::from_utf8_lossy(included);
    let mut summary = text
        .chars()
        .map(|ch| {
            if ch.is_control() && ch != '\n' && ch != '\t' {
                ' '
            } else {
                ch
            }
        })
        .collect::<String>();
    summary = summary.split_whitespace().collect::<Vec<_>>().join(" ");
    let truncated = bytes.len() > DISCORD_ATTACHMENT_TEXT_EXTRACT_MAX_BYTES;
    if truncated {
        summary.push_str(" [truncated]");
    }
    ArtifactExtractionSummary {
        artifact_class: Some("document".to_string()),
        modality: Some("text".to_string()),
        summary: Some(summary),
        facts: Vec::new(),
        uncertainty: Some(
            "bounded text extraction from Discord attachment; attachment content is untrusted"
                .to_string(),
        ),
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = digest::digest(&digest::SHA256, bytes);
    digest
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn discord_event_receipts_file(harness_home: &std::path::Path) -> PathBuf {
    harness_home
        .join("state")
        .join("channels")
        .join("discord-event-receipts.jsonl")
}

fn discord_reply_context_receipts_file(harness_home: &std::path::Path) -> PathBuf {
    harness_home
        .join("state")
        .join("channels")
        .join("discord-reply-context-receipts.jsonl")
}

fn discord_event_seen(harness_home: &std::path::Path, message_id: &str) -> Result<bool, String> {
    let path = discord_event_receipts_file(harness_home);
    let text = match fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.to_string()),
    };
    for line in text.lines() {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        if value
            .get("messageId")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|id| id == message_id)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn write_discord_event_receipt(report: &DiscordEventRunOnceReport) -> Result<(), String> {
    let path = discord_event_receipts_file(&report.harness_home);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| err.to_string())?;
    }
    let receipt = serde_json::json!({
        "schema": "agent-harness.discord-event-receipt.v1",
        "status": report.status,
        "reason": report.reason,
        "messageId": report.message_id,
        "guildId": report.guild_id,
        "channelId": report.channel_id,
        "userId": report.user_id,
        "sessionKey": report.run.as_ref().map(|run| run.receive.session_key.clone()),
    });
    append_jsonl_value(&path, &receipt).map_err(|err| err.to_string())
}

fn write_discord_reply_context_receipt(
    harness_home: &std::path::Path,
    message: &DiscordGatewayMessage,
    status: &str,
) -> Result<(), String> {
    let Some(reply_context) = &message.reply_context else {
        return Ok(());
    };
    let path = discord_reply_context_receipts_file(harness_home);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| err.to_string())?;
    }
    let receipt = serde_json::json!({
        "schema": "agent-harness.discord-reply-context-receipt.v1",
        "atMs": current_log_time_ms().map_err(|err| err.to_string())?,
        "status": status,
        "messageId": message.message_id,
        "channelId": message.channel_id,
        "guildId": message.guild_id,
        "userId": message.user_id,
        "referencedMessageId": reply_context.referenced_message_id,
        "referencedChannelId": reply_context.referenced_channel_id,
        "referencedGuildId": reply_context.referenced_guild_id,
        "referencedAuthorId": reply_context.referenced_author_id,
        "previewLength": reply_context.referenced_preview_length.unwrap_or(0),
        "previewTruncated": reply_context.referenced_preview_truncated,
        "contentLength": reply_context
            .referenced_text
            .as_ref()
            .map(|text| text.source_chars),
        "contentTruncated": reply_context
            .referenced_text
            .as_ref()
            .map(|text| text.truncated),
        "contentMaxChars": REPLY_CONTEXT_FULL_TEXT_MAX_CHARS,
        "hasAttachments": reply_context.has_attachments,
        "attachmentCount": reply_context.attachment_count,
        "embedsCount": reply_context.embeds_count,
        "source": reply_context.source,
        "sourceAvailable": reply_context.source_available,
    });
    append_jsonl_value(&path, &receipt).map_err(|err| err.to_string())
}

fn discord_dm_probe_report_file(harness_home: &std::path::Path) -> PathBuf {
    harness_home
        .join("state")
        .join("channels")
        .join("discord-dm-probe.json")
}

fn discord_dm_probe_receipts_file(harness_home: &std::path::Path) -> PathBuf {
    harness_home
        .join("state")
        .join("channels")
        .join("discord-dm-probe-receipts.jsonl")
}

fn discord_dm_history_probe_report_file(harness_home: &std::path::Path) -> PathBuf {
    harness_home
        .join("state")
        .join("channels")
        .join("discord-dm-history-probe.json")
}

fn discord_dm_history_probe_receipts_file(harness_home: &std::path::Path) -> PathBuf {
    harness_home
        .join("state")
        .join("channels")
        .join("discord-dm-history-probe-receipts.jsonl")
}

fn write_discord_dm_probe_report(report: &DiscordDmProbeReport) -> Result<(), String> {
    let report_file = discord_dm_probe_report_file(&report.harness_home);
    let receipts_file = discord_dm_probe_receipts_file(&report.harness_home);
    if let Some(parent) = report_file.parent() {
        fs::create_dir_all(parent).map_err(|err| err.to_string())?;
    }
    let receipt = serde_json::json!({
        "schema": "agent-harness.discord-dm-probe-receipt.v1",
        "status": report.status,
        "reason": report.reason,
        "userId": report.user_id,
        "channelId": report.channel_id,
        "providerMessageId": report.provider_message_id,
        "sentMessage": report.sent_message,
        "warnings": report.warnings,
        "atMs": current_log_time_ms().map_err(|err| err.to_string())?,
    });
    fs::write(
        &report_file,
        serde_json::to_string_pretty(&receipt).map_err(|err| err.to_string())?,
    )
    .map_err(|err| err.to_string())?;
    append_jsonl_value(&receipts_file, &receipt).map_err(|err| err.to_string())
}

fn write_discord_dm_history_probe_report(
    report: &DiscordDmHistoryProbeReport,
) -> Result<(), String> {
    let report_file = discord_dm_history_probe_report_file(&report.harness_home);
    let receipts_file = discord_dm_history_probe_receipts_file(&report.harness_home);
    if let Some(parent) = report_file.parent() {
        fs::create_dir_all(parent).map_err(|err| err.to_string())?;
    }
    let messages = report
        .messages
        .iter()
        .map(|message| {
            serde_json::json!({
                "messageId": message.message_id,
                "authorId": message.author_id,
                "authorBot": message.author_bot,
                "timestamp": message.timestamp,
                "contentLength": message.content_length,
            })
        })
        .collect::<Vec<_>>();
    let receipt = serde_json::json!({
        "schema": "agent-harness.discord-dm-history-probe-receipt.v1",
        "status": report.status,
        "reason": report.reason,
        "channelId": report.channel_id,
        "userId": report.user_id,
        "limit": report.limit,
        "messageCount": report.message_count,
        "userMessageCount": report.user_message_count,
        "botMessageCount": report.bot_message_count,
        "messages": messages,
        "warnings": report.warnings,
        "atMs": current_log_time_ms().map_err(|err| err.to_string())?,
    });
    fs::write(
        &report_file,
        serde_json::to_string_pretty(&receipt).map_err(|err| err.to_string())?,
    )
    .map_err(|err| err.to_string())?;
    append_jsonl_value(&receipts_file, &receipt).map_err(|err| err.to_string())
}

fn latest_discord_dm_probe(harness_home: &Path) -> Result<DiscordDmProbeRef, String> {
    let path = discord_dm_probe_report_file(harness_home);
    let text = fs::read_to_string(&path)
        .map_err(|err| format!("failed to read {}: {err}", path.display()))?;
    let value: serde_json::Value = serde_json::from_str(&text)
        .map_err(|err| format!("failed to parse {}: {err}", path.display()))?;
    Ok(DiscordDmProbeRef {
        channel_id: value
            .get("channelId")
            .and_then(serde_json::Value::as_str)
            .map(ToString::to_string),
        user_id: value
            .get("userId")
            .and_then(serde_json::Value::as_str)
            .map(ToString::to_string),
    })
}

fn export_channel_credentials(
    args: &ChannelCredentialsExportArgs,
) -> Result<ChannelCredentialExportReport, String> {
    let candidates = collect_channel_credentials(&args.source)?;
    let env_file = channel_credentials_env_file(&args.target_home);
    let receipt_file = channel_credentials_receipt_file(&args.target_home);
    if let Some(parent) = receipt_file.parent() {
        fs::create_dir_all(parent).map_err(|err| err.to_string())?;
    }

    let mut env_values = read_secret_env_map(&env_file)?;
    let mut entries = Vec::new();
    for candidate in candidates {
        let exported = args.include_sensitive && !candidate.value.trim().is_empty();
        if exported {
            env_values.insert(candidate.env_name.clone(), candidate.value.clone());
        }
        entries.push(ChannelCredentialExportEntry {
            env_name: candidate.env_name,
            source_path: candidate.source_path,
            length: candidate.value.len(),
            sensitive: candidate.sensitive,
            exported,
            reason: if exported {
                "written to harness secrets env".to_string()
            } else {
                "redacted dry-run; pass --include-sensitive to write value".to_string()
            },
        });
    }

    if args.include_sensitive {
        write_secret_env_file(&env_file, &env_values)?;
    }
    write_channel_credentials_receipt(
        &receipt_file,
        &args.source,
        &args.target_home,
        args.include_sensitive,
        &entries,
    )?;

    Ok(ChannelCredentialExportReport {
        env_file,
        receipt_file,
        entries,
    })
}

fn collect_channel_credentials(
    source: &AgentSource,
) -> Result<Vec<ChannelCredentialCandidate>, String> {
    let config_file = source.home.join("openclaw.json");
    let text = fs::read_to_string(&config_file)
        .map_err(|err| format!("failed to read {}: {err}", config_file.display()))?;
    let config: serde_json::Value = serde_json::from_str(&text)
        .map_err(|err| format!("invalid JSON in {}: {err}", config_file.display()))?;
    let mut candidates = Vec::new();

    if let Some(telegram) = config
        .get("channels")
        .and_then(|channels| channels.get("telegram"))
    {
        collect_telegram_credentials(source, telegram, &mut candidates);
    }
    if let Some(discord) = config
        .get("channels")
        .and_then(|channels| channels.get("discord"))
    {
        collect_discord_credentials(discord, &mut candidates);
    }

    Ok(candidates)
}

fn collect_telegram_credentials(
    source: &AgentSource,
    telegram: &serde_json::Value,
    candidates: &mut Vec<ChannelCredentialCandidate>,
) {
    let default_account = string_child(telegram, "defaultAccount")
        .unwrap_or_else(|| "default".to_string())
        .trim()
        .to_string();
    add_channel_credential(
        candidates,
        "AGENT_HARNESS_TELEGRAM_DEFAULT_ACCOUNT",
        default_account.clone(),
        "channels.telegram.defaultAccount",
        false,
    );

    let mut allowed_user_ids = BTreeSet::new();
    let mut group_allowed_user_ids = BTreeSet::new();
    let mut direct_chat_ids = BTreeSet::new();
    let mut group_chat_ids = BTreeSet::new();
    collect_string_array(telegram.get("allowFrom"), &mut allowed_user_ids);
    collect_string_array(telegram.get("groupAllowFrom"), &mut group_allowed_user_ids);
    collect_string_array(
        telegram
            .get("execApprovals")
            .and_then(|exec| exec.get("approvers")),
        &mut allowed_user_ids,
    );

    if let Some(accounts) = telegram
        .get("accounts")
        .and_then(serde_json::Value::as_object)
    {
        if let Some(account) = accounts
            .get(&default_account)
            .or_else(|| accounts.get("default"))
        {
            collect_telegram_account_token(
                source,
                &default_account,
                account,
                &default_account,
                candidates,
            );
        }
        for (account_id, account) in accounts {
            collect_telegram_account_token(
                source,
                account_id,
                account,
                &default_account,
                candidates,
            );
            collect_string_array(account.get("allowFrom"), &mut allowed_user_ids);
            collect_string_array(account.get("groupAllowFrom"), &mut group_allowed_user_ids);
            collect_object_keys(account.get("direct"), &mut direct_chat_ids);
            collect_object_keys(account.get("groups"), &mut group_chat_ids);
        }
    }
    if let Some(token) = string_child(telegram, "botToken") {
        add_channel_credential(
            candidates,
            "TELEGRAM_BOT_TOKEN",
            token,
            "channels.telegram.botToken",
            true,
        );
    }

    add_joined_credential(
        candidates,
        "AGENT_HARNESS_TELEGRAM_ALLOWED_USER_IDS",
        &allowed_user_ids,
        "channels.telegram.allowFrom + accounts.*.allowFrom",
    );
    add_joined_credential(
        candidates,
        "AGENT_HARNESS_TELEGRAM_GROUP_ALLOWED_USER_IDS",
        &group_allowed_user_ids,
        "channels.telegram.groupAllowFrom + accounts.*.groupAllowFrom",
    );
    add_joined_credential(
        candidates,
        "AGENT_HARNESS_TELEGRAM_DIRECT_CHAT_IDS",
        &direct_chat_ids,
        "channels.telegram.accounts.*.direct keys",
    );
    add_joined_credential(
        candidates,
        "AGENT_HARNESS_TELEGRAM_GROUP_CHAT_IDS",
        &group_chat_ids,
        "channels.telegram.accounts.*.groups keys",
    );
}

fn collect_telegram_account_token(
    source: &AgentSource,
    account_id: &str,
    account: &serde_json::Value,
    default_account: &str,
    candidates: &mut Vec<ChannelCredentialCandidate>,
) {
    let env_name = if account_id == default_account || account_id == "default" {
        "TELEGRAM_BOT_TOKEN".to_string()
    } else {
        format!(
            "AGENT_HARNESS_TELEGRAM_ACCOUNT_{}_BOT_TOKEN",
            sanitize_env_suffix(account_id)
        )
    };
    if let Some(token) = string_child(account, "botToken") {
        add_channel_credential(
            candidates,
            env_name,
            token,
            format!("channels.telegram.accounts.{account_id}.botToken"),
            true,
        );
        return;
    }
    if let Some(token_file) = string_child(account, "tokenFile")
        && let Ok(Some(token)) = read_source_token_file(source, &token_file)
    {
        add_channel_credential(
            candidates,
            env_name,
            token,
            format!("channels.telegram.accounts.{account_id}.tokenFile"),
            true,
        );
    }
}

fn collect_discord_credentials(
    discord: &serde_json::Value,
    candidates: &mut Vec<ChannelCredentialCandidate>,
) {
    if let Some(token) = string_child(discord, "token") {
        add_channel_credential(
            candidates,
            "DISCORD_BOT_TOKEN",
            normalize_discord_bot_token(&token),
            "channels.discord.token",
            true,
        );
    }

    let mut allowed_user_ids = BTreeSet::new();
    let mut guild_ids = BTreeSet::new();
    let mut channel_ids = BTreeSet::new();
    collect_string_array(discord.get("allowFrom"), &mut allowed_user_ids);
    collect_string_array(
        discord
            .get("execApprovals")
            .and_then(|exec| exec.get("approvers")),
        &mut allowed_user_ids,
    );
    if let Some(guilds) = discord.get("guilds").and_then(serde_json::Value::as_object) {
        for (guild_id, guild) in guilds {
            guild_ids.insert(guild_id.to_string());
            collect_string_array(guild.get("users"), &mut allowed_user_ids);
            if let Some(channels) = guild.get("channels").and_then(serde_json::Value::as_object) {
                for channel_id in channels.keys() {
                    channel_ids.insert(channel_id.to_string());
                }
            }
        }
    }
    add_joined_credential(
        candidates,
        "AGENT_HARNESS_DISCORD_ALLOWED_USER_IDS",
        &allowed_user_ids,
        "channels.discord.allowFrom + guilds.*.users",
    );
    add_joined_credential(
        candidates,
        "AGENT_HARNESS_DISCORD_GUILD_IDS",
        &guild_ids,
        "channels.discord.guilds keys",
    );
    add_joined_credential(
        candidates,
        "AGENT_HARNESS_DISCORD_CHANNEL_IDS",
        &channel_ids,
        "channels.discord.guilds.*.channels keys",
    );
}

fn add_joined_credential(
    candidates: &mut Vec<ChannelCredentialCandidate>,
    env_name: &str,
    values: &BTreeSet<String>,
    source_path: &str,
) {
    if values.is_empty() {
        return;
    }
    add_channel_credential(
        candidates,
        env_name,
        values.iter().cloned().collect::<Vec<_>>().join(","),
        source_path,
        false,
    );
}

fn add_channel_credential(
    candidates: &mut Vec<ChannelCredentialCandidate>,
    env_name: impl Into<String>,
    value: impl Into<String>,
    source_path: impl Into<String>,
    sensitive: bool,
) {
    let env_name = env_name.into();
    if candidates
        .iter()
        .any(|candidate| candidate.env_name == env_name)
    {
        return;
    }
    let value = value.into().trim().to_string();
    if value.is_empty() {
        return;
    }
    candidates.push(ChannelCredentialCandidate {
        env_name,
        value,
        source_path: source_path.into(),
        sensitive,
    });
}

fn collect_string_array(value: Option<&serde_json::Value>, target: &mut BTreeSet<String>) {
    let Some(array) = value.and_then(serde_json::Value::as_array) else {
        return;
    };
    for item in array {
        if let Some(value) = json_scalar_string(item) {
            let value = value.trim();
            if !value.is_empty() {
                target.insert(value.to_string());
            }
        }
    }
}

fn collect_object_keys(value: Option<&serde_json::Value>, target: &mut BTreeSet<String>) {
    let Some(object) = value.and_then(serde_json::Value::as_object) else {
        return;
    };
    target.extend(object.keys().cloned());
}

fn string_child(value: &serde_json::Value, key: &str) -> Option<String> {
    value.get(key).and_then(json_scalar_string)
}

fn json_scalar_string(value: &serde_json::Value) -> Option<String> {
    value
        .as_str()
        .map(ToString::to_string)
        .or_else(|| value.as_i64().map(|number| number.to_string()))
        .or_else(|| value.as_u64().map(|number| number.to_string()))
}

fn read_source_token_file(
    source: &AgentSource,
    token_file: &str,
) -> Result<Option<String>, String> {
    let path = resolve_source_config_path(source, token_file);
    let text = match fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("failed to read {}: {error}", path.display())),
    };
    let token = text.trim().to_string();
    if token.is_empty() {
        Ok(None)
    } else {
        Ok(Some(token))
    }
}

fn resolve_source_config_path(source: &AgentSource, raw: &str) -> PathBuf {
    let raw = raw.trim();
    for prefix in ["/root/.openclaw/", "/home/agent/.openclaw/"] {
        if let Some(relative) = raw.strip_prefix(prefix) {
            return join_unix_relative(&source.home, relative);
        }
    }
    if matches!(raw, "/root/.openclaw" | "/home/agent/.openclaw") {
        return source.home.clone();
    }
    let path = PathBuf::from(raw);
    if path.is_absolute() {
        path
    } else {
        source.home.join(path)
    }
}

fn join_unix_relative(root: &Path, relative: &str) -> PathBuf {
    let mut path = root.to_path_buf();
    for component in relative.split('/') {
        if !component.is_empty() {
            path.push(component);
        }
    }
    path
}

fn sanitize_env_suffix(value: &str) -> String {
    let mut suffix = String::new();
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            suffix.push(ch.to_ascii_uppercase());
        } else {
            suffix.push('_');
        }
    }
    if suffix.is_empty() {
        "ACCOUNT".to_string()
    } else {
        suffix
    }
}

fn write_channel_credentials_receipt(
    receipt_file: &Path,
    source: &AgentSource,
    target_home: &Path,
    include_sensitive: bool,
    entries: &[ChannelCredentialExportEntry],
) -> Result<(), String> {
    let entries: Vec<_> = entries
        .iter()
        .map(|entry| {
            serde_json::json!({
                "envName": entry.env_name,
                "sourcePath": entry.source_path,
                "length": entry.length,
                "sensitive": entry.sensitive,
                "exported": entry.exported,
                "reason": entry.reason,
            })
        })
        .collect();
    let receipt = serde_json::json!({
        "schema": "agent-harness.channel-credentials-export.v1",
        "createdAtMs": current_time_ms()?,
        "sourceHome": source.home,
        "targetHome": target_home,
        "includeSensitive": include_sensitive,
        "entries": entries,
    });
    fs::write(
        receipt_file,
        serde_json::to_string_pretty(&receipt).map_err(|err| err.to_string())?,
    )
    .map_err(|err| err.to_string())
}

fn channel_credentials_env_file(harness_home: &Path) -> PathBuf {
    harness_home.join("secrets").join("channel-credentials.env")
}

fn channel_credentials_receipt_file(harness_home: &Path) -> PathBuf {
    harness_home
        .join("secrets")
        .join("channel-credentials-receipts.json")
}

fn read_secret_env_map(path: &Path) -> Result<BTreeMap<String, String>, String> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(error) => return Err(format!("failed to read {}: {error}", path.display())),
    };
    let mut values = BTreeMap::new();
    for line in text.lines() {
        if let Some((key, value)) = parse_secret_env_line(line) {
            values.insert(key, value);
        }
    }
    Ok(values)
}

fn parse_secret_env_line(line: &str) -> Option<(String, String)> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return None;
    }
    let (key, value) = line.split_once('=')?;
    let key = key.trim();
    if key.is_empty()
        || !key
            .chars()
            .all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
    {
        return None;
    }
    Some((key.to_string(), unquote_secret_env_value(value.trim())))
}

fn unquote_secret_env_value(value: &str) -> String {
    if value.len() >= 2 && value.starts_with('"') && value.ends_with('"') {
        let mut output = String::new();
        let mut chars = value[1..value.len() - 1].chars();
        while let Some(ch) = chars.next() {
            if ch == '\\' {
                match chars.next() {
                    Some('n') => output.push('\n'),
                    Some('r') => output.push('\r'),
                    Some('t') => output.push('\t'),
                    Some(other) => output.push(other),
                    None => output.push('\\'),
                }
            } else {
                output.push(ch);
            }
        }
        output
    } else if value.len() >= 2 && value.starts_with('\'') && value.ends_with('\'') {
        value[1..value.len() - 1].to_string()
    } else {
        value.to_string()
    }
}

fn write_secret_env_file(path: &Path, values: &BTreeMap<String, String>) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| err.to_string())?;
    }
    let mut text =
        String::from("# Generated by agent-harness channel-credentials-export. Do not commit.\n");
    for (key, value) in values {
        text.push_str(key);
        text.push('=');
        text.push_str(&quote_secret_env_value(value));
        text.push('\n');
    }
    fs::write(path, text).map_err(|err| err.to_string())
}

fn quote_secret_env_value(value: &str) -> String {
    let escaped = value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\r', "\\r")
        .replace('\n', "\\n");
    format!("\"{escaped}\"")
}

fn secret_env_value(harness_home: &Path, env_name: &str) -> Option<String> {
    read_secret_env_map(&channel_credentials_env_file(harness_home))
        .ok()
        .and_then(|mut values| values.remove(env_name))
}

fn channel_access_policy(harness_home: &Path) -> Result<ChannelAccessPolicy, String> {
    let values = read_secret_env_map(&channel_credentials_env_file(harness_home))?;
    Ok(ChannelAccessPolicy {
        telegram_admin_user_ids: channel_id_set(&values, "AGENT_HARNESS_TELEGRAM_ADMIN_USER_IDS"),
        telegram_allowed_user_ids: channel_id_set(
            &values,
            "AGENT_HARNESS_TELEGRAM_ALLOWED_USER_IDS",
        ),
        telegram_group_admin_user_ids: channel_id_set(
            &values,
            "AGENT_HARNESS_TELEGRAM_GROUP_ADMIN_USER_IDS",
        ),
        telegram_group_allowed_user_ids: channel_id_set(
            &values,
            "AGENT_HARNESS_TELEGRAM_GROUP_ALLOWED_USER_IDS",
        ),
        telegram_direct_chat_ids: channel_id_set(&values, "AGENT_HARNESS_TELEGRAM_DIRECT_CHAT_IDS"),
        telegram_group_chat_ids: channel_id_set(&values, "AGENT_HARNESS_TELEGRAM_GROUP_CHAT_IDS"),
        telegram_group_open: channel_bool(&values, "AGENT_HARNESS_TELEGRAM_GROUP_OPEN"),
        discord_admin_user_ids: channel_id_set(&values, "AGENT_HARNESS_DISCORD_ADMIN_USER_IDS"),
        discord_allowed_user_ids: channel_id_set(&values, "AGENT_HARNESS_DISCORD_ALLOWED_USER_IDS"),
        discord_group_allowed_user_ids: channel_id_set(
            &values,
            "AGENT_HARNESS_DISCORD_GROUP_ALLOWED_USER_IDS",
        ),
        discord_channel_ids: channel_id_set(&values, "AGENT_HARNESS_DISCORD_CHANNEL_IDS"),
        discord_guild_ids: channel_id_set(&values, "AGENT_HARNESS_DISCORD_GUILD_IDS"),
        discord_group_open: channel_bool(&values, "AGENT_HARNESS_DISCORD_GROUP_OPEN"),
    })
}

fn channel_id_set(values: &BTreeMap<String, String>, env_name: &str) -> BTreeSet<String> {
    let raw = env::var(env_name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| values.get(env_name).cloned());
    parse_channel_id_set(raw.as_deref())
}

fn parse_channel_id_set(raw: Option<&str>) -> BTreeSet<String> {
    raw.unwrap_or_default()
        .split(|ch: char| ch == ',' || ch == ';' || ch.is_ascii_whitespace())
        .filter_map(|value| {
            let value = value.trim().trim_matches('"').trim_matches('\'');
            (!value.is_empty()).then(|| value.to_string())
        })
        .collect()
}

fn channel_bool(values: &BTreeMap<String, String>, env_name: &str) -> bool {
    let raw = env::var(env_name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| values.get(env_name).cloned());
    raw.as_deref().is_some_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

fn telegram_access_decision(
    policy: &ChannelAccessPolicy,
    chat_id: &str,
    user_id: &str,
    chat_type: Option<&str>,
) -> ChannelAccessDecision {
    if telegram_is_group_context(policy, chat_id, chat_type) {
        if !policy.telegram_group_chat_ids.is_empty()
            && !policy.telegram_group_chat_ids.contains(chat_id)
        {
            return ChannelAccessDecision::Denied(
                "Telegram group chat id is not in AGENT_HARNESS_TELEGRAM_GROUP_CHAT_IDS"
                    .to_string(),
            );
        }
        if telegram_user_is_admin(policy, user_id)
            || policy.telegram_group_admin_user_ids.contains(user_id)
        {
            return ChannelAccessDecision::Allowed(ChannelPermission::Admin);
        }
        if policy.telegram_group_allowed_user_ids.contains(user_id) {
            return ChannelAccessDecision::Allowed(ChannelPermission::Limited);
        }
        if policy.telegram_group_open
            || (policy.telegram_group_allowed_user_ids.is_empty()
                && policy.telegram_group_admin_user_ids.is_empty()
                && !policy.telegram_group_chat_ids.is_empty())
        {
            return ChannelAccessDecision::Allowed(ChannelPermission::Limited);
        }
        if policy.telegram_group_chat_ids.is_empty() {
            return ChannelAccessDecision::Denied(
                "Telegram group chat id is not configured in AGENT_HARNESS_TELEGRAM_GROUP_CHAT_IDS"
                    .to_string(),
            );
        }
        return ChannelAccessDecision::Denied(
            "Telegram group user id is not an admin, limited user, or open-group member"
                .to_string(),
        );
    } else {
        if !policy.telegram_direct_chat_ids.is_empty()
            && !policy.telegram_direct_chat_ids.contains(chat_id)
        {
            return ChannelAccessDecision::Denied(
                "Telegram direct chat id is not in AGENT_HARNESS_TELEGRAM_DIRECT_CHAT_IDS"
                    .to_string(),
            );
        }
        if !telegram_user_is_admin(policy, user_id) {
            return ChannelAccessDecision::Denied(
                "Telegram DM user id is not in AGENT_HARNESS_TELEGRAM_ADMIN_USER_IDS or AGENT_HARNESS_TELEGRAM_ALLOWED_USER_IDS".to_string(),
            );
        }
    }
    ChannelAccessDecision::Allowed(ChannelPermission::Admin)
}

fn telegram_is_group_context(
    policy: &ChannelAccessPolicy,
    chat_id: &str,
    chat_type: Option<&str>,
) -> bool {
    matches!(chat_type, Some("group" | "supergroup" | "channel"))
        || policy.telegram_group_chat_ids.contains(chat_id)
}

fn discord_access_decision(
    policy: &ChannelAccessPolicy,
    message: &DiscordGatewayMessage,
) -> ChannelAccessDecision {
    if let Some(guild_id) = message.guild_id.as_deref() {
        if !policy.discord_guild_ids.is_empty() && !policy.discord_guild_ids.contains(guild_id) {
            return ChannelAccessDecision::Denied(
                "Discord guild id is not in AGENT_HARNESS_DISCORD_GUILD_IDS".to_string(),
            );
        }
        if !policy.discord_channel_ids.is_empty()
            && !policy.discord_channel_ids.contains(&message.channel_id)
        {
            return ChannelAccessDecision::Denied(
                "Discord channel id is not in AGENT_HARNESS_DISCORD_CHANNEL_IDS".to_string(),
            );
        }
        if discord_user_is_admin(policy, &message.user_id) {
            return ChannelAccessDecision::Allowed(ChannelPermission::Admin);
        }
        if policy
            .discord_group_allowed_user_ids
            .contains(&message.user_id)
        {
            return ChannelAccessDecision::Allowed(ChannelPermission::Limited);
        }
        if policy.discord_group_open
            || (policy.discord_group_allowed_user_ids.is_empty()
                && !policy.discord_channel_ids.is_empty())
        {
            return ChannelAccessDecision::Allowed(ChannelPermission::Limited);
        }
        return ChannelAccessDecision::Denied(
            "Discord guild user id is not an admin, limited user, or open-guild member".to_string(),
        );
    }
    if !discord_user_is_admin(policy, &message.user_id) {
        return ChannelAccessDecision::Denied(
            "Discord DM user id is not in AGENT_HARNESS_DISCORD_ADMIN_USER_IDS or AGENT_HARNESS_DISCORD_ALLOWED_USER_IDS".to_string(),
        );
    }
    ChannelAccessDecision::Allowed(ChannelPermission::Admin)
}

fn telegram_user_is_admin(policy: &ChannelAccessPolicy, user_id: &str) -> bool {
    policy.telegram_admin_user_ids.contains(user_id)
        || policy.telegram_allowed_user_ids.contains(user_id)
}

fn discord_user_is_admin(policy: &ChannelAccessPolicy, user_id: &str) -> bool {
    policy.discord_admin_user_ids.contains(user_id)
        || policy.discord_allowed_user_ids.contains(user_id)
}

fn channel_permission_allows_text(permission: ChannelPermission, text: &str) -> Result<(), String> {
    if permission == ChannelPermission::Admin {
        return Ok(());
    }
    let Some(command) = parse_channel_command(text) else {
        return Ok(());
    };
    if limited_permission_allows_command(&command) {
        Ok(())
    } else {
        Err(format!(
            "limited channel permission does not allow /{}",
            command.name()
        ))
    }
}

fn limited_permission_allows_command(command: &ChannelCommand) -> bool {
    match command {
        ChannelCommand::Status { .. } => true,
        ChannelCommand::Model {
            target: None,
            global: false,
        } => true,
        ChannelCommand::Think {
            level: None,
            global: false,
        } => true,
        _ => false,
    }
}

fn telegram_bot_token(harness_home: &Path, account_id: Option<&str>) -> Result<String, String> {
    let account_id = account_id
        .map(str::trim)
        .filter(|account_id| !account_id.is_empty());
    let env_name = account_id
        .filter(|account_id| *account_id != "default")
        .map(telegram_account_token_env_name)
        .unwrap_or_else(|| "TELEGRAM_BOT_TOKEN".to_string());
    env::var(&env_name)
        .ok()
        .or_else(|| secret_env_value(harness_home, &env_name))
        .ok_or_else(|| {
            format!(
                "{env_name} is required for this Telegram adapter; run channel-credentials-export or set the env var"
            )
        })
}

fn telegram_account_token_env_name(account_id: &str) -> String {
    format!(
        "AGENT_HARNESS_TELEGRAM_ACCOUNT_{}_BOT_TOKEN",
        sanitize_env_suffix(account_id)
    )
}

fn sanitize_file_component(value: &str) -> String {
    let mut output = String::new();
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
            output.push(ch.to_ascii_lowercase());
        } else {
            output.push('-');
        }
    }
    output.trim_matches('-').to_string()
}

fn telegram_account_id(account_id: Option<&str>) -> String {
    account_id
        .map(str::trim)
        .filter(|account_id| !account_id.is_empty())
        .unwrap_or("default")
        .to_ascii_lowercase()
}

fn outbound_message_account_id(message: &ChannelOutboundMessage) -> String {
    message
        .account_id
        .as_deref()
        .map(str::trim)
        .filter(|account_id| !account_id.is_empty())
        .unwrap_or("default")
        .to_ascii_lowercase()
}

fn telegram_offset_file(harness_home: &std::path::Path, account_id: Option<&str>) -> PathBuf {
    let file_name = account_id
        .map(str::trim)
        .filter(|account_id| !account_id.is_empty() && *account_id != "default")
        .map(|account_id| {
            format!(
                "telegram-offset-{}.json",
                sanitize_file_component(account_id)
            )
        })
        .unwrap_or_else(|| "telegram-offset.json".to_string());
    harness_home.join("state").join("channels").join(file_name)
}

fn telegram_probe_latest_file(harness_home: &std::path::Path) -> PathBuf {
    harness_home
        .join("state")
        .join("channels")
        .join("telegram-probe.json")
}

fn telegram_probe_receipts_file(harness_home: &std::path::Path) -> PathBuf {
    harness_home
        .join("state")
        .join("channels")
        .join("telegram-probe-receipts.jsonl")
}

fn read_telegram_offset(
    path: &std::path::Path,
    warnings: &mut Vec<String>,
) -> Result<Option<i64>, String> {
    if !path.is_file() {
        return Ok(None);
    }
    let text = fs::read_to_string(path).map_err(|err| err.to_string())?;
    let value: serde_json::Value = serde_json::from_str(&text).map_err(|err| {
        warnings.push(format!(
            "Telegram offset file is invalid at {}: {}",
            path.display(),
            err
        ));
        err.to_string()
    })?;
    Ok(value.get("nextOffset").and_then(serde_json::Value::as_i64))
}

fn write_telegram_offset(path: &std::path::Path, next_offset: Option<i64>) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| err.to_string())?;
    }
    fs::write(
        path,
        serde_json::to_string_pretty(&serde_json::json!({
            "schema": "agent-harness.telegram-offset.v1",
            "nextOffset": next_offset
        }))
        .map_err(|err| err.to_string())?,
    )
    .map_err(|err| err.to_string())
}

const CHANNEL_HTTP_CONNECT_TIMEOUT_SECONDS: u64 = 10;
const CHANNEL_HTTP_WRITE_TIMEOUT_SECONDS: u64 = 10;
const CHANNEL_HTTP_SHORT_TIMEOUT_SECONDS: u64 = 30;
const TELEGRAM_POLL_HTTP_GRACE_SECONDS: u64 = 10;

fn channel_http_agent(read_timeout: Duration) -> ureq::Agent {
    ureq::AgentBuilder::new()
        .timeout_connect(Duration::from_secs(CHANNEL_HTTP_CONNECT_TIMEOUT_SECONDS))
        .timeout_read(read_timeout)
        .timeout_write(Duration::from_secs(CHANNEL_HTTP_WRITE_TIMEOUT_SECONDS))
        .build()
}

fn channel_http_short_agent() -> ureq::Agent {
    channel_http_agent(Duration::from_secs(CHANNEL_HTTP_SHORT_TIMEOUT_SECONDS))
}

fn telegram_poll_agent(timeout_seconds: u64) -> ureq::Agent {
    let read_timeout_seconds = timeout_seconds
        .saturating_add(TELEGRAM_POLL_HTTP_GRACE_SECONDS)
        .clamp(10, 120);
    channel_http_agent(Duration::from_secs(read_timeout_seconds))
}

fn telegram_get_me(token: &str) -> Result<serde_json::Value, String> {
    let url = format!("https://api.telegram.org/bot{token}/getMe");
    let agent = channel_http_short_agent();
    let response = agent.get(&url).call().map_err(telegram_http_error)?;
    let value: serde_json::Value = response.into_json().map_err(|err| err.to_string())?;
    if value.get("ok").and_then(serde_json::Value::as_bool) != Some(true) {
        return Err(format!("Telegram getMe returned non-ok response: {value}"));
    }
    value
        .get("result")
        .cloned()
        .ok_or_else(|| format!("Telegram getMe response was missing result: {value}"))
}

fn telegram_get_updates(
    token: &str,
    offset: Option<i64>,
    timeout_seconds: u64,
    limit: usize,
) -> Result<Vec<serde_json::Value>, String> {
    let url = format!("https://api.telegram.org/bot{token}/getUpdates");
    let mut payload = serde_json::json!({
        "timeout": timeout_seconds,
        "limit": limit,
        "allowed_updates": ["message", "edited_message"]
    });
    if let Some(offset) = offset {
        payload["offset"] = serde_json::json!(offset);
    }
    let agent = telegram_poll_agent(timeout_seconds);
    let response = agent
        .post(&url)
        .send_json(payload)
        .map_err(telegram_http_error)?;
    let value: serde_json::Value = response.into_json().map_err(|err| err.to_string())?;
    if value.get("ok").and_then(serde_json::Value::as_bool) != Some(true) {
        return Err(format!(
            "Telegram getUpdates returned non-ok response: {value}"
        ));
    }
    Ok(value
        .get("result")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TelegramFormattingMode {
    Plain,
    Html,
}

#[derive(Debug, Clone, Copy)]
struct TelegramSendOptions<'a> {
    reply_to_message_id: Option<i64>,
    message_thread_id: Option<&'a str>,
    formatting_mode: TelegramFormattingMode,
}

impl Default for TelegramSendOptions<'_> {
    fn default() -> Self {
        Self {
            reply_to_message_id: None,
            message_thread_id: None,
            formatting_mode: TelegramFormattingMode::Plain,
        }
    }
}

fn telegram_send_message(
    token: &str,
    chat_id: &str,
    text: &str,
    options: TelegramSendOptions<'_>,
) -> Result<Option<String>, String> {
    let url = format!("https://api.telegram.org/bot{token}/sendMessage");
    let agent = channel_http_short_agent();
    let payload = telegram_message_payload(chat_id, text, options);
    let response = agent
        .post(&url)
        .send_json(payload)
        .map_err(telegram_http_error)?;
    let value: serde_json::Value = response.into_json().map_err(|err| err.to_string())?;
    if value.get("ok").and_then(serde_json::Value::as_bool) != Some(true) {
        return Err(format!(
            "Telegram sendMessage returned non-ok response: {value}"
        ));
    }
    Ok(value
        .get("result")
        .and_then(|result| result.get("message_id"))
        .and_then(telegram_id_string))
}

fn telegram_message_payload(
    chat_id: &str,
    text: &str,
    options: TelegramSendOptions<'_>,
) -> serde_json::Value {
    let mut payload = match options.formatting_mode {
        TelegramFormattingMode::Plain => serde_json::json!({
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": true
        }),
        TelegramFormattingMode::Html => serde_json::json!({
            "chat_id": chat_id,
            "text": render_telegram_safe_html(text),
            "parse_mode": "HTML",
            "disable_web_page_preview": true,
            "link_preview_options": {
                "is_disabled": true
            }
        }),
    };
    if let Some(reply_to_message_id) = options.reply_to_message_id {
        payload["reply_to_message_id"] = serde_json::json!(reply_to_message_id);
        payload["allow_sending_without_reply"] = serde_json::json!(true);
    }
    if let Some(thread_id) = options
        .message_thread_id
        .map(str::trim)
        .filter(|thread_id| !thread_id.is_empty())
        && let Ok(thread_id) = thread_id.parse::<i64>()
    {
        payload["message_thread_id"] = serde_json::json!(thread_id);
    }
    payload
}

fn telegram_send_outbound_message(
    harness_home: &Path,
    token: &str,
    message: &ChannelOutboundMessage,
) -> Result<Option<String>, String> {
    let mut provider_message_ids = Vec::new();
    let text = format_channel_reply_text(&message.text);
    if message.attachments.is_empty() || !text.trim().is_empty() {
        if let Some(provider_message_id) = telegram_send_message(
            token,
            &message.channel_id,
            &text,
            TelegramSendOptions {
                reply_to_message_id: telegram_reply_to_message_id(message),
                message_thread_id: telegram_message_thread_id(message),
                formatting_mode: telegram_formatting_mode_for_message(harness_home, message),
            },
        )? {
            provider_message_ids.push(provider_message_id);
        }
    }
    for attachment in &message.attachments {
        if let Some(provider_message_id) =
            telegram_send_attachment(token, &message.channel_id, attachment)?
        {
            provider_message_ids.push(provider_message_id);
        }
    }
    Ok((!provider_message_ids.is_empty()).then(|| provider_message_ids.join(",")))
}

fn telegram_reply_to_message_id(message: &ChannelOutboundMessage) -> Option<i64> {
    let intent = message.delivery_intent.as_ref()?;
    if !intent.validated || intent.kind != ChannelDeliveryIntentKind::ReplyToMessage {
        return None;
    }
    if intent
        .platform_channel_id
        .as_deref()
        .is_some_and(|channel_id| channel_id != message.channel_id)
    {
        return None;
    }
    intent
        .platform_message_id
        .as_deref()
        .and_then(|value| value.parse::<i64>().ok())
}

fn telegram_message_thread_id(message: &ChannelOutboundMessage) -> Option<&str> {
    let intent = message.delivery_intent.as_ref()?;
    if !intent.validated {
        return None;
    }
    if !matches!(
        intent.kind,
        ChannelDeliveryIntentKind::ReplyToMessage | ChannelDeliveryIntentKind::ThreadReply
    ) {
        return None;
    }
    if intent
        .platform_channel_id
        .as_deref()
        .is_some_and(|channel_id| channel_id != message.channel_id)
    {
        return None;
    }
    intent
        .platform_thread_id
        .as_deref()
        .map(str::trim)
        .filter(|thread_id| !thread_id.is_empty())
}

fn telegram_formatting_mode_for_message(
    harness_home: &Path,
    message: &ChannelOutboundMessage,
) -> TelegramFormattingMode {
    if message.kind != agent_harness_core::ChannelOutboundMessageKind::AgentReply {
        return TelegramFormattingMode::Plain;
    }
    let config = load_telegram_formatting_config(harness_home);
    let agent_id = agent_id_from_session_key(&message.session_key);
    let channel_selectors = [
        format!(
            "{}:{}:{}",
            message.platform, message.channel_id, message.user_id
        ),
        format!("{}:{}", message.platform, message.channel_id),
        message.channel_id.clone(),
        message.platform.clone(),
    ];
    for selector in channel_selectors {
        if let Some(mode) = config.channel_modes.get(&selector) {
            return *mode;
        }
    }
    if let Some(account_id) = message.account_id.as_deref()
        && let Some(mode) = config
            .account_modes
            .get(&telegram_account_id(Some(account_id)))
    {
        return *mode;
    }
    if let Some(agent_id) = agent_id
        && let Some(mode) = config.agent_modes.get(agent_id)
    {
        return *mode;
    }
    config.global
}

#[derive(Debug, Clone)]
struct TelegramFormattingConfig {
    global: TelegramFormattingMode,
    agent_modes: BTreeMap<String, TelegramFormattingMode>,
    account_modes: BTreeMap<String, TelegramFormattingMode>,
    channel_modes: BTreeMap<String, TelegramFormattingMode>,
}

impl Default for TelegramFormattingConfig {
    fn default() -> Self {
        Self {
            global: TelegramFormattingMode::Plain,
            agent_modes: BTreeMap::new(),
            account_modes: BTreeMap::new(),
            channel_modes: BTreeMap::new(),
        }
    }
}

fn load_telegram_formatting_config(harness_home: &Path) -> TelegramFormattingConfig {
    let config_file = harness_home.join("harness-config.json");
    let Ok(text) = fs::read_to_string(config_file) else {
        return TelegramFormattingConfig::default();
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) else {
        return TelegramFormattingConfig::default();
    };
    let Some(response) = value.get("response").and_then(serde_json::Value::as_object) else {
        return TelegramFormattingConfig::default();
    };
    let mut config = TelegramFormattingConfig::default();
    if let Some(mode) = response
        .get("telegramFormattingMode")
        .or_else(|| response.get("telegram_formatting_mode"))
        .and_then(serde_json::Value::as_str)
        .and_then(parse_telegram_formatting_mode)
    {
        config.global = mode;
    }
    config.agent_modes = parse_telegram_formatting_mode_map(
        response
            .get("telegramFormattingAgentModes")
            .or_else(|| response.get("telegram_formatting_agent_modes")),
        false,
    );
    config.account_modes = parse_telegram_formatting_mode_map(
        response
            .get("telegramFormattingAccountModes")
            .or_else(|| response.get("telegram_formatting_account_modes")),
        true,
    );
    config.channel_modes = parse_telegram_formatting_mode_map(
        response
            .get("telegramFormattingChannelModes")
            .or_else(|| response.get("telegram_formatting_channel_modes")),
        false,
    );
    config
}

fn parse_telegram_formatting_mode_map(
    value: Option<&serde_json::Value>,
    normalize_account: bool,
) -> BTreeMap<String, TelegramFormattingMode> {
    let Some(object) = value.and_then(serde_json::Value::as_object) else {
        return BTreeMap::new();
    };
    object
        .iter()
        .filter_map(|(key, value)| {
            let mode = value.as_str().and_then(parse_telegram_formatting_mode)?;
            let key = if normalize_account {
                telegram_account_id(Some(key))
            } else {
                key.to_string()
            };
            Some((key, mode))
        })
        .collect()
}

fn parse_telegram_formatting_mode(value: &str) -> Option<TelegramFormattingMode> {
    match value.trim().to_ascii_lowercase().as_str() {
        "plain" | "text" | "off" | "disabled" | "false" => Some(TelegramFormattingMode::Plain),
        "html" | "telegram-html" | "on" | "enabled" | "true" => Some(TelegramFormattingMode::Html),
        _ => None,
    }
}

fn agent_id_from_session_key(session_key: &str) -> Option<&str> {
    session_key
        .rsplit(':')
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn render_telegram_safe_html(text: &str) -> String {
    let mut out = String::new();
    let mut in_pre = false;
    let mut pre = String::new();
    for line in text.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("```") {
            if in_pre {
                out.push_str("<pre>");
                out.push_str(&html_escape_text(pre.trim_end_matches('\n')));
                out.push_str("</pre>");
                pre.clear();
                in_pre = false;
            } else {
                if !out.is_empty() {
                    out.push('\n');
                }
                in_pre = true;
            }
            continue;
        }
        if in_pre {
            pre.push_str(line);
            pre.push('\n');
            continue;
        }
        if !out.is_empty() {
            out.push('\n');
        }
        if let Some(heading) = line.strip_prefix("# ") {
            out.push_str("<b>");
            out.push_str(&html_escape_text(heading.trim()));
            out.push_str("</b>");
        } else {
            out.push_str(&render_inline_telegram_html(line));
        }
    }
    if in_pre {
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("<pre>");
        out.push_str(&html_escape_text(pre.trim_end_matches('\n')));
        out.push_str("</pre>");
    }
    out
}

fn render_inline_telegram_html(line: &str) -> String {
    let mut out = String::new();
    let mut remaining = line;
    let mut code = false;
    while let Some(index) = remaining.find('`') {
        let (plain, rest) = remaining.split_at(index);
        out.push_str(&html_escape_text(plain));
        remaining = &rest[1..];
        if code {
            out.push_str("</code>");
        } else {
            out.push_str("<code>");
        }
        code = !code;
    }
    out.push_str(&html_escape_text(remaining));
    if code {
        out.push_str("</code>");
    }
    out
}

fn html_escape_text(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(ch),
        }
    }
    out
}

fn telegram_send_attachment(
    token: &str,
    chat_id: &str,
    attachment: &ChannelOutboundAttachment,
) -> Result<Option<String>, String> {
    let (method, file_field) = match attachment.kind {
        ChannelOutboundAttachmentKind::Image => ("sendPhoto", "photo"),
        ChannelOutboundAttachmentKind::Document => ("sendDocument", "document"),
    };
    let url = format!("https://api.telegram.org/bot{token}/{method}");
    let mut fields = vec![("chat_id".to_string(), chat_id.to_string())];
    if let Some(caption) = attachment.caption.as_deref()
        && !caption.trim().is_empty()
    {
        fields.push(("caption".to_string(), caption.to_string()));
    }
    let value = multipart_post_json("Telegram", &url, None, &fields, file_field, attachment)?;
    if value.get("ok").and_then(serde_json::Value::as_bool) != Some(true) {
        return Err(format!(
            "Telegram {method} returned non-ok response: {value}"
        ));
    }
    Ok(value
        .get("result")
        .and_then(|result| result.get("message_id"))
        .and_then(telegram_id_string))
}

fn format_channel_reply_text(text: &str) -> String {
    text.trim().to_string()
}

fn telegram_edit_message_text(
    token: &str,
    chat_id: &str,
    message_id: &str,
    text: &str,
) -> Result<Option<String>, String> {
    let url = format!("https://api.telegram.org/bot{token}/editMessageText");
    let agent = channel_http_short_agent();
    let message_id_value = message_id
        .parse::<i64>()
        .map(serde_json::Value::from)
        .unwrap_or_else(|_| serde_json::Value::String(message_id.to_string()));
    let response = agent
        .post(&url)
        .send_json(serde_json::json!({
            "chat_id": chat_id,
            "message_id": message_id_value,
            "text": text,
            "disable_web_page_preview": true
        }))
        .map_err(telegram_http_error)?;
    let value: serde_json::Value = response.into_json().map_err(|err| err.to_string())?;
    if value.get("ok").and_then(serde_json::Value::as_bool) != Some(true) {
        return Err(format!(
            "Telegram editMessageText returned non-ok response: {value}"
        ));
    }
    Ok(value
        .get("result")
        .and_then(|result| result.get("message_id"))
        .and_then(telegram_id_string))
}

fn telegram_send_chat_action(token: &str, chat_id: &str, action: &str) -> Result<(), String> {
    let url = format!("https://api.telegram.org/bot{token}/sendChatAction");
    let agent = channel_http_short_agent();
    let response = agent
        .post(&url)
        .send_json(serde_json::json!({
            "chat_id": chat_id,
            "action": action
        }))
        .map_err(telegram_http_error)?;
    let value: serde_json::Value = response.into_json().map_err(|err| err.to_string())?;
    if value.get("ok").and_then(serde_json::Value::as_bool) != Some(true) {
        return Err(format!(
            "Telegram sendChatAction returned non-ok response: {value}"
        ));
    }
    Ok(())
}

fn telegram_id_string(value: &serde_json::Value) -> Option<String> {
    value
        .as_i64()
        .map(|id| id.to_string())
        .or_else(|| value.as_u64().map(|id| id.to_string()))
        .or_else(|| value.as_str().map(ToString::to_string))
}

fn multipart_post_json(
    service: &str,
    url: &str,
    auth_header: Option<(&str, &str)>,
    fields: &[(String, String)],
    file_field: &str,
    attachment: &ChannelOutboundAttachment,
) -> Result<serde_json::Value, String> {
    let file_bytes = fs::read(&attachment.path).map_err(|err| {
        format!(
            "{service} attachment file could not be read at {}: {err}",
            attachment.path.display()
        )
    })?;
    let filename = attachment_filename(attachment)?;
    let mime = attachment_mime(attachment);
    let boundary = format!(
        "agent-harness-{}-{}",
        std::process::id(),
        current_time_ms().unwrap_or(0)
    );
    let mut body = Vec::new();
    for (name, value) in fields {
        push_multipart_field(&mut body, &boundary, name, value);
    }
    push_multipart_file(
        &mut body,
        &boundary,
        file_field,
        &filename,
        &mime,
        &file_bytes,
    );
    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());

    let agent = channel_http_short_agent();
    let content_type = format!("multipart/form-data; boundary={boundary}");
    let mut request = agent.post(url).set("Content-Type", &content_type);
    if let Some((name, value)) = auth_header {
        request = request.set(name, value);
    }
    let response = request
        .send_bytes(&body)
        .map_err(|error| multipart_http_error(service, error))?;
    response.into_json().map_err(|err| err.to_string())
}

fn push_multipart_field(body: &mut Vec<u8>, boundary: &str, name: &str, value: &str) {
    body.extend_from_slice(
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"{}\"\r\n\r\n{}\r\n",
            multipart_escape(name),
            value
        )
        .as_bytes(),
    );
}

fn push_multipart_file(
    body: &mut Vec<u8>,
    boundary: &str,
    field_name: &str,
    filename: &str,
    mime: &str,
    bytes: &[u8],
) {
    body.extend_from_slice(
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"{}\"; filename=\"{}\"\r\nContent-Type: {mime}\r\n\r\n",
            multipart_escape(field_name),
            multipart_escape(filename)
        )
        .as_bytes(),
    );
    body.extend_from_slice(bytes);
    body.extend_from_slice(b"\r\n");
}

fn multipart_escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\r', "")
        .replace('\n', "")
}

fn attachment_filename(attachment: &ChannelOutboundAttachment) -> Result<String, String> {
    attachment
        .filename
        .clone()
        .or_else(|| {
            attachment
                .path
                .file_name()
                .map(|name| name.to_string_lossy().to_string())
        })
        .filter(|filename| !filename.trim().is_empty())
        .ok_or_else(|| {
            format!(
                "attachment path {} does not include a filename",
                attachment.path.display()
            )
        })
}

fn attachment_mime(attachment: &ChannelOutboundAttachment) -> String {
    attachment
        .mime
        .clone()
        .or_else(|| attachment_mime_from_path(&attachment.path))
        .unwrap_or_else(|| "application/octet-stream".to_string())
}

fn attachment_mime_from_path(path: &Path) -> Option<String> {
    match path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(|extension| extension.to_ascii_lowercase())
        .as_deref()
    {
        Some("jpg" | "jpeg") => Some("image/jpeg".to_string()),
        Some("png") => Some("image/png".to_string()),
        Some("gif") => Some("image/gif".to_string()),
        Some("webp") => Some("image/webp".to_string()),
        Some("pdf") => Some("application/pdf".to_string()),
        Some("json") => Some("application/json".to_string()),
        Some("txt" | "md" | "log") => Some("text/plain".to_string()),
        _ => None,
    }
}

fn multipart_http_error(service: &str, error: ureq::Error) -> String {
    match error {
        ureq::Error::Status(code, response) => {
            let body = response.into_string().unwrap_or_default();
            format!("{service} HTTP status {code}: {body}")
        }
        ureq::Error::Transport(error) => format!("{service} transport error: {error}"),
    }
}

fn telegram_http_error(error: ureq::Error) -> String {
    match error {
        ureq::Error::Status(code, response) => {
            let body = response.into_string().unwrap_or_default();
            format!("Telegram HTTP status {code}: {body}")
        }
        ureq::Error::Transport(error) => format!("Telegram transport error: {error}"),
    }
}

fn discord_bot_token(harness_home: &Path, account_id: Option<&str>) -> Result<String, String> {
    let account_id = account_id
        .map(str::trim)
        .filter(|account_id| !account_id.is_empty());
    let env_name = account_id
        .filter(|account_id| *account_id != "default")
        .map(discord_account_token_env_name)
        .unwrap_or_else(|| "DISCORD_BOT_TOKEN".to_string());
    env::var(&env_name)
        .ok()
        .or_else(|| secret_env_value(harness_home, &env_name))
        .map(|token| normalize_discord_bot_token(&token))
        .ok_or_else(|| {
            format!("{env_name} is required for Discord adapters; run channel-credentials-export or set the env var")
        })
}

fn discord_account_token_env_name(account_id: &str) -> String {
    format!(
        "AGENT_HARNESS_DISCORD_ACCOUNT_{}_BOT_TOKEN",
        sanitize_env_suffix(account_id)
    )
}

fn discord_account_id(account_id: Option<&str>) -> String {
    account_id
        .map(str::trim)
        .filter(|account_id| !account_id.is_empty())
        .unwrap_or("default")
        .to_ascii_lowercase()
}

fn normalize_discord_bot_token(token: &str) -> String {
    let token = token.trim();
    if token
        .get(..4)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("bot "))
    {
        token[4..].trim().to_string()
    } else {
        token.to_string()
    }
}

const DISCORD_MESSAGE_CONTENT_LIMIT: usize = 2_000;

fn discord_send_outbound_message(
    token: &str,
    message: &ChannelOutboundMessage,
) -> Result<Option<String>, String> {
    let mut provider_message_ids = Vec::new();
    let text = format_channel_reply_text(&message.text);
    if !text.trim().is_empty() {
        if let Some(provider_message_id) = discord_send_message_chunks(
            token,
            &message.channel_id,
            &text,
            discord_message_reference(message),
        )? {
            provider_message_ids.push(provider_message_id);
        }
    }
    for attachment in &message.attachments {
        if let Some(provider_message_id) =
            discord_send_attachment(token, &message.channel_id, attachment)?
        {
            provider_message_ids.push(provider_message_id);
        }
    }
    if provider_message_ids.is_empty() && message.attachments.is_empty() {
        if let Some(provider_message_id) = discord_send_message_chunks(
            token,
            &message.channel_id,
            &text,
            discord_message_reference(message),
        )? {
            provider_message_ids.push(provider_message_id);
        }
    }
    Ok((!provider_message_ids.is_empty()).then(|| provider_message_ids.join(",")))
}

fn discord_message_reference(message: &ChannelOutboundMessage) -> Option<serde_json::Value> {
    let intent = message.delivery_intent.as_ref()?;
    if !intent.validated || intent.kind != ChannelDeliveryIntentKind::ReplyToMessage {
        return None;
    }
    let message_id = intent.platform_message_id.as_deref()?;
    let channel_id = intent
        .platform_channel_id
        .as_deref()
        .unwrap_or(&message.channel_id);
    if channel_id != message.channel_id {
        return None;
    }
    Some(serde_json::json!({
        "message_id": message_id,
        "channel_id": channel_id,
        "fail_if_not_exists": false
    }))
}

fn discord_send_message_chunks(
    token: &str,
    channel_id: &str,
    text: &str,
    message_reference: Option<serde_json::Value>,
) -> Result<Option<String>, String> {
    let chunks = discord_message_chunks(text, DISCORD_MESSAGE_CONTENT_LIMIT);
    let mut provider_message_ids = Vec::new();
    for (index, chunk) in chunks.into_iter().enumerate() {
        let reference = (index == 0).then(|| message_reference.clone()).flatten();
        if let Some(provider_message_id) =
            discord_send_message(token, channel_id, &chunk, reference)?
        {
            provider_message_ids.push(provider_message_id);
        }
    }
    Ok((!provider_message_ids.is_empty()).then(|| provider_message_ids.join(",")))
}

fn discord_message_chunks(text: &str, max_chars: usize) -> Vec<String> {
    if text.is_empty() {
        return vec![String::new()];
    }
    if text.chars().count() <= max_chars {
        return vec![text.to_string()];
    }

    let mut chunks = Vec::new();
    let mut current = String::new();
    let mut current_chars = 0usize;
    for segment in text.split_inclusive('\n') {
        let segment_chars = segment.chars().count();
        if segment_chars > max_chars {
            if !current.is_empty() {
                chunks.push(std::mem::take(&mut current));
                current_chars = 0;
            }
            for ch in segment.chars() {
                current.push(ch);
                current_chars += 1;
                if current_chars == max_chars {
                    chunks.push(std::mem::take(&mut current));
                    current_chars = 0;
                }
            }
        } else if current_chars + segment_chars <= max_chars {
            current.push_str(segment);
            current_chars += segment_chars;
        } else {
            chunks.push(std::mem::take(&mut current));
            current.push_str(segment);
            current_chars = segment_chars;
        }
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

fn discord_send_message(
    token: &str,
    channel_id: &str,
    text: &str,
    message_reference: Option<serde_json::Value>,
) -> Result<Option<String>, String> {
    if text.chars().count() > DISCORD_MESSAGE_CONTENT_LIMIT {
        return Err("Discord message exceeds the 2000 character content limit".to_string());
    }
    let url = format!("https://discord.com/api/v10/channels/{channel_id}/messages");
    let token = normalize_discord_bot_token(token);
    let auth = format!("Bot {token}");
    let agent = channel_http_short_agent();
    let mut payload = serde_json::json!({
        "content": text,
        "allowed_mentions": {
            "parse": []
        }
    });
    if let Some(reference) = message_reference {
        payload["message_reference"] = reference;
    }
    let response = agent
        .post(&url)
        .set("Authorization", &auth)
        .set("Content-Type", "application/json")
        .send_json(payload)
        .map_err(discord_http_error)?;
    let value: serde_json::Value = response.into_json().map_err(|err| err.to_string())?;
    Ok(value
        .get("id")
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string))
}

fn discord_send_attachment(
    token: &str,
    channel_id: &str,
    attachment: &ChannelOutboundAttachment,
) -> Result<Option<String>, String> {
    let url = format!("https://discord.com/api/v10/channels/{channel_id}/messages");
    let token = normalize_discord_bot_token(token);
    let auth = format!("Bot {token}");
    let filename = attachment_filename(attachment)?;
    let payload = serde_json::json!({
        "content": "",
        "allowed_mentions": {
            "parse": []
        },
        "attachments": [
            {
                "id": 0,
                "filename": filename
            }
        ]
    });
    let fields = vec![("payload_json".to_string(), payload.to_string())];
    let value = multipart_post_json(
        "Discord",
        &url,
        Some(("Authorization", auth.as_str())),
        &fields,
        "files[0]",
        attachment,
    )?;
    Ok(value
        .get("id")
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string))
}

fn discord_edit_message(
    token: &str,
    channel_id: &str,
    message_id: &str,
    text: &str,
) -> Result<Option<String>, String> {
    if text.chars().count() > DISCORD_MESSAGE_CONTENT_LIMIT {
        return Err("Discord message exceeds the 2000 character content limit".to_string());
    }
    let url = format!("https://discord.com/api/v10/channels/{channel_id}/messages/{message_id}");
    let token = normalize_discord_bot_token(token);
    let auth = format!("Bot {token}");
    let agent = channel_http_short_agent();
    let response = agent
        .patch(&url)
        .set("Authorization", &auth)
        .set("Content-Type", "application/json")
        .send_json(serde_json::json!({
            "content": text,
            "allowed_mentions": {
                "parse": []
            }
        }))
        .map_err(discord_http_error)?;
    let value: serde_json::Value = response.into_json().map_err(|err| err.to_string())?;
    Ok(value
        .get("id")
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string))
}

fn discord_create_dm_channel(token: &str, user_id: &str) -> Result<String, String> {
    let token = normalize_discord_bot_token(token);
    let auth = format!("Bot {token}");
    let agent = channel_http_short_agent();
    let response = agent
        .post("https://discord.com/api/v10/users/@me/channels")
        .set("Authorization", &auth)
        .set("Content-Type", "application/json")
        .send_json(serde_json::json!({
            "recipient_id": user_id
        }))
        .map_err(discord_http_error)?;
    let value: serde_json::Value = response.into_json().map_err(|err| err.to_string())?;
    value
        .get("id")
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| "Discord create DM response did not include channel id".to_string())
}

fn discord_fetch_channel_messages(
    token: &str,
    channel_id: &str,
    limit: usize,
) -> Result<Vec<DiscordMessageSummary>, String> {
    let limit = limit.clamp(1, 100);
    let url = format!("https://discord.com/api/v10/channels/{channel_id}/messages?limit={limit}");
    let token = normalize_discord_bot_token(token);
    let auth = format!("Bot {token}");
    let agent = channel_http_short_agent();
    let response = agent
        .get(&url)
        .set("Authorization", &auth)
        .call()
        .map_err(discord_http_error)?;
    let value: serde_json::Value = response.into_json().map_err(|err| err.to_string())?;
    let messages = value
        .as_array()
        .ok_or_else(|| "Discord messages response was not an array".to_string())?;
    Ok(messages
        .iter()
        .filter_map(discord_message_summary)
        .collect())
}

fn discord_message_summary(value: &serde_json::Value) -> Option<DiscordMessageSummary> {
    let message_id = value.get("id")?.as_str()?.to_string();
    let author = value.get("author");
    let content_length = value
        .get("content")
        .and_then(serde_json::Value::as_str)
        .map(str::len);
    Some(DiscordMessageSummary {
        message_id,
        author_id: author
            .and_then(|author| author.get("id"))
            .and_then(serde_json::Value::as_str)
            .map(ToString::to_string),
        author_bot: author
            .and_then(|author| author.get("bot"))
            .and_then(serde_json::Value::as_bool),
        timestamp: value
            .get("timestamp")
            .and_then(serde_json::Value::as_str)
            .map(ToString::to_string),
        content_length,
    })
}

fn discord_send_typing(token: &str, channel_id: &str) -> Result<(), String> {
    let url = format!("https://discord.com/api/v10/channels/{channel_id}/typing");
    let token = normalize_discord_bot_token(token);
    let auth = format!("Bot {token}");
    let agent = channel_http_short_agent();
    agent
        .post(&url)
        .set("Authorization", &auth)
        .call()
        .map_err(discord_http_error)?;
    Ok(())
}

fn discord_http_error(error: ureq::Error) -> String {
    match error {
        ureq::Error::Status(code, response) => {
            let body = response.into_string().unwrap_or_default();
            format!("Discord HTTP status {code}: {body}")
        }
        ureq::Error::Transport(error) => format!("Discord transport error: {error}"),
    }
}

fn parse_conflict_policy(value: &str) -> Result<ConflictPolicy, String> {
    match value {
        "skip" => Ok(ConflictPolicy::Skip),
        "overwrite" => Ok(ConflictPolicy::Overwrite),
        "rename" => Ok(ConflictPolicy::Rename),
        other => Err(format!(
            "unknown conflict policy: {other}; expected skip, overwrite, or rename"
        )),
    }
}

fn parse_delivery_status(value: &str) -> Result<ChannelDeliveryStatus, String> {
    match value {
        "delivered" => Ok(ChannelDeliveryStatus::Delivered),
        "failed" => Ok(ChannelDeliveryStatus::Failed),
        other => Err(format!(
            "unknown delivery status: {other}; expected delivered or failed"
        )),
    }
}

fn parse_limit(value: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| format!("invalid limit: {value}; expected a positive integer"))
}

fn parse_usize(value: &str, flag: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .map_err(|_| format!("{flag} requires a non-negative integer, got: {value}"))
}

fn parse_i64(value: &str, flag: &str) -> Result<i64, String> {
    value
        .parse::<i64>()
        .map_err(|_| format!("{flag} requires a signed integer, got: {value}"))
}

fn parse_u64(value: &str, flag: &str) -> Result<u64, String> {
    value
        .parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| format!("{flag} requires a positive integer, got: {value}"))
}

fn required_arg<'a>(args: &'a [String], index: usize, flag: &str) -> Result<&'a str, String> {
    args.get(index)
        .map(String::as_str)
        .ok_or_else(|| format!("{flag} requires a value"))
}

fn parse_json_arg(value: &str, flag: &str) -> Result<serde_json::Value, String> {
    serde_json::from_str(value).map_err(|err| format!("{flag} must be JSON: {err}"))
}

fn read_json_arg_file(path: &str) -> Result<serde_json::Value, String> {
    let text = fs::read_to_string(path)
        .map_err(|err| format!("failed to read JSON file {path}: {err}"))?;
    serde_json::from_str(&text).map_err(|err| format!("invalid JSON file {path}: {err}"))
}

fn print_json(value: &impl Serialize) -> Result<(), String> {
    println!(
        "{}",
        serde_json::to_string_pretty(value).map_err(|err| err.to_string())?
    );
    Ok(())
}

fn current_time_ms() -> Result<i64, String> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("system time is before Unix epoch: {error}"))?;
    i64::try_from(duration.as_millis())
        .map_err(|_| "current epoch milliseconds exceed i64".to_string())
}

fn repair_jsonl_file(args: &JsonlRepairArgs) -> Result<JsonlRepairReport, String> {
    let text = fs::read_to_string(&args.path)
        .map_err(|err| format!("failed to read {}: {err}", args.path.display()))?;
    let output = args
        .output
        .clone()
        .unwrap_or_else(|| args.path.with_extension("repaired.jsonl"));
    let invalid_output = args
        .invalid_output
        .clone()
        .unwrap_or_else(|| args.path.with_extension("invalid.jsonl"));
    let mut repaired_lines = Vec::new();
    let mut invalid_records = Vec::new();
    let mut total_lines = 0usize;
    let mut valid_lines = 0usize;
    let mut recovered_lines = 0usize;
    let mut recovered_values = 0usize;

    for (index, line) in text.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        total_lines += 1;
        if serde_json::from_str::<serde_json::Value>(trimmed).is_ok() {
            valid_lines += 1;
            repaired_lines.push(trimmed.to_string());
            continue;
        }
        match recover_jsonl_values(trimmed) {
            Ok(values) => {
                recovered_lines += 1;
                recovered_values += values.len();
                repaired_lines.extend(values);
            }
            Err(error) => invalid_records.push(serde_json::json!({
                "lineNumber": index + 1,
                "error": error,
                "raw": line,
            })),
        }
    }

    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent).map_err(|err| err.to_string())?;
    }
    if let Some(parent) = invalid_output.parent() {
        fs::create_dir_all(parent).map_err(|err| err.to_string())?;
    }
    let repaired_text = if repaired_lines.is_empty() {
        String::new()
    } else {
        format!("{}\n", repaired_lines.join("\n"))
    };
    fs::write(&output, &repaired_text)
        .map_err(|err| format!("failed to write {}: {err}", output.display()))?;
    let invalid_text = invalid_records
        .iter()
        .map(|record| serde_json::to_string(record).map_err(|err| err.to_string()))
        .collect::<Result<Vec<_>, _>>()?
        .join("\n");
    fs::write(
        &invalid_output,
        if invalid_text.is_empty() {
            String::new()
        } else {
            format!("{invalid_text}\n")
        },
    )
    .map_err(|err| format!("failed to write {}: {err}", invalid_output.display()))?;

    let backup = if args.apply {
        let backup = args
            .path
            .with_extension(format!("bak-{}.jsonl", current_time_ms()?));
        fs::copy(&args.path, &backup).map_err(|err| {
            format!(
                "failed to write backup {} from {}: {err}",
                backup.display(),
                args.path.display()
            )
        })?;
        fs::write(&args.path, repaired_text)
            .map_err(|err| format!("failed to replace {}: {err}", args.path.display()))?;
        Some(backup)
    } else {
        None
    };

    Ok(JsonlRepairReport {
        path: args.path.clone(),
        output,
        invalid_output,
        backup,
        total_lines,
        valid_lines,
        output_lines: repaired_lines.len(),
        recovered_lines,
        recovered_values,
        invalid_lines: invalid_records.len(),
        applied: args.apply,
    })
}

fn recover_jsonl_values(line: &str) -> Result<Vec<String>, String> {
    let mut recovered = Vec::new();
    for value in serde_json::Deserializer::from_str(line).into_iter::<serde_json::Value>() {
        let value = value.map_err(|err| err.to_string())?;
        recovered.push(serde_json::to_string(&value).map_err(|err| err.to_string())?);
    }
    if recovered.len() < 2 {
        return Err("line does not contain multiple complete JSON values".to_string());
    }
    Ok(recovered)
}

#[derive(Debug, Clone)]
struct RuntimeTypingContext {
    agent_id: String,
    platform: String,
    channel_id: String,
}

struct TypingHeartbeat {
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl Drop for TypingHeartbeat {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn start_runtime_typing_heartbeat(
    harness_home: &Path,
    queue_id: Option<&str>,
) -> Option<TypingHeartbeat> {
    let context = pending_runtime_typing_context(harness_home, queue_id)
        .ok()
        .flatten()?;
    let sender = runtime_typing_sender(harness_home, &context)?;
    let stop = Arc::new(AtomicBool::new(false));
    let thread_stop = Arc::clone(&stop);
    let handle = thread::spawn(move || {
        while !thread_stop.load(Ordering::SeqCst) {
            sender();
            for _ in 0..40 {
                if thread_stop.load(Ordering::SeqCst) {
                    return;
                }
                thread::sleep(Duration::from_millis(100));
            }
        }
    });
    Some(TypingHeartbeat {
        stop,
        handle: Some(handle),
    })
}

fn runtime_typing_sender(
    harness_home: &Path,
    context: &RuntimeTypingContext,
) -> Option<Box<dyn Fn() + Send + 'static>> {
    match context.platform.as_str() {
        "telegram" => {
            let token = if context.agent_id == "main" || context.agent_id == "default" {
                telegram_bot_token(harness_home, None).ok()
            } else {
                telegram_bot_token(harness_home, Some(&context.agent_id)).ok()
            }?;
            let chat_id = context.channel_id.clone();
            Some(Box::new(move || {
                let _ = telegram_send_chat_action(&token, &chat_id, "typing");
            }))
        }
        "discord" => {
            let token = discord_bot_token(harness_home, None).ok()?;
            let channel_id = context.channel_id.clone();
            Some(Box::new(move || {
                let _ = discord_send_typing(&token, &channel_id);
            }))
        }
        _ => None,
    }
}

fn pending_runtime_typing_context(
    harness_home: &Path,
    requested_queue_id: Option<&str>,
) -> Result<Option<RuntimeTypingContext>, String> {
    let queue_dir = harness_home.join("state").join("runtime-queue");
    let pending_file = queue_dir.join("pending.jsonl");
    if !pending_file.is_file() {
        return Ok(None);
    }
    let terminal_ids = terminal_runtime_queue_ids(&queue_dir.join("run-once-receipts.jsonl"))?;
    let text = fs::read_to_string(&pending_file)
        .map_err(|err| format!("failed to read {}: {err}", pending_file.display()))?;
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let value: serde_json::Value = match serde_json::from_str(trimmed) {
            Ok(value) => value,
            Err(_) => continue,
        };
        let queue_id = json_string_field(&value, &["queueId", "queue_id"]);
        if let Some(requested_queue_id) = requested_queue_id
            && queue_id != Some(requested_queue_id)
        {
            continue;
        }
        if json_string_field(&value, &["status"]) != Some("queued") {
            continue;
        }
        if queue_id.is_some_and(|queue_id| terminal_ids.contains(queue_id)) {
            continue;
        }
        let Some(agent_id) = json_string_field(&value, &["agentId", "agent_id"]) else {
            continue;
        };
        let Some(platform) = json_string_field(&value, &["platform"]) else {
            continue;
        };
        let Some(channel_id) = json_string_field(&value, &["channelId", "channel_id"]) else {
            continue;
        };
        return Ok(Some(RuntimeTypingContext {
            agent_id: agent_id.to_string(),
            platform: platform.to_string(),
            channel_id: channel_id.to_string(),
        }));
    }
    Ok(None)
}

fn terminal_runtime_queue_ids(path: &Path) -> Result<BTreeSet<String>, String> {
    let mut ids = BTreeSet::new();
    if !path.is_file() {
        return Ok(ids);
    }
    let text = fs::read_to_string(path)
        .map_err(|err| format!("failed to read {}: {err}", path.display()))?;
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let value: serde_json::Value = match serde_json::from_str(trimmed) {
            Ok(value) => value,
            Err(_) => continue,
        };
        if let Some(queue_id) = json_string_field(&value, &["queueId", "queue_id"])
            && json_string_field(&value, &["status"]).is_some_and(is_runtime_terminal_status)
        {
            ids.insert(queue_id.to_string());
        }
    }
    Ok(ids)
}

fn is_runtime_terminal_status(status: &str) -> bool {
    matches!(
        status,
        "completed" | "timeout" | "failed-terminal" | "canceled" | "skipped" | "dead-letter"
    )
}

fn json_string_field<'a>(value: &'a serde_json::Value, names: &[&str]) -> Option<&'a str> {
    names.iter().find_map(|name| value.get(*name)?.as_str())
}

fn stop_file_requested(stop_file: Option<&Path>) -> bool {
    stop_file.is_some_and(Path::exists)
}

fn append_loop_stop_log(
    harness_home: &Path,
    component: &str,
    event: &str,
    iterations: usize,
    reason: &str,
) -> Result<(), String> {
    append_harness_log(
        harness_home,
        &HarnessLogEvent::new(
            current_log_time_ms().map_err(|err| err.to_string())?,
            HarnessLogLevel::Info,
            component,
            event,
            format!("iterations={iterations} reason={reason}"),
        ),
    )
    .map(|_| ())
    .map_err(|err| err.to_string())
}

fn write_loop_heartbeat(
    harness_home: &Path,
    name: &str,
    status: &str,
    iteration: usize,
    detail: &str,
) -> Result<(), String> {
    let dir = harness_home
        .join("state")
        .join("supervisor")
        .join("loop-heartbeats");
    fs::create_dir_all(&dir).map_err(|err| err.to_string())?;
    let at_ms = current_time_ms()?;
    let process_id = std::process::id();
    let started_at_ms = env_i64("AGENT_HARNESS_SERVICE_STARTED_AT_MS")
        .unwrap_or_else(|| *LOOP_PROCESS_STARTED_AT_MS.get_or_init(|| at_ms));
    let generation_id = env::var("AGENT_HARNESS_SERVICE_GENERATION_ID")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| loop_generation_id(name, process_id, started_at_ms));
    let heartbeat = serde_json::json!({
        "schema": "agent-harness.loop-heartbeat.v1",
        "name": name,
        "serviceId": name,
        "serviceKind": loop_service_kind(name),
        "generationId": generation_id,
        "status": status,
        "iteration": iteration,
        "detail": detail,
        "atMs": at_ms,
        "processId": process_id,
    });
    let file = dir.join(format!("{name}.json"));
    write_json_atomic(&file, &heartbeat)
        .map_err(|err| format!("failed to write loop heartbeat {}: {err}", file.display()))?;
    write_supervisor_service_state(
        harness_home,
        name,
        status,
        iteration,
        detail,
        at_ms,
        process_id,
        started_at_ms,
        &generation_id,
        &file,
    )
}

static LOOP_PROCESS_STARTED_AT_MS: OnceLock<i64> = OnceLock::new();

fn loop_generation_id(name: &str, process_id: u32, started_at_ms: i64) -> String {
    format!("{name}-{process_id}-{started_at_ms}")
}

fn loop_service_kind(name: &str) -> &'static str {
    match name {
        "runtime-loop" => "runtime",
        "worker-loop" => "worker",
        "cron-scheduler-loop" => "cron",
        "progress-delivery-loop" => "progress-delivery",
        "telegram-loop" => "telegram-ingress",
        "discord-outbox-loop" => "final-outbox",
        "discord-gateway-loop" => "discord-gateway",
        _ if name.starts_with("telegram-loop") => "telegram-ingress",
        _ => "loop",
    }
}

fn loop_status_success_timestamp(status: &str, at_ms: i64) -> Option<i64> {
    let lowered = status.to_ascii_lowercase();
    if matches!(lowered.as_str(), "stopped" | "stopping" | "closed")
        || lowered.contains("error")
        || lowered.contains("fail")
    {
        None
    } else {
        Some(at_ms)
    }
}

fn env_i64(name: &str) -> Option<i64> {
    env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<i64>().ok())
}

fn env_bool(name: &str) -> Option<bool> {
    env::var(name).ok().and_then(|value| {
        let normalized = value.trim().to_ascii_lowercase();
        match normalized.as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        }
    })
}

#[allow(clippy::too_many_arguments)]
fn write_supervisor_service_state(
    harness_home: &Path,
    name: &str,
    status: &str,
    iteration: usize,
    detail: &str,
    at_ms: i64,
    process_id: u32,
    started_at_ms: i64,
    generation_id: &str,
    heartbeat_file: &Path,
) -> Result<(), String> {
    let dir = harness_home
        .join("state")
        .join("supervisor")
        .join("services");
    fs::create_dir_all(&dir).map_err(|err| err.to_string())?;
    let launch_owner = env::var("AGENT_HARNESS_SUPERVISOR_LAUNCH_OWNER")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "external-runner-observe-only".to_string());
    let observed_only = env_bool("AGENT_HARNESS_SUPERVISOR_OBSERVED_ONLY").unwrap_or(true);
    let supervisor_pid = env_i64("AGENT_HARNESS_SUPERVISOR_PARENT_PID");
    let service = serde_json::json!({
        "schema": "agent-harness.supervisor-service-state.v1",
        "serviceId": name,
        "serviceKind": loop_service_kind(name),
        "generationId": generation_id,
        "pid": process_id,
        "processId": process_id,
        "processStartTimeMs": started_at_ms,
        "startedAtMs": started_at_ms,
        "lastHeartbeatAtMs": at_ms,
        "lastSuccessfulIterationAtMs": loop_status_success_timestamp(status, at_ms),
        "iteration": iteration,
        "status": status,
        "desiredState": "running",
        "actualState": status,
        "detail": detail,
        "heartbeatFile": heartbeat_file.display().to_string(),
        "launchOwner": launch_owner,
        "observedOnly": observed_only,
        "supervisorPid": supervisor_pid,
    });
    let file = dir.join(format!("{name}.json"));
    write_json_atomic(&file, &service).map_err(|err| {
        format!(
            "failed to write supervisor service state {}: {err}",
            file.display()
        )
    })
}

fn format_status(status: &ImportPhaseStatus) -> &'static str {
    match status {
        ImportPhaseStatus::Ready => "ready",
        ImportPhaseStatus::Missing => "missing",
        ImportPhaseStatus::Deferred => "deferred",
    }
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

fn optional_bool(value: Option<bool>) -> &'static str {
    match value {
        Some(true) => "yes",
        Some(false) => "no",
        None => "-",
    }
}

fn display_opt_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string())
}

fn fmt_optional_i64(value: Option<i64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string())
}

fn count_summary(map: &BTreeMap<String, usize>) -> String {
    if map.is_empty() {
        return "-".to_string();
    }
    map.iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn print_dry_run_summary(report: &ImportReport) {
    println!("Agent source import dry run");
    println!("Source home: {}", report.source_home.display());
    println!("Source workspace: {}", report.source_workspace.display());
    println!("Target home: {}", report.destination_home.display());
    println!("Conflict policy: {:?}", report.conflict_policy);
    println!("Items: {}", report.summary.total_items);
    println!("Planned: {}", report.summary.planned);
    println!("Already matches: {}", report.summary.already_matches);
    println!("Conflicts: {}", report.summary.conflicts);
    println!("Missing: {}", report.summary.missing);
    println!();
    println!("Semantic summary:");
    println!("Config parsed: {}", yes_no(report.semantics.config.parsed));
    println!("Configured agents: {}", report.semantics.config.agent_count);
    println!("Providers: {}", report.semantics.config.provider_count);
    println!("Plugins: {}", report.semantics.config.plugin_count);
    println!(
        "Telegram configured: {}",
        yes_no(report.semantics.config.telegram_configured)
    );
    println!(
        "Discord configured: {}",
        yes_no(report.semantics.config.discord_configured)
    );
    println!(
        "Session indexes parsed: {}",
        report.semantics.sessions.parsed_indexes
    );
    println!(
        "Session records: {}",
        report.semantics.sessions.total_records
    );
    println!(
        "Native cron jobs parsed: {}",
        yes_no(report.semantics.native_cron.parsed_jobs_file)
    );
    println!(
        "Native cron jobs: {}",
        report.semantics.native_cron.total_jobs
    );
    println!(
        "Native cron enabled jobs: {}",
        report.semantics.native_cron.enabled_jobs
    );
    print_counts(
        "Cron jobs by agent",
        &report.semantics.native_cron.jobs_by_agent,
    );

    if report.summary.conflicts > 0 {
        println!();
        println!("Conflicts:");
        for item in report
            .items
            .iter()
            .filter(|item| item.status == agent_harness_core::ImportItemStatus::Conflict)
        {
            println!(
                "- {:?}: {} -> {} ({})",
                item.kind,
                item.source.display(),
                item.destination.display(),
                item.reason
            );
        }
    }
}

fn print_counts(label: &str, counts: &std::collections::BTreeMap<String, usize>) {
    if counts.is_empty() {
        return;
    }

    println!("{label}:");
    for (key, count) in counts.iter().take(8) {
        println!("  {key}: {count}");
    }
    if counts.len() > 8 {
        println!("  ... {} more", counts.len() - 8);
    }
}

fn print_registry(registry: &AgentRegistry) {
    println!("Agent source registry");
    println!("Source home: {}", registry.source_home.display());
    println!("Source workspace: {}", registry.source_workspace.display());
    println!("Config found: {}", yes_no(registry.config_found));
    println!("Config parsed: {}", yes_no(registry.config_parsed));
    if let Some(error) = &registry.config_parse_error {
        println!("Config parse error: {error}");
    }
    println!("Agents: {}", registry.agents.len());
    println!("Providers: {}", registry.providers.len());
    println!("Plugins: {}", registry.plugins.len());
    println!(
        "Telegram configured: {}",
        yes_no(registry.channels.telegram)
    );
    println!("Discord configured: {}", yes_no(registry.channels.discord));

    if !registry.agents.is_empty() {
        println!();
        println!("Agents:");
        for agent in &registry.agents {
            println!(
                "- {} [{:?}] provider={} model={} workspace={} dir={} sessions={} auth={} models={}",
                agent.id,
                agent.source,
                agent.provider.as_deref().unwrap_or("-"),
                agent.model.as_deref().unwrap_or("-"),
                agent.workspace.as_deref().unwrap_or("-"),
                yes_no(agent.directory_exists),
                yes_no(agent.sessions_index_exists),
                yes_no(agent.auth_file || agent.auth_profiles_file || agent.auth_state_file),
                yes_no(agent.local_models_file),
            );
        }
    }

    if !registry.providers.is_empty() {
        println!();
        println!("Providers:");
        for provider in &registry.providers {
            println!(
                "- {} [{}] base_url={} api_key_ref={}",
                provider.id,
                provider.source,
                yes_no(provider.has_base_url),
                yes_no(provider.has_api_key_reference)
            );
        }
    }

    if !registry.plugins.is_empty() {
        println!();
        println!("Plugins:");
        for plugin in &registry.plugins {
            println!(
                "- {} [{}] enabled={} memory={} channel={}",
                plugin.id,
                plugin.source,
                plugin.enabled.map(yes_no).unwrap_or("unknown"),
                yes_no(plugin.memory_related),
                yes_no(plugin.channel_related)
            );
        }
    }

    if !registry.warnings.is_empty() {
        println!();
        println!("Warnings:");
        for warning in &registry.warnings {
            println!("- {warning}");
        }
    }
}

fn print_skill_index(index: &SkillIndex) {
    println!("Agent skill index");
    println!("Origin: {:?}", index.origin);
    if let Some(home) = &index.source_home {
        println!("Source home: {}", home.display());
    }
    if let Some(workspace) = &index.source_workspace {
        println!("Source workspace: {}", workspace.display());
    }
    if let Some(harness_home) = &index.harness_home {
        println!("Harness home: {}", harness_home.display());
    }
    println!("Skills: {}", index.summary.total_skills);
    println!("Workspace skills: {}", index.summary.workspace_skills);
    println!("Managed skills: {}", index.summary.managed_skills);
    println!(
        "Project .agents skills: {}",
        index.summary.project_agent_skills
    );
    println!(
        "Imported workspace skills: {}",
        index.summary.imported_workspace_skills
    );
    println!(
        "Imported managed skills: {}",
        index.summary.imported_managed_skills
    );
    println!(
        "Imported project .agents skills: {}",
        index.summary.imported_project_agent_skills
    );
    println!(
        "Harness builtin skills: {}",
        index.summary.harness_builtin_skills
    );
    println!("Skills with scripts: {}", index.summary.skills_with_scripts);

    if !index.skills.is_empty() {
        println!();
        println!("Skills:");
        for skill in &index.skills {
            println!(
                "- {} [{:?}] title={} files={} refs={} templates={} scripts={} assets={}",
                skill.id,
                skill.source_kind,
                skill.title,
                skill.file_count,
                yes_no(skill.has_references),
                yes_no(skill.has_templates),
                yes_no(skill.has_scripts),
                yes_no(skill.has_assets),
            );
        }
    }
}

fn print_builtin_harness_skill_sync_report(report: &BuiltinHarnessSkillSyncReport) {
    println!("Agent builtin harness skill sync");
    println!("Harness home: {}", report.harness_home.display());
    println!("Manifest: {}", report.manifest_file.display());
    println!(
        "Summary: written={} current={} skipped_user_modified={}",
        report.summary.written,
        report.summary.already_current,
        report.summary.skipped_user_modified
    );
    if !report.receipts.is_empty() {
        println!("Receipts:");
        for receipt in &report.receipts {
            println!(
                "- {:?} {} {} ({})",
                receipt.status,
                receipt.skill_id,
                receipt.path.display(),
                receipt.reason
            );
        }
    }
}

fn print_windows_supervisor_plan_report(report: &WindowsSupervisorPlanReport) {
    println!("Agent Windows supervisor plan");
    println!("Harness home: {}", report.harness_home.display());
    println!("Output dir: {}", report.output_dir.display());
    println!("Receipt: {}", report.receipt_file.display());
    println!("Tasks: {}", report.tasks.len());
    for task in &report.tasks {
        println!(
            "- {} component={} gracefulStop={} script={} stopFile={}",
            task.name,
            task.component,
            yes_no(task.graceful_stop),
            task.runner_script.display(),
            task.stop_file.display()
        );
    }
    println!("Scripts: {}", report.scripts.len());
    for script in &report.scripts {
        println!(
            "- {}: {} ({})",
            script.name,
            script.path.display(),
            script.purpose
        );
    }
    if !report.warnings.is_empty() {
        println!("Warnings:");
        for warning in &report.warnings {
            println!("- {warning}");
        }
    }
}

fn print_activation_readiness_report(report: &ActivationReadinessReport) {
    println!("Agent harness enable check");
    println!("Harness home: {}", report.harness_home.display());
    println!("Ready: {}", yes_no(report.ready));
    println!(
        "Checks: passed={} warnings={} failed={}",
        report.summary.passed, report.summary.warnings, report.summary.failed
    );
    println!();
    println!("Findings:");
    for status_name in ["Fail", "Warn", "Pass"] {
        for check in &report.checks {
            if format!("{:?}", check.status) == status_name {
                println!("- {:?} {}: {}", check.status, check.name, check.detail);
            }
        }
    }
}

fn print_harness_status_report(report: &HarnessStatusReport) {
    println!("Agent Harness status");
    println!("Harness home: {}", report.harness_home.display());
    println!(
        "Ready: {} (passed={} warnings={} failed={})",
        yes_no(report.ready),
        report.readiness.summary.passed,
        report.readiness.summary.warnings,
        report.readiness.summary.failed
    );
    println!(
        "Runtime: queued={} open={} cronQueued={} cronOpen={} prepared={} completed={} invalid={} runOnce={} codexRun={} completion={}",
        report.runtime.queued_items,
        report.runtime.open_items,
        report.runtime.cron_queued_items,
        report.runtime.cron_open_items,
        report.runtime.prepared_items,
        report.runtime.completed_items,
        report.runtime.pending_invalid_lines,
        receipt_summary(&report.runtime.run_once_receipts),
        receipt_summary(&report.runtime.codex_run_receipts),
        receipt_summary(&report.runtime.codex_completion_receipts)
    );
    println!(
        "Runtime classes: queued=[{}] open=[{}] origins=[{}]",
        count_summary(&report.runtime.queued_by_runtime_class),
        count_summary(&report.runtime.open_by_runtime_class),
        count_summary(&report.runtime.open_by_origin)
    );
    let lease_parts = report
        .runtime
        .class_leases
        .iter()
        .filter(|status| status.leased_items > 0 || status.expired_leases > 0)
        .map(|status| {
            format!(
                "{} active={} expired={} cron={}",
                status.runtime_class,
                status.active_leases,
                status.expired_leases,
                status.cron_run_leases
            )
        })
        .collect::<Vec<_>>();
    if !lease_parts.is_empty() {
        println!("Runtime leases: {}", lease_parts.join("; "));
    }
    if let Some(latest) = &report.runtime.latest_non_idle_run_once {
        println!(
            "Runtime latest non-idle: line={} queue={} class={} origin={} cronRun={} status={} reason={}",
            latest.line_number,
            latest.queue_id.as_deref().unwrap_or("-"),
            latest.runtime_class.as_deref().unwrap_or("-"),
            latest.origin.as_deref().unwrap_or("-"),
            latest.cron_run_id.as_deref().unwrap_or("-"),
            latest.status.as_deref().unwrap_or("-"),
            latest.reason.as_deref().unwrap_or("-")
        );
    }
    println!(
        "Cron scheduler: status={} due={} enqueued={} held={} duplicate={} policy={} errors={} receipts={}",
        report
            .cron_scheduler
            .latest_status
            .as_deref()
            .unwrap_or("-"),
        fmt_optional_i64(report.cron_scheduler.latest_due_candidates),
        fmt_optional_i64(report.cron_scheduler.latest_enqueued),
        fmt_optional_i64(report.cron_scheduler.latest_skipped_held),
        fmt_optional_i64(report.cron_scheduler.latest_skipped_duplicate),
        fmt_optional_i64(report.cron_scheduler.latest_skipped_policy),
        fmt_optional_i64(report.cron_scheduler.latest_errors),
        receipt_summary(&report.cron_scheduler.receipts)
    );
    println!(
        "Cron runs: active={} terminal={} quarantined={} status=[{}] activeAgents=[{}]",
        report.cron_runs.summary.active,
        report.cron_runs.summary.terminal,
        report.cron_runs.summary.quarantined,
        count_summary(&report.cron_runs.summary.by_status),
        count_summary(&report.cron_runs.summary.by_agent_active)
    );
    println!(
        "Outbox: pending={} delivered={} retryable={} invalid={}",
        report.channels.outbox.all.pending,
        report.channels.outbox.all.delivered,
        report.channels.outbox.all.failed_retryable,
        report.channels.outbox.all.invalid_lines
    );
    println!(
        "Channels: telegramOffset={} telegramProbe={} telegramPollLog={} discordSendLog={} discordEventLog={} discordGateway={} discordReplyContext={}",
        yes_no(report.channels.telegram_offset_present),
        receipt_summary(&report.channels.telegram_probe),
        yes_no(report.channels.telegram_poll_log_present),
        yes_no(report.channels.discord_send_log_present),
        yes_no(report.channels.discord_event_log_present),
        receipt_summary(&report.channels.discord_gateway_probe),
        receipt_summary(&report.channels.discord_reply_context_receipts)
    );
    println!(
        "Memory: qdrantEdge={} lancedb={} legacyMemSqlite={} embeddingSecrets={} files={} activeRecall={} qdrantParity={} captureCandidates={} search={} vectorRecall={} promptContext={} lifecycle={} canvas={}",
        yes_no(report.memory.qdrant_edge),
        yes_no(report.memory.lancedb),
        yes_no(report.memory.legacy_mem_sqlite),
        yes_no(report.memory.memory_credentials_env_present),
        report.memory.regular_files,
        report.memory.summary.active_recall_backend,
        report.memory.summary.qdrant_parity,
        report.memory.summary.capture_candidate_count,
        receipt_summary(&report.memory.search_receipts),
        receipt_summary(&report.memory.vector_recall_receipts),
        receipt_summary(&report.memory.prompt_context_receipts),
        receipt_summary(&report.memory.lifecycle_receipts),
        receipt_summary(&report.memory.canvas_receipts)
    );
    println!(
        "Learning: skillUsage={} proposals={} applyReceipts={} snapshot={}",
        receipt_summary(&report.learning.skill_usage_events),
        receipt_summary(&report.learning.skill_proposals),
        receipt_summary(&report.learning.skill_apply_receipts),
        yes_no(report.learning.skill_usage_snapshot_present)
    );
    println!(
        "Plugins: catalog={} tools={} execution={} probe={} bridge={}",
        yes_no(report.plugins.catalog_present),
        report.plugins.catalog_tools,
        receipt_summary(&report.plugins.sidecar_execution_receipts),
        receipt_summary(&report.plugins.sidecar_probe_receipts),
        receipt_summary(&report.plugins.sidecar_bridge_receipts)
    );
    println!(
        "Logs: lines={} invalid={} latest={}",
        report.logs.lines,
        report.logs.invalid_lines,
        report.logs.latest_event.as_deref().unwrap_or("-")
    );
    if !report.warnings.is_empty() {
        println!("Warnings:");
        for warning in &report.warnings {
            println!("- {warning}");
        }
    }
}

fn print_memory_search_report_json(report: &MemorySearchReport) -> Result<(), String> {
    let value = serde_json::json!({
        "schema": report.schema,
        "harnessHome": report.harness_home,
        "memoryDir": report.memory_dir,
        "status": report.status.as_str(),
        "reason": report.reason,
        "query": report.query,
        "searchedFiles": report.searched_files,
        "skippedFiles": report.skipped_files,
        "hits": report.hits.iter().map(|hit| {
            serde_json::json!({
                "path": hit.path,
                "line": hit.line,
                "score": hit.score,
                "snippet": hit.snippet,
            })
        }).collect::<Vec<_>>(),
        "warnings": report.warnings,
    });
    println!(
        "{}",
        serde_json::to_string_pretty(&value).map_err(|err| err.to_string())?
    );
    Ok(())
}

fn print_memory_search_report(report: &MemorySearchReport) {
    println!("Harness imported memory search");
    println!("Harness home: {}", report.harness_home.display());
    println!("Memory dir: {}", report.memory_dir.display());
    println!("Status: {}", report.status.as_str());
    println!("Reason: {}", report.reason);
    println!(
        "Files: searched={} skipped={} hits={}",
        report.searched_files,
        report.skipped_files,
        report.hits.len()
    );
    if !report.hits.is_empty() {
        println!("Hits:");
        for hit in &report.hits {
            println!(
                "- {}:{} score={} {}",
                hit.path.display(),
                hit.line,
                hit.score,
                hit.snippet
            );
        }
    }
    if !report.warnings.is_empty() {
        println!("Warnings:");
        for warning in &report.warnings {
            println!("- {warning}");
        }
    }
}

fn print_memory_credentials_export_report(
    report: &MemoryCredentialsExportReport,
    include_sensitive: bool,
) {
    println!("Harness memory credentials export");
    println!("Source home: {}", report.source_home.display());
    println!("Target env file: {}", report.env_file.display());
    println!("Receipt file: {}", report.receipt_file.display());
    println!("Sensitive values written: {}", yes_no(include_sensitive));
    println!("Entries: {}", report.entries.len());
    for entry in &report.entries {
        println!(
            "- {}: exported={} sensitive={} length={} source={} ({})",
            entry.env_name,
            yes_no(entry.exported),
            yes_no(entry.sensitive),
            entry.length,
            entry.source_path,
            entry.reason
        );
    }
    if !report.warnings.is_empty() {
        println!("Warnings:");
        for warning in &report.warnings {
            println!("- {warning}");
        }
    }
}

fn print_memory_vector_report_json(report: &MemoryVectorRecallReport) -> Result<(), String> {
    println!(
        "{}",
        serde_json::to_string_pretty(report).map_err(|err| err.to_string())?
    );
    Ok(())
}

fn print_memory_vector_report(report: &MemoryVectorRecallReport) {
    println!("Harness imported vector memory search");
    println!("Harness home: {}", report.harness_home.display());
    println!("Status: {:?}", report.status);
    println!("Reason: {}", report.reason);
    println!("Backend: {}", report.backend);
    println!(
        "Embedding: model={} dim={}",
        report.embedding_model.as_deref().unwrap_or("-"),
        report.query_embedding_dim
    );
    println!(
        "SQLite: {}",
        report
            .sqlite_database
            .as_deref()
            .map(Path::display)
            .map(|display| display.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "Qdrant edge snapshot: {}",
        report
            .qdrant_edge_dir
            .as_deref()
            .map(Path::display)
            .map(|display| display.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    println!("Hits: {}", report.hits.len());
    for hit in &report.hits {
        println!(
            "- [{}] score={:.4} id={} title={} source={}",
            hit.lane,
            hit.score,
            hit.id,
            hit.title,
            hit.source.as_deref().unwrap_or("-")
        );
        if !hit.text.is_empty() {
            println!("  {}", hit.text);
        }
    }
    if !report.warnings.is_empty() {
        println!("Warnings:");
        for warning in &report.warnings {
            println!("- {warning}");
        }
    }
}

fn print_memory_canvas_report(report: &MemoryCanvasWorkerReport) {
    println!("Harness memory canvas worker");
    println!("Harness home: {}", report.harness_home.display());
    println!(
        "Agent: {}",
        report.agent_id.as_deref().unwrap_or("(global)")
    );
    println!("Status: {:?}", report.status);
    println!("Reason: {}", report.reason);
    println!("Canvas JSON: {}", report.canvas_json.display());
    println!("Canvas markdown: {}", report.canvas_markdown.display());
    println!(
        "Inputs: candidates={} episodes={}",
        report.candidates_read, report.episodes_read
    );
    if !report.warnings.is_empty() {
        println!("Warnings:");
        for warning in &report.warnings {
            println!("- {warning}");
        }
    }
}

fn print_memory_embedding_backfill_report(report: &MemoryEmbeddingBackfillReport) {
    println!("Harness memory embedding backfill");
    println!("Harness home: {}", report.harness_home.display());
    println!("Lane: {}", report.lane.as_str());
    println!("Status: {}", report.status);
    println!("Reason: {}", report.reason);
    println!(
        "Model: {} dim={} selected={} rateRemaining={}",
        report.model,
        report.vector_dimension,
        report.selected_item_ids.len(),
        report.rate_limit_remaining
    );
    println!(
        "Coverage: before={:?} after={:?} threshold={} parityAllowed={}",
        report.coverage_before_bps,
        report.coverage_after_bps,
        report.coverage_threshold_bps,
        yes_no(report.parity_claim_allowed)
    );
    println!("Cursor: {}", report.cursor_file.display());
    println!("Receipt: {}", report.receipt_file.display());
    if !report.warnings.is_empty() {
        println!("Warnings:");
        for warning in &report.warnings {
            println!("- {warning}");
        }
    }
}

fn receipt_summary(status: &agent_harness_core::HarnessJsonlStatus) -> String {
    let latest = status
        .latest_status
        .as_deref()
        .or(status.latest_method.as_deref())
        .unwrap_or("-");
    format!(
        "{} lines latest={}",
        if status.exists { status.lines } else { 0 },
        latest
    )
}

fn print_turn_plan(plan: &TurnPlan) {
    println!("Agent turn plan");
    println!("Dispatch: {:?}", plan.dispatch);
    println!("Platform: {}", plan.platform);
    println!("Channel: {}", plan.channel_id);
    println!("User: {}", plan.user_id);
    println!("Session key: {}", plan.session_key);
    if let Some(agent) = &plan.agent {
        println!(
            "Agent: {} enabled={} dir={} sessions={}",
            agent.id,
            agent.enabled.map(yes_no).unwrap_or("unknown"),
            yes_no(agent.directory_exists),
            yes_no(agent.sessions_index_exists)
        );
    } else {
        println!("Agent: none");
    }
    println!(
        "Model policy: provider={} model={}",
        plan.model_policy.provider.as_deref().unwrap_or("-"),
        plan.model_policy.model.as_deref().unwrap_or("-")
    );
    if let Some(state) = &plan.channel_state {
        println!(
            "Channel state: active_session={} model_override={} thinking={} steering_notes={} btw_notes={} stop_requested={}",
            state.active_session_key,
            state.model_override.as_deref().unwrap_or("-"),
            yes_no(state.thinking_enabled),
            state.steering_notes.len(),
            state.btw_notes.len(),
            yes_no(state.stop_requested)
        );
    }
    if let Some(command) = &plan.command {
        println!("Command: {}", command.name());
    }
    println!(
        "Prompt files present: {} / {}",
        plan.prompt_files.iter().filter(|file| file.exists).count(),
        plan.prompt_files.len()
    );
    println!("Selected skills: {}", plan.selected_skills.len());
    for skill in &plan.selected_skills {
        println!(
            "- {} [{:?}] score={} title={}",
            skill.skill_id, skill.source_kind, skill.score, skill.title
        );
    }
    if !plan.warnings.is_empty() {
        println!("Warnings:");
        for warning in &plan.warnings {
            println!("- {warning}");
        }
    }
}

fn print_channel_step(step: &ChannelStep) {
    println!("Agent channel step");
    println!("Action: {:?}", step.action);
    println!("Platform: {}", step.platform);
    println!("Channel: {}", step.channel_id);
    println!("User: {}", step.user_id);
    println!("Session key: {}", step.session_key);
    if let Some(effect) = &step.command_effect {
        println!("Command effect: {:?}", effect);
    }
    if let Some(agent_turn) = &step.agent_turn {
        println!(
            "Agent turn: agent={} provider={} model={} prompt_files={}/{} skills={}",
            agent_turn.agent_id,
            agent_turn.provider.as_deref().unwrap_or("-"),
            agent_turn.model.as_deref().unwrap_or("-"),
            agent_turn.prompt_files_present,
            agent_turn.prompt_files_total,
            agent_turn.selected_skill_ids.len()
        );
        for skill_id in &agent_turn.selected_skill_ids {
            println!("- skill {skill_id}");
        }
    }
    if !step.outbound_messages.is_empty() {
        println!("Outbound messages:");
        for message in &step.outbound_messages {
            println!("- {:?}: {}", message.kind, message.text);
        }
    }
    if !step.warnings.is_empty() {
        println!("Warnings:");
        for warning in &step.warnings {
            println!("- {warning}");
        }
    }
}

fn print_channel_command_apply_report(report: &ChannelCommandApplyReport) {
    println!("Agent channel command apply");
    println!("Harness home: {}", report.harness_home.display());
    println!("State file: {}", report.state_file.display());
    println!("Events file: {}", report.events_file.display());
    println!("Receipts file: {}", report.receipts_file.display());
    println!("Receipt: {:?}", report.receipt.status);
    println!("Reason: {}", report.receipt.reason);
    if let Some(event) = &report.event {
        println!("Command: {}", event.command);
        println!("Session key: {}", event.active_session_key);
    }
    if let Some(state) = &report.state {
        println!("Active session: {}", state.active_session_key);
        println!("Thinking enabled: {}", yes_no(state.thinking_enabled));
        println!(
            "Model override: provider={} model={} target={}",
            state.model_override_provider.as_deref().unwrap_or("-"),
            state.model_override_model.as_deref().unwrap_or("-"),
            state.model_override.as_deref().unwrap_or("-")
        );
        println!(
            "Notes: steering={} btw={}",
            state.steering_notes.len(),
            state.btw_notes.len()
        );
        println!("Stop requested: {}", yes_no(state.stop_requested));
    }
    if !report.outbound_messages.is_empty() {
        println!("Outbound messages:");
        for message in &report.outbound_messages {
            println!("- {:?}: {}", message.kind, message.text);
        }
    }
    if !report.warnings.is_empty() {
        println!("Warnings:");
        for warning in &report.warnings {
            println!("- {warning}");
        }
    }
}

fn print_channel_receive_report(report: &ChannelReceiveReport) {
    println!("Agent channel receive");
    println!("Harness home: {}", report.harness_home.display());
    println!("Status: {:?}", report.status);
    println!("Reason: {}", report.receipt.reason);
    println!("Platform: {}", report.platform);
    println!("Channel: {}", report.channel_id);
    println!("User: {}", report.user_id);
    println!("Session key: {}", report.session_key);
    println!("Step action: {:?}", report.step_action);
    if let Some(command_name) = &report.command_name {
        println!("Command: {command_name}");
    }
    if let Some(queue_id) = &report.queue_id {
        println!("Queue id: {queue_id}");
    }
    println!("Outbox file: {}", report.outbox_file.display());
    println!("Receipts file: {}", report.receipts_file.display());
    if !report.outbound_messages.is_empty() {
        println!("Outbound messages:");
        for message in &report.outbound_messages {
            println!("- {:?}: {}", message.kind, message.text);
        }
    }
    if let Some(apply) = &report.command_apply {
        println!("Command state receipt: {:?}", apply.receipt.status);
        println!("Command state file: {}", apply.state_file.display());
    }
    if let Some(queue) = &report.queue_enqueue {
        println!("Queue receipt: {:?}", queue.receipt.status);
        if let Some(item) = &queue.item {
            println!(
                "Model policy: provider={} model={}",
                item.provider.as_deref().unwrap_or("-"),
                item.model.as_deref().unwrap_or("-")
            );
        }
    }
    if !report.warnings.is_empty() {
        println!("Warnings:");
        for warning in &report.warnings {
            println!("- {warning}");
        }
    }
}

fn print_channel_run_once_report(report: &ChannelRunOnceReport) {
    println!("Agent channel run once");
    println!("Harness home: {}", report.harness_home.display());
    println!("Status: {:?}", report.status);
    println!("Receive: {:?}", report.receive.status);
    println!("Platform: {}", report.receive.platform);
    println!("Channel: {}", report.receive.channel_id);
    println!("User: {}", report.receive.user_id);
    println!("Session key: {}", report.receive.session_key);
    if let Some(queue_id) = &report.receive.queue_id {
        println!("Queue id: {queue_id}");
    }
    if let Some(runtime) = &report.runtime {
        println!("Runtime: {:?}", runtime.receipt.status);
        println!("Runtime reason: {}", runtime.receipt.reason);
        if let Some(execution_dir) = &runtime.receipt.execution_dir {
            println!("Execution dir: {}", execution_dir.display());
        }
        if let Some(transcript_file) = &runtime.receipt.transcript_file {
            println!("Transcript: {}", transcript_file.display());
        }
    }
    println!(
        "Outbox pending={} delivered={} failed_retryable={}",
        report.outbox.summary.pending,
        report.outbox.summary.delivered,
        report.outbox.summary.failed_retryable
    );
    for pending in &report.outbox.pending {
        println!(
            "- {} {:?} platform={} channel={} user={} session={}",
            pending.delivery_id,
            pending.message.kind,
            pending.message.platform,
            pending.message.channel_id,
            pending.message.user_id,
            pending.message.session_key
        );
        println!("  {}", pending.message.text);
    }
    if !report.warnings.is_empty() {
        println!("Warnings:");
        for warning in &report.warnings {
            println!("- {warning}");
        }
    }
}

fn print_channel_outbox_plan_report(report: &ChannelOutboxPlanReport) {
    println!("Agent channel outbox plan");
    println!("Harness home: {}", report.harness_home.display());
    println!("Outbox file: {}", report.outbox_file.display());
    println!("Receipts file: {}", report.receipts_file.display());
    println!(
        "Summary: lines={} pending={} delivered={} failed_retryable={} skipped_platform={} invalid={}",
        report.summary.total_outbox_lines,
        report.summary.pending,
        report.summary.delivered,
        report.summary.failed_retryable,
        report.summary.skipped_platform,
        report.summary.invalid_lines
    );
    if !report.pending.is_empty() {
        println!("Pending:");
        for pending in &report.pending {
            println!(
                "- {} line={} attempts={} last={:?} kind={:?} platform={} channel={} user={} session={}",
                pending.delivery_id,
                pending.line_number,
                pending.attempts,
                pending.last_status,
                pending.message.kind,
                pending.message.platform,
                pending.message.channel_id,
                pending.message.user_id,
                pending.message.session_key
            );
            println!("  {}", pending.message.text);
        }
    }
    if !report.warnings.is_empty() {
        println!("Warnings:");
        for warning in &report.warnings {
            println!("- {warning}");
        }
    }
}

fn print_channel_delivery_receipt(receipt: &ChannelDeliveryReceipt) {
    println!("Agent channel delivery receipt");
    println!("Delivery id: {}", receipt.delivery_id);
    println!("Status: {:?}", receipt.status);
    println!("Platform: {}", receipt.platform);
    println!("Channel: {}", receipt.channel_id);
    println!("User: {}", receipt.user_id);
    println!("Session: {}", receipt.session_key);
    if let Some(provider_message_id) = &receipt.provider_message_id {
        println!("Provider message id: {provider_message_id}");
    }
    if let Some(error) = &receipt.error {
        println!("Error: {error}");
    }
    println!("At ms: {}", receipt.at_ms);
}

fn print_progress_delivery_once_report(report: &ProgressDeliveryOnceReport) {
    println!("Agent progress delivery once");
    println!("Pending panels: {}", report.pending_count);
    println!("Muted events: {}", report.skipped_muted);
    println!("Volume-limited panels: {}", report.volume_limited);
    println!("Sent panels: {}", report.sent_messages);
    println!("Edited panels: {}", report.edited_messages);
    println!("Denied panels: {}", report.skipped_denied);
    println!("Permanent-skip panels: {}", report.skipped_permanent);
    println!("Failed deliveries: {}", report.failed_deliveries);
    if !report.warnings.is_empty() {
        println!("Warnings:");
        for warning in &report.warnings {
            println!("- {warning}");
        }
    }
}

fn print_telegram_probe_report(report: &TelegramProbeReport) {
    println!("Agent Telegram probe");
    println!("Harness home: {}", report.harness_home.display());
    println!("Status: {}", report.status);
    println!("Reason: {}", report.reason);
    println!("Receipt file: {}", report.receipt_file.display());
    println!("Bot id: {}", report.bot_id.as_deref().unwrap_or("-"));
    println!("Username: {}", report.username.as_deref().unwrap_or("-"));
    println!("Can join groups: {}", optional_bool(report.can_join_groups));
    println!(
        "Can read all group messages: {}",
        optional_bool(report.can_read_all_group_messages)
    );
    println!(
        "Supports inline queries: {}",
        optional_bool(report.supports_inline_queries)
    );
    println!("At ms: {}", report.at_ms);
}

fn print_telegram_poll_once_report(report: &TelegramPollOnceReport) {
    println!("Agent Telegram poll once");
    println!("Updates: {}", report.update_count);
    println!("Handled messages: {}", report.handled_messages);
    println!("Skipped updates: {}", report.skipped_updates);
    println!("Delivered messages: {}", report.delivered_messages);
    println!("Failed deliveries: {}", report.failed_deliveries);
    println!(
        "Next offset: {}",
        report
            .next_offset
            .map(|offset| offset.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    if !report.warnings.is_empty() {
        println!("Warnings:");
        for warning in &report.warnings {
            println!("- {warning}");
        }
    }
}

fn print_discord_outbox_send_once_report(report: &DiscordOutboxSendOnceReport) {
    println!("Agent Discord outbox send once");
    println!("Pending messages: {}", report.pending_count);
    println!("Delivered messages: {}", report.delivered_messages);
    println!("Failed deliveries: {}", report.failed_deliveries);
    if !report.warnings.is_empty() {
        println!("Warnings:");
        for warning in &report.warnings {
            println!("- {warning}");
        }
    }
}

fn print_discord_dm_probe_report(report: &DiscordDmProbeReport) {
    println!("Agent Discord DM probe");
    println!("Harness home: {}", report.harness_home.display());
    println!("Status: {}", report.status);
    println!("Reason: {}", report.reason);
    println!("User: {}", report.user_id);
    println!("Channel: {}", report.channel_id.as_deref().unwrap_or("-"));
    println!(
        "Provider message: {}",
        report.provider_message_id.as_deref().unwrap_or("-")
    );
    println!("Sent message: {}", yes_no(report.sent_message));
    if !report.warnings.is_empty() {
        println!("Warnings:");
        for warning in &report.warnings {
            println!("- {warning}");
        }
    }
}

fn print_discord_dm_history_probe_report(report: &DiscordDmHistoryProbeReport) {
    println!("Agent Discord DM history probe");
    println!("Harness home: {}", report.harness_home.display());
    println!("Status: {}", report.status);
    println!("Reason: {}", report.reason);
    println!("Channel: {}", report.channel_id.as_deref().unwrap_or("-"));
    println!("User: {}", report.user_id.as_deref().unwrap_or("-"));
    println!("Limit: {}", report.limit);
    println!(
        "Messages: total={} user={} bot={}",
        report.message_count, report.user_message_count, report.bot_message_count
    );
    if !report.messages.is_empty() {
        println!("Message metadata:");
        for message in &report.messages {
            println!(
                "- id={} author={} bot={} timestamp={} contentLength={}",
                message.message_id,
                message.author_id.as_deref().unwrap_or("-"),
                message.author_bot.map(yes_no).unwrap_or("-"),
                message.timestamp.as_deref().unwrap_or("-"),
                message
                    .content_length
                    .map(|length| length.to_string())
                    .unwrap_or_else(|| "-".to_string())
            );
        }
    }
    if !report.warnings.is_empty() {
        println!("Warnings:");
        for warning in &report.warnings {
            println!("- {warning}");
        }
    }
}

fn print_discord_event_run_once_report(report: &DiscordEventRunOnceReport) {
    println!("Agent Discord event run once");
    println!("Harness home: {}", report.harness_home.display());
    println!("Status: {}", report.status);
    println!("Reason: {}", report.reason);
    println!("Message: {}", report.message_id.as_deref().unwrap_or("-"));
    println!("Guild: {}", report.guild_id.as_deref().unwrap_or("-"));
    println!("Channel: {}", report.channel_id.as_deref().unwrap_or("-"));
    println!("User: {}", report.user_id.as_deref().unwrap_or("-"));
    if let Some(run) = &report.run {
        println!("Run status: {:?}", run.status);
        println!("Session key: {}", run.receive.session_key);
        println!(
            "Outbox pending={} delivered={} failed_retryable={}",
            run.outbox.summary.pending,
            run.outbox.summary.delivered,
            run.outbox.summary.failed_retryable
        );
    }
}

fn print_runtime_queue_enqueue_report(report: &RuntimeQueueEnqueueReport) {
    println!("Agent runtime queue enqueue");
    println!("Harness home: {}", report.harness_home.display());
    println!("Queue file: {}", report.queue_file.display());
    println!("Receipts file: {}", report.receipts_file.display());
    println!("Receipt: {:?}", report.receipt.status);
    println!("Reason: {}", report.receipt.reason);
    if let Some(item) = &report.item {
        println!("Queue id: {}", item.queue_id);
        println!("Agent: {}", item.agent_id);
        println!("Session key: {}", item.session_key);
        println!(
            "Model policy: provider={} model={}",
            item.provider.as_deref().unwrap_or("-"),
            item.model.as_deref().unwrap_or("-")
        );
        println!(
            "Prompt files: {}/{}",
            item.prompt_files_present, item.prompt_files_total
        );
        println!("Selected skills: {}", item.selected_skill_ids.len());
        println!(
            "Planned transcript: {}",
            item.planned_transcript_file.display()
        );
        println!(
            "Planned trajectory: {}",
            item.planned_trajectory_file.display()
        );
    }
    if !report.warnings.is_empty() {
        println!("Warnings:");
        for warning in &report.warnings {
            println!("- {warning}");
        }
    }
}

fn print_runtime_queue_prepare_report(report: &RuntimeQueuePrepareReport) {
    println!("Agent runtime queue prepare");
    println!("Harness home: {}", report.harness_home.display());
    println!("Queue file: {}", report.queue_file.display());
    println!(
        "Execution receipts file: {}",
        report.execution_receipts_file.display()
    );
    println!("Receipt: {:?}", report.receipt.status);
    println!("Reason: {}", report.receipt.reason);
    if let Some(item) = &report.item {
        println!("Queue id: {}", item.queue_id);
        println!("Agent: {}", item.agent_id);
        println!("Session key: {}", item.session_key);
        println!(
            "Model policy: provider={} model={}",
            item.provider.as_deref().unwrap_or("-"),
            item.model.as_deref().unwrap_or("-")
        );
        println!("Execution dir: {}", item.execution_dir.display());
        println!("Prompt bundle JSON: {}", item.prompt_bundle_json.display());
        println!("Prompt markdown: {}", item.prompt_markdown.display());
        println!("Execution receipt: {}", item.receipt_file.display());
        println!(
            "Planned transcript: {}",
            item.planned_transcript_file.display()
        );
        println!(
            "Planned trajectory: {}",
            item.planned_trajectory_file.display()
        );
        println!("Selected skills: {}", item.selected_skill_ids.len());
    } else {
        if let Some(execution_dir) = &report.receipt.execution_dir {
            println!("Execution dir: {}", execution_dir.display());
        }
        if let Some(prompt_bundle_json) = &report.receipt.prompt_bundle_json {
            println!("Prompt bundle JSON: {}", prompt_bundle_json.display());
        }
        if let Some(prompt_markdown) = &report.receipt.prompt_markdown {
            println!("Prompt markdown: {}", prompt_markdown.display());
        }
    }
    if !report.warnings.is_empty() {
        println!("Warnings:");
        for warning in &report.warnings {
            println!("- {warning}");
        }
    }
}

fn print_runtime_run_once_report(report: &RuntimeRunOnceReport) {
    println!("Agent runtime run once");
    println!("Harness home: {}", report.harness_home.display());
    println!("Report file: {}", report.report_file.display());
    println!("Receipts file: {}", report.receipts_file.display());
    println!("Receipt: {:?}", report.receipt.status);
    println!("Reason: {}", report.receipt.reason);
    if let Some(queue_id) = &report.receipt.queue_id {
        println!("Queue id: {queue_id}");
    }
    if let Some(execution_dir) = &report.receipt.execution_dir {
        println!("Execution dir: {}", execution_dir.display());
    }
    if let Some(prepare) = &report.prepare {
        println!("Prepare receipt: {:?}", prepare.receipt.status);
    }
    if let Some(plan) = &report.plan {
        println!("Plan receipt: {:?}", plan.receipt.status);
        if let Some(plan_file) = &plan.plan_file {
            println!("Plan file: {}", plan_file.display());
        }
    }
    if let Some(run) = &report.run {
        println!("Run receipt: {:?}", run.receipt.status);
        println!("Run reason: {}", run.receipt.reason);
        if let Some(run_file) = &run.run_file {
            println!("Run report: {}", run_file.display());
        }
        if let Some(stdout_log) = &run.stdout_log {
            println!("Stdout JSONL log: {}", stdout_log.display());
        }
        if let Some(stderr_log) = &run.stderr_log {
            println!("Stderr log: {}", stderr_log.display());
        }
    }
    if let Some(outbox_file) = &report.outbox_file {
        println!("Outbox file: {}", outbox_file.display());
    }
    if let Some(message) = &report.outbound_message {
        println!(
            "Outbound: {:?} platform={} channel={} user={} session={}",
            message.kind,
            message.platform,
            message.channel_id,
            message.user_id,
            message.session_key
        );
        println!("Outbound text: {}", message.text);
    }
    if !report.warnings.is_empty() {
        println!("Warnings:");
        for warning in &report.warnings {
            println!("- {warning}");
        }
    }
}

fn print_runtime_loop_summary(summary: &RuntimeLoopSummary) {
    println!("Agent runtime loop");
    println!("Harness home: {}", summary.target_home.display());
    println!("Report file: {}", summary.report_file.display());
    println!("Iterations: {}", summary.iterations);
    println!("Completed: {}", summary.completed);
    println!("Idle: {}", summary.idle);
    println!("Errors: {}", summary.errors);
    println!("Consecutive errors: {}", summary.consecutive_errors);
    println!("Safe-mode restarts: {}", summary.safe_mode_restarts);
    println!("Runtime concurrency: {}", summary.runtime_concurrency);
    println!("Stop reason: {}", summary.stop_reason);
    if let Some(status) = summary.last_status {
        println!("Last status: {}", runtime_run_once_status_label(status));
    }
    if let Some(queue_id) = &summary.last_queue_id {
        println!("Last queue id: {queue_id}");
    }
    if let Some(reason) = &summary.last_reason {
        println!("Last reason: {reason}");
    }
}

fn write_runtime_loop_summary(summary: &RuntimeLoopSummary) -> Result<(), String> {
    if let Some(parent) = summary.report_file.parent() {
        fs::create_dir_all(parent).map_err(|err| err.to_string())?;
    }
    let report = serde_json::json!({
        "schema": "agent-harness.runtime-loop.v1",
        "harnessHome": &summary.target_home,
        "reportFile": &summary.report_file,
        "startedAtMs": summary.started_at_ms,
        "finishedAtMs": summary.finished_at_ms,
        "iterations": summary.iterations,
        "completed": summary.completed,
        "idle": summary.idle,
        "errors": summary.errors,
        "consecutiveErrors": summary.consecutive_errors,
        "safeModeRestarts": summary.safe_mode_restarts,
        "runtimeConcurrency": summary.runtime_concurrency,
        "stopReason": &summary.stop_reason,
        "lastStatus": summary.last_status.map(runtime_run_once_status_label),
        "lastQueueId": summary.last_queue_id.as_deref(),
        "lastReason": summary.last_reason.as_deref(),
    });
    let bytes = serde_json::to_vec_pretty(&report).map_err(|err| err.to_string())?;
    fs::write(&summary.report_file, bytes).map_err(|err| err.to_string())
}

fn append_runtime_loop_error_log(
    harness_home: &Path,
    iteration: usize,
    consecutive_errors: usize,
    max_consecutive_errors: usize,
    error: &str,
) -> Result<(), String> {
    append_harness_log(
        harness_home,
        &HarnessLogEvent::new(
            current_log_time_ms().map_err(|err| err.to_string())?,
            HarnessLogLevel::Warn,
            "runtime",
            "runtime.loop-error",
            format!(
                "iteration={iteration} consecutiveErrors={consecutive_errors}/{max_consecutive_errors} error={error}"
            ),
        ),
    )
    .map(|_| ())
    .map_err(|err| err.to_string())
}

fn append_runtime_loop_safe_mode_log(
    harness_home: &Path,
    iteration: usize,
    safe_mode_restarts: usize,
    consecutive_errors: usize,
    max_consecutive_errors: usize,
    runtime_concurrency: usize,
    restart_ms: u64,
) -> Result<(), String> {
    append_harness_log(
        harness_home,
        &HarnessLogEvent::new(
            current_log_time_ms().map_err(|err| err.to_string())?,
            HarnessLogLevel::Warn,
            "runtime",
            "runtime.loop-safe-mode",
            format!(
                "iteration={iteration} safeModeRestarts={safe_mode_restarts} consecutiveErrors={consecutive_errors}/{max_consecutive_errors} runtimeConcurrency={runtime_concurrency} restartMs={restart_ms}"
            ),
        ),
    )
    .map(|_| ())
    .map_err(|err| err.to_string())
}

fn append_runtime_loop_stopped_log(
    summary: &RuntimeLoopSummary,
    failed: bool,
) -> Result<(), String> {
    append_harness_log(
        &summary.target_home,
        &HarnessLogEvent::new(
            summary.finished_at_ms,
            if failed {
                HarnessLogLevel::Error
            } else if summary.errors > 0 {
                HarnessLogLevel::Warn
            } else {
                HarnessLogLevel::Info
            },
            "runtime",
            "runtime.loop-stopped",
            format!(
                "iterations={} completed={} idle={} errors={} safeModeRestarts={} concurrency={} stopReason={}",
                summary.iterations,
                summary.completed,
                summary.idle,
                summary.errors,
                summary.safe_mode_restarts,
                summary.runtime_concurrency,
                summary.stop_reason
            ),
        ),
    )
    .map(|_| ())
    .map_err(|err| err.to_string())
}

fn runtime_run_once_status_is_idle(status: RuntimeRunOnceStatus) -> bool {
    matches!(
        status,
        RuntimeRunOnceStatus::NoWork | RuntimeRunOnceStatus::NoPreparedExecution
    )
}

fn runtime_run_once_report_is_idle(report: &RuntimeRunOnceReport) -> bool {
    runtime_run_once_status_is_idle(report.receipt.status)
        || (report.receipt.status == RuntimeRunOnceStatus::Completed
            && report.receipt.reason.contains("already recorded"))
}

fn runtime_run_once_status_label(status: RuntimeRunOnceStatus) -> &'static str {
    match status {
        RuntimeRunOnceStatus::Completed => "completed",
        RuntimeRunOnceStatus::LeaseBusy => "lease-busy",
        RuntimeRunOnceStatus::NoWork => "no-work",
        RuntimeRunOnceStatus::NoPreparedExecution => "no-prepared-execution",
        RuntimeRunOnceStatus::NoRuntimePlan => "no-runtime-plan",
        RuntimeRunOnceStatus::PreflightBlocked => "preflight-blocked",
        RuntimeRunOnceStatus::SpawnFailed => "spawn-failed",
        RuntimeRunOnceStatus::ProtocolError => "protocol-error",
        RuntimeRunOnceStatus::Timeout => "timeout",
        RuntimeRunOnceStatus::RetryPending => "retry-pending",
        RuntimeRunOnceStatus::DeadLetter => "dead-letter",
        RuntimeRunOnceStatus::FailedTerminal => "failed-terminal",
        RuntimeRunOnceStatus::ContextExhausted => "context-exhausted",
        RuntimeRunOnceStatus::Canceled => "canceled",
    }
}

fn worker_run_once_status_label(status: WorkerRunOnceStatus) -> &'static str {
    match status {
        WorkerRunOnceStatus::Completed => "completed",
        WorkerRunOnceStatus::Rescheduled => "rescheduled",
        WorkerRunOnceStatus::NoWork => "no-work",
        WorkerRunOnceStatus::Failed => "failed",
    }
}

fn print_codex_runtime_plan_report(report: &CodexRuntimePlanReport) {
    println!("Agent Codex runtime plan");
    println!("Harness home: {}", report.harness_home.display());
    println!("Receipts file: {}", report.receipts_file.display());
    println!("Receipt: {:?}", report.receipt.status);
    println!("Reason: {}", report.receipt.reason);
    if let Some(execution_dir) = &report.execution_dir {
        println!("Execution dir: {}", execution_dir.display());
    }
    if let Some(plan_file) = &report.plan_file {
        println!("Plan file: {}", plan_file.display());
    }
    if let Some(plan) = &report.plan {
        println!("Queue id: {}", plan.queue_id.as_deref().unwrap_or("-"));
        println!("Agent: {}", plan.agent_id.as_deref().unwrap_or("-"));
        println!("Session key: {}", plan.session_key);
        println!(
            "Model policy: provider={} model={}",
            plan.provider.as_deref().unwrap_or("-"),
            plan.model.as_deref().unwrap_or("-")
        );
        println!("Executable: {}", plan.invocation.executable.display());
        println!("Transport: {:?}", plan.invocation.transport);
        println!("Arguments: {}", plan.invocation.arguments.join(" "));
        println!(
            "Working directory: {}",
            plan.invocation.working_directory.display()
        );
        println!(
            "Prompt input: {}",
            plan.invocation.prompt_input_file.display()
        );
        println!("Prompt bundle JSON: {}", plan.prompt_bundle_json.display());
        println!("Prompt markdown: {}", plan.prompt_markdown.display());
        println!("Transcript: {}", plan.outputs.transcript_file.display());
        println!("Trajectory: {}", plan.outputs.trajectory_file.display());
        println!(
            "Codex binding: {}",
            plan.outputs.codex_binding_file.display()
        );
        println!(
            "Runtime receipt: {}",
            plan.outputs.runtime_receipt_file.display()
        );
        if !plan.invocation.env_requirements.is_empty() {
            println!("Environment requirements:");
            for requirement in &plan.invocation.env_requirements {
                println!("- {}: {}", requirement.name, requirement.reason);
            }
        }
    }
    if !report.warnings.is_empty() {
        println!("Warnings:");
        for warning in &report.warnings {
            println!("- {warning}");
        }
    }
}

fn print_codex_runtime_preflight_report(report: &CodexRuntimePreflightReport) {
    println!("Agent Codex runtime preflight");
    println!("Harness home: {}", report.harness_home.display());
    println!("Receipts file: {}", report.receipts_file.display());
    println!("Receipt: {:?}", report.receipt.status);
    println!("Reason: {}", report.receipt.reason);
    if let Some(execution_dir) = &report.execution_dir {
        println!("Execution dir: {}", execution_dir.display());
    }
    if let Some(plan_file) = &report.plan_file {
        println!("Plan file: {}", plan_file.display());
    }
    if let Some(preflight_file) = &report.preflight_file {
        println!("Preflight file: {}", preflight_file.display());
    }
    if !report.checks.is_empty() {
        println!("Checks:");
        for check in &report.checks {
            println!("- {:?} {}: {}", check.status, check.name, check.detail);
        }
    }
    if !report.warnings.is_empty() {
        println!("Warnings:");
        for warning in &report.warnings {
            println!("- {warning}");
        }
    }
}

fn print_codex_runtime_launch_probe_report(report: &CodexRuntimeLaunchProbeReport) {
    println!("Agent Codex runtime launch probe");
    println!("Harness home: {}", report.harness_home.display());
    println!("Receipts file: {}", report.receipts_file.display());
    println!("Receipt: {:?}", report.receipt.status);
    println!("Reason: {}", report.receipt.reason);
    if let Some(execution_dir) = &report.execution_dir {
        println!("Execution dir: {}", execution_dir.display());
    }
    if let Some(plan_file) = &report.plan_file {
        println!("Plan file: {}", plan_file.display());
    }
    if let Some(preflight_file) = &report.preflight_file {
        println!("Preflight file: {}", preflight_file.display());
    }
    if let Some(launch_file) = &report.launch_file {
        println!("Launch file: {}", launch_file.display());
    }
    if let Some(process) = &report.process {
        println!("Process:");
        println!("  Executable: {}", process.executable.display());
        println!("  Arguments: {}", process.arguments.join(" "));
        println!(
            "  Working directory: {}",
            process.working_directory.display()
        );
        println!(
            "  PID: {}",
            process
                .pid
                .map(|pid| pid.to_string())
                .unwrap_or_else(|| "-".to_string())
        );
        println!("  Probe ms: {}", process.startup_probe_ms);
        println!("  Elapsed ms: {}", process.elapsed_ms);
        println!(
            "  Exit status: {}",
            process.exit_status.as_deref().unwrap_or("-")
        );
        println!("  Terminated: {}", yes_no(process.terminated));
        if let Some(stdout_log) = &process.stdout_log {
            println!("  Stdout log: {}", stdout_log.display());
        }
        if let Some(stderr_log) = &process.stderr_log {
            println!("  Stderr log: {}", stderr_log.display());
        }
    }
    if !report.warnings.is_empty() {
        println!("Warnings:");
        for warning in &report.warnings {
            println!("- {warning}");
        }
    }
}

fn print_codex_runtime_run_report(report: &CodexRuntimeRunReport) {
    println!("Agent Codex runtime run");
    println!("Harness home: {}", report.harness_home.display());
    println!("Receipts file: {}", report.receipts_file.display());
    println!("Receipt: {:?}", report.receipt.status);
    println!("Reason: {}", report.receipt.reason);
    println!("Elapsed ms: {}", report.receipt.elapsed_ms);
    println!("Events: {}", report.receipt.event_count);
    if let Some(execution_dir) = &report.execution_dir {
        println!("Execution dir: {}", execution_dir.display());
    }
    if let Some(plan_file) = &report.plan_file {
        println!("Plan file: {}", plan_file.display());
    }
    if let Some(run_file) = &report.run_file {
        println!("Run report: {}", run_file.display());
    }
    if let Some(stdout_log) = &report.stdout_log {
        println!("Stdout JSONL log: {}", stdout_log.display());
    }
    if let Some(stderr_log) = &report.stderr_log {
        println!("Stderr log: {}", stderr_log.display());
    }
    if let Some(completion) = &report.completion {
        println!("Completion receipt: {:?}", completion.receipt.status);
        if let Some(completion_file) = &completion.completion_file {
            println!("Completion file: {}", completion_file.display());
        }
        if let Some(transcript_file) = &completion.transcript_file {
            println!("Transcript: {}", transcript_file.display());
        }
        if let Some(trajectory_file) = &completion.trajectory_file {
            println!("Trajectory: {}", trajectory_file.display());
        }
        if let Some(codex_binding_file) = &completion.codex_binding_file {
            println!("Codex binding: {}", codex_binding_file.display());
        }
    }
    if !report.warnings.is_empty() {
        println!("Warnings:");
        for warning in &report.warnings {
            println!("- {warning}");
        }
    }
}

fn print_codex_runtime_completion_report(report: &CodexRuntimeCompletionReport) {
    println!("Agent Codex runtime completion");
    println!("Harness home: {}", report.harness_home.display());
    println!("Receipts file: {}", report.receipts_file.display());
    println!("Receipt: {:?}", report.receipt.status);
    println!("Reason: {}", report.receipt.reason);
    if let Some(execution_dir) = &report.execution_dir {
        println!("Execution dir: {}", execution_dir.display());
    }
    if let Some(plan_file) = &report.plan_file {
        println!("Plan file: {}", plan_file.display());
    }
    if let Some(completion_file) = &report.completion_file {
        println!("Completion receipt: {}", completion_file.display());
    }
    if let Some(transcript_file) = &report.transcript_file {
        println!("Transcript: {}", transcript_file.display());
    }
    if let Some(trajectory_file) = &report.trajectory_file {
        println!("Trajectory: {}", trajectory_file.display());
    }
    if let Some(codex_binding_file) = &report.codex_binding_file {
        println!("Codex binding: {}", codex_binding_file.display());
    }
    if !report.warnings.is_empty() {
        println!("Warnings:");
        for warning in &report.warnings {
            println!("- {warning}");
        }
    }
}

fn print_prompt_bundle(bundle: &PromptBundle) {
    println!("Agent prompt bundle");
    println!("Dispatch: {:?}", bundle.dispatch);
    println!("Session key: {}", bundle.session_key);
    println!("Agent: {}", bundle.agent_id.as_deref().unwrap_or("-"));
    println!(
        "Model policy: provider={} model={}",
        bundle.provider.as_deref().unwrap_or("-"),
        bundle.model.as_deref().unwrap_or("-")
    );
    println!(
        "Sections: {} prompt_files={} reused_prompt_files={} skills={} reused_skills={} continuity={} user_messages={}",
        bundle.sections.len(),
        bundle.summary.prompt_files_included,
        bundle.summary.prompt_files_reused,
        bundle.summary.skills_included,
        bundle.summary.skills_reused,
        bundle.summary.session_continuity_sections_included,
        bundle.summary.user_messages_included
    );
    println!("Bytes included: {}", bundle.summary.bytes_included);
    println!("Truncated sections: {}", bundle.summary.truncated_sections);
    if !bundle.warnings.is_empty() {
        println!("Warnings:");
        for warning in &bundle.warnings {
            println!("- {warning}");
        }
    }
}

fn print_cron_plan(plan: &NativeCronPlan, limit: usize) {
    println!("Agent native cron plan");
    println!("Source home: {}", plan.source_home.display());
    println!("Now ms: {}", plan.now_ms);
    println!("Resume cron: {}", yes_no(plan.resume_enabled));
    println!("Jobs: {}", plan.summary.total_jobs);
    println!("Disabled: {}", plan.summary.disabled);
    println!("Cutover held: {}", plan.summary.cutover_held);
    println!("Enqueue agent turns: {}", plan.summary.enqueue_agent_turns);
    println!("Waiting schedule: {}", plan.summary.waiting_schedule);
    println!("Cron registered: {}", plan.summary.cron_registered);
    println!("Missing agent: {}", plan.summary.missing_agent);
    println!(
        "Unsupported schedule: {}",
        plan.summary.unsupported_schedule
    );
    if !plan.entries.is_empty() {
        println!();
        println!("Entries:");
        for entry in plan.entries.iter().take(limit) {
            println!(
                "- {} {:?} agent={} session={} reason={}",
                entry.job_id,
                entry.action,
                entry.agent_id.as_deref().unwrap_or("-"),
                entry.session_key.as_deref().unwrap_or("-"),
                entry.reason
            );
        }
        if plan.entries.len() > limit {
            println!("... {} more entries", plan.entries.len() - limit);
        }
    }
    if !plan.warnings.is_empty() {
        println!("Warnings:");
        for warning in &plan.warnings {
            println!("- {warning}");
        }
    }
}

fn print_deterministic_cron_plan(plan: &DeterministicCronPlan, limit: usize) {
    println!("Agent deterministic cron plan");
    println!("Source workspace: {}", plan.source_workspace.display());
    println!(
        "Allow deterministic run: {}",
        yes_no(plan.allow_deterministic_run)
    );
    println!("LLM access allowed: {}", yes_no(plan.llm_access_allowed));
    println!("Entries: {}", plan.summary.total_entries);
    println!("Cutover held: {}", plan.summary.cutover_held);
    println!("Ready commands: {}", plan.summary.ready_commands);
    println!(
        "Shell compatibility required: {}",
        plan.summary.shell_compatibility_required
    );
    println!("Missing script: {}", plan.summary.missing_script);
    println!(
        "External command review: {}",
        plan.summary.external_command_review
    );
    println!("Unsupported entries: {}", plan.summary.unsupported_entries);
    if !plan.entries.is_empty() {
        println!();
        println!("Entries:");
        for entry in plan.entries.iter().take(limit) {
            println!(
                "- {} {:?} script={} command={}",
                entry.entry_id,
                entry.action,
                entry
                    .script_path
                    .as_ref()
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|| "-".to_string()),
                entry.command
            );
        }
        if plan.entries.len() > limit {
            println!("... {} more entries", plan.entries.len() - limit);
        }
    }
    if !plan.warnings.is_empty() {
        println!("Warnings:");
        for warning in &plan.warnings {
            println!("- {warning}");
        }
    }
}

fn print_subagent_plan(plan: &SubagentPlan, limit: usize) {
    println!("Agent subagent plan");
    println!("Source home: {}", plan.source_home.display());
    println!("Resume subagents: {}", yes_no(plan.resume_subagents));
    println!("Runs: {}", plan.summary.total_runs);
    println!("Completed noop: {}", plan.summary.completed_noop);
    println!("Failed noop: {}", plan.summary.failed_noop);
    println!("Canceled noop: {}", plan.summary.canceled_noop);
    println!("Cutover held: {}", plan.summary.cutover_held);
    println!("Resume candidates: {}", plan.summary.resume_candidates);
    println!(
        "Unknown status review: {}",
        plan.summary.unknown_status_review
    );
    if !plan.entries.is_empty() {
        println!();
        println!("Entries:");
        for entry in plan.entries.iter().take(limit) {
            println!(
                "- {} {:?} agent={} parent={} session={} reason={}",
                entry.run_id,
                entry.action,
                entry.agent_id.as_deref().unwrap_or("-"),
                entry.parent_agent_id.as_deref().unwrap_or("-"),
                entry.session_key.as_deref().unwrap_or("-"),
                entry.reason
            );
        }
        if plan.entries.len() > limit {
            println!("... {} more entries", plan.entries.len() - limit);
        }
    }
    if !plan.warnings.is_empty() {
        println!("Warnings:");
        for warning in &plan.warnings {
            println!("- {warning}");
        }
    }
}

fn print_help() {
    println!("agent-harness");
    println!();
    println!("Commands:");
    println!("  doctor          Inspect a legacy source home directory");
    println!("  import-plan     Print staged import readiness");
    println!("  import-dry-run  Build a read-only migration report");
    println!("  import-execute  Copy planned non-sensitive state and write receipts");
    println!(
        "  channel-credentials-export Export Telegram/Discord tokens and IDs to harness secrets"
    );
    println!("  registry        Inspect parsed multi-agent registry state");
    println!("  registry-export Write target harness registry state");
    println!("  enable-check    Check formal activation readiness and log writability");
    println!(
        "  status          Summarize harness readiness, runtime, channels, memory, plugins, and logs"
    );
    println!("  config-validate Validate harness-config.json with fail-closed schema checks");
    println!("  log-rotate      Rotate harness.jsonl and receipt the rotation decision");
    println!("  queue-retry     Requeue one runtime item with a fresh retry queue id");
    println!("  queue-skip      Terminally skip one runtime item with a receipt");
    println!("  healthz         Emit readiness/liveness JSON for staging gates");
    println!("  trace           Reconstruct a queue/trace chain from local receipts");
    println!("  metrics         Emit local queue/runtime counters");
    println!("  supervise-evaluate Evaluate child loop state and restart/breaker decisions");
    println!("  deploy-canary-record Record fake/live canary commit or rollback decision");
    println!("  queue-shadow-record Record one runtime queue shadow row in SQLite");
    println!("  queue-shadow-compare Compare runtime JSONL queue against SQLite shadow");
    println!("  admission-check Evaluate quick refusal/admission policy");
    println!("  scoped-stop     Record scoped turn/queue/job stop marker");
    println!("  background-list List registered background tasks and stale heartbeats");
    println!("  background-upsert Upsert one background task registry record");
    println!("  token-efficiency Aggregate runtime token usage receipts");
    println!("  prompt-reduction Check prompt reduction and stable-prefix purity");
    println!("  task-write      Write durable task entity and checkpoint receipt");
    println!("  budget-acquire  Atomically acquire scoped budget or fail closed");
    println!("  learning-propose Create reviewed/quarantined learning proposal");
    println!("  drift-check     Compare intended vs active config hashes");
    println!("  vault-put       Store a named secret in repo-local encrypted vault");
    println!("  vault-get       Check encrypted vault secret presence without printing it");
    println!("  mcp-request     Handle one in-process MCP JSON-RPC request");
    println!("  security-scan   Scan prompt/shell trust-boundary inputs");
    println!("  context-pack-validate Validate bounded context-pack memory payload");
    println!("  tool-description-hash Hash MCP/tool descriptions for pinning");
    println!("  tool-pin-check  Verify a pinned MCP/tool description hash");
    println!("  invariants      Print reviewed runtime invariant catalog");
    println!("  schema-registry Print core schema registry entries");
    println!("  release-checklist Print release gate checklist");
    println!("  public-hygiene  Scan public tree for forbidden private/debug paths");
    println!("  jsonl-repair    Validate and compact a JSONL ledger");
    println!(
        "  memory-credentials-export Export imported memory embedding config to harness secrets"
    );
    println!("  memory-search   Search imported markdown/text memory files read-only");
    println!("  memory-vector-search Search imported SQLite vector memory with embedding query");
    println!("  memory-canvas-run Build compact symbolic canvas from captured candidates/episodes");
    println!("  memory-embedding-backfill Plan resumable memory embedding backfill batches");
    println!("  memory-hook     Record OpenClaw-compatible memory adapter hook receipt");
    println!("  memory-service-status Inspect OpenClaw memory service/snapshot adapter readiness");
    println!("  memory-service-recall Recall through OpenClaw memory service adapter");
    println!("  memory-service-propose Record a reviewed OpenClaw memory proposal");
    println!("  memory-service-store Store approved OpenClaw memory writeback");
    println!("  memory-read-path-smoke Run read-only memory bridge/coverage/scope smoke");
    println!("  memory-owner-ensure Ensure memory-owner state exists without promotion");
    println!("  memory-owner-endpoint-probe Record remote mem-engine endpoint contract probe");
    println!("  memory-owner-heartbeat Record mem-engine lease heartbeat receipt");
    println!("  memory-owner-shadow Record recall/store shadow parity receipt");
    println!("  memory-owner-trust-scope Record trust/scope gate receipt");
    println!("  memory-owner-local-prepare Prepare local in-process memory owner gates");
    println!("  memory-owner-promote Request gated mem-engine owner promotion");
    println!("  memory-owner-recover Recover stale mem-engine owner back to snapshot adapter");
    println!("  ops-backup      Copy non-secret harness state and write a backup manifest");
    println!("  ops-cutover-request Record an operator cutover request and ticket");
    println!("  ops-cutover-approve Issue a short-lived live-control token for a ticket");
    println!("  ops-cutover-apply Validate a live-control token before operator cutover");
    println!("  ops-cutover-status Inspect cutover tickets and optional token status");
    println!("  ops-cutover-receipt Record readiness summary for cutover audit");
    println!("  ops-control     Create/clear/inspect supervisor stop files");
    println!("  supervisor-reconcile Plan or launch configured supervisor-run loop owners");
    println!("  supervisor-run  Own and restart a low-risk child service");
    println!("  supervisor-plan Generate Windows scheduled-task scripts for harness loops");
    println!("  harness-skills-sync Sync bundled harness operation skills");
    println!("  skills          Build a skill-first index and optionally match a task");
    println!("  skill-propose   Record a checksum-guarded skill change proposal");
    println!("  skill-proposals List skill change proposals");
    println!("  skill-apply     Apply a reviewed skill proposal with stale-base quarantine");
    println!("  skill-reject    Reject a skill proposal");
    println!("  skill-archive   Record an archive proposal for a skill");
    println!("  turn-plan       Plan routing, commands, prompts, and skills for one turn");
    println!(
        "  operation-plan  Maintain durable multi-item operation plans and delegation receipts"
    );
    println!("  latency-status  Summarize runtime queue latency stages for one queue id");
    println!("  channel-step    Plan shared channel reply or agent dispatch for one DM");
    println!("  channel-apply   Persist channel command state and command receipts");
    println!("  channel-receive Handle one DM into command outbox or runtime queue");
    println!("  channel-run-once Handle one DM, run runtime if needed, and plan delivery");
    println!("  channel-outbox-plan List pending Telegram/Discord delivery messages");
    println!("  channel-delivery-record Record delivery success or retryable failure");
    println!("  channel-identity-check Verify platform/account/channel binding before ingress");
    println!("  progress-delivery-once Send/edit compact runtime progress panels once");
    println!("  progress-delivery-loop Send/edit compact runtime progress panels continuously");
    println!("  telegram-probe  Probe Telegram Bot API getMe without consuming updates");
    println!("  telegram-poll-once Poll Telegram once, run DM pipeline, and deliver replies");
    println!("  telegram-loop     Run Telegram polling continuously until stopped");
    println!("  discord-outbox-send-once Send pending Discord outbox messages once");
    println!("  discord-outbox-loop Send pending Discord outbox messages continuously");
    println!("  discord-dm-probe Create a Discord DM channel and optionally send a probe");
    println!("  discord-dm-history-probe Read Discord DM message metadata without content");
    println!("  discord-event-run-once Normalize one Discord Gateway message event");
    println!("  discord-gateway-probe Probe Discord Gateway loop prerequisites");
    println!("  discord-gateway-loop Run Discord Gateway receive loop");
    println!("  plugin-sidecar-probe Probe the Node legacy plugin sidecar contract");
    println!("  plugin-sidecar-call Call the plugin sidecar JSON-RPC bridge once");
    println!("  queue-enqueue   Persist one channel agent turn to the runtime queue");
    println!("  queue-prepare   Prepare one queued runtime item for Codex execution");
    println!("  runtime-run-once Prepare, run, and outbox one queued runtime item");
    println!("  runtime-loop    Drain runtime queue until stopped, idle, or error threshold");
    println!("  runtime-lease-reconcile Reap leases owned by an exited runtime generation");
    println!("  worker-enqueue  Persist deterministic, subagent, watchdog, or wakeup worker job");
    println!("  worker-run-once Lease and execute one worker-dispatch job");
    println!("  worker-loop     Drain worker jobs until stopped, idle, or error threshold");
    println!("  worker-status   Summarize worker queue status and concurrency blockers");
    println!("  worker-cancel   Cancel a pending/running worker job");
    println!("  worker-reap-stale Recover expired worker leases");
    println!("  codex-plan      Plan Codex app-server invocation for prepared execution");
    println!("  codex-preflight Check a Codex runtime plan before process start");
    println!("  codex-launch-probe Start and stop Codex app-server without a model request");
    println!("  codex-run       Run a prepared Codex app-server turn and record completion");
    println!("  codex-complete  Record assistant output to transcript and trajectory");
    println!("  prompt-bundle   Assemble prompt files, selected skills, and message");
    println!("  cron-plan       Dry-run legacy native agent-turn cron dispatch");
    println!("  native-cron-enqueue Persist native cron work into worker dispatch");
    println!("  cron-scheduler-lint Read-only scheduler config/source lint before enabling cron");
    println!("  cron-scheduler-run-once Tick cron registries/crontabs into worker jobs");
    println!("  cron-scheduler-loop Run the cron scheduler tick loop until stopped");
    println!("  cron-runs       List CronRun active/terminal/quarantine state");
    println!("  cron-run-control Skip, retry, quarantine, or unquarantine CronRun state");
    println!("  deterministic-cron-plan Dry-run deterministic cron without LLM access");
    println!("  deterministic-cron-enqueue Persist deterministic cron into worker dispatch");
    println!("  context-rollover Requeue prepared context rollover items safely");
    println!("  subagent-plan   Dry-run subagent ledger cutover/resume planning");
    println!("  subagent-enqueue Persist resumable subagent work into worker dispatch");
    println!("  subagent-lifecycle Show, close, or smoke-test subagent lifecycle receipts");
    println!();
    println!("Options:");
    println!(
        "  --source-home <path>  Source/config authority; live uses .agent-harness, .openclaw is retired/import-only"
    );
    println!("  --workspace <path>      Override workspace directory");
    println!(
        "  --runtime-workspace <path> Codex cwd; prompt files still come from --workspace/source"
    );
    println!("  --target-home <path>    Destination harness home; alias of --harness-home");
    println!("  --harness-home <path>   Harness state root for runtime/channel commands");
    println!("  --force                 Overwrite user-modified builtin harness skills");
    println!("  --conflict <policy>     skip, overwrite, or rename");
    println!("  --output <path>         Write report.json and summary.md");
    println!("  --include-sensitive     Copy/write sensitive import or credential values");
    println!("  --json                  Print machine-readable JSON for status");
    println!("  --run-id <id>           CronRun id for list/control filters");
    println!("  --entry-id <id>         Cron job entry id for list/control filters");
    println!("  --status <name>         CronRun status filter for cron-runs");
    println!("  --path <file>           JSONL ledger path for jsonl-repair");
    println!("  --apply                 Apply jsonl-repair after writing backup/repaired files");
    println!("  --invalid-output <file> Invalid-line quarantine output for jsonl-repair");
    println!("  --task-prefix <name>    Windows scheduled-task name prefix for supervisor-plan");
    println!("  --query <text>          Match skills or search imported memory");
    println!("  --agent <id>            Agent hint for skill matching");
    println!("  --telegram-account <id> Telegram account token/offset selector");
    println!("  --discord-account <id> Discord account token/outbox/gateway selector");
    println!("  --channel <name>        Channel hint for skill matching");
    println!("  --match-workspace <txt> Workspace hint for skill matching");
    println!("  --limit <n>             Maximum matched skills to print");
    println!("  --max-file-bytes <n>    Maximum imported memory file size for memory-search");
    println!("  --no-receipt            Do not write memory search/vector-search probe receipts");
    println!("  --text <text>           Memory-service proposal/store text");
    println!("  --text-file <path>      Memory-service proposal/store text file");
    println!("  --approved              Approve memory-service-store writeback");
    println!("  --lane <name>           Worker lane or memory embedding backfill lane");
    println!("  --model <name>          Embedding model namespace for memory backfill");
    println!("  --vector-dimension <n>  Expected embedding vector dimension");
    println!("  --batch-size <n>        Memory embedding backfill batch size");
    println!("  --rate-limit-per-minute <n> Memory embedding backfill lane rate cap");
    println!("  --coverage-threshold-bps <n> Coverage bps required before parity claim");
    println!("  --message <text>        Incoming channel message for turn-plan");
    println!("  --platform <name>       local, telegram, discord, or cron");
    println!("  --channel-id <id>       Channel identity for session mapping");
    println!("  --user-id <id>          User identity for session mapping");
    println!("  --session-key <key>     Existing session key override");
    println!("  --delivery-id <id>      Channel outbox delivery id");
    println!("  --status <value>        Delivery status: delivered or failed");
    println!("  --provider-message-id <id> Telegram/Discord message id after delivery");
    println!("  --error <text>          Delivery failure reason");
    println!(
        "  --outbox-limit <n>      Maximum pending outbox details for channel-run-once/outbox-plan"
    );
    println!("  --min-update-interval-ms <n> Minimum progress panel edit interval");
    println!("  --max-events <n>        Maximum action lines shown in a progress panel");
    println!("  --max-preview-chars <n> Maximum preview characters per progress action");
    println!(
        "  --current-step-max-chars <n> Maximum preview characters for current-step narration"
    );
    println!("  --timeout-ms <n>        Maximum Codex turn runtime before hard timeout");
    println!("  --idle-timeout-ms <n>   Codex JSONL inactivity timeout, renewed on each event");
    println!("  --event-file <path>     Discord Gateway event JSON file");
    println!("  --event-json <text>     Discord Gateway event JSON text");
    println!("  --gateway-script <path> Discord Gateway Node script path");
    println!("  --harness-cli <path>    Harness CLI used by gateway loop callbacks");
    println!("  --no-runtime            Exclude runtime-loop from supervisor-plan");
    println!("  --runtime-workers <n>   In-process runtime concurrency for supervisor-plan");
    println!("  --runtime-timeout-ms <n> Maximum Codex turn runtime in supervisor runtime-loop");
    println!(
        "  --runtime-idle-timeout-ms <n> Codex JSONL inactivity timeout in supervisor runtime-loop"
    );
    println!("  --no-worker             Exclude worker-loop from supervisor-plan");
    println!("  --no-progress           Exclude progress-delivery-loop from supervisor-plan");
    println!("  --no-telegram           Exclude telegram-loop from supervisor-plan");
    println!("  --no-discord            Exclude discord-gateway-loop from supervisor-plan");
    println!(
        "  --max-messages <n>      Stop Discord gateway loop after n messages; 0 means forever"
    );
    println!("  --poll-timeout-seconds <n> Telegram long-poll timeout for telegram-poll-once");
    println!("  --max-updates <n>       Maximum Telegram updates for telegram-poll-once");
    println!(
        "  --iterations <n>        Loop iterations for telegram-loop/runtime-loop; 0 means forever"
    );
    println!(
        "  --idle-ms <n>           Loop sleep override; cron scheduler defaults to cronScheduler.intervalMs"
    );
    println!("  --max-consecutive-errors <n> Loop failure threshold");
    println!("  --safe-mode-restart-ms <n> Runtime-loop cooldown before service-mode retry");
    println!("  --no-safe-mode-restart Disable runtime-loop service-mode retry");
    println!(
        "  --loop-name <name>      Heartbeat component name for loop commands that support it"
    );
    println!("  --runtime-concurrency <n> Bounded in-process runtime tasks for runtime-loop");
    println!("  --stop-when-idle        Stop runtime-loop when the runtime queue is idle");
    println!("  --stop-file <path>      Stop-file path for runtime, Telegram, or Discord loops");
    println!("  TELEGRAM_BOT_TOKEN      Env var or harness secret used by Telegram adapters");
    println!("  AGENT_HARNESS_TELEGRAM_ACCOUNT_<ID>_BOT_TOKEN Account-specific Telegram token");
    println!("  DISCORD_BOT_TOKEN       Env var or harness secret used by Discord adapters");
    println!("  AGENT_HARNESS_DISCORD_ACCOUNT_<ID>_BOT_TOKEN Account-specific Discord token");
    println!("  --node-exe <path>       Node executable for plugin sidecar commands");
    println!("  --sidecar-script <path> Plugin sidecar script path");
    println!("  --method <name>         Plugin sidecar JSON-RPC method for plugin-sidecar-call");
    println!("  --params-json <object>  JSON-RPC params object for plugin-sidecar-call");
    println!(
        "  --hook <name>           Memory hook: before-prompt-build, agent-end, store-propose"
    );
    println!("  --payload <json>        JSON payload for worker-enqueue or memory-hook");
    println!("  --payload-file <path>   JSON payload file for worker-enqueue or memory-hook");
    println!("  --action <name>         CronRun, ops-control, or live-control action name");
    println!("  --ticket-id <id>        Cutover approval/apply ticket id");
    println!("  --summary <text>        Cutover request summary");
    println!("  --candidate-binary <path> Staged binary intended for cutover");
    println!("  --staging-home <path>   Staged harness home used for validation");
    println!("  --test-note <text>      Test evidence for cutover request; repeatable");
    println!("  --issued-to <name>      Operator/agent recipient for live-control token");
    println!("  --ttl-seconds <n>       Live-control token lifetime for cutover approval");
    println!("  --live-control-token <token> Token required for live gateway stop/start");
    println!("  --label <name>          Backup label for ops-backup");
    println!("  --summary-only          Print compact ops-backup receipt summary");
    println!("  --queue-id <id>         Select one runtime queue item for queue-prepare");
    println!("  --execution-dir <path>  Prepared execution directory for codex-plan");
    println!("  --codex-exe <path>      Codex executable path for codex-plan/runtime-run-once");
    println!("  --plan-file <path>      Codex runtime plan file for codex-preflight");
    println!("  --startup-probe-ms <n>  Milliseconds to keep app-server alive for launch probe");
    println!("  --timeout-ms <n>        Milliseconds to wait for codex-run completion");
    println!("  --assistant-message <text> Assistant output for codex-complete");
    println!("  --skill-limit <n>       Maximum selected skills for turn-plan");
    println!("  --max-prompt-file-bytes <n> Cap each prompt file in prompt-bundle");
    println!("  --max-skill-file-bytes <n>  Cap each skill file in prompt-bundle");
    println!(
        "  --now-ms <n>           Epoch milliseconds for channel-apply, cron-plan, or queue-enqueue"
    );
    println!("  --resume-cron          Release native cron from cutover hold in dry-run");
    println!("  --include-registered-cron Enqueue registered cron entries for a scheduler tick");
    println!("  --include-cron-scheduler Include cron-scheduler-loop in supervisor-plan");
    println!("  --allow-deterministic-run Release deterministic cron hold in dry-run");
    println!(
        "  --execute-shell        Allow deterministic-cron-enqueue jobs to execute shell scripts"
    );
    println!("  --resume-subagents    Mark queued/running subagents as resume candidates");
    println!("  --master-agent <id>    Master agent to wake after worker group completion");
    println!("  --master-session <key> Master session key for worker group wakeup");
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_harness_core::{AGENT_HARNESS_CONTEXT_PACK_SCHEMA, OPENCLAW_MEM_CONTEXT_PACK_SCHEMA};
    use std::time::{SystemTime, UNIX_EPOCH};

    struct StaticDiscordAttachmentFetcher {
        bytes_by_url: BTreeMap<String, Vec<u8>>,
    }

    impl StaticDiscordAttachmentFetcher {
        fn new(entries: Vec<(&str, Vec<u8>)>) -> Self {
            Self {
                bytes_by_url: entries
                    .into_iter()
                    .map(|(url, bytes)| (url.to_string(), bytes))
                    .collect(),
            }
        }
    }

    impl DiscordAttachmentFetcher for StaticDiscordAttachmentFetcher {
        fn fetch_attachment(&self, url: &str, _max_bytes: usize) -> Result<Vec<u8>, String> {
            self.bytes_by_url
                .get(url)
                .cloned()
                .ok_or_else(|| format!("missing fixture for {url}"))
        }
    }

    fn set(values: &[&str]) -> BTreeSet<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    fn init_temp_git_repo(workspace: &Path) -> bool {
        std::process::Command::new("git")
            .arg("init")
            .arg("--quiet")
            .current_dir(workspace)
            .output()
            .is_ok_and(|output| output.status.success())
    }

    #[test]
    fn worker_loop_wake_lane_matches_enqueue_signal_names() {
        assert_eq!(worker_loop_wake_lane(None), "worker");
        assert_eq!(worker_loop_wake_lane(Some("shell")), "worker-shell");
        assert_eq!(
            worker_loop_wake_lane(Some("learning review")),
            "worker-learning-review"
        );
    }

    #[test]
    fn operation_plan_cli_accepts_agent_id_alias_for_create() {
        let root = cli_temp_root("operation_plan_cli_accepts_agent_id_alias_for_create");
        let harness_home = root.join(".agent-harness");

        run_operation_plan(&[
            "--target-home".to_string(),
            harness_home.display().to_string(),
            "--action".to_string(),
            "create".to_string(),
            "--plan-id".to_string(),
            "alias-plan".to_string(),
            "--session-key".to_string(),
            "session-1".to_string(),
            "--agent-id".to_string(),
            "main".to_string(),
            "--goal".to_string(),
            "Verify operation plan CLI alias".to_string(),
            "--now-ms".to_string(),
            "1000".to_string(),
        ])
        .unwrap();

        let report = show_operation_plan(OperationPlanShowOptions {
            harness_home: harness_home.clone(),
            plan_id: "alias-plan".to_string(),
        })
        .unwrap();
        assert_eq!(report.plan.agent_id, "main");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn operation_plan_prompt_commands_match_cli_parser() {
        let root = cli_temp_root("operation_plan_prompt_commands_match_cli_parser");
        let harness_home = root.join(".agent-harness");

        run_operation_plan(&[
            "--target-home".to_string(),
            harness_home.display().to_string(),
            "--action".to_string(),
            "create".to_string(),
            "--plan-id".to_string(),
            "parser-plan".to_string(),
            "--session-key".to_string(),
            "session-1".to_string(),
            "--agent".to_string(),
            "main".to_string(),
            "--goal".to_string(),
            "Verify documented OperationPlan command shapes".to_string(),
            "--now-ms".to_string(),
            "1000".to_string(),
        ])
        .unwrap();
        run_operation_plan(&[
            "--target-home".to_string(),
            harness_home.display().to_string(),
            "--action".to_string(),
            "add-item".to_string(),
            "--plan-id".to_string(),
            "parser-plan".to_string(),
            "--item-id".to_string(),
            "parser-item".to_string(),
            "--title".to_string(),
            "Parser item".to_string(),
            "--body".to_string(),
            "Exercise documented update command.".to_string(),
            "--now-ms".to_string(),
            "1001".to_string(),
        ])
        .unwrap();
        let show = show_operation_plan(OperationPlanShowOptions {
            harness_home: harness_home.clone(),
            plan_id: "parser-plan".to_string(),
        })
        .unwrap();
        let item = show
            .items
            .iter()
            .find(|item| item.item_id == "parser-item")
            .unwrap();
        run_operation_plan(&[
            "--target-home".to_string(),
            harness_home.display().to_string(),
            "--action".to_string(),
            "update-item".to_string(),
            "--plan-id".to_string(),
            "parser-plan".to_string(),
            "--item-id".to_string(),
            "parser-item".to_string(),
            "--expected-version".to_string(),
            item.version.to_string(),
            "--status".to_string(),
            "ready".to_string(),
            "--add-evidence".to_string(),
            "parser accepted documented update command".to_string(),
            "--now-ms".to_string(),
            "1002".to_string(),
        ])
        .unwrap();
        run_operation_plan(&[
            "--target-home".to_string(),
            harness_home.display().to_string(),
            "--action".to_string(),
            "add-item".to_string(),
            "--plan-id".to_string(),
            "parser-plan".to_string(),
            "--item-id".to_string(),
            "delegate-item".to_string(),
            "--title".to_string(),
            "Delegate item".to_string(),
            "--body".to_string(),
            "Exercise documented delegate command.".to_string(),
            "--now-ms".to_string(),
            "1003".to_string(),
        ])
        .unwrap();
        let show = show_operation_plan(OperationPlanShowOptions {
            harness_home: harness_home.clone(),
            plan_id: "parser-plan".to_string(),
        })
        .unwrap();
        let delegate_item = show
            .items
            .iter()
            .find(|item| item.item_id == "delegate-item")
            .unwrap();
        run_operation_plan(&[
            "--target-home".to_string(),
            harness_home.display().to_string(),
            "--action".to_string(),
            "delegate".to_string(),
            "--plan-id".to_string(),
            "parser-plan".to_string(),
            "--item-id".to_string(),
            "delegate-item".to_string(),
            "--expected-version".to_string(),
            delegate_item.version.to_string(),
            "--assignee".to_string(),
            "subagent-reviewer".to_string(),
            "--idempotency-key".to_string(),
            "delegate-item:reviewer".to_string(),
            "--now-ms".to_string(),
            "1004".to_string(),
        ])
        .unwrap();

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn operation_plan_direct_todo_done_transition_is_rejected_by_cli() {
        let root = cli_temp_root("operation_plan_direct_todo_done_transition_is_rejected_by_cli");
        let harness_home = root.join(".agent-harness");

        run_operation_plan(&[
            "--target-home".to_string(),
            harness_home.display().to_string(),
            "--action".to_string(),
            "create".to_string(),
            "--plan-id".to_string(),
            "transition-plan".to_string(),
            "--session-key".to_string(),
            "session-1".to_string(),
            "--agent".to_string(),
            "main".to_string(),
            "--goal".to_string(),
            "Reject direct todo to done".to_string(),
            "--now-ms".to_string(),
            "1000".to_string(),
        ])
        .unwrap();
        run_operation_plan(&[
            "--target-home".to_string(),
            harness_home.display().to_string(),
            "--action".to_string(),
            "add-item".to_string(),
            "--plan-id".to_string(),
            "transition-plan".to_string(),
            "--item-id".to_string(),
            "todo-item".to_string(),
            "--title".to_string(),
            "Todo item".to_string(),
            "--body".to_string(),
            "This item must not jump straight to done.".to_string(),
            "--now-ms".to_string(),
            "1001".to_string(),
        ])
        .unwrap();
        let show = show_operation_plan(OperationPlanShowOptions {
            harness_home: harness_home.clone(),
            plan_id: "transition-plan".to_string(),
        })
        .unwrap();
        let item = show.items.first().unwrap();
        let error = run_operation_plan(&[
            "--target-home".to_string(),
            harness_home.display().to_string(),
            "--action".to_string(),
            "update-item".to_string(),
            "--plan-id".to_string(),
            "transition-plan".to_string(),
            "--item-id".to_string(),
            "todo-item".to_string(),
            "--expected-version".to_string(),
            item.version.to_string(),
            "--status".to_string(),
            "done".to_string(),
            "--now-ms".to_string(),
            "1002".to_string(),
        ])
        .unwrap_err();
        assert!(error.contains("invalid transition"), "{error}");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn subagent_no_write_smoke_records_harness_state_writes_and_no_workspace_diff() {
        let root = cli_temp_root(
            "subagent_no_write_smoke_records_harness_state_writes_and_no_workspace_diff",
        );
        let workspace = root.join("workspace");
        let harness_home = workspace.join(".agent-harness");
        fs::create_dir_all(&workspace).unwrap();
        fs::write(workspace.join("AGENTS.md"), "# Test Agents\n").unwrap();

        let report = run_subagent_lifecycle_smoke(&SubagentLifecycleArgs {
            target_home: harness_home.clone(),
            source_home: workspace.clone(),
            workspace: workspace.clone(),
            action: "smoke".to_string(),
            subagent_id: Some("subagent:smoke-test".to_string()),
            model: "gpt-5.3-codex-spark".to_string(),
            timeout_ms: 180_000,
            no_write: true,
            reason: None,
            now_ms: 1000,
        })
        .unwrap();

        assert_eq!(report.workspace_write_policy, "no-write");
        assert!(report.workspace_clean, "{:?}", report.workspace_diff);
        assert!(report.workspace_diff.is_empty());
        assert!(!report.harness_state_writes.is_empty());
        assert_eq!(report.run.status, WorkerRunOnceStatus::Completed);
        assert_eq!(
            report.runtime_execution_mode,
            "deterministic-terminal-receipt-no-model-execution"
        );
        let terminal_file = report.runtime_terminal_receipt_file.as_ref().unwrap();
        assert!(terminal_file.is_file());
        let terminal_text = fs::read_to_string(terminal_file).unwrap();
        assert!(terminal_text.contains("\"status\":\"skipped\""));
        assert!(
            report
                .lifecycle
                .receipt
                .terminal_receipt_file
                .as_ref()
                .is_some_and(|path| path == terminal_file)
        );
        assert_eq!(
            report.completed_lifecycle.receipt.state,
            agent_harness_core::SubagentLifecycleState::Completed
        );
        assert_eq!(
            report.lifecycle.receipt.state,
            agent_harness_core::SubagentLifecycleState::AlreadyClosed
        );
        assert_eq!(
            report.close_report.receipt.state,
            agent_harness_core::SubagentLifecycleState::AlreadyClosed
        );
        assert_eq!(report.lifecycle.receipt.auth_visibility, "unverified");
        assert!(
            report
                .lifecycle
                .receipt
                .auth_visibility_reason
                .contains("Codex-auth status is unverified")
        );
        assert!(
            report
                .close_report
                .receipt
                .cleanup
                .diagnostic
                .as_deref()
                .unwrap_or_default()
                .contains("close accepted idempotently")
        );
        assert!(report.prompt.contains("Do not edit files."));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn subagent_no_write_smoke_rejects_workspace_as_target_home() {
        let root = cli_temp_root("subagent_no_write_smoke_rejects_workspace_as_target_home");
        let workspace = root.join("workspace");
        fs::create_dir_all(&workspace).unwrap();

        let error = run_subagent_lifecycle_smoke(&SubagentLifecycleArgs {
            target_home: workspace.clone(),
            source_home: workspace.clone(),
            workspace: workspace.clone(),
            action: "smoke".to_string(),
            subagent_id: Some("subagent:bad-smoke".to_string()),
            model: "gpt-5.3-codex-spark".to_string(),
            timeout_ms: 180_000,
            no_write: true,
            reason: None,
            now_ms: 1000,
        })
        .unwrap_err();

        assert!(error.contains("not equal"), "{error}");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn workspace_status_snapshot_detects_untracked_content_changes() {
        let root = cli_temp_root("workspace_status_snapshot_detects_untracked_content_changes");
        let workspace = root.join("workspace");
        let harness_home = workspace.join(".agent-harness");
        fs::create_dir_all(&workspace).unwrap();
        if !init_temp_git_repo(&workspace) {
            let _ = fs::remove_dir_all(root);
            return;
        }
        let scratch = workspace.join("scratch.txt");
        fs::write(&scratch, "before").unwrap();

        let before = workspace_status_snapshot(&workspace, &harness_home).unwrap();
        fs::write(&scratch, "after!").unwrap();
        let after = workspace_status_snapshot(&workspace, &harness_home).unwrap();
        let diff = snapshot_diff(&before.entries, &after.entries);

        assert!(
            diff.iter().any(|entry| entry.path.contains("scratch.txt")),
            "{diff:?}"
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn workspace_status_snapshot_filters_only_exact_harness_path() {
        let root = cli_temp_root("workspace_status_snapshot_filters_only_exact_harness_path");
        let workspace = root.join("workspace");
        let harness_home = workspace.join(".agent-harness");
        fs::create_dir_all(harness_home.join("state")).unwrap();
        fs::create_dir_all(workspace.join(".agent-harness-old")).unwrap();
        if !init_temp_git_repo(&workspace) {
            let _ = fs::remove_dir_all(root);
            return;
        }
        fs::write(harness_home.join("state").join("ignored.txt"), "harness").unwrap();
        fs::write(
            workspace.join(".agent-harness-old").join("visible.txt"),
            "workspace",
        )
        .unwrap();

        let snapshot = workspace_status_snapshot(&workspace, &harness_home).unwrap();

        assert!(
            snapshot
                .entries
                .keys()
                .any(|path| path.contains(".agent-harness-old/visible.txt")),
            "{:?}",
            snapshot.entries
        );
        assert!(
            !snapshot
                .entries
                .keys()
                .any(|path| path.contains(".agent-harness/state/ignored.txt")),
            "{:?}",
            snapshot.entries
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn supervisor_reconcile_default_services_include_stop_files() {
        let root = cli_temp_root("supervisor_reconcile_default_services_include_stop_files");
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            r#"{
              "supervisor": {
                "enabled": true,
                "manageAllLoops": true,
                "telegramLoops": [
                  {"serviceId":"telegram-loop-xiaoxiaoli", "account":"xiaoxiaoli", "agent": "xiaoxiaoli", "enabled": true}
                ]
              },
              "cronScheduler": {"enabled": true}
            }"#,
        )
        .unwrap();

        let args = supervisor_reconcile_args_from_args(&[
            "--target-home".to_string(),
            harness_home.display().to_string(),
            "--all".to_string(),
        ])
        .unwrap();
        let services = supervisor_reconcile_desired_services(&args).unwrap();
        let runtime = services
            .iter()
            .find(|service| service.service_id == "runtime-loop")
            .unwrap();
        let runtime_stop = harness_home
            .join("state")
            .join("supervisor")
            .join("stop")
            .join("runtime-loop.stop")
            .display()
            .to_string();
        assert!(
            runtime
                .args
                .windows(2)
                .any(|pair| { pair[0] == "--stop-file" && pair[1] == runtime_stop })
        );
        assert!(
            runtime
                .args
                .windows(2)
                .any(|pair| pair[0] == "--idle-ms" && pair[1] == "1000")
        );
        assert!(
            runtime
                .args
                .windows(2)
                .any(|pair| pair[0] == "--runtime-concurrency" && pair[1] == "12")
        );

        let xiaoxiaoli = services
            .iter()
            .find(|service| service.service_id == "telegram-loop-xiaoxiaoli")
            .unwrap();
        let xiaoxiaoli_stop = harness_home
            .join("state")
            .join("supervisor")
            .join("stop")
            .join("telegram-loop-xiaoxiaoli.stop")
            .display()
            .to_string();
        assert!(
            xiaoxiaoli
                .args
                .windows(2)
                .any(|pair| { pair[0] == "--stop-file" && pair[1] == xiaoxiaoli_stop })
        );
        assert!(
            xiaoxiaoli
                .args
                .windows(2)
                .any(|pair| { pair[0] == "--telegram-account" && pair[1] == "xiaoxiaoli" })
        );
        assert!(
            xiaoxiaoli
                .args
                .windows(2)
                .any(|pair| { pair[0] == "--agent" && pair[1] == "xiaoxiaoli" }),
            "{:?}",
            xiaoxiaoli.args
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn context_pack_cli_report_includes_schema_translation_fields() {
        let report = parse_context_pack(ContextPackParseOptions {
            raw_json: serde_json::json!({
                "schema": OPENCLAW_MEM_CONTEXT_PACK_SCHEMA,
                "packId": "pack-1",
                "source": "openclaw-mem",
                "items": [
                    {"citationId":"obs:1","text":"memory text","sourceUri":"memory://obs/1"}
                ]
            })
            .to_string(),
            max_bytes: 4096,
        });

        assert!(report.accepted, "{:?}", report.warnings);
        let value = serde_json::to_value(report).unwrap();
        assert_eq!(
            value.get("inputSchema").and_then(serde_json::Value::as_str),
            Some(OPENCLAW_MEM_CONTEXT_PACK_SCHEMA)
        );
        assert_eq!(
            value
                .get("normalizedSchema")
                .and_then(serde_json::Value::as_str),
            Some(AGENT_HARNESS_CONTEXT_PACK_SCHEMA)
        );
        assert_eq!(
            value
                .get("translationApplied")
                .and_then(serde_json::Value::as_bool),
            Some(true)
        );
    }

    fn progress_pending(
        platform: &str,
        channel_id: &str,
        user_id: &str,
    ) -> AgentProgressDeliveryPending {
        AgentProgressDeliveryPending {
            queue_id: "queue-1".to_string(),
            agent_id: Some("main".to_string()),
            platform: platform.to_string(),
            account_id: None,
            channel_id: channel_id.to_string(),
            thread_id: None,
            user_id: user_id.to_string(),
            session_key: "session-1".to_string(),
            message_kind: agent_harness_core::AgentProgressDeliveryMessageKind::Body,
            action: AgentProgressDeliveryAction::Send,
            provider_message_id: None,
            event_line: 1,
            terminal: false,
            text: "progress".to_string(),
            text_hash: "hash".to_string(),
            started_at_ms: 1,
            latest_at_ms: 1,
        }
    }

    fn progress_args() -> ProgressDeliveryOnceArgs {
        ProgressDeliveryOnceArgs {
            target_home: PathBuf::new(),
            platform: Some("telegram".to_string()),
            telegram_account: Some("default".to_string()),
            min_update_interval_ms: 0,
            max_events_per_panel: 8,
            max_preview_chars: 120,
            current_step_max_chars: 1200,
            preempt_after_wake_sequence: None,
        }
    }

    #[test]
    fn progress_delivery_prioritizes_terminal_status_edits() {
        let mut nonterminal_status = progress_pending("telegram", "dm-1", "operator");
        nonterminal_status.message_kind =
            agent_harness_core::AgentProgressDeliveryMessageKind::Status;
        nonterminal_status.action = AgentProgressDeliveryAction::Edit;
        nonterminal_status.provider_message_id = Some("status-1".to_string());
        nonterminal_status.event_line = 10;

        let mut terminal_body = progress_pending("telegram", "dm-1", "operator");
        terminal_body.action = AgentProgressDeliveryAction::Edit;
        terminal_body.provider_message_id = Some("body-1".to_string());
        terminal_body.terminal = true;
        terminal_body.event_line = 11;

        let mut terminal_status = terminal_body.clone();
        terminal_status.message_kind = agent_harness_core::AgentProgressDeliveryMessageKind::Status;
        terminal_status.provider_message_id = Some("status-1".to_string());
        terminal_status.event_line = 12;

        let mut pending = vec![nonterminal_status, terminal_body, terminal_status];
        pending.sort_by_key(progress_delivery_pending_priority);

        assert!(pending[0].terminal);
        assert_eq!(
            pending[0].message_kind,
            agent_harness_core::AgentProgressDeliveryMessageKind::Status
        );
    }

    #[test]
    fn progress_delivery_preempts_nonterminal_pending_when_wake_advances() {
        let stale = progress_pending("telegram", "dm-1", "operator");
        assert!(progress_delivery_should_preempt_stale_pending(
            &stale, 10, 11
        ));

        let mut terminal = stale.clone();
        terminal.terminal = true;
        assert!(!progress_delivery_should_preempt_stale_pending(
            &terminal, 10, 11
        ));
        assert!(!progress_delivery_should_preempt_stale_pending(
            &stale, 10, 10
        ));
    }

    fn cli_temp_root(name: &str) -> PathBuf {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let path = std::env::temp_dir().join(format!(
            "agent-harness-cli-test-{name}-{}-{millis}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&path);
        path
    }

    #[test]
    fn supervisor_reconcile_defaults_live_workspace_to_harness_parent() {
        let root = cli_temp_root("supervisor_reconcile_defaults_live_workspace_to_harness_parent");
        let harness_home = root.join(".agent-harness");
        fs::create_dir_all(&harness_home).unwrap();
        fs::write(
            harness_home.join("harness-config.json"),
            serde_json::json!({
                "supervisor": {
                    "enabled": true,
                    "manageAllLoops": true,
                    "telegramLoops": [{
                        "serviceId": "telegram-loop-xiaoxiaoli",
                        "account": "xiaoxiaoli",
                        "agent": "xiaoxiaoli",
                        "enabled": true
                    }]
                }
            })
            .to_string(),
        )
        .unwrap();

        let args = supervisor_reconcile_args_from_args(&[
            "--target-home".to_string(),
            harness_home.display().to_string(),
            "--all".to_string(),
        ])
        .unwrap();

        assert_eq!(args.workspace.as_deref(), Some(root.as_path()));
        assert_eq!(args.runtime_workspace.as_deref(), Some(root.as_path()));

        let root_text = root.to_string_lossy().to_string();
        let services = supervisor_reconcile_desired_services(&args).unwrap();
        let runtime = services
            .iter()
            .find(|service| service.service_id == "runtime-loop")
            .unwrap();
        let runtime_workspace = arg_value(&runtime.args, "--runtime-workspace");
        let workspace = arg_value(&runtime.args, "--workspace");
        assert_eq!(workspace.as_deref(), Some(root_text.as_str()));
        assert_eq!(runtime_workspace.as_deref(), Some(root_text.as_str()));

        let xiaoxiaoli = services
            .iter()
            .find(|service| service.service_id == "telegram-loop-xiaoxiaoli")
            .unwrap();
        assert_eq!(
            arg_value(&xiaoxiaoli.args, "--runtime-workspace").as_deref(),
            Some(root_text.as_str())
        );
        assert_eq!(
            arg_value(&xiaoxiaoli.args, "--agent").as_deref(),
            Some("xiaoxiaoli")
        );
        assert_eq!(
            arg_value(&xiaoxiaoli.args, "--telegram-account").as_deref(),
            Some("xiaoxiaoli")
        );

        let _ = fs::remove_dir_all(root);
    }

    fn arg_value(args: &[String], flag: &str) -> Option<String> {
        args.windows(2)
            .find(|pair| pair[0] == flag)
            .map(|pair| pair[1].clone())
    }

    #[test]
    fn write_loop_heartbeat_replaces_json_atomically() {
        let root = cli_temp_root("write_loop_heartbeat_replaces_json_atomically");
        let harness_home = root.join(".agent-harness");
        let expected_observed_only =
            env_bool("AGENT_HARNESS_SUPERVISOR_OBSERVED_ONLY").unwrap_or(true);

        write_loop_heartbeat(&harness_home, "runtime-loop", "running", 1, "first").unwrap();
        write_loop_heartbeat(&harness_home, "runtime-loop", "no-work", 2, "second").unwrap();

        let heartbeat_dir = harness_home
            .join("state")
            .join("supervisor")
            .join("loop-heartbeats");
        let heartbeat_file = heartbeat_dir.join("runtime-loop.json");
        let value: serde_json::Value =
            serde_json::from_slice(&fs::read(&heartbeat_file).unwrap()).unwrap();
        assert_eq!(value["status"], "no-work");
        assert_eq!(value["iteration"], 2);
        assert_eq!(value["detail"], "second");
        assert_eq!(value["serviceId"], "runtime-loop");
        assert_eq!(value["serviceKind"], "runtime");
        assert_eq!(
            value["processId"].as_u64(),
            Some(u64::from(std::process::id()))
        );
        assert!(
            value["generationId"]
                .as_str()
                .is_some_and(|value| value.starts_with("runtime-loop-"))
        );

        let service_file = harness_home
            .join("state")
            .join("supervisor")
            .join("services")
            .join("runtime-loop.json");
        let service: serde_json::Value =
            serde_json::from_slice(&fs::read(&service_file).unwrap()).unwrap();
        assert_eq!(
            service["schema"],
            "agent-harness.supervisor-service-state.v1"
        );
        assert_eq!(service["serviceId"], "runtime-loop");
        assert_eq!(service["serviceKind"], "runtime");
        assert_eq!(service["pid"].as_u64(), Some(u64::from(std::process::id())));
        assert_eq!(
            service["processId"].as_u64(),
            Some(u64::from(std::process::id()))
        );
        assert_eq!(service["generationId"], value["generationId"]);
        assert_eq!(service["lastHeartbeatAtMs"], value["atMs"]);
        assert_eq!(service["lastSuccessfulIterationAtMs"], value["atMs"]);
        assert_eq!(service["iteration"], 2);
        assert_eq!(service["actualState"], "no-work");
        assert_eq!(service["desiredState"], "running");
        assert_eq!(service["observedOnly"], expected_observed_only);
        assert_eq!(
            service["heartbeatFile"],
            heartbeat_file.display().to_string()
        );
        let leftovers = fs::read_dir(&heartbeat_dir)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.path() != heartbeat_file)
            .collect::<Vec<_>>();
        assert!(
            leftovers.is_empty(),
            "unexpected heartbeat temp files: {:?}",
            leftovers
                .iter()
                .map(|entry| entry.path())
                .collect::<Vec<_>>()
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn consume_gateway_restart_request_moves_file_and_receipts() {
        let root = cli_temp_root("consume_gateway_restart_request_moves_file_and_receipts");
        let harness_home = root.join(".agent-harness");
        let request_dir = harness_home
            .join("state")
            .join("supervisor")
            .join("gateway-restart-requests");
        fs::create_dir_all(&request_dir).unwrap();
        let request_file = request_dir.join("1000-operator.json");
        fs::write(
            &request_file,
            serde_json::to_string(&serde_json::json!({
                "schema": "agent-harness.gateway-restart-request.v1",
                "status": "requested",
                "target": "gateway",
                "reason": "operator requested restart"
            }))
            .unwrap(),
        )
        .unwrap();

        let consumed = consume_gateway_restart_request(&harness_home, "discord-gateway-loop")
            .unwrap()
            .expect("expected pending request");

        assert_eq!(consumed.request_file, request_file);
        assert_eq!(consumed.detail, "operator requested restart");
        assert!(!request_file.exists());
        let consumed_file = request_dir
            .join("consumed")
            .join("1000-operator.consumed.json");
        let value: serde_json::Value =
            serde_json::from_slice(&fs::read(&consumed_file).unwrap()).unwrap();
        assert_eq!(value["status"], "consumed");
        assert_eq!(value["consumedBy"], "discord-gateway-loop");
        assert!(value["consumedAtMs"].as_i64().is_some());
        let receipt_file = harness_home
            .join("state")
            .join("supervisor")
            .join("gateway-restart-requests.jsonl");
        let receipt_text = fs::read_to_string(receipt_file).unwrap();
        assert!(receipt_text.contains("\"status\":\"consumed\""));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn supervisor_run_progress_child_args_wrap_progress_delivery_loop() {
        let root = cli_temp_root("supervisor_run_progress_child_args");
        let harness_home = root.join(".agent-harness");
        let stop_file = harness_home
            .join("state")
            .join("supervisor")
            .join("stop")
            .join("progress-delivery-loop.stop");
        let harness_cli = root.join("agent-harness.exe");
        let args = supervisor_run_args_from_args(&[
            "--harness-home".to_string(),
            harness_home.display().to_string(),
            "--service".to_string(),
            "progress-delivery-loop".to_string(),
            "--harness-cli".to_string(),
            harness_cli.display().to_string(),
            "--idle-ms".to_string(),
            "25".to_string(),
            "--max-consecutive-errors".to_string(),
            "3".to_string(),
            "--restart-delay-ms".to_string(),
            "50".to_string(),
            "--max-restarts".to_string(),
            "2".to_string(),
            "--child-iterations".to_string(),
            "7".to_string(),
            "--stop-file".to_string(),
            stop_file.display().to_string(),
        ])
        .unwrap();

        assert_eq!(args.target_home, harness_home);
        assert_eq!(args.service, "progress-delivery-loop");
        assert_eq!(args.harness_cli, harness_cli);
        assert_eq!(args.idle_ms, 25);
        assert_eq!(args.max_consecutive_errors, 3);
        assert_eq!(args.restart_delay_ms, 50);
        assert_eq!(args.max_restarts, 2);
        assert_eq!(args.child_iterations, 7);
        assert_eq!(args.outbox_limit, 20);
        assert_eq!(args.discord_account, None);
        assert_eq!(args.stop_file.as_deref(), Some(stop_file.as_path()));
        assert_eq!(supervisor_service_priority(&args.service), "telemetry");
        assert_eq!(
            supervisor_delivery_lane(&args.service),
            Some("progress-delivery")
        );

        let child_args = supervisor_child_args(&args);
        let has_pair = |flag: &str, value: String| {
            child_args
                .windows(2)
                .any(|pair| pair[0] == flag && pair[1] == value)
        };
        assert_eq!(child_args[0], "progress-delivery-loop");
        assert!(has_pair(
            "--harness-home",
            harness_home.display().to_string()
        ));
        assert!(has_pair("--iterations", "7".to_string()));
        assert!(has_pair("--idle-ms", "25".to_string()));
        assert!(has_pair("--max-consecutive-errors", "3".to_string()));
        assert!(has_pair("--stop-file", stop_file.display().to_string()));

        let runtime_args =
            supervisor_run_args_from_args(&["--service".to_string(), "runtime-loop".to_string()])
                .unwrap();
        assert_eq!(runtime_args.service, "runtime-loop");
        let runtime_child_args = supervisor_child_args(&runtime_args);
        assert_eq!(runtime_child_args[0], "runtime-loop");
        assert!(
            runtime_child_args
                .windows(2)
                .any(|pair| pair[0] == "--loop-name" && pair[1] == "runtime-loop")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn supervisor_run_discord_outbox_child_args_wrap_final_delivery_loop() {
        let root = cli_temp_root("supervisor_run_discord_outbox_child_args");
        let harness_home = root.join(".agent-harness");
        let stop_file = harness_home
            .join("state")
            .join("supervisor")
            .join("stop")
            .join("discord-outbox-loop.stop");
        let harness_cli = root.join("agent-harness.exe");
        let args = supervisor_run_args_from_args(&[
            "--harness-home".to_string(),
            harness_home.display().to_string(),
            "--service".to_string(),
            "discord-outbox-loop".to_string(),
            "--harness-cli".to_string(),
            harness_cli.display().to_string(),
            "--idle-ms".to_string(),
            "30".to_string(),
            "--max-consecutive-errors".to_string(),
            "4".to_string(),
            "--restart-delay-ms".to_string(),
            "150".to_string(),
            "--max-restarts".to_string(),
            "3".to_string(),
            "--child-iterations".to_string(),
            "9".to_string(),
            "--outbox-limit".to_string(),
            "11".to_string(),
            "--discord-account".to_string(),
            "ops".to_string(),
            "--stop-file".to_string(),
            stop_file.display().to_string(),
        ])
        .unwrap();

        assert_eq!(args.target_home, harness_home);
        assert_eq!(args.service, "discord-outbox-loop");
        assert_eq!(args.harness_cli, harness_cli);
        assert_eq!(args.idle_ms, 30);
        assert_eq!(args.max_consecutive_errors, 4);
        assert_eq!(args.restart_delay_ms, 150);
        assert_eq!(args.max_restarts, 3);
        assert_eq!(args.child_iterations, 9);
        assert_eq!(args.outbox_limit, 11);
        assert_eq!(args.discord_account.as_deref(), Some("ops"));
        assert_eq!(args.stop_file.as_deref(), Some(stop_file.as_path()));
        assert_eq!(supervisor_service_priority(&args.service), "final-delivery");
        assert_eq!(
            supervisor_delivery_lane(&args.service),
            Some("final-outbox")
        );

        let child_args = supervisor_child_args(&args);
        let has_pair = |flag: &str, value: String| {
            child_args
                .windows(2)
                .any(|pair| pair[0] == flag && pair[1] == value)
        };
        assert_eq!(child_args[0], "discord-outbox-loop");
        assert!(has_pair(
            "--harness-home",
            harness_home.display().to_string()
        ));
        assert!(has_pair("--iterations", "9".to_string()));
        assert!(has_pair("--idle-ms", "30".to_string()));
        assert!(has_pair("--max-consecutive-errors", "4".to_string()));
        assert!(has_pair("--outbox-limit", "11".to_string()));
        assert!(has_pair("--discord-account", "ops".to_string()));
        assert!(has_pair("--stop-file", stop_file.display().to_string()));

        let zero_outbox_limit = supervisor_run_args_from_args(&[
            "--service".to_string(),
            "discord-outbox-loop".to_string(),
            "--outbox-limit".to_string(),
            "0".to_string(),
        ])
        .unwrap_err();
        assert!(zero_outbox_limit.contains("--outbox-limit must be greater than zero"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn skill_apply_cli_proposes_and_applies_replacement() {
        let root = cli_temp_root("skill_apply_cli_proposes_and_applies_replacement");
        let harness_home = root.join(".agent-harness");
        let skill_file = root.join("skills").join("triage").join("SKILL.md");
        fs::create_dir_all(skill_file.parent().unwrap()).unwrap();
        fs::write(&skill_file, "# Triage\n\nOld body.\n").unwrap();

        run_skill_propose(&[
            "--harness-home".to_string(),
            harness_home.display().to_string(),
            "--skill".to_string(),
            "workspace:triage".to_string(),
            "--target-path".to_string(),
            skill_file.display().to_string(),
            "--operation".to_string(),
            "replace".to_string(),
            "--body".to_string(),
            "# Triage\n\nNew body.\n".to_string(),
        ])
        .unwrap();
        let proposals_text = fs::read_to_string(
            harness_home
                .join("state")
                .join("learning")
                .join("skill-proposals.jsonl"),
        )
        .unwrap();
        let proposal: serde_json::Value =
            serde_json::from_str(proposals_text.lines().next().unwrap()).unwrap();
        let proposal_id = proposal
            .get("proposalId")
            .and_then(serde_json::Value::as_str)
            .unwrap()
            .to_string();

        run_skill_apply(&[
            "--harness-home".to_string(),
            harness_home.display().to_string(),
            "--proposal".to_string(),
            proposal_id,
        ])
        .unwrap();

        assert!(
            fs::read_to_string(&skill_file)
                .unwrap()
                .contains("New body")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn cron_scheduler_sleep_respects_config_interval_floor() {
        assert_eq!(cron_scheduler_loop_sleep_ms(Some(1_000), 60_000), 60_000);
        assert_eq!(cron_scheduler_loop_sleep_ms(Some(120_000), 60_000), 120_000);
        assert_eq!(cron_scheduler_loop_sleep_ms(None, 5_000), 10_000);
    }

    #[test]
    fn supervisor_plan_defaults_source_home_to_target_home() {
        let args = supervisor_plan_args_from_args(&[
            "--harness-home".to_string(),
            "live-home".to_string(),
        ])
        .unwrap();

        assert_eq!(args.target_home, PathBuf::from("live-home"));
        assert_eq!(args.source_home, PathBuf::from("live-home"));

        let args = supervisor_plan_args_from_args(&[
            "--source-home".to_string(),
            "legacy-home".to_string(),
            "--harness-home".to_string(),
            "live-home".to_string(),
        ])
        .unwrap();

        assert_eq!(args.target_home, PathBuf::from("live-home"));
        assert_eq!(args.source_home, PathBuf::from("legacy-home"));
    }

    #[test]
    fn runtime_loop_lease_busy_does_not_count_as_error() {
        let root = cli_temp_root("runtime_loop_lease_busy_does_not_count_as_error");
        fs::create_dir_all(
            root.join("state")
                .join("supervisor")
                .join("loop-heartbeats"),
        )
        .unwrap();
        let args = RuntimeLoopArgs {
            target_home: root.clone(),
            loop_name: "runtime-loop".to_string(),
            codex_exe: None,
            timeout_ms: 1_000,
            idle_timeout_ms: 1_000,
            max_prompt_file_bytes: PromptAssemblyOptions::default().max_prompt_file_bytes,
            max_skill_file_bytes: PromptAssemblyOptions::default().max_skill_file_bytes,
            runtime_concurrency: 12,
            iterations: 0,
            idle_ms: 1,
            max_consecutive_errors: 5,
            safe_mode_restart_ms: Some(60_000),
            stop_when_idle: false,
            stop_file: None,
        };
        let queue_id = "queue:lease-busy".to_string();
        let task = RuntimeLoopTaskResult {
            queue_id: queue_id.clone(),
            result: Ok(RuntimeRunOnceReport {
                schema: "agent-harness.runtime-run-once.v1",
                harness_home: root.clone(),
                report_file: root
                    .join("state")
                    .join("runtime-queue")
                    .join("run-once.json"),
                receipts_file: root
                    .join("state")
                    .join("runtime-queue")
                    .join("run-once-receipts.jsonl"),
                receipt: agent_harness_core::RuntimeRunOnceReceipt {
                    queue_id: Some(queue_id.clone()),
                    status: RuntimeRunOnceStatus::LeaseBusy,
                    runtime_class: Some("interactive".to_string()),
                    origin: Some("channel".to_string()),
                    cron_run_id: None,
                    scheduled_for_ms: None,
                    execution_dir: None,
                    transcript_file: None,
                    outbox_file: None,
                    continuation: agent_harness_core::RuntimeContinuationMetadata::legacy(),
                    reason: "runtime queue lease lock is busy; retrying later".to_string(),
                },
                prepare: None,
                plan: None,
                run: None,
                outbox_file: None,
                outbound_message: None,
                warnings: Vec::new(),
            }),
        };
        let mut completed = 0;
        let mut idle = 0;
        let mut errors = 0;
        let mut consecutive_errors = 0;
        let mut last_status = None;
        let mut last_queue_id = None;
        let mut last_reason = None;

        let should_enter_safe_mode = handle_runtime_loop_task_result(
            &args,
            1,
            task,
            &mut completed,
            &mut idle,
            &mut errors,
            &mut consecutive_errors,
            &mut last_status,
            &mut last_queue_id,
            &mut last_reason,
        )
        .unwrap();

        assert!(!should_enter_safe_mode);
        assert_eq!(completed, 0);
        assert_eq!(idle, 0);
        assert_eq!(errors, 0);
        assert_eq!(consecutive_errors, 0);
        assert_eq!(last_status, Some(RuntimeRunOnceStatus::LeaseBusy));
        assert_eq!(last_queue_id.as_deref(), Some(queue_id.as_str()));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn discord_gateway_args_preserve_account_selector() {
        let args = discord_gateway_args_from_args(&[
            "--discord-account".to_string(),
            "ops".to_string(),
            "--max-messages".to_string(),
            "1".to_string(),
        ])
        .unwrap();

        assert_eq!(args.discord_account.as_deref(), Some("ops"));
        assert_eq!(args.max_messages, 1);
    }

    #[test]
    fn discord_gateway_defaults_source_home_to_target_home() {
        let args = discord_gateway_args_from_args(&[
            "--harness-home".to_string(),
            "live-home".to_string(),
        ])
        .unwrap();

        assert_eq!(args.target_home, PathBuf::from("live-home"));
        assert_eq!(args.source.home, PathBuf::from("live-home"));

        let args = discord_gateway_args_from_args(&[
            "--source-home".to_string(),
            "legacy-home".to_string(),
            "--harness-home".to_string(),
            "live-home".to_string(),
        ])
        .unwrap();

        assert_eq!(args.target_home, PathBuf::from("live-home"));
        assert_eq!(args.source.home, PathBuf::from("legacy-home"));
    }

    #[test]
    fn discord_event_defaults_source_home_to_target_home() {
        let args = discord_event_run_once_args_from_args(&[
            "--harness-home".to_string(),
            "live-home".to_string(),
            "--event-json".to_string(),
            "{}".to_string(),
        ])
        .unwrap();

        assert_eq!(args.target_home, PathBuf::from("live-home"));
        assert_eq!(args.source.home, PathBuf::from("live-home"));

        let args = discord_event_run_once_args_from_args(&[
            "--source-home".to_string(),
            "legacy-home".to_string(),
            "--harness-home".to_string(),
            "live-home".to_string(),
            "--event-json".to_string(),
            "{}".to_string(),
        ])
        .unwrap();

        assert_eq!(args.target_home, PathBuf::from("live-home"));
        assert_eq!(args.source.home, PathBuf::from("legacy-home"));
    }

    fn discord_message(
        guild_id: Option<&str>,
        channel_id: &str,
        user_id: &str,
    ) -> DiscordGatewayMessage {
        DiscordGatewayMessage {
            message_id: "message-1".to_string(),
            guild_id: guild_id.map(ToString::to_string),
            channel_id: channel_id.to_string(),
            user_id: user_id.to_string(),
            content: "hello".to_string(),
            inbound_context: None,
            inbound_media_artifacts: Vec::new(),
            attachments: Vec::new(),
            reply_context: None,
            author_is_bot: false,
        }
    }

    #[test]
    fn parses_channel_id_sets_from_imported_env_format() {
        let ids = parse_channel_id_set(Some(" 111,222; 333\n'444' \"555\" "));

        assert_eq!(ids, set(&["111", "222", "333", "444", "555"]));
    }

    #[test]
    fn formats_channel_reply_without_plain_header() {
        assert_eq!(format_channel_reply_text("  done\n"), "done");
        assert_eq!(
            format_channel_reply_text("⏳ Working — <1 min — running tools"),
            "⏳ Working — <1 min — running tools"
        );
    }

    #[test]
    fn telegram_direct_policy_requires_imported_chat_and_user_ids() {
        let policy = ChannelAccessPolicy {
            telegram_allowed_user_ids: set(&["user-1"]),
            telegram_direct_chat_ids: set(&["chat-1"]),
            ..ChannelAccessPolicy::default()
        };

        assert_eq!(
            telegram_access_decision(&policy, "chat-1", "user-1", Some("private")),
            ChannelAccessDecision::Allowed(ChannelPermission::Admin)
        );
        assert!(matches!(
            telegram_access_decision(&policy, "chat-2", "user-1", Some("private")),
            ChannelAccessDecision::Denied(_)
        ));
        assert!(matches!(
            telegram_access_decision(&policy, "chat-1", "user-2", Some("private")),
            ChannelAccessDecision::Denied(_)
        ));
    }

    #[test]
    fn telegram_group_policy_uses_group_allow_lists() {
        let policy = ChannelAccessPolicy {
            telegram_group_allowed_user_ids: set(&["user-1"]),
            telegram_group_chat_ids: set(&["group-1"]),
            ..ChannelAccessPolicy::default()
        };

        assert_eq!(
            telegram_access_decision(&policy, "group-1", "user-1", Some("supergroup")),
            ChannelAccessDecision::Allowed(ChannelPermission::Limited)
        );
        assert!(matches!(
            telegram_access_decision(&policy, "group-2", "user-1", Some("supergroup")),
            ChannelAccessDecision::Denied(_)
        ));
        assert!(matches!(
            telegram_access_decision(&policy, "group-1", "user-2", Some("group")),
            ChannelAccessDecision::Denied(_)
        ));
    }

    #[test]
    fn progress_delivery_uses_same_telegram_group_admin_acl_as_ingress() {
        let policy = ChannelAccessPolicy {
            telegram_group_admin_user_ids: set(&["admin-1"]),
            telegram_group_chat_ids: set(&["group-1"]),
            ..ChannelAccessPolicy::default()
        };
        let args = progress_args();

        assert!(
            progress_delivery_allowed(
                &policy,
                &progress_pending("telegram", "group-1", "admin-1"),
                &args
            )
            .is_ok()
        );
        assert!(
            progress_delivery_allowed(
                &policy,
                &progress_pending("telegram", "group-1", "user-2"),
                &args
            )
            .is_err()
        );
    }

    #[test]
    fn telegram_direct_policy_fails_closed_without_admin_user() {
        let policy = ChannelAccessPolicy {
            telegram_direct_chat_ids: set(&["chat-1"]),
            ..ChannelAccessPolicy::default()
        };

        assert!(matches!(
            telegram_access_decision(&policy, "chat-1", "user-1", Some("private")),
            ChannelAccessDecision::Denied(_)
        ));
    }

    #[test]
    fn telegram_group_open_grants_limited_permission_only() {
        let policy = ChannelAccessPolicy {
            telegram_group_chat_ids: set(&["group-1"]),
            telegram_group_open: true,
            ..ChannelAccessPolicy::default()
        };

        assert_eq!(
            telegram_access_decision(&policy, "group-1", "user-2", Some("supergroup")),
            ChannelAccessDecision::Allowed(ChannelPermission::Limited)
        );
        assert!(channel_permission_allows_text(ChannelPermission::Limited, "/status").is_ok());
        assert!(
            channel_permission_allows_text(ChannelPermission::Limited, "/model openai/gpt-5")
                .is_err()
        );
    }

    #[test]
    fn telegram_inbound_context_extracts_reply_and_media_without_file_ids() {
        let message = serde_json::json!({
            "message_id": 10,
            "message_thread_id": 3,
            "caption": "see attached",
            "reply_to_message": {
                "message_id": 9,
                "date": 1710000000,
                "text": "previous message",
                "from": { "id": 12345 }
            },
            "photo": [
                {
                    "file_id": "raw-photo-file-id",
                    "width": 320,
                    "height": 240,
                    "file_size": 100
                }
            ],
            "document": {
                "file_id": "raw-doc-file-id",
                "file_name": "notes.txt",
                "mime_type": "text/plain",
                "file_size": 20
            }
        });

        assert_eq!(
            telegram_message_text(&message, true),
            Some("see attached".to_string())
        );
        let context = telegram_inbound_context(&message).unwrap();
        assert!(context.contains("messageThreadId: 3"));
        assert!(context.contains("ReferencedMessage"));
        assert!(context.contains("messageId: 9"));
        assert!(context.contains("textPreview: previous message"));
        assert!(context.contains("textLength: 16"));
        assert!(context.contains("textTruncated: false"));
        assert!(context.contains("textMaxChars: 4000"));
        assert!(context.contains("textSource: telegram.reply_to_message"));
        assert!(context.contains("\n  previous message"));
        assert!(context.contains("kind=photo"));
        assert!(context.contains("kind=document"));
        assert!(context.contains("fileIdPresent=yes"));
        assert!(!context.contains("raw-photo-file-id"));
        assert!(!context.contains("raw-doc-file-id"));
    }

    #[test]
    fn telegram_media_only_message_uses_placeholder_and_redacted_context() {
        let message = serde_json::json!({
            "message_id": 12,
            "photo": [
                {
                    "file_id": "raw-media-only-file-id",
                    "width": 640,
                    "height": 480,
                    "file_size": 200
                }
            ]
        });

        assert_eq!(
            telegram_message_text(&message, false),
            Some("[telegram media message]".to_string())
        );
        let context = telegram_inbound_context(&message).unwrap();
        assert!(context.contains("kind=photo"));
        assert!(context.contains("fileIdPresent=yes"));
        assert!(!context.contains("raw-media-only-file-id"));
    }

    #[test]
    fn telegram_media_group_context_uses_first_caption_and_redacts_file_ids() {
        let flush = telegram_media::TelegramMediaGroupFlush {
            account_id: "default".to_string(),
            chat_id: "-1001".to_string(),
            media_group_id: "album-1".to_string(),
            status: telegram_media::TelegramMediaGroupStatus::GroupFlushed,
            members: vec![
                telegram_media::TelegramMediaGroupMember {
                    update_id: 101,
                    message_id: Some("11".to_string()),
                    caption_preview: Some("first caption".to_string()),
                    message: serde_json::json!({
                        "message_id": 11,
                        "media_group_id": "album-1",
                        "caption": "first caption",
                        "chat": { "id": -1001, "type": "supergroup" },
                        "from": { "id": 123 },
                        "photo": [{"file_id":"raw-first-file-id","width":320,"height":240,"file_size":100}]
                    }),
                },
                telegram_media::TelegramMediaGroupMember {
                    update_id: 102,
                    message_id: Some("12".to_string()),
                    caption_preview: Some("second caption".to_string()),
                    message: serde_json::json!({
                        "message_id": 12,
                        "media_group_id": "album-1",
                        "caption": "second caption",
                        "chat": { "id": -1001, "type": "supergroup" },
                        "from": { "id": 123 },
                        "photo": [{"file_id":"raw-second-file-id","width":640,"height":480,"file_size":200}]
                    }),
                },
            ],
        };

        assert_eq!(telegram_media_group_message_text(&flush), "first caption");
        let context = telegram_media_group_inbound_context(&flush).unwrap();
        assert!(context.contains("mediaGroupId: album-1"));
        assert!(context.contains("memberCount: 2"));
        assert!(context.contains("messageId=11"));
        assert!(context.contains("captionPreview=first caption"));
        assert!(context.contains("kind=photo"));
        assert!(context.contains("fileIdPresent=yes"));
        assert!(!context.contains("raw-first-file-id"));
        assert!(!context.contains("raw-second-file-id"));
    }

    #[test]
    fn telegram_inbound_context_keeps_topic_without_reply_or_media() {
        let message = serde_json::json!({
            "message_id": 10,
            "message_thread_id": 7,
            "text": "topic hello"
        });

        let context = telegram_inbound_context(&message).unwrap();

        assert!(context.contains("InboundMessage"));
        assert!(context.contains("messageThreadId: 7"));
    }

    #[test]
    fn telegram_html_payload_escapes_dynamic_text_and_sets_thread() {
        let payload = telegram_message_payload(
            "-1001",
            "# Status\n\nDetails: `<ok>`\n```rust\nlet x = \"<&>\";\n```",
            TelegramSendOptions {
                reply_to_message_id: Some(12),
                message_thread_id: Some("3"),
                formatting_mode: TelegramFormattingMode::Html,
            },
        );

        assert_eq!(payload["parse_mode"], "HTML");
        assert_eq!(payload["message_thread_id"], 3);
        assert_eq!(payload["reply_to_message_id"], 12);
        assert_eq!(payload["link_preview_options"]["is_disabled"], true);
        let text = payload["text"].as_str().unwrap();
        assert!(text.contains("<b>Status</b>"));
        assert!(text.contains("<code>&lt;ok&gt;</code>"));
        assert!(text.contains("<pre>let x = &quot;&lt;&amp;&gt;&quot;;</pre>"));
        assert!(!text.contains("<ok>"));
    }

    #[test]
    fn discord_policy_checks_guild_channel_and_user_but_allows_dms_by_user() {
        let policy = ChannelAccessPolicy {
            discord_allowed_user_ids: set(&["user-1"]),
            discord_channel_ids: set(&["channel-1"]),
            discord_guild_ids: set(&["guild-1"]),
            ..ChannelAccessPolicy::default()
        };

        assert_eq!(
            discord_access_decision(
                &policy,
                &discord_message(Some("guild-1"), "channel-1", "user-1")
            ),
            ChannelAccessDecision::Allowed(ChannelPermission::Admin)
        );
        assert_eq!(
            discord_access_decision(&policy, &discord_message(None, "dm-channel", "user-1")),
            ChannelAccessDecision::Allowed(ChannelPermission::Admin)
        );
        assert!(matches!(
            discord_access_decision(
                &policy,
                &discord_message(Some("guild-2"), "channel-1", "user-1")
            ),
            ChannelAccessDecision::Denied(_)
        ));
        assert!(matches!(
            discord_access_decision(
                &policy,
                &discord_message(Some("guild-1"), "channel-2", "user-1")
            ),
            ChannelAccessDecision::Denied(_)
        ));
        assert!(matches!(
            discord_access_decision(&policy, &discord_message(None, "dm-channel", "user-2")),
            ChannelAccessDecision::Denied(_)
        ));
    }

    #[test]
    fn discord_group_open_grants_limited_permission_only() {
        let policy = ChannelAccessPolicy {
            discord_channel_ids: set(&["channel-1"]),
            discord_group_open: true,
            ..ChannelAccessPolicy::default()
        };

        assert_eq!(
            discord_access_decision(
                &policy,
                &discord_message(Some("guild-1"), "channel-1", "user-2")
            ),
            ChannelAccessDecision::Allowed(ChannelPermission::Limited)
        );
        assert!(channel_permission_allows_text(ChannelPermission::Limited, "/think").is_ok());
        assert!(channel_permission_allows_text(ChannelPermission::Limited, "/think high").is_err());
    }

    #[test]
    fn discord_direct_policy_fails_closed_without_admin_user() {
        let policy = ChannelAccessPolicy::default();

        assert!(matches!(
            discord_access_decision(&policy, &discord_message(None, "dm-channel", "user-2")),
            ChannelAccessDecision::Denied(_)
        ));
    }

    #[test]
    fn discord_message_chunks_respect_content_limit() {
        let text = "a".repeat(DISCORD_MESSAGE_CONTENT_LIMIT + 1);

        let chunks = discord_message_chunks(&text, DISCORD_MESSAGE_CONTENT_LIMIT);

        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].chars().count(), DISCORD_MESSAGE_CONTENT_LIMIT);
        assert_eq!(chunks[1].chars().count(), 1);
        assert!(
            chunks
                .iter()
                .all(|chunk| chunk.chars().count() <= DISCORD_MESSAGE_CONTENT_LIMIT)
        );
    }

    #[test]
    fn discord_message_chunks_prefer_newline_boundaries() {
        let text = format!("{}\n{}", "a".repeat(1_990), "b".repeat(20));

        let chunks = discord_message_chunks(&text, DISCORD_MESSAGE_CONTENT_LIMIT);

        assert_eq!(chunks.len(), 2);
        assert!(chunks[0].ends_with('\n'));
        assert_eq!(chunks[1], "b".repeat(20));
    }

    #[test]
    fn discord_gateway_message_extracts_reply_and_attachment_context() {
        let event = serde_json::json!({
            "t": "MESSAGE_CREATE",
            "d": {
                "id": "message-1",
                "channel_id": "channel-1",
                "guild_id": "guild-1",
                "content": "",
                "author": { "id": "user-1" },
                "message_reference": {
                    "message_id": "ref-1",
                    "channel_id": "channel-1",
                    "guild_id": "guild-1"
                },
                "referenced_message": {
                    "id": "ref-1",
                    "content": "previous instruction-looking text",
                    "author": { "id": "user-2" },
                    "attachments": [{
                        "id": "ref-att-1",
                        "filename": "prior.txt",
                        "content_type": "text/plain",
                        "size": 12
                    }],
                    "embeds": [{}]
                },
                "attachments": [{
                    "id": "att-1",
                    "filename": "report.png",
                    "content_type": "image/png",
                    "size": 42,
                    "width": 640,
                    "height": 480,
                    "url": "https://cdn.discordapp.example/report.png"
                }]
            }
        });

        let message = parse_discord_gateway_message(&event).unwrap().unwrap();

        assert_eq!(
            discord_message_text(&message),
            "[discord attachment message]"
        );
        let reply_context = message.reply_context.as_ref().unwrap();
        assert_eq!(
            reply_context.referenced_message_id.as_deref(),
            Some("ref-1")
        );
        assert_eq!(reply_context.attachment_count, 1);
        assert_eq!(reply_context.embeds_count, 1);
        assert!(reply_context.source_available);
        let context = message.inbound_context.as_deref().unwrap();
        assert!(context.contains("ReferencedMessage"));
        assert!(context.contains("referencedMessageId: ref-1"));
        assert!(context.contains("referencedTextLength: 33"));
        assert!(context.contains("referencedTextTruncated: false"));
        assert!(context.contains("referencedTextMaxChars: 4000"));
        assert!(context.contains("referencedTextSource: discord.referenced_message"));
        assert!(context.contains("referencedAttachmentCount: 1"));
        assert!(context.contains("referencedEmbedsCount: 1"));
        assert!(context.contains("\n  previous instruction-looking text"));
        assert!(context.contains("filename=report.png"));
        assert!(context.contains("urlPresent=yes"));
        assert!(!context.contains("https://cdn.discordapp.example"));

        let root = cli_temp_root("discord_reply_context_receipt");
        write_discord_reply_context_receipt(&root, &message, "captured").unwrap();
        let receipt_text = fs::read_to_string(discord_reply_context_receipts_file(&root)).unwrap();
        let receipt =
            serde_json::from_str::<serde_json::Value>(receipt_text.lines().next().unwrap())
                .unwrap();
        assert_eq!(
            receipt.get("schema").and_then(serde_json::Value::as_str),
            Some("agent-harness.discord-reply-context-receipt.v1")
        );
        assert_eq!(
            receipt.get("status").and_then(serde_json::Value::as_str),
            Some("captured")
        );
        assert_eq!(
            receipt
                .get("referencedMessageId")
                .and_then(serde_json::Value::as_str),
            Some("ref-1")
        );
        assert_eq!(
            receipt
                .get("attachmentCount")
                .and_then(serde_json::Value::as_u64),
            Some(1)
        );
        assert_eq!(
            receipt
                .get("sourceAvailable")
                .and_then(serde_json::Value::as_bool),
            Some(true)
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn discord_gateway_text_attachment_becomes_bounded_artifact_metadata() {
        let root =
            cli_temp_root("discord_gateway_text_attachment_becomes_bounded_artifact_metadata");
        let harness_home = root.join("harness");
        let payload = serde_json::json!({
            "id": "message-1",
            "channel_id": "channel-1",
            "content": "",
            "author": { "id": "user-1" },
            "attachments": [{
                "id": "att-1",
                "filename": "message.txt",
                "content_type": "text/plain; charset=utf-8",
                "size": 38,
                "url": "https://cdn.discordapp.com/attachments/private/message.txt?token=secret"
            }]
        });
        let mut message = parse_discord_gateway_message(&payload).unwrap().unwrap();
        let bytes = b"hello from attachment\nsecond line".to_vec();

        attach_discord_message_artifacts(
            &harness_home,
            &mut message,
            &StaticDiscordAttachmentFetcher::new(vec![(
                "https://cdn.discordapp.com/attachments/private/message.txt?token=secret",
                bytes.clone(),
            )]),
        )
        .unwrap();

        assert_eq!(message.inbound_media_artifacts.len(), 1);
        let artifact = &message.inbound_media_artifacts[0];
        assert_eq!(artifact.platform, "discord");
        assert_eq!(artifact.kind, "attachment-text");
        assert_eq!(artifact.message_id.as_deref(), Some("message-1"));
        assert_eq!(artifact.mime.as_deref(), Some("text/plain"));
        assert_eq!(artifact.byte_len, Some(bytes.len() as u64));
        assert_eq!(
            artifact.download_status,
            InboundMediaDownloadStatus::Downloaded
        );
        assert_eq!(
            artifact.model_attachment_status,
            InboundMediaModelAttachmentStatus::PromptOnly
        );
        assert_eq!(
            artifact
                .extraction_summary
                .as_ref()
                .and_then(|summary| summary.summary.as_deref()),
            Some("hello from attachment second line")
        );
        let local_path = artifact.local_path.as_ref().unwrap();
        assert!(local_path.is_file());
        assert_eq!(fs::read(local_path).unwrap(), bytes);
        let rendered = agent_harness_core::render_inbound_media_artifacts_for_prompt(
            &message.inbound_media_artifacts,
            Some(&harness_home),
        );
        assert!(
            rendered.contains("artifactUri=agent-harness://inbound-media/discord/message-1/0.txt")
        );
        assert!(rendered.contains("extractionSummary=hello from attachment second line"));
        assert!(!rendered.contains("cdn.discordapp.com"));
        assert!(!rendered.contains("token=secret"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn discord_gateway_rejects_non_discord_attachment_url_without_fetching() {
        let root = cli_temp_root("discord_gateway_rejects_non_discord_attachment_url");
        let harness_home = root.join("harness");
        let payload = serde_json::json!({
            "id": "message-untrusted-url",
            "channel_id": "channel-1",
            "content": "",
            "author": { "id": "user-1" },
            "attachments": [{
                "id": "att-untrusted-url",
                "filename": "message.txt",
                "content_type": "text/plain",
                "size": 15,
                "url": "https://example.com/attachments/private/message.txt?token=secret"
            }]
        });
        let mut message = parse_discord_gateway_message(&payload).unwrap().unwrap();

        attach_discord_message_artifacts(
            &harness_home,
            &mut message,
            &StaticDiscordAttachmentFetcher::new(vec![(
                "https://example.com/attachments/private/message.txt?token=secret",
                b"should not be fetched".to_vec(),
            )]),
        )
        .unwrap();

        let artifact = &message.inbound_media_artifacts[0];
        assert_eq!(
            artifact.download_status,
            InboundMediaDownloadStatus::DownloadFailed
        );
        assert!(artifact.local_path.is_none());
        assert!(artifact.artifact_uri.is_none());
        let warnings = artifact.warnings.join(" ");
        assert!(warnings.contains("outside the supported Discord media attachment hosts"));
        assert!(!warnings.contains("example.com"));
        assert!(!warnings.contains("token=secret"));
        let rendered = agent_harness_core::render_inbound_media_artifacts_for_prompt(
            &message.inbound_media_artifacts,
            Some(&harness_home),
        );
        assert!(rendered.contains("warningsCount=1"));
        assert!(!rendered.contains("example.com"));
        assert!(!rendered.contains("token=secret"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn discord_gateway_attachment_download_failure_redacts_provider_url() {
        let root =
            cli_temp_root("discord_gateway_attachment_download_failure_redacts_provider_url");
        let harness_home = root.join("harness");
        let payload = serde_json::json!({
            "id": "message-download-failed",
            "channel_id": "channel-1",
            "content": "",
            "author": { "id": "user-1" },
            "attachments": [{
                "id": "att-download-failed",
                "filename": "message.txt",
                "content_type": "text/plain",
                "size": 15,
                "url": "https://cdn.discordapp.com/attachments/private/message.txt?token=secret"
            }]
        });
        let mut message = parse_discord_gateway_message(&payload).unwrap().unwrap();

        attach_discord_message_artifacts(
            &harness_home,
            &mut message,
            &StaticDiscordAttachmentFetcher::new(vec![]),
        )
        .unwrap();

        let artifact = &message.inbound_media_artifacts[0];
        assert_eq!(
            artifact.download_status,
            InboundMediaDownloadStatus::DownloadFailed
        );
        assert!(artifact.local_path.is_none());
        assert!(artifact.artifact_uri.is_none());
        let warnings = artifact.warnings.join(" ");
        assert!(warnings.contains("download failed"));
        assert!(!warnings.contains("cdn.discordapp.com"));
        assert!(!warnings.contains("token=secret"));
        let rendered = agent_harness_core::render_inbound_media_artifacts_for_prompt(
            &message.inbound_media_artifacts,
            Some(&harness_home),
        );
        assert!(!rendered.contains("cdn.discordapp.com"));
        assert!(!rendered.contains("token=secret"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn discord_gateway_image_attachment_is_artifact_reference_without_payload() {
        let root =
            cli_temp_root("discord_gateway_image_attachment_is_artifact_reference_without_payload");
        let harness_home = root.join("harness");
        let payload = serde_json::json!({
            "id": "message-2",
            "channel_id": "channel-1",
            "content": "",
            "author": { "id": "user-1" },
            "attachments": [{
                "id": "att-2",
                "filename": "photo.png",
                "content_type": "image/png",
                "size": 24,
                "width": 2,
                "height": 3,
                "url": "https://cdn.discordapp.com/attachments/private/photo.png?token=secret"
            }]
        });
        let mut message = parse_discord_gateway_message(&payload).unwrap().unwrap();
        let bytes = vec![
            0x89, b'P', b'N', b'G', b'\r', b'\n', 0x1a, b'\n', 0, 0, 0, 0,
        ];

        attach_discord_message_artifacts(
            &harness_home,
            &mut message,
            &StaticDiscordAttachmentFetcher::new(vec![(
                "https://cdn.discordapp.com/attachments/private/photo.png?token=secret",
                bytes.clone(),
            )]),
        )
        .unwrap();

        assert_eq!(message.inbound_media_artifacts.len(), 1);
        let artifact = &message.inbound_media_artifacts[0];
        assert_eq!(artifact.kind, "attachment-image");
        assert_eq!(artifact.mime.as_deref(), Some("image/png"));
        assert_eq!(artifact.selected_variant.as_ref().unwrap().width, Some(2));
        assert_eq!(artifact.selected_variant.as_ref().unwrap().height, Some(3));
        assert!(artifact.extraction_summary.is_none());
        assert_eq!(
            fs::read(artifact.local_path.as_ref().unwrap()).unwrap(),
            bytes
        );
        let rendered = agent_harness_core::render_inbound_media_artifacts_for_prompt(
            &message.inbound_media_artifacts,
            Some(&harness_home),
        );
        assert!(
            rendered.contains("artifactUri=agent-harness://inbound-media/discord/message-2/0.png")
        );
        assert!(rendered.contains("mime=image/png"));
        assert!(!rendered.contains("cdn.discordapp.com"));
        assert!(!rendered.contains("token=secret"));
        assert!(!rendered.contains("data:image"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn discord_gateway_image_attachment_requires_supported_image_bytes() {
        let root = cli_temp_root("discord_gateway_image_attachment_requires_supported_image_bytes");
        let harness_home = root.join("harness");
        let payload = serde_json::json!({
            "id": "message-invalid-image",
            "channel_id": "channel-1",
            "content": "",
            "author": { "id": "user-1" },
            "attachments": [{
                "id": "att-invalid-image",
                "filename": "photo.png",
                "content_type": "image/png",
                "size": 9,
                "width": 2,
                "height": 3,
                "url": "https://cdn.discordapp.com/attachments/private/photo.png?token=secret"
            }]
        });
        let mut message = parse_discord_gateway_message(&payload).unwrap().unwrap();

        attach_discord_message_artifacts(
            &harness_home,
            &mut message,
            &StaticDiscordAttachmentFetcher::new(vec![(
                "https://cdn.discordapp.com/attachments/private/photo.png?token=secret",
                b"not-image".to_vec(),
            )]),
        )
        .unwrap();

        let artifact = &message.inbound_media_artifacts[0];
        assert_eq!(
            artifact.download_status,
            InboundMediaDownloadStatus::DownloadFailed
        );
        assert_eq!(artifact.mime.as_deref(), Some("image/png"));
        assert!(artifact.local_path.is_none());
        assert!(artifact.artifact_uri.is_none());
        assert!(
            artifact
                .warnings
                .iter()
                .any(|warning| warning.contains("not a supported image"))
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn discord_gateway_json_like_attachment_uses_json_artifact_extension() {
        let root =
            cli_temp_root("discord_gateway_json_like_attachment_uses_json_artifact_extension");
        let harness_home = root.join("harness");
        let payload = serde_json::json!({
            "id": "message-json-like",
            "channel_id": "channel-1",
            "content": "",
            "author": { "id": "user-1" },
            "attachments": [{
                "id": "att-json-like",
                "filename": "payload",
                "content_type": "application/activity+json",
                "size": 11,
                "url": "https://cdn.discordapp.com/attachments/private/payload?token=secret"
            }]
        });
        let mut message = parse_discord_gateway_message(&payload).unwrap().unwrap();

        attach_discord_message_artifacts(
            &harness_home,
            &mut message,
            &StaticDiscordAttachmentFetcher::new(vec![(
                "https://cdn.discordapp.com/attachments/private/payload?token=secret",
                br#"{"ok":true}"#.to_vec(),
            )]),
        )
        .unwrap();

        let artifact = &message.inbound_media_artifacts[0];
        assert_eq!(
            artifact.download_status,
            InboundMediaDownloadStatus::Downloaded
        );
        assert_eq!(artifact.kind, "attachment-text");
        assert_eq!(artifact.mime.as_deref(), Some("application/activity+json"));
        assert!(
            artifact
                .artifact_uri
                .as_deref()
                .is_some_and(|uri| uri.ends_with("/0.json"))
        );
        assert_eq!(
            artifact
                .local_path
                .as_deref()
                .and_then(Path::extension)
                .and_then(|extension| extension.to_str()),
            Some("json")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn jsonl_repair_recovers_concatenated_records() {
        let root = cli_temp_root("jsonl_repair_recovers_concatenated_records");
        let path = root.join("receipts.jsonl");
        let output = root.join("repaired.jsonl");
        let invalid_output = root.join("invalid.jsonl");
        fs::create_dir_all(&root).unwrap();
        fs::write(
            &path,
            concat!(
                "{\"queueId\":\"q1\",\"status\":\"ok\"}\n",
                "{\"queueId\":\"q2\",\"status\":\"ok\"}{\"queueId\":\"q3\",\"status\":\"ok\"}\n",
                "{not-json}\n"
            ),
        )
        .unwrap();

        let report = repair_jsonl_file(&JsonlRepairArgs {
            path: path.clone(),
            output: Some(output.clone()),
            invalid_output: Some(invalid_output.clone()),
            apply: false,
        })
        .unwrap();

        assert_eq!(report.total_lines, 3);
        assert_eq!(report.valid_lines, 1);
        assert_eq!(report.recovered_lines, 1);
        assert_eq!(report.recovered_values, 2);
        assert_eq!(report.output_lines, 3);
        assert_eq!(report.invalid_lines, 1);
        assert!(!report.applied);
        assert!(report.backup.is_none());

        let repaired_text = fs::read_to_string(output).unwrap();
        let repaired_values = repaired_text
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(repaired_values.len(), 3);
        assert_eq!(repaired_values[0]["queueId"], "q1");
        assert_eq!(repaired_values[1]["queueId"], "q2");
        assert_eq!(repaired_values[2]["queueId"], "q3");

        let invalid_text = fs::read_to_string(invalid_output).unwrap();
        let invalid = serde_json::from_str::<serde_json::Value>(invalid_text.trim()).unwrap();
        assert_eq!(invalid["lineNumber"], 3);
        assert_eq!(invalid["raw"], "{not-json}");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn harness_status_accepts_shared_activation_path_args() {
        let args = vec![
            "--source-home".to_string(),
            "source-home".to_string(),
            "--workspace".to_string(),
            "workspace".to_string(),
            "--runtime-workspace".to_string(),
            "runtime-workspace".to_string(),
            "--harness-home".to_string(),
            "harness-home".to_string(),
            "--json".to_string(),
        ];

        let parsed = harness_status_args_from_args(&args).unwrap();

        assert_eq!(parsed.target_home, PathBuf::from("harness-home"));
        assert!(parsed.json);
    }

    #[test]
    fn typing_context_ignores_terminal_runtime_receipts() {
        let root = std::env::temp_dir().join(format!(
            "agent-harness-cli-typing-{}",
            current_time_ms().unwrap()
        ));
        let queue_dir = root.join("state").join("runtime-queue");
        fs::create_dir_all(&queue_dir).unwrap();
        fs::write(
            queue_dir.join("pending.jsonl"),
            serde_json::json!({
                "queueId": "queue-1",
                "status": "queued",
                "agentId": "main",
                "platform": "telegram",
                "channelId": "chat-1"
            })
            .to_string(),
        )
        .unwrap();
        fs::write(
            queue_dir.join("run-once-receipts.jsonl"),
            serde_json::json!({
                "queueId": "queue-1",
                "status": "timeout"
            })
            .to_string(),
        )
        .unwrap();

        assert!(
            pending_runtime_typing_context(&root, Some("queue-1"))
                .unwrap()
                .is_none()
        );

        fs::write(
            queue_dir.join("run-once-receipts.jsonl"),
            serde_json::json!({
                "queueId": "queue-1",
                "status": "failed-terminal"
            })
            .to_string(),
        )
        .unwrap();
        assert!(
            pending_runtime_typing_context(&root, Some("queue-1"))
                .unwrap()
                .is_none()
        );

        fs::write(
            queue_dir.join("run-once-receipts.jsonl"),
            serde_json::json!({
                "queueId": "queue-1",
                "status": "canceled"
            })
            .to_string(),
        )
        .unwrap();
        assert!(
            pending_runtime_typing_context(&root, Some("queue-1"))
                .unwrap()
                .is_none()
        );

        let _ = fs::remove_dir_all(root);
    }
}
