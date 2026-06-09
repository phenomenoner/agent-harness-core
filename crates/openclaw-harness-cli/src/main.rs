use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use openclaw_harness_core::{
    ActivationReadinessOptions, ActivationReadinessReport, AgentProgressDeliveryAction,
    AgentProgressDeliveryPending, AgentProgressDeliveryPlanOptions,
    AgentProgressDeliveryRecordOptions, AgentProgressDeliveryStatus, AgentRegistry,
    BuiltinHarnessSkillSyncOptions, BuiltinHarnessSkillSyncReport, ChannelCommand,
    ChannelCommandApplyOptions, ChannelCommandApplyReport, ChannelDeliveryReceipt,
    ChannelDeliveryRecordOptions, ChannelDeliveryStatus, ChannelOutboxPlanOptions,
    ChannelOutboxPlanReport, ChannelReceiveOptions, ChannelReceiveReport, ChannelRunOnceOptions,
    ChannelRunOnceReport, ChannelStep, CodexRuntimeCompletionOptions, CodexRuntimeCompletionReport,
    CodexRuntimeLaunchProbeOptions, CodexRuntimeLaunchProbeReport, CodexRuntimePlanOptions,
    CodexRuntimePlanReport, CodexRuntimePreflightOptions, CodexRuntimePreflightReport,
    CodexRuntimeRunOptions, CodexRuntimeRunReport, ConflictPolicy, DeterministicCronPlan,
    DeterministicCronPlanInput, DryRunImportOptions, ExecuteImportOptions, HarnessLogEvent,
    HarnessLogLevel, HarnessStatusOptions, HarnessStatusReport, ImportPhaseStatus, ImportReport,
    MemoryCanvasWorkerOptions, MemoryCanvasWorkerReport, MemoryCanvasWorkerStatus,
    MemoryCredentialsExportOptions, MemoryCredentialsExportReport, MemorySearchOptions,
    MemorySearchReport, MemoryVectorRecallOptions, MemoryVectorRecallReport,
    MemoryVectorRecallStatus, NativeCronPlan, NativeCronPlanInput, OpenClawSource,
    PromptAssemblyOptions, PromptBundle, RuntimeQueueEnqueueOptions, RuntimeQueueEnqueueReport,
    RuntimeQueuePrepareOptions, RuntimeQueuePrepareReport, RuntimeRunOnceOptions,
    RuntimeRunOnceReport, RuntimeRunOnceStatus, SkillIndex, SkillSelectionQuery, SubagentPlan,
    SubagentPlanInput, TurnPlan, TurnPlanInput, WindowsSupervisorPlanOptions,
    WindowsSupervisorPlanReport, append_harness_log, apply_channel_command_step,
    assemble_prompt_bundle, build_channel_step, build_dry_run_report, build_harness_skill_index,
    build_import_plan, build_runtime_skill_index, build_source_skill_index, build_turn_plan,
    check_activation_readiness, collect_harness_status, current_log_time_ms, enqueue_channel_step,
    execute_import, export_harness_registry_files, export_memory_credentials, inventory,
    load_agent_registry, load_deterministic_cron_store, load_native_cron_store,
    load_subagent_ledger, parse_channel_command, plan_agent_progress_delivery, plan_channel_outbox,
    plan_codex_runtime, plan_deterministic_cron, plan_native_cron, plan_subagents,
    preflight_codex_runtime, prepare_runtime_queue_item, probe_codex_runtime_launch,
    receive_channel_message, record_agent_progress_delivery, record_channel_delivery,
    record_codex_runtime_completion, run_channel_once, run_codex_runtime, run_memory_canvas_worker,
    run_runtime_queue_once, search_imported_memory, search_imported_vector_memory, select_skills,
    sync_builtin_harness_skills, write_channel_step, write_deterministic_cron_plan,
    write_memory_search_receipt, write_memory_vector_recall_receipt, write_native_cron_plan,
    write_prompt_bundle, write_report_files, write_skill_index, write_subagent_plan,
    write_turn_plan, write_windows_supervisor_plan,
};

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
        "memory-credentials-export" => run_memory_credentials_export(&rest),
        "memory-search" => run_memory_search(&rest),
        "memory-vector-search" => run_memory_vector_search(&rest),
        "memory-canvas-run" => run_memory_canvas_run(&rest),
        "supervisor-plan" => run_supervisor_plan(&rest),
        "harness-skills-sync" => run_harness_skills_sync(&rest),
        "skills" => run_skills(&rest),
        "turn-plan" => run_turn_plan(&rest),
        "channel-step" => run_channel_step(&rest),
        "channel-apply" => run_channel_apply(&rest),
        "channel-receive" => run_channel_receive(&rest),
        "channel-run-once" => run_channel_run_once(&rest),
        "channel-outbox-plan" => run_channel_outbox_plan(&rest),
        "channel-delivery-record" => run_channel_delivery_record(&rest),
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
        "codex-plan" => run_codex_plan(&rest),
        "codex-preflight" => run_codex_preflight(&rest),
        "codex-launch-probe" => run_codex_launch_probe(&rest),
        "codex-run" => run_codex_run(&rest),
        "codex-complete" => run_codex_complete(&rest),
        "prompt-bundle" => run_prompt_bundle(&rest),
        "cron-plan" => run_cron_plan(&rest),
        "deterministic-cron-plan" => run_deterministic_cron_plan(&rest),
        "subagent-plan" => run_subagent_plan(&rest),
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
    println!("OpenClaw home: {}", source.home.display());
    println!("Workspace: {}", source.workspace.display());

    let inv = inventory(source).map_err(|err| err.to_string())?;
    if inv.is_empty() {
        println!("No OpenClaw data detected at this source.");
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
        yes_no(inv.memory_openclaw_mem_sqlite)
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

    println!("OpenClaw import execute");
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

    println!("OpenClaw channel credentials export");
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
        source_home: args.openclaw_home,
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

fn run_supervisor_plan(args: &[String]) -> Result<(), String> {
    let args = supervisor_plan_args_from_args(args)?;
    let report = write_windows_supervisor_plan(WindowsSupervisorPlanOptions {
        harness_home: args.target_home,
        openclaw_home: args.openclaw_home,
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
        include_progress: args.include_progress,
        include_telegram: args.include_telegram,
        include_discord: args.include_discord,
        idle_ms: args.idle_ms,
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
                limit: args.limit,
            },
        );
        println!();
        println!("Matched skills: {}", selections.len());
        for selection in selections {
            println!(
                "- {} [{:?}] score={} title={}",
                selection.skill_id, selection.source_kind, selection.score, selection.title
            );
            if !selection.reasons.is_empty() {
                println!("  {}", selection.reasons.join("; "));
            }
            println!("  {}", selection.directory.display());
        }
    }

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
        channel_id: args.turn.channel_id,
        user_id: args.turn.user_id,
        agent_id: args.turn.agent_id,
        session_key: args.turn.session_key,
        message: args.turn.message,
        inbound_context: None,
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
        channel_id: args.turn.channel_id,
        user_id: args.turn.user_id,
        agent_id: args.turn.agent_id,
        session_key: args.turn.session_key,
        message: args.turn.message,
        inbound_context: None,
        skill_limit: args.turn.skill_limit,
        now_ms: args.now_ms,
        codex_executable: args.codex_exe,
        timeout_ms: args.timeout_ms,
        prompt_options: PromptAssemblyOptions {
            max_prompt_file_bytes: args.turn.max_prompt_file_bytes,
            max_skill_file_bytes: args.turn.max_skill_file_bytes,
            harness_home: Some(args.target_home),
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
        match execute_progress_delivery_once(&args.send) {
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
        thread::sleep(Duration::from_millis(args.idle_ms));
    }
    Ok(())
}

fn execute_progress_delivery_once(
    args: &ProgressDeliveryOnceArgs,
) -> Result<ProgressDeliveryOnceReport, String> {
    let plan = plan_agent_progress_delivery(AgentProgressDeliveryPlanOptions {
        harness_home: args.target_home.clone(),
        platform: args.platform.clone(),
        now_ms: current_time_ms()?,
        min_update_interval_ms: args.min_update_interval_ms,
        max_events_per_panel: args.max_events_per_panel,
        max_preview_chars: args.max_preview_chars,
    })
    .map_err(|err| err.to_string())?;
    let mut warnings = plan.warnings.clone();
    let policy = channel_access_policy(&args.target_home)?;
    let pending_count = plan.pending.len();
    let mut sent_messages = 0usize;
    let mut edited_messages = 0usize;
    let mut skipped_denied = 0usize;
    let mut failed_deliveries = 0usize;

    for pending in plan.pending {
        if let Err(reason) = progress_delivery_allowed(&policy, &pending) {
            record_progress_delivery(
                args,
                &pending,
                pending.action,
                AgentProgressDeliveryStatus::SkippedDenied,
                pending.provider_message_id.clone(),
                Some(reason.clone()),
            )?;
            warnings.push(format!(
                "progress delivery for {} denied by channel access policy: {}",
                pending.queue_id, reason
            ));
            skipped_denied += 1;
            continue;
        }

        match deliver_progress_pending(args, &pending) {
            Ok((actual_action, provider_message_id)) => {
                record_progress_delivery(
                    args,
                    &pending,
                    actual_action,
                    AgentProgressDeliveryStatus::Delivered,
                    provider_message_id,
                    None,
                )?;
                match actual_action {
                    AgentProgressDeliveryAction::Send => sent_messages += 1,
                    AgentProgressDeliveryAction::Edit => edited_messages += 1,
                }
            }
            Err(error) => {
                record_progress_delivery(
                    args,
                    &pending,
                    pending.action,
                    AgentProgressDeliveryStatus::Failed,
                    pending.provider_message_id.clone(),
                    Some(error.clone()),
                )?;
                warnings.push(error);
                failed_deliveries += 1;
            }
        }
    }

    let report = ProgressDeliveryOnceReport {
        pending_count,
        sent_messages,
        edited_messages,
        skipped_denied,
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
                "pending={} sent={} edited={} denied={} failed={}",
                report.pending_count,
                report.sent_messages,
                report.edited_messages,
                report.skipped_denied,
                report.failed_deliveries
            ),
        ),
    )
    .map_err(|err| err.to_string())?;
    Ok(report)
}

fn deliver_progress_pending(
    args: &ProgressDeliveryOnceArgs,
    pending: &AgentProgressDeliveryPending,
) -> Result<(AgentProgressDeliveryAction, Option<String>), String> {
    match pending.platform.as_str() {
        "telegram" => {
            let token = telegram_bot_token(&args.target_home, args.telegram_account.as_deref())?;
            match pending.action {
                AgentProgressDeliveryAction::Send => {
                    telegram_send_message(&token, &pending.channel_id, &pending.text)
                        .map(|id| (AgentProgressDeliveryAction::Send, id))
                }
                AgentProgressDeliveryAction::Edit => {
                    let Some(message_id) = pending.provider_message_id.as_deref() else {
                        return telegram_send_message(&token, &pending.channel_id, &pending.text)
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
                        telegram_send_message(&token, &pending.channel_id, &pending.text).map(
                            |id| {
                                (
                                    AgentProgressDeliveryAction::Send,
                                    id.or(Some(message_id.to_string())),
                                )
                            },
                        ).map_err(|send_error| {
                            format!(
                                "Telegram progress edit failed ({edit_error}); replacement send failed ({send_error})"
                            )
                        })
                    })
                }
            }
        }
        "discord" => {
            let token = discord_bot_token(&args.target_home)?;
            match pending.action {
                AgentProgressDeliveryAction::Send => {
                    discord_send_message(&token, &pending.channel_id, &pending.text)
                        .map(|id| (AgentProgressDeliveryAction::Send, id))
                }
                AgentProgressDeliveryAction::Edit => {
                    let Some(message_id) = pending.provider_message_id.as_deref() else {
                        return discord_send_message(&token, &pending.channel_id, &pending.text)
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
                            discord_send_message(&token, &pending.channel_id, &pending.text)
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
    error: Option<String>,
) -> Result<(), String> {
    record_agent_progress_delivery(AgentProgressDeliveryRecordOptions {
        harness_home: args.target_home.clone(),
        queue_id: pending.queue_id.clone(),
        platform: pending.platform.clone(),
        channel_id: pending.channel_id.clone(),
        user_id: pending.user_id.clone(),
        session_key: pending.session_key.clone(),
        message_kind: pending.message_kind,
        action,
        status,
        provider_message_id,
        event_line: pending.event_line,
        text_hash: pending.text_hash.clone(),
        terminal: pending.terminal,
        error,
        now_ms: current_time_ms()?,
    })
    .map(|_| ())
    .map_err(|err| err.to_string())
}

fn progress_delivery_allowed(
    policy: &ChannelAccessPolicy,
    pending: &AgentProgressDeliveryPending,
) -> Result<(), String> {
    match pending.platform.as_str() {
        "telegram" => {
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
                    return Ok(());
                }
                return Err(
                    "Telegram group progress target is not authorized for this user id".to_string(),
                );
            }
            if telegram_user_is_admin(policy, &pending.user_id) {
                Ok(())
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
                    return Ok(());
                }
                return Err(
                    "Discord channel progress target is not authorized for this user id"
                        .to_string(),
                );
            }
            if discord_user_is_admin(policy, &pending.user_id) {
                Ok(())
            } else {
                Err("Discord DM progress target user id is not an admin/allowed user".to_string())
            }
        }
        other => Err(format!(
            "progress delivery does not know access policy for platform `{other}`"
        )),
    }
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
                "telegram-loop",
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
            "telegram-loop",
            "running",
            iterations,
            "polling Telegram updates",
        )?;
        match execute_telegram_poll_once(&args.poll, &token) {
            Ok(report) => {
                consecutive_errors = 0;
                write_loop_heartbeat(
                    &args.poll.target_home,
                    "telegram-loop",
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
                    "telegram-loop",
                    "error",
                    iterations,
                    &format!("consecutiveErrors={consecutive_errors} error={error}"),
                )?;
                eprintln!(
                    "telegram-loop iteration {iterations} failed ({consecutive_errors}/{}): {error}",
                    args.max_consecutive_errors
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
                        "telegram-loop exceeded {} consecutive errors; last error: {error}",
                        args.max_consecutive_errors
                    ));
                }
            }
        }

        if args.iterations > 0 && iterations >= args.iterations {
            break;
        }
        thread::sleep(Duration::from_millis(args.idle_ms));
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
        "schema": "openclaw-harness.telegram-probe-receipt.v1",
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
    fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&report.receipt_file)
        .and_then(|mut file| writeln!(file, "{receipt}"))
        .map_err(|err| err.to_string())?;
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
    let mut handled_messages = 0;
    let mut skipped_updates = 0;
    let mut next_offset = offset;
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
            channel_id: chat_id,
            user_id,
            agent_id: telegram_effective_agent_id(args),
            session_key: None,
            message: text,
            inbound_context,
            skill_limit: args.skill_limit,
            now_ms: current_time_ms()?,
            codex_executable: args.codex_exe.clone(),
            timeout_ms: args.timeout_ms,
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
        let text = format_channel_reply_text(&pending.message.text);
        match telegram_send_message(token, &pending.message.channel_id, &text) {
            Ok(provider_message_id) => {
                record_channel_delivery(ChannelDeliveryRecordOptions {
                    harness_home: args.target_home.clone(),
                    delivery_id: pending.delivery_id,
                    status: ChannelDeliveryStatus::Delivered,
                    platform: pending.message.platform,
                    channel_id: pending.message.channel_id,
                    user_id: pending.message.user_id,
                    session_key: pending.message.session_key,
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
                    platform: pending.message.platform,
                    channel_id: pending.message.channel_id,
                    user_id: pending.message.user_id,
                    session_key: pending.message.session_key,
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
    let token = discord_bot_token(&args.target_home)?;
    let report = execute_discord_outbox_send_once(&args, &token)?;
    print_discord_outbox_send_once_report(&report);
    Ok(())
}

fn run_discord_outbox_loop(args: &[String]) -> Result<(), String> {
    let args = discord_outbox_loop_args_from_args(args)?;
    let token = discord_bot_token(&args.send.target_home)?;
    let mut iterations = 0usize;
    let mut consecutive_errors = 0usize;

    loop {
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
        thread::sleep(Duration::from_millis(args.idle_ms));
    }

    Ok(())
}

fn run_discord_dm_probe(args: &[String]) -> Result<(), String> {
    let args = discord_dm_probe_args_from_args(args)?;
    let token = discord_bot_token(&args.target_home)?;
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
    let token = discord_bot_token(&args.target_home)?;
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
        match discord_send_message(token, &channel_id, &args.message) {
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
        let text = format_channel_reply_text(&pending.message.text);
        match discord_send_message_chunks(&token, &pending.message.channel_id, &text) {
            Ok(provider_message_id) => {
                record_channel_delivery(ChannelDeliveryRecordOptions {
                    harness_home: args.target_home.clone(),
                    delivery_id: pending.delivery_id,
                    status: ChannelDeliveryStatus::Delivered,
                    platform: pending.message.platform,
                    channel_id: pending.message.channel_id,
                    user_id: pending.message.user_id,
                    session_key: pending.message.session_key,
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
                    platform: pending.message.platform,
                    channel_id: pending.message.channel_id,
                    user_id: pending.message.user_id,
                    session_key: pending.message.session_key,
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
        Some(message) => {
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
                        if let Ok(token) = discord_bot_token(&args.target_home)
                            && let Err(error) = discord_send_typing(&token, &message.channel_id)
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
                            channel_id: message.channel_id.clone(),
                            user_id: message.user_id.clone(),
                            agent_id: args.agent_id.clone(),
                            session_key: None,
                            message: message_text,
                            inbound_context: message.inbound_context.clone(),
                            skill_limit: args.skill_limit,
                            now_ms: current_time_ms()?,
                            codex_executable: args.codex_exe.clone(),
                            timeout_ms: args.timeout_ms,
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
                            reason: "Discord message normalized into channel-run-once".to_string(),
                            message_id: Some(message.message_id),
                            guild_id: message.guild_id,
                            channel_id: Some(message.channel_id),
                            user_id: Some(message.user_id),
                            run: Some(run),
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
        write_loop_heartbeat(
            &args.target_home,
            "discord-gateway-loop",
            "spawning",
            iterations,
            "starting Discord gateway subprocess",
        )?;
        match discord_gateway_command(&args).status() {
            Ok(status) if status.success() => {
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
            Ok(status) => {
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

fn discord_gateway_command(args: &DiscordGatewayArgs) -> Command {
    let mut command = Command::new(&args.node_exe);
    command
        .arg(&args.gateway_script)
        .arg("--harness-home")
        .arg(&args.target_home)
        .arg("--openclaw-home")
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
    if let Some(codex_exe) = &args.codex_exe {
        command.arg("--codex-exe").arg(codex_exe);
    }
    if let Some(stop_file) = &args.stop_file {
        command.arg("--stop-file").arg(stop_file);
    }
    if env::var_os("DISCORD_BOT_TOKEN").is_none()
        && let Some(token) = secret_env_value(&args.target_home, "DISCORD_BOT_TOKEN")
    {
        command.env("DISCORD_BOT_TOKEN", normalize_discord_bot_token(&token));
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
        "id": "openclaw-harness-cli",
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
            "id": "openclaw-harness-cli",
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
        prompt_options: PromptAssemblyOptions {
            max_prompt_file_bytes: args.max_prompt_file_bytes,
            max_skill_file_bytes: args.max_skill_file_bytes,
            harness_home: Some(args.target_home),
        },
    })
    .map_err(|err| err.to_string())?;

    print_runtime_run_once_report(&report);
    Ok(())
}

fn run_runtime_loop(args: &[String]) -> Result<(), String> {
    let args = runtime_loop_args_from_args(args)?;
    let started_at_ms = current_log_time_ms().map_err(|err| err.to_string())?;
    let mut iterations = 0usize;
    let mut completed = 0usize;
    let mut idle = 0usize;
    let mut errors = 0usize;
    let mut consecutive_errors = 0usize;
    let mut last_status: Option<RuntimeRunOnceStatus> = None;
    let mut last_queue_id: Option<String> = None;
    let mut last_reason: Option<String> = None;
    let mut failed = false;
    let stop_reason;

    loop {
        if stop_file_requested(args.stop_file.as_deref()) {
            stop_reason = "stopped after stop file request".to_string();
            write_loop_heartbeat(
                &args.target_home,
                "runtime-loop",
                "stopped",
                iterations,
                "stop file requested",
            )?;
            break;
        }
        iterations += 1;
        write_loop_heartbeat(
            &args.target_home,
            "runtime-loop",
            "running",
            iterations,
            "checking runtime queue",
        )?;
        let _typing = start_runtime_typing_heartbeat(&args.target_home, None);
        match run_runtime_queue_once(RuntimeRunOnceOptions {
            harness_home: args.target_home.clone(),
            queue_id: None,
            codex_executable: args.codex_exe.clone(),
            timeout_ms: args.timeout_ms,
            prompt_options: PromptAssemblyOptions {
                max_prompt_file_bytes: args.max_prompt_file_bytes,
                max_skill_file_bytes: args.max_skill_file_bytes,
                harness_home: Some(args.target_home.clone()),
            },
        }) {
            Ok(report) => {
                let status = report.receipt.status;
                last_status = Some(status);
                last_queue_id = report.receipt.queue_id.clone();
                last_reason = Some(report.receipt.reason.clone());
                write_loop_heartbeat(
                    &args.target_home,
                    "runtime-loop",
                    runtime_run_once_status_label(status),
                    iterations,
                    &report.receipt.reason,
                )?;
                println!(
                    "Runtime loop iteration {iterations}: {} ({})",
                    runtime_run_once_status_label(status),
                    report.receipt.reason
                );

                if runtime_run_once_report_is_idle(&report) {
                    idle += 1;
                    consecutive_errors = 0;
                    if args.stop_when_idle {
                        stop_reason = format!(
                            "stopped after idle runtime result {}",
                            runtime_run_once_status_label(status)
                        );
                        break;
                    }
                } else if status == RuntimeRunOnceStatus::Completed {
                    completed += 1;
                    consecutive_errors = 0;
                } else {
                    errors += 1;
                    consecutive_errors += 1;
                    append_runtime_loop_error_log(
                        &args.target_home,
                        iterations,
                        consecutive_errors,
                        args.max_consecutive_errors,
                        &format!(
                            "{}: {}",
                            runtime_run_once_status_label(status),
                            report.receipt.reason
                        ),
                    )?;
                    if consecutive_errors >= args.max_consecutive_errors {
                        failed = true;
                        stop_reason = format!(
                            "stopped after {} consecutive runtime errors",
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
                    "runtime-loop",
                    "error",
                    iterations,
                    &format!("consecutiveErrors={consecutive_errors} error={error}"),
                )?;
                eprintln!(
                    "runtime-loop iteration {iterations} failed ({consecutive_errors}/{}): {error}",
                    args.max_consecutive_errors
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
                    failed = true;
                    stop_reason = format!(
                        "stopped after {} consecutive runtime errors",
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
        thread::sleep(Duration::from_millis(args.idle_ms));
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
        stop_reason,
        last_status,
        last_queue_id,
        last_reason,
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

fn source_from_args(args: &[String]) -> Result<OpenClawSource, String> {
    let mut home = default_openclaw_home();
    let mut workspace = None;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "--openclaw-home" => {
                i += 1;
                home = args
                    .get(i)
                    .map(PathBuf::from)
                    .ok_or_else(|| "--openclaw-home requires a path".to_string())?;
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
        Some(workspace) => OpenClawSource::with_workspace(home, workspace),
        None => OpenClawSource::new(home),
    })
}

struct DryRunArgs {
    source: OpenClawSource,
    target_home: PathBuf,
    conflict_policy: ConflictPolicy,
    output_dir: Option<PathBuf>,
}

struct ExecuteArgs {
    source: OpenClawSource,
    target_home: PathBuf,
    conflict_policy: ConflictPolicy,
    include_sensitive: bool,
}

struct ChannelCredentialsExportArgs {
    source: OpenClawSource,
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
    source: OpenClawSource,
    target_home: PathBuf,
    conflict_policy: ConflictPolicy,
}

struct EnableCheckArgs {
    target_home: PathBuf,
}

struct HarnessStatusArgs {
    target_home: PathBuf,
    json: bool,
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
    openclaw_home: PathBuf,
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
    json: bool,
}

struct SupervisorPlanArgs {
    target_home: PathBuf,
    openclaw_home: PathBuf,
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
    include_progress: bool,
    include_telegram: bool,
    include_discord: bool,
    idle_ms: u64,
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
    source: OpenClawSource,
    harness_home: Option<PathBuf>,
    output_dir: Option<PathBuf>,
    query: Option<String>,
    agent_id: Option<String>,
    channel: Option<String>,
    match_workspace: Option<String>,
    limit: usize,
}

struct TurnPlanArgs {
    source: OpenClawSource,
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
    sent_messages: usize,
    edited_messages: usize,
    skipped_denied: usize,
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
    source: OpenClawSource,
    runtime_workspace: Option<PathBuf>,
    target_home: PathBuf,
    agent_id: Option<String>,
    telegram_account: Option<String>,
    skill_limit: usize,
    codex_exe: Option<PathBuf>,
    timeout_ms: u64,
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
    iterations: usize,
    idle_ms: u64,
    max_consecutive_errors: usize,
    stop_file: Option<PathBuf>,
}

struct DiscordOutboxSendOnceArgs {
    target_home: PathBuf,
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
    source: OpenClawSource,
    runtime_workspace: Option<PathBuf>,
    target_home: PathBuf,
    agent_id: Option<String>,
    skill_limit: usize,
    codex_exe: Option<PathBuf>,
    timeout_ms: u64,
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
    reply_context: Option<DiscordReplyContext>,
    author_is_bot: bool,
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
    source: OpenClawSource,
    runtime_workspace: Option<PathBuf>,
    target_home: PathBuf,
    node_exe: PathBuf,
    gateway_script: PathBuf,
    harness_cli: PathBuf,
    agent_id: Option<String>,
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
    max_prompt_file_bytes: usize,
    max_skill_file_bytes: usize,
}

struct RuntimeLoopArgs {
    target_home: PathBuf,
    codex_exe: Option<PathBuf>,
    timeout_ms: u64,
    max_prompt_file_bytes: usize,
    max_skill_file_bytes: usize,
    iterations: usize,
    idle_ms: u64,
    max_consecutive_errors: usize,
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
}

struct CodexCompleteArgs {
    target_home: PathBuf,
    execution_dir: Option<PathBuf>,
    plan_file: Option<PathBuf>,
    assistant_message: String,
    finished_at_ms: i64,
}

struct CronPlanArgs {
    source: OpenClawSource,
    output_dir: Option<PathBuf>,
    now_ms: i64,
    resume_cron: bool,
    limit: usize,
}

struct DeterministicCronPlanArgs {
    source: OpenClawSource,
    output_dir: Option<PathBuf>,
    allow_deterministic_run: bool,
    limit: usize,
}

struct SubagentPlanArgs {
    source: OpenClawSource,
    output_dir: Option<PathBuf>,
    resume_subagents: bool,
    limit: usize,
}

fn dry_run_args_from_args(args: &[String]) -> Result<DryRunArgs, String> {
    let mut home = default_openclaw_home();
    let mut workspace = None;
    let mut target_home = default_harness_home();
    let mut conflict_policy = ConflictPolicy::Skip;
    let mut output_dir = None;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "--openclaw-home" => {
                i += 1;
                home = args
                    .get(i)
                    .map(PathBuf::from)
                    .ok_or_else(|| "--openclaw-home requires a path".to_string())?;
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
        Some(workspace) => OpenClawSource::with_workspace(home, workspace),
        None => OpenClawSource::new(home),
    };

    Ok(DryRunArgs {
        source,
        target_home,
        conflict_policy,
        output_dir,
    })
}

fn execute_args_from_args(args: &[String]) -> Result<ExecuteArgs, String> {
    let mut home = default_openclaw_home();
    let mut workspace = None;
    let mut target_home = default_harness_home();
    let mut conflict_policy = ConflictPolicy::Skip;
    let mut include_sensitive = false;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "--openclaw-home" => {
                i += 1;
                home = args
                    .get(i)
                    .map(PathBuf::from)
                    .ok_or_else(|| "--openclaw-home requires a path".to_string())?;
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
        Some(workspace) => OpenClawSource::with_workspace(home, workspace),
        None => OpenClawSource::new(home),
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
    let mut home = default_openclaw_home();
    let mut workspace = None;
    let mut target_home = default_harness_home();
    let mut include_sensitive = false;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "--openclaw-home" => {
                i += 1;
                home = args
                    .get(i)
                    .map(PathBuf::from)
                    .ok_or_else(|| "--openclaw-home requires a path".to_string())?;
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
        Some(workspace) => OpenClawSource::with_workspace(home, workspace),
        None => OpenClawSource::new(home),
    };

    Ok(ChannelCredentialsExportArgs {
        source,
        target_home,
        include_sensitive,
    })
}

fn registry_export_args_from_args(args: &[String]) -> Result<RegistryExportArgs, String> {
    let mut home = default_openclaw_home();
    let mut workspace = None;
    let mut target_home = default_harness_home();
    let mut conflict_policy = ConflictPolicy::Skip;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "--openclaw-home" => {
                i += 1;
                home = args
                    .get(i)
                    .map(PathBuf::from)
                    .ok_or_else(|| "--openclaw-home requires a path".to_string())?;
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
        Some(workspace) => OpenClawSource::with_workspace(home, workspace),
        None => OpenClawSource::new(home),
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
            "--openclaw-home" | "--workspace" | "--runtime-workspace" => {
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
    let mut openclaw_home = default_openclaw_home();
    let mut target_home = default_harness_home();
    let mut include_sensitive = false;
    let mut json = false;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "--openclaw-home" => {
                i += 1;
                openclaw_home = args
                    .get(i)
                    .map(PathBuf::from)
                    .ok_or_else(|| "--openclaw-home requires a path".to_string())?;
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
        openclaw_home,
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
    let mut json = false;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
            }
            "--json" => json = true,
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    Ok(MemoryCanvasRunArgs { target_home, json })
}

fn supervisor_plan_args_from_args(args: &[String]) -> Result<SupervisorPlanArgs, String> {
    let mut target_home = default_harness_home();
    let mut openclaw_home = default_openclaw_home();
    let mut workspace = None;
    let mut runtime_workspace = None;
    let mut harness_cli = default_harness_cli();
    let mut codex_exe = None;
    let mut node_exe = PathBuf::from("node");
    let mut gateway_script = PathBuf::from("tools")
        .join("openclaw-discord-gateway")
        .join("index.mjs");
    let mut agent_id = None;
    let mut output_dir = None;
    let mut task_prefix = "OpenClawHarness".to_string();
    let mut include_runtime = true;
    let mut include_progress = true;
    let mut include_telegram = true;
    let mut include_discord = true;
    let mut idle_ms = 1_000;
    let mut max_consecutive_errors = 5;
    let mut telegram_poll_timeout_seconds = 1;
    let mut telegram_max_updates = 10;
    let mut telegram_outbox_limit = 20;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "--openclaw-home" => {
                i += 1;
                openclaw_home = args
                    .get(i)
                    .map(PathBuf::from)
                    .ok_or_else(|| "--openclaw-home requires a path".to_string())?;
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

    Ok(SupervisorPlanArgs {
        target_home,
        openclaw_home,
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
        include_progress,
        include_telegram,
        include_discord,
        idle_ms,
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
    let mut home = default_openclaw_home();
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
            "--openclaw-home" => {
                i += 1;
                home = args
                    .get(i)
                    .map(PathBuf::from)
                    .ok_or_else(|| "--openclaw-home requires a path".to_string())?;
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
        Some(workspace) => OpenClawSource::with_workspace(home, workspace),
        None => OpenClawSource::new(home),
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
    let mut home = default_openclaw_home();
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
            "--openclaw-home" => {
                i += 1;
                home = args
                    .get(i)
                    .map(PathBuf::from)
                    .ok_or_else(|| "--openclaw-home requires a path".to_string())?;
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
        Some(workspace) => OpenClawSource::with_workspace(home, workspace),
        None => OpenClawSource::new(home),
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
    let mut timeout_ms = 300_000;
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
    let mut home = default_openclaw_home();
    let mut workspace = None;
    let mut runtime_workspace = None;
    let mut target_home = default_harness_home();
    let mut agent_id = None;
    let mut telegram_account = None;
    let mut skill_limit = 5;
    let mut codex_exe = None;
    let mut timeout_ms = 300_000;
    let mut poll_timeout_seconds = 1;
    let mut max_updates = 10;
    let mut outbox_limit = 20;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "--openclaw-home" => {
                i += 1;
                home = args
                    .get(i)
                    .map(PathBuf::from)
                    .ok_or_else(|| "--openclaw-home requires a path".to_string())?;
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
        Some(workspace) => OpenClawSource::with_workspace(home, workspace),
        None => OpenClawSource::new(home),
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
        poll_timeout_seconds,
        max_updates,
        outbox_limit,
    })
}

fn telegram_loop_args_from_args(args: &[String]) -> Result<TelegramLoopArgs, String> {
    let mut home = default_openclaw_home();
    let mut workspace = None;
    let mut runtime_workspace = None;
    let mut target_home = default_harness_home();
    let mut agent_id = None;
    let mut telegram_account = None;
    let mut skill_limit = 5;
    let mut codex_exe = None;
    let mut timeout_ms = 300_000;
    let mut poll_timeout_seconds = 1;
    let mut max_updates = 10;
    let mut outbox_limit = 20;
    let mut iterations = 0;
    let mut idle_ms = 1_000;
    let mut max_consecutive_errors = 5;
    let mut stop_file = None;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "--openclaw-home" => {
                i += 1;
                home = args
                    .get(i)
                    .map(PathBuf::from)
                    .ok_or_else(|| "--openclaw-home requires a path".to_string())?;
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
        Some(workspace) => OpenClawSource::with_workspace(home, workspace),
        None => OpenClawSource::new(home),
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
            poll_timeout_seconds,
            max_updates,
            outbox_limit,
        },
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
    let mut outbox_limit = 20;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            flag if is_harness_home_arg(flag) => {
                i += 1;
                target_home = parse_harness_home_path(args, i, flag)?;
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
        outbox_limit,
    })
}

fn discord_outbox_loop_args_from_args(args: &[String]) -> Result<DiscordOutboxLoopArgs, String> {
    let mut target_home = default_harness_home();
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
    let mut home = default_openclaw_home();
    let mut workspace = None;
    let mut runtime_workspace = None;
    let mut target_home = default_harness_home();
    let mut agent_id = None;
    let mut skill_limit = 5;
    let mut codex_exe = None;
    let mut timeout_ms = 300_000;
    let mut outbox_limit = 20;
    let mut event_file = None;
    let mut event_json = None;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "--openclaw-home" => {
                i += 1;
                home = args
                    .get(i)
                    .map(PathBuf::from)
                    .ok_or_else(|| "--openclaw-home requires a path".to_string())?;
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

    let source = match workspace {
        Some(workspace) => OpenClawSource::with_workspace(home, workspace),
        None => OpenClawSource::new(home),
    };
    Ok(DiscordEventRunOnceArgs {
        source,
        runtime_workspace,
        target_home,
        agent_id,
        skill_limit,
        codex_exe,
        timeout_ms,
        outbox_limit,
        event_file,
        event_json,
    })
}

fn discord_gateway_args_from_args(args: &[String]) -> Result<DiscordGatewayArgs, String> {
    let mut home = default_openclaw_home();
    let mut workspace = None;
    let mut runtime_workspace = None;
    let mut target_home = default_harness_home();
    let mut node_exe = PathBuf::from("node");
    let mut gateway_script = PathBuf::from("tools")
        .join("openclaw-discord-gateway")
        .join("index.mjs");
    let mut harness_cli = default_harness_cli();
    let mut agent_id = None;
    let mut codex_exe = None;
    let mut max_messages = 0;
    let mut stop_file = None;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "--openclaw-home" => {
                i += 1;
                home = args
                    .get(i)
                    .map(PathBuf::from)
                    .ok_or_else(|| "--openclaw-home requires a path".to_string())?;
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

    let source = match workspace {
        Some(workspace) => OpenClawSource::with_workspace(home, workspace),
        None => OpenClawSource::new(home),
    };
    Ok(DiscordGatewayArgs {
        source,
        runtime_workspace,
        target_home,
        node_exe,
        gateway_script,
        harness_cli,
        agent_id,
        codex_exe,
        max_messages,
        stop_file,
    })
}

fn plugin_sidecar_probe_args_from_args(args: &[String]) -> Result<PluginSidecarProbeArgs, String> {
    let mut target_home = default_harness_home();
    let mut node_exe = PathBuf::from("node");
    let mut sidecar_script = PathBuf::from("tools")
        .join("openclaw-plugin-sidecar")
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
        .join("openclaw-plugin-sidecar")
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

    Ok(PluginSidecarCallArgs {
        target_home,
        node_exe,
        sidecar_script,
        method,
        params,
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
    let mut timeout_ms = 300_000;
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
        max_prompt_file_bytes,
        max_skill_file_bytes,
    })
}

fn runtime_loop_args_from_args(args: &[String]) -> Result<RuntimeLoopArgs, String> {
    let mut target_home = default_harness_home();
    let mut codex_exe = None;
    let mut timeout_ms = 300_000;
    let mut max_prompt_file_bytes = PromptAssemblyOptions::default().max_prompt_file_bytes;
    let mut max_skill_file_bytes = PromptAssemblyOptions::default().max_skill_file_bytes;
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
                target_home = parse_harness_home_path(args, i, flag)?;
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

    Ok(RuntimeLoopArgs {
        target_home,
        codex_exe,
        timeout_ms,
        max_prompt_file_bytes,
        max_skill_file_bytes,
        iterations,
        idle_ms,
        max_consecutive_errors,
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
    let mut timeout_ms = 300_000;
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
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    Ok(CodexRunArgs {
        target_home,
        execution_dir,
        plan_file,
        timeout_ms,
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
    let mut home = default_openclaw_home();
    let mut workspace = None;
    let mut output_dir = None;
    let mut now_ms = current_time_ms()?;
    let mut resume_cron = false;
    let mut limit = 20;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "--openclaw-home" => {
                i += 1;
                home = args
                    .get(i)
                    .map(PathBuf::from)
                    .ok_or_else(|| "--openclaw-home requires a path".to_string())?;
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
        Some(workspace) => OpenClawSource::with_workspace(home, workspace),
        None => OpenClawSource::new(home),
    };

    Ok(CronPlanArgs {
        source,
        output_dir,
        now_ms,
        resume_cron,
        limit,
    })
}

fn deterministic_cron_plan_args_from_args(
    args: &[String],
) -> Result<DeterministicCronPlanArgs, String> {
    let mut home = default_openclaw_home();
    let mut workspace = None;
    let mut output_dir = None;
    let mut allow_deterministic_run = false;
    let mut limit = 20;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "--openclaw-home" => {
                i += 1;
                home = args
                    .get(i)
                    .map(PathBuf::from)
                    .ok_or_else(|| "--openclaw-home requires a path".to_string())?;
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
        Some(workspace) => OpenClawSource::with_workspace(home, workspace),
        None => OpenClawSource::new(home),
    };

    Ok(DeterministicCronPlanArgs {
        source,
        output_dir,
        allow_deterministic_run,
        limit,
    })
}

fn subagent_plan_args_from_args(args: &[String]) -> Result<SubagentPlanArgs, String> {
    let mut home = default_openclaw_home();
    let mut workspace = None;
    let mut output_dir = None;
    let mut resume_subagents = false;
    let mut limit = 20;
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "--openclaw-home" => {
                i += 1;
                home = args
                    .get(i)
                    .map(PathBuf::from)
                    .ok_or_else(|| "--openclaw-home requires a path".to_string())?;
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
        Some(workspace) => OpenClawSource::with_workspace(home, workspace),
        None => OpenClawSource::new(home),
    };

    Ok(SubagentPlanArgs {
        source,
        output_dir,
        resume_subagents,
        limit,
    })
}

fn default_openclaw_home() -> PathBuf {
    if let Ok(value) = env::var("OPENCLAW_HOME") {
        return PathBuf::from(value);
    }

    if let Ok(value) = env::var("USERPROFILE") {
        return PathBuf::from(value).join(".openclaw");
    }

    PathBuf::from(".openclaw")
}

fn default_harness_home() -> PathBuf {
    if let Ok(value) = env::var("OPENCLAW_HARNESS_HOME") {
        return PathBuf::from(value);
    }

    if let Ok(value) = env::var("USERPROFILE") {
        return PathBuf::from(value).join(".openclaw-harness");
    }

    PathBuf::from(".openclaw-harness")
}

fn default_harness_cli() -> PathBuf {
    let exe = if cfg!(windows) {
        "openclaw-harness.exe"
    } else {
        "openclaw-harness"
    };
    PathBuf::from("target").join("debug").join(exe)
}

fn is_harness_home_arg(flag: &str) -> bool {
    matches!(flag, "--target-home" | "--harness-home")
}

fn parse_harness_home_path(args: &[String], index: usize, flag: &str) -> Result<PathBuf, String> {
    args.get(index)
        .map(PathBuf::from)
        .ok_or_else(|| format!("{flag} requires a path"))
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
        "schema": "openclaw-harness.plugin-sidecar-bridge-receipt.v1",
        "status": status,
        "method": method,
        "responseFile": response_file.display().to_string(),
        "reason": reason,
    });
    fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(receipts_file)
        .and_then(|mut file| writeln!(file, "{receipt}"))
        .map_err(|err| err.to_string())
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
    let reply_context = discord_reply_context(payload);
    let inbound_context = discord_inbound_context(payload);

    Ok(Some(DiscordGatewayMessage {
        message_id: message_id.to_string(),
        guild_id,
        channel_id: channel_id.to_string(),
        user_id: user_id.to_string(),
        content: content.to_string(),
        inbound_context,
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
        "schema": "openclaw-harness.discord-event-receipt.v1",
        "status": report.status,
        "reason": report.reason,
        "messageId": report.message_id,
        "guildId": report.guild_id,
        "channelId": report.channel_id,
        "userId": report.user_id,
        "sessionKey": report.run.as_ref().map(|run| run.receive.session_key.clone()),
    });
    fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .and_then(|mut file| writeln!(file, "{receipt}"))
        .map_err(|err| err.to_string())
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
        "schema": "openclaw-harness.discord-reply-context-receipt.v1",
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
    fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .and_then(|mut file| writeln!(file, "{receipt}"))
        .map_err(|err| err.to_string())
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
        "schema": "openclaw-harness.discord-dm-probe-receipt.v1",
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
    fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(receipts_file)
        .and_then(|mut file| writeln!(file, "{receipt}"))
        .map_err(|err| err.to_string())
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
        "schema": "openclaw-harness.discord-dm-history-probe-receipt.v1",
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
    fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(receipts_file)
        .and_then(|mut file| writeln!(file, "{receipt}"))
        .map_err(|err| err.to_string())
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
    source: &OpenClawSource,
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
    source: &OpenClawSource,
    telegram: &serde_json::Value,
    candidates: &mut Vec<ChannelCredentialCandidate>,
) {
    let default_account = string_child(telegram, "defaultAccount")
        .unwrap_or_else(|| "default".to_string())
        .trim()
        .to_string();
    add_channel_credential(
        candidates,
        "OPENCLAW_TELEGRAM_DEFAULT_ACCOUNT",
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
        "OPENCLAW_TELEGRAM_ALLOWED_USER_IDS",
        &allowed_user_ids,
        "channels.telegram.allowFrom + accounts.*.allowFrom",
    );
    add_joined_credential(
        candidates,
        "OPENCLAW_TELEGRAM_GROUP_ALLOWED_USER_IDS",
        &group_allowed_user_ids,
        "channels.telegram.groupAllowFrom + accounts.*.groupAllowFrom",
    );
    add_joined_credential(
        candidates,
        "OPENCLAW_TELEGRAM_DIRECT_CHAT_IDS",
        &direct_chat_ids,
        "channels.telegram.accounts.*.direct keys",
    );
    add_joined_credential(
        candidates,
        "OPENCLAW_TELEGRAM_GROUP_CHAT_IDS",
        &group_chat_ids,
        "channels.telegram.accounts.*.groups keys",
    );
}

fn collect_telegram_account_token(
    source: &OpenClawSource,
    account_id: &str,
    account: &serde_json::Value,
    default_account: &str,
    candidates: &mut Vec<ChannelCredentialCandidate>,
) {
    let env_name = if account_id == default_account || account_id == "default" {
        "TELEGRAM_BOT_TOKEN".to_string()
    } else {
        format!(
            "OPENCLAW_TELEGRAM_ACCOUNT_{}_BOT_TOKEN",
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
        && let Ok(Some(token)) = read_openclaw_token_file(source, &token_file)
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
        "OPENCLAW_DISCORD_ALLOWED_USER_IDS",
        &allowed_user_ids,
        "channels.discord.allowFrom + guilds.*.users",
    );
    add_joined_credential(
        candidates,
        "OPENCLAW_DISCORD_GUILD_IDS",
        &guild_ids,
        "channels.discord.guilds keys",
    );
    add_joined_credential(
        candidates,
        "OPENCLAW_DISCORD_CHANNEL_IDS",
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

fn read_openclaw_token_file(
    source: &OpenClawSource,
    token_file: &str,
) -> Result<Option<String>, String> {
    let path = resolve_openclaw_config_path(source, token_file);
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

fn resolve_openclaw_config_path(source: &OpenClawSource, raw: &str) -> PathBuf {
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
    source: &OpenClawSource,
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
        "schema": "openclaw-harness.channel-credentials-export.v1",
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
    let mut text = String::from(
        "# Generated by openclaw-harness channel-credentials-export. Do not commit.\n",
    );
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
        telegram_admin_user_ids: channel_id_set(&values, "OPENCLAW_TELEGRAM_ADMIN_USER_IDS"),
        telegram_allowed_user_ids: channel_id_set(&values, "OPENCLAW_TELEGRAM_ALLOWED_USER_IDS"),
        telegram_group_admin_user_ids: channel_id_set(
            &values,
            "OPENCLAW_TELEGRAM_GROUP_ADMIN_USER_IDS",
        ),
        telegram_group_allowed_user_ids: channel_id_set(
            &values,
            "OPENCLAW_TELEGRAM_GROUP_ALLOWED_USER_IDS",
        ),
        telegram_direct_chat_ids: channel_id_set(&values, "OPENCLAW_TELEGRAM_DIRECT_CHAT_IDS"),
        telegram_group_chat_ids: channel_id_set(&values, "OPENCLAW_TELEGRAM_GROUP_CHAT_IDS"),
        telegram_group_open: channel_bool(&values, "OPENCLAW_TELEGRAM_GROUP_OPEN"),
        discord_admin_user_ids: channel_id_set(&values, "OPENCLAW_DISCORD_ADMIN_USER_IDS"),
        discord_allowed_user_ids: channel_id_set(&values, "OPENCLAW_DISCORD_ALLOWED_USER_IDS"),
        discord_group_allowed_user_ids: channel_id_set(
            &values,
            "OPENCLAW_DISCORD_GROUP_ALLOWED_USER_IDS",
        ),
        discord_channel_ids: channel_id_set(&values, "OPENCLAW_DISCORD_CHANNEL_IDS"),
        discord_guild_ids: channel_id_set(&values, "OPENCLAW_DISCORD_GUILD_IDS"),
        discord_group_open: channel_bool(&values, "OPENCLAW_DISCORD_GROUP_OPEN"),
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
                "Telegram group chat id is not in OPENCLAW_TELEGRAM_GROUP_CHAT_IDS".to_string(),
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
                "Telegram group chat id is not configured in OPENCLAW_TELEGRAM_GROUP_CHAT_IDS"
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
                "Telegram direct chat id is not in OPENCLAW_TELEGRAM_DIRECT_CHAT_IDS".to_string(),
            );
        }
        if !telegram_user_is_admin(policy, user_id) {
            return ChannelAccessDecision::Denied(
                "Telegram DM user id is not in OPENCLAW_TELEGRAM_ADMIN_USER_IDS or OPENCLAW_TELEGRAM_ALLOWED_USER_IDS".to_string(),
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
                "Discord guild id is not in OPENCLAW_DISCORD_GUILD_IDS".to_string(),
            );
        }
        if !policy.discord_channel_ids.is_empty()
            && !policy.discord_channel_ids.contains(&message.channel_id)
        {
            return ChannelAccessDecision::Denied(
                "Discord channel id is not in OPENCLAW_DISCORD_CHANNEL_IDS".to_string(),
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
            "Discord DM user id is not in OPENCLAW_DISCORD_ADMIN_USER_IDS or OPENCLAW_DISCORD_ALLOWED_USER_IDS".to_string(),
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
        "OPENCLAW_TELEGRAM_ACCOUNT_{}_BOT_TOKEN",
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

fn telegram_effective_agent_id(args: &TelegramPollOnceArgs) -> Option<String> {
    args.agent_id.clone().or_else(|| {
        args.telegram_account.as_ref().and_then(|account| {
            let account = account.trim();
            if account.is_empty() || account == "default" {
                None
            } else {
                Some(account.to_string())
            }
        })
    })
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
            "schema": "openclaw-harness.telegram-offset.v1",
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

fn telegram_send_message(token: &str, chat_id: &str, text: &str) -> Result<Option<String>, String> {
    let url = format!("https://api.telegram.org/bot{token}/sendMessage");
    let agent = channel_http_short_agent();
    let response = agent
        .post(&url)
        .send_json(serde_json::json!({
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": true
        }))
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

fn format_channel_reply_text(text: &str) -> String {
    let trimmed = text.trim();
    if trimmed.is_empty()
        || trimmed.starts_with("◆ OpenClaw")
        || trimmed.starts_with("⏳ ")
        || trimmed.starts_with("✅ ")
        || trimmed.starts_with("⚠️ ")
    {
        return trimmed.to_string();
    }
    format!("◆ OpenClaw\n\n{trimmed}")
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

fn telegram_http_error(error: ureq::Error) -> String {
    match error {
        ureq::Error::Status(code, response) => {
            let body = response.into_string().unwrap_or_default();
            format!("Telegram HTTP status {code}: {body}")
        }
        ureq::Error::Transport(error) => format!("Telegram transport error: {error}"),
    }
}

fn discord_bot_token(harness_home: &Path) -> Result<String, String> {
    env::var("DISCORD_BOT_TOKEN")
        .ok()
        .or_else(|| secret_env_value(harness_home, "DISCORD_BOT_TOKEN"))
        .map(|token| normalize_discord_bot_token(&token))
        .ok_or_else(|| {
            "DISCORD_BOT_TOKEN is required for Discord adapters; run channel-credentials-export or set the env var".to_string()
        })
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

fn discord_send_message_chunks(
    token: &str,
    channel_id: &str,
    text: &str,
) -> Result<Option<String>, String> {
    let chunks = discord_message_chunks(text, DISCORD_MESSAGE_CONTENT_LIMIT);
    let mut provider_message_ids = Vec::new();
    for chunk in chunks {
        if let Some(provider_message_id) = discord_send_message(token, channel_id, &chunk)? {
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
) -> Result<Option<String>, String> {
    if text.chars().count() > DISCORD_MESSAGE_CONTENT_LIMIT {
        return Err("Discord message exceeds the 2000 character content limit".to_string());
    }
    let url = format!("https://discord.com/api/v10/channels/{channel_id}/messages");
    let token = normalize_discord_bot_token(token);
    let auth = format!("Bot {token}");
    let agent = channel_http_short_agent();
    let response = agent
        .post(&url)
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

fn current_time_ms() -> Result<i64, String> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("system time is before Unix epoch: {error}"))?;
    i64::try_from(duration.as_millis())
        .map_err(|_| "current epoch milliseconds exceed i64".to_string())
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
            let token = discord_bot_token(harness_home).ok()?;
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
            && json_string_field(&value, &["status"]) == Some("completed")
        {
            ids.insert(queue_id.to_string());
        }
    }
    Ok(ids)
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
    let heartbeat = serde_json::json!({
        "schema": "openclaw-harness.loop-heartbeat.v1",
        "name": name,
        "status": status,
        "iteration": iteration,
        "detail": detail,
        "atMs": current_time_ms()?,
        "processId": std::process::id(),
    });
    let file = dir.join(format!("{name}.json"));
    fs::write(
        &file,
        serde_json::to_string_pretty(&heartbeat).map_err(|err| err.to_string())?,
    )
    .map_err(|err| format!("failed to write loop heartbeat {}: {err}", file.display()))
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

fn print_dry_run_summary(report: &ImportReport) {
    println!("OpenClaw import dry run");
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
            .filter(|item| item.status == openclaw_harness_core::ImportItemStatus::Conflict)
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
    println!("OpenClaw agent registry");
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
    println!("OpenClaw skill index");
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
    println!("OpenClaw builtin harness skill sync");
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
    println!("OpenClaw Windows supervisor plan");
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
    println!("OpenClaw harness enable check");
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
        "Runtime: queued={} open={} prepared={} completed={} invalid={} runOnce={} codexRun={} completion={}",
        report.runtime.queued_items,
        report.runtime.open_items,
        report.runtime.prepared_items,
        report.runtime.completed_items,
        report.runtime.pending_invalid_lines,
        receipt_summary(&report.runtime.run_once_receipts),
        receipt_summary(&report.runtime.codex_run_receipts),
        receipt_summary(&report.runtime.codex_completion_receipts)
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
        "Memory: qdrantEdge={} lancedb={} openclawMemSqlite={} embeddingSecrets={} files={} activeRecall={} qdrantParity={} captureCandidates={} search={} vectorRecall={} promptContext={} lifecycle={} canvas={}",
        yes_no(report.memory.qdrant_edge),
        yes_no(report.memory.lancedb),
        yes_no(report.memory.openclaw_mem_sqlite),
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

fn receipt_summary(status: &openclaw_harness_core::HarnessJsonlStatus) -> String {
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
    println!("OpenClaw turn plan");
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
    println!("OpenClaw channel step");
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
    println!("OpenClaw channel command apply");
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
    println!("OpenClaw channel receive");
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
    println!("OpenClaw channel run once");
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
    println!("OpenClaw channel outbox plan");
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
    println!("OpenClaw channel delivery receipt");
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
    println!("OpenClaw progress delivery once");
    println!("Pending panels: {}", report.pending_count);
    println!("Sent panels: {}", report.sent_messages);
    println!("Edited panels: {}", report.edited_messages);
    println!("Denied panels: {}", report.skipped_denied);
    println!("Failed deliveries: {}", report.failed_deliveries);
    if !report.warnings.is_empty() {
        println!("Warnings:");
        for warning in &report.warnings {
            println!("- {warning}");
        }
    }
}

fn print_telegram_probe_report(report: &TelegramProbeReport) {
    println!("OpenClaw Telegram probe");
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
    println!("OpenClaw Telegram poll once");
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
    println!("OpenClaw Discord outbox send once");
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
    println!("OpenClaw Discord DM probe");
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
    println!("OpenClaw Discord DM history probe");
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
    println!("OpenClaw Discord event run once");
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
    println!("OpenClaw runtime queue enqueue");
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
    println!("OpenClaw runtime queue prepare");
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
    println!("OpenClaw runtime run once");
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
    println!("OpenClaw runtime loop");
    println!("Harness home: {}", summary.target_home.display());
    println!("Report file: {}", summary.report_file.display());
    println!("Iterations: {}", summary.iterations);
    println!("Completed: {}", summary.completed);
    println!("Idle: {}", summary.idle);
    println!("Errors: {}", summary.errors);
    println!("Consecutive errors: {}", summary.consecutive_errors);
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
        "schema": "openclaw-harness.runtime-loop.v1",
        "harnessHome": &summary.target_home,
        "reportFile": &summary.report_file,
        "startedAtMs": summary.started_at_ms,
        "finishedAtMs": summary.finished_at_ms,
        "iterations": summary.iterations,
        "completed": summary.completed,
        "idle": summary.idle,
        "errors": summary.errors,
        "consecutiveErrors": summary.consecutive_errors,
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
                "iterations={} completed={} idle={} errors={} stopReason={}",
                summary.iterations,
                summary.completed,
                summary.idle,
                summary.errors,
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
        RuntimeRunOnceStatus::NoWork => "no-work",
        RuntimeRunOnceStatus::NoPreparedExecution => "no-prepared-execution",
        RuntimeRunOnceStatus::NoRuntimePlan => "no-runtime-plan",
        RuntimeRunOnceStatus::PreflightBlocked => "preflight-blocked",
        RuntimeRunOnceStatus::SpawnFailed => "spawn-failed",
        RuntimeRunOnceStatus::ProtocolError => "protocol-error",
        RuntimeRunOnceStatus::Timeout => "timeout",
    }
}

fn print_codex_runtime_plan_report(report: &CodexRuntimePlanReport) {
    println!("OpenClaw Codex runtime plan");
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
    println!("OpenClaw Codex runtime preflight");
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
    println!("OpenClaw Codex runtime launch probe");
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
    println!("OpenClaw Codex runtime run");
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
    println!("OpenClaw Codex runtime completion");
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
    println!("OpenClaw prompt bundle");
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
    println!("OpenClaw native cron plan");
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
    println!("OpenClaw deterministic cron plan");
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
    println!("OpenClaw subagent plan");
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
    println!("openclaw-harness");
    println!();
    println!("Commands:");
    println!("  doctor          Inspect an OpenClaw home directory");
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
    println!(
        "  memory-credentials-export Export imported memory embedding config to harness secrets"
    );
    println!("  memory-search   Search imported markdown/text memory files read-only");
    println!("  memory-vector-search Search imported SQLite vector memory with embedding query");
    println!("  memory-canvas-run Build compact symbolic canvas from captured candidates/episodes");
    println!("  supervisor-plan Generate Windows scheduled-task scripts for harness loops");
    println!("  harness-skills-sync Sync bundled harness operation skills");
    println!("  skills          Build a skill-first index and optionally match a task");
    println!("  turn-plan       Plan routing, commands, prompts, and skills for one turn");
    println!("  channel-step    Plan shared channel reply or agent dispatch for one DM");
    println!("  channel-apply   Persist channel command state and command receipts");
    println!("  channel-receive Handle one DM into command outbox or runtime queue");
    println!("  channel-run-once Handle one DM, run runtime if needed, and plan delivery");
    println!("  channel-outbox-plan List pending Telegram/Discord delivery messages");
    println!("  channel-delivery-record Record delivery success or retryable failure");
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
    println!("  plugin-sidecar-probe Probe the Node OpenClaw plugin sidecar contract");
    println!("  plugin-sidecar-call Call the plugin sidecar JSON-RPC bridge once");
    println!("  queue-enqueue   Persist one channel agent turn to the runtime queue");
    println!("  queue-prepare   Prepare one queued runtime item for Codex execution");
    println!("  runtime-run-once Prepare, run, and outbox one queued runtime item");
    println!("  runtime-loop    Drain runtime queue until stopped, idle, or error threshold");
    println!("  codex-plan      Plan Codex app-server invocation for prepared execution");
    println!("  codex-preflight Check a Codex runtime plan before process start");
    println!("  codex-launch-probe Start and stop Codex app-server without a model request");
    println!("  codex-run       Run a prepared Codex app-server turn and record completion");
    println!("  codex-complete  Record assistant output to transcript and trajectory");
    println!("  prompt-bundle   Assemble prompt files, selected skills, and message");
    println!("  cron-plan       Dry-run OpenClaw native agent-turn cron dispatch");
    println!("  deterministic-cron-plan Dry-run deterministic cron without LLM access");
    println!("  subagent-plan   Dry-run subagent ledger cutover/resume planning");
    println!();
    println!("Options:");
    println!("  --openclaw-home <path>  Source .openclaw directory");
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
    println!("  --task-prefix <name>    Windows scheduled-task name prefix for supervisor-plan");
    println!("  --query <text>          Match skills or search imported memory");
    println!("  --agent <id>            Agent hint for skill matching");
    println!("  --telegram-account <id> Telegram account token/offset selector");
    println!("  --channel <name>        Channel hint for skill matching");
    println!("  --match-workspace <txt> Workspace hint for skill matching");
    println!("  --limit <n>             Maximum matched skills to print");
    println!("  --max-file-bytes <n>    Maximum imported memory file size for memory-search");
    println!("  --no-receipt            Do not write memory search/vector-search probe receipts");
    println!("  --message <text>        Incoming channel message for turn-plan");
    println!("  --platform <name>       local, telegram, discord, or cron");
    println!("  --channel-id <id>       Channel identity for session mapping");
    println!("  --user-id <id>          User identity for session mapping");
    println!("  --session-key <key>     Existing session key override");
    println!("  --delivery-id <id>      Channel outbox delivery id");
    println!("  --status <value>        Delivery status: delivered or failed");
    println!("  --provider-message-id <id> Telegram/Discord message id after delivery");
    println!("  --error <text>          Delivery failure reason");
    println!("  --outbox-limit <n>      Maximum pending outbox items for channel-run-once");
    println!("  --min-update-interval-ms <n> Minimum progress panel edit interval");
    println!("  --max-events <n>        Maximum action lines shown in a progress panel");
    println!("  --max-preview-chars <n> Maximum preview characters per progress action");
    println!("  --event-file <path>     Discord Gateway event JSON file");
    println!("  --event-json <text>     Discord Gateway event JSON text");
    println!("  --gateway-script <path> Discord Gateway Node script path");
    println!("  --harness-cli <path>    Harness CLI used by gateway loop callbacks");
    println!("  --no-runtime            Exclude runtime-loop from supervisor-plan");
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
    println!("  --idle-ms <n>           Loop sleep after each poll or runtime run");
    println!("  --max-consecutive-errors <n> Loop failure threshold");
    println!("  --stop-when-idle        Stop runtime-loop when the runtime queue is idle");
    println!("  --stop-file <path>      Stop-file path for runtime, Telegram, or Discord loops");
    println!("  TELEGRAM_BOT_TOKEN      Env var or harness secret used by Telegram adapters");
    println!("  OPENCLAW_TELEGRAM_ACCOUNT_<ID>_BOT_TOKEN Account-specific Telegram token");
    println!("  DISCORD_BOT_TOKEN       Env var or harness secret used by Discord adapters");
    println!("  --node-exe <path>       Node executable for plugin sidecar commands");
    println!("  --sidecar-script <path> Plugin sidecar script path");
    println!("  --method <name>         Plugin sidecar JSON-RPC method for plugin-sidecar-call");
    println!("  --params-json <object>  JSON-RPC params object for plugin-sidecar-call");
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
    println!("  --allow-deterministic-run Release deterministic cron hold in dry-run");
    println!("  --resume-subagents    Mark queued/running subagents as resume candidates");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn set(values: &[&str]) -> BTreeSet<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    fn progress_pending(
        platform: &str,
        channel_id: &str,
        user_id: &str,
    ) -> AgentProgressDeliveryPending {
        AgentProgressDeliveryPending {
            queue_id: "queue-1".to_string(),
            platform: platform.to_string(),
            channel_id: channel_id.to_string(),
            user_id: user_id.to_string(),
            session_key: "session-1".to_string(),
            message_kind: openclaw_harness_core::AgentProgressDeliveryMessageKind::Body,
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

    fn cli_temp_root(name: &str) -> PathBuf {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let path = std::env::temp_dir().join(format!(
            "openclaw-harness-cli-test-{name}-{}-{millis}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&path);
        path
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
    fn formats_channel_reply_with_short_plain_header() {
        assert_eq!(format_channel_reply_text("  done\n"), "◆ OpenClaw\n\ndone");
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

        assert!(
            progress_delivery_allowed(&policy, &progress_pending("telegram", "group-1", "admin-1"))
                .is_ok()
        );
        assert!(
            progress_delivery_allowed(&policy, &progress_pending("telegram", "group-1", "user-2"))
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
            Some("openclaw-harness.discord-reply-context-receipt.v1")
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
    fn harness_status_accepts_shared_activation_path_args() {
        let args = vec![
            "--openclaw-home".to_string(),
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
    fn typing_context_ignores_retryable_runtime_receipts() {
        let root = std::env::temp_dir().join(format!(
            "openclaw-harness-cli-typing-{}",
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

        let context = pending_runtime_typing_context(&root, Some("queue-1"))
            .unwrap()
            .unwrap();
        assert_eq!(context.platform, "telegram");
        assert_eq!(context.channel_id, "chat-1");

        fs::write(
            queue_dir.join("run-once-receipts.jsonl"),
            serde_json::json!({
                "queueId": "queue-1",
                "status": "completed"
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
