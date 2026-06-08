use std::env;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use openclaw_harness_core::{
    ActivationReadinessOptions, ActivationReadinessReport, AgentRegistry,
    BuiltinHarnessSkillSyncOptions, BuiltinHarnessSkillSyncReport, ChannelCommandApplyOptions,
    ChannelCommandApplyReport, ChannelDeliveryReceipt, ChannelDeliveryRecordOptions,
    ChannelDeliveryStatus, ChannelOutboxPlanOptions, ChannelOutboxPlanReport,
    ChannelReceiveOptions, ChannelReceiveReport, ChannelRunOnceOptions, ChannelRunOnceReport,
    ChannelStep, CodexRuntimeCompletionOptions, CodexRuntimeCompletionReport,
    CodexRuntimeLaunchProbeOptions, CodexRuntimeLaunchProbeReport, CodexRuntimePlanOptions,
    CodexRuntimePlanReport, CodexRuntimePreflightOptions, CodexRuntimePreflightReport,
    CodexRuntimeRunOptions, CodexRuntimeRunReport, ConflictPolicy, DeterministicCronPlan,
    DeterministicCronPlanInput, DryRunImportOptions, ExecuteImportOptions, HarnessLogEvent,
    HarnessLogLevel, ImportPhaseStatus, ImportReport, NativeCronPlan, NativeCronPlanInput,
    OpenClawSource, PromptAssemblyOptions, PromptBundle, RuntimeQueueEnqueueOptions,
    RuntimeQueueEnqueueReport, RuntimeQueuePrepareOptions, RuntimeQueuePrepareReport,
    RuntimeRunOnceOptions, RuntimeRunOnceReport, SkillIndex, SkillSelectionQuery, SubagentPlan,
    SubagentPlanInput, TurnPlan, TurnPlanInput, append_harness_log, apply_channel_command_step,
    assemble_prompt_bundle, build_channel_step, build_dry_run_report, build_harness_skill_index,
    build_import_plan, build_runtime_skill_index, build_source_skill_index, build_turn_plan,
    check_activation_readiness, current_log_time_ms, enqueue_channel_step, execute_import,
    export_harness_registry_files, inventory, load_agent_registry, load_deterministic_cron_store,
    load_native_cron_store, load_subagent_ledger, plan_channel_outbox, plan_codex_runtime,
    plan_deterministic_cron, plan_native_cron, plan_subagents, preflight_codex_runtime,
    prepare_runtime_queue_item, probe_codex_runtime_launch, receive_channel_message,
    record_channel_delivery, record_codex_runtime_completion, run_channel_once, run_codex_runtime,
    run_runtime_queue_once, select_skills, sync_builtin_harness_skills, write_channel_step,
    write_deterministic_cron_plan, write_native_cron_plan, write_prompt_bundle, write_report_files,
    write_skill_index, write_subagent_plan, write_turn_plan,
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
        "registry" => run_registry(&rest),
        "registry-export" => run_registry_export(&rest),
        "enable-check" => run_enable_check(&rest),
        "harness-skills-sync" => run_harness_skills_sync(&rest),
        "skills" => run_skills(&rest),
        "turn-plan" => run_turn_plan(&rest),
        "channel-step" => run_channel_step(&rest),
        "channel-apply" => run_channel_apply(&rest),
        "channel-receive" => run_channel_receive(&rest),
        "channel-run-once" => run_channel_run_once(&rest),
        "channel-outbox-plan" => run_channel_outbox_plan(&rest),
        "channel-delivery-record" => run_channel_delivery_record(&rest),
        "telegram-poll-once" => run_telegram_poll_once(&rest),
        "telegram-loop" => run_telegram_loop(&rest),
        "discord-outbox-send-once" => run_discord_outbox_send_once(&rest),
        "discord-event-run-once" => run_discord_event_run_once(&rest),
        "plugin-sidecar-probe" => run_plugin_sidecar_probe(&rest),
        "plugin-sidecar-call" => run_plugin_sidecar_call(&rest),
        "queue-enqueue" => run_queue_enqueue(&rest),
        "queue-prepare" => run_queue_prepare(&rest),
        "runtime-run-once" => run_runtime_run_once(&rest),
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
        harness_home: args.target_home,
        skill_index,
        platform: args.turn.platform,
        channel_id: args.turn.channel_id,
        user_id: args.turn.user_id,
        agent_id: args.turn.agent_id,
        session_key: args.turn.session_key,
        message: args.turn.message,
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
        harness_home: args.target_home.clone(),
        platform: args.turn.platform,
        channel_id: args.turn.channel_id,
        user_id: args.turn.user_id,
        agent_id: args.turn.agent_id,
        session_key: args.turn.session_key,
        message: args.turn.message,
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

fn run_telegram_poll_once(args: &[String]) -> Result<(), String> {
    let args = telegram_poll_once_args_from_args(args)?;
    let token = telegram_bot_token()?;
    let report = execute_telegram_poll_once(&args, &token)?;
    print_telegram_poll_once_report(&report);
    Ok(())
}

fn run_telegram_loop(args: &[String]) -> Result<(), String> {
    let args = telegram_loop_args_from_args(args)?;
    let token = telegram_bot_token()?;
    let mut iterations = 0usize;
    let mut consecutive_errors = 0usize;

    loop {
        iterations += 1;
        match execute_telegram_poll_once(&args.poll, &token) {
            Ok(report) => {
                consecutive_errors = 0;
                println!("Telegram loop iteration: {iterations}");
                print_telegram_poll_once_report(&report);
            }
            Err(error) => {
                consecutive_errors += 1;
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

fn execute_telegram_poll_once(
    args: &TelegramPollOnceArgs,
    token: &str,
) -> Result<TelegramPollOnceReport, String> {
    let mut warnings = Vec::new();
    let offset_file = telegram_offset_file(&args.target_home);
    let offset = read_telegram_offset(&offset_file, &mut warnings)?;
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
        let Some(text) = message.get("text").and_then(serde_json::Value::as_str) else {
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
        run_channel_once(ChannelRunOnceOptions {
            source: args.source.clone(),
            harness_home: args.target_home.clone(),
            platform: "telegram".to_string(),
            channel_id: chat_id,
            user_id,
            agent_id: args.agent_id.clone(),
            session_key: None,
            message: text.to_string(),
            skill_limit: args.skill_limit,
            now_ms: current_time_ms()?,
            codex_executable: args.codex_exe.clone(),
            timeout_ms: args.timeout_ms,
            prompt_options: PromptAssemblyOptions {
                harness_home: Some(args.target_home.clone()),
                ..PromptAssemblyOptions::default()
            },
            outbox_limit: args.outbox_limit,
        })
        .map_err(|err| err.to_string())?;
        handled_messages += 1;
        write_telegram_offset(&offset_file, next_offset)?;
    }

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
        match telegram_send_message(token, &pending.message.channel_id, &pending.message.text) {
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

fn run_discord_outbox_send_once(args: &[String]) -> Result<(), String> {
    let args = discord_outbox_send_once_args_from_args(args)?;
    let token = discord_bot_token()?;
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
        match discord_send_message(&token, &pending.message.channel_id, &pending.message.text) {
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
    print_discord_outbox_send_once_report(&report);
    Ok(())
}

fn run_discord_event_run_once(args: &[String]) -> Result<(), String> {
    let args = discord_event_run_once_args_from_args(args)?;
    let event = read_discord_event_json(&args)?;
    let parsed = parse_discord_gateway_message(&event)?;
    let report = match parsed {
        None => DiscordEventRunOnceReport {
            harness_home: args.target_home.clone(),
            status: "skipped".to_string(),
            reason: "event is not a Discord MESSAGE_CREATE payload".to_string(),
            message_id: None,
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
                channel_id: Some(message.channel_id),
                user_id: Some(message.user_id),
                run: None,
            };
            write_discord_event_receipt(&report)?;
            report
        }
        Some(message) if message.content.trim().is_empty() => {
            let report = DiscordEventRunOnceReport {
                harness_home: args.target_home.clone(),
                status: "skipped".to_string(),
                reason: "message content is empty".to_string(),
                message_id: Some(message.message_id),
                channel_id: Some(message.channel_id),
                user_id: Some(message.user_id),
                run: None,
            };
            write_discord_event_receipt(&report)?;
            report
        }
        Some(message) if discord_event_seen(&args.target_home, &message.message_id)? => {
            let report = DiscordEventRunOnceReport {
                harness_home: args.target_home.clone(),
                status: "duplicate".to_string(),
                reason: "message id already has a Discord event receipt".to_string(),
                message_id: Some(message.message_id),
                channel_id: Some(message.channel_id),
                user_id: Some(message.user_id),
                run: None,
            };
            write_discord_event_receipt(&report)?;
            report
        }
        Some(message) => {
            let run = run_channel_once(ChannelRunOnceOptions {
                source: args.source.clone(),
                harness_home: args.target_home.clone(),
                platform: "discord".to_string(),
                channel_id: message.channel_id.clone(),
                user_id: message.user_id.clone(),
                agent_id: args.agent_id.clone(),
                session_key: None,
                message: message.content,
                skill_limit: args.skill_limit,
                now_ms: current_time_ms()?,
                codex_executable: args.codex_exe.clone(),
                timeout_ms: args.timeout_ms,
                prompt_options: PromptAssemblyOptions {
                    harness_home: Some(args.target_home.clone()),
                    ..PromptAssemblyOptions::default()
                },
                outbox_limit: args.outbox_limit,
            })
            .map_err(|err| err.to_string())?;
            let report = DiscordEventRunOnceReport {
                harness_home: args.target_home.clone(),
                status: "handled".to_string(),
                reason: "Discord message normalized into channel-run-once".to_string(),
                message_id: Some(message.message_id),
                channel_id: Some(message.channel_id),
                user_id: Some(message.user_id),
                run: Some(run),
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

    println!("OpenClaw plugin sidecar probe");
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
        "params": {}
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

    println!("OpenClaw plugin sidecar call");
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

struct RegistryExportArgs {
    source: OpenClawSource,
    target_home: PathBuf,
    conflict_policy: ConflictPolicy,
}

struct EnableCheckArgs {
    target_home: PathBuf,
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

struct TelegramPollOnceArgs {
    source: OpenClawSource,
    target_home: PathBuf,
    agent_id: Option<String>,
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

struct TelegramLoopArgs {
    poll: TelegramPollOnceArgs,
    iterations: usize,
    idle_ms: u64,
    max_consecutive_errors: usize,
}

struct DiscordOutboxSendOnceArgs {
    target_home: PathBuf,
    outbox_limit: usize,
}

struct DiscordEventRunOnceArgs {
    source: OpenClawSource,
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
    channel_id: String,
    user_id: String,
    content: String,
    author_is_bot: bool,
}

struct DiscordEventRunOnceReport {
    harness_home: PathBuf,
    status: String,
    reason: String,
    message_id: Option<String>,
    channel_id: Option<String>,
    user_id: Option<String>,
    run: Option<ChannelRunOnceReport>,
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
}

struct DiscordOutboxSendOnceReport {
    pending_count: usize,
    delivered_messages: usize,
    failed_deliveries: usize,
    warnings: Vec<String>,
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

fn telegram_poll_once_args_from_args(args: &[String]) -> Result<TelegramPollOnceArgs, String> {
    let mut home = default_openclaw_home();
    let mut workspace = None;
    let mut target_home = default_harness_home();
    let mut agent_id = None;
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
        target_home,
        agent_id,
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
    let mut target_home = default_harness_home();
    let mut agent_id = None;
    let mut skill_limit = 5;
    let mut codex_exe = None;
    let mut timeout_ms = 300_000;
    let mut poll_timeout_seconds = 1;
    let mut max_updates = 10;
    let mut outbox_limit = 20;
    let mut iterations = 0;
    let mut idle_ms = 1_000;
    let mut max_consecutive_errors = 5;
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
            target_home,
            agent_id,
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

fn discord_event_run_once_args_from_args(
    args: &[String],
) -> Result<DiscordEventRunOnceArgs, String> {
    let mut home = default_openclaw_home();
    let mut workspace = None;
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
            flag => return Err(format!("unknown argument: {flag}")),
        }
        i += 1;
    }

    Ok(PluginSidecarCallArgs {
        target_home,
        node_exe,
        sidecar_script,
        method,
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

    Ok(Some(DiscordGatewayMessage {
        message_id: message_id.to_string(),
        channel_id: channel_id.to_string(),
        user_id: user_id.to_string(),
        content: content.to_string(),
        author_is_bot,
    }))
}

fn discord_event_receipts_file(harness_home: &std::path::Path) -> PathBuf {
    harness_home
        .join("state")
        .join("channels")
        .join("discord-event-receipts.jsonl")
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

fn telegram_bot_token() -> Result<String, String> {
    env::var("TELEGRAM_BOT_TOKEN")
        .map_err(|_| "TELEGRAM_BOT_TOKEN is required for Telegram adapters".to_string())
}

fn telegram_offset_file(harness_home: &std::path::Path) -> PathBuf {
    harness_home
        .join("state")
        .join("channels")
        .join("telegram-offset.json")
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
    let response = ureq::post(&url)
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
    let response = ureq::post(&url)
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
        ureq::Error::Transport(_) => "Telegram transport error".to_string(),
    }
}

fn discord_bot_token() -> Result<String, String> {
    env::var("DISCORD_BOT_TOKEN")
        .map_err(|_| "DISCORD_BOT_TOKEN is required for Discord adapters".to_string())
}

fn discord_send_message(
    token: &str,
    channel_id: &str,
    text: &str,
) -> Result<Option<String>, String> {
    if text.chars().count() > 2_000 {
        return Err("Discord message exceeds the 2000 character content limit".to_string());
    }
    let url = format!("https://discord.com/api/v10/channels/{channel_id}/messages");
    let auth = format!("Bot {token}");
    let response = ureq::post(&url)
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

fn discord_http_error(error: ureq::Error) -> String {
    match error {
        ureq::Error::Status(code, response) => {
            let body = response.into_string().unwrap_or_default();
            format!("Discord HTTP status {code}: {body}")
        }
        ureq::Error::Transport(_) => "Discord transport error".to_string(),
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

fn print_discord_event_run_once_report(report: &DiscordEventRunOnceReport) {
    println!("OpenClaw Discord event run once");
    println!("Harness home: {}", report.harness_home.display());
    println!("Status: {}", report.status);
    println!("Reason: {}", report.reason);
    println!("Message: {}", report.message_id.as_deref().unwrap_or("-"));
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
    println!("  registry        Inspect parsed multi-agent registry state");
    println!("  registry-export Write target harness registry state");
    println!("  enable-check    Check formal activation readiness and log writability");
    println!("  harness-skills-sync Sync bundled harness operation skills");
    println!("  skills          Build a skill-first index and optionally match a task");
    println!("  turn-plan       Plan routing, commands, prompts, and skills for one turn");
    println!("  channel-step    Plan shared channel reply or agent dispatch for one DM");
    println!("  channel-apply   Persist channel command state and command receipts");
    println!("  channel-receive Handle one DM into command outbox or runtime queue");
    println!("  channel-run-once Handle one DM, run runtime if needed, and plan delivery");
    println!("  channel-outbox-plan List pending Telegram/Discord delivery messages");
    println!("  channel-delivery-record Record delivery success or retryable failure");
    println!("  telegram-poll-once Poll Telegram once, run DM pipeline, and deliver replies");
    println!("  telegram-loop     Run Telegram polling continuously until stopped");
    println!("  discord-outbox-send-once Send pending Discord outbox messages once");
    println!("  discord-event-run-once Normalize one Discord Gateway message event");
    println!("  plugin-sidecar-probe Probe the Node OpenClaw plugin sidecar contract");
    println!("  plugin-sidecar-call Call the plugin sidecar JSON-RPC bridge once");
    println!("  queue-enqueue   Persist one channel agent turn to the runtime queue");
    println!("  queue-prepare   Prepare one queued runtime item for Codex execution");
    println!("  runtime-run-once Prepare, run, and outbox one queued runtime item");
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
    println!("  --target-home <path>    Destination harness home; alias of --harness-home");
    println!("  --harness-home <path>   Harness state root for runtime/channel commands");
    println!("  --force                 Overwrite user-modified builtin harness skills");
    println!("  --conflict <policy>     skip, overwrite, or rename");
    println!("  --output <path>         Write report.json and summary.md");
    println!("  --include-sensitive     Copy raw sensitive files during import-execute");
    println!("  --query <text>          Match skills for a task turn");
    println!("  --agent <id>            Agent hint for skill matching");
    println!("  --channel <name>        Channel hint for skill matching");
    println!("  --match-workspace <txt> Workspace hint for skill matching");
    println!("  --limit <n>             Maximum matched skills to print");
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
    println!("  --event-file <path>     Discord Gateway event JSON file");
    println!("  --event-json <text>     Discord Gateway event JSON text");
    println!("  --poll-timeout-seconds <n> Telegram long-poll timeout for telegram-poll-once");
    println!("  --max-updates <n>       Maximum Telegram updates for telegram-poll-once");
    println!("  --iterations <n>        Telegram loop iterations; 0 means forever");
    println!("  --idle-ms <n>           Telegram loop sleep after each poll");
    println!("  --max-consecutive-errors <n> Telegram loop failure threshold");
    println!("  TELEGRAM_BOT_TOKEN      Environment variable used by Telegram adapters");
    println!("  DISCORD_BOT_TOKEN       Environment variable used by Discord adapters");
    println!("  --node-exe <path>       Node executable for plugin sidecar commands");
    println!("  --sidecar-script <path> Plugin sidecar script path");
    println!("  --method <name>         Plugin sidecar JSON-RPC method for plugin-sidecar-call");
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
