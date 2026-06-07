use std::env;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use openclaw_harness_core::{
    AgentRegistry, ConflictPolicy, DeterministicCronPlan, DeterministicCronPlanInput,
    DryRunImportOptions, ExecuteImportOptions, ImportPhaseStatus, ImportReport, NativeCronPlan,
    NativeCronPlanInput, OpenClawSource, PromptAssemblyOptions, PromptBundle, SkillIndex,
    SkillSelectionQuery, SubagentPlan, SubagentPlanInput, TurnPlan, TurnPlanInput,
    assemble_prompt_bundle, build_dry_run_report, build_harness_skill_index, build_import_plan,
    build_source_skill_index, build_turn_plan, execute_import, export_harness_registry_files,
    inventory, load_agent_registry, load_deterministic_cron_store, load_native_cron_store,
    load_subagent_ledger, plan_deterministic_cron, plan_native_cron, plan_subagents, select_skills,
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
        "skills" => run_skills(&rest),
        "turn-plan" => run_turn_plan(&rest),
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
        Some(harness_home) => build_harness_skill_index(harness_home),
        None => build_source_skill_index(&args.source),
    }
    .map_err(|err| err.to_string())?;
    let plan = build_turn_plan(
        &args.source,
        &registry,
        &skill_index,
        TurnPlanInput {
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

fn run_prompt_bundle(args: &[String]) -> Result<(), String> {
    let args = turn_plan_args_from_args(args)?;
    let registry = load_agent_registry(&args.source).map_err(|err| err.to_string())?;
    let skill_index = match &args.harness_home {
        Some(harness_home) => build_harness_skill_index(harness_home),
        None => build_source_skill_index(&args.source),
    }
    .map_err(|err| err.to_string())?;
    let plan = build_turn_plan(
        &args.source,
        &registry,
        &skill_index,
        TurnPlanInput {
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
            "--target-home" => {
                i += 1;
                target_home = args
                    .get(i)
                    .map(PathBuf::from)
                    .ok_or_else(|| "--target-home requires a path".to_string())?;
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
            "--target-home" => {
                i += 1;
                target_home = args
                    .get(i)
                    .map(PathBuf::from)
                    .ok_or_else(|| "--target-home requires a path".to_string())?;
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
            "--target-home" => {
                i += 1;
                target_home = args
                    .get(i)
                    .map(PathBuf::from)
                    .ok_or_else(|| "--target-home requires a path".to_string())?;
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

fn parse_limit(value: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| format!("invalid limit: {value}; expected a positive integer"))
}

fn parse_i64(value: &str, flag: &str) -> Result<i64, String> {
    value
        .parse::<i64>()
        .map_err(|_| format!("{flag} requires a signed integer, got: {value}"))
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
        "Sections: {} prompt_files={} skills={} user_messages={}",
        bundle.sections.len(),
        bundle.summary.prompt_files_included,
        bundle.summary.skills_included,
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
    println!("  skills          Build a skill-first index and optionally match a task");
    println!("  turn-plan       Plan routing, commands, prompts, and skills for one turn");
    println!("  prompt-bundle   Assemble prompt files, selected skills, and message");
    println!("  cron-plan       Dry-run OpenClaw native agent-turn cron dispatch");
    println!("  deterministic-cron-plan Dry-run deterministic cron without LLM access");
    println!("  subagent-plan   Dry-run subagent ledger cutover/resume planning");
    println!();
    println!("Options:");
    println!("  --openclaw-home <path>  Source .openclaw directory");
    println!("  --workspace <path>      Override workspace directory");
    println!("  --target-home <path>    Destination harness home for import/export commands");
    println!("  --harness-home <path>   Existing harness home for imported skill indexing");
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
    println!("  --skill-limit <n>       Maximum selected skills for turn-plan");
    println!("  --max-prompt-file-bytes <n> Cap each prompt file in prompt-bundle");
    println!("  --max-skill-file-bytes <n>  Cap each skill file in prompt-bundle");
    println!("  --now-ms <n>           Epoch milliseconds for cron-plan");
    println!("  --resume-cron          Release native cron from cutover hold in dry-run");
    println!("  --allow-deterministic-run Release deterministic cron hold in dry-run");
    println!("  --resume-subagents    Mark queued/running subagents as resume candidates");
}
