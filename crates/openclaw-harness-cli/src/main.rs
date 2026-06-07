use std::env;
use std::path::PathBuf;

use openclaw_harness_core::{
    AgentRegistry, ConflictPolicy, DryRunImportOptions, ImportPhaseStatus, ImportReport,
    OpenClawSource, build_dry_run_report, build_import_plan, inventory, load_agent_registry,
    write_report_files,
};

fn main() {
    let mut args = env::args().skip(1);
    let command = args.next().unwrap_or_else(|| "help".to_string());
    let rest: Vec<String> = args.collect();

    let result = match command.as_str() {
        "doctor" => run_doctor(&rest),
        "import-plan" => run_import_plan(&rest),
        "import-dry-run" => run_import_dry_run(&rest),
        "registry" => run_registry(&rest),
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

fn run_registry(args: &[String]) -> Result<(), String> {
    let source = source_from_args(args)?;
    let registry = load_agent_registry(&source).map_err(|err| err.to_string())?;
    print_registry(&registry);
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

fn print_help() {
    println!("openclaw-harness");
    println!();
    println!("Commands:");
    println!("  doctor          Inspect an OpenClaw home directory");
    println!("  import-plan     Print staged import readiness");
    println!("  import-dry-run  Build a read-only migration report");
    println!("  registry        Inspect parsed multi-agent registry state");
    println!();
    println!("Options:");
    println!("  --openclaw-home <path>  Source .openclaw directory");
    println!("  --workspace <path>      Override workspace directory");
    println!("  --target-home <path>    Destination harness home for import-dry-run");
    println!("  --conflict <policy>     skip, overwrite, or rename");
    println!("  --output <path>         Write report.json and summary.md");
}
