use std::env;
use std::path::PathBuf;

use openclaw_harness_core::{ImportPhaseStatus, OpenClawSource, build_import_plan, inventory};

fn main() {
    let mut args = env::args().skip(1);
    let command = args.next().unwrap_or_else(|| "help".to_string());
    let rest: Vec<String> = args.collect();

    let result = match command.as_str() {
        "doctor" => run_doctor(&rest),
        "import-plan" => run_import_plan(&rest),
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

fn default_openclaw_home() -> PathBuf {
    if let Ok(value) = env::var("OPENCLAW_HOME") {
        return PathBuf::from(value);
    }

    if let Ok(value) = env::var("USERPROFILE") {
        return PathBuf::from(value).join(".openclaw");
    }

    PathBuf::from(".openclaw")
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

fn print_help() {
    println!("openclaw-harness");
    println!();
    println!("Commands:");
    println!("  doctor       Inspect an OpenClaw home directory");
    println!("  import-plan  Print staged import readiness");
    println!();
    println!("Options:");
    println!("  --openclaw-home <path>  Source .openclaw directory");
    println!("  --workspace <path>      Override workspace directory");
}
