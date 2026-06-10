use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::AgentSource;

const DETERMINISTIC_CRON_PLAN_SCHEMA: &str = "agent-harness.deterministic-cron-plan.v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum DeterministicCronRunnerKind {
    Main,
    Backup,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeterministicCronStore {
    pub source_workspace: PathBuf,
    pub summary: DeterministicCronStoreSummary,
    pub runners: Vec<DeterministicCronRunner>,
    pub entries: Vec<DeterministicCronEntry>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeterministicCronStoreSummary {
    pub runners_found: usize,
    pub crontab_files: usize,
    pub entries: usize,
    pub job_scripts: usize,
    pub state_files: usize,
    pub lock_files: usize,
    pub log_files: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeterministicCronRunner {
    pub kind: DeterministicCronRunnerKind,
    pub root: PathBuf,
    pub exists: bool,
    pub crontab_files: Vec<PathBuf>,
    pub job_scripts: Vec<PathBuf>,
    pub state_files: Vec<PathBuf>,
    pub lock_files: Vec<PathBuf>,
    pub log_files: Vec<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeterministicCronEntry {
    pub id: String,
    pub runner_kind: DeterministicCronRunnerKind,
    pub crontab_file: PathBuf,
    pub line_number: usize,
    pub schedule: DeterministicCronSchedule,
    pub command: String,
    pub script_path: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase", tag = "kind")]
pub enum DeterministicCronSchedule {
    Cron { expression: String },
    Macro { name: String },
    Unsupported { summary: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeterministicCronPlanInput {
    pub allow_deterministic_run: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeterministicCronPlan {
    pub schema: &'static str,
    pub source_workspace: PathBuf,
    pub allow_deterministic_run: bool,
    pub llm_access_allowed: bool,
    pub summary: DeterministicCronPlanSummary,
    pub entries: Vec<DeterministicCronPlanEntry>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeterministicCronPlanSummary {
    pub total_entries: usize,
    pub cutover_held: usize,
    pub ready_commands: usize,
    pub shell_compatibility_required: usize,
    pub missing_script: usize,
    pub external_command_review: usize,
    pub unsupported_entries: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeterministicCronPlanEntry {
    pub entry_id: String,
    pub runner_kind: DeterministicCronRunnerKind,
    pub crontab_file: PathBuf,
    pub line_number: usize,
    pub schedule: DeterministicCronSchedule,
    pub command: String,
    pub script_path: Option<PathBuf>,
    pub action: DeterministicCronPlanAction,
    pub reason: String,
    pub llm_access_allowed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum DeterministicCronPlanAction {
    CutoverHold,
    ReadyCommand,
    ShellCompatibilityRequired,
    MissingScript,
    ExternalCommandReview,
    UnsupportedEntry,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeterministicCronPlanFile {
    pub json: PathBuf,
}

pub fn load_deterministic_cron_store(source: &AgentSource) -> io::Result<DeterministicCronStore> {
    let mut warnings = Vec::new();
    let mut runners = Vec::new();
    for (kind, relative) in [
        (DeterministicCronRunnerKind::Main, "cron-runner"),
        (DeterministicCronRunnerKind::Backup, "backup-cron-runner"),
    ] {
        runners.push(load_runner(
            kind,
            source.workspace.join("tools").join(relative),
            &mut warnings,
        )?);
    }

    let mut entries = Vec::new();
    for runner in &runners {
        for crontab_file in &runner.crontab_files {
            entries.extend(parse_crontab_file(
                runner.kind,
                &runner.root,
                crontab_file,
                &mut warnings,
            )?);
        }
    }
    entries.sort_by(|left, right| left.id.cmp(&right.id));
    let summary = summarize_store(&runners, &entries);

    Ok(DeterministicCronStore {
        source_workspace: source.workspace.clone(),
        summary,
        runners,
        entries,
        warnings,
    })
}

pub fn plan_deterministic_cron(
    store: &DeterministicCronStore,
    input: DeterministicCronPlanInput,
) -> DeterministicCronPlan {
    let mut entries = Vec::new();
    let mut warnings = store.warnings.clone();
    if !input.allow_deterministic_run {
        warnings.push(
            "allow-deterministic-run is disabled; deterministic cron commands are held at cutover"
                .to_string(),
        );
    }

    for entry in &store.entries {
        entries.push(plan_entry(entry, input.allow_deterministic_run));
    }
    let summary = summarize_plan(&entries);

    DeterministicCronPlan {
        schema: DETERMINISTIC_CRON_PLAN_SCHEMA,
        source_workspace: store.source_workspace.clone(),
        allow_deterministic_run: input.allow_deterministic_run,
        llm_access_allowed: false,
        summary,
        entries,
        warnings,
    }
}

pub fn write_deterministic_cron_plan(
    plan: &DeterministicCronPlan,
    output_dir: impl AsRef<Path>,
) -> io::Result<DeterministicCronPlanFile> {
    let output_dir = output_dir.as_ref();
    fs::create_dir_all(output_dir)?;
    let json = output_dir.join("deterministic-cron-plan.json");
    let text = serde_json::to_string_pretty(plan).map_err(io::Error::other)?;
    fs::write(&json, text)?;
    Ok(DeterministicCronPlanFile { json })
}

fn load_runner(
    kind: DeterministicCronRunnerKind,
    root: PathBuf,
    warnings: &mut Vec<String>,
) -> io::Result<DeterministicCronRunner> {
    if !root.exists() {
        warnings.push(format!(
            "deterministic cron runner root not found at {}",
            root.display()
        ));
        return Ok(DeterministicCronRunner {
            kind,
            root,
            exists: false,
            crontab_files: Vec::new(),
            job_scripts: Vec::new(),
            state_files: Vec::new(),
            lock_files: Vec::new(),
            log_files: Vec::new(),
        });
    }

    let all_files = regular_files_under(&root)?;
    let crontab_files = all_files
        .iter()
        .filter(|path| is_crontab_file(&root, path))
        .cloned()
        .collect();
    let job_scripts = files_under_named_dir(&all_files, "jobs");
    let state_files = files_under_named_dir(&all_files, "state");
    let lock_files = files_under_named_dir(&all_files, "locks");
    let log_files = files_under_named_dir(&all_files, "logs");

    Ok(DeterministicCronRunner {
        kind,
        root,
        exists: true,
        crontab_files,
        job_scripts,
        state_files,
        lock_files,
        log_files,
    })
}

fn parse_crontab_file(
    runner_kind: DeterministicCronRunnerKind,
    runner_root: &Path,
    crontab_file: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<Vec<DeterministicCronEntry>> {
    let text = fs::read_to_string(crontab_file)?;
    let mut entries = Vec::new();
    for (index, line) in text.lines().enumerate() {
        let line_number = index + 1;
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') || is_env_line(trimmed) {
            continue;
        }
        match parse_crontab_line(trimmed, runner_kind, runner_root, crontab_file, line_number) {
            Some(entry) => entries.push(entry),
            None => warnings.push(format!(
                "unsupported deterministic cron line {}:{}",
                crontab_file.display(),
                line_number
            )),
        }
    }
    Ok(entries)
}

fn parse_crontab_line(
    line: &str,
    runner_kind: DeterministicCronRunnerKind,
    runner_root: &Path,
    crontab_file: &Path,
    line_number: usize,
) -> Option<DeterministicCronEntry> {
    let (schedule, command) = if line.starts_with('@') {
        let (name, command) = line.split_once(char::is_whitespace)?;
        (
            DeterministicCronSchedule::Macro {
                name: name.to_string(),
            },
            command.trim().to_string(),
        )
    } else {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 6 {
            return Some(entry_with_unsupported_schedule(
                runner_kind,
                crontab_file,
                line_number,
                line,
                "expected five cron fields and a command",
            ));
        }
        (
            DeterministicCronSchedule::Cron {
                expression: parts[..5].join(" "),
            },
            parts[5..].join(" "),
        )
    };
    let script_path = resolve_script_path(runner_root, &command);
    let id = format!(
        "{}:{}",
        runner_prefix(runner_kind),
        normalize_key_part(&format!("{}:{line_number}", crontab_file.display()))
    );
    Some(DeterministicCronEntry {
        id,
        runner_kind,
        crontab_file: crontab_file.to_path_buf(),
        line_number,
        schedule,
        command,
        script_path,
    })
}

fn entry_with_unsupported_schedule(
    runner_kind: DeterministicCronRunnerKind,
    crontab_file: &Path,
    line_number: usize,
    command: &str,
    summary: &str,
) -> DeterministicCronEntry {
    DeterministicCronEntry {
        id: format!(
            "{}:{}",
            runner_prefix(runner_kind),
            normalize_key_part(&format!("{}:{line_number}", crontab_file.display()))
        ),
        runner_kind,
        crontab_file: crontab_file.to_path_buf(),
        line_number,
        schedule: DeterministicCronSchedule::Unsupported {
            summary: summary.to_string(),
        },
        command: command.to_string(),
        script_path: None,
    }
}

fn plan_entry(
    entry: &DeterministicCronEntry,
    allow_deterministic_run: bool,
) -> DeterministicCronPlanEntry {
    let (action, reason) = if !allow_deterministic_run {
        (
            DeterministicCronPlanAction::CutoverHold,
            "allow-deterministic-run not enabled; command held at cutover".to_string(),
        )
    } else if matches!(
        entry.schedule,
        DeterministicCronSchedule::Unsupported { .. }
    ) {
        (
            DeterministicCronPlanAction::UnsupportedEntry,
            "unsupported crontab schedule shape".to_string(),
        )
    } else if let Some(script_path) = &entry.script_path {
        if !script_path.is_file() {
            (
                DeterministicCronPlanAction::MissingScript,
                format!("script not found at {}", script_path.display()),
            )
        } else if requires_shell_compatibility(script_path) {
            (
                DeterministicCronPlanAction::ShellCompatibilityRequired,
                "script is shell-based and needs WSL/Git Bash or a native port on Windows"
                    .to_string(),
            )
        } else {
            (
                DeterministicCronPlanAction::ReadyCommand,
                "deterministic command can be supervised without LLM access".to_string(),
            )
        }
    } else {
        (
            DeterministicCronPlanAction::ExternalCommandReview,
            "command does not reference a tracked jobs/ script and needs operator review"
                .to_string(),
        )
    };

    DeterministicCronPlanEntry {
        entry_id: entry.id.clone(),
        runner_kind: entry.runner_kind,
        crontab_file: entry.crontab_file.clone(),
        line_number: entry.line_number,
        schedule: entry.schedule.clone(),
        command: entry.command.clone(),
        script_path: entry.script_path.clone(),
        action,
        reason,
        llm_access_allowed: false,
    }
}

fn resolve_script_path(runner_root: &Path, command: &str) -> Option<PathBuf> {
    for token in command.split_whitespace() {
        let token = token.trim_matches('"').trim_matches('\'');
        let normalized = token.replace('\\', "/");
        if let Some(index) = normalized.find("jobs/") {
            let relative = &normalized[index..];
            return Some(runner_root.join(relative.replace('/', std::path::MAIN_SEPARATOR_STR)));
        }
    }
    None
}

fn is_crontab_file(root: &Path, path: &Path) -> bool {
    let name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("");
    if name.eq_ignore_ascii_case("crontab") || name.ends_with(".crontab") {
        return true;
    }
    path.strip_prefix(root).ok().is_some_and(|relative| {
        relative.components().any(|component| {
            component
                .as_os_str()
                .to_str()
                .is_some_and(|value| value.eq_ignore_ascii_case("crontab"))
        })
    })
}

fn files_under_named_dir(files: &[PathBuf], name: &str) -> Vec<PathBuf> {
    files
        .iter()
        .filter(|path| {
            path.components().any(|component| {
                component
                    .as_os_str()
                    .to_str()
                    .is_some_and(|value| value.eq_ignore_ascii_case(name))
            })
        })
        .cloned()
        .collect()
}

fn regular_files_under(root: &Path) -> io::Result<Vec<PathBuf>> {
    if !root.exists() {
        return Ok(Vec::new());
    }

    let mut files = Vec::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            files.extend(regular_files_under(&path)?);
        } else if file_type.is_file() {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

fn is_env_line(line: &str) -> bool {
    let Some((key, _)) = line.split_once('=') else {
        return false;
    };
    !key.is_empty()
        && key
            .chars()
            .all(|ch| ch.is_ascii_uppercase() || ch.is_ascii_digit() || ch == '_')
}

fn requires_shell_compatibility(path: &Path) -> bool {
    path.extension()
        .and_then(|value| value.to_str())
        .is_some_and(|extension| matches!(extension.to_ascii_lowercase().as_str(), "sh" | "bash"))
}

fn runner_prefix(kind: DeterministicCronRunnerKind) -> &'static str {
    match kind {
        DeterministicCronRunnerKind::Main => "main",
        DeterministicCronRunnerKind::Backup => "backup",
    }
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

fn summarize_store(
    runners: &[DeterministicCronRunner],
    entries: &[DeterministicCronEntry],
) -> DeterministicCronStoreSummary {
    DeterministicCronStoreSummary {
        runners_found: runners.iter().filter(|runner| runner.exists).count(),
        crontab_files: runners
            .iter()
            .map(|runner| runner.crontab_files.len())
            .sum(),
        entries: entries.len(),
        job_scripts: runners.iter().map(|runner| runner.job_scripts.len()).sum(),
        state_files: runners.iter().map(|runner| runner.state_files.len()).sum(),
        lock_files: runners.iter().map(|runner| runner.lock_files.len()).sum(),
        log_files: runners.iter().map(|runner| runner.log_files.len()).sum(),
    }
}

fn summarize_plan(entries: &[DeterministicCronPlanEntry]) -> DeterministicCronPlanSummary {
    let mut summary = DeterministicCronPlanSummary {
        total_entries: entries.len(),
        ..DeterministicCronPlanSummary::default()
    };
    for entry in entries {
        match entry.action {
            DeterministicCronPlanAction::CutoverHold => summary.cutover_held += 1,
            DeterministicCronPlanAction::ReadyCommand => summary.ready_commands += 1,
            DeterministicCronPlanAction::ShellCompatibilityRequired => {
                summary.shell_compatibility_required += 1;
            }
            DeterministicCronPlanAction::MissingScript => summary.missing_script += 1,
            DeterministicCronPlanAction::ExternalCommandReview => {
                summary.external_command_review += 1;
            }
            DeterministicCronPlanAction::UnsupportedEntry => summary.unsupported_entries += 1,
        }
    }
    summary
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn load_store_parses_runner_files_and_crontab_entries() {
        let root = temp_root("load_store_parses_runner_files_and_crontab_entries");
        let source = write_deterministic_source(&root);

        let store = load_deterministic_cron_store(&source).unwrap();

        assert_eq!(store.summary.runners_found, 2);
        assert_eq!(store.summary.crontab_files, 2);
        assert_eq!(store.summary.entries, 6);
        assert_eq!(store.summary.job_scripts, 4);
        assert_eq!(store.summary.state_files, 1);
        assert_eq!(store.summary.lock_files, 1);
        assert_eq!(store.summary.log_files, 1);
        assert!(
            store
                .entries
                .iter()
                .any(|entry| entry.command == "jobs/episodic_extract_1m.sh"
                    && matches!(
                        entry.schedule,
                        DeterministicCronSchedule::Cron { ref expression }
                            if expression == "* * * * *"
                    ))
        );
        assert!(
            store.entries.iter().any(|entry| {
                entry.command == "jobs/rotate.ps1"
                    && matches!(entry.schedule, DeterministicCronSchedule::Macro { ref name } if name == "@daily")
            })
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn plan_holds_everything_until_operator_allows_deterministic_run() {
        let root = temp_root("plan_holds_everything_until_operator_allows_deterministic_run");
        let source = write_deterministic_source(&root);
        let store = load_deterministic_cron_store(&source).unwrap();

        let plan = plan_deterministic_cron(
            &store,
            DeterministicCronPlanInput {
                allow_deterministic_run: false,
            },
        );

        assert!(!plan.llm_access_allowed);
        assert_eq!(plan.summary.total_entries, 6);
        assert_eq!(plan.summary.cutover_held, 6);
        assert_eq!(plan.summary.ready_commands, 0);
        assert!(
            plan.warnings
                .iter()
                .any(|warning| warning.contains("cutover"))
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn plan_classifies_deterministic_commands_without_llm_access() {
        let root = temp_root("plan_classifies_deterministic_commands_without_llm_access");
        let source = write_deterministic_source(&root);
        let store = load_deterministic_cron_store(&source).unwrap();

        let plan = plan_deterministic_cron(
            &store,
            DeterministicCronPlanInput {
                allow_deterministic_run: true,
            },
        );

        assert!(!plan.llm_access_allowed);
        assert_eq!(plan.summary.ready_commands, 2);
        assert_eq!(plan.summary.shell_compatibility_required, 1);
        assert_eq!(plan.summary.missing_script, 1);
        assert_eq!(plan.summary.external_command_review, 1);
        assert_eq!(plan.summary.unsupported_entries, 1);
        assert!(plan.entries.iter().all(|entry| !entry.llm_access_allowed));
        let shell = plan
            .entries
            .iter()
            .find(|entry| entry.command == "jobs/episodic_extract_1m.sh")
            .unwrap();
        assert_eq!(
            shell.action,
            DeterministicCronPlanAction::ShellCompatibilityRequired
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn write_plan_outputs_json() {
        let root = temp_root("write_plan_outputs_json");
        let source = write_deterministic_source(&root);
        let store = load_deterministic_cron_store(&source).unwrap();
        let plan = plan_deterministic_cron(
            &store,
            DeterministicCronPlanInput {
                allow_deterministic_run: true,
            },
        );

        let file = write_deterministic_cron_plan(&plan, root.join("out")).unwrap();

        assert!(file.json.is_file());
        let json: serde_json::Value =
            serde_json::from_slice(&fs::read(file.json).unwrap()).unwrap();
        assert_eq!(json["schema"], DETERMINISTIC_CRON_PLAN_SCHEMA);
        assert_eq!(json["llmAccessAllowed"], false);

        let _ = fs::remove_dir_all(root);
    }

    fn write_deterministic_source(root: &Path) -> AgentSource {
        let workspace = root.join("workspace");
        let main = workspace.join("tools").join("cron-runner");
        let backup = workspace.join("tools").join("backup-cron-runner");
        fs::create_dir_all(main.join("crontab")).unwrap();
        fs::create_dir_all(main.join("jobs")).unwrap();
        fs::create_dir_all(main.join("state")).unwrap();
        fs::create_dir_all(main.join("locks")).unwrap();
        fs::create_dir_all(main.join("logs")).unwrap();
        fs::create_dir_all(backup.join("crontab")).unwrap();
        fs::create_dir_all(backup.join("jobs")).unwrap();
        fs::write(
            main.join("crontab").join("openclaw-mem.crontab"),
            r#"
SHELL=/bin/sh
# ignored comment
* * * * * jobs/episodic_extract_1m.sh
*/5 * * * * python jobs/compact.py
@daily jobs/rotate.ps1
bad line
0 0 * * * curl https://example.invalid/health
"#,
        )
        .unwrap();
        fs::write(
            backup.join("crontab").join("backup.crontab"),
            "0 3 * * * jobs/missing.sh\n",
        )
        .unwrap();
        fs::write(
            main.join("jobs").join("episodic_extract_1m.sh"),
            "#!/bin/sh\n",
        )
        .unwrap();
        fs::write(main.join("jobs").join("compact.py"), "print('ok')\n").unwrap();
        fs::write(main.join("jobs").join("rotate.ps1"), "Write-Output ok\n").unwrap();
        fs::write(main.join("state").join("last.json"), "{}").unwrap();
        fs::write(main.join("locks").join("job.lock"), "").unwrap();
        fs::write(main.join("logs").join("supercronic.log"), "").unwrap();
        fs::write(backup.join("jobs").join("present.ps1"), "").unwrap();

        AgentSource::with_workspace(root.join(".openclaw"), workspace)
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-deterministic-cron-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
