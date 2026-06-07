use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::Value;

use crate::{OpenClawSource, PROMPT_FILE_NAMES, SKILL_FILE_NAME};

const REPORT_SCHEMA: &str = "openclaw-harness.import-report.v1";
const IMPORTED_SKILL_NAMESPACE: &str = "openclaw-imports";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ConflictPolicy {
    Skip,
    Overwrite,
    Rename,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ImportItemKind {
    Config,
    PromptFile,
    AgentDirectory,
    WorkspaceSkill,
    ManagedSkill,
    ProjectAgentSkill,
    NativeCronStore,
    DeterministicCronStore,
    SubagentStore,
    MemoryStore,
    PluginRecord,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ImportAction {
    CopyFile,
    CopyDirectory,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ImportItemStatus {
    Planned,
    AlreadyMatches,
    Missing,
    Conflict,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DryRunImportOptions {
    pub source: OpenClawSource,
    pub destination_home: PathBuf,
    pub conflict_policy: ConflictPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ImportReport {
    pub schema: &'static str,
    pub dry_run: bool,
    pub source_home: PathBuf,
    pub source_workspace: PathBuf,
    pub destination_home: PathBuf,
    pub conflict_policy: ConflictPolicy,
    pub summary: ImportReportSummary,
    pub semantics: ImportSemantics,
    pub items: Vec<ImportItem>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ImportReportSummary {
    pub total_items: usize,
    pub planned: usize,
    pub already_matches: usize,
    pub missing: usize,
    pub conflicts: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ImportSemantics {
    pub config: ConfigSemantics,
    pub sessions: SessionSemantics,
    pub native_cron: NativeCronSemantics,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigSemantics {
    pub parsed: bool,
    pub parse_error: Option<String>,
    pub agent_count: usize,
    pub provider_count: usize,
    pub plugin_count: usize,
    pub telegram_configured: bool,
    pub discord_configured: bool,
    pub provider_ids: Vec<String>,
    pub plugin_ids: Vec<String>,
    pub memory_plugins: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionSemantics {
    pub parsed_indexes: usize,
    pub failed_indexes: usize,
    pub total_records: usize,
    pub records_by_agent: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeCronSemantics {
    pub parsed_jobs_file: bool,
    pub jobs_parse_error: Option<String>,
    pub parsed_state_file: bool,
    pub state_parse_error: Option<String>,
    pub total_jobs: usize,
    pub enabled_jobs: usize,
    pub disabled_jobs: usize,
    pub state_entries: usize,
    pub jobs_by_agent: BTreeMap<String, usize>,
    pub schedule_types: BTreeMap<String, usize>,
    pub wake_modes: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ImportItem {
    pub id: usize,
    pub kind: ImportItemKind,
    pub action: ImportAction,
    pub source: PathBuf,
    pub destination: PathBuf,
    pub status: ImportItemStatus,
    pub reason: String,
    pub sensitive: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReportFiles {
    pub json: PathBuf,
    pub summary: PathBuf,
}

pub fn build_dry_run_report(options: DryRunImportOptions) -> io::Result<ImportReport> {
    let mut builder = ImportReportBuilder::new(options);
    builder.add_file(
        ImportItemKind::Config,
        builder.options.source.home.join("openclaw.json"),
        builder.options.destination_home.join("openclaw.json"),
        true,
    )?;

    for prompt_name in PROMPT_FILE_NAMES {
        builder.add_file(
            ImportItemKind::PromptFile,
            builder.options.source.workspace.join(prompt_name),
            builder
                .options
                .destination_home
                .join("workspace")
                .join(prompt_name),
            false,
        )?;
    }

    builder.add_skill_directories(
        ImportItemKind::WorkspaceSkill,
        builder.options.source.workspace.join("skills"),
        builder
            .options
            .destination_home
            .join("skills")
            .join(IMPORTED_SKILL_NAMESPACE)
            .join("workspace"),
    )?;
    builder.add_skill_directories(
        ImportItemKind::ManagedSkill,
        builder.options.source.home.join("skills"),
        builder
            .options
            .destination_home
            .join("skills")
            .join(IMPORTED_SKILL_NAMESPACE)
            .join("managed"),
    )?;
    builder.add_skill_directories(
        ImportItemKind::ProjectAgentSkill,
        builder
            .options
            .source
            .workspace
            .join(".agents")
            .join("skills"),
        builder
            .options
            .destination_home
            .join("skills")
            .join(IMPORTED_SKILL_NAMESPACE)
            .join("project-agents"),
    )?;

    builder.add_child_directories(
        ImportItemKind::AgentDirectory,
        builder.options.source.home.join("agents"),
        builder.options.destination_home.join("agents"),
        true,
    )?;
    builder.add_directory(
        ImportItemKind::NativeCronStore,
        builder.options.source.home.join("cron"),
        builder.options.destination_home.join("cron"),
        false,
    )?;
    builder.add_directory(
        ImportItemKind::DeterministicCronStore,
        builder
            .options
            .source
            .workspace
            .join("tools")
            .join("cron-runner"),
        builder
            .options
            .destination_home
            .join("workspace")
            .join("tools")
            .join("cron-runner"),
        false,
    )?;
    builder.add_directory(
        ImportItemKind::DeterministicCronStore,
        builder
            .options
            .source
            .workspace
            .join("tools")
            .join("backup-cron-runner"),
        builder
            .options
            .destination_home
            .join("workspace")
            .join("tools")
            .join("backup-cron-runner"),
        false,
    )?;
    builder.add_directory(
        ImportItemKind::SubagentStore,
        builder.options.source.home.join("subagents"),
        builder.options.destination_home.join("subagents"),
        false,
    )?;
    builder.add_directory(
        ImportItemKind::MemoryStore,
        builder.options.source.home.join("memory"),
        builder.options.destination_home.join("memory"),
        false,
    )?;
    builder.add_file(
        ImportItemKind::PluginRecord,
        builder
            .options
            .source
            .home
            .join("plugins")
            .join("installs.json"),
        builder
            .options
            .destination_home
            .join("plugins")
            .join("installs.json"),
        false,
    )?;
    builder.add_directory(
        ImportItemKind::PluginRecord,
        builder.options.source.home.join("plugin-state"),
        builder.options.destination_home.join("plugin-state"),
        true,
    )?;

    Ok(builder.finish())
}

pub fn write_report_files(
    report: &ImportReport,
    output_dir: impl AsRef<Path>,
) -> io::Result<ReportFiles> {
    let output_dir = output_dir.as_ref();
    fs::create_dir_all(output_dir)?;

    let json = output_dir.join("report.json");
    let summary = output_dir.join("summary.md");

    let json_text = serde_json::to_string_pretty(report).map_err(io::Error::other)?;
    fs::write(&json, json_text)?;
    fs::write(&summary, render_summary_markdown(report))?;

    Ok(ReportFiles { json, summary })
}

struct ImportReportBuilder {
    options: DryRunImportOptions,
    items: Vec<ImportItem>,
}

impl ImportReportBuilder {
    fn new(options: DryRunImportOptions) -> Self {
        Self {
            options,
            items: Vec::new(),
        }
    }

    fn add_file(
        &mut self,
        kind: ImportItemKind,
        source: PathBuf,
        destination: PathBuf,
        sensitive: bool,
    ) -> io::Result<()> {
        self.add_path(kind, ImportAction::CopyFile, source, destination, sensitive)
    }

    fn add_directory(
        &mut self,
        kind: ImportItemKind,
        source: PathBuf,
        destination: PathBuf,
        sensitive: bool,
    ) -> io::Result<()> {
        self.add_path(
            kind,
            ImportAction::CopyDirectory,
            source,
            destination,
            sensitive,
        )
    }

    fn add_child_directories(
        &mut self,
        kind: ImportItemKind,
        source_root: PathBuf,
        destination_root: PathBuf,
        sensitive: bool,
    ) -> io::Result<()> {
        for source in child_directories(&source_root)? {
            let Some(name) = source.file_name() else {
                continue;
            };
            self.add_directory(kind, source.clone(), destination_root.join(name), sensitive)?;
        }
        Ok(())
    }

    fn add_skill_directories(
        &mut self,
        kind: ImportItemKind,
        source_root: PathBuf,
        destination_root: PathBuf,
    ) -> io::Result<()> {
        for source in skill_directories(&source_root)? {
            let Some(name) = source.file_name() else {
                continue;
            };
            self.add_directory(kind, source.clone(), destination_root.join(name), false)?;
        }
        Ok(())
    }

    fn add_path(
        &mut self,
        kind: ImportItemKind,
        action: ImportAction,
        source: PathBuf,
        destination: PathBuf,
        sensitive: bool,
    ) -> io::Result<()> {
        if !source.exists() {
            return Ok(());
        }

        let (destination, status, reason) =
            resolve_destination(&source, &destination, action, self.options.conflict_policy)?;
        self.items.push(ImportItem {
            id: self.items.len() + 1,
            kind,
            action,
            source,
            destination,
            status,
            reason,
            sensitive,
        });
        Ok(())
    }

    fn finish(self) -> ImportReport {
        let summary = summarize(&self.items);
        let semantics = build_semantics(&self.options.source);
        ImportReport {
            schema: REPORT_SCHEMA,
            dry_run: true,
            source_home: self.options.source.home,
            source_workspace: self.options.source.workspace,
            destination_home: self.options.destination_home,
            conflict_policy: self.options.conflict_policy,
            summary,
            semantics,
            items: self.items,
        }
    }
}

fn build_semantics(source: &OpenClawSource) -> ImportSemantics {
    let mut semantics = ImportSemantics::default();
    read_config_semantics(source, &mut semantics);
    read_session_semantics(source, &mut semantics);
    read_native_cron_semantics(source, &mut semantics);
    semantics
}

fn read_config_semantics(source: &OpenClawSource, semantics: &mut ImportSemantics) {
    let path = source.home.join("openclaw.json");
    if !path.exists() {
        return;
    }

    let Some(value) = read_json(&path, &mut semantics.config.parse_error) else {
        return;
    };
    semantics.config.parsed = true;
    semantics.config.agent_count = count_agents(&value);

    let provider_ids = collect_provider_ids(&value);
    semantics.config.provider_count = provider_ids.len();
    semantics.config.provider_ids = provider_ids;

    let plugin_ids = collect_plugin_ids(&value);
    semantics.config.plugin_count = plugin_ids.len();
    semantics.config.telegram_configured =
        contains_key_recursive(&value, "telegram") || contains_id(&plugin_ids, "telegram");
    semantics.config.discord_configured =
        contains_key_recursive(&value, "discord") || contains_id(&plugin_ids, "discord");
    semantics.config.memory_plugins = plugin_ids
        .iter()
        .filter(|id| {
            let lower = id.to_ascii_lowercase();
            lower.contains("openclaw-mem") || lower.contains("mem-engine")
        })
        .cloned()
        .collect();
    semantics.config.plugin_ids = plugin_ids;
}

fn read_session_semantics(source: &OpenClawSource, semantics: &mut ImportSemantics) {
    let agents_root = source.home.join("agents");
    let session_indexes = match find_named_files(&agents_root, "sessions.json") {
        Ok(paths) => paths,
        Err(error) => {
            semantics.warnings.push(format!(
                "failed to scan session indexes under {}: {error}",
                agents_root.display()
            ));
            return;
        }
    };

    for path in session_indexes {
        let mut parse_error = None;
        let Some(value) = read_json(&path, &mut parse_error) else {
            semantics.sessions.failed_indexes += 1;
            if let Some(error) = parse_error {
                semantics
                    .warnings
                    .push(format!("failed to parse {}: {error}", path.display()));
            }
            continue;
        };

        let count = count_named_records(&value, "sessions");
        let agent_id = agent_id_for_path(source, &path).unwrap_or_else(|| "unknown".to_string());
        semantics.sessions.parsed_indexes += 1;
        semantics.sessions.total_records += count;
        increment(&mut semantics.sessions.records_by_agent, agent_id, count);
    }
}

fn read_native_cron_semantics(source: &OpenClawSource, semantics: &mut ImportSemantics) {
    let jobs_path = source.home.join("cron").join("jobs.json");
    if jobs_path.exists()
        && let Some(value) = read_json(&jobs_path, &mut semantics.native_cron.jobs_parse_error)
    {
        semantics.native_cron.parsed_jobs_file = true;
        for job in records_for_key(&value, "jobs") {
            read_cron_job(job, &mut semantics.native_cron);
        }
    }

    let state_path = source.home.join("cron").join("jobs-state.json");
    if state_path.exists()
        && let Some(value) = read_json(&state_path, &mut semantics.native_cron.state_parse_error)
    {
        semantics.native_cron.parsed_state_file = true;
        semantics.native_cron.state_entries = count_named_records(&value, "jobs");
    }
}

fn read_cron_job(job: &Value, cron: &mut NativeCronSemantics) {
    cron.total_jobs += 1;
    if job.get("enabled").and_then(Value::as_bool) == Some(false) {
        cron.disabled_jobs += 1;
    } else {
        cron.enabled_jobs += 1;
    }

    let agent_id = string_field(job, &["agentId", "agent_id"])
        .unwrap_or("unassigned")
        .to_string();
    increment(&mut cron.jobs_by_agent, agent_id, 1);

    increment(&mut cron.schedule_types, schedule_type(job), 1);

    if let Some(wake_mode) = string_field(job, &["wakeMode", "wake_mode"]) {
        increment(&mut cron.wake_modes, wake_mode.to_string(), 1);
    }
}

fn read_json(path: &Path, parse_error: &mut Option<String>) -> Option<Value> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) => {
            *parse_error = Some(format!("read failed: {error}"));
            return None;
        }
    };

    match serde_json::from_str(&text) {
        Ok(value) => Some(value),
        Err(error) => {
            *parse_error = Some(error.to_string());
            None
        }
    }
}

fn count_agents(value: &Value) -> usize {
    for path in ["/agents/list", "/agents/items", "/agent/list"] {
        if let Some(value) = value.pointer(path) {
            return count_collection(value);
        }
    }

    if let Some(agents) = value.get("agents") {
        if let Some(object) = agents.as_object()
            && object.contains_key("defaults")
        {
            return object
                .keys()
                .filter(|key| key.as_str() != "defaults")
                .count();
        }
        return count_collection(agents);
    }

    0
}

fn collect_provider_ids(value: &Value) -> Vec<String> {
    let mut ids = BTreeMap::new();
    for path in [
        "/models/providers",
        "/models/customProviders",
        "/models/custom_providers",
        "/providers",
        "/modelProviders",
    ] {
        collect_keys_at_path(value, path, &mut ids);
    }
    ids.into_keys().collect()
}

fn collect_plugin_ids(value: &Value) -> Vec<String> {
    let mut ids = BTreeMap::new();
    for path in ["/plugins", "/extensions", "/pluginSlots", "/plugin_slots"] {
        if let Some(value) = value.pointer(path) {
            collect_collection_ids(value, &mut ids);
        }
    }
    ids.into_keys().collect()
}

fn collect_keys_at_path(value: &Value, path: &str, ids: &mut BTreeMap<String, ()>) {
    let Some(value) = value.pointer(path) else {
        return;
    };
    if let Some(object) = value.as_object() {
        for key in object.keys() {
            ids.insert(key.clone(), ());
        }
    } else {
        collect_collection_ids(value, ids);
    }
}

fn collect_collection_ids(value: &Value, ids: &mut BTreeMap<String, ()>) {
    if let Some(object) = value.as_object() {
        for (key, value) in object {
            if let Some(id) = string_field(value, &["id", "name", "package", "plugin"]) {
                ids.insert(id.to_string(), ());
            } else {
                ids.insert(key.clone(), ());
            }
        }
    } else if let Some(array) = value.as_array() {
        for value in array {
            if let Some(id) = string_field(value, &["id", "name", "package", "plugin"]) {
                ids.insert(id.to_string(), ());
            }
        }
    }
}

fn contains_key_recursive(value: &Value, needle: &str) -> bool {
    match value {
        Value::Object(object) => object.iter().any(|(key, value)| {
            key.eq_ignore_ascii_case(needle) || contains_key_recursive(value, needle)
        }),
        Value::Array(array) => array
            .iter()
            .any(|value| contains_key_recursive(value, needle)),
        _ => false,
    }
}

fn contains_id(ids: &[String], needle: &str) -> bool {
    ids.iter().any(|id| id.eq_ignore_ascii_case(needle))
}

fn records_for_key<'a>(value: &'a Value, key: &str) -> Vec<&'a Value> {
    if let Some(value) = value.get(key) {
        return collection_values(value);
    }
    collection_values(value)
}

fn collection_values(value: &Value) -> Vec<&Value> {
    if let Some(array) = value.as_array() {
        array.iter().collect()
    } else if let Some(object) = value.as_object() {
        object.values().collect()
    } else {
        Vec::new()
    }
}

fn count_named_records(value: &Value, key: &str) -> usize {
    if let Some(value) = value.get(key) {
        return count_collection(value);
    }
    count_collection(value)
}

fn count_collection(value: &Value) -> usize {
    if let Some(array) = value.as_array() {
        array.len()
    } else if let Some(object) = value.as_object() {
        object.len()
    } else {
        0
    }
}

fn string_field<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a str> {
    for key in keys {
        if let Some(text) = value.get(*key).and_then(Value::as_str) {
            return Some(text);
        }
    }
    None
}

fn schedule_type(job: &Value) -> String {
    let Some(schedule) = job.get("schedule") else {
        return "missing".to_string();
    };

    if let Some(kind) = string_field(schedule, &["type", "kind"]) {
        return kind.to_ascii_lowercase();
    }

    if schedule.get("cron").is_some() || schedule.get("expression").is_some() {
        return "cron".to_string();
    }

    if schedule.get("at").is_some() || schedule.get("time").is_some() {
        return "at".to_string();
    }

    if let Some(text) = schedule.as_str() {
        if text.split_whitespace().count() >= 5 {
            return "cron".to_string();
        }
        return "string".to_string();
    }

    "unknown".to_string()
}

fn agent_id_for_path(source: &OpenClawSource, path: &Path) -> Option<String> {
    let agents_root = source.home.join("agents");
    let relative = path.strip_prefix(agents_root).ok()?;
    let component = relative.components().next()?;
    let value = component.as_os_str().to_str()?;
    Some(value.to_string())
}

fn increment(map: &mut BTreeMap<String, usize>, key: String, amount: usize) {
    *map.entry(key).or_default() += amount;
}

fn resolve_destination(
    source: &Path,
    destination: &Path,
    action: ImportAction,
    conflict_policy: ConflictPolicy,
) -> io::Result<(PathBuf, ImportItemStatus, String)> {
    if !destination.exists() {
        return Ok((
            destination.to_path_buf(),
            ImportItemStatus::Planned,
            "destination available".to_string(),
        ));
    }

    if action == ImportAction::CopyFile && files_match(source, destination)? {
        return Ok((
            destination.to_path_buf(),
            ImportItemStatus::AlreadyMatches,
            "destination file already matches source".to_string(),
        ));
    }

    match conflict_policy {
        ConflictPolicy::Skip => Ok((
            destination.to_path_buf(),
            ImportItemStatus::Conflict,
            "destination exists; choose overwrite or rename to import this item".to_string(),
        )),
        ConflictPolicy::Overwrite => Ok((
            destination.to_path_buf(),
            ImportItemStatus::Planned,
            "destination exists; would back it up before overwrite".to_string(),
        )),
        ConflictPolicy::Rename => Ok((
            available_renamed_path(destination),
            ImportItemStatus::Planned,
            "destination exists; would copy to renamed destination".to_string(),
        )),
    }
}

fn files_match(source: &Path, destination: &Path) -> io::Result<bool> {
    if !source.is_file() || !destination.is_file() {
        return Ok(false);
    }
    Ok(fs::read(source)? == fs::read(destination)?)
}

fn available_renamed_path(destination: &Path) -> PathBuf {
    let mut index = 1;
    loop {
        let suffix = if index == 1 {
            "-imported".to_string()
        } else {
            format!("-imported-{index}")
        };
        let candidate = destination_with_suffix(destination, &suffix);
        if !candidate.exists() {
            return candidate;
        }
        index += 1;
    }
}

fn destination_with_suffix(destination: &Path, suffix: &str) -> PathBuf {
    let Some(file_name) = destination.file_name().and_then(|value| value.to_str()) else {
        return destination.with_file_name(format!("imported{suffix}"));
    };

    if let Some((stem, extension)) = file_name.rsplit_once('.') {
        destination.with_file_name(format!("{stem}{suffix}.{extension}"))
    } else {
        destination.with_file_name(format!("{file_name}{suffix}"))
    }
}

fn child_directories(root: &Path) -> io::Result<Vec<PathBuf>> {
    if !root.exists() {
        return Ok(Vec::new());
    }

    let mut dirs = Vec::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            dirs.push(entry.path());
        }
    }
    dirs.sort();
    Ok(dirs)
}

fn find_named_files(root: &Path, name: &str) -> io::Result<Vec<PathBuf>> {
    let mut matches = Vec::new();
    visit_files(root, &mut |path| {
        if path.file_name().and_then(|value| value.to_str()) == Some(name) {
            matches.push(path.to_path_buf());
        }
    })?;
    Ok(matches)
}

fn visit_files(root: &Path, on_file: &mut impl FnMut(&Path)) -> io::Result<()> {
    if !root.exists() {
        return Ok(());
    }

    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            visit_files(&path, on_file)?;
        } else if file_type.is_file() {
            on_file(&path);
        }
    }

    Ok(())
}

fn skill_directories(root: &Path) -> io::Result<Vec<PathBuf>> {
    let dirs = child_directories(root)?
        .into_iter()
        .filter(|path| path.join(SKILL_FILE_NAME).is_file())
        .collect();
    Ok(dirs)
}

fn summarize(items: &[ImportItem]) -> ImportReportSummary {
    let mut summary = ImportReportSummary {
        total_items: items.len(),
        ..ImportReportSummary::default()
    };

    for item in items {
        match item.status {
            ImportItemStatus::Planned => summary.planned += 1,
            ImportItemStatus::AlreadyMatches => summary.already_matches += 1,
            ImportItemStatus::Missing => summary.missing += 1,
            ImportItemStatus::Conflict => summary.conflicts += 1,
        }
    }
    summary
}

fn render_summary_markdown(report: &ImportReport) -> String {
    let mut out = String::new();
    out.push_str("# OpenClaw Import Dry Run\n\n");
    out.push_str(&format!(
        "- Source home: `{}`\n",
        report.source_home.display()
    ));
    out.push_str(&format!(
        "- Source workspace: `{}`\n",
        report.source_workspace.display()
    ));
    out.push_str(&format!(
        "- Destination home: `{}`\n",
        report.destination_home.display()
    ));
    out.push_str(&format!(
        "- Conflict policy: `{:?}`\n",
        report.conflict_policy
    ));
    out.push_str(&format!(
        "- Total items: `{}`\n",
        report.summary.total_items
    ));
    out.push_str(&format!("- Planned: `{}`\n", report.summary.planned));
    out.push_str(&format!(
        "- Already matches: `{}`\n",
        report.summary.already_matches
    ));
    out.push_str(&format!("- Conflicts: `{}`\n", report.summary.conflicts));
    out.push_str(&format!("- Missing: `{}`\n\n", report.summary.missing));

    out.push_str("## Semantic Summary\n\n");
    out.push_str(&format!(
        "- Config parsed: `{}`\n",
        report.semantics.config.parsed
    ));
    out.push_str(&format!(
        "- Configured agents: `{}`\n",
        report.semantics.config.agent_count
    ));
    out.push_str(&format!(
        "- Providers: `{}`\n",
        report.semantics.config.provider_count
    ));
    out.push_str(&format!(
        "- Plugins: `{}`\n",
        report.semantics.config.plugin_count
    ));
    out.push_str(&format!(
        "- Telegram configured: `{}`\n",
        report.semantics.config.telegram_configured
    ));
    out.push_str(&format!(
        "- Discord configured: `{}`\n",
        report.semantics.config.discord_configured
    ));
    out.push_str(&format!(
        "- Session records: `{}`\n",
        report.semantics.sessions.total_records
    ));
    out.push_str(&format!(
        "- Native cron jobs: `{}`\n",
        report.semantics.native_cron.total_jobs
    ));
    out.push_str(&format!(
        "- Native cron enabled jobs: `{}`\n\n",
        report.semantics.native_cron.enabled_jobs
    ));

    out.push_str("| Status | Kind | Source | Destination | Reason |\n");
    out.push_str("| --- | --- | --- | --- | --- |\n");
    for item in &report.items {
        out.push_str(&format!(
            "| {:?} | {:?} | `{}` | `{}` | {} |\n",
            item.status,
            item.kind,
            escape_table_path(&item.source),
            escape_table_path(&item.destination),
            escape_table_text(&item.reason)
        ));
    }
    out
}

fn escape_table_path(path: &Path) -> String {
    escape_table_text(&path.display().to_string())
}

fn escape_table_text(value: &str) -> String {
    value.replace('|', "\\|")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn dry_run_report_plans_core_openclaw_paths() {
        let root = temp_root("dry_run_report_plans_core_openclaw_paths");
        let source_home = root.join(".openclaw");
        let workspace = source_home.join("workspace");
        let destination_home = root.join("harness-home");

        fs::create_dir_all(workspace.join("skills").join("triage")).unwrap();
        fs::create_dir_all(source_home.join("skills").join("memory")).unwrap();
        fs::create_dir_all(workspace.join(".agents").join("skills").join("handoff")).unwrap();
        fs::create_dir_all(source_home.join("agents").join("main").join("sessions")).unwrap();
        fs::create_dir_all(source_home.join("cron")).unwrap();
        fs::create_dir_all(workspace.join("tools").join("cron-runner")).unwrap();
        fs::create_dir_all(source_home.join("memory")).unwrap();
        fs::create_dir_all(source_home.join("plugins")).unwrap();

        fs::write(source_home.join("openclaw.json"), "{}").unwrap();
        fs::write(workspace.join("AGENTS.md"), "# Agent").unwrap();
        fs::write(
            workspace
                .join("skills")
                .join("triage")
                .join(SKILL_FILE_NAME),
            "# Triage",
        )
        .unwrap();
        fs::write(
            source_home
                .join("skills")
                .join("memory")
                .join(SKILL_FILE_NAME),
            "# Memory",
        )
        .unwrap();
        fs::write(
            workspace
                .join(".agents")
                .join("skills")
                .join("handoff")
                .join(SKILL_FILE_NAME),
            "# Handoff",
        )
        .unwrap();
        fs::write(
            source_home
                .join("agents")
                .join("main")
                .join("sessions")
                .join("sessions.json"),
            "{}",
        )
        .unwrap();
        fs::write(source_home.join("cron").join("jobs.json"), "{\"jobs\":[]}").unwrap();
        fs::write(source_home.join("memory").join("MEMORY.md"), "# Memory").unwrap();
        fs::write(source_home.join("plugins").join("installs.json"), "{}").unwrap();

        let report = build_dry_run_report(DryRunImportOptions {
            source: OpenClawSource::new(&source_home),
            destination_home,
            conflict_policy: ConflictPolicy::Skip,
        })
        .unwrap();

        assert_eq!(report.summary.conflicts, 0);
        assert!(report.summary.planned >= 8);
        assert!(has_kind(&report, ImportItemKind::Config));
        assert!(has_kind(&report, ImportItemKind::PromptFile));
        assert!(has_kind(&report, ImportItemKind::WorkspaceSkill));
        assert!(has_kind(&report, ImportItemKind::ManagedSkill));
        assert!(has_kind(&report, ImportItemKind::ProjectAgentSkill));
        assert!(has_kind(&report, ImportItemKind::AgentDirectory));
        assert!(has_kind(&report, ImportItemKind::NativeCronStore));
        assert!(has_kind(&report, ImportItemKind::MemoryStore));
        assert!(has_kind(&report, ImportItemKind::PluginRecord));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn dry_run_report_marks_conflicts_and_renames() {
        let root = temp_root("dry_run_report_marks_conflicts_and_renames");
        let source_home = root.join(".openclaw");
        let workspace = source_home.join("workspace");
        let destination_home = root.join("harness-home");
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(destination_home.join("workspace")).unwrap();
        fs::write(workspace.join("AGENTS.md"), "source").unwrap();
        fs::write(destination_home.join("workspace").join("AGENTS.md"), "dest").unwrap();

        let conflict_report = build_dry_run_report(DryRunImportOptions {
            source: OpenClawSource::new(&source_home),
            destination_home: destination_home.clone(),
            conflict_policy: ConflictPolicy::Skip,
        })
        .unwrap();
        let prompt_item = first_kind(&conflict_report, ImportItemKind::PromptFile);
        assert_eq!(prompt_item.status, ImportItemStatus::Conflict);
        assert_eq!(conflict_report.summary.conflicts, 1);

        let rename_report = build_dry_run_report(DryRunImportOptions {
            source: OpenClawSource::new(&source_home),
            destination_home,
            conflict_policy: ConflictPolicy::Rename,
        })
        .unwrap();
        let prompt_item = first_kind(&rename_report, ImportItemKind::PromptFile);
        assert_eq!(prompt_item.status, ImportItemStatus::Planned);
        assert!(prompt_item.destination.ends_with("AGENTS-imported.md"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn write_report_files_outputs_json_and_markdown() {
        let root = temp_root("write_report_files_outputs_json_and_markdown");
        let source_home = root.join(".openclaw");
        let workspace = source_home.join("workspace");
        let destination_home = root.join("harness-home");
        let output_dir = root.join("report");
        fs::create_dir_all(&workspace).unwrap();
        fs::write(workspace.join("AGENTS.md"), "# Agent").unwrap();

        let report = build_dry_run_report(DryRunImportOptions {
            source: OpenClawSource::new(&source_home),
            destination_home,
            conflict_policy: ConflictPolicy::Skip,
        })
        .unwrap();
        let files = write_report_files(&report, &output_dir).unwrap();

        assert!(files.json.is_file());
        assert!(files.summary.is_file());
        let json: serde_json::Value =
            serde_json::from_slice(&fs::read(files.json).unwrap()).unwrap();
        assert_eq!(json["schema"], REPORT_SCHEMA);
        assert!(
            fs::read_to_string(files.summary)
                .unwrap()
                .contains("OpenClaw Import Dry Run")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn dry_run_report_extracts_non_secret_semantics() {
        let root = temp_root("dry_run_report_extracts_non_secret_semantics");
        let source_home = root.join(".openclaw");
        let workspace = source_home.join("workspace");
        let main_sessions = source_home.join("agents").join("main").join("sessions");
        let cron_sessions = source_home
            .join("agents")
            .join("cron-lite")
            .join("sessions");
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(&main_sessions).unwrap();
        fs::create_dir_all(&cron_sessions).unwrap();
        fs::create_dir_all(source_home.join("cron")).unwrap();

        fs::write(
            source_home.join("openclaw.json"),
            r#"{
              "agents": {
                "defaults": { "workspace": "workspace" },
                "list": [
                  { "id": "main" },
                  { "id": "cron-lite" }
                ]
              },
              "models": {
                "providers": {
                  "openai": {},
                  "openrouter": {}
                }
              },
              "plugins": [
                { "id": "telegram" },
                { "id": "discord" },
                { "id": "openclaw-mem-engine" }
              ]
            }"#,
        )
        .unwrap();
        fs::write(
            main_sessions.join("sessions.json"),
            r#"{ "sessions": [{ "id": "a" }, { "id": "b" }] }"#,
        )
        .unwrap();
        fs::write(
            cron_sessions.join("sessions.json"),
            r#"{ "sessions": { "c": {}, "d": {}, "e": {} } }"#,
        )
        .unwrap();
        fs::write(
            source_home.join("cron").join("jobs.json"),
            r#"{
              "jobs": [
                {
                  "id": "job-1",
                  "enabled": true,
                  "agentId": "main",
                  "schedule": { "type": "cron", "expression": "* * * * *" },
                  "wakeMode": "now"
                },
                {
                  "id": "job-2",
                  "enabled": false,
                  "agentId": "cron-lite",
                  "schedule": { "type": "at", "time": "2026-06-08T00:00:00Z" },
                  "wakeMode": "next-heartbeat"
                },
                {
                  "id": "job-3",
                  "agentId": "main",
                  "schedule": "* * * * *"
                }
              ]
            }"#,
        )
        .unwrap();
        fs::write(
            source_home.join("cron").join("jobs-state.json"),
            r#"{ "jobs": { "job-1": {}, "job-2": {} } }"#,
        )
        .unwrap();

        let report = build_dry_run_report(DryRunImportOptions {
            source: OpenClawSource::new(&source_home),
            destination_home: root.join("harness-home"),
            conflict_policy: ConflictPolicy::Skip,
        })
        .unwrap();

        assert!(report.semantics.config.parsed);
        assert_eq!(report.semantics.config.agent_count, 2);
        assert_eq!(
            report.semantics.config.provider_ids,
            ["openai", "openrouter"]
        );
        assert_eq!(report.semantics.config.plugin_count, 3);
        assert!(report.semantics.config.telegram_configured);
        assert!(report.semantics.config.discord_configured);
        assert_eq!(
            report.semantics.config.memory_plugins,
            ["openclaw-mem-engine"]
        );
        assert_eq!(report.semantics.sessions.parsed_indexes, 2);
        assert_eq!(report.semantics.sessions.total_records, 5);
        assert_eq!(report.semantics.sessions.records_by_agent["main"], 2);
        assert_eq!(report.semantics.sessions.records_by_agent["cron-lite"], 3);
        assert_eq!(report.semantics.native_cron.total_jobs, 3);
        assert_eq!(report.semantics.native_cron.enabled_jobs, 2);
        assert_eq!(report.semantics.native_cron.disabled_jobs, 1);
        assert_eq!(report.semantics.native_cron.state_entries, 2);
        assert_eq!(report.semantics.native_cron.jobs_by_agent["main"], 2);
        assert_eq!(report.semantics.native_cron.jobs_by_agent["cron-lite"], 1);
        assert_eq!(report.semantics.native_cron.schedule_types["cron"], 2);
        assert_eq!(report.semantics.native_cron.schedule_types["at"], 1);

        let _ = fs::remove_dir_all(root);
    }

    fn has_kind(report: &ImportReport, kind: ImportItemKind) -> bool {
        report.items.iter().any(|item| item.kind == kind)
    }

    fn first_kind(report: &ImportReport, kind: ImportItemKind) -> &ImportItem {
        report
            .items
            .iter()
            .find(|item| item.kind == kind)
            .unwrap_or_else(|| panic!("missing item kind {kind:?}"))
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "openclaw-harness-importer-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
