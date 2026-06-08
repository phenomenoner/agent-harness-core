use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::{OpenClawSource, SKILL_FILE_NAME};

const SKILL_INDEX_SCHEMA: &str = "openclaw-harness.skill-index.v1";
const IMPORTED_SKILL_NAMESPACE: &str = "openclaw-imports";
pub const HARNESS_BUILTIN_SKILL_NAMESPACE: &str = "openclaw-harness-core";
const MAX_KEYWORDS: usize = 80;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum SkillIndexOrigin {
    OpenClawSource,
    HarnessImport,
    RuntimeMerged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum SkillSourceKind {
    Workspace,
    Managed,
    ProjectAgent,
    ImportedWorkspace,
    ImportedManaged,
    ImportedProjectAgent,
    HarnessBuiltin,
}

impl SkillSourceKind {
    fn prefix(self) -> &'static str {
        match self {
            SkillSourceKind::Workspace => "workspace",
            SkillSourceKind::Managed => "managed",
            SkillSourceKind::ProjectAgent => "project-agent",
            SkillSourceKind::ImportedWorkspace => "imported-workspace",
            SkillSourceKind::ImportedManaged => "imported-managed",
            SkillSourceKind::ImportedProjectAgent => "imported-project-agent",
            SkillSourceKind::HarnessBuiltin => "harness-builtin",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillIndex {
    pub schema: &'static str,
    pub origin: SkillIndexOrigin,
    pub source_home: Option<PathBuf>,
    pub source_workspace: Option<PathBuf>,
    pub harness_home: Option<PathBuf>,
    pub summary: SkillIndexSummary,
    pub skills: Vec<SkillRecord>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillIndexSummary {
    pub total_skills: usize,
    pub workspace_skills: usize,
    pub managed_skills: usize,
    pub project_agent_skills: usize,
    pub imported_workspace_skills: usize,
    pub imported_managed_skills: usize,
    pub imported_project_agent_skills: usize,
    pub harness_builtin_skills: usize,
    pub skills_with_references: usize,
    pub skills_with_templates: usize,
    pub skills_with_scripts: usize,
    pub skills_with_assets: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillRecord {
    pub id: String,
    pub original_id: String,
    pub source_kind: SkillSourceKind,
    pub directory: PathBuf,
    pub skill_file: PathBuf,
    pub title: String,
    pub description: Option<String>,
    pub keywords: Vec<String>,
    pub has_references: bool,
    pub has_templates: bool,
    pub has_scripts: bool,
    pub has_assets: bool,
    pub file_count: usize,
    pub body_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillSelectionQuery {
    pub text: String,
    pub agent_id: Option<String>,
    pub channel: Option<String>,
    pub workspace: Option<String>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillSelection {
    pub skill_id: String,
    pub original_id: String,
    pub source_kind: SkillSourceKind,
    pub title: String,
    pub directory: PathBuf,
    pub score: usize,
    pub reasons: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillIndexFile {
    pub json: PathBuf,
}

pub fn build_source_skill_index(source: &OpenClawSource) -> io::Result<SkillIndex> {
    let mut skills = Vec::new();
    add_skill_root(
        &mut skills,
        SkillSourceKind::Workspace,
        &source.workspace.join("skills"),
    )?;
    add_skill_root(
        &mut skills,
        SkillSourceKind::Managed,
        &source.home.join("skills"),
    )?;
    add_skill_root(
        &mut skills,
        SkillSourceKind::ProjectAgent,
        &source.workspace.join(".agents").join("skills"),
    )?;
    skills.sort_by(|left, right| left.id.cmp(&right.id));

    Ok(SkillIndex {
        schema: SKILL_INDEX_SCHEMA,
        origin: SkillIndexOrigin::OpenClawSource,
        source_home: Some(source.home.clone()),
        source_workspace: Some(source.workspace.clone()),
        harness_home: None,
        summary: summarize_skills(&skills),
        skills,
    })
}

pub fn build_harness_skill_index(harness_home: impl AsRef<Path>) -> io::Result<SkillIndex> {
    let harness_home = harness_home.as_ref();
    let imported_root = harness_home.join("skills").join(IMPORTED_SKILL_NAMESPACE);
    let builtin_root = harness_home
        .join("skills")
        .join(HARNESS_BUILTIN_SKILL_NAMESPACE);
    let mut skills = Vec::new();
    add_skill_root(
        &mut skills,
        SkillSourceKind::ImportedWorkspace,
        &imported_root.join("workspace"),
    )?;
    add_skill_root(
        &mut skills,
        SkillSourceKind::ImportedManaged,
        &imported_root.join("managed"),
    )?;
    add_skill_root(
        &mut skills,
        SkillSourceKind::ImportedProjectAgent,
        &imported_root.join("project-agents"),
    )?;
    add_skill_root(&mut skills, SkillSourceKind::HarnessBuiltin, &builtin_root)?;
    skills.sort_by(|left, right| left.id.cmp(&right.id));

    Ok(SkillIndex {
        schema: SKILL_INDEX_SCHEMA,
        origin: SkillIndexOrigin::HarnessImport,
        source_home: None,
        source_workspace: None,
        harness_home: Some(harness_home.to_path_buf()),
        summary: summarize_skills(&skills),
        skills,
    })
}

pub fn build_runtime_skill_index(
    source: &OpenClawSource,
    harness_home: impl AsRef<Path>,
) -> io::Result<SkillIndex> {
    let harness_home = harness_home.as_ref();
    let mut source_index = build_source_skill_index(source)?;
    let harness_index = build_harness_skill_index(harness_home)?;
    source_index.skills.extend(harness_index.skills);
    source_index
        .skills
        .sort_by(|left, right| left.id.cmp(&right.id));
    let skills = source_index.skills;
    Ok(SkillIndex {
        schema: SKILL_INDEX_SCHEMA,
        origin: SkillIndexOrigin::RuntimeMerged,
        source_home: Some(source.home.clone()),
        source_workspace: Some(source.workspace.clone()),
        harness_home: Some(harness_home.to_path_buf()),
        summary: summarize_skills(&skills),
        skills,
    })
}

pub fn select_skills(index: &SkillIndex, query: &SkillSelectionQuery) -> Vec<SkillSelection> {
    let query_tokens = query_tokens(query);
    if query_tokens.is_empty() {
        return Vec::new();
    }

    let mut selections = Vec::new();
    for skill in &index.skills {
        let (score, reasons) = score_skill(skill, &query_tokens, query);
        if score == 0 {
            continue;
        }
        selections.push(SkillSelection {
            skill_id: skill.id.clone(),
            original_id: skill.original_id.clone(),
            source_kind: skill.source_kind,
            title: skill.title.clone(),
            directory: skill.directory.clone(),
            score,
            reasons,
        });
    }

    selections.sort_by(|left, right| {
        right
            .score
            .cmp(&left.score)
            .then_with(|| left.skill_id.cmp(&right.skill_id))
    });
    selections.truncate(query.limit.max(1));
    selections
}

pub fn write_skill_index(
    index: &SkillIndex,
    output_dir: impl AsRef<Path>,
) -> io::Result<SkillIndexFile> {
    let output_dir = output_dir.as_ref();
    fs::create_dir_all(output_dir)?;
    let json = output_dir.join("skill-index.json");
    let text = serde_json::to_string_pretty(index).map_err(io::Error::other)?;
    fs::write(&json, text)?;
    Ok(SkillIndexFile { json })
}

fn add_skill_root(
    skills: &mut Vec<SkillRecord>,
    source_kind: SkillSourceKind,
    root: &Path,
) -> io::Result<()> {
    if !root.exists() {
        return Ok(());
    }

    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }

        let directory = entry.path();
        let skill_file = directory.join(SKILL_FILE_NAME);
        if !skill_file.is_file() {
            continue;
        }
        skills.push(read_skill_record(source_kind, &directory, &skill_file)?);
    }

    Ok(())
}

fn read_skill_record(
    source_kind: SkillSourceKind,
    directory: &Path,
    skill_file: &Path,
) -> io::Result<SkillRecord> {
    let original_id = directory
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("skill")
        .to_string();
    let bytes = fs::read(skill_file)?;
    let body = String::from_utf8_lossy(&bytes);
    let metadata = parse_skill_metadata(&body, &original_id);
    let keywords = skill_keywords(
        &original_id,
        &metadata.title,
        metadata.description.as_deref(),
        &body,
    );
    let has_references = directory.join("references").is_dir();
    let has_templates = directory.join("templates").is_dir();
    let has_scripts = directory.join("scripts").is_dir();
    let has_assets = directory.join("assets").is_dir();
    let file_count = count_regular_files(directory)?;

    Ok(SkillRecord {
        id: format!("{}:{original_id}", source_kind.prefix()),
        original_id,
        source_kind,
        directory: directory.to_path_buf(),
        skill_file: skill_file.to_path_buf(),
        title: metadata.title,
        description: metadata.description,
        keywords,
        has_references,
        has_templates,
        has_scripts,
        has_assets,
        file_count,
        body_bytes: bytes.len(),
    })
}

struct SkillMetadata {
    title: String,
    description: Option<String>,
}

fn parse_skill_metadata(body: &str, fallback_id: &str) -> SkillMetadata {
    let mut title = frontmatter_value(body, &["title", "name"]);
    let mut description = frontmatter_value(body, &["description"]);
    let content_start = frontmatter_end_line(body).unwrap_or(0);

    for line in body.lines().skip(content_start) {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if title.is_none() && trimmed.starts_with('#') {
            title = Some(trimmed.trim_start_matches('#').trim().to_string());
            continue;
        }
        if description.is_none()
            && !trimmed.starts_with('#')
            && !trimmed.starts_with("- ")
            && !trimmed.starts_with("* ")
            && !trimmed.starts_with("```")
        {
            description = Some(truncate_text(trimmed, 240));
        }
        if title.is_some() && description.is_some() {
            break;
        }
    }

    SkillMetadata {
        title: title.unwrap_or_else(|| fallback_id.to_string()),
        description,
    }
}

fn frontmatter_value(body: &str, keys: &[&str]) -> Option<String> {
    let mut lines = body.lines();
    if lines.next().map(str::trim) != Some("---") {
        return None;
    }

    for line in lines {
        let trimmed = line.trim();
        if trimmed == "---" {
            break;
        }
        let Some((key, value)) = trimmed.split_once(':') else {
            continue;
        };
        if keys
            .iter()
            .any(|candidate| key.trim().eq_ignore_ascii_case(candidate))
        {
            let value = trim_yaml_scalar(value);
            if !value.is_empty() {
                return Some(value);
            }
        }
    }
    None
}

fn frontmatter_end_line(body: &str) -> Option<usize> {
    let mut lines = body.lines();
    if lines.next().map(str::trim) != Some("---") {
        return None;
    }

    for (index, line) in lines.enumerate() {
        if line.trim() == "---" {
            return Some(index + 2);
        }
    }
    None
}

fn trim_yaml_scalar(value: &str) -> String {
    value
        .trim()
        .trim_matches('"')
        .trim_matches('\'')
        .trim()
        .to_string()
}

fn truncate_text(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

fn skill_keywords(
    original_id: &str,
    title: &str,
    description: Option<&str>,
    body: &str,
) -> Vec<String> {
    let mut tokens = BTreeSet::new();
    extend_tokens(&mut tokens, original_id);
    extend_tokens(&mut tokens, title);
    if let Some(description) = description {
        extend_tokens(&mut tokens, description);
    }
    extend_tokens(&mut tokens, &body.chars().take(6000).collect::<String>());
    tokens.into_iter().take(MAX_KEYWORDS).collect()
}

fn query_tokens(query: &SkillSelectionQuery) -> BTreeSet<String> {
    let mut tokens = BTreeSet::new();
    extend_tokens(&mut tokens, &query.text);
    if let Some(agent_id) = &query.agent_id {
        extend_tokens(&mut tokens, agent_id);
    }
    if let Some(channel) = &query.channel {
        extend_tokens(&mut tokens, channel);
    }
    if let Some(workspace) = &query.workspace {
        extend_tokens(&mut tokens, workspace);
    }
    tokens
}

fn extend_tokens(tokens: &mut BTreeSet<String>, text: &str) {
    let mut token = String::new();
    for ch in text.chars() {
        if ch.is_ascii_alphanumeric() {
            token.push(ch.to_ascii_lowercase());
        } else {
            push_token(tokens, &mut token);
        }
    }
    push_token(tokens, &mut token);
}

fn push_token(tokens: &mut BTreeSet<String>, token: &mut String) {
    if token.len() >= 2 {
        tokens.insert(std::mem::take(token));
    } else {
        token.clear();
    }
}

fn score_skill(
    skill: &SkillRecord,
    query_tokens: &BTreeSet<String>,
    query: &SkillSelectionQuery,
) -> (usize, Vec<String>) {
    let mut score = 0;
    let mut reasons = Vec::new();
    let id_tokens = token_set(&skill.original_id);
    let title_tokens = token_set(&skill.title);
    let description_tokens = skill
        .description
        .as_deref()
        .map(token_set)
        .unwrap_or_default();
    let keyword_tokens: BTreeSet<String> = skill.keywords.iter().cloned().collect();

    let id_matches = count_matches(query_tokens, &id_tokens);
    if id_matches > 0 {
        score += id_matches * 20;
        reasons.push(format!("{id_matches} id token match(es)"));
    }

    let title_matches = count_matches(query_tokens, &title_tokens);
    if title_matches > 0 {
        score += title_matches * 12;
        reasons.push(format!("{title_matches} title token match(es)"));
    }

    let description_matches = count_matches(query_tokens, &description_tokens);
    if description_matches > 0 {
        score += description_matches * 8;
        reasons.push(format!("{description_matches} description token match(es)"));
    }

    let keyword_matches = count_matches(query_tokens, &keyword_tokens);
    if keyword_matches > 0 {
        score += keyword_matches * 4;
        reasons.push(format!("{keyword_matches} body keyword match(es)"));
    }

    if query
        .agent_id
        .as_deref()
        .is_some_and(|agent_id| text_contains_token(&skill.original_id, agent_id))
    {
        score += 5;
        reasons.push("agent id appears in skill id".to_string());
    }

    if query
        .channel
        .as_deref()
        .is_some_and(|channel| text_contains_token(&skill.original_id, channel))
    {
        score += 5;
        reasons.push("channel appears in skill id".to_string());
    }

    (score, reasons)
}

fn token_set(text: &str) -> BTreeSet<String> {
    let mut tokens = BTreeSet::new();
    extend_tokens(&mut tokens, text);
    tokens
}

fn count_matches(left: &BTreeSet<String>, right: &BTreeSet<String>) -> usize {
    left.intersection(right).count()
}

fn text_contains_token(text: &str, token: &str) -> bool {
    let token = token.to_ascii_lowercase();
    token_set(text).contains(&token)
}

fn summarize_skills(skills: &[SkillRecord]) -> SkillIndexSummary {
    let mut summary = SkillIndexSummary {
        total_skills: skills.len(),
        ..SkillIndexSummary::default()
    };
    for skill in skills {
        match skill.source_kind {
            SkillSourceKind::Workspace => summary.workspace_skills += 1,
            SkillSourceKind::Managed => summary.managed_skills += 1,
            SkillSourceKind::ProjectAgent => summary.project_agent_skills += 1,
            SkillSourceKind::ImportedWorkspace => summary.imported_workspace_skills += 1,
            SkillSourceKind::ImportedManaged => summary.imported_managed_skills += 1,
            SkillSourceKind::ImportedProjectAgent => summary.imported_project_agent_skills += 1,
            SkillSourceKind::HarnessBuiltin => summary.harness_builtin_skills += 1,
        }
        if skill.has_references {
            summary.skills_with_references += 1;
        }
        if skill.has_templates {
            summary.skills_with_templates += 1;
        }
        if skill.has_scripts {
            summary.skills_with_scripts += 1;
        }
        if skill.has_assets {
            summary.skills_with_assets += 1;
        }
    }
    summary
}

fn count_regular_files(root: &Path) -> io::Result<usize> {
    if !root.exists() {
        return Ok(0);
    }

    let mut count = 0;
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            count += count_regular_files(&path)?;
        } else if file_type.is_file() {
            count += 1;
        }
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn source_skill_index_discovers_openclaw_skill_locations() {
        let root = temp_root("source_skill_index_discovers_openclaw_skill_locations");
        let home = root.join(".openclaw");
        let workspace = home.join("workspace");
        let workspace_skill = workspace.join("skills").join("memory-cron");
        let managed_skill = home.join("skills").join("provider-routing");
        let project_skill = workspace
            .join(".agents")
            .join("skills")
            .join("telegram-agent");
        fs::create_dir_all(workspace_skill.join("references")).unwrap();
        fs::create_dir_all(managed_skill.join("scripts")).unwrap();
        fs::create_dir_all(project_skill.join("assets")).unwrap();
        fs::write(
            workspace_skill.join(SKILL_FILE_NAME),
            "# Memory Cron\n\nMaintain deterministic memory cron jobs.",
        )
        .unwrap();
        fs::write(
            managed_skill.join(SKILL_FILE_NAME),
            "---\ndescription: Route OpenRouter and Codex models.\n---\n# Provider Routing\n",
        )
        .unwrap();
        fs::write(
            project_skill.join(SKILL_FILE_NAME),
            "# Telegram Agent\n\nHandle Telegram DM turns.",
        )
        .unwrap();

        let index =
            build_source_skill_index(&OpenClawSource::with_workspace(&home, &workspace)).unwrap();

        assert_eq!(index.origin, SkillIndexOrigin::OpenClawSource);
        assert_eq!(index.summary.total_skills, 3);
        assert_eq!(index.summary.workspace_skills, 1);
        assert_eq!(index.summary.managed_skills, 1);
        assert_eq!(index.summary.project_agent_skills, 1);
        assert_eq!(index.summary.skills_with_references, 1);
        assert_eq!(index.summary.skills_with_scripts, 1);
        assert_eq!(index.summary.skills_with_assets, 1);
        assert!(
            index
                .skills
                .iter()
                .any(|skill| skill.id == "workspace:memory-cron"
                    && skill.title == "Memory Cron"
                    && skill.has_references)
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn harness_skill_index_discovers_imported_namespace() {
        let root = temp_root("harness_skill_index_discovers_imported_namespace");
        let harness_home = root.join("harness-home");
        let imported_skill = harness_home
            .join("skills")
            .join(IMPORTED_SKILL_NAMESPACE)
            .join("project-agents")
            .join("handoff");
        fs::create_dir_all(&imported_skill).unwrap();
        fs::write(
            imported_skill.join(SKILL_FILE_NAME),
            "# Handoff\n\nCoordinate subagent handoff.",
        )
        .unwrap();

        let index = build_harness_skill_index(&harness_home).unwrap();

        assert_eq!(index.origin, SkillIndexOrigin::HarnessImport);
        assert_eq!(index.summary.total_skills, 1);
        assert_eq!(index.summary.imported_project_agent_skills, 1);
        assert_eq!(index.skills[0].id, "imported-project-agent:handoff");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn harness_skill_index_discovers_builtin_namespace() {
        let root = temp_root("harness_skill_index_discovers_builtin_namespace");
        let harness_home = root.join("harness-home");
        let builtin_skill = harness_home
            .join("skills")
            .join(HARNESS_BUILTIN_SKILL_NAMESPACE)
            .join("openclaw-windows-harness");
        fs::create_dir_all(&builtin_skill).unwrap();
        fs::write(
            builtin_skill.join(SKILL_FILE_NAME),
            "# OpenClaw Windows Harness\n\nOperate the Rust harness.",
        )
        .unwrap();

        let index = build_harness_skill_index(&harness_home).unwrap();

        assert_eq!(index.summary.total_skills, 1);
        assert_eq!(index.summary.harness_builtin_skills, 1);
        assert_eq!(
            index.skills[0].id,
            "harness-builtin:openclaw-windows-harness"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn runtime_skill_index_merges_source_and_harness_skills() {
        let root = temp_root("runtime_skill_index_merges_source_and_harness_skills");
        let home = root.join(".openclaw");
        let workspace = home.join("workspace");
        let workspace_skill = workspace.join("skills").join("memory-cron");
        let harness_home = root.join("harness-home");
        let builtin_skill = harness_home
            .join("skills")
            .join(HARNESS_BUILTIN_SKILL_NAMESPACE)
            .join("openclaw-windows-harness");
        fs::create_dir_all(&workspace_skill).unwrap();
        fs::create_dir_all(&builtin_skill).unwrap();
        fs::write(
            workspace_skill.join(SKILL_FILE_NAME),
            "# Memory Cron\n\nRepair openclaw-mem jobs.",
        )
        .unwrap();
        fs::write(
            builtin_skill.join(SKILL_FILE_NAME),
            "# OpenClaw Windows Harness\n\nOperate Telegram and Discord handoff.",
        )
        .unwrap();

        let index = build_runtime_skill_index(
            &OpenClawSource::with_workspace(&home, &workspace),
            &harness_home,
        )
        .unwrap();

        assert_eq!(index.origin, SkillIndexOrigin::RuntimeMerged);
        assert_eq!(index.summary.total_skills, 2);
        assert_eq!(index.summary.workspace_skills, 1);
        assert_eq!(index.summary.harness_builtin_skills, 1);
        assert!(
            index
                .skills
                .iter()
                .any(|skill| skill.id == "workspace:memory-cron")
        );
        assert!(
            index
                .skills
                .iter()
                .any(|skill| skill.id == "harness-builtin:openclaw-windows-harness")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn skill_selection_ranks_relevant_skill_for_turn_context() {
        let root = temp_root("skill_selection_ranks_relevant_skill_for_turn_context");
        let home = root.join(".openclaw");
        let workspace = home.join("workspace");
        let memory_skill = workspace.join("skills").join("memory-cron");
        let discord_skill = workspace.join("skills").join("discord-channel");
        fs::create_dir_all(&memory_skill).unwrap();
        fs::create_dir_all(&discord_skill).unwrap();
        fs::write(
            memory_skill.join(SKILL_FILE_NAME),
            "# Memory Cron\n\nRepair openclaw-mem deterministic cron extraction jobs.",
        )
        .unwrap();
        fs::write(
            discord_skill.join(SKILL_FILE_NAME),
            "# Discord Channel\n\nHandle Discord DM delivery retries.",
        )
        .unwrap();
        let index =
            build_source_skill_index(&OpenClawSource::with_workspace(&home, &workspace)).unwrap();

        let selections = select_skills(
            &index,
            &SkillSelectionQuery {
                text: "fix openclaw mem cron delivery runner".to_string(),
                agent_id: Some("mem-cron".to_string()),
                channel: None,
                workspace: None,
                limit: 2,
            },
        );

        assert_eq!(selections[0].skill_id, "workspace:memory-cron");
        assert!(selections[0].score > selections[1].score);
        assert!(
            selections[0]
                .reasons
                .iter()
                .any(|reason| reason.contains("id"))
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn write_skill_index_outputs_json() {
        let root = temp_root("write_skill_index_outputs_json");
        let home = root.join(".openclaw");
        let workspace = home.join("workspace");
        let skill = workspace.join("skills").join("triage");
        fs::create_dir_all(&skill).unwrap();
        fs::write(skill.join(SKILL_FILE_NAME), "# Triage\n\nPrioritize tasks.").unwrap();

        let index =
            build_source_skill_index(&OpenClawSource::with_workspace(&home, &workspace)).unwrap();
        let file = write_skill_index(&index, root.join("out")).unwrap();

        assert!(file.json.is_file());
        let json: serde_json::Value =
            serde_json::from_slice(&fs::read(file.json).unwrap()).unwrap();
        assert_eq!(json["schema"], SKILL_INDEX_SCHEMA);
        assert_eq!(json["summary"]["totalSkills"], 1);

        let _ = fs::remove_dir_all(root);
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "openclaw-harness-skills-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
