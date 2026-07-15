use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};

use crate::skill_envelope::skill_body_checksum;
use crate::{
    AgentSource, SKILL_FILE_NAME, SkillUsageSnapshot, append_jsonl_value, current_log_time_ms,
};

const SKILL_INDEX_SCHEMA: &str = "agent-harness.skill-index.v1";
pub const SKILL_SELECTION_RECEIPT_SCHEMA: &str = "agent-harness.skill-selection.v1";
pub const SKILL_MATCHER_NAME: &str = "agent-harness-skill-matcher";
pub const SKILL_MATCHER_VERSION: &str = "v4";
pub const SKILL_MATCHER_TOKENIZER: &str = "mixed-v1";
const IMPORTED_SKILL_NAMESPACE: &str = "legacy-imports";
const OPENCLAW_IMPORTED_SKILL_NAMESPACE: &str = "openclaw-imports";
pub const HARNESS_BUILTIN_SKILL_NAMESPACE: &str = "agent-harness-core";
const MAX_KEYWORDS: usize = 80;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum SkillIndexOrigin {
    AgentSource,
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
    AgentCreated,
    Pack,
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
            SkillSourceKind::AgentCreated => "agent-created",
            SkillSourceKind::Pack => "pack",
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
    pub agent_created_skills: usize,
    pub pack_skills: usize,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub by_category: BTreeMap<String, usize>,
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
    pub frontmatter: SkillFrontmatter,
    pub has_references: bool,
    pub has_templates: bool,
    pub has_scripts: bool,
    pub has_assets: bool,
    pub file_count: usize,
    pub body_bytes: usize,
    pub body_checksum: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillFrontmatter {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub agents: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub triggers: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub platforms: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub requires_tools: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub requires_toolsets: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub agent_modes: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub channels: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery_mode: Option<SkillDeliveryMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_invocable: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disable_model_invocation: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifecycle: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub related_skills: Vec<String>,
}

impl SkillFrontmatter {
    fn is_retired(&self) -> bool {
        self.lifecycle.as_deref().is_some_and(|lifecycle| {
            matches!(
                normalize_frontmatter_key(lifecycle).as_str(),
                "retired" | "retired_historical"
            )
        })
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SkillDeliveryMode {
    IndexOnly,
    #[default]
    InjectedBody,
    InvocationEnvelope,
    ToolView,
}

impl SkillDeliveryMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::IndexOnly => "index-only",
            Self::InjectedBody => "injected-body",
            Self::InvocationEnvelope => "invocation-envelope",
            Self::ToolView => "tool-view",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillSelectionQuery {
    pub text: String,
    pub agent_id: Option<String>,
    pub channel: Option<String>,
    pub workspace: Option<String>,
    pub agent_mode: Option<String>,
    pub available_tools: Vec<String>,
    pub available_toolsets: Vec<String>,
    pub fts_enabled: bool,
    pub vector_tie_break_enabled: bool,
    pub usage_snapshot: Option<SkillUsageSnapshot>,
    pub usage_prior_enabled: bool,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillSelection {
    pub skill_id: String,
    pub original_id: String,
    pub source_kind: SkillSourceKind,
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    pub directory: PathBuf,
    pub score: usize,
    pub score_components: Vec<SkillScoreComponent>,
    pub reasons: Vec<String>,
    pub delivery_mode: SkillDeliveryMode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_instruction: Option<String>,
    pub body_checksum: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selection_receipt_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillScoreComponent {
    pub name: String,
    pub score: usize,
    pub matches: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillSelectionReceipt {
    pub schema: &'static str,
    pub receipt_id: String,
    pub harness_home: PathBuf,
    pub at_ms: i64,
    pub index_schema: &'static str,
    pub matcher_name: &'static str,
    pub matcher_version: &'static str,
    pub tokenizer: &'static str,
    pub fts_enabled: bool,
    pub vector_enabled: bool,
    pub query_text_bytes: usize,
    pub agent_id: Option<String>,
    pub channel: Option<String>,
    pub workspace: Option<String>,
    pub selected: Vec<SkillSelection>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillIndexFile {
    pub json: PathBuf,
}

pub fn build_source_skill_index(source: &AgentSource) -> io::Result<SkillIndex> {
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
        origin: SkillIndexOrigin::AgentSource,
        source_home: Some(source.home.clone()),
        source_workspace: Some(source.workspace.clone()),
        harness_home: None,
        summary: summarize_skills(&skills),
        skills,
    })
}

pub fn build_harness_skill_index(harness_home: impl AsRef<Path>) -> io::Result<SkillIndex> {
    let harness_home = harness_home.as_ref();
    let imported_roots = [
        harness_home.join("skills").join(IMPORTED_SKILL_NAMESPACE),
        harness_home
            .join("skills")
            .join(OPENCLAW_IMPORTED_SKILL_NAMESPACE),
    ];
    let builtin_root = harness_home
        .join("skills")
        .join(HARNESS_BUILTIN_SKILL_NAMESPACE);
    let agent_created_root = harness_home.join("skills").join("agent-created");
    let packs_root = harness_home.join("skills").join("packs");
    let mut skills = Vec::new();
    for imported_root in imported_roots {
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
    }
    add_skill_root(&mut skills, SkillSourceKind::HarnessBuiltin, &builtin_root)?;
    add_skill_root(
        &mut skills,
        SkillSourceKind::AgentCreated,
        &agent_created_root,
    )?;
    add_pack_skill_roots(&mut skills, &packs_root)?;
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
    source: &AgentSource,
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
    let explicit = ExplicitSkillInvocation::parse(&query.text);
    if query_tokens.is_empty() && explicit.is_none() {
        return Vec::new();
    }
    let fts_scores = if query.fts_enabled {
        sqlite_fts_scores(index, &query_tokens).unwrap_or_default()
    } else {
        BTreeMap::new()
    };

    let mut selections = Vec::new();
    for skill in &index.skills {
        let explicit_match = explicit
            .as_ref()
            .filter(|invocation| skill_id_matches(skill, &invocation.skill_id));
        if explicit.is_some() && explicit_match.is_none() {
            continue;
        }
        if selection_eligibility_blocker(skill, explicit_match.is_some(), query.agent_id.as_deref())
            .is_some()
        {
            continue;
        }
        let Some((score, score_components, reasons)) =
            score_skill(skill, &query_tokens, query, &fts_scores)
        else {
            continue;
        };
        if score == 0 {
            continue;
        }
        let delivery_mode = explicit_match
            .map(|_| SkillDeliveryMode::InvocationEnvelope)
            .or(skill.frontmatter.delivery_mode)
            .unwrap_or(SkillDeliveryMode::InjectedBody);
        let user_instruction = explicit_match.map(|invocation| invocation.user_instruction.clone());
        let selection_receipt_id = Some(stable_selection_receipt_id(
            &query.text,
            &skill.id,
            delivery_mode,
            &skill.body_checksum,
        ));
        selections.push(SkillSelection {
            skill_id: skill.id.clone(),
            original_id: skill.original_id.clone(),
            source_kind: skill.source_kind,
            title: skill.title.clone(),
            description: skill.description.clone(),
            category: skill.frontmatter.category.clone(),
            tags: skill.frontmatter.tags.clone(),
            directory: skill.directory.clone(),
            score,
            score_components,
            reasons,
            delivery_mode,
            user_instruction,
            body_checksum: skill.body_checksum.clone(),
            selection_receipt_id,
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

pub fn skill_selection_receipts_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("learning")
        .join("skill-selection-receipts.jsonl")
}

pub fn write_skill_selection_receipt(
    harness_home: impl AsRef<Path>,
    query: &SkillSelectionQuery,
    selections: &[SkillSelection],
) -> io::Result<SkillSelectionReceipt> {
    let harness_home = harness_home.as_ref();
    let at_ms = current_log_time_ms().unwrap_or(0);
    let receipt_id = stable_text_hash(
        "skill-selection",
        &format!(
            "{}|{}|{}|{}|{}",
            at_ms,
            query.text,
            query.agent_id.as_deref().unwrap_or(""),
            query.channel.as_deref().unwrap_or(""),
            selections
                .iter()
                .map(|selection| selection.skill_id.as_str())
                .collect::<Vec<_>>()
                .join(",")
        ),
    );
    let receipt = SkillSelectionReceipt {
        schema: SKILL_SELECTION_RECEIPT_SCHEMA,
        receipt_id,
        harness_home: harness_home.to_path_buf(),
        at_ms,
        index_schema: SKILL_INDEX_SCHEMA,
        matcher_name: SKILL_MATCHER_NAME,
        matcher_version: SKILL_MATCHER_VERSION,
        tokenizer: SKILL_MATCHER_TOKENIZER,
        fts_enabled: query.fts_enabled,
        vector_enabled: query.vector_tie_break_enabled,
        query_text_bytes: query.text.len(),
        agent_id: query.agent_id.clone(),
        channel: query.channel.clone(),
        workspace: query.workspace.clone(),
        selected: selections.to_vec(),
    };
    append_jsonl_value(&skill_selection_receipts_file(harness_home), &receipt)?;
    Ok(receipt)
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
        let Some(name) = directory.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        if name.starts_with('.') {
            continue;
        }
        let skill_file = directory.join(SKILL_FILE_NAME);
        if skill_file.is_file() {
            skills.push(read_skill_record(
                source_kind,
                &directory,
                &skill_file,
                None,
            )?);
            continue;
        }
        for child in fs::read_dir(&directory)? {
            let child = child?;
            if !child.file_type()?.is_dir() {
                continue;
            }
            let child_directory = child.path();
            let Some(child_name) = child_directory.file_name().and_then(|value| value.to_str())
            else {
                continue;
            };
            if child_name.starts_with('.') {
                continue;
            }
            let child_skill_file = child_directory.join(SKILL_FILE_NAME);
            if child_skill_file.is_file() {
                skills.push(read_skill_record(
                    source_kind,
                    &child_directory,
                    &child_skill_file,
                    Some(name),
                )?);
            }
        }
    }

    Ok(())
}

fn add_pack_skill_roots(skills: &mut Vec<SkillRecord>, packs_root: &Path) -> io::Result<()> {
    if !packs_root.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(packs_root)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let pack_root = entry.path();
        let Some(pack_name) = pack_root.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        if pack_name.starts_with('.') {
            continue;
        }
        add_skill_root(skills, SkillSourceKind::Pack, &pack_root)?;
    }
    Ok(())
}

fn read_skill_record(
    source_kind: SkillSourceKind,
    directory: &Path,
    skill_file: &Path,
    inferred_category: Option<&str>,
) -> io::Result<SkillRecord> {
    let original_id = directory
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("skill")
        .to_string();
    let bytes = fs::read(skill_file)?;
    let body = String::from_utf8_lossy(&bytes);
    let mut metadata = parse_skill_metadata(&body, &original_id);
    if metadata.frontmatter.category.is_none() {
        metadata.frontmatter.category = inferred_category.map(ToString::to_string);
    }
    let keywords = skill_keywords(
        &original_id,
        &metadata.title,
        metadata.description.as_deref(),
        &metadata.frontmatter,
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
        frontmatter: metadata.frontmatter,
        has_references,
        has_templates,
        has_scripts,
        has_assets,
        file_count,
        body_bytes: bytes.len(),
        body_checksum: skill_body_checksum(&body),
    })
}

struct SkillMetadata {
    title: String,
    description: Option<String>,
    frontmatter: SkillFrontmatter,
}

fn parse_skill_metadata(body: &str, fallback_id: &str) -> SkillMetadata {
    let mut title = frontmatter_value(body, &["title", "name"]);
    let mut description = frontmatter_value(body, &["description"]);
    let frontmatter = parse_skill_frontmatter(body);
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
        frontmatter,
    }
}

fn parse_skill_frontmatter(body: &str) -> SkillFrontmatter {
    let mut tags = frontmatter_values(body, &["tags", "tag"]);
    tags.extend(frontmatter_nested_values(
        body,
        &["metadata", "agent_harness"],
        &["tags", "tag"],
    ));
    tags.sort();
    tags.dedup();
    SkillFrontmatter {
        agents: frontmatter_values(body, &["agents", "agentIds", "agent_ids"]),
        triggers: frontmatter_values(body, &["triggers", "trigger"]),
        conditions: frontmatter_values(body, &["conditions", "condition"]),
        platforms: frontmatter_values(body, &["platforms", "platform"]),
        requires_tools: frontmatter_values(body, &["requiresTools", "requires_tools"]),
        requires_toolsets: frontmatter_values(
            body,
            &["requiresToolsets", "requires_toolsets", "toolsets"],
        ),
        agent_modes: frontmatter_values(body, &["agentModes", "agent_modes"]),
        channels: frontmatter_values(body, &["channels", "channel"]),
        delivery_mode: frontmatter_value(body, &["deliveryMode", "delivery_mode"])
            .and_then(|value| parse_delivery_mode(&value)),
        user_invocable: frontmatter_bool(
            body,
            &["userInvocable", "user_invocable", "user-invocable"],
        ),
        disable_model_invocation: frontmatter_bool(
            body,
            &[
                "disableModelInvocation",
                "disable_model_invocation",
                "disable-model-invocation",
            ],
        ),
        lifecycle: frontmatter_value(body, &["lifecycle"]),
        category: frontmatter_value(body, &["category"]).or_else(|| {
            frontmatter_nested_value(body, &["metadata", "agent_harness"], &["category"])
        }),
        tags,
        version: frontmatter_value(body, &["version"]),
        author: frontmatter_value(body, &["author"]),
        related_skills: frontmatter_values(body, &["relatedSkills", "related_skills"]),
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

fn frontmatter_bool(body: &str, keys: &[&str]) -> Option<bool> {
    let value = frontmatter_value(body, keys)?;
    match value.trim().to_ascii_lowercase().as_str() {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

fn frontmatter_values(body: &str, keys: &[&str]) -> Vec<String> {
    let Some(block) = frontmatter_block(body) else {
        return Vec::new();
    };
    let mut values = BTreeSet::new();
    let mut lines = block.iter().enumerate().peekable();
    while let Some((index, line)) = lines.next() {
        let trimmed = line.trim();
        let Some((key, value)) = trimmed.split_once(':') else {
            continue;
        };
        if !keys
            .iter()
            .any(|candidate| key.trim().eq_ignore_ascii_case(candidate))
        {
            continue;
        }
        extend_yaml_values(&mut values, value);
        while let Some((_, next)) = lines.peek() {
            let next_trimmed = next.trim();
            if next_trimmed.is_empty() {
                lines.next();
                continue;
            }
            if next_trimmed.starts_with("- ") {
                extend_yaml_values(&mut values, next_trimmed.trim_start_matches("- "));
                lines.next();
                continue;
            }
            if next.contains(':') && !next.starts_with(' ') && !next.starts_with('\t') {
                break;
            }
            if index + 1 < block.len() {
                break;
            }
        }
    }
    values.into_iter().collect()
}

fn frontmatter_nested_value(body: &str, parents: &[&str], keys: &[&str]) -> Option<String> {
    frontmatter_nested_values(body, parents, keys)
        .into_iter()
        .next()
}

fn frontmatter_nested_values(body: &str, parents: &[&str], keys: &[&str]) -> Vec<String> {
    let Some(block) = frontmatter_block(body) else {
        return Vec::new();
    };
    let mut values = BTreeSet::new();
    let mut path: Vec<(usize, String)> = Vec::new();
    let mut lines = block.iter().peekable();
    while let Some(line) = lines.next() {
        let indent = leading_whitespace_count(line);
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        while path
            .last()
            .is_some_and(|(path_indent, _)| *path_indent >= indent)
        {
            path.pop();
        }
        let Some((key, value)) = trimmed.split_once(':') else {
            continue;
        };
        let normalized_key = normalize_frontmatter_key(key);
        if value.trim().is_empty() {
            path.push((indent, normalized_key));
            continue;
        }
        let current_path = path.iter().map(|(_, key)| key.as_str()).collect::<Vec<_>>();
        let parents_match = current_path.len() == parents.len()
            && current_path
                .iter()
                .zip(parents.iter())
                .all(|(left, right)| frontmatter_key_matches(left, right));
        if parents_match
            && keys
                .iter()
                .any(|candidate| frontmatter_key_matches(&normalized_key, candidate))
        {
            extend_yaml_values(&mut values, value);
            while let Some(next) = lines.peek() {
                let next_trimmed = next.trim();
                if next_trimmed.is_empty() {
                    lines.next();
                    continue;
                }
                if leading_whitespace_count(next) <= indent {
                    break;
                }
                if next_trimmed.starts_with("- ") {
                    extend_yaml_values(&mut values, next_trimmed.trim_start_matches("- "));
                    lines.next();
                    continue;
                }
                break;
            }
        }
    }
    values.into_iter().collect()
}

fn frontmatter_block(body: &str) -> Option<Vec<&str>> {
    let mut lines = body.lines();
    if lines.next().map(str::trim) != Some("---") {
        return None;
    }
    let mut block = Vec::new();
    for line in lines {
        if line.trim() == "---" {
            return Some(block);
        }
        block.push(line);
    }
    None
}

fn extend_yaml_values(values: &mut BTreeSet<String>, value: &str) {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return;
    }
    let trimmed = trimmed.trim_start_matches('[').trim_end_matches(']').trim();
    for item in trimmed.split(',') {
        let item = trim_yaml_scalar(item);
        if !item.is_empty() {
            values.insert(item);
        }
    }
}

fn leading_whitespace_count(value: &str) -> usize {
    value
        .chars()
        .take_while(|ch| matches!(ch, ' ' | '\t'))
        .count()
}

fn normalize_frontmatter_key(value: &str) -> String {
    value.trim().replace('-', "_").to_ascii_lowercase()
}

fn frontmatter_key_matches(left: &str, right: &str) -> bool {
    normalize_frontmatter_key(left) == normalize_frontmatter_key(right)
}

fn parse_delivery_mode(value: &str) -> Option<SkillDeliveryMode> {
    match value.trim().to_ascii_lowercase().as_str() {
        "index-only" | "index_only" => Some(SkillDeliveryMode::IndexOnly),
        "injected-body" | "injected_body" | "body" => Some(SkillDeliveryMode::InjectedBody),
        "invocation-envelope" | "invocation_envelope" | "envelope" => {
            Some(SkillDeliveryMode::InvocationEnvelope)
        }
        "tool-view" | "tool_view" => Some(SkillDeliveryMode::ToolView),
        _ => None,
    }
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
    frontmatter: &SkillFrontmatter,
    body: &str,
) -> Vec<String> {
    let mut tokens = BTreeSet::new();
    extend_tokens(&mut tokens, original_id);
    extend_tokens(&mut tokens, title);
    if let Some(description) = description {
        extend_tokens(&mut tokens, description);
    }
    if let Some(category) = &frontmatter.category {
        extend_tokens(&mut tokens, category);
    }
    for tag in &frontmatter.tags {
        extend_tokens(&mut tokens, tag);
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
    let mut ascii_token = String::new();
    let mut cjk_run = String::new();
    for ch in text.chars() {
        if ch.is_ascii_alphanumeric() {
            push_cjk_run_tokens(tokens, &mut cjk_run);
            ascii_token.push(ch.to_ascii_lowercase());
        } else if is_cjk_token_char(ch) {
            push_token(tokens, &mut ascii_token);
            cjk_run.push(ch);
        } else {
            push_token(tokens, &mut ascii_token);
            push_cjk_run_tokens(tokens, &mut cjk_run);
        }
    }
    push_token(tokens, &mut ascii_token);
    push_cjk_run_tokens(tokens, &mut cjk_run);
}

fn push_token(tokens: &mut BTreeSet<String>, token: &mut String) {
    if token.len() >= 2 {
        tokens.insert(std::mem::take(token));
    } else {
        token.clear();
    }
}

fn push_cjk_run_tokens(tokens: &mut BTreeSet<String>, run: &mut String) {
    if run.is_empty() {
        return;
    }
    let chars = run.chars().collect::<Vec<_>>();
    if chars.len() == 1 {
        tokens.insert(chars[0].to_string());
    } else {
        for pair in chars.windows(2) {
            tokens.insert(pair.iter().collect());
        }
    }
    run.clear();
}

fn is_cjk_token_char(ch: char) -> bool {
    matches!(
        ch as u32,
        0x3400..=0x4DBF
            | 0x4E00..=0x9FFF
            | 0x3040..=0x30FF
            | 0xAC00..=0xD7AF
            | 0xF900..=0xFAFF
    )
}

fn score_skill(
    skill: &SkillRecord,
    query_tokens: &BTreeSet<String>,
    query: &SkillSelectionQuery,
    fts_scores: &BTreeMap<String, usize>,
) -> Option<(usize, Vec<SkillScoreComponent>, Vec<String>)> {
    let mut score = 0;
    let mut components = Vec::new();
    let mut reasons = Vec::new();
    if let Some(reason) = frontmatter_gate_blocker(skill, query_tokens, query) {
        return if explicit_invocation_matches(skill, query) {
            reasons.push(format!("explicit invocation bypassed gate: {reason}"));
            Some((
                10_000,
                vec![SkillScoreComponent {
                    name: "explicit-invocation".to_string(),
                    score: 10_000,
                    matches: 1,
                }],
                reasons,
            ))
        } else {
            None
        };
    }

    let explicit_invocation = explicit_invocation_matches(skill, query);
    if explicit_invocation {
        score += 10_000;
        components.push(SkillScoreComponent {
            name: "explicit-invocation".to_string(),
            score: 10_000,
            matches: 1,
        });
        reasons.push("explicit skill invocation".to_string());
    }

    let id_tokens = token_set(&skill.original_id);
    let title_tokens = token_set(&skill.title);
    let description_tokens = skill
        .description
        .as_deref()
        .map(token_set)
        .unwrap_or_default();
    let keyword_tokens: BTreeSet<String> = skill.keywords.iter().cloned().collect();
    let tag_tokens = values_token_set(&skill.frontmatter.tags);
    let category_tokens = skill
        .frontmatter
        .category
        .as_deref()
        .map(token_set)
        .unwrap_or_default();

    let id_matches = count_matches(query_tokens, &id_tokens);
    if id_matches > 0 {
        let component_score = id_matches * 20;
        score += component_score;
        components.push(SkillScoreComponent {
            name: "id".to_string(),
            score: component_score,
            matches: id_matches,
        });
        reasons.push(format!("{id_matches} id token match(es)"));
    }

    let title_matches = count_matches(query_tokens, &title_tokens);
    if title_matches > 0 {
        let component_score = title_matches * 12;
        score += component_score;
        components.push(SkillScoreComponent {
            name: "title".to_string(),
            score: component_score,
            matches: title_matches,
        });
        reasons.push(format!("{title_matches} title token match(es)"));
    }

    let description_matches = count_matches(query_tokens, &description_tokens);
    if description_matches > 0 {
        let component_score = description_matches * 8;
        score += component_score;
        components.push(SkillScoreComponent {
            name: "description".to_string(),
            score: component_score,
            matches: description_matches,
        });
        reasons.push(format!("{description_matches} description token match(es)"));
    }

    let keyword_matches = count_matches(query_tokens, &keyword_tokens);
    if keyword_matches > 0 {
        let component_score = keyword_matches * 4;
        score += component_score;
        components.push(SkillScoreComponent {
            name: "body-keywords".to_string(),
            score: component_score,
            matches: keyword_matches,
        });
        reasons.push(format!("{keyword_matches} body keyword match(es)"));
    }

    let trigger_tokens = values_token_set(&skill.frontmatter.triggers);
    let trigger_matches = count_matches(query_tokens, &trigger_tokens);
    if trigger_matches > 0 {
        let component_score = trigger_matches * 16;
        score += component_score;
        components.push(SkillScoreComponent {
            name: "declared-triggers".to_string(),
            score: component_score,
            matches: trigger_matches,
        });
        reasons.push(format!("{trigger_matches} declared trigger match(es)"));
    }

    let tag_matches = count_matches(query_tokens, &tag_tokens);
    if tag_matches > 0 {
        let component_score = tag_matches * 10;
        score += component_score;
        components.push(SkillScoreComponent {
            name: "tags".to_string(),
            score: component_score,
            matches: tag_matches,
        });
        reasons.push(format!("{tag_matches} tag token match(es)"));
    }

    let category_matches = count_matches(query_tokens, &category_tokens);
    if category_matches > 0 {
        let component_score = category_matches * 6;
        score += component_score;
        components.push(SkillScoreComponent {
            name: "category".to_string(),
            score: component_score,
            matches: category_matches,
        });
        reasons.push(format!("{category_matches} category token match(es)"));
    }

    if query
        .agent_id
        .as_deref()
        .is_some_and(|agent_id| text_contains_token(&skill.original_id, agent_id))
    {
        score += 5;
        components.push(SkillScoreComponent {
            name: "agent-id".to_string(),
            score: 5,
            matches: 1,
        });
        reasons.push("agent id appears in skill id".to_string());
    }

    if query
        .channel
        .as_deref()
        .is_some_and(|channel| text_contains_token(&skill.original_id, channel))
    {
        score += 5;
        components.push(SkillScoreComponent {
            name: "channel".to_string(),
            score: 5,
            matches: 1,
        });
        reasons.push("channel appears in skill id".to_string());
    }

    if let Some(fts_score) = fts_scores
        .get(&skill.id)
        .copied()
        .filter(|score| *score > 0)
    {
        score += fts_score;
        components.push(SkillScoreComponent {
            name: "sqlite-fts5-bm25".to_string(),
            score: fts_score,
            matches: 1,
        });
        reasons.push("SQLite FTS5/BM25 match".to_string());
    }

    if query.usage_prior_enabled
        && let Some(snapshot) = query.usage_snapshot.as_ref()
    {
        let usage_score = usage_prior_score(snapshot, &skill.id);
        if usage_score > 0 {
            score += usage_score;
            components.push(SkillScoreComponent {
                name: "usage-prior".to_string(),
                score: usage_score,
                matches: usage_score,
            });
            reasons.push(format!("usage prior boost {usage_score}"));
        }
    }

    Some((score, components, reasons))
}

fn usage_prior_score(snapshot: &SkillUsageSnapshot, skill_id: &str) -> usize {
    snapshot
        .by_skill_action
        .get(skill_id)
        .map(|actions| {
            actions.get("injected").copied().unwrap_or(0)
                + actions.get("invoked").copied().unwrap_or(0)
        })
        .unwrap_or(0)
        .min(8)
}

fn sqlite_fts_scores(
    index: &SkillIndex,
    query_tokens: &BTreeSet<String>,
) -> io::Result<BTreeMap<String, usize>> {
    if query_tokens.is_empty() {
        return Ok(BTreeMap::new());
    }
    let conn = Connection::open_in_memory().map_err(io::Error::other)?;
    conn.execute_batch(
        "CREATE VIRTUAL TABLE skill_fts USING fts5(skill_id UNINDEXED, title, description, keywords);",
    )
    .map_err(io::Error::other)?;
    for skill in &index.skills {
        conn.execute(
            "INSERT INTO skill_fts(skill_id, title, description, keywords) VALUES (?1, ?2, ?3, ?4)",
            params![
                &skill.id,
                &skill.title,
                skill.description.as_deref().unwrap_or(""),
                skill.keywords.join(" ")
            ],
        )
        .map_err(io::Error::other)?;
    }
    let fts_query = query_tokens
        .iter()
        .map(|token| format!("\"{}\"", token.replace('"', "")))
        .collect::<Vec<_>>()
        .join(" OR ");
    let mut stmt = conn
        .prepare("SELECT skill_id, bm25(skill_fts) FROM skill_fts WHERE skill_fts MATCH ?1")
        .map_err(io::Error::other)?;
    let rows = stmt
        .query_map(params![fts_query], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, f64>(1)?))
        })
        .map_err(io::Error::other)?;
    let mut scores = BTreeMap::new();
    for row in rows {
        let (skill_id, rank) = row.map_err(io::Error::other)?;
        let score = (1000.0 / (1.0 + rank.abs())).round() as usize;
        scores.insert(skill_id, score.max(1));
    }
    Ok(scores)
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

fn values_token_set(values: &[String]) -> BTreeSet<String> {
    let mut tokens = BTreeSet::new();
    for value in values {
        extend_tokens(&mut tokens, value);
    }
    tokens
}

fn frontmatter_gate_blocker(
    skill: &SkillRecord,
    query_tokens: &BTreeSet<String>,
    query: &SkillSelectionQuery,
) -> Option<String> {
    if !skill.frontmatter.platforms.is_empty()
        && !query
            .channel
            .as_deref()
            .is_some_and(|channel| value_matches(&skill.frontmatter.platforms, channel))
    {
        return Some("platform gate did not match query channel".to_string());
    }
    if !skill.frontmatter.channels.is_empty()
        && !query
            .channel
            .as_deref()
            .is_some_and(|channel| value_matches(&skill.frontmatter.channels, channel))
    {
        return Some("channel gate did not match query channel".to_string());
    }
    if !skill.frontmatter.agent_modes.is_empty()
        && !query
            .agent_mode
            .as_deref()
            .is_some_and(|mode| value_matches(&skill.frontmatter.agent_modes, mode))
    {
        return Some("agent mode gate did not match".to_string());
    }
    if !skill.frontmatter.triggers.is_empty()
        && count_matches(query_tokens, &values_token_set(&skill.frontmatter.triggers)) == 0
    {
        return Some("declared triggers did not match".to_string());
    }
    if !skill.frontmatter.conditions.is_empty()
        && count_matches(
            query_tokens,
            &values_token_set(&skill.frontmatter.conditions),
        ) == 0
    {
        return Some("declared conditions did not match".to_string());
    }
    if !query.available_tools.is_empty()
        && !required_values_available(&skill.frontmatter.requires_tools, &query.available_tools)
    {
        return Some("required tools are not available".to_string());
    }
    if !query.available_toolsets.is_empty()
        && !required_values_available(
            &skill.frontmatter.requires_toolsets,
            &query.available_toolsets,
        )
    {
        return Some("required toolsets are not available".to_string());
    }
    None
}

fn selection_eligibility_blocker(
    skill: &SkillRecord,
    explicit_invocation: bool,
    agent_id: Option<&str>,
) -> Option<&'static str> {
    if skill.frontmatter.is_retired() {
        return Some("skill lifecycle is retired");
    }
    if !skill.frontmatter.agents.is_empty()
        && !agent_id.is_some_and(|agent_id| {
            skill
                .frontmatter
                .agents
                .iter()
                .any(|allowed| allowed.eq_ignore_ascii_case(agent_id))
        })
    {
        return Some("skill agent allowlist does not include selected agent");
    }
    if explicit_invocation && skill.frontmatter.user_invocable == Some(false) {
        return Some("skill disables user invocation");
    }
    if !explicit_invocation && skill.frontmatter.disable_model_invocation == Some(true) {
        return Some("skill disables model invocation");
    }
    None
}

fn value_matches(values: &[String], expected: &str) -> bool {
    values.iter().any(|value| {
        value.eq_ignore_ascii_case(expected)
            || token_set(value).contains(&expected.to_ascii_lowercase())
            || token_set(expected).contains(&value.to_ascii_lowercase())
    })
}

fn required_values_available(required: &[String], available: &[String]) -> bool {
    required.iter().all(|required| {
        available
            .iter()
            .any(|value| value_matches(&[required.clone()], value))
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExplicitSkillInvocation {
    skill_id: String,
    user_instruction: String,
}

impl ExplicitSkillInvocation {
    fn parse(text: &str) -> Option<Self> {
        parse_slash_skill_invocation(text).or_else(|| parse_dollar_skill_invocation(text))
    }
}

fn parse_slash_skill_invocation(text: &str) -> Option<ExplicitSkillInvocation> {
    let trimmed = text.trim_start();
    let rest = trimmed.strip_prefix("/skill")?;
    if !rest.is_empty() && !rest.starts_with(char::is_whitespace) {
        return None;
    }
    let rest = rest.trim_start();
    let mut parts = rest.splitn(2, char::is_whitespace);
    let skill_id = parts.next()?.trim();
    if skill_id.is_empty() {
        return None;
    }
    Some(ExplicitSkillInvocation {
        skill_id: skill_id.to_string(),
        user_instruction: parts.next().unwrap_or("").trim().to_string(),
    })
}

fn parse_dollar_skill_invocation(text: &str) -> Option<ExplicitSkillInvocation> {
    let bytes = text.as_bytes();
    for (index, ch) in text.char_indices() {
        if ch != '$' {
            continue;
        }
        if index > 0
            && bytes
                .get(index.saturating_sub(1))
                .is_some_and(|byte| byte.is_ascii_alphanumeric())
        {
            continue;
        }
        let after = index + ch.len_utf8();
        let rest = &text[after..];
        let mut end = rest.len();
        for (offset, item) in rest.char_indices() {
            if item.is_whitespace() {
                end = offset;
                break;
            }
        }
        let skill_id = &rest[..end];
        if skill_id.is_empty() {
            continue;
        }
        let user_instruction = rest[end..].trim();
        return Some(ExplicitSkillInvocation {
            skill_id: skill_id.to_string(),
            user_instruction: user_instruction.to_string(),
        });
    }
    None
}

fn explicit_invocation_matches(skill: &SkillRecord, query: &SkillSelectionQuery) -> bool {
    ExplicitSkillInvocation::parse(&query.text)
        .as_ref()
        .is_some_and(|invocation| skill_id_matches(skill, &invocation.skill_id))
}

pub fn skill_id_matches(skill: &SkillRecord, requested: &str) -> bool {
    let requested = requested.trim().trim_start_matches('$');
    if requested.is_empty() {
        return false;
    }
    skill.id.eq_ignore_ascii_case(requested)
        || skill.original_id.eq_ignore_ascii_case(requested)
        || skill
            .id
            .rsplit_once(':')
            .is_some_and(|(_, suffix)| suffix.eq_ignore_ascii_case(requested))
}

fn stable_selection_receipt_id(
    query_text: &str,
    skill_id: &str,
    delivery_mode: SkillDeliveryMode,
    body_checksum: &str,
) -> String {
    stable_text_hash(
        "skill-selection-item",
        &format!(
            "{query_text}|{skill_id}|{}|{body_checksum}",
            delivery_mode.as_str()
        ),
    )
}

fn stable_text_hash(namespace: &str, text: &str) -> String {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in namespace
        .as_bytes()
        .iter()
        .chain([0].iter())
        .chain(text.as_bytes())
    {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("fnv1a64:{hash:016x}")
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
            SkillSourceKind::AgentCreated => summary.agent_created_skills += 1,
            SkillSourceKind::Pack => summary.pack_skills += 1,
        }
        if let Some(category) = skill.frontmatter.category.as_ref() {
            *summary.by_category.entry(category.clone()).or_default() += 1;
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
    use crate::SkillUsageSnapshot;
    use std::collections::BTreeMap;
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
            build_source_skill_index(&AgentSource::with_workspace(&home, &workspace)).unwrap();

        assert_eq!(index.origin, SkillIndexOrigin::AgentSource);
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
    fn skill_frontmatter_v2_parses_nested_agent_harness_metadata() {
        let root = temp_root("skill_frontmatter_v2_parses_nested_agent_harness_metadata");
        let home = root.join(".openclaw");
        let workspace = home.join("workspace");
        let skill = workspace.join("skills").join("agent-windows-harness");
        fs::create_dir_all(&skill).unwrap();
        fs::write(
            skill.join(SKILL_FILE_NAME),
            "---\nname: agent-windows-harness\ndescription: Operate the Windows harness.\nversion: 0.1.22\nauthor: agent-harness-core\nrelatedSkills: [skill-authoring-standard]\nmetadata:\n  agent_harness:\n    category: operations\n    tags: [windows, cutover, health]\n---\n# Agent Windows Harness\n\nOperate the harness.\n",
        )
        .unwrap();

        let index =
            build_source_skill_index(&AgentSource::with_workspace(&home, &workspace)).unwrap();
        let record = index
            .skills
            .iter()
            .find(|skill| skill.original_id == "agent-windows-harness")
            .unwrap();

        assert_eq!(record.frontmatter.category.as_deref(), Some("operations"));
        assert_eq!(record.frontmatter.version.as_deref(), Some("0.1.22"));
        assert_eq!(
            record.frontmatter.author.as_deref(),
            Some("agent-harness-core")
        );
        assert!(record.frontmatter.tags.contains(&"cutover".to_string()));
        assert!(
            record
                .frontmatter
                .related_skills
                .contains(&"skill-authoring-standard".to_string())
        );
        assert!(record.keywords.contains(&"operations".to_string()));
        assert!(record.keywords.contains(&"cutover".to_string()));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn source_skill_index_discovers_one_level_category_dirs_and_excludes_archive() {
        let root =
            temp_root("source_skill_index_discovers_one_level_category_dirs_and_excludes_archive");
        let home = root.join(".openclaw");
        let workspace = home.join("workspace");
        let category_skill = workspace
            .join("skills")
            .join("trading")
            .join("neoapi-orders");
        let archived_skill = workspace.join("skills").join(".archive").join("old-skill");
        fs::create_dir_all(&category_skill).unwrap();
        fs::create_dir_all(&archived_skill).unwrap();
        fs::write(
            category_skill.join(SKILL_FILE_NAME),
            "---\ndescription: Place NeoAPI orders.\n---\n# NeoAPI Orders\n",
        )
        .unwrap();
        fs::write(archived_skill.join(SKILL_FILE_NAME), "# Old Skill\n").unwrap();

        let index =
            build_source_skill_index(&AgentSource::with_workspace(&home, &workspace)).unwrap();

        assert_eq!(index.summary.total_skills, 1);
        assert_eq!(index.skills[0].id, "workspace:neoapi-orders");
        assert_eq!(
            index.skills[0].frontmatter.category.as_deref(),
            Some("trading")
        );
        assert_eq!(index.summary.by_category.get("trading"), Some(&1));

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
    fn harness_skill_index_discovers_openclaw_imported_namespace() {
        let root = temp_root("harness_skill_index_discovers_openclaw_imported_namespace");
        let harness_home = root.join("harness-home");
        let imported_skill = harness_home
            .join("skills")
            .join(OPENCLAW_IMPORTED_SKILL_NAMESPACE)
            .join("workspace")
            .join("xiaoxiaoli-neoapi-guardrails");
        fs::create_dir_all(&imported_skill).unwrap();
        fs::write(
            imported_skill.join(SKILL_FILE_NAME),
            "# Xiaoxiaoli NeoAPI Guardrails\n\nKeep NeoAPI order guidance bounded.",
        )
        .unwrap();

        let index = build_harness_skill_index(&harness_home).unwrap();

        assert_eq!(index.origin, SkillIndexOrigin::HarnessImport);
        assert_eq!(index.summary.total_skills, 1);
        assert_eq!(index.summary.imported_workspace_skills, 1);
        assert_eq!(
            index.skills[0].id,
            "imported-workspace:xiaoxiaoli-neoapi-guardrails"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn harness_skill_index_discovers_builtin_namespace() {
        let root = temp_root("harness_skill_index_discovers_builtin_namespace");
        let harness_home = root.join("harness-home");
        let builtin_skill = harness_home
            .join("skills")
            .join(HARNESS_BUILTIN_SKILL_NAMESPACE)
            .join("agent-windows-harness");
        fs::create_dir_all(&builtin_skill).unwrap();
        fs::write(
            builtin_skill.join(SKILL_FILE_NAME),
            "# Agent Windows Harness\n\nOperate the Rust harness.",
        )
        .unwrap();

        let index = build_harness_skill_index(&harness_home).unwrap();

        assert_eq!(index.summary.total_skills, 1);
        assert_eq!(index.summary.harness_builtin_skills, 1);
        assert_eq!(index.skills[0].id, "harness-builtin:agent-windows-harness");

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
            .join("agent-windows-harness");
        fs::create_dir_all(&workspace_skill).unwrap();
        fs::create_dir_all(&builtin_skill).unwrap();
        fs::write(
            workspace_skill.join(SKILL_FILE_NAME),
            "# Memory Cron\n\nRepair openclaw-mem jobs.",
        )
        .unwrap();
        fs::write(
            builtin_skill.join(SKILL_FILE_NAME),
            "# Agent Windows Harness\n\nOperate Telegram and Discord handoff.",
        )
        .unwrap();

        let index = build_runtime_skill_index(
            &AgentSource::with_workspace(&home, &workspace),
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
                .any(|skill| skill.id == "harness-builtin:agent-windows-harness")
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
            build_source_skill_index(&AgentSource::with_workspace(&home, &workspace)).unwrap();

        let selections = select_skills(
            &index,
            &SkillSelectionQuery {
                text: "fix openclaw mem cron delivery runner".to_string(),
                agent_id: Some("mem-cron".to_string()),
                channel: None,
                workspace: None,
                agent_mode: None,
                available_tools: Vec::new(),
                available_toolsets: Vec::new(),
                fts_enabled: false,
                vector_tie_break_enabled: false,
                usage_snapshot: None,
                usage_prior_enabled: false,
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
    fn skill_selection_scores_tags_category_and_bounded_usage_prior() {
        let root = temp_root("skill_selection_scores_tags_category_and_bounded_usage_prior");
        let home = root.join(".openclaw");
        let workspace = home.join("workspace");
        let frequent_skill = workspace.join("skills").join("orders-primer");
        let idle_skill = workspace.join("skills").join("orders-review");
        let lexical_skill = workspace.join("skills").join("neoapi-direct-orders");
        fs::create_dir_all(&frequent_skill).unwrap();
        fs::create_dir_all(&idle_skill).unwrap();
        fs::create_dir_all(&lexical_skill).unwrap();
        fs::write(
            frequent_skill.join(SKILL_FILE_NAME),
            "---\ncategory: trading\ntags: [orders]\n---\n# Orders Primer\n\nHandle order workflows.\n",
        )
        .unwrap();
        fs::write(
            idle_skill.join(SKILL_FILE_NAME),
            "---\ncategory: trading\ntags: [orders]\n---\n# Orders Review\n\nHandle order workflows.\n",
        )
        .unwrap();
        fs::write(
            lexical_skill.join(SKILL_FILE_NAME),
            "---\ncategory: trading\ntags: [orders]\n---\n# NeoAPI Direct Orders\n\nUse direct neoapi order routing.\n",
        )
        .unwrap();
        let index =
            build_source_skill_index(&AgentSource::with_workspace(&home, &workspace)).unwrap();
        let mut by_skill_action = BTreeMap::new();
        by_skill_action.insert(
            "workspace:orders-primer".to_string(),
            BTreeMap::from([
                ("injected".to_string(), 20usize),
                ("invoked".to_string(), 5usize),
            ]),
        );
        let snapshot = SkillUsageSnapshot {
            schema: "agent-harness.skill-usage-snapshot.v1".to_string(),
            harness_home: home.clone(),
            events_file: home
                .join("state")
                .join("learning")
                .join("skill-usage.jsonl"),
            total_events: 25,
            by_action: BTreeMap::new(),
            by_skill: BTreeMap::new(),
            by_skill_action,
            by_provenance: BTreeMap::new(),
            latest_at_ms: Some(25),
        };

        let selections = select_skills(
            &index,
            &SkillSelectionQuery {
                text: "neoapi trading orders".to_string(),
                agent_id: None,
                channel: None,
                workspace: None,
                agent_mode: None,
                available_tools: Vec::new(),
                available_toolsets: Vec::new(),
                fts_enabled: false,
                vector_tie_break_enabled: false,
                usage_snapshot: Some(snapshot),
                usage_prior_enabled: true,
                limit: 3,
            },
        );

        assert_eq!(selections[0].skill_id, "workspace:neoapi-direct-orders");
        let frequent = selections
            .iter()
            .find(|skill| skill.skill_id == "workspace:orders-primer")
            .unwrap();
        let idle = selections
            .iter()
            .find(|skill| skill.skill_id == "workspace:orders-review")
            .unwrap();
        assert!(frequent.score > idle.score);
        assert!(
            frequent
                .score_components
                .iter()
                .any(|component| component.name == "usage-prior" && component.score == 8)
        );
        assert!(
            frequent
                .score_components
                .iter()
                .any(|component| component.name == "tags")
        );
        assert!(
            frequent
                .score_components
                .iter()
                .any(|component| component.name == "category")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn skill_selection_frontmatter_gates_and_writes_receipt() {
        let root = temp_root("skill_selection_frontmatter_gates_and_writes_receipt");
        let home = root.join(".openclaw");
        let workspace = home.join("workspace");
        let telegram_skill = workspace.join("skills").join("telegram-media");
        let discord_skill = workspace.join("skills").join("discord-media");
        fs::create_dir_all(&telegram_skill).unwrap();
        fs::create_dir_all(&discord_skill).unwrap();
        fs::write(
            telegram_skill.join(SKILL_FILE_NAME),
            "---\ntriggers: [photo, media]\nchannels: [telegram]\ndeliveryMode: index-only\n---\n# Telegram Media\n\nHandle Telegram media downloads.",
        )
        .unwrap();
        fs::write(
            discord_skill.join(SKILL_FILE_NAME),
            "---\ntriggers: [photo, media]\nchannels: [discord]\n---\n# Discord Media\n\nHandle Discord media downloads.",
        )
        .unwrap();
        let index =
            build_source_skill_index(&AgentSource::with_workspace(&home, &workspace)).unwrap();
        let query = SkillSelectionQuery {
            text: "handle inbound photo media".to_string(),
            agent_id: Some("main".to_string()),
            channel: Some("telegram".to_string()),
            workspace: None,
            agent_mode: None,
            available_tools: Vec::new(),
            available_toolsets: Vec::new(),
            fts_enabled: false,
            vector_tie_break_enabled: false,
            usage_snapshot: None,
            usage_prior_enabled: false,
            limit: 5,
        };
        let selections = select_skills(&index, &query);
        assert_eq!(selections.len(), 1);
        assert_eq!(selections[0].skill_id, "workspace:telegram-media");
        assert_eq!(selections[0].delivery_mode, SkillDeliveryMode::IndexOnly);
        assert!(
            selections[0]
                .score_components
                .iter()
                .any(|component| component.name == "declared-triggers")
        );

        let receipt = write_skill_selection_receipt(&home, &query, &selections).unwrap();
        assert_eq!(receipt.matcher_version, SKILL_MATCHER_VERSION);
        let receipts = fs::read_to_string(skill_selection_receipts_file(&home)).unwrap();
        assert!(receipts.contains(SKILL_SELECTION_RECEIPT_SCHEMA));
        assert!(receipts.contains("telegram-media"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn skill_selection_honors_retirement_and_invocation_controls() {
        let root = temp_root("skill_selection_honors_retirement_and_invocation_controls");
        let home = root.join(".openclaw");
        let workspace = home.join("workspace");
        let retired = workspace.join("skills").join("retired-skill");
        let historical = workspace.join("skills").join("retired-historical-skill");
        let model_disabled = workspace.join("skills").join("model-disabled-skill");
        let user_disabled = workspace.join("skills").join("user-disabled-skill");
        let active = workspace.join("skills").join("active-skill");
        for skill in [
            &retired,
            &historical,
            &model_disabled,
            &user_disabled,
            &active,
        ] {
            fs::create_dir_all(skill).unwrap();
        }
        fs::write(
            retired.join(SKILL_FILE_NAME),
            "---\nlifecycle: retired\n---\n# Retired Skill\n\nHandle lifecycle policy regression checks.\n",
        )
        .unwrap();
        fs::write(
            historical.join(SKILL_FILE_NAME),
            "---\nlifecycle: retired_historical\n---\n# Retired Historical Skill\n\nHandle lifecycle policy regression checks.\n",
        )
        .unwrap();
        fs::write(
            model_disabled.join(SKILL_FILE_NAME),
            "---\ndisable-model-invocation: true\n---\n# Model Disabled Skill\n\nHandle lifecycle policy regression checks.\n",
        )
        .unwrap();
        fs::write(
            user_disabled.join(SKILL_FILE_NAME),
            "---\nuser-invocable: false\n---\n# User Disabled Skill\n\nHandle lifecycle policy regression checks.\n",
        )
        .unwrap();
        fs::write(
            active.join(SKILL_FILE_NAME),
            "# Active Skill\n\nHandle lifecycle policy regression checks.\n",
        )
        .unwrap();

        let index =
            build_source_skill_index(&AgentSource::with_workspace(&home, &workspace)).unwrap();
        let select = |text: &str| {
            select_skills(
                &index,
                &SkillSelectionQuery {
                    text: text.to_string(),
                    agent_id: None,
                    channel: None,
                    workspace: None,
                    agent_mode: None,
                    available_tools: Vec::new(),
                    available_toolsets: Vec::new(),
                    fts_enabled: false,
                    vector_tie_break_enabled: false,
                    usage_snapshot: None,
                    usage_prior_enabled: false,
                    limit: 10,
                },
            )
        };

        let automatic_ids = select("handle lifecycle policy regression checks")
            .into_iter()
            .map(|selection| selection.skill_id)
            .collect::<Vec<_>>();
        assert!(automatic_ids.contains(&"workspace:active-skill".to_string()));
        assert!(automatic_ids.contains(&"workspace:user-disabled-skill".to_string()));
        assert!(!automatic_ids.contains(&"workspace:retired-skill".to_string()));
        assert!(!automatic_ids.contains(&"workspace:retired-historical-skill".to_string()));
        assert!(!automatic_ids.contains(&"workspace:model-disabled-skill".to_string()));

        assert!(select("$retired-skill run it").is_empty());
        assert!(select("$retired-historical-skill run it").is_empty());
        assert!(select("$user-disabled-skill run it").is_empty());

        let explicit = select("$model-disabled-skill run it");
        assert_eq!(explicit.len(), 1);
        assert_eq!(explicit[0].skill_id, "workspace:model-disabled-skill");
        assert_eq!(
            explicit[0].delivery_mode,
            SkillDeliveryMode::InvocationEnvelope
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn skill_selection_agent_allowlist_is_fail_closed_for_model_and_explicit_invocation() {
        let root = temp_root("skill_selection_agent_allowlist_is_fail_closed");
        let home = root.join(".openclaw");
        let workspace = home.join("workspace");
        let skill = workspace.join("skills").join("xiaoxiaoli-coach");
        fs::create_dir_all(&skill).unwrap();
        fs::write(
            skill.join(SKILL_FILE_NAME),
            "---\nagents: [xiaoxiaoli]\n---\n# Xiaoxiaoli Coach\n\nHandle cross agent coaching.\n",
        )
        .unwrap();
        let index =
            build_source_skill_index(&AgentSource::with_workspace(&home, &workspace)).unwrap();
        let select = |agent_id: &str, text: &str| {
            select_skills(
                &index,
                &SkillSelectionQuery {
                    text: text.to_string(),
                    agent_id: Some(agent_id.to_string()),
                    channel: None,
                    workspace: None,
                    agent_mode: None,
                    available_tools: Vec::new(),
                    available_toolsets: Vec::new(),
                    fts_enabled: false,
                    vector_tie_break_enabled: false,
                    usage_snapshot: None,
                    usage_prior_enabled: false,
                    limit: 10,
                },
            )
        };

        assert!(select("main", "handle cross agent coaching").is_empty());
        assert!(select("main", "$xiaoxiaoli-coach run it").is_empty());
        assert_eq!(
            select("xiaoxiaoli", "handle cross agent coaching")[0].skill_id,
            "workspace:xiaoxiaoli-coach"
        );
        assert_eq!(
            select("xiaoxiaoli", "$xiaoxiaoli-coach run it")[0].delivery_mode,
            SkillDeliveryMode::InvocationEnvelope
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
            build_source_skill_index(&AgentSource::with_workspace(&home, &workspace)).unwrap();
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
            "agent-harness-skills-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
