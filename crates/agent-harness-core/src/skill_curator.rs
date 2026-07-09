use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    AgentSource, HARNESS_BUILTIN_SKILL_NAMESPACE, SKILL_FILE_NAME, SkillLearningProposalOperation,
    SkillLearningProposalStatus, SkillProposeOptions, SkillRecord, SkillUsageAction,
    build_runtime_skill_index, create_skill_learning_proposal, current_log_time_ms,
    skill_proposals_file, write_json_atomic,
};

const SKILL_LIFECYCLE_SCHEMA: &str = "agent-harness.skill-lifecycle.v1";
const SKILL_CURATOR_REPORT_SCHEMA: &str = "agent-harness.skill-curator.v1";
const SKILL_RESTORE_REPORT_SCHEMA: &str = "agent-harness.skill-restore.v1";
const SKILL_PIN_REPORT_SCHEMA: &str = "agent-harness.skill-pin.v1";

const DAY_MS: i64 = 86_400_000;
const DEFAULT_STALE_AFTER_DAYS: u64 = 30;
const DEFAULT_ARCHIVE_AFTER_DAYS: u64 = 90;
const DEFAULT_NEVER_USED_GRACE_DAYS: u64 = 7;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SkillLifecycleState {
    Active,
    Stale,
    Archived,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillLifecycleRecord {
    pub skill_id: String,
    pub state: SkillLifecycleState,
    pub pinned: bool,
    pub first_seen_at_ms: i64,
    pub last_active_at_ms: Option<i64>,
    pub archived_at_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub archived_from: Option<PathBuf>,
}

impl SkillLifecycleRecord {
    fn new(skill_id: impl Into<String>, now_ms: i64) -> Self {
        Self {
            skill_id: skill_id.into(),
            state: SkillLifecycleState::Active,
            pinned: false,
            first_seen_at_ms: now_ms,
            last_active_at_ms: None,
            archived_at_ms: None,
            archived_from: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillLifecycleStore {
    pub schema: String,
    pub harness_home: PathBuf,
    pub skills: BTreeMap<String, SkillLifecycleRecord>,
}

impl SkillLifecycleStore {
    fn empty(harness_home: impl AsRef<Path>) -> Self {
        Self {
            schema: SKILL_LIFECYCLE_SCHEMA.to_string(),
            harness_home: harness_home.as_ref().to_path_buf(),
            skills: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillCuratorOptions {
    pub harness_home: PathBuf,
    pub stale_after_days: u64,
    pub archive_after_days: u64,
    pub never_used_grace_days: u64,
    pub include_builtins: bool,
    pub target_skill_ids: Option<Vec<String>>,
    pub dry_run: bool,
    pub now_ms: i64,
}

impl Default for SkillCuratorOptions {
    fn default() -> Self {
        Self {
            harness_home: PathBuf::from(".agent-harness"),
            stale_after_days: DEFAULT_STALE_AFTER_DAYS,
            archive_after_days: DEFAULT_ARCHIVE_AFTER_DAYS,
            never_used_grace_days: DEFAULT_NEVER_USED_GRACE_DAYS,
            include_builtins: false,
            target_skill_ids: None,
            dry_run: false,
            now_ms: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillCuratorCluster {
    pub cluster_type: String,
    pub key: String,
    pub skills: Vec<String>,
    pub rationale: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillCuratorReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub status: String,
    pub now_ms: i64,
    pub dry_run: bool,
    pub evaluated: usize,
    pub active: usize,
    pub stale: usize,
    pub archived: usize,
    pub pinned: usize,
    pub skipped_builtins: usize,
    pub proposals_created: usize,
    pub proposal_ids: Vec<String>,
    pub reason: String,
    pub clusters: Vec<SkillCuratorCluster>,
    pub report_file: Option<PathBuf>,
}

impl SkillCuratorReport {
    fn new(harness_home: PathBuf, dry_run: bool, now_ms: i64) -> Self {
        Self {
            schema: SKILL_CURATOR_REPORT_SCHEMA,
            harness_home,
            status: if dry_run {
                "dry-run".to_string()
            } else {
                "skipped".to_string()
            },
            now_ms,
            dry_run,
            evaluated: 0,
            active: 0,
            stale: 0,
            archived: 0,
            pinned: 0,
            skipped_builtins: 0,
            proposals_created: 0,
            proposal_ids: Vec::new(),
            reason: String::new(),
            clusters: Vec::new(),
            report_file: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillRestoreOptions {
    pub harness_home: PathBuf,
    pub target_skill_id: String,
    pub target_path: Option<PathBuf>,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillRestoreReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub target_skill_id: String,
    pub archived_from: PathBuf,
    pub restored_to: PathBuf,
    pub restored_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillPinOptions {
    pub harness_home: PathBuf,
    pub target_skill_id: String,
    pub pinned: bool,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillPinReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub target_skill_id: String,
    pub pinned: bool,
    pub now_ms: i64,
}

#[derive(Debug, Clone)]
struct CandidateSkill {
    pub skill_id: String,
    pub category: Option<String>,
    pub tags: Vec<String>,
}

impl From<&SkillRecord> for CandidateSkill {
    fn from(record: &SkillRecord) -> Self {
        Self {
            skill_id: record.id.clone(),
            category: record.frontmatter.category.clone(),
            tags: record.frontmatter.tags.clone(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct LifecycleDecision {
    state: SkillLifecycleState,
    archive_due: bool,
}

pub fn skill_lifecycle_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("skills")
        .join("skill-lifecycle.json")
}

pub fn skill_curator_receipts_dir(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("skills")
        .join("curator")
}

fn archived_dir_root(harness_home: &Path) -> PathBuf {
    harness_home.join(".archive")
}

fn archived_skill_dir(harness_home: &Path, skill_id: &str) -> PathBuf {
    archived_dir_root(harness_home).join(skill_leaf(skill_id))
}

pub fn read_skill_lifecycle_store(
    harness_home: impl AsRef<Path>,
) -> io::Result<SkillLifecycleStore> {
    let path = skill_lifecycle_file(&harness_home);
    let text = match fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(SkillLifecycleStore::empty(&harness_home));
        }
        Err(error) => return Err(error),
    };
    let mut store: SkillLifecycleStore = match serde_json::from_str(&text) {
        Ok(store) => store,
        Err(_) => SkillLifecycleStore::empty(&harness_home),
    };
    store.harness_home = harness_home.as_ref().to_path_buf();
    Ok(store)
}

pub fn write_skill_lifecycle_store(
    harness_home: impl AsRef<Path>,
    store: &SkillLifecycleStore,
) -> io::Result<()> {
    let path = skill_lifecycle_file(&harness_home);
    let dir = path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid path"))?;
    fs::create_dir_all(dir)?;
    write_json_atomic(&path, store).map_err(io::Error::other)
}

pub fn skill_id_matches_target(filter: &Option<Vec<String>>, skill_id: &str) -> bool {
    filter
        .as_ref()
        .is_none_or(|targets| targets.iter().any(|target| target == skill_id))
}

pub fn run_skill_curator(mut options: SkillCuratorOptions) -> io::Result<SkillCuratorReport> {
    if options.now_ms <= 0 {
        options.now_ms = current_log_time_ms().unwrap_or(0);
    }
    let stale_after_days = if options.stale_after_days == 0 {
        DEFAULT_STALE_AFTER_DAYS
    } else {
        options.stale_after_days
    };
    let archive_after_days = if options.archive_after_days == 0 {
        DEFAULT_ARCHIVE_AFTER_DAYS
    } else {
        options.archive_after_days
    }
    .max(stale_after_days);
    let never_used_grace_days = if options.never_used_grace_days == 0 {
        DEFAULT_NEVER_USED_GRACE_DAYS
    } else {
        options.never_used_grace_days
    };
    let stale_cutoff_ms = stale_after_days.saturating_mul(DAY_MS as u64) as i64;
    let archive_cutoff_ms = archive_after_days.saturating_mul(DAY_MS as u64) as i64;
    let grace_cutoff_ms = never_used_grace_days.saturating_mul(DAY_MS as u64) as i64;

    let source = AgentSource::new(&options.harness_home);
    let index = build_runtime_skill_index(&source, &options.harness_home)?;
    let mut state = read_skill_lifecycle_store(&options.harness_home)?;
    let usage = read_last_active_skill_events(&options.harness_home)?;
    let open_archive_proposals = read_open_archive_proposals(&options.harness_home)?;

    let mut report = SkillCuratorReport::new(
        options.harness_home.clone(),
        options.dry_run,
        options.now_ms,
    );
    let mut pending_updates = false;
    let target_skills = options
        .target_skill_ids
        .clone()
        .map(|targets| targets.into_iter().collect::<BTreeSet<_>>());

    for skill in &index.skills {
        if target_skills
            .as_ref()
            .is_some_and(|targets| !targets.contains(&skill.id))
        {
            continue;
        }
        report.evaluated += 1;
        if !options.include_builtins && is_builtin_skill_id(&skill.id) {
            report.skipped_builtins += 1;
            continue;
        }
        let last_activity_ms = usage.get(&skill.id).copied();
        let entry = state.skills.entry(skill.id.clone()).or_insert_with(|| {
            report.active += 1;
            SkillLifecycleRecord::new(skill.id.clone(), options.now_ms)
        });
        let before = entry.clone();
        let decision = decide_lifecycle_state(
            entry,
            last_activity_ms,
            options.now_ms,
            stale_cutoff_ms,
            archive_cutoff_ms,
            grace_cutoff_ms,
        );

        if entry.state == SkillLifecycleState::Archived && last_activity_ms.is_some() {
            entry.state = SkillLifecycleState::Active;
            entry.archived_at_ms = None;
            entry.archived_from = None;
        }

        entry.state = decision.state;
        if let Some(last_activity_ms) = last_activity_ms {
            entry.last_active_at_ms = Some(
                entry
                    .last_active_at_ms
                    .unwrap_or(last_activity_ms)
                    .max(last_activity_ms),
            );
        }
        if !entry.pinned && decision.archive_due && !options.dry_run {
            if !open_archive_proposals.contains(&skill.id) {
                let proposal = create_skill_learning_proposal(SkillProposeOptions {
                    harness_home: options.harness_home.clone(),
                    target_skill_id: skill.id.clone(),
                    target_path: skill.skill_file.clone(),
                    operation: SkillLearningProposalOperation::Archive,
                    replacement_body: None,
                    support_files: Vec::new(),
                    diff: Some(format!(
                        "no active usage in {} days (skill lifecycle archive threshold {} days)",
                        archive_cutoff_ms / DAY_MS,
                        archive_after_days
                    )),
                    signals: Vec::new(),
                    source_turn: None,
                    risk_class: "low".to_string(),
                    status: SkillLearningProposalStatus::Proposed,
                    now_ms: options.now_ms,
                })?;
                report.proposals_created += 1;
                report.proposal_ids.push(proposal.proposal_id);
            }
        }

        match entry.state {
            SkillLifecycleState::Active => report.active += 1,
            SkillLifecycleState::Stale => report.stale += 1,
            SkillLifecycleState::Archived => report.archived += 1,
        }
        if entry.pinned {
            report.pinned += 1;
        }

        if entry.pinned {
            continue;
        }
        if before != *entry {
            pending_updates = true;
        }
    }

    let candidates = index
        .skills
        .iter()
        .map(CandidateSkill::from)
        .collect::<Vec<_>>();
    report.clusters = build_curator_clusters(&candidates, 2);
    if pending_updates {
        write_skill_lifecycle_store(&options.harness_home, &state)?;
    }
    if report.proposals_created == 0 {
        report.status = if options.dry_run {
            "dry-run".to_string()
        } else {
            "skipped".to_string()
        };
        report.reason = "no archive proposal due to activity or pinning constraints".to_string();
    } else {
        report.status = if options.dry_run {
            "dry-run".to_string()
        } else {
            "proposed".to_string()
        };
        report.reason = "archive proposal(s) recorded by curator".to_string();
    }

    report.report_file = Some(write_curator_receipt(&options.harness_home, &report)?);
    Ok(report)
}

pub fn move_skill_to_archive(
    harness_home: &Path,
    target_skill_id: &str,
    target_path: &Path,
) -> io::Result<PathBuf> {
    let target_dir = target_path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "target skill path has no parent directory",
        )
    })?;
    if !target_dir.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "skill directory for archive does not exist",
        ));
    }
    let archive_root = archived_dir_root(harness_home);
    fs::create_dir_all(&archive_root)?;
    let archived_dir = archived_skill_dir(harness_home, target_skill_id);
    if archived_dir.exists() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "archived skill directory already exists",
        ));
    }
    fs::rename(target_dir, &archived_dir)?;
    Ok(archived_dir)
}

pub fn restore_skill_from_archive(options: SkillRestoreOptions) -> io::Result<SkillRestoreReport> {
    let now_ms = if options.now_ms <= 0 {
        current_log_time_ms().unwrap_or(0)
    } else {
        options.now_ms
    };
    let mut state = read_skill_lifecycle_store(&options.harness_home)?;
    let restored_location = state
        .skills
        .get(&options.target_skill_id)
        .and_then(|record| record.archived_from.clone())
        .or_else(|| options.target_path.clone());
    let restored_location = restored_location.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "cannot determine archived skill source path",
        )
    })?;
    let archived_dir = archived_skill_dir(&options.harness_home, &options.target_skill_id);
    let archived_dir = if archived_dir.is_dir() {
        archived_dir
    } else if let Some(target_path) = options.target_path.as_ref() {
        if target_path.is_dir() && is_within_archive_root(&options.harness_home, target_path) {
            target_path.clone()
        } else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "archived directory does not exist",
            ));
        }
    } else {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "archived directory does not exist",
        ));
    };
    let destination = restored_location;
    if destination.exists() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "restore target path already exists",
        ));
    }
    if !archived_dir.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "archived source is not a directory",
        ));
    }
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)?;
    } else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid restore target path",
        ));
    }
    fs::rename(&archived_dir, &destination)?;

    let restored_skill = destination.join(SKILL_FILE_NAME);
    if !restored_skill.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "restored skill directory is missing SKILL.md",
        ));
    }

    let record = state
        .skills
        .entry(options.target_skill_id.clone())
        .or_insert_with(|| SkillLifecycleRecord::new(options.target_skill_id.clone(), now_ms));
    record.state = SkillLifecycleState::Active;
    record.archived_at_ms = None;
    record.archived_from = Some(destination.clone());
    write_skill_lifecycle_store(&options.harness_home, &state)?;

    Ok(SkillRestoreReport {
        schema: SKILL_RESTORE_REPORT_SCHEMA,
        harness_home: options.harness_home,
        target_skill_id: options.target_skill_id,
        archived_from: archived_dir,
        restored_to: destination,
        restored_at_ms: now_ms,
    })
}

pub fn set_skill_pin(options: SkillPinOptions) -> io::Result<SkillPinReport> {
    let now_ms = if options.now_ms <= 0 {
        current_log_time_ms().unwrap_or(0)
    } else {
        options.now_ms
    };
    let mut state = read_skill_lifecycle_store(&options.harness_home)?;
    let record = state
        .skills
        .entry(options.target_skill_id.clone())
        .or_insert_with(|| SkillLifecycleRecord::new(options.target_skill_id.clone(), now_ms));
    record.pinned = options.pinned;
    write_skill_lifecycle_store(&options.harness_home, &state)?;

    Ok(SkillPinReport {
        schema: SKILL_PIN_REPORT_SCHEMA,
        harness_home: options.harness_home,
        target_skill_id: options.target_skill_id,
        pinned: options.pinned,
        now_ms,
    })
}

pub fn mark_skill_archived(
    harness_home: impl AsRef<Path>,
    target_skill_id: &str,
    archived_from: &Path,
    now_ms: i64,
) -> io::Result<()> {
    let now_ms = if now_ms <= 0 {
        current_log_time_ms().unwrap_or(0)
    } else {
        now_ms
    };
    let mut state = read_skill_lifecycle_store(&harness_home)?;
    let record = state
        .skills
        .entry(target_skill_id.to_string())
        .or_insert_with(|| SkillLifecycleRecord::new(target_skill_id.to_string(), now_ms));
    record.state = SkillLifecycleState::Archived;
    record.archived_at_ms = Some(now_ms);
    record.archived_from = Some(archived_from.to_path_buf());
    write_skill_lifecycle_store(&harness_home, &state)
}

fn build_curator_clusters(
    skills: &[CandidateSkill],
    min_cluster_size: usize,
) -> Vec<SkillCuratorCluster> {
    let mut clusters = Vec::new();
    let mut prefix_groups: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut overlap_groups: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();

    for skill in skills {
        if let Some(prefix) = pr_prefix_cluster_key(&skill.skill_id) {
            prefix_groups
                .entry(format!("prefix:{prefix}"))
                .or_default()
                .insert(skill.skill_id.clone());
        }
        if let Some(category) = skill
            .category
            .as_ref()
            .filter(|value| !value.trim().is_empty())
        {
            overlap_groups
                .entry(format!("category:{category}"))
                .or_default()
                .insert(skill.skill_id.clone());
        }
        for tag in &skill.tags {
            if tag.trim().is_empty() {
                continue;
            }
            overlap_groups
                .entry(format!("tag:{tag}"))
                .or_default()
                .insert(skill.skill_id.clone());
        }
    }

    for (cluster_key, ids) in prefix_groups.into_iter() {
        if ids.len() >= min_cluster_size {
            clusters.push(SkillCuratorCluster {
                cluster_type: "pr-prefix".to_string(),
                key: cluster_key,
                skills: ids.into_iter().collect(),
                rationale: "shared pr-* prefix".to_string(),
            });
        }
    }
    for (cluster_key, ids) in overlap_groups.into_iter() {
        if ids.len() >= min_cluster_size {
            clusters.push(SkillCuratorCluster {
                cluster_type: "category-tag-overlap".to_string(),
                key: cluster_key,
                skills: ids.into_iter().collect(),
                rationale: "shared category or tag".to_string(),
            });
        }
    }
    clusters
}

fn write_curator_receipt(
    harness_home: impl AsRef<Path>,
    report: &SkillCuratorReport,
) -> io::Result<PathBuf> {
    let dir = skill_curator_receipts_dir(&harness_home);
    fs::create_dir_all(&dir)?;
    let file = dir.join(format!(
        "{}-skill-curator-report.json",
        timestamp_label(report.now_ms)
    ));
    write_json_atomic(&file, report)?;
    Ok(file)
}

fn read_last_active_skill_events(
    harness_home: impl AsRef<Path>,
) -> io::Result<BTreeMap<String, i64>> {
    let path = crate::skill_usage_events_file(&harness_home);
    let text = match fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(error) => return Err(error),
    };

    let mut usage = BTreeMap::new();
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let Ok(event) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let Some(action) = event
            .get("action")
            .and_then(Value::as_str)
            .and_then(skill_usage_action_from_str)
        else {
            continue;
        };
        if !is_activity_action(action) {
            continue;
        }
        let Some(skill_id) = event
            .get("skillId")
            .and_then(Value::as_str)
            .map(str::to_string)
        else {
            continue;
        };
        let Some(at_ms) = event.get("atMs").and_then(Value::as_i64) else {
            continue;
        };
        let entry = usage.entry(skill_id).or_insert(at_ms);
        if at_ms > *entry {
            *entry = at_ms;
        }
    }
    Ok(usage)
}

fn read_open_archive_proposals(harness_home: impl AsRef<Path>) -> io::Result<BTreeSet<String>> {
    let text = match fs::read_to_string(skill_proposals_file(&harness_home)) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(BTreeSet::new()),
        Err(error) => return Err(error),
    };
    let mut ids = BTreeSet::new();
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let is_archive = value.get("operation").and_then(Value::as_str) == Some("archive");
        let is_proposed = value.get("status").and_then(Value::as_str) == Some("proposed");
        if is_archive && is_proposed {
            if let Some(skill_id) = value.get("targetSkillId").and_then(Value::as_str) {
                ids.insert(skill_id.to_string());
            }
        }
    }
    Ok(ids)
}

fn decide_lifecycle_state(
    record: &SkillLifecycleRecord,
    last_activity_ms: Option<i64>,
    now_ms: i64,
    stale_cutoff_ms: i64,
    archive_cutoff_ms: i64,
    never_used_grace_ms: i64,
) -> LifecycleDecision {
    if record.pinned {
        return LifecycleDecision {
            state: SkillLifecycleState::Active,
            archive_due: false,
        };
    }

    let mut last_active_ms = last_activity_ms.or(record.last_active_at_ms);
    if last_active_ms.is_none() {
        last_active_ms = Some(record.first_seen_at_ms);
    }
    let Some(last_active_ms) = last_active_ms else {
        return LifecycleDecision {
            state: SkillLifecycleState::Active,
            archive_due: false,
        };
    };

    let age_ms = now_ms.saturating_sub(last_active_ms);
    if age_ms >= archive_cutoff_ms {
        return LifecycleDecision {
            state: SkillLifecycleState::Stale,
            archive_due: true,
        };
    }
    if age_ms >= stale_cutoff_ms {
        return LifecycleDecision {
            state: SkillLifecycleState::Stale,
            archive_due: false,
        };
    }

    if age_ms >= never_used_grace_ms && last_activity_ms.is_none() {
        return LifecycleDecision {
            state: SkillLifecycleState::Stale,
            archive_due: false,
        };
    }

    LifecycleDecision {
        state: SkillLifecycleState::Active,
        archive_due: false,
    }
}

fn pr_prefix_cluster_key(skill_id: &str) -> Option<String> {
    let leaf = skill_id
        .rsplit(&[':', '/', '\\'][..])
        .next()
        .unwrap_or(skill_id);
    let lower = leaf.to_ascii_lowercase();
    if !lower.starts_with("pr-") {
        return None;
    }
    let mut parts = lower.split('-');
    let first = parts.next()?;
    let second = parts.next()?;
    Some(format!("{first}-{second}"))
}

fn is_builtin_skill_id(skill_id: &str) -> bool {
    skill_id.starts_with(&format!("{HARNESS_BUILTIN_SKILL_NAMESPACE}:"))
}

fn skill_usage_action_from_str(value: &str) -> Option<SkillUsageAction> {
    match value {
        "selected" => Some(SkillUsageAction::Selected),
        "injected" => Some(SkillUsageAction::Injected),
        "invoked" => Some(SkillUsageAction::Invoked),
        "viewed" => Some(SkillUsageAction::Viewed),
        "proposed" => Some(SkillUsageAction::Proposed),
        "patched" => Some(SkillUsageAction::Patched),
        "archived" => Some(SkillUsageAction::Archived),
        "rejected" => Some(SkillUsageAction::Rejected),
        _ => None,
    }
}

fn is_activity_action(action: SkillUsageAction) -> bool {
    matches!(
        action,
        SkillUsageAction::Selected | SkillUsageAction::Injected | SkillUsageAction::Invoked
    )
}

fn is_within_archive_root(harness_home: &Path, path: &Path) -> bool {
    let Ok(archive_root) = archived_dir_root(harness_home).canonicalize() else {
        return false;
    };
    path.canonicalize()
        .map(|canonical| canonical.starts_with(archive_root))
        .unwrap_or(false)
}

fn skill_leaf(skill_id: &str) -> &str {
    skill_id
        .rsplit(&[':', '/', '\\'][..])
        .next()
        .unwrap_or(skill_id)
}

fn timestamp_label(now_ms: i64) -> String {
    let days = now_ms.div_euclid(86_400_000);
    let day_ms = now_ms.rem_euclid(86_400_000);
    let hours = day_ms / 3_600_000;
    let minutes = (day_ms % 3_600_000) / 60_000;
    let seconds = (day_ms % 60_000) / 1_000;
    let (year, month, day) = civil_from_days(days);
    format!("{year:04}{month:02}{day:02}-{hours:02}{minutes:02}{seconds:02}")
}

fn civil_from_days(days_since_unix_epoch: i64) -> (i64, i64, i64) {
    let z = days_since_unix_epoch + 719_468;
    let era = (if z >= 0 { z } else { z - 146_096 }).div_euclid(146_097);
    let day_of_era = z - era * 146_097;
    let year_of_era = (day_of_era - day_of_era / 1_460 + day_of_era / 36_524
        - day_of_era / 146_096)
        .div_euclid(365);
    let year_day = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * year_day + 2).div_euclid(153);
    let day = year_day - (153 * month_prime + 2).div_euclid(5) + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    let year = year_of_era + era * 400 + if month <= 2 { 1 } else { 0 };
    (year, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn computes_lifecycle_transitions_for_active_stale_and_archive_due() {
        let now_ms = 100_000_000_000;
        let stale_ms = 86_400_000;
        let archive_ms = stale_ms * 3;
        let grace_ms = stale_ms * 1;

        let mut record = SkillLifecycleRecord::new("workspace:a", now_ms - (stale_ms * 5));
        let recent = decide_lifecycle_state(
            &record,
            Some(now_ms - 20_000),
            now_ms,
            stale_ms,
            archive_ms,
            grace_ms,
        );
        assert_eq!(recent.state, SkillLifecycleState::Active);
        assert!(!recent.archive_due);

        let stale = decide_lifecycle_state(
            &record,
            Some(now_ms - stale_ms * 2),
            now_ms,
            stale_ms,
            archive_ms,
            grace_ms,
        );
        assert_eq!(stale.state, SkillLifecycleState::Stale);
        assert!(!stale.archive_due);

        let archive_due = decide_lifecycle_state(
            &record,
            Some(now_ms - archive_ms * 2),
            now_ms,
            stale_ms,
            archive_ms,
            grace_ms,
        );
        assert_eq!(archive_due.state, SkillLifecycleState::Stale);
        assert!(archive_due.archive_due);

        let pinned = {
            record.pinned = true;
            decide_lifecycle_state(
                &record,
                Some(now_ms - archive_ms * 2),
                now_ms,
                stale_ms,
                archive_ms,
                grace_ms,
            )
        };
        assert_eq!(pinned.state, SkillLifecycleState::Active);
        assert!(!pinned.archive_due);
    }

    #[test]
    fn archive_and_restore_roundtrip_updates_state_and_location() {
        let root = temp_root("archive_and_restore_roundtrip_updates_state_and_location");
        let home = root.join(".agent-harness");
        let skill = home
            .join("workspace")
            .join("skills")
            .join("pr-alpha-one")
            .join(SKILL_FILE_NAME);
        fs::create_dir_all(skill.parent().unwrap()).unwrap();
        fs::write(&skill, "# Skill\n").unwrap();

        let move_target = run_skill_curator(SkillCuratorOptions {
            harness_home: home.clone(),
            stale_after_days: 1,
            archive_after_days: 2,
            never_used_grace_days: 1,
            include_builtins: false,
            target_skill_ids: Some(vec!["workspace:pr-alpha-one".to_string()]),
            dry_run: false,
            now_ms: 200_000_000,
        })
        .unwrap();
        assert!(move_target.proposal_ids.is_empty() || move_target.report_file.is_some());

        let _ = mark_skill_archived(
            &home,
            "workspace:pr-alpha-one",
            &skill.parent().unwrap(),
            300_000_000,
        );
        let archived_dir = archived_skill_dir(&home, "workspace:pr-alpha-one");
        move_skill_to_archive(&home, "workspace:pr-alpha-one", &skill).unwrap();
        assert!(!skill.exists());
        assert!(archived_dir.is_dir());

        let restore = restore_skill_from_archive(SkillRestoreOptions {
            harness_home: home.clone(),
            target_skill_id: "workspace:pr-alpha-one".to_string(),
            target_path: Some(skill.parent().unwrap().to_path_buf()),
            now_ms: 400_000_000,
        })
        .unwrap();
        assert_eq!(restore.target_skill_id, "workspace:pr-alpha-one");
        assert!(!archived_dir.exists());
        assert!(skill.exists());

        let store = read_skill_lifecycle_store(&home).unwrap();
        let record = store
            .skills
            .get("workspace:pr-alpha-one")
            .expect("restored entry");
        assert_eq!(record.state, SkillLifecycleState::Active);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn pinned_skill_skips_archive_due_to_pin() {
        let root = temp_root("pinned_skill_skips_archive_due_to_pin");
        let home = root.join(".agent-harness");
        let skill = home
            .join("workspace")
            .join("skills")
            .join("pr-pinned-one")
            .join(SKILL_FILE_NAME);
        fs::create_dir_all(skill.parent().unwrap()).unwrap();
        fs::write(&skill, "# Skill\n").unwrap();

        let _ = set_skill_pin(SkillPinOptions {
            harness_home: home.clone(),
            target_skill_id: "workspace:pr-pinned-one".to_string(),
            pinned: true,
            now_ms: 1_000,
        })
        .unwrap();

        let report = run_skill_curator(SkillCuratorOptions {
            harness_home: home.clone(),
            stale_after_days: 1,
            archive_after_days: 2,
            never_used_grace_days: 1,
            include_builtins: false,
            target_skill_ids: Some(vec!["workspace:pr-pinned-one".to_string()]),
            dry_run: false,
            now_ms: 4 * 86_400_000,
        })
        .unwrap();
        assert_eq!(report.proposals_created, 0);
        assert_eq!(report.pinned, 1);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn builds_deterministic_pr_and_tag_clusters() {
        let candidates = vec![
            CandidateSkill {
                skill_id: "workspace:pr-abc-one".to_string(),
                category: Some("ops".to_string()),
                tags: vec!["tag-a".to_string(), "tag-b".to_string()],
            },
            CandidateSkill {
                skill_id: "workspace:pr-abc-two".to_string(),
                category: Some("ops".to_string()),
                tags: vec!["tag-a".to_string()],
            },
            CandidateSkill {
                skill_id: "workspace:other".to_string(),
                category: Some("docs".to_string()),
                tags: vec!["tag-b".to_string()],
            },
        ];
        let clusters = build_curator_clusters(&candidates, 2);
        assert!(
            clusters
                .iter()
                .any(|cluster| cluster.cluster_type == "pr-prefix")
        );
        assert!(
            clusters
                .iter()
                .any(|cluster| cluster.cluster_type == "category-tag-overlap")
        );
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-skill-curator-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
