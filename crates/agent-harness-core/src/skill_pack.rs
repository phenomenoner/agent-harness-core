use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::{
    SkillGuardOptions, SkillGuardVerdict, SkillLintOptions, SkillLintStatus, append_jsonl_value,
    build_harness_skill_index, current_log_time_ms, lint_skill_file, run_skill_guard,
    skill_body_checksum,
};

const SKILL_PACK_SCHEMA: &str = "agent-harness.skill-pack.v1";
const SKILL_PACK_LOCK_SCHEMA: &str = "agent-harness.skill-pack-lock.v1";
const SKILL_PACK_RECEIPT_SCHEMA: &str = "agent-harness.skill-pack-receipt.v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SkillPackAction {
    Export,
    Import,
    Remove,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SkillPackStatus {
    Ok,
    Blocked,
    DryRun,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkillPackConflictPolicy {
    Skip,
    Rename,
    Proposal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillPackManifest {
    pub schema: String,
    pub pack_id: String,
    pub created_at_ms: i64,
    pub skills: Vec<SkillPackManifestSkill>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillPackManifestSkill {
    pub skill_id: String,
    pub original_id: String,
    pub relative_dir: PathBuf,
    pub checksum: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillPackLock {
    pub schema: String,
    pub pack_id: String,
    pub installed_at_ms: i64,
    pub installed: BTreeMap<String, PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillPackExportOptions {
    pub harness_home: PathBuf,
    pub pack_id: String,
    pub output_dir: PathBuf,
    pub skill_ids: Vec<String>,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillPackImportOptions {
    pub harness_home: PathBuf,
    pub pack_dir: PathBuf,
    pub conflict_policy: SkillPackConflictPolicy,
    pub dry_run: bool,
    pub trusted: bool,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillPackRemoveOptions {
    pub harness_home: PathBuf,
    pub pack_id: String,
    pub now_ms: i64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillPackReport {
    pub schema: &'static str,
    pub action: SkillPackAction,
    pub status: SkillPackStatus,
    pub harness_home: PathBuf,
    pub pack_id: String,
    pub pack_dir: PathBuf,
    pub skills: Vec<String>,
    pub reason: String,
    pub receipts_file: PathBuf,
    pub now_ms: i64,
}

pub fn skill_pack_receipts_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("skills")
        .join("pack-receipts.jsonl")
}

pub fn skill_pack_lock_file(harness_home: impl AsRef<Path>, pack_id: &str) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("skills")
        .join("packs")
        .join(format!("{}.lock.json", safe_name(pack_id)))
}

pub fn export_skill_pack(options: SkillPackExportOptions) -> io::Result<SkillPackReport> {
    let now_ms = normalized_now(options.now_ms);
    let pack_dir = options.output_dir.join(&options.pack_id);
    let skills_dir = pack_dir.join("skills");
    fs::create_dir_all(&skills_dir)?;
    let index = build_harness_skill_index(&options.harness_home)?;
    let mut manifest = SkillPackManifest {
        schema: SKILL_PACK_SCHEMA.to_string(),
        pack_id: options.pack_id.clone(),
        created_at_ms: now_ms,
        skills: Vec::new(),
    };
    let mut exported = Vec::new();
    for skill_id in &options.skill_ids {
        let skill = index
            .skills
            .iter()
            .find(|skill| &skill.id == skill_id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "skill not found"))?;
        let body = fs::read_to_string(&skill.skill_file)?;
        public_hygiene_check(&body)?;
        let lint = lint_skill_file(SkillLintOptions {
            harness_home: options.harness_home.clone(),
            target_path: skill.skill_file.clone(),
            target_skill_id: Some(skill.id.clone()),
            replacement_body: Some(body.clone()),
            support_file_paths: Vec::new(),
            scan_trigger_collisions: false,
            now_ms,
        })?;
        if lint.status == SkillLintStatus::Error {
            return blocked_report(
                SkillPackAction::Export,
                &options.harness_home,
                &options.pack_id,
                &pack_dir,
                "lint errors block pack export",
                now_ms,
            );
        }
        let guard = run_skill_guard(SkillGuardOptions {
            harness_home: options.harness_home.clone(),
            target_skill_id: skill.id.clone(),
            target_path: skill.skill_file.clone(),
            body: Some(body.clone()),
            support_file_paths: Vec::new(),
            trusted: false,
            now_ms,
        })?;
        if guard.verdict == SkillGuardVerdict::Dangerous {
            return blocked_report(
                SkillPackAction::Export,
                &options.harness_home,
                &options.pack_id,
                &pack_dir,
                "guard dangerous verdict blocks pack export",
                now_ms,
            );
        }
        let relative_dir = PathBuf::from(&skill.original_id);
        copy_dir_all(&skill.directory, &skills_dir.join(&relative_dir))?;
        manifest.skills.push(SkillPackManifestSkill {
            skill_id: skill.id.clone(),
            original_id: skill.original_id.clone(),
            relative_dir,
            checksum: skill_body_checksum(&body),
        });
        exported.push(skill.id.clone());
    }
    write_json(&pack_dir.join("skill-pack.json"), &manifest)?;
    let report = report(
        SkillPackAction::Export,
        SkillPackStatus::Ok,
        options.harness_home,
        options.pack_id,
        pack_dir,
        exported,
        "skill pack exported",
        now_ms,
    );
    append_jsonl_value(&report.receipts_file, &report)?;
    Ok(report)
}

pub fn import_skill_pack(options: SkillPackImportOptions) -> io::Result<SkillPackReport> {
    let now_ms = normalized_now(options.now_ms);
    let manifest = read_manifest(&options.pack_dir)?;
    let install_root = options
        .harness_home
        .join("skills")
        .join("packs")
        .join(&manifest.pack_id);
    let mut installed = BTreeMap::new();
    let mut skills = Vec::new();
    for skill in &manifest.skills {
        reject_unsafe_relative(&skill.relative_dir)?;
        let source_dir = options.pack_dir.join("skills").join(&skill.relative_dir);
        let source_file = source_dir.join(crate::SKILL_FILE_NAME);
        let body = fs::read_to_string(&source_file)?;
        if skill_body_checksum(&body) != skill.checksum {
            return blocked_report(
                SkillPackAction::Import,
                &options.harness_home,
                &manifest.pack_id,
                &options.pack_dir,
                "checksum mismatch blocks pack import",
                now_ms,
            );
        }
        let guard = run_skill_guard(SkillGuardOptions {
            harness_home: options.harness_home.clone(),
            target_skill_id: format!("pack:{}", skill.original_id),
            target_path: source_file,
            body: Some(body),
            support_file_paths: Vec::new(),
            trusted: options.trusted,
            now_ms,
        })?;
        if guard.verdict == SkillGuardVerdict::Dangerous {
            return blocked_report(
                SkillPackAction::Import,
                &options.harness_home,
                &manifest.pack_id,
                &options.pack_dir,
                "guard dangerous verdict blocks pack import",
                now_ms,
            );
        }
        let destination =
            conflict_destination(&install_root, &skill.original_id, options.conflict_policy)?;
        skills.push(format!(
            "pack:{}",
            destination.file_name().unwrap().to_string_lossy()
        ));
        installed.insert(skill.original_id.clone(), destination.clone());
        if !options.dry_run {
            copy_dir_all(&source_dir, &destination)?;
        }
    }
    if options.dry_run {
        return Ok(report(
            SkillPackAction::Import,
            SkillPackStatus::DryRun,
            options.harness_home,
            manifest.pack_id,
            options.pack_dir,
            skills,
            "skill pack import dry-run",
            now_ms,
        ));
    }
    let lock = SkillPackLock {
        schema: SKILL_PACK_LOCK_SCHEMA.to_string(),
        pack_id: manifest.pack_id.clone(),
        installed_at_ms: now_ms,
        installed,
    };
    write_json(
        &skill_pack_lock_file(&options.harness_home, &manifest.pack_id),
        &lock,
    )?;
    let report = report(
        SkillPackAction::Import,
        SkillPackStatus::Ok,
        options.harness_home,
        manifest.pack_id,
        options.pack_dir,
        skills,
        "skill pack imported",
        now_ms,
    );
    append_jsonl_value(&report.receipts_file, &report)?;
    Ok(report)
}

pub fn remove_skill_pack(options: SkillPackRemoveOptions) -> io::Result<SkillPackReport> {
    let now_ms = normalized_now(options.now_ms);
    let lock_path = skill_pack_lock_file(&options.harness_home, &options.pack_id);
    let lock_text = fs::read_to_string(&lock_path)?;
    let lock: SkillPackLock = serde_json::from_str(&lock_text).map_err(io::Error::other)?;
    let install_root = options
        .harness_home
        .join("skills")
        .join("packs")
        .join(&options.pack_id);
    let mut removed = Vec::new();
    for (skill_id, path) in &lock.installed {
        let canonical_parent = path
            .parent()
            .and_then(|parent| parent.canonicalize().ok())
            .unwrap_or_else(|| install_root.clone());
        if !canonical_parent.starts_with(&install_root) && path.exists() {
            return blocked_report(
                SkillPackAction::Remove,
                &options.harness_home,
                &options.pack_id,
                &install_root,
                "lockfile path outside pack install root",
                now_ms,
            );
        }
        if path.exists() {
            fs::remove_dir_all(path)?;
        }
        removed.push(skill_id.clone());
    }
    let _ = fs::remove_file(lock_path);
    let report = report(
        SkillPackAction::Remove,
        SkillPackStatus::Ok,
        options.harness_home,
        options.pack_id,
        install_root,
        removed,
        "skill pack removed",
        now_ms,
    );
    append_jsonl_value(&report.receipts_file, &report)?;
    Ok(report)
}

fn read_manifest(pack_dir: &Path) -> io::Result<SkillPackManifest> {
    let text = fs::read_to_string(pack_dir.join("skill-pack.json"))?;
    serde_json::from_str(&text).map_err(io::Error::other)
}

fn conflict_destination(
    install_root: &Path,
    original_id: &str,
    policy: SkillPackConflictPolicy,
) -> io::Result<PathBuf> {
    let mut destination = install_root.join(original_id);
    if !destination.exists() {
        return Ok(destination);
    }
    match policy {
        SkillPackConflictPolicy::Skip => Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "pack skill already exists",
        )),
        SkillPackConflictPolicy::Proposal => Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "proposal conflict policy is not applied by direct import",
        )),
        SkillPackConflictPolicy::Rename => {
            for index in 2..100 {
                destination = install_root.join(format!("{original_id}-{index}"));
                if !destination.exists() {
                    return Ok(destination);
                }
            }
            Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "could not find renamed pack destination",
            ))
        }
    }
}

fn blocked_report(
    action: SkillPackAction,
    harness_home: &Path,
    pack_id: &str,
    pack_dir: &Path,
    reason: &str,
    now_ms: i64,
) -> io::Result<SkillPackReport> {
    let report = report(
        action,
        SkillPackStatus::Blocked,
        harness_home.to_path_buf(),
        pack_id.to_string(),
        pack_dir.to_path_buf(),
        Vec::new(),
        reason,
        now_ms,
    );
    append_jsonl_value(&report.receipts_file, &report)?;
    Ok(report)
}

fn report(
    action: SkillPackAction,
    status: SkillPackStatus,
    harness_home: PathBuf,
    pack_id: String,
    pack_dir: PathBuf,
    skills: Vec<String>,
    reason: impl Into<String>,
    now_ms: i64,
) -> SkillPackReport {
    let receipts_file = skill_pack_receipts_file(&harness_home);
    SkillPackReport {
        schema: SKILL_PACK_RECEIPT_SCHEMA,
        action,
        status,
        harness_home,
        pack_id,
        pack_dir,
        skills,
        reason: reason.into(),
        receipts_file,
        now_ms,
    }
}

fn public_hygiene_check(body: &str) -> io::Result<()> {
    let lower = body.to_ascii_lowercase();
    if lower.contains("docs/.private")
        || lower.contains(".env")
        || lower.contains("api_key")
        || lower.contains("token=")
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "public hygiene check blocked pack export",
        ));
    }
    Ok(())
}

fn reject_unsafe_relative(path: &Path) -> io::Result<()> {
    if path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::ParentDir | Component::Prefix(_)))
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "pack relative path escapes pack root",
        ));
    }
    Ok(())
}

fn copy_dir_all(source: &Path, destination: &Path) -> io::Result<()> {
    fs::create_dir_all(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir_all(&source_path, &destination_path)?;
        } else {
            fs::copy(&source_path, &destination_path)?;
        }
    }
    Ok(())
}

fn write_json(path: &Path, value: &impl Serialize) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let body = serde_json::to_string_pretty(value).map_err(io::Error::other)?;
    fs::write(path, body)
}

fn safe_name(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '-'
            }
        })
        .collect()
}

fn normalized_now(now_ms: i64) -> i64 {
    if now_ms <= 0 {
        current_log_time_ms().unwrap_or(0)
    } else {
        now_ms
    }
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::{SkillSelectionQuery, select_skills};

    use super::*;

    #[test]
    fn skill_pack_round_trip_imports_pack_skills_and_selects_them() {
        let root = temp_root("skill_pack_round_trip_imports_pack_skills_and_selects_them");
        let source_home = root.join("source");
        let skill_dir = source_home
            .join("skills")
            .join("agent-created")
            .join("orders");
        fs::create_dir_all(&skill_dir).unwrap();
        fs::write(
            skill_dir.join(crate::SKILL_FILE_NAME),
            "---\nname: orders\ndescription: Handle order routing.\ncategory: operations\ntriggers: [orders]\n---\n# Orders\n\nUse order routing.",
        )
        .unwrap();
        let export = export_skill_pack(SkillPackExportOptions {
            harness_home: source_home.clone(),
            pack_id: "ops-pack".to_string(),
            output_dir: root.join("packs"),
            skill_ids: vec!["agent-created:orders".to_string()],
            now_ms: 1,
        })
        .unwrap();
        assert_eq!(export.status, SkillPackStatus::Ok);

        let target_home = root.join("target");
        let import = import_skill_pack(SkillPackImportOptions {
            harness_home: target_home.clone(),
            pack_dir: export.pack_dir.clone(),
            conflict_policy: SkillPackConflictPolicy::Skip,
            dry_run: false,
            trusted: true,
            now_ms: 2,
        })
        .unwrap();
        assert_eq!(import.status, SkillPackStatus::Ok);
        let index = build_harness_skill_index(&target_home).unwrap();
        assert!(index.skills.iter().any(|skill| skill.id == "pack:orders"));
        let selections = select_skills(
            &index,
            &SkillSelectionQuery {
                text: "orders routing".to_string(),
                agent_id: None,
                channel: None,
                workspace: None,
                agent_mode: None,
                available_tools: Vec::new(),
                available_toolsets: Vec::new(),
                fts_enabled: true,
                vector_tie_break_enabled: false,
                usage_snapshot: None,
                usage_prior_enabled: false,
                limit: 3,
            },
        );
        assert_eq!(selections[0].skill_id, "pack:orders");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn skill_pack_import_fails_closed_on_checksum_mismatch() {
        let root = temp_root("skill_pack_import_fails_closed_on_checksum_mismatch");
        let home = root.join("home");
        let pack_dir = root.join("pack");
        let skill_dir = pack_dir.join("skills").join("orders");
        fs::create_dir_all(&skill_dir).unwrap();
        fs::write(skill_dir.join(crate::SKILL_FILE_NAME), "# Tampered").unwrap();
        write_json(
            &pack_dir.join("skill-pack.json"),
            &SkillPackManifest {
                schema: SKILL_PACK_SCHEMA.to_string(),
                pack_id: "pack".to_string(),
                created_at_ms: 1,
                skills: vec![SkillPackManifestSkill {
                    skill_id: "agent-created:orders".to_string(),
                    original_id: "orders".to_string(),
                    relative_dir: PathBuf::from("orders"),
                    checksum: "wrong".to_string(),
                }],
            },
        )
        .unwrap();
        let report = import_skill_pack(SkillPackImportOptions {
            harness_home: home,
            pack_dir,
            conflict_policy: SkillPackConflictPolicy::Skip,
            dry_run: false,
            trusted: true,
            now_ms: 2,
        })
        .unwrap();
        assert_eq!(report.status, SkillPackStatus::Blocked);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn skill_pack_remove_rejects_lockfile_out_of_root_path() {
        let root = temp_root("skill_pack_remove_rejects_lockfile_out_of_root_path");
        let home = root.join("home");
        let outside = root.join("outside").join("bad");
        fs::create_dir_all(&outside).unwrap();
        let lock = SkillPackLock {
            schema: SKILL_PACK_LOCK_SCHEMA.to_string(),
            pack_id: "pack".to_string(),
            installed_at_ms: 1,
            installed: BTreeMap::from([("bad".to_string(), outside)]),
        };
        write_json(&skill_pack_lock_file(&home, "pack"), &lock).unwrap();
        let report = remove_skill_pack(SkillPackRemoveOptions {
            harness_home: home,
            pack_id: "pack".to_string(),
            now_ms: 2,
        })
        .unwrap();
        assert_eq!(report.status, SkillPackStatus::Blocked);
        let _ = fs::remove_dir_all(root);
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-skill-pack-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
