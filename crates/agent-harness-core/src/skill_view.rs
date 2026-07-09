use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};

use serde::Serialize;

use crate::{
    SkillDeliveryMode, SkillUsageAction, SkillUsageEventOptions, build_harness_skill_index,
    record_skill_usage_event, skills::skill_id_matches,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillViewOptions {
    pub harness_home: PathBuf,
    pub skill_id: String,
    pub file: Option<PathBuf>,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillViewReport {
    pub harness_home: PathBuf,
    pub skill_id: String,
    pub source_path: PathBuf,
    pub bytes: usize,
    pub body_checksum: String,
    pub content: String,
}

pub fn view_skill(options: SkillViewOptions) -> io::Result<SkillViewReport> {
    let index = build_harness_skill_index(&options.harness_home)?;
    let skill = index
        .skills
        .iter()
        .find(|skill| skill_id_matches(skill, &options.skill_id))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("skill `{}` was not found", options.skill_id),
            )
        })?;
    let source_path = match options.file.as_ref() {
        Some(relative) => contained_skill_view_path(&skill.directory, relative)?,
        None => skill.skill_file.clone(),
    };
    let content = fs::read_to_string(&source_path)?;
    let body_checksum = crate::skill_body_checksum(&content);
    let _ = record_skill_usage_event(SkillUsageEventOptions {
        harness_home: options.harness_home.clone(),
        action: SkillUsageAction::Viewed,
        skill_id: skill.id.clone(),
        source_kind: Some(skill.source_kind),
        source_turn_id: None,
        runtime_queue_id: None,
        session_key: None,
        channel: None,
        agent_id: None,
        delivery_mode: Some(SkillDeliveryMode::ToolView),
        body_checksum: Some(body_checksum.clone()),
        selection_receipt_id: None,
        reason: Some("skill-view".to_string()),
        now_ms: options.now_ms,
    });
    Ok(SkillViewReport {
        harness_home: options.harness_home,
        skill_id: skill.id.clone(),
        source_path,
        bytes: content.len(),
        body_checksum,
        content,
    })
}

fn contained_skill_view_path(skill_dir: &Path, relative: &Path) -> io::Result<PathBuf> {
    if relative.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "skill-view file path must be relative",
        ));
    }
    let mut components = relative.components();
    let Some(first) = components.next() else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "skill-view file path is empty",
        ));
    };
    let first = first.as_os_str().to_string_lossy();
    if !matches!(
        first.as_ref(),
        "references" | "templates" | "scripts" | "assets" | "SKILL.md"
    ) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "skill-view file path must be SKILL.md or under references/, templates/, scripts/, or assets/",
        ));
    }
    if relative.components().any(|component| {
        matches!(
            component,
            Component::ParentDir | Component::Prefix(_) | Component::RootDir
        )
    }) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "skill-view file path must not contain traversal",
        ));
    }
    let target = skill_dir.join(relative);
    let canonical_skill_dir = fs::canonicalize(skill_dir)?;
    let canonical_target = fs::canonicalize(&target)?;
    if !canonical_target.starts_with(canonical_skill_dir) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "skill-view file path escapes skill directory",
        ));
    }
    Ok(canonical_target)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SKILL_FILE_NAME;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn skill_view_reads_body_and_records_viewed_usage() {
        let root = temp_root("skill_view_reads_body_and_records_viewed_usage");
        let home = root.join(".agent-harness");
        let skill = home
            .join("skills")
            .join("agent-created")
            .join("general")
            .join("demo");
        fs::create_dir_all(&skill).unwrap();
        fs::write(skill.join(SKILL_FILE_NAME), "# Demo\n\nUse this skill.\n").unwrap();

        let report = view_skill(SkillViewOptions {
            harness_home: home.clone(),
            skill_id: "demo".to_string(),
            file: None,
            now_ms: 123,
        })
        .unwrap();

        assert_eq!(report.skill_id, "agent-created:demo");
        assert!(report.content.contains("Use this skill"));
        let usage = fs::read_to_string(crate::skill_usage_events_file(&home)).unwrap();
        assert!(usage.contains("\"action\":\"viewed\""));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn skill_view_blocks_support_file_traversal() {
        let root = temp_root("skill_view_blocks_support_file_traversal");
        let home = root.join(".agent-harness");
        let skill = home
            .join("skills")
            .join("agent-created")
            .join("general")
            .join("demo");
        fs::create_dir_all(skill.join("references")).unwrap();
        fs::write(skill.join(SKILL_FILE_NAME), "# Demo\n").unwrap();
        fs::write(skill.join("references").join("ok.md"), "ok").unwrap();

        assert!(
            view_skill(SkillViewOptions {
                harness_home: home.clone(),
                skill_id: "demo".to_string(),
                file: Some(PathBuf::from("references/ok.md")),
                now_ms: 1,
            })
            .is_ok()
        );
        assert!(
            view_skill(SkillViewOptions {
                harness_home: home,
                skill_id: "demo".to_string(),
                file: Some(PathBuf::from("../secret.md")),
                now_ms: 1,
            })
            .is_err()
        );

        let _ = fs::remove_dir_all(root);
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-skill-view-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
