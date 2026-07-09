use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::{append_jsonl_value, build_harness_skill_index};

const SKILL_LINT_REPORT_SCHEMA: &str = "agent-harness.skill-lint.v1";
const SKILL_LINT_MAX_BODY_BYTES: usize = 24 * 1024;
const SKILL_LINT_NAME_MAX_BYTES: usize = 64;
const SKILL_LINT_DESCRIPTION_MAX_BYTES: usize = 160;

const SKILL_LINT_DEFAULT_TAXONOMY: [&str; 10] = [
    "operations",
    "trading",
    "memory",
    "media",
    "security",
    "tooling",
    "infrastructure",
    "workflow",
    "developer",
    "analysis",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum SkillLintSeverity {
    Error,
    Warn,
    Info,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillLintFinding {
    pub severity: SkillLintSeverity,
    pub code: &'static str,
    pub message: String,
    pub path: Option<PathBuf>,
}

impl SkillLintFinding {
    pub fn error(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            severity: SkillLintSeverity::Error,
            code,
            message: message.into(),
            path: None,
        }
    }

    pub fn warn(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            severity: SkillLintSeverity::Warn,
            code,
            message: message.into(),
            path: None,
        }
    }

    pub fn with_path(mut self, path: PathBuf) -> Self {
        self.path = Some(path);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum SkillLintStatus {
    Pass,
    Warn,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillLintSummary {
    pub findings: usize,
    pub errors: usize,
    pub warnings: usize,
    pub infos: usize,
    pub trigger_collisions: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillLintReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub target_path: PathBuf,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_skill_id: Option<String>,
    pub status: SkillLintStatus,
    pub scan_trigger_collisions: bool,
    pub findings: Vec<SkillLintFinding>,
    pub summary: SkillLintSummary,
    pub receipts_file: PathBuf,
}

#[derive(Debug, Clone)]
pub struct SkillLintOptions {
    pub harness_home: PathBuf,
    pub target_path: PathBuf,
    pub target_skill_id: Option<String>,
    pub replacement_body: Option<String>,
    pub support_file_paths: Vec<PathBuf>,
    pub scan_trigger_collisions: bool,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SkillLintFrontmatter {
    name: Option<String>,
    description: Option<String>,
    category: Option<String>,
    triggers: Vec<String>,
}

pub fn skill_lint_receipts_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("skills")
        .join("lint-receipts.jsonl")
}

pub fn lint_skill_file(options: SkillLintOptions) -> io::Result<SkillLintReport> {
    let SkillLintOptions {
        harness_home,
        target_path,
        target_skill_id,
        replacement_body,
        support_file_paths,
        scan_trigger_collisions,
        now_ms,
    } = options;

    let body = match replacement_body {
        Some(body) => body,
        None => fs::read_to_string(&target_path)?,
    };

    let metadata = parse_skill_frontmatter(&body);
    let mut findings = Vec::new();

    findings.push(validate_frontmatter_name(&metadata.name));
    findings.push(validate_description(&metadata.description));
    findings.push(validate_category(&metadata.category));
    findings.extend(check_support_file_containment(&support_file_paths));
    findings.push(check_body_capability(&body));

    if scan_trigger_collisions {
        findings.extend(check_trigger_collisions(
            &harness_home,
            &target_skill_id,
            &target_path,
            &metadata.triggers,
        )?);
    }

    normalize_findings(&mut findings);
    let summary = lint_summary(&findings);
    let status = if summary.errors > 0 {
        SkillLintStatus::Error
    } else if summary.warnings > 0 {
        SkillLintStatus::Warn
    } else {
        SkillLintStatus::Pass
    };

    let report = SkillLintReport {
        schema: SKILL_LINT_REPORT_SCHEMA,
        harness_home: harness_home.clone(),
        target_path: target_path.clone(),
        target_skill_id,
        status,
        scan_trigger_collisions,
        findings,
        summary,
        receipts_file: skill_lint_receipts_file(&harness_home),
    };

    let _ = append_jsonl_value(&skill_lint_receipts_file(&harness_home), &report);
    let _ = now_ms;

    Ok(report)
}

fn validate_frontmatter_name(name: &Option<String>) -> SkillLintFinding {
    let Some(name) = name.as_deref() else {
        return SkillLintFinding::error("FRONTMATTER_NAME_MISSING", "frontmatter name is required");
    };
    if name.trim().is_empty() {
        return SkillLintFinding::error("FRONTMATTER_NAME_EMPTY", "frontmatter name is empty");
    }
    if name.len() > SKILL_LINT_NAME_MAX_BYTES {
        return SkillLintFinding::error(
            "FRONTMATTER_NAME_TOO_LONG",
            format!("frontmatter name is longer than {SKILL_LINT_NAME_MAX_BYTES} bytes"),
        );
    }
    if !name
        .chars()
        .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '-' | '_' | '.'))
    {
        return SkillLintFinding::error(
            "FRONTMATTER_NAME_FORMAT",
            "frontmatter name should be lowercase with letters, digits, hyphen, underscore, dot",
        );
    }
    SkillLintFinding {
        severity: SkillLintSeverity::Info,
        code: "FRONTMATTER_NAME_OK",
        message: "frontmatter name format is valid".to_string(),
        path: None,
    }
}

fn validate_description(description: &Option<String>) -> SkillLintFinding {
    let Some(description) = description.as_deref() else {
        return SkillLintFinding::warn("DESCRIPTION_MISSING", "frontmatter description is missing");
    };
    if description.trim().is_empty() {
        return SkillLintFinding::warn("DESCRIPTION_EMPTY", "frontmatter description is empty");
    }
    if description.len() > SKILL_LINT_DESCRIPTION_MAX_BYTES {
        return SkillLintFinding::warn(
            "DESCRIPTION_TOO_LONG",
            format!("description is longer than {SKILL_LINT_DESCRIPTION_MAX_BYTES} bytes"),
        );
    }
    let one_sentenceish = description
        .chars()
        .any(|value| matches!(value, '.' | '。' | '!' | '?' | '！' | '？'));
    if !one_sentenceish {
        return SkillLintFinding::warn(
            "DESCRIPTION_NOT_SENTENCEISH",
            "description should look like a sentence ('.', '!' or '?')",
        );
    }
    SkillLintFinding {
        severity: SkillLintSeverity::Info,
        code: "DESCRIPTION_OK",
        message: "description looks sentence-like".to_string(),
        path: None,
    }
}

fn validate_category(category: &Option<String>) -> SkillLintFinding {
    let Some(category) = category.as_deref() else {
        return SkillLintFinding::warn(
            "CATEGORY_MISSING",
            "frontmatter category is missing; consider using known taxonomy category",
        );
    };
    let normalized = category.to_ascii_lowercase();
    if !SKILL_LINT_DEFAULT_TAXONOMY.contains(&normalized.as_str()) {
        return SkillLintFinding::warn(
            "CATEGORY_UNKNOWN",
            format!("category `{category}` is not in default taxonomy"),
        );
    }
    SkillLintFinding {
        severity: SkillLintSeverity::Info,
        code: "CATEGORY_OK",
        message: "category is in default taxonomy".to_string(),
        path: None,
    }
}

fn check_body_capability(body: &str) -> SkillLintFinding {
    if body.len() > SKILL_LINT_MAX_BODY_BYTES {
        SkillLintFinding::error(
            "BODY_TOO_LARGE",
            format!(
                "SKILL body is {} bytes (max {})",
                body.len(),
                SKILL_LINT_MAX_BODY_BYTES
            ),
        )
    } else {
        SkillLintFinding {
            severity: SkillLintSeverity::Info,
            code: "BODY_SIZE_OK",
            message: "body is within size cap".to_string(),
            path: None,
        }
    }
}

fn check_support_file_containment(paths: &[PathBuf]) -> Vec<SkillLintFinding> {
    let mut findings = Vec::new();
    for path in paths {
        if path.is_absolute() {
            findings.push(
                SkillLintFinding::error("SUPPORT_PATH_ABSOLUTE", "support path must be relative")
                    .with_path(path.clone()),
            );
            continue;
        }
        if has_parent_traversal(path) {
            findings.push(
                SkillLintFinding::error(
                    "SUPPORT_PATH_TRAVERSAL",
                    "support path must not use parent traversal",
                )
                .with_path(path.clone()),
            );
            continue;
        }
        if let Some(first) = path.components().next() {
            let first = first.as_os_str().to_string_lossy();
            if !matches!(
                first.as_ref(),
                "references" | "templates" | "scripts" | "assets"
            ) {
                findings.push(
                    SkillLintFinding::error(
                        "SUPPORT_PATH_DIRECTORY",
                        "support file must be under references/, templates/, scripts/, or assets/",
                    )
                    .with_path(path.clone()),
                );
            }
        } else {
            findings.push(
                SkillLintFinding::error("SUPPORT_PATH_EMPTY", "support path is empty")
                    .with_path(path.clone()),
            );
        }
    }
    findings
}

fn check_trigger_collisions(
    harness_home: &Path,
    target_skill_id: &Option<String>,
    target_path: &Path,
    triggers: &[String],
) -> io::Result<Vec<SkillLintFinding>> {
    if triggers.is_empty() {
        return Ok(vec![SkillLintFinding {
            severity: SkillLintSeverity::Info,
            code: "TRIGGER_MISSING",
            message: "frontmatter trigger list is empty".to_string(),
            path: None,
        }]);
    }
    let index = build_harness_skill_index(harness_home)?;
    let mut target_triggers = BTreeSet::new();
    for trigger in triggers {
        target_triggers.insert(trigger.to_ascii_lowercase());
    }
    let mut collisions = Vec::new();
    for record in index.skills {
        let same_id = target_skill_id.as_ref().is_some_and(|id| record.id == *id)
            || record.skill_file == *target_path;
        if same_id {
            continue;
        }
        let mut overlap: Vec<String> = record
            .frontmatter
            .triggers
            .into_iter()
            .filter(|candidate| target_triggers.contains(&candidate.to_ascii_lowercase()))
            .collect();
        if !overlap.is_empty() {
            overlap.sort_unstable();
            overlap.dedup();
            for trigger in overlap {
                collisions.push(SkillLintFinding::warn(
                    "TRIGGER_COLLISION",
                    format!(
                        "trigger `{}` collides with skill {} in {}",
                        trigger, record.id, record.original_id
                    ),
                ));
            }
        }
    }
    if collisions.is_empty() {
        collisions.push(SkillLintFinding::info(
            "TRIGGER_COLLISION_OK",
            "no trigger collision found",
        ));
    }
    collisions.sort_by(|left, right| {
        left.message
            .cmp(&right.message)
            .then_with(|| left.code.cmp(&right.code))
    });
    Ok(collisions)
}

fn has_parent_traversal(path: &Path) -> bool {
    path.components()
        .any(|component| component == std::path::Component::ParentDir)
}

fn normalize_findings(findings: &mut Vec<SkillLintFinding>) {
    findings.sort_by(|left, right| {
        fn severity_rank(value: &SkillLintSeverity) -> i32 {
            match value {
                SkillLintSeverity::Error => 2,
                SkillLintSeverity::Warn => 1,
                SkillLintSeverity::Info => 0,
            }
        }
        severity_rank(&left.severity)
            .cmp(&severity_rank(&right.severity))
            .reverse()
            .then_with(|| left.code.cmp(&right.code))
            .then_with(|| left.message.cmp(&right.message))
    });
}

fn lint_summary(findings: &[SkillLintFinding]) -> SkillLintSummary {
    let mut summary = SkillLintSummary {
        findings: findings.len(),
        errors: 0,
        warnings: 0,
        infos: 0,
        trigger_collisions: 0,
    };
    for finding in findings {
        match finding.severity {
            SkillLintSeverity::Error => summary.errors += 1,
            SkillLintSeverity::Warn => summary.warnings += 1,
            SkillLintSeverity::Info => summary.infos += 1,
        }
        if finding.code == "TRIGGER_COLLISION" {
            summary.trigger_collisions += 1;
        }
    }
    summary
}

impl SkillLintFinding {
    fn info(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            severity: SkillLintSeverity::Info,
            code,
            message: message.into(),
            path: None,
        }
    }
}

fn parse_skill_frontmatter(body: &str) -> SkillLintFrontmatter {
    SkillLintFrontmatter {
        name: frontmatter_value(body, &["name"]),
        description: frontmatter_value(body, &["description"]),
        category: frontmatter_value(body, &["category"]).or_else(|| {
            frontmatter_nested_value(body, &["metadata", "agent_harness"], &["category"])
        }),
        triggers: frontmatter_values(body, &["triggers", "trigger"]),
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

fn frontmatter_key_matches(left: &str, right: &str) -> bool {
    normalize_frontmatter_key(left) == normalize_frontmatter_key(right)
}

fn normalize_frontmatter_key(value: &str) -> String {
    value.trim().replace('-', "_").to_ascii_lowercase()
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

fn trim_yaml_scalar(value: &str) -> String {
    value
        .trim()
        .trim_matches('"')
        .trim_matches('\'')
        .trim()
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn skill_lint_passes_for_valid_skill_body() {
        let root = temp_root("skill_lint_passes_for_valid_skill_body");
        let harness_home = root.join(".openclaw");
        let target = harness_home
            .join("skills")
            .join("agent-created")
            .join("sample");
        fs::create_dir_all(&target).unwrap();
        let skill_file = target.join("SKILL.md");
        fs::write(
            &skill_file,
            "---\nname: valid-skill\ndescription: Handle routing safely.\ncategory: operations\ntriggers: [route]\n---\n# Valid\n",
        )
        .unwrap();
        let report = lint_skill_file(SkillLintOptions {
            harness_home,
            target_path: skill_file,
            target_skill_id: Some("agent-created:valid-skill".to_string()),
            replacement_body: None,
            support_file_paths: Vec::new(),
            scan_trigger_collisions: false,
            now_ms: 0,
        })
        .unwrap();
        assert_eq!(report.status, SkillLintStatus::Pass);
        assert_eq!(report.summary.errors, 0);
        assert!(report.summary.warnings <= report.summary.findings);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn skill_lint_rejects_bad_frontmatter_name_and_description() {
        let root = temp_root("skill_lint_rejects_bad_frontmatter_name_and_description");
        let harness_home = root.join(".openclaw");
        let file = root.join("SKILL.md");
        fs::create_dir_all(&root).unwrap();
        fs::write(
            &file,
            "---\ndescription: this is not a sentence no punctuation but very long text that breaks sentence boundary and goes too long intentionally for linting validation check this should warn or error soon as it goes over the byte limit for description checks\ncategory: bad-category\n---\n# Bad\n",
        )
        .unwrap();
        let report = lint_skill_file(SkillLintOptions {
            harness_home,
            target_path: file,
            target_skill_id: Some("agent-created:bad".to_string()),
            replacement_body: None,
            support_file_paths: Vec::new(),
            scan_trigger_collisions: false,
            now_ms: 0,
        })
        .unwrap();
        assert_eq!(report.status, SkillLintStatus::Error);
        assert!(
            report
                .findings
                .iter()
                .any(|finding| finding.code == "FRONTMATTER_NAME_MISSING")
        );
        assert!(
            report
                .findings
                .iter()
                .any(|finding| finding.code == "CATEGORY_UNKNOWN")
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn skill_lint_scans_trigger_collisions_when_requested() {
        let root = temp_root("skill_lint_scans_trigger_collisions");
        let harness_home = root.join(".openclaw");
        let skill_dir = harness_home.join("skills").join("agent-created");
        let first = skill_dir.join("first");
        let second = skill_dir.join("second");
        fs::create_dir_all(first.join("SKILL.md").parent().unwrap()).unwrap();
        fs::create_dir_all(second.join("SKILL.md").parent().unwrap()).unwrap();
        fs::write(
            first.join("SKILL.md"),
            "---\nname: first\ndescription: Shared trigger.\ncategory: operations\ntriggers: [triage]\n---\n",
        )
        .unwrap();
        let target = second.join("SKILL.md");
        fs::write(
            &target,
            "---\nname: second\ndescription: This triggers overlap.\ncategory: operations\ntriggers: [triage]\n---\n",
        )
        .unwrap();

        let report = lint_skill_file(SkillLintOptions {
            harness_home,
            target_path: target,
            target_skill_id: Some("agent-created:second".to_string()),
            replacement_body: None,
            support_file_paths: Vec::new(),
            scan_trigger_collisions: true,
            now_ms: 0,
        })
        .unwrap();

        assert_eq!(report.status, SkillLintStatus::Warn);
        assert!(
            report
                .findings
                .iter()
                .any(|finding| finding.code == "TRIGGER_COLLISION")
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn skill_lint_checks_support_file_containment() {
        let root = temp_root("skill_lint_checks_support_file_containment");
        let harness_home = root.join(".openclaw");
        let file = root.join("SKILL.md");
        fs::create_dir_all(&root).unwrap();
        fs::write(
            &file,
            "---\nname: support\ncategory: operations\ndescription: Good. This is good.\n---\n# Support\n",
        )
        .unwrap();
        let report = lint_skill_file(SkillLintOptions {
            harness_home,
            target_path: file,
            target_skill_id: Some("agent-created:support".to_string()),
            replacement_body: None,
            support_file_paths: vec![PathBuf::from("../outside/file.md")],
            scan_trigger_collisions: false,
            now_ms: 0,
        })
        .unwrap();
        assert_eq!(report.status, SkillLintStatus::Error);
        assert!(
            report
                .findings
                .iter()
                .any(|finding| finding.code == "SUPPORT_PATH_TRAVERSAL")
        );
        let _ = fs::remove_dir_all(root);
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-skill-lint-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
