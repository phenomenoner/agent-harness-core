use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    SkillDeliveryMode, SkillSourceKind, append_jsonl_value, current_log_time_ms, write_json_atomic,
};

const SKILL_USAGE_EVENT_SCHEMA: &str = "agent-harness.skill-usage.v1";
const SKILL_USAGE_SNAPSHOT_SCHEMA: &str = "agent-harness.skill-usage-snapshot.v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SkillUsageAction {
    Selected,
    Injected,
    Invoked,
    Viewed,
    Proposed,
    Patched,
    Archived,
    Rejected,
}

impl SkillUsageAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Selected => "selected",
            Self::Injected => "injected",
            Self::Invoked => "invoked",
            Self::Viewed => "viewed",
            Self::Proposed => "proposed",
            Self::Patched => "patched",
            Self::Archived => "archived",
            Self::Rejected => "rejected",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SkillUsageProvenance {
    BundledHarnessSkill,
    ImportedLegacySkill,
    OperatorAuthoredSkill,
    AgentCreatedSkill,
    ExternalProjectSkill,
    Unknown,
}

impl SkillUsageProvenance {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BundledHarnessSkill => "bundled-harness-skill",
            Self::ImportedLegacySkill => "imported-legacy-skill",
            Self::OperatorAuthoredSkill => "operator-authored-skill",
            Self::AgentCreatedSkill => "agent-created-skill",
            Self::ExternalProjectSkill => "external-project-skill",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillUsageEventOptions {
    pub harness_home: PathBuf,
    pub action: SkillUsageAction,
    pub skill_id: String,
    pub source_kind: Option<SkillSourceKind>,
    pub source_turn_id: Option<String>,
    pub runtime_queue_id: Option<String>,
    pub session_key: Option<String>,
    pub channel: Option<String>,
    pub agent_id: Option<String>,
    pub delivery_mode: Option<SkillDeliveryMode>,
    pub body_checksum: Option<String>,
    pub selection_receipt_id: Option<String>,
    pub reason: Option<String>,
    pub now_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillUsageRecord {
    pub schema: &'static str,
    pub at_ms: i64,
    pub action: SkillUsageAction,
    pub skill_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_kind: Option<SkillSourceKind>,
    pub provenance: SkillUsageProvenance,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_turn_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime_queue_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivery_mode: Option<SkillDeliveryMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body_checksum: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selection_receipt_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillUsageReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub events_file: PathBuf,
    pub snapshot_file: PathBuf,
    pub records_appended: usize,
    pub snapshot: SkillUsageSnapshot,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillUsageSnapshot {
    pub schema: String,
    pub harness_home: PathBuf,
    pub events_file: PathBuf,
    pub total_events: usize,
    pub by_action: BTreeMap<String, usize>,
    pub by_skill: BTreeMap<String, usize>,
    pub by_provenance: BTreeMap<String, usize>,
    pub latest_at_ms: Option<i64>,
}

pub fn skill_usage_events_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("learning")
        .join("skill-usage.jsonl")
}

pub fn skill_usage_snapshot_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("state")
        .join("learning")
        .join("skill-usage-snapshot.json")
}

pub fn record_skill_usage_event(options: SkillUsageEventOptions) -> io::Result<SkillUsageReport> {
    let record = SkillUsageRecord {
        schema: SKILL_USAGE_EVENT_SCHEMA,
        at_ms: options.now_ms,
        action: options.action,
        skill_id: options.skill_id,
        source_kind: options.source_kind,
        provenance: classify_skill_provenance(options.source_kind),
        source_turn_id: options.source_turn_id,
        runtime_queue_id: options.runtime_queue_id,
        session_key: options.session_key,
        channel: options.channel,
        agent_id: options.agent_id,
        delivery_mode: options.delivery_mode,
        body_checksum: options.body_checksum,
        selection_receipt_id: options.selection_receipt_id,
        reason: options.reason,
    };
    let events_file = skill_usage_events_file(&options.harness_home);
    append_jsonl_value(&events_file, &record)?;
    let snapshot = collect_skill_usage_snapshot(&options.harness_home)?;
    let snapshot_file = skill_usage_snapshot_file(&options.harness_home);
    write_json_atomic(&snapshot_file, &snapshot)?;
    Ok(SkillUsageReport {
        schema: SKILL_USAGE_EVENT_SCHEMA,
        harness_home: options.harness_home,
        events_file,
        snapshot_file,
        records_appended: 1,
        snapshot,
    })
}

pub fn record_skill_usage_from_prompt_bundle(
    harness_home: impl AsRef<Path>,
    prompt_bundle_json: impl AsRef<Path>,
    runtime_queue_id: Option<&str>,
    reason: &str,
) -> io::Result<SkillUsageReport> {
    let harness_home = harness_home.as_ref();
    let text = fs::read_to_string(prompt_bundle_json.as_ref())?;
    let value: Value = serde_json::from_str(&text).map_err(io::Error::other)?;
    let now_ms = current_log_time_ms().unwrap_or(0);
    let session_key = string_value(&value, "sessionKey");
    let agent_id = string_value(&value, "agentId");
    let channel = value
        .get("sections")
        .and_then(Value::as_array)
        .and_then(|sections| {
            sections.iter().find_map(|section| {
                (section.get("kind").and_then(Value::as_str) == Some("runtime-context"))
                    .then(|| {
                        section
                            .get("content")
                            .and_then(Value::as_str)
                            .and_then(parse_platform_from_runtime_context)
                    })
                    .flatten()
            })
        });
    let mut appended = 0usize;
    if let Some(selections) = value.get("selectedSkills").and_then(Value::as_array) {
        for selection in selections {
            let Some(skill_id) = string_value(selection, "skillId") else {
                continue;
            };
            append_usage_record(
                harness_home,
                SkillUsageAction::Selected,
                &skill_id,
                source_kind_from_value(selection.get("sourceKind")),
                runtime_queue_id,
                session_key.as_deref(),
                channel.as_deref(),
                agent_id.as_deref(),
                delivery_mode_from_value(selection.get("deliveryMode")),
                string_value(selection, "bodyChecksum"),
                string_value(selection, "selectionReceiptId"),
                Some(reason.to_string()),
                now_ms,
            )?;
            appended += 1;
        }
    }
    if let Some(sections) = value.get("sections").and_then(Value::as_array) {
        for section in sections {
            if section.get("kind").and_then(Value::as_str) != Some("skill") {
                continue;
            }
            let Some(skill_id) = string_value(section, "skillId") else {
                continue;
            };
            let delivery_mode = delivery_mode_from_value(section.get("deliveryMode"));
            let action = if delivery_mode == Some(SkillDeliveryMode::InvocationEnvelope) {
                SkillUsageAction::Invoked
            } else {
                SkillUsageAction::Injected
            };
            append_usage_record(
                harness_home,
                action,
                &skill_id,
                None,
                runtime_queue_id,
                session_key.as_deref(),
                channel.as_deref(),
                agent_id.as_deref(),
                delivery_mode,
                string_value(section, "bodyChecksum"),
                None,
                Some(reason.to_string()),
                now_ms,
            )?;
            appended += 1;
        }
    }
    let snapshot = collect_skill_usage_snapshot(harness_home)?;
    let snapshot_file = skill_usage_snapshot_file(harness_home);
    write_json_atomic(&snapshot_file, &snapshot)?;
    Ok(SkillUsageReport {
        schema: SKILL_USAGE_EVENT_SCHEMA,
        harness_home: harness_home.to_path_buf(),
        events_file: skill_usage_events_file(harness_home),
        snapshot_file,
        records_appended: appended,
        snapshot,
    })
}

pub fn collect_skill_usage_snapshot(
    harness_home: impl AsRef<Path>,
) -> io::Result<SkillUsageSnapshot> {
    let harness_home = harness_home.as_ref();
    let events_file = skill_usage_events_file(harness_home);
    let mut snapshot = SkillUsageSnapshot {
        schema: SKILL_USAGE_SNAPSHOT_SCHEMA.to_string(),
        harness_home: harness_home.to_path_buf(),
        events_file: events_file.clone(),
        ..SkillUsageSnapshot::default()
    };
    let text = match fs::read_to_string(&events_file) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(snapshot),
        Err(error) => return Err(error),
    };
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let Ok(record) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let action = delivery_action_from_value(record.get("action"));
        let provenance = provenance_from_value(record.get("provenance"));
        let Some(skill_id) = string_value(&record, "skillId") else {
            continue;
        };
        snapshot.total_events += 1;
        if let Some(action) = action {
            *snapshot.by_action.entry(action.to_string()).or_default() += 1;
        }
        *snapshot.by_skill.entry(skill_id).or_default() += 1;
        if let Some(provenance) = provenance {
            *snapshot
                .by_provenance
                .entry(provenance.to_string())
                .or_default() += 1;
        }
        snapshot.latest_at_ms = snapshot
            .latest_at_ms
            .max(record.get("atMs").and_then(Value::as_i64));
    }
    Ok(snapshot)
}

pub fn classify_skill_provenance(source_kind: Option<SkillSourceKind>) -> SkillUsageProvenance {
    match source_kind {
        Some(SkillSourceKind::HarnessBuiltin) => SkillUsageProvenance::BundledHarnessSkill,
        Some(
            SkillSourceKind::ImportedWorkspace
            | SkillSourceKind::ImportedManaged
            | SkillSourceKind::ImportedProjectAgent,
        ) => SkillUsageProvenance::ImportedLegacySkill,
        Some(SkillSourceKind::Workspace | SkillSourceKind::Managed) => {
            SkillUsageProvenance::OperatorAuthoredSkill
        }
        Some(SkillSourceKind::ProjectAgent) => SkillUsageProvenance::ExternalProjectSkill,
        None => SkillUsageProvenance::Unknown,
    }
}

fn append_usage_record(
    harness_home: &Path,
    action: SkillUsageAction,
    skill_id: &str,
    source_kind: Option<SkillSourceKind>,
    runtime_queue_id: Option<&str>,
    session_key: Option<&str>,
    channel: Option<&str>,
    agent_id: Option<&str>,
    delivery_mode: Option<SkillDeliveryMode>,
    body_checksum: Option<String>,
    selection_receipt_id: Option<String>,
    reason: Option<String>,
    now_ms: i64,
) -> io::Result<()> {
    append_jsonl_value(
        &skill_usage_events_file(harness_home),
        &SkillUsageRecord {
            schema: SKILL_USAGE_EVENT_SCHEMA,
            at_ms: now_ms,
            action,
            skill_id: skill_id.to_string(),
            source_kind,
            provenance: classify_skill_provenance(source_kind),
            source_turn_id: None,
            runtime_queue_id: runtime_queue_id.map(ToString::to_string),
            session_key: session_key.map(ToString::to_string),
            channel: channel.map(ToString::to_string),
            agent_id: agent_id.map(ToString::to_string),
            delivery_mode,
            body_checksum,
            selection_receipt_id,
            reason,
        },
    )
}

fn string_value(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

fn source_kind_from_value(value: Option<&Value>) -> Option<SkillSourceKind> {
    match value.and_then(Value::as_str)? {
        "workspace" => Some(SkillSourceKind::Workspace),
        "managed" => Some(SkillSourceKind::Managed),
        "project-agent" => Some(SkillSourceKind::ProjectAgent),
        "imported-workspace" => Some(SkillSourceKind::ImportedWorkspace),
        "imported-managed" => Some(SkillSourceKind::ImportedManaged),
        "imported-project-agent" => Some(SkillSourceKind::ImportedProjectAgent),
        "harness-builtin" => Some(SkillSourceKind::HarnessBuiltin),
        _ => None,
    }
}

fn delivery_mode_from_value(value: Option<&Value>) -> Option<SkillDeliveryMode> {
    match value.and_then(Value::as_str)? {
        "index-only" => Some(SkillDeliveryMode::IndexOnly),
        "injected-body" => Some(SkillDeliveryMode::InjectedBody),
        "invocation-envelope" => Some(SkillDeliveryMode::InvocationEnvelope),
        "tool-view" => Some(SkillDeliveryMode::ToolView),
        _ => None,
    }
}

fn delivery_action_from_value(value: Option<&Value>) -> Option<&'static str> {
    match value.and_then(Value::as_str)? {
        "selected" => Some("selected"),
        "injected" => Some("injected"),
        "invoked" => Some("invoked"),
        "viewed" => Some("viewed"),
        "proposed" => Some("proposed"),
        "patched" => Some("patched"),
        "archived" => Some("archived"),
        "rejected" => Some("rejected"),
        _ => None,
    }
}

fn provenance_from_value(value: Option<&Value>) -> Option<&'static str> {
    match value.and_then(Value::as_str)? {
        "bundled-harness-skill" => Some("bundled-harness-skill"),
        "imported-legacy-skill" => Some("imported-legacy-skill"),
        "operator-authored-skill" => Some("operator-authored-skill"),
        "agent-created-skill" => Some("agent-created-skill"),
        "external-project-skill" => Some("external-project-skill"),
        "unknown" => Some("unknown"),
        _ => None,
    }
}

fn parse_platform_from_runtime_context(content: &str) -> Option<String> {
    content.lines().find_map(|line| {
        line.strip_prefix("platform:")
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string)
    })
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use serde_json::json;

    use super::*;

    #[test]
    fn skill_usage_records_events_and_snapshot() {
        let root = temp_root("skill_usage_records_events_and_snapshot");
        let home = root.join(".openclaw");
        let report = record_skill_usage_event(SkillUsageEventOptions {
            harness_home: home.clone(),
            action: SkillUsageAction::Injected,
            skill_id: "workspace:triage".to_string(),
            source_kind: Some(SkillSourceKind::Workspace),
            source_turn_id: Some("turn-1".to_string()),
            runtime_queue_id: Some("queue-1".to_string()),
            session_key: Some("telegram:dm:user:main".to_string()),
            channel: Some("telegram".to_string()),
            agent_id: Some("main".to_string()),
            delivery_mode: Some(SkillDeliveryMode::InjectedBody),
            body_checksum: Some("sha256:test".to_string()),
            selection_receipt_id: Some("sel-1".to_string()),
            reason: Some("test".to_string()),
            now_ms: 42,
        })
        .unwrap();

        assert_eq!(report.records_appended, 1);
        assert_eq!(report.snapshot.total_events, 1);
        assert_eq!(
            report.snapshot.by_provenance.get("operator-authored-skill"),
            Some(&1)
        );
        assert!(skill_usage_events_file(&home).is_file());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn skill_usage_reads_prompt_bundle_selected_and_invoked() {
        let root = temp_root("skill_usage_reads_prompt_bundle_selected_and_invoked");
        let home = root.join(".openclaw");
        let prompt = root.join("prompt-bundle.json");
        fs::create_dir_all(&root).unwrap();
        fs::write(
            &prompt,
            serde_json::to_string(&json!({
                "sessionKey": "telegram:dm:user:main",
                "agentId": "main",
                "selectedSkills": [{
                    "skillId": "workspace:triage",
                    "sourceKind": "workspace",
                    "deliveryMode": "invocation-envelope",
                    "bodyChecksum": "sha256:abc",
                    "selectionReceiptId": "sel-1"
                }],
                "sections": [{
                    "kind": "runtime-context",
                    "content": "platform: telegram\n"
                }, {
                    "kind": "skill",
                    "skillId": "workspace:triage",
                    "deliveryMode": "invocation-envelope",
                    "bodyChecksum": "sha256:abc"
                }]
            }))
            .unwrap(),
        )
        .unwrap();

        let report =
            record_skill_usage_from_prompt_bundle(&home, &prompt, Some("queue-1"), "planned")
                .unwrap();
        assert_eq!(report.records_appended, 2);
        assert_eq!(report.snapshot.by_action.get("selected"), Some(&1));
        assert_eq!(report.snapshot.by_action.get("invoked"), Some(&1));

        let _ = fs::remove_dir_all(root);
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-skill-usage-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
