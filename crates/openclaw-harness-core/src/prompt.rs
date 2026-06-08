use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::{SKILL_FILE_NAME, TurnDispatch, TurnPlan};

const PROMPT_BUNDLE_SCHEMA: &str = "openclaw-harness.prompt-bundle.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromptAssemblyOptions {
    pub max_prompt_file_bytes: usize,
    pub max_skill_file_bytes: usize,
    pub harness_home: Option<PathBuf>,
}

impl Default for PromptAssemblyOptions {
    fn default() -> Self {
        Self {
            max_prompt_file_bytes: 64 * 1024,
            max_skill_file_bytes: 96 * 1024,
            harness_home: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PromptBundle {
    pub schema: &'static str,
    pub dispatch: TurnDispatch,
    pub session_key: String,
    pub agent_id: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub summary: PromptBundleSummary,
    pub sections: Vec<PromptSection>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PromptBundleSummary {
    pub prompt_files_included: usize,
    pub prompt_files_reused: usize,
    pub channel_state_sections_included: usize,
    pub skills_included: usize,
    pub skills_reused: usize,
    pub session_continuity_sections_included: usize,
    pub user_messages_included: usize,
    pub bytes_included: usize,
    pub truncated_sections: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PromptSection {
    pub kind: PromptSectionKind,
    pub title: String,
    pub path: Option<PathBuf>,
    pub bytes_original: usize,
    pub bytes_included: usize,
    pub truncated: bool,
    pub content: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum PromptSectionKind {
    RuntimeContext,
    ChannelState,
    SessionContinuity,
    PromptFile,
    Skill,
    UserMessage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromptBundleFiles {
    pub json: PathBuf,
    pub markdown: PathBuf,
}

pub fn assemble_prompt_bundle(
    plan: &TurnPlan,
    options: PromptAssemblyOptions,
) -> io::Result<PromptBundle> {
    let mut sections = Vec::new();
    let mut warnings = plan.warnings.clone();

    sections.push(runtime_context_section(plan));
    if let Some(state) = &plan.channel_state {
        sections.push(channel_state_section(state));
    }

    if plan.dispatch != TurnDispatch::AgentTurn {
        warnings.push(format!(
            "prompt bundle is informational only because dispatch is {:?}",
            plan.dispatch
        ));
    } else {
        warnings.push(
            "session context reuse is disabled until Codex backend thread resume is implemented; authoritative OpenClaw prompt files and skills are injected every agent turn".to_string(),
        );
        sections.push(openclaw_identity_section(plan));

        for prompt_file in &plan.prompt_files {
            if !prompt_file.exists {
                continue;
            }
            sections.push(read_limited_section(
                PromptSectionKind::PromptFile,
                prompt_file.name.clone(),
                &prompt_file.path,
                options.max_prompt_file_bytes,
            )?);
        }

        for skill in &plan.selected_skills {
            let skill_file = skill.directory.join(SKILL_FILE_NAME);
            if !skill_file.is_file() {
                warnings.push(format!(
                    "selected skill `{}` has no {} at {}",
                    skill.skill_id,
                    SKILL_FILE_NAME,
                    skill_file.display()
                ));
                continue;
            }
            sections.push(read_limited_section(
                PromptSectionKind::Skill,
                format!("{} ({})", skill.title, skill.skill_id),
                &skill_file,
                options.max_skill_file_bytes,
            )?);
        }

        sections.push(PromptSection {
            kind: PromptSectionKind::UserMessage,
            title: "Inbound message".to_string(),
            path: None,
            bytes_original: plan.message_text.len(),
            bytes_included: plan.message_text.len(),
            truncated: false,
            content: plan.message_text.clone(),
        });
    }

    let summary = summarize_sections(&sections);
    Ok(PromptBundle {
        schema: PROMPT_BUNDLE_SCHEMA,
        dispatch: plan.dispatch,
        session_key: plan.session_key.clone(),
        agent_id: plan.agent.as_ref().map(|agent| agent.id.clone()),
        provider: plan.model_policy.provider.clone(),
        model: plan.model_policy.model.clone(),
        summary,
        sections,
        warnings,
    })
}

pub fn write_prompt_bundle(
    bundle: &PromptBundle,
    output_dir: impl AsRef<Path>,
) -> io::Result<PromptBundleFiles> {
    let output_dir = output_dir.as_ref();
    fs::create_dir_all(output_dir)?;
    let json = output_dir.join("prompt-bundle.json");
    let markdown = output_dir.join("prompt.md");
    let json_text = serde_json::to_string_pretty(bundle).map_err(io::Error::other)?;
    fs::write(&json, json_text)?;
    fs::write(&markdown, render_prompt_markdown(bundle))?;
    Ok(PromptBundleFiles { json, markdown })
}

fn runtime_context_section(plan: &TurnPlan) -> PromptSection {
    let agent_id = plan
        .agent
        .as_ref()
        .map(|agent| agent.id.as_str())
        .unwrap_or("none");
    let content = format!(
        "dispatch: {:?}\nplatform: {}\nchannel_id: {}\nuser_id: {}\nsession_key: {}\nagent_id: {}\nprovider: {}\nmodel: {}",
        plan.dispatch,
        plan.platform,
        plan.channel_id,
        plan.user_id,
        plan.session_key,
        agent_id,
        plan.model_policy.provider.as_deref().unwrap_or("-"),
        plan.model_policy.model.as_deref().unwrap_or("-"),
    );
    let bytes = content.len();
    PromptSection {
        kind: PromptSectionKind::RuntimeContext,
        title: "Runtime context".to_string(),
        path: None,
        bytes_original: bytes,
        bytes_included: bytes,
        truncated: false,
        content,
    }
}

fn openclaw_identity_section(plan: &TurnPlan) -> PromptSection {
    let agent_id = plan
        .agent
        .as_ref()
        .map(|agent| agent.id.as_str())
        .unwrap_or("unknown");
    let content = format!(
        "\
You are executing an OpenClaw agent turn through a Codex backend runtime.

User-facing identity contract:
- The chat-facing agent identity is the imported OpenClaw agent `{agent_id}`.
- Follow the SOUL.md, AGENTS.md, MEMORY.md, selected skills, and channel state included in this bundle as the authoritative agent context.
- Do not introduce yourself as Codex, OpenAI, Codex CLI, or a generic programming assistant unless the user specifically asks about the backend/runtime implementation.
- Do not mention harness development workspace rules, this prompt bundle, injection ledgers, or backend plumbing to the Telegram/Discord user unless asked for runtime diagnostics.
- If a harness-development instruction conflicts with imported OpenClaw agent context for chat-facing behavior, keep the harness instruction for local safety but answer the channel user with the imported OpenClaw persona and operating rules.

Backend continuity note:
- This harness currently starts a fresh Codex app-server thread per runtime execution. Authoritative OpenClaw prompt files and skills are therefore repeated every agent turn until true Codex thread resume is implemented.
",
    );
    let bytes = content.len();
    PromptSection {
        kind: PromptSectionKind::RuntimeContext,
        title: "OpenClaw runtime identity contract".to_string(),
        path: None,
        bytes_original: bytes,
        bytes_included: bytes,
        truncated: false,
        content,
    }
}

fn channel_state_section(state: &crate::ChannelSessionState) -> PromptSection {
    let mut content = String::new();
    content.push_str(&format!(
        "active_session_key: {}\n",
        state.active_session_key
    ));
    content.push_str(&format!(
        "session_topic: {}\n",
        state.session_topic.as_deref().unwrap_or("-")
    ));
    content.push_str(&format!(
        "model_override: {}\n",
        state.model_override.as_deref().unwrap_or("-")
    ));
    content.push_str(&format!("thinking_enabled: {}\n", state.thinking_enabled));
    content.push_str(&format!(
        "thinking_instruction: {}\n",
        state.thinking_instruction.as_deref().unwrap_or("-")
    ));
    content.push_str(&format!("stop_requested: {}\n", state.stop_requested));
    content.push_str(&format!(
        "stop_reason: {}\n",
        state.stop_reason.as_deref().unwrap_or("-")
    ));
    push_notes(&mut content, "steering", &state.steering_notes);
    push_notes(&mut content, "btw", &state.btw_notes);
    let bytes = content.len();
    PromptSection {
        kind: PromptSectionKind::ChannelState,
        title: "Channel command state".to_string(),
        path: None,
        bytes_original: bytes,
        bytes_included: bytes,
        truncated: false,
        content,
    }
}

fn push_notes(out: &mut String, label: &str, notes: &[crate::ChannelSessionNote]) {
    out.push_str(&format!("{label}_notes_recent:\n"));
    if notes.is_empty() {
        out.push_str("- -\n");
        return;
    }
    for note in notes.iter().rev().take(8).rev() {
        out.push_str(&format!("- [{}] {}\n", note.at_ms, note.text));
    }
}

fn read_limited_section(
    kind: PromptSectionKind,
    title: String,
    path: &Path,
    max_bytes: usize,
) -> io::Result<PromptSection> {
    let bytes = fs::read(path)?;
    let bytes_original = bytes.len();
    let limit = max_bytes.max(1).min(bytes_original);
    let truncated = bytes_original > limit;
    let content = String::from_utf8_lossy(&bytes[..limit]).into_owned();
    Ok(PromptSection {
        kind,
        title,
        path: Some(path.to_path_buf()),
        bytes_original,
        bytes_included: limit,
        truncated,
        content,
    })
}

fn summarize_sections(sections: &[PromptSection]) -> PromptBundleSummary {
    let mut summary = PromptBundleSummary::default();
    for section in sections {
        match section.kind {
            PromptSectionKind::RuntimeContext => {}
            PromptSectionKind::ChannelState => summary.channel_state_sections_included += 1,
            PromptSectionKind::SessionContinuity => {
                summary.session_continuity_sections_included += 1;
            }
            PromptSectionKind::PromptFile => summary.prompt_files_included += 1,
            PromptSectionKind::Skill => summary.skills_included += 1,
            PromptSectionKind::UserMessage => summary.user_messages_included += 1,
        }
        summary.bytes_included += section.bytes_included;
        if section.truncated {
            summary.truncated_sections += 1;
        }
    }
    summary
}

fn render_prompt_markdown(bundle: &PromptBundle) -> String {
    let mut out = String::new();
    out.push_str("# OpenClaw Prompt Bundle\n\n");
    out.push_str(&format!("- Dispatch: `{:?}`\n", bundle.dispatch));
    out.push_str(&format!("- Session key: `{}`\n", bundle.session_key));
    out.push_str(&format!(
        "- Agent: `{}`\n",
        bundle.agent_id.as_deref().unwrap_or("-")
    ));
    out.push_str(&format!(
        "- Provider/model: `{}` / `{}`\n",
        bundle.provider.as_deref().unwrap_or("-"),
        bundle.model.as_deref().unwrap_or("-")
    ));
    out.push_str(&format!(
        "- Prompt files: `{}`\n",
        bundle.summary.prompt_files_included
    ));
    out.push_str(&format!(
        "- Reused prompt files: `{}`\n",
        bundle.summary.prompt_files_reused
    ));
    out.push_str(&format!(
        "- Channel state sections: `{}`\n",
        bundle.summary.channel_state_sections_included
    ));
    out.push_str(&format!("- Skills: `{}`\n", bundle.summary.skills_included));
    out.push_str(&format!(
        "- Reused skills: `{}`\n",
        bundle.summary.skills_reused
    ));
    out.push_str(&format!(
        "- Session continuity sections: `{}`\n",
        bundle.summary.session_continuity_sections_included
    ));
    out.push_str(&format!(
        "- Truncated sections: `{}`\n\n",
        bundle.summary.truncated_sections
    ));

    if !bundle.warnings.is_empty() {
        out.push_str("## Warnings\n\n");
        for warning in &bundle.warnings {
            out.push_str(&format!("- {}\n", escape_markdown_line(warning)));
        }
        out.push('\n');
    }

    for section in &bundle.sections {
        out.push_str(&format!("## {:?}: {}\n\n", section.kind, section.title));
        if let Some(path) = &section.path {
            out.push_str(&format!("- Path: `{}`\n", path.display()));
        }
        out.push_str(&format!(
            "- Bytes: `{}` / `{}`\n",
            section.bytes_included, section.bytes_original
        ));
        out.push_str(&format!("- Truncated: `{}`\n\n", section.truncated));
        out.push_str("```text\n");
        out.push_str(&section.content);
        if !section.content.ends_with('\n') {
            out.push('\n');
        }
        out.push_str("```\n\n");
    }
    out
}

fn escape_markdown_line(value: &str) -> String {
    value.replace('|', "\\|")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        OpenClawSource, TurnPlanInput, build_source_skill_index, build_turn_plan,
        load_agent_registry,
    };
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn prompt_bundle_includes_prompt_files_skills_and_user_message() {
        let root = temp_root("prompt_bundle_includes_prompt_files_skills_and_user_message");
        let source = write_prompt_source(&root);
        let registry = load_agent_registry(&source).unwrap();
        let skills = build_source_skill_index(&source).unwrap();
        let plan = build_turn_plan(
            &source,
            &registry,
            &skills,
            TurnPlanInput {
                harness_home: None,
                platform: "telegram".to_string(),
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                text: "repair memory cron".to_string(),
                requested_agent_id: Some("main".to_string()),
                session_hint: None,
                skill_limit: 3,
            },
        )
        .unwrap();

        let bundle = assemble_prompt_bundle(&plan, PromptAssemblyOptions::default()).unwrap();

        assert_eq!(bundle.dispatch, TurnDispatch::AgentTurn);
        assert_eq!(bundle.agent_id.as_deref(), Some("main"));
        assert_eq!(bundle.summary.prompt_files_included, 2);
        assert_eq!(bundle.summary.skills_included, 1);
        assert_eq!(bundle.summary.user_messages_included, 1);
        assert!(bundle.sections.iter().any(
            |section| section.title == "AGENTS.md" && section.content.contains("Agent prompt")
        ));
        assert!(
            bundle
                .sections
                .iter()
                .any(|section| section.kind == PromptSectionKind::Skill
                    && section.content.contains("Memory Cron"))
        );
        assert!(
            bundle
                .sections
                .iter()
                .any(|section| section.kind == PromptSectionKind::UserMessage
                    && section.content == "repair memory cron")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prompt_bundle_truncates_large_sections() {
        let root = temp_root("prompt_bundle_truncates_large_sections");
        let source = write_prompt_source(&root);
        fs::write(source.workspace.join("SOUL.md"), "abcdef").unwrap();
        let registry = load_agent_registry(&source).unwrap();
        let skills = build_source_skill_index(&source).unwrap();
        let plan = build_turn_plan(
            &source,
            &registry,
            &skills,
            TurnPlanInput {
                harness_home: None,
                platform: "telegram".to_string(),
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                text: "repair memory cron".to_string(),
                requested_agent_id: Some("main".to_string()),
                session_hint: None,
                skill_limit: 3,
            },
        )
        .unwrap();

        let bundle = assemble_prompt_bundle(
            &plan,
            PromptAssemblyOptions {
                max_prompt_file_bytes: 3,
                max_skill_file_bytes: 4,
                harness_home: None,
            },
        )
        .unwrap();

        assert!(bundle.summary.truncated_sections >= 2);
        assert!(
            bundle
                .sections
                .iter()
                .any(|section| section.title == "SOUL.md"
                    && section.truncated
                    && section.content == "abc")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prompt_bundle_includes_channel_command_state() {
        let root = temp_root("prompt_bundle_includes_channel_command_state");
        let source = write_prompt_source(&root);
        let harness_home = root.join(".openclaw-harness");
        write_channel_state(
            &harness_home,
            r#"{
              "schema": "openclaw-harness.channel-session-state.v1",
              "platform": "telegram",
              "channelId": "dm",
              "userId": "user",
              "activeSessionKey": "telegram:dm:user:main:new",
              "agentId": "main",
              "provider": "openai",
              "model": "gpt-5",
              "sessionTopic": "handoff",
              "modelOverride": "openrouter/anthropic/claude-sonnet-4",
              "modelOverrideProvider": "openrouter",
              "modelOverrideModel": "anthropic/claude-sonnet-4",
              "thinkingEnabled": true,
              "thinkingInstruction": "check imported cron state",
              "stopRequested": false,
              "stopReason": null,
              "steeringNotes": [
                { "atMs": 1000, "text": "keep migration notes explicit" }
              ],
              "btwNotes": [
                { "atMs": 1001, "text": "user prefers Codex OAuth" }
              ],
              "lastCommand": "btw",
              "updatedAtMs": 1001
            }"#,
        );
        let registry = load_agent_registry(&source).unwrap();
        let skills = build_source_skill_index(&source).unwrap();
        let plan = build_turn_plan(
            &source,
            &registry,
            &skills,
            TurnPlanInput {
                harness_home: Some(harness_home),
                platform: "telegram".to_string(),
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                text: "continue migration".to_string(),
                requested_agent_id: Some("main".to_string()),
                session_hint: None,
                skill_limit: 3,
            },
        )
        .unwrap();

        let bundle = assemble_prompt_bundle(&plan, PromptAssemblyOptions::default()).unwrap();

        assert_eq!(bundle.summary.channel_state_sections_included, 1);
        assert_eq!(bundle.provider.as_deref(), Some("openrouter"));
        assert_eq!(bundle.model.as_deref(), Some("anthropic/claude-sonnet-4"));
        assert!(bundle.sections.iter().any(|section| {
            section.kind == PromptSectionKind::ChannelState
                && section.content.contains("keep migration notes explicit")
                && section.content.contains("user prefers Codex OAuth")
        }));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prompt_bundle_reinjects_context_until_backend_thread_resume_exists() {
        let root = temp_root("prompt_bundle_reinjects_context_until_backend_thread_resume_exists");
        let source = write_prompt_source(&root);
        let harness_home = root.join(".openclaw-harness");
        let registry = load_agent_registry(&source).unwrap();
        let skills = build_source_skill_index(&source).unwrap();
        let input = TurnPlanInput {
            harness_home: Some(harness_home.clone()),
            platform: "telegram".to_string(),
            channel_id: "dm".to_string(),
            user_id: "user".to_string(),
            text: "repair memory cron".to_string(),
            requested_agent_id: Some("main".to_string()),
            session_hint: Some("telegram:dm:user:main".to_string()),
            skill_limit: 3,
        };
        let first_plan = build_turn_plan(&source, &registry, &skills, input.clone()).unwrap();
        let first = assemble_prompt_bundle(
            &first_plan,
            PromptAssemblyOptions {
                harness_home: Some(harness_home.clone()),
                ..PromptAssemblyOptions::default()
            },
        )
        .unwrap();
        assert_eq!(first.summary.prompt_files_included, 2);
        assert_eq!(first.summary.skills_included, 1);
        assert_eq!(first.summary.prompt_files_reused, 0);
        assert_eq!(first.summary.skills_reused, 0);

        let second_plan = build_turn_plan(&source, &registry, &skills, input).unwrap();
        let second = assemble_prompt_bundle(
            &second_plan,
            PromptAssemblyOptions {
                harness_home: Some(harness_home),
                ..PromptAssemblyOptions::default()
            },
        )
        .unwrap();

        assert_eq!(second.summary.prompt_files_included, 2);
        assert_eq!(second.summary.skills_included, 1);
        assert_eq!(second.summary.prompt_files_reused, 0);
        assert_eq!(second.summary.skills_reused, 0);
        assert_eq!(second.summary.session_continuity_sections_included, 0);
        assert!(second.sections.iter().any(|section| {
            section.kind == PromptSectionKind::RuntimeContext
                && section.title == "OpenClaw runtime identity contract"
                && section.content.contains("fresh Codex app-server thread")
        }));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prompt_bundle_does_not_assemble_command_as_agent_prompt() {
        let root = temp_root("prompt_bundle_does_not_assemble_command_as_agent_prompt");
        let source = write_prompt_source(&root);
        let registry = load_agent_registry(&source).unwrap();
        let skills = build_source_skill_index(&source).unwrap();
        let plan = build_turn_plan(
            &source,
            &registry,
            &skills,
            TurnPlanInput {
                harness_home: None,
                platform: "discord".to_string(),
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                text: "/status cron".to_string(),
                requested_agent_id: Some("main".to_string()),
                session_hint: None,
                skill_limit: 3,
            },
        )
        .unwrap();

        let bundle = assemble_prompt_bundle(&plan, PromptAssemblyOptions::default()).unwrap();

        assert_eq!(bundle.dispatch, TurnDispatch::ChannelCommand);
        assert_eq!(bundle.summary.prompt_files_included, 0);
        assert_eq!(bundle.summary.skills_included, 0);
        assert_eq!(bundle.summary.user_messages_included, 0);
        assert!(
            bundle
                .warnings
                .iter()
                .any(|warning| warning.contains("informational only"))
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn write_prompt_bundle_outputs_json_and_markdown() {
        let root = temp_root("write_prompt_bundle_outputs_json_and_markdown");
        let source = write_prompt_source(&root);
        let registry = load_agent_registry(&source).unwrap();
        let skills = build_source_skill_index(&source).unwrap();
        let plan = build_turn_plan(
            &source,
            &registry,
            &skills,
            TurnPlanInput {
                harness_home: None,
                platform: "telegram".to_string(),
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                text: "repair memory cron".to_string(),
                requested_agent_id: Some("main".to_string()),
                session_hint: None,
                skill_limit: 3,
            },
        )
        .unwrap();
        let bundle = assemble_prompt_bundle(&plan, PromptAssemblyOptions::default()).unwrap();

        let files = write_prompt_bundle(&bundle, root.join("out")).unwrap();

        assert!(files.json.is_file());
        assert!(files.markdown.is_file());
        let json: serde_json::Value =
            serde_json::from_slice(&fs::read(files.json).unwrap()).unwrap();
        assert_eq!(json["schema"], PROMPT_BUNDLE_SCHEMA);
        assert!(
            fs::read_to_string(files.markdown)
                .unwrap()
                .contains("OpenClaw Prompt Bundle")
        );

        let _ = fs::remove_dir_all(root);
    }

    fn write_prompt_source(root: &Path) -> OpenClawSource {
        let home = root.join(".openclaw");
        let workspace = home.join("workspace");
        let skill = workspace.join("skills").join("memory-cron");
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(&skill).unwrap();
        fs::create_dir_all(home.join("agents").join("main").join("sessions")).unwrap();
        fs::write(workspace.join("AGENTS.md"), "# Agent prompt").unwrap();
        fs::write(workspace.join("SOUL.md"), "# Soul prompt").unwrap();
        fs::write(
            skill.join(crate::SKILL_FILE_NAME),
            "# Memory Cron\n\nRepair memory cron jobs.",
        )
        .unwrap();
        fs::write(
            home.join("openclaw.json"),
            r#"{
              "agents": {
                "defaults": { "provider": "openai", "model": "codex" },
                "list": [
                  { "id": "main", "model": "gpt-5", "enabled": true }
                ]
              }
            }"#,
        )
        .unwrap();
        fs::write(
            home.join("agents")
                .join("main")
                .join("sessions")
                .join("sessions.json"),
            "{}",
        )
        .unwrap();
        OpenClawSource::with_workspace(home, workspace)
    }

    fn write_channel_state(harness_home: &Path, state_json: &str) {
        let state_file = harness_home
            .join("state")
            .join("channels")
            .join("telegram")
            .join("dm")
            .join("user")
            .join("state.json");
        fs::create_dir_all(state_file.parent().unwrap()).unwrap();
        fs::write(state_file, state_json).unwrap();
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "openclaw-harness-prompt-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
