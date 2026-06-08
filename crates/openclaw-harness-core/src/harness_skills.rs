use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::{HARNESS_BUILTIN_SKILL_NAMESPACE, SKILL_FILE_NAME};

const BUILTIN_HARNESS_SKILL_SYNC_SCHEMA: &str = "openclaw-harness.builtin-skill-sync.v1";
const BUILTIN_HARNESS_SKILL_MANIFEST_SCHEMA: &str = "openclaw-harness.builtin-skill-manifest.v1";
const OPENCLAW_WINDOWS_HARNESS_SKILL_ID: &str = "openclaw-windows-harness";
const OPENCLAW_WINDOWS_HARNESS_SKILL_VERSION: &str = env!("CARGO_PKG_VERSION");

const OPENCLAW_WINDOWS_HARNESS_SKILL: &str = r#"---
name: openclaw-windows-harness
description: Operate the Rust Windows OpenClaw core harness, channel commands, activation handoff, and Codex prompt continuity policy.
version: 0.1.0
platforms: [windows]
metadata:
  openclaw_harness:
    category: operations
    tags: [openclaw, codex, telegram, discord, migration, activation]
---

# OpenClaw Windows Harness

## When to Use

Use this skill at the start of turns that operate, debug, activate, migrate, or extend the Rust Windows 11 OpenClaw harness.

Use it when the user mentions:

- importing OpenClaw state, memory, cron, plugins, sessions, agents, subagents, or workspace files
- Telegram or Discord DM operation
- slash commands such as /new, /think, /stop, /steer, /btw, /model, or /status
- Codex CLI, Codex OAuth, app-server, prompt injection, tool schema, or session continuity
- activation readiness, runtime queue, operational logs, or gateway handoff

## Operating Lead

1. Treat this skill as the versioned harness runbook. Check it before relying on older docs or session memory.
2. Treat the harness as the orchestrator and Codex CLI as the model/tool runtime.
3. Preserve OpenClaw state shape where possible: source workspace, prompt files, agent registry, sessions, memory files, cron state, plugin state, and receipts.
4. Prefer dry-run, receipt, and append-only JSONL records before irreversible handoff.
5. Keep deterministic cron off the LLM path. Agent-turn cron may enqueue runtime work.
6. Keep Telegram and Discord session keys stable: platform, channel id, user id, and agent id determine continuity unless /new changes it.
7. Keep multi-agent readiness intact. Do not collapse imported agents into a single default agent.
8. Treat credentials as best-effort imports. Codex OAuth is preferred for Codex models; API keys may be provider-specific and model-limited.
9. Treat memory/qdrant-edge as the primary memory backend when present. LanceDB is backup/optional unless the active OpenClaw config points to it.
10. Use a Codex CLI binary that the harness can spawn. On Windows, the Codex Desktop MSIX resource path may be visible on PATH but fail with os error 5; prefer a standalone release or local npm install and pass it with --codex-exe.

## Prompt And Tool Schema Policy

The harness does not own the Codex system prompt or Codex tool schema. Codex CLI or Codex app-server owns:

- base system prompt
- built-in tools and MCP tools
- tool schemas
- sandbox and approval policy
- session continuity

The harness may assemble a turn payload containing OpenClaw prompt files, channel state, matched skills, and the user message. Same-session payload assembly must use the prompt injection ledger:

- first matching fingerprint in a session: include the prompt file or skill body
- same session and same fingerprint: skip repeated body and include a continuity note
- changed fingerprint: include the changed content again and update the ledger

This keeps the turn payload compact and aligns with Codex session continuity instead of repeatedly appending OpenClaw instruction blocks.

## Channel Commands

- /new starts or switches to a fresh session key for the channel and agent.
- /think records reasoning-mode preference or instruction in channel state for future turns.
- /stop records a stop request and reason.
- /steer appends steering notes that affect future skill matching and turn context.
- /btw appends side notes without resetting the session.
- /model records a per-channel or per-session model override.
- /status reports session, queue, runtime, model, and activation state.

Commands should update channel state and receipts before enqueueing agent turns.

## Channel Delivery

- Command replies and agent replies are both appended to state/channels/outbox.jsonl.
- Use channel-run-once as the single-message adapter entrypoint before real Telegram/Discord loops exist.
- Use channel-outbox-plan to list pending delivery work by platform.
- Use channel-delivery-record after Telegram/Discord send attempts to record delivered or failed receipts.
- Use telegram-poll-once for Telegram Bot API smoke tests. It reads TELEGRAM_BOT_TOKEN from the environment, stores offsets in state/channels/telegram-offset.json, runs channel-run-once for text updates, sends pending replies, records delivery receipts, and writes a telegram.poll-once operational log.
- Use telegram-loop for operator-run Telegram handoff. It repeats the same poll-once path with --iterations, --idle-ms, and --max-consecutive-errors. Use finite iterations for tests and --iterations 0 only when the old gateway is not also consuming Telegram updates.
- Use discord-outbox-send-once for Discord outbound smoke. It reads DISCORD_BOT_TOKEN, sends pending platform=discord outbox messages through Discord REST, records delivery receipts, and writes a discord.outbox-send-once operational log. Discord gateway receive is still pending.
- Failed receipts stay retryable; delivered receipts are skipped by future outbox plans.
- Do not send the same already recorded Codex completion twice.

## Activation Checklist

Before replacing the Docker OpenClaw gateway:

1. Run import dry-run and review skipped or sensitive items.
2. Execute import with an explicit conflict policy.
3. Export or confirm the harness registry.
4. Sync builtin harness skills.
5. Run activation readiness checks.
6. Confirm logs are written to state/logs/harness.jsonl.
7. Smoke-test a Telegram command message with telegram-poll-once when TELEGRAM_BOT_TOKEN is configured, or with channel-run-once when testing offline.
8. Confirm enable-check reports telegram-offset, telegram-poll-log, and discord-send-log after channel adapter smoke tests.
9. Confirm memory-qdrant-edge is present when current OpenClaw uses Qdrant edge as primary memory backend.
10. Confirm codex-runtime-launch-probe passes with the intended --codex-exe before any real runtime handoff.
11. Run plugin-sidecar-probe and plugin-sidecar-call for sidecar.status/plugins.list; confirm plugin-sidecar-probe and plugin-sidecar-bridge are pass in enable-check. This proves process startup and JSON-RPC metadata visibility only; hook/tool execution still needs its own bridge.
12. Smoke-test a normal DM turn through channel receive, queue prepare, Codex plan/preflight, launch probe, codex-run, and completion receipt.

## Codex Runtime Flow

For a normal queued channel turn, the current worker-facing path is runtime-run-once:

- It prepares one queue item, plans Codex, runs Codex app-server, records transcript/trajectory/Codex binding outputs, and writes an agent-reply message to state/channels/outbox.jsonl.
- If the Codex completion receipt already exists, it skips the model request/outbox write to avoid duplicate delivery.

For manual debugging of one prepared turn, the expanded path is:

1. channel-receive for an incoming Telegram/Discord-style message.
2. queue-prepare to assemble prompt-bundle.json and prompt.md.
3. codex-plan to write the app-server invocation contract.
4. codex-preflight to check executable, prompt files, output paths, and auth.
5. codex-launch-probe if process startup needs verification without a model request.
6. codex-run to send the prepared OpenClaw payload to Codex app-server, capture assistant deltas, and write transcript/trajectory/Codex binding outputs.

Use --codex-exe for the standalone/local Codex CLI that passed launch probe. Do not rely on the Codex Desktop app resource path for a service runtime unless it has passed codex-launch-probe.

codex-run writes raw app-server stdout/stderr logs under the execution directory and appends operational events to state/logs/harness.jsonl. If a completion receipt already exists, codex-run must skip the model request and return the recorded completion state.

## Skill Maintenance Loop

When a task reveals a repeatable operation:

1. Record the working procedure in a skill or update this skill if it is harness-global.
2. Keep the change narrow and action-oriented.
3. Add verification steps and known failure modes.
4. Avoid storing secrets or raw transcripts in skills.
5. Preserve user-modified skills unless explicitly forced.
"#;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuiltinHarnessSkillSyncOptions {
    pub harness_home: PathBuf,
    pub force: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BuiltinHarnessSkillSyncReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub manifest_file: PathBuf,
    pub summary: BuiltinHarnessSkillSyncSummary,
    pub receipts: Vec<BuiltinHarnessSkillSyncReceipt>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BuiltinHarnessSkillSyncSummary {
    pub written: usize,
    pub already_current: usize,
    pub skipped_user_modified: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BuiltinHarnessSkillSyncReceipt {
    pub skill_id: String,
    pub path: PathBuf,
    pub status: BuiltinHarnessSkillSyncStatus,
    pub reason: String,
    pub version: String,
    pub fingerprint: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum BuiltinHarnessSkillSyncStatus {
    Written,
    AlreadyCurrent,
    SkippedUserModified,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BuiltinHarnessSkillManifest {
    schema: String,
    skills: Vec<BuiltinHarnessSkillManifestEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BuiltinHarnessSkillManifestEntry {
    skill_id: String,
    path: PathBuf,
    version: String,
    fingerprint: String,
}

pub fn sync_builtin_harness_skills(
    options: BuiltinHarnessSkillSyncOptions,
) -> io::Result<BuiltinHarnessSkillSyncReport> {
    let manifest_file = builtin_harness_skill_manifest_file(&options.harness_home);
    let mut manifest = read_manifest(&manifest_file)?;
    let mut receipts = Vec::new();
    let mut summary = BuiltinHarnessSkillSyncSummary::default();

    let receipt = sync_one_builtin_skill(
        &options.harness_home,
        options.force,
        &mut manifest,
        OPENCLAW_WINDOWS_HARNESS_SKILL_ID,
        OPENCLAW_WINDOWS_HARNESS_SKILL_VERSION,
        OPENCLAW_WINDOWS_HARNESS_SKILL,
    )?;
    match receipt.status {
        BuiltinHarnessSkillSyncStatus::Written => summary.written += 1,
        BuiltinHarnessSkillSyncStatus::AlreadyCurrent => summary.already_current += 1,
        BuiltinHarnessSkillSyncStatus::SkippedUserModified => summary.skipped_user_modified += 1,
    }
    receipts.push(receipt);

    write_manifest(&manifest_file, &manifest)?;

    Ok(BuiltinHarnessSkillSyncReport {
        schema: BUILTIN_HARNESS_SKILL_SYNC_SCHEMA,
        harness_home: options.harness_home,
        manifest_file,
        summary,
        receipts,
    })
}

pub fn builtin_harness_skill_manifest_file(harness_home: impl AsRef<Path>) -> PathBuf {
    harness_home
        .as_ref()
        .join("skills")
        .join(".openclaw-harness-builtins.json")
}

fn sync_one_builtin_skill(
    harness_home: &Path,
    force: bool,
    manifest: &mut BuiltinHarnessSkillManifest,
    skill_id: &str,
    version: &str,
    content: &str,
) -> io::Result<BuiltinHarnessSkillSyncReceipt> {
    let path = builtin_skill_file(harness_home, skill_id);
    let target_fingerprint = fingerprint_bytes(content.as_bytes());
    let existing_fingerprint = if path.is_file() {
        Some(fingerprint_bytes(&fs::read(&path)?))
    } else {
        None
    };
    let previous_fingerprint = manifest
        .skills
        .iter()
        .find(|entry| entry.skill_id == skill_id)
        .map(|entry| entry.fingerprint.clone());

    if existing_fingerprint.as_deref() == Some(target_fingerprint.as_str()) {
        upsert_manifest_entry(manifest, skill_id, &path, version, &target_fingerprint);
        return Ok(BuiltinHarnessSkillSyncReceipt {
            skill_id: skill_id.to_string(),
            path,
            status: BuiltinHarnessSkillSyncStatus::AlreadyCurrent,
            reason: "builtin harness skill already matches current version".to_string(),
            version: version.to_string(),
            fingerprint: target_fingerprint,
        });
    }

    let user_modified = existing_fingerprint.is_some()
        && match previous_fingerprint.as_deref() {
            Some(previous) => Some(previous) != existing_fingerprint.as_deref(),
            None => true,
        };
    if user_modified && !force {
        return Ok(BuiltinHarnessSkillSyncReceipt {
            skill_id: skill_id.to_string(),
            path,
            status: BuiltinHarnessSkillSyncStatus::SkippedUserModified,
            reason:
                "existing skill differs from the last synced manifest; use --force to overwrite"
                    .to_string(),
            version: version.to_string(),
            fingerprint: target_fingerprint,
        });
    }

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&path, content)?;
    upsert_manifest_entry(manifest, skill_id, &path, version, &target_fingerprint);
    Ok(BuiltinHarnessSkillSyncReceipt {
        skill_id: skill_id.to_string(),
        path,
        status: BuiltinHarnessSkillSyncStatus::Written,
        reason: "builtin harness skill was written".to_string(),
        version: version.to_string(),
        fingerprint: target_fingerprint,
    })
}

fn builtin_skill_file(harness_home: &Path, skill_id: &str) -> PathBuf {
    harness_home
        .join("skills")
        .join(HARNESS_BUILTIN_SKILL_NAMESPACE)
        .join(skill_id)
        .join(SKILL_FILE_NAME)
}

fn read_manifest(path: &Path) -> io::Result<BuiltinHarnessSkillManifest> {
    if !path.is_file() {
        return Ok(BuiltinHarnessSkillManifest {
            schema: BUILTIN_HARNESS_SKILL_MANIFEST_SCHEMA.to_string(),
            skills: Vec::new(),
        });
    }
    let bytes = fs::read(path)?;
    serde_json::from_slice(&bytes).map_err(io::Error::other)
}

fn write_manifest(path: &Path, manifest: &BuiltinHarnessSkillManifest) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let text = serde_json::to_string_pretty(manifest).map_err(io::Error::other)?;
    fs::write(path, text)
}

fn upsert_manifest_entry(
    manifest: &mut BuiltinHarnessSkillManifest,
    skill_id: &str,
    path: &Path,
    version: &str,
    fingerprint: &str,
) {
    if let Some(entry) = manifest
        .skills
        .iter_mut()
        .find(|entry| entry.skill_id == skill_id)
    {
        entry.path = path.to_path_buf();
        entry.version = version.to_string();
        entry.fingerprint = fingerprint.to_string();
        return;
    }
    manifest.skills.push(BuiltinHarnessSkillManifestEntry {
        skill_id: skill_id.to_string(),
        path: path.to_path_buf(),
        version: version.to_string(),
        fingerprint: fingerprint.to_string(),
    });
}

fn fingerprint_bytes(bytes: &[u8]) -> String {
    format!("fnv1a64:{:016x}:{}", fnv1a64(bytes), bytes.len())
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn sync_builtin_harness_skills_writes_skill_and_manifest() {
        let root = temp_root("sync_builtin_harness_skills_writes_skill_and_manifest");
        let harness_home = root.join("harness-home");

        let report = sync_builtin_harness_skills(BuiltinHarnessSkillSyncOptions {
            harness_home: harness_home.clone(),
            force: false,
        })
        .unwrap();

        assert_eq!(report.summary.written, 1);
        assert!(report.manifest_file.is_file());
        assert!(
            harness_home
                .join("skills")
                .join(HARNESS_BUILTIN_SKILL_NAMESPACE)
                .join(OPENCLAW_WINDOWS_HARNESS_SKILL_ID)
                .join(SKILL_FILE_NAME)
                .is_file()
        );

        let second = sync_builtin_harness_skills(BuiltinHarnessSkillSyncOptions {
            harness_home,
            force: false,
        })
        .unwrap();
        assert_eq!(second.summary.already_current, 1);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn sync_builtin_harness_skills_preserves_user_modified_skill() {
        let root = temp_root("sync_builtin_harness_skills_preserves_user_modified_skill");
        let harness_home = root.join("harness-home");
        sync_builtin_harness_skills(BuiltinHarnessSkillSyncOptions {
            harness_home: harness_home.clone(),
            force: false,
        })
        .unwrap();
        let skill_file = builtin_skill_file(&harness_home, OPENCLAW_WINDOWS_HARNESS_SKILL_ID);
        fs::write(&skill_file, "# User Modified\n").unwrap();

        let skipped = sync_builtin_harness_skills(BuiltinHarnessSkillSyncOptions {
            harness_home: harness_home.clone(),
            force: false,
        })
        .unwrap();
        assert_eq!(skipped.summary.skipped_user_modified, 1);
        assert_eq!(
            fs::read_to_string(&skill_file).unwrap(),
            "# User Modified\n"
        );

        let forced = sync_builtin_harness_skills(BuiltinHarnessSkillSyncOptions {
            harness_home,
            force: true,
        })
        .unwrap();
        assert_eq!(forced.summary.written, 1);
        assert!(
            fs::read_to_string(skill_file)
                .unwrap()
                .contains("OpenClaw Windows Harness")
        );

        let _ = fs::remove_dir_all(root);
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "openclaw-harness-builtin-skills-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
