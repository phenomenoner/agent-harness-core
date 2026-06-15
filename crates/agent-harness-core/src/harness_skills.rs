use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::{HARNESS_BUILTIN_SKILL_NAMESPACE, SKILL_FILE_NAME};

const BUILTIN_HARNESS_SKILL_SYNC_SCHEMA: &str = "agent-harness.builtin-skill-sync.v1";
const BUILTIN_HARNESS_SKILL_MANIFEST_SCHEMA: &str = "agent-harness.builtin-skill-manifest.v1";
const AGENT_WINDOWS_HARNESS_SKILL_ID: &str = "agent-windows-harness";
const AGENT_WINDOWS_HARNESS_SKILL_VERSION: &str = "0.1.11";

const AGENT_WINDOWS_HARNESS_SKILL: &str = r#"---
name: agent-windows-harness
description: Operate the Rust Windows Agent Harness, channel commands, activation handoff, provider isolation, response tone, and Codex prompt continuity policy.
version: 0.1.11
platforms: [windows]
metadata:
  agent_harness:
    category: operations
    tags: [legacy-import, codex, openrouter, telegram, discord, migration, activation, response-tone, reconnect, channel-identity, cron-scheduler, cron-runs, runtime-classes, scheduler-lint, safe-mode]
---

# Agent Windows Harness

## When to Use

Use this skill at the start of turns that operate, debug, activate, migrate, or extend the Rust Windows 11 Agent Harness.

Use it when the user mentions:

- importing legacy source state, memory, cron, plugins, sessions, agents, subagents, or workspace files
- Telegram or Discord DM operation
- channel identity binding, delivery intent, or multi-account adapter operation
- slash commands such as /new, /think, /stop, /steer, /btw, /model, or /status
- Codex CLI, Codex OAuth, app-server, OpenRouter provider routing, prompt injection, tool schema, or session continuity
- response tone, emoji accent, assistant narration, progress panel, or final reply formatting
- activation readiness, runtime queue, runtime class leases, operational logs, or gateway handoff
- cron scheduler ticks, worker enqueue watermarks, CronRunStore recovery, or cron-scheduler-loop supervision

## Operating Lead

1. Treat this skill as the versioned harness runbook. Check it before relying on older docs or session memory.
2. Treat the harness as the orchestrator and Codex CLI as the model/tool runtime.
3. Preserve legacy source state shape where possible: source workspace, prompt files, agent registry, sessions, memory files, cron state, plugin state, and receipts.
4. Prefer dry-run, receipt, and append-only JSONL records before irreversible handoff.
5. Keep deterministic cron off the LLM path. Agent-turn cron may enqueue isolated runtime work only through CronRunStore admission, the `cron` worker lane, and the `cron` runtime class.
6. Keep Telegram and Discord session keys stable: platform, channel id, user id, and agent id determine continuity unless /new changes it.
7. Keep multi-agent readiness intact. Do not collapse imported agents into a single default agent.
8. Treat credentials as best-effort imports. Codex OAuth is preferred for default Codex/OpenAI models; API keys may be provider-specific and model-limited. Do not apply OpenRouter credentials or provider config to the default Codex/OAuth route.
9. Treat memory/qdrant-edge as the primary memory backend when present. LanceDB is backup/optional unless the active legacy source config points to it.
10. Use a Codex CLI binary that the harness can spawn. On Windows, the Codex Desktop MSIX resource path may be visible on PATH but fail with os error 5; prefer a standalone release or local npm install and pass it with --codex-exe.
11. Use tools/agent-fake-codex-app-server for offline runtime smoke when the goal is to verify harness receipts and logs without a model request.
12. Use runtime-loop for operator-run queue draining or service-wrapper handoff; use --stop-when-idle for smoke and --iterations 0 only under an intentional supervisor.
13. Use supervisor-plan to generate Windows Task Scheduler install/start/stop/uninstall scripts. It writes scripts and receipts only; it does not register tasks automatically.
14. Use status --json for operator health checks; it should show runtime openItems=0 and outbox pending=0 before live handoff.
15. Treat harness-config.json security.codexApprovalPolicy and security.codexSandbox as operator-controlled runtime safety settings. Use codexApprovalPolicy="accept" only for an intentionally unattended trusted channel runtime.
16. Keep response tone policy scoped to successful final agent replies. The default is off; subtle emoji accenting is opt-in. Do not post-process command replies, /status, error/failure replies, progress/status panels, code-heavy replies, or risk/security/status replies.
17. For every new functional component or behavior change, add or update a `/docs/` note that records the design rationale and a concise changelog. Do not leave feature history discoverable only through git commits or chat context.
18. Treat channel identity registry misses, disabled bindings, ambiguous bindings, and agent mismatches as fail-closed ingress stops. Allow-lists are necessary but not sufficient when a channel identity registry is present.
19. Treat cron-scheduler-loop as a scheduler/enqueue loop only. Worker-loop owns job execution, retry, leases, and recovery.
20. Treat cron runtime execution as a separate lane from interactive/user turns. Native LLM cron defaults to one-shot `sessionPolicy`, writes transcripts under `agents/<agent>/cron-sessions/`, and dispatches runtime queue items with `runtimeClass=cron` and `origin=cron-scheduler`.
21. Treat live gateway control as a protected control plane. A live channel agent turn must not stop, start, restart, uninstall, kill, mutate supervisor stop files, or replace the gateway binary that carries the current session unless it is following an operator-approved live-control token/cutover flow.
22. If the current task requires changing Agent Harness itself, live gateway behavior, supervisor scripts, channel adapters, runtime loops, or the binary/config carrying this live session, write a concise tech note with the observed bug, user scenario, evidence, suggested fix, validation plan, and risks; tell the user the tech note path; and pause the original task until the user continues from a local/dev session or operator-approved patch flow.

## Prompt And Tool Schema Policy

The harness does not own the Codex system prompt or Codex tool schema. Codex CLI or Codex app-server owns:

- base system prompt
- built-in tools and MCP tools
- tool schemas
- sandbox and approval policy
- session continuity

The default harness-local Codex config is generated under codex-home/config.toml only when harness-local Codex OAuth auth is present. Explicit OpenRouter routes use a provider-specific Codex home under codex-home-providers/openrouter with model_provider="openrouter", base_url="https://openrouter.ai/api/v1", env_key="OPENROUTER_API_KEY", and wire_api="responses". Never write OpenRouter provider config into the shared codex-home; that home is reserved for the default OpenAI/Codex OAuth route. Generated config uses security.codexSandbox from harness-config.json or AGENT_HARNESS_CODEX_SANDBOX, defaulting to Windows sandbox "elevated". Approval requests are controlled separately by security.codexApprovalPolicy or AGENT_HARNESS_CODEX_APPROVAL_POLICY.

The harness may assemble a turn payload containing legacy prompt files, channel state, matched skills, and the user message. Same-session payload assembly must use the prompt injection ledger:

- first matching fingerprint in a session: include the prompt file or skill body
- same session and same fingerprint: skip repeated body and include a continuity note
- changed fingerprint: include the changed content again and update the ledger

This keeps the turn payload compact and aligns with Codex session continuity instead of repeatedly appending legacy instruction blocks.

## Skill Import Layout

- Treat `skills\legacy-imports` and `skills\openclaw-imports` as valid imported skill namespaces.
- Current OpenClaw workspace skills are expected under `skills\openclaw-imports\workspace`; legacy backup imports may still use `skills\legacy-imports`.
- When a live prompt bundle is missing an imported guardrail or specialist skill, first compare the source tree with `agent-harness skills --harness-home <harness> --limit <n>`.
- If an isolated build sees imported skills but `target\debug\agent-harness.exe` does not, create an `ops-cutover-request`, obtain an operator-issued live-control token, perform the controlled live cutover through the supervisor path, then re-run canonical skill-index and local smoke tests.

## JSONL Ledger Hygiene

- Use the shared harness JSONL append path for new receipt/log writers; do not add ad hoc `OpenOptions::append(true)` plus `writeln!` JSONL writers.
- `jsonl-repair --path <ledger>` is the dry-run validation path. It writes repaired and invalid sidecars and reports `valid`, `output`, `recoveredLines`, `recoveredValues`, and `invalid`.
- For live ledger repair, use the cutover token flow before stopping the gateway. Build the canonical binary from staging, validate the token/ticket with `ops-cutover-apply`, run `jsonl-repair --path <ledger> --apply` during the operator-controlled window, then restart the gateway through the supervisor path. `--apply` writes a `.bak-<timestamp>.jsonl` backup before replacing the ledger.
- Treat `recoveredLines>0` with `invalid=0` as successful recovery of concatenated JSON values such as `}{`; treat `invalid>0` as quarantine that needs manual inspection before claiming the ledger is clean.
- After repair, run `healthz --require-writable-state` and `status --json`; affected ledgers should report `invalidLines=0`.

## Runtime Reconnect And Session Recovery

- Telegram and Discord share the same runtime path after ingress: channel receive, queue, prepare, codex-run, runtime-run-once, outbox. Fix reconnect/session recovery in the runtime/Codex layer, not in one adapter.
- Known transient Codex app-server stream disconnect protocol errors are retryable: `Reconnecting...`, `stream disconnected before completion`, and `websocket closed by server before response.completed`.
- Retry-pending runtime failures are non-terminal. They should not write user-visible error replies, and progress should stay resumable for the same queue/session context.
- Retry-pending receipts must make the same queue id immediately claimable again. If a retry-pending Telegram or Discord turn shows `openItems>0` while `runtime-loop` reports `no-work`, inspect the class-scoped lease file under `state/runtime-queue/classes/<runtimeClass>/runtime-leases.json`; legacy root leases may still appear at `state/runtime-queue/runtime-leases.json` during migration. Stale retry-pending leases are a runtime lease-cleanup bug, not a channel adapter issue.
- `lease-busy` is a retryable non-idle runtime status. Do not report it as idle/no-work; inspect competing runtime-loop processes, the relevant runtime class lease file, or a recently active queue lease.
- In supervised infinite mode, keep runtime-loop safe-mode restart enabled. After repeated errors, `safe-mode` keeps the process alive with reduced concurrency and writes heartbeat/log evidence; missing/stale/error/stopped/stopping runtime-loop heartbeats are live readiness failures.
- Non-matching protocol/config/preflight/spawn failures stay failed-terminal. Gateway restart alone does not resume failed-terminal or dead-letter queue items.
- `runtimeBackoff` config controls retry caps and delay hints for retryable runtime failures. When retry attempts are exhausted, runtime-run-once dead-letters the item and writes an operator-friendly error reply. Provider/model fallback is operator-guided; the harness should not silently switch providers on behalf of a user turn.
- Use `queue-retry` for manual recovery of a timeout/dead-letter item. It creates a fresh queue id while preserving `sessionKey`, agent, platform/channel/user, provider/model, selected skills, and planned transcript/trajectory paths.
- To verify a reconnect fix, test both early retry-pending and final dead-letter behavior, and confirm unrelated protocol errors remain terminal.

## Response Tone Policy

- response.emojiAccentMode defaults to off. Use response.emojiAccentMode="subtle" only as an explicit opt-in. It appends one guarded accent only when runtime-run-once writes a successful agent-reply to state/channels/outbox.jsonl.
- Use response.emojiAccentAgentModes for agent-specific overrides and response.emojiAccentChannelModes for channel-specific overrides.
- Channel selectors can be platform:channelId:userId, platform:channelId, channelId, or platform. Channel overrides win over agent overrides; agent overrides win over the global mode.
- The policy must skip command replies, /status, error/failure replies, progress/status panels, fenced code blocks, code-heavy replies, risk/security/status-style replies, and text already ending with an emoji.
- Do not wrap final Telegram/Discord replies with a mechanical `◆ Agent` header. Send trimmed assistant text as-is. Reply/reference targeting is represented by harness-validated outbound `deliveryIntent`, never by model-authored quote text.
- Current-step narration in progress delivery uses assistant narration only, with a separate longer cap (`--current-step-max-chars`, default 1200). If assistant narration is absent, omit the `Current step:` line; keep runtime/tool operation names in action/status lanes and never relabel them as Codex execution summaries.
- Do not implement tone as blind delivery-layer post-processing. Keep it at the successful agent-reply outbox boundary so audit artifacts and non-agent replies stay unpolluted.

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
- Use channel-run-once for offline smoke and adapter-level single-message tests.
- Use channel-outbox-plan to list pending delivery work by platform.
- Use channel-delivery-record after Telegram/Discord send attempts to record delivered or failed receipts.
- Use channel-credentials-export --include-sensitive during legacy cutover to import Telegram/Discord bot tokens and known channel/user/guild IDs into secrets/channel-credentials.env with redacted receipts. Telegram poll and Discord event adapters enforce those imported allow-lists, then resolve channel identity bindings before channel-run-once.
- Use channel-identity-check before live handoff for every enabled platform/account/channel tuple. A bound result identifies the owning agent; missing/disabled/conflicting bindings are fail-closed and must be fixed in config rather than bypassed at runtime.
- Use telegram-probe before live Telegram handoff to validate Bot API getMe without consuming updates or sending messages. It writes state/channels/telegram-probe.json, appends telegram-probe-receipts.jsonl, and logs telegram.probe.
- Use telegram-poll-once for Telegram Bot API smoke tests. It reads TELEGRAM_BOT_TOKEN or AGENT_HARNESS_TELEGRAM_ACCOUNT_<ID>_BOT_TOKEN plus imported Telegram chat/user allow-lists from the environment or secrets/channel-credentials.env, stores account-specific offset state after every successful poll, denies non-allowed/unbound updates before runtime dispatch, runs channel-run-once for allowed text updates, sends matching-account pending replies, records delivery receipts, and writes a telegram.poll-once operational log.
- Use telegram-loop for operator-run Telegram handoff. It repeats the same poll-once path with --telegram-account, --iterations, --idle-ms, --max-consecutive-errors, and optional --stop-file. Use finite iterations for tests and --iterations 0 only when the old gateway is not also consuming Telegram updates.
- Use discord-outbox-send-once for Discord outbound smoke. It reads DISCORD_BOT_TOKEN or AGENT_HARNESS_DISCORD_ACCOUNT_<ID>_BOT_TOKEN from the environment or secrets/channel-credentials.env, sends pending platform=discord outbox messages for the selected account through Discord REST, records delivery receipts, and writes a discord.outbox-send-once operational log.
- Use discord-event-run-once for Discord inbound normalization smoke. It accepts a Discord Gateway MESSAGE_CREATE event from --event-file or --event-json, skips bot/empty/duplicate messages, enforces imported Discord user/channel/guild allow-lists, resolves channel identity, calls channel-run-once for allowed text, writes discord-event receipts, and logs discord.event-run-once. Use discord-gateway-probe before discord-gateway-loop for live WebSocket handoff. The gateway loop accepts --discord-account and --stop-file, passes the account selector to event-run-once, and closes the WebSocket when the stop file appears. INTERACTION_CREATE application commands are acknowledged by the Node gateway, translated to slash-command text, and routed through the same Discord event pipeline; real DM text readiness still requires a MESSAGE_CREATE receipt.
- Failed receipts stay retryable; delivered receipts are skipped by future outbox plans.
- Do not send the same already recorded Codex completion twice.

## Cron Scheduler

- `cron-scheduler-run-once` evaluates imported native agent-turn cron and deterministic crontab/Supercronic-style cron, then enqueues due work into WorkerStore with durable watermarks under `state/cron-scheduler/watermarks.sqlite`.
- Native LLM cron also admits a CronRun record under `state/cron-runs/cron-runs.sqlite` before worker enqueue. This is the control plane for active caps, retry, quarantine, and operator listing.
- Native LLM cron worker jobs run on the `cron` worker lane. Their runtime queue payloads carry `runtimeClass=cron`, `origin=cron-scheduler`, `cronRunId`, `scheduledForMs`, and `sessionPolicy`; the worker must preserve that metadata when appending the runtime queue item.
- One-shot cron sessions are the default. They use deterministic per-run session keys and transcript files under `agents/<agent>/cron-sessions/`, so cron context does not pollute the main interactive session or another cron run. Sticky cron sessions are also forced into the `cron:<agent>:<entry>:sticky:<suffix>` namespace.
- Runtime dispatch capacity is class scoped. Cron class leases live separately from interactive class leases; configure `runtimeDispatch.classes.cron` caps and per-agent/per-job caps so a large cron burst cannot starve normal agent turns. Worker and runtime dispatch paths both re-check CronRunStore controls, tombstone skipped cron runtime items, and avoid overwriting operator skip/quarantine state.
- Use `cron-runs` for operator readback and `cron-run-control --action skip|retry|quarantine|unquarantine` for manual recovery. Retry clears the failed run back to retry-pending so the next scheduler tick can enqueue a new worker job without treating the old watermark as a permanent duplicate.
- Run `cron-scheduler-lint` before enabling or changing live scheduler ticks. Lint is read-only and should fail cutover when status is `error`.
- Use `--dry-run` for readback and `--enable` plus explicit `--resume-cron` or `--allow-deterministic-run` gates for intentional enqueue. A scheduler tick should write job-decision receipts even when entries are skipped by policy.
- `cron-scheduler-loop` repeats the tick with heartbeat, stop-file, consecutive-error, and `loop-last.json` status support. It sleeps at least `cronScheduler.intervalMs` even if `--idle-ms` is smaller. Generate it through `supervisor-plan --include-cron-scheduler` only after operator approval to activate live scheduling.
- On Windows, Linux absolute paths such as `/root/...` in native cron message text are lint/runtime errors because they indicate an imported job would not run in the active environment.

## Activation Checklist

Before replacing the Docker legacy gateway:

1. Run import dry-run and review skipped or sensitive items.
2. Execute import with an explicit conflict policy.
3. Export or confirm the harness registry.
4. Sync builtin harness skills.
5. Run activation readiness checks.
6. Run healthz --require-writable-state and status --json; confirm runtime openItems=0, channel outbox pending=0, and log evidence is present.
7. If live scheduler ticks are enabled, run cron-scheduler-lint and cron-scheduler-run-once --dry-run --enable before supervisor cutover.
8. Confirm logs are written to state/logs/harness.jsonl.
9. Confirm enable-check reports telegram-access-policy and discord-access-policy as pass when importing existing legacy channel IDs.
10. Run telegram-probe when TELEGRAM_BOT_TOKEN is configured to prove Telegram token/API reachability without consuming updates.
11. Smoke-test a Telegram command message with telegram-poll-once when the old gateway is offline, or with channel-run-once when testing offline.
12. Confirm enable-check reports telegram-probe before live handoff, then telegram-offset, telegram-poll-log, and discord-send-log after channel adapter smoke tests.
13. Confirm memory-qdrant-edge is present when the current legacy source uses Qdrant edge as primary memory backend.
14. Run memory-search --harness-home <harness> --query <known term> to prove imported markdown/text memory files are readable. This is a read-only recall probe and does not replace the Qdrant edge vector adapter.
15. Confirm /status security, enable-check codex-approval-policy, and enable-check codex-sandbox show the intended unattended safety posture.
16. Confirm codex-runtime-launch-probe passes with the intended --codex-exe before any real runtime handoff. For OpenRouter smoke, confirm the plan uses codex-home-providers/openrouter; for default Codex/OAuth smoke, confirm shared codex-home/config.toml has no OpenRouter model_provider override.
17. Run plugin-sidecar-probe and plugin-sidecar-call for sidecar.status/plugins.list/tools.probe; set AGENT_HARNESS_PLUGIN_SOURCE_ROOTS when imported manifests live outside the harness home. Confirm plugin-sidecar, plugin-sidecar-probe, and plugin-sidecar-bridge are pass in enable-check. This proves manifest catalog and JSON-RPC bridge readiness; plugin-specific tool executors still need dedicated adapters.
18. Smoke-test a normal DM turn through channel receive, queue prepare, Codex plan/preflight, launch probe, codex-run, and completion receipt. Use tools/agent-fake-codex-app-server/fake-codex-app-server.cmd for offline smoke; use the intended Codex CLI only for operator-run model smoke.
19. Run runtime-loop --stop-when-idle for idle/drain smoke and confirm state/runtime-queue/loop-last.json plus runtime.loop-stopped log evidence.
20. Run supervisor-plan with the intended harness CLI, Codex executable, channel loop selection, and task prefix. Confirm state/supervisor/windows-scheduled-tasks/supervisor-plan.json, absolute paths in generated scripts, no raw token/key/secret strings in scripts, and enable-check supervisor-plan pass.

## Codex Runtime Flow

For a normal queued channel turn, the current worker-facing path is runtime-run-once:

- It prepares one queue item, plans Codex, runs Codex app-server, records transcript/trajectory/Codex binding outputs, and writes an agent-reply message to state/channels/outbox.jsonl only after a successful app-server turn.
- If the Codex completion receipt already exists, it skips the model request/outbox write to avoid duplicate delivery.
- App-server method=error events and failed turn/completed statuses are terminal protocol failures; they must not be converted into placeholder assistant replies.

For operator-run drain or service-wrapper handoff, use runtime-loop:

- It repeats runtime-run-once with finite or infinite --iterations.
- It treats no-work/no-prepared-execution and already recorded completions as idle, supports --stop-when-idle and --stop-file for smoke/supervisor stop, and exits nonzero after --max-consecutive-errors for runtime/preflight/protocol failures.
- It writes state/runtime-queue/loop-last.json and appends runtime.loop-stopped plus runtime.loop-error to state/logs/harness.jsonl.

For Windows supervisor handoff, use supervisor-plan:

- It writes runner scripts for runtime-loop, telegram-loop, and discord-gateway-loop under state/supervisor/windows-scheduled-tasks/scripts.
- It writes install-scheduled-tasks.ps1, start-scheduled-tasks.ps1, stop-scheduled-tasks.ps1, uninstall-scheduled-tasks.ps1, and supervisor-plan.json.
- It uses absolute paths because Task Scheduler does not run from the repo directory by default.
- It uses stop files for graceful loop shutdown and never embeds raw bot tokens or API keys.
- The operator must explicitly run the generated installer/start scripts after the old gateway is offline.

For manual debugging of one prepared turn, the expanded path is:

1. channel-receive for an incoming Telegram/Discord-style message.
2. queue-prepare to assemble prompt-bundle.json and prompt.md.
3. codex-plan to write the app-server invocation contract.
4. codex-preflight to check executable, prompt files, output paths, and auth.
5. codex-launch-probe if process startup needs verification without a model request.
6. codex-run to send the prepared agent payload to Codex app-server, capture assistant deltas, and write transcript/trajectory/Codex binding outputs.

Use --codex-exe for the standalone/local Codex CLI that passed launch probe. Do not rely on the Codex Desktop app resource path for a service runtime unless it has passed codex-launch-probe.

codex-run writes raw app-server stdout/stderr logs under the execution directory and appends operational events to state/logs/harness.jsonl. If a completion receipt already exists, codex-run must skip the model request and return the recorded completion state.

For offline activation smoke, --codex-exe may point at tools/agent-fake-codex-app-server/fake-codex-app-server.cmd. That fixture only proves harness wiring, receipts, transcript/trajectory output, outbox creation, and logs; it is not a model or plugin execution test.

## Health Status

Use status for operator-facing health checks before and after handoff:

- status summarizes readiness, runtime queued/open/prepared/completed items, outbox pending/delivered/retryable counts, Telegram/Discord smoke evidence, memory backend presence, plugin sidecar receipts, and operational log coverage.
- status reports runtime class queued/open counts, class lease counts, CronRun summary counts, and recent cron scheduler decisions. Check the interactive class separately from the cron class when diagnosing a stalled user turn.
- status includes memory-search receipts when the imported markdown/text memory probe has been run.
- status --json is the monitor-friendly form for scheduled tasks or service wrappers.
- healthz --require-writable-state is the live/readiness gate for loops, writable state, runtime backlog, and channel backlog.
- Large ledgers are tail-sampled by status/health/readiness. When sampled is true, counts are an operational window rather than full historical totals.
- runtime-loop writes loop-last.json for the most recent runtime-loop stop/degraded reason, iteration count, idle count, error count, and safe-mode restarts.
- supervisor-plan readiness is checked through enable-check, not status-specific process liveness; installed task health still needs monitor integration.
- Before live channel handoff, interactive openItems should be 0 and outbox pending should be 0 unless the operator intentionally wants the adapter to deliver those pending messages. Cron openItems or active CronRuns should be reviewed separately and either drained, skipped, retried, or quarantined before cutover.

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
        AGENT_WINDOWS_HARNESS_SKILL_ID,
        AGENT_WINDOWS_HARNESS_SKILL_VERSION,
        AGENT_WINDOWS_HARNESS_SKILL,
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
        .join(".agent-harness-builtins.json")
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
                .join(AGENT_WINDOWS_HARNESS_SKILL_ID)
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
        let skill_file = builtin_skill_file(&harness_home, AGENT_WINDOWS_HARNESS_SKILL_ID);
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
                .contains("Agent Windows Harness")
        );

        let _ = fs::remove_dir_all(root);
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-builtin-skills-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
