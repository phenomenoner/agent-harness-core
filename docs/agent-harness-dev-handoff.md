# Agent Harness Development Handoff

Date: 2026-06-10

This handoff is for a developer or a new Codex session continuing implementation of the Rust Windows Agent Harness. It summarizes the project context, current architecture, verified state, important files, and next development priorities.

## Context Pointers

Start with these files before making changes:

- `docs/activation-readiness-plan.md`: activation checklist, verified smoke results, current warnings.
- `docs/openclaw-feature-parity.md`: feature parity matrix against the imported OpenClaw deployment.
- `docs/openclaw-feature-parity.html`: local browser version of the same feature parity summary.
- `AGENTS.md`: workspace instruction override. Important: do not call the openclaw-mem gateway from this repo.
- `imports/activation-harness/state/harness-registry.json`: imported harness registry used for live activation.
- `imports/activation-harness/harness-config.json`: harness runtime/security config.
- `imports/activation-harness/secrets/channel-credentials.env`: imported Telegram/Discord channel secrets. Do not print or commit values.
- `imports/activation-harness/secrets/memory-credentials.env`: imported memory embedding secret. Do not print or commit values.

Primary source snapshot:

- OpenClaw source snapshot: `imports/openclaw-core-snapshot`
- Activation harness state root: `imports/activation-harness`
- Runtime workspace for live loops: `D:\Warehouse\Research\OpenClaw_WSL`
- Harness CLI: `target/debug/openclaw-harness.exe`
- Codex CLI used by loops: `.tools/codex-cli/node_modules/.bin/codex.cmd`

## Current Baseline

Latest local status after the 2026-06-09 progress streaming implementation:

- Readiness: `ready=true`, `passed=53`, `warnings=1`, `failed=0`.
- Runtime: `queued=53`, `open=0`, `prepared=53`, `completed=53`.
- Outbox: `pending=0`, `delivered=95`, `retryable=0`, `invalid=0`.
- Channels: Telegram and Discord are enabled; Telegram probe ready; Discord gateway probe ready; Discord real inbound evidence is present; Discord reply-context receipt file is now tracked by status and will appear after the next handled Discord reply event.
- Loops: runtime, progress delivery, Telegram, Discord outbox, and Discord gateway loops are the expected live set. Expected process shape is 5 `openclaw-harness.exe` loops plus 1 Discord Gateway `node.exe` child.
- Memory: Qdrant edge snapshot present, `openclaw-mem.sqlite` present, active recall via `sqlite-vector`, `qdrantParity=snapshot-preserved; native-recall-not-active`, embedding secret migrated, vector recall ready, prompt context ready, lifecycle receipts present, capture candidates=3, compact canvas written.
- Plugins: sidecar catalog present, 2 manifest-derived tools visible, sidecar bridge OK.

Current non-blocking warning:

- `memory-lancedb`: LanceDB backup is absent; acceptable while SQLite vector recall or another active recall lane is healthy.

## Architecture Map

Workspace crates:

- `crates/openclaw-harness-core`: core library for import, registry, channel routing, command state, prompt assembly, runtime planning, memory, plugins, cron/subagent planning, status/readiness.
- `crates/openclaw-harness-cli`: command-line surface for all runtime operations and smoke checks.

Important core modules:

- `registry.rs`: parses imported OpenClaw registry, providers, agents, plugins.
- `harness_registry.rs`: exports target harness registry and redacted receipts.
- `channel_commands.rs`: parses slash commands.
- `channel_state.rs`: persists per-session command state and per-agent global overrides.
- `channel_runtime.rs`: turns channel input into command replies or runtime dispatch.
- `channel_ingress.rs`, `channel_pipeline.rs`: channel receive/run-once orchestration.
- `channel_delivery.rs`: outbox planning and delivery receipts.
- `turns.rs`: turn planning, model policy, thinking policy, agent routing, skill selection.
- `prompt.rs`: prompt bundle assembly, memory context injection, prompt injection ledger.
- `codex_runtime.rs`: Codex app-server planning/preflight/run/completion.
- `progress.rs`: compact runtime action/status event schema, panel rendering, delivery cursor state, and progress delivery receipts.
- `runtime_queue.rs`, `runtime_worker.rs`, `runtime_pipeline.rs`: queue, preparation, runtime loop.
- `memory.rs`: imported memory search, vector recall, credentials export, lifecycle, canvas worker.
- `status.rs`: `status` report aggregation.
- `activation.rs`: `enable-check` readiness checks.
- `supervisor.rs`: Windows scheduled task script generation.
- `cron.rs`, `deterministic_cron.rs`, `subagents.rs`: import/dry-run planning lanes.

Auxiliary tooling:

- `tools/openclaw-discord-gateway/index.mjs`: Node Discord Gateway wrapper.
- `tools/openclaw-fake-codex-app-server`: offline fake Codex app-server for tests/smoke.

## Runtime Flow

Channel normal message flow:

1. Telegram/Discord adapter receives a DM or configured group/guild message.
2. The adapter enforces admin/limited/open-limited access policy before runtime dispatch.
3. `channel-receive` normalizes it and carries bounded inbound reply/media context when available.
4. Slash commands are handled immediately; ordinary messages enqueue a runtime item.
5. `runtime-loop` calls `runtime-run-once`.
6. `queue-prepare` builds a turn plan and prompt bundle.
7. `codex-plan` and `codex-run` start Codex app-server.
8. Runtime/Codex writes compact action/status events to `state/runtime-queue/progress-events.jsonl`.
9. `progress-delivery-loop` sends or edits separate compact Telegram/Discord action and status messages for authorized targets.
10. `codex-complete` records transcript, trajectory, Codex binding, memory lifecycle evidence, and outbox reply.
11. Telegram/Discord outbox delivery loops send and record delivery receipts.
   Discord delivery splits messages over Discord's 2000-character content limit into multiple sends before recording the original delivery id as delivered.

Prompt strategy:

- The harness does not keep appending stable system/context sections every turn.
- It uses prompt bundle assembly plus prompt-injection ledger.
- Stable prompt files and selected skills can be reused by reference.
- Imported memory context is inserted before the user message as a bounded `MemoryContext` section.
- Telegram reply/media metadata and Discord reply/attachment metadata are inserted before the user message as a bounded untrusted `InboundContext` section.
- Reply targets include preview, source, length, truncation metadata, and up to 4000 characters of referenced text when the platform payload exposes text.
- Raw Telegram file IDs and Discord attachment URLs are deliberately not injected into the prompt bundle.
- `queue-prepare` resolves prompt files, skills, and registry state from the imported OpenClaw source home `workspace` when present; a separate runtime workspace is only used as Codex cwd and must not make prompt files disappear after `/new` or other session changes.
- Codex's app-server/session continuity is relied on for backend continuity where possible.

Reply-context audit:

- Discord handled reply events append `state/channels/discord-reply-context-receipts.jsonl` with referenced ids, source availability, preview/content length, truncation, attachment count, and embed count.
- Denied or duplicate Discord events do not write a reply-context receipt with referenced content.
- Runtime queue items still carry the model-facing `inboundContext` string; a structured `replyContext` queue field remains a follow-up schema slice.

Command state:

- `/model` and `/think` always report current session state first.
- `/model <provider>/<model>` switches the current session.
- `/model <provider>/<model> --global` changes the default for the current agent only.
- `/think <level>` switches the current session thinking level.
- `/think <level> --global` changes the default for the current agent only.
- `/think` levels include `minimal`, `low`, `medium`, `high`, and model-aware `xhigh`; aliases include `x-high`, `extra-high`, `very-high`, `max`, and Chinese `超高`/`最高`. Slash commands tolerate whitespace after `/`, so `/ think 超高` is parsed the same as `/think xhigh`.
- `/new` rotates the session key.
- `/steer` and `/btw` append session notes.
- `/stop` is command-state aware but does not yet provide robust hard cancellation for already-running Codex processes.

Channel access policy:

- Telegram/Discord DMs fail closed and require an admin user id.
- Legacy `OPENCLAW_TELEGRAM_ALLOWED_USER_IDS` and `OPENCLAW_DISCORD_ALLOWED_USER_IDS` remain admin-compatible for migration.
- Configured Telegram groups and Discord guild channels can grant limited users ordinary-message access plus read-only `/status`, `/model`, and `/think` queries.
- Limited users cannot switch model/thinking state and cannot run `/new`, `/stop`, `/steer`, or `/btw`.
- Explicit open-limited group/guild mode is supported through `OPENCLAW_TELEGRAM_GROUP_OPEN` and `OPENCLAW_DISCORD_GROUP_OPEN`.

## Memory Layer

Implemented:

- Imported memory inventory and status/readiness checks.
- Read-only text memory search over imported markdown/text/JSONL files.
- Embedding credential migration from imported config into `secrets/memory-credentials.env`.
- SQLite vector recall using imported `openclaw-mem.sqlite` embedding tables:
  - observations
  - docs chunks
  - episodic events
- Query embedding through `text-embedding-3-small`.
- Prompt context uses vector recall first, then text fallback.
- Post-turn lifecycle adapter records episodes and conservative auto-capture candidates.
- Compact symbolic canvas worker writes:
  - `state/memory/canvas/symbolic-canvas.json`
  - `state/memory/canvas/symbolic-canvas.md`
  - `state/memory/canvas-receipts.jsonl`
- `status --json` includes `memory.summary.activeRecallBackend`, `memory.summary.qdrantParity`, and `memory.summary.captureCandidateCount`.

Important limitation:

- Qdrant edge is preserved, detected, and surfaced as the primary imported snapshot, but the active recall lane currently uses SQLite vector recall. The Rust harness does not raw-read Qdrant segment files. Qdrant-native recall should be added through a sidecar/service or supported snapshot/API path.

Do not call the local openclaw-mem gateway while working in this repo. The repo-level AGENTS override disables Codex-side gateway lookups. Product support for memory adapters is still a requirement, but development in this repo should not query gateway endpoints.

## Plugin Layer

Implemented:

- Node sidecar resolves imported plugin manifests.
- Sidecar writes catalog and receipts.
- JSON-RPC bridge supports status/list/probe flows.
- `openclaw-context-budget` class is handled as native prompt-budget behavior.

Not complete:

- Plugin-specific tool execution.
- Prompt/tool-result/agent-end hook parity.
- Memory slot invocation parity.
- Imported OpenClaw plugin API runtime behavior.

Prioritize:

1. `openclaw-mem`
2. `openclaw-mem-engine`
3. context budget behavior
4. any channel/runtime tools actually needed in live turns

## Cron and Subagents

Current state:

- Native agent-turn cron is imported and planned, but held until explicit resume.
- Deterministic cron is imported/planned with `llmAccessAllowed=false`.
- Subagent ledgers are imported/planned.

Not complete:

- Actual native cron scheduler execution.
- Deterministic Windows-safe runner.
- Subagent worker execution/resume/cancel/handoff.

## Operations and Supervision

Implemented:

- `status` text and JSON.
- `enable-check`.
- Harness JSONL operational log.
- Loop heartbeats.
- Stop files.
- Compact progress event ledger plus Telegram/Discord action/status progress messages.
- `progress-delivery-loop` generated with the supervised loop bundle.
- Windows scheduled-task script generation.

Progress UI notes:

- Codex tool/action previews come from explicit command/path/query/name fields. Raw JSON wrappers and output-only deltas are skipped to keep messages Hermes-style compact.
- Progress delivery maintains separate body/status cursors in `state/channels/progress-delivery-state.json`; older single-message state can be taken over by the body lane.
- Normal Telegram/Discord outbox replies add a short plain-text `◆ OpenClaw` header. Progress messages do not add that header.

Current operational caveat:

- Generated scheduled-task scripts exist under:
  `imports/activation-harness/state/supervisor/windows-scheduled-tasks`
- In the current environment, `OpenClawHarness-*` scheduled tasks were not registered.
- Live loops were manually started as hidden PowerShell processes from the generated scripts.

Useful commands:

```powershell
.\target\debug\openclaw-harness.exe status --harness-home .\imports\activation-harness
.\target\debug\openclaw-harness.exe enable-check --harness-home .\imports\activation-harness
.\target\debug\openclaw-harness.exe channel-outbox-plan --harness-home .\imports\activation-harness --limit 20
.\target\debug\openclaw-harness.exe progress-delivery-once --harness-home .\imports\activation-harness
```

Stop loops:

```powershell
& .\imports\activation-harness\state\supervisor\windows-scheduled-tasks\scripts\stop-scheduled-tasks.ps1
```

Start loops manually when tasks are not registered:

```powershell
$scripts = @('runtime-loop.ps1','progress-delivery-loop.ps1','telegram-loop.ps1','discord-outbox-loop.ps1','discord-gateway-loop.ps1')
$dir = Resolve-Path .\imports\activation-harness\state\supervisor\windows-scheduled-tasks\scripts
foreach ($script in $scripts) {
  Start-Process -FilePath powershell.exe -ArgumentList @('-NoProfile','-ExecutionPolicy','Bypass','-File',(Join-Path $dir $script)) -WindowStyle Hidden
}
```

## Build and Test

Known passing commands:

```powershell
cargo fmt
cargo test -p openclaw-harness-core
cargo test -p openclaw-harness-cli
cargo build -p openclaw-harness-cli
```

Current passing results:

- Core tests: 135 passed.
- CLI tests: 15 passed.

Important test notes:

- Do not rely on Codex Desktop MSIX `codex.exe` for service runtime; it was not spawnable from the harness environment.
- Use `.tools/codex-cli/node_modules/.bin/codex.cmd`.
- Use `tools/openclaw-fake-codex-app-server` for offline runtime tests where model requests are not desired.
- Live Telegram/Discord smoke may make real channel sends.

## Suggested Next Development Order

1. Make supervision durable:
   - Register Task Scheduler tasks or implement Windows service wrapper.
   - Add restart/stale-heartbeat monitor.
   - Add log rotation and backup/export.
2. Close remaining channel parity:
   - Structured `replyContext` queue field for Telegram/Discord.
   - Narrow operator-safe channel history read command/tool.
   - Native attachment delivery for Discord/TG outgoing messages.
   - Attachment download/inspection policy when explicitly allowed.
   - Provider-native typing/cancel signals where the platform supports them.
3. Close memory parity:
   - Qdrant-native recall adapter.
   - LanceDB fallback.
   - routeAuto/autoRecall.
   - propose/store review workflow.
   - full symbolic canvas/graph parity.
4. Close plugin parity:
   - Real tool invocation.
   - Hook slots.
   - Memory plugin paths.
5. Implement cron and subagents:
   - native agent-turn scheduler.
   - deterministic cron runner.
   - subagent worker execution.
6. Broaden provider parity:
   - provider health diagnostics.
   - non-Codex provider execution adapters where needed.

## Safety Rules for Future Work

- Do not print raw tokens, keys, auth files, or `.env` values.
- Do not query the openclaw-mem gateway from this repo.
- Do not stop or restart Docker OpenClaw unless the operator explicitly asks.
- Do not assume scheduled tasks are installed; check `Get-ScheduledTask -TaskName 'OpenClawHarness-*'`.
- Before modifying runtime/channel code, check whether live loops are running.
- If live loops are running and code is rebuilt, restart loops so they use the new binary.
