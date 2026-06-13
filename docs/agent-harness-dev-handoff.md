# Agent Harness Development Handoff

Date: 2026-06-11

This handoff is for a developer or a new Codex session continuing implementation of the Rust Windows Agent Harness. It summarizes the project context, current architecture, verified state, important files, and next development priorities.

## Context Pointers

Start with these files before making changes:

- `README.md`: current live topology, active authority, command shortcuts, and documentation map.
- `docs/activation-readiness-plan.md`: activation checklist, verified smoke results, current warnings.
- `docs/agent-harness-channel-self-check.md`: Telegram DM and Discord DM prompts/checklists for `main` agent live self-checks.
- `docs/agent-harness-feature-parity.md`: feature parity matrix against the imported legacy deployment.
- `docs/agent-harness-feature-parity.html`: local browser version of the same feature parity summary.
- `docs/agent-worker-dispatch-strategy.md`: P4 worker dispatch MVP and remaining hardening for cron/subagents.
- `docs/round3-2-implementation-and-upgrade-plan.md`: timeout/progress implementation notes and the background-task/learning-loop upgrade plan.
- `.agent-harness/state/harness-registry.json`: imported harness registry used for live activation.
- `.agent-harness/harness-config.json`: harness runtime/security config.
- `.agent-harness/secrets/channel-credentials.env`: imported Telegram/Discord channel secrets. Do not print or commit values.
- `.agent-harness/secrets/memory-credentials.env`: imported memory embedding secret. Do not print or commit values.

## Documentation Discipline

Every new functional component and every behavior-changing hotfix must leave a
durable note under `/docs/`. At minimum, record the component's design rationale,
operator-facing behavior, important invariants, and a concise changelog entry
with the implementation date. Do not rely on git history, chat context, or
temporary `.debug` notes as the only explanation of why a feature works the way
it does.

Primary topology:

- Active harness state root: `.agent-harness`
- Active prompt/config authority: `.agent-harness/workspace`, `.agent-harness/openclaw.json`, and `.agent-harness/harness-config.json`
- Legacy source snapshot archive: `imports/openclaw-core-snapshot`
- Previous activation harness backup: `imports/activation-harness`
- Runtime workspace/Codex cwd for live loops: `D:\Warehouse\Research\OpenClaw_WSL`
- Harness CLI: `target/debug/agent-harness.exe`
- Codex CLI used by loops: `.tools/codex-cli/node_modules/.bin/codex.cmd`

## Current Baseline

Latest local status after the 2026-06-10 repo-local harness-home rebase, round3 fixes, review remediation, round3-1 runtime/channel fixes, runtime UX hardening, durable supervisor fallback/log retention, assistant narration routing, supervisor regeneration, and live activation:

- Readiness after the round3-2 rebuild/restart: `ready=true`, `passed=58`, `warnings=0`, `failed=0`.
- Latest `ops-cutover-receipt` recorded `status=ready` with note `runtime UX hardening and durable ops activation: assistant_stream progress suppressed; adaptive Codex JSONL idle timeout with max turn cap; supervisor direct-start fallback and log retention; docs synced`.
- Runtime timeout/progress reconciliation is implemented after the round3-2 triage: `timeout` is terminal for queue selection, status open-item counts, native typing context, and progress delivery state. Latest live readback after restart is `queued=123`, `open=0`, `prepared=123`, `completed=120`. A queued row with a timeout receipt should no longer appear as normal open work; retry requires a new queue id. See `docs/round3-2-implementation-and-upgrade-plan.md` for the full disposition and background-task upgrade plan.
- Outbox: `pending=0`, `delivered=186`, `retryable=0`, `invalid=0`; the old Telegram retry from the pre-narration-routing reply format was manually read back and marked delivered with provider id `manual-readback-20260611`.
- Channels: Telegram and Discord are enabled; Telegram probe ready; Discord gateway probe ready; Discord real inbound evidence is present; Discord reply-context receipt file is now tracked by status and will appear after the next handled Discord reply event.
- Loops: one runtime loop plus worker, progress delivery, Telegram, Discord outbox, and Discord gateway loops are running. The runtime loop owns bounded in-process runtime concurrency via `--runtime-concurrency 12`; regenerated scripts also pass configurable `--timeout-ms` and `--idle-timeout-ms` for Codex app-server turns. `supervisor-plan --runtime-workers <n>` maps to that flag instead of producing `runtime-loop-2` style task scripts. The status surface tracks the 6 canonical loop heartbeats; the supervisor plan contains 6 task entries.
- Runtime UX hardening: low-value `assistant_stream` progress previews are no longer emitted/rendered, Codex `agentMessage` items are split by protocol phase into `assistant_narration` versus final assistant replies, `response.assistantNarrationMode` defaults to `progress_panel`, and the Codex protocol idle timeout renews on each JSONL event while a separate max-turn timeout prevents infinite runs.
- Durable ops: generated `start-scheduled-tasks.ps1` falls back to hidden direct `Start-Process` runner launches when `AgentHarness-*` scheduled tasks are not registered; generated runner scripts retain the newest 20 supervisor logs per component.
- Memory: Qdrant edge snapshot present, `openclaw-mem.sqlite` present, active recall via `sqlite-vector`, `qdrantParity=snapshot-preserved; native-recall-not-active`, embedding secret migrated, vector recall ready, prompt context ready, lifecycle receipts present, capture candidates=7, compact canvas written, and `memory-hook` receipt recorded.
- Plugins: sidecar catalog present, 2 manifest-derived tools visible, sidecar bridge OK, `hooks.invoke` receipt recorded, and `memory.slot` receipt recorded.
- Workers: SQLite worker store is implemented at `state/workers/worker-jobs.sqlite`; `worker-loop` is generated and running; current worker store totals are `total=0`, `pending=0`, `running=0`, `failedTerminal=0`.
- Validation: latest local implementation test pass is `cargo test --workspace` with core 174 tests and CLI 16 tests passing, plus `cargo fmt --all`. Deployment validation passed `cargo build --workspace`, gateway restart, live `status`, live `enable-check`, outbox plan, and process listing. Previous round3-1 activation also passed `cargo fmt --check`, `cargo build --target-dir target\codex-build`, supervisor-plan smoke, process command-line verification, and `jsonl-repair` smoke.
- Live-test readiness: regenerated supervisor scripts point to `target/debug/agent-harness.exe`, `.agent-harness` as `--source-home`, `--runtime-workspace D:\Warehouse\Research\OpenClaw_WSL`, `tools/agent-discord-gateway/index.mjs`, one `runtime-loop.ps1`, and `worker-loop.ps1`; loops were restarted manually as hidden PowerShell processes because `AgentHarness-*` scheduled task registration returned access-denied in this environment; latest `status` and `enable-check` are ready with no warnings.

Current non-blocking warning:

- None. `memory-lancedb` is hidden unless source config explicitly selects LanceDB as the active memory backend.

2026-06-10 rename/debranding and P0 restart state:

- Live harness loops were stopped for development before renaming, then restarted after regenerating the supervisor bundle. Check processes and heartbeat status before rebuilding or changing runtime/channel code.
- Crates are now `crates/agent-harness-core` and `crates/agent-harness-cli`; the binary is `target/debug/agent-harness.exe`.
- Main CLI source argument is now `--source-home`; the default target harness home is `.agent-harness`, and `.agent-harness/` is in `.gitignore`.
- Harness-owned environment keys now use `AGENT_HARNESS_*`; source-home override uses `AGENT_SOURCE_HOME`.
- Ignored local activation secret files under `.agent-harness/secrets` had key names migrated to the new prefix without printing values. Do not commit those files.
- Tool directories are now `tools/agent-discord-gateway`, `tools/agent-fake-codex-app-server`, and `tools/agent-plugin-sidecar`.
- Feature parity docs are now `docs/agent-harness-feature-parity.md` and `docs/agent-harness-feature-parity.html`; update both together.
- Supervisor scripts were regenerated after rename, after review remediation, after round3-1, and after runtime UX/durable ops hardening. `--runtime-workers 12` writes `--runtime-concurrency 12` into `runtime-loop.ps1`, and new supervisor runtime flags write `--timeout-ms` plus `--idle-timeout-ms`. Re-run `supervisor-plan` after future CLI/path changes. In this environment, `AgentHarness-*` scheduled tasks were not registered; the generated start script now falls back to hidden direct runner starts.

## Architecture Map

Workspace crates:

- `crates/agent-harness-core`: core library for import, registry, channel routing, command state, prompt assembly, runtime planning, memory, plugins, cron/subagent planning, status/readiness.
- `crates/agent-harness-cli`: command-line surface for all runtime operations and smoke checks.

Important core modules:

- `registry.rs`: parses imported legacy registry, providers, agents, plugins.
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
- `runtime_queue.rs`, `runtime_worker.rs`, `runtime_pipeline.rs`: queue, preparation, runtime capacity leases, runtime pipeline, and outbox attachment extraction.
- `memory.rs`: imported memory search, vector recall, credentials export, lifecycle, canvas worker.
- `workers.rs`: durable worker store, lease/run/reap/cancel/status, deterministic shell audit, LLM runtime-queue handoff, watchdog and master-wakeup jobs, concurrency and rate-lease gates.
- `worker_adapters.rs`: native cron, deterministic cron, and subagent plan-to-worker enqueue adapters.
- `ops.rs`: non-secret backup, cutover receipt, and supervisor stop-file control.
- `status.rs`: `status` report aggregation.
- `activation.rs`: `enable-check` readiness checks.
- `supervisor.rs`: Windows scheduled task script generation.
- `cron.rs`, `deterministic_cron.rs`, `subagents.rs`: import/dry-run planning lanes.

Auxiliary tooling:

- `tools/agent-discord-gateway/index.mjs`: Node Discord Gateway wrapper.
- `tools/agent-fake-codex-app-server`: offline fake Codex app-server for tests/smoke.

## Runtime Flow

Channel normal message flow:

1. Telegram/Discord adapter receives a DM or configured group/guild message.
2. The adapter enforces admin/limited/open-limited access policy before runtime dispatch.
3. `channel-receive` normalizes it and carries bounded inbound reply/media context when available.
4. Slash commands are handled immediately; ordinary messages enqueue a runtime item.
5. `runtime-loop` inspects runtime capacity and runs claimable queue ids through bounded in-process runtime tasks.
6. `queue-prepare` builds a turn plan and prompt bundle.
7. `codex-plan` and `codex-run` start Codex app-server.
8. Runtime/Codex writes compact action/status events to `state/runtime-queue/progress-events.jsonl`.
9. `progress-delivery-loop` sends or edits separate compact Telegram/Discord action and status messages for authorized targets.
10. `codex-run` captures Codex `agentMessage` item ids/phases. `phase=commentary` becomes `assistant_narration`; `phase=final_answer` becomes the final assistant reply. Default `progress_panel` routes narration to the editable progress status and keeps the final channel reply clean.
11. `codex-complete` records transcript, trajectory, Codex binding, memory lifecycle evidence, and outbox reply.
12. Telegram/Discord outbox delivery loops send text plus structured attachments and record delivery receipts.
   Telegram uses `sendPhoto`/`sendDocument`; Discord uses multipart upload and splits text over Discord's 2000-character content limit before recording the original delivery id as delivered.

Prompt strategy:

- The harness does not keep appending stable system/context sections every turn.
- It uses prompt bundle assembly plus prompt-injection ledger.
- Stable prompt files and selected skills can be reused by reference.
- Each injected prompt file carries an explicit role header so the agent knows how to treat it. Known mappings include `AGENTS.md` for workspace instructions, `SOUL.md` for persona/voice, `TOOLS.md` for tool policy, `USER.md` for user preferences, `IDENTITY.md` for agent identity, `HEARTBEAT.md` for cadence/liveness guidance, and `BOOTSTRAP.md` for startup context.
- Skills are dynamic task context. `/status` and other command-only turns can legitimately show `Skills: 0 selected`; ordinary agent turns use the merged skill index and load relevant `SKILL.md` bodies on demand. Harness imported skill discovery covers both `skills\legacy-imports` and `skills\openclaw-imports`, because current OpenClaw workspace skill imports use the latter namespace.
- Imported memory context is inserted before the user message as a bounded untrusted `<MEMORY_CONTEXT>` section.
- Telegram reply/media metadata and Discord reply/attachment metadata are inserted before the user message as a bounded untrusted `<INBOUND_CHANNEL_CONTEXT>` section.
- Reply targets include preview, source, length, truncation metadata, and up to 4000 characters of referenced text when the platform payload exposes text.
- Raw Telegram file IDs and Discord attachment URLs are deliberately not injected into the prompt bundle.
- `queue-prepare` resolves prompt files, skills, and registry state from the imported legacy source home `workspace` when present; a separate runtime workspace is only used as Codex cwd and must not make prompt files disappear after `/new` or other session changes. Round3 fixed the case where a queued item used `D:\Warehouse\Research\OpenClaw_WSL` as runtime cwd and `/status` showed `Prompt files 0/7`: turn planning now falls back to the imported workspace when the runtime workspace has no prompt files.
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
- `/stop` is command-state aware and writes a fresh session cancel marker under `state/runtime-queue/cancel-requests/`. The Codex app-server runtime polls that marker, sends a best-effort `turn/interrupt`, terminates the child process, and records `canceled` receipts.
- `/stop` does not yet supervise detached services or arbitrary background processes. Round3-2 tracks `background-status` and `/stop job <id>` as the next upgrade for local servers, long cron/image jobs, and other post-turn services.

Channel access policy:

- Telegram/Discord DMs fail closed and require an admin user id.
- Legacy `AGENT_HARNESS_TELEGRAM_ALLOWED_USER_IDS` and `AGENT_HARNESS_DISCORD_ALLOWED_USER_IDS` remain admin-compatible for migration.
- Configured Telegram groups and Discord guild channels can grant limited users ordinary-message access plus read-only `/status`, `/model`, and `/think` queries.
- Limited users cannot switch model/thinking state and cannot run `/new`, `/stop`, `/steer`, or `/btw`.
- Explicit open-limited group/guild mode is supported through `AGENT_HARNESS_TELEGRAM_GROUP_OPEN` and `AGENT_HARNESS_DISCORD_GROUP_OPEN`.

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
- OpenClaw-compatible `memory-hook` adapter receipts for `before-prompt-build`, `agent-end`, `store-propose`, `memory-slot`, `tool-result`, and `canvas-maintenance`.
- `status --json` includes `memory.summary.activeRecallBackend`, `memory.summary.qdrantParity`, and `memory.summary.captureCandidateCount`.

Important limitation:

- Qdrant edge is preserved, detected, and surfaced as the primary imported snapshot, but the active recall lane currently uses SQLite vector recall. The Rust harness does not raw-read Qdrant segment files. Qdrant-native recall should be added through a sidecar/service or supported snapshot/API path.

OpenClaw-mem integration boundary:

- Treat the `openclaw-mem` repo as an upstream/external memory product. This harness consumes its released or checked-out CLI, gateway, plugin, engine, graph, and sidecar surfaces; it should not mutate `openclaw-mem` internals or schemas to create Agent Harness-only behavior.
- Implement compatibility inside Agent Harness through OpenClaw-compatible plugin adapters and agent-turn hooks: `before_prompt_build`/`before_agent_start` recall, tool-result observation capture, `agent_end` episodes/autoCapture, memory slot operations, sidecar/gateway calls, and bounded ContextPack/prompt injection.
- Preserve the OpenClaw hook semantics and payload shapes where possible so `openclaw-mem`, `openclaw-mem-engine`, and related sidecars can remain dual-compatible with both legacy OpenClaw and Agent Harness.

## Plugin Layer

Implemented:

- Node sidecar resolves imported plugin manifests.
- Sidecar writes catalog and receipts.
- JSON-RPC bridge supports status/list/probe flows.
- JSON-RPC bridge supports `hooks.invoke` and `memory.slot` receipt paths for plugin parity smoke.
- `openclaw-context-budget` class is handled as native prompt-budget behavior.

Not complete:

- Plugin-specific tool execution.
- Full prompt/tool-result/agent-end hook execution beyond MVP receipts.
- Full memory slot execution beyond MVP receipts.
- Imported legacy plugin API runtime behavior.

Prioritize:

1. `openclaw-mem`
2. `openclaw-mem-engine`
3. context budget behavior
4. any channel/runtime tools actually needed in live turns

## Cron and Subagents

Current state:

- Native agent-turn cron is imported/planned and can be converted into durable `llm_subagent` worker jobs with `native-cron-enqueue --resume-cron`.
- Extended deterministic crontab/Supercronic-style cron is imported/planned with `llmAccessAllowed=false` and can be converted into durable `deterministic_shell` worker jobs with `deterministic-cron-enqueue --allow-deterministic-run`. The enqueue path defaults to dry-run shell audit unless `--execute-shell` is explicit.
- Subagent ledgers are imported/planned and resumable queued/running entries can be converted into durable `llm_subagent` worker jobs with `subagent-enqueue --resume-subagents`.
- P4 direction is now implemented as an MVP unified worker dispatch, not separate legacy-style cron and subagent runners.
- The design borrows durable job-queue semantics from gbrain Minions: two-phase persistence, leases, retries/backoff, shell audit, rate leases, child jobs, and watchdog/master wakeup orchestration.
- The design does not borrow gbrain's memory strategy; memory remains the harness adapter roadmap.
- The two cron source lanes stay separate for policy and import fidelity: native `.openclaw/cron` may enqueue LLM-backed agent/subagent work; deterministic crontab/Supercronic-style workspace runners enqueue only no-LLM shell jobs.
- Subagent and deterministic child-job completion must be able to wake the master agent. Fan-out work should create a `job_group_id` and a deterministic watchdog that wakes the master on all-completed, any-failed, timeout, checkpoint, or threshold policies with bounded artifact pointers.
- Worker leasing enforces harness-configurable concurrency limits before execution: a global limit, a per-agent/group limit, a per-agent-per-channel limit, optional lane limits, and optional rate leases. The current live defaults are global 12, per-agent 6, per-agent-per-channel 3, lane limits `llm=6`, `shell=6`, `watchdog=2`, `maintenance=2`, `plugin=2`. If a limit is reached, extra subagent, deterministic, or cron jobs stay queued instead of starting.

Implemented MVP:

- `WorkerStore` schema and job state machine.
- Deterministic shell lane with allow-listed scripts, structured argv/env, capped audit output, timeout, and retry.
- LLM subagent/master wakeup lane using the existing runtime queue and Codex runner foundations.
- Native cron, deterministic cron, and imported subagent adapters that enqueue worker jobs.
- Child job grouping, rate leases, watchdog jobs, master wakeup jobs, wake policy idempotency, and bounded artifact-pointer summaries for mixed LLM/deterministic child groups.
- Harness config schema/status reporting for global, per-agent/group, per-agent-per-channel, lane worker concurrency limits, allowed script roots, and rate lease windows.

Remaining hardening:

- Cascading timeout/cancel for active child processes.
- Provider-specific backoff and per-provider rate profiles.
- Watermarking for repeated cron scheduler ticks beyond explicit operator enqueue.
- Round-robin fairness across busy master groups.
- Multi-host store abstraction if a Postgres/service-backed pool becomes necessary.

Implementation target:

- See `docs/agent-worker-dispatch-strategy.md`.
- MVP storage should be SQLite under `state/workers/worker-jobs.sqlite` with a later `WorkerStore` abstraction for Postgres/service-backed deployments if multi-host worker pools become necessary.
- Cron schedulers should enqueue jobs only. Worker loops should own execution and recovery.

## Operations and Supervision

Implemented:

- `status` text and JSON.
- `enable-check`.
- Harness JSONL operational log.
- Loop heartbeats.
- Stop files.
- Worker stop file and `worker-loop` supervisor script.
- `ops-backup`, `ops-cutover-receipt`, and `ops-control stop|start|status`.
- Compact progress event ledger plus Telegram/Discord action/status progress messages.
- `progress-delivery-loop` generated with the supervised loop bundle.
- Windows scheduled-task script generation.
- Atomic JSON writes for mutable state files such as channel state, runtime lease state, run-once reports, delivery cursors, Codex run reports, and execution receipts.
- Runtime failure policy records `failed-terminal` for non-retryable protocol/preflight/spawn/no-plan failures and stops retrying timeout failures after three attempts. Canceled runs are terminal and reply with `Stopped.`.

Progress UI notes:

- Codex tool/action previews come from explicit command/path/query/name fields. Raw JSON wrappers, output-only deltas, and long PowerShell executable paths are compacted or skipped to keep messages Hermes-style compact. Common PowerShell wrappers are summarized as short forms such as `pwsh: read file ...`, `pwsh: get date`, or `pwsh: agent-harness status`.
- Progress delivery maintains separate body/status cursors in `state/channels/progress-delivery-state.json`; older single-message state can be taken over by the body lane.
- Permission-denied or policy-skipped progress deliveries still advance the cursor, so Telegram does not repeatedly receive the same `Working` status event while Discord remains normal.
- Normal Telegram/Discord final replies are sent as trimmed assistant text without a mechanical `◆ Agent` header. Progress messages keep their compact action/status format.

Current operational caveat:

- Generated scheduled-task scripts exist under:
  `.agent-harness/state/supervisor/windows-scheduled-tasks`
- In the current environment, `AgentHarness-*` scheduled tasks were not registered.
- Live loops were manually started as hidden PowerShell processes from the generated scripts.

Useful commands:

```powershell
.\target\debug\agent-harness.exe status --harness-home .\.agent-harness
.\target\debug\agent-harness.exe enable-check --harness-home .\.agent-harness
.\target\debug\agent-harness.exe channel-outbox-plan --harness-home .\.agent-harness --limit 20
.\target\debug\agent-harness.exe progress-delivery-once --harness-home .\.agent-harness
.\target\debug\agent-harness.exe worker-status --harness-home .\.agent-harness
.\target\debug\agent-harness.exe ops-backup --harness-home .\.agent-harness --label pre-cutover
.\target\debug\agent-harness.exe ops-cutover-receipt --harness-home .\.agent-harness --note "pre-cutover"
```

Stop loops:

```powershell
& .\.agent-harness\state\supervisor\windows-scheduled-tasks\scripts\stop-scheduled-tasks.ps1
```

Start loops manually when tasks are not registered:

```powershell
$scripts = @('runtime-loop.ps1','worker-loop.ps1','progress-delivery-loop.ps1','telegram-loop.ps1','discord-outbox-loop.ps1','discord-gateway-loop.ps1')
$dir = Resolve-Path .\.agent-harness\state\supervisor\windows-scheduled-tasks\scripts
foreach ($script in $scripts) {
  Start-Process -FilePath powershell.exe -ArgumentList @('-NoProfile','-ExecutionPolicy','Bypass','-File',(Join-Path $dir $script)) -WindowStyle Hidden
}
```

## Build and Test

Known passing commands:

```powershell
cargo fmt
cargo test -p agent-harness-core
cargo test -p agent-harness-cli
cargo build -p agent-harness-cli
```

Current passing results:

- Core tests: 167 passed after round3 prompt, progress, runtime lease, atomic state writes, cancellation, terminal failure, supervisor, runtime UX hardening, and worker concurrency fixes.
- CLI tests: 16 passed.
- Build: `cargo build -p agent-harness-cli` passed after stopping old live loops that were holding `target/debug/agent-harness.exe`.

Important test notes:

- Do not rely on Codex Desktop MSIX `codex.exe` for service runtime; it was not spawnable from the harness environment.
- Use `.tools/codex-cli/node_modules/.bin/codex.cmd`.
- Use `tools/agent-fake-codex-app-server` for offline runtime tests where model requests are not desired.
- Live Telegram/Discord smoke may make real channel sends.

## Suggested Next Development Order

1. Make supervision durable beyond MVP:
   - Register Task Scheduler tasks or implement Windows service wrapper.
   - Add restart/stale-heartbeat monitor.
   - Add operator restart policy.
2. Close remaining channel parity:
   - Narrow operator-safe channel history read command/tool.
   - Attachment download/inspection policy when explicitly allowed.
   - Provider-native typing/cancel signals where the platform supports them.
3. Close memory parity:
   - Qdrant-native recall adapter.
   - LanceDB fallback only if a future imported source explicitly selects LanceDB.
   - routeAuto/autoRecall.
   - propose/store review workflow.
   - full symbolic canvas/graph parity.
4. Close plugin parity beyond MVP receipts:
   - Real tool invocation.
   - Hook execution policy.
   - Memory plugin paths.
5. Harden unified worker dispatch:
   - cascading timeout/cancel, provider backoff, repeated cron watermarks, fairness, and multi-host store abstraction.
6. Broaden provider parity:
   - provider health diagnostics.
   - non-Codex provider execution adapters where needed.

## Safety Rules for Future Work

- Do not print raw tokens, keys, auth files, or `.env` values.
- Do not stop or restart Docker legacy unless the operator explicitly asks.
- Do not assume scheduled tasks are installed; check `Get-ScheduledTask -TaskName 'AgentHarness-*'`.
- Before modifying runtime/channel code, check whether live loops are running.
- If live loops are running and code is rebuilt, restart loops so they use the new binary.
