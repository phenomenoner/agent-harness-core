# Rust OpenClaw Windows Harness Assessment

Date: 2026-06-08

## Executive Summary

Building a Windows 11 Rust implementation of the core OpenClaw agent harness is feasible. The right target is not a full OpenClaw rewrite. The pragmatic target is a small Rust harness that preserves the OpenClaw data model, delegates coding-agent execution to Codex app-server, and bridges existing OpenClaw plugins through a compatibility sidecar until the runtime contracts are stable enough for native Rust ports.

The initial local inspection found an existing Docker OpenClaw stack:

- Gateway container: `openclaw-ubuntu`
- Gateway ports: `127.0.0.1:18789-18790`
- OpenClaw state mount inside container: `/root/.openclaw`
- Workspace bind mount: `D:\Warehouse\Research\OpenClaw_WSL` to `/workspace`
- Runtime version: `2026.5.26`
- Agents: 11 active configured agents reported by status; deeper filesystem inventory found 24 agent directories under `/root/.openclaw/agents`
- Sessions: 161 session records reported by status
- Channels: Telegram and Discord enabled
- Memory slot: `openclaw-mem-engine`
- Loaded relevant plugins: `codex`, `openai`, `openrouter`, `telegram`, `discord`, `openclaw-mem`, `openclaw-mem-engine`, `openclaw-context-budget`, `acpx`

Additional local findings on 2026-06-08:

- OpenClaw native agent-turn cron lives in `/root/.openclaw/cron`.
- Native cron source files: `jobs.json`, `jobs-state.json`, and `runs/*.jsonl`.
- Current native cron count: 110 jobs, 65 enabled.
- Native cron job distribution by agent: `main` 55, `cron-lite` 35, `steamer-cron` 13, `mem-cron` 3, `comms-cron` 2, unassigned 2.
- Native cron schedules: 105 `cron` schedules and 5 `at` schedules.
- Native cron wake modes: 82 `now`, 28 `next-heartbeat`.
- Native cron job schema includes `agentId`, `createdAtMs`, `delivery`, `enabled`, `id`, `name`, `payload`, `schedule`, `sessionTarget`, `wakeMode`, `failureAlert`, `description`, and per-job `state`.
- Deterministic cron lives under workspace tools, not under native cron: `/root/.openclaw/workspace/tools/cron-runner` and `/root/.openclaw/workspace/tools/backup-cron-runner`.
- Deterministic cron runner files include crontabs, shell job scripts, locks, state, logs, and bundled `supercronic`.
- Current deterministic cron crontabs contain 18 main job entries and 4 backup job entries after ignoring env/header lines.
- `pgrep -af supercronic` did not show a running supercronic process during inspection, so import should preserve configuration/state but startup should be explicit.
- Subagent state exists at `/root/.openclaw/subagents/runs.json`.
- Agent-local state exists under `/root/.openclaw/agents/<agent-id>/agent`, commonly `models.json`, `auth-profiles.json`, `auth-state.json`, and sometimes `auth.json`.
- Agent-local sessions live under `/root/.openclaw/agents/<agent-id>/sessions`.
- The container workspace bind mount is readable directly on the Windows host at `D:\Warehouse\Research\OpenClaw_WSL`; this should be accepted as an explicit `--workspace` source when importing.
- If the Docker container is stopped but not deleted, host-mounted workspace files remain readable from Windows. Container-internal state such as `/root/.openclaw` still needs Docker volume/container copy access or a previous export snapshot.
- The host path `D:\Warehouse\Research\OpenClaw_WSL` is a broad workspace root with multiple OpenClaw workspace subdirectories such as `openclaw-workspace-cron`; dry-run import should target the active workspace subdirectory when importing a specific agent workspace.
- Some workspace entries created from Linux appear on Windows as reparse points, such as `openclaw-workspace-cron\tools`; deterministic cron import needs explicit symlink/reparse handling or a container/WSL-side export to avoid missing linked runner files.

## Recommended Architecture

The project should be split into small boundaries:

1. Rust harness core
   - Owns config loading, agent/session routing, channel envelopes, import planning, policy decisions, and persistence.
   - Keeps OpenClaw-compatible concepts: agent id, workspace, prompt files, memory root, session key, channel identity.

2. Codex runtime adapter
   - Talks to `codex app-server` over stdio JSON-RPC first.
   - Uses Codex app-server for native thread resume, compaction, approval events, tool events, and coding-agent execution.
   - Does not reimplement Codex model/tool loop in the harness.

3. Provider adapter layer
   - OpenAI/Codex route through Codex app-server by default.
   - OpenRouter and other OpenAI-compatible providers can be handled by an HTTP provider adapter.
   - Model selection should remain provider/model based, with a separate runtime selection policy.

4. Channel layer
   - Telegram: use `teloxide`.
   - Discord: use `serenity`/`poise` for a faster bot implementation, or `twilight` when lower-level control matters.
   - Normalize all inbound messages into one internal `ChannelEnvelope`.

5. Plugin compatibility sidecar
   - Do not start by rewriting the OpenClaw TypeScript plugin SDK in Rust.
   - Run a Node/OpenClaw plugin-host sidecar that exposes tools, hooks, memory slot operations, and provider metadata over JSON-RPC or gRPC.
   - Port only high-value stable plugins to native Rust after the bridge contract proves itself.

6. Memory integration
   - Treat `openclaw-mem` as a first-class external service and data source.
   - Use gateway/pack/search contracts instead of direct SQLite mutation where possible.
   - Preserve raw Markdown memory, JSONL observation logs, SQLite DBs, LanceDB/Qdrant/Postgres side data, and receipts.

7. Skill-first runtime
   - Treat skills as procedural memory that the harness can discover, rank, view, create, patch, and reference per task.
   - Keep a small indexed summary for each skill and load full `SKILL.md` plus `references/`, `templates/`, `scripts/`, and `assets/` only when selected.
   - Import OpenClaw workspace skills, managed OpenClaw skills, and project `.agents/skills` into a stable skill registry before runtime prompt assembly.
   - Add a skill writer/linter path so agents can turn repeated task procedures into reviewed skills instead of growing global prompt files.

## Hermes Design References

Hermes contributes two separate ideas, and the harness should keep them separate in implementation:

1. Migration strategy
   - This is about safely moving OpenClaw state into the new harness home.
   - It should use dry-run first, structured reports, redacted receipts, presets, and conflict policies.
   - It should not decide per-turn runtime behavior.

2. Skill-first runtime
   - This happens at the start of every agent turn, after channel command parsing and before prompt assembly.
   - The harness should match task-relevant skills by agent, channel, workspace, tools, and current user intent.
   - During or after the turn, the harness should detect reusable procedures and propose or apply skill create/patch/archive operations with receipts.
   - Imported skills are only one source. Runtime-created and runtime-improved skills must become future turn context through the same index.

## Import Strategy

Use a staged import. The first stage is read-only and produces an import plan. Later stages perform copy/transform/resume.

Hermes Agent is a useful migration reference here. Its OpenClaw migration skill uses `hermes claw migrate`, starts with `--dry-run`, emits structured reports, supports presets such as `user-data` and `full`, keeps secrets opt-in, and handles file conflicts with `skip`, `overwrite`, or `rename`. The same safety shape should be reused, but not copied blindly: Hermes can archive some OpenClaw cron and multi-agent data because Hermes has its own scheduler/profile model. This Rust harness must actively preserve and execute OpenClaw native cron, deterministic cron, multi-agent routing, and subagent ledgers for gateway handoff.

Hermes references checked:

- [Hermes Agent README](https://github.com/NousResearch/hermes-agent)
- [Hermes Skills System](https://hermes-agent.nousresearch.com/docs/user-guide/features/skills/)
- [Hermes OpenClaw migration guide](https://hermes-agent.nousresearch.com/docs/zh-Hans/guides/migrate-from-openclaw)
- [OpenClaw migration skill](https://github.com/NousResearch/hermes-agent/blob/main/optional-skills/migration/openclaw-migration/SKILL.md)
- [OpenClaw to Hermes migration script](https://github.com/NousResearch/hermes-agent/blob/main/optional-skills/migration/openclaw-migration/scripts/openclaw_to_hermes.py)

Importer policy borrowed from Hermes:

- Always provide dry-run first and write `report.json` plus `summary.md` with per-item status, source, destination, reason, and redacted details.
- Use presets: `user-data` for normal handoff, `full` for operator-approved deep import, and module include/exclude flags for narrow repair runs.
- Treat secrets as opt-in. Resolve environment references and known credential fields only when the operator explicitly enables secret migration; otherwise write redacted receipts and prompt for re-entry.
- Use conflict modes: `skip`, `overwrite` with backup, and `rename`. Hash identical files and mark them as already matched rather than overwriting.
- Keep source fallback logic: OpenClaw home workspace, `workspace.default`, `workspace-main`, per-agent workspaces, and explicit Windows host path overrides such as `D:\Warehouse\Research\OpenClaw_WSL`.
- Rebrand only human-readable prompt text where needed. Do not rewrite code, scripts, serialized state, memory DB rows, or historical transcripts in place.
- Keep cron and multi-agent import as active runtime state for this harness, even where Hermes archives those records for manual recreation.

1. Config import
   - Read `openclaw.json`.
   - Preserve provider ids, agent ids, channel configs, plugin entries, plugin slots, and model fallbacks.
   - Move secrets to Windows Credential Manager or an encrypted local key store rather than writing them back to plain text.

2. Agent import
   - Preserve `/agents/<agent-id>` directories, not only `openclaw.json`.
   - Preserve per-agent `sessions/`, `agent/models.json`, `agent/auth-profiles.json`, `agent/auth-state.json`, and non-secret runtime metadata.
   - Import the OpenClaw multi-agent routing model before enabling cron, because cron jobs reference `agentId`.
   - Keep inactive/probe agents available but disabled until their provider credentials are validated.

3. Workspace import
   - Preserve `AGENTS.md`, `SOUL.md`, `TOOLS.md`, `USER.md`, `IDENTITY.md`, `HEARTBEAT.md`, `BOOTSTRAP.md`.
   - Preserve `skills/<skill>/SKILL.md`.
   - Preserve workspace-local `memory/`, tools, scripts, handoffs, and operational state.
   - Support host-mounted workspace import from `D:\Warehouse\Research\OpenClaw_WSL` when the Docker container is stopped.

4. Skill import
   - Import skills from workspace `skills/`, `.openclaw/skills/`, `.agents/skills/`, and workspace `.agents/skills/`.
   - Preserve the full skill directory shape: `SKILL.md`, `references/`, `templates/`, `scripts/`, and `assets/`.
   - Store imported OpenClaw skills in a distinct namespace or category such as `openclaw-imports` while retaining original ids for reference.
   - Build a progressive disclosure index: skill list metadata first, full skill body on demand, referenced files only when the selected skill asks for them.
   - Provide agent-managed skill operations: propose, create, patch, lint, and archive, with receipts and review gates for scripts or destructive shell snippets.

5. Session import
   - Read `/agents/<agent-id>/sessions/sessions.json`.
   - Preserve session transcript files: `*.jsonl`.
   - Preserve trajectories: `*.trajectory.jsonl` and `*.trajectory-path.json`.
   - Preserve Codex binding mirrors: `*.jsonl.codex-app-server.json`.
   - Initial Rust support should expose these as searchable historical context before attempting active native resume.

6. Native cron import
   - Read `/cron/jobs.json` and `/cron/jobs-state.json`.
   - Preserve `id`, `name`, `agentId`, `enabled`, `schedule`, `wakeMode`, `sessionTarget`, `delivery`, and payload metadata.
   - Preserve `runs/*.jsonl` as historical execution receipts.
   - On first cutover, do not immediately fire overdue jobs. Compute a cutover watermark and require an explicit `resume-cron` command.
   - Runtime implementation needs an agent-turn scheduler that can enqueue a message into the selected agent's session and invoke the LLM-backed runtime.

7. Deterministic cron import
   - Read workspace crontabs and job scripts under `tools/cron-runner` and `tools/backup-cron-runner`.
   - Preserve `locks/`, `state/`, and `logs/` as operational evidence.
   - Run these jobs through a deterministic job runner path with no LLM/model request capability.
   - Prefer native Rust process supervision on Windows; use WSL/Docker only as a compatibility fallback for shell scripts that are not portable yet.

8. Subagent import
   - Preserve `/subagents/runs.json`.
   - Preserve subagent ready/running/completed ledgers before enabling native worker execution.
   - Keep subagent execution behind a queue with per-agent concurrency limits, cancellation, retries, and receipt files.

9. Memory import
   - Preserve `.openclaw/memory/*.md`, `openclaw-mem.sqlite`, `openclaw-mem-observations.jsonl`, `openclaw-mem-episodes.jsonl`, mem-engine DBs, LanceDB data, and graph/vector sidecars.
   - SQLite files should be copied from a stopped gateway or through a backup API to avoid WAL loss.

10. Plugin import
   - Import install records and config, but execute plugins through the sidecar initially.
   - Refresh or rebuild stale plugin registry state instead of trusting stale persisted paths.

11. Credential and login-state import
   - Importing raw login state is best-effort only.
   - Provider API keys, Telegram/Discord bot tokens, and OpenClaw gateway secrets should be migrated into Windows Credential Manager or an encrypted harness vault.
   - Browser/session cookies and service-specific login state should be treated as non-portable unless the source plugin explicitly supports export/import.
   - The handoff path should assume credentials may need to be re-entered and make that flow cheap.

## Gateway Handoff Requirements

To shut down the Docker OpenClaw gateway and let the Rust Windows harness take over current work, the MVP needs more than import. It needs runtime parity for the active surfaces currently doing work.

Required for a real cutover:

1. State export/import
   - A read-only `doctor` and `import-plan`.
   - A copy planner that maps `/root/.openclaw` to a Windows harness home.
   - SQLite-safe backup for memory/plugin DBs.
   - Receipts for every copied or skipped path.

2. Multi-agent registry
   - Parse `openclaw.json` `agents.defaults` and `agents.list`.
   - Load agent directories from `/agents/<agent-id>`.
   - Resolve per-agent model/provider/auth settings.
   - Route each inbound channel message or cron job to the correct agent id.

3. Codex runtime adapter
   - Start and supervise the custom Codex CLI/app-server.
   - Create/resume sessions per agent.
   - Feed prompt files, memory pack, channel envelope, and imported session context.
   - Persist transcript, trajectory, and Codex binding mirror files in an OpenClaw-compatible layout.

4. Provider routing
   - Support Codex/OpenAI as the primary path.
   - Support OpenRouter/OpenAI-compatible providers with model selection and fallback.
   - Preserve provider/model policy per agent and per cron payload.

5. Tool execution and approval
   - Implement a tool registry.
   - Bridge OpenClaw plugin tools through a Node sidecar first.
   - Support shell/tool approval policy and audit logs.
   - Keep deterministic cron jobs on a separate execution path that cannot call model runtime.

6. Skill-first task context
   - Load skill metadata before prompt assembly.
   - Select relevant skills by task, agent id, channel, platform, and required tools.
   - Allow agents to propose new skills or patch existing skills after repeated procedures.
   - Treat executable skill snippets as reviewed code paths, not free-form prompt text.

7. Messaging channels
   - Telegram bot receive/send, direct-message mapping, delivery receipts, and retry queue.
   - Discord bot receive/send, DM/thread/channel mapping, delivery receipts, and retry queue.
   - Imported channel identity must map to the same OpenClaw session key shape where practical.
   - Shared channel command parser for `/new`, `/think`, `/stop`, `/steer`, `/btw`, `/model`, `/status`, and future OpenClaw-compatible chat commands.
   - Commands must be parsed before ordinary message dispatch and must behave consistently across Telegram DM and Discord DM.

8. Cron scheduler
   - Native agent-turn cron scheduler for `/cron/jobs.json`.
   - Runtime state writer for `/cron/jobs-state.json`.
   - Run logs compatible with `/cron/runs/*.jsonl`.
   - Deterministic cron scheduler for workspace crontabs and shell jobs.
   - Cutover safety: no automatic catch-up storm on first boot.

9. Memory
   - Import raw memory files and DB snapshots without requiring a running gateway.
   - Optional `openclaw-mem` gateway adapter for pack/search/propose when an operator enables it.
   - Restore mem-engine lookup/writeback jobs.
   - Treat imported memory as evidence, not executable instruction.

10. Plugin compatibility
   - Node plugin-host sidecar that can load OpenClaw plugins, expose tools/hooks/memory slots, and return typed receipts.
   - Rust-native plugin ABI can wait until the bridge has real coverage.

11. Operations
   - Windows service or scheduled startup.
   - Structured logs.
   - Health endpoint.
   - Backup/export command.
   - Dry-run cutover checklist.

Minimum viable handoff order:

1. Import state and agents.
2. Import and index skills.
3. Bring up Codex runtime adapter.
4. Bring up Telegram/Discord.
5. Bring up shared channel commands in Telegram/Discord DM.
6. Bring up memory import and whichever memory adapter is enabled.
7. Enable native cron in dry-run.
8. Enable deterministic cron.
9. Enable plugin sidecar tools.
10. Stop Docker gateway and run Rust harness with cron catch-up disabled.

## Major Risks

The largest risk is full OpenClaw plugin compatibility. OpenClaw plugins can register providers, channels, CLI backends, agent harnesses, hooks, tools, memory slots, services, and commands. Rebuilding that API natively in Rust before the rest of the harness exists would dominate the project.

The second risk is Codex app-server protocol churn. It is the right integration point, but the harness should pin tested Codex versions and isolate protocol structs behind a narrow adapter.

The third risk is import correctness. Sessions, memory DBs, WAL files, channel queues, plugin state, and Codex app-server mirrors can drift if imported while the old gateway is running.

## Implementation Phases

Current implemented foundation:

- `doctor` performs read-only OpenClaw layout inventory.
- `import-plan` reports staged readiness across config, workspace, agents, skills, sessions, native cron, deterministic cron, subagents, memory, and plugins.
- `import-dry-run` builds a structured migration report, supports `skip`, `overwrite`, and `rename` conflict policies, extracts non-secret semantic summaries from OpenClaw config/session/cron JSON, and can write `report.json` plus `summary.md`.
- `registry` builds a read-only multi-agent registry from `openclaw.json` plus `/agents/<id>` directories, including provider/model/workspace metadata and local auth/session/model file presence.
- `registry-export` writes the target harness registry state to `state/harness-registry.json` plus `state/harness-registry-receipts.json`, with conflict policy support and no raw secret migration.
- The dry-run planner currently covers config, prompt files, skill directories, agent directories, native cron store, deterministic cron stores, subagent store, memory store, plugin install record, and plugin-state directory.
- `import-execute` safe-copies planned prompt files, skills, agent directories, sessions, cron stores, subagent ledgers, memory snapshots, and plugin records; it skips raw sensitive items by default, omits known auth/secret files inside copied directories unless `--include-sensitive` is set, backs up overwrite targets, and writes `state/import-execute-receipts.json`.
- Runtime execution, SQLite-consistent backup, Docker volume export, credential vault migration, and plugin execution are still pending.
- A shared channel command parser and runtime-intent mapper exists for `/new`, `/think`, `/stop`, `/steer`, `/btw`, `/model`, and `/status`; `/model` covers show/switch model, and `/status` covers global/scoped status requests.
- `skills` builds a skill-first index from source OpenClaw skill directories or an imported harness home, preserves skill metadata/capability flags, writes `skill-index.json`, and can deterministically rank skills for a task turn using query, agent, channel, and workspace hints.
- `turn-plan` builds a runtime-facing dry-run plan for one inbound message: command-vs-agent dispatch, OpenClaw agent routing, session key mapping, provider/model policy, prompt file availability, and selected skills before any model/tool execution.
- `channel-step` builds the shared Telegram/Discord-style channel bridge contract for one inbound DM: command turns produce typed command effects plus outbound reply text, and ordinary messages produce an agent-turn dispatch envelope for the future runtime queue.
- `queue-enqueue` persists channel agent-turn dispatches to `state/runtime-queue/pending.jsonl`, appends receipts to `state/runtime-queue/receipts.jsonl`, and precomputes OpenClaw-compatible transcript/trajectory paths for the future Codex runtime worker.
- `queue-prepare` reads pending runtime queue items, rebuilds turn context from queued source/workspace/session metadata, writes `prompt-bundle.json` plus `prompt.md` under `state/runtime-queue/executions/<queue-id>/`, and records execution receipts without invoking a model.
- `prompt-bundle` consumes an agent turn plan and writes `prompt-bundle.json` plus `prompt.md` containing runtime context, imported prompt file bodies, selected `SKILL.md` bodies, and the inbound message with byte caps.
- `cron-plan` parses OpenClaw native agent-turn cron jobs/state and produces a dry-run dispatch plan with cutover hold safety; it validates agent ids, extracts cron payload text when possible, classifies due `at` jobs, and registers cron expressions for future scheduler evaluation without firing anything.
- `deterministic-cron-plan` parses workspace `tools/cron-runner` and `tools/backup-cron-runner` crontabs, resolves deterministic `jobs/*` scripts, classifies Windows shell compatibility and missing scripts, and preserves `llmAccessAllowed=false` throughout the dry-run plan.
- `subagent-plan` parses `.openclaw/subagents/runs.json`, summarizes queued/running/completed/failed/canceled/unknown runs, holds queued/running work at cutover by default, and only marks them as resume candidates when `--resume-subagents` is explicitly set.

### Phase 0: Foundation

- Initialize git repo.
- Add this assessment and an architecture decision record.
- Create Rust workspace with `core` and `cli`.
- Implement read-only OpenClaw home inventory.
- Implement import-plan generation.
- Verify with `cargo test`.

### Phase 1: Importer

- Add JSON parsing for `openclaw.json` and `sessions.json`.
- Extend registry parsing into a persisted target harness registry with import receipts.
- Extend raw state safe copy execution beyond the current file/directory copy path with SQLite online backup support, reparse/symlink policy, and module include/exclude presets.
- Add Docker source adapter for exporting `/root/.openclaw` safely.
- Add explicit workspace override support for `D:\Warehouse\Research\OpenClaw_WSL`.
- Keep expanding conflict policy, backup-on-overwrite, report redaction, and per-item receipts following the Hermes migrate shape.
- Add SQLite backup strategy notes and checks.
- Add Windows credential vault integration for provider/channel/plugin secret re-entry and best-effort secret import.

### Phase 1.5: Skill-First Substrate

- Import skill directories from workspace, OpenClaw home, and `.agents/skills`.
- Build a skill metadata index and deterministic task matcher.
- Add selected-skill full-body/reference loading for prompt assembly.
- Add skill conflict modes: skip, overwrite with backup, and rename.
- Add skill lint/security checks for scripts, shell snippets, and platform constraints.
- Add agent-managed skill create/patch/archive receipts.

### Phase 2: Runtime MVP

- Add Codex app-server client.
- Add local direct-message CLI or HTTP channel for testing.
- Use the imported multi-agent registry to route direct messages and cron payloads by `agentId`.
- Extend the runtime worker from prepare-only into Codex app-server execution: consume prepared prompt bundles, start/resume sessions, stream events, and persist transcript/trajectory/Codex binding receipts.
- Extend `cron-plan` into a real native scheduler after the Codex adapter and transcript writer exist.
- Extend `deterministic-cron-plan` into a supervised Windows process runner with explicit WSL/Git Bash fallback policy and no model/tool-runtime access.
- Extend `subagent-plan` into a worker queue with per-agent concurrency limits, cancellation, retry policy, and run receipts after the Codex runtime adapter exists.
- Mirror replies into OpenClaw-compatible transcript files.

### Phase 3: Messaging Channels

- Add Telegram channel.
- Add Discord channel.
- Implement channel session key compatibility.
- Route real Telegram/Discord bot events into `channel-step`, preserving the existing shared parser and command effects for `/new`, `/think`, `/stop`, `/steer`, `/btw`, `/model`, and `/status`.
- Persist command effects such as model switch, new session, steering notes, and stop requests into runtime state instead of only returning typed dry-run plans.
- Implement approval UX for shell/tool requests.

### Phase 4: Plugin And Memory Bridge

- Add plugin-host sidecar protocol.
- Bridge memory tools and `openclaw-mem` ContextPack recall.
- Bridge selected hooks: prompt build, tool result persist, message received/sent.

## Existing References Worth Reusing

- `openclaw/openclaw`: data layout, config shape, plugin system, channel behavior, Codex harness split.
- `openai/codex`: app-server protocol, MCP server interface, Rust implementation patterns.
- `nousresearch/hermes-agent`: skill-first procedural memory, native Windows packaging references, and OpenClaw migration safety patterns.
- `phenomenoner/openclaw-mem`: memory sidecar, ContextPack, gateway approach.
- `teloxide/teloxide`: Telegram bot framework for Rust.
- `serenity-rs/serenity` and `serenity-rs/poise`: Discord bot and command framework.
- `twilight-rs/twilight`: lower-level Discord gateway and HTTP crates.
- `64bit/async-openai`: OpenAI-compatible Rust client reference.

## Current Project Decision

The project began as a Rust workspace with no external dependencies. Now that import reports and OpenClaw JSON parsing are in scope, `serde` and `serde_json` are part of the foundation. Additional crates should still be introduced only when the module that needs them is implemented:

- `serde` and `serde_json` for report/config/session parsing and serialization.
- `tokio` for async runtime.
- `tracing` for logs.
- `clap` for CLI.
- `teloxide` for Telegram.
- `serenity`/`poise` or `twilight` for Discord.
- `tokio-tungstenite` or stdio process management for Codex app-server transport.
