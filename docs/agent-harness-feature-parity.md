# Agent Harness Feature Parity

Date: 2026-06-12

This document compares the Rust Windows Agent Harness against the legacy feature families being replaced from the imported Docker/container deployment. Keep this markdown file and `docs/agent-harness-feature-parity.html` synchronized.

Current activation state after the repo-local harness-home rebase, round3/round3-1 runtime/channel fixes, assistant narration routing, public-facing hygiened export, round3-2 timeout/progress reconciliation, and the OpenRouter/per-agent memory upgrade:

- Readiness before this upgrade: `ready=true`, `passed=58`, `warnings=0`, `failed=0`.
- Harness home: `.agent-harness` under the repo root; `imports/activation-harness` is retained as a pre-rebase backup.
- Runtime queue before this upgrade: `queued=123`, `open=0`, `prepared=123`, `completed=120`. Timeout run-once receipts are terminal for queue selection, status open-item counts, native typing context, and progress delivery state.
- Channel outbox before this upgrade: `pending=0`, `delivered=186`, `retryable=0`, `invalid=0`.
- Channels: Telegram and Discord enabled; Telegram probe ready; Discord gateway probe ready; Discord real inbound evidence present.
- Live loops: one bounded-concurrency runtime loop plus worker, progress delivery, Telegram, Discord outbox, and Discord gateway are run from regenerated scripts. The runtime loop process uses `--runtime-concurrency 12`.
- Runtime response UX: `response.assistantNarrationMode` defaults to `progress_panel`; Codex `phase=commentary` assistant items are stored as `assistant_narration`, rendered as compact `Current step: ...` progress status, and kept out of final channel replies.
- OpenRouter routing: implemented through generated harness-local Codex config using `model_provider = "openrouter"`, `base_url = "https://openrouter.ai/api/v1"`, `env_key = "OPENROUTER_API_KEY"`, and `wire_api = "chat"`. Provider/model parsing now preserves slash-qualified OpenRouter model ids when the provider is explicit.
- Multi-agent isolation: implemented for runtime registry/session routing and memory artifacts. Agent turns write session state under `agents/<agent>/sessions`, while memory prompt/lifecycle/canvas artifacts can write under `state/agents/<agent>/memory` and `agents/<agent>/memory`; the original Unicode agent id is retained in receipts while filesystem path parts are normalized.
- Local verification after this upgrade passed `cargo fmt --all`, `cargo test --workspace` (177 core tests, 16 CLI tests, doctests), `cargo build --workspace`, live `status`, live `enable-check`, and channel outbox plan.

## Summary

The harness is ready for controlled live Telegram/Discord handoff testing and basic agent turns through the Codex backend. The current upgrade also implements the OpenRouter provider path needed for future OpenRouter-backed agents and adds per-agent memory namespaces so `main`, `小小梨`, and other agents can keep independent memory/canvas state.

No feature parity item is left in a vague intermediate state. Harness-owned work is marked `Implemented`. Items that require a user decision, external service contract, live credential, trust policy, or Windows service registration are marked `Blocked` or `Action Required` with the exact next action. Items the user wants but has explicitly chosen to do later are marked `Deferred`.

Runtime loop concurrency currently uses a bounded OS thread scheduler instead of a Tokio runtime. The hot path remains long-running blocking Codex child processes, file leases, JSONL appends, and synchronous HTTP delivery; moving only this layer to Tokio would still require `spawn_blocking` for the runtime work while increasing the migration surface. Tokio becomes a better fit when HTTP clients, queue scans, outbox delivery, and runtime orchestration are converted together to async APIs.

## Feature Matrix

| Feature family | Harness status | Implemented now | Blocker or required action |
|---|---:|---|---|
| State import and migration | Implemented | Imports core files, workspace, registry, channel credentials, memory snapshot, sessions/receipts evidence, plugin catalog inputs, non-secret ops backups, and cutover receipts with redacted receipts. | Action: choose an encrypted/vaulted long-term backup policy if long-lived secret backups are required. |
| Multi-agent registry and isolation | Implemented | Preserves 25 enabled imported agents, per-agent source directories, session indexes, local auth/model hints, per-agent global `/model` and `/think` overrides, per-agent runtime transcript paths, and per-agent memory/canvas namespaces. The upgrade test covers independent `main` and `小小梨` memory roots. | Action: run live smoke across every imported provider/agent as credentials become available. |
| Telegram DM channel | Implemented | Poll loop, allow-list enforcement, typing indicator path, slash command replies, normal-turn queueing, native `sendPhoto`/`sendDocument` attachment delivery, delivery receipts, and offset persistence. | Action: operator sends an allowed-user live Telegram smoke after each deployment window. |
| Discord DM/channel | Implemented | REST outbox, multipart attachment delivery, gateway loop, DM probe, allow-list enforcement, event normalization, reply-context capture, delivery receipts, and live inbound evidence. | Action: operator sends an allowed-user Discord smoke; add reconnect/backoff monitoring if the gateway becomes a long-running unattended service. |
| Chat commands | Implemented | `/status`, `/model`, `/think`, `/new`, `/stop`, `/steer`, `/btw` are parsed and persisted across Telegram/Discord/local state. `/stop` writes a session cancel marker for the active runtime turn. | Action: add provider-native cancel/typing affordances only for backends that expose those APIs. |
| Codex runtime backend | Implemented | Plans/preflights Codex app-server, uses Codex OAuth or provider env keys, starts/resumes sessions, records transcript/trajectory/Codex binding, polls cancel markers, captures token usage when Codex reports it, splits `agentMessage` phases into `assistant_narration` and final assistant replies, isolates runtime workspace, renews an idle JSONL timeout on every app-server event while preserving a hard max-turn timeout, and runs bounded concurrent channel turns through runtime leases. | Action: broaden live cancel/backoff smoke under real provider load. |
| Provider/model routing | Implemented | Imported providers/models are visible; `/model provider/model`, per-agent defaults, explicit-provider slash model ids, and OpenRouter Codex config generation work. OpenRouter preflight requires `OPENROUTER_API_KEY` and writes no secret into config. | Deferred by user decision on 2026-06-12: direct native non-OpenAI-compatible adapters are needed later, but are not a current delivery blocker while OpenRouter is the multi-provider gateway. |
| Prompt files and continuity | Implemented | Imports prompt files; turn planning falls back from runtime cwd to imported workspace when needed; prompt bundle adds per-file role headers for files such as `AGENTS.md`, `SOUL.md`, `TOOLS.md`, `USER.md`, `IDENTITY.md`, `HEARTBEAT.md`, and `BOOTSTRAP.md`; injection ledger avoids repeated stable context in the same session. | Action: add persona-sensitive regression tests for imported agents. |
| Skill-first task guidance | Implemented for runtime selection; blocked for self-modifying learning loop | Imports source skills, syncs bundled harness operation skills, and selects relevant `SKILL.md` bodies at runtime. Command/status turns can correctly select 0 skills. | Blocked until the user approves an agent-created skill propose/patch/archive policy, review boundary, and write locations for Hermes-style learning-loop behavior. |
| Memory inventory and vector recall | Implemented | Detects Qdrant edge, SQLite, memory files, receipts; SQLite vector recall, text recall, service-writeback recall, and prompt-context recall work through the harness OpenClawMem service adapter. LanceDB is hidden unless source config explicitly selects it. | Current `.agent-harness` confirms Qdrant edge is a preserved snapshot, not a live Qdrant service. Action only if upgrading beyond snapshot adapter: provide a runnable remote `openclaw-mem` endpoint, wire contract, credentials, and compatibility fixtures. |
| Memory lifecycle and canvas | Implemented | Post-turn lifecycle adapter records episodes and conservative capture candidates; compact canvas JSON/Markdown exists; `memory-hook` records OpenClaw-compatible before-prompt, agent-end, store-propose, memory-slot, tool-result, and canvas-maintenance receipts; `memory-service-propose/store` and sidecar `openclaw_mem.propose/store` implement reviewed proposals and approved per-agent writeback; memory artifacts can now be scoped per agent. | Action only for future graph/canvas parity beyond the snapshot adapter: supply the `openclaw-mem`/`openclaw-mem-engine` remote service contract and fixtures. |
| Plugin bridge | Implemented for manifest/receipt bridge; blocked for real invocation | Node sidecar resolves manifests/catalog, supports JSON-RPC status/list/probe, `hooks.invoke`, `memory.slot`, and native context-budget behavior. | Blocked until plugin trust policy, allow-list, timeout policy, per-agent/channel permissions, and plugin test fixtures are supplied. |
| Cron/subagent source import | Implemented | Imports native agent-turn cron, extended deterministic crontab/Supercronic-style cron, and subagent ledgers with cutover hold safety; enqueue adapters persist worker jobs instead of direct execution. | Action: live smoke repeated scheduler watermarks, active imported subagent resume/cancel, and cron-to-agent paths. |
| Worker dispatch (cron + subagents) | Implemented | SQLite `WorkerStore`, idempotent enqueue, lease/run/reap/cancel/status, deterministic shell audit, LLM subagent runtime-queue handoff, global/per-agent/per-agent-channel/lane concurrency, optional rate leases, child groups, watchdogs, master wakeups, and artifact pointers. | Action: add cascading timeout/cancel for active child processes, provider backoff profiles, fairness, multi-host store abstraction, and broader live fan-out/watchdog smoke. |
| Operations and observability | Implemented for supervised local operation | Status JSON/text with latest non-idle runtime receipt, readiness checks, JSONL logs, loop heartbeats, supervisor scripts, stop files, compact progress delivery without `assistant_stream` noise, `assistant_narration` current-step progress rendering, terminal timeout reconciliation, progress terminal monotonicity, `worker-loop`, single bounded-concurrency `runtime-loop`, configurable runtime max/idle timeouts, generated direct-start fallback when Task Scheduler entries are absent, generated supervisor log retention, atomic state writes, terminal failure statuses, `jsonl-repair`, `ops-backup`, `ops-cutover-receipt`, and `ops-control`. | Action required for unattended production: approve Task Scheduler registration or a Windows service wrapper, then add restart/stale-heartbeat monitoring and ledger compaction. |
| Credentials and secrets | Implemented for redacted env-file import; blocked for vault migration | Telegram/Discord tokens, OpenRouter key requirements, and memory embedding secrets are represented as environment variables or redacted env files. Local harness keys use `AGENT_HARNESS_*` naming. | Blocked until the user chooses Windows Credential Manager vs encrypted vault and approves migration/rotation for long-lived Telegram, Discord, OpenRouter, and memory keys. |

## Blocked Items and Required User Actions

1. OpenRouter live smoke: set `OPENROUTER_API_KEY`, choose the target model for `小小梨`, and send one controlled agent turn after deployment. The harness-side config generation and route parsing are implemented and unit-tested.
2. Plugin real invocation: approve plugin trust model, allow-list, timeout policy, per-agent/channel permissions, and sample plugin fixtures.
3. Unattended Windows operation: approve either generated Task Scheduler registration or a Windows service wrapper. Current scripts support supervised hidden-process start/stop/status.
4. Vaulted secrets: choose Windows Credential Manager or an encrypted repo-local vault, then approve migration and rotation of long-lived provider/channel/memory keys.
5. External live channel proof: operator must send allowed Telegram/Discord smoke messages; this cannot be completed deterministically from code alone.
6. Optional remote `openclaw-mem` beyond snapshot adapter: provide the runnable service endpoint, API/wire contract, credentials, and fixtures. The current imported artifacts only prove Qdrant edge snapshot, SQLite, JSONL, and engine state.

## Deferred Backlog

1. Native provider adapters beyond Codex/OpenAI/OpenRouter-compatible routing: user confirmed on 2026-06-12 that this is needed, but should be done later. When resumed, provide provider list, SDK/API contract, credentials, model naming rules, cancellation/streaming expectations, and test fixtures.

## Recommended Development Order

### P0: Live proof

1. Done: build `agent-harness.exe`.
2. Done: run `status` and `enable-check` against `.agent-harness`.
3. Done: run offline `channel-run-once` with `tools/agent-fake-codex-app-server`.
4. Done: regenerate supervisor scripts with `agent-harness.exe`, `--source-home`, and `tools/agent-discord-gateway/index.mjs`.
5. Done: restart live loops manually as hidden PowerShell processes when scheduled tasks are not registered.
6. Action: operator sends allowed-user Telegram, Discord, and OpenRouter-backed normal-turn smoke, then records transcript/receipt paths in `docs/activation-readiness-plan.md`.

### P1: Durable Windows operation

1. Done: `supervisor-plan` includes `worker-loop` and stop-file wiring.
2. Done: `ops-control stop|start|status` creates/clears/inspects supervisor stop files and writes receipts.
3. Done: `ops-backup` copies non-secret harness state and writes a manifest.
4. Done: `ops-cutover-receipt` records readiness summary for cutover audit.
5. Done: generated start script falls back to hidden direct `Start-Process` runners when Task Scheduler entries are absent.
6. Done: generated runner scripts retain the newest 20 supervisor logs per component.
7. Done: assistant narration routing keeps final channel replies clean while preserving compact progress status.
8. Action: register generated Task Scheduler tasks or replace them with a Windows service wrapper.
9. Action: add restart policy and stale heartbeat monitor after the unattended service strategy is selected.

### P2: Memory parity

1. Done: keep `openclaw-mem` external and dual-compatible; implement Agent Harness adapters/hooks instead of modifying its internals.
2. Done: `memory-hook` MVP for before-prompt recall, agent-end lifecycle, store-propose receipts, memory-slot receipts, tool-result handoff, and canvas maintenance.
3. Done: per-agent prompt/lifecycle/canvas memory artifact roots for independent multi-agent memory spaces.
4. Done: LanceDB readiness is hidden unless the source explicitly selects LanceDB.
5. Done: OpenClawMem service-adapter CLI and sidecar contract for status, recall, reviewed proposals, approved per-agent store writeback, receipts, and canvas input.
6. Confirmed: current imports use Qdrant edge as a preserved snapshot plus SQLite/JSONL artifacts; no live remote `openclaw-mem` service endpoint/source is active.
7. Action if upgrading beyond the snapshot adapter: provide the remote service wire contract, endpoint, credentials, and fixtures for Qdrant-native/graph parity.

### P3: Plugin parity

1. Done: Node sidecar supports manifest/catalog bridge plus `hooks.invoke` and `memory.slot` receipts.
2. Done: context-budget behavior remains native.
3. Blocked: real plugin tool invocation, hook execution policy, timeout policy, and per-agent/channel permissions require an approved trust model and fixtures.

### P4: Unified worker dispatch, cron, and subagents

1. Done: `WorkerStore` with SQLite MVP storage, idempotent enqueue, lease/reap, status, and cancellation.
2. Done: harness config for global, per-agent/group, per-agent-per-channel, and lane concurrency limits; invalid `global >= group >= channel` settings are capped with a warning.
3. Done: deterministic shell jobs with allow-listed scripts, structured argv/env, capped audit logs, timeout, and retry.
4. Done: LLM subagent/master wakeup jobs enqueue durable runtime turns.
5. Done: native cron, deterministic cron, and imported subagent plans convert into worker enqueue adapters.
6. Done: child groups, rate leases, watchdog jobs, idempotent master wakeups, and bounded artifact pointers.
7. Action: add cascading timeout/cancel for active child processes, provider backoff profiles, repeated scheduler watermarks, fairness, and live worker/cron/subagent smoke.

## Assessment

Harness-owned feature parity work is implemented for the current scope, including OpenRouter provider config generation, multi-agent independent memory namespaces, and the OpenClawMem snapshot/service-adapter bridge. Non-implemented work is either explicitly blocked by plugin trust policy, vault choice, Windows unattended-service approval, optional remote `openclaw-mem` service contract/credentials, or operator live-smoke actions, or explicitly deferred by user decision.
