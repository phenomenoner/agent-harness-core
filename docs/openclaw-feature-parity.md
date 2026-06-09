# Agent Harness vs OpenClaw Feature Parity

Date: 2026-06-09

This document compares the Rust Windows Agent Harness against the main OpenClaw feature families we are replacing from the imported Docker/container deployment. It is based on the current `imports/activation-harness` readiness state and the implementation in this workspace.

Current activation state:

- Readiness: `Ready: yes`, `passed=51`, `warnings=3`, `failed=0`.
- Runtime queue: `queued=15`, `open=0`, `prepared=15`, `completed=15`.
- Channel outbox: `pending=0`, `delivered=41`, `retryable=0`.
- Live loops: runtime, Telegram, Discord outbox, and Discord gateway are running.
- Imported registry: 25 enabled agents, Telegram enabled, Discord enabled, plugin catalog present.
- Memory: Qdrant edge snapshot present, `openclaw-mem.sqlite` present, embedding secret migrated, SQLite vector recall ready, prompt-context recall ready, symbolic canvas written.

## Summary

The harness is usable for live Telegram/Discord handoff testing and basic agent turns through the Codex backend. It can import the main OpenClaw house state, preserve multi-agent registry data, run channel commands, route DMs, assemble prompts without duplicate context injection, run a Codex app-server turn, and write transcripts/receipts/logs.

The largest remaining gaps are not the outer shell. They are the deeper OpenClaw behavior layers: Qdrant-native memory semantics, full openclaw-mem routeAuto/propose/store/canvas parity, plugin hook/tool execution beyond manifest/bridge probing, cron execution, subagent execution, and durable service supervision.

## Feature Matrix

| OpenClaw feature family | Harness status | Implemented now | Remaining gap |
|---|---:|---|---|
| State import and migration | Implemented | Imports core files, workspace, registry, channel credentials, memory snapshot, sessions/receipts evidence, plugin catalog inputs. Uses redacted receipts and safe secret handling. | Add explicit backup/export and cutover receipt command for operator handoff. |
| Multi-agent registry | Implemented | Preserves 25 enabled imported agents, per-agent source directories, session indexes, local auth/model hints, per-agent global `/model` and `/think` overrides. | Agent-specific runtime backend parity for every imported provider/agent still needs more live smoke. |
| Telegram DM channel | Implemented | Poll loop, allow-list enforcement, typing indicator path, slash command replies, normal-turn queueing, delivery receipts, offset persistence. | Continue live DM soak tests and retry/backoff hardening. |
| Discord DM channel | Partially implemented | REST outbox, gateway loop, DM probe, HTTP poll fallback, allow-list enforcement, event normalization, delivery receipts. | Needs an allowed-user real Discord inbound DM to clear readiness warning; add reconnect/backoff and richer live gateway monitoring. |
| Chat commands | Implemented / partial | `/status`, `/model`, `/think`, `/new`, `/stop`, `/steer`, `/btw` are parsed and persisted across Telegram/Discord/local channel state. `/model` and `/think --global` are per-agent. | `/stop` still needs stronger cancellation for an already-running Codex model turn/process. |
| Codex runtime backend | Implemented | Plans/preflights Codex app-server, uses Codex OAuth, starts/resumes sessions, records transcript/trajectory/Codex binding, isolates runtime workspace from this project workspace. | Harden long-running cancellation, retry policy, and provider-specific app-server behavior under load. |
| Provider/model routing | Partially implemented | Imported providers/models are visible; `/model provider/model` can switch session model; per-agent default override exists. | Native provider adapters beyond Codex/OpenAI/OpenRouter-compatible routing need execution parity and live validation. |
| Prompt files and session continuity | Implemented | Imports `SOUL.md`, `AGENTS.md`, related prompt files; prompt bundle uses a sandwich-like assembly and prompt-injection ledger so repeated turns do not append duplicate stable context. | Keep validating with multi-turn live sessions; add regression tests for every imported persona-sensitive agent. |
| Skill-first task guidance | Partially implemented | Imports source skills and bundled harness operation skills; runtime skill index and selection exist; harness operation skill can be updated with versions. | Hermes-like automatic skill creation/upgrade after turns is not implemented. |
| Memory import inventory | Implemented | Detects Qdrant edge, SQLite, LanceDB absence, memory files, memory receipts; exposes in `status` and `enable-check`. | Add stronger schema/version diagnostics for memory DB and vector backend drift. |
| Memory vector recall | Partially implemented | Embedding key migrated to `secrets/memory-credentials.env`; query embedding works with `text-embedding-3-small`; SQLite vector tables search observations/docs/episodes; prompt context uses vector recall first. | Qdrant edge is currently preserved and reported, not raw-read. Add Qdrant-native service/sidecar adapter and LanceDB fallback. |
| Memory lifecycle and auto-capture | Partially implemented | Post-turn lifecycle adapter records episodes and conservative auto-capture candidates; prompt context and vector recall receipts are ready. | Current activation still lacks a live successful turn lifecycle receipt; full routeAuto/autoRecall/propose/store semantics are not implemented. |
| Symbolic canvas | Partially implemented | Minimal `memory-canvas-run` writes compact JSON/Markdown canvas from imported episodes/candidates. | Full OpenClaw/openclaw-mem canvas semantics and graph/canvas update workflow still need parity. |
| OpenClaw plugins | Partially implemented | Node sidecar resolves manifests, writes catalog/receipts, supports `sidecar.status`, `plugins.list`, `tools.probe`, `tools.list`; native adapter handles context-budget class. | Plugin-specific tool execution, hooks, memory slots, and runtime plugin API behavior are not complete. |
| Native agent-turn cron | Planned / dry-run | Imports cron jobs/state and has `cron-plan`; keeps agent-turn cron held until explicit resume. | Implement scheduler execution, enqueue real agent turns, run receipts, catch-up watermark. |
| Deterministic cron | Planned / dry-run | Imports deterministic crontab/job evidence and has `deterministic-cron-plan`; keeps LLM path disabled. | Implement deterministic runner with Windows-safe command policy and logs. |
| Subagents | Planned / dry-run | Imports subagent ledger and has planning/resume classification. | Implement subagent worker execution, handoff, cancellation, and session/tool isolation. |
| Operations and observability | Partially implemented | Status JSON/text, readiness checks, harness JSONL logs, loop heartbeats, supervisor scripts, stop files. | Scheduled tasks are generated but not registered in this environment; add installer smoke, monitor endpoint, log rotation, backup/export. |
| Credentials and secrets | Partially implemented | Telegram/Discord tokens imported; memory embedding key migrated separately from Codex auth; receipts redact sensitive values. | Move long-lived secrets to Windows Credential Manager or encrypted vault; browser/service login state remains best-effort. |

## Recommended Development Order

### P0: Finish live handoff proof

1. Send allowed-user Telegram and Discord DMs and confirm normal turns return to both channels.
2. Confirm a successful live agent turn writes `state/memory/lifecycle-receipts.jsonl`.
3. Clear the Discord real inbound warning with an allowed-user `MESSAGE_CREATE`.
4. Capture the exact smoke transcript/receipt set in `docs/activation-readiness-plan.md`.

### P1: Make it durable as a Windows service

1. Register generated Task Scheduler tasks or replace them with a dedicated Windows service wrapper.
2. Add process restart policy, stale heartbeat monitor, and a single stop/start/status command.
3. Add log rotation and compact operator status output.
4. Add backup/export plus explicit cutover receipt.

### P2: Close memory parity

1. Add Qdrant-native recall through a sidecar/service or supported snapshot API.
2. Add LanceDB fallback if the source snapshot provides it.
3. Implement routeAuto/autoRecall policy matching with model-aware context budget.
4. Implement propose/store workflow with review gates, idempotency, and no raw secret capture.
5. Upgrade symbolic canvas from compact summary to OpenClaw/openclaw-mem graph/canvas behavior.

### P3: Close plugin parity

1. Extend the Node sidecar from manifest/catalog bridge to real tool invocation.
2. Implement plugin hooks around prompt build, tool result, agent end, and memory slots.
3. Prioritize `openclaw-mem`, `openclaw-mem-engine`, and context-budget behavior.
4. Add plugin execution receipts, timeout policy, and per-agent/channel permissions.

### P4: Cron and subagent execution

1. Implement native agent-turn cron scheduler with cutover watermark.
2. Implement deterministic cron runner with a strict no-LLM path.
3. Implement subagent worker execution and resume/cancel semantics.
4. Add live smoke tests for cron-to-agent and parent-to-subagent task flows.

### P5: Broaden provider/runtime parity

1. Validate OpenRouter-compatible models under real live turns.
2. Add provider-specific execution adapters where Codex app-server is not sufficient.
3. Add per-agent credential routing and provider health diagnostics.
4. Add model/thinking policy compatibility tests for every enabled imported agent.

## Current Risks

- Discord inbound is connected but still needs a real allowed-user DM receipt.
- Memory currently works through SQLite vector recall, not Qdrant-native recall.
- Plugin support is catalog/bridge-first, not full hook/tool execution.
- Generated Task Scheduler scripts exist, but tasks were not registered in this environment; current loops were manually launched as hidden PowerShell processes.
- `/stop` is command-state aware, but hard cancellation of an already-running Codex turn needs more work.

## Practical Next Step

Run live Telegram and Discord normal-turn smoke now. If both reply and memory lifecycle writes a receipt, the next engineering sprint should focus on durable Windows supervision, then Qdrant-native memory and plugin hook parity.
