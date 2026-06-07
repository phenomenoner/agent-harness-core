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
- Agents: 11 active configured agents
- Sessions: 161 session records reported by status
- Channels: Telegram and Discord enabled
- Memory slot: `openclaw-mem-engine`
- Loaded relevant plugins: `codex`, `openai`, `openrouter`, `telegram`, `discord`, `openclaw-mem`, `openclaw-mem-engine`, `openclaw-context-budget`, `acpx`

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

## Import Strategy

Use a staged import. The first stage is read-only and produces an import plan. Later stages perform copy/transform/resume.

1. Config import
   - Read `openclaw.json`.
   - Preserve provider ids, agent ids, channel configs, plugin entries, plugin slots, and model fallbacks.
   - Move secrets to Windows Credential Manager or an encrypted local key store rather than writing them back to plain text.

2. Workspace import
   - Preserve `AGENTS.md`, `SOUL.md`, `TOOLS.md`, `USER.md`, `IDENTITY.md`, `HEARTBEAT.md`, `BOOTSTRAP.md`.
   - Preserve `skills/<skill>/SKILL.md`.
   - Preserve workspace-local `memory/`, tools, scripts, handoffs, and operational state.

3. Session import
   - Read `/agents/<agent-id>/sessions/sessions.json`.
   - Preserve session transcript files: `*.jsonl`.
   - Preserve trajectories: `*.trajectory.jsonl` and `*.trajectory-path.json`.
   - Preserve Codex binding mirrors: `*.jsonl.codex-app-server.json`.
   - Initial Rust support should expose these as searchable historical context before attempting active native resume.

4. Memory import
   - Preserve `.openclaw/memory/*.md`, `openclaw-mem.sqlite`, `openclaw-mem-observations.jsonl`, `openclaw-mem-episodes.jsonl`, mem-engine DBs, LanceDB data, and graph/vector sidecars.
   - SQLite files should be copied from a stopped gateway or through a backup API to avoid WAL loss.

5. Plugin import
   - Import install records and config, but execute plugins through the sidecar initially.
   - Refresh or rebuild stale plugin registry state instead of trusting stale persisted paths.

## Major Risks

The largest risk is full OpenClaw plugin compatibility. OpenClaw plugins can register providers, channels, CLI backends, agent harnesses, hooks, tools, memory slots, services, and commands. Rebuilding that API natively in Rust before the rest of the harness exists would dominate the project.

The second risk is Codex app-server protocol churn. It is the right integration point, but the harness should pin tested Codex versions and isolate protocol structs behind a narrow adapter.

The third risk is import correctness. Sessions, memory DBs, WAL files, channel queues, plugin state, and Codex app-server mirrors can drift if imported while the old gateway is running.

## Implementation Phases

### Phase 0: Foundation

- Initialize git repo.
- Add this assessment and an architecture decision record.
- Create Rust workspace with `core` and `cli`.
- Implement read-only OpenClaw home inventory.
- Implement import-plan generation.
- Verify with `cargo test`.

### Phase 1: Importer

- Add JSON parsing for `openclaw.json` and `sessions.json`.
- Add copy planner with dry-run receipts.
- Add Docker source adapter for exporting `/root/.openclaw` safely.
- Add SQLite backup strategy notes and checks.

### Phase 2: Runtime MVP

- Add Codex app-server client.
- Add local direct-message CLI or HTTP channel for testing.
- Add prompt assembly from imported workspace files.
- Mirror replies into OpenClaw-compatible transcript files.

### Phase 3: Messaging Channels

- Add Telegram channel.
- Add Discord channel.
- Implement channel session key compatibility.
- Implement approval UX for shell/tool requests.

### Phase 4: Plugin And Memory Bridge

- Add plugin-host sidecar protocol.
- Bridge memory tools and `openclaw-mem` ContextPack recall.
- Bridge selected hooks: prompt build, tool result persist, message received/sent.

## Existing References Worth Reusing

- `openclaw/openclaw`: data layout, config shape, plugin system, channel behavior, Codex harness split.
- `openai/codex`: app-server protocol, MCP server interface, Rust implementation patterns.
- `phenomenoner/openclaw-mem`: memory sidecar, ContextPack, gateway approach.
- `teloxide/teloxide`: Telegram bot framework for Rust.
- `serenity-rs/serenity` and `serenity-rs/poise`: Discord bot and command framework.
- `twilight-rs/twilight`: lower-level Discord gateway and HTTP crates.
- `64bit/async-openai`: OpenAI-compatible Rust client reference.

## Current Project Decision

The project will begin as a Rust workspace with no external dependencies. This keeps the first build reliable on a fresh Windows Rust install. External crates will be introduced when the module that needs them is implemented:

- `tokio` for async runtime.
- `serde` and `serde_json` for config/session parsing.
- `tracing` for logs.
- `clap` for CLI.
- `teloxide` for Telegram.
- `serenity`/`poise` or `twilight` for Discord.
- `tokio-tungstenite` or stdio process management for Codex app-server transport.
