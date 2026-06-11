# Agent Harness Core

Agent Harness Core is a Rust workspace for running channel-backed agent turns through a Codex app-server runtime. It provides the core state, queue, prompt assembly, channel ingress/outbox, progress, supervisor, provider routing, memory/plugin adapter receipts, and worker-dispatch pieces needed to operate a local agent harness without committing runtime state or credentials.

The project is designed for Windows-first local operation, with most core logic covered by portable Rust tests.

## Workspace

- `crates/agent-harness-core`: core library for registry import, channel state, prompt bundles, runtime queueing, Codex app-server orchestration, provider routing, memory/plugin adapter receipts, workers, status, readiness, and supervisor script generation.
- `crates/agent-harness-cli`: CLI surface for operator commands, channel adapters, runtime loops, worker dispatch, supervisor planning, memory canvas maintenance, and smoke checks.
- `tools/agent-discord-gateway`: Node Discord Gateway wrapper used by the CLI.
- `tools/agent-fake-codex-app-server`: local fake app-server for offline runtime tests.
- `tools/agent-plugin-sidecar`: minimal Node sidecar for legacy plugin manifest/catalog probing.

## Capabilities

- Import and inspect a legacy agent source home without copying secrets by default.
- Build a multi-agent registry and channel command state.
- Queue Telegram/Discord/local channel turns into durable runtime work.
- Assemble prompt bundles with explicit prompt-file role headers and dynamic skill selection.
- Route model selections through imported defaults, per-agent overrides, `/model provider/model`, and explicit provider/model config.
- Generate harness-local Codex provider config for OpenRouter-compatible routes, using `OPENROUTER_API_KEY` at preflight without writing the secret to disk.
- Preserve slash-qualified model ids such as `anthropic/claude-sonnet-4` when a provider is explicitly configured.
- Run prepared turns through `codex app-server`, record transcript/trajectory/binding files, split assistant narration from final replies, and capture usage when available.
- Deliver compact progress messages while suppressing low-value assistant stream previews and routing assistant narration to an editable current-step status.
- Treat timed-out parent runtime turns as terminal for queue selection, status open-item counts, native typing context, and progress delivery state; retries should be represented by new queue ids.
- Keep progress delivery terminal-monotonic so late stray events for a completed/failed parent turn do not turn the status panel back into working.
- Run a bounded-concurrency `runtime-loop` with per-item leases and adaptive Codex JSONL idle timeout renewal.
- Send Telegram/Discord outbox messages and native attachments from structured outbox records.
- Generate Windows supervisor scripts with stop files, direct-start fallback, and per-component log retention.
- Use a durable SQLite worker store for deterministic shell jobs, LLM subagent handoff, watchdogs, and master wakeups.
- Keep agent-scoped prompt context, lifecycle, capture-candidate, and symbolic-canvas memory artifacts in independent per-agent namespaces while retaining legacy global paths for no-agent calls.

## Requirements

- Rust toolchain compatible with the workspace `rust-version`.
- Windows PowerShell for Windows supervisor scripts and the bundled Windows smoke paths.
- Node.js for the Discord Gateway and plugin sidecar tools.
- Codex CLI with `app-server` support for live model-backed runtime turns.
- `OPENROUTER_API_KEY` only when running an OpenRouter-backed route.

## Quick Start

```powershell
cargo test
cargo run -p agent-harness-cli -- help
cargo run -p agent-harness-cli -- doctor --source-home C:\path\to\source
cargo run -p agent-harness-cli -- import-dry-run --source-home C:\path\to\source --target-home C:\path\to\.agent-harness --output imports\dry-run
cargo run -p agent-harness-cli -- status --harness-home C:\path\to\.agent-harness --json
```

Offline runtime smoke without a model request:

```powershell
cargo run -p agent-harness-cli -- channel-run-once --source-home C:\path\to\source --harness-home C:\path\to\.agent-harness --platform telegram --channel-id dm-1 --user-id user-1 --agent main --message "smoke test" --codex-exe tools\agent-fake-codex-app-server\fake-codex-app-server.cmd
```

Generate supervisor scripts:

```powershell
cargo run -p agent-harness-cli -- supervisor-plan --source-home C:\path\to\source --harness-home C:\path\to\.agent-harness --harness-cli C:\path\to\agent-harness.exe --codex-exe C:\path\to\codex.cmd --agent main --runtime-workers 3
```

Plan an OpenRouter-backed runtime route:

```powershell
cargo run -p agent-harness-cli -- channel-apply --source-home C:\path\to\source --target-home C:\path\to\.agent-harness --platform telegram --channel-id dm-1 --user-id user-1 --agent main --message "/model openrouter/anthropic/claude-sonnet-4"
```

Run agent-scoped symbolic canvas maintenance:

```powershell
cargo run -p agent-harness-cli -- memory-canvas-run --target-home C:\path\to\.agent-harness --agent main
```

## Runtime Timeouts

Codex runtime commands support two timeout layers:

- `--timeout-ms`: hard maximum runtime for a turn.
- `--idle-timeout-ms`: JSONL inactivity timeout renewed every time the app-server emits a valid JSONL event.

Supervisor generation exposes the corresponding runtime-loop defaults through `--runtime-timeout-ms` and `--runtime-idle-timeout-ms`.

A `timeout` run-once receipt closes the parent queue id for status, typing, progress delivery, and automatic queue selection. Long-running jobs that should outlive a chat turn should be moved into a managed worker/background-job contract with their own accepted, heartbeat, status, and completion receipts instead of relying on the parent Codex turn to stay open.

## Provider Routing

Provider/model routing is intentionally config-driven:

- Imported provider/model defaults are preserved in the registry.
- Channel commands can switch model route with `/model provider/model`.
- Agent config with explicit `provider` plus slash-qualified `model` keeps the model string intact.
- OpenRouter routes generate a harness-local Codex config stanza for `model_provider = "openrouter"` with `wire_api = "chat"`.
- Secrets stay outside committed config. OpenRouter uses `OPENROUTER_API_KEY` at runtime/preflight.

Native provider adapters beyond Codex/OpenAI/OpenRouter-compatible routing require a provider-specific SDK/API contract, credential model, streaming/cancellation semantics, and fixtures.

## Multi-Agent Memory

When an agent id is present, memory lifecycle and canvas artifacts are scoped under agent-specific roots. This keeps independent agents from sharing prompt-context receipts, capture candidates, episodes, and canvas state by accident. Receipts retain the original agent id, while filesystem path parts are normalized for portable local paths.

Legacy no-agent calls continue to use the original global memory paths so existing operator workflows keep working.

## Assistant Narration

Agent Harness can route intermediate assistant narration separately from final channel replies:

```json
{
  "response": {
    "assistantNarrationMode": "progress_panel",
    "assistantNarrationMaxChars": 500,
    "assistantNarrationProgressMinUpdateMs": 2500,
    "assistantNarrationFinalPrefix": "Work log"
  }
}
```

Supported modes are:

- `progress_panel` (default): show compact narration as the latest progress step and keep final replies focused on the final answer.
- `inline_preface`: prefix the final reply with a compact work log for debugging.
- `off`: preserve runtime artifacts but do not surface narration in progress or final replies.

## Validation

The current public export was validated with:

- `cargo fmt --all`
- `cargo test --workspace` (core, CLI, and doctests)
- `cargo build --workspace`
- local live `status`, `enable-check`, and channel outbox plan in the private operator environment

## Secrets And State

Do not commit harness state or credentials. Runtime data belongs under a local harness home such as `.agent-harness/`, which is ignored by this repository. Channel tokens, provider keys, memory credentials, logs, receipts, transcripts, and imported source snapshots are operator-local data.

Public exports should include source, public docs, and tool wrappers only. Keep debug notes, review files, local harness homes, generated media, credentials, receipts, transcripts, and imported private source snapshots out of public commits.

## License

Licensed under either of:

- Apache License, Version 2.0
- MIT license

at your option.
