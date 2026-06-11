# Agent Harness Core

Agent Harness Core is a Rust workspace for running channel-backed agent turns through a Codex app-server runtime. It provides the core state, queue, prompt assembly, channel ingress/outbox, progress, supervisor, and worker-dispatch pieces needed to operate a local agent harness without committing runtime state or credentials.

The project is designed for Windows-first local operation, with most core logic covered by portable Rust tests.

## Workspace

- `crates/agent-harness-core`: core library for registry import, channel state, prompt bundles, runtime queueing, Codex app-server orchestration, memory/plugin adapter receipts, workers, status, readiness, and supervisor script generation.
- `crates/agent-harness-cli`: CLI surface for operator commands, channel adapters, runtime loops, worker dispatch, supervisor planning, and smoke checks.
- `tools/agent-discord-gateway`: Node Discord Gateway wrapper used by the CLI.
- `tools/agent-fake-codex-app-server`: local fake app-server for offline runtime tests.
- `tools/agent-plugin-sidecar`: minimal Node sidecar for legacy plugin manifest/catalog probing.

## Capabilities

- Import and inspect a legacy agent source home without copying secrets by default.
- Build a multi-agent registry and channel command state.
- Queue Telegram/Discord/local channel turns into durable runtime work.
- Assemble prompt bundles with explicit prompt-file role headers and dynamic skill selection.
- Run prepared turns through `codex app-server`, record transcript/trajectory/binding files, split assistant narration from final replies, and capture usage when available.
- Deliver compact progress messages while suppressing low-value assistant stream previews and routing assistant narration to an editable current-step status.
- Treat timed-out parent runtime turns as terminal for queue selection, status open-item counts, native typing context, and progress delivery state; retries should be represented by new queue ids.
- Keep progress delivery terminal-monotonic so late stray events for a completed/failed parent turn do not turn the status panel back into working.
- Run a bounded-concurrency `runtime-loop` with per-item leases and adaptive Codex JSONL idle timeout renewal.
- Send Telegram/Discord outbox messages and native attachments from structured outbox records.
- Generate Windows supervisor scripts with stop files, direct-start fallback, and per-component log retention.
- Use a durable SQLite worker store for deterministic shell jobs, LLM subagent handoff, watchdogs, and master wakeups.

## Requirements

- Rust toolchain compatible with the workspace `rust-version`.
- Windows PowerShell for Windows supervisor scripts and the bundled Windows smoke paths.
- Node.js for the Discord Gateway and plugin sidecar tools.
- Codex CLI with `app-server` support for live model-backed runtime turns.

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

## Runtime Timeouts

Codex runtime commands support two timeout layers:

- `--timeout-ms`: hard maximum runtime for a turn.
- `--idle-timeout-ms`: JSONL inactivity timeout renewed every time the app-server emits a valid JSONL event.

Supervisor generation exposes the corresponding runtime-loop defaults through `--runtime-timeout-ms` and `--runtime-idle-timeout-ms`.

A `timeout` run-once receipt closes the parent queue id for status, typing, progress delivery, and automatic queue selection. Long-running jobs that should outlive a chat turn should be moved into a managed worker/background-job contract with their own accepted, heartbeat, status, and completion receipts instead of relying on the parent Codex turn to stay open.

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

## Secrets And State

Do not commit harness state or credentials. Runtime data belongs under a local harness home such as `.agent-harness/`, which is ignored by this repository. Channel tokens, provider keys, memory credentials, logs, receipts, transcripts, and imported source snapshots are operator-local data.

Public exports should include source, public docs, and tool wrappers only. Keep debug notes, review files, local harness homes, generated media, credentials, receipts, transcripts, and imported private source snapshots out of public commits.

## License

Licensed under either of:

- Apache License, Version 2.0
- MIT license

at your option.
