# Activation Readiness Plan

Date: 2026-06-08

This is the working checklist for turning the Rust Windows OpenClaw harness from a local core runtime into the active replacement for the Docker OpenClaw gateway.

## Activation Target

The target state is:

- Docker OpenClaw gateway can be stopped, not deleted.
- Rust harness can receive Telegram/Discord DM input.
- Slash commands work consistently across Telegram/Discord.
- Ordinary DM input can route to the right imported agent/session, run Codex through app-server, write transcript/trajectory/Codex binding files, and deliver the assistant reply.
- Imported memory, cron, subagent, plugin, and historical session state remains available for the next implementation lanes, even when not all adapters are active yet.

## Hard Gates

These must pass before cutover.

1. State import gate
   - Run `import-dry-run`.
   - Review conflicts, missing paths, sensitive skips, and reparse/symlink warnings.
   - Run `import-execute` with explicit conflict policy.
   - Confirm `state/import-execute-receipts.json` exists.

2. Registry gate
   - Run `registry-export`.
   - Confirm `state/harness-registry.json` exists.
   - Confirm at least one enabled agent.
   - Confirm configured Telegram/Discord channels match intended activation surface.

3. Skill gate
   - Run `harness-skills-sync`.
   - Confirm `skills/.openclaw-harness-builtins.json` exists.
   - Run `skills --harness-home ... --query "<task>"` and confirm imported plus bundled skills are visible.

4. Credential gate
   - Prefer Codex OAuth for Codex models.
   - Confirm Codex auth through local `CODEX_HOME`, `%USERPROFILE%\.codex\auth.json`, or `%USERPROFILE%\.codex\auth.toml`.
   - Confirm the harness uses a spawnable Codex CLI binary. The Codex Desktop MSIX resource path may resolve on `PATH` but fail to spawn with Windows `os error 5`; use a standalone release or a local npm install such as `.tools/codex-cli/node_modules/.bin/codex.cmd`.
   - Run `channel-credentials-export --include-sensitive` when migrating from an existing OpenClaw home. It writes Telegram/Discord tokens and known channel/user/guild IDs into `secrets/channel-credentials.env` and redacted receipts into `secrets/channel-credentials-receipts.json`.
   - Confirm `TELEGRAM_BOT_TOKEN` is present either in process env or `secrets/channel-credentials.env` when Telegram is enabled.
   - Confirm `DISCORD_BOT_TOKEN` is present either in process env or `secrets/channel-credentials.env` when Discord is enabled.
   - Confirm `OPENROUTER_API_KEY` only when OpenRouter providers are active.
   - Do not rely on an imported OpenClaw embedding-only `OPENAI_API_KEY` for Codex agent turns.

5. Runtime gate
   - Run `channel-receive` for a normal DM.
   - Prefer `channel-run-once` for one-message end-to-end smoke.
   - Run `queue-prepare`, `codex-plan`, `codex-preflight`, and `codex-launch-probe` before the first real model run.
   - Confirm `enable-check` reports `codex-runtime-launch-probe` as pass.
   - Run `runtime-run-once` with the intended Codex executable.
   - Confirm `state/runtime-queue/run-once-last.json`.
   - Confirm `state/runtime-queue/run-once-receipts.jsonl`.
   - Run `runtime-loop --stop-when-idle --iterations 1` for idle/drain smoke, or `runtime-loop --iterations 0` only under an operator/supervisor after handoff.
   - Confirm `state/runtime-queue/loop-last.json`.
   - Confirm `enable-check` reports `runtime-loop` as pass.
   - Confirm transcript and trajectory files under `agents/<agent-id>/sessions`.
   - Confirm raw Codex stdout/stderr logs under the execution directory.

6. Channel delivery gate
   - Run `channel-outbox-plan`.
   - Deliver the pending reply through the target adapter or manual test harness.
   - Run `channel-delivery-record --status delivered`.
   - Confirm future `channel-outbox-plan` skips delivered messages and retries failed messages.
   - For Telegram smoke, set or import `TELEGRAM_BOT_TOKEN` and run `telegram-poll-once` against a controlled DM; confirm command replies and agent replies are delivered by the Bot API.
   - For Telegram handoff rehearsal, run `telegram-loop --iterations 0` only after confirming the old Docker gateway is not also consuming Telegram updates.
   - For Discord outbound smoke, set or import `DISCORD_BOT_TOKEN`, create a Discord outbox item with `channel-run-once --platform discord ...`, then run `discord-outbox-send-once`.
   - For Discord inbound handoff rehearsal, run `discord-gateway-probe`, then run `discord-gateway-loop` only after confirming the old Docker gateway is not also connected.

7. Logging gate
   - Run `enable-check`.
   - Run `status --json`.
   - Confirm `state/logs/harness.jsonl` is writable.
   - Confirm logs include activation, Telegram poll-once or loop, Discord outbox send, channel receive, runtime run-once, runtime loop, Codex run, completion, and delivery events.
   - Confirm `enable-check` reports `telegram-offset`, `telegram-poll-log`, and `discord-send-log` as pass after adapter smoke tests.
   - Confirm `status` reports runtime `openItems=0` and outbox `pending=0` before live adapter handoff.

## Smoke Gates

These should be run before stopping the Docker gateway.

1. Command smoke
   - Telegram DM: `/status`, `/model`, `/new`, `/think`, `/steer`, `/btw`, `/stop`.
   - Discord DM: same command set.
   - Confirm command replies land in `state/channels/outbox.jsonl`.
   - Confirm command state updates under `state/channels/<platform>/<channel-id>/<user-id>/state.json`.

2. Agent turn smoke
   - Telegram DM ordinary message to `main`.
   - Discord DM ordinary message to `main`.
   - Confirm `channel-run-once` or `runtime-run-once` writes fresh agent replies and delivery receipts.
   - Use `--codex-exe` with the standalone/local Codex CLI that passed `codex-launch-probe`, not the Codex Desktop MSIX resource path.

3. Prompt continuity smoke
   - Send two ordinary messages in the same session.
   - Confirm the first prompt bundle includes unchanged prompt files/skills.
   - Confirm the second prompt bundle reuses them through `state/prompt-injection-ledgers/<agent>/<session>.json`.
   - Confirm Codex system prompt/tool schemas are not duplicated by the harness.

4. Multi-agent smoke
   - Route at least one message to a non-main imported agent.
   - Confirm session path is under `agents/<agent-id>/sessions`.
   - Confirm provider/model policy follows imported registry and channel `/model` overrides.

5. Cron dry-run smoke
   - Run `cron-plan`.
   - Confirm agent-turn cron remains held until explicit resume.
   - Run `deterministic-cron-plan`.
   - Confirm `llmAccessAllowed=false`.

6. Historical state smoke
   - Confirm imported session indexes and transcript files are present.
   - Confirm memory files/databases are imported or explicitly documented as unavailable.
   - Confirm `memory/qdrant-edge` is present when Qdrant edge is the active OpenClaw memory backend.
   - Confirm `openclaw-mem.sqlite` and memory JSONL files are present as snapshot/audit sources.
   - Treat LanceDB as backup/optional unless the active config points to LanceDB.
   - Confirm subagent ledgers are held by default.

## Remaining Development To Formal Activation

1. Real Telegram adapter
   - Done for smoke and operator-run handoff: `telegram-poll-once` receives Telegram text updates, normalizes them into `channel-run-once`, delivers pending replies through Telegram Bot API `sendMessage`, records delivery receipts, stores update offsets, and writes a poll summary log. `telegram-loop` repeats the same path with idle sleep and a consecutive-error threshold.
   - Health/status CLI summary is available through `status`.
   - Still required for formal service activation: Windows service or scheduled-task install path, graceful shutdown, token source hardening, and production retry policy.

2. Real Discord adapter
   - Done for outbound smoke: `discord-outbox-send-once` sends pending Discord outbox messages through Discord REST, records delivery receipts, and writes a delivery summary log.
   - Done for inbound normalization smoke: `discord-event-run-once` accepts one Discord Gateway `MESSAGE_CREATE` event from `--event-file` or `--event-json`, dedupes by message id, normalizes text into `channel-run-once`, writes Discord event receipts, and logs `discord.event-run-once`.
   - Still required for formal Discord activation: WebSocket gateway loop that receives Discord DM events and feeds them into `discord-event-run-once` semantics.
   - Add gateway heartbeat/reconnect and live process health reporting beyond the CLI `status` summary.

3. Worker loop
   - Done for CLI handoff/smoke: `runtime-loop` wraps `runtime-run-once`, drains queued work, treats already recorded completions as idle for loop control, supports finite or infinite `--iterations`, sleeps with `--idle-ms`, exits with `--stop-when-idle`, writes `state/runtime-queue/loop-last.json`, and logs `runtime.loop-stopped` plus `runtime.loop-error`.
   - Still required for formal service activation: Windows service/scheduled-task wrapper, graceful shutdown, process supervisor health integration, stop/cancel handling for `/stop`, and richer retry/backoff policy.

4. Scheduler execution
   - Native OpenClaw cron scheduler that can enqueue agent turns.
   - Deterministic cron runner with no model path.
   - Cutover watermark to avoid catch-up storms.

5. Memory adapter
   - Imported memory file/database inventory check.
   - Qdrant edge import/read adapter first, because current OpenClaw uses Qdrant edge as the primary backend.
   - LanceDB import/read support as backup/fallback, not as the first activation blocker.
   - Optional openclaw-mem gateway adapter when explicitly enabled.
   - Memory pack/search/propose integration into prompt assembly.

6. Plugin sidecar
   - Run `plugin-sidecar-probe` and confirm `enable-check` reports `plugin-sidecar-probe` as pass.
   - Run `plugin-sidecar-call --method sidecar.status`, `plugin-sidecar-call --method plugins.list`, and `plugin-sidecar-call --method tools.probe`; confirm `enable-check` reports `plugin-sidecar`, `plugin-sidecar-probe`, and `plugin-sidecar-bridge` as pass.
   - Done for activation catalog: the Node sidecar resolves imported plugin manifests, writes `state/plugin-sidecar/catalog.json`, writes execution receipts, and exposes manifest-derived tools over `tools.list`.
   - `openclaw-context-budget` is handled as a native prompt-budget adapter by the harness instead of requiring Node plugin source.
   - Still required for broader plugin parity: plugin-specific executor adapters for tool/hook/memory slot invocation beyond manifest catalog and bridge receipts.
   - Health check is surfaced in `enable-check`.

7. Operations
   - Windows service or scheduled task install path.
   - Done for CLI health/status: `status` summarizes readiness, runtime queue, channel outbox, memory, plugins, and logs; `--json` is monitor-friendly.
   - Still required: process supervisor health endpoint or scheduled-task monitor integration.
   - Backup/export command.
   - Explicit cutover command that records operator intent.

## Current Activation Snapshot

As of 2026-06-08 local verification:

- Imported activation harness: `imports/activation-harness`.
- Qdrant edge is present and passes as the primary memory backend.
- LanceDB is absent from the filtered activation snapshot and is treated as backup/optional.
- `openclaw-mem.sqlite` is present as a snapshot/audit source.
- Offline `/status` channel smoke passes against the imported registry: 25 enabled agents, 2 providers, 13 plugins, Telegram and Discord enabled.
- Runtime queue prepare, Codex plan, Codex preflight, and Codex launch probe pass when using workspace-local `@openai/codex` via `.tools/codex-cli/node_modules/.bin/codex.cmd`.
- Plugin sidecar probe passes. `openclaw-context-budget` is classified as a native adapter, leaving 5 sidecar-required plugins. `tools.probe` resolves all sidecar-required plugin manifests from local source roots, writes `state/plugin-sidecar/catalog.json`, reports 2 manifest-derived tools, and makes `enable-check` pass `plugin-sidecar`.
- Discord Gateway `MESSAGE_CREATE` event normalizer smoke passes for `/status`, including duplicate-message skip by Discord message id.
- `channel-credentials-export --include-sensitive` imported Telegram/Discord bot tokens plus known allow-list/guild/channel/chat IDs from the local OpenClaw snapshot into `imports/activation-harness/secrets/channel-credentials.env`; readiness sees both token gates as pass.
- `discord-gateway-probe` passes with the imported Discord token and Node 24 global WebSocket support.
- `discord-outbox-send-once` passes with an empty pending outbox, writes `discord.outbox-send-once`, and does not send any message when `pending=0`.
- The Codex Desktop MSIX `codex.exe` path is not spawnable from this harness environment and should not be used for service runtime.
- Offline normal-turn smoke passes through `channel-run-once` with `tools/openclaw-fake-codex-app-server/fake-codex-app-server.cmd`, producing runtime-run-once, Codex run, Codex completion, transcript, outbox, delivery receipt, and operational log evidence without a model request or channel send.
- Runtime loop idle/drain smoke passes with `runtime-loop --stop-when-idle` and writes `state/runtime-queue/loop-last.json` without a model request when no pending queue items remain.
- `status` reports `queued=2 open=0 prepared=2 completed=2`, outbox `pending=0 delivered=4`, Qdrant edge primary memory present, plugin catalog ready with 2 manifest-derived tools, and operational log event coverage for offline runtime/delivery smoke.
- `enable-check` currently reports `Ready: yes` with `passed=29 warnings=3 failed=0`; `runtime-loop` is a pass. Remaining warnings are live operator smoke evidence for Telegram poll/offset and optional LanceDB backup.

## Verification Commands

```powershell
cargo fmt --all
cargo test --workspace --quiet
cargo clippy --workspace --all-targets -- -D warnings
cargo run -p openclaw-harness-cli -- help
cargo run -p openclaw-harness-cli -- channel-credentials-export --openclaw-home C:\path\to\.openclaw --harness-home C:\path\to\.openclaw-harness --include-sensitive
cargo run -p openclaw-harness-cli -- enable-check --harness-home C:\path\to\.openclaw-harness
cargo run -p openclaw-harness-cli -- status --harness-home C:\path\to\.openclaw-harness --json
cargo run -p openclaw-harness-cli -- channel-run-once --harness-home C:\path\to\.openclaw-harness --openclaw-home C:\path\to\.openclaw --platform telegram --channel-id smoke --user-id operator --message /status
cargo run -p openclaw-harness-cli -- channel-run-once --harness-home C:\path\to\.openclaw-harness --openclaw-home C:\path\to\.openclaw --platform telegram --channel-id offline-runtime-smoke --user-id operator --message "offline runtime smoke" --agent main --codex-exe tools\openclaw-fake-codex-app-server\fake-codex-app-server.cmd --timeout-ms 5000
cargo run -p openclaw-harness-cli -- runtime-loop --harness-home C:\path\to\.openclaw-harness --codex-exe tools\openclaw-fake-codex-app-server\fake-codex-app-server.cmd --iterations 1 --idle-ms 1 --stop-when-idle
cargo run -p openclaw-harness-cli -- codex-launch-probe --harness-home C:\path\to\.openclaw-harness --execution-dir C:\path\to\prepared-execution --startup-probe-ms 750
cargo run -p openclaw-harness-cli -- plugin-sidecar-probe --harness-home C:\path\to\.openclaw-harness
cargo run -p openclaw-harness-cli -- plugin-sidecar-call --harness-home C:\path\to\.openclaw-harness --method sidecar.status
cargo run -p openclaw-harness-cli -- plugin-sidecar-call --harness-home C:\path\to\.openclaw-harness --method plugins.list
$env:OPENCLAW_PLUGIN_SOURCE_ROOTS='C:\path\to\openclaw-src\extensions;C:\path\to\openclaw-mem-src\extensions'
cargo run -p openclaw-harness-cli -- plugin-sidecar-call --harness-home C:\path\to\.openclaw-harness --method tools.probe
cargo run -p openclaw-harness-cli -- discord-event-run-once --harness-home C:\path\to\.openclaw-harness --openclaw-home C:\path\to\.openclaw --event-file C:\path\to\discord-message-create.json
cargo run -p openclaw-harness-cli -- discord-gateway-probe --harness-home C:\path\to\.openclaw-harness --openclaw-home C:\path\to\.openclaw
cargo run -p openclaw-harness-cli -- telegram-poll-once --openclaw-home C:\path\to\.openclaw --harness-home C:\path\to\.openclaw-harness --agent main --codex-exe C:\path\to\codex.cmd --poll-timeout-seconds 1 --max-updates 10
cargo run -p openclaw-harness-cli -- telegram-loop --openclaw-home C:\path\to\.openclaw --harness-home C:\path\to\.openclaw-harness --agent main --codex-exe C:\path\to\codex.cmd --iterations 1 --idle-ms 1000
cargo run -p openclaw-harness-cli -- discord-outbox-send-once --harness-home C:\path\to\.openclaw-harness --outbox-limit 20
```

Use `tools/openclaw-fake-codex-app-server` for offline CI and activation smoke. Use real `codex app-server` only in operator-run smoke tests because it may make model requests.
