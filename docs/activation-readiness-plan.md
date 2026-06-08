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
   - Confirm `TELEGRAM_BOT_TOKEN` when Telegram is enabled.
   - Confirm `DISCORD_BOT_TOKEN` when Discord is enabled.
   - Confirm `OPENROUTER_API_KEY` only when OpenRouter providers are active.
   - Do not rely on an imported OpenClaw embedding-only `OPENAI_API_KEY` for Codex agent turns.

5. Runtime gate
   - Run `channel-receive` for a normal DM.
   - Prefer `channel-run-once` for one-message end-to-end smoke.
   - Run `runtime-run-once` with the intended Codex executable.
   - Confirm `state/runtime-queue/run-once-last.json`.
   - Confirm `state/runtime-queue/run-once-receipts.jsonl`.
   - Confirm transcript and trajectory files under `agents/<agent-id>/sessions`.
   - Confirm raw Codex stdout/stderr logs under the execution directory.

6. Channel delivery gate
   - Run `channel-outbox-plan`.
   - Deliver the pending reply through the target adapter or manual test harness.
   - Run `channel-delivery-record --status delivered`.
   - Confirm future `channel-outbox-plan` skips delivered messages and retries failed messages.
   - For Telegram smoke, set `TELEGRAM_BOT_TOKEN` and run `telegram-poll-once` against a controlled DM; confirm command replies and agent replies are delivered by the Bot API.
   - For Telegram handoff rehearsal, run `telegram-loop --iterations 0` only after confirming the old Docker gateway is not also consuming Telegram updates.
   - For Discord outbound smoke, set `DISCORD_BOT_TOKEN`, create a Discord outbox item with `channel-run-once --platform discord ...`, then run `discord-outbox-send-once`.

7. Logging gate
   - Run `enable-check`.
   - Confirm `state/logs/harness.jsonl` is writable.
   - Confirm logs include activation, Telegram poll-once or loop, Discord outbox send, channel receive, runtime run-once, Codex run, completion, and delivery events.
   - Confirm `enable-check` reports `telegram-offset`, `telegram-poll-log`, and `discord-send-log` as pass after adapter smoke tests.

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
   - Still required for formal service activation: Windows service or scheduled-task install path, health/status, graceful shutdown, token source hardening, and production retry policy.

2. Real Discord adapter
   - Done for outbound smoke: `discord-outbox-send-once` sends pending Discord outbox messages through Discord REST, records delivery receipts, and writes a delivery summary log.
   - Still required for formal Discord activation: receive Discord DM events through a gateway adapter.
   - Normalize inbound DMs into `channel-run-once`.
   - Add gateway heartbeat/reconnect, dedupe, and health/status reporting.

3. Worker loop
   - Turn `runtime-run-once` into a supervised loop.
   - Add stop/cancel handling for `/stop`.
   - Add bounded retries and backoff.

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
   - Node sidecar process contract.
   - Tool/hook/memory slot JSON-RPC bridge.
   - Health check surfaced in `enable-check`.

7. Operations
   - Windows service or scheduled task install path.
   - Health/status command for bot process.
   - Backup/export command.
   - Explicit cutover command that records operator intent.

## Verification Commands

```powershell
cargo fmt --all
cargo test --workspace --quiet
cargo clippy --workspace --all-targets -- -D warnings
cargo run -p openclaw-harness-cli -- help
cargo run -p openclaw-harness-cli -- enable-check --target-home C:\path\to\.openclaw-harness
cargo run -p openclaw-harness-cli -- telegram-poll-once --openclaw-home C:\path\to\.openclaw --target-home C:\path\to\.openclaw-harness --agent main --codex-exe C:\path\to\codex.exe --poll-timeout-seconds 1 --max-updates 10
cargo run -p openclaw-harness-cli -- telegram-loop --openclaw-home C:\path\to\.openclaw --target-home C:\path\to\.openclaw-harness --agent main --codex-exe C:\path\to\codex.exe --iterations 1 --idle-ms 1000
cargo run -p openclaw-harness-cli -- discord-outbox-send-once --target-home C:\path\to\.openclaw-harness --outbox-limit 20
```

Use fake app-server tests for CI. Use real `codex app-server` only in operator-run smoke tests because it may make model requests.
