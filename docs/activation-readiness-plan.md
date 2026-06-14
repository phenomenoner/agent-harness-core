# Activation Readiness Plan

Date: 2026-06-15

This is the working checklist for turning the Rust Windows Agent Harness from a local core runtime into the active replacement for the Docker legacy gateway.

## Latest Verified State

2026-06-12 repo-local harness-home baseline after round3-2 timeout/progress reconciliation:

- Round4-3 live robustness implementation is staged on 2026-06-15: runtime-loop Windows lock/JSON sharing-violation paths retry instead of failing silently, runtime queue `lease-busy` is a retryable non-idle status, supervised infinite runtime loops enter `safe-mode` with reduced concurrency instead of exiting after repeated errors, cron scheduler has read-only lint plus interval-floor/stale-lock hardening, and status/readiness/log scans tail-sample large JSONL ledgers.
- Round4-3 staged verification passed fmt, workspace check, 243 core tests, 19 CLI tests, staging build, public hygiene, and `git diff --check` with line-ending warnings only. The first live `cron-scheduler-lint` run found existing imported cron content blockers (`errors=65`, `warnings=26`); those scheduler entries need operator review before scheduler cleanliness can be claimed.
- Live harness home is now repo-local `.agent-harness`; `.agent-harness/` is ignored by git. `imports/activation-harness` remains only as a pre-rebase backup.
- Latest channel identity / delivery intent / cron scheduler / live-control implementation verification passed `cargo fmt --all --check`, staged workspace check, 239 core tests, 18 CLI tests, staged build, public export hygiene, and non-live cutover CLI smoke.
- `agent-harness.exe` builds; `cargo fmt --all` and full `cargo test --workspace` passed with 207 core tests, 16 CLI tests, and doc-tests. Previous activation also passed `cargo build`.
- Earlier deployment validation passed `cargo build --workspace`, gateway stop/start with direct runners, live `status`, live `enable-check`, and outbox plan. Current Round4-2 live cutovers should use `ops-cutover-request`/`approve`/`apply` plus `harness.ps1 gateway start|stop|restart --live-control-token <token>` instead of direct self-stop from a live channel turn.
- Supervisor scripts were regenerated with `target/debug/agent-harness.exe`, `--source-home`, `--runtime-workspace D:\Warehouse\Research\OpenClaw_WSL`, `tools/agent-discord-gateway/index.mjs`, one bounded-concurrency `runtime-loop.ps1`, and `worker-loop.ps1`.
- `AgentHarness-*` scheduled task registration returned access-denied in this environment, so the runtime workers, worker, progress delivery, Telegram, Discord outbox, and Discord gateway loops were started manually as hidden PowerShell processes from the generated scripts.
- Round3-2 timeout/progress reconciliation is implemented: `timeout` is terminal for runtime queue selection, status open-item counts, native typing context, and progress delivery state. A queued pending row with a timeout receipt should no longer be interpreted as open work. See `docs/round3-2-implementation-and-upgrade-plan.md`.
- Latest live status after restart: `ready=true`, `passed=58`, `warnings=0`, `failed=0`; runtime `queued=123`, `open=0`, `prepared=123`, `completed=120`; outbox `pending=0`, `delivered=186`, `retryable=0`, `invalid=0`.
- Loop heartbeats are live for runtime, worker, progress delivery, Telegram, Discord outbox, Discord gateway, and cron scheduler after the Round4-2 scheduler cutover. Deployments that have not enabled scheduler ticks still have the six always-on channel/runtime loops; `supervisor-plan --include-cron-scheduler` intentionally adds the scheduler loop only for live scheduler cutover.
- Worker config is live at global=12, per-agent/group=6, per-agent-per-channel=3, lane limits `llm=6`, `shell=6`, `watchdog=2`, `maintenance=2`, `plugin=2`, with no worker config warnings.
- Prompt-file loading now falls back from runtime cwd to imported workspace when needed, and injected prompt files include role headers for `AGENTS.md`, `SOUL.md`, `TOOLS.md`, `USER.md`, `IDENTITY.md`, `HEARTBEAT.md`, and `BOOTSTRAP.md`. Skills are dynamic task context, so `Skills: 0 selected` can be normal for command/status turns.
- Progress rendering now compacts long PowerShell/Codex tool-call previews, suppressed low-value `assistant_stream` deltas, routes assistant narration to the editable `Current step` status under the default `progress_panel` setting, skipped-denied progress delivery advances the cursor to prevent repeated Telegram `Working` messages, and terminal runtime progress cannot be downgraded by later stray events for the same parent queue id.
- `ops-cutover-receipt` recorded `status=ready`; the latest activation notes include runtime UX hardening, durable ops activation, assistant narration routing, live-control cutover tokens, and docs sync.
- `memory-lancedb` is hidden unless the source config explicitly selects LanceDB as the active memory backend.
- Controlled online testing can proceed by having an allowed Telegram/Discord user send a normal message, then recording transcript and delivery receipt paths here.
- Round4-2 live cutover on 2026-06-14 used ticket `cutover-1781376947099`, regenerated the 7-loop supervisor plan with live-control guards, synced `agent-windows-harness` v0.1.9, started direct runners, and verified `status --json` with `ready=true`, `passed=59`, `warnings=0`, `failed=0`.
- Runtime-loop missing/stale/error/stopped/stopping is now a readiness failure for live operation. Runtime-loop `safe-mode` is a degraded warning that should trigger operator inspection but preserves the communication path.
- Current status/readiness counts over very large logs may be sampled tail windows. Treat sampled warnings as a signal to inspect focused ledgers instead of comparing them with full historical totals.

## Activation Target

The target state is:

- Docker legacy gateway can be stopped, not deleted.
- Rust harness can receive Telegram/Discord DM input.
- Slash commands work consistently across Telegram/Discord.
- Ordinary DM input can route to the right imported agent/session, run Codex through app-server, write transcript/trajectory/Codex binding files, and deliver the assistant reply.
- Imported memory, cron, subagent, plugin, and historical session state remains available; memory now has text recall, SQLite vector recall, lifecycle capture, and symbolic canvas receipts, with deeper Qdrant-native/plugin parity tracked separately.

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
   - Confirm `skills/.agent-harness-builtins.json` exists.
   - Run `skills --harness-home ... --query "<task>"` and confirm imported plus bundled skills are visible.

4. Credential gate
   - Prefer Codex OAuth for Codex models.
   - Confirm Codex auth through local `CODEX_HOME`, `%USERPROFILE%\.codex\auth.json`, or `%USERPROFILE%\.codex\auth.toml`.
   - Confirm the harness uses a spawnable Codex CLI binary. The Codex Desktop MSIX resource path may resolve on `PATH` but fail to spawn with Windows `os error 5`; use a standalone release or a local npm install such as `.tools/codex-cli/node_modules/.bin/codex.cmd`.
   - Run `channel-credentials-export --include-sensitive` when migrating from an existing Source home. It writes Telegram/Discord tokens and known channel/user/guild IDs into `secrets/channel-credentials.env` and redacted receipts into `secrets/channel-credentials-receipts.json`.
   - Confirm `TELEGRAM_BOT_TOKEN` is present either in process env or `secrets/channel-credentials.env` when Telegram is enabled.
   - Confirm `DISCORD_BOT_TOKEN` is present either in process env or `secrets/channel-credentials.env` when Discord is enabled.
   - Confirm `enable-check` reports `telegram-access-policy` and `discord-access-policy` as pass when importing from an existing legacy channel configuration. Missing access policies are warnings because fresh deployments may intentionally configure them later.
   - Run `telegram-probe` to validate Telegram Bot API `getMe` without consuming updates or sending messages.
   - Run `memory-credentials-export --include-sensitive` when migrating imported memory search. It writes `AGENT_HARNESS_MEMORY_EMBEDDING_API_KEY`, model, and base URL into `secrets/memory-credentials.env`; receipts disclose env names, source paths, and lengths only.
   - Confirm `OPENROUTER_API_KEY` only when an OpenRouter route or agent is active; default OpenAI/Codex OAuth turns must not inherit OpenRouter provider config.
   - Do not rely on an imported legacy embedding-only `OPENAI_API_KEY` for Codex agent turns.
   - Run `channel-identity-check` for every enabled platform/account/channel tuple when a channel identity registry is present; missing, disabled, conflicting, or wrong-agent bindings must block ingress.

5. Runtime gate
   - Run `channel-receive` for a normal DM.
   - Prefer `channel-run-once` for one-message end-to-end smoke.
   - Run `queue-prepare`, `codex-plan`, `codex-preflight`, and `codex-launch-probe` before the first real model run. For OpenRouter smoke, confirm the generated Codex home is `codex-home-providers/openrouter`; for default Codex/OAuth smoke, confirm the shared `codex-home` has no OpenRouter `model_provider` override.
   - Confirm `enable-check` reports `codex-runtime-launch-probe` as pass.
   - Run `runtime-run-once` with the intended Codex executable.
   - Confirm `state/runtime-queue/run-once-last.json`.
   - Confirm `state/runtime-queue/run-once-receipts.jsonl`.
   - Confirm a `timeout` run-once receipt closes the parent queue id for status/typing/progress; retry should be represented by a new queue id.
   - Run `runtime-loop --stop-when-idle --iterations 1` for idle/drain smoke, or `runtime-loop --iterations 0` only under an operator/supervisor after handoff.
   - For supervised infinite loops, keep safe-mode restart enabled. `--no-safe-mode-restart` is only for finite/debug runs where the operator wants immediate process exit.
   - Confirm `state/runtime-queue/loop-last.json`.
   - Confirm `enable-check` and `healthz --require-writable-state` report runtime-loop as live/ready. Missing/stale/error/stopped/stopping runtime-loop heartbeat blocks cutover; `safe-mode` is a degraded warning that requires inspection.
   - Run `supervisor-plan` with the intended harness CLI, Codex executable, agent id, and channel loop selection.
   - Add `--include-cron-scheduler` only when the operator is intentionally enabling live scheduler ticks.
   - Before enabling or changing live scheduler ticks, run `cron-scheduler-lint --dry-run --enable` and `cron-scheduler-run-once --dry-run --enable` against the intended harness/source/workspace paths. Lint errors block scheduler cutover.
   - Confirm `state/supervisor/windows-scheduled-tasks/supervisor-plan.json`.
   - Confirm generated scripts use absolute paths and do not contain raw tokens.
   - Confirm `enable-check` reports `supervisor-plan` as pass.
   - Confirm transcript and trajectory files under `agents/<agent-id>/sessions`.
   - Confirm raw Codex stdout/stderr logs under the execution directory.

6. Channel delivery gate
   - Run `channel-outbox-plan`.
   - Deliver the pending reply through the target adapter or manual test harness.
   - Run `channel-delivery-record --status delivered`.
   - Confirm future `channel-outbox-plan` skips delivered messages and retries failed messages.
   - For Telegram smoke, set or import `TELEGRAM_BOT_TOKEN` and the imported Telegram user/chat allow-lists, then run `telegram-poll-once` against a controlled DM; confirm denied updates are skipped and allowed command/agent replies are delivered by the Bot API.
   - For Telegram handoff rehearsal, run `telegram-loop --iterations 0` only after confirming the old Docker gateway is not also consuming Telegram updates.
   - For Discord outbound smoke, set or import `DISCORD_BOT_TOKEN`, create a Discord outbox item with `channel-run-once --platform discord ...`, then run `discord-outbox-send-once`.
   - For Discord inbound handoff rehearsal, import Discord user/channel/guild allow-lists, run `discord-gateway-probe`, then run `discord-gateway-loop` only after confirming the old Docker gateway is not also connected.

7. Logging gate
   - Run `enable-check`.
   - Run `healthz --require-writable-state`.
   - Run `status --json`.
   - Confirm `state/logs/harness.jsonl` is writable.
   - Confirm logs include activation, Telegram probe, Telegram poll-once or loop, Discord outbox send, channel receive, runtime run-once, runtime loop, Codex run, completion, and delivery events.
   - Confirm `enable-check` reports `telegram-probe` as pass after token/API smoke and reports `telegram-offset`, `telegram-poll-log`, and `discord-send-log` as pass after adapter smoke tests.
   - Confirm `status` reports runtime `openItems=0` and outbox `pending=0` before live adapter handoff.
   - If `status`, `healthz`, or `enable-check` warns that a JSONL/log ledger was sampled, treat the affected counts as a tail-window summary and use targeted receipt inspection for full historical evidence.

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
   - Run `cron-scheduler-run-once --dry-run --enable` and confirm scheduler receipts plus `state/cron-scheduler/loop-last.json` or tick readback do not execute jobs directly.

6. Historical state smoke
   - Confirm imported session indexes and transcript files are present.
   - Confirm memory files/databases are imported or explicitly documented as unavailable.
   - Confirm `memory/qdrant-edge` is present when Qdrant edge is the active legacy memory backend.
   - Confirm `openclaw-mem.sqlite` and memory JSONL files are present as snapshot/audit sources.
   - Run `memory-vector-search --query "<known memory>"` and confirm `state/memory/vector-recall-receipts.jsonl` reports `ready` with hits.
   - Run `memory-canvas-run` and confirm `state/memory/canvas-receipts.jsonl` reports `written` or an explicit `skipped` reason.
   - Treat LanceDB as backup/optional unless the active config points to LanceDB.
   - Confirm subagent ledgers are held by default.

## Remaining Development To Formal Activation

1. Real Telegram adapter
   - Done for non-consuming token/API readiness: `telegram-probe` calls Telegram Bot API `getMe`, writes `state/channels/telegram-probe.json`, appends `telegram-probe-receipts.jsonl`, and logs `telegram.probe` without consuming updates or sending messages.
   - Done for smoke and operator-run handoff: `telegram-poll-once` receives Telegram text updates, enforces imported Telegram direct/group chat and user allow-lists before runtime dispatch, normalizes allowed messages into `channel-run-once`, delivers pending replies through Telegram Bot API `sendMessage`, records delivery receipts, stores update offset state after every successful poll, and writes a poll summary log. `telegram-loop` repeats the same path with idle sleep and a consecutive-error threshold.
   - Done for graceful operator stop: `telegram-loop --stop-file <path>` exits before the next poll when the stop file appears.
   - Health/status CLI summary is available through `status`.
   - Still required for formal Telegram activation: live Telegram DM smoke after the old gateway is offline, token source hardening, and production retry policy.

2. Real Discord adapter
   - Done for outbound smoke: `discord-outbox-send-once` sends pending Discord outbox messages through Discord REST, records delivery receipts, and writes a delivery summary log.
   - Done for inbound normalization smoke: `discord-event-run-once` accepts one Discord Gateway `MESSAGE_CREATE` event from `--event-file` or `--event-json`, enforces imported Discord user/channel/guild allow-lists, dedupes by message id, normalizes allowed text into `channel-run-once`, writes Discord event receipts, and logs `discord.event-run-once`.
   - Done for operator-run inbound loop: `discord-gateway-loop` receives Discord DM events through the Node WebSocket wrapper and feeds them into `discord-event-run-once` semantics; it accepts a stop file through the CLI wrapper and closes the WebSocket cleanly when requested.
   - Still required for formal Discord activation: live Discord DM smoke after the old gateway is offline.
   - Add gateway heartbeat/reconnect and live process health reporting beyond the CLI `status` summary.

3. Worker loop
   - Done for CLI handoff/smoke: `runtime-loop` wraps `runtime-run-once`, drains queued work, treats already recorded completions as idle for loop control, supports finite or infinite `--iterations`, sleeps with `--idle-ms`, exits with `--stop-when-idle` or `--stop-file`, writes `state/runtime-queue/loop-last.json`, and logs `runtime.loop-stopped` plus `runtime.loop-error`.
   - Done for scheduled-task handoff path: `supervisor-plan` writes Task Scheduler install/start/stop/uninstall scripts for runtime, Telegram, Discord, worker, progress, and optional cron scheduler loops, with absolute paths and stop files; it does not register tasks automatically.
   - Runtime retry/backoff policy is configurable through `runtimeBackoff`; provider/model fallback remains operator-guided instead of silent automatic switching.
   - Still required for formal service activation: operator execution of generated scheduled-task installer, process supervisor health integration, and `/stop` cancellation of already-running model turns.

4. Unified worker dispatch for schedulers
   - Native legacy `.openclaw/cron` scheduler ticks enqueue LLM-backed agent/subagent jobs.
   - Extended deterministic crontab/Supercronic-style scheduler ticks enqueue no-LLM shell jobs.
   - Done for repeated scheduler ticks: `cron-scheduler-run-once`/`cron-scheduler-loop` write durable watermarks and idempotently enqueue due jobs into WorkerStore.
   - Shared worker execution provides durable leases, retries/backoff, audit logs, rate leases, and timeout/cancel semantics.
   - Harness-configured global, per-agent/group, per-agent-per-channel, and lane concurrency limits prevent fan-out or cron bursts from overloading local processes or provider rate limits; excess jobs remain queued.
   - Fan-out work uses deterministic watchdogs to wake the master agent with child status and artifact pointers on completion, failure, timeout, or checkpoint policies.
   - Cutover watermarks avoid catch-up storms.

5. Memory adapter
   - Done for inventory: imported memory files, Qdrant edge snapshot, and `openclaw-mem.sqlite` are surfaced in `status` and `enable-check`; LanceDB is surfaced only when source config explicitly selects it.
   - Done for text fallback: `memory-search` scans imported markdown/text/JSONL memory files read-only and writes redacted receipts.
   - Done for embedding secrets: `memory-credentials-export` migrates imported embedding key/model/base URL into harness memory secrets without logging raw values.
   - Done for readable vector recall: `memory-vector-search` embeds the query and searches imported SQLite embedding tables for observations, docs chunks, and episodic events; prompt assembly uses this before text fallback when configured.
   - Done for minimal canvas: lifecycle capture writes conservative auto-capture candidates and `memory-canvas-run` builds compact JSON/Markdown symbolic canvas receipts.
   - Qdrant edge is still treated as the preserved primary snapshot. The Rust adapter detects and reports it but does not raw-read Qdrant segment files; Qdrant-native recall should use a sidecar/service or a supported snapshot API.
   - Integration boundary: `openclaw-mem` remains an external dual-consumer memory product. Agent Harness should add OpenClaw-compatible adapters and agent-turn hooks, not patch `openclaw-mem` internals for harness-only behavior.
   - Still required for full parity: LanceDB read fallback only when explicitly selected, direct Qdrant-native adapter, routeAuto/autoRecall policy matching, direct propose/store semantics, and imported Node plugin hook parity when explicitly enabled.

6. Plugin sidecar
   - Run `plugin-sidecar-probe` and confirm `enable-check` reports `plugin-sidecar-probe` as pass.
   - Run `plugin-sidecar-call --method sidecar.status`, `plugin-sidecar-call --method plugins.list`, and `plugin-sidecar-call --method tools.probe`; confirm `enable-check` reports `plugin-sidecar`, `plugin-sidecar-probe`, and `plugin-sidecar-bridge` as pass.
   - Done for activation catalog: the Node sidecar resolves imported plugin manifests, writes `state/plugin-sidecar/catalog.json`, writes execution receipts, and exposes manifest-derived tools over `tools.list`.
   - `openclaw-context-budget` is handled as a native prompt-budget adapter by the harness instead of requiring Node plugin source.
   - Still required for broader plugin parity: plugin-specific executor adapters for tool/hook/memory slot invocation beyond manifest catalog and bridge receipts.
   - Health check is surfaced in `enable-check`.

7. Operations
   - Done for scheduled-task install path: `supervisor-plan` generates user-logon scheduled task scripts and stop/start/uninstall helpers under `state/supervisor/windows-scheduled-tasks`.
   - Done for CLI health/status: `status` summarizes readiness, runtime queue, channel outbox, memory, plugins, and logs; `--json` is monitor-friendly.
   - Still required: operator-run task registration/start smoke, process supervisor health endpoint or scheduled-task monitor integration.
   - Done for backup/export command: `ops-backup`.
   - Done for explicit cutover intent: `ops-cutover-request`, `ops-cutover-approve`, `ops-cutover-apply`, and `ops-cutover-status` record ticket/token/apply/status receipts.

## Current Activation Snapshot

As of 2026-06-11 live verification:

- Live activation harness: `.agent-harness`.
- Previous activation backup: `imports/activation-harness`.
- `.\harness.ps1 gateway status` reports `Ready: yes`, `passed=58`, `warnings=0`, `failed=0`.
- Runtime queue latest stable readback: `queued=117`, `prepared=117`, `completed=113`, `open=1`. The current `open=1` is not a normal clean-idle state; it is linked to the round3-2 Discord stale timeout/background-task triage.
- Channel outbox is clean: `pending=0`, `delivered=177`, `retryable=0`, `invalid=0`. The previous stale Telegram retry was manually read back and marked delivered with provider id `manual-readback-20260611`.
- Supervisor plan has 6 canonical task entries: `runtime-loop`, `worker-loop`, `progress-delivery-loop`, `telegram-loop`, `discord-outbox-loop`, and `discord-gateway-loop`; a seventh `cron-scheduler-loop` entry is present only after explicit scheduler enablement.
- Canonical status heartbeats are live for runtime, progress delivery, Telegram, Discord outbox, Discord gateway, and worker loops.
- Runtime queue leasing uses `state/runtime-queue/runtime-leases.json` plus a lock file to let multiple runtime loops run without duplicating queue items.
- Worker dispatch config is global 12, per-agent/group 6, per-agent-per-channel 3, lane limits `llm=6`, `shell=6`, `watchdog=2`, `maintenance=2`, `plugin=2`.
- Prompt files are resolved from the imported workspace when the runtime workspace is only Codex cwd, so a live `/status` should not show `Prompt files 0/7` for the imported agent. Skills are selected dynamically and may be 0 for status-only turns.
- Progress events use compact tool-call previews, assistant narration current-step status, and permission-skipped progress cursors, preventing repeated Telegram `Working` status spam while keeping final replies clean.

As of 2026-06-08 local verification:

- Imported activation harness: `imports/activation-harness`.
- Qdrant edge is present and passes as the primary memory backend.
- LanceDB is absent from the filtered activation snapshot and is not part of the default readiness surface because the active memory slot uses `openclaw-mem-engine`.
- `openclaw-mem.sqlite` is present as a snapshot/audit source.
- Offline `/status` channel smoke passes against the imported registry: 25 enabled agents, 2 providers, 13 plugins, Telegram and Discord enabled.
- Runtime queue prepare, Codex plan, Codex preflight, and Codex launch probe pass when using workspace-local `@openai/codex` via `.tools/codex-cli/node_modules/.bin/codex.cmd`.
- Plugin sidecar probe passes. `openclaw-context-budget` is classified as a native adapter, leaving 5 sidecar-required plugins. `tools.probe` resolves all sidecar-required plugin manifests from local source roots, writes `state/plugin-sidecar/catalog.json`, reports 2 manifest-derived tools, and makes `enable-check` pass `plugin-sidecar`.
- Discord Gateway `MESSAGE_CREATE` event normalizer smoke passes for `/status`, including duplicate-message skip by Discord message id.
- `channel-credentials-export --include-sensitive` imported Telegram/Discord bot tokens plus known allow-list/guild/channel/chat IDs from the local OpenClaw snapshot into `imports/activation-harness/secrets/channel-credentials.env`; readiness sees both token gates and both access-policy gates as pass.
- `telegram-probe` is implemented and passes against `imports/activation-harness` as the non-consuming Telegram `getMe` readiness check, separating token/API failures from update consumption before live `telegram-poll-once` handoff.
- `discord-gateway-probe` passes with the imported Discord token and Node 24 global WebSocket support.
- `discord-outbox-send-once` passes with an empty pending outbox, writes `discord.outbox-send-once`, and does not send any message when `pending=0`.
- `supervisor-plan` generated three Windows scheduled-task plans (`runtime-loop`, `telegram-loop`, `discord-gateway-loop`) plus install/start/stop/uninstall scripts under `imports/activation-harness/state/supervisor/windows-scheduled-tasks`; generated scripts use absolute paths, point at the local `imports/openclaw-core-snapshot` source because the mounted `D:\Warehouse\Research\OpenClaw_WSL\.openclaw` path is absent, and contain no raw token/key/secret strings.
- The Codex Desktop MSIX `codex.exe` path is not spawnable from this harness environment and should not be used for service runtime.
- Offline normal-turn smoke passes through `channel-run-once` with `tools/agent-fake-codex-app-server/fake-codex-app-server.cmd`, producing runtime-run-once, Codex run, Codex completion, transcript, outbox, delivery receipt, and operational log evidence without a model request or channel send.
- Runtime loop idle/drain smoke passes with `runtime-loop --stop-when-idle` and writes `state/runtime-queue/loop-last.json` without a model request when no pending queue items remain.
- `telegram-poll-once` has run successfully against the imported Telegram token and allow-lists. No pending updates were present, so `state/channels/telegram-offset.json` currently records `nextOffset=null`; this still proves the poll adapter can take over without consuming stale updates.
- `status` reports `queued=2 open=0 prepared=2 completed=2`, outbox `pending=0 delivered=4`, Telegram offset/probe/poll-log present, Qdrant edge primary memory present, plugin catalog ready with 2 manifest-derived tools, and operational log event coverage for offline runtime/delivery smoke.
- Earlier `enable-check` reported `Ready: yes` with `passed=35 warnings=1 failed=0`; that warning was the old optional LanceDB backup absence check and is no longer emitted unless LanceDB is explicitly selected.

As of 2026-06-09 live activation follow-up:

- `status --harness-home imports/activation-harness` reports `Ready: yes` with `passed=51 warnings=3 failed=0` after restarting the runtime, Telegram, Discord outbox, and Discord gateway loops.
- Outbox is clean for handoff: `pending=0 delivered=41 retryable=0 invalid=0`. The remaining local smoke replies were marked delivered with `providerMessageId=local-smoke` because they were local CLI-only command replies, not Telegram/Discord sends.
- Telegram loop stale polling was traced to unbounded ureq HTTP calls. Telegram `getMe`, `getUpdates`, `sendMessage`, and `sendChatAction` now use bounded connect/read/write timeouts; Discord REST helper calls use the same bounded short HTTP agent.
- Live loop heartbeats are present for `runtime-loop`, `telegram-loop`, `discord-outbox-loop`, and `discord-gateway-loop`. Current samples: runtime `status=no-work`, Telegram `status=running` while polling updates, Discord outbox `status=ok`, and Discord gateway heartbeat ack present.
- `/status` channel replies use `Agent Harness Status`, and `/model` plus `/think` report the current session setting before listing or changing options.
- `/model <provider>/<model> --global` and `/think <level> --global` are per-agent defaults. They are persisted under `state/agents/overrides.json` for the current agent only and do not affect other imported agents.
- Discord DM HTTP poll fallback is initialized and records cursors under `state/channels/discord-dm-poll-cursors.json`. A real allowed-user Discord DM still needs to be sent to clear the `discord-real-inbound` warning.
- `openclaw-mem` and `openclaw-mem-engine` plugin manifests are resolved from `D:\Warehouse\Research\OpenClaw_WSL\openclaw-mem-gateway\openclaw-mem-src\extensions`, but their manifest files declare no direct `tools` or `hooks`; the behavior is registered at runtime through the legacy plugin API.
- Original `openclaw-mem-engine` lifecycle behavior uses `before_prompt_build` with fallback `before_agent_start` for auto recall or routeAuto prompt mutation, and `agent_end` for autoCapture. `openclaw-mem` uses tool-result and `agent_end` capture paths for observations and episodes.
- The Agent Harness parity target is to reproduce those OpenClaw hook boundaries in the harness adapter layer, while keeping future `openclaw-mem` engine, graph, and sidecar features usable by both OpenClaw and Agent Harness.
- Imported mem-engine config uses Qdrant edge retrieval, `text-embedding-3-small`, `autoRecall.enabled=false` with `routeAuto.enabled=true`, `autoCapture.enabled=true`, episodes enabled, and symbolic canvas auto build enabled.
- The imported embedding key is present in `imports/openclaw-core-snapshot/openclaw.json` under `plugins.entries["openclaw-mem-engine"].config.embedding.apiKey`. `memory-credentials-export --include-sensitive` migrates it into harness memory secrets under `AGENT_HARNESS_MEMORY_EMBEDDING_API_KEY`, separate from Codex/OpenAI agent-turn auth. A minimal OpenAI embeddings smoke test with that imported key succeeded against `text-embedding-3-small` and returned a 1536-dimensional vector.
- Rust memory support now includes an in-harness lifecycle adapter at the two OpenClaw-equivalent boundaries:
  - pre-turn prompt assembly tries imported SQLite vector recall first when memory embedding secrets are present, then falls back to imported text recall; it injects one bounded `MemoryContext` section before the user message and writes `state/memory/prompt-context-receipts.jsonl`.
  - post-turn successful runtime completion records episode spool lines and conservative auto-capture candidates according to imported `openclaw-mem` / `openclaw-mem-engine` config; it writes `state/memory/lifecycle-receipts.jsonl`.
  - symbolic canvas worker writes `state/memory/canvas/symbolic-canvas.json`, `state/memory/canvas/symbolic-canvas.md`, and `state/memory/canvas-receipts.jsonl`.
  - `/status` and readiness now surface `search`, `vectorRecall`, `promptContext`, `lifecycle`, `canvas`, and `embeddingSecrets` receipt summaries.
- Activation memory smoke passed:
  - `memory-credentials-export --include-sensitive` wrote `secrets/memory-credentials.env` and a redacted receipt.
  - `memory-vector-search --query "Qdrant edge memory backend and symbolic canvas"` returned 5 hits via SQLite vector recall using `text-embedding-3-small`, 1536-dimensional embeddings.
  - `prompt-bundle` smoke wrote a prompt-context receipt with 5 memory hits.
  - `memory-canvas-run` wrote compact canvas JSON/Markdown from 40 imported episodes.
- Current memory readiness has no LanceDB warning; `memory-lancedb` only appears when source config explicitly selects LanceDB.
- Still required for full openclaw-mem parity: direct Qdrant edge service/sidecar recall, routeAuto/autoRecall policy matching, direct openclaw-mem propose/store semantics, and imported Node plugin hook parity.

## Verification Commands

```powershell
cargo fmt --all
cargo test --workspace --quiet
cargo clippy --workspace --all-targets -- -D warnings
cargo run -p agent-harness-cli -- help
cargo run -p agent-harness-cli -- channel-credentials-export --source-home C:\path\to\.openclaw --harness-home C:\path\to\.agent-harness --include-sensitive
cargo run -p agent-harness-cli -- telegram-probe --harness-home C:\path\to\.agent-harness
cargo run -p agent-harness-cli -- enable-check --harness-home C:\path\to\.agent-harness
cargo run -p agent-harness-cli -- healthz --target-home C:\path\to\.agent-harness --require-writable-state
cargo run -p agent-harness-cli -- status --harness-home C:\path\to\.agent-harness --json
cargo run -p agent-harness-cli -- cron-scheduler-lint --harness-home C:\path\to\.agent-harness --source-home C:\path\to\.agent-harness --workspace C:\path\to\.agent-harness\workspace --dry-run --enable
cargo run -p agent-harness-cli -- cron-scheduler-run-once --harness-home C:\path\to\.agent-harness --source-home C:\path\to\.agent-harness --workspace C:\path\to\.agent-harness\workspace --dry-run --enable
cargo run -p agent-harness-cli -- supervisor-plan --harness-home C:\path\to\.agent-harness --source-home C:\path\to\.openclaw --workspace C:\path\to\workspace --harness-cli C:\path\to\agent-harness.exe --codex-exe C:\path\to\codex.cmd --agent main
cargo run -p agent-harness-cli -- channel-run-once --harness-home C:\path\to\.agent-harness --source-home C:\path\to\.openclaw --platform telegram --channel-id smoke --user-id operator --message /status
cargo run -p agent-harness-cli -- channel-run-once --harness-home C:\path\to\.agent-harness --source-home C:\path\to\.openclaw --platform telegram --channel-id offline-runtime-smoke --user-id operator --message "offline runtime smoke" --agent main --codex-exe tools\agent-fake-codex-app-server\fake-codex-app-server.cmd --timeout-ms 5000
cargo run -p agent-harness-cli -- runtime-loop --harness-home C:\path\to\.agent-harness --codex-exe tools\agent-fake-codex-app-server\fake-codex-app-server.cmd --iterations 1 --idle-ms 1 --stop-when-idle
cargo run -p agent-harness-cli -- codex-launch-probe --harness-home C:\path\to\.agent-harness --execution-dir C:\path\to\prepared-execution --startup-probe-ms 750
cargo run -p agent-harness-cli -- plugin-sidecar-probe --harness-home C:\path\to\.agent-harness
cargo run -p agent-harness-cli -- plugin-sidecar-call --harness-home C:\path\to\.agent-harness --method sidecar.status
cargo run -p agent-harness-cli -- plugin-sidecar-call --harness-home C:\path\to\.agent-harness --method plugins.list
$env:AGENT_HARNESS_PLUGIN_SOURCE_ROOTS='C:\path\to\openclaw-src\extensions;C:\path\to\openclaw-mem-src\extensions'
cargo run -p agent-harness-cli -- plugin-sidecar-call --harness-home C:\path\to\.agent-harness --method tools.probe
cargo run -p agent-harness-cli -- discord-event-run-once --harness-home C:\path\to\.agent-harness --source-home C:\path\to\.openclaw --event-file C:\path\to\discord-message-create.json
cargo run -p agent-harness-cli -- discord-gateway-probe --harness-home C:\path\to\.agent-harness --source-home C:\path\to\.openclaw
cargo run -p agent-harness-cli -- telegram-poll-once --source-home C:\path\to\.openclaw --harness-home C:\path\to\.agent-harness --agent main --codex-exe C:\path\to\codex.cmd --poll-timeout-seconds 1 --max-updates 10
cargo run -p agent-harness-cli -- telegram-loop --source-home C:\path\to\.openclaw --harness-home C:\path\to\.agent-harness --agent main --codex-exe C:\path\to\codex.cmd --iterations 1 --idle-ms 1000
cargo run -p agent-harness-cli -- discord-outbox-send-once --harness-home C:\path\to\.agent-harness --outbox-limit 20
```

Use `tools/agent-fake-codex-app-server` for offline CI and activation smoke. Use real `codex app-server` only in operator-run smoke tests because it may make model requests.
