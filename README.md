# Rust OpenClaw Core

Minimal Rust/Windows agent harness inspired by OpenClaw.

The project starts with a small, testable foundation:

- Import planning for an existing OpenClaw home directory.
- A core crate with data-layout detection logic for config, workspace prompts, agents, skills, sessions, native cron, deterministic cron, subagents, memory, and plugins.
- A read-only importer dry-run report with Hermes-style conflict policy and receipts.
- A read-only multi-agent registry parser for OpenClaw agents, providers, plugins, channels, and local agent state.
- A target harness registry exporter that writes non-secret agent/provider/plugin/channel state with receipts.
- An activation readiness checker that validates registry/channel/runtime/Codex/logging prerequisites before cutover.
- A safe-copy import executor that copies planned non-sensitive state, skips raw secrets by default, backs up overwrite targets, and writes receipts.
- A JSONL operational log at `state/logs/harness.jsonl` for activation checks, channel ingress, queue prepare, and Codex completion events.
- A shared channel command parser and runtime-intent contract for OpenClaw-style DM commands.
- A skill-first indexer and deterministic task matcher for source, imported, and bundled harness operation skills.
- A turn planner that maps one inbound channel message to command handling, agent/session/model routing, imported channel command state, prompt files, and selected skills.
- A shared channel runtime bridge that maps one Telegram/Discord-style DM into either an immediate command reply or an agent-turn dispatch envelope.
- A deterministic channel command state writer for `/new`, `/think`, `/stop`, `/steer`, `/btw`, `/model`, and `/status` effects.
- A channel receive handler that turns one DM into either command state/outbox records or a queued agent turn.
- A channel run-once pipeline that handles one DM, runs the runtime when needed, and returns pending delivery work.
- A channel outbox delivery planner/receipt ledger for Telegram/Discord delivery retry.
- Telegram Bot API adapters: `telegram-probe` for non-consuming token/API readiness, `telegram-poll-once` for controlled smoke tests, and `telegram-loop` for continuous polling with bounded consecutive-error handling.
- A Discord REST outbox sender that delivers pending Discord replies, records delivery receipts, and logs delivery summaries.
- A durable runtime queue writer that appends channel agent turns to `state/runtime-queue/pending.jsonl` with receipts and planned transcript paths.
- A runtime queue prepare worker that reads pending items, assembles prompt bundles, and writes execution receipts before the Codex adapter is connected.
- A one-shot runtime pipeline that prepares, plans, runs Codex, records completion, and writes an agent reply to the shared channel outbox.
- A supervised runtime queue loop that drains queued work with idle exit, bounded consecutive-error handling, and a `loop-last.json` status file.
- A Codex runtime planner that turns a prepared queue execution into an inspectable `codex app-server` invocation plan and output-path contract.
- A Codex runtime preflight checker that validates the plan, executable, prompt files, output directories, and required environment variables before process start.
- A Codex runtime launch probe that starts the planned app-server process, sends no prompt or JSON-RPC request, then stops it and records process receipts/log paths.
- A Codex runtime runner that drives one prepared `codex app-server` JSONL turn, records stdout/stderr logs, and writes OpenClaw-compatible completion outputs.
- A Codex completion recorder that writes assistant output into OpenClaw-compatible transcript, trajectory, and Codex binding files.
- A prompt bundle assembler that turns an agent turn plan into inspectable OpenClaw context payloads and uses a per-session injection ledger to avoid repeating prompt files/skill bodies.
- A Windows supervisor planner that writes Task Scheduler install/start/stop/uninstall scripts for runtime, Telegram, and Discord loops without directly registering tasks.
- A native agent-turn cron parser and dry-run dispatch planner with cutover hold safety.
- A deterministic cron parser and no-LLM dry-run planner for workspace cron runners.
- A subagent ledger parser and dry-run planner for `/subagents/runs.json` cutover safety.
- A CLI crate with `doctor`, `import-plan`, `import-dry-run`, `import-execute`, `channel-credentials-export`, `registry`, `registry-export`, `enable-check`, `status`, `supervisor-plan`, `harness-skills-sync`, `skills`, `turn-plan`, `channel-step`, `channel-apply`, `channel-receive`, `channel-run-once`, `channel-outbox-plan`, `channel-delivery-record`, `telegram-probe`, `telegram-poll-once`, `telegram-loop`, `discord-outbox-send-once`, `discord-event-run-once`, `discord-gateway-probe`, `discord-gateway-loop`, `plugin-sidecar-probe`, `plugin-sidecar-call`, `queue-enqueue`, `queue-prepare`, `runtime-run-once`, `runtime-loop`, `codex-plan`, `codex-preflight`, `codex-launch-probe`, `codex-run`, `codex-complete`, `prompt-bundle`, `cron-plan`, `deterministic-cron-plan`, and `subagent-plan` commands.
- Minimal external crates for current scope: `serde`/`serde_json` for stable JSON reports and `ureq` for the first Telegram/Discord REST smoke adapters.

## Quick Start

```powershell
cargo test
cargo run -p openclaw-harness-cli -- doctor
cargo run -p openclaw-harness-cli -- import-plan --openclaw-home C:\path\to\.openclaw
cargo run -p openclaw-harness-cli -- import-dry-run --openclaw-home C:\path\to\.openclaw --target-home C:\path\to\.openclaw-harness --conflict skip --output imports\dry-run
cargo run -p openclaw-harness-cli -- import-execute --openclaw-home C:\path\to\.openclaw --target-home C:\path\to\.openclaw-harness --conflict skip
cargo run -p openclaw-harness-cli -- registry --openclaw-home C:\path\to\.openclaw
cargo run -p openclaw-harness-cli -- registry-export --openclaw-home C:\path\to\.openclaw --target-home C:\path\to\.openclaw-harness --conflict skip
cargo run -p openclaw-harness-cli -- channel-credentials-export --openclaw-home C:\path\to\.openclaw --target-home C:\path\to\.openclaw-harness --include-sensitive
cargo run -p openclaw-harness-cli -- telegram-probe --target-home C:\path\to\.openclaw-harness
cargo run -p openclaw-harness-cli -- harness-skills-sync --target-home C:\path\to\.openclaw-harness
cargo run -p openclaw-harness-cli -- enable-check --target-home C:\path\to\.openclaw-harness
cargo run -p openclaw-harness-cli -- status --target-home C:\path\to\.openclaw-harness --json
cargo run -p openclaw-harness-cli -- supervisor-plan --openclaw-home C:\path\to\.openclaw --target-home C:\path\to\.openclaw-harness --harness-cli C:\path\to\openclaw-harness.exe --codex-exe C:\path\to\codex.cmd --agent main
cargo run -p openclaw-harness-cli -- skills --openclaw-home C:\path\to\.openclaw --query "repair memory cron" --agent mem-cron --limit 3
cargo run -p openclaw-harness-cli -- skills --harness-home C:\path\to\.openclaw-harness --output imports\skills
cargo run -p openclaw-harness-cli -- turn-plan --openclaw-home C:\path\to\.openclaw --platform telegram --channel-id dm-123 --user-id user-456 --agent main --message "repair memory cron"
cargo run -p openclaw-harness-cli -- channel-step --openclaw-home C:\path\to\.openclaw --platform discord --channel-id dm-123 --user-id user-456 --agent main --message "/status channels" --output imports\channel
cargo run -p openclaw-harness-cli -- channel-apply --openclaw-home C:\path\to\.openclaw --target-home C:\path\to\.openclaw-harness --platform telegram --channel-id dm-123 --user-id user-456 --agent main --message "/model openrouter/anthropic/claude-sonnet-4"
cargo run -p openclaw-harness-cli -- channel-receive --openclaw-home C:\path\to\.openclaw --target-home C:\path\to\.openclaw-harness --platform telegram --channel-id dm-123 --user-id user-456 --agent main --message "continue with the selected model"
cargo run -p openclaw-harness-cli -- channel-run-once --openclaw-home C:\path\to\.openclaw --target-home C:\path\to\.openclaw-harness --platform telegram --channel-id dm-123 --user-id user-456 --agent main --message "continue with the selected model" --codex-exe C:\path\to\codex.exe
cargo run -p openclaw-harness-cli -- channel-outbox-plan --target-home C:\path\to\.openclaw-harness --platform telegram --limit 20
cargo run -p openclaw-harness-cli -- telegram-poll-once --openclaw-home C:\path\to\.openclaw --target-home C:\path\to\.openclaw-harness --agent main --codex-exe C:\path\to\codex.exe --poll-timeout-seconds 1 --max-updates 10
cargo run -p openclaw-harness-cli -- telegram-loop --openclaw-home C:\path\to\.openclaw --target-home C:\path\to\.openclaw-harness --agent main --codex-exe C:\path\to\codex.exe --iterations 0 --idle-ms 1000 --max-consecutive-errors 5
cargo run -p openclaw-harness-cli -- discord-outbox-send-once --target-home C:\path\to\.openclaw-harness --outbox-limit 20
cargo run -p openclaw-harness-cli -- turn-plan --openclaw-home C:\path\to\.openclaw --harness-home C:\path\to\.openclaw-harness --platform telegram --channel-id dm-123 --user-id user-456 --agent main --message "continue with the selected model"
cargo run -p openclaw-harness-cli -- queue-enqueue --openclaw-home C:\path\to\.openclaw --target-home C:\path\to\.openclaw-harness --platform telegram --channel-id dm-123 --user-id user-456 --agent main --message "repair memory cron"
cargo run -p openclaw-harness-cli -- queue-prepare --target-home C:\path\to\.openclaw-harness
cargo run -p openclaw-harness-cli -- runtime-run-once --target-home C:\path\to\.openclaw-harness --codex-exe C:\path\to\codex.exe --timeout-ms 300000
cargo run -p openclaw-harness-cli -- runtime-loop --target-home C:\path\to\.openclaw-harness --codex-exe C:\path\to\codex.exe --iterations 0 --idle-ms 1000 --max-consecutive-errors 5
cargo run -p openclaw-harness-cli -- codex-plan --target-home C:\path\to\.openclaw-harness --codex-exe C:\path\to\codex.exe
cargo run -p openclaw-harness-cli -- codex-preflight --target-home C:\path\to\.openclaw-harness
cargo run -p openclaw-harness-cli -- codex-launch-probe --target-home C:\path\to\.openclaw-harness --startup-probe-ms 750
cargo run -p openclaw-harness-cli -- codex-run --target-home C:\path\to\.openclaw-harness --timeout-ms 300000
cargo run -p openclaw-harness-cli -- codex-complete --target-home C:\path\to\.openclaw-harness --assistant-message "Smoke completion recorded."
cargo run -p openclaw-harness-cli -- prompt-bundle --openclaw-home C:\path\to\.openclaw --platform telegram --channel-id dm-123 --user-id user-456 --agent main --message "repair memory cron" --output imports\prompt
cargo run -p openclaw-harness-cli -- cron-plan --openclaw-home C:\path\to\.openclaw --output imports\cron
cargo run -p openclaw-harness-cli -- deterministic-cron-plan --workspace C:\path\to\workspace --output imports\deterministic-cron
cargo run -p openclaw-harness-cli -- subagent-plan --openclaw-home C:\path\to\.openclaw --output imports\subagents
```

If `cargo` is not visible in a newly opened terminal, restart the terminal or use:

```powershell
$env:PATH = "$env:USERPROFILE\.cargo\bin;$env:PATH"
```

## Current Direction

The recommended path is a Rust harness core that delegates native coding-agent execution to Codex app-server, keeps OpenClaw-compatible workspace/session/memory import semantics, and initially bridges OpenClaw plugins through a sidecar instead of reimplementing the full TypeScript plugin SDK.

Skills are first-class runtime state, not documentation leftovers. The importer preserves OpenClaw workspace skills, managed OpenClaw skills, and project `.agents/skills`; `harness-skills-sync` also seeds bundled harness operation skills under `skills/openclaw-harness-core/*` with a manifest so user-modified copies are not overwritten unless `--force` is explicit. The bundled harness operation skill is the versioned runbook for how agents should operate this harness, following the Hermes practice of keeping agent operating lead in skills that are updated with the harness instead of only in static docs. Runtime turn planning uses a merged skill index across source, imported, and bundled harness skills. Agent-created skill propose/patch/archive flows are still pending.

Codex remains the owner of the model system prompt, built-in tool schemas, MCP tools, sandbox, approvals, and session continuity. The Rust harness only builds the OpenClaw turn payload: runtime context, channel command state, imported prompt files, matched skills, and the inbound user message. When a harness home is available, `prompt-bundle` and `queue-prepare` use `state/prompt-injection-ledgers/<agent>/<session>.json` so the same session only receives unchanged prompt files or skill bodies once; later turns receive a compact continuity note and rely on the Codex backend session to retain prior context.

The first importer command is intentionally read-only. `import-dry-run` produces a structured migration report, flags conflicts, supports `skip`, `overwrite`, and `rename` policies, and can write `report.json` plus `summary.md` when `--output` is provided.

`import-execute` applies the same plan as safe copy. It copies prompt files, skills, agent directories, sessions, cron stores, subagent ledgers, memory snapshots, and plugin records when planned. Raw sensitive items are skipped by default, sensitive files inside copied directories are omitted, and `--include-sensitive` is required to copy raw config/auth/plugin-state. `overwrite` creates `.bak` receipts before replacing a destination.

The registry command is also read-only. It merges `openclaw.json` agent config with `/agents/<id>` directories and reports per-agent model/provider/workspace plus local session/auth/model state.

`registry-export` writes the first target harness state files under `state/harness-registry.json` and `state/harness-registry-receipts.json`. It records credential presence as metadata only; it does not copy raw API keys, tokens, or login state.

`channel-credentials-export` is the explicit channel-secret handoff path. It reads Telegram/Discord bot tokens plus known allow-list, chat, channel, and guild IDs from an existing OpenClaw `openclaw.json` and writes them to the target harness `secrets/channel-credentials.env` only when `--include-sensitive` is passed. Receipts in `secrets/channel-credentials-receipts.json` stay redacted and record names, source paths, lengths, and export status, not raw values.

`harness-skills-sync` writes the bundled `openclaw-windows-harness` skill and `.openclaw-harness-builtins.json` manifest into the target harness home. It follows the Hermes-style bundled-skill safety rule: current files are left alone, manifest-matched old files are updated, user-modified files are skipped unless `--force` is set.

`enable-check` is the formal cutover readiness report. It checks the exported registry, enabled agents, Telegram/Discord token presence when those channels are enabled, provider credentials, plugin sidecar blockers, runtime queue receipts, channel outbox/state, Telegram getMe probe evidence, Telegram offset state, Telegram/Discord adapter log evidence, Codex auth, memory-adapter status, and whether `state/logs/harness.jsonl` is writable. It appends an activation event to that log every time it runs.

`status` is the operator health summary for handoff and monitoring. It aggregates readiness, queued/open/prepared/completed runtime work, channel outbox delivery state, Telegram/Discord smoke evidence, memory backend presence, plugin sidecar receipts, and operational log event coverage. Use `--json` when a scheduled task or monitor needs machine-readable output.

`supervisor-plan` writes a Windows Task Scheduler handoff bundle under `state/supervisor/windows-scheduled-tasks` by default. It generates runner scripts for `runtime-loop`, `telegram-loop`, and `discord-gateway-loop`, plus install/start/stop/uninstall scripts and `supervisor-plan.json`. It uses stop files for graceful loop shutdown and writes absolute paths so tasks do not depend on the scheduler working directory. It does not register or start tasks by itself.

Runtime operations write an append-only JSONL operational log at `state/logs/harness.jsonl`. Current events include activation checks, Telegram getMe probes, Telegram poll-once summaries, `channel-receive`, `queue-prepare`, `runtime-run-once`, `runtime-loop`, `codex-run`, `codex-complete`, and channel delivery receipts, with level, component, event name, message, queue id, session key, agent/channel ids, and relevant paths. This complements receipts and transcript/trajectory files and is the file to tail for monitoring/debugging.

Telegram and Discord adapters should share the same channel command parser and intent mapper. Current parser coverage is `/new`, `/think`, `/stop`, `/steer`, `/btw`, `/model`, and `/status`; `/model` maps to show-or-switch model intents, and `/status` maps to scoped or global status intents.

`turn-plan` is the first runtime-facing dry run. It does not call a model or execute tools. It proves the shared pre-dispatch path: parse channel commands before ordinary messages, route to an OpenClaw agent, compute or inherit the active session key, surface provider/model policy, list prompt files, and select relevant skills for prompt assembly. When `--harness-home` is provided, it reads channel command state and selects from the merged runtime skill index so `/new`, `/think`, `/steer`, `/btw`, `/model`, and `/stop` can affect the next ordinary turn.

`channel-step` is the shared channel bridge that Telegram and Discord adapters should call after receiving a DM. It consumes the same turn plan and writes `channel-step.json`. Command turns such as `/status` and `/model` produce immediate outbound command replies plus typed command effects. Plain user messages produce an agent-turn dispatch envelope for the future runtime queue and no immediate model call.

`channel-apply` persists the command side of `channel-step`. It writes per-channel/user state under `state/channels/<platform>/<channel-id>/<user-id>/state.json`, appends command events to `events.jsonl`, appends receipts to `state/channels/command-apply-receipts.jsonl`, and returns the outbound command reply text. It handles `/new`, `/think`, `/stop`, `/steer`, `/btw`, `/model`, and `/status` without enqueueing an agent turn or calling a model. The next `turn-plan`, `queue-enqueue`, and `queue-prepare` pass can read that state and apply active session/model override plus steering/think/btw context.

`channel-receive` is the single-message ingress contract for future Telegram and Discord adapters. It builds the same channel step, applies command turns into channel state and `state/channels/outbox.jsonl`, or queues ordinary agent turns into `state/runtime-queue/pending.jsonl`. It writes `state/channels/receive-receipts.jsonl` for retry/audit and never calls a model directly.

`channel-run-once` is the single-message smoke and future adapter entrypoint. It calls `channel-receive`, runs `runtime-run-once` for ordinary agent turns, then returns a `channel-outbox-plan` view for delivery. Command messages never enter the model path; ordinary messages may start `codex app-server` and make a model request. For offline smoke, pass `--codex-exe tools\openclaw-fake-codex-app-server\fake-codex-app-server.cmd` to exercise prompt assembly, runtime receipts, transcript/trajectory writes, and outbox generation without a model request.

`channel-outbox-plan` reads `state/channels/outbox.jsonl`, filters by platform, excludes delivered messages, and returns retryable pending messages with stable delivery ids, attempt counts, and last delivery status. `channel-delivery-record` appends delivered/failed receipts to `state/channels/delivery-receipts.jsonl` and logs delivery events. Telegram and Discord adapters should use this shared ledger so command replies and agent replies follow the same retry/audit path.

`telegram-probe` is the non-consuming Telegram readiness check. It reads `TELEGRAM_BOT_TOKEN` from the environment or `secrets/channel-credentials.env`, calls Bot API `getMe`, writes `state/channels/telegram-probe.json`, appends `state/channels/telegram-probe-receipts.jsonl`, and logs `telegram.probe`. It does not call `getUpdates` and does not send messages.

`telegram-poll-once` is the first real Telegram Bot API smoke adapter. It reads `TELEGRAM_BOT_TOKEN` from the environment or `secrets/channel-credentials.env`, stores the next update offset at `state/channels/telegram-offset.json`, normalizes text updates into `channel-run-once`, delivers pending Telegram outbox messages through `sendMessage`, records delivery receipts, and logs a `telegram.poll-once` summary.

`telegram-loop` wraps the same poll-once core in a continuous polling loop with `--iterations`, `--idle-ms`, `--max-consecutive-errors`, and optional `--stop-file`. Use `--iterations 1` or another finite count for controlled tests and `--iterations 0` for an operator-run handoff loop.

`discord-outbox-send-once` is the first Discord delivery adapter. It reads `DISCORD_BOT_TOKEN` from the environment or `secrets/channel-credentials.env`, sends pending `platform=discord` outbox messages through Discord's channel message REST endpoint, records delivery receipts, and logs a `discord.outbox-send-once` summary.

`discord-event-run-once` normalizes one Discord Gateway `MESSAGE_CREATE` event into the shared channel pipeline. `discord-gateway-probe` and `discord-gateway-loop` provide the Node WebSocket receive wrapper for operator-run live handoff. The gateway loop accepts `--stop-file` through the CLI wrapper and closes the WebSocket cleanly when the file appears.

`queue-enqueue` persists the agent-turn side of `channel-step`. It appends queued turns to `state/runtime-queue/pending.jsonl`, appends every queued/skipped attempt to `state/runtime-queue/receipts.jsonl`, and precomputes OpenClaw-compatible transcript and trajectory paths under `agents/<agent-id>/sessions/`. Command-only channel steps are recorded as skipped receipts and are not sent to the agent queue.

`queue-prepare` reads one queued runtime item, rebuilds the turn context from its stored source/workspace/session metadata, assembles `prompt-bundle.json` plus `prompt.md` under `state/runtime-queue/executions/<queue-id>/`, and writes `execution-receipt.json` plus `execution-receipts.jsonl`. It uses the merged runtime skill index and the prompt injection ledger, so unchanged prompt files and skill bodies are not repeated in the same session. It treats existing `Prepared` receipts as idempotence state, skips already prepared queue ids during automatic selection, and returns `AlreadyPrepared` when an operator explicitly requests a prepared `--queue-id`. This is the handoff point for the future Codex app-server worker; it does not call a model yet.

`runtime-run-once` is the first worker-facing pipeline. It calls `queue-prepare`, `codex-plan`, and `codex-run` for one queued or already prepared item, writes `state/runtime-queue/run-once-last.json`, appends `run-once-receipts.jsonl`, and writes a `kind=agent-reply` message to `state/channels/outbox.jsonl` when a fresh assistant reply is recorded. This is the core function a Telegram or Discord adapter can call after enqueueing a normal DM. If `codex-run` reports an already recorded completion, it skips the outbox write to avoid duplicate delivery.

`runtime-loop` wraps `runtime-run-once` for operator handoff or a Windows scheduled-task wrapper. It drains queued runtime work until `--iterations` is reached, `--stop-when-idle` sees an idle queue, `--stop-file` appears, or `--max-consecutive-errors` is exceeded. `NoWork`, `NoPreparedExecution`, and already recorded completions are treated as idle for loop control so the worker does not repeatedly process the latest completed execution. It writes `state/runtime-queue/loop-last.json` and appends `runtime.loop-stopped` plus `runtime.loop-error` events to `state/logs/harness.jsonl`.

`codex-plan` reads the latest prepared execution or an explicit `--execution-dir`, writes `codex-runtime-plan.json` plus `codex-runtime-receipt.json`, and appends `codex-runtime-receipts.jsonl`. It plans a stdio `codex app-server` invocation, model/env requirements, and OpenClaw-compatible transcript/trajectory/Codex binding output paths. It still does not start Codex or make a model request.

`codex-preflight` reads the latest `codex-runtime-plan.json`, or an explicit `--execution-dir`/`--plan-file`, and writes `codex-runtime-preflight.json` plus `codex-runtime-preflight-receipts.jsonl`. It checks that the Codex executable can be resolved, prompt files exist, output parents stay under the harness home and are writable, and provider credentials are present. OpenAI/Codex routes accept either `OPENAI_API_KEY` or local Codex OAuth auth state; OpenRouter routes still require `OPENROUTER_API_KEY`. It still does not start Codex or make a model request.

`codex-launch-probe` re-runs preflight, starts the planned app-server process only when preflight is ready, sends no JSON-RPC request and no prompt, waits for `--startup-probe-ms`, then terminates and waits for the child process. It writes `codex-runtime-launch-probe.json`, appends `codex-runtime-launch-receipts.jsonl`, and keeps stdout/stderr logs under the prepared execution directory. This proves process supervision before the worker starts real model-backed turns.

`codex-run` re-runs preflight, skips the model request if a completion receipt already exists, otherwise starts `codex app-server`, sends `initialize`, `initialized`, `thread/start`, and `turn/start` over JSONL stdio, captures assistant message deltas, waits for `turn/completed`, and then calls the deterministic completion sink. It writes `codex-runtime-run.json`, appends `codex-runtime-run-receipts.jsonl`, and keeps raw app-server stdout/stderr logs under the prepared execution directory. The harness sends the assembled OpenClaw turn payload as user input; Codex remains responsible for its own system prompt, built-in tool schemas, MCP/tool inventory, approvals, and session continuity.

`codex-complete` records an assistant message into the output contract from `codex-plan`. It reads `codex-runtime-plan.json`, copies the inbound user message from `prompt-bundle.json`, appends user/assistant entries to the planned transcript JSONL, appends trajectory events, writes the Codex binding mirror, writes `codex-runtime-completion-receipt.json`, and appends `codex-runtime-completion-receipts.jsonl`. This is the deterministic completion sink that the future JSON-RPC app-server adapter should call after it receives a real model response.

`prompt-bundle` consumes the same turn plan and assembles the OpenClaw turn payload that a Codex runtime adapter will eventually send: runtime context, imported channel command state when available, existing OpenClaw prompt files, selected `SKILL.md` bodies, continuity notes, and the inbound message. It writes `prompt-bundle.json` and `prompt.md`, uses per-file byte caps, and when `--harness-home` is provided updates the per-session injection ledger.

Cron import has two separate lanes: OpenClaw native agent-turn cron under `.openclaw/cron`, and deterministic workspace cron runners under `workspace/tools/cron-runner` plus `workspace/tools/backup-cron-runner`. The Rust harness must keep those paths separate because only the native lane is allowed to enqueue LLM-backed agent turns.

`cron-plan` covers the native lane only. It reads `.openclaw/cron/jobs.json` plus `jobs-state.json`, validates agent ids against the imported registry, extracts agent-turn message text where possible, and writes `native-cron-plan.json`. By default, enabled jobs are held under cutover safety; `--resume-cron` must be explicit before the dry-run marks due one-shot jobs as enqueueable or cron expressions as registered for scheduler evaluation.

`deterministic-cron-plan` covers the workspace runner lane only. It scans `workspace/tools/cron-runner` and `workspace/tools/backup-cron-runner`, parses crontab entries, resolves `jobs/*` scripts, and writes `deterministic-cron-plan.json` with `llmAccessAllowed=false`. By default all commands are held; `--allow-deterministic-run` only changes dry-run classification into ready/missing/script-compatibility states and does not execute anything.

`subagent-plan` reads `.openclaw/subagents/runs.json` and writes `subagent-plan.json`. Completed, failed, and canceled runs stay historical no-ops. Queued and running runs are held by default to avoid duplicate worker execution during gateway handoff; `--resume-subagents` only marks them as resume candidates in the dry-run plan and does not start a worker.

This workspace disables Codex-side `openclaw-mem` gateway lookups through [AGENTS.md](AGENTS.md). The harness product requirement still includes importing existing OpenClaw memory files/databases and supporting memory adapters when enabled.

Memory import treats `memory/qdrant-edge` as the primary backend when present. `openclaw-mem.sqlite`, memory JSONL files, and Markdown memory are still imported as snapshot/audit sources; LanceDB is treated as backup/optional unless the active source config points to it.

The cutover checklist is tracked in [Activation Readiness Plan](docs/activation-readiness-plan.md).

See [Project Assessment](docs/project-assessment.md).
