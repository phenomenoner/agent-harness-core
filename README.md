# Rust OpenClaw Core

Minimal Rust/Windows agent harness inspired by OpenClaw.

The project starts with a small, testable foundation:

- Import planning for an existing OpenClaw home directory.
- A core crate with data-layout detection logic for config, workspace prompts, agents, skills, sessions, native cron, deterministic cron, subagents, memory, and plugins.
- A read-only importer dry-run report with Hermes-style conflict policy and receipts.
- A read-only multi-agent registry parser for OpenClaw agents, providers, plugins, channels, and local agent state.
- A target harness registry exporter that writes non-secret agent/provider/plugin/channel state with receipts.
- A safe-copy import executor that copies planned non-sensitive state, skips raw secrets by default, backs up overwrite targets, and writes receipts.
- A shared channel command parser and runtime-intent contract for OpenClaw-style DM commands.
- A skill-first indexer and deterministic task matcher for imported or source OpenClaw skills.
- A turn planner that maps one inbound channel message to command handling, agent/session/model routing, imported channel command state, prompt files, and selected skills.
- A shared channel runtime bridge that maps one Telegram/Discord-style DM into either an immediate command reply or an agent-turn dispatch envelope.
- A deterministic channel command state writer for `/new`, `/think`, `/stop`, `/steer`, `/btw`, `/model`, and `/status` effects.
- A durable runtime queue writer that appends channel agent turns to `state/runtime-queue/pending.jsonl` with receipts and planned transcript paths.
- A runtime queue prepare worker that reads pending items, assembles prompt bundles, and writes execution receipts before the Codex adapter is connected.
- A Codex runtime planner that turns a prepared queue execution into an inspectable `codex app-server` invocation plan and output-path contract.
- A Codex runtime preflight checker that validates the plan, executable, prompt files, output directories, and required environment variables before process start.
- A Codex runtime launch probe that starts the planned app-server process, sends no prompt or JSON-RPC request, then stops it and records process receipts/log paths.
- A Codex completion recorder that writes assistant output into OpenClaw-compatible transcript, trajectory, and Codex binding files.
- A prompt bundle assembler that turns an agent turn plan into inspectable prompt files, selected skill bodies, and the inbound message.
- A native agent-turn cron parser and dry-run dispatch planner with cutover hold safety.
- A deterministic cron parser and no-LLM dry-run planner for workspace cron runners.
- A subagent ledger parser and dry-run planner for `/subagents/runs.json` cutover safety.
- A CLI crate with `doctor`, `import-plan`, `import-dry-run`, `import-execute`, `registry`, `registry-export`, `skills`, `turn-plan`, `channel-step`, `channel-apply`, `queue-enqueue`, `queue-prepare`, `codex-plan`, `codex-preflight`, `codex-launch-probe`, `codex-complete`, `prompt-bundle`, `cron-plan`, `deterministic-cron-plan`, and `subagent-plan` commands.
- Minimal external crates: `serde` and `serde_json` for stable report/config/session JSON handling.

## Quick Start

```powershell
cargo test
cargo run -p openclaw-harness-cli -- doctor
cargo run -p openclaw-harness-cli -- import-plan --openclaw-home C:\path\to\.openclaw
cargo run -p openclaw-harness-cli -- import-dry-run --openclaw-home C:\path\to\.openclaw --target-home C:\path\to\.openclaw-harness --conflict skip --output imports\dry-run
cargo run -p openclaw-harness-cli -- import-execute --openclaw-home C:\path\to\.openclaw --target-home C:\path\to\.openclaw-harness --conflict skip
cargo run -p openclaw-harness-cli -- registry --openclaw-home C:\path\to\.openclaw
cargo run -p openclaw-harness-cli -- registry-export --openclaw-home C:\path\to\.openclaw --target-home C:\path\to\.openclaw-harness --conflict skip
cargo run -p openclaw-harness-cli -- skills --openclaw-home C:\path\to\.openclaw --query "repair memory cron" --agent mem-cron --limit 3
cargo run -p openclaw-harness-cli -- skills --harness-home C:\path\to\.openclaw-harness --output imports\skills
cargo run -p openclaw-harness-cli -- turn-plan --openclaw-home C:\path\to\.openclaw --platform telegram --channel-id dm-123 --user-id user-456 --agent main --message "repair memory cron"
cargo run -p openclaw-harness-cli -- channel-step --openclaw-home C:\path\to\.openclaw --platform discord --channel-id dm-123 --user-id user-456 --agent main --message "/status channels" --output imports\channel
cargo run -p openclaw-harness-cli -- channel-apply --openclaw-home C:\path\to\.openclaw --target-home C:\path\to\.openclaw-harness --platform telegram --channel-id dm-123 --user-id user-456 --agent main --message "/model openrouter/anthropic/claude-sonnet-4"
cargo run -p openclaw-harness-cli -- turn-plan --openclaw-home C:\path\to\.openclaw --harness-home C:\path\to\.openclaw-harness --platform telegram --channel-id dm-123 --user-id user-456 --agent main --message "continue with the selected model"
cargo run -p openclaw-harness-cli -- queue-enqueue --openclaw-home C:\path\to\.openclaw --target-home C:\path\to\.openclaw-harness --platform telegram --channel-id dm-123 --user-id user-456 --agent main --message "repair memory cron"
cargo run -p openclaw-harness-cli -- queue-prepare --target-home C:\path\to\.openclaw-harness
cargo run -p openclaw-harness-cli -- codex-plan --target-home C:\path\to\.openclaw-harness --codex-exe C:\path\to\codex.exe
cargo run -p openclaw-harness-cli -- codex-preflight --target-home C:\path\to\.openclaw-harness
cargo run -p openclaw-harness-cli -- codex-launch-probe --target-home C:\path\to\.openclaw-harness --startup-probe-ms 750
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

Skills are first-class runtime state, not documentation leftovers. The importer preserves OpenClaw workspace skills, managed OpenClaw skills, and project `.agents/skills`; the `skills` command can index those source directories or the imported `skills/openclaw-imports/*` namespace and match task-relevant skills before prompt assembly. Agent-created skill propose/patch/archive flows are still pending.

The first importer command is intentionally read-only. `import-dry-run` produces a structured migration report, flags conflicts, supports `skip`, `overwrite`, and `rename` policies, and can write `report.json` plus `summary.md` when `--output` is provided.

`import-execute` applies the same plan as safe copy. It copies prompt files, skills, agent directories, sessions, cron stores, subagent ledgers, memory snapshots, and plugin records when planned. Raw sensitive items are skipped by default, sensitive files inside copied directories are omitted, and `--include-sensitive` is required to copy raw config/auth/plugin-state. `overwrite` creates `.bak` receipts before replacing a destination.

The registry command is also read-only. It merges `openclaw.json` agent config with `/agents/<id>` directories and reports per-agent model/provider/workspace plus local session/auth/model state.

`registry-export` writes the first target harness state files under `state/harness-registry.json` and `state/harness-registry-receipts.json`. It records credential presence as metadata only; it does not copy raw API keys, tokens, or login state.

Telegram and Discord adapters should share the same channel command parser and intent mapper. Current parser coverage is `/new`, `/think`, `/stop`, `/steer`, `/btw`, `/model`, and `/status`; `/model` maps to show-or-switch model intents, and `/status` maps to scoped or global status intents.

`turn-plan` is the first runtime-facing dry run. It does not call a model or execute tools. It proves the shared pre-dispatch path: parse channel commands before ordinary messages, route to an OpenClaw agent, compute or inherit the active session key, surface provider/model policy, list prompt files, and select relevant skills for prompt assembly. When `--harness-home` is provided, it reads channel command state so `/new`, `/think`, `/steer`, `/btw`, `/model`, and `/stop` can affect the next ordinary turn.

`channel-step` is the shared channel bridge that Telegram and Discord adapters should call after receiving a DM. It consumes the same turn plan and writes `channel-step.json`. Command turns such as `/status` and `/model` produce immediate outbound command replies plus typed command effects. Plain user messages produce an agent-turn dispatch envelope for the future runtime queue and no immediate model call.

`channel-apply` persists the command side of `channel-step`. It writes per-channel/user state under `state/channels/<platform>/<channel-id>/<user-id>/state.json`, appends command events to `events.jsonl`, appends receipts to `state/channels/command-apply-receipts.jsonl`, and returns the outbound command reply text. It handles `/new`, `/think`, `/stop`, `/steer`, `/btw`, `/model`, and `/status` without enqueueing an agent turn or calling a model. The next `turn-plan`, `queue-enqueue`, and `queue-prepare` pass can read that state and apply active session/model override plus steering/think/btw context.

`queue-enqueue` persists the agent-turn side of `channel-step`. It appends queued turns to `state/runtime-queue/pending.jsonl`, appends every queued/skipped attempt to `state/runtime-queue/receipts.jsonl`, and precomputes OpenClaw-compatible transcript and trajectory paths under `agents/<agent-id>/sessions/`. Command-only channel steps are recorded as skipped receipts and are not sent to the agent queue.

`queue-prepare` reads one queued runtime item, rebuilds the turn context from its stored source/workspace/session metadata, assembles `prompt-bundle.json` plus `prompt.md` under `state/runtime-queue/executions/<queue-id>/`, and writes `execution-receipt.json` plus `execution-receipts.jsonl`. It treats existing `Prepared` receipts as idempotence state, skips already prepared queue ids during automatic selection, and returns `AlreadyPrepared` when an operator explicitly requests a prepared `--queue-id`. This is the handoff point for the future Codex app-server worker; it does not call a model yet.

`codex-plan` reads the latest prepared execution or an explicit `--execution-dir`, writes `codex-runtime-plan.json` plus `codex-runtime-receipt.json`, and appends `codex-runtime-receipts.jsonl`. It plans a stdio `codex app-server` invocation, model/env requirements, and OpenClaw-compatible transcript/trajectory/Codex binding output paths. It still does not start Codex or make a model request.

`codex-preflight` reads the latest `codex-runtime-plan.json`, or an explicit `--execution-dir`/`--plan-file`, and writes `codex-runtime-preflight.json` plus `codex-runtime-preflight-receipts.jsonl`. It checks that the Codex executable can be resolved, prompt files exist, output parents stay under the harness home and are writable, and provider credentials are present. OpenAI/Codex routes accept either `OPENAI_API_KEY` or local Codex OAuth auth state; OpenRouter routes still require `OPENROUTER_API_KEY`. It still does not start Codex or make a model request.

`codex-launch-probe` re-runs preflight, starts the planned app-server process only when preflight is ready, sends no JSON-RPC request and no prompt, waits for `--startup-probe-ms`, then terminates and waits for the child process. It writes `codex-runtime-launch-probe.json`, appends `codex-runtime-launch-receipts.jsonl`, and keeps stdout/stderr logs under the prepared execution directory. This proves process supervision before the worker starts real model-backed turns.

`codex-complete` records an assistant message into the output contract from `codex-plan`. It reads `codex-runtime-plan.json`, copies the inbound user message from `prompt-bundle.json`, appends user/assistant entries to the planned transcript JSONL, appends trajectory events, writes the Codex binding mirror, writes `codex-runtime-completion-receipt.json`, and appends `codex-runtime-completion-receipts.jsonl`. This is the deterministic completion sink that the future JSON-RPC app-server adapter should call after it receives a real model response.

`prompt-bundle` consumes the same turn plan and assembles the prompt context that a Codex runtime adapter will eventually send: runtime context, imported channel command state when available, existing OpenClaw prompt files, selected `SKILL.md` bodies, and the inbound message. It writes `prompt-bundle.json` and `prompt.md`, and uses per-file byte caps so oversized imported state can be inspected safely.

Cron import has two separate lanes: OpenClaw native agent-turn cron under `.openclaw/cron`, and deterministic workspace cron runners under `workspace/tools/cron-runner` plus `workspace/tools/backup-cron-runner`. The Rust harness must keep those paths separate because only the native lane is allowed to enqueue LLM-backed agent turns.

`cron-plan` covers the native lane only. It reads `.openclaw/cron/jobs.json` plus `jobs-state.json`, validates agent ids against the imported registry, extracts agent-turn message text where possible, and writes `native-cron-plan.json`. By default, enabled jobs are held under cutover safety; `--resume-cron` must be explicit before the dry-run marks due one-shot jobs as enqueueable or cron expressions as registered for scheduler evaluation.

`deterministic-cron-plan` covers the workspace runner lane only. It scans `workspace/tools/cron-runner` and `workspace/tools/backup-cron-runner`, parses crontab entries, resolves `jobs/*` scripts, and writes `deterministic-cron-plan.json` with `llmAccessAllowed=false`. By default all commands are held; `--allow-deterministic-run` only changes dry-run classification into ready/missing/script-compatibility states and does not execute anything.

`subagent-plan` reads `.openclaw/subagents/runs.json` and writes `subagent-plan.json`. Completed, failed, and canceled runs stay historical no-ops. Queued and running runs are held by default to avoid duplicate worker execution during gateway handoff; `--resume-subagents` only marks them as resume candidates in the dry-run plan and does not start a worker.

This workspace disables Codex-side `openclaw-mem` gateway lookups through [AGENTS.md](AGENTS.md). The harness product requirement still includes importing existing OpenClaw memory files/databases and supporting memory adapters when enabled.

See [Project Assessment](docs/project-assessment.md).
