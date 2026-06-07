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
- A turn planner that maps one inbound channel message to command handling, agent/session/model routing, prompt files, and selected skills.
- A CLI crate with `doctor`, `import-plan`, `import-dry-run`, `import-execute`, `registry`, `registry-export`, `skills`, and `turn-plan` commands.
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

`turn-plan` is the first runtime-facing dry run. It does not call a model or execute tools. It proves the shared pre-dispatch path: parse channel commands before ordinary messages, route to an OpenClaw agent, compute a stable session key, surface provider/model policy, list prompt files, and select relevant skills for prompt assembly.

Cron import has two separate lanes: OpenClaw native agent-turn cron under `.openclaw/cron`, and deterministic workspace cron runners under `workspace/tools/cron-runner` plus `workspace/tools/backup-cron-runner`. The Rust harness must keep those paths separate because only the native lane is allowed to enqueue LLM-backed agent turns.

This workspace disables Codex-side `openclaw-mem` gateway lookups through [AGENTS.md](AGENTS.md). The harness product requirement still includes importing existing OpenClaw memory files/databases and supporting memory adapters when enabled.

See [Project Assessment](docs/project-assessment.md).
