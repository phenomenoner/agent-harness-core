# Rust OpenClaw Core

Minimal Rust/Windows agent harness inspired by OpenClaw.

The project starts with a small, testable foundation:

- Import planning for an existing OpenClaw home directory.
- A core crate with data-layout detection logic for config, workspace prompts, agents, skills, sessions, native cron, deterministic cron, subagents, memory, and plugins.
- A read-only importer dry-run report with Hermes-style conflict policy and receipts.
- A CLI crate with `doctor`, `import-plan`, and `import-dry-run` commands.
- Minimal external crates: `serde` and `serde_json` for stable report/config/session JSON handling.

## Quick Start

```powershell
cargo test
cargo run -p openclaw-harness-cli -- doctor
cargo run -p openclaw-harness-cli -- import-plan --openclaw-home C:\path\to\.openclaw
cargo run -p openclaw-harness-cli -- import-dry-run --openclaw-home C:\path\to\.openclaw --target-home C:\path\to\.openclaw-harness --conflict skip --output imports\dry-run
```

If `cargo` is not visible in a newly opened terminal, restart the terminal or use:

```powershell
$env:PATH = "$env:USERPROFILE\.cargo\bin;$env:PATH"
```

## Current Direction

The recommended path is a Rust harness core that delegates native coding-agent execution to Codex app-server, keeps OpenClaw-compatible workspace/session/memory import semantics, and initially bridges OpenClaw plugins through a sidecar instead of reimplementing the full TypeScript plugin SDK.

Skills are first-class runtime state, not documentation leftovers. The importer should preserve OpenClaw workspace skills, managed OpenClaw skills, and project `.agents/skills`, then expose them through a progressive disclosure index so agents can discover, write, patch, and reference task skills before falling back to broad prompt context.

The first importer command is intentionally read-only. `import-dry-run` produces a structured migration report, flags conflicts, supports `skip`, `overwrite`, and `rename` policies, and can write `report.json` plus `summary.md` when `--output` is provided.

Cron import has two separate lanes: OpenClaw native agent-turn cron under `.openclaw/cron`, and deterministic workspace cron runners under `workspace/tools/cron-runner` plus `workspace/tools/backup-cron-runner`. The Rust harness must keep those paths separate because only the native lane is allowed to enqueue LLM-backed agent turns.

See [Project Assessment](docs/project-assessment.md).
