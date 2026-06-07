# Rust OpenClaw Core

Minimal Rust/Windows agent harness inspired by OpenClaw.

The project starts with a small, testable foundation:

- Import planning for an existing OpenClaw home directory.
- A core crate with data-layout detection logic.
- A CLI crate with `doctor` and `import-plan` commands.
- No external crates yet, so the first build works immediately after installing Rust.

## Quick Start

```powershell
cargo test
cargo run -p openclaw-harness-cli -- doctor
cargo run -p openclaw-harness-cli -- import-plan --openclaw-home C:\path\to\.openclaw
```

If `cargo` is not visible in a newly opened terminal, restart the terminal or use:

```powershell
$env:PATH = "$env:USERPROFILE\.cargo\bin;$env:PATH"
```

## Current Direction

The recommended path is a Rust harness core that delegates native coding-agent execution to Codex app-server, keeps OpenClaw-compatible workspace/session/memory import semantics, and initially bridges OpenClaw plugins through a sidecar instead of reimplementing the full TypeScript plugin SDK.

See [Project Assessment](docs/project-assessment.md).
