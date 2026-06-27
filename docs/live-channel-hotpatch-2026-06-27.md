# Live Channel Hotpatch - 2026-06-27

## Scope

This hotpatch fixes the live channel turn path for Discord and Telegram DM ingress and keeps independent agents from inheriting shared prompt workspace instructions by accident.

The user-visible failures were:

- Discord and Telegram DM turns reached the runtime queue but failed before Codex app-server execution.
- The app-server reported `AbsolutePathBuf deserialized without a base path` for relative runtime paths.
- The independent `xiaoxiaoli` agent was planned from the shared source workspace, so shared `AGENTS.md` text could be injected into an independent agent turn.

## Root Cause

There were two related defects:

- Supervisor reconcile could launch live child loops without explicit `--workspace` and `--runtime-workspace`, leaving runtime plans with relative app-server paths.
- Turn planning selected prompt files from `AgentSource.workspace` after choosing the agent. Directory-backed non-main agents therefore used the shared source workspace unless they were explicitly configured elsewhere.

## Changes

- `codex_runtime` now absolutizes prepared execution paths, prompt bundle paths, prompt markdown paths, runtime workspace paths, prompt source workspace fallback paths, and execution-dir fallback paths before writing app-server invocation plans.
- `agent-harness-cli` supervisor reconcile now infers the live workspace from the harness/source home parent when `--workspace` or `--runtime-workspace` is omitted, and normalizes explicit workspace arguments to absolute paths.
- `turns` now routes prompt files through the selected agent:
  - `main` keeps the existing shared workspace behavior and imported-workspace fallback.
  - Directory-backed non-main agents use their isolated workspace candidate.
  - If an independent agent has no prompt files in its isolated workspace, the turn records a warning and does not fall back to shared source prompt files.

## Validation

Staging validation passed:

- `cargo fmt --all -- --check`
- `cargo check --workspace`
- `cargo test -p agent-harness-core`: 445 unit tests, 5 integration tests, and doc tests passed
- `cargo test -p agent-harness-cli`: 54 tests passed
- `cargo build -p agent-harness-cli`
- Focused regressions:
  - `plan_codex_runtime_absolutizes_relative_app_server_paths`
  - `supervisor_reconcile_defaults_live_workspace_to_harness_parent`
  - independent-agent prompt isolation turn-plan tests
- Synthetic channel smoke with a fake Codex app-server:
  - Discord DM main agent completed to outbox
  - Telegram DM main agent completed to outbox
  - Telegram DM `xiaoxiaoli` completed to outbox
  - Runtime app-server paths were absolute
  - `xiaoxiaoli` prompt included only its independent workspace marker and did not include the shared workspace marker
- Public hygiene:
  - `docs`: `forbiddenHits=[]`
  - public export tree: `forbiddenHits=[]`

Candidate binary:

- `target/staging-live-hotpatch-combined-build/debug/agent-harness.exe`
- SHA-256: `1A636AD1E2339BCA4155F465245A3C326000E2250C3569D9DAFA7B32F8643F6D`

## Cutover Notes

The live cutover should preserve the previous live binary as rollback material, replace only the live `agent-harness.exe` with the staged candidate, ensure `xiaoxiaoli` has an isolated live workspace directory, and restart/reconcile the live gateway loops from the new binary.

After cutover, verify:

- Discord DM main agent queues and completes a turn.
- Telegram DM main agent queues and completes a turn.
- Telegram DM `xiaoxiaoli` queues and completes a turn with source workspace under its own agent directory.
