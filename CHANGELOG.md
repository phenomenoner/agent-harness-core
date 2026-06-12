# Changelog

## Unreleased

### Changed

- Reworked `README.md` into a public-facing overview (positioning, architecture diagram, CLI family table, FAQ, dual-license section); moved the internal live-validation, topology, full command walkthrough, and capability ledger content verbatim to `docs/agent-harness-operations-handbook.md`, now referenced from `AGENTS.md` as the session entry point.
- Replaced the condensed `LICENSE-APACHE` text with the canonical Apache License 2.0 text so GitHub license detection no longer reports "Other".
- Fixed the placeholder workspace `repository` URL in `Cargo.toml` and added crate `description` metadata.
- Added root `DOC-GUIDELINES.md` documentation writing guideline, linked from the README documentation table and the operations handbook documentation map.
- Isolated OpenRouter Codex config into provider-specific Codex homes and added a readiness failure when the shared default Codex/OAuth home contains stale OpenRouter provider config.
- Treat Codex app-server protocol errors and failed `turn/completed` events as terminal runtime failures instead of successful empty assistant replies.
- Updated builtin harness ops skill, release checklist, operations docs, and feature parity docs so stale guidance review covers docs, skills, and CLI help during future behavior-changing upgrades.

### Added

- Staging roadmap implementation for P0-P7, Track T, Track M, P6, and P7 direct-code paths.
- Fail-closed `harness-config.json` validation integrated into activation, worker dispatch, and Codex runtime planning.
- Runtime retry/dead-letter statuses and receipts for timeout exhaustion.
- Harness log rotation with rotation receipts.
- Runtime queue retry/skip control that preserves terminal-state immutability.
- Supervision evaluator with heartbeat stale detection, restart backoff, crash-loop breaker, and receipts.
- Local `healthz`, trace reconstruction, and metrics reports.
- Deploy canary receipt model with fake-only and optional live canary decisions.
- SQLite queue shadow compare and background task registry.
- Admission control and scoped stop receipts.
- Token efficiency and prompt-reduction gates.
- Task entities, SQLite budget counters, config drift checks, and learning proposal/quarantine receipts.
- Minimal in-process MCP JSON-RPC request handler plus CLI single-request gate.
- ContextPack validation, memory ingest idempotency, and MCP/tool description pinning helpers.
- Repo-local encrypted vault using PBKDF2-HMAC-SHA256 and ChaCha20-Poly1305.
- Security scan helpers for prompt boundary markers and shell allowed-root checks.
- Invariants catalog, schema registry, release checklist, trust-boundary documentation, atomic-write audit, and security policy.
- Operator CLI commands for the new staging gates.
- Harness secret-env handoff for provider-specific app-server child processes.

### Verification

- `cargo fmt --all`
- `cargo test -p agent-harness-cli --target-dir target\staging-test-cli`
- `cargo test --workspace --target-dir target\staging-test-workspace`
- `cargo build --workspace --target-dir target\deploy-build`
- `agent-harness public-hygiene --root target\staging-public-hygiene\public-export`
- `agent-harness status --target-home .\.agent-harness --json`
- `agent-harness healthz --target-home .\.agent-harness --require-writable-state`
- `cargo tree --workspace --duplicates`

### Pending Live Evidence

- Seven-day queue shadow parity summary.
- WinSW/service-wrapper restart, ordered shutdown, and reboot proof.
- Telegram/Discord/OpenRouter live smoke receipts.
- Live secret migration/rotation into the encrypted vault.
- Network-backed dependency advisory audit.
