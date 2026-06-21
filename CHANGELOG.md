# Changelog

## Unreleased

No changes yet.

## v0.1.1 - 2026-06-21

### Changed

- Stabilized Round8 gateway loop recovery: runtime queue leases now reap definitely dead legacy `owner="pid:<n>"` owners before queue selection/capacity checks, write `stale-owner-reaped` evidence, and emit a durable `lease-acquired` receipt before execution artifacts are prepared.
- Made loop heartbeat writes atomic and surfaced corrupt/NUL heartbeat files through explicit `corrupt` / `parseError` status and health fields; `healthz` now warns for degraded progress delivery without marking final reply delivery not live when runtime, ingress, and final outbox loops are otherwise healthy.
- Bounded progress delivery planning with a persisted progress-ledger byte cursor plus compacted per-queue cached state, preserving first/terminal events while coalescing repeated low-value cached events.
- Changed generated long-running Windows runner scripts to write process streams directly to per-loop log files instead of using `Tee-Object`; `ops-control stop` now writes structured JSON stop files while preserving legacy plain-text stop-file readability.
- Reworked `README.md` into a public-facing overview (positioning, architecture diagram, CLI family table, FAQ, dual-license section); moved the internal live-validation, topology, full command walkthrough, and capability ledger content verbatim to `docs/agent-harness-operations-handbook.md`, now referenced from `AGENTS.md` as the session entry point.
- Replaced the condensed `LICENSE-APACHE` text with the canonical Apache License 2.0 text so GitHub license detection no longer reports "Other".
- Fixed the placeholder workspace `repository` URL in `Cargo.toml` and added crate `description` metadata.
- Added root `DOC-GUIDELINES.md` documentation writing guideline, linked from the README documentation table and the operations handbook documentation map.
- Isolated OpenRouter Codex config into provider-specific Codex homes and added a readiness failure when the shared default Codex/OAuth home contains stale OpenRouter provider config.
- Treat Codex app-server protocol errors and failed `turn/completed` events as terminal runtime failures instead of successful empty assistant replies.
- Updated builtin harness ops skill, release checklist, operations docs, and feature parity docs so stale guidance review covers docs, skills, and CLI help during future behavior-changing upgrades.
- Updated response UX docs and the builtin harness ops skill for the guarded final-reply tone policy, including removal of stale "before real Telegram/Discord loops exist" channel-run-once guidance.
- Treat known Codex app-server stream disconnect protocol errors (`Reconnecting...`, `stream disconnected before completion`, and `websocket closed by server before response.completed`) as retryable transient failures before dead-lettering, preserving the existing queue/session context across attempts.
- Changed `response.emojiAccentMode` default to `off`, keeping `subtle` as explicit opt-in, and removed the mechanical `◆ Agent` wrapper from successful final Telegram/Discord replies.
- Split progress current-step narration length from short action/error preview length; current-step status now uses a longer default cap while redaction and platform-safe truncation stay in place.
- Route Telegram/Discord ingress through channel identity bindings after allow-list checks, preserving explicit account ids through queue, outbox, delivery receipts, and gateway callbacks.
- Make runtime retry caps and operator fallback hints configurable through `runtimeBackoff` instead of a fixed hard-coded attempt count.
- Isolate native cron LLM turns from interactive runtime turns with a dedicated `cron` worker lane, `runtimeClass=cron`, class-scoped runtime leases, one-shot and namespaced sticky cron sessions under `cron-sessions`, CronRunStore dispatch guards, skipped runtime tombstones, and legacy root lease compatibility during upgrade.
- Completed the Round5 live cutover with ticket `cutover-1781524146730`, backup label `pre-round5-cron-runtime-isolation-cutover`, regenerated 7-loop supervisor plan, bundled skill sync, direct runner start, and post-cutover `healthz`/`status --json` readiness `passed=59 warnings=0 failed=0`.
- Default `supervisor-plan` source-home to the active harness home instead of the retired `.openclaw` import path, and default the standalone Discord gateway wrapper to the selected harness home when no `AGENT_SOURCE_HOME` or `--source-home` is provided.
- Mark `.openclaw`, Docker gateway names, imported snapshots, and Linux/container internal paths as retired import/rollback labels across operations docs, activation docs, development handoff, CLI help, and the builtin harness skill.
- Completed the source-home routing hotfix live cutover with ticket `cutover-1781537737517`, backup label `pre-sourcehome-routing-hotfix-cutover`, bundled skill sync v0.1.12, regenerated 7-loop supervisor plan with `.agent-harness` source-home, and post-cutover Discord DM diagnostic `turn-plan` dispatching `AgentTurn` to `main`.

### Added

- Schema registry entries for `agent-harness.progress-delivery-state.v1` and `agent-harness.supervisor-stop-file.v1`.
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
- Guarded `response.emojiAccentMode` response tone policy with default `off`, opt-in `subtle`, per-agent/channel overrides, and skips for command, status, error, code-heavy, and risk/security replies.
- `channel-identity-check` for platform/account/channel binding smoke checks.
- Harness-validated outbound `deliveryIntent` for provider-native reply references, constructed from captured inbound provider ids rather than model text.
- `cron-scheduler-run-once` and `cron-scheduler-loop`, with scheduler locks, SQLite watermarks, decision receipts, idempotent worker enqueue, status readback, and optional supervisor-plan integration.
- CronRunStore (`state/cron-runs/cron-runs.sqlite`) for native cron admission, active caps, status summaries, skip/retry/quarantine controls, worker/runtime linkage, stale dispatch recovery, and operator-control-safe status writeback.
- `cron-runs` and `cron-run-control` CLI commands, plus status/worker-status summaries for runtime classes, origins, class leases, CronRun totals, and scheduler tick health.
- Account-specific Discord gateway selector support through `--discord-account`, matching the existing event and outbox account selectors.
- Schema registry entries and docs for channel identity, delivery intent, cron scheduler receipts, and CronRunStore.

### Verification

- Round8 gateway stability verification: `cargo fmt --all -- --check`
- Round8 gateway stability verification: `cargo check --workspace --target-dir target\staging-check-round8-gateway-stability`
- Round8 gateway stability verification: `cargo test -p agent-harness-core --target-dir target\staging-test-round8-gateway-stability-core -- --test-threads=1` (339 tests)
- Round8 gateway stability verification: `cargo test -p agent-harness-cli --target-dir target\staging-test-round8-gateway-stability-cli -- --test-threads=1` (39 tests)
- Round8 gateway stability verification: `cargo build -p agent-harness-cli --target-dir target\staging-build-round8-gateway-stability`
- Round8 gateway stability verification: `target\staging-build-round8-gateway-stability\debug\agent-harness.exe public-hygiene --root .public-export\agent-harness-core` (`forbiddenHits=[]`)
- Round8 gateway stability live cutover: ticket `cutover-1782046248578`, canonical live SHA-256 `55692DD0670E538CB0EE099F2F576FD3606CFB7F31FC696325E020F32915EB57`, preserved 8-loop topology, xiaoxiaoli offset repair, no remaining live-script `Tee-Object` references, forced bundled skill sync to v0.1.13, `healthz ready=true live=true`, `enable-check Ready: yes` (`passed=58 warnings=2 failed=0`), `worker-status pending=0 leased=0 running=0 failedRetryable=0 failedTerminal=3`, memory service/read-path smoke `Status: Ready`, and cutover receipt `status=ready` (`passed=59 warnings=1 failed=0`).
- Source-home hotfix verification: `cargo fmt --all --check`
- Source-home hotfix verification: `cargo test -p agent-harness-cli --target-dir target\staging-test-sourcehome-cli -- --test-threads=1` (23 tests)
- Source-home hotfix verification: `cargo test -p agent-harness-core warns_when_source_home_is_retired_openclaw --target-dir target\staging-test-sourcehome-core -- --test-threads=1`
- Source-home hotfix verification: `cargo test -p agent-harness-core --target-dir target\staging-test-sourcehome-core -- --test-threads=1` (257 tests)
- Source-home hotfix verification: `node --check tools\agent-discord-gateway\index.mjs`
- Source-home hotfix verification: `cargo check --workspace --target-dir target\staging-check-sourcehome`
- Source-home hotfix verification: `cargo build -p agent-harness-cli --target-dir target\staging-build-sourcehome`
- Source-home hotfix verification: `git diff --check` (CRLF warnings only)
- Source-home hotfix verification: `target\staging-build-sourcehome\debug\agent-harness.exe public-hygiene --root .public-export\agent-harness-core` (`forbiddenHits=[]`)
- Source-home hotfix verification: staging `supervisor-plan` without `--source-home` generated channel/scheduler scripts with `--source-home` equal to the target harness home.
- Round5 staged verification: `cargo fmt --all --check`
- Round5 staged verification: `cargo check --workspace --target-dir target\staging-check-round5-resume2`
- Round5 staged verification: `cargo test -p agent-harness-core --target-dir target\staging-test-round5-core-resume2 -- --test-threads=1` (255 tests)
- Round5 staged verification: `cargo test -p agent-harness-cli --target-dir target\staging-test-round5-cli-resume2` (20 tests)
- Round5 staged verification: `cargo test --workspace --target-dir target\staging-test-round5-workspace-resume2 -- --test-threads=1` (20 CLI tests, 255 core tests, 0 doctests)
- Round5 staged verification: `cargo build -p agent-harness-cli --target-dir target\staging-build-round5-resume2`
- Round5 staged verification: `git diff --check` (CRLF warnings only)
- Round5 staged verification: `target\staging-build-round5-resume2\debug\agent-harness.exe public-hygiene --root .public-export\agent-harness-core` (`forbiddenHits=[]`)
- `cargo fmt --all`
- `cargo test -p agent-harness-cli --target-dir target\staging-test-cli`
- `cargo test --workspace --target-dir target\staging-test-workspace`
- `cargo build --workspace --target-dir target\deploy-build`
- `agent-harness public-hygiene --root target\staging-public-hygiene\public-export`
- `agent-harness status --target-home .\.agent-harness --json`
- `agent-harness healthz --target-home .\.agent-harness --require-writable-state`
- `cargo tree --workspace --duplicates`
- `cargo test --workspace --target-dir target\staging-test-response-tone-workspace`
- `agent-harness harness-skills-sync --target-home .\.agent-harness`
- `cargo test -p agent-harness-core`
- `cargo test -p agent-harness-cli`
- `cargo build -p agent-harness-cli --target-dir target\staging-build-round4-reconnect-tone`
- `git diff --check`
- `target\staging-build-round4-reconnect-tone\debug\agent-harness.exe public-hygiene --root target\staging-public-hygiene-round4-reconnect-tone\public-export`
- `cargo build -p agent-harness-cli`
- `target\debug\agent-harness.exe config-validate --target-home .\.agent-harness`
- `target\debug\agent-harness.exe harness-skills-sync --target-home .\.agent-harness`
- `target\debug\agent-harness.exe healthz --target-home .\.agent-harness --require-writable-state`
- `target\debug\agent-harness.exe status --target-home .\.agent-harness --json`
- `cargo fmt`
- `cargo check`
- `cargo test`
- tracked-file `public-hygiene` with `forbiddenHits=[]`

### Pending Live Evidence

- Seven-day queue shadow parity summary.
- WinSW/service-wrapper restart, ordered shutdown, and reboot proof.
- Telegram/Discord/OpenRouter live smoke receipts.
- Live secret migration/rotation into the encrypted vault.
- Network-backed dependency advisory audit.
