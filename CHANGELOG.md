# Changelog

## Unreleased

### Changed

- Staged Round12 Track A virtual-session continuity fix: successful official
  Codex `compact-before-turn` outcomes now feed context-rollover accounting for
  the exact interactive/channel lane instead of silently compacting in place
  forever.
- Added idempotent compact attempt keys so replaying the same queue/thread
  official compact success does not double-count toward rollover.
- Updated invariant I13, the topology contract, release checklist output, and
  generated topology explorer data to record the staging evidence while keeping
  broader end-to-end live rollover/final-delivery promotion gates open.
- Staged Round12 Track A2 polluted-thread recovery guard: terminal
  `DeadLetter` failures with polluted Codex context recovery can enqueue a
  virtual child continuation through the existing prepared-requeue path while
  suppressing the parent error outbox; `RetryPending`, max-depth continuations,
  and parent-session sibling pending items do not auto-rollover.
- Added structured `threadHealthStatus` telemetry to Codex context preflight
  and recovery receipts so polluted-thread virtual-session recovery no longer
  depends only on diagnostic reason-string matching.

### Verification

- `cargo test -p agent-harness-core run_codex_runtime_preflight_compacts_existing_thread_before_turn --target-dir target\staging-test-round12-virtual-session-a-green -- --nocapture`
- `cargo test -p agent-harness-core compact_counter_ --target-dir target\staging-test-round12-virtual-session-a-green -- --nocapture`
- `cargo test -p agent-harness-core prepare_runtime_queue_item_rekeys_pending_turn_when_rollover_is_pending --target-dir target\staging-test-round12-virtual-session-a-green -- --nocapture`
- `cargo test -p agent-harness-core context_rollover_blocked_leased_stops_prepare_path --target-dir target\staging-test-round12-virtual-session-a-green -- --nocapture`
- `cargo test -p agent-harness-core quality_catalogs_and_hygiene_report_are_actionable --target-dir target\staging-test-round12-virtual-session-a-green -- --nocapture`
- `cargo fmt --all -- --check`
- `cargo check -p agent-harness-core --target-dir target\staging-check-round12-virtual-session-a`
- `cargo build -p agent-harness-cli --target-dir target\staging-build-round12-virtual-session-a`
- `target\staging-build-round12-virtual-session-a\debug\agent-harness.exe public-hygiene --root .public-export\round12-virtual-session-a-20260630`
- `cargo test -p agent-harness-core polluted_thread_continuation_runs_only_at_dead_letter_and_respects_depth_limit --target-dir target\staging-test-round12-a2-polluted-thread -- --nocapture`
- `cargo test -p agent-harness-core prepared_auto_requeue_blocks_parent_session_sibling --target-dir target\staging-test-round12-a2-polluted-thread -- --nocapture`
- `cargo test -p agent-harness-core run_runtime_queue_once_retries_reconnecting_protocol_error_then_dead_letters --target-dir target\staging-test-round12-a2-polluted-thread -- --nocapture`
- `cargo test -p agent-harness-core context_preflight_compacts_for_bound_thread_inline_image_bloat --target-dir target\staging-test-round12-a2-thread-health-status -- --nocapture`
- `cargo test -p agent-harness-core retryable_protocol_error_after_bloated_thread_rolls_over_to_fresh_thread --target-dir target\staging-test-round12-a2-thread-health-status -- --nocapture`

## v0.2.1 - 2026-06-29

### Difference from v0.2.0

`v0.2.1` is the bridge-polish A/B follow-up to `v0.2.0`. Compared with
`v0.2.0`, public/non-main memory status and read-path receipts now expose
source allow/deny scopes, trust level, and filtered global-imported hit counts
so operators can prove that public-facing agents are not inheriting private
`main` imported memory by accident. This release keeps Phase C open:
native Qdrant, `routeAuto`, autonomous graph parity, provenance, and freshness
promotion gates remain tracked under `openclaw-mem-full-parity-gap`.

This release pairs with `openclaw-mem v1.9.32`, which adds direct Windows
console-wrapper proof for the generated `.venv\Scripts\openclaw-mem.exe`
bridge entrypoint.

### Changed

- Added public/non-main memory scope telemetry:
  `allowedSourceScopes`, `deniedSourceScopes`, `trustLevel`, and
  `filteredGlobalImportedHits`.
- Changed the public/non-main memory trust smoke so
  `globalImportedSnapshotAllowed=false` is expected; the smoke now fails when
  public/non-main agents allow private global imported memory or omit the deny
  receipt.
- Updated the topology contract, feature parity table, and invariant I8 to
  record the A/B trust/source receipts while keeping full openclaw-mem graph
  parity unclaimed.

### Verification

- `cargo test -p agent-harness-core memory::tests::public_agent_read_path_smoke_surfaces_source_allow_list_and_filtered_counts --target-dir target\staging-bridge-polish-final-test -- --test-threads=1`
- `uv run python -m pytest tests\test_windows_console_wrapper_bridge.py -q`
  in the paired `openclaw-mem v1.9.32` local repo.
- `cargo fmt --all -- --check`
- `git diff --check`
- `target\debug\agent-harness.exe public-hygiene --root .public-export\agent-harness-core`
- Post-cutover live checks: live binary SHA-256
  `721D3750D729A560FBC0A466D7FC4DD30BB0AA1E03B07275684FA5B675A88E8F`,
  `healthz ready=true live=true`, outbox pending `0`, bridge
  `fallbackUsed=false`, and public-bot read-path scope/trust smoke green.

## v0.2.0 - 2026-06-29

### Difference from v0.1.7

`v0.2.0` promotes the openclaw-mem bridge-primary memory path and hardens
Windows service executable resolution. Compared with `v0.1.7`, configured
`mem-engine` ownership can use an openclaw-mem subprocess bridge for status,
recall, and approved store operations instead of relying on stale response
files or a read-only migration fallback; Codex and Node service launches also
avoid fragile Windows PATH/shim assumptions after the 2026-06-29 live incident.
This release requires `openclaw-mem >= 1.9.31` for the bridge-primary envelope
contract.

### Changed

- Added `memory.openclawMemBridgeCommand` / `memory.openclawMemBridgeBin`
  validation to `harness-config.json` so managed harness restarts can retain the
  openclaw-mem bridge route without operator shell environment variables.
- Changed `memory-service-status` for active `mem-engine` ownership to ask the
  configured bridge for fresh status telemetry before consulting older recall
  receipts.
- Raised the read-only openclaw-mem bridge deadline to 15 seconds and added one
  bounded retry for `status` and `recall`; approved `store` stays fail-closed
  and is not retried, avoiding duplicate canonical writes.
- Updated the topology contract and invariant I12 to distinguish
  bridge-primary status/recall/store proof from the remaining graph autonomous
  matching parity work.
- Prefer native Windows Codex vendor `codex.exe` for service-mode app-server
  runtime plans, with npm `codex.cmd` shim execution only as fallback.
- Record terminal `ProtocolError` receipts when Codex app-server startup or
  early request writes fail before normal protocol completion.
- Resolve default Node service commands on Windows through
  `AGENT_HARNESS_NODE_EXE`, explicit `node.exe` install paths, or PATH
  `node.exe` before falling back to bare `node`.
- Updated the topology contract and invariant catalog with
  `codex-startup-executable-stability-gap` as the promotion gate for Codex/Node
  service executable stability.

### Verification

- `cargo fmt --all -- --check`
- Focused memory owner/service bridge tests, including harness-config-backed
  bridge routing.
- Quality gate test for invariants and release checklist output.
- openclaw-mem bridge CLI pytest suite from the paired `v1.9.31` local repo.
- Focused Codex/Node service executable tests for native Codex vendor
  resolution, startup terminal receipts, extensionless shim preflight, MSIX path
  blocking, supervisor plan parsing, and Windows Node default resolution.
- Public hygiene and `git diff --check` before tagging.

## v0.1.7 - 2026-06-29

### Changed

- Merged all non-web development branches back to `main`; `gh-pages` remains the separate website branch.
- Made the public repository surface explicit: GitHub-facing docs and tools now stay limited to project architecture, configuration, usage, public status, and reproducible helper utilities.
- Moved live operations handbooks, release/checkpoint handoffs, validation scratch notes, Superpowers plans, staging/cutover notes, `.debug` evidence, and owner-machine helper tools to ignored local-only private paths (`docs/.private/`, `tools/.private/`, `.debug/`).
- Updated `AGENTS.md`, `DOC-GUIDELINES.md`, and the README documentation table with the public/private disclosure rule for future docs and tools.
- Regenerated the topology explorer so public artifacts no longer embed local live-validation evidence from the private operations handbook.
- Updated the README public overview to describe virtual-session long-task continuity, the `/new` task-boundary guarantee, and the current test scale.
- Preserved agent identity across final outbox freshness checks: a same-agent stale session is still suppressed after `/new`, but a completed non-main agent turn sharing the same platform/channel/user is not suppressed solely because shared channel state currently points at another agent.
- Added `docs/agent-harness-topology-contract.md` as the public-safe topology contract and impact matrix for channel, runtime, prompt, outbox, delivery, memory, and cutover changes.
- Added invariant I8 to the docs and machine-readable invariant catalog: `agentId` is a routing boundary across channel state, session freshness, prompt, runtime, outbox, delivery, and memory.
- Tightened the release checklist so channel/runtime/session changes must run the agent-boundary scenario pack.
- Documented explicit ideal-vs-actual gap labels for `openclaw-mem-full-parity-gap`, `multi-agent-full-matrix-gap`, and `virtual-session-continuity-gap` so support-plane graph/readiness evidence is not mistaken for full design parity.
- Added `progress-delivery-volume-gap` after Telegram DM receipts showed provider-visible progress edit storms even when final outbox delivery exists.
- Added invariant I9 and `progress-final-surface-gap` after a Telegram DM probe showed `assistantNarrationMode=progress_panel` narration could be written as a normal final `agent-reply` during recovery.
- Routed progress delivery/narration changes through the topology impact matrix and mirrored the edit-volume plus final-surface gates in the release checklist.
- Completed the Round9-1 lifecycle and image-timeout follow-up live cutover. The live `agent-harness.exe` now runs source commit `628fe36` and preserves `telegram-loop-xiaoxiaoli` as agent `xiaoxiaoli` through supervisor reconciliation instead of falling back to `main`.
- Tightened sub-agent lifecycle receipts so unknown or already-terminal close paths stay idempotent without claiming cleanup proof, and smoke receipts report provider/auth visibility as unverified when the lane is unavailable.
- Documented nested social-image verification as worker/long-task work with terminal image-route summaries instead of relying on longer outer interactive Codex timeouts.
- Added a Round10 completion-repair regression gate for `progress-final-surface-gap`, proving already-recorded completion repair keeps progress-panel narration out of final channel outbox payloads.
- Added `response.progressDeliveryMaxNonterminalUpdatesPerLane` and persisted delivered non-terminal body/status counters so progress delivery can cap provider-visible intermediate sends/edits per queue while still allowing terminal `Done`/`Failed` convergence.
- Extended progress delivery planning and CLI reports with `volume_limited` so edit-volume suppression is visible in operator output instead of silently dropping progress updates.
- Refined Codex image gateway failure classification so an input-specific no-tool result after a successful route probe reports `image_tool_not_called_for_input` instead of misclassifying the global image route as unavailable.
- Added initial Codex tool-use idle-timeout recovery: active tool-use timeouts now record `toolUseTimeout`, stop the stalled app-server/tool path, and hand the task to one bounded fresh-thread recovery prompt instead of immediately dragging the parent queue item to dead-letter.
- Captured review-only tool-timeout recovery output as `agent-harness.external-review-evidence.v1` instead of final parent workflow completion.
- Added `codexContext.highContextUsageCompactTokenLimit` so context preflight compacts an existing bound thread with very high recorded usage even when no `modelContextWindow` ratio is configured.
- Added `docs/topology-explorer.html` and `tools/generate_topology_explorer.py` as a generated interactive topology/canvas view synced from the topology contract and current live-validation summary.
- Upgraded the topology explorer from a raw node browser into a guided big-picture surface with journey cards, step-by-step focus, impact explanations, and an open-gap queue.
- Documented `per-agent-memory-recall-compartment-gap`: Xiaoxiaoli has per-agent memory artifacts, but live recall still allows global imported fallback context under `agent-plus-global-imported` while the mem-engine bridge is absent.

### Verification

- Added regression coverage: `runtime_pipeline::tests::channel_session_freshness_does_not_cross_suppress_other_agent`.
- Round9-1 fresh validation passed `cargo fmt --all -- --check`, workspace check, `agent-harness-core` tests (431 unit tests plus 5 integration tests and doc tests), `agent-harness-cli` tests (53), image helper tests (14), sparse-runner tests (6), staged build, public export hygiene with `forbiddenHits=[]`, and `git diff --check`.
- Live cutover ticket `cutover-1782447196301` advanced canonical `target\debug\agent-harness.exe` to SHA-256 `946D0AC01F6DF266D2D356A5A8342B3D87238E9E7D56C8C653CDEC079BACA908`, backed up the previous binary as `target\debug\agent-harness.pre-round9-1-followup-20260626-122051.exe`, and recorded `ops-cutover-receipt status=ready`.
- Post-cutover validation reported `healthz ready=true live=true readinessReady=true`, readiness `passed=58 warnings=2 failed=0`, worker idle gate `pending=0 leased=0 running=0 failedRetryable=0 runtimeOpenItems=0 activeCronRuns=0`, and `telegram-loop-xiaoxiaoli` command lines containing `--agent xiaoxiaoli`.
- Round10 staging validation passed `cargo fmt --all -- --check`, `cargo check --workspace --target-dir target\staging-round10-check`, focused progress volume-limit tests, focused config validation tests, the completion-repair final-surface regression, and the image gateway helper test suite.
- Tool-timeout guard validation passed `cargo test -p agent-harness-core run_codex_runtime_recovers_tool_use_idle_timeout_with_fresh_thread --target-dir target\staging-round10-tool-timeout-test -- --test-threads=1`.
- External review/context preflight validation passed focused `agent-harness-core` tests for review-only evidence capture, normal tool-timeout recovery, high-usage preflight compact, stream-disconnect fresh-thread rollover, and compact-failure checkpoint fallback under `target\staging-round10-gap-closure-external-review-green` and `target\staging-round10-context-preflight-green`.
- Round10 full pre-cutover validation passed full core tests (450 unit tests plus 5 integration tests and doc tests), full CLI tests (54), image helper tests (15), staged build, public export hygiene, `invariants`, `schema-registry`, and `git diff --check`.
- Live cutover ticket `cutover-1782619189243` advanced canonical `target\debug\agent-harness.exe` to SHA-256 `229656F71806605650D5D7293F6B37F4362F511A18EBA386C22D06F5E45A4D2D`, backed up the previous live binary as `target\debug\agent-harness.pre-round10-tool-timeout-progress-20260628-120311.exe`, and post-cutover validation reported `healthz ready=true live=true`, worker/outbox idle, clean supervisor reconcile, and `ops-cutover-receipt status=ready`.
- Follow-up Round10 live readback confirmed `healthz ready=true live=true readinessReady=true`, targeted Discord and Telegram outbox `pending=0 failed_retryable=0 invalid=0`, worker queue clear except the current interactive channel item, `supervisor-reconcile --all --dry-run` clean, and memory read-path `Ready` through documented migration fallback; it also documented `supervisor-service-health-precedence-gap` for stale `discord-gateway-loop` service metadata that can remain visible behind a fresh loop heartbeat.

## v0.1.2 - 2026-06-21

### Changed

- Added `response.progressDeliveryMode`, `response.progressDeliveryAgentModes`, and `response.progressDeliveryChannelModes` so operators can mute progress panels globally, per agent, or per channel without disabling final replies.
- Treat `timeout` runtime trace records as terminal in `trace`, matching runtime queue and progress delivery timeout semantics.
- Added generated runtime-runner process-exit classification for OOM/resource-exhaustion signatures, recording `errorClass` and a bounded `restartAfterSeconds` in `runtime-loop-runner-safe-mode.json`.
- Generated runtime runners now write a structured temporary stop file for `progress-delivery-loop` and record `memoryGateDecision` before restarting after OOM/resource-exhaustion signatures.
- `status --json` and `healthz` now expose supervisor service registry records, including launch ownership, supervisor PID, restart/backoff, exit, and memory-gate fields.
- Generated progress delivery runners now launch `supervisor-run --service progress-delivery-loop`, moving the low-risk progress child under Rust supervisor ownership while other loops stay on the existing runner path.
- Generated Discord outbox runners now launch `supervisor-run --service discord-outbox-loop`, moving final Discord delivery under Rust supervisor ownership with final-delivery priority and a shorter restart backoff.
- Runtime queue leases now write structured owner envelopes with `serviceId`, `generationId`, `pid`, `processStartTimeMs`, and `acquiredAtMs`, while legacy `owner="pid:<n>"` leases remain readable and reapable.
- Generated runtime runners now stamp runtime-loop child generations and call `runtime-lease-reconcile` after non-zero child exits so leases owned by the exited generation are reaped before restart backoff.
- Telegram and Discord command handling now accepts admin-only `/restart` requests that write a nonpersistent restart stop-file envelope; generated channel runners clear `action=restart` stop files and relaunch instead of staying stopped.

### Added

- Schema registry entry for `agent-harness.runtime-loop-runner-safe-mode.v1`.
- Schema registry entry for `agent-harness.runtime-queue-leases.v1`.
- Schema registry entries for `agent-harness.runtime-queue-lease-reconciliation.v1` and `agent-harness.channel-restart-request.v1`.
- Added `runtime-lease-reconcile` for explicit generation-owned runtime lease cleanup after a supervised child exits.
- Loop heartbeat writers now emit `generationId` metadata and per-service `agent-harness.supervisor-service-state.v1` records under `state/supervisor/services`.
- Added `supervisor-run` for Rust-owned low-risk child supervision, starting with `progress-delivery-loop` and `discord-outbox-loop`.

### Verification

- Round8 observe-only supervisor registry verification: `cargo fmt --all -- --check`
- Round8 observe-only supervisor registry verification: `cargo check --workspace --target-dir target\staging-check-round8-supervisor-registry`
- Round8 observe-only supervisor registry verification: `cargo test -p agent-harness-core --target-dir target\staging-test-round8-supervisor-registry-core-full -- --test-threads=1` (341 tests)
- Round8 observe-only supervisor registry verification: `cargo test -p agent-harness-cli --target-dir target\staging-test-round8-supervisor-registry-cli-full -- --test-threads=1` (39 tests)
- Round8 observe-only supervisor registry verification: `cargo build -p agent-harness-cli --target-dir target\staging-build-round8-supervisor-registry`
- Round8 observe-only supervisor registry verification: `target\staging-build-round8-supervisor-registry\debug\agent-harness.exe public-hygiene --root .public-export\agent-harness-core` plus changed public/operator docs path hygiene (`forbiddenHits=[]`)
- Round8 memory-pressure gate verification: `cargo fmt --all -- --check`
- Round8 memory-pressure gate verification: `cargo check --workspace --target-dir target\staging-check-round8-memory-gate`
- Round8 memory-pressure gate verification: `cargo test -p agent-harness-core --target-dir target\staging-test-round8-memory-gate-core-full -- --test-threads=1` (341 tests)
- Round8 memory-pressure gate verification: `cargo build -p agent-harness-cli --target-dir target\staging-build-round8-memory-gate`
- Round8 memory-pressure gate verification: public export and changed operator docs/skill path hygiene (`forbiddenHits=[]`)
- Round8 supervisor-owned progress verification: `cargo fmt --all -- --check`, `cargo check --workspace --target-dir target\staging-check-round8-supervisor-progress`, full core tests (341), full CLI tests (40), `cargo build -p agent-harness-cli --target-dir target\staging-build-round8-supervisor-progress`, `git diff --check`, focused supervisor-run/status/health/schema coverage, public export hygiene (`forbiddenHits=[]`), and changed public/operator docs path hygiene (`forbiddenHits=[]`).
- Round8 supervisor-owned final outbox verification: `cargo fmt --all -- --check`, `cargo check --workspace --target-dir target\staging-check-round8-supervisor-outbox`, full core tests (341), full CLI tests (41), `cargo build -p agent-harness-cli --target-dir target\staging-build-round8-supervisor-outbox`, `git diff --check`, focused supervisor-run final-outbox/status/health/schema coverage, public export hygiene (`forbiddenHits=[]`), changed public/operator docs path hygiene (`forbiddenHits=[]`), and public-facing content scan (`files=156`, `patterns=8`).
- Round8 runtime lease owner envelope verification: `cargo fmt --all -- --check`, `cargo check --workspace --target-dir target\staging-check-round8-lease-owner`, focused `runtime_worker::tests`, full core tests (342), full CLI tests (41), `cargo build -p agent-harness-cli --target-dir target\staging-build-round8-lease-owner`, `git diff --check`, public export hygiene (`forbiddenHits=[]`), and added-line public/operator docs path hygiene (`forbiddenHits=[]`).
- Round8 runtime generation reconciliation and channel `/restart` verification: `cargo fmt --all -- --check`, `cargo check --workspace --target-dir target\staging-check-round8-restart-clean`, full core tests (344), full CLI tests (41), `cargo build -p agent-harness-cli --target-dir target\staging-build-round8-restart`, `git diff --check`, and public export hygiene excluding `.debug` with `forbiddenHits=[]`.
- Progress delivery mute verification: `cargo fmt --all -- --check`, focused core `trace`, `progress_delivery`, and `config` tests, focused CLI `progress_delivery` tests, `cargo check -p agent-harness-cli --target-dir target\staging-check-progress-mute`, full workspace tests under `target\staging-test-progress-mute-workspace` (41 CLI tests, 351 core tests, 5 integration tests, 0 doc-tests), `cargo build -p agent-harness-cli --target-dir target\staging-build-progress-mute`, `git diff --check`, public export hygiene (`forbiddenHits=[]`), schema registry, invariants, trace samples, guarded live cutover ticket `cutover-1782197836816`, post-cutover `healthz ready=true live=true`, `enable-check passed=59 warnings=1 failed=0`, and fixture-backed mute smoke (`Muted events=1`, `Sent panels=0`).

## v0.1.1 - 2026-06-21

### Changed

- Stabilized Round8 gateway loop recovery: runtime queue leases now reap definitely dead legacy `owner="pid:<n>"` owners before queue selection/capacity checks, write `stale-owner-reaped` evidence, and emit a durable `lease-acquired` receipt before execution artifacts are prepared.
- Made loop heartbeat writes atomic and surfaced corrupt/NUL heartbeat files through explicit `corrupt` / `parseError` status and health fields; `healthz` now warns for degraded progress delivery without marking final reply delivery not live when runtime, ingress, and final outbox loops are otherwise healthy.
- Bounded progress delivery planning with a persisted progress-ledger byte cursor plus compacted per-queue cached state, preserving first/terminal events while coalescing repeated low-value cached events.
- Changed generated long-running Windows runner scripts to write process streams directly to per-loop log files instead of using `Tee-Object`; `ops-control stop` now writes structured JSON stop files while preserving legacy plain-text stop-file readability.
- Reworked `README.md` into a public-facing overview (positioning, architecture diagram, CLI family table, FAQ, dual-license section); moved the internal live-validation, topology, full command walkthrough, and capability ledger content into the operations handbook. That handbook is now kept local-only under `docs/.private/`.
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
