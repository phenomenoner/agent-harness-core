# Agent Harness Core Roadmap Backlog

Date: 2026-06-12

Source roadmap: `D:\Warehouse\Research\Claude_Discuss\ai-agent-harness-review\agent-harness-core-roadmap-2026-06-12.md`
Review baseline: `D:\Warehouse\Research\Claude_Discuss\ai-agent-harness-review\agent-harness-comparative-review-2026-06-12.md`

This is the canonical follow-up development backlog for `agent-harness-core` after the 2026-06-12 review. It incorporates the roadmap recommendations and intentionally ignores the roadmap time estimates. As of the 2026-06-12 staging pass, this document also tracks implementation status, review evidence, and remaining live/soak gates.

## Planning Rules

The north star is operational reliability, not benchmark-style scoring:

> The harness should run real traffic for 30 consecutive days without manual intervention, with zero silent failures, and every failure reconstructable from receipts within five minutes.

SLOs to implement once `/healthz` is live:

| SLO | Target | Measurement source | Backlog owner |
|---|---:|---|---|
| Inbound message to reply or explicit error notification | >= 99.5% success | ingress, queue, completion, outbox, delivery receipts | P1.7, P1.9 |
| Silent failures | 0 | trace chain: ingress without terminal receipt | P1.7 |
| Loop death to recovery MTTR | <= 5 minutes | supervisor child state and alert receipts | P1.1-P1.6 |
| First token / first reply latency p95 | baseline, then ratchet | Codex runtime receipts and metrics | P1.9, P3.4, P4 |
| Manual interventions | trend to 0 | operator receipts | P1, P6, P7 |

Stop-the-line rule: if any SLO is red for seven consecutive days, freeze feature phases P3-P5 and return to stability work until the same SLO is green for seven consecutive days.

Delivery discipline:

- Soak window: every deployed phase needs at least one week of live soak before the next deployment window. Development may continue during soak, but deployment waits for green SLOs.
- Cut line: every phase keeps P0/P1/P2 priority. If scope slips, cut P2 items instead of carrying the whole phase forward.
- Feature flags: new behavior is config-gated, defaults off unless explicitly decided otherwise, and writes receipts when toggled. Default-on behavior must be limited to safe modes such as learning propose-only, or happen only after soak evidence.
- Storage rule: queue-like, lease-like, or hot-polled lifecycle state belongs in the SQLite worker store. Entity documents and audit trails stay as JSON documents or JSONL receipts.
- Schema rule: every new receipt/state schema gets a name, version, compatibility note, and old-version reader for at least one release cycle when a breaking change is introduced.
- Review-aligned acceptance: every completed backlog item must name the review dimension it improves and leave reviewable evidence: automated tests, deterministic simulation seeds, live soak summary, receipt-chain sample, benchmark, security test fixture, CI result, or operator drill receipt.

## User Decisions Recorded 2026-06-12

These decisions remove several planning blockers:

- Staging development is allowed while live `.agent-harness` keeps running. Staging must use a separate harness home such as `.agent-harness-staging`, a separate build target such as `target/staging-build`, fake Codex by default, no live Telegram/Discord poll/gateway loops by default, and a scratch runtime workspace. Live cutover happens only through an explicit stop/swap/start or future `supervise deploy` canary path.
- Secrets vault direction is a repo-local encrypted vault for future cross-platform portability. Windows Credential Manager is no longer the preferred direction for this project.
- Unattended Windows operation should follow the service-wrapper recommendation: WinSW/service-wrapper style supervision is the primary target, while Task Scheduler remains a generated fallback/compatibility path.
- Canary deploy must support fake-only canary. A small live-model canary is optional and stronger, not required when credentials or cost policy make it unsuitable.
- OpenRouter live smoke should use the legacy OpenClaw source key and `小小梨` agent settings when executed by the live/main-agent operator path. This coding-planning session does not extract secrets or run that smoke unless the operator explicitly provides the exact safe command/channel boundary.
- Learning loop policy is default-on in safe propose-only mode. Auto-apply is a config option and must remain opt-in per scope. Bundled, imported, and agent-created skills may all be proposed for patch/archive; mutation requires receipts and backups. Direct destructive deletion remains outside the default policy unless an operator explicitly enables it.
- Plugin real invocation policy follows the recommended conservative model: explicit allow-list, timeout, per-agent/per-channel permissions, receipt trail, and fixture coverage before enabling.

## Staging Implementation Status - 2026-06-12

Status vocabulary:

- `Implemented/staging-tested`: code, CLI/API surface where applicable, and automated tests exist in the staging build.
- `Mechanism implemented; pending live gate`: the deterministic mechanism exists, but completion needs live traffic, OS registration, reboot proof, external credentials, or a soak window.
- `Scaffold implemented; pending broader fixtures`: the safe contract/API exists, but the external integration corpus or long-running fixture set is still needed before claiming production parity.
- `Deferred by policy`: intentionally not enabled because the roadmap itself makes it conditional on measurement, trust policy, or operator decision.

Latest staging evidence:

- `cargo fmt --all` passed.
- `cargo test --workspace --target-dir target\staging-test-workspace` passed with 16 CLI tests, 201 core tests, and 0 doc-tests.
- `cargo tree --workspace --duplicates` was run locally; the only duplicate tree is `webpki-roots` through `ureq`/TLS. A network-backed advisory audit remains a release gate when `cargo audit` or an equivalent advisory DB is available.
- No live `.agent-harness` loop was stopped, restarted, or cut over. All tests used staging target dirs.

### Item Coverage Matrix

| Item | Status | Evidence now | Remaining gate before production-complete |
|---|---|---|---|
| P0.1 Atomic Write Audit | Implemented/staging-tested | Existing `write_json_atomic` retained; new mutable state writes use atomic JSON where applicable; `docs/atomic-write-audit.md`; `logging::write_json_atomic_replaces_existing_json`. | Add kill-during-write OS drill receipt during staging soak. |
| P0.2 Transient Error Policy, Backoff, and Dead Letter | Implemented/staging-tested | `RuntimeRunOnceStatus::RetryPending/DeadLetter`, `RuntimeDeadLetterReceipt`, timeout cap test `timeout_policy_retries_then_dead_letters`. | Live provider transient-failure sample and user notification receipt. |
| P0.4 Receipt Noise and Log Rotation | Implemented/staging-tested | `rotate_harness_log_if_needed`, `log-rotate` CLI, rotation receipt test. | Live trace sample after actual rotation. |
| P0.5 Queue Retry and Skip Commands | Implemented/staging-tested | `control_runtime_queue_item`, `queue-retry`, `queue-skip`, fresh retry queue id test. | Operator drill receipt against staging queue. |
| P0.6 Fail-Closed Config Validation | Implemented/staging-tested | `validate_harness_config`; startup/preflight integration in activation, workers, Codex planning; config validation tests. | Live invalid-config preflight drill before cutover. |
| P1.1 Supervisor Child Tree | Mechanism implemented; pending live gate | `evaluate_supervisor_children`, `supervise-evaluate` CLI, persisted supervision receipts. | Replace generated loop scripts with real service-wrapper child process registration. |
| P1.2 Restart Backoff and Crash-Loop Breaker | Mechanism implemented; pending live gate | Restart/backoff/breaker model and test `evaluates_stale_child_and_crash_loop_breaker`. | Kill-loop/restart transcript under WinSW/service-wrapper staging. |
| P1.3 Heartbeat Liveness Probe | Implemented/staging-tested | `collect_healthz` stale heartbeat detection and test. | Live stale-loop drill and status sample. |
| P1.4 Operator Alert Through Outbox | Mechanism implemented; pending live gate | Supervision/dead-letter alert payloads are represented in receipts and current outbox policy remains permission-gated. | Actual Telegram/Discord admin alert receipt from staging. |
| P1.5 Ordered Shutdown | Mechanism implemented; pending live gate | Existing scoped stop/ops control plus supervision stop-intent model. | Service-wrapper ordered stop transcript. |
| P1.6 OS Persistence, Scheduled Health, Disk Gate, and `/healthz` | Mechanism implemented; pending live gate | `healthz` CLI/local JSON report with writable state probe. | WinSW/service-wrapper registration, reboot recovery proof, and optional local/admin HTTP endpoint if needed. |
| P1.7 End-to-End Trace ID and `trace <id>` CLI | Implemented/staging-tested | `trace_harness_event`, `trace` CLI, legacy id chain test. | End-to-end live traceId propagation sample across ingress/runtime/outbox/delivery. |
| P1.8 `supervise deploy` Canary and Rollback | Implemented/staging-tested | `record_supervise_deploy_canary`, `deploy-canary-record` CLI, rollback hash test. | Staging canary drill with candidate binary and optional live-model canary. |
| P1.9 Metrics and `status --watch` | Mechanism implemented; pending live gate | `collect_harness_metrics`, `metrics` CLI, runtime status counter test. | `status --watch` streaming UI remains pending; metrics snapshot from live soak required. |
| P2.1 `channel_turn` Lane in Worker Store | Scaffold implemented; pending broader fixtures | SQLite shadow table in `state/workers/worker-store.sqlite`; worker-store direction preserved. | Full execution source cutover to WorkerStore lane after shadow soak. |
| P2.2 Shadow Dual-Write and Divergence Receipts | Mechanism implemented; pending live gate | `queue-shadow-record`, `queue-shadow-compare`, divergence receipt test. | Seven live days of zero divergence. |
| P2.3 Remove Runtime Queue Lease Split-Brain | Scaffold implemented; pending broader fixtures | Existing runtime lease idempotency/race tests plus shadow comparator. | Retire JSONL lease source after P2.2 live soak. |
| P2.4 `ops-compact`, Sequence Cursor, WAL, and `VACUUM INTO` | Scaffold implemented; pending broader fixtures | Backup/atomic audit docs and WorkerStore SQLite path exist. | Implement compaction command and restore/compact drill receipts. |
| P2.5 Wake File / Event-Driven Queue Wakeup | Deferred by policy | Current loops remain bounded polling with explicit stop/heartbeat controls. | Add wake-file/event path only after SQLite source cutover. |
| P2.6 Cutover, Drain, and JSONL Retirement | Deferred by policy | Cutover path documented; no live queue source switch performed. | Seven-day shadow summary and explicit operator cutover. |
| P3.1 Scoped `/stop` | Implemented/staging-tested | `record_scoped_stop`, `scoped-stop` CLI, marker/receipt test. | Live scoped stop against active staging turn/job. |
| P3.2 Message Modes: Steer, Followup, Collect, Interrupt | Scaffold implemented; pending broader fixtures | Existing `/steer`/`/btw` parsing and channel command state remain covered; scoped controls added. | Collect/interrupt semantics need scenario replay fixtures and UI receipts. |
| P3.3 Per-Session Serial Lane and Caps | Implemented/staging-tested | Worker dispatch config validation and per-agent/per-channel capacity tests. | Live fan-out/fairness smoke. |
| P3.4 Warm App-Server Pool | Deferred by policy | Token/latency metrics support added for measurement. | Implement only after benchmark proves benefit and isolation risks are closed. |
| P3.5 Rate Limit and Queue-Depth Admission Control | Implemented/staging-tested | `evaluate_admission`, `admission-check` CLI, refusal receipt test. | Flood drill against staging channel. |
| P3.6 Background Registry in Worker Store | Implemented/staging-tested | SQLite `background_tasks`, `background-list`, `background-upsert`, stale heartbeat test. | Live long-task registry sample. |
| P3.7 Long-Task Contract | Mechanism implemented; pending live gate | Background task record carries owner/pid/artifact root/cancel strategy/TTL/heartbeat/status. | Active long-task heartbeat/block/cancel receipts. |
| P4.1 Token Aggregation | Implemented/staging-tested | `collect_token_efficiency`, `token-efficiency` CLI, token receipt aggregation test. | Live token report after staging traffic. |
| P4.2 Identity/Runtime Dedup and Prompt-Cache Prefix Stability | Implemented/staging-tested | `evaluate_prompt_reduction`, `prompt-reduction` CLI, stable-prefix purity check. | Golden same-session >=30% reduction evidence. |
| P4.3 Memory Recall Ledger | Scaffold implemented; pending broader fixtures | Existing memory recall/proposal/writeback receipts plus token report hooks. | Citation ledger samples from scenario replay/live traffic. |
| P4.4 Skill Progressive Disclosure | Implemented/staging-tested | Existing skill selection tests and prompt bundle skill inclusion tests. | Broader persona-sensitive regression fixtures. |
| P4.5 Job-Kind Model Routing | Scaffold implemented; pending broader fixtures | Worker job kinds and runtime registry routing remain tested. | Per-job model routing policy matrix and live smoke. |
| P4.6 Session Rotation and Handoff Summary | Scaffold implemented; pending broader fixtures | Task entity/checkpoint and trace support added. | Handoff summary generation after long-running staging session. |
| P4.7 Golden Bundle Tests | Implemented/staging-tested | Prompt bundle ordering/truncation/injection ledger tests; prompt-reduction gate. | Add captured golden files from sanitized traffic. |
| P5.1 Task Entities | Implemented/staging-tested | `write_task_entity`, `task-write` CLI, checkpoint test. | Live task lifecycle sample. |
| P5.2 Heartbeat | Mechanism implemented; pending live gate | Loop heartbeat/status and background heartbeat age detection. | Deterministic agent heartbeat entity drill. |
| P5.3 Budget Counters | Implemented/staging-tested | SQLite `budget_counters`, `budget-acquire` CLI, budget test. | Concurrent budget race fixture under load. |
| P5.4 Native MCP Server | Scaffold implemented; pending broader fixtures | In-process JSON-RPC handler, `mcp-request` CLI, initialize/list/call/allow-list test. | Long-running stdio server and protocol fixture corpus. |
| P5.5 Chaining | Scaffold implemented; pending broader fixtures | Task/budget foundations and MCP allow-list preflight exist. | Bounded chaining runner and depth/loop tests. |
| P5.6 Drift Detection | Implemented/staging-tested | `check_config_drift`, `drift-check` CLI, hash diff test. | Live intended-vs-active config sample. |
| P5.7 Learning Loop | Implemented/staging-tested | `create_learning_proposal`, `learning-propose` CLI; default propose-only, auto-apply opt-in but still reviewed/quarantined; injection scan test. | Operator review/apply/archive workflow and backup receipts. |
| T1 Invariants Catalog | Implemented/staging-tested | `invariant_catalog`, `invariants` CLI, `docs/invariants.md`. | Keep invariant IDs attached to future simulation failures. |
| T2 Seeded Deterministic Simulation | Scaffold implemented; pending broader fixtures | Invariants and focused race/unit tests exist. | Add seeded crash/interleaving simulation harness with replayable seeds. |
| T3 Scenario Replay Evals | Scaffold implemented; pending broader fixtures | Trace, prompt golden, security, and queue-shadow fixtures form replay primitives. | Sanitized traffic replay corpus. |
| T4 Restore Drills | Scaffold implemented; pending broader fixtures | Existing `ops-backup` tests and `docs/atomic-write-audit.md`. | Restore drill receipt from staging backup. |
| M1 ContextPack v1 Contract | Implemented/staging-tested | `parse_context_pack`, `context-pack-validate` CLI, contract test. | External openclaw-mem fixture exchange. |
| M1+ Consumer-Driven Contract Tests | Scaffold implemented; pending broader fixtures | ContextPack parser fails closed/fail-open without live dependency. | Partner/provider fixture corpus. |
| M2 Receipt Ingest Contract | Scaffold implemented; pending broader fixtures | `decide_memory_ingest` idempotency helper and test. | Receipt ingest fixtures from openclaw-mem. |
| M3 openclaw-mem MCP Direct Path | Scaffold implemented; pending broader fixtures | MCP handler and fail-open memory contract primitives. | Direct remote memory MCP call fixture with fallback proof. |
| M3+ MCP Tool Description Pinning | Implemented/staging-tested | `tool_description_hash`, `tool-pin-check` CLI, hash drift test. | Pin checks wired into live enable/preflight path. |
| M4 End-to-End Memory Provenance | Scaffold implemented; pending broader fixtures | Existing memory service receipts plus trace/provenance primitives. | Recall/proposal/writeback trace sample. |
| M5 Harness Dogfooding Host | Scaffold implemented; pending broader fixtures | MCP and learning proposal local paths exist. | Dogfooding host scenario run. |
| P6.1 Vaulted Secrets | Implemented/staging-tested | Repo-local encrypted vault using PBKDF2-HMAC-SHA256 + ChaCha20-Poly1305, `vault-put`, `vault-get`, no-plaintext roundtrip test. | Migration/rotation receipt for existing live secrets. |
| P6.2 Fence Escaping and Adversarial Tests | Implemented/staging-tested | `scan_security_boundaries`, `security-scan` CLI, injection marker test. | Add larger adversarial prompt corpus. |
| P6.3 Shell Lane Hardening | Implemented/staging-tested | Canonical allowed-root shell path check and test. | Hash pinning/env scrubbing for shell job runner. |
| P6.4 Dependency and Disclosure Hygiene | Scaffold implemented; pending broader fixtures | `cargo tree --workspace --duplicates` evidence; `SECURITY.md`. | Network-backed advisory audit and disclosure dry-run. |
| P6.5 Trust Boundary Document | Implemented/staging-tested | `docs/trust-boundaries.md` and SECURITY policy. | Review against real plugin/MCP enablement before live invocation. |
| P7.1 CI | Scaffold implemented; pending broader fixtures | Local workspace test command and release checklist documented. | Add GitHub Actions or chosen CI runner. |
| P7.2 Code Structure Split | Mechanism implemented; pending live gate | New roadmap modules are split by concern in `agent-harness-core`; CLI remains large but operator commands are grouped. | Continue splitting CLI/runtime modules in low-risk follow-up PRs. |
| P7.3 Codex Version Pin and Wire Fixtures | Scaffold implemented; pending broader fixtures | Existing Codex app-server schema tests remain; MCP fixtures added. | Pin real Codex version and store sanitized wire fixtures. |
| P7.4 Release and Changelog Discipline | Implemented/staging-tested | `CHANGELOG.md`, `docs/release-checklist.md`, `release-checklist` CLI. | Apply checklist on first tagged release. |
| P7.5 Schema Registry | Implemented/staging-tested | `schema_registry_entries`, `schema-registry` CLI, `docs/schema-registry.md`. | Expand registry whenever new schemas land. |

### Pending Live/Soak Evidence

These are intentionally not marked complete by code-only staging tests:

- Seven-day P2 queue shadow parity summary.
- 30-day unattended SLO summary.
- WinSW/service-wrapper reboot recovery and ordered shutdown proof.
- Telegram/Discord/OpenRouter live smoke receipts, including the optional `小小梨` OpenRouter route.
- Live canary deploy/rollback transcript against a candidate binary.
- Network-backed dependency advisory audit.
- Sanitized real-traffic replay corpus and long-running scenario fixtures.

## External Review Alignment

The external comparative review scored Agent Harness Core against OpenClaw and Hermes across ten dimensions. The backlog keeps the north star as reliability/SLOs, but acceptance gates must also produce evidence that a future review can cite. This makes implementation completion useful for review reassessment without optimizing for scores alone.

| Review dimension | Review baseline concern | Backlog evidence needed for a credible lift | Primary backlog owners |
|---|---|---|---|
| Concurrency and throughput | Runtime queue is file-polled and lacks OpenClaw-style message semantics. | Shadow-mode SQLite queue parity, duplicate-execution race tests, per-session lane tests, burst/admission tests, warm-pool latency benchmarks if implemented. | P2, P3 |
| Persistence and data integrity | WorkerStore is strong, but runtime queue and some mutable state are split across JSONL/files. | Atomic-write audit, crash/write-fault tests, WorkerStore channel-turn cutover proof, compaction/backup manifests, restore drill receipts. | P0, P2, T4 |
| Error handling and recovery | Conservative fail-stop is predictable but weak for unattended recovery. | Error classification tests, retry/backoff/dead-letter receipts, automatic restart proof, dead-letter user notification samples, trace evidence for timeout/cancel/failure. | P0, P1, T2 |
| Supervision and operations | No true self-supervising restart path; hidden PowerShell loops are not enough. | `supervise` child receipts, reboot recovery result, kill-loop restart test, crash-loop breaker alert, canary rollback transcript, `/healthz` sample. | P1 |
| Security | Strong local/no-listener posture, but plaintext secrets and shell/plugin/MCP trust boundaries remain. | Vault migration receipts, adversarial prompt tests, shell escape tests, MCP tool-description pinning tests, trust-boundary doc, dependency audit result. | P6, M3+ |
| Observability and debuggability | Already strong, but lacks unified trace id and one-command causality reconstruction. | End-to-end `traceId` propagation tests, `trace <id>` samples for normal/cancel/timeout/dead-letter, SLO silent-failure detector examples. | P1.7, P1.9 |
| Resource and token efficiency | Good runtime efficiency, but token economics are less mature than Hermes. | Token aggregation reports, >=30% same-session token reduction golden tests, prompt-cache prefix stability assertions, recall/skill ledger evidence. | P4 |
| Extensibility and ecosystem | Intentionally narrow channels/plugins; cannot match ecosystem breadth. | Native MCP protocol fixtures, per-agent tool allow-list tests, openclaw-mem ContextPack/ingest fixtures, clear non-goal boundaries. | P5.4, M |
| Testing and quality engineering | Good local tests, but no CI and little crash-interleaving simulation. | Invariants catalog, deterministic simulation seeds, scenario replay fixtures, CI results, real Codex wire fixtures, schema registry checks. | T, P7 |
| Maturity and continuity | Public repo has low maturity signals, bus factor 1, no release process. | CI badge/results, changelog, release checklist, schema registry, SECURITY.md, public export hygiene proof, live 30-day soak summaries. | P7, P1 |

Minimum evidence bundle for each phase:

| Phase | Evidence bundle required before marking complete | Review dimensions expected to improve |
|---|---|---|
| P0 | Atomic-write audit, retry/dead-letter fixtures, config validation fixtures, log rotation/compaction sample, queue retry/skip receipts. | Persistence, error recovery, observability, testing |
| P1 | Kill/restart transcript, reboot or equivalent restart proof, crash-loop alert, `/healthz` JSON sample, `trace <id>` samples, canary rollback proof, metrics snapshot. | Supervision, observability, error recovery, maturity |
| P2 | Seven-day shadow summary, divergence receipts if any, race tests, performance fixture for large ledgers, backup/compact manifest. | Persistence, concurrency, error recovery, testing |
| P3 | Scoped `/stop` tests, collect/interrupt tests, burst/admission tests, background registry status sample, long-task heartbeat/blockage receipts. | Concurrency, operations, observability, recovery |
| P4 | Token reports, golden prompt bundle diffs, >=30% token reduction proof, cache-prefix assertions, recall/skill citation ledger samples. | Token efficiency, observability, testing |
| P5 | Budget race tests, native MCP protocol fixtures, tool-call latency benchmark, proposal/quarantine receipts, learning-loop default-on/propose-only proof, and auto-apply-off-by-default proof. | Extensibility, safety, maturity, token efficiency |
| T | `docs/invariants.md`, seeded simulation output, sanitized replay fixtures, restore drill receipt. | Testing, persistence, recovery |
| M | ContextPack fixtures, ingest idempotency test, MCP memory fail-open proof, tool-description hash drift test, provenance trace sample. | Extensibility, security, observability |
| P6 | Vault migration/rotation proof, shell hardening tests, adversarial injection fixtures, dependency audit, trust-boundary doc. | Security, maturity |
| P7 | CI run, schema registry, Codex wire fixtures, changelog/release checklist, public hygiene report. | Testing, maturity, extensibility |

## Phase Order

| Track | Purpose | Entry gate | Exit gate | Review evidence focus |
|---|---|---|---|---|
| P0 Foundation | Close integrity/retry/config gaps before larger migrations | current live baseline ready | no silent retry loops, config fails closed, log growth bounded | persistence, recovery |
| P1 Supervision and observability | Make unattended operation measurable and recoverable | P0 complete | restart, alert, trace, health, and canary rollback verified | supervision, observability |
| P2 Queue unification | Move channel turns into the durable worker store safely | P0/P1 complete | seven-day shadow parity, then SQLite execution source | persistence, concurrency |
| P3 Message semantics and background work | Make turns, jobs, and services separately controllable | P2 complete | scoped stop, long-task registry, rate/admission controls verified | concurrency, operations |
| P4 Token efficiency | Reduce repeated context with proof | P2 complete | token drop and golden bundle order verified | token efficiency |
| P5 Autonomy and learning loop | Add agent self-maintenance behind review gates | P0-P4 complete | budget-safe, proposal-only, fully receipted autonomous workflows | extensibility, safety |
| T Testing spine | Prove crash/interleaving invariants | starts immediately | seeded replay and restore drills become release gates | testing, recovery |
| M openclaw-mem collaboration | Keep memory integration external, fail-open, and contract-tested | can run in parallel | ContextPack, ingest, MCP/provenance contracts verified | extensibility, observability |
| P6 Security | Harden trust and secret boundaries | runs in parallel | secret vault choice implemented, shell/MCP trust boundaries tested | security |
| P7 Quality/publication | Make the public project maintainable | runs in parallel | CI, release, schema, and wire-fixture discipline in place | maturity, testing |

## P0 - Foundation Cleanup

### P0.1 Atomic Write Audit

Implementation steps:

- Audit residual `fs::write` and direct JSON writes in `crates/agent-harness-core` and `crates/agent-harness-cli`.
- Convert mutable JSON state writes to the existing atomic writer path.
- Classify intentional append-only JSONL writes separately from mutable state files.

Acceptance standard:

- A documented audit list exists.
- Mutable state writes use atomic replacement.
- Fault-injection or kill-during-write tests show no corrupted JSON state.

### P0.2 Transient Error Policy, Backoff, and Dead Letter

Implementation steps:

- Classify runtime/channel/worker errors as transient, terminal, canceled, timeout, or operator-blocked.
- Add attempt cap of 3 for transient runtime/channel jobs.
- Apply exponential backoff with jitter where repeat execution is safe.
- After the cap, write a dead-letter receipt and enqueue a user-visible error notification.

Acceptance standard:

- A transient provider failure retries up to 3 attempts, then dead-letters.
- Dead-letter includes trace id, queue/job id, error class, next operator command, and notification receipt.
- Loop remains healthy after exhausted retries.

### P0.4 Receipt Noise and Log Rotation

Implementation steps:

- Identify repeated low-value receipts in `state/logs/harness.jsonl`.
- Add rotation bounds for harness logs and high-volume receipts where compaction is safe.
- Preserve audit receipts needed for trace reconstruction.

Acceptance standard:

- Long-running live operation keeps log sizes within configured limits.
- `trace <id>` still reconstructs complete causality after rotation/compaction.
- Rotation writes its own receipt.

### P0.5 Queue Retry and Skip Commands

Implementation steps:

- Add operator CLI commands for retrying or skipping queue/dead-letter items.
- Require an explicit id and reason.
- Write receipt entries for retry/skip actions.

Acceptance standard:

- Retrying a timed-out/dead-letter item creates a new queue/job id rather than resurrecting terminal state.
- Skipping an item makes it terminal and excludes it from open-item counts.
- Status surfaces show operator action history.

### P0.6 Fail-Closed Config Validation

Implementation steps:

- Define known config keys and expected types for `harness-config.json`.
- Reject unknown keys, type mismatches, invalid enum values, and invalid concurrency invariants at startup/preflight.
- Add a compatibility escape hatch only if needed, and make it noisy.

Acceptance standard:

- A misspelled flag fails preflight and writes a diagnostic receipt.
- Invalid config never silently falls back to a default.
- Unit tests cover unknown key, wrong type, and invalid enum cases.

## P1 - Supervision and Observability

### P1.1 Supervisor Child Tree

Implementation steps:

- Build a first-class `supervise` process model for runtime, worker, progress, Telegram, Discord outbox, and Discord gateway children.
- Persist child pid, command, start time, last heartbeat, restart count, and stop intent.
- Keep current generated scripts as a compatibility launch path until the supervisor is trusted.

Acceptance standard:

- `supervise status --json` reports every child, pid, heartbeat, and restart count.
- Killing one child is visible within one heartbeat interval.
- Receipts link child lifecycle events to the same supervisor run id.

### P1.2 Restart Backoff and Crash-Loop Breaker

Implementation steps:

- Add per-child restart policy with bounded exponential backoff.
- Add crash-loop threshold and breaker state.
- Distinguish expected stop from crash.

Acceptance standard:

- A killed loop restarts automatically.
- A child that crashes repeatedly trips breaker state and stops restarting until operator action.
- Breaker state sends an operator alert.

### P1.3 Heartbeat Liveness Probe

Implementation steps:

- Add active liveness checks for loops that can wedge without exiting.
- Compare heartbeat age to loop-specific TTL.
- Mark stale separately from stopped.

Acceptance standard:

- A wedged loop is detected without waiting for process exit.
- Stale state appears in `/healthz`, `status`, and supervisor status.

### P1.4 Operator Alert Through Outbox

Implementation steps:

- Route supervisor and dead-letter alerts through existing Telegram/Discord outbox policy.
- Respect admin/permission gates.
- Deduplicate repeated alerts during crash loops.

Acceptance standard:

- A simulated crash-loop produces one bounded operator alert plus a state receipt.
- Permission-denied channels do not receive alert content.

### P1.5 Ordered Shutdown

Implementation steps:

- Define shutdown order for ingress, progress/outbox, runtime, workers, and background jobs.
- Use stop files or supervisor IPC consistently.
- Drain or explicitly mark in-flight work.

Acceptance standard:

- `supervise stop` exits all children without leaving active leases.
- In-flight work is either completed, canceled, or made resumable with receipts.

### P1.6 OS Persistence, Scheduled Health, Disk Gate, and `/healthz`

Implementation steps:

- Implement WinSW/service-wrapper style supervision as the primary durable Windows strategy; keep Task Scheduler generation as fallback/compatibility.
- Add scheduled `enable-check`/health check execution under that strategy.
- Add disk free-space gate and writable-state gate.
- Implement `/healthz` JSON with readiness, liveness, SLO counters, queue/outbox depth, loop health, and disk state.

Acceptance standard:

- Reboot returns all live loops within 60 seconds.
- Low disk or unwritable state flips readiness to false before data loss.
- `/healthz` exposes no secrets and is local/admin-gated by default.

### P1.7 End-to-End Trace ID and `trace <id>` CLI

Implementation steps:

- Generate `traceId` at inbound receipt creation.
- Propagate `traceId` through channel ingress, queue, prepare, Codex plan/run/complete, outbox, delivery, progress, memory lifecycle, and dead-letter receipts.
- Backfill trace lookup by queue id/session where legacy receipts lack `traceId`.
- Add `trace <id>` CLI that reconstructs one message's causality in a single-page output.

Acceptance standard:

- A normal turn, a command-only turn, a canceled turn, a timeout, and a dead-letter can each be reconstructed by trace id.
- Silent failure detection is definable as ingress receipt without terminal delivery/error receipt.
- `trace` output includes missing-link diagnostics instead of failing silently.

### P1.8 `supervise deploy` Canary and Rollback

Implementation steps:

- Add deploy workflow: stop children, swap binary, keep `.prev`, start children, run canary, commit or rollback.
- Canary includes fake-Codex full pipeline and optional small live-model turn.
- Rollback restores `.prev`, restarts children, and sends an alert.

Acceptance standard:

- A deliberately bad binary or failed canary rolls back automatically.
- Rollback leaves a receipt with old/new binary hashes, canary outcome, and alert id.
- Fake-only canary works when live provider credentials are unavailable.

### P1.9 Metrics and `status --watch`

Implementation steps:

- Add counters and histograms for turns, error classes, queue depth, outbox depth, spawn time, first delta time, delivery latency, and restart counts.
- Surface metrics in `/healthz`.
- Add `status --watch` for operator polling.

Acceptance standard:

- Metrics are stable across restarts and bounded in storage.
- `status --watch` exits cleanly on stop and never blocks behind a wedged runtime item.

## P2 - Unified Queue Shadow Migration

### P2.1 `channel_turn` Lane in Worker Store

Implementation steps:

- Add a `channel_turn` job/lane to SQLite `WorkerStore`.
- Preserve existing queue ids or provide a deterministic mapping.
- Store claimable state, terminal state, agent/channel/session keys, retry metadata, and trace id.

Acceptance standard:

- New inbound turns can be represented completely in SQLite.
- Worker store status equals JSONL queue status during shadow mode.

### P2.2 Shadow Dual-Write and Divergence Receipts

Implementation steps:

- During shadow mode, `channel-receive` writes both JSONL execution source and SQLite shadow state.
- Runtime still executes from JSONL until cutover.
- Compare terminal state machine results after every turn.
- Write `queue-shadow-divergence.jsonl` for any mismatch.

Acceptance standard:

- Seven live days show zero divergence before cutover.
- Divergence fixtures prove mismatches are detected and actionable.

### P2.3 Remove Runtime Queue Lease Split-Brain

Implementation steps:

- After cutover, retire JSONL runtime lease files, lock files, and `runtime_capacity_blocker` logic for channel turns.
- Use WorkerStore lease/concurrency gates as the execution authority.

Acceptance standard:

- Two concurrent runtime loops cannot execute the same channel turn.
- Terminal state is irreversible and centrally visible.

### P2.4 `ops-compact`, Sequence Cursor, WAL, and `VACUUM INTO`

Implementation steps:

- Add sequence/cursor model for receipts and queue audit.
- Add `ops-compact` for JSONL audit files.
- Enable SQLite WAL and backup via `VACUUM INTO`.

Acceptance standard:

- 100k historical audit lines keep prepare/claim under 10ms in benchmark fixtures.
- Backup and compact produce verifiable manifests and do not break trace reconstruction.

### P2.5 Wake File / Event-Driven Queue Wakeup

Implementation steps:

- Add wake signal on new work.
- Let loops sleep without hot polling while preserving periodic health checks.

Acceptance standard:

- New work wakes the loop within the configured target.
- Idle CPU usage decreases compared with polling baseline.

### P2.6 Cutover, Drain, and JSONL Retirement

Implementation steps:

- Flip flag only after seven-day shadow parity.
- Drain incomplete JSONL items into SQLite or terminal operator states.
- Keep JSONL as audit-only for one release, then retire write path.

Acceptance standard:

- Rollback path is documented before flag flip.
- No open JSONL-only turns remain after drain.

## P3 - Message Semantics and Background Work

### P3.1 Scoped `/stop`

Implementation steps:

- Extend `/stop` into `/stop turn`, `/stop jobs`, and `/stop job <id>`.
- Link runtime turns to background jobs through trace/session/job ids.
- Preserve current `/stop` as active-turn plus interruptible linked jobs for the same session.

Acceptance standard:

- `/stop turn` never kills unrelated background jobs.
- `/stop job <id>` stops the target registered job, records stopped receipt, and releases resources.
- `/stop jobs` lists jobs needing explicit ids.

### P3.2 Message Modes: Steer, Followup, Collect, Interrupt

Implementation steps:

- Formalize semantics for steer, followup, collect, and interrupt.
- Store mode transitions in session/channel state.
- Ensure collect and interrupt behavior is visible in prompt bundle and receipts.

Acceptance standard:

- Burst followups merge or queue according to documented semantics.
- Interrupt affects only the current target turn.
- Collect mode does not trigger unintended model turns.

### P3.3 Per-Session Serial Lane and Caps

Implementation steps:

- Use `concurrency_group_key` or WorkerStore lane policy to serialize per-session work.
- Keep global/per-agent/per-channel caps.

Acceptance standard:

- Same session runs serially unless an explicit policy says otherwise.
- Different channels/agents can still use available capacity.

### P3.4 Warm App-Server Pool

Implementation steps:

- Measure cold-start/spawn overhead before implementation.
- If p95 gain is meaningful, implement a small warm child pool with per-agent affinity.
- Recycle children after N turns, M minutes, protocol error, config change, or credential change.

Acceptance standard:

- Warm pool p95 latency improvement is proven against baseline.
- A child crash affects only its assigned turn.
- Tests prove no cross-session, cwd, auth, or agent context contamination.

### P3.5 Rate Limit and Queue-Depth Admission Control

Implementation steps:

- Add inbound token-bucket rate limits by user/channel/agent.
- Add queue-depth admission threshold with quick refusal and alert.
- Include provider outage/backpressure state in refusal message.

Acceptance standard:

- Burst traffic is smoothed or refused without unbounded queue growth.
- Queue flood test triggers admission refusal, not resource exhaustion.

### P3.6 Background Registry in Worker Store

Implementation steps:

- Store background lifecycle state in SQLite, not a new JSONL job store.
- Track kind, parentQueueId, traceId, platform/channel/user/session/agent, owner, pid/tree, sockets, artifact root, cancel strategy, TTL, heartbeat, and user-visible status.
- Keep `agent.long_task.*` and background audit receipts as JSONL.

Acceptance standard:

- Local server, watcher, detached cron, media upload, and long-running task records appear in status.
- `gateway status` remains under 5 seconds when a runtime item or background service is wedged.
- No new `background-jobs.jsonl` execution authority is introduced.

### P3.7 Long-Task Contract

Implementation steps:

- Add managed commands such as `long-task-start`, `long-task-status`, and `long-task-watch`.
- Emit `agent.long_task.accepted.v1`, `status.v1`, `completed.v1`, and `blocked.v1`.
- Require accepted receipt within 10 seconds and heartbeat every 30-60 seconds.
- For generated images, hash-check copied outputs and delete source files under `CODEX_HOME/generated_images` after successful copy.

Acceptance standard:

- A managed long task stays active while heartbeat is fresh even if chat/app-server idle timeout expires.
- Stale heartbeat plus dead process marks blocked with diagnostics.
- Image artifact handoff is hash-verified and source cleanup is receipted.

## P4 - Token Telemetry and Efficiency

### P4.1 Token Aggregation

Implementation steps:

- Aggregate runtime token usage across sessions, agents, models, job kinds, and prompt sections.
- Expose summaries in status/metrics without storing secrets or raw prompts unnecessarily.

Acceptance standard:

- Per-agent/model/job-kind token reports are reproducible from receipts.
- Missing provider token usage is represented as unknown, not zero.

### P4.2 Identity/Runtime Dedup and Prompt-Cache Prefix Stability

Implementation steps:

- Deduplicate stable identity/runtime context across same-session turns.
- Keep stable prompt sections before volatile sections.
- Avoid timestamps, queue ids, or trace ids in stable cache-prefix sections.

Acceptance standard:

- Same-session second-turn input tokens drop at least 30% in golden scenarios.
- Golden tests assert prompt section order and stable-prefix purity.

### P4.3 Memory Recall Ledger

Implementation steps:

- Assign citation ids to recalled memory chunks.
- Record selected/not-selected memory, token cost, source, and trace id.
- Share ledger shape with Track M ContextPack provenance.

Acceptance standard:

- A final answer can be tied back to cited memory records where applicable.
- Recall ledger does not inject raw secrets.

### P4.4 Skill Progressive Disclosure

Implementation steps:

- Load skill summaries first, full bodies only on demand.
- Add `skill_get`-style retrieval semantics aligned with future MCP/native tool paths.

Acceptance standard:

- Irrelevant skill bodies are not injected.
- Golden tasks still select required skills and can retrieve full bodies deterministically.

### P4.5 Job-Kind Model Routing

Implementation steps:

- Add routing policy by job kind, agent, model capability, cost, and latency.
- Keep explicit user `/model` overrides higher priority where policy allows.

Acceptance standard:

- Deterministic jobs avoid LLM routes.
- High-cost model routes are visible in receipts and metrics.

### P4.6 Session Rotation and Handoff Summary

Implementation steps:

- Define rotation triggers by token budget, age, provider state, or session health.
- Write handoff summaries before rotation.
- Preserve Codex binding continuity where possible.

Acceptance standard:

- Rotated sessions keep enough context to answer continuity regression tests.
- Rotation receipts include reason and summary artifact pointer.

### P4.7 Golden Bundle Tests

Implementation steps:

- Add golden prompt bundles for command-only, normal turn, memory-heavy, skill-heavy, and reply/media contexts.
- Include token budget, section order, dedup, and prompt-cache assertions.

Acceptance standard:

- Golden fixtures fail on accidental prompt-section churn.
- Expected token reductions are measured and stored.

## P5 - Autonomy and Learning Loop

### P5.1 Task Entities

Implementation steps:

- Store durable task entities as `task.json` documents with JSONL checkpoints.
- Keep hot lifecycle counters in SQLite only when they are queue-like.

Acceptance standard:

- Operators and agents can inspect a task file directly.
- Task state changes have checkpoint receipts and trace links.

### P5.2 Heartbeat

Implementation steps:

- Add deterministic heartbeat prefilter that wakes agents only on meaningful signals.
- Keep idle-token cost at zero.

Acceptance standard:

- Seven-day unattended heartbeat run shows no idle LLM calls.
- Stuck tasks become blocked or alerted within two heartbeat windows.

### P5.3 Budget Counters

Implementation steps:

- Store budget counters and leases in SQLite.
- Check budget before MCP/tool execution and before LLM job enqueue.
- Support per-agent, per-tool, per-day, and per-job-kind budgets.

Acceptance standard:

- Budget cannot be exceeded under concurrent requests.
- Over-budget work is deferred or blocked with receipts, not dropped.

### P5.4 Native MCP Server

Implementation steps:

- Implement `agent-harness mcp-serve` as native Rust JSON-RPC-over-stdio.
- Support MCP initialize, `tools/list`, and `tools/call` for approved harness tools.
- Apply per-agent tool allow-lists, budget checks, permission gates, and receipts in-process.
- Retire Node MCP sidecar design for harness-owned MCP tools.

Acceptance standard:

- MCP tool-call p95 is under 50ms for in-process calls.
- Protocol fixtures cover initialize/list/call, unsupported methods, malformed input, and graceful shutdown.
- Every tool call has budget, policy, and trace receipts.

### P5.5 Chaining

Implementation steps:

- Add bounded agent/job chaining through WorkerStore groups.
- Require explicit max-depth, max-cost, and wake policy.

Acceptance standard:

- Chained jobs cannot exceed depth/budget.
- Parent wakeup receives bounded artifact summaries.

### P5.6 Drift Detection

Implementation steps:

- Detect divergence between intended agent configuration, active runtime config, memory policy, and generated Codex config.
- Surface drift in status and health.

Acceptance standard:

- Intentional config changes write receipts.
- Unapproved drift blocks or warns according to policy.

### P5.7 Learning Loop

Implementation steps:

- Add config root with learning enabled by default in safe propose-only mode; auto-apply stays opt-in by config and scope.
- Implement skill provenance and proposal CLI: `skill-propose`, `skill-apply`, `skill-proposals`, injection scan, never-delete archive, agent-created provenance frontmatter.
- Append skill usage receipts with selected skills and run outcome.
- Add transcript/session search using SQLite FTS5 trigram tokenizer for CJK.
- Add memory nudge that suggests `memory-hook store-propose` or `skill-propose` at configured intervals.
- Add signal-gated background review triggered by user correction, error recovery, or large workflows; output is proposal JSON only.
- Add curator for deterministic stale/archive transitions first, then optional LLM consolidation.
- Add local user-model patch proposals through the same propose/apply path.

Acceptance standard:

- Learning is default-on only for propose-only behavior; auto-apply defaults off and requires explicit config.
- Admin DM can propose; lower-trust contexts are quarantined unless explicitly allowed.
- Bundled, imported, and agent-created skills may all receive patch/archive proposals.
- Any skill mutation writes backup artifacts and receipts before applying.
- Direct destructive deletion is not part of the default policy unless explicitly enabled by the operator.
- Proposal content passes deterministic injection/exfiltration scans.
- Every proposal, apply, quarantine, review, curator decision, and transition writes receipts.

## Track T - Testing Spine

### T1 Invariants Catalog

Implementation steps:

- Create `docs/invariants.md`.
- Define at least these invariants: one allowed inbound triggers at most one model turn; every completed turn has exactly one delivery or dead-letter notification; terminal states are irreversible; cancel only affects target; crash recovery loses no work and duplicates no side effects; over-budget work is deferred not lost; ingress always has a terminal trace chain.

Acceptance standard:

- Each invariant maps to at least one test or planned simulation assertion.
- New state machines must name affected invariants in review notes.

### T2 Seeded Deterministic Simulation

Implementation steps:

- Introduce injectable clock and fault layer around critical fs/SQLite operations.
- Split queue/lease/runtime loops into single-step functions where practical.
- Use seeded schedules for kill, write failure, clock rewind, duplicate wake, and process restart.
- Start with P2 queue/lease/terminal state, then extend to chaining and budget.

Acceptance standard:

- Failing schedules print a reproducible seed.
- Simulation asserts T1 invariants at the end of every schedule.
- CI runs a bounded seed set; local/nightly can run larger sets.

### T3 Scenario Replay Evals

Implementation steps:

- Sanitize real transcript plus receipt traces into fixtures.
- Replay through fake Codex and channel adapters.
- Use as offline canary input for P1.8.

Acceptance standard:

- Replay fixtures cover normal, command-only, cancel, timeout, background, and memory-heavy scenarios.
- Deploy canary fails on output or receipt-chain regression.

### T4 Restore Drills

Implementation steps:

- Add restore procedure for `ops-backup` artifacts.
- Schedule monthly maintenance-lane restore drill.
- Run invariants after restore.

Acceptance standard:

- Backup, destroy-state, restore, and invariant check are executable as an operator drill.
- Drill receipts prove restore was actually tested.

## Track M - openclaw-mem Collaboration

Track rules:

- Do not route through the legacy OpenClaw gateway.
- Do not patch `openclaw-mem` internals for harness-only behavior.
- Fail open: memory service failures degrade recall quality but do not block agent turns.

### M1 ContextPack v1 Contract

Implementation steps:

- Define ContextPack v1 file schema for memory context exchange.
- Enforce size bounds, source metadata, and fail-open behavior.

Acceptance standard:

- Missing or invalid pack produces warning receipts and continues without memory injection.
- Valid pack is injected as bounded untrusted context.

### M1+ Consumer-Driven Contract Tests

Implementation steps:

- Add shared fixtures for valid pack, oversized pack, and missing-field pack.
- Keep fixtures usable by both harness and `openclaw-mem` CI.

Acceptance standard:

- Harness tests prove current parser behavior against all fixtures.
- Schema drift requires fixture update and version bump.

### M2 Receipt Ingest Contract

Implementation steps:

- Define memory receipt ingestion format for observations, episodes, citations, and tool-result handoff.
- Keep ingest append-only and idempotent.

Acceptance standard:

- Duplicate ingest does not create duplicate effective observations.
- Ingest receipts link to trace id and source artifact.

### M3 openclaw-mem MCP Direct Path

Implementation steps:

- Register approved `openclaw-mem` MCP tools in Codex config when enabled.
- Enforce timeouts and automatic fail-open fallback.

Acceptance standard:

- MCP memory recall p95 <= 300ms when service is healthy.
- Timeout or service failure degrades to snapshot/local recall and writes receipts.

### M3+ MCP Tool Description Pinning

Implementation steps:

- Treat MCP tool descriptions as prompt-injection surface.
- Pin reviewed tool-description hashes.
- Make `enable-check` detect changed descriptions and require operator confirmation.

Acceptance standard:

- A changed MCP tool description blocks or warns according to policy before model exposure.
- Trust-boundary documentation includes MCP descriptions as semi-trusted text.

### M4 End-to-End Memory Provenance

Implementation steps:

- Link recall, citation, answer, store proposal, approved store, and later recall through provenance ids.

Acceptance standard:

- An answer using memory can be traced to memory source and later writeback.
- Store proposal never mutates authoritative memory without approval.

### M5 Harness Dogfooding Host

Implementation steps:

- Use agent-harness-core as a dogfooding host for `openclaw-mem` flows.
- Keep evidence in receipts and fixtures, not private-only assumptions.

Acceptance standard:

- Dogfood scenario replay covers recall, proposal, approval, writeback, and degradation.

## P6 - Security Hardening

### P6.1 Vaulted Secrets

Implementation steps:

- Implement a repo-local encrypted vault suitable for future cross-platform use.
- Migrate Telegram, Discord, OpenRouter, memory, and provider secrets.
- Add rotation playbook and redacted receipts.

Acceptance standard:

- Secrets are not stored in plaintext env files for long-lived production use.
- Vault file format, key derivation/unlock flow, backup/restore story, and cross-platform constraints are documented.
- Migration leaves redacted receipts and never prints raw secret values.

### P6.2 Fence Escaping and Adversarial Tests

Implementation steps:

- Harden prompt/context fences and untrusted inbound sections.
- Add adversarial prompt injection fixtures for reply/media/memory/tool descriptions.

Acceptance standard:

- Injection fixtures cannot break section boundaries or promote untrusted context to instruction authority.

### P6.3 Shell Lane Hardening

Implementation steps:

- Canonicalize allowed script paths.
- Pin hashes for approved scripts where practical.
- Scrub environment and cap stdout/stderr.

Acceptance standard:

- Path traversal, symlink/junction escape, env leak, and output flood tests fail closed.

### P6.4 Dependency and Disclosure Hygiene

Implementation steps:

- Add `cargo audit` or equivalent advisory check.
- Add `SECURITY.md`.
- Define private/public export hygiene checklist.

Acceptance standard:

- CI or release gate flags known vulnerable dependencies.
- Public export excludes secrets, private runtime state, `.agent-harness`, `.debug`, `.review`, and generated private artifacts.

### P6.5 Trust Boundary Document

Implementation steps:

- Document trust levels for channel messages, reply context, memory, plugin manifests, MCP tool descriptions, shell jobs, and operator commands.
- Include M3+ hash-pinning policy.

Acceptance standard:

- Security-sensitive code reviews can point to one trust-boundary document.

## P7 - Quality Engineering and Public Release

### P7.1 CI

Implementation steps:

- Add CI for formatting, tests, audit, golden fixtures, simulation seed subset, and public export hygiene.

Acceptance standard:

- CI blocks release on failing docs/schema/golden/simulation checks.

### P7.2 Code Structure Split

Implementation steps:

- Split oversized `main.rs`, `codex_runtime.rs`, and other large modules along existing ownership boundaries.
- Avoid behavior changes during pure moves.

Acceptance standard:

- Refactor diffs are mechanical and covered by unchanged tests.
- Module ownership is documented in dev handoff.

### P7.3 Codex Version Pin and Wire Fixtures

Implementation steps:

- Pin tested Codex CLI/app-server version.
- Record real JSON-RPC wire fixtures for supported protocol events.
- Add compatibility tests for phase split, token usage, cancel, and completion.

Acceptance standard:

- Codex protocol drift is detected before live deployment.

### P7.4 Release and Changelog Discipline

Implementation steps:

- Add release checklist and `CHANGELOG.md`.
- Tie release entries to schema changes, flags, migrations, and rollback notes.

Acceptance standard:

- Every public release has version, changelog, migration notes, and verification record.

### P7.5 Schema Registry

Implementation steps:

- Create one schema registry for receipts and state files: name, version, fields, compatibility policy, and owner module.
- Require version bump for breaking changes.

Acceptance standard:

- New receipts/state files cannot be added without registry entry.
- Old-version readers remain for one release cycle after breaking changes.

## Icebox / Explicit Non-Goals

| Item | Decision | Reconsider only if |
|---|---|---|
| Full Tokio/async rewrite | Do not do now. Blocking child processes and file/SQLite state remain the real hot path. | HTTP clients, delivery, queue, and orchestration are all ready to move together. |
| New channels such as WhatsApp, Slack, Matrix | Do not add. | A real personal use case appears and maintenance cost is accepted. |
| TypeScript plugin SDK rewrite | Do not build. Plugin extensibility should move through MCP. | Never for current product direction. |
| Web dashboard | Do not build. `/healthz`, `trace`, and `status --watch` are the operator surfaces. | Multiple operators become a real requirement. |
| Multi-user / multi-tenant | Out of scope. | Product positioning changes. |
| Cloud deployment | Out of scope. Local Windows is a design constraint. | No planned reconsideration. |
| In-house vector/rerank/memory governance | Out of scope. Use Track M and `openclaw-mem`. | No planned reconsideration. |
| Benchmark leaderboard chasing | Out of scope. Reliability SLOs are the target. | No planned reconsideration. |

## Notes and Adjustments

I agree with the roadmap's main direction. These are implementation constraints to keep the plan coherent with the current repo:

- The roadmap's correction to round3-2 B1 is accepted: background job lifecycle state should go into the SQLite worker store. JSONL remains audit/receipt only.
- The native MCP direction is accepted, but the first implementation should target a minimal supported MCP subset with protocol fixtures instead of trying to implement every optional MCP feature at once.
- Warm app-server pooling is useful only if measurement proves a meaningful p95 gain. It must also prove no session, cwd, auth, or agent-context contamination.
- Vaulted secrets direction is decided: use a repo-local encrypted vault for cross-platform portability.
- Canary deploy must support fake-only canary in credential-constrained environments; the live small-model canary should be an optional stronger gate.
- `/healthz` should default to local/admin-gated access and must never expose secrets or raw prompt/memory content.
