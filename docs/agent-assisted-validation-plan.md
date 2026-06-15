# Agent-Assisted Validation Plan

Date: 2026-06-15

This note lists the validation work that can be delegated to agents or subagents when changing `agent-harness-core`. It is meant to complement the operations handbook and release checklist, not replace them.

## Principles

- Keep live gateway control on the main operator path. Agents may inspect live state, but stop/start/restart, binary replacement, and cutover apply must stay with the main operator flow.
- Prefer staging homes, staging target directories, and synthetic queue entries for behavior validation.
- Give subagents bounded ownership: read-only inspection, diff review, test matrix review, documentation gap checks, or isolated fixture/test work.
- Give every subagent wait a concrete `timeout_ms`; never depend on an unbounded wait. Timed-out, irrelevant, or invalid-dispatch subagents should be closed or retried at most once with a narrower prompt.
- Treat receipts and status JSON as the source of truth. Human-readable logs are supporting evidence only.
- Validation should prove isolation properties, not only that commands exit successfully.

## Recommended Agent Roles

| Role | Scope | Should Not Do | Evidence |
| --- | --- | --- | --- |
| Architecture reviewer | Review queue, worker, cron, runtime, status, and docs for contract gaps | Edit live state or restart gateway | Notes with file/line references and risk level |
| Test matrix reviewer | Map changed code paths to targeted and workspace tests | Declare coverage based only on test names | Test list with requirement coverage |
| Runtime queue auditor | Inspect pending/prepared/completed runtime state and lease files | Manually delete queue files | `status --json`, `healthz`, queue receipts |
| Cron isolation auditor | Inspect CronRunStore, scheduler receipts, runtime metadata, and worker lanes | Trigger live cron execution unless approved | `cron-runs`, `worker-status`, scheduler receipts |
| Diff reviewer | Review final patch for stale docs, accidental scope creep, and missing guards | Revert unrelated user changes | Findings against changed files |
| Public hygiene reviewer | Check public export and docs for private paths/debug state leaks | Rewrite generated exports without main-agent approval | `public-hygiene` output |

## Validation Items

### 1. Runtime Class Isolation

Goal: cron, interactive, worker, and maintenance runtime work must not block each other through a shared active lease domain.

Agent-assisted checks:

- Confirm queued items carry the expected `runtimeClass`.
- Confirm class-scoped lease files exist under `state/runtime-queue/classes/<class>/runtime-leases.json`.
- Confirm legacy root leases are counted during upgrade but do not become the only isolation mechanism.
- Review `runtime_worker.rs`, `runtime_queue.rs`, and `status.rs` for any remaining root-only lease accounting.

Suggested commands:

```powershell
target\debug\agent-harness.exe status --harness-home .\.agent-harness --json
target\debug\agent-harness.exe healthz --harness-home .\.agent-harness --require-writable-state
```

Pass evidence:

- `status.runtime.classLeases` includes `cron` and `interactive`.
- `activeLeases` are reported per class.
- `openByRuntimeClass` and `queuedByRuntimeClass` distinguish cron from interactive work.

### 2. Cron Worker Lane Isolation

Goal: native LLM cron jobs should enter a dedicated `cron` worker lane before reaching the `cron` runtime class.

Agent-assisted checks:

- Verify scheduler-created LLM worker jobs use `lane=cron`.
- Verify default worker lane caps include `cron`.
- Verify worker status reports lane blockers separately.
- Confirm old `llm` lane cron behavior is only historical state, not new enqueue behavior.

Suggested commands:

```powershell
target\debug\agent-harness.exe worker-status --harness-home .\.agent-harness
target\debug\agent-harness.exe cron-runs --harness-home .\.agent-harness --limit 20
```

Pass evidence:

- `worker-status.config.laneConcurrencyLimits.cron` is present.
- New native cron enqueue receipts show `lane=cron`.
- Worker blockers report `blockedByLaneLimit` without blocking unrelated lanes.

### 3. CronRunStore Control And Recovery

Goal: skipped, retried, quarantined, and unquarantined cron runs must be durable and respected by worker/runtime dispatch.

Agent-assisted checks:

- Review `cron_runs.rs` for terminal-state immutability and guarded writeback.
- Confirm worker dispatch checks CronRunStore before marking jobs running.
- Confirm runtime dispatch tombstones skipped/quarantined cron items instead of preparing them.
- Confirm manual retry clears the relevant scheduler watermark and uses an attempt-aware idempotency key.

Suggested commands:

```powershell
target\debug\agent-harness.exe cron-runs --harness-home .\.agent-harness --limit 50
target\debug\agent-harness.exe worker-status --harness-home .\.agent-harness
```

Targeted tests to keep mapped:

```powershell
cargo test -p agent-harness-core prepare_runtime_queue_item_tombstones_skipped_cron_run -- --test-threads=1
cargo test -p agent-harness-core cron_worker_skips_operator_controlled_run_without_runtime_enqueue -- --test-threads=1
```

Pass evidence:

- Operator-controlled CronRun states are not overwritten by stale runtime receipts.
- Blocked cron worker jobs become canceled/skipped receipts instead of opening runtime work.

### 4. Cron Session Isolation

Goal: cron LLM turns must not contaminate interactive sessions or each other's context.

Agent-assisted checks:

- Verify default cron sessions are one-shot: `cron:<agent>:<entry>:<scheduled_ms>`.
- Verify sticky cron sessions are forced under `cron:<agent>:<entry>:sticky:<suffix>`.
- Verify cron session data lives under `agents/<agent>/cron-sessions/`.
- Review prompt injection ledger keys for collisions with interactive session keys.

Targeted tests:

```powershell
cargo test -p agent-harness-core native_cron_sticky_session_key_is_forced_into_cron_namespace -- --test-threads=1
cargo test -p agent-harness-core run_once_enqueues_native_due_at_once_and_dedupes -- --test-threads=1
```

Pass evidence:

- No configured cron sticky session can resolve to an interactive session key.
- Repeated one-shot cron runs do not reuse interactive continuity ledgers.

### 5. Multi-Agent Fairness

Goal: one agent profile's cron load must not starve another agent or the main interactive lane.

Agent-assisted checks:

- Inspect status summaries by agent and runtime class.
- Review scheduler idempotency keys for `agent_id`, `entry_id`, `scheduledForMs`, and attempt handling.
- Confirm worker capacity considers global, group, channel, lane, and rate limits.
- Use synthetic staging fixtures with at least two agents and multiple cron entries each.

Suggested staging scenario:

```powershell
cargo test -p agent-harness-core cron_runtime_selection_interleaves_agents -- --test-threads=1
cargo test -p agent-harness-core cron_llm_worker -- --test-threads=1
```

Pass evidence:

- Selection interleaves agents when multiple cron queues are ready.
- Per-agent or lane caps produce explicit blockers instead of silent starvation.

### 6. Stuck Dispatch Reclaim And Retry

Goal: stale or invalid worker/runtime dispatch should be reaped, retried, skipped, or quarantined through explicit mechanisms.

Agent-assisted checks:

- Verify worker stale lease reaping keeps receipts and does not duplicate side effects.
- Verify runtime retry-pending work becomes claimable without waiting for an old lease forever.
- Confirm malformed cron worker payloads become retry-pending or terminal with a clear reason.
- Check that invalid dispatch cannot sit in an active lane and block unrelated work.

Suggested commands:

```powershell
target\debug\agent-harness.exe worker-reap-stale --harness-home .\.agent-harness
target\debug\agent-harness.exe worker-status --harness-home .\.agent-harness
target\debug\agent-harness.exe status --harness-home .\.agent-harness --json
```

Pass evidence:

- Stale worker leases are counted and receipted.
- `status.runtime.latestNonIdleRunOnce` shows retry/dead-letter/completed outcomes rather than stale silence.
- Queue blockers are visible in `worker-status` and `status --json`.

### 7. Channel And Progress Safety

Goal: queue validation must not break Telegram/Discord ingress, outbox delivery, or progress panels.

Agent-assisted checks:

- Verify channel queues remain interactive runtime class unless explicitly cron-originated.
- Confirm final channel replies still come from terminal runtime completion/outbox receipts.
- Confirm progress delivery loops remain live after runtime class changes.

Suggested commands:

```powershell
target\debug\agent-harness.exe channel-outbox-plan --harness-home .\.agent-harness --outbox-limit 20
target\debug\agent-harness.exe status --harness-home .\.agent-harness --json
```

Pass evidence:

- Outbox invalid lines are `0`.
- Progress, Telegram, Discord outbox, and Discord gateway heartbeats are non-stale.

### 8. Observability And Operator Surfaces

Goal: operators should be able to tell why work is blocked and how to recover it.

Agent-assisted checks:

- Review `status`, `worker-status`, `cron-runs`, and `cron-run-control` output for missing fields.
- Confirm runtime class, origin, CronRun id, scheduled time, lane, and blocker fields appear where useful.
- Confirm docs explain skip/retry/quarantine/unquarantine without stale cancel-only guidance.

Suggested commands:

```powershell
target\debug\agent-harness.exe status --harness-home .\.agent-harness --json
target\debug\agent-harness.exe worker-status --harness-home .\.agent-harness
target\debug\agent-harness.exe cron-runs --harness-home .\.agent-harness --limit 20
```

Pass evidence:

- Runtime and worker blockers are visible without reading raw SQLite/JSONL files.
- CronRun operator actions are documented and tested.

### 9. Documentation And Skill Drift

Goal: docs, generated help, and bundled skills should describe the same behavior as the code.

Agent-assisted checks:

- Compare changed code paths against `README.md`, operations handbook, configuration docs, release checklist, schema registry, and bundled harness skill text.
- Search for stale phrases such as shared cron/runtime queue, `origin=native-cron`, cancel-only cron control, or `lane=llm` as current guidance.
- Confirm `.debug/` notes are not the only place where release-critical guidance exists.

Suggested commands:

```powershell
git diff --check
target\debug\agent-harness.exe release-checklist
target\debug\agent-harness.exe schema-registry
```

Pass evidence:

- No tracked docs contradict the current cron/runtime isolation design.
- Builtin skill sync writes the expected harness skill version and content during cutover.

### 10. Public Hygiene

Goal: public export must not leak local live state, debug notes, secrets, or private paths.

Agent-assisted checks:

- Run public hygiene against the export root, not repo root when repo root intentionally contains ignored live state.
- Review any new docs for private absolute paths, tokens, debug-only paths, or internal-only receipts.
- Confirm release notes distinguish public-facing and operator-only material.

Suggested command:

```powershell
target\debug\agent-harness.exe public-hygiene --root .public-export\agent-harness-core
```

Pass evidence:

- `forbiddenHits=[]`.

## Suggested Validation Workflow

1. Main agent reads the operations handbook and identifies the affected contracts.
2. Read-only subagents inspect architecture, tests, docs, and public hygiene risk in parallel.
3. Main agent waits on subagents only with explicit timeouts, closes completed/timed-out agents, and keeps live-control decisions on the main path.
4. Main agent implements or reviews code changes and owns live-control decisions.
5. Test reviewer maps each changed contract to targeted tests plus workspace-level checks.
6. Main agent runs staging checks in isolated target directories.
7. Diff reviewer checks final staged patch for unrelated changes and stale guidance.
8. Public hygiene reviewer validates the export root.
9. Main agent performs push and intentional live cutover only after staging evidence is complete.
10. Runtime queue auditor confirms post-cutover `healthz`, `status`, `worker-status`, and `cron-runs`.
11. Main agent writes release evidence and keeps the goal open until every required artifact is verified.

## Minimum Evidence For Round-Level Completion

- `cargo fmt --all --check`
- `cargo check --workspace --target-dir <staging-check-dir>`
- Targeted tests for changed runtime/worker/cron behavior
- Full package or workspace tests appropriate to blast radius
- `git diff --check`
- `public-hygiene --root .public-export\agent-harness-core`
- Push confirmation for the branch containing implementation and evidence docs
- Pre-cutover backup label
- Cutover ticket id
- Candidate binary path
- Post-cutover `healthz ready=true live=true`
- Post-cutover `status --json` readiness summary
- `worker-status` lane/blocker summary
- `cron-runs` active/quarantine summary
- Clean git worktree after evidence commit

## Validation Results - 2026-06-15

Validation mode: live read-only plus local build/test checks. No live gateway stop/start/restart, binary replacement, supervisor control, or live cron config mutation was performed.

### Command Receipts

Live/read-only:

- `target\debug\agent-harness.exe healthz --harness-home .\.agent-harness --require-writable-state` -> `ready=true`, `live=true`, writable state, all seven loop heartbeats present/non-stale.
- `target\debug\agent-harness.exe status --harness-home .\.agent-harness --json` -> `ready=true`, readiness `passed=59`, `warnings=0`, `failed=0`; runtime class summaries present.
- `target\debug\agent-harness.exe worker-status --harness-home .\.agent-harness --json` -> worker pending/leased/running `0`, failedRetryable/failedTerminal `0`, lane caps include `cron=3`; by-lane `cron` has `1` succeeded, `llm` has `76` succeeded.
- `target\debug\agent-harness.exe cron-runs --harness-home .\.agent-harness --limit 50` -> total `1`, active `0`, terminal `1`, quarantined `0`; the run for `2fcf6fd4-e483-4649-8a6f-e6a34988cfe3` succeeded with `runtimeClass=cron` and one-shot session key.
- `target\debug\agent-harness.exe cron-scheduler-run-once --harness-home .\.agent-harness --source-home .\.agent-harness --workspace .\.agent-harness\workspace --dry-run --enable --resume-cron --allow-deterministic-run` -> status `dry-run`, nativeEntries `114`, deterministicEntries `26`, dueCandidates `0`, enqueued `0`, skippedPolicy `140`, errors `0`, warnings `0`.
- `target\debug\agent-harness.exe channel-outbox-plan --harness-home .\.agent-harness` -> outbox invalid `0`, failed_retryable `0`; pending `20` includes local smoke leftovers and native-cron BLOCKED/NO_REPLY items.
- `target\debug\agent-harness.exe release-checklist` -> emitted `agent-harness.quality-report.v1`.
- `target\debug\agent-harness.exe schema-registry` -> includes `agent-harness.cron-scheduler.*`, `agent-harness.cron-runs.v1`, and runtime queue schemas.
- `target\debug\agent-harness.exe public-hygiene --root .public-export\agent-harness-core` -> passed, `forbiddenHits=[]`.

Build/test:

- `cargo fmt --all --check` -> passed.
- `cargo check --workspace --target-dir target\validation-agent-assisted-20260615` -> passed.
- `git diff --check` -> passed before this result append.
- `cargo test -p agent-harness-core prepare_runtime_queue_item_tombstones_skipped_cron_run -- --test-threads=1` -> passed.
- `cargo test -p agent-harness-core cron_worker_skips_operator_controlled_run_without_runtime_enqueue -- --test-threads=1` -> passed.
- `cargo test -p agent-harness-core native_cron_sticky_session_key_is_forced_into_cron_namespace -- --test-threads=1` -> passed.
- `cargo test -p agent-harness-core run_once_enqueues_native_due_at_once_and_dedupes -- --test-threads=1` -> passed.
- `cargo test -p agent-harness-core cron_runtime_selection_interleaves_agents -- --test-threads=1` -> passed.
- `cargo test -p agent-harness-core cron_llm_worker -- --test-threads=1` -> passed (`2` tests).

### Item Results

| Item | Result | Evidence / Notes |
| --- | --- | --- |
| 1. Runtime Class Isolation | PASS | `status.runtime.classLeases` reports `cron`, `interactive`, `legacy`, `maintenance`, and `worker`; `queuedByRuntimeClass` separates `cron=77` from `interactive=203`; `openByRuntimeClass` has only `interactive=72`; cron class active leases `0`. |
| 2. Cron Worker Lane Isolation | PASS | `worker-status.config.laneConcurrencyLimits.cron=3`; by-lane `cron` has one succeeded job; `cron-runs` shows the succeeded native cron run reached `runtimeClass=cron`; worker blockers are all `0`. |
| 3. CronRunStore Control And Recovery | PASS | `cron-runs` shows durable terminal state; targeted tests for skipped cron runtime tombstone and worker skip without runtime enqueue passed. No manual `cron-run-control` mutation was performed during this live validation. |
| 4. Cron Session Isolation | PASS | Live CronRun session key is `cron:main:2fcf6fd4-e483-4649-8a6f-e6a34988cfe3:1781538300000`; sticky namespace and one-shot/dedupe tests passed. |
| 5. Multi-Agent Fairness | PASS | `cron_runtime_selection_interleaves_agents` passed; worker config exposes global/group/channel/lane limits; live status separates cron from interactive open work. |
| 6. Stuck Dispatch Reclaim And Retry | PASS WITH LIMIT | Retry/dead-letter/completed outcomes are visible in `status.runtime.latestNonIdleRunOnce` and receipt summaries; worker blockers are visible and zero. `worker-reap-stale` was not run because it mutates live lease state. |
| 7. Channel And Progress Safety | PASS WITH NOTE | Channel replies remain interactive-originated in status; progress/Telegram/Discord loops are non-stale; outbox invalid `0`. Note: `channel-outbox-plan` still has `20` pending items, including native-cron BLOCKED/NO_REPLY outputs from legacy path jobs. |
| 8. Observability And Operator Surfaces | PASS | `status`, `worker-status`, and `cron-runs` expose runtime class, origin, CronRun summary, lane caps, blockers, class leases, and scheduler summaries without direct SQLite/JSONL reads. |
| 9. Documentation And Skill Drift | NEEDS CLEANUP | Current bundled `agent-windows-harness` skill is aligned with cron runtime isolation. Static search found stale guidance candidates in `docs\activation-readiness-plan.md` and `docs\agent-harness-operations-handbook.md`, including older root lease wording and old `native-cron-enqueue` guidance. |
| 10. Public Hygiene | PASS | `public-hygiene --root .public-export\agent-harness-core` passed with `forbiddenHits=[]`. |

### Remaining Blockers

- Legacy cron payload migration is still incomplete. `cron-scheduler-lint --enable` remains `status=error` with old Linux/container path findings; this is outside the runtime isolation validation but blocks full cron activation.
- Documentation drift cleanup is still needed for older root lease/native cron guidance.
- Full `cargo test --workspace` was not run in this pass; targeted runtime/worker/cron tests plus `cargo check --workspace` passed.

### Operator Conclusion

The harness has recovered to a runnable, isolated state for the cron/runtime control plane. Full cron activation should wait for the existing Priority A legacy-job isolation package and follow-up path migration.
