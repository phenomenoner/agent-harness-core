# Agent Worker Dispatch Strategy

Date: 2026-06-15

Status: Round5 cron/runtime isolation implemented in staging; remaining items are live smoke coverage and external-provider hardening.

This document defines the next cron/subagent direction for Agent Harness. The design is inspired by gbrain Minions as a durable background-work model, but the harness does not borrow gbrain's memory strategy or require its storage stack.

Reference: https://github.com/garrytan/gbrain

## Direction

Cron and subagents should land as one unified worker dispatch system instead of independent runners.

The imported container deployment has two cron families that must stay distinct at the source level:

- Native agent-turn cron from `.openclaw/cron`, which is allowed to create LLM-backed agent or subagent work.
- Extended deterministic crontab/Supercronic-style cron from workspace runner directories, which must stay on a no-LLM deterministic shell path.

Both cron families, plus imported subagent ledgers, converge at the worker execution layer, but cron LLM turns no longer share the same runtime capacity lane as interactive channel turns. Worker dispatch remains the durable submit/lease/retry layer. Runtime dispatch now has class-scoped lanes so `interactive`, `cron`, `worker`, and `maintenance` turns can be capped independently.

## Goals

- Replace fire-and-forget background work with two-phase persistence: submit a durable job before side effects, then persist attempt/result state after execution.
- Survive process crashes, Windows reboots, transient network failures, and worker restarts through recoverable pending/running state.
- Route heavy non-reasoning work such as scraping, API calls, token refreshes, sync jobs, backups, and maintenance scripts away from LLM turns.
- Manage deterministic shell jobs and LLM subagent jobs through one queue, status surface, and receipt model.
- Keep cron LLM runs observable and recoverable through a dedicated CronRunStore, including skip, retry, quarantine, and per-agent/per-job active caps.
- Keep cron LLM sessions isolated from interactive sessions by defaulting to one-shot `cron:<agent>:<entry>:<scheduled_ms>` session keys and `agents/<agent>/cron-sessions/` transcripts.
- Support child jobs, cascading timeout/cancel behavior, exponential backoff, rate leases for external providers, and shell-command audit logging.
- Wake the master agent when delegated work reaches a meaningful boundary, such as all child jobs completed, any child failed, a group timeout fired, or an operator-requested checkpoint is reached.
- Enforce global, per-agent/group, and per-agent-per-channel concurrency limits from harness config so fan-out, cron bursts, simultaneous Telegram/Discord work, and retries queue instead of overloading local resources or LLM/API rate limits.
- Keep the MVP practical for the current Windows single-machine harness while leaving a storage trait for future Postgres/Supabase or service-backed deployments.

## Non-Goals

- Do not copy or depend on gbrain's memory architecture.
- Do not make Postgres mandatory for the Windows MVP.
- Do not allow arbitrary shell execution from untrusted channel messages.
- Do not replace Codex app-server ownership of model/tool/session semantics for LLM turns.

## Job Kinds

| Kind | Purpose | LLM access | Typical source |
|---|---|---:|---|
| `deterministic_shell` | Run allow-listed scripts or maintenance commands. | No | extended deterministic crontab/Supercronic-style cron, operator command, maintenance task |
| `llm_subagent` | Run a durable Codex-backed subagent loop with its own prompt/session/tool context. | Yes | native agent-turn cron, parent agent request, resumed imported subagent |
| `watchdog` | Monitor a group of child jobs and decide whether to wake the master agent. | No | parent agent fan-out, cron fan-out, operator checkpoint |
| `master_wakeup` | Queue a bounded summary turn back to the master agent with child status and artifact pointers. | Yes | watchdog boundary event |
| `channel_delivery` | Optional future lane for provider outbox delivery. | No | Telegram/Discord outbox |
| `memory_maintenance` | Optional future lane for indexing, canvas refresh, compaction, or capture review prep. | Usually no | memory lifecycle scheduler |
| `plugin_call` | Optional future lane for long-running plugin operations. | Policy-gated | plugin bridge |

## Persistence Model

MVP storage uses the Rust/SQLite direction:

- `state/workers/worker-jobs.sqlite` for job state, attempts, leases, dependency links, and rate leases.
- `state/cron-runs/cron-runs.sqlite` for native cron LLM admission, active counts, retry/quarantine controls, runtime queue linkage, and failure summaries.
- `state/workers/audit/*.json` for shell audit and capped stdout/stderr summaries.
- `state/logs/harness.jsonl` for compact operator events.
- `state/runtime-queue/classes/<runtimeClass>/runtime-leases.json` for class-scoped runtime execution leases. The old root `state/runtime-queue/runtime-leases.json` is still read for upgrade compatibility.

Postgres can be added later behind a `WorkerStore` trait when multi-host or shared worker pools are required. The first implementation should not block P4 on that migration.

Core job fields:

- `job_id`
- `kind`
- `lane`
- `parent_job_id`
- `job_group_id`
- `master_agent_id`
- `master_session_key`
- `wake_policy_json`
- `source`
- `payload_json`
- `idempotency_key`
- `priority`
- `available_at`
- `lease_owner`
- `lease_expires_at`
- `attempt`
- `max_attempts`
- `timeout_ms`
- `cascade_timeout_ms`
- `rate_key`
- `concurrency_group_key`
- `audit_path`
- `result_json`
- `artifact_refs_json`
- `created_at`, `updated_at`, `finished_at`

Recommended state flow:

```text
Pending -> Leased -> Running -> Succeeded
                         |----> FailedRetryable -> Pending
                         |----> FailedTerminal
                         |----> Canceled
                         |----> Expired
```

Two-phase persistence:

1. Submit phase: insert the job and idempotency key before any side effect. A repeated submit with the same key returns the existing job.
2. Execute phase: lease the job, persist the attempt record, run the work, then persist terminal or retryable result. A stale lease reaper returns expired work to `Pending` when safe.

## Dispatch Rules

Cron schedulers enqueue jobs, not execute work directly. The two cron source lanes share worker management while preserving different policies:

- Native agent-turn cron admits a CronRun, then enqueues `llm_subagent` jobs on the `cron` worker lane with `runtimeClass=cron`, `origin=cron-scheduler`, `cronRunId`, `scheduledForMs`, and `sessionPolicy`.
- Extended deterministic crontab/Supercronic-style cron enqueues `deterministic_shell` jobs with `llmAccessAllowed=false` and defaults to shell dry-run unless `--execute-shell` is explicit.
- Imported subagent ledgers map to `llm_subagent` jobs with explicit `--resume-subagents`.
- Parent agent flows may enqueue child jobs and wait, poll, or detach according to an explicit parent policy.
- Parent fan-out flows should create a `job_group_id`, enqueue the child jobs, and enqueue a `watchdog` job that evaluates group state against the parent's wake policy.

Workers should be lane-aware:

- `worker-run-once` leases and runs one eligible job.
- `worker-loop` drains selected lanes with idle exit, stop-file support, and bounded consecutive-error handling.
- `worker-reap-stale` recovers expired leases.
- `worker-status` reports pending/running/retryable/succeeded/failed counts by lane and parent.

## Concurrency Limits

Worker dispatch enforces concurrency limits before leasing a job:

- Global worker concurrency limit: maximum concurrently executing worker jobs across the harness.
- Per-agent/group concurrency limit: maximum concurrently executing jobs for one master agent or fan-out group.
- Per-agent-per-channel concurrency limit: maximum concurrently executing jobs for one agent/channel pair, so one busy Telegram or Discord channel cannot consume the whole per-agent budget.

The invariant is `globalConcurrencyLimit >= groupConcurrencyLimit >= channelConcurrencyLimit`. The MVP caps the narrower limits down and surfaces a warning if the invariant is violated.

Suggested `harness-config.json` shape:

```json
{
  "workerDispatch": {
    "globalConcurrencyLimit": 12,
    "groupConcurrencyLimit": 6,
    "channelConcurrencyLimit": 3,
    "laneConcurrencyLimits": {
      "cron": 3,
      "llm": 6,
      "shell": 6,
      "watchdog": 2,
      "maintenance": 2,
      "plugin": 2
    },
    "rateLeaseLimit": 0,
    "rateLeaseWindowMs": 60000,
    "allowedScriptRoots": []
  }
}
```

Lease policy:

- A worker can lease a job only when global capacity, lane capacity, per-agent/group capacity, and per-agent-per-channel capacity are all available.
- If capacity is exhausted, eligible jobs stay `Pending` with their original `available_at` and are retried by later scheduler ticks or worker-loop iterations.
- Cron bursts must enqueue and wait like any other jobs. Cron must not bypass concurrency limits.
- Cron LLM worker jobs are isolated first by the `cron` worker lane and then by cron runtime capacity. A cron job can be accepted into the worker queue but still wait behind `runtimeDispatch.classes.cron` when Codex runtime slots are full.
- A stuck or noisy cron job can be skipped or quarantined through `cron-run-control` without consuming the interactive runtime class. Manual retry clears the scheduler watermark for that run's slot and uses the CronRun attempt number in the worker idempotency key. Worker and runtime dispatch both re-check CronRunStore controls, tombstone skipped runtime items, and avoid overwriting operator skip/quarantine state.
- Fan-out children share a `concurrency_group_key`, usually derived from `job_group_id` or the master agent/session group.
- Deterministic shell jobs and LLM subagent jobs both count against global concurrency; lane limits prevent one lane from starving the other.
- Watchdog jobs should have their own small lane limit and should not be permanently starved by full child lanes, because they are responsible for waking the master agent on timeout/failure/checkpoint boundaries.
- Rate leases remain separate from concurrency limits. A job needs both execution capacity and any required provider/API rate lease before it can run. `rateLeaseLimit=0` disables rate leases; when enabled, jobs sharing a `rate_key` are limited within `rateLeaseWindowMs`.

Fairness policy:

- Selection should avoid repeatedly leasing from one busy master group while other groups have available work.
- MVP can use ordered scan with group-capacity checks; later versions should add round-robin or aging if starvation appears.
- `worker-status` should report blocked-by-global-limit, blocked-by-group-limit, blocked-by-channel-limit, blocked-by-lane-limit, and blocked-by-rate-lease counts.

## Shell Job Policy

Deterministic shell jobs need a stricter contract than free-form command strings:

- Allow-list script roots and script names.
- Store argv as structured JSON.
- Use explicit cwd and environment allow-lists.
- Redact secret-looking values before audit.
- Cap stdout/stderr capture and preserve full logs only under controlled local paths.
- Record exit code, duration, timeout reason, and output hashes in the audit ledger.

## LLM Subagent Policy

LLM subagent jobs should use the same prompt-bundle/Codex runtime foundations as normal turns, but with separate worker identity and parent linkage:

- Each subagent gets a stable session key and transcript/trajectory path.
- Native cron LLM subagents default to one-shot session keys and `cron-sessions` transcript paths so cron turns do not contaminate the main interactive session context. Sticky cron is opt-in and still forced under the `cron:<agent>:<entry>:sticky:<suffix>` namespace.
- Parent job id and parent session metadata are stored in the job payload.
- Child completion writes a result reference, not a raw unbounded transcript, back to the parent.
- Cancellation and timeout cascade from parent to child unless the child was explicitly detached.
- Tool access, channel access, and filesystem access follow the selected agent policy.

## Master Wakeup And Watchdogs

Delegated work must be able to resume the master agent after child work finishes. This applies to LLM subagents and deterministic shell jobs.

The parent/master flow should work as follows:

1. The master agent enqueues one or more child jobs with a shared `job_group_id`.
2. The harness records `master_agent_id`, `master_session_key`, parent job metadata, and a `wake_policy_json`.
3. A deterministic `watchdog` job monitors the child group. It does not call a model.
4. When the wake policy fires, the watchdog enqueues one idempotent `master_wakeup` job.
5. The `master_wakeup` job queues a bounded agent turn to the master session with child status, failures, timeout state, and artifact pointers.

Wake policy examples:

- `all_completed`: wake when every child reached `Succeeded`, `FailedTerminal`, `Canceled`, or `Expired`.
- `all_succeeded`: wake only when every child succeeded.
- `any_failed`: wake immediately when any child reaches terminal failure.
- `timeout`: wake when the group deadline expires, even if children are still running.
- `checkpoint`: wake on a fixed interval or operator-requested status check.
- `threshold`: wake after N of M children complete.

The watchdog should be deterministic and idempotent:

- One child group can have many watchdog attempts, but only one wakeup per wake event key.
- Wakeup idempotency key should include `job_group_id`, wake reason, and policy version.
- A watchdog may reschedule itself with backoff while children are still running.
- A watchdog may cancel or expire children when the group timeout policy requires it.

The master wakeup payload should be bounded and pointer-oriented:

- group id and parent job id
- each child job id, kind, lane, status, attempts, duration, and finish time
- failure summaries and timeout/cancel reasons
- artifact refs such as transcript path, trajectory path, shell audit path, stdout/stderr log path, result JSON path, and receipt path
- redacted/capped previews only, never unbounded logs or raw secret-bearing output
- recommended next action when policy can infer one, such as retry failed child, continue with partial results, or inspect artifact

This preserves the older behavior where a subagent completion wakes the master agent, and extends it to fan-out cases such as "master dispatches three workers plus a watchdog". The same mechanism covers mixed groups containing both LLM subagents and deterministic shell jobs.

## Backoff, Rate Leases, And Timeouts

- Retryable failures schedule `available_at` with exponential backoff and jitter.
- Rate leases prevent concurrent provider/API jobs from exceeding configured outbound limits.
- Concurrency limits prevent local CPU/process/LLM fan-out overload before provider-specific rate leases are evaluated.
- Job timeouts terminate the current attempt and produce retryable or terminal state by policy.
- Parent timeout cascades to children unless the child policy is `detached`.
- Repeated terminal failures stay inspectable in `worker-status` and receipts.

## CLI Shape

Initial CLI surface:

```powershell
agent-harness worker-enqueue --kind deterministic-shell --payload-file payload.json
agent-harness worker-enqueue --kind llm-subagent --payload-file payload.json
agent-harness worker-run-once --harness-home .\.agent-harness --lane shell
agent-harness worker-loop --harness-home .\.agent-harness --iterations 0 --idle-ms 1000
agent-harness worker-status --harness-home .\.agent-harness --json
agent-harness worker-cancel --harness-home .\.agent-harness --job-id <id>
agent-harness worker-reap-stale --harness-home .\.agent-harness
agent-harness cron-runs --harness-home .\.agent-harness --limit 20
agent-harness cron-run-control --harness-home .\.agent-harness --action retry --run-id <cronrun-id> --reason "operator retry"
agent-harness cron-run-control --harness-home .\.agent-harness --action quarantine --agent-id <agent> --entry-id <entry> --reason "bad cron"
```

Cron/subagent adapter commands:

```powershell
agent-harness native-cron-enqueue --harness-home .\.agent-harness --source-home .\imports\openclaw-core-snapshot --resume-cron --master-agent main
agent-harness cron-scheduler-lint --harness-home .\.agent-harness --source-home .\imports\openclaw-core-snapshot --dry-run --enable --resume-cron --allow-deterministic-run
agent-harness cron-scheduler-run-once --harness-home .\.agent-harness --source-home .\imports\openclaw-core-snapshot --dry-run --enable --resume-cron --allow-deterministic-run
agent-harness cron-scheduler-loop --harness-home .\.agent-harness --source-home .\imports\openclaw-core-snapshot --iterations 0 --idle-ms 60000 --max-consecutive-errors 5
agent-harness deterministic-cron-enqueue --harness-home .\.agent-harness --workspace D:\path\to\workspace --allow-deterministic-run --dry-run-shell --master-agent main
agent-harness subagent-enqueue --harness-home .\.agent-harness --source-home .\imports\openclaw-core-snapshot --resume-subagents --master-agent main
```

## P4 Implementation Order

1. Done: `WorkerStore` SQLite schema, job state machine, idempotent enqueue, lease/reap, and status reporting.
2. Done: harness config parsing for global, per-agent/group, per-agent-per-channel, lane concurrency, allowed script roots, and optional rate leases.
3. Done: `worker-enqueue`, `worker-run-once`, `worker-loop`, `worker-status`, `worker-cancel`, and `worker-reap-stale`.
4. Done: deterministic shell lane with script allow policy, structured argv/env, audit logs, timeout, and retry.
5. Done: deterministic crontab/Supercronic-style planning can enqueue shell jobs.
6. Done: LLM subagent/master wakeup lane enqueues runtime queue turns on top of the existing prompt bundle/Codex runner.
7. Done: native agent-turn cron and imported subagent resume can enqueue worker jobs.
8. Done: child job linkage, job groups, group concurrency keys, parent refs, rate leases, deterministic watchdog jobs, and idempotent `master_wakeup` jobs for all-completed, any-failed, timeout, and all-succeeded policies.
9. Done: repeated cron scheduler ticks write durable watermarks and idempotently enqueue due native/deterministic cron jobs through WorkerStore.
10. Done: Round5 CronRunStore admission/status/control, native cron `cron` worker lane plus `runtimeClass=cron`, isolated one-shot and namespaced sticky cron sessions, runtime queue metadata propagation, class-scoped runtime leases, status/CLI reporting, worker/runtime control blockers, skipped runtime tombstones, worker failure sync, and retry watermark/idempotency recovery.
11. Remaining: cascading timeout/cancel for active child processes, external-provider rate profiles, fairness across busy master groups, and live smoke tests for global-limit queueing, deterministic cron, native cron-to-agent, parent-to-subagent, and mixed fan-out/watchdog/master-wakeup flows.
