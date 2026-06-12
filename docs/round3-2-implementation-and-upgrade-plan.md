# Round3-2 Implementation And Upgrade Plan

Date: 2026-06-12

This document turns the round3-2 debug notes into an implementation checklist and upgrade plan.

Source notes:

- `.debug/round3-2/discord-typing-background-task-triage.md`
- `.debug/round3-2/long-running-task-progress-contract-tech-note.md`
- `.debug/round3-2/hermes-agent-learning-loop-analysis.md`

## Implemented In This Patch

The stale Discord typing/progress symptom had one immediate root cause: a queue item could stay
`status:"queued"` in `pending.jsonl` while `run-once-receipts.jsonl` recorded `status:"timeout"`.
Several status surfaces treated that as still open. The patch makes timeout terminal for the parent
runtime turn.

Implemented behavior:

- `timeout` is a terminal run-once status for queue selection, capacity inspection, status open-item
  counts, and native typing context selection.
- `prepare_runtime_queue_item` rejects a requested queue id that already has a terminal run receipt,
  including `timeout`. A retry must be a new queue id, not resurrection of the old id.
- `runtime-loop` treats `Timeout` as a terminal runtime result for loop error accounting. It can
  report the timeout, but it does not poison-retry the same parent queue id as ordinary open work.
- Timeout now writes an error outbox reply so a user sees a terminal result instead of silent retry
  behavior.
- Progress delivery is monotonic: once a queue id has a terminal runtime progress event, later stray
  non-runtime progress events cannot turn the status panel back into non-terminal `Working`.
- Progress delivery cursor state keeps `terminal=true` once observed and will not downgrade it to
  `false`.

Regression coverage:

- CLI typing context ignores a queued item with a timeout receipt.
- Harness status reports a pending queue id with a timeout receipt as `openItems=0`.
- Runtime worker automatic and explicit prepare paths skip terminal queue ids.
- Progress rendering and delivery state remain terminal after late events.
- Full verification passed: `cargo fmt --all`; `cargo test --workspace` with 16 CLI tests, 174 core
  tests, and doctests.
- Deployment verification passed: `cargo build --workspace`, gateway stop/start, live `status`
  (`ready=true`, `passed=58`, `warnings=0`, `failed=0`, runtime `open=0`), live `enable-check`,
  outbox plan (`pending=0`), and process listing for the 6 direct-runner loops.

## Requirement Disposition

| Round3-2 item | Disposition |
|---|---|
| Treat timeout as terminal for parent-turn typing/progress | Implemented. `timeout` is terminal in CLI typing, runtime worker, runtime pipeline, and status. |
| Reconcile queued pending row plus terminal receipt | Implemented as derived reconciliation. Pending JSONL remains append-only; selection/status exclude terminal receipt ids. |
| Prevent terminal progress downgrade | Implemented. Rendering and delivery cursor state are terminal-monotonic. |
| Native typing heartbeat ownership | Partially implemented. Typing no longer starts for queued rows with terminal receipts. A future hard TTL tied directly to app-server lease state is still tracked below. |
| `/status` should expose stale mismatch | Implemented for open-count correctness. A richer mismatch list remains a status-surface enhancement. |
| Bounded `gateway status` | Not fully implemented here. Rust `status` remains direct JSONL/SQLite reads; wrapper-level timeout and background-service scan bounds are tracked below. |
| First-class background task/service contract | Planned upgrade. Existing worker store is the durable base; detached services still need registry support. |
| `/stop job <id>` and linked background cancellation | Planned upgrade. Current `/stop` still cancels the active Codex runtime turn only. |
| Long-running task accepted/progress/status/completion receipts | Planned upgrade. The schema and adapter are defined below. |
| Hermes-style learning loop | Planned upgrade. The proposal is decomposed below into config-gated native modules. |

## Background Task And Long-Task Upgrade

Round3-2 identified two background classes that the current UI collapses into the same symptom:

- Long-running production jobs, such as the Lyria social-image cron run.
- Detached local services, such as a review-site `python -m http.server`.

The upgrade should add a first-class background registry instead of relying on ad hoc process
inspection.

### Phase B1: Background Registry

Superseded storage decision from `docs/agent-harness-core-roadmap-backlog.md`: background lifecycle
state must be stored in the SQLite worker store, not in a new JSONL execution registry. The registry
is queue-like/lease-like/hot-polled state, so it follows the same storage rule as worker jobs and
future channel turns. JSONL remains the audit surface for background and long-task receipts only.

Minimum fields:

- `backgroundJobId`
- `kind`: `long-running-task`, `detached-cron`, `local-server`, `watcher`, `media-upload`
- `parentQueueId`
- `traceId`
- `platform`, `channelId`, `userId`, `sessionKey`, `agentId`
- `createdAtMs`, `lastHeartbeatAtMs`
- `owner`: `runtime`, `worker-loop`, `tool-call`, `operator`
- `pid`, `parentPid`, process tree snapshot
- listening sockets and local URL when present
- artifact root plus stdout/stderr paths
- cancel strategy: stop file, HTTP shutdown, process kill, process-tree kill, or no safe cancel
- TTL/expiration policy
- user-visible status

CLI surface:

- `background-register`
- `background-status`
- `background-stop --job-id <id>`

Status integration:

- `status --json` adds `background` with active/stale/stopped counts and latest jobs.
- `/status` displays background jobs separately from runtime queue open items.
- Worker status remains separate but can link worker job ids to background job ids.
- `gateway status` remains bounded under 5 seconds even when a runtime item or background process is
  wedged.

### Phase B2: Long-Task Contract

Add managed long-task commands:

- `long-task-start --kind <kind> --command <cmd> --status-path <path>`
- `long-task-status --job-id <id>`
- `long-task-watch --job-id <id> --json`

Receipt schemas:

- `agent.long_task.accepted.v1`
- `agent.long_task.status.v1`
- `agent.long_task.completed.v1`
- `agent.long_task.blocked.v1`

Behavior:

- Accepted receipt must be written within 10 seconds.
- Progress heartbeat must be written every 30-60 seconds while active.
- Chat/app-server idle timeout must not mark a job failed when a fresh managed heartbeat exists.
- Stale heartbeat plus dead process marks the job blocked with diagnostic receipts.
- For image-generation workflows, copied generated images must be hash-checked and source files under
  `CODEX_HOME/generated_images` deleted after successful copy.

### Phase B3: `/stop` Semantics

Current `/stop` writes a fresh runtime cancel marker for the active Codex app-server turn. It is not a
general process supervisor.

Upgrade command behavior:

- `/stop` cancels the active runtime turn and interruptible linked background jobs for the same
  session.
- `/stop turn` cancels only the runtime turn.
- `/stop jobs` lists active jobs requiring explicit job ids.
- `/stop job <id>` stops a registered background job/service and writes a stopped receipt.

Acceptance tests:

- A tool-launched local server is registered before process start.
- `/status` lists the server with pid, URL, artifact root, and cancel command.
- `/stop job <id>` stops the server, releases the port, and records a stopped receipt.
- A missing parent PID is displayed as detached/orphaned.
- `gateway status` remains bounded under 5 seconds with a wedged runtime item or background server.

## Hermes-Style Learning Loop Upgrade

The Hermes analysis is not an incident fix. It is an autonomous-learning roadmap. Per the 2026-06-12
operator decision, the harness should implement learning as config-gated native modules with
propose-only behavior enabled by default. Auto-apply is a separate config option and remains opt-in by
scope. Durable worker jobs and receipts remain mandatory.

Config root:

```json
{
  "learning": {
    "skillLearning": { "enabled": true, "applyMode": "propose" },
    "memoryNudge": { "enabled": true, "turnInterval": 6 },
    "backgroundReview": { "enabled": true, "trigger": "signal", "dailyJobCap": 24 },
    "curator": { "enabled": true, "intervalHours": 168, "usageWeighted": true },
    "sessionSearch": { "enabled": true, "tokenizer": "trigram" },
    "userModel": { "enabled": true, "provider": "local", "applyMode": "propose" }
  }
}
```

Implementation order:

1. Skill provenance and propose/apply CLI:
   `skill-propose`, `skill-apply`, `skill-proposals`, injection scanning, never-delete archive, and
   agent-created provenance frontmatter.
2. Skill usage receipts:
   append `state/learning/skill-usage.jsonl` from selected skills plus run outcome.
3. Session search:
   SQLite FTS5 with `trigram` tokenizer for CJK recall over transcript JSONL.
4. Memory nudge:
   prompt-bundle counter that suggests `memory-hook store-propose` or `skill-propose` only when a
   configured interval elapses.
5. Signal-gated background review:
   worker job triggered by user correction, error recovery, or large tool-call workflows; output is
   proposal JSON, not direct file mutation.
6. Curator:
   deterministic stale/archive transitions first, then optional LLM consolidation using usage
   receipts.
7. Local user model:
   `USER.md` patch proposals through the same propose/apply path; Honcho-style SaaS providers stay
   behind plugin sidecars.

Safety constraints:

- Default on means propose-only. Auto-apply is disabled unless explicitly enabled by config for the relevant scope.
- Admin DM can propose; lower-trust group/guild contexts are quarantined unless explicitly allowed.
- Bundled, imported, and agent-created skills can all receive patch/archive proposals.
- Applying any skill mutation must first write backups and receipts.
- Direct destructive deletion is not part of the default policy unless explicitly enabled by an operator.
- Proposal contents go through deterministic injection/exfiltration scans.
- Every proposal, apply, quarantine, review, curator decision, and auto transition writes JSONL
  receipts.

## Verification Checklist

Before public release:

- `cargo fmt --all`
- `cargo test --workspace`
- `target/debug/agent-harness.exe status --harness-home .agent-harness`
- `target/debug/agent-harness.exe enable-check --harness-home .agent-harness`
- `target/debug/agent-harness.exe channel-outbox-plan --target-home .agent-harness --limit 20`
- Public export hygiene check: no `.agent-harness`, `.debug`, `.review`, secrets, generated images,
  or private runtime receipts in the public export.
