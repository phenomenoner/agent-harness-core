# Agent-Assisted Validation Follow-up

Date: 2026-06-16

Source reviewed: `docs/agent-assisted-validation-plan.md`

## Executive Summary

The 2026-06-15 agent-assisted validation report is mostly confirmed. The runtime/worker/cron isolation design is live and healthy: the gateway is ready, all seven loops are non-stale, worker queues are not blocked, CronRun active count is zero, and recent native cron LLM work has completed through the `cron` worker/runtime path.

The remaining risk is not worker starvation anymore. The remaining risk is activation quality for imported legacy cron content and documentation drift around old root-lease/native-cron wording. Full cron cleanliness should not be claimed until `cron-scheduler-lint --enable` returns non-error.

No live gateway stop/start/restart, binary replacement, or mutable live queue cleanup was performed during this follow-up.

## Read-only Confirmation

Commands run on 2026-06-16:

- `target\debug\agent-harness.exe healthz --harness-home .\.agent-harness --require-writable-state`
- `target\debug\agent-harness.exe worker-status --harness-home .\.agent-harness`
- `target\debug\agent-harness.exe cron-runs --harness-home .\.agent-harness --limit 20`
- `target\debug\agent-harness.exe cron-scheduler-lint --harness-home .\.agent-harness --source-home .\.agent-harness --workspace .\.agent-harness\workspace --enable`
- `Select-String` over `docs\*.md` for root lease, `native-cron-enqueue`, `cron-run-control`, `lane=llm`, and `origin=native-cron` drift candidates.

Observed state:

- `healthz`: `ready=true`, `live=true`, writable state, all seven loop heartbeats present and non-stale.
- Runtime queue counters moved forward from the 2026-06-15 report: `queued=281`, `open=71`, `prepared=60`, `completed=262`.
- Outbox counters moved forward: `pending=235`, `retryable=0`, `delivered=132`, `invalid=0`.
- `worker-status`: pending/leased/running `0`, failures `0`, blockers `0`; lane caps include `cron=3`.
- Worker lane totals now show `cron=2 succeeded` and `llm=76 succeeded`.
- `cron-runs`: total `2`, active `0`, terminal `2`, quarantined `0`, both succeeded with `runtimeClass=cron` and one-shot `cron:<agent>:<entry>:<scheduled_ms>` session keys.
- `cron-scheduler-lint --enable`: still `status=error`, summary `findings=128`, `errors=65`, `warnings=63`, `nativeEntries=114`, `deterministicEntries=26`.

## Confirmation Against The Agent Report

Confirmed:

- Runtime classes are separated. `cron` and interactive work do not share the same active lease domain.
- Worker lane isolation is functioning. Cron work has its own lane cap and has completed without blocking other worker lanes.
- CronRunStore is doing useful accounting. Terminal cron runs are durable and visible through the operator surface.
- Cron sessions are isolated from interactive sessions by one-shot `cron:*` session keys.
- Observability is adequate for current operation: `healthz`, `status`, `worker-status`, and `cron-runs` expose enough state to diagnose active blockers without opening raw SQLite/JSONL files.

Adjusted from the report:

- The report listed one succeeded CronRun. Current readback now shows two succeeded CronRuns.
- The report noted pending outbox items as local smoke/native-cron leftovers. Current outbox pending count is higher, so this should be triaged as a separate housekeeping item, even though `retryable=0` and `invalid=0`.
- `cron-scheduler-lint` now reports 128 findings rather than the earlier 91/65/26 mix documented in older notes. The error count remains 65, so the blocking condition is unchanged.

## Remaining Risks

1. Legacy cron content is not clean for full activation.
   - `native-runtime-path-mismatch` remains the blocking error class.
   - The affected native cron messages reference Linux absolute paths while the active runtime is Windows.
   - Deterministic cron entries also need WSL/Git Bash/native-port decisions before execution can be claimed clean.

2. Some native cron entries still target `main`.
   - This is now operationally safer because `runtimeClass=cron` isolates runtime capacity.
   - It is still a design smell for long-term multi-agent operation. Prefer cron-specific agent profiles where the job is operational/background work.

3. Documentation drift remains small but real.
   - `docs\activation-readiness-plan.md` still has older root runtime lease wording.
   - `docs\agent-harness-operations-handbook.md` still has a runtime-loop paragraph that emphasizes the legacy root lease path and a `native-cron-enqueue` section that can read like current scheduler guidance.
   - `docs\agent-harness-dev-handoff.md` still mentions `native-cron-enqueue --resume-cron`; that should be framed as historical/manual adapter guidance, not the preferred live scheduler path.

4. Full workspace test coverage was not part of this follow-up.
   - The prior agent report already ran targeted tests plus `cargo check --workspace`.
   - No code change was made in this follow-up, so this is not a release blocker by itself.

## Recommended Next Backlog

## Harness Agent Collection Request

Use this section as the prompt/checklist for an Agent Harness live agent. The agent should collect read-only evidence and append results to this same file under a new section named `## Harness Agent Collection Results - <date>`.

### Scope

Collect enough structured information for the repo-side Codex operator to implement the next cleanup pass. The live agent should classify and summarize; it should not mutate live state.

### Hard Rules

- Do not stop, restart, uninstall, kill, or replace any live gateway process.
- Do not run `ops-control stop|start`, generated supervisor start/stop scripts, or cutover commands.
- Do not run `worker-reap-stale`, `queue-skip`, `queue-retry`, `channel-delivery-record`, `cron-run-control`, or any command that changes queue/worker/CronRun/outbox state.
- Do not edit JSONL, SQLite, secrets, supervisor files, or `.agent-harness` state directly.
- Do not print raw tokens, env files, API keys, auth files, or secret values.
- Prefer summaries and counts over dumping full command output. Include full entry ids only when needed for migration/action tracking.
- If a command hangs or is too large, stop after the command timeout and record the timeout as a finding instead of retrying indefinitely.

### Commands To Run

Read-only live state:

```powershell
target\debug\agent-harness.exe healthz --harness-home .\.agent-harness --require-writable-state
target\debug\agent-harness.exe status --harness-home .\.agent-harness --json
target\debug\agent-harness.exe worker-status --harness-home .\.agent-harness
target\debug\agent-harness.exe cron-runs --harness-home .\.agent-harness --limit 100
target\debug\agent-harness.exe channel-outbox-plan --harness-home .\.agent-harness --outbox-limit 100
target\debug\agent-harness.exe cron-scheduler-lint --harness-home .\.agent-harness --source-home .\.agent-harness --workspace .\.agent-harness\workspace --enable
```

Documentation drift scan:

```powershell
Select-String -Path docs\*.md -Pattern 'runtime-leases\.json|native-cron-enqueue|cron-run-control|root lease|legacy root|lane=llm|origin=native-cron|\.openclaw|openclaw-core-snapshot|Docker|/root/\.openclaw|/home/agent/\.openclaw|/workspace'
```

### Data To Extract

#### 1. Live Health Snapshot

Record:

- `healthz.ready`
- `healthz.live`
- loop names, status, age/staleness
- runtime queued/open/prepared/completed
- outbox pending/retryable/delivered/invalid
- any warnings, especially sampled-ledger warnings

#### 2. Worker And CronRun Snapshot

Record:

- worker totals by lane
- worker blockers
- cron lane cap
- CronRun totals by status
- active CronRuns grouped by agent and entry id
- terminal failed/retryable/quarantined CronRuns, if any

#### 3. Cron Lint Migration Ledger

From `cron-scheduler-lint`, create a compact table grouped by finding code:

| code | severity | count | sourceKind | recommended action |
| --- | --- | ---: | --- | --- |

Then list blocking native cron entries with this shape:

| entryId | agentId if visible | code | issue summary | proposed action |
| --- | --- | --- | --- | --- |

Use one of these proposed actions:

- `rewrite-path`
- `native-port`
- `wsl-wrapper`
- `disable`
- `quarantine`
- `keep-isolated`
- `needs-operator-decision`

For deterministic cron warnings, group by script/crontab family when possible instead of listing every line.

#### 4. Cron Agent Ownership Review

List cron entries targeting `main` and classify them:

| entryId | schedule kind | current agent | suggested owner | rationale | confidence |
| --- | --- | --- | --- | --- | --- |

Suggested owner can be:

- `main`
- `cron-lite`
- `mem-cron`
- `comms-cron`
- `steamer-cron`
- `disable/quarantine`
- `needs-operator-decision`

Do not change ownership; only recommend.

#### 5. Outbox Backlog Triage

From `channel-outbox-plan --outbox-limit 100`, classify pending items:

| class | count | examples | recommended handling |
| --- | ---: | --- | --- |

Use these classes:

- `deliverable`
- `local-smoke-leftover`
- `blocked-cron-no-reply`
- `stale-progress-or-status`
- `needs-manual-review`

Do not mark anything delivered or skipped.

#### 6. Documentation Drift Candidates

List stale or ambiguous docs as:

| file | line | phrase | problem | recommended rewrite |
| --- | ---: | --- | --- | --- |

Focus on wording that could make a future operator believe:

- live runtime uses only the root `runtime-leases.json`
- `native-cron-enqueue` is the preferred live scheduler path
- `.openclaw`, Docker/container paths, or imported snapshots are active source/config authority
- cron work still uses `lane=llm` or `origin=native-cron` as current guidance

### Required Writeback Format

Append a section to this file:

```markdown
## Harness Agent Collection Results - YYYY-MM-DD

### Summary

- ...

### Live Health Snapshot

...

### Worker And CronRun Snapshot

...

### Cron Lint Migration Ledger

...

### Cron Agent Ownership Review

...

### Outbox Backlog Triage

...

### Documentation Drift Candidates

...

### Agent Recommendation

- Immediate repo-side work:
- Requires operator decision:
- Safe to defer:
```

The writeback should be concise but complete enough that a repo-side implementation agent can patch docs/tooling without rerunning the full live investigation.

### P0 - Documentation Cleanup

- Rewrite older root lease wording to say runtime leases are class-scoped under `state/runtime-queue/classes/<class>/runtime-leases.json`, with root `state/runtime-queue/runtime-leases.json` read only for upgrade compatibility.
- Split `native-cron-enqueue` docs into manual/import-adapter guidance and make `cron-scheduler-run-once` / `cron-scheduler-loop` the preferred live scheduler path.
- Keep `.openclaw`, Docker/container paths, and imported snapshots consistently labeled retired/import-only.

### P1 - Cron Content Migration Ledger

- Generate a machine-readable migration ledger from `cron-scheduler-lint` findings:
  - entry id
  - agent id
  - severity/code
  - path or shell compatibility issue
  - proposed action: rewrite path, WSL wrapper, native Windows port, disable, quarantine, or keep isolated
- Treat the 65 `native-runtime-path-mismatch` errors as blocking full cron cleanliness.
- Treat deterministic shell compatibility warnings as requiring explicit operator policy before any `--execute-shell` path.

### P2 - Cron Agent Ownership Review

- For native cron entries targeting `main`, decide whether each should:
  - stay on `main` but rely on `runtimeClass=cron` isolation,
  - move to a cron-specific agent such as `cron-lite`, `mem-cron`, or `comms-cron`,
  - be disabled/quarantined until the job has an owner.
- Add a small report grouping cron entries by agent and severity.

### P3 - Outbox Backlog Triage

- Run `channel-outbox-plan --harness-home .\.agent-harness --outbox-limit 50`.
- Categorize pending entries as deliverable, local-smoke leftovers, blocked cron/no-reply artifacts, or stale.
- Do not manually edit outbox files. Use existing delivery/operator surfaces or add a safe skip/mark surface if one is missing.

### P4 - Soak Monitoring

- Keep live read-only monitoring for at least one scheduler interval batch:
  - `healthz`
  - `status --json`
  - `worker-status`
  - `cron-runs --limit 50`
  - `cron-scheduler-lint --enable`
- Success criteria: no active CronRun buildup, no worker blockers, no runtime cron open items, no stale loop heartbeat.

### P5 - Future Code/Test Work

Only after P0-P3 are scoped:

- Add a focused test or fixture that turns lint findings into a migration ledger.
- Add a safe operator command for outbox cleanup if pending stale artifacts cannot be resolved through current delivery receipts.
- Run full `cargo test --workspace` when code changes are introduced.

## Recommendation

Proceed with documentation cleanup and cron migration planning before another live cutover. The current runtime isolation work is good enough to keep the harness live, but the imported cron set should still be treated as partially activated and policy-gated until lint errors are reduced to zero or explicitly quarantined.

## Harness Agent Collection Results - 2026-06-16

### Summary

- Live control plane is still healthy: `healthz.ready=true`, `healthz.live=true`, state writable, all seven loop heartbeats non-stale.
- Cron isolation is holding: worker `cron` lane has `2` succeeded jobs, CronRunStore has `2` terminal succeeded runs, active CronRuns `0`, quarantined `0`, and runtime `cronOpen=0`.
- Full cron cleanliness is still blocked by lint: `65` native old-path errors remain, plus deterministic shell compatibility warnings and main-agent cron ownership warnings.
- `channel-outbox-plan --outbox-limit 100` was not supported by the pre-fix live CLI. The fallback `channel-outbox-plan --harness-home .\.agent-harness` displayed 20 pending items, while `status --json` reported channel pending `235`; this output mismatch was treated as an observability cleanup item.

### Live Health Snapshot

| Field | Value |
| --- | --- |
| `healthz.ready` | `true` |
| `healthz.live` | `true` |
| `healthz.readinessReady` | `true` |
| Runtime queue | queued `282`, open `72`, prepared `61`, completed `262` |
| Outbox healthz counters | pending `235`, retryable `0`, delivered `133`, invalid `0` |
| Status runtime classes | queued `cron=78`, `interactive=204`; open `interactive=72`, `cron=0` |
| Scheduler latest tick | status `completed`, nativeEntries `114`, deterministicEntries `26`, dueCandidates `0`, enqueued `0`, skippedPolicy `140`, errors `0` |

Loop readback:

| loop | status | stale | ageMs |
| --- | --- | --- | ---: |
| runtime-loop | running | false | 908 |
| progress-delivery-loop | running | false | 152 |
| telegram-loop | running | false | 573 |
| discord-outbox-loop | ok | false | 480 |
| discord-gateway-loop | heartbeat | false | 5906 |
| worker-loop | no-work | false | 46 |
| cron-scheduler-loop | running | false | 40857 |

Warnings are sampled-ledger warnings only:

- delivery receipt status sampled from last 4 MiB
- runtime execution receipts sampled from last 4 MiB
- runtime run-once receipts sampled from last 4 MiB

### Worker And CronRun Snapshot

Worker totals:

| field | value |
| --- | ---: |
| total | 78 |
| pending | 0 |
| leased | 0 |
| running | 0 |
| succeeded | 78 |
| failedRetryable | 0 |
| failedTerminal | 0 |
| canceled | 0 |
| expired | 0 |

Worker lanes:

| lane | cap | total | succeeded | pending | running | failed |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| cron | 3 | 2 | 2 | 0 | 0 | 0 |
| llm | 6 | 76 | 76 | 0 | 0 | 0 |

Worker blockers:

| blocker | count |
| --- | ---: |
| blockedByGlobalLimit | 0 |
| blockedByGroupLimit | 0 |
| blockedByChannelLimit | 0 |
| blockedByLaneLimit | 0 |
| blockedByRateLease | 0 |

CronRunStore:

| field | value |
| --- | ---: |
| total | 2 |
| active | 0 |
| terminal | 2 |
| quarantined | 0 |
| succeeded | 2 |

Recent CronRuns:

| entryId | agentId | status | runtimeClass | sessionPolicy | quarantined |
| --- | --- | --- | --- | --- | --- |
| `0b96c937-901c-4d53-a7bb-3df72e173755` | main | succeeded | cron | one-shot | false |
| `2fcf6fd4-e483-4649-8a6f-e6a34988cfe3` | main | succeeded | cron | one-shot | false |

No active, retry-pending, failed-terminal, or quarantined CronRuns were observed.

### Cron Lint Migration Ledger

Grouped lint findings:

| code | severity | count | sourceKind | recommended action |
| --- | --- | ---: | --- | --- |
| `native-runtime-path-mismatch` | error | 65 | native-cron | rewrite/imported paths, port scripts, or hold/quarantine before full activation |
| `deterministic-shell-compatibility-required` | warn | 26 | deterministic-cron | choose WSL/Git Bash wrapper or native Windows port before `--execute-shell` |
| `native-main-agent-cron` | warn | 37 | native-cron | review ownership; keep isolated or move to cron-specific agents |

Blocking native path errors by proposed action:

| proposed action | count | notes |
| --- | ---: | --- |
| `rewrite-path` | 33 | Prompt/spec points at retired `/root/.openclaw`, `/workspace`, or imported source roots and likely needs new-home path rewrite. |
| `native-port` | 28 | Job depends on shell/subagent/legacy runner behavior; likely needs a Windows-native script or direct harness worker adapter. |
| `needs-operator-decision` | 4 | Strategy/Steamer or ambiguous ownership/path scope; do not auto-port. |

Representative blocking native entries:

| entryId | agentId | issue summary | proposed action |
| --- | --- | --- | --- |
| `09089ac6-05fc-4e78-9f32-5ae7d7e85883` | cron-lite | knowledge-consolidation readiness monitor uses old path references | rewrite-path |
| `2d60cd33-d56c-456f-9996-97a975dee7d7` | cron-lite | docsColdLane ingest path/tooling mismatch | rewrite-path |
| `446a5378-dcbb-418d-96a7-1cf59f97b24d` | cron-lite | daily snapshot imports old job spec/hardening paths | rewrite-path |
| `57eed23a-6601-492d-9d93-0d157bddf22f` | cron-lite | weekly docs mirror still points at old workspace/spec | rewrite-path |
| `284b1e96-3a0b-4bbe-b4c6-cf98724b8455` | main | diagnostic batch review uses legacy subagent/session runner shape | native-port |
| `638c71b9-b7a2-4579-a80e-fb8baca095d8` | main | cron queue delay dashboard uses legacy runner/path | native-port |
| `337bf8c5-bde8-4ab8-af54-36cbba4c5dbd` | main | Steamer online sim verify/autoheal needs operator-owned port | native-port |
| `5137f344-041a-48be-933e-edd7ce34607f` | main | Steamer EC2 power-on/readiness requires operator decision on active lane | native-port |
| `70d08b09-14a2-498a-bf0b-c27f2b71b3f7` | mem-cron | openclaw-mem GitHub scout has legacy path references | rewrite-path |
| `8a291055-75c9-480a-a2a9-63d0cafec193` | cron-lite | workspace-root housekeeping still references old active root assumptions | rewrite-path |

Deterministic cron warnings are concentrated in:

| family | warning | proposed action |
| --- | --- | --- |
| `workspace\tools\cron-runner\crontab\openclaw-mem.crontab` | shell scripts need WSL/Git Bash or native port | native-port or WSL wrapper policy |
| `workspace\tools\backup-cron-runner\crontab\workspace-backup.crontab` and backups | shell scripts need WSL/Git Bash or native port | native-port or WSL wrapper policy |

### Cron Agent Ownership Review

Enabled native cron entries targeting `main`: `36`.

Suggested owner counts:

| suggested owner | count | rationale |
| --- | ---: | --- |
| `steamer-cron` | 18 | Strategy, market, RSS, watcher, dashboard, or Steamer operational jobs should not stay on the interactive persona long-term. |
| `needs-operator-decision` | 11 | Ambiguous job ownership, private strategy scope, or old runner assumptions. |
| `main` | 4 | Harness-local or persona-specific jobs can stay on `main` with `runtimeClass=cron` isolation. |
| `mem-cron` | 3 | Memory/docs/hygiene jobs should move to a memory/cron-specific owner. |

Representative entries:

| entryId | schedule kind | current agent | suggested owner | rationale | confidence |
| --- | --- | --- | --- | --- | --- |
| `0b96c937-901c-4d53-a7bb-3df72e173755` | cron | main | main | P0 harness health check is harness-local and already Windows-native. | medium |
| `2fcf6fd4-e483-4649-8a6f-e6a34988cfe3` | cron | main | main | P0 daily operator packet is harness-local and already isolated. | medium |
| `8f1a43ff-1444-4012-af1b-a259f2c8b205` | cron | main | mem-cron | Docs/memory freshness is operational memory work. | low |
| `54acf85a-ba78-4399-9d78-2bf3a66267c9` | cron | main | mem-cron | AGENTS/MEMORY hygiene should be owned by a memory/ops cron profile. | low |
| `638c71b9-b7a2-4579-a80e-fb8baca095d8` | cron | main | steamer-cron | Cron queue dashboard should be operational/background, not interactive persona. | low |
| `284b1e96-3a0b-4bbe-b4c6-cf98724b8455` | cron | main | steamer-cron | Diagnostic batch review is operational/background. | low |
| `fa376c04-550d-4c34-898c-7c632c6a2870` | cron | main | main | Lyria social image job may remain persona-bound if intentionally user-facing. | medium |

### Outbox Backlog Triage

`channel-outbox-plan --outbox-limit 100` was not accepted by the pre-fix live CLI. Fallback command used:

`target\debug\agent-harness.exe channel-outbox-plan --harness-home .\.agent-harness`

Observed fallback summary:

- displayed outbox lines: `294`
- displayed pending: `20`
- displayed delivered: `274`
- failed_retryable: `0`
- invalid: `0`

`status --json` reports broader channel outbox counters:

- totalOutboxLines: `368`
- pending: `235`
- delivered: `133`
- failedRetryable: `0`
- invalidLines: `0`

The mismatch between `channel-outbox-plan` and `status --json` should be tracked as an operator-surface consistency issue.

Fallback visible pending classification:

| class | count | examples | recommended handling |
| --- | ---: | --- | --- |
| local-smoke-leftover | 11 | `smoke-xiaoxiaoli`, `smoke-or`, OpenRouter smoke failures, offline fake Codex reply | Add or use a safe cleanup/skip surface for local smoke artifacts; do not deliver to real channels. |
| blocked-cron-no-reply | 9 | native-cron BLOCKED/NO_REPLY/ALERT entries for old path jobs | Keep undelivered until cron migration policy decides whether to suppress, archive, or deliver alerts. |
| deliverable | 0 | none in visible fallback window | No action. |
| stale-progress-or-status | 0 | none in visible fallback window | No action. |
| needs-manual-review | 0 | covered by local smoke / cron categories | No action beyond policy review. |

### Documentation Drift Candidates

| file | line | phrase | problem | recommended rewrite |
| --- | ---: | --- | --- | --- |
| `docs\activation-readiness-plan.md` | 186 | `Native legacy .openclaw/cron scheduler ticks enqueue...` | Can read like `.openclaw` is still live authority. | Say imported native cron entries are now evaluated from active `.agent-harness` source/config authority; `.openclaw` is import history only. |
| `docs\activation-readiness-plan.md` | 230 | `Runtime queue leasing uses state/runtime-queue/runtime-leases.json...` | Describes old root-only lease model. | Say class-scoped leases live under `state/runtime-queue/classes/<runtimeClass>/runtime-leases.json`; root lease is upgrade compatibility only. |
| `docs\agent-harness-dev-handoff.md` | 236 | `native-cron-enqueue --resume-cron` | Can read like current preferred live scheduler path. | Mark as manual/import adapter path; preferred live path is `cron-scheduler-run-once` / `cron-scheduler-loop`. |
| `docs\agent-harness-operations-handbook.md` | 197 | `native-cron-enqueue --source-home...` | Example sits near operator command list without enough "manual/import" framing. | Split manual adapter examples from live scheduler examples. |
| `docs\agent-harness-operations-handbook.md` | 274 | `Runtime leasing uses state/runtime-queue/runtime-leases.json...` | Old root lease emphasis contradicts class-scoped runtime lease design. | Rewrite to class-scoped leases first, legacy root readback second. |
| `docs\agent-harness-operations-handbook.md` | 342 | `cron-plan covers the native lane only... .openclaw/cron/jobs.json` | Historical/import detail can be mistaken for active source. | Label as legacy/import planning; live scheduler reads active `.agent-harness` source-home. |
| `docs\agent-harness-operations-handbook.md` | 346 | `native-cron-enqueue converts the native cron plan...` | Can be mistaken for preferred live cron execution. | State it is a manual compatibility adapter; live scheduler path is scheduler loop/run-once. |
| `docs\agent-worker-dispatch-strategy.md` | 267 | `agent-harness native-cron-enqueue ...` | Strategy doc still includes old command example. | Add note that this is manual/import compatibility, not normal live scheduler operation. |

### Agent Recommendation

- Immediate repo-side work:
  - Patch P0 docs drift: class-scoped lease wording, active `.agent-harness` authority, and live scheduler path.
  - Add/extend a migration ledger generator for `cron-scheduler-lint` so the 65 native path errors can be tracked without manual parsing.
  - Fix or document `channel-outbox-plan` limit behavior and reconcile its summary with `status --json` channel counters.
- Requires operator decision:
  - Whether deterministic shell jobs should use WSL/Git Bash wrappers or be ported to native Windows scripts.
  - Whether the 36 `main` cron entries should stay isolated on `main` or move to `cron-lite`, `mem-cron`, `comms-cron`, or `steamer-cron`.
  - Whether visible native-cron BLOCKED/NO_REPLY pending outbox artifacts should be delivered, archived, or skipped through a safe operator surface.
- Safe to defer:
  - Full `cargo test --workspace` until code changes are introduced.
  - Non-blocking docs that already explicitly label Docker/container paths as retired import/rollback context.

## Implementation Adjustment Assessment - 2026-06-16

The agent回寫 confirms the worker/runtime isolation design is holding: live health is ready, worker blockers are zero, CronRun active/quarantined counts are zero, and `cronOpen=0`. The implementation work therefore does not need to change the cron/runtime lane architecture. The needed changes are operator-surface fixes and documentation cleanup:

- Fixed `channel-outbox-plan` observability so the summary scans the full outbox ledger while `limit`/`outbox-limit` only caps displayed pending details. This resolves the pre-fix `20` visible pending vs broader status counter mismatch.
- Added `--outbox-limit` as a `channel-outbox-plan` alias for operator consistency with other channel commands.
- Enriched `cron-scheduler-lint` JSON findings with optional `agentId` and `proposedAction`, so the lint output can serve as the cron migration ledger without manual parsing.
- Rewrote stale docs that implied `.openclaw` or root runtime leases are active live authority. Current live authority is `.agent-harness`, runtime leases are class-scoped, and `native-cron-enqueue` is a manual/import compatibility adapter rather than the preferred live scheduler path.

Validation completed in staging:

- `cargo fmt --all --check`
- `cargo test -p agent-harness-core --target-dir target\staging-test-followup-core -- --test-threads=1` (`259` tests)
- `cargo test -p agent-harness-cli --target-dir target\staging-test-followup-cli` (`23` tests)
- `cargo check --workspace --target-dir target\staging-check-followup`
- `cargo build -p agent-harness-cli --target-dir target\staging-build-followup`
- Staging binary smoke: `channel-outbox-plan --outbox-limit 100` reports full summary counters, and `cron-scheduler-lint` findings include `agentId` plus `proposedAction`.

Still requires operator decision:

- Deterministic shell cron policy: native Windows port vs WSL/Git Bash wrapper.
- Ownership migration for the remaining `main` native cron entries.
- Whether stale local-smoke/native-cron pending outbox artifacts should be delivered, archived, or skipped through a future safe operator surface.

### Live Cutover Result

The follow-up operator-surface build was cut over live after staging validation:

- Ticket: `cutover-1781543670554`
- Backup label: `pre-followup-operator-surfaces-cutover`
- Previous binary backup: `target\debug\agent-harness.pre-followup-operator-surfaces-20260616011809.exe`
- New live binary source: `target\staging-build-followup\debug\agent-harness.exe`
- Bundled skill sync: already current
- Supervisor plan: regenerated with 7 tasks, including `cron-scheduler-loop`
- Direct runners: restarted because scheduled tasks are not registered in this environment

Post-cutover readback:

- `healthz --harness-home .\.agent-harness --require-writable-state`: `ready=true`, `live=true`, all 7 loop heartbeats non-stale.
- `status --harness-home .\.agent-harness --json`: `ready=true`, readiness `passed=59`, `warnings=0`, `failed=0`.
- `worker-status --harness-home .\.agent-harness --json`: totals `pending=0`, `leased=0`, `running=0`, `failedRetryable=0`, `failedTerminal=0`.
- `channel-outbox-plan --harness-home .\.agent-harness --outbox-limit 100`: accepted and reports full summary counters (`lines=370`, `pending=81`, `delivered=289`, `failed_retryable=0`, `invalid=0`).
- `cron-scheduler-lint --harness-home .\.agent-harness --source-home .\.agent-harness --workspace .\.agent-harness\workspace --dry-run --enable --resume-cron --allow-deterministic-run`: still returns expected error for imported cron blockers, and now includes `agentId` plus `proposedAction` in findings.
