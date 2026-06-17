# Harness Agent Validation Next Session - 2026-06-16

This note is an optional handoff for a future session or harness agent. The follow-up operator-surface implementation has already been staged, pushed, and cut over live. No additional harness-agent validation is strictly required before normal operation, but the checks below are useful if we want another read-only evidence pass after some soak time.

## Current Baseline

- Branch: `codex/round5-cron-runtime-isolation`
- Implementation commit: `a19ae04 Fix follow-up operator surfaces`
- Cutover receipt commit: `d4c962d Record follow-up cutover`
- Live cutover ticket: `cutover-1781543670554`
- Backup label: `pre-followup-operator-surfaces-cutover`
- Previous live binary backup: `target\debug\agent-harness.pre-followup-operator-surfaces-20260616011809.exe`
- New live binary source: `target\staging-build-followup\debug\agent-harness.exe`

## Validation Need

Recommended but not mandatory.

The main runtime/cron isolation design is already validated: live health was ready, all 7 loops were non-stale, worker pending/running/failed counts were zero, and CronRun active/quarantined counts were zero after cutover. The remaining useful validation is an observability and policy-readiness pass:

- Confirm the new operator surfaces behave consistently under live data.
- Confirm the cron lint output is usable as a migration ledger.
- Collect read-only policy inputs for cron ownership, deterministic shell strategy, and stale outbox handling.

## Safety Rules

Harness agent must stay read-only.

Allowed:

- `healthz`
- `status --json`
- `worker-status --json`
- `cron-runs --limit`
- `cron-scheduler-lint`
- `cron-scheduler-run-once --dry-run`
- `channel-outbox-plan`
- `turn-plan` diagnostics that do not enqueue or deliver messages
- static `Select-String`/file reads over docs and generated supervisor scripts

Not allowed:

- `ops-control`
- `ops-cutover-*`
- `worker-cancel`
- `worker-reap-stale`
- `cron-run-control`
- `channel-delivery-record`
- `telegram-loop`, `discord-outbox-send-once`, or any command that sends provider messages
- direct edits to `.agent-harness` state, SQLite files, JSONL ledgers, outbox, watermarks, or supervisor scripts

Use explicit timeouts for every shell command. If a command hangs or produces too much output, stop and summarize the partial evidence instead of retrying repeatedly.

## Scope 1 - Post-Cutover Soak Health

Feature range: live supervisor loops, runtime queue, worker dispatch, CronRunStore.

Design purpose: prove the new binary and regenerated 7-loop supervisor plan keep runtime, worker, cron scheduler, progress delivery, Telegram, Discord outbox, and Discord gateway alive without worker buildup.

Suggested commands:

```powershell
.\target\debug\agent-harness.exe healthz --harness-home .\.agent-harness --require-writable-state
.\target\debug\agent-harness.exe status --harness-home .\.agent-harness --json
.\target\debug\agent-harness.exe worker-status --harness-home .\.agent-harness --json
.\target\debug\agent-harness.exe cron-runs --harness-home .\.agent-harness --limit 50
```

Pass criteria:

- `healthz.ready=true`, `healthz.live=true`
- all 7 loops present and `stale=false`
- readiness `failed=0`
- worker `pending=0`, `leased=0`, `running=0`, `failedRetryable=0`, `failedTerminal=0`
- CronRuns `active=0` and `quarantined=0`

## Scope 2 - Channel Outbox Observability Fix

Feature range: channel outbox planning and delivery observability.

Design purpose: prove `channel-outbox-plan` summary counters scan the full ledger, while `--limit` / `--outbox-limit` only caps displayed pending details.

Suggested commands:

```powershell
.\target\debug\agent-harness.exe channel-outbox-plan --harness-home .\.agent-harness --outbox-limit 5
.\target\debug\agent-harness.exe channel-outbox-plan --harness-home .\.agent-harness --outbox-limit 100
.\target\debug\agent-harness.exe channel-outbox-plan --harness-home .\.agent-harness --limit 5
```

Pass criteria:

- `--outbox-limit` is accepted.
- `--limit 5` and `--outbox-limit 5` are equivalent.
- Summary counters such as `lines`, `pending`, `delivered`, `failed_retryable`, and `invalid` do not shrink merely because the displayed pending detail limit is lower.

Useful output:

- Record the three `Summary:` lines.
- Record displayed pending detail counts for each command.
- Classify visible pending entries as `local-smoke-leftover`, `blocked-cron-no-reply`, `deliverable`, `stale-progress-or-status`, or `needs-manual-review`.

## Scope 3 - Cron Lint Migration Ledger

Feature range: cron scheduler lint, imported native cron migration, deterministic cron policy gates.

Design purpose: prove `cron-scheduler-lint` can now produce a machine-readable migration ledger with `agentId` and `proposedAction`, while still blocking full cron cleanliness until imported path and policy issues are resolved.

Suggested command:

```powershell
.\target\debug\agent-harness.exe cron-scheduler-lint --harness-home .\.agent-harness --source-home .\.agent-harness --workspace .\.agent-harness\workspace --dry-run --enable --resume-cron --allow-deterministic-run
```

Expected result:

- Exit may be nonzero because live imported cron blockers are still expected.
- JSON should include findings with `agentId` and `proposedAction`.

Pass criteria:

- `native-runtime-path-mismatch` findings include `agentId` and `proposedAction=rewrite-path-or-native-port`.
- `native-main-agent-cron` findings include `agentId=main` and `proposedAction=review-owner`.
- deterministic shell compatibility findings include a policy-oriented `proposedAction`.

Useful output:

- Group findings by `code`, `severity`, `sourceKind`, and `proposedAction`.
- List representative entries for:
  - native path blockers
  - `main` agent ownership warnings
  - deterministic shell compatibility warnings
- Do not attempt to edit cron jobs or watermarks.

## Scope 4 - Documentation And Skill Drift

Feature range: operational docs, public docs, bundled `agent-windows-harness` skill.

Design purpose: ensure the docs/skill text no longer tells operators that retired `.openclaw` paths, Docker/container paths, root runtime leases, or `native-cron-enqueue` are current live authority.

Suggested static checks:

```powershell
Select-String -Path README.md,docs\*.md,crates\agent-harness-core\src\harness_skills.rs -Pattern 'native-cron-enqueue|state/runtime-queue/runtime-leases.json|\.openclaw/cron|/root/\.openclaw|/home/agent/\.openclaw|Docker|container'
```

Pass criteria:

- `.openclaw` references are explicitly historical/import/retired when they could be confused with live authority.
- root `state/runtime-queue/runtime-leases.json` is described as legacy upgrade compatibility, not the primary lease domain.
- `native-cron-enqueue` is framed as a manual/import compatibility adapter, not the preferred live scheduler path.
- preferred live cron path is `cron-scheduler-run-once` / `cron-scheduler-loop`.

## Scope 5 - Optional Policy Input Collection

Feature range: next design decisions, not current runtime correctness.

Design purpose: collect read-only evidence for operator decisions that should happen before full cron activation cleanup.

Collect:

- deterministic shell policy candidates: native Windows port vs WSL/Git Bash wrapper
- `main` native cron ownership candidates: keep on `main`, move to `mem-cron`, move to `steamer-cron`, or needs operator decision
- stale outbox artifact categories and whether a safe skip/archive command is needed

Do not decide policy inside the harness agent. Return a compact table and mark uncertain entries as `needs-operator-decision`.

## Required Result Format

Append results to this file or write a sibling note named:

`docs/harness-agent-validation-results-YYYYMMDD.md`

Use this structure:

```markdown
## Harness Agent Results - YYYY-MM-DD

### Summary

- Overall status: PASS / PASS WITH NOTES / BLOCKED
- Commands run:
- Mutations performed: none

### Scope Results

| scope | result | evidence | concern |
| --- | --- | --- | --- |

### Findings Requiring Main-Agent Review

| priority | finding | evidence | suggested next owner |
| --- | --- | --- | --- |

### Raw Evidence Pointers

- command:
- file/path:
- relevant counters:
```

## Expected Outcome

If all scopes pass or pass with notes, no immediate code change is required. The next session can focus on policy decisions and optional cleanup tooling:

- deterministic shell cron strategy
- cron ownership migration
- safe outbox skip/archive operator surface

## Goal Closure Pointer - 2026-06-16

CK opened a follow-up goal to execute and test the remaining checkpoint items
without hot-patching `agent-harness-core` or performing live cutover. The safe
read-only validation and repo-side policy closure are recorded in
`docs/harness-agent-validation-goal-results-20260616.md`.

Items that require core/CLI changes, live cron/channel state mutation, or a later
operator cutover are collected in
`docs/harness-agent-validation-deferred-20260616.md`.

## Harness Agent Results - 2026-06-16

### Summary

- Overall status: PASS WITH NOTES
- Commands run:
  - `agent-harness.exe healthz --harness-home .agent-harness --require-writable-state`
  - `agent-harness.exe status --harness-home .agent-harness --json`
  - `agent-harness.exe worker-status --harness-home .agent-harness --json`
  - `agent-harness.exe cron-runs --harness-home .agent-harness --limit 50`
  - `agent-harness.exe channel-outbox-plan --harness-home .agent-harness --outbox-limit 5`
  - `agent-harness.exe channel-outbox-plan --harness-home .agent-harness --outbox-limit 100`
  - `agent-harness.exe channel-outbox-plan --harness-home .agent-harness --limit 5`
  - `agent-harness.exe cron-scheduler-lint --harness-home .agent-harness --source-home .agent-harness --workspace .agent-harness\workspace --dry-run --enable --resume-cron --allow-deterministic-run`
  - `Select-String` static docs/skill drift scan
- Mutations performed: this documentation note only; no `.agent-harness` live state, queue, CronRun, outbox, delivery, supervisor, or watermark mutation.

### Scope Results

| scope | result | evidence | concern |
| --- | --- | --- | --- |
| 1 - Health / worker / cron run control plane | PASS | `healthz` reports `ready=true`, `live=true`, writable state, and all 7 loop heartbeats present/non-stale. `status --json` reports readiness `passed=59`, `warnings=0`, `failed=0`. `worker-status` reports total `78`, pending/leased/running `0`, succeeded `78`, failed `0`; cron lane `2` succeeded. `cron-runs --limit 50` reports total `2`, active `0`, terminal `2`, quarantined `0`, both `succeeded`. | Runtime still has interactive open work (`status`: open interactive `73`; worker downstream open interactive `2` at collection time), but cron class open is `0` and worker lane is not blocked. |
| 2 - Channel outbox observability | PASS WITH NOTES | `--outbox-limit 5`, `--outbox-limit 100`, and legacy `--limit 5` are accepted. All three preserve the same outbox-plan summary: `lines=371`, `pending=81`, `delivered=290`, `failed_retryable=0`, `invalid=0`; only displayed pending details change. | `healthz`/`status` still report a different outbox aggregate (`pending=235`, `delivered=136`). Treat this as a remaining cross-command semantic mismatch to clarify before using these counters for policy automation. |
| 3 - Cron lint migration ledger | PASS WITH EXPECTED ERRORS | Lint emits valid machine-readable JSON before exiting nonzero with `status=error`. Summary is `findings=128`, `errors=65`, `warnings=63`, `nativeEntries=114`, `deterministicEntries=26`. Findings include `agentId` and `proposedAction` fields, e.g. `rewrite-path-or-native-port`, `review-owner`, and `native-port-or-wrapper-policy`. | Errors are real legacy/import cron cleanup work, not a scheduler-loop failure. Current lint output still appends CLI help text after the JSON on nonzero exit, so parsers should extract the JSON object defensively. |
| 4 - Documentation and skill drift | PASS WITH NOTES | Static scan shows active docs/skill mostly label `.agent-harness` as live authority and `.openclaw`, Docker/container paths, `/root/.openclaw`, `/home/agent/.openclaw`, `/workspace`, and import snapshots as retired/import/historical context. Key live guidance appears in `docs/activation-readiness-plan.md`, `docs/agent-harness-operations-handbook.md`, `docs/agent-harness-dev-handoff.md`, `docs/agent-worker-dispatch-strategy.md`, and bundled `agent-windows-harness` skill. | Historical docs such as `docs/project-assessment.md` still contain many old paths, but the file has a top status note marking it historical. Keep future edits careful so old path examples do not regain live-authority wording. |
| 5 - Optional policy input collection | NOT DECIDED | Evidence collected from lint/outbox/runtime counters. | Leave decisions to main/operator: deterministic shell strategy, cron ownership migration, and stale outbox skip/archive policy. |

### Findings Requiring Main-Agent Review

| priority | finding | evidence | suggested next owner |
| --- | --- | --- | --- |
| P1 | Cron payload cleanup is still required before broad/full cron activation. | `cron-scheduler-lint`: 65 `native-runtime-path-mismatch` errors and 26 deterministic shell compatibility warning sources; many affected jobs still reference retired Linux/container paths or shell runners. | main/operator with cron owners (`cron-lite`, `steamer-cron`, `mem-cron`, `comms-cron`) |
| P2 | Outbox aggregate counters are not yet one semantic truth across commands. | `healthz`/`status`: pending `235`, delivered `136`; `channel-outbox-plan`: pending `81`, delivered `290` on the same outbox file. | harness/operator surface owner |
| P2 | `cron-runs` currently has no `--json` flag despite global `--json` help text implying JSON mode in some commands. | `cron-runs --limit 50 --json` exits with `unknown argument: --json`; `cron-runs --limit 50` already emits JSON. | CLI ergonomics owner |
| P3 | Lint JSON is usable, but nonzero output includes trailing CLI help/error text after the JSON body. | `cron-scheduler-lint ...` prints valid `agent-harness.cron-scheduler.lint.v1` JSON with `findings`, then appends command help and `error: cron scheduler lint failed`. | CLI output contract owner |

### Raw Evidence Pointers

- Health counters: `healthz` ready/live/writable true; loop statuses non-stale (`runtime-loop`, `progress-delivery-loop`, `telegram-loop`, `discord-outbox-loop`, `discord-gateway-loop`, `worker-loop`, `cron-scheduler-loop`).
- Runtime counters: `status --json` queued `285`, open `73`, prepared `64`, completed `264`; queued by runtime class `cron=78`, `interactive=207`; open by runtime class `interactive=73`, cron open `0`.
- Worker counters: total `78`, pending/leased/running `0`, succeeded `78`, failed `0`; cron lane `2` succeeded; llm lane `76` succeeded.
- CronRun counters: total `2`, active `0`, terminal `2`, quarantined `0`, byStatus `succeeded=2`.
- Outbox counters: `channel-outbox-plan` summary `lines=371 pending=81 delivered=290 failed_retryable=0 invalid=0`; `healthz`/`status` channel aggregate `pending=235 delivered=136`.
- Lint counters: findings `128`, errors `65`, warnings `63`, native entries `114`, deterministic entries `26`.
