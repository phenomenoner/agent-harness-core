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
