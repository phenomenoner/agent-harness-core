# Memory Importance Maintenance Contract

Date: 2026-06-17
Owner: Lyria / OpenClaw memory operations
Status: active policy, review-only automation enabled

## Purpose

OpenClaw memory importance is not a one-time label. It should decay, consolidate, or be promoted as projects change, evidence ages, and higher-quality summaries replace raw operational traces.

This contract turns the round6-3 backfill lesson into a recurring policy: maintain memory item importance with receipts, without deleting source data by default.

## Scope

Applies to:

- `openclaw-mem` observations, episodic events, and docs chunks in the active harness memory database.
- Importance and retrieval eligibility decisions such as `must_remember`, `nice_to_have`, contextual/default, stale candidate, and ignore/noise.
- Periodic review plans that propose promotion, demotion, stale marking, or exclusion from future embedding/backfill priority sets.

Does not apply to:

- Live gateway/runtime topology.
- Model routing or provider credentials.
- Destructive memory deletion or garbage collection.
- Automatic demotion of hard authority rules without explicit evidence and review.

## Current Seed Evidence

The 2026-06-17 round6-3 embedding recovery showed three practical lanes:

1. Recent high-value memory, `must_remember`, and operator canon should be embedded first.
2. Episodic conversation data needs filtering before embedding, because raw episodes include redacted content, duplicated checkpoint fragments, and low-value turn noise.
3. Historical observations need additional noise demotion before backfill, because tool/status spam can dominate volume without improving recall.

The policy conclusion is: data does not need blind full rebuilds; importance and embedding eligibility need periodic maintenance.

## Importance Tiers

`must_remember`
: Durable identity, authority, safety, rollback, active product canon, user preference, and decisions CK may rely on. Demotion requires explicit evidence and a receipt.

`nice_to_have`
: Useful project context, recent decisions, resolved incidents, and recurring operational patterns. Eligible for embedding when coverage budget allows.

`contextual`
: Default state for ordinary historical records. Searchable through text/FTS, but not automatically prioritized for embedding.

`stale_candidate`
: Superseded, low-hit, duplicate, or project-local material whose value has aged down. It stays inspectable and can be revived.

`ignore`
: Tool spam, wrapper chatter, redacted fragments without useful surrounding context, duplicate status pings, and records with no durable operator value. Ignore means retrieval/backfill deprioritization, not deletion.

## Signals

Promotion signals:

- Referenced by checkpoints, decisions, operator canon, rollback plans, or active docs.
- Recent recall hit with a successful answer or operational action.
- Mentions active project names, durable paths, user preferences, risk boundaries, or command receipts.
- Corrects a known failure mode or stale rule.

Demotion signals:

- Replaced by a newer checkpoint, decision, or contract.
- Repeated tool/status spam with no human decision content.
- Redacted or partial content that cannot be interpreted safely.
- Old observation with no recall hits, no citation chain, and no active project link.
- Duplicate episode from session replay, checkpoint echo, or repeated assistant phrasing.

Hard stop signals:

- Never delete automatically.
- Never demote `must_remember` without an explicit citation to the newer source of truth.
- Never treat redacted content as evidence for promotion.
- Never use a model-only judgment as the sole receipt for irreversible changes.

## Actions

The weekly job is plan-only:

- Inspect current memory table counts, embedding coverage, and noisy candidate counts.
- Write a dated JSON plan and Markdown summary.
- Propose candidate groups for promote/demote/stale/ignore.
- Treat protection lanes such as `protect_must_remember` as guardrails, not actionable cleanup.
- Emit a short actionable notification when review-lane candidate counts cross a bucketed notification digest that has not been reported before.
- Emit `NO_REPLY` when the latest notification digest was already reported and no new action is needed.
- Emit a short `BLOCKED` report if the plan cannot be written or the database cannot be read.

Apply mode is out of scope for the scheduled job. Any future apply path must:

- Be capped by count and tier.
- Write before/after receipts.
- Take a SQLite backup before mutation.
- Support rollback from the backup or an inverse receipt.
- Keep `must_remember` demotions review-gated.

## Cadence

Cadence: weekly, Sunday 06:45 Asia/Taipei.

Rationale:

- Weekly is frequent enough to catch new noisy lanes before they become a backfill tax.
- It avoids daily churn on importance scores.
- It gives enough time for recall/citation signals to accumulate.
- Asia/Taipei does not observe DST; if this job moves to a DST timezone, review cadence drift explicitly.

The weekly cron is intentionally review-only. Monthly deep consolidation can be added later after two to four weekly receipts show stable candidate quality.

## Automation Surface

Tracked planner script:

- `tools/openclaw_mem_importance_maintenance_plan.py`

Runtime artifacts:

- `D:\Warehouse\Rust-OpenClaw-Core\.agent-harness\state\memory\importance-maintenance\<YYYY-MM-DD>\plan.json`
- `D:\Warehouse\Rust-OpenClaw-Core\.agent-harness\state\memory\importance-maintenance\<YYYY-MM-DD>\summary.md`
- `D:\Warehouse\Rust-OpenClaw-Core\.agent-harness\state\memory\importance-maintenance\last-notified.json`

Retention:

- Keep at least the latest 12 weekly plan directories.
- Do not auto-delete receipts until a separate cleanup job exists and has its own rollback/receipt.

Scheduled job:

- ID: `52f01e16-0a14-45c5-8543-ac8b313a2e7e`
- Name: `weekly: openclaw-mem importance maintenance plan`
- Schedule: `45 6 * * 0`
- Timezone: `Asia/Taipei`
- Mode: deterministic script through cron worker, review-only

## Verification

A valid run must prove:

- The active SQLite memory database was opened read-only.
- SQLite `query_only` mode was requested before table inspection.
- Row and embedding coverage counts were captured for observations, episodic events, and docs chunks.
- Candidate counts were grouped by policy lane.
- `plan.json` and `summary.md` were written.
- Actionable findings were either reported once per notification digest or suppressed because the notification digest was already reported.
- No SQLite mutation occurred.

A valid policy update must include:

- Contract diff/readback.
- Cron registry readback.
- Planner dry-run receipt.
- Commit SHA for tracked contract/script changes.

## Rollback

Rollback for this policy:

- Revert the tracked contract/script commit.
- Disable or remove the cron job from `.agent-harness\cron\jobs.json`.
- Delete only generated plan artifacts if CK wants a clean state; generated artifacts are otherwise harmless evidence.

Rollback for future apply mode:

- Store a pre-apply SQLite backup under `D:\Warehouse\Rust-OpenClaw-Core\.agent-harness\state\memory\importance-maintenance\<YYYY-MM-DD>\pre-apply.sqlite`.
- Restore from the pre-apply SQLite backup, or apply the inverse receipt generated by the mutator.

## Ownership

`BLOCKED memory-importance-plan ...` output is owned by Lyria / OpenClaw memory operations. The configured cron failure alert sends to CK's Discord DM. The next action is to inspect the dated plan directory and repair the planner, database path, or schema mismatch before the next weekly run.

## Known V1 Limits

- Tool-noise review uses conservative substring carve-outs for `must_remember`, `checkpoint`, `decision`, and `receipt`. This can under-count noisy rows, which is acceptable for review-only mode because the bias protects receipt-bearing records from accidental demotion.
- Redacted observation review currently uses a literal `[REDACTED` substring because `observations` does not expose a structured `redacted` column. Replace it with a structured flag if the schema adds one.
- Notification buckets reduce single-row churn but can still re-notify if a lane count alternates across a bucket boundary. Weekly cadence and review-only mode make that acceptable for v1.
