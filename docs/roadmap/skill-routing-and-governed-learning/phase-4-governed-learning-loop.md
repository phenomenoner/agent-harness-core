# Phase 4 — Governed Learning Loop

- **Status:** planned
- **Depends on:** accepted reversible serving cohort with exact outcome attribution
- **Unlocks:** serialized, budgeted library maintenance through
  [Ecosystem Operations](phase-5-ecosystem-operations.md)

## Outcome

Verified experience can improve one selected skill revision through an
immutable proposal, independent evaluation, held-out replay, exact-cohort
canary, promotion, re-selection, and tested restoration.

## Purpose in the Whole Project

This phase proves the central product hypothesis: the harness can turn
experience into reusable procedural improvement without unrestricted
self-rewriting.

The goal is not to maximize the number of generated skills or patches. It is to
demonstrate one attributable quality or cost improvement with one mutation
owner, explicit uncertainty, and an exact downgrade. This capability is useful
on its own for carefully governed maintenance even if broader autonomy is never
enabled.

## Starting Point

The reversible-cohort work provides frozen skill revisions, joined outcomes,
and real-use evidence. The current source already has proposals, decision
traces, lint, guard, target locks, backups, quarantine, guarded apply, and
archive/restore. This phase connects those foundations to outcome-governed
evaluation and promotion.

## Scope

### In scope

- one low-risk, selected skill and one clearly attributable improvement class;
- bounded learning review from verified outcomes;
- immutable create, patch, replace, or no-change proposal;
- explicit proposal jurisdiction and one mutation writer;
- deterministic lint, guard, compatibility, checksum, and approved-root gates;
- an evaluator cascade appropriate to evidence and risk;
- held-out comparison against the active revision and no-skill baseline where
  meaningful;
- exact-cohort canary, promotion receipt, re-selection, and re-measurement;
- quarantine, automatic restore, and catalog-epoch restoration.

### Out of scope

- broad multi-skill consolidation;
- changing policy, permissions, or safety rules through skill learning;
- unbounded task transcript ingestion;
- autonomous promotion of privileged or high-risk effects;
- self-modification of the evaluator or promotion policy;
- multiple mutation classes or global cohort expansion.

## Implementation Plan

### 1. Select one learning target

Choose a skill with exact delivered revisions, repeated verified outcomes, and a
specific observed defect or cost. State why revising the skill is smaller and
more appropriate than changing policy, memory, tooling, retrieval, or task
state. New skill synthesis is allowed only when no existing skill can represent
the reusable procedure.

### 2. Build a bounded learning review

Normalize relevant evidence into an episode summary that preserves provenance,
identity, environment, verifier quality, uncertainty, and counterevidence.
Exclude raw private payloads and one-off facts that do not belong in procedural
memory.

### 3. Create an immutable proposal

Bind the proposal to the active skill checksum, catalog epoch, evidence digest,
learning-policy revision, intended benefit, risk class, and rollback target.
Acquire one target writer lease. A stale base, wrong owner, or concurrent writer
must fail before file effects.

### 4. Evaluate without self-attestation

Run deterministic checks first, then statistical or model-assisted comparison
only when needed. The task performer or proposal author cannot be the sole
judge. Freeze held-out cases and acceptance measures before comparing the
candidate.

### 5. Canary and promote

Install the candidate outside the active catalog, verify checksums and
compatibility, then activate only for the exact accepted cohort. Promotion is
an atomic catalog-epoch change with an immutable receipt. Failed or ambiguous
candidates remain blocked or quarantined.

### 6. Re-select, re-measure, and restore

Prove that the new revision is actually selected and delivered where intended,
measure its outcome and cost, and exercise exact restoration of both skill
bytes and catalog epoch. Retain rejection and rollback evidence for later
calibration.

## Acceptance Plan

| Gate | Required evidence | Minimum level |
|---|---|---|
| Attribution | The learning target is supported by exact delivered revisions, verified outcomes, and explicit uncertainty | T3 |
| Jurisdiction and ownership | One approved target and one writer; stale base, wrong owner, and concurrent mutation fail before effects | T2/T3 |
| Candidate safety | Lint, guard, approved-root, permission, compatibility, checksum, and secret checks pass; unsafe candidates quarantine | T2 |
| Independent evaluation | Frozen held-out comparison shows declared outcome improvement or cost reduction without material regression | T3 |
| Canary isolation | Only the exact cohort can receive the candidate revision | T3 plus bounded live evidence when served |
| Atomic promotion | Skill bytes and catalog epoch advance together or not at all | T3 interruption replay |
| Exact restore | Backup, downgrade, and restore recover both bytes and catalog epoch; active sessions follow declared revision policy | T3 |
| Closed loop | Selection, delivery, outcome, proposal, evaluation, promotion, re-selection, and post-promotion outcome form one reproducible evidence chain | T3 end-to-end |

One successful model judgment is not acceptance. The phase requires a replayable
chain and an exercised restore.

## Acceptance Artifacts

- target-selection rationale and evidence digest;
- bounded learning review;
- immutable proposal and decision trace;
- candidate revision and checksum;
- held-out evaluation report;
- canary, promotion, re-selection, and outcome receipts;
- exact restore evidence;
- rejected or quarantined cases used to verify fail-closed behavior;
- phase acceptance record.

## Public Explanation at Acceptance

Public text must identify the procedural skill behavior that improved, the
outcome evidence and evaluator boundary, the canary scope, and the exact restore
that was exercised. It must not describe one accepted low-risk revision as
general autonomous self-improvement.

## Transition from the Reversible Serving Cohort

Consume only exact outcomes from the accepted cohort. Preserve its serving
fallback independently of the mutation path. A learning worker cannot alter
cohort identity, serving authority, or the virtual-session revision history of
an already active task.

## Transition to Ecosystem Operations

Broader ecosystem operations may generalize only mechanisms proven by this
loop: writer fencing, proposal provenance, evaluator independence, atomic
promotion, and exact restore. Broader skill classes need class-specific
evaluators and rollback evidence; one low-risk success does not authorize a
global switch.

## Stop and Rollback Conditions

Stop and quarantine on ambiguous attribution, policy-like content, secret or
private-data capture, stale target checksum, competing writers, evaluation
leakage, lost rare capability, non-atomic promotion, or uncertain restore.
Return serving to the last accepted reversible-cohort revision and preserve all
receipts before diagnosis.
