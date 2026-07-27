# Phase 5 — Ecosystem Operations

- **Status:** planned
- **Depends on:** accepted governed learning and exact-restore loop
- **Unlocks:** evidence-based expansion by skill class, cohort, and supported
  platform

## Outcome

The proven learning and lifecycle loop operates across a broader skill library
through one serialized authority, bounded background work, versioned
relationships, deterministic no-change behavior, and evidence-class-specific
promotion and rollback.

## Purpose in the Whole Project

The governed learning loop proves that adaptation can work once. Ecosystem
operations make it supportable and economical to repeat without turning the
library into an uncontrolled source of prompt growth, conflicting procedures,
background contention, or irreversible churn.

This phase is not "enable all autonomy." It establishes ecosystem operations:
how relationships, consolidation, scheduling, drift, budgets, portability, and
expansion are governed after the narrow loop is trustworthy.

## Starting Point

The governed learning work supplies one accepted evidence chain, evaluator
boundary, writer lease, promotion transaction, and exact restore. Current source
also provides lifecycle state, packs, archive/restore, idle-gated curation,
durable worker jobs, and restart-safe supervision. These mechanisms are reused
rather than replaced.

## Scope

### In scope

- one serialized library writer per agent and library revision;
- versioned relationships for equivalence, subsumption, composition,
  dependency, variant, conflict, alias, and supersession;
- deterministic no-change detection;
- staged consolidation and conflict-resolution candidates;
- foreground-first resource policy;
- durable scheduled reflection and startup catch-up through the existing job
  authority;
- per-skill-class evaluator, canary, promotion, and rollback policy;
- drift, cost, false-promotion, rollback, and lost-coverage monitoring;
- progressive cohort and skill-class expansion;
- public compatibility and portability matrices tied to executed evidence.

### Out of scope

- one global autonomy switch;
- replacing the queue store without measured need;
- unrestricted high-risk or policy mutation;
- deleting history when aliases, archive, or tombstones preserve compatibility;
- warm pools, native MCP, broad knowledge graphs, or new process topology
  unless measurements show they block ecosystem operations;
- claiming cross-platform parity from Windows evidence.

## Implementation Plan

### 1. Establish the serialized library authority

Fence every mutation and consolidation run with an `agent + library revision`
writer lease. Bind inputs, source watermark, policy revision, resource budget,
and output candidate. A failed run does not advance the watermark.

### 2. Add versioned relationship topology

Represent relationships explicitly instead of merging by similarity alone.
Preserve cheaper specialists, complementary roles, tool or risk variants, and
dependency order. Conflicts must resolve before incompatible full procedures
can be disclosed together.

### 3. Make no-change the cheapest outcome

Hash normalized inputs and relationships before semantic review. When evidence
and source are unchanged, complete without model evaluation, catalog revision,
or file churn. Record bounded diagnostic evidence rather than a new proposal.

### 4. Stage consolidation and lifecycle changes

Build candidates outside the active catalog. Preserve aliases, tombstones,
supersession routes, old manifests, pack locks, and downgrade targets.
Multi-skill changes are tested and promoted atomically when their behavior is
coupled.

### 5. Schedule work through existing durable authority

Periodic reflection, startup catch-up, and operator-triggered runs enter the
same durable ledger. Foreground tasks win contention. Open work, runtime leases,
provider effects, mutation locks, budget exhaustion, or uncertain health defer
background work without manufacturing failure or duplicate runs.

### 6. Expand by evidence class

Define evaluators and rollback for each new skill class and cohort. Start with
low-risk procedural improvements, then expand only when false promotion,
restoration, cost, and outcome evidence remain within declared limits.
Privileged, external-effect, or policy-adjacent content retains stricter
authority.

### 7. Maintain portability continuously

Keep skill contracts and shared runtime mechanisms platform-neutral, compile
and replay them on every available supported-platform gate, and isolate native
process, permission, and packaging adapters behind explicit platform
boundaries. Bind operational claims to executed platform evidence. Missing
Linux or macOS direct-owner lifecycle, packaging, or provider proof remains a
deployment gap for that platform; it cannot justify coupling shared product
behavior to Windows.

## Acceptance Plan

| Gate | Required evidence | Minimum level |
|---|---|---|
| Writer serialization | Concurrent, stale, restarted, and successor runs cannot create two active library revisions | T3 |
| Watermark integrity | Validation failure, interruption, or quarantine leaves source watermark and active catalog unchanged | T3 |
| Relationship safety | Equivalent, subsuming, complementary, dependent, variant, conflicting, aliased, and superseded cases preserve intended coverage and order | T3 replay |
| No-change efficiency | Unchanged inputs finish without model evaluation, catalog churn, or mutation receipts beyond bounded diagnostics | T2 plus measured run |
| Atomic coupled changes | Multi-skill candidates promote or restore as one versioned unit | T3 interruption and restore replay |
| Background governance | Foreground priority, leases, budgets, startup catch-up, idempotency, and restart recovery hold under scheduled execution | T4 controlled soak |
| Expansion quality | Each added skill class has a frozen evaluator, held-out evidence, canary, false-promotion bound, and exact rollback | T3/T4 by class |
| Resource bounds | Prompt, evaluator, latency, disk, retry, and maintenance costs remain within declared operating budgets | T4 soak |
| Compatibility | Old virtual-session manifests, aliases, packs, readers, and downgrade targets remain usable | T3 |

The phase is accepted for the tested classes and cohorts only. Acceptance does
not imply unlimited autonomy, every provider, or cross-platform parity.

## Acceptance Artifacts

- serialized writer and source-watermark contract;
- versioned relationship schema and migration evidence;
- no-change and staged-candidate behavior;
- consolidation, conflict, alias, supersession, and restore replay corpus;
- durable scheduling and startup-catch-up evidence;
- controlled soak report with resource bounds;
- per-class evaluator and rollout matrix;
- supported cohort and platform compatibility table;
- phase acceptance record.

## Public Explanation at Acceptance

Public text must name the skill classes and cohorts that can be maintained, the
serialized ownership and resource behavior, the soak and rollback evidence, and
the platforms actually exercised. It must not translate bounded ecosystem
operations into claims of unrestricted autonomy or cross-platform parity.

## Transition from the Governed Learning Loop

Generalize the accepted learning-loop mechanisms without weakening their
guarantees. If a class cannot reuse the evaluator, atomic promotion, or restore
contract, treat it as a new bounded implementation slice rather than silently
widening the old evidence.

## Ongoing Expansion

After ecosystem operations are accepted, roadmap items are selected by measured
constraint:

- improve disclosure when context cost is the bottleneck;
- add routing features when outcome coverage or abstention is the bottleneck;
- add worker or queue capacity when durable scheduling is the bottleneck;
- add platform lifecycle work when a supported deployment requires it;
- add richer topology only when current relationship semantics lose measurable
  capability or create conflict.

This prevents infrastructure ambition from displacing verified task value.

## Stop and Rollback Conditions

Suspend background mutation on competing writers, stale watermarks, repeated
false promotion, lost rare capability, foreground contention, unbounded cost or
disk growth, restart duplication, cross-identity evidence, or failed restore.
Keep the last accepted catalog active, park new jobs durably, and reduce scope
to the last independently accepted skill class or cohort.
