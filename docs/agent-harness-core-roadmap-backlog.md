# Agent Harness Core Roadmap

Updated: 2026-07-26

This roadmap is organized around user-visible outcomes and risk, not internal
project phases. It answers one question: **what is the shortest safe path from a
stable harness to a skill ecosystem that measurably improves task results?**

Agent Harness Core is pre-1.0. Dates are directional; the exit conditions below
are the commitments.

## North Star

Maximize verified task-quality improvement per unit of latency, token cost, and
operator effort without weakening exact-lane authority, provider delivery,
restart recovery, or rollback.

A larger skill library, more generated text, more learning jobs, or a higher
selection rate is not success by itself. The ecosystem is improving only when
held-out tasks show better outcomes at an acceptable cost and the change can be
attributed, contained, and reversed.

## First-Principles Constraints

1. **Identity precedes intelligence.** Every decision and mutation stays bound
   to the exact agent, platform, account, channel, user, session, and virtual
   session that authorized it.
2. **One mutable resource has one owner.** A queue lease, service child, skill
   revision, catalog epoch, or topology revision must never have competing
   writers.
3. **Observe before changing serving.** Shadow decisions may collect bounded
   evidence, but they must not alter prompt bytes, priors, learning targets, or
   provider output.
4. **Outcomes precede optimization.** Selection and delivery receipts become
   useful only after they can be joined to task effects, verified outcomes,
   cost, and the exact skill revisions delivered.
5. **Reversibility precedes autonomy.** Every promoted skill or topology change
   needs an immutable source, candidate checksum, backup, activation receipt,
   and tested downgrade path.
6. **Verification follows blast radius.** Pure formatting needs a small check;
   identity, routing, learning, supervisor, or delivery changes need scenario
   replay. Long-duration reliability claims need an actual soak.
7. **Platform work must unlock a measured constraint.** Queue rewrites,
   additional daemons, warm pools, and new protocols are not prerequisites
   unless current evidence shows that they block the next user outcome.
8. **Public claims match evidence.** Source tests, staged replay, a live
   cutover, provider-visible delivery, and long-duration operation are separate
   claims.

## Stable Foundation

The following capabilities are the baseline, not future roadmap promises:

- durable, account-aware exact-lane queue and continuation authority;
- restart-safe Goals, terminal control, receipts, and provider delivery state;
- guarded external effects with intent, result, reconciliation, and rollback
  boundaries;
- Windows-first standalone supervision with one Rust master directly owning
  configured service children, plus a compatibility topology for migration and
  rollback;
- deterministic skill discovery, eligibility, selection receipts, bounded
  prompt delivery, lint, guard, checksum validation, archive/restore, and
  checksum-locked packs;
- proposal and decision traces for skill mutation, guarded autonomous apply for
  the currently supported agent-created path, and an explicit non-mutating
  mode;
- public invariant, schema, topology, and release-check contracts.

The direct-owner process model is validated on Windows. Linux has native process
identity and ownership leases but not equivalent direct-child containment or
host recovery. macOS lifecycle parity is not yet established. Those are honest
portability gaps, not blockers for the Windows skill-upgrade critical path.

## The Critical Path

The steps below deliberately form one thin vertical slice. Each step must leave
a usable artifact and must avoid introducing a second serving or mutation
authority.

### 1. Freeze the Skill Contract and Baseline

Define one versioned public contract for a skill revision and its serving card:

- stable skill ID, revision, checksum, provenance, lifecycle, and owning agent;
- trigger, competence role, hard eligibility, permissions, tools, and
  compatibility requirements;
- bounded disclosure modes and estimated prompt cost;
- aliases, supersession, conflicts, and rollback target;
- evaluator references and evidence needed for promotion.

Run one deterministic inventory of the existing library. Normalize metadata,
identify duplicate or conflicting entries, record unsupported files, and build
a non-serving candidate catalog. The inventory must not move canonical skill
directories or change active selection.

Exit conditions:

- the same source tree produces the same catalog digest;
- every active entry has a source checksum and a valid owner/scope;
- malformed, ambiguous, or unsafe entries fail closed without blocking
  unrelated skills;
- the current catalog can be restored byte-for-byte;
- a reviewed replay corpus covers positive, negative, ambiguous, CJK, explicit
  invocation, retired, and agent-isolation cases.

### 2. Shadow Role-First Routing and Outcome Join

Run the new role-first matcher beside the current matcher. It may emit private
receipts only; current selection, prompt bytes, usage priors, provider output,
and learning targets remain unchanged.

Each shadow receipt records the task-local competence need, eligible candidates,
hard-gate rejections, calibrated abstention, candidate revisions, estimated
prompt bytes, and the current catalog digest. Raw private task text is not
stored in the receipt.

Join routing and delivery to:

- exact virtual-session manifest;
- tool and external-effect receipts;
- independently verified or explicitly unknown task outcome;
- latency, token, and retry cost;
- terminal learning review.

Exit conditions:

- repeated preparation is idempotent and cannot manufacture extra decisions;
- exact-lane and agent isolation replays pass;
- every measured outcome identifies the exact delivered revisions or records
  that no skill was delivered;
- reviewed labels show the new matcher improves precision, abstention, or prompt
  cost without increasing forbidden selections;
- missing outcome evidence remains `unknown`, never inferred as success.

### 3. Activate a Narrow Serving Cohort

Enable the new router for one exact cohort and a small set of low-risk skills.
Freeze the catalog, topology, and skill revisions for each virtual session.
Deliver the minimum sufficient body and rehydrate that same body across
concrete-session rollover without creating a second usage event.

Exit conditions:

- a feature switch restores the previous serving policy without data
  conversion;
- frozen sessions do not drift when the catalog changes;
- `/new` starts a new manifest and carries no stale task intent;
- selection, delivery, outcome, cost, and final provider behavior replay
  end-to-end;
- held-out outcomes improve or prompt cost falls without a material quality
  loss;
- a failed receipt, uncertain identity, or unknown revision leaves current
  serving unchanged.

### 4. Close One Learning Loop

Start with the smallest useful mutation: improve one selected skill with clear
attribution. New skill synthesis is allowed only when no eligible skill can
represent the learned procedure.

The path is:

```text
verified outcome
  -> bounded learning review
  -> immutable proposal and decision trace
  -> lint, guard, compatibility, and checksum checks
  -> held-out replay against the active revision
  -> exact-cohort canary
  -> promote or automatically restore
  -> re-select and re-measure
```

Deterministic evaluators are preferred. Model-based judgment may contribute
evidence but cannot be the sole authority for its own promotion.

Exit conditions:

- promotion requires a measured improvement or a documented cost reduction
  with no material quality regression;
- rejected and quarantined candidates cannot enter prompt assembly;
- concurrent learning cannot create two active revisions;
- backup, downgrade, and restore are replayed before expansion;
- rollback restores both skill bytes and the active catalog epoch;
- a full selection-to-outcome-to-improvement-to-reselection scenario passes.

### 5. Consolidate and Expand Autonomy

Only after the single-skill loop is reliable, add a serialized library writer,
versioned relationship topology, duplicate consolidation, conflict resolution,
and scheduled reflection.

Foreground work always wins resource contention. Scheduled, startup catch-up,
and operator triggers enter the same durable job ledger and cannot bypass
identity, budget, replay, safety, or rollback policy.

Exit conditions:

- one `agent + library revision` writer lease fences concurrent consolidation;
- staged candidates do not advance the source watermark until validation
  succeeds;
- semantically unchanged runs complete without model review or catalog churn;
- aliases and tombstones preserve old manifests and downgrade;
- a controlled scheduled soak proves bounded resource use, restart recovery,
  idempotency, and exact rollback;
- broader autonomous mutation expands by evidence class and cohort, not with one
  global switch.

## Immediate Delivery Slice

The next implementation slice should include only:

1. the versioned skill card and catalog digest;
2. deterministic library inventory and non-serving rebaseline report;
3. the reviewed routing replay corpus;
4. default-off role-first shadow receipts;
5. an exact join from routing and delivery to outcome and cost;
6. a comparison report that can justify, reject, or refine the first serving
   cohort.

This slice is complete when it can answer, with reproducible evidence:

- Which competence did the task need?
- Which skills were eligible, rejected, selected, and actually delivered?
- Did the task outcome improve?
- What did the improvement cost?
- Can the decision and all active bytes be reproduced and rolled back?

It does **not** need a new queue engine, warm app-server pool, native MCP server,
cross-platform installer, broad knowledge graph, or unattended structural
mutation.

## Continuous Stability Rails

These run alongside the critical path and block promotion when affected. They
are not a separate multi-month phase.

| Boundary | Required evidence |
|---|---|
| Exact identity and authority | Cross-agent, cross-account, cross-channel, rollover, stale-owner, and successor-owner replays |
| Persistence | Atomic write, interruption, malformed record, duplicate replay, and old-reader compatibility tests |
| Runtime ownership | One-master/one-writer assertions, crash recovery, ordered drain, process-tree cleanup, and no residual child |
| Provider delivery | Exact-lane Telegram and Discord send, receipt, readback, retry, and duplicate-suppression checks when adapters change |
| Skill mutation | Lint, guard, target/checksum compare, backup, quarantine, canary, downgrade, and restore |
| Security and privacy | Approved roots, secret/path/identifier scans, bounded evidence, and no raw provider or prompt payload in public artifacts |
| Resource bounds | Queue depth, prompt bytes, tokens, latency, disk growth, retry budget, and scheduled-job contention |
| Release truthfulness | Fresh source tests, scenario replay proportional to blast radius, public-diff review, and separately labeled live or soak evidence |

For a stabilized cross-cutting candidate, one independent review is sufficient
before release. Patch-and-review loops are not a substitute for coherent local
verification; a new review is needed only when the reviewed behavior changes or
new live evidence exposes a materially different risk.

## Evidence Levels

The project uses these plain-language verification levels:

| Level | Meaning | Appropriate claims |
|---|---|---|
| T0 | Static inspection, formatting, schema shape | The artifact is well formed |
| T1 | Focused unit or contract test | A local rule behaves as specified |
| T2 | Component integration | Coupled modules interoperate in a controlled environment |
| T3 | Scenario or replay | User-visible workflow, restart, routing, or rollback behavior survives realistic boundaries |
| T4 | Controlled live or endurance evidence | Provider-visible or long-duration behavior works in the authorized environment |

Passing a lower level never implies a higher one. The required level is chosen
from the changed behavior and blast radius, not from the amount of ceremony.

## Prioritized Backlog

### Now: Directly Unlock the Skill Upgrade

| Outcome | Why now | Promotion gate |
|---|---|---|
| Versioned skill card and deterministic catalog | Makes every later decision reproducible | Schema compatibility, stable digest, rollback |
| Full library inventory and rebaseline | Removes ambiguous inputs before optimization | No serving mutation, reviewed exceptions |
| Role-first shadow router | Measures a better selection policy without risking output | Exact-lane T3 replay and comparison report |
| Outcome/effect attribution join | Distinguishes useful skill behavior from activity | Unknown stays unknown; no self-attestation |
| Narrow serving cohort | Converts evidence into user value with bounded blast radius | Frozen manifest, provider replay, one-switch rollback |
| Single-skill improvement loop | Proves the ecosystem can learn, promote, and recover | Held-out win, canary, exact restore |

### Next: Scale a Proven Loop

| Outcome | Entry condition |
|---|---|
| Serialized library consolidation writer | Single-skill learning loop is stable |
| Versioned relationship topology and conflict policy | Catalog revisions and downgrade are reliable |
| Scheduled reflection and startup catch-up | Foreground priority, budgets, and idempotency are proven |
| Broader skill classes and cohorts | Per-class evaluators and rollback evidence exist |
| Adaptive disclosure depth | Outcome and cost attribution can compare disclosure policies |
| Linux direct-child ownership | Process-group containment, signal drain/escalation, and native host recovery are implemented |
| macOS lifecycle parity | Native identity, containment, recovery, and packaging evidence exist |

### Later: Valuable but Not on the Critical Path

- unified queue-store migration and retirement of legacy JSONL paths;
- warm app-server pooling and more aggressive latency optimization;
- a native MCP control surface;
- broad repository or procedural knowledge graphs;
- richer memory-service collaboration beyond current bounded contracts;
- privileged or pre-logon host-service packaging;
- unattended structural consolidation across high-risk skill classes;
- deletion-based retention policies where archive/tombstone remains sufficient.

These items move forward when measurement shows that the current mechanism is
the bottleneck or when a supported deployment requires them.

## Issue and Release Selection

Work is ranked by expected verified user value divided by risk, delay, and
coordination cost. An item enters the active slice only when:

- its user or reliability outcome is explicit;
- the current bottleneck is evidenced;
- the smallest reversible change is identified;
- an owner and one observable exit condition exist;
- required scenario fixtures can be named before implementation.

An issue closes when its acceptance behavior and regression evidence exist, not
when a planning label changes. A release is ready when the merged source,
version metadata, changelog, public contracts, tests, and tag agree; source,
deployed binary, provider smoke, and endurance status remain separately stated.

## Roadmap Maintenance

- Update this file when evidence changes the critical path, not for every local
  implementation detail.
- Keep command, schema, and invariant detail in their canonical references.
- Move completed outcomes into the stable foundation instead of preserving a
  historical maze of phase codes.
- Add a deferred item only when its value and entry condition can be stated.
- Prefer one coherent release candidate and one risk-appropriate independent
  judgment over repeated ceremonial review cycles.

Related contracts:

- [Skill Ecosystem](skill-ecosystem.md)
- [Adaptive Skill Intelligence](adaptive-skill-intelligence.md)
- [Topology Contract](agent-harness-topology-contract.md)
- [Invariant Catalog](invariants.md)
- [Schema Registry](schema-registry.md)
