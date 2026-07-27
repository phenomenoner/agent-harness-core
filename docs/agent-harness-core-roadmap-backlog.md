# Agent Harness Core Roadmap

Updated: 2026-07-26

This roadmap is organized into one prerequisite third-party-tool hot patch and
five skill-evolution phases defined by independently useful outcomes, not by
internal activity such as design, coding, or testing. It answers one question:
**what is the shortest safe path from a stable harness to a skill ecosystem
that measurably improves task results?**

Agent Harness Core is pre-1.0. Dates are directional; the exit conditions below
are the commitments.

## North Star

Maximize verified task-quality improvement per unit of latency, token cost, and
operator effort without weakening exact-lane authority, truthful external-tool
availability, provider delivery, restart recovery, or rollback.

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
3. **Capabilities precede skill decisions.** A registration command, config
   file, or cached plan cannot prove that a fresh backend process has a required
   external tool. Tool eligibility needs exact initialized capability evidence.
4. **Observe before changing serving.** Shadow decisions may collect bounded
   evidence, but they must not alter prompt bytes, priors, learning targets, or
   provider output.
5. **Outcomes precede optimization.** Selection and delivery receipts become
   useful only after they can be joined to task effects, verified outcomes,
   cost, and the exact skill revisions delivered.
6. **Reversibility precedes autonomy.** Every promoted skill or topology change
   needs an immutable source, candidate checksum, backup, activation receipt,
   and tested downgrade path.
7. **Verification follows blast radius.** Pure formatting needs a small check;
   identity, routing, learning, supervisor, or delivery changes need scenario
   replay. Long-duration reliability claims need an actual soak.
8. **Platform work must unlock a measured constraint.** Queue rewrites,
   additional daemons, warm pools, and new protocols are not prerequisites
   unless current evidence shows that they block the next user outcome.
9. **Public claims match evidence.** Source tests, staged replay, a live
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
deployment-evidence gaps, not permission to make shared product behavior
Windows-specific. Every phase must keep core contracts and implementation
portable, pass the available non-Windows compile/unit gates, and isolate
platform adapters explicitly. A deployment claim remains limited to platforms
whose lifecycle, packaging, permissions, and provider behavior were actually
executed.

## Phase Map

Each phase must leave the product in a coherent state that can be operated,
measured, and supported even if the next phase never starts.
Phase numbers are navigation only; the capability, user or developer impact,
evidence, and remaining limits are the public explanation.

| Phase | Independently useful outcome | Why it exists | Unlocks |
|---|---|---|---|
| [0. Reliable Third-Party MCP Hosting](roadmap/skill-routing-and-governed-learning/phase-0-reliable-third-party-mcp-hosting.md) | Generic MCP registrations survive generated-config updates and are proven in a fresh backend process | Skill and routing evidence is invalid when the host silently removes required tools | Truthful tool eligibility for the skill catalog |
| [1. Reproducible Skill Contract](roadmap/skill-routing-and-governed-learning/phase-1-reproducible-skill-contract.md) | Every active skill and catalog can be identified, reproduced, and restored | Later routing and learning are unsafe if their inputs are ambiguous | Comparable routing experiments |
| [2. Shadow Routing and Attribution](roadmap/skill-routing-and-governed-learning/phase-2-shadow-routing-and-attribution.md) | A better router can be evaluated against real outcomes without changing serving | Selection activity is not evidence of task improvement | An evidence-backed serving cohort |
| [3. Reversible Serving Cohort](roadmap/skill-routing-and-governed-learning/phase-3-reversible-serving-cohort.md) | One exact cohort receives a measured, rollback-safe improvement | Shadow evidence creates no user value until it can be served safely | Outcome evidence from real use |
| [4. Governed Learning Loop](roadmap/skill-routing-and-governed-learning/phase-4-governed-learning-loop.md) | One skill can improve from verified experience and be exactly restored | This proves adaptation is a controlled capability rather than file churn | Safe repetition across skill classes |
| [5. Ecosystem Operations](roadmap/skill-routing-and-governed-learning/phase-5-ecosystem-operations.md) | Proven learning and lifecycle operations scale under one serialized authority | Broad autonomy is valuable only after one narrow loop is reliable | Evidence-based expansion and portability |

The
[phase-plan index](roadmap/skill-routing-and-governed-learning/README.md)
defines the shared acceptance rules, transition contract, and document
ownership. Each linked plan is self-contained enough to implement and accept
independently, while preserving its place in the whole program.

## Phase Roadmap

The steps below deliberately form one thin vertical slice. The v0.13.1 hot
patch first makes third-party tool availability truthful. Each later step must
leave a usable artifact and must avoid introducing a second serving or mutation
authority.

### Phase 0 — Keep Third-Party MCP Tools Available in Fresh Backends

**Project purpose:** prevent harness-managed backend config regeneration from
silently removing operator or plugin MCP registrations, and prove actual tool
availability in a newly started backend process. Without this repair, later
skill eligibility and outcome attribution can confuse a host failure with a
skill-quality failure.

Implementation and acceptance plan:
[Phase 0 — Reliable Third-Party MCP Hosting](roadmap/skill-routing-and-governed-learning/phase-0-reliable-third-party-mcp-hosting.md).

The v0.13.1 hot patch establishes:

- explicit ownership for harness-managed config and generic `mcp_servers`
  registrations;
- semantic deterministic merge with typed malformed, duplicate, unsupported,
  and ownership-ambiguous behavior;
- transactional writes, private permissions, redacted receipts, and exact
  recovery;
- generated-versus-reused plan provenance;
- required and optional external-capability declarations;
- fresh-backend capability proof for exact agent and provider homes;
- a generic mock MCP lifecycle covering regeneration, restart, isolation,
  failure, and rollback.

Exit conditions:

- repeated uncached generation preserves registrations byte-idempotently;
- interrupted or invalid materialization leaves the original config exact;
- cached plan evidence cannot satisfy a regeneration or readiness gate;
- a fresh backend exposes the expected generic capability digest;
- missing required capabilities fail before turn admission while optional
  dependencies warn without false readiness;
- cross-agent and cross-home tests show no registration, profile, secret, or
  capability leakage;
- the frozen candidate passes full regression, one independent review, and
  exact config/runtime restoration;
- public text states supported homes, transports, platforms, executed evidence,
  and whether the exact release tag was deployed.

### Phase 1 — Establish a Reproducible Skill Contract

**Project purpose:** make every later routing, delivery, evaluation, and
mutation decision refer to immutable, reproducible inputs. This phase improves
operability and auditability without changing serving.

Implementation and acceptance plan:
[Phase 1 — Reproducible Skill Contract](roadmap/skill-routing-and-governed-learning/phase-1-reproducible-skill-contract.md).

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

### Phase 2 — Prove Better Routing Without Changing Serving

**Project purpose:** determine whether role-first retrieval and minimal
disclosure improve outcomes or cost before allowing them to affect a prompt.
This phase turns selection receipts into attributable product evidence.

Implementation and acceptance plan:
[Phase 2 — Shadow Routing and Attribution](roadmap/skill-routing-and-governed-learning/phase-2-shadow-routing-and-attribution.md).

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

### Phase 3 — Deliver a Reversible Serving Improvement

**Project purpose:** convert proven shadow evidence into the first user-visible
skill improvement, with exact identity boundaries and a one-switch fallback to
the previous serving policy.

Implementation and acceptance plan:
[Phase 3 — Reversible Serving Cohort](roadmap/skill-routing-and-governed-learning/phase-3-reversible-serving-cohort.md).

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

### Phase 4 — Prove One Governed Learning Loop

**Project purpose:** prove that verified experience can improve one procedural
asset without allowing the task performer, evaluator, and promoter to
self-attest the same deployment.

Implementation and acceptance plan:
[Phase 4 — Governed Learning Loop](roadmap/skill-routing-and-governed-learning/phase-4-governed-learning-loop.md).

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

### Phase 5 — Operate the Ecosystem at Scale

**Project purpose:** reduce the marginal cost of maintaining and improving the
library after the narrow learning loop is reliable. Scale remains serialized,
budgeted, reversible, and subordinate to foreground work.

Implementation and acceptance plan:
[Phase 5 — Ecosystem Operations](roadmap/skill-routing-and-governed-learning/phase-5-ecosystem-operations.md).

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

## Immediate Hot Patch: v0.13.1

The next implementation slice restores truthful third-party MCP availability.
It includes only:

1. the generic config ownership and unsupported-syntax contract;
2. a bounded decision between document/AST merge and the existing lexical
   approach;
3. transactional config materialization with secret and permission safety;
4. generated-versus-reused plan provenance;
5. required and optional external-capability readiness;
6. a generic mock MCP fresh-backend, restart, isolation, and rollback scenario;
7. full frozen-candidate verification, one independent review, public release
   context, and a v0.13.1 rollback bundle.

This slice does **not** implement a consumer-specific adapter, third-party
application protocol, native MCP control surface, marketplace, or untested
cross-platform parity.

## Next Skill Delivery Slice

After the hot patch is accepted, the next skill slice establishes the
reproducible skill contract and the non-serving foundation for shadow routing
and outcome attribution. It should include only:

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
| Third-party MCP hosting | Config ownership, uncached regeneration, fresh-backend capability digest, required/optional behavior, home isolation, secret/ACL safety, and exact rollback |
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
| T3 | Scenario or replay | Config regeneration, fresh backend, user-visible workflow, restart, routing, or rollback behavior survives realistic boundaries |
| T4 | Controlled live or endurance evidence | Exact-tag ordinary-backend, provider-visible, or long-duration behavior works in the authorized environment |

Passing a lower level never implies a higher one. The required level is chosen
from the changed behavior and blast radius, not from the amount of ceremony.

## Prioritized Backlog

### Now: Preserve and Prove Generic Third-Party MCP Tools

| Outcome | Why now | Promotion gate |
|---|---|---|
| Config ownership and semantic MCP preservation | Generated config currently can remove operator-managed registrations | Deterministic merge, malformed/duplicate policy, unchanged original on failure |
| Transactional materialization and redacted provenance | Partial writes and cached-plan ambiguity can create false readiness | Failpoint recovery, ACL/secret scan, generated-versus-reused receipt |
| Fresh-backend capability readiness | File and CLI evidence do not prove ordinary backend tools | Generic real-process restart scenario and exact capability digest |
| Agent and provider-home isolation | Preserved registrations must not cross identity or provider boundaries | Two-agent/default-provider matrix with negative leakage checks |
| v0.13.1 hot-patch release | Downstream skill eligibility needs a trustworthy tool substrate | Full frozen-candidate regression, one independent review, rollback bundle, accurate release text |

### After v0.13.1: Make Skill Routing Reproducible and Outcome-Linked

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
- a native MCP control surface, distinct from preserving third-party backend
  registrations;
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
- Update the applicable phase plan when its implementation boundary,
  acceptance evidence, stop condition, or transition contract changes.
- Keep command, schema, and invariant detail in their canonical references.
- Move completed outcomes into the stable foundation instead of preserving a
  historical maze of phase codes.
- Add a deferred item only when its value and entry condition can be stated.
- Prefer one coherent release candidate and one risk-appropriate independent
  judgment over repeated ceremonial review cycles.

Related contracts:

- [Skill Ecosystem](skill-ecosystem.md)
- [Adaptive Skill Intelligence](adaptive-skill-intelligence.md)
- [Configuration](configuration.md)
- [Trust Boundaries](trust-boundaries.md)
- [Topology Contract](agent-harness-topology-contract.md)
- [Invariant Catalog](invariants.md)
- [Schema Registry](schema-registry.md)
