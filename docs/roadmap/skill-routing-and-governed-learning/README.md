# Skill Routing and Governed Learning Phase Plans

Updated: 2026-07-27

This folder turns the skill-ecosystem roadmap into one prerequisite
third-party-tool repair and five independently implementable,
independently acceptable skill-evolution phases. The Phase 0.1 label used
during live incident closure is retained only as history inside Phase 0; it
does not create a second roadmap. Phase 1 contains a reliability-foundation
workstream and the original reproducible-skill-contract workstream. A phase is
defined by durable capabilities and observable product outcomes. Architecture,
implementation, migration, testing, documentation, and release work happen
inside that phase; they are not separate phases by themselves.

The plans are public product-development documents. Operator credentials, live
identifiers, private receipts, machine paths, rollback commands, and raw
provider payloads belong in private evidence, not here.

## Whole-Program Result

```text
stable exact-identity runtime
  -> reliable third-party MCP availability in fresh backends
  -> prompt, bounded, non-duplicating progress surfaces with causal latency evidence
  -> reproducible skill inputs
  -> outcome-linked shadow comparison
  -> reversible serving improvement
  -> governed learning and exact restore
  -> serialized ecosystem operations
```

The runtime foundation protects identity, persistence, delivery, process
ownership, and rollback. The prerequisite repair makes external tool
availability truthful across config regeneration and backend restart. The
first Phase 1 workstream makes waiting observable and latency attributable
before later routing changes add more work. The later phase sequence adds
adaptive skill behavior without creating a second semantic authority beside
the backend or a second mutation authority beside the harness.

Each phase can be the long-term stopping point:

- after the third-party MCP reliability hot patch, ordinary fresh backend
  sessions can retain and prove generic external tool capabilities;
- after the Phase 1 reliability foundation, users receive prompt, stable
  progress feedback and operators can attribute latency to complete causal
  stages without duplicate terminal surfaces;
- after the reproducible-contract phase, the project has a supportable skill
  catalog whose inputs and rollback targets are exact;
- after the shadow-routing phase, it can compare alternative routing with real
  outcomes without changing serving;
- after the reversible-cohort phase, it has one evidence-backed user-facing
  routing or disclosure improvement;
- after the governed-learning phase, it has one complete and reversible
  outcome-to-skill-improvement loop;
- after the ecosystem-operations phase, it can run that proven loop across a
  broader library under one serialized authority.

## Phase Index

| Phase | Outcome | Plan |
|---|---|---|
| 0 | Generic third-party MCP registration survives config regeneration and is proven in a fresh backend process; incident-derived live closure also restores truthful provider presence and transaction-safe runtime control | [Reliable Third-Party MCP Hosting](phase-0-reliable-third-party-mcp-hosting.md) |
| 1A | Prompt, bounded, non-duplicating progress UX plus complete causal latency and restart evidence | [Progress UX and Latency Foundation](phase-1-progress-ux-and-latency-foundation.md) |
| 1B | Reproducible skill contract, catalog, baseline, and replay corpus | [Reproducible Skill Contract](phase-1-reproducible-skill-contract.md) |
| 2 | Side-effect-free role-first routing with exact outcome and cost attribution | [Shadow Routing and Attribution](phase-2-shadow-routing-and-attribution.md) |
| 3 | Measured serving improvement for one exact cohort with immediate fallback | [Reversible Serving Cohort](phase-3-reversible-serving-cohort.md) |
| 4 | One outcome-governed skill improvement from proposal through exact restore | [Governed Learning Loop](phase-4-governed-learning-loop.md) |
| 5 | Serialized, budgeted ecosystem maintenance and evidence-based expansion | [Ecosystem Operations](phase-5-ecosystem-operations.md) |

Phase 0 verification, live closure, and remaining platform limits are recorded
in
[Reliable Third-Party MCP Hosting Acceptance](phase-0-acceptance.md).

The concise public roadmap remains
[Agent Harness Core Roadmap](../../agent-harness-core-roadmap-backlog.md).

## Current Starting Point

The current source candidate provides:

- exact agent and channel eligibility;
- deterministic skill discovery, selection receipts, and bounded delivery;
- virtual-session continuity foundations;
- proposal-mediated mutation, guarded apply, backup, quarantine, and restore;
- lifecycle and checksum-locked pack operations;
- restart-safe Goals and a Windows direct-owner process topology;
- transactional Codex config ownership that preserves operator-managed generic
  MCP registrations;
- same-connection authentication, server identity, and tool-schema readiness
  before a declared required capability can admit a turn;
- long-worker renewable authority, child-tree containment, and restart-safe
  worker-to-cron terminal reconciliation.

The v0.13.1 release passed its source, staging, independent-review, guarded
cutover, and live-readback gates recorded in the
[third-party MCP hosting acceptance record](phase-0-acceptance.md). The
incident-derived Phase 0.1 closure added explicit Discord presence,
complete-store pre-cutover gating, generation-fenced runtime transactions, and
operator-safe live control. Provider-visible Discord presence and one natural
turn on each supported chat channel were observed after cutover. The release
publishes that accepted implementation after the hash-bound live promotion;
release metadata was finalized afterward, so the live process is not claimed
to run the exact release tag or binary. This is not native-macOS proof, a green
complete-Linux suite, or a first-time named-consumer mount.

## Rules Shared by Every Phase

### Preserve one authority per decision

The backend owns in-turn reasoning and tool sequencing. The harness owns durable
identity, eligibility, revision continuity, evidence joins, promotion policy,
and rollback. A phase must not introduce competing selection, continuation, or
mutation writers.

### Keep identity exact

Routing, delivery, outcome, and mutation evidence remain bound to the exact
agent, platform, account, channel, user, session, and virtual session. Evidence
from a nearby lane cannot satisfy a gate.

### Separate observation, staging, serving, and live proof

A schema test does not prove a replay. A replay does not prove provider-visible
delivery. A live smoke does not prove endurance. Every acceptance record states
the highest verification level actually reached.

### Make uncertainty explicit

Missing outcomes remain `unknown`. Selection, disclosure, tool use, or a model
claim is not success evidence. The system must abstain when eligibility,
identity, revision, or evidence is uncertain.

### Require reversibility before expansion

Every serving or mutation phase defines its prior policy, immutable inputs,
activation record, fallback trigger, and exact restoration check before the new
path can be promoted.

### Prevent self-attestation

The component or model that proposes a behavioral change cannot be the sole
authority that evaluates and promotes it. Deterministic evidence is preferred;
ambiguous or high-impact changes require an independent judgment appropriate
to their blast radius.

### Preserve public and private boundaries

Public contracts explain behavior, compatibility, verification, and open gaps.
Raw task text, provider identifiers, credentials, private receipts, local
operator paths, and recovery commands remain outside public artifacts.

### Explain behavior instead of internal labels

Phase numbers are navigation aids, not public context. A public roadmap entry,
issue, pull request, changelog item, release note, example, or generated page
must state:

1. which product capability changes;
2. who benefits and what becomes possible or safer;
3. what evidence supports the claim;
4. which serving, deployment, provider, platform, or endurance limits remain.

An internal round name, package code, milestone label, phase number, or bare
"upgrade" does not answer those questions and cannot serve as the public
description.

## Verification Levels

| Level | Evidence | Typical use in these plans |
|---|---|---|
| T0 | Static structure, formatting, schema shape | Documentation and generated-artifact checks |
| T1 | Focused rule or contract test | Eligibility, checksum, parser, and fail-closed behavior |
| T2 | Controlled component integration | Catalog, routing, delivery, outcome, and migration joins |
| T3 | Realistic scenario or replay | Exact identity, config regeneration, fresh backend, restart, frozen revision, serving, mutation, and restore |
| T4 | Authorized live or endurance evidence | Exact-tag ordinary-backend proof, provider-visible cohort behavior, and scheduled-operation soak |

A phase plan names its minimum evidence. Lower evidence can support development
but cannot close the phase.

## Phase Transition Contract

Every completed phase produces a short acceptance record containing:

1. the accepted source commit and relevant artifact digests;
2. the contract, catalog, topology, and policy versions in force;
3. executed test and replay counts with failures and exclusions;
4. the highest evidence level for each affected boundary;
5. rollback or restoration evidence;
6. known limitations and explicitly deferred work;
7. the exact inputs the next phase may rely on.

The next phase must consume this record rather than infer state from an old
roadmap checkbox or a stale `PASS`.

## Continuous Tracks

These are not additional phases. They run only when the current phase touches
their boundary:

| Track | Promotion requirement |
|---|---|
| Runtime ownership | One master, one mutable owner, restart recovery, ordered drain, and no residual children |
| Persistence | Atomic write, malformed-record handling, idempotent replay, and prior-reader compatibility |
| External tools | Config ownership, generated-versus-reused provenance, fresh-backend capability digest, home isolation, and required/optional failure behavior |
| Provider delivery | Exact-lane Telegram and Discord evidence when serving or adapter behavior changes |
| Security and privacy | Approved roots, least authority, secret/path/identifier scans, and bounded evidence |
| Resource use | Prompt bytes, latency, tokens, disk growth, retries, and background contention remain bounded |
| Public compatibility | Schema registry, invariants, examples, generated docs, changelog, and release claims agree |

## Work Deliberately Outside the Critical Path

The following work starts only when measurement or a supported deployment proves
it blocks a phase outcome:

- replacing the current queue store;
- warm backend or app-server pools;
- a native MCP control surface;
- broad repository knowledge graphs;
- privileged host-service packaging;
- Linux or macOS direct-owner parity;
- unattended high-risk structural mutation;
- deletion-based retention where archive or tombstone is sufficient.

Deferring these items protects the shortest path to measurable skill value. It
does not classify them as unimportant.

## Using a Phase Plan

Before implementation, fill the plan's concrete owner, candidate branch, target
contracts, replay corpus revision, and acceptance evidence location. During
implementation, keep discoveries in a private WAL when they contain operational
details. At acceptance, update the plan only with durable public conclusions;
do not turn it into a raw execution log.
