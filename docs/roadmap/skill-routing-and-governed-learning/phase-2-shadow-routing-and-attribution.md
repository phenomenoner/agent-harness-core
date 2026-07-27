# Phase 2 — Shadow Routing and Attribution

- **Status:** planned
- **Depends on:** accepted reproducible skill catalog and replay corpus plus the
  accepted progress-contract revision and causal-latency schema
- **Unlocks:** a user-visible, rollback-safe improvement through the
  [Reversible Serving Cohort](phase-3-reversible-serving-cohort.md)

## Outcome

A role-first router runs beside current serving, produces no prompt or provider
side effects, and can be compared with the current policy using exact
revision-bound task outcomes, latency, and cost.

## Purpose in the Whole Project

The project needs evidence that a new selection policy is better before it
changes user-visible behavior. Selection counts, plausible explanations, or
model preference do not prove improvement.

This phase creates the attribution bridge between skill decisions and task
results. It is independently valuable because it reveals missing competence,
irrelevant disclosure, cost, conflicts, and unknown outcomes while current
serving remains untouched.

## Starting Point

The reproducible-contract work supplies immutable skill cards, a catalog
digest, and a reviewed replay corpus. The progress and latency foundation
supplies stable provider-surface authority and causal stage evidence. Current
serving continues to use the existing deterministic matcher and delivery
behavior. The target role-first, variable-depth policy has no serving authority
in this phase.

## Scope

### In scope

- a bounded task-state contract with objective stage, authority, risk,
  capabilities, and available evidence;
- unresolved competence-role identification;
- hard eligibility before candidate ranking;
- role-first candidate generation with calibrated no-skill abstention;
- revision-specific utility, compatibility, disclosure-cost, freshness, and
  conflict features;
- default-off shadow execution beside current serving;
- immutable shadow decision receipts with stable reason codes;
- an exact join across routing, delivered revisions, virtual-session manifest,
  effects, verifier evidence, terminal outcome, latency, tokens, retries, and
  cost;
- a frozen comparison method and report.

### Out of scope

- changing selected skills, prompt bytes, delivery depth, provider output, or
  current usage priors;
- using shadow choices as learning targets;
- promoting a skill or catalog;
- serving a cohort;
- broad Bayesian optimization or unattended mutation.

## Implementation Plan

### 1. Define the bounded task state

Build the smallest task representation needed to identify unresolved competence
roles. Preserve exact identity and authority, but do not persist raw prompt or
provider payloads in the routing receipt. Define redaction and retention before
collecting evidence.

### 2. Implement hard gates and role coverage

Reuse the accepted catalog's eligibility and compatibility results. Determine
which competence roles are unresolved and permit zero candidates when no
procedural memory has positive expected value. Keep role identification
separate from residency or prefetch.

### 3. Rank revision-specific candidates

Compare candidates using declared role fit, compatibility, disclosure cost,
freshness, conflicts, and uncertainty. Produce a deterministic result for
deterministic inputs. Model-assisted signals may be recorded, but their model,
prompt, revision, and uncertainty must be explicit.

### 4. Emit side-effect-free shadow receipts

Record task-state digest, catalog digest, progress-contract revision, causal
latency coverage, policy revision, eligible candidates, hard-gate rejections,
ranked candidates, abstention, estimated disclosure, and reason codes. Shadow
preparation must be idempotent and must not increment serving usage or mutate
learning state.

### 5. Join decisions to real outcomes

Bind current serving and shadow decisions to the exact virtual-session manifest
and actual delivered revisions. Join deterministic verifier results, effects,
provider delivery when relevant, terminal outcome, correction, latency, token,
retry, and evaluator cost. Missing evidence remains `unknown`.

### 6. Freeze and run the comparison

Predeclare the corpus, labels, metrics, exclusion rules, and decision threshold.
Compare current policy, shadow policy, and no-skill cases where meaningful.
Report outcome coverage, abstention, forbidden selection, disclosure cost,
latency, and uncertainty rather than a single aggregate score.

## Acceptance Plan

| Gate | Required evidence | Minimum level |
|---|---|---|
| No serving effects | Current selected revisions, prompt bytes, usage events, provider output, and learning targets are byte-equivalent with shadow disabled and enabled | T3 differential replay |
| Exact identity | Cross-agent, account, channel, user, session, and virtual-session cases cannot join or borrow evidence across lanes | T3 replay |
| Idempotency | Repeated preparation creates one logical shadow decision and no duplicate outcome credit | T2/T3 |
| Attribution | Every measured outcome names exact delivered revisions or explicitly records no delivered skill; missing evidence stays unknown | T3 |
| Privacy | Receipts contain bounded digests and reason codes, not raw private task or provider payloads | T1 plus hygiene scan |
| Comparative value | Frozen reviewed cases improve at least one declared outcome, abstention, or cost dimension without increasing forbidden selections or material outcome regressions | T3 replay |
| Operational bounds | Shadow latency, storage growth, and evaluator cost remain within predeclared budgets | T2 plus measured run |

The phase fails if the comparison cannot distinguish unknown from success, if
shadow mode affects serving, or if an apparent gain depends on post-result
metric changes.

## Acceptance Artifacts

- bounded task-state and shadow-receipt schemas;
- shadow policy revision and deterministic replay fixture;
- exact routing-delivery-outcome join;
- privacy and retention specification;
- frozen comparison protocol;
- current-versus-shadow comparison report with uncertainty and exclusions;
- phase acceptance record.

## Public Explanation at Acceptance

Public text must describe which alternative routing behavior was observed,
which outcome or cost comparison was executed, and what the comparison showed.
It must state that shadow decisions did not control selected skills, prompt
bytes, learning targets, or provider output. "Shadow-routing phase complete"
alone would overstate the result.

## Transition from the Reproducible Skill Contract

Every receipt must bind the accepted reproducible catalog digest, skill
revisions, progress-contract revision, and causal-latency schema. A changed
skill card, corpus label, catalog, progress surface, or timing schema requires a
new comparison revision; old and new results must not be pooled silently.

## Transition to a Reversible Serving Cohort

The serving-cohort work selects one cohort and low-risk skill set from the
accepted comparison, not from intuition or overall selection frequency. Freeze
the shadow policy, catalog digest, cohort identity, expected benefit, cost
budget, fallback policy, and monitoring signals before serving activation.

Shadow collection continues during cohort activation so serving and non-serving
evidence remain comparable, but only one policy owns the prompt.

## Stop and Rollback Conditions

Stop if shadow mode changes serving state, crosses identity boundaries, stores
unbounded private data, creates unjoinable evidence, or exceeds its resource
budget. Rollback is disabling shadow execution and retaining already collected
version-bound receipts for diagnosis; no serving migration should be needed.
