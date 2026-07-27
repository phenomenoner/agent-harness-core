# Phase 1A — Progress UX and Latency Foundation

- **Status:** planned; design and implementation contracts are defined, but
  implementation has not started
- **Depends on:** accepted
  [Reliable Third-Party MCP Hosting](phase-0-reliable-third-party-mcp-hosting.md)
  and its incident-derived live closure
- **Runs before or alongside:** non-serving
  [Reproducible Skill Contract](phase-1-reproducible-skill-contract.md) work
- **Blocks:** serving experiments that would add user-visible work without
  preserving the progress, terminal-authority, and causal-latency contracts

## Outcome

Users can tell promptly that an accepted turn is making progress, see bounded
updates when its state meaningfully changes, and receive one authoritative
terminal result without duplicated tool or terminal blocks. Operators can
attribute end-to-end delay to a complete causal stage timeline and distinguish
productive execution from queueing, preparation, delivery, or supervisor
contention.

## Why This Is Part of the Skill Roadmap

Later phases add catalogs, shadow decisions, attribution, and potentially more
prompt or tool work. Those changes are not supportable if a user cannot tell
whether a turn is alive or if latency measurements omit the stages where time
is spent. This workstream therefore establishes the experience and measurement
foundation before adaptive behavior expands.

The workstream does not change the Phase 1B outcome. Reproducible skill identity
and catalogs remain the primary semantic foundation for routing. Progress UX
and latency evidence make that work observable and diagnosable.

## Starting Point and Baseline

Phase 0 moved an accepted interactive turn to `preparing` immediately after
lease acquisition, bounded memory preparation under one monotonic budget, and
added prompt-stage timing. The incident-derived closure also restored explicit
Discord online presence and collected one natural Discord turn and one natural
Telegram turn without synthetic traffic.

Those two samples measured:

| Span | Discord | Telegram |
|---|---:|---:|
| Inbound acceptance to provider delivery | 140.727 s | 95.251 s |
| Accepted queue to prompt completion | 32.973 s | 18.841 s |
| Model execution | 63.404 s | 39.681 s |
| First provider-visible progress surface | 99.393 s | 80.031 s |
| Outbox enqueue to provider delivery | 0.537 s | 1.346 s |

The samples establish a baseline, not a controlled architectural comparison.
They show measurable pre-execution and execution cost, but incomplete stage
coverage prevents attributing the full totals to one component or to the new
architecture.

The Telegram sample also exposed a duplicate body surface: a terminal body edit
returned provider-not-found, then fallback delivery created a second body after
another terminal surface was already authoritative.

## User Experience Contract

### Prompt acknowledgement

An accepted interactive turn publishes its first provider-visible
acknowledgement without waiting for skill discovery, memory recall, model
startup, or tool planning. The acknowledgement is exact-lane and does not imply
that execution has completed.

### Meaningful progress

Meaningful lifecycle changes update the provider surface as soon as practical:
queued, preparing, running, waiting on a named bounded dependency, delivering,
and terminal. Repeated heartbeats or internal events are coalesced. If no
meaningful transition occurs, a bounded alive indication prevents silent
multi-minute gaps without turning heartbeats into chat noise.

### Stable surface ownership

Each logical turn has at most one status surface and one result-body surface per
provider. Edits update those surfaces when the provider supports them. A
provider-not-found edit may fall back to a new send only while no later
authoritative terminal surface exists.

Once final or `Done` authority is durable, a late terminal edit fallback is
discarded and recorded rather than creating a duplicate tool or terminal block.
Non-terminal fallback remains available so a genuinely missing progress surface
can still recover.

### Exact continuity

Progress, edit, fallback, discard, and final-delivery decisions remain bound to
the exact agent, platform, account, channel, user, session, virtual session, and
logical result. A receipt from another lane cannot suppress or authorize a
surface.

## Latency and Supervisor Contract

Every interactive turn records one monotonic causal timeline spanning at least:

1. provider inbound receipt and durable acceptance;
2. queue wait and lease acquisition;
3. skill discovery or explicit bypass;
4. memory bridge, local fallback, and degraded continuation;
5. prompt materialization;
6. backend process or thread readiness;
7. model execution and tool intervals;
8. result projection;
9. outbox enqueue, provider attempt, and confirmed delivery;
10. first acknowledgement, first meaningful update, and terminal surface.

Missing timestamps are explicit coverage gaps. Stage durations cannot be
invented from wall-clock subtraction across unrelated clocks.

Supervisor restart decisions use durable cause evidence. A maintenance loop
that briefly crosses its heartbeat threshold under host contention must not be
called dead from one ambiguous sample. The repair must distinguish a genuinely
lost owner from delayed observation, preserve bounded recovery, and record the
restart cause and evidence used.

## Implementation Plan

### 1. Freeze surface and terminal authority

Define the provider-neutral status and body surface identities, authoritative
terminal events, edit/fallback precedence, deduplication key, and bounded alive
cadence before changing adapters.

### 2. Add fail-first provider replays

Reproduce delayed first acknowledgement, silent active work, Telegram terminal
edit provider-not-found duplication, out-of-order status/final races, retries,
and restart replay. Include an unrelated-lane control.

### 3. Implement provider adapters behind the shared contract

Make Discord and Telegram consume the same logical surface decisions while
retaining only provider-specific edit and send mechanics. Keep provider payload
limits and failure codes bounded and typed.

### 4. Complete causal timing coverage

Join existing queue, preparation, execution, tool, outbox, and delivery
receipts. Add first-visible-progress and stage-coverage summaries. Preserve raw
private receipts outside public artifacts.

### 5. Repair false-restart classification

Add deterministic heartbeat-threshold, delayed-observation, owner-loss, and
replacement-generation tests. Emit durable restart-cause telemetry before
changing timeout values.

### 6. Optimize only after observability passes

Use same-model, comparable-prompt, exact-lane replays to rank measured
bottlenecks. Change one bounded component at a time and retain the visible
progress contract even when total latency remains long.

## Acceptance Plan

| Gate | Required evidence | Minimum level |
|---|---|---|
| First acknowledgement | Accepted turns publish a provider-visible acknowledgement before expensive preparation on Discord and Telegram | T3 |
| Meaningful updates | State changes update promptly; bounded alive indications cover long unchanged work without flooding | T3 |
| Surface uniqueness | Each provider shows no more than one status and one body surface for a logical turn across edits, retries, restart, and replay | T3 |
| Terminal fallback | Provider-not-found after durable terminal authority is discarded; non-terminal missing-surface recovery still sends | T3 |
| Exact continuity | Cross-agent, cross-channel, and cross-session controls cannot edit, suppress, or authorize another lane | T3 |
| Causal timing | Every required stage is present or explicitly missing, ordered monotonically, and joined to the exact turn | T2/T3 |
| Restart evidence | Delayed heartbeat observation does not cause a false restart; genuine owner loss still recovers with a typed cause | T3 |
| Provider-visible proof | One natural operator-originated turn on each supported channel confirms progress, terminal uniqueness, and delivery | T4 |
| Performance comparison | Same-model and comparable-input evidence separates queue, preparation, execution, tool, and delivery changes | T3/T4 |

## Acceptance Artifacts

- versioned logical progress-surface and terminal-authority contract;
- Discord and Telegram incident-derived replay fixtures;
- exact-lane deduplication and fallback receipts;
- complete causal latency-stage schema and coverage report;
- supervisor restart-cause schema and contention replay;
- controlled before/after performance report;
- provider-visible acceptance summary;
- public contract updates and phase acceptance record.

## Explicit Deferrals

This workstream does not promise a fixed end-to-end service-level target before
baseline variance and missing stages are resolved. It does not add speculative
concurrency, warm pools, new model providers, or broad transport support merely
to make one sample faster. Those changes require their own measured case.

## Transition to the Reproducible Skill Contract

Phase 1B may build its non-serving catalog while this work proceeds, but any
provider-visible health, progress, or serving change must preserve the accepted
surface and timing contracts. Later routing comparisons must record the
progress-contract revision and causal latency coverage used for each cohort.

## Stop and Rollback Conditions

Stop promotion on duplicate terminal surfaces, delayed or missing
acknowledgement, cross-lane edits, progress flooding, invented latency
attribution, lost terminal delivery, or a weaker owner-loss recovery path.
Restore the prior adapter behavior and keep the new telemetry observational
until the exact replay passes.
