# Phase 1B — Reproducible Skill Contract

- **Status:** planned
- **Depends on:** accepted
  [Reliable Third-Party MCP Hosting](phase-0-reliable-third-party-mcp-hosting.md)
  plus the current stable skill discovery and lifecycle foundation
- **Coordinates with:** the
  [Progress UX and Latency Foundation](phase-1-progress-ux-and-latency-foundation.md);
  catalog construction may proceed independently, but provider-visible or
  serving changes must preserve its accepted progress and timing contracts
- **Unlocks:** side-effect-free routing comparison through
  [Shadow Routing and Attribution](phase-2-shadow-routing-and-attribution.md)

## Outcome

Every active skill revision and every catalog used for evaluation can be
identified, reproduced, compared, and restored. Serving behavior remains
unchanged.

## Purpose in the Whole Project

Adaptive routing and learning cannot be safer than their inputs. If skill
identity, ownership, compatibility, disclosure cost, or active revision is
ambiguous, later outcome comparisons cannot be attributed and rollback cannot
be exact.

This phase creates the stable measurement substrate for the rest of the
program. It is independently valuable because it improves supportability,
release and compatibility diagnosis, pack integrity, and catalog recovery even
if adaptive routing is never enabled.

Phase 1A separately makes waiting observable and latency attributable. Phase 1B
does not absorb provider UI mechanics into skill semantics. It records the
accepted progress-contract revision and keeps non-serving catalog work from
silently changing acknowledgement, prompt bytes, or result surfaces.

## Starting Point

The current source already has scoped discovery, hard eligibility, checksums,
selection receipts, lint, guard, lifecycle state, archive/restore, and
checksum-locked packs. It does not yet expose one canonical versioned skill card
and deterministic catalog digest that carry every field required by the later
routing and promotion phases.

The accepted third-party MCP prerequisite supplies truthful external-capability
readiness. Skill cards may bind tool requirements to that capability evidence;
they must not infer availability from a registration file or cached plan.

## Scope

### In scope

- a versioned public skill-card schema;
- stable skill ID, revision, source checksum, provenance, lifecycle, and owner;
- competence roles, triggers, hard eligibility, tools, permissions, and
  platform or backend compatibility;
- disclosure modes, bounded prompt-cost estimates, aliases, conflicts,
  supersession, and rollback target;
- evaluator and evidence references required for later promotion;
- deterministic library inventory and catalog digest;
- a non-serving rebaseline report for malformed, duplicate, conflicting, or
  unsupported entries;
- a frozen replay corpus covering positive, negative, ambiguous, explicit,
  retired, CJK, agent-isolation, and no-skill cases;
- prior-version reading or migration behavior for changed public records.

### Out of scope

- changing the active matcher or prompt bytes;
- updating usage priors from the new catalog;
- generating or modifying skill bodies;
- moving canonical skill directories;
- serving a new router;
- broad topology learning or consolidation.

## Implementation Plan

### 1. Freeze semantics before storage

Define the meaning and ownership of each skill-card field. Reuse canonical
identity, lifecycle, pack, and capability contracts rather than introducing
parallel definitions. Specify which fields are authored, derived, or
environment-specific and which participate in the revision checksum.

### 2. Add versioned readers and deterministic normalization

Implement a reader that accepts the current supported source form and emits one
normalized card. Ordering, optional defaults, path treatment, line endings, and
generated values must be deterministic. Breaking changes require a new schema
version and an explicit prior-version reader.

### 3. Build a non-serving catalog

Inventory every supported namespace without changing the active index. Record
accepted entries and fail-closed exclusions with stable reason codes. Produce a
catalog digest from normalized revision-specific cards, not from filesystem
enumeration order or local absolute paths.

### 4. Produce the rebaseline report

Classify duplicate IDs, equivalent content, conflicting eligibility, invalid
ownership, unsupported files, missing rollback targets, and cost-estimation
gaps. The report recommends later work but does not rewrite or relocate skills.

### 5. Freeze the replay corpus

Create reviewed cases that exercise hard gates before ranking. Each case names
the expected eligibility set, exclusion reasons, explicit-invocation behavior,
and whether selecting no skill is correct. Keep private task text out of public
fixtures.

### 6. Integrate compatibility and health reporting

Expose schema, inventory, digest, and exclusion health through existing doctor
and status boundaries. Update the schema registry, skill ecosystem contract,
invariants, examples, and generated public surfaces.

## Acceptance Plan

| Gate | Required evidence | Minimum level |
|---|---|---|
| Determinism | Repeated inventories of the same source yield byte-identical normalized cards and catalog digest | T2 |
| Identity and isolation | Cross-agent, explicit, retired, and no-skill cases fail or select exactly as specified | T3 replay |
| Fail-closed behavior | Malformed or ambiguous entries are excluded without blocking unrelated valid skills | T2 |
| Compatibility | Current records remain readable or migrate through a tested version boundary | T2 |
| Restoration | The pre-phase active catalog and files can be restored byte-for-byte | T3 scenario |
| Public contract | Schema registry, skill documentation, examples, and generated artifacts agree and pass hygiene checks | T0/T1 |
| Serving non-interference | Current selection receipts and prompt bytes are unchanged for the frozen corpus | T3 differential replay |
| Progress non-interference | Catalog inventory and health reporting do not delay first acknowledgement, duplicate a provider surface, or weaken causal timing coverage | T3 differential replay |

Phase acceptance requires all gates. A stable digest alone is insufficient if
identity isolation, restoration, or serving non-interference is unverified.

## Acceptance Artifacts

- versioned skill-card schema and reader;
- normalized catalog plus digest;
- inventory and rebaseline report;
- frozen replay corpus and expected results;
- differential current-serving report;
- restoration evidence;
- public contract updates;
- phase acceptance record as defined by the [phase-plan index](README.md).

## Public Explanation at Acceptance

Public text must say that the release makes skill identity, compatibility,
catalog generation, and restoration reproducible. It must also say that active
selection and prompt delivery are unchanged. A phrase such as "contract phase
complete" is not a substitute for those behavior and limitation statements.

## Transition to Shadow Routing and Attribution

The shadow-routing work may depend only on accepted skill-card fields and the
catalog digest plus the accepted progress-contract revision. It must not
reinterpret identity, ownership, eligibility, revision, status-surface, or
terminal-authority semantics. The shadow router must record the exact accepted
reproducible-contract catalog digest and progress-contract revision for every
decision.

If routing comparison needs a new competence or cost field, add it through the
reproducible-contract versioning rules before collecting evidence. Do not add
ad hoc fields directly to shadow receipts.

## Stop and Rollback Conditions

Stop promotion if normalization changes active serving, if local paths enter
digests, if two sources claim one active identity, if prior readers cannot
coexist, or if the catalog cannot be exactly restored. Roll back by disabling
the non-serving catalog export and restoring the prior schema reader; no live
serving data conversion should be required.
