# Migrating from 0.7 to 0.8.0

This guide describes the public v0.8.0 compatibility boundary. It does not describe any particular deployment.

## Two Different Compatibility Questions

Rust source compatibility and persisted-artifact compatibility are separate:

- **0.7 source to 0.8.0 source:** downstream Rust code may require edits and recompilation. This is a pre-1.0 minor-version upgrade, so additive fields on public structs and changed behavioral contracts are source-breaking for exhaustive callers.
- **Legacy artifacts read by 0.8.0:** the v0.8.0 reader keeps bounded compatibility paths for specific older queue, lease, owner, plan, and ledger artifacts. This lets a copied deployment move forward without silently treating incomplete identity as authoritative.

The second statement is one-way. An old 0.7 binary cannot be assumed to read any V2 or other state written by 0.8.0.

## Rust Source Changes

Review downstream code that constructs public structs with literals or exhaustively interprets runtime state:

- `PromptAssemblyOptions` now carries an exact full-lane key and backend-context generation. Prefer `PromptAssemblyOptions::default()` plus explicit overrides instead of an exhaustive literal.
- Runtime queue items and prepared items may carry immutable `admissionQueueId` and `authorizedExecutionMode` snapshots. Use the existing v1 enqueue API for legacy admission or the explicit v2 API when the complete snapshot is available; do not manufacture partial v2 state.
- Child dispatch should construct one immutable `ChildExecutionPolicyV1` per child. `ChildExecutionPolicyV2` is an additive wrapper for a separately authorized execution-mode snapshot; ordinary reasoning integrations should leave that snapshot absent.
- Automatic coordinator resume now requires `ExactWorkerResultOwnerV1`. A legacy/incomplete owner can be inspected, but code must not claim it for automatic continuation.
- Child terminal output is no longer a user-final surface. Integrations that previously forwarded child text directly must append a bounded result envelope and let the master continuation synthesize the final reply.

Compiler errors from missing public-struct fields should be fixed explicitly. Do not fill new identity or authorization fields with guessed values just to compile; use a legacy API where supported or derive the exact identity at the owning boundary.

## Model and Reasoning Migration

v0.8.0 replaces two potentially divergent chat controls with one authoritative state:

| 0.7 input or assumption | v0.8.0 behavior |
|---|---|
| `/think high` and `/reasoning low` are separate controls | They are aliases; the last valid write in the same scope wins. |
| A global effort list applies to every model | Effort is authorized against the exact provider/model capability route. |
| `max` may be normalized to another level | Exact `max` remains exact and is the highest currently known legal GPT-5.6 reasoning effort. |
| Exact `ultra` is a reasoning level | Filtered from cached and app-server capability observations, then rejected by every public effort surface; it is not advertised or configurable. |
| `ultra-high` denotes a level above `xhigh` | It remains only a legacy alias for `xhigh`; aliases canonicalize before capability-list duplicate handling. |

Enable `orchestration.features.modelCatalogV2` first in `shadow` mode when comparing a legacy deployment, then use `authoritative` only for the intended `enabledAgentIds` cohort. A route such as `openai/gpt-5.6-sol` receives `max` only when the current catalog for that exact route advertises it.

Future effort names are not capped by a hard-coded enum, but they are legal only when the exact effective route advertises them. Do not copy an `ultra` effort or resource block into v0.8.0 configuration; no such public configuration exists.

## Per-Agent Prompt Migration

The canonical per-agent static prompt inventory is:

`AGENTS.md`, `SOUL.md`, `TOOLS.md`, `USER.md`, `IDENTITY.md`, `HEARTBEAT.md`, `BOOTSTRAP.md`, and `MEMORY.md`.

Legacy `AGENT.md` and `BOOT.md` remain fallback aliases. If both alias and canonical file exist, remove the ambiguity after reviewing content: the canonical file wins and the alias is ignored with a warning.

For non-main agents, place files in the configured isolated workspace or `agents/<agent-id>/workspace`. Do not rely on a non-main agent inheriting the main workspace, even when its directory is missing. Agent ids used for worker session storage must be a single safe path component; migrate ids containing path separators, a drive prefix/colon, `.` / `..`, or control characters before dispatching worker turns. On first v0.8.0 assembly, inspect the per-agent `agent-harness.agent-prompt-manifest.v1` rather than assuming every old file was injected.

Dynamic memory recall remains a separate per-turn context source; recalled records are not added to the eight-file static manifest.

The manifest is exact-lane and backend-generation scoped. Expected migration effects include:

- first observation records an added/included entry;
- unchanged content in the same exact lane and backend generation is reused;
- edits are reinjected;
- deletion creates a tombstone;
- `/new`, another agent/lane, or a changed Codex thread generation does not reuse the old entry;
- an exact-lane OperationPlan fails closed on a different lane, while legacy plans remain readable through their legacy compatibility path.

## Persisted Artifact Reads

The v0.8.0 reader deliberately handles these older states conservatively:

- `agent-harness.runtime-queue-item.v1` remains readable; v2-only snapshots require their immutable admission identity.
- Legacy root runtime lease state remains visible during capacity and reconciliation so an upgrade does not ignore active work.
- Legacy incomplete worker-result owners remain queryable for audit but are excluded from auto-resume.
- Worker terminal reconciliation accepts only the known typed run-once receipt schema with a queue id, runtime class, and origin matching the route derived from `WorkerJobKind`; payload route overrides and unknown schemas do not terminalize a job.
- An expected child with missing or invalid exact-owner terminal evidence is represented by a deterministic failed-omission envelope before coordinator resume; it is not silently omitted.
- Prior prompt/skill injection ledger formats covered by the schema registry remain readable for migration; new per-agent prompt state uses the exact-lane manifest contract.
- Legacy OperationPlans remain readable, but they are not silently treated as an exact-lane authorization.

Unknown, malformed, or partially upgraded state fails closed. Compatibility does not mean missing exact identity will be inferred from a nearby agent, channel, or session.

## Recommended Upgrade Sequence

1. Stop new admissions and wait for, cancel, or explicitly record outstanding work.
2. Back up the complete 0.7 harness home and keep it immutable.
3. Upgrade and compile downstream Rust callers, addressing source changes without guessed authorization fields.
4. Run config validation with `modelCatalogV2` set to `shadow` for a bounded agent cohort; remove exact `ultra` from migrated reasoning state or configuration.
5. Start 0.8.0 against a copy of the harness home, not the only rollback copy.
6. Inspect schema/invariant catalogs and run the three v0.8.0 scenario selectors:
   - `gpt56-reasoning-capability`
   - `per-agent-prompt-manifest`
   - `master-owned-subagent-coordination`
7. Promote to authoritative capability mode and perform an intentional cutover only after the required candidate and provider-visible gates pass.

## Rollback Boundary

There is no bidirectional in-place rollback guarantee. After 0.8.0 writes V2 queue items, exact-owner mailbox rows, prompt manifests, coordinator waits, or resume intents, do not point a 0.7 binary at that mutated home.

A safe rollback means stopping 0.8.0, preserving its evidence for diagnosis, restoring the untouched pre-upgrade 0.7 snapshot, and then starting the 0.7 binary against that restored snapshot. Binary rollback without state rollback is unsupported unless a future migration document explicitly adds that guarantee.
