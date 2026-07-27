# Phase 0 — Reliable Third-Party MCP Hosting

- **Status:** accepted, live-observed on Windows, and published as v0.13.1; the
  live promotion used the accepted implementation before release metadata and
  the tag were finalized
- **Depends on:** the v0.13.0 backend-home and generated-config foundation
- **Unlocks:** reproducible skill and tool eligibility through
  [Reproducible Skill Contract](phase-1-reproducible-skill-contract.md)
- **Acceptance evidence:**
  [Reliable Third-Party MCP Hosting Acceptance](phase-0-acceptance.md)

## Implementation Progress

Updated: 2026-07-27

Initial discovery reproduced whole-file config regeneration that could discard
extension-owned `mcp_servers`, partial materialization, false readiness inferred
from config presence, a false-Goal terminal classification, opaque multi-minute
preparation, long-worker generation loss, and incomplete backup progress
authority.

The accepted repair uses semantic TOML ownership and same-directory
transactional replacement, preserves supported extension and unknown roots,
fails closed on malformed or ambiguous materialization, retains exact recovery
inputs, and emits redacted generated-versus-reused provenance. Non-main agents
receive isolated agent/provider backend homes while the main agent retains its
compatibility paths.

Generic required and optional `stdio` declarations bind canonical tool-schema
and optional server-identity digests. Execution revalidates materialized config
before spawn and queries bounded MCP status pages on the same initialized
official Codex 0.145.0 app-server connection before `turn/start`. Required
capabilities fail closed; optional capabilities warn without claiming
readiness. Real fresh-process replay reached the expected generic server and
tool digest before any turn.

The coherent candidate also carries the ordinary null-Goal classification
repair, bounded preparation and progress receipts, renewable worker-generation
authority, complete descendant containment, linked-cron settlement, and staged
backup progress contracts. Fresh Windows and focused native-Linux verification,
one independent hash-bound review, a guarded Windows cutover, and live readback
passed. The complete native-Linux suite and native macOS execution remain
explicitly limited in the [acceptance record](phase-0-acceptance.md).

### Incident-derived Phase 0.1 closure

The Phase 0.1 label was introduced during live closure and is not an independent
roadmap phase. It repaired gaps exposed only after the first Phase 0 promotion:

- complete-store pre-cutover admission covers CronRuns as well as queue,
  runtime, worker, and outbox state;
- generation-fenced prepare/apply transactions prevent stale or partial
  supervisor mutations;
- live gateway control supports explicit interactive authorization without
  making ordinary start, stop, or restart commands impossible to use;
- Windows launcher and process ownership remain background-safe and tolerate
  supported Scheduled Task action shapes;
- Discord Gateway identification publishes explicit online presence and the
  final live gate waits for post-baseline open, identify, ready, and heartbeat
  acknowledgement evidence;
- latency receipts remain causal and exact-lane across Discord and Telegram.

The repaired candidate passed scenario replay, cross-platform source checks,
independent review, guarded promotion, and T4 readback. The provider-visible
Discord indicator was confirmed online, and one natural operator-originated
turn on Discord and one on Telegram reached delivery without synthetic
messages. Remaining progress-surface, duplicate-terminal, complete-latency, and
false-restart optimization belongs to the
[Phase 1 reliability foundation](phase-1-progress-ux-and-latency-foundation.md).

### Included latency and progress hotfix

Phase 0 also tracks a second cross-cutting hotfix derived from a read-only
Discord incident record. An interactive turn that already held its lease could
remain labeled `queued` while memory-backed prompt preparation consumed
minutes, and the default `/status` path paid runtime skill-discovery and
planning cost before reaching its already-bounded exact-lane renderer.

The repair moves active preparation to the `preparing` lifecycle immediately,
records monotonic stage durations, bounds the total interactive memory-recall
preparation path with explicit degraded-memory receipts, and serves default
status from a bounded exact-lane/current snapshot. Detailed global aggregation
remains an explicit operator scope. The change preserves exact lane/session
identity and is not replaced by higher concurrency.

T3 bridge-timeout and slow-fallback replay, fresh-session progress semantics,
bounded status response, parallel unrelated-lane control, and receipt timing
assertions passed. Live Discord and Telegram observation followed the
independent review and guarded cutover.

The read-only historical replay is bound to the completed Discord session
immediately before a later `/new` transition. It measured 14.728 seconds from
queue acceptance to lease acquisition, 487.765 seconds from lease acquisition
to prompt-bundle write, and 834.026 seconds from queue acceptance to provider
delivery. The deployed journal had no `preparing` event, and its prompt warning
proved a 15-second memory-bridge deadline followed by the read-only
SQLite/vector/text fallback. Because that deployed build had no fallback
substage timings, the remaining delay is attributed only to the opaque fallback
interval, not to an invented individual root cause.

The source repair now parses channel commands before runtime skill discovery,
so default `/status` retains the bounded exact-lane snapshot without paying
runtime skill-index construction. Existing source already emits `preparing`
immediately after durable lease acquisition; a deterministic post-lease
failpoint regression now protects that ordering. Interactive prompt recall uses
one monotonic configurable total budget, suppresses a second full timeout retry,
uses bounded local text fallback, and returns a typed `degraded` prompt-memory
receipt while continuing without memory context when the budget expires.
Prompt-bundle start and completion are part of the durable latency timeline.
Focused and frozen-candidate verification passed before independent review.

A later read-only Telegram observation reproduced the same primary signature on
the old deployed build. Queue-to-lease took 28.153 seconds, while
lease-to-prompt took 914.663 seconds after the same 15-second bridge deadline
and opaque local fallback. The turn then entered an active provider execution
with fresh working events and no terminal output available to deliver. This
separates two claims: the platform-neutral preparation repair directly covers
the proven 914.663-second failure mode, while a still-productive provider turn
is not evidence of a delivery failure and is not shortened by the memory
budget. The new source tests already use a Telegram exact-lane queue fixture for
immediate `preparing`, and the shared memory-budget replay remains
platform-neutral. Post-cutover natural turns showed that the old multi-minute
preparation signature was removed, while also exposing additional provider UX
and latency work now assigned to Phase 1A. The old-build incident remains
fail-first evidence rather than self-attested source proof.

### Included long-running worker and backup robustness hotfix

A scheduled encrypted backup exposed a shared worker-generation defect rather
than storage exhaustion or an archive-format failure. The worker wrapper could
stop publishing lifecycle heartbeats while synchronously executing a long
deterministic job, the transition to `running` discarded its exact lease, and a
replacement wrapper could not safely distinguish the old generation or reject
its late result. Descendants could also outlive an abruptly terminated wrapper,
and operator cancellation did not settle the linked cron run.

The source repair keeps and renews the exact worker-id and attempt lease while a
job is running, reaps expired generations before leasing new work, rejects late
results from a superseded generation, continues wrapper heartbeats during
synchronous execution, and settles linked cron state on stale recovery or
cancellation. Deterministic shell jobs use a kill-on-owner-exit Job Object on
Windows and a dedicated process group on Unix so timeout or abrupt owner loss
contains the complete descendant tree.

The incident also proved that a generic one-hour deadline was shorter than the
normal create interval for the observed backup. The operator contract therefore
uses a four-hour absolute guard for that backup alone and writes atomic
create/verify progress receipts plus typed failure evidence. This is not a
claim that a complete multi-gigabyte backup has been rerun during release
verification; long-job lifecycle, descendant containment, staged success, and
empty-exception regressions are the bounded acceptance evidence.

### Included external-consumer MCP gap

Two read-only consumer incident notes exposed a boundary that is broader than
config preservation alone. A server may remain registered across restart yet
still be unusable because the current backend is not authenticated, a
different server version is mounted, or the consumer expects a separate
identity and task/inbox policy.

Phase 0 covers the generic hosting portion:

- operator-seeded `mcp_servers` registrations survive generated-config writes;
- every execution revalidates the materialized config before process spawn;
- a fresh process queries bounded `mcpServerStatus/list` pages on the same
  initialized connection before `turn/start`;
- required declarations fail closed when authentication is required, the
  optional expected server identity/version digest differs, or the expected
  tool-schema digest differs;
- optional declarations produce bounded warnings without claiming readiness;
- receipts bind the exact agent, provider home, config digest, private spawn
  generation, process, thread, and observed capability digests.

Phase 0 deliberately does not perform a first-time mount, credential login,
consumer skill upgrade, consumer identity selection, task/inbox access,
acknowledgement, or provider-visible call. Those are stopped-state operator and
consumer-integration actions. A named-consumer replay therefore remains a
separately authorized T3/T4 activity after this generic candidate is accepted;
it cannot be inferred from the generic mock or from config presence.

## Outcome

An operator or plugin can register a third-party MCP server once and rely on
that registration surviving harness-managed config regeneration. A newly
started backend process receives the declared server capabilities in the
correct agent and provider home, while failures remain typed, redacted,
fail-closed, and exactly recoverable.

## Purpose in the Whole Project

Skill routing and learning depend on truthful tool availability. If the host
silently removes a third-party registration, or treats a cached plan as proof
that a fresh backend has tools, later outcome attribution will misclassify a
host failure as a bad skill or bad routing decision.

This hot patch establishes the external-capability substrate before the skill
catalog begins making tool-eligibility decisions. It is independently valuable:
ordinary backend sessions can use generic third-party MCP servers reliably even
if no adaptive skill phase is ever enabled.

The ownership boundary remains narrow. The backend owns MCP protocol behavior,
tool schemas, tool execution, approvals, and native session semantics. Agent
Harness Core owns the backend-home config it materializes, preservation of
declared extension-owned registrations, plan provenance, home isolation, and
readiness evidence required before admitting work.

## Starting Point

The v0.13.0 source rebuilds a harness-generated backend config from
harness-owned fields. That whole-file regeneration can remove an
operator-managed `mcp_servers` table. A successful registration command,
config-file inspection, or reused plan receipt does not prove that a newly
started backend process actually exposes the server's tools.

A lexical-preservation prototype and focused tests provide implementation
evidence, but that approach has not completed independent review, full
verification, fresh-backend acceptance, release, or live promotion. The hot
patch must accept or replace it based on the gates below rather than treating
the prototype as accepted behavior.

## Scope

### In scope for v0.13.1

- an explicit ownership contract for harness-managed config and the
  extension-managed `mcp_servers` namespace;
- semantic, deterministic merge that is independent of server names;
- explicit fail-closed behavior for malformed, duplicate, unsupported, or
  ownership-ambiguous TOML;
- same-directory transactional materialization with original-file recovery;
- private permissions and ACL preservation or tightening;
- redacted generated-versus-reused plan provenance;
- a generic declaration for required and optional external MCP capabilities;
- actual fresh-backend readiness based on initialized server and tool-schema
  evidence rather than file or cache presence;
- authentication readiness plus an optional server identity/version digest,
  evaluated before turn admission;
- agent and provider-home isolation;
- a generic mock MCP scenario covering regeneration, fresh process startup,
  host restart, failure, and rollback;
- public configuration, trust-boundary, invariant, schema, topology, changelog,
  and release-context updates;
- bounded current-session status, prompt-preparation stage receipts, interactive
  memory-recall degradation, and prompt lifecycle correction from the included
  Discord and Telegram latency incidents;
- renewable exact worker-generation leases, synchronous wrapper heartbeats,
  stale-result fencing, cross-platform deterministic-shell descendant
  containment, linked-cron settlement, and staged backup progress receipts.

### Explicitly outside the hot patch

- server-specific production code or hard-coded tool inventories;
- third-party application delivery, inbox, task, or acknowledgment semantics;
- a native MCP control surface implemented by Agent Harness Core;
- plugin discovery, marketplace, or installation UX;
- broad arbitrary-config editing beyond the declared ownership contract;
- every remote MCP transport or authentication scheme without an explicit
  supported matrix;
- Linux or macOS parity claims not backed by executed platform evidence;
- long-duration multi-server scaling beyond bounded acceptance and rollback.

## v0.13.1 Hot-Patch Boundary

The release is one coherent compatibility and reliability repair:

1. preserve declared third-party MCP registrations during generated-config
   updates;
2. prevent partial or ambiguous rewrites;
3. distinguish regenerated state from reused plan state;
4. prove capabilities in a fresh backend process;
5. prevent agent, provider-home, secret, and permission leakage;
6. restore the original config and runtime behavior exactly when acceptance
   fails.

Items outside that chain remain later work. They do not need to be implemented
to make the registration and fresh-backend contract trustworthy.

## Implementation Plan

### 1. Freeze config ownership and failure semantics

Document every harness-managed top-level key and table. Declare
`mcp_servers` extension-owned for this release. Unknown top-level data must
follow an explicit policy; automatic regeneration must not silently guess
ownership or discard data.

Malformed or duplicate TOML leaves the original file unchanged and returns a
typed diagnostic. Unsupported syntax must be named in the public compatibility
contract rather than partially preserved.

### 2. Choose semantic merge over scanner growth when practical

Run a bounded `toml_edit` spike against the accepted config corpus. It must
preserve supported quoted keys, nested tables, comments or formatting where
promised, multiline strings, Unicode, extension values, and deterministic
output without exposing secret-bearing content.

The repository does not currently carry a TOML document dependency, so the
spike must report dependency, build, round-trip, and maintenance impact. Prefer
the document/AST merge if it passes. Retain the lexical approach only if the
spike fails a concrete release constraint; in that case freeze a supported
syntax subset and add property, fuzz, and fail-closed tests before promotion.

This is an implementation decision inside the hot patch, not a separate
architecture phase.

### 3. Materialize config transactionally

Read the existing file and security metadata, parse and classify ownership,
build the desired document, validate it, and write a same-directory temporary
file. Flush the new bytes, preserve or tighten file permissions, atomically
replace the target, and verify the final digest. Interruption or validation
failure retains the original config and produces a typed, content-free receipt.

A preserved MCP registration may contain secret-bearing environment values.
Generated-file headers, logs, warnings, diffs, receipts, and backups must not
claim the file is secret-free or expose those values.

### 4. Make plan provenance observable

Expose a bounded result equivalent to:

```json
{
  "planSource": "generated | reused",
  "configEvaluated": true,
  "configChanged": false,
  "configHashBefore": "sha256:...",
  "configHashAfter": "sha256:...",
  "backendHomeClass": "default | provider",
  "ownershipSummary": {
    "managedRootsUpdated": 0,
    "extensionRootsPreserved": 1
  },
  "warnings": []
}
```

Do not expose raw config, environment values, credentials, unnecessary local
paths, or profile content. A reused plan cannot satisfy a regeneration gate.

### 5. Add a generic external-capability declaration

Allow an integration to declare a server identifier, supported transport,
expected capability or tool-schema digest, and whether the dependency is
required or optional. The declaration belongs to the integration boundary; the
core must not know a consumer's product name or hard-code its tools.

Readiness progresses through registration source, merged config, MCP
initialization, accepted server identity, and actual fresh-backend tool schema.
A required dependency that fails any layer blocks turn admission with a typed
error. An optional dependency produces a bounded warning and continues without
claiming the capability.

### 6. Enforce agent and provider-home isolation

Default and provider-specific backend homes retain only their own declared
registrations. Agent or profile boundaries cannot borrow MCP config, connection
profiles, readiness, or capability evidence. Existing files retain or improve
their permissions; newly created files and rollback artifacts begin private.

### 7. Prove the generic fresh-backend lifecycle

Use a generic mock MCP server, not a product-specific production dependency or
fixture name:

```text
register third-party server
  -> initialize and record expected capability digest
  -> generate config
  -> generate again and prove byte idempotency
  -> close the old backend
  -> start a fresh backend and verify actual tool schema
  -> restart the host and repeat readiness
  -> restore the original config and verify fallback
```

Run the scenario for default and provider-specific homes. Add negative cases
for cached plans, malformed and duplicate config, quoted and multiline TOML,
secret-bearing environment values, required and optional absence, cross-home
isolation, interrupted writes, and rollback.

A real third-party consumer may run afterward as a compatibility smoke, but it
is not the production dependency or sole acceptance fixture.

## Acceptance Plan

| Gate | Required evidence | Minimum level |
|---|---|---|
| Ownership | Harness-managed roots refresh while generic `mcp_servers` registrations survive exactly; unknown ownership never disappears silently | T2/T3 |
| Syntax safety | Supported quoted, nested, whitespace, multiline, Unicode, comment, malformed, duplicate, and unsupported cases follow the published policy | T2 plus property/fuzz evidence |
| Idempotency | Two uncached generations produce byte-identical config and stable ownership summary | T3 |
| Transaction and recovery | Parse, validation, write, flush, replace, and post-write failpoints preserve or exactly restore the original file | T3 interruption replay |
| Plan provenance | Generated and reused plans are distinguishable; a cached receipt cannot satisfy config-regeneration acceptance | T2/T3 |
| Fresh-backend readiness | A newly started backend, not the registration CLI or config file, is authenticated as required and exposes the expected generic server identity/version and capability digests | T3 real-process scenario |
| Required and optional behavior | Missing required capability blocks before turn admission; missing optional capability warns without false readiness | T3 |
| Home isolation | Two agents across default and provider-specific homes see only their declared mock servers and capability evidence | T3 |
| Secret and permission safety | Config, receipt, warning, backup, and rollback scans reveal no secret values; file ACL or mode is preserved or tightened | T2/T3 on each supported platform |
| Regression breadth | Focused tests, full workspace checks, and realistic backend restart scenarios pass on the frozen candidate | T3 |
| Long-running operations | A job outlives the wrapper heartbeat interval without false restart; abrupt owner loss reaps descendants; stale results cannot overwrite a successor; cancellation settles linked cron state | T3 |
| Backup observability | The operation-specific deadline exceeds the measured normal interval, and staged create, verify, terminal success, and typed empty-exception paths produce atomic receipts | T2/T3 |
| Independent judgment | One independent review of the stabilized candidate finds no unresolved release-blocking ownership, parser, secret, or rollback defect | Release gate |
| Public contract | Documentation names the actual behavior, supported homes and platforms, executed evidence, and remaining transport or live-deployment gaps | T0/T1 |

The source hot patch is ready to tag only when the frozen candidate satisfies
the required T3 gates. Exact-tag deployment and live ordinary-backend proof are
separate claims and require their own authorized cutover and readback.

## Acceptance Artifacts

- config ownership and unsupported-syntax contract;
- parser or document-merge decision record;
- transactional materialization and recovery receipts;
- generated-versus-reused plan schema;
- generic external-capability declaration;
- generic mock MCP fixtures and expected capability digest;
- default/provider and cross-agent isolation matrix;
- secret and permission scan;
- full frozen-candidate verification report;
- one independent review result;
- rollback bundle and exact restoration readback;
- public documentation and v0.13.1 release notes;
- phase acceptance record as defined by the [phase-plan index](README.md).

## Public Explanation at Acceptance

Public text must say that harness-generated backend config now preserves generic
third-party MCP registrations, that a fresh backend process was verified to
receive the declared capabilities, and which homes, transports, platforms, and
restart cases were exercised. It must also state whether the exact release tag
was deployed.

Do not publish a consumer-specific internal verification name, a bare
"integration upgrade," or a claim that every MCP server or transport is
supported.

## Acceptance and Live Closure (2026-07-27)

The coherent source candidate implements semantic TOML ownership and
transactional restoration, exact Codex 0.145.0 provenance and app-server
compatibility, same-connection external-capability admission, agent/provider
home isolation, ordinary-note false-Goal classification, bounded interactive
preparation, renewable worker authority, process-tree containment, and
transaction-safe backup and supervisor control.

Fresh executed evidence includes:

- Windows formatting and full workspace compilation;
- a serialized Windows core run with 1,553 passed, 0 failed, and one
  intentionally ignored subprocess-helper test out of 1,554 discovered tests;
- a serialized Windows CLI run with 170 passed and 0 failed, including all 15
  standalone-master lifecycle scenarios;
- native Linux full-workspace compilation from a read-only source mount;
- focused native Linux behavior and worker packs covering Unix containment,
  path and permission boundaries, bounded recall, required and optional
  fresh-backend capability admission, renewable authority, contention,
  chronology, late-result fencing, linked-cron reconciliation, and
  cancellation;
- Windows real-process Codex 0.145.0 restart and changed-schema probes;
- incident-derived Phase 0.1 replay for complete-store admission,
  generation-fenced control, Discord presence, causal latency, and
  cross-channel delivery;
- one independent review of the frozen manifest and deployment bundle with no
  release-blocking findings;
- guarded cutover and live readback of one direct-owner generation, nine healthy
  services, no active CronRuns from a complete store scan, empty Discord and
  Telegram outboxes, fresh Discord Gateway readiness, and natural delivery on
  both channels.

The accepted live executable is bound to SHA-256
`09ff6bf26bc1a4e591d690dd56b451c138bfedd342bf68d0093dbd7db2431643`.
The acceptance record keeps the review and rollback provenance separate from
this public plan.

The complete native Linux core run remains a fail-first portability baseline:
1,417 passed and 47 failed out of 1,464 executed. Current Phase 0 Linux
regressions pass, but historical Windows-specific, writable-source,
Python-assuming, and wall-clock fixtures keep that complete suite non-green.
Native macOS execution has not been performed. No first-time named-consumer
mount or consumer identity action is claimed. The v0.13.1 release publishes the
accepted implementation after the hash-bound live promotion; it does not claim
that the live process was launched from the later tag or exact release binary.

## Transition to the Reproducible Skill Contract

The skill-card and catalog work may rely on external tools only after this phase
defines truthful capability readiness. Tool eligibility must bind the accepted
server or capability digest and the exact agent/provider home. Unknown or stale
readiness causes routing to abstain; it cannot be inferred from config presence.

Changing the ownership, readiness, or capability-digest contract after skill
baseline collection requires a new catalog and replay revision.

## Follow-Up After v0.13.1

Move these items forward only when a supported integration needs them:

- additional remote transports and authentication schemes;
- broader extension-owned config namespaces;
- plugin installation and discovery UX;
- Linux and macOS native-package, permission, and fresh-backend executed parity
  beyond the required portable source/compile, Unix process-group containment,
  and contract tests;
- multi-server endurance, backpressure, and startup-latency optimization;
- richer capability negotiation beyond the minimum skill-eligibility digest.

## Stop and Rollback Conditions

Stop promotion on config truncation, ownership ambiguity, nondeterministic
round-trip, weaker permissions, secret exposure, cross-home registration,
generated/reused misclassification, fresh-backend capability mismatch,
unrecoverable partial write, or uncertain rollback.

Keep the original config and v0.13.0 serving behavior authoritative, quarantine
the candidate, preserve bounded evidence, and repair the coherent hot-patch
batch before freezing a new candidate.
