# Reliable Third-Party MCP Hosting Acceptance Record

Updated: 2026-07-27

## Status

The coherent Phase 0 candidate and its incident-derived Phase 0.1 closure passed
fresh Windows and focused native-Linux verification, one independent
hash-bound review, guarded Windows cutover, and T4 live readback. The
provider-visible Discord indicator was confirmed online, and one natural
operator-originated turn on Discord and one on Telegram reached delivery
without synthetic traffic.

This evidence supports the v0.13.1 public release and the hash-bound Windows
deployment that preceded publication. The live promotion used the accepted
implementation before release metadata was finalized; it was not initiated
from the later release tag or exact release binary. This is not native macOS
proof, a green complete-Linux suite, or a first-time named-consumer MCP mount.

## Accepted Candidate Scope

The candidate:

- preserves operator-managed `mcp_servers` and supported unknown TOML roots
  while regenerating harness-owned Codex configuration;
- rejects malformed, duplicate, unsupported, ambiguous, or permission-unsafe
  materialization before partial mutation;
- isolates Codex homes by exact agent and provider;
- revalidates config before every spawn and checks required or optional MCP
  authentication, server identity/version, and tool-schema digests on the same
  initialized app-server connection before `turn/start`;
- upgrades the deployment-owned backend candidate to official Codex CLI
  `0.145.0`;
- prevents an ordinary successful `goal:null` turn from being falsely parked
  while keeping concrete durable-Goal authority conflicts fail closed;
- publishes `preparing` immediately after an interactive lease, bounds bridge
  plus local memory fallback under one monotonic total budget, records prompt
  and provider stage durations, and keeps default exact-lane status independent
  from runtime skill discovery;
- contains Codex descendants with a Windows Job Object or a Unix process group,
  and uses a private portable spawn generation for readiness without making
  macOS process-start metadata mandatory;
- keeps and renews an exact worker-id/attempt lease throughout long synchronous
  execution, pumps wrapper heartbeats, rejects superseded results, reaps
  expired generations, and settles linked cron state;
- contains deterministic-shell descendants on abrupt owner loss or timeout
  with a Windows Job Object or Unix process group;
- applies an operation-specific four-hour backup guard and atomic
  create/verify progress receipts with typed empty-exception evidence.
- treats active CronRuns as first-class pre-cutover work, scans the complete
  store, and fences prepare/apply mutations with exact generation and CAS
  authority;
- exposes usable, explicitly authorized live gateway control while keeping
  launcher mutation and process ownership guarded;
- publishes explicit online Discord presence and requires fresh post-baseline
  open, identify, ready, and heartbeat acknowledgement evidence before the
  gateway is accepted.

## Hash-Bound Artifacts

| Artifact | Digest or provenance |
|---|---|
| Accepted Windows live candidate | SHA-256 `09ff6bf26bc1a4e591d690dd56b451c138bfedd342bf68d0093dbd7db2431643` |
| Final candidate manifest | SHA-256 `10e332c2f4425e054bcc73304d5960b4210d092c0174472718280c6ad4a972c0` |
| Independent-review bundle | SHA-256 `04e3c73b39c350faa2564414f8813b9ffd1d6461de85ec16885d07c3e07b348c` |
| Independent-review record | SHA-256 `7dcb8c0ed034aa207b7d83111db8f0da7b386f1db02769db7845984a424bb74b`; decision `PASS`, blockers `0` |
| Final live recovery summary | SHA-256 `b4286508d4ece80cee79daadf0d8b0f421c662195c85774b6fcbdcfb3cb1f59a` |
| T4 readback record | SHA-256 `32591c15fbb7181e1d7dc15614579a4b8d65b5e047c02e9fe10bbe15102a14e7` |
| Codex CLI executable | `codex-cli 0.145.0`; SHA-256 `83751f15cb6a0a7b97df67752c001e3fe1c20e18ffbfec3ff63567296205eb6c` |
| Official Codex tag | `rust-v0.145.0`; tag object `1635de866c61d1b76e50b31928ee6d61482435a8`; commit `25af12f7e61572b0bc18ddb1008be543b91519b0` |
| Official npm package | integrity `sha512-/PSPSFujjjmiyVFvG2yu/grOFhsWdokTH8t2KGWhXSo/M5n/dIDsnbsnO82/7bLtIoDuzQf7ATBUMWqPWQINlQ==` |
| Sanitized 0.145.0 protocol fixture | SHA-256 `3c2b18a9fe7e53eb6ed70fd0df33225a272dfa7f1dbf27b98799292346cb6942` |

The release commit records the accepted source candidate. Private evidence
paths, machine identifiers, rollback commands, and raw live receipts are not
public artifacts; the digests above allow the accepted bundle to be
distinguished from withdrawn predecessors.

## Verification

| Boundary | Fresh evidence | Highest level |
|---|---|---|
| Windows core | 1,553 executed, 1,553 passed, 0 failed, plus 1 explicitly ignored subprocess helper out of 1,554 discovered | T3 |
| Windows CLI and direct-owner lifecycle | 170 executed, 170 passed: 150 unit, 15 lifecycle, 4 trace, 1 process-boundary | T3 |
| Windows workspace and source integrity | workspace compilation, formatting, and diff-integrity checks passed | T1/T2 |
| Native Linux workspace | full workspace compilation passed under Rust 1.97.1 from a read-only source mount | T1/T2 |
| Native Linux behavior | the earlier portable config, path, permission, Goal, MCP, latency, and containment pack passed; a post-repair 11-execution worker/CLI pack also passed, with the Unix timeout case intentionally common to both packs | T2/T3 |
| Official Codex app-server | fresh-process generic MCP readiness reached the expected server/tool digest before any turn | T3 |
| Interactive preparation | exact-lane status, Telegram `preparing`, total memory budget, typed degradation, stage timing, and unrelated-lane controls passed | T3 |
| Long-running worker generation | fail-first lease-loss, stale-generation, and active-cancel failures were reproduced; the repaired exact-lease, contention, chronology, late-result, cancellation, restart-reconciliation, long-job, abrupt-owner, and Windows/Unix tree-containment scenarios pass in focused and full verification | T3 |
| Backup operator contract | staged create/verify success, typed empty-exception paths, the four-hour job-specific guard, and transaction-bound recovery evidence passed; the complete multi-gigabyte backup was not rerun as a smoke | T2/T3 plus T4 cutover readback |
| Phase 0.1 transaction closure | complete-store admission, CronRun CAS fencing, prepare/apply generation checks, operator-safe control, and supported Scheduled Task action handling passed scenario replay | T3 |
| Provider presence and delivery | fresh Discord open, identify, ready, and heartbeat acknowledgement were observed; the operator confirmed online presence and natural Discord and Telegram turns reached delivery | T4 |
| Rollback and live deployment | the frozen candidate passed guarded promotion, recovery, direct-owner topology, health, complete CronRun scan, queue, and outbox readback | T4 |

One complete Linux core baseline remains red: 1,417 passed and 47 failed out of
1,464 executed. The remaining failures include historical Windows-only
fixtures, read-only-mount assumptions, older bridge fixtures, and wall-clock
tests. This record does not call the complete Linux suite green.

No native macOS runner was available. macOS evidence is limited to portable
target mapping, Unix implementation review, and host-independent contract
tests. It is not native execution, real-process parity, or a macOS release
claim.

## Incident-Derived Coverage

Two old-deployment observations bind the preparation repair to real failure
signatures:

- a Discord turn spent 487.765 seconds from lease to prompt after a 15-second
  memory bridge deadline and opaque local fallback;
- a Telegram turn spent 914.663 seconds from lease to prompt after the same
  bridge-to-fallback signature, then entered an active provider turn with fresh
  working events.

The source scenario matrix covers the proven preparation failure with immediate
`preparing`, one 20-second default bridge-plus-fallback budget, monotonic stage
receipts, typed degraded-memory continuation, and an exact-lane control.

Post-cutover natural turns measured 140.727 seconds from inbound acceptance to
Discord delivery and 95.251 seconds to Telegram delivery. Queue acceptance to
prompt completion was 32.973 and 18.841 seconds; model execution was 63.404 and
39.681 seconds. Those two samples prove that the old multi-minute preparation
signature was removed and establish a future baseline. They do not prove that
the new architecture caused the remaining totals, because several causal stages
are still missing and there is no controlled prior-version comparison.

The Telegram turn also exposed a duplicate terminal body after an edit returned
provider-not-found and fallback sent a second surface. Progress responsiveness,
terminal-surface uniqueness, complete causal timing, and measured optimization
are assigned to the
[Phase 1 reliability foundation](phase-1-progress-ux-and-latency-foundation.md).

## Explicit Deferrals

Generic hosting acceptance does not:

- mount or authenticate a named consumer for the first time;
- synchronize a consumer-specific skill version;
- choose a consumer agent identity or exercise its inbox, task, memory, or
  acknowledgement policy;
- perform a first-time named-consumer MCP call or prove that consumer's
  task, inbox, memory, or acknowledgement semantics;
- prove native macOS behavior or make the complete Linux suite green;
- claim that publication itself adds platform or named-consumer evidence beyond
  the accepted candidate.

A named-consumer MCP replay remains a separately authorized stopped-state
T3/T4 activity. The natural Discord and Telegram observations in this record
prove chat-provider delivery, not consumer-specific MCP behavior.

## Completed Closure Gates

1. The final manifest was bound to the rebuilt release binary and deployment
   bundle.
2. One independent review passed on that exact frozen bundle with no blockers.
3. Complete-store admission confirmed zero active CronRuns, zero open runtime
   work, and empty platform outboxes.
4. Previous binary, config, state, tool, process-topology, and recovery inputs
   were retained before promotion.
5. The harness, Codex 0.145.0 tool tree, backup wrapper, and backup cron contract
   were promoted through the guarded transaction and recovery path.
6. Live readback confirmed the direct-owner process topology, one generation,
   nine healthy services, exact binary identity, fresh Discord Gateway
   readiness, empty queues and outboxes, and natural delivery on both channels.

Phase 1 may rely on the generic hosting, exact-home readiness, transaction,
process-ownership, provider-presence, and causal-stage contracts recorded here.
It may not infer native macOS parity, a green complete-Linux suite, or
named-consumer MCP behavior from this acceptance.
