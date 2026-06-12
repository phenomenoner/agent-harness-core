# Agent Harness Core Invariants

Date: 2026-06-12

This catalog is the release-gate source for deterministic simulation, scenario replay, and review evidence. The same invariant IDs are exposed by `agent-harness invariants` through `agent_harness_core::quality::invariant_catalog`.

| ID | Invariant | Owner | Current staging evidence | Remaining live/soak evidence |
|---|---|---|---|---|
| I1 | One allowed inbound message triggers at most one model turn. | `channel_ingress`, `channel_runtime`, `runtime_queue` | Channel receive/enqueue tests; runtime queue idempotency tests. | Burst replay fixture with duplicate inbound events. |
| I2 | Every completed turn has exactly one delivery, explicit error notification, or dead-letter notification. | `runtime_pipeline`, `channel_delivery` | Runtime failure outbox tests; dead-letter timeout policy test. | Live provider timeout/dead-letter user notification sample. |
| I3 | Terminal states are irreversible. | `runtime_pipeline`, `runtime_queue`, `workers` | Timeout receipts are terminal for status/selection; queue retry creates a fresh id instead of resurrecting terminal state. | Operator retry/skip drill against staging queue. |
| I4 | Cancel/stop only affects the requested turn, queue item, worker job, or declared scope. | `admission`, `channel_state`, `runtime_queue`, `workers` | Scoped stop marker/receipt test; existing `/stop` cancel marker tests. | Live active-turn and active-job scoped stop receipt. |
| I5 | Crash recovery loses no work and duplicates no side effects. | `queue_shadow`, `supervision`, `runtime_worker` | Lease/idempotency tests; queue shadow divergence tests; supervisor breaker tests. | Seeded crash simulation and seven-day shadow parity summary. |
| I6 | Over-budget work is deferred or blocked, not dropped silently. | `autonomy`, `workers`, `mcp` | SQLite budget counter test; MCP allow-list rejection receipt. | Concurrent budget race fixture under load. |
| I7 | Every accepted ingress has a terminal trace chain reconstructable by one command. | `trace`, `status`, `logging` | `trace_harness_event` scans runtime/channel/log receipts and detects terminal status. | Live traceId sample across ingress, runtime, outbox, and delivery. |

Release rule: a regression that violates I1-I7 blocks cutover even if feature tests pass. Any new queue, tool, memory, or background-work path must either reuse these invariants or add a new invariant before release.
