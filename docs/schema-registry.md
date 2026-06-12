# Agent Harness Schema Registry

Date: 2026-06-12

The authoritative in-code registry is `agent_harness_core::quality::schema_registry_entries`, exposed by `agent-harness schema-registry`. This document records the current public compatibility contract for review and release checks.

| Schema | Owner module | Compatibility rule | Current status |
|---|---|---|---|
| `agent-harness.runtime-run-once.v1` | `runtime_pipeline` | Append-only JSONL; additive fields only in v1. | Existing reader accepts legacy `timeout`; v1 adds `retry-pending` and `dead-letter`. |
| `agent-harness.runtime-dead-letter.v1` | `runtime_pipeline` | Additive fields only in v1; terminal receipt semantics are immutable. | Implemented in staging. |
| `agent-harness.runtime-queue-control.v1` | `runtime_queue` | Retry/skip receipts are append-only; terminal source ids are never mutated. | Implemented in staging. |
| `agent-harness.config-validation.v1` | `config` | Additive diagnostics only in v1; invalid config remains fail-closed. | Implemented in staging. |
| `agent-harness.log-rotation.v1` | `logging` | Additive fields only in v1; rotation receipts are append-only. | Implemented in staging. |
| `agent-harness.supervision-evaluation.v1` | `supervision` | Additive child fields only in v1. | Implemented in staging. |
| `agent-harness.healthz.v1` | `health` | Local/admin JSON status; additive fields only in v1. | Implemented in staging. |
| `agent-harness.trace.v1` | `trace` | Additive record fields only in v1. | Implemented in staging. |
| `agent-harness.metrics.v1` | `metrics` | Counter names are stable once published; new counters may be added. | Implemented in staging. |
| `agent-harness.supervise-deploy-canary.v1` | `deploy` | Additive fields only in v1; `commit`/`rollback` decisions remain stable. | Implemented in staging. |
| `agent-harness.queue-shadow-record.v1` | `queue_shadow` | Additive fields only in v1. | Implemented in staging. |
| `agent-harness.queue-shadow-compare.v1` | `queue_shadow` | Additive divergence fields only in v1. | Implemented in staging. |
| `agent-harness.admission-decision.v1` | `admission` | Additive fields only in v1; refusal must remain explicit. | Implemented in staging. |
| `agent-harness.scoped-stop.v1` | `admission` | Target shape is stable in v1; add new target kinds only with compatibility tests. | Implemented in staging. |
| `agent-harness.background-registry.v1` | `background` | SQLite task JSON may add fields in v1; status enum changes require migration. | Implemented in staging. |
| `agent-harness.token-efficiency.v1` | `token_efficiency` | Additive fields only in v1. | Implemented in staging. |
| `agent-harness.task-entity.v1` | `autonomy` | Additive fields only in v1; checkpoint JSONL remains append-only. | Implemented in staging. |
| `agent-harness.budget-decision.v1` | `autonomy` | Additive fields only in v1; accepted/blocked decision semantics remain stable. | Implemented in staging. |
| `agent-harness.learning-proposal.v1` | `autonomy` | Proposal JSON may add review fields in v1; auto-apply remains opt-in. | Implemented in staging. |
| `agent-harness.drift-report.v1` | `autonomy` | Additive fields only in v1. | Implemented in staging. |
| `agent-harness.context-pack.v1` | `memory_contracts` | Breaking memory contract changes require v2 and fail-open consumer tests. | Implemented in staging. |
| `agent-harness.encrypted-vault.v1` | `vault` | Breaking crypto/KDF changes require v2 and migration receipt. | Implemented in staging. |
| `agent-harness.security-scan.v1` | `security` | Additive findings only in v1. | Implemented in staging. |
| `agent-harness.quality-report.v1` | `quality` | Additive fields only in v1. | Implemented in staging. |

Release rule: every new receipt/state schema must be added here and to `schema_registry_entries` before release. Breaking changes require a v2 schema, old-version reader, and migration receipt for one release cycle.
