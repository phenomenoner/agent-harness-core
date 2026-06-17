# Agent Harness Schema Registry

Date: 2026-06-17

The authoritative in-code registry is `agent_harness_core::quality::schema_registry_entries`, exposed by `agent-harness schema-registry`. This document records the current public compatibility contract for review and release checks.

| Schema | Owner module | Compatibility rule | Current status |
|---|---|---|---|
| `agent-harness.runtime-run-once.v1` | `runtime_pipeline` | Append-only JSONL; additive fields only in v1. | Existing reader accepts legacy `timeout`; v1 adds `retry-pending`, `dead-letter`, `context-exhausted`, Round5 runtime metadata fields (`runtimeClass`, `origin`, `cronRunId`, `scheduledForMs`), and skipped cron tombstones emitted when CronRunStore control blocks stale runtime dispatch. |
| `agent-harness.runtime-dead-letter.v1` | `runtime_pipeline` | Additive fields only in v1; terminal receipt semantics are immutable. | Implemented in staging. |
| `agent-harness.runtime-queue-control.v1` | `runtime_queue` | Retry/skip receipts are append-only; terminal source ids are never mutated. | Implemented in staging. |
| `agent-harness.codex-context-preflight.v1` | `codex_runtime` | Append-only JSONL plus per-execution JSON; additive fields only in v1. | Implemented in staging. |
| `agent-harness.codex-context-checkpoint.v1` | `codex_runtime` | Per-execution recovery artifact; additive fields only in v1. | Implemented in staging. |
| `agent-harness.codex-context-rollover.v1` | `codex_runtime` | Per-execution recovery artifact; binding backup path remains optional. | Implemented in staging. |
| `agent-harness.channel-identity-check.v1` | `channel_identity` | Additive fields only in v1; non-bound statuses remain fail-closed. | Implemented. |
| `agent-harness.channel-identity-registry.v1` | `channel_identity` | Additive binding fields only in v1; ambiguous bindings must fail closed. | Implemented. |
| `agent-harness.channel-delivery-intent.v1` | `channel_runtime` | Additive fields only in v1; provider ids must come from captured inbound context. | Implemented. |
| `agent-harness.cron-scheduler.run-once.v1` | `cron_scheduler` | Additive fields only in v1; dry-run must not enqueue or write watermarks. | Implemented. |
| `agent-harness.cron-scheduler.lint.v1` | `cron_scheduler` | Additive diagnostics only in v1; error status remains fail-closed. | Implemented in staging. |
| `agent-harness.cron-scheduler.tick.v1` | `cron_scheduler` | Append-only receipts; additive fields only in v1. | Implemented. |
| `agent-harness.cron-scheduler.job-decision.v1` | `cron_scheduler` | Append-only receipts; idempotency key semantics are stable in v1; retry attempts may add an `:attempt:<n>` suffix. | Implemented. |
| `agent-harness.cron-runs.v1` | `cron_runs` | SQLite state table; additive columns only in v1; status enum changes require migration. | Implemented in staging. |
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
| `agent-harness.skill-invocation-envelope.v1` | `skill_envelope` | Byte-framed envelope with declared lengths/checksum; parser must ignore nested sentinel text inside body bytes. | Implemented in staging. |
| `agent-harness.skill-selection.v1` | `skills` | Append-only selection receipts with matcher metadata, delivery mode, body checksum, score components, and deterministic tie-breaks. | Implemented in staging. |
| `agent-harness.prompt-injection-ledger.v2` | `prompt` | V2 skill entries are keyed by session, agent, skill id, body checksum, and delivery mode; v1 path/fingerprint ledgers remain readable for migration. | Implemented in staging. |
| `agent-harness.skill-usage.v1` | `skill_usage` | Append-only skill usage/provenance JSONL; action enum additions require status and curator compatibility tests. | Implemented in staging. |
| `agent-harness.skill-usage-snapshot.v1` | `skill_usage` | Derived compact status/curator artifact rebuildable from `skill-usage.jsonl`. | Implemented in staging. |
| `agent-harness.skill-proposal.v1` | `skill_learning` | Append-only proposal state records; apply remains checksum-guarded and operator-mediated. | Implemented in staging. |
| `agent-harness.skill-apply-receipt.v1` | `skill_apply` | Append-only apply receipts; stale-base quarantine and backup-before-mutation semantics are stable in v1. | Implemented in staging. |
| `agent-harness.learning-review.v1` | `skill_learning` | Deterministic learning-review report; worker jobs may propose but never mutate skill files directly. | Implemented in staging. |
| `agent-harness.drift-report.v1` | `autonomy` | Additive fields only in v1. | Implemented in staging. |
| `agent-harness.context-pack.v1` | `memory_contracts` | Canonical normalized memory context pack; breaking changes require v2 and fail-open consumer tests. | Implemented in staging. |
| `openclaw-mem.context-pack.v1` | `memory_contracts` | Accepted producer schema translated to `agent-harness.context-pack.v1`; unknown versions fail open. | Implemented in staging. |
| `agent-harness.encrypted-vault.v1` | `vault` | Breaking crypto/KDF changes require v2 and migration receipt. | Implemented in staging. |
| `agent-harness.security-scan.v1` | `security` | Additive findings only in v1. | Implemented in staging. |
| `agent-harness.quality-report.v1` | `quality` | Additive fields only in v1. | Implemented in staging. |

Release rule: every new receipt/state schema must be added here and to `schema_registry_entries` before release. Breaking changes require a v2 schema, old-version reader, and migration receipt for one release cycle.
