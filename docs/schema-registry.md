# Agent Harness Schema Registry

Date: 2026-07-02

The authoritative in-code registry is `agent_harness_core::quality::schema_registry_entries`, exposed by `agent-harness schema-registry`. This document records the current public compatibility contract for review and release checks.

| Schema | Owner module | Compatibility rule | Current status |
|---|---|---|---|
| `agent-harness.runtime-run-once.v1` | `runtime_pipeline` | Append-only JSONL; additive fields only in v1. | Existing reader accepts legacy `timeout`; v1 adds `retry-pending`, `dead-letter`, `context-exhausted`, Round5 runtime metadata fields (`runtimeClass`, `origin`, `cronRunId`, `scheduledForMs`), and skipped cron tombstones emitted when CronRunStore control blocks stale runtime dispatch. |
| `agent-harness.codex-runtime-run.v1` | `codex_runtime` | Append-only JSONL plus per-execution JSON; additive fields only in v1. | Round10 adds optional `toolUseTimeout` metadata when an active Codex tool-use idle timeout is stopped and routed through bounded fresh-thread recovery. |
| `agent-harness.external-review-evidence.v1` | `codex_runtime` | Per-execution recovery artifact; additive fields only in v1. | Local Round10 gap closure captures review-only recovery output as evidence instead of final parent workflow completion. |
| `agent-harness.inbound-media-artifact.v1` | `media` | Artifact metadata is additive in v1; prompt-facing consumers must treat raw payload/provider fields as redaction candidates and use bounded extraction summaries for durable context. | Round10 adds optional `lifecycleStatus` and `extractionSummary` fields so images, audio/transcripts, generated media, browser captures, documents/downloads, large tool logs/review transcripts, worker reports, and provider-native attachments can be represented by refs plus summaries instead of raw blobs. Channel-media staging adds optional `provenance` so current-message and referenced-message media can be distinguished without exposing provider payloads. |
| `agent-harness.outbound-media-policy.v1` | `media_delivery_policy` | Append-only policy receipts; accepted extension/kind mappings may add entries in v1, while denied prefixes and max-byte checks must stay fail-closed. | Channel-media staging validates local path and harness-artifact outbound attachments before provider delivery and records accepted/rejected policy decisions. |
| `agent-harness.media-delivery-lint.v1` | `runtime_pipeline` | Append-only lint receipts; warning diagnostics are additive, and fail-closed mode must preserve terminal error semantics. | Channel-media staging masks protected spans before parsing media directives, records directive diagnostics, and can fail closed through `media.lintFailClosed`. |
| `agent-harness.runtime-dead-letter.v1` | `runtime_pipeline` | Additive fields only in v1; terminal receipt semantics are immutable. | Implemented in staging. |
| `agent-harness.runtime-queue-control.v1` | `runtime_queue` | Retry/skip receipts are append-only; terminal source ids are never mutated. | Implemented in staging. |
| `agent-harness.runtime-queue-leases.v1` | `runtime_worker` | Class-scoped state JSON accepts legacy `owner: "pid:<n>"` strings and structured owner envelopes in v1. | Implemented in staging. |
| `agent-harness.runtime-queue-lease-reconciliation.v1` | `runtime_worker` | Operator report for supervisor generation lease reaping; additive fields only in v1. | Implemented in staging. |
| `agent-harness.runtime-queue-latency.v1` | `latency` | Append-only per-stage queue latency receipts; additive stages and timestamps only in v1. | Implemented in staging. |
| `agent-harness.latency-status.v1` | `agent-harness-cli` | Read-only CLI summary over latency receipts; additive summary fields only in v1. | Implemented in staging. |
| `agent-harness.progress-delivery-state.v1` | `progress` | State JSON may add cursor/cache/counter fields in v1; existing lane cursors remain readable. | Round10 tracks body/status lane delivery counters; body/action lane non-terminal cap is finite while status/current-step heartbeat remains editable after body cap using `progressDeliveryStatusHeartbeatAfterBodyCapMs`. |
| `agent-harness.codex-context-preflight.v1` | `codex_runtime` | Append-only JSONL plus per-execution JSON; v1 adds thread-health scan details for inline image/tool-output bloat and compact-before-turn decisions. | Implemented in staging. |
| `agent-harness.codex-context-checkpoint.v1` | `codex_runtime` | Per-execution recovery artifact; additive fields only in v1. | Implemented in staging. |
| `agent-harness.codex-context-rollover.v1` | `codex_runtime` | Per-execution recovery artifact; binding backup path remains optional. | Implemented in staging. |
| `agent-harness.virtual-session-working-context.v1` | `virtual_session_context` | Read-only bounded resolver envelope over exact-lane working-set/session receipts; additive fields only in v1 and evidence anchors remain pointers, not payloads. | Implemented in staging. |
| `agent-harness.channel-identity-check.v1` | `channel_identity` | Additive fields only in v1; non-bound statuses remain fail-closed. | Implemented. |
| `agent-harness.channel-identity-registry.v1` | `channel_identity` | Additive binding fields only in v1; ambiguous bindings must fail closed. | Implemented. |
| `agent-harness.channel-delivery-intent.v1` | `channel_runtime` | Additive fields only in v1; provider ids must come from captured inbound context. | Implemented. |
| `agent-harness.rich-message-presentation.v1` | `rich_presentation` | Optional outbound presentation field; old outbox JSON without `presentation` must deserialize as plain text. v1 accepts additive semantic fields only after validation. | Package A adds schema structs, fail-closed validation, backward-compatibility coverage, and render-only Telegram/Discord fixtures; Package B adds rendered batch units and action capability gates; Package C stages provider send helper integration for adapter-rendered Telegram/Discord text/media plus rendered-unit delivery receipts while callbacks remain disabled. Channel-media staging resolves inbound-media and generated-image/generated-media `artifactRef` units into policy-validated attachments and records `renderedUnits.attachmentKind`. |
| `agent-harness.channel-restart-request.v1` | `channel_runtime` | Restart request receipts are append-only; stop-file envelope action fields are additive in v1. | Implemented in staging. |
| `agent-harness.gateway-restart-request.v1` | `channel_state` | Protected gateway restart requests are append-only; plain `/restart` remains request-only in v1. | Implemented in staging. |
| `agent-harness.cron-scheduler.run-once.v1` | `cron_scheduler` | Additive fields only in v1; dry-run must not enqueue or write watermarks. | Implemented. |
| `agent-harness.cron-scheduler.lint.v1` | `cron_scheduler` | Additive diagnostics only in v1; error status remains fail-closed. | Implemented in staging. |
| `agent-harness.cron-scheduler.tick.v1` | `cron_scheduler` | Append-only receipts; additive fields only in v1. | Implemented. |
| `agent-harness.cron-scheduler.job-decision.v1` | `cron_scheduler` | Append-only receipts; idempotency key semantics are stable in v1; retry attempts may add an `:attempt:<n>` suffix. | Implemented. |
| `agent-harness.cron-runs.v1` | `cron_runs` | SQLite state table; additive columns only in v1; status enum changes require migration. | Implemented in staging. |
| `agent-harness.config-validation.v1` | `config` | Additive diagnostics only in v1; invalid config remains fail-closed. | Implemented in staging. |
| `agent-harness.log-rotation.v1` | `logging` | Additive fields only in v1; rotation receipts are append-only. | Implemented in staging. |
| `agent-harness.supervision-evaluation.v1` | `supervision` | Additive child fields only in v1. | Implemented in staging. |
| `agent-harness.supervisor-stop-file.v1` | `ops` | JSON stop-file envelope may add metadata in v1; legacy plain-text reasons stay readable. | Implemented in staging. |
| `agent-harness.runtime-loop-runner-safe-mode.v1` | `supervisor` | Runner safe-mode JSON may add diagnostic fields in v1; `restartAfterSeconds` and `memoryGateDecision` remain advisory. | Implemented in staging. |
| `agent-harness.supervisor-service-state.v1` | `supervisor` | Service state JSON may add diagnostic fields in v1; `launchOwner` and `servicePriority` distinguish observe-only external runners from `rust-supervisor-run` children. | Implemented in staging. |
| `agent-harness.supervisor-inventory.v1` | `supervisor_inventory` | Desired-service inventory reports may add health fields in v1; missing/stale/launch action semantics remain stable. | Implemented in staging. |
| `agent-harness.supervisor-reconcile.v1` | `agent-harness-cli` | CLI reconcile output may add launch diagnostics in v1; apply remains explicit and never implied by dry-run. | Implemented in staging. |
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
| `agent-harness.wake-sequence.v1` | `wake` | Per-lane wake sequence files may add diagnostic fields in v1; sequence remains monotonic best-effort. | Implemented in staging. |
| `agent-harness.task-entity.v1` | `autonomy` | Additive fields only in v1; checkpoint JSONL remains append-only. | Implemented in staging. |
| `agent-harness.budget-decision.v1` | `autonomy` | Additive fields only in v1; accepted/blocked decision semantics remain stable. | Implemented in staging. |
| `agent-harness.learning-proposal.v1` | `autonomy` | Proposal JSON may add review fields in v1; auto-apply remains opt-in. | Implemented in staging. |
| `agent-harness.operation-plan.v1` | `operation_plan` | Plan JSON may add metadata in v1; plan id and status semantics remain stable. | Implemented in staging. |
| `agent-harness.operation-plan-item.v1` | `operation_plan` | Item JSON may add metadata in v1; evidence-required completion remains stable. | Implemented in staging. |
| `agent-harness.operation-plan-event.v1` | `operation_plan` | Append-only plan event records; additive fields only in v1. | Implemented in staging. |
| `agent-harness.operation-plan-comment.v1` | `operation_plan` | Append-only comments; additive fields only in v1. | Implemented in staging. |
| `agent-harness.operation-plan-receipt.v1` | `operation_plan` | Append-only receipts; idempotency keys and action names remain stable in v1. | Implemented in staging. |
| `agent-harness.builtin-skill-sync.v1` | `harness_skills` | Builtin skill sync receipts are additive; user-modified skills remain protected unless forced. | Skill ecosystem staged contract. |
| `agent-harness.builtin-skill-manifest.v1` | `harness_skills` | Builtin manifest entries keep skill id, path, version, and fingerprint stable in v1. | Skill ecosystem staged contract. |
| `agent-harness.skill-index.v1` | `skills` | Skill index output may add summary facets in v1; skill ids, source kinds, paths, checksums, and frontmatter fields remain stable evidence anchors. | Skill ecosystem staged contract. |
| `agent-harness.skill-invocation-envelope.v1` | `skill_envelope` | Byte-framed envelope with declared lengths/checksum; parser must ignore nested sentinel text inside body bytes. | Implemented in staging. |
| `agent-harness.skill-selection.v1` | `skills` | Append-only selection receipts with matcher metadata, delivery mode, body checksum, score components, and deterministic tie-breaks. | Matcher v3 uses tokenizer `mixed-v1` and adds CJK, tag/category, FTS, usage-prior, and catalog metadata fields without a schema break. |
| `agent-harness.prompt-injection-ledger.v2` | `prompt` | V2 skill entries are keyed by session, agent, skill id, body checksum, and delivery mode; v1 path/fingerprint ledgers remain readable for migration. | Implemented in staging. |
| `agent-harness.skill-usage.v1` | `skill_usage` | Append-only skill usage/provenance JSONL; action enum additions require status and curator compatibility tests. | Implemented in staging. |
| `agent-harness.skill-usage-snapshot.v1` | `skill_usage` | Derived compact status/curator artifact rebuildable from `skill-usage.jsonl`. | Adds by-skill action counters for bounded usage-prior scoring. |
| `agent-harness.skill-lint-receipt.v1` | `skill_lint` | Append-only lint receipts; finding codes and severities are additive in v1. | Skill ecosystem staged contract. |
| `agent-harness.skill-guard-receipt.v1` | `skill_guard` | Append-only guard receipts; verdict semantics safe/caution/dangerous remain stable in v1. | Skill ecosystem staged contract. |
| `agent-harness.skill-lifecycle.v1` | `skill_curator` | State JSON may add lifecycle metadata in v1; archive remains restorable move semantics. | Skill ecosystem staged contract. |
| `agent-harness.skill-curator.v1` | `skill_curator` | Per-run reports are additive; dry-run must not mutate skill files. | Skill ecosystem staged contract. |
| `agent-harness.skill-restore.v1` | `skill_curator` | Restore receipts are additive; archive source and restored target paths remain stable in v1. | Skill ecosystem staged contract. |
| `agent-harness.skill-pin.v1` | `skill_curator` | Pin receipts are additive; pinned/unpinned state remains the lifecycle protection contract in v1. | Skill ecosystem staged contract. |
| `agent-harness.skill-pack.v1` | `skill_pack` | Pack manifest is checksum-guarded; additive manifest fields are allowed in v1. | Skill ecosystem staged contract. |
| `agent-harness.skill-pack-lock.v1` | `skill_pack` | Lockfile may add metadata in v1; installed path checksum entries remain stable. | Skill ecosystem staged contract. |
| `agent-harness.skill-pack-receipt.v1` | `skill_pack` | Import/export/remove receipts are additive; installed path and checksum evidence remains stable in v1. | Skill ecosystem staged contract. |
| `agent-harness.skill-synthesis-receipt.v1` | `skill_synthesis` | Append-only synthesis receipts; proposal ids and target paths are stable evidence anchors. | Skill ecosystem staged contract. |
| `agent-harness.skill-autonomous-apply-receipt.v1` | `skill_apply` | Append-only autonomous review receipts; approve, quarantine, and blocked decisions are stable in v1. | Skill ecosystem staged contract; autonomous review/apply is the default synthesis path unless `--propose-only` is requested. |
| `agent-harness.skill-proposal.v1` | `skill_learning` | Append-only proposal state records; apply remains checksum-guarded and may be operator-approved or autonomously reviewed/applied. | Implemented in staging. |
| `agent-harness.skill-apply-receipt.v1` | `skill_apply` | Append-only apply receipts; stale-base quarantine and backup-before-mutation semantics are stable in v1. | Implemented in staging. |
| `agent-harness.skill-doctor.v1` | `skill_doctor` | Aggregate health reports are additive in v1; non-receipt read-only runs preserve report shape. | Skill ecosystem staged contract. |
| `agent-harness.learning-review.v1` | `skill_learning` | Deterministic learning-review report; worker jobs may propose but never mutate skill files directly. | Implemented in staging. |
| `agent-harness.self-improvement-review.v1` | `self_improvement` | Append-only review hook receipts; apply mode aliases are additive and replacements remain checksum-guarded. | Implemented in staging. |
| `agent-harness.drift-report.v1` | `autonomy` | Additive fields only in v1. | Implemented in staging. |
| `agent-harness.context-pack.v1` | `memory_contracts` | Canonical normalized memory context pack; breaking changes require v2 and fail-open consumer tests. | Implemented in staging. |
| `openclaw-mem.context-pack.v1` | `memory_contracts` | Accepted producer schema translated to `agent-harness.context-pack.v1`; unknown versions fail open. | Implemented in staging. |
| `agent-harness.openclaw-mem-local-owner-prepare.v1` | `memory` | Append-only receipts; local prepare may add diagnostics but must not promote without operator approval. | Implemented in staging. |
| `agent-harness.encrypted-vault.v1` | `vault` | Breaking crypto/KDF changes require v2 and migration receipt. | Implemented in staging. |
| `agent-harness.security-scan.v1` | `security` | Additive findings only in v1. | Implemented in staging. |
| `agent-harness.quality-report.v1` | `quality` | Additive fields only in v1. | Implemented in staging. |
| `agent-harness.scenario-matrix.v1` | `quality` | Release-gate catalog; additive scenario entries, evidence fields, and runnable-test pointers only in v1. | Round12 scenario-matrix follow-up exposes topology-sensitive release selectors through `agent-harness scenario-matrix`; Package A+B adds the rich-message-presentation selector for schema/validation/rendered-batch/receipt evidence. |

Release rule: every new receipt/state schema must be added here and to `schema_registry_entries` before release. Breaking changes require a v2 schema, old-version reader, and migration receipt for one release cycle.
