use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::Serialize;

const QUALITY_REPORT_SCHEMA: &str = "agent-harness.quality-report.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicHygieneOptions {
    pub root: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InvariantEntry {
    pub id: &'static str,
    pub statement: &'static str,
    pub owner: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaRegistryEntry {
    pub schema: &'static str,
    pub owner_module: &'static str,
    pub compatibility: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScenarioMatrixEntry {
    pub id: &'static str,
    pub title: &'static str,
    pub changed_areas: Vec<&'static str>,
    pub required_invariants: Vec<&'static str>,
    pub required_evidence: Vec<&'static str>,
    pub runnable_tests: Vec<&'static str>,
    pub promotion_gate: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicHygieneReport {
    pub schema: &'static str,
    pub root: PathBuf,
    pub passed: bool,
    pub forbidden_hits: Vec<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReleaseChecklist {
    pub schema: &'static str,
    pub required_items: Vec<&'static str>,
}

pub fn invariant_catalog() -> Vec<InvariantEntry> {
    vec![
        InvariantEntry {
            id: "I1",
            statement: "one allowed inbound triggers at most one model turn",
            owner: "channel/runtime_queue",
        },
        InvariantEntry {
            id: "I2",
            statement: "every completed turn has exactly one delivery, explicit error notification, or dead-letter notification",
            owner: "runtime_pipeline/channel_delivery",
        },
        InvariantEntry {
            id: "I3",
            statement: "terminal states are irreversible, pending or retryable worker jobs already at their attempt cap become terminal before selection or lease, and append-only queued admission rows cannot override effective terminal state in sibling guards",
            owner: "runtime_pipeline/workers",
        },
        InvariantEntry {
            id: "I4",
            statement: "cancel only affects the requested turn, queue item, job, declared scope, or the selected deterministic job's descendant process tree",
            owner: "admission/channel_state/workers",
        },
        InvariantEntry {
            id: "I5",
            statement: "crash recovery loses no work and duplicates no side effects",
            owner: "queue_shadow/supervision",
        },
        InvariantEntry {
            id: "I6",
            statement: "over-budget work is deferred or blocked, not dropped",
            owner: "autonomy",
        },
        InvariantEntry {
            id: "I7",
            statement: "ingress always has a terminal trace chain",
            owner: "trace",
        },
        InvariantEntry {
            id: "I8",
            statement: "agent identity and /new task boundaries are routing boundaries across channel state, session freshness, prompt, skill source/eligibility/usage priors, runtime, outbox, delivery, and memory namespaces",
            owner: "channel_state/runtime_pipeline/prompt/turns/skills/skill_usage/memory",
        },
        InvariantEntry {
            id: "I9",
            statement: "final channel replies exclude progress/narration stream content and review-only evidence when the parent workflow has not completed",
            owner: "runtime_pipeline/progress/channel_delivery",
        },
        InvariantEntry {
            id: "I10",
            statement: "active Codex tool-use idle timeouts and productive absolute turn timeouts are routed through bounded recovery or virtual-session continuation; the final deadline window drains work and defers late steering instead of starting another long action",
            owner: "codex_runtime/runtime_pipeline/trace/context_rollover/prompt",
        },
        InvariantEntry {
            id: "I11",
            statement: "binary and bulky artifacts enter durable main-session context only as harness artifact references plus bounded extraction summaries; raw bytes, base64, provider URLs, and large tool blobs stay in artifact storage or receipts, including expanded inbound media kinds and referenced-message media",
            owner: "media/prompt/runtime_worker/codex_runtime/workers/memory",
        },
        InvariantEntry {
            id: "I12",
            statement: "active mem-engine ownership uses the openclaw-mem bridge as the primary write/recall path when configured; recall fallback remains read-only and store fails closed unless the memory layer accepts the write",
            owner: "memory/memory_owner/openclaw-mem",
        },
        InvariantEntry {
            id: "I13",
            statement: "inline-image, native-image-input, oversized-output polluted Codex threads, interrupted productive long-task failures, and repeated high-usage stream-unstable retries feed bounded context-rollover continuity when exact-lane depth and effective-sibling-state gates allow it",
            owner: "codex_runtime/context_rollover/runtime_queue/prompt/runtime_pipeline",
        },
        InvariantEntry {
            id: "I14",
            statement: "rich outbound presentation is rendered by provider adapters from a trusted semantic payload; model-authored raw Telegram/Discord syntax is not the safety boundary, and media units carry provider delivery receipts with attachment-kind accounting",
            owner: "runtime_pipeline/channel_delivery/progress/media/trace",
        },
        InvariantEntry {
            id: "I15",
            statement: "concrete channel session history is lane-bound: session/private recall candidates require same agent and lane-qualified session key, while broad project/global recall must be explicit",
            owner: "memory_pack/memory/prompt/context_rollover",
        },
        InvariantEntry {
            id: "I16",
            statement: "outbound channel attachments originate only from policy-validated local paths or resolvable harness artifacts; directive-like text inside protected spans is never delivered; rejected directives leave a visible note plus a machine-readable receipt",
            owner: "runtime_pipeline/media_delivery_policy/channel_delivery",
        },
        InvariantEntry {
            id: "I17",
            statement: "durable control artifacts are authoritative before runtime execution, progress delivery, restart consumption, and sender-class cron notification; deterministic cron uses one exact source, timezone/calendar evaluation, bounded per-entry execution policy, stable occurrence identity, and explicit catch-up authority",
            owner: "runtime_worker/runtime_pipeline/runtime_queue/progress/channel_runtime/dream_director/cron_scheduler/deterministic_cron",
        },
        InvariantEntry {
            id: "I18",
            statement: "final delivery planning treats delivered or skipped-permanent receipts as terminal evidence, even if later retryable provider failures are recorded for the same logical delivery",
            owner: "channel_delivery/agent-harness-cli",
        },
        InvariantEntry {
            id: "I19",
            statement: "a verification command interrupted by a newer same-lane turn records structured interruption evidence and resume guidance instead of being reported as a failed test",
            owner: "codex_runtime/runtime_pipeline/virtual_session_context/prompt",
        },
        InvariantEntry {
            id: "I20",
            statement: "skill ecosystem mutation is proposal-mediated but autonomous review/apply is a first-class path; selection is agent-scoped, rejects weak body-only automatic matches, deduplicates active/imported copies by original id with active-source preference, and preserves hard lifecycle/allowlist/invocation gates and receipts",
            owner: "skills/skill_usage/turns/skill_apply/skill_guard/skill_lint/skill_curator/prompt",
        },
        InvariantEntry {
            id: "I21",
            statement: "reasoning effort is authorized against the effective provider/model route and preserved exactly through queue and runtime; /think and /reasoning are aliases of one last-write-wins state, GPT-5.6 max remains distinct, and ultra is rejected as a reasoning effort",
            owner: "channel_commands/channel_state/channel_runtime/model_catalog/runtime_queue/codex_runtime",
        },
        InvariantEntry {
            id: "I22",
            statement: "each agent prompt is assembled from its own canonical manifest under an exact full-lane and backend-generation key; aliases are fallback-only, deletions create tombstones, and another agent or lane cannot reuse the ledger entry",
            owner: "prompt/turns/virtual_session_context/operation_plan/runtime_worker",
        },
        InvariantEntry {
            id: "I23",
            statement: "each child has an immutable independent model/effort policy and exact master-owned result owner; child terminal results enter the durable mailbox and never directly create the parent final outbox",
            owner: "child_execution_policy/worker_adapters/workers/worker_result_mailbox/runtime_pipeline",
        },
        InvariantEntry {
            id: "I24",
            statement: "coordinator resume is durable, exact-lane, coalesced, and at-most-once: an active parent lease suppresses wakeup, a confirmed released lease schedules one typed continuation, and mailbox acknowledgement follows continuation lease acquisition",
            owner: "worker_coordination/worker_resume/coordinator_resume/workers/runtime_worker",
        },
        InvariantEntry {
            id: "I25",
            statement: "interactive ingress, progress, runtime completion, and final delivery use bounded committed receipt snapshots; expensive history retention is only signaled on their path and runs under the isolated ledger-maintenance owner",
            owner: "runtime_receipt_history/channel_delivery_history/progress_history/ledger_maintenance/supervisor",
        },
        InvariantEntry {
            id: "I26",
            statement: "the Windows Task Scheduler plan preserves every enabled configured supervisor.telegramLoops runner while adding the isolated ledger-maintenance owner; plan and reconcile derive custom channel loop identity from the same harness config",
            owner: "supervisor/harness_config/supervisor_inventory",
        },
        InvariantEntry {
            id: "I27",
            statement: "provider-visible progress delivery is source-authoritative and stop-responsive: a non-fresh progress snapshot cannot replay cached state, historical events without a known provider surface cannot create a fresh surface, and a stop request releases unattempted fresh-send claims before provider I/O",
            owner: "progress/progress_event_index/agent-harness-cli/supervisor",
        },
    ]
}

pub fn schema_registry_entries() -> Vec<SchemaRegistryEntry> {
    vec![
        SchemaRegistryEntry {
            schema: "agent-harness.runtime-run-once.v1",
            owner_module: "runtime_pipeline",
            compatibility: "append-only JSONL, additive fields only in v1 including terminalControlMatched, terminalControlSource, suppressedRunOnceReason, and preparedExecutionTerminalizationReason",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.ledger-maintenance.v1",
            owner_module: "ledger_maintenance",
            compatibility: "read-only maintenance report; additive per-ledger compaction fields only in v1, normal passes remain source-aware and bounded while force is operator-only",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.codex-runtime-run.v1",
            owner_module: "codex_runtime",
            compatibility: "append-only JSONL plus per-execution JSON; v1 accepts additive recovery fields such as toolUseTimeout, interruptionReason, interruptedToolUses, and contextRecovery.threadHealthStatus; productive absolute timeout classification uses bounded stdout evidence rather than eventCount alone",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.codex-active-turn.v1",
            owner_module: "codex_runtime",
            compatibility: "per-session state JSON; absoluteDeadlineAtMs and deadlineDrainAtMs are optional additive fields, and legacy bindings without them remain readable",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.codex-turn-steer-receipt.v1",
            owner_module: "codex_runtime",
            compatibility: "append-only JSONL; deadline-drain sent/accepted/failed/unconfirmed and deferred-deadline-drain statuses are additive in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.external-review-evidence.v1",
            owner_module: "codex_runtime",
            compatibility: "per-execution recovery artifact; additive fields only in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.inbound-media-artifact.v1",
            owner_module: "media",
            compatibility: "artifact metadata is additive in v1; lifecycleStatus, extractionSummary, and provenance are optional additive fields for bounded prompt hygiene",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.outbound-media-policy.v1",
            owner_module: "media_delivery_policy",
            compatibility: "append-only policy receipts; path hashes and reason codes are additive in v1, raw sensitive payloads are never recorded",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.media-delivery-lint.v1",
            owner_module: "runtime_pipeline",
            compatibility: "append-only lint receipts; warning and failed-closed statuses are additive in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.runtime-dead-letter.v1",
            owner_module: "runtime_pipeline",
            compatibility: "additive fields only in v1; terminal semantics are immutable",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.runtime-queue-control.v1",
            owner_module: "runtime_queue",
            compatibility: "append-only receipts; queue-skip is terminal control evidence and retry creates fresh ids",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.runtime-queue-quarantine.v1",
            owner_module: "runtime_worker",
            compatibility: "per-queue marker JSON is additive in v1 and rebuildable from terminalization evidence; presence is terminal control evidence",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.runtime-queue-leases.v1",
            owner_module: "runtime_worker",
            compatibility: "class-scoped state JSON accepts legacy owner strings and structured owner envelopes in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.runtime-queue-lease-reconciliation.v1",
            owner_module: "runtime_worker",
            compatibility: "operator report for generation lease reaping; additive fields only in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.runtime-queue-latency.v1",
            owner_module: "latency",
            compatibility: "append-only per-stage queue latency receipts; additive stages and timestamps only in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.latency-status.v1",
            owner_module: "agent-harness-cli",
            compatibility: "read-only CLI summary over latency receipts; additive summary fields only in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.progress-delivery-state.v1",
            owner_module: "progress",
            compatibility: "state JSON may add cursor/cache/counter fields in v1; existing lane cursors remain readable; progressSuppressedReason is additive on delivery receipts/pending context",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.progress-delivery-plan.v1",
            owner_module: "progress",
            compatibility: "plan summary counters and warnings are additive in v1; non-fresh source snapshots produce no provider pending items, and historical providerless queues stay suppressed",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.codex-context-preflight.v1",
            owner_module: "codex_runtime",
            compatibility: "append-only JSONL plus per-execution JSON; additive fields only in v1, including threadHealthStatus",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.codex-context-checkpoint.v1",
            owner_module: "codex_runtime",
            compatibility: "per-execution recovery artifact; additive fields only in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.codex-context-rollover.v1",
            owner_module: "codex_runtime",
            compatibility: "per-execution recovery artifact; binding backup path remains optional",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.virtual-session-working-context.v1",
            owner_module: "virtual_session_context",
            compatibility: "read-only resolver envelope over existing lane state; additive fields only in v1 and evidence anchors remain bounded pointers, not payloads",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.channel-identity-check.v1",
            owner_module: "channel_identity",
            compatibility: "additive fields only in v1; non-bound statuses remain fail-closed",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.channel-identity-registry.v1",
            owner_module: "channel_identity",
            compatibility: "additive binding fields only in v1; ambiguous bindings must fail closed",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.channel-delivery-intent.v1",
            owner_module: "channel_runtime",
            compatibility: "additive fields only in v1; provider ids must come from captured inbound context",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.rich-message-presentation.v1",
            owner_module: "rich_presentation",
            compatibility: "optional field on channel outbound messages; old outbox JSON without presentation remains plain text, v1 is additive, validates bounded semantic blocks including lists, and provider senders may honor it with adapter-rendered Telegram HTML or Discord safe Markdown while callbacks stay gated",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.channel-delivery-receipt.v1",
            owner_module: "channel_delivery",
            compatibility: "append-only delivery receipts; presentation/renderedUnits and renderedUnits.attachmentKind are additive and legacy receipts without presentation remain readable",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.channel-restart-request.v1",
            owner_module: "channel_runtime",
            compatibility: "append-only restart receipts; stop-file envelope action remains additive in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.gateway-restart-request.v1",
            owner_module: "channel_state",
            compatibility: "append-only protected gateway restart requests; command effect remains request-only in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.cron-scheduler.run-once.v1",
            owner_module: "cron_scheduler",
            compatibility: "additive fields only in v1; dry-run must not enqueue or write watermarks",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.cron-scheduler.lint.v1",
            owner_module: "cron_scheduler",
            compatibility: "additive diagnostics only in v1; error status remains fail-closed",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.cron-scheduler.tick.v1",
            owner_module: "cron_scheduler",
            compatibility: "append-only receipts; additive fields only in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.cron-scheduler.job-decision.v1",
            owner_module: "cron_scheduler",
            compatibility: "append-only receipts; idempotency key semantics are stable in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.cron-runs.v1",
            owner_module: "cron_runs",
            compatibility: "SQLite state table; additive columns only in v1; status enum changes require migration",
        },
        SchemaRegistryEntry {
            schema: "openclaw.mem.dream-director.send-receipt.v1",
            owner_module: "dream_director",
            compatibility: "additive receipt fields only in v1; stale-source suppression remains fail-closed unless force override is receipted",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.config-validation.v1",
            owner_module: "config",
            compatibility: "additive diagnostics only in v1; invalid config remains fail-closed",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.log-rotation.v1",
            owner_module: "logging",
            compatibility: "append-only rotation receipts; additive fields only in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.supervision-evaluation.v1",
            owner_module: "supervision",
            compatibility: "additive child fields only in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.supervisor-stop-file.v1",
            owner_module: "ops",
            compatibility: "JSON stop-file envelope may add metadata in v1; legacy plain-text reasons stay readable",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.runtime-loop-runner-safe-mode.v1",
            owner_module: "supervisor",
            compatibility: "runner safe-mode JSON may add diagnostic fields in v1; restartAfterSeconds remains advisory",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.supervisor-service-state.v1",
            owner_module: "supervisor",
            compatibility: "service state JSON may add diagnostic fields in v1; launchOwner/servicePriority distinguish observe-only external runners from rust-supervisor-run children",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.supervisor-inventory.v1",
            owner_module: "supervisor_inventory",
            compatibility: "desired-service inventory reports may add health fields in v1; missing/stale/launch action semantics remain stable",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.supervisor-reconcile.v1",
            owner_module: "agent-harness-cli",
            compatibility: "CLI reconcile output may add launch diagnostics in v1; apply remains explicit and never implied by dry-run",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.queue-shadow-compare.v1",
            owner_module: "queue_shadow",
            compatibility: "additive divergence fields only in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.queue-shadow-record.v1",
            owner_module: "queue_shadow",
            compatibility: "additive fields only in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.healthz.v1",
            owner_module: "health",
            compatibility: "local JSON status, additive fields only in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.trace.v1",
            owner_module: "trace",
            compatibility: "additive record fields only in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.metrics.v1",
            owner_module: "metrics",
            compatibility: "counter names are stable once published; additive counters allowed",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.supervise-deploy-canary.v1",
            owner_module: "deploy",
            compatibility: "additive fields only in v1; commit/rollback decisions remain stable",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.admission-decision.v1",
            owner_module: "admission",
            compatibility: "additive fields only in v1; refusal remains explicit",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.scoped-stop.v1",
            owner_module: "admission",
            compatibility: "target shape is stable in v1; new target kinds require compatibility tests",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.background-registry.v1",
            owner_module: "background",
            compatibility: "task JSON may add fields in v1; status enum changes require migration",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.token-efficiency.v1",
            owner_module: "token_efficiency",
            compatibility: "additive fields only in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.wake-sequence.v1",
            owner_module: "wake",
            compatibility: "per-lane wake sequence files may add diagnostic fields in v1; sequence remains monotonic best-effort",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.task-entity.v1",
            owner_module: "autonomy",
            compatibility: "additive fields only in v1; checkpoints remain append-only",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.budget-decision.v1",
            owner_module: "autonomy",
            compatibility: "additive fields only in v1; decision semantics remain stable",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.encrypted-vault.v1",
            owner_module: "vault",
            compatibility: "breaking crypto/KDF changes require v2 and migration receipt",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.learning-proposal.v1",
            owner_module: "autonomy",
            compatibility: "proposal JSON may add review fields in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.operation-plan.v1",
            owner_module: "operation_plan",
            compatibility: "plan JSON may add metadata in v1; show/readback reports may add summary fields such as openItems and blockedItems while plan id and status semantics remain stable",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.operation-plan-item.v1",
            owner_module: "operation_plan",
            compatibility: "item JSON may add metadata in v1; evidence-required completion remains stable",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.operation-plan-event.v1",
            owner_module: "operation_plan",
            compatibility: "append-only plan event records; additive fields only in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.operation-plan-comment.v1",
            owner_module: "operation_plan",
            compatibility: "append-only comments; additive fields only in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.operation-plan-receipt.v1",
            owner_module: "operation_plan",
            compatibility: "append-only receipts; idempotency keys and action names remain stable in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.builtin-skill-sync.v1",
            owner_module: "harness_skills",
            compatibility: "builtin skill sync receipts are additive; user-modified skills remain protected unless forced",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.builtin-skill-manifest.v1",
            owner_module: "harness_skills",
            compatibility: "manifest entries keep skill id, path, version, and fingerprint stable in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.skill-index.v1",
            owner_module: "skills",
            compatibility: "index output may add summary facets in v1; skill ids, source kinds, paths, checksums, and frontmatter fields remain stable",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.skill-invocation-envelope.v1",
            owner_module: "skill_envelope",
            compatibility: "byte-framed envelope; declared length/checksum fields are stable in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.skill-selection.v1",
            owner_module: "skills",
            compatibility: "append-only selection receipts; matcherVersion=v4/tokenizer=mixed-v1 records lifecycle/invocation filtering, structured lexical-anchor policy, and deterministic original-id deduplication while additive skill catalog fields remain v1-compatible",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.prompt-injection-ledger.v2",
            owner_module: "prompt",
            compatibility: "v2 skill entries are keyed by session, agent, skill id, body checksum, and delivery mode; v1 path/fingerprint ledgers remain readable for one release cycle",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.skill-usage.v1",
            owner_module: "skill_usage",
            compatibility: "append-only usage JSONL; action enum additions require status and curator compatibility tests",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.skill-usage-snapshot.v1",
            owner_module: "skill_usage",
            compatibility: "derived compact status artifact; additive by-skill action counters are rebuildable from skill-usage JSONL",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.skill-lint-receipt.v1",
            owner_module: "skill_lint",
            compatibility: "append-only lint receipts; finding codes and severities are additive in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.skill-guard-receipt.v1",
            owner_module: "skill_guard",
            compatibility: "append-only guard receipts; verdict semantics safe/caution/dangerous remain stable in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.skill-lifecycle.v1",
            owner_module: "skill_curator",
            compatibility: "state JSON may add lifecycle metadata in v1; archive remains restorable move semantics",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.skill-curator.v1",
            owner_module: "skill_curator",
            compatibility: "per-run reports are additive; dry-run must not mutate skill files",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.skill-restore.v1",
            owner_module: "skill_curator",
            compatibility: "restore receipts are additive; archive source and restored target paths remain stable in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.skill-pin.v1",
            owner_module: "skill_curator",
            compatibility: "pin receipts are additive; pinned/unpinned lifecycle protection remains stable in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.skill-pack.v1",
            owner_module: "skill_pack",
            compatibility: "pack manifest is checksum-guarded; additive manifest fields allowed in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.skill-pack-lock.v1",
            owner_module: "skill_pack",
            compatibility: "lockfile may add metadata in v1; installed path checksum entries remain stable",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.skill-pack-receipt.v1",
            owner_module: "skill_pack",
            compatibility: "import/export/remove receipts are additive; installed path and checksum evidence remains stable in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.skill-synthesis-receipt.v1",
            owner_module: "skill_synthesis",
            compatibility: "append-only synthesis receipts; proposal ids and target paths are stable evidence anchors",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.skill-autonomous-apply-receipt.v1",
            owner_module: "skill_apply",
            compatibility: "append-only autonomous review receipts; approve, quarantine, and blocked decisions are stable in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.skill-proposal.v1",
            owner_module: "skill_learning",
            compatibility: "append-only proposal state records; apply requires checksum match and operator-approved or autonomously reviewed action",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.skill-apply-receipt.v1",
            owner_module: "skill_apply",
            compatibility: "append-only apply receipts; stale-base quarantine semantics are stable in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.skill-doctor.v1",
            owner_module: "skill_doctor",
            compatibility: "aggregate health reports are additive in v1; non-receipt read-only runs preserve report shape",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.learning-review.v1",
            owner_module: "skill_learning",
            compatibility: "deterministic review reports only; worker must not mutate skill files directly",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.self-improvement-review.v1",
            owner_module: "self_improvement",
            compatibility: "append-only review hook receipts; apply mode aliases are additive and replacements remain checksum-guarded",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.drift-report.v1",
            owner_module: "autonomy",
            compatibility: "additive fields only in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.context-pack.v1",
            owner_module: "memory_contracts",
            compatibility: "canonical normalized memory context pack; breaking changes require v2 and fail-open consumer tests",
        },
        SchemaRegistryEntry {
            schema: "openclaw-mem.context-pack.v1",
            owner_module: "memory_contracts",
            compatibility: "accepted producer schema translated to agent-harness.context-pack.v1; unknown versions fail open",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.openclaw-mem-local-owner-prepare.v1",
            owner_module: "memory",
            compatibility: "append-only receipts; local prepare may add diagnostics but must not promote without operator approval",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.security-scan.v1",
            owner_module: "security",
            compatibility: "additive findings only in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.runtime-queue-item.v2",
            owner_module: "runtime_queue/runtime_worker/workers",
            compatibility: "flat additive queue wire; coordinator-resume metadata is a typed V2 variant, while execution snapshots require immutable admissionQueueId and authorizedExecutionMode together; V1 readers remain supported",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.agent-prompt-manifest.v1",
            owner_module: "prompt",
            compatibility: "per-agent canonical source inventory; aliases, tombstones, backend generation, and exact-lane digest fields are additive in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.child-execution-policy.v1",
            owner_module: "child_execution_policy/workers",
            compatibility: "immutable per-child provider/model/reasoning snapshot; open-ended canonical effort strings are additive and validated against the resolved route",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.child-execution-policy.v2",
            owner_module: "child_execution_policy/workers",
            compatibility: "additive wrapper over V1 with an optional default-off execution snapshot; reasoning effort never accepts reserved execution-mode values",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.worker-result-owner.v1",
            owner_module: "worker_result_mailbox",
            compatibility: "exact full-lane, virtual-session, and source identity are immutable; legacy incomplete owners remain auditable but cannot auto-resume",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.worker-result-envelope.v1",
            owner_module: "worker_result_mailbox",
            compatibility: "bounded redacted summaries and opaque artifact pointers only; additive outcome metadata must preserve the content policy",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.worker-result-mailbox.v1",
            owner_module: "worker_result_mailbox/workers",
            compatibility: "SQLite append-once terminal events with additive columns only; owner rebinding conflicts fail closed and acknowledgement is monotonic",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.worker-coordinator-wait.v1",
            owner_module: "worker_coordination/worker_adapters",
            compatibility: "SQLite parent wait state with immutable exact owner and child set; state transitions are monotonic and duplicate admission is idempotent",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.worker-resume-intent.v1",
            owner_module: "worker_resume",
            compatibility: "SQLite exact-lane resume intent; sequence is monotonic, duplicate result sets coalesce, and expired claims are restart-reclaimable",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.coordinator-resume.v1",
            owner_module: "coordinator_resume/workers/runtime_worker",
            compatibility: "typed continuation metadata binds wait, intent, exact owner, and deterministic continuation queue id; additive fields only in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.safe-resume-readiness.v1",
            owner_module: "execution_mode/runtime_queue",
            compatibility: "readiness evidence is observational and fail-closed; additive probes may be added without treating caller booleans as authority",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.quality-report.v1",
            owner_module: "quality",
            compatibility: "additive fields only in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.scenario-matrix.v1",
            owner_module: "quality",
            compatibility: "release-gate catalog; additive scenario entries, evidence fields, and runnable-test pointers only in v1",
        },
    ]
}

pub fn scenario_matrix_catalog() -> Vec<ScenarioMatrixEntry> {
    vec![
        ScenarioMatrixEntry {
            id: "agent-boundary",
            title: "Agent boundary and task freshness",
            changed_areas: vec![
                "channel identity/state",
                "channel ingress/runtime",
                "prompt assembly",
                "runtime pipeline",
            ],
            required_invariants: vec!["I1", "I2", "I7", "I8"],
            required_evidence: vec![
                "same-agent stale-session suppression",
                "different-agent non-suppression on the same platform/channel/user",
                "fresh /new prompt/memory task boundary",
                "trace reconstruction from ingress to terminal outcome",
            ],
            runnable_tests: vec![
                "runtime_pipeline::tests::channel_session_freshness_does_not_cross_suppress_other_agent",
                "prompt::tests::prompt_bundle_new_command_boundary_skips_prior_task_memory_context",
            ],
            promotion_gate: "Run the agent-boundary scenario pack from docs/agent-harness-topology-contract.md for channel/runtime/session changes.",
        },
        ScenarioMatrixEntry {
            id: "final-outbox-delivery-trace",
            title: "Final outbox, delivery, and trace",
            changed_areas: vec![
                "runtime pipeline",
                "final outbox",
                "channel delivery",
                "trace",
            ],
            required_invariants: vec!["I2", "I3", "I7", "I9"],
            required_evidence: vec![
                "exactly one source-correlated final outbox or terminal notification",
                "duplicate suppression by source queue/completion",
                "Telegram and Discord delivery receipt trace",
                "final agent-reply excludes progress/narration stream content",
                "implementation-goal read-only review evidence does not become final agent-reply",
                "structured owner/session routing suppresses owner-mismatched completed output while allowing non-main owned channel lanes to write final outbox",
                "invalid or suppressed outbox rows can be retired with skipped-permanent receipts without counting as delivered",
            ],
            runnable_tests: vec![
                "channel_delivery::tests::outbox_plan_treats_permanent_skip_as_terminal_not_delivered",
                "runtime_pipeline::tests::final_outbox_input_kind_suppresses_read_only_review_only_for_workflow_requests",
                "runtime_pipeline::tests::already_recorded_completion_repair_keeps_progress_panel_out_of_final_outbox",
                "runtime_pipeline::tests::already_recorded_completion_repair_keeps_progress_panel_out_of_discord_final_outbox",
                "runtime_pipeline::tests::run_runtime_queue_once_suppresses_read_only_review_final_for_implementation_goal",
                "runtime_pipeline::tests::run_runtime_queue_once_suppresses_owner_mismatched_agent_final_outbox",
                "runtime_pipeline::tests::run_runtime_queue_once_writes_final_outbox_for_non_main_agent_owned_group_lane",
                "trace::tests::trace_harness_event_detects_terminal_runtime_status",
            ],
            promotion_gate: "Prove completed turns converge to one final/terminal surface with reconstructable queue-to-delivery trace.",
        },
        ScenarioMatrixEntry {
            id: "runtime-terminal-control",
            title: "Runtime terminal-control authority",
            changed_areas: vec![
                "runtime worker",
                "runtime pipeline",
                "runtime queue",
                "lease reconciliation",
            ],
            required_invariants: vec!["I3", "I4", "I7", "I17"],
            required_evidence: vec![
                "queue-skip receipts remain sticky terminal evidence after later non-terminal rows",
                "scoped-stop markers block selection, preparation, and stale lease return",
                "terminal-controlled queues append at most one suppressed run-once receipt",
                "unresumable prepared executions terminalize after the no-prepared threshold and write quarantine markers",
                "lease reconcile re-checks terminal controls before returning stale-owner work to the pool",
                "operator retry of a terminal queue creates a fresh runnable id instead of resurrecting the original",
                "terminal-control suppression writes one final progress surface and then stays silent after late progress events",
                "progress delivery consumes terminal-control evidence before rendering cached non-terminal ghost progress",
                "checked-in sanitized ghost-queue replay plus live or candidate-home evidence show no continuing no-prepared-execution churn",
            ],
            runnable_tests: vec![
                "runtime_worker::tests::queue_skip_receipt_is_sticky_terminal",
                "runtime_worker::tests::scoped_stop_marker_blocks_selection_prepare_and_lease",
                "runtime_worker::tests::suppressed_receipt_emitted_at_most_once",
                "runtime_worker::tests::lease_reconcile_respects_terminal_controls",
                "runtime_worker::tests::queue_retry_of_terminal_item_creates_fresh_runnable_id_only",
                "runtime_pipeline::tests::prepared_protocol_error_terminalizes_after_threshold",
                "runtime_pipeline::tests::scoped_stop_suppresses_missing_prepared_execution_before_no_prepared_churn",
                "runtime_pipeline::tests::terminal_control_queue_gets_one_final_edit_then_silence",
                "runtime_pipeline::tests::untargeted_terminal_control_suppression_appends_progress_with_queue_id",
                "progress::tests::terminal_control_marker_suppresses_cached_nonterminal_ghost_queue",
                "runtime_pipeline::tests::e2e_1_ghost_queue_replay_from_sanitized_fixture",
            ],
            promotion_gate: "Run the focused T1-T6/P5 regression pack plus the checked-in E2E-1 sanitized ghost replay and a live or candidate-home replay of a terminal-controlled pending queue; promotion requires exactly one suppression receipt, one final terminal progress surface, no later status edits, and no recurring no-prepared-execution churn.",
        },
        ScenarioMatrixEntry {
            id: "restart-control-plane",
            title: "Gateway and channel restart closed loop",
            changed_areas: vec![
                "channel runtime",
                "channel state",
                "gateway restart",
                "supervisor loop heartbeat",
                "channel outbox",
            ],
            required_invariants: vec!["I7", "I8", "I17"],
            required_evidence: vec![
                "/restart gateway command is parsed pre-model and writes a protected request",
                "gateway restart consumer moves the request and receipts consumer/process identity",
                "fresh gateway heartbeat closes the request with a completion notice on the requesting lane",
                "/restart status reads request, consumption, completion, and heartbeat generation state",
                "channel restart commands target the live owner's watched stop file or fail explicit ownership ambiguity",
                "checked-in E2E-4 staging replay proves request to consumption to heartbeat to completion ack",
            ],
            runnable_tests: vec![
                "channel_state::tests::gateway_restart_completion_ack_receipts_and_notifies_requesting_lane",
                "channel_state::tests::gateway_restart_status_reads_request_consumption_completion_and_generation",
                "channel_runtime::tests::channel_restart_prefers_live_watched_stop_file",
                "channel_runtime::tests::channel_restart_fails_when_live_owner_is_observed_only",
                "channel_runtime::tests::channel_step_requests_channel_restart_stop_file",
                "channel_runtime::tests::channel_step_replies_to_restart_status_without_agent_turn",
                "agent-harness-cli::tests::consume_gateway_restart_request_moves_file_and_receipts",
                "agent-harness-cli::tests::e2e_4_restart_gateway_staging_closed_loop_replay",
            ],
            promotion_gate: "Run the focused R-series tests plus the checked-in E2E-4 staging replay. Live promotion still requires real supervised child restart and provider-side Discord delivery evidence, not only synthetic heartbeat closure.",
        },
        ScenarioMatrixEntry {
            id: "virtual-session-rollover",
            title: "Virtual session continuity and rollover",
            changed_areas: vec![
                "codex runtime",
                "context rollover",
                "runtime queue",
                "prompt assembly",
                "runtime pipeline",
            ],
            required_invariants: vec!["I2", "I7", "I8", "I13"],
            required_evidence: vec![
                "first official compact success recorded",
                "second official compact success recorded",
                "continuation session or structured skip receipt",
                "inline-image polluted bound thread uses preflight fresh-thread rollover before turn/start",
                "interrupted tool-timeout fallback failure can requeue a guarded continuation before retry churn",
                "no-final-answer interrupted terminal failure can requeue a guarded continuation before parent error delivery",
                "repeated high-usage stream disconnect can requeue a guarded continuation before terminal dead-letter",
                "plain provider outage timeout does not enter interrupted-task continuation",
                "stale timeout stdout capture is rotated before retry recovery can impersonate completion",
                "working-set prompt injection",
                "final outbox/delivery trace from continuation queue item",
                "/new boundary does not inherit previous-task memory context",
                "S1 Telegram same-agent continuation consumes resolver prompt context",
                "S2 Discord same-agent continuation consumes resolver prompt context",
                "S3 same platform/channel/user different agent is not cross-suppressed",
                "S4 /new closes the previous virtual session and starts a fresh task boundary",
                "S5 max-depth terminal-failed guard preserves exact-lane resolver evidence only",
            ],
            runnable_tests: vec![
                "context_rollover::tests::three_turn_compact_rollover_replay_writes_continuation_working_set",
                "codex_runtime::tests::run_codex_runtime_preflight_compacts_existing_thread_before_turn",
                "codex_runtime::tests::context_preflight_rolls_over_bound_thread_inline_image_bloat",
                "codex_runtime::tests::retryable_protocol_error_after_bloated_thread_rolls_over_to_fresh_thread",
                "context_rollover::tests::compact_counter_deduplicates_successful_attempt_key",
                "context_rollover::tests::prepared_auto_requeue_blocks_parent_session_sibling",
                "runtime_pipeline::tests::polluted_thread_continuation_runs_at_terminal_failure_and_respects_depth_limit",
                "runtime_pipeline::tests::tool_timeout_fallback_failure_retry_requeues_continuation",
                "runtime_pipeline::tests::no_final_answer_terminal_interruption_requeues_continuation",
                "runtime_pipeline::tests::plain_provider_timeout_does_not_requeue_interrupted_continuation",
                "runtime_pipeline::tests::stream_unstable_retry_continuation_requires_repeated_high_usage_stream_failure",
                "runtime_pipeline::tests::repeated_stream_disconnect_high_usage_retry_requeues_continuation",
                "runtime_pipeline::tests::stream_unstable_retry_continuation_tombstones_parent_queue_item",
                "codex_runtime::tests::retry_after_timeout_rotates_stale_stdout_before_recovery",
                "prompt::tests::prompt_bundle_new_command_boundary_skips_prior_task_memory_context",
                "prompt::tests::prompt_bundle_includes_virtual_session_resolver_context_section",
                "prompt::tests::prompt_bundle_missing_reply_metadata_hint_uses_resolver_queue_ids",
                "channel_state::tests::new_session_command_closes_previous_virtual_session_record",
                "context_rollover::tests::virtual_session_thread_backfill_updates_matching_working_session",
            ],
            promotion_gate: "Force high-context compact rollover, interrupted long-task rollover, and repeated high-usage stream-unstable retry scenarios, then prove rollover/final-delivery parity across Discord/TG identity axes.",
        },
        ScenarioMatrixEntry {
            id: "codex-deadline-continuation",
            title: "Codex absolute deadline drain and productive continuation",
            changed_areas: vec![
                "codex runtime",
                "context rollover",
                "runtime pipeline",
                "runtime queue",
            ],
            required_invariants: vec!["I2", "I3", "I7", "I8", "I10", "I13"],
            required_evidence: vec![
                "the final bounded fraction of an absolute runtime deadline emits one receipted drain steer and defers later user steering",
                "absolute timeout continuation requires bounded stdout evidence of productive tool, file-change, or completed assistant progress",
                "handshake-only event volume cannot classify an absolute timeout as productive work",
                "historical terminal queue rows cannot block a fresh continuation sibling",
                "late compact completion notifications cannot satisfy an unrelated capability handshake",
            ],
            runnable_tests: vec![
                "codex_runtime::tests::queue_codex_turn_steer_request_defers_inside_deadline_drain_window",
                "codex_runtime::tests::deadline_drain_guard_is_sent_once_with_receipt",
                "runtime_pipeline::tests::productive_absolute_timeout_retry_requeues_continuation_instead_of_replaying_parent",
                "runtime_pipeline::tests::absolute_timeout_without_productive_progress_does_not_requeue_continuation",
                "context_rollover::tests::prepared_auto_requeue_ignores_historical_completed_parent_session_sibling",
                "codex_runtime::tests::managed_reasoning_ignores_late_compact_turn_completed_during_capability_handshake",
            ],
            promotion_gate: "Run the focused deadline-drain, absolute-timeout, sibling-state, and compact-handshake regressions plus the broader codex-runtime/context-rollover/runtime-pipeline packs. Live promotion remains a separate operator-authorized cutover with exact-lane receipts and rollback evidence.",
        },
        ScenarioMatrixEntry {
            id: "skill-selection-anchor-dedupe",
            title: "Skill selection lexical anchors and active-source deduplication",
            changed_areas: vec!["skill selection", "agent-scoped skill sources"],
            required_invariants: vec!["I20"],
            required_evidence: vec![
                "body-only lexical noise cannot auto-select a skill without a structured id, title, description, trigger, tag, or category anchor",
                "explicit invocation remains eligible after automatic-selection hardening",
                "active workspace and imported copies sharing an original id collapse to one selection with active-source preference",
            ],
            runnable_tests: vec![
                "skills::tests::skill_selection_rejects_weak_body_only_matches_and_deduplicates_active_workspace_copy",
                "skills::tests::skill_selection_honors_retirement_and_invocation_controls",
            ],
            promotion_gate: "Run the complete skill-selection regression selector and inspect the selected-skill ledger for one active copy, no body-only false positives, and preserved explicit invocation.",
        },
        ScenarioMatrixEntry {
            id: "progress-surface-volume",
            title: "Progress final surface and delivery volume",
            changed_areas: vec![
                "progress",
                "codex runtime",
                "runtime pipeline",
                "channel delivery",
            ],
            required_invariants: vec!["I2", "I7", "I9", "I13", "I17", "I25", "I27"],
            required_evidence: vec![
                "bounded per-queue body/action edit volume",
                "status/current-step heartbeat after body cap",
                "immediate current-step update for new assistant execution summary",
                "repeated text-hash suppression",
                "terminal progress convergence",
                "terminal progress closes only after source final provider delivery when a source final outbox row exists",
                "no post-terminal edit churn",
                "final outbox contains only final answer",
                "internal worker results summarized without leaking raw worker final text into action progress",
                "queue-local wake preemption with first-surface sends preserved",
                "same-queue providerless orphan surface claims are reclaimed without TTL wait",
                "a non-fresh source snapshot defers cached progress instead of replaying a provider surface",
                "a historical queue without an existing provider surface cannot create a fresh progress send",
                "a watched stop request releases unattempted fresh-send claims before provider I/O",
            ],
            runnable_tests: vec![
                "progress::tests::progress_delivery_repeated_events_converge_to_one_provider_message",
                "progress::tests::progress_delivery_successful_edit_does_not_fresh_send",
                "progress::tests::progress_delivery_duplicate_runtime_events_are_idempotent",
                "progress::tests::fresh_send_requires_enumerated_reason_in_receipt",
                "progress::tests::new_session_hides_old_lane_ghost_status",
                "progress::tests::delivery_plan_status_heartbeat_after_body_cap_is_channel_agnostic",
                "progress::tests::delivery_plan_status_updates_immediately_for_new_current_step_after_body_cap",
                "progress::tests::action_stream_summarizes_internal_worker_results_instead_of_raw_final_text",
                "progress::tests::action_stream_summarizes_structured_subagent_notifications_without_source_label",
                "progress::tests::progress_surface_claim_reclaims_same_queue_orphan_without_ttl",
                "progress::tests::terminal_progress_waits_until_source_final_delivery_is_delivered",
                "agent-harness-cli::tests::progress_delivery_preempts_nonterminal_pending_when_wake_advances",
                "progress::tests::progress_delivery_defers_cached_events_while_index_snapshot_is_not_fresh",
                "progress::tests::progress_delivery_does_not_fresh_send_historical_event_without_existing_surface",
                "agent-harness-cli::tests::progress_delivery_stop_releases_unattempted_surface_claims_before_provider_io",
                "codex_runtime::tests::run_codex_runtime_rejects_stdout_recovery_narration_without_final_answer",
            ],
            promotion_gate: "Replay Telegram and Discord long-running turns through progress caps, recovery, final outbox, terminal convergence, source-index contention, historical-cache recovery, and a watched progress-owner stop.",
        },
        ScenarioMatrixEntry {
            id: "cron-freshness-canon",
            title: "Cron freshness canon and deterministic catch-up",
            changed_areas: vec![
                "cron scheduler",
                "deterministic cron",
                "cron runs",
                "status",
                "health",
                "configuration",
                "Dream Director sender",
            ],
            required_invariants: vec!["I7", "I17"],
            required_evidence: vec![
                "cronScheduler run caps are accepted by config validation",
                "cron-canon monitor blocks surface stale source or keeper receipts in status/health",
                "deterministic cron jobs produce cron-run evidence keyed by canonical cron id",
                "late deterministic slots follow an explicit catch-up policy",
                "sender-class jobs suppress stale sources unless an explicit force override is receipted",
                "deterministic entries use exact crontab sources, timezone/calendar fields, and bounded per-entry execution policy",
                "exhausted jobs terminalize before lease and timeout cancellation covers only the selected process tree",
            ],
            runnable_tests: vec![
                "config::tests::validate_harness_config_accepts_cron_scheduler_run_caps",
                "cron_scheduler::tests::deterministic_cron_execution_reportable_by_canon_id",
                "cron_scheduler::tests::restart_catch_up_respects_per_job_policy",
                "status::tests::collect_status_reports_cron_scheduler_tick_age_and_canon_findings",
                "health::tests::healthz_warns_on_stale_keeper_receipt",
                "health::tests::healthz_evaluates_all_canon_monitor_blocks_generically",
                "workers::tests::deterministic_shell_job_writes_audit_and_succeeds",
                "dream_director::tests::dream_director_sender_suppresses_stale_source",
                "dream_director::tests::dream_director_sender_sends_fresh_source_with_freshness_metadata",
                "dream_director::tests::dream_director_sender_accepts_absolute_source_path_from_relative_home",
                "cron_scheduler::tests::e2e_5_cron_outage_replay_from_sanitized_fixture",
                "cron_scheduler::tests::deterministic_cron_canon_execution_policy_flows_to_cron_run_and_worker",
                "cron_scheduler::tests::lint_rejects_invalid_deterministic_cron_execution_policy",
                "cron_scheduler::tests::backup_cron_restart_catch_up_can_be_suppressed_without_enqueue",
                "cron_scheduler::tests::deterministic_crontab_cron_tz_controls_current_and_catch_up_slots",
                "cron_scheduler::tests::cron_expression_supports_calendar_day_and_month_fields",
                "deterministic_cron::tests::crontab_loader_ignores_backup_and_temporary_files",
                "workers::tests::exhausted_pending_worker_is_terminalized_without_starting_process",
                "workers::tests::deterministic_timeout_terminates_descendant_process_tree",
            ],
            promotion_gate: "Before cutover, run the cron freshness fixture and prove cron config validation, exact source discovery, timezone/calendar scheduling, bounded execution policy, pre-lease attempt exhaustion, process-tree timeout, deterministic cron evidence, catch-up suppression, health/status freshness warnings, and stale-source sender suppression.",
        },
        ScenarioMatrixEntry {
            id: "multi-agent-memory-compartment",
            title: "Multi-agent full matrix and per-agent memory compartment",
            changed_areas: vec![
                "agent registry",
                "channel state",
                "prompt assembly",
                "runtime queue",
                "workers/subagents",
                "memory",
                "final outbox",
                "delivery",
            ],
            required_invariants: vec!["I1", "I2", "I7", "I8", "I12", "I15"],
            required_evidence: vec![
                "new configured agent has independent workspace/prompt source",
                "channel command state preserves agent identity",
                "provider/model command state is agent/session scoped",
                "prompt context hides main-only operation plans from non-main agents",
                "memory recall excludes private main/global imported context unless allowed",
                "session-history retrieval rejects wrong-lane concrete candidates and preserves only explicit project/global fallback",
                "worker/subagent lease ownership remains agent-scoped",
                "owner-mismatched completed output sharing platform/channel/user axes is internal evidence, not parent final outbox",
                "configured non-main agent group-lane completion writes a first-class final outbox row on its own lane",
                "final outbox and delivery receipts preserve agent/lane ownership",
            ],
            runnable_tests: vec![
                "channel_state::tests::new_session_records_agent_from_new_session_key",
                "channel_state::tests::applies_per_agent_global_model_and_thinking_overrides",
                "prompt::tests::prompt_bundle_hides_main_operation_plan_from_other_agent",
                "runtime_worker::tests::prepare_runtime_queue_item_respects_agent_channel_lease_limit",
                "runtime_pipeline::tests::channel_session_freshness_does_not_cross_suppress_other_agent",
                "runtime_pipeline::tests::run_runtime_queue_once_suppresses_owner_mismatched_agent_final_outbox",
                "runtime_pipeline::tests::run_runtime_queue_once_writes_final_outbox_for_non_main_agent_owned_group_lane",
                "memory::tests::non_main_memory_prompt_context_excludes_global_imported_snapshot_by_default",
                "memory::tests::public_agent_read_path_smoke_surfaces_source_allow_list_and_filtered_counts",
                "memory_pack::tests::retrieval_scope_session_requires_same_agent_and_session_key",
                "memory_pack::tests::retrieval_scope_session_rejects_bare_session_key_even_when_equal",
                "memory_pack::tests::retrieval_scope_agent_private_rejects_bare_session_key_even_when_equal",
                "memory_pack::tests::retrieval_candidate_does_not_fall_back_to_wrong_lane_concrete_history",
                "memory_pack::tests::retrieval_candidate_uses_project_after_wrong_lane_concrete_candidate",
                "memory_pack::tests::retrieve_wrong_lane_concrete_history_reports_scope_denied_not_missing",
                "memory_pack::tests::retrieve_wrong_lane_concrete_then_project_returns_explicit_broad_scope",
                "memory_pack::tests::retrieve_wrong_lane_concrete_then_global_imported_returns_explicit_broad_scope",
                "prompt::tests::prompt_bundle_new_command_boundary_skips_prior_task_memory_context",
            ],
            promotion_gate: "Create or reuse a configured-agent matrix covering main, xiaoxiaoli, and a future public/coach agent without rebuilding shared loops. For cross-session search, prove concrete session history is same-agent/session only, non-lane-qualified concrete keys fail closed, wrong-lane concrete candidates fail closed with denial receipts, and project/global fallback is explicit.",
        },
        ScenarioMatrixEntry {
            id: "gpt56-reasoning-capability",
            title: "GPT-5.6 model capability and unified reasoning control",
            changed_areas: vec![
                "channel commands/state",
                "model capability catalog",
                "runtime queue",
                "Codex runtime",
            ],
            required_invariants: vec!["I3", "I8", "I21"],
            required_evidence: vec![
                "/think and /reasoning read and write one authoritative last-write-wins state",
                "gpt-5.6-sol preserves exact max when the effective route advertises it",
                "open-ended future catalog efforts are preserved without hard-coded downgrades",
                "exact ultra is rejected as a reasoning effort and is not advertised or sent to Codex",
                "legacy ultra-high remains the xhigh compatibility alias",
                "turn-start wire evidence binds the resolved route and exact effort",
            ],
            runnable_tests: vec![
                "codex_capability::tests::paginated_catalog_matches_model_field_and_keeps_open_efforts_distinct",
                "channel_runtime::tests::model_catalog_authoritative_reasoning_preserves_exact_sol_max",
                "channel_runtime::tests::think_and_reasoning_alias_share_one_last_write_wins_state",
                "channel_runtime::tests::exact_ultra_is_rejected_as_non_effort_for_both_command_aliases",
                "runtime_queue::tests::enqueue_channel_agent_turn_serializes_exact_max_reasoning_policy",
                "codex_runtime::backend_reasoning_wire_tests::turn_start_effort_is_exact_and_runtime_kill_switch_is_fail_closed",
            ],
            promotion_gate: "Promote only after a current Codex capability probe authorizes the selected GPT-5.6 route, exact max survives command-to-wire verification, both aliases remain identical, and Ultra is absent from the supported effort surface and live configuration.",
        },
        ScenarioMatrixEntry {
            id: "per-agent-prompt-manifest",
            title: "Per-agent prompt manifest and exact-lane virtual-session injection",
            changed_areas: vec![
                "prompt assembly",
                "agent workspace configuration",
                "virtual-session context",
                "operation plan",
                "runtime worker",
            ],
            required_invariants: vec!["I8", "I15", "I22"],
            required_evidence: vec![
                "AGENTS, SOUL, IDENTITY, USER, TOOLS, MEMORY, BOOTSTRAP and other declared agent files are inventoried per agent",
                "canonical names win and aliases are fallback-only",
                "changed sources advance backend generation and deleted sources leave tombstones",
                "prompt ledger reuse requires the exact platform/account/channel/user/agent/runtime/root/concrete lane digest",
                "operation-plan context with a lane digest fails closed on a different lane while legacy plans stay readable",
                "/new and backend-generation changes do not resurrect a prior task prompt context",
            ],
            runnable_tests: vec![
                "turns::tests::prompt_file_aliases_are_fallback_only_and_conflicts_are_deterministic",
                "turns::tests::multi_agent_skill_matrix_isolates_workspaces_allowlists_and_usage_priors",
                "skills::tests::skill_selection_agent_allowlist_is_fail_closed_for_model_and_explicit_invocation",
                "skill_usage::tests::skill_usage_snapshot_for_agent_excludes_other_agent_events",
                "prompt::tests::prompt_manifest_tracks_generation_reinjection_and_delete_tombstone",
                "prompt::tests::prompt_ledger_exact_lane_digest_separates_account_runtime_and_root_axes",
                "prompt::tests::operation_plan_prompt_exact_lane_requires_matching_digest_without_legacy_fallback",
                "operation_plan::tests::operation_plan_v2_persists_exact_lane_digest_and_rejects_mismatched_duplicate",
                "virtual_session_context::tests::exact_lane_denies_legacy_unknown_axes_instead_of_wildcard_matching",
                "prompt::tests::prompt_bundle_new_command_boundary_skips_prior_task_memory_context",
            ],
            promotion_gate: "Promote after the exact-lane prompt matrix passes for two agents sharing platform/channel/user axes, source change/delete reinjection is deterministic, and a /new task plus backend rollover cannot reuse stale prompt or operation-plan context.",
        },
        ScenarioMatrixEntry {
            id: "master-owned-subagent-coordination",
            title: "Master-owned durable subagent result coordination",
            changed_areas: vec![
                "child execution policy",
                "worker admission/store",
                "result mailbox",
                "coordinator wait/resume",
                "runtime queue",
                "final outbox",
            ],
            required_invariants: vec!["I2", "I3", "I5", "I8", "I23", "I24"],
            required_evidence: vec![
                "siblings retain independent provider/model/effort policies",
                "child jobs, parent wait, and watchdog admission commit atomically",
                "terminal child envelopes are append-once and owned by the exact master lane",
                "active parent lease suppresses coordinator wakeup without claiming results",
                "released parent creates one typed coordinator continuation and no legacy master wakeup",
                "duplicate/restart attempts coalesce to one resume intent and acknowledge mailbox rows after lease acquisition",
                "child or owner-mismatched terminal output never reaches the parent final outbox",
                "a controlled real-lane replay admits only an explicit marker-protected two-child source when its exact lane session is current and the main lane plus every planned child execution agent pass authoritative catalog policy before persistence",
            ],
            runnable_tests: vec![
                "child_execution_policy::tests::heterogeneous_siblings_preserve_independent_open_ended_routes_and_efforts",
                "worker_adapters::tests::subagent_adapter_v5_atomically_preserves_heterogeneous_models_and_efforts",
                "worker_adapters::tests::subagent_adapter_v5_rolls_back_children_wait_and_watchdog_together",
                "worker_adapters::tests::controlled_coordinator_smoke_enqueues_only_two_catalog_admitted_children",
                "worker_adapters::tests::controlled_coordinator_smoke_rejects_stale_live_session_before_persisting_workers",
                "worker_adapters::tests::controlled_coordinator_smoke_rejects_non_authoritative_child_agent_before_persisting_workers",
                "worker_adapters::tests::controlled_coordinator_smoke_rejects_ultra_before_persisting_workers",
                "worker_result_mailbox::tests::unread_lookup_and_claim_are_isolated_by_every_exact_owner_axis",
                "worker_resume::tests::active_parent_quarantines_without_claiming_then_idle_schedules_once",
                "worker_resume::tests::restart_replays_same_claim_and_intent_without_duplication",
                "workers::tests::subagent_lifecycle_retry_does_not_duplicate_runtime_queue_item",
                "runtime_pipeline::tests::run_runtime_queue_once_suppresses_owner_mismatched_agent_final_outbox",
                "coordinator_resume::released_parent_schedules_one_durable_coordinator_resume_and_no_master_wakeup",
            ],
            promotion_gate: "Promote after heterogeneous siblings, marker-protected controlled admission that proves the exact channel session is current plus authoritative catalog rollout for the main lane and every child execution agent before persistence, atomic rollback, exact-owner isolation, active/released parent lease transitions, duplicate/restart recovery, mailbox acknowledgement, and child-final suppression pass in one staging candidate; live gateway control remains on the main lane.",
        },
        ScenarioMatrixEntry {
            id: "provider-request-acceleration",
            title: "Provider request acceleration command and service tier routing",
            changed_areas: vec![
                "channel runtime",
                "channel state",
                "turn planning",
                "codex runtime",
                "provider request policy",
            ],
            required_invariants: vec!["I2", "I7", "I8"],
            required_evidence: vec![
                "/fast status|on|off|fast|normal are command replies and do not enqueue model turns",
                "fast mode state is scoped by channel session and optional per-agent default",
                "Codex app-server fast mode is gated by the local model catalog serviceTiers metadata",
                "supported OpenAI/Codex app-server models request serviceTier=priority",
                "normal mode resets supported Codex app-server models with serviceTier=default",
                "unsupported models, unverified proxy providers, and non-Codex native routes do not receive serviceTier/speed fields",
            ],
            runnable_tests: vec![
                "channel_runtime::tests::channel_step_reports_and_switches_fast_mode_with_route_capability",
                "channel_runtime::tests::fast_request_policy_is_codex_model_catalog_gated",
                "channel_state::tests::applies_fast_mode_and_new_session_clears_it",
                "channel_state::tests::applies_global_fast_mode_as_agent_override",
                "turns::tests::turn_plan_applies_fast_mode_as_provider_request_policy",
                "turns::tests::turn_plan_uses_global_fast_mode_when_session_has_no_override",
                "codex_runtime::tests::codex_app_server_service_tier_uses_provider_request_policy",
            ],
            promotion_gate: "Promote only after command-plane `/fast` replies remain model-turn-free, model catalog gating proves supported and unsupported routes, Codex app-server JSON-RPC params carry camelCase serviceTier for supported routes, and live Telegram/Discord smokes show enabled on a Fast-capable model or unsupported on a non-capable model.",
        },
        ScenarioMatrixEntry {
            id: "rich-message-presentation",
            title: "Rich outbound presentation schema and safe renderers",
            changed_areas: vec![
                "runtime pipeline",
                "final outbox",
                "channel delivery",
                "media",
                "trace",
            ],
            required_invariants: vec!["I2", "I7", "I9", "I11", "I14"],
            required_evidence: vec![
                "old outbox JSON without presentation remains plain text",
                "semantic presentation schema validates fallback text, bounded blocks, safe URLs, media refs, and capability-gated actions",
                "Telegram render fixture escapes HTML and disables link previews by default",
                "Discord render fixture chunks under provider limit and keeps allowed_mentions.parse empty",
                "plain-final Markdown subset maps bold, inline code, safe links, and lists into harness-owned semantic render output",
                "rendered rich batches expose deterministic text/media/action units",
                "delivery presentation receipts capture provider render mode, fallback reason, and full-text-preserved status",
                "per-unit delivery receipts preserve partial rich-batch failures as retryable instead of delivered",
                "callback actions require provider capability and mark re-entry gating",
                "provider send helpers use adapter-rendered Telegram HTML and Discord content when presentation is present",
                "rich media attachmentIndex captions are delivered through provider attachment payloads",
                "ordinary successful agent replies are bridged to presentation payloads without progress/narration leakage",
            ],
            runnable_tests: vec![
                "rich_presentation::tests::legacy_channel_outbound_message_without_presentation_stays_plain_text",
                "rich_presentation::tests::plain_final_bridge_builds_safe_paragraph_and_code_blocks",
                "rich_presentation::tests::plain_final_bridge_renders_markdown_subset_as_semantic_blocks",
                "rich_presentation::tests::plain_final_bridge_maps_attachments_to_rendered_media_units",
                "rich_presentation::tests::rich_presentation_validation_fails_closed_for_unsafe_shapes",
                "rich_presentation::tests::telegram_render_fixture_escapes_html_and_disables_preview",
                "rich_presentation::tests::discord_render_fixture_splits_and_suppresses_mentions",
                "rich_presentation::tests::telegram_rendered_batch_gates_callback_actions_and_units",
                "rich_presentation::tests::discord_rendered_batch_accounts_chunks_and_action_units",
                "runtime_pipeline::tests::run_runtime_queue_once_records_agent_reply_outbox",
                "runtime_pipeline::tests::run_runtime_queue_once_keeps_media_attachments_in_rich_presentation",
                "runtime_pipeline::tests::already_recorded_completion_repair_keeps_progress_panel_out_of_final_outbox",
                "channel_delivery::tests::rich_delivery_receipt_records_units_and_retries_partial_failure",
                "channel_delivery::tests::delivery_receipt_without_presentation_field_stays_readable",
                "channel_delivery::tests::rich_delivery_rejects_delivered_receipt_when_any_unit_failed",
                "agent-harness-cli::tests::telegram_rich_sender_records_html_presentation_receipt",
                "agent-harness-cli::tests::telegram_rich_outbound_delivery_records_html_receipt_closed_loop",
                "agent-harness-cli::tests::telegram_rich_sender_falls_back_to_plain_on_validation_failure",
                "agent-harness-cli::tests::telegram_rich_sender_falls_back_to_plain_on_provider_failure",
                "agent-harness-cli::tests::discord_rich_sender_records_safe_markdown_presentation_receipt",
                "agent-harness-cli::tests::discord_rich_sender_falls_back_to_plain_on_provider_failure",
                "agent-harness-cli::tests::telegram_trusted_html_payload_keeps_renderer_output_unescaped",
                "agent-harness-cli::tests::discord_attachment_payload_uses_caption_without_mentions",
            ],
            promotion_gate: "Promote Package D only after schema/validation/render fixtures, default final-to-presentation bridge tests, rendered-batch accounting, provider-rich text/media delivery integration, partial-failure receipt semantics, and action capability gates pass. Live Telegram inline callbacks, Discord components, clicked-action ingress re-entry, artifact/provider URL redaction coverage for generated presentation payloads, and live preview remain separate gates.",
        },
        ScenarioMatrixEntry {
            id: "channel-media-delivery",
            title: "Channel media directive, policy, and provider delivery",
            changed_areas: vec![
                "runtime pipeline",
                "media delivery policy",
                "channel delivery",
                "Telegram adapter",
                "Discord adapter",
                "media",
                "codex runtime",
                "prompt",
            ],
            required_invariants: vec!["I2", "I7", "I9", "I11", "I13", "I14", "I16"],
            required_evidence: vec![
                "MEDIA directive parser extracts standalone, inline, quoted, and backticked absolute local paths while preserving unknown extensions",
                "MEDIA directives inside code fences, inline code, and blockquotes are ignored and not stripped",
                "media delivery policy rejects denied roots, unsupported paths, missing files, oversize files, and unsafe artifact refs with visible degradation plus receipts",
                "channel-output prompt contract tells agents how to attach files and how policy rejection behaves",
                "final-outbox media lint warns by default and can fail closed through media.lintFailClosed",
                "rich media artifactRef-only inbound and generated-image artifact refs resolve to attachment-backed media after policy evaluation",
                "Telegram sends photo/document/audio/voice/video and batches image albums in chunks of 10 with caption overflow text",
                "Discord batches multipart attachments with files[0..9] and disabled mentions",
                "Telegram inbound documents, voice/audio/video, static stickers, and reply media become bounded artifacts without provider URL/file-id leakage",
                "Discord referenced-message attachments are fetched through the same host-gated downloader and marked referenced",
                "native image input is config-gated and pending native image bloat triggers fresh-thread rollover before another turn",
            ],
            runnable_tests: vec![
                "runtime_pipeline::tests::split_outbound_media_directives_extracts_attachments",
                "runtime_pipeline::tests::outbound_media_parser_masks_protected_spans_and_preserves_unknown_tags",
                "runtime_pipeline::tests::rejected_outbound_media_directive_leaves_visible_note",
                "runtime_pipeline::tests::media_delivery_lint_warns_or_fails_closed_from_config",
                "runtime_pipeline::tests::rich_media_artifact_ref_resolves_to_attachment_backed_unit",
                "runtime_pipeline::tests::rich_media_generated_image_artifact_ref_resolves_to_attachment",
                "runtime_pipeline::tests::rich_media_artifact_ref_policy_rejects_oversize_resolved_path",
                "media_delivery_policy::tests::deliverable_extension_table_covers_core_media_kinds",
                "media_delivery_policy::tests::policy_accepts_workspace_file_and_rejects_denied_state_file",
                "prompt::tests::prompt_bundle_includes_channel_output_contract_once",
                "media::tests::prompt_rendering_uses_safe_relative_paths_and_redacts_provider_urls",
                "media::tests::codex_media_planner_model_attaches_only_when_native_enabled_and_path_contained",
                "codex_runtime::tests::context_preflight_rolls_over_bound_thread_native_image_bloat",
                "agent-harness-cli::tests::telegram_media_group_payload_uses_attach_files_and_first_caption",
                "agent-harness-cli::tests::telegram_rich_sender_batches_image_media_units_as_albums",
                "agent-harness-cli::tests::telegram_plain_sender_chunks_twelve_images_as_albums",
                "agent-harness-cli::tests::telegram_plain_sender_truncates_attachment_caption_and_sends_remainder",
                "agent-harness-cli::tests::discord_attachments_payload_batches_multiple_files_without_mentions",
                "agent-harness-cli::tests::discord_referenced_message_attachment_becomes_referenced_artifact",
                "telegram_media::tests::telegram_media_downloads_non_image_document_with_bounded_extraction",
                "telegram_media::tests::telegram_media_downloads_voice_as_audio_metadata_only",
                "telegram_media::tests::telegram_reply_to_message_media_is_referenced_provenance",
                "telegram_media::tests::telegram_static_webp_sticker_downloads_as_image_artifact",
            ],
            promotion_gate: "Promote only after parser/policy, lint, provider senders, inbound artifact hygiene, referenced-media provenance, native-input readiness, and context-bloat rollover tests pass for both channel adapters; live cutover keeps media.nativeImageInput and media.lintFailClosed default-off until post-cutover enablement.",
        },
        ScenarioMatrixEntry {
            id: "bounded-ledger-maintenance",
            title: "Bounded interactive receipt reads and isolated retention owner",
            changed_areas: vec![
                "runtime receipt history",
                "channel delivery receipt history",
                "progress history",
                "ledger maintenance supervisor owner",
            ],
            required_invariants: vec!["I3", "I5", "I9", "I25"],
            required_evidence: vec![
                "terminal append paths signal a coalesced maintenance wake without synchronously replaying or compacting history",
                "interactive readers return a committed snapshot or conservative result when an append/index lock is busy",
                "one-hundred-thousand-row receipt and progress histories compact without retaining unbounded hot payloads",
                "each ledger fails closed independently while the maintenance owner records an isolated warning and continues other ledgers",
                "supervisor dry-run includes the dedicated ledger-maintenance owner before live promotion",
            ],
            runnable_tests: vec![
                "ledger_maintenance::tests::maintenance_wake_coalesces_sequence_and_latest_reason",
                "ledger_maintenance::tests::normal_maintenance_leaves_missing_ledgers_untouched",
                "ledger_maintenance::tests::forced_maintenance_contains_one_ledger_failure_and_continues_independently",
                "runtime_receipt_history::tests::nonblocking_reader_returns_would_block_for_an_exclusive_history_lock",
                "channel_delivery::tests::outbox_plan_uses_last_committed_index_while_an_outbox_append_is_locked",
                "channel_delivery_history::tests::compaction_plan_handles_one_hundred_thousand_terminal_v2_records",
                "progress_history::tests::compacts_one_hundred_thousand_terminal_events_without_retaining_an_unbounded_hot_payload",
            ],
            promotion_gate: "Promote only after the isolated maintenance owner is present in candidate supervisor dry-run, large-history and busy-lock regressions are green, and a controlled live long-turn proves progress/final delivery ordering without compaction on the interactive path.",
        },
        ScenarioMatrixEntry {
            id: "supervisor-plan-configured-loop-parity",
            title: "Task Scheduler plan preserves configured channel loops with maintenance owner",
            changed_areas: vec![
                "Windows Task Scheduler plan",
                "configured Telegram loop inventory",
                "ledger maintenance cutover ownership",
            ],
            required_invariants: vec!["I26"],
            required_evidence: vec![
                "the authoritative harness config validates before plan generation",
                "a staged plan includes the main Telegram loop, every enabled configured Telegram loop, and ledger-maintenance-loop",
                "the generated start/stop bundle contains each configured channel loop and the maintenance owner",
                "candidate reconcile and generated Task Scheduler plan agree before live promotion",
            ],
            runnable_tests: vec![
                "supervisor::tests::plan_includes_enabled_configured_telegram_loop_with_ledger_maintenance",
                "supervisor_inventory::tests::inventory_accepts_isolated_ledger_maintenance_service_and_plans_owner",
                "agent-harness-cli::tests::supervisor_reconcile_all_includes_isolated_ledger_maintenance_owner",
            ],
            promotion_gate: "Promote only after a staged nine-owner bundle and candidate reconcile preserve configured channel loops, then post-cutover readback confirms all expected owner heartbeats are fresh.",
        },
    ]
}

pub fn run_public_hygiene(options: PublicHygieneOptions) -> io::Result<PublicHygieneReport> {
    let mut forbidden_hits = Vec::new();
    visit(&options.root, &mut |path| {
        let rendered = path.to_string_lossy().to_ascii_lowercase();
        if rendered.contains(".agent-harness")
            || rendered.contains("\\secrets\\")
            || rendered.contains("/secrets/")
            || rendered.contains(".review")
            || rendered.contains(".debug")
        {
            forbidden_hits.push(path.to_path_buf());
        }
    })?;
    Ok(PublicHygieneReport {
        schema: QUALITY_REPORT_SCHEMA,
        root: options.root,
        passed: forbidden_hits.is_empty(),
        forbidden_hits,
    })
}

pub fn release_checklist() -> ReleaseChecklist {
    ReleaseChecklist {
        schema: QUALITY_REPORT_SCHEMA,
        required_items: vec![
            "cargo fmt --all",
            "cargo test --workspace",
            "schema registry updated",
            "scenario matrix gate reviewed for changed components",
            "CHANGELOG.md updated",
            "docs/skills/help stale guidance review completed",
            "skill ecosystem changes passed per-agent source/allowlist/usage-prior isolation, selection, autonomous synthesis/apply, lint, guard, lifecycle, pack, doctor, and closed-loop scenario gates",
            "topology contract impact matrix reviewed for changed modules",
            "channel/runtime changes passed the agent-boundary scenario matrix",
            "runtime terminal-control changes passed sticky terminal, suppression idempotency, final progress surface silence, lease reconcile, prepared-terminalization, retry-fresh-id, and live ghost-queue replay checks",
            "Round16 control-plane lifecycle changes passed exact T/D/S/P/V/K/R/C selectors plus E2E-1..E2E-5, or retain an explicit no-cutover blocker for missing E2E fixture, staging, or live evidence",
            "prompt/memory changes passed /new task-boundary and per-agent memory recall checks",
            "channel session history search/retrieval changes passed lane-bound candidate classification checks",
            "openclaw-mem bridge ownership changes passed configured-bridge and fallback gates",
            "response/runtime changes passed final-surface separation checks, including stdout recovery without final_answer, read-only review evidence suppression, and skipped-permanent retirement for invalid final outbox rows",
            "rich-message presentation changes passed adapter-rendering, no-ping, escaping, multi-unit receipt, and action re-entry verified-or-deferred checks",
            "channel media delivery changes passed parser/policy, provider batching, inbound artifact hygiene, referenced-media, and native-image bloat scenario checks",
            "context rollover changes passed official-compact accounting and polluted-thread recovery checks",
            "virtual-session working-context changes passed resolver exact-lane, CLI read surface, root snapshot enrichment, and carry-forward inheritance checks",
            "progress delivery changes passed edit-volume replay checks",
            "progress final-order changes passed source final provider-delivery-before-terminal-progress replay checks",
            "progress ordering changes passed queue-local preemption, first-surface send, orphan-claim recovery, and terminal-control ghost-close replay checks",
            "progress source-authority changes passed non-fresh-cache defer, historical fresh-send suppression, and stop-file claim-release checks",
            "progress panel lane-cap heartbeat/current-step checks passed across channel platforms",
            "interactive receipt readers passed bounded-history, busy-lock, and isolated ledger-maintenance-owner scenario checks",
            "cron freshness changes passed config run-cap validation, cron-canon health/status warnings, deterministic catch-up, and stale-source sender suppression checks",
            "cron/backup incident hardening passed exact-source, timezone/calendar, per-entry timeout/attempt, pre-lease exhaustion, process-tree timeout, occurrence-idempotency, verified-retention, and zero-stale-catch-up checks",
            "Codex tool-use timeout changes passed bounded recovery checks",
            "Round18 delivery/runtime interruption changes passed Telegram overlong final chunking, terminal receipt precedence, permanent provider rejection, structured interrupted command, safe-rerun, prompt/resolver, and runtime failure wording checks",
            "artifact/context hygiene changes passed generic artifact prompt/progress redaction and Discord attachment extraction checks",
            "GPT-5.6 and multi-agent orchestration changes passed max-only reasoning, exact-lane prompt, heterogeneous-child, master-owned mailbox, lease-safe resume, and child-final suppression scenario gates",
            "public hygiene report passed",
            "rollback notes recorded",
            "staging healthz and trace samples captured",
        ],
    }
}

fn visit(root: &Path, on_path: &mut impl FnMut(&Path)) -> io::Result<()> {
    if !root.exists() {
        return Ok(());
    }
    on_path(root);
    if root.is_dir() {
        for entry in fs::read_dir(root)? {
            let entry = entry?;
            visit(&entry.path(), on_path)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn quality_catalogs_and_hygiene_report_are_actionable() {
        assert!(invariant_catalog().len() >= 19);
        let scenario_matrix = scenario_matrix_catalog();
        assert!(
            scenario_matrix
                .iter()
                .any(|entry| entry.id == "agent-boundary")
        );
        assert!(
            scenario_matrix
                .iter()
                .any(|entry| entry.id == "final-outbox-delivery-trace")
        );
        assert!(
            scenario_matrix
                .iter()
                .any(|entry| entry.id == "runtime-terminal-control")
        );
        assert!(
            scenario_matrix
                .iter()
                .any(|entry| entry.id == "restart-control-plane")
        );
        assert!(
            scenario_matrix
                .iter()
                .any(|entry| entry.id == "virtual-session-rollover")
        );
        assert!(
            scenario_matrix
                .iter()
                .any(|entry| entry.id == "codex-deadline-continuation")
        );
        assert!(
            scenario_matrix
                .iter()
                .any(|entry| entry.id == "skill-selection-anchor-dedupe")
        );
        assert!(
            scenario_matrix
                .iter()
                .any(|entry| entry.id == "progress-surface-volume")
        );
        assert!(
            scenario_matrix
                .iter()
                .any(|entry| entry.id == "multi-agent-memory-compartment")
        );
        assert!(
            scenario_matrix
                .iter()
                .any(|entry| entry.id == "rich-message-presentation")
        );
        assert!(
            schema_registry_entries()
                .iter()
                .any(|entry| entry.schema == "agent-harness.encrypted-vault.v1")
        );
        assert!(
            schema_registry_entries()
                .iter()
                .any(|entry| entry.schema == "agent-harness.codex-context-preflight.v1")
        );
        assert!(
            release_checklist()
                .required_items
                .contains(&"public hygiene report passed")
        );
        assert!(
            release_checklist()
                .required_items
                .contains(&"docs/skills/help stale guidance review completed")
        );
        assert!(
            release_checklist()
                .required_items
                .contains(&"topology contract impact matrix reviewed for changed modules")
        );
        assert!(
            release_checklist()
                .required_items
                .contains(&"channel/runtime changes passed the agent-boundary scenario matrix")
        );
        assert!(
            release_checklist().required_items.contains(&"runtime terminal-control changes passed sticky terminal, suppression idempotency, final progress surface silence, lease reconcile, prepared-terminalization, retry-fresh-id, and live ghost-queue replay checks")
        );
        assert!(release_checklist().required_items.contains(&"Round16 control-plane lifecycle changes passed exact T/D/S/P/V/K/R/C selectors plus E2E-1..E2E-5, or retain an explicit no-cutover blocker for missing E2E fixture, staging, or live evidence"));
        assert!(release_checklist().required_items.contains(
            &"prompt/memory changes passed /new task-boundary and per-agent memory recall checks"
        ));
        assert!(release_checklist().required_items.contains(
            &"openclaw-mem bridge ownership changes passed configured-bridge and fallback gates"
        ));
        assert!(
            release_checklist()
                .required_items
                .contains(&"response/runtime changes passed final-surface separation checks, including stdout recovery without final_answer, read-only review evidence suppression, and skipped-permanent retirement for invalid final outbox rows")
        );
        assert!(
            release_checklist()
                .required_items
                .contains(&"rich-message presentation changes passed adapter-rendering, no-ping, escaping, multi-unit receipt, and action re-entry verified-or-deferred checks")
        );
        assert!(
            release_checklist()
                .required_items
                .contains(&"progress delivery changes passed edit-volume replay checks")
        );
        assert!(release_checklist().required_items.contains(
            &"progress ordering changes passed queue-local preemption, first-surface send, orphan-claim recovery, and terminal-control ghost-close replay checks"
        ));
        assert!(release_checklist().required_items.contains(
            &"progress source-authority changes passed non-fresh-cache defer, historical fresh-send suppression, and stop-file claim-release checks"
        ));
        assert!(release_checklist().required_items.contains(
            &"progress panel lane-cap heartbeat/current-step checks passed across channel platforms"
        ));
        assert!(
            release_checklist()
                .required_items
                .contains(&"Codex tool-use timeout changes passed bounded recovery checks")
        );
        assert!(release_checklist().required_items.contains(&"Round18 delivery/runtime interruption changes passed Telegram overlong final chunking, terminal receipt precedence, permanent provider rejection, structured interrupted command, safe-rerun, prompt/resolver, and runtime failure wording checks"));
        assert!(release_checklist().required_items.contains(
            &"artifact/context hygiene changes passed generic artifact prompt/progress redaction and Discord attachment extraction checks"
        ));
        let invariant_ids: Vec<&str> = invariant_catalog().iter().map(|entry| entry.id).collect();
        assert!(invariant_ids.contains(&"I18"));
        assert!(invariant_ids.contains(&"I19"));
        for entry in &scenario_matrix {
            assert!(
                !entry.changed_areas.is_empty(),
                "scenario matrix entry {} must name changed areas",
                entry.id
            );
            assert!(
                !entry.required_evidence.is_empty(),
                "scenario matrix entry {} must name required evidence",
                entry.id
            );
            assert!(
                !entry.runnable_tests.is_empty(),
                "scenario matrix entry {} must point to runnable tests",
                entry.id
            );
            assert!(
                !entry.promotion_gate.trim().is_empty(),
                "scenario matrix entry {} must name a promotion gate",
                entry.id
            );
            for invariant in &entry.required_invariants {
                assert!(
                    invariant_ids.contains(invariant),
                    "scenario matrix entry {} references unknown invariant {}",
                    entry.id,
                    invariant
                );
            }
        }

        let root = temp_root("quality_catalogs_and_hygiene_report_are_actionable");
        fs::create_dir_all(root.join(".agent-harness").join("secrets")).unwrap();
        fs::write(
            root.join(".agent-harness").join("secrets").join("key.env"),
            "secret",
        )
        .unwrap();
        let report = run_public_hygiene(PublicHygieneOptions { root: root.clone() }).unwrap();
        assert!(!report.passed);
        assert!(!report.forbidden_hits.is_empty());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn virtual_session_rollover_catalog_promotes_cutover_b_s1_s5_pack() {
        let scenario_matrix = scenario_matrix_catalog();
        let entry = scenario_matrix
            .iter()
            .find(|entry| entry.id == "virtual-session-rollover")
            .expect("virtual-session-rollover matrix entry");

        for evidence in [
            "S1 Telegram same-agent continuation consumes resolver prompt context",
            "S2 Discord same-agent continuation consumes resolver prompt context",
            "S3 same platform/channel/user different agent is not cross-suppressed",
            "S4 /new closes the previous virtual session and starts a fresh task boundary",
            "S5 max-depth terminal-failed guard preserves exact-lane resolver evidence only",
        ] {
            assert!(
                entry.required_evidence.contains(&evidence),
                "missing Cutover B required evidence: {evidence}"
            );
        }
        for runnable in [
            "prompt::tests::prompt_bundle_includes_virtual_session_resolver_context_section",
            "prompt::tests::prompt_bundle_missing_reply_metadata_hint_uses_resolver_queue_ids",
            "channel_state::tests::new_session_command_closes_previous_virtual_session_record",
            "context_rollover::tests::virtual_session_thread_backfill_updates_matching_working_session",
            "runtime_pipeline::tests::polluted_thread_continuation_runs_at_terminal_failure_and_respects_depth_limit",
        ] {
            assert!(
                entry.runnable_tests.contains(&runnable),
                "missing Cutover B runnable test: {runnable}"
            );
        }
    }

    #[test]
    fn quality_catalogs_include_cron_freshness_canon_gate() {
        let scenario_matrix = scenario_matrix_catalog();
        let entry = scenario_matrix
            .iter()
            .find(|entry| entry.id == "cron-freshness-canon")
            .expect("cron freshness scenario matrix entry");

        assert!(entry.required_invariants.contains(&"I17"));
        assert!(
            entry.runnable_tests.contains(
                &"config::tests::validate_harness_config_accepts_cron_scheduler_run_caps"
            )
        );
        assert!(
            entry
                .runnable_tests
                .contains(&"dream_director::tests::dream_director_sender_suppresses_stale_source")
        );
        assert!(entry.runnable_tests.contains(
            &"dream_director::tests::dream_director_sender_sends_fresh_source_with_freshness_metadata"
        ));
        assert!(entry.promotion_gate.contains("cron freshness fixture"));
        assert!(release_checklist().required_items.contains(&"cron freshness changes passed config run-cap validation, cron-canon health/status warnings, deterministic catch-up, and stale-source sender suppression checks"));
    }

    #[test]
    fn quality_catalogs_include_restart_control_plane_gate() {
        let scenario_matrix = scenario_matrix_catalog();
        let entry = scenario_matrix
            .iter()
            .find(|entry| entry.id == "restart-control-plane")
            .expect("restart-control-plane matrix entry");

        assert!(entry.required_invariants.contains(&"I17"));
        assert!(entry.runnable_tests.contains(
            &"agent-harness-cli::tests::e2e_4_restart_gateway_staging_closed_loop_replay"
        ));
        assert!(
            entry
                .promotion_gate
                .contains("real supervised child restart")
        );
    }

    #[test]
    fn quality_catalogs_register_gpt56_prompt_and_coordination_contracts() {
        let invariant_ids = invariant_catalog()
            .into_iter()
            .map(|entry| entry.id)
            .collect::<std::collections::HashSet<_>>();
        for invariant in ["I21", "I22", "I23", "I24", "I25", "I26", "I27"] {
            assert!(
                invariant_ids.contains(invariant),
                "missing orchestration invariant {invariant}"
            );
        }

        let schemas = schema_registry_entries()
            .into_iter()
            .map(|entry| entry.schema)
            .collect::<std::collections::HashSet<_>>();
        for schema in [
            "agent-harness.runtime-queue-item.v2",
            "agent-harness.agent-prompt-manifest.v1",
            "agent-harness.child-execution-policy.v1",
            "agent-harness.worker-result-owner.v1",
            "agent-harness.worker-result-envelope.v1",
            "agent-harness.worker-result-mailbox.v1",
            "agent-harness.worker-coordinator-wait.v1",
            "agent-harness.worker-resume-intent.v1",
            "agent-harness.coordinator-resume.v1",
            "agent-harness.safe-resume-readiness.v1",
            "agent-harness.ledger-maintenance.v1",
            "agent-harness.progress-delivery-plan.v1",
        ] {
            assert!(
                schemas.contains(schema),
                "missing schema registry entry {schema}"
            );
        }

        let scenarios = scenario_matrix_catalog();
        let reasoning = scenarios
            .iter()
            .find(|entry| entry.id == "gpt56-reasoning-capability")
            .expect("GPT-5.6 reasoning scenario");
        assert!(reasoning.required_invariants.contains(&"I21"));
        assert!(reasoning.runnable_tests.contains(
            &"channel_runtime::tests::think_and_reasoning_alias_share_one_last_write_wins_state"
        ));
        assert!(reasoning.runnable_tests.contains(
            &"channel_runtime::tests::exact_ultra_is_rejected_as_non_effort_for_both_command_aliases"
        ));

        let maintenance = scenarios
            .iter()
            .find(|entry| entry.id == "bounded-ledger-maintenance")
            .expect("bounded ledger maintenance scenario");
        assert!(maintenance.required_invariants.contains(&"I25"));
        assert!(maintenance.runnable_tests.contains(
            &"ledger_maintenance::tests::maintenance_wake_coalesces_sequence_and_latest_reason"
        ));

        let supervisor_plan = scenarios
            .iter()
            .find(|entry| entry.id == "supervisor-plan-configured-loop-parity")
            .expect("supervisor plan configured loop parity scenario");
        assert!(supervisor_plan.required_invariants.contains(&"I26"));
        assert!(supervisor_plan.runnable_tests.contains(
            &"supervisor::tests::plan_includes_enabled_configured_telegram_loop_with_ledger_maintenance"
        ));

        let progress = scenarios
            .iter()
            .find(|entry| entry.id == "progress-surface-volume")
            .expect("progress source-authority scenario");
        assert!(progress.required_invariants.contains(&"I27"));
        assert!(progress.runnable_tests.contains(
            &"progress::tests::progress_delivery_defers_cached_events_while_index_snapshot_is_not_fresh"
        ));
        assert!(progress.runnable_tests.contains(
            &"agent-harness-cli::tests::progress_delivery_stop_releases_unattempted_surface_claims_before_provider_io"
        ));

        let prompt = scenarios
            .iter()
            .find(|entry| entry.id == "per-agent-prompt-manifest")
            .expect("per-agent prompt manifest scenario");
        assert!(prompt.required_invariants.contains(&"I22"));
        assert!(prompt.runnable_tests.contains(
            &"turns::tests::prompt_file_aliases_are_fallback_only_and_conflicts_are_deterministic"
        ));

        let coordination = scenarios
            .iter()
            .find(|entry| entry.id == "master-owned-subagent-coordination")
            .expect("master-owned coordination scenario");
        assert!(coordination.required_invariants.contains(&"I23"));
        assert!(coordination.required_invariants.contains(&"I24"));
        assert!(coordination.runnable_tests.contains(
            &"worker_adapters::tests::controlled_coordinator_smoke_enqueues_only_two_catalog_admitted_children"
        ));
        assert!(coordination.runnable_tests.contains(
            &"worker_adapters::tests::controlled_coordinator_smoke_rejects_stale_live_session_before_persisting_workers"
        ));
        assert!(coordination.runnable_tests.contains(
            &"worker_adapters::tests::controlled_coordinator_smoke_rejects_non_authoritative_child_agent_before_persisting_workers"
        ));
        assert!(coordination
            .runnable_tests
            .contains(&"coordinator_resume::released_parent_schedules_one_durable_coordinator_resume_and_no_master_wakeup"));

        assert!(release_checklist().required_items.contains(
            &"GPT-5.6 and multi-agent orchestration changes passed max-only reasoning, exact-lane prompt, heterogeneous-child, master-owned mailbox, lease-safe resume, and child-final suppression scenario gates"
        ));
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-quality-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
