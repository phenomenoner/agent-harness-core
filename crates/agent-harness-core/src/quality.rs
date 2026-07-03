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
            statement: "terminal states are irreversible",
            owner: "runtime_pipeline/workers",
        },
        InvariantEntry {
            id: "I4",
            statement: "cancel only affects the requested turn, queue item, job, or scope",
            owner: "admission/channel_state",
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
            statement: "agent identity and /new task boundaries are routing boundaries across channel state, session freshness, prompt, runtime, outbox, delivery, and memory namespaces",
            owner: "channel_state/runtime_pipeline/prompt/memory",
        },
        InvariantEntry {
            id: "I9",
            statement: "final channel replies exclude progress/narration stream content and review-only evidence when the parent workflow has not completed",
            owner: "runtime_pipeline/progress/channel_delivery",
        },
        InvariantEntry {
            id: "I10",
            statement: "active Codex tool-use idle timeouts are stopped and routed through bounded recovery or an explicit terminal trace instead of directly dead-lettering the parent task",
            owner: "codex_runtime/runtime_pipeline/trace",
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
            statement: "inline-image, native-image-input, or oversized-output polluted Codex threads rotate to fresh-thread recovery before the next turn, while successful official compaction and repeated high-usage stream-unstable retries still feed bounded context-rollover continuity",
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
    ]
}

pub fn schema_registry_entries() -> Vec<SchemaRegistryEntry> {
    vec![
        SchemaRegistryEntry {
            schema: "agent-harness.runtime-run-once.v1",
            owner_module: "runtime_pipeline",
            compatibility: "append-only JSONL, additive fields only in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.codex-runtime-run.v1",
            owner_module: "codex_runtime",
            compatibility: "append-only JSONL plus per-execution JSON; v1 accepts additive recovery fields such as toolUseTimeout and contextRecovery.threadHealthStatus",
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
            compatibility: "append-only receipts; retry creates fresh ids",
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
            compatibility: "state JSON may add cursor/cache/counter fields in v1; existing lane cursors remain readable",
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
            schema: "agent-harness.skill-invocation-envelope.v1",
            owner_module: "skill_envelope",
            compatibility: "byte-framed envelope; declared length/checksum fields are stable in v1",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.skill-selection.v1",
            owner_module: "skills",
            compatibility: "append-only selection receipts; matcher metadata and score components may add fields in v1",
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
            compatibility: "derived compact status artifact; rebuildable from skill-usage JSONL",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.skill-proposal.v1",
            owner_module: "skill_learning",
            compatibility: "append-only proposal state records; apply requires checksum match and explicit operator action",
        },
        SchemaRegistryEntry {
            schema: "agent-harness.skill-apply-receipt.v1",
            owner_module: "skill_apply",
            compatibility: "append-only apply receipts; stale-base quarantine semantics are stable in v1",
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
                "structured owner/session routing suppresses non-main or mismatched-run completed output from final outbox",
                "invalid or suppressed outbox rows can be retired with skipped-permanent receipts without counting as delivered",
            ],
            runnable_tests: vec![
                "channel_delivery::tests::outbox_plan_treats_permanent_skip_as_terminal_not_delivered",
                "runtime_pipeline::tests::final_outbox_input_kind_suppresses_read_only_review_only_for_workflow_requests",
                "runtime_pipeline::tests::already_recorded_completion_repair_keeps_progress_panel_out_of_final_outbox",
                "runtime_pipeline::tests::already_recorded_completion_repair_keeps_progress_panel_out_of_discord_final_outbox",
                "runtime_pipeline::tests::run_runtime_queue_once_suppresses_read_only_review_final_for_implementation_goal",
                "runtime_pipeline::tests::run_runtime_queue_once_suppresses_non_main_agent_final_outbox",
                "trace::tests::trace_harness_event_detects_terminal_runtime_status",
            ],
            promotion_gate: "Prove completed turns converge to one final/terminal surface with reconstructable queue-to-delivery trace.",
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
                "repeated high-usage stream disconnect can requeue a guarded continuation before terminal dead-letter",
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
                "runtime_pipeline::tests::stream_unstable_retry_continuation_requires_repeated_high_usage_stream_failure",
                "runtime_pipeline::tests::repeated_stream_disconnect_high_usage_retry_requeues_continuation",
                "runtime_pipeline::tests::stream_unstable_retry_continuation_tombstones_parent_queue_item",
                "prompt::tests::prompt_bundle_new_command_boundary_skips_prior_task_memory_context",
                "prompt::tests::prompt_bundle_includes_virtual_session_resolver_context_section",
                "prompt::tests::prompt_bundle_missing_reply_metadata_hint_uses_resolver_queue_ids",
                "channel_state::tests::new_session_command_closes_previous_virtual_session_record",
                "context_rollover::tests::virtual_session_thread_backfill_updates_matching_working_session",
            ],
            promotion_gate: "Force high-context compact rollover and repeated high-usage stream-unstable retry scenarios, then prove rollover/final-delivery parity across Discord/TG identity axes.",
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
            required_invariants: vec!["I2", "I7", "I9", "I13"],
            required_evidence: vec![
                "bounded per-queue body/action edit volume",
                "status/current-step heartbeat after body cap",
                "immediate current-step update for new assistant execution summary",
                "repeated text-hash suppression",
                "terminal progress convergence",
                "no post-terminal edit churn",
                "final outbox contains only final answer",
                "internal worker results summarized without leaking raw worker final text into action progress",
            ],
            runnable_tests: vec![
                "progress::tests::progress_surface_volume_replay_converges_without_post_terminal_churn",
                "progress::tests::delivery_plan_status_heartbeat_after_body_cap_is_channel_agnostic",
                "progress::tests::delivery_plan_status_updates_immediately_for_new_current_step_after_body_cap",
                "progress::tests::action_stream_summarizes_internal_worker_results_instead_of_raw_final_text",
                "progress::tests::action_stream_summarizes_structured_subagent_notifications_without_source_label",
                "codex_runtime::tests::run_codex_runtime_rejects_stdout_recovery_narration_without_final_answer",
            ],
            promotion_gate: "Replay Telegram and Discord long-running turns through progress caps, recovery, final outbox, and terminal convergence.",
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
                "non-main agent completed output sharing platform/channel/user axes is internal evidence, not parent final outbox",
                "final outbox and delivery receipts preserve agent/lane ownership",
            ],
            runnable_tests: vec![
                "channel_state::tests::new_session_records_agent_from_new_session_key",
                "channel_state::tests::applies_per_agent_global_model_and_thinking_overrides",
                "prompt::tests::prompt_bundle_hides_main_operation_plan_from_other_agent",
                "runtime_worker::tests::prepare_runtime_queue_item_respects_agent_channel_lease_limit",
                "runtime_pipeline::tests::channel_session_freshness_does_not_cross_suppress_other_agent",
                "runtime_pipeline::tests::run_runtime_queue_once_suppresses_non_main_agent_final_outbox",
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
            "topology contract impact matrix reviewed for changed modules",
            "channel/runtime changes passed the agent-boundary scenario matrix",
            "prompt/memory changes passed /new task-boundary and per-agent memory recall checks",
            "channel session history search/retrieval changes passed lane-bound candidate classification checks",
            "openclaw-mem bridge ownership changes passed configured-bridge and fallback gates",
            "response/runtime changes passed final-surface separation checks, including stdout recovery without final_answer, read-only review evidence suppression, and skipped-permanent retirement for invalid final outbox rows",
            "rich-message presentation changes passed adapter-rendering, no-ping, escaping, multi-unit receipt, and action re-entry verified-or-deferred checks",
            "channel media delivery changes passed parser/policy, provider batching, inbound artifact hygiene, referenced-media, and native-image bloat scenario checks",
            "context rollover changes passed official-compact accounting and polluted-thread recovery checks",
            "virtual-session working-context changes passed resolver exact-lane, CLI read surface, root snapshot enrichment, and carry-forward inheritance checks",
            "progress delivery changes passed edit-volume replay checks",
            "progress panel lane-cap heartbeat/current-step checks passed across channel platforms",
            "Codex tool-use timeout changes passed bounded recovery checks",
            "artifact/context hygiene changes passed generic artifact prompt/progress redaction and Discord attachment extraction checks",
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
        assert!(invariant_catalog().len() >= 11);
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
                .any(|entry| entry.id == "virtual-session-rollover")
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
            &"progress panel lane-cap heartbeat/current-step checks passed across channel platforms"
        ));
        assert!(
            release_checklist()
                .required_items
                .contains(&"Codex tool-use timeout changes passed bounded recovery checks")
        );
        assert!(release_checklist().required_items.contains(
            &"artifact/context hygiene changes passed generic artifact prompt/progress redaction and Discord attachment extraction checks"
        ));
        let invariant_ids: Vec<&str> = invariant_catalog().iter().map(|entry| entry.id).collect();
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
