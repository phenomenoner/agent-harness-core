use std::fs;
use std::io;
use std::path::{Path, PathBuf};

pub mod activation;
pub mod admission;
pub mod autonomy;
pub mod backend_reasoning;
pub(crate) mod backend_reasoning_execution;
pub mod background;
pub mod channel_commands;
pub mod channel_delivery;
pub mod channel_identity;
pub mod channel_ingress;
pub mod channel_pipeline;
pub mod channel_runtime;
pub mod channel_state;
pub mod child_execution_policy;
pub(crate) mod codex_capability;
pub mod codex_runtime;
pub mod config;
pub mod context_rollover;
pub mod cron;
pub mod cron_runs;
pub mod cron_scheduler;
pub mod deploy;
pub mod deterministic_cron;
pub mod dream_director;
pub mod harness_registry;
pub mod harness_skills;
pub mod health;
pub mod importer;
pub mod lane;
pub mod latency;
pub mod live_control;
pub mod logging;
pub mod loop_diagnostics;
pub mod loop_health;
pub mod mcp;
pub mod media;
pub mod media_delivery_policy;
pub mod memory;
pub mod memory_backfill;
pub mod memory_contracts;
pub mod memory_owner;
pub mod memory_pack;
pub mod metrics;
pub mod model_catalog;
pub mod nudge;
pub mod operation_plan;
pub mod ops;
pub mod progress;
pub mod prompt;
pub mod quality;
pub mod queue_shadow;
pub mod registry;
pub mod response_tone;
pub mod rich_presentation;
pub mod runtime_pipeline;
pub mod runtime_policy;
pub mod runtime_queue;
pub mod runtime_worker;
pub mod security;
pub mod self_improvement;
pub mod skill_apply;
pub mod skill_curator;
pub mod skill_doctor;
pub mod skill_envelope;
pub mod skill_guard;
pub mod skill_learning;
pub mod skill_lint;
pub mod skill_matcher;
pub mod skill_pack;
pub mod skill_synthesis;
pub mod skill_usage;
pub mod skill_view;
pub mod skills;
pub mod status;
pub mod subagent_lifecycle;
pub mod subagents;
pub mod supervision;
pub mod supervisor;
pub mod supervisor_inventory;
pub mod token_efficiency;
pub mod trace;
pub mod turns;
pub mod vault;
pub mod virtual_session_context;
pub mod wake;
pub mod worker_adapters;
pub mod worker_result_mailbox;
pub mod workers;

pub use activation::{
    ActivationReadinessCheck, ActivationReadinessOptions, ActivationReadinessReport,
    ActivationReadinessStatus, ActivationReadinessSummary, check_activation_readiness,
};
pub use admission::{
    AdmissionDecisionOptions, AdmissionDecisionReport, ScopedStopOptions, ScopedStopReceipt,
    ScopedStopTarget, evaluate_admission, record_scoped_stop,
};
pub use autonomy::{
    BudgetAcquireOptions, BudgetDecisionReport, DriftCheckOptions, DriftReport,
    LearningProposalOptions, LearningProposalReport, LearningProposalStatus, TaskEntity,
    TaskEntityOptions, TaskStatus, acquire_budget, check_config_drift, create_learning_proposal,
    write_task_entity,
};
pub use background::{
    BackgroundTaskListOptions, BackgroundTaskRecord, BackgroundTaskRegistryReport,
    BackgroundTaskStatus, BackgroundTaskUpsertOptions, BackgroundTaskView, list_background_tasks,
    upsert_background_task,
};
pub use channel_commands::{
    ChannelCommand, ChannelCommandIntent, DEFAULT_THINKING_LEVEL, FastCommandMode, THINKING_LEVELS,
    XHIGH_THINKING_LEVEL, normalize_thinking_level, parse_channel_command,
    parse_channel_command_intent,
};
pub use channel_delivery::{
    ChannelDeliveryPending, ChannelDeliveryPresentationFallbackReason,
    ChannelDeliveryPresentationReceipt, ChannelDeliveryReceipt, ChannelDeliveryRecordOptions,
    ChannelDeliveryRenderedUnitKind, ChannelDeliveryRenderedUnitReceipt, ChannelDeliveryStatus,
    ChannelDeliveryUnitStatus, ChannelOutboxPlanOptions, ChannelOutboxPlanReport,
    ChannelOutboxPlanSummary, plan_channel_outbox, record_channel_delivery,
};
pub use channel_identity::{
    ChannelIdentityBinding, ChannelIdentityLookup, ChannelIdentityRegistry,
    ChannelIdentityResolution, ChannelIdentityResolutionStatus,
    channel_identity_registry_candidates, resolve_channel_identity,
};
pub use channel_ingress::{
    ChannelReceiveOptions, ChannelReceiveReceipt, ChannelReceiveReport, ChannelReceiveStatus,
    receive_channel_message,
};
pub use channel_pipeline::{
    ChannelRunOnceOptions, ChannelRunOnceReport, ChannelRunOnceStatus, run_channel_once,
};
pub use channel_runtime::{
    ChannelAgentTurnDispatch, ChannelCommandEffect, ChannelDeliveryIntent,
    ChannelDeliveryIntentKind, ChannelOutboundAttachment, ChannelOutboundAttachmentKind,
    ChannelOutboundMessage, ChannelOutboundMessageKind, ChannelStatusSnapshot, ChannelStep,
    ChannelStepAction, ChannelStepFile, FastRequestRoutePolicy, build_channel_step,
    fast_request_policy_for_route, write_channel_step,
};
pub use channel_state::{
    AgentOverride, AgentOverridesStore, ChannelCommandApplyOptions, ChannelCommandApplyReceipt,
    ChannelCommandApplyReceiptStatus, ChannelCommandApplyReport, ChannelCommandEvent,
    ChannelSessionNote, ChannelSessionState, agent_overrides_file, apply_channel_command_step,
    channel_session_state_file, read_agent_override, read_channel_session_state,
};
pub use codex_runtime::{
    AssistantNarrationConfig, AssistantNarrationMode, CodexApprovalPolicy,
    CodexApprovalPolicyInspection, CodexAssistantNarration,
    CodexBackendReasoningExecutionReference, CodexEnvRequirement, CodexInvocationPlan,
    CodexOutputPlan, CodexProviderConfig, CodexRuntimeCompletionOptions,
    CodexRuntimeCompletionReceipt, CodexRuntimeCompletionReport, CodexRuntimeCompletionStatus,
    CodexRuntimeLaunchProbeOptions, CodexRuntimeLaunchProbeReceipt, CodexRuntimeLaunchProbeReport,
    CodexRuntimeLaunchProbeStatus, CodexRuntimeLaunchProcess, CodexRuntimePlan,
    CodexRuntimePlanOptions, CodexRuntimePlanReport, CodexRuntimePreflightCheck,
    CodexRuntimePreflightCheckStatus, CodexRuntimePreflightOptions, CodexRuntimePreflightReceipt,
    CodexRuntimePreflightReport, CodexRuntimePreflightStatus, CodexRuntimeReceipt,
    CodexRuntimeReceiptStatus, CodexRuntimeRunOptions, CodexRuntimeRunReceipt,
    CodexRuntimeRunReport, CodexRuntimeRunStatus, CodexSandboxInspection, CodexTransportPlan,
    CodexTurnSteerQueueStatus, CodexTurnSteerRequestOptions, CodexTurnSteerRequestReport,
    inspect_codex_approval_policy, inspect_codex_sandbox, inspect_codex_sandbox_policy,
    load_assistant_narration_config, plan_codex_runtime, preflight_codex_runtime,
    probe_codex_runtime_launch, queue_codex_turn_steer_request, record_codex_runtime_completion,
    run_codex_runtime,
};
pub use config::{
    HARNESS_CONFIG_FILE_NAME, HarnessConfigValidationReport, HarnessConfigValidationStatus,
    harness_config_candidates, validate_harness_config,
};
pub use context_rollover::{
    CompletedTurnWorkingSetSnapshotOptions, CompletedTurnWorkingSetSnapshotReport,
    ContextCompactAttemptOptions, ContextCompactCounter, ContextCompactCounterOptions,
    ContextRolloverBeforeTurnOptions, ContextRolloverConfig, ContextRolloverEpisode,
    ContextRolloverLane, ContextRolloverMode, ContextRolloverPreparedRequeueReport,
    ContextRolloverReceipt, ContextRolloverRequeuePreparedOptions, ContextRolloverStatus,
    ContextStaticRecordRefs, ContextVirtualSessionRecord, ContextWorkingSetGoal,
    ContextWorkingSetMemory, RuntimeContinuationMetadata, VirtualSessionTaskBoundaryCloseOptions,
    VirtualSessionTerminalOptions, VirtualSessionThreadBackfillOptions,
    apply_context_rollover_before_turn, backfill_virtual_session_codex_thread_id,
    close_virtual_session_for_task_boundary, context_compact_counter_file,
    context_rollover_episode_index_file, context_rollover_prepared_requeues_file,
    context_rollover_receipts_file, continuation_session_key, is_rollover_completion_kind,
    load_context_rollover_config, load_or_create_context_compact_counter,
    load_working_set_continuity_section, mark_virtual_session_terminal, parse_rollover_mode,
    planned_session_files, record_completed_turn_working_set_snapshot,
    record_context_compact_attempt, requeue_prepared_context_rollover,
    requeue_prepared_context_rollover_if_no_parent_siblings, root_working_session_key,
    working_set_session_index_file,
};
pub use cron::{
    NativeCronJob, NativeCronJobState, NativeCronPlan, NativeCronPlanAction, NativeCronPlanEntry,
    NativeCronPlanFile, NativeCronPlanInput, NativeCronPlanSummary, NativeCronSchedule,
    NativeCronStore, NativeCronStoreSummary, load_native_cron_store, plan_native_cron,
    write_native_cron_plan,
};
pub use cron_runs::{
    CronRun, CronRunAdmitOptions, CronRunControlAction, CronRunControlOptions,
    CronRunControlReport, CronRunListOptions, CronRunListReport, CronRunStatus, CronRunSummary,
    admit_cron_run, collect_cron_run_summary, control_cron_run, cron_run_active_count_for_agent,
    cron_run_active_count_for_job, cron_run_id, cron_run_is_quarantined,
    cron_run_runtime_dispatch_blocker, cron_run_worker_dispatch_blocker, cron_runs_db_file,
    get_cron_run_by_slot, init_cron_run_store, list_cron_runs, mark_cron_run_runtime_enqueued,
    mark_cron_run_runtime_status_by_queue_id, mark_cron_run_worker_enqueued,
    mark_cron_run_worker_status,
};
pub use cron_scheduler::{
    CronSchedulerConfig, CronSchedulerDeterministicConfig, CronSchedulerJobDecision,
    CronSchedulerJobDecisionStatus, CronSchedulerLintFinding, CronSchedulerLintReport,
    CronSchedulerLintSeverity, CronSchedulerLintStatus, CronSchedulerLintSummary,
    CronSchedulerLoopOptions, CronSchedulerNativeConfig, CronSchedulerRunOnceOptions,
    CronSchedulerRunOnceReport, CronSchedulerTickReceipt, CronSchedulerTickStatus,
    CronSchedulerTickSummary, lint_cron_scheduler, run_cron_scheduler_once,
};
pub use deploy::{
    SuperviseDeployCanaryOptions, SuperviseDeployCanaryReport, SuperviseDeployDecision,
    record_supervise_deploy_canary,
};
pub use deterministic_cron::{
    DeterministicCronEntry, DeterministicCronPlan, DeterministicCronPlanAction,
    DeterministicCronPlanEntry, DeterministicCronPlanFile, DeterministicCronPlanInput,
    DeterministicCronPlanSummary, DeterministicCronRunner, DeterministicCronRunnerKind,
    DeterministicCronSchedule, DeterministicCronStore, DeterministicCronStoreSummary,
    load_deterministic_cron_store, plan_deterministic_cron, write_deterministic_cron_plan,
};
pub use dream_director::{
    DEFAULT_DREAM_DIRECTOR_MAX_CHARS, DEFAULT_DREAM_DIRECTOR_SOURCE_MAX_AGE_HOURS,
    DREAM_DIRECTOR_SEND_RECEIPT_SCHEMA, DreamDirectorSendOptions, DreamDirectorSendReceipt,
    DreamDirectorSendReport, dream_director_daily_state_dir, dream_director_send_receipt_file,
    run_dream_director_send,
};
pub use harness_registry::{
    CredentialStatus, HarnessAgent, HarnessPlugin, HarnessProvider, HarnessRegistry,
    HarnessRegistryExport, HarnessRegistryReceipt, HarnessRegistryReceiptFile,
    HarnessRegistryReceiptKind, HarnessRegistryReceiptStatus, build_harness_registry,
    export_harness_registry_files,
};
pub use harness_skills::{
    BuiltinHarnessSkillSyncOptions, BuiltinHarnessSkillSyncReceipt, BuiltinHarnessSkillSyncReport,
    BuiltinHarnessSkillSyncStatus, BuiltinHarnessSkillSyncSummary,
    builtin_harness_skill_manifest_file, sync_builtin_harness_skills,
};
pub use health::{
    HealthzLoop, HealthzOptions, HealthzOutbox, HealthzQueue, HealthzReport, HealthzSkills,
    HealthzState, collect_healthz,
};
pub use importer::{
    ConfigSemantics, ConflictPolicy, DryRunImportOptions, ExecuteImportOptions, ImportAction,
    ImportExecuteReceipt, ImportExecuteReport, ImportExecuteStatus, ImportExecuteSummary,
    ImportItem, ImportItemKind, ImportItemStatus, ImportReport, ImportReportSummary,
    ImportSemantics, NativeCronSemantics, ReportFiles, SessionSemantics, build_dry_run_report,
    execute_import, write_report_files,
};
pub use live_control::{
    LiveControlAction, LiveControlIntent, LiveControlTokenRecord, LiveControlTokenStatus,
    LiveControlTokenValidation, classify_approval_request, classify_live_control_command,
    env_live_control_token, hash_live_control_token, is_live_agent_session_env,
    is_live_harness_home, live_control_tokens_file, validate_live_control_token,
};
pub use logging::{
    HarnessLogEvent, HarnessLogLevel, HarnessLogRotationOptions, HarnessLogRotationReport,
    HarnessLogRotationStatus, HarnessLogWrite, append_harness_log, append_jsonl_value,
    append_jsonl_value_once_by_event_key, current_log_time_ms, harness_log_file,
    probe_harness_log_writable, rotate_harness_log_if_needed, write_json_atomic,
};
pub use mcp::{McpRequestOptions, McpToolReceipt, handle_mcp_request};
pub use media::{
    ArtifactExtractionSummary, DEFAULT_INBOUND_MEDIA_MAX_BYTES_PER_ITEM,
    DEFAULT_INBOUND_MEDIA_MAX_ITEMS_PER_TURN, INBOUND_MEDIA_ARTIFACT_SCHEMA,
    INBOUND_MEDIA_CACHE_REPORT_SCHEMA, INBOUND_MEDIA_INPUT_PLAN_SCHEMA,
    INBOUND_MEDIA_SAFETY_REPORT_SCHEMA, INBOUND_MEDIA_VISION_ANALYSIS_SCHEMA, InboundMediaArtifact,
    InboundMediaCacheReport, InboundMediaDownloadStatus, InboundMediaInputPlan,
    InboundMediaInputPlanOptions, InboundMediaModelAttachmentStatus, InboundMediaNativeInputPart,
    InboundMediaSafetyPolicy, InboundMediaSafetyReport, InboundMediaSelectedVariant,
    InboundMediaVisionAnalysis, analyze_inbound_media_file, collect_inbound_media_cache_report,
    inbound_media_attachment_root, plan_inbound_media_inputs,
    render_inbound_media_artifacts_for_prompt, resolve_inbound_media_artifact_reference,
    validate_inbound_media_artifact_paths, validate_inbound_media_safety,
};
pub use media_delivery_policy::{
    DEFAULT_OUTBOUND_MEDIA_MAX_MB_PER_ATTACHMENT, DEFAULT_OUTBOUND_MEDIA_TRUST_RECENT_SECONDS,
    HarnessMediaConfig, MediaDeliveryEvaluation, MediaDeliveryLintConfig, MediaDeliveryPolicy,
    MediaDeliveryVerdict, OUTBOUND_MEDIA_POLICY_SCHEMA, OutboundMediaPolicyReceipt,
    all_deliverable_extensions, attachment_kind_from_extension, attachment_kind_from_path,
    attachment_mime_from_path, evaluate_outbound_media_path, is_deliverable_media_path,
    load_harness_media_config, media_policy_receipts_file, write_media_policy_receipt,
};
pub use memory::{
    MemoryAdapterReadinessReport, MemoryCanvasWorkerOptions, MemoryCanvasWorkerReport,
    MemoryCanvasWorkerStatus, MemoryCredentialBridgeReport, MemoryCredentialsExportEntry,
    MemoryCredentialsExportOptions, MemoryCredentialsExportReport, MemoryEmbeddingCoverageReport,
    MemoryGraphFreshnessOptions, MemoryGraphFreshnessReport, MemoryGraphFreshnessStatus,
    MemoryGraphReadinessReport, MemoryHookAdapterOptions, MemoryHookKind, MemoryHookReport,
    MemoryHookStatus, MemoryLifecycleReport, MemoryLifecycleStatus, MemoryLifecycleTurnOptions,
    MemoryMemEngineCanaryReport, MemoryMemEngineOwnershipReport, MemoryPromptContextOptions,
    MemoryPromptContextReport, MemoryPromptContextStatus, MemoryProvenanceChainOptions,
    MemoryProvenanceChainReport, MemoryProvenanceChainStatus, MemoryRecallPlanOptions,
    MemoryRecallPlanReport, MemoryRecallPlanStatus, MemoryRecallSourceBudget,
    MemoryScopePolicyReport, MemorySearchHit, MemorySearchOptions, MemorySearchReport,
    MemorySearchStatus, MemorySemanticCoverageLane, MemorySemanticCoverageReport,
    MemoryTrustPolicyReport, MemoryVectorHit, MemoryVectorRecallOptions, MemoryVectorRecallReport,
    MemoryVectorRecallStatus, OpenClawMemLocalOwnerPrepareOptions,
    OpenClawMemLocalOwnerPrepareReport, OpenClawMemReadPathSmokeOptions,
    OpenClawMemReadPathSmokeReport, OpenClawMemServiceHit, OpenClawMemServiceProposalReport,
    OpenClawMemServiceProposalStatus, OpenClawMemServiceProposeOptions,
    OpenClawMemServiceRecallOptions, OpenClawMemServiceRecallReport,
    OpenClawMemServiceRecallStatus, OpenClawMemServiceStatus, OpenClawMemServiceStatusOptions,
    OpenClawMemServiceStatusReport, OpenClawMemServiceStoreOptions, OpenClawMemServiceStoreReport,
    OpenClawMemServiceStoreStatus, build_memory_prompt_context, collect_memory_embedding_coverage,
    collect_memory_semantic_coverage, export_memory_credentials, inspect_openclaw_mem_service,
    memory_canvas_latest_file, memory_canvas_latest_file_for_agent, memory_canvas_receipts_file,
    memory_canvas_receipts_file_for_agent, memory_credentials_env_file,
    memory_credentials_receipt_file, memory_graph_freshness_latest_file,
    memory_graph_freshness_receipts_file, memory_hook_latest_file, memory_hook_receipts_file,
    memory_lifecycle_latest_file, memory_lifecycle_latest_file_for_agent,
    memory_lifecycle_receipts_file, memory_lifecycle_receipts_file_for_agent,
    memory_prompt_context_latest_file, memory_prompt_context_latest_file_for_agent,
    memory_prompt_context_receipts_file, memory_prompt_context_receipts_file_for_agent,
    memory_provenance_chain_latest_file, memory_provenance_chain_receipts_file,
    memory_recall_plan_latest_file, memory_recall_plan_receipts_file, memory_search_latest_file,
    memory_search_receipts_file, memory_slot_receipts_file, memory_store_proposals_file,
    memory_vector_recall_latest_file, memory_vector_recall_receipts_file,
    openclaw_mem_service_proposal_receipts_file_for_agent,
    openclaw_mem_service_proposals_file_for_agent,
    openclaw_mem_service_recall_latest_file_for_agent,
    openclaw_mem_service_recall_receipts_file_for_agent, openclaw_mem_service_status_latest_file,
    openclaw_mem_service_status_receipts_file, openclaw_mem_service_store_file_for_agent,
    openclaw_mem_service_store_receipts_file_for_agent, plan_memory_policy_recall,
    prepare_openclaw_mem_local_owner, propose_openclaw_mem_service_memory,
    recall_openclaw_mem_service, record_memory_graph_freshness, record_memory_lifecycle_turn,
    record_memory_provenance_chain, run_memory_canvas_worker, run_memory_hook_adapter,
    run_openclaw_mem_read_path_smoke, search_imported_memory, search_imported_vector_memory,
    search_imported_vector_memory_with_embedding, store_openclaw_mem_service_memory,
    write_memory_prompt_context_receipt, write_memory_search_receipt,
    write_memory_vector_recall_receipt,
};
pub use memory_backfill::{
    DEFAULT_MEMORY_BACKFILL_BATCH_SIZE, DEFAULT_MEMORY_BACKFILL_COVERAGE_THRESHOLD_BPS,
    DEFAULT_MEMORY_BACKFILL_MAX_ITEMS, DEFAULT_MEMORY_BACKFILL_RATE_LIMIT_PER_MINUTE,
    DEFAULT_MEMORY_BACKFILL_RETRY_CAP, DEFAULT_MEMORY_BACKFILL_VECTOR_DIMENSION,
    MEMORY_EMBEDDING_BACKFILL_CURSOR_SCHEMA, MEMORY_EMBEDDING_BACKFILL_SCHEMA,
    MemoryEmbeddingBackfillCursor, MemoryEmbeddingBackfillLane, MemoryEmbeddingBackfillOptions,
    MemoryEmbeddingBackfillReport, memory_embedding_backfill_cursor_file,
    memory_embedding_backfill_latest_file, memory_embedding_backfill_receipts_file,
    run_memory_embedding_backfill,
};
pub use memory_contracts::{
    AGENT_HARNESS_CONTEXT_PACK_SCHEMA, ContextPackChunk, ContextPackParseOptions,
    ContextPackParseReport, ContextPackV1, MemoryIngestDecision, OPENCLAW_MEM_CONTEXT_PACK_SCHEMA,
    ToolDescriptionPinReport, check_tool_description_pin, decide_memory_ingest, parse_context_pack,
    tool_description_hash,
};
pub use memory_owner::{
    DEFAULT_MEMORY_OWNER_HEARTBEAT_MAX_AGE_MS, MEM_ENGINE_OWNER,
    MEMORY_OWNER_ENDPOINT_PROBE_SCHEMA, MEMORY_OWNER_PROMOTION_RECEIPT_SCHEMA,
    MEMORY_OWNER_ROLLBACK_RECEIPT_SCHEMA, MEMORY_OWNER_SHADOW_RECEIPT_SCHEMA,
    MEMORY_OWNER_STATE_SCHEMA, MemoryOwnerEndpointProbeOptions, MemoryOwnerEndpointProbeReport,
    MemoryOwnerEndpointProbeState, MemoryOwnerEnsureOptions, MemoryOwnerHeartbeatOptions,
    MemoryOwnerHeartbeatReport, MemoryOwnerPromotionGates, MemoryOwnerPromotionOptions,
    MemoryOwnerPromotionReceipt, MemoryOwnerReceiptRef, MemoryOwnerRecoveryOptions,
    MemoryOwnerRollbackReceipt, MemoryOwnerShadowKind, MemoryOwnerShadowOptions,
    MemoryOwnerShadowReceipt, MemoryOwnerState, MemoryOwnerTrustScopeOptions,
    MemoryOwnerTrustScopeReceipt, OPENCLAW_MEM_LOCAL_IN_PROCESS_CONTRACT,
    OPENCLAW_MEM_REMOTE_SERVICE_CONTRACT, SNAPSHOT_MEMORY_OWNER, default_owner_state,
    ensure_memory_owner_state, memory_owner_endpoint_probe_receipts_file,
    memory_owner_heartbeat_receipts_file, memory_owner_promotion_receipts_file,
    memory_owner_rollback_receipts_file, memory_owner_shadow_receipts_file,
    memory_owner_state_file, memory_owner_trust_scope_receipts_file, read_memory_owner_state,
    read_memory_owner_state_or_default, record_memory_owner_endpoint_probe,
    record_memory_owner_heartbeat, record_memory_owner_shadow_receipt,
    record_memory_owner_trust_scope_receipt, recover_memory_owner_state,
    request_memory_owner_promotion,
};
pub use memory_pack::{
    PACK_ARTIFACT_MARKER_PREFIX, PACK_ARTIFACT_MARKER_SUFFIX, PACK_ARTIFACT_PUT_RECEIPT_SCHEMA,
    PACK_CANARY_REPORT_SCHEMA, PACK_OBSERVE_REPORT_SCHEMA, PACK_RECEIPT_SCHEMA,
    PACK_RETRIEVE_RECEIPT_SCHEMA, PackAdmissionConfig, PackArtifactMarker, PackArtifactMetadata,
    PackArtifactPutOptions, PackArtifactPutReport, PackArtifactRetrieveOptions,
    PackArtifactRetrieveReport, PackCanary, PackCanaryReport, PackCanaryValidationReport,
    PackCandidateOptions, PackCandidateReport, PackObserveReport, PackOmissionSummary,
    PackPolicyDecision, PackStrategyConfig, PackTtlPolicy, collect_pack_observe_report,
    pack_artifact_hash_for_bytes, pack_artifact_put_receipts_file,
    pack_artifact_retrieve_receipts_file, pack_artifact_store_file, pack_candidate,
    pack_observe_latest_file, pack_receipts_file, pack_strategy_config_file,
    parse_pack_artifact_marker, put_pack_artifact, read_pack_strategy_config,
    retrieve_pack_artifact, validate_pack_canary_schema, write_pack_strategy_config,
};
pub use metrics::{HarnessMetricsOptions, HarnessMetricsReport, collect_harness_metrics};
pub use nudge::{
    NudgeConfig, NudgeDueInfo, PerSessionNudgeState, advance_learning_nudge_counters,
    learning_nudge_state_file, load_memory_nudge_config, load_skill_nudge_config,
    reset_session_nudge_state, should_trigger_nudge,
};
pub use operation_plan::{
    CreateOperationPlanOptions, OperationPlan, OperationPlanAddItemOptions,
    OperationPlanAddItemReport, OperationPlanBlockOptions, OperationPlanBlockReport,
    OperationPlanComment, OperationPlanCommentOptions, OperationPlanCommentReport,
    OperationPlanCompleteOptions, OperationPlanCompleteReport, OperationPlanCreateReport,
    OperationPlanDelegateItemOptions, OperationPlanEvent, OperationPlanEventKind,
    OperationPlanItem, OperationPlanItemStatus, OperationPlanPromoteDependenciesOptions,
    OperationPlanPromoteDependenciesReport, OperationPlanReceipt, OperationPlanReceiptAction,
    OperationPlanShowOptions, OperationPlanShowReport, OperationPlanStatus, OperationPlanSummary,
    OperationPlanUpdateItemOptions, OperationPlanUpdateItemReport, add_operation_plan_item,
    block_operation_plan, comment_on_operation_plan, complete_operation_plan,
    create_operation_plan, delegate_operation_plan_item, list_operation_plans,
    promote_operation_plan_items_from_dependencies, show_operation_plan,
    update_operation_plan_item,
};
pub use ops::{
    OpsBackupEntry, OpsBackupOptions, OpsBackupReport, OpsControlAction, OpsControlOptions,
    OpsControlReport, OpsCutoverApplyOptions, OpsCutoverApplyReport, OpsCutoverApproveOptions,
    OpsCutoverApproveReport, OpsCutoverReceiptOptions, OpsCutoverReceiptReport,
    OpsCutoverRequestOptions, OpsCutoverRequestReport, OpsCutoverStatusOptions,
    OpsCutoverStatusReport, OpsStopFileStatus, collect_ops_cutover_status, create_ops_backup,
    record_ops_control, record_ops_cutover_apply, record_ops_cutover_approval,
    record_ops_cutover_receipt, record_ops_cutover_request,
};
pub use progress::{
    AgentProgressContext, AgentProgressDeliveryAction, AgentProgressDeliveryFreshSendReason,
    AgentProgressDeliveryMessageKind, AgentProgressDeliveryPending,
    AgentProgressDeliveryPlanOptions, AgentProgressDeliveryPlanReport,
    AgentProgressDeliveryPlanSummary, AgentProgressDeliveryReceipt,
    AgentProgressDeliveryRecordContext, AgentProgressDeliveryRecordOptions,
    AgentProgressDeliveryStatus, AgentProgressEvent, AgentProgressKind,
    AgentProgressSessionSupersedeOptions, AgentProgressSessionSupersedeReport, AgentProgressStatus,
    agent_progress_delivery_receipts_file, agent_progress_delivery_state_file,
    agent_progress_events_file, agent_progress_session_supersede_receipts_file,
    append_agent_progress_event, latest_agent_progress_event_line_for_queue,
    plan_agent_progress_delivery, record_agent_progress_delivery,
    record_agent_progress_delivery_with_context, release_agent_progress_surface_claim,
    render_agent_progress_panel, sanitize_progress_preview,
    supersede_agent_progress_session_surfaces,
};
pub use prompt::{
    PromptAssemblyOptions, PromptBundle, PromptBundleFiles, PromptBundleSummary, PromptSection,
    PromptSectionKind, PromptSectionTier, assemble_prompt_bundle, write_prompt_bundle,
};
pub use quality::{
    InvariantEntry, PublicHygieneOptions, PublicHygieneReport, ReleaseChecklist,
    ScenarioMatrixEntry, SchemaRegistryEntry, invariant_catalog, release_checklist,
    run_public_hygiene, scenario_matrix_catalog, schema_registry_entries,
};
pub use queue_shadow::{
    QueueShadowCompareOptions, QueueShadowCompareReport, QueueShadowDivergence,
    QueueShadowDivergenceKind, QueueShadowRecordOptions, QueueShadowRecordReport,
    compare_channel_turn_shadow, record_channel_turn_shadow,
};
pub use registry::{
    AgentDefaults, AgentProfile, AgentProfileSource, AgentRegistry, ChannelRegistry, PluginProfile,
    ProviderProfile, load_agent_registry,
};
pub use response_tone::{
    EmojiAccentMode, ResponseToneConfig, ResponseToneContext, apply_response_tone,
    load_response_tone_config, parse_emoji_accent_mode,
};
pub use rich_presentation::{
    RICH_MESSAGE_PRESENTATION_SCHEMA, RenderedDiscordPresentation, RenderedRichBatch,
    RenderedRichUnit, RenderedRichUnitKind, RenderedTelegramPresentation, RichMessagePresentation,
    RichPresentationAction, RichPresentationActionKind, RichPresentationAtomicity,
    RichPresentationBlock, RichPresentationDeliveryPolicy, RichPresentationField,
    RichPresentationLinkPreview, RichPresentationLinkPreviewMode, RichPresentationMediaRef,
    RichPresentationTextStyle, RichPresentationValidationError, RichPresentationValidationOptions,
    render_rich_presentation_batch_for_discord, render_rich_presentation_batch_for_telegram,
    render_rich_presentation_for_discord, render_rich_presentation_for_telegram,
    rich_presentation_from_plain_final, rich_presentation_from_plain_final_with_attachment_count,
    validate_rich_message_presentation,
};
pub use runtime_pipeline::{
    RuntimeRunOnceOptions, RuntimeRunOnceReceipt, RuntimeRunOnceReport, RuntimeRunOnceStatus,
    run_runtime_queue_once,
};
pub use runtime_policy::{
    RuntimeBackoffPolicy, RuntimeBackoffPolicyInspection, RuntimeProviderFallbackRule,
    inspect_runtime_backoff_policy,
};
pub use runtime_queue::{
    RuntimeQueueControlAction, RuntimeQueueControlOptions, RuntimeQueueControlReport,
    RuntimeQueueControlStatus, RuntimeQueueEnqueueOptions, RuntimeQueueEnqueueReport,
    RuntimeQueueItem, RuntimeQueueItemStatus, RuntimeQueueReceipt, RuntimeQueueReceiptStatus,
    RuntimeQueueSource, RuntimeQueueSourceKind, control_runtime_queue_item, enqueue_channel_step,
};
pub use runtime_worker::{
    RuntimeDispatchClassConfig, RuntimeDispatchConfig, RuntimeExecutionReceipt,
    RuntimeExecutionReceiptStatus, RuntimeQueueCapacityOptions, RuntimeQueueCapacityReport,
    RuntimeQueueClassCapacity, RuntimeQueuePrepareOptions, RuntimeQueuePrepareReport,
    RuntimeQueuePreparedItem, inspect_runtime_queue_capacity, load_runtime_dispatch_config,
    prepare_runtime_queue_item, release_runtime_queue_lease, write_runtime_queue_quarantine_marker,
};
pub use security::{SecurityScanOptions, SecurityScanReport, scan_security_boundaries};
pub use self_improvement::{
    SelfImprovementNotificationTarget, SelfImprovementReviewConfig,
    SelfImprovementReviewHookOptions, SelfImprovementReviewHookReport, SelfImprovementReviewMode,
    append_self_improvement_notification, load_self_improvement_review_config,
    run_self_improvement_review_hook, self_improvement_review_receipts_file,
};
pub use skill_apply::{
    SkillApplyOptions, SkillApplyReport, SkillApplyStatus, SkillAutonomousApplyOptions,
    SkillAutonomousApplyReport, SkillAutonomousReviewDecision, SkillProposalActionOptions,
    SkillProposalActionReport, SkillProposalActionStatus, SkillProposalListOptions,
    SkillProposalListReport, apply_skill_proposal, autonomous_apply_skill_proposal,
    list_skill_proposals, reject_skill_proposal, skill_apply_receipts_file,
    skill_autonomous_apply_receipts_file,
};
pub use skill_curator::{
    SkillCuratorOptions as SkillLifecycleCuratorOptions,
    SkillCuratorReport as SkillLifecycleCuratorReport, SkillLifecycleRecord, SkillLifecycleState,
    SkillLifecycleStore, SkillPinOptions, SkillPinReport, SkillRestoreOptions, SkillRestoreReport,
    mark_skill_archived, move_skill_to_archive, read_skill_lifecycle_store,
    restore_skill_from_archive, run_skill_curator as run_skill_lifecycle_curator, set_skill_pin,
    skill_curator_receipts_dir, skill_lifecycle_file,
};
pub use skill_doctor::{
    SkillDoctorFinding, SkillDoctorOptions, SkillDoctorReport, SkillDoctorStatus,
    SkillDoctorSummary, run_skill_doctor, skill_doctor_receipts_file,
};
pub use skill_envelope::{
    SKILL_INVOCATION_ENVELOPE_SCHEMA, SkillEnvelopeError, SkillInvocationEnvelope,
    extract_user_instruction_from_skill_envelope, render_skill_invocation_envelope,
    skill_body_checksum, strip_skill_envelopes_for_memory,
};
pub use skill_guard::{
    SkillGuardFinding, SkillGuardOptions, SkillGuardReport, SkillGuardVerdict, run_skill_guard,
    skill_guard_receipts_file,
};
pub use skill_learning::{
    LearningReviewOptions, LearningReviewReport, SkillArchiveOptions, SkillCuratorOptions,
    SkillLearningProposal, SkillLearningProposalOperation, SkillLearningProposalStatus,
    SkillLearningSignal, SkillProposeOptions, SkillStructuredPatch, SkillSupportFileOperation,
    build_self_improvement_replacement_body, create_skill_archive_proposal,
    create_skill_learning_proposal, run_learning_review, run_skill_curator, skill_proposals_file,
};
pub use skill_lint::{
    SkillLintFinding, SkillLintOptions, SkillLintReport, SkillLintSeverity, SkillLintStatus,
    SkillLintSummary, lint_skill_file, skill_lint_receipts_file,
};
pub use skill_matcher::{SkillMatcherInfo, skill_matcher_info};
pub use skill_pack::{
    SkillPackAction, SkillPackConflictPolicy, SkillPackExportOptions, SkillPackImportOptions,
    SkillPackLock, SkillPackManifest, SkillPackManifestSkill, SkillPackRemoveOptions,
    SkillPackReport, SkillPackStatus, export_skill_pack, import_skill_pack, remove_skill_pack,
    skill_pack_lock_file, skill_pack_receipts_file,
};
pub use skill_synthesis::{
    SkillSynthesisOptions, SkillSynthesisReport, skill_synthesis_receipts_file, synthesize_skill,
};
pub use skill_usage::{
    SkillUsageAction, SkillUsageEventOptions, SkillUsageProvenance, SkillUsageRecord,
    SkillUsageReport, SkillUsageSnapshot, collect_skill_usage_snapshot, record_skill_usage_event,
    record_skill_usage_from_prompt_bundle, skill_usage_events_file, skill_usage_snapshot_file,
};
pub use skill_view::{SkillViewOptions, SkillViewReport, view_skill};
pub use skills::{
    HARNESS_BUILTIN_SKILL_NAMESPACE, SKILL_SELECTION_RECEIPT_SCHEMA, SkillDeliveryMode,
    SkillFrontmatter, SkillIndex, SkillIndexFile, SkillIndexOrigin, SkillIndexSummary, SkillRecord,
    SkillScoreComponent, SkillSelection, SkillSelectionQuery, SkillSelectionReceipt,
    SkillSourceKind, build_harness_skill_index, build_runtime_skill_index,
    build_source_skill_index, select_skills, skill_selection_receipts_file, write_skill_index,
    write_skill_selection_receipt,
};
pub use status::{
    GatewayRestartCompletionStatus, GatewayRestartHeartbeatStatus, GatewayRestartReceiptStatus,
    GatewayRestartServiceStatus, GatewayRestartStatusReport, HarnessChannelStatus,
    HarnessCronSchedulerStatus, HarnessJsonlStatus, HarnessLearningStatus, HarnessMemoryStatus,
    HarnessOperationalLogStatus, HarnessOutboxStatus, HarnessPluginStatus,
    HarnessRuntimeReceiptStatus, HarnessRuntimeStatus, HarnessSkillStatus, HarnessStatusOptions,
    HarnessStatusReport, collect_gateway_restart_status, collect_harness_status,
};
pub use subagent_lifecycle::{
    SubagentLifecycleCleanup, SubagentLifecycleCloseOptions, SubagentLifecycleReceipt,
    SubagentLifecycleRecordOptions, SubagentLifecycleShowOptions, SubagentLifecycleShowReport,
    SubagentLifecycleState, close_subagent_lifecycle, record_subagent_lifecycle,
    show_subagent_lifecycle, subagent_lifecycle_receipts_file, subagent_lifecycle_snapshot_file,
};
pub use subagents::{
    SubagentLedger, SubagentLedgerSummary, SubagentPlan, SubagentPlanAction, SubagentPlanEntry,
    SubagentPlanFile, SubagentPlanInput, SubagentPlanSummary, SubagentRun, SubagentRunStatus,
    load_subagent_ledger, plan_subagents, write_subagent_plan,
};
pub use supervision::{
    SupervisionEvaluateOptions, SupervisionEvaluationReport, SupervisionSummary, SupervisorAlert,
    SupervisorChildEvaluation, SupervisorChildSpec, SupervisorChildState, SupervisorChildStatus,
    SupervisorRestartPolicy, default_supervisor_child_specs, evaluate_supervisor_children,
};
pub use supervisor::{
    WindowsSupervisorPlanOptions, WindowsSupervisorPlanReport, WindowsSupervisorScript,
    WindowsSupervisorTask, write_windows_supervisor_plan,
};
pub use supervisor_inventory::{
    SupervisorInventoryOptions, SupervisorInventoryReport, SupervisorInventoryServiceConfig,
    SupervisorInventoryServiceSummary, SupervisorInventoryStatus, SupervisorLaunchCommand,
    reconcile_supervisor_inventory,
};
pub use token_efficiency::{
    PromptReductionOptions, PromptReductionReport, TokenEfficiencyOptions, TokenEfficiencyReport,
    collect_token_efficiency, evaluate_prompt_reduction,
};
pub use trace::{TraceOptions, TraceRecord, TraceReport, trace_harness_event};
pub use turns::{
    TurnAgent, TurnDispatch, TurnModelPolicy, TurnPlan, TurnPlanFile, TurnPlanInput,
    TurnPromptFile, TurnProviderRequestPolicy, TurnThinkingPolicy, build_turn_plan,
    write_turn_plan,
};
pub use vault::{
    EncryptedVaultFile, EncryptedVaultRecord, VaultGetOptions, VaultPutOptions, get_vault_secret,
    put_vault_secret,
};
pub use virtual_session_context::{
    VIRTUAL_SESSION_WORKING_CONTEXT_SCHEMA, VirtualSessionContextQuery,
    VirtualSessionEvidenceAnchor, VirtualSessionEvidenceAnchors, VirtualSessionLane,
    VirtualSessionScopeDecision, VirtualSessionWorkingContext,
    resolve_virtual_session_working_context,
};
pub use worker_adapters::{
    DeterministicCronWorkerEnqueueOptions, NativeCronWorkerEnqueueOptions,
    SubagentWorkerEnqueueOptions, SubagentWorkerEnqueueOptionsV2, WorkerAdapterEnqueueReport,
    WorkerAdapterEnqueueSummary, WorkerAdapterJobRef, enqueue_deterministic_cron_workers,
    enqueue_native_cron_workers, enqueue_subagent_workers, enqueue_subagent_workers_v2,
};
pub use workers::{
    WorkerCancelOptions, WorkerCancelReport, WorkerCapacityBlockedSummary, WorkerDispatchConfig,
    WorkerDownstreamRuntimeStatus, WorkerEnqueueOptions, WorkerEnqueueOptionsV2,
    WorkerEnqueueReport, WorkerJob, WorkerJobExecutionResult, WorkerJobKind, WorkerJobStatus,
    WorkerLaneStatus, WorkerReapStaleOptions, WorkerReapStaleReport, WorkerRunOnceOptions,
    WorkerRunOnceReport, WorkerRunOnceStatus, WorkerStatusOptions, WorkerStatusReport,
    WorkerStatusTotals, cancel_worker_job, collect_worker_status, enqueue_worker_job,
    enqueue_worker_job_v2, init_worker_store, load_worker_dispatch_config, reap_stale_worker_jobs,
    run_worker_once, worker_db_file,
};

#[cfg(test)]
mod virtual_session_context_tests {
    use super::*;
    use serde_json::Value;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_root(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "agent_harness_virtual_session_context_{}_{}_{}",
            name,
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    fn write_state(
        harness_home: &std::path::Path,
        platform: &str,
        channel_id: &str,
        user_id: &str,
        agent_id: &str,
        session_key: &str,
    ) {
        let state_file = channel_session_state_file(harness_home, platform, channel_id, user_id);
        fs::create_dir_all(state_file.parent().unwrap()).unwrap();
        write_json_atomic(
            &state_file,
            &ChannelSessionState {
                schema: "agent-harness.channel-session-state.v1".to_string(),
                platform: platform.to_string(),
                channel_id: channel_id.to_string(),
                user_id: user_id.to_string(),
                active_session_key: session_key.to_string(),
                agent_id: Some(agent_id.to_string()),
                provider: None,
                model: None,
                session_topic: None,
                model_override: None,
                model_override_provider: None,
                model_override_model: None,
                thinking_enabled: false,
                thinking_level: None,
                thinking_instruction: None,
                reasoning_preference: None,
                backend_reasoning_policy: None,
                fast_mode: None,
                stop_requested: false,
                stop_reason: None,
                steering_notes: Vec::new(),
                btw_notes: Vec::new(),
                last_command: None,
                updated_at_ms: 10,
            },
        )
        .unwrap();
    }

    #[test]
    fn resolver_returns_same_lane_envelope_for_active_continuation() {
        let root = temp_root("resolver_returns_same_lane_envelope_for_active_continuation");
        let harness_home = root.join(".agent-harness");
        let session_key = "telegram:dm:user:main";
        let cont_key = "telegram:dm:user:main:cont-1";
        write_state(&harness_home, "telegram", "dm", "user", "main", cont_key);
        record_completed_turn_working_set_snapshot(CompletedTurnWorkingSetSnapshotOptions {
            harness_home: harness_home.clone(),
            platform: "telegram".to_string(),
            channel_id: "dm".to_string(),
            user_id: "user".to_string(),
            agent_id: "main".to_string(),
            working_session_key: session_key.to_string(),
            queue_id: Some("queue-root".to_string()),
            message_text: Some("root task".to_string()),
            status: "completed".to_string(),
            run_once_receipt_file: None,
            outbox_file: None,
            completion_file: None,
            now_ms: 11,
        })
        .unwrap();
        record_completed_turn_working_set_snapshot(CompletedTurnWorkingSetSnapshotOptions {
            harness_home: harness_home.clone(),
            platform: "telegram".to_string(),
            channel_id: "dm".to_string(),
            user_id: "user".to_string(),
            agent_id: "main".to_string(),
            working_session_key: cont_key.to_string(),
            queue_id: Some("queue-cont".to_string()),
            message_text: Some("continuation task".to_string()),
            status: "completed".to_string(),
            run_once_receipt_file: None,
            outbox_file: None,
            completion_file: None,
            now_ms: 12,
        })
        .unwrap();

        let envelope = resolve_virtual_session_working_context(VirtualSessionContextQuery {
            harness_home: harness_home.clone(),
            platform: "telegram".to_string(),
            channel_id: "dm".to_string(),
            user_id: "user".to_string(),
            agent_id: "main".to_string(),
            session_key: None,
            now_ms: 13,
        })
        .unwrap();

        assert_eq!(
            envelope.schema,
            "agent-harness.virtual-session-working-context.v1"
        );
        assert_eq!(envelope.lane.agent_id, "main");
        assert_eq!(envelope.current_session_key.as_deref(), Some(cont_key));
        assert_eq!(
            envelope.predecessor_session_key.as_deref(),
            Some(session_key)
        );
        assert_eq!(envelope.continuation_index, 1);
        assert_eq!(envelope.scope_decision.status, "same-virtual-session");
        assert!(
            envelope
                .recent_queue_ids
                .contains(&"queue-cont".to_string())
        );
        let json = serde_json::to_string(&envelope).unwrap();
        assert!(json.len() <= 4096);
        assert!(!json.contains("data:image"));
        assert!(!json.contains("provider.example"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn resolver_keeps_colon_delimited_queue_id_from_validation() {
        let root = temp_root("resolver_keeps_colon_delimited_queue_id_from_validation");
        let harness_home = root.join(".agent-harness");
        let session_key = "telegram:dm:user:main";
        let queue_id = "turn:telegram:dm:user:main:1782983000000";
        write_state(&harness_home, "telegram", "dm", "user", "main", session_key);
        let report =
            record_completed_turn_working_set_snapshot(CompletedTurnWorkingSetSnapshotOptions {
                harness_home: harness_home.clone(),
                platform: "telegram".to_string(),
                channel_id: "dm".to_string(),
                user_id: "user".to_string(),
                agent_id: "main".to_string(),
                working_session_key: session_key.to_string(),
                queue_id: Some(queue_id.to_string()),
                message_text: Some("colon queue task".to_string()),
                status: "completed".to_string(),
                run_once_receipt_file: None,
                outbox_file: None,
                completion_file: None,
                now_ms: 14,
            })
            .unwrap();
        let mut memory: ContextWorkingSetMemory =
            serde_json::from_slice(&fs::read(&report.working_set_file).unwrap()).unwrap();
        memory.pending_queue_item = None;
        write_json_atomic(&report.working_set_file, &memory).unwrap();

        let envelope = resolve_virtual_session_working_context(VirtualSessionContextQuery {
            harness_home: harness_home.clone(),
            platform: "telegram".to_string(),
            channel_id: "dm".to_string(),
            user_id: "user".to_string(),
            agent_id: "main".to_string(),
            session_key: None,
            now_ms: 15,
        })
        .unwrap();

        assert!(envelope.recent_queue_ids.contains(&queue_id.to_string()));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn resolver_discord_and_telegram_lanes_do_not_substitute() {
        let root = temp_root("resolver_discord_and_telegram_lanes_do_not_substitute");
        let harness_home = root.join(".agent-harness");
        write_state(
            &harness_home,
            "telegram",
            "same-channel",
            "same-user",
            "main",
            "telegram:same-channel:same-user:main",
        );
        write_state(
            &harness_home,
            "discord",
            "same-channel",
            "same-user",
            "main",
            "discord:same-channel:same-user:main",
        );
        record_completed_turn_working_set_snapshot(CompletedTurnWorkingSetSnapshotOptions {
            harness_home: harness_home.clone(),
            platform: "telegram".to_string(),
            channel_id: "same-channel".to_string(),
            user_id: "same-user".to_string(),
            agent_id: "main".to_string(),
            working_session_key: "telegram:same-channel:same-user:main".to_string(),
            queue_id: Some("telegram-queue".to_string()),
            message_text: Some("telegram task".to_string()),
            status: "completed".to_string(),
            run_once_receipt_file: None,
            outbox_file: None,
            completion_file: None,
            now_ms: 20,
        })
        .unwrap();

        let discord = resolve_virtual_session_working_context(VirtualSessionContextQuery {
            harness_home: harness_home.clone(),
            platform: "discord".to_string(),
            channel_id: "same-channel".to_string(),
            user_id: "same-user".to_string(),
            agent_id: "main".to_string(),
            session_key: None,
            now_ms: 21,
        })
        .unwrap();

        assert_eq!(discord.lane.platform, "discord");
        assert!(
            !discord
                .recent_queue_ids
                .contains(&"telegram-queue".to_string())
        );
        assert_ne!(
            discord.current_session_key.as_deref(),
            Some("telegram:same-channel:same-user:main")
        );
        let serialized: Value = serde_json::to_value(&discord).unwrap();
        assert_eq!(serialized["scopeDecision"]["fallbackUsed"], false);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn resolver_non_main_agent_does_not_read_main_lane_state() {
        let root = temp_root("resolver_non_main_agent_does_not_read_main_lane_state");
        let harness_home = root.join(".agent-harness");
        write_state(
            &harness_home,
            "telegram",
            "dm",
            "user",
            "main",
            "telegram:dm:user:main",
        );
        record_completed_turn_working_set_snapshot(CompletedTurnWorkingSetSnapshotOptions {
            harness_home: harness_home.clone(),
            platform: "telegram".to_string(),
            channel_id: "dm".to_string(),
            user_id: "user".to_string(),
            agent_id: "main".to_string(),
            working_session_key: "telegram:dm:user:main".to_string(),
            queue_id: Some("main-queue".to_string()),
            message_text: Some("main task".to_string()),
            status: "completed".to_string(),
            run_once_receipt_file: None,
            outbox_file: None,
            completion_file: None,
            now_ms: 30,
        })
        .unwrap();

        let non_main = resolve_virtual_session_working_context(VirtualSessionContextQuery {
            harness_home: harness_home.clone(),
            platform: "telegram".to_string(),
            channel_id: "dm".to_string(),
            user_id: "user".to_string(),
            agent_id: "xiaoxiaoli".to_string(),
            session_key: None,
            now_ms: 31,
        })
        .unwrap();

        assert_eq!(non_main.lane.agent_id, "xiaoxiaoli");
        assert_eq!(non_main.scope_decision.status, "denied");
        assert!(non_main.recent_queue_ids.is_empty());
        assert!(non_main.working_set_file.is_none());
        let _ = fs::remove_dir_all(root);
    }
}

pub const PROMPT_FILE_NAMES: &[&str] = &[
    "AGENTS.md",
    "SOUL.md",
    "TOOLS.md",
    "USER.md",
    "IDENTITY.md",
    "HEARTBEAT.md",
    "BOOTSTRAP.md",
];

pub const SKILL_FILE_NAME: &str = "SKILL.md";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentSource {
    pub home: PathBuf,
    pub workspace: PathBuf,
}

impl AgentSource {
    pub fn new(home: impl Into<PathBuf>) -> Self {
        let home = home.into();
        let workspace = home.join("workspace");
        Self { home, workspace }
    }

    pub fn with_workspace(home: impl Into<PathBuf>, workspace: impl Into<PathBuf>) -> Self {
        Self {
            home: home.into(),
            workspace: workspace.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentSourceInventory {
    pub source: AgentSource,
    pub has_config: bool,
    pub prompt_files: Vec<PathBuf>,
    pub agent_dirs: usize,
    pub agent_config_files: usize,
    pub session_indexes: Vec<PathBuf>,
    pub transcript_files: usize,
    pub trajectory_files: usize,
    pub codex_binding_files: usize,
    pub workspace_skill_dirs: usize,
    pub managed_skill_dirs: usize,
    pub project_agent_skill_dirs: usize,
    pub native_cron_jobs: bool,
    pub native_cron_state: bool,
    pub native_cron_run_logs: usize,
    pub deterministic_crontabs: usize,
    pub deterministic_cron_job_scripts: usize,
    pub deterministic_cron_logs: usize,
    pub subagent_state_files: usize,
    pub memory_files: usize,
    pub memory_qdrant_edge: bool,
    pub memory_lancedb: bool,
    pub memory_legacy_mem_sqlite: bool,
    pub plugin_install_record: bool,
    pub plugin_state_db: bool,
}

impl AgentSourceInventory {
    pub fn is_empty(&self) -> bool {
        !self.has_config
            && self.prompt_files.is_empty()
            && self.agent_dirs == 0
            && self.agent_config_files == 0
            && self.session_indexes.is_empty()
            && self.transcript_files == 0
            && self.trajectory_files == 0
            && self.codex_binding_files == 0
            && self.workspace_skill_dirs == 0
            && self.managed_skill_dirs == 0
            && self.project_agent_skill_dirs == 0
            && !self.native_cron_jobs
            && !self.native_cron_state
            && self.native_cron_run_logs == 0
            && self.deterministic_crontabs == 0
            && self.deterministic_cron_job_scripts == 0
            && self.deterministic_cron_logs == 0
            && self.subagent_state_files == 0
            && self.memory_files == 0
            && !self.memory_qdrant_edge
            && !self.memory_lancedb
            && !self.memory_legacy_mem_sqlite
            && !self.plugin_install_record
            && !self.plugin_state_db
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportPlan {
    pub phases: Vec<ImportPhase>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportPhase {
    pub name: &'static str,
    pub required: bool,
    pub status: ImportPhaseStatus,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImportPhaseStatus {
    Ready,
    Missing,
    Deferred,
}

pub fn inventory(source: AgentSource) -> io::Result<AgentSourceInventory> {
    let has_config = source.home.join("openclaw.json").is_file();
    let prompt_files = existing_prompt_files(&source.workspace);
    let agents_root = source.home.join("agents");
    let agent_dirs = count_child_dirs(&agents_root)?;
    let agent_config_files = count_agent_config_files(&agents_root)?;
    let session_indexes = find_named_files(&agents_root, "sessions.json")?;
    let transcript_files = count_transcript_files(&agents_root)?;
    let trajectory_files = count_files_with_suffix(&agents_root, ".trajectory.jsonl")?;
    let codex_binding_files =
        count_files_with_suffix(&agents_root, ".jsonl.codex-app-server.json")?;
    let workspace_skill_dirs = count_skill_dirs(&source.workspace.join("skills"))?;
    let managed_skill_dirs = count_skill_dirs(&source.home.join("skills"))?;
    let project_agent_skill_dirs =
        count_skill_dirs(&source.workspace.join(".agents").join("skills"))?;
    let cron_root = source.home.join("cron");
    let native_cron_jobs = cron_root.join("jobs.json").is_file();
    let native_cron_state = cron_root.join("jobs-state.json").is_file();
    let native_cron_run_logs = count_files_with_suffix(&cron_root.join("runs"), ".jsonl")?;
    let deterministic_crontabs = count_named_files_under(&source.workspace, "crontab")?
        + count_files_with_suffix(&source.workspace, ".crontab")?;
    let deterministic_cron_job_scripts = count_regular_files(
        &source
            .workspace
            .join("tools")
            .join("cron-runner")
            .join("jobs"),
    )? + count_regular_files(
        &source
            .workspace
            .join("tools")
            .join("backup-cron-runner")
            .join("jobs"),
    )?;
    let deterministic_cron_logs = count_regular_files(
        &source
            .workspace
            .join("tools")
            .join("cron-runner")
            .join("logs"),
    )? + count_regular_files(
        &source
            .workspace
            .join("tools")
            .join("backup-cron-runner")
            .join("logs"),
    )?;
    let subagent_state_files = count_regular_files(&source.home.join("subagents"))?;
    let memory_root = source.home.join("memory");
    let memory_files = count_regular_files(&memory_root)?;
    let memory_qdrant_edge = memory_root.join("qdrant-edge").is_dir();
    let memory_lancedb = memory_root.join("lancedb").is_dir();
    let memory_legacy_mem_sqlite = memory_root.join("openclaw-mem.sqlite").is_file();
    let plugin_install_record = source.home.join("plugins").join("installs.json").is_file();
    let plugin_state_db = source
        .home
        .join("plugin-state")
        .join("state.sqlite")
        .is_file();

    Ok(AgentSourceInventory {
        source,
        has_config,
        prompt_files,
        agent_dirs,
        agent_config_files,
        session_indexes,
        transcript_files,
        trajectory_files,
        codex_binding_files,
        workspace_skill_dirs,
        managed_skill_dirs,
        project_agent_skill_dirs,
        native_cron_jobs,
        native_cron_state,
        native_cron_run_logs,
        deterministic_crontabs,
        deterministic_cron_job_scripts,
        deterministic_cron_logs,
        subagent_state_files,
        memory_files,
        memory_qdrant_edge,
        memory_lancedb,
        memory_legacy_mem_sqlite,
        plugin_install_record,
        plugin_state_db,
    })
}

pub fn build_import_plan(inv: &AgentSourceInventory) -> ImportPlan {
    let mut phases = Vec::new();
    let total_skill_dirs =
        inv.workspace_skill_dirs + inv.managed_skill_dirs + inv.project_agent_skill_dirs;

    phases.push(ImportPhase {
        name: "config",
        required: true,
        status: if inv.has_config {
            ImportPhaseStatus::Ready
        } else {
            ImportPhaseStatus::Missing
        },
        notes: vec![if inv.has_config {
            "openclaw.json found; parse and redact secrets before writing new config".to_string()
        } else {
            "openclaw.json not found at source home".to_string()
        }],
    });

    phases.push(ImportPhase {
        name: "workspace",
        required: true,
        status: if inv.prompt_files.is_empty() {
            ImportPhaseStatus::Missing
        } else {
            ImportPhaseStatus::Ready
        },
        notes: vec![format!(
            "{} prompt files found under workspace",
            inv.prompt_files.len()
        )],
    });

    phases.push(ImportPhase {
        name: "agents",
        required: true,
        status: if inv.agent_dirs == 0 {
            ImportPhaseStatus::Missing
        } else {
            ImportPhaseStatus::Ready
        },
        notes: vec![format!(
            "{} agent directories, {} agent-local config/auth/model files; preserve multi-agent routing and per-agent sessions",
            inv.agent_dirs, inv.agent_config_files
        )],
    });

    phases.push(ImportPhase {
        name: "skills",
        required: false,
        status: if total_skill_dirs == 0 {
            ImportPhaseStatus::Missing
        } else {
            ImportPhaseStatus::Ready
        },
        notes: vec![format!(
            "{} workspace, {} managed, {} project .agents skill directories; import into a skill-first index with explicit conflict policy",
            inv.workspace_skill_dirs, inv.managed_skill_dirs, inv.project_agent_skill_dirs
        )],
    });

    phases.push(ImportPhase {
        name: "sessions",
        required: false,
        status: if inv.session_indexes.is_empty() {
            ImportPhaseStatus::Missing
        } else {
            ImportPhaseStatus::Ready
        },
        notes: vec![format!(
            "{} session indexes, {} transcripts, {} trajectories, {} Codex binding mirrors",
            inv.session_indexes.len(),
            inv.transcript_files,
            inv.trajectory_files,
            inv.codex_binding_files
        )],
    });

    phases.push(ImportPhase {
        name: "native-cron",
        required: true,
        status: if inv.native_cron_jobs {
            ImportPhaseStatus::Ready
        } else {
            ImportPhaseStatus::Missing
        },
        notes: vec![format!(
            "jobs.json: {}, jobs-state.json: {}, {} run logs; import imported agent-turn cron before gateway handoff",
            inv.native_cron_jobs, inv.native_cron_state, inv.native_cron_run_logs
        )],
    });

    phases.push(ImportPhase {
        name: "deterministic-cron",
        required: false,
        status: if inv.deterministic_crontabs == 0 && inv.deterministic_cron_job_scripts == 0 {
            ImportPhaseStatus::Missing
        } else {
            ImportPhaseStatus::Ready
        },
        notes: vec![format!(
            "{} crontab files, {} job scripts, {} log/state files; run without LLM request path",
            inv.deterministic_crontabs,
            inv.deterministic_cron_job_scripts,
            inv.deterministic_cron_logs
        )],
    });

    phases.push(ImportPhase {
        name: "subagents",
        required: false,
        status: if inv.subagent_state_files == 0 {
            ImportPhaseStatus::Missing
        } else {
            ImportPhaseStatus::Ready
        },
        notes: vec![format!(
            "{} subagent state files found; preserve ready queue/run ledger before enabling native worker execution",
            inv.subagent_state_files
        )],
    });

    phases.push(ImportPhase {
        name: "memory",
        required: false,
        status: if inv.memory_files == 0 {
            ImportPhaseStatus::Missing
        } else {
            ImportPhaseStatus::Ready
        },
        notes: vec![format!(
            "{} memory files found; qdrant-edge={}, lancedb={}, openclaw-mem.sqlite={}; qdrant-edge is the primary backend when present, LanceDB is backup/optional, and SQLite sources require stopped gateway or backup API",
            inv.memory_files,
            inv.memory_qdrant_edge,
            inv.memory_lancedb,
            inv.memory_legacy_mem_sqlite
        )],
    });

    phases.push(ImportPhase {
        name: "plugins",
        required: false,
        status: if inv.plugin_install_record || inv.plugin_state_db {
            ImportPhaseStatus::Deferred
        } else {
            ImportPhaseStatus::Missing
        },
        notes: vec![format!(
            "install record: {}, plugin state db: {}; execution should route through sidecar first",
            inv.plugin_install_record, inv.plugin_state_db
        )],
    });

    ImportPlan { phases }
}

fn existing_prompt_files(workspace: &Path) -> Vec<PathBuf> {
    PROMPT_FILE_NAMES
        .iter()
        .map(|name| workspace.join(name))
        .filter(|path| path.is_file())
        .collect()
}

fn find_named_files(root: &Path, name: &str) -> io::Result<Vec<PathBuf>> {
    let mut matches = Vec::new();
    visit_files(root, &mut |path| {
        if path.file_name().and_then(|value| value.to_str()) == Some(name) {
            matches.push(path.to_path_buf());
        }
    })?;
    Ok(matches)
}

fn count_named_files_under(root: &Path, name: &str) -> io::Result<usize> {
    let mut count = 0;
    visit_files(root, &mut |path| {
        if path.file_name().and_then(|value| value.to_str()) == Some(name) {
            count += 1;
        }
    })?;
    Ok(count)
}

fn count_child_dirs(root: &Path) -> io::Result<usize> {
    if !root.exists() {
        return Ok(0);
    }

    let mut count = 0;
    for entry in fs::read_dir(root)? {
        if entry?.file_type()?.is_dir() {
            count += 1;
        }
    }
    Ok(count)
}

fn count_skill_dirs(root: &Path) -> io::Result<usize> {
    if !root.exists() {
        return Ok(0);
    }

    let mut count = 0;
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() && entry.path().join(SKILL_FILE_NAME).is_file() {
            count += 1;
        }
    }
    Ok(count)
}

fn count_agent_config_files(root: &Path) -> io::Result<usize> {
    const AGENT_CONFIG_NAMES: &[&str] = &[
        "auth.json",
        "auth-profiles.json",
        "auth-state.json",
        "models.json",
    ];

    let mut count = 0;
    visit_files(root, &mut |path| {
        let name = path.file_name().and_then(|value| value.to_str());
        if name.is_some_and(|name| AGENT_CONFIG_NAMES.contains(&name)) {
            count += 1;
        }
    })?;
    Ok(count)
}

fn count_regular_files(root: &Path) -> io::Result<usize> {
    let mut count = 0;
    visit_files(root, &mut |_| count += 1)?;
    Ok(count)
}

fn count_files_with_suffix(root: &Path, suffix: &str) -> io::Result<usize> {
    let mut count = 0;
    visit_files(root, &mut |path| {
        if path.to_string_lossy().ends_with(suffix) {
            count += 1;
        }
    })?;
    Ok(count)
}

fn count_transcript_files(root: &Path) -> io::Result<usize> {
    let mut count = 0;
    visit_files(root, &mut |path| {
        let path = path.to_string_lossy();
        if path.ends_with(".jsonl") && !path.ends_with(".trajectory.jsonl") {
            count += 1;
        }
    })?;
    Ok(count)
}

fn visit_files(root: &Path, on_file: &mut impl FnMut(&Path)) -> io::Result<()> {
    if !root.exists() {
        return Ok(());
    }

    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            visit_files(&path, on_file)?;
        } else if file_type.is_file() {
            on_file(&path);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn inventory_detects_openclaw_layout() {
        let root = temp_root("inventory_detects_openclaw_layout");
        let home = root.join(".openclaw");
        let workspace = home.join("workspace");
        let agent_sessions = home.join("agents").join("main").join("sessions");
        let agent_home = home.join("agents").join("main").join("agent");
        let cron_runs = home.join("cron").join("runs");
        let deterministic_jobs = workspace.join("tools").join("cron-runner").join("jobs");
        let deterministic_logs = workspace.join("tools").join("cron-runner").join("logs");
        let workspace_skill = workspace.join("skills").join("triage");
        let managed_skill = home.join("skills").join("memory-maintenance");
        let project_agent_skill = workspace.join(".agents").join("skills").join("handoff");
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(&agent_sessions).unwrap();
        fs::create_dir_all(&agent_home).unwrap();
        fs::create_dir_all(&cron_runs).unwrap();
        fs::create_dir_all(&deterministic_jobs).unwrap();
        fs::create_dir_all(&deterministic_logs).unwrap();
        fs::create_dir_all(home.join("memory")).unwrap();
        fs::create_dir_all(home.join("memory").join("qdrant-edge")).unwrap();
        fs::create_dir_all(home.join("memory").join("lancedb")).unwrap();
        fs::create_dir_all(home.join("plugins")).unwrap();
        fs::create_dir_all(home.join("plugin-state")).unwrap();
        fs::create_dir_all(home.join("subagents")).unwrap();
        fs::create_dir_all(&workspace_skill).unwrap();
        fs::create_dir_all(&managed_skill).unwrap();
        fs::create_dir_all(&project_agent_skill).unwrap();

        fs::write(home.join("openclaw.json"), "{}").unwrap();
        fs::write(workspace.join("AGENTS.md"), "# Agent").unwrap();
        fs::write(agent_home.join("models.json"), "{}").unwrap();
        fs::write(agent_home.join("auth-state.json"), "{}").unwrap();
        fs::write(agent_sessions.join("sessions.json"), "{}").unwrap();
        fs::write(agent_sessions.join("abc.jsonl"), "{}\n").unwrap();
        fs::write(agent_sessions.join("abc.trajectory.jsonl"), "{}\n").unwrap();
        fs::write(agent_sessions.join("abc.jsonl.codex-app-server.json"), "{}").unwrap();
        fs::write(workspace_skill.join(SKILL_FILE_NAME), "# Triage").unwrap();
        fs::write(managed_skill.join(SKILL_FILE_NAME), "# Memory").unwrap();
        fs::write(project_agent_skill.join(SKILL_FILE_NAME), "# Handoff").unwrap();
        fs::write(home.join("cron").join("jobs.json"), "{\"jobs\":[]}").unwrap();
        fs::write(home.join("cron").join("jobs-state.json"), "{\"jobs\":{}}").unwrap();
        fs::write(cron_runs.join("run.jsonl"), "{}\n").unwrap();
        fs::create_dir_all(workspace.join("tools").join("cron-runner").join("crontab")).unwrap();
        fs::write(
            workspace
                .join("tools")
                .join("cron-runner")
                .join("crontab")
                .join("openclaw-mem.crontab"),
            "* * * * * jobs/episodic_extract_1m.sh\n",
        )
        .unwrap();
        fs::write(
            deterministic_jobs.join("episodic_extract_1m.sh"),
            "#!/bin/sh\n",
        )
        .unwrap();
        fs::write(deterministic_logs.join("supercronic.log"), "").unwrap();
        fs::write(home.join("subagents").join("runs.json"), "{\"runs\":[]}").unwrap();
        fs::write(home.join("memory").join("2026-06-08.md"), "# Memory").unwrap();
        fs::write(home.join("memory").join("openclaw-mem.sqlite"), "").unwrap();
        fs::write(home.join("plugins").join("installs.json"), "{}").unwrap();
        fs::write(home.join("plugin-state").join("state.sqlite"), "").unwrap();

        let inv = inventory(AgentSource::new(&home)).unwrap();

        assert!(inv.has_config);
        assert_eq!(inv.prompt_files.len(), 1);
        assert_eq!(inv.agent_dirs, 1);
        assert_eq!(inv.agent_config_files, 2);
        assert_eq!(inv.session_indexes.len(), 1);
        assert_eq!(inv.transcript_files, 1);
        assert_eq!(inv.trajectory_files, 1);
        assert_eq!(inv.codex_binding_files, 1);
        assert_eq!(inv.workspace_skill_dirs, 1);
        assert_eq!(inv.managed_skill_dirs, 1);
        assert_eq!(inv.project_agent_skill_dirs, 1);
        assert!(inv.native_cron_jobs);
        assert!(inv.native_cron_state);
        assert_eq!(inv.native_cron_run_logs, 1);
        assert_eq!(inv.deterministic_crontabs, 1);
        assert_eq!(inv.deterministic_cron_job_scripts, 1);
        assert_eq!(inv.deterministic_cron_logs, 1);
        assert_eq!(inv.subagent_state_files, 1);
        assert_eq!(inv.memory_files, 2);
        assert!(inv.memory_qdrant_edge);
        assert!(inv.memory_lancedb);
        assert!(inv.memory_legacy_mem_sqlite);
        assert!(inv.plugin_install_record);
        assert!(inv.plugin_state_db);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn import_plan_marks_ready_and_deferred_phases() {
        let inv = AgentSourceInventory {
            source: AgentSource::new("unused"),
            has_config: true,
            prompt_files: vec![PathBuf::from("AGENTS.md")],
            agent_dirs: 2,
            agent_config_files: 4,
            session_indexes: vec![PathBuf::from("sessions.json")],
            transcript_files: 2,
            trajectory_files: 1,
            codex_binding_files: 1,
            workspace_skill_dirs: 2,
            managed_skill_dirs: 1,
            project_agent_skill_dirs: 1,
            native_cron_jobs: true,
            native_cron_state: true,
            native_cron_run_logs: 8,
            deterministic_crontabs: 2,
            deterministic_cron_job_scripts: 6,
            deterministic_cron_logs: 3,
            subagent_state_files: 1,
            memory_files: 3,
            memory_qdrant_edge: true,
            memory_lancedb: true,
            memory_legacy_mem_sqlite: true,
            plugin_install_record: true,
            plugin_state_db: true,
        };

        let plan = build_import_plan(&inv);

        assert_phase_status(&plan, "config", ImportPhaseStatus::Ready);
        assert_phase_status(&plan, "workspace", ImportPhaseStatus::Ready);
        assert_phase_status(&plan, "agents", ImportPhaseStatus::Ready);
        assert_phase_status(&plan, "skills", ImportPhaseStatus::Ready);
        assert_phase_status(&plan, "sessions", ImportPhaseStatus::Ready);
        assert_phase_status(&plan, "native-cron", ImportPhaseStatus::Ready);
        assert_phase_status(&plan, "deterministic-cron", ImportPhaseStatus::Ready);
        assert_phase_status(&plan, "subagents", ImportPhaseStatus::Ready);
        assert_phase_status(&plan, "memory", ImportPhaseStatus::Ready);
        assert_phase_status(&plan, "plugins", ImportPhaseStatus::Deferred);
    }

    fn assert_phase_status(plan: &ImportPlan, name: &str, expected: ImportPhaseStatus) {
        let phase = plan
            .phases
            .iter()
            .find(|phase| phase.name == name)
            .unwrap_or_else(|| panic!("missing phase {name}"));
        assert_eq!(phase.status, expected);
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "agent-harness-core-{test_name}-{}-{nanos}",
            std::process::id()
        ))
    }
}
