use std::fs;
use std::io;
use std::path::{Path, PathBuf};

pub mod activation;
pub mod channel_commands;
pub mod channel_delivery;
pub mod channel_ingress;
pub mod channel_pipeline;
pub mod channel_runtime;
pub mod channel_state;
pub mod codex_runtime;
pub mod cron;
pub mod deterministic_cron;
pub mod harness_registry;
pub mod harness_skills;
pub mod importer;
pub mod logging;
pub mod memory;
pub mod ops;
pub mod progress;
pub mod prompt;
pub mod registry;
pub mod runtime_pipeline;
pub mod runtime_queue;
pub mod runtime_worker;
pub mod skills;
pub mod status;
pub mod subagents;
pub mod supervisor;
pub mod turns;
pub mod worker_adapters;
pub mod workers;

pub use activation::{
    ActivationReadinessCheck, ActivationReadinessOptions, ActivationReadinessReport,
    ActivationReadinessStatus, ActivationReadinessSummary, check_activation_readiness,
};
pub use channel_commands::{
    ChannelCommand, ChannelCommandIntent, DEFAULT_THINKING_LEVEL, THINKING_LEVELS,
    XHIGH_THINKING_LEVEL, normalize_thinking_level, parse_channel_command,
    parse_channel_command_intent,
};
pub use channel_delivery::{
    ChannelDeliveryPending, ChannelDeliveryReceipt, ChannelDeliveryRecordOptions,
    ChannelDeliveryStatus, ChannelOutboxPlanOptions, ChannelOutboxPlanReport,
    ChannelOutboxPlanSummary, plan_channel_outbox, record_channel_delivery,
};
pub use channel_ingress::{
    ChannelReceiveOptions, ChannelReceiveReceipt, ChannelReceiveReport, ChannelReceiveStatus,
    receive_channel_message,
};
pub use channel_pipeline::{
    ChannelRunOnceOptions, ChannelRunOnceReport, ChannelRunOnceStatus, run_channel_once,
};
pub use channel_runtime::{
    ChannelAgentTurnDispatch, ChannelCommandEffect, ChannelOutboundAttachment,
    ChannelOutboundAttachmentKind, ChannelOutboundMessage, ChannelOutboundMessageKind,
    ChannelStatusSnapshot, ChannelStep, ChannelStepAction, ChannelStepFile, build_channel_step,
    write_channel_step,
};
pub use channel_state::{
    AgentOverride, AgentOverridesStore, ChannelCommandApplyOptions, ChannelCommandApplyReceipt,
    ChannelCommandApplyReceiptStatus, ChannelCommandApplyReport, ChannelCommandEvent,
    ChannelSessionNote, ChannelSessionState, agent_overrides_file, apply_channel_command_step,
    channel_session_state_file, read_agent_override, read_channel_session_state,
};
pub use codex_runtime::{
    AssistantNarrationConfig, AssistantNarrationMode, CodexApprovalPolicy,
    CodexApprovalPolicyInspection, CodexAssistantNarration, CodexEnvRequirement,
    CodexInvocationPlan, CodexOutputPlan, CodexProviderConfig, CodexRuntimeCompletionOptions,
    CodexRuntimeCompletionReceipt, CodexRuntimeCompletionReport, CodexRuntimeCompletionStatus,
    CodexRuntimeLaunchProbeOptions, CodexRuntimeLaunchProbeReceipt, CodexRuntimeLaunchProbeReport,
    CodexRuntimeLaunchProbeStatus, CodexRuntimeLaunchProcess, CodexRuntimePlan,
    CodexRuntimePlanOptions, CodexRuntimePlanReport, CodexRuntimePreflightCheck,
    CodexRuntimePreflightCheckStatus, CodexRuntimePreflightOptions, CodexRuntimePreflightReceipt,
    CodexRuntimePreflightReport, CodexRuntimePreflightStatus, CodexRuntimeReceipt,
    CodexRuntimeReceiptStatus, CodexRuntimeRunOptions, CodexRuntimeRunReceipt,
    CodexRuntimeRunReport, CodexRuntimeRunStatus, CodexSandboxInspection, CodexTransportPlan,
    inspect_codex_approval_policy, inspect_codex_sandbox, inspect_codex_sandbox_policy,
    load_assistant_narration_config, plan_codex_runtime, preflight_codex_runtime,
    probe_codex_runtime_launch, record_codex_runtime_completion, run_codex_runtime,
};
pub use cron::{
    NativeCronJob, NativeCronJobState, NativeCronPlan, NativeCronPlanAction, NativeCronPlanEntry,
    NativeCronPlanFile, NativeCronPlanInput, NativeCronPlanSummary, NativeCronSchedule,
    NativeCronStore, NativeCronStoreSummary, load_native_cron_store, plan_native_cron,
    write_native_cron_plan,
};
pub use deterministic_cron::{
    DeterministicCronEntry, DeterministicCronPlan, DeterministicCronPlanAction,
    DeterministicCronPlanEntry, DeterministicCronPlanFile, DeterministicCronPlanInput,
    DeterministicCronPlanSummary, DeterministicCronRunner, DeterministicCronRunnerKind,
    DeterministicCronSchedule, DeterministicCronStore, DeterministicCronStoreSummary,
    load_deterministic_cron_store, plan_deterministic_cron, write_deterministic_cron_plan,
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
pub use importer::{
    ConfigSemantics, ConflictPolicy, DryRunImportOptions, ExecuteImportOptions, ImportAction,
    ImportExecuteReceipt, ImportExecuteReport, ImportExecuteStatus, ImportExecuteSummary,
    ImportItem, ImportItemKind, ImportItemStatus, ImportReport, ImportReportSummary,
    ImportSemantics, NativeCronSemantics, ReportFiles, SessionSemantics, build_dry_run_report,
    execute_import, write_report_files,
};
pub use logging::{
    HarnessLogEvent, HarnessLogLevel, HarnessLogWrite, append_harness_log, current_log_time_ms,
    harness_log_file, probe_harness_log_writable, write_json_atomic,
};
pub use memory::{
    MemoryCanvasWorkerOptions, MemoryCanvasWorkerReport, MemoryCanvasWorkerStatus,
    MemoryCredentialsExportEntry, MemoryCredentialsExportOptions, MemoryCredentialsExportReport,
    MemoryHookAdapterOptions, MemoryHookKind, MemoryHookReport, MemoryHookStatus,
    MemoryLifecycleReport, MemoryLifecycleStatus, MemoryLifecycleTurnOptions,
    MemoryPromptContextOptions, MemoryPromptContextReport, MemoryPromptContextStatus,
    MemorySearchHit, MemorySearchOptions, MemorySearchReport, MemorySearchStatus, MemoryVectorHit,
    MemoryVectorRecallOptions, MemoryVectorRecallReport, MemoryVectorRecallStatus,
    OpenClawMemServiceHit, OpenClawMemServiceProposalReport, OpenClawMemServiceProposalStatus,
    OpenClawMemServiceProposeOptions, OpenClawMemServiceRecallOptions,
    OpenClawMemServiceRecallReport, OpenClawMemServiceRecallStatus, OpenClawMemServiceStatus,
    OpenClawMemServiceStatusOptions, OpenClawMemServiceStatusReport,
    OpenClawMemServiceStoreOptions, OpenClawMemServiceStoreReport, OpenClawMemServiceStoreStatus,
    build_memory_prompt_context, export_memory_credentials, inspect_openclaw_mem_service,
    memory_canvas_latest_file, memory_canvas_latest_file_for_agent, memory_canvas_receipts_file,
    memory_canvas_receipts_file_for_agent, memory_credentials_env_file,
    memory_credentials_receipt_file, memory_hook_latest_file, memory_hook_receipts_file,
    memory_lifecycle_latest_file, memory_lifecycle_latest_file_for_agent,
    memory_lifecycle_receipts_file, memory_lifecycle_receipts_file_for_agent,
    memory_prompt_context_latest_file, memory_prompt_context_latest_file_for_agent,
    memory_prompt_context_receipts_file, memory_prompt_context_receipts_file_for_agent,
    memory_search_latest_file, memory_search_receipts_file, memory_slot_receipts_file,
    memory_store_proposals_file, memory_vector_recall_latest_file,
    memory_vector_recall_receipts_file, openclaw_mem_service_proposal_receipts_file_for_agent,
    openclaw_mem_service_proposals_file_for_agent,
    openclaw_mem_service_recall_latest_file_for_agent,
    openclaw_mem_service_recall_receipts_file_for_agent, openclaw_mem_service_status_latest_file,
    openclaw_mem_service_status_receipts_file, openclaw_mem_service_store_file_for_agent,
    openclaw_mem_service_store_receipts_file_for_agent, propose_openclaw_mem_service_memory,
    recall_openclaw_mem_service, record_memory_lifecycle_turn, run_memory_canvas_worker,
    run_memory_hook_adapter, search_imported_memory, search_imported_vector_memory,
    search_imported_vector_memory_with_embedding, store_openclaw_mem_service_memory,
    write_memory_prompt_context_receipt, write_memory_search_receipt,
    write_memory_vector_recall_receipt,
};
pub use ops::{
    OpsBackupEntry, OpsBackupOptions, OpsBackupReport, OpsControlAction, OpsControlOptions,
    OpsControlReport, OpsCutoverReceiptOptions, OpsCutoverReceiptReport, OpsStopFileStatus,
    create_ops_backup, record_ops_control, record_ops_cutover_receipt,
};
pub use progress::{
    AgentProgressContext, AgentProgressDeliveryAction, AgentProgressDeliveryMessageKind,
    AgentProgressDeliveryPending, AgentProgressDeliveryPlanOptions,
    AgentProgressDeliveryPlanReport, AgentProgressDeliveryPlanSummary,
    AgentProgressDeliveryReceipt, AgentProgressDeliveryRecordOptions, AgentProgressDeliveryStatus,
    AgentProgressEvent, AgentProgressKind, AgentProgressStatus,
    agent_progress_delivery_receipts_file, agent_progress_delivery_state_file,
    agent_progress_events_file, append_agent_progress_event, plan_agent_progress_delivery,
    record_agent_progress_delivery, render_agent_progress_panel, sanitize_progress_preview,
};
pub use prompt::{
    PromptAssemblyOptions, PromptBundle, PromptBundleFiles, PromptBundleSummary, PromptSection,
    PromptSectionKind, assemble_prompt_bundle, write_prompt_bundle,
};
pub use registry::{
    AgentDefaults, AgentProfile, AgentProfileSource, AgentRegistry, ChannelRegistry, PluginProfile,
    ProviderProfile, load_agent_registry,
};
pub use runtime_pipeline::{
    RuntimeRunOnceOptions, RuntimeRunOnceReceipt, RuntimeRunOnceReport, RuntimeRunOnceStatus,
    run_runtime_queue_once,
};
pub use runtime_queue::{
    RuntimeQueueEnqueueOptions, RuntimeQueueEnqueueReport, RuntimeQueueItem,
    RuntimeQueueItemStatus, RuntimeQueueReceipt, RuntimeQueueReceiptStatus, RuntimeQueueSource,
    RuntimeQueueSourceKind, enqueue_channel_step,
};
pub use runtime_worker::{
    RuntimeExecutionReceipt, RuntimeExecutionReceiptStatus, RuntimeQueueCapacityOptions,
    RuntimeQueueCapacityReport, RuntimeQueuePrepareOptions, RuntimeQueuePrepareReport,
    RuntimeQueuePreparedItem, inspect_runtime_queue_capacity, prepare_runtime_queue_item,
    release_runtime_queue_lease,
};
pub use skills::{
    HARNESS_BUILTIN_SKILL_NAMESPACE, SkillIndex, SkillIndexFile, SkillIndexOrigin,
    SkillIndexSummary, SkillRecord, SkillSelection, SkillSelectionQuery, SkillSourceKind,
    build_harness_skill_index, build_runtime_skill_index, build_source_skill_index, select_skills,
    write_skill_index,
};
pub use status::{
    HarnessChannelStatus, HarnessJsonlStatus, HarnessMemoryStatus, HarnessOperationalLogStatus,
    HarnessOutboxStatus, HarnessPluginStatus, HarnessRuntimeReceiptStatus, HarnessRuntimeStatus,
    HarnessStatusOptions, HarnessStatusReport, collect_harness_status,
};
pub use subagents::{
    SubagentLedger, SubagentLedgerSummary, SubagentPlan, SubagentPlanAction, SubagentPlanEntry,
    SubagentPlanFile, SubagentPlanInput, SubagentPlanSummary, SubagentRun, SubagentRunStatus,
    load_subagent_ledger, plan_subagents, write_subagent_plan,
};
pub use supervisor::{
    WindowsSupervisorPlanOptions, WindowsSupervisorPlanReport, WindowsSupervisorScript,
    WindowsSupervisorTask, write_windows_supervisor_plan,
};
pub use turns::{
    TurnAgent, TurnDispatch, TurnModelPolicy, TurnPlan, TurnPlanFile, TurnPlanInput,
    TurnPromptFile, TurnThinkingPolicy, build_turn_plan, write_turn_plan,
};
pub use worker_adapters::{
    DeterministicCronWorkerEnqueueOptions, NativeCronWorkerEnqueueOptions,
    SubagentWorkerEnqueueOptions, WorkerAdapterEnqueueReport, WorkerAdapterEnqueueSummary,
    WorkerAdapterJobRef, enqueue_deterministic_cron_workers, enqueue_native_cron_workers,
    enqueue_subagent_workers,
};
pub use workers::{
    WorkerCancelOptions, WorkerCancelReport, WorkerCapacityBlockedSummary, WorkerDispatchConfig,
    WorkerEnqueueOptions, WorkerEnqueueReport, WorkerJob, WorkerJobExecutionResult, WorkerJobKind,
    WorkerJobStatus, WorkerLaneStatus, WorkerReapStaleOptions, WorkerReapStaleReport,
    WorkerRunOnceOptions, WorkerRunOnceReport, WorkerRunOnceStatus, WorkerStatusOptions,
    WorkerStatusReport, WorkerStatusTotals, cancel_worker_job, collect_worker_status,
    enqueue_worker_job, init_worker_store, load_worker_dispatch_config, reap_stale_worker_jobs,
    run_worker_once, worker_db_file,
};

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
