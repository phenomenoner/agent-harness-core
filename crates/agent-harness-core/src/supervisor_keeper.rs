use std::fs::{self, File};
use std::io::{self, Read, Write};
#[cfg(windows)]
use std::os::windows::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use ring::digest::{Context, SHA256};
use serde::Serialize;
use serde_json::Value;

pub const WINDOWS_RECONCILE_KEEPER_PLAN_SCHEMA: &str =
    "agent-harness.windows-reconcile-keeper-plan.v1";
pub const WINDOWS_RECONCILE_KEEPER_OBSERVATION_SCHEMA: &str =
    "agent-harness.windows-reconcile-keeper-observation.v1";

const DEFAULT_KEEPER_TASK_NAME_PREFIX: &str = "AgentHarness-Supervisor-Reconcile-Keeper";
const KEEPER_STATE_SCHEMA: &str = "agent-harness.supervisor-reconcile-keeper.v1";
const TASK_RESTART_COUNT: u16 = 255;
const KEEPER_RECOVERY_TRIGGER_KIND: &str = "Time";
const KEEPER_RECOVERY_START_BOUNDARY: &str = "2000-01-01T00:00:00";
const KEEPER_RECOVERY_INTERVAL: &str = "PT1M";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowsReconcileKeeperPlanOptions {
    pub harness_home: PathBuf,
    pub source_home: PathBuf,
    pub workspace: Option<PathBuf>,
    pub runtime_workspace: PathBuf,
    pub harness_cli: PathBuf,
    pub codex_executable: Option<PathBuf>,
    pub node_executable: Option<PathBuf>,
    pub discord_gateway_script: Option<PathBuf>,
    pub user_id: String,
    pub task_name: Option<String>,
    /// Live plans are restricted to the deployed harness tree. Tests and
    /// developer staging must opt out explicitly.
    pub live_deployment: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WindowsReconcileKeeperPlan {
    pub schema: &'static str,
    pub keeper_state_schema: &'static str,
    pub deployment_mode: &'static str,
    pub deployment_identity: String,
    pub harness_home: PathBuf,
    pub source_home: PathBuf,
    pub workspace: Option<PathBuf>,
    pub runtime_workspace: PathBuf,
    pub output_dir: PathBuf,
    pub plan_file: PathBuf,
    pub stop_file: PathBuf,
    pub state_file: PathBuf,
    pub lease_file: PathBuf,
    pub observation_file: PathBuf,
    pub fallback_receipt_file: PathBuf,
    pub executable: WindowsReconcileKeeperExecutable,
    pub action: WindowsReconcileKeeperAction,
    pub scheduler_action: WindowsReconcileKeeperSchedulerAction,
    pub task: WindowsReconcileKeeperTask,
    pub scripts: Vec<WindowsReconcileKeeperScript>,
    pub plan_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WindowsReconcileKeeperExecutable {
    pub path: PathBuf,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WindowsReconcileKeeperAction {
    pub executable: PathBuf,
    pub arguments: String,
    pub argument_vector: Vec<String>,
    pub working_directory: PathBuf,
    pub direct_executable: bool,
    pub digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WindowsReconcileKeeperSchedulerAction {
    pub executable: PathBuf,
    pub arguments: String,
    pub argument_vector: Vec<String>,
    pub working_directory: PathBuf,
    pub window_style: &'static str,
    pub child_create_no_window: bool,
    pub synchronous_wait: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WindowsReconcileKeeperTask {
    pub name: String,
    pub path: &'static str,
    pub description: String,
    pub principal: WindowsReconcileKeeperPrincipal,
    pub trigger: WindowsReconcileKeeperTrigger,
    pub settings: WindowsReconcileKeeperSettings,
    pub registration_enabled: bool,
    pub ownership_identity: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WindowsReconcileKeeperPrincipal {
    pub user_id: String,
    pub current_user_only: bool,
    pub logon_type: &'static str,
    pub run_level: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WindowsReconcileKeeperTrigger {
    pub kind: &'static str,
    pub user_id: String,
    pub recovery_kind: &'static str,
    pub recovery_start_boundary: &'static str,
    pub recovery_interval: &'static str,
    pub recovery_duration: Option<&'static str>,
    pub recovery_stop_at_duration_end: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WindowsReconcileKeeperSettings {
    pub multiple_instances: &'static str,
    pub restart_interval: &'static str,
    pub restart_count: u16,
    pub start_when_available: bool,
    pub allow_start_on_batteries: bool,
    pub stop_if_going_on_batteries: bool,
    pub execution_time_limit: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WindowsReconcileKeeperScript {
    pub name: &'static str,
    pub path: PathBuf,
    pub purpose: &'static str,
    pub sha256: String,
}

pub fn write_windows_reconcile_keeper_plan(
    options: WindowsReconcileKeeperPlanOptions,
) -> io::Result<WindowsReconcileKeeperPlan> {
    if options.live_deployment {
        return write_and_promote_windows_reconcile_keeper_plan(options);
    }
    let bundle = build_windows_reconcile_keeper_plan(options)?;
    write_windows_reconcile_keeper_bundle_direct(bundle)
}

pub fn write_and_promote_windows_reconcile_keeper_plan(
    options: WindowsReconcileKeeperPlanOptions,
) -> io::Result<WindowsReconcileKeeperPlan> {
    if !options.live_deployment {
        return Err(invalid_input(
            "atomic keeper promotion is reserved for live deployment plans",
        ));
    }
    let bundle = build_windows_reconcile_keeper_plan(options)?;
    promote_windows_reconcile_keeper_bundle(bundle)
}

struct WindowsReconcileKeeperBundle {
    plan: WindowsReconcileKeeperPlan,
    generated_scripts: Vec<(PathBuf, String)>,
}

fn build_windows_reconcile_keeper_plan(
    options: WindowsReconcileKeeperPlanOptions,
) -> io::Result<WindowsReconcileKeeperBundle> {
    let harness_home = canonical_directory(&options.harness_home, "harness home")?;
    let source_home = canonical_directory(&options.source_home, "source home")?;
    let workspace = options
        .workspace
        .as_deref()
        .map(|path| canonical_directory(path, "workspace"))
        .transpose()?;
    let runtime_workspace = canonical_directory(&options.runtime_workspace, "runtime workspace")?;
    let harness_cli = canonical_file(&options.harness_cli, "harness CLI")?;
    let codex_executable = options
        .codex_executable
        .as_deref()
        .map(|path| canonical_file(path, "Codex executable"))
        .transpose()?;
    let node_executable = options
        .node_executable
        .as_deref()
        .map(|path| canonical_file(path, "Node executable"))
        .transpose()?;
    let discord_gateway_script = options
        .discord_gateway_script
        .as_deref()
        .map(|path| canonical_file(path, "Discord gateway script"))
        .transpose()?;

    let user_id = options.user_id.trim();
    if user_id.is_empty() {
        return Err(invalid_input("scheduled-task user id must not be empty"));
    }
    let default_task_name = format!(
        "{DEFAULT_KEEPER_TASK_NAME_PREFIX}-{}",
        &deployment_identity_digest(&harness_home)[..12]
    );
    let task_name = options
        .task_name
        .as_deref()
        .unwrap_or(&default_task_name)
        .trim();
    validate_task_name(task_name)?;

    let output_dir = harness_home
        .join("state")
        .join("supervisor")
        .join("windows-reconcile-keeper");
    let scripts_dir = output_dir.join("scripts");
    let plan_file = output_dir.join("keeper-plan.json");
    let stop_file = harness_home
        .join("state")
        .join("supervisor")
        .join("stop")
        .join("supervisor-reconcile.stop");
    let state_file = harness_home
        .join("state")
        .join("supervisor")
        .join("reconcile-keeper.json");
    let lease_file = harness_home
        .join("state")
        .join("supervisor")
        .join("reconcile-keeper.lock");
    let observation_file = output_dir.join("readback-latest.json");
    let fallback_receipt_file = output_dir.join("stop-fallback-latest.json");

    let install_script = scripts_dir.join("install.ps1");
    let hidden_launch_script = scripts_dir.join("launch-hidden.ps1");
    let start_script = scripts_dir.join("start.ps1");
    let stop_script = scripts_dir.join("stop.ps1");
    let uninstall_script = scripts_dir.join("uninstall.ps1");
    let rollback_script = scripts_dir.join("rollback.ps1");
    let readback_script = scripts_dir.join("readback.ps1");
    let scripts = vec![
        WindowsReconcileKeeperScript {
            name: "launch-hidden",
            path: hidden_launch_script.clone(),
            purpose: "launch and synchronously own the reconcile master without a console window",
            sha256: String::new(),
        },
        WindowsReconcileKeeperScript {
            name: "install",
            path: install_script.clone(),
            purpose: "register the user-scoped reconcile keeper disabled",
            sha256: String::new(),
        },
        WindowsReconcileKeeperScript {
            name: "start",
            path: start_script.clone(),
            purpose: "clear the supervised hold and start the keeper",
            sha256: String::new(),
        },
        WindowsReconcileKeeperScript {
            name: "stop",
            path: stop_script.clone(),
            purpose: "request graceful stop with a receipted forced-stop fallback",
            sha256: String::new(),
        },
        WindowsReconcileKeeperScript {
            name: "uninstall",
            path: uninstall_script.clone(),
            purpose: "gracefully stop and unregister the keeper",
            sha256: String::new(),
        },
        WindowsReconcileKeeperScript {
            name: "rollback",
            path: rollback_script.clone(),
            purpose: "restore the exact prior launch authority from an external rollback package",
            sha256: String::new(),
        },
        WindowsReconcileKeeperScript {
            name: "readback",
            path: readback_script.clone(),
            purpose: "atomically observe the registered task and keeper runtime state",
            sha256: String::new(),
        },
    ];

    let mut argument_vector = vec![
        "supervisor-reconcile".to_string(),
        "--all".to_string(),
        "--auto-reconcile".to_string(),
        "--iterations".to_string(),
        "0".to_string(),
        "--quiet".to_string(),
        "--stop-file".to_string(),
        path_arg(&stop_file),
        "--harness-home".to_string(),
        path_arg(&harness_home),
        "--source-home".to_string(),
        path_arg(&source_home),
        "--runtime-workspace".to_string(),
        path_arg(&runtime_workspace),
        "--harness-cli".to_string(),
        path_arg(&harness_cli),
    ];
    push_optional_path_arg(&mut argument_vector, "--workspace", workspace.as_deref());
    push_optional_path_arg(
        &mut argument_vector,
        "--codex-exe",
        codex_executable.as_deref(),
    );
    push_optional_path_arg(
        &mut argument_vector,
        "--node-exe",
        node_executable.as_deref(),
    );
    push_optional_path_arg(
        &mut argument_vector,
        "--gateway-script",
        discord_gateway_script.as_deref(),
    );
    let arguments = argument_vector
        .iter()
        .map(|argument| windows_quote_argument(argument))
        .collect::<Vec<_>>()
        .join(" ");

    let executable = WindowsReconcileKeeperExecutable {
        path: harness_cli.clone(),
        sha256: sha256_file(&harness_cli)?,
    };
    let mut action = WindowsReconcileKeeperAction {
        executable: harness_cli,
        arguments,
        argument_vector,
        working_directory: runtime_workspace.clone(),
        direct_executable: true,
        digest: String::new(),
    };
    action.digest = action_digest(&action)?;
    let scheduler_argument_vector = vec![
        "-NoLogo".to_string(),
        "-NoProfile".to_string(),
        "-NonInteractive".to_string(),
        "-WindowStyle".to_string(),
        "Hidden".to_string(),
        "-ExecutionPolicy".to_string(),
        "Bypass".to_string(),
        "-File".to_string(),
        path_arg(&hidden_launch_script),
    ];
    let scheduler_arguments = scheduler_argument_vector
        .iter()
        .map(|argument| windows_quote_argument(argument))
        .collect::<Vec<_>>()
        .join(" ");
    let scheduler_action = WindowsReconcileKeeperSchedulerAction {
        executable: windows_powershell_executable(),
        arguments: scheduler_arguments,
        argument_vector: scheduler_argument_vector,
        working_directory: runtime_workspace.clone(),
        window_style: "hidden",
        child_create_no_window: true,
        synchronous_wait: true,
    };
    let deployment_identity = deployment_identity_digest(&harness_home);
    let ownership_identity = format!(
        "agent-harness.windows-reconcile-keeper:{}:{}",
        deployment_identity, action.digest
    );
    let task = WindowsReconcileKeeperTask {
        name: task_name.to_string(),
        path: "\\",
        description: format!(
            "Agent Harness reconcile keeper; taskPath=\\; ownership={ownership_identity}"
        ),
        principal: WindowsReconcileKeeperPrincipal {
            user_id: user_id.to_string(),
            current_user_only: true,
            logon_type: "Interactive",
            run_level: "Limited",
        },
        trigger: WindowsReconcileKeeperTrigger {
            kind: "AtLogOn",
            user_id: user_id.to_string(),
            recovery_kind: KEEPER_RECOVERY_TRIGGER_KIND,
            recovery_start_boundary: KEEPER_RECOVERY_START_BOUNDARY,
            recovery_interval: KEEPER_RECOVERY_INTERVAL,
            recovery_duration: None,
            recovery_stop_at_duration_end: false,
        },
        settings: WindowsReconcileKeeperSettings {
            multiple_instances: "IgnoreNew",
            restart_interval: "PT1M",
            restart_count: TASK_RESTART_COUNT,
            start_when_available: true,
            allow_start_on_batteries: true,
            stop_if_going_on_batteries: false,
            execution_time_limit: "PT0S",
        },
        registration_enabled: false,
        ownership_identity,
    };

    let mut plan = WindowsReconcileKeeperPlan {
        schema: WINDOWS_RECONCILE_KEEPER_PLAN_SCHEMA,
        keeper_state_schema: KEEPER_STATE_SCHEMA,
        deployment_mode: if options.live_deployment {
            "live"
        } else {
            "developer-staging"
        },
        deployment_identity,
        harness_home,
        source_home,
        workspace,
        runtime_workspace,
        output_dir,
        plan_file,
        stop_file,
        state_file,
        lease_file,
        observation_file,
        fallback_receipt_file,
        executable,
        action,
        scheduler_action,
        task,
        scripts,
        plan_digest: String::new(),
    };
    let generated_scripts = vec![
        (
            "launch-hidden",
            hidden_launch_script.clone(),
            hidden_launch_script_body(&plan.plan_file, &plan.harness_home),
        ),
        (
            "install",
            install_script.clone(),
            install_script_body(&plan.plan_file, &plan.harness_home, options.live_deployment),
        ),
        (
            "start",
            start_script.clone(),
            start_script_body(&plan.plan_file, &plan.harness_home, options.live_deployment),
        ),
        (
            "stop",
            stop_script.clone(),
            stop_script_body(&plan.plan_file, &plan.harness_home, options.live_deployment),
        ),
        (
            "uninstall",
            uninstall_script.clone(),
            uninstall_script_body(&plan.plan_file, &plan.harness_home, options.live_deployment),
        ),
        (
            "rollback",
            rollback_script.clone(),
            rollback_script_body(&plan.plan_file, &plan.harness_home),
        ),
        (
            "readback",
            readback_script.clone(),
            readback_script_body(&plan.plan_file, &plan.harness_home),
        ),
    ];
    for script in &mut plan.scripts {
        let (_, _, body) = generated_scripts
            .iter()
            .find(|(name, _, _)| *name == script.name)
            .ok_or_else(|| invalid_input(format!("missing generated script {}", script.name)))?;
        script.sha256 = sha256_bytes(body.as_bytes());
    }
    plan.plan_digest = plan_digest(&plan)?;
    if options.live_deployment {
        validate_live_windows_reconcile_keeper_plan(
            &serde_json::to_value(&plan).map_err(io::Error::other)?,
        )
        .map_err(invalid_input)?;
    }

    Ok(WindowsReconcileKeeperBundle {
        plan,
        generated_scripts: generated_scripts
            .into_iter()
            .map(|(_, path, body)| (path, body))
            .collect(),
    })
}

fn write_windows_reconcile_keeper_bundle_direct(
    bundle: WindowsReconcileKeeperBundle,
) -> io::Result<WindowsReconcileKeeperPlan> {
    ensure_keeper_write_path_physically_contained(
        &bundle.plan.harness_home,
        &bundle.plan.output_dir,
        "keeper output directory",
    )?;
    fs::create_dir_all(&bundle.plan.output_dir)?;
    ensure_keeper_write_path_physically_contained(
        &bundle.plan.harness_home,
        &bundle.plan.output_dir,
        "created keeper output directory",
    )?;
    for (path, body) in &bundle.generated_scripts {
        ensure_keeper_write_path_physically_contained(
            &bundle.plan.harness_home,
            path,
            "keeper script",
        )?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
            ensure_keeper_write_path_physically_contained(
                &bundle.plan.harness_home,
                parent,
                "created keeper script directory",
            )?;
        }
        fs::write(path, body)?;
        ensure_keeper_write_path_physically_contained(
            &bundle.plan.harness_home,
            path,
            "materialized keeper script",
        )?;
    }
    ensure_keeper_write_path_physically_contained(
        &bundle.plan.harness_home,
        &bundle.plan.plan_file,
        "keeper plan",
    )?;
    crate::write_json_atomic(&bundle.plan.plan_file, &bundle.plan)?;
    ensure_keeper_write_path_physically_contained(
        &bundle.plan.harness_home,
        &bundle.plan.plan_file,
        "materialized keeper plan",
    )?;
    verify_materialized_keeper_bundle(&bundle.plan.output_dir, &bundle.plan)?;
    Ok(bundle.plan)
}

fn promote_windows_reconcile_keeper_bundle(
    bundle: WindowsReconcileKeeperBundle,
) -> io::Result<WindowsReconcileKeeperPlan> {
    let output_dir = bundle.plan.output_dir.clone();
    let parent = output_dir
        .parent()
        .ok_or_else(|| invalid_input("keeper output directory has no parent"))?;
    ensure_keeper_write_path_physically_contained(
        &bundle.plan.harness_home,
        parent,
        "keeper output parent",
    )?;
    fs::create_dir_all(parent)?;
    ensure_keeper_write_path_physically_contained(
        &bundle.plan.harness_home,
        parent,
        "created keeper output parent",
    )?;
    let nonce = format!(
        "{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    );
    let candidate_dir = parent.join(format!(".windows-reconcile-keeper.candidate.{nonce}"));
    let previous_dir = parent.join(format!(".windows-reconcile-keeper.previous.{nonce}"));
    let failed_dir = parent.join(format!(".windows-reconcile-keeper.failed.{nonce}"));
    for (path, label) in [
        (&candidate_dir, "keeper candidate directory"),
        (&previous_dir, "keeper previous directory"),
        (&failed_dir, "keeper failed directory"),
        (&output_dir, "keeper output directory"),
    ] {
        ensure_keeper_write_path_physically_contained(&bundle.plan.harness_home, path, label)?;
    }
    fs::create_dir(&candidate_dir)?;
    ensure_keeper_write_path_physically_contained(
        &bundle.plan.harness_home,
        &candidate_dir,
        "created keeper candidate directory",
    )?;

    let materialize = (|| -> io::Result<()> {
        for (canonical_path, body) in &bundle.generated_scripts {
            let relative = canonical_path.strip_prefix(&output_dir).map_err(|_| {
                invalid_input(format!(
                    "keeper script {} is outside canonical output {}",
                    canonical_path.display(),
                    output_dir.display()
                ))
            })?;
            let candidate_path = candidate_dir.join(relative);
            ensure_keeper_write_path_physically_contained(
                &bundle.plan.harness_home,
                &candidate_path,
                "candidate keeper script",
            )?;
            if let Some(parent) = candidate_path.parent() {
                fs::create_dir_all(parent)?;
                ensure_keeper_write_path_physically_contained(
                    &bundle.plan.harness_home,
                    parent,
                    "created candidate keeper script directory",
                )?;
            }
            let mut file = File::create(&candidate_path)?;
            file.write_all(body.as_bytes())?;
            file.sync_all()?;
            ensure_keeper_write_path_physically_contained(
                &bundle.plan.harness_home,
                &candidate_path,
                "materialized candidate keeper script",
            )?;
        }
        let plan_bytes = serde_json::to_vec_pretty(&bundle.plan).map_err(io::Error::other)?;
        let candidate_plan = candidate_dir.join("keeper-plan.json");
        ensure_keeper_write_path_physically_contained(
            &bundle.plan.harness_home,
            &candidate_plan,
            "candidate keeper plan",
        )?;
        let mut plan_file = File::create(&candidate_plan)?;
        plan_file.write_all(&plan_bytes)?;
        plan_file.sync_all()?;
        ensure_keeper_write_path_physically_contained(
            &bundle.plan.harness_home,
            &candidate_plan,
            "materialized candidate keeper plan",
        )?;
        verify_materialized_keeper_bundle(&candidate_dir, &bundle.plan)
    })();
    if let Err(error) = materialize {
        return Err(io::Error::new(
            error.kind(),
            format!(
                "keeper candidate generation failed at {}; candidate retained: {error}",
                candidate_dir.display()
            ),
        ));
    }

    let had_previous = output_dir.exists();
    if had_previous {
        ensure_keeper_write_path_physically_contained(
            &bundle.plan.harness_home,
            &output_dir,
            "current keeper generation before preservation",
        )?;
        ensure_keeper_write_path_physically_contained(
            &bundle.plan.harness_home,
            &previous_dir,
            "previous keeper generation destination",
        )?;
        fs::rename(&output_dir, &previous_dir).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "failed to preserve current keeper generation {} at {}: {error}",
                    output_dir.display(),
                    previous_dir.display()
                ),
            )
        })?;
        ensure_keeper_write_path_physically_contained(
            &bundle.plan.harness_home,
            &previous_dir,
            "preserved keeper generation",
        )?;
    }
    ensure_keeper_write_path_physically_contained(
        &bundle.plan.harness_home,
        &candidate_dir,
        "candidate keeper generation before promotion",
    )?;
    ensure_keeper_write_path_physically_contained(
        &bundle.plan.harness_home,
        &output_dir,
        "keeper promotion destination",
    )?;
    if let Err(promote_error) = fs::rename(&candidate_dir, &output_dir) {
        let restore_error = if had_previous {
            ensure_keeper_write_path_physically_contained(
                &bundle.plan.harness_home,
                &previous_dir,
                "preserved keeper generation before restore",
            )
            .and_then(|_| {
                ensure_keeper_write_path_physically_contained(
                    &bundle.plan.harness_home,
                    &output_dir,
                    "keeper restore destination",
                )
            })
            .and_then(|_| fs::rename(&previous_dir, &output_dir))
            .err()
        } else {
            None
        };
        return Err(io::Error::new(
            promote_error.kind(),
            format!(
                "failed to atomically promote keeper candidate {} to {}; prior restore error: {}",
                candidate_dir.display(),
                output_dir.display(),
                restore_error
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "none".to_string())
            ),
        ));
    }
    ensure_keeper_write_path_physically_contained(
        &bundle.plan.harness_home,
        &output_dir,
        "promoted keeper generation",
    )?;

    if let Err(verification_error) = verify_materialized_keeper_bundle(&output_dir, &bundle.plan) {
        let quarantine_error = ensure_keeper_write_path_physically_contained(
            &bundle.plan.harness_home,
            &output_dir,
            "failed keeper generation before quarantine",
        )
        .and_then(|_| {
            ensure_keeper_write_path_physically_contained(
                &bundle.plan.harness_home,
                &failed_dir,
                "failed keeper quarantine destination",
            )
        })
        .and_then(|_| fs::rename(&output_dir, &failed_dir))
        .err();
        let restore_error = if had_previous {
            ensure_keeper_write_path_physically_contained(
                &bundle.plan.harness_home,
                &previous_dir,
                "preserved keeper generation before verification restore",
            )
            .and_then(|_| {
                ensure_keeper_write_path_physically_contained(
                    &bundle.plan.harness_home,
                    &output_dir,
                    "keeper verification restore destination",
                )
            })
            .and_then(|_| fs::rename(&previous_dir, &output_dir))
            .err()
        } else {
            None
        };
        return Err(io::Error::new(
            verification_error.kind(),
            format!(
                "promoted keeper generation failed readback verification and was not accepted: {verification_error}; failed-generation quarantine error: {}; prior restore error: {}",
                quarantine_error
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "none".to_string()),
                restore_error
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "none".to_string())
            ),
        ));
    }
    Ok(bundle.plan)
}

fn verify_materialized_keeper_bundle(
    physical_output_dir: &Path,
    expected_plan: &WindowsReconcileKeeperPlan,
) -> io::Result<()> {
    ensure_keeper_write_path_physically_contained(
        &expected_plan.harness_home,
        physical_output_dir,
        "keeper bundle readback directory",
    )?;
    let plan_path = physical_output_dir.join("keeper-plan.json");
    ensure_keeper_write_path_physically_contained(
        &expected_plan.harness_home,
        &plan_path,
        "keeper plan readback",
    )?;
    let persisted: Value =
        serde_json::from_slice(&fs::read(&plan_path)?).map_err(io::Error::other)?;
    let observed_plan_digest =
        windows_reconcile_keeper_plan_digest(&persisted).map_err(invalid_input)?;
    if persisted.get("planDigest").and_then(Value::as_str) != Some(observed_plan_digest.as_str())
        || persisted.get("planDigest").and_then(Value::as_str)
            != Some(expected_plan.plan_digest.as_str())
    {
        return Err(invalid_input(format!(
            "materialized keeper plan digest does not verify at {}",
            plan_path.display()
        )));
    }
    for script in &expected_plan.scripts {
        let relative = script
            .path
            .strip_prefix(&expected_plan.output_dir)
            .map_err(|_| {
                invalid_input(format!(
                    "keeper script {} is outside output directory",
                    script.path.display()
                ))
            })?;
        let physical_path = physical_output_dir.join(relative);
        ensure_keeper_write_path_physically_contained(
            &expected_plan.harness_home,
            &physical_path,
            "keeper script readback",
        )?;
        let observed = sha256_file(&physical_path)?;
        if observed != script.sha256 {
            return Err(invalid_input(format!(
                "keeper script digest mismatch at {}",
                physical_path.display()
            )));
        }
    }
    Ok(())
}

fn install_script_body(plan_file: &Path, harness_home: &Path, live_deployment: bool) -> String {
    let mut body = script_control_header(plan_file, harness_home, live_deployment);
    body.push_str(atomic_json_function_body());
    body.push_str(ensure_keeper_hold_function_body());
    body.push_str(account_identity_function_body());
    body.push_str(task_scheduler_guard_function_body());
    body.push_str(legacy_task_preflight_function_body());
    if live_deployment {
        body.push_str("$KeeperFixtureMode = $false\n");
    } else {
        body.push_str(
            r#"$KeeperFixtureMode = ($Plan.deploymentMode -eq 'developer-staging') -and (-not [string]::IsNullOrWhiteSpace($env:AGENT_HARNESS_KEEPER_TEST_TASK_OBSERVATION_FILE))
"#,
        );
    }
    body.push_str(
        r#"Assert-KeeperLiveControlAuthorization -Action 'restart'
Assert-LegacySupervisorTasksQuiescent -Phase 'install'

$CurrentUserId = if ($KeeperFixtureMode) { [string]$Plan.task.principal.userId } else { [Security.Principal.WindowsIdentity]::GetCurrent().Name }
if (-not (Test-KeeperAccountIdentityExact -ExpectedUserId ([string]$Plan.task.principal.userId) -ObservedUserId $CurrentUserId)) {
  throw "keeper task principal mismatch: plan=$($Plan.task.principal.userId) current=$CurrentUserId"
}
if (-not (Test-Path -LiteralPath $Plan.executable.path -PathType Leaf)) {
  throw "canonical harness CLI is missing: $($Plan.executable.path)"
}
$ObservedExecutableDigest = Get-FileSha256 -Path $Plan.executable.path
if ($ObservedExecutableDigest -ne [string]$Plan.executable.sha256) {
  throw "canonical harness CLI digest mismatch"
}
$Existing = Get-KeeperTaskSnapshot
if ($Existing.exists -eq $true -and [string]$Existing.task.state -eq 'Running') {
  throw 'keeper task is running; execute the guarded keeper stop before install or upgrade'
}
$ExactCurrent = Test-KeeperTaskExact -Snapshot $Existing
$SameDeploymentStale = Test-KeeperTaskSameDeploymentStale -Snapshot $Existing
if ($Existing.exists -eq $true -and -not $ExactCurrent -and -not $SameDeploymentStale) {
  throw "refusing to overwrite foreign scheduled task: $($Plan.task.name)"
}

if ($ExactCurrent -and $Existing.task.enabled -eq $false -and [string]$Existing.task.state -eq 'Disabled') {
  if ($KeeperFixtureMode) {
    Write-KeeperFixtureEffect -Action 'install-idempotent'
  } else {
    Invoke-KeeperScript -Path (($Plan.scripts | Where-Object { $_.name -eq 'readback' }).path)
  }
  return
}

$UpgradeReceiptFile = Join-Path ([string]$Plan.outputDir) 'install-upgrade-rollback-latest.json'
$PriorKeeperXml = $null
$UpgradeReceipt = $null
if ($SameDeploymentStale) {
  if (-not (Test-ExactKeeperHold)) {
    throw 'same-deployment keeper upgrade requires an exact persistent global hold'
  }
  $PriorKeeperXml = if ($KeeperFixtureMode) {
    [string]$Existing.exportedXml
  } else {
    Export-ScheduledTask -TaskPath $Plan.task.path -TaskName $Plan.task.name -ErrorAction Stop
  }
  if ([string]::IsNullOrWhiteSpace($PriorKeeperXml)) {
    throw 'same-deployment keeper upgrade cannot proceed without an exported prior task definition'
  }
  $PriorKeeperXml = Set-KeeperTaskXmlDisabled -Xml $PriorKeeperXml
  $UpgradeReceipt = [ordered]@{
    schema = 'agent-harness.windows-reconcile-keeper-install-upgrade-rollback.v1'
    deploymentIdentity = $Plan.deploymentIdentity
    planDigest = $Plan.planDigest
    taskPath = $Plan.task.path
    taskName = $Plan.task.name
    priorState = [string]$Existing.task.state
    priorEnabled = [bool]$Existing.task.enabled
    priorDefinitionXml = $PriorKeeperXml
    status = 'prepared'
    observedAtMs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
  }
  Write-AtomicJson -Path $UpgradeReceiptFile -Value $UpgradeReceipt
}

try {
  if ($SameDeploymentStale) {
    $StaleBeforeUnregister = Assert-KeeperTaskSameDeploymentStale -Mutation 'Unregister'
    if ($KeeperFixtureMode) {
      Write-KeeperFixtureEffect -Action 'unregister-task'
    } else {
      Unregister-ScheduledTask -TaskPath $Plan.task.path -TaskName $Plan.task.name -Confirm:$false -ErrorAction Stop
    }
  }

  $BeforeRegister = Get-KeeperTaskSnapshot
  if ($BeforeRegister.exists -eq $true) {
    if (-not (Test-KeeperTaskExact -Snapshot $BeforeRegister)) {
      throw 'keeper task appeared or changed immediately before Register; refusing scheduler mutation'
    }
  } else {
    if ($KeeperFixtureMode) {
      Write-KeeperFixtureEffect -Action 'register-disabled-task'
    } else {
      $TaskXml = New-KeeperTaskXml
      Register-ScheduledTask -TaskPath $Plan.task.path -TaskName $Plan.task.name -Xml $TaskXml -ErrorAction Stop | Out-Null
    }
    $AfterRegister = Assert-KeeperTaskExact -Mutation 'post-Register validation'
    if ($AfterRegister.task.enabled -eq $true -or [string]$AfterRegister.task.state -ne 'Disabled') {
      throw 'keeper registration was not atomically disabled'
    }
  }

  $BeforeDisable = Get-KeeperTaskSnapshot
  if ($BeforeDisable.exists -ne $true -or -not (Test-KeeperTaskExact -Snapshot $BeforeDisable)) {
    throw 'keeper task is missing or changed before install disabled-state proof'
  }
  if ($BeforeDisable.task.enabled -eq $true -or [string]$BeforeDisable.task.state -ne 'Disabled') {
    if (-not (Test-ExactKeeperHold) -and $Existing.exists -eq $true) {
      throw 'disabling an existing keeper task during install requires an exact persistent global hold'
    }
    [void](Invoke-ExactKeeperTaskMutation -Mutation 'Disable')
    $AfterDisable = Assert-KeeperTaskExact -Mutation 'post-Disable validation'
    if ($AfterDisable.task.enabled -eq $true -or [string]$AfterDisable.task.state -ne 'Disabled') {
      throw 'keeper install did not leave the exact task disabled'
    }
  }
  if ($null -ne $UpgradeReceipt) {
    $UpgradeReceipt.status = 'upgraded'
    $UpgradeReceipt.completedAtMs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
    Write-AtomicJson -Path $UpgradeReceiptFile -Value $UpgradeReceipt
  }
} catch {
  $InstallFailure = $_.Exception.Message
  if ($null -eq $UpgradeReceipt) { throw }
  Ensure-KeeperHold -Reason 'keeper-install-upgrade-rollback'
  $UpgradeErrors = @()
  try {
    $RollbackCurrent = Get-KeeperTaskSnapshot
    if ($RollbackCurrent.exists -eq $true) {
      if (Test-KeeperTaskExact -Snapshot $RollbackCurrent) {
        [void](Invoke-ExactKeeperTaskMutation -Mutation 'Unregister')
      } elseif (Test-KeeperTaskSameDeploymentStale -Snapshot $RollbackCurrent) {
        $RollbackStale = Assert-KeeperTaskSameDeploymentStale -Mutation 'upgrade rollback Unregister'
        if ($KeeperFixtureMode) {
          Write-KeeperFixtureEffect -Action 'unregister-partial-upgrade-task'
        } else {
          Unregister-ScheduledTask -TaskPath $Plan.task.path -TaskName $Plan.task.name -Confirm:$false -ErrorAction Stop
        }
      } else {
        throw 'foreign replacement appeared during upgrade rollback'
      }
    }
    $BeforeRestore = Get-KeeperTaskSnapshot
    if ($BeforeRestore.exists -eq $true) {
      throw 'task still exists immediately before prior keeper restore'
    }
    if ($KeeperFixtureMode) {
      Write-KeeperFixtureEffect -Action 'restore-prior-disabled-task'
    } else {
      Register-ScheduledTask -TaskPath $Plan.task.path -TaskName $Plan.task.name -Xml $PriorKeeperXml -ErrorAction Stop | Out-Null
      $Restored = Assert-KeeperTaskSameDeploymentStale -Mutation 'post-upgrade rollback restore'
      if ($Restored.task.enabled -eq $true -or [string]$Restored.task.state -eq 'Running') {
        throw 'restored prior keeper definition is not disabled and quiescent'
      }
    }
  } catch {
    $UpgradeErrors += $_.Exception.Message
  }
  $UpgradeReceipt.failure = $InstallFailure
  $UpgradeReceipt.rollbackErrors = $UpgradeErrors
  $UpgradeReceipt.status = if ($UpgradeErrors.Count -eq 0) { 'prior-definition-restored-disabled' } else { 'manual-recovery-required' }
  $UpgradeReceipt.completedAtMs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
  Write-AtomicJson -Path $UpgradeReceiptFile -Value $UpgradeReceipt
  if ($UpgradeErrors.Count -gt 0) {
    throw "keeper upgrade failed and prior task restore is partial; manual recovery required: $($UpgradeErrors -join '; ')"
  }
  throw "keeper upgrade failed; prior task definition was restored disabled: $InstallFailure"
}
if (-not $KeeperFixtureMode) {
  Invoke-KeeperScript -Path (($Plan.scripts | Where-Object { $_.name -eq 'readback' }).path)
}
"#,
    );
    body
}

fn hidden_launch_script_body(plan_file: &Path, harness_home: &Path) -> String {
    let mut body = script_plan_header(plan_file, harness_home);
    body.push_str(
        r#"if ([string]$Plan.schedulerAction.windowStyle -ne 'hidden' -or
    $Plan.schedulerAction.childCreateNoWindow -ne $true -or
    $Plan.schedulerAction.synchronousWait -ne $true) {
  throw 'keeper scheduler action does not require the hidden synchronous launch policy'
}
$CurrentPowerShell = [IO.Path]::GetFullPath((Join-Path $PSHOME 'powershell.exe'))
$PlannedPowerShell = [IO.Path]::GetFullPath([string]$Plan.schedulerAction.executable)
if (-not [string]::Equals($CurrentPowerShell, $PlannedPowerShell, [StringComparison]::OrdinalIgnoreCase)) {
  throw "keeper hidden launcher host mismatch: planned=$PlannedPowerShell current=$CurrentPowerShell"
}
if (-not (Test-Path -LiteralPath $Plan.executable.path -PathType Leaf)) {
  throw "canonical harness CLI is missing: $($Plan.executable.path)"
}
$ObservedExecutableDigest = Get-FileSha256 -Path $Plan.executable.path
if ($ObservedExecutableDigest -ne [string]$Plan.executable.sha256) {
  throw 'canonical harness CLI digest mismatch'
}

$StartInfo = New-Object Diagnostics.ProcessStartInfo
$StartInfo.FileName = [string]$Plan.action.executable
$StartInfo.Arguments = [string]$Plan.action.arguments
$StartInfo.WorkingDirectory = [string]$Plan.action.workingDirectory
$StartInfo.UseShellExecute = $false
$StartInfo.CreateNoWindow = $true
$StartInfo.WindowStyle = [Diagnostics.ProcessWindowStyle]::Hidden
$Child = [Diagnostics.Process]::Start($StartInfo)
if ($null -eq $Child) {
  throw 'keeper hidden launcher did not receive a child process'
}
try {
  $Child.WaitForExit()
  $ChildExitCode = $Child.ExitCode
} finally {
  $Child.Dispose()
}
exit $ChildExitCode
"#,
    );
    body
}

fn start_script_body(plan_file: &Path, harness_home: &Path, live_deployment: bool) -> String {
    let mut body = script_control_header(plan_file, harness_home, live_deployment);
    body.push_str(atomic_json_function_body());
    body.push_str(ensure_keeper_hold_function_body());
    body.push_str(account_identity_function_body());
    body.push_str(task_scheduler_guard_function_body());
    body.push_str(legacy_task_preflight_function_body());
    body.push_str(&start_rollback_function_body(live_deployment));
    body.push_str(
        r#"if (-not (Test-Path -LiteralPath $Plan.executable.path -PathType Leaf)) {
  throw "canonical harness CLI is missing: $($Plan.executable.path)"
}
$ObservedExecutableDigest = Get-FileSha256 -Path $Plan.executable.path
if ($ObservedExecutableDigest -ne [string]$Plan.executable.sha256) {
  throw "canonical harness CLI digest mismatch"
}
"#,
    );
    if live_deployment {
        body.push_str("$KeeperFixtureMode = $false\n");
    } else {
        body.push_str(
            "$KeeperFixtureMode = ($Plan.deploymentMode -eq 'developer-staging') -and (-not [string]::IsNullOrWhiteSpace($env:AGENT_HARNESS_KEEPER_TEST_OPS_RECEIPT_FILE)) -and (-not [string]::IsNullOrWhiteSpace($env:AGENT_HARNESS_KEEPER_TEST_TASK_OBSERVATION_FILE))\n",
        );
    }
    body.push_str(
r#"Assert-KeeperLiveControlAuthorization -Action 'start'
Assert-LegacySupervisorTasksQuiescent -Phase 'start'
$StartProcessTreeSnapshot = if ($KeeperFixtureMode) { @() } else { @(Get-KeeperRollbackProcessTreeSnapshot) }
$StartProcessTreeSnapshot = @($StartProcessTreeSnapshot)
$StartStopMarkerSnapshot = @()
$StopDirectory = Join-Path ([string]$Plan.harnessHome) 'state\supervisor\stop'
foreach ($Marker in @(Get-ChildItem -LiteralPath $StopDirectory -Filter '*.stop' -File -ErrorAction SilentlyContinue)) {
  $StartStopMarkerSnapshot += [ordered]@{
    path = $Marker.FullName
    sha256 = Get-FileSha256 -Path $Marker.FullName
  }
}
try {
  # Re-query at the last safe point before ops-control clears any hold.
  Assert-LegacySupervisorTasksQuiescent -Phase 'start-pre-hold-clear'
"#,
    );
    body.push_str(&ops_control_invocation_body(
        "Start",
        "start",
        live_deployment,
    ));
    body.push_str(
        r#"if ($OpsStartExitCode -ne 0) {
  throw "ops-control start failed: $($OpsStartOutput -join [Environment]::NewLine)"
}
try {
  $OpsStart = ($OpsStartOutput -join [Environment]::NewLine) | ConvertFrom-Json -ErrorAction Stop
} catch {
  throw "ops-control start returned invalid JSON: $($_.Exception.Message)"
}
$StartReady = ([string]$OpsStart.schema -eq 'agent-harness.ops-control-receipt.v1') -and
  ([string]$OpsStart.action -eq 'start') -and
  ([string]$OpsStart.status -eq 'start-ready') -and
  (@($OpsStart.stopFiles | Where-Object { $_.present -eq $true }).Count -eq 0) -and
  (-not (Test-Path -LiteralPath $Plan.stopFile -PathType Leaf))
if (-not $StartReady) {
  throw "ops-control start did not produce exact start-ready state: $([string]$OpsStart.status)"
}
  [void](Invoke-ExactKeeperTaskMutation -Mutation 'Enable')
  $StartAttemptAtMs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
  [void](Invoke-ExactKeeperTaskMutation -Mutation 'Start')
  if ($KeeperFixtureMode) {
    if ([string]$env:AGENT_HARNESS_KEEPER_TEST_START_POST_MUTATION_FAILURE -ne '') {
      throw "fixture post-mutation failure: $($env:AGENT_HARNESS_KEEPER_TEST_START_POST_MUTATION_FAILURE)"
    }
  } else {
    $StartDeadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds)
    $KeeperReady = $false
    do {
      $StartedTaskSnapshot = Get-KeeperTaskSnapshot
      $StartedState = $null
      if (Test-Path -LiteralPath $Plan.stateFile -PathType Leaf) {
        try {
          $StartedState = Get-Content -LiteralPath $Plan.stateFile -Raw -ErrorAction Stop | ConvertFrom-Json -ErrorAction Stop
        } catch {
          $StartedState = $null
        }
      }
      $StartedProcessAlive = if ($null -ne $StartedState -and $null -ne $StartedState.pid) {
        $null -ne (Get-Process -Id ([int]$StartedState.pid) -ErrorAction SilentlyContinue)
      } else {
        $false
      }
      $KeeperReady = ($StartedTaskSnapshot.exists -eq $true) -and
        (Test-KeeperTaskExact -Snapshot $StartedTaskSnapshot) -and
        ([string]$StartedTaskSnapshot.task.state -eq 'Running') -and
        ($null -ne $StartedState) -and
        ([string]$StartedState.schema -eq 'agent-harness.supervisor-reconcile-keeper.v1') -and
        ([string]$StartedState.status -eq 'running') -and
        ([string]$StartedState.planDigest -eq [string]$Plan.planDigest) -and
        ([string]$StartedState.actionDigest -eq [string]$Plan.action.digest) -and
        ([string]$StartedState.taskName -eq [string]$Plan.task.name) -and
        ([string]$StartedState.leaseFile -eq [string]$Plan.leaseFile) -and
        ([string]$StartedState.ownershipMode -eq 'scheduled-task-plan') -and
        ([int64]$StartedState.lastHeartbeatAtMs -ge $StartAttemptAtMs) -and
        $StartedProcessAlive -and
        (-not (Test-Path -LiteralPath $Plan.stopFile -PathType Leaf))
      if (-not $KeeperReady) {
        Start-Sleep -Milliseconds 250
      }
    } while ((-not $KeeperReady) -and ([DateTimeOffset]::UtcNow -lt $StartDeadline))
    if (-not $KeeperReady) {
      throw 'scheduled reconcile keeper did not reach exact running state before timeout'
    }
    Invoke-KeeperScript -Path (($Plan.scripts | Where-Object { $_.name -eq 'readback' }).path)
  }
} catch {
  $StartFailure = $_.Exception.Message
  Invoke-KeeperTransactionalStartRollback -Failure $StartFailure
  throw
}
"#,
    );
    body
}

fn stop_script_body(plan_file: &Path, harness_home: &Path, live_deployment: bool) -> String {
    let mut body = script_control_header(plan_file, harness_home, live_deployment);
    body.push_str(atomic_json_function_body());
    body.push_str(ensure_keeper_hold_function_body());
    body.push_str(account_identity_function_body());
    body.push_str(task_scheduler_guard_function_body());
    if live_deployment {
        body.push_str("$KeeperFixtureMode = $false\n");
    } else {
        body.push_str(
            "$KeeperFixtureMode = ($Plan.deploymentMode -eq 'developer-staging') -and (-not [string]::IsNullOrWhiteSpace($env:AGENT_HARNESS_KEEPER_TEST_OPS_RECEIPT_FILE)) -and (-not [string]::IsNullOrWhiteSpace($env:AGENT_HARNESS_KEEPER_TEST_TASK_OBSERVATION_FILE))\n",
        );
    }
    body.push_str(
        r#"function Test-ExactKeeperProcessAlive {
  param([Parameter(Mandatory = $true)] $State)
  $ProcessId = if ($null -ne $State.processId) { [int]$State.processId } else { [int]$State.pid }
  if ($ProcessId -le 0 -or $null -eq $State.processStartTimeMs) { return $false }
  $Process = Get-CimInstance Win32_Process -Filter "ProcessId = $ProcessId" -ErrorAction SilentlyContinue
  if ($null -eq $Process) { return $false }
  try {
    $ObservedStartMs = ([DateTimeOffset]$Process.CreationDate).ToUnixTimeMilliseconds()
    return [int64]$ObservedStartMs -eq [int64]$State.processStartTimeMs
  } catch {
    return $true
  }
}

function Get-KeeperState {
  if (-not (Test-Path -LiteralPath $Plan.stateFile -PathType Leaf)) { return $null }
  try {
    return Get-Content -LiteralPath $Plan.stateFile -Raw -ErrorAction Stop | ConvertFrom-Json -ErrorAction Stop
  } catch {
    return [pscustomobject]@{ schema = 'invalid'; parseError = $_.Exception.Message }
  }
}

function Get-KeeperPinnedArgument {
  param([Parameter(Mandatory = $true)][string] $Flag)
  $Arguments = @($Plan.action.argumentVector)
  for ($Index = 0; $Index -lt ($Arguments.Count - 1); $Index++) {
    if ([string]$Arguments[$Index] -eq $Flag) { return [string]$Arguments[$Index + 1] }
  }
  return $null
}

function Get-ExactKeeperProcessTreeSnapshot {
  param($KeeperState)
  $AllProcesses = @(Get-CimInstance Win32_Process -ErrorAction SilentlyContinue)
  $RootIds = New-Object 'Collections.Generic.HashSet[int]'
  if ($null -ne $KeeperState -and [int]$KeeperState.pid -gt 0 -and $null -ne $KeeperState.processStartTimeMs) {
    $KeeperProcess = @($AllProcesses | Where-Object { [int]$_.ProcessId -eq [int]$KeeperState.pid })
    if ($KeeperProcess.Count -eq 1) {
      $KeeperStartMs = $null
      try { $KeeperStartMs = ([DateTimeOffset]$KeeperProcess[0].CreationDate).ToUnixTimeMilliseconds() } catch {}
      if ($null -ne $KeeperStartMs -and [int64]$KeeperStartMs -eq [int64]$KeeperState.processStartTimeMs) {
        [void]$RootIds.Add([int]$KeeperState.pid)
      }
    }
  }
  $ServicesDir = Join-Path ([string]$Plan.harnessHome) 'state\supervisor\services'
  foreach ($StateFile in @(Get-ChildItem -LiteralPath $ServicesDir -Filter '*.json' -File -ErrorAction SilentlyContinue)) {
    try {
      $ServiceState = Get-Content -LiteralPath $StateFile.FullName -Raw -ErrorAction Stop | ConvertFrom-Json -ErrorAction Stop
      $Candidate = if ($null -ne $ServiceState.processId) { [int]$ServiceState.processId } else { [int]$ServiceState.pid }
      if ($Candidate -gt 0 -and $null -ne $ServiceState.processStartTimeMs) {
        $CandidateProcess = @($AllProcesses | Where-Object { [int]$_.ProcessId -eq $Candidate })
        if ($CandidateProcess.Count -eq 1) {
          $CandidateStartMs = $null
          try { $CandidateStartMs = ([DateTimeOffset]$CandidateProcess[0].CreationDate).ToUnixTimeMilliseconds() } catch {}
          if ($null -ne $CandidateStartMs -and [int64]$CandidateStartMs -eq [int64]$ServiceState.processStartTimeMs) {
            [void]$RootIds.Add($Candidate)
          }
        }
      }
    } catch {
      # The final owner inventory turns unreadable state into a typed failure.
    }
  }
  $TreeIds = New-Object 'Collections.Generic.HashSet[int]'
  foreach ($RootId in $RootIds) { [void]$TreeIds.Add($RootId) }
  $Changed = $true
  while ($Changed) {
    $Changed = $false
    foreach ($Process in $AllProcesses) {
      if ($TreeIds.Contains([int]$Process.ParentProcessId) -and $TreeIds.Add([int]$Process.ProcessId)) {
        $Changed = $true
      }
    }
  }
  return @($AllProcesses | Where-Object { $TreeIds.Contains([int]$_.ProcessId) } | ForEach-Object {
    $StartMs = $null
    try { $StartMs = ([DateTimeOffset]$_.CreationDate).ToUnixTimeMilliseconds() } catch {}
    [pscustomobject]@{
      pid = [int]$_.ProcessId
      parentPid = [int]$_.ParentProcessId
      processStartTimeMs = $StartMs
      name = [string]$_.Name
      executablePath = [string]$_.ExecutablePath
      commandLine = [string]$_.CommandLine
    }
  })
}

function Get-ExactKeeperProcessTreeSurvivors {
  param([AllowNull()][AllowEmptyCollection()][object[]] $InitialTree = @())
  $InitialTree = @($InitialTree)
  $AllProcesses = @(Get-CimInstance Win32_Process -ErrorAction SilentlyContinue)
  $TrackedIds = New-Object 'Collections.Generic.HashSet[int]'
  foreach ($Process in $AllProcesses) {
    $ObservedStartMs = $null
    try { $ObservedStartMs = ([DateTimeOffset]$Process.CreationDate).ToUnixTimeMilliseconds() } catch {}
    $InitialExact = @($InitialTree | Where-Object {
      [int]$_.pid -eq [int]$Process.ProcessId -and
      (($null -eq $_.processStartTimeMs) -or ([int64]$_.processStartTimeMs -eq [int64]$ObservedStartMs))
    }).Count -gt 0
    if ($InitialExact) { [void]$TrackedIds.Add([int]$Process.ProcessId) }
  }
  $Changed = $true
  while ($Changed) {
    $Changed = $false
    foreach ($Process in $AllProcesses) {
      if ($TrackedIds.Contains([int]$Process.ParentProcessId) -and $TrackedIds.Add([int]$Process.ProcessId)) {
        $Changed = $true
      }
    }
  }
  $PinnedNode = Get-KeeperPinnedArgument -Flag '--node-exe'
  $PinnedGateway = Get-KeeperPinnedArgument -Flag '--gateway-script'
  $PinnedCodex = Get-KeeperPinnedArgument -Flag '--codex-exe'
  $Survivors = @()
  foreach ($Process in $AllProcesses) {
    $ObservedStartMs = $null
    try { $ObservedStartMs = ([DateTimeOffset]$Process.CreationDate).ToUnixTimeMilliseconds() } catch {}
    $TrackedDescendant = $TrackedIds.Contains([int]$Process.ProcessId)
    $ExecutableExact = [string]::Equals([string]$Process.ExecutablePath, [string]$Plan.executable.path, [StringComparison]::OrdinalIgnoreCase)
    $PinnedNodeGateway = (-not [string]::IsNullOrWhiteSpace($PinnedNode)) -and
      (-not [string]::IsNullOrWhiteSpace($PinnedGateway)) -and
      [string]::Equals([string]$Process.ExecutablePath, $PinnedNode, [StringComparison]::OrdinalIgnoreCase) -and
      (([string]$Process.CommandLine).IndexOf($PinnedGateway, [StringComparison]::OrdinalIgnoreCase) -ge 0)
    $PinnedCodexExact = (-not [string]::IsNullOrWhiteSpace($PinnedCodex)) -and
      [string]::Equals([string]$Process.ExecutablePath, $PinnedCodex, [StringComparison]::OrdinalIgnoreCase)
    if ($TrackedDescendant -or $ExecutableExact -or $PinnedNodeGateway -or $PinnedCodexExact) {
      $Survivors += [pscustomobject]@{
        pid = [int]$Process.ProcessId
        parentPid = [int]$Process.ParentProcessId
        processStartTimeMs = $ObservedStartMs
        name = [string]$Process.Name
        executablePath = [string]$Process.ExecutablePath
        commandLine = [string]$Process.CommandLine
        disposition = 'exact-planned-process-tree-still-alive-manual-recovery-required'
      }
    }
  }
  return $Survivors
}

Assert-KeeperLiveControlAuthorization -Action 'stop'
$InitialTask = Get-KeeperTaskSnapshot
$PreState = Get-KeeperState
$InitialProcessTree = if ($KeeperFixtureMode) { @() } else { @(Get-ExactKeeperProcessTreeSnapshot -KeeperState $PreState) }
$InitialProcessTree = @($InitialProcessTree)
$StopRequestedAtMs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
"#,
    );
    body.push_str(&ops_control_invocation_body(
        "Stop",
        "stop",
        live_deployment,
    ));
    body.push_str(
        r#"if ($OpsStopExitCode -ne 0) {
  Ensure-KeeperHold -Reason 'ops-control-stop-exit-failure'
  throw "ops-control stop failed: $($OpsStopOutput -join [Environment]::NewLine)"
}
try {
  $OpsStop = ($OpsStopOutput -join [Environment]::NewLine) | ConvertFrom-Json -ErrorAction Stop
} catch {
  Ensure-KeeperHold -Reason 'ops-control-stop-invalid-json'
  throw "ops-control stop returned invalid JSON: $($_.Exception.Message)"
}
$KeeperStopTarget = @($OpsStop.stopFiles | Where-Object { ([string]$_.component -eq 'supervisor-reconcile') -and ($_.present -eq $true) })
$StopRequested = ([string]$OpsStop.schema -eq 'agent-harness.ops-control-receipt.v1') -and
  ([string]$OpsStop.action -eq 'stop') -and
  ([string]$OpsStop.status -eq 'stop-requested') -and
  (@($OpsStop.stopFiles | Where-Object { $_.present -ne $true }).Count -eq 0) -and
  ($KeeperStopTarget.Count -eq 1) -and
  (Test-Path -LiteralPath $Plan.stopFile -PathType Leaf)
if (-not $StopRequested) {
  Ensure-KeeperHold -Reason ('ops-control-stop-not-complete:' + [string]$OpsStop.status)
  throw "ops-control stop did not produce exact stop-requested state: $([string]$OpsStop.status)"
}

$StopErrors = @()
try {
  [void](Invoke-ExactKeeperTaskMutation -Mutation 'Disable' -AllowMissing)
} catch {
  $StopErrors += "keeper task could not be safely disabled: $($_.Exception.Message)"
}

$PreStateExact = ($null -ne $PreState) -and
  ([string]$PreState.schema -eq 'agent-harness.supervisor-reconcile-keeper.v1') -and
  ([string]$PreState.planDigest -eq [string]$Plan.planDigest) -and
  ([string]$PreState.actionDigest -eq [string]$Plan.action.digest) -and
  ([string]$PreState.taskName -eq [string]$Plan.task.name) -and
  (-not [string]::IsNullOrWhiteSpace([string]$PreState.generationId)) -and
  ([int]$PreState.pid -gt 0) -and
  ($null -ne $PreState.processStartTimeMs)
$InitialTaskRunning = ($InitialTask.exists -eq $true) -and ([string]$InitialTask.task.state -eq 'Running')
if ($InitialTaskRunning -and -not $PreStateExact) {
  $StopErrors += 'running keeper task lacked an exact pre-request generation/PID snapshot'
}

$FallbackRequested = $false
$Proof = $null
$Deadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds)
do {
  $PostRequestTerminalObserved = $false
  $IdempotentExternalQuiescence = $false
  if ($KeeperFixtureMode -and -not [string]::IsNullOrWhiteSpace($env:AGENT_HARNESS_KEEPER_TEST_STOP_PROOF_FILE)) {
    $Proof = Get-Content -LiteralPath $env:AGENT_HARNESS_KEEPER_TEST_STOP_PROOF_FILE -Raw -ErrorAction Stop | ConvertFrom-Json -ErrorAction Stop
    $PostRequestTerminalObserved = $Proof.keeperTerminalMatches -eq $true
    $IdempotentExternalQuiescence = $Proof.idempotentExternalQuiescence -eq $true
    $TerminalMatches = $PostRequestTerminalObserved -or $IdempotentExternalQuiescence
    $KeeperPidAlive = $Proof.keeperPidAlive -eq $true
    $OwnerDispositions = @($Proof.ownerDispositions)
    $DeploymentProcesses = @($Proof.deploymentProcesses)
    $ProcessTreeSurvivors = @($Proof.processTreeSurvivors)
  } else {
    $PostState = Get-KeeperState
    $KeeperPidAlive = if ($PreStateExact) { Test-ExactKeeperProcessAlive -State $PreState } else { $false }
    $PostRequestTerminalObserved = if ($PreStateExact) {
      ($null -ne $PostState) -and
        ([string]$PostState.schema -eq 'agent-harness.supervisor-reconcile-keeper.v1') -and
        ([string]$PostState.generationId -eq [string]$PreState.generationId) -and
        ([int]$PostState.pid -eq [int]$PreState.pid) -and
        ([int64]$PostState.processStartTimeMs -eq [int64]$PreState.processStartTimeMs) -and
        (@('stopped', 'dead') -contains ([string]$PostState.status).ToLowerInvariant()) -and
        ([int64]$PostState.lastHeartbeatAtMs -ge $StopRequestedAtMs)
    } else {
      $false
    }
    $IdempotentExternalQuiescence = (-not $InitialTaskRunning) -and
      (-not $KeeperPidAlive) -and
      ($InitialProcessTree.Count -eq 0)
    $TerminalMatches = $PostRequestTerminalObserved -or $IdempotentExternalQuiescence
    $OwnerDispositions = @()
    $ServicesDir = Join-Path ([string]$Plan.harnessHome) 'state\supervisor\services'
    foreach ($StateFile in @(Get-ChildItem -LiteralPath $ServicesDir -Filter '*.json' -File -ErrorAction SilentlyContinue)) {
      try {
        $ServiceState = Get-Content -LiteralPath $StateFile.FullName -Raw -ErrorAction Stop | ConvertFrom-Json -ErrorAction Stop
        if ([string]$ServiceState.schema -ne 'agent-harness.supervisor-service-state.v1' -or
            [string]::IsNullOrWhiteSpace([string]$ServiceState.serviceId)) {
          $OwnerDispositions += [pscustomobject]@{ serviceId = $StateFile.BaseName; disposition = 'invalid-owner-state-manual-recovery-required' }
        } elseif (Test-ExactKeeperProcessAlive -State $ServiceState) {
          $OwnerDispositions += [pscustomobject]@{
            serviceId = [string]$ServiceState.serviceId
            pid = if ($null -ne $ServiceState.processId) { $ServiceState.processId } else { $ServiceState.pid }
            processStartTimeMs = $ServiceState.processStartTimeMs
            disposition = 'exact-owner-still-alive-manual-recovery-required'
          }
        }
      } catch {
        $OwnerDispositions += [pscustomobject]@{ serviceId = $StateFile.BaseName; disposition = 'unreadable-owner-state-manual-recovery-required' }
      }
    }
    $DeploymentProcesses = @(
      Get-CimInstance Win32_Process -Filter "name = 'agent-harness.exe'" -ErrorAction SilentlyContinue |
        Where-Object {
          ([string]$_.CommandLine).IndexOf([string]$Plan.harnessHome, [StringComparison]::OrdinalIgnoreCase) -ge 0
        } |
        ForEach-Object { [pscustomobject]@{ pid = $_.ProcessId; commandLine = $_.CommandLine } }
    )
    $ProcessTreeSurvivors = @(Get-ExactKeeperProcessTreeSurvivors -InitialTree $InitialProcessTree)
  }
  $StopProofComplete = $TerminalMatches -and (-not $KeeperPidAlive) -and
    ($OwnerDispositions.Count -eq 0) -and ($DeploymentProcesses.Count -eq 0) -and
    ($ProcessTreeSurvivors.Count -eq 0)
  if (-not $StopProofComplete -and -not $FallbackRequested -and [DateTimeOffset]::UtcNow -ge $Deadline) {
    $FallbackRequested = $true
    $FallbackReceipt = [ordered]@{
      schema = 'agent-harness.windows-reconcile-keeper-stop-fallback.v1'
      planDigest = $Plan.planDigest
      taskPath = $Plan.task.path
      taskName = $Plan.task.name
      action = 'Stop-ScheduledTask'
      status = 'requested'
      reason = 'post-request terminal or idempotent external-quiescence proof timeout'
      timeoutSeconds = $TimeoutSeconds
      observedAtMs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
    }
    Write-AtomicJson -Path $Plan.fallbackReceiptFile -Value $FallbackReceipt
    try {
      [void](Invoke-ExactKeeperTaskMutation -Mutation 'Stop' -AllowMissing)
      $FallbackReceipt.status = 'completed'
    } catch {
      $FallbackReceipt.status = 'failed'
      $FallbackReceipt.error = $_.Exception.Message
      $StopErrors += "keeper task forced-stop failed closed: $($_.Exception.Message)"
    }
    $FallbackReceipt.completedAtMs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
    Write-AtomicJson -Path $Plan.fallbackReceiptFile -Value $FallbackReceipt
    $Deadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds)
  } elseif (-not $StopProofComplete) {
    Start-Sleep -Milliseconds 250
  }
} while ((-not $StopProofComplete) -and
         ((-not $FallbackRequested) -or [DateTimeOffset]::UtcNow -lt $Deadline))

try {
  $FinalTask = Assert-KeeperTaskExact -Mutation 'post-stop proof' -AllowMissing
  if ($FinalTask.exists -eq $true -and
      ($FinalTask.task.enabled -eq $true -or [string]$FinalTask.task.state -eq 'Running')) {
    $StopErrors += 'exact keeper task remains enabled or running after stop'
  }
} catch {
  $StopErrors += "post-stop task identity proof failed: $($_.Exception.Message)"
  $FinalTask = $null
}
if (-not $StopProofComplete) {
  $StopErrors += 'matching post-request terminal state or idempotent external quiescence, dead keeper PID, and exact child/process absence were not proven'
}
$StopProofReceipt = [ordered]@{
  schema = 'agent-harness.windows-reconcile-keeper-stop-proof.v1'
  deploymentIdentity = $Plan.deploymentIdentity
  planDigest = $Plan.planDigest
  actionDigest = $Plan.action.digest
  taskPath = $Plan.task.path
  taskName = $Plan.task.name
  stopRequestedAtMs = $StopRequestedAtMs
  preState = $PreState
  terminalMatchesPreRequestGeneration = $TerminalMatches
  postRequestTerminalObserved = $PostRequestTerminalObserved
  idempotentExternalQuiescence = $IdempotentExternalQuiescence
  keeperPidAlive = $KeeperPidAlive
  ownerDispositions = $OwnerDispositions
  deploymentProcesses = $DeploymentProcesses
  initialProcessTree = $InitialProcessTree
  processTreeSurvivors = $ProcessTreeSurvivors
  finalTask = $FinalTask
  fallbackRequested = $FallbackRequested
  errors = $StopErrors
  status = if ($StopErrors.Count -eq 0) { 'stopped' } else { 'manual-recovery-required' }
  observedAtMs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
}
$StopProofFile = Join-Path ([string]$Plan.outputDir) 'stop-proof-latest.json'
Write-AtomicJson -Path $StopProofFile -Value $StopProofReceipt
if ($StopErrors.Count -gt 0) {
  throw "keeper stop is partial; manual recovery required: $($StopErrors -join '; ')"
}
if (-not $KeeperFixtureMode) {
  Invoke-KeeperScript -Path (($Plan.scripts | Where-Object { $_.name -eq 'readback' }).path)
}
"#,
    );
    body
}

fn uninstall_script_body(plan_file: &Path, harness_home: &Path, live_deployment: bool) -> String {
    let mut body = script_control_header(plan_file, harness_home, live_deployment);
    body.push_str(atomic_json_function_body());
    body.push_str(ensure_keeper_hold_function_body());
    body.push_str(account_identity_function_body());
    body.push_str(task_scheduler_guard_function_body());
    body.push_str(
        r#"$KeeperFixtureMode = ($Plan.deploymentMode -eq 'developer-staging') -and (-not [string]::IsNullOrWhiteSpace($env:AGENT_HARNESS_KEEPER_TEST_TASK_OBSERVATION_FILE))
Assert-KeeperLiveControlAuthorization -Action 'uninstall'
$StopScript = ($Plan.scripts | Where-Object { $_.name -eq 'stop' }).path
"#,
    );
    if live_deployment {
        body.push_str(
            "Invoke-KeeperScript -Path $StopScript -Arguments @('-TimeoutSeconds', [string]$TimeoutSeconds)\n",
        );
    } else {
        body.push_str(
            "Invoke-KeeperScript -Path $StopScript -Arguments @('-TimeoutSeconds', [string]$TimeoutSeconds)\n",
        );
    }
    body.push_str(
        r#"
$StopProofFile = Join-Path ([string]$Plan.outputDir) 'stop-proof-latest.json'
if (-not (Test-Path -LiteralPath $StopProofFile -PathType Leaf)) {
  throw 'keeper uninstall refuses to unregister without a durable stop proof'
}
$StopProof = Get-Content -LiteralPath $StopProofFile -Raw -ErrorAction Stop | ConvertFrom-Json -ErrorAction Stop
$StopProofExact = ([string]$StopProof.schema -eq 'agent-harness.windows-reconcile-keeper-stop-proof.v1') -and
  ([string]$StopProof.status -eq 'stopped') -and
  ([string]$StopProof.deploymentIdentity -eq [string]$Plan.deploymentIdentity) -and
  ([string]$StopProof.planDigest -eq [string]$Plan.planDigest) -and
  ([string]$StopProof.actionDigest -eq [string]$Plan.action.digest) -and
  ([string]$StopProof.taskPath -eq [string]$Plan.task.path) -and
  ([string]$StopProof.taskName -eq [string]$Plan.task.name) -and
  ([int64]$StopProof.observedAtMs -ge [int64]$StopProof.stopRequestedAtMs) -and
  (@($StopProof.ownerDispositions).Count -eq 0) -and
  (@($StopProof.deploymentProcesses).Count -eq 0) -and
  (@($StopProof.processTreeSurvivors).Count -eq 0) -and
  ($StopProof.keeperPidAlive -ne $true)
if (-not $StopProofExact) {
  throw 'keeper uninstall refuses to unregister because stop proof is partial, stale, or foreign'
}
if ($KeeperFixtureMode -and -not [string]::IsNullOrWhiteSpace($env:AGENT_HARNESS_KEEPER_TEST_UNINSTALL_TASK_OBSERVATION_FILE)) {
  $env:AGENT_HARNESS_KEEPER_TEST_TASK_OBSERVATION_FILE = $env:AGENT_HARNESS_KEEPER_TEST_UNINSTALL_TASK_OBSERVATION_FILE
  $script:KeeperFixtureObservationDocument = $null
  $script:KeeperFixtureObservationIndex = 0
}
$Unregistered = Invoke-ExactKeeperTaskMutation -Mutation 'Unregister' -AllowMissing
$AfterUnregister = Get-KeeperTaskSnapshot
if ($AfterUnregister.exists -eq $true) {
  throw 'exact keeper task still exists after uninstall unregister'
}
$UninstallReceipt = [ordered]@{
  schema = 'agent-harness.windows-reconcile-keeper-uninstall.v1'
  deploymentIdentity = $Plan.deploymentIdentity
  planDigest = $Plan.planDigest
  actionDigest = $Plan.action.digest
  taskPath = $Plan.task.path
  taskName = $Plan.task.name
  stopProofFile = $StopProofFile
  taskWasPresent = [bool]$Unregistered
  status = 'uninstalled'
  observedAtMs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
}
Write-AtomicJson -Path (Join-Path ([string]$Plan.outputDir) 'uninstall-latest.json') -Value $UninstallReceipt
if (-not $KeeperFixtureMode) {
  Invoke-KeeperScript -Path (($Plan.scripts | Where-Object { $_.name -eq 'readback' }).path)
}
"#,
    );
    body
}

fn rollback_script_body(plan_file: &Path, harness_home: &Path) -> String {
    let mut body = String::from(
        "param([Parameter(Mandatory = $true)][string] $PriorGenerationPackage, [ValidateRange(1, 300)][int] $TimeoutSeconds = 30)\n",
    );
    body.push_str(&script_plan_header(plan_file, harness_home));
    body.push_str(live_control_token_body());
    body.push_str(atomic_json_function_body());
    body.push_str(ensure_keeper_hold_function_body());
    body.push_str(account_identity_function_body());
    body.push_str(task_scheduler_guard_function_body());
    body.push_str(legacy_task_preflight_function_body());
    body.push_str(
        r#"$KeeperFixtureMode = ($Plan.deploymentMode -eq 'developer-staging') -and (-not [string]::IsNullOrWhiteSpace($env:AGENT_HARNESS_KEEPER_TEST_TASK_OBSERVATION_FILE))
Assert-KeeperLiveControlAuthorization -Action 'restart'
if (-not (Test-Path -LiteralPath $PriorGenerationPackage -PathType Leaf)) {
  throw 'an explicit prior-generation rollback package JSON file is required'
}
$PackageFile = [IO.Path]::GetFullPath($PriorGenerationPackage)
$PackageRoot = [IO.Path]::GetFullPath((Split-Path -Parent $PackageFile)).TrimEnd('\', '/')
$Package = Get-Content -LiteralPath $PackageFile -Raw -ErrorAction Stop | ConvertFrom-Json -ErrorAction Stop
$MigrationManifestFile = Join-Path ([string]$Plan.outputDir) 'legacy-task-migration.json'
if (-not (Test-Path -LiteralPath $MigrationManifestFile -PathType Leaf)) {
  throw 'keeper rollback requires the durable pre-cutover legacy migration manifest'
}
$MigrationManifestSha256 = Get-FileSha256 -Path $MigrationManifestFile
$MigrationManifest = Get-Content -LiteralPath $MigrationManifestFile -Raw -ErrorAction Stop | ConvertFrom-Json -ErrorAction Stop
$PriorExecutable = [IO.Path]::GetFullPath([string]$Package.priorExecutable.path)
$CandidateBackup = [IO.Path]::GetFullPath([string]$Package.candidateBackupPath)
$RollbackRoot = [IO.Path]::GetFullPath((Join-Path ([string]$Plan.harnessHome) 'rollback\hot')).TrimEnd('\', '/')
$PackageExact = ([string]$Package.schema -eq 'agent-harness.windows-reconcile-keeper-prior-generation-package.v1') -and
  ([string]$Package.deploymentIdentity -eq [string]$Plan.deploymentIdentity) -and
  ([string]$Package.candidatePlanDigest -eq [string]$Plan.planDigest) -and
  (-not [string]::IsNullOrWhiteSpace([string]$Package.priorGenerationId)) -and
  ([string]$Package.legacyMigrationManifestSha256 -eq $MigrationManifestSha256) -and
  $PriorExecutable.StartsWith($PackageRoot + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase) -and
  $CandidateBackup.StartsWith($RollbackRoot + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase) -and
  (-not [string]::Equals($PriorExecutable, [string]$Plan.executable.path, [StringComparison]::OrdinalIgnoreCase))
if (-not $PackageExact) {
  throw 'prior-generation rollback package identity or path boundary is invalid'
}
if (-not (Test-Path -LiteralPath $PriorExecutable -PathType Leaf) -or
    (Get-FileSha256 -Path $PriorExecutable) -ne [string]$Package.priorExecutable.sha256) {
  throw 'prior-generation rollback package executable is missing or hash-mismatched'
}
if ((Get-FileSha256 -Path $Plan.executable.path) -ne [string]$Plan.executable.sha256) {
  throw 'candidate canonical executable is not the exact plan-bound generation'
}
$ManifestExact = ([string]$MigrationManifest.schema -eq 'agent-harness.windows-reconcile-keeper-legacy-migration.v1') -and
  ([string]$MigrationManifest.deploymentIdentity -eq [string]$Plan.deploymentIdentity) -and
  ([string]$MigrationManifest.planDigest -eq [string]$Plan.planDigest) -and
  ([string]$MigrationManifest.legacyPlanSha256 -eq [string]$Package.legacyPlanSha256)
if (-not $ManifestExact) {
  throw 'legacy migration manifest is corrupt, stale, or for a different candidate plan'
}
Assert-LegacySupervisorTasksQuiescent -Phase 'rollback-pre-stop'

$OfflineRecoveryFile = Join-Path ([string]$Plan.outputDir) 'rollback-offline-recovery.json'
$RollbackReceiptFile = Join-Path ([string]$Plan.outputDir) 'rollback-latest.json'
$OfflineRecovery = [ordered]@{
  schema = 'agent-harness.windows-reconcile-keeper-offline-recovery.v1'
  deploymentIdentity = $Plan.deploymentIdentity
  candidatePlanDigest = $Plan.planDigest
  candidateExecutable = $Plan.executable.path
  candidateExecutableSha256 = $Plan.executable.sha256
  candidateBackupPath = $CandidateBackup
  priorGenerationPackage = $PackageFile
  priorGenerationId = $Package.priorGenerationId
  priorExecutable = $PriorExecutable
  priorExecutableSha256 = $Package.priorExecutable.sha256
  legacyMigrationManifest = $MigrationManifestFile
  requiredOrder = @(
    'hold all exact supervisors',
    'prove keeper and exact child/process trees stopped',
    'reprove and unregister exact candidate keeper task',
    'restore exact legacy task definitions disabled',
    'replace canonical binary from the hash-bound external package',
    'restore recorded legacy enablement/running state'
  )
  status = 'prepared-before-mutation'
  observedAtMs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
}
Write-AtomicJson -Path $OfflineRecoveryFile -Value $OfflineRecovery

$StopScript = ($Plan.scripts | Where-Object { $_.name -eq 'stop' }).path
Invoke-KeeperScript -Path $StopScript -Arguments @('-TimeoutSeconds', [string]$TimeoutSeconds)
$StopProofFile = Join-Path ([string]$Plan.outputDir) 'stop-proof-latest.json'
$StopProof = Get-Content -LiteralPath $StopProofFile -Raw -ErrorAction Stop | ConvertFrom-Json -ErrorAction Stop
$StopProofExact = ([string]$StopProof.schema -eq 'agent-harness.windows-reconcile-keeper-stop-proof.v1') -and
  ([string]$StopProof.status -eq 'stopped') -and
  ([string]$StopProof.planDigest -eq [string]$Plan.planDigest) -and
  ([string]$StopProof.taskPath -eq [string]$Plan.task.path) -and
  (@($StopProof.ownerDispositions).Count -eq 0) -and
  (@($StopProof.processTreeSurvivors).Count -eq 0) -and
  (@($StopProof.deploymentProcesses).Count -eq 0)
if (-not $StopProofExact) {
  throw 'candidate rollback refuses mutation without exact stop/process-tree proof'
}

$RollbackErrors = @()
$RestoredTasks = @()
try {
  [void](Invoke-ExactKeeperTaskMutation -Mutation 'Disable' -AllowMissing)
  [void](Invoke-ExactKeeperTaskMutation -Mutation 'Unregister' -AllowMissing)
  $AfterCandidateUnregister = Get-KeeperTaskSnapshot
  if ($AfterCandidateUnregister.exists -eq $true) {
    throw 'candidate keeper task still exists after exact unregister'
  }

  foreach ($Legacy in @($MigrationManifest.tasks)) {
    $Matches = @(Get-ScheduledTask -TaskName ([string]$Legacy.name) -ErrorAction SilentlyContinue)
    if ($Legacy.originallyMissing -eq $true) {
      if ($Matches.Count -ne 0) {
        throw "legacy task $($Legacy.name) was originally missing but now occupies scheduler state"
      }
      $RestoredTasks += [pscustomobject]@{ taskPath = '\'; name = $Legacy.name; disposition = 'retained-originally-missing' }
      continue
    }
    if ([string]::IsNullOrWhiteSpace([string]$Legacy.exportedXml)) {
      throw "legacy task $($Legacy.name) lacks an exported rollback definition"
    }
    if ($Matches.Count -eq 0) {
      $DisabledXml = Set-KeeperTaskXmlDisabled -Xml ([string]$Legacy.exportedXml)
      Register-ScheduledTask -TaskPath '\' -TaskName ([string]$Legacy.name) -Xml $DisabledXml -ErrorAction Stop | Out-Null
      $Matches = @(Get-ScheduledTask -TaskName ([string]$Legacy.name) -ErrorAction Stop)
    }
    if ($Matches.Count -ne 1 -or [string]$Matches[0].TaskPath -ne '\') {
      throw "legacy task $($Legacy.name) is duplicate, foreign-path, or ambiguous during rollback"
    }
    $ObservedXml = [xml](Export-ScheduledTask -TaskPath '\' -TaskName ([string]$Legacy.name) -ErrorAction Stop)
    $ExpectedXml = [xml]([string]$Legacy.exportedXml)
    $ExpectedDisabledXml = [xml](Set-KeeperTaskXmlDisabled -Xml $ExpectedXml.OuterXml)
    if ($ObservedXml.OuterXml -ne $ExpectedDisabledXml.OuterXml) {
      throw "legacy task $($Legacy.name) definition changed; left untouched"
    }
    $RestoredTasks += [pscustomobject]@{
      taskPath = '\'
      name = $Legacy.name
      originalEnabled = $Legacy.originalEnabled
      originalState = $Legacy.originalState
      disposition = 'definition-restored-disabled'
    }
  }

  New-Item -ItemType Directory -Force -Path (Split-Path -Parent $CandidateBackup) | Out-Null
  if (Test-Path -LiteralPath $CandidateBackup -PathType Leaf) {
    if ((Get-FileSha256 -Path $CandidateBackup) -ne [string]$Plan.executable.sha256) {
      throw 'existing candidate rollback backup has a foreign digest'
    }
  } else {
    Copy-Item -LiteralPath $Plan.executable.path -Destination $CandidateBackup -ErrorAction Stop
  }
  $CanonicalParent = Split-Path -Parent ([string]$Plan.executable.path)
  $PriorTemp = Join-Path $CanonicalParent ('.keeper-prior.' + [Guid]::NewGuid().ToString('N') + '.tmp')
  try {
    Copy-Item -LiteralPath $PriorExecutable -Destination $PriorTemp -ErrorAction Stop
    if ((Get-FileSha256 -Path $PriorTemp) -ne [string]$Package.priorExecutable.sha256) {
      throw 'staged prior executable digest changed before atomic replacement'
    }
    [IO.File]::Replace($PriorTemp, [string]$Plan.executable.path, $null, $true)
  } finally {
    Remove-Item -LiteralPath $PriorTemp -Force -ErrorAction SilentlyContinue
  }
  if ((Get-FileSha256 -Path $Plan.executable.path) -ne [string]$Package.priorExecutable.sha256) {
    throw 'canonical executable does not match the prior generation after replacement'
  }

  foreach ($Legacy in @($MigrationManifest.tasks | Where-Object { $_.originallyMissing -ne $true })) {
    $Matches = @(Get-ScheduledTask -TaskName ([string]$Legacy.name) -ErrorAction Stop)
    if ($Matches.Count -ne 1 -or [string]$Matches[0].TaskPath -ne '\') {
      throw "legacy task $($Legacy.name) changed before enablement restoration"
    }
    $ObservedBeforeEnableXml = [xml](Export-ScheduledTask -TaskPath '\' -TaskName ([string]$Legacy.name) -ErrorAction Stop)
    $ExpectedBeforeEnableXml = [xml](Set-KeeperTaskXmlDisabled -Xml ([string]$Legacy.exportedXml))
    if ($ObservedBeforeEnableXml.OuterXml -ne $ExpectedBeforeEnableXml.OuterXml) {
      throw "legacy task $($Legacy.name) ownership changed immediately before Enable"
    }
    if ($Legacy.originalEnabled -eq $true) {
      Enable-ScheduledTask -TaskPath '\' -TaskName ([string]$Legacy.name) -ErrorAction Stop | Out-Null
    }
    if ([string]$Legacy.originalState -eq 'Running') {
      $BeforeStart = @(Get-ScheduledTask -TaskPath '\' -TaskName ([string]$Legacy.name) -ErrorAction Stop)
      if ($BeforeStart.Count -ne 1) {
        throw "legacy task $($Legacy.name) changed immediately before Start"
      }
      $ObservedBeforeStartXml = [xml](Export-ScheduledTask -TaskPath '\' -TaskName ([string]$Legacy.name) -ErrorAction Stop)
      $ExpectedBeforeStartXml = [xml]([string]$Legacy.exportedXml)
      if ($ObservedBeforeStartXml.OuterXml -ne $ExpectedBeforeStartXml.OuterXml) {
        throw "legacy task $($Legacy.name) ownership changed immediately before Start"
      }
      Start-ScheduledTask -TaskPath '\' -TaskName ([string]$Legacy.name) -ErrorAction Stop
    }
  }
} catch {
  $RollbackErrors += $_.Exception.Message
}

$RollbackReceipt = [ordered]@{
  schema = 'agent-harness.windows-reconcile-keeper-rollback.v1'
  deploymentIdentity = $Plan.deploymentIdentity
  candidatePlanDigest = $Plan.planDigest
  priorGenerationId = $Package.priorGenerationId
  packageFile = $PackageFile
  offlineRecoveryFile = $OfflineRecoveryFile
  stopProofFile = $StopProofFile
  restoredTasks = $RestoredTasks
  candidateBackupPath = $CandidateBackup
  errors = $RollbackErrors
  status = if ($RollbackErrors.Count -eq 0) { 'prior-generation-restored' } else { 'manual-recovery-required' }
  observedAtMs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
}
Write-AtomicJson -Path $RollbackReceiptFile -Value $RollbackReceipt
$OfflineRecovery.status = $RollbackReceipt.status
$OfflineRecovery.rollbackReceipt = $RollbackReceiptFile
$OfflineRecovery.completedAtMs = $RollbackReceipt.observedAtMs
Write-AtomicJson -Path $OfflineRecoveryFile -Value $OfflineRecovery
if ($RollbackErrors.Count -gt 0) {
  throw "prior-generation rollback is partial; use the offline recovery artifact: $($RollbackErrors -join '; ')"
}
"#,
    );
    body
}

fn readback_script_body(plan_file: &Path, harness_home: &Path) -> String {
    let mut body = String::from("param([string] $QueryNonce)\n");
    body.push_str(&script_plan_header(plan_file, harness_home));
    body.push_str(atomic_json_function_body());
    body.push_str(account_identity_function_body());
    body.push_str(
        r#"$TaskMatches = @(Get-ScheduledTask -TaskPath $Plan.task.path -TaskName $Plan.task.name -ErrorAction SilentlyContinue)
if ($TaskMatches.Count -gt 1) { throw "keeper readback returned multiple tasks for exact path/name" }
$Task = if ($TaskMatches.Count -eq 1) { $TaskMatches[0] } else { $null }
$TaskInfo = if ($null -ne $Task) { Get-ScheduledTaskInfo -TaskPath $Plan.task.path -TaskName $Plan.task.name -ErrorAction SilentlyContinue } else { $null }
$Actions = @()
$Triggers = @()
if ($null -ne $Task) {
  $Actions = @($Task.Actions)
  $Triggers = @($Task.Triggers)
}
$LogonTriggers = @($Triggers | Where-Object { [string]$_.CimClass.CimClassName -eq 'MSFT_TaskLogonTrigger' })
$RecoveryTriggers = @($Triggers | Where-Object { [string]$_.CimClass.CimClassName -eq 'MSFT_TaskTimeTrigger' })
$Action = if ($Actions.Count -eq 1) { $Actions[0] } else { $null }
$Trigger = if ($LogonTriggers.Count -eq 1) { $LogonTriggers[0] } else { $null }
$RecoveryTrigger = if ($RecoveryTriggers.Count -eq 1) { $RecoveryTriggers[0] } else { $null }
$KeeperState = $null
$KeeperStateError = $null
if (Test-Path -LiteralPath $Plan.stateFile -PathType Leaf) {
  try {
    $KeeperState = Get-Content -LiteralPath $Plan.stateFile -Raw -ErrorAction Stop | ConvertFrom-Json -ErrorAction Stop
  } catch {
    $KeeperStateError = $_.Exception.Message
  }
}
$ExecutableExists = Test-Path -LiteralPath $Plan.executable.path -PathType Leaf
$ExecutableDigest = if ($ExecutableExists) { Get-FileSha256 -Path $Plan.executable.path } else { $null }
$ObservedPrincipalUserId = if ($null -ne $Task) { [string]$Task.Principal.UserId } else { $null }
$PrincipalIdentityMatches = ($null -ne $Task) -and (Test-KeeperAccountIdentityExact -ExpectedUserId ([string]$Plan.task.principal.userId) -ObservedUserId $ObservedPrincipalUserId)
$ObservedTriggerUserId = if ($null -ne $Trigger) { [string]$Trigger.UserId } else { $null }
$TriggerIdentityMatches = ($null -ne $Trigger) -and (Test-KeeperAccountIdentityExact -ExpectedUserId ([string]$Plan.task.trigger.userId) -ObservedUserId $ObservedTriggerUserId)
$RecoveryDuration = if ($null -ne $RecoveryTrigger -and -not [string]::IsNullOrWhiteSpace([string]$RecoveryTrigger.Repetition.Duration)) { [string]$RecoveryTrigger.Repetition.Duration } else { $null }
$Observation = [ordered]@{
  schema = 'agent-harness.windows-reconcile-keeper-observation.v1'
  planSchema = $Plan.schema
  planDigest = $Plan.planDigest
  actionDigest = $Plan.action.digest
  queryNonce = $QueryNonce
  observedAtMs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
  task = [ordered]@{
    path = $Plan.task.path
    observedPath = if ($null -ne $Task) { [string]$Task.TaskPath } else { $null }
    name = $Plan.task.name
    description = if ($null -ne $Task) { [string]$Task.Description } else { $null }
    exists = ($null -ne $Task)
    state = if ($null -ne $Task) { [string]$Task.State } else { 'Missing' }
    enabled = if ($null -ne $Task) { [bool]$Task.Settings.Enabled } else { $false }
  }
  principal = [ordered]@{
    plannedUserId = $Plan.task.principal.userId
    observedUserId = $ObservedPrincipalUserId
    userId = if ($PrincipalIdentityMatches) { [string]$Plan.task.principal.userId } else { $ObservedPrincipalUserId }
    identityMatches = $PrincipalIdentityMatches
    logonType = if ($null -ne $Task) { [string]$Task.Principal.LogonType } else { $null }
    runLevel = if ($null -ne $Task) { [string]$Task.Principal.RunLevel } else { $null }
  }
  action = [ordered]@{
    count = $Actions.Count
    execute = if ($null -ne $Action) { [string]$Action.Execute } else { $null }
    arguments = if ($null -ne $Action) { [string]$Action.Arguments } else { $null }
    workingDirectory = if ($null -ne $Action) { [string]$Action.WorkingDirectory } else { $null }
  }
  trigger = [ordered]@{
    count = $Triggers.Count
    logonCount = $LogonTriggers.Count
    recoveryCount = $RecoveryTriggers.Count
    plannedUserId = $Plan.task.trigger.userId
    observedUserId = $ObservedTriggerUserId
    userId = if ($TriggerIdentityMatches) { [string]$Plan.task.trigger.userId } else { $ObservedTriggerUserId }
    identityMatches = $TriggerIdentityMatches
    triggerType = if ($null -ne $Trigger) { [string]$Trigger.CimClass.CimClassName } else { $null }
    recoveryTriggerType = if ($null -ne $RecoveryTrigger) { [string]$RecoveryTrigger.CimClass.CimClassName } else { $null }
    recoveryStartBoundary = if ($null -ne $RecoveryTrigger) { [string]$RecoveryTrigger.StartBoundary } else { $null }
    recoveryInterval = if ($null -ne $RecoveryTrigger) { [string]$RecoveryTrigger.Repetition.Interval } else { $null }
    recoveryDuration = $RecoveryDuration
    recoveryStopAtDurationEnd = if ($null -ne $RecoveryTrigger) { [bool]$RecoveryTrigger.Repetition.StopAtDurationEnd } else { $null }
  }
  settings = [ordered]@{
    multipleInstances = if ($null -ne $Task) { [string]$Task.Settings.MultipleInstances } else { $null }
    restartCount = if ($null -ne $Task) { $Task.Settings.RestartCount } else { $null }
    restartInterval = if ($null -ne $Task) { [string]$Task.Settings.RestartInterval } else { $null }
    startWhenAvailable = if ($null -ne $Task) { [bool]$Task.Settings.StartWhenAvailable } else { $null }
    disallowStartIfOnBatteries = if ($null -ne $Task) { [bool]$Task.Settings.DisallowStartIfOnBatteries } else { $null }
    stopIfGoingOnBatteries = if ($null -ne $Task) { [bool]$Task.Settings.StopIfGoingOnBatteries } else { $null }
    executionTimeLimit = if ($null -ne $Task) { [string]$Task.Settings.ExecutionTimeLimit } else { $null }
  }
  runtime = [ordered]@{
    lastRunTime = if ($null -ne $TaskInfo -and $null -ne $TaskInfo.LastRunTime) { $TaskInfo.LastRunTime.ToUniversalTime().ToString('o') } else { $null }
    nextRunTime = if ($null -ne $TaskInfo -and $null -ne $TaskInfo.NextRunTime) { $TaskInfo.NextRunTime.ToUniversalTime().ToString('o') } else { $null }
    lastTaskResult = if ($null -ne $TaskInfo) { $TaskInfo.LastTaskResult } else { $null }
    numberOfMissedRuns = if ($null -ne $TaskInfo) { $TaskInfo.NumberOfMissedRuns } else { $null }
    keeperStateFile = $Plan.stateFile
    keeperState = $KeeperState
    keeperStateError = $KeeperStateError
    stopFile = $Plan.stopFile
    stopFileExists = (Test-Path -LiteralPath $Plan.stopFile -PathType Leaf)
  }
  executable = [ordered]@{
    path = $Plan.executable.path
    plannedSha256 = $Plan.executable.sha256
    exists = $ExecutableExists
    observedSha256 = $ExecutableDigest
    digestMatches = ($ExecutableDigest -eq [string]$Plan.executable.sha256)
  }
}
Write-AtomicJson -Path $Plan.observationFile -Value $Observation
$Observation | ConvertTo-Json -Depth 16
"#,
    );
    body
}

fn script_plan_header(plan_file: &Path, harness_home: &Path) -> String {
    let mut body = format!(
        "$ErrorActionPreference = 'Stop'\n$ExpectedHarnessHome = {}\n$PlanFile = {}\n$CurrentScriptPath = [IO.Path]::GetFullPath([string]$MyInvocation.MyCommand.Path)\n",
        ps_quote_path(harness_home),
        ps_quote_path(plan_file),
    );
    body.push_str(keeper_physical_path_function_body());
    body.push_str(
        r#"[void](Assert-KeeperPhysicalContainment -Root $ExpectedHarnessHome -Path $PlanFile -RequireExisting $true)
[void](Assert-KeeperPhysicalContainment -Root $ExpectedHarnessHome -Path $CurrentScriptPath -RequireExisting $true)
$Plan = Get-Content -LiteralPath $PlanFile -Raw -ErrorAction Stop | ConvertFrom-Json -ErrorAction Stop
"#,
    );
    body.push_str(file_sha256_function_body());
    body.push_str(keeper_plan_integrity_function_body());
    body.push_str(
        r#"Assert-KeeperPlanIntegrity
Assert-KeeperPlanPhysicalContainment

$ScriptManifestMatches = @($Plan.scripts | Where-Object {
  [string]::Equals([IO.Path]::GetFullPath([string]$_.path), $CurrentScriptPath, [StringComparison]::OrdinalIgnoreCase)
})
if ($ScriptManifestMatches.Count -ne 1) {
  throw 'current keeper script is not uniquely bound to the current plan manifest'
}
$ScriptManifestEntry = $ScriptManifestMatches[0]
$ObservedScriptSha256 = Get-FileSha256 -Path $CurrentScriptPath
if ([string]::IsNullOrWhiteSpace([string]$ScriptManifestEntry.sha256) -or
    $ObservedScriptSha256 -ne ([string]$ScriptManifestEntry.sha256).ToLowerInvariant()) {
  throw 'current keeper script content digest does not match the current plan manifest'
}
"#,
    );
    body.push_str(invoke_keeper_script_function_body());
    body
}

fn script_control_header(plan_file: &Path, harness_home: &Path, live_deployment: bool) -> String {
    let mut body = if live_deployment {
        String::from("param([ValidateRange(1, 300)][int] $TimeoutSeconds = 30)\n")
    } else {
        String::from(
            "param([string] $LiveControlToken, [ValidateRange(1, 300)][int] $TimeoutSeconds = 30)\n",
        )
    };
    body.push_str(&script_plan_header(plan_file, harness_home));
    if live_deployment {
        body.push_str(live_control_token_body());
    } else {
        body.push_str(developer_staging_control_body());
    }
    body
}

fn developer_staging_control_body() -> &'static str {
    r#"$ResolvedLiveControlToken = $null
function Assert-KeeperLiveControlAuthorization {
  param([Parameter(Mandatory = $true)][ValidateSet('start', 'stop', 'restart', 'uninstall', 'cutover')][string] $Action)
  if ([string]$Plan.deploymentMode -ne 'developer-staging') {
    throw 'developer-staging keeper control script refuses a non-staging plan'
  }
  return [pscustomobject]@{ status = 'developer-staging'; action = $Action }
}
"#
}

fn live_control_token_body() -> &'static str {
    r#"$ResolvedLiveControlToken = if (-not [string]::IsNullOrWhiteSpace($env:AGENT_HARNESS_LIVE_CONTROL_TOKEN)) { $env:AGENT_HARNESS_LIVE_CONTROL_TOKEN } else { $null }
if ([string]::IsNullOrWhiteSpace($ResolvedLiveControlToken)) {
  throw 'AGENT_HARNESS_LIVE_CONTROL_TOKEN is required for reconcile keeper control'
}

function Assert-KeeperLiveControlAuthorization {
  param([Parameter(Mandatory = $true)][ValidateSet('start', 'stop', 'restart', 'uninstall', 'cutover')][string] $Action)
  if ($KeeperFixtureMode -and -not [string]::IsNullOrWhiteSpace($env:AGENT_HARNESS_KEEPER_TEST_AUTH_RECEIPT_FILE)) {
    $AuthorizationDocument = Get-Content -LiteralPath $env:AGENT_HARNESS_KEEPER_TEST_AUTH_RECEIPT_FILE -Raw -ErrorAction Stop | ConvertFrom-Json -ErrorAction Stop
    $AuthorizationProperty = $AuthorizationDocument.PSObject.Properties[$Action]
    $Authorization = if ($null -ne $AuthorizationProperty) { $AuthorizationProperty.Value } else { $AuthorizationDocument }
    $AuthorizationExitCode = 0
  } else {
    $AuthorizationOutput = & $Plan.executable.path 'ops-cutover-status' '--harness-home' $Plan.harnessHome '--action' $Action 2>&1
    $AuthorizationExitCode = $LASTEXITCODE
    if ($AuthorizationExitCode -eq 0) {
      try {
        $Authorization = ($AuthorizationOutput -join [Environment]::NewLine) | ConvertFrom-Json -ErrorAction Stop
      } catch {
        throw "keeper $Action authorization returned invalid JSON: $($_.Exception.Message)"
      }
    }
  }
  if ($AuthorizationExitCode -ne 0) {
    throw "keeper $Action authorization failed with exit code $AuthorizationExitCode"
  }
  $AuthorizationExact = ([string]$Authorization.schema -eq 'agent-harness.ops-cutover-status.v1') -and
    ([string]$Authorization.status -eq 'ready') -and
    ([string]$Authorization.action -eq $Action) -and
    ([string]$Authorization.liveControl.status -eq 'valid') -and
    [string]::Equals([string]$Authorization.harnessHome, [string]$Plan.harnessHome, [StringComparison]::OrdinalIgnoreCase)
  if (-not $AuthorizationExact) {
    throw "keeper $Action authorization is missing, invalid, stale, or for a different action/deployment"
  }
  return $Authorization
}
"#
}

fn ops_control_invocation_body(
    variable_suffix: &str,
    action: &str,
    live_deployment: bool,
) -> String {
    let native = if live_deployment {
        format!(
            "$Ops{variable_suffix}Output = & $Plan.executable.path 'ops-control' '{action}' '--harness-home' $Plan.harnessHome '--reason' 'windows reconcile keeper {action}' 2>&1\n$Ops{variable_suffix}ExitCode = $LASTEXITCODE\n"
        )
    } else {
        format!(
            "$Ops{variable_suffix}Output = & $Plan.executable.path 'ops-control' '{action}' '--harness-home' $Plan.harnessHome '--reason' 'windows reconcile keeper {action}' 2>&1\n$Ops{variable_suffix}ExitCode = $LASTEXITCODE\n"
        )
    };
    if live_deployment {
        return format!("$KeeperFixtureMode = $false\n{native}");
    }
    format!(
        r#"$KeeperFixtureMode = ($Plan.deploymentMode -eq 'developer-staging') -and (-not [string]::IsNullOrWhiteSpace($env:AGENT_HARNESS_KEEPER_TEST_OPS_RECEIPT_FILE))
if ($KeeperFixtureMode) {{
  $OpsFixtureDocument = Get-Content -LiteralPath $env:AGENT_HARNESS_KEEPER_TEST_OPS_RECEIPT_FILE -Raw -ErrorAction Stop | ConvertFrom-Json -ErrorAction Stop
  $OpsFixtureReceipt = if ($null -ne $OpsFixtureDocument.{action}) {{ $OpsFixtureDocument.{action} }} else {{ $OpsFixtureDocument }}
  $Ops{variable_suffix}Output = $OpsFixtureReceipt | ConvertTo-Json -Depth 16
  $Ops{variable_suffix}ExitCode = 0
}} else {{
{native}}}
"#
    )
}

fn start_rollback_function_body(live_deployment: bool) -> String {
    let rollback_invocation = if live_deployment {
        "$RollbackStopOutput = & $Plan.executable.path 'ops-control' 'stop' '--harness-home' $Plan.harnessHome '--reason' 'windows reconcile keeper transactional start rollback' 2>&1\n$RollbackStopExitCode = $LASTEXITCODE\n".to_string()
    } else {
        r#"if ($KeeperFixtureMode) {
  $RollbackFixtureDocument = Get-Content -LiteralPath $env:AGENT_HARNESS_KEEPER_TEST_OPS_RECEIPT_FILE -Raw -ErrorAction Stop | ConvertFrom-Json -ErrorAction Stop
  if ($null -ne $RollbackFixtureDocument.rollbackStop) {
    $RollbackStopOutput = $RollbackFixtureDocument.rollbackStop | ConvertTo-Json -Depth 16
  } else {
    $RollbackStopOutput = $RollbackFixtureDocument | ConvertTo-Json -Depth 16
  }
  $RollbackStopExitCode = 0
} else {
  $RollbackStopOutput = & $Plan.executable.path 'ops-control' 'stop' '--harness-home' $Plan.harnessHome '--reason' 'windows reconcile keeper transactional start rollback' 2>&1
  $RollbackStopExitCode = $LASTEXITCODE
}
"#.to_string()
    };
    format!(
        r#"function Get-KeeperRollbackPinnedArgument {{
  param([Parameter(Mandatory = $true)][string] $Flag)
  $Arguments = @($Plan.action.argumentVector)
  for ($Index = 0; $Index -lt ($Arguments.Count - 1); $Index++) {{
    if ([string]$Arguments[$Index] -eq $Flag) {{ return [string]$Arguments[$Index + 1] }}
  }}
  return $null
}}

function Get-KeeperRollbackProcessTreeSnapshot {{
  $AllProcesses = @(Get-CimInstance Win32_Process -ErrorAction SilentlyContinue)
  $TreeIds = New-Object 'Collections.Generic.HashSet[int]'
  if (Test-Path -LiteralPath $Plan.stateFile -PathType Leaf) {{
    try {{
      $KeeperState = Get-Content -LiteralPath $Plan.stateFile -Raw -ErrorAction Stop | ConvertFrom-Json -ErrorAction Stop
      if ([int]$KeeperState.pid -gt 0 -and $null -ne $KeeperState.processStartTimeMs) {{
        $KeeperProcess = @($AllProcesses | Where-Object {{ [int]$_.ProcessId -eq [int]$KeeperState.pid }})
        if ($KeeperProcess.Count -eq 1) {{
          $KeeperStartMs = $null
          try {{ $KeeperStartMs = ([DateTimeOffset]$KeeperProcess[0].CreationDate).ToUnixTimeMilliseconds() }} catch {{}}
          if ($null -ne $KeeperStartMs -and [int64]$KeeperStartMs -eq [int64]$KeeperState.processStartTimeMs) {{
            [void]$TreeIds.Add([int]$KeeperState.pid)
          }}
        }}
      }}
    }} catch {{}}
  }}
  $ServicesDir = Join-Path ([string]$Plan.harnessHome) 'state\supervisor\services'
  foreach ($StateFile in @(Get-ChildItem -LiteralPath $ServicesDir -Filter '*.json' -File -ErrorAction SilentlyContinue)) {{
    try {{
      $ServiceState = Get-Content -LiteralPath $StateFile.FullName -Raw -ErrorAction Stop | ConvertFrom-Json -ErrorAction Stop
      $Candidate = if ($null -ne $ServiceState.processId) {{ [int]$ServiceState.processId }} else {{ [int]$ServiceState.pid }}
      if ($Candidate -gt 0 -and $null -ne $ServiceState.processStartTimeMs) {{
        $CandidateProcess = @($AllProcesses | Where-Object {{ [int]$_.ProcessId -eq $Candidate }})
        if ($CandidateProcess.Count -eq 1) {{
          $CandidateStartMs = $null
          try {{ $CandidateStartMs = ([DateTimeOffset]$CandidateProcess[0].CreationDate).ToUnixTimeMilliseconds() }} catch {{}}
          if ($null -ne $CandidateStartMs -and [int64]$CandidateStartMs -eq [int64]$ServiceState.processStartTimeMs) {{
            [void]$TreeIds.Add($Candidate)
          }}
        }}
      }}
    }} catch {{}}
  }}
  $Changed = $true
  while ($Changed) {{
    $Changed = $false
    foreach ($Process in $AllProcesses) {{
      if ($TreeIds.Contains([int]$Process.ParentProcessId) -and $TreeIds.Add([int]$Process.ProcessId)) {{ $Changed = $true }}
    }}
  }}
  return @($AllProcesses | Where-Object {{ $TreeIds.Contains([int]$_.ProcessId) }} | ForEach-Object {{
    $StartMs = $null
    try {{ $StartMs = ([DateTimeOffset]$_.CreationDate).ToUnixTimeMilliseconds() }} catch {{}}
    [pscustomobject]@{{
      pid = [int]$_.ProcessId
      parentPid = [int]$_.ParentProcessId
      processStartTimeMs = $StartMs
      executablePath = [string]$_.ExecutablePath
      commandLine = [string]$_.CommandLine
    }}
  }})
}}

function Get-KeeperRollbackProcessTreeSurvivors {{
  param([AllowNull()][AllowEmptyCollection()][object[]] $InitialTree = @())
  $InitialTree = @($InitialTree)
  $AllProcesses = @(Get-CimInstance Win32_Process -ErrorAction SilentlyContinue)
  $TrackedIds = New-Object 'Collections.Generic.HashSet[int]'
  foreach ($Process in $AllProcesses) {{
    $ObservedStartMs = $null
    try {{ $ObservedStartMs = ([DateTimeOffset]$Process.CreationDate).ToUnixTimeMilliseconds() }} catch {{}}
    $InitialExact = @($InitialTree | Where-Object {{
      [int]$_.pid -eq [int]$Process.ProcessId -and
      (($null -eq $_.processStartTimeMs) -or ([int64]$_.processStartTimeMs -eq [int64]$ObservedStartMs))
    }}).Count -gt 0
    if ($InitialExact) {{ [void]$TrackedIds.Add([int]$Process.ProcessId) }}
  }}
  $Changed = $true
  while ($Changed) {{
    $Changed = $false
    foreach ($Process in $AllProcesses) {{
      if ($TrackedIds.Contains([int]$Process.ParentProcessId) -and $TrackedIds.Add([int]$Process.ProcessId)) {{
        $Changed = $true
      }}
    }}
  }}
  $PinnedNode = Get-KeeperRollbackPinnedArgument -Flag '--node-exe'
  $PinnedGateway = Get-KeeperRollbackPinnedArgument -Flag '--gateway-script'
  $PinnedCodex = Get-KeeperRollbackPinnedArgument -Flag '--codex-exe'
  $Survivors = @()
  foreach ($Process in $AllProcesses) {{
    $ObservedStartMs = $null
    try {{ $ObservedStartMs = ([DateTimeOffset]$Process.CreationDate).ToUnixTimeMilliseconds() }} catch {{}}
    $TrackedDescendant = $TrackedIds.Contains([int]$Process.ProcessId)
    $ExecutableExact = [string]::Equals([string]$Process.ExecutablePath, [string]$Plan.executable.path, [StringComparison]::OrdinalIgnoreCase)
    $PinnedNodeGateway = (-not [string]::IsNullOrWhiteSpace($PinnedNode)) -and
      (-not [string]::IsNullOrWhiteSpace($PinnedGateway)) -and
      [string]::Equals([string]$Process.ExecutablePath, $PinnedNode, [StringComparison]::OrdinalIgnoreCase) -and
      (([string]$Process.CommandLine).IndexOf($PinnedGateway, [StringComparison]::OrdinalIgnoreCase) -ge 0)
    $PinnedCodexExact = (-not [string]::IsNullOrWhiteSpace($PinnedCodex)) -and
      [string]::Equals([string]$Process.ExecutablePath, $PinnedCodex, [StringComparison]::OrdinalIgnoreCase)
    if ($TrackedDescendant -or $ExecutableExact -or $PinnedNodeGateway -or $PinnedCodexExact) {{
      $Survivors += [pscustomobject]@{{
        pid = [int]$Process.ProcessId
        parentPid = [int]$Process.ParentProcessId
        processStartTimeMs = $ObservedStartMs
        executablePath = [string]$Process.ExecutablePath
        commandLine = [string]$Process.CommandLine
        disposition = 'exact-planned-process-tree-still-alive-manual-recovery-required'
      }}
    }}
  }}
  return $Survivors
}}

function Test-ExactKeeperServiceOwnerAlive {{
  param([Parameter(Mandatory = $true)] $State)
  $ProcessId = if ($null -ne $State.processId) {{ [int]$State.processId }} else {{ [int]$State.pid }}
  if ($ProcessId -le 0 -or $null -eq $State.processStartTimeMs) {{ return $false }}
  $Process = Get-CimInstance Win32_Process -Filter "ProcessId = $ProcessId" -ErrorAction SilentlyContinue
  if ($null -eq $Process) {{ return $false }}
  try {{
    $ObservedStartMs = ([DateTimeOffset]$Process.CreationDate).ToUnixTimeMilliseconds()
    return [int64]$ObservedStartMs -eq [int64]$State.processStartTimeMs
  }} catch {{
    return $true
  }}
}}

function Invoke-KeeperTransactionalStartRollback {{
  param([Parameter(Mandatory = $true)][string] $Failure)
  $RollbackProcessTreeSnapshot = if ($KeeperFixtureMode) {{ @() }} else {{ @(Get-KeeperRollbackProcessTreeSnapshot) }}
  $RollbackProcessTreeSnapshot = @($RollbackProcessTreeSnapshot)
  $RollbackTrackedProcessTree = @($StartProcessTreeSnapshot) + @($RollbackProcessTreeSnapshot) |
    Group-Object -Property pid,processStartTimeMs |
    ForEach-Object {{ $_.Group[0] }}
  $RollbackTrackedProcessTree = @($RollbackTrackedProcessTree)
  Ensure-KeeperHold -Reason 'transactional-start-rollback'
{rollback_invocation}  $RollbackErrors = @()
  $RollbackStop = $null
  if ($RollbackStopExitCode -ne 0) {{
    $RollbackErrors += "ops-control stop failed with exit code $RollbackStopExitCode"
  }} else {{
    try {{
      $RollbackStop = ($RollbackStopOutput -join [Environment]::NewLine) | ConvertFrom-Json -ErrorAction Stop
      $RollbackExact = ([string]$RollbackStop.schema -eq 'agent-harness.ops-control-receipt.v1') -and
        ([string]$RollbackStop.action -eq 'stop') -and
        ([string]$RollbackStop.status -eq 'stop-requested') -and
        (@($RollbackStop.stopFiles).Count -gt 0) -and
        (@($RollbackStop.stopFiles | Where-Object {{ $_.present -ne $true }}).Count -eq 0) -and
        (@($RollbackStop.stopFiles | Where-Object {{ [string]$_.component -eq 'supervisor-reconcile' }}).Count -eq 1) -and
        (Test-ExactKeeperHold)
      if (-not $RollbackExact) {{ $RollbackErrors += 'ops-control rollback stop receipt was partial or non-exact' }}
    }} catch {{
      $RollbackErrors += "ops-control rollback stop receipt was invalid: $($_.Exception.Message)"
    }}
  }}

  $OwnerDispositions = @()
  if ($KeeperFixtureMode -and -not [string]::IsNullOrWhiteSpace($env:AGENT_HARNESS_KEEPER_TEST_ROLLBACK_OWNERS_FILE)) {{
    $RollbackOwnerFixture = Get-Content -LiteralPath $env:AGENT_HARNESS_KEEPER_TEST_ROLLBACK_OWNERS_FILE -Raw -ErrorAction Stop | ConvertFrom-Json -ErrorAction Stop
    $OwnerDispositions = @($RollbackOwnerFixture.owners)
    $RollbackProcessTreeSurvivors = @($RollbackOwnerFixture.processTreeSurvivors)
    $RollbackDeploymentProcesses = @($RollbackOwnerFixture.deploymentProcesses)
  }} else {{
    $ServicesDir = Join-Path ([string]$Plan.harnessHome) 'state\supervisor\services'
    $Deadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds)
    do {{
      $OwnerDispositions = @()
      foreach ($StateFile in @(Get-ChildItem -LiteralPath $ServicesDir -Filter '*.json' -File -ErrorAction SilentlyContinue)) {{
        try {{
          $ServiceState = Get-Content -LiteralPath $StateFile.FullName -Raw -ErrorAction Stop | ConvertFrom-Json -ErrorAction Stop
          if ([string]$ServiceState.schema -eq 'agent-harness.supervisor-service-state.v1' -and (Test-ExactKeeperServiceOwnerAlive -State $ServiceState)) {{
            $OwnerDispositions += [pscustomobject]@{{
              serviceId = [string]$ServiceState.serviceId
              pid = if ($null -ne $ServiceState.processId) {{ $ServiceState.processId }} else {{ $ServiceState.pid }}
              processStartTimeMs = $ServiceState.processStartTimeMs
              disposition = 'manual-recovery-required'
            }}
          }}
        }} catch {{
          $OwnerDispositions += [pscustomobject]@{{ serviceId = $StateFile.BaseName; disposition = 'invalid-owner-state-manual-recovery-required' }}
        }}
      }}
      if ($OwnerDispositions.Count -gt 0) {{ Start-Sleep -Milliseconds 250 }}
    }} while ($OwnerDispositions.Count -gt 0 -and [DateTimeOffset]::UtcNow -lt $Deadline)
    $RollbackProcessTreeSurvivors = @(Get-KeeperRollbackProcessTreeSurvivors -InitialTree $RollbackTrackedProcessTree)
    $RollbackDeploymentProcesses = @(
      Get-CimInstance Win32_Process -Filter "name = 'agent-harness.exe'" -ErrorAction SilentlyContinue |
        Where-Object {{ ([string]$_.CommandLine).IndexOf([string]$Plan.harnessHome, [StringComparison]::OrdinalIgnoreCase) -ge 0 }} |
        ForEach-Object {{ [pscustomobject]@{{ pid = $_.ProcessId; commandLine = $_.CommandLine }} }}
    )
  }}
  if (@($OwnerDispositions | Where-Object {{ [string]$_.disposition -match 'manual-recovery-required' -or $_.alive -eq $true }}).Count -gt 0) {{
    $RollbackErrors += 'one or more exact planned child owners did not stop'
  }}
  if ($RollbackProcessTreeSurvivors.Count -gt 0 -or $RollbackDeploymentProcesses.Count -gt 0) {{
    $RollbackErrors += 'one or more exact planned descendant/tool processes survived rollback stop'
  }}

  try {{
    $RollbackTask = Assert-KeeperTaskExact -Mutation 'transactional rollback Disable' -AllowMissing
    if ($RollbackTask.exists -eq $true -and
        ($RollbackTask.task.enabled -eq $true -or [string]$RollbackTask.task.state -eq 'Running')) {{
      [void](Invoke-ExactKeeperTaskMutation -Mutation 'Disable' -AllowMissing)
    }}
  }} catch {{
    $RollbackErrors += "keeper task could not be safely disabled: $($_.Exception.Message)"
  }}
  $Receipt = [ordered]@{{
    schema = 'agent-harness.windows-reconcile-keeper-start-rollback.v1'
    planDigest = $Plan.planDigest
    actionDigest = $Plan.action.digest
    taskPath = $Plan.task.path
    taskName = $Plan.task.name
    failure = $Failure
    stopMarkerSnapshot = $StartStopMarkerSnapshot
    rollbackStopReceipt = $RollbackStop
    ownerDispositions = $OwnerDispositions
    initialProcessTree = $StartProcessTreeSnapshot
    rollbackEntryProcessTree = $RollbackProcessTreeSnapshot
    trackedProcessTree = $RollbackTrackedProcessTree
    processTreeSurvivors = $RollbackProcessTreeSurvivors
    deploymentProcesses = $RollbackDeploymentProcesses
    errors = $RollbackErrors
    status = if ($RollbackErrors.Count -eq 0) {{ 'rolled-back' }} else {{ 'manual-recovery-required' }}
    observedAtMs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
  }}
  Write-AtomicJson -Path (Join-Path ([string]$Plan.outputDir) 'start-rollback-latest.json') -Value $Receipt
  if ($RollbackErrors.Count -gt 0) {{
    throw "transactional keeper start rollback is partial; manual recovery required: $($RollbackErrors -join '; ')"
  }}
}}
"#
    )
}

fn ensure_keeper_hold_function_body() -> &'static str {
    r#"function Ensure-KeeperHold {
  param([Parameter(Mandatory = $true)][string] $Reason)
  $Hold = [ordered]@{
    schema = 'agent-harness.supervisor-stop-file.v1'
    serviceId = 'supervisor-reconcile'
    reason = $Reason
    createdBy = 'windows-reconcile-keeper-script'
    createdAtMs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
    persistent = $true
  }
  Write-AtomicJson -Path $Plan.stopFile -Value $Hold
}
function Write-KeeperFixtureEffect {
  param([Parameter(Mandatory = $true)][string] $Action)
  if ([string]::IsNullOrWhiteSpace($env:AGENT_HARNESS_KEEPER_TEST_EFFECT_FILE)) {
    throw 'developer keeper fixture effect file is required'
  }
  $PriorEffects = @()
  if (Test-Path -LiteralPath $env:AGENT_HARNESS_KEEPER_TEST_EFFECT_FILE -PathType Leaf) {
    try {
      $Prior = Get-Content -LiteralPath $env:AGENT_HARNESS_KEEPER_TEST_EFFECT_FILE -Raw -ErrorAction Stop | ConvertFrom-Json -ErrorAction Stop
      $PriorEffects = @($Prior.effects)
    } catch {
      throw "developer keeper fixture effect history is corrupt: $($_.Exception.Message)"
    }
  }
  $Effect = [ordered]@{
    action = $Action
    atMs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
  }
  Write-AtomicJson -Path $env:AGENT_HARNESS_KEEPER_TEST_EFFECT_FILE -Value ([ordered]@{
    schema = 'agent-harness.windows-reconcile-keeper-test-effect.v1'
    action = $Action
    atMs = $Effect.atMs
    effects = @($PriorEffects) + @($Effect)
  })
}
"#
}

fn account_identity_function_body() -> &'static str {
    r#"function Resolve-KeeperAccountSid {
  param([Parameter(Mandatory = $true)][string] $UserId)
  if ([string]::IsNullOrWhiteSpace($UserId)) { return $null }
  try {
    if ($UserId -match '^S-1-(?:\d+-)+\d+$') {
      return ([Security.Principal.SecurityIdentifier]::new($UserId)).Value
    }
    return ([Security.Principal.NTAccount]::new($UserId)).Translate([Security.Principal.SecurityIdentifier]).Value
  } catch {
    return $null
  }
}

function Test-KeeperAccountIdentityExact {
  param(
    [Parameter(Mandatory = $true)][string] $ExpectedUserId,
    [Parameter(Mandatory = $true)][string] $ObservedUserId
  )
  if ([string]::Equals($ExpectedUserId, $ObservedUserId, [StringComparison]::OrdinalIgnoreCase)) {
    return $true
  }
  $ExpectedSid = Resolve-KeeperAccountSid -UserId $ExpectedUserId
  $ObservedSid = Resolve-KeeperAccountSid -UserId $ObservedUserId
  return (-not [string]::IsNullOrWhiteSpace($ExpectedSid)) -and
    (-not [string]::IsNullOrWhiteSpace($ObservedSid)) -and
    [string]::Equals($ExpectedSid, $ObservedSid, [StringComparison]::OrdinalIgnoreCase)
}

"#
}

fn task_scheduler_guard_function_body() -> &'static str {
    r#"function Get-KeeperRecoveryStartBoundaryText {
  $Value = $Plan.task.trigger.recoveryStartBoundary
  if ($Value -is [DateTime]) {
    return $Value.ToString("yyyy-MM-dd'T'HH:mm:ss", [Globalization.CultureInfo]::InvariantCulture)
  }
  return [string]$Value
}

function Get-KeeperTaskSnapshot {
  if ($KeeperFixtureMode) {
    if ([string]::IsNullOrWhiteSpace($env:AGENT_HARNESS_KEEPER_TEST_TASK_OBSERVATION_FILE)) {
      throw 'developer keeper task observation file is required'
    }
    if ($null -eq $script:KeeperFixtureObservationDocument) {
      $script:KeeperFixtureObservationDocument = Get-Content -LiteralPath $env:AGENT_HARNESS_KEEPER_TEST_TASK_OBSERVATION_FILE -Raw -ErrorAction Stop | ConvertFrom-Json -ErrorAction Stop
      $script:KeeperFixtureObservationIndex = 0
    }
    $Sequence = if ($null -ne $script:KeeperFixtureObservationDocument.sequence) {
      @($script:KeeperFixtureObservationDocument.sequence)
    } else {
      @($script:KeeperFixtureObservationDocument)
    }
    if ($Sequence.Count -eq 0) {
      throw 'developer keeper task observation sequence is empty'
    }
    $Index = [Math]::Min([int]$script:KeeperFixtureObservationIndex, $Sequence.Count - 1)
    $script:KeeperFixtureObservationIndex = [int]$script:KeeperFixtureObservationIndex + 1
    return $Sequence[$Index]
  }

  $Tasks = @(Get-ScheduledTask -TaskPath $Plan.task.path -TaskName $Plan.task.name -ErrorAction SilentlyContinue)
  if ($Tasks.Count -eq 0) {
    return [pscustomobject]@{ exists = $false; task = [pscustomobject]@{ path = $Plan.task.path; name = $Plan.task.name; state = 'Missing'; enabled = $false } }
  }
  if ($Tasks.Count -ne 1) {
    throw "keeper task query returned $($Tasks.Count) objects for exact path/name"
  }
  $Task = $Tasks[0]
  $Actions = @($Task.Actions)
  $Triggers = @($Task.Triggers)
  $LogonTriggers = @($Triggers | Where-Object { [string]$_.CimClass.CimClassName -eq 'MSFT_TaskLogonTrigger' })
  $RecoveryTriggers = @($Triggers | Where-Object { [string]$_.CimClass.CimClassName -eq 'MSFT_TaskTimeTrigger' })
  $Action = if ($Actions.Count -eq 1) { $Actions[0] } else { $null }
  $Trigger = if ($LogonTriggers.Count -eq 1) { $LogonTriggers[0] } else { $null }
  $RecoveryTrigger = if ($RecoveryTriggers.Count -eq 1) { $RecoveryTriggers[0] } else { $null }
  $RecoveryDuration = if ($null -ne $RecoveryTrigger -and -not [string]::IsNullOrWhiteSpace([string]$RecoveryTrigger.Repetition.Duration)) { [string]$RecoveryTrigger.Repetition.Duration } else { $null }
  return [pscustomobject]@{
    exists = $true
    task = [pscustomobject]@{
      path = [string]$Task.TaskPath
      name = [string]$Task.TaskName
      description = [string]$Task.Description
      state = [string]$Task.State
      enabled = [bool]$Task.Settings.Enabled
    }
    principal = [pscustomobject]@{
      userId = [string]$Task.Principal.UserId
      logonType = [string]$Task.Principal.LogonType
      runLevel = [string]$Task.Principal.RunLevel
    }
    action = [pscustomobject]@{
      count = $Actions.Count
      execute = [string]$Action.Execute
      arguments = [string]$Action.Arguments
      workingDirectory = [string]$Action.WorkingDirectory
    }
    trigger = [pscustomobject]@{
      count = $Triggers.Count
      logonCount = $LogonTriggers.Count
      recoveryCount = $RecoveryTriggers.Count
      userId = [string]$Trigger.UserId
      triggerType = [string]$Trigger.CimClass.CimClassName
      recoveryTriggerType = [string]$RecoveryTrigger.CimClass.CimClassName
      recoveryStartBoundary = [string]$RecoveryTrigger.StartBoundary
      recoveryInterval = [string]$RecoveryTrigger.Repetition.Interval
      recoveryDuration = $RecoveryDuration
      recoveryStopAtDurationEnd = [bool]$RecoveryTrigger.Repetition.StopAtDurationEnd
    }
    settings = [pscustomobject]@{
      multipleInstances = [string]$Task.Settings.MultipleInstances
      restartCount = [int]$Task.Settings.RestartCount
      restartInterval = [string]$Task.Settings.RestartInterval
      startWhenAvailable = [bool]$Task.Settings.StartWhenAvailable
      disallowStartIfOnBatteries = [bool]$Task.Settings.DisallowStartIfOnBatteries
      stopIfGoingOnBatteries = [bool]$Task.Settings.StopIfGoingOnBatteries
      executionTimeLimit = [string]$Task.Settings.ExecutionTimeLimit
    }
  }
}

function Test-KeeperTaskExact {
  param([Parameter(Mandatory = $true)] $Snapshot)
  if ($Snapshot.exists -ne $true) { return $false }
  return ([string]$Snapshot.task.path -eq [string]$Plan.task.path) -and
    ([string]$Snapshot.task.name -eq [string]$Plan.task.name) -and
    ([string]$Snapshot.task.description -eq [string]$Plan.task.description) -and
    (Test-KeeperAccountIdentityExact -ExpectedUserId ([string]$Plan.task.principal.userId) -ObservedUserId ([string]$Snapshot.principal.userId)) -and
    ([string]$Snapshot.principal.logonType -eq [string]$Plan.task.principal.logonType) -and
    ([string]$Snapshot.principal.runLevel -eq [string]$Plan.task.principal.runLevel) -and
    ([int]$Snapshot.action.count -eq 1) -and
    [string]::Equals([string]$Snapshot.action.execute, [string]$Plan.schedulerAction.executable, [StringComparison]::OrdinalIgnoreCase) -and
    ([string]$Snapshot.action.arguments -eq [string]$Plan.schedulerAction.arguments) -and
    [string]::Equals([string]$Snapshot.action.workingDirectory, [string]$Plan.schedulerAction.workingDirectory, [StringComparison]::OrdinalIgnoreCase) -and
    ([int]$Snapshot.trigger.count -eq 2) -and
    ([int]$Snapshot.trigger.logonCount -eq 1) -and
    ([int]$Snapshot.trigger.recoveryCount -eq 1) -and
    (Test-KeeperAccountIdentityExact -ExpectedUserId ([string]$Plan.task.trigger.userId) -ObservedUserId ([string]$Snapshot.trigger.userId)) -and
    ([string]$Snapshot.trigger.triggerType -eq 'MSFT_TaskLogonTrigger') -and
    ([string]$Snapshot.trigger.recoveryTriggerType -eq 'MSFT_TaskTimeTrigger') -and
    ([string]$Snapshot.trigger.recoveryStartBoundary -eq (Get-KeeperRecoveryStartBoundaryText)) -and
    ([string]$Snapshot.trigger.recoveryInterval -eq [string]$Plan.task.trigger.recoveryInterval) -and
    ([string]$Snapshot.trigger.recoveryDuration -eq [string]$Plan.task.trigger.recoveryDuration) -and
    ([bool]$Snapshot.trigger.recoveryStopAtDurationEnd -eq [bool]$Plan.task.trigger.recoveryStopAtDurationEnd) -and
    ([string]$Snapshot.settings.multipleInstances -eq [string]$Plan.task.settings.multipleInstances) -and
    ([int]$Snapshot.settings.restartCount -eq [int]$Plan.task.settings.restartCount) -and
    ([string]$Snapshot.settings.restartInterval -eq [string]$Plan.task.settings.restartInterval) -and
    ([bool]$Snapshot.settings.startWhenAvailable -eq [bool]$Plan.task.settings.startWhenAvailable) -and
    ([bool]$Snapshot.settings.disallowStartIfOnBatteries -ne [bool]$Plan.task.settings.allowStartOnBatteries) -and
    ([bool]$Snapshot.settings.stopIfGoingOnBatteries -eq [bool]$Plan.task.settings.stopIfGoingOnBatteries) -and
    ([string]$Snapshot.settings.executionTimeLimit -eq [string]$Plan.task.settings.executionTimeLimit)
}

function Test-KeeperTaskSameDeploymentStale {
  param([Parameter(Mandatory = $true)] $Snapshot)
  if ($Snapshot.exists -ne $true) { return $false }
  $OwnershipPrefix = 'Agent Harness reconcile keeper; taskPath=' + [string]$Plan.task.path + '; ownership=agent-harness.windows-reconcile-keeper:' + [string]$Plan.deploymentIdentity + ':'
  $LegacyOwnershipPrefix = 'Agent Harness reconcile keeper; ownership=agent-harness.windows-reconcile-keeper:' + [string]$Plan.deploymentIdentity + ':'
  try {
    $ExpectedExecutable = [IO.Path]::GetFullPath([string]$Plan.schedulerAction.executable)
    $ObservedExecutable = [IO.Path]::GetFullPath([string]$Snapshot.action.execute)
    $RuntimeRoot = [IO.Path]::GetFullPath((Join-Path ([string]$Plan.harnessHome) 'runtime-workspace')).TrimEnd('\', '/')
    $ObservedWorkingDirectory = [IO.Path]::GetFullPath([string]$Snapshot.action.workingDirectory).TrimEnd('\', '/')
    $WorkingDirectoryContained = $ObservedWorkingDirectory.StartsWith($RuntimeRoot + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)
  } catch {
    return $false
  }
  return ([string]$Snapshot.task.path -eq [string]$Plan.task.path) -and
    ([string]$Snapshot.task.name -eq [string]$Plan.task.name) -and
    (([string]$Snapshot.task.description).StartsWith($OwnershipPrefix, [StringComparison]::Ordinal) -or
      ([string]$Snapshot.task.description).StartsWith($LegacyOwnershipPrefix, [StringComparison]::Ordinal)) -and
    (Test-KeeperAccountIdentityExact -ExpectedUserId ([string]$Plan.task.principal.userId) -ObservedUserId ([string]$Snapshot.principal.userId)) -and
    ([string]$Snapshot.principal.logonType -eq [string]$Plan.task.principal.logonType) -and
    ([string]$Snapshot.principal.runLevel -eq [string]$Plan.task.principal.runLevel) -and
    [string]::Equals($ObservedExecutable, $ExpectedExecutable, [StringComparison]::OrdinalIgnoreCase) -and
    $WorkingDirectoryContained -and
    (Test-KeeperAccountIdentityExact -ExpectedUserId ([string]$Plan.task.trigger.userId) -ObservedUserId ([string]$Snapshot.trigger.userId)) -and
    ([string]$Snapshot.trigger.triggerType -eq 'MSFT_TaskLogonTrigger') -and
    ([string]$Snapshot.task.state -ne 'Running')
}

function Assert-KeeperTaskExact {
  param(
    [Parameter(Mandatory = $true)][string] $Mutation,
    [switch] $AllowMissing
  )
  $Snapshot = Get-KeeperTaskSnapshot
  if ($Snapshot.exists -ne $true) {
    if ($AllowMissing -and (Test-ExactKeeperHold)) { return $Snapshot }
    if ($AllowMissing) {
      throw "keeper task is missing before ${Mutation}, but an exact persistent global hold is not proven"
    }
    throw "keeper task is missing immediately before ${Mutation}: $($Plan.task.name)"
  }
  if (-not (Test-KeeperTaskExact -Snapshot $Snapshot)) {
    throw "keeper task ownership changed immediately before ${Mutation}; refusing scheduler mutation"
  }
  return $Snapshot
}

function Assert-KeeperTaskSameDeploymentStale {
  param([Parameter(Mandatory = $true)][string] $Mutation)
  $Snapshot = Get-KeeperTaskSnapshot
  if (-not (Test-KeeperTaskSameDeploymentStale -Snapshot $Snapshot)) {
    throw "keeper task is foreign, running, or outside the deployment boundary immediately before ${Mutation}"
  }
  return $Snapshot
}

function Test-ExactKeeperHold {
  if (-not (Test-Path -LiteralPath $Plan.stopFile -PathType Leaf)) { return $false }
  try {
    $Hold = Get-Content -LiteralPath $Plan.stopFile -Raw -ErrorAction Stop | ConvertFrom-Json -ErrorAction Stop
    return ([string]$Hold.schema -eq 'agent-harness.supervisor-stop-file.v1') -and
      ([string]$Hold.serviceId -eq 'supervisor-reconcile') -and
      ($Hold.persistent -eq $true)
  } catch {
    return $false
  }
}

function Set-KeeperTaskXmlDisabled {
  param([Parameter(Mandatory = $true)][string] $Xml)
  [xml]$Document = $Xml
  $Namespace = New-Object Xml.XmlNamespaceManager($Document.NameTable)
  $Namespace.AddNamespace('t', 'http://schemas.microsoft.com/windows/2004/02/mit/task')
  $Enabled = $Document.SelectSingleNode('/t:Task/t:Settings/t:Enabled', $Namespace)
  if ($null -eq $Enabled) {
    $Enabled = $Document.CreateElement('Enabled', $Document.DocumentElement.NamespaceURI)
    [void]$Document.SelectSingleNode('/t:Task/t:Settings', $Namespace).AppendChild($Enabled)
  }
  $Enabled.InnerText = 'false'
  return $Document.OuterXml
}

function New-KeeperTaskXml {
  $Description = [Security.SecurityElement]::Escape([string]$Plan.task.description)
  $UserId = [Security.SecurityElement]::Escape([string]$Plan.task.principal.userId)
  $Command = [Security.SecurityElement]::Escape([string]$Plan.schedulerAction.executable)
  $Arguments = [Security.SecurityElement]::Escape([string]$Plan.schedulerAction.arguments)
  $WorkingDirectory = [Security.SecurityElement]::Escape([string]$Plan.schedulerAction.workingDirectory)
  $RecoveryStartBoundary = [Security.SecurityElement]::Escape((Get-KeeperRecoveryStartBoundaryText))
  $RecoveryInterval = [Security.SecurityElement]::Escape([string]$Plan.task.trigger.recoveryInterval)
  return @"
<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.4" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo><Description>$Description</Description></RegistrationInfo>
  <Triggers>
    <LogonTrigger><Enabled>true</Enabled><UserId>$UserId</UserId></LogonTrigger>
    <TimeTrigger><Repetition><Interval>$RecoveryInterval</Interval><StopAtDurationEnd>false</StopAtDurationEnd></Repetition><StartBoundary>$RecoveryStartBoundary</StartBoundary><Enabled>true</Enabled></TimeTrigger>
  </Triggers>
  <Principals><Principal id="Author"><UserId>$UserId</UserId><LogonType>InteractiveToken</LogonType><RunLevel>LeastPrivilege</RunLevel></Principal></Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <StartWhenAvailable>true</StartWhenAvailable>
    <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>
    <Enabled>false</Enabled>
    <RestartOnFailure><Interval>PT1M</Interval><Count>255</Count></RestartOnFailure>
  </Settings>
  <Actions Context="Author"><Exec><Command>$Command</Command><Arguments>$Arguments</Arguments><WorkingDirectory>$WorkingDirectory</WorkingDirectory></Exec></Actions>
</Task>
"@
}

function Invoke-ExactKeeperTaskMutation {
  param(
    [Parameter(Mandatory = $true)][ValidateSet('Enable', 'Start', 'Disable', 'Stop', 'Unregister')][string] $Mutation,
    [switch] $AllowMissing
  )
  $Snapshot = Assert-KeeperTaskExact -Mutation $Mutation -AllowMissing:$AllowMissing
  if ($Snapshot.exists -ne $true) { return $false }
  if ($KeeperFixtureMode) {
    Write-KeeperFixtureEffect -Action ($Mutation.ToLowerInvariant() + '-task')
    return $true
  }
  switch ($Mutation) {
    'Enable' { Enable-ScheduledTask -TaskPath $Plan.task.path -TaskName $Plan.task.name -ErrorAction Stop | Out-Null }
    'Start' { Start-ScheduledTask -TaskPath $Plan.task.path -TaskName $Plan.task.name -ErrorAction Stop }
    'Disable' { Disable-ScheduledTask -TaskPath $Plan.task.path -TaskName $Plan.task.name -ErrorAction Stop | Out-Null }
    'Stop' { Stop-ScheduledTask -TaskPath $Plan.task.path -TaskName $Plan.task.name -ErrorAction Stop }
    'Unregister' { Unregister-ScheduledTask -TaskPath $Plan.task.path -TaskName $Plan.task.name -Confirm:$false -ErrorAction Stop }
  }
  return $true
}
"#
}

fn legacy_task_preflight_function_body() -> &'static str {
    r#"function Get-LegacyFixtureSnapshot {
  if (-not $KeeperFixtureMode -or [string]::IsNullOrWhiteSpace($env:AGENT_HARNESS_KEEPER_TEST_LEGACY_OBSERVATION_FILE)) {
    return $null
  }
  if ($null -eq $script:KeeperLegacyFixtureDocument) {
    $script:KeeperLegacyFixtureDocument = Get-Content -LiteralPath $env:AGENT_HARNESS_KEEPER_TEST_LEGACY_OBSERVATION_FILE -Raw -ErrorAction Stop | ConvertFrom-Json -ErrorAction Stop
    $script:KeeperLegacyFixtureIndex = 0
  }
  $Sequence = if ($null -ne $script:KeeperLegacyFixtureDocument.sequence) {
    @($script:KeeperLegacyFixtureDocument.sequence)
  } else {
    @($script:KeeperLegacyFixtureDocument)
  }
  if ($Sequence.Count -eq 0) {
    throw 'developer legacy task observation sequence is empty'
  }
  $Index = [Math]::Min([int]$script:KeeperLegacyFixtureIndex, $Sequence.Count - 1)
  $script:KeeperLegacyFixtureIndex = [int]$script:KeeperLegacyFixtureIndex + 1
  return $Sequence[$Index]
}

function Write-LegacyPreflightReceipt {
  param(
    [Parameter(Mandatory = $true)][string] $Phase,
    [Parameter(Mandatory = $true)][string] $Status,
    [Parameter(Mandatory = $true)] $Tasks,
    [string] $LegacyPlanSha256,
    [string] $Reason
  )
  $Receipt = [ordered]@{
    schema = 'agent-harness.windows-reconcile-keeper-legacy-preflight.v1'
    phase = $Phase
    deploymentIdentity = $Plan.deploymentIdentity
    planDigest = $Plan.planDigest
    taskPath = $Plan.task.path
    legacyPlanSha256 = $LegacyPlanSha256
    observedAtMs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
    tasks = @($Tasks)
    status = $Status
    reason = $Reason
  }
  Write-AtomicJson -Path (Join-Path ([string]$Plan.outputDir) 'legacy-task-preflight-latest.json') -Value $Receipt
}

function Assert-LegacySupervisorTasksQuiescent {
  param([Parameter(Mandatory = $true)][string] $Phase)
  $LegacyRoot = Join-Path ([string]$Plan.harnessHome) 'state\supervisor\windows-scheduled-tasks'
  $LegacyScriptsRoot = [IO.Path]::GetFullPath((Join-Path $LegacyRoot 'scripts')).TrimEnd('\', '/')
  $LegacyPlanFile = Join-Path $LegacyRoot 'supervisor-plan.json'
  $FixtureSnapshot = Get-LegacyFixtureSnapshot
  $ManifestFile = Join-Path ([string]$Plan.outputDir) 'legacy-task-migration.json'

  if (-not (Test-Path -LiteralPath $LegacyPlanFile -PathType Leaf)) {
    if ($null -ne $FixtureSnapshot) {
      $Orphans = @($FixtureSnapshot.tasks)
    } else {
      $Orphans = @()
      foreach ($ScheduledTask in @(Get-ScheduledTask -ErrorAction SilentlyContinue)) {
        foreach ($ScheduledAction in @($ScheduledTask.Actions)) {
          $Arguments = [string]$ScheduledAction.Arguments
          if ($Arguments.IndexOf($LegacyScriptsRoot, [StringComparison]::OrdinalIgnoreCase) -ge 0) {
            $Orphans += [pscustomobject]@{
              taskPath = [string]$ScheduledTask.TaskPath
              name = [string]$ScheduledTask.TaskName
              exists = $true
              exactOwned = $false
              state = [string]$ScheduledTask.State
              enabled = [bool]$ScheduledTask.Settings.Enabled
              execute = [string]$ScheduledAction.Execute
              arguments = $Arguments
              reason = 'orphan action rooted in the exact deployment legacy runner directory'
            }
          }
        }
      }
    }
    if ($Orphans.Count -gt 0) {
      Write-LegacyPreflightReceipt -Phase $Phase -Status 'orphan-legacy-task-blocked' -Tasks $Orphans -Reason 'legacy plan is absent but scheduler actions still reference the exact deployment runner directory'
      throw 'legacy supervisor plan is absent but exact-deployment legacy scheduler actions remain; left untouched'
    }
    Write-LegacyPreflightReceipt -Phase $Phase -Status 'ready-no-legacy-inventory' -Tasks @() -Reason 'fresh exact deployment scheduler inventory found no legacy runner actions'
    return
  }

  $LegacyPlan = Get-Content -LiteralPath $LegacyPlanFile -Raw -ErrorAction Stop | ConvertFrom-Json -ErrorAction Stop
  $LegacyPlanSha256 = Get-FileSha256 -Path $LegacyPlanFile
  if ([string]$LegacyPlan.schema -ne 'agent-harness.windows-supervisor-plan.v1' -or
      -not [string]::Equals([string]$LegacyPlan.harnessHome, [string]$Plan.harnessHome, [StringComparison]::OrdinalIgnoreCase)) {
    Write-LegacyPreflightReceipt -Phase $Phase -Status 'foreign-plan-blocked' -Tasks @() -LegacyPlanSha256 $LegacyPlanSha256 -Reason 'legacy plan schema or harnessHome is not exact'
    throw 'legacy supervisor plan is not an exact deployment-owned plan'
  }
  $DuplicatePlannedNames = @(
    @($LegacyPlan.tasks) |
      Group-Object { ([string]$_.name).ToLowerInvariant() } |
      Where-Object { $_.Count -ne 1 }
  )
  if ($DuplicatePlannedNames.Count -gt 0) {
    Write-LegacyPreflightReceipt -Phase $Phase -Status 'ambiguous-plan-blocked' -Tasks @() -LegacyPlanSha256 $LegacyPlanSha256 -Reason 'legacy plan contains duplicate exact task names'
    throw 'legacy supervisor plan contains duplicate task names and cannot be migrated safely'
  }

  $Observed = @()
  foreach ($LegacyTask in @($LegacyPlan.tasks)) {
    $ExpectedRunner = [IO.Path]::GetFullPath([string]$LegacyTask.runnerScript)
    $ExpectedArguments = '-NoProfile -ExecutionPolicy Bypass -File "' + $ExpectedRunner + '"'
    $RunnerOwned = $ExpectedRunner.StartsWith($LegacyScriptsRoot + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)
    if (-not $RunnerOwned) {
      $Observed += [pscustomobject]@{
        taskPath = '\'
        name = [string]$LegacyTask.name
        exists = $true
        exactOwned = $false
        state = 'Unknown'
        enabled = $false
        reason = 'legacy plan runner is outside the exact deployment legacy scripts directory'
      }
      continue
    }

    if ($null -ne $FixtureSnapshot) {
      $Matches = @($FixtureSnapshot.tasks | Where-Object { [string]$_.name -eq [string]$LegacyTask.name })
      if ($Matches.Count -eq 0) {
        $TaskObservation = [pscustomobject]@{
          taskPath = '\'
          name = [string]$LegacyTask.name
          exists = $false
          exactOwned = $true
          state = 'Missing'
          enabled = $false
          originalEnabled = $false
          originalState = 'Missing'
          originallyMissing = $true
          runnerScript = $ExpectedRunner
          exportedXml = $null
        }
      } elseif ($Matches.Count -ne 1) {
        $TaskObservation = [pscustomobject]@{
          taskPath = $null
          name = [string]$LegacyTask.name
          exists = $true
          exactOwned = $false
          state = 'Ambiguous'
          enabled = $false
          reason = "fixture returned $($Matches.Count) tasks for exact legacy name"
        }
      } else {
        $TaskObservation = $Matches[0]
        $TaskObservation | Add-Member -NotePropertyName originalEnabled -NotePropertyValue ([bool]$TaskObservation.enabled) -Force
        $TaskObservation | Add-Member -NotePropertyName originalState -NotePropertyValue ([string]$TaskObservation.state) -Force
        $TaskObservation | Add-Member -NotePropertyName originallyMissing -NotePropertyValue $false -Force
      }
    } else {
      $Matches = @(Get-ScheduledTask -TaskName ([string]$LegacyTask.name) -ErrorAction SilentlyContinue)
      if ($Matches.Count -eq 0) {
        $TaskObservation = [pscustomobject]@{
          taskPath = '\'
          name = [string]$LegacyTask.name
          exists = $false
          exactOwned = $true
          state = 'Missing'
          enabled = $false
          originalEnabled = $false
          originalState = 'Missing'
          originallyMissing = $true
          runnerScript = $ExpectedRunner
          exportedXml = $null
        }
      } elseif ($Matches.Count -ne 1) {
        $TaskObservation = [pscustomobject]@{
          taskPath = $null
          name = [string]$LegacyTask.name
          exists = $true
          exactOwned = $false
          state = 'Ambiguous'
          enabled = $false
          reason = "scheduler returned $($Matches.Count) tasks across paths for exact legacy name"
        }
      } else {
        $LegacyScheduledTask = $Matches[0]
        $LegacyAction = @($LegacyScheduledTask.Actions)[0]
        $LegacyTrigger = @($LegacyScheduledTask.Triggers)[0]
        $ExactOwned = ([string]$LegacyScheduledTask.TaskPath -eq '\') -and
          [string]::Equals([string]$LegacyAction.Execute, 'powershell.exe', [StringComparison]::OrdinalIgnoreCase) -and
          ([string]$LegacyAction.Arguments -eq $ExpectedArguments) -and
          ([string]$LegacyTrigger.CimClass.CimClassName -eq 'MSFT_TaskLogonTrigger')
        $TaskObservation = [pscustomobject]@{
          taskPath = [string]$LegacyScheduledTask.TaskPath
          name = [string]$LegacyScheduledTask.TaskName
          exists = $true
          exactOwned = $ExactOwned
          state = [string]$LegacyScheduledTask.State
          enabled = [bool]$LegacyScheduledTask.Settings.Enabled
          originalEnabled = [bool]$LegacyScheduledTask.Settings.Enabled
          originalState = [string]$LegacyScheduledTask.State
          originallyMissing = $false
          runnerScript = $ExpectedRunner
          exportedXml = Export-ScheduledTask -TaskPath '\' -TaskName ([string]$LegacyTask.name) -ErrorAction Stop
          definition = [ordered]@{
            description = [string]$LegacyScheduledTask.Description
            principal = $LegacyScheduledTask.Principal
            actions = @($LegacyScheduledTask.Actions)
            triggers = @($LegacyScheduledTask.Triggers)
            settings = $LegacyScheduledTask.Settings
          }
        }
      }
    }
    if ($TaskObservation.exists -eq $true) {
      $TaskObservation | Add-Member -NotePropertyName runnerScript -NotePropertyValue $ExpectedRunner -Force
      $PathExact = [string]$TaskObservation.taskPath -eq '\'
      $FixtureDefinitionExact = if ($null -ne $FixtureSnapshot) {
        ($TaskObservation.exactOwned -eq $true) -and $PathExact
      } else {
        $TaskObservation.exactOwned -eq $true
      }
      $TaskObservation | Add-Member -NotePropertyName exactOwned -NotePropertyValue $FixtureDefinitionExact -Force
    }
    $Observed += $TaskObservation
  }

  $Foreign = @($Observed | Where-Object { $_.exists -eq $true -and $_.exactOwned -ne $true })
  $Active = @($Observed | Where-Object { $_.exists -eq $true -and ($_.enabled -eq $true -or [string]$_.state -eq 'Running') })
  if ($Foreign.Count -gt 0) {
    Write-LegacyPreflightReceipt -Phase $Phase -Status 'foreign-task-blocked' -Tasks $Observed -LegacyPlanSha256 $LegacyPlanSha256 -Reason 'an exact legacy task name is ambiguous, foreign, or outside root TaskPath'
    throw 'foreign, duplicate, or non-root task occupies an exact legacy plan task name; left untouched'
  }
  if (Test-Path -LiteralPath $ManifestFile -PathType Leaf) {
    try {
      $ExistingManifest = Get-Content -LiteralPath $ManifestFile -Raw -ErrorAction Stop | ConvertFrom-Json -ErrorAction Stop
    } catch {
      Write-LegacyPreflightReceipt -Phase $Phase -Status 'migration-manifest-corrupt' -Tasks $Observed -LegacyPlanSha256 $LegacyPlanSha256 -Reason $_.Exception.Message
      throw "legacy migration manifest is corrupt: $($_.Exception.Message)"
    }
    $ManifestExact = ([string]$ExistingManifest.schema -eq 'agent-harness.windows-reconcile-keeper-legacy-migration.v1') -and
      ([string]$ExistingManifest.deploymentIdentity -eq [string]$Plan.deploymentIdentity) -and
      ([string]$ExistingManifest.planDigest -eq [string]$Plan.planDigest) -and
      ([string]$ExistingManifest.legacyPlanFile -eq [string]$LegacyPlanFile) -and
      ([string]$ExistingManifest.legacyPlanSha256 -eq [string]$LegacyPlanSha256) -and
      (@($ExistingManifest.tasks).Count -eq @($LegacyPlan.tasks).Count)
    if (-not $ManifestExact) {
      Write-LegacyPreflightReceipt -Phase $Phase -Status 'migration-manifest-identity-mismatch' -Tasks $Observed -LegacyPlanSha256 $LegacyPlanSha256 -Reason 'immutable deployment or plan identity changed'
      throw 'legacy migration manifest does not match the immutable deployment and plan identity'
    }
    $ManifestTaskIdentityFailures = @()
    foreach ($LegacyTask in @($LegacyPlan.tasks)) {
      $ExpectedRunner = [IO.Path]::GetFullPath([string]$LegacyTask.runnerScript)
      $ManifestMatches = @($ExistingManifest.tasks | Where-Object {
        [string]::Equals([string]$_.name, [string]$LegacyTask.name, [StringComparison]::OrdinalIgnoreCase)
      })
      if ($ManifestMatches.Count -ne 1 -or
          [string]$ManifestMatches[0].taskPath -ne '\' -or
          -not [string]::Equals([string]$ManifestMatches[0].runnerScript, $ExpectedRunner, [StringComparison]::OrdinalIgnoreCase)) {
        $ManifestTaskIdentityFailures += [string]$LegacyTask.name
      }
    }
    if ($ManifestTaskIdentityFailures.Count -gt 0) {
      Write-LegacyPreflightReceipt -Phase $Phase -Status 'migration-manifest-task-identity-mismatch' -Tasks $Observed -LegacyPlanSha256 $LegacyPlanSha256 -Reason ('manifest task identity differs for: ' + ($ManifestTaskIdentityFailures -join ', '))
      throw 'legacy migration manifest task identities do not exactly match the immutable legacy plan'
    }
    $IncompleteDefinitions = @($ExistingManifest.tasks | Where-Object {
      ($_.exists -eq $true -and ([string]::IsNullOrWhiteSpace([string]$_.exportedXml) -or $null -eq $_.originalEnabled -or [string]::IsNullOrWhiteSpace([string]$_.originalState) -or $_.originallyMissing -ne $false)) -or
      ($_.exists -ne $true -and $_.originallyMissing -ne $true)
    })
    if ($IncompleteDefinitions.Count -gt 0) {
      Write-LegacyPreflightReceipt -Phase $Phase -Status 'migration-manifest-incomplete' -Tasks $Observed -LegacyPlanSha256 $LegacyPlanSha256 -Reason 'rollback definitions or original task state are absent'
      throw 'legacy migration manifest lacks full rollback definitions or original enablement state'
    }
  } else {
    $IncompleteDefinitions = @($Observed | Where-Object {
      ($_.exists -eq $true -and ([string]::IsNullOrWhiteSpace([string]$_.exportedXml) -or $null -eq $_.originalEnabled -or [string]::IsNullOrWhiteSpace([string]$_.originalState) -or $_.originallyMissing -ne $false)) -or
      ($_.exists -ne $true -and $_.originallyMissing -ne $true)
    })
    if ($IncompleteDefinitions.Count -gt 0) {
      Write-LegacyPreflightReceipt -Phase $Phase -Status 'migration-capture-incomplete' -Tasks $Observed -LegacyPlanSha256 $LegacyPlanSha256 -Reason 'rollback definitions or original task state could not be captured'
      throw 'cannot promote keeper without full rollback definitions and original legacy task state'
    }
    Write-AtomicJson -Path $ManifestFile -Value ([ordered]@{
      schema = 'agent-harness.windows-reconcile-keeper-legacy-migration.v1'
      deploymentIdentity = $Plan.deploymentIdentity
      planDigest = $Plan.planDigest
      legacyPlanFile = $LegacyPlanFile
      legacyPlanSha256 = $LegacyPlanSha256
      taskPath = '\'
      capturedAtMs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
      tasks = $Observed
      rollbackDisposition = 'retain-exact-owned-tasks-disabled; restore only from exported XML and recorded original state'
    })
  }
  if ($Active.Count -gt 0) {
    Write-LegacyPreflightReceipt -Phase $Phase -Status 'legacy-task-active' -Tasks $Observed -LegacyPlanSha256 $LegacyPlanSha256 -Reason 'an exact-owned legacy task is enabled or running; original state was captured before blocking promotion'
    throw 'exact-owned legacy supervisor task is enabled or running; guarded stop is required before keeper promotion'
  }
  Write-LegacyPreflightReceipt -Phase $Phase -Status 'ready' -Tasks $Observed -LegacyPlanSha256 $LegacyPlanSha256 -Reason 'all exact-owned legacy tasks are disabled and non-running'
}
"#
}

fn invoke_keeper_script_function_body() -> &'static str {
    r#"function Invoke-KeeperScript {
  param(
    [Parameter(Mandatory = $true)][string] $Path,
    [string[]] $Arguments = @()
  )
  $WindowsPowerShell = Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
  if (-not (Test-Path -LiteralPath $WindowsPowerShell -PathType Leaf)) {
    throw "Windows PowerShell is missing: $WindowsPowerShell"
  }
  $Output = & $WindowsPowerShell -NoLogo -NoProfile -NonInteractive -ExecutionPolicy Bypass -File $Path @Arguments 2>&1
  $ExitCode = $LASTEXITCODE
  if ($ExitCode -ne 0) {
    throw "keeper script failed with exit code ${ExitCode}: $Path`n$($Output -join [Environment]::NewLine)"
  }
  $Output
}
"#
}

fn keeper_physical_path_function_body() -> &'static str {
    r#"function Assert-KeeperPhysicalContainment {
  param(
    [Parameter(Mandatory = $true)][string] $Root,
    [Parameter(Mandatory = $true)][string] $Path,
    [Parameter(Mandatory = $true)][bool] $RequireExisting
  )
  $RootFull = [IO.Path]::GetFullPath($Root).TrimEnd('\', '/')
  $PathFull = [IO.Path]::GetFullPath($Path)
  $RootPrefix = $RootFull + [IO.Path]::DirectorySeparatorChar
  if (-not [string]::Equals($PathFull, $RootFull, [StringComparison]::OrdinalIgnoreCase) -and
      -not $PathFull.StartsWith($RootPrefix, [StringComparison]::OrdinalIgnoreCase)) {
    throw "keeper physical containment rejected path outside harness home: $PathFull"
  }

  $VolumeRoot = [IO.Path]::GetPathRoot($PathFull)
  if ([string]::IsNullOrWhiteSpace($VolumeRoot)) {
    throw "keeper physical containment could not resolve a volume root: $PathFull"
  }
  $Current = $VolumeRoot
  $Relative = $PathFull.Substring($VolumeRoot.Length)
  $Segments = $Relative.Split([char[]]@('\', '/'), [StringSplitOptions]::RemoveEmptyEntries)
  foreach ($Segment in $Segments) {
    $Current = Join-Path $Current $Segment
    if (-not (Test-Path -LiteralPath $Current)) { break }
    $Item = Get-Item -LiteralPath $Current -Force -ErrorAction Stop
    if (([int]$Item.Attributes -band [int][IO.FileAttributes]::ReparsePoint) -ne 0) {
      throw "keeper physical containment rejected reparse point: $Current"
    }
  }
  if ($RequireExisting -and -not (Test-Path -LiteralPath $PathFull)) {
    throw "keeper physical containment requires an existing path: $PathFull"
  }
  return $PathFull
}

function Assert-KeeperPlanPhysicalContainment {
  if (-not [string]::Equals([IO.Path]::GetFullPath([string]$Plan.harnessHome), [IO.Path]::GetFullPath($ExpectedHarnessHome), [StringComparison]::OrdinalIgnoreCase)) {
    throw 'keeper physical containment rejected a plan for a different harness home'
  }
  if (-not [string]::Equals([IO.Path]::GetFullPath([string]$Plan.planFile), [IO.Path]::GetFullPath($PlanFile), [StringComparison]::OrdinalIgnoreCase)) {
    throw 'keeper physical containment rejected a redirected plan file'
  }
  foreach ($RequiredPath in @($Plan.outputDir, $Plan.planFile)) {
    [void](Assert-KeeperPhysicalContainment -Root $ExpectedHarnessHome -Path ([string]$RequiredPath) -RequireExisting $true)
  }
  foreach ($Script in @($Plan.scripts)) {
    [void](Assert-KeeperPhysicalContainment -Root $ExpectedHarnessHome -Path ([string]$Script.path) -RequireExisting $true)
  }
  foreach ($PlannedPath in @($Plan.stopFile, $Plan.stateFile, $Plan.leaseFile, $Plan.observationFile, $Plan.fallbackReceiptFile)) {
    [void](Assert-KeeperPhysicalContainment -Root $ExpectedHarnessHome -Path ([string]$PlannedPath) -RequireExisting $false)
  }
}
"#
}

fn file_sha256_function_body() -> &'static str {
    r#"function Get-FileSha256 {
  param([Parameter(Mandatory = $true)][string] $Path)
  $Stream = [IO.File]::Open($Path, [IO.FileMode]::Open, [IO.FileAccess]::Read, [IO.FileShare]::ReadWrite)
  try {
    $Hasher = [Security.Cryptography.SHA256]::Create()
    try {
      $Hash = $Hasher.ComputeHash($Stream)
    } finally {
      $Hasher.Dispose()
    }
    ([BitConverter]::ToString($Hash)).Replace('-', '').ToLowerInvariant()
  } finally {
    $Stream.Dispose()
  }
}
"#
}

fn keeper_plan_integrity_function_body() -> &'static str {
    r#"function ConvertTo-KeeperCanonicalJsonString {
  param([AllowNull()][AllowEmptyString()][string] $Value)
  if ($null -eq $Value) { return 'null' }
  $Builder = New-Object Text.StringBuilder
  [void]$Builder.Append('"')
  foreach ($Character in $Value.ToCharArray()) {
    $Code = [int]$Character
    if ($Code -eq 34) {
      [void]$Builder.Append('\"')
    } elseif ($Code -eq 92) {
      [void]$Builder.Append('\\')
    } elseif ($Code -eq 8) {
      [void]$Builder.Append('\b')
    } elseif ($Code -eq 9) {
      [void]$Builder.Append('\t')
    } elseif ($Code -eq 10) {
      [void]$Builder.Append('\n')
    } elseif ($Code -eq 12) {
      [void]$Builder.Append('\f')
    } elseif ($Code -eq 13) {
      [void]$Builder.Append('\r')
    } elseif ($Code -lt 32) {
      [void]$Builder.Append(('\u' + $Code.ToString('x4', [Globalization.CultureInfo]::InvariantCulture)))
    } else {
      [void]$Builder.Append($Character)
    }
  }
  [void]$Builder.Append('"')
  return $Builder.ToString()
}

function ConvertTo-KeeperCanonicalJson {
  param(
    [AllowNull()] $Value,
    [AllowNull()][string] $ExcludedProperty
  )
  if ($null -eq $Value) { return 'null' }
  if ($Value -is [string] -or $Value -is [char]) {
    return ConvertTo-KeeperCanonicalJsonString -Value ([string]$Value)
  }
  if ($Value -is [bool]) {
    if ([bool]$Value) { return 'true' }
    return 'false'
  }
  if ($Value -is [byte] -or $Value -is [sbyte] -or
      $Value -is [int16] -or $Value -is [uint16] -or
      $Value -is [int32] -or $Value -is [uint32] -or
      $Value -is [int64] -or $Value -is [uint64]) {
    return ([IFormattable]$Value).ToString($null, [Globalization.CultureInfo]::InvariantCulture)
  }
  if ($Value -is [decimal] -or $Value -is [single] -or $Value -is [double]) {
    throw 'keeper plan canonical JSON does not permit floating-point values'
  }
  if ($Value -is [Collections.IDictionary]) {
    $Names = New-Object 'Collections.Generic.List[string]'
    foreach ($Key in $Value.Keys) {
      if ($Key -isnot [string]) { throw 'keeper plan canonical JSON object key is not a string' }
      if ([string]::Equals([string]$Key, $ExcludedProperty, [StringComparison]::Ordinal)) { continue }
      $Names.Add([string]$Key)
    }
    $Names.Sort([StringComparer]::Ordinal)
    $Entries = New-Object 'Collections.Generic.List[string]'
    foreach ($Name in $Names) {
      $KeyJson = ConvertTo-KeeperCanonicalJsonString -Value $Name
      $ValueJson = ConvertTo-KeeperCanonicalJson -Value $Value[$Name]
      $Entries.Add($KeyJson + ':' + $ValueJson)
    }
    return '{' + [string]::Join(',', $Entries.ToArray()) + '}'
  }
  if ($Value -is [Collections.IEnumerable]) {
    $Items = New-Object 'Collections.Generic.List[string]'
    foreach ($Item in $Value) {
      $Items.Add((ConvertTo-KeeperCanonicalJson -Value $Item))
    }
    return '[' + [string]::Join(',', $Items.ToArray()) + ']'
  }
  if ($Value -is [pscustomobject]) {
    $Names = New-Object 'Collections.Generic.List[string]'
    foreach ($Property in $Value.PSObject.Properties) {
      if ([string]::Equals([string]$Property.Name, $ExcludedProperty, [StringComparison]::Ordinal)) { continue }
      $Names.Add([string]$Property.Name)
    }
    $Names.Sort([StringComparer]::Ordinal)
    $Entries = New-Object 'Collections.Generic.List[string]'
    foreach ($Name in $Names) {
      $KeyJson = ConvertTo-KeeperCanonicalJsonString -Value $Name
      $Property = $Value.PSObject.Properties[$Name]
      $ValueJson = ConvertTo-KeeperCanonicalJson -Value $Property.Value
      $Entries.Add($KeyJson + ':' + $ValueJson)
    }
    return '{' + [string]::Join(',', $Entries.ToArray()) + '}'
  }
  throw "keeper plan canonical JSON encountered unsupported type: $($Value.GetType().FullName)"
}

function Get-KeeperCanonicalJsonSha256 {
  param(
    [Parameter(Mandatory = $true)] $Value,
    [Parameter(Mandatory = $true)][string] $ExcludedProperty
  )
  $CanonicalJson = ConvertTo-KeeperCanonicalJson -Value $Value -ExcludedProperty $ExcludedProperty
  $Utf8NoBom = New-Object Text.UTF8Encoding($false)
  $Bytes = $Utf8NoBom.GetBytes($CanonicalJson)
  $Hasher = [Security.Cryptography.SHA256]::Create()
  try {
    $Hash = $Hasher.ComputeHash($Bytes)
  } finally {
    $Hasher.Dispose()
  }
  return ([BitConverter]::ToString($Hash)).Replace('-', '').ToLowerInvariant()
}

function Assert-KeeperPlanIntegrity {
  if ($null -eq $Plan.action) {
    throw 'keeper plan action is missing'
  }
  $StoredActionDigest = [string]$Plan.action.digest
  $ObservedActionDigest = Get-KeeperCanonicalJsonSha256 -Value $Plan.action -ExcludedProperty 'digest'
  if (-not [string]::Equals($StoredActionDigest, $ObservedActionDigest, [StringComparison]::Ordinal)) {
    throw 'keeper action digest does not match canonical action content'
  }
  $StoredPlanDigest = [string]$Plan.planDigest
  $ObservedPlanDigest = Get-KeeperCanonicalJsonSha256 -Value $Plan -ExcludedProperty 'planDigest'
  if (-not [string]::Equals($StoredPlanDigest, $ObservedPlanDigest, [StringComparison]::Ordinal)) {
    throw 'keeper plan digest does not match canonical plan content'
  }
}
"#
}

fn atomic_json_function_body() -> &'static str {
    r#"function Write-AtomicJson([string] $Path, [object] $Value) {
  Assert-KeeperPlanPhysicalContainment
  $FixtureEffectWrite = ([string]$Plan.deploymentMode -eq 'developer-staging') -and
    (-not [string]::IsNullOrWhiteSpace($env:AGENT_HARNESS_KEEPER_TEST_EFFECT_FILE)) -and
    [string]::Equals([IO.Path]::GetFullPath($Path), [IO.Path]::GetFullPath($env:AGENT_HARNESS_KEEPER_TEST_EFFECT_FILE), [StringComparison]::OrdinalIgnoreCase)
  if (-not $FixtureEffectWrite) {
    [void](Assert-KeeperPhysicalContainment -Root $ExpectedHarnessHome -Path $Path -RequireExisting $false)
  }
  $Parent = Split-Path -Parent $Path
  New-Item -ItemType Directory -Force -Path $Parent | Out-Null
  if (-not $FixtureEffectWrite) {
    [void](Assert-KeeperPhysicalContainment -Root $ExpectedHarnessHome -Path $Parent -RequireExisting $true)
    [void](Assert-KeeperPhysicalContainment -Root $ExpectedHarnessHome -Path $Path -RequireExisting $false)
  }
  $Nonce = $PID.ToString() + '.' + [Guid]::NewGuid().ToString('N')
  $TempPath = Join-Path $Parent ('.' + [IO.Path]::GetFileName($Path) + '.tmp.' + $Nonce)
  $BackupPath = Join-Path $Parent ('.' + [IO.Path]::GetFileName($Path) + '.bak.' + $Nonce)
  try {
    $Utf8NoBom = New-Object Text.UTF8Encoding($false)
    [IO.File]::WriteAllText($TempPath, ($Value | ConvertTo-Json -Depth 16), $Utf8NoBom)
    if (Test-Path -LiteralPath $Path -PathType Leaf) {
      [IO.File]::Replace($TempPath, $Path, $BackupPath, $true)
    } else {
      [IO.File]::Move($TempPath, $Path)
    }
  } finally {
    Remove-Item -LiteralPath $TempPath -Force -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath $BackupPath -Force -ErrorAction SilentlyContinue
  }
}
"#
}

fn ensure_keeper_write_path_physically_contained(
    harness_home: &Path,
    target: &Path,
    label: &str,
) -> io::Result<()> {
    let canonical_harness_home = canonical_directory(harness_home, "keeper harness home")?;
    if !keeper_paths_equal(&canonical_harness_home, harness_home) {
        return Err(invalid_input(format!(
            "keeper physical containment failed because harness home now resolves to {} instead of {}",
            canonical_harness_home.display(),
            harness_home.display()
        )));
    }
    let relative = target.strip_prefix(harness_home).map_err(|_| {
        invalid_input(format!(
            "keeper physical containment failed for {label}: {} is outside {}",
            target.display(),
            harness_home.display()
        ))
    })?;
    if relative.components().any(|component| {
        !matches!(
            component,
            std::path::Component::Normal(_) | std::path::Component::CurDir
        )
    }) {
        return Err(invalid_input(format!(
            "keeper physical containment failed for {label}: {} has a non-normal relative component",
            target.display()
        )));
    }

    let mut current = harness_home.to_path_buf();
    ensure_keeper_existing_path_component(
        &canonical_harness_home,
        &current,
        "keeper harness home",
    )?;
    for component in relative.components() {
        if matches!(component, std::path::Component::CurDir) {
            continue;
        }
        current.push(component.as_os_str());
        match fs::symlink_metadata(&current) {
            Ok(_) => {
                ensure_keeper_existing_path_component(&canonical_harness_home, &current, label)?
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => break,
            Err(error) => {
                return Err(io::Error::new(
                    error.kind(),
                    format!(
                        "keeper physical containment could not inspect {label} {}: {error}",
                        current.display()
                    ),
                ));
            }
        }
    }
    Ok(())
}

fn ensure_keeper_existing_path_component(
    canonical_harness_home: &Path,
    path: &Path,
    label: &str,
) -> io::Result<()> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!(
                "keeper physical containment could not inspect {label} {}: {error}",
                path.display()
            ),
        )
    })?;
    if keeper_metadata_is_reparse(&metadata) {
        return Err(invalid_input(format!(
            "keeper physical containment rejected a reparse point in {label}: {}",
            path.display()
        )));
    }
    let resolved = fs::canonicalize(path)
        .map(normalize_canonical_path)
        .map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "keeper physical containment could not resolve {label} {}: {error}",
                    path.display()
                ),
            )
        })?;
    if !keeper_path_is_within(&resolved, canonical_harness_home) {
        return Err(invalid_input(format!(
            "keeper physical containment rejected {label} {} because it resolves outside {}",
            path.display(),
            canonical_harness_home.display()
        )));
    }
    Ok(())
}

#[cfg(windows)]
fn keeper_metadata_is_reparse(metadata: &fs::Metadata) -> bool {
    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x400;
    metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
}

#[cfg(not(windows))]
fn keeper_metadata_is_reparse(metadata: &fs::Metadata) -> bool {
    metadata.file_type().is_symlink()
}

fn keeper_paths_equal(left: &Path, right: &Path) -> bool {
    #[cfg(windows)]
    {
        left.to_string_lossy()
            .replace('/', "\\")
            .eq_ignore_ascii_case(&right.to_string_lossy().replace('/', "\\"))
    }
    #[cfg(not(windows))]
    {
        left == right
    }
}

fn keeper_path_is_within(path: &Path, root: &Path) -> bool {
    if keeper_paths_equal(path, root) {
        return true;
    }
    #[cfg(windows)]
    {
        let path = path.to_string_lossy().replace('/', "\\");
        let mut root = root.to_string_lossy().replace('/', "\\");
        while root.ends_with('\\') {
            root.pop();
        }
        path.len() > root.len()
            && path
                .get(..root.len())
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case(&root))
            && path.as_bytes().get(root.len()) == Some(&b'\\')
    }
    #[cfg(not(windows))]
    {
        path.starts_with(root)
    }
}

fn canonical_directory(path: &Path, label: &str) -> io::Result<PathBuf> {
    let canonical = fs::canonicalize(path)
        .map(normalize_canonical_path)
        .map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("failed to resolve {label} {}: {error}", path.display()),
            )
        })?;
    if !canonical.is_dir() {
        return Err(invalid_input(format!(
            "{label} is not a directory: {}",
            canonical.display()
        )));
    }
    Ok(canonical)
}

fn canonical_file(path: &Path, label: &str) -> io::Result<PathBuf> {
    let canonical = fs::canonicalize(path)
        .map(normalize_canonical_path)
        .map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("failed to resolve {label} {}: {error}", path.display()),
            )
        })?;
    if !canonical.is_file() {
        return Err(invalid_input(format!(
            "{label} is not a file: {}",
            canonical.display()
        )));
    }
    Ok(canonical)
}

fn normalize_canonical_path(path: PathBuf) -> PathBuf {
    #[cfg(windows)]
    {
        let text = path.to_string_lossy();
        if let Some(rest) = text.strip_prefix(r"\\?\UNC\") {
            return PathBuf::from(format!(r"\\{rest}"));
        }
        if let Some(rest) = text.strip_prefix(r"\\?\") {
            return PathBuf::from(rest);
        }
    }
    path
}

fn windows_powershell_executable() -> PathBuf {
    std::env::var_os("SystemRoot")
        .map(PathBuf::from)
        .map(|root| {
            root.join("System32")
                .join("WindowsPowerShell")
                .join("v1.0")
                .join("powershell.exe")
        })
        .and_then(|path| canonical_file(&path, "Windows PowerShell").ok())
        .unwrap_or_else(|| PathBuf::from("powershell.exe"))
}

fn validate_task_name(task_name: &str) -> io::Result<()> {
    if task_name.is_empty()
        || task_name.len() > 238
        || task_name
            .chars()
            .any(|character| character.is_control() || matches!(character, '\\' | '/'))
    {
        return Err(invalid_input(
            "scheduled-task name must be a non-empty root task name without slashes or controls",
        ));
    }
    Ok(())
}

fn push_optional_path_arg(args: &mut Vec<String>, flag: &str, value: Option<&Path>) {
    if let Some(value) = value {
        args.push(flag.to_string());
        args.push(path_arg(value));
    }
}

fn path_arg(path: &Path) -> String {
    path.display().to_string()
}

fn windows_quote_argument(argument: &str) -> String {
    if !argument.is_empty()
        && !argument
            .chars()
            .any(|character| character.is_whitespace() || character == '"')
    {
        return argument.to_string();
    }

    let mut quoted = String::from("\"");
    let mut backslashes = 0usize;
    for character in argument.chars() {
        match character {
            '\\' => backslashes += 1,
            '"' => {
                quoted.push_str(&"\\".repeat(backslashes * 2 + 1));
                quoted.push('"');
                backslashes = 0;
            }
            _ => {
                quoted.push_str(&"\\".repeat(backslashes));
                backslashes = 0;
                quoted.push(character);
            }
        }
    }
    quoted.push_str(&"\\".repeat(backslashes * 2));
    quoted.push('"');
    quoted
}

fn sha256_file(path: &Path) -> io::Result<String> {
    let mut file = File::open(path)?;
    let mut context = Context::new(&SHA256);
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        context.update(&buffer[..read]);
    }
    Ok(hex_lower(context.finish().as_ref()))
}

fn sha256_bytes(bytes: &[u8]) -> String {
    hex_lower(ring::digest::digest(&SHA256, bytes).as_ref())
}

fn deployment_identity_digest(harness_home: &Path) -> String {
    let mut identity = harness_home.display().to_string().replace('\\', "/");
    if cfg!(windows) {
        identity.make_ascii_lowercase();
    }
    hex_lower(ring::digest::digest(&SHA256, identity.as_bytes()).as_ref())
}

fn action_digest(action: &WindowsReconcileKeeperAction) -> io::Result<String> {
    let material = serde_json::to_value(action).map_err(io::Error::other)?;
    windows_reconcile_keeper_action_digest(&material).map_err(invalid_input)
}

fn plan_digest(plan: &WindowsReconcileKeeperPlan) -> io::Result<String> {
    let material = serde_json::to_value(plan).map_err(io::Error::other)?;
    windows_reconcile_keeper_plan_digest(&material).map_err(invalid_input)
}

pub fn windows_reconcile_keeper_action_digest(action: &Value) -> Result<String, String> {
    json_digest_without(action, "digest")
}

pub fn windows_reconcile_keeper_plan_digest(plan: &Value) -> Result<String, String> {
    json_digest_without(plan, "planDigest")
}

fn json_digest_without(value: &Value, field: &str) -> Result<String, String> {
    let mut material = value.clone();
    material
        .as_object_mut()
        .ok_or_else(|| "keeper digest material must be a JSON object".to_string())?
        .remove(field);
    let encoded = serde_json::to_vec(&material)
        .map_err(|error| format!("failed to serialize keeper digest material: {error}"))?;
    Ok(hex_lower(ring::digest::digest(&SHA256, &encoded).as_ref()))
}

/// Revalidates the deployable Windows keeper boundary without trusting the
/// generator that produced the plan.
pub fn validate_live_windows_reconcile_keeper_plan(plan: &Value) -> Result<(), String> {
    if plan.get("deploymentMode").and_then(Value::as_str) != Some("live") {
        return Err("keeper plan is not a live deployment plan".to_string());
    }
    if plan.pointer("/task/path").and_then(Value::as_str) != Some("\\") {
        return Err("live keeper task path must be the exact root task path".to_string());
    }
    if plan
        .pointer("/task/principal/currentUserOnly")
        .and_then(Value::as_bool)
        != Some(true)
        || plan
            .pointer("/task/principal/logonType")
            .and_then(Value::as_str)
            != Some("Interactive")
        || plan
            .pointer("/task/principal/runLevel")
            .and_then(Value::as_str)
            != Some("Limited")
    {
        return Err(
            "live keeper principal must be the current interactive user at limited privilege"
                .to_string(),
        );
    }
    if plan.pointer("/task/trigger/kind").and_then(Value::as_str) != Some("AtLogOn")
        || plan
            .pointer("/task/trigger/recoveryKind")
            .and_then(Value::as_str)
            != Some(KEEPER_RECOVERY_TRIGGER_KIND)
        || plan
            .pointer("/task/trigger/recoveryStartBoundary")
            .and_then(Value::as_str)
            != Some(KEEPER_RECOVERY_START_BOUNDARY)
        || plan
            .pointer("/task/trigger/recoveryInterval")
            .and_then(Value::as_str)
            != Some(KEEPER_RECOVERY_INTERVAL)
        || !matches!(
            plan.pointer("/task/trigger/recoveryDuration"),
            Some(Value::Null)
        )
        || plan
            .pointer("/task/trigger/recoveryStopAtDurationEnd")
            .and_then(Value::as_bool)
            != Some(false)
    {
        return Err(
            "live keeper must have the exact same-user logon and indefinite one-minute recovery trigger contract"
                .to_string(),
        );
    }
    if plan
        .pointer("/task/settings/multipleInstances")
        .and_then(Value::as_str)
        != Some("IgnoreNew")
        || plan
            .pointer("/task/settings/executionTimeLimit")
            .and_then(Value::as_str)
            != Some("PT0S")
        || plan
            .pointer("/task/registrationEnabled")
            .and_then(Value::as_bool)
            != Some(false)
    {
        return Err(
            "live keeper must be registered disabled with IgnoreNew and no execution limit"
                .to_string(),
        );
    }
    let harness_home = canonical_value_directory(plan, "/harnessHome", "harness home")?;
    let source_home = canonical_value_directory(plan, "/sourceHome", "source home")?;
    if source_home != harness_home {
        return Err("live keeper source home must equal harness home".to_string());
    }
    let workspace = canonical_value_directory(plan, "/workspace", "workspace")?;
    let workspace_root = harness_home.join("workspace");
    if workspace != workspace_root && !workspace.starts_with(&workspace_root) {
        return Err(format!(
            "live keeper workspace must be below {}",
            workspace_root.display()
        ));
    }
    let runtime_workspace =
        canonical_value_directory(plan, "/runtimeWorkspace", "runtime workspace")?;
    let runtime_root = harness_home.join("runtime-workspace");
    if runtime_workspace == runtime_root || !runtime_workspace.starts_with(&runtime_root) {
        return Err(format!(
            "live keeper runtime workspace must be a child of {}",
            runtime_root.display()
        ));
    }

    let executable = canonical_value_file(plan, "/executable/path", "harness CLI")?;
    let expected_executable = canonical_file(
        &harness_home
            .join("bin")
            .join("current")
            .join(if cfg!(windows) {
                "agent-harness.exe"
            } else {
                "agent-harness"
            }),
        "canonical live harness CLI",
    )
    .map_err(|error| error.to_string())?;
    if executable != expected_executable
        || plan.pointer("/action/executable").and_then(Value::as_str)
            != Some(expected_executable.to_string_lossy().as_ref())
    {
        return Err(format!(
            "live keeper executable must be exactly {}",
            expected_executable.display()
        ));
    }
    if plan
        .pointer("/action/workingDirectory")
        .and_then(Value::as_str)
        != Some(runtime_workspace.to_string_lossy().as_ref())
    {
        return Err(
            "live keeper action working directory is not the runtime workspace".to_string(),
        );
    }
    let scheduler_executable = canonical_value_file(
        plan,
        "/schedulerAction/executable",
        "keeper scheduler PowerShell",
    )?;
    let expected_scheduler_executable = canonical_file(
        &std::env::var_os("SystemRoot")
            .map(PathBuf::from)
            .ok_or_else(|| "SystemRoot is unavailable for live keeper validation".to_string())?
            .join("System32")
            .join("WindowsPowerShell")
            .join("v1.0")
            .join("powershell.exe"),
        "canonical Windows PowerShell",
    )
    .map_err(|error| error.to_string())?;
    if scheduler_executable != expected_scheduler_executable
        || plan
            .pointer("/schedulerAction/workingDirectory")
            .and_then(Value::as_str)
            != Some(runtime_workspace.to_string_lossy().as_ref())
        || plan
            .pointer("/schedulerAction/windowStyle")
            .and_then(Value::as_str)
            != Some("hidden")
        || plan
            .pointer("/schedulerAction/childCreateNoWindow")
            .and_then(Value::as_bool)
            != Some(true)
        || plan
            .pointer("/schedulerAction/synchronousWait")
            .and_then(Value::as_bool)
            != Some(true)
    {
        return Err(
            "live keeper scheduler action must use canonical Windows PowerShell and the hidden synchronous child policy"
                .to_string(),
        );
    }
    let hidden_launcher = harness_home
        .join("state")
        .join("supervisor")
        .join("windows-reconcile-keeper")
        .join("scripts")
        .join("launch-hidden.ps1");
    let expected_scheduler_argument_vector = vec![
        "-NoLogo".to_string(),
        "-NoProfile".to_string(),
        "-NonInteractive".to_string(),
        "-WindowStyle".to_string(),
        "Hidden".to_string(),
        "-ExecutionPolicy".to_string(),
        "Bypass".to_string(),
        "-File".to_string(),
        hidden_launcher.display().to_string(),
    ];
    let observed_scheduler_argument_vector = plan
        .pointer("/schedulerAction/argumentVector")
        .and_then(Value::as_array)
        .ok_or_else(|| "live keeper scheduler argument vector is missing".to_string())?
        .iter()
        .map(|value| {
            value.as_str().map(ToString::to_string).ok_or_else(|| {
                "live keeper scheduler argument vector contains non-text".to_string()
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let expected_scheduler_arguments = expected_scheduler_argument_vector
        .iter()
        .map(|argument| windows_quote_argument(argument))
        .collect::<Vec<_>>()
        .join(" ");
    if observed_scheduler_argument_vector != expected_scheduler_argument_vector
        || plan
            .pointer("/schedulerAction/arguments")
            .and_then(Value::as_str)
            != Some(expected_scheduler_arguments.as_str())
    {
        return Err(
            "live keeper scheduler action does not exactly bind the hidden launcher".to_string(),
        );
    }

    let tools_root = harness_home.join("tools");
    for (flag, label) in [
        ("--codex-exe", "Codex executable"),
        ("--node-exe", "Node executable"),
        ("--gateway-script", "Discord gateway script"),
    ] {
        let path = action_argument_path(plan, flag)
            .ok_or_else(|| format!("live keeper plan is missing pinned {label}"))?;
        let canonical = canonical_file(&path, label).map_err(|error| error.to_string())?;
        if canonical == tools_root || !canonical.starts_with(&tools_root) {
            return Err(format!(
                "live keeper {label} must be below {}",
                tools_root.display()
            ));
        }
    }
    for (flag, expected) in [
        ("--harness-home", harness_home.as_path()),
        ("--source-home", source_home.as_path()),
        ("--workspace", workspace.as_path()),
        ("--runtime-workspace", runtime_workspace.as_path()),
        ("--harness-cli", executable.as_path()),
    ] {
        let actual = action_argument_path(plan, flag)
            .ok_or_else(|| format!("live keeper plan is missing {flag}"))?;
        let canonical = if flag == "--harness-cli" {
            canonical_file(&actual, flag).map_err(|error| error.to_string())?
        } else {
            canonical_directory(&actual, flag).map_err(|error| error.to_string())?
        };
        if canonical != expected {
            return Err(format!(
                "live keeper {flag} does not match its canonical plan field"
            ));
        }
    }
    for (pointer, expected) in [
        (
            "/stopFile",
            harness_home
                .join("state")
                .join("supervisor")
                .join("stop")
                .join("supervisor-reconcile.stop"),
        ),
        (
            "/stateFile",
            harness_home
                .join("state")
                .join("supervisor")
                .join("reconcile-keeper.json"),
        ),
        (
            "/leaseFile",
            harness_home
                .join("state")
                .join("supervisor")
                .join("reconcile-keeper.lock"),
        ),
    ] {
        if plan.pointer(pointer).and_then(Value::as_str)
            != Some(expected.to_string_lossy().as_ref())
        {
            return Err(format!(
                "live keeper {pointer} is outside its exact deployment path"
            ));
        }
    }
    let expected_deployment_identity = deployment_identity_digest(&harness_home);
    if plan.get("deploymentIdentity").and_then(Value::as_str)
        != Some(expected_deployment_identity.as_str())
    {
        return Err("live keeper deployment identity does not match harness home".to_string());
    }
    let expected_output_dir = harness_home
        .join("state")
        .join("supervisor")
        .join("windows-reconcile-keeper");
    if plan.get("outputDir").and_then(Value::as_str)
        != Some(expected_output_dir.to_string_lossy().as_ref())
    {
        return Err(
            "live keeper output directory is outside its exact deployment path".to_string(),
        );
    }
    let scripts = plan
        .get("scripts")
        .and_then(Value::as_array)
        .ok_or_else(|| "live keeper script manifest is missing".to_string())?;
    for name in [
        "launch-hidden",
        "install",
        "start",
        "stop",
        "uninstall",
        "rollback",
        "readback",
    ] {
        let matches = scripts
            .iter()
            .filter(|script| script.get("name").and_then(Value::as_str) == Some(name))
            .collect::<Vec<_>>();
        if matches.len() != 1 {
            return Err(format!(
                "live keeper script manifest must contain exactly one {name} script"
            ));
        }
        let expected_path = expected_output_dir
            .join("scripts")
            .join(format!("{name}.ps1"));
        if matches[0].get("path").and_then(Value::as_str)
            != Some(expected_path.to_string_lossy().as_ref())
        {
            return Err(format!(
                "live keeper {name} script path is outside its exact deployment path"
            ));
        }
        let digest = matches[0]
            .get("sha256")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(format!(
                "live keeper {name} script digest is missing or invalid"
            ));
        }
    }
    let stored_action_digest = plan.pointer("/action/digest").and_then(Value::as_str);
    let computed_action_digest = windows_reconcile_keeper_action_digest(
        plan.pointer("/action")
            .ok_or_else(|| "keeper plan action is missing".to_string())?,
    )?;
    if stored_action_digest != Some(computed_action_digest.as_str()) {
        return Err("live keeper action digest is invalid".to_string());
    }
    let stored_plan_digest = plan.get("planDigest").and_then(Value::as_str);
    let computed_plan_digest = windows_reconcile_keeper_plan_digest(plan)?;
    if stored_plan_digest != Some(computed_plan_digest.as_str()) {
        return Err("live keeper plan digest is invalid".to_string());
    }
    Ok(())
}

/// Pure identity predicate shared by readiness tests and the generated
/// install-script contract. It never calls Task Scheduler.
pub fn keeper_task_identity_matches(plan: &Value, observed: &Value) -> bool {
    for (planned, actual) in [
        ("/task/path", "/task/observedPath"),
        ("/task/name", "/task/name"),
        ("/task/description", "/task/description"),
        ("/task/principal/userId", "/principal/userId"),
        ("/task/trigger/userId", "/trigger/userId"),
        (
            "/task/trigger/recoveryStartBoundary",
            "/trigger/recoveryStartBoundary",
        ),
        (
            "/task/trigger/recoveryInterval",
            "/trigger/recoveryInterval",
        ),
        (
            "/task/trigger/recoveryDuration",
            "/trigger/recoveryDuration",
        ),
    ] {
        if plan.pointer(planned).and_then(Value::as_str)
            != observed.pointer(actual).and_then(Value::as_str)
        {
            return false;
        }
    }
    let scheduled_action = plan
        .get("schedulerAction")
        .or_else(|| plan.get("action"))
        .and_then(Value::as_object);
    let scheduled_action_matches = scheduled_action.is_some_and(|action| {
        action.get("executable").and_then(Value::as_str)
            == observed.pointer("/action/execute").and_then(Value::as_str)
            && action.get("arguments").and_then(Value::as_str)
                == observed
                    .pointer("/action/arguments")
                    .and_then(Value::as_str)
            && action.get("workingDirectory").and_then(Value::as_str)
                == observed
                    .pointer("/action/workingDirectory")
                    .and_then(Value::as_str)
    });
    scheduled_action_matches
        && observed.pointer("/action/count").and_then(Value::as_u64) == Some(1)
        && observed.pointer("/trigger/count").and_then(Value::as_u64) == Some(2)
        && observed
            .pointer("/trigger/logonCount")
            .and_then(Value::as_u64)
            == Some(1)
        && observed
            .pointer("/trigger/recoveryCount")
            .and_then(Value::as_u64)
            == Some(1)
        && observed
            .pointer("/trigger/triggerType")
            .and_then(Value::as_str)
            == Some("MSFT_TaskLogonTrigger")
        && observed
            .pointer("/trigger/recoveryTriggerType")
            .and_then(Value::as_str)
            == Some("MSFT_TaskTimeTrigger")
        && observed
            .pointer("/trigger/recoveryStopAtDurationEnd")
            .and_then(Value::as_bool)
            == plan
                .pointer("/task/trigger/recoveryStopAtDurationEnd")
                .and_then(Value::as_bool)
}

fn canonical_value_directory(plan: &Value, pointer: &str, label: &str) -> Result<PathBuf, String> {
    let value = plan
        .pointer(pointer)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("keeper plan {label} is missing"))?;
    canonical_directory(Path::new(value), label).map_err(|error| error.to_string())
}

fn canonical_value_file(plan: &Value, pointer: &str, label: &str) -> Result<PathBuf, String> {
    let value = plan
        .pointer(pointer)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("keeper plan {label} is missing"))?;
    canonical_file(Path::new(value), label).map_err(|error| error.to_string())
}

fn action_argument_path(plan: &Value, flag: &str) -> Option<PathBuf> {
    let args = plan.pointer("/action/argumentVector")?.as_array()?;
    args.windows(2).find_map(|pair| {
        (pair[0].as_str() == Some(flag))
            .then(|| pair[1].as_str().map(PathBuf::from))
            .flatten()
    })
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn ps_quote_path(path: &Path) -> String {
    format!("'{}'", path.display().to_string().replace('\'', "''"))
}

fn invalid_input(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(windows)]
    use std::sync::Mutex;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[cfg(windows)]
    struct Command;

    #[cfg(windows)]
    impl Command {
        fn new(program: impl AsRef<std::ffi::OsStr>) -> std::process::Command {
            let mut command = std::process::Command::new(program);
            crate::process_launch::configure_noninteractive_command(&mut command);
            command
        }
    }

    #[cfg(windows)]
    static KEEPER_SCRIPT_ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn plan_has_one_direct_task_and_fully_pinned_reconcile_arguments() {
        let fixture = Fixture::new("one_direct_task");
        let plan = write_windows_reconcile_keeper_plan(fixture.options()).unwrap();

        assert_eq!(plan.schema, WINDOWS_RECONCILE_KEEPER_PLAN_SCHEMA);
        assert!(plan.action.direct_executable);
        assert_eq!(plan.action.executable, plan.executable.path);
        assert_eq!(plan.action.working_directory, fixture.runtime_workspace());
        assert_eq!(plan.scheduler_action.window_style, "hidden");
        assert!(plan.scheduler_action.child_create_no_window);
        assert!(plan.scheduler_action.synchronous_wait);
        assert_eq!(
            plan.scheduler_action.argument_vector,
            vec![
                "-NoLogo".to_string(),
                "-NoProfile".to_string(),
                "-NonInteractive".to_string(),
                "-WindowStyle".to_string(),
                "Hidden".to_string(),
                "-ExecutionPolicy".to_string(),
                "Bypass".to_string(),
                "-File".to_string(),
                plan.output_dir
                    .join("scripts")
                    .join("launch-hidden.ps1")
                    .display()
                    .to_string(),
            ]
        );
        assert!(
            plan.task
                .name
                .starts_with(&format!("{DEFAULT_KEEPER_TASK_NAME_PREFIX}-"))
        );
        assert_eq!(
            plan.task
                .name
                .strip_prefix(&format!("{DEFAULT_KEEPER_TASK_NAME_PREFIX}-"))
                .unwrap()
                .len(),
            12
        );
        assert_eq!(plan.task.principal.user_id, "TEST\\keeper-user");
        assert_eq!(plan.task.trigger.user_id, plan.task.principal.user_id);
        assert_eq!(plan.task.trigger.recovery_kind, "Time");
        assert_eq!(
            plan.task.trigger.recovery_start_boundary,
            KEEPER_RECOVERY_START_BOUNDARY
        );
        assert_eq!(
            plan.task.trigger.recovery_interval,
            KEEPER_RECOVERY_INTERVAL
        );
        assert_eq!(plan.task.trigger.recovery_duration, None);
        assert!(!plan.task.trigger.recovery_stop_at_duration_end);
        assert_eq!(plan.executable.sha256.len(), 64);

        for flag in [
            "supervisor-reconcile",
            "--all",
            "--auto-reconcile",
            "--iterations",
            "--quiet",
            "--stop-file",
            "--harness-home",
            "--source-home",
            "--workspace",
            "--runtime-workspace",
            "--harness-cli",
            "--codex-exe",
            "--node-exe",
            "--gateway-script",
        ] {
            assert!(
                plan.action
                    .argument_vector
                    .iter()
                    .any(|value| value == flag),
                "missing {flag}: {:?}",
                plan.action.argument_vector
            );
        }
        assert_eq!(arg_value(&plan.action.argument_vector, "--iterations"), "0");
        assert_eq!(
            PathBuf::from(arg_value(&plan.action.argument_vector, "--harness-home")),
            fixture.harness_home()
        );
        assert_eq!(
            PathBuf::from(arg_value(&plan.action.argument_vector, "--stop-file")),
            fixture
                .harness_home()
                .join("state/supervisor/stop/supervisor-reconcile.stop")
        );
        assert!(plan.output_dir.starts_with(fixture.harness_home()));
        #[cfg(windows)]
        for path in [
            &plan.harness_home,
            &plan.source_home,
            &plan.runtime_workspace,
            &plan.output_dir,
            &plan.plan_file,
            &plan.executable.path,
        ] {
            assert!(
                !path.display().to_string().starts_with(r"\\?\"),
                "PowerShell/Task Scheduler plan path retained an extended prefix: {}",
                path.display()
            );
        }
        assert_eq!(
            plan.output_dir,
            fixture
                .harness_home()
                .join("state/supervisor/windows-reconcile-keeper")
        );
        assert_eq!(plan.task.path, "\\");
        assert!(!fixture.root.join("sibling-output").exists());
        assert_eq!(plan.scripts.len(), 7);
        assert!(plan.scripts.iter().all(|script| script.sha256.len() == 64));

        fixture.remove();
    }

    #[test]
    fn install_contract_is_interactive_limited_dual_trigger_and_registered_disabled() {
        let fixture = Fixture::new("install_contract");
        let plan = write_windows_reconcile_keeper_plan(fixture.options()).unwrap();
        let install = fixture.script(&plan, "install");

        assert_eq!(plan.task.principal.logon_type, "Interactive");
        assert_eq!(plan.task.principal.run_level, "Limited");
        assert_eq!(plan.task.trigger.kind, "AtLogOn");
        assert!(!plan.task.registration_enabled);
        assert_eq!(plan.task.settings.multiple_instances, "IgnoreNew");
        assert_eq!(plan.task.settings.restart_interval, "PT1M");
        assert!(plan.task.settings.restart_count <= 255);
        assert_eq!(plan.task.settings.execution_time_limit, "PT0S");
        assert!(plan.task.settings.start_when_available);
        assert!(plan.task.settings.allow_start_on_batteries);
        assert!(!plan.task.settings.stop_if_going_on_batteries);

        for required in [
            "WindowsIdentity]::GetCurrent().Name",
            "function New-KeeperTaskXml",
            "<LogonType>InteractiveToken</LogonType>",
            "<RunLevel>LeastPrivilege</RunLevel>",
            "<LogonTrigger><Enabled>true</Enabled><UserId>$UserId</UserId></LogonTrigger>",
            "<TimeTrigger><Repetition><Interval>$RecoveryInterval</Interval><StopAtDurationEnd>false</StopAtDurationEnd></Repetition><StartBoundary>$RecoveryStartBoundary</StartBoundary><Enabled>true</Enabled></TimeTrigger>",
            "<MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>",
            "<StartWhenAvailable>true</StartWhenAvailable>",
            "<ExecutionTimeLimit>PT0S</ExecutionTimeLimit>",
            "<Enabled>false</Enabled>",
            "<RestartOnFailure><Interval>PT1M</Interval><Count>255</Count>",
            "Register-ScheduledTask -TaskPath $Plan.task.path -TaskName $Plan.task.name -Xml $TaskXml",
            "keeper registration was not atomically disabled",
        ] {
            assert!(
                install.contains(required),
                "missing install contract: {required}"
            );
        }
        assert!(install.contains("priorDefinitionXml = $PriorKeeperXml"));
        assert!(install.contains("restore-prior-disabled-task"));
        assert!(
            !install.contains("Register-ScheduledTask -TaskName $Plan.task.name -Action $Action -Trigger $Trigger -Principal $Principal -Settings $Settings -Description $Plan.task.description -Force"),
            "installer must never force-overwrite a task between identity check and registration"
        );

        fixture.remove();
    }

    #[test]
    fn start_and_stop_scripts_preserve_guarded_control_ordering() {
        let fixture = Fixture::new("control_ordering");
        let plan = write_windows_reconcile_keeper_plan(fixture.options()).unwrap();
        let install = fixture.script(&plan, "install");
        let start = fixture.script(&plan, "start");
        let stop = fixture.script(&plan, "stop");
        let uninstall = fixture.script(&plan, "uninstall");

        let ops_start = start.find("'ops-control' 'start'").unwrap();
        let enable = start
            .find("Invoke-ExactKeeperTaskMutation -Mutation 'Enable'")
            .unwrap();
        let scheduled_start = start
            .find("Invoke-ExactKeeperTaskMutation -Mutation 'Start'")
            .unwrap();
        assert!(ops_start < enable && enable < scheduled_start);
        assert!(
            !start.contains("--live-control-token' $ResolvedLiveControlToken"),
            "developer-staging control must not forward a live token"
        );

        let ops_stop = stop.find("'ops-control' 'stop'").unwrap();
        let deadline = stop.find("$Deadline =").unwrap();
        let fallback_receipt = stop.find("$FallbackReceipt =").unwrap();
        let forced_stop = stop
            .find("Invoke-ExactKeeperTaskMutation -Mutation 'Stop' -AllowMissing")
            .unwrap();
        assert!(
            ops_stop < deadline && deadline < fallback_receipt && fallback_receipt < forced_stop
        );
        assert!(stop.contains("terminalMatchesPreRequestGeneration"));
        assert!(stop.contains("processTreeSurvivors"));
        assert!(stop.contains("Write-AtomicJson -Path $Plan.fallbackReceiptFile"));
        assert!(
            stop.find("Write-AtomicJson -Path $Plan.fallbackReceiptFile")
                .unwrap()
                < forced_stop
        );

        for script in [&install, &start, &stop, &uninstall] {
            assert!(script.contains("-ExecutionPolicy Bypass -File $Path @Arguments"));
        }

        let live_plan = write_windows_reconcile_keeper_plan(fixture.live_options()).unwrap();
        for name in ["install", "start", "stop", "uninstall", "rollback"] {
            let live_script = fixture.script(&live_plan, name);
            assert!(
                live_script.contains("AGENT_HARNESS_LIVE_CONTROL_TOKEN is required"),
                "live {name} must retain the environment-only token gate"
            );
            assert!(
                !live_script.contains("param([string] $LiveControlToken"),
                "live {name} must not accept a token through process argv"
            );
            assert!(
                !live_script.contains("-LiveControlToken"),
                "live {name} must not forward a token through PowerShell argv"
            );
            assert!(
                !live_script.contains("--live-control-token"),
                "live {name} must not forward a token through child argv"
            );
        }
        assert!(
            fixture
                .script(&live_plan, "install")
                .contains("Assert-KeeperLiveControlAuthorization -Action 'restart'")
        );
        assert!(
            fixture
                .script(&live_plan, "rollback")
                .contains("Assert-KeeperLiveControlAuthorization -Action 'restart'")
        );

        let live_uninstall = fixture.script(&live_plan, "uninstall");
        let authorize_uninstall = live_uninstall.find("'ops-cutover-status'").unwrap();
        let reuse_stop = live_uninstall
            .find("Invoke-KeeperScript -Path $StopScript")
            .unwrap();
        let unregister = live_uninstall
            .find("Invoke-ExactKeeperTaskMutation -Mutation 'Unregister' -AllowMissing")
            .unwrap();
        assert!(authorize_uninstall < reuse_stop && reuse_stop < unregister);
        assert!(
            live_uninstall.contains("Assert-KeeperLiveControlAuthorization -Action 'uninstall'")
        );
        assert!(live_uninstall.contains("stop-proof-latest.json"));

        fixture.remove();
    }

    #[test]
    fn readback_is_atomic_and_covers_task_action_settings_and_runtime() {
        let fixture = Fixture::new("readback_contract");
        let plan = write_windows_reconcile_keeper_plan(fixture.options()).unwrap();
        let readback = fixture.script(&plan, "readback");

        for required in [
            "Get-ScheduledTask -TaskPath $Plan.task.path -TaskName",
            "Get-ScheduledTaskInfo -TaskPath $Plan.task.path -TaskName",
            "schema = 'agent-harness.windows-reconcile-keeper-observation.v1'",
            "planDigest = $Plan.planDigest",
            "queryNonce = $QueryNonce",
            "principal = [ordered]@{",
            "observedUserId = $ObservedPrincipalUserId",
            "identityMatches = $PrincipalIdentityMatches",
            "$Actions = @()",
            "$Actions = @($Task.Actions)",
            "action = [ordered]@{",
            "count = $Actions.Count",
            "settings = [ordered]@{",
            "recoveryTriggerType",
            "recoveryStartBoundary",
            "recoveryInterval",
            "recoveryDuration",
            "recoveryStopAtDurationEnd",
            "runtime = [ordered]@{",
            "$null -ne $TaskInfo.NextRunTime",
            "keeperStateFile = $Plan.stateFile",
            "stopFileExists",
            "observedSha256",
            "[IO.File]::Replace",
            "$BackupPath",
            "[IO.File]::Move",
            "Write-AtomicJson -Path $Plan.observationFile",
        ] {
            assert!(
                readback.contains(required),
                "missing readback field: {required}"
            );
        }
        assert_eq!(
            plan.state_file,
            fixture
                .harness_home()
                .join("state/supervisor/reconcile-keeper.json")
        );
        assert_eq!(
            plan.observation_file,
            plan.output_dir.join("readback-latest.json")
        );

        fixture.remove();
    }

    #[cfg(windows)]
    #[test]
    fn account_identity_helper_accepts_task_scheduler_aliases_by_sid_only() {
        let fixture = Fixture::new("windows_account_identity");
        let script = fixture.root.join("account-identity.ps1");
        fs::write(
            &script,
            format!(
                r#"{}
$Expected = [Security.Principal.WindowsIdentity]::GetCurrent().Name
$Observed = [Environment]::UserName
$ExpectedSid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
if (-not (Test-KeeperAccountIdentityExact -ExpectedUserId $Expected -ObservedUserId $Observed)) {{ throw 'short Task Scheduler alias did not resolve to the current account SID' }}
if (-not (Test-KeeperAccountIdentityExact -ExpectedUserId $Expected -ObservedUserId $ExpectedSid)) {{ throw 'SID alias did not resolve to the current account' }}
if (Test-KeeperAccountIdentityExact -ExpectedUserId $Expected -ObservedUserId 'S-1-0-0') {{ throw 'foreign SID was accepted' }}
"#,
                account_identity_function_body()
            ),
        )
        .unwrap();
        let powershell = PathBuf::from(std::env::var_os("SystemRoot").unwrap())
            .join("System32/WindowsPowerShell/v1.0/powershell.exe");
        let output = Command::new(&powershell)
            .args([
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
            ])
            .arg(&script)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "account identity helper failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        fixture.remove();
    }

    #[cfg(windows)]
    #[test]
    fn task_boundary_helper_preserves_iso_text_after_windows_powershell_json_parse() {
        let fixture = Fixture::new("windows_task_boundary");
        let plan = write_windows_reconcile_keeper_plan(fixture.options()).unwrap();
        let script = fixture.root.join("task-boundary.ps1");
        fs::write(
            &script,
            format!(
                r#"$ErrorActionPreference = 'Stop'
$Plan = Get-Content -LiteralPath {} -Raw | ConvertFrom-Json
{}
$Actual = Get-KeeperRecoveryStartBoundaryText
if ($Actual -ne '{}') {{ throw "recovery boundary normalized to $Actual" }}
[xml]$TaskXml = New-KeeperTaskXml
$Namespace = New-Object Xml.XmlNamespaceManager($TaskXml.NameTable)
$Namespace.AddNamespace('t', 'http://schemas.microsoft.com/windows/2004/02/mit/task')
$XmlBoundary = $TaskXml.SelectSingleNode('/t:Task/t:Triggers/t:TimeTrigger/t:StartBoundary', $Namespace).InnerText
if ($XmlBoundary -ne '{}') {{ throw "task XML recovery boundary normalized to $XmlBoundary" }}
"#,
                ps_quote_path(&plan.plan_file),
                task_scheduler_guard_function_body(),
                KEEPER_RECOVERY_START_BOUNDARY,
                KEEPER_RECOVERY_START_BOUNDARY,
            ),
        )
        .unwrap();
        let powershell = PathBuf::from(std::env::var_os("SystemRoot").unwrap())
            .join("System32/WindowsPowerShell/v1.0/powershell.exe");
        let output = Command::new(&powershell)
            .args([
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
            ])
            .arg(&script)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "task recovery boundary normalization failed: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );

        fixture.remove();
    }

    #[cfg(windows)]
    #[test]
    fn generated_scripts_parse_and_readback_atomically_overwrites_on_windows() {
        let fixture = Fixture::new("windows_readback_execution");
        let plan = write_windows_reconcile_keeper_plan(fixture.options()).unwrap();
        let powershell = PathBuf::from(std::env::var_os("SystemRoot").unwrap())
            .join("System32/WindowsPowerShell/v1.0/powershell.exe");

        for script in &plan.scripts {
            let command = format!(
                "$tokens=$null;$errors=$null;[void][System.Management.Automation.Language.Parser]::ParseFile({},[ref]$tokens,[ref]$errors);if($errors.Count){{$errors|Out-String|Write-Error;exit 1}}",
                ps_quote_path(&script.path)
            );
            let output = Command::new(&powershell)
                .args([
                    "-NoLogo",
                    "-NoProfile",
                    "-NonInteractive",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-Command",
                    &command,
                ])
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{} failed PowerShell parse: {}{}",
                script.path.display(),
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }

        let readback = plan
            .scripts
            .iter()
            .find(|script| script.name == "readback")
            .unwrap();
        for attempt in 1..=2 {
            let output = Command::new(&powershell)
                .args([
                    "-NoLogo",
                    "-NoProfile",
                    "-NonInteractive",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-File",
                ])
                .arg(&readback.path)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "readback attempt {attempt} failed: {}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }

        let observation: serde_json::Value =
            serde_json::from_slice(&fs::read(&plan.observation_file).unwrap()).unwrap();
        assert_eq!(
            observation["schema"],
            WINDOWS_RECONCILE_KEEPER_OBSERVATION_SCHEMA
        );
        assert_eq!(observation["task"]["exists"], false);
        let atomic_debris = fs::read_dir(&plan.output_dir)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                name.contains(".tmp.") || name.contains(".bak.")
            })
            .count();
        assert_eq!(atomic_debris, 0);

        fixture.remove();
    }

    #[test]
    fn scheduler_root_uses_hash_bound_hidden_synchronous_launcher() {
        let fixture = Fixture::new("hidden_scheduler_root");
        let plan = write_windows_reconcile_keeper_plan(fixture.options()).unwrap();
        let launcher = fixture.script(&plan, "launch-hidden");
        let install = fixture.script(&plan, "install");

        assert_eq!(plan.scheduler_action.window_style, "hidden");
        assert!(plan.scheduler_action.child_create_no_window);
        assert!(plan.scheduler_action.synchronous_wait);
        assert!(
            plan.scheduler_action
                .argument_vector
                .windows(2)
                .any(|pair| pair == ["-WindowStyle", "Hidden"])
        );
        assert!(
            plan.scheduler_action
                .argument_vector
                .last()
                .is_some_and(|value| value.ends_with("launch-hidden.ps1"))
        );
        for required in [
            "$StartInfo.UseShellExecute = $false",
            "$StartInfo.CreateNoWindow = $true",
            "$StartInfo.WindowStyle = [Diagnostics.ProcessWindowStyle]::Hidden",
            "$Child.WaitForExit()",
            "$ChildExitCode = $Child.ExitCode",
            "exit $ChildExitCode",
        ] {
            assert!(
                launcher.contains(required),
                "hidden launcher is missing {required}"
            );
        }
        let integrity = launcher.find("\nAssert-KeeperPlanIntegrity\n").unwrap();
        let spawn = launcher.find("[Diagnostics.Process]::Start").unwrap();
        assert!(integrity < spawn);
        assert!(install.contains("$Plan.schedulerAction.executable"));
        assert!(install.contains("$Plan.schedulerAction.arguments"));
        assert!(install.contains("$Plan.schedulerAction.workingDirectory"));

        fixture.remove();
    }

    #[test]
    fn plan_digest_is_deterministic_and_excludes_only_its_own_field() {
        let fixture = Fixture::new("deterministic_digest");
        let first = write_windows_reconcile_keeper_plan(fixture.options()).unwrap();
        let second = write_windows_reconcile_keeper_plan(fixture.options()).unwrap();

        assert_eq!(first.plan_digest, second.plan_digest);
        assert_eq!(first.plan_digest, plan_digest(&first).unwrap());
        assert_eq!(first.plan_digest.len(), 64);
        let persisted: serde_json::Value =
            serde_json::from_slice(&fs::read(&first.plan_file).unwrap()).unwrap();
        assert_eq!(persisted["planDigest"], first.plan_digest);
        assert_eq!(persisted["executable"]["sha256"], first.executable.sha256);

        fixture.remove();
    }

    #[cfg(windows)]
    #[test]
    fn edited_mutation_script_fails_digest_check_before_any_effect() {
        let _guard = KEEPER_SCRIPT_ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let fixture = Fixture::new("script_digest_tamper");
        let plan = write_windows_reconcile_keeper_plan(fixture.options()).unwrap();
        let start = plan
            .scripts
            .iter()
            .find(|script| script.name == "start")
            .unwrap();
        let mut bytes = fs::read(&start.path).unwrap();
        bytes.extend_from_slice(b"\n# one-byte-equivalent tamper\n");
        fs::write(&start.path, bytes).unwrap();
        let effect_file = fixture.root.join("effect.json");
        let powershell = PathBuf::from(std::env::var_os("SystemRoot").unwrap())
            .join("System32/WindowsPowerShell/v1.0/powershell.exe");
        let output = Command::new(&powershell)
            .args([
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
            ])
            .arg(&start.path)
            .args(["-LiveControlToken", "fixture-token"])
            .env("AGENT_HARNESS_KEEPER_TEST_EFFECT_FILE", &effect_file)
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr)
                .contains("content digest does not match the current plan manifest")
        );
        assert!(!effect_file.exists());
        assert!(!plan.stop_file.exists());
        fixture.remove();
    }

    #[cfg(windows)]
    #[test]
    fn developer_control_scripts_reach_post_authorization_guards_without_live_token() {
        let _guard = KEEPER_SCRIPT_ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let fixture = Fixture::new("developer_tokenless_control");
        let plan = write_windows_reconcile_keeper_plan(fixture.options()).unwrap();
        let task_file = fixture.root.join("task.json");
        let ops_file = fixture.root.join("ops.json");
        let effect_file = fixture.root.join("effect.json");
        fs::write(
            &task_file,
            serde_json::to_vec(&exact_task_snapshot(&plan, "Ready", true)).unwrap(),
        )
        .unwrap();
        fs::write(
            &ops_file,
            serde_json::to_vec(&serde_json::json!({
                "start": {
                    "schema": "agent-harness.ops-control-receipt.v1",
                    "action": "start",
                    "status": "partial-start-ready",
                    "stopFiles": []
                },
                "stop": {
                    "schema": "agent-harness.ops-control-receipt.v1",
                    "action": "stop",
                    "status": "partial-stop-request",
                    "stopFiles": []
                }
            }))
            .unwrap(),
        )
        .unwrap();
        let powershell = PathBuf::from(std::env::var_os("SystemRoot").unwrap())
            .join("System32/WindowsPowerShell/v1.0/powershell.exe");

        for (script_name, post_authorization_guard) in [
            (
                "install",
                "same-deployment keeper upgrade requires an exact persistent global hold",
            ),
            ("start", "transactional keeper start rollback is partial"),
            (
                "stop",
                "ops-control stop did not produce exact stop-requested state",
            ),
            (
                "uninstall",
                "ops-control stop did not produce exact stop-requested state",
            ),
        ] {
            fs::remove_file(&effect_file).ok();
            let script = plan
                .scripts
                .iter()
                .find(|script| script.name == script_name)
                .unwrap();
            let output = Command::new(&powershell)
                .args([
                    "-NoLogo",
                    "-NoProfile",
                    "-NonInteractive",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-File",
                ])
                .arg(&script.path)
                .env(
                    "AGENT_HARNESS_KEEPER_TEST_TASK_OBSERVATION_FILE",
                    &task_file,
                )
                .env("AGENT_HARNESS_KEEPER_TEST_OPS_RECEIPT_FILE", &ops_file)
                .env("AGENT_HARNESS_KEEPER_TEST_EFFECT_FILE", &effect_file)
                .output()
                .unwrap();
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(
                !output.status.success(),
                "{script_name} unexpectedly passed its deliberate post-authorization guard"
            );
            assert!(
                stderr.contains(post_authorization_guard),
                "{script_name} did not reach its post-authorization guard: {stderr}"
            );
            assert!(
                !stderr.contains("live-control token is required"),
                "{script_name} incorrectly required a live token in developer staging: {stderr}"
            );
            if script_name == "start" {
                let effect: Value =
                    serde_json::from_slice(&fs::read(&effect_file).unwrap()).unwrap();
                assert_eq!(effect["action"], "disable-task");
                assert!(
                    effect["effects"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .all(|entry| entry["action"] == "disable-task"),
                    "partial start must only reach the fail-safe rollback disable seam"
                );
            } else {
                assert!(
                    !effect_file.exists(),
                    "{script_name} reached the scheduled-task mutation seam"
                );
            }
        }

        fixture.remove();
    }

    #[cfg(windows)]
    #[test]
    fn live_control_scripts_require_action_bound_token_before_mutation() {
        let _guard = KEEPER_SCRIPT_ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let fixture = Fixture::new("live_control_auth");
        let plan = write_windows_reconcile_keeper_plan(fixture.live_options()).unwrap();
        let powershell = PathBuf::from(std::env::var_os("SystemRoot").unwrap())
            .join("System32/WindowsPowerShell/v1.0/powershell.exe");

        for (script_name, action, mutation) in [
            ("install", "restart", "Register-ScheduledTask"),
            (
                "start",
                "start",
                "Invoke-ExactKeeperTaskMutation -Mutation 'Enable'",
            ),
            (
                "stop",
                "stop",
                "Invoke-ExactKeeperTaskMutation -Mutation 'Disable'",
            ),
            (
                "uninstall",
                "uninstall",
                "Invoke-ExactKeeperTaskMutation -Mutation 'Unregister'",
            ),
        ] {
            let script = plan
                .scripts
                .iter()
                .find(|script| script.name == script_name)
                .unwrap();
            let output = Command::new(&powershell)
                .args([
                    "-NoLogo",
                    "-NoProfile",
                    "-NonInteractive",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-File",
                ])
                .arg(&script.path)
                .output()
                .unwrap();
            assert!(!output.status.success());
            assert!(
                String::from_utf8_lossy(&output.stderr)
                    .contains("AGENT_HARNESS_LIVE_CONTROL_TOKEN is required"),
                "{script_name} did not fail closed on a missing live token"
            );

            let generated = fs::read_to_string(&script.path).unwrap();
            let action_check = generated
                .find("([string]$Authorization.action -eq $Action)")
                .unwrap();
            let action_call = generated
                .find(&format!(
                    "Assert-KeeperLiveControlAuthorization -Action '{action}'"
                ))
                .unwrap();
            let mutation = generated.rfind(mutation).unwrap();
            assert!(
                action_check < action_call && action_call < mutation,
                "{script_name} must validate the exact action before its first scheduler mutation"
            );
        }
        assert!(!plan.stop_file.exists());
        fixture.remove();
    }

    #[test]
    fn live_writer_promotes_a_complete_generation_and_retains_the_previous_one() {
        let fixture = Fixture::new("atomic_live_promotion");
        let options = fixture.live_options();
        let first = write_and_promote_windows_reconcile_keeper_plan(options.clone()).unwrap();
        let first_digest = first.plan_digest.clone();
        fs::write(options.harness_cli.as_path(), b"live-cli-v2").unwrap();
        let second = write_and_promote_windows_reconcile_keeper_plan(options).unwrap();
        assert_ne!(second.plan_digest, first_digest);
        verify_materialized_keeper_bundle(&second.output_dir, &second).unwrap();
        let previous = fs::read_dir(second.output_dir.parent().unwrap())
            .unwrap()
            .filter_map(Result::ok)
            .find(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".windows-reconcile-keeper.previous.")
            })
            .expect("previous canonical keeper generation must be retained");
        let previous_plan: Value =
            serde_json::from_slice(&fs::read(previous.path().join("keeper-plan.json")).unwrap())
                .unwrap();
        assert_eq!(
            previous_plan.get("planDigest").and_then(Value::as_str),
            Some(first_digest.as_str())
        );
        fixture.remove();
    }

    #[test]
    fn live_plan_rejects_non_deployment_executables_but_developer_staging_is_explicit() {
        let fixture = Fixture::new("strict_live_paths");
        let developer = write_windows_reconcile_keeper_plan(fixture.options()).unwrap();
        assert_eq!(developer.deployment_mode, "developer-staging");

        let mut live_options = fixture.live_options();
        let live = write_windows_reconcile_keeper_plan(live_options.clone()).unwrap();
        assert_eq!(live.deployment_mode, "live");
        assert!(
            validate_live_windows_reconcile_keeper_plan(&serde_json::to_value(&live).unwrap())
                .is_ok()
        );

        live_options.harness_cli = fixture.harness_cli.clone();
        let outside_cli = write_windows_reconcile_keeper_plan(live_options.clone()).unwrap_err();
        assert!(
            outside_cli
                .to_string()
                .contains("executable must be exactly")
        );

        live_options.harness_cli = fixture.live_harness_cli();
        live_options.node_executable = Some(fixture.node_executable.clone());
        let global_node = write_windows_reconcile_keeper_plan(live_options).unwrap_err();
        assert!(
            global_node
                .to_string()
                .contains("Node executable must be below")
        );
        fixture.remove();
    }

    #[test]
    fn existing_task_identity_fixture_rejects_foreign_task_without_mutation() {
        let fixture = Fixture::new("foreign_task_identity");
        let plan = write_windows_reconcile_keeper_plan(fixture.options()).unwrap();
        let plan_value = serde_json::to_value(&plan).unwrap();
        let mut observed = serde_json::json!({
            "task": {
                "observedPath": plan.task.path,
                "name": plan.task.name,
                "description": plan.task.description,
            },
            "principal": {"userId": plan.task.principal.user_id},
            "action": {
                "count": 1,
                "execute": plan.scheduler_action.executable,
                "arguments": plan.scheduler_action.arguments,
                "workingDirectory": plan.scheduler_action.working_directory,
            },
            "trigger": {
                "count": 2,
                "logonCount": 1,
                "recoveryCount": 1,
                "userId": plan.task.trigger.user_id,
                "triggerType": "MSFT_TaskLogonTrigger",
                "recoveryTriggerType": "MSFT_TaskTimeTrigger",
                "recoveryStartBoundary": plan.task.trigger.recovery_start_boundary,
                "recoveryInterval": plan.task.trigger.recovery_interval,
                "recoveryDuration": plan.task.trigger.recovery_duration,
                "recoveryStopAtDurationEnd": plan.task.trigger.recovery_stop_at_duration_end,
            },
        });
        assert!(keeper_task_identity_matches(&plan_value, &observed));
        observed["trigger"]["recoveryInterval"] = Value::String("PT2M".to_string());
        assert!(!keeper_task_identity_matches(&plan_value, &observed));
        observed["trigger"]["recoveryInterval"] =
            Value::String(plan.task.trigger.recovery_interval.to_string());
        observed["task"]["description"] = Value::String("foreign owner".to_string());
        assert!(!keeper_task_identity_matches(&plan_value, &observed));

        let install = fixture.script(&plan, "install");
        assert!(
            install.find("$ExactCurrent").unwrap()
                < install.find("Register-ScheduledTask -TaskPath").unwrap()
        );
        assert!(install.contains("refusing to overwrite foreign scheduled task"));
        fixture.remove();
    }

    #[cfg(windows)]
    #[test]
    fn keeper_scripts_reject_tampered_plan_digests_before_scheduler_mutation() {
        let _guard = KEEPER_SCRIPT_ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let powershell = PathBuf::from(std::env::var_os("SystemRoot").unwrap())
            .join("System32/WindowsPowerShell/v1.0/powershell.exe");

        for (name, tamper, expected_error) in [
            (
                "stale_plan_digest",
                "task",
                "keeper plan digest does not match canonical plan content",
            ),
            (
                "stale_action_digest",
                "action",
                "keeper action digest does not match canonical action content",
            ),
        ] {
            let fixture = Fixture::new(name);
            let plan = write_windows_reconcile_keeper_plan(fixture.options()).unwrap();
            let install = plan
                .scripts
                .iter()
                .find(|script| script.name == "install")
                .unwrap();
            let observation_file = fixture.root.join("task.json");
            let legacy_file = fixture.root.join("legacy.json");
            let effect_file = fixture.root.join("effect.json");
            let missing = missing_task_snapshot(&plan);
            let exact_disabled = exact_task_snapshot(&plan, "Disabled", false);
            fs::write(
                &observation_file,
                serde_json::to_vec(&serde_json::json!({
                    "sequence": [
                        missing.clone(),
                        missing,
                        exact_disabled.clone(),
                        exact_disabled
                    ]
                }))
                .unwrap(),
            )
            .unwrap();
            fs::write(&legacy_file, br#"{"tasks":[]}"#).unwrap();

            let mut persisted: Value =
                serde_json::from_slice(&fs::read(&plan.plan_file).unwrap()).unwrap();
            match tamper {
                "task" => {
                    persisted["task"]["settings"]["restartCount"] =
                        Value::Number(serde_json::Number::from(254));
                }
                "action" => {
                    persisted["action"]["arguments"] =
                        Value::String("tampered action arguments".to_string());
                    persisted["planDigest"] =
                        Value::String(windows_reconcile_keeper_plan_digest(&persisted).unwrap());
                }
                _ => unreachable!(),
            }
            fs::write(&plan.plan_file, serde_json::to_vec(&persisted).unwrap()).unwrap();

            let output = Command::new(&powershell)
                .args([
                    "-NoLogo",
                    "-NoProfile",
                    "-NonInteractive",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-File",
                ])
                .arg(&install.path)
                .env(
                    "AGENT_HARNESS_KEEPER_TEST_TASK_OBSERVATION_FILE",
                    &observation_file,
                )
                .env(
                    "AGENT_HARNESS_KEEPER_TEST_LEGACY_OBSERVATION_FILE",
                    &legacy_file,
                )
                .env("AGENT_HARNESS_KEEPER_TEST_EFFECT_FILE", &effect_file)
                .output()
                .unwrap();
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(
                !output.status.success(),
                "{name} unexpectedly reached a successful install"
            );
            assert!(
                stderr.contains(expected_error),
                "{name} did not fail at the plan-integrity gate: {stderr}"
            );
            assert!(
                !effect_file.exists(),
                "{name} reached the scheduled-task mutation seam"
            );
            fixture.remove();
        }
    }

    #[test]
    fn every_generated_keeper_script_validates_both_digests_before_scheduler_access() {
        let fixture = Fixture::new("all_script_integrity_order");
        let plan = write_windows_reconcile_keeper_plan(fixture.options()).unwrap();
        for script in &plan.scripts {
            let generated = fs::read_to_string(&script.path).unwrap();
            let action_digest = generated
                .find("Get-KeeperCanonicalJsonSha256 -Value $Plan.action")
                .unwrap_or_else(|| panic!("{} lacks action digest recomputation", script.name));
            let plan_digest = generated
                .find("Get-KeeperCanonicalJsonSha256 -Value $Plan -ExcludedProperty 'planDigest'")
                .unwrap_or_else(|| panic!("{} lacks plan digest recomputation", script.name));
            let integrity_call = generated
                .find("\nAssert-KeeperPlanIntegrity\n")
                .unwrap_or_else(|| panic!("{} lacks the integrity gate", script.name));
            assert!(
                action_digest < integrity_call && plan_digest < integrity_call,
                "{} invokes its integrity gate before defining both recomputations",
                script.name
            );

            let first_control_or_spawn_access = [
                "Get-ScheduledTask",
                "Get-ScheduledTaskInfo",
                "Register-ScheduledTask",
                "Unregister-ScheduledTask",
                "Enable-ScheduledTask",
                "Disable-ScheduledTask",
                "Start-ScheduledTask",
                "Stop-ScheduledTask",
                "Export-ScheduledTask",
                "[Diagnostics.Process]::Start",
            ]
            .iter()
            .filter_map(|marker| generated.find(marker))
            .min()
            .unwrap_or_else(|| panic!("{} has no scheduler or spawn seam", script.name));
            assert!(
                integrity_call < first_control_or_spawn_access,
                "{} accesses Task Scheduler or spawns before validating the plan",
                script.name
            );
        }
        fixture.remove();
    }

    #[cfg(windows)]
    #[test]
    fn keeper_generator_rejects_preexisting_state_junction_before_any_write() {
        let fixture = Fixture::new("generator_state_junction");
        let external_state = fixture.root.join("external-state");
        let state_junction = fixture.harness_home.join("state");
        fs::create_dir_all(&external_state).unwrap();
        create_directory_junction(&state_junction, &external_state);

        let result = write_windows_reconcile_keeper_plan(fixture.options());
        let escaped_output = external_state
            .join("supervisor/windows-reconcile-keeper")
            .exists();
        let error = result.err().map(|error| error.to_string());

        fs::remove_dir(&state_junction).unwrap();
        fixture.remove();

        assert!(
            error
                .as_deref()
                .is_some_and(|message| message.contains("physical containment")),
            "pre-existing state junction was not rejected: {error:?}"
        );
        assert!(
            !escaped_output,
            "keeper output escaped through the pre-existing state junction"
        );
    }

    #[cfg(windows)]
    #[test]
    fn keeper_script_rejects_post_generation_state_junction_before_any_effect() {
        let _guard = KEEPER_SCRIPT_ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let fixture = Fixture::new("runtime_state_junction");
        let plan = write_windows_reconcile_keeper_plan(fixture.options()).unwrap();
        let state_path = fixture.harness_home.join("state");
        let relocated_state = fixture.root.join("relocated-state");
        fs::rename(&state_path, &relocated_state).unwrap();
        create_directory_junction(&state_path, &relocated_state);

        let observation_file = fixture.root.join("task.json");
        let legacy_file = fixture.root.join("legacy.json");
        let effect_file = fixture.root.join("effect.json");
        let missing = missing_task_snapshot(&plan);
        let exact_disabled = exact_task_snapshot(&plan, "Disabled", false);
        fs::write(
            &observation_file,
            serde_json::to_vec(&serde_json::json!({
                "sequence": [
                    missing.clone(),
                    missing,
                    exact_disabled.clone(),
                    exact_disabled
                ]
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(&legacy_file, br#"{"tasks":[]}"#).unwrap();

        let install = plan
            .scripts
            .iter()
            .find(|script| script.name == "install")
            .unwrap();
        let powershell = PathBuf::from(std::env::var_os("SystemRoot").unwrap())
            .join("System32/WindowsPowerShell/v1.0/powershell.exe");
        let output = Command::new(&powershell)
            .args([
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
            ])
            .arg(&install.path)
            .env(
                "AGENT_HARNESS_KEEPER_TEST_TASK_OBSERVATION_FILE",
                &observation_file,
            )
            .env(
                "AGENT_HARNESS_KEEPER_TEST_LEGACY_OBSERVATION_FILE",
                &legacy_file,
            )
            .env("AGENT_HARNESS_KEEPER_TEST_EFFECT_FILE", &effect_file)
            .output()
            .unwrap();
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        let reached_effect = effect_file.exists();

        fs::remove_dir(&state_path).unwrap();
        fs::rename(&relocated_state, &state_path).unwrap();
        fixture.remove();

        assert!(
            !output.status.success(),
            "runtime state junction unexpectedly reached a successful install"
        );
        assert!(
            stderr.contains("physical containment"),
            "runtime state junction did not fail at the physical containment gate: {stderr}"
        );
        assert!(
            !reached_effect,
            "runtime state junction reached the scheduled-task mutation seam"
        );
    }

    #[cfg(windows)]
    #[test]
    fn developer_install_script_refuses_foreign_task_without_scheduler_mutation() {
        let _guard = KEEPER_SCRIPT_ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let fixture = Fixture::new("foreign_task_install_script");
        let plan = write_windows_reconcile_keeper_plan(fixture.options()).unwrap();
        let observation_file = fixture.root.join("observed-task.json");
        let effect_file = fixture.root.join("task-effect.json");
        let auth_file = fixture.root.join("auth.json");
        let legacy_file = fixture.root.join("legacy.json");
        let mut foreign = exact_task_snapshot(&plan, "Disabled", false);
        foreign["task"]["description"] = Value::String("foreign owner".to_string());
        fs::write(&observation_file, serde_json::to_vec(&foreign).unwrap()).unwrap();
        fs::write(
            &auth_file,
            serde_json::to_vec(&authorization_receipt(&plan, "cutover")).unwrap(),
        )
        .unwrap();
        fs::write(&legacy_file, br#"{"tasks":[]}"#).unwrap();
        let powershell = PathBuf::from(std::env::var_os("SystemRoot").unwrap())
            .join("System32/WindowsPowerShell/v1.0/powershell.exe");
        let install = plan
            .scripts
            .iter()
            .find(|script| script.name == "install")
            .unwrap();
        let output = Command::new(&powershell)
            .args([
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
            ])
            .arg(&install.path)
            .args(["-LiveControlToken", "fixture-token"])
            .env(
                "AGENT_HARNESS_KEEPER_TEST_TASK_OBSERVATION_FILE",
                &observation_file,
            )
            .env("AGENT_HARNESS_KEEPER_TEST_AUTH_RECEIPT_FILE", &auth_file)
            .env(
                "AGENT_HARNESS_KEEPER_TEST_LEGACY_OBSERVATION_FILE",
                &legacy_file,
            )
            .env("AGENT_HARNESS_KEEPER_TEST_EFFECT_FILE", &effect_file)
            .output()
            .unwrap();

        assert!(
            !output.status.success(),
            "foreign task was accepted: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            String::from_utf8_lossy(&output.stderr)
                .contains("refusing to overwrite foreign scheduled task")
        );
        assert!(
            !effect_file.exists(),
            "fixture effect proves the script reached registration"
        );
        fixture.remove();
    }

    #[cfg(windows)]
    #[test]
    fn developer_install_is_disabled_first_idempotent_and_restores_stale_owner_on_failure() {
        let _guard = KEEPER_SCRIPT_ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let fixture = Fixture::new("install_transactions");
        let plan = write_windows_reconcile_keeper_plan(fixture.options()).unwrap();
        let install = plan
            .scripts
            .iter()
            .find(|script| script.name == "install")
            .unwrap();
        let observation_file = fixture.root.join("task.json");
        let effect_file = fixture.root.join("effect.json");
        let auth_file = fixture.root.join("auth.json");
        let legacy_file = fixture.root.join("legacy.json");
        fs::write(
            &auth_file,
            serde_json::to_vec(&authorization_receipt(&plan, "cutover")).unwrap(),
        )
        .unwrap();
        fs::write(&legacy_file, br#"{"tasks":[]}"#).unwrap();
        let powershell = PathBuf::from(std::env::var_os("SystemRoot").unwrap())
            .join("System32/WindowsPowerShell/v1.0/powershell.exe");
        let run_install = |observation: &Value| {
            fs::write(&observation_file, serde_json::to_vec(observation).unwrap()).unwrap();
            Command::new(&powershell)
                .args([
                    "-NoLogo",
                    "-NoProfile",
                    "-NonInteractive",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-File",
                ])
                .arg(&install.path)
                .args(["-LiveControlToken", "fixture-token"])
                .env(
                    "AGENT_HARNESS_KEEPER_TEST_TASK_OBSERVATION_FILE",
                    &observation_file,
                )
                .env("AGENT_HARNESS_KEEPER_TEST_EFFECT_FILE", &effect_file)
                .env("AGENT_HARNESS_KEEPER_TEST_AUTH_RECEIPT_FILE", &auth_file)
                .env(
                    "AGENT_HARNESS_KEEPER_TEST_LEGACY_OBSERVATION_FILE",
                    &legacy_file,
                )
                .output()
                .unwrap()
        };

        let exact_disabled = exact_task_snapshot(&plan, "Disabled", false);
        let missing = missing_task_snapshot(&plan);
        let output = run_install(&serde_json::json!({
            "sequence": [
                missing.clone(),
                missing.clone(),
                exact_disabled.clone(),
                exact_disabled.clone()
            ]
        }));
        assert!(
            output.status.success(),
            "missing-task install failed: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let effect: Value = serde_json::from_slice(&fs::read(&effect_file).unwrap()).unwrap();
        assert_eq!(effect["action"], "register-disabled-task");
        assert!(
            !fixture
                .script(&plan, "install")
                .contains("Register-ScheduledTask -TaskPath $Plan.task.path -TaskName $Plan.task.name -Xml $TaskXml -Force"),
            "disabled-first registration must not force-overwrite a task"
        );

        fs::remove_file(&effect_file).ok();
        let output = run_install(&exact_task_snapshot(&plan, "Disabled", false));
        assert!(
            output.status.success(),
            "idempotent install failed: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let effect: Value = serde_json::from_slice(&fs::read(&effect_file).unwrap()).unwrap();
        assert_eq!(effect["action"], "install-idempotent");

        crate::write_json_atomic(
            &plan.stop_file,
            &serde_json::json!({
                "schema": "agent-harness.supervisor-stop-file.v1",
                "serviceId": "supervisor-reconcile",
                "persistent": true
            }),
        )
        .unwrap();
        let mut stale = exact_task_snapshot(&plan, "Disabled", false);
        stale["task"]["description"] = Value::String(format!(
            "Agent Harness reconcile keeper; taskPath=\\; ownership=agent-harness.windows-reconcile-keeper:{}:stale-action",
            plan.deployment_identity
        ));
        stale["exportedXml"] = Value::String(
            r#"<?xml version="1.0"?><Task xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task"><Settings><Enabled>false</Enabled></Settings></Task>"#
                .to_string(),
        );
        let mut exact_enabled = exact_task_snapshot(&plan, "Ready", true);
        exact_enabled["exportedXml"] = stale["exportedXml"].clone();
        fs::remove_file(&effect_file).ok();
        let output = run_install(&serde_json::json!({
            "sequence": [
                stale.clone(),
                stale,
                missing.clone(),
                exact_enabled.clone(),
                exact_enabled.clone(),
                exact_enabled,
                missing
            ]
        }));
        assert!(
            !output.status.success(),
            "fixture should inject a post-registration disabled-state failure"
        );
        let effect: Value = serde_json::from_slice(&fs::read(&effect_file).unwrap()).unwrap();
        assert_eq!(effect["action"], "restore-prior-disabled-task");
        let receipt: Value = serde_json::from_slice(
            &fs::read(plan.output_dir.join("install-upgrade-rollback-latest.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(receipt["status"], "prior-definition-restored-disabled");

        fixture.remove();
    }

    #[cfg(windows)]
    #[test]
    fn legacy_preflight_blocks_active_or_ambiguous_tasks_and_validates_immutable_manifest() {
        let _guard = KEEPER_SCRIPT_ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let fixture = Fixture::new("legacy_preflight");
        let plan = write_windows_reconcile_keeper_plan(fixture.options()).unwrap();
        let legacy_root = plan
            .harness_home
            .join("state/supervisor/windows-scheduled-tasks");
        let runner = legacy_root.join("scripts/runtime-loop.ps1");
        fs::create_dir_all(runner.parent().unwrap()).unwrap();
        fs::write(&runner, b"# exact legacy runner").unwrap();
        let legacy_plan_file = legacy_root.join("supervisor-plan.json");
        crate::write_json_atomic(
            &legacy_plan_file,
            &serde_json::json!({
                "schema": "agent-harness.windows-supervisor-plan.v1",
                "harnessHome": plan.harness_home,
                "tasks": [{"name": "Legacy-Runtime", "runnerScript": runner}]
            }),
        )
        .unwrap();
        let observation_file = fixture.root.join("task.json");
        let auth_file = fixture.root.join("auth.json");
        let legacy_file = fixture.root.join("legacy.json");
        let effect_file = fixture.root.join("effect.json");
        fs::write(
            &observation_file,
            serde_json::to_vec(&exact_task_snapshot(&plan, "Disabled", false)).unwrap(),
        )
        .unwrap();
        fs::write(
            &auth_file,
            serde_json::to_vec(&authorization_receipt(&plan, "cutover")).unwrap(),
        )
        .unwrap();
        let install = plan
            .scripts
            .iter()
            .find(|script| script.name == "install")
            .unwrap();
        let powershell = PathBuf::from(std::env::var_os("SystemRoot").unwrap())
            .join("System32/WindowsPowerShell/v1.0/powershell.exe");
        let run_install = |legacy: &Value| {
            fs::write(&legacy_file, serde_json::to_vec(legacy).unwrap()).unwrap();
            Command::new(&powershell)
                .args([
                    "-NoLogo",
                    "-NoProfile",
                    "-NonInteractive",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-File",
                ])
                .arg(&install.path)
                .args(["-LiveControlToken", "fixture-token"])
                .env(
                    "AGENT_HARNESS_KEEPER_TEST_TASK_OBSERVATION_FILE",
                    &observation_file,
                )
                .env("AGENT_HARNESS_KEEPER_TEST_EFFECT_FILE", &effect_file)
                .env("AGENT_HARNESS_KEEPER_TEST_AUTH_RECEIPT_FILE", &auth_file)
                .env(
                    "AGENT_HARNESS_KEEPER_TEST_LEGACY_OBSERVATION_FILE",
                    &legacy_file,
                )
                .output()
                .unwrap()
        };
        let disabled = serde_json::json!({
            "tasks": [{
                "taskPath": "\\",
                "name": "Legacy-Runtime",
                "exists": true,
                "exactOwned": true,
                "state": "Disabled",
                "enabled": false,
                "exportedXml": "<?xml version=\"1.0\"?><Task xmlns=\"http://schemas.microsoft.com/windows/2004/02/mit/task\"><Settings><Enabled>false</Enabled></Settings></Task>"
            }]
        });
        let mut active = disabled.clone();
        active["tasks"][0]["state"] = Value::String("Ready".to_string());
        active["tasks"][0]["enabled"] = Value::Bool(true);
        let output = run_install(&active);
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("enabled or running"));
        assert!(!effect_file.exists());

        let mut different_path = disabled.clone();
        different_path["tasks"][0]["taskPath"] = Value::String("\\Foreign\\".to_string());
        let output = run_install(&different_path);
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("foreign, duplicate, or non-root")
        );

        let mut duplicate = disabled.clone();
        duplicate["tasks"]
            .as_array_mut()
            .unwrap()
            .push(disabled["tasks"][0].clone());
        let output = run_install(&duplicate);
        assert!(!output.status.success());

        let output = run_install(&disabled);
        assert!(
            output.status.success(),
            "disabled exact legacy task should be retained: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let manifest_file = plan.output_dir.join("legacy-task-migration.json");
        let mut manifest: Value =
            serde_json::from_slice(&fs::read(&manifest_file).unwrap()).unwrap();
        assert_eq!(manifest["tasks"][0]["originallyMissing"], false);
        assert_eq!(manifest["tasks"][0]["originalEnabled"], true);

        fs::write(&legacy_file, serde_json::to_vec(&active).unwrap()).unwrap();
        fs::write(
            &auth_file,
            serde_json::to_vec(&authorization_receipt(&plan, "start")).unwrap(),
        )
        .unwrap();
        let ops_file = fixture.root.join("ops.json");
        fs::write(
            &ops_file,
            serde_json::to_vec(&serde_json::json!({
                "schema": "agent-harness.ops-control-receipt.v1",
                "action": "start",
                "status": "start-ready",
                "stopFiles": [{
                    "component": "supervisor-reconcile",
                    "stopFile": plan.stop_file,
                    "present": false
                }]
            }))
            .unwrap(),
        )
        .unwrap();
        fs::remove_file(&effect_file).ok();
        let start = plan
            .scripts
            .iter()
            .find(|script| script.name == "start")
            .unwrap();
        let output = Command::new(&powershell)
            .args([
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
            ])
            .arg(&start.path)
            .args(["-LiveControlToken", "fixture-token"])
            .env(
                "AGENT_HARNESS_KEEPER_TEST_TASK_OBSERVATION_FILE",
                &observation_file,
            )
            .env("AGENT_HARNESS_KEEPER_TEST_EFFECT_FILE", &effect_file)
            .env("AGENT_HARNESS_KEEPER_TEST_AUTH_RECEIPT_FILE", &auth_file)
            .env(
                "AGENT_HARNESS_KEEPER_TEST_LEGACY_OBSERVATION_FILE",
                &legacy_file,
            )
            .env("AGENT_HARNESS_KEEPER_TEST_OPS_RECEIPT_FILE", &ops_file)
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("enabled or running"),
            "external re-enable after install must block start before hold clear"
        );
        assert!(!effect_file.exists());

        fs::write(
            &auth_file,
            serde_json::to_vec(&authorization_receipt(&plan, "cutover")).unwrap(),
        )
        .unwrap();
        let original_manifest = manifest.clone();
        manifest["tasks"][0]["name"] = Value::String("Substituted-Legacy".to_string());
        crate::write_json_atomic(&manifest_file, &manifest).unwrap();
        let output = run_install(&disabled);
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr)
                .contains("task identities do not exactly match")
        );

        manifest = original_manifest;
        manifest["deploymentIdentity"] = Value::String("corrupt".to_string());
        crate::write_json_atomic(&manifest_file, &manifest).unwrap();
        let output = run_install(&disabled);
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("immutable deployment"));

        fs::remove_file(&manifest_file).unwrap();
        let output = run_install(&serde_json::json!({"tasks": []}));
        assert!(
            output.status.success(),
            "an originally missing planned legacy task should remain absent"
        );
        let missing_manifest: Value =
            serde_json::from_slice(&fs::read(&manifest_file).unwrap()).unwrap();
        assert_eq!(missing_manifest["tasks"][0]["originallyMissing"], true);

        fs::remove_file(&legacy_plan_file).unwrap();
        let output = run_install(&serde_json::json!({
            "tasks": [{
                "taskPath": "\\",
                "name": "Orphan-Legacy",
                "exists": true,
                "exactOwned": false,
                "state": "Disabled",
                "enabled": false
            }]
        }));
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("legacy scheduler actions remain")
        );
        fixture.remove();
    }

    #[cfg(windows)]
    #[test]
    fn developer_script_seam_fails_closed_on_partial_ops_receipts() {
        let _guard = KEEPER_SCRIPT_ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let fixture = Fixture::new("partial_ops_receipts");
        let plan = write_windows_reconcile_keeper_plan(fixture.options()).unwrap();
        let receipt_file = fixture.root.join("ops-receipt.json");
        let effect_file = fixture.root.join("task-effect.json");
        let task_file = fixture.root.join("task.json");
        let auth_file = fixture.root.join("auth.json");
        let legacy_file = fixture.root.join("legacy.json");
        let stop_proof_file = fixture.root.join("stop-proof.json");
        let rollback_owners_file = fixture.root.join("rollback-owners.json");
        let powershell = PathBuf::from(std::env::var_os("SystemRoot").unwrap())
            .join("System32/WindowsPowerShell/v1.0/powershell.exe");
        fs::write(
            &task_file,
            serde_json::to_vec(&exact_task_snapshot(&plan, "Ready", true)).unwrap(),
        )
        .unwrap();
        fs::write(
            &auth_file,
            serde_json::to_vec(&serde_json::json!({
                "start": authorization_receipt(&plan, "start"),
                "stop": authorization_receipt(&plan, "stop")
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(&legacy_file, br#"{"tasks":[]}"#).unwrap();
        fs::write(
            &stop_proof_file,
            br#"{"keeperTerminalMatches":true,"keeperPidAlive":false,"ownerDispositions":[],"deploymentProcesses":[],"processTreeSurvivors":[]}"#,
        )
        .unwrap();
        fs::write(
            &rollback_owners_file,
            br#"{"owners":[],"deploymentProcesses":[],"processTreeSurvivors":[]}"#,
        )
        .unwrap();
        unsafe {
            std::env::set_var("AGENT_HARNESS_KEEPER_TEST_OPS_RECEIPT_FILE", &receipt_file);
            std::env::set_var("AGENT_HARNESS_KEEPER_TEST_EFFECT_FILE", &effect_file);
            std::env::set_var(
                "AGENT_HARNESS_KEEPER_TEST_TASK_OBSERVATION_FILE",
                &task_file,
            );
            std::env::set_var("AGENT_HARNESS_KEEPER_TEST_AUTH_RECEIPT_FILE", &auth_file);
            std::env::set_var(
                "AGENT_HARNESS_KEEPER_TEST_LEGACY_OBSERVATION_FILE",
                &legacy_file,
            );
            std::env::set_var(
                "AGENT_HARNESS_KEEPER_TEST_STOP_PROOF_FILE",
                &stop_proof_file,
            );
            std::env::set_var(
                "AGENT_HARNESS_KEEPER_TEST_ROLLBACK_OWNERS_FILE",
                &rollback_owners_file,
            );
        }

        for (script_name, action, status) in [
            ("start", "start", "partial-start-ready"),
            ("stop", "stop", "partial-stop-request"),
        ] {
            fs::remove_file(&plan.stop_file).ok();
            fs::remove_file(&effect_file).ok();
            let task_observation = if script_name == "start" {
                exact_task_snapshot(&plan, "Disabled", false)
            } else {
                exact_task_snapshot(&plan, "Ready", true)
            };
            fs::write(&task_file, serde_json::to_vec(&task_observation).unwrap()).unwrap();
            fs::write(
                &receipt_file,
                serde_json::to_vec(&serde_json::json!({
                    "schema": "agent-harness.ops-control-receipt.v1",
                    "action": action,
                    "status": status,
                    "stopFiles": [{
                        "component": "supervisor-reconcile",
                        "stopFile": plan.stop_file,
                        "present": false
                    }]
                }))
                .unwrap(),
            )
            .unwrap();
            let script = plan
                .scripts
                .iter()
                .find(|script| script.name == script_name)
                .unwrap();
            let output = Command::new(&powershell)
                .args([
                    "-NoLogo",
                    "-NoProfile",
                    "-NonInteractive",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-File",
                ])
                .arg(&script.path)
                .args(["-LiveControlToken", "non-live-fixture-token"])
                .output()
                .unwrap();
            assert!(
                !output.status.success(),
                "{script_name} unexpectedly accepted {status}: {}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(
                plan.stop_file.is_file(),
                "{script_name} did not preserve/recreate the global hold"
            );
            assert!(
                !effect_file.exists(),
                "{script_name} reached the scheduled-task mutation seam"
            );
        }

        for (script_name, action, status, stop_present, expected_effect) in [
            ("start", "start", "start-ready", false, "start-task"),
            ("stop", "stop", "stop-requested", true, "disable-task"),
        ] {
            fs::remove_file(&plan.stop_file).ok();
            fs::remove_file(&effect_file).ok();
            let task_observation = if script_name == "stop" {
                serde_json::json!({
                    "sequence": [
                        exact_task_snapshot(&plan, "Ready", true),
                        exact_task_snapshot(&plan, "Ready", true),
                        exact_task_snapshot(&plan, "Disabled", false)
                    ]
                })
            } else {
                exact_task_snapshot(&plan, "Disabled", false)
            };
            fs::write(&task_file, serde_json::to_vec(&task_observation).unwrap()).unwrap();
            if stop_present {
                fs::create_dir_all(plan.stop_file.parent().unwrap()).unwrap();
                crate::write_json_atomic(
                    &plan.stop_file,
                    &serde_json::json!({
                        "schema": "agent-harness.supervisor-stop-file.v1",
                        "serviceId": "supervisor-reconcile",
                        "persistent": true
                    }),
                )
                .unwrap();
            }
            fs::write(
                &receipt_file,
                serde_json::to_vec(&serde_json::json!({
                    "schema": "agent-harness.ops-control-receipt.v1",
                    "action": action,
                    "status": status,
                    "stopFiles": [{
                        "component": "supervisor-reconcile",
                        "stopFile": plan.stop_file,
                        "present": stop_present
                    }]
                }))
                .unwrap(),
            )
            .unwrap();
            let script = plan
                .scripts
                .iter()
                .find(|script| script.name == script_name)
                .unwrap();
            let output = Command::new(&powershell)
                .args([
                    "-NoLogo",
                    "-NoProfile",
                    "-NonInteractive",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-File",
                ])
                .arg(&script.path)
                .args(["-LiveControlToken", "non-live-fixture-token"])
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{script_name} rejected exact {status}: {}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            let effect: Value = serde_json::from_slice(&fs::read(&effect_file).unwrap()).unwrap();
            assert_eq!(effect["action"], expected_effect);
        }

        fs::write(
            &task_file,
            serde_json::to_vec(&serde_json::json!({
                "sequence": [
                    exact_task_snapshot(&plan, "Ready", true),
                    exact_task_snapshot(&plan, "Ready", true),
                    exact_task_snapshot(&plan, "Disabled", false)
                ]
            }))
            .unwrap(),
        )
        .unwrap();
        crate::write_json_atomic(
            &plan.state_file,
            &serde_json::json!({
                "schema": KEEPER_STATE_SCHEMA,
                "planDigest": plan.plan_digest,
                "actionDigest": plan.action.digest,
                "taskName": plan.task.name,
                "generationId": "already-stopped-generation",
                "pid": 2_000_000_000u32,
                "processStartTimeMs": 1,
                "lastHeartbeatAtMs": 1,
                "status": "stopped"
            }),
        )
        .unwrap();
        fs::remove_file(&effect_file).ok();
        let stop = plan
            .scripts
            .iter()
            .find(|script| script.name == "stop")
            .unwrap();
        let output = Command::new(&powershell)
            .args([
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
            ])
            .arg(&stop.path)
            .args([
                "-LiveControlToken",
                "non-live-fixture-token",
                "-TimeoutSeconds",
                "1",
            ])
            .env_remove("AGENT_HARNESS_KEEPER_TEST_STOP_PROOF_FILE")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "stop rejected an empty crash-path process tree: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let stop_proof: Value = serde_json::from_slice(
            &fs::read(plan.output_dir.join("stop-proof-latest.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(stop_proof["status"], "stopped");
        assert_eq!(stop_proof["initialProcessTree"], serde_json::json!([]));
        assert_eq!(stop_proof["processTreeSurvivors"], serde_json::json!([]));
        assert_eq!(stop_proof["postRequestTerminalObserved"], false);
        assert_eq!(stop_proof["idempotentExternalQuiescence"], true);
        assert_eq!(stop_proof["fallbackRequested"], false);

        fs::remove_file(&plan.state_file).unwrap();
        fs::write(
            &task_file,
            serde_json::to_vec(&serde_json::json!({
                "sequence": [
                    exact_task_snapshot(&plan, "Ready", true),
                    exact_task_snapshot(&plan, "Ready", true),
                    exact_task_snapshot(&plan, "Disabled", false)
                ]
            }))
            .unwrap(),
        )
        .unwrap();
        fs::remove_file(&effect_file).ok();
        let output = Command::new(&powershell)
            .args([
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
            ])
            .arg(&stop.path)
            .args([
                "-LiveControlToken",
                "non-live-fixture-token",
                "-TimeoutSeconds",
                "1",
            ])
            .env_remove("AGENT_HARNESS_KEEPER_TEST_STOP_PROOF_FILE")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "stop rejected externally quiescent missing-state path: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let stop_proof: Value = serde_json::from_slice(
            &fs::read(plan.output_dir.join("stop-proof-latest.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(stop_proof["postRequestTerminalObserved"], false);
        assert_eq!(stop_proof["idempotentExternalQuiescence"], true);
        assert_eq!(stop_proof["fallbackRequested"], false);

        unsafe {
            for name in [
                "AGENT_HARNESS_KEEPER_TEST_OPS_RECEIPT_FILE",
                "AGENT_HARNESS_KEEPER_TEST_EFFECT_FILE",
                "AGENT_HARNESS_KEEPER_TEST_TASK_OBSERVATION_FILE",
                "AGENT_HARNESS_KEEPER_TEST_AUTH_RECEIPT_FILE",
                "AGENT_HARNESS_KEEPER_TEST_LEGACY_OBSERVATION_FILE",
                "AGENT_HARNESS_KEEPER_TEST_STOP_PROOF_FILE",
                "AGENT_HARNESS_KEEPER_TEST_ROLLBACK_OWNERS_FILE",
            ] {
                std::env::remove_var(name);
            }
        }
        fixture.remove();
    }

    #[cfg(windows)]
    #[test]
    fn transactional_start_failure_reholds_stops_and_disables_exact_task() {
        let _guard = KEEPER_SCRIPT_ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let fixture = Fixture::new("start_rollback");
        let plan = write_windows_reconcile_keeper_plan(fixture.options()).unwrap();
        let task_file = fixture.root.join("task.json");
        let auth_file = fixture.root.join("auth.json");
        let legacy_file = fixture.root.join("legacy.json");
        let ops_file = fixture.root.join("ops.json");
        let owners_file = fixture.root.join("owners.json");
        let effect_file = fixture.root.join("effect.json");
        fs::write(
            &task_file,
            serde_json::to_vec(&exact_task_snapshot(&plan, "Ready", true)).unwrap(),
        )
        .unwrap();
        fs::write(
            &auth_file,
            serde_json::to_vec(&authorization_receipt(&plan, "start")).unwrap(),
        )
        .unwrap();
        fs::write(&legacy_file, br#"{"tasks":[]}"#).unwrap();
        fs::write(
            &ops_file,
            serde_json::to_vec(&serde_json::json!({
                "start": {
                    "schema": "agent-harness.ops-control-receipt.v1",
                    "action": "start",
                    "status": "start-ready",
                    "stopFiles": [{
                        "component": "supervisor-reconcile",
                        "stopFile": plan.stop_file,
                        "present": false
                    }]
                },
                "rollbackStop": {
                    "schema": "agent-harness.ops-control-receipt.v1",
                    "action": "stop",
                    "status": "stop-requested",
                    "stopFiles": [{
                        "component": "supervisor-reconcile",
                        "stopFile": plan.stop_file,
                        "present": true
                    }]
                }
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            &owners_file,
            br#"{"owners":[],"deploymentProcesses":[],"processTreeSurvivors":[]}"#,
        )
        .unwrap();
        let start = plan
            .scripts
            .iter()
            .find(|script| script.name == "start")
            .unwrap();
        let powershell = PathBuf::from(std::env::var_os("SystemRoot").unwrap())
            .join("System32/WindowsPowerShell/v1.0/powershell.exe");
        let output = Command::new(&powershell)
            .args([
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
            ])
            .arg(&start.path)
            .args(["-LiveControlToken", "fixture-token"])
            .env(
                "AGENT_HARNESS_KEEPER_TEST_TASK_OBSERVATION_FILE",
                &task_file,
            )
            .env("AGENT_HARNESS_KEEPER_TEST_EFFECT_FILE", &effect_file)
            .env("AGENT_HARNESS_KEEPER_TEST_AUTH_RECEIPT_FILE", &auth_file)
            .env(
                "AGENT_HARNESS_KEEPER_TEST_LEGACY_OBSERVATION_FILE",
                &legacy_file,
            )
            .env("AGENT_HARNESS_KEEPER_TEST_OPS_RECEIPT_FILE", &ops_file)
            .env(
                "AGENT_HARNESS_KEEPER_TEST_ROLLBACK_OWNERS_FILE",
                &owners_file,
            )
            .env(
                "AGENT_HARNESS_KEEPER_TEST_START_POST_MUTATION_FAILURE",
                "injected-after-start",
            )
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(plan.stop_file.is_file());
        let effect: Value = serde_json::from_slice(&fs::read(&effect_file).unwrap()).unwrap();
        let actions = effect["effects"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|effect| effect["action"].as_str())
            .collect::<Vec<_>>();
        assert_eq!(actions, ["enable-task", "start-task", "disable-task"]);
        let receipt: Value = serde_json::from_slice(
            &fs::read(plan.output_dir.join("start-rollback-latest.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(receipt["status"], "rolled-back");
        assert_eq!(receipt["processTreeSurvivors"], serde_json::json!([]));
        fixture.remove();
    }

    #[cfg(windows)]
    #[test]
    fn stop_foreign_replacement_and_stale_terminal_proof_fail_closed() {
        let _guard = KEEPER_SCRIPT_ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let fixture = Fixture::new("stop_fail_closed");
        let plan = write_windows_reconcile_keeper_plan(fixture.options()).unwrap();
        let task_file = fixture.root.join("task.json");
        let auth_file = fixture.root.join("auth.json");
        let ops_file = fixture.root.join("ops.json");
        let proof_file = fixture.root.join("proof.json");
        let effect_file = fixture.root.join("effect.json");
        let exact = exact_task_snapshot(&plan, "Running", true);
        let mut foreign = exact.clone();
        foreign["task"]["path"] = Value::String("\\Foreign\\".to_string());
        fs::write(
            &task_file,
            serde_json::to_vec(&serde_json::json!({
                "sequence": [exact, foreign]
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            &auth_file,
            serde_json::to_vec(&authorization_receipt(&plan, "stop")).unwrap(),
        )
        .unwrap();
        crate::write_json_atomic(
            &plan.stop_file,
            &serde_json::json!({
                "schema": "agent-harness.supervisor-stop-file.v1",
                "serviceId": "supervisor-reconcile",
                "persistent": true
            }),
        )
        .unwrap();
        fs::write(
            &ops_file,
            serde_json::to_vec(&serde_json::json!({
                "schema": "agent-harness.ops-control-receipt.v1",
                "action": "stop",
                "status": "stop-requested",
                "stopFiles": [{
                    "component": "supervisor-reconcile",
                    "stopFile": plan.stop_file,
                    "present": true
                }]
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            &proof_file,
            br#"{"keeperTerminalMatches":false,"keeperPidAlive":false,"ownerDispositions":[],"deploymentProcesses":[],"processTreeSurvivors":[]}"#,
        )
        .unwrap();
        let stop = plan
            .scripts
            .iter()
            .find(|script| script.name == "stop")
            .unwrap();
        let powershell = PathBuf::from(std::env::var_os("SystemRoot").unwrap())
            .join("System32/WindowsPowerShell/v1.0/powershell.exe");
        let output = Command::new(&powershell)
            .args([
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
            ])
            .arg(&stop.path)
            .args(["-LiveControlToken", "fixture-token", "-TimeoutSeconds", "1"])
            .env(
                "AGENT_HARNESS_KEEPER_TEST_TASK_OBSERVATION_FILE",
                &task_file,
            )
            .env("AGENT_HARNESS_KEEPER_TEST_EFFECT_FILE", &effect_file)
            .env("AGENT_HARNESS_KEEPER_TEST_AUTH_RECEIPT_FILE", &auth_file)
            .env("AGENT_HARNESS_KEEPER_TEST_OPS_RECEIPT_FILE", &ops_file)
            .env("AGENT_HARNESS_KEEPER_TEST_STOP_PROOF_FILE", &proof_file)
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(!effect_file.exists());
        let receipt: Value = serde_json::from_slice(
            &fs::read(plan.output_dir.join("stop-proof-latest.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(receipt["status"], "manual-recovery-required");
        assert_eq!(receipt["terminalMatchesPreRequestGeneration"], false);
        fixture.remove();
    }

    fn exact_task_snapshot(plan: &WindowsReconcileKeeperPlan, state: &str, enabled: bool) -> Value {
        serde_json::json!({
            "exists": true,
            "task": {
                "path": plan.task.path,
                "name": plan.task.name,
                "description": plan.task.description,
                "state": state,
                "enabled": enabled,
            },
            "principal": {
                "userId": plan.task.principal.user_id,
                "logonType": plan.task.principal.logon_type,
                "runLevel": plan.task.principal.run_level,
            },
            "action": {
                "count": 1,
                "execute": plan.scheduler_action.executable,
                "arguments": plan.scheduler_action.arguments,
                "workingDirectory": plan.scheduler_action.working_directory,
            },
            "trigger": {
                "count": 2,
                "logonCount": 1,
                "recoveryCount": 1,
                "userId": plan.task.trigger.user_id,
                "triggerType": "MSFT_TaskLogonTrigger",
                "recoveryTriggerType": "MSFT_TaskTimeTrigger",
                "recoveryStartBoundary": plan.task.trigger.recovery_start_boundary,
                "recoveryInterval": plan.task.trigger.recovery_interval,
                "recoveryDuration": plan.task.trigger.recovery_duration,
                "recoveryStopAtDurationEnd": plan.task.trigger.recovery_stop_at_duration_end,
            },
            "settings": {
                "multipleInstances": plan.task.settings.multiple_instances,
                "restartCount": plan.task.settings.restart_count,
                "restartInterval": plan.task.settings.restart_interval,
                "startWhenAvailable": plan.task.settings.start_when_available,
                "disallowStartIfOnBatteries": !plan.task.settings.allow_start_on_batteries,
                "stopIfGoingOnBatteries": plan.task.settings.stop_if_going_on_batteries,
                "executionTimeLimit": plan.task.settings.execution_time_limit,
            }
        })
    }

    fn missing_task_snapshot(plan: &WindowsReconcileKeeperPlan) -> Value {
        serde_json::json!({
            "exists": false,
            "task": {
                "path": plan.task.path,
                "name": plan.task.name,
                "state": "Missing",
                "enabled": false,
            }
        })
    }

    fn authorization_receipt(plan: &WindowsReconcileKeeperPlan, action: &str) -> Value {
        serde_json::json!({
            "schema": "agent-harness.ops-cutover-status.v1",
            "harnessHome": plan.harness_home,
            "status": "ready",
            "reason": "fixture token is valid",
            "action": action,
            "liveControl": {"status": "valid"}
        })
    }

    fn arg_value(args: &[String], flag: &str) -> String {
        let index = args.iter().position(|value| value == flag).unwrap();
        args[index + 1].clone()
    }

    #[cfg(windows)]
    fn create_directory_junction(link: &Path, target: &Path) {
        let output = Command::new("cmd.exe")
            .args(["/D", "/C", "mklink", "/J"])
            .arg(link)
            .arg(target)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "failed to create test junction {} -> {}: {}{}",
            link.display(),
            target.display(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    struct Fixture {
        root: PathBuf,
        harness_home: PathBuf,
        source_home: PathBuf,
        workspace: PathBuf,
        runtime_workspace: PathBuf,
        harness_cli: PathBuf,
        codex_executable: PathBuf,
        node_executable: PathBuf,
        gateway_script: PathBuf,
    }

    impl Fixture {
        fn new(name: &str) -> Self {
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let root = std::env::temp_dir().join(format!(
                "agent-harness-supervisor-keeper-{name}-{}-{nonce}",
                std::process::id()
            ));
            let harness_home = root.join("harness home");
            let source_home = root.join("source home");
            let workspace = root.join("workspace");
            let runtime_workspace = harness_home.join("runtime-workspace/default");
            for directory in [&harness_home, &source_home, &workspace, &runtime_workspace] {
                fs::create_dir_all(directory).unwrap();
            }
            let harness_cli = root.join("agent harness.exe");
            let codex_executable = root.join("codex.exe");
            let node_executable = root.join("node.exe");
            let gateway_script = root.join("discord gateway.mjs");
            fs::write(&harness_cli, b"keeper-cli-v1").unwrap();
            fs::write(&codex_executable, b"codex-v1").unwrap();
            fs::write(&node_executable, b"node-v1").unwrap();
            fs::write(&gateway_script, b"gateway-v1").unwrap();
            Self {
                root,
                harness_home,
                source_home,
                workspace,
                runtime_workspace,
                harness_cli,
                codex_executable,
                node_executable,
                gateway_script,
            }
        }

        fn options(&self) -> WindowsReconcileKeeperPlanOptions {
            WindowsReconcileKeeperPlanOptions {
                harness_home: self.harness_home.clone(),
                source_home: self.source_home.clone(),
                workspace: Some(self.workspace.clone()),
                runtime_workspace: self.runtime_workspace.clone(),
                harness_cli: self.harness_cli.clone(),
                codex_executable: Some(self.codex_executable.clone()),
                node_executable: Some(self.node_executable.clone()),
                discord_gateway_script: Some(self.gateway_script.clone()),
                user_id: "TEST\\keeper-user".to_string(),
                task_name: None,
                live_deployment: false,
            }
        }

        fn live_options(&self) -> WindowsReconcileKeeperPlanOptions {
            let harness_home = self.harness_home();
            let workspace = harness_home.join("workspace");
            let runtime_workspace = harness_home.join("runtime-workspace/default");
            let harness_cli = self.live_harness_cli();
            let codex = harness_home.join("tools/codex/codex.exe");
            let node = harness_home.join("tools/node/node.exe");
            let gateway = harness_home.join("tools/discord/index.mjs");
            for directory in [
                workspace.as_path(),
                runtime_workspace.as_path(),
                harness_cli.parent().unwrap(),
                codex.parent().unwrap(),
                node.parent().unwrap(),
                gateway.parent().unwrap(),
            ] {
                fs::create_dir_all(directory).unwrap();
            }
            fs::write(&harness_cli, b"live-cli").unwrap();
            fs::write(&codex, b"live-codex").unwrap();
            fs::write(&node, b"live-node").unwrap();
            fs::write(&gateway, b"live-gateway").unwrap();
            WindowsReconcileKeeperPlanOptions {
                harness_home: harness_home.clone(),
                source_home: harness_home,
                workspace: Some(workspace),
                runtime_workspace,
                harness_cli,
                codex_executable: Some(codex),
                node_executable: Some(node),
                discord_gateway_script: Some(gateway),
                user_id: "TEST\\keeper-user".to_string(),
                task_name: None,
                live_deployment: true,
            }
        }

        fn live_harness_cli(&self) -> PathBuf {
            self.harness_home()
                .join("bin/current")
                .join(if cfg!(windows) {
                    "agent-harness.exe"
                } else {
                    "agent-harness"
                })
        }

        fn harness_home(&self) -> PathBuf {
            normalize_canonical_path(fs::canonicalize(&self.harness_home).unwrap())
        }

        fn runtime_workspace(&self) -> PathBuf {
            normalize_canonical_path(fs::canonicalize(&self.runtime_workspace).unwrap())
        }

        fn script(&self, plan: &WindowsReconcileKeeperPlan, name: &str) -> String {
            let path = plan
                .scripts
                .iter()
                .find(|script| script.name == name)
                .unwrap()
                .path
                .clone();
            fs::read_to_string(path).unwrap()
        }

        fn remove(self) {
            fs::remove_dir_all(self.root).unwrap();
        }
    }
}
