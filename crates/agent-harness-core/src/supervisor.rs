use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::Serialize;

const WINDOWS_SUPERVISOR_PLAN_SCHEMA: &str = "agent-harness.windows-supervisor-plan.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowsSupervisorPlanOptions {
    pub harness_home: PathBuf,
    pub source_home: PathBuf,
    pub workspace: Option<PathBuf>,
    pub runtime_workspace: Option<PathBuf>,
    pub harness_cli: PathBuf,
    pub codex_executable: Option<PathBuf>,
    pub node_executable: PathBuf,
    pub discord_gateway_script: PathBuf,
    pub agent_id: Option<String>,
    pub output_dir: Option<PathBuf>,
    pub task_prefix: String,
    pub include_runtime: bool,
    pub runtime_workers: usize,
    pub include_worker: bool,
    pub include_cron_scheduler: bool,
    pub include_progress: bool,
    pub include_telegram: bool,
    pub include_discord: bool,
    pub idle_ms: u64,
    pub runtime_timeout_ms: u64,
    pub runtime_idle_timeout_ms: u64,
    pub max_consecutive_errors: usize,
    pub telegram_poll_timeout_seconds: u64,
    pub telegram_max_updates: usize,
    pub telegram_outbox_limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WindowsSupervisorPlanReport {
    pub schema: &'static str,
    pub harness_home: PathBuf,
    pub output_dir: PathBuf,
    pub receipt_file: PathBuf,
    pub scripts: Vec<WindowsSupervisorScript>,
    pub tasks: Vec<WindowsSupervisorTask>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WindowsSupervisorScript {
    pub name: String,
    pub path: PathBuf,
    pub purpose: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WindowsSupervisorTask {
    pub name: String,
    pub component: String,
    pub runner_script: PathBuf,
    pub stop_file: PathBuf,
    pub graceful_stop: bool,
}

pub fn write_windows_supervisor_plan(
    options: WindowsSupervisorPlanOptions,
) -> io::Result<WindowsSupervisorPlanReport> {
    let harness_home = absolutize_path(&options.harness_home)?;
    let source_home = absolutize_path(&options.source_home)?;
    let workspace = options
        .workspace
        .as_deref()
        .map(absolutize_path)
        .transpose()?;
    let runtime_workspace = options
        .runtime_workspace
        .as_deref()
        .map(absolutize_path)
        .transpose()?;
    let harness_cli = absolutize_path(&options.harness_cli)?;
    let codex_executable = options
        .codex_executable
        .as_deref()
        .map(absolutize_path)
        .transpose()?;
    let node_executable = absolutize_command_path(&options.node_executable)?;
    let discord_gateway_script = absolutize_path(&options.discord_gateway_script)?;
    let output_dir = match &options.output_dir {
        Some(output_dir) => absolutize_path(output_dir)?,
        None => harness_home
            .join("state")
            .join("supervisor")
            .join("windows-scheduled-tasks"),
    };
    let scripts_dir = output_dir.join("scripts");
    let stop_dir = output_dir.join("stop");
    let log_dir = harness_home.join("state").join("logs").join("supervisor");
    fs::create_dir_all(&scripts_dir)?;
    fs::create_dir_all(&stop_dir)?;
    fs::create_dir_all(&log_dir)?;

    let mut scripts = Vec::new();
    let mut tasks = Vec::new();
    let mut warnings = Vec::new();

    if codex_executable.is_none() && options.include_runtime {
        warnings.push(
            "codex executable was not pinned; generated runtime commands will rely on PATH"
                .to_string(),
        );
    }

    if options.include_runtime {
        let component = "runtime-loop";
        let runner_script = scripts_dir.join(format!("{component}.ps1"));
        let stop_file = stop_dir.join(format!("{component}.stop"));
        let mut args = vec![
            "runtime-loop".to_string(),
            "--harness-home".to_string(),
            path_arg(&harness_home),
            "--loop-name".to_string(),
            component.to_string(),
            "--runtime-concurrency".to_string(),
            options.runtime_workers.max(1).to_string(),
            "--timeout-ms".to_string(),
            options.runtime_timeout_ms.to_string(),
            "--idle-timeout-ms".to_string(),
            options.runtime_idle_timeout_ms.to_string(),
            "--iterations".to_string(),
            "0".to_string(),
            "--idle-ms".to_string(),
            options.idle_ms.to_string(),
            "--max-consecutive-errors".to_string(),
            options.max_consecutive_errors.to_string(),
            "--stop-file".to_string(),
            path_arg(&stop_file),
        ];
        if let Some(codex) = &codex_executable {
            args.extend(["--codex-exe".to_string(), path_arg(codex)]);
        }
        write_runner_script(&runner_script, &harness_cli, &args, &log_dir, component)?;
        push_task(
            &mut scripts,
            &mut tasks,
            &options.task_prefix,
            component,
            runner_script,
            stop_file,
            true,
        );
    }

    if options.include_worker {
        let component = "worker-loop";
        let runner_script = scripts_dir.join(format!("{component}.ps1"));
        let stop_file = stop_dir.join(format!("{component}.stop"));
        let args = vec![
            "worker-loop".to_string(),
            "--harness-home".to_string(),
            path_arg(&harness_home),
            "--iterations".to_string(),
            "0".to_string(),
            "--idle-ms".to_string(),
            options.idle_ms.to_string(),
            "--max-consecutive-errors".to_string(),
            options.max_consecutive_errors.to_string(),
            "--stop-file".to_string(),
            path_arg(&stop_file),
        ];
        write_runner_script(&runner_script, &harness_cli, &args, &log_dir, component)?;
        push_task(
            &mut scripts,
            &mut tasks,
            &options.task_prefix,
            component,
            runner_script,
            stop_file,
            true,
        );
    }

    if options.include_cron_scheduler {
        let component = "cron-scheduler-loop";
        let runner_script = scripts_dir.join(format!("{component}.ps1"));
        let stop_file = stop_dir.join(format!("{component}.stop"));
        let mut args = vec![
            "cron-scheduler-loop".to_string(),
            "--source-home".to_string(),
            path_arg(&source_home),
            "--harness-home".to_string(),
            path_arg(&harness_home),
            "--iterations".to_string(),
            "0".to_string(),
            "--idle-ms".to_string(),
            options.idle_ms.to_string(),
            "--max-consecutive-errors".to_string(),
            options.max_consecutive_errors.to_string(),
            "--stop-file".to_string(),
            path_arg(&stop_file),
        ];
        if let Some(workspace) = &workspace {
            args.extend(["--workspace".to_string(), path_arg(workspace)]);
        }
        if let Some(runtime_workspace) = &runtime_workspace {
            args.extend([
                "--runtime-workspace".to_string(),
                path_arg(runtime_workspace),
            ]);
        }
        write_runner_script(&runner_script, &harness_cli, &args, &log_dir, component)?;
        push_task(
            &mut scripts,
            &mut tasks,
            &options.task_prefix,
            component,
            runner_script,
            stop_file,
            true,
        );
    }

    if options.include_progress {
        let component = "progress-delivery-loop";
        let runner_script = scripts_dir.join(format!("{component}.ps1"));
        let stop_file = stop_dir.join(format!("{component}.stop"));
        let args = vec![
            "progress-delivery-loop".to_string(),
            "--harness-home".to_string(),
            path_arg(&harness_home),
            "--iterations".to_string(),
            "0".to_string(),
            "--idle-ms".to_string(),
            options.idle_ms.to_string(),
            "--max-consecutive-errors".to_string(),
            options.max_consecutive_errors.to_string(),
            "--stop-file".to_string(),
            path_arg(&stop_file),
        ];
        write_runner_script(&runner_script, &harness_cli, &args, &log_dir, component)?;
        push_task(
            &mut scripts,
            &mut tasks,
            &options.task_prefix,
            component,
            runner_script,
            stop_file,
            true,
        );
    }

    if options.include_telegram {
        let component = "telegram-loop";
        let runner_script = scripts_dir.join(format!("{component}.ps1"));
        let stop_file = stop_dir.join(format!("{component}.stop"));
        let mut args = vec![
            "telegram-loop".to_string(),
            "--source-home".to_string(),
            path_arg(&source_home),
            "--harness-home".to_string(),
            path_arg(&harness_home),
            "--iterations".to_string(),
            "0".to_string(),
            "--idle-ms".to_string(),
            options.idle_ms.to_string(),
            "--max-consecutive-errors".to_string(),
            options.max_consecutive_errors.to_string(),
            "--poll-timeout-seconds".to_string(),
            options.telegram_poll_timeout_seconds.to_string(),
            "--max-updates".to_string(),
            options.telegram_max_updates.to_string(),
            "--outbox-limit".to_string(),
            options.telegram_outbox_limit.to_string(),
            "--stop-file".to_string(),
            path_arg(&stop_file),
        ];
        if let Some(workspace) = &workspace {
            args.extend(["--workspace".to_string(), path_arg(workspace)]);
        }
        if let Some(runtime_workspace) = &runtime_workspace {
            args.extend([
                "--runtime-workspace".to_string(),
                path_arg(runtime_workspace),
            ]);
        }
        if let Some(agent_id) = &options.agent_id {
            args.extend(["--agent".to_string(), agent_id.clone()]);
        }
        if let Some(codex) = &codex_executable {
            args.extend(["--codex-exe".to_string(), path_arg(codex)]);
        }
        write_runner_script(&runner_script, &harness_cli, &args, &log_dir, component)?;
        push_task(
            &mut scripts,
            &mut tasks,
            &options.task_prefix,
            component,
            runner_script,
            stop_file,
            true,
        );
    }

    if options.include_discord {
        let component = "discord-outbox-loop";
        let runner_script = scripts_dir.join(format!("{component}.ps1"));
        let stop_file = stop_dir.join(format!("{component}.stop"));
        let args = vec![
            "discord-outbox-loop".to_string(),
            "--harness-home".to_string(),
            path_arg(&harness_home),
            "--iterations".to_string(),
            "0".to_string(),
            "--idle-ms".to_string(),
            options.idle_ms.to_string(),
            "--max-consecutive-errors".to_string(),
            options.max_consecutive_errors.to_string(),
            "--outbox-limit".to_string(),
            options.telegram_outbox_limit.to_string(),
            "--stop-file".to_string(),
            path_arg(&stop_file),
        ];
        write_runner_script(&runner_script, &harness_cli, &args, &log_dir, component)?;
        push_task(
            &mut scripts,
            &mut tasks,
            &options.task_prefix,
            component,
            runner_script,
            stop_file,
            true,
        );

        let component = "discord-gateway-loop";
        let runner_script = scripts_dir.join(format!("{component}.ps1"));
        let stop_file = stop_dir.join(format!("{component}.stop"));
        let mut args = vec![
            "discord-gateway-loop".to_string(),
            "--source-home".to_string(),
            path_arg(&source_home),
            "--harness-home".to_string(),
            path_arg(&harness_home),
            "--node-exe".to_string(),
            path_arg(&node_executable),
            "--gateway-script".to_string(),
            path_arg(&discord_gateway_script),
            "--harness-cli".to_string(),
            path_arg(&harness_cli),
            "--max-messages".to_string(),
            "0".to_string(),
            "--stop-file".to_string(),
            path_arg(&stop_file),
        ];
        if let Some(workspace) = &workspace {
            args.extend(["--workspace".to_string(), path_arg(workspace)]);
        }
        if let Some(runtime_workspace) = &runtime_workspace {
            args.extend([
                "--runtime-workspace".to_string(),
                path_arg(runtime_workspace),
            ]);
        }
        if let Some(agent_id) = &options.agent_id {
            args.extend(["--agent".to_string(), agent_id.clone()]);
        }
        if let Some(codex) = &codex_executable {
            args.extend(["--codex-exe".to_string(), path_arg(codex)]);
        }
        write_runner_script(&runner_script, &harness_cli, &args, &log_dir, component)?;
        push_task(
            &mut scripts,
            &mut tasks,
            &options.task_prefix,
            component,
            runner_script,
            stop_file,
            true,
        );
    }

    if tasks.is_empty() {
        warnings.push("no supervisor loops were selected".to_string());
    }

    let install_script = scripts_dir.join("install-scheduled-tasks.ps1");
    fs::write(
        &install_script,
        install_script_body(&tasks, "Agent Harness loop"),
    )?;
    scripts.push(WindowsSupervisorScript {
        name: "install-scheduled-tasks".to_string(),
        path: install_script,
        purpose: "register user logon scheduled tasks".to_string(),
    });

    let start_script = scripts_dir.join("start-scheduled-tasks.ps1");
    fs::write(
        &start_script,
        start_script_body(&tasks, &harness_cli, &harness_home),
    )?;
    scripts.push(WindowsSupervisorScript {
        name: "start-scheduled-tasks".to_string(),
        path: start_script,
        purpose: "clear stop files and start registered tasks".to_string(),
    });

    let stop_script = scripts_dir.join("stop-scheduled-tasks.ps1");
    fs::write(
        &stop_script,
        stop_script_body(&tasks, &harness_cli, &harness_home),
    )?;
    scripts.push(WindowsSupervisorScript {
        name: "stop-scheduled-tasks".to_string(),
        path: stop_script,
        purpose: "create stop files and request task stop".to_string(),
    });

    let uninstall_script = scripts_dir.join("uninstall-scheduled-tasks.ps1");
    fs::write(
        &uninstall_script,
        uninstall_script_body(&tasks, &harness_cli, &harness_home),
    )?;
    scripts.push(WindowsSupervisorScript {
        name: "uninstall-scheduled-tasks".to_string(),
        path: uninstall_script,
        purpose: "unregister generated scheduled tasks".to_string(),
    });

    let receipt_file = output_dir.join("supervisor-plan.json");
    let report = WindowsSupervisorPlanReport {
        schema: WINDOWS_SUPERVISOR_PLAN_SCHEMA,
        harness_home,
        output_dir,
        receipt_file,
        scripts,
        tasks,
        warnings,
    };
    let bytes = serde_json::to_vec_pretty(&report).map_err(io::Error::other)?;
    fs::write(&report.receipt_file, bytes)?;
    Ok(report)
}

fn push_task(
    scripts: &mut Vec<WindowsSupervisorScript>,
    tasks: &mut Vec<WindowsSupervisorTask>,
    task_prefix: &str,
    component: &str,
    runner_script: PathBuf,
    stop_file: PathBuf,
    graceful_stop: bool,
) {
    scripts.push(WindowsSupervisorScript {
        name: component.to_string(),
        path: runner_script.clone(),
        purpose: format!("run {component} under Task Scheduler"),
    });
    tasks.push(WindowsSupervisorTask {
        name: format!("{task_prefix}-{component}"),
        component: component.to_string(),
        runner_script,
        stop_file,
        graceful_stop,
    });
}

fn write_runner_script(
    script: &Path,
    executable: &Path,
    args: &[String],
    log_dir: &Path,
    log_name: &str,
) -> io::Result<()> {
    let invocation = command_invocation(executable, args);
    let body = format!(
        "$ErrorActionPreference = 'Continue'\n\
         $LogDir = {}\n\
         New-Item -ItemType Directory -Force -Path $LogDir | Out-Null\n\
         Get-ChildItem -LiteralPath $LogDir -Filter '{}-*.log' -File -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending | Select-Object -Skip 20 | Remove-Item -Force -ErrorAction SilentlyContinue\n\
         $LogFile = Join-Path $LogDir (\"{}-$(Get-Date -Format yyyyMMdd-HHmmss).log\")\n\
         {} *>&1 | Tee-Object -FilePath $LogFile\n\
         exit $LASTEXITCODE\n",
        ps_quote_path(log_dir),
        ps_escape_single(log_name),
        ps_escape_single(log_name),
        invocation
    );
    fs::write(script, body)
}

fn command_invocation(executable: &Path, args: &[String]) -> String {
    let mut parts = Vec::with_capacity(args.len() + 2);
    parts.push("&".to_string());
    parts.push(ps_quote_path(executable));
    parts.extend(args.iter().map(|arg| ps_quote(arg)));
    parts.join(" ")
}

fn install_script_body(tasks: &[WindowsSupervisorTask], description: &str) -> String {
    let mut body = String::from(
        "$ErrorActionPreference = 'Stop'\n\
         $Tasks = @(\n",
    );
    for task in tasks {
        body.push_str(&format!(
            "  @{{ Name = {}; Script = {}; Description = {} }}\n",
            ps_quote(&task.name),
            ps_quote_path(&task.runner_script),
            ps_quote(&format!("{description}: {}", task.component))
        ));
    }
    body.push_str(
        ")\n\
         foreach ($Task in $Tasks) {\n\
           $Action = New-ScheduledTaskAction -Execute 'powershell.exe' -Argument (\"-NoProfile -ExecutionPolicy Bypass -File `\"{0}`\"\" -f $Task.Script)\n\
           $Trigger = New-ScheduledTaskTrigger -AtLogOn\n\
           $Settings = New-ScheduledTaskSettingsSet -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1) -MultipleInstances IgnoreNew -AllowStartIfOnBatteries\n\
           Register-ScheduledTask -TaskName $Task.Name -Action $Action -Trigger $Trigger -Settings $Settings -Description $Task.Description -Force | Out-Null\n\
           Write-Host \"Registered $($Task.Name)\"\n\
         }\n",
    );
    body
}

fn start_script_body(
    tasks: &[WindowsSupervisorTask],
    harness_cli: &Path,
    harness_home: &Path,
) -> String {
    let mut body = live_control_guard_script(harness_cli, harness_home, "start");
    for task in tasks {
        body.push_str(&format!(
            "Remove-Item -Force -ErrorAction SilentlyContinue -LiteralPath {}\n\
             $Task = Get-ScheduledTask -TaskName {} -ErrorAction SilentlyContinue\n\
             if ($null -ne $Task) {{\n\
               Start-ScheduledTask -TaskName {} -ErrorAction SilentlyContinue\n\
               Write-Host \"Started scheduled task {}\"\n\
             }} else {{\n\
               Start-Process -FilePath 'powershell.exe' -ArgumentList @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', {}) -WindowStyle Hidden\n\
               Write-Host \"Started direct runner {} because scheduled task is not registered\"\n\
             }}\n",
            ps_quote_path(&task.stop_file),
            ps_quote(&task.name),
            ps_quote(&task.name),
            task.name,
            ps_quote_path(&task.runner_script),
            task.name
        ));
    }
    body
}

fn stop_script_body(
    tasks: &[WindowsSupervisorTask],
    harness_cli: &Path,
    harness_home: &Path,
) -> String {
    let mut body = live_control_guard_script(harness_cli, harness_home, "stop");
    for task in tasks {
        body.push_str(&format!(
            "New-Item -ItemType File -Force -Path {} | Out-Null\n",
            ps_quote_path(&task.stop_file)
        ));
    }
    body.push_str("Start-Sleep -Seconds 3\n");
    for task in tasks {
        body.push_str(&format!(
            "Stop-ScheduledTask -TaskName {} -ErrorAction SilentlyContinue\n",
            ps_quote(&task.name)
        ));
    }
    body
}

fn uninstall_script_body(
    tasks: &[WindowsSupervisorTask],
    harness_cli: &Path,
    harness_home: &Path,
) -> String {
    let mut body = live_control_guard_script(harness_cli, harness_home, "uninstall");
    for task in tasks {
        body.push_str(&format!(
            "Unregister-ScheduledTask -TaskName {} -Confirm:$false -ErrorAction SilentlyContinue\n",
            ps_quote(&task.name)
        ));
    }
    body
}

fn live_control_guard_script(harness_cli: &Path, harness_home: &Path, action: &str) -> String {
    format!(
        "param([string] $LiveControlToken)\n\
         $ErrorActionPreference = 'Continue'\n\
         function Test-AgentHarnessLiveFlag([string] $Value) {{\n\
           if ([string]::IsNullOrWhiteSpace($Value)) {{ return $false }}\n\
           return @('1', 'true', 'yes', 'on', 'live') -contains $Value.Trim().ToLowerInvariant()\n\
         }}\n\
         if (Test-AgentHarnessLiveFlag $env:AGENT_HARNESS_LIVE_SESSION) {{\n\
           $ResolvedLiveControlToken = if (-not [string]::IsNullOrWhiteSpace($LiveControlToken)) {{ $LiveControlToken }} elseif (-not [string]::IsNullOrWhiteSpace($env:AGENT_HARNESS_LIVE_CONTROL_TOKEN)) {{ $env:AGENT_HARNESS_LIVE_CONTROL_TOKEN }} else {{ $null }}\n\
           if ([string]::IsNullOrWhiteSpace($ResolvedLiveControlToken)) {{ throw 'live-control token is required for live agent-harness supervisor control' }}\n\
           $LiveControlStatus = & {} ops-cutover-status --target-home {} --action {} --live-control-token $ResolvedLiveControlToken | ConvertFrom-Json\n\
           if ($LASTEXITCODE -ne 0 -or $LiveControlStatus.status -ne 'ready') {{ throw 'live-control token validation failed for live agent-harness supervisor control' }}\n\
           $env:AGENT_HARNESS_LIVE_CONTROL_TOKEN = $ResolvedLiveControlToken\n\
         }}\n",
        ps_quote_path(harness_cli),
        ps_quote_path(harness_home),
        ps_quote(action)
    )
}

fn path_arg(path: &Path) -> String {
    path.display().to_string()
}

fn absolutize_path(path: &Path) -> io::Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

fn absolutize_command_path(path: &Path) -> io::Result<PathBuf> {
    if path.is_absolute() || path.components().count() == 1 {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

fn ps_quote_path(path: &Path) -> String {
    ps_quote(&path.display().to_string())
}

fn ps_quote(value: &str) -> String {
    format!("'{}'", ps_escape_single(value))
}

fn ps_escape_single(value: &str) -> String {
    value.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn writes_windows_supervisor_scripts_and_receipt() {
        let root = temp_root("writes_windows_supervisor_scripts_and_receipt");
        let harness_home = root.join(".agent-harness");
        let output_dir = root.join("supervisor");
        let report = write_windows_supervisor_plan(WindowsSupervisorPlanOptions {
            harness_home: harness_home.clone(),
            source_home: root.join(".openclaw"),
            workspace: Some(root.join("workspace")),
            runtime_workspace: None,
            harness_cli: root.join("agent-harness.exe"),
            codex_executable: Some(root.join("codex.cmd")),
            node_executable: PathBuf::from("node"),
            discord_gateway_script: root.join("tools").join("discord").join("index.mjs"),
            agent_id: Some("main".to_string()),
            output_dir: Some(output_dir.clone()),
            task_prefix: "AgentHarness".to_string(),
            include_runtime: true,
            runtime_workers: 2,
            include_worker: true,
            include_cron_scheduler: false,
            include_progress: true,
            include_telegram: true,
            include_discord: true,
            idle_ms: 1000,
            runtime_timeout_ms: 1_800_000,
            runtime_idle_timeout_ms: 300_000,
            max_consecutive_errors: 5,
            telegram_poll_timeout_seconds: 1,
            telegram_max_updates: 10,
            telegram_outbox_limit: 20,
        })
        .unwrap();

        assert_eq!(report.tasks.len(), 6);
        assert!(report.receipt_file.is_file());
        assert!(
            report
                .scripts
                .iter()
                .any(|script| script.name == "install-scheduled-tasks" && script.path.is_file())
        );
        let runtime_script =
            fs::read_to_string(output_dir.join("scripts").join("runtime-loop.ps1")).unwrap();
        assert!(runtime_script.contains("--stop-file"));
        assert!(runtime_script.contains("runtime-loop"));
        assert!(runtime_script.contains("--loop-name"));
        assert!(runtime_script.contains("--runtime-concurrency"));
        assert!(runtime_script.contains("--timeout-ms"));
        assert!(runtime_script.contains("'1800000'"));
        assert!(runtime_script.contains("--idle-timeout-ms"));
        assert!(runtime_script.contains("'300000'"));
        assert!(runtime_script.contains("'2'"));
        let worker_script =
            fs::read_to_string(output_dir.join("scripts").join("worker-loop.ps1")).unwrap();
        assert!(worker_script.contains("worker-loop"));
        assert!(worker_script.contains("--stop-file"));
        let progress_script = fs::read_to_string(
            output_dir
                .join("scripts")
                .join("progress-delivery-loop.ps1"),
        )
        .unwrap();
        assert!(progress_script.contains("progress-delivery-loop"));
        let discord_outbox_script =
            fs::read_to_string(output_dir.join("scripts").join("discord-outbox-loop.ps1")).unwrap();
        assert!(discord_outbox_script.contains("discord-outbox-loop"));
        assert!(discord_outbox_script.contains("$(Get-Date -Format yyyyMMdd-HHmmss)"));
        assert!(discord_outbox_script.contains("Select-Object -Skip 20"));
        let start_script =
            fs::read_to_string(output_dir.join("scripts").join("start-scheduled-tasks.ps1"))
                .unwrap();
        assert!(start_script.contains("Get-ScheduledTask"));
        assert!(start_script.contains("Start-Process"));
        assert!(start_script.contains("-WindowStyle Hidden"));
        assert!(start_script.contains("AGENT_HARNESS_LIVE_SESSION"));
        assert!(start_script.contains("ops-cutover-status"));
        let stop_script =
            fs::read_to_string(output_dir.join("scripts").join("stop-scheduled-tasks.ps1"))
                .unwrap();
        assert!(stop_script.contains("New-Item -ItemType File"));
        assert!(stop_script.contains("Stop-ScheduledTask"));
        assert!(stop_script.contains("AGENT_HARNESS_LIVE_SESSION"));
        assert!(stop_script.contains("ops-cutover-status"));

        let _ = fs::remove_dir_all(root);
    }

    fn temp_root(test_name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("agent_harness_supervisor_{test_name}_{nanos}"))
    }
}
