use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::Serialize;

const WINDOWS_SUPERVISOR_PLAN_SCHEMA: &str = "agent-harness.windows-supervisor-plan.v1";
const LEGACY_RUNTIME_WORKSPACE_ROOTS: &[&str] = &["D:\\Warehouse\\Research\\OpenClaw_WSL"];

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
    let runtime_workspace = match options.runtime_workspace.as_deref() {
        Some(path) => absolutize_path_from_base(path, &harness_home)?,
        None => harness_home.clone(),
    };
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
    if is_retired_legacy_source_home(&source_home, &harness_home) {
        warnings.push(format!(
            "source-home {} points at retired legacy .openclaw/import routing; live supervisor plans should use the active harness home as source-home",
            source_home.display()
        ));
    }
    if is_legacy_runtime_workspace(&runtime_workspace) {
        warnings.push(format!(
            "runtime-workspace {} points at a retired legacy workspace; generated plans should use the active harness home unless this is an explicit compatibility run",
            runtime_workspace.display()
        ));
    }
    warnings.extend(scan_stale_runtime_workspace_artifacts(
        &harness_home,
        &output_dir,
    )?);

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
            "--max-consecutive-errors".to_string(),
            options.max_consecutive_errors.to_string(),
            "--safe-mode-restart-ms".to_string(),
            "60000".to_string(),
            "--stop-file".to_string(),
            path_arg(&stop_file),
        ];
        if let Some(codex) = &codex_executable {
            args.extend(["--codex-exe".to_string(), path_arg(codex)]);
        }
        write_runner_script(
            &runner_script,
            &harness_cli,
            &args,
            &log_dir,
            component,
            &harness_home,
            &stop_file,
        )?;
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
            "--max-consecutive-errors".to_string(),
            options.max_consecutive_errors.to_string(),
            "--stop-file".to_string(),
            path_arg(&stop_file),
        ];
        write_runner_script(
            &runner_script,
            &harness_cli,
            &args,
            &log_dir,
            component,
            &harness_home,
            &stop_file,
        )?;
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
        args.extend([
            "--runtime-workspace".to_string(),
            path_arg(&runtime_workspace),
        ]);
        write_runner_script(
            &runner_script,
            &harness_cli,
            &args,
            &log_dir,
            component,
            &harness_home,
            &stop_file,
        )?;
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
            "supervisor-run".to_string(),
            "--service".to_string(),
            "progress-delivery-loop".to_string(),
            "--harness-home".to_string(),
            path_arg(&harness_home),
            "--harness-cli".to_string(),
            path_arg(&harness_cli),
            "--child-iterations".to_string(),
            "0".to_string(),
            "--idle-ms".to_string(),
            options.idle_ms.to_string(),
            "--max-consecutive-errors".to_string(),
            options.max_consecutive_errors.to_string(),
            "--restart-delay-ms".to_string(),
            "60000".to_string(),
            "--stop-file".to_string(),
            path_arg(&stop_file),
        ];
        write_runner_script(
            &runner_script,
            &harness_cli,
            &args,
            &log_dir,
            component,
            &harness_home,
            &stop_file,
        )?;
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
        args.extend([
            "--runtime-workspace".to_string(),
            path_arg(&runtime_workspace),
        ]);
        if let Some(agent_id) = &options.agent_id {
            args.extend(["--agent".to_string(), agent_id.clone()]);
        }
        if let Some(codex) = &codex_executable {
            args.extend(["--codex-exe".to_string(), path_arg(codex)]);
        }
        write_runner_script(
            &runner_script,
            &harness_cli,
            &args,
            &log_dir,
            component,
            &harness_home,
            &stop_file,
        )?;
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
            "supervisor-run".to_string(),
            "--service".to_string(),
            "discord-outbox-loop".to_string(),
            "--harness-home".to_string(),
            path_arg(&harness_home),
            "--harness-cli".to_string(),
            path_arg(&harness_cli),
            "--child-iterations".to_string(),
            "0".to_string(),
            "--idle-ms".to_string(),
            options.idle_ms.to_string(),
            "--max-consecutive-errors".to_string(),
            options.max_consecutive_errors.to_string(),
            "--restart-delay-ms".to_string(),
            "15000".to_string(),
            "--outbox-limit".to_string(),
            options.telegram_outbox_limit.to_string(),
            "--stop-file".to_string(),
            path_arg(&stop_file),
        ];
        write_runner_script(
            &runner_script,
            &harness_cli,
            &args,
            &log_dir,
            component,
            &harness_home,
            &stop_file,
        )?;
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
        args.extend([
            "--runtime-workspace".to_string(),
            path_arg(&runtime_workspace),
        ]);
        if let Some(agent_id) = &options.agent_id {
            args.extend(["--agent".to_string(), agent_id.clone()]);
        }
        if let Some(codex) = &codex_executable {
            args.extend(["--codex-exe".to_string(), path_arg(codex)]);
        }
        write_runner_script(
            &runner_script,
            &harness_cli,
            &args,
            &log_dir,
            component,
            &harness_home,
            &stop_file,
        )?;
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
    harness_home: &Path,
    stop_file: &Path,
) -> io::Result<()> {
    let invocation = command_invocation(executable, args);
    let body = if log_name == "runtime-loop" {
        format!(
            "$ErrorActionPreference = 'Continue'\n\
             $LogDir = {}\n\
             $HarnessHome = {}\n\
             $HarnessCli = {}\n\
             $SupervisorStopDir = Join-Path $HarnessHome 'state\\supervisor\\stop'\n\
             New-Item -ItemType Directory -Force -Path $LogDir | Out-Null\n\
             $SafeModeState = Join-Path $LogDir '{}-runner-safe-mode.json'\n\
             $SafeModeRestarts = 0\n\
             while ($true) {{\n\
               Get-ChildItem -LiteralPath $LogDir -Filter '{}-*.log' -File -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending | Select-Object -Skip 20 | Remove-Item -Force -ErrorAction SilentlyContinue\n\
              $StartedAtMs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()\n\
              $GenerationId = '{}-supervised-' + $PID + '-' + $StartedAtMs + '-' + $SafeModeRestarts\n\
              $env:AGENT_HARNESS_SERVICE_GENERATION_ID = $GenerationId\n\
              $env:AGENT_HARNESS_SERVICE_STARTED_AT_MS = [string]$StartedAtMs\n\
              $env:AGENT_HARNESS_SUPERVISOR_LAUNCH_OWNER = 'windows-runtime-runner'\n\
              $env:AGENT_HARNESS_SUPERVISOR_OBSERVED_ONLY = 'false'\n\
              $env:AGENT_HARNESS_SUPERVISOR_PARENT_PID = [string]$PID\n\
               $LogFile = Join-Path $LogDir (\"{}-$(Get-Date -Format yyyyMMdd-HHmmss).log\")\n\
               {} *> $LogFile\n\
               $ExitCode = $LASTEXITCODE\n\
               if ($ExitCode -eq 0) {{ exit 0 }}\n\
               try {{\n\
                 & $HarnessCli 'runtime-lease-reconcile' '--harness-home' $HarnessHome '--service' '{}' '--generation-id' $GenerationId *>> $LogFile\n\
               }} catch {{\n\
                 Add-Content -LiteralPath $LogFile -Value (\"runtime-lease-reconcile failed: \" + $_.Exception.Message)\n\
               }}\n\
               $SafeModeRestarts += 1\n\
               $LogTail = ''\n\
               try {{ $LogTail = (Get-Content -LiteralPath $LogFile -Tail 200 -ErrorAction SilentlyContinue) -join \"`n\" }} catch {{ $LogTail = '' }}\n\
               $ErrorClass = 'process-exit'\n\
               $RestartAfterSeconds = 60\n\
               $MemoryGateDecision = $null\n\
               if ($LogTail -match '(?i)(out of memory|\\boom\\b|memory allocation|memory pressure|not enough memory|insufficient memory|resource exhausted|STATUS_NO_MEMORY|0xC0000017)') {{\n\
                 $ErrorClass = 'resource-exhausted'\n\
                 $RestartAfterSeconds = 300\n\
                 New-Item -ItemType Directory -Force -Path $SupervisorStopDir | Out-Null\n\
                 $ProgressStopFile = Join-Path $SupervisorStopDir 'progress-delivery-loop.stop'\n\
                 $CreatedAtMs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()\n\
                 $ExpiresAtMs = $CreatedAtMs + 300000\n\
                 @{{ schema = 'agent-harness.supervisor-stop-file.v1'; serviceId = 'progress-delivery-loop'; reason = 'memory-pressure-gate: runtime-loop resource exhaustion'; createdBy = 'runtime-loop-runner'; createdAtMs = $CreatedAtMs; expiresAtMs = $ExpiresAtMs; persistent = $false }} | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $ProgressStopFile -Encoding UTF8\n\
                 $MemoryGateDecision = @{{ action = 'pause-low-priority-service'; serviceIds = @('progress-delivery-loop'); stopFiles = @($ProgressStopFile); reason = 'resource-exhausted'; expiresAtMs = $ExpiresAtMs }}\n\
               }}\n\
               @{{ schema = 'agent-harness.runtime-loop-runner-safe-mode.v1'; component = '{}'; exitCode = $ExitCode; restarts = $SafeModeRestarts; errorClass = $ErrorClass; logFile = $LogFile; at = (Get-Date).ToString('o'); restartAfterSeconds = $RestartAfterSeconds; memoryGateDecision = $MemoryGateDecision }} | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $SafeModeState -Encoding UTF8\n\
               Start-Sleep -Seconds $RestartAfterSeconds\n\
             }}\n",
            ps_quote_path(log_dir),
            ps_quote_path(harness_home),
            ps_quote_path(executable),
            ps_escape_single(log_name),
            ps_escape_single(log_name),
            ps_escape_single(log_name),
            ps_escape_single(log_name),
            invocation,
            ps_escape_single(log_name),
            ps_escape_single(log_name)
        )
    } else {
        format!(
            "$ErrorActionPreference = 'Continue'\n\
             $LogDir = {}\n\
             $StopFile = {}\n\
             while ($true) {{\n\
               New-Item -ItemType Directory -Force -Path $LogDir | Out-Null\n\
               Get-ChildItem -LiteralPath $LogDir -Filter '{}-*.log' -File -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending | Select-Object -Skip 20 | Remove-Item -Force -ErrorAction SilentlyContinue\n\
               $LogFile = Join-Path $LogDir (\"{}-$(Get-Date -Format yyyyMMdd-HHmmss).log\")\n\
               {} *> $LogFile\n\
               $ExitCode = $LASTEXITCODE\n\
               $RestartRequested = $false\n\
               if (Test-Path -LiteralPath $StopFile) {{\n\
                 try {{\n\
                   $StopEnvelope = Get-Content -LiteralPath $StopFile -Raw -ErrorAction Stop | ConvertFrom-Json -ErrorAction Stop\n\
                   if (($StopEnvelope.action -eq 'restart') -or ($StopEnvelope.restart -eq $true) -or (($StopEnvelope.persistent -eq $false) -and ($StopEnvelope.createdBy -eq 'channel-restart-command'))) {{ $RestartRequested = $true }}\n\
                 }} catch {{ $RestartRequested = $false }}\n\
               }}\n\
               if ($RestartRequested) {{\n\
                 Remove-Item -Force -ErrorAction SilentlyContinue -LiteralPath $StopFile\n\
                 Start-Sleep -Seconds 2\n\
                 continue\n\
               }}\n\
               exit $ExitCode\n\
             }}\n",
            ps_quote_path(log_dir),
            ps_quote_path(stop_file),
            ps_escape_single(log_name),
            ps_escape_single(log_name),
            invocation
        )
    };
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

fn absolutize_path_from_base(path: &Path, base: &Path) -> io::Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(base.join(path))
    }
}

fn absolutize_command_path(path: &Path) -> io::Result<PathBuf> {
    if path.is_absolute() || path.components().count() == 1 {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

fn is_retired_legacy_source_home(source_home: &Path, harness_home: &Path) -> bool {
    if source_home == harness_home {
        return false;
    }
    source_home
        .file_name()
        .and_then(|name| name.to_str())
        .map(|name| name.eq_ignore_ascii_case(".openclaw"))
        .unwrap_or(false)
}

fn is_legacy_runtime_workspace(path: &Path) -> bool {
    let normalized = normalize_path_text(path);
    LEGACY_RUNTIME_WORKSPACE_ROOTS
        .iter()
        .any(|root| normalized.starts_with(&normalize_path_text(Path::new(root))))
}

fn scan_stale_runtime_workspace_artifacts(
    harness_home: &Path,
    supervisor_output_dir: &Path,
) -> io::Result<Vec<String>> {
    let mut warnings = Vec::new();
    let supervisor_scripts = supervisor_output_dir.join("scripts");
    let supervisor_hits = count_files_containing_legacy_roots(&supervisor_scripts)?;
    if supervisor_hits > 0 {
        warnings.push(format!(
            "detected {supervisor_hits} existing supervisor script(s) with retired legacy runtime workspace references under {}; report-only, not rewritten",
            supervisor_scripts.display()
        ));
    }

    let session_hits = count_files_containing_legacy_roots(&harness_home.join("agents"))?;
    if session_hits > 0 {
        warnings.push(format!(
            "detected {session_hits} existing Codex app-server session metadata file(s) with retired legacy workingDirectory references under {}; report-only, not rewritten",
            harness_home.join("agents").display()
        ));
    }
    Ok(warnings)
}

fn count_files_containing_legacy_roots(root: &Path) -> io::Result<usize> {
    let mut count = 0;
    let mut stack = vec![root.to_path_buf()];
    while let Some(path) = stack.pop() {
        let entries = match fs::read_dir(&path) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            let metadata = entry.metadata()?;
            if metadata.is_dir() {
                stack.push(path);
            } else if metadata.is_file() && file_contains_legacy_root(&path)? {
                count += 1;
            }
        }
    }
    Ok(count)
}

fn file_contains_legacy_root(path: &Path) -> io::Result<bool> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::InvalidData => return Ok(false),
        Err(error) => return Err(error),
    };
    Ok(LEGACY_RUNTIME_WORKSPACE_ROOTS
        .iter()
        .any(|root| text.contains(root)))
}

fn normalize_path_text(path: &Path) -> String {
    path.display()
        .to_string()
        .replace('/', "\\")
        .trim_end_matches('\\')
        .to_ascii_lowercase()
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
        assert!(runtime_script.contains("--safe-mode-restart-ms"));
        assert!(runtime_script.contains("'2'"));
        assert!(!runtime_script.contains("Tee-Object"));
        assert!(runtime_script.contains("*> $LogFile"));
        assert!(runtime_script.contains("$ErrorClass = 'process-exit'"));
        assert!(runtime_script.contains("$ErrorClass = 'resource-exhausted'"));
        assert!(runtime_script.contains("restartAfterSeconds = $RestartAfterSeconds"));
        assert!(runtime_script.contains("memoryGateDecision = $MemoryGateDecision"));
        assert!(runtime_script.contains("agent-harness.supervisor-stop-file.v1"));
        assert!(runtime_script.contains("progress-delivery-loop.stop"));
        assert!(runtime_script.contains("pause-low-priority-service"));
        assert!(runtime_script.contains("memory-pressure-gate: runtime-loop resource exhaustion"));
        assert!(runtime_script.contains("AGENT_HARNESS_SERVICE_GENERATION_ID"));
        assert!(runtime_script.contains("AGENT_HARNESS_SERVICE_STARTED_AT_MS"));
        assert!(runtime_script.contains("windows-runtime-runner"));
        assert!(runtime_script.contains("runtime-lease-reconcile"));
        assert!(runtime_script.contains("--generation-id"));
        assert!(runtime_script.contains("Start-Sleep -Seconds $RestartAfterSeconds"));
        let worker_script =
            fs::read_to_string(output_dir.join("scripts").join("worker-loop.ps1")).unwrap();
        assert!(worker_script.contains("worker-loop"));
        assert!(worker_script.contains("--stop-file"));
        assert!(!worker_script.contains("Tee-Object"));
        let progress_script = fs::read_to_string(
            output_dir
                .join("scripts")
                .join("progress-delivery-loop.ps1"),
        )
        .unwrap();
        assert!(progress_script.contains("progress-delivery-loop"));
        assert!(progress_script.contains("supervisor-run"));
        assert!(progress_script.contains("--service"));
        assert!(progress_script.contains("--child-iterations"));
        assert!(progress_script.contains("--restart-delay-ms"));
        assert!(!progress_script.contains("Tee-Object"));
        assert!(progress_script.contains("*> $LogFile"));
        let discord_outbox_script =
            fs::read_to_string(output_dir.join("scripts").join("discord-outbox-loop.ps1")).unwrap();
        assert!(discord_outbox_script.contains("discord-outbox-loop"));
        assert!(discord_outbox_script.contains("supervisor-run"));
        assert!(discord_outbox_script.contains("--service"));
        assert!(discord_outbox_script.contains("--child-iterations"));
        assert!(discord_outbox_script.contains("--restart-delay-ms"));
        assert!(discord_outbox_script.contains("--outbox-limit"));
        assert!(discord_outbox_script.contains("$(Get-Date -Format yyyyMMdd-HHmmss)"));
        assert!(discord_outbox_script.contains("Select-Object -Skip 20"));
        assert!(!discord_outbox_script.contains("Tee-Object"));
        let telegram_script =
            fs::read_to_string(output_dir.join("scripts").join("telegram-loop.ps1")).unwrap();
        assert!(telegram_script.contains("$StopFile"));
        assert!(telegram_script.contains("channel-restart-command"));
        assert!(
            telegram_script.contains(
                "Remove-Item -Force -ErrorAction SilentlyContinue -LiteralPath $StopFile"
            )
        );
        let discord_gateway_script =
            fs::read_to_string(output_dir.join("scripts").join("discord-gateway-loop.ps1"))
                .unwrap();
        assert!(discord_gateway_script.contains("$StopFile"));
        assert!(discord_gateway_script.contains("channel-restart-command"));
        assert!(
            discord_gateway_script.contains(
                "Remove-Item -Force -ErrorAction SilentlyContinue -LiteralPath $StopFile"
            )
        );
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

    #[test]
    fn warns_when_source_home_is_retired_openclaw() {
        let root = temp_root("warns_when_source_home_is_retired_openclaw");
        let harness_home = root.join(".agent-harness");
        let report = write_windows_supervisor_plan(WindowsSupervisorPlanOptions {
            harness_home: harness_home.clone(),
            source_home: root.join(".openclaw"),
            workspace: None,
            runtime_workspace: None,
            harness_cli: root.join("agent-harness.exe"),
            codex_executable: Some(root.join("codex.cmd")),
            node_executable: PathBuf::from("node"),
            discord_gateway_script: root.join("tools").join("discord").join("index.mjs"),
            agent_id: None,
            output_dir: Some(root.join("supervisor")),
            task_prefix: "AgentHarness".to_string(),
            include_runtime: false,
            runtime_workers: 1,
            include_worker: false,
            include_cron_scheduler: true,
            include_progress: false,
            include_telegram: false,
            include_discord: false,
            idle_ms: 1000,
            runtime_timeout_ms: 1_800_000,
            runtime_idle_timeout_ms: 300_000,
            max_consecutive_errors: 5,
            telegram_poll_timeout_seconds: 1,
            telegram_max_updates: 10,
            telegram_outbox_limit: 20,
        })
        .unwrap();

        assert!(
            report
                .warnings
                .iter()
                .any(|warning| warning.contains("retired legacy .openclaw/import routing"))
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn harness_home_named_openclaw_is_not_warned_as_external_legacy_source() {
        let root = temp_root("harness_home_named_openclaw_is_not_warned");
        let harness_home = root.join(".openclaw");
        let report = write_windows_supervisor_plan(WindowsSupervisorPlanOptions {
            harness_home: harness_home.clone(),
            source_home: harness_home,
            workspace: None,
            runtime_workspace: None,
            harness_cli: root.join("agent-harness.exe"),
            codex_executable: Some(root.join("codex.cmd")),
            node_executable: PathBuf::from("node"),
            discord_gateway_script: root.join("tools").join("discord").join("index.mjs"),
            agent_id: None,
            output_dir: Some(root.join("supervisor")),
            task_prefix: "AgentHarness".to_string(),
            include_runtime: false,
            runtime_workers: 1,
            include_worker: false,
            include_cron_scheduler: true,
            include_progress: false,
            include_telegram: false,
            include_discord: false,
            idle_ms: 1000,
            runtime_timeout_ms: 1_800_000,
            runtime_idle_timeout_ms: 300_000,
            max_consecutive_errors: 5,
            telegram_poll_timeout_seconds: 1,
            telegram_max_updates: 10,
            telegram_outbox_limit: 20,
        })
        .unwrap();

        assert!(
            !report
                .warnings
                .iter()
                .any(|warning| warning.contains("retired legacy .openclaw/import routing"))
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn defaults_runtime_workspace_to_harness_home() {
        let root = temp_root("defaults_runtime_workspace_to_harness_home");
        let harness_home = root.join(".agent-harness");
        let output_dir = root.join("supervisor");
        let report = write_windows_supervisor_plan(WindowsSupervisorPlanOptions {
            harness_home: harness_home.clone(),
            source_home: harness_home.clone(),
            workspace: Some(harness_home.join("workspace")),
            runtime_workspace: None,
            harness_cli: root.join("agent-harness.exe"),
            codex_executable: Some(root.join("codex.cmd")),
            node_executable: PathBuf::from("node"),
            discord_gateway_script: root.join("tools").join("discord").join("index.mjs"),
            agent_id: Some("main".to_string()),
            output_dir: Some(output_dir.clone()),
            task_prefix: "AgentHarness".to_string(),
            include_runtime: false,
            runtime_workers: 1,
            include_worker: false,
            include_cron_scheduler: true,
            include_progress: false,
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

        assert!(report.warnings.is_empty());
        for script_name in [
            "cron-scheduler-loop.ps1",
            "telegram-loop.ps1",
            "discord-gateway-loop.ps1",
        ] {
            let script = fs::read_to_string(output_dir.join("scripts").join(script_name)).unwrap();
            assert!(script.contains("--runtime-workspace"));
            assert!(script.contains(&harness_home.display().to_string()));
        }

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn warns_for_legacy_runtime_workspace_without_rewriting_existing_artifacts() {
        let root = temp_root("warns_for_legacy_runtime_workspace");
        let harness_home = root.join(".agent-harness");
        let output_dir = root.join("supervisor");
        let scripts_dir = output_dir.join("scripts");
        let session_dir = harness_home.join("agents").join("main").join("sessions");
        fs::create_dir_all(&scripts_dir).unwrap();
        fs::create_dir_all(&session_dir).unwrap();
        let legacy_script = scripts_dir.join("telegram-loop.ps1");
        let legacy_session = session_dir.join("session.codex-app-server.json");
        fs::write(
            &legacy_script,
            "before D:\\Warehouse\\Research\\OpenClaw_WSL after",
        )
        .unwrap();
        fs::write(
            &legacy_session,
            r#"{"workingDirectory":"D:\Warehouse\Research\OpenClaw_WSL"}"#,
        )
        .unwrap();

        let report = write_windows_supervisor_plan(WindowsSupervisorPlanOptions {
            harness_home: harness_home.clone(),
            source_home: harness_home.clone(),
            workspace: Some(harness_home.join("workspace")),
            runtime_workspace: Some(PathBuf::from("D:\\Warehouse\\Research\\OpenClaw_WSL")),
            harness_cli: root.join("agent-harness.exe"),
            codex_executable: Some(root.join("codex.cmd")),
            node_executable: PathBuf::from("node"),
            discord_gateway_script: root.join("tools").join("discord").join("index.mjs"),
            agent_id: Some("main".to_string()),
            output_dir: Some(output_dir.clone()),
            task_prefix: "AgentHarness".to_string(),
            include_runtime: false,
            runtime_workers: 1,
            include_worker: false,
            include_cron_scheduler: false,
            include_progress: false,
            include_telegram: true,
            include_discord: false,
            idle_ms: 1000,
            runtime_timeout_ms: 1_800_000,
            runtime_idle_timeout_ms: 300_000,
            max_consecutive_errors: 5,
            telegram_poll_timeout_seconds: 1,
            telegram_max_updates: 10,
            telegram_outbox_limit: 20,
        })
        .unwrap();

        assert!(report.warnings.iter().any(|warning| {
            warning.contains("runtime-workspace") && warning.contains("retired legacy workspace")
        }));
        assert!(report.warnings.iter().any(|warning| {
            warning.contains("existing supervisor script") && warning.contains("report-only")
        }));
        assert!(report.warnings.iter().any(|warning| {
            warning.contains("Codex app-server session metadata") && warning.contains("report-only")
        }));
        assert!(
            fs::read_to_string(&legacy_session)
                .unwrap()
                .contains("D:\\Warehouse\\Research\\OpenClaw_WSL")
        );

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
