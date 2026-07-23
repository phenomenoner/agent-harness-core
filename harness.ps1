param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]] $CommandArgs
)

$ErrorActionPreference = 'Stop'

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$HarnessHome = Join-Path $Root '.agent-harness'
$HarnessExe = Join-Path $HarnessHome 'bin\current\agent-harness.exe'
$AgentWorkspace = Join-Path $HarnessHome 'workspace'
$RuntimeWorkspace = Join-Path $HarnessHome 'runtime-workspace\default'
$CodexExe = Join-Path $HarnessHome 'tools\codex-cli\node_modules\@openai\codex-win32-x64\vendor\x86_64-pc-windows-msvc\bin\codex.exe'
$DiscordGateway = Join-Path $HarnessHome 'tools\agent-discord-gateway\index.mjs'
$NodeExe = Join-Path $HarnessHome 'tools\node\node.exe'
$KeeperRoot = Join-Path $HarnessHome 'state\supervisor\windows-reconcile-keeper'
$KeeperPlan = Join-Path $KeeperRoot 'keeper-plan.json'
$KeeperScripts = Join-Path $KeeperRoot 'scripts'
$SupervisorLogs = Join-Path $HarnessHome 'state\logs\supervisor'
$HarnessLog = Join-Path $HarnessHome 'state\logs\harness.jsonl'
$ProgressLog = Join-Path $HarnessHome 'state\runtime-queue\progress-events.jsonl'

function Show-Help {
    @"
Usage:
  harness gateway start --live-control-token <token>
  harness gateway stop --live-control-token <token>
  harness gateway restart --live-control-token <token>
  harness gateway status
  harness gateway ps
  harness gateway logs
  harness gateway tail <component> [lines]
  harness gateway keeper-plan --live-control-token <token>
  harness gateway keeper-install --live-control-token <token>
  harness gateway keeper-readback
  harness gateway keeper-uninstall --live-control-token <token>
  harness gateway stop --live-control-token <token>

Components for tail:
  runtime, telegram, discord, discord-gateway, discord-outbox, progress, worker, harness, progress-events

Examples:
  harness gateway restart --live-control-token <token>
  harness gateway status
  harness gateway tail runtime 200
  harness gateway tail harness 300
"@
}

function Require-File {
    param(
        [string] $Path,
        [string] $Hint
    )
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "Missing file: $Path`n$Hint"
    }
}

function Invoke-LiveOpsControl {
    param(
        [ValidateSet('start', 'stop')]
        [string] $Action,
        [string] $LiveControlToken
    )
    Assert-LiveGatewayControlAllowed $Action $LiveControlToken
    $args = @('ops-control', '--harness-home', $HarnessHome, '--action', $Action,
        '--reason', "harness.ps1 gateway $Action")
    if (-not [string]::IsNullOrWhiteSpace($LiveControlToken)) {
        $args += @('--live-control-token', $LiveControlToken)
    }
    $output = & $HarnessExe @args 2>&1
    $exitCode = $LASTEXITCODE
    if ($exitCode -ne 0) {
        throw "ops-control $Action failed with exit code ${exitCode}: $($output -join [Environment]::NewLine)"
    }
    try {
        $receipt = ($output -join [Environment]::NewLine) | ConvertFrom-Json -ErrorAction Stop
    } catch {
        throw "ops-control $Action returned invalid JSON: $($_.Exception.Message)"
    }
    $expectedStatus = if ($Action -eq 'start') { 'start-ready' } else { 'stop-requested' }
    if ([string]$receipt.schema -ne 'agent-harness.ops-control-receipt.v1' -or
        [string]$receipt.action -ne $Action -or
        [string]$receipt.status -ne $expectedStatus) {
        throw "ops-control $Action did not reach exact $expectedStatus state"
    }
    if ($Action -eq 'stop' -and @($receipt.stopFiles | Where-Object { $_.present -ne $true }).Count -ne 0) {
        throw 'ops-control stop receipt contains an unheld supervisor'
    }
    if ($Action -eq 'start' -and @($receipt.stopFiles | Where-Object { $_.present -eq $true }).Count -ne 0) {
        throw 'ops-control start receipt contains a remaining supervisor hold'
    }
    $receipt | ConvertTo-Json -Depth 16
}

function Write-LiveKeeperPlan {
    param([string] $LiveControlToken)
    Assert-LiveGatewayControlAllowed 'cutover' $LiveControlToken
    Require-File $HarnessExe "Deploy .agent-harness\bin\current\agent-harness.exe first."
    Require-File $CodexExe "Deploy the Codex CLI below .agent-harness\tools first."
    Require-File $NodeExe "Install or deploy the pinned Node executable first."
    Require-File $DiscordGateway "Deploy the Discord gateway below .agent-harness\tools first."
    if (-not (Test-Path -LiteralPath $AgentWorkspace -PathType Container)) {
        throw "Missing agent workspace: $AgentWorkspace"
    }
    if (-not (Test-Path -LiteralPath $RuntimeWorkspace -PathType Container)) {
        throw "Missing confined runtime workspace: $RuntimeWorkspace"
    }
    & $HarnessExe supervisor-keeper-plan --harness-home $HarnessHome --source-home $HarnessHome `
        --workspace $AgentWorkspace --runtime-workspace $RuntimeWorkspace --harness-cli $HarnessExe `
        --codex-exe $CodexExe --node-exe $NodeExe --gateway-script $DiscordGateway `
        --live-control-token $LiveControlToken
    if ($LASTEXITCODE -ne 0) {
        throw "supervisor-keeper-plan failed with exit code $LASTEXITCODE"
    }
}

function Invoke-LiveKeeperScript {
    param(
        [ValidateSet('install', 'start', 'stop', 'uninstall', 'readback')]
        [string] $Name,
        [string] $LiveControlToken
    )
    Require-File $KeeperPlan "Run 'harness gateway keeper-plan' with the deployed candidate first."
    $script = Join-Path $KeeperScripts "$Name.ps1"
    Require-File $script "Regenerate the reconcile keeper plan before live control."
    $windowsPowerShell = Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
    Require-File $windowsPowerShell 'Windows PowerShell is required for Scheduled Tasks control.'
    $scriptArgs = @('-NoLogo', '-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass', '-File', $script)
    if ($Name -in @('install', 'start', 'stop', 'uninstall')) {
        $scriptArgs += @('-LiveControlToken', $LiveControlToken)
    }
    & $windowsPowerShell @scriptArgs
    if ($LASTEXITCODE -ne 0) {
        throw "keeper $Name failed with exit code $LASTEXITCODE"
    }
}

function Start-LiveSupervisors {
    param([string] $LiveControlToken)
    Require-File $HarnessExe "Deploy .agent-harness\bin\current\agent-harness.exe first."
    Require-File $CodexExe "Deploy the Codex CLI below .agent-harness\tools first."
    Require-File $NodeExe "Install or deploy the pinned Node executable first."
    Require-File $DiscordGateway "Deploy the Discord gateway below .agent-harness\tools first."
    Require-File $KeeperPlan "Generate and install the reconcile keeper before starting live supervisors."
    Require-File (Join-Path $KeeperScripts 'start.ps1') "Regenerate the reconcile keeper plan before starting live supervisors."
    Invoke-LiveKeeperScript 'start' $LiveControlToken
}

function Stop-LiveSupervisors {
    param([string] $LiveControlToken)
    $stopScript = Join-Path $KeeperScripts 'stop.ps1'
    if ((Test-Path -LiteralPath $KeeperPlan -PathType Leaf) -and
        (Test-Path -LiteralPath $stopScript -PathType Leaf)) {
        Invoke-LiveKeeperScript 'stop' $LiveControlToken
    } else {
        Write-Warning 'Reconcile keeper plan is not installed; applying the guarded global hold and child stop directly.'
        Invoke-LiveOpsControl 'stop' $LiveControlToken
    }
}

function Wait-LiveSupervisorsStopped {
    $deadline = [DateTime]::UtcNow.AddSeconds(120)
    do {
        $remaining = @(Get-CimInstance Win32_Process -Filter "name = 'agent-harness.exe'" |
            Where-Object { $_.CommandLine -like "*$HarnessHome*" })
        if ($remaining.Count -eq 0) {
            return
        }
        Start-Sleep -Seconds 1
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "Timed out waiting for live Agent Harness processes to stop"
}

function Get-LiveControlTokenArg {
    for ($i = 0; $i -lt $CommandArgs.Count; $i++) {
        if ($CommandArgs[$i] -eq '--live-control-token') {
            if ($i + 1 -ge $CommandArgs.Count) {
                throw "--live-control-token requires a value"
            }
            return $CommandArgs[$i + 1]
        }
    }
    if (-not [string]::IsNullOrWhiteSpace($env:AGENT_HARNESS_LIVE_CONTROL_TOKEN)) {
        return $env:AGENT_HARNESS_LIVE_CONTROL_TOKEN
    }
    return $null
}

function Assert-LiveGatewayControlAllowed {
    param(
        [string] $Action,
        [string] $LiveControlToken
    )
    if ([string]::IsNullOrWhiteSpace($LiveControlToken)) {
        throw "live-control token is required for live gateway $Action"
    }
    Require-File $HarnessExe "Deploy .agent-harness\bin\current\agent-harness.exe first. Cargo target\ is source/staging output only."
    $output = & $HarnessExe ops-cutover-status --harness-home $HarnessHome --action $Action --live-control-token $LiveControlToken 2>&1
    $exitCode = $LASTEXITCODE
    if ($exitCode -ne 0) {
        throw "live-control token validation failed for live gateway $Action with exit code $exitCode"
    }
    try {
        $status = ($output -join [Environment]::NewLine) | ConvertFrom-Json -ErrorAction Stop
    } catch {
        throw "live-control token validation returned invalid JSON for live gateway ${Action}: $($_.Exception.Message)"
    }
    $exact = ([string]$status.schema -eq 'agent-harness.ops-cutover-status.v1') -and
        ([string]$status.status -eq 'ready') -and
        ([string]$status.action -eq $Action) -and
        ([string]$status.liveControl.status -eq 'valid') -and
        [string]::Equals([string]$status.harnessHome, [string]$HarnessHome, [StringComparison]::OrdinalIgnoreCase)
    if (-not $exact) {
        throw "live-control token validation failed for live gateway $Action"
    }
}

function Show-Status {
    Require-File $HarnessExe "Deploy .agent-harness\bin\current\agent-harness.exe first. Cargo target\ is source/staging output only."
    & $HarnessExe status --harness-home $HarnessHome
}

function Show-Processes {
    Get-CimInstance Win32_Process -Filter "name = 'agent-harness.exe'" |
        Select-Object ProcessId,CommandLine |
        Format-List
}

function Show-Logs {
    if (-not (Test-Path -LiteralPath $SupervisorLogs -PathType Container)) {
        throw "Missing supervisor log directory: $SupervisorLogs"
    }
    Get-ChildItem -LiteralPath $SupervisorLogs -Filter '*.log' |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 20 Name,LastWriteTime,Length
}

function Component-To-Filter {
    param([string] $Component)
    switch ($Component.ToLowerInvariant()) {
        'runtime' { 'runtime-loop-*.log'; break }
        'runtime-loop' { 'runtime-loop-*.log'; break }
        'telegram' { 'telegram-loop-*.log'; break }
        'telegram-loop' { 'telegram-loop-*.log'; break }
        'discord' { 'discord-gateway-loop-*.log'; break }
        'discord-gateway' { 'discord-gateway-loop-*.log'; break }
        'discord-gateway-loop' { 'discord-gateway-loop-*.log'; break }
        'discord-outbox' { 'discord-outbox-loop-*.log'; break }
        'discord-outbox-loop' { 'discord-outbox-loop-*.log'; break }
        'progress' { 'progress-delivery-loop-*.log'; break }
        'progress-delivery' { 'progress-delivery-loop-*.log'; break }
        'progress-delivery-loop' { 'progress-delivery-loop-*.log'; break }
        'worker' { 'worker-loop-*.log'; break }
        'worker-loop' { 'worker-loop-*.log'; break }
        default { $null }
    }
}

function Tail-ComponentLog {
    param(
        [string] $Component,
        [int] $Lines = 200
    )

    if ($Component.ToLowerInvariant() -eq 'harness') {
        Require-File $HarnessLog "The harness operational log has not been created yet."
        Get-Content -LiteralPath $HarnessLog -Tail $Lines -Wait
        return
    }

    if ($Component.ToLowerInvariant() -in @('progress-events', 'events')) {
        Require-File $ProgressLog "The runtime progress log has not been created yet."
        Get-Content -LiteralPath $ProgressLog -Tail $Lines -Wait
        return
    }

    $filter = Component-To-Filter $Component
    if ($null -eq $filter) {
        throw "Unknown log component: $Component`n$(Show-Help)"
    }
    if (-not (Test-Path -LiteralPath $SupervisorLogs -PathType Container)) {
        throw "Missing supervisor log directory: $SupervisorLogs"
    }

    $log = Get-ChildItem -LiteralPath $SupervisorLogs -Filter $filter |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1

    if ($null -eq $log) {
        throw "No log file matched $filter under $SupervisorLogs"
    }

    Write-Host "Tailing $($log.FullName)"
    Get-Content -LiteralPath $log.FullName -Tail $Lines -Wait
}

if ($CommandArgs.Count -eq 0 -or $CommandArgs[0] -in @('help', '-h', '--help')) {
    Show-Help
    exit 0
}

$scope = $CommandArgs[0].ToLowerInvariant()
if ($scope -notin @('gateway', 'gw')) {
    throw "Unknown command scope: $($CommandArgs[0])`n$(Show-Help)"
}

$action = if ($CommandArgs.Count -ge 2) { $CommandArgs[1].ToLowerInvariant() } else { 'help' }
$liveControlToken = Get-LiveControlTokenArg

switch ($action) {
    'start' {
        Assert-LiveGatewayControlAllowed 'start' $liveControlToken
        Start-LiveSupervisors $liveControlToken
    }
    'stop' {
        Assert-LiveGatewayControlAllowed 'stop' $liveControlToken
        Stop-LiveSupervisors $liveControlToken
    }
    'restart' {
        # Prove both action-specific grants and every start prerequisite before
        # stopping anything, so a missing candidate cannot strand the live set.
        Assert-LiveGatewayControlAllowed 'stop' $liveControlToken
        Assert-LiveGatewayControlAllowed 'start' $liveControlToken
        Require-File $HarnessExe "Deploy .agent-harness\bin\current\agent-harness.exe first."
        Require-File $CodexExe "Deploy the Codex CLI below .agent-harness\tools first."
        Require-File $NodeExe "Install or deploy the pinned Node executable first."
        Require-File $DiscordGateway "Deploy the Discord gateway below .agent-harness\tools first."
        Require-File $KeeperPlan "Generate and install the reconcile keeper before restarting live supervisors."
        Require-File (Join-Path $KeeperScripts 'start.ps1') "Regenerate the reconcile keeper plan before restarting live supervisors."
        Require-File (Join-Path $KeeperScripts 'stop.ps1') "Regenerate the reconcile keeper plan before restarting live supervisors."
        Stop-LiveSupervisors $liveControlToken
        Wait-LiveSupervisorsStopped
        Start-LiveSupervisors $liveControlToken
    }
    'keeper-plan' {
        Write-LiveKeeperPlan $liveControlToken
    }
    'keeper-install' {
        Assert-LiveGatewayControlAllowed 'cutover' $liveControlToken
        Invoke-LiveKeeperScript 'install' $liveControlToken
    }
    'keeper-readback' {
        Invoke-LiveKeeperScript 'readback' $null
    }
    'keeper-uninstall' {
        Assert-LiveGatewayControlAllowed 'uninstall' $liveControlToken
        Invoke-LiveKeeperScript 'uninstall' $liveControlToken
    }
    'status' {
        Show-Status
    }
    'ps' {
        Show-Processes
    }
    'process' {
        Show-Processes
    }
    'processes' {
        Show-Processes
    }
    'logs' {
        Show-Logs
    }
    'log' {
        Show-Logs
    }
    'tail' {
        $component = if ($CommandArgs.Count -ge 3) { $CommandArgs[2] } else { 'runtime' }
        $lines = 200
        if ($CommandArgs.Count -ge 4) {
            if (-not [int]::TryParse($CommandArgs[3], [ref] $lines)) {
                throw "lines must be an integer: $($CommandArgs[3])"
            }
        }
        Tail-ComponentLog $component $lines
    }
    'help' {
        Show-Help
    }
    default {
        throw "Unknown gateway action: $action`n$(Show-Help)"
    }
}
