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
$NodeExe = 'C:\Program Files\nodejs\node.exe'
$SupervisorLogs = Join-Path $HarnessHome 'state\logs\supervisor'
$HarnessLog = Join-Path $HarnessHome 'state\logs\harness.jsonl'
$ProgressLog = Join-Path $HarnessHome 'state\runtime-queue\progress-events.jsonl'

function Show-Help {
    @"
Usage:
  harness gateway start
  harness gateway stop
  harness gateway restart
  harness gateway status
  harness gateway ps
  harness gateway logs
  harness gateway tail <component> [lines]
  harness gateway stop --live-control-token <token>

Components for tail:
  runtime, telegram, discord, discord-gateway, discord-outbox, progress, worker, harness, progress-events

Examples:
  harness gateway restart
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
    $args = @('ops-control', '--harness-home', $HarnessHome, '--action', $Action,
        '--reason', "harness.ps1 gateway $Action")
    if (-not [string]::IsNullOrWhiteSpace($LiveControlToken)) {
        $args += @('--live-control-token', $LiveControlToken)
    }
    & $HarnessExe @args
    if ($LASTEXITCODE -ne 0) {
        throw "ops-control $Action failed with exit code $LASTEXITCODE"
    }
}

function Start-LiveSupervisors {
    param([string] $LiveControlToken)
    Require-File $HarnessExe "Deploy .agent-harness\bin\current\agent-harness.exe first."
    Require-File $CodexExe "Deploy the Codex CLI below .agent-harness\tools first."
    Require-File $DiscordGateway "Deploy the Discord gateway below .agent-harness\tools first."
    Invoke-LiveOpsControl 'start' $LiveControlToken
    & $HarnessExe supervisor-reconcile --harness-home $HarnessHome --source-home $HarnessHome `
        --workspace $AgentWorkspace --runtime-workspace $RuntimeWorkspace --harness-cli $HarnessExe `
        --codex-exe $CodexExe --node-exe $NodeExe --gateway-script $DiscordGateway --all --apply
    if ($LASTEXITCODE -ne 0) {
        throw "supervisor-reconcile --apply failed with exit code $LASTEXITCODE"
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

function Test-LiveFlag {
    param([string] $Value)
    if ([string]::IsNullOrWhiteSpace($Value)) {
        return $false
    }
    return @('1', 'true', 'yes', 'on', 'live') -contains $Value.Trim().ToLowerInvariant()
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
    if (-not (Test-LiveFlag $env:AGENT_HARNESS_LIVE_SESSION)) {
        return
    }
    if ([string]::IsNullOrWhiteSpace($LiveControlToken)) {
        throw "live-control token is required for live gateway $Action"
    }
    Require-File $HarnessExe "Deploy .agent-harness\bin\current\agent-harness.exe first. Cargo target\ is source/staging output only."
    $status = & $HarnessExe ops-cutover-status --harness-home $HarnessHome --action $Action --live-control-token $LiveControlToken | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0 -or $status.status -ne 'ready') {
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
        Invoke-LiveOpsControl 'stop' $liveControlToken
    }
    'restart' {
        Assert-LiveGatewayControlAllowed 'restart' $liveControlToken
        Invoke-LiveOpsControl 'stop' $liveControlToken
        Wait-LiveSupervisorsStopped
        Start-LiveSupervisors $liveControlToken
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
