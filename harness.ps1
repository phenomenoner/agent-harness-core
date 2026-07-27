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
$HarnessConfig = Join-Path $HarnessHome 'harness-config.json'
$DirectOwnerHostRoot = Join-Path $HarnessHome 'state\supervisor\direct-owner-host'
$DirectOwnerHostPlan = Join-Path $DirectOwnerHostRoot 'host-plan.json'
$DirectOwnerHostScript = Join-Path $HarnessHome 'tools\supervisor-direct-owner\launch-direct-owner-hidden.ps1'
$DirectOwnerMasterState = Join-Path $HarnessHome 'state\supervisor\master\master-state.json'
$SupervisorStopRoot = Join-Path $HarnessHome 'state\supervisor\stop'
$SupervisorLogs = Join-Path $HarnessHome 'state\logs\supervisor'
$HarnessLog = Join-Path $HarnessHome 'state\logs\harness.jsonl'
$ProgressLog = Join-Path $HarnessHome 'state\runtime-queue\progress-events.jsonl'

function Show-Help {
    @"
Usage:
  harness gateway start [--approve-live-control | --live-control-token <token>]
  harness gateway stop [--approve-live-control | --live-control-token <token>]
  harness gateway restart [--approve-live-control | --live-control-token <restart-token>]
  harness gateway status
  harness gateway ps
  harness gateway logs
  harness gateway tail <component> [lines]
  harness gateway keeper-plan --live-control-token <token>
  harness gateway keeper-install --live-control-token <token>
  harness gateway keeper-readback
  harness gateway keeper-uninstall --live-control-token <token>

Components for tail:
  runtime, telegram, discord, discord-gateway, discord-outbox, progress, worker, harness, progress-events

Examples:
  harness gateway restart
  harness gateway restart --approve-live-control
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

function Get-LiveOwnershipMode {
    Require-File $HarnessConfig 'Deploy the authoritative harness configuration first.'
    $config = Get-Content -LiteralPath $HarnessConfig -Raw | ConvertFrom-Json -ErrorAction Stop
    $mode = [string]$config.supervisor.ownershipMode
    if ($mode -notin @('compatibility', 'direct-owner')) {
        throw "Unsupported live supervisor ownership mode: $mode"
    }
    return $mode
}

function Get-ScheduledTaskActionField {
    param(
        [AllowNull()][object] $Action,
        [Parameter(Mandatory = $true)][string] $Name
    )
    if ($null -eq $Action) {
        return $null
    }
    $property = $Action.PSObject.Properties[$Name]
    if ($null -eq $property) {
        return $null
    }
    return [string]$property.Value
}

function Get-ExactDirectOwnerHostTask {
    Require-File $DirectOwnerHostPlan 'Deploy the direct-owner host plan first.'
    Require-File $DirectOwnerHostScript 'Deploy the direct-owner hidden host adapter first.'
    $planSha256 = (Get-FileHash -LiteralPath $DirectOwnerHostPlan -Algorithm SHA256).Hash.ToLowerInvariant()
    $windowsPowerShell = Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
    $expectedArguments = (
        '-NoLogo -NoProfile -NonInteractive -ExecutionPolicy Bypass -WindowStyle Hidden ' +
        "-File `"$DirectOwnerHostScript`" -PlanFile `"$DirectOwnerHostPlan`" " +
        "-ExpectedPlanSha256 $planSha256"
    )
    $matches = @(
        Get-ScheduledTask -ErrorAction Stop |
            Where-Object {
                $actions = @($_.Actions)
                $actions.Count -eq 1 -and
                [string]::Equals(
                    (Get-ScheduledTaskActionField -Action $actions[0] -Name 'Execute'),
                    $windowsPowerShell,
                    [StringComparison]::OrdinalIgnoreCase
                ) -and
                [string]::Equals(
                    (Get-ScheduledTaskActionField -Action $actions[0] -Name 'Arguments'),
                    $expectedArguments,
                    [StringComparison]::Ordinal
                ) -and
                [string]::Equals(
                    (Get-ScheduledTaskActionField -Action $actions[0] -Name 'WorkingDirectory'),
                    $RuntimeWorkspace,
                    [StringComparison]::OrdinalIgnoreCase
                )
            }
    )
    if ($matches.Count -ne 1) {
        throw "Expected exactly one hash-bound direct-owner host task; observed $($matches.Count)"
    }
    return $matches[0]
}

function Write-DirectOwnerContainmentStop {
    param(
        [string] $Reason = 'direct-owner launcher start containment'
    )
    [IO.Directory]::CreateDirectory($SupervisorStopRoot) | Out-Null
    $path = Join-Path $SupervisorStopRoot 'supervisor-master.stop'
    Assert-DirectOwnerLauncherMasterStopOwnership
    $temp = "$path.$PID.$([Guid]::NewGuid().ToString('N')).tmp"
    try {
        $value = [ordered]@{
            schema = 'agent-harness.supervisor-stop-file.v1'
            serviceId = 'supervisor-master'
            persistent = $true
            reason = $Reason
        }
        [IO.File]::WriteAllText(
            $temp,
            (ConvertTo-Json -InputObject $value -Depth 8) + "`n",
            [Text.UTF8Encoding]::new($false)
        )
        Move-Item -LiteralPath $temp -Destination $path -Force
    }
    finally {
        Remove-Item -LiteralPath $temp -Force -ErrorAction SilentlyContinue
    }
}

function Assert-DirectOwnerLauncherMasterStopOwnership {
    $path = Join-Path $SupervisorStopRoot 'supervisor-master.stop'
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
        return
    }
    $stop = Get-Content -LiteralPath $path -Raw | ConvertFrom-Json -ErrorAction Stop
    if (
        [string]$stop.schema -ne 'agent-harness.supervisor-stop-file.v1' -or
        [string]$stop.serviceId -ne 'supervisor-master' -or
        -not ([string]$stop.reason).StartsWith(
            'direct-owner launcher ',
            [StringComparison]::Ordinal
        )
    ) {
        throw 'refusing to alter an unrecognized supervisor-master hold'
    }
}

function Remove-DirectOwnerLauncherMasterStop {
    $path = Join-Path $SupervisorStopRoot 'supervisor-master.stop'
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
        return
    }
    Assert-DirectOwnerLauncherMasterStopOwnership
    Remove-Item -LiteralPath $path -Force -ErrorAction Stop
}

function Assert-DirectOwnerReconcileFence {
    $path = Join-Path $SupervisorStopRoot 'supervisor-reconcile.stop'
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
        throw 'direct-owner compatibility reconcile fence is missing'
    }
    $stop = Get-Content -LiteralPath $path -Raw | ConvertFrom-Json -ErrorAction Stop
    if (
        [string]$stop.schema -ne 'agent-harness.supervisor-stop-file.v1' -or
        [string]$stop.serviceId -ne 'supervisor-reconcile' -or
        $stop.persistent -ne $true
    ) {
        throw 'direct-owner compatibility reconcile fence is malformed'
    }
}

function Write-DirectOwnerReconcileFence {
    [IO.Directory]::CreateDirectory($SupervisorStopRoot) | Out-Null
    $path = Join-Path $SupervisorStopRoot 'supervisor-reconcile.stop'
    if (Test-Path -LiteralPath $path -PathType Leaf) {
        Assert-DirectOwnerReconcileFence
        return
    }
    $temp = "$path.$PID.$([Guid]::NewGuid().ToString('N')).tmp"
    try {
        $value = [ordered]@{
            schema = 'agent-harness.supervisor-stop-file.v1'
            serviceId = 'supervisor-reconcile'
            persistent = $true
            reason = 'direct-owner launcher compatibility fence'
            createdBy = 'harness.ps1'
            createdAtMs = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
        }
        [IO.File]::WriteAllText(
            $temp,
            (ConvertTo-Json -InputObject $value -Depth 8) + "`n",
            [Text.UTF8Encoding]::new($false)
        )
        try {
            Move-Item -LiteralPath $temp -Destination $path -ErrorAction Stop
        }
        catch {
            if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
                throw
            }
        }
        Assert-DirectOwnerReconcileFence
    }
    finally {
        Remove-Item -LiteralPath $temp -Force -ErrorAction SilentlyContinue
    }
}

function Start-DirectOwnerHost {
    param([string] $LiveControlToken)
    $task = Get-ExactDirectOwnerHostTask
    try {
        Assert-DirectOwnerLauncherMasterStopOwnership
        Invoke-LiveOpsControl 'start' $LiveControlToken
        Write-DirectOwnerReconcileFence
        Remove-DirectOwnerLauncherMasterStop
        Enable-ScheduledTask -TaskPath $task.TaskPath -TaskName $task.TaskName -ErrorAction Stop | Out-Null
        Start-ScheduledTask -TaskPath $task.TaskPath -TaskName $task.TaskName -ErrorAction Stop
        $deadline = [DateTime]::UtcNow.AddSeconds(90)
        do {
            Start-Sleep -Milliseconds 250
            $task = Get-ScheduledTask -TaskPath $task.TaskPath -TaskName $task.TaskName -ErrorAction Stop
            if (
                [string]$task.State -eq 'Running' -and
                (Test-Path -LiteralPath $DirectOwnerMasterState -PathType Leaf)
            ) {
                $state = Get-Content -LiteralPath $DirectOwnerMasterState -Raw |
                    ConvertFrom-Json -ErrorAction Stop
                $master = @(
                    Get-CimInstance Win32_Process -Filter "name = 'agent-harness.exe'" |
                        Where-Object {
                            [int]$_.ProcessId -eq [int]$state.pid -and
                            [string]::Equals(
                                [string]$_.ExecutablePath,
                                $HarnessExe,
                                [StringComparison]::OrdinalIgnoreCase
                            )
                        }
                )
                if (
                    [string]$state.schema -eq 'agent-harness.supervisor-master-state.v1' -and
                    [string]$state.phase -eq 'running' -and
                    [string]$state.mode -eq 'direct-owner' -and
                    $master.Count -eq 1
                ) {
                    Assert-DirectOwnerReconcileFence
                    return
                }
            }
        } while ([DateTime]::UtcNow -lt $deadline)
        throw 'Timed out waiting for the exact direct-owner master generation'
    }
    catch {
        $startError = $_
        Disable-ScheduledTask -TaskPath $task.TaskPath -TaskName $task.TaskName -ErrorAction SilentlyContinue |
            Out-Null
        Stop-ScheduledTask -TaskPath $task.TaskPath -TaskName $task.TaskName -ErrorAction SilentlyContinue
        try {
            Write-DirectOwnerContainmentStop
        } catch {
            # A foreign or malformed hold already provides fail-closed containment.
        }
        throw $startError
    }
}

function Stop-DirectOwnerHost {
    param([string] $LiveControlToken)
    $task = Get-ExactDirectOwnerHostTask
    Assert-DirectOwnerLauncherMasterStopOwnership
    Invoke-LiveOpsControl 'stop' $LiveControlToken
    Write-DirectOwnerContainmentStop 'direct-owner launcher stop'
    Disable-ScheduledTask -TaskPath $task.TaskPath -TaskName $task.TaskName -ErrorAction Stop | Out-Null
    Stop-ScheduledTask -TaskPath $task.TaskPath -TaskName $task.TaskName -ErrorAction SilentlyContinue
}

function Start-LiveSupervisors {
    param([string] $LiveControlToken)
    Require-File $HarnessExe "Deploy .agent-harness\bin\current\agent-harness.exe first."
    Require-File $CodexExe "Deploy the Codex CLI below .agent-harness\tools first."
    Require-File $NodeExe "Install or deploy the pinned Node executable first."
    Require-File $DiscordGateway "Deploy the Discord gateway below .agent-harness\tools first."
    if ((Get-LiveOwnershipMode) -eq 'direct-owner') {
        Start-DirectOwnerHost $LiveControlToken
        return
    }
    Require-File $KeeperPlan "Generate and install the reconcile keeper before starting live supervisors."
    Require-File (Join-Path $KeeperScripts 'start.ps1') "Regenerate the reconcile keeper plan before starting live supervisors."
    Invoke-LiveKeeperScript 'start' $LiveControlToken
}

function Stop-LiveSupervisors {
    param([string] $LiveControlToken)
    if ((Get-LiveOwnershipMode) -eq 'direct-owner') {
        Stop-DirectOwnerHost $LiveControlToken
        return
    }
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

function Test-LiveControlApprovalFlag {
    return $CommandArgs -contains '--approve-live-control'
}

function Confirm-LiveGatewayControl {
    param([ValidateSet('start', 'stop', 'restart')][string] $Action)
    if (Test-LiveControlApprovalFlag) {
        return
    }
    $inputRedirected = $false
    try {
        $inputRedirected = [Console]::IsInputRedirected
    } catch {
        $inputRedirected = $true
    }
    if ($inputRedirected) {
        throw "live gateway $Action requires an explicit token or --approve-live-control in non-interactive mode"
    }
    $expected = $Action.ToUpperInvariant()
    $confirmation = Read-Host "Type $expected to authorize live gateway $Action"
    if (-not [string]::Equals($confirmation, $expected, [StringComparison]::Ordinal)) {
        throw "live gateway $Action was not authorized"
    }
}

function New-EphemeralLiveControlToken {
    param([ValidateSet('start', 'stop')][string] $Action)
    Require-File $HarnessExe "Deploy .agent-harness\bin\current\agent-harness.exe first. Cargo target\ is source/staging output only."
    $requestOutput = & $HarnessExe ops-cutover-request --harness-home $HarnessHome `
        --action $Action --summary "interactive harness gateway $Action" `
        --test-note 'operator-confirmed launcher control' 2>&1
    $requestExitCode = $LASTEXITCODE
    if ($requestExitCode -ne 0) {
        throw "live-control request failed for live gateway $Action with exit code $requestExitCode"
    }
    try {
        $request = ($requestOutput -join [Environment]::NewLine) | ConvertFrom-Json -ErrorAction Stop
    } catch {
        throw "live-control request returned invalid JSON for live gateway $Action"
    }
    $requestExact = ([string]$request.schema -eq 'agent-harness.ops-cutover-request.v1') -and
        ([string]$request.status -eq 'requested') -and
        ([string]$request.action -eq $Action) -and
        (-not [string]::IsNullOrWhiteSpace([string]$request.ticketId)) -and
        [string]::Equals([string]$request.harnessHome, [string]$HarnessHome, [StringComparison]::OrdinalIgnoreCase)
    if (-not $requestExact) {
        throw "live-control request was not exact for live gateway $Action"
    }

    $operator = if (-not [string]::IsNullOrWhiteSpace($env:USERNAME)) {
        if (-not [string]::IsNullOrWhiteSpace($env:USERDOMAIN)) {
            "$($env:USERDOMAIN)\$($env:USERNAME)"
        } else {
            $env:USERNAME
        }
    } else {
        'interactive-operator'
    }
    $approvalOutput = & $HarnessExe ops-cutover-approve --harness-home $HarnessHome `
        --ticket-id ([string]$request.ticketId) --action $Action --issued-to $operator `
        --ttl-seconds 300 --reason "operator-confirmed harness gateway $Action" 2>&1
    $approvalExitCode = $LASTEXITCODE
    if ($approvalExitCode -ne 0) {
        throw "live-control approval failed for live gateway $Action with exit code $approvalExitCode"
    }
    try {
        $approval = ($approvalOutput -join [Environment]::NewLine) | ConvertFrom-Json -ErrorAction Stop
    } catch {
        # Approval output can contain a bearer token, so never reflect it.
        throw "live-control approval returned invalid JSON for live gateway $Action"
    }
    $approvalExact = ([string]$approval.schema -eq 'agent-harness.ops-cutover-approval.v1') -and
        ([string]$approval.status -eq 'approved') -and
        ([string]$approval.action -eq $Action) -and
        ([string]$approval.ticketId -eq [string]$request.ticketId) -and
        (-not [string]::IsNullOrWhiteSpace([string]$approval.token)) -and
        [string]::Equals([string]$approval.harnessHome, [string]$HarnessHome, [StringComparison]::OrdinalIgnoreCase)
    if (-not $approvalExact) {
        throw "live-control approval was not exact for live gateway $Action"
    }
    return [string]$approval.token
}

function Resolve-LiveGatewayControlToken {
    param(
        [ValidateSet('start', 'stop')][string] $Action,
        [string] $ExistingToken,
        [switch] $AlreadyConfirmed
    )
    if (-not [string]::IsNullOrWhiteSpace($ExistingToken)) {
        return $ExistingToken
    }
    if (-not $AlreadyConfirmed) {
        Confirm-LiveGatewayControl $Action
    }
    return New-EphemeralLiveControlToken $Action
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
        $liveControlToken = Resolve-LiveGatewayControlToken 'start' $liveControlToken
        Assert-LiveGatewayControlAllowed 'start' $liveControlToken
        Start-LiveSupervisors $liveControlToken
    }
    'stop' {
        $liveControlToken = Resolve-LiveGatewayControlToken 'stop' $liveControlToken
        Assert-LiveGatewayControlAllowed 'stop' $liveControlToken
        Stop-LiveSupervisors $liveControlToken
    }
    'restart' {
        # A supplied restart grant authorizes deriving short-lived exact stop
        # and start grants. Without one, obtain one operator confirmation and
        # issue both exact grants; keeper scripts intentionally validate exact
        # actions before mutating anything.
        if (-not [string]::IsNullOrWhiteSpace($liveControlToken)) {
            Assert-LiveGatewayControlAllowed 'restart' $liveControlToken
        } else {
            Confirm-LiveGatewayControl 'restart'
        }
        $stopLiveControlToken = New-EphemeralLiveControlToken 'stop'
        $startLiveControlToken = New-EphemeralLiveControlToken 'start'
        Require-File $HarnessExe "Deploy .agent-harness\bin\current\agent-harness.exe first."
        Require-File $CodexExe "Deploy the Codex CLI below .agent-harness\tools first."
        Require-File $NodeExe "Install or deploy the pinned Node executable first."
        Require-File $DiscordGateway "Deploy the Discord gateway below .agent-harness\tools first."
        Assert-LiveGatewayControlAllowed 'stop' $stopLiveControlToken
        Assert-LiveGatewayControlAllowed 'start' $startLiveControlToken
        Stop-LiveSupervisors $stopLiveControlToken
        Wait-LiveSupervisorsStopped
        Start-LiveSupervisors $startLiveControlToken
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
