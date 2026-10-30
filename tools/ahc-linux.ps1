param(
    [Parameter(Mandatory=$true)]
    [ValidateSet('start','status','stop','restart','logs','health')]
    [string]$Action,
    [Parameter(Mandatory=$true)]
    [ValidatePattern('^ahc-[A-Za-z0-9][A-Za-z0-9_.-]*\.service$')]
    [string]$Unit,
    [Parameter(Mandatory=$true)]
    [ValidatePattern('^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$')]
    [string]$Distribution,
    [Parameter(Mandatory=$true)]
    [ValidatePattern('^[a-z_][a-z0-9_-]{0,31}$')]
    [string]$LinuxUser,
    [Alias('TimeoutSeconds')]
    [ValidateRange(1,60)]
    [int]$HealthTimeoutSeconds = 15
)
$ErrorActionPreference = 'Stop'
if ($LinuxUser -eq 'root') { throw 'AHC user services require a non-root Linux user' }
. "$PSScriptRoot/ahc-health.ps1"
if ($Action -in @('health', 'status')) {
    $health = Invoke-AhcUnitHealth -Distribution $Distribution -LinuxUser $LinuxUser -Unit $Unit -TimeoutSeconds $HealthTimeoutSeconds
    $health['action'] = $Action
    $health | ConvertTo-Json -Depth 3 -Compress
    if ($health.observation -eq 'observed') { exit 0 }
    exit 2
}
# Windows is only an operator entrance. All state and process control stay in Linux.
# Explicit argv, one distribution, one named unit; no shell or global WSL control.
$entryArguments = @('--distribution', $Distribution, '--user', $LinuxUser, '--cd', '/', '--exec')
if ($Action -eq 'logs') {
    $entryArguments += @('/usr/bin/journalctl', '--user', '--unit', $Unit, '--lines', '100', '--no-pager')
} else {
    $entryArguments += @('/usr/bin/systemctl', '--user', $Action, '--no-pager', $Unit)
}
$probe = Invoke-AhcBoundedRead -FileName "$env:SystemRoot/System32/wsl.exe" -Arguments ($entryArguments -join ' ') -TimeoutSeconds $HealthTimeoutSeconds
if ($probe.category -eq 'observed') {
    if ($probe.output -ne '') { Write-Output $probe.output }
    exit 0
}
# A transport failure cannot establish Linux liveness or whether a control committed.
# Report only this entrance; never retry, replay a command or recover a distribution.
$observation = ConvertTo-AhcUnitHealth -Probe $probe -Unit $Unit
$observation['schema'] = 'ahc.operator-observation.v1'
$observation['action'] = $Action
$observation['observationScope'] = 'windows-operator'
$observation['sampledAt'] = [DateTime]::UtcNow.ToString('o')
$observation['distribution'] = $Distribution
$observation['linuxUser'] = $LinuxUser
$observation['unit'] = $Unit
if ($Action -in @('start', 'stop', 'restart')) {
    $observation['controlOutcome'] = 'unknown'
}
$observation | ConvertTo-Json -Depth 3 -Compress
exit 2
