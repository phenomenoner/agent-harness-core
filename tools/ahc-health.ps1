# No explicit service/WSL recovery. Entering a stopped distro can run its enabled services.
function Invoke-AhcBoundedRead {
    param([string]$FileName, [string]$Arguments, [int]$TimeoutSeconds)
    $timer = [Diagnostics.Stopwatch]::StartNew()
    $process = New-Object Diagnostics.Process
    $process.StartInfo.FileName = $FileName
    $process.StartInfo.Arguments = $Arguments
    $process.StartInfo.UseShellExecute = $false
    $process.StartInfo.CreateNoWindow = $true
    $process.StartInfo.RedirectStandardOutput = $true
    $process.StartInfo.RedirectStandardError = $true
    $process.StartInfo.StandardOutputEncoding = [Text.Encoding]::UTF8
    $started = $false
    try {
        $started = $process.Start()
        # Retain the handle; never reopen a PID to terminate the diagnostic client.
        $null = $process.Handle
        $stdout = $process.StandardOutput.ReadToEndAsync()
        $stderr = $process.StandardError.ReadToEndAsync()
        $remaining = [Math]::Max(0, $TimeoutSeconds * 1000 - $timer.ElapsedMilliseconds)
        if (-not $process.WaitForExit([int]$remaining)) {
            return @{ category = 'observation-timeout'; output = '' }
        }
        # A descendant may retain a pipe after the direct client exits. Reading it
        # must share the original deadline rather than block on ReadToEnd forever.
        $remaining = [Math]::Max(0, $TimeoutSeconds * 1000 - $timer.ElapsedMilliseconds)
        $reads = [Threading.Tasks.Task[]]@($stdout, $stderr)
        if (-not [Threading.Tasks.Task]::WaitAll($reads, [int]$remaining)) {
            return @{ category = 'observation-timeout'; output = '' }
        }
        if ($process.ExitCode -ne 0) {
            return @{ category = 'probe-failed'; output = '' }
        }
        if ($stdout.Result.Length -gt 16384) {
            return @{ category = 'invalid-observation'; output = '' }
        }
        return @{ category = 'observed'; output = $stdout.Result }
    } catch {
        # Do not disclose raw WSL errors, environment data or exception payloads.
        return @{ category = 'probe-failed'; output = '' }
    } finally {
        if ($started) {
            try {
                if (-not $process.HasExited) {
                    $process.Kill()
                    $null = $process.WaitForExit(1000)
                }
            } catch { }
        }
        $process.Dispose()
    }
}

function ConvertTo-AhcUnitHealth {
    param([hashtable]$Probe, [string]$Unit)
    $result = [ordered]@{
        observation = $Probe.category
        unitState = 'unknown'
        receiverReadiness = 'unknown'
        backendReadiness = 'unknown'
        memoryReadiness = 'unknown'
        reason = $Probe.category
    }
    if ($Probe.category -ne 'observed') { return $result }
    $fields = @{}
    foreach ($line in ($Probe.output -split '\r?\n')) {
        if ($line -eq '') { continue }
        $parts = $line -split '=', 2
        if ($parts.Count -ne 2 -or $fields.ContainsKey($parts[0])) {
            $result.observation = 'invalid-observation'
            $result.reason = 'invalid-observation'
            return $result
        }
        $fields[$parts[0]] = $parts[1]
    }
    $required = @('Id', 'LoadState', 'ActiveState', 'SubState', 'MainPID', 'InvocationID')
    $valid = $fields.Count -eq $required.Count
    foreach ($key in $required) { $valid = $valid -and $fields.ContainsKey($key) }
    $valid = $valid -and $fields.Id -ceq $Unit
    $valid = $valid -and $fields.MainPID -cmatch '^(0|[1-9][0-9]{0,9})$'
    $valid = $valid -and $fields.InvocationID -cmatch '^([a-f0-9]{32})?$'
    $valid = $valid -and $fields.LoadState -cin @('loaded', 'not-found', 'masked', 'error', 'bad-setting')
    $valid = $valid -and $fields.ActiveState -cin @('active', 'inactive', 'failed', 'activating', 'deactivating', 'reloading', 'maintenance', 'refreshing')
    $valid = $valid -and $fields.SubState -cmatch '^[a-z][a-z-]{0,63}$'
    if (-not $valid) {
        $result.observation = 'invalid-observation'
        $result.reason = 'invalid-observation'
        return $result
    }
    if ($fields.LoadState -ne 'loaded') {
        $result.unitState = 'unavailable'
    } elseif ($fields.ActiveState -eq 'active' -and $fields.SubState -eq 'running' -and
              $fields.MainPID -ne '0' -and $fields.InvocationID -match '[1-9a-f]') {
        $result.unitState = 'running'
    } elseif ($fields.ActiveState -in @('inactive', 'failed')) {
        $result.unitState = $fields.ActiveState
    } else {
        $result.unitState = 'not-ready'
    }
    $result.reason = 'unit-observation-only'
    return $result
}

function Invoke-AhcUnitHealth {
    param([string]$Distribution, [string]$LinuxUser, [string]$Unit, [int]$TimeoutSeconds)
    # These arguments are validated by the operator entry before reaching this helper.
    $arguments = "--distribution $Distribution --user $LinuxUser --cd / --exec /usr/bin/systemctl --user show --no-pager --property=Id,LoadState,ActiveState,SubState,MainPID,InvocationID $Unit"
    $timer = [Diagnostics.Stopwatch]::StartNew()
    $probe = Invoke-AhcBoundedRead -FileName "$env:SystemRoot/System32/wsl.exe" -Arguments $arguments -TimeoutSeconds $TimeoutSeconds
    $result = ConvertTo-AhcUnitHealth -Probe $probe -Unit $Unit
    $result.schema = 'ahc.operator-health.v1'
    $result.observationScope = 'windows-operator'
    $result.sampledAt = [DateTime]::UtcNow.ToString('o')
    $result.durationMs = $timer.ElapsedMilliseconds
    $result.validForSeconds = 120
    $result.distribution = $Distribution
    $result.linuxUser = $LinuxUser
    $result.unit = $Unit
    return $result
}
