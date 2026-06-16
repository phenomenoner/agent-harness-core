[CmdletBinding(PositionalBinding = $false)]
param(
    [string]$HarnessHome = ".\.agent-harness",
    [string]$Command = "openclaw-mem",
    [Parameter(Position = 0, ValueFromRemainingArguments = $true)]
    [string[]]$OpenClawMemArgs
)

$ErrorActionPreference = "Stop"

function Read-EnvFile {
    param([string]$Path)

    $values = @{}
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        return $values
    }

    foreach ($line in Get-Content -LiteralPath $Path) {
        $trimmed = $line.Trim()
        if ($trimmed.Length -eq 0 -or $trimmed.StartsWith("#")) {
            continue
        }
        $parts = $trimmed -split "=", 2
        if ($parts.Count -ne 2) {
            continue
        }
        $key = $parts[0].Trim()
        $value = $parts[1].Trim()
        $value = Unquote-EnvValue -Value $value
        $values[$key] = $value
    }
    return $values
}

function Unquote-EnvValue {
    param([string]$Value)

    $trimmed = $Value.Trim()
    if ($trimmed.Length -ge 2 -and $trimmed.StartsWith('"') -and $trimmed.EndsWith('"')) {
        try {
            return ($trimmed | ConvertFrom-Json -ErrorAction Stop)
        } catch {
            return $trimmed.Trim('"')
        }
    }
    return $trimmed
}

function Expand-EnvReference {
    param([string]$Value)

    $trimmed = $Value.Trim()
    if ($trimmed.StartsWith('${') -and $trimmed.EndsWith('}') -and $trimmed.Length -gt 3) {
        $name = $trimmed.Substring(2, $trimmed.Length - 3)
        $expanded = [Environment]::GetEnvironmentVariable($name)
        if (-not [string]::IsNullOrWhiteSpace($expanded)) {
            return $expanded
        }
    }
    return $trimmed
}

function Get-BridgeValue {
    param(
        [hashtable]$Values,
        [string]$Name,
        [string]$DefaultValue = ""
    )

    $fromProcess = [Environment]::GetEnvironmentVariable($Name)
    if (-not [string]::IsNullOrWhiteSpace($fromProcess)) {
        return $fromProcess
    }
    if ($Values.ContainsKey($Name) -and -not [string]::IsNullOrWhiteSpace($Values[$Name])) {
        return (Expand-EnvReference -Value $Values[$Name])
    }
    return $DefaultValue
}

$resolvedHarnessHome = if (Test-Path -LiteralPath $HarnessHome) {
    (Resolve-Path -LiteralPath $HarnessHome).Path
} else {
    $HarnessHome
}

$envFile = Join-Path $resolvedHarnessHome "secrets\memory-credentials.env"
$values = Read-EnvFile -Path $envFile

$apiKeyName = "AGENT_HARNESS_MEMORY_EMBEDDING_API_KEY"
$baseUrlName = "AGENT_HARNESS_MEMORY_EMBEDDING_BASE_URL"
$modelName = "AGENT_HARNESS_MEMORY_EMBEDDING_MODEL"

$apiKey = Get-BridgeValue -Values $values -Name $apiKeyName
if ([string]::IsNullOrWhiteSpace($apiKey)) {
    throw "$apiKeyName is missing or empty in process env and $envFile"
}

$baseUrl = (Get-BridgeValue -Values $values -Name $baseUrlName -DefaultValue "https://api.openai.com/v1").TrimEnd("/")
$model = Get-BridgeValue -Values $values -Name $modelName -DefaultValue "text-embedding-3-small"

$env:OPENAI_API_KEY = $apiKey
$env:OPENAI_BASE_URL = $baseUrl
$env:OPENAI_API_BASE = $baseUrl
$env:AGENT_HARNESS_MEMORY_EMBEDDING_MODEL = $model
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"

& $Command @OpenClawMemArgs
exit $LASTEXITCODE
