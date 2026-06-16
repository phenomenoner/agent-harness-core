$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$wrapper = Join-Path $repoRoot "tools\openclaw-mem-env.ps1"
$root = Join-Path ([System.IO.Path]::GetTempPath()) ("agent-harness-openclaw-mem-env-test-" + [guid]::NewGuid().ToString("n"))
$bin = Join-Path $root "bin"
$out = Join-Path $root "capture.txt"

function Assert-Line {
    param(
        [string[]]$Lines,
        [string]$Expected
    )
    if ($Lines -notcontains $Expected) {
        throw "expected line not found: $Expected`nactual:`n$($Lines -join "`n")"
    }
}

function Install-FakeOpenClawMem {
    New-Item -ItemType Directory -Force -Path $bin | Out-Null
    @'
@echo off
(
echo args=%*
echo key=%OPENAI_API_KEY%
echo base=%OPENAI_BASE_URL%
echo apibase=%OPENAI_API_BASE%
echo model=%AGENT_HARNESS_MEMORY_EMBEDDING_MODEL%
echo py=%PYTHONUTF8%
echo enc=%PYTHONIOENCODING%
) > "%OPENCLAW_MEM_ENV_TEST_OUT%"
exit /b 0
'@ | Set-Content -LiteralPath (Join-Path $bin "openclaw-mem.cmd") -Encoding ascii
}

try {
    Install-FakeOpenClawMem
    $env:PATH = "$bin;$env:PATH"
    $env:OPENCLAW_MEM_ENV_TEST_OUT = $out

    $harness = Join-Path $root "harness"
    New-Item -ItemType Directory -Force -Path (Join-Path $harness "secrets") | Out-Null
    $env:OPENCLAW_MEM_ENV_TEST_KEY = "sk-wrapper-test"
    @'
AGENT_HARNESS_MEMORY_EMBEDDING_API_KEY=${OPENCLAW_MEM_ENV_TEST_KEY}
AGENT_HARNESS_MEMORY_EMBEDDING_MODEL="text-embedding-3-small"
AGENT_HARNESS_MEMORY_EMBEDDING_BASE_URL="https://api.openai.com/v1/"
'@ | Set-Content -LiteralPath (Join-Path $harness "secrets\memory-credentials.env") -Encoding utf8NoBOM

    & $wrapper -HarnessHome $harness pack --db smoke.sqlite --json
    if ($LASTEXITCODE -ne 0) {
        throw "wrapper exited with $LASTEXITCODE for env-file case"
    }
    $lines = Get-Content -LiteralPath $out
    Assert-Line $lines "args=pack --db smoke.sqlite --json"
    Assert-Line $lines "key=sk-wrapper-test"
    Assert-Line $lines "base=https://api.openai.com/v1"
    Assert-Line $lines "apibase=https://api.openai.com/v1"
    Assert-Line $lines "model=text-embedding-3-small"
    Assert-Line $lines "py=1"
    Assert-Line $lines "enc=utf-8"

    $missingHarness = Join-Path $root "missing-harness"
    $env:AGENT_HARNESS_MEMORY_EMBEDDING_API_KEY = "sk-env-fallback"
    $env:AGENT_HARNESS_MEMORY_EMBEDDING_MODEL = "fallback-model"
    $env:AGENT_HARNESS_MEMORY_EMBEDDING_BASE_URL = "https://example.invalid/v1/"
    & $wrapper -HarnessHome $missingHarness status --json
    if ($LASTEXITCODE -ne 0) {
        throw "wrapper exited with $LASTEXITCODE for process-env fallback case"
    }
    $lines = Get-Content -LiteralPath $out
    Assert-Line $lines "args=status --json"
    Assert-Line $lines "key=sk-env-fallback"
    Assert-Line $lines "base=https://example.invalid/v1"
    Assert-Line $lines "apibase=https://example.invalid/v1"
    Assert-Line $lines "model=fallback-model"

    "openclaw-mem-env-tests-ok"
} finally {
    Remove-Item Env:\OPENCLAW_MEM_ENV_TEST_OUT -ErrorAction SilentlyContinue
    Remove-Item Env:\OPENCLAW_MEM_ENV_TEST_KEY -ErrorAction SilentlyContinue
    Remove-Item Env:\AGENT_HARNESS_MEMORY_EMBEDDING_API_KEY -ErrorAction SilentlyContinue
    Remove-Item Env:\AGENT_HARNESS_MEMORY_EMBEDDING_MODEL -ErrorAction SilentlyContinue
    Remove-Item Env:\AGENT_HARNESS_MEMORY_EMBEDDING_BASE_URL -ErrorAction SilentlyContinue
}
