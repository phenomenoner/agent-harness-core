"""Exercise the Windows observer without a distro, credentials or live receivers."""

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

TOOLS = Path(__file__).resolve().parents[2] / "tools"
POWERSHELL = Path(os.environ.get("SystemRoot", "C:/Windows")) / (
    "System32/WindowsPowerShell/v1.0/powershell.exe"
)
pytestmark = pytest.mark.skipif(sys.platform != "win32", reason="Windows operator boundary")


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def run(script, timeout=12):
    result = subprocess.run(
        [str(POWERSHELL), "-NoProfile", "-NonInteractive", "-Command",
         f". {quote(TOOLS / 'ahc-health.ps1')}; " + script],
        capture_output=True, text=True, timeout=timeout,
        creationflags=subprocess.CREATE_NO_WINDOW,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def unit_output(**changes):
    values = dict(Id="ahc-test.service", LoadState="loaded", ActiveState="active",
                  SubState="running", MainPID="42", InvocationID="a" * 32)
    values.update(changes)
    return "\n".join(f"{key}={value}" for key, value in values.items())


@pytest.mark.parametrize("changes,state", [
    ({}, "running"),
    ({"ActiveState": "failed", "SubState": "failed", "MainPID": "0"}, "failed"),
    ({"ActiveState": "inactive", "SubState": "dead", "MainPID": "0"}, "inactive"),
    ({"LoadState": "not-found"}, "unavailable"),
    ({"MainPID": "0"}, "not-ready"),
    ({"InvocationID": "0" * 32}, "not-ready"),
])
def test_unit_state_never_proves_receiver_readiness(changes, state):
    result = run(
        f"$p=@{{category='observed';output={quote(unit_output(**changes))}}}; "
        "ConvertTo-AhcUnitHealth $p ahc-test.service | ConvertTo-Json -Compress"
    )
    assert result["observation"] == "observed"
    assert result["unitState"] == state
    assert result["receiverReadiness"] == "unknown"
    assert result["backendReadiness"] == "unknown"
    assert result["memoryReadiness"] == "unknown"


@pytest.mark.parametrize("output", [
    unit_output(Id="ahc-other.service"), unit_output() + "\nMainPID=99",
    "", unit_output(InvocationID="secret"), unit_output(MainPID="-1"),
])
def test_wrong_owner_or_malformed_readback_fails_closed(output):
    result = run(
        f"$p=@{{category='observed';output={quote(output)}}}; "
        "ConvertTo-AhcUnitHealth $p ahc-test.service | ConvertTo-Json -Compress"
    )
    assert result["observation"] == "invalid-observation"
    assert result["unitState"] == "unknown"
    assert "secret" not in json.dumps(result)


@pytest.mark.parametrize("code,category", [
    ("print('safe')", "observed"),
    ("import sys; print('private-error',file=sys.stderr); sys.exit(7)", "probe-failed"),
    ("import time; time.sleep(5)", "observation-timeout"),
])
def test_real_subprocess_deadline_and_error_redaction(code, category):
    arguments = subprocess.list2cmdline(["-I", "-c", code])
    started = time.monotonic()
    result = run(
        f"Invoke-AhcBoundedRead -FileName {quote(sys.executable)} "
        f"-Arguments {quote(arguments)} -TimeoutSeconds 1 | ConvertTo-Json -Compress"
    )
    assert result["category"] == category
    assert "private-error" not in json.dumps(result)
    assert time.monotonic() - started < 4


def test_inherited_pipe_cannot_extend_deadline():
    code = (
        "import subprocess,sys; "
        "subprocess.Popen([sys.executable,'-I','-c','import time; time.sleep(3)']); "
        "print('parent-exited')"
    )
    arguments = subprocess.list2cmdline(["-I", "-c", code])
    started = time.monotonic()
    result = run(
        f"Invoke-AhcBoundedRead -FileName {quote(sys.executable)} "
        f"-Arguments {quote(arguments)} -TimeoutSeconds 1 | ConvertTo-Json -Compress"
    )
    assert result["category"] == "observation-timeout"
    assert time.monotonic() - started < 3


@pytest.mark.parametrize("category,exit_code", [
    ("observed", 0), ("observation-timeout", 2), ("probe-failed", 2),
])
def test_entry_health_routes_and_labels_timestamp_without_wsl(tmp_path, category, exit_code):
    # Substitute only the process seam. Execute the actual entry and parser.
    (tmp_path / "ahc-linux.ps1").write_bytes((TOOLS / "ahc-linux.ps1").read_bytes())
    helper = (TOOLS / "ahc-health.ps1").read_text()
    helper += (
        "\nfunction Invoke-AhcBoundedRead { param($FileName,$Arguments,$TimeoutSeconds)\n"
        "if ($Arguments -notlike '*--cd / --exec /usr/bin/systemctl --user show *') "
        "{ throw 'wrong command' }\n"
        f"return @{{category={quote(category)};output={quote(unit_output())}}}\n}}\n"
    )
    (tmp_path / "ahc-health.ps1").write_text(helper)
    result = subprocess.run(
        [str(POWERSHELL), "-NoProfile", "-NonInteractive", "-File",
         str(tmp_path / "ahc-linux.ps1"), "-Action", "health", "-Unit", "ahc-test.service",
         "-Distribution", "AHC-test", "-LinuxUser", "ahc"],
        capture_output=True, text=True, timeout=10,
        creationflags=subprocess.CREATE_NO_WINDOW,
    )
    assert result.returncode == exit_code, result.stderr
    health = json.loads(result.stdout)
    assert health["schema"] == "ahc.operator-health.v1"
    assert health["observation"] == category
    assert health["unitState"] == ("running" if category == "observed" else "unknown")
    assert health["receiverReadiness"] == "unknown"
    assert health["sampledAt"].endswith("Z")
    assert health["validForSeconds"] == 120


@pytest.mark.parametrize("action", ["status", "logs", "start", "stop", "restart"])
@pytest.mark.parametrize("category", ["observed", "observation-timeout", "probe-failed"])
def test_all_operator_actions_use_one_bounded_client_without_wsl(tmp_path, action, category):
    (tmp_path / "ahc-linux.ps1").write_bytes((TOOLS / "ahc-linux.ps1").read_bytes())
    helper = (TOOLS / "ahc-health.ps1").read_text()
    output = unit_output() if action == "status" else "bounded output"
    helper += (
        "\nfunction Invoke-AhcBoundedRead { param($FileName,$Arguments,$TimeoutSeconds)\n"
        "if ($TimeoutSeconds -ne 1 -or $Arguments -notlike '--distribution AHC-test "
        "--user ahc --cd / --exec /usr/bin/*') { throw 'wrong bounded command' }\n"
        f"return @{{category={quote(category)};output={quote(output)}}}\n}}\n"
    )
    (tmp_path / "ahc-health.ps1").write_text(helper)
    script = (
        "function wsl.exe { throw 'direct WSL execution is forbidden in this test' }; "
        f"& {quote(tmp_path / 'ahc-linux.ps1')} -Action {action} -Unit ahc-test.service "
        "-Distribution AHC-test -LinuxUser ahc -HealthTimeoutSeconds 1; exit $LASTEXITCODE"
    )
    result = subprocess.run(
        [str(POWERSHELL), "-NoProfile", "-NonInteractive", "-Command", script],
        capture_output=True, text=True, timeout=10,
        creationflags=subprocess.CREATE_NO_WINDOW,
    )
    assert result.returncode == (0 if category == "observed" else 2), result.stderr
    if category != "observed" or action == "status":
        observation = json.loads(result.stdout)
        assert observation["observation"] == category
        assert observation["unitState"] == ("running" if category == "observed" else "unknown")
        assert observation["receiverReadiness"] == "unknown"
        if action in ("start", "stop", "restart"):
            assert observation["controlOutcome"] == "unknown"
    else:
        assert result.stdout.strip() == "bounded output"
