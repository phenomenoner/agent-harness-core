"""Split stdlib controllers remain standalone without installed AHC."""

import json
import subprocess
import sys

import pytest

from ahc.adapters.wsl_stream_runner import WslStreamRunner
from ahc.adapters.wsl_unit_runner import WslUnitRunner


@pytest.mark.parametrize("stream", [False, True])
def test_split_helper_bootstrap_is_standalone_and_bounded(stream):
    runner_type = WslStreamRunner if stream else WslUnitRunner
    runner = runner_type("C:/Windows/System32/wsl.exe", "AHC")
    source = runner._controller_source() if stream else runner._helper_source()
    assert (
        len(subprocess.list2cmdline(["wsl.exe", "--exec", "python3", "-I", "-c", source])) < 30000
    )
    result = subprocess.run(
        [sys.executable, "-I", "-c", source],
        input=b'{"action":"create","unit":"invalid"}\n',
        capture_output=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {
        "ok": False,
        "error": "unit stream unverified" if stream else "unit control unverified",
    }
