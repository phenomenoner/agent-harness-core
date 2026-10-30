import ast
import base64
import subprocess
import sys
import zlib

import pytest
from test_foreground_config_v2 import valid_config

from ahc.adapters.ipython_kernel import IPYTHON_KERNEL
from ahc.adapters.workspace_kernel import KERNEL
from ahc.adapters.workspace_launch import kernel_command
from ahc.cli.foreground_config_v2 import parse_foreground_config_v2
from ahc.domain.sessions import ContractError


def test_cpython_launcher_bytes_unchanged_and_ipython_payload_exact():
    cwd = "/home/ahc"
    assert kernel_command(cwd) == "import os;os.chdir(" + repr(cwd) + ");exec(" + repr(KERNEL) + ")"
    command = kernel_command(cwd, "ipython")
    tree = ast.parse(command)
    payload = next(n.value for n in ast.walk(tree) if isinstance(n, ast.Constant)
                   and isinstance(n.value, str) and len(n.value) > 100)
    assert zlib.decompress(base64.b64decode(payload)).decode() == IPYTHON_KERNEL
    assert len(command) <= 4096
    compile(IPYTHON_KERNEL, "<ipython-kernel>", "exec")
    with pytest.raises(ContractError):
        kernel_command(cwd, "unknown")


def test_ipython_is_explicit_and_changes_permission_environment_identity():
    value = valid_config()
    python = parse_foreground_config_v2(value)
    value["workspace"]["kind"] = "wsl-ipython"
    ipython = parse_foreground_config_v2(value)
    assert python.configuration_digest != ipython.configuration_digest
    assert python.effective_policy.environment_digest != ipython.effective_policy.environment_digest
    assert ipython.preview()["workspace"]["kind"] == "wsl-ipython"


def test_missing_ipython_exits_without_python_fallback():
    result = subprocess.run([sys._base_executable, "-I", "-S", "-c", IPYTHON_KERNEL],
                            input=b'', capture_output=True, timeout=5)
    assert result.returncode != 0
    assert result.stdout == b''
    assert b"ModuleNotFoundError" in result.stderr and b"IPython" in result.stderr
