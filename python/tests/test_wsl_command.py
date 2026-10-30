"""WSL command-boundary checks; these do not qualify actual WSL execution."""

from dataclasses import replace

import pytest

from ahc.adapters.wsl_command import WslCommand
from ahc.domain.sessions import ContractError


def command():
    return WslCommand(
        "C:/Windows/System32/wsl.exe", "AHC-Codex-Ubuntu-24.04", "ahc",
        "/home/ahc/work area", "/opt/ahc/launcher",
    )


def test_explicit_distribution_user_and_exec_keep_paths_as_single_arguments():
    assert command().argv() == (
        "C:/Windows/System32/wsl.exe", "--distribution", "AHC-Codex-Ubuntu-24.04",
        "--user", "ahc", "--cd", "/home/ahc/work area", "--exec", "/opt/ahc/launcher",
    )
    # Shell metacharacters are literal path data, never a shell program.
    assert replace(command(), directory="/home/ahc/$(id);data").argv()[6] == (
        "/home/ahc/$(id);data"
    )


@pytest.mark.parametrize("field,value", [
    ("host_executable", "wsl.exe"),
    ("host_executable", "C:wsl.exe"),
    ("host_executable", "//server/share/wsl.exe"),
    ("host_executable", "C:/Windows/../wsl.exe"),
    ("distribution", ""), ("distribution", "--help"),
    ("distribution", "Ubuntu\n--terminate"),
    ("user", "root"), ("user", "0"), ("user", ""),
    ("directory", "C:/workspace"), ("directory", "/home/../root"),
    ("directory", "/home//ahc"), ("directory", "/home/ahc/"),
    ("launcher", "/"), ("launcher", "codex"),
    ("launcher", "/opt/ahc\x00/launcher"),
])
def test_invalid_or_implicit_target_rejected(field, value):
    with pytest.raises(ContractError):
        replace(command(), **{field: value})
