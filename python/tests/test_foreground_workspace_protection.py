"""Protected foreground configuration is explicit; no whole-runtime authority."""

import hashlib
from pathlib import Path

import pytest
from test_foreground_config_native import native_config as unprotected_native_config

from ahc.cli.foreground_config_native import parse_foreground_config_native
from ahc.cli.foreground_config_v2 import parse_foreground_config_v2
from ahc.cli.foreground_workspace_protection import native_workspace_protection
from ahc.domain.sessions import ContractError


def native_config():
    value = unprotected_native_config()
    value["workspace"]["kind"] = "linux-python"
    return value


def protection():
    return {
        "executable": "/usr/bin/bwrap",
        "sha256": "a" * 64,
        "readonly_roots": ["/usr"],
        "protected_roots": ["/home/ahc/product/state"],
        "resource_limits": {
            "memory_bytes": 128 * 1024**2,
            "pids": 16,
            "cpu_quota_us": 50000,
            "cpu_period_us": 100000,
            "swap_bytes": 0,
        },
    }


def test_loader_preserves_explicit_protection_without_host_fs_lookup():
    value = native_config()
    value["workspace"]["protection"] = protection()
    config = parse_foreground_config_native(value)
    assert config.workspace["protection"] == protection()
    value["workspace"]["protection"]["resource_limits"]["pids"] = 200
    assert config.workspace["protection"]["resource_limits"]["pids"] == 16


@pytest.mark.parametrize(
    "case",
    [
        "missing-field",
        "extra-field",
        "empty-roots",
        "duplicates",
        "relative-root",
        "bad-sha",
        "bool-limit",
        "missing-limit",
        "too-large-limit",
        "runtime-not-hidden",
    ],
)
def test_invalid_protection_is_not_silently_dropped(case):
    value = native_config()
    p = protection()
    value["workspace"]["protection"] = p
    if case == "missing-field":
        p.pop("executable")
    if case == "extra-field":
        p["enabled"] = True
    if case == "empty-roots":
        p["readonly_roots"] = []
    if case == "duplicates":
        p["readonly_roots"] = ["/usr", "/usr"]
    if case == "relative-root":
        p["protected_roots"] = ["relative"]
    if case == "bad-sha":
        p["sha256"] = "g" * 64
    if case == "bool-limit":
        p["resource_limits"]["pids"] = True
    if case == "missing-limit":
        p["resource_limits"].pop("swap_bytes")
    if case == "too-large-limit":
        p["resource_limits"]["pids"] = 4097
    if case == "runtime-not-hidden":
        p["protected_roots"] = ["/other/state"]
    with pytest.raises(ContractError):
        parse_foreground_config_native(value)


def test_v2_rejects_native_protection():
    from test_foreground_config_v2 import valid_config

    value = valid_config()
    value["workspace"]["protection"] = protection()
    with pytest.raises(ContractError):
        parse_foreground_config_v2(value)


def test_default_has_no_inferred_boundary():
    workspace = parse_foreground_config_native(native_config()).workspace
    assert native_workspace_protection(workspace) == {}


def test_actual_boundary_and_explicit_limits_reach_runner(tmp_path):
    from ahc.adapters.native_process_runner import NativeProcessRunner

    workspace = tmp_path / "workspace"
    workspace.mkdir()
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    p = protection()
    p.update(
        executable=str(Path("/usr/bin/true").resolve()),
        sha256=hashlib.sha256(Path("/usr/bin/true").read_bytes()).hexdigest(),
        protected_roots=[str(runtime)],
    )
    configured = {"cwd": str(workspace), "protection": p}
    options = native_workspace_protection(configured)
    runner = NativeProcessRunner(cwd=str(workspace), environment={}, persistent=True, **options)
    assert runner._boundary.workspace == workspace
    assert runner._boundary.protected_roots == (runtime,)
    assert runner._resource_limits.pids == 16
    assert runner._resource_limits.memory_bytes == 128 * 1024**2


def test_overlapping_runtime_is_refused_before_creation(tmp_path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    p = protection()
    p.update(executable=str(Path("/usr/bin/true").resolve()), protected_roots=[str(workspace)])
    with pytest.raises(ContractError, match="overlap"):
        native_workspace_protection({"cwd": str(workspace), "protection": p})
