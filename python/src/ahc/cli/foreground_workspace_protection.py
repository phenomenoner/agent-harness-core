"""Explicit native workspace boundary, not whole-foreground promotion authority."""

from ahc.domain.native_resource_limits import NativeResourceLimits
from ahc.domain.sessions import ContractError
from ahc.domain.turns import _sha256_hex


def parse_native_protection(value, cwd):
    from .foreground_config_v2 import _posix_path

    if type(value) is not dict or set(value) != {
        "executable",
        "sha256",
        "readonly_roots",
        "protected_roots",
        "resource_limits",
    }:
        raise ContractError("workspace.protection fields are invalid")
    executable = _posix_path(
        value["executable"], "workspace.protection.executable", executable=True
    )
    _sha256_hex(value["sha256"])
    result = {"executable": executable, "sha256": value["sha256"]}
    for key in ("readonly_roots", "protected_roots"):
        paths = value[key]
        if type(paths) is not list or not paths:
            raise ContractError("workspace.protection requires nonempty root lists")
        result[key] = [_posix_path(path, "workspace.protection." + key) for path in paths]
        if len(set(result[key])) != len(result[key]):
            raise ContractError("workspace.protection roots must be unique")
    limits = value["resource_limits"]
    if type(limits) is not dict or set(limits) != {
        "memory_bytes",
        "pids",
        "cpu_quota_us",
        "cpu_period_us",
        "swap_bytes",
    }:
        raise ContractError("workspace.protection requires explicit resource limits")
    NativeResourceLimits(**limits)
    result["resource_limits"] = dict(limits)
    # Mount/alias/executable checks are deliberately deferred to the actual native
    # composition. Parsing another platform's config must not touch its filesystem.
    return result


def native_workspace_protection(workspace):
    value = workspace.get("protection")
    if value is None:
        return {}
    from ahc.infrastructure.process.linux_workspace_boundary import LinuxWorkspaceBoundary

    return {
        "boundary": LinuxWorkspaceBoundary(
            executable=value["executable"],
            sha256=value["sha256"],
            workspace=workspace["cwd"],
            readonly_roots=value["readonly_roots"],
            protected_roots=value["protected_roots"],
        ),
        "resource_limits": NativeResourceLimits(**value["resource_limits"]),
    }
