"""Read the active owner's actual deadline and observed native resources."""

import math
import os
import time
from pathlib import Path

from ahc.domain.sessions import ContractError
from ahc.domain.workspace import WorkspaceIdentity


def process_deadline(port, now):
    if getattr(getattr(port, "_runner", None), "owner_lifetime", None) is True:
        return None
    opened = getattr(port, "_opened_at", None)
    lifetime = getattr(port, "_lifetime", None)
    if (any(type(v) not in (int, float) or not math.isfinite(v) for v in (opened, lifetime))
            or opened < 0 or opened > now or lifetime <= 0):
        raise ContractError("workspace process lifetime is unverified")
    return opened + lifetime


def job_deadline(port, now):
    budget = getattr(port, "_turn_budget", None)
    hard = budget.current_deadline() if budget is not None else getattr(port, "_turn_deadline", None)
    if type(hard) not in (int, float) or not math.isfinite(hard) or hard <= now:
        raise ContractError("workspace job requires an admitted turn")
    lifetime = process_deadline(port, now)
    return min(hard, lifetime) if lifetime is not None else hard


def budget(workspace):
    port = workspace.port
    now = time.monotonic()
    adaptive = getattr(port, "_turn_budget", None)
    deadline = adaptive.current_deadline() if adaptive is not None else getattr(port, "_turn_deadline", None)
    timeout = getattr(port, "_timeout", None)
    owner_lifetime = getattr(getattr(port, "_runner", None), "owner_lifetime", None)
    result = {
        "identity": workspace.identity.to_dict(),
        "deadline": {
            "source": ("admitted-adaptive-task" if adaptive is not None else
                       "admitted-task" if deadline is not None else "direct-call-default"),
            "remaining_ms": (max(0, int((deadline - now) * 1000))
                             if deadline is not None else None),
            "direct_call_timeout_seconds": timeout,
            "direct_timeout_applies_to_admitted_cell": False if deadline is not None else True,
        },
        "lifetime": {
            "configured_seconds": getattr(port, "_lifetime", None),
            "mode": "owner" if owner_lifetime is True else "bounded-process",
            "hard_task_extension_supported": adaptive is not None,
            "adaptive": adaptive.status() if adaptive is not None else None,
        },
        "configured_metadata": getattr(port, "budget_context", None),
        "resources": {},
    }
    try:
        lifetime = process_deadline(port, now)
        result["lifetime"]["remaining_ms"] = (
            max(0, int((lifetime - now) * 1000)) if lifetime is not None else None)
        effective = min(deadline, lifetime) if deadline is not None and lifetime is not None else deadline
        result["deadline"]["effective_remaining_ms"] = (
            max(0, int((effective - now) * 1000)) if effective is not None else None)
    except ContractError:
        result["lifetime"]["remaining_ms"] = None
        result["lifetime"]["observation"] = "unverified"
        result["deadline"]["effective_remaining_ms"] = None
    lease = getattr(port, "_job_lease", None)
    if lease is not None:
        result["job_lease"] = lease.to_dict(now)
    try:
        from ahc.adapters.effective_resources import observe_process_resources
        result["resources"]["controller"] = observe_process_resources(os.getpid())
        unit = getattr(port, "_unit", None)
        if getattr(unit, "kind", None) != "linux-cgroup.v1":
            raise ValueError()
        directory = Path("/sys/fs/cgroup") / unit.cgroup.lstrip("/")
        descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            stat = os.fstat(descriptor)
            if (stat.st_dev, stat.st_ino) != (unit.device, unit.inode):
                raise ValueError()
            procs = os.open("cgroup.procs", os.O_RDONLY | os.O_NOFOLLOW, dir_fd=descriptor)
            try:
                raw = os.read(procs, 4096).decode("ascii")
            finally:
                os.close(procs)
            candidates = [int(item) for item in raw.split()[:16]]
            if not candidates:
                raise ValueError()
            observed = observe_process_resources(candidates[0])
            if observed.get("cgroup_path") != unit.cgroup:
                raise ValueError()
            result["resources"]["workspace"] = observed
        finally:
            os.close(descriptor)
    except Exception:
        result["resources"]["workspace"] = {"status": "unavailable",
                                             "reason": "exact-native-owner-not-observed"}
    return result


def invoke_budget(workspace, arguments):
    try:
        if (type(arguments) is not dict or set(arguments) != {"expected"}
                or WorkspaceIdentity.from_dict(arguments["expected"]) != workspace.identity):
            raise ContractError("workspace budget owner mismatch")
        return {"disposition": "observed", "unresolved_effects": False,
                "result": budget(workspace)}
    except Exception:
        return {"disposition": "rejected", "reason": "workspace-budget-unavailable",
                "unresolved_effects": False}
