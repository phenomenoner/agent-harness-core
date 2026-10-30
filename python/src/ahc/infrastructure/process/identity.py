"""Process identity primitives backed by psutil."""

from __future__ import annotations

import math
import os
import sys
from pathlib import Path
from typing import Any

import psutil

from ahc.domain import ProcessIdentity

_LINUX = sys.platform == "linux"


def _linux_generation(pid: int) -> tuple[str, int, int]:
    """Read kernel generation values, independent of wall-clock boot-time estimates."""
    root = Path("/proc")
    stat = (root / str(pid) / "stat").read_text()
    if int(stat.split(" ", 1)[0]) != pid:
        raise ValueError("process identity changed")
    # comm may contain spaces or parentheses; fields after its final ')' are fixed.
    ticks = int(stat.rsplit(")", 1)[1].split()[19])
    namespace = (root / str(pid) / "ns/pid").stat().st_ino
    boot = (root / "sys/kernel/random/boot_id").read_text().strip()
    return boot, ticks, namespace


class ProcessIdentityError(RuntimeError):
    """Base class for safe process-control failures."""


class StaleProcessIdentity(ProcessIdentityError):
    """The PID is gone or now belongs to another process generation."""


def identity_for_process(process: psutil.Process) -> ProcessIdentity:
    try:
        if _LINUX:
            before = _linux_generation(process.pid)
            start = process.create_time()
            if _linux_generation(process.pid) != before:
                raise StaleProcessIdentity("process generation changed during observation")
            return ProcessIdentity(process.pid, start, *before)
        return ProcessIdentity(pid=process.pid, start_time=process.create_time())
    except (psutil.Error, OSError, ValueError, IndexError) as exc:
        raise StaleProcessIdentity(f"cannot read process identity for PID {process.pid}") from exc


def identity_for_pid(pid: int) -> ProcessIdentity:
    try:
        return identity_for_process(psutil.Process(pid))
    except (psutil.NoSuchProcess, psutil.AccessDenied) as exc:
        raise StaleProcessIdentity(f"PID {pid} is not controllable") from exc


def current_identity() -> ProcessIdentity:
    return identity_for_pid(os.getpid())


def identity_matches(identity: ProcessIdentity, process: psutil.Process | None = None) -> bool:
    """Match native kernel generation, or the unchanged strict legacy time contract."""

    try:
        candidate = process if process is not None else psutil.Process(identity.pid)
        actual = identity_for_process(candidate)
    except (ProcessIdentityError, psutil.Error, OSError):
        return False
    if identity.linux_boot_id is not None:
        return (actual.pid, actual.linux_boot_id, actual.linux_start_ticks,
                actual.linux_pid_namespace) == (
                    identity.pid, identity.linux_boot_id, identity.linux_start_ticks,
                    identity.linux_pid_namespace)
    return actual.pid == identity.pid and math.isclose(
        actual.start_time, identity.start_time, rel_tol=0.0, abs_tol=1e-6
    )


def require_identity(identity: ProcessIdentity) -> psutil.Process:
    """Resolve an identity or fail before any signal can be sent."""

    try:
        process = psutil.Process(identity.pid)
    except (psutil.NoSuchProcess, psutil.AccessDenied) as exc:
        raise StaleProcessIdentity(f"PID {identity.pid} is no longer present") from exc
    if not identity_matches(identity, process):
        raise StaleProcessIdentity(f"PID {identity.pid} no longer has the observed start time")
    return process


def safe_identity(value: Any) -> ProcessIdentity | None:
    """Best-effort conversion used while recording an already-exited child."""

    try:
        if isinstance(value, psutil.Process):
            return identity_for_process(value)
        return identity_for_pid(int(value))
    except (ProcessIdentityError, TypeError, ValueError):
        return None
