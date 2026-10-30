"""Explicit prestart setup within one admitted delegated service descriptor.

Never enables controllers on ancestors, moves processes, or disables existing
controllers. A failed readback stops startup; it does not undo uncertain writes.
"""

import os

from ahc.domain.sessions import ContractError

_REQUIRED = frozenset(("cpu", "memory", "pids"))


def _read(directory, name):
    descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
                         dir_fd=directory)
    try:
        raw = os.read(descriptor, 4096)
        if len(raw) == 4096:
            raise ContractError("service controller observation exceeded its bound")
        return raw.decode("ascii").split()
    finally:
        os.close(descriptor)


def enable_service_controllers(directory: int) -> None:
    """Caller must supply the descriptor admitted for the ExecStartPre owner."""
    if _read(directory, "cgroup.procs"):
        raise ContractError("resource partition requires an empty service root")
    if not _REQUIRED.issubset(_read(directory, "cgroup.controllers")):
        raise ContractError("required controllers are not delegated to this service")
    missing = _REQUIRED.difference(_read(directory, "cgroup.subtree_control"))
    if missing:
        descriptor = os.open("cgroup.subtree_control",
                             os.O_WRONLY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=directory)
        try:
            raw = (" ".join("+" + name for name in sorted(missing)) + "\n").encode("ascii")
            if os.write(descriptor, raw) != len(raw):
                raise ContractError("service controller setup write was incomplete")
        finally:
            os.close(descriptor)
    if not _REQUIRED.issubset(_read(directory, "cgroup.subtree_control")):
        raise ContractError("service controller setup readback differs")
