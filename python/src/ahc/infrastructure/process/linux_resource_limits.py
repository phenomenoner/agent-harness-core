"""Limit only an owned, empty child cgroup through its retained descriptor.

No controller enablement, parent write, systemd setting or fallback is provided.
CPU bandwidth is the kernel's fair-class cpu.max contract, not a wall-time limit.
"""

import os

from ahc.domain.native_resource_limits import NativeResourceLimits
from ahc.domain.sessions import ContractError


def _require(group, limits):
    if (
        type(limits) is not NativeResourceLimits
        or group.owner_pid != os.getpid() or group.claimed
        or group.complete or group.kill_attempted or group.directory_fd < 0
        or group._populated()
    ):
        raise ContractError("resource limits require an owned empty unclaimed group")


def _read(group, name):
    descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
                         dir_fd=group.directory_fd)
    try:
        return " ".join(os.read(descriptor, 256).decode("ascii").split())
    finally:
        os.close(descriptor)


def _verify(group, limits):
    for name, expected in limits.kernel_values():
        if _read(group, name) != expected:
            raise ContractError("native resource limit readback differs from admission")


def configure_limits(group, limits):
    with group.lock:
        _require(group, limits)
        for name, value in limits.kernel_values():
            descriptor = os.open(name, os.O_WRONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
                                 dir_fd=group.directory_fd)
            try:
                raw = (value + "\n").encode("ascii")
                if os.write(descriptor, raw) != len(raw):
                    raise ContractError("native resource limit write was incomplete")
            finally:
                os.close(descriptor)
        _verify(group, limits)


def verify_limits(group, limits):
    with group.lock:
        _require(group, limits)
        _verify(group, limits)
