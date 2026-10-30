"""Observe retained native custody outside its former service; never signal it.

Only an exact, empty cgroup or proven path absence in this user's cgroup domain
can retire custody. An existing replacement, running resource, foreign host,
different boot, or unavailable observation remains unresolved.
"""

import os
import sys
from pathlib import Path, PurePosixPath

from ahc.domain.native_process_identity import (
    NativeCreationAttempt,
    NativeProcessIdentity,
    NativeProcessObservation,
)
from ahc.infrastructure.backend_custody import BackendCustodyUnavailable

_CGROUP_ROOT = "/sys/fs/cgroup"
_MACHINE_ID = Path("/etc/machine-id")
_BOOT_ID = Path("/proc/sys/kernel/random/boot_id")


class NativeRetirementObserver:
    """Read-only OS adapter for BackendCustody.recover under its existing lease."""

    def observe(self, identity):
        if sys.platform != "linux" or type(identity) is not NativeProcessIdentity:
            raise BackendCustodyUnavailable("native retirement requires a native Linux identity")
        directory = events = -1
        try:
            uid = os.geteuid()
            prefix = ("/", "user.slice", f"user-{uid}.slice", f"user@{uid}.service")
            parts = PurePosixPath(identity.cgroup).parts
            if (uid == 0 or parts[:4] != prefix
                    or _MACHINE_ID.read_text().strip() != identity.host_id
                    or _BOOT_ID.read_text().strip()
                    != identity.boot_id):
                raise ValueError()
            flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
            directory = os.open(_CGROUP_ROOT, flags)
            for part in parts[1:]:
                try:
                    child = os.open(part, flags, dir_fd=directory)
                except FileNotFoundError:
                    return NativeProcessObservation(identity, "absent", True)
                os.close(directory)
                directory = child
            actual = os.fstat(directory)
            if (actual.st_uid != uid
                    or (actual.st_dev, actual.st_ino) != (identity.device, identity.inode)):
                raise ValueError()
            events = os.open("cgroup.events", os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
                             dir_fd=directory)
            lines = os.pread(events, 4096, 0).decode("ascii").splitlines()
            fields = dict(line.split() for line in lines)
            if fields.get("populated") not in ("0", "1"):
                raise ValueError()
            empty = fields["populated"] == "0"
            return NativeProcessObservation(identity, "stopped" if empty else "running", empty)
        except (OSError, ValueError, UnicodeError):
            raise BackendCustodyUnavailable(
                "native retirement identity or absence unverified") from None
        finally:
            for descriptor in (events, directory):
                if descriptor >= 0:
                    os.close(descriptor)

    def reconcile_creation(self, attempt):
        if type(attempt) is not NativeCreationAttempt:
            raise BackendCustodyUnavailable("native retirement cannot reconcile legacy creation")
        observation = self.observe(attempt.identity)
        if not observation.cgroup_empty:
            raise BackendCustodyUnavailable("native creation remains running")
        return attempt.identity

    def stop(self, identity):
        raise BackendCustodyUnavailable("retirement observer has no stop authority")
