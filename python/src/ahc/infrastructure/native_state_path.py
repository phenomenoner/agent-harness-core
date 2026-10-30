"""Read back native persistent filesystem placement before opening runtime state."""

import os
import re
import sys
from pathlib import Path

from ahc.domain.sessions import ContractError

_PERSISTENT = frozenset({"ext2", "ext3", "ext4", "xfs", "btrfs", "f2fs", "zfs", "bcachefs"})


def _mount_type(path, device, mountinfo):
    selected = None
    for line in mountinfo.splitlines():
        before, separator, after = line.partition(" - ")
        fields, filesystem = before.split(), after.split()
        if not separator or len(fields) < 6 or len(filesystem) < 3:
            raise ValueError("invalid mount observation")
        mount = Path(re.sub(r"\\([0-7]{3})", lambda m: chr(int(m[1], 8)), fields[4]))
        if path == mount or mount in path.parents:
            if selected is None or len(mount.parts) > selected[0]:
                selected = (len(mount.parts), fields[2], filesystem[0])
    if selected is None or selected[1] != f"{os.major(device)}:{os.minor(device)}":
        raise ValueError("filesystem observation mismatch")
    return selected[2]


def require_native_state_directory(value):
    try:
        if sys.platform != "linux":
            raise ValueError()
        path = Path(value)
        if not path.is_absolute() or not path.is_dir():
            raise ValueError()
        if any(parent.is_symlink() for parent in (path, *path.parents)):
            raise ValueError()
        path = path.resolve(strict=True)
        kind = _mount_type(path, path.stat().st_dev, Path("/proc/self/mountinfo").read_text())
        if kind not in _PERSISTENT:
            raise ValueError()
        return kind
    except (OSError, ValueError):
        raise ContractError("native runtime requires verified persistent Linux storage") from None
