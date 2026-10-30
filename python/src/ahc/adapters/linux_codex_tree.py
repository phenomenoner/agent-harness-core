"""Stdlib extracted-tree verification shared with the isolated Linux controller."""

import hashlib
import os
import stat
import sys
from dataclasses import dataclass
from pathlib import Path


class TreeRejected(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class LinuxTreeVerification:
    root: Path
    target: str
    permissions_checked: bool
    root_ownership_checked: bool = False


def _protected_root_metadata(info, *, sticky_parent=False):
    return info.st_uid == 0 and (
        not (info.st_mode & 0o022) or (sticky_parent and bool(info.st_mode & stat.S_ISVTX))
    )


def verify_linux_tree(
    root: Path, *, pin: dict, target: str, require_root_owned: bool = False
) -> LinuxTreeVerification:
    """Verify a quiescent extracted tree, without executing it.

    Windows staging can prove content, but cannot prove Linux modes. The returned
    permissions flag must not be treated as custody or atomic launch authorization;
    a launcher still needs a protected bundle and its own admission checks.
    """
    if target != pin["target"]:
        raise TreeRejected("Linux Codex tree target mismatch")
    if type(require_root_owned) is not bool:
        raise TreeRejected("Linux Codex ownership requirement invalid")
    if require_root_owned and sys.platform != "linux":
        raise TreeRejected("root ownership qualification requires Linux")
    root = Path(root)
    if not root.is_absolute() or ".." in root.parts or not root.is_dir():
        raise TreeRejected("Linux Codex requires an explicit tree root")
    if any(p.is_symlink() or p.is_junction() for p in (root, *root.parents)):
        raise TreeRejected("Linux Codex tree cannot traverse redirects")
    expected = {item["path"]: item for item in pin["members"]}
    directories = {item["path"]: item["mode"] for item in pin["directories"]}
    permissions = sys.platform == "linux"
    seen = set()
    pending = [root]
    try:
        if require_root_owned:
            for path in (root, *root.parents):
                if not _protected_root_metadata(path.lstat(), sticky_parent=path != root):
                    raise TreeRejected("Linux Codex root or ancestor is replaceable")
        while pending:
            with os.scandir(pending.pop()) as entries:
                for entry in entries:
                    path = Path(entry.path)
                    name = path.relative_to(root).as_posix()
                    if entry.is_symlink() or path.is_junction():
                        raise TreeRejected("Linux Codex tree contains redirect")
                    # Windows DirEntry metadata can omit the actual hard-link count.
                    info = path.lstat()
                    if require_root_owned and not _protected_root_metadata(info):
                        raise TreeRejected("Linux Codex member is not root protected")
                    if stat.S_ISDIR(info.st_mode):
                        if name not in directories:
                            raise TreeRejected("Linux Codex tree contains extra directory")
                        if permissions and stat.S_IMODE(info.st_mode) != directories[name]:
                            raise TreeRejected("Linux Codex tree directory mode mismatch")
                        pending.append(path)
                    else:
                        pin = expected.get(name)
                        if pin is None or not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                            raise TreeRejected("Linux Codex tree member mismatch")
                        if info.st_size != pin["size"]:
                            raise TreeRejected("Linux Codex tree member size mismatch")
                        if permissions and stat.S_IMODE(info.st_mode) != pin["mode"]:
                            raise TreeRejected("Linux Codex tree member mode mismatch")
                        with path.open("rb") as stream:
                            if hashlib.file_digest(stream, "sha256").hexdigest() != pin["sha256"]:
                                raise TreeRejected("Linux Codex tree member digest mismatch")
                    seen.add(name)
        if seen != set(expected) | set(directories):
            raise TreeRejected("Linux Codex tree members missing")
    except OSError:
        raise TreeRejected("Linux Codex tree unavailable") from None
    return LinuxTreeVerification(root, target, permissions, require_root_owned)
