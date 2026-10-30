"""Explicit offline filesystem/process boundary for Python learning experiments.

This does not retrofit protection onto an existing trusted foreground or grant
automatic promotion. Every model-controlled execution route must be admitted
separately before a foreground can claim protected learning controls.
"""

import hashlib
import os
import re
import stat
from pathlib import Path

from ahc.domain.native_scratch_limits import NativeScratchLimits
from ahc.domain.sessions import ContractError


def _path(value):
    if type(value) is not str or not value.startswith("/") or "\0" in value:
        raise ContractError("protected workspace requires absolute native paths")
    path = Path(value)
    if str(path) != value or path.resolve() != path or not path.exists():
        raise ContractError("protected workspace path is absent, noncanonical or linked")
    return path


def _overlap(left, right):
    return left.is_relative_to(right) or right.is_relative_to(left)


class LinuxWorkspaceBoundary:
    """A mount allowlist, isolated PID/IPC/network namespaces, and no fallback.

    By default the owned work directory is the only persistent writable mount.
    Explicit scratch limits instead hide it behind a size-limited empty tmpfs;
    /tmp is also size-limited. Results must leave through the bounded protocol.
    Neither Unix sockets nor pre-existing hardlinks may smuggle host access through
    a persistent work mount.
    Runtime roots are read-only. Source policy/evidence roots must be invisible.
    Enclosing cgroup/deadline custody remains the process runner's responsibility.
    """

    def __init__(self, *, executable, sha256, workspace, readonly_roots, protected_roots,
                 scratch_limits=None):
        if os.name != "posix" or not Path("/proc/self/ns/user").exists():
            raise ContractError("protected workspace requires Linux namespaces")
        self.executable = _path(executable)
        self.workspace = _path(workspace)
        if scratch_limits is not None and type(scratch_limits) is not NativeScratchLimits:
            raise ContractError("protected workspace scratch limits invalid")
        self.scratch_limits = scratch_limits
        if (
            not self.executable.is_file()
            or not self.workspace.is_dir()
            or type(sha256) is not str
            or not re.fullmatch("[0-9a-f]{64}", sha256)
            or type(readonly_roots) not in (tuple, list)
            or not readonly_roots
            or type(protected_roots) not in (tuple, list)
            or not protected_roots
        ):
            raise ContractError("protected workspace configuration invalid")
        self.sha256 = sha256
        self.readonly_roots = tuple(_path(value) for value in readonly_roots)
        self.protected_roots = tuple(_path(value) for value in protected_roots)
        nested = sorted((path for path in self.protected_roots
                         if path != self.workspace and path.is_relative_to(self.workspace)),
                        key=lambda path: len(path.parts))
        self.masked_roots = tuple(path for path in nested
                                 if not any(
                                     path.is_relative_to(parent) for parent in nested
                                     if parent != path and len(parent.parts) < len(path.parts)))
        broad = {"/", "/home", "/mnt", "/run", "/proc", "/sys", "/dev", "/tmp", "/etc"}
        if (
            str(self.workspace) in broad
            or any(str(path) in broad for path in self.readonly_roots)
            or any(not path.is_dir() for path in self.readonly_roots)
            or any(_overlap(self.workspace, path) for path in self.readonly_roots)
            or self.executable.is_relative_to(self.workspace)
            or any(self.workspace.is_relative_to(path) for path in self.protected_roots)
            or any(not path.is_dir() for path in self.masked_roots)
            or any(_overlap(path, root) for path in self.protected_roots
                   for root in self.readonly_roots)
        ):
            raise ContractError("protected control and workspace mounts overlap")

    def command(self, argv):
        if (
            type(argv) is not tuple
            or not argv
            or any(type(arg) is not str or "\0" in arg for arg in argv)
        ):
            raise ContractError("protected workspace command invalid")
        executable = Path(argv[0])
        if (
            not executable.is_absolute()
            or not executable.is_file()
            or not any(executable.resolve().is_relative_to(path) for path in self.readonly_roots)
        ):
            raise ContractError("protected interpreter must belong to a read-only runtime")
        with self.executable.open("rb") as stream:
            if hashlib.file_digest(stream, "sha256").hexdigest() != self.sha256:
                raise ContractError("workspace isolation executable changed")
        inspected = 0
        for path in self.masked_roots:
            if _path(str(path)) != path or not path.is_dir():
                raise ContractError("protected control mask changed")
        # Ephemeral workspaces hide the host directory rather than importing it.
        source = os.walk(self.workspace, followlinks=False) if self.scratch_limits is None else ()
        for root, directories, files in source:
            directories[:] = [name for name in directories
                              if Path(root) / name not in self.masked_roots]
            for name in (*directories, *files):
                inspected += 1
                if inspected > 20000:
                    raise ContractError("protected workspace exceeds admission scan budget")
                path = Path(root) / name
                metadata = path.lstat()
                if stat.S_ISLNK(metadata.st_mode):
                    try:
                        target = path.resolve()
                    except (OSError, RuntimeError) as error:
                        raise ContractError("protected workspace link is unresolved") from error
                    if (not target.is_relative_to(self.workspace)
                            or any(target.is_relative_to(control)
                                   for control in self.protected_roots)):
                        raise ContractError("protected workspace link leaves ordinary work")
                    # Mount namespaces enforce the same boundary if a link is
                    # replaced after this check. Normal in-work links add no
                    # host mount and must not prevent ordinary tasks.
                    continue
                if not stat.S_ISDIR(metadata.st_mode) and (
                    not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1
                ):
                    raise ContractError("protected workspace contains host IPC or hardlinks")
        command = [
            str(self.executable),
            "--unshare-all",
            "--unshare-user",
            "--disable-userns",
            "--assert-userns-disabled",
            "--die-with-parent",
            "--new-session",
            "--cap-drop",
            "ALL",
            "--clearenv",
        ]
        for path in self.readonly_roots:
            command.extend(("--ro-bind", str(path), str(path)))
        # Ubuntu's merged-/usr aliases do not expose additional host paths.
        for name in ("bin", "sbin", "lib", "lib64"):
            if Path("/usr", name).exists() and Path("/usr") in self.readonly_roots:
                command.extend(("--symlink", "usr/" + name, "/" + name))
        temporary_mount = ("--tmpfs", "/tmp")
        workspace_mount = ("--bind", str(self.workspace), str(self.workspace))
        control_masks = tuple(value for path in self.masked_roots for value in (
            "--size", str(1024**2), "--tmpfs", str(path), "--remount-ro", str(path)))
        if self.scratch_limits is not None:
            if type(self.scratch_limits) is not NativeScratchLimits:
                raise ContractError("protected workspace scratch limits changed")
            temporary_mount = ("--size", str(self.scratch_limits.temporary_bytes),
                               "--tmpfs", "/tmp")
            workspace_mount = ("--size", str(self.scratch_limits.workspace_bytes),
                               "--tmpfs", str(self.workspace))
        command.extend(
            (
                "--proc",
                "/proc",
                "--dev",
                "/dev",
                *temporary_mount,
                "--dir",
                "/tmp/home",
                "--setenv",
                "HOME",
                "/tmp/home",
                "--setenv",
                "PATH",
                "/usr/bin:/bin",
                "--setenv",
                "LANG",
                "C.UTF-8",
                *workspace_mount,
                *control_masks,
                "--chdir",
                str(self.workspace),
                "--",
                *argv,
            )
        )
        return tuple(command)


__all__ = ["LinuxWorkspaceBoundary"]
