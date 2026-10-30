"""Network-capable backend namespace with sealed configuration and retained history."""

import os
import stat
from pathlib import Path

from ahc.domain.native_scratch_limits import NativeScratchLimits
from ahc.domain.sessions import ContractError

from .linux_workspace_boundary import LinuxWorkspaceBoundary, _overlap, _path

BACKEND_HOME = "/tmp/ahc-backend-home"
_HISTORY = ("sessions", "archived_sessions", "state")
_TEMPORARY = ("log", "tmp", "shell_snapshots", ".cache")


def _authentication_file(directory):
    """Inspect an explicitly staged cache without reading credential contents."""
    path = _path(str(directory / "auth.json"))
    metadata, parent = path.lstat(), directory.stat()
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
        or metadata.st_uid != os.getuid()
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or metadata.st_size > 16384
        or parent.st_uid != os.getuid()
        or stat.S_IMODE(parent.st_mode) != 0o700
    ):
        raise ContractError("staged backend authentication ownership or shape invalid")
    return path


def _installation_file(directory):
    """Prepare only the vendor identity file, never writable configuration HOME."""
    path = directory / "installation_id"
    descriptor = -1
    try:
        descriptor = os.open(
            path,
            os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0),
            0o644,
        )
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or metadata.st_size > 64
        ):
            raise ContractError("backend installation identity contains aliases or excess data")
    except OSError:
        raise ContractError("backend installation identity unavailable") from None
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    return path


class LinuxBackendBoundary(LinuxWorkspaceBoundary):
    """Share networking explicitly; never import host HOME, config, hooks or auth.

    Only three dedicated history directories survive backend generations. The
    empty virtual HOME and project configuration directories are read-only.
    This is filesystem/process containment, not a network destination ACL or
    protection from an independently authorized same-user host process.
    """

    def __init__(self, *, state_root, home_bytes, temporary_bytes,
                 staged_authentication=False, **options):
        super().__init__(**options)
        if type(staged_authentication) is not bool:
            raise ContractError("staged backend authentication mode invalid")
        self.staged_authentication = staged_authentication
        self.capacities = NativeScratchLimits(home_bytes, temporary_bytes)
        self.state_root = _path(state_root)
        # A legacy work directory may contain an explicitly masked control
        # directory. Only the selected history mounts below are exposed at the
        # sealed backend HOME; its host path stays hidden and read-only.
        masked_state = (self.state_root.is_relative_to(self.workspace)
            and any(self.state_root.is_relative_to(path) for path in self.masked_roots)
            and all(not _overlap(self.state_root, path) or self.state_root.is_relative_to(path)
                    for path in self.protected_roots))
        if (
            not self.state_root.is_dir()
            or any(_overlap(self.state_root, root) for root in self.readonly_roots)
            or (not masked_state and any(_overlap(self.state_root, root)
                                        for root in (self.workspace,*self.protected_roots)))
            or _overlap(self.workspace, Path(BACKEND_HOME))
        ):
            raise ContractError("backend history overlaps protected or executable mounts")
        self.history_roots = tuple(_path(str(self.state_root / name)) for name in _HISTORY)
        if any(not path.is_dir() for path in self.history_roots):
            raise ContractError("backend requires prepared dedicated history directories")

    def command(self, argv):
        # Revalidate persistent history paths too; no link or host IPC import.
        for path in self.history_roots:
            if _path(str(path)) != path:
                raise ContractError("backend history path changed")
        inspected = 0
        for path in self.history_roots:
            for root, directories, files in os.walk(path, followlinks=False):
                for name in (*directories, *files):
                    inspected += 1
                    metadata = (Path(root) / name).lstat()
                    if (
                        inspected > 20000
                        or stat.S_ISLNK(metadata.st_mode)
                        or (
                            not stat.S_ISDIR(metadata.st_mode)
                            and (not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1)
                        )
                    ):
                        raise ContractError(
                            "backend history contains aliases, IPC or excess inputs"
                        )
        command = list(super().command(argv))
        installation = _installation_file(self.history_roots[-1])
        authentication = (
            _authentication_file(self.state_root) if self.staged_authentication else None
        )
        writer_locks = self.history_roots[-1] / "thread-writer-locks"
        writer_locks.mkdir(mode=0o700, exist_ok=True)
        writer_locks = _path(str(writer_locks))
        if not writer_locks.is_dir():
            raise ContractError("backend thread writer locks require a dedicated directory")
        command.insert(command.index("--unshare-all") + 1, "--share-net")
        temporary = command.index("--tmpfs")
        command[temporary:temporary] = ["--size", str(self.capacities.temporary_bytes)]
        extra = ["--size", str(self.capacities.workspace_bytes), "--tmpfs", BACKEND_HOME]
        for name in (*_HISTORY, *_TEMPORARY):
            extra.extend(("--dir", BACKEND_HOME + "/" + name))
        extra.extend(("--dir", BACKEND_HOME + "/thread-writer-locks"))
        extra.extend(("--bind", str(installation), BACKEND_HOME + "/installation_id"))
        if authentication is not None:
            extra.extend(("--bind", str(authentication), BACKEND_HOME + "/auth.json"))
        extra.extend(("--remount-ro", BACKEND_HOME))
        for name, path in zip(_HISTORY, self.history_roots, strict=True):
            extra.extend(("--bind", str(path), BACKEND_HOME + "/" + name))
        extra.extend(("--bind", str(writer_locks), BACKEND_HOME + "/thread-writer-locks"))
        for name in _TEMPORARY:
            extra.extend(
                (
                    "--size",
                    str(self.capacities.temporary_bytes),
                    "--tmpfs",
                    BACKEND_HOME + "/" + name,
                )
            )
        # Fresh empty mountpoints also mask names not known to AHC. No named
        # server-disable list or empty-table merge is treated as containment.
        ancestors = (self.workspace, *self.workspace.parents)
        for parent in ancestors:
            for name in (".codex", ".agents"):
                destination = str(parent / name)
                extra.extend(
                    ("--size", str(1024**2), "--tmpfs", destination, "--remount-ro", destination)
                )
        # Only public resolver/trust material; /etc/codex and private keys stay
        # absent. Resolved aliases may not refer into protected roots.
        for name in ("/etc/resolv.conf", "/etc/hosts", "/etc/nsswitch.conf", "/etc/ssl/certs"):
            path = Path(name).resolve(strict=True)
            if any(_overlap(path, root) for root in self.protected_roots):
                raise ContractError("backend network material overlaps protected roots")
            extra.extend(("--ro-bind", str(path), name))
        extra.extend(("--setenv", "HOME", BACKEND_HOME, "--setenv", "CODEX_HOME", BACKEND_HOME))
        position = command.index("--chdir")
        command[position:position] = extra
        return tuple(command)
