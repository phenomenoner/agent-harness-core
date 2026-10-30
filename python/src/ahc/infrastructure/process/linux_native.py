"""Native process custody below an explicitly delegated systemd user service.

Not a sandbox against hostile programs running as the same user. No workload is
released before cgroup membership and stable cleanup descriptors are established.
"""

from __future__ import annotations

import math
import os
import select
import socket
import stat
import subprocess
import sys
import threading
import time
from pathlib import Path
from uuid import uuid4

from ahc.domain.native_service_owner import service_path, validate_owner

from .identity import identity_for_pid
from .managed import CleanupResult, ManagedProcess, ProcessCleanupError

_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
_BOOTSTRAP = """import os,socket,sys
control=socket.socket(fileno=int(sys.argv[1]))
control.settimeout(10)
try:
    group=int(sys.argv[2])
    target=os.open('cgroup.procs',os.O_WRONLY,dir_fd=group)
    os.write(target,str(os.getpid()).encode('ascii'))
    os.close(target)
    os.close(group)
    control.sendall(b'R')
    if control.recv(1)!=b'G':
        raise RuntimeError()
    control.close()
    os.execve(sys.argv[3],sys.argv[3:],os.environ)
except BaseException:
    os._exit(78)
"""


def _service_directory(*, setup: bool = False) -> int:
    """Observe the enclosing owner before allocating a new process generation."""
    if sys.platform != "linux" or os.geteuid() == 0:
        raise ProcessCleanupError("native process owner requires non-root Linux")
    try:
        lines = Path("/proc/self/cgroup").read_text().splitlines()
        if len(lines) != 1 or not lines[0].startswith("0::/"):
            raise ValueError()
        relative = lines[0][3:]
        path = service_path(relative, setup=setup)
        properties = subprocess.run(
            ("/usr/bin/systemctl", "--user", "show", path.name,
             "--property=ControlGroup,InvocationID,Delegate,KillMode,SendSIGKILL,DelegateSubgroup"),
            capture_output=True, text=True, timeout=5, check=True,
            env={"PATH": "/usr/bin:/bin", "LC_ALL": "C",
                 "XDG_RUNTIME_DIR": f"/run/user/{os.geteuid()}"},
        )
        fields = dict(line.split("=", 1) for line in properties.stdout.splitlines())
        invocation = os.environ.get("INVOCATION_ID", "")
        path = validate_owner(relative, invocation, fields, setup=setup)
        descriptor = os.open("/sys/fs/cgroup" + str(path), _DIRECTORY_FLAGS)
        if os.fstat(descriptor).st_uid != os.geteuid():
            os.close(descriptor)
            raise ValueError()
        return descriptor
    except (OSError, ValueError, subprocess.SubprocessError):
        raise ProcessCleanupError("native execution requires a delegated user service") from None


class LinuxProcessGroup:
    """One ephemeral UUID group. Kill authority never reopens a path or PID."""

    def __init__(self):
        self.parent_fd = _service_directory()
        self.owner_pid = os.getpid()
        self.claimed = False
        self.name = "ahc-process-" + uuid4().hex
        self.directory_fd = self.kill_fd = self.events_fd = -1
        self.kill_attempted = False
        self.complete = False
        self.lock = threading.Lock()
        try:
            os.mkdir(self.name, dir_fd=self.parent_fd)
            self.directory_fd = os.open(self.name, _DIRECTORY_FLAGS, dir_fd=self.parent_fd)
            self.kill_fd = os.open("cgroup.kill", os.O_WRONLY | os.O_CLOEXEC,
                                   dir_fd=self.directory_fd)
            self.events_fd = os.open("cgroup.events", os.O_RDONLY | os.O_CLOEXEC,
                                     dir_fd=self.directory_fd)
        except BaseException:
            self._release_empty()
            raise

    @property
    def identity(self):
        from ahc.domain.native_process_identity import NativeProcessIdentity

        parent = os.readlink(f"/proc/self/fd/{self.parent_fd}")
        if not parent.startswith("/sys/fs/cgroup/"):
            raise ProcessCleanupError("native cgroup parent unavailable")
        info = os.fstat(self.directory_fd)
        return NativeProcessIdentity(
            Path("/etc/machine-id").read_text().strip(),
            Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
            Path(parent).name, os.environ["INVOCATION_ID"],
            parent.removeprefix("/sys/fs/cgroup") + "/" + self.name,
            info.st_dev, info.st_ino)

    def _populated(self) -> bool:
        raw = os.pread(self.events_fd, 4096, 0).decode("ascii")
        fields = dict(line.split() for line in raw.splitlines())
        if fields.get("populated") not in ("0", "1"):
            raise ValueError("unavailable cgroup population")
        return fields["populated"] == "1"

    def configure_limits(self, limits):
        from .linux_resource_limits import configure_limits

        configure_limits(self, limits)

    def verify_limits(self, limits):
        from .linux_resource_limits import verify_limits

        verify_limits(self, limits)

    def _wait_empty(self, deadline: float) -> bool:
        poller = select.poll()
        poller.register(self.events_fd, select.POLLPRI | select.POLLERR)
        while self._populated():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            poller.poll(max(1, math.ceil(remaining * 1000)))
        return True

    def cleanup(self, process, wait_seconds: float) -> CleanupResult:
        if (type(wait_seconds) not in (int, float) or not math.isfinite(wait_seconds)
                or not 0 <= wait_seconds <= 30):
            raise ValueError("native cleanup timeout must be between 0 and 30 seconds")
        with self.lock:
            if self.complete:
                return CleanupResult(True)
            deadline = time.monotonic() + wait_seconds
            try:
                if not self.kill_attempted and self._populated():
                    # Mark before the effect: a lost/failed acknowledgement cannot
                    # authorize a second write. Later calls only observe completion.
                    self.kill_attempted = True
                    os.write(self.kill_fd, b"1")
                if not self._wait_empty(deadline):
                    return CleanupResult(False, 1)  # Lower bound, not a PID inventory.
                if process is not None:
                    try:
                        process.wait(timeout=max(0, deadline - time.monotonic()))
                    except subprocess.TimeoutExpired:
                        return CleanupResult(False, 1)
                self.complete = True
                self._release_empty()
                return CleanupResult(True)
            except (OSError, ValueError):
                raise ProcessCleanupError("native process cleanup remains unverified") from None

    def _release_empty(self):
        # UUID names are never reused by supported owners. If a name has already
        # been replaced, preserve that directory. It is never a signal target.
        if self.directory_fd >= 0:
            try:
                actual = os.stat(self.name, dir_fd=self.parent_fd, follow_symlinks=False)
                held = os.fstat(self.directory_fd)
                if (stat.S_ISDIR(actual.st_mode)
                        and (actual.st_dev, actual.st_ino) == (held.st_dev, held.st_ino)):
                    os.rmdir(self.name, dir_fd=self.parent_fd)
            except OSError:
                pass  # Empty bookkeeping may remain until enclosing service stop.
        for field in ("events_fd", "kill_fd", "directory_fd", "parent_fd"):
            descriptor = getattr(self, field)
            if descriptor >= 0:
                os.close(descriptor)
                setattr(self, field, -1)


def spawn_native(command, *, cwd, env, protocol_pipes=False, group=None) -> ManagedProcess:
    if (type(command) is not tuple or not command or len(command) > 1024
            or any(type(arg) is not str or "\0" in arg for arg in command)
            or not Path(command[0]).is_absolute()
            or type(env) is not dict or type(protocol_pipes) is not bool
            or any(type(k) is not str or type(v) is not str or "=" in k
                   or "\0" in k or "\0" in v for k, v in env.items())
            or (cwd is not None and (type(cwd) is not str or not Path(cwd).is_absolute()))):
        raise ValueError("invalid explicit native process command or environment")
    group = LinuxProcessGroup() if group is None else group
    if type(group) is not LinuxProcessGroup:
        raise ProcessCleanupError("native process group invalid")
    with group.lock:
        if group.owner_pid != os.getpid() or group.claimed or group.complete or group.kill_attempted:
            raise ProcessCleanupError("native process group already used or foreign")
        group.claimed = True
    parent = child = None
    process = None
    try:
        parent, child = socket.socketpair()
        parent.settimeout(5)
        process = subprocess.Popen(
            (sys.executable, "-I", "-u", "-c", _BOOTSTRAP,
             str(child.fileno()), str(group.directory_fd), *command),
            cwd=cwd, env=env, close_fds=True,
            pass_fds=(child.fileno(), group.directory_fd),
            stdin=subprocess.PIPE if protocol_pipes else subprocess.DEVNULL,
            stdout=subprocess.PIPE if protocol_pipes else subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        child.close()
        child = None
        if parent.recv(1) != b"R":
            raise ProcessCleanupError("native process membership was not acknowledged")
        identity = identity_for_pid(process.pid)
        owned = ManagedProcess(process, identity, _linux_owner=group)
        parent.sendall(b"G")
        return owned
    except BaseException:
        if parent is not None:
            parent.close()  # EOF never admits the waiting bootstrap.
            parent = None
        try:
            result = group.cleanup(process, 5)
            if not result.complete:
                raise ProcessCleanupError("native launch cleanup remains unverified")
        except BaseException as exc:
            # Retain the owner on the exception; enclosing service remains the
            # final crash boundary even if the caller cannot recover this object.
            exc.native_owner = group
            exc.native_process = process
            raise
        finally:
            if process is not None:
                for stream in (process.stdin, process.stdout, process.stderr):
                    if stream is not None:
                        stream.close()
        raise
    finally:
        for endpoint in (parent, child):
            if endpoint is not None:
                endpoint.close()
