"""Local cgroup transport and exact-generation recovery for existing custody."""

import os
import threading
from pathlib import Path, PurePosixPath

from ahc.domain.native_process_identity import (
    NativeCreationAttempt,
    NativeProcessIdentity,
    NativeProcessObservation,
)
from ahc.domain.native_resource_limits import NativeResourceLimits
from ahc.domain.sessions import ContractError
from ahc.infrastructure.backend_custody import BackendCustodyUnavailable


def _open_recorded_group(identity):
    """Return an exact opened group, or proven absence below this verified owner."""
    from ahc.infrastructure.process.linux_native import (
        _DIRECTORY_FLAGS,
        LinuxProcessGroup,
        _service_directory,
    )

    if type(identity) is not NativeProcessIdentity:
        raise BackendCustodyUnavailable("native runner cannot control legacy units")
    parent = directory = kill = events = -1
    try:
        parent = _service_directory()
        root = os.readlink(f"/proc/self/fd/{parent}").removeprefix("/sys/fs/cgroup")
        if (Path("/etc/machine-id").read_text().strip() != identity.host_id
                or PurePosixPath(root).name != identity.service
                or str(PurePosixPath(identity.cgroup).parent) != root):
            raise ValueError()
        name = PurePosixPath(identity.cgroup).name
        try:
            directory = os.open(name, _DIRECTORY_FLAGS, dir_fd=parent)
        except FileNotFoundError:
            return None  # Unique preallocated name; no workload can outlive its cgroup.
        if (Path("/proc/sys/kernel/random/boot_id").read_text().strip() != identity.boot_id
                or os.environ.get("INVOCATION_ID") != identity.invocation_id):
            raise ValueError()  # Another enclosing generation may never be signalled.
        actual = os.fstat(directory)
        if (actual.st_dev, actual.st_ino) != (identity.device, identity.inode):
            raise ValueError()
        kill = os.open("cgroup.kill", os.O_WRONLY | os.O_CLOEXEC, dir_fd=directory)
        events = os.open("cgroup.events", os.O_RDONLY | os.O_CLOEXEC, dir_fd=directory)
        group = object.__new__(LinuxProcessGroup)
        group.parent_fd, group.directory_fd = parent, directory
        group.kill_fd, group.events_fd = kill, events
        group.name, group.owner_pid = name, os.getpid()
        group.claimed, group.kill_attempted, group.complete = True, False, False
        group.lock = threading.Lock()
        parent = directory = kill = events = -1
        return group
    except Exception:
        raise BackendCustodyUnavailable("native process generation unverified") from None
    finally:
        for descriptor in (parent, directory, kill, events):
            if descriptor >= 0:
                os.close(descriptor)


def _close_observation(group):
    # Observation has no cleanup authority: do not remove even an empty directory.
    for field in ("events_fd", "kill_fd", "directory_fd", "parent_fd"):
        descriptor = getattr(group, field)
        if descriptor >= 0:
            os.close(descriptor)
            setattr(group, field, -1)


class NativeProcessRunner:
    def __init__(self, *, cwd, environment, persistent=False, owner_lifetime=False, boundary=None,
                 resource_limits=None):
        from ahc.infrastructure.process.linux_workspace_boundary import LinuxWorkspaceBoundary
        from ahc.infrastructure.process.linux_backend_boundary import LinuxBackendBoundary

        if (type(cwd) is not str or not Path(cwd).is_absolute()
                or type(environment) is not dict or type(persistent) is not bool
                or type(owner_lifetime) is not bool or (owner_lifetime and not persistent)):
            raise ContractError("native runner configuration invalid")
        if resource_limits is not None and type(resource_limits) is not NativeResourceLimits:
            raise ContractError("native runner resource limits invalid")
        if boundary is not None and (
            type(boundary) not in (LinuxWorkspaceBoundary, LinuxBackendBoundary)
            or Path(cwd) != boundary.workspace or environment
        ):
            raise ContractError(
                "isolated runner requires its exact workspace and empty environment")
        self.cwd, self.environment = cwd, dict(environment)
        self._boundary = boundary
        self._resource_limits = resource_limits
        self.persistent, self.owner_lifetime = persistent, owner_lifetime
        self._group = self._owned = self._timer = self._attempt = None
        self._command = self._lifetime = None
        self._launch_command = None
        self._prepared_limits = None
        self._attached = False
        self._expiry_error = None
        self._require_scratch_limits()

    def _require_scratch_limits(self):
        if self._boundary is not None and self._boundary.scratch_limits is not None and (
            self._resource_limits is None or self._resource_limits.swap_bytes != 0
            or self.owner_lifetime
        ):
            raise ContractError(
                "ephemeral runner requires finite process limits, zero swap and a deadline")

    def _wrapped_command(self, command):
        self._require_scratch_limits()
        if self._boundary is None:
            return command
        if self.environment or Path(self.cwd) != self._boundary.workspace:
            raise ContractError("isolated runner configuration changed")
        return self._boundary.command(command)

    def prepare_creation(self, command, *, lifetime_seconds):
        from ahc.infrastructure.process.linux_native import LinuxProcessGroup
        from ahc.domain.execution_budgets import NATIVE_LIFETIME_MAX_SECONDS

        if self._group is not None and self._group.complete:
            self.close()
            self._group = self._owned = self._timer = self._attempt = None
            self._command = self._lifetime = None
            self._launch_command = None
            self._prepared_limits = None
            self._attached = False
            self._expiry_error = None
        if (self._group is not None or type(command) is not tuple or not command
                or any(type(arg) is not str or "\0" in arg for arg in command)
                or not Path(command[0]).is_absolute()
                or type(lifetime_seconds) is not int
                or not 1 <= lifetime_seconds <= NATIVE_LIFETIME_MAX_SECONDS):
            raise ContractError("native creation plan invalid or already owned")
        # Admission happens before allocating the cgroup. Persisted custody still
        # binds the caller's exact command; only this trusted runner wraps it.
        launch = self._wrapped_command(command)
        self._group = LinuxProcessGroup()
        try:
            if self._resource_limits is not None:
                self._group.configure_limits(self._resource_limits)
            self._attempt = NativeCreationAttempt(self._group.identity)
        except BaseException:
            self._group.cleanup(None, 5)
            raise
        self._command, self._lifetime = command, lifetime_seconds
        self._launch_command = launch
        self._prepared_limits = self._resource_limits
        return self._attempt

    def create_attempt(self, attempt, command, *, lifetime_seconds):
        from ahc.infrastructure.process.linux_native import spawn_native

        if (attempt != self._attempt or command != self._command
                or lifetime_seconds != self._lifetime or self._owned is not None):
            raise ContractError("native creation differs from durable attempt")
        if self._resource_limits != self._prepared_limits:
            raise ContractError("native resource limits changed before launch")
        # Recheck the selected boundary before the effect. A refused/changed
        # wrapper cannot fall back to an unconfined launch. Existing custody owns
        # the reserved group and reconciles any failed creation attempt.
        launch = self._wrapped_command(command)
        if launch != self._launch_command:
            raise ContractError("native isolation plan changed before launch")
        if self._resource_limits is not None:
            self._group.verify_limits(self._resource_limits)
        self._owned = spawn_native(launch, cwd=self.cwd, env=self.environment,
                                   protocol_pipes=True, group=self._group)
        if not self.owner_lifetime:
            self._timer = threading.Timer(lifetime_seconds, self._expire, args=(self._owned,))
            self._timer.daemon = True
            self._timer.start()
        return attempt.identity

    def _expire(self, owned):
        try:
            if not owned.cleanup(5).complete and self._owned is owned:
                self._expiry_error = "cleanup-incomplete"
        except Exception:
            if self._owned is owned:
                self._expiry_error = "cleanup-unverified"

    def reconcile_creation(self, attempt):
        if type(attempt) is not NativeCreationAttempt:
            raise BackendCustodyUnavailable("native runner cannot reconcile legacy attempts")
        self.observe(attempt.identity)  # Current owner and full group identity, no workload replay.
        return attempt.identity

    def observe(self, identity):
        group = _open_recorded_group(identity)
        if group is None:
            return NativeProcessObservation(identity, "absent", True)
        try:
            empty = not group._populated()
            return NativeProcessObservation(identity, "stopped" if empty else "running", empty)
        except Exception:
            raise BackendCustodyUnavailable("native process observation unavailable") from None
        finally:
            _close_observation(group)

    def stop(self, identity):
        if self._attempt is not None and identity == self._attempt.identity:
            return self.close()
        group = _open_recorded_group(identity)
        if group is None:
            return
        try:
            if not group.cleanup(None, 5).complete:
                raise BackendCustodyUnavailable("native process stop incomplete")
        except Exception as exc:
            error = BackendCustodyUnavailable("native process stop unverified")
            error.native_owner = group
            raise error from exc

    def attach(self, identity, custody, lease):
        from .codex_transport import CodexStdioTransport

        if (self._attempt is None or identity != self._attempt.identity
                or self._owned is None or self._attached):
            raise ContractError("native stream identity mismatch or already attached")
        with custody.attachment(lease, identity):
            wire = CodexStdioTransport(self._owned, streaming=self.persistent,
                                       max_wait=None if self.owner_lifetime else self._lifetime)
            self._attached = True
            return wire

    def control_ready(self):
        try:
            from ahc.infrastructure.process.linux_native import _service_directory
            descriptor = _service_directory()
            os.close(descriptor)
            return True
        except Exception:
            return False

    def close(self):
        try:
            if (self._group is not None and not self._group.complete
                    and self._attempt is not None
                    and self.observe(self._attempt.identity).state == "absent"):
                # Another custody reconciler can already have removed this exact
                # UUID group. Dead cgroup descriptors return ENODEV, not an empty
                # events file; descriptor link counts do not indicate retirement.
                # Qualified owner + exact recorded path absence proves retirement.
                # No PID or replacement path is used as a signal target.
                if self._owned is not None:
                    self._owned.process.wait(timeout=5)
                self._group.complete = True
                self._group._release_empty()
                from ahc.infrastructure.process.managed import CleanupResult
                result = CleanupResult(True)
            elif self._owned is not None:
                result = self._owned.cleanup(5)
            elif self._group is not None:
                result = self._group.cleanup(None, 5)
            else:
                return
            if not result.complete:
                raise BackendCustodyUnavailable("native process cleanup incomplete")
            if self._timer is not None:
                self._timer.cancel()
            if self._owned is not None:
                for stream in (self._owned.process.stdin, self._owned.process.stdout):
                    if stream is not None:
                        stream.close()
        except Exception:
            raise BackendCustodyUnavailable("native process cleanup unverified") from None
