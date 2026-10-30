"""Identity-bound Windows process effects and Job Object containment."""

from __future__ import annotations

import subprocess
import time
from collections.abc import Callable
from ctypes import wintypes
from dataclasses import dataclass
from typing import Protocol

from ahc.domain import ProcessIdentity

from ._windows_api import (
    CREATE_SUSPENDED,
    PROCESS_QUERY_LIMITED_INFORMATION,
    PROCESS_SET_QUOTA,
    PROCESS_TERMINATE,
    SYNCHRONIZE,
    NativeIdentityMismatch,
    NativeProcessError,
    NativeProcessHandle,
    active_job_processes,
    assign_to_job,
    close_handle,
    create_kill_on_close_job,
    process_is_in_job,
    resume_initial_threads,
    same_identity,
    terminate_job,
)


class _HandleLike(Protocol):
    @property
    def identity(self) -> ProcessIdentity: ...

    def terminate(self, exit_code: int) -> None: ...

    def close(self) -> None: ...


def terminate_exact(
    identity: ProcessIdentity,
    *,
    exit_code: int = 1,
    opener: Callable[[ProcessIdentity], _HandleLike] | None = None,
) -> None:
    """Compare creation time and terminate through the same stable handle."""

    factory = opener or (
        lambda expected: NativeProcessHandle.open_exact(
            expected,
            access=PROCESS_TERMINATE | PROCESS_QUERY_LIMITED_INFORMATION | SYNCHRONIZE,
        )
    )
    handle = factory(identity)
    try:
        if not same_identity(handle.identity, identity):
            raise NativeIdentityMismatch(f"PID {identity.pid} is a different process generation")
        handle.terminate(exit_code)
    finally:
        handle.close()


@dataclass(frozen=True, slots=True)
class JobTermination:
    complete: bool
    active_processes: int


class WindowsJob:
    """A kill-on-close process container established before service execution."""

    def __init__(self, handle: wintypes.HANDLE) -> None:
        self._handle: wintypes.HANDLE | None = handle

    @classmethod
    def create(cls) -> WindowsJob:
        return cls(create_kill_on_close_job())

    def assign(self, process: NativeProcessHandle) -> None:
        assign_to_job(self._raw(), process)

    def active_processes(self) -> int:
        return active_job_processes(self._raw())

    def contains(self, identity: ProcessIdentity) -> bool:
        process = NativeProcessHandle.open_exact(
            identity,
            access=PROCESS_QUERY_LIMITED_INFORMATION | SYNCHRONIZE,
        )
        try:
            return process_is_in_job(self._raw(), process)
        finally:
            process.close()

    def terminate(self, wait_seconds: float) -> JobTermination:
        if wait_seconds < 0:
            raise ValueError("wait_seconds must be non-negative")
        terminate_job(self._raw())
        deadline = time.monotonic() + wait_seconds
        while True:
            active = self.active_processes()
            if active == 0:
                return JobTermination(True, 0)
            if time.monotonic() >= deadline:
                return JobTermination(False, active)
            time.sleep(min(0.01, max(0.0, deadline - time.monotonic())))

    def close(self) -> None:
        if self._handle is not None:
            close_handle(self._handle)
            self._handle = None

    def _raw(self) -> wintypes.HANDLE:
        if self._handle is None:
            raise NativeProcessError("Job Object handle is closed")
        return self._handle


def spawn_suspended_in_job(
    command: tuple[str, ...],
    *,
    cwd: str | None,
    env: dict[str, str],
    protocol_pipes: bool = False,
) -> tuple[subprocess.Popen[bytes], ProcessIdentity, WindowsJob]:
    """Create suspended, assign to a Job Object, verify, then resume initial threads."""

    if type(protocol_pipes) is not bool:
        raise ValueError("protocol_pipes must be boolean")
    job = WindowsJob.create()
    process: subprocess.Popen[bytes] | None = None
    process_handle: NativeProcessHandle | None = None
    try:
        process = subprocess.Popen(
            command,
            cwd=cwd,
            env=env,
            stdin=subprocess.PIPE if protocol_pipes else subprocess.DEVNULL,
            stdout=subprocess.PIPE if protocol_pipes else subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            close_fds=True,
            creationflags=CREATE_SUSPENDED | (subprocess.CREATE_NO_WINDOW if protocol_pipes else 0),
        )
        process_handle = NativeProcessHandle.open_pid(
            process.pid,
            access=(
                PROCESS_TERMINATE
                | PROCESS_SET_QUOTA
                | PROCESS_QUERY_LIMITED_INFORMATION
                | SYNCHRONIZE
            ),
        )
        job.assign(process_handle)
        identity = process_handle.identity
        resume_initial_threads(process.pid)
        return process, identity, job
    except Exception:
        if process is not None:
            process.kill()  # Popen uses its original Windows process handle.
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                pass
            for stream in (process.stdin, process.stdout, process.stderr):
                if stream is not None:
                    stream.close()
        job.close()
        raise
    finally:
        if process_handle is not None:
            process_handle.close()


__all__ = [
    "JobTermination",
    "NativeIdentityMismatch",
    "NativeProcessError",
    "NativeProcessHandle",
    "WindowsJob",
    "spawn_suspended_in_job",
    "terminate_exact",
]
