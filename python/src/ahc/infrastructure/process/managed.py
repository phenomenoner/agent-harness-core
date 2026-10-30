"""Owned processes: Windows Jobs or a delegated Linux service cgroup."""

from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass, field
from typing import Protocol

from ahc.domain import ProcessIdentity


class ProcessCleanupError(RuntimeError):
    """Owned process cleanup could not be completed authoritatively."""


@dataclass(frozen=True, slots=True)
class CleanupResult:
    complete: bool
    remaining_count: int = 0


class _JobTerminationLike(Protocol):
    complete: bool
    active_processes: int


class _JobLike(Protocol):
    def terminate(self, wait_seconds: float) -> _JobTerminationLike: ...

    def close(self) -> None: ...


@dataclass
class ManagedProcess:
    process: subprocess.Popen[bytes]
    identity: ProcessIdentity
    _job: _JobLike | None = None
    _linux_owner: object | None = None
    _cleanup_result: CleanupResult | None = field(default=None, init=False)

    def cleanup(self, wait_seconds: float) -> CleanupResult:
        if self._cleanup_result is not None:
            return self._cleanup_result
        if self._linux_owner is not None:
            result = self._linux_owner.cleanup(self.process, wait_seconds)
            if result.complete:
                self._cleanup_result = result
            return result
        if self._job is not None:
            self._cleanup_result = self._cleanup_windows_job(wait_seconds)
            return self._cleanup_result
        raise ProcessCleanupError("managed service cleanup requires Windows Job Objects")

    def _cleanup_windows_job(self, wait_seconds: float) -> CleanupResult:
        job, self._job = self._job, None
        try:
            result = job.terminate(wait_seconds)
            return CleanupResult(result.complete, result.active_processes)
        except Exception as exc:
            raise ProcessCleanupError(str(exc)) from exc
        finally:
            job.close()


def spawn_managed(
    command: tuple[str, ...],
    *,
    cwd: str | None,
    env: dict[str, str],
    protocol_pipes: bool = False,
) -> ManagedProcess:
    if os.name == "nt":
        from .windows_native import spawn_suspended_in_job

        process, identity, job = spawn_suspended_in_job(
            command, cwd=cwd, env=env, protocol_pipes=protocol_pipes
        )
        return ManagedProcess(process, identity, job)

    if sys.platform == "linux":
        from .linux_native import spawn_native

        return spawn_native(command, cwd=cwd, env=env, protocol_pipes=protocol_pipes)

    raise ProcessCleanupError("managed service execution requires Windows Job Objects")
