"""Windows-native falsifiers for process ownership and containment."""

from __future__ import annotations

import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

from ahc.domain import ProcessIdentity

pytestmark = pytest.mark.skipif(os.name != "nt", reason="Windows-native process contract")


def test_protocol_pipe_assignment_failure_closes_all_created_streams(monkeypatch):
    import io

    from ahc.infrastructure.process import windows_native as native

    class Process:
        pid = 1
        stdin = io.BytesIO()
        stdout = io.BytesIO()
        stderr = None
        killed = False

        def kill(self):
            self.killed = True

        def wait(self, timeout):
            return 0

    class Handle:
        closed = False

        def close(self):
            self.closed = True

    class Job(Handle):
        def assign(self, _handle):
            raise RuntimeError("fixture assignment failure")

    process, handle, job = Process(), Handle(), Job()
    monkeypatch.setattr(native.subprocess, "Popen", lambda *args, **kwargs: process)
    monkeypatch.setattr(native.WindowsJob, "create", lambda: job)
    monkeypatch.setattr(native.NativeProcessHandle, "open_pid", lambda *args, **kwargs: handle)
    with pytest.raises(RuntimeError, match="fixture assignment failure"):
        native.spawn_suspended_in_job(("fixture",), cwd=None, env={}, protocol_pipes=True)
    assert process.killed and process.stdin.closed and process.stdout.closed
    assert handle.closed and job.closed


@dataclass
class _Target:
    terminated: bool = False


class _ReplacingHandle:
    def __init__(
        self,
        identity: ProcessIdentity,
        bound: _Target,
        replacement: _Target,
        registry: dict[int, _Target],
    ) -> None:
        self._identity = identity
        self._bound = bound
        self._replacement = replacement
        self._registry = registry
        self.closed = False

    @property
    def identity(self) -> ProcessIdentity:
        self._registry[self._identity.pid] = self._replacement
        return self._identity

    def terminate(self, exit_code: int) -> None:
        del exit_code
        self._bound.terminated = True

    def close(self) -> None:
        self.closed = True


def test_nonwindows_managed_service_execution_fails_closed(monkeypatch) -> None:
    from ahc.infrastructure.process import managed

    monkeypatch.setattr(managed, "os", SimpleNamespace(name="posix"))
    with pytest.raises(managed.ProcessCleanupError, match="requires Windows Job Objects"):
        managed.spawn_managed(
            (sys.executable, "-c", "raise SystemExit"),
            cwd=None,
            env=os.environ.copy(),
        )


def test_stable_handle_does_not_terminate_replacement_after_final_observation() -> None:
    from ahc.infrastructure.process.windows_native import terminate_exact

    identity = ProcessIdentity(pid=1234, start_time=10.0)
    original = _Target()
    replacement = _Target()
    registry = {identity.pid: original}
    handle = _ReplacingHandle(identity, original, replacement, registry)

    terminate_exact(identity, opener=lambda _expected: handle)

    assert registry[identity.pid] is replacement
    assert original.terminated is True
    assert replacement.terminated is False
    assert handle.closed is True


def test_mismatched_handle_refuses_effect() -> None:
    from ahc.infrastructure.process.windows_native import (
        NativeIdentityMismatch,
        terminate_exact,
    )

    expected = ProcessIdentity(pid=1234, start_time=10.0)
    wrong = ProcessIdentity(pid=1234, start_time=11.0)
    original = _Target()
    replacement = _Target()
    registry = {expected.pid: replacement}
    handle = _ReplacingHandle(wrong, original, replacement, registry)

    with pytest.raises(NativeIdentityMismatch):
        terminate_exact(expected, opener=lambda _expected: handle)

    assert original.terminated is False
    assert replacement.terminated is False
    assert handle.closed is True


def test_native_handle_refuses_wrong_generation_then_terminates_exact_child() -> None:
    from ahc.infrastructure.process.identity import identity_for_pid
    from ahc.infrastructure.process.windows_native import (
        NativeIdentityMismatch,
        terminate_exact,
    )

    process = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(60)"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    identity = identity_for_pid(process.pid)
    wrong = ProcessIdentity(identity.pid, identity.start_time + 1.0)
    try:
        with pytest.raises(NativeIdentityMismatch):
            terminate_exact(wrong)
        assert process.poll() is None
        terminate_exact(identity)
        assert process.wait(timeout=5) != 0
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)


def test_job_cleanup_reaps_descendant_after_root_already_exited(tmp_path: Path) -> None:
    from ahc.infrastructure.process.identity import identity_for_pid, identity_matches
    from ahc.infrastructure.process.managed import spawn_managed
    from ahc.infrastructure.process.windows_native import WindowsJob

    pid_file = tmp_path / "grandchild.pid"
    child_code = (
        "import pathlib,subprocess,sys; "
        "p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)']); "
        f"pathlib.Path({str(pid_file)!r}).write_text(str(p.pid), encoding='utf-8')"
    )
    managed = spawn_managed(
        (sys.executable, "-c", child_code),
        cwd=str(tmp_path),
        env=os.environ.copy(),
    )

    managed.process.wait(timeout=5)
    deadline = time.monotonic() + 5
    while not pid_file.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    grandchild_pid = int(pid_file.read_text(encoding="utf-8"))
    grandchild_identity = identity_for_pid(grandchild_pid)
    job = cast(WindowsJob, managed._job)
    assert job.contains(grandchild_identity) is True
    assert job.active_processes() >= 1

    result = managed.cleanup(5.0)

    assert result.complete is True
    assert result.remaining_count == 0
    assert managed.process.poll() is not None
    deadline = time.monotonic() + 2
    while identity_matches(grandchild_identity) and time.monotonic() < deadline:
        time.sleep(0.01)
    assert not identity_matches(grandchild_identity)
