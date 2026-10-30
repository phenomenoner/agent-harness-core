from __future__ import annotations

import os
import sys
import threading
import time
from typing import cast

import psutil
import pytest

from ahc import AppConfig, RestartPolicy, ServiceSpec, Supervisor, inspect_status, request_stop
from ahc.application.lifecycle import LEASE_BUSY, SUPERVISOR_FAILURE
from ahc.application.supervisor import _Child
from ahc.infrastructure.persistence import StateStore
from ahc.infrastructure.process import current_identity
from ahc.infrastructure.process.managed import CleanupResult, ManagedProcess


def _wait_for(predicate, timeout: float = 2.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return predicate()


def _sleep_service(seconds: int = 30) -> tuple[str, ...]:
    return ("{python}", "-c", f"import time; time.sleep({seconds})")


@pytest.mark.skipif(os.name != "nt", reason="Windows Job Object ownership contract")
def test_foreground_status_stop_cleans_configured_child(tmp_path) -> None:
    config = AppConfig(
        state_dir=tmp_path,
        services=(ServiceSpec("worker", _sleep_service()),),
        poll_interval_seconds=0.01,
        child_stop_timeout_seconds=1.0,
    )
    supervisor = Supervisor(config)
    result: list[int] = []
    thread = threading.Thread(target=lambda: result.append(supervisor.run()), daemon=True)
    thread.start()
    child: dict[str, object] | None = None
    try:
        assert _wait_for(lambda: inspect_status(config)[1].get("status") == "running")
        code, running = inspect_status(config)
        assert code == 0
        child = running["children"]["worker"]  # type: ignore[index]
        identity = child["identity"]  # type: ignore[index]
        assert identity["pid"] > 0  # type: ignore[index]
        stop_code, stopped = request_stop(config, wait_seconds=2.0)
        assert stop_code == 0
        assert stopped["status"] == "stopped"
    finally:
        if thread.is_alive():
            request_stop(config, wait_seconds=2.0)
        thread.join(timeout=3)
    assert result == [0]
    assert inspect_status(config)[1]["status"] == "stopped"
    assert child is not None
    identity = child["identity"]  # type: ignore[index]
    assert not psutil.pid_exists(identity["pid"])  # type: ignore[index]


@pytest.mark.skipif(os.name != "nt", reason="Windows Job Object ownership contract")
def test_restart_policy_delay_and_bound_are_applied(tmp_path) -> None:
    failing = (sys.executable, "-c", "raise SystemExit(7)")
    config = AppConfig(
        state_dir=tmp_path,
        services=(
            ServiceSpec(
                "flaky",
                failing,
                RestartPolicy.ON_FAILURE,
                max_restarts=2,
                restart_delay_seconds=0.02,
            ),
        ),
        poll_interval_seconds=0.01,
    )
    started = time.monotonic()
    assert Supervisor(config).run() == SUPERVISOR_FAILURE
    elapsed = time.monotonic() - started
    snapshot = StateStore(tmp_path).read()
    child = snapshot["children"]["flaky"]  # type: ignore[index]
    assert child["restarts"] == 2  # type: ignore[index]
    assert child["return_code"] == 7  # type: ignore[index]
    assert snapshot["status"] == "degraded"
    assert elapsed >= 0.04


def test_lease_blocks_second_foreground_supervisor(tmp_path) -> None:
    from ahc.infrastructure.process import InstanceLease

    config = AppConfig(state_dir=tmp_path)
    lease = InstanceLease(config.lease_path)
    lease.acquire()
    try:
        assert Supervisor(config).run(max_runtime_seconds=0) == LEASE_BUSY
    finally:
        lease.release()
    assert Supervisor(config).run(max_runtime_seconds=0) == 0


def test_status_marks_dead_generation_stale_without_mutation(tmp_path) -> None:
    config = AppConfig(state_dir=tmp_path)
    StateStore(tmp_path).write(
        {
            "schema_version": 1,
            "status": "running",
            "supervisor": {"pid": 999_999_999, "start_time": 1.0},
            "children": {},
            "started_at": 1.0,
            "updated_at": 2.0,
        }
    )
    before = config.state_path.read_bytes()
    code, status = inspect_status(config)
    assert code == 1
    assert status["status"] == "stale"
    assert config.state_path.read_bytes() == before


@pytest.mark.skipif(os.name != "nt", reason="Windows Job Object ownership contract")
def test_long_restart_delay_remains_responsive_to_stop(tmp_path) -> None:
    config = AppConfig(
        state_dir=tmp_path,
        services=(
            ServiceSpec(
                "delayed-restart",
                ("{python}", "-c", "raise SystemExit(7)"),
                RestartPolicy.ON_FAILURE,
                max_restarts=1,
                restart_delay_seconds=5.0,
            ),
        ),
        poll_interval_seconds=0.01,
    )
    supervisor = Supervisor(config)
    result: list[int] = []
    thread = threading.Thread(target=lambda: result.append(supervisor.run()), daemon=True)
    thread.start()

    def child_exited() -> bool:
        children = inspect_status(config)[1].get("children")
        if not isinstance(children, dict):
            return False
        child = children.get("delayed-restart")
        return isinstance(child, dict) and child.get("return_code") == 7

    try:
        assert _wait_for(child_exited)
        started = time.monotonic()
        stop_code, stopped = request_stop(config, wait_seconds=1.0)
        assert time.monotonic() - started < 1.0
        assert stop_code == 0
        assert stopped["status"] == "stopped"
    finally:
        thread.join(timeout=2)
    assert result == [0]


@pytest.mark.skipif(os.name != "nt", reason="Windows Job Object ownership contract")
def test_state_publication_failure_after_spawn_still_cleans_child(tmp_path) -> None:
    config = AppConfig(
        state_dir=tmp_path,
        services=(ServiceSpec("worker", _sleep_service()),),
        child_stop_timeout_seconds=2.0,
    )
    supervisor = Supervisor(config)
    real_write = supervisor.state.write
    calls = 0

    def fail_after_starting(snapshot) -> None:
        nonlocal calls
        calls += 1
        if calls > 1:
            raise OSError("injected state publication failure")
        real_write(snapshot)

    supervisor.state.write = fail_after_starting  # type: ignore[method-assign]
    assert supervisor.run(max_runtime_seconds=1.0) == SUPERVISOR_FAILURE
    child = supervisor._children["worker"]
    assert child.identity is not None
    assert not psutil.pid_exists(child.identity.pid)
    assert "state publication failed" in (supervisor._failure or "")


class _ExitedProcess:
    returncode = 0

    @staticmethod
    def poll() -> int:
        return 0

    @staticmethod
    def wait(timeout: float) -> int:
        del timeout
        return 0


class _IncompleteManaged:
    process = _ExitedProcess()
    identity = current_identity()

    @staticmethod
    def cleanup(wait_seconds: float) -> CleanupResult:
        del wait_seconds
        return CleanupResult(False, remaining_count=1)


def test_incomplete_cleanup_publishes_degraded_terminal_truth(tmp_path) -> None:
    config = AppConfig(state_dir=tmp_path)
    supervisor = Supervisor(config)
    supervisor._identity = current_identity()
    supervisor._started_at = time.time()
    spec = ServiceSpec("worker", _sleep_service())
    supervisor._children[spec.name] = _Child(
        spec,
        cast(ManagedProcess, _IncompleteManaged()),
    )

    supervisor._shutdown()

    snapshot = StateStore(tmp_path).read()
    assert snapshot["status"] == "degraded"
    error = snapshot["error"]
    assert isinstance(error, str)
    assert "cleanup incomplete" in error
