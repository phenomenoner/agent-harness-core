"""Pre-effect absence preserves foreground control and pending work; unknown stays fatal."""

import pytest
from test_foreground_runtime import setup

from ahc.application.foreground_runtime import ForegroundRuntime
from ahc.application.workspace_availability import require_workspace_launcher
from ahc.domain.sessions import ContractError
from ahc.infrastructure.runtime_inbox import RuntimeInbox


def test_missing_launcher_preserves_queue_without_retry_and_fresh_start_can_run(tmp_path):
    runtime, requests, invoked, request = setup(tmp_path)
    original = runtime.open_workspace
    attempts = []
    launcher = tmp_path / "launcher.fixture"

    def open_workspace(snapshot):
        attempts.append(True)
        require_workspace_launcher(launcher)
        return original(snapshot)

    runtime.open_workspace = open_workspace
    inbox = RuntimeInbox(runtime.store)
    inbox.submit(request.session, request.operation_id, request.goal_digest, request.input_text)
    requests.append(request)
    runtime.start()
    assert runtime.state == "running"
    assert runtime.workspace_status()["status"] == "unavailable"
    assert runtime.workspace_status()["automatic_retry"] is False
    for _ in range(3):
        assert runtime.step() is None
    assert len(attempts) == 1 and requests == [request] and invoked == []
    assert inbox.query(request.session, request.operation_id)["status"] == "pending"
    saved_session = runtime.session
    runtime.session = saved_session.successor()
    with pytest.raises(ContractError, match="changed"):
        runtime.step()
    runtime.session = saved_session
    runtime.close()
    with runtime.store.foreground_lease(request.session):
        pass
    # A local fixture proves preflight passage; the real workspace here remains the test port.
    launcher.write_text("fixture")
    successor = ForegroundRuntime(runtime.store, request.session, runtime.ingress,
                                 runtime.backend_factory, open_workspace=open_workspace)
    successor.start()
    try:
        assert successor.workspace_status()["status"] == "opened"
        assert successor.step().result["accepted"]
        assert len(attempts) == 2 and invoked == [request.operation_id]
    finally:
        successor.close()


@pytest.mark.parametrize("error", [RuntimeError("private"), FileNotFoundError("private")])
def test_generic_open_failure_does_not_become_preflight_absence(tmp_path, error):
    runtime, _, invoked, _ = setup(tmp_path)

    def fail(snapshot):
        raise error

    runtime.open_workspace = fail
    try:
        with pytest.raises(type(error)):
            runtime.start()
        assert runtime.state == "starting"
        assert runtime.workspace_status()["status"] != "unavailable"
        assert invoked == []
    finally:
        runtime.close()


def test_trusted_composition_missing_launcher_precedes_recovery_and_creation(tmp_path, monkeypatch):
    import json
    import subprocess

    from test_foreground_trusted import config_file

    from ahc.cli.foreground import compose_foreground
    from ahc.cli.trusted_codex_backend import TrustedCodexBackendFactory

    original, request, raw, path = config_file(tmp_path, enabled=True)
    raw["policy"]["workspace_granted"] = True
    raw["workspace"]["wsl_executable"] = str(tmp_path / "missing-wsl.exe")
    path.write_text(json.dumps(raw))
    inbox = RuntimeInbox(original.store)
    inbox.submit(request.session, "pending", request.goal_digest, "work")

    def forbidden(*args, **kwargs):
        pytest.fail("missing launcher attempted recovery or process creation")

    monkeypatch.setattr(TrustedCodexBackendFactory, "recover", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    runtime = compose_foreground(path)
    runtime.start()
    try:
        assert runtime.workspace_status()["reason_code"] == "host-launcher-unavailable"
        assert runtime.step() is None
        assert inbox.query(request.session, "pending")["status"] == "pending"
    finally:
        runtime.close()


def test_supervisor_retains_control_and_closes_degraded_owner(tmp_path):
    from ahc import AppConfig, Supervisor

    runtime, requests, invoked, request = setup(tmp_path)
    attempts = []

    def unavailable(snapshot):
        attempts.append(True)
        require_workspace_launcher(tmp_path / "absent-launcher")

    runtime.open_workspace = unavailable
    requests.append(request)
    config = AppConfig(state_dir=tmp_path / "control", poll_interval_seconds=0.001)
    assert Supervisor(config, runtime=runtime).run(max_runtime_seconds=0.02) == 0
    assert runtime.state == "closed" and requests == [request] and invoked == []
    assert attempts == [True] and runtime.workspace_status()["status"] == "unavailable"
    with runtime.store.foreground_lease(request.session):
        pass
