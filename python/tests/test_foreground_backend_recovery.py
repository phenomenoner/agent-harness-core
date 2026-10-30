"""Startup reconciles backend custody before replacement workspace or input admission."""

import json

import pytest
from test_backend_custody import Runner, held_count
from test_cli_foreground import configuration, wsl_backend
from test_workspace_tools import Port
from test_wsl_unit_runner import IDENTITY

from ahc.cli import wsl_codex_backend
from ahc.application.runtime_service import RuntimeService
from ahc.cli.foreground import compose_foreground
from ahc.infrastructure.backend_custody import BackendCustody
from ahc.infrastructure.runtime_inbox import RuntimeInbox


@pytest.mark.parametrize("fail_stop", [False, True])
def test_restart_recovery_precedes_workspace_and_does_not_need_input(
    tmp_path, monkeypatch, fail_stop
):
    original, request, payload, path, _ = configuration(tmp_path)
    store = original.store
    current = store.read_current(request.session.ref.virtual_session_id)
    workspace = store.reserve_workspace_generation(current, "work")
    custody = BackendCustody(store)
    lease = custody.acquire(request.session, "work")
    try:
        custody.record_created(lease, workspace, IDENTITY)
    finally:
        lease.close()
    payload["backend"] = {**wsl_backend(enabled=True), "distribution": IDENTITY.distribution}
    path.write_text(json.dumps(payload))
    runner = Runner(IDENTITY, fail_stop=fail_stop)
    monkeypatch.setattr(wsl_codex_backend, "WslUnitRunner", lambda *a: runner)
    opened = []

    def open_workspace(service, expected, workspace_id, docker, **kwargs):
        assert held_count(store) == 0
        opened.append(True)
        return service.open_workspace(expected, workspace_id, Port)

    monkeypatch.setattr(RuntimeService, "open_docker_workspace", open_workspace)
    inbox = RuntimeInbox(store)
    if fail_stop:
        inbox.submit(request.session, "queued", request.goal_digest, "retain input")
    runtime = compose_foreground(path)
    try:
        if fail_stop:
            runtime.start()
            assert runtime.state == "running" and runtime.admission_status()["ready"] is False
            assert runtime.step() is None
            assert not opened and held_count(store) == 1
            assert inbox.query(request.session, "queued")["status"] == "pending"
            assert store.read_turn(request.session, "queued") is None
        else:
            runtime.start()
            assert opened == [True] and held_count(store) == 0
            assert runtime.step() is None
        assert runner.calls == ["observe", "stop", "observe"]
        # Both success and failure release the startup lease without erasing held state.
        retry_lease = custody.acquire(request.session, "work")
        retry_lease.close()
    finally:
        runtime.close()
