import json
import sys

import pytest
from test_foreground_runtime import setup
from test_workspace_tools import Port
from test_wsl_codex_backend import SPEC

from ahc.application.runtime_service import RuntimeService
from ahc.cli import main
from ahc.infrastructure.runtime_inbox import RuntimeInbox


def wsl_backend(*, enabled=False):
    return dict(
        kind="codex-wsl-offline",
        enabled=enabled,
        host_executable="C:/Windows/System32/wsl.exe",
        distribution="AHC",
        launch_spec=SPEC,
        model="fixture",
        turn_timeout=30,
        lifetime_seconds=180,
    )


def configuration(tmp_path):
    runtime, _, _, request = setup(tmp_path)
    client = tmp_path / "docker-client"
    client.mkdir()
    payload = {
        "schema_version": "ahc.foreground.config.v1",
        "runtime_root": str(runtime.store.root),
        "session": request.session.to_dict(),
        "workspace_id": "work",
        "docker_executable": sys.executable,
        "docker_client_config": str(client),
        "checkpoint_digest": None,
        "backend": "unavailable",
    }
    path = tmp_path / "foreground.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    config = tmp_path / "app.toml"
    config.write_text(
        'config_version="ahc.standalone.config.v1"\nstate_dir="state"\nservices=[]\n',
        encoding="utf-8",
    )
    return runtime, request, payload, path, config


def test_run_owns_workspace_and_preserves_pending_when_backend_unavailable(
    tmp_path, monkeypatch, capsys
):
    runtime, request, _, path, config = configuration(tmp_path)
    inbox = RuntimeInbox(runtime.store)
    inbox.submit(request.session, "pending", request.goal_digest, "input")
    opened = []

    def open_workspace(service, expected, workspace_id, runner, **kwargs):
        workspace = service.open_workspace(expected, workspace_id, Port)
        opened.append(workspace)
        return workspace

    monkeypatch.setattr(RuntimeService, "open_docker_workspace", open_workspace)
    assert (
        main(
            [
                "run",
                "--config",
                str(config),
                "--runtime-config",
                str(path),
                "--max-runtime",
                "0.02",
            ]
        )
        == 0
    )
    result = json.loads(capsys.readouterr().out)
    assert {key: value for key, value in result["runtime"].items() if key != "channels"} == {
        "backend": "unavailable",
        "state": "closed",
        "workspace": {"status": "opened", "reason_code": None,
                      "observation_scope": "last-startup-attempt", "automatic_retry": False},
        "memory": {"status": "absent", "reason_code": "memory-absent"},
        "schedule": {"status": "not-configured", "observation_scope": "last-idle-step",
                     "execution_verified": False},
        "children": {"observation_scope": "composition-only",
            "subagent": {"status": "absent", "execution_verified": False},
            "minion": {"status": "absent", "reason_code": "native-minion-adapter-not-configured",
                       "execution_verified": False, "fallback": None}},
    }
    channels = result["runtime"]["channels"]
    assert channels["ingress"] == {"state": "not-configured"}
    assert channels["activity"] is None and channels["last_turn_timing"] == {}
    assert channels["admission"]["ready"] is False
    assert channels["admission"]["reason"] == "runtime-not-running"
    assert len(opened) == 1 and opened[0].closed
    assert inbox.query(request.session, "pending")["status"] == "pending"
    assert runtime.store.read_turn(request.session, "pending") is None


@pytest.mark.parametrize(
    "change",
    [
        {"backend": "codex"},
        {"runtime_root": "relative"},
        {"checkpoint_digest": "invalid"},
        {"extra": True},
    ],
)
def test_invalid_configuration_never_opens_workspace(tmp_path, monkeypatch, capsys, change):
    _, _, payload, path, config = configuration(tmp_path)
    payload.update(change)
    path.write_text(json.dumps(payload), encoding="utf-8")
    opened = []
    monkeypatch.setattr(RuntimeService, "open_docker_workspace", lambda *a, **k: opened.append(1))
    assert main(["run", "--config", str(config), "--runtime-config", str(path)]) == 2
    assert not opened and not (tmp_path / "state").exists()
    assert json.loads(capsys.readouterr().out)["ok"] is False


@pytest.mark.parametrize("enabled", [False, True])
def test_explicit_backend_switch_controls_inbox_admission(tmp_path, monkeypatch, enabled):
    from ahc.cli import wsl_codex_backend
    from ahc.cli.foreground import compose_foreground
    from ahc.domain.agent_backend import AgentTurnObservation

    original, request, payload, path, _ = configuration(tmp_path)
    payload["backend"] = wsl_backend(enabled=enabled)
    path.write_text(json.dumps(payload))
    inbox = RuntimeInbox(original.store)
    inbox.submit(request.session, "pending", request.goal_digest, "input")
    called = []

    class Factory:
        def recover(self, session, workspace_id):
            assert enabled and session == request.session and workspace_id == "work"
            return ()

        def __init__(self, store, host, distribution, **kwargs):
            assert kwargs["launch_spec"] == SPEC and kwargs["model"] == "fixture"

        def binding(self, tools):
            return "a" * 64, "b" * 64

        def __call__(self, bound, tools, **kwargs):
            called.append(bound)

            class Backend:
                def invoke(self, request):
                    return AgentTurnObservation(
                        request, "completed", "fixture", 1, 1, "t", "u", "done"
                    )

            return Backend()

    monkeypatch.setattr(wsl_codex_backend, "WslCodexBackendFactory", Factory)
    monkeypatch.setattr(
        RuntimeService,
        "open_docker_workspace",
        lambda service, expected, workspace_id, runner, **kw: service.open_workspace(
            expected, workspace_id, Port
        ),
    )
    runtime = compose_foreground(path)
    runtime.start()
    try:
        result = runtime.step()
        assert runtime.backend_name == ("codex-wsl-offline" if enabled else "unavailable")
        if enabled:
            assert result.result["accepted"] and len(called) == 1
            assert called[0].artifact_digest == "a" * 64
            assert called[0].configuration_digest == "b" * 64
            assert runtime.step() is None and len(called) == 1
            assert inbox.query(request.session, "pending")["status"] == "terminal"
        else:
            assert result is None and not called
            assert inbox.query(request.session, "pending")["status"] == "pending"
            assert original.store.read_turn(request.session, "pending") is None
    finally:
        runtime.close()


@pytest.mark.parametrize(
    "change",
    [
        {"enabled": 1},
        {"enabled": None},
        {"turn_timeout": True},
        {"turn_timeout": 301},
        {"lifetime_seconds": 0},
        {"kind": "codex"},
        {"model": None},
        {"extra": True},
        {"launch_spec": {**SPEC, "policy_sha256": "wrong"}},
        {"launch_spec": {**SPEC, "home_size_mib": True}},
    ],
)
def test_bad_backend_configuration_never_opens_workspace(tmp_path, monkeypatch, capsys, change):
    _, _, payload, path, config = configuration(tmp_path)
    payload["backend"] = {**wsl_backend(), **change}
    path.write_text(json.dumps(payload))
    monkeypatch.setattr(
        RuntimeService, "open_docker_workspace", lambda *a, **kw: pytest.fail("workspace opened")
    )
    assert main(["run", "--config", str(config), "--runtime-config", str(path)]) == 2
    assert json.loads(capsys.readouterr().out)["ok"] is False


def test_unavailable_workspace_preserves_input_before_backend_creation(tmp_path, monkeypatch):
    from ahc.adapters.docker_workspace import WorkspaceUnavailable
    from ahc.adapters.wsl_unit_runner import WslUnitRunner
    from ahc.cli.foreground import compose_foreground

    original, request, payload, path, _ = configuration(tmp_path)
    payload["backend"] = wsl_backend(enabled=True)
    path.write_text(json.dumps(payload))
    inbox = RuntimeInbox(original.store)
    inbox.submit(request.session, "pending", request.goal_digest, "retain this input")

    def unavailable(*args, **kwargs):
        raise WorkspaceUnavailable("engine unavailable")

    monkeypatch.setattr(RuntimeService, "open_docker_workspace", unavailable)
    monkeypatch.setattr(
        WslUnitRunner, "_open_transport", lambda *a: pytest.fail("backend launched")
    )
    runtime = compose_foreground(path)
    try:
        runtime.start()
        assert runtime.admission_status()["ready"] is False
        assert runtime.admission_status()["reason"] == "workspace-unavailable"
        assert inbox.query(request.session, "pending")["status"] == "pending"
        assert original.store.read_turn(request.session, "pending") is None
    finally:
        runtime.close()
