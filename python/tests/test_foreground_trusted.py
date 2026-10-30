import json

import pytest
from test_foreground_config_v2 import valid_config
from test_foreground_runtime import setup
from test_workspace_tools import Port

from ahc.cli import foreground_trusted as composition
from ahc.cli import main
from ahc.cli.foreground import compose_foreground
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.domain.sessions import ContractError
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository


def config_file(tmp_path, *, enabled=False):
    original, _, _, request = setup(tmp_path)
    value = valid_config()
    value["runtime_root"] = str(original.store.root)
    value["session"] = request.session.to_dict()
    value["backend"]["enabled"] = enabled
    path = tmp_path / "trusted.json"
    path.write_text(json.dumps(value), encoding="utf-8")
    return original, request, value, path


def test_disabled_v2_preserves_pending_without_constructing_any_backend(tmp_path, monkeypatch):
    original, request, _, path = config_file(tmp_path)
    inbox = RuntimeInbox(original.store)
    inbox.submit(request.session, "pending", request.goal_digest, "input")

    def forbidden(*args, **kwargs):
        pytest.fail("disabled composition attempted process construction")

    monkeypatch.setattr(composition, "TrustedCodexBackendFactory", forbidden)
    runtime = compose_foreground(path)
    runtime.start()
    try:
        assert runtime.workspace is None
        assert runtime.step() is None
        assert inbox.query(request.session, "pending")["status"] == "pending"
    finally:
        runtime.close()


def test_enabled_ungranted_rejects_before_backend_construction(tmp_path, monkeypatch):
    _, _, value, path = config_file(tmp_path, enabled=True)
    value["policy"]["workspace_granted"] = False
    path.write_text(json.dumps(value), encoding="utf-8")

    def forbidden(*args, **kwargs):
        pytest.fail("missing grant reached backend construction")

    monkeypatch.setattr(composition, "TrustedCodexBackendFactory", forbidden)
    with pytest.raises(ContractError):
        compose_foreground(path)


def test_enabled_local_composition_uses_threaded_store_without_channels(tmp_path, monkeypatch):
    from ahc.infrastructure.threaded_runtime_repository import ThreadedRuntimeRepository

    _, _, _, path = config_file(tmp_path, enabled=True)

    class StopProbe(Exception):
        pass

    def inspect_store(store, config):
        assert isinstance(store, ThreadedRuntimeRepository)
        raise StopProbe()

    monkeypatch.setattr(composition, "TrustedCodexBackendFactory", inspect_store)
    with pytest.raises(StopProbe):
        compose_foreground(path)


def test_preview_needs_no_runtime_and_omits_backend_private_paths(tmp_path, capsys):
    value = valid_config()
    value["runtime_root"] = str(tmp_path / "absent")
    path = tmp_path / "preview.json"
    path.write_text(json.dumps(value), encoding="utf-8")
    assert main(["runtime-config-preview", "--runtime-config", str(path)]) == 0
    output = capsys.readouterr().out
    assert "workspace-write" in output
    assert value["backend"]["codex_home"] not in output
    assert value["backend"]["bundle"] not in output
    assert not (tmp_path / "absent").exists()


@pytest.mark.parametrize("canvas_enabled", [False, True])
def test_v2_composes_two_turns_policy_and_reopened_terminal_without_replay(tmp_path, monkeypatch, canvas_enabled):
    original, request, value, path = config_file(tmp_path, enabled=True)
    if canvas_enabled:
        value["symbolic_canvas"] = {"enabled": True, "min_turns": 2, "trigger_mode": "all"}
        path.write_text(json.dumps(value), encoding="utf-8")
    from ahc.adapters import wsl_python_workspace, wsl_stream_runner

    constructed, invoked, recovered, runners = [], [], [], []

    class Runner:
        def __init__(self, executable, distribution, *, persistent):
            runners.append((executable, distribution, persistent))

    def workspace(store, runner, identity, **options):
        assert options["effective_policy"].workspace_granted
        assert options["checkpoint"] is None
        port = Port(identity, None)
        constructed.append(port)
        return port

    class Factory:
        def __init__(self, store, config):
            self.policy = config.effective_policy

        def recover(self, session, workspace_id):
            recovered.append((session, workspace_id))

        def binding(self, tools):
            return "a" * 64, "b" * 64

        def __call__(self, bound, tools, *, cancelled):
            assert bound.effective_policy == self.policy
            invoked.append(bound.operation_id)

            class Backend:
                def invoke(self, admitted):
                    result = tools.invoke(
                        "workspace.execute",
                        {
                            "expected": tools.workspace.identity.to_dict(),
                            "code": "x=42",
                        },
                    )
                    assert result["disposition"] == "observed"
                    return AgentTurnObservation(
                        admitted, "completed", "fixture", 1, 1, "thread", "turn", "done"
                    )

                def close(self):
                    pass

            return Backend()

    monkeypatch.setattr(composition, "TrustedCodexBackendFactory", Factory)
    monkeypatch.setattr(wsl_stream_runner, "WslStreamRunner", Runner)
    monkeypatch.setattr(wsl_python_workspace, "WslPythonWorkspace", workspace)
    runtime = compose_foreground(path)
    inbox = RuntimeInbox(original.store)
    runtime.start()
    try:
        for operation in ("first", "second"):
            inbox.submit(request.session, operation, request.goal_digest, "input")
            assert runtime.step().result["accepted"]
            assert runtime.step() is None
        assert runtime.workspace.identity.revision == 2
        assert len(constructed) == len(recovered) == len(runners) == 1
        assert runners[0][2] is True
        assert invoked == ["first", "second"]
        if canvas_enabled:
            assert runtime.channel_status()["symbolic_canvas"]["status"] == "written"
            assert runtime.canvas.session == request.session
        else:
            assert runtime.canvas is None
    finally:
        runtime.close()
    reopened = RuntimeInbox(RuntimeRepository(original.store.root))
    assert reopened.next_pending(request.session) is None
    assert reopened.query(request.session, "first")["status"] == "terminal"
