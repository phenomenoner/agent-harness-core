import pytest

from ahc.domain.sessions import ContractError
from ahc.domain.workspace_display import display_fields


@pytest.mark.parametrize("rows", [
    [{"text/html": "x" * 16385}], [{"text/plain": "\ud800"}],
    [{"text/plain": "ok"}] * 17, [{"unknown/type": "x"}],
    [{"text/plain": 1}], [{}],
])
def test_reject_invalid_display(rows):
    with pytest.raises(ContractError):
        display_fields({"displays": rows, "display_truncated": False})


def test_legacy_absent_and_explicit_display():
    assert display_fields({"output": "legacy"}) == {}
    value = {"displays": [{"text/html": "<b>ok</b>"}], "display_truncated": False}
    assert display_fields(value) == value
    with pytest.raises(ContractError):
        display_fields({"displays": []})


def test_malformed_adapter_display_closes_without_reexecution(monkeypatch):
    from test_wsl_python_workspace import open_workspace

    from ahc.adapters.docker_workspace import WorkspaceUnavailable

    workspace, runner, custody = open_workspace(monkeypatch)
    receive = runner.transport.receive
    calls = []

    def invalid(*, deadline):
        calls.append(1)
        reply = receive(deadline=deadline)
        reply.update(displays=[{"text/html": 42}], display_truncated=False)
        return reply

    monkeypatch.setattr(runner.transport, "receive", invalid)
    expected = workspace.identity
    with pytest.raises(WorkspaceUnavailable, match="display is invalid"):
        workspace.execute(expected, "x = 1")
    assert runner.transport.closed and custody.lease.closed
    assert runner.transport.values["x"] == 1
    with pytest.raises((ContractError, WorkspaceUnavailable)):
        workspace.execute(workspace.identity, "x = 2")
    assert calls == [1] and runner.transport.values["x"] == 1
