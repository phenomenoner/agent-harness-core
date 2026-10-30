import pytest
from test_wsl_python_workspace import open_workspace

from ahc.adapters import wsl_python_workspace as module


def test_failed_recovery_remains_visible_on_close_and_never_opens_a_successor(monkeypatch):
    workspace, runner, custody = open_workspace(monkeypatch)
    workspace.execute(workspace.identity, "x=42")
    checkpoint = workspace.checkpoint(workspace.identity, ("x",))

    def fail(*args):
        raise RuntimeError("PRIVATE cleanup diagnostic")

    custody.recover = fail
    with pytest.raises(module.WorkspaceUnavailable, match="cleanup.*unresolved"):
        workspace.restore(workspace.identity, checkpoint)
    with pytest.raises(module.WorkspaceUnavailable, match="cleanup.*unresolved"):
        workspace.close()
    assert len([call for call in custody.calls if call[0] == "create"]) == 1
    assert custody.lease.closed and runner.transport.closed


def test_transport_loss_does_not_hide_failed_cleanup(monkeypatch):
    workspace, runner, custody = open_workspace(monkeypatch)
    runner.transport.receive = lambda **kwargs: {"id": True, "ok": True}

    def fail(*args):
        raise RuntimeError("PRIVATE cleanup diagnostic")

    custody.recover = fail
    with pytest.raises(module.WorkspaceUnavailable, match="cleanup.*unresolved") as error:
        workspace.execute(workspace.identity, "x=42")
    assert "PRIVATE" not in str(error.value)
    with pytest.raises(module.WorkspaceUnavailable, match="cleanup.*unresolved"):
        workspace.close()
