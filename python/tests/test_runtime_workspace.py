import json
from dataclasses import replace

import pytest
from test_runtime_core import setup

from ahc.application.runtime_service import RuntimeService
from ahc.application.working_continuity import WorkingContinuityService
from ahc.domain.workspace import WorkspaceCheckpoint
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.working_repository import WorkingSetConflict


class Port:
    """Inert fixture only: never evaluates supplied code."""

    def __init__(self, identity, checkpoint):
        self.identity = identity
        self.calls = 0
        self.closed = False
        self.value = 42 if checkpoint is None else json.loads(checkpoint.values_json)["x"]

    def execute(self, expected, code):
        assert expected == self.identity
        self.calls += 1
        self.identity = replace(expected, revision=expected.revision + 1)
        if code == "loss":
            raise OSError("fixture transport loss")
        return {"identity": self.identity.to_dict(), "ok": True, "output": "42"}

    def checkpoint(self, expected, names):
        assert expected == self.identity and names == ("x",)
        return WorkspaceCheckpoint(expected, json.dumps({"x": self.value}))

    def close(self):
        self.closed = True


def test_new_generation_fences_old_port_before_another_execution(tmp_path):
    repo, snapshot, _ = setup(tmp_path)
    service = RuntimeService(repo, None)
    with service.open_workspace(snapshot, "work", Port) as old:
        old.execute(old.identity, "fixture")
        checkpoint = old.checkpoint(old.identity, ("x",))
        assert service.checkpoint_workspace(snapshot, old, ("x",)) == checkpoint.digest
        with service.open_workspace(
            snapshot, "work", Port, checkpoint_digest=checkpoint.digest
        ) as new:
            assert new.identity.generation == old.identity.generation + 1
            assert new.identity.revision == 0
            with pytest.raises(WorkingSetConflict):
                old.execute(old.identity, "fixture")
            assert old.port.calls == 1
            assert new.execute(new.identity, "fixture")["ok"]


def test_failed_factory_consumes_generation(tmp_path):
    repo, snapshot, _ = setup(tmp_path)
    service = RuntimeService(repo, None)

    def fail(identity, checkpoint):
        assert identity.generation == 1
        raise OSError("unavailable")

    with pytest.raises(OSError):
        service.open_workspace(snapshot, "work", fail)
    with service.open_workspace(snapshot, "work", Port) as workspace:
        assert workspace.identity.generation == 2


def test_loss_remains_unreconciled_and_reopen_allocates_new_generation(tmp_path):
    repo, snapshot, _ = setup(tmp_path)
    service = RuntimeService(repo, None)
    workspace = service.open_workspace(snapshot, "work", Port)
    with pytest.raises(OSError):
        workspace.execute(workspace.identity, "loss")
    assert workspace.port.closed
    reopened = RuntimeRepository(repo.root)
    with pytest.raises(WorkingSetConflict):
        reopened.require_workspace(workspace.identity)
    with RuntimeService(reopened, None).open_workspace(snapshot, "work", Port) as new:
        assert new.identity.generation == 2 and new.identity.revision == 0
        assert workspace.port.calls == 1


def test_cancellation_during_execution_rejects_late_result(tmp_path):
    repo, snapshot, _ = setup(tmp_path)

    class CancellingPort(Port):
        def execute(self, expected, code):
            result = super().execute(expected, code)
            WorkingContinuityService(repo).cancel(snapshot)
            return result

    with RuntimeService(repo, None).open_workspace(snapshot, "work", CancellingPort) as workspace:
        with pytest.raises(WorkingSetConflict):
            workspace.execute(workspace.identity, "fixture")
        assert workspace.port.closed


def test_checkpoint_cannot_bypass_replacement_fence(tmp_path):
    repo, snapshot, _ = setup(tmp_path)
    service = RuntimeService(repo, None)
    with service.open_workspace(snapshot, "work", Port) as old:
        checkpoint = old.checkpoint(old.identity, ("x",))
        with service.open_workspace(snapshot, "work", Port):
            with pytest.raises(WorkingSetConflict):
                repo.save_workspace_checkpoint(snapshot, checkpoint)
