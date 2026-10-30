from dataclasses import replace

import pytest
from test_backend_custody import Runner
from test_runtime_core import setup

from ahc.domain.backend_identity import LinuxUnitIdentity
from ahc.domain.child_work import ChildWorkEnvelope
from ahc.infrastructure.child_backend_custody import ChildBackendCustody
from ahc.infrastructure.child_backend_recovery import recover_child_backends
from ahc.infrastructure.child_dispatch_repository import ChildDispatchRepository
from ahc.infrastructure.runtime_backup import _digest, backup_runtime_database
from ahc.infrastructure.runtime_restore import prepare_runtime_restore
from ahc.infrastructure.runtime_restore_activation import activate_runtime_restore
from ahc.infrastructure.runtime_restore_transform import transform_runtime_restore
from ahc.infrastructure.working_repository import WorkingSetCorrupt


@pytest.mark.parametrize("corrupt", [False, True])
def test_restored_child_custody_uses_verified_ancestor_and_never_replays(tmp_path, corrupt):
    repo, saved, original = setup(tmp_path)
    workspace = repo.reserve_workspace_generation(saved, original.workspace_scope)
    parent = replace(original, deadline=100)
    repo.begin(repo.read_current("session"), parent)
    children = ChildDispatchRepository(repo, parent, clock=lambda: 90000, monotonic_clock=lambda: 0)
    envelope = ChildWorkEnvelope("child", children.parent, "task", parent.workspace_scope, 100000)
    children.admit(envelope)
    children.claim(children.parent, "child", "token")
    custody = ChildBackendCustody(repo, envelope)
    lease = custody.acquire(parent.session, parent.workspace_scope)
    unit = LinuxUnitIdentity("AHC-Codex-Ubuntu-24.04",
        "12345678-1234-1234-1234-123456789abc", "ahc-backend-" + "c" * 32 + ".service", "b" * 32)
    custody.record_created(lease, workspace, unit)
    lease.close()
    manifest = backup_runtime_database(repo, tmp_path / "backup")
    runner = Runner(unit)
    assert recover_child_backends(repo, parent.session, parent.workspace_scope, runner) == (unit,)
    source = repo.read_current("session")
    digest = _digest(repo.db_path)
    prepare_runtime_restore(tmp_path / "backup", tmp_path / "prep")
    receipt = transform_runtime_restore(tmp_path / "prep", manifest["sha256"], (source,),
                                        digest, tmp_path / "new")
    activate_runtime_restore(repo, tmp_path / "supervisor.lock", tmp_path / "new",
                             receipt["sha256"], manifest["sha256"], digest)
    successor = repo.read_current("session").session
    assert successor.ref.generation > parent.session.ref.generation
    runner.calls.clear()
    if corrupt:
        repo._mutate(lambda c: c.execute("UPDATE runtime_recovery_transitions "
                                        "SET transition_digest=?", ("f" * 64,)))
        with pytest.raises(WorkingSetCorrupt):
            recover_child_backends(repo, successor, parent.workspace_scope, runner)
        assert runner.calls == []
    else:
        assert recover_child_backends(repo, successor, parent.workspace_scope, runner) == (unit,)
        assert runner.calls == ["observe", "observe"]  # already stopped before restore
        assert children.read(children.parent)[0]["state"] == "claimed"
        assert repo.read_turn(successor, "child") is None
