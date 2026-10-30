import json
from dataclasses import replace

import pytest
from test_backend_custody import Runner
from test_runtime_core import setup

from ahc.domain.backend_identity import LinuxUnitIdentity
from ahc.domain.child_work import ChildWorkEnvelope
from ahc.infrastructure.backend_custody import BackendCustody, BackendCustodyUnavailable
from ahc.infrastructure.child_backend_custody import ChildBackendCustody
from ahc.infrastructure.child_backend_recovery import recover_child_backends
from ahc.infrastructure.child_dispatch_repository import ChildDispatchRepository
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.working_repository import WorkingWriterBusy


@pytest.mark.parametrize("case", ["recover", "live-owner", "pending", "identity-change",
                                  "missing-child", "missing-ledger", "orphan-pending"])
def test_reopen_discovers_exact_child_owner_without_replay(tmp_path, case):
    repo, snapshot, original = setup(tmp_path)
    workspace = repo.reserve_workspace_generation(snapshot, original.workspace_scope)
    parent = replace(original, deadline=100)
    repo.begin(repo.read_current("session"), parent)
    children = ChildDispatchRepository(repo, parent, clock=lambda: 90000, monotonic_clock=lambda: 0)
    envelope = ChildWorkEnvelope("child", children.parent, "task", parent.workspace_scope, 100000)
    children.admit(envelope)
    children.claim(children.parent, "child", "token")
    custody = ChildBackendCustody(repo, envelope)
    lease = custody.acquire(parent.session, parent.workspace_scope)
    unit = LinuxUnitIdentity(
        "AHC-Codex-Ubuntu-24.04",
        "12345678-1234-1234-1234-123456789abc",
        "ahc-backend-" + "c" * 32 + ".service",
        "b" * 32,
    )
    custody.record_created(lease, workspace, unit)
    if case in ("pending", "orphan-pending"):
        repo._mutate(
            lambda c: c.execute(
                "INSERT INTO backend_creation_intents VALUES (?,?,'pending')",
                (lease.key, json.dumps(workspace.to_dict()))
            )
        )
        repo._mutate(
            lambda c: c.execute("DELETE FROM backend_custody WHERE owner_key=?", (lease.key,))
        )
    if case != "live-owner":
        lease.close()
    if case in ("missing-child", "orphan-pending"):
        repo._mutate(lambda c: c.execute("DELETE FROM runtime_children WHERE child_id='child'"))
    if case == "missing-ledger":
        repo._mutate(lambda c: c.execute("DROP TABLE runtime_children"))
    parent_custody = BackendCustody(repo)
    parent_lease = parent_custody.acquire(parent.session, parent.workspace_scope)
    parent_unit = replace(unit, unit="ahc-backend-" + "d" * 32 + ".service")
    parent_custody.record_created(parent_lease, workspace, parent_unit)
    runner = Runner(
        unit, changed={"invocation_id": "e" * 32} if case == "identity-change" else None
    )
    reopened = RuntimeRepository(repo.root)
    try:
        if case == "recover":
            assert recover_child_backends(
                reopened, parent.session, parent.workspace_scope, runner
            ) == (unit,)
            assert runner.stopped
            before = list(runner.calls)
            assert (
                recover_child_backends(reopened, parent.session, parent.workspace_scope, runner)
                == ()
            )
            assert runner.calls == before
        else:
            error = WorkingWriterBusy if case == "live-owner" else BackendCustodyUnavailable
            with pytest.raises(error):
                recover_child_backends(reopened, parent.session, parent.workspace_scope, runner)
            assert "stop" not in runner.calls
        assert [item[2] for item in parent_custody._held(parent_lease)] == [parent_unit]
        if case not in ("missing-child", "missing-ledger", "orphan-pending"):
            assert children.read(children.parent)[0]["state"] == "claimed"
        assert repo.read_turn(parent.session, "child") is None
    finally:
        lease.close()
        parent_lease.close()
