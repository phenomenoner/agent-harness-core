"""Real owner process loss; native unit effects remain controlled."""

import json
import subprocess
import sys
from dataclasses import replace

import pytest
from test_backend_custody import Runner
from test_runtime_core import setup

from ahc.domain.backend_identity import LinuxUnitIdentity
from ahc.domain.child_work import ChildWorkEnvelope
from ahc.infrastructure.backend_custody import BackendCustodyUnavailable
from ahc.infrastructure.child_backend_recovery import recover_child_backends
from ahc.infrastructure.child_dispatch_repository import ChildDispatchRepository
from ahc.infrastructure.runtime_repository import RuntimeRepository

CHILD = """
import json, os, sys
from ahc.domain.backend_identity import LinuxUnitIdentity
from ahc.domain.child_work import ChildWorkEnvelope
from ahc.domain.workspace import WorkspaceIdentity
from ahc.infrastructure.child_backend_custody import ChildBackendCustody
from ahc.infrastructure.runtime_repository import RuntimeRepository
root, envelope_json, workspace_json, unit_json, phase = sys.argv[1:]
repo = RuntimeRepository(root)
envelope = ChildWorkEnvelope.from_dict(json.loads(envelope_json))
workspace = WorkspaceIdentity.from_dict(json.loads(workspace_json))
unit = LinuxUnitIdentity(**json.loads(unit_json))
foreground = repo.foreground_lease(workspace.session)
custody = ChildBackendCustody(repo, envelope)
lease = custody.acquire(workspace.session, workspace.workspace_id)
class Runner:
    def create(self, *args, **kwargs):
        if phase == 'pending':
            os._exit(23)
        return unit
custody.create(lease, workspace, Runner(), ('fixture',), lifetime_seconds=10)
os._exit(23)
"""


@pytest.mark.parametrize("phase", ["recorded", "pending"])
def test_process_exit_releases_owner_and_preserves_exact_recovery_boundary(tmp_path, phase):
    from dataclasses import asdict

    repo, snapshot, original = setup(tmp_path)
    workspace = repo.reserve_workspace_generation(snapshot, original.workspace_scope)
    parent = replace(original, deadline=100)
    repo.begin(repo.read_current("session"), parent)
    children = ChildDispatchRepository(repo, parent, clock=lambda: 90000, monotonic_clock=lambda: 0)
    envelope = ChildWorkEnvelope("child", children.parent, "task", parent.workspace_scope, 100000)
    children.admit(envelope)
    children.claim(children.parent, "child", "token")
    unit = LinuxUnitIdentity("AHC-Codex-Ubuntu-24.04",
        "12345678-1234-1234-1234-123456789abc", "ahc-backend-" + "c" * 32 + ".service", "b" * 32)
    process = subprocess.run([sys.executable, "-c", CHILD, str(repo.root),
        json.dumps(envelope.to_dict()), json.dumps(workspace.to_dict()), json.dumps(asdict(unit)),
        phase], capture_output=True, text=True, timeout=15)
    assert process.returncode == 23, process.stderr
    reopened = RuntimeRepository(repo.root)
    runner = Runner(unit)
    with reopened.foreground_lease(parent.session):
        if phase == "recorded":
            assert recover_child_backends(reopened, parent.session, parent.workspace_scope,
                                          runner) == (unit,)
            assert runner.stopped
        else:
            with pytest.raises(BackendCustodyUnavailable, match="indeterminate"):
                recover_child_backends(reopened, parent.session, parent.workspace_scope, runner)
            assert runner.calls == []
    assert children.read(children.parent)[0]["state"] == "claimed"
    with reopened._read() as connection:
        intent = connection.execute("SELECT state FROM backend_creation_intents").fetchone()[0]
        assert intent == phase
        custody = connection.execute("SELECT state FROM backend_custody").fetchall()
        assert [row[0] for row in custody] == (["cleaned"] if phase == "recorded" else [])
