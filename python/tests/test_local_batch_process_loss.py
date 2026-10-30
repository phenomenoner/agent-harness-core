"""Actual process exit at admitted-call, committed-effect and receipt boundaries."""

import json
import subprocess
import sys
from pathlib import Path

import pytest

import ahc
from ahc.application.capability_view import _issue_agent_capability_view
from ahc.application.tool_batch import BatchCall
from ahc.domain.agent_backend import AgentTurnRequest
from ahc.domain.capability_values import ApprovalMode, CapabilityDefinition, EffectClass
from ahc.domain.profile_values import ProfileResourceKind
from ahc.domain.sessions import ContractError
from ahc.infrastructure.runtime_local_batch import PAUSE, RuntimeLocalBatch
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.schedule_repository import ScheduleRepository

CHILD = '''
import os, sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
sys.path.insert(0, sys.argv[2])
from test_runtime_core import setup
from test_schedules import _schedule
from test_local_batch_process_loss import make_batch, call
from ahc.infrastructure.schedule_repository import ScheduleRepository
store, snapshot, request = setup(Path(sys.argv[3]))
store.begin(snapshot, request)
ScheduleRepository(store).create(_schedule(session=snapshot.session,goal_digest=snapshot.goal.digest))
batch = make_batch(store, snapshot.session)
phase = sys.argv[4]
if phase == 'admitted':
    begin = batch.receipts.begin
    def stop(*args):
        begin(*args)
        os._exit(23)
    batch.receipts.begin = stop
elif phase == 'effect':
    original = ScheduleRepository.set_state
    def stop(self, *args):
        original(self, *args)
        os._exit(23)
    ScheduleRepository.set_state = stop
else:
    finish = batch.receipts.finish
    def stop(*args):
        finish(*args)
        os._exit(23)
    batch.receipts.finish = stop
with store.execution_lock(), store.workspace_turn_scope(request):
    batch.run(snapshot.session, (call(),), deadline=100)
raise AssertionError('crash seam not reached')
'''


def call():
    return BatchCall("pause", PAUSE, "runtime", json.dumps({
        "schedule_id": "schedule-main", "expected_revision": 1}))


def make_batch(store, session):
    view = _issue_agent_capability_view((CapabilityDefinition(PAUSE, ProfileResourceKind.WORKSPACE,
        EffectClass.REVERSIBLE_LOCAL, ApprovalMode.NONE),), ((PAUSE, "runtime"),))
    batch = RuntimeLocalBatch(store, session, "workspace", view)
    batch.clock = lambda: 0
    return batch


@pytest.mark.parametrize("phase", ["admitted", "effect", "receipt"])
def test_process_loss_retains_truth_without_reexecuting(tmp_path, phase):
    child = subprocess.run([sys.executable, "-I", "-c", CHILD,
        str(Path(ahc.__file__).parent.parent), str(Path(__file__).parent), str(tmp_path), phase],
        capture_output=True, timeout=10)
    assert child.returncode == 23, child.stderr.decode(errors="replace")
    fresh = RuntimeRepository(tmp_path / "runtime")
    snapshot = fresh.read_current("session")
    request = AgentTurnRequest(snapshot.session, "operation", snapshot.goal.digest, "question",
        (), "a" * 64, "b" * 64, "workspace", 100)
    batch = make_batch(fresh, snapshot.session)
    rows = batch.receipts.read(snapshot.session, "operation")
    assert len(rows) == 1 and rows[0]["state"] == (
        "success" if phase == "receipt" else "unknown")
    with fresh.execution_lock(), fresh.workspace_turn_scope(request), pytest.raises(ContractError):
        batch.run(snapshot.session, (call(),), deadline=100)
    saved = ScheduleRepository(fresh).read(snapshot.session, "schedule-main")
    assert saved.state == ("active" if phase == "admitted" else "paused")
    assert saved.revision == (1 if phase == "admitted" else 2)
    with fresh._read() as connection:
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
