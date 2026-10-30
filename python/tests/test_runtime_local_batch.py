import json
from dataclasses import replace

import pytest
from test_runtime_core import setup
from test_schedules import _schedule

from ahc.application.capability_view import _issue_agent_capability_view
from ahc.application.tool_batch import BatchCall
from ahc.domain.capability_values import ApprovalMode, CapabilityDefinition, EffectClass
from ahc.domain.profile_values import ProfileResourceKind
from ahc.domain.sessions import ContractError
from ahc.infrastructure.runtime_local_batch import PAUSE, RuntimeLocalBatch
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.schedule_repository import ScheduleRepository


@pytest.mark.parametrize("granted", [False, True])
def test_schedule_pause_requires_effect_grant_and_retains_individual_receipts(tmp_path, granted):
    store, snapshot, request = setup(tmp_path)
    store.begin(snapshot, request)
    schedule = _schedule(session=snapshot.session, goal_digest=snapshot.goal.digest)
    schedules = ScheduleRepository(store)
    schedules.create(schedule)
    view = _issue_agent_capability_view((CapabilityDefinition(PAUSE, ProfileResourceKind.WORKSPACE,
        EffectClass.REVERSIBLE_LOCAL if granted else EffectClass.READ_ONLY, ApprovalMode.NONE),),
        ((PAUSE, "runtime"),))
    batch = RuntimeLocalBatch(store, snapshot.session, request.workspace_scope, view)
    batch.clock = lambda: 0
    calls = tuple(BatchCall(name, PAUSE, "runtime", json.dumps({
        "schedule_id": schedule.schedule_id, "expected_revision": revision}))
        for name, revision in (("valid", 1), ("stale", 1)))
    with store.execution_lock(), store.workspace_turn_scope(request):
        with pytest.raises(ContractError, match="sequential"):
            batch.run(snapshot.session, calls, deadline=100, concurrency=2)
        result = batch.run(snapshot.session, calls, deadline=100)
        assert [row["state"] for row in result.receipts] == (
            ["success", "rejected"] if granted else ["rejected", "rejected"])
        with pytest.raises(ContractError):
            batch.run(snapshot.session, calls, deadline=100)
    saved = schedules.read(snapshot.session, schedule.schedule_id)
    assert saved.state == ("paused" if granted else "active")
    assert saved.revision == (2 if granted else 1)


@pytest.mark.parametrize("receipt_committed", [False, True])
def test_effect_survives_lost_receipt_and_fresh_batch_cannot_replay(tmp_path, receipt_committed):
    store, snapshot, request = setup(tmp_path)
    store.begin(snapshot, request)
    schedule = _schedule(session=snapshot.session, goal_digest=snapshot.goal.digest)
    schedules = ScheduleRepository(store)
    schedules.create(schedule)
    schedules.create(replace(schedule, schedule_id="sibling"))
    view = _issue_agent_capability_view((CapabilityDefinition(PAUSE, ProfileResourceKind.WORKSPACE,
        EffectClass.REVERSIBLE_LOCAL, ApprovalMode.NONE),), ((PAUSE, "runtime"),))
    batch = RuntimeLocalBatch(store, snapshot.session, request.workspace_scope, view)
    batch.clock = lambda: 0
    finish = batch.receipts.finish

    def lose_receipt(parent, call_id, state, reason):
        if call_id != "lost" or receipt_committed:
            finish(parent, call_id, state, reason)
        if call_id == "lost":
            raise OSError("private receipt error")

    batch.receipts.finish = lose_receipt
    calls = tuple(BatchCall(call_id, PAUSE, "runtime", json.dumps({
        "schedule_id": schedule_id, "expected_revision": 1}))
        for call_id, schedule_id in (("lost", schedule.schedule_id), ("ok", "sibling")))
    with store.execution_lock(), store.workspace_turn_scope(request):
        result = batch.run(snapshot.session, calls, deadline=100)
    assert [row["state"] for row in result.receipts] == ["unknown", "success"]
    assert result.values[0] is None
    assert "private" not in str(result)
    fresh = RuntimeRepository(store.root)
    reopened = RuntimeLocalBatch(fresh, snapshot.session, request.workspace_scope, view)
    reopened.clock = lambda: 0
    receipts = reopened.receipts.read(snapshot.session, request.operation_id)
    assert [row["state"] for row in receipts] == [
        "success" if receipt_committed else "unknown", "success"]
    with fresh.execution_lock(), fresh.workspace_turn_scope(request), pytest.raises(ContractError):
        reopened.run(snapshot.session, calls, deadline=100)
    for schedule_id in (schedule.schedule_id, "sibling"):
        saved = ScheduleRepository(fresh).read(snapshot.session, schedule_id)
        assert saved.state == "paused" and saved.revision == 2
