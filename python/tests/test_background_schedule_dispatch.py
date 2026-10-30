from test_channel_background_presentation import scheduled

from ahc.domain.channel_ingress import ChannelInboundEvent
from ahc.infrastructure.background_schedule_dispatch import BackgroundScheduleDispatch
from ahc.infrastructure.runtime_inbox import RuntimeInbox


def test_handoff_preserves_route_and_holds_original_against_replay(tmp_path):
    store, current, request, route = scheduled(tmp_path)
    dispatch = BackgroundScheduleDispatch(store)
    binding = dispatch.claim_one(current.session)
    assert binding["source_operation_id"] == request.operation_id
    assert binding["worker_operation_id"] != request.operation_id
    assert binding["worker_session"] != current.session
    assert binding["worker_session"].agent_id == current.session.agent_id
    assert store.read_current(current.session.ref.virtual_session_id).active_turn_id is None
    assert RuntimeInbox(store).next_pending(current.session) is None
    pending = RuntimeInbox(store).next_pending(binding["worker_session"])
    assert pending["input_text"] == request.input_text
    assert pending["operation_id"] == binding["worker_operation_id"]
    with store._read() as connection:
        import json

        stored = json.loads(connection.execute("SELECT route_json FROM runtime_channel_routes "
            "WHERE operation_id=?", (binding["worker_operation_id"],)).fetchone()[0])
    for key in ("platform", "account_id", "channel_id", "user_id"):
        assert stored[key] == route.to_dict()[key]
    assert dispatch.claim_one(current.session) is None
    assert len(BackgroundScheduleDispatch(store).records(current.session)) == 1


def test_already_started_scheduled_work_is_never_handed_off_or_replayed(tmp_path):
    store, current, request, _ = scheduled(tmp_path)
    with store.execution_lock():
        store.begin(current, request)
    dispatch = BackgroundScheduleDispatch(store)
    assert dispatch.claim_one(current.session) is None
    assert dispatch.records(current.session) == ()
    assert store.read_turn(current.session, request.operation_id).status == "submitted"


def test_foreground_selection_skips_scheduled_work_before_handoff(tmp_path):
    store, current, request, route = scheduled(tmp_path)
    event = ChannelInboundEvent(route, "interactive-next", "please answer now")
    inbox = RuntimeInbox(store)
    inbox.submit(current.session, event.operation_id, current.goal.digest,
                 event.input_text, channel_event=event)
    assert inbox.next_pending(current.session)["operation_id"] == request.operation_id
    assert inbox.next_pending(current.session, exclude_scheduled=True)["operation_id"] == event.operation_id


def test_corrupted_handoff_cannot_borrow_schedule_authority(tmp_path):
    import pytest
    from ahc.infrastructure.working_repository import WorkingSetCorrupt

    store, current, _, _ = scheduled(tmp_path)
    dispatch = BackgroundScheduleDispatch(store)
    binding = dispatch.claim_one(current.session)
    store._mutate(lambda c: c.execute("UPDATE runtime_background_dispatch SET due_ms=due_ms+1"))
    with store._read() as c, pytest.raises(WorkingSetCorrupt, match="custody"):
        dispatch.origin_route(c, binding["worker_session"], binding["worker_operation_id"])
