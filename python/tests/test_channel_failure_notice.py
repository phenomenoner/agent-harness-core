"""Failed work has a durable status delivery without becoming a successful answer."""

from dataclasses import replace

import pytest
from test_channel_outbox import state

from ahc.application.channel_delivery import ChannelDelivery
from ahc.application.runtime_service import RuntimeService
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.domain.sessions import ContractError
from ahc.infrastructure.channel_outbox import ChannelOutbox
from ahc.infrastructure.runtime_conversation_context import conversation_context
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.working_repository import WorkingSetCorrupt


def fail(repo, snapshot, request, disposition="unknown"):
    class Backend:
        def invoke(self, value):
            return AgentTurnObservation(value, disposition, "PRIVATE-DIAGNOSTIC", 1, 1,
                                        unresolved_effects=disposition == "unknown")

    return RuntimeService(repo, Backend()).execute(snapshot, request)


@pytest.mark.parametrize("disposition", ["unknown", "not-submitted", "unsupported"])
def test_failed_turn_notifies_once_and_stays_incomplete_after_reopen(tmp_path, disposition):
    repo, snapshot, request, route = state(tmp_path)
    turn = fail(repo, snapshot, request, disposition)
    calls = []

    class Transport:
        def send(self, actual, payload, operation):
            calls.append((actual, payload, operation))
            return "notice-receipt"

    reopened = RuntimeRepository(repo.root)
    outbox = ChannelOutbox(reopened)
    record = ChannelDelivery(outbox, route, Transport()).deliver(request.operation_id, "claim")
    assert record["state"] == "sent"
    assert "任務未完成" in calls[0][1]["output_text"]
    assert "PRIVATE-DIAGNOSTIC" not in calls[0][1]["output_text"]
    assert calls[0][0] == route and calls[0][2] == request.operation_id
    assert reopened.read_turn(route.session, request.operation_id) == turn
    assert not turn.result["accepted"] and turn.result["output_text"] == ""
    assert reopened.intents(route.session)[0]["kind"] == "parked"
    assert reopened.read_answer_seal(route.session, request.operation_id) is None
    with pytest.raises(ContractError):
        ChannelDelivery(outbox, route, Transport()).deliver(request.operation_id, "again")
    assert len(calls) == 1


def test_notice_delivery_is_not_confirmed_assistant_history(tmp_path):
    from test_runtime_conversation_context import _deliver, _submit_channel, _submit_current

    repo, snapshot, template, route = state(tmp_path)
    request = _submit_channel(repo, template, route, "failed", "continue original task", "",
                              disposition="unknown")
    _deliver(repo, route, request.operation_id)
    current = _submit_current(repo, route)
    assert conversation_context(repo, route.session, current) == ()
    history = conversation_context(repo, route.session, current, include_incomplete=True)
    assert history[0]["user"] == "continue original task"
    assert history[0]["status"] == "unknown" and "assistant" not in history[0]
    assert history[0]["unresolved_effects"]


def test_failure_notice_keeps_unknown_delivery_and_never_retries(tmp_path):
    repo, snapshot, request, route = state(tmp_path)
    fail(repo, snapshot, request)
    calls = []

    class Transport:
        def send(self, *_):
            calls.append(True)
            raise OSError("unknown network result")

    outbox = ChannelOutbox(repo)
    assert ChannelDelivery(outbox, route, Transport()).deliver(
        request.operation_id, "claim")["state"] == "unknown"
    with pytest.raises(ContractError):
        ChannelDelivery(outbox, route, Transport()).deliver(request.operation_id, "again")
    assert calls == [True]


def test_failure_notice_revalidates_route_parent_and_payload(tmp_path):
    repo, snapshot, request, route = state(tmp_path)
    fail(repo, snapshot, request)
    outbox = ChannelOutbox(repo)
    with pytest.raises(ContractError):
        outbox.claim(replace(route, channel_id="other"), request.operation_id, "claim")
    repo._mutate(lambda c: c.execute(
        "UPDATE runtime_channel_outbox SET payload_json=? WHERE operation_id=?",
        ('{"output_text":"forged success"}', request.operation_id)))
    with pytest.raises(WorkingSetCorrupt):
        outbox.claim(route, request.operation_id, "claim")


def test_existing_unknown_is_not_backfilled_on_reopen(tmp_path):
    repo, snapshot, request, route = state(tmp_path)
    fail(repo, snapshot, request)
    repo._mutate(lambda c: c.execute("DELETE FROM runtime_channel_outbox"))
    reopened = RuntimeRepository(repo.root)
    assert reopened.reconcile(route.session) == ()
    assert ChannelOutbox(reopened).reconcile(route) == ()
    turn = reopened.read_turn(route.session, request.operation_id)
    assert turn.result["disposition"] == "unknown"


def test_failure_notice_rolls_back_with_terminal_transaction(tmp_path):
    from ahc.infrastructure.working_repository import WorkingRepositoryError

    repo, snapshot, request, route = state(tmp_path)
    repo.begin(snapshot, request)
    result = {"accepted": False, "disposition": "unknown", "output_text": "",
              "unresolved_effects": True}
    with pytest.raises(WorkingRepositoryError, match="injected"):
        repo.finish(request, result, fail_before_commit=True)
    assert repo.read_turn(route.session, request.operation_id).status == "submitted"
    assert ChannelOutbox(repo).read(route) == ()


def test_failure_notice_rejects_replaced_generation_before_transport(tmp_path):
    from ahc.application.working_continuity import WorkingContinuityService
    from ahc.infrastructure.working_repository import WorkingSetConflict

    repo, snapshot, request, route = state(tmp_path)
    fail(repo, snapshot, request)
    WorkingContinuityService(repo).continue_generation(
        repo.read_current("session"), route.session.successor(), "compaction")
    with pytest.raises(WorkingSetConflict):
        ChannelOutbox(repo).claim(route, request.operation_id, "claim")


def test_activity_commentary_cannot_be_presented_as_completion():
    from ahc.adapters.channel_activity_worker import ChannelActivityWorker

    worker = ChannelActivityWorker(None, None, lambda: True, lambda _: True,
                                   lambda _: None, "operation", clock=lambda: 53)
    worker._started = 0
    worker.observe("工具執行中", "I will inspect the repository and report results.")
    worker.finish(False)
    text = worker._text(final=True)
    assert "⚠ 任務未完成" in text and "00:53" in text
    assert "回覆已產生" not in text and "本次處理已結束" not in text
    assert "I will inspect" in text


@pytest.mark.parametrize("platform", ["telegram", "discord"])
@pytest.mark.parametrize("wrong_receipt", [False, True])
def test_failed_turn_notice_reaches_platform_adapter_with_bound_receipt(
        tmp_path, platform, wrong_receipt):
    from test_runtime_core import setup

    from ahc.adapters.channel_text import BotTextTransport
    from ahc.application.channel_ingress import ChannelIngress
    from ahc.domain.channel_ingress import ChannelInboundEvent, ChannelRoute
    from ahc.infrastructure.runtime_inbox import RuntimeInbox

    repo, snapshot, template = setup(tmp_path)
    route = ChannelRoute(platform, "123", "456", "789", snapshot.session)
    event = ChannelInboundEvent(route, "failure", "continue my task")
    ChannelIngress(RuntimeInbox(repo), route).submit(event, snapshot.goal.digest)
    request = replace(template, operation_id=event.operation_id,
                      goal_digest=snapshot.goal.digest, input_text=event.input_text)
    fail(repo, snapshot, request)
    calls = []

    def post(actual_platform, token, channel, body):
        assert (actual_platform, token, channel) == (platform, "fixture", "456")
        calls.append(body)
        received = "999" if wrong_receipt else "456"
        if platform == "telegram":
            assert "任務未完成" in body["text"] and body["parse_mode"] == "HTML"
            return {"ok": True, "result": {"message_id": 1, "chat": {"id": int(received)},
                                            "from": {"id": 123}}}
        assert "任務未完成" in body["content"] and body["allowed_mentions"] == {"parse": []}
        return {"id": "1", "channel_id": received, "author": {"id": "123"}}

    delivery = ChannelDelivery(ChannelOutbox(repo), route,
                               BotTextTransport(route, "fixture", post=post))
    result = delivery.deliver(request.operation_id, "claim")
    assert result["state"] == ("unknown" if wrong_receipt else "sent")
    with pytest.raises(ContractError):
        delivery.deliver(request.operation_id, "again")
    assert len(calls) == 1
