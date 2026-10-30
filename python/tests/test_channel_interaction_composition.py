"""Compose channel activity, approval callbacks, final delivery and held-history controls."""

import http.client
import threading
import time
from dataclasses import replace
from types import SimpleNamespace

import pytest
from test_channel_controls import make
from test_discord_controls_ui import fixture as discord_fixture
from test_discord_controls_ui import frame
from test_telegram_controls_ui import callback

from ahc.adapters.channel_activity import BotActivityTransport
from ahc.adapters.channel_activity_worker import ChannelActivityWorker
from ahc.adapters.channel_control_ui import BotControlTransport
from ahc.adapters.channel_text import BotTextTransport
from ahc.application.channel_delivery import ChannelDelivery
from ahc.application.channel_ingress import ChannelIngress
from ahc.application.runtime_service import RuntimeService
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.domain.channel_ingress import ChannelInboundEvent
from ahc.domain.runtime_approval import ApprovalRequest
from ahc.domain.sessions import ContractError
from ahc.infrastructure.channel_outbox import ChannelOutbox
from ahc.infrastructure.channel_progress_store import ChannelProgressStore
from ahc.infrastructure.runtime_approval_store import RuntimeApprovalStore
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.runtime_repository import RuntimeRepository
from ahc.infrastructure.telegram_inbox import TelegramInbox


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("offline interaction scenario attempted external HTTP")

    monkeypatch.setattr(http.client.HTTPSConnection, "request", forbidden)


def channel(tmp_path, platform):
    if platform == "discord":
        store, current, template, route, controls, receiver = discord_fixture(tmp_path)
    else:
        store, current, template, route, controls, _, _ = make(tmp_path)
        controls.options["native_ui"] = True
        receiver = TelegramInbox(store, [route])
    events = []

    def submit(text):
        event = ChannelInboundEvent(route, f"event-{len(events)}", text)
        events.append(event)
        ChannelIngress(RuntimeInbox(store), route).submit(event, current.goal.digest)
        return event

    return store, current, template, route, controls, receiver, submit


def receipt(route, message):
    if route.platform == "telegram":
        return {"ok": True, "result": {"message_id": int(message),
                "chat": {"id": int(route.channel_id)},
                "from": {"id": int(route.account_id)}}}
    return {"id": str(message), "channel_id": route.channel_id,
            "author": {"id": route.account_id}}


@pytest.mark.parametrize("platform", ["telegram", "discord"])
def test_working_approval_callback_to_rendered_final_once(tmp_path, platform):
    store, current, template, route, controls, receiver, submit = channel(tmp_path, platform)
    event = submit("Perform one approved operation")
    request = replace(template, operation_id=event.operation_id, input_text=event.input_text)
    sent, edited, typing = [], [], []
    created, approval_visible = threading.Event(), threading.Event()
    activity = []

    def post(_platform, _token, _channel, body):
        sent.append(body)
        if "Working" in str(body):
            created.set()
        return receipt(route, len(sent))

    def edit(_platform, _token, _channel, message, text):
        edited.append((message, text))
        if "等待核准" in text and "Checking <input>" in text:
            approval_visible.set()
        return receipt(route, message)

    transport = BotTextTransport(route, "fixture", post=post)
    approvals = RuntimeApprovalStore(store)

    class Backend:
        def invoke(self, bound):
            ledger = ChannelProgressStore(store)
            worker = ChannelActivityWorker(
                SimpleNamespace(start=lambda: typing.append("start"),
                                close=lambda: typing.append("close")),
                BotActivityTransport(route, "fixture", text_transport=transport, edit=edit),
                lambda: True,
                lambda text: ledger.reserve(bound, route, "activity-status", text,
                                            "activity-token", int(time.time() * 1000)),
                lambda value: ledger.finish(bound, route, "activity-status",
                                            "activity-token", value),
                bound.operation_id, interval=0.01,
            )
            activity.append(worker)
            worker.start()
            assert created.wait(2)
            worker.observe("等待核准", "Checking <input>")
            assert approval_visible.wait(2)
            approval = ApprovalRequest(
                bound.session, bound.operation_id, bound.effective_policy.digest,
                "thread", "turn", "wire", "b" * 64, time.time() + 5,
                "Execute the selected operation",
            )
            approvals.publish(bound, approval)
            controls.notify_approval(approval)
            controls.deliver_one({route: BotControlTransport(route, "fixture", post=post)})
            message = str(len(sent))
            if platform == "telegram":
                data = sent[-1]["reply_markup"]["inline_keyboard"][0][0]["callback_data"]

                def click(number, foreign=False):
                    update = callback(route, number, data, message_id=int(message),
                                      sender="999" if foreign else None)
                    return receiver.admit([update], expected_offset=None if number == 1 else number)
            else:
                data = sent[-1]["components"][0]["components"][0]["custom_id"]

                def click(number, foreign=False):
                    update = frame(route, number, {"component_type": 2, "custom_id": data}, 3)
                    update["d"]["message"] = {"id": message, "channel_id": route.channel_id,
                                                "author": {"id": route.account_id, "bot": True}}
                    if foreign:
                        update["d"]["user"]["id"] = "999"
                    return receiver.admit(update)

            click(1, foreign=True)
            assert approvals.query(bound.session, approval.digest)["state"] == "pending"
            click(2)
            answered = approvals.query(bound.session, approval.digest)
            assert answered["state"] == "answered" and answered["outcome"] == "approve-once"
            click(3)
            assert approvals.query(bound.session, approval.digest) == answered
            return AgentTurnObservation(bound, "completed", "fixture", 1, 1,
                                        "thread", "turn", "**Done**\n\n`a < b` & safe")

    try:
        result = RuntimeService(store, Backend()).execute(current, request)
        assert result.result["accepted"]
        activity[0].finish(True)
    finally:
        for worker in activity:
            worker.close()
    assert typing == ["start", "close"]
    assert len(sent) == 2  # Exactly one working card and one approval notice.
    assert edited and all(message == "1" for message, _ in edited)
    assert "回覆已產生" in edited[-1][1]
    outbox = ChannelOutbox(store)
    delivery = ChannelDelivery(outbox, route, transport)
    assert delivery.deliver(request.operation_id, "final-token")["state"] == "sent"
    if platform == "telegram":
        assert sent[-1]["parse_mode"] == "HTML"
        assert sent[-1]["text"] == "<b>Done</b>\n\n<code>a &lt; b</code> &amp; safe"
    else:
        assert sent[-1]["content"] == "**Done**\n\n`a < b` & safe"
        assert sent[-1]["allowed_mentions"] == {"parse": []}
    reopened = ChannelDelivery(ChannelOutbox(RuntimeRepository(store.root)), route, transport)
    with pytest.raises(ContractError, match="not pending"):
        reopened.deliver(request.operation_id, "reopened-token")
    assert len(sent) == 3
    with store._read() as c:
        assert c.execute("SELECT count(*) FROM runtime_submissions").fetchone()[0] == 1
        assert c.execute("SELECT state FROM runtime_channel_progress").fetchone()[0] == "sent"


@pytest.mark.parametrize("platform", ["telegram", "discord"])
def test_stop_then_new_preserves_session_and_holds_old_queue(tmp_path, platform):
    store, current, template, route, controls, _, submit = channel(tmp_path, platform)
    first = submit("active work")
    request = replace(template, operation_id=first.operation_id, input_text=first.input_text)
    old_queue, calls = [], []

    class Backend:
        def invoke(self, bound):
            calls.append(bound.operation_id)
            old_queue.append(submit("queued before stop"))
            submit("/stop")
            assert controls.cancelled(bound.operation_id)
            assert store.read_current(bound.session.ref.virtual_session_id).session == bound.session
            submit("/new")  # The active parent still owns the session; reset cannot proceed.
            assert controls.preferences_for(bound.operation_id)["history_after"] == 0
            return AgentTurnObservation(bound, "completed", "fixture", 1, 1,
                                        "thread", "turn", "Stopped at a safe point")

    result = RuntimeService(store, Backend()).execute(current, request)
    assert result.result["accepted"] and calls == [first.operation_id]
    inbox = RuntimeInbox(store)
    assert inbox.next_pending(route.session)["operation_id"] == old_queue[0].operation_id
    submit("/new")
    assert inbox.next_pending(route.session) is None
    assert store.read_current(route.session.ref.virtual_session_id).session == route.session
    fresh = submit("fresh after new")
    assert controls.preferences_for(fresh.operation_id)["history_after"] > 0
    reopened = RuntimeRepository(store.root)
    assert RuntimeInbox(reopened).next_pending(route.session)["operation_id"] == fresh.operation_id
    with reopened._read() as c:
        assert c.execute("SELECT 1 FROM channel_held WHERE operation_id=?",
                         (old_queue[0].operation_id,)).fetchone()
        assert c.execute("SELECT count(*) FROM runtime_turns").fetchone()[0] == 1
    assert reopened.read_turn(route.session, first.operation_id).result == result.result
