"""Real inbox transaction, exact-message callbacks, and scoped command discovery."""

import time
from dataclasses import replace

import pytest
from test_channel_controls import make

from ahc.adapters.channel_control_ui import BotControlTransport
from ahc.adapters.telegram_poll import TelegramPoll
from ahc.domain.runtime_approval import ApprovalRequest
from ahc.infrastructure.runtime_approval_store import RuntimeApprovalStore
from ahc.infrastructure.telegram_inbox import TelegramInbox


def callback(route, number, data, *, message_id=10, sender=None):
    return {
        "update_id": number,
        "callback_query": {
            "id": f"click-{number}",
            "data": data,
            "from": {"id": int(sender or route.user_id), "is_bot": False},
            "message": {
                "message_id": message_id,
                "chat": {"id": int(route.channel_id)},
                "from": {"id": int(route.account_id), "is_bot": True},
            },
        },
    }


def pending(tmp_path):
    store, current, template, route, controls, _, submit = make(tmp_path)
    controls.options["native_ui"] = True
    event = submit("Use the selected tool")
    request = replace(template, operation_id=event.operation_id, input_text=event.input_text)
    store.begin(current, request)
    approval = ApprovalRequest(
        request.session,
        request.operation_id,
        request.effective_policy.digest,
        "thread",
        "turn",
        "wire",
        "b" * 64,
        time.time() + 5,
        'workspace.execute: {"code":"print(42)","expected":{"revision":3}}',
    )
    approvals = RuntimeApprovalStore(store)
    approvals.publish(request, approval)
    controls.notify_approval(approval)
    sent = []

    def post(platform, token, channel, body):
        sent.append(body)
        return {
            "ok": True,
            "result": {
                "message_id": 10,
                "chat": {"id": int(channel)},
                "from": {"id": int(route.account_id)},
            },
        }

    controls.deliver_one({route: BotControlTransport(route, "fixture", post=post)})
    return store, route, controls, approval, sent, submit


def test_exact_button_admits_once_and_never_turns_callback_into_model_work(tmp_path):
    store, route, _, approval, sent, _ = pending(tmp_path)
    buttons = sent[0]["reply_markup"]["inline_keyboard"][0]
    assert len(buttons) == 2 and all(len(b["callback_data"].encode()) <= 64 for b in buttons)
    assert "expected" not in sent[0]["text"] and "print(42)" in sent[0]["text"]
    inbox = TelegramInbox(store, [route])
    update = callback(route, 1, buttons[0]["callback_data"])
    result = inbox.admit([update], expected_offset=None)
    assert "核准這一次" in result["callbacks"][0]["text"]
    # A different click on the same old button cannot approve again.
    stale = callback(route, 2, buttons[0]["callback_data"])
    result = inbox.admit([stale], expected_offset=2)
    assert "沒有唯一" in result["callbacks"][0]["text"]
    with store._read() as c:
        assert c.execute("SELECT count(*) FROM runtime_submissions").fetchone()[0] == 1
        assert (
            c.execute(
                "SELECT state FROM runtime_approvals WHERE request_digest=?", (approval.digest,)
            ).fetchone()[0]
            == "answered"
        )


@pytest.mark.parametrize("variation", ["sender", "message", "digest", "stopped", "expired"])
def test_wrong_or_stale_callback_never_approves(tmp_path, variation):
    store, route, _, approval, sent, submit = pending(tmp_path)
    data = sent[0]["reply_markup"]["inline_keyboard"][0][0]["callback_data"]
    update = callback(
        route,
        1,
        data,
        sender="999" if variation == "sender" else None,
        message_id=11 if variation == "message" else 10,
    )
    if variation == "digest":
        update["callback_query"]["data"] = "ahc:approve:" + "0" * 24
    if variation == "stopped":
        submit("/stop")
    if variation == "expired":
        # Let the real wall-clock guard reject; no native/provider execution.
        from unittest.mock import patch

        import ahc.infrastructure.channel_control_store as module

        with patch.object(module.time, "time", return_value=approval.expires_at + 1):
            TelegramInbox(store, [route]).admit([update], expected_offset=None)
    else:
        TelegramInbox(store, [route]).admit([update], expected_offset=None)
    with store._read() as c:
        assert (
            c.execute(
                "SELECT state FROM runtime_approvals WHERE request_digest=?", (approval.digest,)
            ).fetchone()[0]
            == "pending"
        )


def test_menu_is_scoped_and_callback_polling_is_explicit():
    from ahc.domain.channel_ingress import ChannelRoute
    from ahc.domain.sessions import VirtualSessionIdentity

    route = ChannelRoute("telegram", "1", "2", "3", VirtualSessionIdentity.initial("s", "a", "r"))
    calls = []

    def request(token, method, body):
        calls.append((method, body))
        if method == "getMe":
            return {"ok": True, "result": {"id": 1, "is_bot": True, "username": "OurBot"}}
        if method == "getUpdates":
            return {"ok": True, "result": [{"update_id": 1, "message": {"text": "/help@OurBot"}}]}
        if method == "getMyCommands":
            return {"ok": True, "result": []}
        return {"ok": True, "result": True}

    poll = TelegramPoll("1", "fixture", request=request, interactions=True)
    assert poll.poll()[0]["message"]["text"] == "/help"
    assert calls[-1][1]["allowed_updates"] == ["message", "callback_query"]
    assert poll.configure_commands([route])
    menu = next(body for method, body in calls if method == "setMyCommands")
    assert menu["scope"] == {"type": "chat", "chat_id": "2"}
    assert {"approve", "deny", "stop", "model"} <= {r["command"] for r in menu["commands"]}
    assert poll.answer_callback({"id": "c", "text": "done"})
