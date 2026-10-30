import json
from dataclasses import replace
from types import SimpleNamespace

from test_channel_controls import make

from ahc.adapters.codex_client_wire import _CONTINUE, _receive_one
from ahc.adapters.codex_steering import (
    accepted_turn_steer_response,
    rejected_turn_steer_response,
    turn_steer_message,
)
from ahc.domain.skill_values import digest_json
from ahc.infrastructure.runtime_inbox import RuntimeInbox


def test_turn_steer_wire_is_exact_and_correlated():
    message = turn_steer_message("steer-1", "thread-1", "turn-1", "keep going")
    assert message == {
        "id": "steer-1",
        "method": "turn/steer",
        "params": {
            "threadId": "thread-1",
            "expectedTurnId": "turn-1",
            "input": [{"type": "text", "text": "keep going"}],
        },
    }
    assert accepted_turn_steer_response(
        {"id": "steer-1", "result": {"turnId": "turn-1"}}, "steer-1", "turn-1"
    )
    assert not accepted_turn_steer_response(
        {"id": "steer-1", "result": {"turnId": "other"}}, "steer-1", "turn-1"
    )
    assert rejected_turn_steer_response(
        {"id": "steer-1", "error": {"code": -32000, "message": "stale"}}, "steer-1"
    )


def test_decoder_idle_callback_is_the_only_steer_response_consumer():
    calls = []

    class Port:
        def poll(self, client):
            calls.append("poll")

        def consume_response(self, message):
            calls.append(("consume", message["id"]))
            return True

    class Transport:
        def receive(self, *, deadline, on_idle):
            on_idle()
            return {"id": "steer-1", "result": {"turnId": "turn-1"}}

    client = SimpleNamespace(
        frames=0,
        transport=Transport(),
        request=SimpleNamespace(deadline=1),
        io_deadline=1,
        steering_port=Port(),
        submitted=False,
        thread=None,
        turn=None,
    )
    from ahc.adapters.codex_native_collaboration import NativeCollaboration
    client._native_collaboration = NativeCollaboration(client)
    assert _receive_one(client) is _CONTINUE
    assert calls == ["poll", ("consume", "steer-1")]


def test_default_queue_and_explicit_idle_steer_preserve_full_text(tmp_path):
    store, current, _, _, controls, _, submit = make(tmp_path)
    ordinary = submit("queued")
    assert controls.preferences_for(ordinary.operation_id)["messages"] == "queue"
    queued = submit("/steer keep   all the context")
    pending = RuntimeInbox(store).next_pending(current.session)
    assert pending["operation_id"] == ordinary.operation_id
    with store._read() as c:
        row = c.execute(
            "SELECT state,input_text FROM channel_steering WHERE operation_id=?",
            (queued.operation_id,),
        ).fetchone()
        fallback = c.execute(
            "SELECT payload_json FROM runtime_submissions WHERE operation_id=?",
            (queued.operation_id,),
        ).fetchone()
    assert tuple(row) == ("queued", "keep   all the context")
    assert json.loads(fallback[0])["input_text"] == "keep   all the context"


def test_active_mode_steer_is_written_once_and_decoder_reconciles_reply(tmp_path):
    store, current, request, _, controls, _, submit = make(tmp_path)
    active = submit("active work")
    request = replace(request, operation_id=active.operation_id, input_text=active.input_text)
    store.begin(current, request)
    submit("/messages steer")
    directed = submit("guide this turn")
    assert RuntimeInbox(store).query(current.session, directed.operation_id) is None

    sent = []
    client = SimpleNamespace(
        thread="thread-1", turn="turn-1", _send=lambda message: sent.append(message)
    )
    port = controls.steering_port(request)
    port.poll(client)
    assert sent == [turn_steer_message(
        "steer-" + digest_json(directed.operation_id),
        "thread-1", "turn-1", "guide this turn",
    )]
    assert port.consume_response({"id": sent[0]["id"], "result": {"turnId": "turn-1"}})
    with store._read() as c:
        assert c.execute(
            "SELECT state FROM channel_steering WHERE operation_id=?", (directed.operation_id,)
        ).fetchone()[0] == "sent"
        replies = [row[0] for row in c.execute(
            "SELECT reply FROM channel_controls WHERE route_json=(SELECT route_json FROM "
            "channel_steering WHERE operation_id=?)", (directed.operation_id,)
        ).fetchall()]
    assert "已收到引導要求，等待 Codex 確認。" in replies
    assert "Codex 已確認收到引導。" in replies


def test_active_queue_mode_has_visible_ack_and_retains_next_turn_text(tmp_path):
    store, current, request, _, controls, _, submit = make(tmp_path)
    active = submit("active work")
    request = replace(request, operation_id=active.operation_id, input_text=active.input_text)
    store.begin(current, request)
    queued = submit("run this next")
    assert RuntimeInbox(store).query(current.session, queued.operation_id)["status"] == "pending"
    with store._read() as c:
        assert c.execute(
            "SELECT reply FROM channel_controls WHERE operation_id=?", (queued.operation_id,)
        ).fetchone()[0] == "已排入下一個工作。"


def test_terminal_race_queues_unsent_steer_with_distinct_visible_receipt(tmp_path):
    store, current, request, _, controls, _, submit = make(tmp_path)
    active = submit("active work")
    request = replace(request, operation_id=active.operation_id, input_text=active.input_text)
    store.begin(current, request)
    directed = submit("/steer continue after terminal")
    controls.steering_port(request).reconcile_terminal()
    assert RuntimeInbox(store).query(current.session, directed.operation_id)["status"] == "pending"
    with store._read() as c:
        state = c.execute(
            "SELECT state FROM channel_steering WHERE operation_id=?", (directed.operation_id,)
        ).fetchone()[0]
        replies = [row[0] for row in c.execute(
            "SELECT reply FROM channel_controls WHERE route_json=(SELECT route_json FROM "
            "channel_steering WHERE operation_id=?)", (directed.operation_id,)
        ).fetchall()]
    assert state == "queued"
    assert "目前回合已結束；內容已排入下一個工作。" in replies


def test_correlated_rejection_is_durable_and_never_replayed(tmp_path):
    store, current, request, _, controls, _, submit = make(tmp_path)
    active = submit("active work")
    request = replace(request, operation_id=active.operation_id, input_text=active.input_text)
    store.begin(current, request)
    directed = submit("/steer preserve this")
    sent = []
    port = controls.steering_port(request)
    port.poll(SimpleNamespace(thread="thread-1", turn="turn-1", _send=sent.append))
    assert port.consume_response({
        "id": sent[0]["id"], "error": {"code": -32000, "message": "stale"},
    })
    port.poll(SimpleNamespace(thread="thread-1", turn="turn-1", _send=sent.append))
    assert len(sent) == 1
    with store._read() as c:
        assert tuple(c.execute(
            "SELECT state,input_text FROM channel_steering WHERE operation_id=?",
            (directed.operation_id,),
        ).fetchone()) == ("rejected", "preserve this")


def test_terminal_reconciliation_marks_sent_write_unknown_without_replay(tmp_path):
    store, current, request, _, controls, _, submit = make(tmp_path)
    active = submit("active work")
    request = replace(request, operation_id=active.operation_id, input_text=active.input_text)
    store.begin(current, request)
    directed = submit("/steer do not replay")
    sent = []
    port = controls.steering_port(request)
    port.poll(SimpleNamespace(thread="thread-1", turn="turn-1", _send=sent.append))
    port.reconcile_terminal()
    port.poll(SimpleNamespace(thread="thread-1", turn="turn-1", _send=sent.append))
    assert len(sent) == 1
    assert RuntimeInbox(store).query(current.session, directed.operation_id) is None
    with store._read() as c:
        state = c.execute(
            "SELECT state FROM channel_steering WHERE operation_id=?", (directed.operation_id,)
        ).fetchone()[0]
        replies = [row[0] for row in c.execute(
            "SELECT reply FROM channel_controls WHERE route_json=(SELECT route_json FROM "
            "channel_steering WHERE operation_id=?)", (directed.operation_id,)
        ).fetchall()]
    assert state == "unknown"
    assert "引導結果未確認；內容已保留，不會自動重送。" in replies
