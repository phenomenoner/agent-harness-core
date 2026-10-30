import json
from dataclasses import replace
from types import SimpleNamespace

import pytest
from test_channel_controls import make, reply

from ahc.domain.channel_files import ChannelFile
from ahc.domain.channel_ingress import ChannelInboundEvent
from ahc.domain.sessions import ContractError
from ahc.infrastructure.channel_context_store import read_context
from ahc.infrastructure.channel_file_failure import record_failure
from ahc.infrastructure.channel_file_inbox import ChannelFileInbox
from ahc.infrastructure.runtime_inbox import RuntimeInbox
from ahc.infrastructure.telegram_inbox import TelegramInbox


def admit(store, current, event):
    return RuntimeInbox(store).submit(current.session, event.operation_id, current.goal.digest,
                                      event.input_text, channel_event=event)


def test_context_roundtrip_and_legacy_wire_unchanged(tmp_path):
    _, _, _, route, _, _, _ = make(tmp_path)
    legacy = ChannelInboundEvent(route, "one", "continue")
    assert "context" not in legacy.to_dict()
    rich = replace(legacy, context={"reply_to_id": "10", "quote": "/stop"})
    assert ChannelInboundEvent.from_dict(rich.to_dict()) == rich
    assert rich.operation_id == legacy.operation_id
    with pytest.raises(ContractError):
        replace(legacy, context={"token": "not allowed"})


def test_quote_durable_separate_command_and_duplicate_binding(tmp_path):
    store, current, _, route, _, _, _ = make(tmp_path)
    event = ChannelInboundEvent(route, "one", "continue", context={"quote": "/stop"})
    admit(store, current, event)
    req = SimpleNamespace(operation_id=event.operation_id, session=current.session)
    assert read_context(store, req) == {"quote": "/stop"}
    assert RuntimeInbox(store).next_pending(current.session)["input_text"] == "continue"
    admit(store, current, event)
    with pytest.raises(ContractError, match="context operation"):
        admit(store, current, replace(event, context={"quote": "different"}))


def test_quote_survives_terminal_history_and_projects_to_next_turn(tmp_path):
    from ahc.application.runtime_service import RuntimeService
    from ahc.cli.foreground_turn_context import turn_context
    from ahc.domain.agent_backend import AgentTurnObservation
    from ahc.infrastructure.runtime_conversation_context import conversation_context

    store, current, request, route, controls, _, _ = make(tmp_path)
    event = ChannelInboundEvent(route, "quoted", "continue", context={"quote": "original task"})
    admit(store, current, event)
    bound = replace(request, operation_id=event.operation_id, input_text=event.input_text)
    class Backend:
        def invoke(self, value):
            return AgentTurnObservation(value, "completed", "fixture", 1, 1,
                                        output_text="answer", unresolved_effects=False)
    RuntimeService(store, Backend()).execute(current, bound)
    fresh = replace(event, event_id="next", input_text="what next?", context={"quote": "new quote"})
    current = store.read_current(current.session.ref.virtual_session_id)
    admit(store, current, fresh)
    history = conversation_context(store, current.session, fresh.operation_id,
                                   include_incomplete=True)
    assert history[0]["channel_message"]["quote"] == "original task"
    config = SimpleNamespace(session=current.session,
        to_dict=lambda: {"agent_context": {"history_turns": 4, "recall": False}})
    tools = SimpleNamespace(workspace=SimpleNamespace(store=store), capabilities=lambda: ())
    projected = turn_context(store, config, tools, replace(bound, operation_id=fresh.operation_id,
                                                           input_text=fresh.input_text))
    assert '"quote": "new quote"' in projected
    assert '"quote": "original task"' in projected


def test_notice_is_durable_control_not_model_and_does_not_block_next(tmp_path):
    store, current, _, route, _, _, _ = make(tmp_path)
    event = ChannelInboundEvent(route, "notice", "attachment", context={"notice": "不支援影片"})
    admit(store, current, event)
    admit(store, current, event)
    assert reply(store, event) == "不支援影片"
    assert RuntimeInbox(store).next_pending(current.session) is None
    next_event = replace(event, event_id="good", input_text="hello", context=None)
    admit(store, current, next_event)
    assert (RuntimeInbox(store).next_pending(current.session)["operation_id"]
            == next_event.operation_id)


def test_forwarded_slash_is_data_not_a_runtime_command(tmp_path):
    store, current, _, route, _, _, _ = make(tmp_path)
    event = ChannelInboundEvent(route, "forward", "/new", context={"forwarded": True})
    admit(store, current, event)
    assert RuntimeInbox(store).next_pending(current.session)["operation_id"] == event.operation_id
    with store._read() as c:
        assert not c.execute("SELECT 1 FROM channel_controls WHERE operation_id=?",
                             (event.operation_id,)).fetchone()


def test_rich_active_steer_is_explicitly_queued_without_losing_context(tmp_path):
    store, current, request, route, _, _, submit = make(tmp_path)
    active = submit("working")
    store.begin(current, replace(request, operation_id=active.operation_id,
                                 input_text=active.input_text))
    submit("/messages steer")
    event = ChannelInboundEvent(route, "quote", "/steer continue", context={"quote": "task"})
    admit(store, current, event)
    with store._read() as c:
        payload = json.loads(c.execute("SELECT payload_json FROM runtime_submissions WHERE "
                                       "operation_id=?", (event.operation_id,)).fetchone()[0])
        assert not c.execute("SELECT 1 FROM channel_steering WHERE operation_id=?",
                             (event.operation_id,)).fetchone()
    assert payload["input_text"] == "continue"
    assert "完整排入" in reply(store, event)


def file_event(route, event_id="file"):
    return ChannelInboundEvent(route, event_id, "see file",
        (ChannelFile("telegram", "2", "file-id", "image.png", 4, "image/png"),),
        {"attachments": [{"filename": "image.png", "media_type": "image/png",
                          "status": "pending"}]})


def test_file_failure_survives_reopen_and_new_holds_staged_file(tmp_path):
    store, current, _, route, _, _, submit = make(tmp_path)
    event = file_event(route)
    inbox = ChannelFileInbox(store)
    inbox.stage(event, current.goal.digest)
    record_failure(store, event)
    record_failure(store, event)
    assert inbox.pending(route) == []
    assert "尚未執行" in reply(store, event)
    fresh = file_event(route, "two")
    inbox.stage(fresh, current.goal.digest)
    assert len(inbox.pending(route)) == 1
    submit("/new")
    assert inbox.pending(route) == []


def test_real_decoder_bad_media_between_good_messages_advances_cursor(tmp_path):
    store, current, _, route, _, _, _ = make(tmp_path)
    def update(n, **content):
        return {"update_id": n, "message": {"message_id": n, "chat": {"id": 2},
                    "from": {"id": 3, "is_bot": False}, **content}}
    inbox = TelegramInbox(store, [route], include_files=True)
    bad = update(2, document={"file_id": "f", "file_name": "big.bin", "file_size": 1048577})
    result = inbox.admit([update(1, text="one"), bad, update(3, text="three")],
                         expected_offset=None)
    assert result["next_offset"] == 4
    with store._read() as c:
        assert c.execute("SELECT count(*) FROM runtime_submissions").fetchone()[0] == 2
        assert c.execute("SELECT count(*) FROM channel_controls").fetchone()[0] == 1
