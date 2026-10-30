"""A stopped task remains conversational context, never execution authority."""

from dataclasses import replace
from types import SimpleNamespace

from test_channel_controls import make, reply
from test_runtime_conversation_context import _deliver, _submit_channel

from ahc.cli.foreground_turn_context import turn_context
from ahc.infrastructure.channel_control_store import ChannelControlStore
from ahc.infrastructure.threaded_runtime_repository import ThreadedRuntimeRepository


def render(store, session, operation_id):
    config = SimpleNamespace(
        session=session, to_dict=lambda: {"agent_context": {"history_turns": 4, "recall": False}}
    )
    tools = SimpleNamespace(workspace=SimpleNamespace(store=store), capabilities=lambda: ())
    request = SimpleNamespace(
        session=session,
        operation_id=operation_id,
        input_text="Continue the task after changing approvals.",
    )
    return turn_context(store, config, tools, request)


def test_stopped_unknown_task_survives_next_turn_and_reopen_without_replay(tmp_path):
    store, current, template, route, controls, _, submit = make(tmp_path)
    old = _submit_channel(store, template, route, "old", "old unrelated task", "old answer")
    _deliver(store, route, old.operation_id)
    submit("/new")
    stopped = _submit_channel(
        store,
        template,
        route,
        "stopped",
        "Compare the old deployment and current workspace.",
        "",
        disposition="unknown",
    )
    store._mutate(
        lambda c: c.execute("INSERT INTO channel_cancel VALUES(?)", (stopped.operation_id,))
    )
    submit("/approvals auto-approve")
    fresh = submit("Continue the task after changing approvals.")
    reopened = ThreadedRuntimeRepository(store.root)
    reopened.channel_controls = ChannelControlStore(reopened, controls.config)
    text = render(reopened, route.session, fresh.operation_id)
    assert "Compare the old deployment and current workspace." in text
    assert '"stop_requested": true' in text
    assert '"unresolved_effects": true' in text
    assert "old unrelated task" not in text
    assert reopened.read_current("session").session == current.session
    assert (
        reopened.read_turn(route.session, stopped.operation_id).result["unresolved_effects"] is True
    )
    assert "Do not replay" in text


def test_new_resets_interrupted_context_but_stop_does_not(tmp_path):
    store, _, template, route, _, _, submit = make(tmp_path)
    _submit_channel(
        store,
        template,
        route,
        "stopped",
        "retained task",
        "unverified answer",
        disposition="unknown",
    )
    assert "沒有執行中" in reply(store, submit("/stop"))
    fresh = submit("continue")
    text = render(store, route.session, fresh.operation_id)
    assert "retained task" in text and "unverified answer" not in text
    submit("/new")
    newer = submit("new topic")
    assert "retained task" not in render(store, route.session, newer.operation_id)


def test_incomplete_context_never_borrows_another_route(tmp_path):
    store, _, template, route, controls, _, submit = make(tmp_path)
    other = replace(route, channel_id="999")
    controls.config.to_dict()["channels"].append({"route": other.to_dict(), "enabled": True})
    # Enabled routes are intentionally captured at controller construction.
    store.channel_controls = ChannelControlStore(store, controls.config)
    _submit_channel(store, template, other, "other", "foreign task", "", disposition="unknown")
    fresh = submit("continue")
    assert "foreign task" not in render(store, route.session, fresh.operation_id)


def test_oversized_latest_pair_is_labelled_and_does_not_erase_context(tmp_path):
    store, _, template, route, _, _, submit = make(tmp_path)
    old = _submit_channel(
        store, template, route, "large", "task " + "x" * 3990, "answer " + "y" * 3990
    )
    _deliver(store, route, old.operation_id)
    fresh = submit("continue")
    text = render(store, route.session, fresh.operation_id)
    assert "task " in text and "answer " in text and '"truncated": true' in text
