"""Native wire ordering and lifecycle counterexamples from the actual pinned trace."""

from copy import deepcopy

import pytest
from test_codex_client import setup
from test_codex_native_collaboration import (
    child_mcp,
    child_status,
    child_turn,
    completed_child_events,
    run_events,
    spawn_completed,
    spawn_started,
)


def native_order():
    return [spawn_started(), child_status(), child_mcp(), spawn_completed(),
            *completed_child_events()]


def call(method, tool="followupTask", status="inProgress"):
    event = spawn_started(item_id="followup", tool=tool,
                          receiver_thread_ids=["child"], model=None, reasoning_effort=None)
    event["method"] = method
    event["params"]["item"]["status"] = status
    return event


def test_captured_native_notification_order_completes_only_parent_answer():
    _, _, _, result = run_events(native_order())
    assert result.disposition == "completed" and result.output_text == "answer"


def test_discarded_mcp_advisory_cannot_enroll_a_nearby_foreign_thread():
    _, client, _, result = run_events([spawn_started(), child_mcp(thread="unrelated"),
                                 spawn_completed(), *completed_child_events()])
    assert result.disposition == "completed"
    assert "unrelated" not in client._native_collaboration.children


@pytest.mark.parametrize("finish", [False, True])
def test_enrolled_active_child_must_finish_before_parent(finish):
    events = [spawn_started(), spawn_completed(), child_turn()]
    if finish:
        events.append(child_turn("turn/completed", status="completed"))
    assert run_events(events)[-1].disposition == ("completed" if finish else "unknown")


@pytest.mark.parametrize("finish", [False, True])
def test_followup_cannot_reuse_previous_child_terminal(finish):
    events = native_order() + [call("item/started"), call("item/completed", status="completed")]
    if finish:
        events += completed_child_events(turn="next-child-turn")
    assert run_events(events)[-1].disposition == ("completed" if finish else "unknown")


def test_late_status_alone_cannot_reactivate_a_completed_turn():
    _, client, _, result = run_events(native_order() + [child_status()])
    assert result.disposition == "completed"
    assert client._native_collaboration.children["child"].terminal


def test_nested_native_child_is_scoped_to_its_known_parent():
    events = [spawn_started(), spawn_completed(), child_turn(),
              spawn_started(parent_thread="child", parent_turn="child-turn", item_id="nested"),
              child_status(thread="grandchild"),
              spawn_completed(parent_thread="child", parent_turn="child-turn", item_id="nested",
                              child="grandchild"),
              *completed_child_events(child="grandchild", turn="grand-turn"),
              child_turn("turn/completed", status="completed")]
    assert run_events(events)[-1].disposition == "completed"


@pytest.mark.parametrize("drift", [False, True])
def test_child_settings_use_its_selected_model_without_parent_policy_drift(drift):
    peer, _, _ = setup()
    settings = deepcopy(peer.messages[1]["result"])
    settings.pop("thread")
    settings["model"], settings["effort"] = "gpt-5.6-luna", "max"
    settings["sandboxPolicy"] = settings.pop("sandbox")
    if drift:
        settings["sandboxPolicy"] = {"type": "dangerFullAccess"}
    event = {"method": "thread/settings/updated",
             "params": {"threadId": "child", "threadSettings": settings}}
    events = [spawn_started(), spawn_completed(), event, *completed_child_events()]
    assert run_events(events)[-1].disposition == ("unknown" if drift else "completed")


@pytest.mark.parametrize("cancel", [False, True])
def test_parent_stop_with_active_child_remains_unknown_and_closes_transport(cancel):
    peer, client, request = setup()
    peer.messages[3:3] = [spawn_started(), spawn_completed(), child_turn()]
    if cancel:
        client.cancelled = lambda: len(peer.messages) == 1
    else:
        client.clock = lambda: 101 if len(peer.messages) == 1 else 0
    result = client.run_admitted(request)
    assert result.disposition == "unknown" and result.unresolved_effects and peer.closed
    interrupts = [m for m in peer.sent if m.get("method") == "turn/interrupt"]
    assert len(interrupts) == int(cancel)
    if interrupts:
        assert interrupts[0]["params"] == {"threadId": "thread", "turnId": "turn"}
