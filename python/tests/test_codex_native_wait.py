"""Wait completion lists describe returned targets, not every requested target."""

from copy import deepcopy

import pytest

from test_codex_native_collaboration import (
    child_turn,
    completed_child_events,
    run_events,
    spawn_completed,
    spawn_started,
)


def wait_pair(targets, returned, *, tool="wait", status="completed"):
    started = spawn_started(item_id="wait", tool=tool,
                            receiver_thread_ids=targets, model=None,
                            reasoning_effort=None)
    completed = deepcopy(started)
    completed["method"] = "item/completed"
    completed["params"]["item"].update(
        status=status, receiverThreadIds=returned,
        agentsStates={target: {"status": "completed", "message": "done"}
                      for target in returned},
    )
    return started, completed


def enrolled():
    return [spawn_started(), spawn_completed(),
            spawn_started(item_id="second"),
            spawn_completed(item_id="second", child="second"),
            *completed_child_events(),
            *completed_child_events(child="second", turn="second-turn")]


@pytest.mark.parametrize("returned", [[], ["child"], ["second"],
                                      ["second", "child"], ["child", "second"]])
def test_wait_accepts_returned_subset_of_exact_started_targets(returned):
    events = enrolled() + list(wait_pair(["child", "second"], returned))
    assert run_events(events)[-1].disposition == "completed"


@pytest.mark.parametrize("finish", [False, True])
def test_timeout_does_not_finish_running_child(finish):
    events = [spawn_started(), spawn_completed(), child_turn(),
              *wait_pair(["child"], [])]
    if finish:
        events.append(child_turn("turn/completed", status="completed"))
    assert run_events(events)[-1].disposition == ("completed" if finish else "unknown")


@pytest.mark.parametrize("returned", [["second"], ["foreign"], ["child", "child"]])
def test_wait_rejects_targets_not_requested_even_if_owned(returned):
    events = enrolled() + list(wait_pair(["child"], returned))
    assert run_events(events)[-1].disposition == "unknown"


@pytest.mark.parametrize("change", ["item", "sender", "turn", "states", "status"])
def test_wait_subset_still_requires_exact_call_and_consistent_shape(change):
    start, end = wait_pair(["child"], [])
    item = end["params"]["item"]
    if change == "item":
        item["id"] = "unstarted"
    elif change == "sender":
        item["senderThreadId"] = "foreign"
    elif change == "turn":
        end["params"]["turnId"] = "other-turn"
    elif change == "states":
        item["receiverThreadIds"] = ["child"]
    else:
        item["status"] = "inProgress"
    assert run_events(enrolled() + [start, end])[-1].disposition == "unknown"


@pytest.mark.parametrize("tool", ["sendInput", "resumeAgent", "closeAgent"])
def test_other_native_operations_cannot_shrink_receiver_binding(tool):
    assert run_events(enrolled() + list(wait_pair(["child"], [], tool=tool)))[-1].disposition == "unknown"


def test_partial_wait_does_not_settle_other_running_target():
    events = [spawn_started(), spawn_completed(), *completed_child_events(),
              spawn_started(item_id="second"),
              spawn_completed(item_id="second", child="second"),
              child_turn(thread="second", turn="second-turn"),
              *wait_pair(["child", "second"], ["child"])]
    assert run_events(events)[-1].disposition == "unknown"
