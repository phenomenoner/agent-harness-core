"""A native child's first turn may precede the parent's spawn confirmation."""

import pytest
from test_codex_app_server_contract import activity
from test_codex_native_collaboration import child_turn, run_events, spawn_started


def provenance():
    return {"method":"thread/started", "params":{"thread":{"id":"child",
        "source":{"subAgent":{"thread_spawn":{"parent_thread_id":"thread",
                                                  "agent_path":"/root/worker"}}}}}}


@pytest.mark.parametrize("source_first", [False, True])
def test_early_child_turn_is_bound_by_later_exact_spawn_evidence(source_first):
    events = [child_turn()]
    if source_first:
        events.append(provenance())
    events += [activity(), child_turn("turn/completed", status="completed")]
    _, client, _, result = run_events(events)
    assert result.disposition == "completed" and result.output_text == "answer"
    assert client._native_collaboration.children["child"].terminal


def test_unproven_foreign_telemetry_cannot_supply_parent_answer_or_block_it():
    events = [child_turn(thread="unrelated"),
        child_turn("turn/completed", thread="unrelated", status="completed", items=[{
            "type":"agentMessage", "id":"forged", "phase":"final_answer", "text":"FOREIGN"}])]
    _, client, _, result = run_events(events)
    assert result.disposition == "completed" and result.output_text == "answer"
    assert client._native_collaboration.children == {}


def test_early_child_still_requires_its_own_exact_terminal_after_binding():
    assert run_events([child_turn(), activity()])[-1].disposition == "unknown"


def test_foreign_spawn_notification_cannot_enroll_work_in_current_parent():
    _, client, _, result = run_events([spawn_started(parent_thread="foreign")])
    assert result.disposition == "completed" and result.output_text == "answer"
    assert client._native_collaboration.children == {} and client._native_collaboration.spawns == {}
