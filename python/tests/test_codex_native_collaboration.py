"""Native collaboration notifications stay scoped to the parent and enrolled children."""

from copy import deepcopy

import pytest
from test_codex_client import setup

PARENT_THREAD = "thread"
PARENT_TURN = "turn"
CHILD_THREAD = "child"
CHILD_TURN = "child-turn"


def _spawn(
    method,
    *,
    item_id="spawn",
    parent_thread=PARENT_THREAD,
    parent_turn=PARENT_TURN,
    status="inProgress",
    receiver_thread_ids=(),
    agents_states=None,
    tool="spawnAgent",
    model="gpt-5.6-luna",
    reasoning_effort="max",
):
    item = {
        "type": "collabAgentToolCall",
        "id": item_id,
        "tool": tool,
        "status": status,
        "senderThreadId": parent_thread,
        "receiverThreadIds": list(receiver_thread_ids),
        "prompt": "child task",
        "model": model,
        "reasoningEffort": reasoning_effort,
        "agentsStates": {} if agents_states is None else deepcopy(agents_states),
    }
    return {
        "method": method,
        "params": {
            "threadId": parent_thread,
            "turnId": parent_turn,
            "item": item,
        },
    }


def spawn_started(**changes):
    return _spawn("item/started", **changes)


def spawn_completed(*, child=CHILD_THREAD, **changes):
    return _spawn(
        "item/completed",
        status="completed",
        receiver_thread_ids=[child],
        agents_states={child: {"status": "pendingInit", "message": None}},
        **changes,
    )


def child_turn(
    method="turn/started",
    *,
    thread=CHILD_THREAD,
    turn=CHILD_TURN,
    status="inProgress",
    items=None,
    items_view="summary",
    error=None,
):
    return {
        "method": method,
        "params": {
            "threadId": thread,
            "turn": {
                "id": turn,
                "status": status,
                "itemsView": items_view,
                "items": [] if items is None else deepcopy(items),
                "error": error,
            },
        },
    }


def child_status(*, thread=CHILD_THREAD):
    return {
        "method": "thread/status/changed",
        "params": {
            "threadId": thread,
            "status": {"type": "active", "activeFlags": []},
        },
    }


def child_mcp(*, thread=CHILD_THREAD):
    return {
        "method": "mcpServer/startupStatus/updated",
        "params": {
            "threadId": thread,
            "name": "fixture",
            "status": "ready",
            "error": None,
            "failureReason": None,
        },
    }


def child_item(*, thread=CHILD_THREAD, turn=CHILD_TURN):
    return {
        "method": "item/started",
        "params": {
            "threadId": thread,
            "turnId": turn,
            "item": {"type": "reasoning", "id": "child-item", "summary": [], "content": []},
        },
    }


def token_usage(*, thread, turn, total):
    counts = {
        "inputTokens": 1,
        "outputTokens": 1,
        "cachedInputTokens": 0,
        "reasoningOutputTokens": 0,
        "totalTokens": total,
    }
    return {
        "method": "thread/tokenUsage/updated",
        "params": {
            "threadId": thread,
            "turnId": turn,
            "tokenUsage": {
                "last": counts,
                "total": dict(counts, totalTokens=total + 10),
            },
        },
    }


def child_delta(*, thread=CHILD_THREAD, turn=CHILD_TURN, text="child progress"):
    return {
        "method": "item/agentMessage/delta",
        "params": {
            "threadId": thread,
            "turnId": turn,
            "itemId": "child-message",
            "delta": text,
        },
    }


def run_events(events, *, parent_items=None):
    peer, client, request = setup()
    peer.messages[3:3] = deepcopy(events)
    if parent_items is not None:
        peer.messages[-1]["params"]["turn"]["items"] = deepcopy(parent_items)
    return peer, client, request, client.run_admitted(request)


def completed_child_events(*, child=CHILD_THREAD, turn=CHILD_TURN, status="completed", items=None):
    return [
        child_turn(thread=child, turn=turn),
        child_turn(
            "turn/completed",
            thread=child,
            turn=turn,
            status=status,
            items=items,
        ),
    ]


def natural_events(*, child=CHILD_THREAD, item_id="spawn", child_status_value="completed",
                   items=None):
    return [
        spawn_started(item_id=item_id),
        *completed_child_events(
            child=child,
            turn=f"{child}-turn",
            status=child_status_value,
            items=items,
        ),
        spawn_completed(item_id=item_id, child=child),
    ]


def test_early_child_turn_is_buffered_until_exact_spawn_completion():
    peer, _, _, result = run_events(natural_events())

    assert result.disposition == "completed"
    assert result.output_text == "answer"
    assert result.accepted_backend_turns == 1
    assert peer.closed


@pytest.mark.parametrize("notification", [child_status(thread=PARENT_THREAD),
                                         child_mcp(thread=PARENT_THREAD)])
def test_parent_status_notifications_remain_before_parent_final(notification):
    events = natural_events()
    events.insert(-1, notification)
    _, _, _, result = run_events(events)

    assert result.disposition == "completed"
    assert result.output_text == "answer"


def test_child_summary_final_is_not_projected_as_parent_final():
    child_items = [{
        "type": "agentMessage",
        "id": "child-final",
        "phase": "final_answer",
        "text": "child answer",
    }]
    _, _, _, result = run_events(natural_events(items=child_items))

    assert result.disposition == "completed"
    assert result.output_text == "answer"
    assert "child answer" not in result.output_text


@pytest.mark.parametrize("status", ["interrupted", "failed"])
def test_interrupted_or_failed_child_is_terminal_for_parent_completion(status):
    _, _, _, result = run_events(natural_events(child_status_value=status))

    assert result.disposition == "completed"
    assert result.output_text == "answer"


def test_parent_token_snapshot_excludes_child_token_snapshot():
    events = natural_events()
    events.insert(2, token_usage(thread=PARENT_THREAD, turn=PARENT_TURN, total=7))
    events.insert(3, token_usage(thread=CHILD_THREAD, turn=CHILD_TURN, total=99))
    _, _, _, result = run_events(events)

    assert result.disposition == "completed"
    assert result.token_observation.last.total_tokens == 7
    assert result.token_observation.total.total_tokens == 17


@pytest.mark.parametrize("notification", [child_status(), child_mcp(), child_item()])
def test_known_child_status_mcp_and_item_notifications_are_isolated(notification):
    started, completed = completed_child_events()
    events = [spawn_started(), started, notification, completed, spawn_completed()]
    _, _, _, result = run_events(events)

    assert result.disposition == "completed"
    assert result.output_text == "answer"


@pytest.mark.parametrize(
    "events",
    [
        [spawn_started(), child_turn()],
        [spawn_started(), *completed_child_events(),],
    ],
    ids=["active-child", "pending-spawn"],
)
def test_parent_final_requires_terminal_children_and_completed_spawn(events):
    _, _, _, result = run_events(events)

    assert result.disposition == "unknown"


@pytest.mark.parametrize(
    "events",
    [
        [spawn_started(parent_turn="foreign")],
        [spawn_started(item_id="spawn", parent_thread=PARENT_THREAD),
         child_turn(thread="foreign", turn="child-turn")],
        [spawn_started(), child_turn(turn="foreign")],
        [spawn_started(), *completed_child_events(), spawn_completed(item_id="other")],
    ],
    ids=["parent-turn", "child-thread", "child-turn", "spawn-item"],
)
def test_foreign_parent_child_or_spawn_binding_is_rejected(events):
    _, _, _, result = run_events(events)

    assert result.disposition == "unknown"


@pytest.mark.parametrize(
    "field,value",
    [
        ("tool", "wait"),
        ("status", "completed"),
        ("receiverThreadIds", [CHILD_THREAD]),
        ("agentsStates", []),
        ("model", "gpt-5.6-terra"),
        ("reasoningEffort", "medium"),
        ("id", ""),
    ],
)
def test_malformed_spawn_item_is_rejected(field, value):
    event = spawn_started()
    event["params"]["item"][field] = value
    _, _, _, result = run_events([event, spawn_completed(), *completed_child_events()])

    assert result.disposition == "unknown"


@pytest.mark.parametrize(
    "events",
    [
        [spawn_started(), spawn_started()],
        [spawn_started(), *completed_child_events(), spawn_completed(), spawn_completed()],
        [spawn_started(), *completed_child_events(), *completed_child_events(), spawn_completed()],
    ],
    ids=["duplicate-spawn-start", "duplicate-spawn-complete", "duplicate-child-terminal"],
)
def test_duplicate_spawn_or_child_records_are_rejected(events):
    _, _, _, result = run_events(events)

    assert result.disposition == "unknown"


@pytest.mark.parametrize("count", [32, 33])
def test_unenrolled_child_mcp_advisories_do_not_fill_the_effect_buffer(count):
    events = [spawn_started()]
    events.extend(child_mcp() for _ in range(count))
    events.extend([spawn_completed(), *completed_child_events()])
    _, _, _, result = run_events(events)

    assert result.disposition == "completed"


@pytest.mark.parametrize("count,size", [(40, 10), (1, 128 * 1024 + 1)])
def test_valid_prebinding_frames_have_no_extra_count_or_byte_quota(count, size):
    events = [spawn_started(), child_turn()]
    events.extend(child_delta(text="x" * size) for _ in range(count))
    events.extend([child_turn("turn/completed", status="completed"), spawn_completed()])
    _, _, _, result = run_events(events)

    assert result.disposition == "completed"


def test_child_server_request_never_receives_parent_authority():
    request = {
        "id": 9,
        "method": "item/commandExecution/requestApproval",
        "params": {
            "threadId": CHILD_THREAD,
            "turnId": CHILD_TURN,
            "itemId": "child-command",
            "reason": "child request",
        },
    }
    events = [spawn_started(), *completed_child_events(), request, spawn_completed()]
    peer, _, _, result = run_events(events)

    assert result.disposition == "unknown"
    assert peer.sent[-1] == {"id": 9, "error": {"code": -32601, "message": "unsupported method"}}


def test_parent_final_without_final_answer_remains_unknown():
    _, _, _, result = run_events(natural_events(), parent_items=[])

    assert result.disposition == "unknown"


@pytest.mark.parametrize("count", [16, 17])
def test_correlated_children_have_no_lifetime_registration_quota(count):
    events = []
    for index in range(count):
        child = f"child-{index}"
        events.extend(natural_events(child=child, item_id=f"spawn-{index}"))
    _, _, _, result = run_events(events)

    assert result.disposition == "completed"


def test_valid_child_turns_continue_past_the_former_64_turn_quota():
    events = [spawn_started(), spawn_completed()]
    for index in range(65):
        events.extend(completed_child_events(turn=f"child-turn-{index}"))
    _, _, _, result = run_events(events)

    assert result.disposition == "completed"


def test_correlated_native_calls_have_no_extra_32_call_quota():
    events = [spawn_started(), spawn_completed(), *completed_child_events()]
    for index in range(33):
        events.append(spawn_started(item_id=f"wait-{index}", tool="wait",
            receiver_thread_ids=[CHILD_THREAD]))
    for index in range(33):
        events.append(_spawn("item/completed", item_id=f"wait-{index}", tool="wait",
            status="completed", receiver_thread_ids=[CHILD_THREAD],
            agents_states={CHILD_THREAD: {"status": "completed"}}))
    _, _, _, result = run_events(events)

    assert result.disposition == "completed"


def test_completed_native_call_id_cannot_be_reused():
    events = natural_events()
    pair = [spawn_started(item_id="wait", tool="wait", receiver_thread_ids=[CHILD_THREAD]),
        _spawn("item/completed", item_id="wait", tool="wait", status="completed",
            receiver_thread_ids=[CHILD_THREAD], agents_states={CHILD_THREAD: {"status": "completed"}})]
    events.extend(pair + deepcopy(pair))
    _, _, _, result = run_events(events)

    assert result.disposition == "unknown"


@pytest.mark.parametrize("wake", ["followupTask", "sendInput", "resumeAgent", "active-status"])
def test_old_child_terminal_cannot_settle_newly_active_work(wake):
    events = natural_events()
    if wake == "active-status":
        events.append(child_status())
    else:
        events.append(spawn_started(item_id="followup", tool=wake, receiver_thread_ids=[CHILD_THREAD]))
    events.append(child_turn("turn/completed", turn=f"{CHILD_THREAD}-turn", status="completed"))
    if wake != "active-status":
        events.append(_spawn("item/completed", item_id="followup", tool=wake, status="completed",
            receiver_thread_ids=[CHILD_THREAD], agents_states={CHILD_THREAD: {"status": "running"}}))
    _, _, _, result = run_events(events)

    assert result.disposition == "unknown"


@pytest.mark.parametrize("wake", ["followupTask", "sendInput", "resumeAgent"])
@pytest.mark.parametrize("terminal", ["completed", "failed", "interrupted"])
def test_unique_child_turn_can_settle_after_wake(wake, terminal):
    events = natural_events() + [
        spawn_started(item_id="followup", tool=wake, receiver_thread_ids=[CHILD_THREAD]),
        _spawn("item/completed", item_id="followup", tool=wake, status="completed",
            receiver_thread_ids=[CHILD_THREAD], agents_states={CHILD_THREAD: {"status": "running"}}),
        *completed_child_events(turn="new-child-turn", status=terminal),
    ]
    _, _, _, result = run_events(events)
    assert result.disposition == "completed"


@pytest.mark.parametrize("first,second", [("wait", "closeAgent"), ("spawnAgent", "wait"), ("wait", "spawnAgent")])
def test_native_call_ids_are_unique_across_tool_types(first, second):
    events = natural_events()
    identity = "spawn" if first == "spawnAgent" else "shared"
    if first != "spawnAgent":
        events.extend([spawn_started(item_id=identity, tool=first, receiver_thread_ids=[CHILD_THREAD]),
            _spawn("item/completed", item_id=identity, tool=first, status="completed",
                receiver_thread_ids=[CHILD_THREAD], agents_states={CHILD_THREAD: {"status": "completed"}})])
    receivers = [] if second == "spawnAgent" else [CHILD_THREAD]
    events.append(spawn_started(item_id=identity, tool=second, receiver_thread_ids=receivers))
    if second == "spawnAgent":
        events.extend([spawn_completed(item_id=identity, child="second"),
                       *completed_child_events(child="second", turn="second-turn")])
    else:
        events.append(_spawn("item/completed", item_id=identity, tool=second, status="completed",
            receiver_thread_ids=receivers, agents_states={CHILD_THREAD: {"status": "completed"}}))
    assert run_events(events)[-1].disposition == "unknown"


def test_different_native_senders_and_turns_can_use_their_own_item_ids():
    events = [spawn_started(), spawn_completed()]
    for thread, turn in [(PARENT_THREAD, PARENT_TURN), (CHILD_THREAD, "first"), (CHILD_THREAD, "second")]:
        if thread == CHILD_THREAD:
            events.append(child_turn(turn=turn))
        receivers = [CHILD_THREAD] if thread == PARENT_THREAD else [PARENT_THREAD]
        events.extend([
            spawn_started(item_id="same-item", tool="listAgents", parent_thread=thread,
                          parent_turn=turn, receiver_thread_ids=receivers),
            _spawn("item/completed", item_id="same-item", tool="listAgents", parent_thread=thread,
                parent_turn=turn, status="completed", receiver_thread_ids=receivers,
                agents_states={receivers[0]: {"status": "running"}}),
        ])
        if thread == CHILD_THREAD:
            events.append(child_turn("turn/completed", turn=turn, status="completed"))
    assert run_events(events)[-1].disposition == "completed"
