"""Final answer evidence is streamed; history readback is finite and turn-bound."""
from copy import deepcopy

import pytest

from test_codex_client import setup


def streamed(peer, *, before_ack=False, text="answer"):
    full = peer.messages[-1]["params"]["turn"]
    item = deepcopy(full["items"][0])
    item["text"] = text
    event = {"method": "item/completed", "params": {
        "threadId": "thread", "turnId": "turn", "item": item}}
    peer.messages.insert(2 if before_ack else -1, event)
    peer.messages[-1]["params"]["turn"] = {**full, "items": [], "itemsView": "summary"}
    return event


def test_streamed_final_and_summary_never_reread_whole_turn():
    peer, client, request = setup()
    streamed(peer)
    result = client.run_admitted(request)
    assert result.disposition == "completed"
    assert result.output_text == "answer"
    assert not any(m.get("method", "").startswith("thread/") and
                   m["method"] not in {"thread/start", "thread/resume"} for m in peer.sent)


def test_streamed_final_before_start_ack_is_retained():
    peer, client, request = setup()
    streamed(peer, before_ack=True)
    result = client.run_admitted(request)
    assert result.disposition == "completed" and result.output_text == "answer"
    assert len([m for m in peer.sent if m.get("method") == "turn/start"]) == 1


def summary_only(peer):
    full = deepcopy(peer.messages[-1]["params"]["turn"])
    peer.messages[-1]["params"]["turn"] = {**full, "items": [], "itemsView": "summary"}
    return full["items"][0]


def page(item, cursor=None, turn="turn"):
    return {"id": 5, "result": {"data": [{"turnId": turn, "item": item}],
                                "nextCursor": cursor}}


@pytest.mark.parametrize("view", ["summary", "notLoaded"])
def test_multiple_completed_finals_keep_order_and_ignore_other_payloads(view):
    peer, client, request = setup()
    event = streamed(peer, text="first")
    second = deepcopy(event)
    second["params"]["item"].update(id="second", text="second")
    peer.messages.insert(-1, second)
    for changes in [{"phase": "commentary"}, {"delivery": "async"}, {"questions": ["q"]},
                    {"type": "mcpToolCall"}]:
        ignored = deepcopy(event)
        ignored["params"]["item"].update(id="ignored", text="PRIVATE", **changes)
        peer.messages.insert(-1, ignored)
    peer.messages[-1]["params"]["turn"]["itemsView"] = view
    result = client.run_admitted(request)
    assert result.disposition == "completed" and result.output_text == "first\nsecond"
    assert not any(m.get("method") == "thread/items/list" for m in peer.sent)


@pytest.mark.parametrize("failure", ["missing-terminal", "failed", "interrupted", "thread",
                                   "turn", "duplicate", "invalid-text", "surrogate"])
def test_streamed_answer_never_replaces_exact_successful_terminal(failure):
    peer, client, request = setup()
    event = streamed(peer)
    if failure == "missing-terminal":
        peer.messages.pop()
    elif failure in {"failed", "interrupted"}:
        peer.messages[-1]["params"]["turn"]["status"] = failure
    elif failure in {"thread", "turn"}:
        event["params"]["threadId" if failure == "thread" else "turnId"] = "other"
    elif failure == "duplicate":
        peer.messages.insert(-1, deepcopy(event))
    else:
        event["params"]["item"]["text"] = 7 if failure == "invalid-text" else chr(0xD800)
    result = client.run_admitted(request)
    assert result.disposition == "unknown" and not result.output_text
    assert len([m for m in peer.sent if m.get("method") == "turn/start"]) == 1
    assert not any(m.get("method") == "thread/turns/list" for m in peer.sent)


def test_fallback_pages_exact_turn_and_reconstructs_chronological_answers():
    peer, client, request = setup()
    final = summary_only(peer)
    peer.messages.extend([page({**final, "id": "second", "text": "second"}, "next"),
                          page({**final, "id": "first", "text": "first"})])
    result = client.run_admitted(request)
    assert result.disposition == "completed" and result.output_text == "first\nsecond"
    reads = [m for m in peer.sent if m.get("method") == "thread/items/list"]
    assert len(reads) == 2
    assert all(m["params"]["turnId"] == "turn" and m["params"]["limit"] == 1 for m in reads)
    assert "cursor" not in reads[0]["params"] and reads[1]["params"]["cursor"] == "next"


@pytest.mark.parametrize("fault", ["wrong-turn", "duplicate-item", "cursor-loop", "empty-cursor",
                                 "bad-cursor", "over-page", "empty-progress", "missing-final",
                                 "unsupported"])
def test_invalid_or_unsupported_readback_is_unknown_without_full_fallback(fault):
    peer, client, request = setup()
    final = summary_only(peer)
    first = page(final, "next")
    second = page({**final, "id": "second"})
    if fault == "wrong-turn":
        first["result"]["data"][0]["turnId"] = "other"
    elif fault == "duplicate-item":
        second = page(final)
    elif fault == "cursor-loop":
        second["result"]["nextCursor"] = "next"
    elif fault in {"empty-cursor", "bad-cursor"}:
        first["result"]["nextCursor"] = "" if fault == "empty-cursor" else 7
    elif fault == "over-page":
        first["result"]["data"] *= 2
    elif fault == "empty-progress":
        first["result"]["data"] = []
    elif fault == "missing-final":
        first = page({"type": "mcpToolCall", "id": "tool", "text": "not an answer"})
    else:
        first = {"id": 5, "error": {"code": -32601, "message": "unsupported"}}
    peer.messages.extend([first, second])
    result = client.run_admitted(request)
    assert result.disposition == "unknown" and not result.output_text
    assert not any(m.get("method") == "thread/turns/list" for m in peer.sent)
    assert len([m for m in peer.sent if m.get("method") == "turn/start"]) == 1


@pytest.mark.parametrize("over", [False, True])
def test_readback_page_budget_at_boundary(monkeypatch, over):
    from ahc.adapters import codex_terminal_readback as readback
    monkeypatch.setattr(readback, "MAX_READBACK_PAGES", 2)
    peer, client, request = setup()
    final = summary_only(peer)
    peer.messages.extend([page(final, "next"),
                          page({**final, "id": "second"}, "more" if over else None)])
    result = client.run_admitted(request)
    assert result.disposition == ("unknown" if over else "completed")
    assert len([m for m in peer.sent if m.get("method") == "thread/items/list"]) == 2


@pytest.mark.parametrize("over", [False, True])
def test_readback_byte_budget_at_boundary(monkeypatch, over):
    import json
    from ahc.adapters import codex_terminal_readback as readback
    peer, client, request = setup()
    reply = page(summary_only(peer))
    size = len(json.dumps(reply["result"], ensure_ascii=True, allow_nan=False).encode("utf-8"))
    monkeypatch.setattr(readback, "MAX_READBACK_BYTES", size - int(over))
    peer.messages.append(reply)
    result = client.run_admitted(request)
    assert result.disposition == ("unknown" if over else "completed")


@pytest.mark.parametrize("over", [False, True])
def test_streamed_answer_memory_budget_at_boundary(monkeypatch, over):
    from ahc.adapters import codex_final_messages as messages
    monkeypatch.setattr(messages, "MAX_FINAL_BYTES", 5 - int(over))
    peer, client, request = setup()
    streamed(peer, text="answer"[:5])
    result = client.run_admitted(request)
    assert result.disposition == ("unknown" if over else "completed")


def test_only_unused_diff_notification_is_suppressed():
    from ahc.adapters.codex_protocol import initialize_message
    options = initialize_message()["params"]["capabilities"]
    assert options["optOutNotificationMethods"] == ["turn/diff/updated"]


@pytest.mark.parametrize("over", [False, True])
def test_streamed_answer_item_budget_at_boundary(monkeypatch, over):
    from ahc.adapters import codex_final_messages as messages
    monkeypatch.setattr(messages, "MAX_FINAL_ITEMS", 1)
    peer, client, request = setup()
    event = streamed(peer)
    if over:
        second = deepcopy(event)
        second["params"]["item"]["id"] = "second"
        peer.messages.insert(-1, second)
    result = client.run_admitted(request)
    assert result.disposition == ("unknown" if over else "completed")
