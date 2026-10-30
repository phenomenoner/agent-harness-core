"""Bounded advisory warnings cannot authorize effects or finish a turn."""
import json

import pytest
from test_codex_client import setup


@pytest.mark.parametrize("thread", ["thread", None])
def test_warning_status_preserves_full_completed_turn_and_redacts_payload(thread):
    peer, client, request = setup()
    peer.messages.insert(3, {
        "method": "warning", "params": {"threadId": thread, "message": "PRIVATE/SECRET"},
        "emittedAtMs": 1,
    })
    result = client.run_admitted(request)
    assert result.disposition == "completed" and result.output_text == "answer"
    assert result.backend_diagnostic["last_method"] == "warning"
    assert result.backend_diagnostic["code"] == "advisory-metadata-ignored"
    assert "PRIVATE" not in json.dumps(result.public_dict())
    assert sum(m.get("method") == "turn/start" for m in peer.sent) == 1


@pytest.mark.parametrize("invalid", [
    "unbound-thread", "missing-thread", "empty-message", "oversized", "surrogate",
    "message-type", "permission-field", "request", "timestamp",
])
def test_warning_payloads_are_discarded_but_rpc_requests_remain_strict(invalid):
    peer, client, request = setup()
    message = {"method": "warning", "params": {"threadId": "thread", "message": "notice"}}
    if invalid == "unbound-thread":
        message["params"]["threadId"] = "other"
    elif invalid == "missing-thread":
        del message["params"]["threadId"]
    elif invalid == "empty-message":
        message["params"]["message"] = ""
    elif invalid == "oversized":
        message["params"]["message"] = "a" * 4097
    elif invalid == "surrogate":
        message["params"]["message"] = "\ud800"
    elif invalid == "message-type":
        message["params"]["message"] = True
    elif invalid == "permission-field":
        message["params"]["approved"] = True
    elif invalid == "request":
        message["id"] = 90
    else:
        message["emittedAtMs"] = True
    peer.messages.insert(3, message)
    result = client.run_admitted(request)
    if invalid == "request":
        assert result.disposition == "unknown" and result.unresolved_effects
        assert result.output_text == ""
    else:
        assert result.disposition == "completed" and result.output_text == "answer"
        assert not result.unresolved_effects
        assert "notice" not in json.dumps(result.public_dict())


def test_warning_alone_cannot_complete_and_parent_deadline_stops_flood():
    peer, client, request = setup()
    warning = {"method": "warning", "params": {"threadId": "thread", "message": "notice"}}
    peer.messages[3:] = [warning] * 5000
    original_receive = peer.receive

    def receive(*, deadline):
        message = original_receive(deadline=deadline)
        if client.frames >= 25:
            client.clock = lambda: request.deadline
        return message

    peer.receive = receive
    result = client.run_admitted(request)
    assert result.disposition == "unknown" and result.unresolved_effects
    assert result.output_text == "" and peer.closed
    assert client.frames == 25


def test_warning_cannot_mask_policy_drift():
    peer, client, request = setup()
    settings = dict(peer.messages[1]["result"])
    settings.pop("thread")
    settings["sandboxPolicy"] = {"type": "dangerFullAccess"}
    settings.pop("sandbox")
    peer.messages[3:3] = [
        {"method": "warning", "params": {"threadId": "thread", "message": "notice"}},
        {"method": "thread/settings/updated", "params": {
            "threadId": "thread", "threadSettings": settings,
        }},
    ]
    assert client.run_admitted(request).disposition == "unknown"
