"""Pinned, decoder-owned ``turn/steer`` wire messages.

The caller owns durable intent and reply reconciliation.  This module only
validates the exact 0.153.4 request/response shapes, so it never learns or
consumes an unrelated app-server frame.
"""

from ahc.domain.sessions import _identifier
from ahc.domain.turns import _input_text


def turn_steer_message(request_id, thread_id, expected_turn_id, text):
    """Build the schema-pinned request for one active Codex turn."""
    _identifier(request_id, "Codex steer request")
    _identifier(thread_id, "Codex steer thread")
    _identifier(expected_turn_id, "Codex steer expected turn")
    _input_text(text, "Codex steer text")
    return {
        "id": request_id,
        "method": "turn/steer",
        "params": {
            "threadId": thread_id,
            "expectedTurnId": expected_turn_id,
            "input": [{"type": "text", "text": text}],
        },
    }


def accepted_turn_steer_response(message, request_id, expected_turn_id):
    """Return whether *message* is the exact successful steer acknowledgement."""
    _identifier(request_id, "Codex steer request")
    _identifier(expected_turn_id, "Codex steer expected turn")
    return (
        type(message) is dict
        and set(message) == {"id", "result"}
        and message.get("id") == request_id
        and type(message["result"]) is dict
        and set(message["result"]) == {"turnId"}
        and message["result"].get("turnId") == expected_turn_id
    )


def rejected_turn_steer_response(message, request_id):
    """Recognize a correlated JSON-RPC rejection without interpreting vendor text.

    A rejection is a confirmed non-acceptance of this request.  Its error
    payload remains deliberately opaque, because it is not durable evidence.
    """
    _identifier(request_id, "Codex steer request")
    return (
        type(message) is dict
        and set(message) == {"id", "error"}
        and message.get("id") == request_id
        and type(message["error"]) is dict
        and type(message["error"].get("code")) is int
        and type(message["error"].get("message")) is str
    )
