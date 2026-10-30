"""Native terminal interaction is progress, never a final or a new tool request."""
import json

import pytest
from test_codex_client import setup

from ahc.adapters.codex_protocol import CodexTurnDecoder
from ahc.domain.runtime_turn import accept_observation
from ahc.domain.sessions import ContractError


def notification(**changes):
    return {"method": "item/commandExecution/terminalInteraction", "params": {
        "threadId": "thread", "turnId": "turn", "itemId": "call-1",
        "processId": "15430", "stdin": "", **changes,
    }}


@pytest.mark.parametrize("stdin", ["", "PRIVATE-STDIN\n", "\x03"])
def test_valid_interaction_allows_exact_final_without_retaining_input(stdin):
    peer, client, request = setup()
    peer.messages.insert(3, notification(stdin=stdin))
    result = accept_observation(request, client.run_admitted(request))
    assert result["accepted"] and result["output_text"] == "answer"
    assert "PRIVATE-STDIN" not in json.dumps(result)
    assert not any(m.get("method") == "turn/interrupt" for m in peer.sent)
    assert sum(m.get("method") == "turn/start" for m in peer.sent) == 1


def test_interaction_alone_never_completes_turn():
    decoder = CodexTurnDecoder("thread", "turn")
    assert decoder.receive(notification()) is None
    assert decoder.terminal is False


@pytest.mark.parametrize("changes", [
    {"threadId": "foreign"}, {"turnId": "foreign"}, {"itemId": ""},
    {"processId": ""}, {"itemId": 123}, {"stdin": None}, {"processId": []},
    {"stdin": "\ud800"}, {"processId": "x" * 129}, {"stdin": "x" * (4 * 1048576 + 1)},
])
def test_foreign_or_malformed_interaction_is_rejected(changes):
    with pytest.raises(ContractError):
        CodexTurnDecoder("thread", "turn").receive(notification(**changes))


@pytest.mark.parametrize("field", ["threadId", "turnId", "itemId", "processId", "stdin"])
def test_required_fields_remain_required(field):
    message = notification()
    del message["params"][field]
    with pytest.raises(ContractError):
        CodexTurnDecoder("thread", "turn").receive(message)


def test_request_with_same_name_is_not_a_progress_notification():
    message = notification()
    message["id"] = 7
    with pytest.raises(ContractError):
        CodexTurnDecoder("thread", "turn").receive(message)
