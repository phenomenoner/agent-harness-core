"""Version-bound Codex wire and terminal acceptance regressions; no backend process."""

from copy import deepcopy
from types import SimpleNamespace

import pytest

from ahc.adapters.codex_protocol import (
    CodexTurnDecoder,
    initialize_message,
    interrupt_message,
    thread_message,
    turn_message,
    validate_initialize_response,
)
from ahc.domain.sessions import ContractError


def terminal(**changes):
    turn = {
        "id": "turn",
        "status": "completed",
        "error": None,
        "items": [
            {"type": "agentMessage", "id": "item", "phase": "final_answer", "text": "answer"}
        ],
    }
    turn.update(changes)
    return {"method": "turn/completed", "params": {"threadId": "thread", "turn": turn}}


def test_vendor_messages_do_not_send_inert_dialect_fields():
    assert initialize_message()["params"]["clientInfo"]["name"] == "ahc"
    start = thread_message(cwd="workspace", model="selected-model")
    assert start["params"]["sandbox"] == "read-only" and start["params"]["ephemeral"] is False
    resume = thread_message(cwd="workspace", model="selected-model", backend_thread="thread")
    assert resume["method"] == "thread/resume" and "ephemeral" not in resume["params"]
    turn = turn_message(SimpleNamespace(input_text="question"), "thread", cwd="workspace")
    assert turn["params"]["input"] == [{"type": "text", "text": "question"}]
    assert turn["params"]["sandboxPolicy"] == {"type": "readOnly", "networkAccess": False}
    assert set(turn["params"]) == {"threadId", "cwd", "approvalPolicy", "sandboxPolicy", "input"}
    assert interrupt_message("thread", "turn")["params"] == {"threadId": "thread", "turnId": "turn"}


def test_initialize_projects_no_private_home_or_user_agent():
    payload = {
        "id": 1,
        "result": {
            "codexHome": "PRIVATE-SENTINEL",
            "platformFamily": "windows",
            "platformOs": "windows",
            "userAgent": "PRIVATE-SENTINEL",
        },
    }
    result = validate_initialize_response(payload)
    assert "PRIVATE-SENTINEL" not in str(result)
    payload["id"] = True
    with pytest.raises(ContractError):
        validate_initialize_response(payload)


def test_deltas_and_item_completion_do_not_complete_turn():
    decoder = CodexTurnDecoder("thread", "turn")
    for method in ("item/agentMessage/delta", "item/completed"):
        assert (
            decoder.receive(
                {
                    "method": method,
                    "params": {
                        "threadId": "thread",
                        "turnId": "turn",
                        "delta": "not final",
                    },
                }
            )
            is None
        )
    assert decoder.receive(terminal())["output_text"] == "answer"
    with pytest.raises(ContractError, match="terminal"):
        decoder.receive(terminal())


@pytest.mark.parametrize("kind", ["thread", "turn", "partial", "duplicate", "server-request"])
def test_wrong_binding_or_incomplete_evidence_cannot_complete(kind):
    event = terminal()
    if kind == "thread":
        event["params"]["threadId"] = "other"
    elif kind == "turn":
        event["params"]["turn"]["id"] = "other"
    elif kind == "partial":
        event["params"]["turn"]["itemsView"] = "summary"
    elif kind == "duplicate":
        event["params"]["turn"]["items"] *= 2
    else:
        event = {"id": 9, "method": "account/chatgptAuthTokens/refresh", "params": {}}
    with pytest.raises(ContractError):
        CodexTurnDecoder("thread", "turn").receive(event)


def test_terminal_excludes_commentary_and_async_questions():
    event = terminal()
    items = event["params"]["turn"]["items"]
    comment = {**deepcopy(items[0]), "id": "comment", "phase": "commentary", "text": "working"}
    question = {**deepcopy(items[0]), "id": "question", "delivery": "async", "text": "question"}
    items[:0] = [comment, question]
    assert CodexTurnDecoder("thread", "turn").receive(event)["output_text"] == "answer"


@pytest.mark.parametrize("status", ["failed", "interrupted"])
def test_non_success_is_unknown(status):
    result = CodexTurnDecoder("thread", "turn").receive(terminal(status=status))
    assert result == {"disposition": "unknown", "output_text": "", "unresolved_effects": True}


@pytest.mark.parametrize("backend_thread", [None, "thread"])
def test_named_profile_never_emits_legacy_policy(backend_thread):
    start = thread_message(
        cwd="workspace", model="fixture", backend_thread=backend_thread,
        permissions="ahc_offline",
    )
    assert start["params"]["permissions"] == "ahc_offline"
    assert "sandbox" not in start["params"]
    turn = turn_message(
        SimpleNamespace(input_text="question"), "thread", cwd="workspace",
        permissions="ahc_offline",
    )
    assert turn["params"]["permissions"] == "ahc_offline"
    assert "sandboxPolicy" not in turn["params"]
    assert turn["params"]["approvalPolicy"] == "never"


@pytest.mark.parametrize("profile", ["", ":danger-full-access", "a\n", True, {}, "a" * 129])
def test_named_profile_invalid_selector_rejected(profile):
    with pytest.raises(ContractError):
        thread_message(cwd="workspace", model="fixture", permissions=profile)
    with pytest.raises(ContractError):
        turn_message(SimpleNamespace(input_text="question"), "thread", cwd="workspace",
                     permissions=profile)


def test_command_output_delta_is_not_terminal_or_final_output():
    decoder = CodexTurnDecoder("thread", "turn")
    event = {"method": "item/commandExecution/outputDelta", "params": {
        "threadId": "thread", "turnId": "turn", "itemId": "command",
        "delta": "untrusted command output"}}
    assert decoder.receive(event) is None
    assert decoder.receive(terminal())["output_text"] == "answer"


@pytest.mark.parametrize("field,value", [
    ("threadId", "foreign"), ("turnId", "foreign"), ("itemId", None), ("delta", 42)])
def test_command_output_delta_rejects_wrong_binding_or_shape(field, value):
    event = {"method": "item/commandExecution/outputDelta", "params": {
        "threadId": "thread", "turnId": "turn", "itemId": "command", "delta": "text"}}
    event["params"][field] = value
    with pytest.raises(ContractError):
        CodexTurnDecoder("thread", "turn").receive(event)
