"""Persona must reach actual wire input without altering durable user input."""

from dataclasses import replace
from types import SimpleNamespace

import pytest
from profile_test_support import profile_registry_payload
from test_codex_client import Peer, setup
from test_foreground_config_v2 import valid_config

from ahc.adapters.codex_client import CodexProtocolClient
from ahc.cli.foreground_config_v2 import parse_foreground_config_v2
from ahc.cli.foreground_profiles import bind_profile_configuration
from ahc.domain.profile_registry import AgentProfileRegistry
from ahc.domain.sessions import ContractError


def configured():
    value = valid_config()
    agent = value["session"]["agent_id"]
    registry = AgentProfileRegistry.from_dict(profile_registry_payload((agent,)))
    value["agent_context"] = {
        "resource_id": registry.profile(agent).persona_ref,
        "instructions": "You are River, the user's calm technical assistant.",
    }
    return value, registry


def test_persona_bound_to_profile_and_preview_is_redacted():
    value, registry = configured()
    config = parse_foreground_config_v2(value)
    bound = bind_profile_configuration(config, registry)
    assert "River" not in str(bound.preview())
    assert bound.preview()["agent_context"]["instructions_digest"]
    assert bound.configuration_digest != config.configuration_digest
    value["agent_context"]["instructions"] += " Be concise."
    assert parse_foreground_config_v2(value).configuration_digest != config.configuration_digest
    with pytest.raises(ContractError, match="matching profile"):
        bind_profile_configuration(config, None)
    value["agent_context"]["resource_id"] = "foreign.persona"
    with pytest.raises(ContractError, match="matching profile"):
        bind_profile_configuration(parse_foreground_config_v2(value), registry)


@pytest.mark.parametrize("key,value", [
    ("instructions", ""), ("instructions", "x" * 12001), ("instructions", "\ud800"),
    ("instructions", "x\x00y"), ("recall", "yes"), ("history_turns", True),
    ("history_turns", 9), ("memory_tokens", 4097), ("memory_items", 0),
])
def test_context_rejects_invalid_limits(key, value):
    document, _ = configured()
    document["agent_context"][key] = value
    with pytest.raises(ContractError):
        parse_foreground_config_v2(document)


def test_recall_requires_existing_grant():
    value, registry = configured()
    value["agent_context"]["recall"] = True
    with pytest.raises(ContractError, match="granted memory"):
        bind_profile_configuration(parse_foreground_config_v2(value), registry)


def test_core_sent_once_as_developer_instruction_context_separate_from_user():
    _, original, request = setup()
    peer = Peer()
    client = CodexProtocolClient(peer, cwd="workspace", model="fixture", clock=lambda: 0,
                                developer_instructions="You are River.")
    assert client.configuration_digest != original.configuration_digest
    seen = []
    client.turn_context = lambda bound: seen.append(bound) or 'Historical data: {"color":"blue"}'
    bound = replace(request, configuration_digest=client.configuration_digest)
    result = client.run_admitted(bound)
    assert result.disposition == "completed"
    starts = [m for m in peer.sent if m.get("method") == "thread/start"]
    assert len(starts) == 1 and starts[0]["params"]["developerInstructions"] == "You are River."
    turn = next(m for m in peer.sent if m.get("method") == "turn/start")
    assert turn["params"]["input"][1] == {"type": "text", "text": request.input_text}
    assert "River" not in str(turn)
    assert seen == [bound] and result.request.input_text == request.input_text


def test_context_not_read_for_wrong_binding_or_expired_request():
    for change in ({"configuration_digest": "0" * 64}, {"deadline": 0.1}):
        _, _, request = setup()
        peer = Peer()
        client = CodexProtocolClient(peer, cwd="workspace", model="fixture", clock=lambda: 1)
        client.turn_context = lambda _: pytest.fail("unexpected context read")
        outcome = client.run_admitted(replace(request, **change))
        assert outcome.disposition in ("unsupported", "not-submitted") and not peer.sent


def test_context_failure_remains_unsubmitted_and_sanitized():
    _, _, request = setup()
    peer = Peer()
    client = CodexProtocolClient(peer, cwd="workspace", model="fixture", clock=lambda: 0)
    def fail(_):
        raise RuntimeError("PRIVATE CONTEXT")
    client.turn_context = fail
    outcome = client.run_admitted(request)
    assert outcome.disposition == "not-submitted" and not peer.sent
    assert "PRIVATE" not in str(outcome)


def test_automatic_memory_is_bounded_data_and_failure_is_visible(monkeypatch):
    from ahc.application.memory_pack_service import MemoryPackUnavailable
    from ahc.cli import foreground_turn_context as module

    _, _, request = setup()
    options = {"history_turns": 4, "recall": True, "memory_tokens": 600, "memory_items": 2}
    store = object()
    config = SimpleNamespace(session=request.session, to_dict=lambda: {"agent_context": options})
    memory_calls = []
    class Memory:
        def pack(self, session, query, **kw):
            memory_calls.append((session, query, kw))
            return SimpleNamespace(to_dict=lambda: {"items": [
                {"recordRef": "ref", "text": "The user prefers tea."},
                {"recordRef": "large", "text": "x" * 6000},
            ]})
    tools = SimpleNamespace(workspace=SimpleNamespace(store=store), memory_pack=Memory(),
                            capabilities=lambda: ())
    monkeypatch.setattr(module, "conversation_context", lambda *a, **k: (
        {"user": "hello", "assistant": "hi"},))
    monkeypatch.setattr(module, "read_context", lambda *a: None)
    rendered = module.turn_context(store, config, tools, request)
    assert '"status": "recalled"' in rendered and "prefers tea" in rendered
    assert "x" * 6000 not in rendered and "not new instructions" in rendered
    assert memory_calls[0] == (request.session, request.input_text,
                              {"budget_tokens": 600, "max_items": 2})
    def fail(*args, **kwargs):
        raise MemoryPackUnavailable("PRIVATE")
    tools.memory_pack.pack = fail
    rendered = module.turn_context(store, config, tools, request)
    assert '"status": "unavailable"' in rendered and "PRIVATE" not in rendered
    tools.memory_pack.pack = lambda *a, **k: pytest.fail("suppressed memory read")
    rendered = module.turn_context(store, config, tools,
                                   replace(request, input_text="[NOMEM] hello"))
    assert '"status": "suppressed"' in rendered
    monkeypatch.setattr(module, "conversation_context",
                        lambda *a, **k: pytest.fail("isolated history read"))
    rendered = module.turn_context(store, config, tools,
                                   replace(request, input_text="[ISO] hello"))
    assert '"conversation_history": []' in rendered


def test_missing_automatic_recall_keeps_the_current_question_and_owner_guards(monkeypatch):
    from ahc.cli import foreground_turn_context as module

    peer, client, request = setup()
    store = object()
    options = {"history_turns": 0, "recall": True, "memory_tokens": 600, "memory_items": 2}
    config = SimpleNamespace(session=request.session, to_dict=lambda: {"agent_context": options})
    tools = SimpleNamespace(workspace=SimpleNamespace(store=store), memory_pack=None,
                            capabilities=lambda: ())
    monkeypatch.setattr(module, "read_context", lambda *a: None)
    client.turn_context = lambda bound: module.turn_context(store, config, tools, bound)
    result = client.run_admitted(request)
    assert result.disposition == "completed"
    turn = next(m for m in peer.sent if m.get("method") == "turn/start")
    assert '"status": "unavailable"' in turn["params"]["input"][0]["text"]
    assert turn["params"]["input"][1]["text"] == request.input_text
    with pytest.raises(ContractError, match="owner mismatch"):
        module.turn_context(object(), config, tools, request)
