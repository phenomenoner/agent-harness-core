"""Pinned dynamic-tool framing only; no provider or experimental client launch."""

import json
from dataclasses import replace

import pytest
from test_codex_client import setup

from ahc.adapters.codex_workspace_tools import CodexWorkspaceTools
from ahc.domain.sessions import ContractError
from ahc.domain.workspace import WorkspaceIdentity


def bridge():
    peer, client, request = setup()
    request = replace(request, capabilities=("workspace.execute",))

    class Tools:
        calls = 0

        class workspace:
            identity = WorkspaceIdentity(request.session, "work")

        def capabilities(self):
            return ("workspace.execute",)

        def invoke(self, name, args):
            self.calls += 1
            return {"disposition": "observed", "result": {"ok": True, "output": "42"}}

    tools = Tools()
    return CodexWorkspaceTools(request, tools, "thread", "turn", clock=lambda: 0), tools


def message():
    return {
        "id": 7,
        "method": "item/tool/call",
        "params": {
            "threadId": "thread",
            "turnId": "turn",
            "namespace": "workspace",
            "tool": "execute",
            "callId": "call",
            "arguments": {},
        },
    }


def test_namespace_declaration_response_and_no_replay():
    adapter, tools = bridge()
    (namespace,) = adapter.declarations()
    assert namespace["type"] == "namespace" and namespace["name"] == "workspace"
    assert namespace["tools"][0]["type"] == "function"
    response = adapter.respond(message())
    assert response["result"]["success"] is True
    assert json.loads(response["result"]["contentItems"][0]["text"])["result"]["output"] == "42"
    with pytest.raises(ContractError):
        adapter.respond(message())
    assert tools.calls == 1


def test_unresolved_tool_effect_survives_later_observation():
    adapter, tools = bridge()
    replies = iter([{"disposition": "unknown", "unresolved_effects": True},
                    {"disposition": "observed", "unresolved_effects": False}])
    tools.invoke = lambda *_: next(replies)
    adapter.respond(message())
    following = message()
    following["id"] = 8
    following["params"]["callId"] = "second"
    adapter.respond(following)
    assert adapter.unresolved is True


@pytest.mark.parametrize("field", ["threadId", "turnId", "namespace", "tool"])
def test_foreign_binding_never_invokes(field):
    adapter, tools = bridge()
    request = message()
    request["params"][field] = "foreign"
    with pytest.raises(ContractError):
        adapter.respond(request)
    assert tools.calls == 0


def test_expired_call_is_not_invoked():
    adapter, tools = bridge()
    adapter.clock = lambda: 100
    with pytest.raises(ContractError):
        adapter.respond(message())
    assert tools.calls == 0


def test_deadline_after_tool_return_preserves_the_actual_observation():
    adapter, tools = bridge()
    now = [0]
    adapter.clock = lambda: now[0]
    invoke = tools.invoke
    def at_deadline(*args):
        result = invoke(*args)
        now[0] = adapter.request.deadline
        return result
    tools.invoke = at_deadline
    reply = adapter.respond(message())
    payload = json.loads(reply["result"]["contentItems"][0]["text"])
    assert reply["result"]["success"] and payload["disposition"] == "observed"
    assert payload["tool_budget"]["remaining_ms"] == 0 and not adapter.unresolved
    assert tools.calls == 1


def test_authorization_wait_cannot_start_a_tool_after_the_original_deadline():
    from types import SimpleNamespace

    adapter, tools = bridge()
    now = [0]
    adapter.clock = lambda: now[0]
    def authorize(*args):
        now[0] = adapter.request.deadline
        return True
    adapter.gate = SimpleNamespace(authorize=authorize)
    reply = adapter.respond(message())
    payload = json.loads(reply["result"]["contentItems"][0]["text"])
    assert payload["reason"] == "tool-deadline-before-execution" and tools.calls == 0
    assert not payload["unresolved_effects"] and not adapter.unresolved


@pytest.mark.parametrize("capability", ["workspace.child", "workspace.child_batch"])
def test_child_timeout_schema_preserves_owner_duration(capability):
    adapter, tools = bridge()
    request = replace(adapter.request, capabilities=(capability,))
    declaration = CodexWorkspaceTools.describe(request, tools)[0]["tools"][0]
    properties = declaration["inputSchema"]["properties"]
    timeout = (properties["timeout_ms"] if capability.endswith(".child") else
               properties["children"]["items"]["properties"]["timeout_ms"])
    assert timeout == {"type": "integer", "minimum": 1}


@pytest.mark.parametrize("capability,field", [
    ("workspace.artifact", "lineage"), ("workspace.inspect", "names")])
def test_array_declarations_use_the_provider_supported_schema(capability, field):
    adapter, tools = bridge()
    request = replace(adapter.request, capabilities=(capability,))
    declaration = CodexWorkspaceTools.describe(request, tools)[0]["tools"][0]
    assert "uniqueItems" not in declaration["inputSchema"]["properties"][field]


def test_duplicate_artifact_lineage_rejects_before_capture():
    from ahc.application.workspace_artifact_tools import capture

    adapter, tools = bridge()
    tools.workspace.capture_artifact = lambda *_: pytest.fail("captured rejected lineage")
    result = capture(tools.workspace, None, {
        "expected": tools.workspace.identity.to_dict(), "name": "report",
        "media_type": "text/plain", "lineage": ["a" * 64, "a" * 64]})
    assert result["disposition"] == "rejected"


def test_bound_workspace_constants_have_explicit_provider_types():
    adapter, _ = bridge()
    expected = adapter.declarations()[0]["tools"][0]["inputSchema"]["properties"]["expected"]
    assert expected["properties"]["workspace_id"]["type"] == "string"
    assert expected["properties"]["generation"]["type"] == "integer"


def test_exact_session_schema_has_no_composite_constants_after_vendor_lowering():
    adapter, tools = bridge()
    expected = adapter.declarations()[0]["tools"][0]["inputSchema"]["properties"]["expected"]

    def exact_value(schema):
        if schema["type"] == "object":
            # The pinned vendor lowers const to enum. A composite enum is
            # rejected by the provider; required exact leaves express it fully.
            assert "const" not in schema and "enum" not in schema
            assert schema["additionalProperties"] is False
            assert set(schema["required"]) == set(schema["properties"])
            return {key: exact_value(value) for key, value in schema["properties"].items()}
        assert type(schema["const"]) in (str, int, type(None))
        return schema["const"]

    session = tools.workspace.identity.session.to_dict()
    assert exact_value(expected["properties"]["session"]) == session


def test_more_than_sixteen_calls_can_finish_within_the_task_deadline():
    adapter, tools = bridge()
    for ordinal in range(1, 26):
        frame = message()
        frame["id"] = ordinal
        frame["params"]["callId"] = str(ordinal)
        reply = json.loads(adapter.respond(frame)["result"]["contentItems"][0]["text"])
        assert reply["tool_budget"] == {"calls": ordinal, "remaining_ms": 100000}
    assert tools.calls == 25


def test_valid_tool_result_over_legacy_bound_reaches_the_model_once():
    adapter, tools = bridge()
    # A Unicode environment/recall result can exceed the old 32 KiB JSON cap
    # while remaining comfortably inside the existing stdio frame budget.
    output = "資料" * 7000
    tools.invoke = lambda *_: {"disposition": "observed", "unresolved_effects": False,
                               "result": {"guide": output}}
    response = adapter.respond(message())
    assert response["result"]["success"] is True
    assert json.loads(response["result"]["contentItems"][0]["text"])["result"]["guide"] == output
    from ahc.adapters.codex_transport import CodexStdioTransport
    assert len(json.dumps(response).encode()) + 1 < CodexStdioTransport.MAX_OUTBOUND_FRAME
    with pytest.raises(ContractError):
        adapter.respond(message())
