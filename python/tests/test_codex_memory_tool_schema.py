from dataclasses import replace

import pytest
from test_codex_workspace_tools import bridge

from ahc.application.workspace_memory_search import search


@pytest.mark.parametrize(
    "operation,optional",
    [("memory_pack", {"budget_tokens", "max_items"}), ("memory_search", {"limit"})],
)
def test_memory_schema_matches_runtime_query_contract(operation, optional):
    adapter, tools = bridge()
    capability = "workspace." + operation
    adapter.request = replace(adapter.request, capabilities=(capability,))
    tools.capabilities = lambda: (capability,)
    schema = adapter.declarations()[0]["tools"][0]["inputSchema"]
    assert set(schema["properties"]) == {"expected", "query"} | optional
    assert schema["required"] == ["expected", "query"]
    assert schema["properties"]["query"] == {"type": "string", "minLength": 1, "maxLength": 4096}
    assert "names" not in schema["properties"]


def test_declared_minimal_search_arguments_reach_real_runtime_wrapper():
    adapter, tools = bridge()
    adapter.request = replace(adapter.request, capabilities=("workspace.memory_search",))
    tools.capabilities = lambda: ("workspace.memory_search",)
    declaration = adapter.declarations()[0]["tools"][0]
    args = {"expected": tools.workspace.identity.to_dict(), "query": "orchid"}
    assert set(args) == set(declaration["inputSchema"]["required"])

    class Service:
        def search(self, session, query, *, limit):
            assert session == tools.workspace.identity.session
            assert query == "orchid" and limit == 8
            return {"records": []}

    result = search(tools.workspace, Service(), args)
    assert result["disposition"] == "observed"


def test_native_status_documented_example_reaches_runtime():
    import json
    from types import SimpleNamespace

    from ahc.application.workspace_native_memory import WorkspaceNativeMemory

    adapter, tools = bridge()
    adapter.request = replace(adapter.request, capabilities=("workspace.memory_native",))
    description = adapter.declarations()[0]["tools"][0]["description"]
    argument_text = description.split("exact arguments: ", 1)[1]
    arguments, _ = json.JSONDecoder().raw_decode(argument_text)
    observed = []

    def status(session):
        observed.append(session)
        return {"status": "ready"}

    native = WorkspaceNativeMemory(SimpleNamespace(status=status), None)
    result = native.invoke(tools.workspace, arguments)
    assert result["disposition"] == "observed"
    assert observed == [tools.workspace.identity.session]


@pytest.mark.parametrize("enabled", [False, True])
def test_revision_actions_are_declared_only_for_composed_service(enabled):
    from ahc.application.workspace_native_memory import WorkspaceNativeMemory

    adapter, tools = bridge()
    adapter.request = replace(adapter.request, capabilities=("workspace.memory_native",))
    tools.native_memory = WorkspaceNativeMemory(None, None, revisions=object() if enabled else None)
    declaration = adapter.declarations()[0]["tools"][0]["inputSchema"]["properties"]
    actions = declaration["action"]["enum"]
    fields = declaration["payload"]["properties"]
    assert ("revise" in actions) is enabled
    assert ("tombstone" in actions) is enabled
    assert ("reconcile_revision" in actions) is enabled
    assert ("expected_ref" in fields) is enabled
    assert ("revision_digest" in fields) is enabled
