from dataclasses import replace

import pytest
from test_codex_workspace_tools import bridge, message

from ahc.domain.sessions import ContractError


def test_nonreserved_mcp_wire_name_maps_to_existing_capability_once():
    adapter, tools = bridge()
    adapter.request = replace(adapter.request, capabilities=("workspace.mcp",))
    declaration = adapter.declarations()[0]["tools"][0]
    assert declaration["name"] == "call_mcp"
    observed = []
    tools.invoke = lambda name, args: (observed.append(name) or {"disposition": "observed"})
    event = message()
    event["params"]["tool"] = "call_mcp"
    assert adapter.respond(event)["result"]["success"]
    assert observed == ["workspace.mcp"]
    with pytest.raises(ContractError):
        adapter.respond(event)
    assert observed == ["workspace.mcp"]


@pytest.mark.parametrize("name,capabilities", [
    ("mcp", ("workspace.mcp",)), ("call_mcp", ("workspace.execute",))])
def test_wire_alias_cannot_bypass_admitted_capability(name, capabilities):
    adapter, tools = bridge()
    adapter.request = replace(adapter.request, capabilities=capabilities)
    event = message()
    event["params"]["tool"] = name
    with pytest.raises(ContractError):
        adapter.respond(event)
    assert tools.calls == 0
