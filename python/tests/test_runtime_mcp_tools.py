import os
import sys
from dataclasses import replace

import pytest
from test_foreground_runtime import setup
from test_mcp_stdio import SERVER

from ahc.adapters.codex_workspace_tools import CodexWorkspaceTools
from ahc.domain.agent_backend import AgentTurnObservation
from ahc.domain.mcp_profiles import McpStdioProfile
from ahc.domain.skill_values import digest_json
from ahc.infrastructure.mcp_receipts import read_mcp_receipts
from ahc.infrastructure.runtime_mcp_tools import RuntimeMcpTools


@pytest.mark.skipif(os.name != "nt", reason="Windows owned stdio")
def test_foreground_mcp_scope_real_server_and_duplicate(tmp_path):
    pytest.importorskip("jsonschema")
    runtime, requests, _, request = setup(tmp_path)
    env = {k: os.environ[k] for k in ("SystemRoot", "WINDIR") if k in os.environ}
    digest = digest_json({"server_id": "fixture", "protocol": "2025-11-25",
        "tools": [{"name": "echo", "inputSchema": {"type": "object"}}]})
    profile = McpStdioProfile("fixture", request.session.agent_id,
        (sys.executable, "-u", "-c", SERVER, "echo"), str(tmp_path), tuple(env),
        ("echo",), digest, True)
    runtime.mcp = RuntimeMcpTools(runtime.store, (profile,), environments={"fixture": env})
    observed = []

    def factory(bound, tools, *, cancelled):
        class Backend:
            def invoke(self, bound):
                arguments = {"expected": tools.workspace.identity.to_dict(),
                    "server_id": "fixture", "tool": "echo", "call_id": "mcp-call",
                    "arguments": {"text": "through-foreground"}}
                observed.append(tools.invoke("workspace.mcp", arguments))
                observed.append(tools.invoke("workspace.mcp", arguments))
                return AgentTurnObservation(bound, "completed", "fixture", 1, 1,
                                            "thread", "turn", "done")
        return Backend()

    runtime.backend_factory = factory
    runtime.start()
    try:
        request = replace(request, capabilities=tuple(runtime.tools.capabilities()))
        declarations = CodexWorkspaceTools.describe(request, runtime.tools)
        assert '"call_id"' in __import__("json").dumps(declarations)
        requests.append(request)
        assert runtime.step().result["accepted"]
        assert observed[0]["disposition"] == "observed"
        assert observed[1]["disposition"] == "rejected"
        rows = read_mcp_receipts(runtime.store, request.session, request.operation_id)
        assert len(rows) == 1 and rows[0]["status"] == "observed"
        outside = runtime.tools.invoke("workspace.mcp", {
            "expected": runtime.workspace.identity.to_dict(), "server_id": "fixture",
            "tool": "echo", "call_id": "outside", "arguments": {}})
        assert outside["disposition"] == "rejected"
    finally:
        runtime.close()
