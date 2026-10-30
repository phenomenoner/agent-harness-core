"""Progress tool failures remain explicit without rejecting a completed model turn."""

import json
import time
from dataclasses import replace
from types import SimpleNamespace

import pytest
from test_codex_client import Peer
from test_result_artifact_commit import prepared

from ahc.adapters.codex_client import CodexProtocolClient
from ahc.application.runtime_service import RuntimeService
from ahc.application.workspace_tools import WorkspaceTools
from ahc.cli.foreground_progress import ForegroundProgress


@pytest.mark.parametrize("failure", [False, True])
def test_progress_tool_persists_outcome_and_allows_final(tmp_path, failure):
    repo, snapshot, request, route, workspace, _ = prepared(tmp_path)
    calls = []
    try:
        class Transport:
            def send(self, bound, payload, delivery_id):
                assert bound == route and payload == {"output_text": "Working on report"}
                calls.append(delivery_id)
                if failure:
                    raise TimeoutError("private-progress-fixture")
                return "progress-receipt"

        progress = ForegroundProgress(repo, {route: Transport()})
        tools = WorkspaceTools(workspace, progress=progress)
        peer = Peer()
        client = CodexProtocolClient(peer, cwd="workspace", model="fixture",
                                     clock=lambda: 0, workspace_tools=tools)
        request = replace(request, configuration_digest=client.configuration_digest,
            workspace_scope="work", capabilities=tools.capabilities(),
            deadline=time.monotonic() + 100, artifact_digest=client.executable_sha256)
        peer.messages.insert(3, {"id": 7, "method": "item/tool/call", "params": {
            "threadId": "thread", "turnId": "turn", "namespace": "workspace",
            "tool": "progress", "callId": "progress-tool", "arguments": {
                "expected": workspace.identity.to_dict(), "call_id": "progress-one",
                "text": "Working on report"}}})
        result = RuntimeService(repo, SimpleNamespace(invoke=client.run_admitted)).execute(
            snapshot, request)
        assert result.result["accepted"] is True and result.result["output_text"] == "answer"
        reply = next(item for item in peer.sent if item.get("id") == 7)["result"]
        assert reply["success"] is (not failure)
        value = json.loads(reply["contentItems"][0]["text"])
        assert value["result"]["status"] == ("unknown" if failure else "sent")
        assert "private-progress" not in str(reply)
        assert len(calls) == 1
        late = tools.invoke("workspace.progress", {"expected": workspace.identity.to_dict(),
                            "call_id": "late", "text": "too late"})
        assert late["disposition"] == "rejected" and len(calls) == 1
        with repo._read() as connection:
            rows = connection.execute("SELECT state FROM runtime_channel_progress").fetchall()
            assert len(rows) == 1 and rows[0]["state"] == ("unknown" if failure else "sent")
    finally:
        workspace.close()
