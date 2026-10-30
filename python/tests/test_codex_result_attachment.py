"""Actual dynamic-tool/client acceptance path with a controlled protocol peer."""

from dataclasses import replace
from types import SimpleNamespace

import pytest
from test_codex_client import Peer
from test_result_artifact_commit import prepared

from ahc.adapters.codex_client import CodexProtocolClient
from ahc.application.runtime_service import RuntimeService
from ahc.application.workspace_tools import WorkspaceTools
from ahc.infrastructure.artifact_repository import ArtifactRepository
from ahc.infrastructure.channel_attachment_outbox import ChannelAttachmentOutbox


@pytest.mark.parametrize("fault", [None, "missing", "stale", "cleanup"])
def test_explicit_attachment_tool_reaches_accepted_result_only(tmp_path, fault):
    repo, snapshot, request, route, workspace, observation = prepared(tmp_path)
    try:
        peer = Peer()
        tools = WorkspaceTools(workspace, artifacts=ArtifactRepository(repo))
        client = CodexProtocolClient(peer, cwd="workspace", model="fixture",
                                     clock=lambda: 0, workspace_tools=tools)
        request = replace(request, configuration_digest=client.configuration_digest,
                          workspace_scope="work", capabilities=tools.capabilities(), deadline=100,
                          artifact_digest=client.executable_sha256)
        expected = workspace.identity.to_dict()
        if fault == "stale":
            expected["revision"] += 1
        peer.messages.insert(3, {"id": 7, "method": "item/tool/call", "params": {
            "threadId": "thread", "turnId": "turn", "namespace": "workspace",
            "tool": "attach", "callId": "select-one", "arguments": {
                "expected": expected, "artifact_id": "0" * 64 if fault == "missing" else
                observation.artifacts[0].artifact_id, "filename": "report.txt"}}})
        peer.fail_close = fault == "cleanup"
        result = RuntimeService(repo, SimpleNamespace(invoke=client.run_admitted)).execute(
            snapshot, request)
        tool_reply = next(item for item in peer.sent if item.get("id") == 7)
        assert tool_reply["result"]["success"] is (fault not in {"missing", "stale"})
        rows = ChannelAttachmentOutbox(repo).read(route)
        if fault in (None, "cleanup"):
            assert result.result["artifacts"] == [observation.artifacts[0].to_dict()]
            assert len(rows) == 1 and rows[0]["state"] == "pending"
            if fault == "cleanup":
                assert result.result["accepted"] and result.result["cleanup_pending"]
        else:
            assert "artifacts" not in result.result and rows == ()
            assert result.result["accepted"] is (fault != "cleanup")
        declaration = peer.sent[2]["params"]["dynamicTools"]
        assert any(tool["name"] == "attach" for tool in declaration[0]["tools"])
    finally:
        workspace.close()
