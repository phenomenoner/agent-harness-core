"""Actual protocol client discovers and reads imported files during admitted turns."""

import base64
import json
from dataclasses import replace
from types import SimpleNamespace

from test_codex_client import Peer
from test_runtime_core import setup
from test_workspace_tools import Port

from ahc.adapters.codex_client import CodexProtocolClient
from ahc.application.runtime_service import RuntimeService
from ahc.application.workspace_tools import WorkspaceTools
from ahc.domain.channel_files import ChannelFile
from ahc.domain.channel_ingress import ChannelInboundEvent, ChannelRoute
from ahc.infrastructure.artifact_repository import ArtifactRepository
from ahc.infrastructure.channel_file_import import ChannelFileImport
from ahc.infrastructure.channel_file_inbox import ChannelFileInbox


def test_codex_discovers_then_reads_imported_file(tmp_path):
    repo, snapshot, request = setup(tmp_path)
    workspace = RuntimeService(repo, None).open_workspace(snapshot, "work", Port)
    try:
        route = ChannelRoute("telegram", "123", "456", "789", snapshot.session)
        event = ChannelInboundEvent(route, "update-1", "read report", (
            ChannelFile("telegram", "456", "2", "report.txt", 4, "text/plain"),))
        ChannelFileInbox(repo).stage(event, snapshot.goal.digest)
        importer = ChannelFileImport(repo)
        selections = importer.run(event, snapshot.goal.digest, workspace.identity,
                                  SimpleNamespace(fetch=lambda ref: b"data"))
        importer.submit(event, snapshot.goal.digest)
        tools = WorkspaceTools(workspace, artifacts=ArtifactRepository(repo))
        peer = Peer()
        client = CodexProtocolClient(peer, cwd="workspace", model="fixture",
                                     clock=lambda: 0, workspace_tools=tools)
        request = replace(request, operation_id=event.operation_id, input_text=event.input_text,
                          configuration_digest=client.configuration_digest, workspace_scope="work",
                          capabilities=tools.capabilities(),
                          artifact_digest=client.executable_sha256)
        expected = workspace.identity.to_dict()
        for identifier, name, arguments in reversed([
                (7, "incoming_artifacts", {"expected": expected}),
                (8, "read_artifact", {"expected": expected,
                    "artifact_id": selections[0]["artifact_id"], "offset": 0, "length": 4096})]):
            peer.messages.insert(3, {"id": identifier, "method": "item/tool/call", "params": {
                "threadId": "thread", "turnId": "turn", "namespace": "workspace",
                "tool": name, "callId": f"call-{identifier}", "arguments": arguments}})
        service = RuntimeService(repo, SimpleNamespace(invoke=client.run_admitted))
        result = service.execute(snapshot, request)
        assert result.result["accepted"] is True
        replies = {item["id"]: item["result"] for item in peer.sent if item.get("id") in (7, 8)}
        assert replies[7]["success"] and replies[8]["success"]
        listed = json.loads(replies[7]["contentItems"][0]["text"])["result"]
        assert listed == selections
        data = json.loads(replies[8]["contentItems"][0]["text"])["result"]["data"]
        assert base64.b64decode(data) == b"data"
        assert service.execute(snapshot, request) == result
        assert peer.closed
    finally:
        workspace.close()
