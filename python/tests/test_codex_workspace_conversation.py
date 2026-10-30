"""Full fixture conversation through the durable runtime and scoped workspace tools."""

import json

import pytest
from test_codex_client import Peer
from test_workspace_tools import setup

from ahc.adapters.codex_client import CodexProtocolClient
from ahc.adapters.codex_protocol import CODEX_EXECUTABLE_SHA256
from ahc.application.runtime_service import RuntimeService
from ahc.domain.agent_backend import AgentTurnRequest


@pytest.mark.parametrize("mode", ["completed", "duplicate", "lost"])
def test_client_dynamic_tool_runtime_conversation(tmp_path, mode):
    repo, workspace, tools = setup(tmp_path)
    peer = Peer()
    client = CodexProtocolClient(
        peer, cwd="workspace", model="fixture", clock=lambda: 0, workspace_tools=tools
    )
    snapshot = repo.read_current("session")
    request = AgentTurnRequest(
        snapshot.session,
        "operation",
        snapshot.goal.digest,
        "question",
        (),
        CODEX_EXECUTABLE_SHA256,
        client.configuration_digest,
        "work",
        100,
        capabilities=tools.capabilities(),
    )
    call = {
        "id": 7,
        "method": "item/tool/call",
        "params": {
            "threadId": "thread",
            "turnId": "turn",
            "namespace": "workspace",
            "tool": "execute",
            "callId": "call",
            "arguments": {"expected": workspace.identity.to_dict(), "code": "x=42"},
        },
    }
    peer.messages.insert(3, call)
    if mode == "duplicate":
        peer.messages.insert(4, call)
    if mode == "lost":

        def fail(expected, code):
            workspace.port.calls += 1
            raise RuntimeError("PRIVATE")

        workspace.port.execute = fail

    class Backend:
        def invoke(self, bound):
            return client.run_admitted(bound)

    try:
        result = RuntimeService(repo, Backend()).execute(snapshot, request)
        assert result.result["accepted"] is (mode == "completed")
        assert result.result["disposition"] == ("completed" if mode == "completed" else "unknown")
        if mode == "lost":
            assert result.result["backend_diagnostic"]["category"] == "workspace"
            assert result.result["backend_diagnostic"]["phase"] == "tool"
            assert result.result["backend_diagnostic"]["code"] == "workspace-effects-unresolved"
            assert result.result["backend_diagnostic"]["workspace_failure"] == {
                "phase":"execute", "code":"internal", "cleanup":"confirmed"}
        assert workspace.port.calls == 1
        replies = [m for m in peer.sent if m.get("id") == 7]
        assert len(replies) == 1
        payload = json.loads(replies[0]["result"]["contentItems"][0]["text"])
        assert payload["disposition"] == ("unknown" if mode == "lost" else "observed")
        assert peer.sent[0]["params"]["capabilities"]["experimentalApi"] is True
        thread = next(m for m in peer.sent if m.get("method") == "thread/start")
        assert thread["params"]["dynamicTools"][0]["type"] == "namespace"
        assert peer.closed
    finally:
        workspace.close()


def test_multistep_turn_beyond_sixteen_tools_completes_and_survives_reopen(tmp_path):
    repo, workspace, tools = setup(tmp_path)
    peer = Peer()
    client = CodexProtocolClient(
        peer, cwd="workspace", model="fixture", clock=lambda: 0, workspace_tools=tools
    )
    snapshot = repo.read_current("session")
    request = AgentTurnRequest(
        snapshot.session, "operation", snapshot.goal.digest, "question", (),
        CODEX_EXECUTABLE_SHA256, client.configuration_digest, "work", 100,
        capabilities=tools.capabilities(),
    )
    calls = []
    for ordinal in range(1, 18):
        expected = workspace.identity.to_dict()
        expected["revision"] = ordinal - 1
        calls.append({
            "id": ordinal + 7, "method": "item/tool/call",
            "params": {
                "threadId": "thread", "turnId": "turn", "namespace": "workspace",
                "tool": "execute", "callId": "call-" + str(ordinal),
                "arguments": {"expected": expected, "code": "x=42"},
            },
        })
    peer.messages[3:3] = calls

    class Backend:
        def invoke(self, bound):
            return client.run_admitted(bound)

    try:
        turn = RuntimeService(repo, Backend()).execute(snapshot, request)
        assert workspace.port.calls == 17
        replies = [frame for frame in peer.sent if "result" in frame]
        assert len(replies) == 17 and all(frame["result"]["success"] for frame in replies)
        assert json.loads(replies[-1]["result"]["contentItems"][0]["text"])["tool_budget"] == {
            "calls": 17, "remaining_ms": 100000,
        }
        assert turn.result["accepted"] is True
        assert turn.result["disposition"] == "completed" and not turn.result["unresolved_effects"]
        assert type(repo)(repo.root).read_turn(snapshot.session, request.operation_id) == turn
        assert peer.closed
    finally:
        workspace.close()


def test_schema_rejection_feedback_reaches_provider_before_a_corrected_call(tmp_path):
    repo, workspace, tools = setup(tmp_path)
    peer = Peer()
    client = CodexProtocolClient(
        peer, cwd="workspace", model="fixture", clock=lambda: 0, workspace_tools=tools
    )
    snapshot = repo.read_current("session")
    request = AgentTurnRequest(
        snapshot.session, "operation", snapshot.goal.digest, "question", (),
        CODEX_EXECUTABLE_SHA256, client.configuration_digest, "work", 100,
        capabilities=tools.capabilities(),
    )
    calls = []
    for rpc_id in (7, 8):
        expected = workspace.identity.to_dict()
        if rpc_id == 7:
            expected["session"]["ref"]["schema_version"] = "PRIVATE-INVENTED-VERSION"
        calls.append({
            "id": rpc_id, "method": "item/tool/call",
            "params": {
                "threadId": "thread", "turnId": "turn", "namespace": "workspace",
                "tool": "execute", "callId": "call-" + str(rpc_id),
                "arguments": {"expected": expected, "code": "x=42"},
            },
        })
    peer.messages[3:3] = calls

    class Backend:
        def invoke(self, bound):
            return client.run_admitted(bound)

    try:
        turn = RuntimeService(repo, Backend()).execute(snapshot, request)
        assert turn.result["accepted"] and workspace.port.calls == 1
        replies = {frame["id"]: frame["result"] for frame in peer.sent
                   if frame.get("id") in (7, 8) and "result" in frame}
        assert not replies[7]["success"] and replies[8]["success"]
        first = json.loads(replies[7]["contentItems"][0]["text"])
        assert first["invalid_fields"] == ["expected.session.ref.schema_version"]
        assert first["required_schema_versions"] == {
            "expected.session.ref.schema_version": "ahc.virtual-session-ref.v1",
        }
        assert "PRIVATE" not in json.dumps(first)
        assert first["tool_budget"] == {"calls": 1, "remaining_ms": 100000}
        second = json.loads(replies[8]["contentItems"][0]["text"])
        assert second["tool_budget"] == {"calls": 2, "remaining_ms": 100000}
    finally:
        workspace.close()
