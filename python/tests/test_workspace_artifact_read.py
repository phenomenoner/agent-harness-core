"""Agent-facing artifact reads enforce ownership, current workspace and byte bounds."""

import base64
from dataclasses import replace
from types import SimpleNamespace

from test_artifacts import state

from ahc.adapters.codex_workspace_tools import CodexWorkspaceTools
from ahc.application.workspace_tools import WorkspaceTools


def test_chunk_read_and_schema(tmp_path):
    repo, _, producer, artifacts = state(tmp_path)
    manifest = artifacts.publish(producer, b"hello" * 1000, "text/plain")
    workspace = SimpleNamespace(identity=producer, store=repo, closed=False)
    tools = WorkspaceTools(workspace, artifacts=artifacts)
    arguments = {"expected": producer.to_dict(), "artifact_id": manifest.artifact_id,
                 "offset": 0, "length": 4096}
    first = tools.invoke("workspace.read_artifact", arguments)
    assert first["disposition"] == "observed"
    assert len(base64.b64decode(first["result"]["data"])) == 4096
    assert first["result"]["next_offset"] == 4096 and not first["result"]["eof"]
    last = tools.invoke("workspace.read_artifact", {**arguments, "offset": 4096})
    assert last["result"]["eof"] and last["result"]["next_offset"] == 5000
    request = SimpleNamespace(capabilities=("workspace.read_artifact",))
    declaration = CodexWorkspaceTools.describe(request, tools)[0]["tools"][0]
    assert declaration["name"] == "read_artifact"
    assert declaration["inputSchema"]["properties"]["length"]["maximum"] == 4096
    for change in ({"offset": True}, {"offset": 5001}, {"length": 4097},
                   {"artifact_id": "f" * 64},
                   {"expected": replace(producer, revision=9).to_dict()}):
        result = tools.invoke("workspace.read_artifact", {**arguments, **change})
        assert result == {"disposition": "rejected", "unresolved_effects": False,
                          "reason": "artifact-read-unavailable"}
    other = replace(producer, session=replace(producer.session, agent_id="other"))
    workspace.identity = other
    assert tools.invoke("workspace.read_artifact", {
        **arguments, "expected": other.to_dict()})["disposition"] == "rejected"


def test_offset_above_the_former_total_limit_reads_the_actual_tail(tmp_path):
    repo, _, producer, artifacts = state(tmp_path)
    content = b"x" * 1048577 + b"tail"
    manifest = artifacts.publish(producer, content, "application/octet-stream")
    workspace = SimpleNamespace(identity=producer, store=repo, closed=False)
    tools = WorkspaceTools(workspace, artifacts=artifacts)
    reply = tools.invoke("workspace.read_artifact", {"expected": producer.to_dict(),
        "artifact_id": manifest.artifact_id, "offset": 1048577, "length": 4096})
    assert reply["disposition"] == "observed" and base64.b64decode(reply["result"]["data"]) == b"tail"
    assert reply["result"]["eof"]
    schema = CodexWorkspaceTools.describe(SimpleNamespace(capabilities=("workspace.read_artifact",)),
                                         tools)[0]["tools"][0]["inputSchema"]
    assert "maximum" not in schema["properties"]["offset"]
